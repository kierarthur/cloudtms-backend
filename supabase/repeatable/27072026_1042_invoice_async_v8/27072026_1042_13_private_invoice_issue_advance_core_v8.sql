create or replace function private._invoice_issue_advance_core_v8(
  p_claims jsonb,
  p_now_utc timestamptz
) returns jsonb
language plpgsql
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_now timestamptz:=coalesce(p_now_utc,now());
  v_result jsonb:='[]'::jsonb;
  v_part jsonb;
begin
  if p_claims is null or jsonb_typeof(p_claims) is distinct from 'array' then
    raise exception using errcode='22023',
      message='p_claims must be a JSON array containing 1..100 claims';
  end if;
  if jsonb_array_length(p_claims) < 1 or jsonb_array_length(p_claims) > 100 then
    raise exception using errcode='22023',
      message='p_claims must be a JSON array containing 1..100 claims';
  end if;

  -- VALIDATE preserves stable issue blocker codes and makes no legal transition.
  with ids as materialized (
    select case when coalesce(x->>'chunk_id','')~
      '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
      then (x->>'chunk_id')::uuid end chunk_id
    from jsonb_array_elements(p_claims) x
    where x->>'phase'='VALIDATE'
  ),
  requests as materialized (
    select c.id chunk_id,c.operation_id,c.entity_id invoice_id,
      case when pg_input_is_valid(
          nullif(c.payload_json->>'evaluation_date',''),'date')
        then(c.payload_json->>'evaluation_date')::date end
        evaluation_date,
      jsonb_build_object(
        'request_key',c.id::text,
        'invoice_id',c.entity_id,
        'operation_id',c.operation_id,
        'expected_revision',c.payload_json->'source_revision',
        'allow_early',coalesce(c.payload_json->'allow_early','false'::jsonb),
        'deliver',coalesce(c.payload_json->'deliver','false'::jsonb),
        'recipient_set',coalesce(c.payload_json#>'{delivery_intent,recipient_set}',
          '[]'::jsonb),
        'cc',coalesce(c.payload_json#>'{delivery_intent,cc}','[]'::jsonb),
        'bcc',coalesce(c.payload_json#>'{delivery_intent,bcc}','[]'::jsonb)
      ) request_json
    from ids x
    join public.invoice_operation_chunks c on c.id=x.chunk_id
  ),
  validation_groups as materialized (
    select r.evaluation_date,
      jsonb_agg(r.request_json order by r.chunk_id) request_json
    from requests r
    group by r.evaluation_date
  ),
  shared_validation as materialized (
    select v.*
    from validation_groups g
    cross join lateral private._invoice_issue_validate_batch(
      g.request_json,g.evaluation_date) v
  ),
  eval as materialized (
    select r.chunk_id,r.operation_id,r.invoice_id,
      array(select jsonb_array_elements_text(
        coalesce(v.hard_blocker_codes,'[]'::jsonb))) blockers
    from requests r
    left join shared_validation v
      on v.request_key=r.chunk_id::text and v.invoice_id=r.invoice_id
  ),
  updated as (
    update public.invoice_operation_chunks c set
      phase=case when cardinality(e.blockers)=0 then 'FREEZE' else 'BLOCKED' end,
      status=case when cardinality(e.blockers)=0 then 'QUEUED' else 'BLOCKED' end,
      error_json=case when cardinality(e.blockers)=0 then null else
        jsonb_build_object('code',e.blockers[1],'blocker_codes',to_jsonb(e.blockers),'invoice_id',e.invoice_id) end,
      progress_json=jsonb_build_object('status_message',
        case when cardinality(e.blockers)=0 then 'Issue validation passed' else 'Issue blocked' end,
        'blocker_codes',to_jsonb(e.blockers)),
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,
      updated_at_utc=v_now
    from eval e where c.id=e.chunk_id returning c.id,c.status,c.phase,c.error_json
  )
  select coalesce(jsonb_agg(jsonb_build_object('chunk_id',id,'status',status,'phase',phase,'error',error_json)),'[]')
    into v_part from updated;
  v_result:=v_result||coalesce(v_part,'[]');

  -- FREEZE: immutable legal/render snapshot; invoice deliberately remains DRAFT.
  with ids as materialized (
    select case when coalesce(x->>'chunk_id','') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then (x->>'chunk_id')::uuid end chunk_id from jsonb_array_elements(p_claims) x where x->>'phase'='FREEZE'
  ),
  freeze_route_requests as materialized (
    select c.id chunk_id,
      case when pg_input_is_valid(
          nullif(c.payload_json->>'evaluation_date',''),'date')
        then(c.payload_json->>'evaluation_date')::date end
        evaluation_date,
      jsonb_build_object(
      'request_key',c.id::text,
      'invoice_id',c.entity_id,
      'recipient_set',coalesce(
        c.payload_json#>'{delivery_intent,recipient_set}','[]'::jsonb),
      'cc',coalesce(c.payload_json#>'{delivery_intent,cc}','[]'::jsonb),
      'bcc',coalesce(c.payload_json#>'{delivery_intent,bcc}','[]'::jsonb),
      'delivery_policy',coalesce(
        c.payload_json#>>'{delivery_intent,delivery_policy}','ATTACH'),
      'template_version',coalesce(
        c.payload_json#>>'{delivery_intent,template_version}',
        'invoice-delivery-v1')
    ) request_json
    from ids x join public.invoice_operation_chunks c on c.id=x.chunk_id
  ),
  freeze_route_groups as materialized (
    select q.evaluation_date,
      jsonb_agg(q.request_json order by q.chunk_id) request_json
    from freeze_route_requests q
    group by q.evaluation_date
  ),
  frozen_routes as materialized (
    select r.*
    from freeze_route_groups g
    cross join lateral private._invoice_delivery_routes_batch(
      g.request_json,g.evaluation_date) r
  ),
  freeze_seed as materialized (
    select c.id chunk_id,c.operation_id,c.entity_id invoice_id,c.payload_json,
      i.document_revision,i.invoice_no,i.client_id,
      frozen_clock.issue_at_utc,
      frozen_clock.issue_at_utc tax_point_utc,
      frozen_clock.issue_at_utc+make_interval(days=>coalesce(
        case when pg_input_is_valid(nullif(i.header_snapshot_json->>'payment_terms_days',''),'integer')
          then greatest(0,least(3650,(i.header_snapshot_json->>'payment_terms_days')::integer)) end,
        cl.payment_terms_days,30)) due_at_utc,
      rr.request_json routing_request,
      jsonb_build_object(
        'request_key',dr.request_key,
        'to',coalesce(dr.canonical_to,'[]'::jsonb),
        'cc',coalesce(dr.canonical_cc,'[]'::jsonb),
        'bcc',coalesce(dr.canonical_bcc,'[]'::jsonb),
        'recipient_set_hash',dr.recipient_set_hash,
        'route_policy_hash',dr.route_policy_hash,
        'route_source',dr.route_source,
        'do_not_send',dr.do_not_send,
        'delivery_suppressed',dr.delivery_suppressed,
        'suppression_reason',dr.suppression_reason,
        'client_settings_id',dr.client_settings_id,
        'contract_settings_ids',dr.contract_settings_ids,
        'effective_date',dr.effective_date,
        'client_id',dr.client_id,
        'invoice_group_identity',dr.invoice_group_identity,
        'self_bill',dr.self_bill,
        'grouping_identity',dr.grouping_identity,
        'template_version',coalesce(c.payload_json#>>'{delivery_intent,template_version}','invoice-delivery-v1'),
        'delivery_policy',upper(coalesce(c.payload_json#>>'{delivery_intent,delivery_policy}','ATTACH')),
        'warning_codes',coalesce(dr.warning_codes,'[]'::jsonb),
        'blocker_codes',coalesce(dr.blocker_codes,'[]'::jsonb),
        'evaluated_date',dr.effective_date
      ) delivery_route,
      coalesce(nullif(c.payload_json->>'delivery_request_token',''),
        'ISSUE:'||coalesce(c.payload_json->>'command_token',c.id::text)) delivery_request_token,
      coalesce(c.payload_json->'deliver','false'::jsonb) deliver
    from ids x
    join public.invoice_operation_chunks c on c.id=x.chunk_id
    join public.invoices i on i.id=c.entity_id and i.status='DRAFT'
    join public.clients cl on cl.id=i.client_id
    cross join lateral (
      select case when pg_input_is_valid(nullif(c.payload_json->>'frozen_issue_at_utc',''),'timestamptz')
        then (c.payload_json->>'frozen_issue_at_utc')::timestamptz else v_now end issue_at_utc
    ) frozen_clock
    left join frozen_routes dr on dr.request_key=c.id::text and dr.invoice_id=i.id
    left join freeze_route_requests rr on rr.chunk_id=c.id
  ),
  presentation_requests as materialized (
    select coalesce(jsonb_agg(jsonb_build_object(
        'request_key',fs.chunk_id::text,
        'entity_type','INVOICE',
        'entity_id',fs.invoice_id,
        'purpose','FINAL_ISSUE',
        'template_version','invoice-professional-v2',
        'issue_at_utc',fs.issue_at_utc,
        'tax_point_utc',fs.tax_point_utc,
        'due_at_utc',fs.due_at_utc
      ) order by fs.chunk_id),'[]'::jsonb) requests
    from freeze_seed fs
  ),
  presentation_batch as materialized (
    select p.*
    from (
      select pr.requests
      from presentation_requests pr
      where jsonb_typeof(pr.requests)='array'
        and jsonb_array_length(pr.requests)>0
    ) pr
    cross join lateral private._invoice_presentation_snapshot_batch(
      pr.requests,v_now) p
  ),
  frozen as materialized (
    select fs.chunk_id,fs.operation_id,fs.invoice_id,fs.payload_json,
      fs.document_revision,fs.invoice_no,fs.client_id,
      (
        pb.snapshot_json
        || jsonb_build_object(
          'snapshot_schema_version','FINAL_ISSUE_PRESENTATION_SNAPSHOT_V5',
          'issue_date_utc',fs.issue_at_utc,
          'tax_point_utc',fs.tax_point_utc,
          'due_date_utc',fs.due_at_utc,
          'routing_request',coalesce(fs.routing_request,'{}'::jsonb),
          'delivery_route',coalesce(fs.delivery_route,'{}'::jsonb),
          'delivery_intent',coalesce(fs.payload_json->'delivery_intent','{}'::jsonb),
          'delivery_request_token',fs.delivery_request_token,
          'deliver',fs.deliver,
          'presentation_model_hash',coalesce(pb.presentation_model->>'presentation_model_hash',pb.snapshot_json->>'presentation_model_hash',encode(digest(coalesce(pb.presentation_model,'{}'::jsonb)::text,'sha256'),'hex'))
        )
      ) snapshot,
      pb.valid presentation_valid,
      pb.error_code presentation_error_code,
      pb.error_detail presentation_error_detail
    from freeze_seed fs
    left join presentation_batch pb on pb.request_key=fs.chunk_id::text
  ),
  existing_versions as materialized (
    select f.*,v.id existing_document_version_id,v.operation_id doc_operation_id,
      o.control_version doc_control_version,v.status document_status,
      v.snapshot_hash existing_snapshot_hash
    from frozen f join public.invoice_document_versions v
      on coalesce(f.presentation_valid,false) is true
      and v.entity_type='INVOICE' and v.entity_id=f.invoice_id
      and v.purpose='FINAL_ISSUE'
      and v.snapshot_hash=encode(digest(f.snapshot::text,'sha256'),'hex')
      and v.template_version='invoice-professional-v2'
      and v.status in('PLANNING','WAITING_FOR_INPUTS','RENDERING',
        'ASSEMBLING','VERIFYING','READY')
    join public.invoice_operations o on o.id=v.operation_id
  ),
  doc_ops as materialized (
    insert into public.invoice_operations(parent_operation_id,operation_type,entity_type,entity_id,actor_user_id,
      idempotency_key,status,phase,priority,source_revision,template_version,input_json,config_json,progress_json,
      total_units,chunk_count,control_version,change_seq,created_at_utc,updated_at_utc)
    select f.operation_id,'BUILD_DOCUMENT','INVOICE',f.invoice_id,o.actor_user_id,
      encode(digest('FINAL_ISSUE|'||f.invoice_id||'|'||
        encode(digest(f.snapshot::text,'sha256'),'hex')||
        '|invoice-professional-v2','sha256'),'hex'),
      'QUEUED','BUILD_MANIFEST',850,f.document_revision::text,'invoice-professional-v2',
      jsonb_build_object('invoice_id',f.invoice_id,'purpose','FINAL_ISSUE'),
      jsonb_build_object('processor_policy',o.config_json->'processor_policy'),
      '{}',1,1,1,
      nextval('public.invoice_operation_change_seq'),v_now,v_now
    from frozen f join public.invoice_operations o on o.id=f.operation_id
    where coalesce(f.presentation_valid,false) is true
      and not exists(select 1 from existing_versions e where e.chunk_id=f.chunk_id)
    on conflict(idempotency_key) where status in ('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
    do update set priority=greatest(invoice_operations.priority,850),updated_at_utc=v_now
    returning *
  ),
  selected as materialized (
    select f.chunk_id,f.operation_id,f.invoice_id,f.payload_json,f.document_revision,
      f.invoice_no,f.client_id,f.snapshot,d.id doc_operation_id,d.control_version doc_control_version,
      null::uuid existing_document_version_id,null::text document_status,
      null::text existing_snapshot_hash
    from frozen f join doc_ops d on d.entity_id=f.invoice_id
    where coalesce(f.presentation_valid,false) is true
    union all
    select f.chunk_id,f.operation_id,f.invoice_id,f.payload_json,f.document_revision,
      f.invoice_no,f.client_id,f.snapshot,e.doc_operation_id,e.doc_control_version,
      e.existing_document_version_id,e.document_status,e.existing_snapshot_hash
    from frozen f join existing_versions e on e.chunk_id=f.chunk_id
    where coalesce(f.presentation_valid,false) is true
  ),
  presentation_blocked as (
    update public.invoice_operation_chunks c set
      status='BLOCKED',phase='BLOCKED',failed_at_utc=v_now,
      error_json=jsonb_build_object(
        'code',coalesce(f.presentation_error_code,'FINAL_ISSUE_PRESENTATION_INVALID'),
        'detail',coalesce(f.presentation_error_detail,'{}'::jsonb),
        'invoice_id',f.invoice_id),
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,
      updated_at_utc=v_now
    from frozen f
    where c.id=f.chunk_id and coalesce(f.presentation_valid,false) is not true
    returning c.id,c.status,c.phase,c.document_version_id
  ),
  versions as materialized (
    insert into public.invoice_document_versions(entity_type,entity_id,purpose,operation_id,source_revision,
      template_version,status,snapshot_json,snapshot_hash,manifest_json,manifest_hash,created_at_utc)
    select 'INVOICE',s.invoice_id,'FINAL_ISSUE',s.doc_operation_id,s.document_revision::text,
      'invoice-professional-v2','PLANNING',s.snapshot,encode(digest(s.snapshot::text,'sha256'),'hex'),
      '[]',encode(digest('[]','sha256'),'hex'),v_now
    from selected s
    where s.existing_document_version_id is null
    on conflict(entity_type,entity_id,purpose,snapshot_hash,template_version)
      where purpose='FINAL_ISSUE'
        and status in ('PLANNING','WAITING_FOR_INPUTS','RENDERING','ASSEMBLING','VERIFYING','READY')
    do nothing
    returning id,entity_id,operation_id,snapshot_hash
  ),
  exact_versions as materialized (
    select s.*,coalesce(s.existing_document_version_id,v.id) document_version_id,
      coalesce(s.existing_snapshot_hash,v.snapshot_hash) snapshot_hash,
      coalesce(s.document_status,'PLANNING') exact_document_status
    from selected s left join versions v
      on v.entity_id=s.invoice_id and v.operation_id=s.doc_operation_id
  ),
  doc_chunks as (
    insert into public.invoice_operation_chunks(operation_id,chunk_type,phase,work_key,sequence_no,entity_type,entity_id,
      document_version_id,status,priority,run_after_utc,payload_json,operation_control_version,created_at_utc,updated_at_utc)
    select s.doc_operation_id,'DOCUMENT_PLAN','BUILD_MANIFEST',
      encode(digest(concat_ws('|','DOCUMENT_PLAN',
        s.document_version_id::text,s.document_revision::text,
        'invoice-professional-v2','1'),'sha256'),'hex'),
      0,'INVOICE',s.invoice_id,s.document_version_id,
      'QUEUED',850,v_now,jsonb_build_object('purpose','FINAL_ISSUE'),s.doc_control_version,v_now,v_now
    from exact_versions s
    where s.exact_document_status<>'READY'
    on conflict(operation_id,chunk_type,level_no,sequence_no,work_key) do nothing returning id
  ),
  invoice_state as (
    update public.invoices i set issue_state='PREPARING_DOCUMENT',
      active_issue_operation_id=s.operation_id,
      active_document_operation_id=case when s.exact_document_status='READY'
        then i.active_document_operation_id else s.doc_operation_id end,
      document_state=case when s.exact_document_status='READY'
        then i.document_state else 'PREPARING' end,updated_at=v_now
    from exact_versions s where i.id=s.invoice_id and i.status='DRAFT' returning i.id
  ),
  advanced as (
    update public.invoice_operation_chunks c set
      phase=case when s.exact_document_status='READY' then 'FINALISE' else 'WAIT_DOCUMENT' end,
      status=case when s.exact_document_status='READY' then 'QUEUED' else 'WAITING' end,
      document_version_id=s.document_version_id,
      payload_json=c.payload_json||jsonb_build_object('frozen_document_revision',s.document_revision,
        'document_version_id',s.document_version_id,'document_operation_id',s.doc_operation_id,
        'snapshot_hash',s.snapshot_hash,
        'routing_request',s.snapshot->'routing_request',
        'frozen_delivery_route',s.snapshot->'delivery_route',
        'delivery_request_token',s.snapshot->>'delivery_request_token'),
      progress_json=jsonb_build_object('status_message',
        case when s.exact_document_status='READY'
          then 'Exact final document is ready' else 'Final issue document is preparing' end),
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,
      updated_at_utc=v_now
    from exact_versions s where c.id=s.chunk_id
    returning c.id,c.status,c.phase,c.document_version_id
  ),
  freeze_outcomes as (
    select id,status,phase,document_version_id from advanced
    union all
    select id,status,phase,document_version_id from presentation_blocked
  )
  select coalesce(jsonb_agg(jsonb_build_object('chunk_id',id,'status',status,'phase',phase,
    'document_version_id',document_version_id)),'[]') into v_part from freeze_outcomes;
  v_result:=v_result||coalesce(v_part,'[]');

  -- WAIT_DOCUMENT observes only the exact row and never R2.
  with ids as materialized (
    select case when coalesce(x->>'chunk_id','') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then (x->>'chunk_id')::uuid end chunk_id from jsonb_array_elements(p_claims) x where x->>'phase'='WAIT_DOCUMENT'
  ),
  updated as (
    update public.invoice_operation_chunks c set
      phase=case when v.status='READY' then 'FINALISE'
                 when v.status in ('FAILED','SUPERSEDED','CANCELLED') then 'BLOCKED'
                 else 'WAIT_DOCUMENT' end,
      status=case when v.status='READY' then 'QUEUED'
                  when v.status in ('FAILED','SUPERSEDED','CANCELLED') then 'BLOCKED'
                  else 'WAITING' end,
      error_json=case when v.status in ('FAILED','SUPERSEDED','CANCELLED')
        then jsonb_build_object('code','FINAL_DOCUMENT_'||v.status,'document_version_id',v.id,'detail',v.error_json)
        else null end,run_after_utc=case when v.status='READY' then v_now else c.run_after_utc end,
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,
      updated_at_utc=v_now
    from ids x,public.invoice_document_versions v
    where c.id=x.chunk_id and v.id=c.document_version_id
    returning c.id,c.status,c.phase,c.error_json
  ),
  missing as (
    update public.invoice_operation_chunks c set
      phase='BLOCKED',status='BLOCKED',
      error_json=jsonb_build_object(
        'code','FINAL_DOCUMENT_VERSION_MISSING',
        'document_version_id',c.document_version_id),
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,
      updated_at_utc=v_now
    from ids x
    where c.id=x.chunk_id
      and not exists(
        select 1 from public.invoice_document_versions v
        where v.id=c.document_version_id)
    returning c.id,c.status,c.phase,c.error_json
  ),
  outcomes as (
    select * from updated
    union all
    select * from missing
  )
  select coalesce(jsonb_agg(jsonb_build_object('chunk_id',id,'status',status,'phase',phase,'error',error_json)),'[]')
    into v_part from outcomes;
  v_result:=v_result||coalesce(v_part,'[]');

  -- FINALISE is the only legal issue transition and demands the exact verified version.
  with ids as materialized (
    select case when coalesce(x->>'chunk_id','') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then (x->>'chunk_id')::uuid end chunk_id from jsonb_array_elements(p_claims) x where x->>'phase'='FINALISE'
  ),
  locked_invoices as materialized (
    select invoice.id
    from ids supplied
    join public.invoice_operation_chunks carrier
      on carrier.id=supplied.chunk_id
    join public.invoices invoice
      on invoice.id=carrier.entity_id
    order by invoice.id
    for update of invoice
  ),
  eligible as materialized (
    select c.*,i.document_revision,v.id final_document_version_id,v.r2_key,v.sha256,v.size_bytes,v.page_count,
      v.verified_at_utc,
      case when pg_input_is_valid(
        nullif(v.snapshot_json->>'issue_date_utc',''),'timestamptz')
        then (v.snapshot_json->>'issue_date_utc')::timestamptz
        else v_now end issue_at,
      case when pg_input_is_valid(
        nullif(v.snapshot_json->>'tax_point_utc',''),'timestamptz')
        then (v.snapshot_json->>'tax_point_utc')::timestamptz end tax_point_at,
      case when pg_input_is_valid(
        nullif(v.snapshot_json->>'due_date_utc',''),'timestamptz')
        then (v.snapshot_json->>'due_date_utc')::timestamptz end due_at,
      v.snapshot_hash
    from ids x join public.invoice_operation_chunks c on c.id=x.chunk_id
    join locked_invoices locked_invoice on locked_invoice.id=c.entity_id
    join public.invoices i on i.id=locked_invoice.id and i.status='DRAFT'
      and i.issue_state='PREPARING_DOCUMENT'
      and i.active_issue_operation_id=c.operation_id
    join public.invoice_document_versions v on v.id=c.document_version_id and v.purpose='FINAL_ISSUE'
      and v.entity_type='INVOICE' and v.entity_id=c.entity_id
      and v.status='READY' and v.verified_at_utc is not null and v.r2_key is not null
      and v.sha256 is not null and v.size_bytes>0 and v.page_count>0
    where case when pg_input_is_valid(
        nullif(c.payload_json->>'frozen_document_revision',''),'bigint')
      then(c.payload_json->>'frozen_document_revision')::bigint=i.document_revision
      else false end
      and v.source_revision=i.document_revision::text
      and c.payload_json->>'snapshot_hash'=v.snapshot_hash
  ),
  issued as (
    update public.invoices i set status='ISSUED',status_date_utc=v_now,
      issued_at_utc=e.issue_at,due_at_utc=e.due_at,
      on_hold_reason=null,issued_document_version_id=e.final_document_version_id,
      invoice_pdf_r2_key=e.r2_key,invoice_pdf_generated_at_utc=e.verified_at_utc,
      issue_state='ISSUED',document_state='READY',active_issue_operation_id=null,
      active_document_operation_id=case
        when pg_input_is_valid(
          nullif(e.payload_json->>'document_operation_id',''),'uuid')
          then case when i.active_document_operation_id=
              (e.payload_json->>'document_operation_id')::uuid
            then null else i.active_document_operation_id end
        else i.active_document_operation_id end,updated_at=v_now
    from eligible e where i.id=e.entity_id returning i.id
  ),
  audits as (
    insert into public.audit_events(ts_utc,actor_user_id,actor_display,actor_role_at_time,
      object_type,object_id_text,action,after_json,reason)
    select v_now,o.actor_user_id,u.display_name,u.role,'invoice',e.entity_id::text,'INVOICE_ISSUED',
      jsonb_build_object('invoice_id',e.entity_id,'document_version_id',e.final_document_version_id,
        'issue_at_utc',e.issue_at,'tax_point_utc',e.tax_point_at,'due_at_utc',e.due_at,
        'sha256',e.sha256,'size_bytes',e.size_bytes,'page_count',e.page_count),'INVOICE_OPERATION_QUEUE'
    from eligible e join public.invoice_operations o on o.id=e.operation_id
    left join public.tms_users u on u.id=o.actor_user_id returning id
  ),
  advanced as (
    update public.invoice_operation_chunks c set
      phase=case when lower(coalesce(c.payload_json->>'deliver','false'))
        in('true','t','1','yes') then 'QUEUE_DELIVERY' else 'COMPLETE' end,
      status=case when lower(coalesce(c.payload_json->>'deliver','false'))
        in('true','t','1','yes') then 'QUEUED' else 'COMPLETE' end,
      completed_at_utc=case when lower(coalesce(
        c.payload_json->>'deliver','false')) in('true','t','1','yes')
        then null else v_now end,
      result_json=jsonb_build_object('invoice_id',e.entity_id,'document_version_id',e.final_document_version_id,
        'issue_at_utc',e.issue_at,'tax_point_utc',e.tax_point_at,'due_at_utc',e.due_at,
        'r2_key',e.r2_key,'sha256',e.sha256,'size_bytes',e.size_bytes,'page_count',e.page_count),
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,
      updated_at_utc=v_now
    from eligible e where c.id=e.id
    returning c.id,c.status,c.phase,c.result_json,c.error_json
  ),
  ineligible as (
    update public.invoice_operation_chunks c
    set status='BLOCKED',phase='BLOCKED',failed_at_utc=v_now,
        error_json=jsonb_build_object(
          'code','ISSUE_FINALISE_OWNERSHIP_OR_DOCUMENT_MISMATCH',
          'invoice_id',c.entity_id,
          'document_version_id',c.document_version_id),
        lease_owner=null,lease_token=null,lease_expires_at_utc=null,
        updated_at_utc=v_now
    from ids x
    where c.id=x.chunk_id
      and not exists(select 1 from eligible e where e.id=c.id)
    returning c.id,c.status,c.phase,c.result_json,c.error_json
  ),
  outcomes as (
    select * from advanced
    union all
    select * from ineligible
  )
  select coalesce(jsonb_agg(jsonb_build_object(
    'chunk_id',id,'status',status,'phase',phase,
    'result',result_json,'error',error_json)),'[]')
    into v_part from outcomes;
  v_result:=v_result||coalesce(v_part,'[]');

  -- QUEUE_DELIVERY durably delegates to the single delivery-routing authority.
  with ids as materialized (
    select case when coalesce(x->>'chunk_id','') ~*
      '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      then (x->>'chunk_id')::uuid end chunk_id
    from jsonb_array_elements(p_claims) x
    where x->>'phase'='QUEUE_DELIVERY'
  ),
  delivery_specs as materialized (
    select c.id chunk_id,c.operation_id parent_operation_id,c.entity_id invoice_id,
      o.actor_user_id,i.issued_document_version_id,
      coalesce(c.payload_json->'delivery_intent','{}'::jsonb) delivery_intent,
      coalesce(c.payload_json->'routing_request','{}'::jsonb)
        routing_request,
      coalesce(c.payload_json->'frozen_delivery_route','{}'::jsonb)
        frozen_delivery_route,
      coalesce(nullif(c.payload_json->>'delivery_request_token',''),
        'ISSUE:'||coalesce(c.payload_json->>'command_token',c.id::text))
        delivery_request_token,
      nullif(c.payload_json#>>'{frozen_delivery_route,template_version}','')
        template_version,
      nullif(c.payload_json#>>'{frozen_delivery_route,recipient_set_hash}','')
        recipient_set_hash,
      nullif(c.payload_json#>>'{frozen_delivery_route,route_policy_hash}','')
        route_policy_hash,
      coalesce(c.payload_json#>'{frozen_delivery_route,to}','[]'::jsonb)
        canonical_to,
      coalesce(c.payload_json#>'{frozen_delivery_route,cc}','[]'::jsonb)
        canonical_cc,
      coalesce(c.payload_json#>'{frozen_delivery_route,bcc}','[]'::jsonb)
        canonical_bcc,
      lower(coalesce(
        c.payload_json#>>'{frozen_delivery_route,do_not_send}','false'))
        in('true','t','1','yes') do_not_send,
      lower(coalesce(
        c.payload_json#>>'{frozen_delivery_route,delivery_suppressed}','false'))
        in('true','t','1','yes') delivery_suppressed,
      c.payload_json#>>'{frozen_delivery_route,route_source}' route_source,
      coalesce(c.payload_json#>'{frozen_delivery_route,warning_codes}',
        '[]'::jsonb) route_warnings,
      coalesce(c.payload_json#>'{frozen_delivery_route,blocker_codes}',
        '[]'::jsonb) route_blockers,
      c.payload_json#>>'{frozen_delivery_route,route_policy_hash}' is not null
        and c.payload_json#>>'{frozen_delivery_route,recipient_set_hash}'
          is not null
        and c.payload_json#>>'{frozen_delivery_route,template_version}'
          is not null
        and upper(coalesce(
          c.payload_json#>>'{frozen_delivery_route,delivery_policy}',''))
          in('ATTACH','SPLIT','SECURE_LINK')
        and lower(coalesce(
          c.payload_json#>>'{frozen_delivery_route,do_not_send}','false'))
          not in('true','t','1','yes')
        and lower(coalesce(
          c.payload_json#>>'{frozen_delivery_route,delivery_suppressed}','false'))
          not in('true','t','1','yes')
        and (case
          when jsonb_typeof(c.payload_json#>'{frozen_delivery_route,blocker_codes}')='array'
            then jsonb_array_length(c.payload_json#>'{frozen_delivery_route,blocker_codes}')
          when c.payload_json#>'{frozen_delivery_route,blocker_codes}' is null then 0
          else 1
        end)=0
        frozen_route_usable,
      coalesce(case when coalesce(c.payload_json#>>'{delivery_intent,part_number}','') ~
        '^[0-9]+$' then(c.payload_json#>>'{delivery_intent,part_number}')::integer end,1)
        part_number,
      case when upper(coalesce(
          c.payload_json#>>'{delivery_intent,delivery_policy}','')) in(
            'ATTACH','SPLIT','SECURE_LINK')
        then upper(c.payload_json#>>'{delivery_intent,delivery_policy}')
        when upper(coalesce(
          c.payload_json#>>'{frozen_delivery_route,delivery_policy}','')) in(
            'ATTACH','SPLIT','SECURE_LINK')
        then upper(c.payload_json#>>'{frozen_delivery_route,delivery_policy}')
      end delivery_policy
    from ids x join public.invoice_operation_chunks c on c.id=x.chunk_id
    join public.invoice_operations o on o.id=c.operation_id
    join public.invoices i on i.id=c.entity_id and i.status='ISSUED'
      and i.issued_document_version_id=c.document_version_id
  ),
  delivery_groups as materialized (
    select d.parent_operation_id,
      (array_agg(d.actor_user_id order by d.chunk_id))[1] actor_user_id,
      (array_agg(d.template_version order by d.chunk_id))[1] template_version,
      encode(digest(string_agg(concat_ws('|',d.invoice_id::text,
        d.route_policy_hash),'||' order by d.invoice_id),'sha256'),'hex')
        route_policy_hash,
      (array_agg(d.delivery_policy order by d.chunk_id))[1] delivery_policy,
      array_agg(d.invoice_id order by d.invoice_id) invoice_ids,
      array_agg(d.issued_document_version_id order by d.invoice_id)
        issued_document_version_ids,
      array_agg(d.delivery_request_token order by d.invoice_id)
        delivery_request_tokens,
      count(*)::integer unit_count,
      encode(digest(string_agg(concat_ws('|',d.invoice_id::text,
        d.issued_document_version_id::text,d.delivery_request_token),
        '||' order by d.invoice_id),'sha256'),'hex') group_request_hash,
      encode(digest(string_agg(concat_ws('|',d.invoice_id::text,
        d.issued_document_version_id::text,d.delivery_request_token,
        d.route_policy_hash),
        '||' order by d.invoice_id),'sha256'),'hex') delivery_revision,
      encode(digest(concat_ws('|','DELIVER_INVOICES',d.parent_operation_id::text,
        encode(digest(string_agg(concat_ws('|',d.invoice_id::text,
          d.issued_document_version_id::text,d.delivery_request_token),
          '||' order by d.invoice_id),'sha256'),'hex')),'sha256'),'hex')
        delivery_key
    from delivery_specs d
    group by d.parent_operation_id
  ),
  existing_delivery as materialized (
    select g.parent_operation_id,o.id operation_id,o.control_version,o.status
    from delivery_groups g join lateral(
      select x.* from public.invoice_operations x
      where x.idempotency_key=g.delivery_key
        and x.status in(
          'QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED','COMPLETE')
      order by(x.status='COMPLETE') desc,x.created_at_utc desc limit 1
    ) o on true
  ),
  inserted_delivery as materialized (
    insert into public.invoice_operations(parent_operation_id,operation_type,
      entity_type,entity_id,actor_user_id,idempotency_key,status,phase,priority,
      source_revision,template_version,input_json,config_json,progress_json,
      total_units,chunk_count,control_version,change_seq,requires_user_action,
      error_json,created_at_utc,updated_at_utc)
    select g.parent_operation_id,'DELIVER_INVOICES','INVOICE_BATCH',null,
      g.actor_user_id,g.delivery_key,'QUEUED','PREPARE',700,
      g.delivery_revision,g.template_version,
      jsonb_build_object('invoice_ids',to_jsonb(g.invoice_ids),
        'issued_document_version_ids',to_jsonb(g.issued_document_version_ids),
        'template_version',g.template_version,
        'delivery_request_tokens',to_jsonb(g.delivery_request_tokens),
        'group_request_hash',g.group_request_hash,
        'route_policy_hash',g.route_policy_hash,
        'delivery_policy',g.delivery_policy),
      jsonb_build_object(
        'delivery_request_tokens',to_jsonb(g.delivery_request_tokens),
        'group_request_hash',g.group_request_hash,
        'route_policy_hash',g.route_policy_hash,
        'processor_policy',parent.config_json->'processor_policy'),
      jsonb_build_object('status_message','Batch delivery preparation queued'),
      g.unit_count,g.unit_count,1,nextval('public.invoice_operation_change_seq'),
      false,null,v_now,v_now
    from delivery_groups g
    join public.invoice_operations parent on parent.id=g.parent_operation_id
    where not exists(
      select 1 from existing_delivery e
      where e.parent_operation_id=g.parent_operation_id)
    on conflict do nothing
    returning *
  ),
  selected_delivery_groups as materialized (
    select g.*,coalesce(e.operation_id,n.id) delivery_operation_id,
      coalesce(e.control_version,n.control_version) delivery_control_version,
      coalesce(e.status,n.status) delivery_status
    from delivery_groups g
    left join existing_delivery e
      on e.parent_operation_id=g.parent_operation_id
    left join inserted_delivery n on n.idempotency_key=g.delivery_key
  ),
  selected_delivery as materialized (
    select d.*,g.delivery_operation_id,g.delivery_control_version,
      g.delivery_status
    from delivery_specs d
    join selected_delivery_groups g
      on g.parent_operation_id=d.parent_operation_id
  ),
  delivery_chunk_specs as materialized (
    select d.*,gen_random_uuid() delivery_chunk_id
    from selected_delivery d
    where d.delivery_status<>'COMPLETE'
  ),
  delivery_chunks as (
    insert into public.invoice_operation_chunks(id,operation_id,chunk_type,phase,
      work_key,sequence_no,entity_type,entity_id,document_version_id,status,priority,
      run_after_utc,payload_json,error_json,operation_control_version,
      created_at_utc,updated_at_utc)
    select d.delivery_chunk_id,d.delivery_operation_id,'DELIVERY_PREPARE',
      case when d.frozen_route_usable then 'PREPARE' else 'BLOCKED' end,
      encode(digest(concat_ws('|','DELIVERY_PREPARE',
        d.issued_document_version_id::text,d.route_policy_hash,
        d.template_version,d.part_number::text,d.delivery_request_token),
        'sha256'),'hex'),
      row_number() over(partition by d.delivery_operation_id
        order by d.invoice_id)::integer-1,
      'INVOICE',d.invoice_id,d.issued_document_version_id,
      case when d.frozen_route_usable then 'QUEUED' else 'BLOCKED' end,
      700,v_now,
      jsonb_build_object('request_key',d.delivery_chunk_id::text,
        'invoice_id',d.invoice_id,
        'issued_document_version_id',d.issued_document_version_id,
        'recipient_set',d.canonical_to,'cc',d.canonical_cc,'bcc',d.canonical_bcc,
        'recipient_set_hash',d.recipient_set_hash,'template_version',d.template_version,
        'route_policy_hash',d.route_policy_hash,
        'delivery_part_number',d.part_number,'do_not_send',d.do_not_send,
        'delivery_suppressed',d.delivery_suppressed,
        'delivery_policy',d.delivery_policy,
        'route_source',d.route_source,
        'delivery_request_token',d.delivery_request_token,
        'routing_request',d.routing_request,
        'frozen_delivery_route',d.frozen_delivery_route),
      case when d.frozen_route_usable then null else jsonb_build_object(
        'code','FROZEN_DELIVERY_ROUTE_UNUSABLE',
        'frozen_route',d.frozen_delivery_route,
        'route_blockers',d.route_blockers) end,
      d.delivery_control_version,v_now,v_now
    from delivery_chunk_specs d
    on conflict(operation_id,chunk_type,level_no,sequence_no,work_key) do nothing
    returning id
  ),
  completed_delivery_request as (
    update public.invoice_operation_chunks c set status='COMPLETE',phase='COMPLETE',
      completed_at_utc=v_now,updated_at_utc=v_now,
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,
      payload_json=coalesce(c.payload_json,'{}'::jsonb)
        || jsonb_build_object(
          'blocked_for_sending',
          not d.frozen_route_usable
        ),
      result_json=coalesce(c.result_json,'{}')||jsonb_build_object(
        'delivery_operation_id',d.delivery_operation_id,
        'delivery_status',case when d.frozen_route_usable
          then d.delivery_status else 'BLOCKED' end,
        'delivery_requested',true,
        'delivery_request_token',d.delivery_request_token,
        'delivery_blocker',case when d.frozen_route_usable then null
          else jsonb_build_object('code','FROZEN_DELIVERY_ROUTE_UNUSABLE',
            'frozen_route',d.frozen_delivery_route,
            'route_blockers',d.route_blockers) end)
    from selected_delivery d where c.id=d.chunk_id
    returning c.id,c.status,c.phase,c.result_json
  )
  select coalesce(jsonb_agg(jsonb_build_object('chunk_id',id,'status',status,
    'phase',phase,'result',result_json)),'[]') into v_part
  from completed_delivery_request;
  v_result:=v_result||coalesce(v_part,'[]');

  return coalesce(v_result,'[]'::jsonb);
end;
$function$;

alter function private._invoice_issue_advance_core_v8(jsonb,timestamptz) owner to postgres;
revoke all on function private._invoice_issue_advance_core_v8(jsonb,timestamptz) from public,anon,authenticated;
grant execute on function private._invoice_issue_advance_core_v8(jsonb,timestamptz) to service_role;
