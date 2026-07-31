create or replace function private._invoice_document_operation_ensure_batch(
  p_requests jsonb,
  p_now_utc timestamptz default now()
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_requests jsonb := coalesce(p_requests,'[]'::jsonb);
  v_now timestamptz := coalesce(p_now_utc, now());
  v_processor_policy jsonb;
  v_result jsonb;
begin
  if jsonb_typeof(v_requests) is distinct from 'array' then
    raise exception using errcode='22023', message='INVOICE_DOCUMENT_ENSURE_REQUESTS_INVALID';
  end if;

  if jsonb_array_length(v_requests) < 1 or jsonb_array_length(v_requests) > 250 then
    raise exception using errcode='22023', message='INVOICE_DOCUMENT_ENSURE_REQUEST_COUNT_INVALID';
  end if;

  select private._invoice_processor_limits() into v_processor_policy;

  with
  raw_requests as materialized (
    select value request_json, ordinality::integer request_no
    from jsonb_array_elements(v_requests) with ordinality request_row(value, ordinality)
  ),
  requests as materialized (
    select
      request_no,
      nullif(btrim(coalesce(request_json->>'request_key','')),'') request_key,
      case when pg_input_is_valid(coalesce(request_json->>'invoice_id',''),'uuid')
        then (request_json->>'invoice_id')::uuid end invoice_id,
      case when coalesce(request_json->>'document_revision','') ~ '^[1-9][0-9]{0,8}$'
        then (request_json->>'document_revision')::integer end document_revision,
      upper(coalesce(nullif(request_json->>'purpose',''),'DRAFT_PREVIEW')) purpose,
      case when coalesce(request_json->>'priority','') ~ '^[0-9]{1,8}$'
        then greatest(0,least((request_json->>'priority')::integer,2000))
        else 450 end priority,
      case when pg_input_is_valid(coalesce(request_json->>'parent_operation_id',''),'uuid')
        then (request_json->>'parent_operation_id')::uuid end parent_operation_id,
      case when pg_input_is_valid(coalesce(request_json->>'actor_user_id',''),'uuid')
        then (request_json->>'actor_user_id')::uuid end actor_user_id,
      coalesce(nullif(request_json->>'template_version',''),'invoice-professional-v2') template_version,
      request_json
    from raw_requests
  ),
  invalid_requests as materialized (
    select request_no, coalesce(request_key,'request:'||request_no::text) request_key,
      case
        when request_key is null then 'REQUEST_KEY_REQUIRED'
        when invoice_id is null then 'INVOICE_ID_INVALID'
        when document_revision is null then 'DOCUMENT_REVISION_INVALID'
        when purpose <> 'DRAFT_PREVIEW' then 'DOCUMENT_PURPOSE_INVALID'
        when template_version <> 'invoice-professional-v2' then 'DOCUMENT_TEMPLATE_VERSION_INVALID'
        when parent_operation_id is null then 'PARENT_OPERATION_ID_REQUIRED'
        when actor_user_id is null then 'ACTOR_USER_ID_REQUIRED'
        else null end code
    from requests
    where request_key is null
       or invoice_id is null
       or document_revision is null
       or purpose <> 'DRAFT_PREVIEW'
       or template_version <> 'invoice-professional-v2'
       or parent_operation_id is null
       or actor_user_id is null
  ),
  duplicate_requests as materialized (
    select request_key, count(*) count_rows
    from requests
    where request_key is not null
    group by request_key
    having count(*) > 1
  ),
  valid_requests as materialized (
    select r.*
    from requests r
    where not exists (select 1 from invalid_requests i where i.request_no=r.request_no)
      and not exists (select 1 from duplicate_requests d where d.request_key=r.request_key)
  ),
  locked_invoices as materialized (
    select r.*,i.id locked_invoice_id,i.status invoice_status,i.document_revision current_document_revision,
      i.preview_document_version_id,i.issued_document_version_id,
      i.client_id
    from valid_requests r
    join public.invoices i on i.id=r.invoice_id
    order by i.id
    for update of i
  ),
  invoice_not_found as materialized (
    select r.request_no,r.request_key,'INVOICE_NOT_FOUND' code
    from valid_requests r
    where not exists (select 1 from locked_invoices l where l.request_no=r.request_no)
  ),
  source_changed as materialized (
    select request_no,request_key,'SOURCE_CHANGED' code
    from locked_invoices
    where coalesce(current_document_revision,0) <> document_revision
  ),
  invoice_blocked as materialized (
    select request_no,request_key,
      case
        when invoice_status::text in ('ISSUED','PAID') and purpose='DRAFT_PREVIEW' then 'INVOICE_ALREADY_ISSUED'
        when invoice_status::text not in ('DRAFT','ON_HOLD','ISSUED','PAID') then 'INVOICE_STATUS_UNSUPPORTED'
      end code
    from locked_invoices
    where (invoice_status::text in ('ISSUED','PAID') and purpose='DRAFT_PREVIEW')
       or invoice_status::text not in ('DRAFT','ON_HOLD','ISSUED','PAID')
  ),
  actionable as materialized (
    select l.*
    from locked_invoices l
    where not exists (select 1 from source_changed s where s.request_no=l.request_no)
      and not exists (select 1 from invoice_blocked b where b.request_no=l.request_no)
  ),
  ready_versions as materialized (
    select distinct on(a.request_no)
      a.request_no,a.request_key,v.id document_version_id,v.r2_key,v.sha256,v.size_bytes,v.page_count
    from actionable a
    join public.invoice_document_versions v
      on v.entity_type='INVOICE'
     and v.entity_id=a.invoice_id
     and v.purpose=a.purpose
     and v.source_revision=a.document_revision::text
     and v.template_version=a.template_version
     and v.status='READY'
     and v.r2_key is not null
     and v.sha256 ~ '^[0-9a-f]{64}$'
     and coalesce(v.size_bytes,0)>0
     and coalesce(v.page_count,0)>0
    order by a.request_no,v.ready_at_utc desc nulls last,v.created_at_utc desc,v.id desc
  ),
  active_operations as materialized (
    select distinct on(a.request_no)
      a.request_no,a.request_key,o.id operation_id,o.status,o.phase,o.change_seq
    from actionable a
    join public.invoice_operations o
      on o.operation_type='BUILD_DOCUMENT'
     and o.entity_type='INVOICE'
     and o.entity_id=a.invoice_id
     and o.source_revision=a.document_revision::text
     and coalesce(o.input_json->>'purpose','DRAFT_PREVIEW')=a.purpose
     and coalesce(o.template_version,'')=a.template_version
     and o.status in ('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
    where not exists (select 1 from ready_versions rv where rv.request_no=a.request_no)
    order by a.request_no,o.priority desc,o.created_at_utc desc,o.id desc
  ),
  to_create as materialized (
    select a.*,
      encode(digest(concat_ws('|','BUILD_DOCUMENT',a.purpose,a.invoice_id::text,a.document_revision::text,a.template_version,a.request_key),'sha256'),'hex') idempotency_key
    from actionable a
    where not exists (select 1 from ready_versions rv where rv.request_no=a.request_no)
      and not exists (select 1 from active_operations ao where ao.request_no=a.request_no)
  ),
  inserted_operations as materialized (
    insert into public.invoice_operations(
      parent_operation_id,operation_type,entity_type,entity_id,actor_user_id,
      idempotency_key,status,phase,priority,source_revision,template_version,
      input_json,config_json,progress_json,total_units,completed_units,failed_units,chunk_count,
      created_at_utc,updated_at_utc
    )
    select
      parent_operation_id,'BUILD_DOCUMENT','INVOICE',invoice_id,actor_user_id,
      idempotency_key,'QUEUED','BUILD_MANIFEST',priority,document_revision::text,template_version,
      jsonb_build_object(
        'invoice_id',invoice_id,
        'purpose',purpose,
        'expected_revision',document_revision,
        'source_revision',document_revision::text,
        'request_key',request_key,
        'created_by','private._invoice_document_operation_ensure_batch'
      ),
      jsonb_build_object('processor_policy',v_processor_policy),
      jsonb_build_object('status_message','Queued document preparation'),
      1,0,0,1,
      v_now,v_now
    from to_create
    on conflict (idempotency_key)
    where status in ('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
    do update set
      priority=greatest(public.invoice_operations.priority, excluded.priority),
      updated_at_utc=excluded.updated_at_utc
    returning id operation_id,entity_id invoice_id,source_revision,control_version,status,phase,change_seq,idempotency_key
  ),
  inserted_operation_requests as materialized (
    select c.request_no,c.request_key,c.invoice_id,c.document_revision,c.purpose,c.priority,c.template_version,
      o.operation_id,o.control_version,o.status,o.phase,o.change_seq
    from to_create c
    join inserted_operations o
      on o.invoice_id=c.invoice_id and o.source_revision=c.document_revision::text
     and o.idempotency_key=c.idempotency_key
  ),
  inserted_versions as materialized (
    insert into public.invoice_document_versions(
      entity_type,entity_id,purpose,operation_id,source_revision,template_version,
      status,snapshot_json,snapshot_hash,manifest_json,manifest_hash,created_at_utc)
    select 'INVOICE',invoice_id,purpose,operation_id,document_revision::text,
      template_version,'PLANNING','{}'::jsonb,encode(digest('{}','sha256'),'hex'),
      '[]'::jsonb,encode(digest('[]','sha256'),'hex'),v_now
    from inserted_operation_requests
    on conflict(entity_type,entity_id,purpose,source_revision,template_version)
      where purpose in('DRAFT_PREVIEW','TIMESHEET')
        and status in('PLANNING','WAITING_FOR_INPUTS','RENDERING','ASSEMBLING','VERIFYING','READY')
    do nothing
    returning id document_version_id,entity_id invoice_id,operation_id,source_revision,template_version,purpose,status
  ),
  ensured_inserted_versions as materialized (
    select ir.*,v.document_version_id,v.status document_status
    from inserted_operation_requests ir
    join inserted_versions v
      on v.operation_id=ir.operation_id
     and v.invoice_id=ir.invoice_id
     and v.source_revision=ir.document_revision::text
     and v.template_version=ir.template_version
     and v.purpose=ir.purpose
    union all
    select ir.*,v.id document_version_id,v.status document_status
    from inserted_operation_requests ir
    join public.invoice_document_versions v
      on v.entity_type='INVOICE'
     and v.entity_id=ir.invoice_id
     and v.purpose=ir.purpose
     and v.source_revision=ir.document_revision::text
     and v.template_version=ir.template_version
     and v.status in('PLANNING','WAITING_FOR_INPUTS','RENDERING','ASSEMBLING','VERIFYING','READY')
    where not exists (
      select 1
      from inserted_versions inserted
      where inserted.operation_id=ir.operation_id
        and inserted.invoice_id=ir.invoice_id
        and inserted.source_revision=ir.document_revision::text
        and inserted.template_version=ir.template_version
        and inserted.purpose=ir.purpose
    )
  ),
  inserted_chunks as materialized (
    insert into public.invoice_operation_chunks(
      operation_id,chunk_type,phase,work_key,plan_generation,sequence_no,level_no,
      entity_type,entity_id,document_version_id,status,priority,run_after_utc,payload_json,
      operation_control_version,created_at_utc,updated_at_utc
    )
    select
      operation_id,'DOCUMENT_PLAN','BUILD_MANIFEST',
      encode(digest(concat_ws('|','DOCUMENT_PLAN',document_version_id::text,document_revision::text,purpose,template_version,'1'),'sha256'),'hex'),
      1,0,0,
      'INVOICE',invoice_id,document_version_id,'QUEUED',priority,v_now,
      jsonb_build_object(
        'invoice_id',invoice_id,
        'purpose',purpose,
        'expected_revision',document_revision,
        'source_revision',document_revision::text,
        'template_version',template_version,
        'request_key',request_key
      ),
      control_version,v_now,v_now
    from ensured_inserted_versions
    where document_status <> 'READY'
    on conflict(operation_id,chunk_type,level_no,sequence_no,work_key) do update set
      priority=greatest(public.invoice_operation_chunks.priority, excluded.priority),
      updated_at_utc=excluded.updated_at_utc
    returning id chunk_id,operation_id,document_version_id
  ),
  outcomes as materialized (
    select request_no,request_key,false ok,'INVALID' status,code,null::uuid operation_id,null::uuid document_version_id,null::uuid chunk_id
    from invalid_requests
    union all
    select r.request_no,r.request_key,false,'INVALID','REQUEST_KEY_DUPLICATE',null::uuid,null::uuid,null::uuid
    from requests r join duplicate_requests d on d.request_key=r.request_key
    union all
    select request_no,request_key,false,'BLOCKED',code,null::uuid,null::uuid,null::uuid
    from invoice_not_found
    union all
    select request_no,request_key,false,'BLOCKED',code,null::uuid,null::uuid,null::uuid
    from source_changed
    union all
    select request_no,request_key,false,'BLOCKED',code,null::uuid,null::uuid,null::uuid
    from invoice_blocked
    union all
    select request_no,request_key,true,'READY',null,null::uuid,document_version_id,null::uuid
    from ready_versions
    union all
    select request_no,request_key,true,status,null,operation_id,null::uuid,null::uuid
    from active_operations
    union all
    select ev.request_no,ev.request_key,true,ev.status,null,ev.operation_id,ev.document_version_id,ic.chunk_id
    from ensured_inserted_versions ev
    left join inserted_chunks ic on ic.operation_id=ev.operation_id and ic.document_version_id=ev.document_version_id
  )
  select jsonb_build_object(
    'contract_version','INVOICE_DOCUMENT_OPERATION_ENSURE_V1',
    'ok',not exists(select 1 from outcomes where ok is false),
    'results',coalesce(jsonb_agg(jsonb_build_object(
      'request_no',request_no,
      'request_key',request_key,
      'ok',ok,
      'status',status,
      'code',code,
      'operation_id',operation_id,
      'document_version_id',document_version_id,
      'chunk_id',chunk_id
    ) order by request_no),'[]'::jsonb),
    'created_count',(select count(*) from inserted_operation_requests),
    'ready_count',(select count(*) from ready_versions),
    'active_count',(select count(*) from active_operations),
    'blocked_count',(select count(*) from outcomes where ok is false),
    'checked_at_utc',v_now
  )
  into v_result
  from outcomes;

  return coalesce(v_result,jsonb_build_object(
    'contract_version','INVOICE_DOCUMENT_OPERATION_ENSURE_V1',
    'ok',false,
    'results','[]'::jsonb,
    'code','NO_OUTCOMES'
  ));
end;
$function$;

revoke all on function private._invoice_document_operation_ensure_batch(jsonb,timestamptz) from public, anon, authenticated;
grant execute on function private._invoice_document_operation_ensure_batch(jsonb,timestamptz) to service_role;
