create or replace function private._invoice_delivery_advance_batch(
  p_claims jsonb,
  p_now_utc timestamptz
) returns jsonb
language plpgsql
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_now timestamptz:=coalesce(p_now_utc,now());
  v_result jsonb;
begin
  /*
   * The legal issue snapshot remains frozen. Delivery re-resolves the original
   * routing request once, set-wise, and sends only when the stable V4 policy
   * identity still matches the frozen issue-time route.
   */
  with recursive
  ids as materialized (
    select distinct (x->>'chunk_id')::uuid chunk_id
    from jsonb_array_elements(coalesce(p_claims,'[]'::jsonb)) x
    where x->>'phase' in('PREPARE','QUEUE_DELIVERY')
      and coalesce(x->>'chunk_id','') ~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
  ),
  requested as materialized (
    select c.id chunk_id,c.operation_id,c.entity_id invoice_id,
      c.payload_json,o.actor_user_id,o.config_json->'processor_policy'
        processor_policy,
      nullif(btrim(c.payload_json->>'request_key'),'') request_key,
      i.id found_invoice_id,i.client_id,i.invoice_no,i.status invoice_status,
      i.issued_document_version_id,
      case when coalesce(
          c.payload_json->>'issued_document_version_id','')~*
          '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
        then(c.payload_json->>'issued_document_version_id')::uuid
        else c.document_version_id end requested_document_version_id,
      case when jsonb_typeof(
          c.payload_json->'frozen_delivery_route')='object'
        then c.payload_json->'frozen_delivery_route'
        else '{}'::jsonb end frozen_route,
      case when jsonb_typeof(c.payload_json->'routing_request')='object'
        then c.payload_json->'routing_request'
        else '{}'::jsonb end routing_request,
      nullif(btrim(c.payload_json->>'delivery_request_token'),'')
        delivery_request_token,
      v.id document_version_id,v.entity_type document_entity_type,
      v.entity_id document_entity_id,v.purpose document_purpose,
      v.status document_status,v.r2_key,v.sha256,v.size_bytes,v.page_count
    from ids x
    join public.invoice_operation_chunks c on c.id=x.chunk_id
    join public.invoice_operations o on o.id=c.operation_id
    left join public.invoices i on i.id=c.entity_id
    left join public.invoice_document_versions v
      on v.id=case when coalesce(
          c.payload_json->>'issued_document_version_id','')~*
          '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
        then(c.payload_json->>'issued_document_version_id')::uuid
        else c.document_version_id end
  ),
  current_route_input as materialized (
    select coalesce(jsonb_agg(jsonb_build_object(
      'request_key',r.chunk_id::text,
      'invoice_id',r.invoice_id,
      'recipient_set',case when jsonb_typeof(
          r.routing_request->'recipient_set')='array'
        then r.routing_request->'recipient_set' else '[]'::jsonb end,
      'cc',case when jsonb_typeof(r.routing_request->'cc')='array'
        then r.routing_request->'cc' else '[]'::jsonb end,
      'bcc',case when jsonb_typeof(r.routing_request->'bcc')='array'
        then r.routing_request->'bcc' else '[]'::jsonb end,
      'delivery_policy',coalesce(
        nullif(r.routing_request->>'delivery_policy',''),
        nullif(r.frozen_route->>'delivery_policy',''),'ATTACH'),
      'template_version',coalesce(
        nullif(r.routing_request->>'template_version',''),
        nullif(r.frozen_route->>'template_version',''),
        'invoice-delivery-v1'))
      order by r.chunk_id),'[]'::jsonb) requests
    from requested r
  ),
  current_routes as materialized (
    select route.*
    from current_route_input input
    cross join lateral private._invoice_delivery_routes_batch(
      input.requests,(v_now at time zone 'Europe/London')::date) route
  ),
  frozen as materialized (
    select r.*,
      route.canonical_to,route.canonical_cc,route.canonical_bcc,
      route.recipient_set_hash,route.route_policy_hash,
      route.route_source,
      coalesce(nullif(r.routing_request->>'template_version',''),
        nullif(r.frozen_route->>'template_version',''))
        template_version,
      upper(coalesce(nullif(r.routing_request->>'delivery_policy',''),
        r.frozen_route->>'delivery_policy',''))
        requested_delivery_policy,
      route.invoice_group_identity,
      route.suppression_reason,route.do_not_send,route.self_bill,
      route.delivery_suppressed,
      coalesce(route.warning_codes,'[]'::jsonb) warnings,
      coalesce(route.blocker_codes,'[]'::jsonb) route_blockers,
      nullif(r.frozen_route->>'route_policy_hash','')
        frozen_route_policy_hash,
      nullif(r.frozen_route->>'recipient_set_hash','')
        frozen_recipient_set_hash,
      route.route_policy_hash is distinct from
        nullif(r.frozen_route->>'route_policy_hash','') route_changed,
      case when coalesce(r.processor_policy#>>
          '{delivery,max_attachments_per_message}','')~'^[1-9][0-9]{0,3}$'
        then(r.processor_policy#>>
          '{delivery,max_attachments_per_message}')::integer end
        max_attachments_per_message,
      case when coalesce(r.processor_policy#>>
          '{delivery,max_cumulative_attachment_bytes}','')
          ~'^[1-9][0-9]{0,17}$'
        then(r.processor_policy#>>
          '{delivery,max_cumulative_attachment_bytes}')::bigint end
        max_cumulative_attachment_bytes,
      case when coalesce(r.processor_policy#>>
          '{delivery,max_individual_attachment_bytes}','')
          ~'^[1-9][0-9]{0,17}$'
        then(r.processor_policy#>>
          '{delivery,max_individual_attachment_bytes}')::bigint end
        max_individual_attachment_bytes,
      case when coalesce(r.processor_policy#>>
          '{delivery,secure_link_threshold_bytes}','')
          ~'^[1-9][0-9]{0,17}$'
        then(r.processor_policy#>>
          '{delivery,secure_link_threshold_bytes}')::bigint end
        secure_link_threshold_bytes
    from requested r
    left join current_routes route
      on route.request_key=r.chunk_id::text
     and route.invoice_id=r.invoice_id
  ),
  classified as materialized (
    select f.*,
      case
        when f.processor_policy->>'version'<>'INVOICE_PROCESSOR_LIMITS_V3'
          or f.max_attachments_per_message is null
          or f.max_cumulative_attachment_bytes is null
          or f.max_individual_attachment_bytes is null
          or f.secure_link_threshold_bytes is null
          or jsonb_typeof(f.processor_policy#>
            '{delivery,allowed_policies}')<>'array'
          then 'PROCESSOR_POLICY_INVALID'
        when f.request_key is distinct from f.chunk_id::text
          then 'REQUEST_CORRELATION_INVALID'
        when f.found_invoice_id is null then 'INVOICE_NOT_FOUND'
        when f.invoice_status not in('ISSUED','PAID')
          then 'INVOICE_NOT_ISSUED'
        when f.requested_document_version_id is null
          then 'ISSUED_DOCUMENT_MISSING'
        when f.issued_document_version_id is distinct from
          f.requested_document_version_id
          then 'ISSUED_DOCUMENT_POINTER_MISMATCH'
        when f.document_version_id is null then 'ISSUED_DOCUMENT_NOT_FOUND'
        when f.document_entity_type<>'INVOICE'
          or f.document_entity_id<>f.invoice_id
          then 'ISSUED_DOCUMENT_ENTITY_MISMATCH'
        when f.document_purpose<>'FINAL_ISSUE'
          then 'ISSUED_DOCUMENT_PURPOSE_MISMATCH'
        when f.document_status<>'READY' or f.r2_key is null
          or f.sha256!~'^[0-9a-f]{64}$'
          or coalesce(f.size_bytes,0)<=0 or coalesce(f.page_count,0)<=0
          then 'ISSUED_DOCUMENT_NOT_READY'
        when f.delivery_request_token is null
          then 'DELIVERY_REQUEST_TOKEN_MISSING'
        when nullif(f.frozen_route->>'route_policy_hash','')
            !~'^[0-9a-f]{64}$'
          or nullif(f.frozen_route->>'recipient_set_hash','')
            !~'^[0-9a-f]{64}$'
          or nullif(f.frozen_route->>'template_version','') is null
          then 'FROZEN_DELIVERY_ROUTE_MISSING'
        when f.route_policy_hash is null
          then 'CURRENT_DELIVERY_ROUTE_MISSING'
        when jsonb_typeof(f.canonical_to)<>'array'
          or jsonb_typeof(f.canonical_cc)<>'array'
          or jsonb_typeof(f.canonical_bcc)<>'array'
          or f.recipient_set_hash!~'^[0-9a-f]{64}$'
          or f.route_policy_hash!~'^[0-9a-f]{64}$'
          or f.template_version is null
          or f.invoice_group_identity is null
          then 'CURRENT_DELIVERY_ROUTE_INVALID'
        when f.requested_delivery_policy not in(
          'ATTACH','SPLIT','SECURE_LINK')
          or not(f.processor_policy#>'{delivery,allowed_policies}'
            @> jsonb_build_array(f.requested_delivery_policy))
          then 'PROCESSOR_POLICY_INVALID'
        when jsonb_typeof(f.route_blockers)<>'array'
          then 'FROZEN_DELIVERY_ROUTE_INVALID'
        when jsonb_array_length(f.route_blockers)>0
          then coalesce(f.route_blockers->>0,'DELIVERY_ROUTE_BLOCKED')
        when f.route_changed then 'DELIVERY_ROUTE_CHANGED'
        when not coalesce(f.delivery_suppressed,false)
          and jsonb_array_length(f.canonical_to)=0
          then 'RECIPIENT_MISSING'
      end blocker_code
    from frozen f
  ),
  deliverable as materialized (
    select c.*,
      case
        when c.requested_delivery_policy='SECURE_LINK'
          or c.size_bytes>c.max_individual_attachment_bytes
          or c.size_bytes>c.max_cumulative_attachment_bytes
          or c.size_bytes>=c.secure_link_threshold_bytes
          then 'SECURE_LINK' else 'ATTACHMENT'
      end descriptor_mode,
      (select string_agg(v.value,',' order by v.value)
        from jsonb_array_elements_text(c.canonical_to) v(value)) recipient_key,
      (select string_agg(v.value,',' order by v.value)
        from jsonb_array_elements_text(c.canonical_cc) v(value)) cc_key,
      (select string_agg(v.value,',' order by v.value)
        from jsonb_array_elements_text(c.canonical_bcc) v(value)) bcc_key
    from classified c
    where c.blocker_code is null and not c.delivery_suppressed
  ),
  compatibility as materialized (
    select d.*,
      encode(digest(jsonb_build_object(
        'delivery_root',d.operation_id,
        'client_id',d.client_id,
        'invoice_group_identity',d.invoice_group_identity,
        'to',d.canonical_to,'cc',d.canonical_cc,'bcc',d.canonical_bcc,
        'route_policy_hash',d.route_policy_hash,
        'template_version',d.template_version,
        'delivery_mode',d.descriptor_mode)::text,'sha256'),'hex')
        compatibility_key
    from deliverable d
  ),
  member_identity as materialized (
    select c.*,
      encode(digest(string_agg(concat_ws('|',
        c.invoice_id::text,c.document_version_id::text,
        c.delivery_request_token),'||')
        over(partition by c.compatibility_key
          order by c.invoice_no nulls last,c.invoice_id
          rows between unbounded preceding and unbounded following),
        'sha256'),'hex')
        ordered_member_token_hash,
      row_number() over(partition by c.compatibility_key
        order by c.invoice_no nulls last,c.invoice_id)::integer input_no
    from compatibility c
  ),
  delivery_pack(
    compatibility_key,input_no,part_no,part_count,direct_bytes
  ) as (
    select n.compatibility_key,n.input_no,1,1,
      case when n.descriptor_mode='ATTACHMENT'
        then n.size_bytes else 0 end
    from member_identity n where n.input_no=1
    union all
    select n.compatibility_key,n.input_no,
      p.part_no+case when split.start_new then 1 else 0 end,
      case when split.start_new then 1 else p.part_count+1 end,
      case when split.start_new
        then case when n.descriptor_mode='ATTACHMENT'
          then n.size_bytes else 0 end
        else p.direct_bytes+case when n.descriptor_mode='ATTACHMENT'
          then n.size_bytes else 0 end end
    from delivery_pack p
    join member_identity n
      on n.compatibility_key=p.compatibility_key
     and n.input_no=p.input_no+1
    cross join lateral(
      select p.part_count+1>n.max_attachments_per_message
        or(n.descriptor_mode='ATTACHMENT'
          and p.direct_bytes+n.size_bytes>
            n.max_cumulative_attachment_bytes) start_new
    ) split
  ),
  assigned as materialized (
    select n.*,p.part_no
    from member_identity n
    join delivery_pack p
      on p.compatibility_key=n.compatibility_key
     and p.input_no=n.input_no
  ),
  part_totals as materialized (
    select compatibility_key,max(part_no)::integer part_total
    from assigned group by compatibility_key
  ),
  message_groups as materialized (
    select a.operation_id,a.client_id,a.invoice_group_identity,
      a.recipient_key,a.cc_key,a.bcc_key,a.recipient_set_hash,
      a.route_policy_hash,a.template_version,a.descriptor_mode,
      a.ordered_member_token_hash,a.part_no,max(pt.part_total) part_total,
      min(a.actor_user_id::text)::uuid actor_user_id,
      min(a.invoice_id::text)::uuid first_invoice_id,
      array_agg(a.chunk_id order by a.input_no) chunk_ids,
      array_agg(a.invoice_id order by a.input_no) invoice_ids,
      array_agg(a.document_version_id order by a.input_no)
        document_version_ids,
      jsonb_agg(
        case when a.descriptor_mode='SECURE_LINK' then
          jsonb_build_object(
            'invoice_id',a.invoice_id,
            'document_version_id',a.document_version_id,
            'filename','Invoice_'||coalesce(nullif(regexp_replace(
              a.invoice_no,'[^A-Za-z0-9_-]+','','g'),''),
              a.invoice_id::text)||'.pdf',
            'sha256',a.sha256,'size_bytes',a.size_bytes,
            'page_count',a.page_count,'delivery_mode','SECURE_LINK',
            'secure_link_required',true)
        else jsonb_build_object(
            'invoice_id',a.invoice_id,
            'document_version_id',a.document_version_id,
            'filename','Invoice_'||coalesce(nullif(regexp_replace(
              a.invoice_no,'[^A-Za-z0-9_-]+','','g'),''),
              a.invoice_id::text)||'.pdf',
            'r2_key',a.r2_key,'sha256',a.sha256,
            'mime_type','application/pdf','size_bytes',a.size_bytes,
            'page_count',a.page_count,'delivery_mode','ATTACHMENT')
        end order by a.input_no) attachments,
      sum(case when a.descriptor_mode='ATTACHMENT'
        then a.size_bytes else 0 end)::bigint attachment_total_bytes
    from assigned a
    join part_totals pt using(compatibility_key)
    group by a.operation_id,a.client_id,a.invoice_group_identity,
      a.recipient_key,a.cc_key,a.bcc_key,a.recipient_set_hash,
      a.route_policy_hash,a.template_version,a.descriptor_mode,
      a.ordered_member_token_hash,a.part_no
  ),
  planned_mail as materialized (
    select g.*,
      'INVOICE_DELIVERY_V1:'||encode(digest(concat_ws('|',
        g.operation_id::text,g.ordered_member_token_hash,
        g.route_policy_hash,g.template_version,g.descriptor_mode,
        g.part_no::text),'sha256'),'hex') reference_key,
      encode(digest(concat_ws('|','INVOICE_DELIVERY',
        g.operation_id::text,g.ordered_member_token_hash,
        g.route_policy_hash,g.template_version,g.descriptor_mode,
        g.part_no::text),'sha256'),'hex') deterministic_key
    from message_groups g
  ),
  inserted_mail as (
    insert into public.mail_outbox(
      id,type,"to",cc,bcc,importance,email_type,subject,body_html,
      body_text,attachments,status,created_at_utc,created_by,reference,
      recipient_kind,recipient_id,context_kind,context_id,
      attachments_ready,waiting_invoice_operation_id,
      attachment_total_bytes,attachment_delivery_policy,
      deterministic_outbox_key
    )
    select gen_random_uuid(),'INVOICE',p.recipient_key,p.cc_key,p.bcc_key,
      'Normal','plain',
      'Invoices – Week ending '||
        case when p.invoice_group_identity='NO_WEEK'
          then '' else p.invoice_group_identity end,
      '<p>Please find the attached invoices.</p>',
      'Please find the attached invoices.',
      p.attachments,'QUEUED',v_now,p.actor_user_id,p.reference_key,
      'client',p.client_id,'invoices',
      case when cardinality(p.invoice_ids)=1
        then p.first_invoice_id end,
      true,null,p.attachment_total_bytes,
      case when p.descriptor_mode='SECURE_LINK' then 'SECURE_LINK'
        when p.part_total>1 then 'SPLIT' else 'ATTACH' end,
      p.deterministic_key
    from planned_mail p
    on conflict(reference)
      where reference like 'INVOICE_DELIVERY_V1:%' do nothing
    returning id,reference
  ),
  selected_mail as materialized (
    select m.id,m.reference,p.chunk_ids,p.invoice_ids,
      p.document_version_ids,p.ordered_member_token_hash
    from planned_mail p
    join inserted_mail m on m.reference=p.reference_key
    union all
    select m.id,m.reference,p.chunk_ids,p.invoice_ids,
      p.document_version_ids,p.ordered_member_token_hash
    from planned_mail p
    join public.mail_outbox m on m.reference=p.reference_key
    where not exists(select 1 from inserted_mail im
      where im.reference=p.reference_key)
  ),
  invalid_updates as (
    update public.invoice_operation_chunks c
    set status=case when k.blocker_code in(
          'INVOICE_NOT_FOUND','INVOICE_NOT_ISSUED',
          'ISSUED_DOCUMENT_MISSING','ISSUED_DOCUMENT_POINTER_MISMATCH',
          'ISSUED_DOCUMENT_NOT_FOUND','ISSUED_DOCUMENT_ENTITY_MISMATCH',
          'ISSUED_DOCUMENT_PURPOSE_MISMATCH','ISSUED_DOCUMENT_NOT_READY')
        then 'FAILED' else 'BLOCKED' end,
      phase=case when k.blocker_code in(
          'INVOICE_NOT_FOUND','INVOICE_NOT_ISSUED',
          'ISSUED_DOCUMENT_MISSING','ISSUED_DOCUMENT_POINTER_MISMATCH',
          'ISSUED_DOCUMENT_NOT_FOUND','ISSUED_DOCUMENT_ENTITY_MISMATCH',
          'ISSUED_DOCUMENT_PURPOSE_MISMATCH','ISSUED_DOCUMENT_NOT_READY')
        then 'FAILED' else 'BLOCKED' end,
      error_json=jsonb_build_object('code',k.blocker_code,
        'invoice_id',k.invoice_id,
        'document_version_id',k.requested_document_version_id,
        'frozen_route',jsonb_build_object(
          'route_policy_hash',k.frozen_route_policy_hash,
          'recipient_set_hash',k.frozen_recipient_set_hash,
          'to',k.frozen_route->'to','cc',k.frozen_route->'cc',
          'bcc',k.frozen_route->'bcc'),
        'current_route',jsonb_build_object(
          'route_policy_hash',k.route_policy_hash,
          'recipient_set_hash',k.recipient_set_hash,
          'to',k.canonical_to,'cc',k.canonical_cc,'bcc',k.canonical_bcc,
          'delivery_suppressed',k.delivery_suppressed,
          'suppression_reason',k.suppression_reason),
        'warnings',k.warnings),
      failed_at_utc=case when k.blocker_code in(
          'INVOICE_NOT_FOUND','INVOICE_NOT_ISSUED',
          'ISSUED_DOCUMENT_MISSING','ISSUED_DOCUMENT_POINTER_MISMATCH',
          'ISSUED_DOCUMENT_NOT_FOUND','ISSUED_DOCUMENT_ENTITY_MISMATCH',
          'ISSUED_DOCUMENT_PURPOSE_MISMATCH','ISSUED_DOCUMENT_NOT_READY')
        then v_now end,
      completed_at_utc=null,updated_at_utc=v_now,
      lease_owner=null,lease_token=null,lease_expires_at_utc=null
    from classified k
    where c.id=k.chunk_id and k.blocker_code is not null
    returning c.id,c.status,c.phase,c.result_json,c.error_json
  ),
  completed_updates as (
    update public.invoice_operation_chunks c
    set status='COMPLETE',phase='COMPLETE',completed_at_utc=v_now,
      failed_at_utc=null,error_json=null,updated_at_utc=v_now,
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,
      result_json=jsonb_build_object(
        'invoice_id',k.invoice_id,
        'document_version_id',k.document_version_id,
        'delivery_request_token',k.delivery_request_token,
        'route_policy_hash',k.route_policy_hash,
        'recipient_set_hash',k.recipient_set_hash,
        'mail_outbox_ids',coalesce((
          select jsonb_agg(distinct sm.id order by sm.id)
          from selected_mail sm where k.chunk_id=any(sm.chunk_ids)
        ),'[]'::jsonb),
        'delivery_skipped',k.delivery_suppressed,
        'skip_reason',coalesce(k.suppression_reason,
          case when k.do_not_send then 'DO_NOT_SEND' end),
        'warnings',k.warnings)
    from classified k
    where c.id=k.chunk_id and k.blocker_code is null
    returning c.id,c.status,c.phase,c.result_json,c.error_json
  ),
  results as (
    select * from invalid_updates
    union all
    select * from completed_updates
  )
  select coalesce(jsonb_agg(jsonb_build_object(
    'chunk_id',id,'status',status,'phase',phase,
    'result',result_json,'error',error_json) order by id),'[]'::jsonb)
  into v_result from results;

  return coalesce(v_result,'[]'::jsonb);
end;
$function$;

revoke all on function private._invoice_delivery_advance_batch(jsonb,timestamptz)
  from public,anon,authenticated;
grant execute on function private._invoice_delivery_advance_batch(jsonb,timestamptz)
  to service_role;
