drop function if exists public.invoice_operation_start_batch(jsonb,uuid,timestamptz);
create function public.invoice_operation_start_batch(
  p_commands jsonb,
  p_actor_user_id uuid,
  p_now_utc timestamptz default now()
) returns jsonb
language plpgsql
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_now timestamptz:=coalesce(p_now_utc,now());
  v_jwt_role text:=coalesce(
    nullif(current_setting('request.jwt.claim.role',true),''),
    auth.jwt()->>'role','');
  v_auth_user uuid:=auth.uid();
  v_result jsonb;
begin
  if jsonb_typeof(p_commands)<>'array'
     or jsonb_array_length(p_commands)<1
     or jsonb_array_length(p_commands)>1000 then
    raise exception using errcode='22023',
      message='p_commands must be a JSON array containing 1..1000 commands';
  end if;

  if p_actor_user_id is null
     or not exists(select 1 from public.tms_users u
       where u.id=p_actor_user_id and u.is_active and lower(u.role)='admin')
     or(v_jwt_role<>'service_role' and v_auth_user is distinct from p_actor_user_id) then
    raise exception using errcode='42501',
      message='Active administrator actor and matching authenticated/service caller required';
  end if;

  with raw as materialized (
    select e.ordinality::integer command_no,e.value command_json,
      upper(btrim(coalesce(e.value->>'command_type',e.value->>'type',''))) command_type
    from jsonb_array_elements(p_commands) with ordinality e(value,ordinality)
  ),
  command_ids as materialized (
    select r.command_no,r.command_type,x.ordinality::integer item_no,x.value id_text,
      x.value ~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        valid_uuid
    from raw r
    cross join lateral jsonb_array_elements_text(
      case
        when r.command_type='GENERATE_NHSP'
          and jsonb_typeof(r.command_json->'nhsp_shift_ids')='array'
          then r.command_json->'nhsp_shift_ids'
        when r.command_type in(
          'GENERATE_SELECTED','GENERATE_BY_WEEK','GENERATE_HOURS',
          'GENERATE_EXPENSES','GENERATE_NHSP','GENERATE_AUTO')
          and jsonb_typeof(r.command_json->'source_ids')='array'
          then r.command_json->'source_ids'
        when r.command_type in(
          'GENERATE_SELECTED','GENERATE_BY_WEEK','GENERATE_HOURS',
          'GENERATE_EXPENSES','GENERATE_NHSP','GENERATE_AUTO')
          and jsonb_typeof(r.command_json->'timesheet_ids')='array'
          then r.command_json->'timesheet_ids'
        when r.command_type in('GENERATE_CREDIT_NOTE','ISSUE_INVOICES','DELIVER_INVOICES')
          and jsonb_typeof(r.command_json->'invoice_ids')='array'
          then r.command_json->'invoice_ids'
        when r.command_type='GENERATE_CREDIT_NOTE'
          and coalesce(r.command_json->>'base_invoice_id','')<>'' then
          jsonb_build_array(r.command_json->>'base_invoice_id')
        when r.command_type in(
          'GENERATE_CREDIT_NOTE','VIEW_INVOICE_DOCUMENT','DELIVER_INVOICES')
          and coalesce(r.command_json->>'invoice_id','')<>'' then
          jsonb_build_array(r.command_json->>'invoice_id')
        when r.command_type='VIEW_TIMESHEET_DOCUMENT'
          and coalesce(r.command_json->>'timesheet_id','')<>'' then
          jsonb_build_array(r.command_json->>'timesheet_id')
        else '[]'::jsonb
      end
    ) with ordinality x(value,ordinality)
  ),
  canonical_ids as materialized (
    select command_no,array_agg(distinct id_text::uuid order by id_text::uuid) ids
    from command_ids where valid_uuid group by command_no
  ),
  generation_groups as materialized (
    select *
    from private._invoice_generation_resolve_command_groups(
      p_commands,p_actor_user_id,v_now)
  ),
  view_facts as materialized (
    select r.command_no,r.command_type,
      case when r.command_type='VIEW_INVOICE_DOCUMENT' then 'INVOICE' else 'TIMESHEET' end entity_type,
      (array_agg(ci.id_text::uuid order by ci.item_no))[1] entity_id,
      case when r.command_type='VIEW_INVOICE_DOCUMENT'
        then i.document_revision else t.document_revision end document_revision,
      case when r.command_type='VIEW_INVOICE_DOCUMENT'
        then 'DRAFT_PREVIEW' else 'TIMESHEET' end purpose,
      coalesce(nullif(r.command_json->>'template_version',''),
        case when r.command_type='VIEW_INVOICE_DOCUMENT'
          then 'invoice-professional-v1' else 'timesheet-professional-v1' end) template_version
    from raw r
    left join command_ids ci on ci.command_no=r.command_no and ci.valid_uuid
    left join public.invoices i
      on r.command_type='VIEW_INVOICE_DOCUMENT' and i.id=ci.id_text::uuid
    left join public.timesheets t
      on r.command_type='VIEW_TIMESHEET_DOCUMENT' and t.timesheet_id=ci.id_text::uuid
    where r.command_type in('VIEW_INVOICE_DOCUMENT','VIEW_TIMESHEET_DOCUMENT')
    group by r.command_no,r.command_type,i.document_revision,t.document_revision,r.command_json
  ),
  issue_facts as materialized (
    select r.command_no,
      array_agg(i.id order by i.id) invoice_ids,
      encode(digest(string_agg(i.id||':'||i.document_revision,
        '|' order by i.id),'sha256'),'hex') invoice_revision_hash,
      count(*)::integer resolved_count
    from raw r join canonical_ids ci on ci.command_no=r.command_no
    cross join unnest(ci.ids) invoice_id
    join public.invoices i on i.id=invoice_id
    where r.command_type='ISSUE_INVOICES'
    group by r.command_no
  ),
  delivery_facts as materialized (
    select r.command_no,
      array_agg(i.id order by i.id) invoice_ids,
      array_agg(i.issued_document_version_id order by i.id) document_version_ids,
      encode(digest(string_agg(concat_ws(':',i.id::text,
        i.issued_document_version_id::text,v.sha256,v.size_bytes::text,v.page_count::text),
        '|' order by i.id),'sha256'),'hex') delivery_revision,
      count(*)::integer resolved_count
    from raw r join canonical_ids ci on ci.command_no=r.command_no
    cross join unnest(ci.ids) invoice_id
    join public.invoices i on i.id=invoice_id
    join public.invoice_document_versions v
      on v.id=i.issued_document_version_id and v.status='READY'
      and v.purpose='FINAL_ISSUE' and v.entity_type='INVOICE' and v.entity_id=i.id
    where r.command_type='DELIVER_INVOICES' and i.status in('ISSUED','PAID')
    group by r.command_no
  ),
  recipient_sets as materialized (
    select r.command_no,
      coalesce((select jsonb_agg(email order by email)
        from(
          select distinct lower(btrim(e.value)) email
          from jsonb_array_elements_text(
            case when jsonb_typeof(r.command_json->'recipient_set')='array'
              then r.command_json->'recipient_set' else '[]'::jsonb end) e(value)
          where nullif(btrim(e.value),'') is not null
            and btrim(e.value)~*'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$'
        ) q),'[]'::jsonb) canonical_recipients,
      coalesce((select jsonb_agg(email order by email)
        from(
          select distinct lower(btrim(e.value)) email
          from jsonb_array_elements_text(
            case when jsonb_typeof(r.command_json->'cc')='array'
              then r.command_json->'cc' else '[]'::jsonb end) e(value)
          where nullif(btrim(e.value),'') is not null
            and btrim(e.value)~*'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$'
        ) q),'[]'::jsonb) canonical_cc,
      coalesce((select jsonb_agg(email order by email)
        from(
          select distinct lower(btrim(e.value)) email
          from jsonb_array_elements_text(
            case when jsonb_typeof(r.command_json->'bcc')='array'
              then r.command_json->'bcc' else '[]'::jsonb end) e(value)
          where nullif(btrim(e.value),'') is not null
            and btrim(e.value)~*'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$'
        ) q),'[]'::jsonb) canonical_bcc,
      (select count(*)::integer
        from(
          select value from jsonb_array_elements_text(
            case when jsonb_typeof(r.command_json->'recipient_set')='array'
              then r.command_json->'recipient_set' else '[]'::jsonb end)
          union all
          select value from jsonb_array_elements_text(
            case when jsonb_typeof(r.command_json->'cc')='array'
              then r.command_json->'cc' else '[]'::jsonb end)
          union all
          select value from jsonb_array_elements_text(
            case when jsonb_typeof(r.command_json->'bcc')='array'
              then r.command_json->'bcc' else '[]'::jsonb end)
        ) e
        where nullif(btrim(e.value),'') is not null
          and btrim(e.value)!~*'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$')
        invalid_count
    from raw r
    where r.command_type='DELIVER_INVOICES'
  ),
  delivery_route_requests as materialized (
    select r.command_no,i.invoice_id,jsonb_build_object(
      'request_key',r.command_no::text||':'||i.invoice_id::text,
      'invoice_id',i.invoice_id,
      'recipient_set',rs.canonical_recipients,
      'cc',rs.canonical_cc,
      'bcc',rs.canonical_bcc,
      'delivery_policy',upper(coalesce(
        nullif(r.command_json->>'delivery_policy',''),'ATTACH')),
      'template_version',coalesce(
        nullif(r.command_json->>'delivery_template_version',''),
        'invoice-delivery-v1')) request_json
    from raw r
    join delivery_facts f on f.command_no=r.command_no
    join recipient_sets rs on rs.command_no=r.command_no
    cross join unnest(f.invoice_ids) i(invoice_id)
    where r.command_type='DELIVER_INVOICES'
  ),
  delivery_routes as materialized (
    select q.command_no,d.*
    from private._invoice_delivery_routes_batch(coalesce((
      select jsonb_agg(q.request_json order by q.command_no,q.invoice_id)
      from delivery_route_requests q),'[]'::jsonb),
      (v_now at time zone 'Europe/London')::date) d
    join delivery_route_requests q
      on q.request_json->>'request_key'=d.request_key
      and q.invoice_id=d.invoice_id
  ),
  delivery_route_groups as materialized (
    select command_no,
      encode(digest(string_agg(invoice_id::text||':'||route_policy_hash,
        '|' order by invoice_id),'sha256'),'hex') route_set_hash,
      jsonb_agg(jsonb_build_object(
        'invoice_id',invoice_id,'to',canonical_to,'cc',canonical_cc,
        'bcc',canonical_bcc,'recipient_set_hash',recipient_set_hash,
        'route_policy_hash',route_policy_hash,
        'route_source',route_source,'do_not_send',do_not_send,
        'warning_codes',warning_codes,'blocker_codes',blocker_codes,
        'evaluated_date',(v_now at time zone 'Europe/London')::date)
        order by invoice_id) frozen_routes
    from delivery_routes
    group by command_no
  ),
  issue_delivery_intents as materialized (
    select r.command_no,
      (intent.intent_json-'recipient_set'-'cc'-'bcc')
        ||jsonb_build_object(
          'recipient_set',recipients.canonical,
          'cc',cc.canonical,'bcc',bcc.canonical) canonical_intent,
      recipients.requested_count+cc.requested_count+bcc.requested_count requested_count,
      recipients.valid_count+cc.valid_count+bcc.valid_count valid_count
    from raw r
    cross join lateral(
      select case when jsonb_typeof(r.command_json->'delivery_intent')='object'
        then r.command_json->'delivery_intent' else '{}'::jsonb end intent_json
    ) intent
    cross join lateral(
      select coalesce((select jsonb_agg(email order by email)
          from(
            select distinct lower(btrim(e.value)) email
            from jsonb_array_elements_text(
              case when jsonb_typeof(intent.intent_json->'recipient_set')='array'
                then intent.intent_json->'recipient_set' else '[]'::jsonb end) e(value)
            where nullif(btrim(e.value),'') is not null
              and btrim(e.value)~*'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$'
          ) valid),'[]'::jsonb) canonical,
        (select count(*)::integer
          from jsonb_array_elements_text(
            case when jsonb_typeof(intent.intent_json->'recipient_set')='array'
              then intent.intent_json->'recipient_set' else '[]'::jsonb end) e(value)
          where nullif(btrim(e.value),'') is not null) requested_count,
        (select count(*)::integer
          from jsonb_array_elements_text(
            case when jsonb_typeof(intent.intent_json->'recipient_set')='array'
              then intent.intent_json->'recipient_set' else '[]'::jsonb end) e(value)
          where nullif(btrim(e.value),'') is not null
            and btrim(e.value)~*'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$')
          valid_count
    ) recipients
    cross join lateral(
      select coalesce((select jsonb_agg(email order by email)
          from(
            select distinct lower(btrim(e.value)) email
            from jsonb_array_elements_text(
              case when jsonb_typeof(intent.intent_json->'cc')='array'
                then intent.intent_json->'cc' else '[]'::jsonb end) e(value)
            where nullif(btrim(e.value),'') is not null
              and btrim(e.value)~*'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$'
          ) valid),'[]'::jsonb) canonical,
        (select count(*)::integer
          from jsonb_array_elements_text(
            case when jsonb_typeof(intent.intent_json->'cc')='array'
              then intent.intent_json->'cc' else '[]'::jsonb end) e(value)
          where nullif(btrim(e.value),'') is not null) requested_count,
        (select count(*)::integer
          from jsonb_array_elements_text(
            case when jsonb_typeof(intent.intent_json->'cc')='array'
              then intent.intent_json->'cc' else '[]'::jsonb end) e(value)
          where nullif(btrim(e.value),'') is not null
            and btrim(e.value)~*'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$')
          valid_count
    ) cc
    cross join lateral(
      select coalesce((select jsonb_agg(email order by email)
          from(
            select distinct lower(btrim(e.value)) email
            from jsonb_array_elements_text(
              case when jsonb_typeof(intent.intent_json->'bcc')='array'
                then intent.intent_json->'bcc' else '[]'::jsonb end) e(value)
            where nullif(btrim(e.value),'') is not null
              and btrim(e.value)~*'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$'
          ) valid),'[]'::jsonb) canonical,
        (select count(*)::integer
          from jsonb_array_elements_text(
            case when jsonb_typeof(intent.intent_json->'bcc')='array'
              then intent.intent_json->'bcc' else '[]'::jsonb end) e(value)
          where nullif(btrim(e.value),'') is not null) requested_count,
        (select count(*)::integer
          from jsonb_array_elements_text(
            case when jsonb_typeof(intent.intent_json->'bcc')='array'
              then intent.intent_json->'bcc' else '[]'::jsonb end) e(value)
          where nullif(btrim(e.value),'') is not null
            and btrim(e.value)~*'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$')
          valid_count
    ) bcc
    where r.command_type='ISSUE_INVOICES'
  ),
  asset_specs as materialized (
    select r.command_no,r.command_json,
      upper(btrim(coalesce(r.command_json->>'source_kind',''))) source_kind,
      case when coalesce(r.command_json->>'source_id','') ~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        then(r.command_json->>'source_id')::uuid end source_id,
      nullif(btrim(r.command_json->>'source_revision'),'') source_revision,
      nullif(btrim(r.command_json->>'original_r2_key'),'') original_r2_key,
      nullif(btrim(r.command_json->>'original_filename'),'') original_filename,
      nullif(btrim(r.command_json->>'declared_media_type'),'') declared_media_type,
      case when coalesce(r.command_json->>'rotation_degrees','') ~ '^(0|90|180|270)$'
        then(r.command_json->>'rotation_degrees')::integer end rotation_degrees,
      r.command_json?'rotation_degrees' rotation_supplied
    from raw r where r.command_type='PREPARE_ASSET'
  ),
  issue_entity_locks as materialized (
    select pg_advisory_xact_lock(
      hashtextextended('ISSUE_INVOICE|'||ci.id_text,0))
    from command_ids ci join raw r on r.command_no=ci.command_no
    where r.command_type='ISSUE_INVOICES' and ci.valid_uuid
    order by ci.id_text
  ),
  command_validation as materialized (
    select r.command_no,r.command_type,
      case
        when jsonb_typeof(r.command_json)<>'object'
          then 'COMMAND_MUST_BE_OBJECT'
        when r.command_type not in(
          'GENERATE_SELECTED','GENERATE_BY_WEEK','GENERATE_HOURS','GENERATE_EXPENSES',
          'GENERATE_NHSP','GENERATE_CREDIT_NOTE','GENERATE_AUTO',
          'VIEW_INVOICE_DOCUMENT','VIEW_TIMESHEET_DOCUMENT','PREPARE_ASSET',
          'ISSUE_INVOICES','DELIVER_INVOICES','RECONCILE')
          then 'UNSUPPORTED_COMMAND_TYPE'
        when r.command_type in(
          'GENERATE_SELECTED','GENERATE_BY_WEEK','GENERATE_HOURS',
          'GENERATE_EXPENSES','GENERATE_AUTO')
          and not(
            jsonb_typeof(r.command_json->'source_ids')='array'
            or jsonb_typeof(r.command_json->'timesheet_ids')='array')
          then 'GENERATION_SOURCE_ARRAY_REQUIRED'
        when r.command_type='GENERATE_NHSP'
          and jsonb_typeof(r.command_json->'nhsp_shift_ids')<>'array'
          then 'NHSP_SHIFT_ARRAY_REQUIRED'
        when r.command_type like 'GENERATE_%'
          and r.command_type<>'GENERATE_CREDIT_NOTE'
          and r.command_json?'consolidation_mode'
          and upper(coalesce(r.command_json->>'consolidation_mode','')) not in(
            'NONE','BY_WEEK','ANY_WEEK')
          then 'INVALID_CONSOLIDATION_MODE'
        when r.command_type like 'GENERATE_%'
          and r.command_type<>'GENERATE_CREDIT_NOTE'
          and r.command_json?'allow_early'
          and jsonb_typeof(r.command_json->'allow_early')<>'boolean'
          then 'ALLOW_EARLY_MUST_BE_BOOLEAN'
        when r.command_type like 'GENERATE_%'
          and r.command_type<>'GENERATE_CREDIT_NOTE'
          and nullif(btrim(coalesce(
            r.command_json->>'target_invoice_week','')),'') is not null
          and not pg_input_is_valid(
            r.command_json->>'target_invoice_week','date')
          then 'INVALID_TARGET_INVOICE_WEEK'
        when r.command_type='GENERATE_CREDIT_NOTE'
          and nullif(btrim(coalesce(r.command_json->>'base_invoice_id','')),'') is null
          then 'BASE_INVOICE_ID_REQUIRED'
        when r.command_type='GENERATE_CREDIT_NOTE'
          and(select count(*) from command_ids x
            where x.command_no=r.command_no)<>1
          then 'EXACTLY_ONE_BASE_INVOICE_REQUIRED'
        when r.command_type='GENERATE_CREDIT_NOTE'
          and nullif(btrim(coalesce(r.command_json->>'credit_reason','')),'') is null
          then 'CREDIT_REASON_REQUIRED'
        when r.command_type='GENERATE_CREDIT_NOTE'
          and nullif(btrim(coalesce(r.command_json->>'command_token','')),'') is null
          then 'CREDIT_COMMAND_TOKEN_REQUIRED'
        when exists(select 1 from command_ids x
          where x.command_no=r.command_no and not x.valid_uuid)
          then 'MALFORMED_UUID'
        when r.command_type like 'GENERATE_%' and not exists(
          select 1 from generation_groups g where g.command_no=r.command_no)
          then 'GENERATION_SOURCES_REQUIRED'
        when r.command_type like 'GENERATE_%'
          and (select count(*) from generation_groups g
            where g.command_no=r.command_no)>500
          then 'OPERATION_SCOPE_TOO_LARGE'
        when r.command_type in('VIEW_INVOICE_DOCUMENT','VIEW_TIMESHEET_DOCUMENT')
          and(select count(*) from command_ids x where x.command_no=r.command_no)<>1
          then 'EXACTLY_ONE_DOCUMENT_ENTITY_REQUIRED'
        when r.command_type in('VIEW_INVOICE_DOCUMENT','VIEW_TIMESHEET_DOCUMENT')
          and nullif(btrim(coalesce(r.command_json->>'priority_reason','')),'') is null
          then 'DOCUMENT_PRIORITY_REASON_REQUIRED'
        when r.command_type='VIEW_INVOICE_DOCUMENT'
          and upper(coalesce(nullif(r.command_json->>'purpose',''),
            'DRAFT_PREVIEW'))<>'DRAFT_PREVIEW'
          then 'INVALID_INVOICE_DOCUMENT_PURPOSE'
        when r.command_type='VIEW_TIMESHEET_DOCUMENT'
          and upper(coalesce(nullif(r.command_json->>'purpose',''),
            'TIMESHEET'))<>'TIMESHEET'
          then 'INVALID_TIMESHEET_DOCUMENT_PURPOSE'
        when r.command_type in('VIEW_INVOICE_DOCUMENT','VIEW_TIMESHEET_DOCUMENT')
          and not exists(select 1 from view_facts v
            where v.command_no=r.command_no and v.document_revision is not null)
          then 'DOCUMENT_ENTITY_NOT_FOUND'
        when r.command_type='PREPARE_ASSET' and not exists(
          select 1 from asset_specs a
          where a.command_no=r.command_no
            and a.source_kind in(
              'MANUAL_TIMESHEET','TIMESHEET_EVIDENCE','OTHER_EVIDENCE',
              'HR_REPORT','NHSP_REPORT','HIGHER_RATE_SUPPORT')
            and a.source_id is not null and a.source_revision is not null
            and a.original_r2_key is not null and a.original_filename is not null
            and a.declared_media_type is not null
            and(not a.rotation_supplied or a.rotation_degrees is not null))
          then 'INVALID_ASSET_SOURCE_METADATA'
        when r.command_type='PREPARE_ASSET'
          and r.command_json?'rotation_degrees'
          and coalesce(r.command_json->>'rotation_degrees','')!~'^(0|90|180|270)$'
          then 'INVALID_ASSET_ROTATION'
        when r.command_type='ISSUE_INVOICES'
          and(jsonb_typeof(r.command_json->'invoice_ids')<>'array'
            or coalesce(cardinality((select ids from canonical_ids x
              where x.command_no=r.command_no)),0)=0
            or coalesce((select resolved_count from issue_facts f
              where f.command_no=r.command_no),0)<>
              coalesce(cardinality((select ids from canonical_ids x
                where x.command_no=r.command_no)),0))
          then 'ISSUE_INVOICES_NOT_RESOLVED'
        when r.command_type='ISSUE_INVOICES'
          and (r.command_json?'allow_early'
            and jsonb_typeof(r.command_json->'allow_early')<>'boolean'
            or r.command_json?'deliver'
            and jsonb_typeof(r.command_json->'deliver')<>'boolean')
          then 'ISSUE_FLAGS_MUST_BE_BOOLEAN'
        when r.command_type='ISSUE_INVOICES'
          and coalesce(nullif(r.command_json->>'command_token',''),'')=''
          then 'ISSUE_COMMAND_TOKEN_REQUIRED'
        when r.command_type='ISSUE_INVOICES'
          and coalesce(cardinality((select ids from canonical_ids x
            where x.command_no=r.command_no)),0)>500
          then 'OPERATION_SCOPE_TOO_LARGE'
        when r.command_type='ISSUE_INVOICES'
          and r.command_json?'delivery_intent'
          and jsonb_typeof(r.command_json->'delivery_intent')<>'object'
          then 'ISSUE_DELIVERY_INTENT_MUST_BE_OBJECT'
        when r.command_type='ISSUE_INVOICES'
          and jsonb_typeof(r.command_json->'delivery_intent')='object'
          and (
            r.command_json->'delivery_intent'?'recipient_set'
              and jsonb_typeof(r.command_json->'delivery_intent'->'recipient_set')<>'array'
            or r.command_json->'delivery_intent'?'cc'
              and jsonb_typeof(r.command_json->'delivery_intent'->'cc')<>'array'
            or r.command_json->'delivery_intent'?'bcc'
              and jsonb_typeof(r.command_json->'delivery_intent'->'bcc')<>'array')
          then 'ISSUE_DELIVERY_RECIPIENT_SET_MUST_BE_ARRAY'
        when r.command_type='ISSUE_INVOICES'
          and exists(
            select 1 from issue_delivery_intents intent
            where intent.command_no=r.command_no
              and intent.requested_count<>intent.valid_count)
          then 'ISSUE_DELIVERY_RECIPIENT_SET_INVALID'
        when r.command_type='ISSUE_INVOICES' and exists(
          select 1 from command_ids x
          join public.invoice_operation_chunks c
            on c.chunk_type='ISSUE_INVOICE' and c.entity_type='INVOICE'
            and c.entity_id=x.id_text::uuid
            and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
          where x.command_no=r.command_no and x.valid_uuid)
          and not exists(
            select 1
            from public.invoice_operations exact
            join issue_facts f on f.command_no=r.command_no
            join issue_delivery_intents intent on intent.command_no=r.command_no
            where exact.idempotency_key=encode(digest(concat_ws('|',
              'ISSUE_INVOICES',f.invoice_revision_hash,
              coalesce(r.command_json->>'allow_early','false'),
              coalesce(r.command_json->>'deliver','false'),
              coalesce(r.command_json->>'command_token',''),
              encode(digest(intent.canonical_intent::text,'sha256'),'hex')),
              'sha256'),'hex')
              and exact.status in(
                'QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED'))
          then 'ACTIVE_ISSUE_CONFLICT'
        when r.command_type='DELIVER_INVOICES'
          and not(
            jsonb_typeof(r.command_json->'invoice_ids')='array'
            or nullif(btrim(coalesce(r.command_json->>'invoice_id','')),'') is not null)
          then 'DELIVERY_INVOICE_IDS_REQUIRED'
        when r.command_type='DELIVER_INVOICES'
          and nullif(btrim(coalesce(
            r.command_json->>'delivery_request_token','')),'') is null
          then 'DELIVERY_REQUEST_TOKEN_REQUIRED'
        when r.command_type='DELIVER_INVOICES'
          and coalesce(cardinality((select ids from canonical_ids x
            where x.command_no=r.command_no)),0)>500
          then 'OPERATION_SCOPE_TOO_LARGE'
        when r.command_type='DELIVER_INVOICES'
          and r.command_json?'recipient_set'
          and jsonb_typeof(r.command_json->'recipient_set')<>'array'
          then 'DELIVERY_RECIPIENT_SET_MUST_BE_ARRAY'
        when r.command_type='DELIVER_INVOICES'
          and ((r.command_json?'cc' and jsonb_typeof(r.command_json->'cc')<>'array')
            or(r.command_json?'bcc' and jsonb_typeof(r.command_json->'bcc')<>'array'))
          then 'DELIVERY_CC_BCC_MUST_BE_ARRAY'
        when r.command_type='DELIVER_INVOICES'
          and exists(select 1 from recipient_sets rs
            where rs.command_no=r.command_no and rs.invalid_count>0)
          then 'DELIVERY_RECIPIENT_SET_INVALID'
        when r.command_type='DELIVER_INVOICES'
          and r.command_json?'delivery_part_number'
          and coalesce(r.command_json->>'delivery_part_number','') !~ '^[1-9][0-9]*$'
          then 'INVALID_DELIVERY_PART_NUMBER'
        when r.command_type='DELIVER_INVOICES'
          and r.command_json?'delivery_policy'
          and upper(coalesce(r.command_json->>'delivery_policy',''))
            not in('ATTACH','SPLIT','SECURE_LINK')
          then 'INVALID_DELIVERY_POLICY'
        when r.command_type='DELIVER_INVOICES'
          and(coalesce((select resolved_count from delivery_facts f
              where f.command_no=r.command_no),0)<>
              coalesce(cardinality((select ids from canonical_ids x
                where x.command_no=r.command_no)),0))
          then 'ISSUED_DOCUMENT_NOT_READY'
        when r.command_type='RECONCILE'
          and not(r.command_json?'operation_ids'
            or r.command_json?'older_than_seconds') then 'RECONCILE_SCOPE_REQUIRED'
        when r.command_type='RECONCILE'
          and r.command_json?'operation_ids'
          and jsonb_typeof(r.command_json->'operation_ids')<>'array'
          then 'RECONCILE_OPERATION_IDS_MUST_BE_ARRAY'
        when r.command_type='RECONCILE'
          and jsonb_typeof(r.command_json->'operation_ids')='array'
          and jsonb_array_length(r.command_json->'operation_ids')>500
          then 'RECONCILE_OPERATION_SCOPE_TOO_LARGE'
        when r.command_type='RECONCILE'
          and r.command_json?'older_than_seconds'
          and coalesce(r.command_json->>'older_than_seconds','') !~ '^[0-9]{1,6}$'
          then 'RECONCILE_AGE_MUST_BE_INTEGER_SECONDS'
      end error_code,
      case when r.command_type like 'GENERATE_%' then (
        select jsonb_agg(jsonb_build_object(
          'group_key',g.group_key,'code',g.blocker_code,'detail',g.blocker_detail)
          order by g.group_key)
        from generation_groups g
        where g.command_no=r.command_no and g.blocker_code is not null)
      end error_detail
    from raw r cross join(select count(*) from issue_entity_locks) lock_barrier
  ),
  valid_commands as materialized (
    select r.* from raw r join command_validation v using(command_no,command_type)
    where v.error_code is null
  ),
  inserted_assets as materialized (
    insert into public.invoice_document_assets(
      source_kind,source_id,source_revision,original_r2_key,original_filename,
      declared_media_type,orientation_degrees,status,normalised_manifest_json,
      created_at_utc,updated_at_utc)
    select a.source_kind,a.source_id,a.source_revision,a.original_r2_key,
      a.original_filename,a.declared_media_type,a.rotation_degrees,
      'DISCOVERED','[]'::jsonb,v_now,v_now
    from asset_specs a
    join valid_commands v on v.command_no=a.command_no
    on conflict(source_kind,source_id,source_revision,original_r2_key) do nothing
    returning *
  ),
  assets as materialized (
    select a.command_no,inserted.*
    from asset_specs a
    join valid_commands v on v.command_no=a.command_no
    join inserted_assets inserted
      on inserted.source_kind=a.source_kind
      and inserted.source_id=a.source_id
      and inserted.source_revision=a.source_revision
      and inserted.original_r2_key=a.original_r2_key
    union all
    select a.command_no,registered.*
    from asset_specs a
    join valid_commands v on v.command_no=a.command_no
    join public.invoice_document_assets registered
      on registered.source_kind=a.source_kind
      and registered.source_id=a.source_id
      and registered.source_revision=a.source_revision
      and registered.original_r2_key=a.original_r2_key
    where not exists(
      select 1 from inserted_assets inserted
      where inserted.source_kind=a.source_kind
        and inserted.source_id=a.source_id
        and inserted.source_revision=a.source_revision
        and inserted.original_r2_key=a.original_r2_key)
  ),
  asset_workflow_state as materialized (
    select a.*,
      exists(
        select 1
        from public.invoice_operation_chunks c
        join public.invoice_operations o on o.id=c.operation_id
        where c.document_asset_id=a.id
          and c.chunk_type in('ASSET_INSPECT','ASSET_NORMALISE')
          and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT')
          and o.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT')
      ) has_active_work
    from assets a
  ),
  operation_specs as materialized (
    select v.command_no,v.command_type,v.command_json,
      'GENERATE_INVOICES'::text operation_type,
      case when count(*)=1 then 'CLIENT' else 'INVOICE_BATCH' end entity_type,
      case when count(*)=1 then(array_agg(g.client_id order by g.client_id))[1]
        else null::uuid end entity_id,
      encode(digest(string_agg(g.source_revision_hash,'|' order by g.group_key),
        'sha256'),'hex') source_revision,
      null::text template_version,
      case when v.command_type='GENERATE_AUTO' then 200 else 600 end priority,
      count(*)::integer unit_count,
      jsonb_build_object('generation_groups',
        jsonb_agg(g.idempotency_components order by g.group_key)) canonical_input,
      encode(digest('GENERATE_INVOICES|'||
        string_agg(g.idempotency_components::text,'|' order by g.group_key),
        'sha256'),'hex') idempotency_key
    from valid_commands v join generation_groups g on g.command_no=v.command_no
    where v.command_type like 'GENERATE_%'
    group by v.command_no,v.command_type,v.command_json
    union all
    select v.command_no,v.command_type,v.command_json,'BUILD_DOCUMENT',
      f.entity_type,f.entity_id,f.document_revision::text,f.template_version,1000,1,
      jsonb_build_object('entity_type',f.entity_type,'entity_id',f.entity_id,
        'purpose',f.purpose,'document_revision',f.document_revision,
        'template_version',f.template_version),
      encode(digest(concat_ws('|','BUILD_DOCUMENT',f.entity_type,f.entity_id::text,
        f.purpose,f.document_revision::text,f.template_version),'sha256'),'hex')
    from valid_commands v join view_facts f on f.command_no=v.command_no
    where v.command_type in('VIEW_INVOICE_DOCUMENT','VIEW_TIMESHEET_DOCUMENT')
      and not exists(
        select 1 from public.invoice_document_versions ready
        where ready.entity_type=f.entity_type and ready.entity_id=f.entity_id
          and ready.purpose=f.purpose
          and ready.source_revision=f.document_revision::text
          and ready.template_version=f.template_version and ready.status='READY')
    union all
    select v.command_no,v.command_type,v.command_json,'PREPARE_ASSET',
      'DOCUMENT_ASSET',a.id,a.source_revision,null,550,1,
      jsonb_build_object('document_asset_id',a.id,'source_kind',a.source_kind,
        'source_id',a.source_id,'source_revision',a.source_revision,
        'original_r2_key',a.original_r2_key),
      encode(digest(concat_ws('|','PREPARE_ASSET',a.source_kind,a.source_id::text,
        a.source_revision,a.original_r2_key),'sha256'),'hex')
    from valid_commands v join asset_workflow_state a
      on a.command_no=v.command_no
    where v.command_type='PREPARE_ASSET'
      and a.status in('DISCOVERED','INSPECTING','NORMALISING')
      and not(a.status in('INSPECTING','NORMALISING')
        and not a.has_active_work)
    union all
    select v.command_no,v.command_type,v.command_json,'ISSUE_INVOICES',
      'INVOICE_BATCH',null,f.invoice_revision_hash,null,850,
      cardinality(f.invoice_ids),
      jsonb_build_object('invoice_ids',to_jsonb(f.invoice_ids),
        'invoice_revision_hash',f.invoice_revision_hash,
        'allow_early',coalesce(v.command_json->'allow_early','false'::jsonb),
        'deliver',coalesce(v.command_json->'deliver','false'::jsonb),
        'command_token',v.command_json->>'command_token',
        'delivery_request_token','ISSUE:'||(v.command_json->>'command_token'),
        'delivery_intent',intent.canonical_intent),
      encode(digest(concat_ws('|','ISSUE_INVOICES',f.invoice_revision_hash,
        coalesce(v.command_json->>'allow_early','false'),
        coalesce(v.command_json->>'deliver','false'),
        coalesce(v.command_json->>'command_token',''),
        encode(digest(intent.canonical_intent::text,'sha256'),'hex')),
        'sha256'),'hex')
    from valid_commands v join issue_facts f on f.command_no=v.command_no
    join issue_delivery_intents intent on intent.command_no=v.command_no
    where v.command_type='ISSUE_INVOICES'
    union all
    select v.command_no,v.command_type,v.command_json,'DELIVER_INVOICES',
      'INVOICE_BATCH',null,f.delivery_revision,
      coalesce(nullif(v.command_json->>'delivery_template_version',''),
        'invoice-delivery-v1'),400,cardinality(f.invoice_ids),
      jsonb_build_object('invoice_ids',to_jsonb(f.invoice_ids),
        'issued_document_version_ids',to_jsonb(f.document_version_ids),
        'delivery_revision',f.delivery_revision,
        'recipient_set',rs.canonical_recipients,
        'recipient_set_hash',drg.route_set_hash,
        'frozen_delivery_routes',drg.frozen_routes,
        'template_version',coalesce(nullif(v.command_json->>'delivery_template_version',''),
          'invoice-delivery-v1'),
        'delivery_part_number',coalesce(v.command_json->'delivery_part_number','1'::jsonb),
        'delivery_request_token',v.command_json->>'delivery_request_token'),
      encode(digest(concat_ws('|','DELIVER_INVOICES',f.delivery_revision,
        drg.route_set_hash,
        coalesce(nullif(v.command_json->>'delivery_template_version',''),
          'invoice-delivery-v1'),
        coalesce(v.command_json->>'delivery_part_number','1'),
        v.command_json->>'delivery_request_token'),'sha256'),'hex')
    from valid_commands v join delivery_facts f on f.command_no=v.command_no
    join recipient_sets rs on rs.command_no=v.command_no
    join delivery_route_groups drg on drg.command_no=v.command_no
    where v.command_type='DELIVER_INVOICES'
    union all
    select v.command_no,v.command_type,v.command_json,'RECONCILE_INVOICE_WORK',
      'SYSTEM',null,
      encode(digest(coalesce(v.command_json->'operation_ids','[]')::text||'|'||
        coalesce(v.command_json->>'older_than_seconds',''),'sha256'),'hex'),
      null,50,1,
      jsonb_build_object(
        'operation_ids',coalesce(v.command_json->'operation_ids','[]'::jsonb),
        'older_than_seconds',least(604800,greatest(30,case
          when coalesce(v.command_json->>'older_than_seconds','') ~ '^[0-9]{1,6}$'
            then(v.command_json->>'older_than_seconds')::integer else 300 end)),
        'max_rows',least(500,greatest(1,case
          when coalesce(v.command_json->>'max_rows','') ~ '^[0-9]{1,6}$'
            then(v.command_json->>'max_rows')::integer else 100 end))),
      encode(digest('RECONCILE|'||coalesce(v.command_json->'operation_ids','[]')::text||
        '|'||coalesce(v.command_json->>'older_than_seconds',''),'sha256'),'hex')
    from valid_commands v where v.command_type='RECONCILE'
  ),
  locks as materialized (
    select pg_advisory_xact_lock(hashtextextended(x.lock_key,0))
    from(
      select idempotency_key lock_key from operation_specs
      union
      select 'ISSUE_INVOICE|'||unnest(f.invoice_ids)::text
      from issue_facts f join valid_commands v on v.command_no=f.command_no
      where v.command_type='ISSUE_INVOICES'
    ) x order by x.lock_key
  ),
  existing_before as materialized (
    select s.command_no,o.*
    from operation_specs s
    join lateral (
      select candidate.*
      from public.invoice_operations candidate
      where candidate.idempotency_key=s.idempotency_key
        and candidate.status in(
          'QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED','COMPLETE')
      order by(candidate.status='COMPLETE') desc,candidate.created_at_utc desc
      limit 1
    ) o on true
  ),
  inserted_operations as materialized (
    insert into public.invoice_operations(
      operation_type,entity_type,entity_id,actor_user_id,idempotency_key,
      status,phase,priority,source_revision,template_version,input_json,
      config_json,progress_json,total_units,chunk_count,control_version,
      change_seq,created_at_utc,updated_at_utc)
    select s.operation_type,s.entity_type,s.entity_id,p_actor_user_id,
      s.idempotency_key,'QUEUED','SUBMITTED',s.priority,s.source_revision,
      s.template_version,s.canonical_input,
      jsonb_build_object(
        'command_type',s.command_type,
        'processor_policy',private._invoice_processor_limits()),
      jsonb_build_object('status_message','Accepted','total_units',s.unit_count,
        'completed_units',0,'failed_units',0),
      s.unit_count,s.unit_count,1,nextval('public.invoice_operation_change_seq'),
      v_now,v_now
    from operation_specs s cross join(select count(*) from locks) lock_barrier
    where not exists(select 1 from existing_before e where e.command_no=s.command_no)
    on conflict do nothing
    returning *
  ),
  chosen as materialized (
    select s.*,coalesce(i.id,e.id) operation_id,
      coalesce(i.status,e.status) operation_status,
      coalesce(i.phase,e.phase) operation_phase,
      coalesce(i.control_version,e.control_version) control_version,
      coalesce(i.change_seq,e.change_seq) change_seq,
      coalesce(i.priority,e.priority,s.priority) current_priority,
      i.id is not null created,e.id is not null reused
    from operation_specs s
    left join inserted_operations i on i.idempotency_key=s.idempotency_key
    left join existing_before e on e.command_no=s.command_no
  ),
  raised as materialized (
    update public.invoice_operations o
    set priority=greatest(o.priority,c.priority),updated_at_utc=v_now,
        change_seq=case when o.priority<c.priority
          then nextval('public.invoice_operation_change_seq') else o.change_seq end
    from chosen c
    where c.reused and o.id=c.operation_id
      and o.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
    returning o.id
  ),
  existing_document_versions as materialized (
    select c.command_no,c.operation_id,v.*
    from chosen c join view_facts f on f.command_no=c.command_no
    join public.invoice_document_versions v
      on v.entity_type=f.entity_type and v.entity_id=f.entity_id
      and v.purpose=f.purpose and v.source_revision=f.document_revision::text
      and v.template_version=f.template_version
      and v.status in(
        'PLANNING','WAITING_FOR_INPUTS','RENDERING','ASSEMBLING','VERIFYING','READY')
    where c.command_type in('VIEW_INVOICE_DOCUMENT','VIEW_TIMESHEET_DOCUMENT')
  ),
  document_versions as materialized (
    insert into public.invoice_document_versions(
      entity_type,entity_id,purpose,operation_id,source_revision,template_version,
      status,snapshot_json,snapshot_hash,manifest_json,manifest_hash,created_at_utc)
    select f.entity_type,f.entity_id,f.purpose,c.operation_id,
      f.document_revision::text,f.template_version,'PLANNING','{}'::jsonb,
      encode(digest('{}','sha256'),'hex'),'[]'::jsonb,
      encode(digest('[]','sha256'),'hex'),v_now
    from chosen c join view_facts f on f.command_no=c.command_no
    where c.command_type in('VIEW_INVOICE_DOCUMENT','VIEW_TIMESHEET_DOCUMENT')
      and c.operation_status<>'COMPLETE'
    on conflict(entity_type,entity_id,purpose,source_revision,template_version)
      where purpose in('DRAFT_PREVIEW','TIMESHEET') and status in(
        'PLANNING','WAITING_FOR_INPUTS','RENDERING','ASSEMBLING','VERIFYING','READY')
    do nothing
    returning *
  ),
  all_document_versions as materialized (
    select c.command_no,c.operation_id,v.*
    from chosen c join view_facts f on f.command_no=c.command_no
    join document_versions v
      on v.entity_type=f.entity_type and v.entity_id=f.entity_id
      and v.purpose=f.purpose and v.source_revision=f.document_revision::text
      and v.template_version=f.template_version
    where c.command_type in('VIEW_INVOICE_DOCUMENT','VIEW_TIMESHEET_DOCUMENT')
    union all
    select e.* from existing_document_versions e
  ),
  chunk_specs as materialized (
    select c.operation_id,'GENERATION_GROUP'::text chunk_type,
      'VALIDATE_SOURCES'::text phase,
      encode(digest(concat_ws('|','GENERATION_GROUP',g.group_key,
        g.source_revision_hash,c.command_type,
        coalesce(g.idempotency_components->>'consolidation_mode',''),
        coalesce(g.idempotency_components->>'invoice_stream',''),'1'),
        'sha256'),'hex') work_key,
      row_number() over(partition by c.operation_id order by g.group_key)::integer-1 sequence_no,
      'CLIENT'::text entity_type,g.client_id entity_id,null::uuid document_version_id,
      null::uuid document_asset_id,
      g.idempotency_components||jsonb_build_object(
        'canonical_source_ids',to_jsonb(g.canonical_source_ids),
        'canonical_source_members',g.canonical_source_members,
        'source_types',to_jsonb(g.source_types),
        'source_revision',g.source_revision_hash,
        'resolver_blocker_code',g.blocker_code,
        'resolver_blocker_detail',g.blocker_detail) payload_json,
      c.priority,c.control_version
    from chosen c join generation_groups g on g.command_no=c.command_no
    where c.command_type like 'GENERATE_%' and c.operation_status<>'COMPLETE'
    union all
    select c.operation_id,'DOCUMENT_PLAN','BUILD_MANIFEST',
      encode(digest(concat_ws('|','DOCUMENT_PLAN',v.id::text,
        f.document_revision::text,f.template_version,'1'),'sha256'),'hex'),
      0,f.entity_type,f.entity_id,
      v.id,null,jsonb_build_object('purpose',f.purpose,
        'source_revision',f.document_revision::text,
        'template_version',f.template_version),c.priority,c.control_version
    from chosen c join view_facts f on f.command_no=c.command_no
    join all_document_versions v on v.command_no=c.command_no
    where c.command_type in('VIEW_INVOICE_DOCUMENT','VIEW_TIMESHEET_DOCUMENT')
      and c.operation_status<>'COMPLETE' and v.status<>'READY'
    union all
    select c.operation_id,'ASSET_INSPECT','INSPECT',
      encode(digest(concat_ws('|','ASSET_INSPECT',a.id::text,a.source_revision,
        private._invoice_processor_limits()->>'policy_version'),'sha256'),'hex'),
      0,'DOCUMENT_ASSET',a.id,
      null,a.id,jsonb_build_object('source_revision',a.source_revision,
        'source_kind',a.source_kind,'source_id',a.source_id,
        'rotation_degrees',a.orientation_degrees),c.priority,c.control_version
    from chosen c join assets a on a.command_no=c.command_no
    where c.command_type='PREPARE_ASSET' and c.operation_status<>'COMPLETE'
      and a.status not in('READY','INSPECTING','NORMALISING')
    union all
    select c.operation_id,'ISSUE_INVOICE','VALIDATE',
      encode(digest(concat_ws('|','ISSUE_INVOICE',i.id::text,
        i.document_revision::text,c.command_json->>'command_token'),'sha256'),'hex'),
      row_number() over(partition by c.operation_id order by i.id)::integer-1,
      'INVOICE',i.id,null,null,
      jsonb_build_object(
        'request_key',c.command_no::text||':'||i.id::text,
        'invoice_id',i.id,'source_revision',i.document_revision,
        'evaluation_date',(v_now at time zone 'Europe/London')::date,
        'frozen_issue_at_utc',v_now,
        'allow_early',coalesce(c.command_json->'allow_early','false'::jsonb),
        'deliver',coalesce(c.command_json->'deliver','false'::jsonb),
        'command_token',c.command_json->>'command_token',
        'delivery_request_token','ISSUE:'||(c.command_json->>'command_token'),
        'delivery_intent',intent.canonical_intent),
      c.priority,c.control_version
    from chosen c join issue_facts f on f.command_no=c.command_no
    join issue_delivery_intents intent on intent.command_no=c.command_no
    cross join unnest(f.invoice_ids) invoice_id
    join public.invoices i on i.id=invoice_id
    where c.command_type='ISSUE_INVOICES' and c.operation_status<>'COMPLETE'
    union all
    select c.operation_id,'DELIVERY_PREPARE','PREPARE',
      encode(digest(concat_ws('|','DELIVERY_PREPARE',
        i.issued_document_version_id::text,dr.route_policy_hash,
        coalesce(nullif(c.command_json->>'delivery_template_version',''),
          'invoice-delivery-v1'),
        coalesce(c.command_json->>'delivery_part_number','1'),
        c.command_json->>'delivery_request_token'),'sha256'),'hex'),
      row_number() over(partition by c.operation_id order by i.id)::integer-1,
      'INVOICE',i.id,i.issued_document_version_id,null,
      jsonb_build_object(
        'invoice_id',i.id,'issued_document_version_id',i.issued_document_version_id,
        'recipient_set',dr.canonical_to,'cc',dr.canonical_cc,
        'bcc',dr.canonical_bcc,
        'recipient_set_hash',dr.recipient_set_hash,
        'route_policy_hash',dr.route_policy_hash,
        'frozen_delivery_route',jsonb_build_object(
          'request_key',dr.request_key,
          'to',dr.canonical_to,'cc',dr.canonical_cc,'bcc',dr.canonical_bcc,
          'recipient_set_hash',dr.recipient_set_hash,
          'route_policy_hash',dr.route_policy_hash,
          'route_source',dr.route_source,'do_not_send',dr.do_not_send,
          'warning_codes',dr.warning_codes,'blocker_codes',dr.blocker_codes,
          'evaluated_date',(v_now at time zone 'Europe/London')::date),
        'template_version',coalesce(nullif(c.command_json->>'delivery_template_version',''),
          'invoice-delivery-v1'),
        'delivery_policy',upper(coalesce(
          nullif(c.command_json->>'delivery_policy',''),'ATTACH')),
        'delivery_part_number',coalesce(c.command_json->'delivery_part_number','1'::jsonb),
        'delivery_request_token',encode(digest(concat_ws('|',
          'DELIVERY_REQUEST',c.command_json->>'delivery_request_token',
          i.id::text,i.issued_document_version_id::text),'sha256'),'hex'),
        'user_command_token',c.command_json->>'delivery_request_token'),
      c.priority,c.control_version
    from chosen c join delivery_facts f on f.command_no=c.command_no
    join recipient_sets rs on rs.command_no=c.command_no
    cross join unnest(f.invoice_ids) invoice_id
    join public.invoices i on i.id=invoice_id
    join delivery_routes dr on dr.command_no=c.command_no
      and dr.request_key=c.command_no::text||':'||i.id::text
      and dr.invoice_id=i.id
    where c.command_type='DELIVER_INVOICES' and c.operation_status<>'COMPLETE'
    union all
    select c.operation_id,'RECONCILE','RECONCILE',
      encode(digest(concat_ws('|','RECONCILE',c.canonical_input::text,'1'),
        'sha256'),'hex'),
      0,'SYSTEM',null,null,null,
      c.canonical_input,c.priority,c.control_version
    from chosen c where c.command_type='RECONCILE' and c.operation_status<>'COMPLETE'
  ),
  inserted_chunks as materialized (
    insert into public.invoice_operation_chunks(
      operation_id,chunk_type,phase,work_key,sequence_no,entity_type,entity_id,
      document_version_id,document_asset_id,status,priority,run_after_utc,
      payload_json,operation_control_version,created_at_utc,updated_at_utc)
    select c.operation_id,c.chunk_type,c.phase,c.work_key,c.sequence_no,c.entity_type,c.entity_id,
      c.document_version_id,c.document_asset_id,'QUEUED',c.priority,v_now,
      c.payload_json,c.control_version,v_now,v_now
    from chunk_specs c
    on conflict do nothing
    returning *
  ),
  asset_operation_links as (
    update public.invoice_document_assets a
    set operation_id=c.operation_id,
        status=case when a.status='DISCOVERED' then 'INSPECTING' else a.status end,
        updated_at_utc=v_now
    from chosen c join assets x on x.command_no=c.command_no
    where c.command_type='PREPARE_ASSET' and a.id=x.id and a.status<>'READY'
    returning a.id
  ),
  entity_pointers as (
    update public.invoices i
    set active_document_operation_id=case
          when c.command_type='VIEW_INVOICE_DOCUMENT' then c.operation_id
          else i.active_document_operation_id end,
        document_state=case when c.command_type='VIEW_INVOICE_DOCUMENT'
          then 'QUEUED' else i.document_state end,
        active_issue_operation_id=case when c.command_type='ISSUE_INVOICES'
          then c.operation_id else i.active_issue_operation_id end,
        issue_state=case when c.command_type='ISSUE_INVOICES'
          then 'VALIDATING' else i.issue_state end,
        updated_at=v_now
    from chosen c
    where(c.command_type='VIEW_INVOICE_DOCUMENT' and i.id=c.entity_id)
      or(c.command_type='ISSUE_INVOICES' and exists(
        select 1
        from issue_facts f
        where f.command_no=c.command_no
          and i.id=any(f.invoice_ids)))
    returning i.id
  ),
  timesheet_pointers as (
    update public.timesheets t
    set active_document_operation_id=c.operation_id,document_state='QUEUED',
        updated_at=v_now
    from chosen c
    where c.command_type='VIEW_TIMESHEET_DOCUMENT' and t.timesheet_id=c.entity_id
    returning t.timesheet_id
  ),
  refreshed_counts as (
    update public.invoice_operations o
    set total_units=greatest(o.total_units,q.n),
        chunk_count=greatest(o.chunk_count,q.n),
        progress_json=coalesce(o.progress_json,'{}'::jsonb)||jsonb_build_object(
          'status_message','Queued',
          'total_units',greatest(o.total_units,q.n)),
        updated_at_utc=v_now,
        change_seq=nextval('public.invoice_operation_change_seq')
    from(
      select intended.operation_id,count(*)::integer n
      from chunk_specs intended
      cross join(
        select count(*) from inserted_chunks
      ) ensure_chunk_insert
      group by intended.operation_id
    ) q
    where o.id=q.operation_id and o.status<>'COMPLETE'
    returning o.id,o.chunk_count n
  ),
  ready_results as (
    select r.command_no,jsonb_build_object(
      'command_no',r.command_no,'command_type',r.command_type,
      'accepted',true,'operation_id',null,'status','READY',
      'document_version_id',v.id,'r2_key',v.r2_key,
      'created',false,'reused_active',false,'reused_ready',true,
      'priority_raised',false,'blocked',false,'terminal_error',null) result
    from raw r join view_facts f on f.command_no=r.command_no
    join public.invoice_document_versions v
      on v.entity_type=f.entity_type and v.entity_id=f.entity_id
      and v.purpose=f.purpose and v.source_revision=f.document_revision::text
      and v.template_version=f.template_version and v.status='READY'
    where r.command_type in('VIEW_INVOICE_DOCUMENT','VIEW_TIMESHEET_DOCUMENT')
    union all
    select r.command_no,jsonb_build_object(
      'command_no',r.command_no,'command_type',r.command_type,
      'accepted',true,'operation_id',null,'status','READY',
      'document_asset_id',a.id,'created',false,'reused_active',false,
      'reused_ready',true,'priority_raised',false,'blocked',false,
      'terminal_error',null) result
    from raw r join assets a on a.command_no=r.command_no
    where r.command_type='PREPARE_ASSET' and a.status='READY'
    union all
    select r.command_no,jsonb_build_object(
      'command_no',r.command_no,'command_type',r.command_type,
      'accepted',true,'operation_id',a.operation_id,'status',a.status,
      'document_asset_id',a.id,'created',false,'reused_active',false,
      'reused_ready',false,'priority_raised',false,'blocked',true,
      'retry_required',true,'terminal_error',coalesce(a.error_json,
        jsonb_build_object('code','ASSET_'||a.status))) result
    from raw r join assets a on a.command_no=r.command_no
    where r.command_type='PREPARE_ASSET'
      and a.status in('UNSUPPORTED','CORRUPT','MISSING','FAILED','SUPERSEDED')
    union all
    select r.command_no,jsonb_build_object(
      'command_no',r.command_no,'command_type',r.command_type,
      'accepted',true,'operation_id',a.operation_id,'status','BLOCKED',
      'document_asset_id',a.id,'created',false,'reused_active',false,
      'reused_ready',false,'priority_raised',false,'blocked',true,
      'retry_required',true,'reconcile_available',true,
      'terminal_error',jsonb_build_object(
        'code','ASSET_WORKFLOW_INCONSISTENT',
        'asset_status',a.status,'document_asset_id',a.id)) result
    from raw r join asset_workflow_state a on a.command_no=r.command_no
    where r.command_type='PREPARE_ASSET'
      and a.status in('INSPECTING','NORMALISING')
      and not a.has_active_work
  ),
  operation_results as (
    select c.command_no,jsonb_build_object(
      'command_no',c.command_no,'command_type',c.command_type,'accepted',true,
      'operation_id',c.operation_id,'operation_type',c.operation_type,
      'status',coalesce(o.status,c.operation_status),
      'phase',coalesce(o.phase,c.operation_phase),
      'source_revision',c.source_revision,
      'change_seq',coalesce(o.change_seq,c.change_seq),
      'created',c.created,
      'reused_active',c.reused and coalesce(o.status,c.operation_status)<>'COMPLETE',
      'reused_ready',c.reused and coalesce(o.status,c.operation_status)='COMPLETE',
      'priority_raised',c.reused and c.priority>c.current_priority,
      'blocked',coalesce(o.status,c.operation_status)='BLOCKED',
      'terminal_error',null,
      'chunk_count',greatest(c.unit_count,(select count(*)
        from chunk_specs intended
        where intended.operation_id=c.operation_id)))
      result
    from chosen c
    left join public.invoice_operations o on o.id=c.operation_id
    left join refreshed_counts rc on rc.id=c.operation_id
  ),
  error_results as (
    select v.command_no,jsonb_build_object(
      'command_no',v.command_no,'command_type',v.command_type,
      'accepted',false,'created',false,'reused_active',false,
      'reused_ready',false,'priority_raised',false,
      'blocked',false,
      'terminal_error',jsonb_build_object('code',v.error_code,
        'detail',v.error_detail),
      'error',jsonb_build_object('code',v.error_code,'detail',v.error_detail)) result
    from command_validation v where v.error_code is not null
  ),
  combined as (
    select * from ready_results
    union all
    select o.* from operation_results o
    where not exists(select 1 from ready_results r where r.command_no=o.command_no)
    union all
    select * from error_results
  )
  select coalesce(jsonb_agg(result order by command_no),'[]'::jsonb)
  into v_result from combined;

  return v_result;
end;
$function$;

revoke all on function public.invoice_operation_start_batch(jsonb,uuid,timestamptz)
  from public,anon;
grant execute on function public.invoice_operation_start_batch(jsonb,uuid,timestamptz)
  to authenticated,service_role;
