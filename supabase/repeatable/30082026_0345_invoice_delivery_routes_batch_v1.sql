-- Complete current source owner for the private invoice delivery-route batch.
-- The signed baseline and current contract already contain this exact helper;
-- LEGACY_UPGRADE proved that it previously lacked an active repeatable owner.
-- The function body below is byte-for-byte the signed baseline definition.

CREATE OR REPLACE FUNCTION private._invoice_delivery_routes_batch(p_requests jsonb, p_evaluation_date date)
 RETURNS TABLE(request_key text, invoice_id uuid, canonical_to jsonb, canonical_cc jsonb, canonical_bcc jsonb, invalid_to_count integer, invalid_cc_count integer, invalid_bcc_count integer, recipient_set_hash text, route_policy_hash text, route_source text, client_settings_id uuid, contract_settings_ids jsonb, effective_date date, client_id uuid, invoice_group_identity text, self_bill boolean, do_not_send boolean, delivery_suppressed boolean, suppression_reason text, warning_codes jsonb, warning_details jsonb, blocker_codes jsonb, blocker_details jsonb, grouping_identity text)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
with raw as materialized (
  select x.ordinality::integer request_no,x.value request_json,
    nullif(btrim(coalesce(x.value->>'request_key','')),'') request_key,
    case when coalesce(x.value->>'invoice_id','')~*
      '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      then(x.value->>'invoice_id')::uuid end invoice_id,
    case when jsonb_typeof(x.value->'recipient_set')='array'
      then x.value->'recipient_set' else '[]'::jsonb end requested_to,
    case when jsonb_typeof(x.value->'cc')='array'
      then x.value->'cc' else '[]'::jsonb end requested_cc,
    case when jsonb_typeof(x.value->'bcc')='array'
      then x.value->'bcc' else '[]'::jsonb end requested_bcc,
    upper(coalesce(nullif(btrim(x.value->>'delivery_policy'),''),
      'ATTACH')) delivery_policy,
    coalesce(nullif(btrim(x.value->>'template_version'),''),
      'invoice-delivery-v1') template_version
  from jsonb_array_elements(case when jsonb_typeof(p_requests)='array'
    then p_requests else '[]'::jsonb end)
    with ordinality x(value,ordinality)
  where jsonb_typeof(x.value)='object'
),
request_counts as materialized (
  select r.request_key,count(*)::integer request_key_count
  from raw r
  where r.request_key is not null
  group by r.request_key
),
facts as materialized (
  select r.*,coalesce(rc.request_key_count,0) request_key_count,
    i.client_id,
    coalesce(nullif(i.header_snapshot_json#>>'{meta,invoice_week_start}',''),
      nullif(i.header_snapshot_json->>'invoice_week_start',''),'NO_WEEK')
      invoice_week_identity,
    coalesce(i.do_not_send,false) invoice_do_not_send,
    lower(coalesce(i.header_snapshot_json#>>'{meta,self_bill}',
      i.header_snapshot_json->>'self_bill','false'))
      in('true','t','1','yes') invoice_self_bill,
    case when private._candidate_feature_enabled_current_v1('candidate_expense_invoice_routing_v1')
      then upper(coalesce(nullif(btrim(i.header_snapshot_json->>'invoice_stream'),''),
        case when lower(coalesce(i.header_snapshot_json#>>'{meta,self_bill}',
          i.header_snapshot_json->>'self_bill','false')) in('true','t','1','yes')
          then 'SELF_BILL' else 'NORMAL' end))
      else case when lower(coalesce(i.header_snapshot_json#>>'{meta,self_bill}',
          i.header_snapshot_json->>'self_bill','false')) in('true','t','1','yes')
          then 'SELF_BILL' else 'NORMAL' end
    end invoice_stream,
    nullif(btrim(coalesce(i.header_snapshot_json->>
      'client_primary_invoice_email',cl.primary_invoice_email,'')),'')
      primary_email,
    cs.id client_settings_id,cs.effective_from client_settings_effective_from,
    cs.send_manual_invoices_to_different_email client_alt_enabled,
    nullif(btrim(cs.manual_invoices_alt_email_address),'') client_alt_email,
    lower(nullif(btrim(cs.candidate_expense_invoice_email),''))
      client_expense_invoice_email,
    cs.self_bill_no_invoices_sent
  from raw r
  left join request_counts rc on rc.request_key=r.request_key
  left join public.invoices i on i.id=r.invoice_id
  left join public.clients cl on cl.id=i.client_id
  left join lateral (
    select s.id,s.effective_from,s.send_manual_invoices_to_different_email,
      s.manual_invoices_alt_email_address,s.candidate_expense_invoice_email,
      s.self_bill_no_invoices_sent
    from public.client_settings s
    where p_evaluation_date is not null
      and s.client_id=i.client_id
      and(s.effective_from is null or s.effective_from<=p_evaluation_date)
    order by s.effective_from desc nulls last,s.updated_at desc nulls last,
      s.created_at desc nulls last,s.id desc
    limit 1
  ) cs on true
),
line_routes as materialized (
  select f.request_no,f.request_key,f.invoice_id,l.timesheet_id,
    f.client_expense_invoice_email,
    l.timesheet_id is not null and ts.timesheet_id is null
      missing_current_timesheet,
    (
      (coalesce(ts.is_adjustment,false) or coalesce(cw.is_adjustment,false))
      and(
        upper(coalesce(ts.submission_mode::text,'')) in('MANUAL','QR')
        or nullif(btrim(coalesce(ts.qr_status::text,'')),'') is not null
        or nullif(btrim(coalesce(ts.qr_token::text,'')),'') is not null)
      and not(
        coalesce(ts.is_adjustment,false)
        and(
          left(upper(coalesce(ts.adjustment_origin::text,'')),7)='IMPORT_'
          or ts.correction_id is not null
          or nullif(btrim(coalesce(ts.correction_kind::text,'')),'') is not null))
    ) manual_adjustment,
    coalesce(ts.contract_id,cw.contract_id) contract_id
  from facts f
  join public.invoice_lines l on l.invoice_id=f.invoice_id
  left join public.timesheets ts
    on ts.timesheet_id=l.timesheet_id and ts.is_current
  left join lateral (
    select coalesce(bool_or(coalesce(w.is_adjustment,false)),false)
        is_adjustment,
      (array_agg(w.contract_id order by w.updated_at desc nulls last,
        w.created_at desc nulls last,w.id desc)
        filter(where w.contract_id is not null))[1] contract_id
    from public.contract_weeks w
    where w.timesheet_id=l.timesheet_id
  ) cw on true
  where l.timesheet_id is not null
),
contract_routes as materialized (
  select lr.*,ct.id is null contract_missing,
    coalesce(ct.overrideclientsettings,false)
      and coalesce(ct.send_manual_invoices_to_different_email,false)
      override_enabled,
    nullif(btrim(coalesce(ct.manual_invoices_alt_email_address,'')),'')
      alt_email,
    lower(nullif(btrim(coalesce(ct.manual_invoices_alt_email_address,'')),'') )
      contract_alt_policy_email,
    lower(coalesce(
      nullif(btrim(ct.candidate_expense_invoice_email_override),''),
      lr.client_expense_invoice_email)) effective_expense_invoice_email
  from line_routes lr
  left join public.contracts ct on ct.id=lr.contract_id
),
route_rollup as materialized (
  select f.request_no,f.request_key,f.invoice_id,
    coalesce(bool_or(cr.missing_current_timesheet),false)
      missing_current_timesheet,
    coalesce(bool_or(cr.manual_adjustment),false) has_manual_adjustment,
    coalesce(bool_or(cr.manual_adjustment and cr.contract_id is null),false)
      missing_contract,
    coalesce(bool_or(cr.manual_adjustment and cr.contract_missing),false)
      contract_data_missing,
    coalesce(bool_or(cr.manual_adjustment and cr.override_enabled),false)
      has_contract_override,
    coalesce(bool_or(cr.manual_adjustment and cr.override_enabled
      and cr.alt_email is null),false) contract_alt_missing,
    count(distinct lower(cr.alt_email))
      filter(where cr.manual_adjustment and cr.override_enabled
        and cr.alt_email is not null) contract_alt_count,
    min(lower(cr.alt_email))
      filter(where cr.manual_adjustment and cr.override_enabled
        and cr.alt_email is not null) contract_alt_email,
    count(distinct cr.effective_expense_invoice_email)
      filter(where cr.effective_expense_invoice_email is not null)
      expense_email_count,
    min(cr.effective_expense_invoice_email)
      filter(where cr.effective_expense_invoice_email is not null)
      expense_invoice_email,
    coalesce(jsonb_agg(distinct jsonb_build_object(
      'contract_id',cr.contract_id,
      'override_client_settings',cr.override_enabled,
      'manual_invoice_alternate_email',cr.contract_alt_policy_email,
      'contract_missing',cr.contract_missing)
      order by jsonb_build_object(
        'contract_id',cr.contract_id,
        'override_client_settings',cr.override_enabled,
        'manual_invoice_alternate_email',cr.contract_alt_policy_email,
        'contract_missing',cr.contract_missing))
      filter(where cr.manual_adjustment and cr.contract_id is not null),
      '[]'::jsonb) contract_setting_identities
  from facts f
  left join contract_routes cr
    on cr.request_no=f.request_no and cr.invoice_id=f.invoice_id
  group by f.request_no,f.request_key,f.invoice_id
),
chosen as materialized (
  select f.*,rr.missing_current_timesheet,rr.has_manual_adjustment,
    rr.missing_contract,rr.contract_data_missing,rr.has_contract_override,
    rr.contract_alt_missing,rr.contract_alt_count,rr.contract_alt_email,
    rr.expense_email_count,rr.expense_invoice_email,
    rr.contract_setting_identities,
    case
      when private._candidate_feature_enabled_current_v1('candidate_expense_invoice_routing_v1')
       and f.invoice_stream='EXPENSE' then 'EXPENSE_INVOICE_EMAIL'
      when jsonb_array_length(f.requested_to)>0 then 'REQUESTED'
      when rr.contract_alt_count=1 then 'CONTRACT_MANUAL_ALTERNATE'
      when rr.has_manual_adjustment and not rr.has_contract_override
        and coalesce(f.client_alt_enabled,false)
        then 'CLIENT_MANUAL_ALTERNATE'
      else 'CLIENT_PRIMARY'
    end route_source,
    case
      when private._candidate_feature_enabled_current_v1('candidate_expense_invoice_routing_v1')
       and f.invoice_stream='EXPENSE'
        then jsonb_build_array(rr.expense_invoice_email)
      when jsonb_array_length(f.requested_to)>0 then f.requested_to
      when rr.contract_alt_count=1 then jsonb_build_array(rr.contract_alt_email)
      when rr.has_manual_adjustment and not rr.has_contract_override
        and coalesce(f.client_alt_enabled,false)
        then jsonb_build_array(f.client_alt_email)
      else jsonb_build_array(f.primary_email)
    end selected_to,
    f.invoice_do_not_send
      or(f.invoice_stream<>'EXPENSE' and f.invoice_self_bill
        and coalesce(f.self_bill_no_invoices_sent,true))
      delivery_suppressed,
    case
      when f.invoice_do_not_send then 'DO_NOT_SEND'
      when f.invoice_stream<>'EXPENSE' and f.invoice_self_bill
        and coalesce(f.self_bill_no_invoices_sent,true)
        then 'SELF_BILL_SUPPRESSED'
    end suppression_reason
  from facts f
  left join route_rollup rr on rr.request_no=f.request_no
),
canonical as materialized (
  select c.*,
    coalesce((select jsonb_agg(e order by e) from(
      select distinct lower(btrim(v.value)) e
      from jsonb_array_elements_text(c.selected_to) v(value)
      where nullif(btrim(v.value),'') is not null
        and btrim(v.value)~*'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$'
    ) q),'[]'::jsonb) to_json,
    coalesce((select jsonb_agg(e order by e) from(
      select distinct lower(btrim(v.value)) e
      from jsonb_array_elements_text(c.requested_cc) v(value)
      where nullif(btrim(v.value),'') is not null
        and btrim(v.value)~*'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$'
    ) q),'[]'::jsonb) cc_json,
    coalesce((select jsonb_agg(e order by e) from(
      select distinct lower(btrim(v.value)) e
      from jsonb_array_elements_text(c.requested_bcc) v(value)
      where nullif(btrim(v.value),'') is not null
        and btrim(v.value)~*'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$'
    ) q),'[]'::jsonb) bcc_json,
    (select count(*)::integer
      from jsonb_array_elements_text(c.selected_to) v(value)
      where nullif(btrim(v.value),'') is not null
        and btrim(v.value)!~*'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$')
      invalid_to,
    (select count(*)::integer
      from jsonb_array_elements_text(c.requested_cc) v(value)
      where nullif(btrim(v.value),'') is not null
        and btrim(v.value)!~*'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$')
      invalid_cc,
    (select count(*)::integer
      from jsonb_array_elements_text(c.requested_bcc) v(value)
      where nullif(btrim(v.value),'') is not null
        and btrim(v.value)!~*'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$')
      invalid_bcc
  from chosen c
),
classified as materialized (
  select c.*,
    array_remove(array[
      case when c.missing_current_timesheet then
        'EMAIL_ROUTING_CHECK_FAILED' end,
      case when c.has_manual_adjustment and c.missing_contract then
        'CONTRACT_ROUTING_CHECK_FAILED' end,
      case when c.has_manual_adjustment and c.contract_data_missing then
        'CONTRACT_ROUTING_CHECK_FAILED' end,
      case when c.has_manual_adjustment and c.contract_alt_missing then
        'CONTRACT_MANUAL_EMAIL_MISSING' end,
      case when c.has_manual_adjustment and c.contract_alt_count>1 then
        'CONTRACT_MANUAL_EMAIL_CONFLICT' end,
      case when c.has_manual_adjustment and not c.has_contract_override
        and coalesce(c.client_alt_enabled,false) and c.client_alt_email is null
        then 'CLIENT_MANUAL_EMAIL_MISSING' end
    ],null)::text[] warnings,
    array_remove(array[
      case when c.request_key is null then 'REQUEST_KEY_REQUIRED' end,
      case when c.request_key_count>1 then 'REQUEST_KEY_DUPLICATE' end,
      case when c.invoice_id is null then 'INVOICE_ID_INVALID' end,
      case when c.client_id is null and c.invoice_id is not null
        then 'INVOICE_NOT_FOUND' end,
      case when p_evaluation_date is null then 'EVALUATION_DATE_REQUIRED' end,
      case when c.delivery_policy not in('ATTACH','SPLIT','SECURE_LINK')
        then 'DELIVERY_POLICY_INVALID' end,
      case when private._candidate_feature_enabled_current_v1('candidate_expense_invoice_routing_v1')
       and c.invoice_stream='EXPENSE'
        and (coalesce(c.expense_email_count,0)<>1
          or jsonb_array_length(c.to_json)=0)
        then 'EXPENSE_INVOICE_EMAIL_REQUIRED' end,
      case when c.invoice_stream<>'EXPENSE'
        and not c.delivery_suppressed and jsonb_array_length(c.to_json)=0
        then 'MISSING_RECIPIENT' end,
      case when c.invalid_to>0 then 'INVALID_TO_RECIPIENT' end,
      case when c.invalid_cc>0 then 'INVALID_CC_RECIPIENT' end,
      case when c.invalid_bcc>0 then 'INVALID_BCC_RECIPIENT' end
    ],null)::text[] blockers
  from canonical c
),
hashed as materialized (
  select c.*,
    encode(digest(jsonb_build_object(
      'to',case when c.delivery_suppressed then '[]'::jsonb else c.to_json end,
      'cc',case when c.delivery_suppressed then '[]'::jsonb else c.cc_json end,
      'bcc',case when c.delivery_suppressed then '[]'::jsonb else c.bcc_json end
    )::text,'sha256'),'hex') calculated_recipient_set_hash
  from classified c
),
policy_hashed as materialized (
  select h.*,
    encode(digest((jsonb_build_object(
      'policy_version',case
        when private._candidate_feature_enabled_current_v1('candidate_expense_invoice_routing_v1')
          then 'INVOICE_DELIVERY_ROUTE_V6'
        else 'INVOICE_DELIVERY_ROUTE_V5'
      end,
      'client_id',h.client_id,
      'invoice_week_identity',h.invoice_week_identity,
      'recipient_set_hash',h.calculated_recipient_set_hash,
      'to',case when h.delivery_suppressed then '[]'::jsonb else h.to_json end,
      'cc',case when h.delivery_suppressed then '[]'::jsonb else h.cc_json end,
      'bcc',case when h.delivery_suppressed then '[]'::jsonb else h.bcc_json end,
      'route_source',h.route_source,
      'client_settings_id',h.client_settings_id,
      'client_settings_effective_from',h.client_settings_effective_from,
      'contract_settings',h.contract_setting_identities,
      'self_bill',h.invoice_self_bill,
      'do_not_send',h.invoice_do_not_send,
      'delivery_suppressed',h.delivery_suppressed,
      'suppression_reason',h.suppression_reason,
      'warnings',to_jsonb(h.warnings),'blockers',to_jsonb(h.blockers),
      'template_version',h.template_version,
      'delivery_policy',h.delivery_policy
    ) || case
      when private._candidate_feature_enabled_current_v1('candidate_expense_invoice_routing_v1')
        then jsonb_build_object('invoice_stream',h.invoice_stream)
      else '{}'::jsonb
    end)::text,'sha256'),'hex') calculated_route_policy_hash
  from hashed h
)
select p.request_key,p.invoice_id,
  case when p.delivery_suppressed then '[]'::jsonb else p.to_json end,
  case when p.delivery_suppressed then '[]'::jsonb else p.cc_json end,
  case when p.delivery_suppressed then '[]'::jsonb else p.bcc_json end,
  p.invalid_to,p.invalid_cc,p.invalid_bcc,p.calculated_recipient_set_hash,
  p.calculated_route_policy_hash,p.route_source,p.client_settings_id,
  p.contract_setting_identities,p_evaluation_date,p.client_id,
  p.invoice_week_identity,p.invoice_self_bill,p.invoice_do_not_send,
  p.delivery_suppressed,p.suppression_reason,to_jsonb(p.warnings),
  jsonb_build_object(
    'missing_current_timesheet',p.missing_current_timesheet,
    'manual_adjustment',p.has_manual_adjustment,
    'contract_override_count',p.contract_alt_count)
    || case
      when private._candidate_feature_enabled_current_v1('candidate_expense_invoice_routing_v1')
        then jsonb_build_object(
          'invoice_stream',p.invoice_stream,
          'expense_email_count',p.expense_email_count)
      else '{}'::jsonb
    end,
  to_jsonb(p.blockers),
  jsonb_build_object(
    'invalid_to_count',p.invalid_to,'invalid_cc_count',p.invalid_cc,
    'invalid_bcc_count',p.invalid_bcc),
  p.calculated_route_policy_hash
from policy_hashed p
order by p.request_key nulls first,p.invoice_id nulls first,p.request_no;
$function$;

alter function private._invoice_delivery_routes_batch(jsonb, date) owner to "postgres";
revoke all privileges on function private._invoice_delivery_routes_batch(jsonb, date) from PUBLIC, anon, authenticated, service_role, authenticator, supabase_admin;
grant execute on function private._invoice_delivery_routes_batch(jsonb, date) to "postgres";
grant execute on function private._invoice_delivery_routes_batch(jsonb, date) to service_role;

