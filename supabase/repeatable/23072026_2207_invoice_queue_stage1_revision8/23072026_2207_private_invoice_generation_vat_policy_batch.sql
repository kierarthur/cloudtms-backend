drop function if exists private._invoice_generation_vat_policy_batch(jsonb);

create function private._invoice_generation_vat_policy_batch(
  p_sources jsonb
) returns table(
  source_member_key text,
  source_type text,
  source_id uuid,
  timesheet_id uuid,
  segment_id text,
  vat_rate numeric,
  policy_source text,
  effective_date date,
  policy_identity text,
  valid boolean,
  blocker_code text,
  detail_json jsonb
)
language sql
stable
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
with supplied as materialized (
  select e.ordinality::integer source_no,
    upper(btrim(coalesce(e.value->>'source_type','TIMESHEET'))) source_type,
    case when coalesce(e.value->>'source_id',
        e.value->>'timesheet_id',e.value->>'related_timesheet_id','')~*
      '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      then coalesce(e.value->>'source_id',
        e.value->>'timesheet_id',e.value->>'related_timesheet_id')::uuid end
      source_id,
    nullif(btrim(e.value->>'segment_id'),'') segment_id,
    coalesce(nullif(btrim(e.value->>'source_member_key'),''),
      encode(digest(concat_ws('|',
        coalesce(e.value->>'timesheet_id',
          e.value->>'related_timesheet_id',e.value->>'source_id',''),
        coalesce(e.value->>'segment_id','WHOLE'),
        coalesce(e.value->>'effective_date','')),'sha256'),'hex'))
      source_member_key,
    case when coalesce(e.value->>'timesheet_id',
      e.value->>'related_timesheet_id',e.value->>'source_id','')~*
      '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      then coalesce(e.value->>'timesheet_id',
        e.value->>'related_timesheet_id',e.value->>'source_id')::uuid end
      timesheet_id,
    case when coalesce(e.value->>'ordinary_rate','')~'^[+-]?[0-9]+([.][0-9]+)?$'
      then(e.value->>'ordinary_rate')::numeric end supplied_ordinary_rate,
    case when coalesce(e.value->>'effective_date','')~'^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
      and pg_input_is_valid(e.value->>'effective_date','date')
      then(e.value->>'effective_date')::date end supplied_effective_date
  from jsonb_array_elements(case when jsonb_typeof(p_sources)='array'
    then p_sources else '[]'::jsonb end) with ordinality e(value,ordinality)
  where jsonb_typeof(e.value)='object'
),
source_rows as materialized (
  select s.*,t.correction_kind,t.adjustment_origin,t.week_ending_date,
    s.supplied_effective_date resolved_effective_date,
    coalesce(s.supplied_ordinary_rate,
      case when coalesce(
        (a.authority_json#>>'{values,client_vat_chargeable}')::boolean,true
      ) then (a.authority_json#>>'{values,vat_rate_pct}')::numeric else 0 end)
      ordinary_rate,
    coalesce((a.authority_json#>>'{values,client_vat_chargeable}')::boolean,true)
      client_vat_chargeable,
    nullif(a.authority_json->>'client_settings_id','')::uuid client_settings_id,
    nullif(a.authority_json->>'client_settings_effective_from','')::date
      client_settings_effective_from,
    nullif(a.authority_json->>'finance_settings_id','')::uuid finance_settings_id,
    nullif(a.authority_json->>'finance_settings_date_from','')::date
      finance_settings_date_from,
    coalesce(
      t.candidate_hint_text->'correction_financials_policy_envelope',
      f.policy_snapshot_json->'correction_financials_policy_envelope',
      f.rate_source_refs_json->'correction_financials_policy_envelope') envelope
  from supplied s
  left join public.timesheets t
    on t.timesheet_id=s.timesheet_id and t.is_current
  left join public.timesheets_financials f
    on f.timesheet_id=s.timesheet_id and f.is_current
  left join lateral (
    select private._timesheet_settings_authority_frozen_v1(t.timesheet_id)
      authority_json
    where t.timesheet_id is not null
  ) a on true
),
classified as materialized (
  select s.*,
    upper(btrim(coalesce(s.correction_kind,''))) kind,
    upper(btrim(coalesce(s.adjustment_origin,''))) origin,
    case
      when upper(btrim(coalesce(s.correction_kind,''))) in(
        'CHANGED_HOURS_REVERSAL','CANCELLATION_REVERSAL')
        then s.envelope->'reversal'
      when upper(btrim(coalesce(s.correction_kind,''))) in(
        'CHANGED_HOURS_REPLACEMENT','CANCELLATION_REPLACEMENT')
        then s.envelope->'replacement'
    end leg,
    coalesce(
      upper(btrim(coalesce(s.adjustment_origin,''))) in(
        'IMPORT_CORRECTION','IMPORT_CANCELLATION',
        'HEALTHROSTER_CHANGED_HOURS','NHSP_CHANGED_HOURS',
        'HEALTHROSTER_CANCELLATION','NHSP_CANCELLATION')
      and upper(btrim(coalesce(s.correction_kind,''))) in(
        'CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT',
        'CANCELLATION_REVERSAL','CANCELLATION_REPLACEMENT')
      and jsonb_typeof(s.envelope)='object'
      and s.envelope->>'policy_schema_version'=
        'IMPORT_CORRECTION_FINANCIALS_POLICY_V2'
      and s.envelope->>'route_family'='IMPORT_AUTHORITATIVE'
      and lower(coalesce(s.envelope#>>'{classification,canonical}','false'))
        in('true','t','1','yes')
      and nullif(s.envelope#>>'{operation,operation_id}','') is not null
      and nullif(s.envelope->>'correction_chain_id','') is not null
      and s.envelope->>'envelope_fingerprint'=
        encode(digest(convert_to(
          (s.envelope-'envelope_fingerprint')::text,'UTF8'),'sha256'),'hex')
      and(
        (upper(btrim(coalesce(s.correction_kind,''))) like 'CHANGED_HOURS_%'
          and s.envelope#>>'{operation,correction_action}'='CHANGED_HOURS')
        or
        (upper(btrim(coalesce(s.correction_kind,''))) like 'CANCELLATION_%'
          and s.envelope#>>'{operation,correction_action}'='CANCELLATION')),
      false) authoritative
  from source_rows s
),
policy as materialized (
  select c.*,c.leg->'invoice_policy' invoice_policy,
    c.leg->'tsfin_policy' tsfin_policy,
    case when coalesce(c.leg#>>'{invoice_policy,applied_vat_rate_pct}','')
      ~'^[+-]?[0-9]+([.][0-9]+)?$'
      then(c.leg#>>'{invoice_policy,applied_vat_rate_pct}')::numeric end
      applied_rate,
    case when lower(coalesce(
      c.leg#>>'{invoice_policy,invoice_vat_chargeable}','')) in(
        'true','t','1','yes','false','f','0','no')
      then lower(c.leg#>>'{invoice_policy,invoice_vat_chargeable}')
        in('true','t','1','yes') end vat_chargeable
  from classified c
)
select p.source_member_key,p.source_type,p.source_id,p.timesheet_id,p.segment_id,
  case when not p.authoritative then p.ordinary_rate
    when validation.blocker_code is null then p.applied_rate end vat_rate,
  case when p.authoritative then 'IMPORT_CORRECTION_POLICY'
    else 'ORDINARY_FINANCE_POLICY' end policy_source,
  p.resolved_effective_date effective_date,
  encode(digest(jsonb_build_object(
    'source_member_key',p.source_member_key,
    'effective_date',p.resolved_effective_date,
    'authoritative',p.authoritative,
    'vat_chargeable',p.vat_chargeable,
    'ordinary_rate',p.ordinary_rate,
    'applied_rate',p.applied_rate,
    'client_settings_id',p.client_settings_id,
    'client_settings_effective_from',p.client_settings_effective_from,
    'finance_settings_id',p.finance_settings_id,
    'finance_settings_date_from',p.finance_settings_date_from,
    'envelope_fingerprint',p.envelope->>'envelope_fingerprint',
    'invoice_policy_fingerprint',
      p.invoice_policy->>'invoice_policy_fingerprint')::text,'sha256'),'hex')
    policy_identity,
  p.timesheet_id is not null and p.resolved_effective_date is not null
    and p.ordinary_rate is not null
    and validation.blocker_code is null valid,
  case
    when p.timesheet_id is null then 'VAT_SOURCE_ID_INVALID'
    when p.resolved_effective_date is null then 'VAT_EFFECTIVE_DATE_REQUIRED'
    when p.ordinary_rate is null or p.ordinary_rate<0 or p.ordinary_rate>100
      then 'ORDINARY_VAT_RATE_INVALID'
    else validation.blocker_code
  end blocker_code,
  jsonb_build_object(
    'source_member_key',p.source_member_key,
    'authoritative_correction',p.authoritative,
    'effective_date',p.resolved_effective_date,
    'client_settings_id',p.client_settings_id,
    'client_settings_effective_from',p.client_settings_effective_from,
    'finance_settings_id',p.finance_settings_id,
    'finance_settings_date_from',p.finance_settings_date_from,
    'ordinary_rate',p.ordinary_rate,'applied_rate',p.applied_rate,
    'invoice_vat_chargeable',p.vat_chargeable,
    'correction_kind',nullif(p.kind,''),
    'envelope_fingerprint',p.envelope->>'envelope_fingerprint',
    'leg_fingerprint',p.leg->>'leg_fingerprint',
    'invoice_policy_fingerprint',
      p.invoice_policy->>'invoice_policy_fingerprint')
from policy p
cross join lateral (
  select case
    when not p.authoritative then null
    when jsonb_typeof(p.leg)<>'object' then 'IMPORT_CORRECTION_POLICY_LEG_MISSING'
    when nullif(p.leg->>'leg_fingerprint','') is null
      or p.leg->>'leg_fingerprint'<>encode(digest(convert_to(
        (p.leg-'leg_fingerprint')::text,'UTF8'),'sha256'),'hex')
      then 'IMPORT_CORRECTION_POLICY_LEG_FINGERPRINT_INVALID'
    when jsonb_typeof(p.tsfin_policy)<>'object'
      or nullif(p.tsfin_policy->>'tsfin_policy_fingerprint','') is null
      or p.tsfin_policy->>'tsfin_policy_fingerprint'<>encode(digest(convert_to(
        (p.tsfin_policy-'tsfin_policy_fingerprint')::text,'UTF8'),
        'sha256'),'hex')
      then 'IMPORT_CORRECTION_TSFIN_SUBPOLICY_FINGERPRINT_INVALID'
    when jsonb_typeof(p.invoice_policy)<>'object'
      or nullif(p.invoice_policy->>'invoice_policy_fingerprint','') is null
      or p.invoice_policy->>'invoice_policy_fingerprint'<>encode(digest(convert_to(
        (p.invoice_policy-'invoice_policy_fingerprint')::text,'UTF8'),
        'sha256'),'hex')
      or p.invoice_policy->>'invoice_stream'
        is distinct from p.envelope->>'invoice_stream'
      then 'IMPORT_CORRECTION_INVOICE_SUBPOLICY_FINGERPRINT_INVALID'
    when lower(coalesce(p.invoice_policy->>'applicable','false'))
      not in('true','t','1','yes')
      or p.invoice_policy->>'materialisation_stage'<>'INVOICE_GENERATION'
      or lower(coalesce(
        p.invoice_policy->>'final_invoice_vat_materialised','true'))
        not in('false','f','0','no')
      or nullif(p.invoice_policy->>'source_vat_rate_pct','') is null
      or p.vat_chargeable is null or p.applied_rate is null
      then 'IMPORT_CORRECTION_INVOICE_VAT_UNRESOLVED'
    when p.applied_rate<0 or p.applied_rate>100
      then 'IMPORT_CORRECTION_INVOICE_VAT_INVALID'
    when p.vat_chargeable is false and p.applied_rate<>0
      then 'IMPORT_CORRECTION_INVOICE_VAT_CHARGEABILITY_MISMATCH'
  end blocker_code
) validation
order by p.source_no;
$function$;

revoke all on function private._invoice_generation_vat_policy_batch(jsonb)
  from public,anon,authenticated;
grant execute on function private._invoice_generation_vat_policy_batch(jsonb)
  to service_role;
