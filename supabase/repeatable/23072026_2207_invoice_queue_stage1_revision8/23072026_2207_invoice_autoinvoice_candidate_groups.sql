drop function if exists public.invoice_autoinvoice_candidate_groups(integer);
create function public.invoice_autoinvoice_candidate_groups(p_limit integer default 5000)
returns table(
  client_id uuid,
  invoice_week_start date,
  source_ids uuid[],
  source_revision_hash text,
  consolidation_mode text,
  stream text,
  auto_invoice_policy_origin text,
  canonical_source_members jsonb,
  eligible_for_submission boolean,
  blocker_code text,
  correction_validation jsonb
)
language sql
stable
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
with anchor as materialized (
  select now() evaluation_utc,(now() at time zone 'Europe/London')::date today
),
eligible as materialized (
  select distinct tf.timesheet_id,
    case when authority.settings_json#>>'{sources,contract_governed_settings}'='CONTRACT_OVERRIDE'
      then 'CONTRACT' else 'CLIENT_DEFAULT' end policy_origin
  from public.timesheets_financials tf
  join public.timesheets ts
    on ts.timesheet_id=tf.timesheet_id and ts.is_current and ts.revoked_at is null
  cross join lateral (
    select private._timesheet_settings_authority_frozen_v1(ts.timesheet_id)
      as settings_json
  ) authority
  cross join anchor a
  where tf.is_current and tf.processing_status='READY_FOR_INVOICE'
    and not tf.is_stale and tf.locked_by_invoice_id is null
    and tf.paid_at_utc is null and tf.client_id is not null
    and ts.week_ending_date is not null
    and not(
      upper(coalesce(ts.submission_mode::text,''))='QR'
      and(nullif(ts.qr_signed_hash,'') is null
        or ts.qr_signed_at_utc is null))
    and(
      (coalesce(tf.mileage_pay_ex_vat,0)=0
        and coalesce(tf.mileage_charge_ex_vat,0)=0)
      or exists(
        select 1 from public.timesheet_evidence e
        join public.invoice_document_assets asset
          on asset.id=e.document_asset_id
         and asset.status not in(
           'UNSUPPORTED','CORRUPT','MISSING','FAILED','SUPERSEDED')
        where e.timesheet_id=tf.timesheet_id
          and upper(coalesce(e.kind,''))='MILEAGE'
          and nullif(e.storage_key,'') is not null))
    and(
      (coalesce(tf.expenses_pay_ex_vat,0)=0
        and coalesce(tf.expenses_charge_ex_vat,0)=0
        and coalesce(tf.travel_pay_ex_vat,0)=0
        and coalesce(tf.travel_charge_ex_vat,0)=0
        and coalesce(tf.accommodation_pay_ex_vat,0)=0
        and coalesce(tf.accommodation_charge_ex_vat,0)=0)
      or exists(
        select 1 from public.timesheet_evidence e
        join public.invoice_document_assets asset
          on asset.id=e.document_asset_id
         and asset.status not in(
           'UNSUPPORTED','CORRUPT','MISSING','FAILED','SUPERSEDED')
        where e.timesheet_id=tf.timesheet_id
          and upper(coalesce(e.kind,'')) in(
            'TRAVEL','ACCOMMODATION','OTHER','EXPENSE','EXPENSES')
          and nullif(e.storage_key,'') is not null))
    and not exists(
      select 1
      from public.timesheet_evidence e
      join public.invoice_document_assets asset
        on asset.id=e.document_asset_id
      where e.timesheet_id=tf.timesheet_id
        and asset.status in(
          'UNSUPPORTED','CORRUPT','MISSING','FAILED','SUPERSEDED'))
    and coalesce((authority.settings_json#>>'{values,auto_invoice}')::boolean,false)
    and exists(
      select 1
      from lateral (
        select null::jsonb segment
        where coalesce(tf.invoice_breakdown_json->>'mode','')<>'SEGMENTS'
          or jsonb_array_length(case
            when jsonb_typeof(tf.invoice_breakdown_json->'segments')='array'
              then tf.invoice_breakdown_json->'segments' else '[]'::jsonb end)=0
        union all
        select x.value
        from jsonb_array_elements(case
          when jsonb_typeof(tf.invoice_breakdown_json->'segments')='array'
            then tf.invoice_breakdown_json->'segments' else '[]'::jsonb end) x(value)
        where coalesce(tf.invoice_breakdown_json->>'mode','')='SEGMENTS'
          and nullif(x.value->>'invoice_locked_invoice_id','') is null
      ) available
      where case
        when pg_input_is_valid(
          coalesce(available.segment->>'invoice_target_week_start',''),'date')
          then case
            when(available.segment->>'invoice_target_week_start')::date<>
                (ts.week_ending_date-6)
              then(available.segment->>'invoice_target_week_start')::date<=a.today
            else ts.week_ending_date<a.today
          end
        else ts.week_ending_date<a.today end)
  order by tf.timesheet_id
),
command as materialized (
  select jsonb_build_array(jsonb_build_object(
    'command_type','GENERATE_AUTO','source_ids',
    coalesce(jsonb_agg(e.timesheet_id order by e.timesheet_id),'[]'::jsonb),
    'allow_early',false)) commands
  from eligible e
),
resolved_unchecked as materialized (
  select r.*
  from command c cross join anchor a
  cross join lateral private._invoice_generation_resolve_command_groups(
    c.commands,null,a.evaluation_utc) r
  where r.blocker_code is null
),
vat_policy as materialized (
  select v.*
  from private._invoice_generation_vat_policy_batch(coalesce((
    select jsonb_agg(jsonb_build_object(
      'source_member_key',m.value->>'source_member_key',
      'source_type',m.value->>'source_type',
      'source_id',m.value->>'source_id',
      'timesheet_id',m.value->>'related_timesheet_id',
      'segment_id',m.value->>'segment_id',
      'effective_date',r.effective_settings_date)
      order by r.group_key,m.value->>'source_member_key')
    from resolved_unchecked r
    cross join lateral jsonb_array_elements(
      r.canonical_source_members) m(value)
  ),'[]'::jsonb)) v
),
reference_policy as materialized (
  select ref.*
  from private._invoice_source_reference_validate_batch(coalesce((
    select jsonb_agg(jsonb_build_object(
      'source_member_key',m.value->>'source_member_key',
      'source_type',m.value->>'source_type',
      'source_id',m.value->>'source_id',
      'related_timesheet_id',m.value->>'related_timesheet_id',
      'segment_id',m.value->>'segment_id',
      'target_invoice_week',r.target_invoice_week,
      'invoice_stream',r.invoice_stream,
      'consolidation_mode',r.consolidation_mode)
      order by r.group_key,m.value->>'source_member_key')
    from resolved_unchecked r
    cross join lateral jsonb_array_elements(
      r.canonical_source_members) m(value)
  ),'[]'::jsonb)) ref
),
correction_scopes as materialized (
  select coalesce(jsonb_agg(jsonb_build_object(
    'request_key','autoinvoice-candidate:'||r.group_key,
    'scope_key',r.group_key,
    'validation_purpose','AUTOMATIC_CANDIDATE',
    'expected_client_id',r.client_id,
    'expected_contract_id',case when cardinality(r.contract_ids)=1
      then r.contract_ids[1] end,
    'natural_source_week',case when cardinality(r.natural_source_weeks)=1
      then r.natural_source_weeks[1] end,
    'target_invoice_week',r.target_invoice_week,
    'expected_invoice_stream',r.invoice_stream,
    'planned_members',coalesce((select jsonb_agg(jsonb_build_object(
      'timesheet_id',m.value->>'related_timesheet_id',
      'source_type',m.value->>'source_type',
      'source_id',m.value->>'source_id',
      'source_member_key',m.value->>'source_member_key',
      'segment_id',m.value->>'segment_id',
      'target_invoice_week',r.target_invoice_week,
      'vat_rate_pct',v.vat_rate)
      order by m.value->>'source_member_key')
      from jsonb_array_elements(r.canonical_source_members) m(value)
      left join vat_policy v
        on v.source_member_key=m.value->>'source_member_key'),
      '[]'::jsonb)) order by r.group_key),'[]'::jsonb) scopes
  from resolved_unchecked r
),
correction_validation as materialized (
  select c.*
  from correction_scopes s
  cross join lateral private._invoice_correction_validate_batch(
    s.scopes,(select today from anchor)) c
),
correction_group_results as materialized (
  select c.scope_key group_key,c.valid,c.blocker_code,c.blocker_codes,
    c.detail_json details
  from correction_validation c
),resolved as materialized (
  select r.*
  from resolved_unchecked r
  where not exists(
    select 1
    from jsonb_array_elements(r.canonical_source_members) m(value)
    left join vat_policy v
      on v.source_member_key=m.value->>'source_member_key'
    left join reference_policy ref
      on ref.source_member_key=m.value->>'source_member_key'
    left join public.v_ts_invoice_precheck pc
      on pc.timesheet_id=case when pg_input_is_valid(
        coalesce(m.value->>'related_timesheet_id',
          m.value->>'source_id'),'uuid')
        then coalesce(m.value->>'related_timesheet_id',
          m.value->>'source_id')::uuid end
    where coalesce(v.valid,false) is not true
       or(coalesce(pc.require_reference_to_invoice,false)
          and coalesce(ref.reference_ready,false) is not true))
),
policy as materialized (
  select r.group_key,
    case when bool_or(e.policy_origin='CONTRACT') then 'CONTRACT'
      else 'CLIENT_DEFAULT' end policy_origin
  from resolved r
  cross join unnest(r.canonical_source_ids) source_id
  join eligible e on e.timesheet_id=source_id
  group by r.group_key
)
select r.client_id,r.target_invoice_week,r.canonical_source_ids,
  r.source_revision_hash,r.consolidation_mode,r.invoice_stream,p.policy_origin,
  r.canonical_source_members,
  coalesce(c.valid,true),
  case when coalesce(c.valid,true) then null
    else coalesce(c.blocker_code,'INVOICE_CORRECTION_UNIT_INVALID') end,
  coalesce(c.details,'[]'::jsonb)
from resolved r join policy p using(group_key)
left join correction_group_results c using(group_key)
where not exists(
  select 1 from public.invoice_operation_chunks c
  join public.invoice_operations o on o.id=c.operation_id
  where c.chunk_type='GENERATION_GROUP'
    and c.payload_json->>'group_key'=r.group_key
    and c.payload_json->>'source_revision'=r.source_revision_hash
    and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
    and o.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED'))
order by r.target_invoice_week nulls last,r.client_id,r.invoice_stream,r.group_key
limit greatest(0,least(coalesce(p_limit,5000),20000));
$function$;

revoke all on function public.invoice_autoinvoice_candidate_groups(integer)
  from public,anon,authenticated;
grant execute on function public.invoice_autoinvoice_candidate_groups(integer)
  to service_role;
