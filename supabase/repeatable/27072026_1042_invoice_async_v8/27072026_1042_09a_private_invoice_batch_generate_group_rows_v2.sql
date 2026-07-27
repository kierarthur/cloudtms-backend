create or replace function private._invoice_batch_generate_group_rows_v2(
  p_allow_early boolean default false,
  p_limit integer default 25001,
  p_scope_keys text[] default null,
  p_now_utc timestamptz default now()
) returns table(
  client_id uuid,
  client_name text,
  group_json jsonb
)
language plpgsql
stable
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_scope_keys text[];
begin
  if p_scope_keys is not null then
    if cardinality(p_scope_keys)>500 then
      raise exception using
        errcode='22023',
        message='CANDIDATE_SCOPE_KEY_LIMIT_EXCEEDED';
    end if;
    if exists(
      select 1
      from unnest(p_scope_keys) value
      where value is null
        or btrim(value)=''
        or length(btrim(value))>512
    ) then
      raise exception using
        errcode='22023',
        message='CANDIDATE_SCOPE_KEY_INVALID';
    end if;
    select coalesce(array_agg(value order by value),'{}'::text[])
    into v_scope_keys
    from(
      select distinct btrim(raw_value) value
      from unnest(p_scope_keys) raw_value
    ) deduplicated;
  end if;

  return query
with anchor as materialized (
  select coalesce(p_now_utc,now()) evaluation_utc,(coalesce(p_now_utc,now()) at time zone 'Europe/London')::date today
),
source_candidates as materialized (
  select distinct tf.timesheet_id
  from public.timesheets_financials tf
  join public.timesheets ts
    on ts.timesheet_id=tf.timesheet_id and ts.is_current and ts.revoked_at is null
  cross join anchor a
  where tf.is_current and tf.client_id is not null
  order by tf.timesheet_id
),
command as materialized (
  select jsonb_build_array(jsonb_build_object(
    'command_type','GENERATE_SELECTED',
    'source_ids',coalesce(jsonb_agg(e.timesheet_id order by e.timesheet_id),'[]'::jsonb),
    'allow_early',coalesce(p_allow_early,false))) commands
  from source_candidates e
),
resolved_groups as materialized (
  select r.*
  from command c cross join anchor a
  cross join lateral private._invoice_generation_resolve_command_groups(
    c.commands,null,a.evaluation_utc) r
),
groups as materialized (
  select r.*
  from resolved_groups r
  where v_scope_keys is null
     or r.group_key=any(v_scope_keys)
  order by r.target_invoice_week nulls last,r.client_id,r.invoice_stream,r.group_key
  limit case
    when v_scope_keys is null
      then greatest(1,least(coalesce(p_limit,25001),25001))
    else 500
  end
),
group_sources as materialized (
  select g.*,m.value member,
    case when coalesce(m.value->>'source_id','')~
      '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
      then (m.value->>'source_id')::uuid end source_id,
    case when coalesce(m.value->>'related_timesheet_id',
        m.value->>'source_id','')~
      '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
      then coalesce(m.value->>'related_timesheet_id',
        m.value->>'source_id')::uuid end timesheet_id,
    nullif(m.value->>'segment_id','') segment_id
  from groups g
  cross join lateral jsonb_array_elements(g.canonical_source_members) m(value)
),
vat_policy as materialized (
  select v.*
  from private._invoice_generation_vat_policy_batch(coalesce((
    select jsonb_agg(jsonb_build_object(
      'source_member_key',gs.member->>'source_member_key',
      'source_type',gs.member->>'source_type',
      'source_id',gs.source_id,
      'timesheet_id',gs.member->>'related_timesheet_id',
      'segment_id',gs.segment_id,
      'effective_date',gs.effective_settings_date)
      order by gs.group_key,gs.member->>'source_member_key')
    from group_sources gs
  ),'[]'::jsonb)) v
),
reference_policy as materialized (
  select r.*
  from private._invoice_source_reference_validate_batch(coalesce((
    select jsonb_agg(jsonb_build_object(
      'source_member_key',gs.member->>'source_member_key',
      'source_type',gs.member->>'source_type',
      'source_id',gs.source_id,
      'related_timesheet_id',gs.timesheet_id,
      'segment_id',gs.segment_id,
      'target_invoice_week',gs.target_invoice_week,
      'invoice_stream',gs.invoice_stream,
      'consolidation_mode',gs.consolidation_mode)
      order by gs.group_key,gs.member->>'source_member_key')
    from group_sources gs
  ),'[]'::jsonb)) r
),
correction_scopes as materialized (
  select coalesce(jsonb_agg(jsonb_build_object(
    'request_key','generate-candidate:'||g.group_key,
    'scope_key',g.group_key,
    'validation_purpose','CANDIDATE_GENERATION',
    'expected_client_id',g.client_id,
    'expected_contract_id',case when cardinality(g.contract_ids)=1
      then g.contract_ids[1] end,
    'natural_source_week',case when cardinality(g.natural_source_weeks)=1
      then g.natural_source_weeks[1] end,
    'target_invoice_week',g.target_invoice_week,
    'expected_invoice_stream',g.invoice_stream,
    'planned_members',coalesce((select jsonb_agg(jsonb_build_object(
      'timesheet_id',gs.timesheet_id,
      'source_type',gs.member->>'source_type',
      'source_id',gs.source_id,
      'source_member_key',gs.member->>'source_member_key',
      'segment_id',gs.segment_id,
      'target_invoice_week',gs.target_invoice_week,
      'vat_rate_pct',vat.vat_rate)
      order by gs.member->>'source_member_key')
      from group_sources gs
      left join vat_policy vat
        on vat.source_member_key=gs.member->>'source_member_key'
      where gs.group_key=g.group_key),'[]'::jsonb))
    order by g.group_key),'[]'::jsonb) scopes
  from groups g
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
),independent_member_blockers as materialized (
  select gs.group_key,gs.member->>'source_member_key' source_member_key,
    blocker.code blocker_code,blocker.ordinality blocker_order
  from group_sources gs
  left join public.timesheets ts
    on ts.timesheet_id=gs.timesheet_id and ts.is_current
  left join public.timesheets_financials tf
    on tf.timesheet_id=gs.timesheet_id and tf.is_current
  left join public.v_ts_invoice_precheck pc
    on pc.timesheet_id=gs.timesheet_id
  left join reference_policy ref
    on ref.source_member_key=gs.member->>'source_member_key'
  left join vat_policy vat
    on vat.source_member_key=gs.member->>'source_member_key'
  cross join lateral unnest(array_remove(array[
    case when ts.timesheet_id is null then 'TIMESHEET_NOT_CURRENT' end,
    case when tf.id is null then 'CURRENT_FINANCIALS_MISSING' end,
    case when coalesce(tf.is_stale,true) then 'FINANCIALS_STALE' end,
    case when coalesce(tf.processing_status::text,'')<>'READY_FOR_INVOICE'
      then 'NOT_READY_FOR_INVOICE' end,
    case when coalesce(tf.has_rate_issue,false) then 'RATE_MISSING' end,
    case when coalesce(tf.has_pay_channel_issue,false)
      then 'PAY_CHANNEL_MISSING' end,
    case when tf.locked_by_invoice_id is not null
      then 'SOURCE_ALREADY_LOCKED' end,
    case when upper(coalesce(ts.submission_mode::text,''))='QR'
      and(nullif(ts.qr_signed_hash,'') is null
        or ts.qr_signed_at_utc is null)
      then 'QR_TIMESHEET_UNSIGNED' end,
    case when coalesce(pc.require_reference_to_invoice,false)
        and coalesce(ref.reference_ready,false) is not true
      then coalesce(ref.blocker_code,'MISSING_REFERENCE') end,
    case when(coalesce(tf.mileage_pay_ex_vat,0)<>0
        or coalesce(tf.mileage_charge_ex_vat,0)<>0)
      and not exists(
        select 1 from public.timesheet_evidence e
        where e.timesheet_id=gs.timesheet_id
          and upper(coalesce(e.kind,''))='MILEAGE'
          and nullif(e.storage_key,'') is not null)
      then 'MISSING_MILEAGE_EVIDENCE' end,
    case when(coalesce(tf.expenses_pay_ex_vat,0)<>0
        or coalesce(tf.expenses_charge_ex_vat,0)<>0
        or coalesce(tf.travel_pay_ex_vat,0)<>0
        or coalesce(tf.travel_charge_ex_vat,0)<>0
        or coalesce(tf.accommodation_pay_ex_vat,0)<>0
        or coalesce(tf.accommodation_charge_ex_vat,0)<>0)
      and not exists(
        select 1 from public.timesheet_evidence e
        where e.timesheet_id=gs.timesheet_id
          and upper(coalesce(e.kind,'')) in(
            'TRAVEL','ACCOMMODATION','OTHER','EXPENSE','EXPENSES')
          and nullif(e.storage_key,'') is not null)
      then 'MISSING_EXPENSE_EVIDENCE' end,
    case when exists(
      select 1
      from public.timesheet_evidence e
      join public.invoice_document_assets a on a.id=e.document_asset_id
      where e.timesheet_id=gs.timesheet_id
        and a.status in(
          'UNSUPPORTED','CORRUPT','MISSING','FAILED','SUPERSEDED'))
      then 'REQUIRED_ASSET_PERMANENT_FAILURE' end,
    case when coalesce(vat.valid,false) is not true
      then coalesce(vat.blocker_code,'VAT_POLICY_UNRESOLVED') end,
    case when exists(
      select 1 from public.invoice_lines l
      join public.invoices i on i.id=l.invoice_id
      where l.timesheet_id=gs.timesheet_id
        and i.status in('DRAFT','ISSUED','ON_HOLD')
        and coalesce(tf.invoice_breakdown_json->>'mode','')<>'SEGMENTS')
      then 'SOURCE_ALREADY_INVOICED' end
  ],null)) with ordinality blocker(code,ordinality)
),
independent_group_blockers as materialized (
  select b.group_key,
    (array_agg(b.blocker_code
      order by b.source_member_key,b.blocker_order))[1] blocker_code,
    jsonb_build_object(
      'code',(array_agg(b.blocker_code
        order by b.source_member_key,b.blocker_order))[1],
      'sources',jsonb_agg(jsonb_build_object(
        'source_member_key',b.source_member_key,
        'code',b.blocker_code)
        order by b.source_member_key,b.blocker_order)) blocker_detail
  from independent_member_blockers b
  group by b.group_key
),
selected_totals as materialized (
  select gs.group_key,gs.timesheet_id,
    case when bool_or(gs.segment_id is not null) then
      round(coalesce(sum(case when coalesce(seg.value->>'charge_amount','')~
        '^[+-]?[0-9]+([.][0-9]+)?$'
        then (seg.value->>'charge_amount')::numeric
        when coalesce(seg.value->>'charge_ex_vat','')~
        '^[+-]?[0-9]+([.][0-9]+)?$'
        then (seg.value->>'charge_ex_vat')::numeric else 0 end),0),2)
      else max(round(coalesce(tf.total_charge_ex_vat,0),2)) end total_charge_ex_vat,
    case when bool_or(gs.segment_id is not null) then
      round(coalesce(sum(
        (case when coalesce(seg.value->>'hours_day','')~
          '^[+-]?[0-9]+([.][0-9]+)?$' then (seg.value->>'hours_day')::numeric else 0 end)
        +(case when coalesce(seg.value->>'hours_night','')~
          '^[+-]?[0-9]+([.][0-9]+)?$' then (seg.value->>'hours_night')::numeric else 0 end)
        +(case when coalesce(seg.value->>'hours_sat','')~
          '^[+-]?[0-9]+([.][0-9]+)?$' then (seg.value->>'hours_sat')::numeric else 0 end)
        +(case when coalesce(seg.value->>'hours_sun','')~
          '^[+-]?[0-9]+([.][0-9]+)?$' then (seg.value->>'hours_sun')::numeric else 0 end)
        +(case when coalesce(seg.value->>'hours_bh','')~
          '^[+-]?[0-9]+([.][0-9]+)?$' then (seg.value->>'hours_bh')::numeric else 0 end)
      ),0),2)
      else max(round(coalesce(tf.total_hours,0),2)) end total_hours
  from group_sources gs
  join public.timesheets_financials tf
    on tf.timesheet_id=gs.timesheet_id and tf.is_current
  left join lateral jsonb_array_elements(
    case when jsonb_typeof(tf.invoice_breakdown_json->'segments')='array'
      then tf.invoice_breakdown_json->'segments' else '[]'::jsonb end) seg(value)
    on gs.segment_id is not null and seg.value->>'segment_id'=gs.segment_id
  group by gs.group_key,gs.timesheet_id
),
source_display as materialized (
  select distinct gs.group_key,tf.timesheet_id,tf.client_id,
    ts.week_ending_date,ts.submission_mode,tf.basis,
    totals.total_charge_ex_vat,totals.total_hours,
    s.client_name,s.candidate_name,s.validation_status,
    coalesce(s.hr_validation_required_for_invoice,false) hr_validation_required_for_invoice,
    coalesce(s.hr_validation_required_for_invoice,false)
      and(s.validation_status is null or s.validation_status<>all(array[
        'VALIDATION_OK'::public.validation_status_enum,
        'OVERRIDDEN'::public.validation_status_enum])) blocked_by_hr_validation,
    pc.precheck_status,
    exists(
      select 1 from public.timesheet_evidence e
      left join public.invoice_document_assets a on a.id=e.document_asset_id
      where e.timesheet_id=tf.timesheet_id
        and e.processing_state not in('SUPERSEDED')
        and(a.id is null or a.status<>'READY')) unready_evidence_asset,
    exists(
      select 1 from public.invoice_document_versions dv
      where dv.entity_type='TIMESHEET'
        and dv.entity_id=tf.timesheet_id
        and dv.purpose='TIMESHEET'
        and dv.source_revision=ts.document_revision::text
        and dv.status='READY'
        and dv.r2_key is not null and dv.sha256~'^[0-9a-f]{64}$'
        and coalesce(dv.size_bytes,0)>0
        and coalesce(dv.page_count,0)>0) timesheet_document_ready,
    ts.document_revision,ts.document_state,ts.current_document_version_id
  from group_sources gs
  join public.timesheets_financials tf
    on tf.timesheet_id=gs.timesheet_id and tf.is_current
  join public.timesheets ts on ts.timesheet_id=tf.timesheet_id and ts.is_current
  join selected_totals totals
    on totals.group_key=gs.group_key
   and totals.timesheet_id=gs.timesheet_id
  join public.v_ts_invoice_precheck pc on pc.timesheet_id=tf.timesheet_id
  left join public.v_timesheets_summary_base s on s.timesheet_id=tf.timesheet_id
),
active_exact as materialized (
  select distinct on(c.payload_json->>'group_key',c.payload_json->>'source_revision')
    c.payload_json->>'group_key' group_key,o.id operation_id,o.status,
    c.payload_json->>'source_revision' source_revision,
    o.progress_json,o.error_json,o.updated_at_utc
  from public.invoice_operation_chunks c
  join public.invoice_operations o on o.id=c.operation_id
  where c.chunk_type='GENERATION_GROUP'
    and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
    and o.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
    and coalesce(c.payload_json->>'is_selection_expander','false')<>'true'
    and(not c.is_manifest_member or c.manifest_committed)
    and(not c.is_manifest_member or coalesce(c.entity_type,'')<>'OPERATION')
  order by c.payload_json->>'group_key',c.payload_json->>'source_revision',
    o.priority desc,o.created_at_utc desc
),
group_json as materialized (
  select g.client_id,g.group_key,g.target_invoice_week,
    g.consolidation_mode,g.invoice_stream,g.source_revision_hash,
    g.canonical_source_ids,g.canonical_source_members,
    coalesce(sum(sd.total_charge_ex_vat),0)::numeric subtotal_ex_vat,
    coalesce(sum(sd.total_hours),0)::numeric total_hours,
    jsonb_agg(jsonb_build_object(
      'timesheet_id',sd.timesheet_id,
      'candidate_name',sd.candidate_name,
      'week_ending_date',sd.week_ending_date,
      'total_charge_ex_vat',sd.total_charge_ex_vat,
      'total_hours',sd.total_hours,
      'basis',sd.basis::text,
      'submission_mode',coalesce(sd.submission_mode::text,''),
      'validation_status',coalesce(sd.validation_status::text,''),
      'hr_validation_required_for_invoice',sd.hr_validation_required_for_invoice,
      'blocked_by_hr_validation',sd.blocked_by_hr_validation,
      'precheck_status',coalesce(sd.precheck_status::text,''),
      'document_revision',sd.document_revision,
      'document_state',sd.document_state,
      'current_document_version_id',sd.current_document_version_id,
      'timesheet_document_ready',sd.timesheet_document_ready,
      'unready_evidence_asset',sd.unready_evidence_asset)
      order by sd.candidate_name nulls last,sd.timesheet_id) timesheets,
    case
      when g.blocker_code is not null then g.blocker_code
      when independent.blocker_code is not null
        then independent.blocker_code
      when coalesce(correction.valid,true) is not true
        then coalesce(correction.blocker_code,
          'INVOICE_CORRECTION_UNIT_INVALID')
      when bool_or(sd.blocked_by_hr_validation) then 'HR_VALIDATION_BLOCKED'
      when not coalesce(p_allow_early,false)
        and exists(
          select 1 from jsonb_array_elements(g.canonical_source_members) early(value)
          where case when pg_input_is_valid(
              coalesce(early.value->>'target_invoice_week',''),'date')
            then(early.value->>'target_invoice_week')::date+6
              >=(select today from anchor)
            else false end)
      then 'EARLY_GENERATION_NOT_ALLOWED'
      end blocker_code,
    case
      when g.blocker_code is not null then g.blocker_detail
      when independent.blocker_code is not null
        then independent.blocker_detail
      when coalesce(correction.valid,true) is not true
        then jsonb_build_object(
          'code',coalesce(correction.blocker_code,
            'INVOICE_CORRECTION_UNIT_INVALID'),
          'correction_validation',correction.details)
      when bool_or(sd.blocked_by_hr_validation)
        then jsonb_build_object('code','HR_VALIDATION_BLOCKED',
          'sources',coalesce(jsonb_agg(sd.timesheet_id order by sd.timesheet_id)
            filter(where sd.blocked_by_hr_validation),'[]'::jsonb))
      when not coalesce(p_allow_early,false)
        and exists(
          select 1 from jsonb_array_elements(g.canonical_source_members) early(value)
          where case when pg_input_is_valid(
              coalesce(early.value->>'target_invoice_week',''),'date')
            then(early.value->>'target_invoice_week')::date+6
              >=(select today from anchor)
            else false end)
      then jsonb_build_object('code','EARLY_GENERATION_NOT_ALLOWED')
      end blocker_detail,
    a.operation_id active_operation_id,a.status active_status,
    a.progress_json active_progress,a.error_json active_error,
    coalesce(correction.details,'[]'::jsonb) correction_validation
  from groups g join source_display sd on sd.group_key=g.group_key
  left join independent_group_blockers independent
    on independent.group_key=g.group_key
  left join correction_group_results correction
    on correction.group_key=g.group_key
  left join active_exact a on a.group_key=g.group_key
    and a.source_revision=g.source_revision_hash
  group by g.client_id,g.group_key,g.target_invoice_week,g.consolidation_mode,
    g.invoice_stream,g.source_revision_hash,g.canonical_source_ids,
    g.canonical_source_members,g.blocker_code,g.blocker_detail,
    independent.blocker_code,independent.blocker_detail,
    correction.valid,correction.blocker_code,correction.details,
    a.operation_id,a.status,a.progress_json,a.error_json
)
select
  g.client_id,
  c.name::text client_name,
  jsonb_build_object(
    'group_key',g.group_key,
    'invoice_week_start',g.target_invoice_week,
    'week_ending_date',case when g.target_invoice_week is null
      then null else g.target_invoice_week+6 end,
    'subtotal_ex_vat',round(g.subtotal_ex_vat,2),
    'total_hours',round(g.total_hours,2),
    'stream',g.invoice_stream,
    'consolidation_mode',g.consolidation_mode,
    'canonical_source_ids',to_jsonb(g.canonical_source_ids),
    'canonical_source_members',g.canonical_source_members,
    'canonical_source_revision',g.source_revision_hash,
    'blocker_code',g.blocker_code,
    'blocker_detail',g.blocker_detail,
    'correction_validation',g.correction_validation,
    'document_dependencies',coalesce((
      select jsonb_agg(jsonb_build_object(
        'timesheet_id',d.value->>'timesheet_id',
        'code','TIMESHEET_DOCUMENT_NOT_READY')
        order by d.value->>'timesheet_id')
      from jsonb_array_elements(g.timesheets) d(value)
      where coalesce((d.value->>'timesheet_document_ready')::boolean,false) is false
    ),'[]'::jsonb),
    'command_payload',jsonb_build_object(
      'command_type','GENERATE_SELECTED',
      'canonical_source_ids',to_jsonb(g.canonical_source_ids),
      'canonical_source_members',g.canonical_source_members,
      'group_key',g.group_key,
      'source_revision',g.source_revision_hash,
      'target_invoice_week',g.target_invoice_week,
      'consolidation_mode',g.consolidation_mode,
      'invoice_stream',g.invoice_stream,
      'correction_validation',g.correction_validation,
      'allow_early',coalesce(p_allow_early,false)),
    'timesheets',g.timesheets,
    'active_generation_operation_id',g.active_operation_id,
    'active_generation_status',g.active_status,
    'active_generation_progress',g.active_progress,
    'last_generation_error',g.active_error,
    'retry_available',g.active_status in('FAILED','DEAD_LETTER','BLOCKED','RETRY_WAIT')
  ) group_json
from group_json g
join public.clients c on c.id=g.client_id
order by g.target_invoice_week nulls last,g.client_id,g.group_key;
end;
$function$;

alter function private._invoice_batch_generate_group_rows_v2(boolean,integer,text[],timestamptz)
  owner to postgres;
revoke all on function private._invoice_batch_generate_group_rows_v2(boolean,integer,text[],timestamptz)
  from public,anon,authenticated;
grant execute on function private._invoice_batch_generate_group_rows_v2(boolean,integer,text[],timestamptz)
  to service_role;
