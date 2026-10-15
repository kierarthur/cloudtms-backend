set local jit=off;
set local pg_show_plans.is_enabled=off;
select current_database() as database,
 encode(sha256(convert_to(coalesce((select jsonb_agg(jsonb_build_object('oid',p.oid,'definition',pg_get_functiondef(p.oid),'owner',p.proowner,'acl',p.proacl) order by p.oid)::text from pg_proc p join pg_namespace n on n.oid=p.pronamespace where n.nspname||'.'||p.proname in('private.timesheet_correction_a0_booking_before_v1','private.timesheet_correction_a0_candidate_before_v1','private.timesheet_correction_a0_child_reference_before_v1','private.timesheet_correction_a0_clock_scope_v1','private.timesheet_correction_a0_family_tokens_retain_v1','private.timesheet_correction_a0_fence_acquire_v1','private.timesheet_correction_a0_fence_scalar_acquire_core_v1','private.timesheet_correction_a0_fence_tokens_v1','private.timesheet_correction_a0_fence_tokens_v2','private.timesheet_correction_a0_native_input_witness_v1','private.timesheet_correction_a0_native_lock_fence_v1','private.timesheet_correction_a0_reference_before_v1','private.timesheet_correction_a0_request_tokens_retain_v1','private.timesheet_correction_a0_send_family_fence_v1','private.timesheet_correction_a0_signed_precursor_v1','private.timesheet_correction_a0_source_coverage_acquire_core_v1','private.timesheet_correction_a0_source_coverage_acquire_v1','private.timesheet_correction_a0_source_token_assert_v1','private.timesheet_correction_a0_writer_admit_v1','private.timesheet_correction_canonical_input_assert_v1','private.timesheet_correction_component_negate_v1','private.timesheet_correction_leg_create_v1','private.timesheet_correction_native_original_reversal_v1','private.timesheet_correction_projection_by_request_v1','private.timesheet_correction_reversal_assert_v1','private.timesheet_correction_reversal_component_assert_v1','private.timesheet_correction_snapshot_identity_assert_v1','private.timesheet_correction_week_allocate_v1','public.timesheet_correction_authorise_atomic_v1','public.timesheet_correction_create_projection_v1','public.timesheet_correction_prepare_v1','public.timesheet_correction_request_projection_v1','public.timesheet_correction_send_v1','public.timesheet_correction_workflow_projection_v1')),'[]'),'UTF8')),'hex') as routine_fingerprint,
 encode(sha256(convert_to(coalesce((select jsonb_agg(to_jsonb(t) order by t.oid)::text from pg_trigger t where t.tgrelid in(select to_regclass(x) from unnest(array['public.timesheets','public.candidates','public.timesheet_validations','public.invoice_lines','public.timesheets_financials','public.candidate_submission_workflows','public.candidate_approval_requests','public.candidate_submission_components','public.candidate_expense_components','public.timesheet_evidence','public.nhsp_shifts','public.weekly_source_row_timesheet_lineages','public.contract_weeks','public.weekly_source_cycles','private.timesheet_correction_requests','private.timesheet_correction_a0_transaction_fences']::text[]) x)),'[]'),'UTF8')),'hex') as trigger_fingerprint,
 to_regclass('private.timesheet_correction_requests') is null as request_absent,
 to_regclass('private.timesheet_correction_a0_transaction_fences') is null as arbiter_absent;
do $vacancy$ begin if to_regclass('private.timesheet_correction_requests') is not null or to_regclass('private.timesheet_correction_a0_transaction_fences') is not null then raise exception 'EXPECTED_NEW_A0_SCHEMA_NOT_VACANT';end if;end $vacancy$;
-- One-time CloudTMS schema/data migration: healthroster_timesheet_correction_requests_v1
-- State the exact authority, safety boundary, and verification before implementation.





-- SEND_5 approves this exact request store because cycle/cohort-bound submission
-- requests and workflow-bound approvals cannot represent a pre-workflow P0.
-- It contains no parallel financial, query, signature or schedule authority.
create table private.timesheet_correction_requests (
  id uuid primary key default gen_random_uuid(),
  correction_id text not null unique,
  origin text not null check(origin in('AGENCY_INVOICE_CORRECTION','SIGNED_TIMESHEET_CORRECTION')),
  state text not null default 'WAITING' check(state in('WAITING','APPLIED','CANCELLED')),
  state_revision bigint not null default 1 check(state_revision>=1),
  proposal_revision bigint not null default 1 check(proposal_revision=1),
  root_timesheet_id uuid not null references public.timesheets(timesheet_id),
  original_booking_id text not null,
  reversed_timesheet_id uuid not null references public.timesheets(timesheet_id),
  reversed_version integer not null check(reversed_version>=1),
  reversed_financial_id uuid,
  candidate_id uuid not null references public.candidates(id),
  client_id uuid not null references public.clients(id),
  contract_id uuid references public.contracts(id),
  work_date date not null,
  week_ending_date date not null,
  replacement_timesheet_id uuid not null unique,
  replacement_booking_id text not null unique,
  replacement_week_id uuid,
  reversal_timesheet_id uuid unique,
  reversal_booking_id text unique,
  reversal_week_id uuid,
  request_key text not null unique check(length(request_key) between 1 and 200),
  request_fingerprint text not null check(request_fingerprint~'^[0-9a-f]{64}$'),
  target_revision text not null check(target_revision~'^[0-9a-f]{64}$'),
  reason text not null check(length(btrim(reason)) between 1 and 2000),
  frozen_original_json jsonb not null check(jsonb_typeof(frozen_original_json)='object'),
  proposal_json jsonb not null check(jsonb_typeof(proposal_json)='object'),
  original_invoice_id uuid references public.invoices(id),
  return_invoice_id uuid references public.invoices(id),
  workflow_id uuid,
  workflow_generation integer,
  terminal_receipt_json jsonb,
  created_by_user_id uuid not null references public.tms_users(id),
  created_at_utc timestamptz not null default now(),
  updated_at_utc timestamptz not null default now(),
  applied_at_utc timestamptz,
  cancelled_at_utc timestamptz,
  check(replacement_booking_id<>original_booking_id),
  check(replacement_timesheet_id<>reversed_timesheet_id and replacement_timesheet_id<>root_timesheet_id),
  check(reversal_timesheet_id is null or reversal_timesheet_id<>replacement_timesheet_id),
  check((state='WAITING' and terminal_receipt_json is null and applied_at_utc is null and cancelled_at_utc is null)
    or (state='APPLIED' and jsonb_typeof(terminal_receipt_json)='object' and applied_at_utc is not null
      and cancelled_at_utc is null and reversal_timesheet_id is not null and reversal_booking_id is not null)
    or (state='CANCELLED' and jsonb_typeof(terminal_receipt_json)='object' and cancelled_at_utc is not null
      and applied_at_utc is null and reversal_timesheet_id is null)),
  check((workflow_id is null and workflow_generation is null)
    or (workflow_id is not null and workflow_generation>=1))
);
create unique index timesheet_correction_request_waiting_root_v1
  on private.timesheet_correction_requests(root_timesheet_id) where state='WAITING';
create index timesheet_correction_request_reversed_v1
  on private.timesheet_correction_requests(reversed_timesheet_id,state);
alter table private.timesheet_correction_requests OWNER TO CURRENT_USER;
alter table private.timesheet_correction_requests enable row level security;
alter table private.timesheet_correction_requests force row level security;
create policy cloudtms_miget_service_owner_all on private.timesheet_correction_requests
  for all to current_user,service_role using(true) with check(true);
revoke all on private.timesheet_correction_requests from public,anon,authenticated,service_role;
grant all on private.timesheet_correction_requests to CURRENT_USER;
comment on table private.timesheet_correction_requests is
  'One immutable original/proposal and exact physical P0 request; service RPCs delegate existing lifecycle/signature/document authorities.';



create or replace function private.timesheet_correction_canonical_input_assert_v1(
  p_snapshot jsonb,p_timesheet_id uuid,p_version integer,p_candidate uuid,p_client uuid
) returns void language plpgsql security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
begin
  if jsonb_typeof(p_snapshot) is distinct from 'object'
    or (p_snapshot?'is_stale' and p_snapshot->'is_stale' is distinct from 'false'::jsonb)
    or p_snapshot->'has_rate_issue' is distinct from 'false'::jsonb
    or p_snapshot->'has_pay_channel_issue' is distinct from 'false'::jsonb then
    raise exception 'CORRECTION_CANONICAL_INPUT_INVALID' using errcode='55000';
  end if;
  -- Validation projection only: reuse the complete existing identity/lifecycle
  -- assertion after separately qualifying the allowed native omission.
  perform private.timesheet_correction_snapshot_identity_assert_v1(
    p_snapshot||jsonb_build_object('is_stale',false),p_timesheet_id,p_version,p_candidate,p_client);
end;
$function$;
create or replace function private.timesheet_correction_a0_clock_scope_v1(p_request_id uuid)
returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_request private.timesheet_correction_requests%rowtype;
  v_ts public.timesheets%rowtype; v_contract public.contracts%rowtype;
  v_settings jsonb; v_members jsonb; v_member jsonb; v_scope jsonb;
begin
  select r.* into v_request from private.timesheet_correction_requests r where r.id=p_request_id;
  if not found or v_request.state<>'WAITING' or v_request.origin<>'SIGNED_TIMESHEET_CORRECTION'
    or v_request.reversed_financial_id is not null or v_request.original_invoice_id is not null
    or v_request.return_invoice_id is not null then
    raise exception 'CORRECTION_A0_NATIVE_CLOCK_SCOPE_UNPROVED' using errcode='55000';
  end if;
  select t.* into v_ts from public.timesheets t where t.timesheet_id=v_request.reversed_timesheet_id;
  if not found or v_ts.version<>v_request.reversed_version or not v_ts.is_current
    or v_ts.sheet_scope::text<>'WEEKLY' or v_ts.archived_at_utc is not null
    or v_ts.authorised_at_server is not null or v_ts.revoked_at is not null
    or v_ts.contract_id is distinct from v_request.contract_id
    or v_ts.week_ending_date is distinct from v_request.week_ending_date
    or v_ts.booking_id is distinct from v_request.frozen_original_json#>>'{timesheet,booking_id}'
    or private.timesheet_correction_timesheet_authority_hash_v1(to_jsonb(v_ts))
      is distinct from private.timesheet_correction_timesheet_authority_hash_v1(v_request.frozen_original_json->'timesheet')
    or v_request.frozen_original_json->'financial_snapshot' is distinct from 'null'::jsonb
    or v_request.frozen_original_json->'financial_authority_hash' is distinct from 'null'::jsonb
    or jsonb_typeof(v_request.frozen_original_json->'manager_signed_witness') is distinct from 'object'
    or exists(select 1 from public.timesheets_financials f where f.timesheet_id=v_ts.timesheet_id)
    or exists(select 1 from public.nhsp_shifts s where s.timesheet_id=v_ts.timesheet_id and s.cancelled_at_utc is null)
    or exists(select 1 from public.weekly_source_row_timesheet_lineages l where l.timesheet_id=v_ts.timesheet_id)
    -- The unchanged weekly clock calculator preserves expense/mileage from
    -- curFin. With no curFin an extant native claim must not become fabricated
    -- zero. A separate native claim materialisation journey is required.
    or exists(select 1 from public.candidate_expense_components e
      where e.removed_at_utc is null and (e.owning_timesheet_id=v_ts.timesheet_id
        or (e.owning_timesheet_id is null and e.workflow_id in(
          select w.id from public.candidate_submission_workflows w
          where w.target_timesheet_id in(v_ts.timesheet_id,v_request.replacement_timesheet_id)
            or w.anchor_timesheet_id in(v_ts.timesheet_id,v_request.replacement_timesheet_id)))
        or (e.owning_timesheet_id=v_request.replacement_timesheet_id
          and not exists(select 1 from public.timesheets_financials pf
            where pf.timesheet_id=v_request.replacement_timesheet_id and pf.is_current))))
    then raise exception 'CORRECTION_A0_NATIVE_CLOCK_SCOPE_UNPROVED' using errcode='55000'; end if;
  select c.* into v_contract from public.contracts c where c.id=v_ts.contract_id;
  if not found or v_contract.candidate_id is distinct from v_request.candidate_id
    or v_contract.client_id is distinct from v_request.client_id
    or v_contract.start_date is null or v_contract.end_date is null
    or jsonb_typeof(v_ts.actual_schedule_json) is distinct from 'array'
    then raise exception 'CORRECTION_A0_NATIVE_CLOCK_SCOPE_UNPROVED' using errcode='55000'; end if;
  if jsonb_array_length(v_ts.actual_schedule_json)=0 then
    raise exception 'CORRECTION_A0_NATIVE_CLOCK_SCOPE_UNPROVED' using errcode='55000'; end if;
  -- Validate dates separately before casting. Native clock/break owners retain
  -- complete range/overnight/time bucket calculation and their own refusals.
  if exists(select 1 from jsonb_array_elements(v_ts.actual_schedule_json) s(value)
    where jsonb_typeof(s.value) is distinct from 'object'
      or pg_input_is_valid(s.value->>'date','date') is distinct from true
      or (s.value->>'date' ~ '^\d{4}-\d{2}-\d{2}$') is distinct from true) then
    raise exception 'CORRECTION_A0_NATIVE_CLOCK_SCOPE_UNPROVED' using errcode='55000'; end if;
  if exists(select 1 from jsonb_array_elements(v_ts.actual_schedule_json) s(value)
    where (s.value->>'date')::date not between v_ts.week_ending_date-6 and v_ts.week_ending_date
      or (s.value->>'date')::date not between v_contract.start_date and v_contract.end_date) then
    raise exception 'CORRECTION_A0_NATIVE_CLOCK_SCOPE_UNPROVED' using errcode='55000'; end if;
  v_settings:=private._timesheet_settings_authority_frozen_v1(v_ts.timesheet_id);
  if v_settings->>'client_id' is distinct from v_request.client_id::text
    or v_settings->>'contract_id' is distinct from v_request.contract_id::text
    or v_settings#>'{applicability,configuration_valid}' is distinct from 'true'::jsonb
    or v_settings#>'{applicability,import_authoritative}' is distinct from 'false'::jsonb
    or private.weekly_source_signed_check_only_v1(v_ts.timesheet_id) is distinct from true
    then raise exception 'CORRECTION_A0_NATIVE_CLOCK_SCOPE_UNPROVED' using errcode='55000'; end if;
  -- Require actual controlled NULL-rule SIGNED_CHECK_ONLY activation. NULL
  -- effective rules alone cannot prove a legacy/native clock price scope.
  select jsonb_agg(jsonb_build_object(
      'source_group_id',g.id,'source_group_version',g.version::text,
      'membership_id',gc.id,'membership_valid_from',gc.valid_from,'membership_valid_to',gc.valid_to,
      'assignment_id',a.id,'assignment_version',a.version::text,
      'activation_release_id',a.feature_release_id,'activation_request_id',a.feature_request_id,
      'activation_request_sha256',a.feature_request_sha256,
      'activation_receipt_sha256',encode(extensions.digest(convert_to(a.feature_receipt_json::text,'UTF8'),'sha256'),'hex'))
      order by g.id,gc.id,a.id) into v_members
    from public.weekly_source_groups g
    join public.weekly_source_group_clients gc on gc.source_group_id=g.id and gc.client_id=v_request.client_id
    join public.weekly_source_group_rule_assignments a on a.source_group_id=g.id
    join private.cloudtms_database_releases rel on rel.release_id=a.feature_release_id
    where g.active and g.source_family='ROSTER'
      and v_ts.week_ending_date between gc.valid_from and coalesce(gc.valid_to,'infinity'::date)
      and a.feature_enabled and a.feature_purpose='SIGNED_CHECK_ONLY' and a.rule_set_version_id is null
      and v_ts.week_ending_date between a.effective_from_week_ending and coalesce(a.effective_to_week_ending,'infinity'::date)
      and a.feature_changed_at_utc is not null and a.feature_request_sha256 ~ '^[0-9a-f]{64}$'
      and a.feature_receipt_json->>'contract'='WEEKLY_SOURCE_RULE_FEATURE_RECEIPT_V1'
      and a.feature_receipt_json->>'assignment_id'=a.id::text
      and a.feature_receipt_json->>'feature_purpose'='SIGNED_CHECK_ONLY'
      and a.feature_receipt_json->'feature_enabled'='true'::jsonb
      and a.feature_receipt_json->>'release_id'=a.feature_release_id
      and rel.status='VERIFIED' and rel.completed_at_utc is not null
      and rel.installed_contract_sha256=rel.repository_contract_sha256;
  if v_members is null or jsonb_array_length(v_members)<>1
    or exists(select 1 from public.weekly_source_group_clients gc
      join public.weekly_source_group_rule_assignments a on a.source_group_id=gc.source_group_id
      where gc.client_id=v_request.client_id and a.feature_purpose='RULE_SET'
        and (a.feature_enabled or a.feature_release_id is not null or a.feature_changed_at_utc is not null)
        and v_ts.week_ending_date between gc.valid_from and coalesce(gc.valid_to,'infinity'::date)
        and v_ts.week_ending_date between a.effective_from_week_ending and coalesce(a.effective_to_week_ending,'infinity'::date)) then
    raise exception 'CORRECTION_A0_NATIVE_CLOCK_SCOPE_UNPROVED' using errcode='55000'; end if;
  v_member:=v_members->0;
  v_scope:=jsonb_build_object('contract','TIMESHEET_CORRECTION_A0_CLOCK_SCOPE_V1',
    'request_id',v_request.id,'original_timesheet_id',v_ts.timesheet_id,'original_version',v_ts.version,
    'original_booking_id',v_ts.booking_id,'candidate_id',v_request.candidate_id,'client_id',v_request.client_id,
    'contract_id',v_contract.id,'week_ending_date',v_ts.week_ending_date,
    'pricing_origin','SIGNED_CHECK_ONLY_CONTRACT_RATES','rule_set_version_id',null,
    'settings_authority_fingerprint',v_settings->>'authority_fingerprint',
    'timesheet_authority_hash',private.timesheet_correction_timesheet_authority_hash_v1(to_jsonb(v_ts)),
    -- Exact native read input, never labelled as an invented historical rate.
    'contract_input',to_jsonb(v_contract),'controlled_scope',v_member);
  return v_scope||jsonb_build_object('scope_sha256',encode(extensions.digest(convert_to(v_scope::text,'UTF8'),'sha256'),'hex'));
end;
$function$;
create or replace function private.timesheet_correction_a0_native_input_witness_v1(p_request_id uuid)
returns jsonb language plpgsql stable security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare r private.timesheet_correction_requests%rowtype; v_rows jsonb; v_result jsonb;
  v_ids uuid[]; v_weekly jsonb; v_imports jsonb; v_validations jsonb;
begin
  select q.* into strict r from private.timesheet_correction_requests q where q.id=p_request_id;
  if r.state<>'WAITING' or r.origin<>'SIGNED_TIMESHEET_CORRECTION' or r.reversed_financial_id is not null
    or r.frozen_original_json#>>'{timesheet,sheet_scope}' is distinct from 'WEEKLY' then
    raise exception 'CORRECTION_A0_NATIVE_INPUT_UNPROVED' using errcode='55000'; end if;
  v_ids:=array[r.reversed_timesheet_id,r.replacement_timesheet_id];
  select jsonb_agg(to_jsonb(ctx) order by ctx.effective_timesheet_id) into v_rows
    from public.tsfin_load_context_batch(v_ids) ctx;
  if v_rows is null or jsonb_array_length(v_rows)<>2
    or exists(select 1 from jsonb_array_elements(v_rows) ctx(value)
      where ctx.value#>>'{out_candidate,id}' is distinct from r.candidate_id::text
        or ctx.value->>'out_client_id' is distinct from r.client_id::text
        or ctx.value#>>'{out_timesheet,contract_id}' is distinct from r.contract_id::text
        or ctx.value->>'effective_timesheet_id' not in(r.reversed_timesheet_id::text,r.replacement_timesheet_id::text))
    or (select count(distinct ctx.value->>'effective_timesheet_id') from jsonb_array_elements(v_rows) ctx(value))<>2 then
    raise exception 'CORRECTION_A0_NATIVE_INPUT_UNPROVED' using errcode='55000'; end if;
  select jsonb_agg(to_jsonb(ctx) order by ctx.timesheet_id) into v_weekly
    from public.tsfin_load_weekly_context_batch(v_ids) ctx;
  select jsonb_agg(to_jsonb(ctx) order by ctx.timesheet_id) into v_imports
    from public.tsfin_load_nhsp_shifts_batch(v_ids) ctx;
  if v_weekly is null or jsonb_array_length(v_weekly)<>2 or v_imports is null or jsonb_array_length(v_imports)<>2
    or exists(select 1 from jsonb_array_elements(v_weekly) ctx(value)
      where ctx.value->>'timesheet_id' not in(r.reversed_timesheet_id::text,r.replacement_timesheet_id::text)
        or ctx.value#>>'{out_cw,timesheet_id}' is distinct from ctx.value->>'timesheet_id'
        or ctx.value#>>'{out_cw,contract_id}' is distinct from r.contract_id::text
        or ctx.value#>>'{out_contract,id}' is distinct from r.contract_id::text
        or ctx.value#>>'{out_contract,candidate_id}' is distinct from r.candidate_id::text
        or ctx.value#>>'{out_contract,client_id}' is distinct from r.client_id::text)
    or (select count(distinct ctx.value->>'timesheet_id') from jsonb_array_elements(v_weekly) ctx(value))<>2
    or exists(select 1 from jsonb_array_elements(v_imports) ctx(value)
      where ctx.value->>'timesheet_id' not in(r.reversed_timesheet_id::text,r.replacement_timesheet_id::text)
        or jsonb_typeof(ctx.value->'shifts') is distinct from 'array')
    or (select count(distinct ctx.value->>'timesheet_id') from jsonb_array_elements(v_imports) ctx(value))<>2 then
    raise exception 'CORRECTION_A0_NATIVE_INPUT_UNPROVED' using errcode='55000'; end if;
  -- Exact fields/order used by the existing Worker readWeeklyValidation path.
  -- Empty succeeds as an explicit successful read, never missing authority.
  select jsonb_agg(jsonb_build_object('timesheet_id',input.id,'rows',coalesce(read_rows.rows,'[]'::jsonb)) order by input.id)
    into v_validations from unnest(v_ids) input(id)
    left join lateral(select jsonb_agg(to_jsonb(v) order by v.updated_at desc) rows from(
      select tv.timesheet_id,tv.status,tv.reason_code,tv.validated_at_utc,tv.updated_at
      from public.timesheet_validations tv where tv.timesheet_id=input.id order by tv.updated_at desc limit 2
    ) v) read_rows on true;
  v_result:=jsonb_build_object('contract','TIMESHEET_CORRECTION_A0_NATIVE_INPUT_WITNESS_V1',
    'request_id',r.id,'contexts',v_rows,'weekly_contexts',v_weekly,'import_contexts',v_imports,'validations',v_validations);
  return v_result||jsonb_build_object('sha256',encode(extensions.digest(convert_to(v_result::text,'UTF8'),'sha256'),'hex'));
end;
$function$;
create or replace function private.timesheet_correction_component_negate_v1(p_value jsonb,p_path text[] default array[]::text[])
returns jsonb language plpgsql immutable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare result jsonb; item record; key_name text; signed_value boolean;
begin
  if cardinality(p_path)>32 then raise exception 'CORRECTION_REVERSAL_COMPONENT_DEPTH_INVALID' using errcode='55000'; end if;
  if jsonb_typeof(p_value)='object' then
    result:='{}'::jsonb;
    for item in select key,value from jsonb_each(p_value) loop
      if item.key not in('invoice_locked_invoice_id','invoice_locked_at_utc','pay_locked','pay_locked_at_utc','is_reversal') then
        result:=result||jsonb_build_object(item.key,private.timesheet_correction_component_negate_v1(item.value,p_path||item.key));
      end if;
    end loop;
    if p_value?'is_reversal' then result:=result||jsonb_build_object('is_reversal',true); end if;
    return result;
  elsif jsonb_typeof(p_value)='array' then
    select coalesce(jsonb_agg(private.timesheet_correction_component_negate_v1(value,p_path||ordinality::text) order by ordinality),'[]'::jsonb)
      into result from jsonb_array_elements(p_value) with ordinality;
    return result;
  end if;
  key_name:=p_path[cardinality(p_path)];
  signed_value:=key_name in('hours_day','hours_night','hours_sat','hours_sun','hours_bh','hours_total','total_hours',
    'pay_amount','charge_amount','pay_ex_vat','charge_ex_vat','margin_ex_vat','total_pay_ex_vat','total_charge_ex_vat',
    'unit_count','units','total_units','units_total','pay_cost_ex_vat_including_erni','vat_amount','total_inc_vat')
    or (cardinality(p_path)>1 and p_path[cardinality(p_path)-1]='days')
    or (cardinality(p_path)>1 and p_path[cardinality(p_path)-1]='base_hours' and key_name in('day','night','sat','sun','bh'));
  if signed_value and jsonb_typeof(p_value)<>'null' then
    if not pg_input_is_valid(p_value#>>'{}','numeric') then raise exception 'CORRECTION_REVERSAL_COMPONENT_NUMERIC_INVALID' using errcode='55000'; end if;
    return case when jsonb_typeof(p_value)='string' then to_jsonb((-(p_value#>>'{}')::numeric)::text)
      else to_jsonb(-(p_value#>>'{}')::numeric) end;
  end if;
  return p_value;
end;
$function$;
create or replace function private.timesheet_correction_native_original_reversal_v1(p_original jsonb,p_reversal_id uuid)
returns jsonb language plpgsql immutable security definer
set search_path to 'pg_catalog','private','pg_temp'
as $function$
declare result jsonb; key_name text;
begin
  if jsonb_typeof(p_original) is distinct from 'object' or p_reversal_id is null
    or jsonb_typeof(p_original->'invoice_breakdown_json') is distinct from 'object' then
    raise exception 'CORRECTION_NATIVE_ORIGINAL_REVERSAL_CONTEXT_INVALID' using errcode='55000';
  end if;
  result:=p_original-array['id','created_at','updated_at','computed_at_utc','processed_at_utc','processed_by_user_id',
    'payment_reference','remittance_last_sent_at_utc','remittance_send_count','unlocked_by_credit_note_id'];
  foreach key_name in array array['hours_day','hours_night','hours_sat','hours_sun','hours_bh','total_hours',
    'total_pay_ex_vat','total_charge_ex_vat','margin_ex_vat','pay_vat_amount_snapshot','pay_total_inc_vat_snapshot',
    'expenses_pay_ex_vat','expenses_charge_ex_vat','travel_pay_ex_vat','travel_charge_ex_vat',
    'accommodation_pay_ex_vat','accommodation_charge_ex_vat','other_pay_ex_vat','other_charge_ex_vat',
    'mileage_units','mileage_pay_ex_vat','mileage_charge_ex_vat','additional_pay_ex_vat','additional_charge_ex_vat','additional_margin_ex_vat'] loop
    if result?key_name and jsonb_typeof(result->key_name)<>'null' then
      if not pg_input_is_valid(result->>key_name,'numeric') then raise exception 'CORRECTION_NATIVE_ORIGINAL_AMOUNT_INVALID' using errcode='55000',detail=key_name; end if;
      result:=jsonb_set(result,array[key_name],case when jsonb_typeof(result->key_name)='string'
        then to_jsonb((-(result->>key_name)::numeric)::text) else to_jsonb(-(result->>key_name)::numeric) end,true);
    end if;
  end loop;
  result:=result||jsonb_build_object('timesheet_id',p_reversal_id,'timesheet_version',1,'processing_status','PENDING_AUTH',
    'authorised_at_utc',null,'authorised_by_user_id',null,'paid_at_utc',null,'paid_by_user_id',null,
    'locked_by_invoice_id',null,'locked_at_utc',null,'pay_on_hold',false,'pay_on_hold_reason',null,'pay_on_hold_since_utc',null,
    'is_current',true,'is_stale',false,'stale_reason',null,
    'invoice_breakdown_json',private.timesheet_correction_component_negate_v1(p_original->'invoice_breakdown_json'),
    'additional_units_json',private.timesheet_correction_component_negate_v1(p_original->'additional_units_json',array['additional_units_json']));
  return result;
end;
$function$;
create or replace function private.timesheet_correction_snapshot_identity_assert_v1(
  p_snapshot jsonb,p_timesheet_id uuid,p_version integer,p_candidate uuid,p_client uuid
) returns void language plpgsql security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
begin
  if jsonb_typeof(p_snapshot) is distinct from 'object'
    or p_snapshot->>'timesheet_id' is distinct from p_timesheet_id::text
    or p_snapshot->>'timesheet_version' is distinct from p_version::text
    or p_snapshot->>'candidate_id' is distinct from p_candidate::text
    or p_snapshot->>'client_id' is distinct from p_client::text
    or coalesce((p_snapshot->>'is_stale')::boolean,true)
    or coalesce((p_snapshot->>'has_rate_issue')::boolean,true)
    or coalesce((p_snapshot->>'has_pay_channel_issue')::boolean,true)
    or nullif(btrim(p_snapshot->>'pay_method'),'') is null
    or jsonb_typeof(p_snapshot->'invoice_breakdown_json') is distinct from 'object'
    or nullif(p_snapshot->>'authorised_at_utc','') is not null
    or nullif(p_snapshot->>'paid_at_utc','') is not null
    or nullif(p_snapshot->>'locked_by_invoice_id','') is not null then
    raise exception 'CORRECTION_CANONICAL_SNAPSHOT_INVALID' using errcode='55000';
  end if;
end;
$function$;
create or replace function private.timesheet_correction_reversal_component_assert_v1(
  p_original jsonb,p_reversal jsonb,p_path text[] default array[]::text[]
) returns void language plpgsql security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare v_original jsonb; v_reversal jsonb; v_row record; v_key text; v_signed boolean;
begin
  if cardinality(p_path)>32 or jsonb_typeof(p_original) is distinct from jsonb_typeof(p_reversal) then
    raise exception 'CORRECTION_REVERSAL_COMPONENT_SHAPE_MISMATCH' using errcode='55000';
  end if;
  if jsonb_typeof(p_original)='object' then
    v_original:=p_original-array['invoice_locked_invoice_id','invoice_locked_at_utc','pay_locked','pay_locked_at_utc','is_reversal'];
    v_reversal:=p_reversal-array['invoice_locked_invoice_id','invoice_locked_at_utc','pay_locked','pay_locked_at_utc','is_reversal'];
    if (select array_agg(k order by k) from jsonb_object_keys(v_original) k)
      is distinct from (select array_agg(k order by k) from jsonb_object_keys(v_reversal) k)
      or (p_reversal?'is_reversal' and p_reversal->>'is_reversal' is distinct from 'true') then
      raise exception 'CORRECTION_REVERSAL_COMPONENT_SET_MISMATCH' using errcode='55000';
    end if;
    for v_row in select key,value from jsonb_each(v_original) loop
      perform private.timesheet_correction_reversal_component_assert_v1(v_row.value,v_reversal->v_row.key,p_path||v_row.key);
    end loop;
  elsif jsonb_typeof(p_original)='array' then
    if jsonb_array_length(p_original)<>jsonb_array_length(p_reversal) then
      raise exception 'CORRECTION_REVERSAL_COMPONENT_SET_MISMATCH' using errcode='55000';
    end if;
    for v_row in select o.value original,r.value reversal,o.n from jsonb_array_elements(p_original) with ordinality o(value,n)
      join jsonb_array_elements(p_reversal) with ordinality r(value,n) using(n) loop
      perform private.timesheet_correction_reversal_component_assert_v1(v_row.original,v_row.reversal,p_path||v_row.n::text);
    end loop;
  else
    v_key:=p_path[cardinality(p_path)];
    v_signed:=v_key in('hours_day','hours_night','hours_sat','hours_sun','hours_bh','hours_total','total_hours',
      'pay_amount','charge_amount','pay_ex_vat','charge_ex_vat','margin_ex_vat','total_pay_ex_vat','total_charge_ex_vat',
      'unit_count','units','total_units','units_total',
      'pay_cost_ex_vat_including_erni','vat_amount','total_inc_vat')
      or (cardinality(p_path)>1 and p_path[cardinality(p_path)-1]='days')
      or (cardinality(p_path)>1 and p_path[cardinality(p_path)-1]='base_hours'
        and v_key in('day','night','sat','sun','bh'));
    if v_signed and jsonb_typeof(p_original)<>'null' then
      if not pg_input_is_valid(p_original#>>'{}','numeric') or not pg_input_is_valid(p_reversal#>>'{}','numeric')
        or (p_reversal#>>'{}')::numeric is distinct from -(p_original#>>'{}')::numeric then
        raise exception 'CORRECTION_REVERSAL_COMPONENT_PENCE_MISMATCH' using errcode='55000',detail=array_to_string(p_path,'.');
      end if;
    elsif p_original is distinct from p_reversal then
      raise exception 'CORRECTION_REVERSAL_COMPONENT_IDENTITY_MISMATCH' using errcode='55000',detail=array_to_string(p_path,'.');
    end if;
  end if;
end;
$function$;
create or replace function private.timesheet_correction_reversal_assert_v1(p_original jsonb,p_reversal jsonb)
returns void language plpgsql security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare
  v_key text; v_original jsonb; v_reversed jsonb; v_row record;
  v_signed_fields text[]:=array['hours_day','hours_night','hours_sat','hours_sun','hours_bh','total_hours',
    'total_pay_ex_vat','total_charge_ex_vat','margin_ex_vat','pay_vat_amount_snapshot','pay_total_inc_vat_snapshot',
    'expenses_pay_ex_vat','expenses_charge_ex_vat','travel_pay_ex_vat','travel_charge_ex_vat',
    'accommodation_pay_ex_vat','accommodation_charge_ex_vat','other_pay_ex_vat','other_charge_ex_vat',
    'mileage_units','mileage_pay_ex_vat','mileage_charge_ex_vat','additional_pay_ex_vat','additional_charge_ex_vat','additional_margin_ex_vat'];
  v_component_fields text[]:=array['hours_day','hours_night','hours_sat','hours_sun','hours_bh','hours_total',
    'total_hours','pay_amount','charge_amount','pay_ex_vat','charge_ex_vat','margin_ex_vat',
    'pay_cost_ex_vat_including_erni','vat_amount','total_inc_vat'];
  v_lock_fields text[]:=array['invoice_locked_invoice_id','invoice_locked_at_utc','pay_locked','pay_locked_at_utc'];
begin
  if jsonb_typeof(p_original) is distinct from 'object' or jsonb_typeof(p_reversal) is distinct from 'object'
    or p_original->'policy_snapshot_json' is distinct from p_reversal->'policy_snapshot_json'
    or p_original->'rate_source_refs_json' is distinct from p_reversal->'rate_source_refs_json'
    or p_original->'pay_method' is distinct from p_reversal->'pay_method'
    or p_original->'pay_vat_rate_pct_snapshot' is distinct from p_reversal->'pay_vat_rate_pct_snapshot'
    or p_original->'pay_wtr_rate_pct_snapshot' is distinct from p_reversal->'pay_wtr_rate_pct_snapshot' then
    raise exception 'CORRECTION_REVERSAL_HISTORIC_AUTHORITY_MISMATCH' using errcode='55000';
  end if;
  foreach v_key in array array['pay_day','pay_night','pay_sat','pay_sun','pay_bh','charge_day','charge_night','charge_sat','charge_sun','charge_bh',
    'mileage_pay_rate','mileage_charge_rate','basis','role','band','occupant_key_norm'] loop
    if p_original->v_key is distinct from p_reversal->v_key then
      raise exception 'CORRECTION_REVERSAL_HISTORIC_AUTHORITY_MISMATCH' using errcode='55000',detail=v_key;
    end if;
  end loop;
  perform private.timesheet_correction_reversal_component_assert_v1(p_original->'invoice_breakdown_json',p_reversal->'invoice_breakdown_json');
  perform private.timesheet_correction_reversal_component_assert_v1(p_original->'additional_units_json',p_reversal->'additional_units_json',array['additional_units_json']);
  foreach v_key in array v_signed_fields loop
    if coalesce((p_reversal->>v_key)::numeric,0) is distinct from -coalesce((p_original->>v_key)::numeric,0) then
      raise exception 'CORRECTION_REVERSAL_ORIGINAL_PENCE_MISMATCH' using errcode='55000',detail=v_key;
    end if;
  end loop;
  if p_original#>>'{invoice_breakdown_json,mode}' is distinct from p_reversal#>>'{invoice_breakdown_json,mode}' then
    raise exception 'CORRECTION_REVERSAL_COMPONENT_MODE_MISMATCH' using errcode='55000';
  end if;
  if p_original#>>'{invoice_breakdown_json,mode}'='SEGMENTS' then
    if jsonb_typeof(p_original#>'{invoice_breakdown_json,segments}') is distinct from 'array'
      or jsonb_typeof(p_reversal#>'{invoice_breakdown_json,segments}') is distinct from 'array'
      or jsonb_array_length(p_original#>'{invoice_breakdown_json,segments}')
        <>jsonb_array_length(p_reversal#>'{invoice_breakdown_json,segments}') then
      raise exception 'CORRECTION_REVERSAL_COMPONENT_SET_MISMATCH' using errcode='55000';
    end if;
    for v_row in select o.value original,s.value reversed from
      jsonb_array_elements(p_original#>'{invoice_breakdown_json,segments}') with ordinality o(value,n)
      join jsonb_array_elements(p_reversal#>'{invoice_breakdown_json,segments}') with ordinality s(value,n) using(n)
    loop
      v_original:=v_row.original; v_reversed:=v_row.reversed;
      if (v_original-v_component_fields-v_lock_fields-'is_reversal') is distinct from (v_reversed-v_component_fields-v_lock_fields-'is_reversal')
        or (nullif(p_reversal->>'authorised_at_utc','') is null
          and exists(select 1 from unnest(v_lock_fields) k where nullif(v_reversed->>k,'') is not null
            and not (k='pay_locked' and v_reversed->>k='false'))) then
        raise exception 'CORRECTION_REVERSAL_COMPONENT_IDENTITY_MISMATCH' using errcode='55000';
      end if;
      foreach v_key in array v_component_fields loop
        if (v_original?v_key) is distinct from (v_reversed?v_key)
          or (v_original?v_key and (v_reversed->>v_key)::numeric is distinct from -(v_original->>v_key)::numeric) then
          raise exception 'CORRECTION_REVERSAL_COMPONENT_PENCE_MISMATCH' using errcode='55000',detail=v_key;
        end if;
      end loop;
    end loop;
  else
    foreach v_key in array array['pay_ex_vat','charge_ex_vat','margin_ex_vat'] loop
      if coalesce((p_reversal#>>array['invoice_breakdown_json','base_hours',v_key])::numeric,0)
        is distinct from -coalesce((p_original#>>array['invoice_breakdown_json','base_hours',v_key])::numeric,0) then
        raise exception 'CORRECTION_REVERSAL_BASE_PENCE_MISMATCH' using errcode='55000',detail=v_key;
      end if;
    end loop;
  end if;
end;
$function$;
create or replace function public.timesheet_correction_prepare_v1(p_request_id uuid,p_actor_user_id uuid)
returns jsonb language plpgsql security definer
set search_path to 'pg_catalog','public','private','extensions','pg_temp'
as $function$
declare
  v_request private.timesheet_correction_requests%rowtype; v_original public.timesheets%rowtype;
  v_p0 public.timesheets%rowtype; v_fin public.timesheets_financials%rowtype; v_pfin public.timesheets_financials%rowtype;
  v_witness jsonb; v_context jsonb; v_hash text;
begin
  if private.timesheet_correction_actor_allowed_v1(p_actor_user_id) is distinct from true then
    raise exception 'CORRECTION_ACTOR_NOT_ALLOWED' using errcode='42501';
  end if;
  select request.* into strict v_request from private.timesheet_correction_requests request where request.id=p_request_id;
  if v_request.state<>'WAITING' then return v_request.terminal_receipt_json||jsonb_build_object('replayed',true); end if;
  select t.* into strict v_original from public.timesheets t where t.timesheet_id=v_request.reversed_timesheet_id;
  select t.* into strict v_p0 from public.timesheets t where t.timesheet_id=v_request.replacement_timesheet_id;
  select tf.* into v_fin from public.timesheets_financials tf where tf.timesheet_id=v_original.timesheet_id and tf.is_current;
  select tf.* into v_pfin from public.timesheets_financials tf where tf.timesheet_id=v_p0.timesheet_id and tf.is_current;
  if private.timesheet_correction_origin_v1(v_original.timesheet_id) is distinct from v_request.origin
    or v_original.version<>v_request.reversed_version or not v_original.is_current or v_original.archived_at_utc is not null
    or v_original.booking_id is distinct from v_request.frozen_original_json#>>'{timesheet,booking_id}'
    or private.timesheet_correction_timesheet_authority_hash_v1(to_jsonb(v_original))
      is distinct from private.timesheet_correction_timesheet_authority_hash_v1(v_request.frozen_original_json->'timesheet')
    or v_fin.id is distinct from v_request.reversed_financial_id
    or (v_fin.id is not null and private.timesheet_correction_financial_authority_hash_v1(to_jsonb(v_fin))
      is distinct from v_request.frozen_original_json->>'financial_authority_hash')
    or v_p0.correction_id is distinct from v_request.correction_id
    or v_p0.correction_kind is distinct from 'WHOLE_TIMESHEET_REPLACEMENT'
    or v_p0.booking_id is distinct from v_request.replacement_booking_id
    or v_p0.parent_timesheet_id is distinct from v_request.root_timesheet_id
    or not v_p0.is_current or v_p0.archived_at_utc is not null or v_p0.authorised_at_server is not null
    or v_pfin.authorised_at_utc is not null or v_pfin.paid_at_utc is not null or v_pfin.locked_by_invoice_id is not null
    or exists(select 1 from public.invoice_lines l where l.timesheet_id=v_p0.timesheet_id) then
    raise exception 'CORRECTION_PREPARE_AUTHORITY_CHANGED' using errcode='40001';
  end if;
  if v_request.original_invoice_id is not null and not exists(select 1 from public.invoices i
    where i.id=v_request.original_invoice_id and i.type='INVOICE' and i.status in('ISSUED','PAID')
      and exists(select 1 from public.invoice_lines l where l.invoice_id=i.id and l.timesheet_id=v_original.timesheet_id)) then
    raise exception 'CORRECTION_ORIGINAL_INVOICE_CHANGED' using errcode='40001';
  end if;
  if v_request.origin='SIGNED_TIMESHEET_CORRECTION' then
    if (private.timesheet_correction_signed_witness_v1(v_original.timesheet_id,false)-'document_version_id')
      is distinct from (v_request.frozen_original_json->'manager_signed_witness'-'document_version_id') then
      raise exception 'CORRECTION_ORIGINAL_SIGNATURE_CHANGED' using errcode='40001';
    end if;
    v_witness:=private.timesheet_correction_signed_witness_v1(v_p0.timesheet_id,false);
  end if;
  v_context:=jsonb_build_object('request_id',v_request.id,'state_revision',v_request.state_revision,
    'target_revision',v_request.target_revision,'proposal_revision',1,
    'root_timesheet_id',v_request.root_timesheet_id,'origin',v_request.origin,
    'original_timesheet_id',v_original.timesheet_id,'original_timesheet_version',v_original.version,
    'original_financial_id',v_fin.id,'original_financial_authority_hash',v_request.frozen_original_json->>'financial_authority_hash',
    'replacement_timesheet_id',v_p0.timesheet_id,'replacement_version',v_p0.version,
    'replacement_financial_id',v_pfin.id,'replacement_financial_authority_hash',
      case when v_pfin.id is not null then private.timesheet_correction_financial_authority_hash_v1(to_jsonb(v_pfin)) end,
    'replacement_schedule',v_p0.actual_schedule_json,'replacement_signed_witness',v_witness,
    'planned_reversal_timesheet_id',v_request.frozen_original_json->>'planned_reversal_timesheet_id',
    'planned_reversal_week_id',v_request.frozen_original_json->>'planned_reversal_week_id',
    'original_invoice_id',v_request.original_invoice_id,'return_invoice_id',v_request.return_invoice_id);
  if v_fin.id is null and v_original.sheet_scope::text='WEEKLY' then
    -- Exact server-owned scope participates in the native PREPARE/CAS hash.
    -- Missing A0 never silently changes Source/Rule/ordinary pricing origin.
    v_context:=v_context||jsonb_build_object('original_a0_clock_scope',
      private.timesheet_correction_a0_clock_scope_v1(v_request.id),
      'original_a0_native_input_witness',private.timesheet_correction_a0_native_input_witness_v1(v_request.id));
  end if;
  v_hash:=encode(extensions.digest(v_context::text,'sha256'),'hex');
  -- Internal service DTO only. Original frozen snapshot and exact P0 context
  -- go to existing producer/preloader; Candidate detail never exposes these.
  return jsonb_build_object('ok',true,'contract_version','TIMESHEET_CORRECTION_PREPARE_V1',
    'prepare_fingerprint',v_hash,'authority',v_context,'candidate_id',v_request.candidate_id,'client_id',v_request.client_id,
    'frozen_original',v_request.frozen_original_json,'replacement_timesheet',to_jsonb(v_p0),
    'original_current_snapshot',case when v_fin.id is not null then to_jsonb(v_fin) end,
    'replacement_current_snapshot',case when v_pfin.id is not null then to_jsonb(v_pfin) end,
    'original_materialisation_required',v_fin.id is null,'original_authorisation_required',v_original.authorised_at_server is null);
end;
$function$;
create or replace function public.timesheet_correction_authorise_atomic_v1(
  p_request_id uuid,p_expected_state_revision bigint,p_prepared jsonb,p_actor_user_id uuid,p_now_utc timestamptz default now()
) returns jsonb language plpgsql security definer
set search_path to 'pg_catalog','public','private','extensions','pg_temp'
as $function$
declare
  v_request private.timesheet_correction_requests%rowtype; v_prepare jsonb; v_original jsonb; v_leg jsonb;
  v_r uuid; v_p uuid; v_items jsonb; v_result jsonb; v_write jsonb; v_receipt jsonb; v_now timestamptz:=coalesce(p_now_utc,now());
  v_original_before jsonb; v_original_fin_before jsonb; v_after_fin jsonb; v_t0_authorise boolean; v_version integer;
  v_claim jsonb; v_batch_result record; v_reversal_snapshot jsonb; v_a0_lock_fence jsonb;
begin
  perform private.timesheet_correction_actor_assert_v1(p_actor_user_id);
  select request.* into strict v_request from private.timesheet_correction_requests request where request.id=p_request_id;
  if v_request.state='WAITING' and v_request.origin='SIGNED_TIMESHEET_CORRECTION'
    and v_request.reversed_financial_id is null
    and v_request.frozen_original_json#>>'{timesheet,sheet_scope}'='WEEKLY' then
    -- Immutable request branch, never inferred from a concurrent current FIN.
    -- Complete native gate/I-1 precedes every established business row lock.
    v_a0_lock_fence:=private.timesheet_correction_a0_native_lock_fence_v1(v_request.id,p_actor_user_id);
  end if;
  perform 1 from public.invoices i where i.id in(v_request.original_invoice_id,v_request.return_invoice_id) order by i.id for update;
  perform pg_advisory_xact_lock(hashtextextended('CORRECTION_CHAIN|'||v_request.root_timesheet_id::text,21072026));
  select request.* into strict v_request from private.timesheet_correction_requests request where request.id=p_request_id for update;
  if v_request.state<>'WAITING' then return v_request.terminal_receipt_json||jsonb_build_object('replayed',true); end if;
  if v_request.state_revision is distinct from p_expected_state_revision then
    raise exception 'CORRECTION_REQUEST_CAS_STALE' using errcode='40001';
  end if;
  perform 1 from public.candidate_submission_workflows w where w.id in(select t.candidate_workflow_id from public.timesheets t
    where t.timesheet_id in(v_request.reversed_timesheet_id,v_request.replacement_timesheet_id)) order by w.id for update;
  perform 1 from public.contract_weeks w where w.id=v_request.replacement_week_id order by w.id for update;
  perform 1 from public.timesheets t where t.timesheet_id in(v_request.reversed_timesheet_id,v_request.replacement_timesheet_id)
    order by t.timesheet_id for update;
  perform 1 from public.timesheets_financials tf where tf.timesheet_id in(v_request.reversed_timesheet_id,v_request.replacement_timesheet_id)
    order by tf.timesheet_id,tf.id for update;
  v_prepare:=public.timesheet_correction_prepare_v1(p_request_id,p_actor_user_id);
  if jsonb_typeof(p_prepared) is distinct from 'object'
    or p_prepared->>'prepare_fingerprint' is distinct from v_prepare->>'prepare_fingerprint' then
    raise exception 'CORRECTION_PREPARED_RESULT_STALE' using errcode='40001';
  end if;
  select to_jsonb(t) into v_original_before from public.timesheets t where t.timesheet_id=v_request.reversed_timesheet_id;
  v_original_fin_before:=v_prepare->'original_current_snapshot';
  v_t0_authorise:=(v_prepare->>'original_authorisation_required')::boolean;
  v_original:=case when jsonb_typeof(v_original_fin_before)='object' then v_original_fin_before else p_prepared->'original_snapshot' end;
  if v_request.return_invoice_id is not null then
    if v_t0_authorise then raise exception 'CORRECTION_D1_UNAUTHORISED_PARENT_INVALID' using errcode='55000'; end if;
    perform private.timesheet_correction_draft_round_trip_v1(v_request.id,'REMOVE',p_actor_user_id);
  end if;
  if (v_prepare->>'original_materialisation_required')::boolean then
    if not v_t0_authorise then raise exception 'CORRECTION_AUTHORISED_ORIGINAL_FINANCE_MISSING' using errcode='55000'; end if;
    perform private.timesheet_correction_canonical_input_assert_v1(v_original,v_request.reversed_timesheet_id,
      v_request.reversed_version,v_request.candidate_id,v_request.client_id);
    v_write:=public.tsfin_write_current_snapshot_single_bounded(v_request.reversed_timesheet_id,v_request.reversed_version,
      v_original,p_actor_user_id,v_now);
    if coalesce((v_write->>'ok')::boolean,false) is not true then
      raise exception 'CORRECTION_ORIGINAL_WRITE_FAILED' using errcode='55000',detail=v_write::text;
    end if;
    -- Native normalisation is authoritative for a newly materialised A0.
    -- Sign-copy may now use the actual stored output inside this same unit;
    -- no separate materialisation transaction or guessed normalisation exists.
    select to_jsonb(tf) into strict v_original from public.timesheets_financials tf
      where tf.timesheet_id=v_request.reversed_timesheet_id and tf.is_current;
    perform private.timesheet_correction_snapshot_identity_assert_v1(v_original,v_request.reversed_timesheet_id,
      v_request.reversed_version,v_request.candidate_id,v_request.client_id);
  end if;
  v_p:=v_request.replacement_timesheet_id;
  select t.version into v_version from public.timesheets t where t.timesheet_id=v_p;
  perform private.timesheet_correction_canonical_input_assert_v1(p_prepared->'replacement_snapshot',v_p,v_version,
    v_request.candidate_id,v_request.client_id);
  v_write:=public.tsfin_write_current_snapshot_single_bounded(v_p,v_version,
    (p_prepared->'replacement_snapshot')||jsonb_build_object('processing_status','PENDING_AUTH'),p_actor_user_id,v_now);
  if coalesce((v_write->>'ok')::boolean,false) is not true then
    raise exception 'CORRECTION_REPLACEMENT_WRITE_FAILED' using errcode='55000',detail=v_write::text;
  end if;
  select to_jsonb(tf) into strict v_after_fin from public.timesheets_financials tf where tf.timesheet_id=v_p and tf.is_current;
  perform private.timesheet_correction_snapshot_identity_assert_v1(v_after_fin,v_p,v_version,
    v_request.candidate_id,v_request.client_id);
  v_leg:=private.timesheet_correction_leg_create_v1(v_request,'WHOLE_TIMESHEET_REVERSAL','[]'::jsonb,v_now);
  v_r:=(v_leg->>'timesheet_id')::uuid;
  v_reversal_snapshot:=case when (v_prepare->>'original_materialisation_required')::boolean
      and (p_prepared->'reversal_snapshot' is null or p_prepared->'reversal_snapshot'='null'::jsonb)
    then private.timesheet_correction_native_original_reversal_v1(v_original,v_r)
    else p_prepared->'reversal_snapshot' end;
  perform private.timesheet_correction_snapshot_identity_assert_v1(v_reversal_snapshot,v_r,1,
    v_request.candidate_id,v_request.client_id);
  perform private.timesheet_correction_reversal_assert_v1(v_original,v_reversal_snapshot);
  -- Use the existing real queue/targeted lease and canonical batch writer.
  -- The single writer intentionally clamps mileage units and is unsuitable
  -- for the frozen negative reversal. No fake outbox id or writer bypass.
  perform public.enqueue_ts_financials(v_r,'MANUAL'::public.ts_fin_reason_enum);
  select coalesce(jsonb_agg(to_jsonb(claimed) order by claimed.id),'[]'::jsonb) into v_claim
    from public.tsfin_dequeue_specific(array[v_r],1) claimed;
  if jsonb_array_length(v_claim)<>1 or v_claim#>>'{0,timesheet_id}' is distinct from v_r::text
    or v_claim#>>'{0,reason}' is distinct from 'MANUAL'
    or not exists(select 1 from public.ts_financials_outbox o where o.id=(v_claim#>>'{0,id}')::uuid
      and o.timesheet_id=v_r and o.reason='MANUAL' and o.attempt_count>0 and o.next_attempt_at>now()) then
    raise exception 'CORRECTION_REVERSAL_CANONICAL_CLAIM_UNPROVED' using errcode='55000';
  end if;
  select * into strict v_batch_result from public.tsfin_write_snapshots_and_complete(jsonb_build_array(jsonb_build_object(
    'outbox_id',v_claim#>>'{0,id}','timesheet_id',v_r,'snapshot',
    v_reversal_snapshot||jsonb_build_object('processing_status','PENDING_AUTH'))));
  if v_batch_result.ok_count is distinct from 1 or v_batch_result.fail_count is distinct from 0
    or v_batch_result.errors is distinct from '[]'::jsonb
    or exists(select 1 from public.ts_financials_outbox o where o.id=(v_claim#>>'{0,id}')::uuid) then
    raise exception 'CORRECTION_REVERSAL_WRITE_FAILED' using errcode='55000',detail=to_jsonb(v_batch_result)::text;
  end if;
  select to_jsonb(tf) into strict v_after_fin from public.timesheets_financials tf where tf.timesheet_id=v_r and tf.is_current;
  -- The bounded writer normalises totals. Check its actual output against the
  -- frozen source again so any pence/component drift aborts the entire unit.
  perform private.timesheet_correction_reversal_assert_v1(v_original,v_after_fin);
  v_receipt:=jsonb_build_object('ok',true,'state','APPLIED','request_id',v_request.id,'correction_id',v_request.correction_id,
    'reversed_timesheet_id',v_request.reversed_timesheet_id,'reversal_timesheet_id',v_r,'replacement_timesheet_id',v_p,
    'state_revision',v_request.state_revision+1,'prepare_fingerprint',v_prepare->>'prepare_fingerprint',
    'original_authorisation_required',v_t0_authorise,
    'original_materialised_in_unit',(v_prepare->>'original_materialisation_required')::boolean,
    'original_timesheet_authority_hash',private.timesheet_correction_timesheet_authority_hash_v1(v_original_before),
    'reversal_timesheet_authority_hash',private.timesheet_correction_timesheet_authority_hash_v1((select to_jsonb(t)
      from public.timesheets t where t.timesheet_id=v_r)),
    'replacement_timesheet_authority_hash',private.timesheet_correction_timesheet_authority_hash_v1((select to_jsonb(t)
      from public.timesheets t where t.timesheet_id=v_p)),
    'original_financial_id',(select tf.id from public.timesheets_financials tf where tf.timesheet_id=v_request.reversed_timesheet_id and tf.is_current),
    'original_financial_authority_hash',private.timesheet_correction_financial_authority_hash_v1((select to_jsonb(tf) from public.timesheets_financials tf
      where tf.timesheet_id=v_request.reversed_timesheet_id and tf.is_current)),
    'reversal_financial_id',(select tf.id from public.timesheets_financials tf where tf.timesheet_id=v_r and tf.is_current),
    'reversal_financial_authority_hash',private.timesheet_correction_financial_authority_hash_v1(v_after_fin),
    'replacement_financial_id',(select tf.id from public.timesheets_financials tf where tf.timesheet_id=v_p and tf.is_current),
    'replacement_financial_authority_hash',private.timesheet_correction_financial_authority_hash_v1((select to_jsonb(tf) from public.timesheets_financials tf
      where tf.timesheet_id=v_p and tf.is_current)),
    'ordinary_notification_timesheet_ids',jsonb_build_array(v_p));
  update private.timesheet_correction_requests request set state='APPLIED',state_revision=state_revision+1,
    reversal_timesheet_id=v_r,reversal_booking_id=v_leg->>'booking_id',reversal_week_id=(v_leg->>'contract_week_id')::uuid,
    workflow_id=(v_prepare#>>'{authority,replacement_signed_witness,workflow_id}')::uuid,
    workflow_generation=(v_prepare#>>'{authority,replacement_signed_witness,workflow_generation}')::integer,
    terminal_receipt_json=v_receipt,applied_at_utc=v_now,updated_at_utc=v_now where request.id=v_request.id;
  select jsonb_agg(jsonb_build_object('timesheet_id',t.timesheet_id,'expected_timesheet_id',t.timesheet_id,
    'expected_version',t.version,'lifecycle_group_id','correction:'||v_request.correction_id,
    'lifecycle_group_size',case when v_t0_authorise then 3 else 2 end) order by t.timesheet_id) into v_items
  from public.timesheets t where t.timesheet_id in(v_r,v_p) or (v_t0_authorise and t.timesheet_id=v_request.reversed_timesheet_id);
  -- This generic owner is pinned/call-only. Returned partial failures must
  -- raise, just like exceptions, to roll back rows, workflows and receipts.
  v_result:=public.timesheet_authorise_bulk_atomic(v_items,p_actor_user_id,v_now);
  if coalesce((v_result->>'all_success')::boolean,false) is not true
    or (v_result->>'success_count')::integer is distinct from (case when v_t0_authorise then 3 else 2 end)
    or exists(select 1 from jsonb_array_elements(coalesce(v_result->'results','[]'::jsonb)) r
      where coalesce((r->>'success')::boolean,false) is not true)
    or (select count(*) from public.timesheets t join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
      where t.timesheet_id in(v_r,v_p) and t.authorised_at_server is not null and tf.authorised_at_utc is not null)<>2 then
    raise exception 'CORRECTION_COMPLETE_UNIT_AUTHORISATION_FAILED' using errcode='55000',detail=v_result::text;
  end if;
  if not v_t0_authorise and ((select to_jsonb(t) from public.timesheets t where t.timesheet_id=v_request.reversed_timesheet_id)
      is distinct from v_original_before or (select to_jsonb(tf) from public.timesheets_financials tf
        where tf.timesheet_id=v_request.reversed_timesheet_id and tf.is_current) is distinct from v_original_fin_before)
      and v_request.return_invoice_id is null then
    raise exception 'CORRECTION_ISSUED_ORIGINAL_MUTATED' using errcode='55000';
  end if;
  if v_request.return_invoice_id is not null then
    if private.timesheet_correction_timesheet_authority_hash_v1((select to_jsonb(t) from public.timesheets t
      where t.timesheet_id=v_request.reversed_timesheet_id))
      is distinct from private.timesheet_correction_timesheet_authority_hash_v1(v_original_before)
      or private.timesheet_correction_financial_authority_hash_v1((select to_jsonb(tf) from public.timesheets_financials tf
      where tf.timesheet_id=v_request.reversed_timesheet_id and tf.is_current))
      is distinct from v_request.frozen_original_json->>'financial_authority_hash' then
      raise exception 'CORRECTION_D1_ORIGINAL_ECONOMICS_CHANGED' using errcode='55000';
    end if;
    v_write:=private.timesheet_correction_draft_round_trip_v1(v_request.id,'RETURN',p_actor_user_id);
    return v_receipt||jsonb_build_object('authorisation',v_result,'draft_return',v_write);
  end if;
  return v_receipt||jsonb_build_object('authorisation',v_result);
end;
$function$;
-- One-time exact Source/A0 arbiter schema; no application data.

-- SOURCE successor only. One physical arbiter; no business authority store.
-- No frozen predecessor was rewritten or assumed installed.
do $native_fence_domains$
declare r record;v_count integer:=0;
begin
  if current_setting('block_size')::integer<>8192 or not exists(
    select 1 from pg_catalog.pg_extension e join pg_catalog.pg_namespace n on n.oid=e.extnamespace
      where e.extname='btree_gist' and n.nspname='public') then
    raise exception 'CORRECTION_A0_NATIVE_EXCLUSION_PROFILE_UNPROVED' using errcode='55000';end if;
  if not exists(select 1 from pg_catalog.pg_collation c where c.oid='pg_catalog.default'::regcollation
    and c.collisdeterministic) then
    raise exception 'CORRECTION_A0_JSONB_DEDUP_EQUALITY_UNPROVED' using errcode='55000';end if;
  if not exists(select 1 from pg_catalog.pg_opclass c join pg_catalog.pg_namespace n on n.oid=c.opcnamespace
    join pg_catalog.pg_am a on a.oid=c.opcmethod where n.nspname='public' and c.opcname='gist_bool_ops'
      and c.opcintype='boolean'::regtype and a.amname='gist') then
    raise exception 'CORRECTION_A0_SOURCE_COVERAGE_OPCLASS_UNPROVED' using errcode='55000';end if;
  for r in select a.atttypid,c.collisdeterministic from pg_catalog.pg_attribute a
    join pg_catalog.pg_class t on t.oid=a.attrelid join pg_catalog.pg_namespace n on n.oid=t.relnamespace
    join pg_catalog.pg_collation c on c.oid=a.attcollation
    where n.nspname='public' and a.attnum>0 and not a.attisdropped and (t.relname,a.attname) in(values
      ('timesheets','booking_id'),('invoice_lines','booking_id'),('timesheets','occupant_key_norm'),('candidates','key_norm'),
      ('weekly_source_row_timesheet_lineages','family_booking_id'))
  loop
    v_count:=v_count+1;
    if r.atttypid<>'text'::regtype or r.collisdeterministic is distinct from true then
      raise exception 'CORRECTION_A0_NATIVE_TEXT_EQUALITY_UNPROVED' using errcode='55000';end if;
  end loop;
  if v_count<>5 or not exists(select 1 from pg_catalog.pg_attribute a where a.attrelid='public.candidates'::regclass
    and a.attname='nhsp_hr_name_aliases' and not a.attisdropped and a.atttypid='jsonb'::regtype)
    or to_regclass('private.timesheet_correction_a0_transaction_fences') is not null then
    raise exception 'CORRECTION_A0_NATIVE_TOKEN_SCHEMA_UNPROVED' using errcode='55000';end if;
  if not exists(select 1 from pg_catalog.pg_opclass c join pg_catalog.pg_namespace n on n.oid=c.opcnamespace
    join pg_catalog.pg_am a on a.oid=c.opcmethod where n.nspname='public' and c.opcname='gist_uuid_ops'
    and c.opcintype='uuid'::regtype and a.amname='gist') or not exists(
    select 1 from pg_catalog.pg_opclass c join pg_catalog.pg_namespace n on n.oid=c.opcnamespace
    join pg_catalog.pg_am a on a.oid=c.opcmethod where n.nspname='public' and c.opcname='gist_text_ops'
    and c.opcintype='text'::regtype and a.amname='gist') then
    raise exception 'CORRECTION_A0_NATIVE_EXCLUSION_OPCLASS_UNPROVED' using errcode='55000';end if;
end;
$native_fence_domains$;
create table private.timesheet_correction_a0_transaction_fences(
  token_kind text collate "C" not null,
  uuid_identity uuid,
  text_identity text collate "C",
  retained boolean not null,
  intent_span numrange not null,
  coverage_span daterange,
  touch_bit boolean not null default false,
  constraint timesheet_correction_a0_fences_identity_shape check(
    (coverage_span is null and (
      (token_kind in('FAMILY_UUID','PHYSICAL_UUID','WORKFLOW_UUID','CONTRACT_WEEK_UUID','SOURCE_CYCLE_UUID')
        and uuid_identity is not null and text_identity is null)
      or(token_kind in('BOOKING','KEY_NORM','ALIAS_JSON_STRING','SOURCE_OBJECT','SOURCE_GROUP_CLIENT',
        'SOURCE_CYCLE_CLIENT','SOURCE_GLOBAL_SETTINGS','SOURCE_REVISION_CLIENT','SOURCE_REVISION_EVENT','SOURCE_RESOLUTION_SLOT','SOURCE_LINEAGE_RESOLUTION_CLIENT','SOURCE_LINEAGE_EVENT_CLIENT','SOURCE_GROUP_REPORT_REVISION','SOURCE_GROUP_REVISION_EVENT') and uuid_identity is null
        and text_identity is not null and octet_length(text_identity)<=1536)))
    or(token_kind in('SOURCE_CONTRACT_COVERAGE','SOURCE_POLICY_MEMBERSHIP_COVERAGE') and uuid_identity is not null and text_identity is null
      and coverage_span is not null and not isempty(coverage_span))),
  constraint timesheet_correction_a0_fences_span_shape check(
    not isempty(intent_span) and ((retained and lower_inf(intent_span) and upper_inf(intent_span))
      or(not retained and not lower_inf(intent_span) and not upper_inf(intent_span)
        and lower_inc(intent_span) and not upper_inc(intent_span) and lower(intent_span)>=0
        and trunc(lower(intent_span))=lower(intent_span) and upper(intent_span)=lower(intent_span)+1))),
  constraint timesheet_correction_a0_fences_uuid_excl exclude using gist(
    token_kind public.gist_text_ops with =,uuid_identity public.gist_uuid_ops with =,intent_span with &&)
    where(uuid_identity is not null and coverage_span is null),
  constraint timesheet_correction_a0_fences_text_excl exclude using gist(
    token_kind public.gist_text_ops with =,text_identity public.gist_text_ops with =,intent_span with &&)
    where(text_identity is not null),
  constraint timesheet_correction_a0_fences_coverage_excl exclude using gist(
    token_kind public.gist_text_ops with =,uuid_identity public.gist_uuid_ops with =,
    coverage_span with &&,intent_span with &&,retained public.gist_bool_ops with <>)
    where(coverage_span is not null)
);
-- Retained scopes stay exact. Different overlapping scopes are not merged or
-- client-wide serialized; an ordinary range conflicts with either real scope.
create unique index timesheet_correction_a0_fences_coverage_retained_unique
  on private.timesheet_correction_a0_transaction_fences(token_kind,uuid_identity,coverage_span)
  where(retained and coverage_span is not null);
alter table private.timesheet_correction_a0_transaction_fences OWNER TO CURRENT_USER;
alter table private.timesheet_correction_a0_transaction_fences enable row level security;
create policy cloudtms_miget_service_owner_all on private.timesheet_correction_a0_transaction_fences
  for all to CURRENT_USER,service_role using(true) with check(true);
revoke all on table private.timesheet_correction_a0_transaction_fences from public,anon,authenticated,service_role;
-- Ordinary spans use only the actual top-level XID and are immediately deleted.
-- Correction span is unbounded. No ordinary store, retirement timer or history bump.



-- Source kernel foundation only; full caller/RAW/ACTIVE integration is separate.

-- Closed grammar for actual Source dependencies in the SAME proposed A0 arbiter.
-- No caller SQL, fake family identity or monetary permission is introduced.
create or replace function private.timesheet_correction_a0_source_token_assert_v1(p_token jsonb)
returns void language plpgsql immutable security definer set search_path='pg_catalog','pg_temp'
as $source_token$
declare kind text:=p_token->>'kind';identity jsonb; i integer; expected jsonb; span daterange;
begin
 if kind not in('SOURCE_OBJECT','SOURCE_GROUP_CLIENT','SOURCE_CYCLE_CLIENT','SOURCE_GLOBAL_SETTINGS','SOURCE_CONTRACT_COVERAGE','SOURCE_POLICY_MEMBERSHIP_COVERAGE','SOURCE_REVISION_CLIENT','SOURCE_REVISION_EVENT','SOURCE_RESOLUTION_SLOT','SOURCE_LINEAGE_RESOLUTION_CLIENT','SOURCE_LINEAGE_EVENT_CLIENT','SOURCE_GROUP_REPORT_REVISION','SOURCE_GROUP_REVISION_EVENT') then
  if p_token ? 'coverage' then raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
  return;
 end if;
 if kind in('SOURCE_CONTRACT_COVERAGE','SOURCE_POLICY_MEMBERSHIP_COVERAGE') then
  if jsonb_typeof(p_token->'coverage') is distinct from 'array' or jsonb_array_length(p_token->'coverage')<>2
   or exists(select 1 from jsonb_array_elements(p_token->'coverage') x where jsonb_typeof(x) not in('string','null')
    or(jsonb_typeof(x)='string' and pg_input_is_valid(x#>>'{}','date') is distinct from true)) then
   raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
  span:=daterange((p_token->'coverage'->>0)::date,(p_token->'coverage'->>1)::date,'[]');
  if isempty(span) then raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
  return;
 end if;
 if p_token ? 'coverage' or jsonb_typeof(p_token->'json') is distinct from 'string' then
  raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
 if kind='SOURCE_GLOBAL_SETTINGS' then
  if p_token->>'json' is distinct from 'true' then raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
  return;
 end if;
 begin identity:=(p_token->>'json')::jsonb;
 exception when invalid_text_representation then raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end;
 if jsonb_typeof(identity) is distinct from 'array' or jsonb_array_length(identity)<>(case when kind='SOURCE_RESOLUTION_SLOT' then 3 else 2 end) then
  raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
 if kind='SOURCE_RESOLUTION_SLOT' then
  if jsonb_typeof(identity->0) is distinct from 'string' or pg_input_is_valid(identity->>0,'uuid') is distinct from true
   or jsonb_typeof(identity->1) is distinct from 'number' or pg_input_is_valid(identity->>1,'bigint') is distinct from true
   or jsonb_typeof(identity->2) is distinct from 'string' or pg_input_is_valid(identity->>2,'uuid') is distinct from true then
   raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
  expected:=jsonb_build_array((identity->>0)::uuid,(identity->>1)::bigint,(identity->>2)::uuid);
 elsif kind='SOURCE_OBJECT' then
  if jsonb_typeof(identity->0) is distinct from 'string' or identity->>0 not in('public.weekly_source_groups','public.weekly_source_cycles','public.weekly_source_uploads','public.weekly_source_upload_rows','public.weekly_source_projection_publications','public.weekly_source_row_resolutions','public.weekly_source_row_economic_snapshots','public.weekly_source_row_expense_policy_snapshots','public.weekly_expense_authority_generations','public.weekly_source_expense_cell_evidence','public.weekly_source_charge_checks','public.weekly_source_charge_acceptances','public.weekly_source_invoice_hold_releases','public.weekly_source_state_transitions','public.weekly_source_report_scopes','public.weekly_source_self_bill_reports','public.weekly_source_self_bill_report_line_bindings','public.weekly_work_events','public.weekly_source_final_revisions','public.weekly_source_final_snapshot_lines','public.weekly_source_row_timesheet_lineages','public.weekly_source_ordinary_pay_projection_receipts','public.weekly_discrepancy_incidents','public.weekly_issue_comparison_revisions','public.weekly_source_billing_movements','public.weekly_source_group_clients','public.weekly_source_client_policies','public.weekly_source_contract_policies','public.weekly_source_group_rule_assignments','public.weekly_source_rule_set_versions','public.weekly_source_rule_sets','public.weekly_source_format_profiles','public.clients','public.client_settings','public.contracts','public.timesheets','public.timesheets_financials')
   or jsonb_typeof(identity->1) is distinct from 'string' or pg_input_is_valid(identity->>1,'uuid') is distinct from true then
   raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
  expected:=jsonb_build_array(identity->>0,(identity->>1)::uuid);
 else
  for i in 0..1 loop
   if jsonb_typeof(identity->i) is distinct from 'string' or pg_input_is_valid(identity->>i,'uuid') is distinct from true then
    raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
  end loop;
  expected:=jsonb_build_array((identity->>0)::uuid,(identity->>1)::uuid);
 end if;
 if p_token->>'json' is distinct from expected::text then
  raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
end;
$source_token$;
alter function private.timesheet_correction_a0_source_token_assert_v1(jsonb) OWNER TO CURRENT_USER;
revoke all on function private.timesheet_correction_a0_source_token_assert_v1(jsonb) from public,anon,authenticated,service_role;

create or replace function private.timesheet_correction_a0_fence_scalar_acquire_core_v1(p_tokens jsonb,p_retain boolean)
returns void language plpgsql volatile security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare t jsonb;k text;u uuid;s text;v_inserted integer;v_count integer;
  v_timeout text:=current_setting('lock_timeout');v_xid numeric;v_span numrange;
begin
  if p_retain is null or jsonb_typeof(p_tokens) is distinct from 'array' or exists(
    select 1 from jsonb_array_elements(p_tokens) x(value) where jsonb_typeof(x.value) is distinct from 'object'
      or x.value->>'kind' is null or x.value->>'kind' not in('FAMILY_UUID','PHYSICAL_UUID','BOOKING','KEY_NORM',
        'ALIAS_JSON_STRING','WORKFLOW_UUID','CONTRACT_WEEK_UUID','SOURCE_CYCLE_UUID',
        'SOURCE_OBJECT','SOURCE_GROUP_CLIENT','SOURCE_CYCLE_CLIENT','SOURCE_GLOBAL_SETTINGS','SOURCE_REVISION_CLIENT','SOURCE_REVISION_EVENT','SOURCE_RESOLUTION_SLOT','SOURCE_LINEAGE_RESOLUTION_CLIENT','SOURCE_LINEAGE_EVENT_CLIENT','SOURCE_GROUP_REPORT_REVISION','SOURCE_GROUP_REVISION_EVENT')
      or exists(select 1 from jsonb_object_keys(x.value) n(key) where n.key not in('kind','uuid','json'))
      or(x.value->>'kind' in('FAMILY_UUID','PHYSICAL_UUID','WORKFLOW_UUID','CONTRACT_WEEK_UUID','SOURCE_CYCLE_UUID')
        and(pg_input_is_valid(x.value->>'uuid','uuid') is distinct from true or x.value ? 'json'))
      or(x.value->>'kind' in('BOOKING','KEY_NORM','ALIAS_JSON_STRING','SOURCE_OBJECT','SOURCE_GROUP_CLIENT','SOURCE_CYCLE_CLIENT','SOURCE_GLOBAL_SETTINGS','SOURCE_REVISION_CLIENT','SOURCE_REVISION_EVENT','SOURCE_RESOLUTION_SLOT','SOURCE_LINEAGE_RESOLUTION_CLIENT','SOURCE_LINEAGE_EVENT_CLIENT','SOURCE_GROUP_REPORT_REVISION','SOURCE_GROUP_REVISION_EVENT')
        and(jsonb_typeof(x.value->'json') is distinct from 'string' or x.value ? 'uuid'))) then
    raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
  if p_retain and exists(select 1 from jsonb_array_elements(p_tokens) x(value)
    where x.value ? 'json' and octet_length(x.value->'json'#>>'{}')>1536) then
    raise exception 'CORRECTION_A0_TOKEN_IDENTITY_UNPROVED' using errcode='55000';end if;
  if jsonb_array_length(p_tokens)=0 then return;end if;
  if p_retain then v_span:=numrange(null,null,'()');else
    -- xid8 is converted through exact decimal text, never narrowed to bigint.
    -- Native API returns the top-level XID even inside a subtransaction.
    v_xid:=pg_current_xact_id()::text::numeric;v_span:=numrange(v_xid,v_xid+1,'[)');
  end if;
  begin
    -- One call orders PHYSICAL identities before parent/dependency identities.
    -- Owner callers still supply Candidate/I1 -> existing family -> dependency
    -- -> genuinely allocated phases, with the full native CAS after retention.
    for t in select d.value from(select distinct x.value from jsonb_array_elements(p_tokens) x(value)
      where not(x.value ? 'json') or octet_length(x.value->'json'#>>'{}')<=1536) d
      order by case when d.value->>'kind' in('WORKFLOW_UUID','CONTRACT_WEEK_UUID','SOURCE_CYCLE_UUID','SOURCE_OBJECT','SOURCE_GROUP_CLIENT','SOURCE_CYCLE_CLIENT','SOURCE_GLOBAL_SETTINGS','SOURCE_REVISION_CLIENT','SOURCE_REVISION_EVENT','SOURCE_RESOLUTION_SLOT','SOURCE_LINEAGE_RESOLUTION_CLIENT','SOURCE_LINEAGE_EVENT_CLIENT','SOURCE_GROUP_REPORT_REVISION','SOURCE_GROUP_REVISION_EVENT') then 1 else 0 end,d.value
    loop
      k:=t->>'kind';u:=case when t ? 'uuid' then(t->>'uuid')::uuid end;
      s:=case when t ? 'json' then t->'json'#>>'{}' end;
      perform set_config('lock_timeout','1ms',true);
      if p_retain then
        perform 1 from private.timesheet_correction_a0_transaction_fences f
          where f.retained and f.token_kind=k and((u is not null and f.uuid_identity=u)
            or(s is not null and f.text_identity=s collate "C")) for update nowait;
        if found then
          update private.timesheet_correction_a0_transaction_fences f set touch_bit=not f.touch_bit
            where f.retained and f.token_kind=k and((u is not null and f.uuid_identity=u)
              or(s is not null and f.text_identity=s collate "C"));
          get diagnostics v_count=row_count;
          if v_count<>1 then raise exception 'CORRECTION_A0_TOKEN_CHANGED' using errcode='40001';end if;
        else
          v_inserted:=null;
          if u is not null then
            insert into private.timesheet_correction_a0_transaction_fences(token_kind,uuid_identity,text_identity,retained,intent_span,touch_bit)
              values(k,u,null,true,v_span,true) on conflict on constraint timesheet_correction_a0_fences_uuid_excl
              do nothing returning 1 into v_inserted;
          else
            insert into private.timesheet_correction_a0_transaction_fences(token_kind,uuid_identity,text_identity,retained,intent_span,touch_bit)
              values(k,null,s,true,v_span,true) on conflict on constraint timesheet_correction_a0_fences_text_excl
              do nothing returning 1 into v_inserted;
          end if;
          if v_inserted is distinct from 1 then
            perform 1 from private.timesheet_correction_a0_transaction_fences f where f.retained and f.token_kind=k
              and((u is not null and f.uuid_identity=u) or(s is not null and f.text_identity=s collate "C")) for update nowait;
            if not found then raise exception 'CORRECTION_A0_TOKEN_CHANGED' using errcode='40001';end if;
            update private.timesheet_correction_a0_transaction_fences f set touch_bit=not f.touch_bit
              where f.retained and f.token_kind=k and((u is not null and f.uuid_identity=u)
                or(s is not null and f.text_identity=s collate "C"));
            get diagnostics v_count=row_count;
            if v_count<>1 then raise exception 'CORRECTION_A0_TOKEN_CHANGED' using errcode='40001';end if;
          end if;
        end if;
      else
        perform 1 from private.timesheet_correction_a0_transaction_fences f where f.retained and f.token_kind=k
          and((u is not null and f.uuid_identity=u) or(s is not null and f.text_identity=s collate "C")) for share nowait;
        if not found then
          v_inserted:=null;
          if u is not null then
            insert into private.timesheet_correction_a0_transaction_fences(token_kind,uuid_identity,text_identity,retained,intent_span,touch_bit)
              values(k,u,null,false,v_span,false) on conflict on constraint timesheet_correction_a0_fences_uuid_excl
              do nothing returning 1 into v_inserted;
          else
            insert into private.timesheet_correction_a0_transaction_fences(token_kind,uuid_identity,text_identity,retained,intent_span,touch_bit)
              values(k,null,s,false,v_span,false) on conflict on constraint timesheet_correction_a0_fences_text_excl
              do nothing returning 1 into v_inserted;
          end if;
          if v_inserted=1 then
            -- Delete only this top-level transaction's own newly inserted span.
            -- Other ordinary spans are disjoint. This never deletes a retained token.
            delete from private.timesheet_correction_a0_transaction_fences f where not f.retained and f.intent_span=v_span
              and f.token_kind=k and((u is not null and f.uuid_identity=u) or(s is not null and f.text_identity=s collate "C"));
          else
            perform 1 from private.timesheet_correction_a0_transaction_fences f where f.retained and f.token_kind=k
              and((u is not null and f.uuid_identity=u) or(s is not null and f.text_identity=s collate "C")) for share nowait;
            if not found then raise exception 'CORRECTION_A0_TOKEN_CHANGED' using errcode='40001';end if;
          end if;
        end if;
      end if;
    end loop;
    perform set_config('lock_timeout',v_timeout,true);
  exception when others then perform set_config('lock_timeout',v_timeout,true);raise;end;
end;
$function$;
alter function private.timesheet_correction_a0_fence_scalar_acquire_core_v1(jsonb,boolean) OWNER TO CURRENT_USER;
revoke all on function private.timesheet_correction_a0_fence_scalar_acquire_core_v1(jsonb,boolean) from public,anon,authenticated,service_role;

-- The same arbiter, native date overlap and opposite intent classes.
-- Engine contention/dirty visibility/old-RR and whole caller order remain UNRUN.
create or replace function private.timesheet_correction_a0_source_coverage_acquire_core_v1(p_kind text,p_client uuid,p_coverage daterange,p_retain boolean)
returns void language plpgsql volatile security definer set search_path='pg_catalog','pg_temp'
as $coverage$
declare old_timeout text:=current_setting('lock_timeout');span numrange;xid numeric;inserted integer;changed integer;
begin
 if p_kind is null or p_kind not in('SOURCE_CONTRACT_COVERAGE','SOURCE_POLICY_MEMBERSHIP_COVERAGE') or p_client is null or p_coverage is null or isempty(p_coverage) or p_retain is null then
  raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
 if p_retain then span:=numrange(null,null,'()');else
  xid:=pg_current_xact_id()::text::numeric;span:=numrange(xid,xid+1,'[)');end if;
 begin
  perform set_config('lock_timeout','1ms',true);
  if p_retain then
   perform 1 from private.timesheet_correction_a0_transaction_fences f
    where f.retained and f.token_kind=p_kind and f.uuid_identity=p_client
      and f.coverage_span=p_coverage for update nowait;
   if not found then
    insert into private.timesheet_correction_a0_transaction_fences(token_kind,uuid_identity,retained,intent_span,coverage_span,touch_bit)
     values(p_kind,p_client,true,span,p_coverage,true)
     on conflict(token_kind,uuid_identity,coverage_span) where retained and coverage_span is not null
      do nothing returning 1 into inserted;
    if inserted is distinct from 1 then
     perform 1 from private.timesheet_correction_a0_transaction_fences f
      where f.retained and f.token_kind=p_kind and f.uuid_identity=p_client
       and f.coverage_span=p_coverage for update nowait;
     if not found then raise exception 'CORRECTION_A0_TOKEN_CHANGED' using errcode='40001';end if;
    end if;
   end if;
   if inserted is distinct from 1 then
    update private.timesheet_correction_a0_transaction_fences f set touch_bit=not f.touch_bit
     where f.retained and f.token_kind=p_kind and f.uuid_identity=p_client and f.coverage_span=p_coverage;
    get diagnostics changed=row_count;
    if changed<>1 then raise exception 'CORRECTION_A0_TOKEN_CHANGED' using errcode='40001';end if;
   end if;
  else
   perform 1 from private.timesheet_correction_a0_transaction_fences f
    where f.retained and f.token_kind=p_kind and f.uuid_identity=p_client
     and f.coverage_span && p_coverage for share nowait;
   if not found then
    insert into private.timesheet_correction_a0_transaction_fences(token_kind,uuid_identity,retained,intent_span,coverage_span,touch_bit)
     values(p_kind,p_client,false,span,p_coverage,false)
     on conflict on constraint timesheet_correction_a0_fences_coverage_excl do nothing returning 1 into inserted;
    if inserted=1 then
     delete from private.timesheet_correction_a0_transaction_fences f
      where not f.retained and f.token_kind=p_kind and f.uuid_identity=p_client
       and f.coverage_span=p_coverage and f.intent_span=span;
    else
     perform 1 from private.timesheet_correction_a0_transaction_fences f
      where f.retained and f.token_kind=p_kind and f.uuid_identity=p_client
       and f.coverage_span && p_coverage for share nowait;
     if not found then raise exception 'CORRECTION_A0_TOKEN_CHANGED' using errcode='40001';end if;
    end if;
   end if;
  end if;
  perform set_config('lock_timeout',old_timeout,true);
 exception when others then perform set_config('lock_timeout',old_timeout,true);raise;end;
end;
$coverage$;
alter function private.timesheet_correction_a0_source_coverage_acquire_core_v1(text,uuid,daterange,boolean) OWNER TO CURRENT_USER;
revoke all on function private.timesheet_correction_a0_source_coverage_acquire_core_v1(text,uuid,daterange,boolean) from public,anon,authenticated,service_role;

-- Preserve the original Contract-coverage helper ABI and its exact domain.
create or replace function private.timesheet_correction_a0_source_coverage_acquire_v1(p_client uuid,p_coverage daterange,p_retain boolean)
returns void language sql volatile security definer set search_path='pg_catalog','pg_temp'
as $coverage_contract$
 select private.timesheet_correction_a0_source_coverage_acquire_core_v1('SOURCE_CONTRACT_COVERAGE',p_client,p_coverage,p_retain);
$coverage_contract$;
alter function private.timesheet_correction_a0_source_coverage_acquire_v1(uuid,daterange,boolean) OWNER TO CURRENT_USER;
revoke all on function private.timesheet_correction_a0_source_coverage_acquire_v1(uuid,daterange,boolean) from public,anon,authenticated,service_role;

-- Preserve the exact original public-to-private caller signature. No Source
-- token means one unchanged native scalar call, preserving its old validation.
create or replace function private.timesheet_correction_a0_fence_acquire_v1(p_tokens jsonb,p_retain boolean)
returns void language plpgsql volatile security definer set search_path='pg_catalog','pg_temp'
as $source_acquire$
declare t jsonb;scalar_tokens jsonb;
begin
 if jsonb_typeof(p_tokens) is distinct from 'array' then
  perform private.timesheet_correction_a0_fence_scalar_acquire_core_v1(p_tokens,p_retain);return;end if;
 if not exists(select 1 from jsonb_array_elements(p_tokens) x where x->>'kind' in
  ('SOURCE_OBJECT','SOURCE_GROUP_CLIENT','SOURCE_CYCLE_CLIENT','SOURCE_GLOBAL_SETTINGS','SOURCE_CONTRACT_COVERAGE','SOURCE_POLICY_MEMBERSHIP_COVERAGE','SOURCE_REVISION_CLIENT','SOURCE_REVISION_EVENT','SOURCE_RESOLUTION_SLOT','SOURCE_LINEAGE_RESOLUTION_CLIENT','SOURCE_LINEAGE_EVENT_CLIENT','SOURCE_GROUP_REPORT_REVISION','SOURCE_GROUP_REVISION_EVENT')) then
  perform private.timesheet_correction_a0_fence_scalar_acquire_core_v1(p_tokens,p_retain);return;end if;
 if p_retain is null then raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
 -- Validate the entire new envelope BEFORE an acquisition. Neither a malformed
 -- coverage token nor mixed native token may partially admit an owner.
 for t in select value from jsonb_array_elements(p_tokens) loop
  if jsonb_typeof(t) is distinct from 'object' or exists(select 1 from jsonb_object_keys(t) key
    where key not in('kind','uuid','json','coverage')) then
   raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
  perform private.timesheet_correction_a0_source_token_assert_v1(t);
  if t->>'kind' in('SOURCE_CONTRACT_COVERAGE','SOURCE_POLICY_MEMBERSHIP_COVERAGE') and (t ? 'json' or jsonb_typeof(t->'uuid') is distinct from 'string'
    or pg_input_is_valid(t->>'uuid','uuid') is distinct from true) then
   raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
 end loop;
 select coalesce(jsonb_agg(value order by value),'[]'::jsonb) into scalar_tokens
  from jsonb_array_elements(p_tokens) where coalesce(value->>'kind','') not in('SOURCE_CONTRACT_COVERAGE','SOURCE_POLICY_MEMBERSHIP_COVERAGE');
 perform private.timesheet_correction_a0_fence_scalar_acquire_core_v1(scalar_tokens,p_retain);
 for t in select distinct value from jsonb_array_elements(p_tokens)
   where value->>'kind' in('SOURCE_CONTRACT_COVERAGE','SOURCE_POLICY_MEMBERSHIP_COVERAGE') order by value loop
  perform private.timesheet_correction_a0_source_coverage_acquire_core_v1(t->>'kind',(t->>'uuid')::uuid,
   daterange((t->'coverage'->>0)::date,(t->'coverage'->>1)::date,'[]'),p_retain);
 end loop;
end;
$source_acquire$;
alter function private.timesheet_correction_a0_fence_acquire_v1(jsonb,boolean) OWNER TO CURRENT_USER;
revoke all on function private.timesheet_correction_a0_fence_acquire_v1(jsonb,boolean) from public,anon,authenticated,service_role;

notify pgrst, 'reload schema';


-- Canonical source union, not yet qualified for release.

create or replace function private.timesheet_correction_a0_fence_tokens_v2(
  p_family uuid[],p_physical uuid[],p_bookings text[],p_keys text[],p_aliases jsonb[],
  p_workflows uuid[],p_weeks uuid[],p_cycles uuid[]
) returns jsonb language sql immutable security definer set search_path to 'pg_catalog','pg_temp'
as $function$
with tokens as(
  select jsonb_build_object('kind',d.kind,'uuid',u.id::text) value
    from(values('FAMILY_UUID',p_family),('PHYSICAL_UUID',p_physical),('WORKFLOW_UUID',p_workflows),
      ('CONTRACT_WEEK_UUID',p_weeks),('SOURCE_CYCLE_UUID',p_cycles)) d(kind,ids)
    cross join lateral unnest(d.ids) u(id) where u.id is not null
  union all select jsonb_build_object('kind','BOOKING','json',to_jsonb(btrim(b.value)))
    from unnest(p_bookings) b(value) where b.value is not null
  union all select jsonb_build_object('kind','KEY_NORM','json',to_jsonb(k.value))
    from unnest(p_keys) k(value) where k.value is not null
  union all select jsonb_build_object('kind','ALIAS_JSON_STRING','json',a.value)
    from unnest(p_aliases) j(value) cross join lateral jsonb_array_elements(
      case when jsonb_typeof(j.value)='array' then j.value else '[]'::jsonb end) a(value)
    where jsonb_typeof(a.value)='string'
)
select coalesce(jsonb_agg(value order by value),'[]'::jsonb) from(select distinct value from tokens) d;
$function$;
alter function private.timesheet_correction_a0_fence_tokens_v2(uuid[],uuid[],text[],text[],jsonb[],uuid[],uuid[],uuid[]) OWNER TO CURRENT_USER;
revoke all on function private.timesheet_correction_a0_fence_tokens_v2(uuid[],uuid[],text[],text[],jsonb[],uuid[],uuid[],uuid[]) from public,anon,authenticated,service_role;
create or replace function private.timesheet_correction_a0_fence_tokens_v1(
  p_family uuid[],p_physical uuid[],p_bookings text[],p_keys text[],p_aliases jsonb[]
) returns jsonb language sql immutable security definer set search_path to 'pg_catalog','pg_temp'
as $function$
select private.timesheet_correction_a0_fence_tokens_v2(p_family,p_physical,p_bookings,p_keys,p_aliases,null,null,null);
$function$;
alter function private.timesheet_correction_a0_fence_tokens_v1(uuid[],uuid[],text[],text[],jsonb[]) OWNER TO CURRENT_USER;
revoke all on function private.timesheet_correction_a0_fence_tokens_v1(uuid[],uuid[],text[],text[],jsonb[]) from public,anon,authenticated,service_role;
create or replace function private.timesheet_correction_a0_family_tokens_retain_v1(p_root uuid,p_members uuid[])
returns void language plpgsql volatile security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare v_count integer;v_expected integer;v_ids uuid[];v_bookings text[];v_keys text[];v_aliases jsonb[];
  v_workflows uuid[];v_weeks uuid[];v_cycles uuid[];v_before jsonb;v_after jsonb;
begin
  -- Exact native Candidate / whole I1 / family-CAS admission belongs to caller.
  -- Phase PHYSICAL closes workflow birth/reparent and every raw reference first.
  select count(distinct x.id) into v_expected from unnest(p_members) x(id) where x.id is not null;
  select count(*),array_agg(t.timesheet_id order by t.timesheet_id),array_agg(t.booking_id order by t.timesheet_id),
    array_agg(t.occupant_key_norm order by t.timesheet_id),array_agg(to_jsonb(array[t.occupant_key_norm]::text[]) order by t.timesheet_id)
    into v_count,v_ids,v_bookings,v_keys,v_aliases from public.timesheets t where t.timesheet_id=any(p_members);
  if p_root is null or v_expected=0 or v_expected>256 or v_count<>v_expected or not p_root=any(v_ids) then
    raise exception 'CORRECTION_A0_TOKEN_SCOPE_UNPROVED' using errcode='55000';end if;
  perform private.timesheet_correction_a0_fence_acquire_v1(
    private.timesheet_correction_a0_fence_tokens_v1(array[p_root],v_ids,v_bookings,v_keys,v_aliases),true);
  -- Phase DEPENDENCIES is discovered AFTER PHYSICAL intent is retained. New
  -- workflow/lineage births must carry raw OLD/NEW physical intent themselves.
  select array_agg(w.id order by w.id) into v_workflows from public.candidate_submission_workflows w
    where w.target_timesheet_id=any(v_ids) or w.anchor_timesheet_id=any(v_ids)
      or w.id in(select t.candidate_workflow_id from public.timesheets t where t.timesheet_id=any(v_ids));
  select array_agg(w.id order by w.id) into v_weeks from public.contract_weeks w
    where w.timesheet_id=any(v_ids) or w.id in(select f.contract_week_id from public.candidate_submission_workflows f where f.id=any(v_workflows));
  select array_agg(distinct l.source_cycle_id order by l.source_cycle_id) into v_cycles
    from public.weekly_source_row_timesheet_lineages l where l.timesheet_id=any(v_ids);
  v_before:=jsonb_build_object(
    'workflows',coalesce((select jsonb_agg(to_jsonb(w) order by w.id) from public.candidate_submission_workflows w where w.id=any(v_workflows)),'[]'::jsonb),
    'weeks',coalesce((select jsonb_agg(to_jsonb(w) order by w.id) from public.contract_weeks w where w.id=any(v_weeks)),'[]'::jsonb),
    'cycles',coalesce((select jsonb_agg(to_jsonb(c) order by c.id) from public.weekly_source_cycles c where c.id=any(v_cycles)),'[]'::jsonb),
    'approvals',coalesce((select jsonb_agg(to_jsonb(a) order by a.id) from public.candidate_approval_requests a where a.workflow_id=any(v_workflows)),'[]'::jsonb),
    'components',coalesce((select jsonb_agg(to_jsonb(c) order by c.id) from public.candidate_submission_components c
      where c.workflow_id=any(v_workflows) or c.timesheet_id=any(v_ids)),'[]'::jsonb),
    'expenses',coalesce((select jsonb_agg(to_jsonb(e) order by e.expense_component_id) from public.candidate_expense_components e
      where e.workflow_id=any(v_workflows) or e.owning_timesheet_id=any(v_ids)),'[]'::jsonb),
    'evidence',coalesce((select jsonb_agg(to_jsonb(e) order by e.id) from public.timesheet_evidence e where e.timesheet_id=any(v_ids)),'[]'::jsonb),
    'shifts',coalesce((select jsonb_agg(to_jsonb(s) order by s.id) from public.nhsp_shifts s where s.timesheet_id=any(v_ids)),'[]'::jsonb),
    'lineages',coalesce((select jsonb_agg(to_jsonb(l) order by l.id) from public.weekly_source_row_timesheet_lineages l where l.timesheet_id=any(v_ids)),'[]'::jsonb),
    'validations',coalesce((select jsonb_agg(to_jsonb(v) order by v.id) from public.timesheet_validations v where v.timesheet_id=any(v_ids)),'[]'::jsonb),
    'invoices',coalesce((select jsonb_agg(to_jsonb(l) order by l.id) from public.invoice_lines l where l.timesheet_id=any(v_ids)),'[]'::jsonb),
    'financials',coalesce((select jsonb_agg(to_jsonb(f) order by f.id) from public.timesheets_financials f where f.timesheet_id=any(v_ids)),'[]'::jsonb));
  perform private.timesheet_correction_a0_fence_acquire_v1(
    private.timesheet_correction_a0_fence_tokens_v2(null,null,null,null,null,v_workflows,v_weeks,v_cycles),true);
  -- An ordinary dependency mutation that was already inside its probe can
  -- finish before exclusion acquisition. Its actual changed row must refuse
  -- this purpose; retaining a token cannot replace the native reread/CAS.
  v_after:=jsonb_build_object(
    'workflows',coalesce((select jsonb_agg(to_jsonb(w) order by w.id) from public.candidate_submission_workflows w
      where w.target_timesheet_id=any(v_ids) or w.anchor_timesheet_id=any(v_ids)
        or w.id in(select t.candidate_workflow_id from public.timesheets t where t.timesheet_id=any(v_ids))),'[]'::jsonb),
    'weeks',coalesce((select jsonb_agg(to_jsonb(w) order by w.id) from public.contract_weeks w
      where w.timesheet_id=any(v_ids) or w.id in(select f.contract_week_id from public.candidate_submission_workflows f where f.id=any(v_workflows))),'[]'::jsonb),
    'cycles',coalesce((select jsonb_agg(to_jsonb(c) order by c.id) from public.weekly_source_cycles c
      where c.id in(select l.source_cycle_id from public.weekly_source_row_timesheet_lineages l where l.timesheet_id=any(v_ids))),'[]'::jsonb),
    'approvals',coalesce((select jsonb_agg(to_jsonb(a) order by a.id) from public.candidate_approval_requests a where a.workflow_id=any(v_workflows)),'[]'::jsonb),
    'components',coalesce((select jsonb_agg(to_jsonb(c) order by c.id) from public.candidate_submission_components c
      where c.workflow_id=any(v_workflows) or c.timesheet_id=any(v_ids)),'[]'::jsonb),
    'expenses',coalesce((select jsonb_agg(to_jsonb(e) order by e.expense_component_id) from public.candidate_expense_components e
      where e.workflow_id=any(v_workflows) or e.owning_timesheet_id=any(v_ids)),'[]'::jsonb),
    'evidence',coalesce((select jsonb_agg(to_jsonb(e) order by e.id) from public.timesheet_evidence e where e.timesheet_id=any(v_ids)),'[]'::jsonb),
    'shifts',coalesce((select jsonb_agg(to_jsonb(s) order by s.id) from public.nhsp_shifts s where s.timesheet_id=any(v_ids)),'[]'::jsonb),
    'lineages',coalesce((select jsonb_agg(to_jsonb(l) order by l.id) from public.weekly_source_row_timesheet_lineages l where l.timesheet_id=any(v_ids)),'[]'::jsonb),
    'validations',coalesce((select jsonb_agg(to_jsonb(v) order by v.id) from public.timesheet_validations v where v.timesheet_id=any(v_ids)),'[]'::jsonb),
    'invoices',coalesce((select jsonb_agg(to_jsonb(l) order by l.id) from public.invoice_lines l where l.timesheet_id=any(v_ids)),'[]'::jsonb),
    'financials',coalesce((select jsonb_agg(to_jsonb(f) order by f.id) from public.timesheets_financials f where f.timesheet_id=any(v_ids)),'[]'::jsonb));
  if v_after is distinct from v_before then
    raise exception 'CORRECTION_A0_TOKEN_DEPENDENCY_CHANGED' using errcode='40001';end if;
end;
$function$;
alter function private.timesheet_correction_a0_family_tokens_retain_v1(uuid,uuid[]) OWNER TO CURRENT_USER;
revoke all on function private.timesheet_correction_a0_family_tokens_retain_v1(uuid,uuid[]) from public,anon,authenticated,service_role;
create or replace function private.timesheet_correction_a0_request_tokens_retain_v1(p_request private.timesheet_correction_requests)
returns void language plpgsql volatile security definer set search_path to 'pg_catalog','public','private','extensions','pg_temp'
as $function$
declare t public.timesheets%rowtype;q private.timesheet_correction_requests%rowtype;v_r uuid;v_r_booking text;
  v_keys text[];v_aliases jsonb[];v_allocated uuid[];v_weeks uuid[];v_saved boolean;
begin
  if p_request.id is null or p_request.root_timesheet_id is null or p_request.reversed_timesheet_id is null
    or p_request.replacement_timesheet_id is null or p_request.origin is distinct from 'SIGNED_TIMESHEET_CORRECTION'
    or p_request.reversed_financial_id is not null or p_request.frozen_original_json#>>'{timesheet,sheet_scope}' is distinct from 'WEEKLY'
    or pg_input_is_valid(p_request.frozen_original_json->>'planned_reversal_timesheet_id','uuid') is distinct from true then
    raise exception 'CORRECTION_A0_TOKEN_SCOPE_UNPROVED' using errcode='55000';end if;
  select s.* into strict t from public.timesheets s where s.timesheet_id=p_request.reversed_timesheet_id;
  if t.contract_id is distinct from p_request.contract_id or t.version is distinct from p_request.reversed_version
    or private.timesheet_correction_a0_signed_precursor_v1(t.timesheet_id) is distinct from true then
    raise exception 'CORRECTION_A0_TOKEN_SCOPE_UNPROVED' using errcode='55000';end if;
  v_r:=(p_request.frozen_original_json->>'planned_reversal_timesheet_id')::uuid;
  v_r_booking:='bk_awc_'||substr(encode(digest(p_request.id::text||'|R0','sha256'),'hex'),1,24);
  select s.* into q from private.timesheet_correction_requests s where s.id=p_request.id;v_saved:=found;
  if v_saved and (q.state is distinct from 'WAITING' or q.origin is distinct from p_request.origin
    or q.root_timesheet_id is distinct from p_request.root_timesheet_id or q.reversed_timesheet_id is distinct from t.timesheet_id
    or q.replacement_timesheet_id is distinct from p_request.replacement_timesheet_id
    or q.replacement_booking_id is distinct from p_request.replacement_booking_id
    or q.frozen_original_json is distinct from p_request.frozen_original_json) then
    raise exception 'CORRECTION_A0_TOKEN_SCOPE_UNPROVED' using errcode='55000';end if;
  select array_agg(s.occupant_key_norm order by s.timesheet_id),array_agg(to_jsonb(array[s.occupant_key_norm]::text[]) order by s.timesheet_id)
    into v_keys,v_aliases from public.timesheets s where s.timesheet_id in(p_request.root_timesheet_id,t.timesheet_id);
  -- Genuine owner-assigned planned weeks use the same physical week domain.
  select array_agg(x.id order by x.id) into v_weeks from unnest(array[p_request.replacement_week_id,
    case when pg_input_is_valid(p_request.frozen_original_json->>'planned_reversal_week_id','uuid')
      then(p_request.frozen_original_json->>'planned_reversal_week_id')::uuid end]) x(id) where x.id is not null;
  perform private.timesheet_correction_a0_fence_acquire_v1(private.timesheet_correction_a0_fence_tokens_v2(
    array[p_request.root_timesheet_id],array[p_request.root_timesheet_id,t.timesheet_id,p_request.replacement_timesheet_id,v_r],
    array[p_request.original_booking_id,t.booking_id,p_request.replacement_booking_id,v_r_booking],v_keys,v_aliases,null,v_weeks,null),true);
  -- An ordinary no-FK reference that committed before the ALLOCATED exclusion
  -- phase must not be silently carried into a new physical leg. This is only
  -- a genuine newly allocated UUID check, never an ordinary dangling/NULL ban.
  v_allocated:=case when v_saved then array[v_r] else array[p_request.replacement_timesheet_id,v_r] end;
  if exists(select 1 from public.timesheets s where s.timesheet_id=any(v_allocated))
    or exists(select 1 from public.timesheets_financials f where f.timesheet_id=any(v_allocated))
    or exists(select 1 from public.timesheet_validations v where v.timesheet_id=any(v_allocated))
    or exists(select 1 from public.invoice_lines l where l.timesheet_id=any(v_allocated))
    or exists(select 1 from public.nhsp_shifts s where s.timesheet_id=any(v_allocated))
    or exists(select 1 from public.weekly_source_row_timesheet_lineages l where l.timesheet_id=any(v_allocated))
    or exists(select 1 from public.timesheet_evidence e where e.timesheet_id=any(v_allocated))
    or exists(select 1 from public.candidate_expense_components e where e.owning_timesheet_id=any(v_allocated))
    or exists(select 1 from public.candidate_submission_components c where c.timesheet_id=any(v_allocated))
    or exists(select 1 from public.candidate_submission_workflows w where w.target_timesheet_id=any(v_allocated) or w.anchor_timesheet_id=any(v_allocated))
    or exists(select 1 from public.contract_weeks w where w.timesheet_id=any(v_allocated)) then
    raise exception 'CORRECTION_A0_ALLOCATED_REFERENCE_EXISTS' using errcode='40001';end if;
end;
$function$;
alter function private.timesheet_correction_a0_request_tokens_retain_v1(private.timesheet_correction_requests) OWNER TO CURRENT_USER;
revoke all on function private.timesheet_correction_a0_request_tokens_retain_v1(private.timesheet_correction_requests) from public,anon,authenticated,service_role;

-- Adjacent review proposal. No installation/exposure or RR qualification.
-- Uses existing parent rows and call-only native Candidate/I-1 owners.
-- This read-only predicate identifies only an actual native signed precursor.
-- It grants no pricing/rule/history authority. Unproved signatures cannot be
-- admitted to missing-A0 by SEND either; ordinary unsigned records are a no-op.
create or replace function private.timesheet_correction_a0_signed_precursor_v1(p_timesheet_id uuid)
returns boolean language plpgsql volatile security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare t public.timesheets%rowtype; v_witness jsonb;
begin
  select ts.* into t from public.timesheets ts where ts.timesheet_id=p_timesheet_id;
  if not found or t.sheet_scope::text is distinct from 'WEEKLY' or t.submission_mode::text is distinct from 'ELECTRONIC'
    or t.contract_id is null or exists(select 1 from public.timesheets_financials f where f.timesheet_id=t.timesheet_id)
    or private.timesheet_correction_origin_v1(t.timesheet_id) is distinct from 'SIGNED_TIMESHEET_CORRECTION' then return false; end if;
  v_witness:=private.timesheet_correction_signed_witness_v1(t.timesheet_id,false);
  return v_witness is not null;
exception when sqlstate '55000' then return false;
end;
$function$;
alter function private.timesheet_correction_a0_signed_precursor_v1(uuid) OWNER TO CURRENT_USER;
revoke all on function private.timesheet_correction_a0_signed_precursor_v1(uuid) from public,anon,authenticated,service_role;

create or replace function private.timesheet_correction_a0_writer_admit_v1(p_ids uuid[],p_bookings text[],p_kind text)
returns jsonb language plpgsql volatile security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_requests jsonb; v_after jsonb; v_req_ids uuid[]; v_candidates uuid[]; v_roots uuid[];
  v_parent_ids uuid[]; v_members uuid[]; v_gate jsonb; v_lock jsonb; v_family_before jsonb; v_family_after jsonb;
  v_old_timeout text:=current_setting('lock_timeout'); r record;
begin
  if p_kind not in('VALIDATION_REFERENCE','INVOICE_REFERENCE','BOOKING_IDENTITY','CANDIDATE_IDENTITY') then
    raise exception 'CORRECTION_A0_WRITER_TRIGGER_CONTEXT_INVALID' using errcode='55000'; end if;
  -- Actual private immutable request/physical identity is the only active arm.
  -- Caller arguments are trigger-owned OLD/NEW references, not client flags.
  select jsonb_agg(jsonb_build_object('id',q.id,'state_revision',q.state_revision::text,'target_revision',q.target_revision,
    'candidate_id',q.candidate_id,'client_id',q.client_id,'contract_id',q.contract_id,'root',q.root_timesheet_id,
    'original',q.reversed_timesheet_id,'p0',q.replacement_timesheet_id,'original_booking',q.original_booking_id,
    'reversed_booking',q.frozen_original_json#>>'{timesheet,booking_id}','p0_booking',q.replacement_booking_id) order by q.id)
    into v_requests from private.timesheet_correction_requests q
    where q.state='WAITING' and q.origin='SIGNED_TIMESHEET_CORRECTION' and q.reversed_financial_id is null
      and q.frozen_original_json#>>'{timesheet,sheet_scope}'='WEEKLY'
      and (q.root_timesheet_id=any(p_ids) or q.reversed_timesheet_id=any(p_ids) or q.replacement_timesheet_id=any(p_ids)
        or exists(select 1 from public.timesheets t where t.timesheet_id=any(p_ids)
          and t.booking_id in(q.original_booking_id,q.frozen_original_json#>>'{timesheet,booking_id}',q.replacement_booking_id))
        or exists(select 1 from unnest(p_bookings) b(value) where btrim(b.value) in(
          btrim(q.original_booking_id),btrim(q.frozen_original_json#>>'{timesheet,booking_id}'),btrim(q.replacement_booking_id))));
  if v_requests is not null then
    -- Refusal is limited to an actual visible active missing-A0 request.
    -- An older RR snapshot that cannot see the parent/request is separately
    -- HELD in the native recipe, never certified by this source predicate.
    if current_setting('transaction_isolation')<>'read committed' then
      raise exception 'CORRECTION_A0_TRANSACTION_SNAPSHOT_UNPROVED' using errcode='55000'; end if;
    if jsonb_array_length(v_requests)>4 then raise exception 'CORRECTION_A0_WRITER_SCOPE_UNPROVED' using errcode='55000'; end if;
    select array_agg((x.value->>'id')::uuid order by x.value->>'id') into v_req_ids from jsonb_array_elements(v_requests) x(value);
    select array_agg(distinct (x.value->>'candidate_id')::uuid order by (x.value->>'candidate_id')::uuid)
      into v_candidates from jsonb_array_elements(v_requests) x(value);
    select array_agg(distinct t.timesheet_id order by t.timesheet_id) into v_roots
      from jsonb_array_elements(v_requests) x(value)
      cross join lateral unnest(array[(x.value->>'root')::uuid,(x.value->>'original')::uuid,(x.value->>'p0')::uuid]) i(id)
      join public.timesheets t on t.timesheet_id=i.id;
    -- P0 may be the same unit's not-yet-inserted physical row. Original/root
    -- must already exist; no missing existing family is silently dropped.
    if v_roots is null or exists(select 1 from jsonb_array_elements(v_requests) x(value)
      where not exists(select 1 from public.timesheets t where t.timesheet_id=(x.value->>'root')::uuid)
        or not exists(select 1 from public.timesheets t where t.timesheet_id=(x.value->>'original')::uuid)) then
      raise exception 'CORRECTION_A0_WRITER_SCOPE_UNPROVED' using errcode='55000'; end if;
    select jsonb_agg(to_jsonb(s) order by s.requested_timesheet_id,s.family_version,s.family_timesheet_id)
      into v_family_before from public._pay_timesheet_rotation_scope(v_roots) s;
    begin
      perform set_config('lock_timeout','1ms',true);
      -- ALL Candidate gates precede ONE native complete family acquisition.
      for r in select c.id from unnest(v_candidates) c(id) order by c.id loop
        v_gate:=private.weekly_source_candidate_serial_gate_v1(r.id,'WORKBENCH_CANDIDATE_FIRST_AUTHORISATION',
          v_req_ids[1],'CORRECTION_A0_SCOPED_CREATOR');
        if v_gate->'ok' is distinct from 'true'::jsonb or v_gate->>'gate' is distinct from 'GRANTED' then
          raise exception 'CORRECTION_A0_AUTHORITY_BUSY' using errcode='55P03'; end if;
      end loop;
      perform set_config('lock_timeout','1ms',true);
      -- Optional Candidate NULL is the native multi-Candidate set interface;
      -- exact persisted request/Contract dimensions are checked below.
      v_lock:=private.weekly_source_lock_family_rows_v1(v_roots,null);
      if v_lock->'ok' is distinct from 'true'::jsonb then
        raise exception 'CORRECTION_A0_WRITER_SCOPE_UNPROVED' using errcode='55000'; end if;
      perform set_config('lock_timeout',v_old_timeout,true);
    exception when others then perform set_config('lock_timeout',v_old_timeout,true); raise; end;
    select jsonb_agg(to_jsonb(s) order by s.requested_timesheet_id,s.family_version,s.family_timesheet_id)
      into v_family_after from public._pay_timesheet_rotation_scope(v_roots) s;
    if v_family_before is null or v_family_after is distinct from v_family_before then
      raise exception 'CORRECTION_A0_WRITER_SCOPE_CHANGED' using errcode='40001'; end if;
    perform 1 from private.timesheet_correction_requests q where q.id=any(v_req_ids) order by q.id for share nowait;
    select jsonb_agg(jsonb_build_object('id',q.id,'state_revision',q.state_revision::text,'target_revision',q.target_revision,
      'candidate_id',q.candidate_id,'client_id',q.client_id,'contract_id',q.contract_id,'root',q.root_timesheet_id,
      'original',q.reversed_timesheet_id,'p0',q.replacement_timesheet_id,'original_booking',q.original_booking_id,
      'reversed_booking',q.frozen_original_json#>>'{timesheet,booking_id}','p0_booking',q.replacement_booking_id) order by q.id)
      into v_after from private.timesheet_correction_requests q
      where q.state='WAITING' and q.origin='SIGNED_TIMESHEET_CORRECTION' and q.reversed_financial_id is null
        and q.frozen_original_json#>>'{timesheet,sheet_scope}'='WEEKLY'
        and (q.root_timesheet_id=any(p_ids) or q.reversed_timesheet_id=any(p_ids) or q.replacement_timesheet_id=any(p_ids)
          or exists(select 1 from public.timesheets t where t.timesheet_id=any(p_ids)
            and t.booking_id in(q.original_booking_id,q.frozen_original_json#>>'{timesheet,booking_id}',q.replacement_booking_id))
          or exists(select 1 from unnest(p_bookings) b(value) where btrim(b.value) in(
            btrim(q.original_booking_id),btrim(q.frozen_original_json#>>'{timesheet,booking_id}'),btrim(q.replacement_booking_id))));
    if v_after is distinct from v_requests then raise exception 'CORRECTION_A0_WRITER_SCOPE_CHANGED' using errcode='40001'; end if;
    if exists(select 1 from jsonb_array_elements(v_requests) x(value)
      join public.timesheets t on t.timesheet_id=(x.value->>'original')::uuid
      left join public.contracts c on c.id=t.contract_id
      where t.contract_id is distinct from (x.value->>'contract_id')::uuid
        or c.candidate_id is distinct from (x.value->>'candidate_id')::uuid
        or c.client_id is distinct from (x.value->>'client_id')::uuid) then
      raise exception 'CORRECTION_A0_WRITER_SCOPE_UNPROVED' using errcode='55000'; end if;
    select array_agg(distinct s.family_timesheet_id order by s.family_timesheet_id) into v_members
      from public._pay_timesheet_rotation_scope(v_roots) s;
  end if;
  -- Retain the existing parent intent through commit even before SEND exists.
  -- This narrowly covers the actual manager-signed weekly/no-FIN precursor
  -- required by the missing-A0 arm; it grants no pricing/signature authority.
  -- Other ordinary, historical NULL/dangling and existing-finance rows have
  -- no new parent admission outside an actual active request.
  -- Invoice already has the unchanged archived-Timesheet guard's real parent
  -- intent. Do not duplicate or widen its ordinary/dangling reference policy.
  select array_agg(distinct t.timesheet_id order by t.timesheet_id) into v_parent_ids
    from public.timesheets t where (t.timesheet_id=any(p_ids) or exists(select 1 from unnest(p_bookings) b(value)
      where btrim(t.booking_id)=btrim(b.value)))
      and (t.timesheet_id=any(v_members)
        or (p_kind<>'INVOICE_REFERENCE' and private.timesheet_correction_a0_signed_precursor_v1(t.timesheet_id)));
  if v_parent_ids is not null then
    if cardinality(v_parent_ids)>256 then raise exception 'CORRECTION_A0_WRITER_SCOPE_UNPROVED' using errcode='55000'; end if;
    perform 1 from public.timesheets t where t.timesheet_id=any(v_parent_ids) order by t.timesheet_id for key share nowait;
  end if;
  return jsonb_build_object('requests',coalesce(v_requests,'[]'::jsonb),'parent_ids',coalesce(to_jsonb(v_parent_ids),'[]'::jsonb));
end;
$function$;
alter function private.timesheet_correction_a0_writer_admit_v1(uuid[],text[],text) OWNER TO CURRENT_USER;
revoke all on function private.timesheet_correction_a0_writer_admit_v1(uuid[],text[],text) from public,anon,authenticated,service_role;

create or replace function private.timesheet_correction_a0_reference_before_v1()
returns trigger language plpgsql volatile security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare v_ids uuid[];v_bookings text[];
begin
  if tg_table_schema<>'public' or tg_table_name not in('timesheet_validations','invoice_lines','timesheets_financials') or tg_op not in('INSERT','UPDATE','DELETE') then
    raise exception 'CORRECTION_A0_WRITER_TRIGGER_CONTEXT_INVALID' using errcode='55000';end if;
  if tg_op='DELETE' then v_ids:=array[old.timesheet_id];
    if tg_table_name='invoice_lines' then v_bookings:=array[old.booking_id];end if;
  elsif tg_op='UPDATE' then v_ids:=array[old.timesheet_id,new.timesheet_id];
    if tg_table_name='invoice_lines' then v_bookings:=array[old.booking_id,new.booking_id];end if;
  else v_ids:=array[new.timesheet_id];
    if tg_table_name='invoice_lines' then v_bookings:=array[new.booking_id];end if;
  end if;
  select array_agg(distinct x.id order by x.id) into v_ids from unnest(v_ids) x(id) where x.id is not null;
  -- Every actual reference mutation participates, including unchanged pointers
  -- with changed validation/invoice facts and historical DELETE. No FK/lookup
  -- is required for a raw future P0 UUID. NULL/no-identity keeps the native arm.
  perform private.timesheet_correction_a0_fence_acquire_v1(private.timesheet_correction_a0_fence_tokens_v1(
    null,v_ids,v_bookings,null,null),false);
  -- FIN's native owners retain every financial rule/admission. The only added
  -- fact is raw PHYSICAL_UUID serialization before native FIN/AFTER effects.
  -- Existing T0 parent locks do not refresh an older RR snapshot.
  if tg_table_name='timesheets_financials' then
    if tg_op='DELETE' then return old;end if;return new;
  end if;
  if v_ids is not null or v_bookings is not null then
    perform private.timesheet_correction_a0_writer_admit_v1(v_ids,v_bookings,
      case when tg_table_name='invoice_lines' then 'INVOICE_REFERENCE' else 'VALIDATION_REFERENCE' end);
  end if;
  if tg_op='DELETE' then return old;end if;return new;
end;
$function$;
alter function private.timesheet_correction_a0_reference_before_v1() OWNER TO CURRENT_USER;
revoke all on function private.timesheet_correction_a0_reference_before_v1() from public,anon,authenticated,service_role;

create or replace function private.timesheet_correction_a0_booking_before_v1()
returns trigger language plpgsql volatile security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare v_ids uuid[];v_bookings text[];v_parents uuid[];v_keys text[];v_aliases jsonb[];v_admit jsonb;r record;v_match boolean;
begin
  if tg_table_schema<>'public' or tg_table_name<>'timesheets' or tg_op not in('INSERT','UPDATE','DELETE') then
    raise exception 'CORRECTION_A0_WRITER_TRIGGER_CONTEXT_INVALID' using errcode='55000';end if;
  if tg_op='DELETE' then
    v_ids:=array[old.timesheet_id];v_bookings:=array[old.booking_id];v_parents:=array[old.parent_timesheet_id];
    v_keys:=array[old.occupant_key_norm];
  elsif tg_op='UPDATE' then
    v_ids:=array[old.timesheet_id,new.timesheet_id];v_bookings:=array[old.booking_id,new.booking_id];
    v_parents:=array[old.parent_timesheet_id,new.parent_timesheet_id];v_keys:=array[old.occupant_key_norm,new.occupant_key_norm];
  else
    v_ids:=array[new.timesheet_id];v_bookings:=array[new.booking_id];v_parents:=array[new.parent_timesheet_id];v_keys:=array[new.occupant_key_norm];
  end if;
  select array_agg(to_jsonb(array[k.value]::text[])) into v_aliases from unnest(v_keys) k(value);
  -- Full Timesheet authority/signature/schedule/rank mutation coverage, not
  -- merely booking/Contract changes. Parent is a native raw UUID, not a resolver.
  perform private.timesheet_correction_a0_fence_acquire_v1(private.timesheet_correction_a0_fence_tokens_v1(
    v_parents,v_ids||v_parents,v_bookings,v_keys,v_aliases),false);
  v_admit:=private.timesheet_correction_a0_writer_admit_v1(v_ids,v_bookings,'BOOKING_IDENTITY');
  if tg_op='DELETE' then return old;end if;
  for r in select x.value from jsonb_array_elements(v_admit->'requests') x(value) loop
    v_match:=btrim(new.booking_id) in(btrim(r.value->>'original_booking'),btrim(r.value->>'reversed_booking'),btrim(r.value->>'p0_booking'));
    if v_match and (new.contract_id is distinct from (r.value->>'contract_id')::uuid
      or new.booking_id not in(r.value->>'original_booking',r.value->>'reversed_booking',r.value->>'p0_booking')) then
      raise exception 'CORRECTION_A0_WRITER_IDENTITY_UNPROVED' using errcode='55000';end if;
    if new.timesheet_id=(r.value->>'p0')::uuid and (new.booking_id is distinct from r.value->>'p0_booking'
      or new.contract_id is distinct from (r.value->>'contract_id')::uuid) then
      raise exception 'CORRECTION_A0_WRITER_IDENTITY_UNPROVED' using errcode='55000';end if;
    if new.timesheet_id=(r.value->>'original')::uuid and (new.booking_id is distinct from r.value->>'reversed_booking'
      or new.contract_id is distinct from (r.value->>'contract_id')::uuid) then
      raise exception 'CORRECTION_A0_WRITER_IDENTITY_UNPROVED' using errcode='55000';end if;
    if new.timesheet_id=(r.value->>'root')::uuid and new.booking_id is distinct from r.value->>'original_booking' then
      raise exception 'CORRECTION_A0_WRITER_IDENTITY_UNPROVED' using errcode='55000';end if;
  end loop;
  return new;
end;
$function$;
alter function private.timesheet_correction_a0_booking_before_v1() OWNER TO CURRENT_USER;
revoke all on function private.timesheet_correction_a0_booking_before_v1() from public,anon,authenticated,service_role;

-- Concrete existing-table trigger proposal, not a new FK or independent root.
-- Root must qualify enablement/order and every existing trigger's mutation of
-- the protected reference fields before installing or exposing this closure.
drop trigger if exists zzzz_healthroster_a0_reference_before_v1 on public.timesheet_validations;
create trigger zzzz_healthroster_a0_reference_before_v1 before insert or update or delete on public.timesheet_validations
for each row execute function private.timesheet_correction_a0_reference_before_v1();
-- Native FIN direct writers and synchronous late hooks use this same raw
-- physical fence. No Bank function or native financial decision is replaced.
drop trigger if exists aaa_healthroster_a0_financial_before_v1 on public.timesheets_financials;
create trigger aaa_healthroster_a0_financial_before_v1 before insert or update or delete on public.timesheets_financials
for each row execute function private.timesheet_correction_a0_reference_before_v1();
-- No existing invoice BEFORE owner changes the reference; this guard runs
-- before archived guard's parent intent so actual active gates/I-1 come first.
drop trigger if exists aaa_healthroster_a0_reference_before_v1 on public.invoice_lines;
create trigger aaa_healthroster_a0_reference_before_v1 before insert or update or delete on public.invoice_lines
for each row execute function private.timesheet_correction_a0_reference_before_v1();
-- Earliest BEFORE admission is required: the retained native booking-change
-- guards can acquire the native family key. No reviewed BEFORE owner rewrites
-- booking_id/contract_id; root must refuse install if that census changes.
drop trigger if exists aaa_healthroster_a0_booking_before_v1 on public.timesheets;
create trigger aaa_healthroster_a0_booking_before_v1 before insert or update or delete on public.timesheets
for each row execute function private.timesheet_correction_a0_booking_before_v1();

-- The real context reader ranks key_norm, then nhsp_hr_name_aliases, then
-- updated_at/created_at. ALL Candidate mutations can change that ranking,
-- including an unrelated edit of a foreign row with an unchanged shared alias.
create or replace function private.timesheet_correction_a0_candidate_before_v1()
returns trigger language plpgsql volatile security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare v_old_key text; v_new_key text; v_old_aliases jsonb; v_new_aliases jsonb; v_ids uuid[];
begin
  if tg_table_schema<>'public' or tg_table_name<>'candidates' or tg_op not in('INSERT','UPDATE','DELETE') then
    raise exception 'CORRECTION_A0_WRITER_TRIGGER_CONTEXT_INVALID' using errcode='55000'; end if;
  if tg_op<>'INSERT' then v_old_key:=old.key_norm;v_old_aliases:=old.nhsp_hr_name_aliases; end if;
  if tg_op<>'DELETE' then v_new_key:=new.key_norm;v_new_aliases:=new.nhsp_hr_name_aliases; end if;
  -- Raw OLD/NEW key/alias tokens precede every snapshot-visible classification.
  -- Even a born-later physical family and unchanged foreign ranking alias
  -- intersect the actual retained occupant-domain tokens.
  perform private.timesheet_correction_a0_fence_acquire_v1(private.timesheet_correction_a0_fence_tokens_v1(
    null,null,null,array[v_old_key,v_new_key],array[v_old_aliases,v_new_aliases]),false);
  -- Exact same equality/JSON containment as tsfin_load_context_batch; no
  -- additional name normalization, Contract identity fallback or new resolver.
  select array_agg(distinct t.timesheet_id order by t.timesheet_id) into v_ids from public.timesheets t
    where t.occupant_key_norm is not null and (t.occupant_key_norm in(v_old_key,v_new_key)
      or v_old_aliases @> to_jsonb(array[t.occupant_key_norm]::text[])
      or v_new_aliases @> to_jsonb(array[t.occupant_key_norm]::text[]))
      and (private.timesheet_correction_a0_signed_precursor_v1(t.timesheet_id)
        or exists(select 1 from private.timesheet_correction_requests q where q.state='WAITING'
          and q.origin='SIGNED_TIMESHEET_CORRECTION' and q.reversed_financial_id is null
          and q.frozen_original_json#>>'{timesheet,sheet_scope}'='WEEKLY'
          and t.timesheet_id in(q.root_timesheet_id,q.reversed_timesheet_id,q.replacement_timesheet_id)));
  if v_ids is not null then perform private.timesheet_correction_a0_writer_admit_v1(v_ids,null,'CANDIDATE_IDENTITY'); end if;
  if tg_op='DELETE' then return old; end if; return new;
end;
$function$;
alter function private.timesheet_correction_a0_candidate_before_v1() OWNER TO CURRENT_USER;
revoke all on function private.timesheet_correction_a0_candidate_before_v1() from public,anon,authenticated,service_role;
drop trigger if exists zzzz_healthroster_a0_candidate_before_v1 on public.candidates;
create trigger zzzz_healthroster_a0_candidate_before_v1 before insert or update or delete on public.candidates
for each row execute function private.timesheet_correction_a0_candidate_before_v1();

-- Exact missing-original-finance SEND arm only; no ordinary authorisation,
-- pricing, signature, invoice or financial producer is replaced.
create or replace function private.timesheet_correction_a0_send_family_fence_v1(p_timesheet_id uuid)
returns void language plpgsql volatile security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  t public.timesheets%rowtype; c public.contracts%rowtype; v_chain jsonb; v_after_chain jsonb;
  v_ids uuid[]; v_before jsonb; v_after jsonb; v_gate jsonb; v_lock jsonb; v_timeout text:=current_setting('lock_timeout');
  v_context jsonb; v_context_count integer; v_signed jsonb;
begin
  select ts.* into t from public.timesheets ts where ts.timesheet_id=p_timesheet_id;
  if not found then return; end if;
  -- This invocation is an exact new correction purpose. Existing finance,
  -- ordinary, Daily and unsigned agency commands retain their native body.
  if t.sheet_scope::text<>'WEEKLY' or exists(select 1 from public.timesheets_financials f where f.timesheet_id=t.timesheet_id)
    or private.timesheet_correction_origin_v1(t.timesheet_id) is distinct from 'SIGNED_TIMESHEET_CORRECTION' then return; end if;
  -- Reuse the exact native immutable manager-pair admission before this new
  -- purpose fence. An unsigned record retains its existing signature refusal.
  v_signed:=private.timesheet_correction_signed_witness_v1(t.timesheet_id,false);
  -- Saved endpoint source does not force transaction snapshot isolation. Do
  -- not substitute a stale RR snapshot for fresh postlock missing-A0 truth.
  if current_setting('transaction_isolation')<>'read committed' then
    raise exception 'CORRECTION_A0_TRANSACTION_SNAPSHOT_UNPROVED' using errcode='55000'; end if;
  select contract.* into c from public.contracts contract where contract.id=t.contract_id;
  if not found or c.candidate_id is null or c.client_id is null then
    raise exception 'CORRECTION_A0_WRITER_SCOPE_UNPROVED' using errcode='55000'; end if;
  v_chain:=public.timesheet_correction_chain_scope_v1(t.timesheet_id,false,32,100);
  if v_chain->'valid' is distinct from 'true'::jsonb or v_chain->>'latest_positive_timesheet_id' is distinct from t.timesheet_id::text then
    raise exception 'CORRECTION_A0_WRITER_SCOPE_UNPROVED' using errcode='55000'; end if;
  select array_agg(distinct x.id order by x.id) into v_ids from unnest(array[t.timesheet_id,(v_chain->>'root_timesheet_id')::uuid]) x(id);
  select jsonb_agg(to_jsonb(s) order by s.requested_timesheet_id,s.family_version,s.family_timesheet_id) into v_before
    from public._pay_timesheet_rotation_scope(v_ids) s;
  begin
    perform set_config('lock_timeout','1ms',true);
    v_gate:=private.weekly_source_candidate_serial_gate_v1(c.candidate_id,'WORKBENCH_CANDIDATE_FIRST_AUTHORISATION',
      t.timesheet_id,'CORRECTION_A0_SEND_FIRST_ADMISSION');
    if v_gate->'ok' is distinct from 'true'::jsonb or v_gate->>'gate' is distinct from 'GRANTED' then
      raise exception 'CORRECTION_A0_AUTHORITY_BUSY' using errcode='55P03'; end if;
    perform set_config('lock_timeout','1ms',true);
    v_lock:=private.weekly_source_lock_family_rows_v1(v_ids,c.candidate_id);
    if v_lock->'ok' is distinct from 'true'::jsonb then
      raise exception 'CORRECTION_A0_WRITER_SCOPE_UNPROVED' using errcode='55000'; end if;
    perform set_config('lock_timeout',v_timeout,true);
  exception when others then perform set_config('lock_timeout',v_timeout,true); raise; end;
  select jsonb_agg(to_jsonb(s) order by s.requested_timesheet_id,s.family_version,s.family_timesheet_id) into v_after
    from public._pay_timesheet_rotation_scope(v_ids) s;
  v_after_chain:=public.timesheet_correction_chain_scope_v1(t.timesheet_id,false,32,100);
  if v_before is null or v_after is distinct from v_before or v_after_chain is distinct from v_chain
    or private.timesheet_correction_signed_witness_v1(t.timesheet_id,false) is distinct from v_signed then
    raise exception 'CORRECTION_A0_WRITER_SCOPE_CHANGED' using errcode='40001'; end if;
  -- A raw null/foreign-Contract sibling committed before SEND is not valid
  -- missing-A0 family evidence. Refuse this purpose, never guess its history.
  if exists(select 1 from public._pay_timesheet_rotation_scope(v_ids) s
    join public.timesheets m on m.timesheet_id=s.family_timesheet_id
    left join public.contracts mc on mc.id=m.contract_id
    where m.contract_id is null or mc.candidate_id is distinct from c.candidate_id or mc.client_id is distinct from c.client_id) then
    raise exception 'CORRECTION_A0_FAMILY_IDENTITY_UNPROVED' using errcode='55000'; end if;
  if exists(select 1 from public.timesheets_financials f where f.timesheet_id=t.timesheet_id)
    or not exists(select 1 from public.timesheets current_ts where current_ts.timesheet_id=t.timesheet_id and current_ts.is_current
      and current_ts.version=t.version and current_ts.booking_id=t.booking_id and current_ts.contract_id=t.contract_id)
    or private.timesheet_correction_origin_v1(t.timesheet_id) is distinct from 'SIGNED_TIMESHEET_CORRECTION' then
    raise exception 'CORRECTION_A0_WRITER_SCOPE_CHANGED' using errcode='40001'; end if;
  -- Phase EXISTING follows the unchanged native Candidate then whole I1.
  -- Acquire all actual existing family identities before canonical reread.
  select array_agg(distinct s.family_timesheet_id order by s.family_timesheet_id) into v_ids
    from public._pay_timesheet_rotation_scope(v_ids) s;
  perform private.timesheet_correction_a0_family_tokens_retain_v1((v_chain->>'root_timesheet_id')::uuid,v_ids);
  -- Bound acquisition can resolve an already-running ordinary probe. Recheck
  -- actual family/signature/chain after PHYSICAL and DEPENDENCY retention.
  select jsonb_agg(to_jsonb(s) order by s.requested_timesheet_id,s.family_version,s.family_timesheet_id)
    into v_after from public._pay_timesheet_rotation_scope(array[t.timesheet_id,(v_chain->>'root_timesheet_id')::uuid]) s;
  if v_after is distinct from v_before
    or public.timesheet_correction_chain_scope_v1(t.timesheet_id,false,32,100) is distinct from v_chain
    or private.timesheet_correction_signed_witness_v1(t.timesheet_id,false) is distinct from v_signed
    or exists(select 1 from public.timesheets_financials f where f.timesheet_id=t.timesheet_id) then
    raise exception 'CORRECTION_A0_TOKEN_DEPENDENCY_CHANGED' using errcode='40001';end if;
  select count(*),jsonb_agg(to_jsonb(ctx))->0 into v_context_count,v_context
    from public.tsfin_load_context_batch(array[t.timesheet_id]) ctx;
  if v_context_count<>1 or v_context->>'effective_timesheet_id' is distinct from t.timesheet_id::text
    or v_context#>>'{out_candidate,id}' is distinct from c.candidate_id::text
    or v_context->>'out_client_id' is distinct from c.client_id::text
    or v_context->'out_cur_fin' is distinct from 'null'::jsonb then
    raise exception 'CORRECTION_A0_FAMILY_IDENTITY_UNPROVED' using errcode='55000'; end if;
end;
$function$;
alter function private.timesheet_correction_a0_send_family_fence_v1(uuid) OWNER TO CURRENT_USER;
revoke all on function private.timesheet_correction_a0_send_family_fence_v1(uuid) from public,anon,authenticated,service_role;

create or replace function private.timesheet_correction_week_allocate_v1(
  p_contract_id uuid,p_week_ending date,p_submission public.submission_mode_enum,p_planned_id uuid,p_now timestamptz
) returns uuid language plpgsql security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare v_seq integer; v_id uuid:=coalesce(p_planned_id,gen_random_uuid());
begin
  perform pg_advisory_xact_lock(hashtextextended('CORRECTION_ADJUSTMENT_WEEK|'||p_contract_id||'|'||p_week_ending,0));
  perform 1 from public.contract_weeks w where w.contract_id=p_contract_id and w.week_ending_date=p_week_ending
    order by w.id for update;
  select coalesce(max(w.additional_seq),0)+1 into v_seq from public.contract_weeks w
    where w.contract_id=p_contract_id and w.week_ending_date=p_week_ending;
  insert into public.contract_weeks(id,contract_id,week_ending_date,additional_seq,is_adjustment,submission_mode_snapshot,status,created_at,updated_at)
    values(v_id,p_contract_id,p_week_ending,v_seq,true,p_submission,'SUBMITTED',p_now,p_now);
  return v_id;
end;
$function$;

create or replace function public.timesheet_correction_workflow_projection_v1(p_workflow_id uuid,p_candidate_id uuid)
returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare v_workflow public.candidate_submission_workflows%rowtype; v_request private.timesheet_correction_requests%rowtype;
begin
  select w.* into v_workflow from public.candidate_submission_workflows w
    where w.id=p_workflow_id and w.candidate_id=p_candidate_id;
  if not found then return null; end if;
  select request.* into v_request from private.timesheet_correction_requests request
    where request.candidate_id=p_candidate_id
      and request.replacement_timesheet_id in(v_workflow.target_timesheet_id,v_workflow.anchor_timesheet_id)
    order by request.created_at_utc desc,request.id desc limit 1;
  if not found then return null; end if;
  return private.timesheet_correction_projection_by_request_v1(v_request.id,p_candidate_id);
end;
$function$;

-- Physical family allocation follows the existing adjustment-week owner shape.
-- The booking below is physical identity only; original_booking_id remains
-- immutable business authority and no economic-key derivation is introduced.
create or replace function private.timesheet_correction_leg_create_v1(
  p_request private.timesheet_correction_requests,p_kind text,p_schedule jsonb,p_now timestamptz
) returns jsonb language plpgsql security definer
set search_path to 'pg_catalog','public','private','extensions','pg_temp'
as $function$
declare v_original public.timesheets%rowtype; v_id uuid; v_booking text; v_week uuid; v_seq integer; v_json jsonb;
begin
  if p_kind not in('WHOLE_TIMESHEET_REVERSAL','WHOLE_TIMESHEET_REPLACEMENT') or jsonb_typeof(p_schedule)<>'array' then
    raise exception 'CORRECTION_LEG_SHAPE_INVALID' using errcode='22023';
  end if;
  select t.* into strict v_original from public.timesheets t where t.timesheet_id=p_request.reversed_timesheet_id;
  v_id:=case when p_kind='WHOLE_TIMESHEET_REPLACEMENT' then p_request.replacement_timesheet_id
    else(p_request.frozen_original_json->>'planned_reversal_timesheet_id')::uuid end;
  v_booking:=case when p_kind='WHOLE_TIMESHEET_REPLACEMENT' then p_request.replacement_booking_id
    else 'bk_awc_'||substr(encode(digest(p_request.id::text||'|R0','sha256'),'hex'),1,24) end;
  if v_original.sheet_scope::text='WEEKLY' then
    if p_request.contract_id is null then raise exception 'CORRECTION_WEEKLY_CONTRACT_REQUIRED' using errcode='55000'; end if;
    if p_kind='WHOLE_TIMESHEET_REPLACEMENT' then
      v_week:=p_request.replacement_week_id;
      if not exists(select 1 from public.contract_weeks w where w.id=v_week and w.contract_id=p_request.contract_id
        and w.week_ending_date=p_request.week_ending_date and w.is_adjustment and w.timesheet_id is null) then
        raise exception 'CORRECTION_P0_WEEK_AUTHORITY_INVALID' using errcode='55000';
      end if;
    else
      v_week:=private.timesheet_correction_week_allocate_v1(p_request.contract_id,p_request.week_ending_date,
        v_original.submission_mode,(p_request.frozen_original_json->>'planned_reversal_week_id')::uuid,p_now);
    end if;
  end if;
  v_json:=to_jsonb(v_original)||jsonb_build_object(
    'timesheet_id',v_id,'booking_id',v_booking,'version',1,'is_current',true,'status','RECEIVED',
    'is_adjustment',true,'parent_timesheet_id',p_request.root_timesheet_id,'correction_id',p_request.correction_id,
    'correction_kind',p_kind,'adjustment_origin',p_request.origin,'actual_schedule_json',p_schedule,
    'additional_units_week','{}'::jsonb,'additional_units_per_day','{}'::jsonb,
    'authorised_at_server',null,'auth_name',null,'auth_job_title',null,'r2_nurse_key',null,'r2_auth_key',null,
    'img_sha256_nurse',null,'img_sha256_auth',null,'manual_pdf_r2_key',null,'manual_document_asset_id',null,
    'qr_status',null,'qr_token',null,'qr_payload_json','{}'::jsonb,'qr_signed_hash',null,'qr_signed_at_utc',null,
    'qr_generated_at',null,'qr_scanned_at',null,'qr_scan_info_json',null,'qr_r2_key',null,
    'qr_last_sent_hash',null,'qr_last_sent_at_utc',null,'qr_sent_refs_sig',null,'qr_sent_refs_snapshot_json',null,
    'qr_sent_refs_captured_at_utc',null,'generated_pdf_at_utc',null,'generated_pdf_refs_sig',null,
    'generated_pdf_refs_snapshot_json',null,'generated_pdf_refs_captured_at_utc',null)
    ||jsonb_build_object(
    'revoked_at',null,'revoked_reason',null,'revoked_by',null,'archived_at_utc',null,'archived_by_user_id',null,
    'archived_reason_code',null,'idempotency_key',null,'client_hash',null,'client_ua',null,
    'worked_start_iso',null,'worked_end_iso',null,'break_start_iso',null,'break_end_iso',null,'break_minutes',null,'worked_minutes',null,
    'document_revision',1,'document_state','STALE','current_document_version_id',null,'active_document_operation_id',null,
    'last_document_error_json',null,'candidate_workflow_id',null,'candidate_workflow_generation',null,
    'candidate_manager_approved_at_utc',null,'candidate_submission_route_intent',null,
    'reference_number',null,'reference_set_at',null,'day_references_json',null,'created_at',p_now,'updated_at',p_now,
    'candidate_hint_text',jsonb_build_object('timesheet_correction_request',jsonb_build_object(
      'request_id',p_request.id,'root_timesheet_id',p_request.root_timesheet_id,'original_booking_id',p_request.original_booking_id,
      'reversed_timesheet_id',p_request.reversed_timesheet_id,'target_revision',p_request.target_revision,
      'candidate_id',p_request.candidate_id,'client_id',p_request.client_id,'work_date',p_request.work_date),
      'daily_receipt',p_request.frozen_original_json->'daily_receipt'));
  insert into public.timesheets select (jsonb_populate_record(null::public.timesheets,v_json)).*;
  if v_week is not null then update public.contract_weeks set timesheet_id=v_id where id=v_week; end if;
  return jsonb_build_object('timesheet_id',v_id,'booking_id',v_booking,'contract_week_id',v_week);
end;
$function$;

create or replace function public.timesheet_correction_send_v1(p_request jsonb,p_actor_user_id uuid,p_now_utc timestamptz default now())
returns jsonb language plpgsql security definer
set search_path to 'pg_catalog','public','private','extensions','pg_temp'
as $function$
declare
  v_now timestamptz:=coalesce(p_now_utc,now()); v_id uuid; v_root uuid; v_ts public.timesheets%rowtype;
  v_fin public.timesheets_financials%rowtype; v_req private.timesheet_correction_requests%rowtype;
  v_existing private.timesheet_correction_requests%rowtype; v_origin text; v_chain jsonb; v_witness jsonb;
  v_invoice uuid; v_invoice_status text; v_key text; v_hash text; v_leg jsonb; v_candidate uuid; v_client uuid; v_date date;
  v_a0_transaction_fence boolean:=false;
begin
  perform private.timesheet_correction_actor_assert_v1(p_actor_user_id);
  if jsonb_typeof(p_request)<>'object' or not pg_input_is_valid(p_request->>'timesheet_id','uuid')
    or not pg_input_is_valid(p_request->>'expected_version','integer')
    or jsonb_typeof(p_request->'proposal') is distinct from 'object'
    or jsonb_typeof(p_request#>'{proposal,schedule}') is distinct from 'array'
    or length(btrim(coalesce(p_request->>'reason',''))) not between 1 and 1000 or nullif(btrim(p_request->>'request_key'),'') is null
    or exists(select 1 from jsonb_object_keys(p_request) k where k not in('timesheet_id','expected_version','proposal','reason','request_key')) then
    raise exception 'CORRECTION_SEND_SHAPE_INVALID' using errcode='22023';
  end if;
  if exists(select 1 from jsonb_object_keys(p_request->'proposal') k where k not in('schedule','changed_dates'))
    or exists(select 1 from jsonb_array_elements(p_request#>'{proposal,schedule}') s where jsonb_typeof(s) is distinct from 'object')
    or (p_request#>'{proposal,changed_dates}' is not null and jsonb_typeof(p_request#>'{proposal,changed_dates}') is distinct from 'array') then
    raise exception 'CORRECTION_SEND_SHAPE_INVALID' using errcode='22023';
  end if;
  v_id:=(p_request->>'timesheet_id')::uuid; v_key:=p_request->>'request_key';
  v_hash:=encode(digest(jsonb_build_object('actor',p_actor_user_id,'request',p_request)::text,'sha256'),'hex');
  -- Serialize exact command receipt before checking it. Concurrent same-key
  -- Send never allocates a second P0 or loses replay to the unique index.
  perform pg_advisory_xact_lock(hashtextextended('CORRECTION_SEND|'||v_key,13102026));
  select request.* into v_existing from private.timesheet_correction_requests request where request.request_key=v_key;
  if found then
    if v_existing.request_fingerprint is distinct from v_hash or v_existing.created_by_user_id is distinct from p_actor_user_id then
      raise exception 'CORRECTION_SEND_REPLAY_CONFLICT' using errcode='40001';
    end if;
    return jsonb_build_object('ok',true,'request_id',v_existing.id,'state',v_existing.state,
      'replacement_timesheet_id',v_existing.replacement_timesheet_id,'state_revision',v_existing.state_revision,'replayed',true);
  end if;
  -- New missing-A0 purpose acquires canonical native Candidate/I-1 before
  -- the existing invoice/chain/workflow/TS order. All other arms return unchanged.
  perform private.timesheet_correction_a0_send_family_fence_v1(v_id);
  v_a0_transaction_fence:=private.timesheet_correction_a0_signed_precursor_v1(v_id);
  -- Lock every existing invoice first, using the same authority as all credit writers.
  perform i.id from public.invoices i where i.id in(select distinct l.invoice_id from public.invoice_lines l
    where l.timesheet_id=v_id) order by i.id for update;
  v_chain:=public.timesheet_correction_chain_scope_v1(v_id,false,32,100);
  if (v_chain->>'valid')::boolean is distinct from true or v_chain->>'latest_positive_timesheet_id' is distinct from v_id::text then
    raise exception 'CORRECTION_LATEST_POSITIVE_REQUIRED' using errcode='55000';
  end if;
  v_root:=(v_chain->>'root_timesheet_id')::uuid;
  perform pg_advisory_xact_lock(hashtextextended('CORRECTION_CHAIN|'||v_root::text,21072026));
  v_chain:=public.timesheet_correction_chain_scope_v1(v_id,false,32,100);
  if (v_chain->>'valid')::boolean is distinct from true or v_chain->>'latest_positive_timesheet_id' is distinct from v_id::text then
    raise exception 'CORRECTION_LATEST_POSITIVE_REQUIRED' using errcode='40001';
  end if;
  select t.* into strict v_ts from public.timesheets t where t.timesheet_id=v_id;
  -- The workflow lock precedes Timesheet/TSFIN locks, matching native Candidate owners.
  if v_ts.candidate_workflow_id is not null then
    perform 1 from public.candidate_submission_workflows w where w.id=v_ts.candidate_workflow_id for update;
  end if;
  select t.* into strict v_ts from public.timesheets t where t.timesheet_id=v_id for update;
  if not v_ts.is_current or v_ts.archived_at_utc is not null or v_ts.version<>(p_request->>'expected_version')::integer then
    raise exception 'CORRECTION_ORIGINAL_CHANGED' using errcode='40001';
  end if;
  v_origin:=private.timesheet_correction_origin_v1(v_id);
  if v_origin is null then raise exception 'CORRECTION_ORIGIN_NOT_ALLOWED' using errcode='55000'; end if;
  if v_origin='SIGNED_TIMESHEET_CORRECTION' then v_witness:=private.timesheet_correction_signed_witness_v1(v_id,true); end if;
  select tf.* into v_fin from public.timesheets_financials tf where tf.timesheet_id=v_id and tf.is_current for update;
  select summary.candidate_id,summary.client_id into v_candidate,v_client from public.v_timesheets_summary_base summary
    where summary.timesheet_id=v_id;
  if v_candidate is null or v_client is null then raise exception 'CORRECTION_BUSINESS_SCOPE_UNPROVED' using errcode='55000'; end if;
  select l.invoice_id,i.status::text into v_invoice,v_invoice_status from public.invoice_lines l
    join public.invoices i on i.id=l.invoice_id where l.timesheet_id=v_id and i.type='INVOICE'
      and i.status in('DRAFT','ISSUED','PAID') group by l.invoice_id,i.status;
  if (select count(distinct l.invoice_id) from public.invoice_lines l join public.invoices i on i.id=l.invoice_id
    where l.timesheet_id=v_id and i.type='INVOICE' and i.status in('DRAFT','ISSUED','PAID'))>1 then
    raise exception 'CORRECTION_ORIGINAL_SPLIT_INVOICES' using errcode='55000';
  end if;
  if v_origin='AGENCY_INVOICE_CORRECTION' and coalesce(v_invoice_status,'') not in('ISSUED','PAID') then
    raise exception 'AGENCY_WHOLE_CORRECTION_REQUIRES_ISSUED_PARENT' using errcode='55000';
  end if;
  v_date:=case when v_ts.sheet_scope::text='DAILY' then coalesce(
    case when pg_input_is_valid(v_ts.candidate_hint_text#>>'{daily_receipt,work_date}','date')
      then(v_ts.candidate_hint_text#>>'{daily_receipt,work_date}')::date end,
    (v_ts.worked_start_iso at time zone 'Europe/London')::date) else v_ts.week_ending_date end;
  if v_date is null then raise exception 'CORRECTION_WORK_DATE_UNPROVED' using errcode='55000'; end if;
  v_req.id:=gen_random_uuid(); v_req.correction_id:='awc:'||v_root||':'||v_req.id;
  v_req.origin:=v_origin; v_req.state:='WAITING'; v_req.state_revision:=1; v_req.proposal_revision:=1;
  v_req.root_timesheet_id:=v_root;
  select t.booking_id into v_req.original_booking_id from public.timesheets t where t.timesheet_id=v_root;
  v_req.reversed_timesheet_id:=v_id; v_req.reversed_version:=v_ts.version; v_req.reversed_financial_id:=v_fin.id;
  v_req.candidate_id:=v_candidate; v_req.client_id:=v_client; v_req.contract_id:=v_ts.contract_id;
  v_req.work_date:=v_date; v_req.week_ending_date:=v_ts.week_ending_date;
  v_req.replacement_timesheet_id:=gen_random_uuid();
  v_req.replacement_booking_id:='bk_awc_'||substr(encode(digest(v_req.id::text||'|P0','sha256'),'hex'),1,24);
  v_req.request_key:=v_key; v_req.request_fingerprint:=v_hash; v_req.reason:=btrim(p_request->>'reason');
  v_req.frozen_original_json:=jsonb_build_object('timesheet',to_jsonb(v_ts),
    'financial_snapshot',case when v_fin.id is not null then to_jsonb(v_fin) end,
    'financial_authority_hash',case when v_fin.id is not null then private.timesheet_correction_financial_authority_hash_v1(to_jsonb(v_fin)) end,
    'planned_reversal_timesheet_id',gen_random_uuid(),
    'planned_reversal_week_id',case when v_ts.sheet_scope::text='WEEKLY' then gen_random_uuid() end,
    'schedule',coalesce(v_ts.actual_schedule_json,'[]'::jsonb),'manager_signed_witness',v_witness,
    'daily_receipt',v_ts.candidate_hint_text->'daily_receipt');
  v_req.proposal_json:=p_request->'proposal';
  v_req.target_revision:=encode(digest(jsonb_build_object('request',v_req.id,'root',v_root,'original_booking',v_req.original_booking_id,
    'reversed',v_id,'version',v_ts.version,'candidate',v_candidate,'client',v_client,'date',v_date,
    'original',v_req.frozen_original_json,'proposal',v_req.proposal_json)::text,'sha256'),'hex');
  v_req.original_invoice_id:=case when v_invoice_status in('ISSUED','PAID') then v_invoice end;
  v_req.return_invoice_id:=case when v_invoice_status='DRAFT' then v_invoice end;
  v_req.created_by_user_id:=p_actor_user_id; v_req.created_at_utc:=v_now; v_req.updated_at_utc:=v_now;
  -- Phase ALLOCATED uses the genuine owner-assigned P0/R0 UUIDs/bookings.
  -- It precedes adjustment-week, WAITING and physical P0 publication.
  if v_a0_transaction_fence then perform private.timesheet_correction_a0_request_tokens_retain_v1(v_req);end if;
  if v_ts.sheet_scope::text='WEEKLY' then
    v_req.replacement_week_id:=private.timesheet_correction_week_allocate_v1(v_req.contract_id,v_req.week_ending_date,
      v_ts.submission_mode,null,v_now);
  end if;
  -- P0 week is now genuinely allocated by the unchanged native owner. Retain
  -- its actual UUID before WAITING/P0 publication; no fabricated early UUID.
  if v_a0_transaction_fence then
    perform private.timesheet_correction_a0_request_tokens_retain_v1(v_req);
    if private.timesheet_correction_signed_witness_v1(v_id,false) is distinct from v_witness
      or public.timesheet_correction_chain_scope_v1(v_id,false,32,100) is distinct from v_chain
      or exists(select 1 from public.timesheets_financials f where f.timesheet_id=v_id)
      or exists(select 1 from public.timesheets t where t.timesheet_id=v_id
        and private.timesheet_correction_timesheet_authority_hash_v1(to_jsonb(t)) is distinct from
          private.timesheet_correction_timesheet_authority_hash_v1(v_req.frozen_original_json->'timesheet')) then
      raise exception 'CORRECTION_A0_TOKEN_DEPENDENCY_CHANGED' using errcode='40001';end if;
  end if;
  -- The exact request exists before native overlap/derived triggers see P0.
  insert into private.timesheet_correction_requests select (v_req).*;
  v_leg:=private.timesheet_correction_leg_create_v1(v_req,'WHOLE_TIMESHEET_REPLACEMENT',
    case when v_origin='SIGNED_TIMESHEET_CORRECTION' then '[]'::jsonb else v_req.proposal_json->'schedule' end,v_now);
  insert into public.audit_events(ts_utc,actor_user_id,object_type,object_id_text,action,after_json,reason)
    values(v_now,p_actor_user_id,'timesheets',v_id::text,'TIMESHEET_CORRECTION_SENT',
      jsonb_build_object('request_id',v_req.id,'replacement_timesheet_id',v_req.replacement_timesheet_id),v_req.reason);
  return jsonb_build_object('ok',true,'request_id',v_req.id,'state','WAITING','state_revision',1,
    'replacement_timesheet_id',v_req.replacement_timesheet_id,'replacement_booking_id',v_req.replacement_booking_id,
    'target_revision',v_req.target_revision,'proposal_revision',1);
end;
$function$;

create or replace function private.timesheet_correction_projection_by_request_v1(p_request_id uuid,p_candidate_id uuid)
returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','private','public','pg_temp'
as $function$
declare v_req private.timesheet_correction_requests%rowtype;
begin
  select request.* into v_req from private.timesheet_correction_requests request
    where request.id=p_request_id and request.candidate_id=p_candidate_id;
  if not found then return null; end if;
  return jsonb_build_object('schema_version',1,'request_id',v_req.id,'correction_id',v_req.correction_id,
    'origin',v_req.origin,'state',v_req.state,'state_revision',v_req.state_revision,'proposal_revision',1,
    'target_revision',v_req.target_revision,'root_timesheet_id',v_req.root_timesheet_id,'reversed_timesheet_id',v_req.reversed_timesheet_id,
    'original_booking_id',v_req.original_booking_id,'replacement_timesheet_id',v_req.replacement_timesheet_id,
    'replacement_booking_id',v_req.replacement_booking_id,'replacement_week_id',v_req.replacement_week_id,
    'candidate_id',v_req.candidate_id,'reason',v_req.reason,
    'original_schedule',v_req.frozen_original_json->'schedule','proposed_schedule',v_req.proposal_json->'schedule',
    'frozen_daily_receipt',v_req.frozen_original_json->'daily_receipt',
    'frozen_daily_shift',case when v_req.frozen_original_json#>>'{timesheet,sheet_scope}'='DAILY' then jsonb_build_object(
      'booking_id',v_req.original_booking_id,'work_date',v_req.work_date,'week_ending_date',v_req.week_ending_date,
      'hospital',v_req.frozen_original_json#>'{timesheet,hospital_norm}',
      'ward',v_req.frozen_original_json#>'{timesheet,ward_norm}',
      'job_title',v_req.frozen_original_json#>'{timesheet,job_title_norm}',
      'shift_type',v_req.frozen_original_json#>'{timesheet,shift_label_norm}',
      'shift_starts_at',v_req.frozen_original_json#>'{timesheet,scheduled_start_iso}',
      'shift_ends_at',v_req.frozen_original_json#>'{timesheet,scheduled_end_iso}') end,
    'original_manager_approved_at_utc',v_req.frozen_original_json#>'{manager_signed_witness,manager_approved_at_utc}',
    'changed_dates',coalesce(v_req.proposal_json->'changed_dates','[]'::jsonb),
    'draft_scope',jsonb_build_object('kind','TIMESHEET_CORRECTION','request_id',v_req.id,
      'replacement_timesheet_id',v_req.replacement_timesheet_id,
      'allowed_timesheet_aliases',case when v_req.state='WAITING' then jsonb_build_array(v_req.replacement_timesheet_id) else '[]'::jsonb end,
      'allowed_booking_aliases',case when v_req.state='WAITING' then jsonb_build_array(v_req.replacement_booking_id) else '[]'::jsonb end));
end;
$function$;

create or replace function public.timesheet_correction_request_projection_v1(p_timesheet_id uuid,p_candidate_id uuid)
returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','private','public','pg_temp'
as $function$
declare v_request_id uuid;
begin
  select request.id into v_request_id from private.timesheet_correction_requests request
    where request.candidate_id=p_candidate_id
      and p_timesheet_id in(request.root_timesheet_id,request.reversed_timesheet_id,request.replacement_timesheet_id,request.reversal_timesheet_id)
    order by case when request.state='WAITING' then 0 else 1 end,request.created_at_utc desc,request.id desc limit 1;
  if not found then return null; end if;
  return private.timesheet_correction_projection_by_request_v1(v_request_id,p_candidate_id);
end;
$function$;

-- CREATE has no workflow yet. An owned physical P0 selector proves only this
-- purpose context; all ordinary validation and errors remain with CREATE.
create or replace function public.timesheet_correction_create_projection_v1(
  p_candidate_id uuid,p_contract_week_id uuid default null,p_target_timesheet_id uuid default null,p_anchor_timesheet_id uuid default null
) returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','private','public','pg_temp'
as $function$
declare v_request private.timesheet_correction_requests%rowtype; v_p public.timesheets%rowtype;
begin
  select request.* into v_request from private.timesheet_correction_requests request
    where request.candidate_id=p_candidate_id
      and ((p_contract_week_id is not null and request.replacement_week_id=p_contract_week_id)
        or (p_target_timesheet_id is not null and request.replacement_timesheet_id=p_target_timesheet_id)
        or (p_anchor_timesheet_id is not null and request.replacement_timesheet_id=p_anchor_timesheet_id))
    order by request.created_at_utc desc,request.id desc limit 1;
  if not found then return null; end if;
  if (p_contract_week_id is not null and p_contract_week_id is distinct from v_request.replacement_week_id)
    or (p_target_timesheet_id is not null and p_target_timesheet_id is distinct from v_request.replacement_timesheet_id)
    or (p_anchor_timesheet_id is not null and p_anchor_timesheet_id is distinct from v_request.replacement_timesheet_id) then
    raise exception 'CANDIDATE_CONTEXT_STALE' using errcode='40001';
  end if;
  if v_request.state<>'CANCELLED' then
    select t.* into v_p from public.timesheets t where t.timesheet_id=v_request.replacement_timesheet_id;
    if v_p.timesheet_id is null or not v_p.is_current or v_p.archived_at_utc is not null
      or v_p.booking_id is distinct from v_request.replacement_booking_id
      or v_p.contract_id is distinct from v_request.contract_id
      or v_p.parent_timesheet_id is distinct from v_request.root_timesheet_id
      or v_p.correction_id is distinct from v_request.correction_id
      or v_p.correction_kind is distinct from 'WHOLE_TIMESHEET_REPLACEMENT' then
      raise exception 'CANDIDATE_CONTEXT_STALE' using errcode='40001';
    end if;
  end if;
  return private.timesheet_correction_projection_by_request_v1(v_request.id,p_candidate_id);
end;
$function$;

-- Bounded exact-request successor proposal. Call-only native Candidate/I-1.
-- No global table mutex, new lock key, calculator, financial or readiness store.
create or replace function private.timesheet_correction_a0_native_lock_fence_v1(p_request_id uuid,p_actor_user_id uuid)
returns jsonb language plpgsql volatile security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  r private.timesheet_correction_requests%rowtype; v_scope jsonb; v_after jsonb;
  v_ids uuid[]; v_members uuid[]; v_groups uuid[]; v_groups_after uuid[]; v_workflows uuid[];
  v_gate jsonb; v_locks jsonb; v_before_families jsonb; v_after_families jsonb;
  v_old_timeout text:=current_setting('lock_timeout'); v_umbrella uuid;
begin
  perform private.timesheet_correction_actor_assert_v1(p_actor_user_id);
  select q.* into strict r from private.timesheet_correction_requests q where q.id=p_request_id;
  if current_setting('transaction_isolation')<>'read committed' then
    raise exception 'CORRECTION_A0_TRANSACTION_SNAPSHOT_UNPROVED' using errcode='55000'; end if;
  v_scope:=private.timesheet_correction_a0_clock_scope_v1(r.id);
  select array_agg(distinct x.id order by x.id) into v_ids from unnest(
    array[r.root_timesheet_id,r.reversed_timesheet_id,r.replacement_timesheet_id]) x(id);
  select array_agg(distinct gc.source_group_id order by gc.source_group_id) into v_groups
    from public.weekly_source_group_clients gc where gc.client_id=r.client_id;
  if v_groups is null or cardinality(v_groups)>64 then
    raise exception 'CORRECTION_A0_LOCK_SCOPE_UNPROVED' using errcode='55000'; end if;
  select jsonb_agg(to_jsonb(s) order by s.requested_timesheet_id,s.family_version,s.family_timesheet_id)
    into v_before_families from public._pay_timesheet_rotation_scope(v_ids) s;
  -- The installed I-1 helper itself locks Timesheet family rows. A Candidate
  -- or expense writer may already own an earlier workflow/component/TS row.
  -- Bound that native acquisition, then use NOWAIT on every later row class.
  -- Refusal raises through the atomic caller, rolling back all unit mutations.
  begin
    perform set_config('lock_timeout','1ms',true);
    v_gate:=private.weekly_source_candidate_serial_gate_v1(r.candidate_id,
      'WORKBENCH_CANDIDATE_FIRST_AUTHORISATION',r.id,'CORRECTION_A0_FIRST_AUTHORISATION');
    if v_gate->'ok' is distinct from 'true'::jsonb or v_gate->>'gate' is distinct from 'GRANTED' then
      raise exception 'CORRECTION_A0_AUTHORITY_BUSY' using errcode='55P03'; end if;
    -- Reassert the acquisition bound at the native I-1 boundary. Future gate
    -- successor bodies must not silently widen this exact-request envelope.
    perform set_config('lock_timeout','1ms',true);
    v_locks:=private.weekly_source_lock_family_rows_v1(v_ids,r.candidate_id);
    if v_locks->'ok' is distinct from 'true'::jsonb then
      raise exception 'CORRECTION_A0_LOCK_SCOPE_UNPROVED' using errcode='55000'; end if;
    perform set_config('lock_timeout',v_old_timeout,true);
  exception when others then
    perform set_config('lock_timeout',v_old_timeout,true);
    raise;
  end;
  select jsonb_agg(to_jsonb(s) order by s.requested_timesheet_id,s.family_version,s.family_timesheet_id)
    into v_after_families from public._pay_timesheet_rotation_scope(v_ids) s;
  if v_before_families is null or v_after_families is distinct from v_before_families then
    raise exception 'CORRECTION_A0_LOCK_SCOPE_CHANGED' using errcode='40001'; end if;
  select array_agg(distinct s.family_timesheet_id order by s.family_timesheet_id) into v_members
    from public._pay_timesheet_rotation_scope(v_ids) s;
  if v_members is null or cardinality(v_members)>256 then
    raise exception 'CORRECTION_A0_LOCK_SCOPE_UNPROVED' using errcode='55000'; end if;
  if not pg_try_advisory_xact_lock(hashtextextended('CORRECTION_CHAIN|'||r.root_timesheet_id::text,21072026)) then
    raise exception 'CORRECTION_A0_AUTHORITY_BUSY' using errcode='55P03'; end if;
  perform 1 from private.timesheet_correction_requests q where q.id=r.id for update nowait;
  -- Sorted native groups precede Client/configuration locks. No later family
  -- acquisition is permitted except native reentry or the fresh unpublished R0.
  perform 1 from public.weekly_source_groups g where g.id=any(v_groups) order by g.id for update nowait;
  perform 1 from public.clients c where c.id=r.client_id for update nowait;
  perform 1 from public.candidates c where c.id=r.candidate_id for update nowait;
  select c.umbrella_id into v_umbrella from public.candidates c where c.id=r.candidate_id;
  perform 1 from public.umbrellas u where u.id=v_umbrella for share nowait;
  perform 1 from public.tms_users a where a.id=p_actor_user_id for share nowait;
  perform 1 from public.contracts c where c.id=r.contract_id for update nowait;
  perform 1 from public.weekly_source_group_clients gc where gc.client_id=r.client_id order by gc.id for update nowait;
  perform 1 from public.weekly_source_group_rule_assignments a where a.source_group_id=any(v_groups) order by a.id for update nowait;
  perform 1 from public.weekly_source_client_policies p where p.client_id=r.client_id order by p.id for update nowait;
  perform 1 from public.client_settings c where c.client_id=r.client_id order by c.id for update nowait;
  select array_agg(distinct w.id order by w.id) into v_workflows from public.candidate_submission_workflows w
    where w.target_timesheet_id=any(v_members) or w.anchor_timesheet_id=any(v_members)
      or w.id in(select t.candidate_workflow_id from public.timesheets t where t.timesheet_id=any(v_members));
  perform 1 from public.candidate_submission_workflows w where w.id=any(v_workflows) order by w.id for update nowait;
  perform 1 from public.contract_weeks w where w.timesheet_id=any(v_members) or w.id=r.replacement_week_id
    or w.id in(select f.contract_week_id from public.candidate_submission_workflows f where f.id=any(v_workflows))
    order by w.id for update nowait;
  perform 1 from public.candidate_approval_requests a where a.workflow_id=any(v_workflows) order by a.id for update nowait;
  perform 1 from public.candidate_submission_components c where c.workflow_id=any(v_workflows)
    or c.timesheet_id=any(v_members) order by c.id for update nowait;
  -- Include inactive/history rows: an UPDATE can reactivate without touching
  -- its FK and would not contend with the parent alone.
  perform 1 from public.timesheets_financials f where f.timesheet_id=any(v_members) order by f.id for update nowait;
  perform 1 from public.nhsp_shifts s where s.timesheet_id=any(v_members) order by s.id for update nowait;
  perform 1 from public.weekly_source_row_timesheet_lineages l where l.timesheet_id=any(v_members) order by l.id for update nowait;
  perform 1 from public.candidate_expense_components e where e.owning_timesheet_id=any(v_members)
    or e.workflow_id=any(v_workflows) order by e.expense_component_id for update nowait;
  perform 1 from public.timesheet_evidence e where e.timesheet_id=any(v_members) order by e.id for update nowait;
  -- No Timesheet FK exists on validations. This fences existing rows only;
  -- the actual insertion owner must separately prove native family admission.
  perform 1 from public.timesheet_validations v where v.timesheet_id=any(v_members) order by v.id for update nowait;
  -- Likewise, invoice_lines.timesheet_id has no Timesheet FK. This protects
  -- existing lines; absence requires qualification of the actual invoice owner.
  perform 1 from public.invoice_lines l where l.timesheet_id=any(v_members) order by l.id for update nowait;
  -- Row locks also hold native RowShare relation locks until unit completion.
  -- Require the actual FK fence and enabled RI triggers, never a guessed FK.
  if current_setting('session_replication_role')<>'origin' or exists(
    select 1 from (values
      ('public.timesheets_financials','timesheets_financials_timesheet_id_fkey','timesheet_id','public.timesheets','timesheet_id'),
      ('public.nhsp_shifts','nhsp_shifts_timesheet_id_fkey','timesheet_id','public.timesheets','timesheet_id'),
      ('public.candidate_expense_components','candidate_expense_components_owning_timesheet_id_fkey','owning_timesheet_id','public.timesheets','timesheet_id'),
      ('public.weekly_source_row_timesheet_lineages','weekly_source_row_timesheet_lineages_timesheet_id_fkey','timesheet_id','public.timesheets','timesheet_id'),
      ('public.weekly_source_group_clients','weekly_source_group_clients_client_id_fkey','client_id','public.clients','id'),
      ('public.weekly_source_group_clients','weekly_source_group_clients_source_group_id_fkey','source_group_id','public.weekly_source_groups','id'),
      ('public.weekly_source_client_policies','weekly_source_client_policies_client_id_fkey','client_id','public.clients','id'),
      ('public.client_settings','client_settings_client_id_fkey','client_id','public.clients','id'),
      ('public.weekly_source_group_rule_assignments','weekly_source_group_rule_assignments_source_group_id_fkey','source_group_id','public.weekly_source_groups','id'),
      ('public.candidate_approval_requests','candidate_approval_requests_workflow_id_fkey','workflow_id','public.candidate_submission_workflows','id'),
      ('public.candidate_submission_components','candidate_submission_components_workflow_id_fkey','workflow_id','public.candidate_submission_workflows','id'),
      ('public.candidate_submission_components','candidate_submission_components_timesheet_id_fkey','timesheet_id','public.timesheets','timesheet_id'),
      ('public.timesheet_evidence','timesheet_evidence_timesheet_id_fkey','timesheet_id','public.timesheets','timesheet_id'),
      ('public.candidate_submission_workflows','candidate_submission_workflows_target_timesheet_id_fkey','target_timesheet_id','public.timesheets','timesheet_id'),
      ('public.candidate_submission_workflows','candidate_submission_workflows_anchor_timesheet_id_fkey','anchor_timesheet_id','public.timesheets','timesheet_id'),
      ('public.contract_weeks','contract_weeks_contract_id_fkey','contract_id','public.contracts','id')
    ) expected(child,name,child_column,parent,parent_column)
    where not exists(select 1 from pg_catalog.pg_constraint c
      where c.conrelid=to_regclass(expected.child) and c.conname=expected.name and c.contype='f'
        and c.confrelid=to_regclass(expected.parent) and c.convalidated and not c.condeferrable
        and c.conkey=array(select a.attnum from pg_catalog.pg_attribute a
          where a.attrelid=c.conrelid and a.attname=expected.child_column and not a.attisdropped)
        and c.confkey=array(select a.attnum from pg_catalog.pg_attribute a
          where a.attrelid=c.confrelid and a.attname=expected.parent_column and not a.attisdropped)
        and exists(select 1 from pg_catalog.pg_trigger t where t.tgconstraint=c.oid and t.tgisinternal
          and t.tgrelid=c.conrelid and (t.tgtype&4)=4 and t.tgenabled in('O','A'))
        and exists(select 1 from pg_catalog.pg_trigger t where t.tgconstraint=c.oid and t.tgisinternal
          and t.tgrelid=c.conrelid and (t.tgtype&16)=16 and t.tgenabled in('O','A'))
        and not exists(select 1 from pg_catalog.pg_trigger t where t.tgconstraint=c.oid and t.tgisinternal and t.tgenabled not in('O','A')))) then
    raise exception 'CORRECTION_A0_FOREIGN_KEY_FENCE_UNPROVED' using errcode='55000'; end if;
  -- Native Candidate / complete I1 / existing row frame already obtained.
  -- PHYSICAL -> DEPENDENCIES -> genuine ALLOCATED, then final current CAS.
  perform private.timesheet_correction_a0_family_tokens_retain_v1(r.root_timesheet_id,v_members);
  perform private.timesheet_correction_a0_request_tokens_retain_v1(r);
  select array_agg(distinct gc.source_group_id order by gc.source_group_id) into v_groups_after
    from public.weekly_source_group_clients gc where gc.client_id=r.client_id;
  v_after:=private.timesheet_correction_a0_clock_scope_v1(r.id);
  if v_groups_after is distinct from v_groups or v_after is distinct from v_scope then
    raise exception 'CORRECTION_A0_LOCK_SCOPE_CHANGED' using errcode='40001'; end if;
  perform private.timesheet_correction_actor_assert_v1(p_actor_user_id);
  return jsonb_build_object('contract','TIMESHEET_CORRECTION_A0_NATIVE_LOCK_FENCE_V1',
    'request_id',r.id,'candidate_id',r.candidate_id,'family_member_ids',v_members,'source_group_ids',v_groups);
end;
$function$;
alter function private.timesheet_correction_a0_native_lock_fence_v1(uuid,uuid) OWNER TO CURRENT_USER;
revoke all on function private.timesheet_correction_a0_native_lock_fence_v1(uuid,uuid) from public,anon,authenticated,service_role;

-- One native arbiter, raw actual OLD/NEW fields only. No child-state resolver,
-- new entitlement, FK, actor flag or replacement financial authority.
create or replace function private.timesheet_correction_a0_child_reference_before_v1()
returns trigger language plpgsql volatile security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare r jsonb;v_rows jsonb[];v_ids uuid[]:=array[]::uuid[];v_workflows uuid[]:=array[]::uuid[];
  v_weeks uuid[]:=array[]::uuid[];v_cycles uuid[]:=array[]::uuid[];v_bookings text[]:=array[]::text[];
begin
  if tg_table_schema<>'public' or tg_op not in('INSERT','UPDATE','DELETE') or tg_table_name not in(
    'candidate_submission_workflows','candidate_approval_requests','candidate_submission_components','candidate_expense_components',
    'timesheet_evidence','nhsp_shifts','weekly_source_row_timesheet_lineages','contract_weeks','weekly_source_cycles') then
    raise exception 'CORRECTION_A0_CHILD_TRIGGER_CONTEXT_INVALID' using errcode='55000';end if;
  if tg_op='INSERT' then v_rows:=array[to_jsonb(new)];elsif tg_op='DELETE' then v_rows:=array[to_jsonb(old)];
    else v_rows:=array[to_jsonb(old),to_jsonb(new)];end if;
  foreach r in array v_rows loop
    -- Every field below is pinned to an actual native UUID/text column. There
    -- is no snapshot-visible lookup before raw physical/workflow/week intent.
    if tg_table_name='candidate_submission_workflows' then
      v_ids:=v_ids||array[(r->>'target_timesheet_id')::uuid,(r->>'anchor_timesheet_id')::uuid];
      v_workflows:=v_workflows||array[(r->>'id')::uuid,(r->>'replacement_of_workflow_id')::uuid];
      v_weeks:=v_weeks||array[(r->>'contract_week_id')::uuid];
    elsif tg_table_name='candidate_approval_requests' then
      v_workflows:=v_workflows||array[(r->>'workflow_id')::uuid];
    elsif tg_table_name='candidate_submission_components' then
      v_ids:=v_ids||array[(r->>'timesheet_id')::uuid];v_workflows:=v_workflows||array[(r->>'workflow_id')::uuid];
    elsif tg_table_name='candidate_expense_components' then
      v_ids:=v_ids||array[(r->>'owning_timesheet_id')::uuid];v_workflows:=v_workflows||array[(r->>'workflow_id')::uuid];
    elsif tg_table_name='weekly_source_row_timesheet_lineages' then
      v_ids:=v_ids||array[(r->>'timesheet_id')::uuid];v_weeks:=v_weeks||array[(r->>'contract_week_id')::uuid];
      v_cycles:=v_cycles||array[(r->>'source_cycle_id')::uuid];v_bookings:=v_bookings||array[r->>'family_booking_id'];
    elsif tg_table_name='weekly_source_cycles' then
      v_cycles:=v_cycles||array[(r->>'id')::uuid];
    elsif tg_table_name='contract_weeks' then
      v_ids:=v_ids||array[(r->>'timesheet_id')::uuid];v_weeks:=v_weeks||array[(r->>'id')::uuid];
    else v_ids:=v_ids||array[(r->>'timesheet_id')::uuid];end if;
  end loop;
  perform private.timesheet_correction_a0_fence_acquire_v1(private.timesheet_correction_a0_fence_tokens_v2(
    null,v_ids,v_bookings,null,null,v_workflows,v_weeks,v_cycles),false);
  -- Native immutable/lineage/financial/state/actor owners retain all decisions.
  if tg_op='DELETE' then return old;end if;return new;
end;
$function$;
alter function private.timesheet_correction_a0_child_reference_before_v1() OWNER TO CURRENT_USER;
revoke all on function private.timesheet_correction_a0_child_reference_before_v1() from public,anon,authenticated,service_role;

drop trigger if exists aaa_healthroster_a0_child_before_v1 on public.candidate_submission_workflows;
create trigger aaa_healthroster_a0_child_before_v1 before insert or update or delete on public.candidate_submission_workflows
  for each row execute function private.timesheet_correction_a0_child_reference_before_v1();
drop trigger if exists aaa_healthroster_a0_child_before_v1 on public.candidate_approval_requests;
create trigger aaa_healthroster_a0_child_before_v1 before insert or update or delete on public.candidate_approval_requests
  for each row execute function private.timesheet_correction_a0_child_reference_before_v1();
drop trigger if exists aaa_healthroster_a0_child_before_v1 on public.candidate_submission_components;
create trigger aaa_healthroster_a0_child_before_v1 before insert or update or delete on public.candidate_submission_components
  for each row execute function private.timesheet_correction_a0_child_reference_before_v1();
drop trigger if exists aaa_healthroster_a0_child_before_v1 on public.candidate_expense_components;
create trigger aaa_healthroster_a0_child_before_v1 before insert or update or delete on public.candidate_expense_components
  for each row execute function private.timesheet_correction_a0_child_reference_before_v1();
drop trigger if exists aaa_healthroster_a0_child_before_v1 on public.timesheet_evidence;
create trigger aaa_healthroster_a0_child_before_v1 before insert or update or delete on public.timesheet_evidence
  for each row execute function private.timesheet_correction_a0_child_reference_before_v1();
drop trigger if exists aaa_healthroster_a0_child_before_v1 on public.nhsp_shifts;
create trigger aaa_healthroster_a0_child_before_v1 before insert or update or delete on public.nhsp_shifts
  for each row execute function private.timesheet_correction_a0_child_reference_before_v1();
drop trigger if exists aaa_healthroster_a0_child_before_v1 on public.weekly_source_row_timesheet_lineages;
create trigger aaa_healthroster_a0_child_before_v1 before insert or update or delete on public.weekly_source_row_timesheet_lineages
  for each row execute function private.timesheet_correction_a0_child_reference_before_v1();
drop trigger if exists aaa_healthroster_a0_child_before_v1 on public.contract_weeks;
create trigger aaa_healthroster_a0_child_before_v1 before insert or update or delete on public.contract_weeks
  for each row execute function private.timesheet_correction_a0_child_reference_before_v1();

drop trigger if exists aaa_healthroster_a0_child_before_v1 on public.weekly_source_cycles;
create trigger aaa_healthroster_a0_child_before_v1 before insert or update or delete on public.weekly_source_cycles
  for each row execute function private.timesheet_correction_a0_child_reference_before_v1();

alter function private.timesheet_correction_canonical_input_assert_v1(jsonb,uuid,integer,uuid,uuid) OWNER TO CURRENT_USER;
revoke all on function private.timesheet_correction_canonical_input_assert_v1(jsonb,uuid,integer,uuid,uuid) from public,anon,authenticated,service_role;
alter function private.timesheet_correction_a0_clock_scope_v1(uuid) OWNER TO CURRENT_USER;
revoke all on function private.timesheet_correction_a0_clock_scope_v1(uuid) from public,anon,authenticated,service_role;
alter function private.timesheet_correction_a0_native_input_witness_v1(uuid) OWNER TO CURRENT_USER;
revoke all on function private.timesheet_correction_a0_native_input_witness_v1(uuid) from public,anon,authenticated,service_role;
alter function private.timesheet_correction_a0_native_lock_fence_v1(uuid,uuid) OWNER TO CURRENT_USER;
revoke all on function private.timesheet_correction_a0_native_lock_fence_v1(uuid,uuid) from public,anon,authenticated,service_role;
alter function private.timesheet_correction_a0_signed_precursor_v1(uuid) OWNER TO CURRENT_USER;
revoke all on function private.timesheet_correction_a0_signed_precursor_v1(uuid) from public,anon,authenticated,service_role;
alter function private.timesheet_correction_a0_writer_admit_v1(uuid[],text[],text) OWNER TO CURRENT_USER;
revoke all on function private.timesheet_correction_a0_writer_admit_v1(uuid[],text[],text) from public,anon,authenticated,service_role;
alter function private.timesheet_correction_a0_reference_before_v1() OWNER TO CURRENT_USER;
revoke all on function private.timesheet_correction_a0_reference_before_v1() from public,anon,authenticated,service_role;
alter function private.timesheet_correction_a0_booking_before_v1() OWNER TO CURRENT_USER;
revoke all on function private.timesheet_correction_a0_booking_before_v1() from public,anon,authenticated,service_role;
alter function private.timesheet_correction_a0_candidate_before_v1() OWNER TO CURRENT_USER;
revoke all on function private.timesheet_correction_a0_candidate_before_v1() from public,anon,authenticated,service_role;
alter function private.timesheet_correction_a0_send_family_fence_v1(uuid) OWNER TO CURRENT_USER;
revoke all on function private.timesheet_correction_a0_send_family_fence_v1(uuid) from public,anon,authenticated,service_role;
alter function private.timesheet_correction_component_negate_v1(jsonb,text[]) OWNER TO CURRENT_USER;
revoke all on function private.timesheet_correction_component_negate_v1(jsonb,text[]) from public,anon,authenticated,service_role;
alter function private.timesheet_correction_native_original_reversal_v1(jsonb,uuid) OWNER TO CURRENT_USER;
revoke all on function private.timesheet_correction_native_original_reversal_v1(jsonb,uuid) from public,anon,authenticated,service_role;
alter function private.timesheet_correction_snapshot_identity_assert_v1(jsonb,uuid,integer,uuid,uuid) OWNER TO CURRENT_USER;
revoke all on function private.timesheet_correction_snapshot_identity_assert_v1(jsonb,uuid,integer,uuid,uuid) from public,anon,authenticated,service_role;
alter function private.timesheet_correction_reversal_component_assert_v1(jsonb,jsonb,text[]) OWNER TO CURRENT_USER;
revoke all on function private.timesheet_correction_reversal_component_assert_v1(jsonb,jsonb,text[]) from public,anon,authenticated,service_role;
alter function private.timesheet_correction_reversal_assert_v1(jsonb,jsonb) OWNER TO CURRENT_USER;
revoke all on function private.timesheet_correction_reversal_assert_v1(jsonb,jsonb) from public,anon,authenticated,service_role;
alter function public.timesheet_correction_prepare_v1(uuid,uuid) OWNER TO CURRENT_USER;
revoke all on function public.timesheet_correction_prepare_v1(uuid,uuid) from public,anon,authenticated,service_role;
grant execute on function public.timesheet_correction_prepare_v1(uuid,uuid) to service_role;
alter function public.timesheet_correction_authorise_atomic_v1(uuid,bigint,jsonb,uuid,timestamp with time zone) OWNER TO CURRENT_USER;
revoke all on function public.timesheet_correction_authorise_atomic_v1(uuid,bigint,jsonb,uuid,timestamp with time zone) from public,anon,authenticated,service_role;
grant execute on function public.timesheet_correction_authorise_atomic_v1(uuid,bigint,jsonb,uuid,timestamp with time zone) to service_role;
alter function private.timesheet_correction_week_allocate_v1(uuid,date,public.submission_mode_enum,uuid,timestamp with time zone) OWNER TO CURRENT_USER;
revoke all on function private.timesheet_correction_week_allocate_v1(uuid,date,public.submission_mode_enum,uuid,timestamp with time zone) from public,anon,authenticated,service_role;
alter function public.timesheet_correction_workflow_projection_v1(uuid,uuid) OWNER TO CURRENT_USER;
revoke all on function public.timesheet_correction_workflow_projection_v1(uuid,uuid) from public,anon,authenticated,service_role;
grant execute on function public.timesheet_correction_workflow_projection_v1(uuid,uuid) to service_role;
alter function private.timesheet_correction_leg_create_v1(private.timesheet_correction_requests,text,jsonb,timestamp with time zone) OWNER TO CURRENT_USER;
revoke all on function private.timesheet_correction_leg_create_v1(private.timesheet_correction_requests,text,jsonb,timestamp with time zone) from public,anon,authenticated,service_role;
alter function public.timesheet_correction_send_v1(jsonb,uuid,timestamp with time zone) OWNER TO CURRENT_USER;
revoke all on function public.timesheet_correction_send_v1(jsonb,uuid,timestamp with time zone) from public,anon,authenticated,service_role;
grant execute on function public.timesheet_correction_send_v1(jsonb,uuid,timestamp with time zone) to service_role;
alter function private.timesheet_correction_projection_by_request_v1(uuid,uuid) OWNER TO CURRENT_USER;
revoke all on function private.timesheet_correction_projection_by_request_v1(uuid,uuid) from public,anon,authenticated,service_role;
alter function public.timesheet_correction_request_projection_v1(uuid,uuid) OWNER TO CURRENT_USER;
revoke all on function public.timesheet_correction_request_projection_v1(uuid,uuid) from public,anon,authenticated,service_role;
grant execute on function public.timesheet_correction_request_projection_v1(uuid,uuid) to service_role;
alter function public.timesheet_correction_create_projection_v1(uuid,uuid,uuid,uuid) OWNER TO CURRENT_USER;
revoke all on function public.timesheet_correction_create_projection_v1(uuid,uuid,uuid,uuid) from public,anon,authenticated,service_role;
grant execute on function public.timesheet_correction_create_projection_v1(uuid,uuid,uuid,uuid) to service_role;
notify pgrst, 'reload schema';


do $source_kernel_verify$
declare e record;p record;role_name text;n integer;
begin
 for e in select * from(values
   ('private.timesheet_correction_a0_source_token_assert_v1(jsonb)','9c740017062e8b796b0e6c7f1c00c79fcf4108d68aa38d81ed2d4000c37ca000','i'),
   ('private.timesheet_correction_a0_fence_scalar_acquire_core_v1(jsonb,boolean)','ecc4320761f57d9c2e6e376121ab07cb5576c925cbaddba2072b1c8e548b6a31','v'),
   ('private.timesheet_correction_a0_source_coverage_acquire_core_v1(text,uuid,daterange,boolean)','a5ca75a91a3bd2725ff2f399f7432c7e5f0052bba07469affca2540686011b49','v'),
   ('private.timesheet_correction_a0_source_coverage_acquire_v1(uuid,daterange,boolean)','7b37741ba0377552a288ae700e95dde53b2f21ca7905bd81b4782071a6a7a2b8','v'),
   ('private.timesheet_correction_a0_fence_acquire_v1(jsonb,boolean)','37eeb5ac3857fb815126e71b725eb264d20ec958ae7a4473248d1425c18abb1b','v')
 ) expected(signature,body_sha256,volatility) loop
  select f.* into p from pg_catalog.pg_proc f where f.oid=to_regprocedure(e.signature);
  if not found or p.proowner<>(select oid from pg_catalog.pg_roles where rolname=current_user)
   or not p.prosecdef or p.provolatile::text<>e.volatility
   or p.proconfig is distinct from array['search_path=pg_catalog, pg_temp']::text[]
   or encode(pg_catalog.sha256(pg_catalog.convert_to(p.prosrc,'UTF8')),'hex')<>e.body_sha256 then
    raise exception 'HEALTHROSTER_SOURCE_KERNEL_OWNER_BODY_PROFILE_INVALID' using errcode='55000';end if;
  foreach role_name in array array['anon','authenticated','service_role'] loop
   if has_function_privilege(role_name,p.oid,'EXECUTE') then
    raise exception 'HEALTHROSTER_SOURCE_KERNEL_PRIVATE_EXECUTE_EXPOSED' using errcode='42501';end if;
  end loop;
 end loop;
 select c.* into p from pg_catalog.pg_class c where c.oid=to_regclass('private.timesheet_correction_a0_transaction_fences');
 if not found or p.relkind<>'r' or not p.relrowsecurity or p.relforcerowsecurity
  or p.relowner<>(select oid from pg_catalog.pg_roles where rolname=current_user) then
   raise exception 'HEALTHROSTER_SOURCE_KERNEL_TABLE_OWNER_RLS_INVALID' using errcode='55000';end if;
 foreach role_name in array array['anon','authenticated','service_role'] loop
  if has_table_privilege(role_name,p.oid,'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER') then
   raise exception 'HEALTHROSTER_SOURCE_KERNEL_TABLE_PRIVILEGE_EXPOSED' using errcode='42501';end if;
 end loop;
 select count(*) into n from pg_catalog.pg_constraint c where c.conrelid=p.oid and c.contype='x'
  and c.conname in('timesheet_correction_a0_fences_uuid_excl','timesheet_correction_a0_fences_text_excl','timesheet_correction_a0_fences_coverage_excl');
 if n<>3 or not exists(select 1 from pg_catalog.pg_constraint c where c.conrelid=p.oid
  and c.conname='timesheet_correction_a0_fences_coverage_excl' and pg_get_constraintdef(c.oid) like '%retained WITH <>%')
  or to_regclass('private.timesheet_correction_a0_fences_coverage_retained_unique') is null then
   raise exception 'HEALTHROSTER_SOURCE_KERNEL_NATIVE_EXCLUSION_PROFILE_INVALID' using errcode='55000';end if;
 if not exists(select 1 from pg_catalog.pg_policy policy where policy.polrelid=p.oid
  and policy.polname='cloudtms_miget_service_owner_all' and policy.polcmd='*'
  and policy.polroles @> array[(select oid from pg_catalog.pg_roles where rolname=current_user),(select oid from pg_catalog.pg_roles where rolname='service_role')]
  and cardinality(policy.polroles)=2 and pg_get_expr(policy.polqual,policy.polrelid)='true'
  and pg_get_expr(policy.polwithcheck,policy.polrelid)='true') then
   raise exception 'HEALTHROSTER_SOURCE_KERNEL_PRIVATE_POLICY_INVALID' using errcode='55000';end if;
end $source_kernel_verify$;
do $a0_source_contract$
declare v_oid oid;r record;v_count integer;
begin
  v_oid:=to_regclass('private.timesheet_correction_a0_transaction_fences');
  if v_oid is null or not exists(select 1 from pg_catalog.pg_class c where c.oid=v_oid and c.relkind='r'
      and c.relrowsecurity and c.relowner=(current_user::regrole)::oid and c.relchecks=2)
    or exists(select 1 from pg_catalog.pg_class c cross join lateral pg_catalog.aclexplode(
      coalesce(c.relacl,pg_catalog.acldefault('r',c.relowner))) a where c.oid=v_oid and a.grantee<>c.relowner)
    or exists(select 1 from pg_catalog.pg_trigger t where t.tgrelid=v_oid)
    or exists(select 1 from pg_catalog.pg_constraint c where c.conrelid=v_oid and c.contype not in('c','x')) then
    raise exception 'CORRECTION_A0_PRIVATE_ARBITER_CONTRACT_INVALID' using errcode='55000';end if;
  if (select count(*) from pg_catalog.pg_attribute a where a.attrelid=v_oid and a.attnum>0 and not a.attisdropped)<>7
    or exists(select 1 from(values(1,'token_kind','text',true),(2,'uuid_identity','uuid',false),
      (3,'text_identity','text',false),(4,'retained','boolean',true),(5,'intent_span','numrange',true),
      (6,'coverage_span','daterange',false),(7,'touch_bit','boolean',true)) e(num,name,typ,nn) where not exists(select 1 from pg_catalog.pg_attribute a
        where a.attrelid=v_oid and a.attnum=e.num and a.attname=e.name and a.atttypid=to_regtype(e.typ)
        and a.attnotnull=e.nn and not a.attisdropped and a.attidentity='' and a.attgenerated=''
        and (e.typ<>'text' or a.attcollation='pg_catalog."C"'::regcollation))) then
    raise exception 'CORRECTION_A0_ARBITER_COLUMNS_CHANGED' using errcode='55000';end if;
  if current_setting('block_size')::integer<>8192 or not exists(select 1 from pg_catalog.pg_extension e
      join pg_catalog.pg_namespace n on n.oid=e.extnamespace where e.extname='btree_gist' and n.nspname='public')
    or not exists(select 1 from pg_catalog.pg_collation c where c.oid='pg_catalog.default'::regcollation and c.collisdeterministic) then
    raise exception 'CORRECTION_A0_NATIVE_EXCLUSION_PROFILE_UNPROVED' using errcode='55000';end if;
  if (select count(*) from pg_catalog.pg_constraint c where c.conrelid=v_oid)<>5
    or (select count(*) from pg_catalog.pg_constraint c where c.conrelid=v_oid and c.contype='c' and c.convalidated
      and c.conname in('timesheet_correction_a0_fences_identity_shape','timesheet_correction_a0_fences_span_shape'))<>2
    or (select count(*) from pg_catalog.pg_policy p where p.polrelid=v_oid)<>1
    or not exists(select 1 from pg_catalog.pg_policy p where p.polrelid=v_oid
      and p.polname='cloudtms_miget_service_owner_all' and p.polcmd='*' and p.polpermissive
      and pg_catalog.pg_get_expr(p.polqual,p.polrelid,false)='true'
      and pg_catalog.pg_get_expr(p.polwithcheck,p.polrelid,false)='true'
      and (current_user::regrole)::oid=any(p.polroles) and 'service_role'::regrole::oid=any(p.polroles)
      and not exists(select 1 from unnest(p.polroles) x(roleid)
        where x.roleid not in((current_user::regrole)::oid,'service_role'::regrole::oid))) then
    raise exception 'CORRECTION_A0_ARBITER_CONSTRAINTS_CHANGED' using errcode='55000';end if;
  for r in select * from(values
    ('candidate_submission_workflows','id','uuid'),
    ('candidate_submission_workflows','target_timesheet_id','uuid'),
    ('candidate_submission_workflows','anchor_timesheet_id','uuid'),
    ('candidate_submission_workflows','contract_week_id','uuid'),
    ('candidate_submission_workflows','replacement_of_workflow_id','uuid'),
    ('candidate_approval_requests','workflow_id','uuid'),
    ('candidate_submission_components','workflow_id','uuid'),
    ('candidate_submission_components','timesheet_id','uuid'),
    ('candidate_expense_components','workflow_id','uuid'),
    ('candidate_expense_components','owning_timesheet_id','uuid'),
    ('timesheet_evidence','timesheet_id','uuid'),
    ('nhsp_shifts','timesheet_id','uuid'),
    ('weekly_source_row_timesheet_lineages','timesheet_id','uuid'),
    ('weekly_source_row_timesheet_lineages','contract_week_id','uuid'),
    ('weekly_source_row_timesheet_lineages','source_cycle_id','uuid'),
    ('weekly_source_row_timesheet_lineages','family_booking_id','text'),
    ('contract_weeks','id','uuid'),
    ('contract_weeks','timesheet_id','uuid'),
    ('weekly_source_cycles','id','uuid'),
    ('timesheets','booking_id','text'),
    ('invoice_lines','booking_id','text'),
    ('timesheets','occupant_key_norm','text'),
    ('candidates','key_norm','text'),
    ('candidates','nhsp_hr_name_aliases','jsonb')
  ) e(table_name,column_name,type_name) loop
    if not exists(select 1 from pg_catalog.pg_attribute a where a.attrelid=to_regclass('public.'||r.table_name)
      and a.attnum>0 and not a.attisdropped and a.attname=r.column_name and a.atttypid=to_regtype(r.type_name)
      and(r.type_name<>'text' or exists(select 1 from pg_catalog.pg_collation c
        where c.oid=a.attcollation and c.collisdeterministic))) then
      raise exception 'CORRECTION_A0_RAW_NATIVE_FIELD_CHANGED' using errcode='55000';end if;
  end loop;
  for r in select * from(values('timesheet_correction_a0_fences_uuid_excl',2,'uuid','gist_uuid_ops','uuid_identity'),
      ('timesheet_correction_a0_fences_text_excl',3,'text','gist_text_ops','text_identity')) e(name,num,typ,opclass,column_name) loop
    if not exists(select 1 from pg_catalog.pg_constraint c join pg_catalog.pg_index i on i.indexrelid=c.conindid
      join pg_catalog.pg_class x on x.oid=i.indexrelid join pg_catalog.pg_am am on am.oid=x.relam
      where c.conrelid=v_oid and c.conname=r.name and c.contype='x' and c.convalidated and not c.condeferrable
        and not c.condeferred and c.conkey=array[1,r.num,5]::smallint[]
        and c.conexclop=array['pg_catalog.=(text,text)'::regoperator::oid,
          to_regoperator('pg_catalog.=('||r.typ||','||r.typ||')')::oid,'pg_catalog.&&(anyrange,anyrange)'::regoperator::oid]
        and am.amname='gist' and i.indisexclusion and i.indisvalid and i.indisready and i.indislive
        and i.indnatts=3 and i.indnkeyatts=3 and i.indexprs is null
        and i.indclass[0]=(select o.oid from pg_catalog.pg_opclass o join pg_catalog.pg_namespace n on n.oid=o.opcnamespace
          where n.nspname='public' and o.opcname='gist_text_ops' and o.opcmethod=x.relam)
        and i.indclass[1]=(select o.oid from pg_catalog.pg_opclass o join pg_catalog.pg_namespace n on n.oid=o.opcnamespace
          where n.nspname='public' and o.opcname=r.opclass and o.opcmethod=x.relam)
        and i.indclass[2]=(select o.oid from pg_catalog.pg_opclass o join pg_catalog.pg_namespace n on n.oid=o.opcnamespace
          where n.nspname='pg_catalog' and o.opcname='range_ops' and o.opcmethod=x.relam)
        and i.indcollation[0]='pg_catalog."C"'::regcollation
        and i.indcollation[1]=case when r.typ='text' then 'pg_catalog."C"'::regcollation::oid else 0::oid end
        and pg_catalog.pg_get_expr(i.indpred,i.indrelid,false)=case when r.typ='uuid' then '((uuid_identity IS NOT NULL) AND (coverage_span IS NULL))' else '(text_identity IS NOT NULL)' end) then
      raise exception 'CORRECTION_A0_EXCLUSION_ARBITER_CHANGED' using errcode='55000';end if;
  end loop;
  for r in select * from(values
    ('private.timesheet_correction_a0_fence_tokens_v2(uuid[],uuid[],text[],text[],jsonb[],uuid[],uuid[],uuid[])','sql','i','search_path=pg_catalog, pg_temp'),
    ('private.timesheet_correction_a0_fence_tokens_v1(uuid[],uuid[],text[],text[],jsonb[])','sql','i','search_path=pg_catalog, pg_temp'),
    ('private.timesheet_correction_a0_fence_acquire_v1(jsonb,boolean)','plpgsql','v','search_path=pg_catalog, pg_temp'),
    ('private.timesheet_correction_a0_family_tokens_retain_v1(uuid,uuid[])','plpgsql','v','search_path=pg_catalog, pg_temp'),
    ('private.timesheet_correction_a0_request_tokens_retain_v1(private.timesheet_correction_requests)','plpgsql','v','search_path=pg_catalog, public, private, extensions, pg_temp'),
    ('private.timesheet_correction_a0_child_reference_before_v1()','plpgsql','v','search_path=pg_catalog, pg_temp')
  ) e(signature,language,volatility,configuration) loop
    if to_regprocedure(r.signature) is null or not exists(select 1 from pg_catalog.pg_proc p
      join pg_catalog.pg_language l on l.oid=p.prolang where p.oid=to_regprocedure(r.signature)
      and p.prosecdef and p.proowner=(current_user::regrole)::oid and l.lanname=r.language
      and p.provolatile::text=r.volatility and p.proconfig=array[r.configuration] and not p.proleakproof)
      or exists(select 1 from pg_catalog.pg_proc p cross join lateral pg_catalog.aclexplode(
        coalesce(p.proacl,pg_catalog.acldefault('f',p.proowner))) a where p.oid=to_regprocedure(r.signature) and a.grantee<>p.proowner)
      or has_function_privilege('anon',to_regprocedure(r.signature),'EXECUTE')
      or has_function_privilege('authenticated',to_regprocedure(r.signature),'EXECUTE')
      or has_function_privilege('service_role',to_regprocedure(r.signature),'EXECUTE') then
      raise exception 'CORRECTION_A0_PRIVATE_HELPER_CONTRACT_CHANGED' using errcode='55000';end if;
  end loop;
  for r in select * from(values
    ('timesheet_validations','zzzz_healthroster_a0_reference_before_v1','private.timesheet_correction_a0_reference_before_v1()'),
    ('invoice_lines','aaa_healthroster_a0_reference_before_v1','private.timesheet_correction_a0_reference_before_v1()'),
    ('timesheets_financials','aaa_healthroster_a0_financial_before_v1','private.timesheet_correction_a0_reference_before_v1()'),
    ('timesheets','aaa_healthroster_a0_booking_before_v1','private.timesheet_correction_a0_booking_before_v1()'),
    ('candidates','zzzz_healthroster_a0_candidate_before_v1','private.timesheet_correction_a0_candidate_before_v1()'),
    ('candidate_submission_workflows','aaa_healthroster_a0_child_before_v1','private.timesheet_correction_a0_child_reference_before_v1()'),
    ('candidate_approval_requests','aaa_healthroster_a0_child_before_v1','private.timesheet_correction_a0_child_reference_before_v1()'),
    ('candidate_submission_components','aaa_healthroster_a0_child_before_v1','private.timesheet_correction_a0_child_reference_before_v1()'),
    ('candidate_expense_components','aaa_healthroster_a0_child_before_v1','private.timesheet_correction_a0_child_reference_before_v1()'),
    ('timesheet_evidence','aaa_healthroster_a0_child_before_v1','private.timesheet_correction_a0_child_reference_before_v1()'),
    ('nhsp_shifts','aaa_healthroster_a0_child_before_v1','private.timesheet_correction_a0_child_reference_before_v1()'),
    ('weekly_source_row_timesheet_lineages','aaa_healthroster_a0_child_before_v1','private.timesheet_correction_a0_child_reference_before_v1()'),
    ('contract_weeks','aaa_healthroster_a0_child_before_v1','private.timesheet_correction_a0_child_reference_before_v1()'),
    ('weekly_source_cycles','aaa_healthroster_a0_child_before_v1','private.timesheet_correction_a0_child_reference_before_v1()')
  ) e(table_name,trigger_name,signature) loop
    if not exists(select 1 from pg_catalog.pg_trigger t where t.tgrelid=to_regclass('public.'||r.table_name)
      and t.tgname=r.trigger_name and t.tgfoid=to_regprocedure(r.signature) and t.tgenabled='O'
      and t.tgtype=31 and t.tgqual is null and t.tgnargs=0 and t.tgattr=''::int2vector and t.tgconstraint=0) then
      raise exception 'CORRECTION_A0_RAW_ALL_MUTATION_GUARD_CHANGED' using errcode='55000';end if;
  end loop;
  for r in select * from(values
    ('candidate_approval_requests','candidate_manager_request_mail_guard_v1','private._candidate_manager_request_mail_guard_v1()','CREATE TRIGGER candidate_manager_request_mail_guard_v1 BEFORE UPDATE OF state ON candidate_approval_requests FOR EACH ROW EXECUTE FUNCTION private._candidate_manager_request_mail_guard_v1()'),
    ('candidate_expense_components','candidate_expense_source_root_guard_v1','private._candidate_expense_source_root_guard_v1()','CREATE TRIGGER candidate_expense_source_root_guard_v1 BEFORE INSERT OR UPDATE OF owning_timesheet_id, lifecycle_state ON candidate_expense_components FOR EACH ROW EXECUTE FUNCTION private._candidate_expense_source_root_guard_v1()'),
    ('candidate_submission_components','candidate_submission_components_immutability_guard','private._candidate_component_immutability_guard_v1()','CREATE TRIGGER candidate_submission_components_immutability_guard BEFORE DELETE OR UPDATE ON candidate_submission_components FOR EACH ROW EXECUTE FUNCTION private._candidate_component_immutability_guard_v1()'),
    ('candidate_submission_workflows','candidate_workflow_creation_identity_guard_v1','private._candidate_workflow_creation_identity_guard_v1()','CREATE TRIGGER candidate_workflow_creation_identity_guard_v1 BEFORE INSERT OR UPDATE ON candidate_submission_workflows FOR EACH ROW EXECUTE FUNCTION private._candidate_workflow_creation_identity_guard_v1()'),
    ('candidate_submission_workflows','trg_timesheet_submission_workflow_overlap_biu','private._timesheet_submission_workflow_overlap_trg_v1()','CREATE TRIGGER trg_timesheet_submission_workflow_overlap_biu BEFORE INSERT OR UPDATE ON candidate_submission_workflows FOR EACH ROW EXECUTE FUNCTION private._timesheet_submission_workflow_overlap_trg_v1()'),
    ('candidates','set_tms_ref','public.trg_set_tms_ref()','CREATE TRIGGER set_tms_ref BEFORE INSERT OR UPDATE ON candidates FOR EACH ROW EXECUTE FUNCTION trg_set_tms_ref()'),
    ('candidates','trg_candidates_set_bank_hash','public._trg_candidates_set_bank_hash()','CREATE TRIGGER trg_candidates_set_bank_hash BEFORE INSERT OR UPDATE OF sort_code, account_number, account_holder ON candidates FOR EACH ROW EXECUTE FUNCTION _trg_candidates_set_bank_hash()'),
    ('candidates','trg_candidates_set_rev','public.trg_set_rev()','CREATE TRIGGER trg_candidates_set_rev BEFORE INSERT OR UPDATE ON candidates FOR EACH ROW EXECUTE FUNCTION trg_set_rev()'),
    ('candidates','trg_candidates_set_updated_at','public.set_updated_at()','CREATE TRIGGER trg_candidates_set_updated_at BEFORE UPDATE ON candidates FOR EACH ROW EXECUTE FUNCTION set_updated_at()'),
    ('contract_weeks','bpay_expense_classification_week_before_v1','private.bpay_expense_classification_week_v1()','CREATE TRIGGER bpay_expense_classification_week_before_v1 BEFORE INSERT OR DELETE OR UPDATE OF timesheet_id, contract_id, week_ending_date, additional_seq, is_adjustment ON contract_weeks FOR EACH ROW EXECUTE FUNCTION private.bpay_expense_classification_week_v1()'),
    ('contract_weeks','contract_week_settings_authority_before_v1','private._contract_week_settings_authority_before_v1()','CREATE TRIGGER contract_week_settings_authority_before_v1 BEFORE INSERT OR UPDATE OF contract_id, week_ending_date, timesheet_id, settings_authority_json, settings_authority_version, settings_authority_fingerprint, settings_authority_resolved_at ON contract_weeks FOR EACH ROW EXECUTE FUNCTION private._contract_week_settings_authority_before_v1()'),
    ('contract_weeks','trg_contract_weeks_set_updated_at','public.set_updated_at()','CREATE TRIGGER trg_contract_weeks_set_updated_at BEFORE UPDATE ON contract_weeks FOR EACH ROW EXECUTE FUNCTION set_updated_at()'),
    ('invoice_lines','trg_invoice_line_archived_timesheet_guard_v1','public.invoice_line_archived_timesheet_guard_v1()','CREATE TRIGGER trg_invoice_line_archived_timesheet_guard_v1 BEFORE INSERT OR UPDATE ON invoice_lines FOR EACH ROW EXECUTE FUNCTION invoice_line_archived_timesheet_guard_v1()'),
    ('invoice_lines','weekly_source_invoice_line_owner_guard','private.weekly_source_invoice_line_owner_guard_v1()','CREATE TRIGGER weekly_source_invoice_line_owner_guard BEFORE INSERT OR DELETE OR UPDATE ON invoice_lines FOR EACH ROW EXECUTE FUNCTION private.weekly_source_invoice_line_owner_guard_v1()'),
    ('timesheet_evidence','timesheet_evidence_candidate_lineage_guard','private._candidate_evidence_lineage_guard_v1()','CREATE TRIGGER timesheet_evidence_candidate_lineage_guard BEFORE INSERT OR UPDATE OF candidate_component_id, document_role, timesheet_id, storage_key ON timesheet_evidence FOR EACH ROW EXECUTE FUNCTION private._candidate_evidence_lineage_guard_v1()'),
    ('timesheet_evidence','trg_timesheet_archived_evidence_guard_v1','public.timesheet_archived_evidence_guard_v1()','CREATE TRIGGER trg_timesheet_archived_evidence_guard_v1 BEFORE INSERT OR DELETE OR UPDATE ON timesheet_evidence FOR EACH ROW EXECUTE FUNCTION timesheet_archived_evidence_guard_v1()'),
    ('timesheets','bpay_expense_classification_ts_before_v1','private.bpay_expense_classification_ts_v1()','CREATE TRIGGER bpay_expense_classification_ts_before_v1 BEFORE INSERT OR UPDATE OF booking_id, contract_id, week_ending_date, sheet_scope, line_type, is_adjustment, settings_authority_json, settings_authority_version, settings_authority_resolved_at ON timesheets FOR EACH ROW EXECUTE FUNCTION private.bpay_expense_classification_ts_v1()'),
    ('timesheets','bpay_source_timesheet_family_gate_v1','private.bpay_source_timesheet_family_gate_v1()','CREATE TRIGGER bpay_source_timesheet_family_gate_v1 BEFORE INSERT OR DELETE OR UPDATE OF booking_id, is_current, version ON timesheets FOR EACH ROW EXECUTE FUNCTION private.bpay_source_timesheet_family_gate_v1()'),
    ('timesheets','bpay_timesheet_work_revision_v1','private.bpay_timesheet_work_revision_v1()','CREATE TRIGGER bpay_timesheet_work_revision_v1 BEFORE INSERT OR UPDATE OF booking_id, bpay_work_revision_v1 ON timesheets FOR EACH ROW EXECUTE FUNCTION private.bpay_timesheet_work_revision_v1()'),
    ('timesheets','timesheet_settings_authority_before_v1','private._timesheet_settings_authority_before_v1()','CREATE TRIGGER timesheet_settings_authority_before_v1 BEFORE INSERT OR UPDATE OF contract_id, week_ending_date, sheet_scope, settings_authority_json, settings_authority_version, settings_authority_fingerprint, settings_authority_resolved_at ON timesheets FOR EACH ROW EXECUTE FUNCTION private._timesheet_settings_authority_before_v1()'),
    ('timesheets','timesheets_candidate_qr_pack_ready_notification_trg','private._candidate_qr_pack_ready_notification_v1()','CREATE TRIGGER timesheets_candidate_qr_pack_ready_notification_trg BEFORE UPDATE OF document_state, current_document_version_id ON timesheets FOR EACH ROW EXECUTE FUNCTION private._candidate_qr_pack_ready_notification_v1()'),
    ('timesheets','trg_timesheet_archive_row_guard_v1','public.timesheet_archive_row_guard_v1()','CREATE TRIGGER trg_timesheet_archive_row_guard_v1 BEFORE INSERT OR DELETE OR UPDATE ON timesheets FOR EACH ROW EXECUTE FUNCTION timesheet_archive_row_guard_v1()'),
    ('timesheets','trg_timesheet_authorisation_overlap_biu','private._timesheet_authorisation_overlap_trg_v1()','CREATE TRIGGER trg_timesheet_authorisation_overlap_biu BEFORE INSERT OR UPDATE OF authorised_at_server ON timesheets FOR EACH ROW EXECUTE FUNCTION private._timesheet_authorisation_overlap_trg_v1()'),
    ('timesheets','trg_timesheets_updated_at','public.set_updated_at()','CREATE TRIGGER trg_timesheets_updated_at BEFORE UPDATE ON timesheets FOR EACH ROW EXECUTE FUNCTION set_updated_at()'),
    ('timesheets','weekly_source_managed_root_authorisation_guard_bu','private.weekly_source_ordinary_authorisation_guard_tg_v1()','CREATE TRIGGER weekly_source_managed_root_authorisation_guard_bu BEFORE UPDATE ON timesheets FOR EACH ROW WHEN ((old.authorised_at_server IS DISTINCT FROM new.authorised_at_server)) EXECUTE FUNCTION private.weekly_source_ordinary_authorisation_guard_tg_v1()'),
    ('timesheets','zz_bpay_timesheet_work_key_v1','private.bpay_timesheet_work_key_v1()','CREATE TRIGGER zz_bpay_timesheet_work_key_v1 BEFORE INSERT OR UPDATE ON timesheets FOR EACH ROW EXECUTE FUNCTION private.bpay_timesheet_work_key_v1()'),
    ('timesheets_financials','trg_set_updated_at_tsfin','public.set_updated_at_tsfin()','CREATE TRIGGER trg_set_updated_at_tsfin BEFORE UPDATE ON timesheets_financials FOR EACH ROW EXECUTE FUNCTION set_updated_at_tsfin()'),
    ('weekly_source_row_timesheet_lineages','bpay_expense_classification_lineage_before_v1','private.bpay_expense_classification_lineage_v1()','CREATE TRIGGER bpay_expense_classification_lineage_before_v1 BEFORE INSERT ON weekly_source_row_timesheet_lineages FOR EACH ROW EXECUTE FUNCTION private.bpay_expense_classification_lineage_v1()'),
    ('weekly_source_row_timesheet_lineages','bpay_lineage_work_binding_v1','private.bpay_lineage_work_binding_v1()','CREATE TRIGGER bpay_lineage_work_binding_v1 BEFORE INSERT ON weekly_source_row_timesheet_lineages FOR EACH ROW EXECUTE FUNCTION private.bpay_lineage_work_binding_v1()'),
    ('weekly_source_row_timesheet_lineages','bpay_source_lineage_family_gate_v1','private.bpay_source_lineage_family_gate_v1()','CREATE TRIGGER bpay_source_lineage_family_gate_v1 BEFORE INSERT ON weekly_source_row_timesheet_lineages FOR EACH ROW EXECUTE FUNCTION private.bpay_source_lineage_family_gate_v1()'),
    ('weekly_source_row_timesheet_lineages','weekly_source_immutable_record_guard','private._weekly_source_immutable_record_guard_v1()','CREATE TRIGGER weekly_source_immutable_record_guard BEFORE DELETE OR UPDATE ON weekly_source_row_timesheet_lineages FOR EACH ROW EXECUTE FUNCTION private._weekly_source_immutable_record_guard_v1()')
  ) e(table_name,trigger_name,signature,definition) loop
    if not exists(select 1 from pg_catalog.pg_trigger t where t.tgrelid=to_regclass('public.'||r.table_name)
      and t.tgname=r.trigger_name and t.tgfoid=to_regprocedure(r.signature)
      and t.tgenabled=case when r.table_name='timesheets' and r.trigger_name='zz_bpay_timesheet_work_key_v1' then 'A'::"char" else 'O'::"char" end
      and pg_catalog.pg_get_triggerdef(t.oid,false)=replace(r.definition,' ON '||quote_ident(r.table_name)||' ',' ON public.'||quote_ident(r.table_name)||' ')) then
      raise exception 'CORRECTION_A0_NATIVE_BEFORE_OWNER_CHANGED' using errcode='55000';end if;
  end loop;
  if exists(select 1 from pg_catalog.pg_trigger t join pg_catalog.pg_class c on c.oid=t.tgrelid
    join pg_catalog.pg_namespace n on n.oid=c.relnamespace where n.nspname='public' and c.relname in('timesheets','candidates','invoice_lines','timesheet_validations','timesheets_financials','candidate_submission_workflows','candidate_approval_requests','candidate_submission_components','candidate_expense_components','timesheet_evidence','nhsp_shifts','weekly_source_row_timesheet_lineages','contract_weeks','weekly_source_cycles')
      and not t.tgisinternal and(t.tgtype&1)=1 and(t.tgtype&2)=2 and not exists(select 1 from(values
    ('candidate_approval_requests','candidate_manager_request_mail_guard_v1'),
    ('candidate_expense_components','candidate_expense_source_root_guard_v1'),
    ('candidate_submission_components','candidate_submission_components_immutability_guard'),
    ('candidate_submission_workflows','candidate_workflow_creation_identity_guard_v1'),
    ('candidate_submission_workflows','trg_timesheet_submission_workflow_overlap_biu'),
    ('candidates','set_tms_ref'),
    ('candidates','trg_candidates_set_bank_hash'),
    ('candidates','trg_candidates_set_rev'),
    ('candidates','trg_candidates_set_updated_at'),
    ('contract_weeks','bpay_expense_classification_week_before_v1'),
    ('contract_weeks','contract_week_settings_authority_before_v1'),
    ('contract_weeks','trg_contract_weeks_set_updated_at'),
    ('invoice_lines','trg_invoice_line_archived_timesheet_guard_v1'),
    ('invoice_lines','weekly_source_invoice_line_owner_guard'),
    ('timesheet_evidence','timesheet_evidence_candidate_lineage_guard'),
    ('timesheet_evidence','trg_timesheet_archived_evidence_guard_v1'),
    ('timesheets','bpay_expense_classification_ts_before_v1'),
    ('timesheets','bpay_source_timesheet_family_gate_v1'),
    ('timesheets','bpay_timesheet_work_revision_v1'),
    ('timesheets','timesheet_settings_authority_before_v1'),
    ('timesheets','timesheets_candidate_qr_pack_ready_notification_trg'),
    ('timesheets','trg_timesheet_archive_row_guard_v1'),
    ('timesheets','trg_timesheet_authorisation_overlap_biu'),
    ('timesheets','trg_timesheets_updated_at'),
    ('timesheets','weekly_source_managed_root_authorisation_guard_bu'),
    ('timesheets','zz_bpay_timesheet_work_key_v1'),
    ('timesheets_financials','trg_set_updated_at_tsfin'),
    ('weekly_source_row_timesheet_lineages','bpay_expense_classification_lineage_before_v1'),
    ('weekly_source_row_timesheet_lineages','bpay_lineage_work_binding_v1'),
    ('weekly_source_row_timesheet_lineages','bpay_source_lineage_family_gate_v1'),
    ('weekly_source_row_timesheet_lineages','weekly_source_immutable_record_guard'),
    ('timesheet_validations','zzzz_healthroster_a0_reference_before_v1'),
    ('invoice_lines','aaa_healthroster_a0_reference_before_v1'),
    ('timesheets_financials','aaa_healthroster_a0_financial_before_v1'),
    ('timesheets','aaa_healthroster_a0_booking_before_v1'),
    ('candidates','zzzz_healthroster_a0_candidate_before_v1'),
    ('candidate_submission_workflows','aaa_healthroster_a0_child_before_v1'),
    ('candidate_approval_requests','aaa_healthroster_a0_child_before_v1'),
    ('candidate_submission_components','aaa_healthroster_a0_child_before_v1'),
    ('candidate_expense_components','aaa_healthroster_a0_child_before_v1'),
    ('timesheet_evidence','aaa_healthroster_a0_child_before_v1'),
    ('nhsp_shifts','aaa_healthroster_a0_child_before_v1'),
    ('weekly_source_row_timesheet_lineages','aaa_healthroster_a0_child_before_v1'),
    ('contract_weeks','aaa_healthroster_a0_child_before_v1'),
    ('weekly_source_cycles','aaa_healthroster_a0_child_before_v1')
      ) e(table_name,trigger_name) where e.table_name=c.relname and e.trigger_name=t.tgname)) then
    raise exception 'CORRECTION_A0_UNKNOWN_NATIVE_BEFORE_OWNER' using errcode='55000';end if;
  -- No positive SEND/atomic or isolation change: preserve all nine RR roots and snapshot assertions.
end;
$a0_source_contract$;
do $a0_source_bodies$
declare r record;v_body text;
begin
  for r in select * from(values
    ('private._candidate_component_immutability_guard_v1()','cc5121f35abb2b6a0c77760608e6a8b4e4bda59461a820d2dce24c7683127178'),
    ('private._candidate_evidence_lineage_guard_v1()','65abbaadbe535324ba3bbb1be6cc232e998b10277750b18a74d67e63997de63a'),
    ('private._candidate_expense_source_root_guard_v1()','94aefebb1adc587aed74740d10b3ad2bf158675d320601f5a506655071462c15'),
    ('private._candidate_manager_request_mail_guard_v1()','1ac1928939c942f8fe77761c3e1c9dec7dacae5b59c797c8283943243023282d'),
    ('private._candidate_qr_pack_ready_notification_v1()','803bdc79f42c28d76f0b3fb757658c26592a528803f4ed3bb306b5f4688eb0c8'),
    ('private._candidate_workflow_creation_identity_guard_v1()','17418b0ac5b8679c482cdba3446a591015a5f4583fef6e94ee60582686f2e142'),
    ('private._contract_week_settings_authority_before_v1()','f339020e6f918f93e6f16d104c45389af536fd0b21ce1b1cc2e7ccfa30ceeb07'),
    ('private._timesheet_authorisation_overlap_trg_v1()','93b4dcadaf386ddee7d26a00467c2277e2b4178148f14c9a68a6e61ad37f65bd'),
    ('private._timesheet_settings_authority_before_v1()','2867d1400328ed5056c804173355249a0063888620f581255f8dac0afede2a76'),
    ('private._timesheet_submission_workflow_overlap_trg_v1()','7b0ce5d902c6cc19ace31bd633b680c8c10fe0912ebafede4ef0c46667563609'),
    ('private._weekly_source_immutable_record_guard_v1()','34cfb47b73e45a5da42fe8c2484a5f46d2cd7bae6440eb5561a9d08b3e119d19'),
    ('private.bpay_expense_classification_lineage_v1()','ff63712555516cf52649c78bf6dd6125b66de603b75feffcc3b16508918b9993'),
    ('private.bpay_expense_classification_ts_v1()','677d92c62a6882a28ee55c72dab11d67986cc3758dee8100399e36ed6dbe9823'),
    ('private.bpay_expense_classification_week_v1()','fc8312e105e3f6a03db88d63e102da13244231d4c4fc7902c1c8571cb41d7a51'),
    ('private.bpay_lineage_work_binding_v1()','b902950c07e36f00dbb8b4b6b7de291d483af3f5ad08a7965d54489aa09a6132'),
    ('private.bpay_source_lineage_family_gate_v1()','a0c4bff622ee55c38657fd6faa5bce238c750a4193899c8d720c6b184a186864'),
    ('private.bpay_source_timesheet_family_gate_v1()','fcd331f7bd34c608d12fd4788e8701096d7bb377d4f318ba377d270955881a2d'),
    ('private.bpay_timesheet_work_key_v1()','55afc5b66acf4a9643dd3bcf167352ca04c9ecd82bc4accaefd20a16d208f807'),
    ('private.bpay_timesheet_work_revision_v1()','aa7fc506cd7aafb9543e18cfad07582c9cea23fac0805129ac1bcb7c17bebee1'),
    ('private.timesheet_correction_a0_booking_before_v1()','0fe1736bd3a3a11e49c75dc2a10ed20fc0d11464b67ac3809dd2526bd58e67f2'),
    ('private.timesheet_correction_a0_candidate_before_v1()','3a9445e369763184b205cdd291480fa35b884829e92546dc4d8e03db892a4ecf'),
    ('private.timesheet_correction_a0_child_reference_before_v1()','6ce412481ccf50abcb5c38d1126dcfaff1ea6ff8ebdd5baf87eeb95152eaa38e'),
    ('private.timesheet_correction_a0_family_tokens_retain_v1(uuid,uuid[])','2fe2d485c7ba278a02e0fc99df03c4d174e718ccaf8d73748d4963ba8f44abad'),
    ('private.timesheet_correction_a0_fence_acquire_v1(jsonb,boolean)','37eeb5ac3857fb815126e71b725eb264d20ec958ae7a4473248d1425c18abb1b'),
    ('private.timesheet_correction_a0_fence_tokens_v1(uuid[],uuid[],text[],text[],jsonb[])','2276441e9f9102d6600eb0dd602a16c752a88b85638dee1be460b1e000eb2e88'),
    ('private.timesheet_correction_a0_fence_tokens_v2(uuid[],uuid[],text[],text[],jsonb[],uuid[],uuid[],uuid[])','6b984c49bb9b9a16b919b3c216536ec792c02ddd907c59ea49171f9bcadefdd2'),
    ('private.timesheet_correction_a0_native_lock_fence_v1(uuid,uuid)','e30bdcde0b26b16c6ae204d5788970b80be81c2d237373d8fc8a2906bdd04ccc'),
    ('private.timesheet_correction_a0_reference_before_v1()','a9171a266474afa69997ad36228a02ba8acd4b7b0ed8873b86ec72ed41aeeb9c'),
    ('private.timesheet_correction_a0_request_tokens_retain_v1(private.timesheet_correction_requests)','fdca019504cd29e0e36b45e90d041db7509d0129b8fb5c7c7b11ada0d3ffe1ff'),
    ('private.timesheet_correction_a0_send_family_fence_v1(uuid)','d9378451a3990b86d6262fd521f324a486a2ef003a1fcda3fb1cb20f6106c726'),
    ('private.weekly_source_invoice_line_owner_guard_v1()','bd502dd144ff22c0942fceb82dcf8788dd18383808789641812704178e46077c'),
    ('private.weekly_source_ordinary_authorisation_guard_tg_v1()','c36aac0612610316ed238fed62b5052bd902db2b86a547d05964de73658750f0'),
    ('public._trg_candidates_set_bank_hash()','48687f54964a0c0fb8f6dc0dfdb1985ff55f654903123755b8bdb6df9306a8c3'),
    ('public.invoice_line_archived_timesheet_guard_v1()','8819c3ea60ae3c9332cf8596230b42f7e11091c65ceff75129090eac155e0982'),
    ('public.set_updated_at_tsfin()','8095d4d16b4bea82cd80cc435829bc380cb8d6658bc66e64aefc75f187650be0'),
    ('public.set_updated_at()','3c6d6c41d6262a20e7c102dbd49bb3383bd86a4138c8a3ab6b9b04a1ec2420a5'),
    ('public.timesheet_archive_row_guard_v1()','2ed947ef7bdb0e23eaa8a068fa8304ce91db07db6adbd2ee6f87f0b7f31781b2'),
    ('public.timesheet_archived_evidence_guard_v1()','014e41644de1a277705f3e3d90543bb611306d548327d80ce2b7970aec292ec8'),
    ('public.timesheet_correction_send_v1(jsonb,uuid,timestamp with time zone)','511c13488a210181b978180d9fefca90f5493b92d91cba2b00a10009e70649d8'),
    ('public.trg_set_rev()','be01dc3eeeadb2f096f463f79c9a72bbf10eeef7b77e2b5d6158972da96237d0'),
    ('public.trg_set_tms_ref()','ddb16167a415d5fc4a5c1bf34340b80ad1b990e3938940612fe4de837f9d4741')
  ) expected(signature,sha256) loop
    select p.prosrc into v_body from pg_catalog.pg_proc p where p.oid=to_regprocedure(r.signature);
    if not found or pg_catalog.encode(extensions.digest(pg_catalog.convert_to(
      pg_catalog.replace(v_body,E'\r\n',E'\n'),'UTF8'),'sha256'),'hex') is distinct from r.sha256 then
      raise exception 'CORRECTION_A0_REVIEWED_BODY_CHANGED' using errcode='55000';end if;
  end loop;
end;
$a0_source_bodies$;
select true as canonical_physical_module_and_complete_verifiers_passed;
