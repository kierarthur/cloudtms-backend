-- Repeatable CloudTMS authority: weekly_source_settings_expenses_rates_v1
-- Release-controlled source profiles and the single effective policy resolver.
-- A configurable Roster source always normalises to HEALTHROSTER_WEEKLY at the
-- protected-pay/C1 boundary; no client-specific Banking Pay mode exists.

\set ON_ERROR_STOP on

begin;

do $seed_weekly_source_profiles$
declare
  v_row record;
  v_expected_hash bytea;
  v_saved record;
begin
  for v_row in
    select *
    from (values
      (
        '31111111-1111-4111-8111-111111111111'::uuid,
        'NHSP_PREFINAL_RELEASED_V1',1,
        'GENERIC_COMPLETE_SNAPSHOT','XLSX','CANCEL_INSIDE_CONFIRMED_COVERAGE',
        'CHECKING_ONLY','ACTUAL_START_END_BREAK',false,null::text,false,false,false,
        '{"candidate_and_manager_wording":"MISSING_OR_NOT_YET_AUTHORISED","contract_hours_ignored":true,"purpose":"PREFINAL_CHECKING","source_family":"NHSP"}'::jsonb
      ),
      (
        '32222222-2222-4222-8222-222222222222'::uuid,
        'NHSP_FINAL_BACKING_V1',1,
        'NHSP_TRUST_BACKING_REPORT','XLSX','NO_INFERENCE',
        'PHYSICAL_SIGNED_MOVEMENTS','ACTUAL_START_END_BREAK',false,'NHSP_FULL_REVERSAL',true,true,true,
        '{"candidate_and_manager_wording":"MISSING_OR_NOT_YET_AUTHORISED","contract_hours_ignored":true,"fmc_must_equal_zero":true,"invoice_amount":"SIGNED_COMMISSION_PLUS_TOTAL_COST_AFTER_PRICE_GATE","one_report_per_client":true,"source_family":"NHSP"}'::jsonb
      ),
      (
        '33333333-3333-4333-8333-333333333333'::uuid,
        'HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1',1,
        'HEALTHROSTER_ACTUAL_ROWS','XLSX','CANCEL_INSIDE_CONFIRMED_COVERAGE',
        'FINALISED_ROW_ONLY','ACTUAL_START_END_BREAK',false,null::text,false,true,false,
        '{"actual_columns":["Start","End","Actual Break","Hours"],"finalisation_columns":["Finalised Date","Timesheet Finalised By"],"planned_columns_ignored_for_worked_time":["From","To","Break"],"source_family":"ROSTER"}'::jsonb
      ),
      (
        '34444444-4444-4444-8444-444444444444'::uuid,
        'HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1',1,
        'HEALTHROSTER_ACTUAL_ROWS','XLSX','CANCEL_INSIDE_CONFIRMED_COVERAGE',
        'FINALISED_ROW_ONLY','ACTUAL_START_END_BREAK',false,null::text,false,true,false,
        '{"actual_columns":["Actual Start","Actual End","Actual Break","Actual Hours"],"finalisation_columns":["Timesheet Finalised By"],"planned_columns_ignored_for_worked_time":["Start","End"],"source_family":"ROSTER"}'::jsonb
      ),
      (
        '35555555-5555-4555-8555-555555555555'::uuid,
        'ROSTER_WEEKLY_SUMMARY_ACTUAL_V1',1,
        'GENERIC_COMPLETE_SNAPSHOT','CSV','CANCEL_INSIDE_CONFIRMED_COVERAGE',
        'COMPLETE_FILE_ROWS','BOOKING_START_END_AND_TOTAL_HOURS',false,null::text,false,false,false,
        '{"client_name_independent":true,"expense_column":"Expenses","expense_meaning":"CONFIGURABLE_SOURCE_FIXED_EX_VAT","rate_classification":"CONFIGURABLE_WHOLE_SHIFT","source_family":"ROSTER","worked_columns":["Booking Start","Booking End","Total Hours"]}'::jsonb
      )
    ) as seeded(
      id,profile_code,version,final_authority_kind,container_kind,omission_meaning,
      row_finalisation_capability,worked_duration_authority,scheduled_hours_fallback,
      physical_negative_meaning,report_number_required,single_client_required,
      fmc_must_equal_zero,profile_json
    )
  loop
    v_expected_hash:=extensions.digest(
      pg_catalog.convert_to(v_row.profile_json::text,'UTF8'),'sha256'
    );

    insert into public.weekly_source_format_profiles(
      id,profile_code,version,final_authority_kind,container_kind,omission_meaning,
      row_finalisation_capability,worked_duration_authority,scheduled_hours_fallback,
      physical_negative_meaning,report_number_required,single_client_required,
      fmc_must_equal_zero,profile_json,profile_sha256,active
    ) values (
      v_row.id,v_row.profile_code,v_row.version,v_row.final_authority_kind,
      v_row.container_kind,v_row.omission_meaning,v_row.row_finalisation_capability,
      v_row.worked_duration_authority,v_row.scheduled_hours_fallback,
      v_row.physical_negative_meaning,v_row.report_number_required,
      v_row.single_client_required,v_row.fmc_must_equal_zero,v_row.profile_json,
      v_expected_hash,true
    ) on conflict (profile_code,version) do nothing;

    select p.* into strict v_saved
    from public.weekly_source_format_profiles p
    where p.profile_code=v_row.profile_code and p.version=v_row.version;

    if v_saved.id is distinct from v_row.id
       or v_saved.final_authority_kind is distinct from v_row.final_authority_kind
       or v_saved.container_kind is distinct from v_row.container_kind
       or v_saved.omission_meaning is distinct from v_row.omission_meaning
       or v_saved.row_finalisation_capability is distinct from v_row.row_finalisation_capability
       or v_saved.worked_duration_authority is distinct from v_row.worked_duration_authority
       or v_saved.scheduled_hours_fallback is distinct from v_row.scheduled_hours_fallback
       or v_saved.physical_negative_meaning is distinct from v_row.physical_negative_meaning
       or v_saved.report_number_required is distinct from v_row.report_number_required
       or v_saved.single_client_required is distinct from v_row.single_client_required
       or v_saved.fmc_must_equal_zero is distinct from v_row.fmc_must_equal_zero
       or v_saved.profile_json is distinct from v_row.profile_json
       or v_saved.profile_sha256 is distinct from v_expected_hash
       or v_saved.active is not true then
      raise exception 'WEEKLY_SOURCE_PROFILE_VERSION_CONFLICT: % v%',
        v_row.profile_code,v_row.version using errcode='55000';
    end if;
  end loop;
end;
$seed_weekly_source_profiles$;

create or replace function private._weekly_source_effective_policy_v1(
  p_client_id uuid,
  p_contract_id uuid,
  p_work_date date
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_group record;
  v_client_policy public.weekly_source_client_policies%rowtype;
  v_contract_policy public.weekly_source_contract_policies%rowtype;
  v_group_count integer;
  v_client_policy_count integer;
  v_contract_policy_count integer;
  v_contract_client_id uuid;
  v_policy jsonb;
  v_source_expenses boolean;
  v_source_expense_vat boolean;
  v_candidate_queries boolean;
  v_manager_queries boolean;
  v_completed_copy boolean;
  v_manager_recipient text;
  v_completed_recipient text;
begin
  if p_client_id is null or p_work_date is null then
    raise exception 'WEEKLY_SOURCE_POLICY_SCOPE_REQUIRED' using errcode='22023';
  end if;

  select count(*) into v_group_count
  from public.weekly_source_group_clients gc
  join public.weekly_source_groups g on g.id=gc.source_group_id
  where gc.client_id=p_client_id
    and g.active
    and p_work_date between gc.valid_from and coalesce(gc.valid_to,'infinity'::date);
  if v_group_count<>1 then
    raise exception 'WEEKLY_SOURCE_GROUP_CARDINALITY_INVALID'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'reason_code','WEEKLY_SOURCE_GROUP_CARDINALITY_INVALID',
              'client_id',p_client_id,
              'work_date',p_work_date,
              'match_count',v_group_count
            )::text;
  end if;

  select g.*,gc.id as membership_id into strict v_group
  from public.weekly_source_group_clients gc
  join public.weekly_source_groups g on g.id=gc.source_group_id
  where gc.client_id=p_client_id
    and g.active
    and p_work_date between gc.valid_from and coalesce(gc.valid_to,'infinity'::date);

  select count(*) into v_client_policy_count
  from public.weekly_source_client_policies cp
  where cp.source_group_id=v_group.id
    and cp.client_id=p_client_id
    and p_work_date between cp.effective_from and coalesce(cp.effective_to,'infinity'::date);
  if v_client_policy_count<>1 then
    raise exception 'WEEKLY_SOURCE_CLIENT_POLICY_CARDINALITY_INVALID'
      using errcode='55000',
            detail=pg_catalog.jsonb_build_object(
              'reason_code','WEEKLY_SOURCE_CLIENT_POLICY_CARDINALITY_INVALID',
              'client_id',p_client_id,
              'source_group_id',v_group.id,
              'work_date',p_work_date,
              'match_count',v_client_policy_count
            )::text;
  end if;

  select cp.* into strict v_client_policy
  from public.weekly_source_client_policies cp
  where cp.source_group_id=v_group.id
    and cp.client_id=p_client_id
    and p_work_date between cp.effective_from and coalesce(cp.effective_to,'infinity'::date);

  if p_contract_id is not null then
    select c.client_id into strict v_contract_client_id
    from public.contracts c where c.id=p_contract_id;
    if v_contract_client_id is distinct from p_client_id then
      raise exception 'WEEKLY_SOURCE_CONTRACT_CLIENT_MISMATCH' using errcode='22023';
    end if;
    select count(*) into v_contract_policy_count
    from public.weekly_source_contract_policies ctp
    where ctp.contract_id=p_contract_id
      and p_work_date between ctp.effective_from and coalesce(ctp.effective_to,'infinity'::date);
    if v_contract_policy_count>1 then
      raise exception 'WEEKLY_SOURCE_CONTRACT_POLICY_CARDINALITY_INVALID' using errcode='55000';
    end if;
    if v_contract_policy_count=1 then
      select ctp.* into strict v_contract_policy
      from public.weekly_source_contract_policies ctp
      where ctp.contract_id=p_contract_id
        and p_work_date between ctp.effective_from and coalesce(ctp.effective_to,'infinity'::date);
    end if;
  end if;

  v_source_expenses:=coalesce(
    v_contract_policy.source_fixed_expenses_enabled_override,
    v_client_policy.source_fixed_expenses_enabled
  );
  v_source_expense_vat:=v_source_expenses and coalesce(
    v_contract_policy.source_expense_vat_enabled_override,
    v_client_policy.source_expense_vat_enabled
  );
  v_candidate_queries:=coalesce(
    v_contract_policy.candidate_queries_enabled_override,
    v_client_policy.candidate_queries_enabled
  );
  v_manager_queries:=coalesce(
    v_contract_policy.manager_queries_enabled_override,
    v_client_policy.manager_queries_enabled
  );
  v_completed_copy:=coalesce(
    v_contract_policy.completed_pack_copy_enabled_override,
    v_client_policy.completed_pack_copy_enabled
  );
  v_manager_recipient:=case when v_manager_queries then nullif(pg_catalog.btrim(coalesce(
    v_contract_policy.manager_query_recipient_override,
    v_client_policy.manager_query_recipient
  )), '') end;
  v_completed_recipient:=case when v_completed_copy then nullif(pg_catalog.btrim(coalesce(
    v_contract_policy.completed_pack_recipient_override,
    v_client_policy.completed_pack_recipient
  )), '') end;

  if v_completed_copy and v_completed_recipient is null then
    raise exception 'WEEKLY_SOURCE_COMPLETED_PACK_RECIPIENT_REQUIRED' using errcode='55000';
  end if;

  v_policy:=pg_catalog.jsonb_build_object(
    'policy_version','WEEKLY_SOURCE_EFFECTIVE_POLICY_V1',
    'source_group_id',v_group.id,
    'source_group_membership_id',v_group.membership_id,
    'source_group_code',v_group.code,
    'source_family',v_group.source_family,
    'timezone',v_group.timezone,
    'cutoff_weekday',v_group.cutoff_weekday,
    'cutoff_local_time',v_group.cutoff_local_time,
    'client_id',p_client_id,
    'contract_id',p_contract_id,
    'work_date',p_work_date,
    'authority_mode',v_client_policy.authority_mode,
    'document_mode',v_client_policy.document_mode,
    'self_bill_enabled',v_client_policy.self_bill_enabled,
    'self_bill_correction_presentation',v_client_policy.self_bill_correction_presentation,
    'source_fixed_expenses_enabled',v_source_expenses,
    'source_expense_vat_enabled',v_source_expense_vat,
    'weekly_rate_classification_method',coalesce(
      v_contract_policy.weekly_rate_classification_method_override,
      v_client_policy.weekly_rate_classification_method
    ),
    'duration_break_tie_rule',coalesce(
      v_contract_policy.duration_break_tie_rule_override,
      v_client_policy.duration_break_tie_rule
    ),
    'candidate_queries_enabled',v_candidate_queries,
    'manager_queries_enabled',v_manager_queries,
    'manager_query_recipient',v_manager_recipient,
    'completed_pack_copy_enabled',v_completed_copy,
    'completed_pack_recipient',v_completed_recipient,
    'c1_source_mode',case
      when v_client_policy.authority_mode='SOURCE_AUTHORITY'
       and v_client_policy.self_bill_enabled
       and v_group.source_family='NHSP' then 'NHSP_WEEKLY'
      when v_client_policy.authority_mode='SOURCE_AUTHORITY'
       and v_client_policy.self_bill_enabled
       and v_group.source_family='ROSTER' then 'HEALTHROSTER_WEEKLY'
      else null
    end,
    'client_policy_id',v_client_policy.id,
    'contract_policy_id',v_contract_policy.id
  );

  return v_policy || pg_catalog.jsonb_build_object(
    'policy_sha256',pg_catalog.encode(
      extensions.digest(pg_catalog.convert_to(v_policy::text,'UTF8'),'sha256'),'hex'
    )
  );
exception
  when no_data_found then
    raise exception 'WEEKLY_SOURCE_POLICY_SCOPE_NOT_FOUND' using errcode='22023';
end;
$function$;

alter function private._weekly_source_effective_policy_v1(uuid,uuid,date) owner to postgres;
revoke all on function private._weekly_source_effective_policy_v1(uuid,uuid,date)
  from public,anon,authenticated,service_role;

comment on function private._weekly_source_effective_policy_v1(uuid,uuid,date) is
  'Single effective Plan 6 Client/Contract policy owner. ROSTER source authority maps only to HEALTHROSTER_WEEKLY at the C1 boundary.';

commit;
