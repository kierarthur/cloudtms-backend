-- Repeatable CloudTMS authority: weekly_source_finalisation_v1
-- Seals the exact current Weekly source publication and emits immutable source
-- billing movements.  It deliberately does not read protected-pay/query state
-- and it never creates or mutates Banking Pay, Workbench or Draft records.

\set ON_ERROR_STOP on

begin;

create or replace function private.weekly_source_finalisation_hex32_v1(
  p_value text,
  p_error_code text
) returns bytea
language plpgsql immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_value text:=pg_catalog.lower(pg_catalog.btrim(coalesce(p_value,'')));
begin
  if v_value!~'^[0-9a-f]{64}$' then
    raise exception '%',coalesce(nullif(p_error_code,''),'WEEKLY_SOURCE_HASH_INVALID')
      using errcode='22023';
  end if;
  return pg_catalog.decode(v_value,'hex');
end;
$function$;

create or replace function private.weekly_source_finalisation_vector_v1(
  p_economic_snapshot_id uuid,
  p_vector_kind text,
  p_multiplier smallint default 1
) returns jsonb
language plpgsql stable security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_economic public.weekly_source_row_economic_snapshots%rowtype;
  v_kind text:=pg_catalog.upper(pg_catalog.btrim(coalesce(p_vector_kind,'')));
begin
  if p_economic_snapshot_id is null or v_kind not in ('PAY','CHARGE')
     or p_multiplier not in (-1,1) then
    raise exception 'WEEKLY_SOURCE_VECTOR_INPUT_INVALID' using errcode='22023';
  end if;
  select * into v_economic
  from public.weekly_source_row_economic_snapshots
  where id=p_economic_snapshot_id;
  if not found then
    raise exception 'WEEKLY_SOURCE_ECONOMIC_SNAPSHOT_MISSING' using errcode='55000';
  end if;
  return pg_catalog.jsonb_build_object(
    'schema_version','WEEKLY_SOURCE_RATE_VECTOR_V1',
    'kind',v_kind,
    'source_mode',v_economic.source_mode,
    'rate_method',v_economic.rate_method,
    'row_sign',(v_economic.row_sign*p_multiplier),
    'paid_minutes',v_economic.paid_minutes,
    'break_minutes',v_economic.break_minutes,
    'hours',pg_catalog.jsonb_build_object(
      'day',v_economic.hours_day*p_multiplier,
      'night',v_economic.hours_night*p_multiplier,
      'sat',v_economic.hours_sat*p_multiplier,
      'sun',v_economic.hours_sun*p_multiplier,
      'bh',v_economic.hours_bh*p_multiplier
    ),
    'rates',case when v_kind='PAY' then pg_catalog.jsonb_build_object(
      'day',v_economic.pay_day,'night',v_economic.pay_night,
      'sat',v_economic.pay_sat,'sun',v_economic.pay_sun,'bh',v_economic.pay_bh
    ) else pg_catalog.jsonb_build_object(
      'day',v_economic.charge_day,'night',v_economic.charge_night,
      'sat',v_economic.charge_sat,'sun',v_economic.charge_sun,'bh',v_economic.charge_bh
    ) end,
    'total_pence',case when v_kind='PAY'
      then v_economic.total_pay_pence*p_multiplier
      else v_economic.calculated_charge_pence*p_multiplier end,
    'calculation_fingerprint',pg_catalog.encode(v_economic.calculation_fingerprint,'hex')
  );
end;
$function$;

create or replace function private.weekly_source_finalisation_negate_vector_v1(
  p_vector jsonb
) returns jsonb
language plpgsql immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_hours jsonb;
  v_row_sign integer;
  v_total bigint;
begin
  if pg_catalog.jsonb_typeof(p_vector)<>'object'
     or p_vector->>'schema_version'<>'WEEKLY_SOURCE_RATE_VECTOR_V1'
     or p_vector->>'kind' not in ('PAY','CHARGE')
     or pg_catalog.jsonb_typeof(p_vector->'hours')<>'object'
     or pg_catalog.jsonb_typeof(p_vector->'rates')<>'object' then
    raise exception 'WEEKLY_SOURCE_VECTOR_INVALID' using errcode='22023';
  end if;
  begin
    v_row_sign:=(p_vector->>'row_sign')::integer;
    v_total:=(p_vector->>'total_pence')::bigint;
    if v_row_sign not in (-1,1) or exists(
      select 1 from (values ('day'),('night'),('sat'),('sun'),('bh')) bucket(name)
      where coalesce(p_vector#>>array['hours',bucket.name],'')!~'^[+-]?[0-9]+([.][0-9]+)?$'
    ) then
      raise exception 'WEEKLY_SOURCE_VECTOR_INVALID' using errcode='22023';
    end if;
    v_hours:=pg_catalog.jsonb_build_object(
      'day',-((p_vector#>>'{hours,day}')::numeric),
      'night',-((p_vector#>>'{hours,night}')::numeric),
      'sat',-((p_vector#>>'{hours,sat}')::numeric),
      'sun',-((p_vector#>>'{hours,sun}')::numeric),
      'bh',-((p_vector#>>'{hours,bh}')::numeric)
    );
  exception when invalid_text_representation or numeric_value_out_of_range then
    raise exception 'WEEKLY_SOURCE_VECTOR_INVALID' using errcode='22023';
  end;
  return p_vector||pg_catalog.jsonb_build_object(
    'row_sign',-v_row_sign,'hours',v_hours,'total_pence',-v_total
  );
end;
$function$;

create or replace function private.weekly_source_finalisation_economic_assert_v1(
  p_row_resolution_id uuid,
  p_source_cycle_id uuid,
  p_expected_source_mode text
) returns uuid
language plpgsql stable security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_resolution public.weekly_source_row_resolutions%rowtype;
  v_row public.weekly_source_upload_rows%rowtype;
  v_economic public.weekly_source_row_economic_snapshots%rowtype;
  v_expected_pay bigint;
  v_expected_charge bigint;
  v_expected_sign smallint;
begin
  if p_row_resolution_id is null or p_source_cycle_id is null
     or p_expected_source_mode not in ('NHSP_WEEKLY','HEALTHROSTER_WEEKLY') then
    raise exception 'WEEKLY_SOURCE_ECONOMIC_ASSERT_INPUT_INVALID' using errcode='22023';
  end if;
  select * into v_resolution
  from public.weekly_source_row_resolutions where id=p_row_resolution_id;
  if not found or v_resolution.mapping_state<>'RESOLVED' then
    raise exception 'WEEKLY_SOURCE_RESOLUTION_NOT_FINALISABLE' using errcode='55000';
  end if;
  select * into strict v_row
  from public.weekly_source_upload_rows where id=v_resolution.upload_row_id;
  select * into v_economic
  from public.weekly_source_row_economic_snapshots
  where row_resolution_id=v_resolution.id;
  if not found or (select pg_catalog.count(*) from public.weekly_source_row_economic_snapshots
                    where row_resolution_id=v_resolution.id)<>1 then
    raise exception 'WEEKLY_SOURCE_ECONOMIC_SNAPSHOT_CARDINALITY' using errcode='55000';
  end if;

  -- A valid zero-charge NHSP row is a positive worked row whose exact source
  -- invoice value happens to be £0.  Only a negative physical source amount is
  -- a reversal; Candidate entitlement remains independently calculated.
  v_expected_sign:=case when p_expected_source_mode='NHSP_WEEKLY' then
    case when v_row.source_shift_charge_pence<0 then -1 else 1 end
    else 1 end;
  if v_expected_sign not in (-1,1)
     or v_economic.row_sign is distinct from v_expected_sign
     or v_economic.source_mode is distinct from p_expected_source_mode
     or v_economic.upload_row_id is distinct from v_row.id
     or not exists(
       select 1
       from public.weekly_source_uploads upload
       where upload.id=v_row.upload_id
         and upload.source_cycle_id=p_source_cycle_id
     )
     or v_economic.generation is distinct from v_resolution.generation
     or v_economic.work_event_id is distinct from v_resolution.work_event_id
     or v_economic.candidate_id is distinct from v_resolution.candidate_id
     or v_economic.client_id is distinct from v_resolution.client_id
     or v_economic.contract_id is distinct from v_resolution.contract_id
     or v_economic.paid_minutes is distinct from v_row.actual_net_minutes
     or v_economic.break_minutes is distinct from v_row.break_minutes
     or v_resolution.paid_minutes is distinct from v_row.actual_net_minutes
     or v_resolution.contract_and_rate_fingerprint is distinct from v_economic.contract_and_rate_fingerprint
     or v_resolution.effective_policy_fingerprint is distinct from v_economic.effective_policy_fingerprint then
    raise exception 'WEEKLY_SOURCE_ECONOMIC_SNAPSHOT_IDENTITY_MISMATCH' using errcode='55000';
  end if;

  if pg_catalog.jsonb_typeof(v_economic.canonical_result_json) is distinct from 'object'
     or pg_catalog.jsonb_typeof(v_economic.canonical_result_json->'bucket_minutes') is distinct from 'object'
     or pg_catalog.jsonb_typeof(v_economic.canonical_result_json->'hours') is distinct from 'object'
     or pg_catalog.jsonb_typeof(v_economic.canonical_result_json->'pay_rates') is distinct from 'object'
     or pg_catalog.jsonb_typeof(v_economic.canonical_result_json->'charge_rates') is distinct from 'object' then
    raise exception 'WEEKLY_SOURCE_ECONOMIC_SNAPSHOT_INCOMPLETE' using errcode='55000';
  end if;
  if not (v_economic.canonical_result_json ?& array[
       'schema_version','calculator_version','source_mode','rate_method','sign',
       'paid_minutes','break_minutes','bucket_minutes','hours','pay_rates','charge_rates',
       'total_pay_pence','calculated_charge_pence','invoice_vat_chargeable',
       'invoice_vat_rate_pct','source_expense_vat_enabled','contract_and_rate_fingerprint',
       'effective_policy_fingerprint','invoice_vat_policy_fingerprint','break_allocation'
     ]::text[])
     or exists(
       select 1
       from pg_catalog.jsonb_object_keys(v_economic.canonical_result_json) key_name
       where not (key_name=any(array[
         'schema_version','calculator_version','source_mode','rate_method','sign',
         'paid_minutes','break_minutes','bucket_minutes','hours','pay_rates','charge_rates',
         'total_pay_pence','calculated_charge_pence','invoice_vat_chargeable',
         'invoice_vat_rate_pct','source_expense_vat_enabled','contract_and_rate_fingerprint',
         'effective_policy_fingerprint','invoice_vat_policy_fingerprint','break_allocation'
       ]::text[]))
     )
     or exists(
       select 1
       from (values ('bucket_minutes'),('hours'),('pay_rates'),('charge_rates')) object_name(name)
       where not ((v_economic.canonical_result_json->object_name.name) ?&
                    array['day','night','sat','sun','bh']::text[])
          or exists(
            select 1
            from pg_catalog.jsonb_object_keys(
              v_economic.canonical_result_json->object_name.name
            ) bucket_name
            where not (bucket_name=any(array['day','night','sat','sun','bh']::text[]))
          )
     ) then
    raise exception 'WEEKLY_SOURCE_ECONOMIC_SNAPSHOT_SHAPE_MISMATCH' using errcode='55000';
  end if;

  v_expected_pay:=(pg_catalog.round(pg_catalog.round(
    pg_catalog.abs(v_economic.hours_day)*v_economic.pay_day+
    pg_catalog.abs(v_economic.hours_night)*v_economic.pay_night+
    pg_catalog.abs(v_economic.hours_sat)*v_economic.pay_sat+
    pg_catalog.abs(v_economic.hours_sun)*v_economic.pay_sun+
    pg_catalog.abs(v_economic.hours_bh)*v_economic.pay_bh,2)*100,0)::bigint)*v_expected_sign;
  v_expected_charge:=(pg_catalog.round(pg_catalog.round(
    pg_catalog.abs(v_economic.hours_day)*v_economic.charge_day+
    pg_catalog.abs(v_economic.hours_night)*v_economic.charge_night+
    pg_catalog.abs(v_economic.hours_sat)*v_economic.charge_sat+
    pg_catalog.abs(v_economic.hours_sun)*v_economic.charge_sun+
    pg_catalog.abs(v_economic.hours_bh)*v_economic.charge_bh,2)*100,0)::bigint)*v_expected_sign;
  if v_economic.canonical_result_json->>'schema_version'
       is distinct from 'WEEKLY_SOURCE_CANONICAL_ECONOMIC_SNAPSHOT_V1'
     or v_economic.canonical_result_json->>'rate_method' is distinct from v_economic.rate_method
     or (v_economic.canonical_result_json#>>'{bucket_minutes,day}')::integer
          is distinct from v_economic.minutes_day
     or (v_economic.canonical_result_json#>>'{bucket_minutes,night}')::integer
          is distinct from v_economic.minutes_night
     or (v_economic.canonical_result_json#>>'{bucket_minutes,sat}')::integer
          is distinct from v_economic.minutes_sat
     or (v_economic.canonical_result_json#>>'{bucket_minutes,sun}')::integer
          is distinct from v_economic.minutes_sun
     or (v_economic.canonical_result_json#>>'{bucket_minutes,bh}')::integer
          is distinct from v_economic.minutes_bh
     or (v_economic.canonical_result_json#>>'{hours,day}')::numeric
          is distinct from v_economic.hours_day
     or (v_economic.canonical_result_json#>>'{hours,night}')::numeric
          is distinct from v_economic.hours_night
     or (v_economic.canonical_result_json#>>'{hours,sat}')::numeric
          is distinct from v_economic.hours_sat
     or (v_economic.canonical_result_json#>>'{hours,sun}')::numeric
          is distinct from v_economic.hours_sun
     or (v_economic.canonical_result_json#>>'{hours,bh}')::numeric
          is distinct from v_economic.hours_bh
     or (v_economic.canonical_result_json#>>'{pay_rates,day}')::numeric
          is distinct from v_economic.pay_day
     or (v_economic.canonical_result_json#>>'{pay_rates,night}')::numeric
          is distinct from v_economic.pay_night
     or (v_economic.canonical_result_json#>>'{pay_rates,sat}')::numeric
          is distinct from v_economic.pay_sat
     or (v_economic.canonical_result_json#>>'{pay_rates,sun}')::numeric
          is distinct from v_economic.pay_sun
     or (v_economic.canonical_result_json#>>'{pay_rates,bh}')::numeric
          is distinct from v_economic.pay_bh
     or (v_economic.canonical_result_json#>>'{charge_rates,day}')::numeric
          is distinct from v_economic.charge_day
     or (v_economic.canonical_result_json#>>'{charge_rates,night}')::numeric
          is distinct from v_economic.charge_night
     or (v_economic.canonical_result_json#>>'{charge_rates,sat}')::numeric
          is distinct from v_economic.charge_sat
     or (v_economic.canonical_result_json#>>'{charge_rates,sun}')::numeric
          is distinct from v_economic.charge_sun
     or (v_economic.canonical_result_json#>>'{charge_rates,bh}')::numeric
          is distinct from v_economic.charge_bh
     or v_economic.total_pay_pence is distinct from v_expected_pay
     or v_economic.calculated_charge_pence is distinct from v_expected_charge
     or v_economic.calculation_fingerprint is distinct from
       private.weekly_source_sha256_jsonb_v1(
         'WEEKLY_SOURCE_CANONICAL_ECONOMIC_SNAPSHOT_V1',v_economic.canonical_result_json
       )
     or v_economic.canonical_result_json->>'calculator_version' is distinct from v_economic.calculator_version
     or v_economic.canonical_result_json->>'source_mode' is distinct from v_economic.source_mode
     or v_economic.canonical_result_json->>'rate_method' is distinct from v_economic.rate_method
     or (v_economic.canonical_result_json->>'sign')::smallint is distinct from v_economic.row_sign
     or (v_economic.canonical_result_json->>'paid_minutes')::integer is distinct from v_economic.paid_minutes
     or (v_economic.canonical_result_json->>'break_minutes')::integer is distinct from v_economic.break_minutes
     or (v_economic.canonical_result_json->>'total_pay_pence')::bigint is distinct from v_economic.total_pay_pence
     or (v_economic.canonical_result_json->>'calculated_charge_pence')::bigint is distinct from v_economic.calculated_charge_pence
     or (v_economic.canonical_result_json->>'invoice_vat_chargeable')::boolean is distinct from v_economic.invoice_vat_chargeable
     or (v_economic.canonical_result_json->>'invoice_vat_rate_pct')::numeric is distinct from v_economic.invoice_vat_rate_pct
     or (v_economic.canonical_result_json->>'source_expense_vat_enabled')::boolean is distinct from v_economic.source_expense_vat_enabled
     or private.weekly_source_finalisation_hex32_v1(
          v_economic.canonical_result_json->>'contract_and_rate_fingerprint',
          'WEEKLY_SOURCE_CONTRACT_RATE_FINGERPRINT_INVALID'
        ) is distinct from v_economic.contract_and_rate_fingerprint
     or private.weekly_source_finalisation_hex32_v1(
          v_economic.canonical_result_json->>'effective_policy_fingerprint',
          'WEEKLY_SOURCE_POLICY_FINGERPRINT_INVALID'
        ) is distinct from v_economic.effective_policy_fingerprint
     or private.weekly_source_finalisation_hex32_v1(
          v_economic.canonical_result_json->>'invoice_vat_policy_fingerprint',
          'WEEKLY_SOURCE_VAT_FINGERPRINT_INVALID'
        ) is distinct from v_economic.invoice_vat_policy_fingerprint then
    raise exception 'WEEKLY_SOURCE_ECONOMIC_SNAPSHOT_INCONSISTENT' using errcode='55000';
  end if;
  return v_economic.id;
exception
  when invalid_text_representation or numeric_value_out_of_range then
    raise exception 'WEEKLY_SOURCE_ECONOMIC_SNAPSHOT_INCONSISTENT' using errcode='55000';
end;
$function$;

create or replace function private.weekly_source_finalisation_expense_policy_assert_v1(
  p_row_resolution_id uuid,
  p_source_cycle_id uuid
) returns uuid
language plpgsql stable security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_resolution public.weekly_source_row_resolutions%rowtype;
  v_row public.weekly_source_upload_rows%rowtype;
  v_snapshot public.weekly_source_row_expense_policy_snapshots%rowtype;
  v_profile public.weekly_source_format_profiles%rowtype;
  v_policy jsonb;
  v_source_group_id uuid;
  v_expected_hash bytea;
begin
  if p_row_resolution_id is null or p_source_cycle_id is null then
    raise exception 'WEEKLY_SOURCE_EXPENSE_POLICY_ASSERT_INPUT_INVALID' using errcode='22023';
  end if;
  select * into v_resolution
  from public.weekly_source_row_resolutions where id=p_row_resolution_id;
  if not found or v_resolution.mapping_state<>'RESOLVED' then
    raise exception 'WEEKLY_SOURCE_EXPENSE_RESOLUTION_NOT_FINALISABLE' using errcode='55000';
  end if;
  select * into strict v_row
  from public.weekly_source_upload_rows where id=v_resolution.upload_row_id;
  select profile.* into strict v_profile
  from public.weekly_source_uploads upload
  join public.weekly_source_format_profiles profile
    on profile.id=upload.source_format_profile_id
  where upload.id=v_row.upload_id and upload.source_cycle_id=p_source_cycle_id;
  select cycle.source_group_id into strict v_source_group_id
  from public.weekly_source_cycles cycle
  where cycle.id=p_source_cycle_id;
  if v_profile.profile_code<>'ROSTER_WEEKLY_SUMMARY_ACTUAL_V1'
     or v_row.row_finalisation_state not in ('NOT_APPLICABLE','SOURCE_WORKED','SOURCE_ABSENT_ZERO') then
    raise exception 'WEEKLY_SOURCE_EXPENSE_POLICY_PROFILE_INVALID' using errcode='55000';
  end if;
  select * into v_snapshot
  from public.weekly_source_row_expense_policy_snapshots
  where row_resolution_id=v_resolution.id;
  if not found or (select pg_catalog.count(*)
                    from public.weekly_source_row_expense_policy_snapshots
                    where row_resolution_id=v_resolution.id)<>1 then
    raise exception 'WEEKLY_SOURCE_EXPENSE_POLICY_SNAPSHOT_CARDINALITY' using errcode='55000';
  end if;
  v_policy:=private._weekly_source_effective_policy_v1(
    v_resolution.client_id,v_resolution.contract_id,v_row.work_date
  );
  if (v_policy->>'authority_mode') is distinct from 'SOURCE_AUTHORITY'
     or coalesce((v_policy->>'self_bill_enabled')::boolean,false) is not true
     or coalesce((v_policy->>'source_fixed_expenses_enabled')::boolean,false) is not true
     or (v_policy->>'source_group_id')::uuid is distinct from v_source_group_id
     or private.weekly_source_finalisation_hex32_v1(
          v_policy->>'policy_sha256','WEEKLY_SOURCE_POLICY_FINGERPRINT_INVALID'
        ) is distinct from v_snapshot.effective_policy_fingerprint
     or coalesce((v_policy->>'source_expense_vat_enabled')::boolean,false)
          is distinct from v_snapshot.source_expense_vat_enabled
     or pg_catalog.upper(pg_catalog.btrim(coalesce(
          v_policy->>'self_bill_correction_presentation',''
        ))) is distinct from v_snapshot.correction_presentation then
    raise exception 'WEEKLY_SOURCE_EXPENSE_POLICY_STALE' using errcode='40001';
  end if;
  v_expected_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_ROW_EXPENSE_POLICY_SNAPSHOT_V1',
    pg_catalog.jsonb_build_object(
      'row_resolution_id',v_resolution.id,'upload_row_id',v_row.id,
      'generation',v_resolution.generation,'work_event_id',v_resolution.work_event_id,
      'candidate_id',v_resolution.candidate_id,'client_id',v_resolution.client_id,
      'contract_id',v_resolution.contract_id,'row_finalisation_state',v_row.row_finalisation_state,
      'source_expense_pence',v_row.source_expense_pence,
      'source_expense_parse_state',v_row.source_expense_parse_state,
      'source_expense_vat_enabled',v_snapshot.source_expense_vat_enabled,
      'invoice_vat_chargeable',v_snapshot.invoice_vat_chargeable,
      'invoice_vat_rate_pct',v_snapshot.invoice_vat_rate_pct,
      'correction_presentation',v_snapshot.correction_presentation,
      'effective_policy_fingerprint',pg_catalog.encode(v_snapshot.effective_policy_fingerprint,'hex'),
      'invoice_vat_policy_fingerprint',pg_catalog.encode(v_snapshot.invoice_vat_policy_fingerprint,'hex')
    )
  );
  if v_snapshot.upload_row_id is distinct from v_row.id
     or v_snapshot.generation is distinct from v_resolution.generation
     or v_snapshot.work_event_id is distinct from v_resolution.work_event_id
     or v_snapshot.candidate_id is distinct from v_resolution.candidate_id
     or v_snapshot.client_id is distinct from v_resolution.client_id
     or v_snapshot.contract_id is distinct from v_resolution.contract_id
     or v_snapshot.source_expense_pence is distinct from v_row.source_expense_pence
     or v_snapshot.source_expense_parse_state is distinct from v_row.source_expense_parse_state
     or v_resolution.effective_policy_fingerprint is distinct from v_snapshot.effective_policy_fingerprint
     or (v_row.row_finalisation_state='SOURCE_ABSENT_ZERO' and exists(
          select 1 from public.weekly_source_row_economic_snapshots economic
          where economic.row_resolution_id=v_resolution.id
        ))
     or (v_snapshot.source_expense_pence>0 and (
          v_row.start_at_local is null or v_row.end_at_local is null
          or v_row.end_at_local<=v_row.start_at_local or v_row.break_minutes is null
        ))
     or v_snapshot.snapshot_hash is distinct from v_expected_hash then
    raise exception 'WEEKLY_SOURCE_EXPENSE_POLICY_SNAPSHOT_INCONSISTENT' using errcode='55000';
  end if;
  return v_snapshot.id;
end;
$function$;

create or replace function private.weekly_source_finalisation_lineage_assert_v1(
  p_row_resolution_id uuid,
  p_source_cycle_id uuid
) returns uuid
language plpgsql stable security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_resolution public.weekly_source_row_resolutions%rowtype;
  v_event public.weekly_work_events%rowtype;
  v_lineage public.weekly_source_row_timesheet_lineages%rowtype;
  v_contract public.contracts%rowtype;
  v_contract_week public.contract_weeks%rowtype;
  v_timesheet public.timesheets%rowtype;
  v_expected bytea;
  v_root_identity jsonb;
begin
  select * into v_resolution
  from public.weekly_source_row_resolutions where id=p_row_resolution_id;
  if not found or v_resolution.mapping_state<>'RESOLVED' then
    raise exception 'WEEKLY_SOURCE_RESOLUTION_NOT_FINALISABLE' using errcode='55000';
  end if;
  select * into strict v_event from public.weekly_work_events where id=v_resolution.work_event_id;
  -- Decision D8: the lineage relation is the per-source-row BINDING, one row
  -- per resolution, with no authorisation generation.  The authorisation record
  -- is per root, in public.weekly_source_root_authorisations.
  select * into v_lineage
  from public.weekly_source_row_timesheet_lineages
  where row_resolution_id=v_resolution.id;
  if not found or (select pg_catalog.count(*) from public.weekly_source_row_timesheet_lineages
                    where row_resolution_id=v_resolution.id)<>1 then
    raise exception 'WEEKLY_SOURCE_TIMESHEET_LINEAGE_CARDINALITY' using errcode='55000';
  end if;
  select * into strict v_contract from public.contracts where id=v_resolution.contract_id;
  select * into strict v_contract_week from public.contract_weeks where id=v_lineage.contract_week_id;
  select * into strict v_timesheet from public.timesheets where timesheet_id=v_lineage.timesheet_id;

  -- G6-8 / proof/34 section 5 step 4 and section 6.  The stored physical id is
  -- resolved through the installed Workbench rotation authority and compared
  -- with the retained family identity and version.  A rotation observed on this
  -- later path is WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE, never a stale rebuild.
  v_root_identity:=private.weekly_source_root_integrity_assert_v1(
    v_lineage.timesheet_id,v_lineage.family_booking_id,v_lineage.timesheet_version
  );
  if p_source_cycle_id is null
     or not exists(
       select 1
       from public.weekly_source_upload_rows source_row
       join public.weekly_source_uploads upload on upload.id=source_row.upload_id
       where source_row.id=v_resolution.upload_row_id
         and upload.source_cycle_id=p_source_cycle_id
     )
     or v_lineage.source_cycle_id is distinct from p_source_cycle_id
     or v_lineage.work_event_id is distinct from v_resolution.work_event_id
     or v_lineage.candidate_id is distinct from v_resolution.candidate_id
     or v_lineage.client_id is distinct from v_resolution.client_id
     or v_lineage.contract_id is distinct from v_resolution.contract_id
     or v_event.candidate_id is distinct from v_resolution.candidate_id
     or v_event.client_id is distinct from v_resolution.client_id
     or v_contract.candidate_id is distinct from v_resolution.candidate_id
     or v_contract.client_id is distinct from v_resolution.client_id
     or v_contract_week.contract_id is distinct from v_resolution.contract_id
     or v_contract_week.week_ending_date is distinct from v_lineage.week_ending_date
     or v_contract_week.additional_seq<>0
     or v_contract_week.is_adjustment
     or v_contract_week.timesheet_id is distinct from v_lineage.timesheet_id
     or v_timesheet.contract_id is distinct from v_resolution.contract_id
     or v_timesheet.week_ending_date is distinct from v_lineage.week_ending_date
     or v_timesheet.sheet_scope<>'WEEKLY'::public.timesheet_scope_enum
     -- Review F11: timesheets.submission_mode is nullable, and `<>` on a NULL
     -- makes the whole or-chain NULL, so the `if` does not fire and the assert
     -- passes.  `is distinct from` is NULL-safe.
     or v_timesheet.submission_mode
        is distinct from 'MANUAL'::public.submission_mode_enum
     or v_timesheet.line_type<>'HOURS'::public.timesheet_line_type_enum
     or v_timesheet.is_adjustment
     or not v_timesheet.is_current
     or v_timesheet.revoked_at is not null
     or v_lineage.family_booking_id is distinct from v_timesheet.booking_id
     or v_lineage.timesheet_version is distinct from v_timesheet.version
     or v_lineage.family_booking_id is distinct from
        (v_root_identity->>'family_booking_id')
     or v_lineage.timesheet_version is distinct from
        (v_root_identity->>'canonical_version')::integer
     or coalesce((v_root_identity->>'requested_is_canonical')::boolean,false)
        is not true then
    raise exception 'WEEKLY_SOURCE_TIMESHEET_LINEAGE_INVALID' using errcode='55000';
  end if;
  v_expected:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_ROW_TIMESHEET_LINEAGE_V1',
    pg_catalog.jsonb_build_object(
      'row_resolution_id',v_lineage.row_resolution_id,
      'source_cycle_id',v_lineage.source_cycle_id,
      'work_event_id',v_lineage.work_event_id,
      'candidate_id',v_lineage.candidate_id,
      'client_id',v_lineage.client_id,
      'contract_id',v_lineage.contract_id,
      'contract_week_id',v_lineage.contract_week_id,
      'timesheet_id',v_lineage.timesheet_id,
      'week_ending_date',v_lineage.week_ending_date
    )
  );
  if v_lineage.lineage_fingerprint is distinct from v_expected then
    raise exception 'WEEKLY_SOURCE_TIMESHEET_LINEAGE_FINGERPRINT_MISMATCH' using errcode='55000';
  end if;
  return v_lineage.timesheet_id;
end;
$function$;

create or replace function private.weekly_source_final_state_fingerprint_v1(
  p_state jsonb
) returns bytea
language sql immutable
set search_path to 'private','pg_catalog','pg_temp'
as $function$
  select private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_FINAL_STATE_V1',coalesce(p_state,'null'::jsonb)
  );
$function$;

create or replace function private.weekly_source_finalisation_insert_current_movement_v1(
  p_transition_id uuid,
  p_nhsp_upload_row_id uuid,
  p_final_revision_id uuid,
  p_finalisation_cycle_id uuid,
  p_source_profile_kind text,
  p_movement_role text,
  p_source_line_kind text,
  p_row_resolution_id uuid,
  p_economic_snapshot_id uuid,
  p_invoice_timesheet_id uuid,
  p_source_validation_charge_pence bigint,
  p_invoice_presentation_charge_pence bigint,
  p_price_check_result text,
  p_price_check_fingerprint bytea,
  p_charge_acceptance_id uuid,
  p_source_facts_json jsonb,
  p_correction_unit_id uuid default null,
  p_prior_movement_id uuid default null
) returns uuid
language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_resolution public.weekly_source_row_resolutions%rowtype;
  v_source_row public.weekly_source_upload_rows%rowtype;
  v_economic public.weekly_source_row_economic_snapshots%rowtype;
  v_upload public.weekly_source_uploads%rowtype;
  v_pay_vector jsonb;
  v_charge_vector jsonb;
  v_mapping_fingerprint bytea;
  v_vat_rate numeric(5,2);
  v_vat numeric(12,2);
  v_total numeric(12,2);
  v_hash bytea;
  v_id uuid;
begin
  if (p_transition_id is null)::integer+(p_nhsp_upload_row_id is null)::integer<>1
     or p_final_revision_id is null or p_finalisation_cycle_id is null
     or p_source_profile_kind not in (
       'GENERIC_COMPLETE_SNAPSHOT','NHSP_TRUST_BACKING_REPORT','HEALTHROSTER_ACTUAL_ROWS'
     )
     or p_movement_role not in ('POSITIVE','REPLACEMENT','REVERSAL')
     or p_source_line_kind not in (
       'SOURCE_ORDINARY','SOURCE_REPLACEMENT','NHSP_PHYSICAL_POSITIVE','NHSP_PHYSICAL_FULL_NEGATIVE'
     )
     or pg_catalog.jsonb_typeof(p_source_facts_json)<>'object' then
    raise exception 'WEEKLY_SOURCE_MOVEMENT_INPUT_INVALID' using errcode='22023';
  end if;
  select * into strict v_resolution
  from public.weekly_source_row_resolutions where id=p_row_resolution_id;
  select * into strict v_source_row
  from public.weekly_source_upload_rows where id=v_resolution.upload_row_id;
  select * into strict v_upload
  from public.weekly_source_uploads where id=v_source_row.upload_id;
  select * into strict v_economic
  from public.weekly_source_row_economic_snapshots where id=p_economic_snapshot_id;
  if v_economic.row_resolution_id is distinct from v_resolution.id
     or v_resolution.mapping_state<>'RESOLVED'
     or p_invoice_timesheet_id is null
     or not exists(
       select 1
       from public.weekly_source_final_revisions revision
       where revision.id=p_final_revision_id
         and revision.source_cycle_id=p_finalisation_cycle_id
         and (
           revision.state='CURRENT'
           or (
             revision.state='PREPARED'
             and revision.reason='CORRECT_FINAL_SOURCE'
             and exists(
               select 1
               from public.weekly_final_source_correction_sessions correction
               join public.weekly_source_uploads replacement_upload
                 on replacement_upload.id=correction.replacement_correction_upload_id
                and replacement_upload.id=revision.upload_id
                and replacement_upload.source_cycle_id=revision.source_cycle_id
                and replacement_upload.state='CORRECTION_READY'
               join public.weekly_source_projection_publications replacement_publication
                 on replacement_publication.id=correction.replacement_projection_publication_id
                and replacement_publication.upload_id=replacement_upload.id
                and replacement_publication.source_cycle_id=revision.source_cycle_id
                and replacement_publication.state='CORRECTION_READY'
               where correction.source_cycle_id=revision.source_cycle_id
                 and correction.authority_scope_kind=revision.authority_scope_kind
                 and correction.report_scope_id is not distinct from revision.report_scope_id
                 and correction.state='PREPARING'
                 and correction.expected_current_final_revision_id=revision.predecessor_revision_id
             )
           )
         )
     )
     or private.weekly_source_finalisation_economic_assert_v1(
          p_row_resolution_id,p_finalisation_cycle_id,
          case when p_source_profile_kind='NHSP_TRUST_BACKING_REPORT'
            then 'NHSP_WEEKLY' else 'HEALTHROSTER_WEEKLY' end
        ) is distinct from p_economic_snapshot_id
     or private.weekly_source_finalisation_lineage_assert_v1(
          p_row_resolution_id,p_finalisation_cycle_id
        ) is distinct from p_invoice_timesheet_id
     or (p_source_profile_kind='NHSP_TRUST_BACKING_REPORT') is distinct from
        (p_nhsp_upload_row_id is not null)
     or (p_source_profile_kind='NHSP_TRUST_BACKING_REPORT' and (
       p_nhsp_upload_row_id is distinct from v_source_row.id
       or p_source_validation_charge_pence is null
       or p_source_validation_charge_pence is distinct from v_source_row.source_shift_charge_pence
       or p_source_validation_charge_pence is distinct from p_invoice_presentation_charge_pence
       or p_price_check_result not in ('EXACT','SOURCE_ROUNDING_EQUIVALENT','ACCEPTED_DISPARITY','ACCEPTED_ZERO')
       or p_price_check_fingerprint is distinct from v_economic.calculation_fingerprint
       or (p_price_check_result in ('EXACT','SOURCE_ROUNDING_EQUIVALENT')) is distinct from
          (p_charge_acceptance_id is null)
       or (p_price_check_result in ('ACCEPTED_DISPARITY','ACCEPTED_ZERO') and not exists(
         select 1
         from public.weekly_source_charge_acceptances acceptance
         join public.weekly_source_charge_checks charge_check
           on charge_check.id=acceptance.charge_check_id
         where acceptance.id=p_charge_acceptance_id
           and acceptance.upload_row_id=v_source_row.id
           and acceptance.row_resolution_id=v_resolution.id
           and acceptance.source_upload_id=v_upload.id
           and acceptance.contract_id=v_resolution.contract_id
           and acceptance.acceptance_kind=p_price_check_result
           and acceptance.source_upload_hash=v_upload.content_sha256
           and acceptance.source_row_fingerprint=v_resolution.source_row_fingerprint
           and acceptance.contract_and_rate_fingerprint=v_economic.contract_and_rate_fingerprint
           and acceptance.effective_policy_fingerprint=v_economic.effective_policy_fingerprint
           and acceptance.charge_calculation_fingerprint=v_economic.calculation_fingerprint
           and acceptance.acceptance_policy_fingerprint=
             private.weekly_source_charge_acceptance_policy_fingerprint_v1()
           and charge_check.upload_row_id=v_source_row.id
           and charge_check.row_resolution_id=v_resolution.id
           and charge_check.charge_calculation_fingerprint=v_economic.calculation_fingerprint
           and charge_check.comparison_result=case p_price_check_result
             when 'ACCEPTED_ZERO' then 'ZERO_SOURCE_CHARGE' else 'MISMATCH' end
       ))
       or p_correction_unit_id is not null
       or p_prior_movement_id is not null
       or not (
         (v_economic.row_sign=1 and p_movement_role='POSITIVE'
           and p_source_line_kind='NHSP_PHYSICAL_POSITIVE'
           and v_source_row.source_shift_charge_pence>=0)
         or (v_economic.row_sign=-1 and p_movement_role='REVERSAL'
           and p_source_line_kind='NHSP_PHYSICAL_FULL_NEGATIVE'
           and v_source_row.source_shift_charge_pence<0)
       )
     ))
     or (p_source_profile_kind<>'NHSP_TRUST_BACKING_REPORT' and (
       p_source_validation_charge_pence is not null
       or p_invoice_presentation_charge_pence is distinct from v_economic.calculated_charge_pence
       or p_price_check_result<>'NOT_APPLICABLE'
       or p_price_check_fingerprint is not null
       or p_charge_acceptance_id is not null
       or v_economic.row_sign<>1
       or not (
         (p_movement_role='POSITIVE' and p_source_line_kind='SOURCE_ORDINARY'
           and p_correction_unit_id is null and p_prior_movement_id is null)
         or (p_movement_role='REPLACEMENT' and p_source_line_kind='SOURCE_REPLACEMENT'
           and p_correction_unit_id is not null and p_prior_movement_id is not null)
       )
     )) then
    raise exception 'WEEKLY_SOURCE_MOVEMENT_AUTHORITY_INVALID' using errcode='55000';
  end if;
  if p_source_profile_kind='NHSP_TRUST_BACKING_REPORT' then
    if not exists(
      select 1
      from public.weekly_source_final_revisions revision
      where revision.id=p_final_revision_id
        and revision.authority_scope_kind='NHSP_REPORT_SCOPE'
    ) then
      raise exception 'WEEKLY_SOURCE_MOVEMENT_SCOPE_INVALID' using errcode='55000';
    end if;
  elsif not exists(
    select 1
    from public.weekly_source_state_transitions transition_row
    where transition_row.id=p_transition_id
      and transition_row.final_revision_id=p_final_revision_id
      and transition_row.finalisation_cycle_id=p_finalisation_cycle_id
      and transition_row.work_event_id=v_resolution.work_event_id
      and transition_row.outcome=case when p_movement_role='POSITIVE' then 'ADD' else 'AMEND' end
  ) then
    raise exception 'WEEKLY_SOURCE_MOVEMENT_TRANSITION_INVALID' using errcode='55000';
  end if;

  v_pay_vector:=private.weekly_source_finalisation_vector_v1(v_economic.id,'PAY',1::smallint);
  v_charge_vector:=private.weekly_source_finalisation_vector_v1(v_economic.id,'CHARGE',1::smallint);
  v_mapping_fingerprint:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_MAPPING_RATE_POLICY_V1',
    pg_catalog.jsonb_build_object(
      'row_resolution_id',v_resolution.id,
      'work_event_id',v_resolution.work_event_id,
      'candidate_id',v_resolution.candidate_id,
      'client_id',v_resolution.client_id,
      'contract_id',v_resolution.contract_id,
      'contract_and_rate_fingerprint',pg_catalog.encode(v_economic.contract_and_rate_fingerprint,'hex'),
      'effective_policy_fingerprint',pg_catalog.encode(v_economic.effective_policy_fingerprint,'hex'),
      'invoice_vat_policy_fingerprint',pg_catalog.encode(v_economic.invoice_vat_policy_fingerprint,'hex'),
      'calculation_fingerprint',pg_catalog.encode(v_economic.calculation_fingerprint,'hex')
    )
  );
  v_vat_rate:=case when v_economic.invoice_vat_chargeable
    then v_economic.invoice_vat_rate_pct else 0 end;
  v_vat:=pg_catalog.round((p_invoice_presentation_charge_pence::numeric/100)*(v_vat_rate/100),2);
  v_total:=(p_invoice_presentation_charge_pence::numeric/100)+v_vat;
  v_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_BILLING_MOVEMENT_V1',
    pg_catalog.jsonb_build_object(
      'transition_id',p_transition_id,'nhsp_upload_row_id',p_nhsp_upload_row_id,
      'final_revision_id',p_final_revision_id,'finalisation_cycle_id',p_finalisation_cycle_id,
      'actual_client_id',v_resolution.client_id,'candidate_id',v_resolution.candidate_id,
      'contract_id',v_resolution.contract_id,'work_event_id',v_resolution.work_event_id,
      'movement_role',p_movement_role,'correction_unit_id',p_correction_unit_id,
      'prior_movement_id',p_prior_movement_id,'source_profile_kind',p_source_profile_kind,
      'source_line_kind',p_source_line_kind,'source_facts',p_source_facts_json,
      'pay_vector',v_pay_vector,'charge_vector',v_charge_vector,
      'total_pay_pence',v_economic.total_pay_pence,
      'calculated_comparison_charge_pence',v_economic.calculated_charge_pence,
      'source_validation_charge_pence',p_source_validation_charge_pence,
      'invoice_presentation_charge_pence',p_invoice_presentation_charge_pence,
      'vat_rate_pct',v_vat_rate,'vat_amount',v_vat,'total_inc_vat',v_total,
      'price_check_result',p_price_check_result,
      'price_check_fingerprint',case when p_price_check_fingerprint is null then null
        else pg_catalog.encode(p_price_check_fingerprint,'hex') end,
      'charge_acceptance_id',p_charge_acceptance_id,
      'mapping_rate_policy_fingerprint',pg_catalog.encode(v_mapping_fingerprint,'hex'),
      'invoice_timesheet_id',p_invoice_timesheet_id
    )
  );
  insert into public.weekly_source_billing_movements(
    transition_id,nhsp_upload_row_id,final_revision_id,finalisation_cycle_id,
    actual_client_id,candidate_id,contract_id,work_event_id,movement_role,
    correction_unit_id,prior_movement_id,source_profile_kind,source_line_kind,
    source_facts_json,canonical_pay_vector_json,canonical_charge_vector_json,
    total_pay_ex_vat,calculated_comparison_charge_pence,
    source_validation_charge_pence,invoice_presentation_charge_pence,
    vat_rate_pct,vat_amount,total_inc_vat,price_check_result,price_check_fingerprint,
    charge_acceptance_id,mapping_rate_policy_fingerprint,invoice_timesheet_id,original_cycle_key,
    movement_economic_hash,placement_state
  ) values (
    p_transition_id,p_nhsp_upload_row_id,p_final_revision_id,p_finalisation_cycle_id,
    v_resolution.client_id,v_resolution.candidate_id,v_resolution.contract_id,
    v_resolution.work_event_id,p_movement_role,p_correction_unit_id,p_prior_movement_id,
    p_source_profile_kind,p_source_line_kind,p_source_facts_json,v_pay_vector,v_charge_vector,
    v_economic.total_pay_pence::numeric/100,v_economic.calculated_charge_pence,
    p_source_validation_charge_pence,p_invoice_presentation_charge_pence,
    v_vat_rate,v_vat,v_total,p_price_check_result,p_price_check_fingerprint,
    p_charge_acceptance_id,v_mapping_fingerprint,p_invoice_timesheet_id,p_finalisation_cycle_id::text,
    v_hash,'UNPLACED'
  ) returning id into v_id;
  return v_id;
end;
$function$;

create or replace function private.weekly_source_finalisation_insert_reversal_v1(
  p_transition_id uuid,
  p_final_revision_id uuid,
  p_finalisation_cycle_id uuid,
  p_prior_movement_id uuid,
  p_correction_unit_id uuid,
  p_source_profile_kind text,
  p_source_facts_json jsonb
) returns uuid
language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_prior public.weekly_source_billing_movements%rowtype;
  v_pay_vector jsonb;
  v_charge_vector jsonb;
  v_hash bytea;
  v_id uuid;
begin
  select * into v_prior
  from public.weekly_source_billing_movements where id=p_prior_movement_id;
  if not found or v_prior.source_profile_kind='NHSP_TRUST_BACKING_REPORT'
     or v_prior.movement_role not in ('POSITIVE','REPLACEMENT')
     or p_transition_id is null or p_final_revision_id is null
     or p_finalisation_cycle_id is null or p_correction_unit_id is null
     or p_source_profile_kind not in ('GENERIC_COMPLETE_SNAPSHOT','HEALTHROSTER_ACTUAL_ROWS')
     or p_source_profile_kind is distinct from v_prior.source_profile_kind
     or pg_catalog.jsonb_typeof(p_source_facts_json)<>'object' then
    raise exception 'WEEKLY_SOURCE_REVERSAL_INPUT_INVALID' using errcode='55000';
  end if;
  v_pay_vector:=private.weekly_source_finalisation_negate_vector_v1(v_prior.canonical_pay_vector_json);
  v_charge_vector:=private.weekly_source_finalisation_negate_vector_v1(v_prior.canonical_charge_vector_json);
  v_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_BILLING_MOVEMENT_V1',
    pg_catalog.jsonb_build_object(
      'transition_id',p_transition_id,'final_revision_id',p_final_revision_id,
      'finalisation_cycle_id',p_finalisation_cycle_id,'prior_movement_id',v_prior.id,
      'correction_unit_id',p_correction_unit_id,'source_profile_kind',p_source_profile_kind,
      'source_line_kind','GENERATED_HISTORICAL_REVERSAL','source_facts',p_source_facts_json,
      'pay_vector',v_pay_vector,'charge_vector',v_charge_vector,
      'total_pay_ex_vat',-v_prior.total_pay_ex_vat,
      'calculated_comparison_charge_pence',-v_prior.calculated_comparison_charge_pence,
      'source_validation_charge_pence',case when v_prior.source_validation_charge_pence is null
        then null else -v_prior.source_validation_charge_pence end,
      'invoice_presentation_charge_pence',-v_prior.invoice_presentation_charge_pence,
      'vat_rate_pct',v_prior.vat_rate_pct,'vat_amount',-v_prior.vat_amount,
      'total_inc_vat',-v_prior.total_inc_vat,
      'invoice_timesheet_id',v_prior.invoice_timesheet_id
    )
  );
  insert into public.weekly_source_billing_movements(
    transition_id,nhsp_upload_row_id,final_revision_id,finalisation_cycle_id,
    actual_client_id,candidate_id,contract_id,work_event_id,movement_role,
    correction_unit_id,prior_movement_id,source_profile_kind,source_line_kind,
    source_facts_json,canonical_pay_vector_json,canonical_charge_vector_json,
    total_pay_ex_vat,calculated_comparison_charge_pence,
    source_validation_charge_pence,invoice_presentation_charge_pence,
    vat_rate_pct,vat_amount,total_inc_vat,price_check_result,price_check_fingerprint,
    mapping_rate_policy_fingerprint,invoice_timesheet_id,original_cycle_key,
    movement_economic_hash,placement_state
  ) values (
    p_transition_id,null,p_final_revision_id,p_finalisation_cycle_id,
    v_prior.actual_client_id,v_prior.candidate_id,v_prior.contract_id,v_prior.work_event_id,
    'REVERSAL',p_correction_unit_id,v_prior.id,p_source_profile_kind,
    'GENERATED_HISTORICAL_REVERSAL',p_source_facts_json,v_pay_vector,v_charge_vector,
    -v_prior.total_pay_ex_vat,-v_prior.calculated_comparison_charge_pence,
    case when v_prior.source_validation_charge_pence is null then null
      else -v_prior.source_validation_charge_pence end,
    -v_prior.invoice_presentation_charge_pence,v_prior.vat_rate_pct,
    -v_prior.vat_amount,-v_prior.total_inc_vat,v_prior.price_check_result,
    v_prior.price_check_fingerprint,v_prior.mapping_rate_policy_fingerprint,
    v_prior.invoice_timesheet_id,v_prior.original_cycle_key,v_hash,'UNPLACED'
  ) returning id into v_id;
  return v_id;
end;
$function$;

create or replace function private.weekly_source_finalisation_materialise_expense_invoice_v1(
  p_final_revision_id uuid,
  p_finalisation_cycle_id uuid,
  p_source_profile_kind text,
  p_expense_authority_generation_id uuid,
  p_prior_expense_authority_generation_id uuid,
  p_expense_policy_snapshot_id uuid,
  p_invoice_timesheet_id uuid,
  p_correction_presentation text
) returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_current public.weekly_expense_authority_generations%rowtype;
  v_prior public.weekly_expense_authority_generations%rowtype;
  v_expense_policy public.weekly_source_row_expense_policy_snapshots%rowtype;
  v_contract public.contracts%rowtype;
  v_event public.weekly_work_events%rowtype;
  v_prior_movement public.weekly_source_billing_movements%rowtype;
  v_revision public.weekly_source_final_revisions%rowtype;
  v_old_pence bigint:=0;
  v_new_pence bigint;
  v_correction_unit_id uuid;
  v_mapping_fingerprint bytea;
  v_reversal_hash bytea;
  v_replacement_hash bytea;
  v_reversal_id uuid;
  v_replacement_id uuid;
  v_reversal_vector jsonb;
  v_replacement_pay_vector jsonb;
  v_replacement_charge_vector jsonb;
  v_vat_rate numeric(5,2);
  v_vat numeric(12,2);
  v_total numeric(12,2);
  v_count integer:=0;
  v_prepared boolean:=false;
begin
  if p_final_revision_id is null or p_finalisation_cycle_id is null
     or p_expense_authority_generation_id is null
     or p_source_profile_kind not in ('GENERIC_COMPLETE_SNAPSHOT','HEALTHROSTER_ACTUAL_ROWS')
     or p_correction_presentation not in ('FULL_REVERSAL_REPLACEMENT','NET_DIFFERENCE_PRESENTATION') then
    raise exception 'WEEKLY_SOURCE_EXPENSE_MOVEMENT_INPUT_INVALID' using errcode='22023';
  end if;

  select * into strict v_revision
  from public.weekly_source_final_revisions
  where id=p_final_revision_id and source_cycle_id=p_finalisation_cycle_id
    and state in ('PREPARED','CURRENT') and authority_scope_kind='CYCLE';
  v_prepared:=v_revision.state='PREPARED';
  select * into strict v_current
  from public.weekly_expense_authority_generations
  where id=p_expense_authority_generation_id and final_revision_id=p_final_revision_id
    and state=case when v_prepared then 'PREPARED' else 'CURRENT' end;
  v_new_pence:=(v_current.client_charge_ex_vat*100)::bigint;
  if v_current.source_expense_pence is distinct from v_new_pence
     or v_current.candidate_reimbursement_ex_vat is distinct from v_current.client_charge_ex_vat
     or v_current.prior_expense_authority_generation_id
          is distinct from p_prior_expense_authority_generation_id
     or v_current.correction_presentation is distinct from p_correction_presentation then
    raise exception 'WEEKLY_SOURCE_EXPENSE_AUTHORITY_INVALID' using errcode='55000';
  end if;

  if p_prior_expense_authority_generation_id is not null then
    select * into strict v_prior
    from public.weekly_expense_authority_generations
    where id=p_prior_expense_authority_generation_id
      and work_event_id=v_current.work_event_id
      and (
        (not v_prepared and generation=v_current.generation-1)
        or (v_prepared and generation<v_current.generation)
      )
      and (
        (not v_prepared and state='SUPERSEDED')
        or (v_prepared and state in ('CURRENT','SUPERSEDED'))
      );
    if v_prepared and exists(
      select 1
      from public.weekly_expense_authority_generations skipped
      where skipped.work_event_id=v_current.work_event_id
        and skipped.generation>v_prior.generation
        and skipped.generation<v_current.generation
        and skipped.final_revision_id is distinct from v_revision.predecessor_revision_id
    ) then
      raise exception 'WEEKLY_SOURCE_PREPARED_EXPENSE_HISTORY_INVALID'
        using errcode='55000';
    end if;
    v_old_pence:=(v_prior.client_charge_ex_vat*100)::bigint;
    if v_prior.source_expense_pence is distinct from v_old_pence
       or v_prior.candidate_reimbursement_ex_vat is distinct from v_prior.client_charge_ex_vat then
      raise exception 'WEEKLY_SOURCE_PRIOR_EXPENSE_AUTHORITY_INVALID' using errcode='55000';
    end if;
  elsif v_current.generation<>1 then
    if not v_prepared or exists(
      select 1
      from public.weekly_expense_authority_generations skipped
      where skipped.work_event_id=v_current.work_event_id
        and skipped.generation<v_current.generation
        and skipped.final_revision_id is distinct from v_revision.predecessor_revision_id
    ) then
      raise exception 'WEEKLY_SOURCE_PRIOR_EXPENSE_AUTHORITY_MISSING' using errcode='55000';
    end if;
  end if;

  if v_new_pence=v_old_pence and (
       p_prior_expense_authority_generation_id is null
       or (v_current.source_expense_vat_enabled is not distinct from v_prior.source_expense_vat_enabled
           and v_current.contract_id is not distinct from v_prior.contract_id)
     ) then
    return pg_catalog.jsonb_build_object(
      'movement_count',0,'primary_movement_id',null,'zero_noop',true
    );
  end if;
  v_correction_unit_id:=case when v_old_pence>0 then pg_catalog.gen_random_uuid() end;

  if v_old_pence>0 then
    select * into v_prior_movement
    from public.weekly_source_billing_movements movement
    where movement.expense_authority_generation_id=p_prior_expense_authority_generation_id
      and movement.source_line_kind='SOURCE_FIXED_EXPENSE'
      and movement.movement_role in ('EXPENSE_POSITIVE','EXPENSE_REPLACEMENT')
      and movement.invoice_presentation_charge_pence=v_old_pence
    order by movement.created_at_utc desc,movement.id desc
    limit 1;
    if not found then
      raise exception 'WEEKLY_SOURCE_PRIOR_EXPENSE_MOVEMENT_MISSING' using errcode='55000';
    end if;
    v_reversal_vector:=pg_catalog.jsonb_build_object(
      'schema_version','WEEKLY_SOURCE_FIXED_EXPENSE_VECTOR_V1',
      'kind','PAY_AND_CHARGE','source_expense_pence',-v_old_pence,
      'source_expense_vat_enabled',v_prior.source_expense_vat_enabled
    );
    v_mapping_fingerprint:=private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_FIXED_EXPENSE_MAPPING_V1',
      pg_catalog.jsonb_build_object(
        'current_expense_authority_generation_id',v_current.id,
        'prior_expense_authority_generation_id',v_prior.id,
        'prior_movement_id',v_prior_movement.id,
        'role','EXPENSE_REVERSAL'
      )
    );
    v_reversal_hash:=private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_BILLING_MOVEMENT_V1',
      pg_catalog.jsonb_build_object(
        'expense_authority_generation_id',v_current.id,
        'final_revision_id',p_final_revision_id,
        'finalisation_cycle_id',p_finalisation_cycle_id,
        'actual_client_id',v_prior_movement.actual_client_id,
        'candidate_id',v_prior_movement.candidate_id,
        'contract_id',v_prior_movement.contract_id,
        'work_event_id',v_prior_movement.work_event_id,
        'movement_role','EXPENSE_REVERSAL','correction_unit_id',v_correction_unit_id,
        'prior_movement_id',v_prior_movement.id,
        'source_profile_kind',p_source_profile_kind,
        'source_line_kind','SOURCE_FIXED_EXPENSE',
        'source_facts',pg_catalog.jsonb_build_object(
          'schema_version','WEEKLY_SOURCE_FIXED_EXPENSE_FACTS_V1',
          'correction_presentation',p_correction_presentation,
          'old_expense_pence',v_old_pence,'new_expense_pence',v_new_pence,
          'authority_generation_id',v_current.id,
          'prior_authority_generation_id',v_prior.id,
          'row_expense_policy_snapshot_id',v_current.row_expense_policy_snapshot_id,
          'source_observation_kind',v_current.source_observation_kind
        ),
        'vector',v_reversal_vector,'invoice_presentation_charge_pence',-v_old_pence,
        'vat_rate_pct',v_prior_movement.vat_rate_pct,
        'vat_amount',-v_prior_movement.vat_amount,
        'total_inc_vat',-v_prior_movement.total_inc_vat,
        'mapping_rate_policy_fingerprint',pg_catalog.encode(v_mapping_fingerprint,'hex'),
        'invoice_timesheet_id',v_prior_movement.invoice_timesheet_id
      )
    );
    insert into public.weekly_source_billing_movements(
      transition_id,nhsp_upload_row_id,expense_authority_generation_id,
      final_revision_id,finalisation_cycle_id,actual_client_id,candidate_id,
      contract_id,work_event_id,movement_role,correction_unit_id,prior_movement_id,
      source_profile_kind,source_line_kind,source_facts_json,
      canonical_pay_vector_json,canonical_charge_vector_json,total_pay_ex_vat,
      calculated_comparison_charge_pence,source_validation_charge_pence,
      invoice_presentation_charge_pence,vat_rate_pct,vat_amount,total_inc_vat,
      price_check_result,price_check_fingerprint,mapping_rate_policy_fingerprint,
      invoice_timesheet_id,original_cycle_key,movement_economic_hash,placement_state
    ) values (
      null,null,v_current.id,p_final_revision_id,p_finalisation_cycle_id,
      v_prior_movement.actual_client_id,v_prior_movement.candidate_id,
      v_prior_movement.contract_id,v_prior_movement.work_event_id,
      'EXPENSE_REVERSAL',v_correction_unit_id,v_prior_movement.id,
      p_source_profile_kind,'SOURCE_FIXED_EXPENSE',
      pg_catalog.jsonb_build_object(
        'schema_version','WEEKLY_SOURCE_FIXED_EXPENSE_FACTS_V1',
        'correction_presentation',p_correction_presentation,
        'old_expense_pence',v_old_pence,'new_expense_pence',v_new_pence,
        'authority_generation_id',v_current.id,
        'prior_authority_generation_id',v_prior.id,
        'row_expense_policy_snapshot_id',v_current.row_expense_policy_snapshot_id,
        'source_observation_kind',v_current.source_observation_kind
      ),v_reversal_vector,v_reversal_vector,-v_old_pence::numeric/100,
      -v_old_pence,-v_old_pence,-v_old_pence,v_prior_movement.vat_rate_pct,
      -v_prior_movement.vat_amount,-v_prior_movement.total_inc_vat,
      'NOT_APPLICABLE',null,v_mapping_fingerprint,
      v_prior_movement.invoice_timesheet_id,p_finalisation_cycle_id::text,
      v_reversal_hash,'UNPLACED'
    ) returning id into v_reversal_id;
    v_count:=v_count+1;
  end if;

  if v_new_pence>0 then
    if p_expense_policy_snapshot_id is null or p_invoice_timesheet_id is null
       or v_current.source_observation_kind<>'ROW_PRESENT'
       or v_current.row_expense_policy_snapshot_id is distinct from p_expense_policy_snapshot_id then
      raise exception 'WEEKLY_SOURCE_EXPENSE_POLICY_SNAPSHOT_MISSING' using errcode='55000';
    end if;
    select * into strict v_expense_policy
    from public.weekly_source_row_expense_policy_snapshots
    where id=p_expense_policy_snapshot_id;
    select * into strict v_contract
    from public.contracts where id=v_current.contract_id;
    select * into strict v_event
    from public.weekly_work_events where id=v_current.work_event_id;
    if v_expense_policy.work_event_id is distinct from v_current.work_event_id
       or v_expense_policy.contract_id is distinct from v_current.contract_id
       or v_expense_policy.source_expense_pence is distinct from v_current.source_expense_pence
       or v_contract.id is distinct from v_expense_policy.contract_id
       or v_contract.client_id is distinct from v_expense_policy.client_id
       or v_contract.candidate_id is distinct from v_expense_policy.candidate_id
       or v_event.client_id is distinct from v_expense_policy.client_id
       or v_event.candidate_id is distinct from v_expense_policy.candidate_id
       or v_expense_policy.source_expense_vat_enabled is distinct from v_current.source_expense_vat_enabled then
      raise exception 'WEEKLY_SOURCE_EXPENSE_POLICY_SNAPSHOT_INVALID' using errcode='55000';
    end if;
    v_vat_rate:=case when v_current.source_expense_vat_enabled
      then v_expense_policy.invoice_vat_rate_pct else 0 end;
    v_vat:=pg_catalog.round((v_new_pence::numeric/100)*(v_vat_rate/100),2);
    v_total:=v_new_pence::numeric/100+v_vat;
    v_replacement_pay_vector:=pg_catalog.jsonb_build_object(
      'schema_version','WEEKLY_SOURCE_FIXED_EXPENSE_VECTOR_V1','kind','PAY',
      'source_expense_pence',v_new_pence,
      'source_expense_vat_enabled',v_current.source_expense_vat_enabled
    );
    v_replacement_charge_vector:=v_replacement_pay_vector||pg_catalog.jsonb_build_object('kind','CHARGE');
    v_mapping_fingerprint:=private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_FIXED_EXPENSE_MAPPING_V1',
      pg_catalog.jsonb_build_object(
        'current_expense_authority_generation_id',v_current.id,
        'prior_expense_authority_generation_id',p_prior_expense_authority_generation_id,
        'expense_policy_snapshot_id',v_expense_policy.id,
        'invoice_vat_policy_fingerprint',pg_catalog.encode(v_expense_policy.invoice_vat_policy_fingerprint,'hex'),
        'role',case when v_old_pence=0 then 'EXPENSE_POSITIVE' else 'EXPENSE_REPLACEMENT' end
      )
    );
    v_replacement_hash:=private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_BILLING_MOVEMENT_V1',
      pg_catalog.jsonb_build_object(
        'expense_authority_generation_id',v_current.id,
        'final_revision_id',p_final_revision_id,
        'finalisation_cycle_id',p_finalisation_cycle_id,
        'actual_client_id',v_expense_policy.client_id,'candidate_id',v_expense_policy.candidate_id,
        'contract_id',v_expense_policy.contract_id,'work_event_id',v_expense_policy.work_event_id,
        'movement_role',case when v_old_pence=0 then 'EXPENSE_POSITIVE' else 'EXPENSE_REPLACEMENT' end,
        'correction_unit_id',v_correction_unit_id,'prior_movement_id',v_prior_movement.id,
        'source_profile_kind',p_source_profile_kind,'source_line_kind','SOURCE_FIXED_EXPENSE',
        'source_facts',pg_catalog.jsonb_build_object(
          'schema_version','WEEKLY_SOURCE_FIXED_EXPENSE_FACTS_V1',
          'correction_presentation',p_correction_presentation,
          'old_expense_pence',v_old_pence,'new_expense_pence',v_new_pence,
          'authority_generation_id',v_current.id,
          'prior_authority_generation_id',p_prior_expense_authority_generation_id,
          'row_expense_policy_snapshot_id',v_current.row_expense_policy_snapshot_id,
          'source_observation_kind',v_current.source_observation_kind
        ),
        'pay_vector',v_replacement_pay_vector,'charge_vector',v_replacement_charge_vector,
        'invoice_presentation_charge_pence',v_new_pence,'vat_rate_pct',v_vat_rate,
        'vat_amount',v_vat,'total_inc_vat',v_total,
        'mapping_rate_policy_fingerprint',pg_catalog.encode(v_mapping_fingerprint,'hex'),
        'invoice_timesheet_id',p_invoice_timesheet_id
      )
    );
    insert into public.weekly_source_billing_movements(
      transition_id,nhsp_upload_row_id,expense_authority_generation_id,
      final_revision_id,finalisation_cycle_id,actual_client_id,candidate_id,
      contract_id,work_event_id,movement_role,correction_unit_id,prior_movement_id,
      source_profile_kind,source_line_kind,source_facts_json,
      canonical_pay_vector_json,canonical_charge_vector_json,total_pay_ex_vat,
      calculated_comparison_charge_pence,source_validation_charge_pence,
      invoice_presentation_charge_pence,vat_rate_pct,vat_amount,total_inc_vat,
      price_check_result,price_check_fingerprint,mapping_rate_policy_fingerprint,
      invoice_timesheet_id,original_cycle_key,movement_economic_hash,placement_state
    ) values (
      null,null,v_current.id,p_final_revision_id,p_finalisation_cycle_id,
      v_expense_policy.client_id,v_expense_policy.candidate_id,v_expense_policy.contract_id,
      v_expense_policy.work_event_id,
      case when v_old_pence=0 then 'EXPENSE_POSITIVE' else 'EXPENSE_REPLACEMENT' end,
      v_correction_unit_id,case when v_old_pence>0 then v_prior_movement.id end,
      p_source_profile_kind,'SOURCE_FIXED_EXPENSE',
      pg_catalog.jsonb_build_object(
        'schema_version','WEEKLY_SOURCE_FIXED_EXPENSE_FACTS_V1',
        'correction_presentation',p_correction_presentation,
        'old_expense_pence',v_old_pence,'new_expense_pence',v_new_pence,
        'authority_generation_id',v_current.id,
        'prior_authority_generation_id',p_prior_expense_authority_generation_id,
        'row_expense_policy_snapshot_id',v_current.row_expense_policy_snapshot_id,
        'source_observation_kind',v_current.source_observation_kind
      ),v_replacement_pay_vector,v_replacement_charge_vector,v_new_pence::numeric/100,
      v_new_pence,v_new_pence,v_new_pence,v_vat_rate,v_vat,v_total,
      'NOT_APPLICABLE',null,v_mapping_fingerprint,p_invoice_timesheet_id,
      p_finalisation_cycle_id::text,v_replacement_hash,'UNPLACED'
    ) returning id into v_replacement_id;
    v_count:=v_count+1;
  end if;

  return pg_catalog.jsonb_build_object(
    'movement_count',v_count,
    'reversal_movement_id',v_reversal_id,
    'replacement_movement_id',v_replacement_id,
    'primary_movement_id',coalesce(v_replacement_id,v_reversal_id),
    'zero_noop',false
  );
end;
$function$;

create or replace function private.weekly_source_finalise_engine_v1(
  p_request jsonb,
  p_correction_session_id uuid,
  p_prepare_only boolean
) returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_allowed_keys constant text[]:=array[
    'actor_user_id','source_cycle_id','authority_scope_kind','report_scope_id',
    'upload_id','projection_publication_id','expected_authority_scope_version',
    'expected_row_manifest_hash','expected_comparison_manifest_hash',
    'expected_issue_set_hash'
  ]::text[];
  v_unknown_key text;
  v_actor uuid;
  v_cycle_id uuid;
  v_scope_kind text;
  v_report_scope_id uuid;
  v_upload_id uuid;
  v_publication_id uuid;
  v_scope_version bigint;
  v_expected_row_hash bytea;
  v_expected_comparison_hash bytea;
  v_expected_issue_hash bytea;
  v_cycle public.weekly_source_cycles%rowtype;
  v_scope public.weekly_source_report_scopes%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_upload public.weekly_source_uploads%rowtype;
  v_publication public.weekly_source_projection_publications%rowtype;
  v_profile public.weekly_source_format_profiles%rowtype;
  v_existing_revision public.weekly_source_final_revisions%rowtype;
  v_correction public.weekly_final_source_correction_sessions%rowtype;
  v_guard jsonb;
  v_client_id uuid;
  v_resolved_client_id uuid;
  v_resolved_client_count integer;
  v_source_profile_kind text;
  v_source_mode text;
  v_generation integer;
  v_coverage_start date;
  v_coverage_end date;
  v_prior_revision_id uuid;
  v_revision_number integer;
  v_revision_id uuid;
  v_revision_manifest_hash bytea;
  v_policy_fingerprint bytea;
  v_finalised_at timestamptz:=pg_catalog.transaction_timestamp();
  v_row record;
  v_finalisation_root_ids uuid[];
  v_finalisation_root_recheck uuid[];
  v_finalisation_lock_result jsonb;
  v_economic_id uuid;
  v_timesheet_id uuid;
  v_economic public.weekly_source_row_economic_snapshots%rowtype;
  v_snapshot_id uuid;
  v_snapshot_hash bytea;
  v_mapping_fingerprint bytea;
  v_pay_vector jsonb;
  v_charge_vector jsonb;
  v_transition record;
  v_prior_snapshot public.weekly_source_final_snapshot_lines%rowtype;
  v_current_snapshot public.weekly_source_final_snapshot_lines%rowtype;
  v_prior_state bytea;
  v_current_state bytea;
  v_outcome text;
  v_transition_id uuid;
  v_transition_fingerprint bytea;
  v_prior_movement public.weekly_source_billing_movements%rowtype;
  v_correction_unit_id uuid;
  v_charge_check public.weekly_source_charge_checks%rowtype;
  v_charge_acceptance public.weekly_source_charge_acceptances%rowtype;
  v_price_admission_result text;
  v_movement_id uuid;
  v_backing_report_number text;
  v_source_total_cost bigint;
  v_source_commission bigint;
  v_source_invoice_total bigint;
  v_line_count integer;
  v_manifest record;
  v_client_manifest_id uuid;
  v_client_manifest_hash bytea;
  v_completion_generation integer;
  v_completion_hash bytea;
  v_expense_observation record;
  v_target_expense_pence bigint;
  v_target_expense_vat boolean;
  v_target_expense_contract_id uuid;
  v_expense_observation_kind text;
  v_current_expense public.weekly_expense_authority_generations%rowtype;
  v_expense_policy public.weekly_source_row_expense_policy_snapshots%rowtype;
  v_expense_generation integer;
  v_expense_hash bytea;
  v_expense_authority_id uuid;
  v_prior_expense_authority_id uuid;
  v_expense_timesheet_id uuid;
  v_expense_policy_id uuid;
  v_expense_movement_result jsonb;
  v_effective_policy jsonb;
  v_correction_presentation text;
  v_movement_count integer:=0;
  v_transition_count integer:=0;
  v_snapshot_count integer:=0;
  v_unchanged_count integer:=0;
  v_expense_generation_count integer:=0;
begin
  perform pg_catalog.set_config('lock_timeout','5s',true);
  if coalesce(current_setting('request.jwt.claim.role',true),
       nullif(current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;
  if p_prepare_only is null
     or (p_prepare_only and p_correction_session_id is null)
     or (not p_prepare_only and p_correction_session_id is not null) then
    raise exception 'WEEKLY_SOURCE_FINALISE_EXECUTION_MODE_INVALID' using errcode='22023';
  end if;
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object' then
    raise exception 'WEEKLY_SOURCE_FINALISE_REQUEST_INVALID' using errcode='22023';
  end if;
  select key into v_unknown_key
  from pg_catalog.jsonb_object_keys(p_request) key
  where not (key=any(v_allowed_keys))
  order by key limit 1;
  if v_unknown_key is not null then
    raise exception 'WEEKLY_SOURCE_FINALISE_REQUEST_UNKNOWN_FIELD'
      using errcode='22023',detail=v_unknown_key;
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_cycle_id:=(p_request->>'source_cycle_id')::uuid;
    v_scope_kind:=pg_catalog.upper(pg_catalog.btrim(coalesce(p_request->>'authority_scope_kind','')));
    v_report_scope_id:=nullif(p_request->>'report_scope_id','')::uuid;
    v_upload_id:=(p_request->>'upload_id')::uuid;
    v_publication_id:=(p_request->>'projection_publication_id')::uuid;
    v_scope_version:=(p_request->>'expected_authority_scope_version')::bigint;
  exception when invalid_text_representation or numeric_value_out_of_range then
    raise exception 'WEEKLY_SOURCE_FINALISE_REQUEST_INVALID' using errcode='22023';
  end;
  if v_actor is null or v_cycle_id is null or v_upload_id is null
     or v_publication_id is null or v_scope_version is null or v_scope_version<1
     or v_scope_kind not in ('CYCLE','NHSP_REPORT_SCOPE')
     or (v_scope_kind='NHSP_REPORT_SCOPE') is distinct from (v_report_scope_id is not null) then
    raise exception 'WEEKLY_SOURCE_FINALISE_SCOPE_INVALID' using errcode='22023';
  end if;
  v_expected_row_hash:=private.weekly_source_finalisation_hex32_v1(
    p_request->>'expected_row_manifest_hash','WEEKLY_SOURCE_ROW_MANIFEST_HASH_REQUIRED'
  );
  v_expected_comparison_hash:=private.weekly_source_finalisation_hex32_v1(
    p_request->>'expected_comparison_manifest_hash','WEEKLY_SOURCE_COMPARISON_HASH_REQUIRED'
  );
  v_expected_issue_hash:=private.weekly_source_finalisation_hex32_v1(
    p_request->>'expected_issue_set_hash','WEEKLY_SOURCE_ISSUE_HASH_REQUIRED'
  );

  select * into v_cycle
  from public.weekly_source_cycles where id=v_cycle_id for update;
  if not found then
    raise exception 'WEEKLY_SOURCE_CYCLE_NOT_FOUND' using errcode='22023';
  end if;
  select * into strict v_group
  from public.weekly_source_groups where id=v_cycle.source_group_id;
  if v_scope_kind='NHSP_REPORT_SCOPE' then
    select * into v_scope
    from public.weekly_source_report_scopes where id=v_report_scope_id for update;
    if not found or v_scope.source_cycle_id is distinct from v_cycle.id
       or v_scope.source_group_id is distinct from v_group.id
       or v_scope.environment is distinct from v_group.environment
       or v_scope.agency_id is distinct from v_group.agency_id
       or v_scope.cutoff_at_utc is distinct from v_cycle.cutoff_at_utc then
      raise exception 'WEEKLY_SOURCE_REPORT_SCOPE_MISMATCH' using errcode='22023';
    end if;
  end if;
  select * into v_upload
  from public.weekly_source_uploads where id=v_upload_id for share;
  if not found then
    raise exception 'WEEKLY_SOURCE_UPLOAD_NOT_FOUND' using errcode='22023';
  end if;
  select * into strict v_publication
  from public.weekly_source_projection_publications where id=v_publication_id for share;
  select * into strict v_profile
  from public.weekly_source_format_profiles where id=v_upload.source_format_profile_id;

  if p_prepare_only then
    select * into v_correction
    from public.weekly_final_source_correction_sessions
    where id=p_correction_session_id for share;
    if not found or v_correction.state<>'PREPARING'
       or v_correction.source_cycle_id is distinct from v_cycle.id
       or v_correction.authority_scope_kind is distinct from v_scope_kind
       or v_correction.report_scope_id is distinct from v_report_scope_id
       or v_correction.replacement_correction_upload_id is distinct from v_upload.id
       or v_correction.replacement_projection_publication_id is distinct from v_publication.id
       or v_upload.purpose<>'FINAL_SOURCE_CORRECTION'
       or v_upload.correction_session_id is distinct from v_correction.id
       or v_upload.state<>'CORRECTION_READY'
       or v_publication.state<>'CORRECTION_READY'
       or v_publication.correction_session_id is distinct from v_correction.id
       or v_publication.upload_id is distinct from v_upload.id
       or v_publication.authority_scope_version is distinct from v_scope_version then
      raise exception 'WEEKLY_SOURCE_CORRECTION_SESSION_STALE' using errcode='40001';
    end if;
    v_guard:=pg_catalog.jsonb_build_object(
      'row_manifest_hash',pg_catalog.encode(v_upload.row_manifest_hash,'hex'),
      'comparison_manifest_hash',pg_catalog.encode(v_publication.comparison_manifest_hash,'hex'),
      'issue_set_hash',pg_catalog.encode(v_publication.issue_set_hash,'hex')
    );
  else
    v_guard:=private.weekly_source_current_publication_guard_v1(
      v_cycle.id,v_scope_kind,v_report_scope_id,v_upload.id,v_publication.id,v_scope_version
    );
  end if;
  if private.weekly_source_finalisation_hex32_v1(
       v_guard->>'row_manifest_hash','WEEKLY_SOURCE_GUARD_ROW_HASH_INVALID'
     ) is distinct from v_expected_row_hash
     or private.weekly_source_finalisation_hex32_v1(
       v_guard->>'comparison_manifest_hash','WEEKLY_SOURCE_GUARD_COMPARISON_HASH_INVALID'
     ) is distinct from v_expected_comparison_hash
     or private.weekly_source_finalisation_hex32_v1(
       v_guard->>'issue_set_hash','WEEKLY_SOURCE_GUARD_ISSUE_HASH_INVALID'
     ) is distinct from v_expected_issue_hash then
    raise exception 'WEEKLY_SOURCE_FINALISE_PREVIEW_CHANGED' using errcode='40001';
  end if;

  v_client_id:=case when v_scope_kind='NHSP_REPORT_SCOPE' then v_scope.client_id
    else nullif(v_upload.file_metadata_json->>'client_id','')::uuid end;
  perform private.weekly_source_office_authority_v1(
    v_actor,case when p_prepare_only then 'CORRECT_FINAL_SOURCE' else 'FINALISE_WEEK' end,
    v_group.id,v_client_id,v_cycle.finalisation_week_ending
  );
  if p_correction_session_id is null then
    if v_upload.purpose<>'ORDINARY' or v_upload.correction_session_id is not null then
      raise exception 'WEEKLY_SOURCE_CORRECTION_REQUIRES_DEDICATED_OWNER' using errcode='55000';
    end if;
  else
    if v_correction.id is null or v_correction.state<>'PREPARING'
       or v_correction.source_cycle_id is distinct from v_cycle.id
       or v_correction.authority_scope_kind is distinct from v_scope_kind
       or v_correction.report_scope_id is distinct from v_report_scope_id
       or v_correction.replacement_correction_upload_id is distinct from v_upload.id
       or v_correction.replacement_projection_publication_id is distinct from v_publication.id
       or v_upload.purpose<>'FINAL_SOURCE_CORRECTION'
       or v_upload.correction_session_id is distinct from v_correction.id then
      raise exception 'WEEKLY_SOURCE_CORRECTION_SESSION_STALE' using errcode='40001';
    end if;
  end if;
  if v_finalised_at<coalesce(v_scope.cutoff_at_utc,v_cycle.cutoff_at_utc) then
    raise exception 'WEEKLY_SOURCE_CUTOFF_NOT_REACHED' using errcode='55000';
  end if;

  if v_upload.state<>(case when p_prepare_only then 'CORRECTION_READY' else 'CURRENT' end)
     or v_upload.row_manifest_hash is null
     or v_upload.blocked_count<>0 or v_upload.blocking_economic_duplicate_count<>0
     or v_upload.malformed_count<>0
     or v_upload.physical_row_count<>
       v_upload.header_count+v_upload.trailer_count+v_upload.continuation_count+
       v_upload.accepted_count+v_upload.blocking_economic_duplicate_count+v_upload.malformed_count
     or (select pg_catalog.count(*) from public.weekly_source_upload_rows
         where upload_id=v_upload.id)<>v_upload.accepted_count then
    raise exception 'WEEKLY_SOURCE_UPLOAD_NOT_FINALISABLE' using errcode='55000';
  end if;
  -- WP-54, pack 03 section 7: "Overlapping candidate work events are checked
  -- BEFORE FINALISATION.  The system does not assume two overlapping records
  -- are valid because their references differ."  The same guard runs at the
  -- seal, where a refused report never becomes CURRENT.  It runs again here
  -- because this is the owner the pack names and because the rows this engine
  -- is about to turn into movements are the rows that must satisfy it -- pack
  -- 14 section 8.1, the database decides source authority, never the browser.
  -- Deliberately NOT accompanied by a cutoff re-check: NHSP-BR-006 governs the
  -- cutoff occurrence Office confirms when the report is accepted, and the
  -- seal is where that confirmation is made authoritative.
  perform private.weekly_source_overlap_admission_assert_v1(v_upload.id);
  if v_scope_version>2147483647
     or coalesce(v_publication.projection_generation,0)>2147483647 then
    raise exception 'WEEKLY_SOURCE_GENERATION_OVERFLOW' using errcode='22003';
  end if;
  v_generation:=coalesce(
    v_publication.projection_generation,
    v_scope_version::integer
  );
  if (select pg_catalog.count(*)
      from public.weekly_source_row_resolutions resolution
      join public.weekly_source_upload_rows source_row on source_row.id=resolution.upload_row_id
      where source_row.upload_id=v_upload.id and resolution.generation=v_generation)
       <>v_upload.accepted_count
     or exists(
       select 1
       from public.weekly_source_upload_rows source_row
       left join public.weekly_source_row_resolutions resolution
         on resolution.upload_row_id=source_row.id and resolution.generation=v_generation
       where source_row.upload_id=v_upload.id
         and (resolution.id is null or resolution.mapping_state<>'RESOLVED')
     )
     or (v_profile.final_authority_kind<>'NHSP_TRUST_BACKING_REPORT' and exists(
       select 1
       from public.weekly_source_row_resolutions resolution
       join public.weekly_source_upload_rows source_row on source_row.id=resolution.upload_row_id
       where source_row.upload_id=v_upload.id and resolution.generation=v_generation
       group by resolution.work_event_id having pg_catalog.count(*)<>1
     )) then
    raise exception 'WEEKLY_SOURCE_RESOLUTION_CENSUS_NOT_FINALISABLE' using errcode='55000';
  end if;
  if v_scope_kind='CYCLE' then
    select pg_catalog.count(distinct resolution.client_id)::integer,
           pg_catalog.min(resolution.client_id::text)::uuid
    into v_resolved_client_count,v_resolved_client_id
    from public.weekly_source_row_resolutions resolution
    join public.weekly_source_upload_rows source_row on source_row.id=resolution.upload_row_id
    where source_row.upload_id=v_upload.id and resolution.generation=v_generation;
    if v_resolved_client_count>1 then
      raise exception 'WEEKLY_SOURCE_SINGLE_CLIENT_RESOLUTION_REQUIRED' using errcode='55000';
    end if;
    if v_resolved_client_count=1 and v_client_id is not null
       and v_client_id is distinct from v_resolved_client_id then
      raise exception 'WEEKLY_SOURCE_UPLOAD_CLIENT_MISMATCH' using errcode='55000';
    end if;
    v_client_id:=coalesce(v_resolved_client_id,v_client_id);
    if v_client_id is null or not exists(
      select 1
      from public.weekly_source_group_clients group_client
      where group_client.source_group_id=v_group.id
        and group_client.client_id=v_client_id
        and v_cycle.finalisation_week_ending between group_client.valid_from
          and coalesce(group_client.valid_to,'infinity'::date)
    ) then
      raise exception 'WEEKLY_SOURCE_FINAL_CLIENT_SCOPE_REQUIRED' using errcode='55000';
    end if;
    perform private.weekly_source_office_authority_v1(
      v_actor,case when p_prepare_only then 'CORRECT_FINAL_SOURCE' else 'FINALISE_WEEK' end,
      v_group.id,v_client_id,v_cycle.finalisation_week_ending
    );
  elsif not exists(
    select 1
    from public.weekly_source_group_clients group_client
    where group_client.source_group_id=v_group.id
      and group_client.client_id=v_client_id
      and v_cycle.finalisation_week_ending between group_client.valid_from
        and coalesce(group_client.valid_to,'infinity'::date)
  ) then
    raise exception 'WEEKLY_SOURCE_FINAL_CLIENT_SCOPE_REQUIRED' using errcode='55000';
  end if;

  v_source_profile_kind:=v_profile.final_authority_kind;
  if v_profile.profile_code='NHSP_FINAL_BACKING_V1' then
    if v_scope_kind<>'NHSP_REPORT_SCOPE' or v_group.source_family<>'NHSP'
       or v_source_profile_kind<>'NHSP_TRUST_BACKING_REPORT'
       or v_profile.omission_meaning<>'NO_INFERENCE'
       or not v_profile.report_number_required or not v_profile.fmc_must_equal_zero
       or v_client_id is null
       or exists(select 1 from public.weekly_source_upload_rows source_row
         where source_row.upload_id=v_upload.id
           and (source_row.row_finalisation_state not in ('NOT_APPLICABLE','SOURCE_WORKED')
              or source_row.source_money_parse_state<>'VALID'
              or source_row.source_shift_charge_pence is null
              or source_row.source_expense_parse_state<>'NOT_APPLICABLE'
             or source_row.source_expense_pence is not null)) then
      raise exception 'WEEKLY_SOURCE_NHSP_FINAL_NOT_FINALISABLE' using errcode='55000';
    end if;
    v_source_mode:='NHSP_WEEKLY';
    v_coverage_start:=null;
    v_coverage_end:=null;
    v_backing_report_number:=private.weekly_source_canonical_report_number_v1(
      v_upload.file_metadata_json->>'nhsp_report_number'
    );
    if v_backing_report_number is null then
      raise exception 'WEEKLY_SOURCE_NHSP_REPORT_NUMBER_REQUIRED' using errcode='55000';
    end if;
  elsif v_profile.profile_code in (
    'HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1','HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1'
  ) then
    if v_scope_kind<>'CYCLE' or v_group.source_family<>'ROSTER'
       or v_source_profile_kind<>'HEALTHROSTER_ACTUAL_ROWS'
       or v_profile.omission_meaning<>'CANCEL_INSIDE_CONFIRMED_COVERAGE'
       or v_client_id is null
       or exists(select 1 from public.weekly_source_upload_rows source_row
         where source_row.upload_id=v_upload.id
           and (source_row.row_finalisation_state not in ('SOURCE_WORKED','SOURCE_UNFINALISED')
             or source_row.source_expense_parse_state<>'NOT_APPLICABLE'
             or source_row.source_expense_pence is not null)) then
      raise exception 'WEEKLY_SOURCE_HEALTHROSTER_NOT_FINALISABLE' using errcode='55000';
    end if;
    v_source_mode:='HEALTHROSTER_WEEKLY';
    v_coverage_start:=v_upload.confirmed_coverage_start_local_date;
    v_coverage_end:=v_upload.confirmed_coverage_end_local_date;
  elsif v_profile.profile_code='ROSTER_WEEKLY_SUMMARY_ACTUAL_V1' then
    if v_scope_kind<>'CYCLE' or v_group.source_family<>'ROSTER'
       or v_source_profile_kind<>'GENERIC_COMPLETE_SNAPSHOT'
       or v_profile.omission_meaning<>'CANCEL_INSIDE_CONFIRMED_COVERAGE'
       or v_client_id is null
       or exists(select 1 from public.weekly_source_upload_rows source_row
         where source_row.upload_id=v_upload.id
           and (source_row.row_finalisation_state not in ('NOT_APPLICABLE','SOURCE_WORKED','SOURCE_ABSENT_ZERO')
             or source_row.source_expense_parse_state not in ('NOT_APPLICABLE','VALID','OMITTED_ZERO')
             or (source_row.source_expense_parse_state='NOT_APPLICABLE')
                  is distinct from (source_row.source_expense_pence is null)
             or (source_row.source_expense_parse_state='OMITTED_ZERO'
                 and source_row.source_expense_pence<>0))) then
      raise exception 'WEEKLY_SOURCE_ROSTER_COMPLETE_NOT_FINALISABLE' using errcode='55000';
    end if;
    v_source_mode:='HEALTHROSTER_WEEKLY';
    v_coverage_start:=v_upload.confirmed_coverage_start_local_date;
    v_coverage_end:=v_upload.confirmed_coverage_end_local_date;
  else
    raise exception 'WEEKLY_SOURCE_PROFILE_NOT_FINAL_AUTHORITY' using errcode='55000';
  end if;
  if v_scope_kind='CYCLE' and (
    v_upload.coverage_state<>'COMPLETE' or v_upload.coverage_timezone<>'Europe/London'
    or v_coverage_start is null or v_coverage_end is null or v_coverage_start>v_coverage_end
  ) then
    raise exception 'WEEKLY_SOURCE_CONFIRMED_COVERAGE_REQUIRED' using errcode='55000';
  end if;
  if exists(
    select 1
    from public.weekly_source_row_resolutions resolution
    join public.weekly_source_upload_rows source_row on source_row.id=resolution.upload_row_id
    where source_row.upload_id=v_upload.id and resolution.generation=v_generation
      and (resolution.client_id is distinct from v_client_id
        or source_row.work_date<coalesce(v_coverage_start,source_row.work_date)
        or source_row.work_date>coalesce(v_coverage_end,source_row.work_date))
  ) then
    raise exception 'WEEKLY_SOURCE_RESOLUTION_OUTSIDE_FINAL_SCOPE' using errcode='55000';
  end if;

  -- One transaction at a time may decide the final order for a logical
  -- group/Client/source-authority history.  Both supported HealthRoster
  -- layouts share the same final_authority_kind and therefore the same lock.
  -- The lock is acquired only after the exact current publication and Client
  -- have been resolved; no other owner acquires this finaliser-only domain.
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
    'weekly_source_finalise_order:'||v_group.id::text||':'||v_client_id::text||':'||v_source_profile_kind,
    0
  ));

  if p_prepare_only then
    if coalesce(v_scope.current_final_revision_id,v_cycle.current_final_revision_id)
         is distinct from v_correction.expected_current_final_revision_id then
      raise exception 'WEEKLY_SOURCE_CORRECTION_FINAL_REVISION_STALE' using errcode='40001';
    end if;
  elsif coalesce(v_scope.current_final_revision_id,v_cycle.current_final_revision_id) is not null then
    select * into strict v_existing_revision
    from public.weekly_source_final_revisions
    where id=coalesce(v_scope.current_final_revision_id,v_cycle.current_final_revision_id);
    if v_existing_revision.upload_id=v_upload.id
       and v_existing_revision.authority_scope_kind=v_scope_kind
       and v_existing_revision.report_scope_id is not distinct from v_report_scope_id
       and v_existing_revision.state='CURRENT' then
      return pg_catalog.jsonb_build_object(
        'ok',true,'status','FINALISED','idempotent',true,
        'final_revision_id',v_existing_revision.id,'source_cycle_id',v_cycle.id,
        'authority_scope_kind',v_scope_kind,'report_scope_id',v_report_scope_id,
        'upload_id',v_upload.id
      );
    end if;
    raise exception 'WEEKLY_SOURCE_SCOPE_ALREADY_FINALISED' using errcode='55000';
  end if;

  if v_scope_kind='CYCLE' and exists(
    select 1
    from public.weekly_source_final_revisions later_revision
    join public.weekly_source_cycles later_cycle
      on later_cycle.id=later_revision.source_cycle_id
    join public.weekly_source_client_manifests later_manifest
      on later_manifest.final_revision_id=later_revision.id
     and later_manifest.client_id=v_client_id
    join public.weekly_source_uploads later_upload
      on later_upload.id=later_revision.upload_id
    join public.weekly_source_format_profiles later_profile
      on later_profile.id=later_upload.source_format_profile_id
    where later_revision.authority_scope_kind='CYCLE'
      and later_revision.state='CURRENT'
      and later_cycle.source_group_id=v_group.id
      and later_cycle.finalisation_week_ending>v_cycle.finalisation_week_ending
      and later_profile.final_authority_kind=v_source_profile_kind
  ) then
    raise exception 'WEEKLY_SOURCE_FINALISATION_OUT_OF_ORDER' using errcode='55000';
  end if;

  -- Review F2.  Finalisation calls the lineage ensure owner once per source row
  -- inside ONE transaction, so without this it accumulates family lock sets in
  -- source_row_ordinal order rather than the (btrim(booking_id), booking_id)
  -- order proof/32 section 6 step 2 fixes.  That is an A-B / B-A cycle against
  -- any sorted I-1 caller -- a publication, a release tick or a first
  -- authorisation -- and the I-1 caller was the victim (reproduction D3).
  --
  -- The complete set of roots this cycle will touch is therefore locked ONCE,
  -- up front, through the same helper, which sorts them.  Every later per-row
  -- call then re-enters locks this transaction already holds.
  --
  -- The Candidate serial gate is deliberately NOT taken here.  proof/32
  -- section 6 step 1 makes the gate per Candidate and pins it to four job
  -- types: first authorisation, first-authorisation withdrawal, entitlement
  -- publication and pending release.  Finalisation is none of them -- it
  -- authorises nothing and publishes no entitlement -- and one finalisation
  -- spans many Candidates, so a single gate call cannot cover it and a gate per
  -- Candidate would invent a lock ladder the pack does not describe.
  -- proof/34 section 5 asks this path only for the family lock set and the
  -- resolver, which is exactly what it takes.
  select pg_catalog.array_agg(distinct contract_week.timesheet_id)
  into v_finalisation_root_ids
  from public.weekly_source_upload_rows source_row
  join public.weekly_source_row_resolutions resolution
    on resolution.upload_row_id=source_row.id and resolution.generation=v_generation
  join public.contract_weeks contract_week
    on contract_week.contract_id=resolution.contract_id
   and contract_week.additional_seq=0
   and contract_week.week_ending_date=source_row.work_date+
       ((coalesce((
          select contract.week_ending_weekday_snapshot
          from public.contracts contract where contract.id=resolution.contract_id
        ),0)-extract(dow from source_row.work_date)::integer+7)%7)
  where source_row.upload_id=v_upload.id
    and contract_week.timesheet_id is not null;

  if v_finalisation_root_ids is not null
     and pg_catalog.array_length(v_finalisation_root_ids,1) is not null then
    v_finalisation_lock_result:=private.weekly_source_lock_family_rows_v1(
      v_finalisation_root_ids,null
    );
    if coalesce((v_finalisation_lock_result->>'ok')::boolean,false) is not true then
      raise exception '%',v_finalisation_lock_result->>'code'
        using errcode='55000',detail=v_finalisation_lock_result::text;
    end if;
  end if;

  -- Review G2.  The set above was computed from an UNLOCKED read filtered on a
  -- non-null Contract Week Timesheet id, so a root another owner commits between
  -- that read and the loop would be locked later, in source-row order, out of
  -- the sorted order proof/32 section 6 step 2 fixes.  Recompute it under the
  -- family locks now held; if it grew, refuse retryably (40001) rather than take
  -- a second lock set out of order.  A retry recomputes the larger set up front.
  select pg_catalog.array_agg(distinct contract_week.timesheet_id)
  into v_finalisation_root_recheck
  from public.weekly_source_upload_rows source_row
  join public.weekly_source_row_resolutions resolution
    on resolution.upload_row_id=source_row.id and resolution.generation=v_generation
  join public.contract_weeks contract_week
    on contract_week.contract_id=resolution.contract_id
   and contract_week.additional_seq=0
   and contract_week.week_ending_date=source_row.work_date+
       ((coalesce((
          select contract.week_ending_weekday_snapshot
          from public.contracts contract where contract.id=resolution.contract_id
        ),0)-extract(dow from source_row.work_date)::integer+7)%7)
  where source_row.upload_id=v_upload.id
    and contract_week.timesheet_id is not null;

  if coalesce(v_finalisation_root_recheck,array[]::uuid[])
     <>coalesce(v_finalisation_root_ids,array[]::uuid[]) then
    raise exception 'WEEKLY_SOURCE_FINALISATION_ROOT_SET_CHANGED_DURING_LOCK'
      using errcode='40001',
        detail=pg_catalog.jsonb_build_object(
          'locked_root_count',
            coalesce(pg_catalog.array_length(v_finalisation_root_ids,1),0),
          'observed_root_count',
            coalesce(pg_catalog.array_length(v_finalisation_root_recheck,1),0)
        )::text;
  end if;

  -- Publish the locked set for the per-row calls.  The lineage ensure owner
  -- refuses 40001 when it discovers a root this set does not contain, so a root
  -- that appears after the recheck can never be locked out of order either.
  -- This is a Weekly-Source-private setting, never a Workbench-owned one.
  perform pg_catalog.set_config(
    'cloudtms.weekly_source_finalisation_root_set',
    coalesce(pg_catalog.array_to_string(v_finalisation_root_ids,','),''),
    true
  );

  -- The source Timesheet owner is the only component permitted to create or
  -- reuse ordinary Timesheet lineage.  Finalisation calls it, then validates
  -- the resulting immutable binding; it never accepts a caller-owned id.
  for v_row in
    select resolution.id as resolution_id,source_row.row_finalisation_state
    from public.weekly_source_upload_rows source_row
    join public.weekly_source_row_resolutions resolution
      on resolution.upload_row_id=source_row.id and resolution.generation=v_generation
    where source_row.upload_id=v_upload.id
      and (v_source_profile_kind='NHSP_TRUST_BACKING_REPORT'
        or source_row.row_finalisation_state='SOURCE_WORKED'
        or (v_source_profile_kind='GENERIC_COMPLETE_SNAPSHOT'
          and source_row.row_finalisation_state='NOT_APPLICABLE'))
    order by source_row.source_row_ordinal
  loop
    perform public.weekly_source_timesheet_lineage_ensure_atomic_v1(v_row.resolution_id,v_actor);
    v_economic_id:=private.weekly_source_finalisation_economic_assert_v1(
      v_row.resolution_id,v_cycle.id,v_source_mode
    );
    v_timesheet_id:=private.weekly_source_finalisation_lineage_assert_v1(
      v_row.resolution_id,v_cycle.id
    );
  end loop;

  if v_scope_kind='CYCLE' then
    select revision.id into v_prior_revision_id
    from public.weekly_source_final_revisions revision
    join public.weekly_source_cycles prior_cycle on prior_cycle.id=revision.source_cycle_id
    join public.weekly_source_client_manifests prior_manifest
      on prior_manifest.final_revision_id=revision.id and prior_manifest.client_id=v_client_id
    join public.weekly_source_uploads prior_upload on prior_upload.id=revision.upload_id
    join public.weekly_source_format_profiles prior_profile
      on prior_profile.id=prior_upload.source_format_profile_id
    where revision.authority_scope_kind='CYCLE' and revision.state='CURRENT'
      and prior_cycle.source_group_id=v_group.id and prior_cycle.id<>v_cycle.id
      and prior_cycle.finalisation_week_ending<v_cycle.finalisation_week_ending
      and prior_profile.final_authority_kind=v_source_profile_kind
    order by prior_cycle.finalisation_week_ending desc,revision.finalised_at_utc desc,revision.id desc
    limit 1;
  end if;

  select coalesce(pg_catalog.max(revision.revision_number),0)+1 into v_revision_number
  from public.weekly_source_final_revisions revision
  where revision.source_cycle_id=v_cycle.id
    and revision.authority_scope_kind=v_scope_kind
    and revision.report_scope_id is not distinct from v_report_scope_id;
  select private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_FINAL_POLICY_SET_V1',
    pg_catalog.jsonb_build_object(
      'profile_code',v_profile.profile_code,'profile_version',v_profile.version,
      'source_profile_kind',v_source_profile_kind,'source_mode',v_source_mode,
      'economics',coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
        'row_resolution_id',resolution.id,
        'contract_and_rate_fingerprint',case when economic.id is null then null
          else pg_catalog.encode(economic.contract_and_rate_fingerprint,'hex') end,
        'effective_policy_fingerprint',case when economic.id is null then null
          else pg_catalog.encode(economic.effective_policy_fingerprint,'hex') end,
        'invoice_vat_policy_fingerprint',case when economic.id is null then null
          else pg_catalog.encode(economic.invoice_vat_policy_fingerprint,'hex') end,
        'calculation_fingerprint',case when economic.id is null then null
          else pg_catalog.encode(economic.calculation_fingerprint,'hex') end,
        'expense_policy_snapshot_id',expense_policy.id,
        'expense_policy_snapshot_hash',case when expense_policy.id is null then null
          else pg_catalog.encode(expense_policy.snapshot_hash,'hex') end
      ) order by source_row.source_row_ordinal),'[]'::jsonb)
    )
  ) into v_policy_fingerprint
  from public.weekly_source_upload_rows source_row
  join public.weekly_source_row_resolutions resolution
    on resolution.upload_row_id=source_row.id and resolution.generation=v_generation
  left join public.weekly_source_row_economic_snapshots economic
    on economic.row_resolution_id=resolution.id
  left join public.weekly_source_row_expense_policy_snapshots expense_policy
    on expense_policy.row_resolution_id=resolution.id
  where source_row.upload_id=v_upload.id;
  v_revision_manifest_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_FINAL_REVISION_MANIFEST_V1',
    pg_catalog.jsonb_build_object(
      'source_cycle_id',v_cycle.id,'authority_scope_kind',v_scope_kind,
      'report_scope_id',v_report_scope_id,'revision_number',v_revision_number,
      'upload_id',v_upload.id,'authority_scope_version',v_scope_version,
      'row_manifest_hash',pg_catalog.encode(v_expected_row_hash,'hex'),
      'comparison_manifest_hash',pg_catalog.encode(v_expected_comparison_hash,'hex'),
      'issue_set_hash',pg_catalog.encode(v_expected_issue_hash,'hex'),
      'profile_code',v_profile.profile_code,'profile_version',v_profile.version,
      'coverage_start_local_date',v_coverage_start,'coverage_end_local_date',v_coverage_end,
      'reason',case when p_correction_session_id is null then 'INITIAL_FINALISATION'
        else 'CORRECT_FINAL_SOURCE' end,
      'predecessor_revision_id',case when p_correction_session_id is null then null
        else v_correction.expected_current_final_revision_id end,
      'prior_state_cutoff_revision_id',v_prior_revision_id,
      'policy_fingerprint',pg_catalog.encode(v_policy_fingerprint,'hex')
    )
  );
  insert into public.weekly_source_final_revisions(
    source_cycle_id,authority_scope_kind,report_scope_id,revision_number,upload_id,
    predecessor_revision_id,coverage_start_local_date,coverage_end_local_date,
    coverage_timezone,prior_state_cutoff_revision_id,reason,finalised_by_user_id,
    finalised_at_utc,manifest_hash,policy_fingerprint,state
  ) values (
    v_cycle.id,v_scope_kind,v_report_scope_id,v_revision_number,v_upload.id,
    case when p_correction_session_id is null then null
      else v_correction.expected_current_final_revision_id end,
    v_coverage_start,v_coverage_end,
    case when v_scope_kind='CYCLE' then 'Europe/London' else null end,
    v_prior_revision_id,
    case when p_correction_session_id is null then 'INITIAL_FINALISATION'
      else 'CORRECT_FINAL_SOURCE' end,
    v_actor,v_finalised_at,
    v_revision_manifest_hash,v_policy_fingerprint,
    case when p_prepare_only then 'PREPARED' else 'CURRENT' end
  ) returning id into v_revision_id;

  if v_source_profile_kind in ('GENERIC_COMPLETE_SNAPSHOT','HEALTHROSTER_ACTUAL_ROWS') then
    for v_row in
      select source_row.*,resolution.id as resolution_id,
             resolution.work_event_id,resolution.candidate_id,resolution.client_id,
             resolution.contract_id,resolution.work_event_match_kind,
             resolution.work_event_match_fingerprint
      from public.weekly_source_upload_rows source_row
      join public.weekly_source_row_resolutions resolution
        on resolution.upload_row_id=source_row.id and resolution.generation=v_generation
      where source_row.upload_id=v_upload.id
        and (source_row.row_finalisation_state='SOURCE_WORKED'
          or (v_source_profile_kind='GENERIC_COMPLETE_SNAPSHOT'
            and source_row.row_finalisation_state='NOT_APPLICABLE'))
      order by source_row.source_row_ordinal
    loop
      v_economic_id:=private.weekly_source_finalisation_economic_assert_v1(
        v_row.resolution_id,v_cycle.id,'HEALTHROSTER_WEEKLY'
      );
      v_timesheet_id:=private.weekly_source_finalisation_lineage_assert_v1(
        v_row.resolution_id,v_cycle.id
      );
      select * into strict v_economic
      from public.weekly_source_row_economic_snapshots where id=v_economic_id;
      v_pay_vector:=private.weekly_source_finalisation_vector_v1(v_economic.id,'PAY',1::smallint);
      v_charge_vector:=private.weekly_source_finalisation_vector_v1(v_economic.id,'CHARGE',1::smallint);
      v_mapping_fingerprint:=private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_SOURCE_FINAL_MAPPING_V1',
        pg_catalog.jsonb_build_object(
          'work_event_id',v_row.work_event_id,'candidate_id',v_row.candidate_id,
          'client_id',v_row.client_id,'contract_id',v_row.contract_id
        )
      );
      v_snapshot_hash:=private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_SOURCE_FINAL_SNAPSHOT_LINE_V1',
        pg_catalog.jsonb_build_object(
          'final_revision_id',v_revision_id,'upload_row_id',v_row.id,
          'row_resolution_id',v_row.resolution_id,'work_event_id',v_row.work_event_id,
          'candidate_id',v_row.candidate_id,'client_id',v_row.client_id,
          'contract_id',v_row.contract_id,'work_date',v_row.work_date,
          'start_at_local',v_row.start_at_local,'end_at_local',v_row.end_at_local,
          'break_minutes',v_row.break_minutes,'actual_net_minutes',v_row.actual_net_minutes,
          'pay_vector',v_pay_vector,'charge_vector',v_charge_vector,
          'source_expense_pence',v_row.source_expense_pence,
          'source_fingerprint',pg_catalog.encode(v_row.normalised_row_hash,'hex'),
          'mapping_fingerprint',pg_catalog.encode(v_mapping_fingerprint,'hex'),
          'rate_fingerprint',pg_catalog.encode(v_economic.calculation_fingerprint,'hex'),
          'policy_fingerprint',pg_catalog.encode(v_economic.effective_policy_fingerprint,'hex')
        )
      );
      insert into public.weekly_source_final_snapshot_lines(
        final_revision_id,upload_row_id,row_resolution_id,charge_check_id,
        source_profile_kind,work_event_id,external_event_identity,candidate_id,
        client_id,contract_id,work_date,start_at_local,end_at_local,break_minutes,
        actual_net_minutes,source_classifications_json,pay_vector_json,charge_vector_json,
        source_expense_pence,source_fingerprint,mapping_fingerprint,rate_fingerprint,
        policy_fingerprint,snapshot_line_hash
      ) values (
        v_revision_id,v_row.id,v_row.resolution_id,null,v_source_profile_kind,
        v_row.work_event_id,v_row.external_source_key,v_row.candidate_id,v_row.client_id,
        v_row.contract_id,v_row.work_date,v_row.start_at_local,v_row.end_at_local,
        v_row.break_minutes,v_row.actual_net_minutes,v_economic.canonical_result_json,
        v_pay_vector,v_charge_vector,v_row.source_expense_pence,v_row.normalised_row_hash,
        v_mapping_fingerprint,v_economic.calculation_fingerprint,
        v_economic.effective_policy_fingerprint,v_snapshot_hash
      ) returning id into v_snapshot_id;
      v_snapshot_count:=v_snapshot_count+1;
    end loop;

    for v_transition in
      with prior_states as (
        select distinct on (prior_transition.work_event_id)
               prior_transition.work_event_id,
               prior_transition.new_snapshot_line_id
        from public.weekly_source_state_transitions prior_transition
        join public.weekly_source_final_revisions prior_revision
          on prior_revision.id=prior_transition.final_revision_id
         and prior_revision.state='CURRENT'
        join public.weekly_source_cycles prior_cycle
          on prior_cycle.id=prior_transition.finalisation_cycle_id
        join public.weekly_source_client_manifests prior_manifest
          on prior_manifest.final_revision_id=prior_revision.id
         and prior_manifest.client_id=v_client_id
        where prior_transition.source_profile_kind=v_source_profile_kind
          and prior_cycle.source_group_id=v_group.id
          and prior_cycle.finalisation_week_ending<v_cycle.finalisation_week_ending
        order by prior_transition.work_event_id,
                 prior_cycle.finalisation_week_ending desc,
                 prior_revision.finalised_at_utc desc,
                 prior_revision.revision_number desc,
                 prior_transition.created_at_utc desc,
                 prior_transition.id desc
      ), prior_lines as (
        select prior_line.id,prior_state.work_event_id
        from prior_states prior_state
        join public.weekly_source_final_snapshot_lines prior_line
          on prior_line.id=prior_state.new_snapshot_line_id
        where prior_line.client_id=v_client_id
          and (
            prior_line.work_date between v_coverage_start and v_coverage_end
            or exists(
              select 1
              from public.weekly_source_row_resolutions current_resolution
              join public.weekly_source_upload_rows current_row
                on current_row.id=current_resolution.upload_row_id
              where current_row.upload_id=v_upload.id
                and current_resolution.generation=v_generation
                and current_resolution.work_event_id=prior_line.work_event_id
            )
          )
      ), current_lines as (
        select current_line.id,current_line.work_event_id
        from public.weekly_source_final_snapshot_lines current_line
        where current_line.final_revision_id=v_revision_id
      )
      select coalesce(prior_lines.work_event_id,current_lines.work_event_id) as work_event_id,
             prior_lines.id as prior_snapshot_id,current_lines.id as current_snapshot_id
      from prior_lines full join current_lines using(work_event_id)
      order by coalesce(prior_lines.work_event_id,current_lines.work_event_id)
    loop
      select * into v_prior_snapshot
      from public.weekly_source_final_snapshot_lines
      where id=v_transition.prior_snapshot_id;
      select * into v_current_snapshot
      from public.weekly_source_final_snapshot_lines
      where id=v_transition.current_snapshot_id;
      v_effective_policy:=private._weekly_source_effective_policy_v1(
        coalesce(v_current_snapshot.client_id,v_prior_snapshot.client_id),
        coalesce(v_current_snapshot.contract_id,v_prior_snapshot.contract_id),
        coalesce(v_current_snapshot.work_date,v_prior_snapshot.work_date)
      );
      if (v_effective_policy->>'authority_mode') is distinct from 'SOURCE_AUTHORITY'
         or coalesce((v_effective_policy->>'self_bill_enabled')::boolean,false) is not true
         or (v_effective_policy->>'source_group_id')::uuid is distinct from v_group.id
         or private.weekly_source_finalisation_hex32_v1(
              v_effective_policy->>'policy_sha256','WEEKLY_SOURCE_POLICY_FINGERPRINT_INVALID'
            ) is distinct from coalesce(
              v_current_snapshot.policy_fingerprint,v_prior_snapshot.policy_fingerprint
            ) then
        raise exception 'WEEKLY_SOURCE_FINALISATION_POLICY_STALE' using errcode='40001';
      end if;
      v_correction_presentation:=pg_catalog.upper(pg_catalog.btrim(coalesce(
        v_effective_policy->>'self_bill_correction_presentation',''
      )));
      if v_correction_presentation not in (
        'FULL_REVERSAL_REPLACEMENT','NET_DIFFERENCE_PRESENTATION'
      ) then
        raise exception 'WEEKLY_SOURCE_SELF_BILL_CORRECTION_POLICY_REQUIRED'
          using errcode='55000';
      end if;
      v_prior_state:=case when v_transition.prior_snapshot_id is null then null else
        private.weekly_source_final_state_fingerprint_v1(pg_catalog.jsonb_build_object(
          'work_event_id',v_prior_snapshot.work_event_id,
          'candidate_id',v_prior_snapshot.candidate_id,'client_id',v_prior_snapshot.client_id,
          'contract_id',v_prior_snapshot.contract_id,'work_date',v_prior_snapshot.work_date,
          'start_at_local',v_prior_snapshot.start_at_local,'end_at_local',v_prior_snapshot.end_at_local,
          'break_minutes',v_prior_snapshot.break_minutes,
          'actual_net_minutes',v_prior_snapshot.actual_net_minutes,
          'pay_vector',v_prior_snapshot.pay_vector_json,
          'charge_vector',v_prior_snapshot.charge_vector_json,
          'mapping_fingerprint',pg_catalog.encode(v_prior_snapshot.mapping_fingerprint,'hex'),
          'rate_fingerprint',pg_catalog.encode(v_prior_snapshot.rate_fingerprint,'hex'),
          'policy_fingerprint',pg_catalog.encode(v_prior_snapshot.policy_fingerprint,'hex')
        )) end;
      v_current_state:=case when v_transition.current_snapshot_id is null then null else
        private.weekly_source_final_state_fingerprint_v1(pg_catalog.jsonb_build_object(
          'work_event_id',v_current_snapshot.work_event_id,
          'candidate_id',v_current_snapshot.candidate_id,'client_id',v_current_snapshot.client_id,
          'contract_id',v_current_snapshot.contract_id,'work_date',v_current_snapshot.work_date,
          'start_at_local',v_current_snapshot.start_at_local,'end_at_local',v_current_snapshot.end_at_local,
          'break_minutes',v_current_snapshot.break_minutes,
          'actual_net_minutes',v_current_snapshot.actual_net_minutes,
          'pay_vector',v_current_snapshot.pay_vector_json,
          'charge_vector',v_current_snapshot.charge_vector_json,
          'mapping_fingerprint',pg_catalog.encode(v_current_snapshot.mapping_fingerprint,'hex'),
          'rate_fingerprint',pg_catalog.encode(v_current_snapshot.rate_fingerprint,'hex'),
          'policy_fingerprint',pg_catalog.encode(v_current_snapshot.policy_fingerprint,'hex')
        )) end;
      v_outcome:=case
        when v_transition.prior_snapshot_id is null then 'ADD'
        when v_transition.current_snapshot_id is null then 'CANCEL'
        when v_prior_state=v_current_state then 'NO_CHANGE'
        else 'AMEND' end;
      v_transition_fingerprint:=private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_SOURCE_STATE_TRANSITION_V1',
        pg_catalog.jsonb_build_object(
          'final_revision_id',v_revision_id,'work_event_id',v_transition.work_event_id,
          'previous_snapshot_line_id',v_transition.prior_snapshot_id,
          'new_snapshot_line_id',v_transition.current_snapshot_id,
          'outcome',v_outcome,
          'prior_state_fingerprint',case when v_prior_state is null then null
            else pg_catalog.encode(v_prior_state,'hex') end,
          'new_state_fingerprint',case when v_current_state is null then null
            else pg_catalog.encode(v_current_state,'hex') end
        )
      );
      insert into public.weekly_source_state_transitions(
        final_revision_id,finalisation_cycle_id,source_profile_kind,work_event_id,
        previous_present,new_present,previous_snapshot_line_id,new_snapshot_line_id,
        outcome,identity_match_kind,match_confidence,prior_state_fingerprint,
        new_state_fingerprint,transition_fingerprint,
        ordinary_source_entitlement_projection_state
      ) values (
        v_revision_id,v_cycle.id,v_source_profile_kind,v_transition.work_event_id,
        v_transition.prior_snapshot_id is not null,v_transition.current_snapshot_id is not null,
        v_transition.prior_snapshot_id,v_transition.current_snapshot_id,v_outcome,
        (select event.identity_kind from public.weekly_work_events event
          where event.id=v_transition.work_event_id),
        case when (select event.identity_kind from public.weekly_work_events event
          where event.id=v_transition.work_event_id)='PROFILE_EXTERNAL_KEY'
          then 'EXACT_PROFILE_KEY' else 'EXACT_DURABLE_LINEAGE' end,
        v_prior_state,v_current_state,v_transition_fingerprint,
        case when v_outcome='NO_CHANGE' then 'NOT_APPLICABLE' else 'PENDING' end
      ) returning id into v_transition_id;
      v_transition_count:=v_transition_count+1;

      if v_outcome='NO_CHANGE' then
        v_unchanged_count:=v_unchanged_count+1;
      else
        if v_outcome in ('AMEND','CANCEL') then
          select movement.* into v_prior_movement
          from public.weekly_source_billing_movements movement
          join public.weekly_source_final_revisions movement_revision
            on movement_revision.id=movement.final_revision_id
          join public.weekly_source_cycles movement_cycle
            on movement_cycle.id=movement.finalisation_cycle_id
          where movement.work_event_id=v_transition.work_event_id
            and movement.source_profile_kind=v_source_profile_kind
            and movement.movement_role in ('POSITIVE','REPLACEMENT')
            and movement.final_revision_id<>v_revision_id
            and movement_cycle.finalisation_week_ending<v_cycle.finalisation_week_ending
          order by movement_cycle.finalisation_week_ending desc,
                   movement_revision.finalised_at_utc desc,movement.created_at_utc desc,movement.id desc
          limit 1;
          if not found then
            raise exception 'WEEKLY_SOURCE_PRIOR_MOVEMENT_MISSING' using errcode='55000';
          end if;
          v_correction_unit_id:=pg_catalog.gen_random_uuid();
          v_movement_id:=private.weekly_source_finalisation_insert_reversal_v1(
            v_transition_id,v_revision_id,v_cycle.id,v_prior_movement.id,
            v_correction_unit_id,v_source_profile_kind,
            pg_catalog.jsonb_build_object(
              'schema_version','WEEKLY_SOURCE_GENERATED_REVERSAL_FACTS_V1',
              'outcome',v_outcome,'prior_snapshot_line_id',v_transition.prior_snapshot_id,
              'prior_movement_id',v_prior_movement.id,
              'correction_presentation',v_correction_presentation
            )
          );
          v_movement_count:=v_movement_count+1;
        else
          v_correction_unit_id:=null;
          v_prior_movement.id:=null;
        end if;
        if v_outcome in ('ADD','AMEND') then
          select resolution.id as resolution_id into strict v_row
          from public.weekly_source_row_resolutions resolution
          where resolution.id=v_current_snapshot.row_resolution_id;
          v_economic_id:=private.weekly_source_finalisation_economic_assert_v1(
            v_row.resolution_id,v_cycle.id,'HEALTHROSTER_WEEKLY'
          );
          v_timesheet_id:=private.weekly_source_finalisation_lineage_assert_v1(
            v_row.resolution_id,v_cycle.id
          );
          select * into strict v_economic
          from public.weekly_source_row_economic_snapshots where id=v_economic_id;
          v_movement_id:=private.weekly_source_finalisation_insert_current_movement_v1(
            v_transition_id,null,v_revision_id,v_cycle.id,v_source_profile_kind,
            case when v_outcome='ADD' then 'POSITIVE' else 'REPLACEMENT' end,
            case when v_outcome='ADD' then 'SOURCE_ORDINARY' else 'SOURCE_REPLACEMENT' end,
            v_row.resolution_id,v_economic.id,v_timesheet_id,null,
            v_economic.calculated_charge_pence,'NOT_APPLICABLE',null,
            null,
            pg_catalog.jsonb_build_object(
              'schema_version','WEEKLY_SOURCE_CURRENT_FACTS_V1','outcome',v_outcome,
              'snapshot_line_id',v_current_snapshot.id,
              'upload_row_id',v_current_snapshot.upload_row_id,
              'row_resolution_id',v_current_snapshot.row_resolution_id,
              'correction_presentation',v_correction_presentation
            ),v_correction_unit_id,
            case when v_outcome='AMEND' then v_prior_movement.id else null end
          );
          v_movement_count:=v_movement_count+1;
        end if;
      end if;

    end loop;

    if v_profile.profile_code='ROSTER_WEEKLY_SUMMARY_ACTUAL_V1' then
      -- The profile can serve Clients with or without source-fixed expenses.
      -- A snapshot is mandatory exactly where the sealed effective policy
      -- enables that route, and forbidden otherwise.  This prevents both a
      -- missing configured expense and accidental contamination of ordinary
      -- evidence-led expenses.
      for v_row in
        select source_row.work_date,resolution.id as resolution_id,
               resolution.client_id,resolution.contract_id,
               resolution.effective_policy_fingerprint,
               expense_policy.id as expense_policy_snapshot_id
        from public.weekly_source_upload_rows source_row
        join public.weekly_source_row_resolutions resolution
          on resolution.upload_row_id=source_row.id and resolution.generation=v_generation
        left join public.weekly_source_row_expense_policy_snapshots expense_policy
          on expense_policy.row_resolution_id=resolution.id
        where source_row.upload_id=v_upload.id
        order by source_row.source_row_ordinal
      loop
        v_effective_policy:=private._weekly_source_effective_policy_v1(
          v_row.client_id,v_row.contract_id,v_row.work_date
        );
        if (v_effective_policy->>'authority_mode') is distinct from 'SOURCE_AUTHORITY'
           or coalesce((v_effective_policy->>'self_bill_enabled')::boolean,false) is not true
           or (v_effective_policy->>'source_group_id')::uuid is distinct from v_group.id
           or private.weekly_source_finalisation_hex32_v1(
                v_effective_policy->>'policy_sha256','WEEKLY_SOURCE_POLICY_FINGERPRINT_INVALID'
              ) is distinct from v_row.effective_policy_fingerprint then
          raise exception 'WEEKLY_SOURCE_FINALISATION_POLICY_STALE' using errcode='40001';
        end if;
        if coalesce((v_effective_policy->>'source_fixed_expenses_enabled')::boolean,false) then
          if v_row.expense_policy_snapshot_id is null
             or private.weekly_source_finalisation_expense_policy_assert_v1(
                  v_row.resolution_id,v_cycle.id
                ) is distinct from v_row.expense_policy_snapshot_id then
            raise exception 'WEEKLY_SOURCE_EXPENSE_POLICY_SNAPSHOT_CARDINALITY'
              using errcode='55000';
          end if;
        elsif v_row.expense_policy_snapshot_id is not null then
          raise exception 'WEEKLY_SOURCE_EXPENSE_POLICY_NOT_ENABLED' using errcode='55000';
        end if;
      end loop;

      for v_expense_observation in
        with current_observation as (
          select expense_policy.id as expense_policy_snapshot_id,
                 expense_policy.row_resolution_id,expense_policy.work_event_id,
                 expense_policy.contract_id,expense_policy.source_expense_pence,
                 expense_policy.source_expense_vat_enabled,
                 expense_policy.correction_presentation
          from public.weekly_source_row_expense_policy_snapshots expense_policy
          join public.weekly_source_row_resolutions resolution
            on resolution.id=expense_policy.row_resolution_id
          join public.weekly_source_upload_rows source_row
            on source_row.id=resolution.upload_row_id
          where source_row.upload_id=v_upload.id and resolution.generation=v_generation
        ), prior_authority as (
          select
                 case when p_prepare_only
                           and authority.final_revision_id=v_correction.expected_current_final_revision_id
                        then baseline.id else authority.id end as prior_authority_id,
                 authority.work_event_id,
                 case when p_prepare_only
                           and authority.final_revision_id=v_correction.expected_current_final_revision_id
                        then baseline.contract_id else authority.contract_id end as contract_id,
                 case when p_prepare_only
                           and authority.final_revision_id=v_correction.expected_current_final_revision_id
                        then baseline.source_expense_pence else authority.source_expense_pence end
                   as source_expense_pence,
                 case when p_prepare_only
                           and authority.final_revision_id=v_correction.expected_current_final_revision_id
                        then baseline.source_expense_vat_enabled
                        else authority.source_expense_vat_enabled end
                   as source_expense_vat_enabled,
                 case when p_prepare_only
                           and authority.final_revision_id=v_correction.expected_current_final_revision_id
                        then baseline.correction_presentation
                        else authority.correction_presentation end
                   as correction_presentation,
                 event.work_date
          from public.weekly_expense_authority_generations authority
          join public.weekly_work_events event on event.id=authority.work_event_id
          left join public.weekly_expense_authority_generations baseline
            on p_prepare_only
           and authority.final_revision_id=v_correction.expected_current_final_revision_id
           and baseline.id=authority.prior_expense_authority_generation_id
          where authority.state='CURRENT'
            and event.first_source_group_id=v_group.id
            and event.client_id=v_client_id
            and not (
              p_prepare_only
              and authority.final_revision_id=v_correction.expected_current_final_revision_id
              and baseline.id is null
            )
            and (
              event.work_date between v_coverage_start and v_coverage_end
              or exists(
                select 1 from current_observation current_match
                where current_match.work_event_id=authority.work_event_id
              )
            )
        )
        select coalesce(current_observation.work_event_id,prior_authority.work_event_id) as work_event_id,
               current_observation.expense_policy_snapshot_id,
               current_observation.row_resolution_id,
               current_observation.contract_id as current_contract_id,
               current_observation.source_expense_pence as current_expense_pence,
               current_observation.source_expense_vat_enabled as current_expense_vat_enabled,
               current_observation.correction_presentation as current_correction_presentation,
               prior_authority.prior_authority_id,
               prior_authority.contract_id as prior_contract_id,
               prior_authority.source_expense_pence as prior_expense_pence,
               prior_authority.source_expense_vat_enabled as prior_expense_vat_enabled,
               prior_authority.correction_presentation as prior_correction_presentation
        from current_observation
        full join prior_authority using(work_event_id)
        order by coalesce(current_observation.work_event_id,prior_authority.work_event_id)
      loop
        v_expense_policy_id:=v_expense_observation.expense_policy_snapshot_id;
        v_prior_expense_authority_id:=v_expense_observation.prior_authority_id;
        v_expense_authority_id:=null;
        v_expense_timesheet_id:=null;
        v_current_expense.id:=null;
        if v_expense_policy_id is not null then
          if private.weekly_source_finalisation_expense_policy_assert_v1(
               v_expense_observation.row_resolution_id,v_cycle.id
             ) is distinct from v_expense_policy_id then
            raise exception 'WEEKLY_SOURCE_EXPENSE_POLICY_SNAPSHOT_INVALID' using errcode='55000';
          end if;
          select * into strict v_expense_policy
          from public.weekly_source_row_expense_policy_snapshots
          where id=v_expense_policy_id;
          v_target_expense_pence:=v_expense_policy.source_expense_pence;
          v_target_expense_vat:=v_expense_policy.source_expense_vat_enabled;
          v_target_expense_contract_id:=v_expense_policy.contract_id;
          v_expense_observation_kind:='ROW_PRESENT';
          v_correction_presentation:=v_expense_policy.correction_presentation;
          if v_target_expense_pence>0 then
            perform public.weekly_source_timesheet_lineage_ensure_atomic_v1(
              v_expense_policy.row_resolution_id,v_actor
            );
            v_expense_timesheet_id:=private.weekly_source_finalisation_lineage_assert_v1(
              v_expense_policy.row_resolution_id,v_cycle.id
            );
          end if;
        else
          v_target_expense_pence:=0;
          v_target_expense_vat:=v_expense_observation.prior_expense_vat_enabled;
          v_target_expense_contract_id:=v_expense_observation.prior_contract_id;
          v_expense_observation_kind:='OMITTED_IN_COMPLETE_COVERAGE';
          v_correction_presentation:=v_expense_observation.prior_correction_presentation;
        end if;
        if v_prior_expense_authority_id is not null then
          select * into strict v_current_expense
          from public.weekly_expense_authority_generations expense
          where expense.id=v_prior_expense_authority_id
            and expense.state in ('CURRENT','SUPERSEDED')
            and expense.work_event_id=v_expense_observation.work_event_id
          for update;
        end if;
        if v_prior_expense_authority_id is null
           or v_current_expense.source_expense_pence is distinct from v_target_expense_pence
           or v_current_expense.source_expense_vat_enabled is distinct from v_target_expense_vat
           or v_current_expense.contract_id is distinct from v_target_expense_contract_id
           or v_current_expense.source_observation_kind is distinct from v_expense_observation_kind then
          if v_prior_expense_authority_id is not null then
            if not p_prepare_only then
              update public.weekly_expense_authority_generations
              set state='SUPERSEDED' where id=v_prior_expense_authority_id;
            end if;
          end if;
          select coalesce(pg_catalog.max(expense.generation),0)+1 into v_expense_generation
          from public.weekly_expense_authority_generations expense
          where expense.work_event_id=v_expense_observation.work_event_id;
          v_expense_hash:=private.weekly_source_sha256_jsonb_v1(
            'WEEKLY_SOURCE_EXPENSE_AUTHORITY_GENERATION_V1',
            pg_catalog.jsonb_build_object(
              'final_revision_id',v_revision_id,
              'work_event_id',v_expense_observation.work_event_id,
              'contract_id',v_target_expense_contract_id,
              'row_expense_policy_snapshot_id',v_expense_policy_id,
              'prior_expense_authority_generation_id',v_prior_expense_authority_id,
              'generation',v_expense_generation,
              'source_observation_kind',v_expense_observation_kind,
              'correction_presentation',v_correction_presentation,
              'source_expense_pence',v_target_expense_pence,
              'source_expense_vat_enabled',v_target_expense_vat
            )
          );
          insert into public.weekly_expense_authority_generations(
            final_revision_id,work_event_id,contract_id,row_expense_policy_snapshot_id,
            prior_expense_authority_generation_id,generation,source_observation_kind,
            correction_presentation,source_expense_pence,source_expense_vat_enabled,
            candidate_reimbursement_ex_vat,client_charge_ex_vat,authority_hash,state
          ) values (
            v_revision_id,v_expense_observation.work_event_id,v_target_expense_contract_id,
            v_expense_policy_id,v_prior_expense_authority_id,v_expense_generation,
            v_expense_observation_kind,v_correction_presentation,v_target_expense_pence,
            v_target_expense_vat,v_target_expense_pence::numeric/100,
            v_target_expense_pence::numeric/100,v_expense_hash,
            case when p_prepare_only then 'PREPARED' else 'CURRENT' end
          ) returning id into v_expense_authority_id;
          v_expense_movement_result:=
            private.weekly_source_finalisation_materialise_expense_invoice_v1(
              v_revision_id,v_cycle.id,v_source_profile_kind,
              v_expense_authority_id,v_prior_expense_authority_id,
              v_expense_policy_id,v_expense_timesheet_id,
              v_correction_presentation
            );
          v_movement_count:=v_movement_count+
            coalesce((v_expense_movement_result->>'movement_count')::integer,0);
          v_expense_generation_count:=v_expense_generation_count+1;
        end if;
        v_current_expense.id:=null;
      end loop;
    end if;
  else
    v_source_total_cost:=0;
    v_source_commission:=0;
    v_source_invoice_total:=0;
    v_line_count:=0;
    for v_row in
      select source_row.*,resolution.id as resolution_id,
             resolution.work_event_id,resolution.candidate_id,resolution.client_id,
             resolution.contract_id
      from public.weekly_source_upload_rows source_row
      join public.weekly_source_row_resolutions resolution
        on resolution.upload_row_id=source_row.id and resolution.generation=v_generation
      where source_row.upload_id=v_upload.id
      order by source_row.source_row_ordinal
    loop
      v_economic_id:=private.weekly_source_finalisation_economic_assert_v1(
        v_row.resolution_id,v_cycle.id,'NHSP_WEEKLY'
      );
      v_timesheet_id:=private.weekly_source_finalisation_lineage_assert_v1(
        v_row.resolution_id,v_cycle.id
      );
      select * into strict v_economic
      from public.weekly_source_row_economic_snapshots where id=v_economic_id;
      select * into v_charge_check
      from public.weekly_source_charge_checks charge_check
      where charge_check.upload_row_id=v_row.id
        and charge_check.row_resolution_id=v_row.resolution_id
        and charge_check.generation=v_generation;
      v_charge_acceptance.id:=null;
      if found and v_charge_check.comparison_result in ('MISMATCH','ZERO_SOURCE_CHARGE') then
        select * into v_charge_acceptance
        from public.weekly_source_charge_acceptances acceptance
        where acceptance.charge_check_id=v_charge_check.id
          and acceptance.upload_row_id=v_row.id
          and acceptance.row_resolution_id=v_row.resolution_id
          and acceptance.source_upload_id=v_upload.id
          and acceptance.contract_id=v_row.contract_id
          and acceptance.acceptance_kind=case v_charge_check.comparison_result
            when 'ZERO_SOURCE_CHARGE' then 'ACCEPTED_ZERO' else 'ACCEPTED_DISPARITY' end
          and acceptance.source_upload_hash=v_upload.content_sha256
          and acceptance.source_row_fingerprint=(select resolution.source_row_fingerprint
            from public.weekly_source_row_resolutions resolution where resolution.id=v_row.resolution_id)
          and acceptance.contract_and_rate_fingerprint=v_economic.contract_and_rate_fingerprint
          and acceptance.effective_policy_fingerprint=v_economic.effective_policy_fingerprint
          and acceptance.charge_calculation_fingerprint=v_economic.calculation_fingerprint
          and acceptance.acceptance_policy_fingerprint=
            private.weekly_source_charge_acceptance_policy_fingerprint_v1();
      end if;
      v_price_admission_result:=case v_charge_check.comparison_result
        when 'ZERO_SOURCE_CHARGE' then 'ACCEPTED_ZERO'
        when 'MISMATCH' then 'ACCEPTED_DISPARITY'
        else v_charge_check.comparison_result end;
      if not found or (select pg_catalog.count(*)
          from public.weekly_source_charge_checks charge_check
          where charge_check.upload_row_id=v_row.id
            and charge_check.row_resolution_id=v_row.resolution_id
            and charge_check.generation=v_generation)<>1
         or v_charge_check.source_commission_pence is distinct from v_row.source_commission_pence
         or v_charge_check.source_total_cost_pence is distinct from v_row.source_total_cost_pence
         or v_charge_check.source_shift_charge_pence is distinct from v_row.source_shift_charge_pence
         or v_charge_check.calculated_segment_charge_pence is distinct from v_economic.calculated_charge_pence
         or v_charge_check.source_charge_difference_pence is distinct from
              (v_row.source_shift_charge_pence-v_economic.calculated_charge_pence)
         or v_charge_check.charge_calculation_fingerprint is distinct from
              v_economic.calculation_fingerprint
         or v_charge_check.comparison_profile_version<>'NHSP_TWO_COMPONENT_PENCE_V1'
         or v_charge_check.comparison_result not in (
              'EXACT','SOURCE_ROUNDING_EQUIVALENT','MISMATCH','ZERO_SOURCE_CHARGE'
            )
         or not (
           (v_charge_check.comparison_result='EXACT'
             and v_charge_check.source_charge_difference_pence=0)
           or (v_charge_check.comparison_result='SOURCE_ROUNDING_EQUIVALENT'
             and v_charge_check.row_sign_kind in ('POSITIVE','FULL_NEGATIVE')
             and (v_charge_check.source_charge_difference_pence=1
               or v_charge_check.source_charge_difference_pence=-1)
             and ((v_charge_check.source_shift_charge_pence>0
                   and v_charge_check.calculated_segment_charge_pence>0)
               or (v_charge_check.source_shift_charge_pence<0
                   and v_charge_check.calculated_segment_charge_pence<0)))
           or (v_charge_check.comparison_result='MISMATCH'
             and v_charge_acceptance.id is not null
             and v_charge_acceptance.acceptance_kind='ACCEPTED_DISPARITY')
           or (v_charge_check.comparison_result='ZERO_SOURCE_CHARGE'
             and v_charge_check.source_commission_pence=0
             and v_charge_check.source_total_cost_pence=0
             and v_charge_check.source_shift_charge_pence=0
             and v_charge_check.calculated_segment_charge_pence>0
             and v_charge_acceptance.id is not null
             and v_charge_acceptance.acceptance_kind='ACCEPTED_ZERO')
         )
         or v_charge_check.row_sign_kind is distinct from (case
              when v_economic.row_sign=1 then 'POSITIVE' else 'FULL_NEGATIVE' end)
         or v_charge_check.phase_severity='FINALISATION_BLOCKER'
         or v_charge_check.blocker_code is not null then
        raise exception 'WEEKLY_SOURCE_NHSP_PRICE_GATE_FAILED'
          using errcode='55000',detail=v_row.source_row_ordinal::text;
      end if;
      v_movement_id:=private.weekly_source_finalisation_insert_current_movement_v1(
        null,v_row.id,v_revision_id,v_cycle.id,'NHSP_TRUST_BACKING_REPORT',
        case when v_economic.row_sign=1 then 'POSITIVE' else 'REVERSAL' end,
        case when v_economic.row_sign=1
          then 'NHSP_PHYSICAL_POSITIVE' else 'NHSP_PHYSICAL_FULL_NEGATIVE' end,
        v_row.resolution_id,v_economic.id,v_timesheet_id,
        v_row.source_shift_charge_pence,v_row.source_shift_charge_pence,
        v_price_admission_result,v_charge_check.charge_calculation_fingerprint,
        v_charge_acceptance.id,
        pg_catalog.jsonb_build_object(
          'schema_version','WEEKLY_SOURCE_NHSP_PHYSICAL_FACTS_V1',
          'backing_report_number',v_backing_report_number,
          'report_scope_id',v_report_scope_id,'source_row_ordinal',v_row.source_row_ordinal,
          'upload_row_id',v_row.id,'row_resolution_id',v_row.resolution_id,
          'work_date',v_row.work_date,'start_at_local',v_row.start_at_local,
          'end_at_local',v_row.end_at_local,'break_minutes',v_row.break_minutes,
          'actual_net_minutes',v_row.actual_net_minutes,
          'source_commission_pence',v_row.source_commission_pence,
          'source_total_cost_pence',v_row.source_total_cost_pence,
          'source_shift_charge_pence',v_row.source_shift_charge_pence,
          'calculated_comparison_charge_pence',v_economic.calculated_charge_pence,
          'comparison_result',v_charge_check.comparison_result,
          'price_admission_result',v_price_admission_result,
          'charge_acceptance_id',v_charge_acceptance.id,
          'physical_sign',case when v_economic.row_sign=1
            then 'POSITIVE' else 'FULL_NEGATIVE' end
        ),null,null
      );
      v_source_total_cost:=v_source_total_cost+v_row.source_total_cost_pence;
      v_source_commission:=v_source_commission+v_row.source_commission_pence;
      v_source_invoice_total:=v_source_invoice_total+v_row.source_shift_charge_pence;
      v_line_count:=v_line_count+1;
      v_movement_count:=v_movement_count+1;
    end loop;
    if v_line_count<>v_upload.accepted_count
       or v_source_invoice_total<>v_source_total_cost+v_source_commission then
      raise exception 'WEEKLY_SOURCE_NHSP_REPORT_TOTAL_INVALID' using errcode='55000';
    end if;
    insert into public.weekly_source_nhsp_backing_reports(
      report_scope_id,final_revision_id,upload_id,client_id,backing_report_number,
      cutoff_at_utc,physical_line_count,source_total_cost_pence,
      source_commission_pence,source_invoice_total_pence,row_manifest_hash
    ) values (
      v_report_scope_id,v_revision_id,v_upload.id,v_client_id,v_backing_report_number,
      v_scope.cutoff_at_utc,v_line_count,v_source_total_cost,v_source_commission,
      v_source_invoice_total,v_upload.row_manifest_hash
    );
  end if;

  for v_manifest in
    select client_id
    from (
      select movement.actual_client_id as client_id
      from public.weekly_source_billing_movements movement
      where movement.final_revision_id=v_revision_id
      union
      select v_client_id
    ) clients
    where client_id is not null
    order by client_id
  loop
    select private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_CLIENT_MANIFEST_V1',
      pg_catalog.jsonb_build_object(
        'final_revision_id',v_revision_id,'source_group_id',v_group.id,
        'source_cycle_id',v_cycle.id,'client_id',v_manifest.client_id,
        'finalisation_week_ending',v_cycle.finalisation_week_ending,
        'backing_report_number',v_backing_report_number,
        'movements',coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
          'movement_id',movement.id,
          'movement_economic_hash',pg_catalog.encode(movement.movement_economic_hash,'hex')
        ) order by pg_catalog.encode(movement.movement_economic_hash,'hex')),
        '[]'::jsonb),
        'expense_authorities',coalesce((
          select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
            'expense_authority_generation_id',expense.id,
            'authority_hash',pg_catalog.encode(expense.authority_hash,'hex')
          ) order by pg_catalog.encode(expense.authority_hash,'hex'))
          from public.weekly_expense_authority_generations expense
          join public.weekly_work_events event on event.id=expense.work_event_id
          where expense.final_revision_id=v_revision_id and event.client_id=v_manifest.client_id
        ),'[]'::jsonb)
      )
    ) into v_client_manifest_hash
    from public.weekly_source_billing_movements movement
    where movement.final_revision_id=v_revision_id
      and movement.actual_client_id=v_manifest.client_id;
    insert into public.weekly_source_client_manifests(
      final_revision_id,source_group_id,source_cycle_id,client_id,
      finalisation_week_ending,backing_report_number,manifest_hash,movement_count,
      unchanged_snapshot_count,expense_line_count,invoice_state
    ) values (
      v_revision_id,v_group.id,v_cycle.id,v_manifest.client_id,
      v_cycle.finalisation_week_ending,v_backing_report_number,v_client_manifest_hash,
      (select pg_catalog.count(*)::integer
       from public.weekly_source_billing_movements movement
       where movement.final_revision_id=v_revision_id
         and movement.actual_client_id=v_manifest.client_id),
      (select pg_catalog.count(*)::integer
       from public.weekly_source_state_transitions transition_row
       join public.weekly_source_final_snapshot_lines snapshot_line
         on snapshot_line.id=transition_row.new_snapshot_line_id
       where transition_row.final_revision_id=v_revision_id
         and transition_row.outcome='NO_CHANGE'
         and snapshot_line.client_id=v_manifest.client_id),
      (select pg_catalog.count(*)::integer
       from public.weekly_expense_authority_generations expense
       join public.weekly_work_events event on event.id=expense.work_event_id
       where expense.final_revision_id=v_revision_id
         and event.client_id=v_manifest.client_id),
      'READY'
    ) returning id into v_client_manifest_id;
    insert into public.weekly_source_manifest_movements(
      client_manifest_id,billing_movement_id,manifest_ordinal,movement_hash
    )
    select v_client_manifest_id,movement.id,
           pg_catalog.row_number() over(
             order by pg_catalog.encode(movement.movement_economic_hash,'hex')
           )::integer,
           movement.movement_economic_hash
    from public.weekly_source_billing_movements movement
    where movement.final_revision_id=v_revision_id
      and movement.actual_client_id=v_manifest.client_id;
  end loop;

  if p_prepare_only then
    return pg_catalog.jsonb_build_object(
      'ok',true,'status','PREPARED','idempotent',false,
      'final_revision_id',v_revision_id,'source_cycle_id',v_cycle.id,
      'authority_scope_kind',v_scope_kind,'report_scope_id',v_report_scope_id,
      'upload_id',v_upload.id,'profile_code',v_profile.profile_code,
      'client_id',v_client_id,'snapshot_count',v_snapshot_count,
      'transition_count',v_transition_count,'movement_count',v_movement_count,
      'unchanged_count',v_unchanged_count,
      'expense_generation_count',v_expense_generation_count
    );
  end if;

  update public.weekly_source_client_cycle_completions completion
  set state='SUPERSEDED',superseded_at_utc=v_finalised_at
  where completion.source_cycle_id=v_cycle.id
    and completion.client_id=v_client_id and completion.state='CURRENT';
  select coalesce(pg_catalog.max(completion.completion_generation),0)+1
  into v_completion_generation
  from public.weekly_source_client_cycle_completions completion
  where completion.source_cycle_id=v_cycle.id and completion.client_id=v_client_id;
  v_completion_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_CLIENT_CYCLE_COMPLETION_V1',
    pg_catalog.jsonb_build_object(
      'source_cycle_id',v_cycle.id,'source_group_id',v_group.id,
      'client_id',v_client_id,'completion_generation',v_completion_generation,
      'completion_kind','FINAL_SOURCE','final_revision_id',v_revision_id,
      'actor_user_id',v_actor
    )
  );
  insert into public.weekly_source_client_cycle_completions(
    source_cycle_id,source_group_id,client_id,completion_generation,
    completion_kind,final_revision_id,attested_by_user_id,attested_at_utc,
    attestation_text,completion_hash,state
  ) values (
    v_cycle.id,v_group.id,v_client_id,v_completion_generation,
    'FINAL_SOURCE',v_revision_id,v_actor,v_finalised_at,null,v_completion_hash,'CURRENT'
  );

  if v_scope_kind='CYCLE' then
    update public.weekly_source_cycles
    set current_final_revision_id=v_revision_id,state='FINALISED',
        finalised_at_utc=v_finalised_at,finalised_by_user_id=v_actor
    where id=v_cycle.id and current_complete_upload_id=v_upload.id
      and current_projection_publication_id=v_publication.id
      and version=v_scope_version and current_final_revision_id is null;
    if not found then
      raise exception 'WEEKLY_SOURCE_FINALISE_CAS_LOST' using errcode='40001';
    end if;
  else
    update public.weekly_source_report_scopes
    set current_final_revision_id=v_revision_id,state='FINALISED',
        updated_at_utc=v_finalised_at
    where id=v_scope.id and current_complete_upload_id=v_upload.id
      and current_projection_publication_id=v_publication.id
      and version=v_scope_version and current_final_revision_id is null;
    if not found then
      raise exception 'WEEKLY_SOURCE_FINALISE_CAS_LOST' using errcode='40001';
    end if;
    if not exists(
      select 1
      from public.weekly_source_group_clients membership
      where membership.source_group_id=v_group.id
        and v_cycle.finalisation_week_ending between membership.valid_from
          and coalesce(membership.valid_to,'infinity'::date)
        and not exists(
          select 1
          from public.weekly_source_client_cycle_completions completion
          where completion.source_cycle_id=v_cycle.id
            and completion.client_id=membership.client_id
            and completion.state='CURRENT'
        )
    ) then
      update public.weekly_source_cycles
      set state='FINALISED',finalised_at_utc=v_finalised_at,
          finalised_by_user_id=v_actor
      where id=v_cycle.id;
    else
      update public.weekly_source_cycles
      set state='FINALISABLE'
      where id=v_cycle.id and state in ('OPEN','FINALISABLE');
    end if;
  end if;

  if p_correction_session_id is null and exists(
    select 1
    from public.weekly_source_cycles cycle
    where cycle.id=v_cycle.id and cycle.state='FINALISED'
  ) then
    perform private._weekly_source_settings_ensure_open_cycle_v1(
      v_group.id,v_finalised_at
    );
  end if;

  return pg_catalog.jsonb_build_object(
    'ok',true,'status',case when p_correction_session_id is null
      then 'FINALISED' else 'CORRECTED' end,'idempotent',false,
    'final_revision_id',v_revision_id,'source_cycle_id',v_cycle.id,
    'authority_scope_kind',v_scope_kind,'report_scope_id',v_report_scope_id,
    'upload_id',v_upload.id,'profile_code',v_profile.profile_code,
    'client_id',v_client_id,'snapshot_count',v_snapshot_count,
    'transition_count',v_transition_count,'movement_count',v_movement_count,
    'unchanged_count',v_unchanged_count,
    'expense_generation_count',v_expense_generation_count
  );
exception
  when invalid_text_representation or numeric_value_out_of_range or datetime_field_overflow then
    raise exception 'WEEKLY_SOURCE_FINALISE_VALUE_INVALID' using errcode='22023';
end;
$function$;

create or replace function private.weekly_source_finalise_core_v1(
  p_request jsonb,
  p_correction_session_id uuid
) returns jsonb
language sql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
  select private.weekly_source_finalise_engine_v1(
    p_request,p_correction_session_id,false
  );
$function$;

create or replace function public.weekly_source_finalise_atomic_v1(
  p_request jsonb
) returns jsonb
language sql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
  select private.weekly_source_finalise_core_v1(p_request,null::uuid);
$function$;

alter function private.weekly_source_finalisation_hex32_v1(text,text) owner to postgres;
alter function private.weekly_source_finalisation_vector_v1(uuid,text,smallint) owner to postgres;
alter function private.weekly_source_finalisation_negate_vector_v1(jsonb) owner to postgres;
alter function private.weekly_source_finalisation_economic_assert_v1(uuid,uuid,text) owner to postgres;
alter function private.weekly_source_finalisation_expense_policy_assert_v1(uuid,uuid) owner to postgres;
alter function private.weekly_source_finalisation_lineage_assert_v1(uuid,uuid) owner to postgres;
alter function private.weekly_source_final_state_fingerprint_v1(jsonb) owner to postgres;
alter function private.weekly_source_finalisation_insert_current_movement_v1(
  uuid,uuid,uuid,uuid,text,text,text,uuid,uuid,uuid,bigint,bigint,text,bytea,uuid,jsonb,uuid,uuid
) owner to postgres;
alter function private.weekly_source_finalisation_insert_reversal_v1(
  uuid,uuid,uuid,uuid,uuid,text,jsonb
) owner to postgres;
alter function private.weekly_source_finalisation_materialise_expense_invoice_v1(
  uuid,uuid,text,uuid,uuid,uuid,uuid,text
) owner to postgres;
alter function private.weekly_source_finalise_engine_v1(jsonb,uuid,boolean) owner to postgres;
alter function private.weekly_source_finalise_core_v1(jsonb,uuid) owner to postgres;
alter function public.weekly_source_finalise_atomic_v1(jsonb) owner to postgres;

revoke all on function private.weekly_source_finalisation_hex32_v1(text,text)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_finalisation_vector_v1(uuid,text,smallint)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_finalisation_negate_vector_v1(jsonb)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_finalisation_economic_assert_v1(uuid,uuid,text)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_finalisation_expense_policy_assert_v1(uuid,uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_finalisation_lineage_assert_v1(uuid,uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_final_state_fingerprint_v1(jsonb)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_finalisation_insert_current_movement_v1(
  uuid,uuid,uuid,uuid,text,text,text,uuid,uuid,uuid,bigint,bigint,text,bytea,uuid,jsonb,uuid,uuid
) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_finalisation_insert_reversal_v1(
  uuid,uuid,uuid,uuid,uuid,text,jsonb
) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_finalisation_materialise_expense_invoice_v1(
  uuid,uuid,text,uuid,uuid,uuid,uuid,text
) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_finalise_engine_v1(jsonb,uuid,boolean)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_finalise_core_v1(jsonb,uuid)
  from public,anon,authenticated,service_role;
revoke all on function public.weekly_source_finalise_atomic_v1(jsonb)
  from public,anon,authenticated;
grant execute on function public.weekly_source_finalise_atomic_v1(jsonb) to service_role;

comment on function public.weekly_source_finalise_atomic_v1(jsonb) is
  'Atomically seals the current final Weekly source publication into immutable source snapshots, exact movements and per-client manifests. Generic and HealthRoster histories are serialised and must be finalised chronologically. Protected-pay/query state never gates source invoicing; Banking Pay and Workbench are untouched.';

notify pgrst, 'reload schema';

commit;
