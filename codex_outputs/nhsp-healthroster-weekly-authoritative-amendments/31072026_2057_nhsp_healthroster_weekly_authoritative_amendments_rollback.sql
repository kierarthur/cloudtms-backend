-- Exact TEST rollback captured before the NHSP/HealthRoster Weekly authoritative amendments install.
-- Captured 31/07/2026 from Supabase project test-cloudtms (yakevhtttcsljosbdpov).
-- Restore order is reverse dependency order. This file changes only the nine target definitions.
-- _import_review_action_catalog_core_v1: pre-install pg_get_functiondef md5 3e5eba7dde25668c430ce9ebe9848349
-- _import_review_apply_envelope_core_v1: pre-install pg_get_functiondef md5 1af234831c9e96fe4b89454de16fa376
-- _import_review_effective_invoice_balance_core_v1: pre-install pg_get_functiondef md5 09cde3980c305d7184595cc8aba9a370
-- hr_weekly_apply_transactional: pre-install pg_get_functiondef md5 3cf496208e3a4f0f748e536b2a69724e
-- hr_weekly_phase3_apply_adjustment_truth: pre-install pg_get_functiondef md5 c029f37bfb98d9c9b081a22106fe9b0b
-- import_review_correction_generation_transition_v1: pre-install pg_get_functiondef md5 60c170a4033d6d0dbb40f97ebf494942
-- nhsp_weekly_apply_transactional: pre-install pg_get_functiondef md5 bde9890924c97fb64720c455e29de153
-- nhsp_weekly_phase3_apply_adjustment_truth: pre-install pg_get_functiondef md5 ef0ee5f3544c8df497400b787d0278b9
-- timesheet_paid_uninvoiced_rollover_v1: pre-install pg_get_functiondef md5 30115251ce45976b9ae9209279053007

begin;

CREATE OR REPLACE FUNCTION public.nhsp_weekly_apply_transactional(p_import_id uuid, p_payload jsonb, p_actor_user_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
 SET "plpgsql_check.mode" TO 'disabled'
AS $function$
declare
  v_now timestamptz := now();

  -- import header
  v_import_source_system text;

  -- payload parts
  v_payload jsonb := coalesce(p_payload, '{}'::jsonb);
  v_actions_json jsonb := '[]'::jsonb;

  -- normalized selections
  v_selected_action_ids text[] := array[]::text[];
  v_selected_truth_keys text[] := array[]::text[];
  v_selected_cancel_shift_ids uuid[] := array[]::uuid[];

  -- deterministic external keys in this import (NHSP, OK rows)
  v_all_ok_external_keys text[] := array[]::text[];

  -- selected truth keys constrained to OK rows
  v_selected_truth_keys_ok text[] := array[]::text[];

  -- tick-only enforced lists
  v_force_keys_final text[] := array[]::text[];
  v_skip_keys_final text[] := array[]::text[];

  -- changed-hours partition (selected keys only)
  v_invoiced_changed_keys text[] := array[]::text[];
  v_not_invoiced_changed_keys text[] := array[]::text[];

  -- phase3 / phase1 / phase1.5
  v_phase3_result jsonb := null;
  v_changed_preflight jsonb := null;
  v_changed_timesheet_ids uuid[] := array[]::uuid[];
  v_reauthorise_timesheet_ids uuid[] := array[]::uuid[];
  v_auto_authorise_timesheet_ids uuid[] := array[]::uuid[];
  v_lifecycle_items jsonb := '[]'::jsonb;
  v_unauthorise_result jsonb := null;
  v_reconciliation_action_ids text[] := array[]::text[];
  v_operation_bound_correction_action_ids text[] := array[]::text[];
  v_operation_bound_correction_keys text[] := array[]::text[];
  v_operation_bound_correction_timesheet_ids uuid[] := array[]::uuid[];
  v_general_authorise_timesheet_ids uuid[] := array[]::uuid[];
  v_reconciliation_transition jsonb := null;
  v_reconciliation_units jsonb := '[]'::jsonb;
  v_phase1_result jsonb := null;
  v_phase15_ok int := 0;
  v_phase15_updated int := 0;

  -- policy A replacement-day enforcement + cancellation reasoning
  v_selected_cancel_shift_id_set text[] := array[]::text[];

  -- cancellations
  v_cancel_actions jsonb := '[]'::jsonb;
  v_cancellations_result jsonb := null;

  -- affected timesheets
  v_affected_timesheet_ids uuid[] := array[]::uuid[];
  v_force_keys_non_invoiced text[] := array[]::text[];

  -- debug / audit
  v_sample_selected_action_ids jsonb := '[]'::jsonb;
  v_sample_force_keys jsonb := '[]'::jsonb;
  v_sample_skip_keys jsonb := '[]'::jsonb;
  v_sample_cancel_shift_ids jsonb := '[]'::jsonb;
  v_steps jsonb := '[]'::jsonb;

  v_selected_action_ids_count int := 0;
  v_selected_row_keys_count int := 0;
  v_selected_cancel_shift_ids_count int := 0;

  v_ok_keys_total int := 0;
  v_force_keys_count int := 0;
  v_skip_keys_count int := 0;

  v_invoiced_changed_keys_count int := 0;
  v_not_invoiced_changed_keys_count int := 0;

  v_cancellations_count int := 0;

  v_phase1_shifts_created int := 0;
  v_phase1_shifts_updated int := 0;

  v_phase3_created_count int := 0;
  v_phase3_updated_count int := 0;
  v_cancel_adjustment_count int := 0;
  v_correction_timesheets_created_count int := 0;

  v_should_run_phase1 boolean := false;
  v_should_run_phase15 boolean := false;
  v_should_run_phase3 boolean := false;
  v_should_run_cancellations boolean := false;

  v_review_contract jsonb := coalesce(v_payload->'review_contract','{}'::jsonb);
  v_review_selected_ids jsonb := coalesce(v_payload->'review_selected_action_ids','[]'::jsonb);
  v_review_operation_id uuid;
  v_review_guard jsonb;
  v_review_result jsonb;

  -- ENSURE BASE WEEKLY TIMESHEET + ATTACH ACTIVE SHIFTS (invariant)
  v_ensure_pairs_count int := 0;
  v_ensure_pairs_skipped_no_active int := 0;

  v_ensure_base_week_created_count int := 0;
  v_ensure_base_week_existing_count int := 0;

  v_ensure_timesheet_created_count int := 0;
  v_ensure_timesheet_reused_count int := 0;
  v_ensure_timesheet_missing_reference_count int := 0;

  v_ensure_shifts_attached_count int := 0;
  v_ensure_shifts_relinked_invalid_ts_count int := 0;
  v_ensure_remaining_active_detached_count int := 0;

  v_ensure_sample_pairs jsonb := '[]'::jsonb;
  v_ensure_sample_created_ts_ids jsonb := '[]'::jsonb;

  -- loop vars for ensure
  v_pair_contract_id uuid;
  v_pair_candidate_id uuid;
  v_pair_client_id uuid;
  v_pair_week_ending_date date;

  v_active_count int := 0;

  v_base_week_id uuid := null;
  v_base_week_ts_id uuid := null;

  v_ts_exists boolean := false;

  v_candidate_display_name text := null;
  v_candidate_tms_ref text := null;
  v_client_name text := null;
  v_contract_display_site text := null;
  v_contract_ward_hint text := null;
  v_contract_role text := null;

  v_occupant_norm text := null;
  v_hospital_norm text := null;
  v_ward_norm text := null;
  v_role_norm text := null;

  v_booking_base text := null;
  v_hash_hex text := null;
  v_booking_id text := null;
  v_shift_label_norm text := null;

  v_new_ts_id uuid := null;

  v_attached_null_count int := 0;
  v_relinked_invalid_count int := 0;

  -- shared error
  v_sqlstate text;
  v_err text;
begin
  v_steps := v_steps || jsonb_build_array(jsonb_build_object('step','START'));

  -- ─────────────────────────────────────────────
  -- 0) Validate import exists and is NHSP
  -- ─────────────────────────────────────────────
  select upper(coalesce(hi.source_system::text, ''))
  into v_import_source_system
  from public.hr_imports hi
  where hi.id = p_import_id
  limit 1;

  if v_import_source_system is null or v_import_source_system = '' then
    raise exception 'nhsp_weekly_apply_transactional: import % not found in hr_imports.', p_import_id;
  end if;

  if v_import_source_system <> 'NHSP' then
    raise exception 'nhsp_weekly_apply_transactional: import % source_system=%; expected NHSP.', p_import_id, v_import_source_system;
  end if;

  if not exists(select 1 from public.import_review_states where import_id=p_import_id) then
    raise exception 'IMPORT_REVIEW_REQUIRED' using errcode='55000';
  end if;
  if jsonb_typeof(v_payload)<>'object' then
    raise exception 'IMPORT_REVIEW_APPLY_PAYLOAD_INVALID' using errcode='22023';
  end if;
  if exists(select 1 from jsonb_object_keys(v_payload) as keys(key_name)
    where keys.key_name not in ('review_contract','review_selected_action_ids','invalidation_action_ids')) then
    raise exception 'IMPORT_REVIEW_BROWSER_AUTHORITY_REJECTED' using errcode='22023';
  end if;
  if jsonb_typeof(v_review_contract)<>'object' or jsonb_typeof(v_review_selected_ids)<>'array'
    or not(v_payload?'invalidation_action_ids') or jsonb_typeof(v_payload->'invalidation_action_ids')<>'array' then
    raise exception 'IMPORT_REVIEW_APPLY_CONTRACT_REQUIRED' using errcode='22023';
  end if;
  v_review_operation_id:=(v_review_contract->>'operation_id')::uuid;
  v_review_guard:=public.import_review_apply_guard_v1(p_import_id,(v_review_contract->>'state_version')::bigint,
    v_review_contract->>'coverage_fingerprint',v_review_contract->>'preview_fingerprint',v_review_operation_id,
    v_review_contract->>'request_hash',v_review_selected_ids,v_payload->'invalidation_action_ids',p_actor_user_id);
  if coalesce((v_review_guard->>'replay')::boolean,false) then return v_review_guard->'stored_response'; end if;
  select coalesce(jsonb_agg(to_jsonb(case when d.action_kind='APPLY_CANCELLATION' then 'CANCEL:'||d.shift_id::text else 'ROW:'||d.source_identity end) order by d.action_id),'[]'::jsonb)
    into v_actions_json
  from public.import_review_decisions d
  where d.import_id=p_import_id and d.is_current and d.selected
    and d.action_id in (select jsonb_array_elements_text(v_review_guard->'selected_action_ids'))
    and d.action_kind in ('INCLUDE_SHIFT','APPLY_AMENDMENT','APPLY_CANCELLATION');

  v_steps := v_steps || jsonb_build_array(jsonb_build_object('step','IMPORT_OK'));

  -- ─────────────────────────────────────────────
  -- 1) Parse and normalize selection payload (ROW:/CANCEL:)
  -- ─────────────────────────────────────────────
  if jsonb_typeof(v_actions_json) <> 'array' then
    raise exception 'nhsp_weekly_apply_transactional: selected_action_ids must be a JSON array.';
  end if;

  create temporary table tmp_sel_ids(
    action_id text primary key
  ) on commit drop;

  insert into tmp_sel_ids(action_id)
  select distinct nullif(btrim(x.value), '')
  from jsonb_array_elements_text(v_actions_json) as x(value)
  where nullif(btrim(x.value), '') is not null
  on conflict do nothing;

  if exists (
    select 1
    from tmp_sel_ids tsi
    where tsi.action_id !~ '^(ROW|CANCEL):'
  ) then
    raise exception 'nhsp_weekly_apply_transactional: invalid action_id in selection (expected ROW:<external_row_key> or CANCEL:<shift_id>).';
  end if;

  select coalesce(array_agg(tsi.action_id order by tsi.action_id), array[]::text[])
  into v_selected_action_ids
  from tmp_sel_ids tsi;

  select coalesce(array_agg(distinct substring(tsi.action_id from 5) order by substring(tsi.action_id from 5)), array[]::text[])
  into v_selected_truth_keys
  from tmp_sel_ids tsi
  where tsi.action_id like 'ROW:%';

  select coalesce(array_agg(distinct (substring(tsi.action_id from 8))::uuid order by (substring(tsi.action_id from 8))::uuid), array[]::uuid[])
  into v_selected_cancel_shift_ids
  from tmp_sel_ids tsi
  where tsi.action_id like 'CANCEL:%';

  v_selected_action_ids_count := coalesce(array_length(v_selected_action_ids, 1), 0);
  v_selected_row_keys_count := coalesce(array_length(v_selected_truth_keys, 1), 0);
  v_selected_cancel_shift_ids_count := coalesce(array_length(v_selected_cancel_shift_ids, 1), 0);

  v_steps := v_steps || jsonb_build_array(jsonb_build_object(
    'step','SELECTION_PARSED',
    'selected_action_ids_count', v_selected_action_ids_count,
    'selected_row_keys_count', v_selected_row_keys_count,
    'selected_cancel_shift_ids_count', v_selected_cancel_shift_ids_count
  ));

  -- sample selected action ids (cap 40)
  select coalesce(jsonb_agg(x.action_id), '[]'::jsonb)
  into v_sample_selected_action_ids
  from (
    select s.action_id
    from unnest(coalesce(v_selected_action_ids, array[]::text[])) as s(action_id)
    order by s.action_id
    limit 40
  ) as x;

  -- ─────────────────────────────────────────────
  -- 2) Load weekly_import_phase2 for NHSP and constrain selection to OK rows
  -- ─────────────────────────────────────────────
  create temporary table tmp_p2_all on commit drop as
  select *
  from public.weekly_import_phase2(p_import_id := p_import_id, p_system_type := 'NHSP');

  create temporary table tmp_p2_ok on commit drop as
  select
    p2.hr_row_id,
    p2.external_row_key,
    p2.work_date,
    p2.week_ending_date,
    p2.candidate_id,
    p2.client_id,
    p2.contract_id,
    upper(coalesce(p2.action::text,'')) as action
  from tmp_p2_all p2
  where upper(coalesce(p2.action::text,'')) = 'OK'
    and p2.external_row_key is not null
    and p2.candidate_id is not null
    and p2.client_id is not null
    and p2.contract_id is not null
    and p2.work_date is not null
    and p2.week_ending_date is not null;

  select coalesce(array_agg(distinct p2.external_row_key order by p2.external_row_key), array[]::text[])
  into v_all_ok_external_keys
  from tmp_p2_ok p2;

  v_ok_keys_total := coalesce(array_length(v_all_ok_external_keys, 1), 0);

  -- selected truth keys must be present in OK universe
  if exists (
    select 1
    from unnest(coalesce(v_selected_truth_keys, array[]::text[])) as k(external_row_key)
    left join (select distinct p2.external_row_key from tmp_p2_ok p2) as okk
      on okk.external_row_key = k.external_row_key
    where okk.external_row_key is null
  ) then
    raise exception 'nhsp_weekly_apply_transactional: selection includes ROW:<external_row_key> that is not an OK/resolved NHSP row (resolve mappings first).';
  end if;

  select coalesce(array_agg(distinct k.external_row_key order by k.external_row_key), array[]::text[])
  into v_selected_truth_keys_ok
  from (
    select distinct k.external_row_key
    from unnest(coalesce(v_selected_truth_keys, array[]::text[])) as k(external_row_key)
    join (select distinct p2.external_row_key from tmp_p2_ok p2) as okk
      on okk.external_row_key = k.external_row_key
  ) as k;

  -- Tick = PROCEED semantics
  v_force_keys_final := coalesce(v_selected_truth_keys_ok, array[]::text[]);

  select coalesce(array_agg(x.external_row_key order by x.external_row_key), array[]::text[])
  into v_skip_keys_final
  from (
    select distinct okk.external_row_key
    from unnest(coalesce(v_all_ok_external_keys, array[]::text[])) as okk(external_row_key)
    left join unnest(coalesce(v_force_keys_final, array[]::text[])) as fk(external_row_key)
      on fk.external_row_key = okk.external_row_key
    where fk.external_row_key is null
  ) as x;

  v_force_keys_count := coalesce(array_length(v_force_keys_final, 1), 0);
  v_skip_keys_count := coalesce(array_length(v_skip_keys_final, 1), 0);
  v_cancellations_count := coalesce(array_length(v_selected_cancel_shift_ids, 1), 0);

  -- Import-authoritative NHSP shifts and calculated expenses must never share
  -- a timesheet.  Check both an existing imported shift and the base
  -- contract-week timesheet that a new row would reuse, before any source or
  -- financial mutation begins.
  if exists (
    select 1
    from (
      select cw.timesheet_id
      from tmp_p2_ok p2
      join public.contract_weeks cw
        on cw.contract_id=p2.contract_id
       and cw.week_ending_date=p2.week_ending_date
       and cw.is_adjustment=false
       and coalesce(cw.additional_seq,0)=0
      where p2.external_row_key=any(coalesce(v_force_keys_final,array[]::text[]))
        and cw.timesheet_id is not null
        and not exists (
          select 1
          from public.nhsp_shifts existing_import_shift
          where existing_import_shift.source_system='NHSP'::public.hr_source_enum
            and existing_import_shift.external_row_key=p2.external_row_key
            and existing_import_shift.cancelled_at_utc is null
        )
      union
      select ns.timesheet_id
      from public.nhsp_shifts ns
      where ns.source_system='NHSP'::public.hr_source_enum
        and ns.timesheet_id is not null
        and (
          ns.external_row_key=any(coalesce(v_force_keys_final,array[]::text[]))
          or ns.id=any(coalesce(v_selected_cancel_shift_ids,array[]::uuid[]))
        )
    ) expense_target
    where public._import_review_timesheet_has_calculated_expenses_core_v1(expense_target.timesheet_id)
  ) then
    raise exception using
      message='IMPORT_AUTHORITATIVE_EXPENSE_SEPARATION_REQUIRED',
      errcode='P0001',
      detail=jsonb_build_object(
        'code','IMPORT_AUTHORITATIVE_EXPENSE_SEPARATION_REQUIRED',
        'message','Timesheet occupied by expenses. Remove the expenses from this timesheet, save or recalculate it, then choose Recheck. Expenses must be invoiced on a separate timesheet for import-authoritative work; no import mutation was applied.'
      )::text;
  end if;

  v_steps := v_steps || jsonb_build_array(jsonb_build_object(
    'step','PHASE2_OK_LOADED',
    'ok_keys_total', v_ok_keys_total,
    'force_keys_count', v_force_keys_count,
    'skip_keys_count', v_skip_keys_count,
    'cancellations_count', v_cancellations_count
  ));

  -- samples (cap 40 each)
  select coalesce(jsonb_agg(x.k), '[]'::jsonb)
  into v_sample_force_keys
  from (
    select k as k
    from unnest(coalesce(v_force_keys_final, array[]::text[])) as k
    order by k
    limit 40
  ) as x;

  select coalesce(jsonb_agg(x.k), '[]'::jsonb)
  into v_sample_skip_keys
  from (
    select k as k
    from unnest(coalesce(v_skip_keys_final, array[]::text[])) as k
    order by k
    limit 40
  ) as x;

  select coalesce(jsonb_agg(y.s), '[]'::jsonb)
  into v_sample_cancel_shift_ids
  from (
    select s::text as s
    from unnest(coalesce(v_selected_cancel_shift_ids, array[]::uuid[])) as s
    order by s::text
    limit 40
  ) as y;

  -- ─────────────────────────────────────────────
  -- ✅ FIX: No-op apply guard
  -- ─────────────────────────────────────────────
  v_should_run_phase1 := (v_force_keys_count > 0);
  v_should_run_phase15 := v_should_run_phase1;
  v_should_run_cancellations := (v_cancellations_count > 0);

  v_steps := v_steps || jsonb_build_array(jsonb_build_object(
    'step','NOOP_GUARD_EVAL',
    'should_run_phase1', v_should_run_phase1,
    'should_run_phase15', v_should_run_phase15,
    'should_run_cancellations', v_should_run_cancellations,
    'reason',
      case
        when (v_should_run_phase1 is false and v_should_run_cancellations is false)
          then 'NO_SELECTION_NO_AUTONEW_NO_CANCELLATION => SKIP_TRUTH_MUTATION'
        when (v_should_run_phase1 is true and v_should_run_cancellations is false)
          then 'HAS_SELECTED_ROWS'
        when (v_should_run_phase1 is false and v_should_run_cancellations is true)
          then 'HAS_SELECTED_CANCELLATIONS'
        else 'HAS_SELECTED_ROWS_AND_CANCELLATIONS'
      end
  ));

  if (v_should_run_phase1 is false and v_should_run_cancellations is false) then
    update public.hr_imports hi_noop
    set import_scope = 'NHSP'
    where hi_noop.id = p_import_id;

    v_steps := v_steps || jsonb_build_array(jsonb_build_object('step','IMPORT_BATCH_APPLIED_NOOP'));

    perform public._imp_debug_audit(
      p_actor_user_id,
      'NHSP_WEEKLY_APPLY_DEBUG',
      jsonb_build_object(
        'import_id', p_import_id::text,
        'steps', v_steps,

        'selected_action_ids_count', v_selected_action_ids_count,
        'selected_row_keys_count', v_selected_row_keys_count,
        'selected_cancel_shift_ids_count', v_selected_cancel_shift_ids_count,

        'selected_action_ids_sample', v_sample_selected_action_ids,

        'ok_keys_total', v_ok_keys_total,
        'force_keys_count', v_force_keys_count,
        'skip_keys_count', v_skip_keys_count,

        'phase1_called', false,
        'phase1_force_keys_sample', v_sample_force_keys,
        'phase1_skip_keys_sample', v_sample_skip_keys,

        'cancellations_called', false,
        'sample_cancel_shift_ids', v_sample_cancel_shift_ids,

        'invoiced_changed_keys_count', 0,
        'not_invoiced_changed_keys_count', 0,
        'phase3_created_count', 0,
        'phase3_updated_count', 0,
        'cancel_adjustment_count', 0,
        'correction_timesheets_created_count', 0,
        'affected_timesheet_ids_count', 0
      ),
      'hr_imports',
      p_import_id::text,
      null,
      null,
      null,
      null
    );

    v_review_result:=jsonb_build_object(
      'import_id', p_import_id,
      'mode_b', jsonb_build_object(
        'selected_truth_keys', to_jsonb(array[]::text[]),
        'force_overwrite_external_row_keys', to_jsonb(array[]::text[]),
        'skip_external_row_keys', to_jsonb(coalesce(v_all_ok_external_keys, array[]::text[])),
        'phase3', null,
        'phase1', null,
        'phase15', jsonb_build_object('ok_rows', 0, 'shift_updated_rows', 0),
        'cancellations', null
      ),
      'affected_timesheet_ids', to_jsonb(array[]::uuid[]),
      'post_commit_email_action_ids','[]'::jsonb,
      'review_operation_id',v_review_operation_id
    );
    perform public._import_review_apply_complete_core_v1(p_import_id,v_review_operation_id,p_actor_user_id,v_review_result,false);
    return v_review_result;
  end if;

  v_steps := v_steps || jsonb_build_array(jsonb_build_object('step','NOOP_GUARD_PASSED'));

  -- ─────────────────────────────────────────────
  -- 3) Snapshot changed-hours rows for selected keys BEFORE any truth mutation
  -- ─────────────────────────────────────────────
  create temporary table tmp_changed_sel on commit drop as
  select
    ch.external_row_key,
    ch.timesheet_id,
    ch.is_paid,
    ch.is_invoiced
  from public.weekly_import_changed_hours_phase3(p_import_id := p_import_id, p_system_type := 'NHSP') as ch
  where ch.external_row_key = any(coalesce(v_force_keys_final, array[]::text[]));

  if to_regclass('pg_temp.import_review_reconciliation_units_v1') is not null then
    if exists(select 1 from pg_temp.import_review_reconciliation_units_v1 u
      where u.unit_json->>'source_system'<>'NHSP'
         or u.unit_json->>'schema_version'<>'IMPORT_AUTHORITATIVE_RECONCILIATION_V1'
         or u.route not in ('AMEND_SOURCE','AMEND_PAID_UNINVOICED_SOURCE','AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT')) then
      raise exception 'IMPORT_REVIEW_RECONCILIATION_UNIT_INVALID' using errcode='22023';
    end if;
    select coalesce(array_agg(u.action_id order by u.action_id),array[]::text[]),
      coalesce(array_agg(u.action_id order by u.action_id) filter(where u.route in ('AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT')),array[]::text[]),
      coalesce(array_agg(u.source_identity order by u.source_identity) filter(where u.route in ('AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT')),array[]::text[])
    into v_reconciliation_action_ids,v_operation_bound_correction_action_ids,v_operation_bound_correction_keys
    from pg_temp.import_review_reconciliation_units_v1 u;
  end if;

  select coalesce(array_agg(cs.external_row_key order by cs.external_row_key), array[]::text[])
  into v_invoiced_changed_keys
  from tmp_changed_sel cs
  where cs.is_invoiced is true;

  select coalesce(array_agg(cs.external_row_key order by cs.external_row_key), array[]::text[])
  into v_not_invoiced_changed_keys
  from tmp_changed_sel cs
  where cs.is_invoiced is false;

  if cardinality(v_reconciliation_action_ids)>0 then
    select coalesce(array_agg(u.source_identity order by u.source_identity)
      filter(where u.route in ('AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT')),array[]::text[]),
      coalesce(array_agg(u.source_identity order by u.source_identity)
      filter(where u.route in ('AMEND_SOURCE','AMEND_PAID_UNINVOICED_SOURCE')),array[]::text[])
    into v_invoiced_changed_keys,v_not_invoiced_changed_keys
    from pg_temp.import_review_reconciliation_units_v1 u;
    if cardinality(v_operation_bound_correction_action_ids)>0 then
      v_reconciliation_transition:=public.import_review_correction_generation_transition_v1(
        p_import_id,v_review_operation_id,v_review_contract->>'request_hash','PREPARE',p_actor_user_id,
        v_operation_bound_correction_action_ids,v_now);
    end if;
  end if;

  v_invoiced_changed_keys_count := coalesce(array_length(v_invoiced_changed_keys, 1), 0);
  v_not_invoiced_changed_keys_count := coalesce(array_length(v_not_invoiced_changed_keys, 1), 0);

  select coalesce(array_agg(distinct cs.timesheet_id order by cs.timesheet_id), array[]::uuid[])
    into v_changed_timesheet_ids
  from tmp_changed_sel cs
  where cs.timesheet_id is not null;

  if coalesce(array_length(v_changed_timesheet_ids, 1), 0) > 0 then
    select public.import_timesheet_financial_preflight_v1(
      p_timesheet_ids := v_changed_timesheet_ids,
      p_action := 'IMPORT_CHANGED_HOURS',
      p_actor_user_id := p_actor_user_id,
      p_expected_state_json := '{}'::jsonb,
      p_lock_rows := true,
      p_max_scope := 100
    ) into v_changed_preflight;

    if coalesce((v_changed_preflight->>'allowed')::boolean, false) is not true then
      raise exception using message='IMPORT_FINANCIAL_PREFLIGHT_BLOCKED', errcode='P0001', detail=v_changed_preflight::text;
    end if;

    if exists (
      select 1 from tmp_changed_sel cs
      where cs.is_invoiced is false
        and exists (select 1 from public.timesheets_financials paid_tf where paid_tf.timesheet_id=cs.timesheet_id and paid_tf.paid_at_utc is not null)
        and not exists (
          select 1 from public.timesheets_financials current_tf
          where current_tf.timesheet_id=cs.timesheet_id and current_tf.is_current=true
            and current_tf.stale_reason='IMPORT_PAID_TSFIN_ROLLOVER_PENDING_CALCULATION'
            and coalesce((current_tf.policy_snapshot_json->>'requires_frozen_correction_policy')::boolean,false)=true
        )
    ) then
      raise exception using message='PAID_UNINVOICED_ROLLOVER_REQUIRED', errcode='P0001',
        detail=jsonb_build_object(
          'code','PAID_UNINVOICED_ROLLOVER_REQUIRED',
          'required_path',jsonb_build_array(
            'UNAUTHORISE','PAID_UNINVOICED_ROLLOVER','AMEND','RECALCULATE','REAUTHORISE'
          ),
          'invoice_policy_without_history','NOW',
          'timesheet_ids',to_jsonb(v_changed_timesheet_ids)
        )::text;
    end if;
  end if;

  -- Preserve the lifecycle state of authorised, mutable source timesheets.
  -- The source transaction performs the canonical unauthorise step before
  -- changing truth.  The Worker reauthorises exactly this persisted set only
  -- after its bounded TSFIN follow-up has completed successfully.
  select coalesce(array_agg(distinct lifecycle_scope.timesheet_id order by lifecycle_scope.timesheet_id),array[]::uuid[])
  into v_reauthorise_timesheet_ids
  from (
    select cs.timesheet_id
    from tmp_changed_sel cs
    join public.timesheets ts on ts.timesheet_id=cs.timesheet_id and ts.is_current=true
    left join public.timesheets_financials tf on tf.timesheet_id=ts.timesheet_id and tf.is_current=true
    left join public.contract_weeks cw on cw.timesheet_id=ts.timesheet_id
    where cs.timesheet_id is not null
      and not (cs.external_row_key=any(coalesce(v_operation_bound_correction_keys,array[]::text[])))
      and cs.is_invoiced is false
      and cs.is_paid is false
      and (ts.authorised_at_server is not null or tf.authorised_at_utc is not null
        or cw.status='AUTHORISED'::public.contract_week_status_enum)
    union
    -- A protected source timesheet can already have a later, still-mutable
    -- correction pair. Phase 3 deliberately amends that pair's replacement
    -- in place. Put its currently authorised replacement through the same
    -- canonical lifecycle; the bulk RPC expands it to the complete pair.
    select existing_replacement.timesheet_id
    from tmp_changed_sel cs
    cross join lateral (
      select ns_existing.id
      from public.nhsp_shifts ns_existing
      where ns_existing.source_system='NHSP'::public.hr_source_enum
        and ns_existing.external_row_key=cs.external_row_key
        and ns_existing.cancelled_at_utc is null
      order by ns_existing.updated_at desc nulls last,ns_existing.created_at desc nulls last
      limit 1
    ) source_shift
    cross join lateral (
      select tpos.timesheet_id
      from public.timesheets tpos
      where tpos.is_adjustment is true
        and tpos.is_current is true
        and tpos.correction_kind='CHANGED_HOURS_REPLACEMENT'
        and jsonb_typeof(tpos.actual_schedule_json)='array'
        and tpos.actual_schedule_json @> jsonb_build_array(jsonb_build_object(
          'shift_id',source_shift.id::text,
          'external_row_key',cs.external_row_key
        ))
      order by tpos.updated_at desc nulls last,tpos.created_at desc nulls last
      limit 1
    ) existing_replacement
    join public.timesheets pair_pos
      on pair_pos.timesheet_id=existing_replacement.timesheet_id and pair_pos.is_current=true
    left join public.timesheets_financials pair_tf
      on pair_tf.timesheet_id=pair_pos.timesheet_id and pair_tf.is_current=true
    left join public.contract_weeks pair_cw on pair_cw.timesheet_id=pair_pos.timesheet_id
    where cs.is_invoiced is true
      and not (cs.external_row_key=any(coalesce(v_operation_bound_correction_keys,array[]::text[])))
      and coalesce((public._import_review_timesheet_protection_core_v1(existing_replacement.timesheet_id)->>'paid')::boolean,false)=false
      and coalesce((public._import_review_timesheet_protection_core_v1(existing_replacement.timesheet_id)->>'invoice_locked')::boolean,false)=false
      and (pair_pos.authorised_at_server is not null or pair_tf.authorised_at_utc is not null
        or pair_cw.status='AUTHORISED'::public.contract_week_status_enum)
    union
    select ns.timesheet_id
    from public.nhsp_shifts ns
    join public.timesheets ts on ts.timesheet_id=ns.timesheet_id and ts.is_current=true
    left join public.timesheets_financials tf on tf.timesheet_id=ts.timesheet_id and tf.is_current=true
    left join public.contract_weeks cw on cw.timesheet_id=ts.timesheet_id
    where ns.id=any(coalesce(v_selected_cancel_shift_ids,array[]::uuid[]))
      and ns.timesheet_id is not null
      and coalesce((public._import_review_timesheet_protection_core_v1(ns.timesheet_id)->>'paid')::boolean,false)=false
      and coalesce((public._import_review_timesheet_protection_core_v1(ns.timesheet_id)->>'invoice_locked')::boolean,false)=false
      and (ts.authorised_at_server is not null or tf.authorised_at_utc is not null
        or cw.status='AUTHORISED'::public.contract_week_status_enum)
  ) lifecycle_scope;

  if cardinality(v_reauthorise_timesheet_ids)>100 then
    raise exception 'IMPORT_REVIEW_REAUTHORISE_SCOPE_TOO_LARGE' using errcode='54000';
  end if;
  if cardinality(v_reauthorise_timesheet_ids)>0 then
    select coalesce(jsonb_agg(jsonb_build_object(
      'timesheet_id',target_id::text,
      'expected_timesheet_id',target_id::text
    ) order by target_id),'[]'::jsonb)
    into v_lifecycle_items
    from unnest(v_reauthorise_timesheet_ids) as lifecycle_target(target_id);

    select public.timesheet_unauthorise_bulk_atomic(v_lifecycle_items,p_actor_user_id,v_now)
    into v_unauthorise_result;
    if coalesce((v_unauthorise_result->>'ok')::boolean,false) is not true
      or coalesce((v_unauthorise_result->>'all_success')::boolean,false) is not true then
      raise exception using message='IMPORT_REVIEW_CANONICAL_UNAUTHORISE_FAILED',errcode='P0001',
        detail=jsonb_build_object(
          'code','IMPORT_REVIEW_CANONICAL_UNAUTHORISE_FAILED',
          'timesheet_ids',to_jsonb(v_reauthorise_timesheet_ids),
          'failure_count',coalesce((v_unauthorise_result->>'failure_count')::int,cardinality(v_reauthorise_timesheet_ids))
        )::text;
    end if;
    v_steps:=v_steps||jsonb_build_array(jsonb_build_object(
      'step','CANONICAL_UNAUTHORISE_COMPLETE',
      'reauthorise_timesheet_count',cardinality(v_reauthorise_timesheet_ids)
    ));
  end if;

  v_should_run_phase3 := (v_invoiced_changed_keys_count > 0);

  v_steps := v_steps || jsonb_build_array(jsonb_build_object(
    'step','CHANGED_HOURS_PARTITIONED',
    'invoiced_changed_keys_count', v_invoiced_changed_keys_count,
    'not_invoiced_changed_keys_count', v_not_invoiced_changed_keys_count,
    'phase3_should_run', v_should_run_phase3
  ));

  -- ─────────────────────────────────────────────
  -- 4) Policy A replacement-day enforcement (NHSP)
  -- ─────────────────────────────────────────────
  create temporary table tmp_selected_replacement_keys(
    candidate_id uuid,
    client_id uuid,
    old_work_date date,
    replacement_day_key text
  ) on commit drop;

  if array_length(v_force_keys_final, 1) is not null then
    create temporary table tmp_sel_truth_p2 on commit drop as
    select
      p2.external_row_key,
      p2.candidate_id,
      p2.client_id,
      p2.work_date as import_work_date
    from tmp_p2_ok p2
    where p2.external_row_key = any(v_force_keys_final);

    create temporary table tmp_existing_by_key on commit drop as
    select distinct on (ns.external_row_key)
      ns.external_row_key,
      ns.id as shift_id,
      ns.candidate_id as candidate_id,
      ns.client_id as client_id,
      ns.work_date as old_work_date
    from public.nhsp_shifts ns
    where ns.source_system = 'NHSP'::public.hr_source_enum
      and ns.cancelled_at_utc is null
      and ns.external_row_key = any(v_force_keys_final)
      and ns.work_date is not null
    order by ns.external_row_key, ns.updated_at desc nulls last, ns.created_at desc nulls last;

    insert into tmp_selected_replacement_keys(candidate_id, client_id, old_work_date, replacement_day_key)
    select distinct
      (coalesce(ex.candidate_id, st.candidate_id))::uuid as candidate_id,
      (coalesce(ex.client_id, st.client_id))::uuid as client_id,
      ex.old_work_date as old_work_date,
      ((coalesce(ex.candidate_id, st.candidate_id))::text || '|' ||
       (coalesce(ex.client_id, st.client_id))::text || '|' ||
       (ex.old_work_date)::text) as replacement_day_key
    from tmp_sel_truth_p2 st
    join tmp_existing_by_key ex
      on ex.external_row_key = st.external_row_key
    where ex.old_work_date is not null
      and st.import_work_date is not null
      and ex.old_work_date <> st.import_work_date;

    select coalesce(array_agg(x::text), array[]::text[])
    into v_selected_cancel_shift_id_set
    from unnest(coalesce(v_selected_cancel_shift_ids, array[]::uuid[])) as x;

    if exists (select 1 from tmp_selected_replacement_keys) then
      create temporary table tmp_required_cancel_ids on commit drop as
      select distinct
        rk.replacement_day_key,
        ns2.id as shift_id
      from tmp_selected_replacement_keys rk
      join public.nhsp_shifts ns2
        on ns2.source_system = 'NHSP'::public.hr_source_enum
       and ns2.cancelled_at_utc is null
       and ns2.candidate_id = rk.candidate_id
       and ns2.client_id = rk.client_id
       and ns2.work_date = rk.old_work_date;

      if exists (
        select 1
        from tmp_required_cancel_ids rc
        left join unnest(v_selected_cancel_shift_id_set) as sel(shift_id_text)
          on sel.shift_id_text = rc.shift_id::text
        where sel.shift_id_text is null
      ) then
        raise exception 'nhsp_weekly_apply_transactional: Policy A violation (replacement-day selected without selecting all required cancellations).';
      end if;
    end if;
  end if;

  v_steps := v_steps || jsonb_build_array(jsonb_build_object('step','POLICY_A_OK'));

  -- ─────────────────────────────────────────────
  -- 5) Changed-hours correction series for invoiced keys (BEFORE Phase 1)
  -- ─────────────────────────────────────────────
  if v_should_run_phase3 then
    select public.nhsp_weekly_phase3_apply_adjustment_truth(
      p_import_id := p_import_id,
      p_selected_external_row_keys := v_invoiced_changed_keys,
      p_actor_user_id := p_actor_user_id
    )
    into v_phase3_result;
  end if;

  v_phase3_created_count := jsonb_array_length(coalesce(v_phase3_result->'created_timesheet_ids', '[]'::jsonb));
  v_phase3_updated_count := jsonb_array_length(coalesce(v_phase3_result->'updated_timesheet_ids', '[]'::jsonb));

  v_steps := v_steps || jsonb_build_array(jsonb_build_object(
    'step','PHASE3_CORRECTIONS_DONE',
    'phase3_called', v_should_run_phase3,
    'phase3_created_count', v_phase3_created_count,
    'phase3_updated_count', v_phase3_updated_count
  ));

  -- ─────────────────────────────────────────────
  -- 6) Phase 1 upsert (NHSP) with tick-only skip/force
  -- ─────────────────────────────────────────────
  if v_should_run_phase1 then
    select public.nhsp_apply_import_phase1(
      p_import_id := p_import_id,
      p_selected_group_ids := array[]::text[],
      p_skip_external_row_keys := v_skip_keys_final,
      p_force_overwrite_external_row_keys := v_force_keys_final
    )
    into v_phase1_result;
  else
    v_phase1_result := null;
  end if;

  v_phase1_shifts_created := coalesce(nullif((coalesce(v_phase1_result,'{}'::jsonb)->>'shifts_created')::int, null), 0);
  v_phase1_shifts_updated := coalesce(nullif((coalesce(v_phase1_result,'{}'::jsonb)->>'shifts_updated')::int, null), 0);

  v_steps := v_steps || jsonb_build_array(jsonb_build_object(
    'step','PHASE1_DONE',
    'phase1_called', v_should_run_phase1,
    'phase1_shifts_created', v_phase1_shifts_created,
    'phase1_shifts_updated', v_phase1_shifts_updated
  ));

  -- ─────────────────────────────────────────────
  -- 7) Phase 1.5 repair (NHSP)
  -- ─────────────────────────────────────────────
  if v_should_run_phase15 then
    create temporary table tmp_phase15_rows on commit drop as
    select *
    from public.weekly_import_apply_phase2(p_import_id := p_import_id, p_system_type := 'NHSP');

    select count(*)::int
    into v_phase15_ok
    from tmp_phase15_rows r
    where upper(coalesce(r.action::text,'')) = 'OK';

    select count(*)::int
    into v_phase15_updated
    from tmp_phase15_rows r
    where coalesce(r.shift_updated,false) is true;
  else
    v_phase15_ok := 0;
    v_phase15_updated := 0;
  end if;

  v_steps := v_steps || jsonb_build_array(jsonb_build_object(
    'step','PHASE15_DONE',
    'phase15_called', v_should_run_phase15,
    'phase15_ok_rows', v_phase15_ok,
    'phase15_shift_updated_rows', v_phase15_updated
  ));

  -- ─────────────────────────────────────────────
  -- 8) Apply selected cancellations (explicit shift_id only; NHSP)
  -- ─────────────────────────────────────────────
  if v_should_run_cancellations then
    create temporary table tmp_cancel_meta on commit drop as
    select
      ns.id as shift_id,
      ns.candidate_id,
      ns.client_id,
      ns.work_date
    from public.nhsp_shifts ns
    where ns.id = any(v_selected_cancel_shift_ids);

    create temporary table tmp_selected_rep_keys_text on commit drop as
    select distinct
      rk.replacement_day_key
    from tmp_selected_replacement_keys rk;

    select coalesce(
      jsonb_agg(
        jsonb_build_object(
          'shift_id', cm.shift_id::text,
          'reason',
            case
              when exists (
                select 1
                from tmp_selected_rep_keys_text sr
                where sr.replacement_day_key = (cm.candidate_id::text || '|' || cm.client_id::text || '|' || cm.work_date::text)
              ) then 'REPLACEMENT_DAY'
              else 'MISSING_FROM_IMPORT'
            end
        )
      ),
      '[]'::jsonb
    )
    into v_cancel_actions
    from tmp_cancel_meta cm;

    select public.nhsp_weekly_apply_cancellations(
      p_import_id := p_import_id,
      p_actions := v_cancel_actions,
      p_actor_user_id := p_actor_user_id
    )
    into v_cancellations_result;
  else
    v_cancellations_result := null;
  end if;

  v_steps := v_steps || jsonb_build_array(jsonb_build_object(
    'step','CANCELLATIONS_DONE',
    'cancellations_called', v_should_run_cancellations
  ));

  -- ─────────────────────────────────────────────
  -- ✅ 8.5) ENSURE BASE WEEKLY TIMESHEET EXISTS + ATTACH ACTIVE NHSP SHIFTS
  -- ─────────────────────────────────────────────
  v_steps := v_steps || jsonb_build_array(jsonb_build_object('step','ENSURE_BASE_WEEKLY_START'));

  create temporary table tmp_ensure_pairs(
    contract_id uuid,
    candidate_id uuid,
    client_id uuid,
    week_ending_date date
  ) on commit drop;

  insert into tmp_ensure_pairs(contract_id, candidate_id, client_id, week_ending_date)
  select distinct
    p2ok.contract_id,
    p2ok.candidate_id,
    p2ok.client_id,
    p2ok.week_ending_date
  from tmp_p2_ok p2ok
  where p2ok.external_row_key = any(coalesce(v_force_keys_final, array[]::text[]));

  if array_length(v_selected_cancel_shift_ids, 1) is not null then
    insert into tmp_ensure_pairs(contract_id, candidate_id, client_id, week_ending_date)
    select distinct
      ns.contract_id,
      ns.candidate_id,
      ns.client_id,
      ns.week_ending_date
    from public.nhsp_shifts ns
    where ns.id = any(v_selected_cancel_shift_ids)
      and ns.contract_id is not null
      and ns.client_id is not null
      and ns.candidate_id is not null
      and ns.week_ending_date is not null;
  end if;

  create temporary table tmp_ensure_pairs_u on commit drop as
  select distinct
    tep.contract_id,
    tep.candidate_id,
    tep.client_id,
    tep.week_ending_date
  from tmp_ensure_pairs tep
  where tep.contract_id is not null
    and tep.client_id is not null
    and tep.candidate_id is not null
    and tep.week_ending_date is not null;

  select count(*)::int
  into v_ensure_pairs_count
  from tmp_ensure_pairs_u teu;

  select coalesce(jsonb_agg(jsonb_build_object(
    'contract_id', teu.contract_id::text,
    'week_ending_date', teu.week_ending_date::text
  )), '[]'::jsonb)
  into v_ensure_sample_pairs
  from (
    select teu.contract_id, teu.week_ending_date
    from tmp_ensure_pairs_u teu
    order by teu.contract_id::text, teu.week_ending_date::text
    limit 20
  ) as teu;

  drop table if exists pg_temp.tmp_aff_ts;
  create temporary table tmp_aff_ts(
    timesheet_id uuid primary key
  ) on commit drop;

  create temporary table tmp_ensure_created_ts_ids(
    timesheet_id uuid primary key
  ) on commit drop;

  for v_pair_contract_id, v_pair_candidate_id, v_pair_client_id, v_pair_week_ending_date in
    select teu.contract_id, teu.candidate_id, teu.client_id, teu.week_ending_date
    from tmp_ensure_pairs_u teu
    order by teu.contract_id::text, teu.week_ending_date::text
  loop

    select count(*)::int
    into v_active_count
    from public.nhsp_shifts ns_active
    where ns_active.source_system = 'NHSP'::public.hr_source_enum
      and ns_active.cancelled_at_utc is null
      and ns_active.contract_id = v_pair_contract_id
      and ns_active.candidate_id = v_pair_candidate_id
      and ns_active.client_id = v_pair_client_id
      and ns_active.week_ending_date = v_pair_week_ending_date;

    if coalesce(v_active_count, 0) <= 0 then
      v_ensure_pairs_skipped_no_active := v_ensure_pairs_skipped_no_active + 1;
      continue;
    end if;

    v_base_week_id := null;
    v_base_week_ts_id := null;

    select cw0.id, cw0.timesheet_id
    into v_base_week_id, v_base_week_ts_id
    from public.contract_weeks cw0
    where cw0.contract_id = v_pair_contract_id
      and cw0.week_ending_date = v_pair_week_ending_date
      and cw0.is_adjustment is false
      and coalesce(cw0.additional_seq, 0) = 0
    limit 1
    for update;

    if v_base_week_id is null then
      insert into public.contract_weeks(
        contract_id,
        week_ending_date,
        additional_seq,
        status,
        submission_mode_snapshot,
        timesheet_id,
        planned_schedule_json,
        created_at,
        updated_at,
        is_adjustment
      )
      values (
        v_pair_contract_id,
        v_pair_week_ending_date,
        0,
        'SUBMITTED'::public.contract_week_status_enum,
        'MANUAL'::public.submission_mode_enum,
        null,
        null,
        v_now,
        v_now,
        false
      )
      returning id into v_base_week_id;

      v_ensure_base_week_created_count := v_ensure_base_week_created_count + 1;
      v_base_week_ts_id := null;
    else
      v_ensure_base_week_existing_count := v_ensure_base_week_existing_count + 1;
    end if;

    if v_base_week_ts_id is not null then
      select exists(
        select 1
        from public.timesheets tchk
        where tchk.timesheet_id = v_base_week_ts_id
        limit 1
      )
      into v_ts_exists;

      if v_ts_exists is not true then
        update public.contract_weeks cw0u
        set
          timesheet_id = null,
          updated_at = v_now
        where cw0u.id = v_base_week_id;

        v_ensure_timesheet_missing_reference_count := v_ensure_timesheet_missing_reference_count + 1;
        v_base_week_ts_id := null;
      end if;
    end if;

    select ct.display_site, ct.ward_hint, ct.role
    into v_contract_display_site, v_contract_ward_hint, v_contract_role
    from public.contracts ct
    where ct.id = v_pair_contract_id
    limit 1;

    select cand.display_name, cand.tms_ref
    into v_candidate_display_name, v_candidate_tms_ref
    from public.candidates cand
    where cand.id = v_pair_candidate_id
    limit 1;

    select cli.name
    into v_client_name
    from public.clients cli
    where cli.id = v_pair_client_id
    limit 1;

    v_occupant_norm := lower(coalesce(v_candidate_tms_ref, v_candidate_display_name, v_pair_candidate_id::text));
    v_hospital_norm := lower(coalesce(v_contract_display_site, v_client_name, v_pair_client_id::text));
    v_ward_norm := lower(coalesce(v_contract_ward_hint, 'contract'));
    v_role_norm := lower(coalesce(v_contract_role, 'weekly'));

    v_shift_label_norm := 'weekly-0';

    v_booking_base :=
      v_occupant_norm || '|' ||
      v_pair_week_ending_date::text || '|' ||
      v_hospital_norm || '|' ||
      v_ward_norm || '|' ||
      v_role_norm || '|' ||
      v_shift_label_norm;

    v_hash_hex := encode(extensions.digest(convert_to(v_booking_base, 'utf8'), 'sha256'::text), 'hex');
    v_booking_id := 'bk_' || substr(v_hash_hex, 1, 16);

    if v_base_week_ts_id is null then
      v_new_ts_id := null;

      insert into public.timesheets(
        booking_id,
        version,
        is_current,
        status,

        sheet_scope,
        submission_mode,
        line_type,
        authorised_at_server,

        occupant_key_norm,
        hospital_norm,
        ward_norm,
        job_title_norm,
        shift_label_norm,

        week_ending_date,
        contract_id,

        manual_pdf_r2_key,
        actual_schedule_json,

        qr_payload_json,
        candidate_hint_text,

        is_adjustment,
        parent_timesheet_id,
        correction_id,
        correction_kind,
        adjustment_origin,

        created_at,
        updated_at
      )
      values (
        v_booking_id,
        1,
        true,
        'RECEIVED'::public.timesheet_status_enum,

        'WEEKLY'::public.timesheet_scope_enum,
        'MANUAL'::public.submission_mode_enum,
        'HOURS'::public.timesheet_line_type_enum,
        null,

        v_occupant_norm,
        v_hospital_norm,
        v_ward_norm,
        v_role_norm,
        v_shift_label_norm,

        v_pair_week_ending_date,
        v_pair_contract_id,

        null,
        '[]'::jsonb,

        '{}'::jsonb,
        null,

        false,
        null,
        null,
        null,
        null,

        v_now,
        v_now
      )
      returning timesheet_id into v_new_ts_id;

      v_ensure_timesheet_created_count := v_ensure_timesheet_created_count + 1;
      v_base_week_ts_id := v_new_ts_id;

      insert into tmp_ensure_created_ts_ids(timesheet_id)
      values (v_new_ts_id)
      on conflict do nothing;

      update public.contract_weeks cw0link
      set
        timesheet_id = v_new_ts_id,
        status = case
          when cw0link.status = 'AUTHORISED'::public.contract_week_status_enum then cw0link.status
          else 'SUBMITTED'::public.contract_week_status_enum
        end,
        submission_mode_snapshot = 'MANUAL'::public.submission_mode_enum,
        updated_at = v_now
      where cw0link.id = v_base_week_id;

      -- ✅ NEW: user-facing audit line for "birth of base weekly timesheet" (NHSP)
      perform public._audit_insert(
        'timesheets',
        v_new_ts_id::text,
        'NHSP_IMPORT_TIMESHEET_CREATED',
        null,
        jsonb_build_object(
          'import_id', p_import_id::text,
          'source_system', 'NHSP',
          'kind', 'BASE_WEEKLY',
          'contract_id', v_pair_contract_id::text,
          'contract_week_id', v_base_week_id::text,
          'candidate_id', v_pair_candidate_id::text,
          'client_id', v_pair_client_id::text,
          'week_ending_date', v_pair_week_ending_date::text,
          'booking_id', v_booking_id,
          'active_shifts_count', v_active_count
        ),
        'IMPORT_BIRTH',
        p_actor_user_id
      );

    else
      v_ensure_timesheet_reused_count := v_ensure_timesheet_reused_count + 1;

      update public.contract_weeks cw0keep
      set
        status = case
          when cw0keep.status = 'AUTHORISED'::public.contract_week_status_enum then cw0keep.status
          else 'SUBMITTED'::public.contract_week_status_enum
        end,
        submission_mode_snapshot = 'MANUAL'::public.submission_mode_enum,
        updated_at = v_now
      where cw0keep.id = v_base_week_id;

      if exists (
        select 1
        from public.timesheets identity_target
        where identity_target.timesheet_id=v_base_week_ts_id
          and (
            identity_target.week_ending_date is distinct from v_pair_week_ending_date
            or identity_target.contract_id is distinct from v_pair_contract_id
            or identity_target.occupant_key_norm is distinct from v_occupant_norm
            or identity_target.hospital_norm is distinct from v_hospital_norm
            or identity_target.ward_norm is distinct from v_ward_norm
            or identity_target.job_title_norm is distinct from v_role_norm
          )
      ) then
        select public.import_timesheet_financial_preflight_v1(
          p_timesheet_ids := array[v_base_week_ts_id]::uuid[],
          p_action := 'IMPORT_FINANCIAL_IDENTITY_CHANGE',
          p_actor_user_id := p_actor_user_id,
          p_expected_state_json := '{}'::jsonb,
          p_lock_rows := true,
          p_max_scope := 100
        ) into v_changed_preflight;

        if coalesce((v_changed_preflight->>'allowed')::boolean,false) is not true then
          raise exception using message='IMPORT_FINANCIAL_PREFLIGHT_BLOCKED',errcode='P0001',detail=v_changed_preflight::text;
        end if;

        if v_changed_preflight->>'required_path'='CREATE_OR_UPDATE_CORRECTION_CHAIN' then
          raise exception using message='IMPORT_INVOICED_CORRECTION_REQUIRED',errcode='P0001',
            detail=jsonb_build_object(
              'code','IMPORT_INVOICED_CORRECTION_REQUIRED','timesheet_id',v_base_week_ts_id,
              'reason','FINANCIAL_IDENTITY_CHANGE',
              'required_path','CREATE_OR_UPDATE_CORRECTION_CHAIN'
            )::text;
        elsif v_changed_preflight->>'required_path'='UNAUTHORISE_AMEND_RECALCULATE_REAUTHORISE' then
          raise exception using message='CANONICAL_UNAUTHORISE_REQUIRED',errcode='P0001',
            detail=jsonb_build_object(
              'code','CANONICAL_UNAUTHORISE_REQUIRED','timesheet_id',v_base_week_ts_id,
              'reason','FINANCIAL_IDENTITY_CHANGE',
              'required_path',jsonb_build_array('UNAUTHORISE','AMEND','RECALCULATE','REAUTHORISE'),
              'paid_uninvoiced_rollover_required',false
            )::text;
        elsif v_changed_preflight->>'required_path'='PAID_UNINVOICED_ROLLOVER'
          and not exists (
            select 1 from public.timesheets_financials rollover_identity
            where rollover_identity.timesheet_id=v_base_week_ts_id
              and rollover_identity.is_current=true
              and rollover_identity.stale_reason='IMPORT_PAID_TSFIN_ROLLOVER_PENDING_CALCULATION'
              and coalesce((rollover_identity.policy_snapshot_json->>'requires_frozen_correction_policy')::boolean,false)=true
          ) then
          raise exception using message='PAID_UNINVOICED_ROLLOVER_REQUIRED',errcode='P0001',
            detail=jsonb_build_object(
              'code','PAID_UNINVOICED_ROLLOVER_REQUIRED','timesheet_id',v_base_week_ts_id,
              'reason','FINANCIAL_IDENTITY_CHANGE',
              'required_path',jsonb_build_array(
                'UNAUTHORISE','PAID_UNINVOICED_ROLLOVER','AMEND','RECALCULATE','REAUTHORISE'
              ),
              'invoice_policy_without_history','NOW'
            )::text;
        end if;
      end if;

      update public.timesheets tnorm
      set
        is_current = true,
        status = 'RECEIVED'::public.timesheet_status_enum,
        sheet_scope = 'WEEKLY'::public.timesheet_scope_enum,
        submission_mode = 'MANUAL'::public.submission_mode_enum,
        line_type = 'HOURS'::public.timesheet_line_type_enum,
        week_ending_date = v_pair_week_ending_date,
        contract_id = v_pair_contract_id,
        occupant_key_norm = v_occupant_norm,
        hospital_norm = v_hospital_norm,
        ward_norm = v_ward_norm,
        job_title_norm = v_role_norm,
        shift_label_norm = v_shift_label_norm,
        updated_at = v_now
      where tnorm.timesheet_id = v_base_week_ts_id;
    end if;

    if exists (
      select 1 from public.nhsp_shifts ns_scope
      where ns_scope.source_system = 'NHSP'::public.hr_source_enum
        and ns_scope.cancelled_at_utc is null
        and ns_scope.contract_id = v_pair_contract_id
        and ns_scope.candidate_id = v_pair_candidate_id
        and ns_scope.client_id = v_pair_client_id
        and ns_scope.week_ending_date = v_pair_week_ending_date
        and (
          ns_scope.timesheet_id is null
          or not exists (
            select 1 from public.timesheets existing_link
            where existing_link.timesheet_id=ns_scope.timesheet_id
          )
        )
    ) then
      select public.import_timesheet_financial_preflight_v1(
        p_timesheet_ids := array[v_base_week_ts_id]::uuid[],
        p_action := 'IMPORT_SOURCE_ASSIGNMENT',
        p_actor_user_id := p_actor_user_id,
        p_expected_state_json := '{}'::jsonb,
        p_lock_rows := true,
        p_max_scope := 100
      ) into v_changed_preflight;

      if coalesce((v_changed_preflight->>'allowed')::boolean,false) is not true then
        raise exception using message='IMPORT_FINANCIAL_PREFLIGHT_BLOCKED',errcode='P0001',detail=v_changed_preflight::text;
      end if;

      if v_changed_preflight->>'required_path'='CREATE_OR_UPDATE_CORRECTION_CHAIN' then
        raise exception using message='IMPORT_INVOICED_CORRECTION_REQUIRED',errcode='P0001',
          detail=jsonb_build_object(
            'code','IMPORT_INVOICED_CORRECTION_REQUIRED','timesheet_id',v_base_week_ts_id,
            'reason','FINANCIAL_SOURCE_ASSIGNMENT_CHANGE',
            'required_path','CREATE_OR_UPDATE_CORRECTION_CHAIN'
          )::text;
      end if;

      if exists (
        select 1 from public.timesheets source_target
        left join public.timesheets_financials source_target_tf
          on source_target_tf.timesheet_id=source_target.timesheet_id and source_target_tf.is_current=true
        where source_target.timesheet_id=v_base_week_ts_id
          and (source_target.authorised_at_server is not null or source_target_tf.authorised_at_utc is not null)
          and not exists (
            select 1 from public.timesheets_financials paid_target
            where paid_target.timesheet_id=source_target.timesheet_id
              and paid_target.paid_at_utc is not null
          )
      ) then
        raise exception using message='CANONICAL_UNAUTHORISE_REQUIRED',errcode='P0001',
          detail=jsonb_build_object(
            'code','CANONICAL_UNAUTHORISE_REQUIRED','timesheet_id',v_base_week_ts_id,
            'reason','FINANCIAL_SOURCE_ASSIGNMENT_CHANGE',
            'required_path',jsonb_build_array('UNAUTHORISE','AMEND','RECALCULATE','REAUTHORISE'),
            'paid_uninvoiced_rollover_required',false
          )::text;
      end if;

      if exists (
        select 1 from public.timesheets_financials paid_source
        where paid_source.timesheet_id=v_base_week_ts_id and paid_source.paid_at_utc is not null
      ) and not exists (
        select 1 from public.timesheets_financials rollover_source
        where rollover_source.timesheet_id=v_base_week_ts_id and rollover_source.is_current=true
          and rollover_source.stale_reason='IMPORT_PAID_TSFIN_ROLLOVER_PENDING_CALCULATION'
          and coalesce((rollover_source.policy_snapshot_json->>'requires_frozen_correction_policy')::boolean,false)=true
      ) then
        raise exception using message='PAID_UNINVOICED_ROLLOVER_REQUIRED',errcode='P0001',
          detail=jsonb_build_object(
            'code','PAID_UNINVOICED_ROLLOVER_REQUIRED','timesheet_id',v_base_week_ts_id,
            'reason','FINANCIAL_SOURCE_ASSIGNMENT_CHANGE',
            'required_path',jsonb_build_array(
              'UNAUTHORISE','PAID_UNINVOICED_ROLLOVER','AMEND','RECALCULATE','REAUTHORISE'
            ),
            'invoice_policy_without_history','NOW'
          )::text;
      end if;
    end if;

    update public.nhsp_shifts nsu0
    set
      timesheet_id = v_base_week_ts_id,
      updated_at = v_now
    where nsu0.source_system = 'NHSP'::public.hr_source_enum
      and nsu0.cancelled_at_utc is null
      and nsu0.contract_id = v_pair_contract_id
      and nsu0.candidate_id = v_pair_candidate_id
      and nsu0.client_id = v_pair_client_id
      and nsu0.week_ending_date = v_pair_week_ending_date
      and nsu0.timesheet_id is null;

    get diagnostics v_attached_null_count = row_count;
    v_ensure_shifts_attached_count := v_ensure_shifts_attached_count + coalesce(v_attached_null_count, 0);

    update public.nhsp_shifts nsu1
    set
      timesheet_id = v_base_week_ts_id,
      updated_at = v_now
    where nsu1.source_system = 'NHSP'::public.hr_source_enum
      and nsu1.cancelled_at_utc is null
      and nsu1.contract_id = v_pair_contract_id
      and nsu1.candidate_id = v_pair_candidate_id
      and nsu1.client_id = v_pair_client_id
      and nsu1.week_ending_date = v_pair_week_ending_date
      and nsu1.timesheet_id is not null
      and not exists (
        select 1
        from public.timesheets tmiss
        where tmiss.timesheet_id = nsu1.timesheet_id
        limit 1
      );

    get diagnostics v_relinked_invalid_count = row_count;
    v_ensure_shifts_relinked_invalid_ts_count := v_ensure_shifts_relinked_invalid_ts_count + coalesce(v_relinked_invalid_count, 0);

    select count(*)::int
    into v_active_count
    from public.nhsp_shifts nscheck
    where nscheck.source_system = 'NHSP'::public.hr_source_enum
      and nscheck.cancelled_at_utc is null
      and nscheck.contract_id = v_pair_contract_id
      and nscheck.candidate_id = v_pair_candidate_id
      and nscheck.client_id = v_pair_client_id
      and nscheck.week_ending_date = v_pair_week_ending_date
      and (
        nscheck.timesheet_id is null
        or not exists (
          select 1
          from public.timesheets tchk2
          where tchk2.timesheet_id = nscheck.timesheet_id
          limit 1
        )
      );

    if coalesce(v_active_count, 0) > 0 then
      v_ensure_remaining_active_detached_count := v_ensure_remaining_active_detached_count + v_active_count;
      raise exception
        'nhsp_weekly_apply_transactional: ENSURE invariant failed (active NHSP shifts remain detached or linked to missing timesheets) contract_id=% week_ending_date=% remaining=%.',
        v_pair_contract_id, v_pair_week_ending_date, v_active_count;
    end if;

    insert into tmp_aff_ts(timesheet_id)
    values (v_base_week_ts_id)
    on conflict do nothing;

  end loop;

  select coalesce(jsonb_agg(x.ts_id), '[]'::jsonb)
  into v_ensure_sample_created_ts_ids
  from (
    select tct.timesheet_id::text as ts_id
    from tmp_ensure_created_ts_ids tct
    order by tct.timesheet_id::text
    limit 20
  ) as x;

  v_steps := v_steps || jsonb_build_array(jsonb_build_object(
    'step','ENSURE_BASE_WEEKLY_DONE',
    'ensure_pairs_count', v_ensure_pairs_count,
    'ensure_pairs_skipped_no_active', v_ensure_pairs_skipped_no_active,
    'base_week_created_count', v_ensure_base_week_created_count,
    'base_week_existing_count', v_ensure_base_week_existing_count,
    'base_timesheet_created_count', v_ensure_timesheet_created_count,
    'base_timesheet_reused_count', v_ensure_timesheet_reused_count,
    'missing_timesheet_reference_count', v_ensure_timesheet_missing_reference_count,
    'shifts_attached_null_count', v_ensure_shifts_attached_count,
    'shifts_relinked_invalid_ts_count', v_ensure_shifts_relinked_invalid_ts_count,
    'sample_pairs', v_ensure_sample_pairs,
    'sample_created_ts_ids', v_ensure_sample_created_ts_ids
  ));

  -- ─────────────────────────────────────────────
  -- 9) Compute affected_timesheet_ids (union of ensure + corrections + cancellations + non-invoiced updates)
  -- ─────────────────────────────────────────────
  select coalesce(array_agg(k.external_row_key order by k.external_row_key), array[]::text[])
  into v_force_keys_non_invoiced
  from (
    select distinct fk.external_row_key
    from unnest(coalesce(v_force_keys_final, array[]::text[])) as fk(external_row_key)
    left join unnest(coalesce(v_invoiced_changed_keys, array[]::text[])) as ik(external_row_key)
      on ik.external_row_key = fk.external_row_key
    where ik.external_row_key is null
  ) as k;

  insert into tmp_aff_ts(timesheet_id)
  select (x.value)::uuid
  from jsonb_array_elements_text(coalesce(v_cancellations_result->'affected_timesheet_ids', '[]'::jsonb)) as x(value)
  where nullif(btrim(x.value), '') is not null
  on conflict do nothing;

  insert into tmp_aff_ts(timesheet_id)
  select (x2.value)::uuid
  from jsonb_array_elements_text(coalesce(v_phase3_result->'created_timesheet_ids', '[]'::jsonb)) as x2(value)
  where nullif(btrim(x2.value), '') is not null
  on conflict do nothing;

  insert into tmp_aff_ts(timesheet_id)
  select (x3.value)::uuid
  from jsonb_array_elements_text(coalesce(v_phase3_result->'updated_timesheet_ids', '[]'::jsonb)) as x3(value)
  where nullif(btrim(x3.value), '') is not null
  on conflict do nothing;

  insert into tmp_aff_ts(timesheet_id)
  select distinct ns.timesheet_id
  from public.nhsp_shifts ns
  where ns.source_system = 'NHSP'::public.hr_source_enum
    and ns.cancelled_at_utc is null
    and ns.external_row_key = any(coalesce(v_force_keys_non_invoiced, array[]::text[]))
    and ns.timesheet_id is not null
  on conflict do nothing;

  insert into tmp_aff_ts(timesheet_id)
  select distinct partner.timesheet_id
  from tmp_aff_ts seed
  join public.timesheets seed_ts
    on seed_ts.timesheet_id=seed.timesheet_id
   and seed_ts.is_current=true
   and seed_ts.correction_id is not null
   and upper(btrim(coalesce(seed_ts.adjustment_origin,''))) in (
     'IMPORT_CORRECTION','IMPORT_CANCELLATION','HEALTHROSTER_CHANGED_HOURS',
     'NHSP_CHANGED_HOURS','HEALTHROSTER_CANCELLATION','NHSP_CANCELLATION'
   )
  join public.timesheets partner
    on partner.correction_id=seed_ts.correction_id
   and partner.is_current=true
   and upper(btrim(coalesce(partner.adjustment_origin,''))) in (
     'IMPORT_CORRECTION','IMPORT_CANCELLATION','HEALTHROSTER_CHANGED_HOURS',
     'NHSP_CHANGED_HOURS','HEALTHROSTER_CANCELLATION','NHSP_CANCELLATION'
   )
  on conflict do nothing;

  -- Persist and enqueue complete correction units, including unchanged legs.
  select coalesce(array_agg(distinct a.timesheet_id order by a.timesheet_id), array[]::uuid[])
  into v_affected_timesheet_ids
  from tmp_aff_ts a
  where a.timesheet_id is not null;

  -- Authorised-state restoration and imported financial correction members
  -- are mandatory regardless of the ordinary auto-authorise setting.  This
  -- keeps a reversal/replacement unit authorised together and also covers a
  -- reversal-only cancellation without fabricating a replacement.
  select coalesce(array_agg(distinct required.timesheet_id order by required.timesheet_id),array[]::uuid[])
  into v_reauthorise_timesheet_ids
  from (
    select existing.timesheet_id
    from unnest(coalesce(v_reauthorise_timesheet_ids,array[]::uuid[])) existing(timesheet_id)
    union all
    select correction.timesheet_id
    from unnest(coalesce(v_affected_timesheet_ids,array[]::uuid[])) affected(timesheet_id)
    join public.timesheets correction on correction.timesheet_id=affected.timesheet_id
    where correction.is_current=true
      and correction.revoked_at is null
      and coalesce(correction.is_adjustment,false)
      and correction.correction_id is not null
  ) required
  where required.timesheet_id is not null;

  if to_regclass('pg_temp.import_review_reconciliation_units_v1') is not null then
    if exists(
      select 1 from pg_temp.import_review_reconciliation_units_v1 u
      where u.route in ('AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT') and (
        nullif(u.unit_json->>'correction_id','') is null
        or jsonb_typeof(u.unit_json->'applied_member_ids')<>'array'
        or jsonb_array_length(u.unit_json->'applied_member_ids')<>2
        or (select count(*)=2
              and count(*) filter(where t.correction_kind='CHANGED_HOURS_REVERSAL')=1
              and count(*) filter(where t.correction_kind='CHANGED_HOURS_REPLACEMENT')=1
              and count(distinct t.parent_timesheet_id)=1
              and count(t.parent_timesheet_id)=2
            from public.timesheets t
            where t.correction_id=u.unit_json->>'correction_id' and t.is_current and t.archived_at_utc is null
              and t.adjustment_origin='IMPORT_CORRECTION'
              and t.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')) is not true
      )
    ) then
      raise exception 'IMPORT_REVIEW_APPLY_POSTCONDITION_FAILED' using errcode='55000',
        detail=jsonb_build_object('reason_code','CORRECTION_MEMBER_SET_INCOMPLETE')::text;
    end if;
    select coalesce(array_agg(distinct x.value::uuid order by x.value::uuid) filter (where x.value is not null),array[]::uuid[])
    into v_operation_bound_correction_timesheet_ids
    from pg_temp.import_review_reconciliation_units_v1 u
    left join lateral jsonb_array_elements_text(coalesce(u.unit_json->'applied_member_ids','[]'::jsonb)) x(value) on true
    where u.route in ('AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT');
    select coalesce(jsonb_agg(u.unit_json order by u.action_id),'[]'::jsonb) into v_reconciliation_units
    from pg_temp.import_review_reconciliation_units_v1 u;
    select coalesce(array_agg(x order by x),array[]::uuid[]) into v_reauthorise_timesheet_ids
    from unnest(coalesce(v_reauthorise_timesheet_ids,array[]::uuid[])) x
    where not (x=any(coalesce(v_operation_bound_correction_timesheet_ids,array[]::uuid[])));
  end if;

  v_auto_authorise_timesheet_ids:=public._import_review_auto_authorise_targets_core_v1(
    v_affected_timesheet_ids,'NHSP'::public.hr_source_enum,false
  );
  select coalesce(array_agg(x order by x),array[]::uuid[]) into v_general_authorise_timesheet_ids
  from unnest(coalesce(v_auto_authorise_timesheet_ids,array[]::uuid[])||coalesce(v_reauthorise_timesheet_ids,array[]::uuid[])) x
  where not (x=any(coalesce(v_operation_bound_correction_timesheet_ids,array[]::uuid[])));

  if array_length(v_affected_timesheet_ids, 1) is not null then
    perform public.enqueue_ts_financials_priority(v_affected_timesheet_ids, 'CONTEXT_CHANGED'::public.ts_fin_reason_enum);
  end if;

  if jsonb_array_length(coalesce(v_cancellations_result->'affected_timesheet_ids', '[]'::jsonb)) > 0 then
    create temporary table tmp_cancel_aff_ts(ts_id uuid primary key) on commit drop;

    insert into tmp_cancel_aff_ts(ts_id)
    select distinct (x4.value)::uuid
    from jsonb_array_elements_text(coalesce(v_cancellations_result->'affected_timesheet_ids', '[]'::jsonb)) as x4(value)
    where nullif(btrim(x4.value), '') is not null
    on conflict do nothing;

    select count(*)::int
    into v_cancel_adjustment_count
    from tmp_cancel_aff_ts cts
    join public.timesheets tts
      on tts.timesheet_id = cts.ts_id
    where tts.is_adjustment is true;
  else
    v_cancel_adjustment_count := 0;
  end if;

  v_correction_timesheets_created_count := (v_phase3_created_count + v_phase3_updated_count + coalesce(v_cancel_adjustment_count, 0));

  v_steps := v_steps || jsonb_build_array(jsonb_build_object(
    'step','AFFECTED_TS_DONE',
    'affected_timesheet_ids_count', coalesce(array_length(v_affected_timesheet_ids, 1), 0),
    'cancel_adjustment_count', v_cancel_adjustment_count,
    'correction_timesheets_created_count', v_correction_timesheets_created_count
  ));

  -- ─────────────────────────────────────────────
  -- 10) Preserve the source route.  Whole-import completion is owned by
  -- _import_review_apply_complete_core_v1 only after no work remains.
  -- ─────────────────────────────────────────────
  update public.hr_imports hi3
  set import_scope = 'NHSP'
  where hi3.id = p_import_id;

  v_steps := v_steps || jsonb_build_array(jsonb_build_object('step','IMPORT_BATCH_APPLIED'));

  -- ─────────────────────────────────────────────
  -- 11) Debug audit (invoice_debug gated inside _imp_debug_audit)
  -- ─────────────────────────────────────────────
  perform public._imp_debug_audit(
    p_actor_user_id,
    'NHSP_WEEKLY_APPLY_DEBUG',
    jsonb_build_object(
      'import_id', p_import_id::text,
      'steps', v_steps,

      'selected_action_ids_count', v_selected_action_ids_count,
      'selected_row_keys_count', v_selected_row_keys_count,
      'selected_cancel_shift_ids_count', v_selected_cancel_shift_ids_count,
      'selected_action_ids_sample', v_sample_selected_action_ids,

      'ok_keys_total', v_ok_keys_total,

      'phase1_called', v_should_run_phase1,
      'phase1_force_keys_count', v_force_keys_count,
      'phase1_skip_keys_count', v_skip_keys_count,
      'phase1_force_keys_sample', v_sample_force_keys,
      'phase1_skip_keys_sample', v_sample_skip_keys,
      'phase1_shifts_created', v_phase1_shifts_created,
      'phase1_shifts_updated', v_phase1_shifts_updated,

      'phase15_called', v_should_run_phase15,
      'phase15_ok_rows', v_phase15_ok,
      'phase15_shift_updated_rows', v_phase15_updated,

      'invoiced_changed_keys_count', v_invoiced_changed_keys_count,
      'not_invoiced_changed_keys_count', v_not_invoiced_changed_keys_count,
      'phase3_called', v_should_run_phase3,
      'phase3_created_count', v_phase3_created_count,
      'phase3_updated_count', v_phase3_updated_count,

      'cancellations_called', v_should_run_cancellations,
      'cancellations_count', v_cancellations_count,
      'sample_cancel_shift_ids', v_sample_cancel_shift_ids,

      'ensure_pairs_count', v_ensure_pairs_count,
      'ensure_pairs_skipped_no_active', v_ensure_pairs_skipped_no_active,
      'ensure_base_week_created_count', v_ensure_base_week_created_count,
      'ensure_base_week_existing_count', v_ensure_base_week_existing_count,
      'ensure_timesheet_created_count', v_ensure_timesheet_created_count,
      'ensure_timesheet_reused_count', v_ensure_timesheet_reused_count,
      'ensure_timesheet_missing_reference_count', v_ensure_timesheet_missing_reference_count,
      'ensure_shifts_attached_null_count', v_ensure_shifts_attached_count,
      'ensure_shifts_relinked_invalid_ts_count', v_ensure_shifts_relinked_invalid_ts_count,
      'ensure_sample_pairs', v_ensure_sample_pairs,
      'ensure_sample_created_ts_ids', v_ensure_sample_created_ts_ids,

      'cancel_adjustment_count', v_cancel_adjustment_count,
      'correction_timesheets_created_count', v_correction_timesheets_created_count,

      'affected_timesheet_ids_count', coalesce(array_length(v_affected_timesheet_ids, 1), 0)
    ),
    'hr_imports',
    p_import_id::text,
    null,
    null,
    null,
    null
  );

  v_review_result:=jsonb_build_object(
    'import_id', p_import_id,
    'mode_b', jsonb_build_object(
      'selected_truth_keys', to_jsonb(coalesce(v_selected_truth_keys_ok, array[]::text[])),
      'force_overwrite_external_row_keys', to_jsonb(coalesce(v_force_keys_final, array[]::text[])),
      'skip_external_row_keys', to_jsonb(coalesce(v_skip_keys_final, array[]::text[])),
      'phase3', v_phase3_result,
      'phase1', v_phase1_result,
      'phase15', jsonb_build_object(
        'ok_rows', v_phase15_ok,
        'shift_updated_rows', v_phase15_updated
      ),
      'cancellations', v_cancellations_result
    ),
    'affected_timesheet_ids', to_jsonb(coalesce(v_affected_timesheet_ids, array[]::uuid[])),
    'auto_authorise_timesheet_ids',to_jsonb(coalesce(v_auto_authorise_timesheet_ids,array[]::uuid[])),
    'post_commit_reauthorise_timesheet_ids',to_jsonb(coalesce(v_reauthorise_timesheet_ids,array[]::uuid[])),
    'reconciliation_action_ids',to_jsonb(coalesce(v_reconciliation_action_ids,array[]::text[])),
    'operation_bound_correction_action_ids',to_jsonb(coalesce(v_operation_bound_correction_action_ids,array[]::text[])),
    'general_authorise_timesheet_ids',to_jsonb(coalesce(v_general_authorise_timesheet_ids,array[]::uuid[])),
    'operation_bound_correction_timesheet_ids',to_jsonb(coalesce(v_operation_bound_correction_timesheet_ids,array[]::uuid[])),
    'reconciliation_units',coalesce(v_reconciliation_units,'[]'::jsonb),
    'post_commit_email_action_ids','[]'::jsonb,
    'review_operation_id',v_review_operation_id
  );
  perform public._import_review_apply_complete_core_v1(p_import_id,v_review_operation_id,p_actor_user_id,v_review_result,
    cardinality(v_affected_timesheet_ids)>0);
  return v_review_result;

exception when others then
  get stacked diagnostics v_sqlstate = returned_sqlstate, v_err = message_text;

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'NHSP_WEEKLY_APPLY_ERROR',
      jsonb_build_object(
        'import_id', p_import_id::text,
        'steps', v_steps,
        'sqlstate', v_sqlstate,
        'error', v_err,

        'selected_action_ids_count', v_selected_action_ids_count,
        'selected_row_keys_count', v_selected_row_keys_count,
        'selected_cancel_shift_ids_count', v_selected_cancel_shift_ids_count,
        'selected_action_ids_sample', v_sample_selected_action_ids
      ),
      'hr_imports',
      p_import_id::text,
      null,
      null,
      null,
      null
    );
  exception when others then
    null;
  end;

  raise;
end;
$function$;

CREATE OR REPLACE FUNCTION public.hr_weekly_apply_transactional(p_import_id uuid, p_payload jsonb, p_actor_user_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
 SET "plpgsql_check.mode" TO 'disabled'
AS $function$
declare
  v_now timestamptz := now();

  -- import header
  v_import_source_system text;
  v_import_client_id uuid;

  -- payload parts
  v_payload jsonb := coalesce(p_payload, '{}'::jsonb);
  v_actions_json jsonb := '[]'::jsonb;

  -- Expanded only from persisted, selected review decisions.
  v_invalidation_actions jsonb := '[]'::jsonb;
  v_invalidation_actions_count int := 0;

  -- normalized selections
  v_selected_action_ids text[] := array[]::text[];
  v_selected_truth_keys text[] := array[]::text[];
  v_selected_cancel_shift_ids uuid[] := array[]::uuid[];

  -- derived mode key sets
  v_mode_a_external_keys text[] := array[]::text[];
  v_mode_b_external_keys text[] := array[]::text[];

  -- selected truth keys constrained to MODE_B
  v_selected_truth_keys_mode_b text[] := array[]::text[];

  -- Mode B tick-only enforced lists
  v_force_keys_final text[] := array[]::text[];
  v_skip_keys_final text[] := array[]::text[];

  -- changed-hours partition (selected keys only, MODE_B)
  v_invoiced_changed_keys text[] := array[]::text[];
  v_not_invoiced_changed_keys text[] := array[]::text[];
  v_protected_source_timesheet_ids uuid[] := array[]::uuid[];
  v_force_keys_non_invoiced text[] := array[]::text[];

  v_phase3_result jsonb := null;
  v_changed_preflight jsonb := null;
  v_changed_timesheet_ids uuid[] := array[]::uuid[];
  v_reauthorise_timesheet_ids uuid[] := array[]::uuid[];
  v_lifecycle_items jsonb := '[]'::jsonb;
  v_unauthorise_result jsonb := null;
  v_reconciliation_action_ids text[] := array[]::text[];
  v_operation_bound_correction_action_ids text[] := array[]::text[];
  v_operation_bound_correction_keys text[] := array[]::text[];
  v_operation_bound_correction_timesheet_ids uuid[] := array[]::uuid[];
  v_general_authorise_timesheet_ids uuid[] := array[]::uuid[];
  v_reconciliation_transition jsonb := null;
  v_reconciliation_units jsonb := '[]'::jsonb;

  -- Phase 1 / 1.5 (MODE_B)
  v_phase1_result jsonb := null;
  v_phase15_ok int := 0;
  v_phase15_updated int := 0;

  -- cancellations (MODE_B)
  v_cancel_actions jsonb := '[]'::jsonb;
  v_cancellations_result jsonb := null;

  -- mirror (MODE_A)
  v_mirror_result jsonb := null;

  -- validation (MODE_A)
  v_weekly_val_payload jsonb := null;
  v_validations_upserted int := 0;
  v_mismatched_tsids uuid[] := array[]::uuid[];

  -- ✅ NEW: validation-changed timesheets (MODE_A) that must trigger TSFIN recompute
  v_validation_changed_timesheet_ids uuid[] := array[]::uuid[];

  -- ✅ NEW: count of ref clears due to missing shifts (MODE_A)
  v_mode_a_ref_cleared_count int := 0;

  -- ✅ NEW: count of ref sets due to matched shifts (MODE_A)
  v_mode_a_ref_set_count int := 0;

  -- ✅ NEW: timesheets whose reference truth changed (for post-apply QR reissue + regen)
  v_ref_updated_timesheet_ids uuid[] := array[]::uuid[];
  v_ref_updated_timesheet_ids_count int := 0;

  -- ✅ NEW: consolidated email jobs + items
  v_email_jobs jsonb := '[]'::jsonb;

  -- affected timesheets for TSFIN drain (MODE_B + MODE_A validation changes)
  v_affected_timesheet_ids uuid[] := array[]::uuid[];
  -- MODE_B targets are ordinary authoritative work.  MODE_A targets are kept
  -- in a separate array and enter auto-authorisation only after the stricter
  -- complete whole-timesheet validation gate below.
  v_authoritative_affected_timesheet_ids uuid[] := array[]::uuid[];
  v_validation_auto_authorise_timesheet_ids uuid[] := array[]::uuid[];
  v_auto_authorise_timesheet_ids uuid[] := array[]::uuid[];

  -- policy A replacement-day
  v_selected_cancel_shift_id_set text[] := array[]::text[];

  -- debug counts
  v_steps jsonb := '[]'::jsonb;

  v_selected_action_ids_count int := 0;
  v_selected_row_keys_count int := 0;
  v_selected_cancel_shift_ids_count int := 0;

  v_mode_a_ok_keys_total int := 0;
  v_mode_b_ok_keys_total int := 0;

  v_force_keys_count int := 0;
  v_skip_keys_count int := 0;

  v_invoiced_changed_keys_count int := 0;
  v_not_invoiced_changed_keys_count int := 0;

  v_cancellations_count int := 0;

  v_phase3_created_count int := 0;
  v_phase3_updated_count int := 0;
  v_cancel_adjustment_count int := 0;
  v_correction_timesheets_created_count int := 0;

  v_val_rows_count int := 0;
  v_email_actions_count int := 0;
  v_email_jobs_count int := 0;

  v_sample_force_keys jsonb := '[]'::jsonb;
  v_sample_cancel_shift_ids jsonb := '[]'::jsonb;
  v_selected_action_ids_sample jsonb := '[]'::jsonb;

  v_mode_b_phase1_called boolean := false;
  v_mode_b_phase15_called boolean := false;
  v_mode_b_cancellations_called boolean := false;
  v_mode_b_phase3_called boolean := false;

  v_mode_b_should_run_phase1 boolean := false;
  v_mode_b_should_run_phase15 boolean := false;
  v_mode_b_should_run_cancellations boolean := false;
  v_mode_b_should_run_phase3 boolean := false;

  -- Server-owned review contract. A review state is mandatory for every call.
  v_review_contract jsonb := coalesce(v_payload->'review_contract','{}'::jsonb);
  v_review_selected_ids jsonb := coalesce(v_payload->'review_selected_action_ids','[]'::jsonb);
  v_review_operation_id uuid;
  v_review_guard jsonb;
  v_review_result jsonb;
  v_post_commit_email_action_ids jsonb := '[]'::jsonb;

  v_phase1_shifts_created int := null;
  v_phase1_shifts_updated int := null;

  v_last_shift_id uuid := null;

  -- ─────────────────────────────────────────────
  -- ✅ ENSURE BASE WEEKLY TIMESHEET + ATTACH ACTIVE HEALTHROSTER SHIFTS (invariant)
  -- ─────────────────────────────────────────────
  v_ensure_pairs_count int := 0;
  v_ensure_pairs_skipped_no_active int := 0;

  v_ensure_base_week_created_count int := 0;
  v_ensure_base_week_existing_count int := 0;

  v_ensure_timesheet_created_count int := 0;
  v_ensure_timesheet_reused_count int := 0;
  v_ensure_timesheet_missing_reference_count int := 0;

  v_ensure_shifts_attached_count int := 0;
  v_ensure_shifts_relinked_invalid_ts_count int := 0;
  v_ensure_remaining_active_detached_count int := 0;

  -- ✅ NEW: MODE_A shift→timesheet linking (fix evidence + HR crosscheck + ref propagation)
  v_mode_a_shifts_attached_count int := 0;
  v_mode_a_ts_linked_count int := 0;

  v_ensure_sample_pairs jsonb := '[]'::jsonb;
  v_ensure_sample_created_ts_ids jsonb := '[]'::jsonb;

  -- loop vars for ensure
  v_pair_contract_id uuid;
  v_pair_candidate_id uuid;
  v_pair_client_id uuid;
  v_pair_week_ending_date date;

  v_active_count int := 0;

  v_base_week_id uuid := null;
  v_base_week_ts_id uuid := null;

  v_ts_exists boolean := false;

  v_candidate_display_name text := null;
  v_candidate_tms_ref text := null;
  v_client_name text := null;
  v_contract_display_site text := null;
  v_contract_ward_hint text := null;
  v_contract_role text := null;

  v_occupant_norm text := null;
  v_hospital_norm text := null;
  v_ward_norm text := null;
  v_role_norm text := null;

  v_booking_base text := null;
  v_hash_hex text := null;
  v_booking_id text := null;
  v_shift_label_norm text := null;

  v_new_ts_id uuid := null;

  v_attached_null_count int := 0;
  v_relinked_invalid_count int := 0;

  v_sqlstate text;
  v_err text;
begin
  v_steps := v_steps || jsonb_build_array(jsonb_build_object('step','START'));

  -- ─────────────────────────────────────────────
  -- 0) Validate import + header fields
  -- ─────────────────────────────────────────────
  select
    upper(coalesce(hi.source_system::text, '')),
    hi.client_id
  into
    v_import_source_system,
    v_import_client_id
  from public.hr_imports hi
  where hi.id = p_import_id
  limit 1;

  if v_import_source_system is null or v_import_source_system = '' then
    raise exception 'hr_weekly_apply_transactional: import % not found in hr_imports.', p_import_id;
  end if;

  if v_import_source_system <> 'HEALTHROSTER' then
    raise exception 'hr_weekly_apply_transactional: import % source_system=%; expected HEALTHROSTER.', p_import_id, v_import_source_system;
  end if;

  if v_import_client_id is null then
    raise exception 'hr_weekly_apply_transactional: import % missing client_id.', p_import_id;
  end if;

  if not exists(select 1 from public.import_review_states where import_id=p_import_id) then
    raise exception 'IMPORT_REVIEW_REQUIRED' using errcode='55000';
  end if;
  if jsonb_typeof(v_payload)<>'object' then
    raise exception 'IMPORT_REVIEW_APPLY_PAYLOAD_INVALID' using errcode='22023';
  end if;
  if exists(select 1 from jsonb_object_keys(v_payload) as keys(key_name)
    where keys.key_name not in ('review_contract','review_selected_action_ids','invalidation_action_ids')) then
    raise exception 'IMPORT_REVIEW_BROWSER_AUTHORITY_REJECTED' using errcode='22023';
  end if;
  if jsonb_typeof(v_review_contract)<>'object' or jsonb_typeof(v_review_selected_ids)<>'array'
    or not(v_payload?'invalidation_action_ids') or jsonb_typeof(v_payload->'invalidation_action_ids')<>'array' then
    raise exception 'IMPORT_REVIEW_APPLY_CONTRACT_REQUIRED' using errcode='22023';
  end if;
  v_review_operation_id:=(v_review_contract->>'operation_id')::uuid;
  v_review_guard:=public.import_review_apply_guard_v1(p_import_id,(v_review_contract->>'state_version')::bigint,
    v_review_contract->>'coverage_fingerprint',v_review_contract->>'preview_fingerprint',v_review_operation_id,
    v_review_contract->>'request_hash',v_review_selected_ids,v_payload->'invalidation_action_ids',p_actor_user_id);
  if coalesce((v_review_guard->>'replay')::boolean,false) then return v_review_guard->'stored_response'; end if;

  -- The server guard has reduced the request to complete, ready
  -- candidate/client units.  Keep that boundary available to every MODE_A
  -- validation/mirror step; otherwise a partial batch could validate or link
  -- rows belonging to a candidate that the operator deliberately left
  -- pending.
  drop table if exists pg_temp.tmp_review_batch_units;
  create temporary table tmp_review_batch_units(
    candidate_id uuid not null,
    client_id uuid not null,
    primary key(candidate_id,client_id)
  ) on commit drop;
  insert into tmp_review_batch_units(candidate_id,client_id)
  select distinct d.candidate_id,d.client_id
  from public.import_review_decisions d
  where d.import_id=p_import_id and d.is_current
    and d.action_id in (select jsonb_array_elements_text(v_review_guard->'selected_action_ids'))
    and d.candidate_id is not null and d.client_id is not null
  on conflict do nothing;
  if not exists(select 1 from tmp_review_batch_units) then
    raise exception 'IMPORT_REVIEW_BATCH_SCOPE_EMPTY' using errcode='55000';
  end if;
  select coalesce(jsonb_agg(to_jsonb(case when d.action_kind='APPLY_CANCELLATION' then 'CANCEL:'||d.shift_id::text else 'ROW:'||d.source_identity end) order by d.action_id),'[]'::jsonb)
    into v_actions_json
  from public.import_review_decisions d
  where d.import_id=p_import_id and d.is_current and d.selected
    and d.action_id in (select jsonb_array_elements_text(v_review_guard->'selected_action_ids'))
    and d.action_kind in ('INCLUDE_SHIFT','APPLY_AMENDMENT','APPLY_CANCELLATION');
  select coalesce(jsonb_agg(jsonb_build_object('timesheet_id',d.timesheet_id,'comparison_key',d.source_identity,'invalidate',true) order by d.action_id),'[]'::jsonb)
    into v_invalidation_actions
  from public.import_review_decisions d
  where d.import_id=p_import_id and d.is_current and d.selected
    and d.action_id in (select jsonb_array_elements_text(v_review_guard->'selected_action_ids'))
    and d.action_kind='INVALIDATE_REFERENCE';
  select coalesce(jsonb_agg(to_jsonb(d.action_id) order by d.action_id),'[]'::jsonb)
    into v_post_commit_email_action_ids
  from public.import_review_decisions d
  where d.import_id=p_import_id and d.is_current and d.selected
    and d.action_id in (select jsonb_array_elements_text(v_review_guard->'selected_action_ids'))
    and d.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER');
  v_email_actions_count:=jsonb_array_length(v_post_commit_email_action_ids);

  v_steps := v_steps || jsonb_build_array(jsonb_build_object('step','IMPORT_OK','client_id',v_import_client_id::text));

  -- ─────────────────────────────────────────────
  -- 1) Parse and normalize selection payload (ROW:/CANCEL:)
  -- ─────────────────────────────────────────────
  if jsonb_typeof(v_actions_json) <> 'array' then
    raise exception 'hr_weekly_apply_transactional: selected_action_ids must be a JSON array.';
  end if;

  if jsonb_typeof(v_invalidation_actions) <> 'array' then
    raise exception 'hr_weekly_apply_transactional: invalidation_actions must be a JSON array.';
  end if;

  v_invalidation_actions_count := jsonb_array_length(v_invalidation_actions);

  create temporary table tmp_sel_ids(
    action_id text primary key
  ) on commit drop;

  insert into tmp_sel_ids(action_id)
  select distinct nullif(btrim(x.value), '')
  from jsonb_array_elements_text(v_actions_json) as x(value)
  where nullif(btrim(x.value), '') is not null
  on conflict do nothing;

  if exists (
    select 1
    from tmp_sel_ids s
    where s.action_id !~ '^(ROW|CANCEL):'
  ) then
    raise exception 'hr_weekly_apply_transactional: invalid action_id in selection (expected ROW:<external_row_key> or CANCEL:<shift_id>).';
  end if;

  select coalesce(array_agg(s.action_id order by s.action_id), array[]::text[])
  into v_selected_action_ids
  from tmp_sel_ids s;

  select coalesce(array_agg(distinct substring(s.action_id from 5) order by substring(s.action_id from 5)), array[]::text[])
  into v_selected_truth_keys
  from tmp_sel_ids s
  where s.action_id like 'ROW:%';

  select coalesce(array_agg(distinct (substring(s.action_id from 8))::uuid order by (substring(s.action_id from 8))::uuid), array[]::uuid[])
  into v_selected_cancel_shift_ids
  from tmp_sel_ids s
  where s.action_id like 'CANCEL:%';

  v_selected_action_ids_count := coalesce(array_length(v_selected_action_ids, 1), 0);
  v_selected_row_keys_count := coalesce(array_length(v_selected_truth_keys, 1), 0);
  v_selected_cancel_shift_ids_count := coalesce(array_length(v_selected_cancel_shift_ids, 1), 0);

  select to_jsonb(coalesce(array_agg(x.a), array[]::text[]))
  into v_selected_action_ids_sample
  from (
    select a as a
    from unnest(coalesce(v_selected_action_ids, array[]::text[])) as a
    order by a
    limit 20
  ) as x;

  v_steps := v_steps || jsonb_build_array(
    jsonb_build_object(
      'step','SELECTION_PARSED',
      'selected_action_ids_count', v_selected_action_ids_count,
      'selected_row_keys_count', v_selected_row_keys_count,
      'selected_cancel_shift_ids_count', v_selected_cancel_shift_ids_count,
      'email_actions_count', v_email_actions_count,
      'invalidation_actions_count', v_invalidation_actions_count,
      'selected_action_ids_sample', v_selected_action_ids_sample
    )
  );

  -- ─────────────────────────────────────────────
  -- ✅ tmp_aff_ts must exist early (PK + ON CONFLICT supported)
  -- ─────────────────────────────────────────────
  drop table if exists pg_temp.tmp_aff_ts;
  create temporary table tmp_aff_ts(
    timesheet_id uuid primary key
  ) on commit drop;

  -- ✅ NEW: reference-updated timesheets (used for QR reissue + regen decisions)
  drop table if exists pg_temp.tmp_ref_updated_ts;
  create temporary table tmp_ref_updated_ts(
    timesheet_id uuid primary key
  ) on commit drop;

  -- ─────────────────────────────────────────────
  -- 2) Load weekly_import_phase2 + compute per-group authority through the
  --    shared current-setting core used by staging and catalogue generation.
  -- ─────────────────────────────────────────────
  create temporary table tmp_p2_all on commit drop as
  select *
  from public.weekly_import_phase2(p_import_id := p_import_id, p_system_type := 'HR_WEEKLY');

  create temporary table tmp_p2_ok on commit drop as
  select
    p2.external_row_key,
    p2.candidate_id,
    p2.client_id,
    p2.contract_id,
    p2.week_ending_date,
    p2.work_date,
    upper(coalesce(p2.action::text,'')) as action
  from tmp_p2_all p2
  where upper(coalesce(p2.action::text,'')) = 'OK'
    and p2.external_row_key is not null
    and p2.contract_id is not null
    and p2.candidate_id is not null
    and p2.client_id is not null
    and p2.week_ending_date is not null;

  create temporary table tmp_group_mode on commit drop as
  select distinct
    t.contract_id,
    t.candidate_id,
    t.client_id,
    t.week_ending_date,
    ('grp:' || t.contract_id::text || ':' || t.week_ending_date::text || ':' || t.candidate_id::text) as group_id,
    case a.authority_mode when 'AUTHORITATIVE' then 'MODE_B'
      when 'VALIDATION_ONLY' then 'MODE_A' else 'OUT_OF_SCOPE' end as mode
  from (
    select distinct p2ok.contract_id, p2ok.candidate_id, p2ok.client_id, p2ok.week_ending_date
    from tmp_p2_ok p2ok
  ) as t
  join public.contracts c
    on c.id = t.contract_id
  cross join lateral public._import_review_effective_authority_core_v1(
    'HR_WEEKLY',c.id,c.client_id,t.week_ending_date) a;

  if exists(select 1 from tmp_group_mode gm join tmp_review_batch_units bu
      on bu.candidate_id=gm.candidate_id and bu.client_id=gm.client_id
      where gm.mode='OUT_OF_SCOPE') then
    raise exception 'HR_WEEKLY_IMPORT_AUTHORITY_OUT_OF_SCOPE' using errcode='40001';
  end if;

  create temporary table tmp_p2_ok_mode on commit drop as
  select
    p2ok.external_row_key,
    p2ok.candidate_id,
    p2ok.client_id,
    p2ok.contract_id,
    p2ok.week_ending_date,
    p2ok.work_date,
    gm.group_id,
    gm.mode
  from tmp_p2_ok p2ok
  join tmp_group_mode gm
    on gm.contract_id = p2ok.contract_id
   and gm.candidate_id = p2ok.candidate_id
   and gm.week_ending_date = p2ok.week_ending_date;

  select coalesce(array_agg(distinct m.external_row_key order by m.external_row_key), array[]::text[])
  into v_mode_a_external_keys
  from tmp_p2_ok_mode m
  join tmp_review_batch_units bu
    on bu.candidate_id=m.candidate_id and bu.client_id=m.client_id
  where m.mode = 'MODE_A';

  select coalesce(array_agg(distinct m.external_row_key order by m.external_row_key), array[]::text[])
  into v_mode_b_external_keys
  from tmp_p2_ok_mode m
  where m.mode = 'MODE_B';

  v_mode_a_ok_keys_total := coalesce(array_length(v_mode_a_external_keys, 1), 0);
  v_mode_b_ok_keys_total := coalesce(array_length(v_mode_b_external_keys, 1), 0);

  if exists (
    select 1
    from unnest(coalesce(v_selected_truth_keys, array[]::text[])) as k(external_row_key)
    left join (select distinct mb.external_row_key from tmp_p2_ok_mode mb where mb.mode = 'MODE_B') as mbok
      on mbok.external_row_key = k.external_row_key
    where mbok.external_row_key is null
  ) then
    raise exception 'hr_weekly_apply_transactional: selection includes ROW:<external_row_key> that is not MODE_B (timesheet required).';
  end if;

  select coalesce(array_agg(k.external_row_key order by k.external_row_key), array[]::text[])
  into v_selected_truth_keys_mode_b
  from (
    select distinct k.external_row_key
    from unnest(coalesce(v_selected_truth_keys, array[]::text[])) as k(external_row_key)
    join (select distinct mb.external_row_key from tmp_p2_ok_mode mb where mb.mode = 'MODE_B') as mbok
      on mbok.external_row_key = k.external_row_key
  ) as k;

  v_steps := v_steps || jsonb_build_array(
    jsonb_build_object(
      'step','PHASE2_OK_LOADED',
      'mode_a_ok_keys_total', v_mode_a_ok_keys_total,
      'mode_b_ok_keys_total', v_mode_b_ok_keys_total
    )
  );

  -- ─────────────────────────────────────────────
  -- 3) MODE_B tick = PROCEED (no decisions)
  -- ─────────────────────────────────────────────
  v_force_keys_final := coalesce(v_selected_truth_keys_mode_b, array[]::text[]);

  select coalesce(array_agg(x.external_row_key order by x.external_row_key), array[]::text[])
  into v_skip_keys_final
  from (
    select distinct okk.external_row_key
    from unnest(coalesce(v_mode_b_external_keys, array[]::text[])) as okk(external_row_key)
    left join unnest(coalesce(v_force_keys_final, array[]::text[])) as fk(external_row_key)
      on fk.external_row_key = okk.external_row_key
    where fk.external_row_key is null
  ) as x;

  v_force_keys_count := coalesce(array_length(v_force_keys_final, 1), 0);
  v_skip_keys_count := coalesce(array_length(v_skip_keys_final, 1), 0);
  v_cancellations_count := coalesce(array_length(v_selected_cancel_shift_ids, 1), 0);

  -- Only MODE_B is import-authoritative.  Ignore unrelated expense-only
  -- timesheets, but refuse to reuse a base contract-week timesheet occupied by
  -- calculated expenses or to amend/reverse an imported shift whose own linked
  -- timesheet contains them.  This runs before any source mutation.
  if exists (
    select 1
    from (
      select cw.timesheet_id
      from tmp_p2_ok_mode p2
      join public.contract_weeks cw
        on cw.contract_id=p2.contract_id
       and cw.week_ending_date=p2.week_ending_date
       and cw.is_adjustment=false
       and coalesce(cw.additional_seq,0)=0
      where p2.mode='MODE_B'
        and p2.external_row_key=any(coalesce(v_force_keys_final,array[]::text[]))
        and cw.timesheet_id is not null
        and not exists (
          select 1
          from public.nhsp_shifts existing_import_shift
          where existing_import_shift.source_system='HEALTHROSTER'::public.hr_source_enum
            and existing_import_shift.client_id=v_import_client_id
            and existing_import_shift.external_row_key=p2.external_row_key
            and existing_import_shift.cancelled_at_utc is null
        )
      union
      select ns.timesheet_id
      from public.nhsp_shifts ns
      where ns.source_system='HEALTHROSTER'::public.hr_source_enum
        and ns.client_id=v_import_client_id
        and ns.timesheet_id is not null
        and (
          ns.external_row_key=any(coalesce(v_force_keys_final,array[]::text[]))
          or ns.id=any(coalesce(v_selected_cancel_shift_ids,array[]::uuid[]))
        )
    ) expense_target
    where public._import_review_timesheet_has_calculated_expenses_core_v1(expense_target.timesheet_id)
  ) then
    raise exception using
      message='IMPORT_AUTHORITATIVE_EXPENSE_SEPARATION_REQUIRED',
      errcode='P0001',
      detail=jsonb_build_object(
        'code','IMPORT_AUTHORITATIVE_EXPENSE_SEPARATION_REQUIRED',
        'message','Timesheet occupied by expenses. Remove the expenses from this timesheet, save or recalculate it, then choose Recheck. Expenses must be invoiced on a separate timesheet for import-authoritative work; no import mutation was applied.'
      )::text;
  end if;

  v_mode_b_should_run_phase1 := (v_force_keys_count > 0);
  v_mode_b_should_run_phase15 := (v_force_keys_count > 0);
  v_mode_b_should_run_cancellations := (v_cancellations_count > 0);

  v_steps := v_steps || jsonb_build_array(
    jsonb_build_object(
      'step','TICK_PROCEED_KEYS_READY',
      'mode_b_force_keys_count', v_force_keys_count,
      'mode_b_skip_keys_count', v_skip_keys_count,
      'mode_b_cancellations_count', v_cancellations_count
    )
  );

  -- ─────────────────────────────────────────────
  -- 4) MODE_B: do NOT run truth mutation work when there is nothing to apply
  -- ─────────────────────────────────────────────
  if (v_mode_b_should_run_phase1 is false) and (v_mode_b_should_run_cancellations is false) then
    v_steps := v_steps || jsonb_build_array(
      jsonb_build_object(
        'step','MODE_B_NOOP_GUARD',
        'reason','NO_SELECTION_NO_CANCELLATION => SKIP_MODE_B_TRUTH_MUTATION',
        'should_run_phase1', false,
        'should_run_phase15', false,
        'should_run_phase3', false,
        'should_run_cancellations', false
      )
    );
  else
    -- MODE_B PHASE3 / PHASE1 / PHASE1.5 / CANCELLATIONS BLOCKS
    create temporary table tmp_changed_sel on commit drop as
    select
      ch.external_row_key,
      ch.timesheet_id,
      ch.is_paid,
      ch.is_invoiced
    from public.weekly_import_changed_hours_phase3(p_import_id := p_import_id, p_system_type := 'HEALTHROSTER') as ch
    where ch.external_row_key = any(coalesce(v_force_keys_final, array[]::text[]));

    if to_regclass('pg_temp.import_review_reconciliation_units_v1') is not null then
      if exists(select 1 from pg_temp.import_review_reconciliation_units_v1 u
        where u.unit_json->>'source_system'<>'HEALTHROSTER'
           or u.unit_json->>'schema_version'<>'IMPORT_AUTHORITATIVE_RECONCILIATION_V1'
           or u.route not in ('AMEND_SOURCE','AMEND_PAID_UNINVOICED_SOURCE','AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT')) then
        raise exception 'IMPORT_REVIEW_RECONCILIATION_UNIT_INVALID' using errcode='22023';
      end if;
      select coalesce(array_agg(u.action_id order by u.action_id),array[]::text[]),
        coalesce(array_agg(u.action_id order by u.action_id) filter(where u.route in ('AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT')),array[]::text[]),
        coalesce(array_agg(u.source_identity order by u.source_identity) filter(where u.route in ('AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT')),array[]::text[])
      into v_reconciliation_action_ids,v_operation_bound_correction_action_ids,v_operation_bound_correction_keys
      from pg_temp.import_review_reconciliation_units_v1 u;
    end if;

    select coalesce(array_agg(cs.external_row_key order by cs.external_row_key), array[]::text[])
    into v_invoiced_changed_keys
    from tmp_changed_sel cs
    where cs.is_invoiced is true
       or cs.is_paid is true;

    select coalesce(array_agg(cs.external_row_key order by cs.external_row_key), array[]::text[])
    into v_not_invoiced_changed_keys
    from tmp_changed_sel cs
    where cs.is_invoiced is false
      and cs.is_paid is false;

    if cardinality(v_reconciliation_action_ids)>0 then
      select coalesce(array_agg(u.source_identity order by u.source_identity)
        filter(where u.route in ('AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT')),array[]::text[]),
        coalesce(array_agg(u.source_identity order by u.source_identity)
        filter(where u.route in ('AMEND_SOURCE','AMEND_PAID_UNINVOICED_SOURCE')),array[]::text[])
      into v_invoiced_changed_keys,v_not_invoiced_changed_keys
      from pg_temp.import_review_reconciliation_units_v1 u;
      if cardinality(v_operation_bound_correction_action_ids)>0 then
        v_reconciliation_transition:=public.import_review_correction_generation_transition_v1(
          p_import_id,v_review_operation_id,v_review_contract->>'request_hash','PREPARE',p_actor_user_id,
          v_operation_bound_correction_action_ids,v_now);
      end if;
    end if;

    v_invoiced_changed_keys_count := coalesce(array_length(v_invoiced_changed_keys, 1), 0);
    v_not_invoiced_changed_keys_count := coalesce(array_length(v_not_invoiced_changed_keys, 1), 0);

    select coalesce(array_agg(distinct cs.timesheet_id order by cs.timesheet_id), array[]::uuid[])
      into v_changed_timesheet_ids
    from tmp_changed_sel cs
    where cs.timesheet_id is not null;

    select coalesce(array_agg(distinct cs.timesheet_id order by cs.timesheet_id),array[]::uuid[])
      into v_protected_source_timesheet_ids
    from tmp_changed_sel cs
    where cs.timesheet_id is not null
      and (cs.is_invoiced is true or cs.is_paid is true);

    if coalesce(array_length(v_changed_timesheet_ids, 1), 0) > 0 then
      select public.import_timesheet_financial_preflight_v1(
        p_timesheet_ids := v_changed_timesheet_ids,
        p_action := 'IMPORT_CHANGED_HOURS',
        p_actor_user_id := p_actor_user_id,
        p_expected_state_json := '{}'::jsonb,
        p_lock_rows := true,
        p_max_scope := 100
      ) into v_changed_preflight;

      if coalesce((v_changed_preflight->>'allowed')::boolean, false) is not true then
        raise exception using
          message = 'IMPORT_FINANCIAL_PREFLIGHT_BLOCKED',
          errcode = 'P0001',
          detail = v_changed_preflight::text;
      end if;

      if exists (
        select 1
        from tmp_changed_sel cs
        where cs.is_invoiced is false
          and exists (
            select 1 from public.timesheets_financials paid_tf
            where paid_tf.timesheet_id = cs.timesheet_id
              and paid_tf.paid_at_utc is not null
          )
          and not exists (
            select 1 from public.timesheets_financials current_tf
            where current_tf.timesheet_id = cs.timesheet_id
              and current_tf.is_current = true
              and current_tf.stale_reason = 'IMPORT_PAID_TSFIN_ROLLOVER_PENDING_CALCULATION'
              and coalesce((current_tf.policy_snapshot_json->>'requires_frozen_correction_policy')::boolean,false) = true
          )
      ) then
        raise exception using
          message = 'PAID_UNINVOICED_ROLLOVER_REQUIRED',
          errcode = 'P0001',
          detail = jsonb_build_object(
            'code','PAID_UNINVOICED_ROLLOVER_REQUIRED',
            'required_path',jsonb_build_array(
              'UNAUTHORISE','PAID_UNINVOICED_ROLLOVER',
              'AMEND','RECALCULATE','REAUTHORISE'
            ),
            'invoice_policy_without_history','NOW',
            'timesheet_ids',to_jsonb(v_changed_timesheet_ids)
          )::text;
      end if;
    end if;

    -- Mode B only: preserve the lifecycle state of authorised, mutable source
    -- timesheets.  Reauthorisation is deliberately deferred until the Worker
    -- has completed the bounded TSFIN refresh for this committed operation.
    select coalesce(array_agg(distinct lifecycle_scope.timesheet_id order by lifecycle_scope.timesheet_id),array[]::uuid[])
    into v_reauthorise_timesheet_ids
    from (
      select cs.timesheet_id
      from tmp_changed_sel cs
      join public.timesheets ts on ts.timesheet_id=cs.timesheet_id and ts.is_current=true
      left join public.timesheets_financials tf on tf.timesheet_id=ts.timesheet_id and tf.is_current=true
      left join public.contract_weeks cw on cw.timesheet_id=ts.timesheet_id
      where cs.timesheet_id is not null
        and not (cs.external_row_key=any(coalesce(v_operation_bound_correction_keys,array[]::text[])))
        and cs.is_invoiced is false
        and cs.is_paid is false
        and (ts.authorised_at_server is not null or tf.authorised_at_utc is not null
          or cw.status='AUTHORISED'::public.contract_week_status_enum)
      union
      select ns.timesheet_id
      from public.nhsp_shifts ns
      join public.timesheets ts on ts.timesheet_id=ns.timesheet_id and ts.is_current=true
      left join public.timesheets_financials tf on tf.timesheet_id=ts.timesheet_id and tf.is_current=true
      left join public.contract_weeks cw on cw.timesheet_id=ts.timesheet_id
      where ns.id=any(coalesce(v_selected_cancel_shift_ids,array[]::uuid[]))
        and ns.timesheet_id is not null
        and coalesce((public._import_review_timesheet_protection_core_v1(ns.timesheet_id)->>'paid')::boolean,false)=false
        and coalesce((public._import_review_timesheet_protection_core_v1(ns.timesheet_id)->>'invoice_locked')::boolean,false)=false
        and (ts.authorised_at_server is not null or tf.authorised_at_utc is not null
          or cw.status='AUTHORISED'::public.contract_week_status_enum)
    ) lifecycle_scope;

    if cardinality(v_reauthorise_timesheet_ids)>100 then
      raise exception 'IMPORT_REVIEW_REAUTHORISE_SCOPE_TOO_LARGE' using errcode='54000';
    end if;
    if cardinality(v_reauthorise_timesheet_ids)>0 then
      select coalesce(jsonb_agg(jsonb_build_object(
        'timesheet_id',target_id::text,
        'expected_timesheet_id',target_id::text
      ) order by target_id),'[]'::jsonb)
      into v_lifecycle_items
      from unnest(v_reauthorise_timesheet_ids) as lifecycle_target(target_id);

      select public.timesheet_unauthorise_bulk_atomic(v_lifecycle_items,p_actor_user_id,v_now)
      into v_unauthorise_result;
      if coalesce((v_unauthorise_result->>'ok')::boolean,false) is not true
        or coalesce((v_unauthorise_result->>'all_success')::boolean,false) is not true then
        raise exception using message='IMPORT_REVIEW_CANONICAL_UNAUTHORISE_FAILED',errcode='P0001',
          detail=jsonb_build_object(
            'code','IMPORT_REVIEW_CANONICAL_UNAUTHORISE_FAILED',
            'timesheet_ids',to_jsonb(v_reauthorise_timesheet_ids),
            'failure_count',coalesce((v_unauthorise_result->>'failure_count')::int,cardinality(v_reauthorise_timesheet_ids))
          )::text;
      end if;
      v_steps:=v_steps||jsonb_build_array(jsonb_build_object(
        'step','CANONICAL_UNAUTHORISE_COMPLETE',
        'reauthorise_timesheet_count',cardinality(v_reauthorise_timesheet_ids)
      ));
    end if;

    v_mode_b_should_run_phase3 := (v_invoiced_changed_keys_count > 0);

    v_steps := v_steps || jsonb_build_array(
      jsonb_build_object(
        'step','CHANGED_HOURS_PARTITIONED',
        'invoiced_changed_keys_count', v_invoiced_changed_keys_count,
        'not_invoiced_changed_keys_count', v_not_invoiced_changed_keys_count
      )
    );

    create temporary table tmp_selected_replacement_keys(
      candidate_id uuid,
      client_id uuid,
      old_work_date date,
      replacement_day_key text
    ) on commit drop;

    if array_length(v_force_keys_final, 1) is not null then
      create temporary table tmp_sel_truth_p2 on commit drop as
      select
        m.external_row_key,
        m.candidate_id,
        m.client_id,
        m.work_date as import_work_date
      from tmp_p2_ok_mode m
      where m.mode = 'MODE_B'
        and m.external_row_key = any(v_force_keys_final);

      create temporary table tmp_existing_by_key on commit drop as
      select distinct on (ns.external_row_key)
        ns.external_row_key,
        ns.id as shift_id,
        ns.candidate_id as candidate_id,
        ns.client_id as client_id,
        ns.work_date as old_work_date
      from public.nhsp_shifts ns
      where ns.source_system = 'HEALTHROSTER'::public.hr_source_enum
        and ns.client_id = v_import_client_id
        and ns.cancelled_at_utc is null
        and ns.external_row_key = any(v_force_keys_final)
        and ns.work_date is not null
      order by ns.external_row_key, ns.updated_at desc nulls last, ns.created_at desc nulls last;

      insert into tmp_selected_replacement_keys(candidate_id, client_id, old_work_date, replacement_day_key)
      select distinct
        (coalesce(ex.candidate_id, st.candidate_id))::uuid as candidate_id,
        (coalesce(ex.client_id, st.client_id))::uuid as client_id,
        ex.old_work_date as old_work_date,
        ((coalesce(ex.candidate_id, st.candidate_id))::text || '|' ||
         (coalesce(ex.client_id, st.client_id))::text || '|' ||
         (ex.old_work_date)::text) as replacement_day_key
      from tmp_sel_truth_p2 st
      join tmp_existing_by_key ex
        on ex.external_row_key = st.external_row_key
      where ex.old_work_date is not null
        and st.import_work_date is not null
        and ex.old_work_date <> st.import_work_date;

      select coalesce(array_agg(x::text), array[]::text[])
      into v_selected_cancel_shift_id_set
      from unnest(coalesce(v_selected_cancel_shift_ids, array[]::uuid[])) as x;

      if exists (select 1 from tmp_selected_replacement_keys) then
        create temporary table tmp_required_cancel_ids on commit drop as
        select distinct
          rk.replacement_day_key,
          ns2.id as shift_id
        from tmp_selected_replacement_keys rk
        join public.nhsp_shifts ns2
          on ns2.source_system = 'HEALTHROSTER'::public.hr_source_enum
         and ns2.client_id = v_import_client_id
         and ns2.cancelled_at_utc is null
         and ns2.candidate_id = rk.candidate_id
         and ns2.client_id = rk.client_id
         and ns2.work_date = rk.old_work_date;

        if exists (
          select 1
          from tmp_required_cancel_ids rc
          left join unnest(coalesce(v_selected_cancel_shift_id_set, array[]::text[])) as sel(shift_id_text)
            on sel.shift_id_text = rc.shift_id::text
          where sel.shift_id_text is null
        ) then
          raise exception 'hr_weekly_apply_transactional: Policy A violation (replacement-day selected without selecting all required cancellations).';
        end if;
      end if;
    end if;

    v_steps := v_steps || jsonb_build_array(jsonb_build_object('step','POLICY_A_OK'));

    if v_mode_b_should_run_phase3 then
      select public.hr_weekly_phase3_apply_adjustment_truth(
        p_import_id := p_import_id,
        p_selected_external_row_keys := v_invoiced_changed_keys,
        p_actor_user_id := p_actor_user_id
      )
      into v_phase3_result;

      v_mode_b_phase3_called := true;
    end if;

    v_phase3_created_count := jsonb_array_length(coalesce(v_phase3_result->'created_timesheet_ids', '[]'::jsonb));
    v_phase3_updated_count := jsonb_array_length(coalesce(v_phase3_result->'updated_timesheet_ids', '[]'::jsonb));

    v_steps := v_steps || jsonb_build_array(
      jsonb_build_object(
        'step','PHASE3_CORRECTIONS_DONE',
        'phase3_called', v_mode_b_phase3_called,
        'phase3_created_count', v_phase3_created_count,
        'phase3_updated_count', v_phase3_updated_count
      )
    );

    if v_mode_b_should_run_phase1 then
      select public.hr_autoprocess_apply_phase1(
        import_id := p_import_id,
        selected_group_ids := array[]::text[],
        p_skip_external_row_keys := v_skip_keys_final,
        p_force_overwrite_external_row_keys := v_force_keys_final
      )
      into v_phase1_result;

      v_mode_b_phase1_called := true;

      v_phase1_shifts_created :=
        case
          when v_phase1_result is not null
           and jsonb_typeof(v_phase1_result) = 'object'
           and (v_phase1_result ? 'shifts_created')
           and (v_phase1_result->>'shifts_created') ~ '^[0-9]+$'
          then (v_phase1_result->>'shifts_created')::int
          else null
        end;

      v_phase1_shifts_updated :=
        case
          when v_phase1_result is not null
           and jsonb_typeof(v_phase1_result) = 'object'
           and (v_phase1_result ? 'shifts_updated')
           and (v_phase1_result->>'shifts_updated') ~ '^[0-9]+$'
          then (v_phase1_result->>'shifts_updated')::int
          else null
        end;

      if v_mode_b_should_run_phase15 then
        create temporary table tmp_phase15_rows on commit drop as
        select *
        from public.weekly_import_apply_phase2(p_import_id := p_import_id, p_system_type := 'HR_WEEKLY');

        select count(*)::int
        into v_phase15_ok
        from tmp_phase15_rows r
        where upper(coalesce(r.action::text,'')) = 'OK';

        select count(*)::int
        into v_phase15_updated
        from tmp_phase15_rows r
        where coalesce(r.shift_updated,false) is true;

        v_mode_b_phase15_called := true;
      end if;
    end if;

    v_steps := v_steps || jsonb_build_array(
      jsonb_build_object(
        'step','PHASE1_PHASE15_DONE',
        'phase1_called', v_mode_b_phase1_called,
        'phase15_called', v_mode_b_phase15_called,
        'phase1_shifts_created', v_phase1_shifts_created,
        'phase1_shifts_updated', v_phase1_shifts_updated,
        'phase15_ok_rows', v_phase15_ok,
        'phase15_shift_updated_rows', v_phase15_updated
      )
    );

    if v_mode_b_should_run_cancellations then
      create temporary table tmp_cancel_meta on commit drop as
      select
        ns.id as shift_id,
        ns.candidate_id,
        ns.client_id,
        ns.work_date
      from public.nhsp_shifts ns
      where ns.id = any(coalesce(v_selected_cancel_shift_ids, array[]::uuid[]));

      create temporary table tmp_selected_rep_keys_text on commit drop as
      select distinct rk.replacement_day_key
      from tmp_selected_replacement_keys rk;

      select coalesce(
        jsonb_agg(
          jsonb_build_object(
            'shift_id', cm.shift_id::text,
            'reason',
              case
                when exists (
                  select 1
                  from tmp_selected_rep_keys_text sr
                  where sr.replacement_day_key = (cm.candidate_id::text || '|' || cm.client_id::text || '|' || cm.work_date::text)
                ) then 'REPLACEMENT_DAY'
                else 'MISSING_FROM_IMPORT'
              end
          )
        ),
        '[]'::jsonb
      )
      into v_cancel_actions
      from tmp_cancel_meta cm;

      select public.weekly_import_apply_cancellations(
        p_import_id := p_import_id,
        p_actions := v_cancel_actions,
        p_actor_user_id := p_actor_user_id
      )
      into v_cancellations_result;

      v_mode_b_cancellations_called := true;
    end if;

    v_steps := v_steps || jsonb_build_array(jsonb_build_object('step','CANCELLATIONS_DONE'));

    -- ─────────────────────────────────────────────
    -- 8.5) ENSURE BASE WEEKLY TIMESHEET + ATTACH ACTIVE HEALTHROSTER MODE_B SHIFTS
    --
    -- Policy X guardrail:
    --   * This block runs only for HEALTHROSTER MODE_B / import-authoritative groups.
    --   * It creates/reuses the canonical weekly timesheet container and links active
    --     imported shifts to that container.
    --   * It does not use imported shift identifiers as Banking Pay economic keys.
    --   * It refuses to silently relink protected detached rows.
    -- ─────────────────────────────────────────────
    v_steps := v_steps || jsonb_build_array(jsonb_build_object('step','HR_MODE_B_ENSURE_BASE_WEEKLY_START'));

    drop table if exists pg_temp.tmp_hr_mode_b_groups;
    create temporary table tmp_hr_mode_b_groups(
      contract_id uuid not null,
      candidate_id uuid not null,
      client_id uuid not null,
      week_ending_date date not null,
      primary key (contract_id, candidate_id, client_id, week_ending_date)
    ) on commit drop;

    insert into tmp_hr_mode_b_groups(contract_id, candidate_id, client_id, week_ending_date)
    select distinct
      p2ok.contract_id,
      p2ok.candidate_id,
      p2ok.client_id,
      p2ok.week_ending_date
    from tmp_p2_ok_mode p2ok
    where p2ok.mode = 'MODE_B'
      and p2ok.external_row_key = any(coalesce(v_force_keys_final, array[]::text[]))
      and p2ok.contract_id is not null
      and p2ok.candidate_id is not null
      and p2ok.client_id is not null
      and p2ok.week_ending_date is not null
    on conflict do nothing;

    if array_length(v_selected_cancel_shift_ids, 1) is not null then
      insert into tmp_hr_mode_b_groups(contract_id, candidate_id, client_id, week_ending_date)
      select distinct
        ns.contract_id,
        ns.candidate_id,
        ns.client_id,
        ns.week_ending_date
      from public.nhsp_shifts ns
      join public.contracts c_cancel on c_cancel.id = ns.contract_id
      cross join lateral public._import_review_effective_authority_core_v1(
        'HR_WEEKLY',c_cancel.id,c_cancel.client_id,ns.week_ending_date) a_cancel
      where ns.id = any(v_selected_cancel_shift_ids)
        and ns.source_system = 'HEALTHROSTER'::public.hr_source_enum
        and ns.client_id = v_import_client_id
        and ns.contract_id is not null
        and ns.candidate_id is not null
        and ns.client_id is not null
        and ns.week_ending_date is not null
        and a_cancel.import_authoritative
      on conflict do nothing;
    end if;

    -- Include all active HealthRoster shifts in the affected MODE_B groups.  This
    -- makes the transaction repair the whole canonical week bucket, not just the
    -- single selected row, while staying inside the MODE_B classification.
    insert into tmp_hr_mode_b_groups(contract_id, candidate_id, client_id, week_ending_date)
    select distinct
      ns.contract_id,
      ns.candidate_id,
      ns.client_id,
      ns.week_ending_date
    from public.nhsp_shifts ns
    join tmp_group_mode gm2
      on gm2.contract_id = ns.contract_id
     and gm2.candidate_id = ns.candidate_id
     and gm2.client_id = ns.client_id
     and gm2.week_ending_date = ns.week_ending_date
     and gm2.mode = 'MODE_B'
    where ns.source_system = 'HEALTHROSTER'::public.hr_source_enum
      and ns.cancelled_at_utc is null
      and ns.contract_id is not null
      and ns.candidate_id is not null
      and ns.client_id is not null
      and ns.week_ending_date is not null
      and (
        ns.external_row_key = any(coalesce(v_force_keys_final, array[]::text[]))
        or ns.id = any(coalesce(v_selected_cancel_shift_ids, array[]::uuid[]))
        or exists (
          select 1
          from tmp_hr_mode_b_groups g0
          where g0.contract_id = ns.contract_id
            and g0.candidate_id = ns.candidate_id
            and g0.client_id = ns.client_id
            and g0.week_ending_date = ns.week_ending_date
        )
      )
    on conflict do nothing;

    select count(*)::int
    into v_ensure_pairs_count
    from tmp_hr_mode_b_groups g;

    select coalesce(jsonb_agg(jsonb_build_object(
      'contract_id', g.contract_id::text,
      'candidate_id', g.candidate_id::text,
      'client_id', g.client_id::text,
      'week_ending_date', g.week_ending_date::text
    )), '[]'::jsonb)
    into v_ensure_sample_pairs
    from (
      select g.contract_id, g.candidate_id, g.client_id, g.week_ending_date
      from tmp_hr_mode_b_groups g
      order by g.contract_id::text, g.candidate_id::text, g.client_id::text, g.week_ending_date::text
      limit 20
    ) as g;

    drop table if exists pg_temp.tmp_hr_mode_b_created_ts_ids;
    create temporary table tmp_hr_mode_b_created_ts_ids(
      timesheet_id uuid primary key
    ) on commit drop;

    drop table if exists pg_temp.tmp_hr_mode_b_protected_shift_ids;
    create temporary table tmp_hr_mode_b_protected_shift_ids(
      shift_id uuid primary key,
      reason text not null
    ) on commit drop;

    for v_pair_contract_id, v_pair_candidate_id, v_pair_client_id, v_pair_week_ending_date in
      select g.contract_id, g.candidate_id, g.client_id, g.week_ending_date
      from tmp_hr_mode_b_groups g
      order by g.contract_id::text, g.candidate_id::text, g.client_id::text, g.week_ending_date::text
    loop
      select count(*)::int
      into v_active_count
      from public.nhsp_shifts ns_active
      where ns_active.source_system = 'HEALTHROSTER'::public.hr_source_enum
        and ns_active.cancelled_at_utc is null
        and ns_active.contract_id = v_pair_contract_id
        and ns_active.candidate_id = v_pair_candidate_id
        and ns_active.client_id = v_pair_client_id
        and ns_active.week_ending_date = v_pair_week_ending_date;

      if coalesce(v_active_count, 0) <= 0 then
        v_ensure_pairs_skipped_no_active := v_ensure_pairs_skipped_no_active + 1;
        continue;
      end if;

      v_base_week_id := null;
      v_base_week_ts_id := null;

      select cw0.id, cw0.timesheet_id
      into v_base_week_id, v_base_week_ts_id
      from public.contract_weeks cw0
      where cw0.contract_id = v_pair_contract_id
        and cw0.week_ending_date = v_pair_week_ending_date
        and cw0.is_adjustment is false
        and coalesce(cw0.additional_seq, 0) = 0
      limit 1
      for update;

      if v_base_week_id is null then
        insert into public.contract_weeks(
          contract_id,
          week_ending_date,
          additional_seq,
          status,
          submission_mode_snapshot,
          timesheet_id,
          planned_schedule_json,
          created_at,
          updated_at,
          is_adjustment
        )
        values (
          v_pair_contract_id,
          v_pair_week_ending_date,
          0,
          'SUBMITTED'::public.contract_week_status_enum,
          'MANUAL'::public.submission_mode_enum,
          null,
          null,
          v_now,
          v_now,
          false
        )
        returning id into v_base_week_id;

        v_ensure_base_week_created_count := v_ensure_base_week_created_count + 1;
        v_base_week_ts_id := null;
      else
        v_ensure_base_week_existing_count := v_ensure_base_week_existing_count + 1;
      end if;

      if v_base_week_ts_id is not null then
        select exists(
          select 1
          from public.timesheets tchk
          where tchk.timesheet_id = v_base_week_ts_id
            and tchk.is_current is true
            and tchk.revoked_at is null
          limit 1
        )
        into v_ts_exists;

        if v_ts_exists is not true then
          update public.contract_weeks cw0u
          set
            timesheet_id = null,
            updated_at = v_now
          where cw0u.id = v_base_week_id;

          v_ensure_timesheet_missing_reference_count := v_ensure_timesheet_missing_reference_count + 1;
          v_base_week_ts_id := null;
        end if;
      end if;

      select ct.display_site, ct.ward_hint, ct.role
      into v_contract_display_site, v_contract_ward_hint, v_contract_role
      from public.contracts ct
      where ct.id = v_pair_contract_id
      limit 1;

      select cand.display_name, cand.tms_ref
      into v_candidate_display_name, v_candidate_tms_ref
      from public.candidates cand
      where cand.id = v_pair_candidate_id
      limit 1;

      select cli.name
      into v_client_name
      from public.clients cli
      where cli.id = v_pair_client_id
      limit 1;

      v_occupant_norm := lower(coalesce(v_candidate_tms_ref, v_candidate_display_name, v_pair_candidate_id::text));
      v_hospital_norm := lower(coalesce(v_contract_display_site, v_client_name, v_pair_client_id::text));
      v_ward_norm := lower(coalesce(v_contract_ward_hint, 'contract'));
      v_role_norm := lower(coalesce(v_contract_role, 'weekly'));

      v_shift_label_norm := 'weekly-0';

      v_booking_base :=
        v_occupant_norm || '|' ||
        v_pair_week_ending_date::text || '|' ||
        v_hospital_norm || '|' ||
        v_ward_norm || '|' ||
        v_role_norm || '|' ||
        v_shift_label_norm;

      v_hash_hex := encode(extensions.digest(convert_to(v_booking_base, 'utf8'), 'sha256'::text), 'hex');
      v_booking_id := 'bk_' || substr(v_hash_hex, 1, 16);

      if v_base_week_ts_id is null then
        v_new_ts_id := null;

        insert into public.timesheets(
          booking_id,
          version,
          is_current,
          status,

          sheet_scope,
          submission_mode,
          line_type,
          authorised_at_server,

          occupant_key_norm,
          hospital_norm,
          ward_norm,
          job_title_norm,
          shift_label_norm,

          week_ending_date,
          contract_id,

          manual_pdf_r2_key,
          actual_schedule_json,

          qr_payload_json,
          candidate_hint_text,

          is_adjustment,
          parent_timesheet_id,
          correction_id,
          correction_kind,
          adjustment_origin,

          created_at,
          updated_at
        )
        values (
          v_booking_id,
          1,
          true,
          'RECEIVED'::public.timesheet_status_enum,

          'WEEKLY'::public.timesheet_scope_enum,
          'MANUAL'::public.submission_mode_enum,
          'HOURS'::public.timesheet_line_type_enum,
          null,

          v_occupant_norm,
          v_hospital_norm,
          v_ward_norm,
          v_role_norm,
          v_shift_label_norm,

          v_pair_week_ending_date,
          v_pair_contract_id,

          null,
          '[]'::jsonb,

          '{}'::jsonb,
          null,

          false,
          null,
          null,
          null,
          null,

          v_now,
          v_now
        )
        returning timesheet_id into v_new_ts_id;

        v_ensure_timesheet_created_count := v_ensure_timesheet_created_count + 1;
        v_base_week_ts_id := v_new_ts_id;

        insert into tmp_hr_mode_b_created_ts_ids(timesheet_id)
        values (v_new_ts_id)
        on conflict do nothing;

        update public.contract_weeks cw0link
        set
          timesheet_id = v_new_ts_id,
          status = case
            when cw0link.status = 'AUTHORISED'::public.contract_week_status_enum then cw0link.status
            else 'SUBMITTED'::public.contract_week_status_enum
          end,
          submission_mode_snapshot = 'MANUAL'::public.submission_mode_enum,
          updated_at = v_now
        where cw0link.id = v_base_week_id;

        perform public._audit_insert(
          'timesheets',
          v_new_ts_id::text,
          'HEALTHROSTER_IMPORT_TIMESHEET_CREATED',
          null,
          jsonb_build_object(
            'import_id', p_import_id::text,
            'source_system', 'HEALTHROSTER',
            'mode', 'MODE_B',
            'kind', 'BASE_WEEKLY',
            'contract_id', v_pair_contract_id::text,
            'contract_week_id', v_base_week_id::text,
            'candidate_id', v_pair_candidate_id::text,
            'client_id', v_pair_client_id::text,
            'week_ending_date', v_pair_week_ending_date::text,
            'booking_id', v_booking_id,
            'active_shifts_count', v_active_count
          ),
          'IMPORT_BIRTH',
          p_actor_user_id
        );
      else
        v_ensure_timesheet_reused_count := v_ensure_timesheet_reused_count + 1;

        update public.contract_weeks cw0keep
        set
          status = case
            when cw0keep.status = 'AUTHORISED'::public.contract_week_status_enum then cw0keep.status
            else 'SUBMITTED'::public.contract_week_status_enum
          end,
          submission_mode_snapshot = 'MANUAL'::public.submission_mode_enum,
          updated_at = v_now
        where cw0keep.id = v_base_week_id;

        if exists (
          select 1
          from public.timesheets identity_target
          where identity_target.timesheet_id=v_base_week_ts_id
            and (
              identity_target.week_ending_date is distinct from v_pair_week_ending_date
              or identity_target.contract_id is distinct from v_pair_contract_id
              or identity_target.occupant_key_norm is distinct from v_occupant_norm
              or identity_target.hospital_norm is distinct from v_hospital_norm
              or identity_target.ward_norm is distinct from v_ward_norm
              or identity_target.job_title_norm is distinct from v_role_norm
            )
        ) then
          select public.import_timesheet_financial_preflight_v1(
            p_timesheet_ids := array[v_base_week_ts_id]::uuid[],
            p_action := 'IMPORT_FINANCIAL_IDENTITY_CHANGE',
            p_actor_user_id := p_actor_user_id,
            p_expected_state_json := '{}'::jsonb,
            p_lock_rows := true,
            p_max_scope := 100
          ) into v_changed_preflight;

          if coalesce((v_changed_preflight->>'allowed')::boolean,false) is not true then
            raise exception using message='IMPORT_FINANCIAL_PREFLIGHT_BLOCKED',errcode='P0001',detail=v_changed_preflight::text;
          end if;

          if v_changed_preflight->>'required_path'='CREATE_OR_UPDATE_CORRECTION_CHAIN' then
            raise exception using message='IMPORT_INVOICED_CORRECTION_REQUIRED',errcode='P0001',
              detail=jsonb_build_object(
                'code','IMPORT_INVOICED_CORRECTION_REQUIRED','timesheet_id',v_base_week_ts_id,
                'reason','FINANCIAL_IDENTITY_CHANGE',
                'required_path','CREATE_OR_UPDATE_CORRECTION_CHAIN'
              )::text;
          elsif v_changed_preflight->>'required_path'='UNAUTHORISE_AMEND_RECALCULATE_REAUTHORISE' then
            raise exception using message='CANONICAL_UNAUTHORISE_REQUIRED',errcode='P0001',
              detail=jsonb_build_object(
                'code','CANONICAL_UNAUTHORISE_REQUIRED','timesheet_id',v_base_week_ts_id,
                'reason','FINANCIAL_IDENTITY_CHANGE',
                'required_path',jsonb_build_array('UNAUTHORISE','AMEND','RECALCULATE','REAUTHORISE'),
                'paid_uninvoiced_rollover_required',false
              )::text;
          elsif v_changed_preflight->>'required_path'='PAID_UNINVOICED_ROLLOVER'
            and not exists (
              select 1 from public.timesheets_financials rollover_identity
              where rollover_identity.timesheet_id=v_base_week_ts_id
                and rollover_identity.is_current=true
                and rollover_identity.stale_reason='IMPORT_PAID_TSFIN_ROLLOVER_PENDING_CALCULATION'
                and coalesce((rollover_identity.policy_snapshot_json->>'requires_frozen_correction_policy')::boolean,false)=true
            ) then
            raise exception using message='PAID_UNINVOICED_ROLLOVER_REQUIRED',errcode='P0001',
              detail=jsonb_build_object(
                'code','PAID_UNINVOICED_ROLLOVER_REQUIRED','timesheet_id',v_base_week_ts_id,
                'reason','FINANCIAL_IDENTITY_CHANGE',
                'required_path',jsonb_build_array(
                  'UNAUTHORISE','PAID_UNINVOICED_ROLLOVER','AMEND','RECALCULATE','REAUTHORISE'
                ),
                'invoice_policy_without_history','NOW'
              )::text;
          end if;
        end if;

        update public.timesheets tnorm
        set
          is_current = true,
          status = 'RECEIVED'::public.timesheet_status_enum,
          sheet_scope = 'WEEKLY'::public.timesheet_scope_enum,
          submission_mode = 'MANUAL'::public.submission_mode_enum,
          line_type = 'HOURS'::public.timesheet_line_type_enum,
          week_ending_date = v_pair_week_ending_date,
          contract_id = v_pair_contract_id,
          occupant_key_norm = v_occupant_norm,
          hospital_norm = v_hospital_norm,
          ward_norm = v_ward_norm,
          job_title_norm = v_role_norm,
          shift_label_norm = v_shift_label_norm,
          updated_at = v_now
        where tnorm.timesheet_id = v_base_week_ts_id;
      end if;

      truncate table tmp_hr_mode_b_protected_shift_ids;

      insert into tmp_hr_mode_b_protected_shift_ids(shift_id, reason)
      select distinct
        ns_lock.id,
        case
          when ns_lock.invoice_id is not null then 'SHIFT_INVOICED'
          when tf_lock.timesheet_id is not null then 'TIMESHEET_FINANCIALS_LOCKED_OR_PAID'
          when pbi_lock.timesheet_id is not null then 'PAY_BATCH_ITEM_EXISTS'
          when t_lock.timesheet_id is not null then 'CORRECTION_OR_ADJUSTMENT_OWNED_TIMESHEET'
          else 'PROTECTED_DETACHED_OR_INVALID_LINK'
        end as reason
      from public.nhsp_shifts ns_lock
      left join public.timesheets_financials tf_lock
        on tf_lock.timesheet_id = ns_lock.timesheet_id
       and tf_lock.is_current is true
       and (
         tf_lock.locked_by_invoice_id is not null
         or tf_lock.paid_at_utc is not null
       )
      left join public.pay_batch_items pbi_lock
        on pbi_lock.timesheet_id = ns_lock.timesheet_id
       and coalesce(pbi_lock.is_voided, false) is false
      left join public.timesheets t_lock
        on t_lock.timesheet_id = ns_lock.timesheet_id
       and (
         coalesce(t_lock.is_adjustment, false) is true
         or t_lock.parent_timesheet_id is not null
         or t_lock.correction_id is not null
         or t_lock.correction_kind is not null
         or t_lock.adjustment_origin is not null
       )
      where ns_lock.source_system = 'HEALTHROSTER'::public.hr_source_enum
        and ns_lock.cancelled_at_utc is null
        and ns_lock.contract_id = v_pair_contract_id
        and ns_lock.candidate_id = v_pair_candidate_id
        and ns_lock.client_id = v_pair_client_id
        and ns_lock.week_ending_date = v_pair_week_ending_date
        and (
          ns_lock.timesheet_id is null
          or not exists (
            select 1
            from public.timesheets tvalid
            where tvalid.timesheet_id = ns_lock.timesheet_id
              and tvalid.is_current is true
              and tvalid.revoked_at is null
            limit 1
          )
        )
        and (
          ns_lock.invoice_id is not null
          or tf_lock.timesheet_id is not null
          or pbi_lock.timesheet_id is not null
          or t_lock.timesheet_id is not null
        )
      on conflict do nothing;

      select count(*)::int
      into v_active_count
      from tmp_hr_mode_b_protected_shift_ids p;

      if coalesce(v_active_count, 0) > 0 then
        raise exception
          'hr_weekly_apply_transactional: protected active HEALTHROSTER MODE_B shifts are detached or linked to an invalid timesheet; refusing silent relink. contract_id=% candidate_id=% client_id=% week_ending_date=% protected_count=% sample=%',
          v_pair_contract_id,
          v_pair_candidate_id,
          v_pair_client_id,
          v_pair_week_ending_date,
          v_active_count,
          (
            select coalesce(jsonb_agg(jsonb_build_object('shift_id', p2.shift_id::text, 'reason', p2.reason)), '[]'::jsonb)
            from (
              select p.shift_id, p.reason
              from tmp_hr_mode_b_protected_shift_ids p
              order by p.shift_id::text
              limit 10
            ) p2
          );
      end if;

      if exists (
        select 1 from public.nhsp_shifts ns_scope
        where ns_scope.source_system = 'HEALTHROSTER'::public.hr_source_enum
            and ns_scope.cancelled_at_utc is null
            and ns_scope.contract_id = v_pair_contract_id
            and ns_scope.candidate_id = v_pair_candidate_id
            and ns_scope.client_id = v_pair_client_id
            and ns_scope.week_ending_date = v_pair_week_ending_date
            and (
              ns_scope.timesheet_id is null
              or not exists (
                select 1 from public.timesheets existing_link
                where existing_link.timesheet_id=ns_scope.timesheet_id
                  and existing_link.is_current=true
                  and existing_link.revoked_at is null
              )
            )
            and not exists (
              select 1 from tmp_hr_mode_b_protected_shift_ids protected
              where protected.shift_id=ns_scope.id
            )
      ) then
        select public.import_timesheet_financial_preflight_v1(
          p_timesheet_ids := array[v_base_week_ts_id]::uuid[],
          p_action := 'IMPORT_SOURCE_ASSIGNMENT',
          p_actor_user_id := p_actor_user_id,
          p_expected_state_json := '{}'::jsonb,
          p_lock_rows := true,
          p_max_scope := 100
        ) into v_changed_preflight;

        if coalesce((v_changed_preflight->>'allowed')::boolean,false) is not true then
          raise exception using message='IMPORT_FINANCIAL_PREFLIGHT_BLOCKED',errcode='P0001',detail=v_changed_preflight::text;
        end if;

        if v_changed_preflight->>'required_path'='CREATE_OR_UPDATE_CORRECTION_CHAIN' then
          raise exception using message='IMPORT_INVOICED_CORRECTION_REQUIRED',errcode='P0001',
            detail=jsonb_build_object(
              'code','IMPORT_INVOICED_CORRECTION_REQUIRED','timesheet_id',v_base_week_ts_id,
              'reason','FINANCIAL_SOURCE_ASSIGNMENT_CHANGE',
              'required_path','CREATE_OR_UPDATE_CORRECTION_CHAIN'
            )::text;
        end if;

        if exists (
          select 1 from public.timesheets source_target
          left join public.timesheets_financials source_target_tf
            on source_target_tf.timesheet_id=source_target.timesheet_id and source_target_tf.is_current=true
          where source_target.timesheet_id=v_base_week_ts_id
            and (source_target.authorised_at_server is not null or source_target_tf.authorised_at_utc is not null)
            and not exists (
              select 1 from public.timesheets_financials paid_target
              where paid_target.timesheet_id=source_target.timesheet_id
                and paid_target.paid_at_utc is not null
            )
        ) then
          raise exception using message='CANONICAL_UNAUTHORISE_REQUIRED',errcode='P0001',
            detail=jsonb_build_object(
              'code','CANONICAL_UNAUTHORISE_REQUIRED','timesheet_id',v_base_week_ts_id,
              'reason','FINANCIAL_SOURCE_ASSIGNMENT_CHANGE',
              'required_path',jsonb_build_array('UNAUTHORISE','AMEND','RECALCULATE','REAUTHORISE'),
              'paid_uninvoiced_rollover_required',false
            )::text;
        end if;

        if exists (
          select 1 from public.timesheets_financials paid_source
          where paid_source.timesheet_id=v_base_week_ts_id and paid_source.paid_at_utc is not null
        ) and not exists (
          select 1 from public.timesheets_financials rollover_source
          where rollover_source.timesheet_id=v_base_week_ts_id and rollover_source.is_current=true
            and rollover_source.stale_reason='IMPORT_PAID_TSFIN_ROLLOVER_PENDING_CALCULATION'
            and coalesce((rollover_source.policy_snapshot_json->>'requires_frozen_correction_policy')::boolean,false)=true
        ) then
          raise exception using message='PAID_UNINVOICED_ROLLOVER_REQUIRED',errcode='P0001',
            detail=jsonb_build_object(
              'code','PAID_UNINVOICED_ROLLOVER_REQUIRED','timesheet_id',v_base_week_ts_id,
              'reason','FINANCIAL_SOURCE_ASSIGNMENT_CHANGE',
              'required_path',jsonb_build_array(
                'UNAUTHORISE','PAID_UNINVOICED_ROLLOVER','AMEND','RECALCULATE','REAUTHORISE'
              ),
              'invoice_policy_without_history','NOW'
            )::text;
        end if;
      end if;

      update public.nhsp_shifts nsu0
      set
        timesheet_id = v_base_week_ts_id,
        updated_at = v_now
      where nsu0.source_system = 'HEALTHROSTER'::public.hr_source_enum
        and nsu0.cancelled_at_utc is null
        and nsu0.contract_id = v_pair_contract_id
        and nsu0.candidate_id = v_pair_candidate_id
        and nsu0.client_id = v_pair_client_id
        and nsu0.week_ending_date = v_pair_week_ending_date
        and nsu0.timesheet_id is null
        and not exists (
          select 1
          from tmp_hr_mode_b_protected_shift_ids p
          where p.shift_id = nsu0.id
        );

      get diagnostics v_attached_null_count = row_count;
      v_ensure_shifts_attached_count := v_ensure_shifts_attached_count + coalesce(v_attached_null_count, 0);

      update public.nhsp_shifts nsu1
      set
        timesheet_id = v_base_week_ts_id,
        updated_at = v_now
      where nsu1.source_system = 'HEALTHROSTER'::public.hr_source_enum
        and nsu1.cancelled_at_utc is null
        and nsu1.contract_id = v_pair_contract_id
        and nsu1.candidate_id = v_pair_candidate_id
        and nsu1.client_id = v_pair_client_id
        and nsu1.week_ending_date = v_pair_week_ending_date
        and nsu1.timesheet_id is not null
        and not exists (
          select 1
          from public.timesheets tvalid2
          where tvalid2.timesheet_id = nsu1.timesheet_id
            and tvalid2.is_current is true
            and tvalid2.revoked_at is null
          limit 1
        )
        and not exists (
          select 1
          from tmp_hr_mode_b_protected_shift_ids p2
          where p2.shift_id = nsu1.id
        );

      get diagnostics v_relinked_invalid_count = row_count;
      v_ensure_shifts_relinked_invalid_ts_count := v_ensure_shifts_relinked_invalid_ts_count + coalesce(v_relinked_invalid_count, 0);

      select count(*)::int
      into v_active_count
      from public.nhsp_shifts nscheck
      where nscheck.source_system = 'HEALTHROSTER'::public.hr_source_enum
        and nscheck.cancelled_at_utc is null
        and nscheck.contract_id = v_pair_contract_id
        and nscheck.candidate_id = v_pair_candidate_id
        and nscheck.client_id = v_pair_client_id
        and nscheck.week_ending_date = v_pair_week_ending_date
        and (
          nscheck.timesheet_id is null
          or not exists (
            select 1
            from public.timesheets tchk2
            where tchk2.timesheet_id = nscheck.timesheet_id
              and tchk2.is_current is true
              and tchk2.revoked_at is null
            limit 1
          )
        );

      if coalesce(v_active_count, 0) > 0 then
        v_ensure_remaining_active_detached_count := v_ensure_remaining_active_detached_count + v_active_count;
        raise exception
          'hr_weekly_apply_transactional: ENSURE invariant failed (active HEALTHROSTER MODE_B shifts remain detached or linked to missing/non-current/revoked timesheets) contract_id=% candidate_id=% client_id=% week_ending_date=% remaining=%.',
          v_pair_contract_id, v_pair_candidate_id, v_pair_client_id, v_pair_week_ending_date, v_active_count;
      end if;

      insert into tmp_aff_ts(timesheet_id)
      values (v_base_week_ts_id)
      on conflict do nothing;
    end loop;

    select coalesce(jsonb_agg(x.ts_id), '[]'::jsonb)
    into v_ensure_sample_created_ts_ids
    from (
      select tct.timesheet_id::text as ts_id
      from tmp_hr_mode_b_created_ts_ids tct
      order by tct.timesheet_id::text
      limit 20
    ) as x;

    v_steps := v_steps || jsonb_build_array(jsonb_build_object(
      'step','HR_MODE_B_ENSURE_BASE_WEEKLY_DONE',
      'ensure_pairs_count', v_ensure_pairs_count,
      'ensure_pairs_skipped_no_active', v_ensure_pairs_skipped_no_active,
      'base_week_created_count', v_ensure_base_week_created_count,
      'base_week_existing_count', v_ensure_base_week_existing_count,
      'base_timesheet_created_count', v_ensure_timesheet_created_count,
      'base_timesheet_reused_count', v_ensure_timesheet_reused_count,
      'missing_timesheet_reference_count', v_ensure_timesheet_missing_reference_count,
      'shifts_attached_null_count', v_ensure_shifts_attached_count,
      'shifts_relinked_invalid_ts_count', v_ensure_shifts_relinked_invalid_ts_count,
      'remaining_active_detached_count', v_ensure_remaining_active_detached_count,
      'sample_pairs', v_ensure_sample_pairs,
      'sample_created_ts_ids', v_ensure_sample_created_ts_ids
    ));
  end if;

  -- ─────────────────────────────────────────────
  -- 9) MODE_A mirror ingestion
  -- ─────────────────────────────────────────────
  if array_length(v_mode_a_external_keys, 1) is not null then
    select public.hr_weekly_mirror_upsert_deterministic(
      p_import_id := p_import_id,
      p_external_row_keys := v_mode_a_external_keys,
      p_actor_user_id := p_actor_user_id
    )
    into v_mirror_result;
  end if;

  v_steps := v_steps || jsonb_build_array(jsonb_build_object('step','MODE_A_MIRROR_DONE'));

  -- ─────────────────────────────────────────────
  -- ✅ MODE_A shift→timesheet linking
  -- ─────────────────────────────────────────────
  create temporary table tmp_mode_a_ts_map(
    external_row_key text primary key,
    timesheet_id uuid not null
  ) on commit drop;

  insert into tmp_mode_a_ts_map(external_row_key, timesheet_id)
  select distinct
    p2m.external_row_key,
    cw0.timesheet_id
  from tmp_p2_ok_mode p2m
  join public.contract_weeks cw0
    on cw0.contract_id = p2m.contract_id
   and cw0.week_ending_date = p2m.week_ending_date
   and cw0.is_adjustment is false
   and coalesce(cw0.additional_seq, 0) = 0
  where p2m.mode = 'MODE_A'
    and p2m.external_row_key = any(coalesce(v_mode_a_external_keys,array[]::text[]))
    and p2m.external_row_key is not null
    and cw0.timesheet_id is not null
  on conflict do nothing;

  select count(*)::int
  into v_mode_a_ts_linked_count
  from tmp_mode_a_ts_map mt;

  create temporary table tmp_mode_a_locked_shift_ids(
    shift_id uuid primary key
  ) on commit drop;

  insert into tmp_mode_a_locked_shift_ids(shift_id)
  select distinct ns_lock.id as shift_id
  from public.nhsp_shifts ns_lock
  join tmp_mode_a_ts_map mt_lock
    on mt_lock.external_row_key = ns_lock.external_row_key
  where ns_lock.source_system = 'HEALTHROSTER'::public.hr_source_enum
    and ns_lock.cancelled_at_utc is null
    and ns_lock.latest_import_id = p_import_id
    and (
      ns_lock.invoice_id is not null
      or exists (
        select 1
        from public.timesheets_financials tf_lock
        cross join lateral jsonb_array_elements(coalesce(tf_lock.invoice_breakdown_json->'segments','[]'::jsonb)) as seg_lock(value)
        where tf_lock.is_current = true
          and tf_lock.timesheet_id = ns_lock.timesheet_id
          and nullif(btrim(seg_lock.value->>'nhsp_shift_id'), '') = ns_lock.id::text
          and nullif(btrim(seg_lock.value->>'invoice_locked_invoice_id'), '') is not null
        limit 1
      )
    )
  on conflict do nothing;

  update public.nhsp_shifts nsu
     set timesheet_id = mt.timesheet_id,
         updated_at = v_now
    from tmp_mode_a_ts_map mt
   where nsu.source_system = 'HEALTHROSTER'::public.hr_source_enum
     and nsu.cancelled_at_utc is null
     and nsu.latest_import_id = p_import_id
     and nsu.external_row_key = mt.external_row_key
     and (nsu.timesheet_id is distinct from mt.timesheet_id)
     and not exists (
       select 1
       from tmp_mode_a_locked_shift_ids l
       where l.shift_id = nsu.id
     );

  get diagnostics v_mode_a_shifts_attached_count = row_count;

  insert into tmp_aff_ts(timesheet_id)
  select distinct mt2.timesheet_id
  from tmp_mode_a_ts_map mt2
  where mt2.timesheet_id is not null
  on conflict do nothing;

  v_steps := v_steps || jsonb_build_array(
    jsonb_build_object(
      'step','MODE_A_SHIFTS_LINKED',
      'mode_a_ts_linked_count', v_mode_a_ts_linked_count,
      'mode_a_shifts_attached_count', v_mode_a_shifts_attached_count,
      'mode_a_locked_shift_count', (select count(*)::int from tmp_mode_a_locked_shift_ids)
    )
  );

  -- ─────────────────────────────────────────────
  -- 10) MODE_A weekly validation upserts + email state
  -- ─────────────────────────────────────────────
  select public.hr_weekly_validation_preview(p_import_id := p_import_id)
  into v_weekly_val_payload;

  if v_weekly_val_payload is null or jsonb_typeof(v_weekly_val_payload) <> 'object' then
    raise exception 'hr_weekly_apply_transactional: hr_weekly_validation_preview returned non-object payload.';
  end if;

  if jsonb_typeof(v_weekly_val_payload->'rows') <> 'array' then
    raise exception 'hr_weekly_apply_transactional: hr_weekly_validation_preview payload missing rows array.';
  end if;

  create temporary table tmp_val_rows on commit drop as
  select
    nullif(btrim(r.value->>'timesheet_id'), '')::uuid as timesheet_id,
    nullif(btrim(r.value->>'candidate_id'), '')::uuid as candidate_id,
    nullif(btrim(r.value->>'contract_id'), '')::uuid as contract_id,
    nullif(btrim(r.value->>'week_ending_date'), '')::date as week_ending_date,
    nullif(btrim(r.value->>'client_id'), '')::uuid as client_id,
    upper(coalesce(r.value->>'overall_status','')) as overall_status,
    (lower(coalesce(r.value->>'has_mismatch','false')) in ('true','1')) as has_mismatch,
    r.value as row_json
  from jsonb_array_elements(v_weekly_val_payload->'rows') as r(value)
  where nullif(btrim(r.value->>'timesheet_id'), '') is not null
    and nullif(btrim(r.value->>'candidate_id'), '') is not null
    and nullif(btrim(r.value->>'contract_id'), '') is not null
    and nullif(btrim(r.value->>'week_ending_date'), '') is not null
    and nullif(btrim(r.value->>'client_id'), '') is not null
    and exists (
      select 1 from tmp_review_batch_units bu
      where bu.candidate_id=nullif(btrim(r.value->>'candidate_id'), '')::uuid
        and bu.client_id=nullif(btrim(r.value->>'client_id'), '')::uuid
    );

  select count(*)::int
  into v_val_rows_count
  from tmp_val_rows;

  create temporary table tmp_val_mode on commit drop as
  select
    vr.timesheet_id,
    case aval.authority_mode when 'VALIDATION_ONLY' then 'MODE_A'
      when 'AUTHORITATIVE' then 'MODE_B' else 'OUT_OF_SCOPE' end as mode
  from tmp_val_rows vr
  join public.contracts cval
    on cval.id = vr.contract_id
  cross join lateral public._import_review_effective_authority_core_v1(
    'HR_WEEKLY',cval.id,cval.client_id,vr.week_ending_date) aval;

  if exists(select 1 from tmp_val_mode where mode='OUT_OF_SCOPE') then
    raise exception 'HR_WEEKLY_VALIDATION_AUTHORITY_OUT_OF_SCOPE' using errcode='40001';
  end if;

  create temporary table tmp_invalidation_actions(
    timesheet_id uuid not null,
    comparison_key text not null,
    invalidate boolean not null,
    primary key (timesheet_id, comparison_key)
  ) on commit drop;

  if v_invalidation_actions_count > 0 then
    insert into tmp_invalidation_actions(timesheet_id, comparison_key, invalidate)
    select
      nullif(btrim(a.value->>'timesheet_id'), '')::uuid as timesheet_id,
      nullif(btrim(a.value->>'comparison_key'), '') as comparison_key,
      (lower(coalesce(a.value->>'invalidate','true')) in ('true','1')) as invalidate
    from jsonb_array_elements(v_invalidation_actions) as a(value)
    where nullif(btrim(a.value->>'timesheet_id'), '') is not null
      and nullif(btrim(a.value->>'comparison_key'), '') is not null
    on conflict (timesheet_id, comparison_key) do update
      set invalidate = excluded.invalidate;
  end if;

  create temporary table tmp_mode_a_missing_ref_clear(
    timesheet_id uuid not null,
    comparison_key text not null,
    work_date date not null,
    ts_start_hhmm text not null,
    ts_end_hhmm text not null,
    ts_break_mins int not null,
    ref_before text null,
    primary key (timesheet_id, comparison_key)
  ) on commit drop;

  insert into tmp_mode_a_missing_ref_clear(timesheet_id, comparison_key, work_date, ts_start_hhmm, ts_end_hhmm, ts_break_mins, ref_before)
  select distinct
    vr.timesheet_id,
    nullif(btrim(coalesce(cx.value->>'comparison_key','')), '') as comparison_key,
    nullif(btrim(cx.value->>'work_date'), '')::date as work_date,
    nullif(btrim(cx.value->>'timesheet_start'), '') as ts_start_hhmm,
    nullif(btrim(cx.value->>'timesheet_end'), '') as ts_end_hhmm,
    coalesce(nullif(btrim(cx.value->>'timesheet_break_mins'), '')::int, 0) as ts_break_mins,
    nullif(btrim(cx.value->>'ref_before'), '') as ref_before
  from tmp_val_rows vr
  join tmp_val_mode vmc
    on vmc.timesheet_id = vr.timesheet_id
   and vmc.mode = 'MODE_A'
  cross join lateral jsonb_array_elements(coalesce(vr.row_json->'comparisons', '[]'::jsonb)) as cx(value)
  left join tmp_invalidation_actions ia
    on ia.timesheet_id = vr.timesheet_id
   and ia.comparison_key = nullif(btrim(coalesce(cx.value->>'comparison_key','')), '')
  where upper(coalesce(cx.value->>'match_status','')) in ('UNMATCHED','MISMATCH')
    and (lower(coalesce(cx.value->>'invoice_locked','false')) in ('true','1')) is false
    and nullif(btrim(coalesce(cx.value->>'invoice_locked_invoice_id','')), '') is null
    and nullif(btrim(coalesce(cx.value->>'ref_before','')), '') is not null
    and nullif(btrim(coalesce(cx.value->>'timesheet_start','')), '') is not null
    and nullif(btrim(coalesce(cx.value->>'timesheet_end','')), '') is not null
    and nullif(btrim(coalesce(cx.value->>'work_date','')), '') is not null
    and ia.timesheet_id is not null
    and ia.invalidate is true
  on conflict (timesheet_id, comparison_key) do nothing;

  -- ✅ Capture exact timesheets whose refs were cleared (for post-apply QR reissue + regen)
  drop table if exists pg_temp.tmp_mode_a_ref_clear_upd;
  create temporary table tmp_mode_a_ref_clear_upd(
    timesheet_id uuid not null
  ) on commit drop;

  with upd as (
    update public.nhsp_shifts nsclr
       set ref_num = null,
           hr_request_id = null,
           updated_at = v_now
      from tmp_mode_a_missing_ref_clear mrc
      left join public.timesheets_financials tfc
        on tfc.timesheet_id = mrc.timesheet_id
       and tfc.is_current = true
     where nsclr.source_system = 'HEALTHROSTER'::public.hr_source_enum
       and nsclr.cancelled_at_utc is null
       and nsclr.timesheet_id = mrc.timesheet_id
       and nsclr.work_date = mrc.work_date
       and nsclr.ref_num is not null
       and nsclr.invoice_id is null
       and (tfc.timesheet_id is null or (tfc.locked_by_invoice_id is null and tfc.paid_at_utc is null))
       and to_char((date_trunc('minute', nsclr.start_utc) at time zone 'Europe/London'), 'HH24:MI') = mrc.ts_start_hhmm
       and to_char((date_trunc('minute', nsclr.end_utc) at time zone 'Europe/London'), 'HH24:MI') = mrc.ts_end_hhmm
       and coalesce(nsclr.break_mins,0) = coalesce(mrc.ts_break_mins,0)
       and not exists (
         select 1
         from public.timesheets_financials tf_lock
         cross join lateral jsonb_array_elements(coalesce(tf_lock.invoice_breakdown_json->'segments','[]'::jsonb)) as seg(value)
         where tf_lock.is_current = true
           and tf_lock.timesheet_id = nsclr.timesheet_id
           and nullif(btrim(seg.value->>'nhsp_shift_id'), '') = nsclr.id::text
           and nullif(btrim(seg.value->>'invoice_locked_invoice_id'), '') is not null
         limit 1
       )
    returning nsclr.timesheet_id
  )
  insert into tmp_mode_a_ref_clear_upd(timesheet_id)
  select upd.timesheet_id
  from upd
  where upd.timesheet_id is not null;

  get diagnostics v_mode_a_ref_cleared_count = row_count;

  insert into tmp_ref_updated_ts(timesheet_id)
  select distinct u.timesheet_id
  from tmp_mode_a_ref_clear_upd u
  where u.timesheet_id is not null
  on conflict do nothing;

  insert into tmp_aff_ts(timesheet_id)
  select distinct u2.timesheet_id
  from tmp_mode_a_ref_clear_upd u2
  where u2.timesheet_id is not null
  on conflict do nothing;

  insert into tmp_aff_ts(timesheet_id)
  select distinct mrc2.timesheet_id
  from tmp_mode_a_missing_ref_clear mrc2
  where mrc2.timesheet_id is not null
  on conflict do nothing;

  v_steps := v_steps || jsonb_build_array(
    jsonb_build_object(
      'step','MODE_A_MISSING_SHIFT_REF_CLEARED',
      'ref_cleared_count', v_mode_a_ref_cleared_count,
      'invalidation_actions_count', v_invalidation_actions_count
    )
  );

  -- ─────────────────────────────────────────────
  -- ✅ MODE_A matched ref propagation (Request Id / booking reference)
  -- ─────────────────────────────────────────────
  drop table if exists pg_temp.tmp_mode_a_ref_set;
  create temporary table tmp_mode_a_ref_set(
    timesheet_id uuid not null,
    comparison_key text not null,
    work_date date not null,
    ts_start_hhmm text not null,
    ts_end_hhmm text not null,
    ts_break_mins int not null,
    ref_after text not null,
    primary key (timesheet_id, comparison_key)
  ) on commit drop;

  insert into tmp_mode_a_ref_set(timesheet_id, comparison_key, work_date, ts_start_hhmm, ts_end_hhmm, ts_break_mins, ref_after)
  select distinct
    vrm.timesheet_id,
    nullif(btrim(coalesce(cx2.value->>'comparison_key','')), '') as comparison_key,
    nullif(btrim(cx2.value->>'work_date'), '')::date as work_date,
    nullif(btrim(cx2.value->>'timesheet_start'), '') as ts_start_hhmm,
    nullif(btrim(cx2.value->>'timesheet_end'), '') as ts_end_hhmm,
    coalesce(nullif(btrim(cx2.value->>'timesheet_break_mins'), '')::int, 0) as ts_break_mins,
    nullif(btrim(coalesce(cx2.value->>'ref_after','')), '') as ref_after
  from tmp_val_rows vrm
  join tmp_val_mode vmm
    on vmm.timesheet_id = vrm.timesheet_id
   and vmm.mode = 'MODE_A'
  cross join lateral jsonb_array_elements(coalesce(vrm.row_json->'comparisons', '[]'::jsonb)) as cx2(value)
  where vrm.timesheet_id is not null
    and (
      upper(coalesce(cx2.value->>'match_status','')) in ('MATCH','MATCHED','OK','PASS')
      or (lower(coalesce(cx2.value->>'match','false')) in ('true','1'))
    )
    and nullif(btrim(coalesce(cx2.value->>'ref_after','')), '') is not null
    and nullif(btrim(coalesce(cx2.value->>'timesheet_start','')), '') is not null
    and nullif(btrim(coalesce(cx2.value->>'timesheet_end','')), '') is not null
    and nullif(btrim(coalesce(cx2.value->>'work_date','')), '') is not null
  on conflict (timesheet_id, comparison_key) do nothing;

  drop table if exists pg_temp.tmp_mode_a_ref_set_upd;
  create temporary table tmp_mode_a_ref_set_upd(
    timesheet_id uuid not null
  ) on commit drop;

  with upd as (
    update public.nhsp_shifts nsset
       set ref_num = mrs.ref_after,
           hr_request_id = mrs.ref_after,
           updated_at = v_now
      from tmp_mode_a_ref_set mrs
      left join public.timesheets_financials tfm
        on tfm.timesheet_id = mrs.timesheet_id
       and tfm.is_current = true
     where nsset.source_system = 'HEALTHROSTER'::public.hr_source_enum
       and nsset.cancelled_at_utc is null
       and nsset.timesheet_id = mrs.timesheet_id
       and nsset.work_date = mrs.work_date
       and nsset.invoice_id is null
       and (tfm.timesheet_id is null or (tfm.locked_by_invoice_id is null and tfm.paid_at_utc is null))
       and to_char((date_trunc('minute', nsset.start_utc) at time zone 'Europe/London'), 'HH24:MI') = mrs.ts_start_hhmm
       and to_char((date_trunc('minute', nsset.end_utc) at time zone 'Europe/London'), 'HH24:MI') = mrs.ts_end_hhmm
       and coalesce(nsset.break_mins,0) = coalesce(mrs.ts_break_mins,0)
       and (
         nsset.ref_num is distinct from mrs.ref_after
         or nsset.hr_request_id is distinct from mrs.ref_after
       )
       and not exists (
         select 1
         from public.timesheets_financials tf_lock2
         cross join lateral jsonb_array_elements(coalesce(tf_lock2.invoice_breakdown_json->'segments','[]'::jsonb)) as seg2(value)
         where tf_lock2.is_current = true
           and tf_lock2.timesheet_id = nsset.timesheet_id
           and nullif(btrim(seg2.value->>'nhsp_shift_id'), '') = nsset.id::text
           and nullif(btrim(seg2.value->>'invoice_locked_invoice_id'), '') is not null
         limit 1
       )
    returning nsset.timesheet_id
  )
  insert into tmp_mode_a_ref_set_upd(timesheet_id)
  select upd.timesheet_id
  from upd
  where upd.timesheet_id is not null;

  get diagnostics v_mode_a_ref_set_count = row_count;

  insert into tmp_ref_updated_ts(timesheet_id)
  select distinct u.timesheet_id
  from tmp_mode_a_ref_set_upd u
  where u.timesheet_id is not null
  on conflict do nothing;

  insert into tmp_aff_ts(timesheet_id)
  select distinct u2.timesheet_id
  from tmp_mode_a_ref_set_upd u2
  where u2.timesheet_id is not null
  on conflict do nothing;

  v_steps := v_steps || jsonb_build_array(
    jsonb_build_object(
      'step','MODE_A_MATCHED_REF_SET',
      'ref_set_count', v_mode_a_ref_set_count
    )
  );

  -- ─────────────────────────────────────────────
  -- ✅ UPDATED: tmp_val_upsert now computes new_pre_validated
  --   new_pre_validated = true when:
  --     - validation result is OK/OVERRIDDEN (=> new_status VALIDATION_OK)
  --     - AND timesheet is NOT authorised yet (timesheets.authorised_at_server IS NULL)
  -- ─────────────────────────────────────────────
  create temporary table tmp_val_upsert on commit drop as
  select
    vr.timesheet_id,
    case
      when vr.overall_status in ('OK','PASS','VALIDATION_OK','OVERRIDDEN','OVERRIDE') then 'VALIDATION_OK'::public.validation_status_enum
      else 'VALIDATION_ERROR'::public.validation_status_enum
    end as new_status,
    'HEALTHROSTER_WEEKLY'::text as new_reason_code,
    case when vr.overall_status in ('OK','PASS','VALIDATION_OK','OVERRIDDEN','OVERRIDE') then v_now else null end as new_validated_at_utc,
    p_import_id as new_last_source,
    case
      when vr.overall_status in ('OK','PASS','VALIDATION_OK','OVERRIDDEN','OVERRIDE')
       and tva.timesheet_id is not null
       and tva.authorised_at_server is null
      then true
      else false
    end as new_pre_validated
  from tmp_val_rows vr
  join tmp_val_mode vm
    on vm.timesheet_id = vr.timesheet_id
  left join public.timesheets tva
    on tva.timesheet_id = vr.timesheet_id
   and tva.is_current = true
  where vm.mode = 'MODE_A'
    and vr.timesheet_id is not null;

  -- ✅ UPDATED: include pre_validated changes as "validation_changed"
  select coalesce(array_agg(distinct x.timesheet_id order by x.timesheet_id), array[]::uuid[])
  into v_validation_changed_timesheet_ids
  from (
    select u.timesheet_id
    from tmp_val_upsert u
    left join public.timesheet_validations tv
      on tv.timesheet_id = u.timesheet_id
    where tv.timesheet_id is null
       or tv.status is distinct from u.new_status
       or tv.validated_at_utc is distinct from u.new_validated_at_utc
       or tv.last_source is distinct from u.new_last_source
       or tv.reason_code is distinct from u.new_reason_code
       or tv.pre_validated is distinct from u.new_pre_validated
  ) as x;

  -- ✅ UPDATED: insert/upsert includes pre_validated
  insert into public.timesheet_validations(
    timesheet_id,
    status,
    reason_code,
    validated_at_utc,
    last_source,
    pre_validated,
    updated_at
  )
  select
    u.timesheet_id,
    u.new_status,
    u.new_reason_code,
    u.new_validated_at_utc,
    u.new_last_source,
    u.new_pre_validated,
    v_now
  from tmp_val_upsert u
  on conflict (timesheet_id) do update
    set status = excluded.status,
        reason_code = excluded.reason_code,
        validated_at_utc = excluded.validated_at_utc,
        last_source = excluded.last_source,
        pre_validated = excluded.pre_validated,
        updated_at = excluded.updated_at;

  get diagnostics v_validations_upserted = row_count;

  insert into tmp_aff_ts(timesheet_id)
  select distinct t.tsid
  from unnest(coalesce(v_validation_changed_timesheet_ids, array[]::uuid[])) as t(tsid)
  where t.tsid is not null
  on conflict do nothing;

  select coalesce(array_agg(distinct vr.timesheet_id), array[]::uuid[])
  into v_mismatched_tsids
  from tmp_val_rows vr
  join tmp_val_mode vm
    on vm.timesheet_id = vr.timesheet_id
  where vm.mode = 'MODE_A'
    and vr.has_mismatch is true
    and vr.timesheet_id is not null;

  -- A validation-only Weekly timesheet is eligible for configured
  -- auto-authorisation only when the immutable coverage says omissions are
  -- meaningful and every segment on the whole timesheet has one exact
  -- HealthRoster match whose reference has been durably written.  Processing
  -- one selected row, one day or one matching segment can never authorise the
  -- rest of the timesheet by implication.
  select coalesce(array_agg(distinct vr.timesheet_id order by vr.timesheet_id),array[]::uuid[])
  into v_validation_auto_authorise_timesheet_ids
  from tmp_val_rows vr
  join tmp_val_mode vm
    on vm.timesheet_id=vr.timesheet_id
   and vm.mode='MODE_A'
  join tmp_val_upsert vu
    on vu.timesheet_id=vr.timesheet_id
  join public.timesheets t
    on t.timesheet_id=vr.timesheet_id
   and t.is_current=true
   and t.revoked_at is null
  join public.hr_imports hi
    on hi.id=p_import_id
  left join public.timesheets_financials tf
    on tf.timesheet_id=t.timesheet_id
   and tf.is_current=true
  cross join lateral (
    select case
      when jsonb_typeof(t.actual_schedule_json)='array'
       and jsonb_array_length(t.actual_schedule_json)>0
        then jsonb_array_length(t.actual_schedule_json)
      when jsonb_typeof(tf.invoice_breakdown_json)='object'
       and jsonb_typeof(tf.invoice_breakdown_json->'segments')='array'
        then jsonb_array_length(tf.invoice_breakdown_json->'segments')
      else 0
    end as segment_count
  ) whole_timesheet
  where vu.new_status='VALIDATION_OK'::public.validation_status_enum
    and vu.new_pre_validated=true
    and hi.coverage_mode in ('COMPLETE_ALL','COMPLETE_SELECTED_CANDIDATES')
    and (
      hi.coverage_mode='COMPLETE_ALL'
      or exists (
        select 1
        from public.import_review_scope_candidates scoped_candidate
        where scoped_candidate.import_id=p_import_id
          and scoped_candidate.candidate_id=vr.candidate_id
      )
    )
    and jsonb_typeof(vr.row_json->'comparisons')='array'
    and jsonb_array_length(vr.row_json->'comparisons')>0
    and whole_timesheet.segment_count=jsonb_array_length(vr.row_json->'comparisons')
    and not exists (
      select 1
      from jsonb_array_elements(vr.row_json->'comparisons') comparison(value)
      where not (
        (
          upper(coalesce(comparison.value->>'match_status','')) in ('MATCH','MATCHED','OK','PASS')
          or lower(coalesce(comparison.value->>'match','false')) in ('true','1')
        )
        and lower(coalesce(comparison.value->>'time_match','false')) in ('true','1')
        and nullif(btrim(comparison.value->>'ref_after'),'') is not null
        and nullif(btrim(comparison.value->>'work_date'),'') is not null
        and nullif(btrim(comparison.value->>'timesheet_start'),'') is not null
        and nullif(btrim(comparison.value->>'timesheet_end'),'') is not null
        and exists (
          select 1
          from public.nhsp_shifts matched_shift
          where matched_shift.source_system='HEALTHROSTER'::public.hr_source_enum
            and matched_shift.cancelled_at_utc is null
            and matched_shift.timesheet_id=vr.timesheet_id
            and matched_shift.work_date=(comparison.value->>'work_date')::date
            and to_char((date_trunc('minute',matched_shift.start_utc) at time zone 'Europe/London'),'HH24:MI')=
              comparison.value->>'timesheet_start'
            and to_char((date_trunc('minute',matched_shift.end_utc) at time zone 'Europe/London'),'HH24:MI')=
              comparison.value->>'timesheet_end'
            and coalesce(matched_shift.break_mins,0)=coalesce(nullif(btrim(comparison.value->>'timesheet_break_mins'),'')::integer,0)
            and matched_shift.ref_num=comparison.value->>'ref_after'
            and matched_shift.hr_request_id=comparison.value->>'ref_after'
        )
      )
    );

  -- Query emails are intentionally outside the source transaction. The
  -- database returns selected action IDs; the Worker later calls the
  -- idempotent outbox-backed enqueue RPC after source commit.

  v_steps := v_steps || jsonb_build_array(
    jsonb_build_object(
      'step','MODE_A_VALIDATIONS_DONE',
      'val_rows_count', v_val_rows_count,
      'validations_upserted', v_validations_upserted,
      'validation_changed_timesheet_ids_count', coalesce(array_length(v_validation_changed_timesheet_ids, 1), 0),
      'mismatched_timesheet_ids_count', coalesce(array_length(v_mismatched_tsids, 1), 0),
      'email_actions_count', v_email_actions_count,
      'email_jobs_count', v_email_jobs_count,
      'mode_a_ref_cleared_count', v_mode_a_ref_cleared_count
    )
  );

  -- ─────────────────────────────────────────────
  -- ✅ Compute ref_updated_timesheet_ids (for post-apply QR reissue + tspdf regen decisions)
  -- ─────────────────────────────────────────────
  select coalesce(array_agg(distinct rts.timesheet_id order by rts.timesheet_id), array[]::uuid[])
  into v_ref_updated_timesheet_ids
  from tmp_ref_updated_ts rts
  where rts.timesheet_id is not null;

  v_ref_updated_timesheet_ids_count := coalesce(array_length(v_ref_updated_timesheet_ids, 1), 0);

  -- ─────────────────────────────────────────────
  -- 11) Compute affected_timesheet_ids (MODE_B work + MODE_A validation changes)
  -- ─────────────────────────────────────────────
  -- Build the ordinary authoritative MODE_B scope separately from the
  -- whole-timesheet MODE_A validation scope calculated above.
  -- active imported rows, protected amendment correction pairs and
  -- cancellation/reversal results.  MODE_A validation/reference work is
  -- deliberately excluded even though it remains part of the TSFIN refresh.
  select coalesce(array_agg(distinct target.timesheet_id order by target.timesheet_id),array[]::uuid[])
  into v_authoritative_affected_timesheet_ids
  from (
    select ns.timesheet_id
    from public.nhsp_shifts ns
    where ns.source_system='HEALTHROSTER'::public.hr_source_enum
      and ns.client_id=v_import_client_id
      and ns.cancelled_at_utc is null
      and ns.external_row_key=any(coalesce(v_force_keys_final,array[]::text[]))
      -- Protected changed-hours rows are represented financially by their
      -- immutable reversal/replacement pair.  The source shift remains linked
      -- to the root for import identity, but refreshing that settled root as
      -- well would count the same delta twice in the correction-chain
      -- residual (live root truth plus the signed pair).
      and not (
        ns.external_row_key=any(
          coalesce(v_invoiced_changed_keys,array[]::text[])
        )
      )
      and ns.timesheet_id is not null
    union all
    select phase3_created.value::uuid
    from jsonb_array_elements_text(coalesce(v_phase3_result->'created_timesheet_ids','[]'::jsonb)) phase3_created(value)
    union all
    select phase3_updated.value::uuid
    from jsonb_array_elements_text(coalesce(v_phase3_result->'updated_timesheet_ids','[]'::jsonb)) phase3_updated(value)
    union all
    select cancelled.value::uuid
    from jsonb_array_elements_text(coalesce(v_cancellations_result->'affected_timesheet_ids','[]'::jsonb)) cancelled(value)
  ) target
  where target.timesheet_id is not null;

  -- A protected correction must remain a complete TSFIN/lifecycle unit even
  -- when this batch changes only its mutable replacement member.
  select coalesce(array_agg(distinct expanded.timesheet_id order by expanded.timesheet_id),array[]::uuid[])
  into v_authoritative_affected_timesheet_ids
  from (
    select requested.timesheet_id
    from unnest(coalesce(v_authoritative_affected_timesheet_ids,array[]::uuid[])) requested(timesheet_id)
    where requested.timesheet_id is not null
    union
    select partner.timesheet_id
    from unnest(coalesce(v_authoritative_affected_timesheet_ids,array[]::uuid[])) requested(timesheet_id)
    join public.timesheets seed
      on seed.timesheet_id=requested.timesheet_id
     and seed.is_current=true
     and seed.correction_id is not null
     and upper(btrim(coalesce(seed.adjustment_origin,''))) in (
       'IMPORT_CORRECTION','IMPORT_CANCELLATION','HEALTHROSTER_CHANGED_HOURS',
       'NHSP_CHANGED_HOURS','HEALTHROSTER_CANCELLATION','NHSP_CANCELLATION'
     )
    join public.timesheets partner
      on partner.correction_id=seed.correction_id
     and partner.is_current=true
     and upper(btrim(coalesce(partner.adjustment_origin,''))) in (
       'IMPORT_CORRECTION','IMPORT_CANCELLATION','HEALTHROSTER_CHANGED_HOURS',
       'NHSP_CHANGED_HOURS','HEALTHROSTER_CANCELLATION','NHSP_CANCELLATION'
     )
  ) expanded
  where expanded.timesheet_id is not null;
  insert into tmp_aff_ts(timesheet_id)
  select authoritative.timesheet_id
  from unnest(coalesce(v_authoritative_affected_timesheet_ids,array[]::uuid[])) authoritative(timesheet_id)
  where authoritative.timesheet_id is not null
  on conflict do nothing;

  select coalesce(array_agg(distinct a.timesheet_id order by a.timesheet_id), array[]::uuid[])
  into v_affected_timesheet_ids
  from tmp_aff_ts a
  where a.timesheet_id is not null
    -- A protected source is immutable financial history. It can enter the
    -- generic affected set through MODE_A validation/reference bookkeeping
    -- even though the authoritative MODE_B scope above correctly selected
    -- only the new correction pair. Never let that bookkeeping requeue the
    -- settled root for TSFIN recalculation.
    and not (
      a.timesheet_id=any(
        coalesce(v_protected_source_timesheet_ids,array[]::uuid[])
      )
    );

  -- Restore every previously-authorised mutable source and authorise every
  -- financial correction member regardless of the ordinary setting.  A
  -- changed-hours reversal/replacement pair therefore moves together, while
  -- a true cancellation contributes its reversal only.
  select coalesce(array_agg(distinct required.timesheet_id order by required.timesheet_id),array[]::uuid[])
  into v_reauthorise_timesheet_ids
  from (
    select existing.timesheet_id
    from unnest(coalesce(v_reauthorise_timesheet_ids,array[]::uuid[])) existing(timesheet_id)
    union all
    select correction.timesheet_id
    from unnest(coalesce(v_authoritative_affected_timesheet_ids,array[]::uuid[])) affected(timesheet_id)
    join public.timesheets correction on correction.timesheet_id=affected.timesheet_id
    where correction.is_current=true
      and correction.revoked_at is null
      and coalesce(correction.is_adjustment,false)
      and correction.correction_id is not null
  ) required
  where required.timesheet_id is not null;

  if to_regclass('pg_temp.import_review_reconciliation_units_v1') is not null then
    if exists(
      select 1 from pg_temp.import_review_reconciliation_units_v1 u
      where u.route in ('AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT') and (
        nullif(u.unit_json->>'correction_id','') is null
        or jsonb_typeof(u.unit_json->'applied_member_ids')<>'array'
        or jsonb_array_length(u.unit_json->'applied_member_ids')<>2
        or (select count(*)=2
              and count(*) filter(where t.correction_kind='CHANGED_HOURS_REVERSAL')=1
              and count(*) filter(where t.correction_kind='CHANGED_HOURS_REPLACEMENT')=1
              and count(distinct t.parent_timesheet_id)=1
              and count(t.parent_timesheet_id)=2
            from public.timesheets t
            where t.correction_id=u.unit_json->>'correction_id' and t.is_current and t.archived_at_utc is null
              and t.adjustment_origin='IMPORT_CORRECTION'
              and t.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')) is not true
      )
    ) then
      raise exception 'IMPORT_REVIEW_APPLY_POSTCONDITION_FAILED' using errcode='55000',
        detail=jsonb_build_object('reason_code','CORRECTION_MEMBER_SET_INCOMPLETE')::text;
    end if;
    select coalesce(array_agg(distinct x.value::uuid order by x.value::uuid) filter (where x.value is not null),array[]::uuid[])
    into v_operation_bound_correction_timesheet_ids
    from pg_temp.import_review_reconciliation_units_v1 u
    left join lateral jsonb_array_elements_text(coalesce(u.unit_json->'applied_member_ids','[]'::jsonb)) x(value) on true
    where u.route in ('AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT');
    select coalesce(jsonb_agg(u.unit_json order by u.action_id),'[]'::jsonb) into v_reconciliation_units
    from pg_temp.import_review_reconciliation_units_v1 u;
    select coalesce(array_agg(x order by x),array[]::uuid[]) into v_reauthorise_timesheet_ids
    from unnest(coalesce(v_reauthorise_timesheet_ids,array[]::uuid[])) x
    where not (x=any(coalesce(v_operation_bound_correction_timesheet_ids,array[]::uuid[])));
  end if;

  v_auto_authorise_timesheet_ids:=public._import_review_auto_authorise_targets_core_v1(
    v_authoritative_affected_timesheet_ids,'HEALTHROSTER'::public.hr_source_enum,false
  );

  select coalesce(array_agg(distinct eligible.timesheet_id order by eligible.timesheet_id),array[]::uuid[])
  into v_auto_authorise_timesheet_ids
  from (
    select unnest(coalesce(v_auto_authorise_timesheet_ids,array[]::uuid[])) as timesheet_id
    union all
    select unnest(public._import_review_auto_authorise_targets_core_v1(
      v_validation_auto_authorise_timesheet_ids,'HEALTHROSTER'::public.hr_source_enum,true
    )) as timesheet_id
  ) eligible
  where eligible.timesheet_id is not null;
  select coalesce(array_agg(x order by x),array[]::uuid[]) into v_general_authorise_timesheet_ids
  from unnest(coalesce(v_auto_authorise_timesheet_ids,array[]::uuid[])||coalesce(v_reauthorise_timesheet_ids,array[]::uuid[])) x
  where not (x=any(coalesce(v_operation_bound_correction_timesheet_ids,array[]::uuid[])));

  if array_length(v_affected_timesheet_ids, 1) is not null then
    perform public.enqueue_ts_financials_priority(v_affected_timesheet_ids, 'CONTEXT_CHANGED'::public.ts_fin_reason_enum);
  end if;

  -- ─────────────────────────────────────────────
  -- 12) Preserve the source route.  Whole-import completion is owned by
  -- _import_review_apply_complete_core_v1 after it has proved that no
  -- deferred/selectable work or blockers remain.  An incremental batch must
  -- never make the staged import look globally applied.
  -- ─────────────────────────────────────────────
  update public.hr_imports hi3
  set import_scope = 'HR_WEEKLY'
  where hi3.id = p_import_id;

  v_steps := v_steps || jsonb_build_array(jsonb_build_object('step','IMPORT_BATCH_APPLIED'));

  -- ─────────────────────────────────────────────
  -- 13) Logging (invoice_debug only, via _imp_debug_audit)
  -- ─────────────────────────────────────────────
  perform public._imp_debug_audit(
    p_actor_user_id,
    'HR_WEEKLY_VALIDATIONS_DEBUG',
    jsonb_build_object(
      'import_id', p_import_id::text,
      'client_id', v_import_client_id::text,
      'val_rows_count', v_val_rows_count,
      'validations_upserted', v_validations_upserted,
      'validation_changed_timesheet_ids_count', coalesce(array_length(v_validation_changed_timesheet_ids, 1), 0),
      'mismatched_timesheet_ids_count', coalesce(array_length(v_mismatched_tsids, 1), 0),
      'email_actions_count', v_email_actions_count,
      'email_jobs_count', v_email_jobs_count,
      'mode_a_ref_cleared_count', v_mode_a_ref_cleared_count,
      'mode_a_ref_set_count', v_mode_a_ref_set_count,
      'ref_updated_timesheet_ids_count', v_ref_updated_timesheet_ids_count,
      'invalidation_actions_count', v_invalidation_actions_count
    ),
    'hr_imports',
    p_import_id::text,
    null,
    null,
    null,
    null
  );

  perform public._imp_debug_audit(
    p_actor_user_id,
    'HR_WEEKLY_APPLY_DEBUG',
    jsonb_build_object(
      'import_id', p_import_id::text,
      'client_id', v_import_client_id::text,
      'steps', v_steps
    ),
    'hr_imports',
    p_import_id::text,
    null,
    null,
    null,
    null
  );

  v_review_result:=jsonb_build_object(
    'import_id', p_import_id,
    'client_id', v_import_client_id,
    'mode_b', jsonb_build_object(
      'selected_truth_keys', to_jsonb(coalesce(v_selected_truth_keys_mode_b, array[]::text[])),
      'force_overwrite_external_row_keys', to_jsonb(coalesce(v_force_keys_final, array[]::text[])),
      'skip_external_row_keys', to_jsonb(coalesce(v_skip_keys_final, array[]::text[])),
      'phase1', v_phase1_result,
      'phase15', jsonb_build_object(
        'ok_rows', v_phase15_ok,
        'shift_updated_rows', v_phase15_updated
      ),
      'phase3', v_phase3_result,
      'cancellations', v_cancellations_result
    ),
    'mode_a', jsonb_build_object(
      'mirror', v_mirror_result,
      'validations_upserted', v_validations_upserted,
      'mismatched_timesheet_ids', to_jsonb(coalesce(v_mismatched_tsids, array[]::uuid[])),
      'validation_affected_timesheet_ids', to_jsonb(coalesce(v_validation_changed_timesheet_ids, array[]::uuid[])),
      'mode_a_ref_cleared_count', v_mode_a_ref_cleared_count,
      'mode_a_ref_set_count', v_mode_a_ref_set_count,
      'ref_updated_timesheet_ids', to_jsonb(coalesce(v_ref_updated_timesheet_ids, array[]::uuid[])),
      'whole_timesheet_auto_authorise_eligible_ids',to_jsonb(coalesce(v_validation_auto_authorise_timesheet_ids,array[]::uuid[]))
    ),
    'email_jobs', v_email_jobs,
    'affected_timesheet_ids', to_jsonb(coalesce(v_affected_timesheet_ids, array[]::uuid[])),
    'auto_authorise_timesheet_ids',to_jsonb(coalesce(v_auto_authorise_timesheet_ids,array[]::uuid[])),
    'post_commit_reauthorise_timesheet_ids',to_jsonb(coalesce(v_reauthorise_timesheet_ids,array[]::uuid[])),
    'reconciliation_action_ids',to_jsonb(coalesce(v_reconciliation_action_ids,array[]::text[])),
    'operation_bound_correction_action_ids',to_jsonb(coalesce(v_operation_bound_correction_action_ids,array[]::text[])),
    'general_authorise_timesheet_ids',to_jsonb(coalesce(v_general_authorise_timesheet_ids,array[]::uuid[])),
    'operation_bound_correction_timesheet_ids',to_jsonb(coalesce(v_operation_bound_correction_timesheet_ids,array[]::uuid[])),
    'reconciliation_units',coalesce(v_reconciliation_units,'[]'::jsonb),
    'validation_affected_timesheet_ids', to_jsonb(coalesce(v_validation_changed_timesheet_ids, array[]::uuid[])),
    'ref_updated_timesheet_ids', to_jsonb(coalesce(v_ref_updated_timesheet_ids, array[]::uuid[])),
    'post_commit_email_action_ids',v_post_commit_email_action_ids,
    'review_operation_id',v_review_operation_id
  );
  perform public._import_review_apply_complete_core_v1(p_import_id,v_review_operation_id,p_actor_user_id,v_review_result,
    jsonb_array_length(v_post_commit_email_action_ids)>0 or cardinality(v_affected_timesheet_ids)>0);
  return v_review_result;

exception when others then
  get stacked diagnostics v_sqlstate = returned_sqlstate, v_err = message_text;

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'HR_WEEKLY_APPLY_ERROR',
      jsonb_build_object(
        'import_id', p_import_id::text,
        'client_id', case when v_import_client_id is null then null else v_import_client_id::text end,
        'steps', v_steps,
        'sqlstate', v_sqlstate,
        'error', v_err,
        'last_shift_id', case when v_last_shift_id is null then null else v_last_shift_id::text end
      ),
      'hr_imports',
      p_import_id::text,
      null,
      null,
      null,
      null
    );
  exception when others then
    null;
  end;

  raise;
end;
$function$;

CREATE OR REPLACE FUNCTION public.nhsp_weekly_phase3_apply_adjustment_truth(p_import_id uuid, p_selected_external_row_keys text[], p_actor_user_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_now timestamptz := now();

  v_src public.hr_source_enum;

  v_selected_keys text[] := '{}';
  v_key text;
  v_last_key text := null;

  v_row jsonb;

  v_is_invoiced boolean := false;
  v_invoice_id_detected uuid := null;

  v_contract_id uuid;
  v_candidate_id uuid;
  v_client_id uuid;
  v_work_date date;

  -- Week ending date (MUST be contract-driven or base-timesheet driven; not assumed Sunday)
  v_week_ending_date date;
  v_base_timesheet_id uuid := null;
  v_base_week_ending_date date := null;

  -- ✅ NEW: inherit policy identity from the parent/base timesheet (so adjustments follow parent stream)
  v_parent_sheet_scope public.timesheet_scope_enum := 'WEEKLY'::public.timesheet_scope_enum;
  v_parent_submission_mode public.submission_mode_enum := 'MANUAL'::public.submission_mode_enum;

  v_contract_week_ending_weekday_snapshot int := 0;
  v_work_dow int := 0;
  v_we_delta int := 0;

  v_correction_id text;
  v_kind text;

  v_old_start_utc timestamptz;
  v_old_end_utc   timestamptz;
  v_new_start_utc timestamptz;
  v_new_end_utc   timestamptz;
  v_old_break_mins int;
  v_new_break_mins int;

  -- ✅ Keep original string forms for deterministic correction_id hashing
  v_old_start_str text := null;
  v_old_end_str text := null;
  v_new_start_str text := null;
  v_new_end_str text := null;
  v_old_break_str text := null;
  v_new_break_str text := null;

  v_seg_start_utc timestamptz;
  v_seg_end_utc timestamptz;
  v_seg_break_mins int;

  v_ref_num text := null;

  -- ✅ Evidence linkage (NHSP)
  v_shift_id uuid := null;
  v_shift_prev_import_id uuid := null;
  v_schedule_import_id uuid := null;

  -- ✅ Existing replacement (POS₀) handling to avoid stacking corrections
  v_existing_pos_ts_id uuid := null;
  v_existing_pos_correction_id text := null;
  v_existing_pos_schedule jsonb := null;
  v_existing_pos_hint jsonb := null;
  v_existing_pos_is_invoiced boolean := false;
  v_existing_pos_tf_locked_by_invoice_id uuid := null;
  v_existing_pos_tf_invoice_breakdown_json jsonb := null;
  v_existing_pos_seg_invoice_id uuid := null;
  v_existing_pos_seg jsonb := null;

  v_existing_pos_old_start_str text := null;
  v_existing_pos_old_end_str text := null;
  v_existing_pos_old_break_str text := null;
  v_existing_pos_import_id uuid := null;
  v_existing_pair_parent_timesheet_id uuid := null;

  -- ✅ NEW: Existing base reversal (NEG₀) for edge-case deletion
  v_existing_neg_ts_id uuid := null;
  v_existing_neg_schedule jsonb := null;
  v_existing_neg_is_invoiced boolean := false;
  v_existing_neg_tf_locked_by_invoice_id uuid := null;
  v_existing_neg_tf_invoice_breakdown_json jsonb := null;
  v_existing_neg_seg_invoice_id uuid := null;

  v_existing_neg_base_start_utc timestamptz := null;
  v_existing_neg_base_end_utc timestamptz := null;
  v_existing_neg_base_break_mins int := null;

  v_existing_pos_count int := 0;
  v_existing_neg_count int := 0;

  v_deleted_redundant_pair boolean := false;
  v_reconciliation_unit jsonb := null;
  v_reconciliation_route text := null;
  v_reconciliation_b_schedule jsonb := null;
  -- Historical correction finance authority (Policy X pre-draft only)
  v_chain_scope jsonb := null;
  v_financial_preflight jsonb := null;
  v_correction_financials_policy_envelope jsonb := null;
  v_correction_financials_policy_envelope_fingerprint text := null;
  v_correction_operation_id uuid := null;
  v_root_timesheet_id uuid := null;
  v_latest_positive_timesheet_id uuid := null;


  v_updated_existing_replacement boolean := false;

  -- Per-key audit helpers
  v_old_paid_minutes int := null;
  v_new_paid_minutes int := null;
  v_delta_paid_minutes int := null;

  v_invoice_number_text text := null;

  v_rev_ts_id uuid := null;
  v_rep_ts_id uuid := null;

  v_shift_date_ymd text;

  v_contract_display_site text;
  v_contract_ward_hint text;
  v_contract_role text;

  v_client_name text;
  v_candidate_display_name text;
  v_candidate_tms_ref text;

  v_booking_base text;
  v_booking_id text;
  v_hash_hex text;

  v_base_week_id uuid;

  v_existing_ts_id uuid;
  v_existing_cw_id uuid;
  v_existing_cw_seq int;
  v_existing_cw_is_adjustment boolean;

  v_next_additional_seq int;

  v_ts_id uuid;

  v_ins_count int := 0;
  v_upd_count int := 0;
  v_skipped_count int := 0;

  v_created_ts_ids uuid[] := '{}';
  v_updated_ts_ids uuid[] := '{}';

  -- fnv1a32 helper vars
  v_fnv_h bigint;
  v_fnv_i int;
  v_fnv_s text;
  v_fnv_hex text;

  v_hospital_norm text;
  v_ward_norm text;
  v_role_norm text;
  v_shift_label text;
  v_shift_label_norm text;

  v_schedule jsonb;
  v_hint jsonb;

  v_try int;

  -- debug sample (invoice_debug gated inside _imp_debug_audit)
  v_sample jsonb := '[]'::jsonb;
  v_sample_n int := 0;
  v_key_ts jsonb;
  v_kind_op text;

  v_sqlstate text;
  v_err text;
begin
  -- ---- Validate import exists and is NHSP ----
  select hi.source_system
  into v_src
  from public.hr_imports hi
  where hi.id = p_import_id
  limit 1;

  if v_src is null then
    raise exception 'nhsp_weekly_phase3_apply_adjustment_truth: import_not_found (import_id=%)', p_import_id;
  end if;

  if v_src <> 'NHSP'::public.hr_source_enum then
    raise exception
      'nhsp_weekly_phase3_apply_adjustment_truth: source_system_mismatch (import_id=% actual=% expected=NHSP)',
      p_import_id, v_src;
  end if;

  -- ---- Normalise selected keys ----
  select coalesce(array_agg(distinct btrim(k)), '{}')
  into v_selected_keys
  from unnest(coalesce(p_selected_external_row_keys, '{}'::text[])) as k
  where k is not null and btrim(k) <> '';

  if array_length(v_selected_keys, 1) is null then
    return jsonb_build_object(
      'import_id', p_import_id,
      'selected_count', 0,
      'skipped_count', 0,
      'inserted_count', 0,
      'updated_count', 0,
      'created_timesheet_ids', '[]'::jsonb,
      'updated_timesheet_ids', '[]'::jsonb
    );
  end if;

  -- ---- Load Phase 3 rows for selected keys into a lookup ----
  create temporary table tmp_phase3_by_key(
    external_row_key text primary key,
    row_json jsonb not null
  ) on commit drop;

  insert into tmp_phase3_by_key(external_row_key, row_json)
  select
    r.external_row_key,
    to_jsonb(r) as row_json
  from public.weekly_import_changed_hours_phase3(
    p_import_id := p_import_id,
    p_system_type := 'NHSP'
  ) as r
  where r.external_row_key = any(v_selected_keys)
  on conflict (external_row_key) do nothing;

  -- ---- Process each selected key ----
  foreach v_key in array v_selected_keys loop
    v_last_key := v_key;

    -- reset per-key flags
    v_updated_existing_replacement := false;
    v_deleted_redundant_pair := false;
    v_reconciliation_unit := null;
    v_reconciliation_route := null;
    v_reconciliation_b_schedule := null;

    if to_regclass('pg_temp.import_review_reconciliation_units_v1') is not null then
      select u.unit_json into v_reconciliation_unit
      from pg_temp.import_review_reconciliation_units_v1 u
      where u.source_identity=v_key;
      if v_reconciliation_unit is not null then
        if v_reconciliation_unit->>'source_system'<>'NHSP'
           or v_reconciliation_unit->>'route' not in ('AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT') then
          raise exception 'IMPORT_REVIEW_RECONCILIATION_UNIT_INVALID' using errcode='22023';
        end if;
        v_reconciliation_route:=v_reconciliation_unit->>'route';
        v_reconciliation_b_schedule:=coalesce(v_reconciliation_unit->'B_standard_schedule_json','[]'::jsonb);
      end if;
    end if;

    v_existing_pos_ts_id := null;
    v_existing_pos_correction_id := null;
    v_existing_pos_schedule := null;
    v_existing_pos_is_invoiced := false;
    v_existing_pos_tf_locked_by_invoice_id := null;
    v_existing_pos_tf_invoice_breakdown_json := null;
    v_existing_pos_seg_invoice_id := null;
    v_existing_pos_seg := null;
    v_existing_pos_old_start_str := null;
    v_existing_pos_old_end_str := null;
    v_existing_pos_old_break_str := null;
    v_existing_pos_import_id := null;
    v_existing_pair_parent_timesheet_id := null;

    v_existing_neg_ts_id := null;
    v_existing_neg_schedule := null;
    v_existing_neg_is_invoiced := false;
    v_existing_neg_tf_locked_by_invoice_id := null;
    v_existing_neg_tf_invoice_breakdown_json := null;
    v_existing_neg_seg_invoice_id := null;

    v_existing_neg_base_start_utc := null;
    v_existing_neg_base_end_utc := null;
    v_existing_neg_base_break_mins := null;

    v_existing_pos_count := 0;
    v_existing_neg_count := 0;

    select t.row_json
    into v_row
    from tmp_phase3_by_key t
    where t.external_row_key = v_key;

    if v_row is null then
      raise exception 'nhsp_weekly_phase3_apply_adjustment_truth: Phase 3 row not found for selected external_row_key=%', v_key;
    end if;

    -- Determine invoiced flag (from Phase3 row) for logging only
    v_is_invoiced :=
      case
        when lower(coalesce(v_row->>'is_invoiced','')) in ('true','1') then true
        else false
      end;

    begin
      v_invoice_id_detected := nullif(btrim(coalesce(v_row->>'invoice_id_detected','')), '')::uuid;
    exception when others then
      v_invoice_id_detected := null;
    end;

    -- Extract required mapping fields
    begin
      v_contract_id := (v_row->>'contract_id')::uuid;
      v_candidate_id := (v_row->>'candidate_id')::uuid;
      v_client_id := (v_row->>'client_id')::uuid;
      v_work_date := (v_row->>'work_date')::date;
    exception when others then
      raise exception 'nhsp_weekly_phase3_apply_adjustment_truth: Phase 3 row missing/invalid contract_id/candidate_id/client_id/work_date for external_row_key=%', v_key;
    end;

    -- ---- Resolve week_ending_date (DO NOT assume Sunday) ----
    v_week_ending_date := null;
    v_base_timesheet_id := null;
    v_base_week_ending_date := null;

    -- ✅ reset inherited policy identity defaults for this key (avoid leaking previous key’s parent settings)
    v_parent_sheet_scope := 'WEEKLY'::public.timesheet_scope_enum;
    v_parent_submission_mode := 'MANUAL'::public.submission_mode_enum;

    -- 1) Prefer base timesheet week_ending_date when timesheet_id exists (authoritative)
    begin
      v_base_timesheet_id := nullif(btrim(coalesce(v_row->>'timesheet_id','')), '')::uuid;
    exception when others then
      v_base_timesheet_id := null;
    end;
    if v_base_timesheet_id is not null then
      select
        ts.week_ending_date,
        ts.sheet_scope,
        ts.submission_mode
      into
        v_base_week_ending_date,
        v_parent_sheet_scope,
        v_parent_submission_mode
      from public.timesheets ts
      where ts.timesheet_id = v_base_timesheet_id
        and ts.is_current = true
      limit 1;

      if v_base_week_ending_date is not null then
        v_week_ending_date := v_base_week_ending_date;
      end if;
    end if;

    -- 2) Next: use week_ending_date present on Phase3 row if provided
    if v_week_ending_date is null then
      begin
        v_week_ending_date := nullif(btrim(coalesce(v_row->>'week_ending_date','')), '')::date;
      exception when others then
        v_week_ending_date := null;
      end;
    end if;

    -- 3) Final fallback: derive from contracts.week_ending_weekday_snapshot (0=Sunday) and work_date
    if v_week_ending_date is null then
      select coalesce(ct.week_ending_weekday_snapshot, 0)
      into v_contract_week_ending_weekday_snapshot
      from public.contracts ct
      where ct.id = v_contract_id
      limit 1;

      v_work_dow := extract(dow from v_work_date)::int; -- 0=Sun..6=Sat
      v_we_delta := ((v_contract_week_ending_weekday_snapshot - v_work_dow + 7) % 7);
      v_week_ending_date := (v_work_date + v_we_delta)::date;
    end if;

    if v_week_ending_date is null then
      raise exception 'nhsp_weekly_phase3_apply_adjustment_truth: Failed to resolve week_ending_date for external_row_key=% (contract_id=% work_date=%)', v_key, v_contract_id, v_work_date;
    end if;

    if v_base_timesheet_id is null then
      raise exception using message='CORRECTION_BASE_TIMESHEET_REQUIRED', errcode='P0001',
        detail=jsonb_build_object('code','CORRECTION_BASE_TIMESHEET_REQUIRED','external_row_key',v_key)::text;
    end if;

    begin
      v_new_paid_minutes := nullif(btrim(v_row ->> 'new_paid_minutes'), '')::integer;
    exception when invalid_text_representation or numeric_value_out_of_range then
      raise exception using message='CORRECTION_NEW_PAID_MINUTES_INVALID', errcode='22023',
        detail=jsonb_build_object('external_row_key',v_key,'new_paid_minutes',v_row ->> 'new_paid_minutes')::text;
    end;
    if v_new_paid_minutes is null then
      raise exception using message='CORRECTION_NEW_PAID_MINUTES_REQUIRED', errcode='P0001',
        detail=jsonb_build_object('external_row_key',v_key)::text;
    end if;
    if v_new_paid_minutes = 0 then
      raise exception using message='ZERO_HOURS_MUST_USE_CANCELLATION', errcode='P0001',
        detail=jsonb_build_object(
          'external_row_key',v_key,
          'required_action','CANCELLATION',
          'required_shape','REVERSAL_ONLY',
          'replacement_timesheet_required',false
        )::text;
    end if;

    select public.timesheet_correction_chain_scope_v1(
      v_base_timesheet_id, true, 32, 100
    ) into v_chain_scope;

    if coalesce((v_chain_scope->>'valid')::boolean,false) is not true
       and v_reconciliation_unit is null then
      raise exception using message='CORRECTION_CHAIN_UNRESOLVED', errcode='P0001', detail=v_chain_scope::text;
    end if;

    v_root_timesheet_id := nullif(v_chain_scope->>'root_timesheet_id','')::uuid;
    if v_root_timesheet_id is null then
      raise exception 'IMPORT_REVIEW_RECONCILIATION_PARENT_INVALID' using errcode='55000';
    end if;
    v_latest_positive_timesheet_id := coalesce(
      nullif(v_chain_scope->>'latest_positive_timesheet_id','')::uuid,
      v_base_timesheet_id
    );
    v_correction_operation_id := public._ctms_import_correction_operation_find_v1(
      p_import_id,
      v_root_timesheet_id,
      v_key,
      'CHANGED_HOURS',
      'REVERSAL_REPLACEMENT'
    );
    v_correction_financials_policy_envelope := public.correction_financials_policy_resolve_v1(
      v_base_timesheet_id,
      v_correction_operation_id,
      v_key,
      'CHANGED_HOURS',
      null::text,
      true,
      32
    );
    v_correction_financials_policy_envelope_fingerprint :=
      v_correction_financials_policy_envelope ->> 'envelope_fingerprint';

    if v_reconciliation_unit is null then
      select public.import_timesheet_financial_preflight_v1(
        p_timesheet_ids := array[v_base_timesheet_id]::uuid[],
        p_action := 'IMPORT_CHANGED_HOURS_CORRECTION',
        p_actor_user_id := p_actor_user_id,
        p_expected_state_json := jsonb_build_object(
          'chain_fingerprints',jsonb_build_object(v_root_timesheet_id::text,v_chain_scope->>'chain_fingerprint')
        ),
        p_lock_rows := true,
        p_max_scope := 100
      ) into v_financial_preflight;

      if coalesce((v_financial_preflight->>'allowed')::boolean,false) is not true then
        raise exception using message='IMPORT_FINANCIAL_PREFLIGHT_BLOCKED', errcode='P0001', detail=v_financial_preflight::text;
      end if;
    elsif nullif(current_setting('cloudtms.import_reconciliation_operation_id',true),'') is null
       or nullif(current_setting('cloudtms.import_reconciliation_request_hash',true),'') is null then
      raise exception 'IMPORT_REVIEW_RECONCILIATION_GUARD_REQUIRED' using errcode='55000';
    end if;

    -- Extract old/new shift times and break mins
    begin
      v_old_start_utc := nullif(v_row->>'old_start_utc','')::timestamptz;
      v_old_end_utc   := nullif(v_row->>'old_end_utc','')::timestamptz;
      v_new_start_utc := nullif(v_row->>'new_start_utc','')::timestamptz;
      v_new_end_utc   := nullif(v_row->>'new_end_utc','')::timestamptz;
    exception when others then
      raise exception 'nhsp_weekly_phase3_apply_adjustment_truth: Phase 3 row has invalid timestamp fields for external_row_key=%', v_key;
    end;

    if v_old_start_utc is null or v_old_end_utc is null or v_new_start_utc is null or v_new_end_utc is null then
      raise exception 'nhsp_weekly_phase3_apply_adjustment_truth: Phase 3 row missing old/new start/end timestamps for external_row_key=%', v_key;
    end if;

    begin
      v_old_break_mins := coalesce(nullif(v_row->>'old_break_mins','')::int, 0);
    exception when others then
      v_old_break_mins := 0;
    end;

    begin
      v_new_break_mins := coalesce(nullif(v_row->>'new_break_mins','')::int, 0);
    exception when others then
      v_new_break_mins := 0;
    end;

    -- ✅ Preserve string forms for deterministic correction_id hashing
    v_old_start_str := coalesce(v_row->>'old_start_utc', '');
    v_old_end_str   := coalesce(v_row->>'old_end_utc', '');
    v_new_start_str := coalesce(v_row->>'new_start_utc', '');
    v_new_end_str   := coalesce(v_row->>'new_end_utc', '');
    v_old_break_str := coalesce(v_row->>'old_break_mins', '');
    v_new_break_str := coalesce(v_row->>'new_break_mins', '');

    if v_reconciliation_unit is not null then
      if jsonb_typeof(v_reconciliation_unit->'A_schedule_json')<>'array'
         or jsonb_array_length(v_reconciliation_unit->'A_schedule_json')<>1 then
        raise exception 'IMPORT_REVIEW_RECONCILIATION_UNIT_INVALID' using errcode='22023';
      end if;
      begin
        v_new_start_utc:=(v_reconciliation_unit#>>'{A_schedule_json,0,start_utc}')::timestamptz;
        v_new_end_utc:=(v_reconciliation_unit#>>'{A_schedule_json,0,end_utc}')::timestamptz;
        v_new_break_mins:=coalesce((v_reconciliation_unit#>>'{A_schedule_json,0,break_mins}')::integer,0);
      exception when others then
        raise exception 'IMPORT_REVIEW_RECONCILIATION_UNIT_INVALID' using errcode='22023';
      end;
      if v_new_start_utc is null or v_new_end_utc is null or v_new_end_utc<=v_new_start_utc then
        raise exception 'IMPORT_REVIEW_RECONCILIATION_UNIT_INVALID' using errcode='22023';
      end if;
      v_new_start_str:=v_reconciliation_unit#>>'{A_schedule_json,0,start_utc}';
      v_new_end_str:=v_reconciliation_unit#>>'{A_schedule_json,0,end_utc}';
      v_new_break_str:=coalesce(v_reconciliation_unit#>>'{A_schedule_json,0,break_mins}','0');
    end if;

    -- Compute correction_id (stable + deterministic)
    v_fnv_s :=
      coalesce(p_import_id::text,'') || '|' ||
      coalesce(v_key,'') || '|' ||
      coalesce(v_old_start_str,'') || '|' ||
      coalesce(v_new_start_str,'') || '|' ||
      coalesce(v_old_end_str,'')   || '|' ||
      coalesce(v_new_end_str,'')   || '|' ||
      coalesce(v_old_break_str,'') || '|' ||
      coalesce(v_new_break_str,'');

    v_fnv_h := 2166136261;
    for v_fnv_i in 1..char_length(v_fnv_s) loop
      v_fnv_h := (v_fnv_h # ascii(substring(v_fnv_s from v_fnv_i for 1)));
      v_fnv_h := (v_fnv_h * 16777619) % 4294967296;
    end loop;

    v_fnv_hex := lpad(lower(to_hex(v_fnv_h)), 8, '0');
    v_correction_id := 'chg:' || p_import_id::text || ':' || v_key || ':' || v_fnv_hex;

    -- Load contract + optional client/candidate display context for norms
    select
      c.display_site,
      c.ward_hint,
      c.role
    into
      v_contract_display_site,
      v_contract_ward_hint,
      v_contract_role
    from public.contracts c
    where c.id = v_contract_id
    limit 1;

    select cl.name
    into v_client_name
    from public.clients cl
    where cl.id = v_client_id
    limit 1;

    select cand.display_name, cand.tms_ref
    into v_candidate_display_name, v_candidate_tms_ref
    from public.candidates cand
    where cand.id = v_candidate_id
    limit 1;

    v_hospital_norm := lower(coalesce(v_contract_display_site, v_client_name, v_client_id::text));
    v_ward_norm := lower(coalesce(v_contract_ward_hint, 'contract'));
    v_role_norm := lower(coalesce(v_contract_role, 'weekly'));

    v_booking_base :=
      'scope=WEEKLY' || '|' ||
      'client_id=' || coalesce(v_client_id::text,'') || '|' ||
      'candidate_id=' || coalesce(v_candidate_id::text,'') || '|' ||
      'contract_id=' || coalesce(v_contract_id::text,'') || '|' ||
      'week_ending_date=' || coalesce(v_week_ending_date::text,'') || '|' ||
      'hospital=' || v_hospital_norm || '|' ||
      'ward=' || v_ward_norm || '|' ||
      'role=' || v_role_norm;

    -- Ensure base week exists (additional_seq=0, is_adjustment=false)
    v_base_week_id := null;

    select cw0.id
    into v_base_week_id
    from public.contract_weeks cw0
    where cw0.contract_id = v_contract_id
      and cw0.week_ending_date = v_week_ending_date
      and cw0.is_adjustment is false
      and coalesce(cw0.additional_seq, 0) = 0
    limit 1
    for update;

    if v_base_week_id is null then
      insert into public.contract_weeks(
        contract_id,
        week_ending_date,
        additional_seq,
        is_adjustment,
        status,
        created_at,
        updated_at
      )
      values (
        v_contract_id,
        v_week_ending_date,
        0,
        false,
        'SUBMITTED'::public.contract_week_status_enum,
        v_now,
        v_now
      )
      returning id into v_base_week_id;
    end if;

    -- Resolve reference number for this external_row_key (used on BOTH reversal + replacement schedules)
    v_ref_num := null;

    select ns_ref.ref_num
    into v_ref_num
    from public.nhsp_shifts ns_ref
    where ns_ref.source_system = 'NHSP'::public.hr_source_enum
      and ns_ref.external_row_key = v_key
    order by ns_ref.updated_at desc nulls last, ns_ref.created_at desc nulls last
    limit 1;

    if nullif(btrim(coalesce(v_ref_num,'')), '') is null then
      v_ref_num := nullif(btrim(coalesce(v_row->>'ref_num', v_row->>'reference', '')), '');
    end if;

    if nullif(btrim(coalesce(v_ref_num,'')), '') is null then
      v_ref_num := nullif(btrim(split_part(v_key, '|', 5)), '');
    end if;

    -- ✅ Resolve shift_id + previous import id (used for evidence on schedules)
    v_shift_id := null;
    v_shift_prev_import_id := null;

    select
      ns0.id,
      ns0.latest_import_id
    into
      v_shift_id,
      v_shift_prev_import_id
    from public.nhsp_shifts ns0
    where ns0.source_system = 'NHSP'::public.hr_source_enum
      and ns0.external_row_key = v_key
      and ns0.cancelled_at_utc is null
    order by ns0.updated_at desc nulls last, ns0.created_at desc nulls last
    limit 1;

    if v_shift_id is null then
      raise exception 'nhsp_weekly_phase3_apply_adjustment_truth: cannot resolve nhsp_shifts.id (shift_id) for external_row_key=% (required for evidence linkage).', v_key;
    end if;

    -- ✅ Find current POS (replacement) for this shift
    select count(*)::int
    into v_existing_pos_count
    from public.timesheets tpos_cnt
    where tpos_cnt.is_adjustment is true
      and tpos_cnt.is_current is true
      and tpos_cnt.correction_kind = 'CHANGED_HOURS_REPLACEMENT'
      and jsonb_typeof(tpos_cnt.actual_schedule_json) = 'array'
      and tpos_cnt.actual_schedule_json @> jsonb_build_array(
        jsonb_build_object(
          'shift_id', v_shift_id::text,
          'external_row_key', v_key
        )
      );

    select
      tpos.timesheet_id,
      tpos.correction_id,
      tpos.actual_schedule_json,
      coalesce(tpos.candidate_hint_text,tpos.qr_payload_json,'{}'::jsonb)
    into
      v_existing_pos_ts_id,
      v_existing_pos_correction_id,
      v_existing_pos_schedule,
      v_existing_pos_hint
    from public.timesheets tpos
    where tpos.is_adjustment is true
      and tpos.is_current is true
      and tpos.correction_kind = 'CHANGED_HOURS_REPLACEMENT'
      and jsonb_typeof(tpos.actual_schedule_json) = 'array'
      and tpos.actual_schedule_json @> jsonb_build_array(
        jsonb_build_object(
          'shift_id', v_shift_id::text,
          'external_row_key', v_key
        )
      )
    order by tpos.updated_at desc nulls last, tpos.created_at desc nulls last
    limit 1;

    if v_existing_pos_ts_id is not null then
      select
        tf.locked_by_invoice_id,
        tf.invoice_breakdown_json
      into
        v_existing_pos_tf_locked_by_invoice_id,
        v_existing_pos_tf_invoice_breakdown_json
      from public.timesheets_financials tf
      where tf.timesheet_id = v_existing_pos_ts_id
        and tf.is_current = true
      order by tf.created_at desc
      limit 1;

      v_existing_pos_seg_invoice_id := null;

      begin
        select
          nullif(btrim(coalesce(s2.seg->>'invoice_locked_invoice_id','')), '')::uuid
        into v_existing_pos_seg_invoice_id
        from (
          select s2.seg
          from jsonb_array_elements(
            case
              when v_existing_pos_tf_invoice_breakdown_json is not null
               and jsonb_typeof(v_existing_pos_tf_invoice_breakdown_json) = 'object'
               and jsonb_typeof(v_existing_pos_tf_invoice_breakdown_json->'segments') = 'array'
              then v_existing_pos_tf_invoice_breakdown_json->'segments'
              else '[]'::jsonb
            end
          ) as s2(seg)
          where nullif(btrim(coalesce(s2.seg->>'invoice_locked_invoice_id','')), '') is not null
          limit 1
        ) as s2;
      exception when others then
        v_existing_pos_seg_invoice_id := null;
      end;

      v_existing_pos_is_invoiced :=
        (v_existing_pos_tf_locked_by_invoice_id is not null)
        or (v_existing_pos_seg_invoice_id is not null);
    end if;

    -- ✅ Find current NEG (base reversal) for this shift (needed for edge-case deletion)
    select count(*)::int
    into v_existing_neg_count
    from public.timesheets tneg_cnt
    where tneg_cnt.is_adjustment is true
      and tneg_cnt.is_current is true
      and tneg_cnt.correction_kind = 'CHANGED_HOURS_REVERSAL'
      and jsonb_typeof(tneg_cnt.actual_schedule_json) = 'array'
      and tneg_cnt.actual_schedule_json @> jsonb_build_array(
        jsonb_build_object(
          'shift_id', v_shift_id::text,
          'external_row_key', v_key
        )
      );

    select
      tneg.timesheet_id,
      tneg.actual_schedule_json
    into
      v_existing_neg_ts_id,
      v_existing_neg_schedule
    from public.timesheets tneg
    where tneg.is_adjustment is true
      and tneg.is_current is true
      and tneg.correction_kind = 'CHANGED_HOURS_REVERSAL'
      and jsonb_typeof(tneg.actual_schedule_json) = 'array'
      and tneg.actual_schedule_json @> jsonb_build_array(
        jsonb_build_object(
          'shift_id', v_shift_id::text,
          'external_row_key', v_key
        )
      )
    order by tneg.updated_at desc nulls last, tneg.created_at desc nulls last
    limit 1;

    if v_existing_neg_ts_id is not null then
      select
        tf.locked_by_invoice_id,
        tf.invoice_breakdown_json
      into
        v_existing_neg_tf_locked_by_invoice_id,
        v_existing_neg_tf_invoice_breakdown_json
      from public.timesheets_financials tf
      where tf.timesheet_id = v_existing_neg_ts_id
        and tf.is_current = true
      order by tf.created_at desc
      limit 1;

      v_existing_neg_seg_invoice_id := null;

      begin
        select
          nullif(btrim(coalesce(s3.seg->>'invoice_locked_invoice_id','')), '')::uuid
        into v_existing_neg_seg_invoice_id
        from (
          select s3.seg
          from jsonb_array_elements(
            case
              when v_existing_neg_tf_invoice_breakdown_json is not null
               and jsonb_typeof(v_existing_neg_tf_invoice_breakdown_json) = 'object'
               and jsonb_typeof(v_existing_neg_tf_invoice_breakdown_json->'segments') = 'array'
              then v_existing_neg_tf_invoice_breakdown_json->'segments'
              else '[]'::jsonb
            end
          ) as s3(seg)
          where nullif(btrim(coalesce(s3.seg->>'invoice_locked_invoice_id','')), '') is not null
          limit 1
        ) as s3;
      exception when others then
        v_existing_neg_seg_invoice_id := null;
      end;

      v_existing_neg_is_invoiced :=
        (v_existing_neg_tf_locked_by_invoice_id is not null)
        or (v_existing_neg_seg_invoice_id is not null);
    end if;

    -- Policy X retained-history rule: retain prior correction members even when
    -- the latest truth returns to the original schedule. The pair is amended to
    -- a zero residual only after its canonical pair lifecycle transition.
    v_deleted_redundant_pair := false;

    if v_reconciliation_unit is not null then
      if jsonb_typeof(v_reconciliation_b_schedule)<>'array' or jsonb_array_length(v_reconciliation_b_schedule)<>1 then
        raise exception 'IMPORT_REVIEW_EFFECTIVE_POSITION_NOT_STANDARD_REPRESENTABLE' using errcode='55000';
      end if;
      v_existing_pos_old_start_str:=v_reconciliation_b_schedule#>>'{0,start_utc}';
      v_existing_pos_old_end_str:=v_reconciliation_b_schedule#>>'{0,end_utc}';
      v_existing_pos_old_break_str:=coalesce(v_reconciliation_b_schedule#>>'{0,break_mins}','0');
      v_old_start_utc:=v_existing_pos_old_start_str::timestamptz;
      v_old_end_utc:=v_existing_pos_old_end_str::timestamptz;
      v_old_break_mins:=v_existing_pos_old_break_str::integer;
      v_old_start_str:=v_existing_pos_old_start_str;
      v_old_end_str:=v_existing_pos_old_end_str;
      v_old_break_str:=v_existing_pos_old_break_str;
      if v_reconciliation_route='AMEND_EXISTING_REPLACEMENT' then
        v_correction_id:=v_reconciliation_unit->>'correction_id';
        if nullif(v_correction_id,'') is null then
          raise exception 'IMPORT_REVIEW_RECONCILIATION_UNIT_INVALID' using errcode='22023';
        end if;
      end if;
      if v_reconciliation_route='CREATE_REVERSAL_REPLACEMENT'
         and v_existing_pos_ts_id is not null and v_existing_pos_is_invoiced then
        v_base_timesheet_id:=v_existing_pos_ts_id;
      elsif nullif(v_reconciliation_unit->>'parent_timesheet_id','') is not null then
        v_base_timesheet_id:=(v_reconciliation_unit->>'parent_timesheet_id')::uuid;
      end if;
      if not exists(select 1 from public.timesheets parent_ts
        where parent_ts.timesheet_id=v_base_timesheet_id and parent_ts.is_current and parent_ts.archived_at_utc is null) then
        raise exception 'IMPORT_REVIEW_RECONCILIATION_PARENT_INVALID' using errcode='55000';
      end if;
      v_existing_pos_ts_id:=null;
      v_existing_pos_is_invoiced:=false;
      v_existing_neg_ts_id:=null;
      v_existing_neg_is_invoiced:=false;
    end if;

        -- If the latest POS is invoiced, the new series must reverse POS (not the original base).
    if v_existing_pos_ts_id is not null and v_existing_pos_is_invoiced is true then
      v_existing_pos_seg := null;
      v_existing_pos_old_start_str := null;
      v_existing_pos_old_end_str := null;
      v_existing_pos_old_break_str := null;
      v_existing_pos_import_id := null;

      -- ✅ Treat the invoiced POS as the effective parent for policy inheritance
      v_base_timesheet_id := v_existing_pos_ts_id;

      select
        coalesce(ts.sheet_scope, 'WEEKLY'::public.timesheet_scope_enum),
        coalesce(ts.submission_mode, 'MANUAL'::public.submission_mode_enum)
      into
        v_parent_sheet_scope,
        v_parent_submission_mode
      from public.timesheets ts
      where ts.timesheet_id = v_base_timesheet_id
        and ts.is_current = true
      limit 1;

      if not found then
        v_parent_sheet_scope := 'WEEKLY'::public.timesheet_scope_enum;
        v_parent_submission_mode := 'MANUAL'::public.submission_mode_enum;
      end if;

      if v_existing_pos_schedule is not null and jsonb_typeof(v_existing_pos_schedule) = 'array' then
        v_existing_pos_seg := v_existing_pos_schedule->0;
      end if;

      if v_existing_pos_seg is not null then
        v_existing_pos_old_start_str := nullif(btrim(coalesce(v_existing_pos_seg->>'start_utc','')), '');
        v_existing_pos_old_end_str   := nullif(btrim(coalesce(v_existing_pos_seg->>'end_utc','')), '');
        v_existing_pos_old_break_str := nullif(btrim(coalesce(v_existing_pos_seg->>'break_mins','')), '');


        begin
          if (v_existing_pos_seg ? 'import_id')
             and (v_existing_pos_seg->>'import_id') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
            v_existing_pos_import_id := (v_existing_pos_seg->>'import_id')::uuid;
          else
            v_existing_pos_import_id := null;
          end if;
        exception when others then
          v_existing_pos_import_id := null;
        end;

        begin
          if v_existing_pos_old_start_str is not null then
            v_old_start_utc := v_existing_pos_old_start_str::timestamptz;
          end if;
        exception when others then
          null;
        end;

        begin
          if v_existing_pos_old_end_str is not null then
            v_old_end_utc := v_existing_pos_old_end_str::timestamptz;
          end if;
        exception when others then
          null;
        end;

        begin
          if v_existing_pos_old_break_str is not null and v_existing_pos_old_break_str ~ '^[0-9]+$' then
            v_old_break_mins := v_existing_pos_old_break_str::int;
          end if;
        exception when others then
          null;
        end;

        -- Override evidence "previous import" to the POS import_id when present (so new NEG shows correct raw row)
        if v_existing_pos_import_id is not null then
          v_shift_prev_import_id := v_existing_pos_import_id;
        end if;

        -- Recompute correction_id deterministically using POS-as-old values (stable strings)
        v_old_start_str := coalesce(v_existing_pos_old_start_str, v_old_start_str);
        v_old_end_str   := coalesce(v_existing_pos_old_end_str, v_old_end_str);
        v_old_break_str := coalesce(v_existing_pos_old_break_str, v_old_break_str);

        v_fnv_s :=
          coalesce(p_import_id::text,'') || '|' ||
          coalesce(v_key,'') || '|' ||
          coalesce(v_old_start_str,'') || '|' ||
          coalesce(v_new_start_str,'') || '|' ||
          coalesce(v_old_end_str,'')   || '|' ||
          coalesce(v_new_end_str,'')   || '|' ||
          coalesce(v_old_break_str,'') || '|' ||
          coalesce(v_new_break_str,'');

        v_fnv_h := 2166136261;
        for v_fnv_i in 1..char_length(v_fnv_s) loop
          v_fnv_h := (v_fnv_h # ascii(substring(v_fnv_s from v_fnv_i for 1)));
          v_fnv_h := (v_fnv_h * 16777619) % 4294967296;
        end loop;

        v_fnv_hex := lpad(lower(to_hex(v_fnv_h)), 8, '0');
        v_correction_id := 'chg:' || p_import_id::text || ':' || v_key || ':' || v_fnv_hex;
      end if;
    end if;

    -- Reset per-key outputs (so we can write a single meaningful audit entry)
    v_rev_ts_id := null;
    v_rep_ts_id := null;

    -- Best-effort invoice number lookup for UI using the current invoices.invoice_no column only.
    -- Do not reference legacy/stale invoice_number or number columns.
    v_invoice_number_text := null;

    if v_invoice_id_detected is not null then
      begin
        select nullif(btrim(coalesce(i.invoice_no::text, '')), '')
        into v_invoice_number_text
        from public.invoices as i
        where i.id = v_invoice_id_detected
        limit 1;
      exception when undefined_table then
        v_invoice_number_text := null;
      when others then
        v_invoice_number_text := null;
      end;
    end if;

    -- Paid minutes (prefer Phase3 row fields; fallback to timestamp diff - break mins)
    begin
      v_old_paid_minutes := nullif(btrim(coalesce(v_row->>'old_paid_minutes','')), '')::int;
    exception when others then
      v_old_paid_minutes := null;
    end;

    begin
      v_new_paid_minutes := nullif(btrim(coalesce(v_row->>'new_paid_minutes','')), '')::int;
    exception when others then
      v_new_paid_minutes := null;
    end;

    if v_old_paid_minutes is null then
      v_old_paid_minutes :=
        greatest(
          0,
          (extract(epoch from (v_old_end_utc - v_old_start_utc)) / 60)::int - coalesce(v_old_break_mins, 0)
        );
    end if;

    if v_new_paid_minutes is null then
      v_new_paid_minutes :=
        greatest(
          0,
          (extract(epoch from (v_new_end_utc - v_new_start_utc)) / 60)::int - coalesce(v_new_break_mins, 0)
        );
    end if;

    v_delta_paid_minutes := coalesce(v_new_paid_minutes, 0) - coalesce(v_old_paid_minutes, 0);

    v_key_ts := coalesce(v_key_ts, '[]'::jsonb);

    -- ✅ Case A: if latest POS exists and is NOT invoiced -> update POS in place and do NOT create new series.
    if v_existing_pos_ts_id is not null and v_existing_pos_is_invoiced is false then
      -- Use existing POS correction_id for audit consistency
      if nullif(btrim(coalesce(v_existing_pos_correction_id,'')), '') is not null then
        v_correction_id := v_existing_pos_correction_id;
      end if;

      -- Build replacement schedule from NEW truth (the new import)
      v_shift_date_ymd := to_char((v_new_start_utc at time zone 'Europe/London')::date, 'YYYY-MM-DD');

      v_schedule := jsonb_build_array(
        jsonb_build_object(
          'date', v_shift_date_ymd,
          'ward', nullif(btrim(coalesce(v_contract_ward_hint,'contract')), ''),
          'start_utc', v_new_start_utc::text,
          'end_utc', v_new_end_utc::text,
          'break_mins', greatest(0, v_new_break_mins),
          'ref_num', nullif(btrim(coalesce(v_ref_num,'')), ''),
          'shift_id', v_shift_id::text,
          'external_row_key', v_key,
          'import_id', p_import_id::text
        )
      );

      v_hint := jsonb_build_object(
        'import_correction', jsonb_build_object(
          'import_id', p_import_id::text,
          'external_row_key', v_key,
          'correction_id', v_correction_id,
          'correction_kind', 'CHANGED_HOURS_REPLACEMENT',
          'updated_from_import_id', p_import_id::text
        )
      );

      v_hint := v_hint || jsonb_build_object(
        'correction_financials_policy_envelope', v_correction_financials_policy_envelope,
        'correction_financials_policy_envelope_fingerprint', v_correction_financials_policy_envelope_fingerprint,
        'root_timesheet_id', v_root_timesheet_id::text,
        'latest_positive_timesheet_id', coalesce(v_latest_positive_timesheet_id,v_base_timesheet_id)::text
      );
      if v_reconciliation_unit is not null then
        v_hint:=v_hint||jsonb_build_object('import_authoritative_reconciliation',jsonb_build_object(
          'operation_id',current_setting('cloudtms.import_reconciliation_operation_id',true),
          'unit_fingerprint',v_reconciliation_unit->>'unit_fingerprint','route',v_reconciliation_route,
          'source_identity',v_key));
      end if;


      -- This is an amendment within the existing correction unit, not a new
      -- correction unit. Preserve the pair's shared frozen policy envelope and
      -- operation identity; otherwise the untouched reversal and amended
      -- replacement become two invalid one-member units. Only append bounded
      -- provenance for the import that amended the mutable replacement.
      if jsonb_typeof(v_existing_pos_hint) <> 'object'
         or jsonb_typeof(v_existing_pos_hint->'correction_financials_policy_envelope') <> 'object'
         or nullif(v_existing_pos_hint#>>'{correction_financials_policy_envelope,operation,operation_id}','') is null then
        raise exception using message='EXISTING_CORRECTION_POLICY_ENVELOPE_INVALID',errcode='P0001',
          detail=jsonb_build_object(
            'code','EXISTING_CORRECTION_POLICY_ENVELOPE_INVALID',
            'timesheet_id',v_existing_pos_ts_id
          )::text;
      end if;
      v_hint := v_existing_pos_hint || jsonb_build_object(
        'import_correction',coalesce(v_existing_pos_hint->'import_correction','{}'::jsonb)
          || jsonb_build_object('updated_from_import_id',p_import_id::text)
      );
      -- Lock the complete existing correction unit before validating or
      -- repairing its shared parent identity.
      perform 1
       from public.timesheets tlock
       where tlock.correction_id = v_existing_pos_correction_id
         and tlock.is_current = true
         and tlock.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
       order by tlock.timesheet_id
       for update;

      if (
        select count(*) = 2
          and count(*) filter (where pair_check.correction_kind='CHANGED_HOURS_REVERSAL') = 1
          and count(*) filter (where pair_check.correction_kind='CHANGED_HOURS_REPLACEMENT') = 1
        from public.timesheets pair_check
        where pair_check.correction_id=v_existing_pos_correction_id
          and pair_check.is_current=true
          and pair_check.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
      ) is not true then
        raise exception using message='CORRECTION_PAIR_INCOMPLETE',errcode='P0001',
          detail=jsonb_build_object('code','CORRECTION_PAIR_INCOMPLETE','correction_id',v_existing_pos_correction_id)::text;
      end if;

      if exists (
        select 1
        from public.timesheets pair_ts
        left join public.timesheets_financials pair_tf
          on pair_tf.timesheet_id=pair_ts.timesheet_id and pair_tf.is_current=true
        where pair_ts.correction_id=v_existing_pos_correction_id
          and pair_ts.is_current=true
          and pair_ts.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
          and (
            pair_ts.authorised_at_server is not null
            or pair_tf.authorised_at_utc is not null
            or pair_tf.paid_at_utc is not null
            or pair_tf.locked_by_invoice_id is not null
            or exists (select 1 from public.invoice_lines il where il.timesheet_id=pair_ts.timesheet_id)
          )
      ) then
        raise exception using message='CORRECTION_PAIR_LIFECYCLE_TRANSITION_REQUIRED', errcode='P0001',
          detail=jsonb_build_object(
            'code','CORRECTION_PAIR_LIFECYCLE_TRANSITION_REQUIRED',
            'correction_id',v_existing_pos_correction_id,
            'required_path','PAIR_UNAUTHORISE_AMEND_RECALCULATE_REAUTHORISE'
          )::text;
      end if;

      select pair_reversal.parent_timesheet_id
      into v_existing_pair_parent_timesheet_id
      from public.timesheets pair_reversal
      where pair_reversal.correction_id=v_existing_pos_correction_id
        and pair_reversal.is_current=true
        and pair_reversal.correction_kind='CHANGED_HOURS_REVERSAL'
      limit 1;

      if v_existing_pair_parent_timesheet_id is null then
        raise exception using message='CORRECTION_PAIR_PARENT_MISSING',errcode='P0001',
          detail=jsonb_build_object(
            'code','CORRECTION_PAIR_PARENT_MISSING',
            'correction_id',v_existing_pos_correction_id
          )::text;
      end if;

      -- A previous implementation could rewrite only the mutable replacement
      -- to the latest base timesheet during replay, splitting the pair's
      -- parent identity. Repair only that exact, complete, mutable pair before
      -- continuing; lifecycle/frozen evidence was rejected above.
      update public.timesheets pair_replacement
      set parent_timesheet_id=v_existing_pair_parent_timesheet_id,
          updated_at=v_now
      where pair_replacement.correction_id=v_existing_pos_correction_id
        and pair_replacement.is_current=true
        and pair_replacement.correction_kind='CHANGED_HOURS_REPLACEMENT'
        and pair_replacement.parent_timesheet_id is distinct from v_existing_pair_parent_timesheet_id;

      if (
        select count(*) = 2
          and count(*) filter (where pair_check.correction_kind='CHANGED_HOURS_REVERSAL') = 1
          and count(*) filter (where pair_check.correction_kind='CHANGED_HOURS_REPLACEMENT') = 1
          and count(distinct pair_check.parent_timesheet_id) = 1
          and count(pair_check.parent_timesheet_id) = 2
        from public.timesheets pair_check
        where pair_check.correction_id=v_existing_pos_correction_id
          and pair_check.is_current=true
          and pair_check.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
      ) is not true then
        raise exception using message='CORRECTION_PAIR_INCOMPLETE',errcode='P0001',
          detail=jsonb_build_object('code','CORRECTION_PAIR_INCOMPLETE','correction_id',v_existing_pos_correction_id)::text;
      end if;

      update public.timesheets tup
      set
        actual_schedule_json = v_schedule,
        qr_payload_json = v_hint,
        candidate_hint_text = v_hint,

        -- ✅ inherit policy identity from base timesheet
        sheet_scope = v_parent_sheet_scope,
        submission_mode = v_parent_submission_mode,

        updated_at = v_now
      where tup.timesheet_id = v_existing_pos_ts_id;

      v_rep_ts_id := v_existing_pos_ts_id;
      v_updated_existing_replacement := true;

      v_upd_count := v_upd_count + 1;
      v_updated_ts_ids := array_append(v_updated_ts_ids, v_existing_pos_ts_id);

      v_key_ts := v_key_ts || jsonb_build_array(jsonb_build_object(
        'kind', 'CHANGED_HOURS_REPLACEMENT',
        'timesheet_id', v_existing_pos_ts_id::text,
        'op', 'UPDATED_IN_PLACE'
      ));
    end if;

    -- Two correction kinds per selected key: reversal + replacement
    if v_updated_existing_replacement is false then
      foreach v_kind in array array['CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT'] loop
        v_kind_op := null;

        if v_kind = 'CHANGED_HOURS_REVERSAL' then
          v_seg_start_utc := v_old_start_utc;
          v_seg_end_utc := v_old_end_utc;
          v_seg_break_mins := greatest(0, v_old_break_mins);
          v_schedule_import_id := v_shift_prev_import_id;
        else
          v_seg_start_utc := v_new_start_utc;
          v_seg_end_utc := v_new_end_utc;
          v_seg_break_mins := greatest(0, v_new_break_mins);
          v_schedule_import_id := p_import_id;
        end if;

        v_shift_date_ymd := to_char((v_seg_start_utc at time zone 'Europe/London')::date, 'YYYY-MM-DD');

        v_hint := jsonb_build_object(
          'import_correction', jsonb_build_object(
            'import_id', p_import_id::text,
            'external_row_key', v_key,
            'correction_id', v_correction_id,
            'correction_kind', v_kind
          )
        );

        v_hint := v_hint || jsonb_build_object(
          'correction_financials_policy_envelope', v_correction_financials_policy_envelope,
          'correction_financials_policy_envelope_fingerprint', v_correction_financials_policy_envelope_fingerprint,
          'root_timesheet_id', v_root_timesheet_id::text,
          'latest_positive_timesheet_id', coalesce(v_latest_positive_timesheet_id,v_base_timesheet_id)::text
        );
        if v_reconciliation_unit is not null then
          v_hint:=v_hint||jsonb_build_object('import_authoritative_reconciliation',jsonb_build_object(
            'operation_id',current_setting('cloudtms.import_reconciliation_operation_id',true),
            'unit_fingerprint',v_reconciliation_unit->>'unit_fingerprint','route',v_reconciliation_route,
            'source_identity',v_key));
        end if;


        v_shift_label := 'weekly-correction-' || lower(v_kind) || '-' || v_correction_id;

        v_shift_label_norm :=
          regexp_replace(
            regexp_replace(lower(trim(v_shift_label)), '\s+', ' ', 'g'),
            '[^\w\s\-@&\/,:]',
            '',
            'g'
          );

        -- ✅ booking_id must be UNIQUE per correction kind (REVERSAL vs REPLACEMENT)
        v_hash_hex := substring(
          encode(
            extensions.digest(
              convert_to(
                (v_booking_base || '|shift_label_norm=' || coalesce(v_shift_label_norm, '')),
                'utf8'
              ),
              'sha256'::text
            ),
            'hex'
          )
          from 1 for 16
        );
        v_booking_id := 'bk_' || v_hash_hex;

        -- ✅ Schedule includes evidence linkage (shift_id, external_row_key, import_id)
        v_schedule := jsonb_build_array(
          jsonb_build_object(
            'date', v_shift_date_ymd,
            'ward', nullif(btrim(coalesce(v_contract_ward_hint,'contract')), ''),
            'start_utc', v_seg_start_utc::text,
            'end_utc', v_seg_end_utc::text,
            'break_mins', v_seg_break_mins,
            'ref_num', nullif(btrim(coalesce(v_ref_num,'')), ''),
            'shift_id', v_shift_id::text,
            'external_row_key', v_key,
            'import_id', case when v_schedule_import_id is null then null else v_schedule_import_id::text end
          )
        );

        -- Idempotency: reuse existing correction timesheet (unique on correction_id+kind)
        v_existing_ts_id := null;

        select t.timesheet_id
        into v_existing_ts_id
        from public.timesheets t
        where t.correction_id = v_correction_id
          and t.correction_kind = v_kind
          and (v_reconciliation_unit is null or (t.is_current and t.archived_at_utc is null))
        order by t.is_current desc, t.version desc
        limit 1
        for update;

        if v_existing_ts_id is not null then
          -- Ensure there is an adjustment contract_week linked; reuse it if present.
          v_existing_cw_id := null;
          v_existing_cw_seq := null;
          v_existing_cw_is_adjustment := null;

          select
            cw.id,
            cw.additional_seq,
            cw.is_adjustment
          into
            v_existing_cw_id,
            v_existing_cw_seq,
            v_existing_cw_is_adjustment
          from public.contract_weeks cw
          where cw.timesheet_id = v_existing_ts_id
            and cw.contract_id = v_contract_id
            and cw.week_ending_date = v_week_ending_date
          limit 1
          for update;

          if v_existing_cw_id is not null then
            if v_existing_cw_is_adjustment is not true or coalesce(v_existing_cw_seq,0) <= 0 then
              update public.contract_weeks cw2
              set
                is_adjustment = true,
                status = 'SUBMITTED'::public.contract_week_status_enum,
                updated_at = v_now
              where cw2.id = v_existing_cw_id;
            end if;

             update public.timesheets t2
            set
              actual_schedule_json = v_schedule,
              qr_payload_json = v_hint,

              -- ✅ inherit policy identity from base timesheet
              sheet_scope = v_parent_sheet_scope,
              submission_mode = v_parent_submission_mode,
              parent_timesheet_id = v_base_timesheet_id,

              updated_at = v_now
            where t2.timesheet_id = v_existing_ts_id;

            if v_kind = 'CHANGED_HOURS_REVERSAL' then
              v_rev_ts_id := v_existing_ts_id;
            else
              v_rep_ts_id := v_existing_ts_id;
            end if;

            v_upd_count := v_upd_count + 1;
            v_updated_ts_ids := array_append(v_updated_ts_ids, v_existing_ts_id);
            v_kind_op := 'UPDATED';

            v_key_ts := v_key_ts || jsonb_build_array(jsonb_build_object(
              'kind', v_kind,
              'timesheet_id', v_existing_ts_id::text,
              'op', v_kind_op
            ));

            continue;
          end if;

          -- If we have an existing correction timesheet but no linked contract_week, create one.
          select coalesce(max(cw3.additional_seq), 0) + 1
          into v_next_additional_seq
          from public.contract_weeks cw3
          where cw3.contract_id = v_contract_id
            and cw3.week_ending_date = v_week_ending_date
            and cw3.is_adjustment is true;

          insert into public.contract_weeks(
            contract_id,
            week_ending_date,
            additional_seq,
            is_adjustment,
            status,
            timesheet_id,
            created_at,
            updated_at
          )
          values (
            v_contract_id,
            v_week_ending_date,
            v_next_additional_seq,
            true,
            'SUBMITTED'::public.contract_week_status_enum,
            v_existing_ts_id,
            v_now,
            v_now
          )
          returning id into v_existing_cw_id;
          update public.timesheets t2b
          set
            actual_schedule_json = v_schedule,
            qr_payload_json = v_hint,

            -- ✅ inherit policy identity from base timesheet
            sheet_scope = v_parent_sheet_scope,
            submission_mode = v_parent_submission_mode,
            parent_timesheet_id = v_base_timesheet_id,

            updated_at = v_now
          where t2b.timesheet_id = v_existing_ts_id;

          if v_kind = 'CHANGED_HOURS_REVERSAL' then
            v_rev_ts_id := v_existing_ts_id;
          else
            v_rep_ts_id := v_existing_ts_id;
          end if;

          v_upd_count := v_upd_count + 1;
          v_updated_ts_ids := array_append(v_updated_ts_ids, v_existing_ts_id);
          v_kind_op := 'UPDATED';

          v_key_ts := v_key_ts || jsonb_build_array(jsonb_build_object(
            'kind', v_kind,
            'timesheet_id', v_existing_ts_id::text,
            'op', v_kind_op
          ));

          continue;
        end if;

        -- No existing correction timesheet: create new adjustment contract_week + timesheet
        v_ts_id := null;

        for v_try in 1..5 loop
          select coalesce(max(cw4.additional_seq), 0) + 1
          into v_next_additional_seq
          from public.contract_weeks cw4
          where cw4.contract_id = v_contract_id
            and cw4.week_ending_date = v_week_ending_date
            and cw4.is_adjustment is true;

          begin
               insert into public.timesheets(
              booking_id,
              version,
              is_current,
              status,
              occupant_key_norm,
              hospital_norm,
              ward_norm,
              job_title_norm,
              shift_label_norm,
              week_ending_date,
              contract_id,
              sheet_scope,
              submission_mode,
              line_type,
              manual_pdf_r2_key,
              actual_schedule_json,
              additional_units_week,
              additional_units_per_day,
              day_references_json,
              qr_status,
              qr_token,
              qr_generated_at,
              qr_scanned_at,
              qr_scan_info_json,
              qr_r2_key,
              qr_payload_json,
              created_at,
              updated_at,
              is_adjustment,
              parent_timesheet_id,
              candidate_hint_text,
              correction_id,
              correction_kind,
              adjustment_origin
            )
            values (
              v_booking_id,
              1,
              true,
              'RECEIVED'::public.timesheet_status_enum,
              lower(coalesce(v_candidate_tms_ref, v_candidate_display_name, v_candidate_id::text)),
              lower(coalesce(v_contract_display_site, v_client_name, v_client_id::text)),
              lower(coalesce(v_contract_ward_hint,'contract')),
              lower(coalesce(v_contract_role,'weekly')),
              v_shift_label_norm,
              v_week_ending_date,
              v_contract_id,

              -- ✅ inherit policy identity from base timesheet
              v_parent_sheet_scope,
              v_parent_submission_mode,

              'HOURS'::public.timesheet_line_type_enum,
              null,
              v_schedule,
              '{}'::jsonb,
              '{}'::jsonb,
              '{}'::jsonb,
              null,
              null,
              null,
              null,
              '{}'::jsonb,
              null,
              v_hint,
              v_now,
              v_now,
              true,

              -- ✅ link to parent/base timesheet (may be null if not provided)
              v_base_timesheet_id,

              v_hint,
              v_correction_id,
              v_kind,
              'IMPORT_CORRECTION'
            )
            returning timesheet_id into v_ts_id;


            if v_kind = 'CHANGED_HOURS_REVERSAL' then
              v_rev_ts_id := v_ts_id;
            else
              v_rep_ts_id := v_ts_id;
            end if;

            insert into public.contract_weeks(
              contract_id,
              week_ending_date,
              additional_seq,
              is_adjustment,
              status,
              timesheet_id,
              created_at,
              updated_at
            )
            values (
              v_contract_id,
              v_week_ending_date,
              v_next_additional_seq,
              true,
              'SUBMITTED'::public.contract_week_status_enum,
              v_ts_id,
              v_now,
              v_now
            );

            v_ins_count := v_ins_count + 1;
            v_created_ts_ids := array_append(v_created_ts_ids, v_ts_id);
            v_kind_op := 'CREATED';

            v_key_ts := v_key_ts || jsonb_build_array(jsonb_build_object(
              'kind', v_kind,
              'timesheet_id', v_ts_id::text,
              'op', v_kind_op
            ));

            exit;
          exception
            when unique_violation then
              v_ts_id := null;
          end;

          exit when v_ts_id is not null;
        end loop;

        if v_ts_id is null then
          raise exception 'nhsp_weekly_phase3_apply_adjustment_truth: Failed to allocate correction timesheet/contract_week after retries (external_row_key=% kind=%)', v_key, v_kind;
        end if;

      end loop; -- kind loop
    end if; -- updated_existing_replacement

    if v_reconciliation_unit is not null then
      if v_rev_ts_id is null or v_rep_ts_id is null then
        raise exception 'IMPORT_REVIEW_APPLY_POSTCONDITION_FAILED' using errcode='55000',
          detail=jsonb_build_object('reason_code','CORRECTION_MEMBER_SET_INCOMPLETE','source_identity',v_key)::text;
      end if;
      update pg_temp.import_review_reconciliation_units_v1 u
      set unit_json=u.unit_json||jsonb_build_object(
        'correction_id',v_correction_id,'M_active_member_ids',jsonb_build_array(v_rev_ts_id,v_rep_ts_id),
        'parent_timesheet_id',v_base_timesheet_id,'applied_member_ids',jsonb_build_array(v_rev_ts_id,v_rep_ts_id))
      where u.source_identity=v_key;
    end if;

    -- ─────────────────────────────────────────────
    -- ✅ User-facing audit entries (timesheet modal + invoice history)
    -- ─────────────────────────────────────────────
    begin
      -- Timesheet audit: reversal
      if v_rev_ts_id is not null then
        perform public._audit_insert(
          'timesheets',
          v_rev_ts_id::text,
          'NHSP_IMPORT_CORRECTION_APPLIED',
          null,
          jsonb_build_object(
            'import_id', p_import_id::text,
            'evidence_import_id', case when v_shift_prev_import_id is null then null else v_shift_prev_import_id::text end,
            'external_row_key', v_key,
            'shift_id', v_shift_id::text,
            'work_date', v_work_date::text,
            'ref_num', nullif(btrim(coalesce(v_ref_num,'')), ''),
            'invoice_id', case when v_invoice_id_detected is null then null else v_invoice_id_detected::text end,
            'invoice_number', v_invoice_number_text,
            'correction_id', v_correction_id,
            'correction_kind', 'CHANGED_HOURS_REVERSAL',
            'old_paid_minutes', v_old_paid_minutes,
            'new_paid_minutes', v_new_paid_minutes,
            'delta_paid_minutes', v_delta_paid_minutes,
            'counterpart_timesheet_id', case when v_rep_ts_id is null then null else v_rep_ts_id::text end,
            'op', case
                    when v_rev_ts_id = any(coalesce(v_created_ts_ids, '{}'::uuid[])) then 'CREATED'
                    when v_rev_ts_id = any(coalesce(v_updated_ts_ids, '{}'::uuid[])) then 'UPDATED'
                    else 'UPSERT'
                  end
          ),
          'IMPORT_CORRECTION',
          p_actor_user_id
        );
      end if;

      -- Timesheet audit: replacement
      if v_rep_ts_id is not null then
        perform public._audit_insert(
          'timesheets',
          v_rep_ts_id::text,
          'NHSP_IMPORT_CORRECTION_APPLIED',
          null,
          jsonb_build_object(
            'import_id', p_import_id::text,
            'evidence_import_id', p_import_id::text,
            'external_row_key', v_key,
            'shift_id', v_shift_id::text,
            'work_date', v_work_date::text,
            'ref_num', nullif(btrim(coalesce(v_ref_num,'')), ''),
            'invoice_id', case when v_invoice_id_detected is null then null else v_invoice_id_detected::text end,
            'invoice_number', v_invoice_number_text,
            'correction_id', v_correction_id,
            'correction_kind', 'CHANGED_HOURS_REPLACEMENT',
            'old_paid_minutes', v_old_paid_minutes,
            'new_paid_minutes', v_new_paid_minutes,
            'delta_paid_minutes', v_delta_paid_minutes,
            'counterpart_timesheet_id', case when v_rev_ts_id is null then null else v_rev_ts_id::text end,
            'op', case
                    when v_rep_ts_id = any(coalesce(v_created_ts_ids, '{}'::uuid[])) then 'CREATED'
                    when v_rep_ts_id = any(coalesce(v_updated_ts_ids, '{}'::uuid[])) then 'UPDATED'
                    else 'UPSERT'
                  end
          ),
          'IMPORT_CORRECTION',
          p_actor_user_id
        );
      end if;

      -- Invoice history entry
      if v_invoice_id_detected is not null then
        perform public._inv_write_audit(
          p_actor_user_id,
          'NHSP_IMPORT_CORRECTION_APPLIED',
          jsonb_build_object(
            'import_id', p_import_id::text,
            'external_row_key', v_key,
            'shift_id', v_shift_id::text,
            'work_date', v_work_date::text,
            'ref_num', nullif(btrim(coalesce(v_ref_num,'')), ''),
            'invoice_id', v_invoice_id_detected::text,
            'invoice_number', v_invoice_number_text,
            'correction_id', v_correction_id,
            'old_paid_minutes', v_old_paid_minutes,
            'new_paid_minutes', v_new_paid_minutes,
            'delta_paid_minutes', v_delta_paid_minutes,
            'reversal_timesheet_id', case when v_rev_ts_id is null then null else v_rev_ts_id::text end,
            'replacement_timesheet_id', case when v_rep_ts_id is null then null else v_rep_ts_id::text end,
            'replacement_updated_in_place', v_updated_existing_replacement,
            'redundant_pair_deleted', v_deleted_redundant_pair
          ),
          'invoices',
          v_invoice_id_detected::text,
          null,
          'IMPORT_CORRECTION',
          null,
          null,
          null
        );
      end if;
    exception when others then
      null;
    end;

    if v_sample_n < 20 then
      v_sample := v_sample || jsonb_build_array(jsonb_build_object(
        'external_row_key', v_key,
        'is_invoiced', v_is_invoiced,
        'invoice_id_detected', case when v_invoice_id_detected is null then null else v_invoice_id_detected::text end,
        'week_ending_date', v_week_ending_date::text,
        'base_timesheet_id', case when v_base_timesheet_id is null then null else v_base_timesheet_id::text end,
        'correction_id', v_correction_id,
        'replacement_updated_in_place', v_updated_existing_replacement,
        'redundant_pair_deleted', v_deleted_redundant_pair,
        'timesheets', v_key_ts
      ));
      v_sample_n := v_sample_n + 1;
    end if;

  end loop; -- selected keys loop

  perform public._imp_debug_audit(
    p_actor_user_id,
    'NHSP_CORRECTION_SERIES_DEBUG',
    jsonb_build_object(
      'import_id', p_import_id::text,
      'selected_count', coalesce(array_length(v_selected_keys, 1), 0),
      'inserted_count', v_ins_count,
      'updated_count', v_upd_count,
      'created_timesheet_ids_count', coalesce(array_length(v_created_ts_ids, 1), 0),
      'updated_timesheet_ids_count', coalesce(array_length(v_updated_ts_ids, 1), 0),
      'sample', v_sample
    ),
    'hr_imports',
    p_import_id::text,
    null,
    null,
    null,
    null
  );

  return jsonb_build_object(
    'import_id', p_import_id,
    'selected_count', coalesce(array_length(v_selected_keys, 1), 0),
    'skipped_count', v_skipped_count,
    'inserted_count', v_ins_count,
    'updated_count', v_upd_count,
    'created_timesheet_ids', to_jsonb(coalesce(v_created_ts_ids, '{}'::uuid[])),
    'updated_timesheet_ids', to_jsonb(coalesce(v_updated_ts_ids, '{}'::uuid[]))
  );

exception when others then
  get stacked diagnostics v_sqlstate = returned_sqlstate, v_err = message_text;

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'NHSP_CORRECTION_SERIES_ERROR',
      jsonb_build_object(
        'import_id', p_import_id::text,
        'last_external_row_key', v_last_key,
        'selected_count', coalesce(array_length(v_selected_keys, 1), 0),
        'inserted_count', v_ins_count,
        'updated_count', v_upd_count,
        'sqlstate', v_sqlstate,
        'error', v_err
      ),
      'hr_imports',
      p_import_id::text,
      null,
      null,
      null,
      null
    );
  exception when others then
    null;
  end;

  raise;
end;
$function$;

CREATE OR REPLACE FUNCTION public.hr_weekly_phase3_apply_adjustment_truth(p_import_id uuid, p_selected_external_row_keys text[], p_actor_user_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_now timestamptz := now();

  v_src public.hr_source_enum;

  v_selected_keys text[] := '{}';
  v_key text;
  v_last_key text := null;

  v_row jsonb;

  v_is_invoiced boolean := false;
  v_invoice_id_detected uuid := null;

  v_contract_id uuid;
  v_candidate_id uuid;
  v_client_id uuid;
  v_work_date date;

  -- Week ending date (contract-driven / base-timesheet driven; never assumed Sunday)
  v_week_ending_date date;
  v_base_timesheet_id uuid := null;
  v_base_week_ending_date date := null;

  -- ✅ NEW: inherit policy identity from parent/base timesheet
  v_effective_sheet_scope public.timesheet_scope_enum := 'WEEKLY'::public.timesheet_scope_enum;
  v_effective_submission_mode public.submission_mode_enum := 'MANUAL'::public.submission_mode_enum;

  v_contract_week_ending_weekday_snapshot int := 0;

  v_work_dow int := 0;
  v_we_delta int := 0;

  v_correction_id text;
  v_kind text;

  v_old_start_utc timestamptz;
  v_old_end_utc   timestamptz;
  v_new_start_utc timestamptz;
  v_new_end_utc   timestamptz;
  v_old_break_mins int;
  v_new_break_mins int;

  -- ✅ keep string forms for deterministic correction id (so we can re-base against prior POS)
  v_old_start_str text := null;
  v_old_end_str text := null;
  v_new_start_str text := null;
  v_new_end_str text := null;
  v_old_break_str text := null;
  v_new_break_str text := null;

  v_old_paid_minutes int := null;
  v_new_paid_minutes int := null;
  v_delta_paid_minutes int := null;

  v_seg_start_utc timestamptz;
  v_seg_end_utc timestamptz;
  v_seg_break_mins int;

  v_shift_date_ymd text;

  v_contract_display_site text;
  v_contract_ward_hint text;
  v_contract_role text;

  v_client_name text;
  v_candidate_display_name text;
  v_candidate_tms_ref text;

  v_booking_base text;
  v_booking_id text;
  v_hash_hex text;

  v_base_week_id uuid;

  v_existing_ts_id uuid;

  v_existing_cw_id uuid;
  v_existing_cw_seq int;
  v_existing_cw_is_adjustment boolean;

  v_next_additional_seq int;
  v_cw_id uuid;

  v_ts_id uuid;

  v_ins_count int := 0;
  v_upd_count int := 0;
  v_skipped_count int := 0;

  v_created_ts_ids uuid[] := '{}';
  v_updated_ts_ids uuid[] := '{}';

  -- fnv1a32 helper vars
  v_fnv_h bigint;
  v_fnv_i int;
  v_fnv_s text;
  v_fnv_hex text;

  v_candidate_norm text;
  v_hospital_norm text;
  v_ward_norm text;
  v_role_norm text;
  v_shift_label text;
  v_shift_label_norm text;

  v_schedule jsonb;
  v_hint jsonb;

  v_try int;

  -- debug sample (invoice_debug gated inside _imp_debug_audit)
  v_sample jsonb := '[]'::jsonb;
  v_sample_n int := 0;
  v_key_ts jsonb;
  v_kind_op text;

  v_sqlstate text;
  v_err text;

  -- ✅ Evidence + reference linkage
  v_shift_id uuid := null;
  v_shift_prev_import_id uuid := null;
  v_shift_hr_request_id text := null;
  v_ref_num text := null;
  v_schedule_import_id uuid := null;

  -- ✅ Per-key artefacts for user-facing audit
  v_rev_ts_id uuid := null;
  v_rep_ts_id uuid := null;
  v_rev_cw_id uuid := null;
  v_rep_cw_id uuid := null;

  -- ✅ POLICY: avoid stacking + delete redundant pair
  v_existing_pos_ts_id uuid := null;
  v_existing_pos_correction_id text := null;
  v_existing_pos_schedule jsonb := null;
  v_existing_pos_is_invoiced boolean := false;
  v_existing_pos_tf_locked_by_invoice_id uuid := null;
  v_existing_pos_tf_invoice_breakdown_json jsonb := null;
  v_existing_pos_seg_invoice_id uuid := null;
  v_existing_pos_seg jsonb := null;

  v_existing_pos_old_start_str text := null;
  v_existing_pos_old_end_str text := null;
  v_existing_pos_old_break_str text := null;
  v_existing_pos_import_id uuid := null;
  v_existing_pair_parent_timesheet_id uuid := null;

  v_existing_neg_ts_id uuid := null;
  v_existing_neg_schedule jsonb := null;
  v_existing_neg_is_invoiced boolean := false;
  v_existing_neg_tf_locked_by_invoice_id uuid := null;
  v_existing_neg_tf_invoice_breakdown_json jsonb := null;
  v_existing_neg_seg_invoice_id uuid := null;

  v_existing_neg_base_start_utc timestamptz := null;
  v_existing_neg_base_end_utc timestamptz := null;
  v_existing_neg_base_break_mins int := null;

  v_existing_pos_count int := 0;
  v_existing_neg_count int := 0;

  v_updated_existing_replacement boolean := false;
  v_deleted_redundant_pair boolean := false;
  v_reconciliation_unit jsonb := null;
  v_reconciliation_route text := null;
  v_reconciliation_b_schedule jsonb := null;
  -- Historical correction finance authority (Policy X pre-draft only)
  v_chain_scope jsonb := null;
  v_financial_preflight jsonb := null;
  v_correction_financials_policy_envelope jsonb := null;
  v_correction_financials_policy_envelope_fingerprint text := null;
  v_correction_operation_id uuid := null;
  v_root_timesheet_id uuid := null;
  v_latest_positive_timesheet_id uuid := null;

begin
  -- ---- Validate import exists and is HEALTHROSTER ----
  select hi.source_system
  into v_src
  from public.hr_imports hi
  where hi.id = p_import_id
  limit 1;

  if v_src is null then
    raise exception 'hr_weekly_phase3_apply_adjustment_truth: import_not_found (import_id=%)', p_import_id;
  end if;

  if v_src <> 'HEALTHROSTER'::public.hr_source_enum then
    raise exception
      'hr_weekly_phase3_apply_adjustment_truth: source_system_mismatch (import_id=% actual=% expected=HEALTHROSTER)',
      p_import_id, v_src;
  end if;

  -- ---- Normalise selected keys ----
  select coalesce(array_agg(distinct btrim(k)), '{}')
  into v_selected_keys
  from unnest(coalesce(p_selected_external_row_keys, '{}'::text[])) as k
  where k is not null and btrim(k) <> '';

  if array_length(v_selected_keys, 1) is null then
    return jsonb_build_object(
      'import_id', p_import_id,
      'selected_count', 0,
      'skipped_count', 0,
      'inserted_count', 0,
      'updated_count', 0,
      'created_timesheet_ids', '[]'::jsonb,
      'updated_timesheet_ids', '[]'::jsonb
    );
  end if;

  -- ---- Load Phase 3 rows for selected keys into a lookup ----
  create temporary table tmp_phase3_by_key(
    external_row_key text primary key,
    row_json jsonb not null
  ) on commit drop;

  insert into tmp_phase3_by_key(external_row_key, row_json)
  select
    r.external_row_key,
    to_jsonb(r) as row_json
  from public.weekly_import_changed_hours_phase3(
    p_import_id := p_import_id,
    p_system_type := 'HEALTHROSTER'
  ) as r
  where r.external_row_key = any(v_selected_keys)
  on conflict (external_row_key) do nothing;

  -- ---- Process each selected key ----
  foreach v_key in array v_selected_keys loop
    v_last_key := v_key;

    -- reset per-key ids for user-facing audit
    v_rev_ts_id := null;
    v_rep_ts_id := null;
    v_rev_cw_id := null;
    v_rep_cw_id := null;

    -- reset policy flags
    v_existing_pos_ts_id := null;
    v_existing_pos_correction_id := null;
    v_existing_pos_schedule := null;
    v_existing_pos_is_invoiced := false;
    v_existing_pos_tf_locked_by_invoice_id := null;
    v_existing_pos_tf_invoice_breakdown_json := null;
    v_existing_pos_seg_invoice_id := null;
    v_existing_pos_seg := null;
    v_existing_pos_old_start_str := null;
    v_existing_pos_old_end_str := null;
    v_existing_pos_old_break_str := null;
    v_existing_pos_import_id := null;
    v_existing_pair_parent_timesheet_id := null;

    v_existing_neg_ts_id := null;
    v_existing_neg_schedule := null;
    v_existing_neg_is_invoiced := false;
    v_existing_neg_tf_locked_by_invoice_id := null;
    v_existing_neg_tf_invoice_breakdown_json := null;
    v_existing_neg_seg_invoice_id := null;
    v_existing_neg_base_start_utc := null;
    v_existing_neg_base_end_utc := null;
    v_existing_neg_base_break_mins := null;

    v_existing_pos_count := 0;
    v_existing_neg_count := 0;

    v_updated_existing_replacement := false;
    v_deleted_redundant_pair := false;
    v_reconciliation_unit := null;
    v_reconciliation_route := null;
    v_reconciliation_b_schedule := null;

    if to_regclass('pg_temp.import_review_reconciliation_units_v1') is not null then
      select u.unit_json into v_reconciliation_unit
      from pg_temp.import_review_reconciliation_units_v1 u
      where u.source_identity=v_key;
      if v_reconciliation_unit is not null then
        if v_reconciliation_unit->>'source_system'<>'HEALTHROSTER'
           or v_reconciliation_unit->>'route' not in ('AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT') then
          raise exception 'IMPORT_REVIEW_RECONCILIATION_UNIT_INVALID' using errcode='22023';
        end if;
        v_reconciliation_route:=v_reconciliation_unit->>'route';
        v_reconciliation_b_schedule:=coalesce(v_reconciliation_unit->'B_standard_schedule_json','[]'::jsonb);
      end if;
    end if;

    select t.row_json
    into v_row
    from tmp_phase3_by_key t
    where t.external_row_key = v_key;

    if v_row is null then
      raise exception 'hr_weekly_phase3_apply_adjustment_truth: Phase 3 row not found for selected external_row_key=%', v_key;
    end if;

    -- Determine invoiced flag (from Phase3 row) for logging only
    v_is_invoiced :=
      case
        when lower(coalesce(v_row->>'is_invoiced','')) in ('true','1') then true
        else false
      end;

    begin
      v_invoice_id_detected := nullif(btrim(coalesce(v_row->>'invoice_id_detected','')), '')::uuid;
    exception when others then
      v_invoice_id_detected := null;
    end;

    -- Extract required mapping fields
    begin
      v_contract_id := (v_row->>'contract_id')::uuid;
      v_candidate_id := (v_row->>'candidate_id')::uuid;
      v_client_id := (v_row->>'client_id')::uuid;
      v_work_date := (v_row->>'work_date')::date;
    exception when others then
      raise exception 'hr_weekly_phase3_apply_adjustment_truth: Phase 3 row missing/invalid contract_id/candidate_id/client_id/work_date for external_row_key=%', v_key;
    end;

    -- ✅ Resolve shift_id + previous import id + request id (ref)
    begin
      v_shift_id := nullif(btrim(coalesce(v_row->>'shift_id','')), '')::uuid;
    exception when others then
      v_shift_id := null;
    end;

    if v_shift_id is null then
      select ns.id
      into v_shift_id
      from public.nhsp_shifts ns
      where ns.external_row_key = v_key
        and ns.source_system = 'HEALTHROSTER'::public.hr_source_enum
        and ns.cancelled_at_utc is null
      order by ns.updated_at desc nulls last, ns.created_at desc nulls last
      limit 1;
    end if;

    if v_shift_id is null then
      raise exception 'hr_weekly_phase3_apply_adjustment_truth: Failed to resolve shift_id for external_row_key=% (required for evidence/audit).', v_key;
    end if;

    select
      ns2.latest_import_id,
      ns2.hr_request_id
    into
      v_shift_prev_import_id,
      v_shift_hr_request_id
    from public.nhsp_shifts ns2
    where ns2.id = v_shift_id
    limit 1;

    v_ref_num := nullif(btrim(coalesce(v_shift_hr_request_id, '')), '');

    -- ---- Resolve week_ending_date (DO NOT assume Sunday) ----
    v_week_ending_date := null;
    v_base_timesheet_id := null;
    v_base_week_ending_date := null;

    -- ✅ reset inherited policy identity defaults for this key
    v_effective_sheet_scope := 'WEEKLY'::public.timesheet_scope_enum;
    v_effective_submission_mode := 'MANUAL'::public.submission_mode_enum;

    -- 1) Prefer base timesheet week_ending_date when timesheet_id exists (authoritative)
    begin
      v_base_timesheet_id := nullif(btrim(coalesce(v_row->>'timesheet_id','')), '')::uuid;
    exception when others then
      v_base_timesheet_id := null;
    end;

     if v_base_timesheet_id is not null then
      select
        ts.week_ending_date,
        ts.sheet_scope,
        ts.submission_mode
      into
        v_base_week_ending_date,
        v_effective_sheet_scope,
        v_effective_submission_mode
      from public.timesheets ts
      where ts.timesheet_id = v_base_timesheet_id
        and ts.is_current = true
      limit 1;

      if v_effective_sheet_scope is null then
        v_effective_sheet_scope := 'WEEKLY'::public.timesheet_scope_enum;
      end if;

      if v_effective_submission_mode is null then
        v_effective_submission_mode := 'MANUAL'::public.submission_mode_enum;
      end if;

      if v_base_week_ending_date is not null then
        v_week_ending_date := v_base_week_ending_date;
      end if;
    end if;

    -- 2) Next: use week_ending_date present on Phase3 row if provided
    if v_week_ending_date is null then
      begin
        v_week_ending_date := nullif(btrim(coalesce(v_row->>'week_ending_date','')), '')::date;
      exception when others then
        v_week_ending_date := null;
      end;
    end if;

    -- 3) Final fallback: derive from contracts.week_ending_weekday_snapshot (0=Sun) and work_date
    if v_week_ending_date is null then
      select coalesce(ct.week_ending_weekday_snapshot, 0)
      into v_contract_week_ending_weekday_snapshot
      from public.contracts ct
      where ct.id = v_contract_id
      limit 1;

      v_work_dow := extract(dow from v_work_date)::int; -- 0=Sun..6=Sat
      v_we_delta := ((v_contract_week_ending_weekday_snapshot - v_work_dow + 7) % 7);
      v_week_ending_date := (v_work_date + v_we_delta)::date;
    end if;

    if v_week_ending_date is null then
      raise exception 'hr_weekly_phase3_apply_adjustment_truth: Failed to resolve week_ending_date for external_row_key=% (contract_id=% work_date=%)', v_key, v_contract_id, v_work_date;
    end if;

    if v_base_timesheet_id is null then
      raise exception using message='CORRECTION_BASE_TIMESHEET_REQUIRED', errcode='P0001',
        detail=jsonb_build_object('code','CORRECTION_BASE_TIMESHEET_REQUIRED','external_row_key',v_key)::text;
    end if;

    begin
      v_new_paid_minutes := nullif(btrim(v_row ->> 'new_paid_minutes'), '')::integer;
    exception when invalid_text_representation or numeric_value_out_of_range then
      raise exception using message='CORRECTION_NEW_PAID_MINUTES_INVALID', errcode='22023',
        detail=jsonb_build_object('external_row_key',v_key,'new_paid_minutes',v_row ->> 'new_paid_minutes')::text;
    end;
    if v_new_paid_minutes is null then
      raise exception using message='CORRECTION_NEW_PAID_MINUTES_REQUIRED', errcode='P0001',
        detail=jsonb_build_object('external_row_key',v_key)::text;
    end if;
    if v_new_paid_minutes = 0 then
      raise exception using message='ZERO_HOURS_MUST_USE_CANCELLATION', errcode='P0001',
        detail=jsonb_build_object(
          'external_row_key',v_key,
          'required_action','CANCELLATION',
          'required_shape','REVERSAL_ONLY',
          'replacement_timesheet_required',false
        )::text;
    end if;

    select public.timesheet_correction_chain_scope_v1(
      v_base_timesheet_id, true, 32, 100
    ) into v_chain_scope;

    if coalesce((v_chain_scope->>'valid')::boolean,false) is not true
       and v_reconciliation_unit is null then
      raise exception using message='CORRECTION_CHAIN_UNRESOLVED', errcode='P0001', detail=v_chain_scope::text;
    end if;

    v_root_timesheet_id := nullif(v_chain_scope->>'root_timesheet_id','')::uuid;
    if v_root_timesheet_id is null then
      raise exception 'IMPORT_REVIEW_RECONCILIATION_PARENT_INVALID' using errcode='55000';
    end if;
    v_latest_positive_timesheet_id := coalesce(
      nullif(v_chain_scope->>'latest_positive_timesheet_id','')::uuid,
      v_base_timesheet_id
    );
    v_correction_operation_id := public._ctms_import_correction_operation_find_v1(
      p_import_id,
      v_root_timesheet_id,
      v_key,
      'CHANGED_HOURS',
      'REVERSAL_REPLACEMENT'
    );
    v_correction_financials_policy_envelope := public.correction_financials_policy_resolve_v1(
      v_base_timesheet_id,
      v_correction_operation_id,
      v_key,
      'CHANGED_HOURS',
      null::text,
      true,
      32
    );
    v_correction_financials_policy_envelope_fingerprint :=
      v_correction_financials_policy_envelope ->> 'envelope_fingerprint';

    if v_reconciliation_unit is null then
      select public.import_timesheet_financial_preflight_v1(
        p_timesheet_ids := array[v_base_timesheet_id]::uuid[],
        p_action := 'IMPORT_CHANGED_HOURS_CORRECTION',
        p_actor_user_id := p_actor_user_id,
        p_expected_state_json := jsonb_build_object(
          'chain_fingerprints',jsonb_build_object(v_root_timesheet_id::text,v_chain_scope->>'chain_fingerprint')
        ),
        p_lock_rows := true,
        p_max_scope := 100
      ) into v_financial_preflight;

      if coalesce((v_financial_preflight->>'allowed')::boolean,false) is not true then
        raise exception using message='IMPORT_FINANCIAL_PREFLIGHT_BLOCKED', errcode='P0001', detail=v_financial_preflight::text;
      end if;
    elsif nullif(current_setting('cloudtms.import_reconciliation_operation_id',true),'') is null
       or nullif(current_setting('cloudtms.import_reconciliation_request_hash',true),'') is null then
      raise exception 'IMPORT_REVIEW_RECONCILIATION_GUARD_REQUIRED' using errcode='55000';
    end if;

    -- Extract old/new shift times and break mins
    begin
      v_old_start_utc := nullif(v_row->>'old_start_utc','')::timestamptz;
      v_old_end_utc   := nullif(v_row->>'old_end_utc','')::timestamptz;
      v_new_start_utc := nullif(v_row->>'new_start_utc','')::timestamptz;
      v_new_end_utc   := nullif(v_row->>'new_end_utc','')::timestamptz;
    exception when others then
      raise exception 'hr_weekly_phase3_apply_adjustment_truth: Phase 3 row has invalid timestamp fields for external_row_key=%', v_key;
    end;

    if v_old_start_utc is null or v_old_end_utc is null or v_new_start_utc is null or v_new_end_utc is null then
      raise exception 'hr_weekly_phase3_apply_adjustment_truth: Phase 3 row missing old/new start/end timestamps for external_row_key=%', v_key;
    end if;

    begin
      v_old_break_mins := coalesce(nullif(v_row->>'old_break_mins','')::int, 0);
    exception when others then
      v_old_break_mins := 0;
    end;

    begin
      v_new_break_mins := coalesce(nullif(v_row->>'new_break_mins','')::int, 0);
    exception when others then
      v_new_break_mins := 0;
    end;

    -- ✅ preserve string forms for correction-id (and potential POS rebase)
    v_old_start_str := coalesce(v_row->>'old_start_utc','');
    v_old_end_str   := coalesce(v_row->>'old_end_utc','');
    v_new_start_str := coalesce(v_row->>'new_start_utc','');
    v_new_end_str   := coalesce(v_row->>'new_end_utc','');
    v_old_break_str := coalesce(v_row->>'old_break_mins','');
    v_new_break_str := coalesce(v_row->>'new_break_mins','');

    if v_reconciliation_unit is not null then
      if jsonb_typeof(v_reconciliation_unit->'A_schedule_json')<>'array'
         or jsonb_array_length(v_reconciliation_unit->'A_schedule_json')<>1 then
        raise exception 'IMPORT_REVIEW_RECONCILIATION_UNIT_INVALID' using errcode='22023';
      end if;
      begin
        v_new_start_utc:=(v_reconciliation_unit#>>'{A_schedule_json,0,start_utc}')::timestamptz;
        v_new_end_utc:=(v_reconciliation_unit#>>'{A_schedule_json,0,end_utc}')::timestamptz;
        v_new_break_mins:=coalesce((v_reconciliation_unit#>>'{A_schedule_json,0,break_mins}')::integer,0);
      exception when others then
        raise exception 'IMPORT_REVIEW_RECONCILIATION_UNIT_INVALID' using errcode='22023';
      end;
      if v_new_start_utc is null or v_new_end_utc is null or v_new_end_utc<=v_new_start_utc then
        raise exception 'IMPORT_REVIEW_RECONCILIATION_UNIT_INVALID' using errcode='22023';
      end if;
      v_new_start_str:=v_reconciliation_unit#>>'{A_schedule_json,0,start_utc}';
      v_new_end_str:=v_reconciliation_unit#>>'{A_schedule_json,0,end_utc}';
      v_new_break_str:=coalesce(v_reconciliation_unit#>>'{A_schedule_json,0,break_mins}','0');
    end if;

    -- Paid minutes (prefer Phase3 values, fallback to computed)
    begin
      v_old_paid_minutes := nullif(btrim(coalesce(v_row->>'old_paid_minutes','')), '')::int;
    exception when others then
      v_old_paid_minutes := null;
    end;

    begin
      v_new_paid_minutes := nullif(btrim(coalesce(v_row->>'new_paid_minutes','')), '')::int;
    exception when others then
      v_new_paid_minutes := null;
    end;

    if v_old_paid_minutes is null then
      v_old_paid_minutes :=
        greatest(
          0,
          (floor(extract(epoch from (v_old_end_utc - v_old_start_utc)) / 60.0))::int
          - greatest(0, coalesce(v_old_break_mins,0))
        );
    end if;

    if v_new_paid_minutes is null then
      v_new_paid_minutes :=
        greatest(
          0,
          (floor(extract(epoch from (v_new_end_utc - v_new_start_utc)) / 60.0))::int
          - greatest(0, coalesce(v_new_break_mins,0))
        );
    end if;

    v_delta_paid_minutes := coalesce(v_new_paid_minutes,0) - coalesce(v_old_paid_minutes,0);

    -- Compute correction_id (stable + deterministic)
    v_fnv_s :=
      coalesce(p_import_id::text,'') || '|' ||
      coalesce(v_key,'') || '|' ||
      coalesce(v_old_start_str,'') || '|' ||
      coalesce(v_new_start_str,'') || '|' ||
      coalesce(v_old_end_str,'') || '|' ||
      coalesce(v_new_end_str,'') || '|' ||
      coalesce(v_old_break_str,'') || '|' ||
      coalesce(v_new_break_str,'');

    v_fnv_h := 2166136261;
    for v_fnv_i in 1..char_length(v_fnv_s) loop
      v_fnv_h := (v_fnv_h # ascii(substring(v_fnv_s from v_fnv_i for 1)));
      v_fnv_h := (v_fnv_h * 16777619) % 4294967296;
    end loop;

    v_fnv_hex := lpad(lower(to_hex(v_fnv_h)), 8, '0');
    v_correction_id := 'chg:' || p_import_id::text || ':' || v_key || ':' || v_fnv_hex;

    -- Load contract + optional client/candidate display context for norms
    select
      c.display_site,
      c.ward_hint,
      c.role
    into
      v_contract_display_site,
      v_contract_ward_hint,
      v_contract_role
    from public.contracts c
    where c.id = v_contract_id
    limit 1;

    select cl.name
    into v_client_name
    from public.clients cl
    where cl.id = v_client_id
    limit 1;

    select cand.display_name, cand.tms_ref
    into v_candidate_display_name, v_candidate_tms_ref
    from public.candidates cand
    where cand.id = v_candidate_id
    limit 1;

    v_candidate_norm :=
      regexp_replace(
        regexp_replace(lower(trim(coalesce(v_candidate_tms_ref, v_candidate_display_name, v_candidate_id::text))), '\s+', ' ', 'g'),
        '[^\w\s\-@&\/,.:]',
        '',
        'g'
      );

    v_hospital_norm :=
      regexp_replace(
        regexp_replace(lower(trim(coalesce(v_contract_display_site, v_client_name, v_client_id::text))), '\s+', ' ', 'g'),
        '[^\w\s\-@&\/,.:]',
        '',
        'g'
      );

    v_ward_norm :=
      regexp_replace(
        regexp_replace(lower(trim(coalesce(v_contract_ward_hint,'contract'))), '\s+', ' ', 'g'),
        '[^\w\s\-@&\/,.:]',
        '',
        'g'
      );

    v_role_norm :=
      regexp_replace(
        regexp_replace(lower(trim(coalesce(v_contract_role,'weekly'))), '\s+', ' ', 'g'),
        '[^\w\s\-@&\/,.:]',
        '',
        'g'
      );

    -- Ensure base contract_week exists (seq=0, is_adjustment=false); never duplicate
    select cw0.id
    into v_base_week_id
    from public.contract_weeks cw0
    where cw0.contract_id = v_contract_id
      and cw0.week_ending_date = v_week_ending_date
      and cw0.additional_seq = 0
      and cw0.is_adjustment = false
    limit 1
    for update;

    if v_base_week_id is null then
      insert into public.contract_weeks(
        contract_id,
        week_ending_date,
        additional_seq,
        is_adjustment
      )
      values (
        v_contract_id,
        v_week_ending_date,
        0,
        false
      )
      returning id into v_base_week_id;
    end if;

    -- ✅ POLICY LOOKUPS: find latest POS + latest NEG for this shift linkage
    select count(*)::int
    into v_existing_pos_count
    from public.timesheets tpos_cnt
    where tpos_cnt.is_adjustment is true
      and tpos_cnt.is_current is true
      and tpos_cnt.correction_kind = 'CHANGED_HOURS_REPLACEMENT'
      and jsonb_typeof(tpos_cnt.actual_schedule_json) = 'array'
      and tpos_cnt.actual_schedule_json @> jsonb_build_array(
        jsonb_build_object('shift_id', v_shift_id::text, 'external_row_key', v_key)
      );

    select
      tpos.timesheet_id,
      tpos.correction_id,
      tpos.actual_schedule_json
    into
      v_existing_pos_ts_id,
      v_existing_pos_correction_id,
      v_existing_pos_schedule
    from public.timesheets tpos
    where tpos.is_adjustment is true
      and tpos.is_current is true
      and tpos.correction_kind = 'CHANGED_HOURS_REPLACEMENT'
      and jsonb_typeof(tpos.actual_schedule_json) = 'array'
      and tpos.actual_schedule_json @> jsonb_build_array(
        jsonb_build_object('shift_id', v_shift_id::text, 'external_row_key', v_key)
      )
    order by tpos.updated_at desc nulls last, tpos.created_at desc nulls last
    limit 1;

    if v_existing_pos_ts_id is not null then
      select
        tf.locked_by_invoice_id,
        tf.invoice_breakdown_json
      into
        v_existing_pos_tf_locked_by_invoice_id,
        v_existing_pos_tf_invoice_breakdown_json
      from public.timesheets_financials tf
      where tf.timesheet_id = v_existing_pos_ts_id
        and tf.is_current = true
      order by tf.created_at desc
      limit 1;

      v_existing_pos_seg_invoice_id := null;
      begin
        select
          nullif(btrim(coalesce(s2.seg->>'invoice_locked_invoice_id','')), '')::uuid
        into v_existing_pos_seg_invoice_id
        from (
          select s2.seg
          from jsonb_array_elements(
            case
              when v_existing_pos_tf_invoice_breakdown_json is not null
               and jsonb_typeof(v_existing_pos_tf_invoice_breakdown_json) = 'object'
               and jsonb_typeof(v_existing_pos_tf_invoice_breakdown_json->'segments') = 'array'
              then v_existing_pos_tf_invoice_breakdown_json->'segments'
              else '[]'::jsonb
            end
          ) as s2(seg)
          where nullif(btrim(coalesce(s2.seg->>'invoice_locked_invoice_id','')), '') is not null
          limit 1
        ) as s2;
      exception when others then
        v_existing_pos_seg_invoice_id := null;
      end;

      v_existing_pos_is_invoiced :=
        (v_existing_pos_tf_locked_by_invoice_id is not null)
        or (v_existing_pos_seg_invoice_id is not null)
        or coalesce((
          public._import_review_timesheet_protection_core_v1(v_existing_pos_ts_id)
            ->>'paid'
        )::boolean,false);

      v_existing_pos_seg := null;
      if v_existing_pos_schedule is not null and jsonb_typeof(v_existing_pos_schedule) = 'array' then
        v_existing_pos_seg := v_existing_pos_schedule->0;
      end if;

      if v_existing_pos_seg is not null then
        v_existing_pos_old_start_str := nullif(btrim(coalesce(v_existing_pos_seg->>'start_utc','')), '');
        v_existing_pos_old_end_str   := nullif(btrim(coalesce(v_existing_pos_seg->>'end_utc','')), '');
        v_existing_pos_old_break_str := nullif(btrim(coalesce(v_existing_pos_seg->>'break_mins','')), '');

        begin
          if (v_existing_pos_seg ? 'import_id')
             and (v_existing_pos_seg->>'import_id') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
            v_existing_pos_import_id := (v_existing_pos_seg->>'import_id')::uuid;
          else
            v_existing_pos_import_id := null;
          end if;
        exception when others then
          v_existing_pos_import_id := null;
        end;
      end if;
    end if;

    select count(*)::int
    into v_existing_neg_count
    from public.timesheets tneg_cnt
    where tneg_cnt.is_adjustment is true
      and tneg_cnt.is_current is true
      and tneg_cnt.correction_kind = 'CHANGED_HOURS_REVERSAL'
      and jsonb_typeof(tneg_cnt.actual_schedule_json) = 'array'
      and tneg_cnt.actual_schedule_json @> jsonb_build_array(
        jsonb_build_object('shift_id', v_shift_id::text, 'external_row_key', v_key)
      );

    select
      tneg.timesheet_id,
      tneg.actual_schedule_json
    into
      v_existing_neg_ts_id,
      v_existing_neg_schedule
    from public.timesheets tneg
    where tneg.is_adjustment is true
      and tneg.is_current is true
      and tneg.correction_kind = 'CHANGED_HOURS_REVERSAL'
      and jsonb_typeof(tneg.actual_schedule_json) = 'array'
      and tneg.actual_schedule_json @> jsonb_build_array(
        jsonb_build_object('shift_id', v_shift_id::text, 'external_row_key', v_key)
      )
    order by tneg.updated_at desc nulls last, tneg.created_at desc nulls last
    limit 1;

    if v_existing_neg_ts_id is not null then
      select
        tf.locked_by_invoice_id,
        tf.invoice_breakdown_json
      into
        v_existing_neg_tf_locked_by_invoice_id,
        v_existing_neg_tf_invoice_breakdown_json
      from public.timesheets_financials tf
      where tf.timesheet_id = v_existing_neg_ts_id
        and tf.is_current = true
      order by tf.created_at desc
      limit 1;

      v_existing_neg_seg_invoice_id := null;
      begin
        select
          nullif(btrim(coalesce(s3.seg->>'invoice_locked_invoice_id','')), '')::uuid
        into v_existing_neg_seg_invoice_id
        from (
          select s3.seg
          from jsonb_array_elements(
            case
              when v_existing_neg_tf_invoice_breakdown_json is not null
               and jsonb_typeof(v_existing_neg_tf_invoice_breakdown_json) = 'object'
               and jsonb_typeof(v_existing_neg_tf_invoice_breakdown_json->'segments') = 'array'
              then v_existing_neg_tf_invoice_breakdown_json->'segments'
              else '[]'::jsonb
            end
          ) as s3(seg)
          where nullif(btrim(coalesce(s3.seg->>'invoice_locked_invoice_id','')), '') is not null
          limit 1
        ) as s3;
      exception when others then
        v_existing_neg_seg_invoice_id := null;
      end;

      v_existing_neg_is_invoiced :=
        (v_existing_neg_tf_locked_by_invoice_id is not null)
        or (v_existing_neg_seg_invoice_id is not null)
        or coalesce((
          public._import_review_timesheet_protection_core_v1(v_existing_neg_ts_id)
            ->>'paid'
        )::boolean,false);
    end if;

    -- Policy X retained-history rule: never delete an existing correction pair.
    -- If truth returns to the original schedule, the retained pair is updated to a
    -- zero residual after canonical pair unauthorisation; prior TSFIN, invoice and
    -- payment history remains authoritative.
    v_deleted_redundant_pair := false;

    -- Import Review reconciliation is authoritative for both the generation
    -- identity and the exact frozen schedule being reversed.  Historical live
    -- timesheets are deliberately not used as financial evidence here.
    if v_reconciliation_unit is not null then
      if jsonb_typeof(v_reconciliation_b_schedule)<>'array' or jsonb_array_length(v_reconciliation_b_schedule)<>1 then
        raise exception 'IMPORT_REVIEW_EFFECTIVE_POSITION_NOT_STANDARD_REPRESENTABLE' using errcode='55000';
      end if;
      v_existing_pos_old_start_str:=v_reconciliation_b_schedule#>>'{0,start_utc}';
      v_existing_pos_old_end_str:=v_reconciliation_b_schedule#>>'{0,end_utc}';
      v_existing_pos_old_break_str:=coalesce(v_reconciliation_b_schedule#>>'{0,break_mins}','0');
      v_old_start_utc:=v_existing_pos_old_start_str::timestamptz;
      v_old_end_utc:=v_existing_pos_old_end_str::timestamptz;
      v_old_break_mins:=v_existing_pos_old_break_str::integer;
      v_old_start_str:=v_existing_pos_old_start_str;
      v_old_end_str:=v_existing_pos_old_end_str;
      v_old_break_str:=v_existing_pos_old_break_str;
      if v_reconciliation_route='AMEND_EXISTING_REPLACEMENT' then
        v_correction_id:=v_reconciliation_unit->>'correction_id';
        if nullif(v_correction_id,'') is null then
          raise exception 'IMPORT_REVIEW_RECONCILIATION_UNIT_INVALID' using errcode='22023';
        end if;
      end if;
      if v_reconciliation_route='CREATE_REVERSAL_REPLACEMENT'
         and v_existing_pos_ts_id is not null and v_existing_pos_is_invoiced then
        -- The next generation reverses the immediately preceding positive.
        v_base_timesheet_id:=v_existing_pos_ts_id;
      elsif nullif(v_reconciliation_unit->>'parent_timesheet_id','') is not null then
        v_base_timesheet_id:=(v_reconciliation_unit->>'parent_timesheet_id')::uuid;
      end if;
      if not exists(select 1 from public.timesheets parent_ts
        where parent_ts.timesheet_id=v_base_timesheet_id and parent_ts.is_current and parent_ts.archived_at_utc is null) then
        raise exception 'IMPORT_REVIEW_RECONCILIATION_PARENT_INVALID' using errcode='55000';
      end if;
      -- Route through the existing role upsert loop.  It reuses any surviving
      -- current role and creates only a physically missing role.
      v_existing_pos_ts_id:=null;
      v_existing_pos_is_invoiced:=false;
      v_existing_neg_ts_id:=null;
      v_existing_neg_is_invoiced:=false;
    end if;

    -- ✅ If latest POS is NOT invoiced: update POS in place (do NOT create new NEG/POS)
    v_updated_existing_replacement := false;
    if v_existing_pos_ts_id is not null and v_existing_pos_is_invoiced is false then
      -- Use existing POS correction_id (for continuity)
      if nullif(btrim(coalesce(v_existing_pos_correction_id,'')), '') is not null then
        v_correction_id := v_existing_pos_correction_id;
      end if;

      v_shift_date_ymd := to_char((v_new_start_utc at time zone 'Europe/London')::date, 'YYYY-MM-DD');

      v_schedule := jsonb_build_array(
        jsonb_build_object(
          'date', v_shift_date_ymd,
          'ward', nullif(btrim(coalesce(v_contract_ward_hint,'contract')), ''),
          'start_utc', v_new_start_utc::text,
          'end_utc', v_new_end_utc::text,
          'break_mins', greatest(0, v_new_break_mins),
          'ref_num', v_ref_num,
          'external_row_key', v_key,
          'shift_id', v_shift_id::text,
          'import_id', p_import_id::text
        )
      );

      v_hint := jsonb_build_object(
        'import_correction', jsonb_build_object(
          'import_id', p_import_id::text,
          'external_row_key', v_key,
          'correction_id', v_correction_id,
          'correction_kind', 'CHANGED_HOURS_REPLACEMENT',
          'updated_from_import_id', p_import_id::text
        )
      );

      v_hint := v_hint || jsonb_build_object(
        'correction_financials_policy_envelope', v_correction_financials_policy_envelope,
        'correction_financials_policy_envelope_fingerprint', v_correction_financials_policy_envelope_fingerprint,
        'root_timesheet_id', v_root_timesheet_id::text,
        'latest_positive_timesheet_id', coalesce(v_latest_positive_timesheet_id,v_base_timesheet_id)::text
      );
      if v_reconciliation_unit is not null then
        v_hint:=v_hint||jsonb_build_object('import_authoritative_reconciliation',jsonb_build_object(
          'operation_id',current_setting('cloudtms.import_reconciliation_operation_id',true),
          'unit_fingerprint',v_reconciliation_unit->>'unit_fingerprint','route',v_reconciliation_route,
          'source_identity',v_key));
      end if;


      perform 1
      from public.timesheets tlock
      where tlock.correction_id = v_existing_pos_correction_id
        and tlock.is_current = true
        and tlock.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
      order by tlock.timesheet_id
      for update;

      if (
        select count(*) = 2
          and count(*) filter (where pair_check.correction_kind='CHANGED_HOURS_REVERSAL') = 1
          and count(*) filter (where pair_check.correction_kind='CHANGED_HOURS_REPLACEMENT') = 1
        from public.timesheets pair_check
        where pair_check.correction_id=v_existing_pos_correction_id
          and pair_check.is_current=true
          and pair_check.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
      ) is not true then
        raise exception using message='CORRECTION_PAIR_INCOMPLETE',errcode='P0001',
          detail=jsonb_build_object('code','CORRECTION_PAIR_INCOMPLETE','correction_id',v_existing_pos_correction_id)::text;
      end if;

      if exists (
        select 1
        from public.timesheets pair_ts
        left join public.timesheets_financials pair_tf
          on pair_tf.timesheet_id=pair_ts.timesheet_id and pair_tf.is_current=true
        where pair_ts.correction_id=v_existing_pos_correction_id
          and pair_ts.is_current=true
          and pair_ts.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
          and (
            pair_ts.authorised_at_server is not null
            or pair_tf.authorised_at_utc is not null
            or pair_tf.paid_at_utc is not null
            or coalesce((
              public._import_review_timesheet_protection_core_v1(pair_ts.timesheet_id)
                ->>'paid'
            )::boolean,false)
            or pair_tf.locked_by_invoice_id is not null
            or exists (select 1 from public.invoice_lines il where il.timesheet_id=pair_ts.timesheet_id)
          )
      ) then
        raise exception using message='CORRECTION_PAIR_LIFECYCLE_TRANSITION_REQUIRED', errcode='P0001',
          detail=jsonb_build_object(
            'code','CORRECTION_PAIR_LIFECYCLE_TRANSITION_REQUIRED',
            'correction_id',v_existing_pos_correction_id,
            'required_path','PAIR_UNAUTHORISE_AMEND_RECALCULATE_REAUTHORISE'
          )::text;
      end if;

      select pair_reversal.parent_timesheet_id
      into v_existing_pair_parent_timesheet_id
      from public.timesheets pair_reversal
      where pair_reversal.correction_id=v_existing_pos_correction_id
        and pair_reversal.is_current=true
        and pair_reversal.correction_kind='CHANGED_HOURS_REVERSAL'
      limit 1;

      if v_existing_pair_parent_timesheet_id is null then
        raise exception using message='CORRECTION_PAIR_PARENT_MISSING',errcode='P0001',
          detail=jsonb_build_object(
            'code','CORRECTION_PAIR_PARENT_MISSING',
            'correction_id',v_existing_pos_correction_id
          )::text;
      end if;

      -- Repair only the known legacy replay split in a complete, mutable pair.
      -- Frozen, invoiced, paid or authorised pair members were rejected above.
      update public.timesheets pair_replacement
      set parent_timesheet_id=v_existing_pair_parent_timesheet_id,
          updated_at=v_now
      where pair_replacement.correction_id=v_existing_pos_correction_id
        and pair_replacement.is_current=true
        and pair_replacement.correction_kind='CHANGED_HOURS_REPLACEMENT'
        and pair_replacement.parent_timesheet_id is distinct from v_existing_pair_parent_timesheet_id;

      if (
        select count(*) = 2
          and count(*) filter (where pair_check.correction_kind='CHANGED_HOURS_REVERSAL') = 1
          and count(*) filter (where pair_check.correction_kind='CHANGED_HOURS_REPLACEMENT') = 1
          and count(distinct pair_check.parent_timesheet_id) = 1
          and count(pair_check.parent_timesheet_id) = 2
        from public.timesheets pair_check
        where pair_check.correction_id=v_existing_pos_correction_id
          and pair_check.is_current=true
          and pair_check.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
      ) is not true then
        raise exception using message='CORRECTION_PAIR_INCOMPLETE',errcode='P0001',
          detail=jsonb_build_object('code','CORRECTION_PAIR_INCOMPLETE','correction_id',v_existing_pos_correction_id)::text;
      end if;

      update public.timesheets tup
      set
        actual_schedule_json = v_schedule,
        qr_payload_json = v_hint,
        candidate_hint_text = v_hint,

        -- ✅ inherit policy identity from base timesheet
        sheet_scope = v_effective_sheet_scope,
        submission_mode = v_effective_submission_mode,

        updated_at = v_now
      where tup.timesheet_id = v_existing_pos_ts_id;


      -- Keep contract_week snapshot in sync with the effective submission mode
      update public.contract_weeks cw_sm
      set submission_mode_snapshot = v_effective_submission_mode,
          updated_at = v_now
      where cw_sm.timesheet_id = v_existing_pos_ts_id
        and cw_sm.contract_id = v_contract_id
        and cw_sm.week_ending_date = v_week_ending_date;

      v_rep_ts_id := v_existing_pos_ts_id;
      v_rep_cw_id := null;

      v_upd_count := v_upd_count + 1;
      v_updated_ts_ids := array_append(v_updated_ts_ids, v_existing_pos_ts_id);
      v_updated_existing_replacement := true;

      v_key_ts := '[]'::jsonb;
      v_key_ts := v_key_ts || jsonb_build_array(jsonb_build_object(
        'kind', 'CHANGED_HOURS_REPLACEMENT',
        'timesheet_id', v_existing_pos_ts_id::text,
        'op', 'UPDATED_IN_PLACE'
      ));
    end if;
    -- ✅ If latest POS IS invoiced, re-base old values to POS (so NEG reverses POS)
    if v_updated_existing_replacement is false and v_existing_pos_ts_id is not null and v_existing_pos_is_invoiced is true then

      -- ✅ Treat the invoiced POS as the effective parent for policy inheritance
      v_base_timesheet_id := v_existing_pos_ts_id;

      select
        coalesce(ts.sheet_scope, 'WEEKLY'::public.timesheet_scope_enum),
        coalesce(ts.submission_mode, 'MANUAL'::public.submission_mode_enum)
      into
        v_effective_sheet_scope,
        v_effective_submission_mode
      from public.timesheets ts
      where ts.timesheet_id = v_base_timesheet_id
        and ts.is_current = true
      limit 1;

      if not found then
        v_effective_sheet_scope := 'WEEKLY'::public.timesheet_scope_enum;
        v_effective_submission_mode := 'MANUAL'::public.submission_mode_enum;
      end if;

      if v_existing_pos_old_start_str is not null then
        begin
          v_old_start_utc := v_existing_pos_old_start_str::timestamptz;
        exception when others then
          null;
        end;
      end if;


      if v_existing_pos_old_end_str is not null then
        begin
          v_old_end_utc := v_existing_pos_old_end_str::timestamptz;
        exception when others then
          null;
        end;
      end if;

      if v_existing_pos_old_break_str is not null and v_existing_pos_old_break_str ~ '^[0-9]+$' then
        begin
          v_old_break_mins := v_existing_pos_old_break_str::int;
        exception when others then
          null;
        end;
      end if;

      if v_existing_pos_import_id is not null then
        v_shift_prev_import_id := v_existing_pos_import_id;
      end if;

      v_old_start_str := coalesce(v_existing_pos_old_start_str, v_old_start_str);
      v_old_end_str   := coalesce(v_existing_pos_old_end_str, v_old_end_str);
      v_old_break_str := coalesce(v_existing_pos_old_break_str, v_old_break_str);

      v_fnv_s :=
        coalesce(p_import_id::text,'') || '|' ||
        coalesce(v_key,'') || '|' ||
        coalesce(v_old_start_str,'') || '|' ||
        coalesce(v_new_start_str,'') || '|' ||
        coalesce(v_old_end_str,'') || '|' ||
        coalesce(v_new_end_str,'') || '|' ||
        coalesce(v_old_break_str,'') || '|' ||
        coalesce(v_new_break_str,'');

      v_fnv_h := 2166136261;
      for v_fnv_i in 1..char_length(v_fnv_s) loop
        v_fnv_h := (v_fnv_h # ascii(substring(v_fnv_s from v_fnv_i for 1)));
        v_fnv_h := (v_fnv_h * 16777619) % 4294967296;
      end loop;

      v_fnv_hex := lpad(lower(to_hex(v_fnv_h)), 8, '0');
      v_correction_id := 'chg:' || p_import_id::text || ':' || v_key || ':' || v_fnv_hex;
    end if;

    -- If we updated POS in place, skip creating new corrections
    if v_updated_existing_replacement is true then
      -- still include in sample; audits below already handle v_rep_ts_id
      -- but we must still run the audit block (it uses v_rep_ts_id)
      null;
    else
      -- Apply two artefacts: REVERSAL and REPLACEMENT
      v_key_ts := '[]'::jsonb;

      for v_kind in select unnest(array['CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT']) loop
        v_kind_op := null;

        if v_kind = 'CHANGED_HOURS_REVERSAL' then
          v_seg_start_utc := v_old_start_utc;
          v_seg_end_utc := v_old_end_utc;
          v_seg_break_mins := greatest(0, v_old_break_mins);
          v_schedule_import_id := v_shift_prev_import_id;
        else
          v_seg_start_utc := v_new_start_utc;
          v_seg_end_utc := v_new_end_utc;
          v_seg_break_mins := greatest(0, v_new_break_mins);
          v_schedule_import_id := p_import_id;
        end if;

        v_shift_date_ymd := to_char((v_seg_start_utc at time zone 'Europe/London')::date, 'YYYY-MM-DD');

        v_hint := jsonb_build_object(
          'import_correction', jsonb_build_object(
            'import_id', p_import_id::text,
            'external_row_key', v_key,
            'correction_id', v_correction_id,
            'correction_kind', v_kind
          )
        );

        v_hint := v_hint || jsonb_build_object(
          'correction_financials_policy_envelope', v_correction_financials_policy_envelope,
          'correction_financials_policy_envelope_fingerprint', v_correction_financials_policy_envelope_fingerprint,
          'root_timesheet_id', v_root_timesheet_id::text,
          'latest_positive_timesheet_id', coalesce(v_latest_positive_timesheet_id,v_base_timesheet_id)::text
        );
        if v_reconciliation_unit is not null then
          v_hint:=v_hint||jsonb_build_object('import_authoritative_reconciliation',jsonb_build_object(
            'operation_id',current_setting('cloudtms.import_reconciliation_operation_id',true),
            'unit_fingerprint',v_reconciliation_unit->>'unit_fingerprint','route',v_reconciliation_route,
            'source_identity',v_key));
        end if;


        v_shift_label := 'weekly-correction-' || lower(v_kind) || '-' || v_correction_id;

        v_shift_label_norm :=
          regexp_replace(
            regexp_replace(lower(trim(v_shift_label)), '\s+', ' ', 'g'),
            '[^\w\s\-@&\/,.:]',
            '',
            'g'
          );

        -- ✅ Schedule carries ref_num + evidence linkage (external_row_key/shift_id/import_id)
        v_schedule := jsonb_build_array(
          jsonb_build_object(
            'date', v_shift_date_ymd,
            'ward', nullif(btrim(coalesce(v_contract_ward_hint,'contract')), ''),
            'start_utc', v_seg_start_utc::text,
            'end_utc', v_seg_end_utc::text,
            'break_mins', v_seg_break_mins,
            'ref_num', v_ref_num,
            'external_row_key', v_key,
            'shift_id', v_shift_id::text,
            'import_id', case when v_schedule_import_id is null then null else v_schedule_import_id::text end
          )
        );

        -- Idempotency: reuse existing correction timesheet (unique on correction_id+kind)
        v_existing_ts_id := null;

        select t.timesheet_id
        into v_existing_ts_id
        from public.timesheets t
        where t.correction_id = v_correction_id
          and t.correction_kind = v_kind
          and (v_reconciliation_unit is null or (t.is_current and t.archived_at_utc is null))
        order by t.is_current desc, t.version desc
        limit 1
        for update;

        if v_existing_ts_id is not null then
          -- Ensure there is an adjustment contract_week linked; reuse it if present.
          v_existing_cw_id := null;
          v_existing_cw_seq := null;
          v_existing_cw_is_adjustment := null;

          select
            cw.id,
            cw.additional_seq,
            cw.is_adjustment
          into
            v_existing_cw_id,
            v_existing_cw_seq,
            v_existing_cw_is_adjustment
          from public.contract_weeks cw
          where cw.timesheet_id = v_existing_ts_id
            and cw.contract_id = v_contract_id
            and cw.week_ending_date = v_week_ending_date
          limit 1
          for update;

          if v_existing_cw_id is not null then
            if v_existing_cw_is_adjustment is not true or coalesce(v_existing_cw_seq,0) <= 0 then
              raise exception 'hr_weekly_phase3_apply_adjustment_truth: existing correction timesheet is linked to a non-adjustment contract_week (timesheet_id=%).', v_existing_ts_id;
            end if;

            update public.contract_weeks cw2
            set
              is_adjustment = true,
              submission_mode_snapshot = v_effective_submission_mode,
              status = 'SUBMITTED'::public.contract_week_status_enum,
              updated_at = v_now
            where cw2.id = v_existing_cw_id;

          else
            -- Create a new adjustment contract_week safely and link it to the existing correction timesheet.
            perform 1
            from public.contract_weeks cwlock
            where cwlock.contract_id = v_contract_id
              and cwlock.week_ending_date = v_week_ending_date
            for update;

            v_try := 0;
            loop
              v_try := v_try + 1;
              if v_try > 10 then
                raise exception 'hr_weekly_phase3_apply_adjustment_truth: failed to allocate additional_seq after retries (contract_id=% week_ending=%).', v_contract_id, v_week_ending_date;
              end if;

              select coalesce(max(cwmax.additional_seq), 0) + 1
              into v_next_additional_seq
              from public.contract_weeks cwmax
              where cwmax.contract_id = v_contract_id
                and cwmax.week_ending_date = v_week_ending_date;

              begin
                insert into public.contract_weeks(
                  contract_id,
                  week_ending_date,
                  additional_seq,
                  is_adjustment,
                  submission_mode_snapshot,
                  status,
                  created_at,
                  updated_at,
                  timesheet_id
                )
                values (
                  v_contract_id,
                  v_week_ending_date,
                  v_next_additional_seq,
                  true,
                  v_effective_submission_mode,
                  'SUBMITTED'::public.contract_week_status_enum,
                  v_now,
                  v_now,
                  v_existing_ts_id
                )
                returning id into v_existing_cw_id;

                exit;
              exception when unique_violation then
                v_existing_cw_id := null;
              end;
            end loop;
          end if;

          -- Update existing correction timesheet to ensure columns match locked contract
             update public.timesheets t2
          set
            is_current = true,
            status = 'RECEIVED'::public.timesheet_status_enum,
            sheet_scope = v_effective_sheet_scope,
            submission_mode = v_effective_submission_mode,
            line_type = 'HOURS',

            week_ending_date = v_week_ending_date,
            contract_id = v_contract_id,
            occupant_key_norm = lower(coalesce(v_candidate_tms_ref, v_candidate_display_name, v_candidate_id::text)),
            hospital_norm = lower(coalesce(v_contract_display_site, v_client_name, v_client_id::text)),
            ward_norm = lower(coalesce(v_contract_ward_hint,'contract')),
            job_title_norm = lower(coalesce(v_contract_role,'weekly')),
            shift_label_norm = v_shift_label_norm,
            manual_pdf_r2_key = null,
            actual_schedule_json = v_schedule,
            additional_units_week = '{}'::jsonb,
            additional_units_per_day = '{}'::jsonb,
            day_references_json = null,
            candidate_hint_text = v_hint,
            is_adjustment = true,
            parent_timesheet_id = v_base_timesheet_id,
            correction_id = v_correction_id,
            correction_kind = v_kind,
            adjustment_origin = 'IMPORT_CORRECTION',
            updated_at = v_now
          where t2.timesheet_id = v_existing_ts_id;

          v_upd_count := v_upd_count + 1;
          v_updated_ts_ids := array_append(v_updated_ts_ids, v_existing_ts_id);
          v_kind_op := 'UPDATED';

          if v_kind = 'CHANGED_HOURS_REVERSAL' then
            v_rev_ts_id := v_existing_ts_id;
            v_rev_cw_id := v_existing_cw_id;
          else
            v_rep_ts_id := v_existing_ts_id;
            v_rep_cw_id := v_existing_cw_id;
          end if;

          v_key_ts := v_key_ts || jsonb_build_array(jsonb_build_object(
            'kind', v_kind,
            'timesheet_id', v_existing_ts_id::text,
            'op', v_kind_op
          ));

        else
          -- Create a new adjustment contract_week (safe additional_seq) + a new correction timesheet linked to it.
          perform 1
          from public.contract_weeks cwlock2
          where cwlock2.contract_id = v_contract_id
            and cwlock2.week_ending_date = v_week_ending_date
          for update;

          v_try := 0;
          loop
            v_try := v_try + 1;
            if v_try > 10 then
              raise exception 'hr_weekly_phase3_apply_adjustment_truth: failed to allocate additional_seq after retries (contract_id=% week_ending=%).', v_contract_id, v_week_ending_date;
            end if;

            select coalesce(max(cwmax2.additional_seq), 0) + 1
            into v_next_additional_seq
            from public.contract_weeks cwmax2
            where cwmax2.contract_id = v_contract_id
              and cwmax2.week_ending_date = v_week_ending_date;

            begin
              insert into public.contract_weeks(
                contract_id,
                week_ending_date,
                additional_seq,
                is_adjustment,
                submission_mode_snapshot,
                status,
                created_at,
                updated_at
              )
              values (
                v_contract_id,
                v_week_ending_date,
                v_next_additional_seq,
                true,
                v_effective_submission_mode,
                'SUBMITTED'::public.contract_week_status_enum,
                v_now,
                v_now
              )
              returning id into v_cw_id;

              exit;
            exception when unique_violation then
              v_cw_id := null;
            end;
          end loop;

          v_booking_base :=
            v_candidate_norm || '|' ||
            v_week_ending_date::text || '|' ||
            v_hospital_norm || '|' ||
            v_ward_norm || '|' ||
            v_role_norm || '|' ||
            regexp_replace(
              regexp_replace(lower(trim('WEEKLY-' || v_next_additional_seq::text || '-' || v_kind || '-' || v_correction_id)), '\s+', ' ', 'g'),
              '[^\w\s\-@&\/,.:]',
              '',
              'g'
            );

          v_hash_hex := encode(extensions.digest(convert_to(v_booking_base, 'utf8'), 'sha256'::text), 'hex');
          v_booking_id := 'bk_' || substr(v_hash_hex, 1, 16);

     begin
         insert into public.timesheets(
  booking_id,
  version,
  is_current,
  status,
  occupant_key_norm,
  hospital_norm,
  ward_norm,
  job_title_norm,
  shift_label_norm,
  week_ending_date,
  contract_id,
  sheet_scope,
  submission_mode,
  line_type,
  manual_pdf_r2_key,
  actual_schedule_json,
  additional_units_week,
  additional_units_per_day,
  day_references_json,
  qr_status,
  qr_token,
  qr_generated_at,
  qr_scanned_at,
  qr_scan_info_json,
  qr_r2_key,
  qr_payload_json,
  created_at,
  updated_at,
  is_adjustment,
  parent_timesheet_id,
  candidate_hint_text,
  correction_id,
  correction_kind,
  adjustment_origin
)
values (
  v_booking_id,
  1,
  true,
  'RECEIVED'::public.timesheet_status_enum,
  lower(coalesce(v_candidate_tms_ref, v_candidate_display_name, v_candidate_id::text)),
  lower(coalesce(v_contract_display_site, v_client_name, v_client_id::text)),
  lower(coalesce(v_contract_ward_hint,'contract')),
  lower(coalesce(v_contract_role,'weekly')),
  v_shift_label_norm,
  v_week_ending_date,
  v_contract_id,
  v_effective_sheet_scope,
  v_effective_submission_mode,
  'HOURS',

  null,
  v_schedule,
  '{}'::jsonb,
  '{}'::jsonb,
  null,
  null,
  null,
  null,
  null,
  null,
  null,
  '{}'::jsonb,
  v_now,
  v_now,
  true,
  v_base_timesheet_id,
  v_hint,
  v_correction_id,
  v_kind,
  'IMPORT_CORRECTION'
)
returning timesheet_id into v_ts_id;



          exception when unique_violation then
            select t3.timesheet_id
            into v_ts_id
            from public.timesheets t3
            where t3.correction_id = v_correction_id
              and t3.correction_kind = v_kind
            order by t3.is_current desc, t3.version desc
            limit 1
            for update;

            if v_ts_id is null then
              raise exception 'hr_weekly_phase3_apply_adjustment_truth: unique_violation inserting correction timesheet but failed to find existing row (correction_id=% kind=%).', v_correction_id, v_kind;
            end if;

                     update public.timesheets t4
            set
              is_current = true,
              status = 'RECEIVED'::public.timesheet_status_enum,
              sheet_scope = v_effective_sheet_scope,
              submission_mode = v_effective_submission_mode,
              line_type = 'HOURS',

              week_ending_date = v_week_ending_date,
              contract_id = v_contract_id,
              occupant_key_norm = lower(coalesce(v_candidate_tms_ref, v_candidate_display_name, v_candidate_id::text)),
              hospital_norm = lower(coalesce(v_contract_display_site, v_client_name, v_client_id::text)),
              ward_norm = lower(coalesce(v_contract_ward_hint,'contract')),
              job_title_norm = lower(coalesce(v_contract_role,'weekly')),
              shift_label_norm = v_shift_label_norm,
              manual_pdf_r2_key = null,
              actual_schedule_json = v_schedule,
              additional_units_week = '{}'::jsonb,
              additional_units_per_day = '{}'::jsonb,
              day_references_json = null,
              candidate_hint_text = v_hint,
              is_adjustment = true,
              parent_timesheet_id = v_base_timesheet_id,
              correction_id = v_correction_id,
              correction_kind = v_kind,
              adjustment_origin = 'IMPORT_CORRECTION',
              updated_at = v_now
            where t4.timesheet_id = v_ts_id;
          end;

          update public.contract_weeks cw3
          set
            timesheet_id = v_ts_id,
            status = 'SUBMITTED'::public.contract_week_status_enum,
            submission_mode_snapshot = v_effective_submission_mode,
            is_adjustment = true,
            updated_at = v_now
          where cw3.id = v_cw_id;

          v_ins_count := v_ins_count + 1;
          v_created_ts_ids := array_append(v_created_ts_ids, v_ts_id);
          v_kind_op := 'CREATED';

          if v_kind = 'CHANGED_HOURS_REVERSAL' then
            v_rev_ts_id := v_ts_id;
            v_rev_cw_id := v_cw_id;
          else
            v_rep_ts_id := v_ts_id;
            v_rep_cw_id := v_cw_id;
          end if;

          v_key_ts := v_key_ts || jsonb_build_array(jsonb_build_object(
            'kind', v_kind,
            'timesheet_id', v_ts_id::text,
            'op', v_kind_op
          ));
        end if;

      end loop; -- kind loop
    end if; -- updated_existing_replacement

    if v_reconciliation_unit is not null then
      if v_rev_ts_id is null or v_rep_ts_id is null then
        raise exception 'IMPORT_REVIEW_APPLY_POSTCONDITION_FAILED' using errcode='55000',
          detail=jsonb_build_object('reason_code','CORRECTION_MEMBER_SET_INCOMPLETE','source_identity',v_key)::text;
      end if;
      update pg_temp.import_review_reconciliation_units_v1 u
      set unit_json=u.unit_json||jsonb_build_object(
        'correction_id',v_correction_id,
        'M_active_member_ids',jsonb_build_array(v_rev_ts_id,v_rep_ts_id),
        'parent_timesheet_id',v_base_timesheet_id,
        'applied_member_ids',jsonb_build_array(v_rev_ts_id,v_rep_ts_id))
      where u.source_identity=v_key;
    end if;

    -- ─────────────────────────────────────────────
    -- ✅ User-facing audit entries (timesheet modal + invoice history)
    -- ─────────────────────────────────────────────
    begin
      -- Timesheet audit: reversal
      if v_rev_ts_id is not null then
        perform public._audit_insert(
          'timesheets',
          v_rev_ts_id::text,
          'HR_IMPORT_CORRECTION_APPLIED',
          null,
          jsonb_build_object(
            'trigger_import_id', p_import_id::text,
            'correction_financials_policy_envelope_fingerprint', v_correction_financials_policy_envelope_fingerprint,
            'evidence_import_id', case when v_shift_prev_import_id is null then null else v_shift_prev_import_id::text end,
            'external_row_key', v_key,
            'shift_id', v_shift_id::text,
            'ref_num', v_ref_num,
            'invoice_id_detected', case when v_invoice_id_detected is null then null else v_invoice_id_detected::text end,
            'correction_id', v_correction_id,
            'correction_kind', 'CHANGED_HOURS_REVERSAL',
            'old_start_utc', v_old_start_utc::text,
            'old_end_utc', v_old_end_utc::text,
            'old_break_mins', v_old_break_mins,
            'new_start_utc', v_new_start_utc::text,
            'new_end_utc', v_new_end_utc::text,
            'new_break_mins', v_new_break_mins,
            'old_paid_minutes', v_old_paid_minutes,
            'new_paid_minutes', v_new_paid_minutes,
            'delta_paid_minutes', v_delta_paid_minutes,
            'counterpart_timesheet_id', case when v_rep_ts_id is null then null else v_rep_ts_id::text end,
            'replacement_updated_in_place', v_updated_existing_replacement,
            'redundant_pair_deleted', v_deleted_redundant_pair
          ),
          'IMPORT_CORRECTION',
          p_actor_user_id
        );
      end if;

      -- Timesheet audit: replacement
      if v_rep_ts_id is not null then
        perform public._audit_insert(
          'timesheets',
          v_rep_ts_id::text,
          'HR_IMPORT_CORRECTION_APPLIED',
          null,
          jsonb_build_object(
            'trigger_import_id', p_import_id::text,
            'correction_financials_policy_envelope_fingerprint', v_correction_financials_policy_envelope_fingerprint,
            'evidence_import_id', p_import_id::text,
            'external_row_key', v_key,
            'shift_id', v_shift_id::text,
            'ref_num', v_ref_num,
            'invoice_id_detected', case when v_invoice_id_detected is null then null else v_invoice_id_detected::text end,
            'correction_id', v_correction_id,
            'correction_kind', 'CHANGED_HOURS_REPLACEMENT',
            'old_start_utc', v_old_start_utc::text,
            'old_end_utc', v_old_end_utc::text,
            'old_break_mins', v_old_break_mins,
            'new_start_utc', v_new_start_utc::text,
            'new_end_utc', v_new_end_utc::text,
            'new_break_mins', v_new_break_mins,
            'old_paid_minutes', v_old_paid_minutes,
            'new_paid_minutes', v_new_paid_minutes,
            'delta_paid_minutes', v_delta_paid_minutes,
            'counterpart_timesheet_id', case when v_rev_ts_id is null then null else v_rev_ts_id::text end,
            'replacement_updated_in_place', v_updated_existing_replacement,
            'redundant_pair_deleted', v_deleted_redundant_pair
          ),
          'IMPORT_CORRECTION',
          p_actor_user_id
        );
      end if;

      -- Optional: contract_week audit
      if v_rev_cw_id is not null then
        perform public._audit_insert(
          'contract_weeks',
          v_rev_cw_id::text,
          'HR_IMPORT_CORRECTION_APPLIED',
          null,
          jsonb_build_object(
            'trigger_import_id', p_import_id::text,
            'external_row_key', v_key,
            'shift_id', v_shift_id::text,
            'ref_num', v_ref_num,
            'correction_id', v_correction_id,
            'correction_kind', 'CHANGED_HOURS_REVERSAL',
            'timesheet_id', case when v_rev_ts_id is null then null else v_rev_ts_id::text end,
            'replacement_updated_in_place', v_updated_existing_replacement,
            'redundant_pair_deleted', v_deleted_redundant_pair
          ),
          'IMPORT_CORRECTION',
          p_actor_user_id
        );
      end if;

      if v_rep_cw_id is not null then
        perform public._audit_insert(
          'contract_weeks',
          v_rep_cw_id::text,
          'HR_IMPORT_CORRECTION_APPLIED',
          null,
          jsonb_build_object(
            'trigger_import_id', p_import_id::text,
            'external_row_key', v_key,
            'shift_id', v_shift_id::text,
            'ref_num', v_ref_num,
            'correction_id', v_correction_id,
            'correction_kind', 'CHANGED_HOURS_REPLACEMENT',
            'timesheet_id', case when v_rep_ts_id is null then null else v_rep_ts_id::text end,
            'replacement_updated_in_place', v_updated_existing_replacement,
            'redundant_pair_deleted', v_deleted_redundant_pair
          ),
          'IMPORT_CORRECTION',
          p_actor_user_id
        );
      end if;

      -- Invoice history entry (ungated)
      if v_invoice_id_detected is not null then
        perform public._inv_write_audit(
          p_actor_user_id,
          'HR_IMPORT_CORRECTION_APPLIED',
          jsonb_build_object(
            'trigger_import_id', p_import_id::text,
            'external_row_key', v_key,
            'shift_id', v_shift_id::text,
            'ref_num', v_ref_num,
            'invoice_id', v_invoice_id_detected::text,
            'correction_id', v_correction_id,
            'old_paid_minutes', v_old_paid_minutes,
            'new_paid_minutes', v_new_paid_minutes,
            'delta_paid_minutes', v_delta_paid_minutes,
            'reversal_timesheet_id', case when v_rev_ts_id is null then null else v_rev_ts_id::text end,
            'replacement_timesheet_id', case when v_rep_ts_id is null then null else v_rep_ts_id::text end,
            'replacement_updated_in_place', v_updated_existing_replacement,
            'redundant_pair_deleted', v_deleted_redundant_pair
          ),
          'invoices',
          v_invoice_id_detected::text,
          null,
          'IMPORT_CORRECTION',
          null,
          null,
          null
        );
      end if;
    exception when others then
      null;
    end;

    if v_sample_n < 20 then
      v_sample := v_sample || jsonb_build_array(jsonb_build_object(
        'external_row_key', v_key,
        'is_invoiced', v_is_invoiced,
        'invoice_id_detected', case when v_invoice_id_detected is null then null else v_invoice_id_detected::text end,
        'week_ending_date', v_week_ending_date::text,
        'base_timesheet_id', case when v_base_timesheet_id is null then null else v_base_timesheet_id::text end,
        'correction_id', v_correction_id,
        'replacement_updated_in_place', v_updated_existing_replacement,
        'redundant_pair_deleted', v_deleted_redundant_pair,
        'timesheets', v_key_ts
      ));
      v_sample_n := v_sample_n + 1;
    end if;

  end loop; -- selected keys loop

  -- Debug audit (invoice_debug gated inside _imp_debug_audit)
  perform public._imp_debug_audit(
    p_actor_user_id,
    'HR_CORRECTION_SERIES_DEBUG',
    jsonb_build_object(
      'import_id', p_import_id::text,
      'selected_count', coalesce(array_length(v_selected_keys, 1), 0),
      'inserted_count', v_ins_count,
      'updated_count', v_upd_count,
      'created_timesheet_ids_count', coalesce(array_length(v_created_ts_ids, 1), 0),
      'updated_timesheet_ids_count', coalesce(array_length(v_updated_ts_ids, 1), 0),
      'sample', v_sample
    ),
    'hr_imports',
    p_import_id::text,
    null,
    null,
    null,
    null
  );

  return jsonb_build_object(
    'import_id', p_import_id,
    'selected_count', coalesce(array_length(v_selected_keys, 1), 0),
    'skipped_count', v_skipped_count,
    'inserted_count', v_ins_count,
    'updated_count', v_upd_count,
    'created_timesheet_ids', to_jsonb(coalesce(v_created_ts_ids, '{}'::uuid[])),
    'updated_timesheet_ids', to_jsonb(coalesce(v_updated_ts_ids, '{}'::uuid[]))
  );

exception when others then
  get stacked diagnostics v_sqlstate = returned_sqlstate, v_err = message_text;

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'HR_CORRECTION_SERIES_ERROR',
      jsonb_build_object(
        'import_id', p_import_id::text,
        'last_external_row_key', v_last_key,
        'selected_count', coalesce(array_length(v_selected_keys, 1), 0),
        'inserted_count', v_ins_count,
        'updated_count', v_upd_count,
        'sqlstate', v_sqlstate,
        'error', v_err
      ),
      'hr_imports',
      p_import_id::text,
      null,
      null,
      null,
      null
    );
  exception when others then
    null;
  end;

  raise;
end;
$function$;

CREATE OR REPLACE FUNCTION public.timesheet_paid_uninvoiced_rollover_v1(p_timesheet_id uuid, p_actor_user_id uuid, p_operation_id uuid, p_expected_current_tsfin_id uuid, p_expected_preflight_fingerprint text, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
DECLARE
  v_now timestamptz := COALESCE(p_now_utc, now());
  v_expected_preflight_fingerprint text :=
    NULLIF(BTRIM(COALESCE(p_expected_preflight_fingerprint, '')), '');

  v_operation public.import_apply_operations%ROWTYPE;
  v_timesheet public.timesheets%ROWTYPE;
  v_old_tsfin public.timesheets_financials%ROWTYPE;
  v_existing_new_tsfin public.timesheets_financials%ROWTYPE;
  v_new_tsfin public.timesheets_financials%ROWTYPE;

  v_chain jsonb;
  v_preflight jsonb;
  v_root_timesheet_id uuid;
  v_correction_financials_policy_envelope jsonb;
  v_correction_financials_policy_envelope_fingerprint text;
  v_actual_policy_envelope_fingerprint text;
  v_replacement_policy jsonb;
  v_operation_unit_count integer := 0;
  v_operation_contract jsonb;
  v_operation_contract_fingerprint text;
  v_operation_unit jsonb;
  v_replay boolean := false;
  v_old_paid_digest text;
BEGIN
  IF p_timesheet_id IS NULL THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_TIMESHEET_ID_REQUIRED'
      USING ERRCODE = '22023';
  END IF;

  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_ACTOR_REQUIRED'
      USING ERRCODE = '22023';
  END IF;

  IF p_operation_id IS NULL THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_OPERATION_ID_REQUIRED'
      USING ERRCODE = '22023';
  END IF;

  IF p_expected_current_tsfin_id IS NULL THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_EXPECTED_TSFIN_ID_REQUIRED'
      USING ERRCODE = '22023';
  END IF;

  IF v_expected_preflight_fingerprint IS NULL
     OR char_length(v_expected_preflight_fingerprint) > 256 THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_PREFLIGHT_FINGERPRINT_INVALID'
      USING ERRCODE = '22023';
  END IF;

  SELECT operation_row.*
  INTO v_operation
  FROM public.import_apply_operations AS operation_row
  WHERE operation_row.id = p_operation_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_OPERATION_NOT_FOUND'
      USING ERRCODE = 'P0002',
            DETAIL = jsonb_build_object(
              'operation_id', p_operation_id::text
            )::text;
  END IF;

  IF v_operation.actor_user_id IS DISTINCT FROM p_actor_user_id THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_ACTOR_MISMATCH'
      USING ERRCODE = '42501',
            DETAIL = jsonb_build_object(
              'operation_id', p_operation_id::text,
              'expected_actor_user_id', v_operation.actor_user_id::text,
              'supplied_actor_user_id', p_actor_user_id::text
            )::text;
  END IF;

  IF v_operation.state <> 'PREPARED' THEN
    IF v_operation.state IN (
      'SOURCE_COMMITTED_TSFIN_PENDING',
      'FINANCIALISED_PENDING_FINALISATION',
      'COMPLETE'
    ) THEN
      SELECT current_financial.*
      INTO v_existing_new_tsfin
      FROM public.timesheets_financials AS current_financial
      WHERE current_financial.timesheet_id = p_timesheet_id
        AND current_financial.is_current = true
        AND current_financial.id <> p_expected_current_tsfin_id
      ORDER BY current_financial.computed_at_utc DESC, current_financial.id DESC
      LIMIT 1;

      IF FOUND THEN
        RETURN jsonb_build_object(
          'ok', true,
          'replay', true,
          'operation_id', p_operation_id::text,
          'timesheet_id', p_timesheet_id::text,
          'historical_paid_tsfin_id', p_expected_current_tsfin_id::text,
          'new_current_tsfin_id', v_existing_new_tsfin.id::text,
          'new_current_processing_status',
            v_existing_new_tsfin.processing_status::text,
          'operation_state', v_operation.state
        );
      END IF;
    END IF;

    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_OPERATION_STATE_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'operation_id', p_operation_id::text,
              'state', v_operation.state,
              'required_state', 'PREPARED'
            )::text;
  END IF;

  v_chain := public.timesheet_correction_chain_scope_v1(
    p_timesheet_id,
    true,
    32,
    100
  );

  IF COALESCE((v_chain ->> 'valid')::boolean, false) IS NOT TRUE THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_CHAIN_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'chain', v_chain
            )::text;
  END IF;


  v_root_timesheet_id :=
    NULLIF(v_chain ->> 'root_timesheet_id', '')::uuid;
  v_operation_contract:=v_operation.response_json#>'{correction_operation_contract}';
  if jsonb_typeof(v_operation_contract)<>'object'
     or v_operation_contract->>'schema_version'<>'IMPORT_CORRECTION_OPERATION_V2'
     or v_operation_contract->>'operation_id' is distinct from p_operation_id::text then
    raise exception 'PAID_TSFIN_ROLLOVER_OPERATION_CONTRACT_INVALID' using errcode='P0001';
  end if;
  v_operation_contract_fingerprint:=encode(extensions.digest(
    convert_to((v_operation_contract-'operation_contract_fingerprint')::text,'UTF8'),
    'sha256'::text
  ),'hex');
  if v_operation_contract->>'operation_contract_fingerprint'
     is distinct from v_operation_contract_fingerprint then
    raise exception 'PAID_TSFIN_ROLLOVER_OPERATION_CONTRACT_FINGERPRINT_INVALID'
      using errcode='P0001';
  end if;
  -- The paid source row predates the correction member, so read the one exact
  -- frozen unit from the durable operation contract rather than trusting a
  -- caller-supplied or obsolete top-level response field.
  select count(*)::integer,min(unit::text)::jsonb
  into v_operation_unit_count,v_operation_unit
  from jsonb_array_elements(
    case when jsonb_typeof(v_operation_contract->'correction_units')='array'
      then v_operation_contract->'correction_units'
      else '[]'::jsonb end
  ) unit
  where unit->>'root_timesheet_id'=v_root_timesheet_id::text;
  if v_operation_unit_count<>1 then
    raise exception 'PAID_TSFIN_ROLLOVER_OPERATION_UNIT_NOT_UNIQUE'
      using errcode='P0001',detail=jsonb_build_object(
        'operation_id',p_operation_id,'root_timesheet_id',v_root_timesheet_id,
        'matching_unit_count',v_operation_unit_count
      )::text;
  end if;
  v_correction_financials_policy_envelope:=v_operation_unit->'policy_envelope';
  v_correction_financials_policy_envelope_fingerprint := NULLIF(
    v_correction_financials_policy_envelope ->> 'envelope_fingerprint', ''
  );
  v_replacement_policy := case
    when v_correction_financials_policy_envelope->>'correction_shape'='REVERSAL_ONLY'
      then v_correction_financials_policy_envelope->'reversal'
    else v_correction_financials_policy_envelope->'replacement' end;

  IF jsonb_typeof(v_correction_financials_policy_envelope) <> 'object'
     OR v_correction_financials_policy_envelope_fingerprint IS NULL
     OR v_correction_financials_policy_envelope#>>'{operation,operation_id}'
        IS DISTINCT FROM p_operation_id::text
     OR v_operation_unit->>'policy_envelope_fingerprint'
        IS DISTINCT FROM v_correction_financials_policy_envelope_fingerprint
     OR coalesce((v_replacement_policy->>'applicable')::boolean,false) IS NOT TRUE THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_POLICY_ENVELOPE_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'root_timesheet_id', v_root_timesheet_id::text,
              'chain_errors', COALESCE(v_chain -> 'errors', '[]'::jsonb)
            )::text;
  END IF;

  v_actual_policy_envelope_fingerprint := encode(
    extensions.digest(
      convert_to(
        (v_correction_financials_policy_envelope - 'envelope_fingerprint')::text,
        'UTF8'
      ),
      'sha256'::text
    ),
    'hex'
  );

  IF v_actual_policy_envelope_fingerprint
       IS DISTINCT FROM v_correction_financials_policy_envelope_fingerprint THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_POLICY_ENVELOPE_FINGERPRINT_INVALID'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object(
              'stored_fingerprint',
                v_correction_financials_policy_envelope_fingerprint,
              'actual_fingerprint', v_actual_policy_envelope_fingerprint
            )::text;
  END IF;

  SELECT timesheet_row.*
  INTO v_timesheet
  FROM public.timesheets AS timesheet_row
  WHERE timesheet_row.timesheet_id = p_timesheet_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_TIMESHEET_NOT_FOUND'
      USING ERRCODE = 'P0002';
  END IF;

  IF COALESCE(v_timesheet.is_current, false) IS NOT TRUE THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_TIMESHEET_NOT_CURRENT'
      USING ERRCODE = 'P0001';
  END IF;

  IF v_timesheet.authorised_at_server IS NOT NULL THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_REQUIRES_UNAUTHORISED_TIMESHEET'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'timesheet_id', p_timesheet_id::text,
              'authorised_at_server', v_timesheet.authorised_at_server
            )::text;
  END IF;

  SELECT financial_row.*
  INTO v_old_tsfin
  FROM public.timesheets_financials AS financial_row
  WHERE financial_row.id = p_expected_current_tsfin_id
    AND financial_row.timesheet_id = p_timesheet_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_EXPECTED_TSFIN_NOT_FOUND'
      USING ERRCODE = 'P0002';
  END IF;

  IF COALESCE(v_old_tsfin.is_current, false) IS NOT TRUE THEN
    SELECT current_financial.*
    INTO v_existing_new_tsfin
    FROM public.timesheets_financials AS current_financial
    WHERE current_financial.timesheet_id = p_timesheet_id
      AND current_financial.is_current = true
      AND current_financial.id <> v_old_tsfin.id
    ORDER BY current_financial.computed_at_utc DESC, current_financial.id DESC
    LIMIT 1;

    IF FOUND THEN
      RETURN jsonb_build_object(
        'ok', true,
        'replay', true,
        'operation_id', p_operation_id::text,
        'timesheet_id', p_timesheet_id::text,
        'historical_paid_tsfin_id', v_old_tsfin.id::text,
        'new_current_tsfin_id', v_existing_new_tsfin.id::text,
        'new_current_processing_status',
          v_existing_new_tsfin.processing_status::text,
        'operation_state', v_operation.state
      );
    END IF;

    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_EXPECTED_TSFIN_NOT_CURRENT'
      USING ERRCODE = 'P0001';
  END IF;

  IF v_old_tsfin.authorised_at_utc IS NOT NULL THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_REQUIRES_UNAUTHORISED_TSFIN'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'timesheet_id', p_timesheet_id::text,
              'tsfin_id', v_old_tsfin.id::text,
              'authorised_at_utc', v_old_tsfin.authorised_at_utc
            )::text;
  END IF;

  IF v_old_tsfin.paid_at_utc IS NULL THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_PAID_EVIDENCE_REQUIRED'
      USING ERRCODE = 'P0001';
  END IF;

  IF v_old_tsfin.locked_by_invoice_id IS NOT NULL
     OR EXISTS (
       SELECT 1
       FROM public.invoice_lines AS invoice_line
       WHERE invoice_line.timesheet_id = p_timesheet_id
     ) THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_INVOICE_EVIDENCE_BLOCKS'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'timesheet_id', p_timesheet_id::text,
              'locked_by_invoice_id', CASE
                WHEN v_old_tsfin.locked_by_invoice_id IS NULL THEN NULL
                ELSE v_old_tsfin.locked_by_invoice_id::text
              END
            )::text;
  END IF;

  v_preflight := public.import_timesheet_financial_preflight_v1(
    ARRAY[p_timesheet_id]::uuid[],
    'PAID_UNINVOICED_ROLLOVER',
    p_actor_user_id,
    jsonb_build_object(
      'chain_fingerprints', jsonb_build_object(
        v_root_timesheet_id::text,
        v_chain ->> 'chain_fingerprint'
      ),
      'correction_financials_policy_envelope_fingerprints', jsonb_build_object(
        v_root_timesheet_id::text,
        v_correction_financials_policy_envelope_fingerprint
      )
    ),
    false,
    100
  );

  IF COALESCE((v_preflight ->> 'allowed')::boolean, false) IS NOT TRUE THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_PREFLIGHT_BLOCKED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'preflight', v_preflight
            )::text;
  END IF;

  IF v_preflight ->> 'preflight_fingerprint'
       IS DISTINCT FROM v_expected_preflight_fingerprint THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_PREFLIGHT_STALE'
      USING ERRCODE = '40001',
            DETAIL = jsonb_build_object(
              'expected_preflight_fingerprint',
                v_expected_preflight_fingerprint,
              'actual_preflight_fingerprint',
                v_preflight ->> 'preflight_fingerprint'
            )::text;
  END IF;

  v_old_paid_digest := encode(
    extensions.digest(
      convert_to(
        jsonb_build_object(
          'id', v_old_tsfin.id::text,
          'timesheet_id', v_old_tsfin.timesheet_id::text,
          'timesheet_version', v_old_tsfin.timesheet_version,
          'paid_at_utc', v_old_tsfin.paid_at_utc,
          'paid_by_user_id', CASE
            WHEN v_old_tsfin.paid_by_user_id IS NULL THEN NULL
            ELSE v_old_tsfin.paid_by_user_id::text
          END,
          'payment_reference', v_old_tsfin.payment_reference,
          'total_hours', v_old_tsfin.total_hours,
          'total_pay_ex_vat', v_old_tsfin.total_pay_ex_vat,
          'total_charge_ex_vat', v_old_tsfin.total_charge_ex_vat,
          'pay_vat_rate_pct_snapshot',
            v_old_tsfin.pay_vat_rate_pct_snapshot,
          'pay_vat_amount_snapshot',
            v_old_tsfin.pay_vat_amount_snapshot,
          'pay_total_inc_vat_snapshot',
            v_old_tsfin.pay_total_inc_vat_snapshot,
          'policy_snapshot_json', v_old_tsfin.policy_snapshot_json,
          'rate_source_refs_json', v_old_tsfin.rate_source_refs_json,
          'actual_schedule_json', v_old_tsfin.actual_schedule_json
        )::text,
        'UTF8'
      ),
      'sha256'
    ),
    'hex'
  );

  UPDATE public.timesheets_financials AS historical_financial
  SET is_current = false
  WHERE historical_financial.id = v_old_tsfin.id
    AND historical_financial.is_current = true;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAID_TSFIN_ROLLOVER_CONCURRENT_CURRENT_CHANGE'
      USING ERRCODE = '40001';
  END IF;

  INSERT INTO public.timesheets_financials (
    timesheet_id,
    timesheet_version,
    basis,
    is_current,
    is_stale,
    stale_reason,
    candidate_id,
    client_id,
    role,
    band,
    pay_method,
    policy_snapshot_json,
    rate_source_refs_json,
    computed_at_utc,
    created_at,
    updated_at,
    occupant_key_norm,
    candidate_assignment,
    processing_status,
    processed_by_user_id,
    processed_at_utc,
    po_number,
    pay_on_hold,
    pay_on_hold_reason,
    pay_on_hold_since_utc,
    expenses_pay_ex_vat,
    expenses_charge_ex_vat,
    expenses_description,
    expenses_evidence_r2_key,
    expenses_evidence_manifest,
    mileage_units,
    mileage_pay_ex_vat,
    mileage_charge_ex_vat,
    mileage_pay_rate,
    mileage_charge_rate,
    mileage_evidence_r2_key,
    mileage_evidence_manifest,
    travel_pay_ex_vat,
    travel_charge_ex_vat,
    accommodation_pay_ex_vat,
    accommodation_charge_ex_vat,
    other_pay_ex_vat,
    other_charge_ex_vat,
    hr_crosscheck_status,
    hr_crosscheck_issues,
    external_source_rows_json,
    actual_schedule_json,
    additional_units_json,
    invoice_breakdown_json,
    nhsp_import_id,
    has_rate_issue,
    has_pay_channel_issue
  )
  VALUES (
    p_timesheet_id,
    v_timesheet.version,
    v_old_tsfin.basis,
    true,
    true,
    'IMPORT_PAID_TSFIN_ROLLOVER_PENDING_CALCULATION',
    v_old_tsfin.candidate_id,
    v_old_tsfin.client_id,
    v_old_tsfin.role,
    v_old_tsfin.band,
    v_old_tsfin.pay_method,
    jsonb_build_object(
      'import_apply_operation_id', p_operation_id::text,
      'rollover_source_tsfin_id', v_old_tsfin.id::text,
      'rollover_source_paid_digest', v_old_paid_digest,
      'correction_financials_policy_envelope', v_correction_financials_policy_envelope,
      'correction_financials_policy_envelope_fingerprint',
        v_correction_financials_policy_envelope_fingerprint,
      'requires_frozen_correction_policy', true,
      'correction_finance_override_fields', jsonb_build_array(
        'erni_pct',
        'apply_erni_to',
        'vat_rate_pct'
      ),
      'erni_pct', v_replacement_policy #> '{tsfin_policy,erni_pct}',
      'apply_erni_to',
        v_replacement_policy #>> '{tsfin_policy,apply_erni_to}',
      'vat_rate_pct',
        v_replacement_policy #> '{tsfin_policy,applied_pay_vat_rate_pct}',
      'pay_vat_rate_pct',
        v_replacement_policy #> '{tsfin_policy,applied_pay_vat_rate_pct}',
      'correction_leg_fingerprint',
        v_replacement_policy ->> 'leg_fingerprint',
      'correction_tsfin_policy',
        v_replacement_policy -> 'tsfin_policy',
      'correction_tsfin_policy_fingerprint',
        v_replacement_policy #>> '{tsfin_policy,tsfin_policy_fingerprint}',
      'correction_invoice_policy',
        v_replacement_policy -> 'invoice_policy',
      'correction_invoice_policy_fingerprint',
        v_replacement_policy #>> '{invoice_policy,invoice_policy_fingerprint}',
      'correction_invoice_stream',
        v_replacement_policy #>> '{invoice_policy,invoice_stream}'
    ),
    COALESCE(v_old_tsfin.rate_source_refs_json, '{}'::jsonb)
      || jsonb_build_object(
        'correction_financials_policy_envelope', v_correction_financials_policy_envelope,
        'correction_financials_policy_envelope_fingerprint',
          v_correction_financials_policy_envelope_fingerprint,
        'rollover_source_tsfin_id', v_old_tsfin.id::text
      ),
    v_now,
    v_now,
    v_now,
    v_old_tsfin.occupant_key_norm,
    v_old_tsfin.candidate_assignment,
    'PENDING_AUTH'::public.ts_fin_processing_status_enum,
    v_old_tsfin.processed_by_user_id,
    v_old_tsfin.processed_at_utc,
    v_old_tsfin.po_number,
    v_old_tsfin.pay_on_hold,
    v_old_tsfin.pay_on_hold_reason,
    v_old_tsfin.pay_on_hold_since_utc,
    v_old_tsfin.expenses_pay_ex_vat,
    v_old_tsfin.expenses_charge_ex_vat,
    v_old_tsfin.expenses_description,
    v_old_tsfin.expenses_evidence_r2_key,
    v_old_tsfin.expenses_evidence_manifest,
    v_old_tsfin.mileage_units,
    v_old_tsfin.mileage_pay_ex_vat,
    v_old_tsfin.mileage_charge_ex_vat,
    v_old_tsfin.mileage_pay_rate,
    v_old_tsfin.mileage_charge_rate,
    v_old_tsfin.mileage_evidence_r2_key,
    v_old_tsfin.mileage_evidence_manifest,
    v_old_tsfin.travel_pay_ex_vat,
    v_old_tsfin.travel_charge_ex_vat,
    v_old_tsfin.accommodation_pay_ex_vat,
    v_old_tsfin.accommodation_charge_ex_vat,
    v_old_tsfin.other_pay_ex_vat,
    v_old_tsfin.other_charge_ex_vat,
    v_old_tsfin.hr_crosscheck_status,
    v_old_tsfin.hr_crosscheck_issues,
    v_old_tsfin.external_source_rows_json,
    COALESCE(v_timesheet.actual_schedule_json, '[]'::jsonb),
    COALESCE(
      jsonb_build_object(
        'week', COALESCE(v_timesheet.additional_units_week, '{}'::jsonb),
        'per_day', COALESCE(
          v_timesheet.additional_units_per_day,
          '{}'::jsonb
        )
      ),
      '{}'::jsonb
    ),
    '{}'::jsonb,
    v_old_tsfin.nhsp_import_id,
    false,
    false
  )
  RETURNING *
  INTO v_new_tsfin;

  PERFORM public._inv_write_audit(
    p_actor_user_id,
    'IMPORT_PAID_TSFIN_ROLLED',
    jsonb_build_object(
      'operation_id', p_operation_id::text,
      'timesheet_id', p_timesheet_id::text,
      'historical_paid_tsfin_id', v_old_tsfin.id::text,
      'historical_paid_digest', v_old_paid_digest,
      'new_current_tsfin_id', v_new_tsfin.id::text,
      'new_current_processing_status',
        v_new_tsfin.processing_status::text,
      'correction_financials_policy_envelope_fingerprint',
        v_correction_financials_policy_envelope_fingerprint
    ),
    'timesheet_financials',
    v_new_tsfin.id::text,
    jsonb_build_object(
      'source_tsfin_id', v_old_tsfin.id::text,
      'source_is_current', true,
      'source_paid_at_utc', v_old_tsfin.paid_at_utc
    ),
    'Paid but uninvoiced TSFIN rollover before import amendment',
    NULL::text,
    NULL::text,
    'import-operation:' || p_operation_id::text
  );

  RETURN jsonb_build_object(
    'ok', true,
    'replay', v_replay,
    'operation_id', p_operation_id::text,
    'timesheet_id', p_timesheet_id::text,
    'historical_paid_tsfin_id', v_old_tsfin.id::text,
    'historical_paid_digest', v_old_paid_digest,
    'new_current_tsfin_id', v_new_tsfin.id::text,
    'new_current_processing_status',
      v_new_tsfin.processing_status::text,
    'new_current_is_stale', v_new_tsfin.is_stale,
    'new_current_stale_reason', v_new_tsfin.stale_reason,
    'correction_financials_policy_envelope', v_correction_financials_policy_envelope,
    'correction_financials_policy_envelope_fingerprint',
      v_correction_financials_policy_envelope_fingerprint,
    'requires_frozen_correction_policy', true,
    'requires_calculation', true,
    'requires_reauthorisation', true
  );
END;
$function$;

CREATE OR REPLACE FUNCTION public.import_review_correction_generation_transition_v1(p_import_id uuid, p_operation_id uuid, p_request_hash text, p_action text, p_actor_user_id uuid, p_action_ids text[] DEFAULT '{}'::text[], p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_operation public.import_apply_operations%rowtype;
  v_action text:=upper(btrim(coalesce(p_action,'')));
  v_units jsonb:='[]'::jsonb;
  v_unit jsonb;
  v_balance jsonb;
  v_capability_token text;
  v_items jsonb;
  v_result jsonb;
  v_target_ids uuid[];
  v_pending_target_ids uuid[];
  v_member_count integer;
  v_bad_count integer;
  v_id uuid;
  v_signature jsonb;
  v_current_invoice_fingerprint text;
  v_recomputed_unit_fingerprint text;
  v_all_authorised boolean:=false;
  v_any_authorised boolean:=false;
  v_unit_fingerprints jsonb:='[]'::jsonb;
begin
  perform public._import_review_assert_actor_v1(p_actor_user_id);
  if session_user not in ('postgres','service_role') and coalesce(
      current_setting('request.jwt.claim.role',true),
      nullif(current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception 'IMPORT_REVIEW_RECONCILIATION_OPERATION_INVALID' using errcode='42501';
  end if;
  if p_import_id is null or p_operation_id is null or length(btrim(coalesce(p_request_hash,''))) not between 16 and 256
     or v_action not in ('PREPARE','VALIDATE','AUTHORISE') or cardinality(coalesce(p_action_ids,array[]::text[]))>100 then
    raise exception 'IMPORT_REVIEW_RECONCILIATION_OPERATION_INVALID' using errcode='22023';
  end if;
  perform set_config('lock_timeout','1500ms',true);
  select * into v_operation from public.import_apply_operations
  where id=p_operation_id and import_id=p_import_id for update;
  if v_operation.id is null or v_operation.request_hash<>lower(btrim(p_request_hash)) then
    raise exception 'IMPORT_REVIEW_RECONCILIATION_OPERATION_INVALID' using errcode='40001';
  end if;
  if v_action in ('VALIDATE','AUTHORISE') and (
      v_operation.committed_at_utc is null
      or v_operation.state not in ('SOURCE_COMMITTED_TSFIN_PENDING','COMPLETE')) then
    raise exception 'IMPORT_REVIEW_RECONCILIATION_OPERATION_INVALID' using errcode='40001';
  end if;

  if v_action='PREPARE' then
    if to_regclass('pg_temp.import_review_reconciliation_units_v1') is null
       or current_setting('cloudtms.import_reconciliation_operation_id',true) is distinct from p_operation_id::text
       or current_setting('cloudtms.import_reconciliation_request_hash',true) is distinct from v_operation.request_hash then
      raise exception 'IMPORT_REVIEW_RECONCILIATION_GUARD_REQUIRED' using errcode='55000';
    end if;
    select coalesce(jsonb_agg(u.unit_json order by u.action_id),'[]'::jsonb) into v_units
    from pg_temp.import_review_reconciliation_units_v1 u
    where cardinality(coalesce(p_action_ids,array[]::text[]))=0 or u.action_id=any(p_action_ids);
  else
    v_units:=coalesce(v_operation.response_json#>'{request_envelope,reconciliation_units}',
      v_operation.response_json->'reconciliation_units','[]'::jsonb);
    if cardinality(coalesce(p_action_ids,array[]::text[]))>0 then
      select coalesce(jsonb_agg(u order by u->>'action_id'),'[]'::jsonb) into v_units
      from jsonb_array_elements(v_units) u where u->>'action_id'=any(p_action_ids);
    end if;
  end if;
  if jsonb_array_length(v_units)=0 then
    return jsonb_build_object('ok',true,'action',v_action,'idempotent',true,'unit_count',0,'timesheet_ids','[]'::jsonb);
  end if;
  select coalesce(jsonb_agg(u->>'unit_fingerprint' order by u->>'action_id'),'[]'::jsonb)
  into v_unit_fingerprints from jsonb_array_elements(v_units) u;
  if exists(select 1 from jsonb_array_elements(v_units) u
      where u->>'schema_version'<>'IMPORT_AUTHORITATIVE_RECONCILIATION_V1'
        or nullif(u->>'unit_fingerprint','') is null
        or nullif(u->>'source_identity','') is null
        or u->>'route' not in ('AMEND_SOURCE','AMEND_PAID_UNINVOICED_SOURCE','AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT')) then
    raise exception 'IMPORT_REVIEW_RECONCILIATION_UNIT_NOT_FOUND' using errcode='22023';
  end if;
  if v_action<>'PREPARE' and exists(
    select 1
    from jsonb_array_elements(v_units) u
    left join public.import_review_action_outcomes outcome
      on outcome.operation_id=p_operation_id and outcome.action_id=u->>'action_id'
    where outcome.action_id is null
       or u->>'unit_fingerprint' is distinct from public._import_review_hash_v1(concat_ws('|','unit-v1',
         u->>'action_id',u->>'source_identity',u->>'route',u->>'reconciliation_fingerprint',outcome.evidence_fingerprint))
  ) then
    raise exception 'IMPORT_REVIEW_RECONCILIATION_FINGERPRINT_MISMATCH' using errcode='40001';
  end if;

  if v_action='PREPARE' then
    select coalesce(array_agg(distinct x.value::uuid order by x.value::uuid),array[]::uuid[]) into v_target_ids
    from jsonb_array_elements(v_units) u
    cross join lateral jsonb_array_elements_text(coalesce(u->'M_active_member_ids','[]'::jsonb)) x(value)
    join public.timesheets t on t.timesheet_id=x.value::uuid and t.is_current and t.archived_at_utc is null
    left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
    left join public.contract_weeks cw on cw.timesheet_id=t.timesheet_id
    where t.authorised_at_server is not null or tf.authorised_at_utc is not null
       or cw.status='AUTHORISED'::public.contract_week_status_enum;
    if exists(select 1 from jsonb_array_elements(v_units) u
      cross join lateral jsonb_array_elements_text(coalesce(u->'M_active_member_ids','[]'::jsonb)) x(value)
      join public.timesheets t on t.timesheet_id=x.value::uuid where t.archived_at_utc is not null) then
      raise exception 'IMPORT_REVIEW_RECONCILIATION_ARCHIVED_MEMBER_EXCLUDED' using errcode='55000';
    end if;
    if cardinality(v_target_ids)>0 then
      v_capability_token:=encode(gen_random_bytes(32),'hex');
      create temporary table if not exists pg_temp.import_review_lifecycle_capability_v1(
        capability_token text not null,txid bigint not null,operation_id uuid not null,request_hash text not null,
        actor_user_id uuid not null,action text not null,action_id text not null,unit_fingerprint text not null,
        timesheet_id uuid not null,expected_timesheet_id uuid not null,expected_version integer,
        expected_row_signature text,expected_tsfin_id uuid,expected_contract_week_id uuid
      ) on commit drop;
      truncate pg_temp.import_review_lifecycle_capability_v1;
      foreach v_id in array v_target_ids loop
        v_signature:=public.timesheet_lifecycle_signature_v1(v_id,null,false);
        insert into pg_temp.import_review_lifecycle_capability_v1
        select v_capability_token,txid_current(),p_operation_id,v_operation.request_hash,p_actor_user_id,'UNAUTHORISE',
          u->>'action_id',u->>'unit_fingerprint',v_id,v_id,t.version,
          coalesce(v_signature->>'backend_row_signature',v_signature->>'row_signature',v_signature->>'signature'),
          tf.id,cw.id
        from jsonb_array_elements(v_units) u
        join lateral jsonb_array_elements_text(coalesce(u->'M_active_member_ids','[]'::jsonb)) x(value) on x.value::uuid=v_id
        join public.timesheets t on t.timesheet_id=v_id and t.is_current and t.archived_at_utc is null
        left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
        left join public.contract_weeks cw on cw.timesheet_id=t.timesheet_id;
      end loop;
      perform set_config('cloudtms.import_reconciliation_capability_token',v_capability_token,true);
      perform set_config('cloudtms.import_reconciliation_operation_id',p_operation_id::text,true);
      perform set_config('cloudtms.import_reconciliation_action','UNAUTHORISE',true);
      select jsonb_agg(jsonb_build_object('timesheet_id',x) order by x) into v_items from unnest(v_target_ids) x;
      v_result:=public.timesheet_unauthorise_bulk_atomic(v_items,p_actor_user_id,coalesce(p_now_utc,now()));
      if coalesce((v_result->>'ok')::boolean,false) is not true or coalesce((v_result->>'all_success')::boolean,false) is not true then
        raise exception 'IMPORT_REVIEW_RECONCILIATION_PREPARE_INCOMPLETE' using errcode='55000',detail=coalesce(v_result->>'error_code','bulk unauthorise incomplete');
      end if;
      truncate pg_temp.import_review_lifecycle_capability_v1;
      perform set_config('cloudtms.import_reconciliation_capability_token','',true);
      perform set_config('cloudtms.import_reconciliation_action','',true);
    end if;
    if exists(select 1 from jsonb_array_elements(v_units) u
      cross join lateral jsonb_array_elements_text(coalesce(u->'M_active_member_ids','[]'::jsonb)) x(value)
      join public.timesheets t on t.timesheet_id=x.value::uuid and t.is_current and t.archived_at_utc is null
      left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
      left join public.contract_weeks cw on cw.timesheet_id=t.timesheet_id
      where t.authorised_at_server is not null or tf.authorised_at_utc is not null
         or cw.status='AUTHORISED'::public.contract_week_status_enum) then
      raise exception 'IMPORT_REVIEW_RECONCILIATION_PREPARE_INCOMPLETE' using errcode='55000';
    end if;
    return jsonb_build_object('ok',true,'action','PREPARE','unit_count',jsonb_array_length(v_units),
      'timesheet_ids',to_jsonb(coalesce(v_target_ids,array[]::uuid[])),'bulk_result',v_result);
  end if;

  for v_unit in select value from jsonb_array_elements(v_units) loop
    perform 1 from public.invoices i where i.id in (
      select x.value::uuid from jsonb_array_elements_text(coalesce(v_unit->'B_effective_invoice_ids','[]'::jsonb)) x(value)
    ) order by i.id for update;
    perform 1 from public.invoice_lines il where il.id in (
      select x.value::uuid from jsonb_array_elements_text(coalesce(v_unit->'B_effective_invoice_line_ids','[]'::jsonb)) x(value)
    ) order by il.id for update;
    perform 1 from public.nhsp_shifts s where s.id=(v_unit->>'source_shift_id')::uuid for update;
    if not exists(select 1 from public.nhsp_shifts s where s.id=(v_unit->>'source_shift_id')::uuid
      and s.external_row_key=v_unit->>'source_identity' and s.cancelled_at_utc is null
      and s.source_system::text=v_unit->>'source_system') then
      raise exception 'IMPORT_REVIEW_RECONCILIATION_SOURCE_MISMATCH' using errcode='40001';
    end if;
    perform 1 from public.timesheets t where t.correction_id=v_unit->>'correction_id'
      and t.is_current and t.archived_at_utc is null order by t.timesheet_id for update;
    perform 1 from public.timesheets_financials tf where tf.is_current and tf.timesheet_id in (
      select t.timesheet_id from public.timesheets t where t.correction_id=v_unit->>'correction_id'
        and t.is_current and t.archived_at_utc is null
    ) order by tf.timesheet_id,tf.id for update;
    perform 1 from public.contract_weeks cw where cw.timesheet_id in (
      select t.timesheet_id from public.timesheets t where t.correction_id=v_unit->>'correction_id'
        and t.is_current and t.archived_at_utc is null
    ) order by cw.id for update;
    select b.balance_json into v_balance
    from public._import_review_effective_invoice_balance_core_v1(p_import_id,jsonb_build_array(jsonb_build_object(
      'source_identity',v_unit->>'source_identity','source_system',v_unit->>'source_system',
      'source_shift_id',v_unit->>'source_shift_id','external_row_key',v_unit->>'source_identity',
      'hr_row_id',v_unit->>'hr_row_id','source_timesheet_id',v_unit->>'source_timesheet_id',
      'candidate_id',v_unit->>'candidate_id','client_id',v_unit->>'client_id','contract_id',v_unit->>'contract_id',
      'week_ending_date',v_unit->>'week_ending_date','invoice_stream',v_unit->>'invoice_stream',
      'authoritative_import_id',p_import_id,'authoritative_schedule_json',v_unit->'A_schedule_json',
      'authoritative_hours',v_unit->'A_hours')),100,512,256,128) b;
    if nullif(v_balance->>'blocking_code','') is not null then
      raise exception 'IMPORT_REVIEW_INVOICE_ACTIVITY_IN_PROGRESS' using errcode='55000',detail=v_balance->>'blocking_code';
    end if;
    v_current_invoice_fingerprint:=v_balance->>'effective_invoice_fingerprint';
    if v_current_invoice_fingerprint is distinct from v_unit->>'B_invoice_fingerprint' then
      raise exception 'IMPORT_REVIEW_SELECTED_ACTION_STALE' using errcode='40001';
    end if;
    if v_unit->>'route' in ('AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT') then
      select count(*) into v_member_count from public.timesheets t
      where t.correction_id=v_unit->>'correction_id' and t.is_current and t.archived_at_utc is null
        and t.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT');
      if v_member_count<>2 or not exists(select 1 from public.timesheets t where t.correction_id=v_unit->>'correction_id'
          and t.is_current and t.archived_at_utc is null and t.correction_kind='CHANGED_HOURS_REVERSAL')
        or not exists(select 1 from public.timesheets t where t.correction_id=v_unit->>'correction_id'
          and t.is_current and t.archived_at_utc is null and t.correction_kind='CHANGED_HOURS_REPLACEMENT') then
        raise exception 'IMPORT_REVIEW_RECONCILIATION_MEMBER_SET_MISMATCH' using errcode='55000';
      end if;
      if exists(select 1 from public.timesheets t
        left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
        where t.correction_id=v_unit->>'correction_id' and t.is_current and t.archived_at_utc is null
          and t.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT') and (
            not coalesce(t.is_adjustment,false) or t.adjustment_origin<>'IMPORT_CORRECTION'
            or t.parent_timesheet_id is distinct from (v_unit->>'parent_timesheet_id')::uuid
            or t.contract_id is distinct from (v_unit->>'contract_id')::uuid
            or t.week_ending_date is distinct from (v_unit->>'week_ending_date')::date
            or t.sheet_scope<>'WEEKLY'::public.timesheet_scope_enum
            or tf.candidate_id is distinct from (v_unit->>'candidate_id')::uuid
            or tf.client_id is distinct from (v_unit->>'client_id')::uuid
            or (v_unit->>'source_system'='NHSP' and tf.basis<>'NHSP_ADJUSTMENT'::public.timesheet_fin_basis_enum)
            or (v_unit->>'source_system'='HEALTHROSTER' and tf.basis<>'HEALTHROSTER_ADJUSTMENT'::public.timesheet_fin_basis_enum)
            or t.candidate_hint_text#>>'{import_authoritative_reconciliation,operation_id}'<>p_operation_id::text
            or t.candidate_hint_text#>>'{import_authoritative_reconciliation,unit_fingerprint}'<>v_unit->>'unit_fingerprint'
            or t.candidate_hint_text#>>'{import_authoritative_reconciliation,source_identity}'<>v_unit->>'source_identity'
            or jsonb_typeof(t.actual_schedule_json)<>'array'
            or jsonb_array_length(t.actual_schedule_json)<>1
            or not t.actual_schedule_json @> jsonb_build_array(jsonb_build_object(
              'shift_id',v_unit->>'source_shift_id','external_row_key',v_unit->>'source_identity'))
            or (select count(*) from public.contract_weeks cw where cw.timesheet_id=t.timesheet_id)<>1
            or exists(select 1 from public.contract_weeks cw where cw.timesheet_id=t.timesheet_id
              and (not coalesce(cw.is_adjustment,false)
                or cw.contract_id is distinct from (v_unit->>'contract_id')::uuid
                or cw.week_ending_date is distinct from (v_unit->>'week_ending_date')::date))
          ))
         or not exists(select 1 from public.timesheets parent_ts
           where parent_ts.timesheet_id=(v_unit->>'parent_timesheet_id')::uuid
             and parent_ts.is_current and parent_ts.archived_at_utc is null) then
        raise exception 'IMPORT_REVIEW_RECONCILIATION_SOURCE_MISMATCH' using errcode='40001';
      end if;
      if exists(select 1 from public.timesheets t
        where t.correction_id=v_unit->>'correction_id' and t.is_current and t.archived_at_utc is null
          and t.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
          and coalesce(t.candidate_hint_text#>>'{correction_financials_policy_envelope,envelope_fingerprint}','')
            is distinct from coalesce(public._ctms_correction_policy_envelope_read_v1(t.timesheet_id)->>'envelope_fingerprint',''))
        or (select count(distinct t.candidate_hint_text#>>'{correction_financials_policy_envelope,envelope_fingerprint}')
            from public.timesheets t where t.correction_id=v_unit->>'correction_id'
              and t.is_current and t.archived_at_utc is null
              and t.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT'))<>1 then
        raise exception 'IMPORT_REVIEW_RECONCILIATION_POLICY_MISMATCH' using errcode='40001';
      end if;
      if exists(select 1 from public.timesheets t
        left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
        where t.correction_id=v_unit->>'correction_id' and t.is_current and t.archived_at_utc is null
          and t.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
          and (tf.id is null or tf.processing_status not in (
            'PENDING_AUTH'::public.ts_fin_processing_status_enum,
            'READY_FOR_HR'::public.ts_fin_processing_status_enum,
            'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum))) then
        raise exception 'IMPORT_REVIEW_RECONCILIATION_TSFIN_NOT_SETTLED' using errcode='55000';
      end if;
      if exists(select 1 from public.timesheets t where t.correction_id=v_unit->>'correction_id'
        and t.is_current and t.archived_at_utc is null and (
          (t.correction_kind='CHANGED_HOURS_REVERSAL' and (
            (t.actual_schedule_json#>>'{0,start_utc}')::timestamptz is distinct from (v_unit#>>'{B_standard_schedule_json,0,start_utc}')::timestamptz
            or (t.actual_schedule_json#>>'{0,end_utc}')::timestamptz is distinct from (v_unit#>>'{B_standard_schedule_json,0,end_utc}')::timestamptz
            or coalesce((t.actual_schedule_json#>>'{0,break_mins}')::integer,0) is distinct from coalesce((v_unit#>>'{B_standard_schedule_json,0,break_mins}')::integer,0)))
          or (t.correction_kind='CHANGED_HOURS_REPLACEMENT' and (
            (t.actual_schedule_json#>>'{0,start_utc}')::timestamptz is distinct from (v_unit#>>'{A_schedule_json,0,start_utc}')::timestamptz
            or (t.actual_schedule_json#>>'{0,end_utc}')::timestamptz is distinct from (v_unit#>>'{A_schedule_json,0,end_utc}')::timestamptz
            or coalesce((t.actual_schedule_json#>>'{0,break_mins}')::integer,0) is distinct from coalesce((v_unit#>>'{A_schedule_json,0,break_mins}')::integer,0)))
        )) then
        raise exception 'IMPORT_REVIEW_RECONCILIATION_BALANCE_MISMATCH' using errcode='55000';
      end if;
      select count(*) into v_bad_count
      from public.timesheets t join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
      where t.correction_id=v_unit->>'correction_id' and t.is_current and t.archived_at_utc is null
        and (coalesce(tf.is_stale,true) or coalesce(tf.has_rate_issue,false) or coalesce(tf.has_pay_channel_issue,false));
      if v_bad_count>0 then raise exception 'IMPORT_REVIEW_RECONCILIATION_TSFIN_NOT_SETTLED' using errcode='55000'; end if;
      if exists(select 1 from public.timesheets t join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
        where t.correction_id=v_unit->>'correction_id' and t.is_current and t.archived_at_utc is null
          and ((t.correction_kind='CHANGED_HOURS_REVERSAL' and (
            tf.hours_day<>-coalesce((v_unit#>>'{B_hours,hours_day}')::numeric,0) or tf.hours_night<>-coalesce((v_unit#>>'{B_hours,hours_night}')::numeric,0)
            or tf.hours_sat<>-coalesce((v_unit#>>'{B_hours,hours_sat}')::numeric,0) or tf.hours_sun<>-coalesce((v_unit#>>'{B_hours,hours_sun}')::numeric,0)
            or tf.hours_bh<>-coalesce((v_unit#>>'{B_hours,hours_bh}')::numeric,0) or tf.total_pay_ex_vat<>-coalesce((v_unit#>>'{B_financials,pay_ex_vat}')::numeric,0)
            or tf.total_charge_ex_vat<>-coalesce((v_unit#>>'{B_financials,charge_ex_vat}')::numeric,0) or tf.margin_ex_vat<>-coalesce((v_unit#>>'{B_financials,margin_ex_vat}')::numeric,0)))
          or (t.correction_kind='CHANGED_HOURS_REPLACEMENT' and (
            tf.hours_day<>coalesce((v_unit#>>'{A_hours,hours_day}')::numeric,0) or tf.hours_night<>coalesce((v_unit#>>'{A_hours,hours_night}')::numeric,0)
            or tf.hours_sat<>coalesce((v_unit#>>'{A_hours,hours_sat}')::numeric,0) or tf.hours_sun<>coalesce((v_unit#>>'{A_hours,hours_sun}')::numeric,0)
            or tf.hours_bh<>coalesce((v_unit#>>'{A_hours,hours_bh}')::numeric,0))))) then
        raise exception 'IMPORT_REVIEW_RECONCILIATION_BALANCE_MISMATCH' using errcode='55000';
      end if;
    else
      if jsonb_array_length(coalesce(v_unit->'B_effective_invoice_ids','[]'::jsonb))<>0
         or jsonb_array_length(coalesce(v_unit->'B_effective_invoice_line_ids','[]'::jsonb))<>0
         or coalesce((v_unit#>>'{B_hours,hours_day}')::numeric,0)<>0
         or coalesce((v_unit#>>'{B_hours,hours_night}')::numeric,0)<>0
         or coalesce((v_unit#>>'{B_hours,hours_sat}')::numeric,0)<>0
         or coalesce((v_unit#>>'{B_hours,hours_sun}')::numeric,0)<>0
         or coalesce((v_unit#>>'{B_hours,hours_bh}')::numeric,0)<>0
         or coalesce((v_unit#>>'{B_financials,pay_ex_vat}')::numeric,0)<>0
         or coalesce((v_unit#>>'{B_financials,charge_ex_vat}')::numeric,0)<>0
         or coalesce((v_unit#>>'{B_financials,margin_ex_vat}')::numeric,0)<>0 then
        raise exception 'IMPORT_REVIEW_RECONCILIATION_BALANCE_MISMATCH' using errcode='55000';
      end if;
      if not exists(select 1 from public.timesheets t join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
        where t.timesheet_id=(v_unit->>'source_timesheet_id')::uuid and t.is_current and t.archived_at_utc is null
          and t.contract_id=(v_unit->>'contract_id')::uuid
          and t.week_ending_date=(v_unit->>'week_ending_date')::date
          and t.sheet_scope='WEEKLY'::public.timesheet_scope_enum
          and tf.candidate_id=(v_unit->>'candidate_id')::uuid
          and tf.client_id=(v_unit->>'client_id')::uuid
          and ((v_unit->>'source_system'='NHSP' and tf.basis='NHSP'::public.timesheet_fin_basis_enum)
            or (v_unit->>'source_system'='HEALTHROSTER' and tf.basis='HEALTHROSTER'::public.timesheet_fin_basis_enum))
          and jsonb_typeof(t.actual_schedule_json)='array' and jsonb_array_length(t.actual_schedule_json)=1
          and (t.actual_schedule_json#>>'{0,start_utc}')::timestamptz=(v_unit#>>'{A_schedule_json,0,start_utc}')::timestamptz
          and (t.actual_schedule_json#>>'{0,end_utc}')::timestamptz=(v_unit#>>'{A_schedule_json,0,end_utc}')::timestamptz
          and coalesce((t.actual_schedule_json#>>'{0,break_mins}')::integer,0)=coalesce((v_unit#>>'{A_schedule_json,0,break_mins}')::integer,0)
          and t.actual_schedule_json @> jsonb_build_array(jsonb_build_object(
            'shift_id',v_unit->>'source_shift_id','external_row_key',v_unit->>'source_identity'))
          and encode(digest(convert_to(coalesce(tf.policy_snapshot_json,'{}'::jsonb)::text,'UTF8'),'sha256'),'hex')
            =v_unit->>'frozen_policy_fingerprint'
          and not coalesce(tf.is_stale,true) and not coalesce(tf.has_rate_issue,false) and not coalesce(tf.has_pay_channel_issue,false)
          and tf.processing_status in ('PENDING_AUTH'::public.ts_fin_processing_status_enum,
            'READY_FOR_HR'::public.ts_fin_processing_status_enum,'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum)
          and tf.hours_day=coalesce((v_unit#>>'{A_hours,hours_day}')::numeric,0)
          and tf.hours_night=coalesce((v_unit#>>'{A_hours,hours_night}')::numeric,0)
          and tf.hours_sat=coalesce((v_unit#>>'{A_hours,hours_sat}')::numeric,0)
          and tf.hours_sun=coalesce((v_unit#>>'{A_hours,hours_sun}')::numeric,0)
          and tf.hours_bh=coalesce((v_unit#>>'{A_hours,hours_bh}')::numeric,0)) then
        raise exception 'IMPORT_REVIEW_RECONCILIATION_TSFIN_NOT_SETTLED' using errcode='55000';
      end if;
    end if;
  end loop;

  if v_action='VALIDATE' then
    if not exists(select 1 from public.audit_events ae where ae.action='IMPORT_REVIEW_RECONCILIATION_VALIDATED'
      and ae.after_json->>'operation_id'=p_operation_id::text
      and ae.after_json->>'request_hash'=v_operation.request_hash
      and ae.after_json->'unit_fingerprints'=v_unit_fingerprints) then
      perform public._audit_insert('import_apply_operations',p_operation_id::text,'IMPORT_REVIEW_RECONCILIATION_VALIDATED',null,
        jsonb_build_object('import_id',p_import_id,'operation_id',p_operation_id,'request_hash',v_operation.request_hash,
          'unit_fingerprints',v_unit_fingerprints),
        'IMPORT_REVIEW',p_actor_user_id);
    end if;
    return jsonb_build_object('ok',true,'action','VALIDATE','unit_count',jsonb_array_length(v_units),'idempotent',false);
  end if;

  select coalesce(array_agg(distinct q.timesheet_id order by q.timesheet_id),array[]::uuid[]) into v_target_ids
  from (
    select t.timesheet_id
    from jsonb_array_elements(v_units) u
    join public.timesheets t on u->>'route' in ('AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT')
      and t.correction_id=u->>'correction_id' and t.is_current and t.archived_at_utc is null
      and t.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
    where coalesce(u->>'intended_authorisation_action','LEAVE_UNAUTHORISED') in ('AUTHORISE','REAUTHORISE')
    union all
    select t.timesheet_id
    from jsonb_array_elements(v_units) u
    join public.timesheets t on u->>'route' in ('AMEND_SOURCE','AMEND_PAID_UNINVOICED_SOURCE')
      and t.timesheet_id=(u->>'source_timesheet_id')::uuid and t.is_current and t.archived_at_utc is null
    where coalesce(u->>'intended_authorisation_action','LEAVE_UNAUTHORISED') in ('AUTHORISE','REAUTHORISE')
  ) q;
  if exists(
    select 1 from unnest(v_target_ids) x(timesheet_id)
    join public.timesheets t on t.timesheet_id=x.timesheet_id
    join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
    join public.contract_weeks cw on cw.timesheet_id=t.timesheet_id
    where (t.authorised_at_server is not null)::integer
        +(tf.authorised_at_utc is not null)::integer
        +(cw.status='AUTHORISED'::public.contract_week_status_enum)::integer not in (0,3)
  ) then
    raise exception 'IMPORT_REVIEW_RECONCILIATION_LIFECYCLE_STATE_INVALID' using errcode='55000';
  end if;
  select coalesce(array_agg(x.timesheet_id order by x.timesheet_id),array[]::uuid[]) into v_pending_target_ids
  from unnest(v_target_ids) x(timesheet_id)
  join public.timesheets t on t.timesheet_id=x.timesheet_id
  join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
  join public.contract_weeks cw on cw.timesheet_id=t.timesheet_id
  where t.authorised_at_server is null and tf.authorised_at_utc is null
    and cw.status<>'AUTHORISED'::public.contract_week_status_enum;
  v_all_authorised:=cardinality(v_target_ids)>0 and cardinality(v_pending_target_ids)=0;
  v_any_authorised:=cardinality(v_target_ids)>cardinality(v_pending_target_ids);
  if cardinality(v_pending_target_ids)>0 then
    v_capability_token:=encode(gen_random_bytes(32),'hex');
    create temporary table if not exists pg_temp.import_review_lifecycle_capability_v1(
      capability_token text not null,txid bigint not null,operation_id uuid not null,request_hash text not null,
      actor_user_id uuid not null,action text not null,action_id text not null,unit_fingerprint text not null,
      timesheet_id uuid not null,expected_timesheet_id uuid not null,expected_version integer,
      expected_row_signature text,expected_tsfin_id uuid,expected_contract_week_id uuid
    ) on commit drop;
    truncate pg_temp.import_review_lifecycle_capability_v1;
    foreach v_id in array v_pending_target_ids loop
      v_signature:=public.timesheet_lifecycle_signature_v1(v_id,null,false);
      insert into pg_temp.import_review_lifecycle_capability_v1
      select v_capability_token,txid_current(),p_operation_id,v_operation.request_hash,p_actor_user_id,'AUTHORISE',
        u->>'action_id',u->>'unit_fingerprint',v_id,v_id,t.version,
        coalesce(v_signature->>'backend_row_signature',v_signature->>'row_signature',v_signature->>'signature'),tf.id,cw.id
      from jsonb_array_elements(v_units) u join public.timesheets t on t.timesheet_id=v_id
        and ((u->>'route' in ('AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT') and t.correction_id=u->>'correction_id')
          or (u->>'route' in ('AMEND_SOURCE','AMEND_PAID_UNINVOICED_SOURCE') and t.timesheet_id=(u->>'source_timesheet_id')::uuid))
      left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
      left join public.contract_weeks cw on cw.timesheet_id=t.timesheet_id;
    end loop;
    perform set_config('cloudtms.import_reconciliation_capability_token',v_capability_token,true);
    perform set_config('cloudtms.import_reconciliation_operation_id',p_operation_id::text,true);
    perform set_config('cloudtms.import_reconciliation_action','AUTHORISE',true);
    select jsonb_agg(jsonb_build_object('timesheet_id',x) order by x) into v_items from unnest(v_pending_target_ids) x;
    v_result:=public.timesheet_authorise_bulk_atomic(v_items,p_actor_user_id,coalesce(p_now_utc,now()));
    if coalesce((v_result->>'ok')::boolean,false) is not true or coalesce((v_result->>'all_success')::boolean,false) is not true then
      raise exception 'IMPORT_REVIEW_RECONCILIATION_AUTHORISE_INCOMPLETE' using errcode='55000',detail=coalesce(v_result->>'error_code','bulk authorise incomplete');
    end if;
    truncate pg_temp.import_review_lifecycle_capability_v1;
    perform set_config('cloudtms.import_reconciliation_capability_token','',true);
    perform set_config('cloudtms.import_reconciliation_action','',true);
  end if;
  if exists(
    select 1 from unnest(v_target_ids) x(timesheet_id)
    left join public.timesheets t on t.timesheet_id=x.timesheet_id and t.is_current and t.archived_at_utc is null
    left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
    left join public.contract_weeks cw on cw.timesheet_id=t.timesheet_id
    where t.timesheet_id is null or tf.id is null or cw.id is null
      or t.authorised_at_server is null or tf.authorised_at_utc is null
      or cw.status<>'AUTHORISED'::public.contract_week_status_enum
  ) then
    raise exception 'IMPORT_REVIEW_RECONCILIATION_AUTHORISE_INCOMPLETE' using errcode='55000';
  end if;
  if not exists(select 1 from public.audit_events ae where ae.action='IMPORT_REVIEW_RECONCILIATION_AUTHORISED'
      and ae.after_json->>'operation_id'=p_operation_id::text and ae.after_json->>'request_hash'=v_operation.request_hash
      and ae.after_json->'unit_fingerprints'=v_unit_fingerprints) then
    perform public._audit_insert('import_apply_operations',p_operation_id::text,'IMPORT_REVIEW_RECONCILIATION_AUTHORISED',null,
      jsonb_build_object('import_id',p_import_id,'operation_id',p_operation_id,'request_hash',v_operation.request_hash,
        'unit_fingerprints',v_unit_fingerprints,'timesheet_ids',to_jsonb(coalesce(v_target_ids,array[]::uuid[]))),
      'IMPORT_REVIEW',p_actor_user_id);
  end if;
  return jsonb_build_object('ok',true,'action','AUTHORISE','unit_count',jsonb_array_length(v_units),
    'timesheet_ids',to_jsonb(coalesce(v_target_ids,array[]::uuid[])),
    'newly_authorised_timesheet_ids',to_jsonb(coalesce(v_pending_target_ids,array[]::uuid[])),
    'idempotent',v_all_authorised,'bulk_result',v_result);
end
$function$;

CREATE OR REPLACE FUNCTION public._import_review_apply_envelope_core_v1(p_import_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_state public.import_review_states%rowtype;
  v_import public.hr_imports%rowtype;
  v_selected_ids text[];
  v_invalidation_ids text[];
  v_correction_units jsonb;
  v_reconciliation_units jsonb;
begin
  select * into v_state from public.import_review_states where import_id=p_import_id;
  select * into v_import from public.hr_imports where id=p_import_id;
  if v_state.import_id is null or v_import.id is null then
    raise exception 'IMPORT_REVIEW_NOT_FOUND' using errcode='P0002';
  end if;
  select coalesce(array_agg(r.action_id order by r.action_id),array[]::text[])
  into v_selected_ids from public._import_review_ready_action_ids_core_v1(p_import_id) r;
  select coalesce(array_agg(d.action_id order by d.action_id),array[]::text[])
  into v_invalidation_ids from public.import_review_decisions d
  where d.import_id=p_import_id and d.is_current and d.action_id=any(v_selected_ids)
    and d.action_kind='INVALIDATE_REFERENCE';
  select coalesce(jsonb_agg(jsonb_build_object(
    'action_id',d.action_id,'root_timesheet_id',d.timesheet_id,'source_row_key',d.source_identity,
    'correction_action',case when d.action_kind='APPLY_AMENDMENT' then 'CHANGED_HOURS' else 'CANCELLATION' end,
    'correction_shape',case when d.action_kind='APPLY_AMENDMENT' then 'REVERSAL_REPLACEMENT' else 'REVERSAL_ONLY' end
  ) order by d.action_id),'[]'::jsonb)
  into v_correction_units
  from public.import_review_decisions d
  cross join lateral (
    select public._import_review_timesheet_protection_core_v1(d.timesheet_id) as protection
  ) pr
  where d.import_id=p_import_id and d.is_current and d.action_id=any(v_selected_ids)
    and d.action_kind in ('APPLY_AMENDMENT','APPLY_CANCELLATION') and d.timesheet_id is not null
    and (coalesce((pr.protection->>'paid')::boolean,false)
      or coalesce((pr.protection->>'invoice_locked')::boolean,false))
    and coalesce((select a.import_authoritative
      from public._import_review_effective_authority_core_v1(
        case when v_import.source_system='NHSP'::public.hr_source_enum then 'NHSP' else 'HR_WEEKLY' end,
        d.contract_id,d.client_id,coalesce(d.summary_json->>'work_date',d.summary_json->>'week_ending_date')::date) a),false);
  select coalesce(jsonb_agg(unit_json order by action_id),'[]'::jsonb)
  into v_reconciliation_units
  from (
    select d.action_id,
      jsonb_build_object(
        'schema_version','IMPORT_AUTHORITATIVE_RECONCILIATION_V1',
        'action_id',d.action_id,'source_identity',d.source_identity,
        'source_system',case when v_import.source_system='NHSP'::public.hr_source_enum then 'NHSP' else 'HEALTHROSTER' end,
        'source_shift_id',d.summary_json->>'existing_shift_id',
        'hr_row_id',d.hr_row_id,
        'authoritative_import_id',p_import_id,
        'source_timesheet_id',d.timesheet_id,
        'candidate_id',d.candidate_id,'client_id',d.client_id,'contract_id',d.contract_id,
        'week_ending_date',d.summary_json->>'week_ending_date',
        'invoice_stream',d.summary_json->>'invoice_stream',
        'source_scope_fingerprint',d.summary_json->>'source_scope_fingerprint',
        'route',d.summary_json->>'amendment_route',
        'reconciliation_mode',d.summary_json->>'reconciliation_mode',
        'B_effective_invoice_ids',coalesce(d.summary_json->'effective_invoice_ids','[]'::jsonb),
        'B_effective_invoice_line_ids',coalesce(d.summary_json->'effective_invoice_line_ids','[]'::jsonb),
        'B_hours',d.summary_json->'B_hours','B_financials',d.summary_json->'B_financials',
        'B_standard_schedule_json',coalesce(d.summary_json->'B_standard_schedule_json','[]'::jsonb),
        'B_invoice_fingerprint',d.summary_json->>'effective_invoice_fingerprint',
        'M_active_member_ids',coalesce(d.summary_json->'active_mutable_member_ids','[]'::jsonb),
        'M_missing_roles',coalesce(d.summary_json->'missing_mutable_roles','[]'::jsonb),
        'M_hours',d.summary_json->'M_hours','M_fingerprint',d.summary_json->>'mutable_generation_fingerprint',
        'A_schedule_json',d.summary_json->'A_schedule_json','A_hours',d.summary_json->'A_hours',
        'A_evidence_fingerprint',d.summary_json->>'authoritative_evidence_fingerprint',
        'archived_timesheet_ids',coalesce(d.summary_json->'archived_timesheet_ids','[]'::jsonb),
        'historical_missing_timesheet_ids',coalesce(d.summary_json->'historical_missing_timesheet_ids','[]'::jsonb),
        'correction_id',d.summary_json->>'correction_id',
        'expected_roles',case when d.summary_json->>'amendment_route' in ('AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT')
          then jsonb_build_array('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT') else '[]'::jsonb end,
        'parent_timesheet_id',coalesce(
          nullif(d.summary_json->>'active_mutable_parent_timesheet_id','')::uuid,
          d.timesheet_id
        ),
        'frozen_policy_fingerprint',coalesce(d.summary_json->>'B_policy_fingerprint',d.summary_json->>'authoritative_evidence_fingerprint'),
        'intended_authorisation_action',d.summary_json->>'intended_authorisation_action',
        'financial_validation_mode',d.summary_json->>'financial_validation_mode',
        'reconciliation_fingerprint',d.summary_json->>'reconciliation_fingerprint',
        'unit_fingerprint',public._import_review_hash_v1(concat_ws('|','unit-v1',d.action_id,d.source_identity,
          d.summary_json->>'amendment_route',d.summary_json->>'reconciliation_fingerprint',d.evidence_fingerprint))
      ) unit_json
    from public.import_review_decisions d
    where d.import_id=p_import_id and d.is_current and d.selected and d.selectable
      and d.action_id=any(v_selected_ids) and d.action_kind='APPLY_AMENDMENT'
      and upper(coalesce(d.summary_json->>'authority_mode',''))='AUTHORITATIVE'
      and coalesce((d.summary_json->>'is_daily')::boolean,false)=false
  ) frozen;
  if exists (
    select 1 from jsonb_array_elements(v_reconciliation_units) u
    where nullif(u->>'action_id','') is null or nullif(u->>'source_identity','') is null
      or nullif(u->>'route','') is null or nullif(u->>'reconciliation_fingerprint','') is null
      or jsonb_typeof(u->'A_schedule_json')<>'array' or jsonb_typeof(u->'A_hours')<>'object'
  ) then
    raise exception 'IMPORT_REVIEW_RECONCILIATION_UNIT_INVALID' using errcode='22023';
  end if;
  return jsonb_build_object(
    'schema_version','IMPORT_REVIEW_APPLY_V1','import_id',p_import_id,
    'selected_action_ids',to_jsonb(v_selected_ids),'coverage_fingerprint',v_import.coverage_fingerprint,
    'preview_fingerprint',v_state.preview_fingerprint,
    'reference_invalidation_action_ids',to_jsonb(v_invalidation_ids),
    'correction_units',v_correction_units,
    'reconciliation_units',v_reconciliation_units,
    'batch_scope_units',coalesce((select jsonb_agg(jsonb_build_object(
      'candidate_id',u.candidate_id,'client_id',u.client_id) order by u.candidate_id,u.client_id)
      from (select distinct d.candidate_id,d.client_id from public.import_review_decisions d
        where d.import_id=p_import_id and d.action_id=any(v_selected_ids)) u),'[]'::jsonb),
    'deferred_action_count',(select count(*) from public.import_review_decisions d
      where d.import_id=p_import_id and d.is_current and d.selectable and not d.selected));
end
$function$;

CREATE OR REPLACE FUNCTION public._import_review_action_catalog_core_v1(p_import_id uuid, p_preview_generation integer, p_max_actions integer DEFAULT 5000)
 RETURNS TABLE(action_id text, action_kind text, action_category text, target_key text, source_identity text, hr_row_id uuid, timesheet_id uuid, shift_id uuid, client_id uuid, candidate_id uuid, contract_id uuid, issue_id uuid, evidence_fingerprint text, selectable boolean, default_selected boolean, blocking boolean, summary_json jsonb)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare v_count integer; v_weekly_preview jsonb;
begin
  if p_import_id is null or p_preview_generation<1 or p_max_actions<1 or p_max_actions>5000 then
    raise exception 'IMPORT_REVIEW_ACTION_CATALOG_INPUT_INVALID' using errcode='22023';
  end if;

  create temporary table if not exists pg_temp.import_review_catalog_v1 (
    action_id text, action_kind text, action_category text, target_key text, source_identity text,
    hr_row_id uuid, timesheet_id uuid, shift_id uuid, client_id uuid, candidate_id uuid,
    contract_id uuid, issue_id uuid, evidence_fingerprint text, selectable boolean,
    default_selected boolean, blocking boolean, summary_json jsonb
  ) on commit drop;
  truncate pg_temp.import_review_catalog_v1;

  insert into pg_temp.import_review_catalog_v1
  with import_row as (
    select hi.* from public.hr_imports hi where hi.id=p_import_id
  ), raw as (
    select r.*, i.source_system::text as source_system, upper(coalesce(i.import_scope,'')) as import_scope,
      i.client_id as import_client_id,
      coalesce(nullif(r.staff_raw,''),nullif(r.payload_json->>'staff_name',''),nullif(r.staff_norm,'')) as staff_label,
      nullif(regexp_replace(lower(coalesce(nullif(r.staff_raw,''),r.payload_json->>'staff_name',r.staff_norm,'')),'[^a-z0-9]+','','g'),'') as staff_key,
      coalesce(nullif(r.payload_json->>'trust',''),nullif(r.payload_json->>'hospital_or_trust',''),nullif(r.unit_raw,''),nullif(r.unit_hint,'')) as client_label,
      nullif(regexp_replace(lower(coalesce(nullif(r.payload_json->>'trust',''),nullif(r.payload_json->>'hospital_or_trust',''),r.unit_raw,r.unit_hint,'')),'[^a-z0-9]+','','g'),'') as client_key,
      lower(btrim(coalesce(nullif(r.assignment_grade_norm,''),r.payload_json->>'grade_raw',r.payload_json->>'Request_Grade',''))) as grade_key,
      coalesce(nullif(r.external_row_key,''),'hr-row:'||r.id::text) as source_row_key
    from public.hr_rows r join import_row i on true where r.import_id=p_import_id
    order by r.id limit 501
  ), mapped as (
    select raw.*,
      coalesce(c_alias.id,c_map.candidate_id,c_exact.candidate_id) as resolved_candidate_id,
      coalesce(raw.import_client_id,ch.client_id,c_client.client_id) as resolved_client_id
    from raw
    left join lateral (
      select c.id from public.candidates c
      where c.nhsp_hr_name_aliases is not null and raw.staff_key is not null
        and c.nhsp_hr_name_aliases @> to_jsonb(array[raw.staff_key]::text[])
      order by c.id limit 1
    ) c_alias on true
    left join lateral (
      select hm.candidate_id from public.hr_name_mappings hm
      where hm.active and hm.hr_name_norm in (lower(btrim(coalesce(raw.staff_label,''))),raw.staff_key)
      order by hm.created_at desc,hm.id limit 1
    ) c_map on c_alias.id is null
    left join lateral (
      select case when count(*)=1 then (array_agg(c.id order by c.id))[1] end as candidate_id
      from public.candidates c where c.active and raw.staff_key is not null
        and (regexp_replace(lower(coalesce(c.first_name,'')||coalesce(c.last_name,'')),'[^a-z0-9]+','','g')=raw.staff_key
          or regexp_replace(lower(coalesce(c.last_name,'')||coalesce(c.first_name,'')),'[^a-z0-9]+','','g')=raw.staff_key)
    ) c_exact on c_alias.id is null and c_map.candidate_id is null
    left join lateral (
      select ch.client_id from public.client_hospitals ch
      where raw.client_key is not null and ch.hospital_name_norm @> to_jsonb(array[raw.client_key]::text[])
      order by ch.id limit 1
    ) ch on raw.import_client_id is null
    left join lateral (
      select case when count(*)=1 then (array_agg(c.id order by c.id))[1] end as client_id
      from public.clients c where raw.client_key is not null
        and regexp_replace(lower(coalesce(c.name,'')),'[^a-z0-9]+','','g')=raw.client_key
    ) c_client on raw.import_client_id is null and ch.client_id is null
  ), weekly_phase as materialized (
    -- weekly_import_phase2 remains the single authority for assignment-code
    -- mapping precedence and contract choice.  The review catalogue consumes
    -- its answer rather than maintaining a second resolver.
    select w.*
    from import_row i
    cross join lateral public.weekly_import_phase2(
      p_import_id,
      case when i.source_system='NHSP'::public.hr_source_enum then 'NHSP' else 'HR_WEEKLY' end
    ) w
    where not (upper(i.source_system::text)='HEALTHROSTER_DAILY'
      or upper(coalesce(i.import_scope,'')) like '%DAILY%')
  ), classified as (
    select m.*,
      case when upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%'
        then case when rtsx.contract_id is not null then 1 else 0 end
        else con.contract_count end as contract_count,
      case when wp.hr_row_id is not null then wp.contract_id
        when upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%'
          then rtsx.contract_id
        else con.contract_id end as resolved_contract_id,
      wp.action as weekly_resolution_action,wp.reason as weekly_resolution_reason,
      wp.incoming_code as weekly_incoming_code,
      wp.week_ending_date as resolved_week_ending_date,
      wm.has_weekly_mapping,wm.mapping_evidence as weekly_mapping_evidence,
      dgm.mapping_count as daily_mapping_count,dgm.mapping_id as daily_mapping_id,
      dgm.role_code as daily_mapped_role,dgm.band_norm as daily_mapped_band,
      dgm.updated_at as daily_mapping_updated_at,(coalesce(dgm.mapping_count,0)=1) as has_grade_mapping,
      tsx.timesheet_count,tsx.timesheet_ids,tsx.auto_timesheet_id,tsx.timesheet_evidence_hash,
      dtsx.submitted_timesheet_count as daily_submitted_timesheet_count,
      dtsx.submitted_timesheet_evidence_hash as daily_submitted_timesheet_evidence_hash,
      tsx.timesheet_contract_ids,dcon.contract_ids as eligible_contract_ids,dcon.contract_evidence_hash,
      cr.route_eligible as contract_route_eligible,cr.rate_complete as contract_rate_complete,
      cr.import_authoritative,cr.authority_mode,cr.authority_fingerprint,
      cr.rate_evidence as contract_rate_evidence,
      wopts.options as weekly_contract_options,dopts.options as daily_role_options,
      res.resolved_timesheet_id as stored_timesheet_id,res.status as resolution_status,
      coalesce(case when res.resolved_timesheet_id=any(coalesce(tsx.timesheet_ids,array[]::uuid[])) then res.resolved_timesheet_id end,
        tsx.auto_timesheet_id) as resolved_timesheet_id,
      nss.id as existing_shift_id,nss.timesheet_id as existing_shift_timesheet_id,
      nss.start_utc as existing_shift_start_utc,nss.end_utc as existing_shift_end_utc,
      nss.break_mins as existing_shift_break_minutes,nss.pay_minutes as existing_shift_paid_minutes,
      nss.assignment_code as existing_shift_role,
      case when upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%' then true else false end as is_daily
    from mapped m
    left join weekly_phase wp on wp.hr_row_id=m.id
    left join lateral (
      select count(*)::integer contract_count,
             case when count(*)=1 then (array_agg(c.id order by c.id))[1] end contract_id
      from public.contracts c
      where c.candidate_id=m.resolved_candidate_id and c.client_id=m.resolved_client_id
        and c.start_date<=m.date_local and (c.end_date is null or c.end_date>=m.date_local)
    ) con on true
    left join lateral (
      select count(*)::integer mapping_count,
        (array_agg(gm.id order by gm.updated_at desc,gm.id))[1] mapping_id,
        (array_agg(gm.role_code order by gm.updated_at desc,gm.id))[1] role_code,
        (array_agg(gm.band_norm order by gm.updated_at desc,gm.id))[1] band_norm,
        (array_agg(gm.updated_at order by gm.updated_at desc,gm.id))[1] updated_at
      from public.hr_daily_grade_role_mappings gm
      where gm.client_id=m.resolved_client_id and gm.incoming_grade_norm=m.grade_key and gm.active
    ) dgm on upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%'
    left join lateral (
      select count(*)::integer contract_count,
        case when count(*)=1 then (array_agg(c.id order by c.id))[1] end contract_id,
        array_agg(c.id order by c.id) contract_ids,
        public._import_review_hash_v1(coalesce(string_agg(concat_ws('|',c.id,c.updated_at,c.role,c.band,a.authority_fingerprint),',' order by c.id),'')) contract_evidence_hash
      from public.contracts c
      cross join lateral public._import_review_effective_authority_core_v1('HR_DAILY',c.id,c.client_id,m.date_local) a
      where c.candidate_id=m.resolved_candidate_id and c.client_id=m.resolved_client_id
        and c.start_date<=m.date_local and (c.end_date is null or c.end_date>=m.date_local)
        and coalesce(dgm.mapping_count,0)=1 and a.route_eligible
        and lower(btrim(coalesce(c.role,'')))=lower(btrim(coalesce(dgm.role_code,'')))
        and (nullif(btrim(coalesce(dgm.band_norm,'')),'') is null
          or lower(btrim(coalesce(c.band,'')))=lower(btrim(dgm.band_norm)))
    ) dcon on upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%'
    left join lateral (
      select count(*)::integer submitted_timesheet_count,
        public._import_review_hash_v1(coalesce(string_agg(concat_ws('|',t.timesheet_id,t.worked_start_iso,
          t.worked_end_iso,t.break_minutes,t.worked_minutes,t.reference_number,t.processing_status,
          t.tsfin_role,t.tsfin_band,ts.contract_id,ts.updated_at),',' order by t.timesheet_id),''))
          submitted_timesheet_evidence_hash
      from public.v_timesheets_daily_match t
      join public.timesheets ts on ts.timesheet_id=t.timesheet_id and ts.is_current and ts.revoked_at is null
      where t.candidate_id=m.resolved_candidate_id and t.client_id=m.resolved_client_id
        and t.sheet_scope::text='DAILY'
        and (t.worked_start_iso at time zone 'Europe/London')::date=m.date_local
    ) dtsx on upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%'
    left join lateral (
      with candidates as (
        select abm.*,
          case when abm.candidate_id=m.resolved_candidate_id and abm.client_id=m.resolved_client_id then 3
            when abm.candidate_id=m.resolved_candidate_id and abm.client_id is null then 2
            when abm.candidate_id is null and abm.client_id=m.resolved_client_id then 1 else 0 end specificity
        from public.assignment_band_mappings abm
        where abm.active and upper(btrim(abm.system_type))=
          case when upper(m.source_system)='NHSP' then 'NHSP' else 'HR_WEEKLY' end
          and lower(btrim(abm.incoming_code))=m.grade_key
          and ((abm.candidate_id=m.resolved_candidate_id and abm.client_id=m.resolved_client_id)
            or (abm.candidate_id=m.resolved_candidate_id and abm.client_id is null)
            or (abm.candidate_id is null and abm.client_id=m.resolved_client_id)
            or (abm.candidate_id is null and abm.client_id is null))
      ), chosen as (select * from candidates where specificity=(select max(specificity) from candidates))
      select exists(select 1 from chosen) has_weekly_mapping,
        public._import_review_hash_v1(coalesce((select string_agg(concat_ws('|',id,updated_at,target_contract_id,band_match_pattern),',' order by id)
          from chosen),'')) mapping_evidence
    ) wm on not (upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%')
    left join lateral (
      select count(*)::integer timesheet_count,
             array_agg(t.timesheet_id order by t.worked_start_iso,t.timesheet_id) timesheet_ids,
             array_agg(ts.contract_id order by t.worked_start_iso,t.timesheet_id) timesheet_contract_ids,
             case when count(*)=1 then (array_agg(t.timesheet_id order by t.timesheet_id))[1] end auto_timesheet_id,
             public._import_review_hash_v1(coalesce(string_agg(concat_ws('|',t.timesheet_id,t.worked_start_iso,t.worked_end_iso,
               t.break_minutes,t.worked_minutes,t.reference_number,t.processing_status,t.tsfin_role,t.tsfin_band,
               ts.contract_id,ts.updated_at),',' order by t.timesheet_id),'')) timesheet_evidence_hash
      from public.v_timesheets_daily_match t
      join public.timesheets ts on ts.timesheet_id=t.timesheet_id and ts.is_current and ts.revoked_at is null
      where t.candidate_id=m.resolved_candidate_id and t.client_id=m.resolved_client_id
        and t.sheet_scope::text='DAILY'
        and (t.worked_start_iso at time zone 'Europe/London')::date=m.date_local
        and coalesce(dgm.mapping_count,0)=1
        and lower(btrim(coalesce(t.tsfin_role,'')))=lower(btrim(coalesce(dgm.role_code,'')))
        and (nullif(btrim(coalesce(dgm.band_norm,'')),'') is null
          or lower(btrim(coalesce(t.tsfin_band,'')))=lower(btrim(dgm.band_norm)))
    ) tsx on upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%'
    left join public.import_review_daily_timesheet_resolutions res
      on res.import_id=p_import_id and res.hr_row_id=m.id and res.status in ('CURRENT','APPLIED')
    left join lateral (
      select ts.contract_id
      from public.timesheets ts
      where ts.timesheet_id=coalesce(
        case when res.resolved_timesheet_id=any(coalesce(tsx.timesheet_ids,array[]::uuid[])) then res.resolved_timesheet_id end,
        tsx.auto_timesheet_id)
        and ts.is_current and ts.revoked_at is null
      order by ts.updated_at desc limit 1
    ) rtsx on upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%'
    left join lateral (
      select a.route_eligible,a.import_authoritative,a.authority_mode,a.authority_fingerprint,
        (jsonb_typeof(c.rates_json)='object'
          and upper(coalesce(c.pay_method_snapshot,'')) in ('PAYE','UMBRELLA')
          and case when upper(c.pay_method_snapshot)='PAYE' then
            (c.rates_json->>'paye_day')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'paye_night')~'^-?[0-9]+([.][0-9]+)?$'
            and (c.rates_json->>'paye_sat')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'paye_sun')~'^-?[0-9]+([.][0-9]+)?$'
            and (c.rates_json->>'paye_bh')~'^-?[0-9]+([.][0-9]+)?$'
          else
            (c.rates_json->>'umb_day')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'umb_night')~'^-?[0-9]+([.][0-9]+)?$'
            and (c.rates_json->>'umb_sat')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'umb_sun')~'^-?[0-9]+([.][0-9]+)?$'
            and (c.rates_json->>'umb_bh')~'^-?[0-9]+([.][0-9]+)?$' end
          and (c.rates_json->>'charge_day')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'charge_night')~'^-?[0-9]+([.][0-9]+)?$'
          and (c.rates_json->>'charge_sat')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'charge_sun')~'^-?[0-9]+([.][0-9]+)?$'
          and (c.rates_json->>'charge_bh')~'^-?[0-9]+([.][0-9]+)?$') rate_complete,
        public._import_review_hash_v1(concat_ws('|',c.id,c.updated_at,c.start_date,c.end_date,c.role,c.band,
          c.pay_method_snapshot,c.rates_json,c.overrideclientsettings,c.is_nhsp,c.autoprocess_hr,c.requires_hr,
          c.no_timesheet_required,a.client_settings_id,a.client_settings_updated_at,
          a.effective_is_nhsp,a.effective_autoprocess_hr,a.effective_requires_hr,
          a.effective_no_timesheet_required,a.authority_fingerprint)) rate_evidence
      from public.contracts c
      cross join lateral public._import_review_effective_authority_core_v1(
        case when upper(m.source_system)='NHSP' then 'NHSP'
          when upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%' then 'HR_DAILY'
          else 'HR_WEEKLY' end,c.id,c.client_id,coalesce(wp.week_ending_date,m.date_local)) a
      where c.id=case when wp.hr_row_id is not null then wp.contract_id
        when upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%'
          then rtsx.contract_id
        else con.contract_id end
    ) cr on true
    left join lateral (
      select coalesce(jsonb_agg(jsonb_build_object(
        'option_id','contract:'||o.id::text,'contract_id',o.id,'candidate_id',o.candidate_id,'client_id',o.client_id,
        'role',o.role,'band',o.band,'site',o.display_site,'start_date',o.start_date,'end_date',o.end_date,
        'source_route_eligible',coalesce(o.route_eligible,false),'rate_complete',coalesce(o.rate_complete,false),
        'authority_mode',o.authority_mode,
        -- Choosing a contract records the server-approved assignment mapping;
        -- it does not apply the import or grant financial authority.  An
        -- authoritative contract with incomplete rates must therefore remain
        -- selectable here and will still be blocked by the refreshed action
        -- catalogue before final application.
        'selectable',coalesce(o.route_eligible,false),
        'disabled_reason_code',case when not coalesce(o.route_eligible,false) then 'CONTRACT_NOT_ELIGIBLE' end,
        'display_label',concat_ws(' · ',nullif(o.role,''),nullif(o.band,''),nullif(o.display_site,''),
          to_char(o.start_date,'DD Mon YYYY')||' to '||coalesce(to_char(o.end_date,'DD Mon YYYY'),'open ended'))
      ) order by lower(coalesce(o.role,'')),lower(coalesce(o.band,'')),o.start_date desc,o.id),'[]'::jsonb) options
      from (
        select c.*,a.route_eligible,a.import_authoritative,a.authority_mode,
          (jsonb_typeof(c.rates_json)='object'
          and upper(coalesce(c.pay_method_snapshot,'')) in ('PAYE','UMBRELLA')
          and case when upper(c.pay_method_snapshot)='PAYE' then
            (c.rates_json->>'paye_day')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'paye_night')~'^-?[0-9]+([.][0-9]+)?$'
            and (c.rates_json->>'paye_sat')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'paye_sun')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'paye_bh')~'^-?[0-9]+([.][0-9]+)?$'
          else (c.rates_json->>'umb_day')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'umb_night')~'^-?[0-9]+([.][0-9]+)?$'
            and (c.rates_json->>'umb_sat')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'umb_sun')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'umb_bh')~'^-?[0-9]+([.][0-9]+)?$' end
          and (c.rates_json->>'charge_day')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'charge_night')~'^-?[0-9]+([.][0-9]+)?$'
          and (c.rates_json->>'charge_sat')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'charge_sun')~'^-?[0-9]+([.][0-9]+)?$' and (c.rates_json->>'charge_bh')~'^-?[0-9]+([.][0-9]+)?$') rate_complete
        from public.contracts c
        cross join lateral public._import_review_effective_authority_core_v1(
          case when upper(m.source_system)='NHSP' then 'NHSP' else 'HR_WEEKLY' end,
          c.id,c.client_id,coalesce(wp.week_ending_date,m.date_local)) a
        where c.candidate_id=m.resolved_candidate_id and c.client_id=m.resolved_client_id
          and c.start_date<=m.date_local and (c.end_date is null or c.end_date>=m.date_local)
        order by c.start_date desc,c.id limit 25
      ) o
    ) wopts on not (upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%')
    left join lateral (
      select coalesce(jsonb_agg(jsonb_build_object(
        'option_id','daily-role:'||public._import_review_hash_v1(lower(concat_ws('|',o.role,o.band))),
        'role_code',o.role,'band_norm',o.band,'selectable',true,
        'display_label',concat_ws(' · ',nullif(o.role,''),coalesce(nullif(o.band,''),'No band'))
      ) order by lower(o.role),lower(coalesce(o.band,''))),'[]'::jsonb) options
      from (
        select distinct t.tsfin_role role,t.tsfin_band band
        from public.v_timesheets_daily_match t
        where t.candidate_id=m.resolved_candidate_id
          and t.client_id=m.resolved_client_id
          and t.sheet_scope::text='DAILY'
          and (t.worked_start_iso at time zone 'Europe/London')::date=m.date_local
          and nullif(btrim(t.tsfin_role),'') is not null
        order by t.tsfin_role,t.tsfin_band
        limit 25
      ) o
    ) dopts on upper(m.source_system)='HEALTHROSTER_DAILY' or m.import_scope like '%DAILY%'
    left join public.nhsp_shifts nss
      on nss.external_row_key=m.source_row_key and nss.source_system::text=m.source_system
      and nss.cancelled_at_utc is null
  ), facts as (
    select c.*,
      ts.worked_start_iso,ts.worked_end_iso,ts.break_minutes as ts_break_minutes,ts.worked_minutes,
      ts.reference_number,ts.processing_status::text,ts.tsfin_role,ts.tsfin_band,
      coalesce(c.existing_shift_timesheet_id,base_week.timesheet_id) as authoritative_target_timesheet_id,
      public._import_review_timesheet_has_calculated_expenses_core_v1(
        coalesce(c.existing_shift_timesheet_id,base_week.timesheet_id)
      ) as authoritative_timesheet_has_calculated_expenses,
      mutable_replacement.timesheet_id as mutable_replacement_timesheet_id,
      mutable_replacement.protection as mutable_replacement_protection,
      source_timesheet.authorised_at_server as source_authorised_at_server,
      source_tf.authorised_at_utc as source_tsfin_authorised_at_utc,
      source_tf.policy_snapshot_json as source_policy_snapshot_json,
      source_tf.basis::text as source_tsfin_basis,
      authoritative_hours.hours_day as authoritative_hours_day,
      authoritative_hours.hours_night as authoritative_hours_night,
      authoritative_hours.hours_sat as authoritative_hours_sat,
      authoritative_hours.hours_sun as authoritative_hours_sun,
      authoritative_hours.hours_bh as authoritative_hours_bh,
      authoritative_hours.total_hours as authoritative_total_hours,
      coalesce((auto_authorise.value->>'effective_value')::boolean,false) as effective_auto_authorise,
      public._import_review_timesheet_protection_core_v1(coalesce(
        c.resolved_timesheet_id,c.existing_shift_timesheet_id,base_week.timesheet_id
      )) as protection
    from classified c
    left join public.v_timesheets_daily_match ts on ts.timesheet_id=c.resolved_timesheet_id
    left join lateral (
      select cw.timesheet_id
      from public.contract_weeks cw
      where not c.is_daily
        and coalesce(c.import_authoritative,false)
        and cw.contract_id=c.resolved_contract_id
        and cw.week_ending_date=coalesce(
          c.resolved_week_ending_date,
          c.date_local + ((7-extract(dow from c.date_local)::integer)%7)
        )
        and cw.is_adjustment=false
        and coalesce(cw.additional_seq,0)=0
      order by cw.id
      limit 1
    ) base_week on true
    left join public.timesheets source_timesheet
      on source_timesheet.timesheet_id=coalesce(c.existing_shift_timesheet_id,base_week.timesheet_id)
    left join public.timesheets_financials source_tf
      on source_tf.timesheet_id=source_timesheet.timesheet_id and source_tf.is_current=true
    left join lateral public._wkimp_bucket_hours_from_policy(
      coalesce(source_tf.policy_snapshot_json,'{}'::jsonb),
      (c.payload_json->>'start_utc')::timestamptz,
      (c.payload_json->>'end_utc')::timestamptz,
      coalesce((c.payload_json->>'actual_break_mins')::integer,
        (c.payload_json->>'actual_break_minutes')::integer,
        (c.payload_json->>'break_mins')::integer,
        (c.payload_json->>'break_minutes')::integer,0)
    ) authoritative_hours on not c.is_daily and coalesce(c.import_authoritative,false)
      and c.existing_shift_id is not null
    left join lateral (
      select case
        when not c.is_daily
          and coalesce(c.import_authoritative,false)
          and c.resolved_client_id is not null
          and c.resolved_contract_id is not null
        then public.import_auto_authorise_policy_resolve_v1(
          case when upper(c.source_system)='NHSP' then 'NHSP'::public.hr_source_enum else 'HEALTHROSTER'::public.hr_source_enum end,
          c.resolved_client_id,c.resolved_contract_id,false
        )
        else null::jsonb
      end as value
    ) auto_authorise on true
    left join lateral (
      select replacement_candidate.timesheet_id,replacement_candidate.protection
      from (
        select
          replacement_timesheet.timesheet_id,
          replacement_timesheet.updated_at,
          replacement_timesheet.created_at,
          public._import_review_timesheet_protection_core_v1(
            replacement_timesheet.timesheet_id
          ) as protection
        from public.timesheets replacement_timesheet
        where not c.is_daily
          and c.existing_shift_id is not null
          and replacement_timesheet.is_adjustment is true
          and replacement_timesheet.is_current is true
          and replacement_timesheet.correction_kind='CHANGED_HOURS_REPLACEMENT'
          and jsonb_typeof(replacement_timesheet.actual_schedule_json)='array'
          and replacement_timesheet.actual_schedule_json @> jsonb_build_array(
            jsonb_build_object(
              'shift_id',c.existing_shift_id::text,
              'external_row_key',c.source_row_key
            )
          )
      ) replacement_candidate
      where coalesce(
          (replacement_candidate.protection->>'paid')::boolean,
          false
        ) is false
        and coalesce(
          (replacement_candidate.protection->>'invoice_locked')::boolean,
          false
        ) is false
      order by
        replacement_candidate.updated_at desc nulls last,
        replacement_candidate.created_at desc nulls last
      limit 1
    ) mutable_replacement on true
  ), reconciliation_source_rows as (
    select
      f.*,
      ((row_number() over (order by f.source_row_key) - 1) / 100)::integer as reconciliation_batch
    from facts f
    where not f.is_daily and coalesce(f.import_authoritative,false) and f.existing_shift_id is not null
      and (
        (f.payload_json->>'start_utc')::timestamptz is distinct from f.existing_shift_start_utc
        or (f.payload_json->>'end_utc')::timestamptz is distinct from f.existing_shift_end_utc
        or ((f.payload_json->>'break_mins') is not null and (f.payload_json->>'break_mins')::integer is distinct from coalesce(f.existing_shift_break_minutes,0))
      )
  ), reconciliation_inputs as (
    select coalesce(jsonb_agg(jsonb_build_object(
      'source_identity',f.source_row_key,
      'source_system',case when upper(f.source_system)='NHSP' then 'NHSP' else 'HEALTHROSTER' end,
      'source_shift_id',f.existing_shift_id,
      'external_row_key',f.source_row_key,
      'hr_row_id',f.id,
      'source_timesheet_id',coalesce(f.existing_shift_timesheet_id,f.authoritative_target_timesheet_id),
      'candidate_id',f.resolved_candidate_id,'client_id',f.resolved_client_id,'contract_id',f.resolved_contract_id,
      'week_ending_date',coalesce(f.resolved_week_ending_date,f.date_local+((7-extract(dow from f.date_local)::integer)%7)),
      'invoice_stream',case when upper(coalesce(f.source_tsfin_basis,'')) in ('NHSP','NHSP_ADJUSTMENT','HEALTHROSTER_SELF_BILL','HEALTHROSTER_ADJUSTMENT') then 'SELF_BILL' else 'NORMAL' end,
      'authoritative_import_id',p_import_id,
      'authoritative_schedule_json',jsonb_build_array(jsonb_strip_nulls(jsonb_build_object(
        'date',f.date_local,'start_utc',f.payload_json->>'start_utc','end_utc',f.payload_json->>'end_utc',
        'break_mins',coalesce((f.payload_json->>'actual_break_mins')::integer,(f.payload_json->>'actual_break_minutes')::integer,
          (f.payload_json->>'break_mins')::integer,(f.payload_json->>'break_minutes')::integer,0),
        'shift_id',f.existing_shift_id,'external_row_key',f.source_row_key,'import_id',p_import_id,
        'ref_num',coalesce(f.hr_request_id,f.payload_json->>'ref_num',f.payload_json->>'reference_number')
      ))),
      'authoritative_hours',jsonb_build_object(
        'hours_day',coalesce(f.authoritative_hours_day,0),'hours_night',coalesce(f.authoritative_hours_night,0),
        'hours_sat',coalesce(f.authoritative_hours_sat,0),'hours_sun',coalesce(f.authoritative_hours_sun,0),
        'hours_bh',coalesce(f.authoritative_hours_bh,0),'total_hours',coalesce(f.authoritative_total_hours,f.hours_worked,0)
      )
    ) order by f.source_row_key),'[]'::jsonb) items
    from reconciliation_source_rows f
    group by f.reconciliation_batch
  ), reconciliation_balances as materialized (
    select b.source_identity,b.balance_json
    from reconciliation_inputs i
    cross join lateral public._import_review_effective_invoice_balance_core_v1(
      p_import_id,i.items,100,512,256,128
    ) b
  ), evidenced as (
    select c.*,
      rb.balance_json as reconciliation_balance,
      public._import_review_hash_v1(concat_ws('|','row-evidence-v1',c.source_row_key,c.staff_key,c.client_key,c.date_local,
        c.start_time_local,c.end_time_local,c.hours_worked,c.hr_request_id,c.resolved_candidate_id,c.resolved_client_id,
        c.resolved_contract_id,c.weekly_resolution_action,c.weekly_incoming_code,c.weekly_mapping_evidence,c.contract_rate_evidence,
        c.daily_mapping_id,c.daily_mapping_updated_at,c.daily_mapped_role,c.daily_mapped_band,
        c.timesheet_evidence_hash,c.daily_submitted_timesheet_evidence_hash,c.contract_evidence_hash,c.authority_fingerprint,
        c.authoritative_target_timesheet_id,c.authoritative_timesheet_has_calculated_expenses,
        c.mutable_replacement_timesheet_id,coalesce(c.mutable_replacement_protection::text,''),
        coalesce(c.eligible_contract_ids::text,''),coalesce(c.timesheet_ids::text,''),
        coalesce(c.timesheet_contract_ids::text,''),c.protection::text,coalesce(rb.balance_json::text,''),
        coalesce(c.payload_json::text,''))) as evidence_hash
    from facts c
    left join reconciliation_balances rb on rb.source_identity=c.source_row_key
  ), main_actions as (
    select
      case
        when f.resolved_candidate_id is null then 'ADVISORY'
        when f.resolved_client_id is null then 'ADVISORY'
        when f.is_daily and not coalesce(f.has_grade_mapping,false) then 'ADVISORY'
        when not f.is_daily and coalesce(f.weekly_resolution_action,'')<>'OK' then 'ADVISORY'
        when not f.is_daily and coalesce(f.contract_count,0)=0 then 'ADVISORY'
        when not f.is_daily and not coalesce(f.contract_route_eligible,false) then 'ADVISORY'
        when f.is_daily and coalesce(f.timesheet_count,0)=0 then 'ADVISORY'
        when f.is_daily and f.resolved_timesheet_id is null then 'DAILY_TIMESHEET_RESOLUTION'
        when f.is_daily then 'NO_ACTION'
        when not coalesce(f.import_authoritative,false) then 'NO_ACTION'
        when not coalesce(f.contract_rate_complete,false) then 'ADVISORY'
        when coalesce(f.authoritative_timesheet_has_calculated_expenses,false) then 'ADVISORY'
        when f.existing_shift_id is null then 'INCLUDE_SHIFT'
        when (f.payload_json->>'start_utc')::timestamptz is distinct from (select n.start_utc from public.nhsp_shifts n where n.id=f.existing_shift_id)
          or (f.payload_json->>'end_utc')::timestamptz is distinct from (select n.end_utc from public.nhsp_shifts n where n.id=f.existing_shift_id)
          or ((f.payload_json->>'break_mins') is not null
            and (f.payload_json->>'break_mins')::integer is distinct from
              coalesce((select n.break_mins from public.nhsp_shifts n where n.id=f.existing_shift_id),0))
          then 'APPLY_AMENDMENT'
        else 'NO_ACTION'
      end action_kind,
      f.*
    from evidenced f
  ), rendered as (
    select
      public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,m.action_kind,m.source_row_key)) action_id,
      m.action_kind,
      case when m.action_kind='ADVISORY'
             or nullif(m.reconciliation_balance->>'blocking_code','') is not null
             or coalesce((m.protection->>'active_pay_draft')::boolean,false) then 'BLOCKED'
           when m.action_kind='DAILY_TIMESHEET_RESOLUTION' then 'PENDING'
           when m.action_kind='NO_ACTION' then 'NO_ACTION' else 'READY' end action_category,
      'hr-row:'||m.id::text target_key,m.source_row_key source_identity,m.id hr_row_id,
      coalesce(m.resolved_timesheet_id,m.existing_shift_timesheet_id) timesheet_id,m.existing_shift_id shift_id,
      m.resolved_client_id client_id,m.resolved_candidate_id candidate_id,m.resolved_contract_id contract_id,
      null::uuid issue_id,m.evidence_hash evidence_fingerprint,
      (m.action_kind in ('INCLUDE_SHIFT','APPLY_AMENDMENT','NO_ACTION')
        and nullif(m.reconciliation_balance->>'blocking_code','') is null
        and not coalesce((m.protection->>'active_pay_draft')::boolean,false)) selectable,
      (m.action_kind in ('INCLUDE_SHIFT','APPLY_AMENDMENT','NO_ACTION')
        and nullif(m.reconciliation_balance->>'blocking_code','') is null
        and not coalesce((m.protection->>'active_pay_draft')::boolean,false)) default_selected,
      (m.action_kind in ('ADVISORY','DAILY_TIMESHEET_RESOLUTION')
        or nullif(m.reconciliation_balance->>'blocking_code','') is not null
        or coalesce((m.protection->>'active_pay_draft')::boolean,false)) blocking,
      jsonb_strip_nulls(jsonb_build_object(
        'reason_code',case
          when m.resolved_candidate_id is null then 'CANDIDATE_UNRESOLVED'
          when m.resolved_client_id is null then 'CLIENT_UNRESOLVED'
          when m.is_daily and not coalesce(m.has_grade_mapping,false) then 'GRADE_MAPPING_REQUIRED'
          when not m.is_daily and m.weekly_resolution_action='REJECT_NO_CONTRACT' then 'CONTRACT_MISSING'
          when not m.is_daily and m.weekly_resolution_action='REJECT_NO_CONTRACT_BAND_MISMATCH'
            and not coalesce(m.has_weekly_mapping,false) then 'GRADE_MAPPING_REQUIRED'
          when not m.is_daily and coalesce(m.weekly_resolution_action,'')<>'OK' then 'CONTRACT_OUT_OF_SCOPE'
          when not m.is_daily and coalesce(m.contract_count,0)=0 then 'CONTRACT_MISSING'
          when not m.is_daily and not coalesce(m.contract_route_eligible,false) then 'CONTRACT_OUT_OF_SCOPE'
          when not m.is_daily and coalesce(m.import_authoritative,false)
            and not coalesce(m.contract_rate_complete,false) then 'CONTRACT_RATES_INCOMPLETE'
          when not m.is_daily and coalesce(m.import_authoritative,false)
            and coalesce(m.authoritative_timesheet_has_calculated_expenses,false)
            then 'TIMESHEET_OCCUPIED_BY_EXPENSES'
          when m.is_daily and coalesce(m.timesheet_count,0)=0
            and coalesce(m.daily_submitted_timesheet_count,0)=0 then 'DAILY_TIMESHEET_NOT_SUBMITTED'
          when m.is_daily and coalesce(m.timesheet_count,0)=0 then 'DAILY_SHIFT_ABSENT_FROM_TIMESHEET'
          when m.is_daily and m.resolved_timesheet_id is null then 'TIMESHEET_AMBIGUOUS'
          when nullif(m.reconciliation_balance->>'blocking_code','') is not null
            then m.reconciliation_balance->>'blocking_code'
          when coalesce((m.protection->>'active_pay_draft')::boolean,false) then 'BLOCKED_ACTIVE_PAY_DRAFT'
          else null end,
        'source_system',m.source_system,'source_route',m.import_scope,'is_daily',m.is_daily,
        'existing_shift_id',m.existing_shift_id,
        'invoice_stream',m.reconciliation_balance->>'invoice_stream',
        'authority_mode',coalesce(m.authority_mode,case when m.is_daily or not coalesce(m.import_authoritative,false)
          then 'VALIDATION_ONLY' else 'AUTHORITATIVE' end),
        'authority_fingerprint',m.authority_fingerprint,
        'amendment_route',case
          when m.action_kind='APPLY_AMENDMENT'
            and coalesce((m.reconciliation_balance->>'active_mutable_generation')::boolean,false)
            then 'AMEND_EXISTING_REPLACEMENT'
          when m.action_kind='APPLY_AMENDMENT'
            and coalesce((m.reconciliation_balance#>>'{B_hours,total_hours}')::numeric,0)>0
            then 'CREATE_REVERSAL_REPLACEMENT'
          when m.action_kind='APPLY_AMENDMENT'
            and coalesce((m.protection->>'paid')::boolean,false)
            then 'AMEND_PAID_UNINVOICED_SOURCE'
          when m.action_kind='APPLY_AMENDMENT' then 'AMEND_SOURCE'
          else null
        end,
        'reconciliation_mode',case
          when m.action_kind='APPLY_AMENDMENT' and coalesce((m.reconciliation_balance#>>'{B_hours,total_hours}')::numeric,0)>0
            then 'FROZEN_INVOICE_BALANCE'
          when m.action_kind='APPLY_AMENDMENT' then 'ORDINARY_SOURCE'
          else null end,
        'mutable_replacement_timesheet_id',coalesce(
          (select x.value::uuid from jsonb_array_elements_text(coalesce(m.reconciliation_balance->'active_mutable_member_ids','[]'::jsonb)) x(value)
            join public.timesheets mutable_ts on mutable_ts.timesheet_id=x.value::uuid and mutable_ts.correction_kind='CHANGED_HOURS_REPLACEMENT' limit 1),
          m.mutable_replacement_timesheet_id),
        'correction_id',m.reconciliation_balance->>'active_mutable_correction_id',
        'correction_generation_required',coalesce((m.reconciliation_balance#>>'{B_hours,total_hours}')::numeric,0)>0
          and not coalesce((m.reconciliation_balance->>'active_mutable_generation')::boolean,false),
        'standard_representable',coalesce((m.reconciliation_balance->>'B_standard_representable')::boolean,true),
        'B_hours',m.reconciliation_balance->'B_hours','B_financials',m.reconciliation_balance->'B_financials',
        'B_standard_schedule_json',m.reconciliation_balance->'B_standard_schedule_json',
        'B_policy_fingerprint',m.reconciliation_balance->>'B_policy_fingerprint',
        'effective_invoice_ids',m.reconciliation_balance->'effective_invoice_ids',
        'effective_invoice_line_ids',m.reconciliation_balance->'effective_invoice_line_ids',
        'M_hours',m.reconciliation_balance->'M_hours','M_existing_financials',m.reconciliation_balance->'M_existing_financials',
        'A_hours',m.reconciliation_balance->'A_hours','A_schedule_json',m.reconciliation_balance->'A_schedule_json',
        'effective_invoice_fingerprint',m.reconciliation_balance->>'effective_invoice_fingerprint',
        'mutable_generation_fingerprint',m.reconciliation_balance->>'active_mutable_fingerprint',
        'authoritative_evidence_fingerprint',m.reconciliation_balance->>'A_evidence_fingerprint',
        'reconciliation_fingerprint',m.reconciliation_balance->>'reconciliation_fingerprint',
        'source_scope_fingerprint',m.reconciliation_balance->>'source_scope_fingerprint'
      ) || jsonb_build_object(
        'archived_timesheet_ids',m.reconciliation_balance->'archived_timesheet_ids',
        'historical_missing_timesheet_ids',m.reconciliation_balance->'historical_missing_timesheet_ids',
        'active_mutable_member_ids',m.reconciliation_balance->'active_mutable_member_ids',
        'missing_mutable_roles',m.reconciliation_balance->'active_mutable_missing_roles',
        'active_mutable_parent_timesheet_id',m.reconciliation_balance->>'active_mutable_parent_timesheet_id',
        'pre_apply_authorised',m.source_authorised_at_server is not null or m.source_tsfin_authorised_at_utc is not null,
        'effective_auto_authorise',m.effective_auto_authorise,
        'intended_authorisation_action',case
          when m.source_authorised_at_server is not null or m.source_tsfin_authorised_at_utc is not null then 'REAUTHORISE'
          when m.effective_auto_authorise then 'AUTHORISE' else 'LEAVE_UNAUTHORISED' end,
        'financial_validation_mode',case
          when m.action_kind='APPLY_AMENDMENT' and coalesce((m.reconciliation_balance#>>'{B_hours,total_hours}')::numeric,0)>0
            then 'CORRECTION_NEGATIVE_MUST_REVERSE_FROZEN_B_AND_POSITIVE_TSFIN_DEFINES_A'
          when m.action_kind='APPLY_AMENDMENT' then 'ORDINARY_TSFIN_DEFINES_A' end,
        'candidate_name',m.staff_label,'client_name',m.client_label,'work_date',m.date_local,
        'week_ending_date',m.date_local + ((7-extract(dow from m.date_local)::integer)%7),
        'start_time',m.start_time_local,'end_time',m.end_time_local,
        'break_minutes',coalesce((m.payload_json->>'actual_break_mins')::integer,(m.payload_json->>'actual_break_minutes')::integer,
          (m.payload_json->>'break_mins')::integer,(m.payload_json->>'break_minutes')::integer),
        'hours_worked',m.hours_worked,'role',coalesce(m.weekly_incoming_code,m.assignment_grade_norm),
        'imported_evidence',jsonb_strip_nulls(jsonb_build_object(
          'work_date',m.date_local,'start',m.start_time_local,'end',m.end_time_local,
          'break_minutes',coalesce((m.payload_json->>'actual_break_mins')::integer,(m.payload_json->>'actual_break_minutes')::integer,
            (m.payload_json->>'break_mins')::integer,(m.payload_json->>'break_minutes')::integer),
          'worked_hours',m.hours_worked,'worked_minutes',case when m.hours_worked is null then null else round(m.hours_worked*60) end,
          'reference',m.hr_request_id,'role',coalesce(m.weekly_incoming_code,m.assignment_grade_norm),'grade',m.grade_key)),
        'current_evidence',case when m.is_daily and m.resolved_timesheet_id is not null then jsonb_strip_nulls(jsonb_build_object(
          'work_date',(m.worked_start_iso at time zone 'Europe/London')::date,'start',m.worked_start_iso,'end',m.worked_end_iso,
          'break_minutes',m.ts_break_minutes,'elapsed_minutes',m.worked_minutes,
          'worked_minutes',greatest(m.worked_minutes-coalesce(m.ts_break_minutes,0),0),
          'worked_hours',round(greatest(m.worked_minutes-coalesce(m.ts_break_minutes,0),0)/60.0,2),
          'reference',m.reference_number,'role',m.tsfin_role,'band',m.tsfin_band,'timesheet_id',m.resolved_timesheet_id))
          when not m.is_daily and m.existing_shift_id is not null then jsonb_strip_nulls(jsonb_build_object(
          'work_date',m.date_local,'start',m.existing_shift_start_utc,'end',m.existing_shift_end_utc,
          'break_minutes',m.existing_shift_break_minutes,'worked_minutes',m.existing_shift_paid_minutes,
          'role',m.existing_shift_role,'timesheet_id',m.existing_shift_timesheet_id,'shift_id',m.existing_shift_id)) end,
        'difference_codes',to_jsonb(array_remove(array[
          case when m.existing_shift_id is null and not m.is_daily then 'NEW_SHIFT'::text end,
          case when m.is_daily and m.resolved_timesheet_id is null then 'TIMESHEET_SELECTION_REQUIRED'::text end,
          case when m.is_daily and m.resolved_timesheet_id is not null and m.start_time_local is distinct from
            (m.worked_start_iso at time zone 'Europe/London')::time then 'START_TIME'::text end,
          case when m.is_daily and m.resolved_timesheet_id is not null and m.end_time_local is distinct from
            (m.worked_end_iso at time zone 'Europe/London')::time then 'END_TIME'::text end,
          case when m.is_daily and m.resolved_timesheet_id is not null
            and coalesce((m.payload_json->>'break_evidence_supplied')::boolean,false)
            and (m.payload_json->>'break_mins')::integer is distinct from coalesce(m.ts_break_minutes,0)
            then 'BREAK_MINUTES'::text end,
          case when m.is_daily and m.resolved_timesheet_id is not null and m.hours_worked is not null
            and abs((m.hours_worked*60)-greatest(m.worked_minutes-coalesce(m.ts_break_minutes,0),0))>1
            then 'WORKED_HOURS'::text end,
          case when not m.is_daily and m.existing_shift_id is not null and m.start_time_local is distinct from
            (m.existing_shift_start_utc at time zone 'Europe/London')::time then 'START_TIME'::text end,
          case when not m.is_daily and m.existing_shift_id is not null and m.end_time_local is distinct from
            (m.existing_shift_end_utc at time zone 'Europe/London')::time then 'END_TIME'::text end,
          case when not m.is_daily and m.existing_shift_id is not null
            and coalesce((m.payload_json->>'break_evidence_supplied')::boolean,false)
            and (m.payload_json->>'break_mins')::integer is distinct from coalesce(m.existing_shift_break_minutes,0)
            then 'BREAK_MINUTES'::text end,
          case when not m.is_daily and m.existing_shift_id is not null and m.hours_worked is not null
            and abs((m.hours_worked*60)-m.existing_shift_paid_minutes)>1 then 'WORKED_HOURS'::text end
        ],null)),
        'outcome_label',case
          when not m.is_daily and not coalesce(m.import_authoritative,false) then 'Validate candidate timesheet'
          when m.is_daily and coalesce(m.timesheet_count,0)=0
            and coalesce(m.daily_submitted_timesheet_count,0)=0 then 'Request timesheet from candidate'
          when m.is_daily and coalesce(m.timesheet_count,0)=0 then 'Candidate timesheet states they did not work this shift'
          when not m.is_daily and coalesce(m.authoritative_timesheet_has_calculated_expenses,false)
            then 'Timesheet occupied by expenses'
          when m.action_kind='INCLUDE_SHIFT' then 'TMS will add shift'
          when m.action_kind='APPLY_AMENDMENT'
            and coalesce((m.reconciliation_balance->>'active_mutable_generation')::boolean,false)
            then 'TMS will repair current correction generation'
          when m.action_kind='APPLY_AMENDMENT'
            and coalesce((m.reconciliation_balance#>>'{B_hours,total_hours}')::numeric,0)>0
            then 'TMS will create correction generation'
          when m.action_kind='APPLY_AMENDMENT' and coalesce((m.protection->>'paid')::boolean,false)
            then 'TMS will amend paid uninvoiced shift'
          when m.action_kind='APPLY_AMENDMENT' then 'TMS will amend shift'
          when m.action_kind='APPLY_CANCELLATION' then case when coalesce((m.protection->>'paid')::boolean,false)
            or coalesce((m.protection->>'invoice_locked')::boolean,false)
            then 'TMS will reverse shift' else 'TMS will cancel shift' end
          when m.action_kind='DAILY_TIMESHEET_RESOLUTION' then 'Choose existing timesheet' when m.action_kind='NO_ACTION' then 'No action required'
          else 'Resolve before continuing' end,
        'resolution_kind',case
          when m.resolved_candidate_id is null then 'CANDIDATE_LINK'
          when m.resolved_client_id is null then 'CLIENT_LINK'
          when m.is_daily and not coalesce(m.has_grade_mapping,false) then 'DAILY_GRADE_ROLE'
          when not m.is_daily and m.weekly_resolution_action='REJECT_NO_CONTRACT_BAND_MISMATCH'
            and not coalesce(m.has_weekly_mapping,false) then 'WEEKLY_ASSIGNMENT_CONTRACT'
          when m.is_daily and m.resolved_timesheet_id is null and coalesce(m.timesheet_count,0)>0 then 'DAILY_EXISTING_TIMESHEET' end,
        'resolution_options',case
          when m.is_daily and not coalesce(m.has_grade_mapping,false) then m.daily_role_options
          when not m.is_daily and m.weekly_resolution_action='REJECT_NO_CONTRACT_BAND_MISMATCH' then m.weekly_contract_options
          else '[]'::jsonb end,
        'mapping_evidence',case when m.is_daily then jsonb_strip_nulls(jsonb_build_object(
          'mapping_id',m.daily_mapping_id,'updated_at',m.daily_mapping_updated_at,'role',m.daily_mapped_role,'band',m.daily_mapped_band))
          else jsonb_strip_nulls(jsonb_build_object('mapping_fingerprint',m.weekly_mapping_evidence,
            'resolution_action',m.weekly_resolution_action,'resolution_reason',m.weekly_resolution_reason)) end,
        'timesheet_options',case when m.is_daily then to_jsonb(coalesce(m.timesheet_ids,array[]::uuid[])) else null end,
        'occupied_timesheet_id',case when coalesce(m.authoritative_timesheet_has_calculated_expenses,false)
          then m.authoritative_target_timesheet_id end,
        'protection',m.protection
      )) summary_json
    from main_actions m
  )
  select * from rendered;

  -- Daily mismatch/query actions are independent of the evidence association.
  insert into pg_temp.import_review_catalog_v1
  with r as (
    select h.*,d.resolved_timesheet_id as timesheet_id,t.candidate_id,t.client_id,t.worked_start_iso,t.worked_end_iso,
      t.break_minutes,t.worked_minutes,t.reference_number,t.processing_status::text,
      c.id contract_id,public._import_review_timesheet_protection_core_v1(d.resolved_timesheet_id) protection
    from public.hr_rows h
    join public.hr_imports i on i.id=h.import_id
    join public.import_review_daily_timesheet_resolutions d on d.import_id=h.import_id and d.hr_row_id=h.id and d.status in ('CURRENT','APPLIED')
    join public.v_timesheets_daily_match t on t.timesheet_id=d.resolved_timesheet_id
    left join public.contracts c on c.id=(select ts.contract_id from public.timesheets ts where ts.timesheet_id=t.timesheet_id)
    where h.import_id=p_import_id and (upper(i.source_system::text)='HEALTHROSTER_DAILY' or upper(coalesce(i.import_scope,'')) like '%DAILY%')
    order by h.id limit 501
  ), mismatch as (
    select r.*,
      case
        when r.hours_worked is not null and r.worked_minutes is not null
          and abs((r.hours_worked*60)-greatest(r.worked_minutes-coalesce(r.break_minutes,0),0))>1
          then 'ACTUAL_HOURS_MISMATCH'
        when r.start_time_local is distinct from (r.worked_start_iso at time zone 'Europe/London')::time then 'START_END_MISMATCH'
        when r.end_time_local is distinct from (r.worked_end_iso at time zone 'Europe/London')::time then 'START_END_MISMATCH'
        when coalesce((r.payload_json->>'break_evidence_supplied')::boolean,false)
          and (r.payload_json->>'break_mins')::integer is distinct from coalesce(r.break_minutes,0)
          then 'BREAK_MINUTES_MISMATCH'
      end reason_code
    from r
  ), issues as (
    select m.*,public._import_review_hash_v1(concat_ws('|','HEALTHROSTER_DAILY',m.reason_code,m.timesheet_id,m.hr_request_id,
      lower(coalesce(m.staff_norm,'')),m.date_local,m.start_time_local,m.end_time_local,m.hours_worked,m.worked_minutes)) issue_fingerprint,
      lower(btrim(case when coalesce(m.contract_id is not null and
        (select c.send_ts_queries_to_different_email from public.contracts c where c.id=m.contract_id),false)
        then (select c.ts_queries_alt_email_address from public.contracts c where c.id=m.contract_id)
        else (select c.ts_queries_email from public.clients c where c.id=m.client_id) end)) route_email
    from mismatch m where m.reason_code is not null
  )
  select
    public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,
      case when e.id is null then 'EMAIL_ISSUE' else 'EMAIL_REMINDER' end,i.issue_fingerprint)),
    case when e.id is null then 'EMAIL_ISSUE' else 'EMAIL_REMINDER' end,'EMAIL',
    'issue:'||i.issue_fingerprint,i.issue_fingerprint,i.id,i.timesheet_id,null::uuid,i.client_id,i.candidate_id,i.contract_id,e.id,
    public._import_review_hash_v1(concat_ws('|','issue-evidence-v1',i.issue_fingerprint,i.protection::text,
      coalesce(e.delivery_history_status,'NEW'),coalesce(e.sent_count,0),
      case when coalesce(i.contract_id is not null and (select c.send_ts_queries_to_different_email from public.contracts c where c.id=i.contract_id),false)
        then (select concat_ws('|',c.updated_at,c.ts_queries_alt_email_address) from public.contracts c where c.id=i.contract_id)
        else (select concat_ws('|',c.rev,c.updated_at,c.ts_queries_email) from public.clients c where c.id=i.client_id) end)),
    not coalesce((i.protection->>'active_pay_draft')::boolean,false) and length(coalesce(i.route_email,'')) between 3 and 320 and position('@' in i.route_email)>1,
    e.id is null and not coalesce((i.protection->>'active_pay_draft')::boolean,false) and length(coalesce(i.route_email,'')) between 3 and 320 and position('@' in i.route_email)>1,
    false,
    jsonb_build_object('reason_code',i.reason_code,'issue_fingerprint',i.issue_fingerprint,'work_date',i.date_local,
      'candidate_name',i.staff_raw,'timesheet_id',i.timesheet_id,'recipient_scope_key',
      case when coalesce(i.contract_id is not null and (select c.send_ts_queries_to_different_email from public.contracts c where c.id=i.contract_id),false)
        then 'CONTRACT_OVERRIDE:'||i.contract_id::text else 'CLIENT_DEFAULT:'||i.client_id::text end,
      'recipient_route_fingerprint',case when coalesce(i.contract_id is not null and
        (select c.send_ts_queries_to_different_email from public.contracts c where c.id=i.contract_id),false)
        then (select public._import_review_hash_v1(concat_ws('|','query-route-v1','CONTRACT_OVERRIDE:'||c.id::text,
          lower(btrim(coalesce(c.ts_queries_alt_email_address,''))),c.updated_at)) from public.contracts c where c.id=i.contract_id)
        else (select public._import_review_hash_v1(concat_ws('|','query-route-v1','CLIENT_DEFAULT:'||c.id::text,
          lower(btrim(coalesce(c.ts_queries_email,''))),c.rev,c.updated_at)) from public.clients c where c.id=i.client_id) end,
      'delivery_history_status',coalesce(e.delivery_history_status,'NEW'),'sent_count',coalesce(e.sent_count,0),
      'default_excluded_reason',case when e.id is not null then 'PREVIOUS_OR_LEGACY_HISTORY_REQUIRES_EXPLICIT_REMINDER'
        when length(coalesce(i.route_email,'')) not between 3 and 320 or position('@' in coalesce(i.route_email,''))<=1 then 'QUERY_RECIPIENT_EMAIL_MISSING_OR_INVALID'
        when coalesce((i.protection->>'active_pay_draft')::boolean,false) then 'BLOCKED_ACTIVE_PAY_DRAFT' end,
      'protection',i.protection)
  from issues i left join public.hr_issue_emails e on e.issue_fingerprint=i.issue_fingerprint;

  -- Weekly validation-only issues use the installed comparison engine, but
  -- normalise every user choice into the same server-owned decision catalogue.
  if exists(select 1 from public.hr_imports i where i.id=p_import_id
      and i.source_system='HEALTHROSTER'::public.hr_source_enum
      and upper(coalesce(i.import_scope,'HR_WEEKLY')) not like '%DAILY%') then
    v_weekly_preview:=public.hr_weekly_validation_preview(p_import_id);

    -- Validation-only Weekly evidence has two distinct, server-proven states.
    -- Neither state is an instruction to mutate CloudTMS financial records.
    insert into pg_temp.import_review_catalog_v1
    with preview_rows as (
      select r.value row_json,
        nullif(r.value->>'timesheet_id','')::uuid timesheet_id,
        nullif(r.value->>'client_id','')::uuid client_id,
        nullif(r.value->>'candidate_id','')::uuid candidate_id,
        nullif(r.value->>'contract_id','')::uuid contract_id
      from jsonb_array_elements(case when jsonb_typeof(v_weekly_preview->'rows')='array'
        then v_weekly_preview->'rows' else '[]'::jsonb end) r(value)
    ), eligible_validation_groups as (
      select d.candidate_id,d.summary_json->>'week_ending_date' week_ending_date
      from pg_temp.import_review_catalog_v1 d
      where d.candidate_id is not null
        and d.summary_json->>'source_route' not like '%DAILY%'
        and d.summary_json->>'authority_mode'='VALIDATION_ONLY'
      group by d.candidate_id,d.summary_json->>'week_ending_date'
      having bool_and(d.action_kind='NO_ACTION' and not d.blocking)
    ), missing_timesheets as (
      select p.row_json,p.timesheet_id,p.candidate_id,
        d.hr_row_id shift_hr_row_id,d.client_id shift_client_id,
        d.contract_id shift_contract_id,d.source_identity shift_source_identity,
        d.evidence_fingerprint shift_evidence_fingerprint,d.summary_json shift_summary_json
      from preview_rows p
      join eligible_validation_groups g on g.candidate_id=p.candidate_id
        and g.week_ending_date=p.row_json->>'week_ending_date'
      join pg_temp.import_review_catalog_v1 d on d.candidate_id=p.candidate_id
        and d.summary_json->>'week_ending_date'=p.row_json->>'week_ending_date'
        and d.summary_json->>'source_route' not like '%DAILY%'
        and d.summary_json->>'authority_mode'='VALIDATION_ONLY'
        and d.action_kind='NO_ACTION' and not d.blocking
      where p.row_json->>'overall_status'='MISSING_TIMESHEET'
    ), omitted_shifts as (
      select p.*,cx.value comparison_json
      from preview_rows p
      join eligible_validation_groups g on g.candidate_id=p.candidate_id
        and g.week_ending_date=p.row_json->>'week_ending_date'
      cross join lateral jsonb_array_elements(coalesce(p.row_json->'comparisons','[]'::jsonb)) cx(value)
      where p.timesheet_id is not null and cx.value->>'match_status'='HR_ONLY'
    )
    select public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,
        'WEEKLY_TIMESHEET_NOT_SUBMITTED',m.shift_hr_row_id)),
      'ADVISORY','BLOCKED',
      concat_ws(':','weekly-timesheet-not-submitted',m.shift_hr_row_id),
      m.shift_source_identity,
      m.shift_hr_row_id,null::uuid,null::uuid,m.shift_client_id,m.candidate_id,m.shift_contract_id,null::uuid,
      public._import_review_hash_v1(concat_ws('|','weekly-timesheet-not-submitted-v2',
        m.shift_evidence_fingerprint,m.row_json::text)),
      false,false,true,
      jsonb_strip_nulls(m.shift_summary_json||jsonb_build_object(
        'reason_code','WEEKLY_TIMESHEET_NOT_SUBMITTED','source_route','HR_WEEKLY','authority_mode','VALIDATION_ONLY',
        'candidate_name',m.row_json->>'candidate_name','week_ending_date',m.row_json->>'week_ending_date',
        'difference_codes',jsonb_build_array('TIMESHEET_NOT_SUBMITTED'),
        'outcome_label','Request timesheet from candidate'))
    from missing_timesheets m
    union all
    select public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,
        'WEEKLY_SHIFT_ABSENT_FROM_TIMESHEET',o.timesheet_id,o.comparison_json->>'work_date',
        o.comparison_json->>'healthroster_start',o.comparison_json->>'healthroster_end')),
      'ADVISORY','BLOCKED',
      concat_ws(':','weekly-shift-absent',o.timesheet_id,o.comparison_json->>'work_date',
        o.comparison_json->>'healthroster_start',o.comparison_json->>'healthroster_end'),
      concat_ws('|',o.timesheet_id,o.comparison_json->>'work_date',
        o.comparison_json->>'healthroster_start',o.comparison_json->>'healthroster_end'),
      null::uuid,o.timesheet_id,null::uuid,o.client_id,o.candidate_id,o.contract_id,null::uuid,
      public._import_review_hash_v1(concat_ws('|','weekly-shift-absent-v1',o.timesheet_id,o.comparison_json::text)),
      false,false,true,
      jsonb_build_object(
        'reason_code','WEEKLY_SHIFT_ABSENT_FROM_TIMESHEET','source_route','HR_WEEKLY','authority_mode','VALIDATION_ONLY',
        'candidate_name',o.row_json->>'candidate_name','week_ending_date',o.row_json->>'week_ending_date',
        'work_date',o.comparison_json->>'work_date',
        'imported_evidence',jsonb_strip_nulls(jsonb_build_object(
          'work_date',o.comparison_json->>'work_date','start',o.comparison_json->>'healthroster_start',
          'end',o.comparison_json->>'healthroster_end',
          'break_minutes',nullif(o.comparison_json->>'healthroster_break_mins','')::integer,
          'reference',o.comparison_json->>'ref_after')),
        'current_evidence',jsonb_build_object('timesheet_id',o.timesheet_id),
        'difference_codes',jsonb_build_array('HR_ONLY'),
        'outcome_label','Candidate timesheet states they did not work this shift')
    from omitted_shifts o;

    insert into pg_temp.import_review_catalog_v1
    with preview_rows as (
      select r.value row_json,
        nullif(r.value->>'timesheet_id','')::uuid timesheet_id,
        nullif(r.value->>'client_id','')::uuid client_id,
        nullif(r.value->>'candidate_id','')::uuid candidate_id,
        nullif(r.value->>'contract_id','')::uuid contract_id,
        nullif(r.value->>'issue_fingerprint','') issue_fingerprint
      from jsonb_array_elements(case when jsonb_typeof(v_weekly_preview->'rows')='array'
        then v_weekly_preview->'rows' else '[]'::jsonb end) r(value)
    ), email_filtered as (
      select p.*,
        coalesce((select jsonb_agg(cx.value order by cx.value->>'work_date',cx.value->>'comparison_key')
          from jsonb_array_elements(coalesce(p.row_json->'comparisons','[]'::jsonb)) cx(value)
          where (
            coalesce(cx.value->>'match_status','MATCH') not in ('MATCH','HR_ONLY')
            or coalesce((cx.value->>'ref_changed')::boolean,false)
          )),'[]'::jsonb) email_comparisons,
        coalesce((select jsonb_agg(day_json.value order by day_json.value->>'date')
          from jsonb_array_elements(coalesce(p.row_json->'days','[]'::jsonb)) day_json(value)
          where exists (
            select 1
            from jsonb_array_elements(coalesce(p.row_json->'comparisons','[]'::jsonb)) cx(value)
            where cx.value->>'work_date'=day_json.value->>'date'
              and (
                coalesce(cx.value->>'match_status','MATCH') not in ('MATCH','HR_ONLY')
                or coalesce((cx.value->>'ref_changed')::boolean,false)
              )
          )),'[]'::jsonb) email_days,
        coalesce((select jsonb_agg(to_jsonb(fr.value))
          from jsonb_array_elements_text(coalesce(p.row_json->'failure_reasons','[]'::jsonb)) fr(value)
          where fr.value<>'HealthRoster has a shift not present on the timesheet.'),'[]'::jsonb) email_failure_reasons
      from preview_rows p
    ), routed as (
      select p.*,public._import_review_hash_v1(concat_ws('|','HEALTHROSTER_WEEKLY','validation-email-v2',
          p.timesheet_id,p.row_json->>'week_ending_date',p.email_comparisons::text)) email_issue_fingerprint,
        c.rev client_rev,c.updated_at client_updated_at,c.ts_queries_email,
        ct.send_ts_queries_to_different_email,ct.ts_queries_alt_email_address,ct.updated_at contract_updated_at,
        case when coalesce(ct.send_ts_queries_to_different_email,false)
          then 'CONTRACT_OVERRIDE:'||ct.id::text else 'CLIENT_DEFAULT:'||c.id::text end recipient_scope_key,
        case when coalesce(ct.send_ts_queries_to_different_email,false)
          then 'CONTRACT_OVERRIDE' else 'CLIENT_DEFAULT' end recipient_scope,
        lower(btrim(case when coalesce(ct.send_ts_queries_to_different_email,false)
          then ct.ts_queries_alt_email_address else c.ts_queries_email end)) recipient_email,
        public._import_review_timesheet_protection_core_v1(p.timesheet_id) protection,
        e.id issue_id,e.delivery_history_status,e.sent_count
      from email_filtered p
      join public.clients c on c.id=p.client_id
      left join public.contracts ct on ct.id=p.contract_id and ct.client_id=p.client_id
      left join public.hr_issue_emails e on e.issue_fingerprint=public._import_review_hash_v1(concat_ws('|',
        'HEALTHROSTER_WEEKLY','validation-email-v2',p.timesheet_id,p.row_json->>'week_ending_date',p.email_comparisons::text))
      where p.timesheet_id is not null and p.issue_fingerprint is not null
        and coalesce((p.row_json->>'has_mismatch')::boolean,false)
        and exists (
          select 1 from jsonb_array_elements(coalesce(p.row_json->'comparisons','[]'::jsonb)) cx(value)
          where coalesce(cx.value->>'match_status','MATCH') not in ('MATCH','HR_ONLY')
            or coalesce((cx.value->>'ref_changed')::boolean,false)
        )
    ), email_actions as (
      select r.*,
        public._import_review_hash_v1(concat_ws('|','query-route-v1',r.recipient_scope_key,r.recipient_email,
          case when r.recipient_scope='CONTRACT_OVERRIDE' then r.contract_updated_at::text
            else concat_ws('|',r.client_rev,r.client_updated_at) end)) route_fingerprint,
        length(coalesce(r.recipient_email,'')) between 3 and 320
          and r.recipient_email~* '^[A-Z0-9.!#$%&''*+/=?^_`{|}~-]+@[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?(?:\.[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?)+$' valid_email
      from routed r
    )
    select public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,
        case when a.issue_id is null then 'EMAIL_ISSUE' else 'EMAIL_REMINDER' end,a.email_issue_fingerprint)),
      case when a.issue_id is null then 'EMAIL_ISSUE' else 'EMAIL_REMINDER' end,
      case when coalesce((a.protection->>'active_pay_draft')::boolean,false) then 'BLOCKED' else 'EMAIL' end,
      'issue:'||a.email_issue_fingerprint,a.email_issue_fingerprint,null::uuid,a.timesheet_id,null::uuid,
      a.client_id,a.candidate_id,a.contract_id,a.issue_id,
      public._import_review_hash_v1(concat_ws('|','weekly-query-evidence-v2',a.timesheet_id,
        a.row_json->>'candidate_name',a.row_json->>'week_ending_date',
        a.email_comparisons::text,a.email_days::text,a.email_failure_reasons::text,a.protection::text,
        a.route_fingerprint,coalesce(a.delivery_history_status,'NEW'),coalesce(a.sent_count,0))),
      a.valid_email and not coalesce((a.protection->>'active_pay_draft')::boolean,false),
      a.issue_id is null and a.valid_email and not coalesce((a.protection->>'active_pay_draft')::boolean,false),
      coalesce((a.protection->>'active_pay_draft')::boolean,false),
      jsonb_build_object('reason_code','HEALTHROSTER_WEEKLY','issue_fingerprint',a.email_issue_fingerprint,
        'candidate_name',a.row_json->>'candidate_name','week_ending_date',a.row_json->>'week_ending_date',
      'failure_reasons',a.email_failure_reasons,
        'days',a.email_days,'comparisons',a.email_comparisons,
        'evidence_rows',coalesce((
          select jsonb_agg(jsonb_build_object(
            'imported_evidence',jsonb_strip_nulls(jsonb_build_object(
              'work_date',cx.value->>'work_date','start',cx.value->>'healthroster_start',
              'end',cx.value->>'healthroster_end','break_minutes',nullif(cx.value->>'healthroster_break_mins','')::integer,
              'worked_minutes',nullif(day_json.value->>'hr_minutes','')::integer,'reference',cx.value->>'ref_after')),
            'current_evidence',jsonb_strip_nulls(jsonb_build_object(
              'work_date',cx.value->>'work_date','start',cx.value->>'timesheet_start',
              'end',cx.value->>'timesheet_end','break_minutes',nullif(cx.value->>'timesheet_break_mins','')::integer,
              'worked_minutes',nullif(day_json.value->>'ts_minutes','')::integer,'reference',cx.value->>'ref_before')),
            'difference_codes',to_jsonb(array_remove(array[
              case when coalesce(cx.value->>'match_status','MATCH')<>'MATCH' then cx.value->>'match_status' end,
              case when coalesce((cx.value->>'ref_changed')::boolean,false) then 'REFERENCE' end,
              case when coalesce(day_json.value->>'day_status','OK')<>'OK' then 'WORKED_HOURS' end
            ],null))
          ) order by cx.value->>'work_date',cx.value->>'comparison_key')
          from jsonb_array_elements(a.email_comparisons) cx(value)
          left join lateral (select d.value from jsonb_array_elements(coalesce(a.row_json->'days','[]'::jsonb)) d(value)
            where d.value->>'date'=cx.value->>'work_date' limit 1) day_json on true
        ),'[]'::jsonb),
        'outcome_label',case when a.issue_id is null then 'Request amend shift' else 'Request amend shift reminder' end,
        'recipient_scope_key',a.recipient_scope_key,'recipient_route_fingerprint',a.route_fingerprint,
        'delivery_history_status',coalesce(a.delivery_history_status,'NEW'),'sent_count',coalesce(a.sent_count,0),
        'default_excluded_reason',case when a.issue_id is not null then 'PREVIOUS_OR_LEGACY_HISTORY_REQUIRES_EXPLICIT_REMINDER'
          when not a.valid_email then 'QUERY_RECIPIENT_EMAIL_MISSING_OR_INVALID'
          when coalesce((a.protection->>'active_pay_draft')::boolean,false) then 'BLOCKED_ACTIVE_PAY_DRAFT' end,
        'protection',a.protection)
    from email_actions a;

    insert into pg_temp.import_review_catalog_v1
    with preview_rows as (
      select r.value row_json,
        nullif(r.value->>'timesheet_id','')::uuid timesheet_id,
        nullif(r.value->>'client_id','')::uuid client_id,
        nullif(r.value->>'candidate_id','')::uuid candidate_id,
        nullif(r.value->>'contract_id','')::uuid contract_id
      from jsonb_array_elements(case when jsonb_typeof(v_weekly_preview->'rows')='array'
        then v_weekly_preview->'rows' else '[]'::jsonb end) r(value)
      where nullif(r.value->>'timesheet_id','') is not null
    ), invalidations as (
      select p.*,cx.value comparison_json,nullif(btrim(cx.value->>'comparison_key'),'') comparison_key,
        public._import_review_timesheet_protection_core_v1(p.timesheet_id) protection
      from preview_rows p
      cross join lateral jsonb_array_elements(coalesce(p.row_json->'comparisons','[]'::jsonb)) cx(value)
      where coalesce((cx.value->>'is_destructive_invalidation')::boolean,false)
        and exists(select 1 from public.hr_imports hi where hi.id=p_import_id
          and hi.coverage_mode in ('COMPLETE_ALL','COMPLETE_SELECTED_CANDIDATES'))
        and nullif(btrim(cx.value->>'comparison_key'),'') is not null
        and nullif(btrim(cx.value->>'ref_before'),'') is not null
    )
    select public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,'INVALIDATE_REFERENCE',i.timesheet_id,i.comparison_key)),
      'INVALIDATE_REFERENCE','PENDING','timesheet:'||i.timesheet_id::text||':'||i.comparison_key,
      i.comparison_key,null::uuid,i.timesheet_id,null::uuid,i.client_id,i.candidate_id,i.contract_id,null::uuid,
      public._import_review_hash_v1(concat_ws('|','weekly-reference-invalidation-v1',i.timesheet_id,i.comparison_json::text,i.protection::text)),
      not coalesce((i.protection->>'protected')::boolean,false),false,false,
      jsonb_build_object('reason_code','REFERENCE_ON_SHIFT_MISSING_OR_MISMATCHED_IN_COMPLETE_IMPORT',
        'candidate_name',i.row_json->>'candidate_name','week_ending_date',i.row_json->>'week_ending_date',
        'timesheet_id',i.timesheet_id,'comparison_key',i.comparison_key,'comparison',i.comparison_json,
        'protection',i.protection,'default_excluded_reason','REFERENCE_INVALIDATION_REQUIRES_EXPLICIT_SELECTION')
    from invalidations i;
  end if;

  -- Complete Daily coverage also exposes existing timesheets that are absent
  -- from the file.  Missing rows are query-email candidates; reference
  -- invalidation is a separate, explicit, default-off decision.
  insert into pg_temp.import_review_catalog_v1
  with i as (
    select * from public.hr_imports where id=p_import_id
  ), missing as (
    select t.*,ts.contract_id,c.first_name,c.last_name,cl.name as client_name,
      public._import_review_timesheet_protection_core_v1(t.timesheet_id) protection,
      lower(btrim(case when coalesce(ts.contract_id is not null and
        (select ct.send_ts_queries_to_different_email from public.contracts ct where ct.id=ts.contract_id),false)
        then (select ct.ts_queries_alt_email_address from public.contracts ct where ct.id=ts.contract_id)
        else cl.ts_queries_email end)) route_email,
      public._import_review_hash_v1(concat_ws('|','HEALTHROSTER_DAILY','MISSING_FROM_IMPORT',
        t.timesheet_id,t.candidate_id,t.client_id,(t.worked_start_iso at time zone 'Europe/London')::date,
        coalesce(t.reference_number,''))) issue_fingerprint
    from public.v_timesheets_daily_match t
    join public.timesheets ts on ts.timesheet_id=t.timesheet_id and ts.is_current
    join public.candidates c on c.id=t.candidate_id
    join public.clients cl on cl.id=t.client_id
    join i on true
    where (upper(i.source_system::text)='HEALTHROSTER_DAILY' or upper(coalesce(i.import_scope,'')) like '%DAILY%')
      and i.coverage_mode in ('COMPLETE_ALL','COMPLETE_SELECTED_CANDIDATES')
      and (t.worked_start_iso at time zone 'Europe/London')::date between i.coverage_start_date and i.coverage_end_date
      and (i.client_id is null or t.client_id=i.client_id)
      and (not exists(select 1 from public.import_review_scope_clients sc where sc.import_id=i.id)
        or exists(select 1 from public.import_review_scope_clients sc where sc.import_id=i.id and sc.client_id=t.client_id))
      and (i.coverage_mode='COMPLETE_ALL' or exists(
        select 1 from public.import_review_scope_candidates sc where sc.import_id=i.id and sc.candidate_id=t.candidate_id))
      and not exists (
        select 1 from public.import_review_daily_timesheet_resolutions r
        where r.import_id=i.id and r.resolved_timesheet_id=t.timesheet_id and r.status in ('CURRENT','APPLIED')
      )
    order by t.timesheet_id limit 501
  )
  select
    public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,
      case when e.id is null then 'EMAIL_ISSUE' else 'EMAIL_REMINDER' end,m.issue_fingerprint)),
    case when e.id is null then 'EMAIL_ISSUE' else 'EMAIL_REMINDER' end,'EMAIL',
    'issue:'||m.issue_fingerprint,m.issue_fingerprint,null::uuid,m.timesheet_id,null::uuid,
    m.client_id,m.candidate_id,m.contract_id,e.id,
    public._import_review_hash_v1(concat_ws('|','missing-daily-email-v1',m.issue_fingerprint,m.protection::text,
      coalesce(e.delivery_history_status,'NEW'),coalesce(e.sent_count,0),
      case when coalesce(m.contract_id is not null and (select ct.send_ts_queries_to_different_email from public.contracts ct where ct.id=m.contract_id),false)
        then (select concat_ws('|',ct.updated_at,ct.ts_queries_alt_email_address) from public.contracts ct where ct.id=m.contract_id)
        else (select concat_ws('|',cl.rev,cl.updated_at,cl.ts_queries_email) from public.clients cl where cl.id=m.client_id) end)),
    not coalesce((m.protection->>'active_pay_draft')::boolean,false) and length(coalesce(m.route_email,'')) between 3 and 320 and position('@' in m.route_email)>1,
    e.id is null and not coalesce((m.protection->>'active_pay_draft')::boolean,false) and length(coalesce(m.route_email,'')) between 3 and 320 and position('@' in m.route_email)>1,false,
    jsonb_build_object('reason_code','MISSING_FROM_IMPORT','issue_fingerprint',m.issue_fingerprint,
      'work_date',(m.worked_start_iso at time zone 'Europe/London')::date,
      'week_ending_date',(m.worked_start_iso at time zone 'Europe/London')::date
        + ((7-extract(dow from (m.worked_start_iso at time zone 'Europe/London')::date)::integer)%7),
      'candidate_name',btrim(concat_ws(' ',m.first_name,m.last_name)),'client_name',m.client_name,
      'timesheet_id',m.timesheet_id,'reference_number',m.reference_number,
      'start_time',(m.worked_start_iso at time zone 'Europe/London')::time,
      'end_time',(m.worked_end_iso at time zone 'Europe/London')::time,
      'break_minutes',m.break_minutes,'role',m.tsfin_role,
      'recipient_scope_key',case when coalesce(m.contract_id is not null and
        (select ct.send_ts_queries_to_different_email from public.contracts ct where ct.id=m.contract_id),false)
        then 'CONTRACT_OVERRIDE:'||m.contract_id::text else 'CLIENT_DEFAULT:'||m.client_id::text end,
      'recipient_route_fingerprint',case when coalesce(m.contract_id is not null and
        (select ct.send_ts_queries_to_different_email from public.contracts ct where ct.id=m.contract_id),false)
        then (select public._import_review_hash_v1(concat_ws('|','query-route-v1','CONTRACT_OVERRIDE:'||ct.id::text,
          lower(btrim(coalesce(ct.ts_queries_alt_email_address,''))),ct.updated_at)) from public.contracts ct where ct.id=m.contract_id)
        else (select public._import_review_hash_v1(concat_ws('|','query-route-v1','CLIENT_DEFAULT:'||cl.id::text,
          lower(btrim(coalesce(cl.ts_queries_email,''))),cl.rev,cl.updated_at)) from public.clients cl where cl.id=m.client_id) end,
      'delivery_history_status',coalesce(e.delivery_history_status,'NEW'),'sent_count',coalesce(e.sent_count,0),
      'default_excluded_reason',case when e.id is not null then 'PREVIOUS_OR_LEGACY_HISTORY_REQUIRES_EXPLICIT_REMINDER'
        when length(coalesce(m.route_email,'')) not between 3 and 320 or position('@' in coalesce(m.route_email,''))<=1 then 'QUERY_RECIPIENT_EMAIL_MISSING_OR_INVALID'
        when coalesce((m.protection->>'active_pay_draft')::boolean,false) then 'BLOCKED_ACTIVE_PAY_DRAFT' end,
      'protection',m.protection)
  from missing m left join public.hr_issue_emails e on e.issue_fingerprint=m.issue_fingerprint;

  insert into pg_temp.import_review_catalog_v1
  with i as (select * from public.hr_imports where id=p_import_id), missing as (
    select t.*,ts.contract_id,public._import_review_timesheet_protection_core_v1(t.timesheet_id) protection
    from public.v_timesheets_daily_match t
    join public.timesheets ts on ts.timesheet_id=t.timesheet_id and ts.is_current
    join i on true
    where (upper(i.source_system::text)='HEALTHROSTER_DAILY' or upper(coalesce(i.import_scope,'')) like '%DAILY%')
      and i.coverage_mode in ('COMPLETE_ALL','COMPLETE_SELECTED_CANDIDATES')
      and (t.worked_start_iso at time zone 'Europe/London')::date between i.coverage_start_date and i.coverage_end_date
      and (i.client_id is null or t.client_id=i.client_id)
      and (not exists(select 1 from public.import_review_scope_clients sc where sc.import_id=i.id)
        or exists(select 1 from public.import_review_scope_clients sc where sc.import_id=i.id and sc.client_id=t.client_id))
      and (i.coverage_mode='COMPLETE_ALL' or exists(
        select 1 from public.import_review_scope_candidates sc where sc.import_id=i.id and sc.candidate_id=t.candidate_id))
      and not exists(select 1 from public.import_review_daily_timesheet_resolutions r
        where r.import_id=i.id and r.resolved_timesheet_id=t.timesheet_id and r.status in ('CURRENT','APPLIED'))
    order by t.timesheet_id limit 501
  )
  select public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,'MARK_VALIDATION_ERROR',m.timesheet_id)),
    'MARK_VALIDATION_ERROR','READY','timesheet:'||m.timesheet_id::text,'missing-daily:'||m.timesheet_id::text,
    null::uuid,m.timesheet_id,null::uuid,m.client_id,m.candidate_id,m.contract_id,null::uuid,
    public._import_review_hash_v1(concat_ws('|','missing-daily-validation-v1',m.timesheet_id,m.worked_start_iso,
      m.worked_end_iso,m.break_minutes,m.worked_minutes,m.reference_number,m.protection::text)),
    not coalesce((m.protection->>'active_pay_draft')::boolean,false),
    not coalesce((m.protection->>'active_pay_draft')::boolean,false),
    coalesce((m.protection->>'active_pay_draft')::boolean,false),
    jsonb_build_object('reason_code',case when coalesce((m.protection->>'active_pay_draft')::boolean,false)
      then 'BLOCKED_ACTIVE_PAY_DRAFT' else 'MISSING_FROM_IMPORT' end,
      'work_date',(m.worked_start_iso at time zone 'Europe/London')::date,'timesheet_id',m.timesheet_id,
      'reference_number',m.reference_number,'start_time',(m.worked_start_iso at time zone 'Europe/London')::time,
      'end_time',(m.worked_end_iso at time zone 'Europe/London')::time,'break_minutes',m.break_minutes,
      'hours_worked',m.worked_minutes/60.0,'role',m.tsfin_role,'protection',m.protection)
  from missing m;

  insert into pg_temp.import_review_catalog_v1
  with i as (select * from public.hr_imports where id=p_import_id), missing as (
    select t.*,ts.contract_id,public._import_review_timesheet_protection_core_v1(t.timesheet_id) protection
    from public.v_timesheets_daily_match t
    join public.timesheets ts on ts.timesheet_id=t.timesheet_id and ts.is_current
    join i on true
    where (upper(i.source_system::text)='HEALTHROSTER_DAILY' or upper(coalesce(i.import_scope,'')) like '%DAILY%')
      and i.coverage_mode in ('COMPLETE_ALL','COMPLETE_SELECTED_CANDIDATES')
      and nullif(btrim(t.reference_number),'') is not null
      and (t.worked_start_iso at time zone 'Europe/London')::date between i.coverage_start_date and i.coverage_end_date
      and (i.client_id is null or t.client_id=i.client_id)
      and (not exists(select 1 from public.import_review_scope_clients sc where sc.import_id=i.id)
        or exists(select 1 from public.import_review_scope_clients sc where sc.import_id=i.id and sc.client_id=t.client_id))
      and (i.coverage_mode='COMPLETE_ALL' or exists(
        select 1 from public.import_review_scope_candidates sc where sc.import_id=i.id and sc.candidate_id=t.candidate_id))
      and not exists (select 1 from public.import_review_daily_timesheet_resolutions r
        where r.import_id=i.id and r.resolved_timesheet_id=t.timesheet_id and r.status in ('CURRENT','APPLIED'))
    order by t.timesheet_id limit 501
  )
  select public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,'INVALIDATE_REFERENCE',m.timesheet_id)),
    'INVALIDATE_REFERENCE','PENDING','timesheet:'||m.timesheet_id::text,'missing-daily:'||m.timesheet_id::text,
    null::uuid,m.timesheet_id,null::uuid,m.client_id,m.candidate_id,m.contract_id,null::uuid,
    public._import_review_hash_v1(concat_ws('|','missing-daily-reference-v1',m.timesheet_id,m.reference_number,m.protection::text)),
    not coalesce((m.protection->>'protected')::boolean,false),false,false,
    jsonb_build_object('reason_code','REFERENCE_ON_SHIFT_MISSING_FROM_COMPLETE_IMPORT',
      'work_date',(m.worked_start_iso at time zone 'Europe/London')::date,'timesheet_id',m.timesheet_id,
      'reference_number',m.reference_number,'protection',m.protection,
      'default_excluded_reason','REFERENCE_INVALIDATION_REQUIRES_EXPLICIT_SELECTION')
  from missing m;

  -- Omitted existing shifts are proposed only inside immutable complete coverage.
  insert into pg_temp.import_review_catalog_v1
  with i as (select * from public.hr_imports where id=p_import_id), missing as (
    select s.*,public._import_review_timesheet_protection_core_v1(s.timesheet_id) protection
    from public.nhsp_shifts s
    join i on true
    cross join lateral public._import_review_effective_authority_core_v1(
      case when i.source_system='NHSP'::public.hr_source_enum then 'NHSP' else 'HR_WEEKLY' end,
      s.contract_id,s.client_id,coalesce(s.week_ending_date,s.work_date)) authority
    where i.coverage_mode in ('COMPLETE_ALL','COMPLETE_SELECTED_CANDIDATES')
      and s.source_system=i.source_system
      and authority.import_authoritative
      and s.cancelled_at_utc is null
      and s.work_date between i.coverage_start_date and i.coverage_end_date
      and (i.client_id is null or s.client_id=i.client_id)
      and (not exists(select 1 from public.import_review_scope_clients sc where sc.import_id=i.id)
        or exists(select 1 from public.import_review_scope_clients sc where sc.import_id=i.id and sc.client_id=s.client_id))
      and (i.coverage_mode='COMPLETE_ALL' or exists (
        select 1 from public.import_review_scope_candidates sc where sc.import_id=i.id and sc.candidate_id=s.candidate_id))
      and not exists (select 1 from public.hr_rows h where h.import_id=i.id and h.external_row_key=s.external_row_key)
    order by s.id limit 501
  )
  select public._import_review_hash_v1(concat_ws('|','action-v1',p_import_id,'APPLY_CANCELLATION',m.id)),
    'APPLY_CANCELLATION','READY','shift:'||m.id::text,m.external_row_key,null::uuid,m.timesheet_id,m.id,m.client_id,m.candidate_id,m.contract_id,null::uuid,
    public._import_review_hash_v1(concat_ws('|','missing-shift-v1',m.id,m.updated_at,m.timesheet_id,m.protection::text)),
    not coalesce((m.protection->>'active_pay_draft')::boolean,false),not coalesce((m.protection->>'active_pay_draft')::boolean,false),
    coalesce((m.protection->>'active_pay_draft')::boolean,false),
    jsonb_build_object('reason_code',case when coalesce((m.protection->>'active_pay_draft')::boolean,false) then 'BLOCKED_ACTIVE_PAY_DRAFT' else 'MISSING_FROM_COMPLETE_IMPORT' end,
      'work_date',m.work_date,'week_ending_date',m.week_ending_date,'candidate_id',m.candidate_id,'client_id',m.client_id,
      'start_time',m.start_utc,'end_time',m.end_utc,'break_minutes',m.break_mins,'role',m.assignment_code,'protection',m.protection)
  from missing m;

  select count(*) into v_count from pg_temp.import_review_catalog_v1;
  if v_count>p_max_actions then
    raise exception 'IMPORT_REVIEW_ACTION_LIMIT_EXCEEDED' using errcode='54000',
      detail=jsonb_build_object('count',v_count,'max',p_max_actions)::text;
  end if;

  return query select c.action_id,c.action_kind,c.action_category,c.target_key,c.source_identity,
    c.hr_row_id,c.timesheet_id,c.shift_id,c.client_id,c.candidate_id,c.contract_id,c.issue_id,
    c.evidence_fingerprint,c.selectable,c.default_selected,c.blocking,c.summary_json
  from pg_temp.import_review_catalog_v1 c order by c.action_id;
end
$function$;

CREATE OR REPLACE FUNCTION public._import_review_effective_invoice_balance_core_v1(p_import_id uuid, p_source_items jsonb, p_max_sources integer DEFAULT 100, p_max_invoice_lines_per_source integer DEFAULT 512, p_max_audit_rows_per_source integer DEFAULT 256, p_max_operations_per_source integer DEFAULT 128)
 RETURNS TABLE(source_identity text, balance_json jsonb)
 LANGUAGE plpgsql
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_item jsonb;
  v_source_identity text;
  v_source_system text;
  v_external_row_key text;
  v_invoice_stream text;
  v_source_shift_id uuid;
  v_source_timesheet_id uuid;
  v_hr_row_id uuid;
  v_candidate_id uuid;
  v_client_id uuid;
  v_contract_id uuid;
  v_week_ending_date date;
  v_authoritative_import_id uuid;
  v_a_schedule jsonb;
  v_a_hours jsonb;
  v_a_fingerprint text;
  v_scope_fingerprint text;
  v_hist_ids uuid[]:=array[]::uuid[];
  v_audit_ids uuid[]:=array[]::uuid[];
  v_archived_ids uuid[]:=array[]::uuid[];
  v_active_ids uuid[]:=array[]::uuid[];
  v_missing_ids uuid[]:=array[]::uuid[];
  v_import_ids uuid[]:=array[]::uuid[];
  v_effective_invoice_ids uuid[]:=array[]::uuid[];
  v_effective_line_ids uuid[]:=array[]::uuid[];
  v_credit_line_ids uuid[]:=array[]::uuid[];
  v_line_count integer:=0;
  v_audit_count integer:=0;
  v_operation_count integer:=0;
  v_effective_component_count integer:=0;
  v_b_day numeric:=0;
  v_b_night numeric:=0;
  v_b_sat numeric:=0;
  v_b_sun numeric:=0;
  v_b_bh numeric:=0;
  v_b_pay numeric:=0;
  v_b_charge numeric:=0;
  v_b_margin numeric:=0;
  v_component_day numeric:=0;
  v_component_night numeric:=0;
  v_component_sat numeric:=0;
  v_component_sun numeric:=0;
  v_component_bh numeric:=0;
  v_component_pay numeric:=0;
  v_component_charge numeric:=0;
  v_component_margin numeric:=0;
  v_b_schedule jsonb:='[]'::jsonb;
  v_candidate_schedule jsonb:='[]'::jsonb;
  v_candidate_hours jsonb:='{}'::jsonb;
  v_b_policy_fingerprint text;
  v_effective_fingerprint text;
  v_line_evidence jsonb:='[]'::jsonb;
  v_line record;
  v_tf record;
  v_seg jsonb;
  v_seg_count integer:=0;
  v_matching_seg_count integer:=0;
  v_single_source boolean:=false;
  v_line_scope_proven boolean:=false;
  v_scope_unprovable boolean:=false;
  v_credit_ambiguous boolean:=false;
  v_stream_conflict boolean:=false;
  v_archived_active_conflict boolean:=false;
  v_archived_invoice_conflict boolean:=false;
  v_partial_invoice_state boolean:=false;
  v_active_invoice_activity boolean:=false;
  v_paid_mutable_state boolean:=false;
  v_mutable_correction_id text;
  v_mutable_member_ids uuid[]:=array[]::uuid[];
  v_mutable_missing_roles text[]:=array[]::text[];
  v_mutable_fingerprint text;
  v_mutable_parent_id uuid;
  v_m_day numeric:=0;
  v_m_night numeric:=0;
  v_m_sat numeric:=0;
  v_m_sun numeric:=0;
  v_m_bh numeric:=0;
  v_m_pay numeric:=0;
  v_m_charge numeric:=0;
  v_m_margin numeric:=0;
  v_m_financials_complete boolean:=true;
  v_b_standard_representable boolean:=false;
  v_blocking_code text;
  v_reconciliation_fingerprint text;
  v_uuid_re constant text:='^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$';
begin
  if p_import_id is null or jsonb_typeof(coalesce(p_source_items,'null'::jsonb))<>'array' then
    raise exception 'IMPORT_REVIEW_RECONCILIATION_INPUT_INVALID' using errcode='22023';
  end if;
  if p_max_sources not between 1 and 100
     or p_max_invoice_lines_per_source not between 1 and 512
     or p_max_audit_rows_per_source not between 1 and 256
     or p_max_operations_per_source not between 1 and 128 then
    raise exception 'IMPORT_REVIEW_RECONCILIATION_BOUND_INVALID' using errcode='22023';
  end if;
  if jsonb_array_length(p_source_items)>p_max_sources then
    raise exception 'IMPORT_REVIEW_SOURCE_LIMIT_EXCEEDED' using errcode='54000';
  end if;
  if exists (
    select 1 from jsonb_array_elements(p_source_items) s(value)
    group by nullif(btrim(s.value->>'source_identity'),'') having count(*)>1
  ) then
    raise exception 'IMPORT_REVIEW_SOURCE_IDENTITY_DUPLICATE' using errcode='22023';
  end if;

  for v_item in select s.value from jsonb_array_elements(p_source_items) s(value)
  loop
    if jsonb_typeof(v_item)<>'object' then
      raise exception 'IMPORT_REVIEW_RECONCILIATION_SOURCE_INVALID' using errcode='22023';
    end if;
    v_source_identity:=nullif(btrim(v_item->>'source_identity'),'');
    v_source_system:=upper(btrim(coalesce(v_item->>'source_system','')));
    v_external_row_key:=nullif(btrim(v_item->>'external_row_key'),'');
    v_invoice_stream:=upper(btrim(coalesce(v_item->>'invoice_stream','')));
    if v_source_identity is null or v_source_system not in ('NHSP','HEALTHROSTER')
       or v_external_row_key is null or v_invoice_stream not in ('NORMAL','SELF_BILL')
       or coalesce(v_item->>'source_shift_id','')!~*v_uuid_re
       or coalesce(v_item->>'hr_row_id','')!~*v_uuid_re
       or coalesce(v_item->>'source_timesheet_id','')!~*v_uuid_re
       or coalesce(v_item->>'candidate_id','')!~*v_uuid_re
       or coalesce(v_item->>'client_id','')!~*v_uuid_re
       or coalesce(v_item->>'contract_id','')!~*v_uuid_re
       or coalesce(v_item->>'authoritative_import_id','')!~*v_uuid_re
       or coalesce(v_item->>'week_ending_date','')!~'^\d{4}-\d{2}-\d{2}$'
       or jsonb_typeof(v_item->'authoritative_schedule_json')<>'array'
       or jsonb_array_length(v_item->'authoritative_schedule_json')<>1
       or jsonb_typeof(v_item->'authoritative_hours')<>'object' then
      raise exception 'IMPORT_REVIEW_RECONCILIATION_SOURCE_INVALID' using errcode='22023',detail=coalesce(v_source_identity,'missing source_identity');
    end if;
    v_source_shift_id:=(v_item->>'source_shift_id')::uuid;
    v_hr_row_id:=(v_item->>'hr_row_id')::uuid;
    v_source_timesheet_id:=(v_item->>'source_timesheet_id')::uuid;
    v_candidate_id:=(v_item->>'candidate_id')::uuid;
    v_client_id:=(v_item->>'client_id')::uuid;
    v_contract_id:=(v_item->>'contract_id')::uuid;
    v_authoritative_import_id:=(v_item->>'authoritative_import_id')::uuid;
    v_week_ending_date:=(v_item->>'week_ending_date')::date;
    v_a_schedule:=v_item->'authoritative_schedule_json';
    v_a_hours:=jsonb_build_object(
      'hours_day',coalesce((v_item#>>'{authoritative_hours,hours_day}')::numeric,0),
      'hours_night',coalesce((v_item#>>'{authoritative_hours,hours_night}')::numeric,0),
      'hours_sat',coalesce((v_item#>>'{authoritative_hours,hours_sat}')::numeric,0),
      'hours_sun',coalesce((v_item#>>'{authoritative_hours,hours_sun}')::numeric,0),
      'hours_bh',coalesce((v_item#>>'{authoritative_hours,hours_bh}')::numeric,0),
      'total_hours',coalesce((v_item#>>'{authoritative_hours,total_hours}')::numeric,0)
    );
    v_a_fingerprint:=encode(digest(convert_to(concat_ws('|','A-v1',v_source_identity,v_authoritative_import_id,v_a_schedule::text,v_a_hours::text),'UTF8'),'sha256'),'hex');
    v_scope_fingerprint:=encode(digest(convert_to(concat_ws('|','source-scope-v1',v_source_identity,v_source_system,v_source_shift_id,v_external_row_key,v_source_timesheet_id,v_candidate_id,v_client_id,v_contract_id,v_week_ending_date,v_invoice_stream),'UTF8'),'sha256'),'hex');

    perform 1 from public.hr_rows r
    where r.id=v_hr_row_id and r.import_id=p_import_id and r.external_row_key=v_external_row_key;
    if not found then
      raise exception 'IMPORT_REVIEW_RECONCILIATION_SOURCE_INVALID' using errcode='22023',detail=v_source_identity;
    end if;
    perform 1 from public.nhsp_shifts s
    where s.id=v_source_shift_id and s.external_row_key=v_external_row_key
      and upper(s.source_system::text)=v_source_system
      and s.candidate_id=v_candidate_id and s.client_id=v_client_id
      and s.contract_id=v_contract_id and s.week_ending_date=v_week_ending_date;
    if not found then
      raise exception 'IMPORT_REVIEW_RECONCILIATION_SOURCE_INVALID' using errcode='22023',detail=v_source_identity;
    end if;

    select count(*)::integer into v_audit_count
    from public.audit_events ae
    where ae.action in ('NHSP_IMPORT_CORRECTION_APPLIED','HR_IMPORT_CORRECTION_APPLIED')
      and (ae.after_json->>'shift_id'=v_source_shift_id::text
        or ae.after_json->>'external_row_key'=v_external_row_key);
    if v_audit_count>p_max_audit_rows_per_source then
      raise exception 'IMPORT_REVIEW_AUDIT_EVIDENCE_LIMIT_EXCEEDED' using errcode='54000',detail=v_source_identity;
    end if;

    select coalesce(array_agg(distinct candidate_id order by candidate_id),array[]::uuid[])
    into v_audit_ids
    from (
      select candidate_id
      from public.audit_events ae
      cross join lateral unnest(array[
        case when ae.object_type='timesheets' then ae.object_id_text end,
        ae.after_json->>'timesheet_id',
        ae.after_json->>'reversal_timesheet_id',
        ae.after_json->>'replacement_timesheet_id',
        ae.after_json->>'counterpart_timesheet_id'
      ]) raw(candidate_text)
      cross join lateral (select case when raw.candidate_text~*v_uuid_re then raw.candidate_text::uuid end candidate_id) parsed
      where ae.action in ('NHSP_IMPORT_CORRECTION_APPLIED','HR_IMPORT_CORRECTION_APPLIED')
        and (ae.after_json->>'shift_id'=v_source_shift_id::text
          or ae.after_json->>'external_row_key'=v_external_row_key)
        and parsed.candidate_id is not null
    ) candidates;

    select coalesce(array_agg(distinct import_id order by import_id),array[]::uuid[])
    into v_import_ids
    from (
      select p_import_id import_id
      union all select v_authoritative_import_id
      union all select s.latest_import_id from public.nhsp_shifts s where s.id=v_source_shift_id
      union all
      select case when raw.import_text~*v_uuid_re then raw.import_text::uuid end
      from public.audit_events ae
      cross join lateral unnest(array[
        ae.after_json->>'import_id',ae.after_json->>'trigger_import_id',ae.after_json->>'evidence_import_id'
      ]) raw(import_text)
      where ae.action in ('NHSP_IMPORT_CORRECTION_APPLIED','HR_IMPORT_CORRECTION_APPLIED')
        and (ae.after_json->>'shift_id'=v_source_shift_id::text
          or ae.after_json->>'external_row_key'=v_external_row_key)
    ) imports where import_id is not null;
    select count(*)::integer into v_operation_count
    from public.import_apply_operations op where op.import_id=any(v_import_ids);
    if v_operation_count>p_max_operations_per_source then
      raise exception 'IMPORT_REVIEW_OPERATION_EVIDENCE_LIMIT_EXCEEDED' using errcode='54000',detail=v_source_identity;
    end if;

    select coalesce(array_agg(distinct timesheet_id order by timesheet_id),array[]::uuid[])
    into v_hist_ids
    from (
      select v_source_timesheet_id timesheet_id
      union all select s.timesheet_id from public.nhsp_shifts s where s.id=v_source_shift_id
      union all select unnest(v_audit_ids)
      union all
      select t.timesheet_id
      from public.timesheets t
      join public.timesheets_financials tf_scope on tf_scope.timesheet_id=t.timesheet_id
      where tf_scope.candidate_id=v_candidate_id and t.contract_id=v_contract_id and t.week_ending_date=v_week_ending_date
        and (
          (jsonb_typeof(t.actual_schedule_json)='array' and t.actual_schedule_json @> jsonb_build_array(jsonb_build_object('shift_id',v_source_shift_id::text,'external_row_key',v_external_row_key)))
          or t.candidate_hint_text#>>'{correction_financials_policy_envelope,classification,source_shift_id}'=v_source_shift_id::text
          or t.candidate_hint_text#>>'{correction_financials_policy_envelope,classification,source_row_key}'=v_external_row_key
        )
    ) ids where timesheet_id is not null;
    select coalesce(array_agg(t.timesheet_id order by t.timesheet_id),array[]::uuid[])
    into v_archived_ids from public.timesheets t where t.timesheet_id=any(v_hist_ids) and t.archived_at_utc is not null;
    select coalesce(array_agg(t.timesheet_id order by t.timesheet_id),array[]::uuid[])
    into v_active_ids from public.timesheets t where t.timesheet_id=any(v_hist_ids) and t.is_current and t.archived_at_utc is null;
    select coalesce(array_agg(x order by x),array[]::uuid[]) into v_missing_ids
    from unnest(v_audit_ids) x where not exists(select 1 from public.timesheets t where t.timesheet_id=x);

    v_effective_invoice_ids:=array[]::uuid[];
    v_effective_line_ids:=array[]::uuid[];
    v_credit_line_ids:=array[]::uuid[];
    v_effective_component_count:=0;
    v_b_day:=0; v_b_night:=0; v_b_sat:=0; v_b_sun:=0; v_b_bh:=0;
    v_b_pay:=0; v_b_charge:=0; v_b_margin:=0;
    v_b_schedule:='[]'::jsonb; v_candidate_schedule:='[]'::jsonb; v_candidate_hours:='{}'::jsonb;
    select encode(digest(convert_to(coalesce(tf.policy_snapshot_json,'{}'::jsonb)::text,'UTF8'),'sha256'),'hex')
    into v_b_policy_fingerprint
    from public.timesheets_financials tf
    where tf.timesheet_id=v_source_timesheet_id and tf.is_current
    order by tf.computed_at_utc desc nulls last,tf.id desc limit 1;
    v_line_evidence:='[]'::jsonb;
    v_scope_unprovable:=false; v_credit_ambiguous:=false; v_stream_conflict:=false;
    v_archived_invoice_conflict:=false; v_active_invoice_activity:=false;

    with directly_scoped as (
      select il.id
      from public.invoice_lines il
      where il.timesheet_id=any(v_hist_ids)
        or case when coalesce(il.meta_json->>'timesheet_id','')~*v_uuid_re
          then (il.meta_json->>'timesheet_id')::uuid=any(v_hist_ids) else false end
    ), scoped as (
      select il.id
      from public.invoice_lines il where il.id in(select id from directly_scoped)
      union
      select credit.id
      from public.invoice_lines credit
      where coalesce(credit.meta_json->>'original_invoice_line_id',credit.meta_json->>'credit_of_line_id','')~*v_uuid_re
        and coalesce(credit.meta_json->>'original_invoice_line_id',credit.meta_json->>'credit_of_line_id')::uuid in(select id from directly_scoped)
    )
    select count(*)::integer into v_line_count from scoped;
    if v_line_count>p_max_invoice_lines_per_source then
      raise exception 'IMPORT_REVIEW_INVOICE_EVIDENCE_LIMIT_EXCEEDED' using errcode='54000',detail=v_source_identity;
    end if;

    for v_line in
      with directly_scoped as (
        select il.id
        from public.invoice_lines il
        where il.timesheet_id=any(v_hist_ids)
          or case when coalesce(il.meta_json->>'timesheet_id','')~*v_uuid_re
            then (il.meta_json->>'timesheet_id')::uuid=any(v_hist_ids) else false end
      ), scoped as (
        select il.id from public.invoice_lines il where il.id in(select id from directly_scoped)
        union
        select credit.id from public.invoice_lines credit
        where coalesce(credit.meta_json->>'original_invoice_line_id',credit.meta_json->>'credit_of_line_id','')~*v_uuid_re
          and coalesce(credit.meta_json->>'original_invoice_line_id',credit.meta_json->>'credit_of_line_id')::uuid in(select id from directly_scoped)
      )
      select il.*,i.type::text invoice_type,i.status::text invoice_status,i.issued_at_utc,
        i.original_invoice_id,i.active_document_operation_id,i.active_issue_operation_id,i.issue_state
      from scoped s join public.invoice_lines il on il.id=s.id join public.invoices i on i.id=il.invoice_id
      order by i.issued_at_utc nulls last,il.id
    loop
      if v_line.invoice_status='DRAFT' or v_line.issued_at_utc is null
         or v_line.active_document_operation_id is not null or v_line.active_issue_operation_id is not null
         or upper(coalesce(v_line.issue_state,'')) not in ('','IDLE','COMPLETE','COMPLETED','ISSUED') then
        v_active_invoice_activity:=true;
      end if;
      if v_line.invoice_status not in ('ISSUED','PAID','ON_HOLD') or v_line.issued_at_utc is null then
        continue;
      end if;
      if v_line.invoice_type='CREDIT_NOTE' and (
        select count(*) from public.invoice_lines other_credit
        join public.invoices other_credit_invoice on other_credit_invoice.id=other_credit.invoice_id
        where coalesce(other_credit.meta_json->>'original_invoice_line_id',other_credit.meta_json->>'credit_of_line_id','')
          =coalesce(v_line.meta_json->>'original_invoice_line_id',v_line.meta_json->>'credit_of_line_id','')
          and other_credit_invoice.type='CREDIT_NOTE' and other_credit_invoice.status in ('ISSUED','PAID','ON_HOLD')
          and other_credit_invoice.issued_at_utc is not null
      )>1 then
        v_credit_ambiguous:=true;
      end if;
      if v_line.timesheet_id=any(v_archived_ids)
         or (coalesce(v_line.meta_json->>'timesheet_id','')~*v_uuid_re and (v_line.meta_json->>'timesheet_id')::uuid=any(v_archived_ids)) then
        v_archived_invoice_conflict:=true;
      end if;

      v_tf:=null; v_seg:=null; v_seg_count:=0; v_matching_seg_count:=0;
      if coalesce(v_line.meta_json->>'tsfin_id','')~*v_uuid_re then
        select tf.* into v_tf from public.timesheets_financials tf where tf.id=(v_line.meta_json->>'tsfin_id')::uuid;
      elsif v_line.timesheet_id is not null then
        select tf.* into v_tf from public.timesheets_financials tf
        where tf.timesheet_id=v_line.timesheet_id
        order by case when tf.is_current then 0 else 1 end,tf.computed_at_utc desc nulls last,tf.id desc limit 1;
      end if;
      if v_tf.id is not null then
        select count(*)::integer,
          count(*) filter(where seg->>'nhsp_shift_id'=v_source_shift_id::text or seg->>'shift_id'=v_source_shift_id::text or seg->>'external_row_key'=v_external_row_key)::integer,
          (array_agg(seg order by case when seg->>'nhsp_shift_id'=v_source_shift_id::text or seg->>'shift_id'=v_source_shift_id::text then 0 else 1 end)
            filter(where seg->>'nhsp_shift_id'=v_source_shift_id::text or seg->>'shift_id'=v_source_shift_id::text or seg->>'external_row_key'=v_external_row_key))[1]
        into v_seg_count,v_matching_seg_count,v_seg
        from jsonb_array_elements(case when jsonb_typeof(v_tf.invoice_breakdown_json->'segments')='array' then v_tf.invoice_breakdown_json->'segments' else '[]'::jsonb end) seg;
      end if;
      v_single_source:=v_matching_seg_count=1 and v_seg_count=1;
      if not v_single_source and v_line.timesheet_id is not null then
        select jsonb_typeof(t.actual_schedule_json)='array' and jsonb_array_length(t.actual_schedule_json)=1
          and t.actual_schedule_json @> jsonb_build_array(jsonb_build_object('shift_id',v_source_shift_id::text,'external_row_key',v_external_row_key))
        into v_single_source from public.timesheets t where t.timesheet_id=v_line.timesheet_id;
        v_single_source:=coalesce(v_single_source,false);
      end if;
      v_line_scope_proven:=v_single_source or v_matching_seg_count=1;
      if not v_line_scope_proven or (v_line.invoice_type='CREDIT_NOTE' and not v_single_source) then
        v_scope_unprovable:=true;
        continue;
      end if;
      if v_tf.id is not null and (case when upper(coalesce(v_tf.basis::text,'')) in ('NHSP','NHSP_ADJUSTMENT','HEALTHROSTER_SELF_BILL','HEALTHROSTER_ADJUSTMENT') then 'SELF_BILL' else 'NORMAL' end)<>v_invoice_stream then
        v_stream_conflict:=true;
        continue;
      end if;

      if v_single_source then
        v_component_day:=coalesce(v_line.hours_day,0); v_component_night:=coalesce(v_line.hours_night,0);
        v_component_sat:=coalesce(v_line.hours_sat,0); v_component_sun:=coalesce(v_line.hours_sun,0); v_component_bh:=coalesce(v_line.hours_bh,0);
        v_component_pay:=coalesce(v_line.total_pay_ex_vat,0); v_component_charge:=coalesce(v_line.total_charge_ex_vat,0); v_component_margin:=coalesce(v_line.margin_ex_vat,v_component_charge-v_component_pay);
      else
        v_component_day:=coalesce((v_seg->>'hours_day')::numeric,0); v_component_night:=coalesce((v_seg->>'hours_night')::numeric,0);
        v_component_sat:=coalesce((v_seg->>'hours_sat')::numeric,0); v_component_sun:=coalesce((v_seg->>'hours_sun')::numeric,0); v_component_bh:=coalesce((v_seg->>'hours_bh')::numeric,0);
        v_component_pay:=coalesce((v_seg->>'pay_amount')::numeric,0); v_component_charge:=coalesce((v_seg->>'charge_amount')::numeric,0); v_component_margin:=v_component_charge-v_component_pay;
      end if;
      v_b_day:=v_b_day+v_component_day; v_b_night:=v_b_night+v_component_night; v_b_sat:=v_b_sat+v_component_sat; v_b_sun:=v_b_sun+v_component_sun; v_b_bh:=v_b_bh+v_component_bh;
      v_b_pay:=v_b_pay+v_component_pay; v_b_charge:=v_b_charge+v_component_charge; v_b_margin:=v_b_margin+v_component_margin;
      v_effective_component_count:=v_effective_component_count+1;
      v_effective_invoice_ids:=array_append(v_effective_invoice_ids,v_line.invoice_id);
      v_effective_line_ids:=array_append(v_effective_line_ids,v_line.id);
      if v_line.invoice_type='CREDIT_NOTE' then v_credit_line_ids:=array_append(v_credit_line_ids,v_line.id); end if;
      v_line_evidence:=v_line_evidence||jsonb_build_array(jsonb_build_object(
        'invoice_id',v_line.invoice_id,'invoice_line_id',v_line.id,'invoice_type',v_line.invoice_type,
        'hours',jsonb_build_object('hours_day',v_component_day,'hours_night',v_component_night,'hours_sat',v_component_sat,'hours_sun',v_component_sun,'hours_bh',v_component_bh),
        'pay_ex_vat',v_component_pay,'charge_ex_vat',v_component_charge,'margin_ex_vat',v_component_margin));
      if v_seg is not null and (v_component_day+v_component_night+v_component_sat+v_component_sun+v_component_bh)>0 then
        v_candidate_schedule:=jsonb_build_array(jsonb_strip_nulls(jsonb_build_object(
          'date',coalesce(v_seg->>'date',(v_a_schedule->0)->>'date'),
          'start_utc',v_seg->>'start_utc','end_utc',v_seg->>'end_utc',
          'break_mins',coalesce((v_seg->>'break_mins')::integer,0),
          'shift_id',v_source_shift_id::text,'external_row_key',v_external_row_key,
          'import_id',coalesce(v_seg->>'import_id',v_authoritative_import_id::text),
          'ref_num',coalesce(v_seg->>'ref_num',v_seg->>'reference_number',(v_a_schedule->0)->>'ref_num')
        )));
        v_candidate_hours:=jsonb_build_object('hours_day',v_component_day,'hours_night',v_component_night,'hours_sat',v_component_sat,'hours_sun',v_component_sun,'hours_bh',v_component_bh,'total_hours',v_component_day+v_component_night+v_component_sat+v_component_sun+v_component_bh);
        v_b_policy_fingerprint:=coalesce(v_tf.policy_snapshot_json->>'correction_financials_policy_envelope_fingerprint',v_tf.policy_snapshot_json#>>'{correction_financials_policy_envelope,envelope_fingerprint}',encode(digest(convert_to(coalesce(v_tf.policy_snapshot_json,'{}'::jsonb)::text,'UTF8'),'sha256'),'hex'));
      end if;
    end loop;

    select coalesce(array_agg(distinct x order by x),array[]::uuid[]) into v_effective_invoice_ids from unnest(v_effective_invoice_ids) x;
    select coalesce(array_agg(distinct x order by x),array[]::uuid[]) into v_effective_line_ids from unnest(v_effective_line_ids) x;
    select coalesce(array_agg(distinct x order by x),array[]::uuid[]) into v_credit_line_ids from unnest(v_credit_line_ids) x;
    v_effective_fingerprint:=encode(digest(convert_to(concat_ws('|','effective-invoice-v1',v_source_identity,v_line_evidence::text,v_b_day,v_b_night,v_b_sat,v_b_sun,v_b_bh,v_b_pay,v_b_charge,v_b_margin),'UTF8'),'sha256'),'hex');

    v_archived_active_conflict:=exists(
      select 1 from public.timesheets a join public.timesheets b on b.correction_id=a.correction_id
      where a.timesheet_id=any(v_hist_ids) and b.timesheet_id=any(v_hist_ids)
        and a.correction_id is not null and a.archived_at_utc is not null
        and b.is_current and b.archived_at_utc is null
    );
    v_partial_invoice_state:=exists(
      with units as (
        select t.correction_id,
          count(*) filter(where t.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')) roles,
          count(*) filter(where exists(select 1 from public.invoice_lines il join public.invoices i on i.id=il.invoice_id
            where (il.timesheet_id=t.timesheet_id or il.meta_json->>'timesheet_id'=t.timesheet_id::text)
              and i.status in ('ISSUED','PAID','ON_HOLD') and i.issued_at_utc is not null)) effective_roles,
          count(*) filter(where exists(select 1 from public.invoice_lines il join public.invoices i on i.id=il.invoice_id
            where (il.timesheet_id=t.timesheet_id or il.meta_json->>'timesheet_id'=t.timesheet_id::text)
              and (i.status='DRAFT' or i.issued_at_utc is null))) draft_roles,
          count(*) filter(where tf.locked_by_invoice_id is not null or upper(coalesce(cw.status::text,''))='INVOICED'
            or exists(select 1 from jsonb_array_elements(case when jsonb_typeof(tf.invoice_breakdown_json->'segments')='array' then tf.invoice_breakdown_json->'segments' else '[]'::jsonb end) seg
              where nullif(seg->>'invoice_locked_invoice_id','') is not null)) locked_roles
        from public.timesheets t
        left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
        left join public.contract_weeks cw on cw.timesheet_id=t.timesheet_id
        where t.timesheet_id=any(v_hist_ids) and t.correction_id is not null
          and t.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
        group by t.correction_id
      ) select 1 from units where effective_roles=1 or draft_roles=1 or locked_roles=1
        or (effective_roles>0 and effective_roles<roles) or (draft_roles>0 and draft_roles<roles) or (locked_roles>0 and locked_roles<roles)
    );

    select q.correction_id into v_mutable_correction_id
    from (
      select t.correction_id,max(coalesce(t.updated_at,t.created_at)) changed_at
      from public.timesheets t
      left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
      left join public.contract_weeks cw on cw.timesheet_id=t.timesheet_id
      where t.timesheet_id=any(v_hist_ids) and t.correction_id is not null and t.is_current and t.archived_at_utc is null
        and t.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
        and not exists(select 1 from public.invoice_lines il where il.timesheet_id=t.timesheet_id or il.meta_json->>'timesheet_id'=t.timesheet_id::text)
        and tf.locked_by_invoice_id is null and upper(coalesce(cw.status::text,''))<>'INVOICED'
        and not exists(select 1 from jsonb_array_elements(case when jsonb_typeof(tf.invoice_breakdown_json->'segments')='array' then tf.invoice_breakdown_json->'segments' else '[]'::jsonb end) seg where nullif(seg->>'invoice_locked_invoice_id','') is not null)
      group by t.correction_id
    ) q order by q.changed_at desc,q.correction_id limit 1;
    if v_mutable_correction_id is null then
      select ae.after_json->>'correction_id' into v_mutable_correction_id
      from public.audit_events ae
      where ae.action in ('NHSP_IMPORT_CORRECTION_APPLIED','HR_IMPORT_CORRECTION_APPLIED')
        and (ae.after_json->>'shift_id'=v_source_shift_id::text or ae.after_json->>'external_row_key'=v_external_row_key)
        and nullif(ae.after_json->>'correction_id','') is not null
        and not exists(select 1 from public.timesheets t where t.correction_id=ae.after_json->>'correction_id')
        and not exists(select 1 from public.invoice_lines il where il.meta_json->>'correction_id'=ae.after_json->>'correction_id')
      order by ae.ts_utc desc,ae.id desc limit 1;
    end if;

    v_mutable_member_ids:=array[]::uuid[]; v_mutable_missing_roles:=array[]::text[];
    v_mutable_parent_id:=null; v_m_day:=0; v_m_night:=0; v_m_sat:=0; v_m_sun:=0; v_m_bh:=0;
    v_m_pay:=0; v_m_charge:=0; v_m_margin:=0; v_m_financials_complete:=true; v_paid_mutable_state:=false;
    if v_mutable_correction_id is not null then
      select coalesce(array_agg(t.timesheet_id order by t.correction_kind,t.timesheet_id),array[]::uuid[]),
        (array_agg(t.parent_timesheet_id order by t.created_at,t.timesheet_id))[1],
        coalesce(sum(tf.hours_day),0),coalesce(sum(tf.hours_night),0),coalesce(sum(tf.hours_sat),0),coalesce(sum(tf.hours_sun),0),coalesce(sum(tf.hours_bh),0),
        coalesce(sum(tf.total_pay_ex_vat),0),coalesce(sum(tf.total_charge_ex_vat),0),coalesce(sum(tf.margin_ex_vat),0),
        count(*)=count(tf.id) and bool_and(not coalesce(tf.is_stale,true) and not coalesce(tf.has_rate_issue,false) and not coalesce(tf.has_pay_channel_issue,false)),
        bool_or(tf.paid_at_utc is not null)
      into v_mutable_member_ids,v_mutable_parent_id,v_m_day,v_m_night,v_m_sat,v_m_sun,v_m_bh,v_m_pay,v_m_charge,v_m_margin,v_m_financials_complete,v_paid_mutable_state
      from public.timesheets t left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
      where t.correction_id=v_mutable_correction_id and t.is_current and t.archived_at_utc is null
        and t.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT');
      if not exists(select 1 from public.timesheets t where t.correction_id=v_mutable_correction_id and t.is_current and t.archived_at_utc is null and t.correction_kind='CHANGED_HOURS_REVERSAL') then
        v_mutable_missing_roles:=array_append(v_mutable_missing_roles,'CHANGED_HOURS_REVERSAL');
      end if;
      if not exists(select 1 from public.timesheets t where t.correction_id=v_mutable_correction_id and t.is_current and t.archived_at_utc is null and t.correction_kind='CHANGED_HOURS_REPLACEMENT') then
        v_mutable_missing_roles:=array_append(v_mutable_missing_roles,'CHANGED_HOURS_REPLACEMENT');
      end if;
    end if;
    v_mutable_fingerprint:=encode(digest(convert_to(concat_ws('|','mutable-v1',v_mutable_correction_id,v_mutable_member_ids::text,v_mutable_missing_roles::text,v_m_day,v_m_night,v_m_sat,v_m_sun,v_m_bh,v_m_pay,v_m_charge,v_m_margin),'UTF8'),'sha256'),'hex');

    v_b_standard_representable:=(v_b_day+v_b_night+v_b_sat+v_b_sun+v_b_bh)=0
      or (v_b_day>=0 and v_b_night>=0 and v_b_sat>=0 and v_b_sun>=0 and v_b_bh>=0
        and coalesce((v_candidate_hours->>'hours_day')::numeric,0)=v_b_day
        and coalesce((v_candidate_hours->>'hours_night')::numeric,0)=v_b_night
        and coalesce((v_candidate_hours->>'hours_sat')::numeric,0)=v_b_sat
        and coalesce((v_candidate_hours->>'hours_sun')::numeric,0)=v_b_sun
        and coalesce((v_candidate_hours->>'hours_bh')::numeric,0)=v_b_bh
        and jsonb_array_length(v_candidate_schedule)=1);
    if v_b_standard_representable and (v_b_day+v_b_night+v_b_sat+v_b_sun+v_b_bh)>0 then v_b_schedule:=v_candidate_schedule; end if;

    v_blocking_code:=case
      when v_archived_invoice_conflict then 'IMPORT_REVIEW_ARCHIVED_INVOICE_STATE_CONFLICT'
      when v_archived_active_conflict then 'IMPORT_REVIEW_ARCHIVED_GENERATION_ACTIVE_MEMBER_CONFLICT'
      when v_partial_invoice_state then 'IMPORT_REVIEW_CORRECTION_GENERATION_PARTIALLY_INVOICED'
      when v_active_invoice_activity then 'IMPORT_REVIEW_INVOICE_ACTIVITY_IN_PROGRESS'
      when v_credit_ambiguous then 'IMPORT_REVIEW_EFFECTIVE_CREDIT_AMBIGUOUS'
      when v_scope_unprovable or v_stream_conflict then 'IMPORT_REVIEW_INVOICE_COMPONENT_SCOPE_UNPROVABLE'
      when v_paid_mutable_state then 'IMPORT_REVIEW_PAID_MUTABLE_GENERATION_ROLLOVER_UNAVAILABLE'
      when (v_b_day+v_b_night+v_b_sat+v_b_sun+v_b_bh)=0 and v_mutable_correction_id is not null then 'IMPORT_REVIEW_ZERO_EFFECTIVE_POSITION_HAS_ACTIVE_CORRECTION_GENERATION'
      when (v_b_day+v_b_night+v_b_sat+v_b_sun+v_b_bh)>0 and not v_b_standard_representable then 'IMPORT_REVIEW_EFFECTIVE_POSITION_NOT_STANDARD_REPRESENTABLE'
      else null end;
    v_reconciliation_fingerprint:=encode(digest(convert_to(concat_ws('|','reconciliation-v1',v_scope_fingerprint,v_effective_fingerprint,v_mutable_fingerprint,v_a_fingerprint,v_blocking_code,v_b_policy_fingerprint),'UTF8'),'sha256'),'hex');

    source_identity:=v_source_identity;
    balance_json:=jsonb_build_object(
      'schema_version','IMPORT_AUTHORITATIVE_RECONCILIATION_BALANCE_V1',
      'source_identity',v_source_identity,'source_system',v_source_system,'source_shift_id',v_source_shift_id,
      'external_row_key',v_external_row_key,'source_timesheet_id',v_source_timesheet_id,
      'candidate_id',v_candidate_id,'client_id',v_client_id,'contract_id',v_contract_id,
      'week_ending_date',v_week_ending_date,'invoice_stream',v_invoice_stream,
      'source_scope_fingerprint',v_scope_fingerprint,
      'archived_timesheet_ids',to_jsonb(v_archived_ids),'active_timesheet_ids',to_jsonb(v_active_ids),
      'historical_missing_timesheet_ids',to_jsonb(v_missing_ids),
      'effective_invoice_ids',to_jsonb(v_effective_invoice_ids),'effective_invoice_line_ids',to_jsonb(v_effective_line_ids),
      'effective_credit_line_ids',to_jsonb(v_credit_line_ids),'effective_invoice_component_count',v_effective_component_count,
      'effective_invoice_fingerprint',v_effective_fingerprint,
      'B_hours',jsonb_build_object('hours_day',v_b_day,'hours_night',v_b_night,'hours_sat',v_b_sat,'hours_sun',v_b_sun,'hours_bh',v_b_bh,'total_hours',v_b_day+v_b_night+v_b_sat+v_b_sun+v_b_bh),
      'B_financials',jsonb_build_object('pay_ex_vat',v_b_pay,'charge_ex_vat',v_b_charge,'margin_ex_vat',v_b_margin),
      'B_standard_schedule_json',v_b_schedule,'B_policy_fingerprint',v_b_policy_fingerprint,'B_standard_representable',v_b_standard_representable,
      'active_mutable_generation',v_mutable_correction_id is not null,'active_mutable_member_ids',to_jsonb(v_mutable_member_ids),
      'active_mutable_missing_roles',to_jsonb(v_mutable_missing_roles),'active_mutable_correction_id',v_mutable_correction_id,
      'active_mutable_parent_timesheet_id',v_mutable_parent_id,'active_mutable_fingerprint',v_mutable_fingerprint,
      'M_hours',jsonb_build_object('hours_day',v_m_day,'hours_night',v_m_night,'hours_sat',v_m_sat,'hours_sun',v_m_sun,'hours_bh',v_m_bh,'total_hours',v_m_day+v_m_night+v_m_sat+v_m_sun+v_m_bh),
      'M_existing_financials',jsonb_build_object('pay_ex_vat',v_m_pay,'charge_ex_vat',v_m_charge,'margin_ex_vat',v_m_margin),'M_financials_complete',v_m_financials_complete,
      'A_schedule_json',v_a_schedule,'A_hours',v_a_hours,'A_evidence_fingerprint',v_a_fingerprint,
      'partial_invoice_state',v_partial_invoice_state,'active_invoice_activity',v_active_invoice_activity,
      'archived_active_conflict',v_archived_active_conflict,'archived_invoice_conflict',v_archived_invoice_conflict,
      'paid_mutable_state',v_paid_mutable_state,
      'recommended_route_inputs',jsonb_build_object('B_positive',(v_b_day+v_b_night+v_b_sat+v_b_sun+v_b_bh)>0,'has_mutable_generation',v_mutable_correction_id is not null,'source_timesheet_active',v_source_timesheet_id=any(v_active_ids)),
      'blocking_code',v_blocking_code,'reconciliation_fingerprint',v_reconciliation_fingerprint
    );
    return next;
  end loop;
end
$function$;

commit;

