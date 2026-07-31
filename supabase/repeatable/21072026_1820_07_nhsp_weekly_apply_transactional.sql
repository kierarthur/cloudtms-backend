-- CloudTMS reviewed direct replacement; installed and verified in TEST on 21 July 2026.
-- Exact TEST baseline body MD5 prefix: df760b6e8a2c.
-- Hard cutover: every call requires the server-owned import review contract.
CREATE OR REPLACE FUNCTION public.nhsp_weekly_apply_transactional(p_import_id uuid, p_payload jsonb, p_actor_user_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
 SET plpgsql_check.mode TO 'disabled'
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
  v_paid_unit jsonb;
  v_paid_timesheet_id uuid;
  v_paid_intent text;
  v_paid_current_count integer := 0;
  v_paid_current_tf public.timesheets_financials%rowtype;
  v_paid_preflight jsonb;
  v_paid_rollover jsonb;
  v_paid_mode text;
  v_paid_historical_id uuid;
  v_paid_shell_id uuid;
  v_paid_applied jsonb;
  v_paid_applied_timesheet_ids uuid[] := array[]::uuid[];
  v_paid_current_contract jsonb;
  v_paid_current_contract_fingerprint text;
  v_paid_current_policy_unit jsonb;
  v_paid_current_policy_count integer := 0;
  v_paid_current_policy jsonb;
  v_paid_origin_operation public.import_apply_operations%rowtype;
  v_paid_origin_contract jsonb;
  v_paid_origin_contract_fingerprint text;
  v_paid_origin_policy_unit jsonb;
  v_paid_origin_policy_count integer := 0;
  v_paid_historical_tf public.timesheets_financials%rowtype;
  v_paid_digest text;
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
      coalesce(array_agg(u.action_id order by u.action_id) filter(where u.route in ('AMEND_PAID_UNINVOICED_SOURCE','AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT')),array[]::text[]),
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
      and (cs.is_paid is false or exists(select 1 from pg_temp.import_review_reconciliation_units_v1 paid_unit
        where paid_unit.source_identity=cs.external_row_key
          and paid_unit.route='AMEND_PAID_UNINVOICED_SOURCE'
          and paid_unit.unit_json->>'intended_authorisation_action'='REAUTHORISE'))
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

  -- Execute the reviewed ordinary paid-but-uninvoiced route before source
  -- truth is amended. Paid status alone never routes a source through phase 3.
  for v_paid_unit in
    select u.unit_json from pg_temp.import_review_reconciliation_units_v1 u
    where u.route='AMEND_PAID_UNINVOICED_SOURCE'
    order by u.source_timesheet_id
  loop
    v_paid_timesheet_id:=nullif(v_paid_unit->>'source_timesheet_id','')::uuid;
    v_paid_intent:=v_paid_unit->>'intended_authorisation_action';
    if v_paid_timesheet_id is null
       or v_paid_intent not in ('REAUTHORISE','AUTHORISE','LEAVE_UNAUTHORISED') then
      raise exception 'IMPORT_REVIEW_RECONCILIATION_UNIT_INVALID' using errcode='22023';
    end if;

    select operation_row.response_json#>'{correction_operation_contract}'
    into v_paid_current_contract from public.import_apply_operations operation_row
    where operation_row.id=v_review_operation_id;
    v_paid_current_contract_fingerprint:=encode(extensions.digest(convert_to(
      (v_paid_current_contract-'operation_contract_fingerprint')::text,'UTF8'),'sha256'),'hex');
    if jsonb_typeof(v_paid_current_contract)<>'object'
       or v_paid_current_contract->>'operation_contract_fingerprint' is distinct from v_paid_current_contract_fingerprint then
      raise exception 'PAID_TSFIN_ROLLOVER_ORDINARY_SOURCE_POLICY_INVALID' using errcode='P0001';
    end if;
    select count(*)::integer,min(policy_unit::text)::jsonb
    into v_paid_current_policy_count,v_paid_current_policy_unit
    from jsonb_array_elements(coalesce(v_paid_current_contract->'correction_units','[]'::jsonb)) policy_unit
    where policy_unit->>'action_id'=v_paid_unit->>'action_id'
      and policy_unit->>'root_timesheet_id'=v_paid_timesheet_id::text
      and policy_unit->>'source_row_key'=v_paid_unit->>'source_identity';
    if v_paid_current_policy_count<>1
       or jsonb_typeof(v_paid_current_policy_unit->'policy_envelope')<>'object' then
      raise exception 'PAID_TSFIN_ROLLOVER_ORDINARY_SOURCE_POLICY_INVALID' using errcode='P0001';
    end if;
    v_paid_current_policy:=v_paid_current_policy_unit->'policy_envelope';

    perform 1 from public.timesheets exact_source
    where exact_source.timesheet_id=v_paid_timesheet_id
      and exact_source.is_current and exact_source.archived_at_utc is null for update;
    if not found then
      raise exception 'IMPORT_REVIEW_SELECTED_ACTION_STALE' using errcode='40001';
    end if;
    perform 1 from public.timesheets_financials current_lock
    where current_lock.timesheet_id=v_paid_timesheet_id and current_lock.is_current
    order by current_lock.id for update;
    select count(*)::integer into v_paid_current_count from public.timesheets_financials current_tf
    where current_tf.timesheet_id=v_paid_timesheet_id and current_tf.is_current;
    if v_paid_current_count<>1 then
      raise exception 'IMPORT_REVIEW_SELECTED_ACTION_STALE' using errcode='40001';
    end if;
    select current_tf.* into v_paid_current_tf from public.timesheets_financials current_tf
    where current_tf.timesheet_id=v_paid_timesheet_id and current_tf.is_current;

    if v_paid_current_tf.paid_at_utc is not null then
      v_paid_preflight:=public.import_timesheet_financial_preflight_v1(
        array[v_paid_timesheet_id]::uuid[],'PAID_UNINVOICED_ROLLOVER',p_actor_user_id,
        '{}'::jsonb,false,100);
      if coalesce((v_paid_preflight->>'allowed')::boolean,false) is not true
         or v_paid_preflight->>'required_path' is distinct from 'PAID_UNINVOICED_ROLLOVER'
         or coalesce((v_paid_preflight->>'input_count')::integer,0)<>1
         or coalesce((v_paid_preflight->>'member_count')::integer,0)<>1
         or coalesce((v_paid_preflight->>'paid_count')::integer,0)<>1
         or coalesce((v_paid_preflight->>'invoice_lined_count')::integer,0)<>0
         or coalesce((v_paid_preflight->>'blocking_batch_count')::integer,0)<>0
         or coalesce((v_paid_preflight->>'stale_tsfin_count')::integer,0)<>0
         or jsonb_array_length(coalesce(v_paid_preflight->'errors','[]'::jsonb))<>0 then
        raise exception 'PAID_TSFIN_ROLLOVER_ORDINARY_SOURCE_PREFLIGHT_INVALID' using errcode='P0001';
      end if;
      v_paid_rollover:=public.timesheet_paid_uninvoiced_rollover_v1(
        v_paid_timesheet_id,p_actor_user_id,v_review_operation_id,v_paid_current_tf.id,
        v_paid_preflight->>'preflight_fingerprint',v_now);
      v_paid_historical_id:=nullif(v_paid_rollover->>'historical_paid_tsfin_id','')::uuid;
      v_paid_shell_id:=nullif(v_paid_rollover->>'new_current_tsfin_id','')::uuid;
      v_paid_mode:='CREATED_CURRENT_OPERATION_SHELL';
      if coalesce((v_paid_rollover->>'ok')::boolean,false) is not true
         or v_paid_historical_id is distinct from v_paid_current_tf.id
         or v_paid_shell_id is null
         or not exists(select 1 from public.timesheets_financials shell
           where shell.id=v_paid_shell_id and shell.timesheet_id=v_paid_timesheet_id and shell.is_current
             and shell.paid_at_utc is null and shell.processing_status='PENDING_AUTH'::public.ts_fin_processing_status_enum
             and shell.stale_reason='IMPORT_PAID_TSFIN_ROLLOVER_PENDING_CALCULATION'
             and coalesce((shell.policy_snapshot_json->>'requires_frozen_correction_policy')::boolean,false)
             and shell.policy_snapshot_json->>'import_apply_operation_id'=v_review_operation_id::text
             and shell.policy_snapshot_json->>'rollover_source_tsfin_id'=v_paid_historical_id::text)
         or (select count(*) from public.timesheets_financials shell
           where shell.timesheet_id=v_paid_timesheet_id and shell.is_current)<>1 then
        raise exception 'PAID_TSFIN_ROLLOVER_ORDINARY_SOURCE_PREFLIGHT_INVALID' using errcode='P0001';
      end if;
    else
      -- Reuse only a settled shell from a completed prior operation whose own
      -- frozen contract and paid lineage prove it, then compare only stable
      -- policy facts with this operation.
      if v_paid_current_tf.locked_by_invoice_id is not null
         or exists(select 1 from public.invoice_lines il where il.timesheet_id=v_paid_timesheet_id)
         or v_paid_current_tf.authorised_at_utc is not null
         or v_paid_current_tf.processing_status<>'PENDING_AUTH'::public.ts_fin_processing_status_enum
         or coalesce(v_paid_current_tf.is_stale,false)
         or v_paid_current_tf.stale_reason is not null
         or not coalesce((v_paid_current_tf.policy_snapshot_json->>'requires_frozen_correction_policy')::boolean,false)
         or coalesce(v_paid_current_tf.policy_snapshot_json->>'import_apply_operation_id','')!~*'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
         or coalesce(v_paid_current_tf.policy_snapshot_json->>'rollover_source_tsfin_id','')!~*'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
         or nullif(v_paid_current_tf.policy_snapshot_json->>'rollover_source_paid_digest','') is null then
        raise exception 'IMPORT_REVIEW_PAID_ROLLOVER_SHELL_INVALID' using errcode='P0001';
      end if;
      select origin.* into v_paid_origin_operation from public.import_apply_operations origin
      where origin.id=(v_paid_current_tf.policy_snapshot_json->>'import_apply_operation_id')::uuid for update;
      if not found or v_paid_origin_operation.state<>'COMPLETE' then
        raise exception 'IMPORT_REVIEW_PAID_ROLLOVER_SHELL_ORIGIN_INCOMPLETE' using errcode='P0001';
      end if;
      v_paid_origin_contract:=v_paid_origin_operation.response_json#>'{correction_operation_contract}';
      v_paid_origin_contract_fingerprint:=encode(extensions.digest(convert_to(
        (v_paid_origin_contract-'operation_contract_fingerprint')::text,'UTF8'),'sha256'),'hex');
      if jsonb_typeof(v_paid_origin_contract)<>'object'
         or v_paid_origin_contract->>'operation_contract_fingerprint' is distinct from v_paid_origin_contract_fingerprint then
        raise exception 'IMPORT_REVIEW_PAID_ROLLOVER_SHELL_INVALID' using errcode='P0001';
      end if;
      select count(*)::integer,min(origin_unit::text)::jsonb
      into v_paid_origin_policy_count,v_paid_origin_policy_unit
      from jsonb_array_elements(coalesce(v_paid_origin_contract->'correction_units','[]'::jsonb)) origin_unit
      where origin_unit->>'root_timesheet_id'=v_paid_timesheet_id::text
        and origin_unit->>'source_row_key'=v_paid_unit->>'source_identity';
      if v_paid_origin_policy_count<>1
         or v_paid_origin_policy_unit->'policy_envelope' is distinct from
           v_paid_current_tf.policy_snapshot_json->'correction_financials_policy_envelope'
         or v_paid_origin_policy_unit->>'policy_envelope_fingerprint' is distinct from
           v_paid_current_tf.policy_snapshot_json->>'correction_financials_policy_envelope_fingerprint' then
        raise exception 'IMPORT_REVIEW_PAID_ROLLOVER_SHELL_INVALID' using errcode='P0001';
      end if;
      select historical.* into v_paid_historical_tf from public.timesheets_financials historical
      where historical.id=(v_paid_current_tf.policy_snapshot_json->>'rollover_source_tsfin_id')::uuid
        and historical.timesheet_id=v_paid_timesheet_id and not historical.is_current
        and historical.paid_at_utc is not null and historical.locked_by_invoice_id is null;
      if not found then raise exception 'IMPORT_REVIEW_PAID_ROLLOVER_SHELL_INVALID' using errcode='P0001'; end if;
      v_paid_digest:=encode(extensions.digest(convert_to(jsonb_build_object(
        'id',v_paid_historical_tf.id::text,'timesheet_id',v_paid_historical_tf.timesheet_id::text,
        'timesheet_version',v_paid_historical_tf.timesheet_version,'paid_at_utc',v_paid_historical_tf.paid_at_utc,
        'paid_by_user_id',case when v_paid_historical_tf.paid_by_user_id is null then null else v_paid_historical_tf.paid_by_user_id::text end,
        'payment_reference',v_paid_historical_tf.payment_reference,'total_hours',v_paid_historical_tf.total_hours,
        'total_pay_ex_vat',v_paid_historical_tf.total_pay_ex_vat,'total_charge_ex_vat',v_paid_historical_tf.total_charge_ex_vat,
        'pay_vat_rate_pct_snapshot',v_paid_historical_tf.pay_vat_rate_pct_snapshot,
        'pay_vat_amount_snapshot',v_paid_historical_tf.pay_vat_amount_snapshot,
        'pay_total_inc_vat_snapshot',v_paid_historical_tf.pay_total_inc_vat_snapshot,
        'policy_snapshot_json',v_paid_historical_tf.policy_snapshot_json,'rate_source_refs_json',v_paid_historical_tf.rate_source_refs_json,
        'actual_schedule_json',v_paid_historical_tf.actual_schedule_json)::text,'UTF8'),'sha256'),'hex');
      if v_paid_digest is distinct from v_paid_current_tf.policy_snapshot_json->>'rollover_source_paid_digest' then
        raise exception 'IMPORT_REVIEW_PAID_ROLLOVER_SHELL_INVALID' using errcode='P0001';
      end if;
      if v_paid_origin_contract->>'source_system' is distinct from v_paid_current_contract->>'source_system'
         or v_paid_origin_policy_unit#>>'{policy_envelope,classification,source_shift_id}' is distinct from v_paid_current_policy#>>'{classification,source_shift_id}'
         or v_paid_origin_policy_unit#>>'{policy_envelope,classification,source_row_key}' is distinct from v_paid_current_policy#>>'{classification,source_row_key}'
         or v_paid_origin_policy_unit#>>'{policy_envelope,root_timesheet_id}' is distinct from v_paid_current_policy->>'root_timesheet_id'
         or v_paid_origin_policy_unit#>>'{policy_envelope,replacement,leg_fingerprint}' is distinct from v_paid_current_policy#>>'{replacement,leg_fingerprint}'
         or v_paid_origin_policy_unit#>>'{policy_envelope,replacement,tsfin_policy,tsfin_policy_fingerprint}' is distinct from v_paid_current_policy#>>'{replacement,tsfin_policy,tsfin_policy_fingerprint}'
         or v_paid_origin_policy_unit#>>'{policy_envelope,replacement,invoice_policy,invoice_policy_fingerprint}' is distinct from v_paid_current_policy#>>'{replacement,invoice_policy,invoice_policy_fingerprint}'
         or v_paid_origin_policy_unit#>>'{policy_envelope,replacement,invoice_policy,invoice_stream}' is distinct from v_paid_current_policy#>>'{replacement,invoice_policy,invoice_stream}' then
        raise exception 'IMPORT_REVIEW_PAID_ROLLOVER_SHELL_POLICY_CHANGED' using errcode='P0001';
      end if;
      v_paid_historical_id:=v_paid_historical_tf.id;
      v_paid_shell_id:=v_paid_current_tf.id;
      v_paid_mode:='REUSED_COMPLETED_OPERATION_SHELL';
    end if;

    v_paid_applied:=jsonb_build_object(
      'applied_timesheet_id',v_paid_timesheet_id,'rollover_mode',v_paid_mode,
      'historical_paid_tsfin_id',v_paid_historical_id,'current_shell_tsfin_id',v_paid_shell_id,
      'intended_authorisation_action',v_paid_intent,
      'reviewed_unit_fingerprint',v_paid_unit->>'unit_fingerprint',
      'reconciliation_fingerprint',v_paid_unit->>'reconciliation_fingerprint');
    update pg_temp.import_review_reconciliation_units_v1 applied_unit
    set unit_json=applied_unit.unit_json||v_paid_applied||jsonb_build_object(
      'applied_result_fingerprint',encode(extensions.digest(convert_to(v_paid_applied::text,'UTF8'),'sha256'),'hex'))
    where applied_unit.action_id=v_paid_unit->>'action_id';
    v_paid_applied_timesheet_ids:=array_append(v_paid_applied_timesheet_ids,v_paid_timesheet_id);
  end loop;

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
  from (
    select a.timesheet_id from tmp_aff_ts a where a.timesheet_id is not null
    union all
    select paid_id from unnest(coalesce(v_paid_applied_timesheet_ids,array[]::uuid[])) paid(paid_id)
  ) a;

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
    if exists(select 1 from pg_temp.import_review_reconciliation_units_v1 u
      where u.route='AMEND_PAID_UNINVOICED_SOURCE' and (
        nullif(u.unit_json->>'applied_timesheet_id','')::uuid is distinct from u.source_timesheet_id
        or nullif(u.unit_json->>'reviewed_unit_fingerprint','') is distinct from u.unit_fingerprint
        or coalesce(u.unit_json->>'rollover_mode','') not in ('CREATED_CURRENT_OPERATION_SHELL','REUSED_COMPLETED_OPERATION_SHELL')
        or coalesce(u.unit_json->>'historical_paid_tsfin_id','')!~*'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        or coalesce(u.unit_json->>'current_shell_tsfin_id','')!~*'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        or u.unit_json->>'applied_result_fingerprint' is distinct from encode(extensions.digest(convert_to(
          jsonb_build_object(
            'applied_timesheet_id',(u.unit_json->>'applied_timesheet_id')::uuid,
            'rollover_mode',u.unit_json->>'rollover_mode',
            'historical_paid_tsfin_id',(u.unit_json->>'historical_paid_tsfin_id')::uuid,
            'current_shell_tsfin_id',(u.unit_json->>'current_shell_tsfin_id')::uuid,
            'intended_authorisation_action',u.unit_json->>'intended_authorisation_action',
            'reviewed_unit_fingerprint',u.unit_json->>'reviewed_unit_fingerprint',
            'reconciliation_fingerprint',u.unit_json->>'reconciliation_fingerprint')::text,'UTF8'),'sha256'),'hex')
        or not exists(select 1 from public.timesheets_financials historical
          where historical.id=(u.unit_json->>'historical_paid_tsfin_id')::uuid
            and historical.timesheet_id=u.source_timesheet_id and not historical.is_current
            and historical.paid_at_utc is not null and historical.locked_by_invoice_id is null)
        or not exists(select 1 from public.timesheets_financials shell
          where shell.id=(u.unit_json->>'current_shell_tsfin_id')::uuid
            and shell.timesheet_id=u.source_timesheet_id and shell.is_current and shell.paid_at_utc is null)
      )) then
      raise exception 'IMPORT_REVIEW_APPLY_POSTCONDITION_FAILED' using errcode='55000',
        detail=jsonb_build_object('reason_code','PAID_SOURCE_ROLLOVER_RESULT_INVALID')::text;
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
  select coalesce(array_agg(distinct target_id order by target_id),array[]::uuid[])
  into v_auto_authorise_timesheet_ids from (
    select target_id from unnest(coalesce(v_auto_authorise_timesheet_ids,array[]::uuid[])) x(target_id)
    where not (target_id=any(coalesce(v_paid_applied_timesheet_ids,array[]::uuid[])))
    union all
    select u.source_timesheet_id from pg_temp.import_review_reconciliation_units_v1 u
    where u.route='AMEND_PAID_UNINVOICED_SOURCE'
      and u.unit_json->>'intended_authorisation_action'='AUTHORISE'
  ) reviewed_auto;
  select coalesce(array_agg(distinct target_id order by target_id),array[]::uuid[])
  into v_reauthorise_timesheet_ids from (
    select target_id from unnest(coalesce(v_reauthorise_timesheet_ids,array[]::uuid[])) x(target_id)
    where not (target_id=any(coalesce(v_paid_applied_timesheet_ids,array[]::uuid[])))
    union all
    select u.source_timesheet_id from pg_temp.import_review_reconciliation_units_v1 u
    where u.route='AMEND_PAID_UNINVOICED_SOURCE'
      and u.unit_json->>'intended_authorisation_action'='REAUTHORISE'
  ) reviewed_reauthorise;
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
-- CloudTMS deployment metadata preserved from the installed TEST definition.
ALTER FUNCTION public.nhsp_weekly_apply_transactional(uuid, jsonb, uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.nhsp_weekly_apply_transactional(uuid, jsonb, uuid) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.nhsp_weekly_apply_transactional(uuid, jsonb, uuid) TO postgres, service_role;
