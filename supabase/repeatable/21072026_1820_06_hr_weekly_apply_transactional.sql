-- CloudTMS reviewed direct replacement; installed and verified in TEST on 21 July 2026.
-- Exact TEST baseline body MD5 prefix: 0305f0d4f038.
-- Hard cutover: every call requires the server-owned import review contract.
CREATE OR REPLACE FUNCTION public.hr_weekly_apply_transactional(p_import_id uuid, p_payload jsonb, p_actor_user_id uuid)
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
        coalesce(array_agg(u.action_id order by u.action_id) filter(where u.route in ('AMEND_PAID_UNINVOICED_SOURCE','AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT')),array[]::text[]),
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
        and (cs.is_paid is false or exists(select 1 from pg_temp.import_review_reconciliation_units_v1 paid_unit
          where paid_unit.source_identity=cs.external_row_key
            and paid_unit.route='AMEND_PAID_UNINVOICED_SOURCE'
            and paid_unit.unit_json->>'intended_authorisation_action'='REAUTHORISE'))
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
      and not (a.timesheet_id=any(coalesce(v_paid_applied_timesheet_ids,array[]::uuid[])))
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
-- CloudTMS deployment metadata preserved from the installed TEST definition.
ALTER FUNCTION public.hr_weekly_apply_transactional(uuid, jsonb, uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.hr_weekly_apply_transactional(uuid, jsonb, uuid) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.hr_weekly_apply_transactional(uuid, jsonb, uuid) TO postgres, service_role;
