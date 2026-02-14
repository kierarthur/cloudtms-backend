create or replace function public.hr_weekly_apply_transactional(
  p_import_id uuid,
  p_payload jsonb,
  p_actor_user_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now timestamptz := now();

  -- import header
  v_import_source_system text;
  v_import_client_id uuid;

  -- payload parts
  v_payload jsonb := coalesce(p_payload, '{}'::jsonb);
  v_actions_json jsonb := coalesce(v_payload->'selected_action_ids', '[]'::jsonb);
  v_actions2_json jsonb := coalesce(v_payload->'selected_actions', '[]'::jsonb);
  v_email_actions jsonb := coalesce(v_payload->'email_actions', '[]'::jsonb);

  -- ✅ NEW: destructive invalidation selections (MODE_A)
  v_invalidation_actions jsonb := coalesce(v_payload->'invalidation_actions', '[]'::jsonb);
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
  v_force_keys_non_invoiced text[] := array[]::text[];

  v_phase3_result jsonb := null;

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

  -- ✅ NEW: consolidated email jobs + items
  v_email_jobs jsonb := '[]'::jsonb;

  -- affected timesheets for TSFIN drain (MODE_B + MODE_A validation changes)
  v_affected_timesheet_ids uuid[] := array[]::uuid[];

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

  v_steps := v_steps || jsonb_build_array(jsonb_build_object('step','IMPORT_OK','client_id',v_import_client_id::text));

  -- ─────────────────────────────────────────────
  -- 1) Parse and normalize selection payload (ROW:/CANCEL:)
  -- ─────────────────────────────────────────────
  if jsonb_typeof(v_actions_json) <> 'array' then
    raise exception 'hr_weekly_apply_transactional: selected_action_ids must be a JSON array.';
  end if;

  if jsonb_typeof(v_actions2_json) <> 'array' then
    raise exception 'hr_weekly_apply_transactional: selected_actions must be a JSON array.';
  end if;

  if jsonb_typeof(v_email_actions) <> 'array' then
    raise exception 'hr_weekly_apply_transactional: email_actions must be a JSON array.';
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

  insert into tmp_sel_ids(action_id)
  select distinct nullif(btrim(a.value->>'action_id'), '')
  from jsonb_array_elements(v_actions2_json) as a(value)
  where nullif(btrim(a.value->>'action_id'), '') is not null
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
  v_email_actions_count := jsonb_array_length(v_email_actions);

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

  -- ─────────────────────────────────────────────
  -- 2) Load weekly_import_phase2 + compute per-group MODE_A/MODE_B
  --     MODE switch: effective_no_timesheet_required
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
    case
      when (
        case
          when (c.overrideclientsettings is true and c.no_timesheet_required is not null)
            then c.no_timesheet_required
          else coalesce(cs.no_timesheet_required, false)
        end
      ) is true then 'MODE_B'
      else 'MODE_A'
    end as mode
  from (
    select distinct p2ok.contract_id, p2ok.candidate_id, p2ok.client_id, p2ok.week_ending_date
    from tmp_p2_ok p2ok
  ) as t
  join public.contracts c
    on c.id = t.contract_id
  left join lateral (
    select cs2.no_timesheet_required
    from public.client_settings cs2
    where cs2.client_id = v_import_client_id
      and (cs2.effective_from is null or cs2.effective_from <= t.week_ending_date)
    order by cs2.effective_from desc nulls last, cs2.updated_at desc nulls last
    limit 1
  ) as cs on true;

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
    -- (UNCHANGED MODE_B PHASE3 / PHASE1 / PHASE1.5 / CANCELLATIONS BLOCKS...)
    create temporary table tmp_changed_sel on commit drop as
    select
      ch.external_row_key,
      ch.is_invoiced
    from public.weekly_import_changed_hours_phase3(p_import_id := p_import_id, p_system_type := 'HEALTHROSTER') as ch
    where ch.external_row_key = any(coalesce(v_force_keys_final, array[]::text[]));

    select coalesce(array_agg(cs.external_row_key order by cs.external_row_key), array[]::text[])
    into v_invoiced_changed_keys
    from tmp_changed_sel cs
    where cs.is_invoiced is true;

    select coalesce(array_agg(cs.external_row_key order by cs.external_row_key), array[]::text[])
    into v_not_invoiced_changed_keys
    from tmp_changed_sel cs
    where cs.is_invoiced is false;

    v_invoiced_changed_keys_count := coalesce(array_length(v_invoiced_changed_keys, 1), 0);
    v_not_invoiced_changed_keys_count := coalesce(array_length(v_not_invoiced_changed_keys, 1), 0);

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

    -- (ENSURE BLOCK continues unchanged in your function...)
  end if;

  -- ─────────────────────────────────────────────
  -- 9) MODE_A mirror ingestion (unchanged)
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
  -- ✅ MODE_A shift→timesheet linking (as supplied; unchanged)
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
  --     ✅ Option B: tmp_val_mode derives directly from effective route flags
  --     ✅ NEW: invalidation_actions controls ref clearing
  --     ✅ NEW: email_jobs are grouped by effective recipient (alt email overrides default)
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
    nullif(btrim(r.value->>'issue_fingerprint'), '') as issue_fingerprint,
    nullif(btrim(r.value->>'recipient_email'), '') as recipient_email,
    (lower(coalesce(r.value->>'emailed_already','false')) in ('true','1')) as emailed_already,
    r.value as row_json
  from jsonb_array_elements(v_weekly_val_payload->'rows') as r(value)
  where nullif(btrim(r.value->>'timesheet_id'), '') is not null
    and nullif(btrim(r.value->>'candidate_id'), '') is not null
    and nullif(btrim(r.value->>'contract_id'), '') is not null
    and nullif(btrim(r.value->>'week_ending_date'), '') is not null
    and nullif(btrim(r.value->>'client_id'), '') is not null;

  select count(*)::int
  into v_val_rows_count
  from tmp_val_rows;

  create temporary table tmp_val_mode on commit drop as
  select
    vr.timesheet_id,
    case
      when (
        coalesce(
          case
            when (cval.overrideclientsettings is true and cval.autoprocess_hr is not null)
              then cval.autoprocess_hr
            else csval.autoprocess_hr
          end,
          false
        ) is true
        and coalesce(
          case
            when (cval.overrideclientsettings is true and cval.requires_hr is not null)
              then cval.requires_hr
            else csval.requires_hr
          end,
          false
        ) is true
        and coalesce(
          case
            when (cval.overrideclientsettings is true and cval.no_timesheet_required is not null)
              then cval.no_timesheet_required
            else csval.no_timesheet_required
          end,
          false
        ) is false
      )
      then 'MODE_A'
      else 'MODE_B'
    end as mode
  from tmp_val_rows vr
  join public.contracts cval
    on cval.id = vr.contract_id
  left join lateral (
    select
      cs2.autoprocess_hr,
      cs2.requires_hr,
      cs2.no_timesheet_required
    from public.client_settings cs2
    where cs2.client_id = v_import_client_id
      and (cs2.effective_from is null or cs2.effective_from <= vr.week_ending_date)
    order by cs2.effective_from desc nulls last, cs2.updated_at desc nulls last
    limit 1
  ) as csval on true;

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
  where upper(coalesce(cx.value->>'match_status','')) = 'UNMATCHED'
    and (lower(coalesce(cx.value->>'invoice_locked','false')) in ('true','1')) is false
    and nullif(btrim(coalesce(cx.value->>'invoice_locked_invoice_id','')), '') is null
    and nullif(btrim(coalesce(cx.value->>'ref_before','')), '') is not null
    and nullif(btrim(coalesce(cx.value->>'timesheet_start','')), '') is not null
    and nullif(btrim(coalesce(cx.value->>'timesheet_end','')), '') is not null
    and nullif(btrim(coalesce(cx.value->>'work_date','')), '') is not null
    and (
      v_invalidation_actions_count = 0
      or (ia.timesheet_id is not null and ia.invalidate is true)
    )
  on conflict (timesheet_id, comparison_key) do nothing;

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
     );

  get diagnostics v_mode_a_ref_cleared_count = row_count;

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

  create temporary table tmp_val_upsert on commit drop as
  select
    vr.timesheet_id,
    case
      when vr.overall_status = 'OK' then 'VALIDATION_OK'::public.validation_status_enum
      else 'VALIDATION_ERROR'::public.validation_status_enum
    end as new_status,
    'HEALTHROSTER_WEEKLY'::text as new_reason_code,
    case when vr.overall_status = 'OK' then v_now else null end as new_validated_at_utc,
    p_import_id as new_last_source
  from tmp_val_rows vr
  join tmp_val_mode vm
    on vm.timesheet_id = vr.timesheet_id
  where vm.mode = 'MODE_A'
    and vr.timesheet_id is not null;

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
  ) as x;

  insert into public.timesheet_validations(
    timesheet_id,
    status,
    reason_code,
    validated_at_utc,
    last_source,
    updated_at
  )
  select
    u.timesheet_id,
    u.new_status,
    u.new_reason_code,
    u.new_validated_at_utc,
    u.new_last_source,
    v_now
  from tmp_val_upsert u
  on conflict (timesheet_id) do update
    set status = excluded.status,
        reason_code = excluded.reason_code,
        validated_at_utc = excluded.validated_at_utc,
        last_source = excluded.last_source,
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

  -- ─────────────────────────────────────────────
  -- ✅ Email actions: allow Alternative Email override per selected mismatch
  --    email_actions[] rows are selection-driven (tick respected).
  --    Effective recipient is:
  --      coalesce(alternative_email, vr.recipient_email)
  --    This allows emailing even when vr.recipient_email is null (can_email=false),
  --    as long as alternative_email is provided.
  -- ─────────────────────────────────────────────
  create temporary table tmp_email_actions on commit drop as
  select
    nullif(btrim(a.value->>'timesheet_id'), '')::uuid as timesheet_id,
    nullif(btrim(a.value->>'issue_fingerprint'), '') as issue_fingerprint,
    nullif(
      btrim(
        coalesce(
          a.value->>'alternative_email',
          a.value->>'alt_email',
          a.value->>'alt_recipient_email'
        )
      ),
      ''
    ) as alternative_email
  from jsonb_array_elements(v_email_actions) as a(value)
  where nullif(btrim(a.value->>'timesheet_id'), '') is not null
    and nullif(btrim(a.value->>'issue_fingerprint'), '') is not null;

  create temporary table tmp_email_join on commit drop as
  select
    ea.timesheet_id,
    ea.issue_fingerprint,
    vr.client_id,
    vr.recipient_email,
    ea.alternative_email,
    coalesce(ea.alternative_email, vr.recipient_email) as effective_recipient_email,
    vr.emailed_already,
    vr.candidate_id,
    vr.contract_id,
    vr.week_ending_date,
    vr.row_json
  from tmp_email_actions ea
  join tmp_val_rows vr
    on vr.timesheet_id = ea.timesheet_id
   and vr.issue_fingerprint = ea.issue_fingerprint
  join tmp_val_mode vm
    on vm.timesheet_id = vr.timesheet_id
  where vm.mode = 'MODE_A'
    and coalesce(ea.alternative_email, vr.recipient_email) is not null;

  insert into public.hr_issue_emails(
    source_system,
    import_id,
    client_id,
    timesheet_id,
    reason_code,
    issue_fingerprint,
    last_sent_at,
    created_at,
    updated_at
  )
  select
    'HEALTHROSTER',
    p_import_id,
    ej.client_id,
    ej.timesheet_id,
    'HEALTHROSTER_WEEKLY',
    ej.issue_fingerprint,
    v_now,
    v_now,
    v_now
  from tmp_email_join ej
  on conflict (issue_fingerprint) do update
    set last_sent_at = excluded.last_sent_at,
        updated_at = excluded.updated_at,
        import_id = excluded.import_id,
        client_id = excluded.client_id,
        timesheet_id = excluded.timesheet_id;

  create temporary table tmp_email_items on commit drop as
  select
    ej.effective_recipient_email as recipient_email,
    ej.timesheet_id as timesheet_id,
    ej.issue_fingerprint as issue_fingerprint,
    nullif(btrim(coalesce(ej.row_json->>'candidate_name','')), '') as candidate_name,
    ej.week_ending_date as week_ending_date,
    nullif(btrim(coalesce(cx.value->>'work_date','')), '')::date as work_date,
    nullif(btrim(coalesce(cx.value->>'timesheet_start','')), '') as timesheet_start,
    nullif(btrim(coalesce(cx.value->>'timesheet_end','')), '') as timesheet_end,
    coalesce(nullif(btrim(coalesce(cx.value->>'timesheet_break_mins','')), '')::int, 0) as timesheet_break_mins,
    nullif(btrim(coalesce(cx.value->>'healthroster_start','')), '') as healthroster_start,
    nullif(btrim(coalesce(cx.value->>'healthroster_end','')), '') as healthroster_end,
    coalesce(nullif(btrim(coalesce(cx.value->>'healthroster_break_mins','')), '')::int, 0) as healthroster_break_mins,
    nullif(btrim(coalesce(cx.value->>'match_status','')), '') as match_status,
    nullif(btrim(coalesce(cx.value->>'ref_before','')), '') as ref_before,
    nullif(btrim(coalesce(cx.value->>'ref_after','')), '') as ref_after,
    (lower(coalesce(cx.value->>'invoice_locked','false')) in ('true','1')) as invoice_locked,
    nullif(btrim(coalesce(cx.value->>'invoice_locked_invoice_id','')), '') as invoice_locked_invoice_id,
    nullif(btrim(coalesce(cx.value->>'comparison_key','')), '') as comparison_key
  from tmp_email_join ej
  cross join lateral jsonb_array_elements(coalesce(ej.row_json->'comparisons','[]'::jsonb)) as cx(value)
  where coalesce((cx.value->>'match')::boolean,false) is false;

  create temporary table tmp_email_jobs on commit drop as
  select
    ej.effective_recipient_email as recipient_email,
    (count(*) filter (where ej.emailed_already is true))::int as reemail_count,
    (count(*) filter (where ej.emailed_already is false))::int as email_count
  from tmp_email_join ej
  group by ej.effective_recipient_email;

  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'recipient_email', tj.recipient_email,
        'email_kind',
          (case
            when tj.recipient_email is null then 'NONE'
            when tj.reemail_count > 0 and tj.email_count = 0 then 'REEMAIL'
            when tj.reemail_count = 0 and tj.email_count > 0 then 'EMAIL'
            when tj.reemail_count > 0 and tj.email_count > 0 then 'MIXED'
            else 'EMAIL'
          end),
        'issue_fingerprints',
          coalesce(
            (
              select to_jsonb(array_agg(distinct ej2.issue_fingerprint order by ej2.issue_fingerprint))
              from tmp_email_join ej2
              where ej2.effective_recipient_email = tj.recipient_email
            ),
            '[]'::jsonb
          ),
        'attachment_timesheet_ids',
          coalesce(
            (
              select to_jsonb(array_agg(distinct ej3.timesheet_id::text order by ej3.timesheet_id::text))
              from tmp_email_join ej3
              where ej3.effective_recipient_email = tj.recipient_email
            ),
            '[]'::jsonb
          ),
        'items',
          coalesce(
            (
              select jsonb_agg(
                jsonb_build_object(
                  'timesheet_id', ti.timesheet_id::text,
                  'issue_fingerprint', ti.issue_fingerprint,
                  'candidate_name', ti.candidate_name,
                  'week_ending_date', ti.week_ending_date::text,
                  'work_date', ti.work_date::text,
                  'timesheet_start', ti.timesheet_start,
                  'timesheet_end', ti.timesheet_end,
                  'timesheet_break_mins', ti.timesheet_break_mins,
                  'healthroster_start', ti.healthroster_start,
                  'healthroster_end', ti.healthroster_end,
                  'healthroster_break_mins', ti.healthroster_break_mins,
                  'match_status', ti.match_status,
                  'ref_before', ti.ref_before,
                  'ref_after', ti.ref_after,
                  'invoice_locked', ti.invoice_locked,
                  'invoice_locked_invoice_id', ti.invoice_locked_invoice_id,
                  'comparison_key', ti.comparison_key
                )
                order by ti.candidate_name nulls last, ti.work_date asc, ti.timesheet_start nulls last
              )
              from tmp_email_items ti
              where ti.recipient_email = tj.recipient_email
            ),
            '[]'::jsonb
          )
      )
      order by tj.recipient_email nulls last
    ),
    '[]'::jsonb
  )
  into v_email_jobs;

  v_email_jobs_count := jsonb_array_length(coalesce(v_email_jobs, '[]'::jsonb));

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
  -- 11) Compute affected_timesheet_ids (MODE_B work + MODE_A validation changes)
  -- ─────────────────────────────────────────────
  select coalesce(array_agg(distinct a.timesheet_id order by a.timesheet_id), array[]::uuid[])
  into v_affected_timesheet_ids
  from tmp_aff_ts a
  where a.timesheet_id is not null;

  if array_length(v_affected_timesheet_ids, 1) is not null then
    perform public.enqueue_ts_financials_priority(v_affected_timesheet_ids, 'CONTEXT_CHANGED'::public.ts_fin_reason_enum);
  end if;

  -- ─────────────────────────────────────────────
  -- 12) Mark import applied (inside transaction)
  -- ─────────────────────────────────────────────
  update public.hr_imports hi3
  set
    import_scope = 'HR_WEEKLY',
    applied_at = v_now
  where hi3.id = p_import_id;

  v_steps := v_steps || jsonb_build_array(jsonb_build_object('step','IMPORT_APPLIED'));

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

  return jsonb_build_object(
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
      'mode_a_ref_cleared_count', v_mode_a_ref_cleared_count
    ),
    'email_jobs', v_email_jobs,
    'affected_timesheet_ids', to_jsonb(coalesce(v_affected_timesheet_ids, array[]::uuid[])),
    'validation_affected_timesheet_ids', to_jsonb(coalesce(v_validation_changed_timesheet_ids, array[]::uuid[]))
  );

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
$$;
