-- CloudTMS reviewed direct replacement; review artifact only, not installed.
-- Exact TEST baseline body MD5 prefix: bde8a243b91a.
-- Ordinary and non-import-authoritative branches remain on the installed implementation.
CREATE OR REPLACE FUNCTION public.hr_weekly_phase3_apply_adjustment_truth(p_import_id uuid, p_selected_external_row_keys text[], p_decisions jsonb, p_actor_user_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_now timestamptz := now();

  v_src public.hr_source_enum;

  v_selected_keys text[] := '{}';
  v_key text;

  v_phase3_rows jsonb := '[]'::jsonb;

  v_decision jsonb;
  v_skip_text text;
  v_skip boolean;

  v_row jsonb;
  v_is_invoiced boolean;

  v_credit_week_start text;
  v_reinvoice_week_start text;

  v_contract_id uuid;
  v_candidate_id uuid;
  v_client_id uuid;
  v_week_ending_date date;
  v_base_timesheet_id uuid;

  v_correction_id text;
  v_kind text;

  v_old_start_utc timestamptz;
  v_old_end_utc   timestamptz;
  v_new_start_utc timestamptz;
  v_new_end_utc   timestamptz;
  v_old_break_mins int;
  v_new_break_mins int;
  v_new_paid_minutes int;

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
  v_existing_ts_is_current boolean;
  v_existing_ts_version bigint;
  v_existing_ts_status text;

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
  -- Historical correction finance authority (Policy X pre-draft only)
  v_chain_scope jsonb := null;
  v_financial_preflight jsonb := null;
  v_correction_financials_policy_envelope jsonb := null;
  v_correction_financials_policy_envelope_fingerprint text := null;
  v_correction_operation_id uuid := null;
  v_root_timesheet_id uuid := null;
  v_latest_positive_timesheet_id uuid := null;


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

  if jsonb_typeof(coalesce(p_decisions,'{}'::jsonb)) <> 'object' then
    raise exception 'hr_weekly_phase3_apply_adjustment_truth: p_decisions must be a JSON object.';
  end if;

  -- ---- Load Phase 3 rows as JSONB (schema-safe) ----
  select coalesce(jsonb_agg(to_jsonb(r)), '[]'::jsonb)
  into v_phase3_rows
  from public.weekly_import_changed_hours_phase3(
    p_import_id := p_import_id,
    p_system_type := 'HEALTHROSTER'
  ) as r;

  -- Build temp lookup for phase3 rows by external_row_key
  create temporary table tmp_phase3_by_key(
    external_row_key text primary key,
    row_json jsonb not null
  ) on commit drop;

  insert into tmp_phase3_by_key(external_row_key, row_json)
  select
    nullif(btrim(x.row_json->>'external_row_key'), '') as external_row_key,
    x.row_json
  from (
    select jsonb_array_elements(v_phase3_rows) as row_json
  ) as x
  where nullif(btrim(x.row_json->>'external_row_key'), '') is not null
  on conflict (external_row_key) do nothing;

  -- ---- Process each selected key ----
  foreach v_key in array v_selected_keys loop
    select t.row_json
    into v_row
    from tmp_phase3_by_key t
    where t.external_row_key = v_key;

    if v_row is null then
      raise exception 'hr_weekly_phase3_apply_adjustment_truth: Phase 3 row not found for selected external_row_key=%', v_key;
    end if;

    -- Validate decision object exists
    v_decision := coalesce(p_decisions->v_key, null);

    if v_decision is null or jsonb_typeof(v_decision) <> 'object' then
      raise exception 'hr_weekly_phase3_apply_adjustment_truth: Missing/invalid decision object for external_row_key=%', v_key;
    end if;

    v_skip_text := v_decision->>'skip';
    if v_skip_text is null then
      raise exception 'hr_weekly_phase3_apply_adjustment_truth: Decision missing skip boolean for external_row_key=%', v_key;
    end if;

    v_skip :=
      case
        when lower(v_skip_text) in ('true','1') then true
        when lower(v_skip_text) in ('false','0') then false
        else null
      end;

    if v_skip is null then
      raise exception 'hr_weekly_phase3_apply_adjustment_truth: Decision skip is not boolean-like for external_row_key=% (skip=%)', v_key, v_skip_text;
    end if;

    if v_skip then
      v_skipped_count := v_skipped_count + 1;
      continue;
    end if;

    -- Determine invoiced flag (from Phase3 row)
    v_is_invoiced :=
      case
        when lower(coalesce(v_row->>'is_invoiced','')) in ('true','1') then true
        else false
      end;

    if v_is_invoiced then
      v_credit_week_start := nullif(btrim(v_decision->>'credit_week_start'), '');
      v_reinvoice_week_start := nullif(btrim(v_decision->>'reinvoice_week_start'), '');

      if v_credit_week_start is null or v_reinvoice_week_start is null then
        raise exception 'hr_weekly_phase3_apply_adjustment_truth: Missing credit/reinvoice weeks for invoiced external_row_key=%', v_key;
      end if;

      begin
        if extract(isodow from (v_credit_week_start::date)) <> 1 then
          raise exception 'credit_week_start must be Monday';
        end if;
      exception when others then
        raise exception 'hr_weekly_phase3_apply_adjustment_truth: Invalid credit_week_start for external_row_key=% (value=%)', v_key, v_credit_week_start;
      end;

      begin
        if extract(isodow from (v_reinvoice_week_start::date)) <> 1 then
          raise exception 'reinvoice_week_start must be Monday';
        end if;
      exception when others then
        raise exception 'hr_weekly_phase3_apply_adjustment_truth: Invalid reinvoice_week_start for external_row_key=% (value=%)', v_key, v_reinvoice_week_start;
      end;
    else
      v_credit_week_start := null;
      v_reinvoice_week_start := null;
    end if;

    -- Extract required mapping fields
    begin
      v_contract_id := (v_row->>'contract_id')::uuid;
      v_candidate_id := (v_row->>'candidate_id')::uuid;
      v_client_id := (v_row->>'client_id')::uuid;
      v_week_ending_date := (v_row->>'week_ending_date')::date;
    exception when others then
      raise exception 'hr_weekly_phase3_apply_adjustment_truth: Phase 3 row missing contract_id/candidate_id/client_id/week_ending_date for external_row_key=%', v_key;
    end;

    begin
      v_base_timesheet_id := nullif(v_row->>'timesheet_id','')::uuid;
    exception when others then
      v_base_timesheet_id := null;
    end;

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

    if coalesce((v_chain_scope->>'valid')::boolean,false) is not true then
      raise exception using message='CORRECTION_CHAIN_UNRESOLVED', errcode='P0001', detail=v_chain_scope::text;
    end if;

    v_root_timesheet_id := nullif(v_chain_scope->>'root_timesheet_id','')::uuid;
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

    v_old_break_mins := coalesce(nullif(v_row->>'old_break_mins','')::int, 0);
    v_new_break_mins := coalesce(nullif(v_row->>'new_break_mins','')::int, 0);

    -- Compute correction_id (must match JS makeWeeklyHoursCorrectionId)
    v_fnv_s :=
      coalesce(p_import_id::text,'') || '|' ||
      coalesce(v_key,'') || '|' ||
      coalesce(v_row->>'old_start_utc','') || '|' ||
      coalesce(v_row->>'new_start_utc','') || '|' ||
      coalesce(v_row->>'old_end_utc','') || '|' ||
      coalesce(v_row->>'new_end_utc','') || '|' ||
      coalesce(v_row->>'old_break_mins','') || '|' ||
      coalesce(v_row->>'new_break_mins','');

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

    -- Apply two artefacts: REVERSAL and REPLACEMENT
    for v_kind in select unnest(array['CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT']) loop

      if v_kind = 'CHANGED_HOURS_REVERSAL' then
        v_seg_start_utc := v_old_start_utc;
        v_seg_end_utc := v_old_end_utc;
        v_seg_break_mins := greatest(0, v_old_break_mins);
      else
        v_seg_start_utc := v_new_start_utc;
        v_seg_end_utc := v_new_end_utc;
        v_seg_break_mins := greatest(0, v_new_break_mins);
      end if;

      v_shift_date_ymd := to_char((v_seg_start_utc at time zone 'Europe/London')::date, 'YYYY-MM-DD');

      v_hint := jsonb_build_object(
        'import_correction', jsonb_build_object(
          'import_id', p_import_id::text,
          'external_row_key', v_key,
          'correction_id', v_correction_id,
          'correction_kind', v_kind,
          'credit_week_start', v_credit_week_start,
          'reinvoice_week_start', v_reinvoice_week_start
        )
      );

      v_hint := v_hint || jsonb_build_object(
        'correction_financials_policy_envelope', v_correction_financials_policy_envelope,
        'correction_financials_policy_envelope_fingerprint', v_correction_financials_policy_envelope_fingerprint,
        'root_timesheet_id', v_root_timesheet_id::text,
        'latest_positive_timesheet_id', coalesce(v_latest_positive_timesheet_id,v_base_timesheet_id)::text
      );


      v_shift_label := 'weekly-correction-' || lower(v_kind) || '-' || v_correction_id;

      v_shift_label_norm :=
        regexp_replace(
          regexp_replace(lower(trim(v_shift_label)), '\s+', ' ', 'g'),
          '[^\w\s\-@&\/,.:]',
          '',
          'g'
        );

      v_schedule := jsonb_build_array(
        jsonb_build_object(
          'date', v_shift_date_ymd,
          'ward', nullif(btrim(coalesce(v_contract_ward_hint,'contract')), ''),
          'start_utc', v_seg_start_utc::text,
          'end_utc', v_seg_end_utc::text,
          'break_mins', v_seg_break_mins
        )
      );

      -- Idempotency: reuse existing correction timesheet (unique on correction_id+kind)
      v_existing_ts_id := null;
      v_existing_ts_is_current := null;
      v_existing_ts_version := null;
      v_existing_ts_status := null;

      select
        t.timesheet_id,
        t.is_current,
        t.version,
        t.status::text
      into
        v_existing_ts_id,
        v_existing_ts_is_current,
        v_existing_ts_version,
        v_existing_ts_status
      from public.timesheets t
      where t.correction_id = v_correction_id
        and t.correction_kind = v_kind
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
            submission_mode_snapshot = 'MANUAL'::public.submission_mode_enum,
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
                'MANUAL'::public.submission_mode_enum,
                'SUBMITTED'::public.contract_week_status_enum,
                v_now,
                v_now,
                v_existing_ts_id
              )
              returning id into v_existing_cw_id;

              exit;
            exception when unique_violation then
              -- another txn took the same seq; retry
              v_existing_cw_id := null;
            end;
          end loop;
        end if;

        -- Update existing correction timesheet to ensure columns match locked contract
        if exists (
          select 1 from public.timesheets guard_ts
          left join public.timesheets_financials guard_tf on guard_tf.timesheet_id=guard_ts.timesheet_id and guard_tf.is_current=true
          where guard_ts.timesheet_id=v_existing_ts_id
            and (guard_ts.authorised_at_server is not null or guard_tf.authorised_at_utc is not null or guard_tf.paid_at_utc is not null or guard_tf.locked_by_invoice_id is not null
                 or exists(select 1 from public.invoice_lines il where il.timesheet_id=guard_ts.timesheet_id))
        ) then
          raise exception using message='CORRECTION_PAIR_LIFECYCLE_TRANSITION_REQUIRED', errcode='P0001',
            detail=jsonb_build_object('code','CORRECTION_PAIR_LIFECYCLE_TRANSITION_REQUIRED','timesheet_id',v_existing_ts_id)::text;
        end if;

        update public.timesheets t2
        set
          is_current = true,
          status = 'RECEIVED'::public.timesheet_status_enum,
          sheet_scope = 'WEEKLY'::public.timesheet_scope_enum,
          submission_mode = 'MANUAL'::public.submission_mode_enum,
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
          correction_id = v_correction_id,
          correction_kind = v_kind,
          adjustment_origin = 'IMPORT_CORRECTION',
          updated_at = v_now
        where t2.timesheet_id = v_existing_ts_id;

        v_upd_count := v_upd_count + 1;
        v_updated_ts_ids := array_append(v_updated_ts_ids, v_existing_ts_id);

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
              'MANUAL'::public.submission_mode_enum,
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

        v_hash_hex := encode(digest(convert_to(v_booking_base, 'utf8'), 'sha256'), 'hex');
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
            'WEEKLY'::public.timesheet_scope_enum,
            'MANUAL'::public.submission_mode_enum,
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
            null,
            '{}'::jsonb,
            v_now,
            v_now,
            true,
            null,
            v_hint,
            v_correction_id,
            v_kind,
            'IMPORT_CORRECTION'
          )
          returning timesheet_id into v_ts_id;

        exception when unique_violation then
          -- If another txn created the same correction timesheet, fetch it and update instead.
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

          if exists (
          select 1 from public.timesheets guard_ts
          left join public.timesheets_financials guard_tf on guard_tf.timesheet_id=guard_ts.timesheet_id and guard_tf.is_current=true
          where guard_ts.timesheet_id=v_ts_id
            and (guard_ts.authorised_at_server is not null or guard_tf.authorised_at_utc is not null or guard_tf.paid_at_utc is not null or guard_tf.locked_by_invoice_id is not null
                 or exists(select 1 from public.invoice_lines il where il.timesheet_id=guard_ts.timesheet_id))
        ) then
          raise exception using message='CORRECTION_PAIR_LIFECYCLE_TRANSITION_REQUIRED', errcode='P0001',
            detail=jsonb_build_object('code','CORRECTION_PAIR_LIFECYCLE_TRANSITION_REQUIRED','timesheet_id',v_ts_id)::text;
        end if;

        update public.timesheets t4
          set
            is_current = true,
            status = 'RECEIVED'::public.timesheet_status_enum,
            sheet_scope = 'WEEKLY'::public.timesheet_scope_enum,
            submission_mode = 'MANUAL'::public.submission_mode_enum,
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
          submission_mode_snapshot = 'MANUAL'::public.submission_mode_enum,
          is_adjustment = true,
          updated_at = v_now
        where cw3.id = v_cw_id;

        v_ins_count := v_ins_count + 1;
        v_created_ts_ids := array_append(v_created_ts_ids, v_ts_id);
      end if;

    end loop; -- kind loop
  end loop; -- selected keys loop

  return jsonb_build_object(
    'import_id', p_import_id,
    'selected_count', coalesce(array_length(v_selected_keys, 1), 0),
    'skipped_count', v_skipped_count,
    'inserted_count', v_ins_count,
    'updated_count', v_upd_count,
    'created_timesheet_ids', to_jsonb(coalesce(v_created_ts_ids, '{}'::uuid[])),
    'updated_timesheet_ids', to_jsonb(coalesce(v_updated_ts_ids, '{}'::uuid[]))
  );
end;
$function$;
-- CloudTMS deployment metadata preserved from the installed TEST definition.
ALTER FUNCTION public.hr_weekly_phase3_apply_adjustment_truth(uuid, text[], jsonb, uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.hr_weekly_phase3_apply_adjustment_truth(uuid, text[], jsonb, uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.hr_weekly_phase3_apply_adjustment_truth(uuid, text[], jsonb, uuid) TO postgres, authenticated, service_role;
