-- CloudTMS reviewed direct replacement; review artifact only, not installed.
-- Exact TEST baseline body MD5 prefix: cb9477adff8a.
-- Ordinary and non-import-authoritative branches remain on the installed implementation.
CREATE OR REPLACE FUNCTION public.weekly_import_apply_cancellations(p_import_id uuid, p_actions jsonb, p_actor_user_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_now timestamptz := now();

  v_actions jsonb := coalesce(p_actions, '[]'::jsonb);
  v_cancelled_count int := 0;

  v_import_source_system text;
  v_import_client_id uuid;

  v_idx int;
  v_item jsonb;

  v_shift_id_text text;
  v_shift_id uuid;
  v_reason text;

  -- shift fields
  v_timesheet_id uuid;
  v_shift_invoice_id uuid;

  v_shift_source_system text;
  v_shift_candidate_id uuid;
  v_shift_client_id uuid;
  v_shift_contract_id uuid;
  v_shift_work_date date;
  v_shift_cancelled_at timestamptz;

  v_shift_external_row_key text;
  v_shift_hr_request_id text;
  v_shift_request_norm text;

  v_shift_start_utc timestamptz;
  v_shift_end_utc timestamptz;
  v_shift_break_mins int;
  v_shift_ward text;
  v_shift_week_ending_date date;

  -- ✅ evidence pointer: shift.latest_import_id (may be overridden by anchor evidence)
  v_shift_latest_import_id uuid;

  -- file request-id set
  v_file_request_count int := 0;
  v_present_in_file boolean := false;

  -- invoiced-at-all detection (segment-level)
  v_tf_locked_by_invoice_id uuid;
  v_tf_invoice_breakdown_json jsonb;

  v_seg_json jsonb := null;
  v_seg_invoice_id uuid;
  v_invoice_id_detected uuid := null;
  v_invoiced_detected boolean := false;

  v_branch text := null;

  -- correction timesheet creation
  v_base_ts_week_ending date;
  v_contract_week_ending_weekday_snapshot int := 0;
  v_week_ending_date date;

  v_contract_display_site text;
  v_contract_ward_hint text;
  v_contract_role text;

  v_client_name text;
  v_candidate_display_name text;
  v_candidate_tms_ref text;

  v_correction_id text;
  v_kind text := 'CANCEL_SHIFT_REVERSAL';

  v_shift_label text;
  v_shift_label_norm text;

  v_booking_base text;
  v_booking_id text;
  v_hash_hex text;

  v_schedule jsonb;
  v_hint jsonb;

  v_base_week_id uuid;

  v_existing_ts_id uuid;
  v_existing_cw_id uuid;

  v_cw_id uuid;
  v_next_additional_seq int;
  v_try int;

  v_ts_id uuid;
  v_correction_ts_id uuid;

  -- fnv1a32 helper vars (deterministic correction_id)
  v_fnv_h bigint;
  v_fnv_i int;
  v_fnv_s text;
  v_fnv_hex text;

  -- return arrays
  v_timesheet_ids uuid[] := array[]::uuid[];
  v_invoice_ids uuid[] := array[]::uuid[];
  v_credit_note_ids uuid[] := array[]::uuid[];
  v_pdf_jobs_enqueued int := 0;

  -- debug sample
  v_sample jsonb := '[]'::jsonb;
  v_sample_n int := 0;
  v_last_shift_id uuid := null;

  v_sqlstate text;
  v_err text;

  -- ✅ Cancellation anchor (what we reverse)
  v_anchor_start_utc timestamptz := null;
  v_anchor_end_utc timestamptz := null;
  v_anchor_break_mins int := 0;
  v_anchor_import_id uuid := null;

  -- ✅ Detect invoiced replacement (POS) to decide anchor
  v_pos_ts_id uuid := null;
  v_pos_schedule jsonb := null;
  v_pos_tf_locked_by_invoice_id uuid := null;
  v_pos_tf_invoice_breakdown_json jsonb := null;
  v_pos_seg_invoice_id uuid := null;
  v_pos_is_invoiced boolean := false;

  -- ✅ Base evidence import via existing CHANGED_HOURS_REVERSAL schedule (when POS is NOT invoiced)
  v_base_evidence_import_id uuid := null;

  -- ✅ Cleanup: remove uninvoiced CHANGED_HOURS corrections when cancelling (POS not invoiced)
  v_cleanup_ts_ids uuid[] := array[]::uuid[];
  v_cleanup_count int := 0;
  -- Canonical historical correction-chain evidence
  v_chain_scope jsonb := null;
  v_financial_preflight jsonb := null;
  v_correction_financials_policy_envelope jsonb := null;
  v_correction_financials_policy_envelope_fingerprint text := null;
  v_correction_operation_id uuid := null;
  v_root_timesheet_id uuid := null;
  v_latest_positive_timesheet_id uuid := null;
  v_reversal_ts_id uuid := null;
  v_replacement_ts_id uuid := null;
  v_replacement_cw_id uuid := null;
  v_replacement_booking_id text := null;
  v_pair_changed boolean := false;

begin
  -- Validate import exists and is HEALTHROSTER + has client_id (Guard B)
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
    raise exception 'weekly_import_apply_cancellations: import % not found in hr_imports.', p_import_id;
  end if;

  if v_import_source_system <> 'HEALTHROSTER' then
    raise exception 'weekly_import_apply_cancellations: import % source_system=%; expected HEALTHROSTER.', p_import_id, v_import_source_system;
  end if;

  if v_import_client_id is null then
    raise exception 'weekly_import_apply_cancellations: import % has null client_id (cannot apply HR cancellations safely).', p_import_id;
  end if;

  -- Validate actions payload
  if jsonb_typeof(v_actions) <> 'array' then
    raise exception 'weekly_import_apply_cancellations: p_actions must be a JSON array.';
  end if;

  if jsonb_array_length(v_actions) = 0 then
    return jsonb_build_object(
      'import_id', p_import_id,
      'cancelled_count', 0,
      'affected_timesheet_ids', to_jsonb(array[]::uuid[]),
      'affected_invoice_ids', to_jsonb(array[]::uuid[]),
      'credit_note_ids_created', to_jsonb(array[]::uuid[]),
      'invoice_pdf_jobs_enqueued', 0
    );
  end if;

  -- Build file request-id set (identity key = HR Request ID)
  create temporary table tmp_file_request_set(
    req_norm text primary key,
    req_raw  text
  ) on commit drop;

  insert into tmp_file_request_set(req_norm, req_raw)
  select
    lower(regexp_replace(btrim(src.req_raw), '\s+', ' ', 'g')) as req_norm,
    src.req_raw as req_raw
  from (
    select distinct
      nullif(
        btrim(
          coalesce(
            nullif(r.hr_request_id, ''),
            nullif(r.payload_json->>'request_id','')
          )
        ),
        ''
      ) as req_raw
    from public.hr_rows r
    where r.import_id = p_import_id
  ) as src
  where src.req_raw is not null
  on conflict (req_norm) do nothing;

  select count(*)::int
  into v_file_request_count
  from tmp_file_request_set;

  -- Apply is transactional: any invalid selected row fails whole apply.
  for v_idx, v_item in
    select (e.ord)::int, e.value
    from jsonb_array_elements(v_actions) with ordinality as e(value, ord)
  loop
    -- reset per-item
    v_anchor_start_utc := null;
    v_anchor_end_utc := null;
    v_anchor_break_mins := 0;
    v_anchor_import_id := null;

    v_pos_ts_id := null;
    v_pos_schedule := null;
    v_pos_tf_locked_by_invoice_id := null;
    v_pos_tf_invoice_breakdown_json := null;
    v_pos_seg_invoice_id := null;
    v_pos_is_invoiced := false;

    v_base_evidence_import_id := null;

    v_cleanup_ts_ids := array[]::uuid[];
    v_cleanup_count := 0;
    v_chain_scope := null;
    v_financial_preflight := null;
    v_correction_financials_policy_envelope := null;
    v_correction_financials_policy_envelope_fingerprint := null;
    v_root_timesheet_id := null;
    v_latest_positive_timesheet_id := null;
    v_reversal_ts_id := null;
    v_replacement_ts_id := null;
    v_replacement_cw_id := null;
    v_replacement_booking_id := null;
    v_pair_changed := false;

    v_seg_json := null;
    v_seg_invoice_id := null;

    -- Policy: no force
    if (v_item ? 'force') then
      raise exception 'weekly_import_apply_cancellations: item % contains disallowed field "force" (policy forbids force/override).', v_idx;
    end if;

    v_shift_id_text := nullif(btrim(coalesce(v_item->>'shift_id','')), '');
    v_reason := nullif(btrim(coalesce(v_item->>'reason','')), '');

    if v_shift_id_text is null then
      raise exception 'weekly_import_apply_cancellations: item % missing "shift_id".', v_idx;
    end if;
    if v_reason is null then
      raise exception 'weekly_import_apply_cancellations: item % missing "reason".', v_idx;
    end if;

    begin
      v_shift_id := v_shift_id_text::uuid;
    exception when invalid_text_representation then
      raise exception 'weekly_import_apply_cancellations: item % has invalid shift_id "%".', v_idx, v_shift_id_text;
    end;

    v_last_shift_id := v_shift_id;

    -- Lock shift row + load required fields
    select
      ns.timesheet_id,
      ns.invoice_id,
      upper(coalesce(ns.source_system::text,'')) as shift_source_system,
      ns.candidate_id,
      ns.client_id,
      ns.contract_id,
      ns.work_date,
      ns.cancelled_at_utc,
      ns.external_row_key,
      ns.hr_request_id,
      ns.start_utc,
      ns.end_utc,
      ns.break_mins,
      ns.ward,
      ns.week_ending_date,
      ns.latest_import_id
    into
      v_timesheet_id,
      v_shift_invoice_id,
      v_shift_source_system,
      v_shift_candidate_id,
      v_shift_client_id,
      v_shift_contract_id,
      v_shift_work_date,
      v_shift_cancelled_at,
      v_shift_external_row_key,
      v_shift_hr_request_id,
      v_shift_start_utc,
      v_shift_end_utc,
      v_shift_break_mins,
      v_shift_ward,
      v_shift_week_ending_date,
      v_shift_latest_import_id
    from public.nhsp_shifts ns
    where ns.id = v_shift_id
    for update;

    if not found then
      raise exception 'weekly_import_apply_cancellations: item % shift % not found in nhsp_shifts.', v_idx, v_shift_id;
    end if;

    if v_shift_cancelled_at is not null then
      raise exception 'weekly_import_apply_cancellations: item % shift % is already cancelled (cancelled_at_utc not null).', v_idx, v_shift_id;
    end if;

    -- Guard: cancellations RPC only operates on HEALTHROSTER shifts
    if v_shift_source_system <> 'HEALTHROSTER' then
      raise exception 'weekly_import_apply_cancellations: item % shift % source_system=%; expected HEALTHROSTER.',
        v_idx, v_shift_id, v_shift_source_system;
    end if;

    -- Guard B: shift must belong to the import client
    if v_shift_client_id is null or v_shift_client_id <> v_import_client_id then
      raise exception 'weekly_import_apply_cancellations: item % shift % client_id mismatch (import_client_id=% shift_client_id=%).',
        v_idx, v_shift_id, v_import_client_id, v_shift_client_id;
    end if;

    if v_shift_contract_id is null then
      raise exception 'weekly_import_apply_cancellations: item % shift % missing contract_id.', v_idx, v_shift_id;
    end if;

    if v_shift_candidate_id is null then
      raise exception 'weekly_import_apply_cancellations: item % shift % missing candidate_id.', v_idx, v_shift_id;
    end if;

    if v_shift_work_date is null then
      raise exception 'weekly_import_apply_cancellations: item % shift % missing work_date.', v_idx, v_shift_id;
    end if;

    -- Guard A: require non-empty nhsp_shifts.hr_request_id
    if nullif(btrim(coalesce(v_shift_hr_request_id,'')), '') is null then
      raise exception
        'weekly_import_apply_cancellations: item % shift % has empty hr_request_id; cannot use request-id cancellation identity.',
        v_idx, v_shift_id;
    end if;

    v_shift_request_norm := lower(regexp_replace(btrim(v_shift_hr_request_id), '\s+', ' ', 'g'));

    -- Presence test: if request id is present in file, cancellation is not eligible
    select exists (
      select 1
      from tmp_file_request_set fr
      where fr.req_norm = v_shift_request_norm
    )
    into v_present_in_file;

    if v_present_in_file then
      raise exception
        'weekly_import_apply_cancellations: item % shift % hr_request_id is present in the import file; cancellation rejected (not missing).',
        v_idx, v_shift_id;
    end if;

    -- Derive week_ending_date for cleanup/pos lookup
    v_week_ending_date := v_shift_week_ending_date;
    if v_week_ending_date is null then
      select coalesce(c.week_ending_weekday_snapshot, 0)
      into v_contract_week_ending_weekday_snapshot
      from public.contracts c
      where c.id = v_shift_contract_id
      limit 1;

      v_week_ending_date :=
        (v_shift_work_date + (((v_contract_week_ending_weekday_snapshot - extract(dow from v_shift_work_date)::int + 7) % 7))::int)::date;
    end if;

    if v_week_ending_date is null then
      raise exception 'weekly_import_apply_cancellations: shift % cannot resolve week_ending_date.', v_shift_id;
    end if;

    -- ─────────────────────────────────────────────
    -- Invoiced-at-all detection (segment-level authoritative)
    -- Also capture matched segment JSON for anchor when POS not invoiced
    -- ─────────────────────────────────────────────
    v_tf_locked_by_invoice_id := null;
    v_tf_invoice_breakdown_json := null;
    v_seg_invoice_id := null;
    v_invoice_id_detected := null;
    v_invoiced_detected := false;

    if v_timesheet_id is not null then
      select
        tf.locked_by_invoice_id,
        tf.invoice_breakdown_json
      into
        v_tf_locked_by_invoice_id,
        v_tf_invoice_breakdown_json
      from public.timesheets_financials tf
      where tf.timesheet_id = v_timesheet_id
        and tf.is_current = true
      limit 1;

      begin
        select s2.seg
        into v_seg_json
        from (
          select s2.seg
          from jsonb_array_elements(
            case
              when v_tf_invoice_breakdown_json is not null
               and jsonb_typeof(v_tf_invoice_breakdown_json) = 'object'
               and upper(coalesce(v_tf_invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
               and jsonb_typeof(v_tf_invoice_breakdown_json->'segments') = 'array'
              then v_tf_invoice_breakdown_json->'segments'
              else '[]'::jsonb
            end
          ) as s2(seg)
          where
            (s2.seg->>'nhsp_shift_id') = v_shift_id::text
            or (
              v_shift_external_row_key is not null
              and (s2.seg->>'external_row_key') = v_shift_external_row_key
            )
          order by
            case when (s2.seg->>'nhsp_shift_id') = v_shift_id::text then 0 else 1 end
          limit 1
        ) as s2;
      exception when others then
        v_seg_json := null;
      end;

      if v_seg_json is not null then
        begin
          v_seg_invoice_id := nullif(btrim(coalesce(v_seg_json->>'invoice_locked_invoice_id','')), '')::uuid;
        exception when others then
          v_seg_invoice_id := null;
        end;
      end if;
    end if;

    v_invoice_id_detected := coalesce(v_seg_invoice_id, v_tf_locked_by_invoice_id, v_shift_invoice_id);
    v_invoiced_detected := (v_invoice_id_detected is not null);

    if v_invoice_id_detected is not null then
      v_invoice_ids := array_append(v_invoice_ids, v_invoice_id_detected);
    end if;

    -- ─────────────────────────────────────────────

    if v_timesheet_id is not null then
      select public.import_timesheet_financial_preflight_v1(
        p_timesheet_ids := array[v_timesheet_id]::uuid[],
        p_action := 'IMPORT_CANCELLATION',
        p_actor_user_id := p_actor_user_id,
        p_expected_state_json := '{}'::jsonb,
        p_lock_rows := true,
        p_max_scope := 100
      ) into v_financial_preflight;

      if coalesce((v_financial_preflight->>'allowed')::boolean,false) is not true then
        raise exception using message='IMPORT_FINANCIAL_PREFLIGHT_BLOCKED', errcode='P0001', detail=v_financial_preflight::text;
      end if;

      if v_financial_preflight->>'required_path' = 'CREATE_OR_UPDATE_CORRECTION_CHAIN'
         and v_invoiced_detected is false then
        select il.invoice_id
        into v_invoice_id_detected
        from public.invoice_lines il
        join public.invoices i on i.id=il.invoice_id
        where il.timesheet_id=v_timesheet_id
        order by coalesce(i.issued_at_utc,i.created_at) desc, il.invoice_id desc
        limit 1;

        if v_invoice_id_detected is null then
          raise exception using message='IMPORT_INVOICE_EVIDENCE_INCOMPLETE',errcode='P0001',
            detail=jsonb_build_object(
              'code','IMPORT_INVOICE_EVIDENCE_INCOMPLETE',
              'timesheet_id',v_timesheet_id,
              'required_path','CREATE_OR_UPDATE_CORRECTION_CHAIN'
            )::text;
        end if;

        v_invoiced_detected := true;
        v_invoice_ids := array_append(v_invoice_ids,v_invoice_id_detected);
      end if;
    end if;

    -- Branch: INPLACE vs CORRECTION
    -- ─────────────────────────────────────────────
    if v_invoiced_detected = false then
      v_branch := 'INPLACE';

      if v_timesheet_id is not null and exists (
        select 1
        from public.timesheets guard_ts
        left join public.timesheets_financials guard_tf
          on guard_tf.timesheet_id=guard_ts.timesheet_id and guard_tf.is_current=true
        where guard_ts.timesheet_id=v_timesheet_id
          and (guard_ts.authorised_at_server is not null or guard_tf.authorised_at_utc is not null)
          and not exists (
            select 1 from public.timesheets_financials paid_guard
            where paid_guard.timesheet_id=guard_ts.timesheet_id
              and paid_guard.paid_at_utc is not null
          )
      ) then
        raise exception using message='CANONICAL_UNAUTHORISE_REQUIRED', errcode='P0001',
          detail=jsonb_build_object(
            'code','CANONICAL_UNAUTHORISE_REQUIRED','timesheet_id',v_timesheet_id,
            'required_path',jsonb_build_array('UNAUTHORISE','AMEND','RECALCULATE','REAUTHORISE'),
            'paid_uninvoiced_rollover_required',false
          )::text;
      end if;

      if v_timesheet_id is not null
         and exists (select 1 from public.timesheets_financials paid_tf where paid_tf.timesheet_id=v_timesheet_id and paid_tf.paid_at_utc is not null)
         and not exists (
           select 1 from public.timesheets_financials current_tf
           where current_tf.timesheet_id=v_timesheet_id and current_tf.is_current=true
             and current_tf.stale_reason='IMPORT_PAID_TSFIN_ROLLOVER_PENDING_CALCULATION'
             and coalesce((current_tf.policy_snapshot_json->>'requires_frozen_correction_policy')::boolean,false)=true
         ) then
        raise exception using message='PAID_UNINVOICED_ROLLOVER_REQUIRED', errcode='P0001',
          detail=jsonb_build_object(
            'code','PAID_UNINVOICED_ROLLOVER_REQUIRED','timesheet_id',v_timesheet_id,
            'required_path',jsonb_build_array(
              'UNAUTHORISE','PAID_UNINVOICED_ROLLOVER','AMEND','RECALCULATE','REAUTHORISE'
            ),
            'invoice_policy_without_history','NOW',
            'replacement_timesheet_required',false
          )::text;
      end if;


      -- Cancel truth + detach
      update public.nhsp_shifts ns2
      set
        cancelled_at_utc = v_now,
        cancelled_by_import_id = p_import_id,
        cancelled_reason = v_reason,
        timesheet_id = null
      where ns2.id = v_shift_id;

      v_cancelled_count := v_cancelled_count + 1;

      -- TSFIN recompute required for base timesheet
      if v_timesheet_id is not null then
        v_timesheet_ids := array_append(v_timesheet_ids, v_timesheet_id);

        update public.timesheets_financials tfu
        set
          is_stale = true,
          stale_reason = 'IMPORT_CANCEL_DETACH',
          updated_at = v_now
        where tfu.is_current = true
          and tfu.timesheet_id = v_timesheet_id;

        perform public.enqueue_ts_financials_priority(array[v_timesheet_id]::uuid[], 'CONTEXT_CHANGED'::public.ts_fin_reason_enum);
      end if;

      v_correction_ts_id := null;

    else
      v_branch := 'CORRECTION';

      if v_timesheet_id is null then
        raise exception using message='CORRECTION_BASE_TIMESHEET_REQUIRED', errcode='P0001',
          detail=jsonb_build_object('code','CORRECTION_BASE_TIMESHEET_REQUIRED','shift_id',v_shift_id)::text;
      end if;

      select public.timesheet_correction_chain_scope_v1(
        v_timesheet_id, true, 32, 100
      ) into v_chain_scope;

      if coalesce((v_chain_scope->>'valid')::boolean,false) is not true then
        raise exception using message='CORRECTION_CHAIN_UNRESOLVED', errcode='P0001', detail=v_chain_scope::text;
      end if;

      v_root_timesheet_id := nullif(v_chain_scope->>'root_timesheet_id','')::uuid;
      v_latest_positive_timesheet_id := coalesce(
        nullif(v_chain_scope->>'latest_positive_timesheet_id','')::uuid,
        v_timesheet_id
      );
      v_correction_operation_id := public._ctms_import_correction_operation_find_v1(
        p_import_id,
        v_root_timesheet_id,
        v_shift_external_row_key,
        'CANCELLATION',
        'REVERSAL_ONLY'
      );
      update public.nhsp_shifts canonical_cancel
      set cancelled_at_utc=coalesce(canonical_cancel.cancelled_at_utc,v_now),
          cancelled_by_import_id=p_import_id
      where canonical_cancel.id=v_shift_id
        and (canonical_cancel.cancelled_by_import_id is null
             or canonical_cancel.cancelled_by_import_id=p_import_id);
      if not found then
        raise exception 'CANCELLATION_IMPORT_EVIDENCE_CONFLICT' using errcode='P0001';
      end if;
      v_correction_financials_policy_envelope := public.correction_financials_policy_resolve_v1(
        v_timesheet_id,
        v_correction_operation_id,
        v_shift_external_row_key,
        'CANCELLATION',
        null::text,
        true,
        32
      );
      v_correction_financials_policy_envelope_fingerprint :=
        v_correction_financials_policy_envelope ->> 'envelope_fingerprint';
      v_kind := 'CANCELLATION_REVERSAL';

      -- ✅ Determine cancellation anchor:
      -- Default anchor = current shift truth (fallback only)
      v_anchor_start_utc := v_shift_start_utc;
      v_anchor_end_utc := v_shift_end_utc;
      v_anchor_break_mins := greatest(0, coalesce(v_shift_break_mins, 0));
      v_anchor_import_id := v_shift_latest_import_id;

      -- Find latest POS (replacement) correction timesheet for this shift/week
      begin
        select
          tpos.timesheet_id,
          tpos.actual_schedule_json
        into
          v_pos_ts_id,
          v_pos_schedule
        from public.timesheets tpos
        where tpos.is_adjustment is true
          and tpos.is_current is true
          and tpos.correction_kind = 'CHANGED_HOURS_REPLACEMENT'
          and tpos.contract_id = v_shift_contract_id
          and tpos.week_ending_date = v_week_ending_date
          and jsonb_typeof(tpos.actual_schedule_json) = 'array'
          and (
            tpos.actual_schedule_json @> jsonb_build_array(jsonb_build_object('shift_id', v_shift_id::text))
            or (
              v_shift_external_row_key is not null
              and tpos.actual_schedule_json @> jsonb_build_array(jsonb_build_object('external_row_key', v_shift_external_row_key))
            )
          )
        order by tpos.updated_at desc nulls last, tpos.created_at desc nulls last
        limit 1
        for update;
      exception when others then
        v_pos_ts_id := null;
        v_pos_schedule := null;
      end;

      if v_pos_ts_id is not null then
        select
          tf.locked_by_invoice_id,
          tf.invoice_breakdown_json
        into
          v_pos_tf_locked_by_invoice_id,
          v_pos_tf_invoice_breakdown_json
        from public.timesheets_financials tf
        where tf.timesheet_id = v_pos_ts_id
          and tf.is_current = true
        order by tf.created_at desc
        limit 1;

        begin
          select
            nullif(btrim(coalesce(s2.seg->>'invoice_locked_invoice_id','')), '')::uuid
          into v_pos_seg_invoice_id
          from (
            select s2.seg
            from jsonb_array_elements(
              case
                when v_pos_tf_invoice_breakdown_json is not null
                 and jsonb_typeof(v_pos_tf_invoice_breakdown_json)='object'
                 and jsonb_typeof(v_pos_tf_invoice_breakdown_json->'segments')='array'
                then v_pos_tf_invoice_breakdown_json->'segments'
                else '[]'::jsonb
              end
            ) s2(seg)
            where nullif(btrim(coalesce(s2.seg->>'invoice_locked_invoice_id','')), '') is not null
            limit 1
          ) as s2;
        exception when others then
          v_pos_seg_invoice_id := null;
        end;

        v_pos_is_invoiced :=
          (v_pos_tf_locked_by_invoice_id is not null)
          or (v_pos_seg_invoice_id is not null);

        -- If POS is invoiced, reverse POS (anchor = POS schedule)
        if v_pos_is_invoiced is true and v_pos_schedule is not null and jsonb_typeof(v_pos_schedule) = 'array' then
          begin
            v_anchor_start_utc := nullif(btrim(coalesce((v_pos_schedule->0)->>'start_utc','')), '')::timestamptz;
          exception when others then
            v_anchor_start_utc := v_shift_start_utc;
          end;

          begin
            v_anchor_end_utc := nullif(btrim(coalesce((v_pos_schedule->0)->>'end_utc','')), '')::timestamptz;
          exception when others then
            v_anchor_end_utc := v_shift_end_utc;
          end;

          begin
            v_anchor_break_mins := greatest(
              0,
              coalesce(nullif(btrim(coalesce((v_pos_schedule->0)->>'break_mins','')), '')::int, 0)
            );
          exception when others then
            v_anchor_break_mins := greatest(0, coalesce(v_shift_break_mins, 0));
          end;

          begin
            if ((v_pos_schedule->0) ? 'import_id')
              and nullif(btrim(coalesce((v_pos_schedule->0)->>'import_id','')), '') is not null
              and (v_pos_schedule->0)->>'import_id' ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            then
              v_anchor_import_id := ((v_pos_schedule->0)->>'import_id')::uuid;
            end if;
          exception when others then
            null;
          end;
        end if;
      end if;

      -- If POS is NOT invoiced, anchor to base locked segment (and use base evidence import_id when available)
      if v_pos_is_invoiced is not true then
        -- Base evidence import id: from CHANGED_HOURS_REVERSAL schedule if present
        begin
          select
            nullif(btrim(coalesce((tneg.actual_schedule_json->0)->>'import_id','')), '')::uuid
          into v_base_evidence_import_id
          from public.timesheets tneg
          where tneg.is_adjustment is true
            and tneg.is_current is true
            and tneg.correction_kind = 'CHANGED_HOURS_REVERSAL'
            and tneg.contract_id = v_shift_contract_id
            and tneg.week_ending_date = v_week_ending_date
            and jsonb_typeof(tneg.actual_schedule_json)='array'
            and (
              tneg.actual_schedule_json @> jsonb_build_array(jsonb_build_object('shift_id', v_shift_id::text))
              or (
                v_shift_external_row_key is not null
                and tneg.actual_schedule_json @> jsonb_build_array(jsonb_build_object('external_row_key', v_shift_external_row_key))
              )
            )
          order by tneg.updated_at desc nulls last, tneg.created_at desc nulls last
          limit 1;
        exception when others then
          v_base_evidence_import_id := null;
        end;

        if v_base_evidence_import_id is not null then
          v_anchor_import_id := v_base_evidence_import_id;
        end if;

        if v_seg_json is not null then
          begin
            if nullif(btrim(coalesce(v_seg_json->>'start_utc','')), '') is not null then
              v_anchor_start_utc := (v_seg_json->>'start_utc')::timestamptz;
            end if;
          exception when others then
            null;
          end;

          begin
            if nullif(btrim(coalesce(v_seg_json->>'end_utc','')), '') is not null then
              v_anchor_end_utc := (v_seg_json->>'end_utc')::timestamptz;
            end if;
          exception when others then
            null;
          end;

          begin
            if nullif(btrim(coalesce(v_seg_json->>'break_mins','')), '') is not null then
              v_anchor_break_mins := greatest(0, (v_seg_json->>'break_mins')::int);
            end if;
          exception when others then
            null;
          end;
        end if;
      end if;

      if v_anchor_start_utc is null or v_anchor_end_utc is null then
        raise exception 'weekly_import_apply_cancellations: shift % missing anchor start/end; cannot create schedule-driven cancellation correction.', v_shift_id;
      end if;

      -- Deterministic correction id (fnv1a32 over stable string using anchor times)
      v_fnv_s :=
        coalesce(p_import_id::text,'') || '|' ||
        coalesce(v_shift_id::text,'') || '|' ||
        coalesce(v_shift_hr_request_id,'') || '|' ||
        coalesce(coalesce(v_shift_external_row_key,''),'') || '|' ||
        coalesce(coalesce(v_anchor_start_utc::text,''),'') || '|' ||
        coalesce(coalesce(v_anchor_end_utc::text,''),'') || '|' ||
        coalesce(coalesce(v_anchor_break_mins,0)::text,'');

      v_fnv_h := 2166136261;
      for v_fnv_i in 1..char_length(v_fnv_s) loop
        v_fnv_h := (v_fnv_h # ascii(substring(v_fnv_s from v_fnv_i for 1)));
        v_fnv_h := (v_fnv_h * 16777619) % 4294967296;
      end loop;
      v_fnv_hex := lpad(lower(to_hex(v_fnv_h)), 8, '0');

      v_correction_id := 'hrcan:' || p_import_id::text || ':' || v_shift_id::text || ':' || v_fnv_hex;

      -- Load contract display fields (best-effort; may be null)
      select
        c2.display_site,
        c2.ward_hint,
        c2.role
      into
        v_contract_display_site,
        v_contract_ward_hint,
        v_contract_role
      from public.contracts c2
      where c2.id = v_shift_contract_id
      limit 1;

      select cl.name
      into v_client_name
      from public.clients cl
      where cl.id = v_shift_client_id
      limit 1;

      select cand.display_name, cand.tms_ref
      into v_candidate_display_name, v_candidate_tms_ref
      from public.candidates cand
      where cand.id = v_shift_candidate_id
      limit 1;

      -- Schedule entry (anchor-based) + evidence import_id
      v_schedule := jsonb_build_array(
        jsonb_build_object(
          'date', v_shift_work_date::text,
          'ward', nullif(btrim(coalesce(v_shift_ward, v_contract_ward_hint, '')), ''),
          'start_utc', v_anchor_start_utc::text,
          'end_utc', v_anchor_end_utc::text,
          'start', to_char((v_anchor_start_utc at time zone 'Europe/London')::time, 'HH24:MI'),
          'end', to_char((v_anchor_end_utc at time zone 'Europe/London')::time, 'HH24:MI'),
          'break_mins', greatest(0, coalesce(v_anchor_break_mins, 0)),
          'hr_request_id', nullif(btrim(coalesce(v_shift_hr_request_id,'')), ''),
          'ref_num', nullif(btrim(coalesce(v_shift_hr_request_id,'')), ''),
          'shift_id', v_shift_id::text,
          'external_row_key', v_shift_external_row_key,
          'import_id', case when v_anchor_import_id is null then null else v_anchor_import_id::text end
        )
      );

      v_hint := jsonb_build_object(
        'import_cancellation', jsonb_build_object(
          'import_id', p_import_id::text,
          'trigger_import_id', p_import_id::text,
          'evidence_import_id', case when v_anchor_import_id is null then null else v_anchor_import_id::text end,
          'key_type', 'HR_REQUEST_ID',
          'hr_request_id', nullif(btrim(coalesce(v_shift_hr_request_id,'')), ''),
          'ref_num', nullif(btrim(coalesce(v_shift_hr_request_id,'')), ''),
          'external_row_key', v_shift_external_row_key,
          'shift_id', v_shift_id::text,
          'correction_id', v_correction_id,
          'correction_kind', 'CANCELLATION_REVERSAL_ONLY',
              'correction_financials_policy_envelope_fingerprint', v_correction_financials_policy_envelope_fingerprint,
          'invoice_id_detected', case when v_invoice_id_detected is null then null else v_invoice_id_detected::text end,
          'anchor_mode', case when v_pos_is_invoiced is true then 'INVOICED_REPLACEMENT' else 'BASE_LOCKED' end
        )
      );
      v_hint := v_hint || jsonb_build_object(
        'correction_financials_policy_envelope',v_correction_financials_policy_envelope,
        'correction_financials_policy_envelope_fingerprint',v_correction_financials_policy_envelope_fingerprint,
        'root_timesheet_id',v_root_timesheet_id::text,
        'latest_positive_timesheet_id',v_latest_positive_timesheet_id::text
      );

      v_shift_label := 'weekly-hr-cancel-reversal-' || v_correction_id;

      v_shift_label_norm :=
        regexp_replace(
          regexp_replace(lower(trim(v_shift_label)), '\s+', ' ', 'g'),
          '[^\w\s\-@&\/,:]',
          '',
          'g'
        );

      v_booking_base :=
        'scope=WEEKLY' || '|' ||
        'contract_id=' || coalesce(v_shift_contract_id::text,'') || '|' ||
        'candidate_id=' || coalesce(v_shift_candidate_id::text,'') || '|' ||
        'client_id=' || coalesce(v_shift_client_id::text,'') || '|' ||
        'week_ending_date=' || coalesce(v_week_ending_date::text,'') || '|' ||
        'correction_id=' || coalesce(v_correction_id,'') || '|' ||
        'correction_kind=' || v_kind;

      v_hash_hex := substring(encode(extensions.digest(convert_to(v_booking_base, 'utf8'), 'sha256'::text), 'hex') from 1 for 16);
      v_booking_id := 'bk_' || v_hash_hex;

      -- Ensure base contract_week exists (seq=0). Do not overwrite if it exists.
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
        v_shift_contract_id,
        v_week_ending_date,
        0,
        'OPEN'::public.contract_week_status_enum,
        'MANUAL'::public.submission_mode_enum,
        null,
        null,
        v_now,
        v_now,
        false
      )
      on conflict (contract_id, week_ending_date, additional_seq) do nothing;

      select cw0.id
      into v_base_week_id
      from public.contract_weeks cw0
      where cw0.contract_id = v_shift_contract_id
        and cw0.week_ending_date = v_week_ending_date
        and cw0.additional_seq = 0
      limit 1
      for update;

      if v_base_week_id is null then
        raise exception 'weekly_import_apply_cancellations: failed to ensure base contract_week exists (contract_id=% week_ending=%).',
          v_shift_contract_id, v_week_ending_date;
      end if;

      -- Idempotency: reuse existing correction timesheet (correction_id+kind)
      v_existing_ts_id := null;

      select t2.timesheet_id
      into v_existing_ts_id
      from public.timesheets t2
      where t2.correction_id = v_correction_id
        and t2.correction_kind in (v_kind, 'CANCEL_SHIFT_REVERSAL')
      order by
        case when t2.correction_kind=v_kind then 0 else 1 end,
        t2.is_current desc,
        t2.version desc,
        t2.timesheet_id
      limit 1
      for update;

      if v_existing_ts_id is not null then
        v_correction_ts_id := v_existing_ts_id;

        -- Ensure adjustment contract_week exists and links to the correction timesheet
        v_existing_cw_id := null;

        select cw2.id
        into v_existing_cw_id
        from public.contract_weeks cw2
        where cw2.timesheet_id = v_existing_ts_id
          and cw2.contract_id = v_shift_contract_id
          and cw2.week_ending_date = v_week_ending_date
        limit 1
        for update;

        if v_existing_cw_id is null then
          perform 1
          from public.contract_weeks cwlock
          where cwlock.contract_id = v_shift_contract_id
            and cwlock.week_ending_date = v_week_ending_date
          for update;

          v_try := 0;
          loop
            v_try := v_try + 1;
            if v_try > 10 then
              raise exception 'weekly_import_apply_cancellations: failed to allocate additional_seq after retries (contract_id=% week_ending=%).',
                v_shift_contract_id, v_week_ending_date;
            end if;

            select coalesce(max(cwmax.additional_seq), 0) + 1
            into v_next_additional_seq
            from public.contract_weeks cwmax
            where cwmax.contract_id = v_shift_contract_id
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
                v_shift_contract_id,
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
              v_existing_cw_id := null;
            end;
          end loop;
        end if;

        if exists (
          select 1
          from public.timesheets desired_reversal
          where desired_reversal.timesheet_id=v_existing_ts_id
            and (
              desired_reversal.actual_schedule_json is distinct from v_schedule
              or desired_reversal.parent_timesheet_id is distinct from v_latest_positive_timesheet_id
              or desired_reversal.contract_id is distinct from v_shift_contract_id
              or desired_reversal.week_ending_date is distinct from v_week_ending_date
              or desired_reversal.correction_id is distinct from v_correction_id
              or desired_reversal.correction_kind is distinct from v_kind
              or coalesce(desired_reversal.candidate_hint_text->>'correction_financials_policy_envelope_fingerprint','')
                   is distinct from coalesce(v_correction_financials_policy_envelope_fingerprint,'')
            )
        ) then
          if exists (
            select 1
            from public.timesheets legacy_reversal
            left join public.timesheets_financials legacy_financial
              on legacy_financial.timesheet_id=legacy_reversal.timesheet_id
             and legacy_financial.is_current=true
            where legacy_reversal.timesheet_id=v_existing_ts_id
              and legacy_reversal.correction_kind='CANCEL_SHIFT_REVERSAL'
              and (
                legacy_reversal.authorised_at_server is not null
                or legacy_financial.authorised_at_utc is not null
                or legacy_financial.paid_at_utc is not null
                or legacy_financial.locked_by_invoice_id is not null
                or exists(select 1 from public.invoice_lines legacy_line where legacy_line.timesheet_id=legacy_reversal.timesheet_id)
              )
          ) then
            raise exception using message='LEGACY_CANCELLATION_CORRECTION_MIGRATION_REQUIRED',errcode='P0001',
              detail=jsonb_build_object('code','LEGACY_CANCELLATION_CORRECTION_MIGRATION_REQUIRED','timesheet_id',v_existing_ts_id,'correction_id',v_correction_id)::text;
          end if;

          if exists (
            select 1
            from public.timesheets guarded_reversal
            left join public.timesheets_financials guarded_reversal_financial
              on guarded_reversal_financial.timesheet_id=guarded_reversal.timesheet_id
             and guarded_reversal_financial.is_current=true
            where guarded_reversal.timesheet_id=v_existing_ts_id
              and (
                guarded_reversal.authorised_at_server is not null
                or guarded_reversal_financial.authorised_at_utc is not null
                or guarded_reversal_financial.paid_at_utc is not null
                or guarded_reversal_financial.locked_by_invoice_id is not null
                or exists(select 1 from public.invoice_lines guarded_line where guarded_line.timesheet_id=guarded_reversal.timesheet_id)
              )
          ) then
            raise exception using message='CORRECTION_PAIR_LIFECYCLE_TRANSITION_REQUIRED',errcode='P0001',
              detail=jsonb_build_object('code','CORRECTION_PAIR_LIFECYCLE_TRANSITION_REQUIRED','timesheet_id',v_existing_ts_id,'required_path','PAIR_UNAUTHORISE_AMEND_RECALCULATE_REAUTHORISE')::text;
          end if;

          update public.timesheets tu
          set
            booking_id = v_booking_id,
            is_current = true,
            status = 'RECEIVED'::public.timesheet_status_enum,
            sheet_scope = 'WEEKLY'::public.timesheet_scope_enum,
            submission_mode = 'MANUAL'::public.submission_mode_enum,
            line_type = 'HOURS'::public.timesheet_line_type_enum,
            authorised_at_server = null,
            occupant_key_norm = lower(coalesce(v_candidate_tms_ref, v_candidate_display_name, v_shift_candidate_id::text)),
            hospital_norm = lower(coalesce(v_contract_display_site, v_client_name, v_shift_client_id::text)),
            ward_norm = lower(coalesce(v_contract_ward_hint, 'contract')),
            job_title_norm = lower(coalesce(v_contract_role, 'weekly')),
            shift_label_norm = v_shift_label_norm,
            week_ending_date = v_week_ending_date,
            contract_id = v_shift_contract_id,
            manual_pdf_r2_key = null,
            actual_schedule_json = v_schedule,
            qr_payload_json = v_hint,
            candidate_hint_text = v_hint,
            is_adjustment = true,
            parent_timesheet_id = v_latest_positive_timesheet_id,
            correction_id = v_correction_id,
            correction_kind = v_kind,
            adjustment_origin = 'IMPORT_CANCELLATION',
            updated_at = v_now
          where tu.timesheet_id = v_existing_ts_id;

          v_pair_changed := true;
        end if;

      else
        -- Create a new adjustment contract_week and new correction timesheet linked to it
        perform 1
        from public.contract_weeks cwlock2
        where cwlock2.contract_id = v_shift_contract_id
          and cwlock2.week_ending_date = v_week_ending_date
        for update;

        v_try := 0;
        loop
          v_try := v_try + 1;
          if v_try > 10 then
            raise exception 'weekly_import_apply_cancellations: failed to allocate additional_seq after retries (contract_id=% week_ending=%).',
              v_shift_contract_id, v_week_ending_date;
          end if;

          select coalesce(max(cwmax2.additional_seq), 0) + 1
          into v_next_additional_seq
          from public.contract_weeks cwmax2
          where cwmax2.contract_id = v_shift_contract_id
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
              v_shift_contract_id,
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

        v_ts_id := null;

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
          submission_mode,
          manual_pdf_r2_key,
          line_type,
          sheet_scope,
          actual_schedule_json,
          additional_units_week,
          additional_units_per_day,
          day_references_json,
          authorised_at_server,
          qr_payload_json,
          is_adjustment,
          candidate_hint_text,
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
          lower(coalesce(v_candidate_tms_ref, v_candidate_display_name, v_shift_candidate_id::text)),
          lower(coalesce(v_contract_display_site, v_client_name, v_shift_client_id::text)),
          lower(coalesce(v_contract_ward_hint, 'contract')),
          lower(coalesce(v_contract_role, 'weekly')),
          v_shift_label_norm,
          v_week_ending_date,
          v_shift_contract_id,
          'MANUAL'::public.submission_mode_enum,
          null,
          'HOURS'::public.timesheet_line_type_enum,
          'WEEKLY'::public.timesheet_scope_enum,
          v_schedule,
          '{}'::jsonb,
          '{}'::jsonb,
          null,
          null,
          v_hint,
          true,
          v_hint,
          v_latest_positive_timesheet_id,
          v_correction_id,
          v_kind,
          'IMPORT_CANCELLATION',
          v_now,
          v_now
        )
        returning timesheet_id into v_ts_id;

        v_correction_ts_id := v_ts_id;
        v_pair_changed := true;

        update public.contract_weeks cwlink
        set
          timesheet_id = v_correction_ts_id,
          status = 'SUBMITTED'::public.contract_week_status_enum,
          submission_mode_snapshot = 'MANUAL'::public.submission_mode_enum,
          is_adjustment = true,
          updated_at = v_now
        where cwlink.id = v_cw_id;

      end if;


      v_reversal_ts_id := v_correction_ts_id;
      if exists (
        select 1 from public.timesheets legacy_replacement
        where legacy_replacement.correction_id=v_correction_id
          and legacy_replacement.correction_kind in ('CHANGED_HOURS_REPLACEMENT','CANCELLATION_REPLACEMENT')
      ) then
        raise exception 'LEGACY_ZERO_HOUR_REPLACEMENT_REQUIRES_RECONCILIATION'
          using errcode='P0001',detail=jsonb_build_object('correction_id',v_correction_id)::text;
      end if;
      v_replacement_ts_id := null;
      v_correction_ts_id := v_reversal_ts_id;
      v_timesheet_ids := array_append(v_timesheet_ids,v_reversal_ts_id);
      if v_pair_changed then
        perform public.enqueue_ts_financials_priority(
          array[v_reversal_ts_id]::uuid[],
          'CONTEXT_CHANGED'::public.ts_fin_reason_enum
        );
      end if;

      -- Update truth (cancel) + detach; do NOT recompute base TSFIN here
      update public.nhsp_shifts ns3
      set
        cancelled_at_utc = v_now,
        cancelled_by_import_id = p_import_id,
        cancelled_reason = v_reason,
        timesheet_id = null
      where ns3.id = v_shift_id;

      v_cancelled_count := v_cancelled_count + 1;

      -- Retained financial history is never deleted. Existing changed-hours
      -- corrections stay in the chain and are reconciled by the reversal-only correction unit.
      v_cleanup_ts_ids := array[]::uuid[];
      v_cleanup_count := 0;

      -- ✅ User-facing audit (UNGATED): correction timesheet + invoice history
      begin
        if v_correction_ts_id is not null then
          perform public._audit_insert(
            'timesheets',
            v_correction_ts_id::text,
            'HR_IMPORT_CANCELLATION_CORRECTION_CREATED',
            null,
            jsonb_build_object(
              'trigger_import_id', p_import_id::text,
              'evidence_import_id', case when v_anchor_import_id is null then null else v_anchor_import_id::text end,
              'branch', v_branch,
              'shift_id', v_shift_id::text,
              'work_date', v_shift_work_date::text,
              'external_row_key', v_shift_external_row_key,
              'hr_request_id', nullif(btrim(coalesce(v_shift_hr_request_id,'')), ''),
              'ref_num', nullif(btrim(coalesce(v_shift_hr_request_id,'')), ''),
              'cancel_reason', v_reason,
              'invoice_id', case when v_invoice_id_detected is null then null else v_invoice_id_detected::text end,
              'correction_id', v_correction_id,
              'correction_kind', 'CANCELLATION_REVERSAL_ONLY',
              'correction_financials_policy_envelope_fingerprint', v_correction_financials_policy_envelope_fingerprint,
              'anchor_mode', case when v_pos_is_invoiced is true then 'INVOICED_REPLACEMENT' else 'BASE_LOCKED' end,
              'retained_changed_hours_count', v_cleanup_count,
              'retained_timesheet_ids', to_jsonb(coalesce(v_cleanup_ts_ids, array[]::uuid[]))
            ),
            'IMPORT_CANCELLATION_CORRECTION',
            p_actor_user_id
          );
        end if;

        if v_invoice_id_detected is not null then
          perform public._inv_write_audit(
            p_actor_user_id,
            'HR_IMPORT_CANCELLATION_CORRECTION_CREATED',
            jsonb_build_object(
              'trigger_import_id', p_import_id::text,
              'evidence_import_id', case when v_anchor_import_id is null then null else v_anchor_import_id::text end,
              'shift_id', v_shift_id::text,
              'work_date', v_shift_work_date::text,
              'external_row_key', v_shift_external_row_key,
              'hr_request_id', nullif(btrim(coalesce(v_shift_hr_request_id,'')), ''),
              'ref_num', nullif(btrim(coalesce(v_shift_hr_request_id,'')), ''),
              'invoice_id', v_invoice_id_detected::text,
              'correction_timesheet_id', case when v_correction_ts_id is null then null else v_correction_ts_id::text end,
              'correction_id', v_correction_id,
              'correction_kind', 'CANCELLATION_REVERSAL_ONLY',
              'correction_financials_policy_envelope_fingerprint', v_correction_financials_policy_envelope_fingerprint,
              'anchor_mode', case when v_pos_is_invoiced is true then 'INVOICED_REPLACEMENT' else 'BASE_LOCKED' end,
              'retained_changed_hours_count', v_cleanup_count
            ),
            'invoices',
            v_invoice_id_detected::text,
            null,
            'IMPORT_CANCELLATION_CORRECTION',
            null,
            null,
            null
          );
        end if;
      exception when others then
        null;
      end;

    end if;

    -- Debug sample (cap 30)
    if v_sample_n < 30 then
      v_sample := v_sample || jsonb_build_array(jsonb_build_object(
        'shift_id', v_shift_id::text,
        'key_type', 'HR_REQUEST_ID',
        'hr_request_id', v_shift_hr_request_id,
        'present_in_file', v_present_in_file,
        'timesheet_id', case when v_timesheet_id is null then null else v_timesheet_id::text end,
        'invoice_id_detected', case when v_invoice_id_detected is null then null else v_invoice_id_detected::text end,
        'invoiced_detected', v_invoiced_detected,
        'branch', v_branch,
        'correction_timesheet_id', case when v_correction_ts_id is null then null else v_correction_ts_id::text end,
        'anchor_mode', case when v_pos_is_invoiced is true then 'INVOICED_REPLACEMENT' else 'BASE_LOCKED' end,
        'retained_changed_hours_count', v_cleanup_count
      ));
      v_sample_n := v_sample_n + 1;
    end if;

  end loop;

  -- Deduplicate arrays
  select coalesce(array_agg(distinct x), array[]::uuid[])
  into v_timesheet_ids
  from unnest(v_timesheet_ids) x
  where x is not null;

  select coalesce(array_agg(distinct x), array[]::uuid[])
  into v_invoice_ids
  from unnest(v_invoice_ids) x
  where x is not null;

  -- Debug audit (invoice_debug gated inside _imp_debug_audit)
  perform public._imp_debug_audit(
    p_actor_user_id,
    'HR_CANCEL_APPLY_DEBUG',
    jsonb_build_object(
      'import_id', p_import_id::text,
      'key_type', 'HR_REQUEST_ID',
      'selected_count', jsonb_array_length(v_actions),
      'cancelled_count', v_cancelled_count,
      'file_request_count', v_file_request_count,
      'affected_timesheet_ids_count', coalesce(array_length(v_timesheet_ids, 1), 0),
      'affected_invoice_ids_count', coalesce(array_length(v_invoice_ids, 1), 0),
      'sample', v_sample
    ),
    'hr_imports',
    p_import_id::text,
    null,
    null,
    null,
    null
  );

  v_credit_note_ids := array[]::uuid[];
  v_pdf_jobs_enqueued := 0;

  return jsonb_build_object(
    'import_id', p_import_id,
    'cancelled_count', v_cancelled_count,
    'affected_timesheet_ids', to_jsonb(v_timesheet_ids),
    'affected_invoice_ids', to_jsonb(v_invoice_ids),
    'credit_note_ids_created', to_jsonb(v_credit_note_ids),
    'invoice_pdf_jobs_enqueued', v_pdf_jobs_enqueued
  );

exception when others then
  get stacked diagnostics v_sqlstate = returned_sqlstate, v_err = message_text;

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'HR_CANCEL_APPLY_ERROR',
      jsonb_build_object(
        'import_id', p_import_id::text,
        'key_type', 'HR_REQUEST_ID',
        'last_shift_id', case when v_last_shift_id is null then null else v_last_shift_id::text end,
        'selected_count', case when jsonb_typeof(v_actions) = 'array' then jsonb_array_length(v_actions) else null end,
        'cancelled_count', v_cancelled_count,
        'file_request_count', v_file_request_count,
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
-- CloudTMS deployment metadata preserved from the installed TEST definition.
ALTER FUNCTION public.weekly_import_apply_cancellations(uuid, jsonb, uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.weekly_import_apply_cancellations(uuid, jsonb, uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.weekly_import_apply_cancellations(uuid, jsonb, uuid) TO postgres, authenticated, service_role;
