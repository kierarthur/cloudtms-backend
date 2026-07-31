-- CloudTMS reviewed direct replacement; review artifact only, not installed.
-- Exact TEST baseline body MD5 prefix: 996444e153e5.
-- Ordinary and non-import-authoritative branches remain on the installed implementation.
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
  v_reviewed_correction_id text;
  v_repair_identity_mode text;
  v_repair_operation_id text;
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
  v_reconciliation_a_schedule jsonb := null;
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
    v_reconciliation_a_schedule := null;

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
        v_reconciliation_a_schedule:=coalesce(v_reconciliation_unit->'A_schedule_json','[]'::jsonb);
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
      if jsonb_typeof(v_reconciliation_a_schedule)<>'array' or jsonb_array_length(v_reconciliation_a_schedule)<>1 then
        raise exception 'IMPORT_REVIEW_MUTABLE_GENERATION_EVIDENCE_UNPROVABLE' using errcode='55000';
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
        v_reviewed_correction_id:=coalesce(v_reconciliation_unit->>'reviewed_existing_correction_id',v_reconciliation_unit->>'correction_id');
        v_correction_id:=v_reviewed_correction_id;
        if nullif(v_correction_id,'') is null then
          raise exception 'IMPORT_REVIEW_RECONCILIATION_UNIT_INVALID' using errcode='22023';
        end if;
        v_repair_identity_mode:=coalesce(v_reconciliation_unit->>'repair_identity_mode','RETAIN_EXISTING_CORRECTION_ID');
        if v_repair_identity_mode not in ('RETAIN_EXISTING_CORRECTION_ID','FRESH_CORRECTION_ID_ARCHIVED_ROLE_IGNORED') then
          raise exception 'IMPORT_REVIEW_RECONCILIATION_UNIT_INVALID' using errcode='22023';
        end if;
        perform 1 from public.timesheets repair_scope
        where repair_scope.correction_id=v_reviewed_correction_id
          and repair_scope.is_current and repair_scope.archived_at_utc is null
          and repair_scope.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
        order by repair_scope.timesheet_id for update;
        if v_repair_identity_mode='FRESH_CORRECTION_ID_ARCHIVED_ROLE_IGNORED' then
          v_repair_operation_id:=current_setting('cloudtms.import_reconciliation_operation_id',true);
          if coalesce(v_repair_operation_id,'')!~*'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
            raise exception 'IMPORT_REVIEW_RECONCILIATION_GUARD_REQUIRED' using errcode='55000';
          end if;
          v_correction_id:='chg:repair:'||encode(extensions.digest(convert_to(concat_ws('|','archived-role-repair-v1',
            v_repair_operation_id,v_key,v_reviewed_correction_id,v_reconciliation_unit->>'reconciliation_fingerprint',
            coalesce(v_reconciliation_unit->'archived_history_roles','[]'::jsonb)::text),'UTF8'),'sha256'),'hex');
          perform 1 from public.timesheets repair_lock
          where repair_lock.timesheet_id in (select x.value::uuid from jsonb_array_elements_text(coalesce(v_reconciliation_unit->'M_active_member_ids','[]'::jsonb)) x(value))
          order by repair_lock.timesheet_id for update;
          update public.timesheets repair_member
          set correction_id=v_correction_id,updated_at=v_now
          where repair_member.timesheet_id in (select x.value::uuid from jsonb_array_elements_text(coalesce(v_reconciliation_unit->'M_active_member_ids','[]'::jsonb)) x(value))
            and repair_member.correction_id=v_reviewed_correction_id and repair_member.is_current and repair_member.archived_at_utc is null;
          if exists(select 1 from public.timesheets remaining where remaining.correction_id=v_reviewed_correction_id
            and remaining.is_current and remaining.archived_at_utc is null) then
            raise exception 'IMPORT_REVIEW_MUTABLE_GENERATION_REPAIR_POSTCONDITION_FAILED' using errcode='55000';
          end if;
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
      perform 1 from public.timesheets parent_lock
      where parent_lock.timesheet_id=v_base_timesheet_id for update;
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
        if v_reconciliation_unit is not null then
          v_schedule:=case when v_kind='CHANGED_HOURS_REVERSAL'
            then v_reconciliation_b_schedule else v_reconciliation_a_schedule end;
        else
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
        end if;

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

          if v_reconciliation_unit is not null and (select count(*) from public.contract_weeks cw
            where cw.timesheet_id=v_existing_ts_id and cw.contract_id=v_contract_id
              and cw.week_ending_date=v_week_ending_date)>1 then
            raise exception 'IMPORT_REVIEW_MUTABLE_GENERATION_CONTRACT_WEEK_AMBIGUOUS' using errcode='55000';
          end if;

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
            qr_payload_json = v_hint,
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
  v_hint,
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
              qr_payload_json = v_hint,
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
      if (select count(*)=2
            and count(*) filter(where t.correction_kind='CHANGED_HOURS_REVERSAL')=1
            and count(*) filter(where t.correction_kind='CHANGED_HOURS_REPLACEMENT')=1
            and count(distinct t.parent_timesheet_id)=1
            and bool_and(t.parent_timesheet_id=v_base_timesheet_id)
            and bool_and(t.contract_id=v_contract_id and t.week_ending_date=v_week_ending_date)
            and bool_and(t.adjustment_origin='IMPORT_CORRECTION' and coalesce(t.is_adjustment,false))
            and bool_and(t.candidate_hint_text#>>'{import_authoritative_reconciliation,operation_id}'=
              current_setting('cloudtms.import_reconciliation_operation_id',true))
            and bool_and(t.candidate_hint_text#>>'{import_authoritative_reconciliation,unit_fingerprint}'=
              v_reconciliation_unit->>'unit_fingerprint')
            and bool_and(case when t.correction_kind='CHANGED_HOURS_REVERSAL'
              then t.actual_schedule_json is not distinct from v_reconciliation_b_schedule
              else t.actual_schedule_json is not distinct from v_reconciliation_a_schedule end)
          from public.timesheets t
          where t.correction_id=v_correction_id and t.is_current and t.archived_at_utc is null
            and t.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')) is not true
         or exists(select 1 from public.timesheets t
           where t.timesheet_id in (v_rev_ts_id,v_rep_ts_id)
             and (select count(*) from public.contract_weeks cw where cw.timesheet_id=t.timesheet_id
               and cw.contract_id=v_contract_id and cw.week_ending_date=v_week_ending_date)<>1)
         or (v_repair_identity_mode='FRESH_CORRECTION_ID_ARCHIVED_ROLE_IGNORED' and exists(
           select 1 from public.timesheets t where t.correction_id=v_reviewed_correction_id
             and t.is_current and t.archived_at_utc is null)) then
        raise exception 'IMPORT_REVIEW_MUTABLE_GENERATION_REPAIR_POSTCONDITION_FAILED' using errcode='55000';
      end if;
      with applied as (
        select jsonb_build_object(
          'correction_id',v_correction_id,
          'reversal_timesheet_id',v_rev_ts_id,
          'replacement_timesheet_id',v_rep_ts_id,
          'M_active_member_ids',jsonb_build_array(v_rev_ts_id,v_rep_ts_id),
          'applied_member_ids',jsonb_build_array(v_rev_ts_id,v_rep_ts_id),
          'parent_timesheet_id',v_base_timesheet_id,
          'repair_identity_mode',coalesce(v_repair_identity_mode,'CREATE_NEW_GENERATION'),
          'reviewed_unit_fingerprint',v_reconciliation_unit->>'unit_fingerprint',
          'reconciliation_fingerprint',v_reconciliation_unit->>'reconciliation_fingerprint'
        ) value
      )
      update pg_temp.import_review_reconciliation_units_v1 u
      set unit_json=u.unit_json||applied.value||jsonb_build_object(
        'applied_result_fingerprint',encode(extensions.digest(convert_to(applied.value::text,'UTF8'),'sha256'),'hex'))
      from applied where u.source_identity=v_key;
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
-- CloudTMS deployment metadata preserved from the installed TEST definition.
ALTER FUNCTION public.hr_weekly_phase3_apply_adjustment_truth(uuid, text[], uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.hr_weekly_phase3_apply_adjustment_truth(uuid, text[], uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.hr_weekly_phase3_apply_adjustment_truth(uuid, text[], uuid) TO postgres, authenticated, service_role;
