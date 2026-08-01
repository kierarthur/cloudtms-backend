-- CloudTMS reviewed direct replacement; review artifact only, not installed.
-- Exact TEST baseline body MD5 prefix: 093bded6a119.
-- Ordinary and non-import-authoritative branches remain on the installed implementation.
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
  v_reconciliation_a_schedule jsonb := null;
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
    v_reconciliation_a_schedule := null;

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
        v_reconciliation_a_schedule:=coalesce(v_reconciliation_unit->'A_schedule_json','[]'::jsonb);
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

    -- A financial-position-only amendment can remain necessary after the
    -- source shift has already adopted the latest authoritative schedule. In
    -- that case the legacy changed-hours reader intentionally returns no row.
    -- Reconstruct the narrow Phase 3 carrier only from the already validated,
    -- transaction-local reconciliation unit: frozen B supplies the schedule
    -- to reverse and authoritative A supplies the replacement schedule.
    if v_row is null and v_reconciliation_unit is not null then
      if jsonb_typeof(v_reconciliation_b_schedule)<>'array'
         or jsonb_array_length(v_reconciliation_b_schedule)<>1
         or jsonb_typeof(v_reconciliation_a_schedule)<>'array'
         or jsonb_array_length(v_reconciliation_a_schedule)<>1 then
        raise exception 'IMPORT_REVIEW_RECONCILIATION_UNIT_INVALID' using errcode='22023';
      end if;

      select jsonb_build_object(
        'shift_id', ns.id,
        'candidate_id', ns.candidate_id,
        'client_id', ns.client_id,
        'contract_id', ns.contract_id,
        'timesheet_id', ns.timesheet_id,
        'work_date', coalesce(
          nullif(v_reconciliation_a_schedule#>>'{0,date}','')::date,
          ((v_reconciliation_a_schedule#>>'{0,start_utc}')::timestamptz at time zone 'Europe/London')::date
        ),
        'week_ending_date', coalesce(ts.week_ending_date,ns.week_ending_date),
        'old_start_utc', v_reconciliation_b_schedule#>>'{0,start_utc}',
        'old_end_utc', v_reconciliation_b_schedule#>>'{0,end_utc}',
        'old_break_mins', coalesce(v_reconciliation_b_schedule#>>'{0,break_mins}','0'),
        'new_start_utc', v_reconciliation_a_schedule#>>'{0,start_utc}',
        'new_end_utc', v_reconciliation_a_schedule#>>'{0,end_utc}',
        'new_break_mins', coalesce(v_reconciliation_a_schedule#>>'{0,break_mins}','0'),
        'old_paid_minutes', greatest(0,
          floor(extract(epoch from (
            (v_reconciliation_b_schedule#>>'{0,end_utc}')::timestamptz
            - (v_reconciliation_b_schedule#>>'{0,start_utc}')::timestamptz
          )) / 60.0)::integer
          - coalesce((v_reconciliation_b_schedule#>>'{0,break_mins}')::integer,0)
        ),
        'new_paid_minutes', greatest(0,
          floor(extract(epoch from (
            (v_reconciliation_a_schedule#>>'{0,end_utc}')::timestamptz
            - (v_reconciliation_a_schedule#>>'{0,start_utc}')::timestamptz
          )) / 60.0)::integer
          - coalesce((v_reconciliation_a_schedule#>>'{0,break_mins}')::integer,0)
        ),
        'is_invoiced', false,
        'invoice_id_detected', null
      )
      into v_row
      from public.nhsp_shifts ns
      left join public.timesheets ts
        on ts.timesheet_id=ns.timesheet_id
       and ts.is_current=true
      where ns.id=nullif(v_reconciliation_unit->>'source_shift_id','')::uuid
        and ns.external_row_key=v_key
        and ns.source_system='NHSP'::public.hr_source_enum
        and ns.cancelled_at_utc is null
        and exists (
          select 1
          from public.hr_rows current_row
          where current_row.import_id=p_import_id
            and current_row.external_row_key=v_key
        )
      limit 1;
    end if;

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
              'ref_num', nullif(btrim(coalesce(v_ref_num,'')), ''),
              'shift_id', v_shift_id::text,
              'external_row_key', v_key,
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
              update public.contract_weeks cw2
              set
                is_adjustment = true,
                status = 'SUBMITTED'::public.contract_week_status_enum,
                updated_at = v_now
              where cw2.id = v_existing_cw_id;
            end if;

             update public.timesheets t2
            set
              is_current = true,
              status = 'RECEIVED'::public.timesheet_status_enum,
              actual_schedule_json = v_schedule,
              qr_payload_json = v_hint,
              candidate_hint_text = v_hint,

              -- ✅ inherit policy identity from base timesheet
              sheet_scope = v_parent_sheet_scope,
              submission_mode = v_parent_submission_mode,
              parent_timesheet_id = v_base_timesheet_id,
              week_ending_date = v_week_ending_date,
              contract_id = v_contract_id,
              is_adjustment = true,
              correction_id = v_correction_id,
              correction_kind = v_kind,
              adjustment_origin = 'IMPORT_CORRECTION',

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
            is_current = true,
            status = 'RECEIVED'::public.timesheet_status_enum,
            actual_schedule_json = v_schedule,
            qr_payload_json = v_hint,
            candidate_hint_text = v_hint,

            -- ✅ inherit policy identity from base timesheet
            sheet_scope = v_parent_sheet_scope,
            submission_mode = v_parent_submission_mode,
            parent_timesheet_id = v_base_timesheet_id,
            week_ending_date = v_week_ending_date,
            contract_id = v_contract_id,
            is_adjustment = true,
            correction_id = v_correction_id,
            correction_kind = v_kind,
            adjustment_origin = 'IMPORT_CORRECTION',

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
-- CloudTMS deployment metadata preserved from the installed TEST definition.
ALTER FUNCTION public.nhsp_weekly_phase3_apply_adjustment_truth(uuid, text[], uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.nhsp_weekly_phase3_apply_adjustment_truth(uuid, text[], uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.nhsp_weekly_phase3_apply_adjustment_truth(uuid, text[], uuid) TO postgres, authenticated, service_role;
