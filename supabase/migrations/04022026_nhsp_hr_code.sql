create or replace function public.hr_weekly_deterministic_rows(
  p_import_id uuid
)
returns table (
  external_row_key text,
  candidate_id uuid,
  client_id uuid,
  work_date date,
  start_utc timestamptz,
  end_utc timestamptz,
  contract_id uuid,
  week_ending_date date
)
language sql
security definer
set search_path = public
as $$
  with imp as (
    select hi.id
    from public.hr_imports hi
    where hi.id = p_import_id
      and hi.source_system = 'HEALTHROSTER'::public.hr_source_enum
    limit 1
  ),
  p2 as (
    select
      p2r.hr_row_id,
      p2r.candidate_id,
      p2r.client_id,
      p2r.contract_id,
      p2r.week_ending_date,
      p2r.action
    from public.weekly_import_phase2(
      p_import_id := p_import_id,
      p_system_type := 'HR_WEEKLY'
    ) as p2r
    where p2r.hr_row_id is not null
  ),
  base as (
    select
      nullif(btrim(r.external_row_key), '') as external_row_key,
      p2.candidate_id as candidate_id,
      p2.client_id as client_id,
      r.date_local as work_date,
      nullif(btrim(r.payload_json->>'start_utc'), '') as start_utc_txt,
      nullif(btrim(r.payload_json->>'end_utc'), '') as end_utc_txt,
      p2.contract_id as contract_id,
      p2.week_ending_date as week_ending_date,
      p2.action as p2_action
    from imp
    join public.hr_rows r
      on r.import_id = p_import_id
    join p2
      on p2.hr_row_id = r.id
  )
  select
    b.external_row_key,
    b.candidate_id,
    b.client_id,
    b.work_date,
    (b.start_utc_txt)::timestamptz as start_utc,
    (b.end_utc_txt)::timestamptz as end_utc,
    b.contract_id,
    b.week_ending_date
  from base b
  where b.external_row_key is not null
    and b.candidate_id is not null
    and b.client_id is not null
    and b.work_date is not null
    and b.start_utc_txt is not null
    and b.end_utc_txt is not null
    and coalesce(upper(b.p2_action::text), '') not like 'REJECT_%'
    and b.start_utc_txt ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2}(\.\d{1,6})?)?(Z|[+-]\d{2}:?\d{2})$'
    and b.end_utc_txt   ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2}(\.\d{1,6})?)?(Z|[+-]\d{2}:?\d{2})$'
$$;

create or replace function public.hr_weekly_mirror_upsert_deterministic(
  p_import_id uuid,
  p_external_row_keys text[],
  p_actor_user_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now timestamptz := now();
  v_src public.hr_source_enum;

  v_requested_keys text[] := '{}';
  v_det_keys text[] := '{}';
  v_collision_keys text[] := '{}';
  v_eligible_keys text[] := '{}';

  v_inserted_count int := 0;
  v_updated_count int := 0;
begin
  -- Validate import exists + is HEALTHROSTER weekly (mirror is only for HR weekly)
  select hi.source_system
  into v_src
  from public.hr_imports hi
  where hi.id = p_import_id
  limit 1;

  if v_src is null then
    raise exception 'hr_weekly_mirror_upsert_deterministic: import_not_found (import_id=%)', p_import_id;
  end if;

  if v_src <> 'HEALTHROSTER'::public.hr_source_enum then
    raise exception
      'hr_weekly_mirror_upsert_deterministic: source_system_mismatch (import_id=% actual=% expected=HEALTHROSTER)',
      p_import_id, v_src;
  end if;

  -- Normalise requested keys
  select coalesce(array_agg(distinct btrim(k)), '{}')
  into v_requested_keys
  from unnest(coalesce(p_external_row_keys, '{}'::text[])) as k
  where k is not null and btrim(k) <> '';

  if array_length(v_requested_keys, 1) is null then
    return jsonb_build_object(
      'import_id', p_import_id,
      'requested_count', 0,
      'deterministic_count', 0,
      'eligible_count', 0,
      'inserted_count', 0,
      'updated_count', 0,
      'excluded', '[]'::jsonb
    );
  end if;

  create temporary table tmp_req_keys(
    external_row_key text primary key
  ) on commit drop;

  insert into tmp_req_keys(external_row_key)
  select unnest(v_requested_keys);

  -- Deterministic rows (locked criteria is enforced by hr_weekly_deterministic_rows)
  create temporary table tmp_det on commit drop as
  select d.*
  from public.hr_weekly_deterministic_rows(p_import_id) as d
  join tmp_req_keys as r
    on r.external_row_key = d.external_row_key;

  select coalesce(array_agg(distinct d.external_row_key), '{}')
  into v_det_keys
  from tmp_det as d;

  -- Collisions: do not overwrite an existing nhsp_shifts row that belongs to a different source_system
  -- NOTE: cannot combine a column-definition temp table with "AS SELECT", so use CTAS here.
  create temporary table tmp_collisions on commit drop as
  select s.external_row_key
  from public.nhsp_shifts as s
  join tmp_det as d
    on d.external_row_key = s.external_row_key
  where s.source_system <> 'HEALTHROSTER'::public.hr_source_enum;

  select coalesce(array_agg(c.external_row_key), '{}')
  into v_collision_keys
  from tmp_collisions as c;

  -- Eligible = deterministic - collisions
  select coalesce(array_agg(x.external_row_key), '{}')
  into v_eligible_keys
  from (
    select distinct d.external_row_key
    from tmp_det as d
    left join tmp_collisions as c
      on c.external_row_key = d.external_row_key
    where c.external_row_key is null
  ) as x;

  -- Upsert mirror rows (timesheet_id must ALWAYS be NULL)
  if array_length(v_eligible_keys, 1) is not null then
    create temporary table tmp_upsert_src on commit drop as
    select
      d.external_row_key,
      p_import_id as latest_import_id,
      d.candidate_id,
      d.client_id,
      d.contract_id,
      null::uuid as timesheet_id,                          -- LOCKED: must remain NULL
      d.work_date,
      coalesce(r.unit_raw, nullif(btrim(r.payload_json->>'ward'), ''), null) as ward,
      d.start_utc,
      d.end_utc,
      greatest(
        0,
        case
          when (r.payload_json ? 'break_mins') and (r.payload_json->>'break_mins') ~ '^[0-9]+$'
            then (r.payload_json->>'break_mins')::int
          else 0
        end
      ) as break_mins,
      greatest(
        0,
        (floor(extract(epoch from (d.end_utc - d.start_utc)) / 60.0))::int
        -
        greatest(
          0,
          case
            when (r.payload_json ? 'break_mins') and (r.payload_json->>'break_mins') ~ '^[0-9]+$'
              then (r.payload_json->>'break_mins')::int
            else 0
          end
        )
      ) as pay_minutes,
      null::numeric as pay_amount_snapshot,
      null::numeric as charge_amount_snapshot,
      'PENDING'::text as invoice_status,
      null::timestamptz as defer_until_run_after,
      null::uuid as invoice_id,
      v_now as created_at,
      v_now as updated_at,
      'HEALTHROSTER'::public.hr_source_enum as source_system,
      r.hr_request_id,
      null::text as held_back_reason,
      r.staff_raw as staff_name,
      r.staff_norm as staff_norm,
      nullif(lower(coalesce(r.unit_raw, r.payload_json->>'ward', '')), '') as ward_norm,
      r.assignment_grade_norm as assignment_code,
      nullif(btrim(r.hr_request_id), '') as ref_num,
      d.week_ending_date,
      null::timestamptz as cancelled_at_utc,
      null::uuid as cancelled_by_import_id,
      null::text as cancelled_reason
    from tmp_det as d
    join public.hr_rows as r
      on r.import_id = p_import_id
     and r.external_row_key = d.external_row_key
    where d.external_row_key = any(v_eligible_keys);

    with up as (
      insert into public.nhsp_shifts(
        external_row_key,
        latest_import_id,
        candidate_id,
        client_id,
        contract_id,
        timesheet_id,
        work_date,
        ward,
        start_utc,
        end_utc,
        break_mins,
        pay_minutes,
        pay_amount_snapshot,
        charge_amount_snapshot,
        invoice_status,
        defer_until_run_after,
        invoice_id,
        created_at,
        updated_at,
        source_system,
        hr_request_id,
        held_back_reason,
        staff_name,
        staff_norm,
        ward_norm,
        assignment_code,
        ref_num,
        week_ending_date,
        cancelled_at_utc,
        cancelled_by_import_id,
        cancelled_reason
      )
      select
        s.external_row_key,
        s.latest_import_id,
        s.candidate_id,
        s.client_id,
        s.contract_id,
        s.timesheet_id,
        s.work_date,
        s.ward,
        s.start_utc,
        s.end_utc,
        s.break_mins,
        s.pay_minutes,
        s.pay_amount_snapshot,
        s.charge_amount_snapshot,
        s.invoice_status,
        s.defer_until_run_after,
        s.invoice_id,
        s.created_at,
        s.updated_at,
        s.source_system,
        s.hr_request_id,
        s.held_back_reason,
        s.staff_name,
        s.staff_norm,
        s.ward_norm,
        s.assignment_code,
        s.ref_num,
        s.week_ending_date,
        s.cancelled_at_utc,
        s.cancelled_by_import_id,
        s.cancelled_reason
      from tmp_upsert_src as s
      on conflict (external_row_key) do update
        set latest_import_id = excluded.latest_import_id,
            candidate_id = excluded.candidate_id,
            client_id = excluded.client_id,
            contract_id = excluded.contract_id,
            work_date = excluded.work_date,
            ward = excluded.ward,
            start_utc = excluded.start_utc,
            end_utc = excluded.end_utc,
            break_mins = excluded.break_mins,
            pay_minutes = excluded.pay_minutes,
            hr_request_id = excluded.hr_request_id,
            ref_num = excluded.ref_num,
            staff_name = excluded.staff_name,
            staff_norm = excluded.staff_norm,
            ward_norm = excluded.ward_norm,
            assignment_code = excluded.assignment_code,
            week_ending_date = excluded.week_ending_date,
            source_system = 'HEALTHROSTER'::public.hr_source_enum,
            timesheet_id = null,                            -- LOCKED: must remain NULL
            updated_at = excluded.updated_at
      returning (xmax = 0) as inserted_flag
    )
    select
      count(*) filter (where up.inserted_flag),
      count(*) filter (where not up.inserted_flag)
    into v_inserted_count, v_updated_count
    from up;
  end if;

  return jsonb_build_object(
    'import_id', p_import_id,
    'requested_count', coalesce(array_length(v_requested_keys, 1), 0),
    'deterministic_count', coalesce(array_length(v_det_keys, 1), 0),
    'eligible_count', coalesce(array_length(v_eligible_keys, 1), 0),
    'inserted_count', v_inserted_count,
    'updated_count', v_updated_count,
    'excluded', (
      select coalesce(
        jsonb_agg(jsonb_build_object('external_row_key', e.external_row_key, 'reason', e.reason) order by e.external_row_key),
        '[]'::jsonb
      )
      from (
        -- Requested but not deterministic (ambiguous / excluded)
        select rk.external_row_key, 'NOT_DETERMINISTIC'::text as reason
        from tmp_req_keys as rk
        left join (select distinct d.external_row_key from tmp_det as d) as dk
          on dk.external_row_key = rk.external_row_key
        where dk.external_row_key is null

        union all

        -- Deterministic but collision with existing non-HEALTHROSTER shift
        select c.external_row_key, 'EXISTS_NON_HEALTHROSTER'::text as reason
        from tmp_collisions as c
      ) as e
    )
  );
end;
$$;


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
        or (v_existing_pos_seg_invoice_id is not null);

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
        or (v_existing_neg_seg_invoice_id is not null);
    end if;

    -- ✅ EDGE CASE: delete redundant NEG+POS when truth returns to base exactly
    v_deleted_redundant_pair := false;
    if v_existing_pos_ts_id is not null
       and v_existing_neg_ts_id is not null
       and v_existing_pos_is_invoiced is false
       and v_existing_neg_is_invoiced is false
       and coalesce(v_existing_pos_count,0) = 1
       and coalesce(v_existing_neg_count,0) = 1
    then
      if v_existing_neg_schedule is not null and jsonb_typeof(v_existing_neg_schedule) = 'array' then
        begin
          v_existing_neg_base_start_utc := nullif(btrim(coalesce((v_existing_neg_schedule->0)->>'start_utc','')), '')::timestamptz;
        exception when others then
          v_existing_neg_base_start_utc := null;
        end;
        begin
          v_existing_neg_base_end_utc := nullif(btrim(coalesce((v_existing_neg_schedule->0)->>'end_utc','')), '')::timestamptz;
        exception when others then
          v_existing_neg_base_end_utc := null;
        end;
        begin
          v_existing_neg_base_break_mins := coalesce(nullif(btrim(coalesce((v_existing_neg_schedule->0)->>'break_mins','')), '')::int, 0);
        exception when others then
          v_existing_neg_base_break_mins := 0;
        end;
      end if;

      if v_existing_neg_base_start_utc is not null
         and v_existing_neg_base_end_utc is not null
         and v_existing_neg_base_start_utc = v_new_start_utc
         and v_existing_neg_base_end_utc = v_new_end_utc
         and coalesce(v_existing_neg_base_break_mins,0) = greatest(0, coalesce(v_new_break_mins,0))
      then
        -- lock both before delete
        perform 1
        from public.timesheets tdel_lock
        where tdel_lock.timesheet_id = any(array[v_existing_neg_ts_id, v_existing_pos_ts_id]::uuid[])
        for update;

        -- delete dependent rows (tables exist in your schema)
        delete from public.ts_financials_outbox ob_del
        where ob_del.timesheet_id = any(array[v_existing_neg_ts_id, v_existing_pos_ts_id]::uuid[]);

        delete from public.ts_pdfs_outbox pdf_del
        where pdf_del.timesheet_id = any(array[v_existing_neg_ts_id, v_existing_pos_ts_id]::uuid[]);

        delete from public.timesheets_financials tf_del
        where tf_del.timesheet_id = any(array[v_existing_neg_ts_id, v_existing_pos_ts_id]::uuid[]);

        delete from public.invoice_lines il_del
        where il_del.timesheet_id = any(array[v_existing_neg_ts_id, v_existing_pos_ts_id]::uuid[]);

        delete from public.pay_batch_items pbi_del
        where pbi_del.timesheet_id = any(array[v_existing_neg_ts_id, v_existing_pos_ts_id]::uuid[]);

        delete from public.pay_item_snoozes pis_del
        where pis_del.timesheet_id = any(array[v_existing_neg_ts_id, v_existing_pos_ts_id]::uuid[]);

        delete from public.ts_pay_adjustments tpa_del
        where tpa_del.timesheet_id = any(array[v_existing_neg_ts_id, v_existing_pos_ts_id]::uuid[]);

        delete from public.timesheet_pay_state_history tph_del
        where tph_del.timesheet_id = any(array[v_existing_neg_ts_id, v_existing_pos_ts_id]::uuid[]);

        delete from public.timesheet_pay_state tps_del
        where tps_del.timesheet_id = any(array[v_existing_neg_ts_id, v_existing_pos_ts_id]::uuid[]);

        delete from public.timesheet_evidence te_del
        where te_del.timesheet_id = any(array[v_existing_neg_ts_id, v_existing_pos_ts_id]::uuid[]);

        delete from public.timesheet_validations tv_del
        where tv_del.timesheet_id = any(array[v_existing_neg_ts_id, v_existing_pos_ts_id]::uuid[]);

        delete from public.manual_timesheet_queue mtq_del
        where mtq_del.timesheet_id = any(array[v_existing_neg_ts_id, v_existing_pos_ts_id]::uuid[]);

        delete from public.hr_results hrr_del
        where hrr_del.timesheet_id = any(array[v_existing_neg_ts_id, v_existing_pos_ts_id]::uuid[]);

        delete from public.hr_issue_emails hie_del
        where hie_del.timesheet_id = any(array[v_existing_neg_ts_id, v_existing_pos_ts_id]::uuid[]);

        delete from public.contract_weeks cw_del
        where cw_del.timesheet_id = any(array[v_existing_neg_ts_id, v_existing_pos_ts_id]::uuid[]);

        delete from public.timesheets t_del
        where t_del.timesheet_id = any(array[v_existing_neg_ts_id, v_existing_pos_ts_id]::uuid[]);

        v_deleted_redundant_pair := true;

        v_key_ts := '[]'::jsonb;
        v_key_ts := v_key_ts || jsonb_build_array(jsonb_build_object(
          'kind', 'REDUNDANT_PAIR',
          'op', 'DELETED_NEG_AND_POS',
          'neg_timesheet_id', v_existing_neg_ts_id::text,
          'pos_timesheet_id', v_existing_pos_ts_id::text
        ));

        v_upd_count := v_upd_count + 2;

        -- no further work for this key
        continue;
      end if;
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

      perform 1
      from public.timesheets tlock
      where tlock.timesheet_id = v_existing_pos_ts_id
      for update;

         update public.timesheets tup
      set
        actual_schedule_json = v_schedule,
        qr_payload_json = v_hint,
        candidate_hint_text = v_hint,
        parent_timesheet_id = v_base_timesheet_id,

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

  -- email (MODE_A; transactional log, send post-commit)
  v_email_jobs jsonb := '[]'::jsonb;

  -- affected timesheets for TSFIN drain
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
    -- NOTE: This section is unchanged from your supplied function; it remains as-is.

    -- 4.1) Partition invoiced vs non-invoiced changed-hours keys (selected MODE_B keys only)
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

    -- 4.2) Policy A replacement-day enforcement (MODE_B only)
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

    -- 4.3) Phase3 (invoiced changed keys)
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

    -- 4.4) Phase1 + Phase1.5
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

    -- 4.5) Cancellations
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
    -- ✅ 4.6) ENSURE BASE WEEKLY TIMESHEET EXISTS + ATTACH ACTIVE HEALTHROSTER SHIFTS
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
      p2m.contract_id,
      p2m.candidate_id,
      p2m.client_id,
      p2m.week_ending_date
    from tmp_p2_ok_mode p2m
    where p2m.mode = 'MODE_B'
      and p2m.external_row_key = any(coalesce(v_force_keys_final, array[]::text[]));

    if array_length(v_selected_cancel_shift_ids, 1) is not null then
      insert into tmp_ensure_pairs(contract_id, candidate_id, client_id, week_ending_date)
      select distinct
        ns.contract_id,
        ns.candidate_id,
        ns.client_id,
        coalesce(
          ns.week_ending_date,
          (
            ns.work_date
            + (
              (
                (coalesce(ct.week_ending_weekday_snapshot, 0) - extract(dow from ns.work_date)::int + 7) % 7
              )
            )::int
          )::date
        ) as week_ending_date
      from public.nhsp_shifts ns
      join public.contracts ct
        on ct.id = ns.contract_id
      where ns.id = any(coalesce(v_selected_cancel_shift_ids, array[]::uuid[]))
        and ns.contract_id is not null
        and ns.client_id is not null
        and ns.candidate_id is not null
        and (ns.week_ending_date is not null or ns.work_date is not null);
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

    -- ids of newly created base timesheets (for debug sampling)
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
      where ns_active.source_system = 'HEALTHROSTER'::public.hr_source_enum
        and ns_active.cancelled_at_utc is null
        and ns_active.contract_id = v_pair_contract_id
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
          status = 'SUBMITTED'::public.contract_week_status_enum,
          submission_mode_snapshot = 'MANUAL'::public.submission_mode_enum,
          updated_at = v_now
        where cw0link.id = v_base_week_id;

        -- ✅ NEW: user-facing audit line for "birth of base weekly timesheet"
        perform public._audit_insert(
          'timesheets',
          v_new_ts_id::text,
          'HR_IMPORT_TIMESHEET_CREATED',
          null,
          jsonb_build_object(
            'import_id', p_import_id::text,
            'client_id', v_import_client_id::text,
            'source_system', 'HEALTHROSTER',
            'kind', 'BASE_WEEKLY',
            'contract_id', v_pair_contract_id::text,
            'contract_week_id', v_base_week_id::text,
            'candidate_id', v_pair_candidate_id::text,
            'week_ending_date', v_pair_week_ending_date::text,
            'booking_id', v_booking_id
          ),
          'IMPORT_BIRTH',
          p_actor_user_id
        );

      else
        v_ensure_timesheet_reused_count := v_ensure_timesheet_reused_count + 1;

        update public.contract_weeks cw0keep
        set
          status = 'SUBMITTED'::public.contract_week_status_enum,
          submission_mode_snapshot = 'MANUAL'::public.submission_mode_enum,
          updated_at = v_now
        where cw0keep.id = v_base_week_id;

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

      update public.nhsp_shifts nsu0
      set
        timesheet_id = v_base_week_ts_id,
        updated_at = v_now
      where nsu0.source_system = 'HEALTHROSTER'::public.hr_source_enum
        and nsu0.cancelled_at_utc is null
        and nsu0.contract_id = v_pair_contract_id
        and nsu0.week_ending_date = v_pair_week_ending_date
        and nsu0.timesheet_id is null;

      get diagnostics v_attached_null_count = row_count;
      v_ensure_shifts_attached_count := v_ensure_shifts_attached_count + coalesce(v_attached_null_count, 0);

      update public.nhsp_shifts nsu1
      set
        timesheet_id = v_base_week_ts_id,
        updated_at = v_now
      where nsu1.source_system = 'HEALTHROSTER'::public.hr_source_enum
        and nsu1.cancelled_at_utc is null
        and nsu1.contract_id = v_pair_contract_id
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
      where nscheck.source_system = 'HEALTHROSTER'::public.hr_source_enum
        and nscheck.cancelled_at_utc is null
        and nscheck.contract_id = v_pair_contract_id
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
          'hr_weekly_apply_transactional: ENSURE invariant failed (active HEALTHROSTER shifts remain detached or linked to missing timesheets) contract_id=% week_ending_date=% remaining=%.',
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

    -- ✅ FIX: mismatched parentheses (was ')));')
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
      'sample_created_ts_ids', v_ensure_sample_created_ts_ids
    ));
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
  -- 10) MODE_A weekly validation upserts + email state (unchanged behaviour)
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
    (lower(coalesce(r.value->>'emailed_already','false')) in ('true','1')) as emailed_already
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
    gm.mode
  from tmp_val_rows vr
  join tmp_group_mode gm
    on gm.contract_id = vr.contract_id
   and gm.candidate_id = vr.candidate_id
   and gm.week_ending_date = vr.week_ending_date
   and gm.client_id = vr.client_id;

  insert into public.timesheet_validations(
    timesheet_id,
    status,
    reason_code,
    validated_at_utc,
    last_source,
    updated_at
  )
  select
    vr.timesheet_id,
    case
      when vr.overall_status = 'OK' then 'VALIDATION_OK'::public.validation_status_enum
      else 'VALIDATION_ERROR'::public.validation_status_enum
    end,
    'HEALTHROSTER_WEEKLY',
    case when vr.overall_status = 'OK' then v_now else null end,
    p_import_id,
    v_now
  from tmp_val_rows vr
  join tmp_val_mode vm
    on vm.timesheet_id = vr.timesheet_id
  where vm.mode = 'MODE_A'
  on conflict (timesheet_id) do update
    set status = excluded.status,
        reason_code = excluded.reason_code,
        validated_at_utc = excluded.validated_at_utc,
        last_source = excluded.last_source,
        updated_at = excluded.updated_at;

  get diagnostics v_validations_upserted = row_count;

  select coalesce(array_agg(distinct vr.timesheet_id), array[]::uuid[])
  into v_mismatched_tsids
  from tmp_val_rows vr
  join tmp_val_mode vm
    on vm.timesheet_id = vr.timesheet_id
  where vm.mode = 'MODE_A'
    and vr.has_mismatch is true
    and vr.timesheet_id is not null;

  create temporary table tmp_email_actions on commit drop as
  select
    nullif(btrim(a.value->>'timesheet_id'), '')::uuid as timesheet_id,
    nullif(btrim(a.value->>'issue_fingerprint'), '') as issue_fingerprint
  from jsonb_array_elements(v_email_actions) as a(value)
  where nullif(btrim(a.value->>'timesheet_id'), '') is not null
    and nullif(btrim(a.value->>'issue_fingerprint'), '') is not null;

  create temporary table tmp_email_join on commit drop as
  select
    ea.timesheet_id,
    ea.issue_fingerprint,
    vr.client_id,
    vr.recipient_email,
    vr.emailed_already
  from tmp_email_actions ea
  join tmp_val_rows vr
    on vr.timesheet_id = ea.timesheet_id
   and vr.issue_fingerprint = ea.issue_fingerprint
  join tmp_val_mode vm
    on vm.timesheet_id = vr.timesheet_id
  where vm.mode = 'MODE_A';

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

  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'timesheet_id', ej.timesheet_id::text,
        'issue_fingerprint', ej.issue_fingerprint,
        'recipient_email', ej.recipient_email,
        'recipient_missing', (ej.recipient_email is null or length(btrim(ej.recipient_email)) = 0),
        'email_kind', case when ej.emailed_already then 'REEMAIL' else 'EMAIL' end
      )
      order by ej.timesheet_id::text
    ),
    '[]'::jsonb
  )
  into v_email_jobs
  from tmp_email_join ej;

  v_email_jobs_count := jsonb_array_length(coalesce(v_email_jobs, '[]'::jsonb));

  v_steps := v_steps || jsonb_build_array(
    jsonb_build_object(
      'step','MODE_A_VALIDATIONS_DONE',
      'val_rows_count', v_val_rows_count,
      'validations_upserted', v_validations_upserted,
      'mismatched_timesheet_ids_count', coalesce(array_length(v_mismatched_tsids, 1), 0),
      'email_actions_count', v_email_actions_count,
      'email_jobs_count', v_email_jobs_count
    )
  );

  -- ─────────────────────────────────────────────
  -- 11) Compute affected_timesheet_ids (MODE_B only)
  -- ─────────────────────────────────────────────
  if (v_mode_b_should_run_phase1 is false) and (v_mode_b_should_run_cancellations is false) then
    v_affected_timesheet_ids := array[]::uuid[];
    v_steps := v_steps || jsonb_build_array(jsonb_build_object('step','AFFECTED_TS_SKIPPED_MODE_B_NOOP'));
  else
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
    where ns.source_system = 'HEALTHROSTER'::public.hr_source_enum
      and ns.client_id = v_import_client_id
      and ns.cancelled_at_utc is null
      and ns.external_row_key = any(coalesce(v_force_keys_non_invoiced, array[]::text[]))
      and ns.timesheet_id is not null
    on conflict do nothing;

    select coalesce(array_agg(distinct a.timesheet_id order by a.timesheet_id), array[]::uuid[])
    into v_affected_timesheet_ids
    from tmp_aff_ts a
    where a.timesheet_id is not null;

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

    v_steps := v_steps || jsonb_build_array(
      jsonb_build_object(
        'step','AFFECTED_TS_DONE',
        'affected_timesheet_ids_count', coalesce(array_length(v_affected_timesheet_ids, 1), 0),
        'cancel_adjustment_count', v_cancel_adjustment_count,
        'correction_timesheets_created_count', v_correction_timesheets_created_count
      )
    );
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
      'mismatched_timesheet_ids_count', coalesce(array_length(v_mismatched_tsids, 1), 0),
      'email_actions_count', v_email_actions_count,
      'email_jobs_count', v_email_jobs_count
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
      'mismatched_timesheet_ids', to_jsonb(coalesce(v_mismatched_tsids, array[]::uuid[]))
    ),
    'email_jobs', v_email_jobs,
    'affected_timesheet_ids', to_jsonb(coalesce(v_affected_timesheet_ids, array[]::uuid[]))
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



create or replace function public.tsfin_outbox_pending_summary(
  p_timesheet_ids uuid[]
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now timestamptz := now();
  v_total int := 0;
  v_ready int := 0;
  v_next_attempt_at_min timestamptz := null;
begin
  if p_timesheet_ids is null or array_length(p_timesheet_ids, 1) is null then
    return jsonb_build_object(
      'total', 0,
      'ready', 0,
      'next_attempt_at_min', null,
      'now', v_now
    );
  end if;

  with wanted as (
    select distinct unnest(p_timesheet_ids) as timesheet_id
  ),
  o as (
    select o.*
    from public.ts_financials_outbox o
    join wanted w on w.timesheet_id = o.timesheet_id
  )
  select
    count(*)::int as total,
    coalesce(sum(case when (o.next_attempt_at is null or o.next_attempt_at <= v_now) then 1 else 0 end), 0)::int as ready,
    min(o.next_attempt_at) filter (where o.next_attempt_at is not null and o.next_attempt_at > v_now) as next_attempt_at_min
  into
    v_total,
    v_ready,
    v_next_attempt_at_min
  from o;

  return jsonb_build_object(
    'total', coalesce(v_total, 0),
    'ready', coalesce(v_ready, 0),
    'next_attempt_at_min', v_next_attempt_at_min,
    'now', v_now
  );
end;
$$;





create or replace function public.nhsp_weekly_apply_cancellations(
  p_import_id uuid,
  p_actions jsonb,
  p_actor_user_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
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
  v_shift_ref_num text;
  v_shift_ref_norm text;

  v_shift_start_utc timestamptz;
  v_shift_end_utc timestamptz;
  v_shift_break_mins int;
  v_shift_pay_minutes int;
  v_shift_ward text;
  v_shift_week_ending_date date;

  -- ✅ evidence pointer: shift's current latest_import_id (may be overwritten by anchor evidence)
  v_shift_latest_import_id uuid;

  -- import scope guard (NHSP may span multiple clients)
  v_import_client_in_scope boolean;

  -- file ref set
  v_file_ref_count int := 0;
  v_present_in_file boolean := false;

  -- invoiced-at-all detection (segment-level)
  v_tf_locked_by_invoice_id uuid;
  v_tf_invoice_breakdown_json jsonb;

  v_seg_json jsonb := null;
  v_seg_invoice_id uuid;
  v_seg_pay_amount numeric := null;
  v_seg_charge_amount numeric := null;

  v_invoiced_detected boolean := false;
  v_invoice_id_detected uuid := null;

  -- invoice number (best-effort; MUST NOT break apply)
  v_invoice_number_text text := null;

  -- cancellation correction timesheet (for invoiced-at-all)
  v_branch text := null;

  v_contract_display_site text;
  v_contract_ward_hint text;
  v_contract_role text;
  v_contract_week_ending_weekday_snapshot int := 0;

  v_candidate_display_name text;
  v_candidate_tms_ref text;
  v_client_name text;

  v_week_ending_date date;
  v_base_ts_week_ending date;

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
  v_cw_id uuid;
  v_next_additional_seq int;
  v_try int;

  v_existing_ts_id uuid;
  v_existing_cw_id uuid;

  v_correction_ts_id uuid;
  v_ts_id uuid;

  -- fnv1a32 helper vars (needed for deterministic correction_id)
  v_fnv_h bigint;
  v_fnv_i int;
  v_fnv_s text;
  v_fnv_hex text;

  -- user-facing audit helpers (minutes + amounts)
  v_old_paid_minutes int := 0;
  v_new_paid_minutes int := 0;
  v_delta_paid_minutes int := 0;

  v_old_pay_amount_ex_vat numeric := null;
  v_old_charge_amount_ex_vat numeric := null;

  v_reversal_pay_amount_ex_vat numeric := null;
  v_reversal_charge_amount_ex_vat numeric := null;

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

  -- ✅ Cleanup: remove uninvoiced CHANGED_HOURS corrections when cancelling
  v_cleanup_ts_ids uuid[] := array[]::uuid[];
  v_cleanup_count int := 0;

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
begin
  -- Validate import exists and is NHSP
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
    raise exception 'nhsp_weekly_apply_cancellations: import % not found in hr_imports.', p_import_id;
  end if;

  if v_import_source_system <> 'NHSP' then
    raise exception 'nhsp_weekly_apply_cancellations: import % source_system=%; expected NHSP.', p_import_id, v_import_source_system;
  end if;

  -- Validate actions payload
  if jsonb_typeof(v_actions) <> 'array' then
    raise exception 'nhsp_weekly_apply_cancellations: p_actions must be a JSON array.';
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

  -- ─────────────────────────────────────────────
  -- 1) Import scope guard (NHSP): client_id must be deterministically present in file
  --     ("deterministically present" = phase2 has candidate_id + client_id + work_date)
  -- ─────────────────────────────────────────────
  create temporary table tmp_import_client_scope(
    client_id uuid primary key
  ) on commit drop;

  insert into tmp_import_client_scope(client_id)
  select distinct p2.client_id
  from public.weekly_import_phase2(
    p_import_id := p_import_id,
    p_system_type := 'NHSP'
  ) as p2
  where p2.client_id is not null
    and p2.candidate_id is not null
    and p2.work_date is not null
  on conflict (client_id) do nothing;

  -- ─────────────────────────────────────────────
  -- 2) Build file ref set (identity key = booking ref)
  --     Spec: coalesce(payload_json->>'ref_num', payload_json->>'Reference', hr_request_id)
  -- ─────────────────────────────────────────────
  create temporary table tmp_file_ref_set(
    ref_norm text primary key,
    ref_raw  text
  ) on commit drop;

  insert into tmp_file_ref_set(ref_norm, ref_raw)
  select
    lower(regexp_replace(btrim(src.ref_raw), '\s+', ' ', 'g')) as ref_norm,
    src.ref_raw as ref_raw
  from (
    select distinct
      nullif(
        btrim(
          coalesce(
            nullif(r.payload_json->>'ref_num',''),
            nullif(r.payload_json->>'Reference',''),
            nullif(r.hr_request_id,'')
          )
        ),
        ''
      ) as ref_raw
    from public.hr_rows r
    where r.import_id = p_import_id
  ) as src
  where src.ref_raw is not null
  on conflict (ref_norm) do nothing;

  select count(*)::int
  into v_file_ref_count
  from tmp_file_ref_set;

  -- ─────────────────────────────────────────────
  -- 3) Apply is transactional: any invalid selected row fails the whole apply.
  -- ─────────────────────────────────────────────
  for v_idx, v_item in
    select (e.ord)::int, e.value
    from jsonb_array_elements(v_actions) with ordinality as e(value, ord)
  loop
    v_shift_id_text := nullif(btrim(coalesce(v_item->>'shift_id', '')), '');
    v_reason := nullif(btrim(coalesce(v_item->>'reason', '')), '');

    if v_shift_id_text is null then
      raise exception 'nhsp_weekly_apply_cancellations: item % missing shift_id.', v_idx;
    end if;

    begin
      v_shift_id := v_shift_id_text::uuid;
    exception when others then
      raise exception 'nhsp_weekly_apply_cancellations: item % invalid shift_id "%".', v_idx, v_shift_id_text;
    end;

    v_last_shift_id := v_shift_id;

    if v_reason is null then
      v_reason := 'MISSING_FROM_IMPORT';
    end if;

    -- reset per-item helpers
    v_tf_locked_by_invoice_id := null;
    v_tf_invoice_breakdown_json := null;
    v_seg_json := null;
    v_seg_invoice_id := null;
    v_seg_pay_amount := null;
    v_seg_charge_amount := null;

    v_invoice_id_detected := null;
    v_invoiced_detected := false;
    v_invoice_number_text := null;

    v_old_paid_minutes := 0;
    v_new_paid_minutes := 0;
    v_delta_paid_minutes := 0;
    v_old_pay_amount_ex_vat := null;
    v_old_charge_amount_ex_vat := null;
    v_reversal_pay_amount_ex_vat := null;
    v_reversal_charge_amount_ex_vat := null;

    v_shift_latest_import_id := null;

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
      ns.ref_num,
      ns.start_utc,
      ns.end_utc,
      ns.break_mins,
      ns.pay_minutes,
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
      v_shift_ref_num,
      v_shift_start_utc,
      v_shift_end_utc,
      v_shift_break_mins,
      v_shift_pay_minutes,
      v_shift_ward,
      v_shift_week_ending_date,
      v_shift_latest_import_id
    from public.nhsp_shifts ns
    where ns.id = v_shift_id
    for update;

    if not found then
      raise exception 'nhsp_weekly_apply_cancellations: item % shift % not found in nhsp_shifts.', v_idx, v_shift_id;
    end if;

    if v_shift_cancelled_at is not null then
      raise exception 'nhsp_weekly_apply_cancellations: item % shift % is already cancelled (cancelled_at_utc not null).', v_idx, v_shift_id;
    end if;

    -- Locked guard: cancellations RPC only operates on NHSP shifts
    if v_shift_source_system <> 'NHSP' then
      raise exception 'nhsp_weekly_apply_cancellations: item % shift % source_system=%; expected NHSP.',
        v_idx, v_shift_id, v_shift_source_system;
    end if;

    if v_shift_contract_id is null then
      raise exception 'nhsp_weekly_apply_cancellations: item % shift % missing contract_id.', v_idx, v_shift_id;
    end if;

    if v_shift_candidate_id is null then
      raise exception 'nhsp_weekly_apply_cancellations: item % shift % missing candidate_id.', v_idx, v_shift_id;
    end if;

    if v_shift_work_date is null then
      raise exception 'nhsp_weekly_apply_cancellations: item % shift % missing work_date.', v_idx, v_shift_id;
    end if;

    if v_shift_client_id is null then
      raise exception 'nhsp_weekly_apply_cancellations: item % shift % missing client_id.', v_idx, v_shift_id;
    end if;

    -- Import scope safety: shift.client_id must be present deterministically in file.
    select exists (
      select 1
      from tmp_import_client_scope sc
      where sc.client_id = v_shift_client_id
    )
    into v_import_client_in_scope;

    if not v_import_client_in_scope then
      raise exception
        'nhsp_weekly_apply_cancellations: item % shift % client_id=% is out-of-scope for this import (client not deterministically present in file).',
        v_idx, v_shift_id, v_shift_client_id;
    end if;

    -- Guard A: require non-empty nhsp_shifts.ref_num (NHSP booking ref)
    if nullif(btrim(coalesce(v_shift_ref_num,'')), '') is null then
      raise exception
        'nhsp_weekly_apply_cancellations: item % shift % has empty ref_num; cannot use ref-based cancellation identity.',
        v_idx, v_shift_id;
    end if;

    v_shift_ref_norm := lower(regexp_replace(btrim(v_shift_ref_num), '\s+', ' ', 'g'));

    -- Presence test: if the ref is present in the file, cancellation is not eligible
    select exists (
      select 1
      from tmp_file_ref_set fr
      where fr.ref_norm = v_shift_ref_norm
    )
    into v_present_in_file;

    if v_present_in_file then
      raise exception
        'nhsp_weekly_apply_cancellations: item % shift % ref_num is present in the import file; cancellation rejected (not missing).',
        v_idx, v_shift_id;
    end if;

    -- ─────────────────────────────────────────────
    -- Invoiced-at-all detection (segment-level authoritative, base timesheet)
    -- ─────────────────────────────────────────────
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

        begin
          v_seg_pay_amount := nullif(btrim(coalesce(v_seg_json->>'pay_amount','')), '')::numeric;
        exception when others then
          v_seg_pay_amount := null;
        end;

        begin
          v_seg_charge_amount := nullif(btrim(coalesce(v_seg_json->>'charge_amount','')), '')::numeric;
        exception when others then
          v_seg_charge_amount := null;
        end;
      end if;
    end if;

    v_invoice_id_detected := coalesce(v_seg_invoice_id, v_tf_locked_by_invoice_id, v_shift_invoice_id);
    v_invoiced_detected := (v_invoice_id_detected is not null);

    if v_invoice_id_detected is not null then
      v_invoice_ids := array_append(v_invoice_ids, v_invoice_id_detected);

      -- Best-effort invoice number lookup; MUST NOT break apply
      begin
        select nullif(btrim(coalesce(i.invoice_no::text,'')), '')
        into v_invoice_number_text
        from public.invoices i
        where i.id = v_invoice_id_detected
        limit 1;
      exception when undefined_table then
        v_invoice_number_text := null;
      when undefined_column then
        begin
          select nullif(btrim(coalesce(i.invoice_number::text,'')), '')
          into v_invoice_number_text
          from public.invoices i
          where i.id = v_invoice_id_detected
          limit 1;
        exception when undefined_table then
          v_invoice_number_text := null;
        when undefined_column then
          begin
            select nullif(btrim(coalesce(i.number::text,'')), '')
            into v_invoice_number_text
            from public.invoices i
            where i.id = v_invoice_id_detected
            limit 1;
          exception when others then
            v_invoice_number_text := null;
          end;
        when others then
          v_invoice_number_text := null;
        end;
      when others then
        v_invoice_number_text := null;
      end;
    end if;

    -- Default audit minute/amount helpers (may be overridden for CORRECTION anchor)
    if v_shift_pay_minutes is not null then
      v_old_paid_minutes := greatest(0, v_shift_pay_minutes);
    elsif v_shift_start_utc is not null and v_shift_end_utc is not null then
      v_old_paid_minutes := greatest(
        0,
        (extract(epoch from (v_shift_end_utc - v_shift_start_utc)) / 60)::int - greatest(0, coalesce(v_shift_break_mins, 0))
      );
    else
      v_old_paid_minutes := 0;
    end if;

    v_new_paid_minutes := 0;
    v_delta_paid_minutes := (0 - v_old_paid_minutes);

    v_old_pay_amount_ex_vat := v_seg_pay_amount;
    v_old_charge_amount_ex_vat := v_seg_charge_amount;

    if v_old_pay_amount_ex_vat is not null then
      v_reversal_pay_amount_ex_vat := (0 - v_old_pay_amount_ex_vat);
    end if;

    if v_old_charge_amount_ex_vat is not null then
      v_reversal_charge_amount_ex_vat := (0 - v_old_charge_amount_ex_vat);
    end if;

    -- ─────────────────────────────────────────────
    -- Branch: INPLACE vs CORRECTION
    -- ─────────────────────────────────────────────
    if v_invoiced_detected = false then
      v_branch := 'INPLACE';

      -- Cancel + detach (no deletions)
      update public.nhsp_shifts ns2
      set
        cancelled_at_utc = v_now,
        cancelled_by_import_id = p_import_id,
        cancelled_reason = v_reason,
        timesheet_id = null
      where ns2.id = v_shift_id;

      v_cancelled_count := v_cancelled_count + 1;

      -- TSFIN recompute is mandatory for any cancel/detach/change (in-place branch only).
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

      -- ✅ Cleanup: if any uninvoiced CHANGED_HOURS corrections exist for this shift, delete them (safe only if not invoiced)
      begin
        create temporary table tmp_cleanup_candidates(timesheet_id uuid primary key) on commit drop;

        insert into tmp_cleanup_candidates(timesheet_id)
        select distinct t.timesheet_id
        from public.timesheets t
        where t.is_adjustment is true
          and t.is_current is true
          and t.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
          and t.contract_id = v_shift_contract_id
          and t.week_ending_date = coalesce(v_shift_week_ending_date, v_shift_work_date)
          and jsonb_typeof(t.actual_schedule_json) = 'array'
          and (
            t.actual_schedule_json @> jsonb_build_array(jsonb_build_object('shift_id', v_shift_id::text))
            or (
              v_shift_external_row_key is not null
              and t.actual_schedule_json @> jsonb_build_array(jsonb_build_object('external_row_key', v_shift_external_row_key))
            )
          )
        on conflict do nothing;

        create temporary table tmp_cleanup_delete(timesheet_id uuid primary key) on commit drop;

        insert into tmp_cleanup_delete(timesheet_id)
        select c.timesheet_id
        from tmp_cleanup_candidates c
        left join public.timesheets_financials tf
          on tf.timesheet_id = c.timesheet_id
         and tf.is_current = true
        where coalesce(tf.locked_by_invoice_id, null) is null
          and not exists (
            select 1
            from jsonb_array_elements(
              case
                when tf.invoice_breakdown_json is not null
                 and jsonb_typeof(tf.invoice_breakdown_json)='object'
                 and jsonb_typeof(tf.invoice_breakdown_json->'segments')='array'
                then tf.invoice_breakdown_json->'segments'
                else '[]'::jsonb
              end
            ) s(seg)
            where nullif(btrim(coalesce(s.seg->>'invoice_locked_invoice_id','')), '') is not null
          )
        on conflict do nothing;

        select coalesce(array_agg(d.timesheet_id), array[]::uuid[])
        into v_cleanup_ts_ids
        from tmp_cleanup_delete d;

        v_cleanup_count := coalesce(array_length(v_cleanup_ts_ids, 1), 0);

        if v_cleanup_count > 0 then
          delete from public.pay_item_snoozes ps where ps.timesheet_id = any(v_cleanup_ts_ids);
          delete from public.pay_batch_items pbi where pbi.timesheet_id = any(v_cleanup_ts_ids);
          delete from public.ts_pay_adjustments tpa where tpa.timesheet_id = any(v_cleanup_ts_ids);
          delete from public.timesheet_pay_state_history tpsh where tpsh.timesheet_id = any(v_cleanup_ts_ids);
          delete from public.timesheet_pay_state tps where tps.timesheet_id = any(v_cleanup_ts_ids);
          delete from public.timesheet_evidence te where te.timesheet_id = any(v_cleanup_ts_ids);
          delete from public.manual_timesheet_queue mtq where mtq.timesheet_id = any(v_cleanup_ts_ids);
          delete from public.ts_pdfs_outbox tpo where tpo.timesheet_id = any(v_cleanup_ts_ids);
          delete from public.ts_financials_outbox tfo where tfo.timesheet_id = any(v_cleanup_ts_ids);
          delete from public.timesheets_financials tfz where tfz.timesheet_id = any(v_cleanup_ts_ids);
          delete from public.contract_weeks cwz where cwz.timesheet_id = any(v_cleanup_ts_ids);
          delete from public.timesheets tz where tz.timesheet_id = any(v_cleanup_ts_ids);
        end if;
      exception when others then
        -- never fail cancellation due to cleanup
        null;
      end;

      -- User-facing audit (UNGATED) for in-place cancellation (timesheet)
      begin
        if v_timesheet_id is not null then
          perform public._audit_insert(
            'timesheets',
            v_timesheet_id::text,
            'NHSP_IMPORT_CANCELLATION_APPLIED',
            null,
            jsonb_build_object(
              'import_id', p_import_id::text,
              'branch', v_branch,
              'shift_id', v_shift_id::text,
              'work_date', v_shift_work_date::text,
              'external_row_key', v_shift_external_row_key,
              'ref_num', nullif(btrim(coalesce(v_shift_ref_num,'')), ''),
              'cancel_reason', v_reason,
              'old_paid_minutes', v_old_paid_minutes,
              'new_paid_minutes', 0,
              'delta_paid_minutes', v_delta_paid_minutes,
              'old_pay_amount_ex_vat', v_old_pay_amount_ex_vat,
              'old_charge_amount_ex_vat', v_old_charge_amount_ex_vat,
              'cleanup_deleted_changed_hours_count', v_cleanup_count
            ),
            'IMPORT_CANCEL_DETACH',
            p_actor_user_id
          );
        end if;
      exception when others then
        null;
      end;

    else
      v_branch := 'CORRECTION';

      -- Resolve week_ending_date for the correction timesheet (never assume Sunday)
      v_week_ending_date := null;
      v_base_ts_week_ending := null;

      if v_timesheet_id is not null then
        select t.week_ending_date
        into v_base_ts_week_ending
        from public.timesheets t
        where t.timesheet_id = v_timesheet_id
        limit 1;

        if v_base_ts_week_ending is not null then
          v_week_ending_date := v_base_ts_week_ending;
        end if;
      end if;

      if v_week_ending_date is null and v_shift_week_ending_date is not null then
        v_week_ending_date := v_shift_week_ending_date;
      end if;

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
        raise exception 'nhsp_weekly_apply_cancellations: shift % cannot resolve week_ending_date for correction.', v_shift_id;
      end if;

      -- ─────────────────────────────────────────────
      -- ✅ Determine cancellation anchor:
      --   - If there is an invoiced/locked CHANGED_HOURS_REPLACEMENT (POS), reverse that.
      --   - Else reverse the BASE locked segment (v_seg_json), and use base import evidence from CHANGED_HOURS_REVERSAL when available.
      -- ─────────────────────────────────────────────
      -- Default anchor = current shift truth (fallback only)
      v_anchor_start_utc := v_shift_start_utc;
      v_anchor_end_utc := v_shift_end_utc;
      v_anchor_break_mins := greatest(0, coalesce(v_shift_break_mins, 0));
      v_anchor_import_id := v_shift_latest_import_id;

      -- Find latest POS (replacement) correction timesheet for this shift
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

        if v_pos_is_invoiced is true and v_pos_schedule is not null and jsonb_typeof(v_pos_schedule)='array' then
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
            v_anchor_break_mins := greatest(0, coalesce(nullif(btrim(coalesce((v_pos_schedule->0)->>'break_mins','')), '')::int, 0));
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

      -- If POS is NOT invoiced, anchor to base locked segment (if available)
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

      -- Require anchor start/end for schedule-driven correction artefact
      if v_anchor_start_utc is null or v_anchor_end_utc is null then
        raise exception 'nhsp_weekly_apply_cancellations: shift % missing anchor start/end; cannot create schedule-driven cancellation correction.', v_shift_id;
      end if;

      -- Recompute minutes helper for audit based on anchor (not current truth)
      v_old_paid_minutes := greatest(
        0,
        (extract(epoch from (v_anchor_end_utc - v_anchor_start_utc)) / 60)::int - greatest(0, coalesce(v_anchor_break_mins, 0))
      );
      v_new_paid_minutes := 0;
      v_delta_paid_minutes := (0 - v_old_paid_minutes);

      -- Build correction_id (stable + deterministic) using anchor times
      v_fnv_s :=
        coalesce(p_import_id::text,'') || '|' ||
        coalesce(v_shift_id::text,'') || '|' ||
        coalesce(v_shift_ref_num,'') || '|' ||
        coalesce(v_shift_external_row_key,'') || '|' ||
        coalesce(v_anchor_start_utc::text,'') || '|' ||
        coalesce(v_anchor_end_utc::text,'') || '|' ||
        coalesce(coalesce(v_anchor_break_mins,0)::text,'');

      v_fnv_h := 2166136261;
      for v_fnv_i in 1..char_length(v_fnv_s) loop
        v_fnv_h := (v_fnv_h # ascii(substring(v_fnv_s from v_fnv_i for 1)));
        v_fnv_h := (v_fnv_h * 16777619) % 4294967296;
      end loop;
      v_fnv_hex := lpad(lower(to_hex(v_fnv_h)), 8, '0');

      v_correction_id := 'can:' || p_import_id::text || ':' || v_shift_id::text || ':' || v_fnv_hex;

      -- Load contract + optional client/candidate display context for norms
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

      -- Schedule entry (anchor-based)
      v_schedule := jsonb_build_array(
        jsonb_build_object(
          'date', v_shift_work_date::text,
          'ward', nullif(btrim(coalesce(v_shift_ward, v_contract_ward_hint, '')), ''),
          'start_utc', v_anchor_start_utc::text,
          'end_utc', v_anchor_end_utc::text,
          'start', to_char((v_anchor_start_utc at time zone 'Europe/London')::time, 'HH24:MI'),
          'end', to_char((v_anchor_end_utc at time zone 'Europe/London')::time, 'HH24:MI'),
          'break_mins', greatest(0, coalesce(v_anchor_break_mins, 0)),
          'ref_num', nullif(btrim(coalesce(v_shift_ref_num,'')), ''),
          'shift_id', v_shift_id::text,
          'external_row_key', v_shift_external_row_key,
          'import_id', case when v_anchor_import_id is null then null else v_anchor_import_id::text end
        )
      );

      v_hint := jsonb_build_object(
        'import_cancellation', jsonb_build_object(
          'import_id', p_import_id::text,
          'shift_id', v_shift_id::text,
          'ref_num', nullif(btrim(coalesce(v_shift_ref_num,'')), ''),
          'correction_id', v_correction_id,
          'correction_kind', v_kind,
          'invoice_id_detected', case when v_invoice_id_detected is null then null else v_invoice_id_detected::text end,
          'anchor', case
                      when v_pos_is_invoiced is true then 'INVOICED_REPLACEMENT'
                      else 'BASE_LOCKED'
                    end
        )
      );

      v_shift_label := 'weekly-cancel-reversal-' || v_correction_id;

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

      -- Ensure base week exists (seq=0)
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
        'SUBMITTED'::public.contract_week_status_enum,
        'MANUAL'::public.submission_mode_enum,
        null,
        '[]'::jsonb,
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
        raise exception 'nhsp_weekly_apply_cancellations: failed to ensure base contract_week exists (contract_id=% week_ending=%).',
          v_shift_contract_id, v_week_ending_date;
      end if;

      -- Idempotency: reuse existing correction timesheet (unique on correction_id+kind)
      v_existing_ts_id := null;

      select t2.timesheet_id
      into v_existing_ts_id
      from public.timesheets t2
      where t2.correction_id = v_correction_id
        and t2.correction_kind = v_kind
      order by t2.is_current desc, t2.version desc
      limit 1
      for update;

      if v_existing_ts_id is not null then
        v_correction_ts_id := v_existing_ts_id;

        -- Ensure there is an adjustment contract_week linked; if missing, create one and link it.
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
              raise exception 'nhsp_weekly_apply_cancellations: failed to allocate additional_seq after retries (contract_id=% week_ending=%).',
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

        -- Update existing correction timesheet to match intended schedule + metadata
        update public.timesheets tu
        set
          booking_id = v_booking_id,
          version = 1,
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
          correction_id = v_correction_id,
          correction_kind = v_kind,
          adjustment_origin = 'IMPORT_CANCELLATION',

          updated_at = v_now
        where tu.timesheet_id = v_existing_ts_id;

      else
        -- Create a new adjustment contract_week (safe additional_seq)
        perform 1
        from public.contract_weeks cwlock2
        where cwlock2.contract_id = v_shift_contract_id
          and cwlock2.week_ending_date = v_week_ending_date
        for update;

        v_try := 0;
        loop
          v_try := v_try + 1;
          if v_try > 10 then
            raise exception 'nhsp_weekly_apply_cancellations: failed to allocate additional_seq after retries (contract_id=% week_ending=%).',
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

        -- Insert correction timesheet (idempotent by uq_timesheets_correction_id_kind)
        begin
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

            lower(coalesce(v_candidate_tms_ref, v_candidate_display_name, v_shift_candidate_id::text)),
            lower(coalesce(v_contract_display_site, v_client_name, v_shift_client_id::text)),
            lower(coalesce(v_contract_ward_hint, 'contract')),
            lower(coalesce(v_contract_role, 'weekly')),
            v_shift_label_norm,

            v_week_ending_date,
            v_shift_contract_id,

            null,
            v_schedule,

            v_hint,
            v_hint,

            true,
            null,
            v_correction_id,
            v_kind,
            'IMPORT_CANCELLATION',

            v_now,
            v_now
          )
          returning timesheet_id into v_ts_id;

          v_correction_ts_id := v_ts_id;

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
            raise exception 'nhsp_weekly_apply_cancellations: unique_violation inserting correction timesheet but failed to find existing row (correction_id=% kind=%).',
              v_correction_id, v_kind;
          end if;

          v_correction_ts_id := v_ts_id;

          update public.timesheets t4
          set
            booking_id = v_booking_id,
            version = 1,
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
            parent_timesheet_id = null,
            correction_id = v_correction_id,
            correction_kind = v_kind,
            adjustment_origin = 'IMPORT_CANCELLATION',

            updated_at = v_now
          where t4.timesheet_id = v_ts_id;
        end;

        -- Link adjustment contract_week -> timesheet
        update public.contract_weeks cwlink
        set
          timesheet_id = v_correction_ts_id,
          status = 'SUBMITTED'::public.contract_week_status_enum,
          submission_mode_snapshot = 'MANUAL'::public.submission_mode_enum,
          is_adjustment = true,
          updated_at = v_now
        where cwlink.id = v_cw_id;

      end if;

      -- Enqueue TSFIN for correction timesheet (do NOT touch base timesheet)
      if v_correction_ts_id is not null then
        v_timesheet_ids := array_append(v_timesheet_ids, v_correction_ts_id);
        perform public.enqueue_ts_financials_priority(array[v_correction_ts_id]::uuid[], 'CONTEXT_CHANGED'::public.ts_fin_reason_enum);
      end if;

      -- Update truth (cancel) but do NOT recompute base TSFIN in this branch
      update public.nhsp_shifts ns3
      set
        cancelled_at_utc = v_now,
        cancelled_by_import_id = p_import_id,
        cancelled_reason = v_reason,
        timesheet_id = null
      where ns3.id = v_shift_id;

      v_cancelled_count := v_cancelled_count + 1;

      -- ✅ Cleanup: if POS is NOT invoiced, delete any uninvoiced CHANGED_HOURS corrections (NEG/POS) for this shift
      begin
        create temporary table tmp_cleanup_candidates2(timesheet_id uuid primary key) on commit drop;

        insert into tmp_cleanup_candidates2(timesheet_id)
        select distinct t.timesheet_id
        from public.timesheets t
        where t.is_adjustment is true
          and t.is_current is true
          and t.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
          and t.contract_id = v_shift_contract_id
          and t.week_ending_date = v_week_ending_date
          and jsonb_typeof(t.actual_schedule_json)='array'
          and (
            t.actual_schedule_json @> jsonb_build_array(jsonb_build_object('shift_id', v_shift_id::text))
            or (
              v_shift_external_row_key is not null
              and t.actual_schedule_json @> jsonb_build_array(jsonb_build_object('external_row_key', v_shift_external_row_key))
            )
          )
        on conflict do nothing;

        create temporary table tmp_cleanup_delete2(timesheet_id uuid primary key) on commit drop;

        insert into tmp_cleanup_delete2(timesheet_id)
        select c.timesheet_id
        from tmp_cleanup_candidates2 c
        left join public.timesheets_financials tf
          on tf.timesheet_id = c.timesheet_id
         and tf.is_current = true
        where coalesce(tf.locked_by_invoice_id, null) is null
          and not exists (
            select 1
            from jsonb_array_elements(
              case
                when tf.invoice_breakdown_json is not null
                 and jsonb_typeof(tf.invoice_breakdown_json)='object'
                 and jsonb_typeof(tf.invoice_breakdown_json->'segments')='array'
                then tf.invoice_breakdown_json->'segments'
                else '[]'::jsonb
              end
            ) s(seg)
            where nullif(btrim(coalesce(s.seg->>'invoice_locked_invoice_id','')), '') is not null
          )
        on conflict do nothing;

        select coalesce(array_agg(d.timesheet_id), array[]::uuid[])
        into v_cleanup_ts_ids
        from tmp_cleanup_delete2 d;

        v_cleanup_count := coalesce(array_length(v_cleanup_ts_ids, 1), 0);

        -- Only perform deletion when POS is NOT invoiced (base-lock cancellation case)
        if v_pos_is_invoiced is not true and v_cleanup_count > 0 then
          delete from public.pay_item_snoozes ps where ps.timesheet_id = any(v_cleanup_ts_ids);
          delete from public.pay_batch_items pbi where pbi.timesheet_id = any(v_cleanup_ts_ids);
          delete from public.ts_pay_adjustments tpa where tpa.timesheet_id = any(v_cleanup_ts_ids);
          delete from public.timesheet_pay_state_history tpsh where tpsh.timesheet_id = any(v_cleanup_ts_ids);
          delete from public.timesheet_pay_state tps where tps.timesheet_id = any(v_cleanup_ts_ids);
          delete from public.timesheet_evidence te where te.timesheet_id = any(v_cleanup_ts_ids);
          delete from public.manual_timesheet_queue mtq where mtq.timesheet_id = any(v_cleanup_ts_ids);
          delete from public.ts_pdfs_outbox tpo where tpo.timesheet_id = any(v_cleanup_ts_ids);
          delete from public.ts_financials_outbox tfo where tfo.timesheet_id = any(v_cleanup_ts_ids);
          delete from public.timesheets_financials tfz where tfz.timesheet_id = any(v_cleanup_ts_ids);
          delete from public.contract_weeks cwz where cwz.timesheet_id = any(v_cleanup_ts_ids);
          delete from public.timesheets tz where tz.timesheet_id = any(v_cleanup_ts_ids);
        end if;
      exception when others then
        null;
      end;

      -- User-facing audit (UNGATED): correction timesheet + contract_week + invoice history
      begin
        if v_correction_ts_id is not null then
          perform public._audit_insert(
            'timesheets',
            v_correction_ts_id::text,
            'NHSP_IMPORT_CANCELLATION_CORRECTION_CREATED',
            null,
            jsonb_build_object(
              'import_id', p_import_id::text,
              'evidence_import_id', case when v_anchor_import_id is null then null else v_anchor_import_id::text end,
              'branch', v_branch,
              'shift_id', v_shift_id::text,
              'work_date', v_shift_work_date::text,
              'external_row_key', v_shift_external_row_key,
              'ref_num', nullif(btrim(coalesce(v_shift_ref_num,'')), ''),
              'cancel_reason', v_reason,
              'invoice_id', case when v_invoice_id_detected is null then null else v_invoice_id_detected::text end,
              'invoice_no', v_invoice_number_text,
              'anchor_start_utc', v_anchor_start_utc::text,
              'anchor_end_utc', v_anchor_end_utc::text,
              'anchor_break_mins', v_anchor_break_mins,
              'anchor_mode', case when v_pos_is_invoiced is true then 'INVOICED_REPLACEMENT' else 'BASE_LOCKED' end,
              'cleanup_deleted_changed_hours_count', v_cleanup_count,
              'cleanup_deleted_timesheet_ids', to_jsonb(coalesce(v_cleanup_ts_ids, array[]::uuid[])),
              'correction_id', v_correction_id,
              'correction_kind', v_kind
            ),
            'IMPORT_CANCELLATION_CORRECTION',
            p_actor_user_id
          );
        end if;

        if v_invoice_id_detected is not null then
          perform public._inv_write_audit(
            p_actor_user_id,
            'NHSP_IMPORT_CANCELLATION_CORRECTION_CREATED',
            jsonb_build_object(
              'import_id', p_import_id::text,
              'evidence_import_id', case when v_anchor_import_id is null then null else v_anchor_import_id::text end,
              'shift_id', v_shift_id::text,
              'work_date', v_shift_work_date::text,
              'external_row_key', v_shift_external_row_key,
              'ref_num', nullif(btrim(coalesce(v_shift_ref_num,'')), ''),
              'invoice_id', v_invoice_id_detected::text,
              'invoice_no', v_invoice_number_text,
              'correction_timesheet_id', case when v_correction_ts_id is null then null else v_correction_ts_id::text end,
              'anchor_mode', case when v_pos_is_invoiced is true then 'INVOICED_REPLACEMENT' else 'BASE_LOCKED' end,
              'cleanup_deleted_changed_hours_count', v_cleanup_count
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
        'ref_num', v_shift_ref_num,
        'present_in_file', v_present_in_file,
        'timesheet_id', case when v_timesheet_id is null then null else v_timesheet_id::text end,
        'invoice_id_detected', case when v_invoice_id_detected is null then null else v_invoice_id_detected::text end,
        'invoice_no', v_invoice_number_text,
        'invoiced_detected', v_invoiced_detected,
        'branch', v_branch,
        'correction_timesheet_id', case when v_correction_ts_id is null then null else v_correction_ts_id::text end,
        'anchor_mode', case when v_pos_is_invoiced is true then 'INVOICED_REPLACEMENT' else 'BASE_LOCKED' end,
        'cleanup_deleted_changed_hours_count', v_cleanup_count
      ));
      v_sample_n := v_sample_n + 1;
    end if;

  end loop;

  -- Deduplicate id arrays for output
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
    'NHSP_CANCEL_APPLY_DEBUG',
    jsonb_build_object(
      'import_id', p_import_id::text,
      'selected_count', jsonb_array_length(v_actions),
      'cancelled_count', v_cancelled_count,
      'file_ref_count', v_file_ref_count,
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

  -- No credit notes / pdf jobs created by this RPC under locked policy
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
      'NHSP_CANCEL_APPLY_ERROR',
      jsonb_build_object(
        'import_id', p_import_id::text,
        'last_shift_id', case when v_last_shift_id is null then null else v_last_shift_id::text end,
        'selected_count', case when jsonb_typeof(v_actions) = 'array' then jsonb_array_length(v_actions) else null end,
        'cancelled_count', v_cancelled_count,
        'file_ref_count', v_file_ref_count,
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
$$;







create or replace function public.nhsp_weekly_phase3_apply_adjustment_truth(
  p_import_id uuid,
  p_selected_external_row_keys text[],
  p_actor_user_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
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
  v_existing_pos_is_invoiced boolean := false;
  v_existing_pos_tf_locked_by_invoice_id uuid := null;
  v_existing_pos_tf_invoice_breakdown_json jsonb := null;
  v_existing_pos_seg_invoice_id uuid := null;
  v_existing_pos_seg jsonb := null;

  v_existing_pos_old_start_str text := null;
  v_existing_pos_old_end_str text := null;
  v_existing_pos_old_break_str text := null;
  v_existing_pos_import_id uuid := null;

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

    -- ✅ Edge case: truth returns to base exactly AND both NEG₀ and POS₀ exist and are uninvoiced
    -- Safe delete only if:
    -- - exactly one NEG and one POS exist for this shift linkage (avoid multi-generation ambiguity)
    -- - both are NOT invoiced/locked
    -- - new truth equals the base (taken from NEG schedule start/end/break)
    if v_existing_pos_ts_id is not null
       and v_existing_neg_ts_id is not null
       and v_existing_pos_is_invoiced is false
       and v_existing_neg_is_invoiced is false
       and coalesce(v_existing_pos_count, 0) = 1
       and coalesce(v_existing_neg_count, 0) = 1
    then
      begin
        if v_existing_neg_schedule is not null and jsonb_typeof(v_existing_neg_schedule) = 'array' then
          begin
            v_existing_neg_base_start_utc := nullif(btrim(coalesce((v_existing_neg_schedule->0)->>'start_utc','')), '')::timestamptz;
          exception when others then
            v_existing_neg_base_start_utc := null;
          end;

          begin
            v_existing_neg_base_end_utc := nullif(btrim(coalesce((v_existing_neg_schedule->0)->>'end_utc','')), '')::timestamptz;
          exception when others then
            v_existing_neg_base_end_utc := null;
          end;

          begin
            v_existing_neg_base_break_mins := coalesce(nullif(btrim(coalesce((v_existing_neg_schedule->0)->>'break_mins','')), '')::int, 0);
          exception when others then
            v_existing_neg_base_break_mins := 0;
          end;
        end if;
      exception when others then
        v_existing_neg_base_start_utc := null;
        v_existing_neg_base_end_utc := null;
        v_existing_neg_base_break_mins := null;
      end;

      if v_existing_neg_base_start_utc is not null
         and v_existing_neg_base_end_utc is not null
         and v_existing_neg_base_start_utc = v_new_start_utc
         and v_existing_neg_base_end_utc = v_new_end_utc
         and coalesce(v_existing_neg_base_break_mins, 0) = greatest(0, coalesce(v_new_break_mins, 0))
      then
        -- Lock both adjustment timesheets before deleting
        perform 1
        from public.timesheets tlock_del
        where tlock_del.timesheet_id = any(array[v_existing_neg_ts_id, v_existing_pos_ts_id]::uuid[])
        for update;

        -- Remove financial snapshots first (avoid FK blocks)
        begin
          delete from public.timesheets_financials tf_del
          where tf_del.timesheet_id = any(array[v_existing_neg_ts_id, v_existing_pos_ts_id]::uuid[]);
        exception when others then
          null;
        end;

        -- Best-effort: remove any outbox entries for these timesheets (table/column may vary)
        begin
          delete from public.ts_financials_outbox ob_del
          where ob_del.timesheet_id = any(array[v_existing_neg_ts_id, v_existing_pos_ts_id]::uuid[]);
        exception when undefined_table then
          null;
        when undefined_column then
          null;
        when others then
          null;
        end;

        -- Remove contract_week links for these corrections
        begin
          delete from public.contract_weeks cw_del
          where cw_del.timesheet_id = any(array[v_existing_neg_ts_id, v_existing_pos_ts_id]::uuid[]);
        exception when others then
          null;
        end;

        -- Finally delete the timesheets themselves
        delete from public.timesheets t_del
        where t_del.timesheet_id = any(array[v_existing_neg_ts_id, v_existing_pos_ts_id]::uuid[]);

        v_deleted_redundant_pair := true;

        -- reflect in debug sample
        v_key_ts := v_key_ts || jsonb_build_array(jsonb_build_object(
          'kind', 'REDUNDANT_PAIR',
          'op', 'DELETED_NEG_AND_POS',
          'neg_timesheet_id', v_existing_neg_ts_id::text,
          'pos_timesheet_id', v_existing_pos_ts_id::text
        ));

        -- count as "updates" (state changed) but do NOT return deleted ids
        v_upd_count := v_upd_count + 2;

        -- continue to next key: nothing else to do for this change
        continue;
      end if;
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

    -- Best-effort invoice number lookup for UI (never fail the function)
    v_invoice_number_text := null;

    if v_invoice_id_detected is not null then
      begin
        begin
          select i.invoice_no::text
          into v_invoice_number_text
          from public.invoices i
          where i.id = v_invoice_id_detected
          limit 1;
        exception when undefined_column then
          begin
            select i.invoice_number::text
            into v_invoice_number_text
            from public.invoices i
            where i.id = v_invoice_id_detected
            limit 1;
          exception when undefined_column then
            begin
              select i.number::text
              into v_invoice_number_text
              from public.invoices i
              where i.id = v_invoice_id_detected
              limit 1;
            exception when undefined_column then
              v_invoice_number_text := null;
            end;
          end;
        end;
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

      -- Lock and update the existing replacement timesheet in place
      perform 1
      from public.timesheets tlock
      where tlock.timesheet_id = v_existing_pos_ts_id
      for update;

      update public.timesheets tup
      set
        actual_schedule_json = v_schedule,
        qr_payload_json = v_hint,
        candidate_hint_text = v_hint,

        -- ✅ inherit policy identity from base timesheet
        sheet_scope = v_parent_sheet_scope,
        submission_mode = v_parent_submission_mode,
        parent_timesheet_id = v_base_timesheet_id,

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
$$;





create or replace function public.nhsp_weekly_apply_transactional(
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

  -- payload parts
  v_payload jsonb := coalesce(p_payload, '{}'::jsonb);
  v_actions_json jsonb := coalesce(v_payload->'selected_action_ids', '[]'::jsonb);
  v_actions2_json jsonb := coalesce(v_payload->'selected_actions', '[]'::jsonb);
  v_auto_apply_json jsonb := coalesce(v_payload->'auto_apply_action_ids', '[]'::jsonb); -- optional / forward-compatible

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

  v_steps := v_steps || jsonb_build_array(jsonb_build_object('step','IMPORT_OK'));

  -- ─────────────────────────────────────────────
  -- 1) Parse and normalize selection payload (ROW:/CANCEL:)
  -- ─────────────────────────────────────────────
  if jsonb_typeof(v_actions_json) <> 'array' then
    raise exception 'nhsp_weekly_apply_transactional: selected_action_ids must be a JSON array.';
  end if;

  if jsonb_typeof(v_actions2_json) <> 'array' then
    raise exception 'nhsp_weekly_apply_transactional: selected_actions must be a JSON array.';
  end if;

  if jsonb_typeof(v_auto_apply_json) <> 'array' then
    raise exception 'nhsp_weekly_apply_transactional: auto_apply_action_ids must be a JSON array when provided.';
  end if;

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

  -- Forward-compatible: allow FE to pass auto_apply_action_ids explicitly (same ROW:/CANCEL: contract)
  insert into tmp_sel_ids(action_id)
  select distinct nullif(btrim(x2.value), '')
  from jsonb_array_elements_text(v_auto_apply_json) as x2(value)
  where nullif(btrim(x2.value), '') is not null
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
    set
      import_scope = 'NHSP',
      applied_at = v_now
    where hi_noop.id = p_import_id;

    v_steps := v_steps || jsonb_build_array(jsonb_build_object('step','IMPORT_APPLIED_NOOP'));

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

    return jsonb_build_object(
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
      'affected_timesheet_ids', to_jsonb(array[]::uuid[])
    );
  end if;

  v_steps := v_steps || jsonb_build_array(jsonb_build_object('step','NOOP_GUARD_PASSED'));

  -- ─────────────────────────────────────────────
  -- 3) Snapshot changed-hours rows for selected keys BEFORE any truth mutation
  -- ─────────────────────────────────────────────
  create temporary table tmp_changed_sel on commit drop as
  select
    ch.external_row_key,
    ch.is_invoiced
  from public.weekly_import_changed_hours_phase3(p_import_id := p_import_id, p_system_type := 'NHSP') as ch
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
        status = 'SUBMITTED'::public.contract_week_status_enum,
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
        status = 'SUBMITTED'::public.contract_week_status_enum,
        submission_mode_snapshot = 'MANUAL'::public.submission_mode_enum,
        updated_at = v_now
      where cw0keep.id = v_base_week_id;

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

    update public.nhsp_shifts nsu0
    set
      timesheet_id = v_base_week_ts_id,
      updated_at = v_now
    where nsu0.source_system = 'NHSP'::public.hr_source_enum
      and nsu0.cancelled_at_utc is null
      and nsu0.contract_id = v_pair_contract_id
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

  select coalesce(array_agg(distinct a.timesheet_id order by a.timesheet_id), array[]::uuid[])
  into v_affected_timesheet_ids
  from tmp_aff_ts a
  where a.timesheet_id is not null;

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
  -- 10) Mark import applied (inside transaction)
  -- ─────────────────────────────────────────────
  update public.hr_imports hi3
  set
    import_scope = 'NHSP',
    applied_at = v_now
  where hi3.id = p_import_id;

  v_steps := v_steps || jsonb_build_array(jsonb_build_object('step','IMPORT_APPLIED'));

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

  return jsonb_build_object(
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
    'affected_timesheet_ids', to_jsonb(coalesce(v_affected_timesheet_ids, array[]::uuid[]))
  );

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
$$;



create or replace function public.hr_issue_emails_touch(
  p_source_system     text,
  p_import_id         uuid,
  p_client_id         uuid,
  p_timesheet_id      uuid,
  p_reason_code       text,
  p_issue_fingerprint text,
  p_actor_user_id     uuid,

  -- Optional (keeps daily HR audit richness; safe to omit)
  p_hr_row_id         uuid default null,
  p_staff_norm        text default null,
  p_hospital_norm     text default null,
  p_work_date         date default null
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now timestamptz := now();
  v_source_system text;
  v_reason_code text;
  v_fingerprint text;

  v_inserted boolean := false;
  v_last_sent_at timestamptz := null;
  v_id uuid := null;
begin
  v_source_system := nullif(btrim(p_source_system), '');
  v_reason_code := nullif(btrim(p_reason_code), '');
  v_fingerprint := nullif(btrim(p_issue_fingerprint), '');

  if v_source_system is null then
    raise exception 'hr_issue_emails_touch: p_source_system is required';
  end if;

  if v_reason_code is null then
    raise exception 'hr_issue_emails_touch: p_reason_code is required';
  end if;

  if v_fingerprint is null then
    raise exception 'hr_issue_emails_touch: p_issue_fingerprint is required';
  end if;

  with upserted as (
    insert into public.hr_issue_emails (
      source_system,
      import_id,
      client_id,
      timesheet_id,
      hr_row_id,
      staff_norm,
      hospital_norm,
      work_date,
      reason_code,
      issue_fingerprint,
      last_sent_at,
      created_at,
      updated_at
    )
    values (
      v_source_system,
      p_import_id,
      p_client_id,
      p_timesheet_id,
      p_hr_row_id,
      p_staff_norm,
      p_hospital_norm,
      p_work_date,
      v_reason_code,
      v_fingerprint,
      v_now,
      v_now,
      v_now
    )
    on conflict (issue_fingerprint) do update
      set
        -- “Re-email” semantics: touch the timestamp
        last_sent_at = excluded.last_sent_at,
        updated_at   = excluded.updated_at,

        -- keep latest context
        source_system = excluded.source_system,
        import_id     = excluded.import_id,
        client_id     = excluded.client_id,
        timesheet_id  = excluded.timesheet_id,
        reason_code   = excluded.reason_code,

        -- only overwrite audit-detail fields when caller provides values
        hr_row_id     = coalesce(excluded.hr_row_id, public.hr_issue_emails.hr_row_id),
        staff_norm    = coalesce(excluded.staff_norm, public.hr_issue_emails.staff_norm),
        hospital_norm = coalesce(excluded.hospital_norm, public.hr_issue_emails.hospital_norm),
        work_date     = coalesce(excluded.work_date, public.hr_issue_emails.work_date)
    returning
      (xmax = 0) as inserted,
      public.hr_issue_emails.id as id,
      public.hr_issue_emails.last_sent_at as last_sent_at
  )
  select
    u.inserted,
    u.id,
    u.last_sent_at
  into
    v_inserted,
    v_id,
    v_last_sent_at
  from upserted u;

  return jsonb_build_object(
    'email_kind', case when v_inserted then 'EMAIL' else 'REEMAIL' end,
    'last_sent_at_utc', v_last_sent_at,
    'id', v_id
  );
end;
$$;


create or replace function public.hr_daily_apply_transactional(
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

  v_src public.hr_source_enum;

  v_payload jsonb := coalesce(p_payload, '{}'::jsonb);
  v_validation_rows_json jsonb := coalesce(v_payload->'validation_rows', '[]'::jsonb);
  v_email_actions_json   jsonb := coalesce(v_payload->'email_actions',   '[]'::jsonb);

  v_validations_upserted int := 0;
  v_timesheets_ref_updated int := 0;

  v_email_logs_upserted int := 0;
  v_email_jobs jsonb := '[]'::jsonb;

  v_email_selected_count int := 0;

begin
  -- 0) Validate import exists + is HEALTHROSTER_DAILY
  select hi.source_system
    into v_src
  from public.hr_imports hi
  where hi.id = p_import_id
  limit 1;

  if v_src is null then
    raise exception 'hr_daily_apply_transactional: import % not found in hr_imports.', p_import_id;
  end if;

  if v_src <> 'HEALTHROSTER_DAILY'::public.hr_source_enum then
    raise exception 'hr_daily_apply_transactional: import % source_system=%; expected HEALTHROSTER_DAILY.', p_import_id, v_src::text;
  end if;

  -- 1) Validate payload shapes
  if jsonb_typeof(v_validation_rows_json) <> 'array' then
    raise exception 'hr_daily_apply_transactional: validation_rows must be a JSON array.';
  end if;

  if jsonb_typeof(v_email_actions_json) <> 'array' then
    raise exception 'hr_daily_apply_transactional: email_actions must be a JSON array.';
  end if;

  -- 2) Normalise validation rows (dedupe per timesheet_id and keep worst-case)
  create temporary table tmp_val_raw(
    timesheet_id uuid not null,
    status_text text null,
    reason_code text null,
    hr_request_id text null
  ) on commit drop;

  insert into tmp_val_raw(timesheet_id, status_text, reason_code, hr_request_id)
  select
    nullif(btrim(j.value->>'timesheet_id'), '')::uuid as timesheet_id,
    nullif(btrim(j.value->>'status'), '') as status_text,
    nullif(btrim(j.value->>'reason_code'), '') as reason_code,
    nullif(btrim(j.value->>'hr_request_id'), '') as hr_request_id
  from jsonb_array_elements(v_validation_rows_json) as j(value)
  where nullif(btrim(j.value->>'timesheet_id'), '') is not null;

  create temporary table tmp_val_by_ts(
    timesheet_id uuid primary key,
    has_error boolean not null,
    chosen_reason_code text null,
    chosen_hr_request_id text null
  ) on commit drop;

  insert into tmp_val_by_ts(timesheet_id, has_error, chosen_reason_code, chosen_hr_request_id)
  select
    vr.timesheet_id,
    bool_or(
      not (upper(coalesce(vr.status_text, '')) in ('VALIDATION_OK','OK','PASS','VALID'))
    ) as has_error,
    case
      when bool_or(
        not (upper(coalesce(vr.status_text, '')) in ('VALIDATION_OK','OK','PASS','VALID'))
      )
      then
        min(vr.reason_code) filter (
          where not (upper(coalesce(vr.status_text, '')) in ('VALIDATION_OK','OK','PASS','VALID'))
        )
      else
        'HEALTHROSTER_DAILY'
    end as chosen_reason_code,
    max(vr.hr_request_id) as chosen_hr_request_id
  from tmp_val_raw vr
  group by vr.timesheet_id;

  -- 3) Upsert timesheet_validations (required + transactional)
  insert into public.timesheet_validations(
    timesheet_id,
    status,
    reason_code,
    validated_at_utc,
    last_source,
    updated_at,
    hr_request_id,
    hr_request_source,
    hr_request_set_by,
    hr_request_set_at_utc
  )
  select
    vt.timesheet_id,
    case when vt.has_error then 'VALIDATION_ERROR'::public.validation_status_enum else 'VALIDATION_OK'::public.validation_status_enum end,
    vt.chosen_reason_code,
    case when vt.has_error then null else v_now end,
    p_import_id,
    v_now,
    vt.chosen_hr_request_id,
    case when vt.chosen_hr_request_id is null then null else 'IMPORTED'::public.reference_source_enum end,
    case when vt.chosen_hr_request_id is null then null else p_actor_user_id end,
    case when vt.chosen_hr_request_id is null then null else v_now end
  from tmp_val_by_ts vt
  on conflict (timesheet_id) do update
    set status               = excluded.status,
        reason_code           = excluded.reason_code,
        validated_at_utc      = excluded.validated_at_utc,
        last_source           = excluded.last_source,
        updated_at            = excluded.updated_at,
        hr_request_id         = excluded.hr_request_id,
        hr_request_source     = excluded.hr_request_source,
        hr_request_set_by     = excluded.hr_request_set_by,
        hr_request_set_at_utc = excluded.hr_request_set_at_utc;

  get diagnostics v_validations_upserted = row_count;

  -- 4) Match existing daily behaviour: when VALIDATION_OK and hr_request_id present, set timesheets.reference_number
  update public.timesheets ts
     set reference_number = vt.chosen_hr_request_id,
         updated_at = v_now
    from tmp_val_by_ts vt
   where ts.timesheet_id = vt.timesheet_id
     and ts.is_current = true
     and vt.has_error is false
     and vt.chosen_hr_request_id is not null
     and (ts.reference_number is distinct from vt.chosen_hr_request_id);

  get diagnostics v_timesheets_ref_updated = row_count;

  -- 5) Email actions: upsert hr_issue_emails (transactional) + return email_jobs
  create temporary table tmp_email_actions(
    timesheet_id uuid not null,
    issue_fingerprint text not null,
    reason_code text null,
    hr_row_id uuid null,
    staff_norm text null,
    hospital_norm text null,
    work_date date null
  ) on commit drop;

  insert into tmp_email_actions(timesheet_id, issue_fingerprint, reason_code, hr_row_id, staff_norm, hospital_norm, work_date)
  select
    nullif(btrim(e.value->>'timesheet_id'), '')::uuid as timesheet_id,
    nullif(btrim(e.value->>'issue_fingerprint'), '') as issue_fingerprint,
    nullif(btrim(e.value->>'reason_code'), '') as reason_code,
    nullif(btrim(e.value->>'hr_row_id'), '')::uuid as hr_row_id,
    nullif(btrim(e.value->>'staff_norm'), '') as staff_norm,
    nullif(btrim(e.value->>'hospital_norm'), '') as hospital_norm,
    nullif(btrim(e.value->>'work_date'), '')::date as work_date
  from jsonb_array_elements(v_email_actions_json) as e(value)
  where nullif(btrim(e.value->>'timesheet_id'), '') is not null
    and nullif(btrim(e.value->>'issue_fingerprint'), '') is not null;

  select count(*) into v_email_selected_count from tmp_email_actions;

  create temporary table tmp_email_enriched on commit drop as
  select
    ea.timesheet_id,
    ea.issue_fingerprint,
    coalesce(nullif(ea.reason_code,''), 'actual_hours_mismatch') as reason_code,
    ea.hr_row_id,
    ea.staff_norm,
    ea.hospital_norm,
    ea.work_date,

    coalesce(tf.client_id, ct.client_id) as client_id,
    cli.ts_queries_email as recipient_email,

    exists(
      select 1
      from public.hr_issue_emails hie
      where hie.issue_fingerprint = ea.issue_fingerprint
      limit 1
    ) as emailed_already
  from tmp_email_actions ea
  left join public.timesheets_financials tf
    on tf.timesheet_id = ea.timesheet_id
   and tf.is_current = true
  left join public.timesheets ts
    on ts.timesheet_id = ea.timesheet_id
   and ts.is_current = true
  left join public.contracts ct
    on ct.id = ts.contract_id
  left join public.clients cli
    on cli.id = coalesce(tf.client_id, ct.client_id);

  insert into public.hr_issue_emails(
    source_system,
    import_id,
    client_id,
    timesheet_id,
    hr_row_id,
    staff_norm,
    hospital_norm,
    work_date,
    reason_code,
    issue_fingerprint,
    last_sent_at,
    created_at,
    updated_at
  )
  select
    'HEALTHROSTER_DAILY',
    p_import_id,
    te.client_id,
    te.timesheet_id,
    te.hr_row_id,
    te.staff_norm,
    te.hospital_norm,
    te.work_date,
    te.reason_code,
    te.issue_fingerprint,
    v_now,
    v_now,
    v_now
  from tmp_email_enriched te
  on conflict (issue_fingerprint) do update
    set last_sent_at  = excluded.last_sent_at,
        updated_at    = excluded.updated_at,
        import_id     = excluded.import_id,
        client_id     = excluded.client_id,
        timesheet_id  = excluded.timesheet_id,
        hr_row_id     = excluded.hr_row_id,
        staff_norm    = excluded.staff_norm,
        hospital_norm = excluded.hospital_norm,
        work_date     = excluded.work_date,
        reason_code   = excluded.reason_code;

  get diagnostics v_email_logs_upserted = row_count;

  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'timesheet_id', te.timesheet_id::text,
        'issue_fingerprint', te.issue_fingerprint,
        'client_id', case when te.client_id is null then null else te.client_id::text end,
        'recipient_email', te.recipient_email,
        'recipient_missing', (te.recipient_email is null or length(btrim(te.recipient_email)) = 0),
        'email_kind', case when te.emailed_already then 'REEMAIL' else 'EMAIL' end,
        'reason_code', te.reason_code,
        'hr_row_id', case when te.hr_row_id is null then null else te.hr_row_id::text end,
        'work_date', case when te.work_date is null then null else te.work_date::text end
      )
      order by te.timesheet_id::text
    ),
    '[]'::jsonb
  )
  into v_email_jobs
  from tmp_email_enriched te;

  -- 6) Mark import applied (inside transaction)
  update public.hr_imports hi2
     set import_scope = 'HR_DAILY',
         applied_at = v_now
   where hi2.id = p_import_id;

  return jsonb_build_object(
    'import_id', p_import_id,
    'source_system', v_src::text,
    'validations_upserted', v_validations_upserted,
    'timesheets_reference_updated', v_timesheets_ref_updated,
    'email_actions_received', v_email_selected_count,
    'email_logs_upserted', v_email_logs_upserted,
    'email_jobs', v_email_jobs
  );
end;
$$;
create or replace function public._imp_debug_audit(
  p_actor_user_id uuid,
  p_action text,
  p_after_json jsonb,
  p_entity text,
  p_subject_id text,
  p_before_json jsonb default null,
  p_ip text default null,
  p_user_agent text default null,
  p_correlation_id text default null
)
returns void
language plpgsql
security definer
set search_path = public
as $$
declare
  v_invoice_debug boolean := false;
begin
  -- Load invoice_debug flag (safe even if column not yet present)
  begin
    select coalesce(sd.invoice_debug, false)
      into v_invoice_debug
    from public.settings_defaults sd
    where sd.id = 1
    limit 1;
  exception when undefined_column then
    v_invoice_debug := false;
  when others then
    v_invoice_debug := false;
  end;

  if not v_invoice_debug then
    return;
  end if;

  -- Never allow audit failures to break the caller
  begin
    perform public._inv_write_audit(
      p_actor_user_id,
      p_action,
      p_after_json,
      p_entity,
      p_subject_id,
      p_before_json,
      'INVOICE_DEBUG',
      p_ip,
      p_user_agent,
      p_correlation_id
    );
  exception when others then
    null;
  end;
end;
$$;



create or replace function public.weekly_import_create_cancellation_corrections(
  p_shift_id uuid,
  p_import_id uuid,
  p_actor_user_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now timestamptz := now();

  -- Shift fields
  v_shift_source_system public.hr_source_enum;
  v_shift_candidate_id uuid;
  v_shift_client_id uuid;
  v_shift_contract_id uuid;
  v_shift_timesheet_id uuid;
  v_shift_work_date date;

  v_shift_start_utc timestamptz;
  v_shift_end_utc timestamptz;
  v_shift_break_mins int;
  v_shift_pay_minutes int;

  v_shift_ref_num text;
  v_shift_hr_request_id text;
  v_shift_external_row_key text;
  v_shift_week_ending_date date;

  -- Week ending resolution
  v_week_ending_date date;
  v_base_ts_week_ending date;
  v_contract_week_ending_weekday_snapshot int := 0;

  -- Correction identity
  v_kind text := 'CANCEL_SHIFT_REVERSAL'; -- contains REVERSAL
  v_correction_id text;

  -- fnv1a32 helper vars (deterministic correction_id)
  v_fnv_h bigint;
  v_fnv_i int;
  v_fnv_s text;
  v_fnv_hex text;

  -- Display / norms
  v_contract_display_site text;
  v_contract_ward_hint text;
  v_contract_role text;

  v_client_name text;
  v_candidate_display_name text;
  v_candidate_tms_ref text;

  v_shift_label text;
  v_shift_label_norm text;

  v_booking_base text;
  v_booking_id text;
  v_hash_hex text;

  v_schedule jsonb;
  v_hint jsonb;

  -- Contract weeks + timesheet creation
  v_base_week_id uuid;
  v_existing_ts_id uuid;
  v_existing_cw_id uuid;

  v_cw_id uuid;
  v_next_additional_seq int;
  v_try int;

  v_ts_id uuid;

  v_created_timesheet_ids uuid[] := array[]::uuid[];

  v_sqlstate text;
  v_err text;
begin
  -- Load shift and lock it (serializes concurrent correction creation for same shift)
  select
    ns.source_system,
    ns.candidate_id,
    ns.client_id,
    ns.contract_id,
    ns.timesheet_id,
    ns.work_date,
    ns.start_utc,
    ns.end_utc,
    ns.break_mins,
    ns.pay_minutes,
    ns.ref_num,
    ns.hr_request_id,
    ns.external_row_key,
    ns.week_ending_date
  into
    v_shift_source_system,
    v_shift_candidate_id,
    v_shift_client_id,
    v_shift_contract_id,
    v_shift_timesheet_id,
    v_shift_work_date,
    v_shift_start_utc,
    v_shift_end_utc,
    v_shift_break_mins,
    v_shift_pay_minutes,
    v_shift_ref_num,
    v_shift_hr_request_id,
    v_shift_external_row_key,
    v_shift_week_ending_date
  from public.nhsp_shifts ns
  where ns.id = p_shift_id
  for update;

  if not found then
    raise exception 'weekly_import_create_cancellation_corrections: shift_not_found (shift_id=%)', p_shift_id;
  end if;

  if v_shift_contract_id is null or v_shift_candidate_id is null or v_shift_client_id is null then
    raise exception 'weekly_import_create_cancellation_corrections: shift missing contract_id/candidate_id/client_id (shift_id=%)', p_shift_id;
  end if;

  if v_shift_work_date is null then
    raise exception 'weekly_import_create_cancellation_corrections: shift missing work_date (shift_id=%)', p_shift_id;
  end if;

  if v_shift_start_utc is null or v_shift_end_utc is null then
    raise exception 'weekly_import_create_cancellation_corrections: shift missing start_utc/end_utc (shift_id=%)', p_shift_id;
  end if;

  v_shift_break_mins := greatest(0, coalesce(v_shift_break_mins, 0));
  v_shift_pay_minutes := greatest(0, coalesce(v_shift_pay_minutes, 0));

  -- Resolve week_ending_date for the correction timesheet (never assume Sunday)
  v_week_ending_date := null;
  v_base_ts_week_ending := null;

  if v_shift_timesheet_id is not null then
    select t.week_ending_date
    into v_base_ts_week_ending
    from public.timesheets t
    where t.timesheet_id = v_shift_timesheet_id
    limit 1;

    if v_base_ts_week_ending is not null then
      v_week_ending_date := v_base_ts_week_ending;
    end if;
  end if;

  if v_week_ending_date is null and v_shift_week_ending_date is not null then
    v_week_ending_date := v_shift_week_ending_date;
  end if;

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
    raise exception 'weekly_import_create_cancellation_corrections: failed to resolve week_ending_date (shift_id=% contract_id=% work_date=%)',
      p_shift_id, v_shift_contract_id, v_shift_work_date;
  end if;

  -- Deterministic correction_id (fnv1a32 over stable string)
  v_fnv_s :=
    coalesce(p_import_id::text,'') || '|' ||
    coalesce(p_shift_id::text,'') || '|' ||
    coalesce(coalesce(v_shift_external_row_key,''),'') || '|' ||
    coalesce(coalesce(v_shift_ref_num,''),'') || '|' ||
    coalesce(coalesce(v_shift_hr_request_id,''),'') || '|' ||
    coalesce(v_shift_start_utc::text,'') || '|' ||
    coalesce(v_shift_end_utc::text,'') || '|' ||
    coalesce(v_shift_break_mins::text,'') || '|' ||
    coalesce(v_week_ending_date::text,'');

  v_fnv_h := 2166136261;
  for v_fnv_i in 1..char_length(v_fnv_s) loop
    v_fnv_h := (v_fnv_h # ascii(substring(v_fnv_s from v_fnv_i for 1)));
    v_fnv_h := (v_fnv_h * 16777619) % 4294967296;
  end loop;
  v_fnv_hex := lpad(lower(to_hex(v_fnv_h)), 8, '0');

  v_correction_id := 'can:' || p_import_id::text || ':' || p_shift_id::text || ':' || v_fnv_hex;

  -- Load display context for norms (best-effort)
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

  -- Build schedule + hint (no “zero timesheet”; reversal only)
  v_schedule := jsonb_build_array(
    jsonb_build_object(
      'date', v_shift_work_date::text,
      'ward', nullif(btrim(coalesce(v_shift_ward, v_contract_ward_hint, '')), ''),
      'start_utc', v_shift_start_utc::text,
      'end_utc', v_shift_end_utc::text,
      'break_mins', v_shift_break_mins,
      'pay_minutes', v_shift_pay_minutes,
      'ref_num', nullif(btrim(coalesce(v_shift_ref_num,'')), ''),
      'hr_request_id', nullif(btrim(coalesce(v_shift_hr_request_id,'')), ''),
      'shift_id', p_shift_id::text,
      'external_row_key', v_shift_external_row_key
    )
  );

  v_hint := jsonb_build_object(
    'import_cancellation', jsonb_build_object(
      'import_id', p_import_id::text,
      'shift_id', p_shift_id::text,
      'ref_num', nullif(btrim(coalesce(v_shift_ref_num,'')), ''),
      'hr_request_id', nullif(btrim(coalesce(v_shift_hr_request_id,'')), ''),
      'external_row_key', v_shift_external_row_key,
      'correction_id', v_correction_id,
      'correction_kind', v_kind,
      'adjustment_origin', 'IMPORT_CANCELLATION'
    )
  );

  v_shift_label := 'weekly-cancel-reversal-' || v_correction_id;

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


  -- Ensure base contract_week exists (seq=0) without altering defaults
  insert into public.contract_weeks(contract_id, week_ending_date, additional_seq)
  values (v_shift_contract_id, v_week_ending_date, 0)
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
    raise exception 'weekly_import_create_cancellation_corrections: failed to ensure base contract_week exists (contract_id=% week_ending=%).',
      v_shift_contract_id, v_week_ending_date;
  end if;

  -- Idempotency: reuse existing correction timesheet (uq_timesheets_correction_id_kind)
  v_existing_ts_id := null;

  select t.timesheet_id
  into v_existing_ts_id
  from public.timesheets t
  where t.correction_id = v_correction_id
    and t.correction_kind = v_kind
  order by t.is_current desc, t.version desc
  limit 1
  for update;

  if v_existing_ts_id is not null then
    -- Ensure an adjustment contract_week exists and points at this timesheet
    v_existing_cw_id := null;

    select cw.id
    into v_existing_cw_id
    from public.contract_weeks cw
    where cw.timesheet_id = v_existing_ts_id
      and cw.contract_id = v_shift_contract_id
      and cw.week_ending_date = v_week_ending_date
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
          raise exception 'weekly_import_create_cancellation_corrections: failed to allocate additional_seq after retries (contract_id=% week_ending=%).',
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

    -- Refresh correction timesheet fields (keeps idempotent identity)
    update public.timesheets t2
    set
      booking_id = v_booking_id,
      is_current = true,
      status = 'RECEIVED'::public.timesheet_status_enum,
      sheet_scope = 'WEEKLY'::public.timesheet_scope_enum,
      submission_mode = 'MANUAL'::public.submission_mode_enum,
      line_type = 'HOURS'::public.timesheet_line_type_enum,
      week_ending_date = v_week_ending_date,
      contract_id = v_shift_contract_id,
      occupant_key_norm = lower(coalesce(v_candidate_tms_ref, v_candidate_display_name, v_shift_candidate_id::text)),
      hospital_norm = lower(coalesce(v_contract_display_site, v_client_name, v_shift_client_id::text)),
      ward_norm = lower(coalesce(v_contract_ward_hint,'contract')),
      job_title_norm = lower(coalesce(v_contract_role,'weekly')),
      shift_label_norm = v_shift_label_norm,
      manual_pdf_r2_key = null,
      actual_schedule_json = v_schedule,
      additional_units_week = '{}'::jsonb,
      additional_units_per_day = '{}'::jsonb,
      day_references_json = null,
      qr_payload_json = v_hint,
      candidate_hint_text = v_hint,
      is_adjustment = true,
      parent_timesheet_id = null,
      correction_id = v_correction_id,
      correction_kind = v_kind,
      adjustment_origin = 'IMPORT_CANCELLATION',
      updated_at = v_now
    where t2.timesheet_id = v_existing_ts_id;

    v_created_timesheet_ids := array_append(v_created_timesheet_ids, v_existing_ts_id);

  else
    -- Create new adjustment contract_week (safe additional_seq) then a new correction timesheet linked to it.
    perform 1
    from public.contract_weeks cwlock2
    where cwlock2.contract_id = v_shift_contract_id
      and cwlock2.week_ending_date = v_week_ending_date
    for update;

    v_try := 0;
    loop
      v_try := v_try + 1;
      if v_try > 10 then
        raise exception 'weekly_import_create_cancellation_corrections: failed to allocate additional_seq after retries (contract_id=% week_ending=%).',
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
      lower(coalesce(v_candidate_tms_ref, v_candidate_display_name, v_shift_candidate_id::text)),
      lower(coalesce(v_contract_display_site, v_client_name, v_shift_client_id::text)),
      lower(coalesce(v_contract_ward_hint,'contract')),
      lower(coalesce(v_contract_role,'weekly')),
      v_shift_label_norm,
      v_week_ending_date,
      v_shift_contract_id,
      'WEEKLY'::public.timesheet_scope_enum,
      'MANUAL'::public.submission_mode_enum,
      'HOURS'::public.timesheet_line_type_enum,
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
      v_hint,
      v_now,
      v_now,
      true,
      null,
      v_hint,
      v_correction_id,
      v_kind,
      'IMPORT_CANCELLATION'
    )
    returning timesheet_id into v_ts_id;

    update public.contract_weeks cwlink
    set
      timesheet_id = v_ts_id,
      status = 'SUBMITTED'::public.contract_week_status_enum,
      submission_mode_snapshot = 'MANUAL'::public.submission_mode_enum,
      is_adjustment = true,
      updated_at = v_now
    where cwlink.id = v_cw_id;

    v_created_timesheet_ids := array_append(v_created_timesheet_ids, v_ts_id);
  end if;

  -- Deduplicate created ids
  select coalesce(array_agg(distinct x), array[]::uuid[])
  into v_created_timesheet_ids
  from unnest(v_created_timesheet_ids) x
  where x is not null;

  perform public._imp_debug_audit(
    p_actor_user_id,
    'WEEKLY_CANCEL_CORRECTION_CREATE_DEBUG',
    jsonb_build_object(
      'import_id', p_import_id::text,
      'shift_id', p_shift_id::text,
      'correction_id', v_correction_id,
      'correction_kind', v_kind,
      'week_ending_date', v_week_ending_date::text,
      'created_timesheet_ids', to_jsonb(v_created_timesheet_ids)
    ),
    'nhsp_shifts',
    p_shift_id::text,
    null,
    null,
    null,
    null
  );

  return jsonb_build_object(
    'import_id', p_import_id,
    'shift_id', p_shift_id,
    'correction_id', v_correction_id,
    'correction_kind', v_kind,
    'created_timesheet_ids', to_jsonb(v_created_timesheet_ids)
  );

exception when others then
  get stacked diagnostics v_sqlstate = returned_sqlstate, v_err = message_text;

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'WEEKLY_CANCEL_CORRECTION_CREATE_ERROR',
      jsonb_build_object(
        'import_id', p_import_id::text,
        'shift_id', p_shift_id::text,
        'sqlstate', v_sqlstate,
        'error', v_err
      ),
      'nhsp_shifts',
      p_shift_id::text,
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
