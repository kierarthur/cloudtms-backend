-- Immutable CloudTMS TEST function snapshot, page 06.
-- Generated from pg_get_functiondef; definitions only, with function body checks deferred for forward references.
-- Do not edit an applied baseline page. Add or replace routine authority in supabase/repeatable.

\set ON_ERROR_STOP on
set check_function_bodies = off;
set search_path = pg_catalog, public, extensions;

-- hr_weekly_mirror_upsert_deterministic(uuid,text[],uuid)
CREATE OR REPLACE FUNCTION public.hr_weekly_mirror_upsert_deterministic(p_import_id uuid, p_external_row_keys text[], p_actor_user_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_now timestamptz := now();
  v_src public.hr_source_enum;

  v_requested_keys text[] := '{}';
  v_det_keys text[] := '{}';
  v_collision_keys text[] := '{}';
  v_eligible_keys text[] := '{}';

  v_inserted_count int := 0;
  v_updated_count int := 0;

  -- ✅ NEW: time-match “rekey” counts + locked skips
  v_rekeyed_count int := 0;
  v_locked_skip_conflict_count int := 0;
  v_locked_skip_rekey_count int := 0;
  v_nochange_skip_count int := 0;
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
      'rekeyed_count', 0,
      'locked_skip_conflict_count', 0,
      'locked_skip_rekey_count', 0,
      'nochange_skip_count', 0,
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

  -- Build upsert source rows (HR weekly truth rows)
  -- ✅ Changes here:
  --   - break_mins uses actual_break_* first (then break_*)
  --   - ref_num uses hr_request_id (import reference wins)
  --   - ward uses unit_raw / payload ward
  --   - timesheet_id is NULL on insert source (but we will preserve existing on update)
  if array_length(v_eligible_keys, 1) is not null then
    create temporary table tmp_upsert_src on commit drop as
    select
      d.external_row_key,
      p_import_id as latest_import_id,
      d.candidate_id,
      d.client_id,
      d.contract_id,
      null::uuid as timesheet_id,
      d.work_date,
      coalesce(r.unit_raw, nullif(btrim(r.payload_json->>'ward'), ''), null) as ward,
      d.start_utc,
      d.end_utc,

      greatest(
        0,
        coalesce(
          case
            when nullif(btrim(coalesce(r.payload_json->>'actual_break_mins','')), '') ~ '^[0-9]+$'
              then (r.payload_json->>'actual_break_mins')::int
            else null
          end,
          case
            when nullif(btrim(coalesce(r.payload_json->>'actual_break_minutes','')), '') ~ '^[0-9]+$'
              then (r.payload_json->>'actual_break_minutes')::int
            else null
          end,
          case
            when nullif(btrim(coalesce(r.payload_json->>'break_mins','')), '') ~ '^[0-9]+$'
              then (r.payload_json->>'break_mins')::int
            else null
          end,
          case
            when nullif(btrim(coalesce(r.payload_json->>'break_minutes','')), '') ~ '^[0-9]+$'
              then (r.payload_json->>'break_minutes')::int
            else null
          end,
          0
        )
      ) as break_mins,

      greatest(
        0,
        (floor(extract(epoch from (d.end_utc - d.start_utc)) / 60.0))::int
        -
        greatest(
          0,
          coalesce(
            case
              when nullif(btrim(coalesce(r.payload_json->>'actual_break_mins','')), '') ~ '^[0-9]+$'
                then (r.payload_json->>'actual_break_mins')::int
              else null
            end,
            case
              when nullif(btrim(coalesce(r.payload_json->>'actual_break_minutes','')), '') ~ '^[0-9]+$'
                then (r.payload_json->>'actual_break_minutes')::int
              else null
            end,
            case
              when nullif(btrim(coalesce(r.payload_json->>'break_mins','')), '') ~ '^[0-9]+$'
                then (r.payload_json->>'break_mins')::int
              else null
            end,
            case
              when nullif(btrim(coalesce(r.payload_json->>'break_minutes','')), '') ~ '^[0-9]+$'
                then (r.payload_json->>'break_minutes')::int
              else null
            end,
            0
          )
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

      -- ✅ import reference wins
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
  end if;

  -- ------------------------------------------------------------
  -- ✅ NEW: If Request Id changes, external_row_key changes.
  -- Prevent duplicates by matching existing HEALTHROSTER shifts by:
  --   (candidate_id, client_id, work_date, start_utc minute, end_utc minute)
  -- and “rekeying” the existing row to the new external_row_key,
  -- BUT NEVER if invoice-locked/invoiced.
  -- ------------------------------------------------------------
  if array_length(v_eligible_keys, 1) is not null then
    create temporary table tmp_time_match on commit drop as
    select
      src.external_row_key as incoming_external_row_key,
      src.candidate_id as candidate_id,
      src.client_id as client_id,
      src.work_date as work_date,
      date_trunc('minute', src.start_utc) as inc_start_utc_min,
      date_trunc('minute', src.end_utc) as inc_end_utc_min,

      s.id as existing_shift_id,
      s.external_row_key as existing_external_row_key,
      s.source_system as existing_source_system,
      s.timesheet_id as existing_timesheet_id,
      s.invoice_id as existing_invoice_id,

      -- invoice-locked check:
      (
        s.invoice_id is not null
        or exists (
          select 1
          from public.timesheets_financials tf
          cross join lateral jsonb_array_elements(coalesce(tf.invoice_breakdown_json->'segments','[]'::jsonb)) as seg(value)
          where tf.is_current = true
            and tf.timesheet_id = s.timesheet_id
            and nullif(btrim(seg.value->>'nhsp_shift_id'), '') = s.id::text
            and nullif(btrim(seg.value->>'invoice_locked_invoice_id'), '') is not null
          limit 1
        )
      ) as existing_is_locked,

      -- does the incoming external_row_key already exist?
      exists (
        select 1
        from public.nhsp_shifts s2
        where s2.external_row_key = src.external_row_key
        limit 1
      ) as incoming_key_exists

    from tmp_upsert_src src
    join public.nhsp_shifts s
      on s.source_system = 'HEALTHROSTER'::public.hr_source_enum
     and s.cancelled_at_utc is null
     and s.candidate_id = src.candidate_id
     and s.client_id = src.client_id
     and s.work_date = src.work_date
     and date_trunc('minute', s.start_utc) = date_trunc('minute', src.start_utc)
     and date_trunc('minute', s.end_utc) = date_trunc('minute', src.end_utc)
    where src.external_row_key is not null
      and src.candidate_id is not null
      and src.client_id is not null
      and src.work_date is not null
      and src.start_utc is not null
      and src.end_utc is not null
      and s.external_row_key is not null
      and s.external_row_key <> src.external_row_key;

    -- locked skips for rekey
    select count(*)::int
    into v_locked_skip_rekey_count
    from tmp_time_match tm
    where tm.existing_is_locked is true;

    -- apply rekeys where safe (not locked, no existing incoming key, and existing is HEALTHROSTER)
    with rekey_rows as (
      select distinct on (tm.incoming_external_row_key)
        tm.incoming_external_row_key,
        tm.existing_shift_id
      from tmp_time_match tm
      where tm.existing_is_locked is false
        and tm.incoming_key_exists is false
        and tm.existing_source_system = 'HEALTHROSTER'::public.hr_source_enum
      order by tm.incoming_external_row_key, tm.existing_shift_id
    ),
    rekey_upd as (
      update public.nhsp_shifts s3
         set external_row_key = src3.external_row_key,
             latest_import_id = p_import_id,
             candidate_id = src3.candidate_id,
             client_id = src3.client_id,
             contract_id = src3.contract_id,
             work_date = src3.work_date,
             ward = src3.ward,
             start_utc = src3.start_utc,
             end_utc = src3.end_utc,
             break_mins = src3.break_mins,
             pay_minutes = src3.pay_minutes,
             hr_request_id = src3.hr_request_id,
             ref_num = src3.ref_num,
             staff_name = src3.staff_name,
             staff_norm = src3.staff_norm,
             ward_norm = src3.ward_norm,
             assignment_code = src3.assignment_code,
             week_ending_date = src3.week_ending_date,
             updated_at = v_now
        from rekey_rows rk
        join tmp_upsert_src src3
          on src3.external_row_key = rk.incoming_external_row_key
       where s3.id = rk.existing_shift_id
         and s3.source_system = 'HEALTHROSTER'::public.hr_source_enum
         and s3.cancelled_at_utc is null
      returning s3.id
    )
    select count(*)::int
    into v_rekeyed_count
    from rekey_upd;

  end if;

  -- ------------------------------------------------------------
  -- ✅ NEW: skip updates when the existing row (by external_row_key) is invoiced/locked
  -- ------------------------------------------------------------
  create temporary table tmp_locked_conflicts(
    external_row_key text primary key
  ) on commit drop;

  insert into tmp_locked_conflicts(external_row_key)
  select distinct src.external_row_key
  from tmp_upsert_src src
  join public.nhsp_shifts s
    on s.external_row_key = src.external_row_key
   and s.source_system = 'HEALTHROSTER'::public.hr_source_enum
  where
    (
      s.invoice_id is not null
      or exists (
        select 1
        from public.timesheets_financials tf
        cross join lateral jsonb_array_elements(coalesce(tf.invoice_breakdown_json->'segments','[]'::jsonb)) as seg(value)
        where tf.is_current = true
          and tf.timesheet_id = s.timesheet_id
          and nullif(btrim(seg.value->>'nhsp_shift_id'), '') = s.id::text
          and nullif(btrim(seg.value->>'invoice_locked_invoice_id'), '') is not null
        limit 1
      )
    )
  on conflict do nothing;

  select count(*)::int
  into v_locked_skip_conflict_count
  from tmp_locked_conflicts;

  -- Filter final upsert source: exclude locked-conflict keys
  create temporary table tmp_upsert_final on commit drop as
  select src.*
  from tmp_upsert_src src
  left join tmp_locked_conflicts lc
    on lc.external_row_key = src.external_row_key
  where lc.external_row_key is null;

  -- Upsert mirror rows
  -- ✅ Changes:
  --   - preserve existing timesheet_id (do NOT clear)
  --   - do NOT write if nothing materially changed (nochange skip)
  if exists (select 1 from tmp_upsert_final) then
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
      from tmp_upsert_final as s
      on conflict (external_row_key) do update
        set
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

          -- ✅ CRITICAL: never clear linkages that were created elsewhere
          timesheet_id = public.nhsp_shifts.timesheet_id,

          -- only touch latest_import_id / updated_at when something actually changed (below)
          latest_import_id = public.nhsp_shifts.latest_import_id,
          updated_at = public.nhsp_shifts.updated_at
      where
        (
          public.nhsp_shifts.candidate_id is distinct from excluded.candidate_id
          or public.nhsp_shifts.client_id is distinct from excluded.client_id
          or public.nhsp_shifts.contract_id is distinct from excluded.contract_id
          or public.nhsp_shifts.work_date is distinct from excluded.work_date
          or public.nhsp_shifts.ward is distinct from excluded.ward
          or date_trunc('minute', public.nhsp_shifts.start_utc) is distinct from date_trunc('minute', excluded.start_utc)
          or date_trunc('minute', public.nhsp_shifts.end_utc) is distinct from date_trunc('minute', excluded.end_utc)
          or coalesce(public.nhsp_shifts.break_mins,0) is distinct from coalesce(excluded.break_mins,0)
          or coalesce(public.nhsp_shifts.pay_minutes,0) is distinct from coalesce(excluded.pay_minutes,0)
          or public.nhsp_shifts.hr_request_id is distinct from excluded.hr_request_id
          or public.nhsp_shifts.ref_num is distinct from excluded.ref_num
          or public.nhsp_shifts.staff_name is distinct from excluded.staff_name
          or public.nhsp_shifts.staff_norm is distinct from excluded.staff_norm
          or public.nhsp_shifts.ward_norm is distinct from excluded.ward_norm
          or public.nhsp_shifts.assignment_code is distinct from excluded.assignment_code
          or public.nhsp_shifts.week_ending_date is distinct from excluded.week_ending_date
        )
      returning
        (xmax = 0) as inserted_flag,
        (xmax <> 0) as updated_flag
    ),
    counts as (
      select
        count(*) filter (where up.inserted_flag) as ins_n,
        count(*) filter (where up.updated_flag) as upd_n
      from up
    )
    select
      coalesce(counts.ins_n,0)::int,
      coalesce(counts.upd_n,0)::int
    into v_inserted_count, v_updated_count
    from counts;

    -- nochange skips = final_upsert rows - (inserted + updated)
    select greatest(0, (select count(*) from tmp_upsert_final) - coalesce(v_inserted_count,0) - coalesce(v_updated_count,0))::int
    into v_nochange_skip_count;
  end if;

  return jsonb_build_object(
    'import_id', p_import_id,
    'requested_count', coalesce(array_length(v_requested_keys, 1), 0),
    'deterministic_count', coalesce(array_length(v_det_keys, 1), 0),
    'eligible_count', coalesce(array_length(v_eligible_keys, 1), 0),

    'inserted_count', v_inserted_count,
    'updated_count', v_updated_count,

    'rekeyed_count', v_rekeyed_count,
    'locked_skip_conflict_count', v_locked_skip_conflict_count,
    'locked_skip_rekey_count', v_locked_skip_rekey_count,
    'nochange_skip_count', v_nochange_skip_count,

    'excluded', (
      select coalesce(
        jsonb_agg(jsonb_build_object('external_row_key', e.external_row_key, 'reason', e.reason) order by e.external_row_key),
        '[]'::jsonb
      )
      from (
        -- Requested but not deterministic
        select rk.external_row_key, 'NOT_DETERMINISTIC'::text as reason
        from tmp_req_keys as rk
        left join (select distinct d.external_row_key from tmp_det as d) as dk
          on dk.external_row_key = rk.external_row_key
        where dk.external_row_key is null

        union all

        -- Deterministic but collision with existing non-HEALTHROSTER shift
        select c.external_row_key, 'EXISTS_NON_HEALTHROSTER'::text as reason
        from tmp_collisions as c

        union all

        -- Locked conflict on same external_row_key
        select lc.external_row_key, 'LOCKED_INVOICED_EXISTING_KEY'::text as reason
        from tmp_locked_conflicts as lc

        union all

        -- Locked time-match that prevented rekey (report under incoming key)
        select tm2.incoming_external_row_key as external_row_key, 'LOCKED_INVOICED_TIME_MATCH'::text as reason
        from tmp_time_match tm2
        where tm2.existing_is_locked is true
      ) as e
    )
  );
end;
$function$;

-- hr_weekly_phase3_apply_adjustment_truth(uuid,text[],jsonb,uuid)
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

-- hr_weekly_phase3_apply_adjustment_truth(uuid,text[],uuid)
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
        and ns.source_system='HEALTHROSTER'::public.hr_source_enum
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

-- hr_weekly_preview_mappings_phase1(uuid)
CREATE OR REPLACE FUNCTION public.hr_weekly_preview_mappings_phase1(p_import_id uuid)
 RETURNS TABLE(hr_row_id uuid, staff_name text, staff_norm text, ward text, ward_norm text, work_date date, client_id uuid, client_name text, candidate_id uuid, candidate_name text)
 LANGUAGE plpgsql
AS $function$
begin
  return query
  with src as (
    select
      r.id          as hr_row_id,
      hi.client_id  as client_id,
      r.date_local  as work_date,

      coalesce(
        nullif((r.payload_json ->> 'staff_name'), ''),
        nullif(r.staff_raw, ''),
        nullif(r.staff_norm, '')
      ) as staff_name,

      coalesce(
        nullif((r.payload_json ->> 'ward'), ''),
        nullif(r.unit_hint, ''),
        nullif(r.unit_raw, '')
      ) as ward
    from public.hr_rows r
    join public.hr_imports hi
      on hi.id = r.import_id
    where r.import_id = p_import_id
      and hi.source_system = 'HEALTHROSTER'::hr_source_enum
      and r.date_local is not null
      and (r.payload_json ->> 'start_utc') is not null
      and (r.payload_json ->> 'end_utc')   is not null
  ),
  normed as (
    select
      s.hr_row_id,
      s.client_id,
      s.work_date,
      s.staff_name,

      -- keep existing behaviour for staff_norm output (lower+trim)
      nullif(lower(trim(coalesce(s.staff_name,''))), '') as staff_norm,

      -- symbol/space stripped variant for matching
      nullif(regexp_replace(lower(coalesce(s.staff_name,'')), '[^a-z0-9]+', '', 'g'), '') as staff_norm2,

      s.ward,
      nullif(lower(trim(coalesce(s.ward,''))), '')       as ward_norm
    from src s
  ),
  resolved as (
    select
      n.*,

      -- Candidate mapping precedence:
      --  1) candidates.nhsp_hr_name_aliases contains staff_norm OR staff_norm2
      --  2) fallback to hr_name_mappings.hr_name_norm = staff_norm OR staff_norm2
      --  3) UNIQUE exact candidate match on (first+last) OR (last+first) using staff_norm2
      coalesce(
        cand_alias.id,
        cand_map.candidate_id,
        cand_exact_unique.cid
      ) as candidate_id,

      coalesce(
        cand_alias.display_name,
        cand_map.display_name,
        cand_exact_unique.cname
      ) as candidate_name

    from normed n

    left join lateral (
      select c.id, c.display_name
      from public.candidates c
      where c.nhsp_hr_name_aliases is not null
        and (
          (n.staff_norm  is not null and c.nhsp_hr_name_aliases @> to_jsonb(array[n.staff_norm]::text[]))
          or
          (n.staff_norm2 is not null and c.nhsp_hr_name_aliases @> to_jsonb(array[n.staff_norm2]::text[]))
        )
      limit 1
    ) cand_alias on true

    left join lateral (
      select hm.candidate_id, c.display_name
      from public.hr_name_mappings hm
      join public.candidates c
        on c.id = hm.candidate_id
      where hm.active = true
        and (
          (n.staff_norm  is not null and hm.hr_name_norm = n.staff_norm)
          or
          (n.staff_norm2 is not null and hm.hr_name_norm = n.staff_norm2)
        )
      order by hm.created_at desc
      limit 1
    ) cand_map on cand_alias.id is null

    -- ✅ FIX: do NOT emit a column named candidate_id inside PL/pgSQL returns-table function
    left join lateral (
      with matches as (
        select
          c.id as cid,
          c.display_name as cname
        from public.candidates c
        where c.active = true
          and n.staff_norm2 is not null
          and (
            regexp_replace(lower(coalesce(c.first_name,'') || coalesce(c.last_name,'')), '[^a-z0-9]+', '', 'g') = n.staff_norm2
            or
            regexp_replace(lower(coalesce(c.last_name,'')  || coalesce(c.first_name,'')), '[^a-z0-9]+', '', 'g') = n.staff_norm2
          )
      )
      select
        case
          when count(*) = 1
            then (array_agg(cid order by cid::text))[1]
        end as cid,
        case
          when count(*) = 1
            then (array_agg(cname order by cid::text))[1]
        end as cname
      from matches
    ) cand_exact_unique on (cand_alias.id is null and cand_map.candidate_id is null)
  )
  select
    r.hr_row_id,
    r.staff_name,
    r.staff_norm,
    r.ward,
    r.ward_norm,
    r.work_date,
    r.client_id,
    cli.name      as client_name,
    r.candidate_id,
    r.candidate_name
  from resolved r
  left join public.clients cli
    on cli.id = r.client_id;

end;
$function$;

-- hr_weekly_validation_preview(uuid)
CREATE OR REPLACE FUNCTION public.hr_weekly_validation_preview(p_import_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_import record;
  v_client_id uuid;
  v_coverage_mode text := 'PARTIAL';

  -- client settings snapshot (latest)
  v_we_dow int := 0;
  v_cs_autoprocess_hr boolean := false;
  v_cs_requires_hr boolean := false;
  v_cs_no_timesheet_required boolean := false;

  v_recipient_email text;

  v_rows jsonb := '[]'::jsonb;
  v_unmapped_candidates int := 0;
  v_unmatched_timesheets int := 0;

  -- unauthorised timesheets (included in validation matches; counted for reporting)
  v_unauthorised_timesheet_triples int := 0;

  -- import file date range (drives missing-shifts warnings)
  v_file_date_min date := null;
  v_file_date_max date := null;
  v_we_min date := null;
  v_we_max date := null;
  v_result jsonb;
  v_review jsonb;
  v_source_row_count integer := 0;
begin
  if p_import_id is null then
    raise exception 'hr_weekly_validation_preview: import_id is required';
  end if;

  select
    hi.id,
    hi.source_system,
    hi.import_scope,
    hi.client_id,
    hi.coverage_mode
  into v_import
  from public.hr_imports hi
  where hi.id = p_import_id
  limit 1;

  if v_import.id is null then
    raise exception 'hr_weekly_validation_preview: import % not found', p_import_id;
  end if;

  if upper(coalesce(v_import.source_system::text,'')) <> 'HEALTHROSTER' then
    raise exception 'hr_weekly_validation_preview: import % is not HEALTHROSTER (source_system=%)', p_import_id, v_import.source_system;
  end if;

  -- allow running weekly validation before apply sets import_scope. only reject if import_scope is explicitly different.
  if v_import.import_scope is not null and upper(coalesce(v_import.import_scope::text,'')) <> 'HR_WEEKLY' then
    raise exception 'hr_weekly_validation_preview: import % is not HR_WEEKLY (import_scope=%)', p_import_id, v_import.import_scope;
  end if;

  v_client_id := v_import.client_id;
  v_coverage_mode := upper(coalesce(v_import.coverage_mode,'PARTIAL'));
  if v_client_id is null then
    raise exception 'hr_weekly_validation_preview: import % has no client_id', p_import_id;
  end if;

  select count(*) into v_source_row_count
  from (select 1 from public.hr_rows r where r.import_id=p_import_id limit 501) bounded_rows;
  if v_source_row_count>500 then
    raise exception 'IMPORT_REVIEW_ACTION_LIMIT_EXCEEDED' using errcode='54000',
      detail=jsonb_build_object('count_at_least',v_source_row_count,'max',500)::text;
  end if;

  -- Resolve client settings snapshot (latest): week-ending weekday + HR flags
  select
    coalesce(cs.week_ending_weekday, 0)::int,
    coalesce(cs.autoprocess_hr, false),
    coalesce(cs.requires_hr, false),
    coalesce(cs.no_timesheet_required, false)
  into
    v_we_dow,
    v_cs_autoprocess_hr,
    v_cs_requires_hr,
    v_cs_no_timesheet_required
  from public.client_settings cs
  where cs.client_id = v_client_id
  order by cs.effective_from desc nulls last, cs.created_at desc
  limit 1;

  -- Resolve recipient (for UI messaging / can_email)
  select nullif(btrim(coalesce(c.ts_queries_email,'')), '')
  into v_recipient_email
  from public.clients c
  where c.id = v_client_id
  limit 1;

  -- Import file date range (drives "missing shifts" warnings)
  select
    min(r2.date_local)::date,
    max(r2.date_local)::date
  into
    v_file_date_min,
    v_file_date_max
  from public.hr_rows r2
  where r2.import_id = p_import_id
    and r2.date_local is not null;

  if v_file_date_min is null or v_file_date_max is null then
    return jsonb_build_object(
      'import_id', p_import_id::text,
      'client_id', v_client_id::text,
      'week_ending_weekday', v_we_dow,
      'recipient_email', v_recipient_email,
      'file_date_min', null,
      'file_date_max', null,
      'unmapped_candidate_rows', 0,
      'unmatched_timesheet_triples', 0,
      'unauthorised_timesheet_triples', 0,
      'rows', '[]'::jsonb,
      'validation_groups', '[]'::jsonb
    );
  end if;

  -- derive inclusive week-ending bounds for selecting weekly timesheets in scope
  v_we_min :=
    (v_file_date_min
      + (((v_we_dow - extract(dow from v_file_date_min)::int + 7) % 7))::int
    )::date;

  v_we_max :=
    (v_file_date_max
      + (((v_we_dow - extract(dow from v_file_date_max)::int + 7) % 7))::int
    )::date;

  with
  -- ─────────────────────────────────────────────
  -- HR import rows in this file (for comparisons + candidate resolution)
  -- ─────────────────────────────────────────────
  hr_raw as (
    select
      r.id as hr_row_id,
      r.external_row_key,
      r.date_local as date_local,
      nullif(btrim(coalesce(r.payload_json->>'staff_name','')), '') as staff_name_payload,
      nullif(btrim(coalesce(r.staff_raw,'')), '') as staff_raw,
      nullif(btrim(coalesce(r.staff_norm,'')), '') as staff_norm_col,
      nullif(btrim(coalesce(r.hr_request_id,'')), '') as hr_request_id_text,
      nullif(btrim(coalesce(r.payload_json->>'request_id','')), '') as hr_request_id_payload,
      nullif(btrim(coalesce(r.payload_json->>'ward','')), '') as ward_payload,
      nullif(btrim(coalesce(r.payload_json->>'unit','')), '') as unit_payload,
      nullif(btrim(coalesce(r.unit_raw,'')), '') as unit_raw,
      nullif(btrim(coalesce(
        r.assignment_grade_norm,
        r.payload_json->>'grade_raw',
        r.payload_json->>'Request_Grade',
        ''
      )), '') as assignment_grade,
      (r.payload_json->>'start_utc')::timestamptz as start_utc_raw,
      (r.payload_json->>'end_utc')::timestamptz as end_utc_raw,
      coalesce(
        nullif(r.payload_json->>'actual_break_mins','')::int,
        nullif(r.payload_json->>'actual_break_minutes','')::int,
        nullif(r.payload_json->>'break_mins','')::int,
        nullif(r.payload_json->>'break_minutes','')::int,
        0
      ) as break_mins
    from public.hr_rows r
    where r.import_id = p_import_id
      and r.external_row_key is not null
      and r.date_local is not null
      and r.date_local between v_file_date_min and v_file_date_max
      and (r.payload_json->>'start_utc') is not null
      and (r.payload_json->>'end_utc') is not null
  ),
  hr_normed as (
    select
      h.hr_row_id,
      h.external_row_key,
      h.date_local as work_date,
      coalesce(h.staff_name_payload, h.staff_raw, h.staff_norm_col) as staff_name,
      nullif(lower(trim(coalesce(coalesce(h.staff_name_payload, h.staff_raw, h.staff_norm_col), ''))), '') as staff_norm,
      nullif(regexp_replace(lower(coalesce(coalesce(h.staff_name_payload, h.staff_raw, h.staff_norm_col), '')), '[^a-z0-9]+', '', 'g'), '') as staff_norm2,
      coalesce(nullif(h.hr_request_id_text,''), nullif(h.hr_request_id_payload,'')) as hr_request_id,
      coalesce(nullif(h.ward_payload,''), nullif(h.unit_payload,''), nullif(h.unit_raw,'')) as hr_location,
      h.assignment_grade,
      date_trunc('minute', h.start_utc_raw) as start_utc,
      date_trunc('minute', h.end_utc_raw) as end_utc,
      greatest(coalesce(h.break_mins, 0), 0)::int as break_mins
    from hr_raw h
  ),
  hr_resolved as (
    select
      n.*,
      coalesce(cand_alias.id, cand_map.candidate_id, cand_exact_unique.cid) as candidate_id,
      coalesce(cand_alias.display_name, cand_map.display_name, cand_exact_unique.cname) as candidate_name
    from hr_normed n
    left join lateral (
      select c.id, c.display_name
      from public.candidates c
      where c.nhsp_hr_name_aliases is not null
        and (
          (n.staff_norm  is not null and c.nhsp_hr_name_aliases @> to_jsonb(array[n.staff_norm]::text[]))
          or
          (n.staff_norm2 is not null and c.nhsp_hr_name_aliases @> to_jsonb(array[n.staff_norm2]::text[]))
        )
      limit 1
    ) cand_alias on true
    left join lateral (
      select hm.candidate_id, c.display_name
      from public.hr_name_mappings hm
      join public.candidates c
        on c.id = hm.candidate_id
      where hm.active = true
        and (
          (n.staff_norm  is not null and hm.hr_name_norm = n.staff_norm)
          or
          (n.staff_norm2 is not null and hm.hr_name_norm = n.staff_norm2)
        )
      order by hm.created_at desc
      limit 1
    ) cand_map on cand_alias.id is null
    left join lateral (
      with matches as (
        select c.id as cid, c.display_name as cname
        from public.candidates c
        where c.active = true
          and n.staff_norm2 is not null
          and (
            regexp_replace(lower(coalesce(c.first_name,'') || coalesce(c.last_name,'')), '[^a-z0-9]+', '', 'g') = n.staff_norm2
            or
            regexp_replace(lower(coalesce(c.last_name,'')  || coalesce(c.first_name,'')), '[^a-z0-9]+', '', 'g') = n.staff_norm2
          )
      )
      select
        case when count(*) = 1 then (array_agg(cid order by cid::text))[1] end as cid,
        case when count(*) = 1 then (array_agg(cname order by cid::text))[1] end as cname
      from matches
    ) cand_exact_unique on (cand_alias.id is null and cand_map.candidate_id is null)
  ),
  hr_with_we as (
    select
      r.candidate_id,
      r.candidate_name,
      r.work_date,
      (
        r.work_date
        + (((v_we_dow - extract(dow from r.work_date)::int + 7) % 7))::int
      )::date as week_ending_date,

      r.hr_row_id,
      r.hr_request_id,
      r.hr_location,
      r.assignment_grade,

      to_char((r.start_utc at time zone 'Europe/London'), 'HH24:MI') as hr_start_hhmm,
      to_char((r.end_utc at time zone 'Europe/London'), 'HH24:MI') as hr_end_hhmm,

      (substring(to_char((r.start_utc at time zone 'Europe/London'), 'HH24:MI'),1,2)::int * 60
        + substring(to_char((r.start_utc at time zone 'Europe/London'), 'HH24:MI'),4,2)::int
      ) as hr_start_min,

      (
        case
          when (
            (substring(to_char((r.end_utc at time zone 'Europe/London'), 'HH24:MI'),1,2)::int * 60
             + substring(to_char((r.end_utc at time zone 'Europe/London'), 'HH24:MI'),4,2)::int
            )
            <=
            (substring(to_char((r.start_utc at time zone 'Europe/London'), 'HH24:MI'),1,2)::int * 60
             + substring(to_char((r.start_utc at time zone 'Europe/London'), 'HH24:MI'),4,2)::int
            )
          )
          then
            (substring(to_char((r.end_utc at time zone 'Europe/London'), 'HH24:MI'),1,2)::int * 60
             + substring(to_char((r.end_utc at time zone 'Europe/London'), 'HH24:MI'),4,2)::int
            ) + 1440
          else
            (substring(to_char((r.end_utc at time zone 'Europe/London'), 'HH24:MI'),1,2)::int * 60
             + substring(to_char((r.end_utc at time zone 'Europe/London'), 'HH24:MI'),4,2)::int
            )
        end
      ) as hr_end_min,

      r.break_mins as hr_break_mins,

      greatest(
        0,
        (
          (
            case
              when (
                (substring(to_char((r.end_utc at time zone 'Europe/London'), 'HH24:MI'),1,2)::int * 60
                 + substring(to_char((r.end_utc at time zone 'Europe/London'), 'HH24:MI'),4,2)::int
                )
                <=
                (substring(to_char((r.start_utc at time zone 'Europe/London'), 'HH24:MI'),1,2)::int * 60
                 + substring(to_char((r.start_utc at time zone 'Europe/London'), 'HH24:MI'),4,2)::int
                )
              )
              then
                (substring(to_char((r.end_utc at time zone 'Europe/London'), 'HH24:MI'),1,2)::int * 60
                 + substring(to_char((r.end_utc at time zone 'Europe/London'), 'HH24:MI'),4,2)::int
                ) + 1440
              else
                (substring(to_char((r.end_utc at time zone 'Europe/London'), 'HH24:MI'),1,2)::int * 60
                 + substring(to_char((r.end_utc at time zone 'Europe/London'), 'HH24:MI'),4,2)::int
                )
            end
          )
          -
          (substring(to_char((r.start_utc at time zone 'Europe/London'), 'HH24:MI'),1,2)::int * 60
           + substring(to_char((r.start_utc at time zone 'Europe/London'), 'HH24:MI'),4,2)::int
          )
          - coalesce(r.break_mins,0)
        )::int
      ) as hr_paid_minutes

    from hr_resolved r
  ),

  unmapped_candidate_rows as (
    select count(*)::int as n
    from hr_with_we h
    where h.candidate_id is null
  ),

  hr_entries_flat as (
    select
      h.candidate_id,
      h.candidate_name,
      h.week_ending_date,
      h.work_date,
      h.hr_row_id,
      h.hr_request_id,
      h.hr_location,
      h.hr_start_hhmm,
      h.hr_end_hhmm,
      h.hr_start_min,
      h.hr_end_min,
      h.hr_break_mins,
      h.hr_paid_minutes,
      h.assignment_grade
    from hr_with_we h
    where h.candidate_id is not null
  ),

  hr_day_totals as (
    select
      h.candidate_id,
      h.candidate_name,
      h.week_ending_date,
      h.work_date,
      sum(h.hr_paid_minutes)::int as hr_paid_minutes
    from hr_with_we h
    where h.candidate_id is not null
    group by h.candidate_id, h.candidate_name, h.week_ending_date, h.work_date
  ),

  hr_triples as (
    select distinct
      h.candidate_id,
      h.candidate_name,
      h.week_ending_date
    from hr_with_we h
    where h.candidate_id is not null
  ),

  contract_effective as (
    select
      c2.id as contract_id,
      coalesce(
        case when coalesce(c2.overrideclientsettings,false) then c2.autoprocess_hr else v_cs_autoprocess_hr end,
        false
      ) as eff_autoprocess_hr,
      coalesce(
        case when coalesce(c2.overrideclientsettings,false) then c2.requires_hr else v_cs_requires_hr end,
        false
      ) as eff_requires_hr,
      coalesce(
        case when coalesce(c2.overrideclientsettings,false) then c2.no_timesheet_required else v_cs_no_timesheet_required end,
        false
      ) as eff_no_timesheet_required
    from public.contracts c2
    where c2.client_id = v_client_id
  ),

  ts_universe_raw as (
    select
      t.timesheet_id,
      t.week_ending_date,
      t.contract_id,
      ct.candidate_id,
      cand.display_name as candidate_name,
      t.actual_schedule_json,
      t.authorised_at_server,
      tfu.invoice_breakdown_json as tsfin_invoice_breakdown_json
    from public.timesheets t
    join public.contracts ct
      on ct.id = t.contract_id
    join contract_effective ce
      on ce.contract_id = ct.id
    left join public.candidates cand
      on cand.id = ct.candidate_id
    left join public.timesheets_financials tfu
      on tfu.timesheet_id = t.timesheet_id
     and tfu.is_current = true
    where t.is_current = true
      and t.revoked_at is null
      and t.archived_at_utc is null
      and t.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum
      and ct.client_id = v_client_id
      and t.week_ending_date is not null
      and t.week_ending_date between v_we_min and v_we_max
      and coalesce(ce.eff_autoprocess_hr,false) = true
      and coalesce(ce.eff_requires_hr,false) = true
      and coalesce(ce.eff_no_timesheet_required,false) = false
      and (
        (jsonb_typeof(t.actual_schedule_json)='array' and jsonb_array_length(t.actual_schedule_json)>0)
        or (
          jsonb_typeof(tfu.invoice_breakdown_json)='object'
          and jsonb_typeof(tfu.invoice_breakdown_json->'segments')='array'
          and jsonb_array_length(tfu.invoice_breakdown_json->'segments')>0
        )
      )
  ),

  ts_matches_raw as (
    select
      tr.candidate_id,
      tr.candidate_name,
      tr.week_ending_date,
      vf.timesheet_id as raw_timesheet_id
    from hr_triples tr
    left join public.v_timesheets_funnel vf
      on vf.kind = 'WEEK'
     and vf.client_id = v_client_id
     and vf.candidate_id = tr.candidate_id
     and vf.week_ending_date = tr.week_ending_date
     and vf.timesheet_id is not null
  ),
  ts_matches as (
    select
      tmr.candidate_id,
      tmr.candidate_name,
      tmr.week_ending_date,
      tmr.raw_timesheet_id as raw_timesheet_id,
      case
        when tmr.raw_timesheet_id is null then null::uuid
        when tur.timesheet_id is null then null::uuid
        when ce2.contract_id is null then null::uuid
        else tmr.raw_timesheet_id
      end as timesheet_id,
      case
        when tmr.raw_timesheet_id is null then false
        when tur.timesheet_id is null then false
        when ce2.contract_id is null then false
        when tts.authorised_at_server is null then true
        else false
      end as awaiting_authorisation,
      case
        when ce2.contract_id is null then null::uuid
        else tts.contract_id
      end as contract_id
    from ts_matches_raw tmr
    left join public.timesheets tts
      on tts.timesheet_id = tmr.raw_timesheet_id
     and tts.is_current = true
    left join ts_universe_raw tur
      on tur.timesheet_id=tmr.raw_timesheet_id
    left join contract_effective ce2
      on ce2.contract_id = tts.contract_id
     and coalesce(ce2.eff_autoprocess_hr,false) = true
     and coalesce(ce2.eff_requires_hr,false) = true
     and coalesce(ce2.eff_no_timesheet_required,false) = false
  ),

  ts_universe as (
    select
      tur.timesheet_id,
      tur.week_ending_date,
      tur.contract_id,
      tur.candidate_id,
      tur.candidate_name,
      tur.actual_schedule_json,
      tur.tsfin_invoice_breakdown_json
    from ts_universe_raw tur
  ),

  hr_exception_evidence as (
    select
      hf.*,
      tu.timesheet_id,
      public._import_review_hash_v1(concat_ws('|',
        'hr-weekly-candidate-did-not-work-v1',p_import_id,hf.hr_row_id,tu.timesheet_id,
        hf.candidate_id,hf.week_ending_date,hf.work_date,hf.hr_start_hhmm,hf.hr_end_hhmm,
        coalesce(hf.hr_break_mins,0),coalesce(hf.hr_request_id,''),coalesce(hf.hr_location,'')
      )) as exception_evidence_fingerprint
    from hr_entries_flat hf
    join ts_universe tu
      on tu.candidate_id=hf.candidate_id
     and tu.week_ending_date=hf.week_ending_date
  ),

  confirmed_hr_exceptions as (
    select he.*
    from hr_exception_evidence he
    join public.import_review_weekly_validation_resolutions r
      on r.import_id=p_import_id
     and r.hr_row_id=he.hr_row_id
     and r.timesheet_id=he.timesheet_id
     and r.resolution_code='CANDIDATE_DID_NOT_WORK'
     and r.status in ('CURRENT','APPLIED')
     and r.evidence_fingerprint=he.exception_evidence_fingerprint
  ),

  hr_day_totals_effective as (
    select
      he.candidate_id,
      he.candidate_name,
      he.week_ending_date,
      he.work_date,
      sum(he.hr_paid_minutes)::int as hr_paid_minutes
    from hr_exception_evidence he
    where not exists (
      select 1 from confirmed_hr_exceptions confirmed
      where confirmed.hr_row_id=he.hr_row_id
        and confirmed.timesheet_id=he.timesheet_id
    )
    group by he.candidate_id,he.candidate_name,he.week_ending_date,he.work_date
  ),

  confirmed_exceptions_by_group as (
    select
      candidate_id,candidate_name,week_ending_date,timesheet_id,
      count(*)::int as confirmed_exception_count,
      jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
        'hr_row_id',hr_row_id,
        'work_date',work_date::text,
        'healthroster_start',hr_start_hhmm,
        'healthroster_end',hr_end_hhmm,
        'healthroster_break_mins',hr_break_mins,
        'reference',hr_request_id,
        'location',hr_location,
        'resolution_code','CANDIDATE_DID_NOT_WORK',
        'evidence_fingerprint',exception_evidence_fingerprint
      )) order by work_date,hr_start_min,hr_end_min,hr_row_id) as confirmed_exceptions_json
    from confirmed_hr_exceptions
    group by candidate_id,candidate_name,week_ending_date,timesheet_id
  ),

  ts_entries_indexed as (
    select
      s.candidate_id,
      s.candidate_name,
      s.week_ending_date,
      s.timesheet_id,
      d.work_date,
      d.start_hhmm,
      d.end_hhmm,
      d.start_minute,
      d.end_minute,
      d.break_mins,
      row_number() over (partition by s.timesheet_id, d.work_date order by d.start_minute asc, d.end_minute asc) as worker_entry_index
    from ts_universe s
    cross join lateral (
      select
        outx.work_date as work_date,
        outx.start_hhmm as start_hhmm,
        outx.end_hhmm as end_hhmm,
        outx.start_minute as start_minute,
        outx.end_minute as end_minute,
        outx.break_mins as break_mins
      from (
        select
          nullif(btrim(coalesce((e.elem->>'date')::text, '')), '') as day_ymd,
          case
            when nullif(btrim(coalesce(e.elem->>'start','')), '') is not null then nullif(btrim(coalesce(e.elem->>'start','')), '')
            when nullif(btrim(coalesce(e.elem->>'start_utc','')), '') is not null then to_char(((e.elem->>'start_utc')::timestamptz at time zone 'Europe/London'), 'HH24:MI')
            else null
          end as start_hhmm,
          case
            when nullif(btrim(coalesce(e.elem->>'end','')), '') is not null then nullif(btrim(coalesce(e.elem->>'end','')), '')
            when nullif(btrim(coalesce(e.elem->>'end_utc','')), '') is not null then to_char(((e.elem->>'end_utc')::timestamptz at time zone 'Europe/London'), 'HH24:MI')
            else null
          end as end_hhmm,
          case
            when (e.elem ? 'break_minutes') and nullif(btrim(coalesce(e.elem->>'break_minutes','')), '') is not null
              then greatest(((e.elem->>'break_minutes')::int), 0)
            when (e.elem ? 'break_mins') and nullif(btrim(coalesce(e.elem->>'break_mins','')), '') is not null
              then greatest(((e.elem->>'break_mins')::int), 0)
            when jsonb_typeof(e.elem->'breaks') = 'array' then (
              select coalesce(sum(
                case
                  when (b->>'start') ~ '^[0-9]{2}:[0-9]{2}$' and (b->>'end') ~ '^[0-9]{2}:[0-9]{2}$' then
                    (
                      (case when substring(b->>'end',1,2)::int*60 + substring(b->>'end',4,2)::int
                                 <= substring(b->>'start',1,2)::int*60 + substring(b->>'start',4,2)::int
                            then (substring(b->>'end',1,2)::int*60 + substring(b->>'end',4,2)::int) + 1440
                            else (substring(b->>'end',1,2)::int*60 + substring(b->>'end',4,2)::int)
                       end)
                      -
                      (substring(b->>'start',1,2)::int*60 + substring(b->>'start',4,2)::int)
                    )
                  else 0
                end
              )::int, 0)
              from jsonb_array_elements(e.elem->'breaks') b
            )
            when nullif(btrim(coalesce(e.elem->>'break_start','')), '') is not null
              and nullif(btrim(coalesce(e.elem->>'break_end','')), '') is not null
              and (e.elem->>'break_start') ~ '^[0-9]{2}:[0-9]{2}$'
              and (e.elem->>'break_end') ~ '^[0-9]{2}:[0-9]{2}$'
              then
                greatest(
                  (
                    (case when substring(e.elem->>'break_end',1,2)::int*60 + substring(e.elem->>'break_end',4,2)::int
                               <= substring(e.elem->>'break_start',1,2)::int*60 + substring(e.elem->>'break_start',4,2)::int
                          then (substring(e.elem->>'break_end',1,2)::int*60 + substring(e.elem->>'break_end',4,2)::int) + 1440
                          else (substring(e.elem->>'break_end',1,2)::int*60 + substring(e.elem->>'break_end',4,2)::int)
                     end)
                    -
                    (substring(e.elem->>'break_start',1,2)::int*60 + substring(e.elem->>'break_start',4,2)::int)
                  )::int,
                  0
                )
            else 0
          end as break_mins
        from jsonb_array_elements(
          case
            when s.actual_schedule_json is not null
             and jsonb_typeof(s.actual_schedule_json) = 'array'
             and jsonb_array_length(s.actual_schedule_json) > 0
            then s.actual_schedule_json

            when s.tsfin_invoice_breakdown_json is not null
             and jsonb_typeof(s.tsfin_invoice_breakdown_json) = 'object'
             and upper(coalesce(s.tsfin_invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
             and jsonb_typeof(s.tsfin_invoice_breakdown_json->'segments') = 'array'
            then s.tsfin_invoice_breakdown_json->'segments'

            else '[]'::jsonb
          end
        ) as e(elem)
      ) base
      cross join lateral (
        select
          case when base.day_ymd ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' then (base.day_ymd)::date else null::date end as day_date,
          base.start_hhmm,
          base.end_hhmm,
          base.break_mins
      ) dd
      cross join lateral (
        select
          case when base.start_hhmm ~ '^[0-9]{2}:[0-9]{2}$'
            then (substring(base.start_hhmm,1,2)::int*60 + substring(base.start_hhmm,4,2)::int)
            else null::int
          end as start_minute_raw,
          case when base.end_hhmm ~ '^[0-9]{2}:[0-9]{2}$'
            then (substring(base.end_hhmm,1,2)::int*60 + substring(base.end_hhmm,4,2)::int)
            else null::int
          end as end_minute_raw
      ) mm
      cross join lateral (
        select
          dd.day_date as work_date,
          base.start_hhmm as start_hhmm,
          base.end_hhmm as end_hhmm,
          mm.start_minute_raw as start_minute,
          case
            when mm.start_minute_raw is null or mm.end_minute_raw is null then null::int
            when mm.end_minute_raw <= mm.start_minute_raw then mm.end_minute_raw + 1440
            else mm.end_minute_raw
          end as end_minute,
          greatest(coalesce(base.break_mins,0),0) as break_mins
      ) outx
      where outx.work_date is not null
        and outx.work_date between v_file_date_min and v_file_date_max
        and outx.start_minute is not null
        and outx.end_minute is not null
    ) d
  ),

  ts_day_totals as (
    select
      t.candidate_id,
      t.candidate_name,
      t.week_ending_date,
      t.timesheet_id,
      t.work_date,
      sum(greatest(0,(t.end_minute - t.start_minute - coalesce(t.break_mins,0))))::int as ts_paid_minutes
    from ts_entries_indexed t
    group by t.candidate_id, t.candidate_name, t.week_ending_date, t.timesheet_id, t.work_date
  ),

  seg_locks as (
    select
      tf.timesheet_id,
      (nullif(btrim(s.value->>'date'), ''))::date as work_date,
      (substring(to_char(((s.value->>'start_utc')::timestamptz at time zone 'Europe/London'),'HH24:MI'),1,2)::int*60
        + substring(to_char(((s.value->>'start_utc')::timestamptz at time zone 'Europe/London'),'HH24:MI'),4,2)::int
      ) as seg_start_min,
      (
        case
          when (
            (substring(to_char(((s.value->>'end_utc')::timestamptz at time zone 'Europe/London'),'HH24:MI'),1,2)::int*60
             + substring(to_char(((s.value->>'end_utc')::timestamptz at time zone 'Europe/London'),'HH24:MI'),4,2)::int
            )
            <=
            (substring(to_char(((s.value->>'start_utc')::timestamptz at time zone 'Europe/London'),'HH24:MI'),1,2)::int*60
             + substring(to_char(((s.value->>'start_utc')::timestamptz at time zone 'Europe/London'),'HH24:MI'),4,2)::int
            )
          )
          then
            (substring(to_char(((s.value->>'end_utc')::timestamptz at time zone 'Europe/London'),'HH24:MI'),1,2)::int*60
             + substring(to_char(((s.value->>'end_utc')::timestamptz at time zone 'Europe/London'),'HH24:MI'),4,2)::int
            ) + 1440
          else
            (substring(to_char(((s.value->>'end_utc')::timestamptz at time zone 'Europe/London'),'HH24:MI'),1,2)::int*60
             + substring(to_char(((s.value->>'end_utc')::timestamptz at time zone 'Europe/London'),'HH24:MI'),4,2)::int
            )
        end
      ) as seg_end_min,
      nullif(btrim(s.value->>'invoice_locked_invoice_id'), '') as invoice_locked_invoice_id,
      nullif(btrim(s.value->>'ref_num'), '') as seg_ref_num
    from public.timesheets_financials tf
    join ts_universe tu
      on tu.timesheet_id = tf.timesheet_id
    cross join lateral jsonb_array_elements(coalesce(tf.invoice_breakdown_json->'segments','[]'::jsonb)) as s(value)
    where tf.is_current = true
      and jsonb_typeof(tf.invoice_breakdown_json) = 'object'
      and jsonb_typeof(tf.invoice_breakdown_json->'segments') = 'array'
      and (s.value ? 'date')
      and (s.value ? 'start_utc')
      and (s.value ? 'end_utc')
      and (nullif(btrim(s.value->>'date'), '') is not null)
  ),

  pairing_counts as (
    select
      te.timesheet_id,
      te.candidate_id,
      te.candidate_name,
      te.week_ending_date,
      te.work_date,
      te.worker_entry_index,
      te.start_hhmm as ts_start_hhmm,
      te.end_hhmm as ts_end_hhmm,
      te.start_minute as ts_start_min,
      te.end_minute as ts_end_min,
      te.break_mins as ts_break_mins,

      count(hf.hr_row_id)::int as match_count,

      case when count(hf.hr_row_id) = 1 then (array_agg(hf.hr_row_id order by hf.hr_row_id::text))[1] end as matched_hr_row_id,
      case when count(hf.hr_row_id) = 1 then (array_agg(hf.hr_start_hhmm order by hf.hr_row_id::text))[1] end as matched_hr_start_hhmm,
      case when count(hf.hr_row_id) = 1 then (array_agg(hf.hr_end_hhmm order by hf.hr_row_id::text))[1] end as matched_hr_end_hhmm,
      case when count(hf.hr_row_id) = 1 then (array_agg(hf.hr_start_min order by hf.hr_row_id::text))[1] end as matched_hr_start_min,
      case when count(hf.hr_row_id) = 1 then (array_agg(hf.hr_end_min order by hf.hr_row_id::text))[1] end as matched_hr_end_min,
      case when count(hf.hr_row_id) = 1 then (array_agg(hf.hr_break_mins order by hf.hr_row_id::text))[1] end as matched_hr_break_mins,
      case when count(hf.hr_row_id) = 1 then (array_agg(hf.hr_request_id order by hf.hr_row_id::text))[1] end as matched_hr_request_id,
      case when count(hf.hr_row_id) = 1 then (array_agg(hf.hr_location order by hf.hr_row_id::text))[1] end as matched_hr_location
    from ts_entries_indexed te
    left join hr_entries_flat hf
      on hf.candidate_id = te.candidate_id
     and hf.week_ending_date = te.week_ending_date
     and hf.work_date = te.work_date
     and (least(te.end_minute, hf.hr_end_min) - greatest(te.start_minute, hf.hr_start_min)) >= 1
    group by
      te.timesheet_id, te.candidate_id, te.candidate_name, te.week_ending_date, te.work_date,
      te.worker_entry_index, te.start_hhmm, te.end_hhmm, te.start_minute, te.end_minute, te.break_mins
  ),

  comparisons_hr_only as (
    select
      tu.timesheet_id,
      tu.candidate_id,
      tu.candidate_name,
      tu.week_ending_date,
      hf.work_date,
      hf.hr_row_id,
      hf.exception_evidence_fingerprint,

      null::text as ts_start_hhmm,
      null::text as ts_end_hhmm,
      null::int as ts_start_min,
      null::int as ts_end_min,
      null::int as ts_break_mins,

      hf.hr_start_hhmm as hr_start_hhmm,
      hf.hr_end_hhmm as hr_end_hhmm,
      hf.hr_start_min as hr_start_min,
      hf.hr_end_min as hr_end_min,
      hf.hr_break_mins as hr_break_mins,
      hf.hr_request_id as hr_request_id,
      hf.hr_location as hr_location,

      false as time_match,
      'HR_ONLY'::text as match_status,

      100000 + hf.hr_start_min as sort_key
    from hr_exception_evidence hf
    join ts_universe tu
      on tu.candidate_id = hf.candidate_id
     and tu.week_ending_date = hf.week_ending_date
    left join pairing_counts pc
      on pc.timesheet_id = tu.timesheet_id
     and pc.work_date = hf.work_date
     and pc.match_count = 1
     and pc.matched_hr_row_id = hf.hr_row_id
    where pc.matched_hr_row_id is null
      and not exists (
        select 1 from confirmed_hr_exceptions confirmed
        where confirmed.hr_row_id=hf.hr_row_id
          and confirmed.timesheet_id=tu.timesheet_id
      )
  ),

  comparisons_worker as (
    select
      pc.timesheet_id,
      pc.candidate_id,
      pc.candidate_name,
      pc.week_ending_date,
      pc.work_date,
      case when pc.match_count=1 then pc.matched_hr_row_id else null end as hr_row_id,
      null::text as exception_evidence_fingerprint,

      pc.ts_start_hhmm,
      pc.ts_end_hhmm,
      pc.ts_start_min,
      pc.ts_end_min,
      pc.ts_break_mins,

      case when pc.match_count = 1 then pc.matched_hr_start_hhmm else null end as hr_start_hhmm,
      case when pc.match_count = 1 then pc.matched_hr_end_hhmm else null end as hr_end_hhmm,
      case when pc.match_count = 1 then pc.matched_hr_start_min else null end as hr_start_min,
      case when pc.match_count = 1 then pc.matched_hr_end_min else null end as hr_end_min,
      case when pc.match_count = 1 then pc.matched_hr_break_mins else null end as hr_break_mins,
      case when pc.match_count = 1 then pc.matched_hr_request_id else null end as hr_request_id,
      case when pc.match_count = 1 then pc.matched_hr_location else null end as hr_location,

      case
        when pc.match_count = 1
         and (pc.ts_start_min - pc.matched_hr_start_min) = 0
         and (pc.ts_end_min - pc.matched_hr_end_min) = 0
         and (coalesce(pc.ts_break_mins,0) - coalesce(pc.matched_hr_break_mins,0)) = 0
        then true
        else false
      end as time_match,

      case
        when pc.match_count = 1
         and (pc.ts_start_min - pc.matched_hr_start_min) = 0
         and (pc.ts_end_min - pc.matched_hr_end_min) = 0
         and (coalesce(pc.ts_break_mins,0) - coalesce(pc.matched_hr_break_mins,0)) = 0
        then 'MATCH'
        when pc.match_count = 1 then 'MISMATCH'
        when pc.match_count = 0 then 'UNMATCHED'
        else 'AMBIGUOUS'
      end as match_status,

      pc.worker_entry_index as sort_key
    from pairing_counts pc
    where v_coverage_mode<>'PARTIAL' or pc.match_count<>0
  ),

  comparisons_union as (
    select * from comparisons_worker
    union all
    select * from comparisons_hr_only
  ),

  comparisons_enriched as (
    select
      cu.timesheet_id,
      cu.candidate_id,
      cu.candidate_name,
      cu.week_ending_date,
      cu.work_date,
      cu.hr_row_id,
      cu.exception_evidence_fingerprint,

      cu.ts_start_hhmm,
      cu.ts_end_hhmm,
      cu.ts_start_min,
      cu.ts_end_min,
      cu.ts_break_mins,

      cu.hr_start_hhmm,
      cu.hr_end_hhmm,
      cu.hr_start_min,
      cu.hr_end_min,
      cu.hr_break_mins,
      cu.hr_request_id,
      cu.hr_location,

      cu.match_status,
      cu.time_match,
      cu.sort_key,

      sl.invoice_locked_invoice_id,
      sl.seg_ref_num,

      prev.prev_ref_num,
      prev.prev_location,
      prev.prev_start_hhmm,
      prev.prev_end_hhmm,
      prev.prev_break_mins,

      coalesce(nullif(btrim(sl.seg_ref_num), ''), nullif(btrim(prev.prev_ref_num), '')) as ref_before,
      nullif(btrim(cu.hr_request_id), '') as ref_after
    from comparisons_union cu
    left join seg_locks sl
      on sl.timesheet_id = cu.timesheet_id
     and sl.work_date = cu.work_date
     and sl.seg_start_min = cu.ts_start_min
     and sl.seg_end_min = cu.ts_end_min
    left join lateral (
      select
        ns.ref_num as prev_ref_num,
        ns.ward as prev_location,
        to_char((date_trunc('minute', ns.start_utc) at time zone 'Europe/London'), 'HH24:MI') as prev_start_hhmm,
        to_char((date_trunc('minute', ns.end_utc) at time zone 'Europe/London'), 'HH24:MI') as prev_end_hhmm,
        coalesce(ns.break_mins,0)::int as prev_break_mins
      from public.nhsp_shifts ns
      cross join lateral (
        select
          (substring(to_char((date_trunc('minute', ns.start_utc) at time zone 'Europe/London'),'HH24:MI'),1,2)::int * 60
            + substring(to_char((date_trunc('minute', ns.start_utc) at time zone 'Europe/London'),'HH24:MI'),4,2)::int
          ) as ns_start_min,
          (
            case
              when (
                (substring(to_char((date_trunc('minute', ns.end_utc) at time zone 'Europe/London'),'HH24:MI'),1,2)::int * 60
                 + substring(to_char((date_trunc('minute', ns.end_utc) at time zone 'Europe/London'),'HH24:MI'),4,2)::int
                )
                <=
                (substring(to_char((date_trunc('minute', ns.start_utc) at time zone 'Europe/London'),'HH24:MI'),1,2)::int * 60
                 + substring(to_char((date_trunc('minute', ns.start_utc) at time zone 'Europe/London'),'HH24:MI'),4,2)::int
                )
              )
              then
                (substring(to_char((date_trunc('minute', ns.end_utc) at time zone 'Europe/London'),'HH24:MI'),1,2)::int * 60
                 + substring(to_char((date_trunc('minute', ns.end_utc) at time zone 'Europe/London'),'HH24:MI'),4,2)::int
                ) + 1440
              else
                (substring(to_char((date_trunc('minute', ns.end_utc) at time zone 'Europe/London'),'HH24:MI'),1,2)::int * 60
                 + substring(to_char((date_trunc('minute', ns.end_utc) at time zone 'Europe/London'),'HH24:MI'),4,2)::int
                )
            end
          ) as ns_end_min
      ) nsm
      cross join lateral (
        select
          case when cu.hr_start_min is not null then cu.hr_start_min else cu.ts_start_min end as win_start_min,
          case when cu.hr_end_min is not null then cu.hr_end_min else cu.ts_end_min end as win_end_min
      ) win
      where ns.source_system = 'HEALTHROSTER'::public.hr_source_enum
        and ns.client_id = v_client_id
        and ns.cancelled_at_utc is null
        and ns.candidate_id = cu.candidate_id
        and ns.work_date = cu.work_date
        and win.win_start_min is not null
        and win.win_end_min is not null
        and (least(nsm.ns_end_min, win.win_end_min) - greatest(nsm.ns_start_min, win.win_start_min)) >= 1
      order by
        (case when (nsm.ns_start_min = win.win_start_min and nsm.ns_end_min = win.win_end_min) then 1 else 0 end) desc,
        (least(nsm.ns_end_min, win.win_end_min) - greatest(nsm.ns_start_min, win.win_start_min)) desc,
        ns.updated_at desc nulls last,
        ns.id desc
      limit 1
    ) prev on true
  ),

  comparisons_by_group as (
    select
      ce.candidate_id,
      ce.candidate_name,
      ce.week_ending_date,
      ce.timesheet_id,

      bool_or(ce.invoice_locked_invoice_id is not null) as any_invoice_locked,

      bool_or(
        (ce.invoice_locked_invoice_id is not null)
        and (coalesce(ce.ref_before,'') <> coalesce(ce.ref_after,''))
      ) as any_locked_ref_change,

      bool_or(
        (ce.invoice_locked_invoice_id is not null)
        and (ce.match_status <> 'MATCH')
      ) as any_locked_time_mismatch,

      jsonb_agg(
        jsonb_build_object(
          'hr_row_id', ce.hr_row_id,
          'exception_evidence_fingerprint',ce.exception_evidence_fingerprint,
          'work_date', ce.work_date::text,

          'timesheet_start', ce.ts_start_hhmm,
          'timesheet_end', ce.ts_end_hhmm,
          'timesheet_break_mins', ce.ts_break_mins,

          'healthroster_start', ce.hr_start_hhmm,
          'healthroster_end', ce.hr_end_hhmm,
          'healthroster_break_mins', ce.hr_break_mins,

          -- stable key for FE checkbox state
          'comparison_key',coalesce(
            case when ce.hr_row_id is not null then 'hr-row:'||ce.hr_row_id::text end,
            ce.work_date::text
              || '|' || coalesce(ce.ts_start_hhmm,'')
              || '|' || coalesce(ce.ts_end_hhmm,'')
              || '|' || coalesce(ce.ts_break_mins,0)::text
          ),

          -- destructive invalidation flags (missing from import OR mismatched + had prior ref + not invoice locked)
          'is_destructive_invalidation',
            (
              (ce.match_status in ('UNMATCHED','MISMATCH'))
              and (nullif(btrim(coalesce(ce.ref_before,'')), '') is not null)
              and (ce.invoice_locked_invoice_id is null)
            ),
          'default_invalidate_checked',
            (
              (ce.match_status in ('UNMATCHED','MISMATCH'))
              and (nullif(btrim(coalesce(ce.ref_before,'')), '') is not null)
              and (ce.invoice_locked_invoice_id is null)
            ),

          -- tick/cross for UI:
          -- time match, but if invoiced AND ref changed, treat as NOT match (cannot change invoiced ref)
          'match',
            (
              ce.time_match
              and not (
                ce.invoice_locked_invoice_id is not null
                and coalesce(ce.ref_before,'') <> coalesce(ce.ref_after,'')
              )
            ),
          'time_match', ce.time_match,
          'match_status', ce.match_status,

          'invoice_locked', (ce.invoice_locked_invoice_id is not null),
          'invoice_locked_invoice_id', ce.invoice_locked_invoice_id,

          -- before/after diffs
          'ref_before', nullif(btrim(ce.ref_before), ''),
          'ref_after', nullif(btrim(ce.ref_after), ''),
          'ref_changed',
            (
              nullif(btrim(ce.ref_before), '') is not null
              and nullif(btrim(ce.ref_after), '') is not null
              and btrim(ce.ref_before) <> btrim(ce.ref_after)
            ),

          'location_before', nullif(btrim(ce.prev_location), ''),
          'location_after', nullif(btrim(ce.hr_location), ''),

          'times_before',
            jsonb_build_object(
              'start', ce.prev_start_hhmm,
              'end', ce.prev_end_hhmm,
              'break_mins', ce.prev_break_mins
            ),

          'times_after',
            jsonb_build_object(
              'start', ce.hr_start_hhmm,
              'end', ce.hr_end_hhmm,
              'break_mins', ce.hr_break_mins
            )
        )
        order by ce.work_date asc, ce.sort_key asc
      ) as comparisons_json
    from comparisons_enriched ce
    group by ce.candidate_id, ce.candidate_name, ce.week_ending_date, ce.timesheet_id
  ),

  day_set as (
    select distinct
      te.timesheet_id,
      te.candidate_id,
      te.candidate_name,
      te.week_ending_date,
      te.work_date
    from ts_entries_indexed te

    union

    select distinct
      tu.timesheet_id,
      hf.candidate_id,
      hf.candidate_name,
      hf.week_ending_date,
      hf.work_date
    from hr_exception_evidence hf
    join ts_universe tu
      on tu.candidate_id = hf.candidate_id
     and tu.week_ending_date = hf.week_ending_date
    where not exists (
      select 1 from confirmed_hr_exceptions confirmed
      where confirmed.hr_row_id=hf.hr_row_id
        and confirmed.timesheet_id=tu.timesheet_id
    )
  ),

  day_eval as (
    select
      ds.timesheet_id,
      ds.candidate_id,
      ds.candidate_name,
      ds.week_ending_date,
      ds.work_date,
      hdt.hr_paid_minutes,
      tdt.ts_paid_minutes,
      (coalesce(hdt.hr_paid_minutes,0) - coalesce(tdt.ts_paid_minutes,0)) as delta_minutes,
      case
        when v_coverage_mode='PARTIAL' then 'OK'
        when (hdt.hr_paid_minutes is distinct from tdt.ts_paid_minutes) then 'FAIL_TOTALS'
        else 'OK'
      end as day_status
    from day_set ds
    left join hr_day_totals_effective hdt
      on hdt.candidate_id = ds.candidate_id
     and hdt.week_ending_date = ds.week_ending_date
     and hdt.work_date = ds.work_date
    left join ts_day_totals tdt
      on tdt.timesheet_id = ds.timesheet_id
     and tdt.work_date = ds.work_date
  ),

  per_ts as (
    select
      de.candidate_id,
      de.candidate_name,
      de.week_ending_date,
      de.timesheet_id,

      jsonb_agg(
        jsonb_build_object(
          'date', de.work_date::text,
          'hr_minutes', de.hr_paid_minutes,
          'ts_minutes', de.ts_paid_minutes,
          'delta_minutes', de.delta_minutes,
          'day_status', de.day_status
        )
        order by de.work_date asc
      ) as days_json,

      bool_or(de.day_status <> 'OK') as has_totals_mismatch,

      string_agg(
        (
          de.work_date::text || ':' || de.day_status || ':' ||
          coalesce(de.hr_paid_minutes,0)::text || ',' || coalesce(de.ts_paid_minutes,0)::text
        ),
        ';' order by de.work_date asc
      ) as sig_text
    from day_eval de
    group by de.candidate_id, de.candidate_name, de.week_ending_date, de.timesheet_id
  ),

  grouped as (
    select
      p.candidate_id,
      p.candidate_name,
      p.week_ending_date,
      p.timesheet_id,
      p.days_json,
      p.has_totals_mismatch,
      p.sig_text,
      cbg.comparisons_json,
      coalesce(ceg.confirmed_exception_count,0) as confirmed_exception_count,
      coalesce(ceg.confirmed_exceptions_json,'[]'::jsonb) as confirmed_exceptions_json,
      coalesce(cbg.any_invoice_locked,false) as any_invoice_locked,
      coalesce(cbg.any_locked_ref_change,false) as any_locked_ref_change,
      coalesce(cbg.any_locked_time_mismatch,false) as any_locked_time_mismatch
    from per_ts p
    left join comparisons_by_group cbg
      on cbg.candidate_id = p.candidate_id
     and cbg.week_ending_date = p.week_ending_date
     and cbg.timesheet_id = p.timesheet_id
    left join confirmed_exceptions_by_group ceg
      on ceg.candidate_id=p.candidate_id
     and ceg.week_ending_date=p.week_ending_date
     and ceg.timesheet_id=p.timesheet_id
  ),

  final_groups as (
    select
      g.*,
      (
        coalesce(g.has_totals_mismatch,false)
        or (
          g.comparisons_json is not null
          and jsonb_typeof(g.comparisons_json) = 'array'
          and exists (
            select 1
            from jsonb_array_elements(g.comparisons_json) as cx(value)
            where coalesce((cx.value->>'match')::boolean,false) is false
          )
        )
      ) as has_mismatch,

      case
        when (
          coalesce(g.any_invoice_locked,false)
          and (coalesce(g.any_locked_ref_change,false) or coalesce(g.any_locked_time_mismatch,false))
        ) then 'FAIL'
        when (
          coalesce(g.has_totals_mismatch,false)
          or (
            g.comparisons_json is not null
            and jsonb_typeof(g.comparisons_json) = 'array'
            and exists (
              select 1
              from jsonb_array_elements(g.comparisons_json) as cx2(value)
              where coalesce((cx2.value->>'match')::boolean,false) is false
            )
          )
        ) then 'FAIL'
        when coalesce(g.confirmed_exception_count,0)>0 then 'OVERRIDDEN'
        else 'OK'
      end as overall_status
    from grouped g
  ),

  with_fp as (
    select
      fg.*,
      case
        when fg.has_mismatch and fg.timesheet_id is not null then
          ('HEALTHROSTER_WEEKLY|validation|' || fg.timesheet_id::text || '|' || fg.week_ending_date::text || '|' || fg.overall_status || '|' || coalesce(fg.sig_text,''))
        else null
      end as issue_fingerprint
    from final_groups fg
  ),

  with_email_state as (
    select
      wf.*,
      (e.issue_fingerprint is not null) as emailed_already
    from with_fp wf
    left join public.hr_issue_emails e
      on e.issue_fingerprint = wf.issue_fingerprint
  ),

  real_rows as (
    select
      jsonb_build_object(
        'client_id', v_client_id::text,
        'recipient_email', v_recipient_email,

        'candidate_id', wes.candidate_id::text,
        'candidate_name', wes.candidate_name,
        'week_ending_date', wes.week_ending_date::text,
        'timesheet_id', wes.timesheet_id::text,

        'contract_id', case when tts.contract_id is null then null else tts.contract_id::text end,

        'overall_status', wes.overall_status,
        'has_mismatch', wes.has_mismatch,
        'confirmed_exception_count',wes.confirmed_exception_count,
        'confirmed_exceptions',wes.confirmed_exceptions_json,

        'failure_reasons',
          (
            case
              when wes.has_mismatch is false then '[]'::jsonb
              else
                (
                  jsonb_build_array(
                    case
                      when wes.any_invoice_locked and (wes.any_locked_ref_change or wes.any_locked_time_mismatch)
                        then 'Warning: an invoiced/locked shift differs from this import. You must not change an invoiced shift.'
                      else null
                    end,
                    case
                      when wes.has_totals_mismatch then 'Totals mismatch within import date range.'
                      else null
                    end
                  )
                  ||
                  coalesce(
                    (
                      select jsonb_agg(
                        distinct
                        case
                          when (cx.value->>'match_status') = 'UNMATCHED' then 'Missing from import: timesheet shift not found in HealthRoster file.'
                          when (cx.value->>'match_status') = 'HR_ONLY' then 'HealthRoster has a shift not present on the timesheet.'
                          when (cx.value->>'match_status') = 'AMBIGUOUS' then 'Ambiguous overlap: shift cannot be paired 1:1.'
                          when (cx.value->>'match_status') = 'MISMATCH' then 'Shift detail mismatch (start/end/break differs).'
                          else null
                        end
                      )
                      from jsonb_array_elements(coalesce(wes.comparisons_json,'[]'::jsonb)) as cx(value)
                      where coalesce(cx.value->>'match_status','') <> 'MATCH'
                    ),
                    '[]'::jsonb
                  )
                )
            end
          ),

        'issue_fingerprint', wes.issue_fingerprint,
        'emailed_already', wes.emailed_already,
        'can_email',
          (
            wes.has_mismatch
            and wes.timesheet_id is not null
            and wes.issue_fingerprint is not null
            and v_recipient_email is not null
            and length(btrim(v_recipient_email)) > 0
            and exists (
              select 1
              from jsonb_array_elements(coalesce(wes.comparisons_json,'[]'::jsonb)) as email_cx(value)
              where coalesce(email_cx.value->>'match_status','MATCH') not in ('MATCH','HR_ONLY')
                or coalesce((email_cx.value->>'ref_changed')::boolean,false)
            )
          ),

        'days', coalesce(wes.days_json, '[]'::jsonb),
        'comparisons', coalesce(wes.comparisons_json, '[]'::jsonb)
      ) as j
    from with_email_state wes
    left join public.timesheets tts
      on tts.timesheet_id = wes.timesheet_id
     and tts.is_current = true
  ),

  missing_ts_rows as (
    select
      jsonb_build_object(
        'client_id', v_client_id::text,
        'recipient_email', v_recipient_email,

        'candidate_id', tr.candidate_id::text,
        'candidate_name', tr.candidate_name,
        'week_ending_date', tr.week_ending_date::text,
        'timesheet_id', null,

        'contract_id', null,

        'overall_status', 'MISSING_TIMESHEET',
        'has_mismatch', true,
        'failure_reasons', jsonb_build_array('No weekly timesheet found for this candidate/week in HR validation scope.'),

        'issue_fingerprint', null,
        'emailed_already', false,
        'can_email', false,

        'days', coalesce((
          select jsonb_agg(jsonb_build_object(
            'date', hd.work_date::text,
            'hr_minutes', hd.hr_paid_minutes,
            'ts_minutes', null,
            'delta_minutes', hd.hr_paid_minutes,
            'day_status', 'TIMESHEET_NOT_SUBMITTED'
          ) order by hd.work_date)
          from hr_day_totals hd
          where hd.candidate_id=tr.candidate_id
            and hd.week_ending_date=tr.week_ending_date
        ), '[]'::jsonb),
        'comparisons', coalesce((
          select jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
            'comparison_key', 'hr-row:'||hf.hr_row_id::text,
            'hr_row_id', hf.hr_row_id,
            'work_date', hf.work_date::text,
            'match', false,
            'time_match', false,
            'match_status', 'TIMESHEET_NOT_SUBMITTED',
            'healthroster_start', hf.hr_start_hhmm,
            'healthroster_end', hf.hr_end_hhmm,
            'healthroster_break_mins', hf.hr_break_mins,
            'healthroster_paid_minutes', hf.hr_paid_minutes,
            'role', hf.assignment_grade,
            'ref_after', hf.hr_request_id,
            'location_after', hf.hr_location,
            'times_after', jsonb_build_object(
              'start', hf.hr_start_hhmm,
              'end', hf.hr_end_hhmm,
              'break_mins', hf.hr_break_mins
            )
          )) order by hf.work_date,hf.hr_start_min,hf.hr_end_min,hf.hr_row_id)
          from hr_entries_flat hf
          where hf.candidate_id=tr.candidate_id
            and hf.week_ending_date=tr.week_ending_date
        ), '[]'::jsonb)
      ) as j
    from hr_triples tr
    where not exists (
      select 1
      from ts_matches tm
      where tm.candidate_id = tr.candidate_id
        and tm.week_ending_date = tr.week_ending_date
        and tm.timesheet_id is not null
    )
  ),

  all_rows_json as (
    select
      jsonb_agg(r.j order by (r.j->>'week_ending_date')::date asc, (r.j->>'candidate_name') nulls last) as rows_json
    from (
      select rr.j from real_rows rr
      union all
      select mr.j from missing_ts_rows mr
    ) as r
  )

  select
    coalesce(arows.rows_json, '[]'::jsonb),
    (select n from unmapped_candidate_rows),
    (select count(*)::int
     from ts_matches tm
     where tm.raw_timesheet_id is null),
    (select count(*)::int
     from ts_matches tm
     where tm.awaiting_authorisation is true)
  into v_rows, v_unmapped_candidates, v_unmatched_timesheets, v_unauthorised_timesheet_triples
  from all_rows_json arows;

  v_result:=jsonb_build_object(
    'import_id', p_import_id::text,
    'client_id', v_client_id::text,
    'week_ending_weekday', v_we_dow,
    'recipient_email', v_recipient_email,

    'file_date_min', v_file_date_min::text,
    'file_date_max', v_file_date_max::text,

    'unmapped_candidate_rows', v_unmapped_candidates,
    'unmatched_timesheet_triples', v_unmatched_timesheets,
    'unauthorised_timesheet_triples', v_unauthorised_timesheet_triples,

    'rows', v_rows,
    'validation_groups', v_rows
  );
  select jsonb_build_object('status',s.status,'state_version',s.state_version,
    'preview_generation',s.preview_generation,'preview_fingerprint',s.preview_fingerprint,
    'coverage_fingerprint',i.coverage_fingerprint,
    'actions',(select coalesce(jsonb_agg(to_jsonb(x) order by x.action_id),'[]'::jsonb)
      from (select d.action_id,d.action_kind,d.action_category,d.target_key,d.timesheet_id,d.shift_id,
        d.client_id,d.candidate_id,d.contract_id,d.selectable,d.default_selected,d.selected,d.blocking,
        d.requires_reconfirmation,d.summary_json
        from public.import_review_decisions d where d.import_id=p_import_id and d.is_current
        order by d.action_id limit 500) x))
    into v_review
  from public.import_review_states s join public.hr_imports i on i.id=s.import_id
  where s.import_id=p_import_id;
  return v_result||case when v_review is null then '{}'::jsonb else jsonb_build_object('review_contract',v_review) end;
end;
$function$;

-- id_consolidation_balance_now(uuid,text,text)
CREATE OR REPLACE FUNCTION public.id_consolidation_balance_now(p_actor_user_id uuid, p_bank_upload_code text, p_note text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_id_ref text;
  v_created_at timestamptz;
  v_bank_uploaded_at timestamptz;

  v_total_ex numeric(12,2) := 0;
  v_total_vat numeric(12,2) := 0;
  v_total_inc numeric(12,2) := 0;

  v_lines jsonb := '[]'::jsonb;

  v_bank_upload_code text;
  v_note text;

  v_draft jsonb;
  v_commit jsonb;

  v_is_on_hold boolean;
begin
  v_bank_upload_code := nullif(btrim(coalesce(p_bank_upload_code,'')), '');
  v_note := nullif(btrim(coalesce(p_note,'')), '');

  if v_bank_upload_code is null then
    raise exception 'BANK_UPLOAD_CODE_REQUIRED';
  end if;

  if to_regclass('public.id_ref_seq') is null then
    raise exception 'ID_REF_SEQ_MISSING';
  end if;
  if to_regclass('public.id_invoice_ledger') is null then
    raise exception 'ID_LEDGER_MISSING';
  end if;
  if to_regclass('public.id_consolidation_runs') is null
     or to_regclass('public.id_consolidation_run_lines') is null then
    raise exception 'ID_RUN_TABLES_MISSING';
  end if;

  -- ✅ Two-phase wrapper:
  -- 1) Draft start (creates run header + snapshot lines, NO ledger updates)
  v_draft := public.id_consolidation_run_draft_start(p_actor_user_id, v_note);

  v_id_ref := nullif(btrim(coalesce(v_draft->>'id_ref','')), '');
  if v_id_ref is null or v_id_ref !~ '^[0-9]{6}$' then
    raise exception 'DRAFT_START_FAILED';
  end if;

  v_created_at := (v_draft->>'created_at_utc')::timestamptz;

  v_total_ex := coalesce((v_draft #>> '{totals,delta_ex_vat}')::numeric, 0)::numeric(12,2);
  v_total_vat := coalesce((v_draft #>> '{totals,delta_vat}')::numeric, 0)::numeric(12,2);
  v_total_inc := coalesce((v_draft #>> '{totals,delta_inc_vat}')::numeric, 0)::numeric(12,2);

  -- Rebuild legacy "lines" payload shape from snapshot run lines
  select
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'invoice_id', rl.invoice_id::text,
          'invoice_number', rl.invoice_number,
          'invoice_status', rl.invoice_status,
          'invoice_type', rl.invoice_type,

          'is_on_hold', (upper(coalesce(rl.invoice_status,'')) = 'ON_HOLD'),

          'delta_ex_vat', coalesce(rl.delta_ex_vat, 0)::numeric(12,2),
          'delta_vat', coalesce(rl.delta_vat, 0)::numeric(12,2),
          'delta_inc_vat', coalesce(rl.delta_inc_vat, 0)::numeric(12,2),

          'current_ex_vat', coalesce(rl.current_ex_vat, 0)::numeric(12,2),
          'current_vat', coalesce(rl.current_vat, 0)::numeric(12,2),
          'current_inc_vat', coalesce(rl.current_inc_vat, 0)::numeric(12,2),

          'reportable_current_ex_vat',
            (case
              when upper(coalesce(rl.invoice_status,'')) = 'ON_HOLD' then 0::numeric(12,2)
              else coalesce(rl.current_ex_vat, 0)::numeric(12,2)
            end),

          'reportable_current_vat',
            (case
              when upper(coalesce(rl.invoice_status,'')) = 'ON_HOLD' then 0::numeric(12,2)
              else coalesce(rl.current_vat, 0)::numeric(12,2)
            end),

          'reportable_current_inc_vat',
            (case
              when upper(coalesce(rl.invoice_status,'')) = 'ON_HOLD' then 0::numeric(12,2)
              else coalesce(rl.current_inc_vat, 0)::numeric(12,2)
            end),

          'last_reported_ex_vat',
            (
              (case
                when upper(coalesce(rl.invoice_status,'')) = 'ON_HOLD' then 0::numeric(12,2)
                else coalesce(rl.current_ex_vat, 0)::numeric(12,2)
              end)
              - coalesce(rl.delta_ex_vat, 0)::numeric(12,2)
            )::numeric(12,2),

          'last_reported_vat',
            (
              (case
                when upper(coalesce(rl.invoice_status,'')) = 'ON_HOLD' then 0::numeric(12,2)
                else coalesce(rl.current_vat, 0)::numeric(12,2)
              end)
              - coalesce(rl.delta_vat, 0)::numeric(12,2)
            )::numeric(12,2),

          'last_reported_inc_vat',
            (
              (case
                when upper(coalesce(rl.invoice_status,'')) = 'ON_HOLD' then 0::numeric(12,2)
                else coalesce(rl.current_inc_vat, 0)::numeric(12,2)
              end)
              - coalesce(rl.delta_inc_vat, 0)::numeric(12,2)
            )::numeric(12,2)
        )
        order by
          nullif(btrim(coalesce(rl.invoice_number,'')),'') nulls last,
          rl.invoice_id
      ),
      '[]'::jsonb
    )
  into v_lines
  from public.id_consolidation_run_lines rl
  where rl.id_ref = v_id_ref;

  -- 2) Commit (stores bank upload code + updates ledger baselines from snapshot)
  v_commit := public.id_consolidation_run_draft_commit(v_id_ref, v_bank_upload_code, p_actor_user_id);

  v_bank_uploaded_at := (v_commit->>'bank_uploaded_at_utc')::timestamptz;

  return jsonb_build_object(
    'id_ref', v_id_ref,
    'created_at_utc', v_created_at,
    'bank_upload_code', v_bank_upload_code,
    'bank_uploaded_at_utc', v_bank_uploaded_at,
    'note', v_note,
    'total_delta_ex_vat', v_total_ex,
    'total_delta_vat', v_total_vat,
    'total_delta_inc_vat', v_total_inc,
    'line_count', jsonb_array_length(v_lines),
    'lines', v_lines
  );
end;
$function$;

-- id_consolidation_preview()
CREATE OR REPLACE FUNCTION public.id_consolidation_preview()
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_lines jsonb := '[]'::jsonb;

  v_total_ex numeric(12,2) := 0;
  v_total_vat numeric(12,2) := 0;
  v_total_inc numeric(12,2) := 0;

  v_total_current_ex numeric(12,2) := 0;
  v_total_current_vat numeric(12,2) := 0;
  v_total_current_inc numeric(12,2) := 0;

  v_total_reportable_ex numeric(12,2) := 0;
  v_total_reportable_vat numeric(12,2) := 0;
  v_total_reportable_inc numeric(12,2) := 0;
begin
  -- Guard (gives an explicit error if migrations not applied)
  if to_regclass('public.id_invoice_ledger') is null then
    raise exception 'ID_LEDGER_MISSING';
  end if;

  with base as (
    select
      l.invoice_id,
      l.invoice_number,
      l.invoice_status,
      l.invoice_type,

      coalesce(l.current_ex_vat,0)::numeric(12,2) as current_ex_vat,
      coalesce(l.current_vat,0)::numeric(12,2) as current_vat,
      coalesce(l.current_inc_vat,0)::numeric(12,2) as current_inc_vat,

      coalesce(l.last_reported_ex_vat,0)::numeric(12,2) as last_reported_ex_vat,
      coalesce(l.last_reported_vat,0)::numeric(12,2) as last_reported_vat,
      coalesce(l.last_reported_inc_vat,0)::numeric(12,2) as last_reported_inc_vat,

      l.updated_at_utc,

      i.client_id as client_id,
      c.name as client_name,

      i.invoice_no as inv_invoice_no,
      i.status::text as inv_status_text,
      i.type::text as inv_type_text,

      i.issued_at_utc,
      i.due_at_utc,
      i.paid_at_utc,
      i.status_date_utc,
      i.credit_note_created_at_utc,

      coalesce(nullif(btrim(coalesce(l.invoice_number, '')), ''), i.invoice_no) as effective_invoice_number,
      coalesce(nullif(btrim(coalesce(l.invoice_status, '')), ''), i.status::text) as effective_invoice_status,
      coalesce(nullif(btrim(coalesce(l.invoice_type, '')), ''), i.type::text) as effective_invoice_type,

      coalesce(i.issued_at_utc, i.status_date_utc, l.updated_at_utc) as sort_ts
    from public.id_invoice_ledger l
    left join public.invoices i
      on i.id = l.invoice_id
    left join public.clients c
      on c.id = i.client_id
  ),
  calc as (
    select
      b.*,

      (upper(coalesce(b.effective_invoice_status,'')) = 'ON_HOLD') as is_on_hold,

      (case
        when upper(coalesce(b.effective_invoice_status,'')) = 'ON_HOLD' then 0::numeric(12,2)
        else coalesce(b.current_ex_vat,0)::numeric(12,2)
      end) as reportable_current_ex_vat,

      (case
        when upper(coalesce(b.effective_invoice_status,'')) = 'ON_HOLD' then 0::numeric(12,2)
        else coalesce(b.current_vat,0)::numeric(12,2)
      end) as reportable_current_vat,

      (case
        when upper(coalesce(b.effective_invoice_status,'')) = 'ON_HOLD' then 0::numeric(12,2)
        else coalesce(b.current_inc_vat,0)::numeric(12,2)
      end) as reportable_current_inc_vat
    from base b
  ),
  changed as (
    select
      c.*,

      (c.reportable_current_ex_vat - coalesce(c.last_reported_ex_vat,0)::numeric(12,2))::numeric(12,2) as delta_ex_vat,
      (c.reportable_current_vat - coalesce(c.last_reported_vat,0)::numeric(12,2))::numeric(12,2) as delta_vat,
      (c.reportable_current_inc_vat - coalesce(c.last_reported_inc_vat,0)::numeric(12,2))::numeric(12,2) as delta_inc_vat,

      (case when c.is_on_hold then 'NON_REPORTABLE' else 'REPORTABLE' end) as line_kind,
      (case when c.is_on_hold then 'ON_HOLD' else null end) as non_reportable_reason
    from calc c
    where
      c.reportable_current_ex_vat <> coalesce(c.last_reported_ex_vat,0)::numeric(12,2)
      or c.reportable_current_vat <> coalesce(c.last_reported_vat,0)::numeric(12,2)
      or c.reportable_current_inc_vat <> coalesce(c.last_reported_inc_vat,0)::numeric(12,2)
  )
  select
    coalesce(sum(ch.delta_ex_vat),0)::numeric(12,2),
    coalesce(sum(ch.delta_vat),0)::numeric(12,2),
    coalesce(sum(ch.delta_inc_vat),0)::numeric(12,2),

    coalesce(sum(ch.current_ex_vat),0)::numeric(12,2),
    coalesce(sum(ch.current_vat),0)::numeric(12,2),
    coalesce(sum(ch.current_inc_vat),0)::numeric(12,2),

    coalesce(sum(ch.reportable_current_ex_vat),0)::numeric(12,2),
    coalesce(sum(ch.reportable_current_vat),0)::numeric(12,2),
    coalesce(sum(ch.reportable_current_inc_vat),0)::numeric(12,2),

    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'invoice_id', ch.invoice_id::text,
          'invoice_number', ch.effective_invoice_number,
          'invoice_status', ch.effective_invoice_status,
          'invoice_type', ch.effective_invoice_type,

          'client_id', case when ch.client_id is null then null else ch.client_id::text end,
          'client_name', ch.client_name,

          'issued_at_utc', ch.issued_at_utc,
          'due_at_utc', ch.due_at_utc,
          'paid_at_utc', ch.paid_at_utc,
          'status_date_utc', ch.status_date_utc,
          'credit_note_created_at_utc', ch.credit_note_created_at_utc,

          'updated_at_utc', ch.updated_at_utc,

          'is_on_hold', ch.is_on_hold,
          'line_kind', ch.line_kind,
          'non_reportable_reason', ch.non_reportable_reason,

          'current_ex_vat', ch.current_ex_vat,
          'current_vat', ch.current_vat,
          'current_inc_vat', ch.current_inc_vat,

          'reportable_current_ex_vat', ch.reportable_current_ex_vat,
          'reportable_current_vat', ch.reportable_current_vat,
          'reportable_current_inc_vat', ch.reportable_current_inc_vat,

          'last_reported_ex_vat', ch.last_reported_ex_vat,
          'last_reported_vat', ch.last_reported_vat,
          'last_reported_inc_vat', ch.last_reported_inc_vat,

          'delta_ex_vat', ch.delta_ex_vat,
          'delta_vat', ch.delta_vat,
          'delta_inc_vat', ch.delta_inc_vat
        )
        order by
          ch.sort_ts desc nulls last,
          nullif(btrim(coalesce(ch.effective_invoice_number,'')),'') nulls last,
          ch.invoice_id
      ),
      '[]'::jsonb
    )
  into
    v_total_ex, v_total_vat, v_total_inc,
    v_total_current_ex, v_total_current_vat, v_total_current_inc,
    v_total_reportable_ex, v_total_reportable_vat, v_total_reportable_inc,
    v_lines
  from changed ch;

  return jsonb_build_object(
    'total_delta_ex_vat', v_total_ex,
    'total_delta_vat', v_total_vat,
    'total_delta_inc_vat', v_total_inc,

    'total_current_ex_vat', v_total_current_ex,
    'total_current_vat', v_total_current_vat,
    'total_current_inc_vat', v_total_current_inc,

    'total_reportable_current_ex_vat', v_total_reportable_ex,
    'total_reportable_current_vat', v_total_reportable_vat,
    'total_reportable_current_inc_vat', v_total_reportable_inc,

    'line_count', jsonb_array_length(v_lines),
    'lines', v_lines
  );
end;
$function$;

-- id_consolidation_run_draft_cancel(text,uuid)
CREATE OR REPLACE FUNCTION public.id_consolidation_run_draft_cancel(p_id_ref text, p_actor_user_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_id_ref text := nullif(btrim(coalesce(p_id_ref,'')), '');

  v_run record;

  v_deleted_lines int := 0;
  v_deleted_run int := 0;

  v_has_commit_marker boolean := false;
begin
  if p_actor_user_id is null then
    raise exception 'ACTOR_USER_ID_REQUIRED';
  end if;

  if to_regclass('public.id_consolidation_runs') is null
     or to_regclass('public.id_consolidation_run_lines') is null then
    raise exception 'ID_RUN_TABLES_MISSING';
  end if;

  if v_id_ref is null or v_id_ref !~ '^[0-9]{6}$' then
    raise exception 'INVALID_ID_REF';
  end if;

  -- Lock run row (if present)
  select
    r.id_ref,
    r.bank_upload_code,
    r.bank_uploaded_at_utc
  into v_run
  from public.id_consolidation_runs r
  where r.id_ref = v_id_ref
  limit 1
  for update;

  if not found then
    return jsonb_build_object(
      'ok', true,
      'id_ref', v_id_ref,
      'cancelled', false,
      'already_missing', true
    );
  end if;

  v_has_commit_marker :=
    (v_run.bank_uploaded_at_utc is not null)
    or (nullif(btrim(coalesce(v_run.bank_upload_code,'')), '') is not null);

  if v_has_commit_marker then
    raise exception 'CANNOT_CANCEL_COMMITTED_RUN';
  end if;

  delete from public.id_consolidation_run_lines rl
  where rl.id_ref = v_id_ref;

  get diagnostics v_deleted_lines = row_count;

  delete from public.id_consolidation_runs r2
  where r2.id_ref = v_id_ref
    and r2.bank_uploaded_at_utc is null;

  get diagnostics v_deleted_run = row_count;

  if v_deleted_run = 0 then
    raise exception 'CANCEL_FAILED';
  end if;

  return jsonb_build_object(
    'ok', true,
    'id_ref', v_id_ref,
    'cancelled', true,
    'already_missing', false,
    'deleted_lines', coalesce(v_deleted_lines, 0)
  );
end;
$function$;

-- id_consolidation_run_draft_commit(text,text,uuid)
CREATE OR REPLACE FUNCTION public.id_consolidation_run_draft_commit(p_id_ref text, p_bank_upload_code text, p_actor_user_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_id_ref text := nullif(btrim(coalesce(p_id_ref,'')), '');
  v_bank_upload_code text := nullif(btrim(coalesce(p_bank_upload_code,'')), '');
  v_now timestamptz := now();

  v_run record;

  v_total_ex numeric(12,2) := 0;
  v_total_vat numeric(12,2) := 0;
  v_total_inc numeric(12,2) := 0;

  v_line_count int := 0;
  v_ledger_rows_updated int := 0;
  v_header_updated int := 0;

  v_has_committed_by_col boolean := false;
  v_existing_code text;
begin
  if p_actor_user_id is null then
    raise exception 'ACTOR_USER_ID_REQUIRED';
  end if;

  if to_regclass('public.id_consolidation_runs') is null
     or to_regclass('public.id_consolidation_run_lines') is null then
    raise exception 'ID_RUN_TABLES_MISSING';
  end if;

  if to_regclass('public.id_invoice_ledger') is null then
    raise exception 'ID_LEDGER_MISSING';
  end if;

  if v_id_ref is null or v_id_ref !~ '^[0-9]{6}$' then
    raise exception 'INVALID_ID_REF';
  end if;

  if v_bank_upload_code is null then
    raise exception 'BANK_UPLOAD_CODE_REQUIRED';
  end if;

  -- Lock run row for idempotency + concurrency safety
  select
    r.id_ref,
    r.created_at_utc,
    r.created_by_user_id,
    r.total_delta_ex_vat,
    r.total_delta_vat,
    r.total_delta_inc_vat,
    r.bank_upload_code,
    r.bank_uploaded_at_utc,
    r.note
  into v_run
  from public.id_consolidation_runs r
  where r.id_ref = v_id_ref
  limit 1
  for update;

  if not found then
    raise exception 'ID_RUN_NOT_FOUND';
  end if;

  v_total_ex := coalesce(v_run.total_delta_ex_vat, 0)::numeric(12,2);
  v_total_vat := coalesce(v_run.total_delta_vat, 0)::numeric(12,2);
  v_total_inc := coalesce(v_run.total_delta_inc_vat, 0)::numeric(12,2);

  v_existing_code := nullif(btrim(coalesce(v_run.bank_upload_code,'')), '');

  -- Already committed (idempotent path)
  if v_run.bank_uploaded_at_utc is not null or v_existing_code is not null then
    if v_existing_code is not distinct from v_bank_upload_code then
      select count(*)::int
      into v_line_count
      from public.id_consolidation_run_lines rl
      where rl.id_ref = v_id_ref;

      return jsonb_build_object(
        'id_ref', v_id_ref,
        'state', 'COMMITTED',
        'bank_upload_code', v_existing_code,
        'bank_uploaded_at_utc', v_run.bank_uploaded_at_utc,
        'committed_by_user_id', p_actor_user_id::text,
        'line_count', coalesce(v_line_count, 0),
        'totals', jsonb_build_object(
          'delta_ex_vat', v_total_ex,
          'delta_vat', v_total_vat,
          'delta_inc_vat', v_total_inc
        ),
        'ledger_rows_updated', 0,
        'did_commit', false
      );
    else
      raise exception 'ID_RUN_ALREADY_COMMITTED_DIFFERENT_CODE';
    end if;
  end if;

  -- Detect optional column committed_by_user_id (NOT present in current schema dump)
  select exists (
    select 1
    from information_schema.columns c
    where c.table_schema = 'public'
      and c.table_name = 'id_consolidation_runs'
      and c.column_name = 'committed_by_user_id'
  )
  into v_has_committed_by_col;

  if v_has_committed_by_col then
    execute
      'update public.id_consolidation_runs
          set bank_upload_code = $1,
              bank_uploaded_at_utc = $2,
              committed_by_user_id = $3
        where id_ref = $4
          and bank_uploaded_at_utc is null'
    using v_bank_upload_code, v_now, p_actor_user_id, v_id_ref;

    get diagnostics v_header_updated = row_count;
  else
    update public.id_consolidation_runs r2
    set
      bank_upload_code = v_bank_upload_code,
      bank_uploaded_at_utc = v_now
    where r2.id_ref = v_id_ref
      and r2.bank_uploaded_at_utc is null;

    get diagnostics v_header_updated = row_count;
  end if;

  if v_header_updated = 0 then
    -- Another session committed concurrently; re-check for idempotency
    select
      r3.bank_upload_code,
      r3.bank_uploaded_at_utc
    into
      v_existing_code,
      v_now
    from public.id_consolidation_runs r3
    where r3.id_ref = v_id_ref
    limit 1;

    v_existing_code := nullif(btrim(coalesce(v_existing_code,'')), '');

    if v_now is not null and v_existing_code is not distinct from v_bank_upload_code then
      select count(*)::int
      into v_line_count
      from public.id_consolidation_run_lines rl
      where rl.id_ref = v_id_ref;

      return jsonb_build_object(
        'id_ref', v_id_ref,
        'state', 'COMMITTED',
        'bank_upload_code', v_existing_code,
        'bank_uploaded_at_utc', v_now,
        'committed_by_user_id', p_actor_user_id::text,
        'line_count', coalesce(v_line_count, 0),
        'totals', jsonb_build_object(
          'delta_ex_vat', v_total_ex,
          'delta_vat', v_total_vat,
          'delta_inc_vat', v_total_inc
        ),
        'ledger_rows_updated', 0,
        'did_commit', false
      );
    end if;

    raise exception 'ID_RUN_COMMIT_RACE';
  end if;

  -- Update ledger baselines from SNAPSHOT lines (not from current ledger state)
  update public.id_invoice_ledger l
  set
    last_reported_ex_vat = (
      case
        when upper(coalesce(rl.invoice_status,'')) = 'ON_HOLD' then 0::numeric(12,2)
        else coalesce(rl.current_ex_vat,0)::numeric(12,2)
      end
    ),
    last_reported_vat = (
      case
        when upper(coalesce(rl.invoice_status,'')) = 'ON_HOLD' then 0::numeric(12,2)
        else coalesce(rl.current_vat,0)::numeric(12,2)
      end
    ),
    last_reported_inc_vat = (
      case
        when upper(coalesce(rl.invoice_status,'')) = 'ON_HOLD' then 0::numeric(12,2)
        else coalesce(rl.current_inc_vat,0)::numeric(12,2)
      end
    ),
    updated_at_utc = v_now
  from public.id_consolidation_run_lines rl
  where rl.id_ref = v_id_ref
    and l.invoice_id = rl.invoice_id;

  get diagnostics v_ledger_rows_updated = row_count;

  select count(*)::int
  into v_line_count
  from public.id_consolidation_run_lines rl2
  where rl2.id_ref = v_id_ref;

  return jsonb_build_object(
    'id_ref', v_id_ref,
    'state', 'COMMITTED',
    'bank_upload_code', v_bank_upload_code,
    'bank_uploaded_at_utc', v_now,
    'committed_by_user_id', p_actor_user_id::text,
    'line_count', coalesce(v_line_count, 0),
    'totals', jsonb_build_object(
      'delta_ex_vat', v_total_ex,
      'delta_vat', v_total_vat,
      'delta_inc_vat', v_total_inc
    ),
    'ledger_rows_updated', coalesce(v_ledger_rows_updated, 0),
    'did_commit', true
  );
end;
$function$;

-- id_consolidation_run_draft_start(uuid,text)
CREATE OR REPLACE FUNCTION public.id_consolidation_run_draft_start(p_actor_user_id uuid, p_note text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_ref_num bigint;
  v_id_ref text;
  v_created_at timestamptz := now();

  v_total_ex numeric(12,2) := 0;
  v_total_vat numeric(12,2) := 0;
  v_total_inc numeric(12,2) := 0;
  v_line_count int := 0;

  v_note text;
begin
  v_note := nullif(btrim(coalesce(p_note,'')), '');

  if to_regclass('public.id_ref_seq') is null then
    raise exception 'ID_REF_SEQ_MISSING';
  end if;
  if to_regclass('public.id_invoice_ledger') is null then
    raise exception 'ID_LEDGER_MISSING';
  end if;
  if to_regclass('public.id_consolidation_runs') is null
     or to_regclass('public.id_consolidation_run_lines') is null then
    raise exception 'ID_RUN_TABLES_MISSING';
  end if;

  -- Allocate new sequential ref and format as 6 digits
  select nextval('public.id_ref_seq') into v_ref_num;
  v_id_ref := lpad(v_ref_num::text, 6, '0');

  -- Build snapshot of changed rows (same delta logic as id_consolidation_balance_now),
  -- and INSERT run header + run lines WITHOUT updating id_invoice_ledger baselines.
  with changed as (
    select
      l.invoice_id,
      l.invoice_number,
      l.invoice_status,
      l.invoice_type,

      (upper(coalesce(l.invoice_status,'')) = 'ON_HOLD') as is_on_hold,

      coalesce(l.current_ex_vat,0)::numeric(12,2) as current_ex_vat,
      coalesce(l.current_vat,0)::numeric(12,2) as current_vat,
      coalesce(l.current_inc_vat,0)::numeric(12,2) as current_inc_vat,

      coalesce(l.last_reported_ex_vat,0)::numeric(12,2) as last_reported_ex_vat,
      coalesce(l.last_reported_vat,0)::numeric(12,2) as last_reported_vat,
      coalesce(l.last_reported_inc_vat,0)::numeric(12,2) as last_reported_inc_vat,

      (case
        when upper(coalesce(l.invoice_status,'')) = 'ON_HOLD' then 0::numeric(12,2)
        else coalesce(l.current_ex_vat,0)::numeric(12,2)
      end) as reportable_current_ex_vat,

      (case
        when upper(coalesce(l.invoice_status,'')) = 'ON_HOLD' then 0::numeric(12,2)
        else coalesce(l.current_vat,0)::numeric(12,2)
      end) as reportable_current_vat,

      (case
        when upper(coalesce(l.invoice_status,'')) = 'ON_HOLD' then 0::numeric(12,2)
        else coalesce(l.current_inc_vat,0)::numeric(12,2)
      end) as reportable_current_inc_vat,

      (
        (case
          when upper(coalesce(l.invoice_status,'')) = 'ON_HOLD' then 0::numeric(12,2)
          else coalesce(l.current_ex_vat,0)::numeric(12,2)
        end)
        - coalesce(l.last_reported_ex_vat,0)::numeric(12,2)
      )::numeric(12,2) as delta_ex_vat,

      (
        (case
          when upper(coalesce(l.invoice_status,'')) = 'ON_HOLD' then 0::numeric(12,2)
          else coalesce(l.current_vat,0)::numeric(12,2)
        end)
        - coalesce(l.last_reported_vat,0)::numeric(12,2)
      )::numeric(12,2) as delta_vat,

      (
        (case
          when upper(coalesce(l.invoice_status,'')) = 'ON_HOLD' then 0::numeric(12,2)
          else coalesce(l.current_inc_vat,0)::numeric(12,2)
        end)
        - coalesce(l.last_reported_inc_vat,0)::numeric(12,2)
      )::numeric(12,2) as delta_inc_vat
    from public.id_invoice_ledger l
    where
      (case
        when upper(coalesce(l.invoice_status,'')) = 'ON_HOLD' then 0::numeric(12,2)
        else coalesce(l.current_ex_vat,0)::numeric(12,2)
      end) <> coalesce(l.last_reported_ex_vat,0)::numeric(12,2)
      or
      (case
        when upper(coalesce(l.invoice_status,'')) = 'ON_HOLD' then 0::numeric(12,2)
        else coalesce(l.current_vat,0)::numeric(12,2)
      end) <> coalesce(l.last_reported_vat,0)::numeric(12,2)
      or
      (case
        when upper(coalesce(l.invoice_status,'')) = 'ON_HOLD' then 0::numeric(12,2)
        else coalesce(l.current_inc_vat,0)::numeric(12,2)
      end) <> coalesce(l.last_reported_inc_vat,0)::numeric(12,2)
    for update
  ),
  agg as (
    select
      coalesce(sum(c.delta_ex_vat),0)::numeric(12,2) as total_ex,
      coalesce(sum(c.delta_vat),0)::numeric(12,2) as total_vat,
      coalesce(sum(c.delta_inc_vat),0)::numeric(12,2) as total_inc,
      count(*)::int as line_count
    from changed c
  ),
  ins_run as (
    insert into public.id_consolidation_runs (
      id_ref,
      created_at_utc,
      created_by_user_id,
      total_delta_ex_vat,
      total_delta_vat,
      total_delta_inc_vat,
      bank_upload_code,
      bank_uploaded_at_utc,
      note
    )
    select
      v_id_ref,
      v_created_at,
      p_actor_user_id,
      a.total_ex,
      a.total_vat,
      a.total_inc,
      null::text,
      null::timestamptz,
      v_note
    from agg a
    returning 1
  ),
  ins_lines as (
    insert into public.id_consolidation_run_lines (
      id_ref,
      invoice_id,
      invoice_number,
      invoice_status,
      invoice_type,
      delta_ex_vat,
      delta_vat,
      delta_inc_vat,
      current_ex_vat,
      current_vat,
      current_inc_vat
    )
    select
      v_id_ref,
      c.invoice_id,
      c.invoice_number,
      c.invoice_status,
      c.invoice_type,
      c.delta_ex_vat,
      c.delta_vat,
      c.delta_inc_vat,
      c.current_ex_vat,
      c.current_vat,
      c.current_inc_vat
    from changed c
    returning 1
  )
  select
    a.total_ex,
    a.total_vat,
    a.total_inc,
    a.line_count
  into
    v_total_ex,
    v_total_vat,
    v_total_inc,
    v_line_count
  from agg a;

  return jsonb_build_object(
    'id_ref', v_id_ref,
    'created_at_utc', v_created_at,
    'line_count', coalesce(v_line_count, 0),
    'totals', jsonb_build_object(
      'delta_ex_vat', coalesce(v_total_ex, 0)::numeric(12,2),
      'delta_vat', coalesce(v_total_vat, 0)::numeric(12,2),
      'delta_inc_vat', coalesce(v_total_inc, 0)::numeric(12,2)
    )
  );
end;
$function$;

-- id_consolidation_run_get(text)
CREATE OR REPLACE FUNCTION public.id_consolidation_run_get(p_id_ref text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_run record;
  v_lines jsonb := '[]'::jsonb;
begin
  if to_regclass('public.id_consolidation_runs') is null
     or to_regclass('public.id_consolidation_run_lines') is null then
    raise exception 'ID_RUN_TABLES_MISSING';
  end if;

  if p_id_ref is null or p_id_ref !~ '^[0-9]{6}$' then
    raise exception 'INVALID_ID_REF';
  end if;

  select
    r.id_ref,
    r.created_at_utc,
    r.created_by_user_id,
    r.committed_by_user_id,
    r.total_delta_ex_vat,
    r.total_delta_vat,
    r.total_delta_inc_vat,
    r.bank_upload_code,
    r.bank_uploaded_at_utc,
    r.note
  into v_run
  from public.id_consolidation_runs r
  where r.id_ref = p_id_ref
  limit 1;

  if not found then
    raise exception 'ID_RUN_NOT_FOUND';
  end if;

  select
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'invoice_id', rl.invoice_id::text,

          'invoice_number',
            coalesce(
              nullif(btrim(coalesce(rl.invoice_number,'')),''),
              i.invoice_no
            ),

          'client_id', case when i.client_id is null then null else i.client_id::text end,
          'client_name', c.name,

          'invoice_status',
            coalesce(
              nullif(btrim(coalesce(rl.invoice_status,'')),''),
              i.status::text
            ),

          'invoice_type',
            coalesce(
              nullif(btrim(coalesce(rl.invoice_type,'')),''),
              i.type::text
            ),

          'delta_ex_vat', rl.delta_ex_vat,
          'delta_vat', rl.delta_vat,
          'delta_inc_vat', rl.delta_inc_vat,

          'current_ex_vat', rl.current_ex_vat,
          'current_vat', rl.current_vat,
          'current_inc_vat', rl.current_inc_vat
        )
        order by
          nullif(btrim(coalesce(rl.invoice_number,'')),'') nulls last,
          rl.invoice_id
      ),
      '[]'::jsonb
    )
  into v_lines
  from public.id_consolidation_run_lines rl
  left join public.invoices i
    on i.id = rl.invoice_id
  left join public.clients c
    on c.id = i.client_id
  where rl.id_ref = p_id_ref;

  return jsonb_build_object(
    'run', jsonb_build_object(
      'id_ref', v_run.id_ref,
      'state', case when v_run.bank_uploaded_at_utc is null then 'DRAFT' else 'COMMITTED' end,
      'created_at_utc', v_run.created_at_utc,
      'created_by_user_id', case when v_run.created_by_user_id is null then null else v_run.created_by_user_id::text end,
      'committed_by_user_id', case when v_run.committed_by_user_id is null then null else v_run.committed_by_user_id::text end,
      'bank_upload_code', v_run.bank_upload_code,
      'bank_uploaded_at_utc', v_run.bank_uploaded_at_utc,
      'note', v_run.note,
      'total_delta_ex_vat', v_run.total_delta_ex_vat,
      'total_delta_vat', v_run.total_delta_vat,
      'total_delta_inc_vat', v_run.total_delta_inc_vat
    ),
    'line_count', jsonb_array_length(v_lines),
    'lines', v_lines
  );
end;
$function$;

-- id_consolidation_runs_list(integer,integer)
CREATE OR REPLACE FUNCTION public.id_consolidation_runs_list(p_limit integer DEFAULT 50, p_offset integer DEFAULT 0)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_limit int := greatest(1, least(coalesce(p_limit,50), 500));
  v_offset int := greatest(coalesce(p_offset,0), 0);
  v_total_count int;
  v_runs jsonb := '[]'::jsonb;
begin
  if to_regclass('public.id_consolidation_runs') is null then
    raise exception 'ID_RUNS_TABLE_MISSING';
  end if;

  -- ✅ Only COMMITTED runs (exclude drafts)
  select count(*)::int into v_total_count
  from public.id_consolidation_runs r
  where r.bank_uploaded_at_utc is not null;

  select
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'id_ref', r.id_ref,
          'state', case when r.bank_uploaded_at_utc is null then 'DRAFT' else 'COMMITTED' end,
          'created_at_utc', r.created_at_utc,
          'created_by_user_id', case when r.created_by_user_id is null then null else r.created_by_user_id::text end,
          'committed_by_user_id', case when r.committed_by_user_id is null then null else r.committed_by_user_id::text end,
          'total_delta_ex_vat', r.total_delta_ex_vat,
          'total_delta_vat', r.total_delta_vat,
          'total_delta_inc_vat', r.total_delta_inc_vat,
          'bank_upload_code', r.bank_upload_code,
          'bank_uploaded_at_utc', r.bank_uploaded_at_utc,
          'note', r.note
        )
        order by r.created_at_utc desc, r.id_ref desc
      ),
      '[]'::jsonb
    )
  into v_runs
  from (
    select r0.*
    from public.id_consolidation_runs r0
    where r0.bank_uploaded_at_utc is not null
    order by r0.created_at_utc desc, r0.id_ref desc
    limit v_limit offset v_offset
  ) r;

  return jsonb_build_object(
    'total_count', coalesce(v_total_count,0),
    'limit', v_limit,
    'offset', v_offset,
    'runs', v_runs
  );
end;
$function$;

-- id_ledger_list(integer,integer,text[],uuid,text,boolean,boolean)
CREATE OR REPLACE FUNCTION public.id_ledger_list(p_limit integer DEFAULT 50, p_offset integer DEFAULT 0, p_status text[] DEFAULT NULL::text[], p_client_id uuid DEFAULT NULL::uuid, p_search text DEFAULT NULL::text, p_only_reportable boolean DEFAULT false, p_only_changed boolean DEFAULT false)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_limit int := greatest(1, least(coalesce(p_limit, 50), 500));
  v_offset int := greatest(coalesce(p_offset, 0), 0);

  v_statuses text[] := null;
  v_search text := nullif(btrim(coalesce(p_search, '')), '');

  v_total_count int := 0;
  v_rows jsonb := '[]'::jsonb;
begin
  if to_regclass('public.id_invoice_ledger') is null then
    raise exception 'ID_LEDGER_MISSING';
  end if;

  -- Normalise status filter to uppercase trimmed values (ignore blanks)
  if p_status is not null then
    select
      array_agg(upper(btrim(x)) order by upper(btrim(x)))
    into v_statuses
    from unnest(p_status) as x
    where nullif(btrim(coalesce(x, '')), '') is not null;

    if v_statuses is not null and array_length(v_statuses, 1) = 0 then
      v_statuses := null;
    end if;
  end if;

  with base as (
    select
      l.invoice_id,
      l.invoice_number,
      l.invoice_status,
      l.invoice_type,

      l.current_ex_vat,
      l.current_vat,
      l.current_inc_vat,

      l.last_reported_ex_vat,
      l.last_reported_vat,
      l.last_reported_inc_vat,

      l.updated_at_utc,

      i.client_id as client_id,
      c.name as client_name,

      i.issued_at_utc,
      i.due_at_utc,
      i.paid_at_utc,
      i.status_date_utc,
      i.credit_note_created_at_utc,

      coalesce(nullif(btrim(coalesce(l.invoice_number, '')), ''), i.invoice_no) as effective_invoice_number,
      coalesce(nullif(btrim(coalesce(l.invoice_status, '')), ''), i.status::text) as effective_invoice_status,
      coalesce(nullif(btrim(coalesce(l.invoice_type, '')), ''), i.type::text) as effective_invoice_type,

      coalesce(i.issued_at_utc, i.status_date_utc, l.updated_at_utc) as sort_ts
    from public.id_invoice_ledger l
    left join public.invoices i
      on i.id = l.invoice_id
    left join public.clients c
      on c.id = i.client_id
  ),
  calc as (
    select
      b.*,

      (upper(coalesce(b.effective_invoice_status, '')) = 'ON_HOLD') as is_on_hold,

      (case
        when upper(coalesce(b.effective_invoice_status, '')) = 'ON_HOLD' then 0::numeric(12,2)
        else coalesce(b.current_ex_vat, 0)::numeric(12,2)
      end) as reportable_current_ex_vat,

      (case
        when upper(coalesce(b.effective_invoice_status, '')) = 'ON_HOLD' then 0::numeric(12,2)
        else coalesce(b.current_vat, 0)::numeric(12,2)
      end) as reportable_current_vat,

      (case
        when upper(coalesce(b.effective_invoice_status, '')) = 'ON_HOLD' then 0::numeric(12,2)
        else coalesce(b.current_inc_vat, 0)::numeric(12,2)
      end) as reportable_current_inc_vat
    from base b
  ),
  filtered0 as (
    select
      c.*,

      (c.reportable_current_ex_vat - coalesce(c.last_reported_ex_vat, 0)::numeric(12,2))::numeric(12,2) as delta_ex_vat,
      (c.reportable_current_vat - coalesce(c.last_reported_vat, 0)::numeric(12,2))::numeric(12,2) as delta_vat,
      (c.reportable_current_inc_vat - coalesce(c.last_reported_inc_vat, 0)::numeric(12,2))::numeric(12,2) as delta_inc_vat,

      (case when c.is_on_hold then 'NON_REPORTABLE' else 'REPORTABLE' end) as line_kind,
      (case when c.is_on_hold then 'ON_HOLD' else null end) as non_reportable_reason
    from calc c
    where
      (v_statuses is null or upper(coalesce(c.effective_invoice_status, '')) = any(v_statuses))
      and (p_client_id is null or c.client_id = p_client_id)
      and (
        v_search is null
        or coalesce(c.effective_invoice_number, '') ilike ('%' || v_search || '%')
        or coalesce(c.client_name, '') ilike ('%' || v_search || '%')
      )
      and (
        coalesce(p_only_reportable, false) = false
        or upper(coalesce(c.effective_invoice_status, '')) <> 'ON_HOLD'
      )
  ),
  filtered as (
    select
      f0.*
    from filtered0 f0
    where
      coalesce(p_only_changed, false) = false
      or (
        f0.delta_ex_vat <> 0::numeric(12,2)
        or f0.delta_vat <> 0::numeric(12,2)
        or f0.delta_inc_vat <> 0::numeric(12,2)
      )
  ),
  total as (
    select count(*)::int as total_count
    from filtered f
  ),
  page as (
    select
      f.*
    from filtered f
    order by
      f.sort_ts desc nulls last,
      nullif(btrim(coalesce(f.effective_invoice_number, '')), '') desc nulls last,
      f.invoice_id desc
    limit v_limit offset v_offset
  )
  select
    coalesce((select t.total_count from total t), 0),
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'invoice_id', p.invoice_id::text,
          'invoice_number', p.effective_invoice_number,
          'invoice_status', p.effective_invoice_status,
          'invoice_type', p.effective_invoice_type,

          'client_id', case when p.client_id is null then null else p.client_id::text end,
          'client_name', p.client_name,

          'issued_at_utc', p.issued_at_utc,
          'due_at_utc', p.due_at_utc,
          'paid_at_utc', p.paid_at_utc,
          'status_date_utc', p.status_date_utc,
          'credit_note_created_at_utc', p.credit_note_created_at_utc,

          'updated_at_utc', p.updated_at_utc,

          'current_ex_vat', coalesce(p.current_ex_vat, 0)::numeric(12,2),
          'current_vat', coalesce(p.current_vat, 0)::numeric(12,2),
          'current_inc_vat', coalesce(p.current_inc_vat, 0)::numeric(12,2),

          'last_reported_ex_vat', coalesce(p.last_reported_ex_vat, 0)::numeric(12,2),
          'last_reported_vat', coalesce(p.last_reported_vat, 0)::numeric(12,2),
          'last_reported_inc_vat', coalesce(p.last_reported_inc_vat, 0)::numeric(12,2),

          'reportable_current_ex_vat', p.reportable_current_ex_vat,
          'reportable_current_vat', p.reportable_current_vat,
          'reportable_current_inc_vat', p.reportable_current_inc_vat,

          'delta_ex_vat', p.delta_ex_vat,
          'delta_vat', p.delta_vat,
          'delta_inc_vat', p.delta_inc_vat,

          'line_kind', p.line_kind,
          'non_reportable_reason', p.non_reportable_reason
        )
        order by
          p.sort_ts desc nulls last,
          nullif(btrim(coalesce(p.effective_invoice_number, '')), '') desc nulls last,
          p.invoice_id desc
      ),
      '[]'::jsonb
    )
  into v_total_count, v_rows
  from page p;

  return jsonb_build_object(
    'ok', true,
    'total_count', v_total_count,
    'limit', v_limit,
    'offset', v_offset,
    'rows', v_rows
  );
end;
$function$;

-- id_ledger_list(integer,integer,text[],uuid,text,boolean)
CREATE OR REPLACE FUNCTION public.id_ledger_list(p_limit integer DEFAULT 50, p_offset integer DEFAULT 0, p_status text[] DEFAULT NULL::text[], p_client_id uuid DEFAULT NULL::uuid, p_search text DEFAULT NULL::text, p_only_reportable boolean DEFAULT false)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_limit int := greatest(1, least(coalesce(p_limit, 50), 500));
  v_offset int := greatest(coalesce(p_offset, 0), 0);

  v_statuses text[] := null;
  v_search text := nullif(btrim(coalesce(p_search, '')), '');

  v_total_count int := 0;
  v_rows jsonb := '[]'::jsonb;
begin
  if to_regclass('public.id_invoice_ledger') is null then
    raise exception 'ID_LEDGER_MISSING';
  end if;

  -- Normalise status filter to uppercase trimmed values (ignore blanks)
  if p_status is not null then
    select
      array_agg(upper(btrim(x)) order by upper(btrim(x)))
    into v_statuses
    from unnest(p_status) as x
    where nullif(btrim(coalesce(x, '')), '') is not null;

    if v_statuses is not null and array_length(v_statuses, 1) = 0 then
      v_statuses := null;
    end if;
  end if;

  with base as (
    select
      l.invoice_id,
      l.invoice_number,
      l.invoice_status,
      l.invoice_type,

      l.current_ex_vat,
      l.current_vat,
      l.current_inc_vat,

      l.last_reported_ex_vat,
      l.last_reported_vat,
      l.last_reported_inc_vat,

      l.updated_at_utc,

      i.client_id as client_id,
      c.name as client_name,

      i.issued_at_utc,
      i.due_at_utc,
      i.paid_at_utc,
      i.status_date_utc,
      i.credit_note_created_at_utc,

      coalesce(nullif(btrim(coalesce(l.invoice_number, '')), ''), i.invoice_no) as effective_invoice_number,
      coalesce(nullif(btrim(coalesce(l.invoice_status, '')), ''), i.status::text) as effective_invoice_status,
      coalesce(nullif(btrim(coalesce(l.invoice_type, '')), ''), i.type::text) as effective_invoice_type,

      coalesce(i.issued_at_utc, i.status_date_utc, l.updated_at_utc) as sort_ts
    from public.id_invoice_ledger l
    left join public.invoices i
      on i.id = l.invoice_id
    left join public.clients c
      on c.id = i.client_id
  ),
  calc as (
    select
      b.*,

      (upper(coalesce(b.effective_invoice_status, '')) = 'ON_HOLD') as is_on_hold,

      (case
        when upper(coalesce(b.effective_invoice_status, '')) = 'ON_HOLD' then 0::numeric(12,2)
        else coalesce(b.current_ex_vat, 0)::numeric(12,2)
      end) as reportable_current_ex_vat,

      (case
        when upper(coalesce(b.effective_invoice_status, '')) = 'ON_HOLD' then 0::numeric(12,2)
        else coalesce(b.current_vat, 0)::numeric(12,2)
      end) as reportable_current_vat,

      (case
        when upper(coalesce(b.effective_invoice_status, '')) = 'ON_HOLD' then 0::numeric(12,2)
        else coalesce(b.current_inc_vat, 0)::numeric(12,2)
      end) as reportable_current_inc_vat
    from base b
  ),
  filtered as (
    select
      c.*,

      (c.reportable_current_ex_vat - coalesce(c.last_reported_ex_vat, 0)::numeric(12,2))::numeric(12,2) as delta_ex_vat,
      (c.reportable_current_vat - coalesce(c.last_reported_vat, 0)::numeric(12,2))::numeric(12,2) as delta_vat,
      (c.reportable_current_inc_vat - coalesce(c.last_reported_inc_vat, 0)::numeric(12,2))::numeric(12,2) as delta_inc_vat,

      (case when c.is_on_hold then 'NON_REPORTABLE' else 'REPORTABLE' end) as line_kind,
      (case when c.is_on_hold then 'ON_HOLD' else null end) as non_reportable_reason
    from calc c
    where
      (v_statuses is null or upper(coalesce(c.effective_invoice_status, '')) = any(v_statuses))
      and (p_client_id is null or c.client_id = p_client_id)
      and (
        v_search is null
        or coalesce(c.effective_invoice_number, '') ilike ('%' || v_search || '%')
        or coalesce(c.client_name, '') ilike ('%' || v_search || '%')
      )
      and (
        coalesce(p_only_reportable, false) = false
        or upper(coalesce(c.effective_invoice_status, '')) <> 'ON_HOLD'
      )
  ),
  total as (
    select count(*)::int as total_count
    from filtered f
  ),
  page as (
    select
      f.*
    from filtered f
    order by
      f.sort_ts desc nulls last,
      nullif(btrim(coalesce(f.effective_invoice_number, '')), '') desc nulls last,
      f.invoice_id desc
    limit v_limit offset v_offset
  )
  select
    coalesce((select t.total_count from total t), 0),
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'invoice_id', p.invoice_id::text,
          'invoice_number', p.effective_invoice_number,
          'invoice_status', p.effective_invoice_status,
          'invoice_type', p.effective_invoice_type,

          'client_id', case when p.client_id is null then null else p.client_id::text end,
          'client_name', p.client_name,

          'issued_at_utc', p.issued_at_utc,
          'due_at_utc', p.due_at_utc,
          'paid_at_utc', p.paid_at_utc,
          'status_date_utc', p.status_date_utc,
          'credit_note_created_at_utc', p.credit_note_created_at_utc,

          'updated_at_utc', p.updated_at_utc,

          'current_ex_vat', coalesce(p.current_ex_vat, 0)::numeric(12,2),
          'current_vat', coalesce(p.current_vat, 0)::numeric(12,2),
          'current_inc_vat', coalesce(p.current_inc_vat, 0)::numeric(12,2),

          'last_reported_ex_vat', coalesce(p.last_reported_ex_vat, 0)::numeric(12,2),
          'last_reported_vat', coalesce(p.last_reported_vat, 0)::numeric(12,2),
          'last_reported_inc_vat', coalesce(p.last_reported_inc_vat, 0)::numeric(12,2),

          'reportable_current_ex_vat', p.reportable_current_ex_vat,
          'reportable_current_vat', p.reportable_current_vat,
          'reportable_current_inc_vat', p.reportable_current_inc_vat,

          'delta_ex_vat', p.delta_ex_vat,
          'delta_vat', p.delta_vat,
          'delta_inc_vat', p.delta_inc_vat,

          'line_kind', p.line_kind,
          'non_reportable_reason', p.non_reportable_reason
        )
        order by
          p.sort_ts desc nulls last,
          nullif(btrim(coalesce(p.effective_invoice_number, '')), '') desc nulls last,
          p.invoice_id desc
      ),
      '[]'::jsonb
    )
  into v_total_count, v_rows
  from page p;

  return jsonb_build_object(
    'ok', true,
    'total_count', v_total_count,
    'limit', v_limit,
    'offset', v_offset,
    'rows', v_rows
  );
end;
$function$;

-- id_ledger_recompute_and_sync_invoice(uuid)
CREATE OR REPLACE FUNCTION public.id_ledger_recompute_and_sync_invoice(p_invoice_id uuid)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
begin
  -- Canonical totals recompute; this also clears invoice_pdf_r2_key. :contentReference[oaicite:3]{index=3}
  begin
    perform public.invoice_recompute_totals(p_invoice_id);
  exception when others then
    -- If invoice missing or recompute fails, fall back to a safe ledger upsert with zeros.
    -- (We do NOT raise: ledger must not break invoice_lines operations.)
    perform public.id_ledger_upsert_from_invoice_row(p_invoice_id, true, null, null, null);
    return;
  end;

  perform public.id_ledger_upsert_from_invoice_row(p_invoice_id, false, null, null, null);
end;
$function$;

-- id_ledger_sync_invoice_metadata(uuid)
CREATE OR REPLACE FUNCTION public.id_ledger_sync_invoice_metadata(p_invoice_id uuid)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
begin
  perform public.id_ledger_upsert_from_invoice_row(p_invoice_id, false, null, null, null);
end;
$function$;

-- id_ledger_upsert_from_invoice_row(uuid,boolean,text,text,text)
CREATE OR REPLACE FUNCTION public.id_ledger_upsert_from_invoice_row(p_invoice_id uuid, p_set_zero boolean DEFAULT false, p_invoice_no text DEFAULT NULL::text, p_status_text text DEFAULT NULL::text, p_type_text text DEFAULT NULL::text)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_inv record;

  v_invoice_no text;
  v_status_text text;
  v_type_text text;

  v_ex numeric := 0;
  v_vat numeric := 0;
  v_inc numeric := 0;
begin
  -- Defensive: avoid breaking triggers if called with NULL
  if p_invoice_id is null then
    return;
  end if;

  -- Prefer reading the current invoices row when present (normal path).
  select
    i.invoice_no,
    i.status::text as status_text,
    i.type::text as type_text,
    coalesce(i.subtotal_ex_vat,0)::numeric as subtotal_ex_vat,
    coalesce(i.vat_amount,0)::numeric as vat_amount,
    coalesce(i.total_inc_vat,0)::numeric as total_inc_vat
  into v_inv
  from public.invoices i
  where i.id = p_invoice_id
  limit 1;

  if found then
    v_invoice_no := nullif(btrim(coalesce(v_inv.invoice_no, '')), '');
    v_status_text := nullif(btrim(coalesce(v_inv.status_text, '')), '');
    v_type_text := nullif(btrim(coalesce(v_inv.type_text, '')), '');

    if p_set_zero then
      v_ex := 0; v_vat := 0; v_inc := 0;
    else
      v_ex := coalesce(v_inv.subtotal_ex_vat,0);
      v_vat := coalesce(v_inv.vat_amount,0);
      v_inc := coalesce(v_inv.total_inc_vat,0);

      -- Safety: if a CREDIT_NOTE ever ends up stored as positive totals, force negative.
      -- (If credit notes are already stored as signed totals, this is a no-op.)
      if v_type_text = 'CREDIT_NOTE' then
        if v_ex > 0 then v_ex := -1 * v_ex; end if;
        if v_vat > 0 then v_vat := -1 * v_vat; end if;
        if v_inc > 0 then v_inc := -1 * v_inc; end if;
      end if;
    end if;

  else
    -- Invoice row not found (e.g. already deleted): use provided snapshots and zero totals.
    v_invoice_no := nullif(btrim(coalesce(p_invoice_no, '')), '');
    v_status_text := nullif(btrim(coalesce(p_status_text, '')), '');
    v_type_text := nullif(btrim(coalesce(p_type_text, '')), '');

    v_ex := 0; v_vat := 0; v_inc := 0;
  end if;

  insert into public.id_invoice_ledger (
    invoice_id,
    invoice_number,
    invoice_status,
    invoice_type,
    current_ex_vat,
    current_vat,
    current_inc_vat,
    updated_at_utc
  )
  values (
    p_invoice_id,
    v_invoice_no,
    v_status_text,
    v_type_text,
    round(coalesce(v_ex,0)::numeric,2),
    round(coalesce(v_vat,0)::numeric,2),
    round(coalesce(v_inc,0)::numeric,2),
    now()
  )
  on conflict (invoice_id) do update
  set
    invoice_number   = excluded.invoice_number,
    invoice_status   = excluded.invoice_status,
    invoice_type     = excluded.invoice_type,
    current_ex_vat   = excluded.current_ex_vat,
    current_vat      = excluded.current_vat,
    current_inc_vat  = excluded.current_inc_vat,
    updated_at_utc   = excluded.updated_at_utc;

end;
$function$;

-- import_apply_finalize_after_tsfin_v1(uuid,uuid,uuid[],text,jsonb,timestamp with time zone,integer)
CREATE OR REPLACE FUNCTION public.import_apply_finalize_after_tsfin_v1(p_operation_id uuid, p_actor_user_id uuid, p_expected_timesheet_ids uuid[], p_expected_preflight_fingerprint text DEFAULT NULL::text, p_response_patch_json jsonb DEFAULT '{}'::jsonb, p_now_utc timestamp with time zone DEFAULT now(), p_max_timesheets integer DEFAULT 100)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
DECLARE
  v_now timestamptz := COALESCE(p_now_utc, now());
  v_expected_preflight_fingerprint text :=
    NULLIF(BTRIM(COALESCE(p_expected_preflight_fingerprint, '')), '');
  v_response_patch jsonb := COALESCE(p_response_patch_json, '{}'::jsonb);

  v_operation public.import_apply_operations%ROWTYPE;
  v_before_operation jsonb;
  v_expected_ids uuid[] := ARRAY[]::uuid[];
  v_raw_expected_count integer := 0;
  v_expected_count integer := 0;

  v_preflight jsonb;
  v_current_preflight_fingerprint text;
  v_member_count integer := 0;
  v_current_tsfin_count integer := 0;
  v_core_financial_ready_count integer := 0;
  v_financial_ready_count integer := 0;
  v_authorised_count integer := 0;
  v_unauthorised_count integer := 0;
  v_stale_count integer := 0;
  v_blocking_batch_count integer := 0;
  v_pair_scope_error_count integer := 0;
  v_pair_anchor_mismatch_count integer := 0;
  v_historical_anchor_mismatch_count integer := 0;

  v_authorisation_items jsonb := '[]'::jsonb;
  v_timesheet_rows jsonb := '[]'::jsonb;
  v_errors jsonb := '[]'::jsonb;

  v_new_state text;
  v_result_code text;
  v_continuation_required boolean := false;
  v_complete boolean := false;
  v_state_changed boolean := false;
BEGIN
  IF p_operation_id IS NULL THEN
    RAISE EXCEPTION 'IMPORT_FINALIZE_OPERATION_ID_REQUIRED'
      USING ERRCODE = '22023';
  END IF;

  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'IMPORT_FINALIZE_ACTOR_REQUIRED'
      USING ERRCODE = '22023';
  END IF;

  IF p_max_timesheets < 1 OR p_max_timesheets > 100 THEN
    RAISE EXCEPTION 'IMPORT_FINALIZE_MAX_TIMESHEETS_OUT_OF_RANGE'
      USING ERRCODE = '22023';
  END IF;

  IF jsonb_typeof(v_response_patch) <> 'object' THEN
    RAISE EXCEPTION 'IMPORT_FINALIZE_RESPONSE_PATCH_MUST_BE_OBJECT'
      USING ERRCODE = '22023';
  END IF;

  IF octet_length(v_response_patch::text) > 131072 THEN
    RAISE EXCEPTION 'IMPORT_FINALIZE_RESPONSE_PATCH_TOO_LARGE'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object(
              'max_bytes', 131072
            )::text;
  END IF;

  IF v_expected_preflight_fingerprint IS NOT NULL
     AND char_length(v_expected_preflight_fingerprint) > 256 THEN
    RAISE EXCEPTION 'IMPORT_FINALIZE_PREFLIGHT_FINGERPRINT_TOO_LONG'
      USING ERRCODE = '22023';
  END IF;

  v_raw_expected_count := COALESCE(cardinality(p_expected_timesheet_ids), 0);
  IF v_raw_expected_count < 1 OR v_raw_expected_count > p_max_timesheets THEN
    RAISE EXCEPTION 'IMPORT_FINALIZE_RAW_TIMESHEET_COUNT_OUT_OF_RANGE'
      USING ERRCODE = '22023';
  END IF;

  SELECT COALESCE(
    array_agg(DISTINCT expected_id ORDER BY expected_id),
    ARRAY[]::uuid[]
  )
  INTO v_expected_ids
  FROM unnest(COALESCE(
    p_expected_timesheet_ids,
    ARRAY[]::uuid[]
  )) AS expected_timesheet(expected_id)
  WHERE expected_id IS NOT NULL;

  v_expected_count := cardinality(v_expected_ids);

  IF v_expected_count < 1 OR v_expected_count > p_max_timesheets THEN
    RAISE EXCEPTION 'IMPORT_FINALIZE_TIMESHEET_COUNT_OUT_OF_RANGE'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object(
              'expected_count', v_expected_count,
              'min', 1,
              'max', p_max_timesheets
            )::text;
  END IF;

  SELECT operation_row.*
  INTO v_operation
  FROM public.import_apply_operations AS operation_row
  WHERE operation_row.id = p_operation_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'IMPORT_FINALIZE_OPERATION_NOT_FOUND'
      USING ERRCODE = 'P0002';
  END IF;

  IF v_operation.actor_user_id IS DISTINCT FROM p_actor_user_id THEN
    RAISE EXCEPTION 'IMPORT_FINALIZE_OPERATION_ACTOR_MISMATCH'
      USING ERRCODE = '42501';
  END IF;

  IF v_operation.state = 'COMPLETE' THEN
    IF COALESCE(v_operation.response_json -> 'expected_timesheet_ids','[]'::jsonb)
         IS DISTINCT FROM to_jsonb(v_expected_ids)
       OR (
         v_expected_preflight_fingerprint IS NOT NULL
         AND COALESCE(
           v_operation.response_json ->> 'preflight_fingerprint',
           v_operation.response_json ->> 'expected_preflight_fingerprint'
         ) IS DISTINCT FROM v_expected_preflight_fingerprint
       ) THEN
      RAISE EXCEPTION 'IMPORT_FINALIZE_COMPLETE_REPLAY_CONFLICT'
        USING ERRCODE = '40001',
              DETAIL = jsonb_build_object(
                'operation_id', p_operation_id,
                'expected_timesheet_ids', to_jsonb(v_expected_ids)
              )::text;
    END IF;
    RETURN jsonb_build_object(
      'ok', true,
      'replay', true,
      'complete', true,
      'continuation_required', false,
      'operation_id', p_operation_id::text,
      'state', v_operation.state,
      'result_code', 'IDEMPOTENT_REPLAY',
      'response_json', v_operation.response_json,
      'finalised_at_utc', v_operation.finalised_at_utc
    );
  END IF;

  IF v_operation.state NOT IN (
    'PREPARED',
    'SOURCE_COMMITTED_TSFIN_PENDING',
    'FINANCIALISED_PENDING_FINALISATION'
  ) THEN
    RAISE EXCEPTION 'IMPORT_FINALIZE_OPERATION_STATE_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'operation_id', p_operation_id::text,
              'state', v_operation.state
            )::text;
  END IF;

  v_before_operation := to_jsonb(v_operation);

  v_preflight := public.import_timesheet_financial_preflight_v1(
    v_expected_ids,
    'IMPORT_FINALIZE_AFTER_TSFIN',
    p_actor_user_id,
    '{}'::jsonb,
    true,
    p_max_timesheets
  );

  v_current_preflight_fingerprint :=
    v_preflight ->> 'preflight_fingerprint';

  IF v_expected_preflight_fingerprint IS NOT NULL
     AND v_current_preflight_fingerprint
         IS DISTINCT FROM v_expected_preflight_fingerprint THEN
    v_errors := v_errors || jsonb_build_array(
      jsonb_build_object(
        'code', 'IMPORT_FINALIZE_PREFLIGHT_FINGERPRINT_MISMATCH',
        'expected_preflight_fingerprint',
          v_expected_preflight_fingerprint,
        'actual_preflight_fingerprint',
          v_current_preflight_fingerprint
      )
    );
  END IF;


  IF jsonb_typeof(v_preflight -> 'errors') = 'array'
     AND jsonb_array_length(v_preflight -> 'errors') > 0 THEN
    v_errors := v_errors || (v_preflight -> 'errors');
  END IF;

  v_member_count :=
    COALESCE((v_preflight ->> 'member_count')::integer, 0);

  IF EXISTS (
    SELECT 1
    FROM unnest(v_expected_ids) AS expected_scope(timesheet_id)
    WHERE NOT EXISTS (
      SELECT 1
      FROM jsonb_array_elements_text(
        COALESCE(
          v_preflight -> 'member_timesheet_ids',
          '[]'::jsonb
        )
      ) AS actual_scope(timesheet_id_text)
      WHERE actual_scope.timesheet_id_text =
            expected_scope.timesheet_id::text
    )
  ) THEN
    v_errors := v_errors || jsonb_build_array(
      jsonb_build_object(
        'code', 'IMPORT_FINALIZE_EXPECTED_SCOPE_NOT_IN_PREFLIGHT',
        'expected_timesheet_ids', to_jsonb(v_expected_ids),
        'actual_member_timesheet_ids',
          COALESCE(
            v_preflight -> 'member_timesheet_ids',
            '[]'::jsonb
          )
      )
    );
  END IF;

  v_blocking_batch_count :=
    COALESCE(
      (v_preflight ->> 'blocking_batch_count')::integer,
      0
    );

  IF v_blocking_batch_count > 0 THEN
    v_errors := v_errors || jsonb_build_array(
      jsonb_build_object(
        'code', 'BLOCKED_ACTIVE_PAY_DRAFT',
        'blocking_batches',
          COALESCE(v_preflight -> 'blocking_batches', '[]'::jsonb)
      )
    );
  END IF;

  WITH current_state_raw AS (
    SELECT
      timesheet_row.timesheet_id,
      timesheet_row.booking_id,
      timesheet_row.version,
      timesheet_row.correction_id,
      timesheet_row.correction_kind,
      timesheet_row.parent_timesheet_id,
      timesheet_row.authorised_at_server,

      current_financial.id AS tsfin_id,
      current_financial.processing_status,
      current_financial.is_stale,
      current_financial.stale_reason,
      current_financial.candidate_id,
      current_financial.client_id,
      current_financial.pay_method,
      current_financial.has_rate_issue,
      current_financial.has_pay_channel_issue,
      current_financial.authorised_at_utc,
      current_financial.computed_at_utc,
      current_financial.policy_snapshot_json,
      current_financial.rate_source_refs_json,
      current_financial.pay_vat_rate_pct_snapshot,

      chain_scope.chain_json ->> 'root_timesheet_id'
        AS root_timesheet_id_text,
      COALESCE(
        (chain_scope.chain_json ->>
          'correction_financials_policy_envelope_required')::boolean,
        false
      ) AS correction_financials_policy_envelope_required,
      chain_scope.chain_json -> 'correction_financials_policy_envelope'
        AS expected_correction_financials_policy_envelope,
      NULLIF(
        BTRIM(COALESCE(
          chain_scope.chain_json ->>
            'correction_financials_policy_envelope_fingerprint',
          ''
        )),
        ''
      ) AS expected_correction_financials_policy_envelope_fingerprint,
      policy_leg.expected_leg AS expected_correction_policy_leg,
      policy_leg.expected_leg ->> 'leg_fingerprint'
        AS expected_correction_leg_fingerprint,
      policy_leg.expected_leg #>> '{tsfin_policy,tsfin_policy_fingerprint}'
        AS expected_correction_tsfin_policy_fingerprint,
      policy_leg.expected_leg #>> '{invoice_policy,invoice_policy_fingerprint}'
        AS expected_correction_invoice_policy_fingerprint,

      COALESCE(
        current_financial.policy_snapshot_json #>
          '{correction_financials_policy_envelope}',
        current_financial.rate_source_refs_json #>
          '{correction_financials_policy_envelope}'
      ) AS actual_correction_financials_policy_envelope,
      NULLIF(
        BTRIM(COALESCE(
          current_financial.policy_snapshot_json ->>
            'correction_financials_policy_envelope_fingerprint',
          current_financial.policy_snapshot_json #>>
            '{correction_financials_policy_envelope,envelope_fingerprint}',
          current_financial.rate_source_refs_json ->>
            'correction_financials_policy_envelope_fingerprint',
          current_financial.rate_source_refs_json #>>
            '{correction_financials_policy_envelope,envelope_fingerprint}',
          ''
        )),
        ''
      ) AS actual_correction_financials_policy_envelope_fingerprint,
      COALESCE(
        current_financial.policy_snapshot_json ->> 'correction_leg_fingerprint',
        current_financial.rate_source_refs_json ->> 'correction_leg_fingerprint'
      ) AS actual_correction_leg_fingerprint,
      COALESCE(
        current_financial.policy_snapshot_json ->> 'correction_tsfin_policy_fingerprint',
        current_financial.rate_source_refs_json ->> 'correction_tsfin_policy_fingerprint'
      ) AS actual_correction_tsfin_policy_fingerprint,
      COALESCE(
        current_financial.policy_snapshot_json ->> 'correction_invoice_policy_fingerprint',
        current_financial.rate_source_refs_json ->> 'correction_invoice_policy_fingerprint'
      ) AS actual_correction_invoice_policy_fingerprint,

      current_financial.policy_snapshot_json ->> 'erni_pct'
        AS actual_erni_pct_text,
      current_financial.policy_snapshot_json ->> 'apply_erni_to'
        AS actual_apply_erni_to,
      COALESCE(
        current_financial.pay_vat_rate_pct_snapshot::text,
        current_financial.policy_snapshot_json ->> 'vat_rate_pct'
      ) AS actual_pay_vat_rate_pct_text,

      v_preflight #>> ARRAY[
        'timesheet_signatures',
        timesheet_row.timesheet_id::text
      ] AS row_signature
    FROM public.timesheets AS timesheet_row
    LEFT JOIN LATERAL (
      SELECT financial_row.*
      FROM public.timesheets_financials AS financial_row
      WHERE financial_row.timesheet_id = timesheet_row.timesheet_id
        AND financial_row.is_current = true
      ORDER BY financial_row.computed_at_utc DESC, financial_row.id DESC
      LIMIT 1
    ) AS current_financial
      ON true
    LEFT JOIN LATERAL (
      SELECT public.timesheet_correction_chain_scope_v1(
        timesheet_row.timesheet_id, false, 32, p_max_timesheets
      ) AS chain_json
    ) AS chain_scope ON true
    LEFT JOIN LATERAL (
      SELECT public._ctms_correction_policy_leg_read_v1(
        timesheet_row.timesheet_id
      ) AS expected_leg
      WHERE COALESCE(
        (chain_scope.chain_json ->>
          'correction_financials_policy_envelope_required')::boolean,
        false
      ) = true
    ) AS policy_leg ON true
    WHERE timesheet_row.timesheet_id = ANY(v_expected_ids)
  ),
  current_state AS (
    SELECT
      raw_state.*,
      CASE
        WHEN jsonb_typeof(
          raw_state.actual_correction_financials_policy_envelope
        ) = 'object'
          THEN encode(
            extensions.digest(
              convert_to(
                (
                  raw_state.actual_correction_financials_policy_envelope
                  - 'envelope_fingerprint'
                )::text,
                'UTF8'
              ),
              'sha256'
            ),
            'hex'
          )
        ELSE NULL::text
      END AS recomputed_correction_financials_policy_envelope_fingerprint,
      CASE
        WHEN raw_state.actual_erni_pct_text ~
          '^[+-]?([0-9]+([.][0-9]+)?|[.][0-9]+)$'
          THEN raw_state.actual_erni_pct_text::numeric
        ELSE NULL::numeric
      END AS actual_erni_pct,
      CASE
        WHEN raw_state.actual_pay_vat_rate_pct_text ~
          '^[+-]?([0-9]+([.][0-9]+)?|[.][0-9]+)$'
          THEN raw_state.actual_pay_vat_rate_pct_text::numeric
        ELSE NULL::numeric
      END AS actual_pay_vat_rate_pct,
      CASE
        WHEN raw_state.expected_correction_policy_leg #>>
          '{tsfin_policy,erni_pct}' ~
          '^[+-]?([0-9]+([.][0-9]+)?|[.][0-9]+)$'
          THEN (
            raw_state.expected_correction_policy_leg #>>
              '{tsfin_policy,erni_pct}'
          )::numeric
        ELSE NULL::numeric
      END AS expected_erni_pct,
      raw_state.expected_correction_policy_leg #>>
        '{tsfin_policy,apply_erni_to}' AS expected_apply_erni_to,
      CASE
        WHEN raw_state.expected_correction_policy_leg #>>
          '{tsfin_policy,applied_pay_vat_rate_pct}' ~
          '^[+-]?([0-9]+([.][0-9]+)?|[.][0-9]+)$'
          THEN (
            raw_state.expected_correction_policy_leg #>>
              '{tsfin_policy,applied_pay_vat_rate_pct}'
          )::numeric
        ELSE NULL::numeric
      END AS expected_pay_vat_rate_pct
    FROM current_state_raw AS raw_state
  ),
  readiness AS (
    SELECT
      state_row.*,
      (
        state_row.tsfin_id IS NOT NULL
        AND COALESCE(state_row.is_stale, false) = false
        AND state_row.candidate_id IS NOT NULL
        AND state_row.client_id IS NOT NULL
        AND NULLIF(BTRIM(COALESCE(
          state_row.pay_method,
          ''
        )), '') IS NOT NULL
        AND COALESCE(state_row.has_rate_issue, false) = false
        AND COALESCE(
          state_row.has_pay_channel_issue,
          false
        ) = false
        AND state_row.processing_status NOT IN (
          'UNASSIGNED'::public.ts_fin_processing_status_enum,
          'CLIENT_UNRESOLVED'::public.ts_fin_processing_status_enum,
          'RATE_MISSING'::public.ts_fin_processing_status_enum,
          'PAY_CHANNEL_MISSING'::public.ts_fin_processing_status_enum
        )
      ) AS core_financial_ready,
      (
        NOT state_row.correction_financials_policy_envelope_required
        OR (
          state_row.expected_correction_financials_policy_envelope_fingerprint
            IS NOT NULL
          AND jsonb_typeof(
            state_row.expected_correction_financials_policy_envelope
          ) = 'object'
          AND jsonb_typeof(
            state_row.actual_correction_financials_policy_envelope
          ) = 'object'
          AND state_row.actual_correction_financials_policy_envelope =
              state_row.expected_correction_financials_policy_envelope
          AND state_row.actual_correction_financials_policy_envelope_fingerprint =
              state_row.expected_correction_financials_policy_envelope_fingerprint
          AND state_row.recomputed_correction_financials_policy_envelope_fingerprint =
              state_row.expected_correction_financials_policy_envelope_fingerprint
          AND state_row.actual_correction_leg_fingerprint =
              state_row.expected_correction_leg_fingerprint
          AND state_row.actual_correction_tsfin_policy_fingerprint =
              state_row.expected_correction_tsfin_policy_fingerprint
          AND state_row.actual_correction_invoice_policy_fingerprint =
              state_row.expected_correction_invoice_policy_fingerprint
          AND state_row.actual_erni_pct
              IS NOT DISTINCT FROM state_row.expected_erni_pct
          AND UPPER(BTRIM(COALESCE(
                state_row.actual_apply_erni_to,
                ''
              ))) =
              UPPER(BTRIM(COALESCE(
                state_row.expected_apply_erni_to,
                ''
              )))
          AND state_row.actual_pay_vat_rate_pct
              IS NOT DISTINCT FROM
                state_row.expected_pay_vat_rate_pct
        )
      ) AS correction_financials_policy_envelope_ready
    FROM current_state AS state_row
  ),
  correction_pair_rollup AS (
    SELECT
      state_row.expected_correction_financials_policy_envelope #>>
        '{operation,operation_id}' AS correction_operation_id,
      min(state_row.correction_id) AS correction_id,
      count(distinct state_row.correction_id)::integer AS correction_id_count,
      min(state_row.expected_correction_financials_policy_envelope ->>
        'correction_shape') AS expected_correction_shape,
      min((state_row.expected_correction_financials_policy_envelope ->>
        'expected_member_count')::integer) AS expected_member_count,
      count(*)::integer AS pair_member_count,
      count(*) FILTER (
        WHERE UPPER(BTRIM(COALESCE(
          state_row.correction_kind,
          ''
        ))) IN ('CHANGED_HOURS_REVERSAL','CANCELLATION_REVERSAL')
      )::integer AS reversal_count,
      count(*) FILTER (
        WHERE UPPER(BTRIM(COALESCE(
          state_row.correction_kind,
          ''
        ))) IN ('CHANGED_HOURS_REPLACEMENT','CANCELLATION_REPLACEMENT')
      )::integer AS replacement_count,
      count(DISTINCT state_row.parent_timesheet_id)::integer
        AS distinct_parent_count,
      count(DISTINCT
        state_row.expected_correction_financials_policy_envelope_fingerprint
      ) FILTER (
        WHERE state_row.correction_financials_policy_envelope_required
      )::integer AS distinct_expected_anchor_count,
      count(DISTINCT
        state_row.actual_correction_financials_policy_envelope_fingerprint
      ) FILTER (
        WHERE state_row.tsfin_id IS NOT NULL
      )::integer AS distinct_actual_anchor_count,
      count(*) FILTER (
        WHERE state_row.correction_financials_policy_envelope_required
          AND state_row.correction_financials_policy_envelope_ready
      )::integer AS matching_anchor_member_count,
      count(*) FILTER (
        WHERE state_row.correction_financials_policy_envelope_required
      )::integer AS required_anchor_member_count
    FROM readiness AS state_row
    WHERE state_row.correction_id IS NOT NULL
       OR state_row.correction_kind IS NOT NULL
    GROUP BY state_row.expected_correction_financials_policy_envelope #>>
      '{operation,operation_id}'
  )
  SELECT
    (SELECT count(*) FROM readiness)::integer,
    (SELECT count(*) FROM readiness WHERE tsfin_id IS NOT NULL)::integer,
    (SELECT count(*) FROM readiness WHERE core_financial_ready)::integer,
    (SELECT count(*) FROM readiness WHERE
      core_financial_ready
      AND correction_financials_policy_envelope_ready
    )::integer,
    (SELECT count(*) FROM readiness WHERE
      authorised_at_server IS NOT NULL
      AND authorised_at_utc IS NOT NULL
    )::integer,
    (SELECT count(*) FROM readiness WHERE
      authorised_at_server IS NULL
      AND authorised_at_utc IS NULL
    )::integer,
    (SELECT count(*) FROM readiness WHERE
      COALESCE(is_stale, false)
    )::integer,
    (SELECT count(*) FROM correction_pair_rollup WHERE
      correction_operation_id IS NULL
      OR correction_id_count <> 1
      OR expected_correction_shape NOT IN ('REVERSAL_ONLY','REVERSAL_REPLACEMENT')
      OR pair_member_count <> expected_member_count
      OR reversal_count <> 1
      OR replacement_count <> CASE
           WHEN expected_correction_shape='REVERSAL_ONLY' THEN 0 ELSE 1 END
      OR distinct_parent_count <> 1
    )::integer,
    (SELECT count(*) FROM correction_pair_rollup WHERE
      required_anchor_member_count <> pair_member_count
      OR distinct_expected_anchor_count <> 1
      OR distinct_actual_anchor_count <> 1
      OR matching_anchor_member_count <>
         required_anchor_member_count
    )::integer,
    (SELECT count(*) FROM readiness WHERE
      correction_financials_policy_envelope_required
      AND NOT correction_financials_policy_envelope_ready
    )::integer,
    COALESCE(
      (
        SELECT jsonb_agg(
          jsonb_strip_nulls(
            jsonb_build_object(
              'timesheet_id', state_row.timesheet_id::text,
              'booking_id', state_row.booking_id,
              'version', state_row.version,
              'root_timesheet_id',
                state_row.root_timesheet_id_text,
              'correction_id', state_row.correction_id,
              'correction_kind', state_row.correction_kind,
              'parent_timesheet_id', CASE
                WHEN state_row.parent_timesheet_id IS NULL THEN NULL
                ELSE state_row.parent_timesheet_id::text
              END,
              'tsfin_id', CASE
                WHEN state_row.tsfin_id IS NULL THEN NULL
                ELSE state_row.tsfin_id::text
              END,
              'processing_status', CASE
                WHEN state_row.processing_status IS NULL THEN NULL
                ELSE state_row.processing_status::text
              END,
              'is_stale', state_row.is_stale,
              'stale_reason', state_row.stale_reason,
              'timesheet_authorised',
                state_row.authorised_at_server IS NOT NULL,
              'tsfin_authorised',
                state_row.authorised_at_utc IS NOT NULL,
              'core_financial_ready',
                state_row.core_financial_ready,
              'correction_financials_policy_envelope_required',
                state_row.correction_financials_policy_envelope_required,
              'correction_financials_policy_envelope_ready',
                state_row.correction_financials_policy_envelope_ready,
              'expected_correction_financials_policy_envelope_fingerprint',
                state_row.expected_correction_financials_policy_envelope_fingerprint,
              'actual_correction_financials_policy_envelope_fingerprint',
                state_row.actual_correction_financials_policy_envelope_fingerprint,
              'recomputed_correction_financials_policy_envelope_fingerprint',
                state_row.recomputed_correction_financials_policy_envelope_fingerprint,
              'expected_erni_pct', state_row.expected_erni_pct,
              'actual_erni_pct', state_row.actual_erni_pct,
              'expected_apply_erni_to',
                state_row.expected_apply_erni_to,
              'actual_apply_erni_to',
                state_row.actual_apply_erni_to,
              'expected_pay_vat_rate_pct',
                state_row.expected_pay_vat_rate_pct,
              'actual_pay_vat_rate_pct',
                state_row.actual_pay_vat_rate_pct,
              'row_signature', state_row.row_signature
            )
          )
          ORDER BY state_row.timesheet_id
        )
        FROM readiness AS state_row
      ),
      '[]'::jsonb
    ),
    COALESCE(
      (
        SELECT jsonb_agg(
          jsonb_strip_nulls(
            jsonb_build_object(
              'timesheet_id', state_row.timesheet_id::text,
              'expected_timesheet_id',
                state_row.timesheet_id::text,
              'expected_row_signature',
                state_row.row_signature,
              'correction_financials_policy_envelope_required',
                state_row.correction_financials_policy_envelope_required,
              'expected_correction_financials_policy_envelope_fingerprint',
                state_row.expected_correction_financials_policy_envelope_fingerprint
            )
          )
          ORDER BY state_row.timesheet_id
        )
        FROM readiness AS state_row
        WHERE state_row.authorised_at_server IS NULL
          AND state_row.authorised_at_utc IS NULL
          AND state_row.core_financial_ready
          AND state_row.correction_financials_policy_envelope_ready
      ),
      '[]'::jsonb
    )
  INTO
    v_member_count,
    v_current_tsfin_count,
    v_core_financial_ready_count,
    v_financial_ready_count,
    v_authorised_count,
    v_unauthorised_count,
    v_stale_count,
    v_pair_scope_error_count,
    v_pair_anchor_mismatch_count,
    v_historical_anchor_mismatch_count,
    v_timesheet_rows,
    v_authorisation_items;

  IF v_member_count <> v_expected_count THEN
    v_errors := v_errors || jsonb_build_array(
      jsonb_build_object(
        'code', 'IMPORT_FINALIZE_TIMESHEET_SCOPE_MISMATCH',
        'expected_count', v_expected_count,
        'actual_count', v_member_count
      )
    );
  END IF;

  IF v_pair_scope_error_count > 0 THEN
    v_errors := v_errors || jsonb_build_array(
      jsonb_build_object(
        'code', 'IMPORT_FINALIZE_CORRECTION_PAIR_INCOMPLETE',
        'pair_error_count', v_pair_scope_error_count
      )
    );
  END IF;

  IF v_pair_anchor_mismatch_count > 0 THEN
    v_errors := v_errors || jsonb_build_array(
      jsonb_build_object(
        'code',
          'IMPORT_FINALIZE_CORRECTION_PAIR_HISTORICAL_ANCHOR_MISMATCH',
        'pair_anchor_mismatch_count',
          v_pair_anchor_mismatch_count
      )
    );
  END IF;

  IF v_historical_anchor_mismatch_count > 0 THEN
    v_errors := v_errors || jsonb_build_array(
      jsonb_build_object(
        'code',
          'IMPORT_FINALIZE_CORRECTION_FINANCIALS_POLICY_ENVELOPE_MISMATCH',
        'mismatching_timesheet_count',
          v_historical_anchor_mismatch_count
      )
    );
  END IF;

  IF jsonb_array_length(v_errors) > 0 THEN
    v_new_state := 'FINANCIALISED_PENDING_FINALISATION';
    v_result_code := CASE
      WHEN v_blocking_batch_count > 0
        THEN 'BLOCKED_ACTIVE_PAY_DRAFT'
      WHEN v_pair_anchor_mismatch_count > 0
        OR v_historical_anchor_mismatch_count > 0
        THEN 'BLOCKED_CORRECTION_FINANCIALS_POLICY_ENVELOPE_MISMATCH'
      ELSE 'FINANCIALISED_FINALISATION_BLOCKED'
    END;
    v_continuation_required := true;
    v_complete := false;
  ELSIF v_current_tsfin_count <> v_expected_count
     OR v_core_financial_ready_count <> v_expected_count
     OR v_stale_count > 0 THEN
    v_new_state := 'SOURCE_COMMITTED_TSFIN_PENDING';
    v_result_code := 'APPLIED_FINANCIALISATION_PENDING';
    v_continuation_required := true;
    v_complete := false;
  ELSIF v_financial_ready_count <> v_expected_count THEN
    v_new_state := 'FINANCIALISED_PENDING_FINALISATION';
    v_result_code := 'BLOCKED_CORRECTION_FINANCIALS_POLICY_ENVELOPE_MISMATCH';
    v_continuation_required := true;
    v_complete := false;
  ELSIF v_authorised_count = v_expected_count THEN
    v_new_state := 'COMPLETE';
    v_result_code := 'APPLIED_AND_FINANCIALISED';
    v_continuation_required := false;
    v_complete := true;
  ELSE
    v_new_state := 'FINANCIALISED_PENDING_FINALISATION';
    v_result_code := 'FINANCIALISED_AUTHORISATION_REQUIRED';
    v_continuation_required := true;
    v_complete := false;
  END IF;

  UPDATE public.import_apply_operations AS operation_row
  SET
    state = v_new_state,
    response_json =
      COALESCE(operation_row.response_json, '{}'::jsonb)
      || v_response_patch
      || jsonb_build_object(
        'operation_id', p_operation_id::text,
        'state', v_new_state,
        'result_code', v_result_code,
        'expected_timesheet_ids', to_jsonb(v_expected_ids),
        'preflight_fingerprint',
          v_current_preflight_fingerprint,
        'current_tsfin_count', v_current_tsfin_count,
        'core_financial_ready_count',
          v_core_financial_ready_count,
        'financial_ready_count', v_financial_ready_count,
        'correction_financials_policy_envelope_fingerprints',
          COALESCE(
            v_preflight ->
              'correction_financials_policy_envelope_fingerprints',
            '{}'::jsonb
          ),
        'authorised_count', v_authorised_count,
        'unauthorised_count', v_unauthorised_count,
        'continuation_required', v_continuation_required,
        'complete', v_complete,
        'errors', v_errors
      ),
    committed_at_utc = COALESCE(
      operation_row.committed_at_utc,
      v_now
    ),
    financialised_at_utc = CASE
      WHEN v_financial_ready_count = v_expected_count
       AND jsonb_array_length(v_errors) = 0
        THEN COALESCE(operation_row.financialised_at_utc, v_now)
      ELSE operation_row.financialised_at_utc
    END,
    finalised_at_utc = CASE
      WHEN v_complete
        THEN COALESCE(operation_row.finalised_at_utc, v_now)
      ELSE operation_row.finalised_at_utc
    END,
    updated_at_utc = v_now
  WHERE operation_row.id = p_operation_id
  RETURNING *
  INTO v_operation;

  v_state_changed :=
    v_before_operation ->> 'state'
      IS DISTINCT FROM v_operation.state;

  IF v_state_changed OR v_complete THEN
    PERFORM public._inv_write_audit(
      p_actor_user_id,
      CASE
        WHEN v_complete THEN 'IMPORT_FINANCIALISATION_COMPLETE'
        ELSE 'IMPORT_FINANCIALISATION_STATE_UPDATED'
      END,
      jsonb_build_object(
        'operation_id', p_operation_id::text,
        'state', v_operation.state,
        'result_code', v_result_code,
        'timesheet_ids', to_jsonb(v_expected_ids),
        'core_financial_ready_count',
          v_core_financial_ready_count,
        'financial_ready_count', v_financial_ready_count,
        'correction_financials_policy_envelope_fingerprints',
          COALESCE(
            v_preflight ->
              'correction_financials_policy_envelope_fingerprints',
            '{}'::jsonb
          ),
        'authorised_count', v_authorised_count,
        'errors', v_errors
      ),
      'import_apply_operation',
      p_operation_id::text,
      v_before_operation,
      'Import apply financialisation/finalisation verification',
      NULL::text,
      NULL::text,
      'import-operation:' || p_operation_id::text
    );
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'replay', false,
    'operation_id', p_operation_id::text,
    'state', v_operation.state,
    'result_code', v_result_code,
    'complete', v_complete,
    'continuation_required', v_continuation_required,
    'requires_authorisation',
      v_result_code = 'FINANCIALISED_AUTHORISATION_REQUIRED',
    'required_backend_rpc', CASE
      WHEN v_result_code = 'FINANCIALISED_AUTHORISATION_REQUIRED'
        THEN 'timesheet_authorise_bulk_atomic'
      ELSE NULL
    END,
    'expected_timesheet_ids', to_jsonb(v_expected_ids),
    'timesheets', v_timesheet_rows,
    'authorisation_items', v_authorisation_items,
    'preflight_fingerprint',
      v_current_preflight_fingerprint,
    'correction_financials_policy_envelopes',
      COALESCE(
        v_preflight -> 'correction_financials_policy_envelopes',
        '{}'::jsonb
      ),
    'correction_financials_policy_envelope_fingerprints',
      COALESCE(
        v_preflight ->
          'correction_financials_policy_envelope_fingerprints',
        '{}'::jsonb
      ),
    'current_tsfin_count', v_current_tsfin_count,
    'core_financial_ready_count',
      v_core_financial_ready_count,
    'financial_ready_count', v_financial_ready_count,
    'historical_anchor_mismatch_count',
      v_historical_anchor_mismatch_count,
    'pair_anchor_mismatch_count',
      v_pair_anchor_mismatch_count,
    'authorised_count', v_authorised_count,
    'unauthorised_count', v_unauthorised_count,
    'blocking_batches',
      COALESCE(v_preflight -> 'blocking_batches', '[]'::jsonb),
    'errors', v_errors,
    'response_json', v_operation.response_json,
    'committed_at_utc', v_operation.committed_at_utc,
    'financialised_at_utc', v_operation.financialised_at_utc,
    'finalised_at_utc', v_operation.finalised_at_utc
  );
END;
$function$;

-- import_apply_operation_claim_v2(uuid,uuid,hr_source_enum,text,text,uuid,jsonb)
CREATE OR REPLACE FUNCTION public.import_apply_operation_claim_v2(p_operation_id uuid, p_import_id uuid, p_source_system hr_source_enum, p_import_revision text, p_request_hash text, p_actor_user_id uuid, p_request_envelope jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
begin
  return public._import_apply_operation_claim_core_v2(p_operation_id,p_import_id,p_source_system,p_import_revision,
    p_request_hash,p_actor_user_id,p_request_envelope);
end $function$;

-- import_auto_authorise_policy_resolve_v1(hr_source_enum,uuid,uuid,boolean)
CREATE OR REPLACE FUNCTION public.import_auto_authorise_policy_resolve_v1(p_source_system hr_source_enum, p_client_id uuid, p_contract_id uuid DEFAULT NULL::uuid, p_validation_context boolean DEFAULT false)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
DECLARE
  v_source_system text := UPPER(BTRIM(COALESCE(p_source_system::text, '')));
  v_policy_source_system text;
  v_global public.settings_defaults%ROWTYPE;
  v_client_setting public.client_settings%ROWTYPE;
  v_contract public.contracts%ROWTYPE;

  v_client_setting_found boolean := false;
  v_contract_found boolean := false;

  v_global_import_value boolean;
  v_client_import_value boolean;
  v_contract_override_value boolean;
  v_effective_import_value boolean;
  v_effective_value boolean;
  v_resolution_source text;
  v_policy_json jsonb;
  v_policy_fingerprint text;
BEGIN
  IF p_client_id IS NULL THEN
    RAISE EXCEPTION 'IMPORT_AUTO_AUTHORISE_CLIENT_ID_REQUIRED'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object(
              'code', 'IMPORT_AUTO_AUTHORISE_CLIENT_ID_REQUIRED'
            )::text;
  END IF;

  IF v_source_system NOT IN ('HEALTHROSTER', 'HEALTHROSTER_DAILY', 'NHSP') THEN
    RAISE EXCEPTION 'IMPORT_AUTO_AUTHORISE_SOURCE_SYSTEM_INVALID'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object(
              'code', 'IMPORT_AUTO_AUTHORISE_SOURCE_SYSTEM_INVALID',
              'source_system', v_source_system
            )::text;
  END IF;

  v_policy_source_system := CASE
    WHEN v_source_system IN ('HEALTHROSTER', 'HEALTHROSTER_DAILY')
      THEN 'HEALTHROSTER'
    ELSE 'NHSP'
  END;

  PERFORM 1
  FROM public.clients AS client_row
  WHERE client_row.id = p_client_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'IMPORT_AUTO_AUTHORISE_CLIENT_NOT_FOUND'
      USING ERRCODE = 'P0002',
            DETAIL = jsonb_build_object(
              'code', 'IMPORT_AUTO_AUTHORISE_CLIENT_NOT_FOUND',
              'client_id', p_client_id::text
            )::text;
  END IF;

  SELECT settings_row.*
  INTO v_global
  FROM public.settings_defaults AS settings_row
  WHERE settings_row.id = 1
  LIMIT 1;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'IMPORT_AUTO_AUTHORISE_GLOBAL_SETTINGS_MISSING'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'IMPORT_AUTO_AUTHORISE_GLOBAL_SETTINGS_MISSING',
              'settings_defaults_id', 1
            )::text;
  END IF;

  SELECT client_settings_row.*
  INTO v_client_setting
  FROM public.client_settings AS client_settings_row
  WHERE client_settings_row.client_id = p_client_id
    AND (
      client_settings_row.effective_from IS NULL
      OR client_settings_row.effective_from <= (CURRENT_TIMESTAMP AT TIME ZONE 'Europe/London')::date
    )
  ORDER BY
    client_settings_row.effective_from DESC NULLS LAST,
    client_settings_row.updated_at DESC,
    client_settings_row.id DESC
  LIMIT 1;

  v_client_setting_found := FOUND;

  IF p_contract_id IS NOT NULL THEN
    SELECT contract_row.*
    INTO v_contract
    FROM public.contracts AS contract_row
    WHERE contract_row.id = p_contract_id
    LIMIT 1;

    v_contract_found := FOUND;

    IF NOT v_contract_found THEN
      RAISE EXCEPTION 'IMPORT_AUTO_AUTHORISE_CONTRACT_NOT_FOUND'
        USING ERRCODE = 'P0002',
              DETAIL = jsonb_build_object(
                'code', 'IMPORT_AUTO_AUTHORISE_CONTRACT_NOT_FOUND',
                'contract_id', p_contract_id::text
              )::text;
    END IF;

    IF v_contract.client_id IS DISTINCT FROM p_client_id THEN
      RAISE EXCEPTION 'IMPORT_AUTO_AUTHORISE_CONTRACT_CLIENT_MISMATCH'
        USING ERRCODE = '22023',
              DETAIL = jsonb_build_object(
                'code', 'IMPORT_AUTO_AUTHORISE_CONTRACT_CLIENT_MISMATCH',
                'contract_id', p_contract_id::text,
                'expected_client_id', p_client_id::text,
                'actual_client_id', v_contract.client_id::text
              )::text;
    END IF;
  END IF;

  IF v_policy_source_system = 'HEALTHROSTER' THEN
    v_global_import_value :=
      v_global.healthroster_import_auto_authorise_default;

    v_client_import_value := CASE
      WHEN v_client_setting_found
        THEN v_client_setting.healthroster_import_auto_authorise
      ELSE v_global_import_value
    END;

    v_contract_override_value := CASE
      WHEN v_contract_found
        THEN v_contract.healthroster_import_auto_authorise_override
      ELSE NULL::boolean
    END;
  ELSE
    v_global_import_value :=
      v_global.nhsp_import_auto_authorise_default;

    v_client_import_value := CASE
      WHEN v_client_setting_found
        THEN v_client_setting.nhsp_import_auto_authorise
      ELSE v_global_import_value
    END;

    v_contract_override_value := CASE
      WHEN v_contract_found
        THEN v_contract.nhsp_import_auto_authorise_override
      ELSE NULL::boolean
    END;
  END IF;

  IF v_contract_override_value IS NOT NULL THEN
    v_effective_import_value := v_contract_override_value;
    v_resolution_source := 'CONTRACT_OVERRIDE';
  ELSIF v_client_setting_found THEN
    v_effective_import_value := v_client_import_value;
    v_resolution_source := 'CLIENT_SETTING';
  ELSE
    v_effective_import_value := v_global_import_value;
    v_resolution_source := 'GLOBAL_FALLBACK_CLIENT_SETTING_MISSING';
  END IF;

  -- Exact-match HealthRoster validation uses the same explicit source policy
  -- hierarchy as import-authoritative processing.  The caller owns the
  -- additional match/reference gates; validation context must not bypass a
  -- contract override or client setting with a separate global switch.
  v_effective_value := v_effective_import_value;
  IF COALESCE(p_validation_context, false) THEN
    v_resolution_source := v_resolution_source || '_VALIDATION_EXACT_MATCH';
  END IF;

  v_policy_json := jsonb_strip_nulls(
    jsonb_build_object(
      'source_system', v_source_system,
      'policy_source_system', v_policy_source_system,
      'validation_context', COALESCE(p_validation_context, false),
      'client_id', p_client_id::text,
      'contract_id', CASE
        WHEN p_contract_id IS NULL THEN NULL
        ELSE p_contract_id::text
      END,
      'global_import_value', v_global_import_value,
      'client_import_value', v_client_import_value,
      'contract_override_value', v_contract_override_value,
      'effective_import_value', v_effective_import_value,
      'global_validation_value', v_global.auto_authorise_on_validation,
      'effective_value', v_effective_value,
      'resolution_source', v_resolution_source,
      'client_setting_found', v_client_setting_found,
      'client_settings_id', CASE
        WHEN v_client_setting_found THEN v_client_setting.id::text
        ELSE NULL
      END,
      'client_settings_effective_from', CASE
        WHEN v_client_setting_found THEN v_client_setting.effective_from
        ELSE NULL
      END,
      'client_settings_updated_at', CASE
        WHEN v_client_setting_found THEN v_client_setting.updated_at
        ELSE NULL
      END,
      'contract_updated_at', CASE
        WHEN v_contract_found THEN v_contract.updated_at
        ELSE NULL
      END,
      'global_settings_updated_at', v_global.updated_at
    )
  );

  v_policy_fingerprint := encode(
    extensions.digest(
      convert_to(v_policy_json::text, 'UTF8'),
      'sha256'
    ),
    'hex'
  );

  RETURN v_policy_json
    || jsonb_build_object(
      'ok', true,
      'policy_fingerprint', v_policy_fingerprint
    );
END;
$function$;

-- import_review_abandon_v1(uuid,bigint,text,uuid)
CREATE OR REPLACE FUNCTION public.import_review_abandon_v1(p_import_id uuid, p_expected_state_version bigint, p_reason text, p_actor_user_id uuid DEFAULT NULL::uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare v public.import_review_states%rowtype;
begin perform public._import_review_assert_actor_v1(p_actor_user_id); if length(btrim(coalesce(p_reason,''))) not between 1 and 500 then raise exception 'IMPORT_REVIEW_ABANDON_REASON_INVALID' using errcode='22023'; end if;
  select * into v from public.import_review_states where import_id=p_import_id for update; if not found then raise exception 'IMPORT_REVIEW_NOT_FOUND' using errcode='P0002'; end if;
  if v.status='ABANDONED' then return jsonb_build_object('ok',true,'replay',true,'status',v.status,'state_version',v.state_version); end if;
  if v.state_version<>p_expected_state_version then raise exception 'IMPORT_REVIEW_VERSION_CONFLICT' using errcode='40001'; end if;
  if v.status not in ('STAGED','IN_REVIEW','BLOCKED','READY') then raise exception 'IMPORT_REVIEW_ABANDON_NOT_ALLOWED' using errcode='55000'; end if;
  update public.import_review_states set status='ABANDONED',state_version=state_version+1,abandoned_at_utc=now(),abandoned_by_user_id=p_actor_user_id,
    abandoned_reason=btrim(p_reason),updated_at_utc=now(),updated_by_user_id=p_actor_user_id where import_id=p_import_id returning * into v;
  insert into public.import_review_events(import_id,state_version,event_code,actor_user_id,event_context_json) values(p_import_id,v.state_version,'REVIEW_ABANDONED',p_actor_user_id,jsonb_build_object('reason',btrim(p_reason)));
  return jsonb_build_object('ok',true,'replay',false,'status',v.status,'state_version',v.state_version); end $function$;

-- import_review_actions_page_v1(uuid,uuid,integer,integer,text,text,text)
CREATE OR REPLACE FUNCTION public.import_review_actions_page_v1(p_import_id uuid, p_actor_user_id uuid DEFAULT NULL::uuid, p_page_number integer DEFAULT 1, p_page_size integer DEFAULT 25, p_sort_by text DEFAULT 'CANDIDATE'::text, p_sort_direction text DEFAULT 'ASC'::text, p_view text DEFAULT 'ALL'::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_page integer:=coalesce(p_page_number,1);
  v_size integer:=coalesce(p_page_size,25);
  v_sort text:=upper(btrim(coalesce(p_sort_by,'CANDIDATE')));
  v_direction text:=upper(btrim(coalesce(p_sort_direction,'ASC')));
  v_view text:=upper(btrim(coalesce(p_view,'ALL')));
  v_items jsonb;
  v_total integer;
  v_counts jsonb;
  v_confirmation_counts jsonb;
begin
  perform public._import_review_assert_actor_v1(p_actor_user_id);
  if p_import_id is null or v_page<1 or v_page>10000 or v_size not in (25,50,75,100)
     or v_sort not in ('CANDIDATE','CLIENT','WEEK_ENDING','WORK_DATE','ACTION','STATUS')
     or v_direction not in ('ASC','DESC')
     or v_view not in ('ALL','PENDING','READY','EMAIL','NO_ACTION','CONFIRM_STANDARD',
       'CONFIRM_NON_STANDARD','CONFIRM_VALIDATION','CONFIRM_EMAIL','CONFIRM_REFERENCE') then
    raise exception 'IMPORT_REVIEW_ACTION_PAGE_INPUT_INVALID' using errcode='22023';
  end if;
  if not exists(select 1 from public.import_review_states s where s.import_id=p_import_id) then
    raise exception 'IMPORT_REVIEW_NOT_FOUND' using errcode='P0002';
  end if;

  with ready_ids as (
    select r.action_id from public._import_review_ready_action_ids_core_v1(p_import_id) r
  ), current_actions as (
    select d.*,(ready.action_id is not null) batch_eligible,
      coalesce(nullif(btrim(concat_ws(' ',c.first_name,c.last_name)),''),nullif(d.summary_json->>'candidate_name',''),'Unknown candidate') candidate_name,
      lower(coalesce(nullif(c.last_name,''),
        case when position(',' in coalesce(d.summary_json->>'candidate_name',''))>0 then split_part(d.summary_json->>'candidate_name',',',1)
             else regexp_replace(btrim(coalesce(d.summary_json->>'candidate_name','')),'^.*\s+','','') end,'')) candidate_surname_sort,
      case when d.candidate_id is not null then 'candidate:'||d.candidate_id::text
        else 'source:'||public._import_review_hash_v1(concat_ws('|',
          regexp_replace(lower(coalesce(d.summary_json->>'candidate_name',d.source_identity,'')),'[^a-z0-9]+','','g'),
          coalesce(d.client_id::text,regexp_replace(lower(coalesce(d.summary_json->>'client_name','')),'[^a-z0-9]+','','g')))) end
        candidate_branch_key,
      coalesce(nullif(cl.name,''),nullif(d.summary_json->>'client_name',''),'Unknown client') client_name,
      case when d.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER') then lower(btrim(case
        when coalesce(ct.send_ts_queries_to_different_email,false) then ct.ts_queries_alt_email_address
        else cl.ts_queries_email end)) end recipient_email,
      case when d.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER') then
        case when nullif(btrim(case when coalesce(ct.send_ts_queries_to_different_email,false)
          then ct.ts_queries_alt_email_address else cl.ts_queries_email end),'') is null
          then 'RECIPIENT_UNAVAILABLE:'||coalesce(d.client_id::text,'UNKNOWN')
          else 'RECIPIENT_EMAIL:'||public._import_review_hash_v1(lower(btrim(case
            when coalesce(ct.send_ts_queries_to_different_email,false) then ct.ts_queries_alt_email_address
            else cl.ts_queries_email end))) end end recipient_group_key,
      case when d.contract_id is null then 'Client default'
        else coalesce(nullif(concat_ws(' · ',nullif(ct.display_site,''),nullif(ct.role,''),nullif(ct.band,'')),''),'Contract') end contract_label,
      coalesce(timesheet_choices.options,'[]'::jsonb) daily_timesheet_options,
      coalesce(d.summary_json->'imported_evidence',case when hr.id is not null then jsonb_strip_nulls(jsonb_build_object(
        'work_date',hr.date_local,'start',hr.start_time_local,'end',hr.end_time_local,
        'break_minutes',coalesce((hr.payload_json->>'actual_break_mins')::integer,(hr.payload_json->>'actual_break_minutes')::integer,
          (hr.payload_json->>'break_mins')::integer,(hr.payload_json->>'break_minutes')::integer),
        'worked_hours',hr.hours_worked,'worked_minutes',case when hr.hours_worked is null then null else round(hr.hours_worked*60) end,
        'reference',hr.hr_request_id,'role',hr.assignment_grade_norm)) end) imported_evidence,
      coalesce(d.summary_json->'current_evidence',case when ts_ev.timesheet_id is not null then jsonb_strip_nulls(jsonb_build_object(
        'work_date',(ts_ev.worked_start_iso at time zone 'Europe/London')::date,'start',ts_ev.worked_start_iso,'end',ts_ev.worked_end_iso,
        'break_minutes',ts_ev.break_minutes,'worked_minutes',ts_ev.worked_minutes,'worked_hours',round(ts_ev.worked_minutes/60.0,2),
        'reference',ts_ev.reference_number,'role',ts_ev.tsfin_role,'band',ts_ev.tsfin_band,'timesheet_id',ts_ev.timesheet_id))
        when shift_ev.id is not null then jsonb_strip_nulls(jsonb_build_object(
        'work_date',shift_ev.work_date,'start',shift_ev.start_utc,'end',shift_ev.end_utc,'break_minutes',shift_ev.break_mins,
        'worked_minutes',shift_ev.pay_minutes,'role',shift_ev.assignment_code,'timesheet_id',shift_ev.timesheet_id,'shift_id',shift_ev.id)) end) current_evidence,
      coalesce(d.summary_json->'difference_codes',case when nullif(d.summary_json->>'reason_code','') is not null
        then jsonb_build_array(d.summary_json->>'reason_code') else '[]'::jsonb end) difference_codes,
      coalesce(d.summary_json->'evidence_rows','[]'::jsonb) evidence_rows,
      coalesce(nullif(d.summary_json->>'outcome_label',''),case d.action_kind
        when 'INCLUDE_SHIFT' then 'TMS will add shift' when 'APPLY_AMENDMENT' then 'TMS will amend shift'
        when 'APPLY_CANCELLATION' then 'TMS will cancel shift' when 'MARK_VALIDATION_ERROR' then 'TMS will record validation issue'
        when 'EMAIL_ISSUE' then case when d.summary_json->>'reason_code'='MISSING_FROM_IMPORT'
          then 'Request new shift' else 'Request amend shift' end
        when 'EMAIL_REMINDER' then case when d.summary_json->>'reason_code'='MISSING_FROM_IMPORT'
          then 'Request new shift reminder' else 'Request amend shift reminder' end
        when 'INVALIDATE_REFERENCE' then 'Clear stored reference' when 'NO_ACTION' then 'Passed checks'
        when 'DAILY_TIMESHEET_RESOLUTION' then 'Choose existing timesheet' else 'Resolve before continuing' end) outcome_label,
      d.summary_json->>'resolution_kind' resolution_kind,
      d.summary_json->>'authority_mode' authority_mode,
      case when d.summary_json->>'resolution_kind'='WEEKLY_ASSIGNMENT_CONTRACT'
        then coalesce(current_weekly_options.options,'[]'::jsonb)
        else coalesce(d.summary_json->'resolution_options','[]'::jsonb) end resolution_options,
      coalesce(d.summary_json->'protection','{}'::jsonb) protection,
      d.summary_json->>'default_excluded_reason' default_excluded_reason,
      nullif(d.summary_json->>'week_ending_date','')::date week_ending_date,
      nullif(d.summary_json->>'work_date','')::date work_date
    from public.import_review_decisions d
    left join ready_ids ready on ready.action_id=d.action_id
    left join public.candidates c on c.id=d.candidate_id
    left join public.clients cl on cl.id=d.client_id
    left join public.contracts ct on ct.id=d.contract_id
    left join public.hr_rows hr on hr.id=d.hr_row_id and hr.import_id=d.import_id
    left join public.v_timesheets_daily_match ts_ev on ts_ev.timesheet_id=d.timesheet_id
    left join public.nhsp_shifts shift_ev on shift_ev.id=d.shift_id
    left join lateral (
      select coalesce(jsonb_agg(jsonb_build_object(
        'timesheet_id',t.timesheet_id,'worked_start_iso',t.worked_start_iso,'worked_end_iso',t.worked_end_iso,
        'break_minutes',t.break_minutes,'worked_minutes',t.worked_minutes,
        'reference_number',t.reference_number,'processing_status',t.processing_status,
        'role',t.tsfin_role,'band',t.tsfin_band,'site',t.hospital_norm,'contract_id',ts.contract_id,
        'display_label',concat_ws(' · ',to_char((t.worked_start_iso at time zone 'Europe/London')::date,'DD Mon YYYY'),
          to_char(t.worked_start_iso at time zone 'Europe/London','HH24:MI')||'–'||to_char(t.worked_end_iso at time zone 'Europe/London','HH24:MI'),
          round(t.worked_minutes/60.0,2)||' hours',nullif(t.tsfin_role,''),nullif(t.tsfin_band,''),nullif(t.hospital_norm,''),
          case when nullif(t.reference_number,'') is not null then 'ref '||t.reference_number end)
      ) order by t.worked_start_iso,t.timesheet_id),'[]'::jsonb) options
      from public.v_timesheets_daily_match t
      left join public.timesheets ts on ts.timesheet_id=t.timesheet_id and ts.is_current
      where d.action_kind='DAILY_TIMESHEET_RESOLUTION'
        and t.timesheet_id in (
          select value::uuid from jsonb_array_elements_text(coalesce(d.summary_json->'timesheet_options','[]'::jsonb)) value
        )
    ) timesheet_choices on true
    left join lateral (
      -- Resolution options are revalidated when they are read so a durable
      -- review created before a settings/contract correction cannot keep
      -- showing stale disabled options.  This only authorises creation of the
      -- assignment mapping; refresh/finalisation still reclassifies the row
      -- and enforces rates, authority and all financial guards independently.
      select coalesce(jsonb_agg(
        option_row.option_json || jsonb_strip_nulls(jsonb_build_object(
          'option_id',case when option_contract.id is not null then 'contract:'||option_contract.id::text end,
          'contract_id',option_contract.id,
          'candidate_id',option_contract.candidate_id,
          'client_id',option_contract.client_id,
          'role',option_contract.role,
          'band',option_contract.band,
          'site',option_contract.display_site,
          'start_date',option_contract.start_date,
          'end_date',option_contract.end_date,
          'source_route_eligible',coalesce(option_authority.route_eligible,false),
          'authority_mode',option_authority.authority_mode,
          'selectable',option_contract.id is not null
            and option_contract.candidate_id=d.candidate_id
            and option_contract.client_id=d.client_id
            and option_contract.start_date<=coalesce(nullif(d.summary_json->>'work_date','')::date,hr.date_local)
            and (option_contract.end_date is null
              or option_contract.end_date>=coalesce(nullif(d.summary_json->>'work_date','')::date,hr.date_local))
            and coalesce(option_authority.route_eligible,false),
          'disabled_reason_code',case when option_contract.id is null
              or option_contract.candidate_id is distinct from d.candidate_id
              or option_contract.client_id is distinct from d.client_id
              or option_contract.start_date>coalesce(nullif(d.summary_json->>'work_date','')::date,hr.date_local)
              or (option_contract.end_date is not null
                and option_contract.end_date<coalesce(nullif(d.summary_json->>'work_date','')::date,hr.date_local))
              or not coalesce(option_authority.route_eligible,false)
            then 'CONTRACT_NOT_ELIGIBLE' end,
          'display_label',case when option_contract.id is not null then concat_ws(' · ',
            nullif(option_contract.role,''),nullif(option_contract.band,''),nullif(option_contract.display_site,''),
            to_char(option_contract.start_date,'DD Mon YYYY')||' to '||
              coalesce(to_char(option_contract.end_date,'DD Mon YYYY'),'open ended')) end
        )) order by lower(coalesce(option_contract.role,option_row.option_json->>'role','')),
          lower(coalesce(option_contract.band,option_row.option_json->>'band','')),
          option_contract.start_date desc nulls last,option_row.option_json->>'option_id'
      ),'[]'::jsonb) options
      from jsonb_array_elements(coalesce(d.summary_json->'resolution_options','[]'::jsonb)) option_row(option_json)
      left join public.contracts option_contract on option_contract.id=case
        when coalesce(option_row.option_json->>'contract_id','')
          ~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$'
          then (option_row.option_json->>'contract_id')::uuid end
      left join lateral public._import_review_effective_authority_core_v1(
        case when upper(coalesce(d.summary_json->>'source_system',d.summary_json->>'source_route',''))='NHSP'
          then 'NHSP' else 'HR_WEEKLY' end,
        option_contract.id,option_contract.client_id,
        coalesce(nullif(d.summary_json->>'work_date','')::date,hr.date_local)
      ) option_authority on option_contract.id is not null
      where d.summary_json->>'resolution_kind'='WEEKLY_ASSIGNMENT_CONTRACT'
    ) current_weekly_options on true
    where d.import_id=p_import_id and d.is_current
  ), branch_badge_rows as (
    select a.candidate_branch_key,b.badge_code,label.badge_label,count(*)::integer badge_count,'ISSUE'::text badge_tone
    from current_actions a
    cross join lateral unnest(array_remove(array[
      case a.summary_json->>'reason_code'
        when 'CANDIDATE_UNRESOLVED' then 'CANDIDATE_NOT_LINKED'
        when 'CLIENT_UNRESOLVED' then 'CLIENT_NOT_LINKED'
        when 'GRADE_MAPPING_REQUIRED' then 'GRADE_NOT_MAPPED'
        when 'CONTRACT_MISSING' then 'NO_CONTRACT'
        when 'CONTRACT_AMBIGUOUS' then 'MULTIPLE_CONTRACTS'
        when 'CONTRACT_OUT_OF_SCOPE' then 'CONTRACT_NOT_ELIGIBLE'
        when 'CONTRACT_RATES_INCOMPLETE' then 'RATES_INCOMPLETE'
        when 'TIMESHEET_OCCUPIED_BY_EXPENSES' then 'TIMESHEET_OCCUPIED_BY_EXPENSES'
        when 'TIMESHEET_NOT_FOUND' then 'TIMESHEET_MISSING'
        when 'WEEKLY_TIMESHEET_NOT_SUBMITTED' then 'TIMESHEET_NOT_SUBMITTED'
        when 'DAILY_TIMESHEET_NOT_SUBMITTED' then 'TIMESHEET_NOT_SUBMITTED'
        when 'WEEKLY_SHIFT_ABSENT_FROM_TIMESHEET' then 'SHIFT_MISSING_FROM_TIMESHEET'
        when 'DAILY_SHIFT_ABSENT_FROM_TIMESHEET' then 'SHIFT_MISSING_FROM_TIMESHEET'
        when 'TIMESHEET_AMBIGUOUS' then 'CHOOSE_TIMESHEET'
        when 'BLOCKED_ACTIVE_PAY_DRAFT' then 'BANKING_PAY_PROTECTED'
        when 'MISSING_FROM_IMPORT' then 'MISSING_FROM_FILE'
        when 'MISSING_FROM_COMPLETE_IMPORT' then 'MISSING_FROM_FILE'
        when 'REFERENCE_ON_SHIFT_MISSING_FROM_COMPLETE_IMPORT' then 'REFERENCE_REVIEW'
        when 'REFERENCE_ON_SHIFT_MISSING_OR_MISMATCHED_IN_COMPLETE_IMPORT' then 'REFERENCE_REVIEW'
        when 'QUERY_RECIPIENT_EMAIL_MISSING_OR_INVALID' then 'EMAIL_NOT_CONFIGURED'
        when 'HEALTHROSTER_WEEKLY' then 'WEEKLY_MISMATCH' end,
      case when a.action_kind='EMAIL_ISSUE' and a.selected then 'EMAIL_REQUEST_SELECTED' end,
      case when a.action_kind='EMAIL_REMINDER' then 'REMINDER_AVAILABLE' end,
      case when a.action_kind='INVALIDATE_REFERENCE' then 'REFERENCE_REVIEW' end,
      case when a.blocking and a.summary_json->>'reason_code' is null then 'NEEDS_RECHECK' end,
      case when coalesce(a.difference_codes,'[]'::jsonb) ?| array['WORKED_HOURS','ACTUAL_HOURS_MISMATCH'] then 'HOURS_DIFFER' end,
      case when coalesce(a.difference_codes,'[]'::jsonb) ?| array['START_TIME','END_TIME','START_END_MISMATCH'] then 'TIMES_DIFFER' end,
      case when coalesce(a.difference_codes,'[]'::jsonb) ?| array['BREAK_MINUTES','BREAK_MINUTES_MISMATCH'] then 'BREAK_DIFFERS' end,
      case when coalesce(a.difference_codes,'[]'::jsonb) ?| array['NEW_SHIFT','HR_ONLY'] then 'NOT_IN_CLOUDTMS' end,
      case when coalesce(a.difference_codes,'[]'::jsonb) ?| array['AMBIGUOUS'] then 'MATCH_UNCLEAR' end,
      case when coalesce(a.difference_codes,'[]'::jsonb) ?| array['REFERENCE'] then 'REFERENCE_ISSUE' end
    ],null)) b(badge_code)
    cross join lateral (select case b.badge_code
      when 'CANDIDATE_NOT_LINKED' then 'Candidate not linked' when 'CLIENT_NOT_LINKED' then 'Client not linked'
      when 'GRADE_NOT_MAPPED' then 'Grade not mapped' when 'NO_CONTRACT' then 'No contract'
      when 'MULTIPLE_CONTRACTS' then 'Multiple contracts' when 'CONTRACT_NOT_ELIGIBLE' then 'Contract not eligible'
      when 'RATES_INCOMPLETE' then 'Rates incomplete'
      when 'TIMESHEET_OCCUPIED_BY_EXPENSES' then 'Timesheet occupied by expenses'
      when 'TIMESHEET_MISSING' then 'Timesheet missing'
      when 'TIMESHEET_NOT_SUBMITTED' then 'Timesheet not submitted'
      when 'SHIFT_MISSING_FROM_TIMESHEET' then 'Shift missing from timesheet'
      when 'CHOOSE_TIMESHEET' then 'Choose timesheet' when 'BANKING_PAY_PROTECTED' then 'Banking Pay protected'
      when 'NEEDS_RECHECK' then 'Needs recheck' when 'HOURS_DIFFER' then 'Hours differ'
      when 'TIMES_DIFFER' then 'Times differ' when 'BREAK_DIFFERS' then 'Break differs'
      when 'MISSING_FROM_FILE' then 'Missing from file' when 'NOT_IN_CLOUDTMS' then 'Shift not in CloudTMS'
      when 'REFERENCE_ISSUE' then 'Reference issue' when 'MATCH_UNCLEAR' then 'Match unclear'
      when 'EMAIL_NOT_CONFIGURED' then 'Email not configured' when 'WEEKLY_MISMATCH' then 'Weekly mismatch'
      when 'EMAIL_REQUEST_SELECTED' then 'Email request selected' when 'REMINDER_AVAILABLE' then 'Reminder available'
      when 'REFERENCE_REVIEW' then 'Reference review' else b.badge_code end badge_label) label
    group by a.candidate_branch_key,b.badge_code,label.badge_label
    union all
    select a.candidate_branch_key,'READY_ACTION:'||a.action_kind,
      case a.action_kind
        when 'INCLUDE_SHIFT' then 'TMS to add shift'
        when 'APPLY_AMENDMENT' then case
          when a.summary_json->>'amendment_route'='AMEND_PAID_UNINVOICED_SOURCE' then 'TMS to amend paid uninvoiced shift'
          when a.summary_json->>'amendment_route'='AMEND_EXISTING_REPLACEMENT' then 'TMS to repair current correction generation'
          when a.summary_json->>'amendment_route'='CREATE_REVERSAL_REPLACEMENT' then 'TMS to create correction generation'
          else 'TMS to amend shift' end
        when 'APPLY_CANCELLATION' then case
          when coalesce((a.protection->>'paid')::boolean,false)
            or coalesce((a.protection->>'invoice_locked')::boolean,false)
          then 'TMS to reverse shift' else 'TMS to cancel shift' end
        when 'MARK_VALIDATION_ERROR' then 'Validate timesheet'
        when 'INVALIDATE_REFERENCE' then 'Clear stored reference'
        when 'DAILY_TIMESHEET_RESOLUTION' then 'Link existing timesheet'
        when 'EMAIL_ISSUE' then 'Request client correction'
        when 'EMAIL_REMINDER' then 'Request client correction reminder'
        else regexp_replace(a.outcome_label,'^TMS will ','TMS to ','i') end,
      count(*)::integer,'READY'::text
    from current_actions a
    where a.batch_eligible and a.selected and a.action_category in ('READY','EMAIL')
    group by a.candidate_branch_key,a.action_kind,
      case a.action_kind
        when 'INCLUDE_SHIFT' then 'TMS to add shift'
        when 'APPLY_AMENDMENT' then case
          when a.summary_json->>'amendment_route'='AMEND_PAID_UNINVOICED_SOURCE' then 'TMS to amend paid uninvoiced shift'
          when a.summary_json->>'amendment_route'='AMEND_EXISTING_REPLACEMENT' then 'TMS to repair current correction generation'
          when a.summary_json->>'amendment_route'='CREATE_REVERSAL_REPLACEMENT' then 'TMS to create correction generation'
          else 'TMS to amend shift' end
        when 'APPLY_CANCELLATION' then case
          when coalesce((a.protection->>'paid')::boolean,false)
            or coalesce((a.protection->>'invoice_locked')::boolean,false)
          then 'TMS to reverse shift' else 'TMS to cancel shift' end
        when 'MARK_VALIDATION_ERROR' then 'Validate timesheet'
        when 'INVALIDATE_REFERENCE' then 'Clear stored reference'
        when 'DAILY_TIMESHEET_RESOLUTION' then 'Link existing timesheet'
        when 'EMAIL_ISSUE' then 'Request client correction'
        when 'EMAIL_REMINDER' then 'Request client correction reminder'
        else regexp_replace(a.outcome_label,'^TMS will ','TMS to ','i') end
    union all
    select a.candidate_branch_key,'DEFERRED_ACTION','Deferred',count(*)::integer,'DEFERRED'::text
    from current_actions a
    where a.selectable and not a.selected and a.action_category in ('READY','EMAIL')
    group by a.candidate_branch_key
    union all
    select 'candidate:'||o.candidate_id::text,'COMPLETED_ACTION:'||o.action_kind,
      o.completed_label,count(*)::integer,'COMPLETED'::text
    from public.import_review_action_outcomes o
    where o.import_id=p_import_id
    group by o.candidate_id,o.action_kind,o.completed_label
  ), branch_badges as (
    select candidate_branch_key,jsonb_agg(jsonb_build_object(
      'code',badge_code,'label',badge_label,'count',badge_count,'tone',badge_tone)
      order by case badge_tone when 'ISSUE' then 1 when 'READY' then 2 when 'DEFERRED' then 3 else 4 end,badge_label) badges
    from branch_badge_rows badges
    where badges.badge_code<>'NOT_IN_CLOUDTMS'
      or not exists (
        select 1 from branch_badge_rows other
        where other.candidate_branch_key=badges.candidate_branch_key
          and other.badge_code<>'NOT_IN_CLOUDTMS' and other.badge_tone='ISSUE'
      )
    group by candidate_branch_key
  ), weekly_validation_holds as (
    select a.candidate_branch_key,a.week_ending_date,
      sum(case
        when a.action_category='EMAIL' and a.summary_json->>'reason_code'='HEALTHROSTER_WEEKLY'
          then greatest(coalesce(nullif(a.summary_json->>'validation_difference_count','')::integer,0),1)
        else 1
      end)::integer hold_count
    from current_actions a
    where a.week_ending_date is not null
      and (
        a.summary_json->>'reason_code'='HEALTHROSTER_WEEKLY'
        or (
          a.summary_json->>'source_route'='HR_WEEKLY'
          and a.summary_json->>'authority_mode'='VALIDATION_ONLY'
        )
      )
      and (
        a.action_category='EMAIL'
        or a.blocking
        or a.action_category in ('PENDING','BLOCKED')
      )
    group by a.candidate_branch_key,a.week_ending_date
  ), week_validation_badges as (
    select h.candidate_branch_key,h.week_ending_date,
      jsonb_build_array(jsonb_build_object(
        'code','WEEKLY_VALIDATION_INCOMPLETE',
        'label','Validation incomplete · '||h.hold_count::text||' shift'
          ||case when h.hold_count=1 then ' differs' else 's differ' end,
        'count',0,
        'tone','ISSUE'
      )) badges
    from weekly_validation_holds h
  ), filtered as (
    select a.*,coalesce(bb.badges,'[]'::jsonb) branch_badges,
      coalesce(wb.badges,'[]'::jsonb) week_validation_badges
    from current_actions a
    left join branch_badges bb using(candidate_branch_key)
    left join week_validation_badges wb using(candidate_branch_key,week_ending_date)
    where case v_view
      when 'PENDING' then a.blocking or a.action_category in ('PENDING','BLOCKED')
      when 'READY' then a.action_category='READY'
      when 'EMAIL' then a.action_category='EMAIL'
      when 'NO_ACTION' then a.action_category='NO_ACTION' and (
        a.summary_json->>'reason_code'='CANDIDATE_DID_NOT_WORK_CONFIRMED'
        or (
          coalesce(jsonb_array_length(case when jsonb_typeof(a.difference_codes)='array' then a.difference_codes else '[]'::jsonb end),0)=0
          and nullif(a.summary_json->>'reason_code','') is null
        )
      )
      when 'CONFIRM_STANDARD' then a.selected and a.batch_eligible and a.action_kind='INCLUDE_SHIFT'
      when 'CONFIRM_NON_STANDARD' then a.selected and a.batch_eligible and a.action_kind in ('APPLY_AMENDMENT','APPLY_CANCELLATION')
      when 'CONFIRM_VALIDATION' then a.selected and a.batch_eligible and a.action_kind='MARK_VALIDATION_ERROR'
      when 'CONFIRM_EMAIL' then a.selected and a.batch_eligible and a.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER')
      when 'CONFIRM_REFERENCE' then a.selected and a.batch_eligible and a.action_kind='INVALIDATE_REFERENCE'
      else true end
  ), ordered as (
    select f.*,
      row_number() over(order by
        case when v_view like 'CONFIRM_%' then lower(client_name) end asc nulls last,
        case when v_view like 'CONFIRM_%' then candidate_surname_sort end asc nulls last,
        case when v_view like 'CONFIRM_%' then lower(candidate_name) end asc nulls last,
        case when v_view like 'CONFIRM_%' then work_date end asc nulls last,
        case when v_sort='CANDIDATE' and v_direction='ASC' then candidate_surname_sort end asc nulls last,
        case when v_sort='CANDIDATE' and v_direction='DESC' then candidate_surname_sort end desc nulls last,
        case when v_sort='CANDIDATE' and v_direction='ASC' then lower(candidate_name) end asc nulls last,
        case when v_sort='CANDIDATE' and v_direction='DESC' then lower(candidate_name) end desc nulls last,
        case when v_sort='CLIENT' and v_direction='ASC' then lower(client_name) end asc nulls last,
        case when v_sort='CLIENT' and v_direction='DESC' then lower(client_name) end desc nulls last,
        case when v_sort='WEEK_ENDING' and v_direction='ASC' then week_ending_date end asc nulls last,
        case when v_sort='WEEK_ENDING' and v_direction='DESC' then week_ending_date end desc nulls last,
        case when v_sort='WORK_DATE' and v_direction='ASC' then work_date end asc nulls last,
        case when v_sort='WORK_DATE' and v_direction='DESC' then work_date end desc nulls last,
        case when v_sort='ACTION' and v_direction='ASC' then action_kind end asc,
        case when v_sort='ACTION' and v_direction='DESC' then action_kind end desc,
        case when v_sort='STATUS' and v_direction='ASC' then action_category end asc,
        case when v_sort='STATUS' and v_direction='DESC' then action_category end desc,
        action_id asc) rn,
      count(*) over() total_count,
      count(*) over(partition by candidate_branch_key) candidate_section_total_count,
      count(*) over(partition by client_id,client_name) client_section_total_count
    from filtered f
  )
  select coalesce(jsonb_agg(jsonb_build_object(
      'action_id',action_id,'action_kind',action_kind,'action_category',action_category,
      'target_key',target_key,'source_identity',source_identity,
      'hr_row_id',hr_row_id,'timesheet_id',timesheet_id,'shift_id',shift_id,
      'client_id',client_id,'candidate_id',candidate_id,'contract_id',contract_id,'issue_id',issue_id,
      'preview_generation',preview_generation,'evidence_fingerprint',evidence_fingerprint,
      'selectable',selectable,'selected',selected,'blocking',blocking,'batch_eligible',batch_eligible,
      'candidate_name',candidate_name,'candidate_surname_sort',candidate_surname_sort,
      'candidate_branch_key',candidate_branch_key,'branch_badges',branch_badges,
      'week_validation_badges',week_validation_badges,
      'candidate_section_total_count',candidate_section_total_count,
      'client_section_total_count',client_section_total_count,
      'client_name',client_name,'week_ending_date',week_ending_date,'work_date',work_date,
      'recipient_email',recipient_email,'recipient_group_key',recipient_group_key,'contract_label',contract_label,
      'daily_timesheet_options',daily_timesheet_options,
      'imported_evidence',imported_evidence,'current_evidence',current_evidence,
      'difference_codes',difference_codes,'evidence_rows',evidence_rows,'outcome_label',outcome_label,
      'resolution_kind',resolution_kind,'authority_mode',authority_mode,'resolution_options',resolution_options,
      'protection',protection,'default_excluded_reason',default_excluded_reason,
      'summary',summary_json
    ) order by rn),'[]'::jsonb),coalesce(max(total_count),0)
    into v_items,v_total
  from ordered where rn>((v_page-1)*v_size) and rn<=v_page*v_size;

  select jsonb_build_object(
    'ALL',count(*),
    'PENDING',count(*) filter(where blocking or action_category in ('PENDING','BLOCKED')),
    'READY',count(*) filter(where action_category='READY'),
    'EMAIL',count(*) filter(where action_category='EMAIL'),
    'NO_ACTION',count(*) filter(where action_category='NO_ACTION' and (
      summary_json->>'reason_code'='CANDIDATE_DID_NOT_WORK_CONFIRMED'
      or (
        coalesce(jsonb_array_length(case when jsonb_typeof(summary_json->'difference_codes')='array'
          then summary_json->'difference_codes' else '[]'::jsonb end),0)=0
        and nullif(summary_json->>'reason_code','') is null
      )
    ))
  ) into v_counts
  from public.import_review_decisions d where d.import_id=p_import_id and d.is_current;

  with ready_ids as (
    select r.action_id from public._import_review_ready_action_ids_core_v1(p_import_id) r
  ), selected_actions as (
    select d.action_kind,coalesce(d.summary_json->'protection','{}'::jsonb) protection,
      d.summary_json->>'amendment_route' amendment_route
    from public.import_review_decisions d
    join ready_ids r on r.action_id=d.action_id
    where d.import_id=p_import_id and d.is_current and d.selected
  )
  select jsonb_build_object(
    'selected_total',count(*),
    'standard',count(*) filter(where action_kind='INCLUDE_SHIFT'),
    'non_standard',count(*) filter(where action_kind in ('APPLY_AMENDMENT','APPLY_CANCELLATION')),
    'amendment',count(*) filter(where action_kind='APPLY_AMENDMENT'
      and amendment_route is distinct from 'CREATE_REVERSAL_REPLACEMENT'),
    'reversal_replacement',count(*) filter(where action_kind='APPLY_AMENDMENT'
      and amendment_route='CREATE_REVERSAL_REPLACEMENT'),
    'cancellation',count(*) filter(where action_kind='APPLY_CANCELLATION'
      and not (coalesce((protection->>'paid')::boolean,false) or coalesce((protection->>'invoice_locked')::boolean,false))),
    'reversal_only',count(*) filter(where action_kind='APPLY_CANCELLATION'
      and (coalesce((protection->>'paid')::boolean,false) or coalesce((protection->>'invoice_locked')::boolean,false))),
    'validation',count(*) filter(where action_kind='MARK_VALIDATION_ERROR'),
    'email',count(*) filter(where action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER')),
    'reference',count(*) filter(where action_kind='INVALIDATE_REFERENCE')
  ) into v_confirmation_counts from selected_actions;

  return jsonb_build_object(
    'ok',true,'import_id',p_import_id,'view',v_view,'view_counts',v_counts,
    'confirmation_counts',v_confirmation_counts,
    'items',v_items,'total_items',v_total,'page_number',v_page,'page_size',v_size,
    'total_pages',case when v_total=0 then 0 else ceiling(v_total::numeric/v_size)::integer end,
    'has_previous',v_page>1,'has_next',v_page*v_size<v_total,
    'sort_by',v_sort,'sort_direction',v_direction
  );
end
$function$;

-- import_review_apply_failed_before_commit_recover_v1(uuid,uuid,text,uuid)
CREATE OR REPLACE FUNCTION public.import_review_apply_failed_before_commit_recover_v1(p_import_id uuid, p_operation_id uuid, p_request_hash text, p_actor_user_id uuid DEFAULT NULL::uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare s public.import_review_states%rowtype; o public.import_apply_operations%rowtype; v_status text; v_blockers integer;
begin
  perform public._import_review_assert_actor_v1(p_actor_user_id);
  if p_import_id is null or p_operation_id is null or btrim(coalesce(p_request_hash,''))!~'^[0-9a-f]{64}$' then
    raise exception 'IMPORT_REVIEW_APPLY_RECOVERY_INPUT_INVALID' using errcode='22023';
  end if;
  select * into s from public.import_review_states where import_id=p_import_id for update;
  select * into o from public.import_apply_operations where id=p_operation_id and import_id=p_import_id for update;
  if s.import_id is null or o.id is null then raise exception 'IMPORT_REVIEW_APPLY_RECOVERY_NOT_FOUND' using errcode='P0002'; end if;
  if o.request_hash is distinct from lower(btrim(p_request_hash)) then
    raise exception 'IMPORT_REVIEW_APPLY_RECOVERY_REQUEST_MISMATCH' using errcode='40001';
  end if;
  if s.status<>'APPLYING' or s.last_operation_id is distinct from p_operation_id
     or o.state<>'FAILED_BEFORE_COMMIT' or o.committed_at_utc is not null then
    raise exception 'IMPORT_REVIEW_APPLY_RECOVERY_NOT_ALLOWED' using errcode='55000';
  end if;
  select count(*) into v_blockers from public.import_review_decisions d
  where d.import_id=p_import_id and d.is_current and d.blocking;
  v_status:=case when v_blockers>0 then 'BLOCKED' else 'READY' end;
  update public.import_review_states set status=v_status,state_version=state_version+1,
    updated_at_utc=now(),updated_by_user_id=p_actor_user_id
  where import_id=p_import_id returning * into s;
  insert into public.import_review_events(import_id,state_version,operation_id,event_code,actor_user_id,event_context_json)
  values(p_import_id,s.state_version,p_operation_id,'APPLY_FAILED_BEFORE_COMMIT_RECOVERED',p_actor_user_id,
    jsonb_build_object('operation_state',o.state,'source_committed',false));
  return jsonb_build_object('ok',true,'import_id',p_import_id,'operation_id',p_operation_id,
    'source_committed',false,'status',s.status,'state_version',s.state_version);
end $function$;

-- import_review_apply_guard_v1(uuid,bigint,text,text,uuid,text,jsonb,jsonb,uuid)
CREATE OR REPLACE FUNCTION public.import_review_apply_guard_v1(p_import_id uuid, p_expected_state_version bigint, p_expected_coverage_fingerprint text, p_expected_preview_fingerprint text, p_operation_id uuid, p_request_hash text, p_selected_action_ids jsonb, p_reference_invalidation_action_ids jsonb, p_actor_user_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare v_s public.import_review_states%rowtype; v_i public.hr_imports%rowtype; v_o public.import_apply_operations%rowtype;
  v_ids text[]; v_db_ids text[]; v_invalidation_ids text[]; v_db_invalidation_ids text[];
  v_op_result jsonb; v_envelope jsonb; v_server_hash text; v_fresh_fingerprint text;
  v_guard_token text;
begin perform public._import_review_assert_actor_v1(p_actor_user_id);
  perform set_config('lock_timeout','1500ms',true);
  if p_operation_id is null or length(btrim(coalesce(p_request_hash,''))) not between 16 and 256
    or jsonb_typeof(coalesce(p_selected_action_ids,'[]'))<>'array'
    or jsonb_typeof(coalesce(p_reference_invalidation_action_ids,'[]'))<>'array'
    or jsonb_array_length(coalesce(p_selected_action_ids,'[]'))>5000
    or jsonb_array_length(coalesce(p_reference_invalidation_action_ids,'[]'))>5000
    or pg_column_size(coalesce(p_selected_action_ids,'[]'))+pg_column_size(coalesce(p_reference_invalidation_action_ids,'[]'))>2097152
  then raise exception 'IMPORT_REVIEW_APPLY_CONTRACT_INVALID' using errcode='22023'; end if;
  if exists(select 1 from jsonb_array_elements(coalesce(p_selected_action_ids,'[]'))x
      where jsonb_typeof(x)<>'string' or trim(both '"' from x::text)!~'^[0-9a-f]{64}$')
    or exists(select 1 from jsonb_array_elements(coalesce(p_reference_invalidation_action_ids,'[]'))x
      where jsonb_typeof(x)<>'string' or trim(both '"' from x::text)!~'^[0-9a-f]{64}$')
  then raise exception 'IMPORT_REVIEW_ACTION_ID_INVALID' using errcode='22023'; end if;
  select coalesce(array_agg(distinct value order by value),array[]::text[]) into v_ids from jsonb_array_elements_text(coalesce(p_selected_action_ids,'[]'))value;
  if cardinality(v_ids)<>jsonb_array_length(coalesce(p_selected_action_ids,'[]')) then raise exception 'IMPORT_REVIEW_ACTION_ID_DUPLICATE' using errcode='22023'; end if;
  select coalesce(array_agg(distinct value order by value),array[]::text[]) into v_invalidation_ids
  from jsonb_array_elements_text(coalesce(p_reference_invalidation_action_ids,'[]')) value;
  if cardinality(v_invalidation_ids)<>jsonb_array_length(coalesce(p_reference_invalidation_action_ids,'[]')) then
    raise exception 'IMPORT_REVIEW_INVALIDATION_ACTION_ID_DUPLICATE' using errcode='22023'; end if;
  select * into v_i from public.hr_imports where id=p_import_id for update; select * into v_s from public.import_review_states where import_id=p_import_id for update;
  if v_i.id is null or v_s.import_id is null then raise exception 'IMPORT_REVIEW_NOT_FOUND' using errcode='P0002'; end if;
  select * into v_o from public.import_apply_operations where id=p_operation_id and import_id=p_import_id;
  if found and v_o.committed_at_utc is not null then
    if lower(btrim(p_request_hash))<>v_o.request_hash then raise exception 'IMPORT_REVIEW_OPERATION_REQUEST_MISMATCH' using errcode='23505'; end if;
    return jsonb_build_object('ok',true,'replay',true,'import_id',p_import_id,'operation_id',p_operation_id,
      'state_version',v_s.state_version,'operation_state',v_o.state,'stored_response',v_o.response_json);
  end if;
  if v_s.status not in ('BLOCKED','READY') or v_s.follow_up_status not in ('NOT_REQUIRED','COMPLETE')
    or v_s.state_version<>p_expected_state_version or v_i.coverage_fingerprint is distinct from p_expected_coverage_fingerprint
    or v_s.preview_fingerprint is distinct from p_expected_preview_fingerprint then raise exception 'IMPORT_REVIEW_APPLY_STALE_OR_NOT_READY' using errcode='40001'; end if;
  select coalesce(array_agg(action_id order by action_id),array[]::text[]) into v_db_ids
  from public._import_review_ready_action_ids_core_v1(p_import_id);
  if cardinality(v_db_ids)=0 then raise exception 'IMPORT_REVIEW_NO_READY_SELECTED_ACTIONS' using errcode='55000'; end if;
  if v_ids is distinct from v_db_ids then raise exception 'IMPORT_REVIEW_SELECTED_ACTION_SET_MISMATCH' using errcode='40001'; end if;
  select coalesce(array_agg(action_id order by action_id),array[]::text[]) into v_db_invalidation_ids
  from public.import_review_decisions where import_id=p_import_id and is_current and action_id=any(v_ids)
    and action_kind='INVALIDATE_REFERENCE';
  if v_invalidation_ids is distinct from v_db_invalidation_ids then
    raise exception 'IMPORT_REVIEW_INVALIDATION_ACTION_SET_MISMATCH' using errcode='40001'; end if;

  -- Freeze the reviewed reconciliation units first, then lock only their exact
  -- source/invoice/current-member scope before the final catalog re-attestation.
  v_envelope:=public._import_review_apply_envelope_core_v1(p_import_id);
  create temporary table if not exists pg_temp.import_review_reconciliation_units_v1(
    action_id text primary key,
    source_identity text not null,
    source_shift_id uuid,
    source_timesheet_id uuid,
    route text not null,
    unit_fingerprint text not null,
    unit_json jsonb not null
  ) on commit drop;
  truncate pg_temp.import_review_reconciliation_units_v1;
  insert into pg_temp.import_review_reconciliation_units_v1(action_id,source_identity,source_shift_id,source_timesheet_id,route,unit_fingerprint,unit_json)
  select u->>'action_id',u->>'source_identity',nullif(u->>'source_shift_id','')::uuid,
    nullif(u->>'source_timesheet_id','')::uuid,u->>'route',u->>'unit_fingerprint',u
  from jsonb_array_elements(coalesce(v_envelope->'reconciliation_units','[]'::jsonb)) u;
  if (select count(*) from pg_temp.import_review_reconciliation_units_v1)<>
     jsonb_array_length(coalesce(v_envelope->'reconciliation_units','[]'::jsonb)) then
    raise exception 'IMPORT_REVIEW_RECONCILIATION_UNIT_INVALID' using errcode='22023';
  end if;
  perform 1 from public.invoices i where i.id in (
    select distinct x.value::uuid from pg_temp.import_review_reconciliation_units_v1 u
    cross join lateral jsonb_array_elements_text(coalesce(u.unit_json->'B_effective_invoice_ids','[]'::jsonb)) x(value)
  ) order by i.id for update;
  perform 1 from public.invoice_lines il where il.id in (
    select distinct x.value::uuid from pg_temp.import_review_reconciliation_units_v1 u
    cross join lateral jsonb_array_elements_text(coalesce(u.unit_json->'B_effective_invoice_line_ids','[]'::jsonb)) x(value)
  ) order by il.id for update;
  perform 1 from public.invoice_operations io where io.entity_id in (
    select distinct x.value::uuid from pg_temp.import_review_reconciliation_units_v1 u
    cross join lateral jsonb_array_elements_text(coalesce(u.unit_json->'B_effective_invoice_ids','[]'::jsonb)) x(value)
  ) and io.status not in ('COMPLETE','FAILED','CANCELLED') order by io.id for update;
  perform 1 from public.invoice_operation_chunks c where c.operation_id in (
    select io.id from public.invoice_operations io where io.entity_id in (
      select distinct x.value::uuid from pg_temp.import_review_reconciliation_units_v1 u
      cross join lateral jsonb_array_elements_text(coalesce(u.unit_json->'B_effective_invoice_ids','[]'::jsonb)) x(value)
    ) and io.status not in ('COMPLETE','FAILED','CANCELLED')
  ) and c.status not in ('COMPLETE','FAILED','CANCELLED') order by c.id for update;
  perform 1 from public.nhsp_shifts s where s.id in (
    select source_shift_id from pg_temp.import_review_reconciliation_units_v1 where source_shift_id is not null
  ) order by s.id for update;
  perform 1 from public.timesheets t
  where t.timesheet_id in (
    select source_timesheet_id from pg_temp.import_review_reconciliation_units_v1 where source_timesheet_id is not null
    union
    select x.value::uuid from pg_temp.import_review_reconciliation_units_v1 u
      cross join lateral jsonb_array_elements_text(coalesce(u.unit_json->'M_active_member_ids','[]'::jsonb)) x(value)
  ) and t.archived_at_utc is null
  order by t.timesheet_id for update;
  perform 1 from public.timesheets_financials tf
  where tf.is_current and tf.timesheet_id in (
    select source_timesheet_id from pg_temp.import_review_reconciliation_units_v1 where source_timesheet_id is not null
    union
    select x.value::uuid from pg_temp.import_review_reconciliation_units_v1 u
      cross join lateral jsonb_array_elements_text(coalesce(u.unit_json->'M_active_member_ids','[]'::jsonb)) x(value)
  )
  order by tf.timesheet_id,tf.id for update;
  perform 1 from public.contract_weeks cw where cw.timesheet_id in (
    select source_timesheet_id from pg_temp.import_review_reconciliation_units_v1 where source_timesheet_id is not null
    union
    select x.value::uuid from pg_temp.import_review_reconciliation_units_v1 u
      cross join lateral jsonb_array_elements_text(coalesce(u.unit_json->'M_active_member_ids','[]'::jsonb)) x(value)
  ) order by cw.id for update;

  create temporary table if not exists pg_temp.review_apply_fresh_actions on commit drop as
    select * from public._import_review_action_catalog_core_v1(p_import_id,v_s.preview_generation,500) with no data;
  truncate pg_temp.review_apply_fresh_actions;
  insert into pg_temp.review_apply_fresh_actions
    select * from public._import_review_action_catalog_core_v1(p_import_id,v_s.preview_generation,500);
  -- Partial reviews fingerprint only work that is still open.  Refresh uses the
  -- same rule before persisting preview_fingerprint, so final re-attestation
  -- must not re-introduce outcomes that this import already committed.
  delete from pg_temp.review_apply_fresh_actions n
  using public.import_review_action_outcomes o
  where o.import_id=p_import_id and o.action_id=n.action_id;
  select public._import_review_hash_v1(coalesce(string_agg(action_id||':'||evidence_fingerprint,'|' order by action_id),''))
  into v_fresh_fingerprint from pg_temp.review_apply_fresh_actions;
  if v_fresh_fingerprint is distinct from v_s.preview_fingerprint then
    raise exception 'IMPORT_REVIEW_APPLY_EVIDENCE_STALE' using errcode='40001'; end if;
  if exists(
    select 1 from pg_temp.review_apply_fresh_actions b
    where b.blocking and exists (
      select 1 from public.import_review_decisions d
      where d.import_id=p_import_id and d.action_id=any(v_ids)
        and b.candidate_id=d.candidate_id
        and b.client_id=d.client_id
    )
  ) then raise exception 'IMPORT_REVIEW_APPLY_REFRESH_REQUIRED' using errcode='40001'; end if;
  if exists(
    select 1 from public.import_review_decisions d
    left join pg_temp.review_apply_fresh_actions n on n.action_id=d.action_id
    where d.import_id=p_import_id and d.is_current and d.action_id=any(v_ids)
      and (n.action_id is null or n.evidence_fingerprint is distinct from d.evidence_fingerprint
        or not n.selectable or n.blocking)
  ) then raise exception 'IMPORT_REVIEW_SELECTED_ACTION_STALE' using errcode='40001'; end if;
  if exists(select 1 from public.import_review_decisions d where d.import_id=p_import_id and d.is_current and d.action_id=any(v_ids)
    and d.action_kind in ('INCLUDE_SHIFT','APPLY_AMENDMENT','APPLY_CANCELLATION','INVALIDATE_REFERENCE','MARK_VALIDATION_ERROR') and d.timesheet_id is not null
    and coalesce((public._import_review_timesheet_protection_core_v1(d.timesheet_id)->>'active_pay_draft')::boolean,false)) then raise exception 'BLOCKED_ACTIVE_PAY_DRAFT' using errcode='55000'; end if;
  if v_i.source_system='HEALTHROSTER_DAILY'::public.hr_source_enum and exists(
    select 1 from public.import_review_daily_timesheet_resolutions r
    where r.import_id=p_import_id and r.status='CURRENT' and r.resolved_timesheet_id is not null
      and exists(select 1 from public.import_review_decisions d where d.import_id=p_import_id
        and d.hr_row_id=r.hr_row_id and d.action_id=any(v_ids))
      and coalesce((public._import_review_timesheet_protection_core_v1(r.resolved_timesheet_id)->>'active_pay_draft')::boolean,false)
  ) then raise exception 'BLOCKED_ACTIVE_PAY_DRAFT' using errcode='55000'; end if;
  if exists(select 1 from public.import_review_decisions d
    where d.import_id=p_import_id and d.is_current and d.action_id=any(v_ids) and d.action_kind='INVALIDATE_REFERENCE'
      and coalesce((public._import_review_timesheet_protection_core_v1(d.timesheet_id)->>'protected')::boolean,false)) then
    raise exception 'IMPORT_REVIEW_REFERENCE_INVALIDATION_PROTECTED' using errcode='55000'; end if;
  v_envelope:=public._import_review_apply_envelope_core_v1(p_import_id);
  if exists(
    select 1
    from pg_temp.import_review_reconciliation_units_v1 frozen
    where not exists (
      select 1
      from jsonb_array_elements(coalesce(v_envelope->'reconciliation_units','[]'::jsonb)) current_unit(u)
      where current_unit.u->>'action_id'=frozen.action_id
        and current_unit.u->>'unit_fingerprint'=frozen.unit_fingerprint
    )
  ) or (select count(*) from pg_temp.import_review_reconciliation_units_v1)<>
      jsonb_array_length(coalesce(v_envelope->'reconciliation_units','[]'::jsonb)) then
    raise exception 'IMPORT_REVIEW_SELECTED_ACTION_STALE' using errcode='40001';
  end if;
  v_server_hash:=public._import_review_hash_v1(v_envelope::text);
  if lower(btrim(p_request_hash))<>v_server_hash then
    raise exception 'IMPORT_REVIEW_APPLY_REQUEST_HASH_MISMATCH' using errcode='22023',detail=jsonb_build_object('server_request_hash',v_server_hash)::text;
  end if;
  v_guard_token:=encode(gen_random_bytes(32),'hex');
  perform set_config('cloudtms.import_reconciliation_guard_token',v_guard_token,true);
  perform set_config('cloudtms.import_reconciliation_operation_id',p_operation_id::text,true);
  perform set_config('cloudtms.import_reconciliation_request_hash',v_server_hash,true);
  perform set_config('cloudtms.import_reconciliation_unit_fingerprints',coalesce((
    select string_agg(unit_fingerprint,',' order by action_id) from pg_temp.import_review_reconciliation_units_v1
  ),''),true);
  v_op_result:=public._import_apply_operation_claim_core_v2(p_operation_id,p_import_id,v_i.source_system,
    concat_ws(':',coalesce(v_i.revision_group_id,v_i.id),coalesce(v_i.revision_no,1)),v_server_hash,p_actor_user_id,v_envelope);
  update public.import_review_states set status='APPLYING',state_version=state_version+1,last_operation_id=p_operation_id,updated_at_utc=now(),updated_by_user_id=p_actor_user_id where import_id=p_import_id returning * into v_s;
  insert into public.import_review_events(import_id,state_version,operation_id,event_code,actor_user_id,event_context_json) values(p_import_id,v_s.state_version,p_operation_id,'APPLY_STARTED',p_actor_user_id,jsonb_build_object('request_hash',btrim(p_request_hash),'selected_count',cardinality(v_ids),'batch_scope_units',v_envelope->'batch_scope_units'));
  return jsonb_build_object('ok',true,'import_id',p_import_id,'operation_id',p_operation_id,'state_version',v_s.state_version,'selected_action_ids',to_jsonb(v_ids),'operation_state',v_op_result->>'state'); end $function$;

-- import_review_apply_status_get_v1(uuid,uuid,text)
CREATE OR REPLACE FUNCTION public.import_review_apply_status_get_v1(p_import_id uuid, p_operation_id uuid, p_request_hash text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare s public.import_review_states%rowtype; o public.import_apply_operations%rowtype;
begin select * into s from public.import_review_states where import_id=p_import_id; if not found then raise exception 'IMPORT_REVIEW_NOT_FOUND' using errcode='P0002'; end if;
  if p_operation_id is not null then select * into o from public.import_apply_operations where id=p_operation_id and import_id=p_import_id; end if;
  if o.id is not null and p_request_hash is not null and o.request_hash<>btrim(p_request_hash) then return jsonb_build_object('ok',false,'status','OPERATION_REQUEST_MISMATCH'); end if;
  return jsonb_build_object('ok',true,'schema_contract_version',s.schema_contract_version,
    'import_id',p_import_id,'review_status',s.status,'follow_up_status',s.follow_up_status,'state_version',s.state_version,
    'follow_up_error_code',s.follow_up_error_code,'follow_up_error_message',s.follow_up_error_message,
    'follow_up_retry_count',s.follow_up_retry_count,
    'operation_id',o.id,'operation_state',o.state,'outcome',case when o.committed_at_utc is not null and s.follow_up_status in ('PENDING','FAILED_RETRYABLE') then 'COMMITTED_WITH_FOLLOW_UP_PENDING'
      when o.committed_at_utc is not null and s.status='APPLIED' then 'COMMITTED_APPLIED'
      when o.committed_at_utc is not null then 'COMMITTED_PARTIAL'
      when s.status='APPLYING' then 'IN_PROGRESS' when s.status in ('ABANDONED','SUPERSEDED') then s.status
      when o.id is null then 'NOT_STARTED' when o.state='FAILED_BEFORE_COMMIT' then 'FAILED_BEFORE_COMMIT' else 'NOT_COMMITTED' end,
    'stored_response',case when o.committed_at_utc is not null then o.response_json end,'committed_at_utc',o.committed_at_utc); end $function$;

-- import_review_attachment_preparation_targets_v1(uuid,uuid,integer)
CREATE OR REPLACE FUNCTION public.import_review_attachment_preparation_targets_v1(p_import_id uuid, p_actor_user_id uuid DEFAULT NULL::uuid, p_max_targets integer DEFAULT 100)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare v_commands jsonb; v_count integer;
begin
  perform public._import_review_assert_actor_v1(p_actor_user_id);
  if p_import_id is null or p_max_targets<1 or p_max_targets>100 then
    raise exception 'IMPORT_REVIEW_ATTACHMENT_TARGET_INPUT_INVALID' using errcode='22023';
  end if;
  if not exists(select 1 from public.import_review_states s where s.import_id=p_import_id) then
    raise exception 'IMPORT_REVIEW_NOT_FOUND' using errcode='P0002';
  end if;
  with targets as (
    select distinct d.timesheet_id,e.evidence_json
    from public.import_review_decisions d
    cross join lateral (select public._import_review_query_evidence_core_v1(d.timesheet_id) evidence_json) e
    where d.import_id=p_import_id and d.is_current
      and d.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER')
      and d.timesheet_id is not null
      and coalesce((e.evidence_json->>'preparation_required')::boolean,false)
    order by d.timesheet_id
    limit p_max_targets+1
  ), bounded as (
    select * from targets order by timesheet_id limit p_max_targets
  )
  select coalesce(jsonb_agg(jsonb_build_object(
      'command_type','VIEW_TIMESHEET_DOCUMENT',
      'timesheet_id',timesheet_id,
      'purpose','TIMESHEET',
      'priority_reason','IMPORT_REVIEW_EMAIL_EVIDENCE',
      'template_version','timesheet-professional-v2',
      'command_token','import-review-evidence:'||public._import_review_hash_v1(concat_ws('|',p_import_id,timesheet_id,
        evidence_json->>'evidence_fingerprint'))
    ) order by timesheet_id),'[]'::jsonb),
    (select count(*) from targets)
  into v_commands,v_count
  from bounded;
  if v_count>p_max_targets then
    raise exception 'IMPORT_REVIEW_ATTACHMENT_TARGET_LIMIT_EXCEEDED' using errcode='54000';
  end if;
  return jsonb_build_object('ok',true,'import_id',p_import_id,'target_count',coalesce(v_count,0),'commands',v_commands);
end $function$;

-- import_review_contract_version_get_v1()
CREATE OR REPLACE FUNCTION public.import_review_contract_version_get_v1()
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_catalog'
AS $function$
DECLARE
  v_projection_contract jsonb :=
    public._pay_workbench_candidate_projection_contract();
  v_selection_carry_table_oid oid :=
    to_regclass(
      'public.banking_pay_workbench_selection_carry_registrations'
    );
  v_canonical_contract_version text;
  v_targeted_family_materialisation_version text;
BEGIN
  v_canonical_contract_version := CASE
    WHEN v_selection_carry_table_oid IS NOT NULL
      AND to_regprocedure(
        'public.pay_workbench_session_carry_forward_preview_selections_v1(uuid,uuid,jsonb)'
      ) IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM pg_catalog.pg_trigger AS trigger_row
        WHERE trigger_row.tgrelid =
          'public.banking_pay_workbench_preview_rows'::regclass
          AND trigger_row.tgname =
            'trg_banking_pay_preview_selection_carry_apply'
          AND trigger_row.tgenabled <> 'D'
          AND trigger_row.tgisinternal IS FALSE
      )
      THEN v_projection_contract ->> 'canonical_correction_carrier_version'
    ELSE 'BANKING_PAY_CANONICAL_CORRECTION_CARRIER_INCOMPLETE'
  END;

  v_targeted_family_materialisation_version := CASE
    WHEN to_regprocedure(
      'public._pay_workbench_refresh_dependency_closure_v1(uuid,uuid[],uuid[],uuid[],integer,integer)'
    ) IS NOT NULL
      THEN v_projection_contract ->> 'targeted_family_materialisation_version'
    ELSE 'BANKING_PAY_TARGETED_FAMILY_MATERIALISATION_INCOMPLETE'
  END;

  RETURN jsonb_build_object(
    'ok', true,
    'schema_contract_version', 'IMPORT_REVIEW_DB_V1',
    'apply_envelope_version', 'IMPORT_REVIEW_APPLY_V1',
    'apply_operation_version', 'IMPORT_APPLY_OPERATION_V2',
    'correction_operation_version', 'IMPORT_CORRECTION_OPERATION_V2',
    'follow_up_component_version', 'IMPORT_REVIEW_FOLLOW_UP_COMPONENT_V1',
    'tsfin_follow_up_settlement_version', 'IMPORT_REVIEW_TSFIN_SETTLEMENT_V1',
    'incremental_apply_version', 'IMPORT_REVIEW_INCREMENTAL_APPLY_V1',
    'review_ui_contract_version', 'IMPORT_REVIEW_UI_V6',
    'email_grouping_version', 'TIMESHEET_QUERY_RECIPIENT_EMAIL_V1',
    'canonical_correction_carrier_version',
      v_canonical_contract_version,
    'targeted_family_materialisation_version',
      v_targeted_family_materialisation_version,
    'legacy_contracts_supported', false
  );
END;
$function$;

-- import_review_correction_generation_transition_v1(uuid,uuid,text,text,uuid,text[],timestamp with time zone)
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
  v_request_units jsonb:='[]'::jsonb;
  v_applied_units jsonb:='[]'::jsonb;
  v_policy_units jsonb:='[]'::jsonb;
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
  v_all_authorised boolean:=false;
  v_any_authorised boolean:=false;
  v_unit_fingerprints jsonb:='[]'::jsonb;
  v_expected_a_day numeric;
  v_expected_a_night numeric;
  v_expected_a_sat numeric;
  v_expected_a_sun numeric;
  v_expected_a_bh numeric;
  v_expected_a_total numeric;
  v_frozen_a_bucket_total numeric;
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
  perform set_config('cloudtms.import_reconciliation_operation_id',p_operation_id::text,true);

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
    v_request_units:=coalesce(v_operation.response_json#>'{request_envelope,reconciliation_units}','[]'::jsonb);
    v_applied_units:=coalesce(v_operation.response_json->'reconciliation_units','[]'::jsonb);
    v_policy_units:=coalesce(v_operation.response_json#>'{correction_operation_contract,correction_units}','[]'::jsonb);
    if cardinality(coalesce(p_action_ids,array[]::text[]))>0 then
      select coalesce(jsonb_agg(u order by u->>'action_id'),'[]'::jsonb) into v_request_units
      from jsonb_array_elements(v_request_units) u where u->>'action_id'=any(p_action_ids);
      select coalesce(jsonb_agg(u order by u->>'action_id'),'[]'::jsonb) into v_applied_units
      from jsonb_array_elements(v_applied_units) u where u->>'action_id'=any(p_action_ids);
      select coalesce(jsonb_agg(u order by u->>'action_id'),'[]'::jsonb) into v_policy_units
      from jsonb_array_elements(v_policy_units) u where u->>'action_id'=any(p_action_ids);
    end if;
    if exists(
      select 1 from jsonb_array_elements(v_request_units) request
      where request->>'route' in ('AMEND_PAID_UNINVOICED_SOURCE','AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT')
        and ((select count(*) from jsonb_array_elements(v_applied_units) applied where applied->>'action_id'=request->>'action_id')<>1
          or (select count(*) from jsonb_array_elements(v_policy_units) policy where policy->>'action_id'=request->>'action_id')<>1)
    ) then
      raise exception 'IMPORT_REVIEW_RECONCILIATION_EVIDENCE_CONTRACT_INVALID' using errcode='40001';
    end if;
    select coalesce(jsonb_agg(
      request
      || case when applied is null then '{}'::jsonb else jsonb_build_object(
          'correction_id',applied->>'correction_id','applied_member_ids',applied->'applied_member_ids',
          'reversal_timesheet_id',applied->>'reversal_timesheet_id','replacement_timesheet_id',applied->>'replacement_timesheet_id',
          'parent_timesheet_id',applied->>'parent_timesheet_id','repair_identity_mode',applied->>'repair_identity_mode',
          'applied_result_fingerprint',applied->>'applied_result_fingerprint',
          'reviewed_unit_fingerprint',applied->>'reviewed_unit_fingerprint','applied_reconciliation_fingerprint',applied->>'reconciliation_fingerprint',
          'applied_timesheet_id',applied->>'applied_timesheet_id','rollover_mode',applied->>'rollover_mode',
          'historical_paid_tsfin_id',applied->>'historical_paid_tsfin_id','current_shell_tsfin_id',applied->>'current_shell_tsfin_id',
          'applied_intended_authorisation_action',applied->>'intended_authorisation_action') end
      || case when policy is null then '{}'::jsonb else jsonb_build_object(
          'operation_policy_envelope',policy->'policy_envelope',
          'operation_policy_fingerprint',policy->>'policy_envelope_fingerprint',
          'operation_policy_root_timesheet_id',policy->>'root_timesheet_id',
          'operation_policy_source_row_key',policy->>'source_row_key',
          'operation_policy_source_shift_id',policy->>'source_shift_id') end
      order by request->>'action_id'),'[]'::jsonb)
    into v_units
    from jsonb_array_elements(v_request_units) request
    left join lateral (select u applied from jsonb_array_elements(v_applied_units) u where u->>'action_id'=request->>'action_id' limit 1) a on true
    left join lateral (select u policy from jsonb_array_elements(v_policy_units) u where u->>'action_id'=request->>'action_id' limit 1) p on true;
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
       or u->>'unit_fingerprint' is distinct from public._import_review_hash_v1(concat_ws('|','unit-v2',
         u->>'action_id',u->>'source_identity',u->>'source_shift_id',u->>'route',u->>'reconciliation_mode',
         u->>'reconciliation_fingerprint',u->>'review_policy_basis_kind',u->>'review_policy_basis_fingerprint',
         outcome.evidence_fingerprint))
  ) then
    raise exception 'IMPORT_REVIEW_RECONCILIATION_FINGERPRINT_MISMATCH' using errcode='40001';
  end if;
  if v_action<>'PREPARE' and exists(
    select 1 from jsonb_array_elements(v_units) u
    where u->>'route' in ('AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT') and (
      nullif(u->>'correction_id','') is null
      or nullif(u->>'reviewed_unit_fingerprint','') is distinct from nullif(u->>'unit_fingerprint','')
      or nullif(u->>'applied_reconciliation_fingerprint','') is distinct from nullif(u->>'reconciliation_fingerprint','')
      or nullif(u->>'operation_policy_source_row_key','') is distinct from nullif(u->>'source_identity','')
      or nullif(u->>'operation_policy_source_shift_id','') is distinct from nullif(u->>'source_shift_id','')
      or nullif(u->>'operation_policy_root_timesheet_id','') is distinct from nullif(u->>'source_timesheet_id','')
      or jsonb_typeof(u->'applied_member_ids')<>'array' or jsonb_array_length(u->'applied_member_ids')<>2
      or coalesce(u->>'reversal_timesheet_id','')!~*'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      or coalesce(u->>'replacement_timesheet_id','')!~*'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      or u->>'reversal_timesheet_id'=u->>'replacement_timesheet_id'
      or not (u->'applied_member_ids' @> jsonb_build_array(u->>'reversal_timesheet_id')
        and u->'applied_member_ids' @> jsonb_build_array(u->>'replacement_timesheet_id'))
      or u->>'applied_result_fingerprint' is distinct from encode(digest(convert_to(jsonb_build_object(
        'correction_id',u->>'correction_id',
        'reversal_timesheet_id',(u->>'reversal_timesheet_id')::uuid,
        'replacement_timesheet_id',(u->>'replacement_timesheet_id')::uuid,
        'M_active_member_ids',u->'applied_member_ids',
        'applied_member_ids',u->'applied_member_ids',
        'parent_timesheet_id',(u->>'parent_timesheet_id')::uuid,
        'repair_identity_mode',u->>'repair_identity_mode',
        'reviewed_unit_fingerprint',u->>'reviewed_unit_fingerprint',
        'reconciliation_fingerprint',u->>'applied_reconciliation_fingerprint')::text,'UTF8'),'sha256'),'hex')
      or jsonb_typeof(u->'operation_policy_envelope')<>'object'
      or nullif(u->>'operation_policy_fingerprint','') is null
      or u#>>'{operation_policy_envelope,envelope_fingerprint}' is distinct from u->>'operation_policy_fingerprint'
      or encode(digest(convert_to(((u->'operation_policy_envelope')-'envelope_fingerprint')::text,'UTF8'),'sha256'),'hex')
        is distinct from u->>'operation_policy_fingerprint'
    )
  ) then
    raise exception 'IMPORT_REVIEW_RECONCILIATION_EVIDENCE_CONTRACT_INVALID' using errcode='40001';
  end if;
  if v_action<>'PREPARE' and exists(
    select 1 from jsonb_array_elements(v_units) u
    where u->>'route'='AMEND_PAID_UNINVOICED_SOURCE' and (
      nullif(u->>'reviewed_unit_fingerprint','') is distinct from nullif(u->>'unit_fingerprint','')
      or nullif(u->>'applied_reconciliation_fingerprint','') is distinct from nullif(u->>'reconciliation_fingerprint','')
      or nullif(u->>'applied_timesheet_id','') is distinct from nullif(u->>'source_timesheet_id','')
      or nullif(u->>'applied_intended_authorisation_action','') is distinct from nullif(u->>'intended_authorisation_action','')
      or coalesce(u->>'rollover_mode','') not in ('CREATED_CURRENT_OPERATION_SHELL','REUSED_COMPLETED_OPERATION_SHELL')
      or coalesce(u->>'historical_paid_tsfin_id','')!~*'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      or coalesce(u->>'current_shell_tsfin_id','')!~*'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      or nullif(u->>'operation_policy_source_row_key','') is distinct from nullif(u->>'source_identity','')
      or nullif(u->>'operation_policy_source_shift_id','') is distinct from nullif(u->>'source_shift_id','')
      or nullif(u->>'operation_policy_root_timesheet_id','') is distinct from nullif(u->>'source_timesheet_id','')
      or jsonb_typeof(u->'operation_policy_envelope')<>'object'
      or nullif(u->>'operation_policy_fingerprint','') is null
      or u#>>'{operation_policy_envelope,envelope_fingerprint}' is distinct from u->>'operation_policy_fingerprint'
      or encode(digest(convert_to(((u->'operation_policy_envelope')-'envelope_fingerprint')::text,'UTF8'),'sha256'),'hex')
        is distinct from u->>'operation_policy_fingerprint'
      or u->>'applied_result_fingerprint' is distinct from encode(digest(convert_to(jsonb_build_object(
        'applied_timesheet_id',(u->>'applied_timesheet_id')::uuid,
        'rollover_mode',u->>'rollover_mode',
        'historical_paid_tsfin_id',(u->>'historical_paid_tsfin_id')::uuid,
        'current_shell_tsfin_id',(u->>'current_shell_tsfin_id')::uuid,
        'intended_authorisation_action',u->>'intended_authorisation_action',
        'reviewed_unit_fingerprint',u->>'reviewed_unit_fingerprint',
        'reconciliation_fingerprint',u->>'reconciliation_fingerprint')::text,'UTF8'),'sha256'),'hex')
    )
  ) then
    raise exception 'IMPORT_REVIEW_RECONCILIATION_EVIDENCE_CONTRACT_INVALID' using errcode='40001';
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
      raise exception 'IMPORT_REVIEW_SELECTED_ACTION_STALE' using errcode='40001';
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
    -- The review-time effective invoice fingerprint also attests mutable role
    -- evidence.  That evidence is expected to change when this operation
    -- repairs the approved pair, so it is not a valid post-mutation equality
    -- check.  Re-attest the immutable B authority directly instead: the exact
    -- invoice and line identities, signed hours and money, and terminal
    -- schedule must all remain byte-for-byte equal to the reviewed envelope.
    if coalesce(v_balance->'effective_invoice_ids','[]'::jsonb)
          is distinct from coalesce(v_unit->'B_effective_invoice_ids','[]'::jsonb)
       or coalesce(v_balance->'effective_invoice_line_ids','[]'::jsonb)
          is distinct from coalesce(v_unit->'B_effective_invoice_line_ids','[]'::jsonb)
       or coalesce(v_balance->'B_hours','{}'::jsonb)
          is distinct from coalesce(v_unit->'B_hours','{}'::jsonb)
       or coalesce(v_balance->'B_financials','{}'::jsonb)
          is distinct from coalesce(v_unit->'B_financials','{}'::jsonb)
       or coalesce(v_balance->'B_standard_schedule_json','[]'::jsonb)
          is distinct from coalesce(v_unit->'B_standard_schedule_json','[]'::jsonb) then
      raise exception 'IMPORT_REVIEW_SELECTED_ACTION_STALE' using errcode='40001';
    end if;
    if v_unit->>'route' in ('AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT') then
      select count(*) into v_member_count from public.timesheets t
      where t.correction_id=v_unit->>'correction_id' and t.is_current and t.archived_at_utc is null
        and t.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT');
      if v_member_count<>2 or not exists(select 1 from public.timesheets t where t.correction_id=v_unit->>'correction_id'
          and t.is_current and t.archived_at_utc is null and t.correction_kind='CHANGED_HOURS_REVERSAL')
        or not exists(select 1 from public.timesheets t where t.correction_id=v_unit->>'correction_id'
          and t.is_current and t.archived_at_utc is null and t.correction_kind='CHANGED_HOURS_REPLACEMENT')
        or exists(select 1 from public.timesheets t where t.correction_id=v_unit->>'correction_id'
          and t.is_current and t.archived_at_utc is null
          and not (v_unit->'applied_member_ids' @> jsonb_build_array(t.timesheet_id::text)))
        or not exists(select 1 from public.timesheets t where t.timesheet_id=(v_unit->>'reversal_timesheet_id')::uuid
          and t.correction_id=v_unit->>'correction_id' and t.correction_kind='CHANGED_HOURS_REVERSAL' and t.is_current and t.archived_at_utc is null)
        or not exists(select 1 from public.timesheets t where t.timesheet_id=(v_unit->>'replacement_timesheet_id')::uuid
          and t.correction_id=v_unit->>'correction_id' and t.correction_kind='CHANGED_HOURS_REPLACEMENT' and t.is_current and t.archived_at_utc is null) then
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
          and (coalesce(public._ctms_correction_policy_envelope_read_v1(t.timesheet_id)->>'envelope_fingerprint','')
            is distinct from coalesce(v_unit->>'operation_policy_fingerprint','')
            or public._ctms_correction_policy_envelope_read_v1(t.timesheet_id)
              is distinct from v_unit->'operation_policy_envelope'))
        or (select count(distinct public._ctms_correction_policy_envelope_read_v1(t.timesheet_id)->>'envelope_fingerprint')
            from public.timesheets t where t.correction_id=v_unit->>'correction_id'
              and t.is_current and t.archived_at_utc is null
              and t.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT'))<>1 then
        raise exception 'IMPORT_REVIEW_RECONCILIATION_POLICY_MISMATCH' using errcode='40001';
      end if;
      select h.hours_day,h.hours_night,h.hours_sat,h.hours_sun,h.hours_bh,h.total_hours
      into v_expected_a_day,v_expected_a_night,v_expected_a_sat,v_expected_a_sun,v_expected_a_bh,v_expected_a_total
      from public.timesheets t
      join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
      cross join lateral public._wkimp_bucket_hours_from_policy(
        coalesce(tf.policy_snapshot_json,'{}'::jsonb),
        (v_unit#>>'{A_schedule_json,0,start_utc}')::timestamptz,
        (v_unit#>>'{A_schedule_json,0,end_utc}')::timestamptz,
        coalesce((v_unit#>>'{A_schedule_json,0,break_mins}')::integer,0)
      ) h
      where t.timesheet_id=(v_unit->>'replacement_timesheet_id')::uuid
        and t.correction_id=v_unit->>'correction_id'
        and t.correction_kind='CHANGED_HOURS_REPLACEMENT'
        and t.is_current and t.archived_at_utc is null
      limit 1;
      v_frozen_a_bucket_total:=coalesce((v_unit#>>'{A_hours,hours_day}')::numeric,0)
        +coalesce((v_unit#>>'{A_hours,hours_night}')::numeric,0)
        +coalesce((v_unit#>>'{A_hours,hours_sat}')::numeric,0)
        +coalesce((v_unit#>>'{A_hours,hours_sun}')::numeric,0)
        +coalesce((v_unit#>>'{A_hours,hours_bh}')::numeric,0);
      if v_expected_a_total is null
        or v_expected_a_total is distinct from coalesce((v_unit#>>'{A_hours,total_hours}')::numeric,0)
        or (v_frozen_a_bucket_total<>0 and (
          v_expected_a_day is distinct from coalesce((v_unit#>>'{A_hours,hours_day}')::numeric,0)
          or v_expected_a_night is distinct from coalesce((v_unit#>>'{A_hours,hours_night}')::numeric,0)
          or v_expected_a_sat is distinct from coalesce((v_unit#>>'{A_hours,hours_sat}')::numeric,0)
          or v_expected_a_sun is distinct from coalesce((v_unit#>>'{A_hours,hours_sun}')::numeric,0)
          or v_expected_a_bh is distinct from coalesce((v_unit#>>'{A_hours,hours_bh}')::numeric,0)
        )) then
        raise exception 'IMPORT_REVIEW_RECONCILIATION_BALANCE_MISMATCH' using errcode='55000';
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
      select count(*) into v_bad_count
      from public.timesheets t
      join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
      where t.correction_id=v_unit->>'correction_id' and t.is_current and t.archived_at_utc is null
        and case t.correction_kind
          when 'CHANGED_HOURS_REVERSAL' then
            tf.hours_day<>-coalesce((v_unit#>>'{B_hours,hours_day}')::numeric,0)
            or tf.hours_night<>-coalesce((v_unit#>>'{B_hours,hours_night}')::numeric,0)
            or tf.hours_sat<>-coalesce((v_unit#>>'{B_hours,hours_sat}')::numeric,0)
            or tf.hours_sun<>-coalesce((v_unit#>>'{B_hours,hours_sun}')::numeric,0)
            or tf.hours_bh<>-coalesce((v_unit#>>'{B_hours,hours_bh}')::numeric,0)
            or tf.total_pay_ex_vat<>-coalesce((v_unit#>>'{B_financials,pay_ex_vat}')::numeric,0)
            or tf.total_charge_ex_vat<>-coalesce((v_unit#>>'{B_financials,charge_ex_vat}')::numeric,0)
          when 'CHANGED_HOURS_REPLACEMENT' then
            tf.hours_day<>v_expected_a_day or tf.hours_night<>v_expected_a_night
            or tf.hours_sat<>v_expected_a_sat or tf.hours_sun<>v_expected_a_sun
            or tf.hours_bh<>v_expected_a_bh
          else false
        end;
      if v_bad_count>0 then
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
          and (v_unit->>'route'<>'AMEND_PAID_UNINVOICED_SOURCE'
            or coalesce(tf.policy_snapshot_json->>'correction_financials_policy_envelope_fingerprint',
              tf.policy_snapshot_json#>>'{correction_financials_policy_envelope,envelope_fingerprint}')
              =v_unit->>'operation_policy_fingerprint')
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

-- import_review_create_v1(uuid,text,date,date,jsonb,jsonb,text,text,uuid,text)
CREATE OR REPLACE FUNCTION public.import_review_create_v1(p_import_id uuid, p_coverage_mode text, p_coverage_start_date date, p_coverage_end_date date, p_scope_clients jsonb DEFAULT '[]'::jsonb, p_scope_candidates jsonb DEFAULT '[]'::jsonb, p_expected_source_file_sha256 text DEFAULT NULL::text, p_expected_parser_version text DEFAULT NULL::text, p_actor_user_id uuid DEFAULT NULL::uuid, p_operation_key text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
begin
  return public._import_review_create_core_v2(p_import_id,p_coverage_mode,p_coverage_start_date,p_coverage_end_date,
    p_scope_clients,p_scope_candidates,p_expected_source_file_sha256,p_expected_parser_version,
    p_actor_user_id,p_operation_key,null,null);
end $function$;

-- import_review_follow_up_component_update_v1(uuid,uuid,text,text,text,text,text,text,uuid)
CREATE OR REPLACE FUNCTION public.import_review_follow_up_component_update_v1(p_import_id uuid, p_operation_id uuid, p_request_hash text, p_component text, p_expected_component_status text, p_new_component_status text, p_error_code text DEFAULT NULL::text, p_error_message text DEFAULT NULL::text, p_actor_user_id uuid DEFAULT NULL::uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v public.import_review_states%rowtype; o public.import_apply_operations%rowtype; v_result jsonb;
  v_component text:=upper(btrim(coalesce(p_component,'')));
  v_expected text:=upper(btrim(coalesce(p_expected_component_status,'')));
  v_new text:=upper(btrim(coalesce(p_new_component_status,'')));
  v_current text; v_status_key text; v_error_code_key text; v_error_message_key text; v_replay boolean:=false;
begin
  perform public._import_review_assert_actor_v1(p_actor_user_id);
  if p_import_id is null or p_operation_id is null or p_request_hash is null or btrim(p_request_hash)!~'^[0-9a-f]{64}$' then
    raise exception 'IMPORT_REVIEW_FOLLOW_UP_COMPONENT_INPUT_INVALID' using errcode='22023';
  end if;
  if v_component not in ('EMAIL','TSFIN') or v_expected not in ('PENDING','COMPLETE','FAILED_RETRYABLE','NOT_REQUIRED')
    or v_new not in ('PENDING','COMPLETE','FAILED_RETRYABLE','NOT_REQUIRED') then
    raise exception 'IMPORT_REVIEW_FOLLOW_UP_COMPONENT_INPUT_INVALID' using errcode='22023';
  end if;
  if length(coalesce(p_error_code,''))>128 or length(coalesce(p_error_message,''))>1000
    or (v_new='FAILED_RETRYABLE' and (nullif(btrim(coalesce(p_error_code,'')),'') is null or nullif(btrim(coalesce(p_error_message,'')),'') is null)) then
    raise exception 'IMPORT_REVIEW_FOLLOW_UP_COMPONENT_ERROR_INVALID' using errcode='22023';
  end if;
  select * into v from public.import_review_states where import_id=p_import_id for update;
  if not found then raise exception 'IMPORT_REVIEW_NOT_FOUND' using errcode='P0002'; end if;
  if v.status not in ('IN_REVIEW','BLOCKED','READY','APPLIED') or v.last_operation_id is distinct from p_operation_id then
    raise exception 'IMPORT_REVIEW_FOLLOW_UP_NOT_COMMITTED_REVIEW' using errcode='55000';
  end if;
  select * into o from public.import_apply_operations where id=p_operation_id and import_id=p_import_id for update;
  if not found or o.committed_at_utc is null then raise exception 'IMPORT_REVIEW_OPERATION_NOT_COMMITTED' using errcode='55000'; end if;
  if o.request_hash is distinct from lower(btrim(p_request_hash)) then
    raise exception 'IMPORT_REVIEW_FOLLOW_UP_REQUEST_HASH_MISMATCH' using errcode='40001';
  end if;
  v_status_key:=case when v_component='EMAIL' then 'review_email_follow_up_status' else 'review_tsfin_follow_up_status' end;
  v_error_code_key:=case when v_component='EMAIL' then 'review_email_follow_up_error_code' else 'review_tsfin_follow_up_error_code' end;
  v_error_message_key:=case when v_component='EMAIL' then 'review_email_follow_up_error_message' else 'review_tsfin_follow_up_error_message' end;
  v_current:=upper(coalesce(o.response_json->>v_status_key,'NOT_REQUIRED'));
  if v_current not in ('PENDING','COMPLETE','FAILED_RETRYABLE','NOT_REQUIRED') then
    raise exception 'IMPORT_REVIEW_FOLLOW_UP_COMPONENT_STATE_INVALID' using errcode='23514';
  end if;
  if v_current=v_new then
    v_replay:=true;
  elsif v_current in ('COMPLETE','NOT_REQUIRED') and v_expected='PENDING' and v_new in ('COMPLETE','FAILED_RETRYABLE') then
    v_replay:=true;
  else
    if v_current<>v_expected then raise exception 'IMPORT_REVIEW_FOLLOW_UP_COMPONENT_CONFLICT' using errcode='40001'; end if;
    if not ((v_current='PENDING' and v_new in ('COMPLETE','FAILED_RETRYABLE'))
      or (v_current='FAILED_RETRYABLE' and v_new='PENDING')) then
      raise exception 'IMPORT_REVIEW_FOLLOW_UP_COMPONENT_TRANSITION_INVALID' using errcode='22023';
    end if;
    update public.import_apply_operations set response_json=response_json||jsonb_build_object(
      v_status_key,v_new,
      v_error_code_key,case when v_new='FAILED_RETRYABLE' then btrim(p_error_code) end,
      v_error_message_key,case when v_new='FAILED_RETRYABLE' then btrim(p_error_message) end),
      updated_at_utc=now() where id=p_operation_id;
    if v_current='FAILED_RETRYABLE' and v_new='PENDING' then
      update public.import_review_states set follow_up_retry_count=follow_up_retry_count+1,
        updated_at_utc=now(),updated_by_user_id=p_actor_user_id where import_id=p_import_id;
    end if;
  end if;
  v_result:=public._import_review_follow_up_reconcile_core_v1(p_import_id,p_operation_id,p_actor_user_id);
  select * into v from public.import_review_states where import_id=p_import_id;
  if not v_replay then
    insert into public.import_review_events(import_id,state_version,operation_id,event_code,actor_user_id,event_context_json)
    values(p_import_id,v.state_version,p_operation_id,v_component||'_FOLLOW_UP_'||v_new,p_actor_user_id,
      jsonb_strip_nulls(jsonb_build_object('previous_component_status',v_current,'error_code',p_error_code)));
  end if;
  return v_result||jsonb_build_object('component',v_component,'component_status',case when v_replay then v_current else v_new end,
    'replay',v_replay,'retry_count',v.follow_up_retry_count);
end $function$;

-- import_review_get_v1(uuid,uuid,text,integer,bigint,integer)
CREATE OR REPLACE FUNCTION public.import_review_get_v1(p_import_id uuid, p_actor_user_id uuid DEFAULT NULL::uuid, p_action_cursor text DEFAULT NULL::text, p_action_limit integer DEFAULT 100, p_event_cursor bigint DEFAULT NULL::bigint, p_event_limit integer DEFAULT 50)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare v_action_limit integer:=least(greatest(coalesce(p_action_limit,100),1),200); v_event_limit integer:=least(greatest(coalesce(p_event_limit,50),1),100);
  v_state public.import_review_states%rowtype; v_import public.hr_imports%rowtype; v_actions jsonb; v_events jsonb; v_last_action text; v_last_event bigint;
  v_apply_envelope jsonb; v_apply_request_hash text; v_allowed_commands jsonb; v_confirmation jsonb; v_read_only boolean;
  v_can_apply boolean; v_batch_ids text[]; v_applied_outcome_count integer; v_deferred_count integer;
begin
  perform public._import_review_assert_actor_v1(p_actor_user_id);
  if (p_action_cursor is not null and (length(p_action_cursor)<>64 or p_action_cursor!~'^[0-9a-f]{64}$'))
    or (p_event_cursor is not null and p_event_cursor<0) then raise exception 'IMPORT_REVIEW_CURSOR_INVALID' using errcode='22023'; end if;
  update public.import_review_states set last_opened_at_utc=now(),last_opened_by_user_id=p_actor_user_id
  where import_id=p_import_id returning * into v_state;
  if not found then raise exception 'IMPORT_REVIEW_NOT_FOUND' using errcode='P0002'; end if;
  select * into v_import from public.hr_imports where id=p_import_id;
  v_apply_envelope:=public._import_review_apply_envelope_core_v1(p_import_id);
  v_apply_request_hash:=public._import_review_hash_v1(v_apply_envelope::text);
  select coalesce(array_agg(value order by value),array[]::text[]) into v_batch_ids
  from jsonb_array_elements_text(coalesce(v_apply_envelope->'selected_action_ids','[]'::jsonb)) value;
  select count(*) into v_applied_outcome_count from public.import_review_action_outcomes o where o.import_id=p_import_id;
  select count(*) into v_deferred_count from public.import_review_decisions d
  where d.import_id=p_import_id and d.is_current and d.selectable and not d.selected;
  v_read_only:=v_state.status in ('APPLYING','APPLIED','ABANDONED','SUPERSEDED');
  v_can_apply:=v_state.status in ('BLOCKED','READY') and cardinality(v_batch_ids)>0
    and v_state.follow_up_status in ('NOT_REQUIRED','COMPLETE');
  v_allowed_commands:=to_jsonb(array_remove(array[
    case when v_state.status in ('STAGED','IN_REVIEW','BLOCKED','READY') then 'SAVE_SELECTIONS'::text end,
    case when v_state.status in ('STAGED','IN_REVIEW','BLOCKED','READY') then 'REFRESH'::text end,
    case when v_state.status in ('STAGED','IN_REVIEW','BLOCKED','READY') then 'ABANDON'::text end,
    case when v_state.status in ('STAGED','IN_REVIEW','BLOCKED','READY') then 'RESOLVE_DAILY_TIMESHEET'::text end,
    case when v_can_apply then 'APPLY'::text end,
    case when v_state.status in ('APPLYING','APPLIED') or v_state.last_operation_id is not null then 'VIEW_APPLY_STATUS'::text end,
    case when v_state.status in ('BLOCKED','READY','APPLIED') and v_state.follow_up_status='FAILED_RETRYABLE' then 'RETRY_FOLLOW_UP'::text end
  ],null));
  select jsonb_build_object(
    'selected_total',count(*) filter(where d.action_id=any(v_batch_ids)),
    'selected_change_count',count(*) filter(where d.action_id=any(v_batch_ids) and d.action_kind in ('INCLUDE_SHIFT','APPLY_AMENDMENT','APPLY_CANCELLATION','MARK_VALIDATION_ERROR')),
    'selected_email_count',count(*) filter(where d.action_id=any(v_batch_ids) and d.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER')),
    'selected_email_issue_count',count(*) filter(where d.action_id=any(v_batch_ids) and d.action_kind='EMAIL_ISSUE'),
    'selected_email_reminder_count',count(*) filter(where d.action_id=any(v_batch_ids) and d.action_kind='EMAIL_REMINDER'),
    'selected_reference_invalidation_count',count(*) filter(where d.action_id=any(v_batch_ids) and d.action_kind='INVALIDATE_REFERENCE'),
    'blocking_count',count(*) filter(where d.blocking),
    'batch_blocking_count',0,
    'deferred_count',v_deferred_count,
    'applied_outcome_count',v_applied_outcome_count,
    'advisory_count',count(*) filter(where d.action_kind='ADVISORY'),
    'daily_resolution_count',count(*) filter(where d.action_kind='DAILY_TIMESHEET_RESOLUTION'),
    'reconfirmation_count',count(*) filter(where d.requires_reconfirmation),
    'action_kind_counts',coalesce((select jsonb_object_agg(k.action_kind,k.item_count) from (
      select d2.action_kind,count(*)::integer item_count from public.import_review_decisions d2
      where d2.import_id=p_import_id and d2.is_current and d2.action_id=any(v_batch_ids) group by d2.action_kind order by d2.action_kind
    ) k),'{}'::jsonb)
  ) into v_confirmation
  from public.import_review_decisions d where d.import_id=p_import_id and d.is_current;
  with p as (select * from public.import_review_decisions d where d.import_id=p_import_id and d.is_current and (p_action_cursor is null or d.action_id>p_action_cursor) order by d.action_id limit v_action_limit+1),
  l as (select * from p order by action_id limit v_action_limit)
  select coalesce(jsonb_agg(jsonb_build_object('action_id',action_id,'kind',action_kind,'category',action_category,'target_key',target_key,
    'evidence_fingerprint',evidence_fingerprint,'selectable',selectable,'default_selected',default_selected,'selected',selected,'blocking',blocking,
    'requires_reconfirmation',requires_reconfirmation,'summary',summary_json) order by action_id),'[]'),max(action_id) into v_actions,v_last_action from l;
  with p as (select * from public.import_review_events e where e.import_id=p_import_id and (p_event_cursor is null or e.id>p_event_cursor) order by e.id limit v_event_limit+1),
  l as (select * from p order by id limit v_event_limit)
  select coalesce(jsonb_agg(jsonb_build_object('id',id,'state_version',state_version,'operation_id',operation_id,'event_code',event_code,
    'actor_user_id',actor_user_id,'context',event_context_json,'created_at_utc',created_at_utc) order by id),'[]'),max(id) into v_events,v_last_event from l;
  return jsonb_build_object('ok',true,'import',jsonb_build_object('id',v_import.id,'filename',v_import.filename,'source_system',v_import.source_system,
    'source_route',v_import.import_scope,'source_file_sha256',v_import.source_file_sha256,'parser_version',v_import.parser_version,
    'coverage_mode',v_import.coverage_mode,'coverage_start_date',v_import.coverage_start_date,'coverage_end_date',v_import.coverage_end_date,
    'coverage_fingerprint',v_import.coverage_fingerprint,'revision_group_id',v_import.revision_group_id,'revision_no',v_import.revision_no,'supersedes_import_id',v_import.supersedes_import_id),
    'state',jsonb_build_object('schema_contract_version',v_state.schema_contract_version,
      'status',v_state.status,'follow_up_status',v_state.follow_up_status,'state_version',v_state.state_version,
      'follow_up_error_code',v_state.follow_up_error_code,'follow_up_error_message',v_state.follow_up_error_message,
      'follow_up_retry_count',v_state.follow_up_retry_count,
      'preview_generation',v_state.preview_generation,'preview_fingerprint',v_state.preview_fingerprint,'ui_state',v_state.ui_state_json,
      'last_opened_at_utc',v_state.last_opened_at_utc,'last_opened_by_user_id',v_state.last_opened_by_user_id,
      'last_operation_id',v_state.last_operation_id,
      'last_operation_request_hash',(select o.request_hash from public.import_apply_operations o
        where o.id=v_state.last_operation_id and o.import_id=p_import_id),
      'read_only',v_read_only,
      'partial_application',v_applied_outcome_count>0 and v_state.status<>'APPLIED',
      'applied_outcome_count',v_applied_outcome_count,'deferred_count',v_deferred_count,
      'editability',jsonb_build_object(
        'read_only',v_read_only,
        'can_edit_selections',v_state.status in ('STAGED','IN_REVIEW','BLOCKED','READY'),
        'can_resolve_daily_timesheet',v_state.status in ('STAGED','IN_REVIEW','BLOCKED','READY'),
        'can_refresh',v_state.status in ('STAGED','IN_REVIEW','BLOCKED','READY'),
        'can_abandon',v_state.status in ('STAGED','IN_REVIEW','BLOCKED','READY'),
        'can_apply',v_can_apply,
        'can_view_apply_status',v_state.status in ('APPLYING','APPLIED') or v_state.last_operation_id is not null,
        'can_retry_follow_up',v_state.status in ('BLOCKED','READY','APPLIED') and v_state.follow_up_status='FAILED_RETRYABLE',
        'allowed_commands',v_allowed_commands),
      'apply_contract',jsonb_build_object('selected_action_ids',v_apply_envelope->'selected_action_ids',
        'reference_invalidation_action_ids',v_apply_envelope->'reference_invalidation_action_ids',
        'request_envelope',v_apply_envelope,'request_hash',v_apply_request_hash)),
    'evidence',jsonb_build_object(
      'source_file_sha256',v_import.source_file_sha256,'parser_version',v_import.parser_version,
      'coverage_fingerprint',v_import.coverage_fingerprint,'preview_fingerprint',v_state.preview_fingerprint,
      'preview_generation',v_state.preview_generation),
    'confirmation_summary',v_confirmation,
    'actions',v_actions,'actions_next_cursor',case when jsonb_array_length(v_actions)=v_action_limit then v_last_action end,
    'events',v_events,'events_next_cursor',case when jsonb_array_length(v_events)=v_event_limit then v_last_event end);
end $function$;

-- import_review_list_v1(text,text,uuid,date,date,timestamp with time zone,uuid,integer)
CREATE OR REPLACE FUNCTION public.import_review_list_v1(p_status_class text DEFAULT 'ACTIVE'::text, p_source_route text DEFAULT NULL::text, p_client_id uuid DEFAULT NULL::uuid, p_date_from date DEFAULT NULL::date, p_date_to date DEFAULT NULL::date, p_cursor_updated_at timestamp with time zone DEFAULT NULL::timestamp with time zone, p_cursor_import_id uuid DEFAULT NULL::uuid, p_page_size integer DEFAULT 50)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare v_limit integer:=least(greatest(coalesce(p_page_size,50),1),100); v_items jsonb;
  v_last_updated_at timestamptz; v_last_import_id uuid; v_has_more boolean:=false;
begin
  if p_date_from is not null and p_date_to is not null and p_date_from>p_date_to then raise exception 'IMPORT_REVIEW_DATE_FILTER_INVALID' using errcode='22023'; end if;
  if upper(coalesce(p_status_class,'ACTIVE')) not in ('ACTIVE','COMPLETED','ABANDONED','SUPERSEDED','ALL') then raise exception 'IMPORT_REVIEW_STATUS_FILTER_INVALID' using errcode='22023'; end if;
  if length(coalesce(p_source_route,''))>128 then raise exception 'IMPORT_REVIEW_ROUTE_FILTER_INVALID' using errcode='22023'; end if;
  with page as (
    select s.*,hi.filename,hi.source_system,hi.import_scope,hi.coverage_mode,hi.coverage_start_date,hi.coverage_end_date,
      hi.coverage_fingerprint,hi.source_file_sha256,
      (select count(*) from public.import_review_decisions d where d.import_id=s.import_id and d.is_current and d.blocking) blocker_count,
      (select count(*) from public.import_review_decisions d where d.import_id=s.import_id and d.is_current and d.selected) selected_count,
      (select count(*) from public.import_review_decisions d where d.import_id=s.import_id and d.is_current and d.selected and d.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER')) selected_email_count,
      (select count(*) from public.import_review_decisions d where d.import_id=s.import_id and d.is_current
        and (d.blocking or d.action_category in ('EMAIL','PENDING','BLOCKED'))) unresolved_current_count,
      (select count(*) from public.import_review_action_outcomes o where o.import_id=s.import_id
        and o.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER')) applied_email_count,
      (select count(*) from public.import_review_action_outcomes o where o.import_id=s.import_id
        and o.action_kind in ('INCLUDE_SHIFT','APPLY_AMENDMENT','APPLY_CANCELLATION')) applied_change_count,
      (select count(distinct o.timesheet_id) from public.import_review_action_outcomes o
        where o.import_id=s.import_id and o.timesheet_id is not null
          and (o.summary_json->>'authority_mode'='VALIDATION_ONLY'
            or coalesce(nullif(o.summary_json->>'is_daily','')::boolean,false))) validation_target_count,
      (select count(distinct o.timesheet_id) from public.import_review_action_outcomes o
        left join public.timesheet_validations tv on tv.timesheet_id=o.timesheet_id
        where o.import_id=s.import_id and o.timesheet_id is not null
          and (o.summary_json->>'authority_mode'='VALIDATION_ONLY'
            or coalesce(nullif(o.summary_json->>'is_daily','')::boolean,false))
          and (tv.timesheet_id is null or tv.status::text<>'VALIDATION_OK' or tv.last_source is distinct from s.import_id)) validation_incomplete_count
    from public.import_review_states s join public.hr_imports hi on hi.id=s.import_id
    where (case upper(coalesce(p_status_class,'ACTIVE')) when 'ACTIVE' then s.status in ('STAGED','IN_REVIEW','BLOCKED','READY','APPLYING')
      when 'COMPLETED' then s.status='APPLIED' when 'ABANDONED' then s.status='ABANDONED' when 'SUPERSEDED' then s.status='SUPERSEDED' else true end)
      and (p_source_route is null or upper(coalesce(hi.import_scope,hi.source_system::text))=upper(p_source_route))
      and (p_client_id is null or hi.client_id=p_client_id or exists(select 1 from public.import_review_scope_clients sc where sc.import_id=hi.id and sc.client_id=p_client_id))
      and (p_date_from is null or hi.coverage_end_date>=p_date_from) and (p_date_to is null or hi.coverage_start_date<=p_date_to)
      and (p_cursor_updated_at is null or (s.updated_at_utc,s.import_id)<(p_cursor_updated_at,coalesce(p_cursor_import_id,'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid)))
    order by s.updated_at_utc desc,s.import_id desc limit v_limit+1
  ), limited as (select * from page order by updated_at_utc desc,import_id desc limit v_limit)
  select coalesce(jsonb_agg(jsonb_build_object('schema_contract_version',schema_contract_version,
    'import_id',import_id,'filename',filename,'source_system',source_system,'source_route',import_scope,
    'source_hash_prefix',left(source_file_sha256,12),'coverage_mode',coverage_mode,'coverage_start_date',coverage_start_date,'coverage_end_date',coverage_end_date,
    'status',status,'display_status',case
      when status='APPLIED'
        and follow_up_status in ('COMPLETE','NOT_REQUIRED')
        and blocker_count=0 and unresolved_current_count=0 and applied_email_count=0
        and validation_incomplete_count=0
        and (
          validation_target_count>0
          or applied_change_count>0
        )
      then 'SUCCESS' else status end,
    'follow_up_status',follow_up_status,'state_version',state_version,'preview_generation',preview_generation,
    'blocker_count',blocker_count,'selected_count',selected_count,'selected_email_count',selected_email_count,
    'unresolved_current_count',unresolved_current_count,'applied_email_count',applied_email_count,
    'applied_change_count',applied_change_count,'validation_target_count',validation_target_count,
    'validation_incomplete_count',validation_incomplete_count,
    'read_only',status in ('APPLYING','APPLIED','ABANDONED','SUPERSEDED'),'updated_at_utc',updated_at_utc) order by updated_at_utc desc,import_id desc),'[]'),
    (select count(*)>v_limit from page) into v_items,v_has_more from limited;
  if v_has_more and jsonb_array_length(v_items)>0 then
    select updated_at_utc,import_id into v_last_updated_at,v_last_import_id from public.import_review_states
    where import_id=(v_items->(jsonb_array_length(v_items)-1)->>'import_id')::uuid;
  end if;
  return jsonb_build_object('ok',true,'items',v_items,'page_size',v_limit,'next_cursor',case when v_has_more then jsonb_build_object('updated_at_utc',v_last_updated_at,'import_id',v_last_import_id) end);
end $function$;

-- import_review_prune_guard_v1()
CREATE OR REPLACE FUNCTION public.import_review_prune_guard_v1()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare v_status text;
begin
  if old.pruned_at is null and new.pruned_at is not null then
    select s.status into v_status from public.import_review_states s where s.import_id=new.id;
    if v_status in ('STAGED','IN_REVIEW','BLOCKED','READY','APPLYING') then
      raise exception 'IMPORT_REVIEW_ACTIVE_PRUNE_BLOCKED' using errcode='55000',
        detail=jsonb_build_object('import_id',new.id,'status',v_status)::text;
    end if;
  end if;
  return new;
end
$function$;

-- import_review_refresh_v1(uuid,bigint,uuid,integer)
CREATE OR REPLACE FUNCTION public.import_review_refresh_v1(p_import_id uuid, p_expected_state_version bigint, p_actor_user_id uuid DEFAULT NULL::uuid, p_max_actions integer DEFAULT 5000)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
 SET "plpgsql_check.mode" TO 'disabled'
AS $function$
begin return public._import_review_refresh_core_v1(p_import_id,p_expected_state_version,p_actor_user_id,least(coalesce(p_max_actions,5000),5000)); end $function$;

-- import_review_replace_v1(uuid,text,date,date,jsonb,jsonb,text,text,uuid,text,uuid,bigint)
CREATE OR REPLACE FUNCTION public.import_review_replace_v1(p_import_id uuid, p_coverage_mode text, p_coverage_start_date date, p_coverage_end_date date, p_scope_clients jsonb, p_scope_candidates jsonb, p_expected_source_file_sha256 text, p_expected_parser_version text, p_actor_user_id uuid, p_operation_key text, p_supersede_import_id uuid, p_expected_supersede_state_version bigint)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
begin
  return public._import_review_create_core_v2(p_import_id,p_coverage_mode,p_coverage_start_date,p_coverage_end_date,
    p_scope_clients,p_scope_candidates,p_expected_source_file_sha256,p_expected_parser_version,
    p_actor_user_id,p_operation_key,p_supersede_import_id,p_expected_supersede_state_version);
end $function$;

-- import_review_save_v1(uuid,bigint,integer,text,jsonb,jsonb,uuid,uuid)
CREATE OR REPLACE FUNCTION public.import_review_save_v1(p_import_id uuid, p_expected_state_version bigint, p_expected_preview_generation integer, p_expected_preview_fingerprint text, p_action_changes jsonb, p_ui_state_json jsonb DEFAULT NULL::jsonb, p_actor_user_id uuid DEFAULT NULL::uuid, p_request_id uuid DEFAULT NULL::uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare v_state public.import_review_states%rowtype; v_changes jsonb:=coalesce(p_action_changes,'[]'); v_ui jsonb; v_hash text; v_prior jsonb;
  v_changed integer; v_blockers integer; v_status text;
begin
  perform public._import_review_assert_actor_v1(p_actor_user_id);
  if p_request_id is null or jsonb_typeof(v_changes)<>'array' or jsonb_array_length(v_changes)>500 or pg_column_size(v_changes)>131072 then
    raise exception 'IMPORT_REVIEW_SAVE_INPUT_INVALID' using errcode='22023'; end if;
  if exists(select 1 from jsonb_array_elements(v_changes)x where jsonb_typeof(x)<>'object' or nullif(x->>'action_id','') is null
    or jsonb_typeof(x->'selected')<>'boolean' or (x-'action_id'-'selected')<>'{}'::jsonb)
    or (select count(*) from jsonb_array_elements(v_changes))<>(select count(distinct x->>'action_id') from jsonb_array_elements(v_changes)x) then
    raise exception 'IMPORT_REVIEW_ACTION_CHANGE_INVALID' using errcode='22023'; end if;
  v_ui:=case when p_ui_state_json is null then null else public._import_review_validate_ui_state_v1(p_ui_state_json) end;
  v_hash:=public._import_review_hash_v1(jsonb_build_object('changes',(select coalesce(jsonb_agg(x order by x->>'action_id'),'[]') from jsonb_array_elements(v_changes)x),'ui',v_ui)::text);
  select e.event_context_json into v_prior from public.import_review_events e where e.import_id=p_import_id and e.operation_id=p_request_id and e.event_code='SELECTION_SAVED' order by e.id desc limit 1;
  if found then if v_prior->>'request_hash'<>v_hash then raise exception 'IMPORT_REVIEW_SAVE_REQUEST_CONFLICT' using errcode='23505'; end if;
    return jsonb_build_object('ok',true,'schema_contract_version','IMPORT_REVIEW_DB_V1',
      'replay',true,'import_id',p_import_id,'state_version',(v_prior->>'resulting_state_version')::bigint,'status',v_prior->>'status'); end if;
  select * into v_state from public.import_review_states where import_id=p_import_id for update;
  if not found then raise exception 'IMPORT_REVIEW_NOT_FOUND' using errcode='P0002'; end if;
  if v_state.status not in ('IN_REVIEW','BLOCKED','READY','STAGED') then raise exception 'IMPORT_REVIEW_SAVE_NOT_ALLOWED' using errcode='55000'; end if;
  if v_state.state_version<>p_expected_state_version then raise exception 'IMPORT_REVIEW_VERSION_CONFLICT' using errcode='40001',detail=v_state.state_version::text; end if;
  if v_state.preview_generation<>p_expected_preview_generation or v_state.preview_fingerprint is distinct from p_expected_preview_fingerprint then
    raise exception 'IMPORT_REVIEW_PREVIEW_STALE' using errcode='40001'; end if;
  if exists(select 1 from jsonb_array_elements(v_changes)x left join public.import_review_decisions d on d.action_id=x->>'action_id' and d.import_id=p_import_id
    where d.action_id is null or not d.is_current or not d.selectable or d.preview_generation<>v_state.preview_generation) then
    raise exception 'IMPORT_REVIEW_ACTION_NOT_SELECTABLE_OR_STALE' using errcode='40001'; end if;
  update public.import_review_decisions d set selected=(x.value->>'selected')::boolean,requires_reconfirmation=false,
    selected_at_utc=now(),selected_by_user_id=p_actor_user_id
  from jsonb_array_elements(v_changes)x(value) where d.import_id=p_import_id and d.action_id=x.value->>'action_id'
    and (d.selected is distinct from (x.value->>'selected')::boolean or d.requires_reconfirmation);
  get diagnostics v_changed=row_count;
  select count(*) into v_blockers from public.import_review_decisions where import_id=p_import_id and is_current and blocking;
  v_status:=case when v_blockers>0 then 'BLOCKED' else 'READY' end;
  update public.import_review_states set status=v_status,state_version=state_version+1,ui_state_json=coalesce(v_ui,ui_state_json),
    updated_at_utc=now(),updated_by_user_id=p_actor_user_id where import_id=p_import_id returning * into v_state;
  insert into public.import_review_events(import_id,state_version,operation_id,event_code,actor_user_id,event_context_json)
  values(p_import_id,v_state.state_version,p_request_id,'SELECTION_SAVED',p_actor_user_id,jsonb_build_object('request_hash',v_hash,
    'changed_count',v_changed,'resulting_state_version',v_state.state_version,'status',v_status));
  return jsonb_build_object('ok',true,'schema_contract_version',v_state.schema_contract_version,
    'replay',false,'import_id',p_import_id,'state_version',v_state.state_version,'status',v_status,
    'preview_generation',v_state.preview_generation,'preview_fingerprint',v_state.preview_fingerprint,'changed_count',v_changed,'blocker_count',v_blockers);
end $function$;

-- import_review_staged_scope_get_v1(uuid,uuid,integer,integer)
CREATE OR REPLACE FUNCTION public.import_review_staged_scope_get_v1(p_import_id uuid, p_actor_user_id uuid DEFAULT NULL::uuid, p_candidate_page integer DEFAULT 1, p_candidate_page_size integer DEFAULT 100)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_import public.hr_imports%rowtype;
  v_page integer:=coalesce(p_candidate_page,1);
  v_size integer:=coalesce(p_candidate_page_size,100);
  v_row_count integer;
  v_from date;
  v_to date;
  v_candidates jsonb;
  v_clients jsonb;
  v_candidate_total integer;
  v_overlap jsonb;
  v_authority_mode text:='UNRESOLVED';
  v_authority_summary jsonb:='{}'::jsonb;
  v_authoritative_contract_count integer:=0;
  v_validation_contract_count integer:=0;
  v_route_ineligible_count integer:=0;
  v_unresolved_row_count integer:=0;
begin
  perform public._import_review_assert_actor_v1(p_actor_user_id);
  if p_import_id is null or v_page<1 or v_page>20 or v_size not in (25,50,75,100,500) then
    raise exception 'IMPORT_REVIEW_STAGED_SCOPE_INPUT_INVALID' using errcode='22023';
  end if;
  select * into v_import from public.hr_imports where id=p_import_id;
  if not found then raise exception 'IMPORT_REVIEW_IMPORT_NOT_FOUND' using errcode='P0002'; end if;
  if v_import.pruned_at is not null then raise exception 'IMPORT_REVIEW_IMPORT_PRUNED' using errcode='55000'; end if;
  if nullif(btrim(coalesce(v_import.source_file_sha256,'')),'') is null
     or nullif(btrim(coalesce(v_import.parser_version,'')),'') is null then
    raise exception 'IMPORT_REVIEW_STAGING_EVIDENCE_REQUIRED' using errcode='55000';
  end if;

  select count(*),min(r.date_local),max(r.date_local)
    into v_row_count,v_from,v_to
  from public.hr_rows r where r.import_id=p_import_id;
  if v_row_count=0 or v_from is null or v_to is null then
    raise exception 'IMPORT_REVIEW_STAGED_ROWS_REQUIRED' using errcode='55000';
  end if;
  if v_row_count>5000 then
    raise exception 'IMPORT_REVIEW_STAGED_SCOPE_ROW_LIMIT_EXCEEDED' using errcode='54000';
  end if;

  with raw as (
    select r.id,
      coalesce(nullif(r.staff_raw,''),nullif(r.payload_json->>'staff_name',''),nullif(r.staff_norm,''),'Unlabelled candidate') staff_label,
      coalesce(
        nullif(regexp_replace(lower(coalesce(nullif(r.staff_raw,''),r.payload_json->>'staff_name',r.staff_norm,'')),'[^a-z0-9]+','','g'),''),
        'unlabelled:'||r.id::text
      ) staff_key
    from public.hr_rows r where r.import_id=p_import_id
  ), distinct_source as (
    select staff_key,min(staff_label) staff_label
    from raw group by staff_key
  ), mapped as (
    select d.*,
      coalesce(alias_match.id,name_map.candidate_id,exact_match.candidate_id) candidate_id
    from distinct_source d
    left join lateral (
      select c.id from public.candidates c
      where c.active and c.nhsp_hr_name_aliases is not null
        and c.nhsp_hr_name_aliases @> to_jsonb(array[d.staff_key]::text[])
      order by c.id limit 1
    ) alias_match on true
    left join lateral (
      select hm.candidate_id from public.hr_name_mappings hm
      where hm.active and hm.hr_name_norm in (lower(btrim(d.staff_label)),d.staff_key)
      order by hm.created_at desc,hm.id limit 1
    ) name_map on alias_match.id is null
    left join lateral (
      select case when count(*)=1 then (array_agg(c.id order by c.id))[1] end candidate_id
      from public.candidates c where c.active and (
        regexp_replace(lower(coalesce(c.first_name,'')||coalesce(c.last_name,'')),'[^a-z0-9]+','','g')=d.staff_key
        or regexp_replace(lower(coalesce(c.last_name,'')||coalesce(c.first_name,'')),'[^a-z0-9]+','','g')=d.staff_key)
    ) exact_match on alias_match.id is null and name_map.candidate_id is null
  ), numbered as (
    select m.*,c.first_name,c.last_name,
      row_number() over(order by lower(coalesce(nullif(c.last_name,''),
        case when position(',' in m.staff_label)>0 then split_part(m.staff_label,',',1)
             else regexp_replace(btrim(m.staff_label),'^.*\s+','','') end,'')),
        lower(coalesce(c.first_name,m.staff_label)),m.staff_key) rn,
      count(*) over() total_count
    from mapped m left join public.candidates c on c.id=m.candidate_id
  )
  select coalesce(jsonb_agg(jsonb_build_object(
      'source_candidate_key',staff_key,
      'source_display_label',staff_label,
      'candidate_id',candidate_id,
      'resolved_display_name',nullif(btrim(concat_ws(' ',first_name,last_name)),''),
      'resolved',candidate_id is not null
    ) order by rn),'[]'::jsonb),coalesce(max(total_count),0)
    into v_candidates,v_candidate_total
  from numbered
  where rn>((v_page-1)*v_size) and rn<=v_page*v_size;

  with raw as (
    select r.id,
      coalesce(nullif(r.payload_json->>'trust',''),nullif(r.payload_json->>'hospital_or_trust',''),
        nullif(r.unit_raw,''),nullif(r.unit_hint,''),nullif(c.name,''),'Unlabelled client') client_label,
      coalesce(
        case when v_import.client_id is not null then 'client:'||v_import.client_id::text end,
        nullif(regexp_replace(lower(coalesce(nullif(r.payload_json->>'trust',''),
          nullif(r.payload_json->>'hospital_or_trust',''),r.unit_raw,r.unit_hint,'')),'[^a-z0-9]+','','g'),''),
        'unlabelled:'||r.id::text
      ) client_key,
      v_import.client_id import_client_id
    from public.hr_rows r left join public.clients c on c.id=v_import.client_id
    where r.import_id=p_import_id
  ), distinct_source as (
    select client_key,min(client_label) client_label,
      (array_agg(import_client_id order by id) filter(where import_client_id is not null))[1] import_client_id
    from raw group by client_key
  ), mapped as (
    select d.*,
      coalesce(d.import_client_id,hospital_match.client_id,exact_match.client_id) client_id
    from distinct_source d
    left join lateral (
      select ch.client_id from public.client_hospitals ch
      where d.import_client_id is null and ch.hospital_name_norm @> to_jsonb(array[d.client_key]::text[])
      order by ch.id limit 1
    ) hospital_match on true
    left join lateral (
      select case when count(*)=1 then (array_agg(c.id order by c.id))[1] end client_id
      from public.clients c where d.import_client_id is null
        and regexp_replace(lower(coalesce(c.name,'')),'[^a-z0-9]+','','g')=d.client_key
    ) exact_match on hospital_match.client_id is null
  )
  select coalesce(jsonb_agg(jsonb_build_object(
      'source_client_key',m.client_key,
      'source_display_label',m.client_label,
      'client_id',m.client_id,
      'resolved_display_name',c.name,
      'resolved',m.client_id is not null
    ) order by lower(coalesce(c.name,m.client_label)),m.client_key),'[]'::jsonb)
    into v_clients
  from mapped m left join public.clients c on c.id=m.client_id;

  if jsonb_array_length(v_clients)>100 then
    raise exception 'IMPORT_REVIEW_STAGED_CLIENT_LIMIT_EXCEEDED' using errcode='54000';
  end if;

  -- Coverage wording is server-owned and uses the same current-setting
  -- authority core as catalogue generation and final application.
  if upper(v_import.source_system::text)='HEALTHROSTER_DAILY'
     or upper(coalesce(v_import.import_scope,'')) like '%DAILY%' then
    select case when a.route_eligible then 'VALIDATION_ONLY' else 'UNRESOLVED' end
      into v_authority_mode
    from public._import_review_effective_authority_core_v1(
      'HR_DAILY',null,v_import.client_id,v_from) a;
    v_authority_summary:=jsonb_build_object(
      'mode',v_authority_mode,
      'source_route','HR_DAILY',
      'authoritative_contract_count',0,
      'validation_contract_count',case when v_authority_mode='VALIDATION_ONLY' then 1 else 0 end,
      'route_ineligible_count',case when v_authority_mode='UNRESOLVED' then 1 else 0 end,
      'unresolved_row_count',0,
      'settings_as_of_date',(statement_timestamp() at time zone 'Europe/London')::date,
      'basis','CURRENT_SETTINGS_DAILY_EXISTING_TIMESHEET_VALIDATION'
    );
  elsif upper(v_import.source_system::text)='HEALTHROSTER'
        and upper(coalesce(v_import.import_scope,'HR_WEEKLY')) not like '%DAILY%' then
    with phase as materialized (
      select * from public.weekly_import_phase2(p_import_id,'HR_WEEKLY')
    ), applicable as (
      select distinct w.contract_id,w.week_ending_date,a.authority_mode,a.authority_fingerprint
      from phase w
      join public.contracts c on c.id=w.contract_id
      cross join lateral public._import_review_effective_authority_core_v1(
        'HR_WEEKLY',c.id,c.client_id,w.week_ending_date) a
      where w.contract_id is not null and upper(coalesce(w.action::text,''))='OK'
    )
    select count(*) filter(where authority_mode='AUTHORITATIVE'),
      count(*) filter(where authority_mode='VALIDATION_ONLY'),
      count(*) filter(where authority_mode='OUT_OF_SCOPE'),
      (select count(*) from phase where contract_id is null or upper(coalesce(action::text,''))<>'OK')
    into v_authoritative_contract_count,v_validation_contract_count,v_route_ineligible_count,v_unresolved_row_count
    from applicable;

    if v_route_ineligible_count>0 or v_unresolved_row_count>0 then
      v_authority_mode:='UNRESOLVED';
    elsif v_authoritative_contract_count>0 and v_validation_contract_count>0 then
      v_authority_mode:='MIXED';
    elsif v_authoritative_contract_count>0 then
      v_authority_mode:='AUTHORITATIVE';
    elsif v_validation_contract_count>0 then
      v_authority_mode:='VALIDATION_ONLY';
    else
      v_authority_mode:='UNRESOLVED';
    end if;

    v_authority_summary:=jsonb_build_object(
      'mode',v_authority_mode,
      'source_route','HR_WEEKLY',
      'authoritative_contract_count',v_authoritative_contract_count,
      'validation_contract_count',v_validation_contract_count,
      'route_ineligible_count',v_route_ineligible_count,
      'unresolved_row_count',v_unresolved_row_count,
      'settings_as_of_date',(statement_timestamp() at time zone 'Europe/London')::date,
      'basis','CURRENT_CLIENT_AND_CONTRACT_SETTINGS'
    );
  elsif upper(v_import.source_system::text)='NHSP' then
    with phase as materialized (
      select * from public.weekly_import_phase2(p_import_id,'NHSP')
    ), applicable as (
      select distinct w.contract_id,w.week_ending_date,a.authority_mode
      from phase w join public.contracts c on c.id=w.contract_id
      cross join lateral public._import_review_effective_authority_core_v1(
        'NHSP',c.id,c.client_id,w.week_ending_date) a
      where w.contract_id is not null and upper(coalesce(w.action::text,''))='OK'
    )
    select count(*) filter(where authority_mode='AUTHORITATIVE'),
      count(*) filter(where authority_mode='OUT_OF_SCOPE'),
      (select count(*) from phase where contract_id is null or upper(coalesce(action::text,''))<>'OK')
    into v_authoritative_contract_count,v_route_ineligible_count,v_unresolved_row_count
    from applicable;
    v_authority_mode:=case when v_route_ineligible_count>0 or v_unresolved_row_count>0
      or v_authoritative_contract_count=0 then 'UNRESOLVED' else 'AUTHORITATIVE' end;
    v_authority_summary:=jsonb_build_object(
      'mode',v_authority_mode,
      'source_route','NHSP',
      'authoritative_contract_count',v_authoritative_contract_count,
      'validation_contract_count',0,
      'route_ineligible_count',v_route_ineligible_count,
      'unresolved_row_count',v_unresolved_row_count,
      'settings_as_of_date',(statement_timestamp() at time zone 'Europe/London')::date,
      'basis','CURRENT_NHSP_SETTINGS'
    );
  else
    v_authority_summary:=jsonb_build_object(
      'mode',v_authority_mode,
      'source_route',coalesce(v_import.import_scope,v_import.source_system::text),
      'authoritative_contract_count',0,
      'validation_contract_count',0,
      'basis','UNRESOLVED_SOURCE_ROUTE'
    );
  end if;

  v_overlap:=public._import_review_overlap_preflight_core_v2(
    p_import_id,v_import.source_system,coalesce(v_import.import_scope,v_import.source_system::text),
    v_from,v_to,v_clients);

  return jsonb_build_object(
    'ok',true,
    'import_id',v_import.id,
    'filename',v_import.filename,
    'source_system',v_import.source_system,
    'source_route',coalesce(v_import.import_scope,v_import.source_system::text),
    'source_file_sha256',v_import.source_file_sha256,
    'parser_version',v_import.parser_version,
    'parse_summary',coalesce(v_import.parse_summary_json,'{}'::jsonb),
    'coverage_start_date',v_from,
    'coverage_end_date',v_to,
    'staged_row_count',v_row_count,
    'scope_clients',v_clients,
    'candidate_options',v_candidates,
    'candidate_page',v_page,
    'candidate_page_size',v_size,
    'candidate_total',v_candidate_total,
    'candidate_total_pages',case when v_candidate_total=0 then 0 else ceiling(v_candidate_total::numeric/v_size)::integer end,
    'candidate_has_previous',v_page>1,
    'candidate_has_next',v_page*v_size<v_candidate_total,
    'authority_mode',v_authority_mode,
    'authority_summary',v_authority_summary,
    'review_already_created',v_import.coverage_locked_at is not null,
    'review_status',(select s.status from public.import_review_states s where s.import_id=p_import_id),
    'overlapping_unfinished_reviews',v_overlap
  );
end
$function$;

-- import_timesheet_financial_preflight_v1(uuid[],text,uuid,jsonb,boolean,integer)
CREATE OR REPLACE FUNCTION public.import_timesheet_financial_preflight_v1(p_timesheet_ids uuid[], p_action text, p_actor_user_id uuid, p_expected_state_json jsonb DEFAULT '{}'::jsonb, p_lock_rows boolean DEFAULT true, p_max_scope integer DEFAULT 100)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
DECLARE
  v_action text := UPPER(BTRIM(COALESCE(p_action, '')));
  v_expected_state jsonb := COALESCE(p_expected_state_json, '{}'::jsonb);

  v_input_ids uuid[] := ARRAY[]::uuid[];
  v_root_ids uuid[] := ARRAY[]::uuid[];
  v_member_ids uuid[] := ARRAY[]::uuid[];

  v_raw_input_count integer := 0;
  v_input_count integer := 0;
  v_root_count integer := 0;
  v_member_count integer := 0;

  v_input_id uuid;
  v_root_id uuid;
  v_member_id uuid;
  v_member_text text;

  v_chain jsonb;
  v_chains_json jsonb := '[]'::jsonb;
  v_initial_chain_fingerprints jsonb := '{}'::jsonb;
  v_initial_anchor_fingerprints jsonb := '{}'::jsonb;
  v_correction_financials_policy_envelopes jsonb := '{}'::jsonb;
  v_correction_financials_policy_envelope_fingerprints jsonb := '{}'::jsonb;
  v_chain_fingerprint text;
  v_anchor_fingerprint text;
  v_expected_fingerprint text;
  v_expected_anchor_fingerprint text;

  v_timesheet_signatures jsonb := '{}'::jsonb;
  v_member_summaries jsonb := '[]'::jsonb;
  v_errors_json jsonb := '[]'::jsonb;
  v_blocking_batches_json jsonb := '[]'::jsonb;

  v_member_row record;
  v_signature_payload jsonb;
  v_timesheet_signature text;
  v_expected_timesheet_signature text;

  v_blocking_batch_count integer := 0;
  v_authorised_count integer := 0;
  v_processed_count integer := 0;
  v_paid_count integer := 0;
  v_invoice_lined_count integer := 0;
  v_stale_tsfin_count integer := 0;

  v_lock_acquired boolean := false;
  v_all_locks_acquired boolean := false;
  v_allowed boolean := false;
  v_required_path text;
  v_preflight_fingerprint text;
BEGIN
  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'IMPORT_PREFLIGHT_ACTOR_REQUIRED'
      USING ERRCODE = '22023';
  END IF;

  PERFORM 1
  FROM public.tms_users AS actor_row
  WHERE actor_row.id = p_actor_user_id
    AND COALESCE(actor_row.is_active, false) = true;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'IMPORT_PREFLIGHT_ACTOR_INVALID'
      USING ERRCODE = '42501',
            DETAIL = jsonb_build_object(
              'code', 'IMPORT_PREFLIGHT_ACTOR_INVALID',
              'actor_user_id', p_actor_user_id::text
            )::text;
  END IF;

  IF v_action = '' OR char_length(v_action) > 64 THEN
    RAISE EXCEPTION 'IMPORT_PREFLIGHT_ACTION_INVALID'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object(
              'code', 'IMPORT_PREFLIGHT_ACTION_INVALID',
              'max_characters', 64
            )::text;
  END IF;

  IF p_max_scope < 1 OR p_max_scope > 100 THEN
    RAISE EXCEPTION 'IMPORT_PREFLIGHT_MAX_SCOPE_OUT_OF_RANGE'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object(
              'code', 'IMPORT_PREFLIGHT_MAX_SCOPE_OUT_OF_RANGE',
              'min', 1,
              'max', 100,
              'supplied', p_max_scope
            )::text;
  END IF;

  IF jsonb_typeof(v_expected_state) <> 'object' THEN
    RAISE EXCEPTION 'IMPORT_PREFLIGHT_EXPECTED_STATE_MUST_BE_OBJECT'
      USING ERRCODE = '22023';
  END IF;

  IF octet_length(v_expected_state::text) > 262144 THEN
    RAISE EXCEPTION 'IMPORT_PREFLIGHT_EXPECTED_STATE_TOO_LARGE'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object(
              'code', 'IMPORT_PREFLIGHT_EXPECTED_STATE_TOO_LARGE',
              'max_bytes', 262144
            )::text;
  END IF;

  -- Enforce the limit before unnest/dedup so duplicate-heavy input cannot
  -- bypass the public RPC cardinality bound.
  v_raw_input_count := COALESCE(cardinality(p_timesheet_ids), 0);
  IF v_raw_input_count < 1 OR v_raw_input_count > 100 THEN
    RAISE EXCEPTION 'IMPORT_PREFLIGHT_RAW_INPUT_COUNT_OUT_OF_RANGE'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object(
              'code', 'IMPORT_PREFLIGHT_RAW_INPUT_COUNT_OUT_OF_RANGE',
              'min', 1, 'max', 100, 'supplied_raw_count', v_raw_input_count
            )::text;
  END IF;

  SELECT COALESCE(
    array_agg(DISTINCT supplied_id ORDER BY supplied_id),
    ARRAY[]::uuid[]
  )
  INTO v_input_ids
  FROM unnest(COALESCE(p_timesheet_ids, ARRAY[]::uuid[]))
    AS supplied_timesheet(supplied_id)
  WHERE supplied_id IS NOT NULL;

  v_input_count := cardinality(v_input_ids);

  IF v_input_count < 1 OR v_input_count > 100 THEN
    RAISE EXCEPTION 'IMPORT_PREFLIGHT_INPUT_COUNT_OUT_OF_RANGE'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object(
              'code', 'IMPORT_PREFLIGHT_INPUT_COUNT_OUT_OF_RANGE',
              'min', 1,
              'max', 100,
              'supplied_distinct_count', v_input_count
            )::text;
  END IF;

  FOREACH v_input_id IN ARRAY v_input_ids LOOP
    v_chain := public.timesheet_correction_chain_scope_v1(
      v_input_id,
      false,
      32,
      100
    );

    IF COALESCE((v_chain ->> 'ok')::boolean, false) IS NOT TRUE THEN
      RAISE EXCEPTION 'IMPORT_PREFLIGHT_CHAIN_SCOPE_FAILED'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'IMPORT_PREFLIGHT_CHAIN_SCOPE_FAILED',
                'timesheet_id', v_input_id::text
              )::text;
    END IF;

    v_root_id := NULLIF(v_chain ->> 'root_timesheet_id', '')::uuid;

    IF v_root_id IS NULL THEN
      RAISE EXCEPTION 'IMPORT_PREFLIGHT_CHAIN_ROOT_MISSING'
        USING ERRCODE = 'P0001';
    END IF;

    v_root_ids := array_append(v_root_ids, v_root_id);

    FOR v_member_text IN
      SELECT member_value
      FROM jsonb_array_elements_text(
        COALESCE(v_chain -> 'member_timesheet_ids', '[]'::jsonb)
      ) AS member_element(member_value)
    LOOP
      v_member_ids := array_append(v_member_ids, v_member_text::uuid);
    END LOOP;

    v_initial_chain_fingerprints :=
      v_initial_chain_fingerprints
      || jsonb_build_object(
        v_root_id::text,
        v_chain ->> 'chain_fingerprint'
      );

    v_initial_anchor_fingerprints :=
      v_initial_anchor_fingerprints
      || jsonb_build_object(
        v_root_id::text,
        v_chain ->> 'correction_financials_policy_envelope_fingerprint'
      );
  END LOOP;

  SELECT COALESCE(
    array_agg(DISTINCT root_id ORDER BY root_id),
    ARRAY[]::uuid[]
  )
  INTO v_root_ids
  FROM unnest(v_root_ids) AS root_scope(root_id);

  SELECT COALESCE(
    array_agg(DISTINCT member_id ORDER BY member_id),
    ARRAY[]::uuid[]
  )
  INTO v_member_ids
  FROM unnest(v_member_ids) AS member_scope(member_id);

  v_root_count := cardinality(v_root_ids);
  v_member_count := cardinality(v_member_ids);

  IF v_member_count < 1 OR v_member_count > p_max_scope THEN
    RAISE EXCEPTION 'IMPORT_PREFLIGHT_EXPANDED_SCOPE_OUT_OF_RANGE'
      USING ERRCODE = '54001',
            DETAIL = jsonb_build_object(
              'code', 'IMPORT_PREFLIGHT_EXPANDED_SCOPE_OUT_OF_RANGE',
              'expanded_member_count', v_member_count,
              'max_scope', p_max_scope
            )::text;
  END IF;

  IF COALESCE(p_lock_rows, true) THEN
    FOREACH v_root_id IN ARRAY v_root_ids LOOP
      v_lock_acquired := pg_try_advisory_xact_lock(
        hashtextextended(
          'TIMESHEET_CORRECTION_CHAIN|' || v_root_id::text,
          24062026
        )
      );

      IF NOT v_lock_acquired THEN
        RAISE EXCEPTION 'IMPORT_PREFLIGHT_LOCK_BUSY'
          USING ERRCODE = '55P03',
                DETAIL = jsonb_build_object(
                  'code', 'IMPORT_PREFLIGHT_LOCK_BUSY',
                  'root_timesheet_id', v_root_id::text,
                  'retryable', true
                )::text;
      END IF;
    END LOOP;

    PERFORM 1
    FROM public.timesheets AS lock_timesheet
    WHERE lock_timesheet.timesheet_id = ANY(v_member_ids)
    ORDER BY lock_timesheet.timesheet_id
    FOR UPDATE;

    PERFORM 1
    FROM public.timesheets_financials AS lock_financial
    WHERE lock_financial.timesheet_id = ANY(v_member_ids)
      AND lock_financial.is_current = true
    ORDER BY lock_financial.timesheet_id, lock_financial.id
    FOR UPDATE;

    v_all_locks_acquired := true;
  END IF;

  FOREACH v_root_id IN ARRAY v_root_ids LOOP
    v_chain := public.timesheet_correction_chain_scope_v1(
      v_root_id,
      false,
      32,
      100
    );

    v_chain_fingerprint := v_chain ->> 'chain_fingerprint';

    IF v_chain_fingerprint IS DISTINCT FROM
       (v_initial_chain_fingerprints ->> v_root_id::text) THEN
      v_errors_json := v_errors_json || jsonb_build_array(
        jsonb_build_object(
          'code', 'STALE_CORRECTION_CHAIN',
          'root_timesheet_id', v_root_id::text,
          'expected_fingerprint',
            v_initial_chain_fingerprints ->> v_root_id::text,
          'actual_fingerprint', v_chain_fingerprint
        )
      );
    END IF;


    v_anchor_fingerprint :=
      NULLIF(v_chain ->> 'correction_financials_policy_envelope_fingerprint', '');

    IF v_anchor_fingerprint IS DISTINCT FROM
       NULLIF(v_initial_anchor_fingerprints ->> v_root_id::text, '') THEN
      v_errors_json := v_errors_json || jsonb_build_array(
        jsonb_build_object(
          'code', 'STALE_CORRECTION_FINANCE_ANCHOR',
          'root_timesheet_id', v_root_id::text,
          'expected_anchor_fingerprint',
            NULLIF(v_initial_anchor_fingerprints ->> v_root_id::text, ''),
          'actual_anchor_fingerprint', v_anchor_fingerprint
        )
      );
    END IF;

    IF COALESCE(
         (v_chain ->> 'correction_financials_policy_envelope_required')::boolean,
         false
       )
       AND v_anchor_fingerprint IS NULL THEN
      v_errors_json := v_errors_json || jsonb_build_array(
        jsonb_build_object(
          'code', 'CORRECTION_FINANCE_ANCHOR_UNRESOLVED',
          'root_timesheet_id', v_root_id::text,
          'details', COALESCE(v_chain -> 'errors', '[]'::jsonb)
        )
      );
    END IF;

    v_expected_anchor_fingerprint := COALESCE(
      v_expected_state #>> ARRAY[
        'correction_financials_policy_envelope_fingerprints',
        v_root_id::text
      ],
      v_expected_state #>> ARRAY[
        'anchor_fingerprints',
        v_root_id::text
      ]
    );

    IF v_expected_anchor_fingerprint IS NOT NULL
       AND v_expected_anchor_fingerprint
             IS DISTINCT FROM v_anchor_fingerprint THEN
      v_errors_json := v_errors_json || jsonb_build_array(
        jsonb_build_object(
          'code', 'EXPECTED_CORRECTION_FINANCE_ANCHOR_MISMATCH',
          'root_timesheet_id', v_root_id::text,
          'expected_anchor_fingerprint', v_expected_anchor_fingerprint,
          'actual_anchor_fingerprint', v_anchor_fingerprint
        )
      );
    END IF;

    v_correction_financials_policy_envelope_fingerprints :=
      v_correction_financials_policy_envelope_fingerprints
      || jsonb_build_object(
        v_root_id::text,
        v_anchor_fingerprint
      );

    v_correction_financials_policy_envelopes :=
      v_correction_financials_policy_envelopes
      || jsonb_build_object(
        v_root_id::text,
        v_chain -> 'correction_financials_policy_envelope'
      );

    v_expected_fingerprint :=
      v_expected_state #>> ARRAY[
        'chain_fingerprints',
        v_root_id::text
      ];

    IF v_expected_fingerprint IS NOT NULL
       AND v_expected_fingerprint IS DISTINCT FROM v_chain_fingerprint THEN
      v_errors_json := v_errors_json || jsonb_build_array(
        jsonb_build_object(
          'code', 'EXPECTED_CORRECTION_CHAIN_FINGERPRINT_MISMATCH',
          'root_timesheet_id', v_root_id::text,
          'expected_fingerprint', v_expected_fingerprint,
          'actual_fingerprint', v_chain_fingerprint
        )
      );
    END IF;

    IF COALESCE((v_chain ->> 'valid')::boolean, false) IS NOT TRUE THEN
      v_errors_json := v_errors_json || jsonb_build_array(
        jsonb_build_object(
          'code', 'CORRECTION_CHAIN_INCOMPLETE',
          'root_timesheet_id', v_root_id::text,
          'details', COALESCE(v_chain -> 'errors', '[]'::jsonb)
        )
      );
    END IF;

    v_chains_json := v_chains_json || jsonb_build_array(v_chain);
  END LOOP;

  FOR v_member_row IN
    SELECT
      timesheet_row.timesheet_id,
      timesheet_row.booking_id,
      timesheet_row.version,
      timesheet_row.is_current,
      timesheet_row.parent_timesheet_id,
      timesheet_row.correction_id,
      timesheet_row.correction_kind,
      timesheet_row.adjustment_origin,
      timesheet_row.contract_id,
      timesheet_row.week_ending_date,
      timesheet_row.actual_schedule_json,
      timesheet_row.additional_units_week,
      timesheet_row.additional_units_per_day,
      timesheet_row.reference_number,
      timesheet_row.authorised_at_server,
      timesheet_row.status,
      timesheet_row.archived_at_utc,

      current_financial.id AS current_tsfin_id,
      current_financial.is_stale AS current_tsfin_is_stale,
      current_financial.processing_status,
      current_financial.processed_at_utc,
      current_financial.authorised_at_utc,
      current_financial.candidate_id,
      current_financial.client_id,
      current_financial.pay_method,
      current_financial.policy_snapshot_json,
      current_financial.rate_source_refs_json,
      current_financial.actual_schedule_json AS tsfin_actual_schedule_json,
      current_financial.total_hours,
      current_financial.total_pay_ex_vat,
      current_financial.total_charge_ex_vat,
      current_financial.pay_vat_rate_pct_snapshot,
      current_financial.pay_vat_amount_snapshot,
      current_financial.pay_total_inc_vat_snapshot,
      COALESCE(
        current_financial.policy_snapshot_json ->>
          'correction_financials_policy_envelope_fingerprint',
        current_financial.policy_snapshot_json #>>
          '{correction_financials_policy_envelope,envelope_fingerprint}',
        current_financial.rate_source_refs_json ->>
          'correction_financials_policy_envelope_fingerprint',
        current_financial.rate_source_refs_json #>>
          '{correction_financials_policy_envelope,envelope_fingerprint}'
      ) AS current_correction_financials_policy_envelope_fingerprint,
      current_financial.computed_at_utc,
      current_financial.locked_by_invoice_id,
      current_financial.paid_at_utc,

      EXISTS (
        SELECT 1
        FROM public.timesheets_financials AS paid_financial
        WHERE paid_financial.timesheet_id = timesheet_row.timesheet_id
          AND paid_financial.paid_at_utc IS NOT NULL
      )
      OR EXISTS (
        SELECT 1
        FROM public.pay_batch_items AS settled_item
        JOIN public.pay_batch_candidates AS settled_candidate
          ON settled_candidate.id = settled_item.pay_batch_candidate_id
        WHERE settled_item.timesheet_id = timesheet_row.timesheet_id
          AND COALESCE(settled_item.is_voided, false) = false
          AND (
            UPPER(BTRIM(COALESCE(
              settled_candidate.settlement_status,
              ''
            ))) = 'SETTLED'
            OR settled_candidate.settled_at_utc IS NOT NULL
          )
      ) AS has_paid_evidence,

      EXISTS (
        SELECT 1
        FROM public.invoice_lines AS invoice_line
        WHERE invoice_line.timesheet_id = timesheet_row.timesheet_id
      )
      OR current_financial.locked_by_invoice_id IS NOT NULL
      OR EXISTS (
        SELECT 1
        FROM public.nhsp_shifts AS invoice_shift
        WHERE invoice_shift.timesheet_id = timesheet_row.timesheet_id
          AND invoice_shift.invoice_id IS NOT NULL
      )
      OR EXISTS (
        SELECT 1
        FROM jsonb_array_elements(
          CASE
            WHEN jsonb_typeof(current_financial.invoice_breakdown_json) = 'array'
              THEN current_financial.invoice_breakdown_json
            WHEN jsonb_typeof(current_financial.invoice_breakdown_json) = 'object'
                 AND jsonb_typeof(current_financial.invoice_breakdown_json -> 'segments') = 'array'
              THEN current_financial.invoice_breakdown_json -> 'segments'
            ELSE '[]'::jsonb
          END
        ) AS invoice_segment(segment)
        WHERE NULLIF(BTRIM(COALESCE(
          invoice_segment.segment ->> 'invoice_locked_invoice_id',
          ''
        )), '') IS NOT NULL
      )
        AS has_invoice_evidence
    FROM public.timesheets AS timesheet_row
    LEFT JOIN LATERAL (
      SELECT financial_row.*
      FROM public.timesheets_financials AS financial_row
      WHERE financial_row.timesheet_id = timesheet_row.timesheet_id
        AND financial_row.is_current = true
      ORDER BY financial_row.computed_at_utc DESC, financial_row.id DESC
      LIMIT 1
    ) AS current_financial
      ON true
    WHERE timesheet_row.timesheet_id = ANY(v_member_ids)
    ORDER BY timesheet_row.timesheet_id
  LOOP
    v_signature_payload := jsonb_strip_nulls(
      jsonb_build_object(
        'timesheet_id', v_member_row.timesheet_id::text,
        'booking_id', v_member_row.booking_id,
        'version', v_member_row.version,
        'is_current', v_member_row.is_current,
        'parent_timesheet_id', CASE
          WHEN v_member_row.parent_timesheet_id IS NULL THEN NULL
          ELSE v_member_row.parent_timesheet_id::text
        END,
        'correction_id', v_member_row.correction_id,
        'correction_kind', v_member_row.correction_kind,
        'adjustment_origin', v_member_row.adjustment_origin,
        'contract_id', CASE
          WHEN v_member_row.contract_id IS NULL THEN NULL
          ELSE v_member_row.contract_id::text
        END,
        'week_ending_date', v_member_row.week_ending_date,
        'actual_schedule_json', COALESCE(
          v_member_row.actual_schedule_json,
          '[]'::jsonb
        ),
        'additional_units_week', COALESCE(
          v_member_row.additional_units_week,
          '{}'::jsonb
        ),
        'additional_units_per_day', COALESCE(
          v_member_row.additional_units_per_day,
          '{}'::jsonb
        ),
        'reference_number', v_member_row.reference_number,
        'authorised_at_server', v_member_row.authorised_at_server,
        'status', v_member_row.status::text,
        'archived_at_utc', v_member_row.archived_at_utc,
        'current_tsfin_id', CASE
          WHEN v_member_row.current_tsfin_id IS NULL THEN NULL
          ELSE v_member_row.current_tsfin_id::text
        END,
        'current_tsfin_is_stale', v_member_row.current_tsfin_is_stale,
        'processing_status', CASE
          WHEN v_member_row.processing_status IS NULL THEN NULL
          ELSE v_member_row.processing_status::text
        END,
        'processed_at_utc', v_member_row.processed_at_utc,
        'tsfin_authorised_at_utc', v_member_row.authorised_at_utc,
        'candidate_id', CASE
          WHEN v_member_row.candidate_id IS NULL THEN NULL
          ELSE v_member_row.candidate_id::text
        END,
        'client_id', CASE
          WHEN v_member_row.client_id IS NULL THEN NULL
          ELSE v_member_row.client_id::text
        END,
        'pay_method', v_member_row.pay_method,
        'policy_snapshot_json', COALESCE(
          v_member_row.policy_snapshot_json,
          '{}'::jsonb
        ),
        'rate_source_refs_json', COALESCE(
          v_member_row.rate_source_refs_json,
          '{}'::jsonb
        ),
        'tsfin_actual_schedule_json', COALESCE(
          v_member_row.tsfin_actual_schedule_json,
          '[]'::jsonb
        ),
        'total_hours', v_member_row.total_hours,
        'total_pay_ex_vat', v_member_row.total_pay_ex_vat,
        'total_charge_ex_vat', v_member_row.total_charge_ex_vat,
        'pay_vat_rate_pct_snapshot',
          v_member_row.pay_vat_rate_pct_snapshot,
        'pay_vat_amount_snapshot',
          v_member_row.pay_vat_amount_snapshot,
        'pay_total_inc_vat_snapshot',
          v_member_row.pay_total_inc_vat_snapshot,
        'correction_financials_policy_envelope_fingerprint',
          v_member_row.current_correction_financials_policy_envelope_fingerprint,
        'computed_at_utc', v_member_row.computed_at_utc,
        'locked_by_invoice_id', CASE
          WHEN v_member_row.locked_by_invoice_id IS NULL THEN NULL
          ELSE v_member_row.locked_by_invoice_id::text
        END,
        'paid_at_utc', v_member_row.paid_at_utc
      )
    );

    v_timesheet_signature := encode(
      extensions.digest(
        convert_to(v_signature_payload::text, 'UTF8'),
        'sha256'
      ),
      'hex'
    );

    v_timesheet_signatures :=
      v_timesheet_signatures
      || jsonb_build_object(
        v_member_row.timesheet_id::text,
        v_timesheet_signature
      );

    v_expected_timesheet_signature :=
      v_expected_state #>> ARRAY[
        'timesheet_signatures',
        v_member_row.timesheet_id::text
      ];

    IF v_expected_timesheet_signature IS NOT NULL
       AND v_expected_timesheet_signature
           IS DISTINCT FROM v_timesheet_signature THEN
      v_errors_json := v_errors_json || jsonb_build_array(
        jsonb_build_object(
          'code', 'EXPECTED_TIMESHEET_SIGNATURE_MISMATCH',
          'timesheet_id', v_member_row.timesheet_id::text,
          'expected_signature', v_expected_timesheet_signature,
          'actual_signature', v_timesheet_signature
        )
      );
    END IF;

    IF v_member_row.authorised_at_server IS NOT NULL
       OR v_member_row.authorised_at_utc IS NOT NULL THEN
      v_authorised_count := v_authorised_count + 1;
    END IF;

    IF v_member_row.processed_at_utc IS NOT NULL
       OR UPPER(BTRIM(COALESCE(
         v_member_row.processing_status::text,
         ''
       ))) = 'PROCESSED' THEN
      v_processed_count := v_processed_count + 1;
    END IF;

    IF COALESCE(v_member_row.has_paid_evidence, false) THEN
      v_paid_count := v_paid_count + 1;
    END IF;

    IF COALESCE(v_member_row.has_invoice_evidence, false) THEN
      v_invoice_lined_count := v_invoice_lined_count + 1;
    END IF;

    IF COALESCE(v_member_row.current_tsfin_is_stale, false) THEN
      v_stale_tsfin_count := v_stale_tsfin_count + 1;
    END IF;

    v_member_summaries := v_member_summaries || jsonb_build_array(
      jsonb_strip_nulls(
        jsonb_build_object(
          'timesheet_id', v_member_row.timesheet_id::text,
          'timesheet_signature', v_timesheet_signature,
          'authorised',
            v_member_row.authorised_at_server IS NOT NULL
            OR v_member_row.authorised_at_utc IS NOT NULL,
          'processed',
            v_member_row.processed_at_utc IS NOT NULL
            OR UPPER(BTRIM(COALESCE(
              v_member_row.processing_status::text,
              ''
            ))) = 'PROCESSED',
          'paid', COALESCE(v_member_row.has_paid_evidence, false),
          'invoice_lined',
            COALESCE(v_member_row.has_invoice_evidence, false),
          'current_tsfin_id', CASE
            WHEN v_member_row.current_tsfin_id IS NULL THEN NULL
            ELSE v_member_row.current_tsfin_id::text
          END,
          'current_tsfin_is_stale',
            v_member_row.current_tsfin_is_stale,
          'candidate_id', CASE
            WHEN v_member_row.candidate_id IS NULL THEN NULL
            ELSE v_member_row.candidate_id::text
          END,
          'client_id', CASE
            WHEN v_member_row.client_id IS NULL THEN NULL
            ELSE v_member_row.client_id::text
          END,
          'pay_method', v_member_row.pay_method,
          'correction_financials_policy_envelope_fingerprint',
            v_member_row.current_correction_financials_policy_envelope_fingerprint
        )
      )
    );
  END LOOP;

  WITH blocking_source AS (
    SELECT
      batch_row.id AS pay_batch_id,
      batch_row.status,
      batch_row.pay_date,
      batch_row.bulk_reference,
      'PAY_BATCH_ITEM'::text AS blocker_source
    FROM public.pay_batch_items AS batch_item
    JOIN public.pay_batch_candidates AS batch_candidate
      ON batch_candidate.id = batch_item.pay_batch_candidate_id
    JOIN public.pay_batches AS batch_row
      ON batch_row.id = batch_candidate.pay_batch_id
    WHERE batch_item.timesheet_id = ANY(v_member_ids)
      AND COALESCE(batch_item.is_voided, false) = false
      AND public._pay_batch_status_is_active_reservation(batch_row.status)

    UNION ALL

    SELECT
      batch_row.id,
      batch_row.status,
      batch_row.pay_date,
      batch_row.bulk_reference,
      'PAY_BATCH_TIMESHEET_SNAPSHOT'
    FROM public.pay_batch_timesheet_snapshots AS batch_snapshot
    JOIN public.pay_batches AS batch_row
      ON batch_row.id = batch_snapshot.pay_batch_id
    WHERE batch_snapshot.timesheet_id = ANY(v_member_ids)
      AND public._pay_batch_status_is_active_reservation(batch_row.status)

    UNION ALL

    SELECT
      batch_row.id,
      batch_row.status,
      batch_row.pay_date,
      batch_row.bulk_reference,
      'PAYMENT_CORRECTION_ITEM'
    FROM public.pay_payment_correction_items AS correction_item
    JOIN public.pay_batches AS batch_row
      ON batch_row.id = correction_item.pay_batch_id
    WHERE correction_item.timesheet_id = ANY(v_member_ids)
      AND UPPER(BTRIM(COALESCE(correction_item.status, '')))
          = 'APPLIED'
      AND public._pay_batch_status_is_active_reservation(batch_row.status)

    UNION ALL

    SELECT
      batch_row.id,
      batch_row.status,
      batch_row.pay_date,
      batch_row.bulk_reference,
      'PAYMENT_OVERRIDE'
    FROM public.timesheet_payment_overrides AS payment_override
    JOIN public.pay_batches AS batch_row
      ON batch_row.id = payment_override.consumed_by_pay_batch_id
    WHERE payment_override.timesheet_id = ANY(v_member_ids)
      AND payment_override.cleared_at_utc IS NULL
      AND public._pay_batch_status_is_active_reservation(batch_row.status)

    UNION ALL

    SELECT
      batch_row.id,
      batch_row.status,
      batch_row.pay_date,
      batch_row.bulk_reference,
      'FINANCE_CASE_RESERVATION'
    FROM public.pay_advances AS finance_case
    JOIN public.pay_advance_reservations AS reservation_row
      ON reservation_row.finance_case_id = finance_case.id
    JOIN public.pay_batches AS batch_row
      ON batch_row.id = reservation_row.pay_batch_id
    WHERE finance_case.linked_timesheet_id = ANY(v_member_ids)
      AND UPPER(BTRIM(COALESCE(reservation_row.status, '')))
          IN ('RESERVED', 'COMMITTED')
      AND reservation_row.released_at_utc IS NULL
      AND reservation_row.settled_at_utc IS NULL
      AND public._pay_batch_status_is_active_reservation(batch_row.status)

    UNION ALL

    SELECT
      batch_row.id,
      batch_row.status,
      batch_row.pay_date,
      batch_row.bulk_reference,
      'FINANCE_COMPONENT_RESERVATION'
    FROM public.pay_finance_case_components AS finance_component
    JOIN public.pay_advance_reservations AS reservation_row
      ON reservation_row.finance_component_id = finance_component.id
    JOIN public.pay_batches AS batch_row
      ON batch_row.id = reservation_row.pay_batch_id
    WHERE finance_component.linked_timesheet_id = ANY(v_member_ids)
      AND UPPER(BTRIM(COALESCE(reservation_row.status, '')))
          IN ('RESERVED', 'COMMITTED')
      AND reservation_row.released_at_utc IS NULL
      AND reservation_row.settled_at_utc IS NULL
      AND public._pay_batch_status_is_active_reservation(batch_row.status)
  ),
  blocking_rollup AS (
    SELECT
      blocker.pay_batch_id,
      min(blocker.status) AS status,
      min(blocker.pay_date) AS pay_date,
      min(blocker.bulk_reference) AS bulk_reference,
      array_agg(DISTINCT blocker.blocker_source ORDER BY blocker.blocker_source)
        AS blocker_sources
    FROM blocking_source AS blocker
    GROUP BY blocker.pay_batch_id
  )
  SELECT
    count(*)::integer,
    COALESCE(
      (
        SELECT jsonb_agg(
          jsonb_build_object(
            'pay_batch_id', limited_blocker.pay_batch_id::text,
            'status', limited_blocker.status,
            'pay_date', limited_blocker.pay_date,
            'bulk_reference', limited_blocker.bulk_reference,
            'sources', to_jsonb(limited_blocker.blocker_sources)
          )
          ORDER BY limited_blocker.pay_date, limited_blocker.pay_batch_id
        )
        FROM (
          SELECT *
          FROM blocking_rollup
          ORDER BY pay_date, pay_batch_id
          LIMIT 100
        ) AS limited_blocker
      ),
      '[]'::jsonb
    )
  INTO v_blocking_batch_count, v_blocking_batches_json
  FROM blocking_rollup;

  IF v_blocking_batch_count > 100 THEN
    v_errors_json := v_errors_json || jsonb_build_array(
      jsonb_build_object(
        'code', 'IMPORT_PREFLIGHT_BLOCKER_LIMIT_EXCEEDED',
        'blocking_batch_count', v_blocking_batch_count,
        'max_reported', 100
      )
    );
  END IF;

  v_allowed :=
    v_blocking_batch_count = 0
    AND jsonb_array_length(v_errors_json) = 0;

  v_required_path := CASE
    WHEN v_blocking_batch_count > 0
      THEN 'BLOCKED_ACTIVE_PAY_DRAFT'
    WHEN jsonb_array_length(v_errors_json) > 0
      THEN 'BLOCKED_STALE_OR_INVALID_SCOPE'
    WHEN v_invoice_lined_count > 0
      THEN 'CREATE_OR_UPDATE_CORRECTION_CHAIN'
    WHEN v_paid_count > 0
      THEN 'PAID_UNINVOICED_ROLLOVER'
    WHEN v_authorised_count > 0
      THEN 'UNAUTHORISE_AMEND_RECALCULATE_REAUTHORISE'
    ELSE 'DIRECT_AMEND_RECALCULATE'
  END;

  v_preflight_fingerprint := encode(
    extensions.digest(
      convert_to(
        jsonb_build_object(
          'action', v_action,
          'input_timesheet_ids', to_jsonb(v_input_ids),
          'root_timesheet_ids', to_jsonb(v_root_ids),
          'member_timesheet_ids', to_jsonb(v_member_ids),
          'chain_fingerprints',
            COALESCE(
              (
                SELECT jsonb_object_agg(
                  chain_element.value ->> 'root_timesheet_id',
                  chain_element.value ->> 'chain_fingerprint'
                )
                FROM jsonb_array_elements(v_chains_json)
                  AS chain_element(value)
              ),
              '{}'::jsonb
            ),
          'correction_financials_policy_envelope_fingerprints',
            v_correction_financials_policy_envelope_fingerprints,
          'timesheet_signatures', v_timesheet_signatures,
          'blocking_batches', v_blocking_batches_json
        )::text,
        'UTF8'
      ),
      'sha256'
    ),
    'hex'
  );

  RETURN jsonb_build_object(
    'ok', true,
    'allowed', v_allowed,
    'action', v_action,
    'required_path', v_required_path,
    'locks_requested', COALESCE(p_lock_rows, true),
    'locks_acquired', v_all_locks_acquired,
    'raw_input_count', v_raw_input_count,
    'input_count', v_input_count,
    'root_count', v_root_count,
    'member_count', v_member_count,
    'input_timesheet_ids', to_jsonb(v_input_ids),
    'root_timesheet_ids', to_jsonb(v_root_ids),
    'member_timesheet_ids', to_jsonb(v_member_ids),
    'chains', v_chains_json,
    'correction_financials_policy_envelopes', v_correction_financials_policy_envelopes,
    'correction_financials_policy_envelope_fingerprints',
      v_correction_financials_policy_envelope_fingerprints,
    'members', v_member_summaries,
    'timesheet_signatures', v_timesheet_signatures,
    'authorised_count', v_authorised_count,
    'processed_count', v_processed_count,
    'paid_count', v_paid_count,
    'invoice_lined_count', v_invoice_lined_count,
    'stale_tsfin_count', v_stale_tsfin_count,
    'blocking_batch_count', v_blocking_batch_count,
    'blocking_batches', v_blocking_batches_json,
    'errors', v_errors_json,
    'preflight_fingerprint', v_preflight_fingerprint
  );
END;
$function$;

-- invoice_apply_edits(uuid,jsonb,uuid)
CREATE OR REPLACE FUNCTION public.invoice_apply_edits(p_invoice_id uuid, p_payload jsonb, p_actor_user_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_now timestamptz := now();
  v_anchor_ymd date := (now() at time zone 'Europe/London')::date;


-- =====================================================
-- DEBUG (invoice_debug): single audit row per RPC call
-- =====================================================
v_invoice_debug boolean := false;
v_dbg_started_at timestamptz := now();
v_dbg_steps jsonb := '[]'::jsonb;
v_dbg_sqlstate text := null;
v_dbg_error text := null;
v_dbg_stats jsonb := '{}'::jsonb;


v_dbg_lines_deleted int := 0;
v_dbg_timesheets_unlocked int := 0;
v_dbg_seg_add_refs int := 0;
v_dbg_seg_remove_refs int := 0;
v_dbg_seg_tsfins int := 0;
v_dbg_seg_timesheets_rebuilt int := 0;
v_dbg_seg_timesheets_removed int := 0;
v_dbg_add_timesheets_found int := 0;
v_dbg_add_timesheets_skipped int := 0;

v_rc int := 0;

  v_inv record;
  v_week_start date;
  v_week_end date;

  v_remove_ids uuid[];
  v_add_ts_ids uuid[];

-- segment move payload (explicit segment add/remove)
v_remove_seg_refs jsonb;
v_add_seg_refs jsonb;
v_has_seg_ops boolean := false;
v_refresh_hr_cache boolean := false;
v_seg_tsfin_ids uuid[] := array[]::uuid[];
v_seg_ts_ids uuid[] := array[]::uuid[];
v_seg_refs_to_lock jsonb := '[]'::jsonb;
v_ref jsonb;
v_tsfin_id uuid;
v_seg_id text;
v_has_additional boolean;
v_has_expense_or_mileage boolean;

  -- reference updates (refs-to-issue)
  v_reference_updates jsonb;
  v_refupd jsonb;
  v_refupd_ts_id uuid;
  v_refupd_count int := 0;
  v_refupd_applied int := 0;
  v_refupd_set_refnum boolean;
  v_refupd_set_dayrefs boolean;
  v_refupd_set_sched boolean;
  v_refupd_dayrefs jsonb;
  v_refupd_sched jsonb;
  v_refupd_refnum text;

  -- timesheet hospital / ward source edits
  v_location_updates jsonb;
  v_location_update jsonb;
  v_location_ts_id uuid;
  v_location_count int := 0;
  v_location_applied int := 0;
  v_location_set_hospital boolean;
  v_location_set_ward boolean;
  v_location_hospital_norm text;
  v_location_ward_norm text;
  v_location_ts_ids uuid[] := array[]::uuid[];
  -- Source-edit contract state. Reference and location inputs are merged into
  -- one desired row per timesheet, then written by one statement so the
  -- statement-level invalidation trigger advances each source exactly once.
  v_source_updates_map jsonb := '{}'::jsonb;
  v_source_update jsonb;
  v_source_edit_key text;
  v_source_expected_revision bigint;
  v_source_changed_ts_ids uuid[] := array[]::uuid[];
  v_source_changed_revisions jsonb := '[]'::jsonb;
  v_source_edit_requested boolean := false;
  v_allowed_reference_fields text[]:=array[
    'timesheet_id','expected_document_revision','reference_number',
    'day_references_json','actual_schedule_json'];
  v_allowed_location_fields text[]:=array[
    'timesheet_id','expected_document_revision','hospital_norm','ward_norm'];
  v_allowed_reference_segment_fields text[]:=array[
    'segment_id','date','start_utc','end_utc','start','end','ref_num',
    'source_system'];
  v_source_unsupported_field text;
  v_source_financial_id uuid;
  v_source_mode text;
  v_source_ib jsonb;
  v_source_payload_schedule jsonb;
  v_source_canonical_schedule jsonb;
  v_source_new_segments jsonb;
  v_source_segment_updates jsonb;
  v_source_segment_reference_changed boolean;
  v_source_timesheet_row_changed boolean;
  v_source_match_count integer;
  v_source_match_segment jsonb;
  v_source_payload_segment jsonb;
  v_source_current_segment jsonb;
  v_source_payload_ord bigint;
  v_source_start timestamptz;
  v_source_end timestamptz;
  v_source_marker_rows jsonb:='{}'::jsonb;
  v_source_marker jsonb:='{}'::jsonb;
  v_source_edit_preexisting_preview boolean := false;
  v_source_edit_invoice_replacement_required boolean := false;
  v_pre_edit_invoice_revision bigint;
  v_pre_edit_preview_document_version_id uuid;
  v_pre_edit_active_document_operation_id uuid;
  v_expected_command_count int := 0;
  v_operation_result jsonb;
  v_operation_id uuid;
  v_document_version_id uuid;
  v_command_no int;
  v_command_type text;
  v_operation_status text;
  v_operation_input jsonb;
  v_validated_operations jsonb := '[]'::jsonb;
  v_document_commands jsonb := '[]'::jsonb;
  v_started_operations jsonb := '[]'::jsonb;

  -- reference update side-effects (meta refresh / segment ref sync)
  v_refupd_ts_ids uuid[] := array[]::uuid[];
  v_refupd_ts_ids_distinct uuid[] := array[]::uuid[];
  v_refupd_meta_rows_updated int := 0;
  v_refupd_tsfin_rows_updated int := 0;
  v_refupd_tsfin_segments_updated int := 0;
  v_refupd_invoice_fallback_invalidated boolean := false;

  -- temp vars for reference-derived meta and optional tsfin segment ref sync
  v_ref_ts_id uuid;
  v_ref_ts record;
  v_ref_schedule_refs jsonb := '[]'::jsonb;
  v_ref_schedule_refs_distinct jsonb := '[]'::jsonb;
  v_ref_sched_map jsonb := '{}'::jsonb;
  v_ref_seen_keys jsonb := '{}'::jsonb;
  v_ref_sched_key text;
  v_ref_sched_ref text;
  v_ref_tsfin_id uuid;
  v_ref_ib jsonb;
  v_ref_new_segments jsonb := '[]'::jsonb;
  v_ref_seg_obj jsonb;
  v_ref_seg_start text;
  v_ref_seg_end text;
    v_ref_seg_id text;
v_ref_seg_cur_ref text;
  v_ref_seg_new_ref text;
  v_ref_seg_has_update boolean := false;
  v_ref_seg_updates_this_ts int := 0;
  v_ref_seg_matches_this_ts int := 0;

  -- audit (history) accumulators (NOT debug-only)
  v_hist_adj jsonb := '[]'::jsonb;
  v_hist_seg_add jsonb := '[]'::jsonb;
  v_hist_seg_remove jsonb := '[]'::jsonb;
  v_hist_lines_removed jsonb := '[]'::jsonb;
  v_hist_add_ts jsonb := '[]'::jsonb;

  -- contract week status touch set
  v_cw_ts_ids uuid[] := array[]::uuid[];

  v_ts_ids_touched uuid[] := array[]::uuid[];
  -- FIX: timesheets fully removed from this invoice (no remaining invoice_lines) should be unlocked
  v_ts_ids_fully_removed uuid[] := array[]::uuid[];

  v_vat_chargeable boolean := true;
  v_vat_rate numeric := 0;

  -- adjustments
  adj jsonb;
  v_adj_token text;
  v_adj_desc text;
  v_adj_ex numeric;
  v_adj_vat numeric;
  v_adj_inc numeric;
  v_adj_source_key text;
  v_meta jsonb;

  -- timesheet loop
  tsid uuid;
  snap record;
  ts record;
  pc record;
  contract_id uuid;
  v_client_daily_calc boolean := false;
  v_contract_override boolean := false;
  v_contract_daily_calc boolean;
  v_contract_bucket_labels jsonb;
  c_daily_calc boolean := false;
  c_bucket_labels jsonb := null;
  c_role text := null;
  c_display_site text := null;
  c_ward_hint text := null;

  -- segment filtering
  v_seg jsonb;
  segments jsonb := '[]'::jsonb;
  seg_target date;
  seg_date text;
  natural_start date;
  seg_locked text;
  seg_ref text;

  -- aggregation for weekly line
  h_day numeric; h_night numeric; h_sat numeric; h_sun numeric; h_bh numeric;
  pay_ex numeric; chg_ex numeric; margin_ex numeric;
  vat_amt numeric; inc_amt numeric;
  line_desc text;
  v_source_key text;
  v_exact_timesheet_document_r2_key text;

  -- daily aggregation record
  r_day record;

  -- segment refs for locking
  seg_refs jsonb := '[]'::jsonb;

  -- additional units
  kv record;
  ex jsonb;
  code text;
  unit_count numeric;
  bucket_name text;
  unit_name text;

  -- expenses notes
  v_note_travel text;
  v_note_accom text;
  v_note_other text;

  -- recompute totals
  v_new_ex numeric := 0;
  v_new_vat numeric := 0;
  v_new_inc numeric := 0;

  -- header meta counters (keep header_snapshot_json.meta in sync with current invoice state)
  v_hdr_ts_count_lines int := 0;
  v_hdr_ts_count_seglocks int := 0;
  v_hdr_seg_locked_count int := 0;
  v_hdr_meta_timesheet_count int := 0;
  v_hdr_meta_segment_count int := 0;

  v_manifest jsonb;
  v_correction_placement_only boolean:=false;
  v_correction_placement_ts_id uuid:=null;
  v_correction_placement_scope jsonb:='{}'::jsonb;
  v_correction_placement_states jsonb:='[]'::jsonb;
  v_service boolean:=coalesce(auth.role(),'')='service_role';
  v_actor_role text;
begin

  if not v_service
     and(auth.uid() is null or auth.uid() is distinct from p_actor_user_id) then
    raise exception using errcode='42501',
      message='AUTHENTICATED_ACTOR_MISMATCH';
  end if;
  select lower(btrim(coalesce(u.role,''))) into v_actor_role
  from public.tms_users u
  where u.id=p_actor_user_id and u.is_active;
  if(not found or v_actor_role<>'admin') and not v_service then
    raise exception using errcode='42501',
      message='INVOICE_ADMINISTRATOR_PERMISSION_REQUIRED';
  end if;

  perform public._ctms_assert_invoice_mutable_draft_v1(
    p_invoice_id,'INVOICE_APPLY_EDITS',true
  );
  v_correction_placement_only:=
    jsonb_typeof(coalesce(p_payload,'{}'::jsonb))='object'
    and not exists(select 1 from jsonb_object_keys(coalesce(p_payload,'{}'::jsonb)) payload_key
      where payload_key not in ('remove_invoice_line_ids','add_timesheet_ids'))
    and coalesce(jsonb_array_length(case when jsonb_typeof(p_payload->'remove_invoice_line_ids')='array'
      then p_payload->'remove_invoice_line_ids' else '[]'::jsonb end),0)
      +coalesce(jsonb_array_length(case when jsonb_typeof(p_payload->'add_timesheet_ids')='array'
      then p_payload->'add_timesheet_ids' else '[]'::jsonb end),0)=1
    and (
      (case when coalesce(p_payload#>>'{remove_invoice_line_ids,0}','')
          ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then exists(
          select 1 from public.invoice_lines placement_line
          where placement_line.id=(p_payload#>>'{remove_invoice_line_ids,0}')::uuid
            and placement_line.invoice_id=p_invoice_id
            and coalesce((public._ctms_import_correction_classify_v1(placement_line.timesheet_id)
              ->>'is_import_authoritative_correction')::boolean,false)) else false end)
      or
      (case when coalesce(p_payload#>>'{add_timesheet_ids,0}','')
          ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
        coalesce((public._ctms_import_correction_classify_v1(
          (p_payload#>>'{add_timesheet_ids,0}')::uuid)->>'is_import_authoritative_correction')::boolean,false)
        else false end)
    );
  if public._ctms_invoice_payload_has_financial_edit_v1(p_payload)
     and not v_correction_placement_only
     and exists (
       select 1 from public.invoice_lines il
       where il.invoice_id=p_invoice_id and il.timesheet_id is not null
         and coalesce((public._ctms_import_correction_classify_v1(il.timesheet_id)
           ->>'is_import_authoritative_correction')::boolean,false)
     ) then
    raise exception 'IMPORT_CORRECTION_INVOICE_FINANCIAL_EDIT_FORBIDDEN'
      using errcode='P0001',detail=jsonb_build_object('invoice_id',p_invoice_id)::text;
  end if;
-- Load invoice_debug flag (safe even if column not yet present)
begin
  select coalesce(sd.invoice_debug, false)
  into v_invoice_debug
  from public.settings_defaults sd
  where sd.id = 1
  limit 1;
exception when undefined_column then
  v_invoice_debug := false;
end;

if v_invoice_debug then
  v_dbg_steps := v_dbg_steps || jsonb_build_array(
    jsonb_build_object(
      'step','start',
      'at_utc', public._inv_iso_utc(v_dbg_started_at),
      'invoice_id', p_invoice_id::text
    )
  );
end if;

  if p_invoice_id is null then
    raise exception 'invoice_id is required';
  end if;

  select *
  into v_inv
  from public.invoices i
  where i.id = p_invoice_id
  for update
  limit 1;

  if not found then
    raise exception 'Invoice not found';
  end if;

  -- Editable gate: DRAFT/ON_HOLD and unpaid
  if v_inv.status::text not in ('DRAFT','ON_HOLD') then
    raise exception 'Invoice is not editable (status=%)', v_inv.status::text;
  end if;

  if v_inv.paid_at_utc is not null then
    raise exception 'Invoice is not editable (already paid)';
  end if;



if v_invoice_debug then
  v_dbg_steps := v_dbg_steps || jsonb_build_array(
    jsonb_build_object(
      'step','invoice_loaded',
      'status', coalesce(v_inv.status::text,''),
      'paid_at_utc', case when v_inv.paid_at_utc is null then null else public._inv_iso_utc(v_inv.paid_at_utc) end
    )
  );
end if;

  -- Require invoice_week_start in header_snapshot_json.meta
  if v_inv.header_snapshot_json is null
     or btrim(coalesce(v_inv.header_snapshot_json #>> '{meta,invoice_week_start}','')) = ''
     or (v_inv.header_snapshot_json #>> '{meta,invoice_week_start}') !~ '^\d{4}-\d{2}-\d{2}$'
  then
    raise exception 'Invoice header_snapshot_json.meta.invoice_week_start is required for edits';
  end if;

  v_week_start := (v_inv.header_snapshot_json #>> '{meta,invoice_week_start}')::date;
  v_week_end := (v_week_start + interval '6 days')::date;


if v_invoice_debug then
  v_dbg_steps := v_dbg_steps || jsonb_build_array(
    jsonb_build_object(
      'step','invoice_week_loaded',
      'invoice_week_start', v_week_start::text,
      'invoice_week_end', v_week_end::text
    )
  );
end if;

  -- Load effective client setting daily_calc_of_invoices (used when contract.overrideclientsettings=false)
  begin
    select coalesce(cs0.daily_calc_of_invoices,false)
    into v_client_daily_calc
    from public.client_settings cs0
    where cs0.client_id = v_inv.client_id
      and (cs0.effective_from <= v_anchor_ymd or cs0.effective_from is null)
    order by cs0.effective_from desc nulls last
    limit 1;
  exception when others then
    v_client_daily_calc := false;
  end;

  -- VAT settings from invoice snapshot
  if jsonb_typeof(v_inv.header_snapshot_json->'vat_chargeable') = 'boolean' then
    v_vat_chargeable := (v_inv.header_snapshot_json->>'vat_chargeable')::boolean;
  else
    v_vat_chargeable := true;
  end if;

  if (v_inv.header_snapshot_json ? 'applied_vat_rate_pct') then
    begin
      v_vat_rate := (v_inv.header_snapshot_json->>'applied_vat_rate_pct')::numeric;
    exception when others then
      v_vat_rate := 0;
    end;
  else
    v_vat_rate := 0;
  end if;

  if v_vat_chargeable = false then
    v_vat_rate := 0;
  end if;

  -- Parse payload arrays
  v_remove_ids := null;
  if p_payload is not null and jsonb_typeof(p_payload) = 'object' and (p_payload ? 'remove_invoice_line_ids') then
    select array_agg((x)::uuid)
    into v_remove_ids
    from jsonb_array_elements_text(coalesce(p_payload->'remove_invoice_line_ids','[]'::jsonb)) x
    where nullif(btrim(coalesce(x,'')),'') is not null;
  end if;

  v_add_ts_ids := null;
  if p_payload is not null and jsonb_typeof(p_payload) = 'object' and (p_payload ? 'add_timesheet_ids') then
    select array_agg((x)::uuid)
    into v_add_ts_ids
    from jsonb_array_elements_text(coalesce(p_payload->'add_timesheet_ids','[]'::jsonb)) x
    where nullif(btrim(coalesce(x,'')),'') is not null;
  end if;

  IF v_correction_placement_only THEN
    IF COALESCE(cardinality(v_remove_ids),0)=1 THEN
      SELECT il.timesheet_id INTO v_correction_placement_ts_id
      FROM public.invoice_lines il
      WHERE il.id=v_remove_ids[1] AND il.invoice_id=p_invoice_id
      FOR UPDATE;
      IF v_correction_placement_ts_id IS NULL
         OR COALESCE((public._ctms_import_correction_classify_v1(v_correction_placement_ts_id)
             ->>'is_import_authoritative_correction')::boolean,false) IS NOT TRUE THEN
        RAISE EXCEPTION 'IMPORT_CORRECTION_PLACEMENT_ONLY_TARGET_INVALID' USING ERRCODE='P0001';
      END IF;
      v_correction_placement_scope:=public.invoice_correction_pair_scope_v1(
        v_correction_placement_ts_id,null::uuid,p_actor_user_id,true,100);
      IF COALESCE((v_correction_placement_scope->>'valid')::boolean,false) IS NOT TRUE
         OR v_correction_placement_scope->>'placement_state' NOT IN (
           'COMPLETE_SAME_INVOICE','COMPLETE_SPLIT_INVOICES') THEN
        RAISE EXCEPTION 'INVOICE_CORRECTION_PAIR_PLACEMENT_MOVE_NOT_STARTABLE'
          USING ERRCODE='P0001',DETAIL=v_correction_placement_scope::text;
      END IF;
    ELSIF COALESCE(cardinality(v_add_ts_ids),0)=1 THEN
      v_correction_placement_ts_id:=v_add_ts_ids[1];
      IF COALESCE((public._ctms_import_correction_classify_v1(v_correction_placement_ts_id)
             ->>'is_import_authoritative_correction')::boolean,false) IS NOT TRUE THEN
        RAISE EXCEPTION 'IMPORT_CORRECTION_PLACEMENT_ONLY_TARGET_INVALID' USING ERRCODE='P0001';
      END IF;
      v_correction_placement_scope:=public.invoice_correction_pair_scope_v1(
        v_correction_placement_ts_id,p_invoice_id,p_actor_user_id,true,100);
      IF COALESCE((v_correction_placement_scope->>'valid')::boolean,false) IS NOT TRUE
         OR v_correction_placement_scope->>'placement_state'<>'INCOMPLETE_MOVE'
         OR v_correction_placement_scope->>'missing_member_kind' IS DISTINCT FROM
              (SELECT t.correction_kind FROM public.timesheets t
               WHERE t.timesheet_id=v_correction_placement_ts_id)
         OR COALESCE((v_correction_placement_scope->>'target_appendable')::boolean,false) IS NOT TRUE THEN
        RAISE EXCEPTION 'INVOICE_CORRECTION_PAIR_PLACEMENT_TARGET_INVALID'
          USING ERRCODE='P0001',DETAIL=v_correction_placement_scope::text;
      END IF;
    ELSE
      RAISE EXCEPTION 'IMPORT_CORRECTION_PLACEMENT_ONLY_PAYLOAD_INVALID' USING ERRCODE='22023';
    END IF;
  END IF;


-- Parse segment move payloads (tsfin_id + segment_id)
v_remove_seg_refs := null;
if p_payload is not null and jsonb_typeof(p_payload) = 'object' and (p_payload ? 'remove_segment_refs') then
  v_remove_seg_refs := coalesce(p_payload->'remove_segment_refs','[]'::jsonb);
  if jsonb_typeof(v_remove_seg_refs) <> 'array' then
    v_remove_seg_refs := '[]'::jsonb;
  end if;
end if;

v_add_seg_refs := null;
if p_payload is not null and jsonb_typeof(p_payload) = 'object' and (p_payload ? 'add_segment_refs') then
  v_add_seg_refs := coalesce(p_payload->'add_segment_refs','[]'::jsonb);
  if jsonb_typeof(v_add_seg_refs) <> 'array' then
    v_add_seg_refs := '[]'::jsonb;
  end if;
end if;



-- Parse and strictly validate source edits. The expected source revision is
-- mandatory because values displayed by an older modal must never overwrite a
-- newer timesheet revision.
v_reference_updates := null;
if p_payload is not null and jsonb_typeof(p_payload)='object'
   and p_payload ? 'reference_updates' then
  v_reference_updates:=p_payload->'reference_updates';
  if jsonb_typeof(v_reference_updates)<>'array' then
    raise exception using errcode='22023',
      message='INVOICE_SOURCE_EDIT_PAYLOAD_INVALID',
      detail=jsonb_build_object('field','reference_updates','reason','ARRAY_REQUIRED')::text;
  end if;
  v_refupd_count:=jsonb_array_length(v_reference_updates);
  for v_refupd in select value from jsonb_array_elements(v_reference_updates) value loop
    if v_refupd is not null and jsonb_typeof(v_refupd)='object' then
      select min(field_name) into v_source_unsupported_field
      from jsonb_object_keys(v_refupd) field_name
      where not (field_name=any(v_allowed_reference_fields));
      if v_source_unsupported_field is not null then
        raise exception using errcode='22023',
          message='INVOICE_SOURCE_EDIT_PAYLOAD_INVALID',
          detail=jsonb_build_object(
            'field','reference_updates','reason','UNSUPPORTED_FIELD',
            'unsupported_field',v_source_unsupported_field)::text;
      end if;
    end if;
    if v_refupd is null or jsonb_typeof(v_refupd)<>'object'
       or nullif(btrim(coalesce(v_refupd->>'timesheet_id','')),'') is null
       or coalesce(v_refupd->>'timesheet_id','') !~
          '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
       or coalesce(v_refupd->>'expected_document_revision','') !~ '^[1-9][0-9]*$'
       or not (
         v_refupd ? 'reference_number'
         or v_refupd ? 'day_references_json'
         or v_refupd ? 'actual_schedule_json'
       )
       or (
         v_refupd ? 'reference_number'
         and jsonb_typeof(v_refupd->'reference_number') not in('string','null')
       )
       or (
         v_refupd ? 'day_references_json'
         and jsonb_typeof(v_refupd->'day_references_json') not in('object','null')
       )
       or (
         v_refupd ? 'actual_schedule_json'
         and jsonb_typeof(v_refupd->'actual_schedule_json') not in('array','null')
       )
    then
      raise exception using errcode='22023',
        message='INVOICE_SOURCE_EDIT_PAYLOAD_INVALID',
        detail=jsonb_build_object('field','reference_updates','reason','INVALID_ITEM')::text;
    end if;
    v_source_edit_key:=lower(v_refupd->>'timesheet_id');
    if v_source_updates_map ? v_source_edit_key then
      raise exception using errcode='22023',
        message='INVOICE_SOURCE_EDIT_DUPLICATE_TIMESHEET',
        detail=jsonb_build_object('timesheet_id',v_source_edit_key,'field','reference_updates')::text;
    end if;
    v_source_updates_map:=jsonb_set(
      v_source_updates_map,array[v_source_edit_key],
      jsonb_build_object(
        'timesheet_id',v_source_edit_key,
        'expected_document_revision',(v_refupd->>'expected_document_revision')::bigint,
        'has_reference_number',v_refupd ? 'reference_number',
        'reference_number',case when v_refupd ? 'reference_number'
          then to_jsonb(nullif(btrim(coalesce(v_refupd->>'reference_number','')),''))
          else 'null'::jsonb end,
        'has_day_references_json',v_refupd ? 'day_references_json',
        'day_references_json',case
          when not (v_refupd ? 'day_references_json') then 'null'::jsonb
          when jsonb_typeof(v_refupd->'day_references_json')='null' then 'null'::jsonb
          else v_refupd->'day_references_json' end,
        'has_actual_schedule_json',v_refupd ? 'actual_schedule_json',
        'actual_schedule_json',case
          when not (v_refupd ? 'actual_schedule_json') then 'null'::jsonb
          when jsonb_typeof(v_refupd->'actual_schedule_json')='null' then 'null'::jsonb
          else v_refupd->'actual_schedule_json' end,
        'has_hospital_norm',false,
        'has_ward_norm',false),
      true);
  end loop;
end if;

-- Hospital/ward edits merge into the same desired source row. A source may
-- appear once in each array, but both items must carry the same revision.
v_location_updates:=null;
if p_payload is not null and jsonb_typeof(p_payload)='object'
   and p_payload ? 'timesheet_location_updates' then
  v_location_updates:=p_payload->'timesheet_location_updates';
  if jsonb_typeof(v_location_updates)<>'array' then
    raise exception using errcode='22023',
      message='INVOICE_SOURCE_EDIT_PAYLOAD_INVALID',
      detail=jsonb_build_object('field','timesheet_location_updates','reason','ARRAY_REQUIRED')::text;
  end if;
  v_location_count:=jsonb_array_length(v_location_updates);
  for v_location_update in select value from jsonb_array_elements(v_location_updates) value loop
    if v_location_update is not null and jsonb_typeof(v_location_update)='object' then
      select min(field_name) into v_source_unsupported_field
      from jsonb_object_keys(v_location_update) field_name
      where not (field_name=any(v_allowed_location_fields));
      if v_source_unsupported_field is not null then
        raise exception using errcode='22023',
          message='INVOICE_SOURCE_EDIT_PAYLOAD_INVALID',
          detail=jsonb_build_object(
            'field','timesheet_location_updates','reason','UNSUPPORTED_FIELD',
            'unsupported_field',v_source_unsupported_field)::text;
      end if;
    end if;
    if v_location_update is null or jsonb_typeof(v_location_update)<>'object'
       or nullif(btrim(coalesce(v_location_update->>'timesheet_id','')),'') is null
       or coalesce(v_location_update->>'timesheet_id','') !~
          '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
       or coalesce(v_location_update->>'expected_document_revision','') !~ '^[1-9][0-9]*$'
       or not (v_location_update ? 'hospital_norm' or v_location_update ? 'ward_norm')
       or (
         v_location_update ? 'hospital_norm'
         and jsonb_typeof(v_location_update->'hospital_norm') not in('string','null')
       )
       or (
         v_location_update ? 'ward_norm'
         and jsonb_typeof(v_location_update->'ward_norm') not in('string','null')
       )
    then
      raise exception using errcode='22023',
        message='INVOICE_SOURCE_EDIT_PAYLOAD_INVALID',
        detail=jsonb_build_object('field','timesheet_location_updates','reason','INVALID_ITEM')::text;
    end if;
    v_source_edit_key:=lower(v_location_update->>'timesheet_id');
    v_source_expected_revision:=(v_location_update->>'expected_document_revision')::bigint;
    if v_source_updates_map ? v_source_edit_key
       and (
         coalesce((v_source_updates_map#>>
           array[v_source_edit_key,'has_hospital_norm'])::boolean,false)
         or coalesce((v_source_updates_map#>>
           array[v_source_edit_key,'has_ward_norm'])::boolean,false)
       ) then
      raise exception using errcode='22023',
        message='INVOICE_SOURCE_EDIT_DUPLICATE_TIMESHEET',
        detail=jsonb_build_object('timesheet_id',v_source_edit_key,'field','timesheet_location_updates')::text;
    end if;
    if v_source_updates_map ? v_source_edit_key
       and (v_source_updates_map#>>array[v_source_edit_key,'expected_document_revision'])::bigint
           is distinct from v_source_expected_revision then
      raise exception using errcode='40001',
        message='INVOICE_SOURCE_EDIT_STALE_REVISION',
        detail=jsonb_build_object('timesheet_id',v_source_edit_key,'reason','EXPECTED_REVISION_CONFLICT')::text;
    end if;
    v_source_update:=coalesce(v_source_updates_map->v_source_edit_key,
      jsonb_build_object(
        'timesheet_id',v_source_edit_key,
        'expected_document_revision',v_source_expected_revision,
        'has_reference_number',false,
        'has_day_references_json',false,
        'has_actual_schedule_json',false));
    v_source_update:=v_source_update||jsonb_build_object(
      'has_hospital_norm',v_location_update ? 'hospital_norm',
      'hospital_norm',case when v_location_update ? 'hospital_norm'
        then to_jsonb(lower(regexp_replace(
          btrim(coalesce(v_location_update->>'hospital_norm','')),
          '[[:space:]]+',' ','g')))
        else 'null'::jsonb end,
      'has_ward_norm',v_location_update ? 'ward_norm',
      'ward_norm',case when v_location_update ? 'ward_norm'
        then to_jsonb(lower(regexp_replace(
          btrim(coalesce(v_location_update->>'ward_norm','')),
          '[[:space:]]+',' ','g')))
        else 'null'::jsonb end);
    v_source_updates_map:=jsonb_set(v_source_updates_map,array[v_source_edit_key],v_source_update,true);
  end loop;
end if;
v_source_edit_requested:=v_source_updates_map<>'{}'::jsonb;
v_has_seg_ops :=
  (v_remove_seg_refs is not null and jsonb_typeof(v_remove_seg_refs)='array' and jsonb_array_length(v_remove_seg_refs) > 0)
  or (v_add_seg_refs is not null and jsonb_typeof(v_add_seg_refs)='array' and jsonb_array_length(v_add_seg_refs) > 0);

  v_refresh_hr_cache := (coalesce(array_length(v_add_ts_ids,1),0) > 0) or coalesce(v_has_seg_ops,false);

  if v_invoice_debug then
    v_dbg_stats := v_dbg_stats || jsonb_build_object(
      'remove_invoice_line_ids_count', coalesce(array_length(v_remove_ids,1),0),
      'add_timesheet_ids_count', coalesce(array_length(v_add_ts_ids,1),0),
      'add_adjustments_count', case when p_payload is not null and jsonb_typeof(p_payload)='object' and (p_payload ? 'add_adjustments') and jsonb_typeof(p_payload->'add_adjustments')='array' then jsonb_array_length(p_payload->'add_adjustments') else 0 end,
      'remove_segment_refs_count', case when v_remove_seg_refs is null then 0 else jsonb_array_length(coalesce(v_remove_seg_refs,'[]'::jsonb)) end,
      'add_segment_refs_count', case when v_add_seg_refs is null then 0 else jsonb_array_length(coalesce(v_add_seg_refs,'[]'::jsonb)) end,
      'timesheet_location_updates_count',v_location_count,
      'has_segment_ops', v_has_seg_ops
    );
    v_dbg_steps := v_dbg_steps || jsonb_build_array(jsonb_build_object('step','payload_parsed','stats',v_dbg_stats));
  end if;

  if v_source_edit_requested then
    -- Source edits are a narrower authority than ordinary draft edits.
    if v_inv.issued_at_utc is not null then
      raise exception using errcode='55000',
        message='INVOICE_SOURCE_EDIT_ISSUED';
    end if;
    if v_inv.paid_at_utc is not null then
      raise exception using errcode='55000',
        message='INVOICE_SOURCE_EDIT_PAID';
    end if;
    if v_inv.active_issue_operation_id is not null
       or upper(coalesce(v_inv.issue_state,'')) in
          ('VALIDATING','PREPARING_DOCUMENT','READY_TO_FINALISE')
       or exists(
         select 1
         from public.invoice_operation_chunks c
         where c.chunk_type='ISSUE_INVOICE'
           and c.entity_type='INVOICE'
           and c.entity_id=p_invoice_id
           and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
       )
    then
      raise exception using errcode='55000',
        message='INVOICE_SOURCE_EDIT_ISSUE_IN_PROGRESS';
    end if;

    v_pre_edit_invoice_revision:=v_inv.document_revision;
    v_pre_edit_preview_document_version_id:=v_inv.preview_document_version_id;
    v_pre_edit_active_document_operation_id:=v_inv.active_document_operation_id;

    -- Only an exact current-revision V8 preview is authoritative. Legacy PDF
    -- summary fields do not independently force or suppress replacement.
    select exists(
      select 1
      from public.invoice_document_versions dv
      left join public.invoice_operations op on op.id=dv.operation_id
      where dv.entity_type='INVOICE'
        and dv.entity_id=p_invoice_id
        and dv.purpose='DRAFT_PREVIEW'
        and dv.source_revision=v_pre_edit_invoice_revision::text
        and (
          (dv.status='READY'
            and nullif(btrim(coalesce(dv.r2_key,'')),'') is not null
            and nullif(btrim(coalesce(dv.sha256,'')),'') is not null
            and coalesce(dv.size_bytes,0)>0
            and coalesce(dv.page_count,0)>0)
          or (
            dv.id=v_pre_edit_preview_document_version_id
            and dv.status in(
              'PLANNING','WAITING_FOR_INPUTS','RENDERING',
              'ASSEMBLING','VERIFYING','READY'))
          or (
            dv.operation_id=v_pre_edit_active_document_operation_id
            and op.operation_type='BUILD_DOCUMENT'
            and op.entity_type='INVOICE'
            and op.entity_id=p_invoice_id
            and op.source_revision=v_pre_edit_invoice_revision::text
            and coalesce(op.input_json->>'purpose','')='DRAFT_PREVIEW'
            and op.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED'))
        )
    ) into v_source_edit_preexisting_preview;

    -- Clear any caller-controlled or earlier transaction-local value before
    -- establishing this function's exact-once invalidation marker.
    perform set_config('cloudtms.invoice_source_edit_marker','{}',true);

    -- Lock every target and its current financial snapshot in deterministic
    -- order before validating ownership, revisions or segment identities.
    for v_ref_ts in
      select t.*
      from public.timesheets t
      join jsonb_each(v_source_updates_map) desired on true
      where t.timesheet_id=(desired.value->>'timesheet_id')::uuid
      order by t.timesheet_id
      for update of t
    loop
      null;
    end loop;
    for v_ref_ts in
      select tf.id
      from public.timesheets_financials tf
      join jsonb_each(v_source_updates_map) desired
        on tf.timesheet_id=(desired.value->>'timesheet_id')::uuid
      where tf.is_current
      order by tf.timesheet_id,tf.id
      for update of tf
    loop
      null;
    end loop;

    if (
      select count(*)
      from public.timesheets t
      join jsonb_each(v_source_updates_map) desired
        on t.timesheet_id=(desired.value->>'timesheet_id')::uuid
      where t.is_current
    )<>(select count(*) from jsonb_each(v_source_updates_map)) then
      raise exception using errcode='55000',
        message='INVOICE_SOURCE_EDIT_SOURCE_NOT_CURRENT';
    end if;

    if exists(
      select 1
      from jsonb_each(v_source_updates_map) desired
      join public.timesheets t
        on t.timesheet_id=(desired.value->>'timesheet_id')::uuid
      where t.document_revision is distinct from
        (desired.value->>'expected_document_revision')::bigint
    ) then
      raise exception using errcode='40001',
        message='INVOICE_SOURCE_EDIT_STALE_REVISION';
    end if;

    if exists(
      select 1
      from jsonb_each(v_source_updates_map) desired
      where not exists(
        select 1
        from public.invoice_lines owned_line
        where owned_line.invoice_id=p_invoice_id
          and (
            owned_line.timesheet_id=(desired.value->>'timesheet_id')::uuid
            or (
              owned_line.timesheet_id is null
              and coalesce(owned_line.meta_json->>'timesheet_id','') ~
                '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
              and (owned_line.meta_json->>'timesheet_id')::uuid=
                  (desired.value->>'timesheet_id')::uuid
            )
          )
      )
    ) then
      raise exception using errcode='55000',
        message='INVOICE_SOURCE_EDIT_SOURCE_NOT_OWNED';
    end if;

    -- Preserve the established import-authoritative correction lock across
    -- both supported source carriers.
    if exists(
      select 1
      from jsonb_each(v_source_updates_map) desired
      where coalesce((
        public._ctms_import_correction_classify_v1(
          (desired.value->>'timesheet_id')::uuid)
          ->>'is_import_authoritative_correction')::boolean,false)
    ) then
      raise exception using errcode='55000',
        message='IMPORT_AUTHORITATIVE_CORRECTION_SOURCE_EDIT_FORBIDDEN';
    end if;

    -- Reject a mixed command that edits a source while removing every carrier
    -- for that source in the same Save.
    if coalesce(array_length(v_remove_ids,1),0)>0 and exists(
      select 1
      from jsonb_each(v_source_updates_map) desired
      where exists(
        select 1
        from public.invoice_lines carrier
        where carrier.invoice_id=p_invoice_id
          and (
            carrier.timesheet_id=(desired.value->>'timesheet_id')::uuid
            or (
              carrier.timesheet_id is null
              and coalesce(carrier.meta_json->>'timesheet_id','') ~
                '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
              and (carrier.meta_json->>'timesheet_id')::uuid=
                  (desired.value->>'timesheet_id')::uuid
            )
          )
      )
      and not exists(
        select 1
        from public.invoice_lines retained
        where retained.invoice_id=p_invoice_id
          and not (retained.id=any(v_remove_ids))
          and (
            retained.timesheet_id=(desired.value->>'timesheet_id')::uuid
            or (
              retained.timesheet_id is null
              and coalesce(retained.meta_json->>'timesheet_id','') ~
                '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
              and (retained.meta_json->>'timesheet_id')::uuid=
                  (desired.value->>'timesheet_id')::uuid
            )
          )
      )
    ) then
      raise exception using errcode='22023',
        message='INVOICE_SOURCE_EDIT_CONFLICTING_COMMAND';
    end if;

    -- Classify each source from the locked current financial snapshot. A
    -- SEGMENTS payload is an editable reference projection only: it can never
    -- replace timesheets.actual_schedule_json or any financial segment object.
    for v_ref_ts in
      select t.*,desired.value desired_value,
        tf.id financial_id,tf.invoice_breakdown_json financial_breakdown
      from public.timesheets t
      join jsonb_each(v_source_updates_map) desired
        on t.timesheet_id=(desired.value->>'timesheet_id')::uuid
      left join lateral(
        select f.id,f.invoice_breakdown_json
        from public.timesheets_financials f
        where f.timesheet_id=t.timesheet_id and f.is_current
        order by f.updated_at desc nulls last,f.created_at desc nulls last,f.id desc
        limit 1
      ) tf on true
      where t.is_current
      order by t.timesheet_id
    loop
      v_source_update:=v_ref_ts.desired_value;
      v_source_financial_id:=v_ref_ts.financial_id;
      v_source_ib:=v_ref_ts.financial_breakdown;
      v_source_mode:=upper(coalesce(v_source_ib->>'mode',''));
      v_source_payload_schedule:=v_source_update->'actual_schedule_json';
      v_source_canonical_schedule:=v_ref_ts.actual_schedule_json;
      v_source_new_segments:=null;
      v_source_segment_updates:='[]'::jsonb;
      v_source_segment_reference_changed:=false;

      if (v_source_update->>'has_actual_schedule_json')::boolean then
        if jsonb_typeof(v_source_payload_schedule)<>'array' then
          raise exception using errcode='22023',
            message='INVOICE_SOURCE_EDIT_PAYLOAD_INVALID',
            detail=jsonb_build_object(
              'field','actual_schedule_json','reason','ARRAY_REQUIRED',
              'timesheet_id',v_ref_ts.timesheet_id)::text;
        end if;

        if v_source_mode='SEGMENTS' then
          if v_source_financial_id is null
             or jsonb_typeof(v_source_ib->'segments')<>'array' then
            raise exception using errcode='55000',
              message='INVOICE_SOURCE_EDIT_SOURCE_NOT_CURRENT';
          end if;

          v_ref_seen_keys:='{}'::jsonb;
          for v_source_payload_segment,v_source_payload_ord in
            select value,ord
            from jsonb_array_elements(v_source_payload_schedule)
              with ordinality payload(value,ord)
          loop
            if jsonb_typeof(v_source_payload_segment)<>'object' then
              raise exception using errcode='22023',
                message='INVOICE_SOURCE_EDIT_PAYLOAD_INVALID',
                detail=jsonb_build_object(
                  'field','actual_schedule_json','reason','INVALID_SEGMENT',
                  'ordinal',v_source_payload_ord)::text;
            end if;
            select min(field_name) into v_source_unsupported_field
            from jsonb_object_keys(v_source_payload_segment) field_name
            where not (field_name=any(v_allowed_reference_segment_fields));
            if v_source_unsupported_field is not null then
              raise exception using errcode='22023',
                message='INVOICE_SOURCE_EDIT_PAYLOAD_INVALID',
                detail=jsonb_build_object(
                  'field','actual_schedule_json','reason','UNSUPPORTED_FIELD',
                  'unsupported_field',v_source_unsupported_field,
                  'ordinal',v_source_payload_ord)::text;
            end if;
            if not (v_source_payload_segment ? 'ref_num') then
              raise exception using errcode='22023',
                message='INVOICE_SOURCE_EDIT_PAYLOAD_INVALID',
                detail=jsonb_build_object(
                  'field','actual_schedule_json','reason','REFERENCE_REQUIRED',
                  'ordinal',v_source_payload_ord)::text;
            end if;

            v_ref_seg_id:=nullif(btrim(coalesce(
              v_source_payload_segment->>'segment_id','')),'');
            v_source_match_count:=0;
            v_source_match_segment:=null;
            if v_ref_seg_id is not null then
              select count(*),min(seg.value::text)::jsonb
              into v_source_match_count,v_source_match_segment
              from jsonb_array_elements(v_source_ib->'segments') seg(value)
              where nullif(btrim(coalesce(seg.value->>'segment_id','')),'')=v_ref_seg_id
                and nullif(btrim(coalesce(
                  seg.value->>'invoice_locked_invoice_id','')),'')=p_invoice_id::text;
            end if;

            if v_source_match_count=0 then
              begin
                v_source_start:=nullif(btrim(coalesce(
                  v_source_payload_segment->>'start_utc',
                  v_source_payload_segment->>'start','')),'')::timestamptz;
                v_source_end:=nullif(btrim(coalesce(
                  v_source_payload_segment->>'end_utc',
                  v_source_payload_segment->>'end','')),'')::timestamptz;
              exception when others then
                raise exception using errcode='22023',
                  message='INVOICE_SOURCE_EDIT_PAYLOAD_INVALID',
                  detail=jsonb_build_object(
                    'field','actual_schedule_json','reason','INVALID_SEGMENT_TIME',
                    'ordinal',v_source_payload_ord)::text;
              end;
              if v_source_start is null or v_source_end is null then
                raise exception using errcode='22023',
                  message='INVOICE_SOURCE_EDIT_PAYLOAD_INVALID',
                  detail=jsonb_build_object(
                    'field','actual_schedule_json','reason','SEGMENT_IDENTITY_REQUIRED',
                    'ordinal',v_source_payload_ord)::text;
              end if;
              select count(*),min(seg.value::text)::jsonb
              into v_source_match_count,v_source_match_segment
              from jsonb_array_elements(v_source_ib->'segments') seg(value)
              where nullif(btrim(coalesce(
                    seg.value->>'invoice_locked_invoice_id','')),'')=p_invoice_id::text
                and nullif(btrim(coalesce(
                    seg.value->>'start_utc',seg.value->>'start','')),'')::timestamptz
                    =v_source_start
                and nullif(btrim(coalesce(
                    seg.value->>'end_utc',seg.value->>'end','')),'')::timestamptz
                    =v_source_end;
            end if;

            if v_source_match_count<>1 or v_source_match_segment is null then
              raise exception using errcode='22023',
                message='INVOICE_SOURCE_EDIT_SEGMENT_REFERENCE_AMBIGUOUS',
                detail=jsonb_build_object(
                  'timesheet_id',v_ref_ts.timesheet_id,
                  'ordinal',v_source_payload_ord,
                  'match_count',v_source_match_count)::text;
            end if;

            v_ref_sched_key:=coalesce(
              'SID:'||nullif(btrim(coalesce(
                v_source_match_segment->>'segment_id','')),''),
              'SE:'||coalesce(v_source_match_segment->>'start_utc',
                v_source_match_segment->>'start','')||'|'||
                coalesce(v_source_match_segment->>'end_utc',
                  v_source_match_segment->>'end',''));
            if v_ref_seen_keys ? v_ref_sched_key then
              raise exception using errcode='22023',
                message='INVOICE_SOURCE_EDIT_SEGMENT_REFERENCE_AMBIGUOUS',
                detail=jsonb_build_object(
                  'timesheet_id',v_ref_ts.timesheet_id,
                  'reason','DUPLICATE_IDENTITY')::text;
            end if;
            v_ref_seen_keys:=jsonb_set(
              v_ref_seen_keys,array[v_ref_sched_key],'true'::jsonb,true);
            v_ref_seg_new_ref:=nullif(btrim(coalesce(
              v_source_payload_segment->>'ref_num','')),'');
            v_source_segment_reference_changed:=
              v_source_segment_reference_changed or (
                nullif(btrim(coalesce(
                  v_source_match_segment->>'ref_num','')),'')
                is distinct from v_ref_seg_new_ref);
            v_source_segment_updates:=v_source_segment_updates||
              jsonb_build_array(jsonb_build_object(
                'segment_id',nullif(btrim(coalesce(
                  v_source_match_segment->>'segment_id','')),''),
                'start_identity',coalesce(v_source_match_segment->>'start_utc',
                  v_source_match_segment->>'start'),
                'end_identity',coalesce(v_source_match_segment->>'end_utc',
                  v_source_match_segment->>'end'),
                'ref_num',v_ref_seg_new_ref));
          end loop;

          v_source_new_segments:='[]'::jsonb;
          for v_source_current_segment in
            select value
            from jsonb_array_elements(v_source_ib->'segments')
              with ordinality current_segment(value,ord)
            order by ord
          loop
            v_source_payload_segment:=null;
            select update_item.value into v_source_payload_segment
            from jsonb_array_elements(v_source_segment_updates) update_item(value)
            where (
              nullif(update_item.value->>'segment_id','') is not null
              and nullif(btrim(coalesce(
                v_source_current_segment->>'segment_id','')),'')
                  =nullif(update_item.value->>'segment_id','')
            ) or (
              nullif(update_item.value->>'segment_id','') is null
              and coalesce(v_source_current_segment->>'start_utc',
                    v_source_current_segment->>'start')
                  is not distinct from update_item.value->>'start_identity'
              and coalesce(v_source_current_segment->>'end_utc',
                    v_source_current_segment->>'end')
                  is not distinct from update_item.value->>'end_identity'
            )
            limit 1;
            if v_source_payload_segment is not null then
              v_source_current_segment:=jsonb_set(
                v_source_current_segment,'{ref_num}',
                coalesce(v_source_payload_segment->'ref_num','null'::jsonb),true);
            end if;
            v_source_new_segments:=v_source_new_segments||
              jsonb_build_array(v_source_current_segment);
          end loop;
        else
          v_source_canonical_schedule:=coalesce(
            v_ref_ts.actual_schedule_json,'[]'::jsonb);
          if jsonb_typeof(v_source_canonical_schedule)<>'array'
             or jsonb_array_length(v_source_payload_schedule)
                <>jsonb_array_length(v_source_canonical_schedule) then
            raise exception using errcode='22023',
              message='INVOICE_SOURCE_EDIT_PAYLOAD_INVALID',
              detail=jsonb_build_object(
                'field','actual_schedule_json','reason','STRUCTURE_MISMATCH',
                'timesheet_id',v_ref_ts.timesheet_id)::text;
          end if;
          v_ref_new_segments:='[]'::jsonb;
          for v_source_payload_segment,v_source_current_segment,v_source_payload_ord in
            select p.value,c.value,p.ord
            from jsonb_array_elements(v_source_payload_schedule)
              with ordinality p(value,ord)
            join jsonb_array_elements(v_source_canonical_schedule)
              with ordinality c(value,ord) using(ord)
            order by p.ord
          loop
            if jsonb_typeof(v_source_payload_segment)<>'object'
               or jsonb_typeof(v_source_current_segment)<>'object'
               or not (v_source_payload_segment ? 'ref_num')
               or (v_source_payload_segment-'ref_num') is distinct from
                  (v_source_current_segment-'ref_num') then
              raise exception using errcode='22023',
                message='INVOICE_SOURCE_EDIT_PAYLOAD_INVALID',
                detail=jsonb_build_object(
                  'field','actual_schedule_json','reason','STRUCTURE_MISMATCH',
                  'ordinal',v_source_payload_ord)::text;
            end if;
            v_ref_new_segments:=v_ref_new_segments||jsonb_build_array(
              jsonb_set(v_source_current_segment,'{ref_num}',
                coalesce(v_source_payload_segment->'ref_num','null'::jsonb),true));
          end loop;
          v_source_canonical_schedule:=v_ref_new_segments;
        end if;
      end if;

      v_source_timesheet_row_changed:=(
        ((v_source_update->>'has_reference_number')::boolean
          and v_ref_ts.reference_number is distinct from
            nullif(btrim(coalesce(v_source_update->>'reference_number','')),''))
        or ((v_source_update->>'has_day_references_json')::boolean
          and v_ref_ts.day_references_json is distinct from
            case when jsonb_typeof(v_source_update->'day_references_json')='null'
              then null else v_source_update->'day_references_json' end)
        or ((v_source_update->>'has_actual_schedule_json')::boolean
          and v_source_mode<>'SEGMENTS'
          and v_ref_ts.actual_schedule_json is distinct from
            v_source_canonical_schedule)
        or ((v_source_update->>'has_hospital_norm')::boolean
          and lower(regexp_replace(
                btrim(coalesce(v_ref_ts.hospital_norm,'')),
                '[[:space:]]+',' ','g')) is distinct from
            coalesce(v_source_update->>'hospital_norm',''))
        or ((v_source_update->>'has_ward_norm')::boolean
          and lower(regexp_replace(
                btrim(coalesce(v_ref_ts.ward_norm,'')),
                '[[:space:]]+',' ','g')) is distinct from
            coalesce(v_source_update->>'ward_norm',''))
      );
      v_source_update:=v_source_update||jsonb_build_object(
        'source_mode',v_source_mode,
        'financial_id',v_source_financial_id,
        'canonical_actual_schedule_json',v_source_canonical_schedule,
        'new_financial_segments',v_source_new_segments,
        'segment_updates',v_source_segment_updates,
        'segment_reference_changed',v_source_segment_reference_changed,
        'timesheet_row_changed',v_source_timesheet_row_changed);
      v_source_updates_map:=jsonb_set(
        v_source_updates_map,array[v_ref_ts.timesheet_id::text],
        v_source_update,true);
    end loop;

    select coalesce(array_agg(
      (desired.value->>'timesheet_id')::uuid order by desired.key),
      array[]::uuid[])
    into v_source_changed_ts_ids
    from jsonb_each(v_source_updates_map) desired
    where (desired.value->>'timesheet_row_changed')::boolean
       or (desired.value->>'segment_reference_changed')::boolean;

    select coalesce(array_agg(
      (desired.value->>'timesheet_id')::uuid order by desired.key),
      array[]::uuid[])
    into v_refupd_ts_ids
    from jsonb_each(v_source_updates_map) desired
    where (desired.value->>'timesheet_id')::uuid=any(v_source_changed_ts_ids)
      and (
        (desired.value->>'has_reference_number')::boolean
        or (desired.value->>'has_day_references_json')::boolean
        or (
          (desired.value->>'has_actual_schedule_json')::boolean
          and (
            (desired.value->>'timesheet_row_changed')::boolean
            or (desired.value->>'segment_reference_changed')::boolean
          )
        )
      );

    select coalesce(array_agg(
      (desired.value->>'timesheet_id')::uuid order by desired.key),
      array[]::uuid[])
    into v_location_ts_ids
    from jsonb_each(v_source_updates_map) desired
    where (desired.value->>'timesheet_row_changed')::boolean
      and (
        (desired.value->>'has_hospital_norm')::boolean
        or (desired.value->>'has_ward_norm')::boolean
      );

    if exists(
      select 1 from jsonb_each(v_source_updates_map) desired
      where (desired.value->>'timesheet_row_changed')::boolean
    ) then
      with desired as materialized(
        select (value->>'timesheet_id')::uuid as timesheet_id,value
        from jsonb_each(v_source_updates_map)
      )
      update public.timesheets t
      set updated_at=v_now,
          reference_number=case when (d.value->>'has_reference_number')::boolean
            then nullif(btrim(coalesce(d.value->>'reference_number','')),'')
            else t.reference_number end,
          day_references_json=case when (d.value->>'has_day_references_json')::boolean
            then case when jsonb_typeof(d.value->'day_references_json')='null'
              then null else d.value->'day_references_json' end
            else t.day_references_json end,
          actual_schedule_json=case
            when (d.value->>'has_actual_schedule_json')::boolean
              and d.value->>'source_mode'<>'SEGMENTS'
            then d.value->'canonical_actual_schedule_json'
            else t.actual_schedule_json end,
          hospital_norm=case when (d.value->>'has_hospital_norm')::boolean
            then coalesce(d.value->>'hospital_norm','') else t.hospital_norm end,
          ward_norm=case when (d.value->>'has_ward_norm')::boolean
            then coalesce(d.value->>'ward_norm','') else t.ward_norm end
      from desired d
      where t.timesheet_id=d.timesheet_id
        and (d.value->>'timesheet_row_changed')::boolean
        and t.is_current;
    end if;

    v_refupd_applied:=cardinality(v_refupd_ts_ids);
    v_location_applied:=cardinality(v_location_ts_ids);

    -- Marker rows exist only where the timesheet statement has already advanced
    -- the source revision and the following SEGMENTS ref update would otherwise
    -- invalidate the same source a second time.
    select coalesce(jsonb_object_agg(
      t.timesheet_id::text,
      jsonb_build_object('expected_revision',t.document_revision)),
      '{}'::jsonb)
    into v_source_marker_rows
    from jsonb_each(v_source_updates_map) desired
    join public.timesheets t
      on t.timesheet_id=(desired.value->>'timesheet_id')::uuid and t.is_current
    where (desired.value->>'timesheet_row_changed')::boolean
      and (desired.value->>'segment_reference_changed')::boolean
      and t.document_revision=
        (desired.value->>'expected_document_revision')::bigint+1;
    if (select count(*) from jsonb_each(v_source_marker_rows))<>(
      select count(*) from jsonb_each(v_source_updates_map) desired
      where (desired.value->>'timesheet_row_changed')::boolean
        and (desired.value->>'segment_reference_changed')::boolean
    ) then
      raise exception using errcode='55000',
        message='SOURCE_EDIT_REPLACEMENT_RESULT_INVALID',
        detail=jsonb_build_object(
          'reason','SOURCE_REVISION_ADVANCE_NOT_EXACT')::text;
    end if;
    v_source_marker:=jsonb_build_object(
      'txid',txid_current()::text,'rows',v_source_marker_rows);
    perform set_config(
      'cloudtms.invoice_source_edit_marker',v_source_marker::text,true);

    -- The legacy per-array loops below remain structurally compatible for old
    -- payload code, but the strict merged statement above is now the sole
    -- execution path for accepted source edits.
    v_reference_updates:=null;
    v_location_updates:=null;
  end if;





  -- 0) Apply reference updates to timesheets (does NOT recompute TSFIN; it updates the source timesheet refs)
  if v_reference_updates is not null and jsonb_typeof(v_reference_updates)='array' and jsonb_array_length(v_reference_updates) > 0 then
    for v_refupd in
      select value from jsonb_array_elements(v_reference_updates) value
    loop
      if v_refupd is null or jsonb_typeof(v_refupd) <> 'object' then
        continue;
      end if;

      if nullif(btrim(coalesce(v_refupd->>'timesheet_id','')), '') is null then
        continue;
      end if;

      v_refupd_ts_id := (v_refupd->>'timesheet_id')::uuid;

      v_refupd_set_refnum := (v_refupd ? 'reference_number');
      v_refupd_set_dayrefs := (v_refupd ? 'day_references_json');
      v_refupd_set_sched := (v_refupd ? 'actual_schedule_json');

      v_refupd_refnum := null;
      if v_refupd_set_refnum then
        v_refupd_refnum := nullif(btrim(coalesce(v_refupd->>'reference_number','')), '');
      end if;

      v_refupd_dayrefs := null;
      if v_refupd_set_dayrefs then
        v_refupd_dayrefs := v_refupd->'day_references_json';
        if v_refupd_dayrefs is not null and jsonb_typeof(v_refupd_dayrefs) = 'null' then
          v_refupd_dayrefs := null;
        end if;
      end if;

      v_refupd_sched := null;
      if v_refupd_set_sched then
        v_refupd_sched := v_refupd->'actual_schedule_json';
        if v_refupd_sched is not null and jsonb_typeof(v_refupd_sched) = 'null' then
          v_refupd_sched := null;
        end if;
      end if;

      update public.timesheets tsu
      set
        updated_at = v_now,
        reference_number = case when v_refupd_set_refnum then v_refupd_refnum else tsu.reference_number end,
        day_references_json = case when v_refupd_set_dayrefs then v_refupd_dayrefs else tsu.day_references_json end,
        actual_schedule_json = case when v_refupd_set_sched then v_refupd_sched else tsu.actual_schedule_json end
      where tsu.timesheet_id = v_refupd_ts_id
        and tsu.is_current = true
        and exists(
          select 1
          from public.invoice_lines owned_line
          where owned_line.invoice_id=p_invoice_id
            and (
              owned_line.timesheet_id=tsu.timesheet_id
              or (
                owned_line.timesheet_id is null
                and coalesce(owned_line.meta_json->>'timesheet_id','')
                  ~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
                and (owned_line.meta_json->>'timesheet_id')::uuid=tsu.timesheet_id
              )
            )
        )
        and (
          (v_refupd_set_refnum and tsu.reference_number is distinct from v_refupd_refnum)
          or (v_refupd_set_dayrefs and tsu.day_references_json is distinct from v_refupd_dayrefs)
          or (v_refupd_set_sched and tsu.actual_schedule_json is distinct from v_refupd_sched)
        );

      get diagnostics v_rc = row_count;
      if coalesce(v_rc,0) > 0 then
        v_refupd_applied := v_refupd_applied + 1;
        v_refupd_ts_ids := v_refupd_ts_ids || v_refupd_ts_id;
      end if;

    end loop;

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object('step','reference_updates_applied','count_requested',v_refupd_count,'count_applied',v_refupd_applied)
      );
    end if;
  end if;

  -- 0a) Apply only material hospital/ward changes to current timesheets that
  -- are owned by this invoice. Identical canonical values perform no UPDATE,
  -- so they cannot dirty documents or queue replacement work.
  if v_location_updates is not null
     and jsonb_typeof(v_location_updates)='array'
     and jsonb_array_length(v_location_updates)>0 then
    for v_location_update in
      select value from jsonb_array_elements(v_location_updates) value
    loop
      if v_location_update is null
         or jsonb_typeof(v_location_update)<>'object'
         or nullif(btrim(coalesce(v_location_update->>'timesheet_id','')),'') is null then
        continue;
      end if;

      v_location_ts_id := (v_location_update->>'timesheet_id')::uuid;
      v_location_set_hospital := v_location_update ? 'hospital_norm';
      v_location_set_ward := v_location_update ? 'ward_norm';
      if not v_location_set_hospital and not v_location_set_ward then
        continue;
      end if;

      v_location_hospital_norm := null;
      if v_location_set_hospital then
        v_location_hospital_norm := nullif(lower(regexp_replace(
          btrim(coalesce(v_location_update->>'hospital_norm','')),
          '[[:space:]]+',' ','g')), '');
      end if;
      v_location_ward_norm := null;
      if v_location_set_ward then
        v_location_ward_norm := nullif(lower(regexp_replace(
          btrim(coalesce(v_location_update->>'ward_norm','')),
          '[[:space:]]+',' ','g')), '');
      end if;

      update public.timesheets tsu
      set updated_at=v_now,
          hospital_norm=case when v_location_set_hospital
            then v_location_hospital_norm else tsu.hospital_norm end,
          ward_norm=case when v_location_set_ward
            then v_location_ward_norm else tsu.ward_norm end
      where tsu.timesheet_id=v_location_ts_id
        and tsu.is_current=true
        and exists(
          select 1
          from public.invoice_lines owned_line
          where owned_line.invoice_id=p_invoice_id
            and (
              owned_line.timesheet_id=tsu.timesheet_id
              or (
                owned_line.timesheet_id is null
                and coalesce(owned_line.meta_json->>'timesheet_id','')
                  ~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
                and (owned_line.meta_json->>'timesheet_id')::uuid=tsu.timesheet_id
              )
            )
        )
        and (
          (v_location_set_hospital
            and tsu.hospital_norm is distinct from v_location_hospital_norm)
          or (v_location_set_ward
            and tsu.ward_norm is distinct from v_location_ward_norm)
        );

      get diagnostics v_rc=row_count;
      if coalesce(v_rc,0)>0 then
        v_location_applied:=v_location_applied+1;
        v_location_ts_ids:=v_location_ts_ids||v_location_ts_id;
      end if;
    end loop;

    if v_invoice_debug then
      v_dbg_steps:=v_dbg_steps||jsonb_build_array(jsonb_build_object(
        'step','timesheet_location_updates_applied',
        'count_requested',v_location_count,
        'count_applied',v_location_applied));
    end if;
  end if;
  -- 0b) After reference updates: refresh invoice_lines meta (ts_reference_number / schedule_ref_nums) and
  -- synchronise only the approved ref_num fields into the locked SEGMENTS
  -- financial projection. The full financial objects and their order are
  -- preserved from the locked current snapshot.
  if v_refupd_ts_ids is not null and coalesce(array_length(v_refupd_ts_ids,1),0) > 0 then

    with desired as materialized(
      select value
      from jsonb_each(v_source_updates_map)
      where (value->>'segment_reference_changed')::boolean
    )
    update public.timesheets_financials tf
    set invoice_breakdown_json=jsonb_set(
      coalesce(tf.invoice_breakdown_json,'{}'::jsonb),
      '{segments}',d.value->'new_financial_segments',true)
    from desired d
    where tf.id=(d.value->>'financial_id')::uuid
      and tf.is_current
      and tf.timesheet_id=(d.value->>'timesheet_id')::uuid
      and upper(coalesce(tf.invoice_breakdown_json->>'mode',''))='SEGMENTS'
      and tf.invoice_breakdown_json->'segments'
        is distinct from d.value->'new_financial_segments';
    get diagnostics v_refupd_tsfin_rows_updated=row_count;
    select coalesce(sum(jsonb_array_length(
      coalesce(value->'segment_updates','[]'::jsonb))),0)::integer
    into v_refupd_tsfin_segments_updated
    from jsonb_each(v_source_updates_map)
    where (value->>'segment_reference_changed')::boolean;

    -- The marker has served its single synchronisation statement. Clear it so
    -- no later statement in this transaction can inherit suppression authority.
    perform set_config('cloudtms.invoice_source_edit_marker','{}',true);

    select array_agg(distinct x)
    into v_refupd_ts_ids_distinct
    from unnest(v_refupd_ts_ids) x
    where x is not null;

    v_refupd_ts_ids_distinct := coalesce(v_refupd_ts_ids_distinct, array[]::uuid[]);

    foreach v_ref_ts_id in array v_refupd_ts_ids_distinct loop
      if v_ref_ts_id is null then
        continue;
      end if;

      select tsu.*
      into v_ref_ts
      from public.timesheets tsu
      where tsu.timesheet_id = v_ref_ts_id
        and tsu.is_current = true
      limit 1;

      if not found then
        continue;
      end if;
      v_source_update:=v_source_updates_map->v_ref_ts_id::text;
      v_source_mode:=upper(coalesce(v_source_update->>'source_mode',''));
      v_source_canonical_schedule:=case when v_source_mode='SEGMENTS'
        then coalesce(v_source_update->'new_financial_segments','[]'::jsonb)
        else coalesce(v_ref_ts.actual_schedule_json,'[]'::jsonb) end;

      -- Build the cache from the exact authority used by this invoice. SEGMENTS
      -- refs come from invoice-owned financial segments; other modes use the
      -- structurally preserved timesheet schedule.
      v_ref_schedule_refs := '[]'::jsonb;

      if nullif(btrim(coalesce(v_ref_ts.reference_number,'')), '') is not null then
        v_ref_schedule_refs := v_ref_schedule_refs || jsonb_build_array(to_jsonb(nullif(btrim(v_ref_ts.reference_number),'')));
      end if;

      if v_ref_ts.day_references_json is not null and jsonb_typeof(v_ref_ts.day_references_json) = 'object' then
        for kv in
          select key as k, value as v
          from jsonb_each_text(v_ref_ts.day_references_json)
        loop
          if nullif(btrim(coalesce(kv.v,'')), '') is not null then
            v_ref_schedule_refs := v_ref_schedule_refs || jsonb_build_array(to_jsonb(nullif(btrim(kv.v),'')));
          end if;
        end loop;
      end if;

      if jsonb_typeof(v_source_canonical_schedule)='array' then
        for v_ref_seg_obj in
          select value
          from jsonb_array_elements(v_source_canonical_schedule) value
        loop
          if v_ref_seg_obj is null or jsonb_typeof(v_ref_seg_obj) <> 'object' then
            continue;
          end if;
          v_ref_sched_ref := nullif(btrim(coalesce(v_ref_seg_obj->>'ref_num','')), '');
          if v_ref_sched_ref is not null then
            v_ref_schedule_refs := v_ref_schedule_refs || jsonb_build_array(to_jsonb(v_ref_sched_ref));
          end if;
        end loop;
      end if;

      select coalesce(jsonb_agg(to_jsonb(x) order by x), '[]'::jsonb)
      into v_ref_schedule_refs_distinct
      from (
        select distinct btrim(t.x) as x
        from jsonb_array_elements_text(coalesce(v_ref_schedule_refs,'[]'::jsonb)) as t(x)
        where nullif(btrim(coalesce(t.x,'')), '') is not null
      ) q;

      -- Refresh meta_json on all invoice lines for this timesheet
      update public.invoice_lines ilu
      set meta_json = coalesce(ilu.meta_json, '{}'::jsonb) || jsonb_build_object(
        'ts_reference_number', nullif(btrim(coalesce(v_ref_ts.reference_number,'')), ''),
        'schedule_ref_nums', coalesce(v_ref_schedule_refs_distinct, '[]'::jsonb),
        'schedule_ref_count', jsonb_array_length(coalesce(v_ref_schedule_refs_distinct, '[]'::jsonb))
      )
      where ilu.invoice_id = p_invoice_id
        and (
          ilu.timesheet_id=v_ref_ts_id
          or (
            ilu.timesheet_id is null
            and coalesce(ilu.meta_json->>'timesheet_id','') ~
              '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
            and (ilu.meta_json->>'timesheet_id')::uuid=v_ref_ts_id
          )
        )
        and (
          ilu.meta_json->'ts_reference_number' is distinct from
            coalesce(to_jsonb(nullif(btrim(coalesce(v_ref_ts.reference_number,'')),'')),'null'::jsonb)
          or ilu.meta_json->'schedule_ref_nums' is distinct from
            coalesce(v_ref_schedule_refs_distinct,'[]'::jsonb)
          or ilu.meta_json->'schedule_ref_count' is distinct from
            to_jsonb(jsonb_array_length(coalesce(v_ref_schedule_refs_distinct,'[]'::jsonb)))
        );

      get diagnostics v_rc = row_count;
      v_refupd_meta_rows_updated := v_refupd_meta_rows_updated + coalesce(v_rc,0);

      -- Best-effort sync: update tsfin.invoice_breakdown_json segments[].ref_num by matching start/end fields
      v_ref_seg_updates_this_ts := 0;
      v_ref_seg_matches_this_ts := 0;
      v_ref_tsfin_id := null;
      v_ref_ib := null;

        select tfu.id, tfu.invoice_breakdown_json
      into v_ref_tsfin_id, v_ref_ib
      from public.timesheets_financials tfu
      where tfu.timesheet_id = v_ref_ts_id
        and tfu.is_current = true
      limit 1
      for update;


      if v_ref_tsfin_id is not null
         and v_source_mode<>'SEGMENTS'
         and v_ref_ib is not null
         and jsonb_typeof(v_ref_ib) = 'object'
         and upper(coalesce(v_ref_ib->>'mode','')) = 'SEGMENTS'
         and jsonb_typeof(v_ref_ib->'segments') = 'array'
         and v_ref_ts.actual_schedule_json is not null
         and jsonb_typeof(v_ref_ts.actual_schedule_json) = 'array'
       then
         -- Build a map of segment_id (preferred) or start|end -> ref_num from actual_schedule_json
        v_ref_sched_map := '{}'::jsonb;
        for v_ref_seg_obj in
          select value
          from jsonb_array_elements(v_ref_ts.actual_schedule_json) value
        loop
          if v_ref_seg_obj is null or jsonb_typeof(v_ref_seg_obj) <> 'object' then
            continue;
          end if;

          v_ref_sched_ref := nullif(btrim(coalesce(v_ref_seg_obj->>'ref_num','')), '');
          v_ref_seg_id := nullif(btrim(coalesce(v_ref_seg_obj->>'segment_id','')), '');
          if v_ref_seg_id is not null then
            v_ref_sched_key := 'SID:' || v_ref_seg_id;
            if v_ref_sched_map ? v_ref_sched_key then
              raise exception using errcode='22023',
                message='INVOICE_SOURCE_EDIT_SEGMENT_REFERENCE_AMBIGUOUS',
                detail=jsonb_build_object(
                  'timesheet_id',v_ref_ts_id,'identity',v_ref_sched_key,
                  'side','TIMESHEET_SCHEDULE')::text;
            end if;
            v_ref_sched_map := jsonb_set(v_ref_sched_map, array[v_ref_sched_key], case when v_ref_sched_ref is null then 'null'::jsonb else to_jsonb(v_ref_sched_ref) end, true);
            continue;
          end if;

          v_ref_seg_start := nullif(btrim(coalesce(v_ref_seg_obj->>'start_utc', v_ref_seg_obj->>'start', '')), '');
          v_ref_seg_end := nullif(btrim(coalesce(v_ref_seg_obj->>'end_utc', v_ref_seg_obj->>'end', '')), '');
          if v_ref_seg_start is null or v_ref_seg_end is null then
            continue;
          end if;

          v_ref_sched_key := 'SE:' || v_ref_seg_start || '|' || v_ref_seg_end;
          if v_ref_sched_map ? v_ref_sched_key then
            raise exception using errcode='22023',
              message='INVOICE_SOURCE_EDIT_SEGMENT_REFERENCE_AMBIGUOUS',
              detail=jsonb_build_object(
                'timesheet_id',v_ref_ts_id,'identity',v_ref_sched_key,
                'side','TIMESHEET_SCHEDULE')::text;
          end if;
          v_ref_sched_map := jsonb_set(v_ref_sched_map, array[v_ref_sched_key], case when v_ref_sched_ref is null then 'null'::jsonb else to_jsonb(v_ref_sched_ref) end, true);
        end loop;

        v_ref_new_segments := '[]'::jsonb;
        v_ref_seen_keys := '{}'::jsonb;
        for v_ref_seg_obj in
          select value
          from jsonb_array_elements(v_ref_ib->'segments') value
        loop
          if v_ref_seg_obj is null or jsonb_typeof(v_ref_seg_obj) <> 'object' then
            v_ref_new_segments := v_ref_new_segments || jsonb_build_array(v_ref_seg_obj);
            continue;
          end if;

          v_ref_seg_new_ref := null;
          v_ref_seg_has_update := false;

          v_ref_seg_id := nullif(btrim(coalesce(v_ref_seg_obj->>'segment_id','')), '');
          if v_ref_seg_id is not null then
            v_ref_sched_key := 'SID:' || v_ref_seg_id;
            if v_ref_seen_keys ? v_ref_sched_key then
              raise exception using errcode='22023',
                message='INVOICE_SOURCE_EDIT_SEGMENT_REFERENCE_AMBIGUOUS',
                detail=jsonb_build_object(
                  'timesheet_id',v_ref_ts_id,'identity',v_ref_sched_key,
                  'side','FINANCIAL_SEGMENTS')::text;
            end if;
            v_ref_seen_keys:=jsonb_set(v_ref_seen_keys,array[v_ref_sched_key],'true'::jsonb,true);
            if v_ref_sched_map ? v_ref_sched_key then
              v_ref_seg_new_ref := nullif(btrim(coalesce(v_ref_sched_map->>v_ref_sched_key,'')), '');
              v_ref_seg_has_update := true;
              v_ref_seg_matches_this_ts := v_ref_seg_matches_this_ts + 1;
            end if;
          end if;

          if not v_ref_seg_has_update then
            v_ref_seg_start := nullif(btrim(coalesce(v_ref_seg_obj->>'start_utc', v_ref_seg_obj->>'start', '')), '');
            v_ref_seg_end := nullif(btrim(coalesce(v_ref_seg_obj->>'end_utc', v_ref_seg_obj->>'end', '')), '');

            if v_ref_seg_start is not null and v_ref_seg_end is not null then
              v_ref_sched_key := 'SE:' || v_ref_seg_start || '|' || v_ref_seg_end;
              if v_ref_seen_keys ? v_ref_sched_key then
                raise exception using errcode='22023',
                  message='INVOICE_SOURCE_EDIT_SEGMENT_REFERENCE_AMBIGUOUS',
                  detail=jsonb_build_object(
                    'timesheet_id',v_ref_ts_id,'identity',v_ref_sched_key,
                    'side','FINANCIAL_SEGMENTS')::text;
              end if;
              v_ref_seen_keys:=jsonb_set(v_ref_seen_keys,array[v_ref_sched_key],'true'::jsonb,true);
              if v_ref_sched_map ? v_ref_sched_key then
                v_ref_seg_new_ref := nullif(btrim(coalesce(v_ref_sched_map->>v_ref_sched_key,'')), '');
                v_ref_seg_has_update := true;
                v_ref_seg_matches_this_ts := v_ref_seg_matches_this_ts + 1;
              end if;
            end if;
          end if;

          if v_ref_seg_has_update then
            v_ref_seg_cur_ref := nullif(btrim(coalesce(v_ref_seg_obj->>'ref_num','')), '');
            if v_ref_seg_cur_ref is distinct from v_ref_seg_new_ref then
              v_ref_seg_obj := jsonb_set(
                v_ref_seg_obj,
                '{ref_num}',
                case when v_ref_seg_new_ref is null then 'null'::jsonb else to_jsonb(v_ref_seg_new_ref) end,
                true
              );
              v_ref_seg_updates_this_ts := v_ref_seg_updates_this_ts + 1;
            end if;
          end if;

          v_ref_new_segments := v_ref_new_segments || jsonb_build_array(v_ref_seg_obj);
        end loop;



        if v_ref_sched_map is not null
           and jsonb_typeof(v_ref_sched_map) = 'object'
           and v_ref_sched_map <> '{}'::jsonb
           and coalesce(jsonb_array_length(coalesce(v_ref_ib->'segments','[]'::jsonb)),0) > 0
           and coalesce(v_ref_seg_matches_this_ts,0) = 0
        then
          raise exception 'SEGMENTS reference sync failed: no segments matched schedule keys (timesheet_id=% tsfin_id=%)', v_ref_ts_id, v_ref_tsfin_id;
        end if;

        if v_ref_seg_updates_this_ts > 0 then
              update public.timesheets_financials tfu2
          set invoice_breakdown_json = jsonb_set(coalesce(tfu2.invoice_breakdown_json, '{}'::jsonb), '{segments}', v_ref_new_segments, true)
          where tfu2.id = v_ref_tsfin_id
            and tfu2.is_current = true;


          get diagnostics v_rc = row_count;
          if coalesce(v_rc,0) > 0 then
            v_refupd_tsfin_rows_updated := v_refupd_tsfin_rows_updated + 1;
            v_refupd_tsfin_segments_updated := v_refupd_tsfin_segments_updated + v_ref_seg_updates_this_ts;
          end if;
        end if;

      end if;

    end loop;

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','reference_updates_meta_refreshed',
          'timesheets_count', coalesce(array_length(v_refupd_ts_ids_distinct,1),0),
          'invoice_line_rows_updated', v_refupd_meta_rows_updated,
          'tsfin_rows_updated', v_refupd_tsfin_rows_updated,
          'tsfin_segments_refnum_updated', v_refupd_tsfin_segments_updated
        )
      );
    end if;
  end if;

  -- 1) Removals (by invoice_line_id)
  if v_remove_ids is not null and coalesce(array_length(v_remove_ids,1),0) > 0 then
    -- collect timesheet_ids touched (any)
    select array_agg(distinct l.timesheet_id) filter (where l.timesheet_id is not null)
    into v_ts_ids_touched
    from public.invoice_lines l
    where l.invoice_id = p_invoice_id
      and l.id = any(v_remove_ids);

    -- record removed lines for history
    v_hist_lines_removed := coalesce(p_payload->'remove_invoice_line_ids','[]'::jsonb);

    -- Only unlock TSFIN when HOURS lines were removed (prevents accidental unlock when deleting only expenses/other lines)
    -- IMPORTANT: compute BEFORE deletion because we match on the removed invoice_line ids.
    select array_agg(distinct l.timesheet_id) filter (where l.timesheet_id is not null)
    into v_cw_ts_ids
    from public.invoice_lines l
    where l.invoice_id = p_invoice_id
      and l.id = any(v_remove_ids)
      and upper(coalesce(l.meta_json->>'line_type','')) in ('HOURS_WEEKLY','HOURS_DAILY');

    v_cw_ts_ids := coalesce(v_cw_ts_ids, array[]::uuid[]);



    if coalesce(array_length(v_cw_ts_ids,1),0) > 0 then
      v_refresh_hr_cache := true;
    end if;
delete from public.invoice_lines
    where invoice_id = p_invoice_id
      and id = any(v_remove_ids);
    get diagnostics v_rc = row_count;
    v_dbg_lines_deleted := v_dbg_lines_deleted + coalesce(v_rc,0);

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','lines_removed',
          'rows_deleted', coalesce(v_rc,0),
          'timesheets_touched', coalesce(array_length(v_ts_ids_touched,1),0),
          'timesheets_to_unlock_count', coalesce(array_length(v_cw_ts_ids,1),0)
        )
      );
    end if;

    -- FIX: if a touched timesheet now has NO remaining invoice_lines on this invoice,
    -- unlock it even if the removed lines were expenses/mileage/additional (expense-only/SEGMENTS-empty case).
    v_ts_ids_fully_removed := array[]::uuid[];
    select array_agg(distinct x)
    into v_ts_ids_fully_removed
    from unnest(coalesce(v_ts_ids_touched, array[]::uuid[])) x
    where x is not null
      and not exists (
        select 1
        from public.invoice_lines l2
        where l2.invoice_id = p_invoice_id
          and l2.timesheet_id = x
      );

    v_ts_ids_fully_removed := coalesce(v_ts_ids_fully_removed, array[]::uuid[]);

    if coalesce(array_length(v_ts_ids_fully_removed,1),0) > 0 then
      -- unlock any segments locked to this invoice (safe no-op for SEGMENTS-empty)
      perform public._inv_unlock_segments_for_invoice(p_invoice_id, v_ts_ids_fully_removed);

      -- clear whole-timesheet lock if it was set for this invoice (SEGMENTS-empty / non-segments / pseudo segment_id null locks)
      update public.timesheets_financials tfu_lock
      set
        locked_by_invoice_id = null,
        locked_at_utc = null,
        updated_at = v_now
      where tfu_lock.is_current = true
        and tfu_lock.timesheet_id = any(v_ts_ids_fully_removed)
        and tfu_lock.locked_by_invoice_id = p_invoice_id;

      get diagnostics v_rc = row_count;
      v_dbg_timesheets_unlocked := v_dbg_timesheets_unlocked + coalesce(v_rc,0);

      v_refresh_hr_cache := true;

      if v_invoice_debug then
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object(
            'step','timesheets_unlocked_after_full_removal',
            'timesheets_fully_removed_count', coalesce(array_length(v_ts_ids_fully_removed,1),0),
            'tsfin_rows_unlocked_count', coalesce(v_rc,0)
          )
        );
      end if;
    end if;

    v_cw_ts_ids := coalesce(v_cw_ts_ids, array[]::uuid[]);

    if v_cw_ts_ids is not null and coalesce(array_length(v_cw_ts_ids,1),0) > 0 then
      perform public._inv_unlock_segments_for_invoice(p_invoice_id, v_cw_ts_ids);
      v_dbg_timesheets_unlocked := v_dbg_timesheets_unlocked + coalesce(array_length(v_cw_ts_ids,1),0);

      -- Cleanup: if no segments remain on THIS invoice for a touched timesheet, remove ALL remaining invoice lines for that timesheet
      foreach tsid in array v_cw_ts_ids loop
        if tsid is null then continue; end if;

        -- detect if any segments are still locked to THIS invoice
        select tf.*
        into snap
        from public.timesheets_financials tf
        where tf.is_current = true
          and tf.timesheet_id = tsid
        limit 1;

        if not found then
          continue;
        end if;

        segments := '[]'::jsonb;
        if snap.invoice_breakdown_json is not null
           and jsonb_typeof(snap.invoice_breakdown_json)='object'
           and coalesce(snap.invoice_breakdown_json->>'mode','')='SEGMENTS'
           and jsonb_typeof(snap.invoice_breakdown_json->'segments')='array'
        then
          for v_seg in
            select value from jsonb_array_elements(snap.invoice_breakdown_json->'segments') value
          loop
            if v_seg is null or jsonb_typeof(v_seg) <> 'object' then
              continue;
            end if;
            seg_locked := nullif(btrim(coalesce(v_seg->>'invoice_locked_invoice_id','')), '');
            if seg_locked = p_invoice_id::text then
              segments := segments || jsonb_build_array(v_seg);
            end if;
          end loop;

          if jsonb_array_length(coalesce(segments,'[]'::jsonb)) = 0 then
            delete from public.invoice_lines
            where invoice_id = p_invoice_id
              and timesheet_id = tsid;
          end if;
        else
          -- Non-segments: if snapshot is no longer locked to this invoice, remove all remaining invoice lines for this timesheet
          if snap.locked_by_invoice_id is null then
            delete from public.invoice_lines
            where invoice_id = p_invoice_id
              and timesheet_id = tsid;
          end if;
        end if;
      end loop;
    end if;

    -- History event (always)
    perform public._audit_insert(
      'invoices',
      p_invoice_id::text,
      'INVOICE_LINES_REMOVED',
      null,
      jsonb_build_object('remove_invoice_line_ids', v_hist_lines_removed, 'timesheet_ids_touched', coalesce(to_jsonb(v_ts_ids_touched), '[]'::jsonb)),
      null,
      p_actor_user_id
    );

  end if;


  -- 1b) Segment edits (SEGMENTS mode only)
-- NOTE: Segment moves are NOT allowed when additional rates OR expenses/mileage exist on the TSFIN snapshot.
-- Payload contract uses tsfin_id + segment_id.
if v_has_seg_ops then

  v_dbg_seg_add_refs := case when v_add_seg_refs is null then 0 else jsonb_array_length(coalesce(v_add_seg_refs,'[]'::jsonb)) end;
  v_dbg_seg_remove_refs := case when v_remove_seg_refs is null then 0 else jsonb_array_length(coalesce(v_remove_seg_refs,'[]'::jsonb)) end;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','segment_ops_start',
        'add_segment_refs_count', v_dbg_seg_add_refs,
        'remove_segment_refs_count', v_dbg_seg_remove_refs
      )
    );
  end if;

  -- Collect distinct tsfin_ids involved in segment ops
  select array_agg(distinct (x->>'tsfin_id')::uuid)
  into v_seg_tsfin_ids
  from (
    select value as x
    from jsonb_array_elements(coalesce(v_remove_seg_refs,'[]'::jsonb))
    union all
    select value as x
    from jsonb_array_elements(coalesce(v_add_seg_refs,'[]'::jsonb))
  ) u
  where jsonb_typeof(x) = 'object'
    and nullif(btrim(coalesce(x->>'tsfin_id','')), '') is not null;

  v_seg_tsfin_ids := coalesce(v_seg_tsfin_ids, array[]::uuid[]);

  v_dbg_seg_tsfins := coalesce(array_length(v_seg_tsfin_ids,1),0);
  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(jsonb_build_object('step','segment_ops_targets','tsfin_count',v_dbg_seg_tsfins));
  end if;

  if coalesce(array_length(v_seg_tsfin_ids,1),0) > 0 then

    -- Validate that each snapshot is current SEGMENTS mode and has NO additional/expenses/mileage
    foreach v_tsfin_id in array v_seg_tsfin_ids loop
      select *
      into snap
      from public.timesheets_financials tf
      where tf.id = v_tsfin_id
      limit 1;

      if not found then
        raise exception 'Segment edit refers to unknown tsfin_id %', v_tsfin_id;
      end if;

      if snap.is_current is not true then
        raise exception 'Segment edit requires current tsfin snapshot (tsfin_id=%)', v_tsfin_id;
      end if;

      if snap.client_id is distinct from v_inv.client_id then
        raise exception 'Segment edit timesheet client mismatch (tsfin_id=%)', v_tsfin_id;
      end if;

      if snap.invoice_breakdown_json is null
         or jsonb_typeof(snap.invoice_breakdown_json) <> 'object'
         or upper(coalesce(snap.invoice_breakdown_json->>'mode','')) <> 'SEGMENTS'
         or jsonb_typeof(snap.invoice_breakdown_json->'segments') <> 'array'
      then
        raise exception 'Segment edit is only supported for SEGMENTS timesheets (tsfin_id=%)', v_tsfin_id;
      end if;

      v_has_additional :=
        public._inv_round2(coalesce(snap.additional_pay_ex_vat,0)) <> 0
        or public._inv_round2(coalesce(snap.additional_charge_ex_vat,0)) <> 0
        or (snap.additional_units_json is not null and jsonb_typeof(snap.additional_units_json)='object' and snap.additional_units_json <> '{}'::jsonb);

      v_has_expense_or_mileage :=
        public._inv_round2(coalesce(snap.expenses_pay_ex_vat,0)) <> 0
        or public._inv_round2(coalesce(snap.expenses_charge_ex_vat,0)) <> 0
        or public._inv_round2(coalesce(snap.mileage_pay_ex_vat,0)) <> 0
        or public._inv_round2(coalesce(snap.mileage_charge_ex_vat,0)) <> 0
        or public._inv_round2(coalesce(snap.mileage_units,0)) <> 0
        or public._inv_round2(coalesce(snap.travel_pay_ex_vat,0)) <> 0
        or public._inv_round2(coalesce(snap.travel_charge_ex_vat,0)) <> 0
        or public._inv_round2(coalesce(snap.accommodation_pay_ex_vat,0)) <> 0
        or public._inv_round2(coalesce(snap.accommodation_charge_ex_vat,0)) <> 0
        or public._inv_round2(coalesce(snap.other_pay_ex_vat,0)) <> 0
        or public._inv_round2(coalesce(snap.other_charge_ex_vat,0)) <> 0;

      if v_has_additional then
        raise exception 'Segments cannot be moved when additional rates exist (timesheet_id=% tsfin_id=%)', snap.timesheet_id, v_tsfin_id;
      end if;

      if v_has_expense_or_mileage then
        raise exception 'Segments cannot be moved when expenses or mileage exist (timesheet_id=% tsfin_id=%)', snap.timesheet_id, v_tsfin_id;
      end if;
    end loop;

    -- Validate and apply add_segment_refs (lock selected segments)
    v_seg_refs_to_lock := '[]'::jsonb;
    if v_add_seg_refs is not null and jsonb_typeof(v_add_seg_refs)='array' and jsonb_array_length(v_add_seg_refs) > 0 then
      for v_ref in
        select value from jsonb_array_elements(v_add_seg_refs) value
      loop
        if v_ref is null or jsonb_typeof(v_ref) <> 'object' then
          continue;
        end if;

        if nullif(btrim(coalesce(v_ref->>'tsfin_id','')), '') is null then
          continue;
        end if;

        v_tsfin_id := (v_ref->>'tsfin_id')::uuid;
        v_seg_id := nullif(btrim(coalesce(v_ref->>'segment_id','')), '');

        if v_seg_id is null then
          raise exception 'add_segment_refs requires segment_id (tsfin_id=%)', v_tsfin_id;
        end if;

          -- Load snapshot + timesheet + precheck (must be OK)
        select
          tf.*,
          tsr.sheet_scope::text as sheet_scope,
          coalesce(tsr.submission_mode::text,'') as submission_mode,
          tsr.day_references_json,
          tsr.actual_schedule_json,
          tsr.week_ending_date,
          cw.contract_id
        into snap
        from public.timesheets_financials tf
        join public.timesheets tsr on tsr.timesheet_id = tf.timesheet_id and tsr.is_current = true
        left join public.contract_weeks cw on cw.timesheet_id = tf.timesheet_id
        join public.v_ts_invoice_precheck pcv on pcv.timesheet_id = tf.timesheet_id
        where tf.id = v_tsfin_id
          and tf.is_current = true
          and tf.client_id = v_inv.client_id
          and upper(coalesce(pcv.precheck_status,''))
            in ('OK','BLOCK_NO_PDF')
        limit 1;

        if not found then
          raise exception 'Segment add failed eligibility (tsfin_id=% segment_id=%)', v_tsfin_id, v_seg_id;
        end if;

        natural_start := (snap.week_ending_date::date - 6);

        -- Locate the segment object
        v_seg := null;
        for v_seg in
          select value from jsonb_array_elements(snap.invoice_breakdown_json->'segments') value
        loop
          if v_seg is null or jsonb_typeof(v_seg) <> 'object' then
            continue;
          end if;
          if nullif(btrim(coalesce(v_seg->>'segment_id','')), '') = v_seg_id then
            exit;
          end if;
          v_seg := null;
        end loop;

        if v_seg is null then
          raise exception 'Segment not found (tsfin_id=% segment_id=%)', v_tsfin_id, v_seg_id;
        end if;

        seg_locked := nullif(btrim(coalesce(v_seg->>'invoice_locked_invoice_id','')), '');
        if seg_locked is not null then
          raise exception 'Segment already invoiced (tsfin_id=% segment_id=%)', v_tsfin_id, v_seg_id;
        end if;

        seg_target := nullif(btrim(coalesce(v_seg->>'invoice_target_week_start','')), '')::date;
        seg_ref := btrim(coalesce(v_seg->>'ref_num',''));

        -- segment-level ref gating if required
        select * into pc from public.v_ts_invoice_precheck where timesheet_id = snap.timesheet_id limit 1;
        if pc.require_reference_to_invoice is true and seg_ref = '' then
          raise exception 'Segment missing reference number (timesheet_id=% segment_id=%)', snap.timesheet_id, v_seg_id;
        end if;

        -- Week eligibility
        if seg_target is null or seg_target = natural_start then
          if v_week_start <> natural_start then
            raise exception 'Segment not eligible for this invoice week (timesheet_id=% segment_id=%)', snap.timesheet_id, v_seg_id;
          end if;
        else
          if v_week_start <> seg_target then
            raise exception 'Delayed segment not eligible for this invoice week (timesheet_id=% segment_id=%)', snap.timesheet_id, v_seg_id;
          end if;
          if seg_target > v_anchor_ymd then
            raise exception 'Delayed segment cannot be invoiced early (timesheet_id=% segment_id=%)', snap.timesheet_id, v_seg_id;
          end if;
        end if;

        v_seg_refs_to_lock := v_seg_refs_to_lock || jsonb_build_array(
          jsonb_build_object(
            'tsfin_id', v_tsfin_id::text,
            'segment_id', v_seg_id
          )
        );
      end loop;

      if jsonb_typeof(v_seg_refs_to_lock) = 'array' and jsonb_array_length(v_seg_refs_to_lock) > 0 then
        perform public._inv_lock_segments_for_invoice(p_invoice_id, v_seg_refs_to_lock);
      end if;
    end if;

      -- Apply remove_segment_refs (unlock selected segments on THIS invoice)
    if v_remove_seg_refs is not null and jsonb_typeof(v_remove_seg_refs)='array' and jsonb_array_length(v_remove_seg_refs) > 0 then
      perform public._inv_unlock_segment_refs_for_invoice(p_invoice_id, v_remove_seg_refs::jsonb, p_actor_user_id);
    end if;



    -- History: segment ops (always)
    if v_add_seg_refs is not null and jsonb_typeof(v_add_seg_refs)='array' and jsonb_array_length(v_add_seg_refs) > 0 then
      perform public._audit_insert(
        'invoices',
        p_invoice_id::text,
        'INVOICE_SEGMENTS_ADDED',
        null,
        jsonb_build_object('add_segment_refs', v_add_seg_refs),
        null,
        p_actor_user_id
      );
    end if;
    if v_remove_seg_refs is not null and jsonb_typeof(v_remove_seg_refs)='array' and jsonb_array_length(v_remove_seg_refs) > 0 then
      perform public._audit_insert(
        'invoices',
        p_invoice_id::text,
        'INVOICE_SEGMENTS_REMOVED',
        null,
        jsonb_build_object('remove_segment_refs', v_remove_seg_refs),
        null,
        p_actor_user_id
      );
    end if;

    -- Rebuild HOURS lines for touched timesheets on this invoice
    select array_agg(distinct tf.timesheet_id)
    into v_seg_ts_ids
    from public.timesheets_financials tf
    where tf.id = any(v_seg_tsfin_ids);

    v_seg_ts_ids := coalesce(v_seg_ts_ids, array[]::uuid[]);

    foreach tsid in array v_seg_ts_ids loop
      if tsid is null then
        continue;
      end if;

      -- Load snapshot + timesheet + contract (no READY_FOR_INVOICE restriction; this is an invoice edit)
      select
        tf.*,
        tsr.booking_id,
        tsr.week_ending_date,
        tsr.reference_number,
        tsr.sheet_scope::text as sheet_scope,
        coalesce(tsr.submission_mode::text,'') as submission_mode,
        tsr.day_references_json,
        tsr.actual_schedule_json,
        cw.contract_id
      into snap
      from public.timesheets_financials tf
      join public.timesheets tsr on tsr.timesheet_id = tf.timesheet_id and tsr.is_current = true
      left join public.contract_weeks cw on cw.timesheet_id = tf.timesheet_id
      where tf.timesheet_id = tsid
        and tf.is_current = true
      limit 1;

      if not found then
        v_dbg_add_timesheets_skipped := v_dbg_add_timesheets_skipped + 1;
        if v_invoice_debug then
          v_dbg_steps := v_dbg_steps || jsonb_build_array(jsonb_build_object('step','add_timesheet_skipped','timesheet_id',tsid::text));
        end if;
        continue;
      end if;

      v_dbg_add_timesheets_found := v_dbg_add_timesheets_found + 1;
      if v_invoice_debug then
        v_dbg_steps := v_dbg_steps || jsonb_build_array(jsonb_build_object('step','add_timesheet_loaded','timesheet_id',tsid::text,'tsfin_id',snap.id::text));
      end if;

      select v.r2_key
      into v_exact_timesheet_document_r2_key
      from public.timesheets t
      join public.invoice_document_versions v
        on v.entity_type='TIMESHEET'
       and v.entity_id=t.timesheet_id
       and v.purpose='TIMESHEET'
       and v.source_revision=t.document_revision::text
       and v.status='READY'
       and nullif(v.r2_key,'') is not null
       and nullif(v.sha256,'') is not null
       and coalesce(v.size_bytes,0)>0
       and coalesce(v.page_count,0)>0
      where t.timesheet_id=tsid and t.is_current
      order by v.ready_at_utc desc nulls last,v.id desc
      limit 1;


      contract_id := snap.contract_id;
      c_daily_calc := false;
      c_bucket_labels := null;
      c_display_site := null;

      if contract_id is not null then
        select
          coalesce(overrideclientsettings,false),
          daily_calc_of_invoices,
          bucket_labels_json,
          nullif(btrim(coalesce(display_site,'')), '')
        into
          v_contract_override, v_contract_daily_calc, v_contract_bucket_labels, c_display_site
        from public.contracts
        where id = contract_id
        limit 1;

        c_daily_calc := case when v_contract_override then coalesce(v_contract_daily_calc,false) else v_client_daily_calc end;
        c_bucket_labels := case when v_contract_override then v_contract_bucket_labels else null end;
      end if;

      if c_bucket_labels is null then
        c_bucket_labels := jsonb_build_object('day','Day','night','Night','sat','Sat','sun','Sun','bh','BH');
      end if;

      -- Build segment set locked to THIS invoice
      segments := '[]'::jsonb;
      if snap.invoice_breakdown_json is not null
         and jsonb_typeof(snap.invoice_breakdown_json)='object'
         and coalesce(snap.invoice_breakdown_json->>'mode','')='SEGMENTS'
         and jsonb_typeof(snap.invoice_breakdown_json->'segments')='array'
      then
        for v_seg in
          select value from jsonb_array_elements(snap.invoice_breakdown_json->'segments') value
        loop
          if v_seg is null or jsonb_typeof(v_seg) <> 'object' then
            continue;
          end if;

          seg_locked := nullif(btrim(coalesce(v_seg->>'invoice_locked_invoice_id','')), '');
          if seg_locked = p_invoice_id::text then
            segments := segments || jsonb_build_array(v_seg);
          end if;
        end loop;
      end if;

      if jsonb_array_length(coalesce(segments,'[]'::jsonb)) = 0 then
        -- If no segments remain on this invoice for this timesheet, remove ALL invoice lines for that timesheet
        delete from public.invoice_lines
        where invoice_id = p_invoice_id
          and timesheet_id = tsid;
        v_dbg_seg_timesheets_removed := v_dbg_seg_timesheets_removed + 1;
        continue;
      end if;

      v_dbg_seg_timesheets_rebuilt := v_dbg_seg_timesheets_rebuilt + 1;

      -- Replace HOURS lines for this timesheet on this invoice
      delete from public.invoice_lines
      where invoice_id = p_invoice_id
        and timesheet_id = tsid
        and upper(coalesce(meta_json->>'line_type','')) in ('HOURS_WEEKLY','HOURS_DAILY');
        -- HOURS lines
        if c_daily_calc then
          -- Daily: group by segment.date
          for r_day in
            with rows as (
              select
                nullif(btrim(coalesce(seg_el->>'date','')), '') as ymd,
                coalesce((seg_el->>'hours_day')::numeric,0)   as h_day,
                coalesce((seg_el->>'hours_night')::numeric,0) as h_night,
                coalesce((seg_el->>'hours_sat')::numeric,0)   as h_sat,
                coalesce((seg_el->>'hours_sun')::numeric,0)   as h_sun,
                coalesce((seg_el->>'hours_bh')::numeric,0)    as h_bh,
                coalesce((seg_el->>'pay_amount')::numeric,0)  as pay_ex,
                coalesce((seg_el->>'charge_amount')::numeric,0) as chg_ex
              from jsonb_array_elements(segments) seg_el
            ),
            agg as (
              select
                ymd,
                sum(rows.h_day)::numeric as hours_day,
                sum(rows.h_night)::numeric as hours_night,
                sum(rows.h_sat)::numeric as hours_sat,
                sum(rows.h_sun)::numeric as hours_sun,
                sum(rows.h_bh)::numeric as hours_bh,
                sum(rows.pay_ex)::numeric as pay_ex,
                sum(rows.chg_ex)::numeric as chg_ex
              from rows
              where ymd is not null and ymd ~ '^\d{4}-\d{2}-\d{2}$'
              group by ymd
            )
            select * from agg order by ymd
          loop
          chg_ex := public._inv_round2(r_day.chg_ex);
if chg_ex = 0 then continue; end if;

if (coalesce(r_day.hours_day,0)+coalesce(r_day.hours_night,0)+coalesce(r_day.hours_sat,0)+coalesce(r_day.hours_sun,0)+coalesce(r_day.hours_bh,0)) = 0 then
  continue;
end if;


            pay_ex := public._inv_round2(r_day.pay_ex);
            margin_ex := public._inv_round2(chg_ex - pay_ex);
            vat_amt := public._inv_round2(chg_ex * v_vat_rate / 100);
            inc_amt := public._inv_round2(chg_ex + vat_amt);

            line_desc := coalesce(nullif(btrim(coalesce(c_display_site,'')) ,''), ('TS '||tsid::text)) ||
                         ' – '|| r_day.ymd || ' – W/E '|| coalesce(snap.week_ending_date::text,'');

            v_meta := jsonb_build_object(
              'line_type','HOURS_DAILY',
              'timesheet_id', tsid::text,
              'tsfin_id', snap.id::text,
              'candidate_display', coalesce(nullif(btrim(coalesce(c_display_site,'')),''), null),
              'role', c_role,
              'hospital', c_display_site,
              'ward', c_ward_hint,
              'week_ending_date', snap.week_ending_date::text,
              'date', r_day.ymd,
              'bucket_labels', c_bucket_labels
            );

            v_source_key := 'TS:' || tsid::text || ':HOURS:' || r_day.ymd;

            insert into public.invoice_lines(
              invoice_id, timesheet_id, booking_id, description,
              hours_day, hours_night, hours_sat, hours_sun, hours_bh,
              pay_day, pay_night, pay_sat, pay_sun, pay_bh,
              charge_day, charge_night, charge_sat, charge_sun, charge_bh,
              total_pay_ex_vat, total_charge_ex_vat, margin_ex_vat,
              vat_rate_pct, vat_amount, total_inc_vat,
              paper_ts_r2_key, meta_json, source_key
            )
            values (
              p_invoice_id, tsid, snap.booking_id, line_desc,
              public._inv_round2(r_day.hours_day), public._inv_round2(r_day.hours_night), public._inv_round2(r_day.hours_sat), public._inv_round2(r_day.hours_sun), public._inv_round2(r_day.hours_bh),
              null,null,null,null,null,
              null,null,null,null,null,
              pay_ex, chg_ex, margin_ex,
              v_vat_rate, vat_amt, inc_amt,
              v_exact_timesheet_document_r2_key,
              v_meta,
              v_source_key
            )
            on conflict (invoice_id, source_key) do nothing;
          end loop;
        else
          -- Weekly hours line
          select
            public._inv_round2(coalesce(sum((seg_el->>'hours_day')::numeric),0)),
            public._inv_round2(coalesce(sum((seg_el->>'hours_night')::numeric),0)),
            public._inv_round2(coalesce(sum((seg_el->>'hours_sat')::numeric),0)),
            public._inv_round2(coalesce(sum((seg_el->>'hours_sun')::numeric),0)),
            public._inv_round2(coalesce(sum((seg_el->>'hours_bh')::numeric),0)),
            public._inv_round2(coalesce(sum((seg_el->>'pay_amount')::numeric),0)),
            public._inv_round2(coalesce(sum((seg_el->>'charge_amount')::numeric),0))
          into h_day, h_night, h_sat, h_sun, h_bh, pay_ex, chg_ex
          from jsonb_array_elements(segments) seg_el;

       if chg_ex <> 0 and (coalesce(h_day,0)+coalesce(h_night,0)+coalesce(h_sat,0)+coalesce(h_sun,0)+coalesce(h_bh,0)) <> 0 then
  margin_ex := public._inv_round2(chg_ex - pay_ex);
  vat_amt := public._inv_round2(chg_ex * v_vat_rate / 100);
  inc_amt := public._inv_round2(chg_ex + vat_amt);

  line_desc := coalesce(nullif(btrim(coalesce(c_display_site,'')) ,''), ('TS '||tsid::text)) ||
               ' – W/E '|| coalesce(snap.week_ending_date::text,'');

  v_meta := jsonb_build_object(
    'line_type','HOURS_WEEKLY',
    'timesheet_id', tsid::text,
    'tsfin_id', snap.id::text,
    'week_ending_date', snap.week_ending_date::text,
    'bucket_labels', c_bucket_labels
  );

  v_source_key := 'TS:' || tsid::text || ':HOURS:WEEK';

  insert into public.invoice_lines(
    invoice_id, timesheet_id, booking_id, description,
    hours_day, hours_night, hours_sat, hours_sun, hours_bh,
    pay_day, pay_night, pay_sat, pay_sun, pay_bh,
    charge_day, charge_night, charge_sat, charge_sun, charge_bh,
    total_pay_ex_vat, total_charge_ex_vat, margin_ex_vat,
    vat_rate_pct, vat_amount, total_inc_vat,
    paper_ts_r2_key, meta_json, source_key
  )
  values (
    p_invoice_id, tsid, snap.booking_id, line_desc,
    h_day, h_night, h_sat, h_sun, h_bh,
    null,null,null,null,null,
    null,null,null,null,null,
    pay_ex, chg_ex, margin_ex,
    v_vat_rate, vat_amt, inc_amt,
    v_exact_timesheet_document_r2_key,
    v_meta,
    v_source_key
  )
  on conflict (invoice_id, source_key) do nothing;
end if;

        end if;

        -- Additional rates


    end loop;
  end if;
end if;

  -- 2) Add adjustments
  if p_payload is not null and jsonb_typeof(p_payload) = 'object' and (p_payload ? 'add_adjustments') then
    if jsonb_typeof(p_payload->'add_adjustments') = 'array' then
      for adj in
        select value from jsonb_array_elements(p_payload->'add_adjustments') value
      loop
        if adj is null or jsonb_typeof(adj) <> 'object' then
          continue;
        end if;

        v_adj_token := nullif(btrim(coalesce(adj->>'client_token','')), '');
        v_adj_desc  := nullif(btrim(coalesce(adj->>'description','')), '');
        begin
          v_adj_ex := (adj->>'amount_ex_vat')::numeric;
        exception when others then
          v_adj_ex := null;
        end;

        if v_adj_token is null or v_adj_desc is null or v_adj_ex is null then
          continue;
        end if;

        v_adj_vat := public._inv_round2(v_adj_ex * v_vat_rate / 100);
        if v_vat_rate = 0 then v_adj_vat := 0; end if;
        v_adj_inc := public._inv_round2(v_adj_ex + v_adj_vat);

        v_adj_source_key := 'ADJ:' || v_adj_token;

        v_meta := jsonb_build_object(
          'line_type','ADJUSTMENT',
          'client_token', v_adj_token,
          'description', v_adj_desc,
          'amount_ex_vat', public._inv_round2(v_adj_ex),
          'vat_rate_pct', v_vat_rate,
          'vat_chargeable', v_vat_chargeable
        );

        v_hist_adj := v_hist_adj || jsonb_build_array(v_meta);

        insert into public.invoice_lines(
          invoice_id, timesheet_id, booking_id, description,
          hours_day, hours_night, hours_sat, hours_sun, hours_bh,
          pay_day, pay_night, pay_sat, pay_sun, pay_bh,
          charge_day, charge_night, charge_sat, charge_sun, charge_bh,
          total_pay_ex_vat, total_charge_ex_vat, margin_ex_vat,
          vat_rate_pct, vat_amount, total_inc_vat,
          paper_ts_r2_key, meta_json, source_key
        )
        values (
          p_invoice_id, null, null, v_adj_desc,
          0,0,0,0,0,
          null,null,null,null,null,
          null,null,null,null,null,
          0, public._inv_round2(v_adj_ex), public._inv_round2(v_adj_ex),
          v_vat_rate, v_adj_vat, v_adj_inc,
          null,
          v_meta,
          v_adj_source_key
        )
        on conflict (invoice_id, source_key) do nothing;
      end loop;
    end if;
  end if;

  -- History: adjustments added (always)
  if jsonb_typeof(v_hist_adj)='array' and jsonb_array_length(v_hist_adj) > 0 then
    perform public._audit_insert(
      'invoices',
      p_invoice_id::text,
      'INVOICE_ADJUSTMENTS_ADDED',
      null,
      jsonb_build_object('adjustments', v_hist_adj),
      null,
      p_actor_user_id
    );
  end if;


  -- 3) Add timesheets (full parity: hours + additional + expenses + mileage), for THIS invoice week_start
  if v_add_ts_ids is not null and coalesce(array_length(v_add_ts_ids,1),0) > 0 then
    foreach tsid in array v_add_ts_ids loop
      if tsid is null then
        continue;
      end if;

      -- Load snapshot + timesheet + precheck
      select
        tf.*,
        tsr.booking_id,
        tsr.week_ending_date,
        tsr.reference_number,
        tsr.sheet_scope::text as sheet_scope,
        coalesce(tsr.submission_mode::text,'') as submission_mode,
        tsr.day_references_json,
        tsr.actual_schedule_json,
        cw.contract_id
      into snap
      from public.timesheets_financials tf
      join public.timesheets tsr on tsr.timesheet_id = tf.timesheet_id and tsr.is_current = true
      left join public.contract_weeks cw on cw.timesheet_id = tf.timesheet_id
      join public.v_ts_invoice_precheck pcv on pcv.timesheet_id = tf.timesheet_id
      where tf.timesheet_id = tsid
        and tf.is_current = true
        and tf.locked_by_invoice_id is null
        and tf.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
        and upper(coalesce(pcv.precheck_status,''))
          in ('OK','BLOCK_NO_PDF')
        and tf.client_id = v_inv.client_id
        and (
          pcv.require_reference_to_invoice is not true
          or public._inv_timesheet_has_invoice_reference(
                tsr.sheet_scope::text,
                coalesce(tsr.submission_mode::text,''),
                tsr.reference_number,
                tsr.day_references_json,
                tsr.actual_schedule_json
             )
        )
      limit 1;

      if not found then
        v_dbg_add_timesheets_skipped := v_dbg_add_timesheets_skipped + 1;
        if v_invoice_debug then
          v_dbg_steps := v_dbg_steps || jsonb_build_array(jsonb_build_object('step','add_timesheet_skipped','timesheet_id',coalesce(tsid::text,'')));
        end if;
        continue;
      end if;

      v_dbg_add_timesheets_found := v_dbg_add_timesheets_found + 1;
      if v_invoice_debug then
        v_dbg_steps := v_dbg_steps || jsonb_build_array(jsonb_build_object('step','add_timesheet_loaded','timesheet_id',coalesce(tsid::text,''),'tsfin_id',coalesce(snap.id::text,'')));
      end if;

      select v.r2_key
      into v_exact_timesheet_document_r2_key
      from public.timesheets t
      join public.invoice_document_versions v
        on v.entity_type='TIMESHEET'
       and v.entity_id=t.timesheet_id
       and v.purpose='TIMESHEET'
       and v.source_revision=t.document_revision::text
       and v.status='READY'
       and nullif(v.r2_key,'') is not null
       and nullif(v.sha256,'') is not null
       and coalesce(v.size_bytes,0)>0
       and coalesce(v.page_count,0)>0
      where t.timesheet_id=tsid and t.is_current
      order by v.ready_at_utc desc nulls last,v.id desc
      limit 1;
      contract_id := snap.contract_id;
      c_daily_calc := false;
      c_bucket_labels := null;
      c_role := null;
      c_display_site := null;
      c_ward_hint := null;

      if contract_id is not null then
        select
          coalesce(overrideclientsettings,false),
          daily_calc_of_invoices,
          bucket_labels_json,
          nullif(btrim(coalesce(role,'')), ''),
          nullif(btrim(coalesce(display_site,'')), ''),
          nullif(btrim(coalesce(ward_hint,'')), '')
        into
          v_contract_override, v_contract_daily_calc, v_contract_bucket_labels, c_role, c_display_site, c_ward_hint
        from public.contracts
        where id = contract_id
        limit 1;

        c_daily_calc := case when v_contract_override then coalesce(v_contract_daily_calc,false) else v_client_daily_calc end;
        c_bucket_labels := case when v_contract_override then v_contract_bucket_labels else null end;
      end if;

      if c_bucket_labels is null then
        c_bucket_labels := jsonb_build_object('day','Day','night','Night','sat','Sat','sun','Sun','bh','BH');
      end if;

      natural_start := (snap.week_ending_date::date - 6);

      segments := '[]'::jsonb;

      -- Build invoiceable segment set for THIS invoice week_start
      if snap.invoice_breakdown_json is not null
         and jsonb_typeof(snap.invoice_breakdown_json)='object'
         and coalesce(snap.invoice_breakdown_json->>'mode','')='SEGMENTS'
         and jsonb_typeof(snap.invoice_breakdown_json->'segments')='array'
      then
        for v_seg in
          select value from jsonb_array_elements(snap.invoice_breakdown_json->'segments') value
        loop
          if v_seg is null or jsonb_typeof(v_seg) <> 'object' then
            continue;
          end if;

          seg_locked := nullif(btrim(coalesce(v_seg->>'invoice_locked_invoice_id','')), '');
          if seg_locked is not null then
            continue; -- already invoiced
          end if;

          seg_target := nullif(btrim(coalesce(v_seg->>'invoice_target_week_start','')), '')::date;
          seg_ref := btrim(coalesce(v_seg->>'ref_num',''));

          -- segment-level ref gating if required
          select * into pc from public.v_ts_invoice_precheck where timesheet_id = tsid limit 1;
          if pc.require_reference_to_invoice is true and seg_ref = '' then
            continue;
          end if;

          -- delayed detection
          if seg_target is null or seg_target = natural_start then
            -- not delayed: belongs to natural week only
            if v_week_start <> natural_start then
              continue;
            end if;
            -- allow early is implicit for invoice edits (invoice is already being edited)
            segments := segments || jsonb_build_array(v_seg);
          else
            -- delayed: only if this invoice week matches target AND target has arrived (no early invoicing for delayed)
            if v_week_start = seg_target and seg_target <= v_anchor_ymd then
              segments := segments || jsonb_build_array(v_seg);
            end if;
          end if;
        end loop;
      else
        -- Non-segments: include only when invoice week matches natural week
        if v_week_start = natural_start then
          segments := jsonb_build_array(
            jsonb_build_object(
              'segment_id', null,
              'date', coalesce(snap.week_ending_date::text, v_week_start::text),
              'hours_day', public._inv_round2(coalesce(snap.hours_day,0)),
              'hours_night', public._inv_round2(coalesce(snap.hours_night,0)),
              'hours_sat', public._inv_round2(coalesce(snap.hours_sat,0)),
              'hours_sun', public._inv_round2(coalesce(snap.hours_sun,0)),
              'hours_bh', public._inv_round2(coalesce(snap.hours_bh,0)),
              'pay_amount', public._inv_round2(
                coalesce(snap.total_pay_ex_vat,0)
                - coalesce(snap.additional_pay_ex_vat,0)
                - coalesce(snap.expenses_pay_ex_vat,0)
                - coalesce(snap.mileage_pay_ex_vat,0)
              ),
              'charge_amount', public._inv_round2(
                coalesce(snap.total_charge_ex_vat,0)
                - coalesce(snap.additional_charge_ex_vat,0)
                - coalesce(snap.expenses_charge_ex_vat,0)
                - coalesce(snap.mileage_charge_ex_vat,0)
              ),
              'ref_num', coalesce(snap.reference_number,'')
            )
          );
        end if;
      end if;

      if jsonb_array_length(coalesce(segments,'[]'::jsonb)) = 0 then
        continue;
      end if;

      -- Build the exact invoice-line economics from the frozen current financial snapshot.
      -- This mirrors Invoice Async V8 generation and never derives economics from mutable live rows.
      select p.vat_rate
      into v_vat_rate
      from private._invoice_generation_vat_policy_batch(jsonb_build_array(jsonb_build_object(
        'source_member_key','invoice-edit:'||tsid::text,
        'source_type','TIMESHEET',
        'source_id',tsid,
        'timesheet_id',tsid,
        'effective_date',snap.week_ending_date
      ))) p
      where p.source_member_key='invoice-edit:'||tsid::text
        and p.valid
        and p.vat_rate is not null
      limit 1;

      if not found then
        raise exception 'INVOICE_EDIT_VAT_POLICY_UNRESOLVED'
          using errcode='P0001',detail=jsonb_build_object(
            'invoice_id',p_invoice_id,'timesheet_id',tsid)::text;
      end if;

      insert into public.invoice_lines(
        invoice_id,timesheet_id,booking_id,description,
        hours_day,hours_night,hours_sat,hours_sun,hours_bh,
        pay_day,pay_night,pay_sat,pay_sun,pay_bh,
        charge_day,charge_night,charge_sat,charge_sun,charge_bh,
        total_pay_ex_vat,total_charge_ex_vat,margin_ex_vat,
        vat_rate_pct,vat_amount,total_inc_vat,
        paper_ts_r2_key,meta_json,source_key
      )
      with base as materialized (
        select coalesce(
          (select nullif(btrim(coalesce(cd.display_name,'')),'')
             from public.candidates cd where cd.id=snap.candidate_id limit 1),
          'TS '||tsid::text
        ) candidate_display
      ),
      segment_rows as materialized (
        select
          case when left(coalesce(s.value->>'date',''),10) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
            then left(s.value->>'date',10) end work_date,
          case when pg_input_is_valid(s.value->>'hours_day','numeric') then (s.value->>'hours_day')::numeric else 0 end h_day,
          case when pg_input_is_valid(s.value->>'hours_night','numeric') then (s.value->>'hours_night')::numeric else 0 end h_night,
          case when pg_input_is_valid(s.value->>'hours_sat','numeric') then (s.value->>'hours_sat')::numeric else 0 end h_sat,
          case when pg_input_is_valid(s.value->>'hours_sun','numeric') then (s.value->>'hours_sun')::numeric else 0 end h_sun,
          case when pg_input_is_valid(s.value->>'hours_bh','numeric') then (s.value->>'hours_bh')::numeric else 0 end h_bh,
          case
            when pg_input_is_valid(s.value->>'pay_amount','numeric') then (s.value->>'pay_amount')::numeric
            when pg_input_is_valid(s.value->>'pay_ex_vat','numeric') then (s.value->>'pay_ex_vat')::numeric
            else 0 end pay_ex,
          case
            when pg_input_is_valid(s.value->>'charge_amount','numeric') then (s.value->>'charge_amount')::numeric
            when pg_input_is_valid(s.value->>'charge_ex_vat','numeric') then (s.value->>'charge_ex_vat')::numeric
            else 0 end charge_ex
        from jsonb_array_elements(segments) s(value)
      ),
      hour_daily as materialized (
        select s.work_date,sum(s.h_day) h_day,sum(s.h_night) h_night,sum(s.h_sat) h_sat,
          sum(s.h_sun) h_sun,sum(s.h_bh) h_bh,sum(s.pay_ex) pay_ex,sum(s.charge_ex) charge_ex
        from segment_rows s
        where c_daily_calc and s.work_date is not null
        group by s.work_date
      ),
      hour_weekly as materialized (
        select sum(s.h_day) h_day,sum(s.h_night) h_night,sum(s.h_sat) h_sat,
          sum(s.h_sun) h_sun,sum(s.h_bh) h_bh,sum(s.pay_ex) pay_ex,sum(s.charge_ex) charge_ex
        from segment_rows s
        having not c_daily_calc or not exists(select 1 from segment_rows d where d.work_date is not null)
      ),
      additional_source as materialized (
        select upper(a.key) code,a.value unit
        from jsonb_each(case when jsonb_typeof(snap.additional_units_json)='object'
          then snap.additional_units_json else '{}'::jsonb end) a
        where jsonb_typeof(a.value)='object' and btrim(a.key)<>''
      ),
      additional_daily as materialized (
        select a.code,left(d.key,10) work_date,
          case when pg_input_is_valid(d.value,'numeric') then d.value::numeric else 0 end units,
          case when pg_input_is_valid(a.unit->>'pay_rate','numeric') then (a.unit->>'pay_rate')::numeric else 0 end pay_rate,
          case when pg_input_is_valid(a.unit->>'charge_rate','numeric') then (a.unit->>'charge_rate')::numeric else 0 end charge_rate,
          coalesce(nullif(a.unit->>'bucket_name',''),a.code) bucket_name,
          coalesce(nullif(a.unit->>'unit_name',''),'units') unit_name,
          a.unit->'frequency' frequency
        from additional_source a
        cross join lateral jsonb_each_text(case when jsonb_typeof(a.unit->'days')='object'
          then a.unit->'days' else '{}'::jsonb end) d
        where c_daily_calc
          and left(d.key,10) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
          and pg_input_is_valid(d.value,'numeric')
          and d.value::numeric<>0
          and exists(select 1 from segment_rows where work_date is not null)
      ),
      additional_weekly as materialized (
        select a.code,
          case when pg_input_is_valid(a.unit->>'unit_count','numeric') then (a.unit->>'unit_count')::numeric else 0 end units,
          case when pg_input_is_valid(a.unit->>'pay_rate','numeric') then (a.unit->>'pay_rate')::numeric else 0 end pay_rate,
          case when pg_input_is_valid(a.unit->>'charge_rate','numeric') then (a.unit->>'charge_rate')::numeric else 0 end charge_rate,
          case when pg_input_is_valid(a.unit->>'pay_ex_vat','numeric') then (a.unit->>'pay_ex_vat')::numeric else 0 end pay_ex,
          case when pg_input_is_valid(a.unit->>'charge_ex_vat','numeric') then (a.unit->>'charge_ex_vat')::numeric else 0 end charge_ex,
          coalesce(nullif(a.unit->>'bucket_name',''),a.code) bucket_name,
          coalesce(nullif(a.unit->>'unit_name',''),'units') unit_name,
          a.unit->'frequency' frequency
        from additional_source a
        where not(c_daily_calc
          and exists(select 1 from jsonb_each_text(case when jsonb_typeof(a.unit->'days')='object'
            then a.unit->'days' else '{}'::jsonb end) d
            where left(d.key,10) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
              and pg_input_is_valid(d.value,'numeric') and d.value::numeric<>0))
          and (
            (case when pg_input_is_valid(a.unit->>'pay_ex_vat','numeric') then (a.unit->>'pay_ex_vat')::numeric else 0 end)<>0
            or (case when pg_input_is_valid(a.unit->>'charge_ex_vat','numeric') then (a.unit->>'charge_ex_vat')::numeric else 0 end)<>0
          )
      ),
      expense_lines as materialized (
        select e.code,e.pay_ex,e.charge_ex
        from (values
          ('TRAVEL',coalesce(snap.travel_pay_ex_vat,0),coalesce(snap.travel_charge_ex_vat,0)),
          ('ACCOMMODATION',coalesce(snap.accommodation_pay_ex_vat,0),coalesce(snap.accommodation_charge_ex_vat,0)),
          ('OTHER',coalesce(snap.other_pay_ex_vat,0),coalesce(snap.other_charge_ex_vat,0)),
          ('EXPENSES_FALLBACK',
            case when coalesce(snap.travel_pay_ex_vat,0)+coalesce(snap.accommodation_pay_ex_vat,0)+coalesce(snap.other_pay_ex_vat,0)=0
              then coalesce(snap.expenses_pay_ex_vat,0) else 0 end,
            case when coalesce(snap.travel_charge_ex_vat,0)+coalesce(snap.accommodation_charge_ex_vat,0)+coalesce(snap.other_charge_ex_vat,0)=0
              then coalesce(snap.expenses_charge_ex_vat,0) else 0 end),
          ('MILEAGE',coalesce(snap.mileage_pay_ex_vat,0),coalesce(snap.mileage_charge_ex_vat,0))
        ) e(code,pay_ex,charge_ex)
        where e.pay_ex<>0 or e.charge_ex<>0
      ),
      line_union as materialized (
        select b.candidate_display||' - '||h.work_date||' - W/E '||coalesce(snap.week_ending_date::text,'' ) description,
          h.h_day,h.h_night,h.h_sat,h.h_sun,h.h_bh,h.pay_ex,h.charge_ex,
          'HOURS_DAILY' line_type,'TS:'||tsid::text||':HOURS:'||h.work_date source_key,
          jsonb_build_object('date',h.work_date) detail
        from hour_daily h cross join base b
        union all
        select b.candidate_display||' - W/E '||coalesce(snap.week_ending_date::text,''),
          h.h_day,h.h_night,h.h_sat,h.h_sun,h.h_bh,h.pay_ex,h.charge_ex,
          'HOURS_WEEKLY','TS:'||tsid::text||':HOURS:WEEK','{}'::jsonb
        from hour_weekly h cross join base b
        union all
        select b.candidate_display||' - '||a.bucket_name||' - '||a.work_date||' - '||a.units||' '||a.unit_name,
          0,0,0,0,0,round(a.units*a.pay_rate,2),round(a.units*a.charge_rate,2),
          'ADDITIONAL_RATE_DAILY','TS:'||tsid::text||':ADD:'||a.code||':'||a.work_date,
          jsonb_build_object('date',a.work_date,'bucket',jsonb_build_object(
            'code',a.code,'bucket_name',a.bucket_name,'unit_name',a.unit_name,'frequency',a.frequency),
            'units',jsonb_build_object('unit_count',a.units,'pay_rate',a.pay_rate,'charge_rate',a.charge_rate))
        from additional_daily a cross join base b
        union all
        select b.candidate_display||' - '||a.bucket_name||' - '||a.units||' '||a.unit_name,
          0,0,0,0,0,a.pay_ex,a.charge_ex,
          'ADDITIONAL_RATE','TS:'||tsid::text||':ADD:'||a.code||':WEEK',
          jsonb_build_object('bucket',jsonb_build_object(
            'code',a.code,'bucket_name',a.bucket_name,'unit_name',a.unit_name,'frequency',a.frequency),
            'units',jsonb_build_object('unit_count',a.units,'pay_rate',a.pay_rate,'charge_rate',a.charge_rate))
        from additional_weekly a cross join base b
        union all
        select case when e.code='MILEAGE' then 'Mileage - '||coalesce(snap.mileage_units,0)||' miles (W/E '||coalesce(snap.week_ending_date::text,'')||')'
          when e.code='EXPENSES_FALLBACK' then 'Expenses (W/E '||coalesce(snap.week_ending_date::text,'')||')'
          else initcap(replace(e.code,'_',' '))||' expenses (W/E '||coalesce(snap.week_ending_date::text,'')||')' end,
          0,0,0,0,0,e.pay_ex,e.charge_ex,
          case when e.code='MILEAGE' then 'MILEAGE'
            when e.code='EXPENSES_FALLBACK' then 'EXPENSES_TOTAL'
            else 'EXPENSE_'||e.code end,
          case when e.code='MILEAGE' then 'TS:'||tsid::text||':MILEAGE'
            when e.code='EXPENSES_FALLBACK' then 'TS:'||tsid::text||':EXP:TOTAL'
            else 'TS:'||tsid::text||':EXP:'||e.code end,
          jsonb_build_object('expense',case when e.code='MILEAGE' then jsonb_build_object(
            'category','MILEAGE','mileage_units',snap.mileage_units,
            'pay_rate',snap.mileage_pay_rate,'charge_rate',snap.mileage_charge_rate,
            'evidence_r2_key',snap.mileage_evidence_r2_key,'evidence_manifest',snap.mileage_evidence_manifest)
            else jsonb_build_object('category',e.code,'note',snap.expenses_description,
              'evidence_r2_key',snap.expenses_evidence_r2_key,'evidence_manifest',snap.expenses_evidence_manifest) end)
        from expense_lines e
      )
      select p_invoice_id,tsid,snap.booking_id,l.description,
        round(l.h_day,2),round(l.h_night,2),round(l.h_sat,2),round(l.h_sun,2),round(l.h_bh,2),
        null,null,null,null,null,
        snap.charge_day,snap.charge_night,snap.charge_sat,snap.charge_sun,snap.charge_bh,
        round(l.pay_ex,2),round(l.charge_ex,2),round(l.charge_ex-l.pay_ex,2),v_vat_rate,
        round(l.charge_ex*v_vat_rate/100,2),round(l.charge_ex+(l.charge_ex*v_vat_rate/100),2),
        v_exact_timesheet_document_r2_key,
        jsonb_build_object('line_type',l.line_type,'timesheet_id',tsid,'tsfin_id',snap.id,
          'candidate_display',l.description,'week_ending_date',snap.week_ending_date,
          'role',c_role,'hospital',c_display_site,'ward',c_ward_hint,'bucket_labels',c_bucket_labels,
          'totals',jsonb_build_object('line_pay_ex_vat',round(l.pay_ex,2),
            'line_charge_ex_vat',round(l.charge_ex,2),'margin_ex_vat',round(l.charge_ex-l.pay_ex,2),
            'vat_rate_pct',v_vat_rate,'vat_amount',round(l.charge_ex*v_vat_rate/100,2),
            'total_inc_vat',round(l.charge_ex+(l.charge_ex*v_vat_rate/100),2)))||l.detail,
        l.source_key
      from line_union l
      on conflict(invoice_id,source_key) do nothing;
      -- Build segment refs to lock
      if snap.invoice_breakdown_json is not null
         and jsonb_typeof(snap.invoice_breakdown_json)='object'
         and coalesce(snap.invoice_breakdown_json->>'mode','')='SEGMENTS'
      then
        for v_seg in
          select value from jsonb_array_elements(segments) value
        loop
          if v_seg is null or jsonb_typeof(v_seg) <> 'object' then
            continue;
          end if;

          seg_refs := seg_refs || jsonb_build_array(
            jsonb_build_object(
              'tsfin_id', snap.id::text,
              'segment_id', nullif(btrim(coalesce(v_seg->>'segment_id','')), '')
            )
          );
        end loop;
      else
        -- lock whole for non-segments
        seg_refs := seg_refs || jsonb_build_array(
          jsonb_build_object('tsfin_id', snap.id::text, 'segment_id', null)
        );
      end if;
    end loop;

    -- Apply segment locks for the exact selected source rows.
    if jsonb_typeof(seg_refs) = 'array' and jsonb_array_length(seg_refs) > 0 then
      perform public._inv_lock_segments_for_invoice(p_invoice_id, seg_refs);
    end if;

    -- History: timesheets added (always; includes requested ids)
    perform public._audit_insert(
      'invoices',
      p_invoice_id::text,
      'INVOICE_TIMESHEETS_ADDED',
      null,
      jsonb_build_object('add_timesheet_ids', coalesce(to_jsonb(v_add_ts_ids), '[]'::jsonb)),
      null,
      p_actor_user_id
    );
  end if;

  -- 3c) Contract week status: set INVOICED only when timesheet is FULLY invoiced (segment-aware), and revert INVOICED -> AUTHORISED if no longer fully invoiced.
  -- Touch set = union of: add_timesheet_ids, line-removal touched (hours), segment-op touched.
  v_cw_ts_ids := array[]::uuid[];
  if v_add_ts_ids is not null and coalesce(array_length(v_add_ts_ids,1),0) > 0 then
    v_cw_ts_ids := v_cw_ts_ids || v_add_ts_ids;
  end if;
  if v_ts_ids_touched is not null and coalesce(array_length(v_ts_ids_touched,1),0) > 0 then
    v_cw_ts_ids := v_cw_ts_ids || v_ts_ids_touched;
  end if;
  if v_seg_ts_ids is not null and coalesce(array_length(v_seg_ts_ids,1),0) > 0 then
    v_cw_ts_ids := v_cw_ts_ids || v_seg_ts_ids;
  end if;

  -- de-dup
  select array_agg(distinct x)
  into v_cw_ts_ids
  from unnest(coalesce(v_cw_ts_ids, array[]::uuid[])) x
  where x is not null;

  v_cw_ts_ids := coalesce(v_cw_ts_ids, array[]::uuid[]);

  if coalesce(array_length(v_cw_ts_ids,1),0) > 0 then
    with src as (
      select
        cw.timesheet_id,
        cw.status as cw_status,
        tf.locked_by_invoice_id,
        tf.invoice_breakdown_json,
        (
          case
            when tf.invoice_breakdown_json is not null
             and jsonb_typeof(tf.invoice_breakdown_json)='object'
             and coalesce(tf.invoice_breakdown_json->>'mode','')='SEGMENTS'
             and jsonb_typeof(tf.invoice_breakdown_json->'segments')='array'
             and jsonb_array_length(tf.invoice_breakdown_json->'segments') > 0
            then
              not exists (
                select 1
                from jsonb_array_elements(tf.invoice_breakdown_json->'segments') s(seg)
                where nullif(btrim(coalesce(s.seg->>'invoice_locked_invoice_id','')), '') is null
              )
            else
              (tf.locked_by_invoice_id is not null)
          end
        ) as fully_invoiced
      from public.contract_weeks cw
      join public.timesheets_financials tf
        on tf.is_current = true
       and tf.timesheet_id = cw.timesheet_id
      where cw.timesheet_id = any(v_cw_ts_ids)
    )
    update public.contract_weeks cw
    set status = case
      when src.fully_invoiced then 'INVOICED'::public.contract_week_status_enum
      when cw.status = 'INVOICED'::public.contract_week_status_enum then 'AUTHORISED'::public.contract_week_status_enum
      else cw.status
    end
    from src
    where cw.timesheet_id = src.timesheet_id;
  end if;


  -- 4) Recompute invoice totals from invoice_lines and clear PDF key
  select
    public._inv_round2(coalesce(sum(coalesce(l.total_charge_ex_vat,0)),0)),
    public._inv_round2(coalesce(sum(coalesce(l.vat_amount,0)),0)),
    public._inv_round2(coalesce(sum(coalesce(l.total_inc_vat,0)),0))
  into v_new_ex, v_new_vat, v_new_inc
  from public.invoice_lines l
  where l.invoice_id = p_invoice_id;


  perform public.invoice_recompute_totals(p_invoice_id);

  -- Recompute header_snapshot_json.meta counters to avoid stale values after edits
  select count(distinct l.timesheet_id)
  into v_hdr_ts_count_lines
  from public.invoice_lines l
  where l.invoice_id = p_invoice_id
    and l.timesheet_id is not null;

  select count(*)
  into v_hdr_seg_locked_count
  from public.timesheets_financials tf
  cross join lateral jsonb_array_elements(coalesce(tf.invoice_breakdown_json->'segments','[]'::jsonb)) as seg(seg_obj)
  where tf.is_current = true
    and coalesce(seg.seg_obj->>'invoice_locked_invoice_id','') = p_invoice_id::text;

  select count(distinct tf.timesheet_id)
  into v_hdr_ts_count_seglocks
  from public.timesheets_financials tf
  where tf.is_current = true
    and exists (
      select 1
      from jsonb_array_elements(coalesce(tf.invoice_breakdown_json->'segments','[]'::jsonb)) as seg(seg_obj)
      where coalesce(seg.seg_obj->>'invoice_locked_invoice_id','') = p_invoice_id::text
    );

  if coalesce(v_hdr_seg_locked_count,0) > 0 then
    v_hdr_meta_timesheet_count := coalesce(v_hdr_ts_count_seglocks,0);
    v_hdr_meta_segment_count := coalesce(v_hdr_seg_locked_count,0);
  else
    v_hdr_meta_timesheet_count := coalesce(v_hdr_ts_count_lines,0);
    v_hdr_meta_segment_count := coalesce(v_hdr_meta_timesheet_count,0);
  end if;

  -- The statement triggers are the sole document/issue invalidation authority.
  -- This update changes only the authoritative business snapshot counters.
  update public.invoices invu
  set header_snapshot_json=jsonb_set(jsonb_set(coalesce(invu.header_snapshot_json,'{}'::jsonb),
      '{meta,timesheet_count}',to_jsonb(v_hdr_meta_timesheet_count),true),
      '{meta,segment_count}',to_jsonb(v_hdr_meta_segment_count),true),
    updated_at=now()
  where invu.id=p_invoice_id;

-- Refresh invoice-level NHSP/HR cache after timesheet/segment changes
  if coalesce(v_refresh_hr_cache,false) = true then
    perform 1
    from public.invoice_source_rows_collect(p_invoice_id, true) sr
    limit 1;
  end if;

  -- Revalidate ownership after every line and segment move in this Save. A
  -- source edit must not queue work for a source that was detached later in the
  -- same transaction, and every edited SEGMENTS identity must still be locked
  -- to this exact invoice.
  if cardinality(v_source_changed_ts_ids)>0 and exists(
    select 1
    from jsonb_each(v_source_updates_map) desired
    where (desired.value->>'timesheet_id')::uuid=any(v_source_changed_ts_ids)
      and not exists(
        select 1 from public.invoice_lines carrier
        where carrier.invoice_id=p_invoice_id
          and (
            carrier.timesheet_id=(desired.value->>'timesheet_id')::uuid
            or (
              carrier.timesheet_id is null
              and coalesce(carrier.meta_json->>'timesheet_id','')~
                '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
              and (carrier.meta_json->>'timesheet_id')::uuid=
                (desired.value->>'timesheet_id')::uuid
            )
          )
      )
      and not exists(
        select 1
        from public.timesheets_financials tf
        cross join lateral jsonb_array_elements(
          coalesce(tf.invoice_breakdown_json->'segments','[]'::jsonb)) seg(value)
        where tf.timesheet_id=(desired.value->>'timesheet_id')::uuid
          and tf.is_current
          and nullif(btrim(coalesce(
            seg.value->>'invoice_locked_invoice_id','')),'')=p_invoice_id::text
      )
  ) then
    raise exception using errcode='22023',
      message='INVOICE_SOURCE_EDIT_CONFLICTING_COMMAND',
      detail=jsonb_build_object('reason','POST_EDIT_SOURCE_DETACHED')::text;
  end if;

  if cardinality(v_source_changed_ts_ids)>0 and exists(
    select 1
    from jsonb_each(v_source_updates_map) desired
    cross join lateral jsonb_array_elements(
      coalesce(desired.value->'segment_updates','[]'::jsonb)) requested(value)
    where desired.value->>'source_mode'='SEGMENTS'
      and not exists(
        select 1
        from public.timesheets_financials tf
        cross join lateral jsonb_array_elements(
          coalesce(tf.invoice_breakdown_json->'segments','[]'::jsonb)) seg(value)
        where tf.id=(desired.value->>'financial_id')::uuid
          and tf.is_current
          and tf.timesheet_id=(desired.value->>'timesheet_id')::uuid
          and nullif(btrim(coalesce(
            seg.value->>'invoice_locked_invoice_id','')),'')=p_invoice_id::text
          and (
            (
              nullif(requested.value->>'segment_id','') is not null
              and nullif(btrim(coalesce(seg.value->>'segment_id','')),'')
                =nullif(requested.value->>'segment_id','')
            ) or (
              nullif(requested.value->>'segment_id','') is null
              and coalesce(seg.value->>'start_utc',seg.value->>'start')
                is not distinct from requested.value->>'start_identity'
              and coalesce(seg.value->>'end_utc',seg.value->>'end')
                is not distinct from requested.value->>'end_identity'
            )
          )
      )
  ) then
    raise exception using errcode='22023',
      message='INVOICE_SOURCE_EDIT_CONFLICTING_COMMAND',
      detail=jsonb_build_object('reason','POST_EDIT_SEGMENT_DETACHED')::text;
  end if;

  -- A material source edit always starts or reuses the exact replacement
  -- timesheet work. Replacement of an existing invoice preview is derived from
  -- the locked pre-edit V8 state; browser flags cannot suppress it.
  if cardinality(v_source_changed_ts_ids)>0 then
    select coalesce(jsonb_agg(jsonb_build_object(
      'command_type','VIEW_TIMESHEET_DOCUMENT',
      'timesheet_id',changed_id,
      'purpose','TIMESHEET',
      'priority_reason','SOURCE_EDIT_REPLACEMENT',
      'template_version','timesheet-professional-v2')
      order by changed_id),'[]'::jsonb)
    into v_document_commands
    from unnest(v_source_changed_ts_ids) changed_id;

    v_source_edit_invoice_replacement_required:=
      v_source_edit_preexisting_preview
      or lower(coalesce(p_payload->>'request_preview','false'))
           in('true','1','yes','on');
    if v_source_edit_invoice_replacement_required then
      v_document_commands:=v_document_commands||jsonb_build_array(
        jsonb_build_object(
          'command_type','VIEW_INVOICE_DOCUMENT',
          'invoice_id',p_invoice_id,
          'purpose','DRAFT_PREVIEW',
          'priority_reason',case when v_source_edit_preexisting_preview
            then 'SOURCE_EDIT_REPLACEMENT' else 'VIEW_NOW' end,
          'template_version','invoice-professional-v2'));
    end if;

    v_expected_command_count:=jsonb_array_length(v_document_commands);
    if v_expected_command_count>1000 then
      raise exception using errcode='22023',
        message='INVOICE_SOURCE_EDIT_TOO_MANY_REPLACEMENTS';
    end if;

    v_started_operations:=public.invoice_operation_start_batch(
      v_document_commands,p_actor_user_id,v_now);

    if jsonb_typeof(v_started_operations)<>'array'
       or jsonb_array_length(v_started_operations)<>v_expected_command_count then
      raise exception using errcode='55000',
        message='SOURCE_EDIT_REPLACEMENT_RESULT_INVALID',
        detail=jsonb_build_object(
          'reason','RESULT_COUNT',
          'expected',v_expected_command_count,
          'actual',case when jsonb_typeof(v_started_operations)='array'
            then jsonb_array_length(v_started_operations) else null end)::text;
    end if;

    for v_command_no in 1..v_expected_command_count loop
      select value into v_operation_result
      from jsonb_array_elements(v_started_operations) value
      where coalesce(value->>'command_no','')~'^[1-9][0-9]*$'
        and (value->>'command_no')::int=v_command_no;

      if not found or (
        select count(*)
        from jsonb_array_elements(v_started_operations) value
        where coalesce(value->>'command_no','')~'^[1-9][0-9]*$'
          and (value->>'command_no')::int=v_command_no
      )<>1 then
        raise exception using errcode='55000',
          message='SOURCE_EDIT_REPLACEMENT_RESULT_INVALID',
          detail=jsonb_build_object('reason','COMMAND_NO','command_no',v_command_no)::text;
      end if;

      v_source_update:=v_document_commands->(v_command_no-1);
      v_command_type:=v_source_update->>'command_type';
      if v_operation_result->>'command_type' is distinct from v_command_type
         or lower(coalesce(v_operation_result->>'accepted','false'))
              not in('true','1','yes','on')
         or lower(coalesce(v_operation_result->>'blocked','false'))
              in('true','1','yes','on') then
        raise exception using errcode='55000',
          message=case
            when lower(coalesce(v_operation_result->>'blocked','false'))
                   in('true','1','yes','on')
              then 'SOURCE_EDIT_REPLACEMENT_BLOCKED'
            else 'SOURCE_EDIT_REPLACEMENT_REJECTED' end,
          detail=jsonb_build_object(
            'command_no',v_command_no,
            'terminal_error',v_operation_result->'terminal_error')::text;
      end if;

      v_operation_status:=upper(coalesce(v_operation_result->>'status',''));
      v_operation_id:=case when coalesce(v_operation_result->>'operation_id','')~
        '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
        then (v_operation_result->>'operation_id')::uuid end;
      v_document_version_id:=case when coalesce(v_operation_result->>'document_version_id','')~
        '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
        then (v_operation_result->>'document_version_id')::uuid end;

      if v_operation_status='READY' then
        if v_document_version_id is null or v_operation_id is not null
           or not exists(
             select 1
             from public.invoice_document_versions dv
             where dv.id=v_document_version_id
               and dv.entity_type=case when v_command_type='VIEW_TIMESHEET_DOCUMENT'
                 then 'TIMESHEET' else 'INVOICE' end
               and dv.entity_id=case when v_command_type='VIEW_TIMESHEET_DOCUMENT'
                 then (v_source_update->>'timesheet_id')::uuid else p_invoice_id end
               and dv.purpose=case when v_command_type='VIEW_TIMESHEET_DOCUMENT'
                 then 'TIMESHEET' else 'DRAFT_PREVIEW' end
               and dv.source_revision=case when v_command_type='VIEW_TIMESHEET_DOCUMENT'
                 then (
                   select t.document_revision::text from public.timesheets t
                   where t.timesheet_id=(v_source_update->>'timesheet_id')::uuid
                     and t.is_current)
                 else (
                   select i.document_revision::text from public.invoices i
                   where i.id=p_invoice_id)
                 end
               and dv.template_version=v_source_update->>'template_version'
               and dv.status='READY'
               and nullif(btrim(coalesce(dv.r2_key,'')),'') is not null
               and nullif(btrim(coalesce(dv.sha256,'')),'') is not null
               and coalesce(dv.size_bytes,0)>0
               and coalesce(dv.page_count,0)>0
           ) then
          raise exception using errcode='55000',
            message='SOURCE_EDIT_REPLACEMENT_RESULT_INVALID',
            detail=jsonb_build_object(
              'reason','READY_IDENTITY','command_no',v_command_no)::text;
        end if;
      else
        if v_operation_id is null
           or v_operation_status not in('QUEUED','RUNNING','WAITING','RETRY_WAIT')
           or not exists(
             select 1
             from public.invoice_operations op
             where op.id=v_operation_id
               and op.operation_type='BUILD_DOCUMENT'
               and op.entity_type=case when v_command_type='VIEW_TIMESHEET_DOCUMENT'
                 then 'TIMESHEET' else 'INVOICE' end
               and op.entity_id=case when v_command_type='VIEW_TIMESHEET_DOCUMENT'
                 then (v_source_update->>'timesheet_id')::uuid else p_invoice_id end
               and op.source_revision=case when v_command_type='VIEW_TIMESHEET_DOCUMENT'
                 then (
                   select t.document_revision::text from public.timesheets t
                   where t.timesheet_id=(v_source_update->>'timesheet_id')::uuid
                     and t.is_current)
                 else (
                   select i.document_revision::text from public.invoices i
                   where i.id=p_invoice_id)
                 end
               and op.template_version=v_source_update->>'template_version'
               and coalesce(op.input_json->>'purpose','')=
                 case when v_command_type='VIEW_TIMESHEET_DOCUMENT'
                   then 'TIMESHEET' else 'DRAFT_PREVIEW' end
               and op.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT')
           ) then
          raise exception using errcode='55000',
            message='SOURCE_EDIT_REPLACEMENT_RESULT_INVALID',
            detail=jsonb_build_object(
              'reason','ACTIVE_IDENTITY','command_no',v_command_no)::text;
        end if;
      end if;

      v_validated_operations:=v_validated_operations||jsonb_build_array(v_operation_result);
    end loop;

    select coalesce(jsonb_agg(jsonb_build_object(
      'timesheet_id',t.timesheet_id,
      'document_revision',t.document_revision)
      order by t.timesheet_id),'[]'::jsonb)
    into v_source_changed_revisions
    from public.timesheets t
    where t.timesheet_id=any(v_source_changed_ts_ids) and t.is_current;
  end if;

-- Return compact state only; never build a manifest or PDF here.
  select jsonb_build_object('invoice_id',i.id,'status',i.status,
    'subtotal_ex_vat',i.subtotal_ex_vat,'vat_amount',i.vat_amount,'total_inc_vat',i.total_inc_vat,
    'document_revision',i.document_revision,'document_state',i.document_state,
    'preview_document_version_id',i.preview_document_version_id,
    'active_document_operation_id',i.active_document_operation_id,
    'issue_state',i.issue_state,'active_issue_operation_id',i.active_issue_operation_id,
    'reference_updates_applied',v_refupd_applied,
    'timesheet_location_updates_applied',v_location_applied,
    'timesheet_source_changed',
      cardinality(v_source_changed_ts_ids)>0,
    'source_edit_queue_contract',case when v_source_edit_requested
      then 'INVOICE_SOURCE_EDIT_QUEUE_V1' else null end,
    'changed_timesheet_ids',coalesce(to_jsonb(v_source_changed_ts_ids),'[]'::jsonb),
    'changed_timesheet_revisions',coalesce(v_source_changed_revisions,'[]'::jsonb),
    'source_edit_preexisting_preview',v_source_edit_preexisting_preview,
    'invoice_replacement_required',v_source_edit_invoice_replacement_required,
    'accepted_operations',coalesce(v_validated_operations,'[]'::jsonb),
    'document_queue_requested',jsonb_array_length(v_document_commands)>0)
  into v_manifest from public.invoices i where i.id=p_invoice_id;

  if v_invoice_debug then
    begin
      -- attach finish marker (avoid extra heavy queries here)
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','finish',
          'at_utc', public._inv_iso_utc(now()),
          'invoice_total_charge_ex_vat', v_new_ex,
          'invoice_vat_amount', v_new_vat,
          'invoice_total_inc_vat', v_new_inc
        )
      );

      v_dbg_stats := v_dbg_stats || jsonb_build_object(
        'lines_deleted', v_dbg_lines_deleted,
        'timesheets_unlocked_via_line_removal', v_dbg_timesheets_unlocked,
        'segment_add_refs', v_dbg_seg_add_refs,
        'segment_remove_refs', v_dbg_seg_remove_refs,
        'segment_tsfin_count', v_dbg_seg_tsfins,
        'segment_timesheets_rebuilt', v_dbg_seg_timesheets_rebuilt,
        'segment_timesheets_removed', v_dbg_seg_timesheets_removed,
        'add_timesheets_found', v_dbg_add_timesheets_found,
        'add_timesheets_skipped', v_dbg_add_timesheets_skipped
      );

      perform public._inv_write_audit(
        p_actor_user_id,
        'INVOICE_APPLY_EDITS_DEBUG',
        jsonb_build_object(
          'invoice_id', p_invoice_id::text,
          'week_start', v_week_start::text,
          'week_end', v_week_end::text,
          'stats', v_dbg_stats,
          'steps', v_dbg_steps
        ),
        'invoices',
        p_invoice_id::text,
        null,
        'INVOICE_DEBUG',
        null, null, null
      );
    exception when others then
      null;
    end;
  end if;

  perform public._ctms_assert_invoice_correction_lines_v1(
    p_invoice_id,p_actor_user_id,false,'INVOICE_APPLY_EDITS_RESULT'
  );

  IF v_correction_placement_ts_id IS NOT NULL THEN
    v_correction_placement_scope:=public.invoice_correction_pair_scope_v1(
      v_correction_placement_ts_id,null::uuid,p_actor_user_id,false,100);
    IF COALESCE((v_correction_placement_scope->>'valid')::boolean,false) IS NOT TRUE
       OR v_correction_placement_scope->>'placement_state' NOT IN (
         'COMPLETE_SAME_INVOICE','COMPLETE_SPLIT_INVOICES','INCOMPLETE_MOVE','UNPLACED') THEN
      RAISE EXCEPTION 'INVOICE_CORRECTION_PAIR_PLACEMENT_RESULT_INVALID'
        USING ERRCODE='P0001',DETAIL=v_correction_placement_scope::text;
    END IF;
    v_correction_placement_states:=jsonb_build_array(jsonb_build_object(
      'correction_id',v_correction_placement_scope->>'correction_id',
      'pair_timesheet_ids',v_correction_placement_scope->'pair_timesheet_ids',
      'placement_state',v_correction_placement_scope->>'placement_state',
      'missing_member_kind',v_correction_placement_scope->>'missing_member_kind',
      'placement_invoices',v_correction_placement_scope->'placement_invoices'));
    v_manifest:=v_manifest||jsonb_build_object(
      'correction_pair_placement_states',v_correction_placement_states);
  END IF;

  return v_manifest;

exception when others then
  v_dbg_sqlstate := SQLSTATE;
  v_dbg_error := SQLERRM;

  if v_invoice_debug then
    begin
      perform public._inv_write_audit(
        p_actor_user_id,
        'INVOICE_APPLY_EDITS_ERROR',
        jsonb_build_object(
          'invoice_id', coalesce(p_invoice_id::text,''),
          'sqlstate', v_dbg_sqlstate,
          'error', v_dbg_error,
          'stats', v_dbg_stats,
          'steps', v_dbg_steps
        ),
        'invoices',
        coalesce(p_invoice_id::text,''),
        null,
        'INVOICE_DEBUG',
        null, null, null
      );
    exception when others then
      null;
    end;
  end if;

  raise;
end;
$function$;

-- invoice_async_contract_get_v2()
CREATE OR REPLACE FUNCTION public.invoice_async_contract_get_v2()
 RETURNS jsonb
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'vault', 'pg_temp'
AS $function$
  with required(identity,procedure_identity) as (
    values
      ('private._invoice_batch_canonical_text_v2(jsonb)',
       to_regprocedure('private._invoice_batch_canonical_text_v2(jsonb)')),
      ('private._invoice_batch_hash_v2(jsonb)',
       to_regprocedure('private._invoice_batch_hash_v2(jsonb)')),
      ('private._invoice_candidate_snapshot_get_v2(text,timestamptz)',
       to_regprocedure('private._invoice_candidate_snapshot_get_v2(text,timestamp with time zone)')),
      ('private._invoice_candidate_snapshot_verify_v2(text,jsonb,timestamptz)',
       to_regprocedure('private._invoice_candidate_snapshot_verify_v2(text,jsonb,timestamp with time zone)')),
      ('private._invoice_candidate_snapshot_bump_v2(boolean,boolean,text,text,timestamptz)',
       to_regprocedure('private._invoice_candidate_snapshot_bump_v2(boolean,boolean,text,text,timestamp with time zone)')),
      ('private._invoice_jsonb_pick_v2(jsonb,text[])',
       to_regprocedure('private._invoice_jsonb_pick_v2(jsonb,text[])')),
      ('private._invoice_candidate_revision_trigger_v2()',
       to_regprocedure('private._invoice_candidate_revision_trigger_v2()')),
      ('private._invoice_result_page_revision_trigger_v2()',
       to_regprocedure('private._invoice_result_page_revision_trigger_v2()')),
      ('private._invoice_current_chunk_ids_v2(uuid[],integer)',
       to_regprocedure('private._invoice_current_chunk_ids_v2(uuid[],integer)')),
      ('private._invoice_batch_selection_rules_v2(jsonb)',
       to_regprocedure('private._invoice_batch_selection_rules_v2(jsonb)')),
      ('private._invoice_batch_query_validate_v2(jsonb,text)',
       to_regprocedure('private._invoice_batch_query_validate_v2(jsonb,text)')),
      ('private._invoice_generation_resolve_command_groups(jsonb,uuid,timestamptz)',
       to_regprocedure('private._invoice_generation_resolve_command_groups(jsonb,uuid,timestamp with time zone)')),
      ('private._invoice_batch_generate_classification_v2(boolean,text[],timestamptz)',
       to_regprocedure('private._invoice_batch_generate_classification_v2(boolean,text[],timestamp with time zone)')),
      ('private._invoice_batch_generate_candidate_keys_v2(jsonb,timestamptz)',
       to_regprocedure('private._invoice_batch_generate_candidate_keys_v2(jsonb,timestamp with time zone)')),
      ('private._invoice_batch_generate_group_rows_v2(boolean,integer,text[],timestamptz)',
       to_regprocedure('private._invoice_batch_generate_group_rows_v2(boolean,integer,text[],timestamp with time zone)')),
      ('private._invoice_batch_generate_candidate_rows_v2(jsonb,timestamptz)',
       to_regprocedure('private._invoice_batch_generate_candidate_rows_v2(jsonb,timestamp with time zone)')),
      ('private._invoice_batch_issue_source_rows_v2(boolean,integer,timestamptz)',
       to_regprocedure('private._invoice_batch_issue_source_rows_v2(boolean,integer,timestamp with time zone)')),
      ('private._invoice_batch_issue_source_rows_core_v2(boolean,integer,timestamptz,uuid[])',
       to_regprocedure('private._invoice_batch_issue_source_rows_core_v2(boolean,integer,timestamp with time zone,uuid[])')),
      ('private._invoice_batch_issue_source_rows_for_ids_v2(uuid[],boolean,timestamptz)',
       to_regprocedure('private._invoice_batch_issue_source_rows_for_ids_v2(uuid[],boolean,timestamp with time zone)')),
      ('private._invoice_batch_issue_classification_v2(boolean,uuid[],timestamptz)',
       to_regprocedure('private._invoice_batch_issue_classification_v2(boolean,uuid[],timestamp with time zone)')),
      ('private._invoice_batch_issue_candidate_keys_v2(jsonb,timestamptz)',
       to_regprocedure('private._invoice_batch_issue_candidate_keys_v2(jsonb,timestamp with time zone)')),
      ('private._invoice_batch_issue_candidate_rows_v2(jsonb,timestamptz)',
       to_regprocedure('private._invoice_batch_issue_candidate_rows_v2(jsonb,timestamp with time zone)')),
      ('private._invoice_operation_start_core_v8(jsonb,uuid,timestamptz)',
       to_regprocedure('private._invoice_operation_start_core_v8(jsonb,uuid,timestamp with time zone)')),
      ('private._invoice_generation_advance_core_v8(jsonb,timestamptz)',
       to_regprocedure('private._invoice_generation_advance_core_v8(jsonb,timestamp with time zone)')),
      ('private._invoice_issue_advance_core_v8(jsonb,timestamptz)',
       to_regprocedure('private._invoice_issue_advance_core_v8(jsonb,timestamp with time zone)')),
      ('private._invoice_operation_rollup_core_v8(uuid[],timestamptz,boolean)',
       to_regprocedure('private._invoice_operation_rollup_core_v8(uuid[],timestamp with time zone,boolean)')),
      ('private._invoice_operation_get_core_v8(uuid[],uuid,text)',
       to_regprocedure('private._invoice_operation_get_core_v8(uuid[],uuid,text)')),
      ('private._invoice_batch_manifest_advance_v2(jsonb,text,timestamptz)',
       to_regprocedure('private._invoice_batch_manifest_advance_v2(jsonb,text,timestamp with time zone)')),
      ('private._invoice_dispatch_advance_batch(jsonb,timestamptz)',
       to_regprocedure('private._invoice_dispatch_advance_batch(jsonb,timestamp with time zone)')),
      ('private._invoice_candidate_triggers_install_v2()',
       to_regprocedure('private._invoice_candidate_triggers_install_v2()')),
      ('public.invoice_batch_generate_candidates(jsonb)',
       to_regprocedure('public.invoice_batch_generate_candidates(jsonb)')),
      ('public.invoice_batch_issue_candidates(jsonb)',
       to_regprocedure('public.invoice_batch_issue_candidates(jsonb)')),
      ('public.invoice_operation_start_batch(jsonb,uuid,timestamptz)',
       to_regprocedure('public.invoice_operation_start_batch(jsonb,uuid,timestamp with time zone)')),
      ('public.invoice_work_claim_batch(text[],text,integer,integer,timestamptz)',
       to_regprocedure('public.invoice_work_claim_batch(text[],text,integer,integer,timestamp with time zone)')),
      ('public.invoice_operation_advance_batch(jsonb,timestamptz)',
       to_regprocedure('public.invoice_operation_advance_batch(jsonb,timestamp with time zone)')),
      ('private._invoice_generation_advance_batch(jsonb,timestamptz)',
       to_regprocedure('private._invoice_generation_advance_batch(jsonb,timestamp with time zone)')),
      ('private._invoice_issue_advance_batch(jsonb,timestamptz)',
       to_regprocedure('private._invoice_issue_advance_batch(jsonb,timestamp with time zone)')),
      ('private._invoice_operation_rollup_batch(uuid[],timestamptz,boolean)',
       to_regprocedure('private._invoice_operation_rollup_batch(uuid[],timestamp with time zone,boolean)')),
      ('public.invoice_operation_get(uuid[],uuid,text,jsonb)',
       to_regprocedure('public.invoice_operation_get(uuid[],uuid,text,jsonb)')),
      ('public.invoice_operation_control_batch(jsonb,uuid,timestamptz)',
       to_regprocedure('public.invoice_operation_control_batch(jsonb,uuid,timestamp with time zone)'))
  ),
  definitions as (
    select
      required.identity,
      required.procedure_identity,
      case
        when required.procedure_identity is not null
          then pg_get_functiondef(required.procedure_identity::oid)
      end definition
    from required
  ),
  manifest as (
    select
      count(*) filter (
        where procedure_identity is null
      ) missing_count,
      count(*) filter (
        where definition like '%_legacy_20260726%'
      ) forbidden_dependency_count,
      count(*) filter (
        where identity like 'private._invoice_batch_%candidate%rows_v2(%'
          and (
            definition like '%public.invoice_batch_generate_candidates(%'
            or definition like '%public.invoice_batch_issue_candidates(%'
          )
      ) public_candidate_dependency_count,
      encode(
        extensions.digest(
          convert_to(
            string_agg(
              identity||E'\n'||coalesce(definition,'MISSING'),
              E'\n--\n'
              order by identity
            ),
            'UTF8'
          ),
          'sha256'
        ),
        'hex'
      ) function_hash_manifest
    from definitions
  ),
  security_state as (
    select count(*) filter (
      where definitions.identity like 'private.%'
        and (
          has_function_privilege(
            'public',
            definitions.procedure_identity,
            'EXECUTE'
          )
          or has_function_privilege(
            'anon',
            definitions.procedure_identity,
            'EXECUTE'
          )
          or has_function_privilege(
            'authenticated',
            definitions.procedure_identity,
            'EXECUTE'
          )
        )
    ) private_exposure_count
    from definitions
    where definitions.procedure_identity is not null
  ),
  legacy_surface_state as (
    select
      count(*) filter (
      where legacy.procedure_identity is not null
        and (
          has_function_privilege(
            'public',
            legacy.procedure_identity,
            'EXECUTE'
          )
          or has_function_privilege(
            'anon',
            legacy.procedure_identity,
            'EXECUTE'
          )
          or has_function_privilege(
            'authenticated',
            legacy.procedure_identity,
            'EXECUTE'
          )
          or has_function_privilege(
            'service_role',
            legacy.procedure_identity,
            'EXECUTE'
          )
        )
      ) legacy_runtime_exposure_count,
      count(*) filter (
        where legacy.procedure_identity is not null
          and coalesce(p.pronargdefaults, 0) <> 0
      ) legacy_rest_overload_ambiguity_count
    from (
      values
        (to_regprocedure(
          'public.invoice_batch_generate_candidates(boolean,integer,text[],jsonb)'
        )),
        (to_regprocedure(
          'public.invoice_batch_issue_candidates(boolean,integer,jsonb)'
        ))
    ) legacy(procedure_identity)
    left join pg_proc p on p.oid = legacy.procedure_identity
  ),
  key_state as (
    select exists (
      select 1
      from private.invoice_async_snapshot_hmac_keys k
      join vault.decrypted_secrets s on s.id=k.vault_secret_id
      where k.is_current
        and k.active_from_utc<=statement_timestamp()
        and (k.active_to_utc is null or k.active_to_utc>statement_timestamp())
        and nullif(s.decrypted_secret,'') is not null
    ) snapshot_key_ready
  ),
  trigger_state as (
    select
      count(*) filter (
        where left(
                t.tgname,
                length('trg_invoice_candidate_revision_v2_')
              ) = 'trg_invoice_candidate_revision_v2_'
          and p.oid=to_regprocedure(
            'private._invoice_candidate_revision_trigger_v2()'
          )::oid
      ) candidate_trigger_count,
      count(*) filter (
        where left(
                t.tgname,
                length('trg_invoice_result_page_revision_v2_')
              ) = 'trg_invoice_result_page_revision_v2_'
          and p.oid=to_regprocedure(
            'private._invoice_result_page_revision_trigger_v2()'
          )::oid
      ) result_trigger_count
    from pg_trigger t
    join pg_proc p on p.oid=t.tgfoid
    where not t.tgisinternal
  ),
  index_state as (
    select bool_and(coalesce(i.indisvalid,false) and coalesce(i.indisready,false))
      indexes_ready
    from (
      values
        ('idx_invoice_manifest_carrier_identity_v8'),
        ('idx_invoice_batch_result_all_v8'),
        ('idx_invoice_batch_result_category_v8'),
        ('idx_invoice_operation_chunks_claim_v8'),
        ('idx_invoice_operation_control_receipt_actor_token_v8')
    ) required(index_name)
    left join pg_class c
      on c.relname=required.index_name
     and c.relnamespace='public'::regnamespace
    left join pg_index i on i.indexrelid=c.oid
  )
  select jsonb_build_object(
    'contract_version','INVOICE_ASYNC_DB_V2',
    'ready',
      manifest.missing_count=0
      and manifest.forbidden_dependency_count=0
      and manifest.public_candidate_dependency_count=0
      and security_state.private_exposure_count=0
      and legacy_surface_state.legacy_runtime_exposure_count=0
      and legacy_surface_state.legacy_rest_overload_ambiguity_count=0
      and key_state.snapshot_key_ready
      and trigger_state.candidate_trigger_count=54
      and trigger_state.result_trigger_count=3
      and coalesce(index_state.indexes_ready,false),
    'candidate_query_contract','INVOICE_BATCH_QUERY_V2',
    'candidate_response_contract','INVOICE_BATCH_CANDIDATES_V2',
    'selection_contract','INVOICE_BATCH_SELECTION_V2',
    'selection_root_contract','INVOICE_BATCH_SELECTION_ROOT_V2',
    'progress_contract','INVOICE_BATCH_PROGRESS_V2',
    'function_hash_manifest',manifest.function_hash_manifest,
    'missing_function_count',manifest.missing_count,
    'forbidden_dependency_count',manifest.forbidden_dependency_count,
    'public_candidate_dependency_count',
      manifest.public_candidate_dependency_count,
    'private_exposure_count',security_state.private_exposure_count,
    'legacy_runtime_exposure_count',
      legacy_surface_state.legacy_runtime_exposure_count,
    'legacy_rest_overload_ambiguity_count',
      legacy_surface_state.legacy_rest_overload_ambiguity_count,
    'trigger_manifest_digest',
      '6777e9b09109c11ff7de3227658d43d876b50a9d3403cfc1809e23696923c973',
    'snapshot_signing_ready',key_state.snapshot_key_ready,
    'candidate_trigger_count',trigger_state.candidate_trigger_count,
    'result_trigger_count',trigger_state.result_trigger_count,
    'operation_control_idempotency_ready',
      coalesce(index_state.indexes_ready,false),
    'indexes_ready',coalesce(index_state.indexes_ready,false)
  )
  from manifest
  cross join security_state
  cross join legacy_surface_state
  cross join key_state
  cross join trigger_state
  cross join index_state;
$function$;

-- invoice_autoinvoice_candidate_groups(integer)
CREATE OR REPLACE FUNCTION public.invoice_autoinvoice_candidate_groups(p_limit integer DEFAULT 5000)
 RETURNS TABLE(client_id uuid, invoice_week_start date, source_ids uuid[], source_revision_hash text, consolidation_mode text, stream text, auto_invoice_policy_origin text, canonical_source_members jsonb, eligible_for_submission boolean, blocker_code text, correction_validation jsonb)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
with anchor as materialized (
  select now() evaluation_utc,(now() at time zone 'Europe/London')::date today
),
eligible as materialized (
  select distinct tf.timesheet_id,
    case when con.id is not null then 'CONTRACT' else 'CLIENT_DEFAULT' end policy_origin
  from public.timesheets_financials tf
  join public.timesheets ts
    on ts.timesheet_id=tf.timesheet_id and ts.is_current and ts.revoked_at is null
  left join lateral (
    select w.contract_id from public.contract_weeks w
    where w.timesheet_id=tf.timesheet_id
    order by w.updated_at desc nulls last,w.id desc limit 1
  ) cw on true
  left join public.contracts con on con.id=coalesce(ts.contract_id,cw.contract_id)
  left join lateral (
    select s.* from public.client_settings s cross join anchor a
    where s.client_id=tf.client_id
      and(s.effective_from is null or s.effective_from<=a.today)
    order by s.effective_from desc nulls last,s.updated_at desc nulls last,
      s.created_at desc nulls last,s.id desc
    limit 1
  ) cs on true
  cross join anchor a
  where tf.is_current and tf.processing_status='READY_FOR_INVOICE'
    and not tf.is_stale and tf.locked_by_invoice_id is null
    and tf.paid_at_utc is null and tf.client_id is not null
    and ts.week_ending_date is not null
    and not(
      upper(coalesce(ts.submission_mode::text,''))='QR'
      and(nullif(ts.qr_signed_hash,'') is null
        or ts.qr_signed_at_utc is null))
    and(
      (coalesce(tf.mileage_pay_ex_vat,0)=0
        and coalesce(tf.mileage_charge_ex_vat,0)=0)
      or exists(
        select 1 from public.timesheet_evidence e
        join public.invoice_document_assets asset
          on asset.id=e.document_asset_id
         and asset.status not in(
           'UNSUPPORTED','CORRUPT','MISSING','FAILED','SUPERSEDED')
        where e.timesheet_id=tf.timesheet_id
          and upper(coalesce(e.kind,''))='MILEAGE'
          and nullif(e.storage_key,'') is not null))
    and(
      (coalesce(tf.expenses_pay_ex_vat,0)=0
        and coalesce(tf.expenses_charge_ex_vat,0)=0
        and coalesce(tf.travel_pay_ex_vat,0)=0
        and coalesce(tf.travel_charge_ex_vat,0)=0
        and coalesce(tf.accommodation_pay_ex_vat,0)=0
        and coalesce(tf.accommodation_charge_ex_vat,0)=0)
      or exists(
        select 1 from public.timesheet_evidence e
        join public.invoice_document_assets asset
          on asset.id=e.document_asset_id
         and asset.status not in(
           'UNSUPPORTED','CORRUPT','MISSING','FAILED','SUPERSEDED')
        where e.timesheet_id=tf.timesheet_id
          and upper(coalesce(e.kind,'')) in(
            'TRAVEL','ACCOMMODATION','OTHER','EXPENSE','EXPENSES')
          and nullif(e.storage_key,'') is not null))
    and not exists(
      select 1
      from public.timesheet_evidence e
      join public.invoice_document_assets asset
        on asset.id=e.document_asset_id
      where e.timesheet_id=tf.timesheet_id
        and asset.status in(
          'UNSUPPORTED','CORRUPT','MISSING','FAILED','SUPERSEDED'))
    and case when con.id is not null
      then coalesce(con.auto_invoice,cs.auto_invoice_default,false)
      else coalesce(cs.auto_invoice_default,false) end
    and exists(
      select 1
      from lateral (
        select null::jsonb segment
        where coalesce(tf.invoice_breakdown_json->>'mode','')<>'SEGMENTS'
          or jsonb_array_length(case
            when jsonb_typeof(tf.invoice_breakdown_json->'segments')='array'
              then tf.invoice_breakdown_json->'segments' else '[]'::jsonb end)=0
        union all
        select x.value
        from jsonb_array_elements(case
          when jsonb_typeof(tf.invoice_breakdown_json->'segments')='array'
            then tf.invoice_breakdown_json->'segments' else '[]'::jsonb end) x(value)
        where coalesce(tf.invoice_breakdown_json->>'mode','')='SEGMENTS'
          and nullif(x.value->>'invoice_locked_invoice_id','') is null
      ) available
      where case
        when pg_input_is_valid(
          coalesce(available.segment->>'invoice_target_week_start',''),'date')
          then case
            when(available.segment->>'invoice_target_week_start')::date<>
                (ts.week_ending_date-6)
              then(available.segment->>'invoice_target_week_start')::date<=a.today
            else ts.week_ending_date<a.today
          end
        else ts.week_ending_date<a.today end)
  order by tf.timesheet_id
),
command as materialized (
  select jsonb_build_array(jsonb_build_object(
    'command_type','GENERATE_AUTO','source_ids',
    coalesce(jsonb_agg(e.timesheet_id order by e.timesheet_id),'[]'::jsonb),
    'allow_early',false)) commands
  from eligible e
),
resolved_unchecked as materialized (
  select r.*
  from command c cross join anchor a
  cross join lateral private._invoice_generation_resolve_command_groups(
    c.commands,null,a.evaluation_utc) r
  where r.blocker_code is null
),
vat_policy as materialized (
  select v.*
  from private._invoice_generation_vat_policy_batch(coalesce((
    select jsonb_agg(jsonb_build_object(
      'source_member_key',m.value->>'source_member_key',
      'source_type',m.value->>'source_type',
      'source_id',m.value->>'source_id',
      'timesheet_id',m.value->>'related_timesheet_id',
      'segment_id',m.value->>'segment_id',
      'effective_date',r.effective_settings_date)
      order by r.group_key,m.value->>'source_member_key')
    from resolved_unchecked r
    cross join lateral jsonb_array_elements(
      r.canonical_source_members) m(value)
  ),'[]'::jsonb)) v
),
reference_policy as materialized (
  select ref.*
  from private._invoice_source_reference_validate_batch(coalesce((
    select jsonb_agg(jsonb_build_object(
      'source_member_key',m.value->>'source_member_key',
      'source_type',m.value->>'source_type',
      'source_id',m.value->>'source_id',
      'related_timesheet_id',m.value->>'related_timesheet_id',
      'segment_id',m.value->>'segment_id',
      'target_invoice_week',r.target_invoice_week,
      'invoice_stream',r.invoice_stream,
      'consolidation_mode',r.consolidation_mode)
      order by r.group_key,m.value->>'source_member_key')
    from resolved_unchecked r
    cross join lateral jsonb_array_elements(
      r.canonical_source_members) m(value)
  ),'[]'::jsonb)) ref
),
correction_scopes as materialized (
  select coalesce(jsonb_agg(jsonb_build_object(
    'request_key','autoinvoice-candidate:'||r.group_key,
    'scope_key',r.group_key,
    'validation_purpose','AUTOMATIC_CANDIDATE',
    'expected_client_id',r.client_id,
    'expected_contract_id',case when cardinality(r.contract_ids)=1
      then r.contract_ids[1] end,
    'natural_source_week',case when cardinality(r.natural_source_weeks)=1
      then r.natural_source_weeks[1] end,
    'target_invoice_week',r.target_invoice_week,
    'expected_invoice_stream',r.invoice_stream,
    'planned_members',coalesce((select jsonb_agg(jsonb_build_object(
      'timesheet_id',m.value->>'related_timesheet_id',
      'source_type',m.value->>'source_type',
      'source_id',m.value->>'source_id',
      'source_member_key',m.value->>'source_member_key',
      'segment_id',m.value->>'segment_id',
      'target_invoice_week',r.target_invoice_week,
      'vat_rate_pct',v.vat_rate)
      order by m.value->>'source_member_key')
      from jsonb_array_elements(r.canonical_source_members) m(value)
      left join vat_policy v
        on v.source_member_key=m.value->>'source_member_key'),
      '[]'::jsonb)) order by r.group_key),'[]'::jsonb) scopes
  from resolved_unchecked r
),
correction_validation as materialized (
  select c.*
  from correction_scopes s
  cross join lateral private._invoice_correction_validate_batch(
    s.scopes,(select today from anchor)) c
),
correction_group_results as materialized (
  select c.scope_key group_key,c.valid,c.blocker_code,c.blocker_codes,
    c.detail_json details
  from correction_validation c
),resolved as materialized (
  select r.*
  from resolved_unchecked r
  where not exists(
    select 1
    from jsonb_array_elements(r.canonical_source_members) m(value)
    left join vat_policy v
      on v.source_member_key=m.value->>'source_member_key'
    left join reference_policy ref
      on ref.source_member_key=m.value->>'source_member_key'
    left join public.v_ts_invoice_precheck pc
      on pc.timesheet_id=case when pg_input_is_valid(
        coalesce(m.value->>'related_timesheet_id',
          m.value->>'source_id'),'uuid')
        then coalesce(m.value->>'related_timesheet_id',
          m.value->>'source_id')::uuid end
    where coalesce(v.valid,false) is not true
       or(coalesce(pc.require_reference_to_invoice,false)
          and coalesce(ref.reference_ready,false) is not true))
),
policy as materialized (
  select r.group_key,
    case when bool_or(e.policy_origin='CONTRACT') then 'CONTRACT'
      else 'CLIENT_DEFAULT' end policy_origin
  from resolved r
  cross join unnest(r.canonical_source_ids) source_id
  join eligible e on e.timesheet_id=source_id
  group by r.group_key
)
select r.client_id,r.target_invoice_week,r.canonical_source_ids,
  r.source_revision_hash,r.consolidation_mode,r.invoice_stream,p.policy_origin,
  r.canonical_source_members,
  coalesce(c.valid,true),
  case when coalesce(c.valid,true) then null
    else coalesce(c.blocker_code,'INVOICE_CORRECTION_UNIT_INVALID') end,
  coalesce(c.details,'[]'::jsonb)
from resolved r join policy p using(group_key)
left join correction_group_results c using(group_key)
where not exists(
  select 1 from public.invoice_operation_chunks c
  join public.invoice_operations o on o.id=c.operation_id
  where c.chunk_type='GENERATION_GROUP'
    and c.payload_json->>'group_key'=r.group_key
    and c.payload_json->>'source_revision'=r.source_revision_hash
    and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
    and o.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED'))
order by r.target_invoice_week nulls last,r.client_id,r.invoice_stream,r.group_key
limit greatest(0,least(coalesce(p_limit,5000),20000));
$function$;

-- invoice_batch_generate_candidates(boolean,integer,text[],jsonb)
CREATE OR REPLACE FUNCTION public.invoice_batch_generate_candidates(p_allow_early boolean, p_limit integer, p_scope_keys text[], p_query jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
DECLARE
  v_query jsonb;
  v_page_size integer;
BEGIN
  IF p_query IS NULL THEN
    IF to_regprocedure('private._invoice_batch_generate_candidates_legacy_20260726(boolean, integer, text[])') IS NULL THEN
      RAISE EXCEPTION USING
        errcode = '42883',
        message = 'LEGACY_INVOICE_BATCH_GENERATE_CANDIDATES_MISSING';
    END IF;

    RETURN private._invoice_batch_generate_candidates_legacy_20260726(
      p_allow_early,
      p_limit,
      p_scope_keys
    );
  END IF;

  IF jsonb_typeof(p_query) IS DISTINCT FROM 'object' THEN
    RAISE EXCEPTION USING errcode = '22023', message = 'BATCH_QUERY_INVALID';
  END IF;

  IF coalesce(p_query->>'contract_version', 'INVOICE_BATCH_QUERY_V1') <> 'INVOICE_BATCH_QUERY_V1' THEN
    RAISE EXCEPTION USING errcode = '22023', message = 'BATCH_QUERY_INVALID';
  END IF;

  IF upper(coalesce(nullif(p_query->>'action', ''), 'GENERATE')) <> 'GENERATE' THEN
    RAISE EXCEPTION USING errcode = '22023', message = 'BATCH_QUERY_ACTION_MISMATCH';
  END IF;

  v_page_size := CASE
    WHEN coalesce(p_query->>'page_size','') ~ '^[1-9][0-9]{0,8}$'
      THEN greatest(1, least((p_query->>'page_size')::integer, 100))
    WHEN p_limit IS NOT NULL
      THEN greatest(1, least(p_limit, 100))
    ELSE 100
  END;

  v_query := (p_query - 'action')
    || jsonb_build_object(
      'contract_version', 'INVOICE_BATCH_QUERY_V1',
      'action', 'GENERATE',
      'allow_early', CASE
        WHEN p_query ? 'allow_early' THEN lower(coalesce(p_query->>'allow_early','false')) IN ('true','t','1','yes','on')
        WHEN coalesce(p_query#>>'{filters,allow_early}','') <> '' THEN lower(coalesce(p_query#>>'{filters,allow_early}','false')) IN ('true','t','1','yes','on')
        ELSE coalesce(p_allow_early, false)
      END,
      'page_size', v_page_size
    );

  RETURN private._invoice_batch_generate_candidate_rows_v1(v_query, now());
END;
$function$;

-- invoice_batch_generate_candidates(jsonb)
CREATE OR REPLACE FUNCTION public.invoice_batch_generate_candidates(p_query jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
declare
  v_query jsonb := coalesce(p_query,'{}'::jsonb);
  v_selection jsonb;
begin
  if coalesce(auth.role(),'') <> 'service_role'
     and not exists (
       select 1
       from public.tms_users u
       where u.id = auth.uid()
         and u.is_active
         and lower(btrim(coalesce(u.role,''))) = 'admin'
     ) then
    raise exception using
      errcode='42501',
      message='Active administrator or service role required';
  end if;

  if jsonb_typeof(v_query) is distinct from 'object' then
    raise exception using errcode='22023', message='INVOICE_BATCH_QUERY_INVALID';
  end if;

  if octet_length(convert_to(v_query::text,'UTF8')) > 4194304 then
    raise exception using errcode='54000', message='BATCH_REQUEST_TOO_LARGE';
  end if;

  v_selection := coalesce(v_query->'selection','{}'::jsonb);
  if octet_length(convert_to(
    private._invoice_batch_canonical_text_v2(v_selection),
    'UTF8'
  )) > 3145728 then
    raise exception using
      errcode='54000',
      message='BATCH_SELECTION_PAYLOAD_TOO_LARGE';
  end if;

  return private._invoice_batch_generate_candidate_rows_v2(v_query,now());
end;
$function$;

-- invoice_batch_issue_candidates(jsonb)
CREATE OR REPLACE FUNCTION public.invoice_batch_issue_candidates(p_query jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
declare
  v_query jsonb := coalesce(p_query,'{}'::jsonb);
  v_selection jsonb;
begin
  if coalesce(auth.role(),'') <> 'service_role'
     and not exists (
       select 1
       from public.tms_users u
       where u.id = auth.uid()
         and u.is_active
         and lower(btrim(coalesce(u.role,''))) = 'admin'
     ) then
    raise exception using
      errcode='42501',
      message='Active administrator or service role required';
  end if;

  if jsonb_typeof(v_query) is distinct from 'object' then
    raise exception using errcode='22023', message='INVOICE_BATCH_QUERY_INVALID';
  end if;

  if octet_length(convert_to(v_query::text,'UTF8')) > 4194304 then
    raise exception using errcode='54000', message='BATCH_REQUEST_TOO_LARGE';
  end if;

  v_selection := coalesce(v_query->'selection','{}'::jsonb);
  if octet_length(convert_to(
    private._invoice_batch_canonical_text_v2(v_selection),
    'UTF8'
  )) > 3145728 then
    raise exception using
      errcode='54000',
      message='BATCH_SELECTION_PAYLOAD_TOO_LARGE';
  end if;

  return private._invoice_batch_issue_candidate_rows_v2(v_query,now());
end;
$function$;

-- invoice_closeout_zero_charge_timesheets(uuid[],uuid)
CREATE OR REPLACE FUNCTION public.invoice_closeout_zero_charge_timesheets(p_timesheet_ids uuid[], p_actor_user_id uuid)
 RETURNS TABLE(client_id uuid, invoice_id uuid, timesheet_ids uuid[], ok boolean, warnings jsonb)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_invoice_debug boolean := false;
  v_steps jsonb := '[]'::jsonb;
  v_run_started timestamptz := now();
  v_now timestamptz;
  v_anchor_ymd date;

  v_client_id uuid;
  v_invoice_id uuid;

  v_def record;
  v_client record;
  v_terms_days int;
  v_vat_rate numeric;
  v_client_vat_override numeric;
  v_vat_chargeable boolean;
  v_due_at timestamptz;
  v_stationery_key text;
  v_margins jsonb;
  v_hide_bank_footer boolean;

  v_header jsonb;

  v_ts_ids_client uuid[];
  v_seg_refs jsonb := '[]'::jsonb;

  r_ts record;
  r_seg jsonb;

  v_skipped jsonb := '[]'::jsonb;
  v_skipped_count int := 0;
  v_created_count int := 0;

  v_err_state text;
  v_err_msg text;
begin
  -- Validate
  if p_timesheet_ids is null or coalesce(array_length(p_timesheet_ids,1),0) = 0 then
    return;
  end if;

  -- Load invoice_debug flag (safe even if column not yet present)
  begin
    select coalesce(sd.invoice_debug, false)
    into v_invoice_debug
    from public.settings_defaults sd
    where sd.id = 1
    limit 1;
  exception when undefined_column then
    v_invoice_debug := false;
  end;

  v_steps := v_steps || jsonb_build_array(jsonb_build_object(
    'step','start',
    'timesheet_count', coalesce(array_length(p_timesheet_ids,1),0),
    'now_utc', public._inv_iso_utc(v_run_started)
  ));

  -- Build eligible set into temp table
  create temporary table if not exists pg_temp._inv_closeout_ts (
    timesheet_id uuid primary key,
    tsfin_id uuid not null,
    client_id uuid not null,
    booking_id text null,
    basis text null,
    total_charge_ex_vat numeric null,
    invoice_breakdown_json jsonb null
  ) on commit drop;

  truncate pg_temp._inv_closeout_ts;

  insert into pg_temp._inv_closeout_ts(timesheet_id, tsfin_id, client_id, booking_id, basis, total_charge_ex_vat, invoice_breakdown_json)
  select
    tf.timesheet_id,
    tf.id as tsfin_id,
    tf.client_id,
    ts.booking_id::text,
    tf.basis::text,
    coalesce(tf.total_charge_ex_vat,0)::numeric,
    tf.invoice_breakdown_json
  from public.timesheets_financials tf
  join public.timesheets ts
    on ts.timesheet_id = tf.timesheet_id
   and ts.is_current = true
  where tf.is_current = true
    and tf.timesheet_id = any(p_timesheet_ids)
    and tf.client_id is not null
    and tf.locked_by_invoice_id is null
    and public._inv_round2(coalesce(tf.total_charge_ex_vat,0)) = 0;

  -- Skip any timesheets not inserted (missing/locked/non-zero)
  v_skipped := v_skipped || coalesce(
    (
      select jsonb_agg(
        jsonb_build_object(
          'timesheet_id', t::text,
          'reason', 'NOT_ELIGIBLE_OR_NOT_FOUND'
        )
      )
      from unnest(p_timesheet_ids) t
      where not exists (select 1 from pg_temp._inv_closeout_ts x where x.timesheet_id = t)
    ),
    '[]'::jsonb
  );

  v_steps := v_steps || jsonb_build_array(jsonb_build_object(
    'step','eligible_loaded',
    'eligible_count', (select count(*) from pg_temp._inv_closeout_ts),
    'skipped_so_far', jsonb_array_length(v_skipped)
  ));

  -- For SEGMENTS snapshots, ensure no segment is already locked (safety)
  delete from pg_temp._inv_closeout_ts x
  where x.invoice_breakdown_json is not null
    and upper(coalesce(x.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
    and jsonb_typeof(x.invoice_breakdown_json->'segments') = 'array'
    and exists (
      select 1
      from jsonb_array_elements(x.invoice_breakdown_json->'segments') s(value)
      where nullif(btrim(coalesce(s.value->>'invoice_locked_invoice_id','')), '') is not null
    )
  returning timesheet_id into r_ts;

  -- NOTE: The above DELETE ... RETURNING can return multiple rows; capture into skipped list via a separate query
  v_skipped := v_skipped || coalesce(
    (
      select jsonb_agg(
        jsonb_build_object(
          'timesheet_id', x.timesheet_id::text,
          'reason', 'SEGMENTS_ALREADY_LOCKED'
        )
      )
      from public.timesheets_financials tf
      join pg_temp._inv_closeout_ts tmp on tmp.tsfin_id = tf.id
      where false
    ),
    '[]'::jsonb
  );

  -- Create closeout invoices per client_id
  for v_client_id in
    select distinct x.client_id
    from pg_temp._inv_closeout_ts x
    order by x.client_id
  loop
    v_now := now();
    v_anchor_ymd := (v_now at time zone 'Europe/London')::date;

    select array_agg(x.timesheet_id)
    into v_ts_ids_client
    from pg_temp._inv_closeout_ts x
    where x.client_id = v_client_id;

    if v_ts_ids_client is null or coalesce(array_length(v_ts_ids_client,1),0) = 0 then
      continue;
    end if;

    -- Load global defaults / finance settings
    select *
    into v_def
    from public.settings_finance_pick(v_anchor_ymd)
    limit 1;

    -- Load client
    select
      c.id,
      c.name,
      c.invoice_address,
      c.primary_invoice_email,
      coalesce(c.vat_chargeable,true) as vat_chargeable,
      coalesce(c.payment_terms_days,30) as payment_terms_days
    into v_client
    from public.clients c
    where c.id = v_client_id
    limit 1;

    if not found then
      v_skipped := v_skipped || jsonb_build_array(jsonb_build_object(
        'client_id', v_client_id::text,
        'timesheet_ids', to_jsonb(v_ts_ids_client),
        'reason', 'CLIENT_NOT_FOUND'
      ));
      continue;
    end if;

    v_vat_chargeable := coalesce(v_client.vat_chargeable,true);
    v_terms_days := coalesce(v_client.payment_terms_days,30);

    -- VAT rate
    v_vat_rate := coalesce(v_def.vat_rate_pct, 20);
    begin
      select cs.vat_rate_pct
      into v_client_vat_override
      from public.client_settings cs
      where cs.client_id = v_client_id
        and cs.effective_from <= v_anchor_ymd
      order by cs.effective_from desc
      limit 1;
    exception when others then
      v_client_vat_override := null;
    end;

    v_vat_rate := case
      when v_vat_chargeable = false then 0
      else coalesce(v_client_vat_override, v_vat_rate, 20)
    end;

    v_due_at := v_now + make_interval(days => v_terms_days);

    -- Stationery defaults (match generator default)
    v_stationery_key := 'Assets/Stationery/Letterhead/A4/Letterhead_v1@300dpi.png';
    v_margins := coalesce(v_def.stationery_margins_mm, jsonb_build_object('top',12,'right',12,'bottom',12,'left',12));
    v_hide_bank_footer := coalesce(v_def.hide_bank_footer, false);

    v_header := jsonb_build_object(
      'client_id', v_client_id::text,
      'client_name', v_client.name,
      'client_invoice_address', v_client.invoice_address,
      'client_primary_invoice_email', v_client.primary_invoice_email,
      'vat_chargeable', v_vat_chargeable,
      'applied_vat_rate_pct', v_vat_rate,
      'payment_terms_days', v_terms_days,
      'issued_at_utc', to_jsonb(v_now),
      'due_at_utc', to_jsonb(v_due_at),
      'stationery_key', v_stationery_key,
      'stationery_margins_mm', v_margins,
      'hide_bank_footer', v_hide_bank_footer,
      'bank', jsonb_build_object(
        'name', v_def.bank_name,
        'sort_code', v_def.bank_sort_code,
        'account_number', v_def.bank_account_number
      ),
      'vat_registration_number', v_def.vat_registration_number,
      'meta', jsonb_build_object(
        'source', 'CLOSEOUT',
        'closeout', true,
        'do_not_send', true,
        'timesheet_count', coalesce(array_length(v_ts_ids_client,1),0),
        'vat_anchor_ymd', v_anchor_ymd::text
      ),
      'attach_policy', jsonb_build_object(
        'requires_hr', false,
        'hr_attach_to_invoice', true,
        'ts_attach_to_invoice', true
      )
    );

    insert into public.invoices(
      client_id,
      type,
      status,
      status_date_utc,
      issued_at_utc,
      due_at_utc,
      subtotal_ex_vat,
      vat_amount,
      total_inc_vat,
      header_snapshot_json,
      do_not_send
    )
    values (
      v_client_id,
      'INVOICE'::public.invoice_type_enum,
      'ISSUED'::public.invoice_status_enum,
      v_now,
      v_now,
      v_due_at,
      0,
      0,
      0,
      v_header,
      true
    )
    returning id into v_invoice_id;

    v_created_count := v_created_count + 1;

    -- Insert one CLOSEOUT line per timesheet (0 totals, timesheet_id set for visibility)
    for r_ts in
      select x.timesheet_id, x.booking_id
      from pg_temp._inv_closeout_ts x
      where x.client_id = v_client_id
      order by x.timesheet_id
    loop
      insert into public.invoice_lines(
        invoice_id, timesheet_id, booking_id, description,
        hours_day, hours_night, hours_sat, hours_sun, hours_bh,
        pay_day, pay_night, pay_sat, pay_sun, pay_bh,
        charge_day, charge_night, charge_sat, charge_sun, charge_bh,
        total_pay_ex_vat, total_charge_ex_vat, margin_ex_vat,
        vat_rate_pct, vat_amount, total_inc_vat,
        paper_ts_r2_key, meta_json, source_key
      )
      values (
        v_invoice_id,
        r_ts.timesheet_id,
        nullif(btrim(coalesce(r_ts.booking_id,'')), ''),
        'Zero-charge closeout (do not send)',
        0,0,0,0,0,
        null,null,null,null,null,
        null,null,null,null,null,
        0,0,0,
        v_vat_rate, 0, 0,
        ('docs-pdf/timesheets/ts_' || r_ts.timesheet_id::text || '.pdf'),
        jsonb_build_object(
          'line_type','CLOSEOUT',
          'closeout', true,
          'do_not_send', true,
          'timesheet_id', r_ts.timesheet_id::text
        ),
        ('CLOSEOUT:TS:' || r_ts.timesheet_id::text)
      )
      on conflict (invoice_id, source_key) do nothing;
    end loop;

    -- Lock timesheets (segment-aware)
    v_seg_refs := '[]'::jsonb;
    for r_ts in
      select x.tsfin_id, x.timesheet_id, x.invoice_breakdown_json
      from pg_temp._inv_closeout_ts x
      where x.client_id = v_client_id
      order by x.timesheet_id
    loop
      if r_ts.invoice_breakdown_json is not null
        and upper(coalesce(r_ts.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
        and jsonb_typeof(r_ts.invoice_breakdown_json->'segments') = 'array'
        and jsonb_array_length(r_ts.invoice_breakdown_json->'segments') > 0
      then
        for r_seg in
          select value
          from jsonb_array_elements(r_ts.invoice_breakdown_json->'segments') value
        loop
          v_seg_refs := v_seg_refs || jsonb_build_array(
            jsonb_build_object(
              'tsfin_id', r_ts.tsfin_id::text,
              'segment_id', nullif(btrim(coalesce(r_seg->>'segment_id','')), '')
            )
          );
        end loop;
      else
        -- Lock whole snapshot (covers non-segments and SEGMENTS with empty/invalid segments array)
        v_seg_refs := v_seg_refs || jsonb_build_array(
          jsonb_build_object(
            'tsfin_id', r_ts.tsfin_id::text,
            'segment_id', null
          )
        );
      end if;
    end loop;

    if jsonb_typeof(v_seg_refs) = 'array' and jsonb_array_length(v_seg_refs) > 0 then
      perform public._inv_lock_segments_for_invoice(v_invoice_id, v_seg_refs);
    end if;

    -- Mark contract weeks INVOICED for these timesheets
    update public.contract_weeks cw
    set status = 'INVOICED'::public.contract_week_status_enum
    where cw.timesheet_id = any(v_ts_ids_client);

    -- Recompute totals (stays 0)
    perform public.invoice_recompute_totals(v_invoice_id);

    -- Standard audit event
    perform public._audit_insert(
      'invoice',
      v_invoice_id::text,
      'INVOICE_CLOSEOUT_CREATED',
      null,
      jsonb_build_object(
        'client_id', v_client_id::text,
        'timesheet_ids', to_jsonb(v_ts_ids_client),
        'do_not_send', true,
        'status', 'ISSUED'
      ),
      null,
      p_actor_user_id
    );

    -- Return
    client_id := v_client_id;
    invoice_id := v_invoice_id;
    timesheet_ids := v_ts_ids_client;
    ok := true;
    warnings := null;
    return next;
  end loop;

  -- Final debug write (one row)
  if v_invoice_debug then
    perform public._inv_write_audit(
      p_actor_user_id,
      'INVOICE_CLOSEOUT_DEBUG',
      jsonb_build_object(
        'run_started_at_utc', public._inv_iso_utc(v_run_started),
        'run_finished_at_utc', public._inv_iso_utc(now()),
        'timesheet_ids', to_jsonb(p_timesheet_ids),
        'created_invoice_count', v_created_count,
        'skipped', v_skipped,
        'steps', v_steps
      ),
      'invoices',
      ('closeout:' || public._inv_iso_utc(v_run_started)),
      null,
      'INVOICE_DEBUG',
      null,
      null,
      null
    );
  end if;

exception when others then
  v_err_state := sqlstate;
  v_err_msg := sqlerrm;

  if v_invoice_debug then
    begin
      perform public._inv_write_audit(
        p_actor_user_id,
        'INVOICE_CLOSEOUT_ERROR',
        jsonb_build_object(
          'run_started_at_utc', public._inv_iso_utc(v_run_started),
          'run_failed_at_utc', public._inv_iso_utc(now()),
          'timesheet_ids', to_jsonb(p_timesheet_ids),
          'sqlstate', v_err_state,
          'error', v_err_msg,
          'steps', v_steps,
          'skipped', v_skipped
        ),
        'invoices',
        ('closeout:' || public._inv_iso_utc(v_run_started)),
        null,
        'INVOICE_DEBUG',
        null,
        null,
        null
      );
    exception when others then
      -- never block rethrow due to debug
      null;
    end;
  end if;

  raise;
end;
$function$;

-- invoice_correction_pair_scope_v1(uuid,uuid,uuid,boolean,integer)
CREATE OR REPLACE FUNCTION public.invoice_correction_pair_scope_v1(p_timesheet_id uuid, p_target_invoice_id uuid DEFAULT NULL::uuid, p_actor_user_id uuid DEFAULT NULL::uuid, p_lock_rows boolean DEFAULT true, p_max_members integer DEFAULT 100)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_chain jsonb;
  v_unit jsonb;
  v_envelope jsonb;
  v_ids uuid[]:=array[]::uuid[];
  v_expected_count integer;
  v_ready_count integer:=0;
  v_line_member_count integer:=0;
  v_line_invoice_count integer:=0;
  v_client_count integer:=0;
  v_contract_count integer:=0;
  v_week_count integer:=0;
  v_stream_count integer:=0;
  v_target public.invoices%rowtype;
  v_rows jsonb:='[]'::jsonb;
  v_errors jsonb:='[]'::jsonb;
  r record;
  v_leg jsonb;
  v_policy_ready boolean;
  v_expected_stream text;
  v_current_stream text;
  v_target_stream text;
  v_line_policy_mismatch_count integer:=0;
  v_reversal_line_count integer:=0;
  v_replacement_line_count integer:=0;
  v_reversal_invoice_ids uuid[]:=array[]::uuid[];
  v_replacement_invoice_ids uuid[]:=array[]::uuid[];
  v_placement_state text:='MALFORMED_PAIR';
  v_placement_compatible boolean:=false;
  v_placement_invoices jsonb:='[]'::jsonb;
  v_compatibility_mode text;
  v_operation public.import_apply_operations%rowtype;
  v_operation_unit jsonb;
  v_operation_unit_count integer:=0;
  v_balance jsonb;
  v_balance_row_count integer:=0;
  v_timesheet public.timesheets%rowtype;
  v_pair_correction_id text;
  v_pair_parent_id uuid;
  v_pair_operation_id uuid;
  v_pair_unit_fingerprint text;
  v_pair_source_identity text;
  v_pair_envelope_count integer:=0;
  v_pair_parent_count integer:=0;
  v_pair_member_count integer:=0;
  v_pair_reversal_count integer:=0;
  v_pair_replacement_count integer:=0;
  v_missing_ids uuid[]:=array[]::uuid[];
  v_recomputed_unit_fingerprint text;
begin
  if p_timesheet_id is null then raise exception 'INVOICE_CORRECTION_TIMESHEET_ID_REQUIRED' using errcode='22023'; end if;
  if p_max_members<1 or p_max_members>100 then raise exception 'INVOICE_CORRECTION_MEMBER_LIMIT_INVALID' using errcode='22023'; end if;
  if p_actor_user_id is not null then
    perform 1 from public.tms_users u where u.id=p_actor_user_id and coalesce(u.is_active,false);
    if not found then raise exception 'INVOICE_CORRECTION_ACTOR_INVALID' using errcode='42501'; end if;
  end if;

  v_chain:=public.timesheet_correction_chain_scope_v1(p_timesheet_id,p_lock_rows,32,p_max_members);
  if coalesce((v_chain->>'valid')::boolean,false) is not true then
    -- The ordinary whole-chain validator remains authoritative.  This narrowly
    -- gated fallback exists only for a current pair committed by the reviewed
    -- authoritative-import reconciliation path when an older issued row was
    -- physically removed but its frozen invoice evidence remains provable.
    select * into v_timesheet from public.timesheets
    where timesheet_id=p_timesheet_id and is_current and archived_at_utc is null;
    if v_timesheet.timesheet_id is null
       or not coalesce(v_timesheet.is_adjustment,false)
       or upper(coalesce(v_timesheet.adjustment_origin,''))<>'IMPORT_CORRECTION'
       or v_timesheet.correction_kind not in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
       or v_timesheet.correction_id is null
       or coalesce(v_timesheet.candidate_hint_text#>>'{import_authoritative_reconciliation,operation_id}','')
          !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' then
      raise exception 'INVOICE_CORRECTION_CHAIN_INVALID' using errcode='P0001',detail=v_chain::text;
    end if;
    v_pair_correction_id:=v_timesheet.correction_id;
    v_pair_operation_id:=(v_timesheet.candidate_hint_text#>>'{import_authoritative_reconciliation,operation_id}')::uuid;
    v_pair_unit_fingerprint:=nullif(v_timesheet.candidate_hint_text#>>'{import_authoritative_reconciliation,unit_fingerprint}','');
    v_pair_source_identity:=nullif(v_timesheet.candidate_hint_text#>>'{import_authoritative_reconciliation,source_identity}','');

    select * into v_operation from public.import_apply_operations o
    where o.id=v_pair_operation_id and o.state='COMPLETE' and o.committed_at_utc is not null;
    if v_operation.id is null then
      raise exception 'INVOICE_CORRECTION_CHAIN_INVALID' using errcode='P0001',detail=v_chain::text;
    end if;
    select count(*)::integer,min(u::text)::jsonb into v_operation_unit_count,v_operation_unit
    from jsonb_array_elements(case when jsonb_typeof(v_operation.response_json->'reconciliation_units')='array'
      then v_operation.response_json->'reconciliation_units' else '[]'::jsonb end) u
    where u->>'schema_version'='IMPORT_AUTHORITATIVE_RECONCILIATION_V1'
      and u->>'correction_id'=v_pair_correction_id
      and u->>'source_identity'=v_pair_source_identity
      and u->>'unit_fingerprint'=v_pair_unit_fingerprint
      and u->>'route' in ('AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT');
    if v_operation_unit_count<>1 then
      raise exception 'INVOICE_CORRECTION_CHAIN_INVALID' using errcode='P0001',detail=v_chain::text;
    end if;

    select public._import_review_hash_v1(concat_ws('|','unit-v1',
      v_operation_unit->>'action_id',v_operation_unit->>'source_identity',v_operation_unit->>'route',
      v_operation_unit->>'reconciliation_fingerprint',outcome.evidence_fingerprint))
    into v_recomputed_unit_fingerprint
    from public.import_review_action_outcomes outcome
    where outcome.operation_id=v_operation.id and outcome.action_id=v_operation_unit->>'action_id';
    if v_recomputed_unit_fingerprint is distinct from v_operation_unit->>'unit_fingerprint' then
      raise exception 'INVOICE_CORRECTION_RECONCILIATION_STALE' using errcode='40001';
    end if;

    select count(*)::integer,
      count(*) filter(where t.correction_kind='CHANGED_HOURS_REVERSAL')::integer,
      count(*) filter(where t.correction_kind='CHANGED_HOURS_REPLACEMENT')::integer,
      count(distinct t.parent_timesheet_id)::integer,
      min(t.parent_timesheet_id),
      count(distinct coalesce(
        t.candidate_hint_text#>>'{correction_financials_policy_envelope,envelope_fingerprint}',
        tf.policy_snapshot_json#>>'{correction_financials_policy_envelope,envelope_fingerprint}'
      ))::integer,
      coalesce(array_agg(t.timesheet_id order by t.correction_kind,t.timesheet_id),array[]::uuid[])
    into v_pair_member_count,v_pair_reversal_count,v_pair_replacement_count,
      v_pair_parent_count,v_pair_parent_id,v_pair_envelope_count,v_ids
    from public.timesheets t
    left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
    where t.correction_id=v_pair_correction_id and t.is_current and t.archived_at_utc is null
      and t.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')
      and t.adjustment_origin='IMPORT_CORRECTION'
      and t.candidate_hint_text#>>'{import_authoritative_reconciliation,operation_id}'=v_operation.id::text
      and t.candidate_hint_text#>>'{import_authoritative_reconciliation,unit_fingerprint}'=v_pair_unit_fingerprint
      and t.candidate_hint_text#>>'{import_authoritative_reconciliation,source_identity}'=v_pair_source_identity;
    if v_pair_member_count<>2 or v_pair_reversal_count<>1 or v_pair_replacement_count<>1
       or v_pair_parent_count<>1 or v_pair_parent_id is null or v_pair_envelope_count<>1
       or v_pair_parent_id is distinct from (v_operation_unit->>'parent_timesheet_id')::uuid
       or not exists(select 1 from public.timesheets parent_ts where parent_ts.timesheet_id=v_pair_parent_id
          and parent_ts.is_current and parent_ts.archived_at_utc is null) then
      raise exception 'INVOICE_CORRECTION_CHAIN_INVALID' using errcode='P0001',detail=v_chain::text;
    end if;
    if exists(select 1 from public.timesheets t left join public.timesheets_financials tf
        on tf.timesheet_id=t.timesheet_id and tf.is_current
      where t.timesheet_id=any(v_ids) and (
        t.contract_id is distinct from (v_operation_unit->>'contract_id')::uuid
        or t.week_ending_date is distinct from (v_operation_unit->>'week_ending_date')::date
        or tf.candidate_id is distinct from (v_operation_unit->>'candidate_id')::uuid
        or tf.client_id is distinct from (v_operation_unit->>'client_id')::uuid
        or jsonb_typeof(t.actual_schedule_json)<>'array'
        or jsonb_array_length(t.actual_schedule_json)<>1
        or not t.actual_schedule_json @> jsonb_build_array(jsonb_build_object(
          'shift_id',v_operation_unit->>'source_shift_id','external_row_key',v_operation_unit->>'source_identity'))
        or (t.correction_kind='CHANGED_HOURS_REVERSAL' and (
          (t.actual_schedule_json#>>'{0,start_utc}')::timestamptz is distinct from (v_operation_unit#>>'{B_standard_schedule_json,0,start_utc}')::timestamptz
          or (t.actual_schedule_json#>>'{0,end_utc}')::timestamptz is distinct from (v_operation_unit#>>'{B_standard_schedule_json,0,end_utc}')::timestamptz
          or coalesce((t.actual_schedule_json#>>'{0,break_mins}')::integer,0) is distinct from coalesce((v_operation_unit#>>'{B_standard_schedule_json,0,break_mins}')::integer,0)))
        or (t.correction_kind='CHANGED_HOURS_REPLACEMENT' and (
          (t.actual_schedule_json#>>'{0,start_utc}')::timestamptz is distinct from (v_operation_unit#>>'{A_schedule_json,0,start_utc}')::timestamptz
          or (t.actual_schedule_json#>>'{0,end_utc}')::timestamptz is distinct from (v_operation_unit#>>'{A_schedule_json,0,end_utc}')::timestamptz
          or coalesce((t.actual_schedule_json#>>'{0,break_mins}')::integer,0) is distinct from coalesce((v_operation_unit#>>'{A_schedule_json,0,break_mins}')::integer,0)))
      )) then
      raise exception 'INVOICE_CORRECTION_RECONCILIATION_STALE' using errcode='40001';
    end if;

    select coalesce(array_agg(x.value::uuid order by x.value::uuid),array[]::uuid[]) into v_missing_ids
    from jsonb_array_elements_text(coalesce(v_operation_unit->'historical_missing_timesheet_ids','[]'::jsonb)) x(value);
    if cardinality(v_missing_ids)=0
       or exists(select 1 from unnest(v_missing_ids) missing_id
          where exists(select 1 from public.timesheets t where t.timesheet_id=missing_id)
             or not exists(select 1 from public.invoice_lines il join public.invoices i on i.id=il.invoice_id
               where (il.timesheet_id=missing_id or il.meta_json->>'timesheet_id'=missing_id::text)
                 and i.status in ('ISSUED','PAID','ON_HOLD') and i.issued_at_utc is not null)
             or not exists(select 1 from public.audit_events ae where ae.object_type='timesheets'
               and ae.object_id_text=missing_id::text
               and ae.action in ('NHSP_IMPORT_CORRECTION_APPLIED','HR_IMPORT_CORRECTION_APPLIED')))
       or exists(select 1 from jsonb_array_elements(coalesce(v_chain->'errors','[]'::jsonb)) e
          where e->>'code'<>'CORRECTION_UNIT_INVALID') then
      raise exception 'INVOICE_CORRECTION_CHAIN_INVALID' using errcode='P0001',detail=v_chain::text;
    end if;

    select count(*)::integer,min(b.balance_json::text)::jsonb into v_balance_row_count,v_balance
    from public._import_review_effective_invoice_balance_core_v1(
      v_operation.import_id,
      jsonb_build_array(jsonb_build_object(
        'source_identity',v_operation_unit->>'source_identity','source_system',v_operation_unit->>'source_system',
        'source_shift_id',v_operation_unit->>'source_shift_id','external_row_key',v_operation_unit->>'source_identity',
        'hr_row_id',v_operation_unit->>'hr_row_id','source_timesheet_id',v_operation_unit->>'source_timesheet_id',
        'candidate_id',v_operation_unit->>'candidate_id','client_id',v_operation_unit->>'client_id',
        'contract_id',v_operation_unit->>'contract_id','week_ending_date',v_operation_unit->>'week_ending_date',
        'invoice_stream',v_operation_unit->>'invoice_stream','authoritative_import_id',v_operation.import_id,
        'authoritative_schedule_json',v_operation_unit->'A_schedule_json','authoritative_hours',v_operation_unit->'A_hours'
      )),1,512,256,128
    ) b;
    if v_balance_row_count<>1 or nullif(v_balance->>'blocking_code','') is not null
       or v_balance->>'effective_invoice_fingerprint' is distinct from v_operation_unit->>'B_invoice_fingerprint'
       or v_balance->'historical_missing_timesheet_ids' is distinct from to_jsonb(v_missing_ids) then
      raise exception 'INVOICE_CORRECTION_RECONCILIATION_STALE' using errcode='40001';
    end if;
    if exists(select 1 from public.timesheets t join public.timesheets_financials tf
        on tf.timesheet_id=t.timesheet_id and tf.is_current
      where t.timesheet_id=any(v_ids) and (
        coalesce(tf.is_stale,true) or coalesce(tf.has_rate_issue,false) or coalesce(tf.has_pay_channel_issue,false)
        or (t.correction_kind='CHANGED_HOURS_REVERSAL' and (
          tf.hours_day<>-coalesce((v_operation_unit#>>'{B_hours,hours_day}')::numeric,0)
          or tf.hours_night<>-coalesce((v_operation_unit#>>'{B_hours,hours_night}')::numeric,0)
          or tf.hours_sat<>-coalesce((v_operation_unit#>>'{B_hours,hours_sat}')::numeric,0)
          or tf.hours_sun<>-coalesce((v_operation_unit#>>'{B_hours,hours_sun}')::numeric,0)
          or tf.hours_bh<>-coalesce((v_operation_unit#>>'{B_hours,hours_bh}')::numeric,0)
          or tf.total_pay_ex_vat<>-coalesce((v_operation_unit#>>'{B_financials,pay_ex_vat}')::numeric,0)
          or tf.total_charge_ex_vat<>-coalesce((v_operation_unit#>>'{B_financials,charge_ex_vat}')::numeric,0)
          or tf.margin_ex_vat<>-coalesce((v_operation_unit#>>'{B_financials,margin_ex_vat}')::numeric,0)))
        or (t.correction_kind='CHANGED_HOURS_REPLACEMENT' and (
          tf.hours_day<>coalesce((v_operation_unit#>>'{A_hours,hours_day}')::numeric,0)
          or tf.hours_night<>coalesce((v_operation_unit#>>'{A_hours,hours_night}')::numeric,0)
          or tf.hours_sat<>coalesce((v_operation_unit#>>'{A_hours,hours_sat}')::numeric,0)
          or tf.hours_sun<>coalesce((v_operation_unit#>>'{A_hours,hours_sun}')::numeric,0)
          or tf.hours_bh<>coalesce((v_operation_unit#>>'{A_hours,hours_bh}')::numeric,0)))
      )) then
      raise exception 'INVOICE_CORRECTION_RECONCILIATION_STALE' using errcode='40001';
    end if;

    v_envelope:=public._ctms_correction_policy_envelope_read_v1(p_timesheet_id);
    v_unit:=jsonb_build_object('valid',true,'correction_id',v_pair_correction_id,
      'correction_shape','REVERSAL_REPLACEMENT','expected_member_count',2,
      'member_ids',to_jsonb(v_ids),'policy_envelope',v_envelope);
    v_chain:=jsonb_build_object('root_timesheet_id',v_envelope->>'root_timesheet_id');
    v_compatibility_mode:='IMPORT_AUTHORITATIVE_RECONCILIATION_V1';
  else
    v_unit:=v_chain->'requested_correction_unit';
  end if;
  if jsonb_typeof(v_unit)<>'object' or coalesce((v_unit->>'valid')::boolean,false) is not true then
    raise exception 'INVOICE_CORRECTION_UNIT_INVALID' using errcode='P0001';
  end if;
  v_envelope:=v_unit->'policy_envelope';
  v_expected_stream:=upper(btrim(coalesce(v_envelope->>'invoice_stream','')));
  if v_expected_stream not in ('NORMAL','SELF_BILL') then
    raise exception 'INVOICE_CORRECTION_FROZEN_STREAM_INVALID' using errcode='P0001';
  end if;
  v_expected_count:=(v_unit->>'expected_member_count')::integer;
  select coalesce(array_agg(value::uuid order by value::text),array[]::uuid[])
  into v_ids from jsonb_array_elements_text(v_unit->'member_ids');
  if cardinality(v_ids)<>v_expected_count then raise exception 'INVOICE_CORRECTION_MEMBER_COUNT_MISMATCH' using errcode='P0001'; end if;

  if p_lock_rows then
    perform 1 from public.timesheets ts where ts.timesheet_id=any(v_ids) order by ts.timesheet_id for update;
    perform 1 from public.timesheets_financials tf where tf.timesheet_id=any(v_ids) and tf.is_current=true
      order by tf.timesheet_id,tf.id for update;
  end if;

  if p_target_invoice_id is not null then
    if p_lock_rows then select * into v_target from public.invoices where id=p_target_invoice_id for update;
    else select * into v_target from public.invoices where id=p_target_invoice_id; end if;
    if not found then raise exception 'INVOICE_CORRECTION_TARGET_NOT_FOUND' using errcode='P0002'; end if;
    if upper(coalesce(v_target.status::text,''))<>'DRAFT' or v_target.issued_at_utc is not null then
      raise exception 'INVOICE_CORRECTION_TARGET_NOT_APPENDABLE' using errcode='P0001',
        detail=jsonb_build_object('invoice_id',p_target_invoice_id,'status',v_target.status,'issued_at_utc',v_target.issued_at_utc)::text;
    end if;
    v_target_stream:=case
      when lower(coalesce(v_target.header_snapshot_json#>>'{meta,self_bill}','false'))='true'
        then 'SELF_BILL' else 'NORMAL' end;
    if v_target_stream is distinct from v_expected_stream then
      v_errors:=v_errors||jsonb_build_array(jsonb_build_object(
        'code','INVOICE_CORRECTION_TARGET_STREAM_MISMATCH',
        'expected_stream',v_expected_stream,
        'target_stream',v_target_stream
      ));
    end if;
  end if;

  for r in
    select ts.timesheet_id,ts.correction_kind,ts.contract_id,ts.week_ending_date,
      tf.id tsfin_id,tf.client_id,tf.basis,tf.processing_status,tf.is_stale,
      tf.policy_snapshot_json,tf.rate_source_refs_json,tf.pay_vat_rate_pct_snapshot,
      c.self_bill
    from public.timesheets ts
    left join public.timesheets_financials tf on tf.timesheet_id=ts.timesheet_id and tf.is_current=true
    left join public.contracts c on c.id=ts.contract_id
    where ts.timesheet_id=any(v_ids)
    order by ts.timesheet_id
  loop
    v_leg:=public._ctms_correction_policy_leg_read_v1(r.timesheet_id);
    v_current_stream:=case
      when upper(coalesce(r.basis::text,'')) in (
        'NHSP','NHSP_ADJUSTMENT',
        'HEALTHROSTER_SELF_BILL','HEALTHROSTER_ADJUSTMENT'
      ) then 'SELF_BILL'
      else 'NORMAL'
    end;
    v_policy_ready:=
      coalesce(r.policy_snapshot_json->>'correction_financials_policy_envelope_fingerprint',
               r.policy_snapshot_json#>>'{correction_financials_policy_envelope,envelope_fingerprint}',
               r.rate_source_refs_json->>'correction_financials_policy_envelope_fingerprint')
        is not distinct from v_envelope->>'envelope_fingerprint'
      and coalesce(r.policy_snapshot_json->>'correction_leg_fingerprint',r.rate_source_refs_json->>'correction_leg_fingerprint')
        is not distinct from v_leg->>'leg_fingerprint'
      and coalesce(r.policy_snapshot_json->>'correction_tsfin_policy_fingerprint',r.rate_source_refs_json->>'correction_tsfin_policy_fingerprint')
        is not distinct from v_leg#>>'{tsfin_policy,tsfin_policy_fingerprint}'
      and coalesce(r.policy_snapshot_json->>'correction_invoice_policy_fingerprint',r.rate_source_refs_json->>'correction_invoice_policy_fingerprint')
        is not distinct from v_leg#>>'{invoice_policy,invoice_policy_fingerprint}'
      and r.policy_snapshot_json->'correction_invoice_policy'
        is not distinct from v_leg->'invoice_policy'
      and upper(btrim(coalesce(r.policy_snapshot_json->>'correction_invoice_stream','')))
        is not distinct from v_expected_stream
      and upper(btrim(coalesce(v_leg#>>'{invoice_policy,invoice_stream}','')))
        is not distinct from v_expected_stream
      and v_current_stream is not distinct from v_expected_stream;
    if r.tsfin_id is not null and not coalesce(r.is_stale,false)
       and r.processing_status='READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
       and v_policy_ready then v_ready_count:=v_ready_count+1; end if;
    if not v_policy_ready then v_errors:=v_errors||jsonb_build_array(jsonb_build_object('code','INVOICE_CORRECTION_POLICY_NOT_FROZEN','timesheet_id',r.timesheet_id)); end if;
    v_rows:=v_rows||jsonb_build_array(jsonb_build_object(
      'timesheet_id',r.timesheet_id,'correction_kind',r.correction_kind,'tsfin_id',r.tsfin_id,
      'client_id',r.client_id,'contract_id',r.contract_id,'week_ending_date',r.week_ending_date,
       'invoice_stream',v_expected_stream,'current_contract_stream',v_current_stream,
       'processing_status',r.processing_status,'policy_ready',v_policy_ready,
       'invoice_vat_chargeable',v_leg#>'{invoice_policy,invoice_vat_chargeable}',
       'invoice_vat_rate_pct',v_leg#>'{invoice_policy,applied_vat_rate_pct}',
       'invoice_policy_fingerprint',v_leg#>>'{invoice_policy,invoice_policy_fingerprint}',
       'leg_fingerprint',v_leg->>'leg_fingerprint'));
  end loop;

  select count(distinct tf.client_id),count(distinct ts.contract_id),count(distinct ts.week_ending_date),
    count(distinct case
      when upper(coalesce(tf.basis::text,'')) in (
        'NHSP','NHSP_ADJUSTMENT',
        'HEALTHROSTER_SELF_BILL','HEALTHROSTER_ADJUSTMENT'
      ) then 'SELF_BILL'
      else 'NORMAL'
    end)
  into v_client_count,v_contract_count,v_week_count,v_stream_count
  from public.timesheets ts
  join public.timesheets_financials tf on tf.timesheet_id=ts.timesheet_id and tf.is_current=true
  where ts.timesheet_id=any(v_ids);

  select count(distinct il.timesheet_id),count(distinct il.invoice_id)
  into v_line_member_count,v_line_invoice_count
  from public.invoice_lines il
  join public.invoices i on i.id=il.invoice_id
  where il.timesheet_id=any(v_ids)
    and upper(coalesce(i.type::text,''))<>'CREDIT_NOTE';

  select
    count(il.invoice_id) filter(where ts.correction_kind='CHANGED_HOURS_REVERSAL')::integer,
    count(il.invoice_id) filter(where ts.correction_kind='CHANGED_HOURS_REPLACEMENT')::integer,
    coalesce(array_agg(il.invoice_id order by il.invoice_id)
      filter(where ts.correction_kind='CHANGED_HOURS_REVERSAL'),array[]::uuid[]),
    coalesce(array_agg(il.invoice_id order by il.invoice_id)
      filter(where ts.correction_kind='CHANGED_HOURS_REPLACEMENT'),array[]::uuid[])
  into v_reversal_line_count,v_replacement_line_count,
       v_reversal_invoice_ids,v_replacement_invoice_ids
  from public.timesheets ts
  left join public.invoice_lines il on il.timesheet_id=ts.timesheet_id
    and exists(select 1 from public.invoices active_invoice where active_invoice.id=il.invoice_id
      and upper(coalesce(active_invoice.type::text,''))<>'CREDIT_NOTE')
  where ts.timesheet_id=any(v_ids);

  select coalesce(jsonb_agg(jsonb_build_object(
    'invoice_id',i.id,'status',i.status,'issued_at_utc',i.issued_at_utc,
    'client_id',i.client_id,
    'invoice_stream',case when lower(coalesce(i.header_snapshot_json#>>'{meta,self_bill}','false'))='true'
      then 'SELF_BILL' else 'NORMAL' end,
    'currency',upper(coalesce(nullif(i.header_snapshot_json->>'currency',''),nullif(i.header_snapshot_json#>>'{meta,currency}',''),'GBP')),
    'invoice_week',coalesce(i.header_snapshot_json#>>'{meta,invoice_week_start}',
      i.header_snapshot_json->>'week_ending_date',i.header_snapshot_json#>>'{meta,week_ending_date}'))
    order by i.id),'[]'::jsonb)
  into v_placement_invoices
  from public.invoices i
  where i.id=any(v_reversal_invoice_ids||v_replacement_invoice_ids);

  if v_expected_count<>2 then
    v_placement_state:='MALFORMED_PAIR';
  elsif v_reversal_line_count>1 or v_replacement_line_count>1 then
    v_placement_state:='DUPLICATE_PLACEMENT';
  elsif v_reversal_line_count=0 and v_replacement_line_count=0 then
    v_placement_state:='UNPLACED';
    v_placement_compatible:=true;
  elsif (v_reversal_line_count=1 and v_replacement_line_count=0)
     or (v_reversal_line_count=0 and v_replacement_line_count=1) then
    v_placement_state:='INCOMPLETE_MOVE';
    v_placement_compatible:=true;
  elsif v_reversal_invoice_ids[1]=v_replacement_invoice_ids[1] then
    v_placement_state:='COMPLETE_SAME_INVOICE';
    v_placement_compatible:=true;
  else
    select count(distinct i.client_id)=1
       and count(distinct case when lower(coalesce(i.header_snapshot_json#>>'{meta,self_bill}','false'))='true'
         then 'SELF_BILL' else 'NORMAL' end)=1
       and count(distinct upper(coalesce(nullif(i.header_snapshot_json->>'currency',''),
         nullif(i.header_snapshot_json#>>'{meta,currency}',''),'GBP')))=1
       and count(distinct coalesce(i.header_snapshot_json#>>'{meta,invoice_week_start}',i.header_snapshot_json->>'week_ending_date',
         i.header_snapshot_json#>>'{meta,week_ending_date}',(select min(ts.week_ending_date)::text
           from public.timesheets ts where ts.timesheet_id=any(v_ids))))=1
    into v_placement_compatible
    from public.invoices i
    where i.id=any(v_reversal_invoice_ids||v_replacement_invoice_ids);
    v_placement_state:=case when v_placement_compatible then 'COMPLETE_SPLIT_INVOICES'
      else 'INCOMPATIBLE_PLACEMENT' end;
  end if;

  select count(*)::integer into v_line_policy_mismatch_count
  from public.invoice_lines il
  cross join lateral (
    select public._ctms_correction_policy_leg_read_v1(il.timesheet_id) leg
  ) expected
  where il.timesheet_id=any(v_ids)
    and (p_target_invoice_id is null or il.invoice_id=p_target_invoice_id)
    and il.vat_rate_pct is distinct from
      (expected.leg#>>'{invoice_policy,applied_vat_rate_pct}')::numeric;

  if v_ready_count<>v_expected_count then v_errors:=v_errors||jsonb_build_array(jsonb_build_object('code','INVOICE_CORRECTION_TSFIN_NOT_READY','ready_count',v_ready_count)); end if;
  if v_client_count<>1 or v_contract_count<>1 or v_week_count<>1 or v_stream_count<>1 then
    v_errors:=v_errors||jsonb_build_array(jsonb_build_object('code','INVOICE_CORRECTION_SCOPE_MIXED'));
  end if;
  if exists (
    select 1
    from public.timesheets ts
    join public.timesheets_financials tf
      on tf.timesheet_id=ts.timesheet_id and tf.is_current=true
    where ts.timesheet_id=any(v_ids)
      and (case
        when upper(coalesce(tf.basis::text,'')) in (
          'NHSP','NHSP_ADJUSTMENT',
          'HEALTHROSTER_SELF_BILL','HEALTHROSTER_ADJUSTMENT'
        ) then 'SELF_BILL'
        else 'NORMAL'
      end)
        is distinct from v_expected_stream
  ) then
    v_errors:=v_errors||jsonb_build_array(jsonb_build_object(
      'code','INVOICE_CORRECTION_FROZEN_STREAM_DRIFT',
      'expected_stream',v_expected_stream
    ));
  end if;
  if v_placement_state='DUPLICATE_PLACEMENT' then
    v_errors:=v_errors||jsonb_build_array(jsonb_build_object('code','INVOICE_CORRECTION_PAIR_DUPLICATE_PLACEMENT'));
  elsif v_placement_state='INCOMPATIBLE_PLACEMENT' then
    v_errors:=v_errors||jsonb_build_array(jsonb_build_object('code','INVOICE_CORRECTION_PAIR_INCOMPATIBLE_PLACEMENT'));
  elsif v_placement_state='MALFORMED_PAIR' then
    v_errors:=v_errors||jsonb_build_array(jsonb_build_object('code','INVOICE_CORRECTION_PAIR_MALFORMED'));
  end if;
  if v_line_policy_mismatch_count<>0 then
    v_errors:=v_errors||jsonb_build_array(jsonb_build_object(
      'code','INVOICE_CORRECTION_LINE_VAT_POLICY_MISMATCH',
      'mismatching_line_count',v_line_policy_mismatch_count
    ));
  end if;
  if p_target_invoice_id is not null and v_target.client_id is distinct from (select tf.client_id from public.timesheets_financials tf where tf.timesheet_id=v_ids[1] and tf.is_current=true) then
    v_errors:=v_errors||jsonb_build_array(jsonb_build_object('code','INVOICE_CORRECTION_TARGET_CLIENT_MISMATCH'));
  end if;
  if p_target_invoice_id is not null and v_placement_state='INCOMPLETE_MOVE'
     and exists (
       select 1
       from public.invoices placed
       where placed.id=any(v_reversal_invoice_ids||v_replacement_invoice_ids)
         and placed.id<>p_target_invoice_id
         and (
           placed.client_id is distinct from v_target.client_id
           or (case when lower(coalesce(placed.header_snapshot_json#>>'{meta,self_bill}','false'))='true'
                 then 'SELF_BILL' else 'NORMAL' end) is distinct from v_target_stream
           or upper(coalesce(nullif(placed.header_snapshot_json->>'currency',''),
                nullif(placed.header_snapshot_json#>>'{meta,currency}',''),'GBP'))
              is distinct from upper(coalesce(nullif(v_target.header_snapshot_json->>'currency',''),
                nullif(v_target.header_snapshot_json#>>'{meta,currency}',''),'GBP'))
           or coalesce(placed.header_snapshot_json#>>'{meta,invoice_week_start}',placed.header_snapshot_json->>'week_ending_date',
                placed.header_snapshot_json#>>'{meta,week_ending_date}',(select min(ts.week_ending_date)::text from public.timesheets ts where ts.timesheet_id=any(v_ids)))
              is distinct from coalesce(v_target.header_snapshot_json#>>'{meta,invoice_week_start}',v_target.header_snapshot_json->>'week_ending_date',
                v_target.header_snapshot_json#>>'{meta,week_ending_date}',(select min(ts.week_ending_date)::text from public.timesheets ts where ts.timesheet_id=any(v_ids)))
         )
     ) then
    v_errors:=v_errors||jsonb_build_array(jsonb_build_object(
      'code','INVOICE_CORRECTION_TARGET_INCOMPATIBLE_WITH_PARTNER'));
  end if;

  return jsonb_build_object(
    'ok',true,'valid',jsonb_array_length(v_errors)=0,'root_timesheet_id',v_chain->>'root_timesheet_id',
    'correction_id',v_unit->>'correction_id','correction_shape',v_unit->>'correction_shape',
    'expected_member_count',v_expected_count,'pair_timesheet_ids',to_jsonb(v_ids),
    'target_invoice_id',p_target_invoice_id,'target_appendable',p_target_invoice_id is null or jsonb_array_length(v_errors)=0,
    'correction_financials_policy_envelope',v_envelope,
    'correction_financials_policy_envelope_fingerprint',v_envelope->>'envelope_fingerprint',
    'invoice_stream',v_expected_stream,
    'pair_rows',v_rows,'ready_count',v_ready_count,'existing_line_member_count',v_line_member_count,
    'existing_line_invoice_count',v_line_invoice_count,
    'placement_state',v_placement_state,
    'placement_complete',v_placement_state in ('COMPLETE_SAME_INVOICE','COMPLETE_SPLIT_INVOICES'),
    'placement_compatible',v_placement_compatible,
    'placement_invoices',v_placement_invoices,
    'reversal_invoice_ids',to_jsonb(v_reversal_invoice_ids),
    'replacement_invoice_ids',to_jsonb(v_replacement_invoice_ids),
    'missing_member_kind',case
      when v_placement_state='INCOMPLETE_MOVE' and v_reversal_line_count=0 then 'CHANGED_HOURS_REVERSAL'
      when v_placement_state='INCOMPLETE_MOVE' and v_replacement_line_count=0 then 'CHANGED_HOURS_REPLACEMENT' end,
    'line_policy_mismatch_count',v_line_policy_mismatch_count,'errors',v_errors,
    'compatibility_mode',v_compatibility_mode);
end;
$function$;

-- invoice_create_credit_note_and_unlock(uuid,uuid)
CREATE OR REPLACE FUNCTION public.invoice_create_credit_note_and_unlock(p_invoice_id uuid, p_actor_user_id uuid)
 RETURNS TABLE(credit_note_id uuid, unlocked_snapshots integer)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_now timestamptz := now();

  v_inv record;
  v_base_hdr jsonb := '{}'::jsonb;

  v_original_issued_at timestamptz;
  v_anchor_ymd date;

  v_stationery_key text;
  v_margins jsonb;
  v_hide_bank_footer boolean;

  v_bank jsonb;
  v_vat_reg text;

  v_client_name text;
  v_client_addr text;
  v_client_email text;
  v_vat_chargeable boolean;
  v_terms_days int;

  v_applied_vat numeric;
  v_global_vat numeric := 20;
  v_client_vat_override numeric;

  v_due_at timestamptz;

  v_credit_id uuid;

  v_ts_ids uuid[];

  v_cn_ex numeric := 0;
  v_cn_vat numeric := 0;
  v_cn_inc numeric := 0;
begin
  if p_invoice_id is null then
    raise exception 'invoice_id is required';
  end if;

  -- Load original invoice
  select *
  into v_inv
  from public.invoices
  where id = p_invoice_id;

  if not found then
    raise exception 'Invoice not found';
  end if;

  if v_inv.type::text = 'CREDIT_NOTE' then
    raise exception 'Cannot credit a CREDIT_NOTE';
  end if;

  if jsonb_typeof(v_inv.header_snapshot_json) = 'object' then
    v_base_hdr := v_inv.header_snapshot_json;
  end if;

  -- Original issued time (prefer invoice.issued_at_utc, else snapshot.issued_at_utc, else now)
  v_original_issued_at := v_inv.issued_at_utc;
  if v_original_issued_at is null and (v_base_hdr ? 'issued_at_utc') then
    begin
      v_original_issued_at := (v_base_hdr->>'issued_at_utc')::timestamptz;
    exception when others then
      v_original_issued_at := null;
    end;
  end if;
  if v_original_issued_at is null then
    v_original_issued_at := v_now;
  end if;

  v_anchor_ymd := (v_original_issued_at at time zone 'Europe/London')::date;

  -- Stationery key
  v_stationery_key := nullif(btrim(coalesce(v_base_hdr->>'stationery_key','')), '');
  if v_stationery_key is null then
    v_stationery_key := 'Assets/Stationery/Letterhead/A4/Letterhead_v1@300dpi.png';
  end if;

  if right(lower(v_stationery_key), 4) = '.pdf' then
    v_stationery_key := left(v_stationery_key, length(v_stationery_key) - 4) || '@300dpi.png';
  end if;

  while left(v_stationery_key, 1) = '/' loop
    v_stationery_key := substr(v_stationery_key, 2);
  end loop;

  -- Margins
  v_margins := v_base_hdr->'stationery_margins_mm';
  if jsonb_typeof(v_margins) = 'array' and jsonb_array_length(v_margins) = 4 then
    v_margins := jsonb_build_object(
      'top',    coalesce((v_margins->>0)::numeric, 32),
      'right',  coalesce((v_margins->>1)::numeric, 12),
      'bottom', coalesce((v_margins->>2)::numeric, 20),
      'left',   coalesce((v_margins->>3)::numeric, 12)
    );
  elsif jsonb_typeof(v_margins) = 'object' then
    v_margins := jsonb_build_object(
      'top',    coalesce((v_margins->>'top')::numeric, 32),
      'right',  coalesce((v_margins->>'right')::numeric, 12),
      'bottom', coalesce((v_margins->>'bottom')::numeric, 20),
      'left',   coalesce((v_margins->>'left')::numeric, 12)
    );
  else
    v_margins := jsonb_build_object('top',32,'right',12,'bottom',20,'left',12);
  end if;

  -- hide_bank_footer default TRUE
  if jsonb_typeof(v_base_hdr->'hide_bank_footer') = 'boolean' then
    v_hide_bank_footer := (v_base_hdr->>'hide_bank_footer')::boolean;
  else
    v_hide_bank_footer := true;
  end if;

  -- Bank + VAT registration
  if jsonb_typeof(v_base_hdr->'bank') = 'object' then
    v_bank := v_base_hdr->'bank';
  else
    v_bank := null;
  end if;

  v_vat_reg := nullif(btrim(coalesce(v_base_hdr->>'vat_registration_number','')), '');

  if v_bank is null or v_vat_reg is null then
    declare
      v_def record;
    begin
      select bank_name, bank_sort_code, bank_account_number, vat_registration_number
      into v_def
      from public.settings_defaults
      where id = 1
      limit 1;

      if v_bank is null then
        v_bank := jsonb_build_object(
          'name', v_def.bank_name,
          'sort_code', v_def.bank_sort_code,
          'account_number', v_def.bank_account_number
        );
      end if;

      if v_vat_reg is null then
        v_vat_reg := v_def.vat_registration_number;
      end if;
    end;
  end if;

  -- Client info
  v_client_name  := nullif(btrim(coalesce(v_base_hdr->>'client_name','')), '');
  v_client_addr  := nullif(btrim(coalesce(v_base_hdr->>'client_invoice_address','')), '');
  v_client_email := nullif(btrim(coalesce(v_base_hdr->>'client_primary_invoice_email','')), '');

  if jsonb_typeof(v_base_hdr->'vat_chargeable') = 'boolean' then
    v_vat_chargeable := (v_base_hdr->>'vat_chargeable')::boolean;
  else
    v_vat_chargeable := null;
  end if;

  if (v_base_hdr ? 'payment_terms_days') then
    begin
      v_terms_days := (v_base_hdr->>'payment_terms_days')::int;
    exception when others then
      v_terms_days := null;
    end;
  else
    v_terms_days := null;
  end if;

  if v_client_name is null or v_client_addr is null or v_vat_chargeable is null or v_terms_days is null then
    declare
      v_cli record;
    begin
      select name, invoice_address, primary_invoice_email, vat_chargeable, payment_terms_days
      into v_cli
      from public.clients
      where id = v_inv.client_id
      limit 1;

      if v_client_name is null then v_client_name := v_cli.name; end if;
      if v_client_addr is null then v_client_addr := v_cli.invoice_address; end if;
      if v_client_email is null then v_client_email := v_cli.primary_invoice_email; end if;

      if v_vat_chargeable is null then
        v_vat_chargeable := coalesce(v_cli.vat_chargeable, true);
      end if;

      if v_terms_days is null then
        v_terms_days := coalesce(v_cli.payment_terms_days, 30);
      end if;
    end;
  end if;

  -- VAT % (prefer original snapshot applied_vat_rate_pct; else compute anchored)
  v_applied_vat := null;
  if (v_base_hdr ? 'applied_vat_rate_pct') then
    begin
      v_applied_vat := (v_base_hdr->>'applied_vat_rate_pct')::numeric;
    exception when others then
      v_applied_vat := null;
    end;
  end if;

  if v_applied_vat is null then
    select coalesce(sf.vat_rate_pct, 20)
    into v_global_vat
    from public.settings_finance_pick(v_anchor_ymd) sf
    limit 1;

    select cs.vat_rate_pct
    into v_client_vat_override
    from public.client_settings cs
    where cs.client_id = v_inv.client_id
      and cs.effective_from <= v_anchor_ymd
    order by cs.effective_from desc
    limit 1;

    v_applied_vat := case
      when v_vat_chargeable = false then 0
      else coalesce(v_client_vat_override, v_global_vat, 20)
    end;
  else
    if v_vat_chargeable = false then
      v_applied_vat := 0;
    end if;
  end if;

  v_due_at := v_now + make_interval(days => coalesce(v_terms_days, 30));

  -- Create the credit note invoice row
  insert into public.invoices (
    client_id,
    type,
    status,
    status_date_utc,
    issued_at_utc,
    due_at_utc,
    subtotal_ex_vat,
    vat_amount,
    total_inc_vat,
    original_invoice_id,
    header_snapshot_json
  )
  values (
    v_inv.client_id,
    'CREDIT_NOTE'::public.invoice_type_enum,
    'ISSUED'::public.invoice_status_enum,
    v_now,
    v_now,
    v_due_at,
    0,
    0,
    0,
    v_inv.id,
    jsonb_build_object(
      'client_id', v_inv.client_id::text,
      'client_name', v_client_name,
      'client_invoice_address', v_client_addr,
      'client_primary_invoice_email', v_client_email,
      'vat_chargeable', coalesce(v_vat_chargeable, true),
      'applied_vat_rate_pct', coalesce(v_applied_vat, 0),
      'payment_terms_days', coalesce(v_terms_days, 30),
      'issued_at_utc', public._inv_iso_utc(v_now),
      'due_at_utc', public._inv_iso_utc(v_due_at),
      'stationery_key', v_stationery_key,
      'stationery_margins_mm', v_margins,
      'hide_bank_footer', v_hide_bank_footer,
      'bank', v_bank,
      'vat_registration_number', v_vat_reg,
      'meta', jsonb_build_object(
        'source', 'CREDIT_NOTE',
        'original_invoice_id', v_inv.id::text,
        'vat_anchor_ymd', v_anchor_ymd::text,
        'original_invoice_issued_at_utc', public._inv_iso_utc(v_original_issued_at)
      )
    )
  )
  returning id into v_credit_id;

  -- ✅ Insert negative mirror lines (one-for-one from original invoice_lines)
  insert into public.invoice_lines(
    invoice_id, timesheet_id, booking_id, description,
    hours_day, hours_night, hours_sat, hours_sun, hours_bh,
    pay_day, pay_night, pay_sat, pay_sun, pay_bh,
    charge_day, charge_night, charge_sat, charge_sun, charge_bh,
    total_pay_ex_vat, total_charge_ex_vat, margin_ex_vat,
    vat_rate_pct, vat_amount, total_inc_vat,
    paper_ts_r2_key, meta_json, source_key
  )
  select
    v_credit_id,
    l.timesheet_id,
    l.booking_id,
    ('CREDIT NOTE – ' || coalesce(l.description,'')),

    l.hours_day, l.hours_night, l.hours_sat, l.hours_sun, l.hours_bh,

    l.pay_day, l.pay_night, l.pay_sat, l.pay_sun, l.pay_bh,
    l.charge_day, l.charge_night, l.charge_sat, l.charge_sun, l.charge_bh,

    public._inv_round2(-1 * coalesce(l.total_pay_ex_vat,0)),
    public._inv_round2(-1 * coalesce(l.total_charge_ex_vat,0)),
    public._inv_round2(-1 * coalesce(l.margin_ex_vat,0)),

    l.vat_rate_pct,
    public._inv_round2(-1 * coalesce(l.vat_amount,0)),
    public._inv_round2(-1 * coalesce(l.total_inc_vat,0)),

    l.paper_ts_r2_key,

    (coalesce(l.meta_json,'{}'::jsonb) ||
      jsonb_build_object(
        'credit_note', true,
        'original_invoice_id', v_inv.id::text,
        'original_invoice_line_id', l.id::text
      )
    ),

    ('CN:' || v_credit_id::text || ':LINE:' || l.id::text)
  from public.invoice_lines l
  where l.invoice_id = p_invoice_id;

  -- Update credit note totals from its lines
  select
    coalesce(sum(l2.total_charge_ex_vat),0)::numeric,
    coalesce(sum(l2.vat_amount),0)::numeric,
    coalesce(sum(l2.total_inc_vat),0)::numeric
  into v_cn_ex, v_cn_vat, v_cn_inc
  from public.invoice_lines l2
  where l2.invoice_id = v_credit_id;

  update public.invoices
  set
    subtotal_ex_vat = public._inv_round2(v_cn_ex),
    vat_amount      = public._inv_round2(v_cn_vat),
    total_inc_vat   = public._inv_round2(v_cn_inc),
    updated_at      = v_now
  where id = v_credit_id;

  -- Audit credit note creation (includes totals)
  perform public._audit_insert(
    'invoice',
    v_credit_id::text,
    'CREDIT_NOTE_CREATED',
    null,
    jsonb_build_object(
      'credit_note_id', v_credit_id::text,
      'original_invoice_id', v_inv.id::text,
      'subtotal_ex_vat', public._inv_round2(v_cn_ex),
      'vat_amount', public._inv_round2(v_cn_vat),
      'total_inc_vat', public._inv_round2(v_cn_inc)
    ),
    null,
    p_actor_user_id
  );

  -- Unlock snapshots locked by the original invoice
  select array_agg(distinct tf.timesheet_id)
  into v_ts_ids
  from public.timesheets_financials tf
  where tf.is_current = true
    and tf.locked_by_invoice_id = p_invoice_id
    and tf.timesheet_id is not null;

  unlocked_snapshots := coalesce(array_length(v_ts_ids, 1), 0);

  if unlocked_snapshots > 0 then
    update public.timesheets_financials tf
    set locked_by_invoice_id = null,
        locked_at_utc = null,
        unlocked_by_credit_note_id = v_credit_id,
        is_stale = true,
        stale_reason = 'UNLOCKED_BY_CREDIT',
        updated_at = v_now
    where tf.is_current = true
      and tf.locked_by_invoice_id = p_invoice_id;

    -- Enqueue recompute (batch, idempotent)
    insert into public.ts_financials_outbox(timesheet_id, reason, attempt_count, next_attempt_at, last_error, created_at)
    select
      x.timesheet_id,
      'VERSION_ROTATED'::public.ts_fin_reason_enum,
      0,
      v_now,
      null,
      v_now
    from (select unnest(v_ts_ids) as timesheet_id) x
    on conflict on constraint uq_tsfin_outbox do nothing;

    perform public._audit_insert(
      'invoice',
      v_credit_id::text,
      'CREDIT_NOTE_UNLOCKED_SNAPSHOTS',
      null,
      jsonb_build_object(
        'credit_note_id', v_credit_id::text,
        'original_invoice_id', v_inv.id::text,
        'timesheet_ids', to_jsonb(coalesce(v_ts_ids, array[]::uuid[])),
        'unlocked_count', unlocked_snapshots
      ),
      null,
      p_actor_user_id
    );
  end if;

  credit_note_id := v_credit_id;
  return next;
end;
$function$;

-- invoice_dequeue_batch_ids(integer)
CREATE OR REPLACE FUNCTION public.invoice_dequeue_batch_ids(p_limit integer DEFAULT 10)
 RETURNS TABLE(outbox_id uuid, kind text, payload jsonb, attempt_count integer, next_attempt_at timestamp with time zone, created_at timestamp with time zone)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_now timestamptz := now();
  v_lim int := greatest(1, least(coalesce(p_limit, 10), 500));
begin
  return query
  with picked as (
    select o.id
    from public.invoice_jobs_outbox o
    where o.next_attempt_at is null or o.next_attempt_at <= v_now
    order by o.next_attempt_at nulls first, o.created_at
    limit v_lim
    for update skip locked
  ),
    leased as (
    update public.invoice_jobs_outbox o
    set attempt_count   = coalesce(o.attempt_count, 0) + 1,
        next_attempt_at = v_now + interval '5 minutes'
    where o.id in (select id from picked)
    returning o.*
  )

  select
    l.id as outbox_id,
    l.kind,
    l.payload,
    l.attempt_count,
    l.next_attempt_at,
    l.created_at
  from leased l;
end;
$function$;

-- invoice_detail_get(uuid,uuid)
CREATE OR REPLACE FUNCTION public.invoice_detail_get(p_invoice_id uuid, p_actor_user_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_temp'
AS $function$
declare
  v_role text;
  v_service boolean:=coalesce(auth.role(),'')='service_role';
  v_result jsonb;
begin
  if p_invoice_id is null then
    raise exception using errcode='22023',message='invoice_id is required';
  end if;
  if not v_service
     and(auth.uid() is null or auth.uid() is distinct from p_actor_user_id) then
    raise exception using errcode='42501',message='Authenticated actor mismatch';
  end if;

  select lower(btrim(coalesce(u.role,''))) into v_role
  from public.tms_users u
  where u.id=p_actor_user_id and u.is_active;
  if(not found or v_role<>'admin') and not v_service then
    raise exception using errcode='42501',
      message='Invoice administrator permission required';
  end if;

  if not exists(select 1 from public.invoices i where i.id=p_invoice_id) then
    return jsonb_build_object('ok',false,'error_code','INVOICE_NOT_FOUND');
  end if;

  with
  inv as materialized (
    select i.* from public.invoices i where i.id=p_invoice_id
  ),
  lines as materialized (
    select l.*,td.r2_key exact_timesheet_document_r2_key,
      pc.precheck_status,pc.has_timesheet_evidence_pdf
    from public.invoice_lines l
    left join public.timesheets t
      on t.timesheet_id=l.timesheet_id and t.is_current
    left join lateral (
      select v.r2_key
      from public.invoice_document_versions v
      where v.entity_type='TIMESHEET'
        and v.entity_id=t.timesheet_id
        and v.purpose='TIMESHEET'
        and v.source_revision=t.document_revision::text
        and v.status='READY'
        and nullif(v.r2_key,'') is not null
        and nullif(v.sha256,'') is not null
        and coalesce(v.size_bytes,0)>0
        and coalesce(v.page_count,0)>0
      order by v.ready_at_utc desc nulls last,v.id desc
      limit 1
    ) td on true
    left join public.v_ts_invoice_precheck pc on pc.timesheet_id=l.timesheet_id
    where l.invoice_id=p_invoice_id
  ),
  source_timesheets as materialized (
    select distinct x.timesheet_id
    from (
      select l.timesheet_id from lines l where l.timesheet_id is not null
      union
      select case when coalesce(l.meta_json->>'timesheet_id','')~
          '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
        then (l.meta_json->>'timesheet_id')::uuid end
      from lines l
      where l.timesheet_id is null
    ) x
    where x.timesheet_id is not null
  ),
  references_batch as materialized (
    select r.*
    from private._invoice_reference_rows_batch(array[p_invoice_id]) r
  ),
  reference_rows as materialized (
    select coalesce(jsonb_agg(
      jsonb_build_object(
        'row_key',r.row_key,'timesheet_id',r.timesheet_id,
        'candidate_id',coalesce(ct.candidate_id,tf.candidate_id),
         'candidate_display',coalesce(nullif(btrim(cc.display_name),''),
           nullif(btrim(cf.display_name),'')),
         'week_ending_date',t.week_ending_date,
         'document_revision',t.document_revision,
         'sheet_scope',r.sheet_scope,'submission_mode',r.submission_mode,
        'ref_target',r.ref_target,'segment_id',r.segment_id,
        'day_ymd',r.day_ymd,'start_utc',r.start_utc,'end_utc',r.end_utc,
        'current_reference',r.current_reference,'is_required',r.is_required)
      order by r.timesheet_id,r.day_ymd nulls last,r.start_utc nulls last,
        r.segment_id nulls last),'[]'::jsonb) rows
    from references_batch r
    left join public.timesheets t
      on t.timesheet_id=r.timesheet_id and t.is_current
    left join public.contracts ct on ct.id=t.contract_id
    left join public.timesheets_financials tf
      on tf.timesheet_id=r.timesheet_id and tf.is_current
    left join public.candidates cc on cc.id=ct.candidate_id
    left join public.candidates cf on cf.id=tf.candidate_id
  ),
  current_financials as materialized (
    select distinct on (tf.timesheet_id) tf.*
    from public.timesheets_financials tf
    join source_timesheets s on s.timesheet_id=tf.timesheet_id
    where tf.is_current
    order by tf.timesheet_id,tf.updated_at desc nulls last,tf.created_at desc nulls last,tf.id desc
  ),
  reference_sources as materialized (
    select coalesce(jsonb_object_agg(t.timesheet_id::text,
       jsonb_build_object(
         'candidate_id',coalesce(ct.candidate_id,f.candidate_id),
         'candidate_display',coalesce(nullif(btrim(cc.display_name),''),
           nullif(btrim(cf.display_name),'')),
         'week_ending_date',t.week_ending_date,
         'document_revision',t.document_revision,
         'reference_number',t.reference_number,
         'hospital_norm',t.hospital_norm,
         'ward_norm',t.ward_norm,
         'source_mode',upper(coalesce(f.invoice_breakdown_json->>'mode','')),
         'day_references_json',t.day_references_json,
        'actual_schedule_json',case
          when upper(coalesce(f.invoice_breakdown_json->>'mode',''))='SEGMENTS'
            and jsonb_typeof(f.invoice_breakdown_json->'segments')='array'
          then coalesce((
            select jsonb_agg(jsonb_build_object(
              'segment_id',seg.value->>'segment_id',
              'date',seg.value->>'date',
              'start_utc',seg.value->>'start_utc',
              'end_utc',seg.value->>'end_utc',
              'start',seg.value->>'start_utc',
              'end',seg.value->>'end_utc',
              'ref_num',seg.value->>'ref_num',
              'source_system',seg.value->>'source_system')
              order by coalesce(seg.value->>'date',''),
                coalesce(seg.value->>'start_utc',''),
                coalesce(seg.value->>'segment_id',''))
            from jsonb_array_elements(f.invoice_breakdown_json->'segments') seg(value)
            where nullif(btrim(coalesce(
              seg.value->>'invoice_locked_invoice_id','')),'')=p_invoice_id::text
          ),'[]'::jsonb)
          when jsonb_typeof(t.actual_schedule_json)='array'
            then t.actual_schedule_json
          else '[]'::jsonb end)
      order by t.timesheet_id),'{}'::jsonb) rows
     from source_timesheets s
     join public.timesheets t on t.timesheet_id=s.timesheet_id and t.is_current
     left join current_financials f on f.timesheet_id=t.timesheet_id
     left join public.contracts ct on ct.id=t.contract_id
     left join public.candidates cc on cc.id=ct.candidate_id
     left join public.candidates cf on cf.id=f.candidate_id
   ),
  line_totals as materialized (
    select count(*)::integer line_count,
      round(coalesce(sum(l.total_charge_ex_vat),0),2) net,
      round(coalesce(sum(l.vat_amount),0),2) vat,
      round(coalesce(sum(l.total_inc_vat),0),2) gross
    from lines l
  ),
  timesheet_readiness as materialized (
    select t.timesheet_id,t.submission_mode,t.document_state,
      t.current_document_version_id,t.manual_document_asset_id,
      summary.client_no_timesheet_required,summary.client_is_nhsp,
      case
        when coalesce(summary.client_no_timesheet_required,false)
          or coalesce(summary.client_is_nhsp,false) then true
        else dv.status='READY'
      end ready,
      case
        when coalesce(summary.client_no_timesheet_required,false) then 'NOT_REQUIRED'
        when coalesce(summary.client_is_nhsp,false) then 'NHSP_SUPPORT'
        when upper(coalesce(t.submission_mode::text,'')) in('MANUAL','QR')
          then 'MANUAL_TIMESHEET'
        else 'ELECTRONIC_TIMESHEET'
      end support_type,
      coalesce(dv.page_count,0) pages
    from source_timesheets s
    left join public.timesheets t
      on t.timesheet_id=s.timesheet_id and t.is_current
    left join public.v_timesheets_summary_base summary
      on summary.timesheet_id=s.timesheet_id
    left join lateral (
      select v.status,v.page_count
      from public.invoice_document_versions v
      where v.entity_type='TIMESHEET'
        and v.entity_id=t.timesheet_id
        and v.purpose='TIMESHEET'
        and v.source_revision=t.document_revision::text
        and v.status='READY'
        and nullif(v.r2_key,'') is not null
        and nullif(v.sha256,'') is not null
        and coalesce(v.size_bytes,0)>0
        and coalesce(v.page_count,0)>0
      order by v.ready_at_utc desc nulls last,v.id desc
      limit 1
    ) dv on true
  ),
  evidence_rows as materialized (
    select e.id,e.timesheet_id,e.kind,e.display_name,e.storage_key,e.created_at,
      e.document_asset_id,e.processing_state,e.processing_error_json,
      a.status asset_status,a.normalised_r2_key,a.normalised_sha256,
      a.normalised_size_bytes,a.normalised_page_count,
      case when e.document_asset_id is null then 'NOT_REGISTERED'
        when a.status='READY' then 'READY'
        when a.status in('UNSUPPORTED','CORRUPT','MISSING','FAILED') then 'FAILED'
        else 'NOT_READY' end readiness
    from public.timesheet_evidence e
    join source_timesheets s on s.timesheet_id=e.timesheet_id
    left join public.invoice_document_assets a on a.id=e.document_asset_id
  ),
  segment_stats as materialized (
    select f.timesheet_id,f.id tsfin_id,
      coalesce(jsonb_agg(seg.value order by coalesce(seg.value->>'date',''),
        coalesce(seg.value->>'segment_id','')) filter(
          where nullif(btrim(seg.value->>'invoice_locked_invoice_id'),'')
            =p_invoice_id::text),'[]'::jsonb) invoiced_segments,
      count(*) filter(where nullif(btrim(seg.value->>'invoice_locked_invoice_id'),'') is null)
        ::integer uninvoiced_segment_count,
      count(*) filter(where nullif(btrim(seg.value->>'invoice_locked_invoice_id'),'') is not null
        and nullif(btrim(seg.value->>'invoice_locked_invoice_id'),'')<>p_invoice_id::text)
        ::integer locked_elsewhere_segment_count
    from current_financials f
    cross join lateral jsonb_array_elements(
      case when upper(coalesce(f.invoice_breakdown_json->>'mode',''))='SEGMENTS'
          and jsonb_typeof(f.invoice_breakdown_json->'segments')='array'
        then f.invoice_breakdown_json->'segments' else '[]'::jsonb end) seg(value)
    group by f.timesheet_id,f.id
  ),
  segment_projection as materialized (
    select coalesce(jsonb_object_agg(s.timesheet_id::text,jsonb_build_object(
      'tsfin_id',s.tsfin_id,'invoiced_segments',s.invoiced_segments,
      'uninvoiced_segment_count',s.uninvoiced_segment_count,
      'locked_elsewhere_segment_count',s.locked_elsewhere_segment_count)
      order by s.timesheet_id),'{}'::jsonb) rows
    from segment_stats s
  ),
  history_audit as materialized (
    select jsonb_build_object(
      'kind','AUDIT','id',ae.id,'ts_utc',ae.ts_utc,
      'actor_user_id',ae.actor_user_id,
      'actor_display',coalesce(ae.actor_display,u.display_name,u.email,'CloudTMS server'),
      'actor_role_at_time',coalesce(ae.actor_role_at_time,u.role,'system'),
      'action',ae.action,'reason',ae.reason,'object_type',ae.object_type,
      'object_id_text',ae.object_id_text,'before_json',ae.before_json,
      'after_json',ae.after_json,'correlation_id',ae.correlation_id) row_json,
      ae.ts_utc
    from public.audit_events ae
    left join public.tms_users u on u.id=ae.actor_user_id
    where ae.object_type in('invoice','invoices')
      and ae.object_id_text=p_invoice_id::text
    order by ae.ts_utc desc,ae.id desc limit 500
  ),
  invoice_mail as materialized (
    select m.*,jsonb_build_object(
      'kind','EMAIL','mail_outbox_id',m.id,'ts_utc',m.created_at_utc,
      'status',m.status,'to',m."to",'cc',m.cc,'subject',m.subject,
      'reference',m.reference,'sent_at',m.sent_at,'failed_at',m.failed_at) row_json
    from public.mail_outbox m
    where upper(coalesce(m.type,''))='INVOICE'
      and jsonb_typeof(m.attachments)='array'
      and exists(
        select 1 from jsonb_array_elements(m.attachments) a(value)
        where a.value->>'invoice_id'=p_invoice_id::text
          or a.value->>'document_version_id'=(
            select i.issued_document_version_id::text from inv i))
    order by m.created_at_utc desc,m.id desc limit 200
  ),
  history_projection as materialized (
    select coalesce(jsonb_agg(x.row_json order by x.ts_utc desc),'[]'::jsonb) rows
    from (
      select h.row_json,h.ts_utc from history_audit h
      union all
      select m.row_json,m.created_at_utc from invoice_mail m
    ) x
  ),
  current_documents as materialized (
    select
      coalesce((
        select jsonb_build_object(
          'id',v.id,'operation_id',v.operation_id,'purpose',v.purpose,
          'source_revision',v.source_revision,'template_version',v.template_version,
          'status',v.status,'r2_key',v.r2_key,'sha256',v.sha256,
          'size_bytes',v.size_bytes,'page_count',v.page_count,
          'created_at_utc',v.created_at_utc,'ready_at_utc',v.ready_at_utc,
          'verified_at_utc',v.verified_at_utc,'error',v.error_json)
        from public.invoice_document_versions v
        join inv i on i.preview_document_version_id=v.id
        where v.entity_type='INVOICE' and v.entity_id=p_invoice_id
          and v.purpose='DRAFT_PREVIEW'),'null'::jsonb) preview,
      coalesce((
        select jsonb_build_object(
          'id',v.id,'operation_id',v.operation_id,'purpose',v.purpose,
          'source_revision',v.source_revision,'template_version',v.template_version,
          'status',v.status,'r2_key',v.r2_key,'sha256',v.sha256,
          'size_bytes',v.size_bytes,'page_count',v.page_count,
          'created_at_utc',v.created_at_utc,'ready_at_utc',v.ready_at_utc,
          'verified_at_utc',v.verified_at_utc,'error',v.error_json)
        from public.invoice_document_versions v
        join inv i on i.issued_document_version_id=v.id
        where v.entity_type='INVOICE' and v.entity_id=p_invoice_id
          and v.purpose='FINAL_ISSUE'),'null'::jsonb) issued,
      coalesce((
        select jsonb_build_object(
          'id',v.id,'operation_id',v.operation_id,'purpose',v.purpose,
          'source_revision',v.source_revision,'template_version',v.template_version,
          'status',v.status,'r2_key',v.r2_key,'sha256',v.sha256,
          'size_bytes',v.size_bytes,'page_count',v.page_count,
          'created_at_utc',v.created_at_utc,'ready_at_utc',v.ready_at_utc,
          'verified_at_utc',v.verified_at_utc,'error',v.error_json)
        from public.invoice_document_versions v
        where v.entity_type='INVOICE' and v.entity_id=p_invoice_id
        order by v.created_at_utc desc,v.id desc limit 1),'null'::jsonb) latest
  ),
  active_document as materialized (
    select o.id operation_id,o.status,o.phase,o.progress_json,o.error_json,o.change_seq
    from public.invoice_operations o
    join inv i on i.active_document_operation_id=o.id
  ),
  active_issue as materialized (
    select c.operation_id,c.id chunk_id,c.status,c.phase,c.progress_json,c.error_json,
      o.change_seq
    from public.invoice_operation_chunks c
    join public.invoice_operations o on o.id=c.operation_id
    where c.chunk_type='ISSUE_INVOICE' and c.entity_type='INVOICE'
      and c.entity_id=p_invoice_id
      and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
     order by c.updated_at_utc desc,c.id desc limit 1
  ),
  source_edit_authority as materialized (
    select
      (
        i.status in('DRAFT','ON_HOLD')
        and i.issued_at_utc is null
        and i.paid_at_utc is null
        and i.active_issue_operation_id is null
        and upper(coalesce(i.issue_state,'')) not in(
          'VALIDATING','PREPARING_DOCUMENT','READY_TO_FINALISE')
        and not exists(select 1 from active_issue)
        and not exists(
          select 1
          from source_timesheets s
          where coalesce((
            public._ctms_import_correction_classify_v1(s.timesheet_id)
              ->>'is_import_authoritative_correction')::boolean,false)
        )
      ) can_edit_source,
      coalesce((
        select jsonb_agg(code order by ordinal)
        from (
          values
            (1,case when i.status not in('DRAFT','ON_HOLD')
              then 'INVOICE_SOURCE_EDIT_STATUS_FORBIDDEN' end),
            (2,case when i.issued_at_utc is not null
              then 'INVOICE_SOURCE_EDIT_ISSUED' end),
            (3,case when i.paid_at_utc is not null
              then 'INVOICE_SOURCE_EDIT_PAID' end),
            (4,case when i.active_issue_operation_id is not null
              or upper(coalesce(i.issue_state,'')) in(
                'VALIDATING','PREPARING_DOCUMENT','READY_TO_FINALISE')
              or exists(select 1 from active_issue)
              then 'INVOICE_SOURCE_EDIT_ISSUE_IN_PROGRESS' end),
            (5,case when exists(
              select 1
              from source_timesheets s
              where coalesce((
                public._ctms_import_correction_classify_v1(s.timesheet_id)
                  ->>'is_import_authoritative_correction')::boolean,false))
              then 'IMPORT_AUTHORITATIVE_CORRECTION_SOURCE_EDIT_FORBIDDEN' end)
        ) blockers(ordinal,code)
        where code is not null
      ),'[]'::jsonb) source_edit_blocker_codes,
      exists(
        select 1
        from public.invoice_document_versions dv
        left join public.invoice_operations op on op.id=dv.operation_id
        where dv.entity_type='INVOICE'
          and dv.entity_id=i.id
          and dv.purpose='DRAFT_PREVIEW'
          and dv.source_revision=i.document_revision::text
          and (
            (dv.status='READY'
              and nullif(btrim(coalesce(dv.r2_key,'')),'') is not null
              and nullif(btrim(coalesce(dv.sha256,'')),'') is not null
              and coalesce(dv.size_bytes,0)>0
              and coalesce(dv.page_count,0)>0)
            or (
              dv.id=i.preview_document_version_id
              and dv.status in(
                'PLANNING','WAITING_FOR_INPUTS','RENDERING',
                'ASSEMBLING','VERIFYING','READY'))
            or (
              dv.operation_id=i.active_document_operation_id
              and op.operation_type='BUILD_DOCUMENT'
              and op.entity_type='INVOICE'
              and op.entity_id=i.id
              and op.source_revision=i.document_revision::text
              and coalesce(op.input_json->>'purpose','')='DRAFT_PREVIEW'
              and op.status in(
                'QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED'))
          )
      ) source_edit_will_replace_preview
    from inv i
  ),
  issue_validation as materialized (
    select v.*
    from inv i
    cross join lateral private._invoice_issue_validate_batch(
      jsonb_build_array(jsonb_build_object(
        'request_key','detail:'||i.id::text,
        'invoice_id',i.id,'expected_revision',i.document_revision,
        'allow_early',false,'deliver',true)),
      (now() at time zone 'Europe/London')::date) v
  ),
  support_summary as materialized (
    select
      count(*) filter(where support_type='ELECTRONIC_TIMESHEET')::integer
        electronic_timesheet_count,
      count(*) filter(where support_type='ELECTRONIC_TIMESHEET'
        and not coalesce(ready,false))::integer electronic_timesheet_not_ready_count,
      count(*) filter(where support_type='MANUAL_TIMESHEET')::integer
        manual_timesheet_count,
      count(*) filter(where support_type='MANUAL_TIMESHEET'
        and not coalesce(ready,false))::integer manual_timesheet_not_ready_count,
      coalesce(sum(pages),0)::integer estimated_timesheet_pages
    from timesheet_readiness
  ),
  evidence_summary as materialized (
    select count(*)::integer evidence_count,
      count(*) filter(where readiness='NOT_REGISTERED')::integer evidence_unregistered_count,
      count(*) filter(where readiness='NOT_READY')::integer evidence_not_ready_count,
      count(*) filter(where readiness='FAILED')::integer evidence_failed_count,
      coalesce(sum(normalised_page_count),0)::integer estimated_evidence_pages
    from evidence_rows
  ),
  projections as materialized (
    select
      (select coalesce(jsonb_agg(
        jsonb_build_object(
          'id',l.id,'invoice_id',l.invoice_id,'timesheet_id',l.timesheet_id,
          'booking_id',l.booking_id,'description',l.description,
          'hours_day',l.hours_day,'hours_night',l.hours_night,
          'hours_sat',l.hours_sat,'hours_sun',l.hours_sun,'hours_bh',l.hours_bh,
          'pay_day',l.pay_day,'pay_night',l.pay_night,'pay_sat',l.pay_sat,
          'pay_sun',l.pay_sun,'pay_bh',l.pay_bh,
          'charge_day',l.charge_day,'charge_night',l.charge_night,
          'charge_sat',l.charge_sat,'charge_sun',l.charge_sun,
          'charge_bh',l.charge_bh,'total_pay_ex_vat',l.total_pay_ex_vat,
          'total_charge_ex_vat',l.total_charge_ex_vat,
          'margin_ex_vat',l.margin_ex_vat,'vat_rate_pct',l.vat_rate_pct,
          'vat_amount',l.vat_amount,'total_inc_vat',l.total_inc_vat,
          'created_at',l.created_at,'paper_ts_r2_key',l.exact_timesheet_document_r2_key,
          'meta_json',l.meta_json,'source_key',l.source_key,
          'precheck_status',l.precheck_status,
          'has_timesheet_evidence_pdf',l.has_timesheet_evidence_pdf,
          'paper_ts_r2_key',l.exact_timesheet_document_r2_key,
          'is_adjustment',l.timesheet_id is null
            or upper(coalesce(l.meta_json->>'line_type',''))='ADJUSTMENT',
          'line_type_norm',upper(coalesce(l.meta_json->>'line_type','')))
        order by l.created_at,l.id),'[]'::jsonb) from lines l) line_rows,
      (select coalesce(jsonb_agg(jsonb_build_object(
        'id',e.id,'timesheet_id',e.timesheet_id,'kind',e.kind,
        'display_name',e.display_name,'storage_key',e.storage_key,
        'created_at',e.created_at,'document_asset_id',e.document_asset_id,
        'processing_state',e.processing_state,
        'processing_error',e.processing_error_json,'asset_status',e.asset_status,
        'normalised_r2_key',e.normalised_r2_key,
        'normalised_sha256',e.normalised_sha256,
        'normalised_size_bytes',e.normalised_size_bytes,
        'normalised_page_count',e.normalised_page_count,
        'readiness',e.readiness) order by e.created_at,e.id),
        '[]'::jsonb) from evidence_rows e) evidence,
      (select coalesce(jsonb_agg(jsonb_build_object(
        'id',e.id,'timesheet_id',e.timesheet_id,'kind',e.kind,
        'display_name',e.display_name,'storage_key',e.storage_key,
        'created_at',e.created_at,'document_asset_id',e.document_asset_id,
        'processing_state',e.processing_state,'asset_status',e.asset_status,
        'normalised_r2_key',e.normalised_r2_key,
        'normalised_sha256',e.normalised_sha256,
        'normalised_size_bytes',e.normalised_size_bytes,
        'normalised_page_count',e.normalised_page_count,
        'readiness',e.readiness) order by e.created_at,e.id),
        '[]'::jsonb) from evidence_rows e
        where upper(coalesce(e.kind,''))='TIMESHEET') timesheet_evidence,
      (select coalesce(jsonb_agg(jsonb_build_object(
        'id',e.id,'timesheet_id',e.timesheet_id,'kind',e.kind,
        'display_name',e.display_name,'storage_key',e.storage_key,
        'created_at',e.created_at,'document_asset_id',e.document_asset_id,
        'processing_state',e.processing_state,'asset_status',e.asset_status,
        'normalised_r2_key',e.normalised_r2_key,
        'normalised_sha256',e.normalised_sha256,
        'normalised_size_bytes',e.normalised_size_bytes,
        'normalised_page_count',e.normalised_page_count,
        'readiness',e.readiness) order by e.created_at,e.id),
        '[]'::jsonb) from evidence_rows e
        where upper(coalesce(e.kind,''))<>'TIMESHEET') evidence_other,
      (select coalesce(jsonb_agg(jsonb_build_object(
        'invoice_id',h.invoice_id,'source_system',h.source_system,
        'import_id',h.import_id,'header_rows',h.header_rows,
        'header_columns',h.header_columns,'rows_json',h.rows_json)
        order by h.source_system,h.import_id),
        '[]'::jsonb) from public.invoice_hr_source_rows h
        where h.invoice_id=p_invoice_id) hr_rows,
      (select coalesce(jsonb_agg(jsonb_build_object(
        'tsfin_id',f.id,'timesheet_id',f.timesheet_id,
        'external_source_rows_json',f.external_source_rows_json,
        'mileage_units',f.mileage_units,'mileage_pay_rate',f.mileage_pay_rate,
        'mileage_charge_rate',f.mileage_charge_rate)
        order by f.timesheet_id),'[]'::jsonb) from current_financials f) tsfin_rows,
      (select coalesce(jsonb_object_agg(f.timesheet_id::text,f.id::text
        order by f.timesheet_id),'{}'::jsonb) from current_financials f) tsfin_map
  ),
  assembled as materialized (
    select i.*,lt.*,rr.rows reference_rows,rs.rows reference_sources,
      sp.rows segments,p.*,hp.rows history_rows,ss.*,es.*,
      sea.can_edit_source,sea.source_edit_blocker_codes,
      sea.source_edit_will_replace_preview,
      cd.preview,cd.issued,cd.latest,
      ad.operation_id document_operation_id,ad.status document_operation_status,
      ad.phase document_operation_phase,ad.progress_json document_operation_progress,
      ad.error_json document_operation_error,ad.change_seq document_change_seq,
      ai.operation_id issue_operation_id,ai.chunk_id issue_chunk_id,
      ai.status issue_status,ai.phase issue_phase,ai.progress_json issue_progress,
      ai.error_json issue_error,ai.change_seq issue_change_seq,
      iv.hard_blocker_codes blocker_codes,iv.document_dependency_codes,
      iv.delivery_blocker_codes,iv.warning_codes,iv.can_issue_only,
      iv.can_issue_and_deliver,iv.route_policy_result,iv.detail_json
    from inv i cross join line_totals lt cross join reference_rows rr
    cross join reference_sources rs cross join segment_projection sp
    cross join projections p cross join history_projection hp
    cross join support_summary ss cross join evidence_summary es
    cross join source_edit_authority sea
    cross join current_documents cd
    left join active_document ad on true left join active_issue ai on true
    left join issue_validation iv on true
  ),
  allowlisted_snapshot as materialized (
    select a.*,
      jsonb_strip_nulls(jsonb_build_object(
        'agency_name',a.header_snapshot_json->'agency_name',
        'agency_logo',a.header_snapshot_json->'agency_logo',
        'agency_logo_url',a.header_snapshot_json->'agency_logo_url',
        'registered_address',a.header_snapshot_json->'registered_address',
        'company_reg_number',a.header_snapshot_json->'company_reg_number',
        'company_registration_number',
          a.header_snapshot_json->'company_registration_number',
        'vat_registration_number',
          a.header_snapshot_json->'vat_registration_number',
        'client_id',a.header_snapshot_json->'client_id',
        'client_name',a.header_snapshot_json->'client_name',
        'client_invoice_address',
          a.header_snapshot_json->'client_invoice_address',
        'client_primary_invoice_email',
          a.header_snapshot_json->'client_primary_invoice_email',
        'issued_at_utc',a.header_snapshot_json->'issued_at_utc',
        'due_at_utc',a.header_snapshot_json->'due_at_utc',
        'payment_terms_days',a.header_snapshot_json->'payment_terms_days',
        'vat_chargeable',a.header_snapshot_json->'vat_chargeable',
        'applied_vat_rate_pct',
          a.header_snapshot_json->'applied_vat_rate_pct',
        'hide_bank_footer',a.header_snapshot_json->'hide_bank_footer',
        'bank',jsonb_strip_nulls(jsonb_build_object(
          'name',a.header_snapshot_json#>'{bank,name}',
          'sort_code',a.header_snapshot_json#>'{bank,sort_code}',
          'account_number',a.header_snapshot_json#>'{bank,account_number}')),
        'stationery_key',a.header_snapshot_json->'stationery_key',
        'stationery_margins_mm',
          a.header_snapshot_json->'stationery_margins_mm',
        'attach_policy',jsonb_strip_nulls(jsonb_build_object(
          'ts_attach_to_invoice',
            a.header_snapshot_json#>'{attach_policy,ts_attach_to_invoice}',
          'hr_attach_to_invoice',
            a.header_snapshot_json#>'{attach_policy,hr_attach_to_invoice}',
          'requires_hr',
            a.header_snapshot_json#>'{attach_policy,requires_hr}')),
        'meta',jsonb_strip_nulls(jsonb_build_object(
          'consolidation_mode',
            a.header_snapshot_json#>'{meta,consolidation_mode}',
          'invoice_week_start',
            a.header_snapshot_json#>'{meta,invoice_week_start}',
          'segment_count',a.header_snapshot_json#>'{meta,segment_count}',
          'self_bill',a.header_snapshot_json#>'{meta,self_bill}',
          'source',a.header_snapshot_json#>'{meta,source}',
          'timesheet_count',
            a.header_snapshot_json#>'{meta,timesheet_count}'))
      )) business_header_snapshot
    from assembled a
  )
  select jsonb_build_object(
    'ok',true,
    'source_edit_queue_contract','INVOICE_SOURCE_EDIT_QUEUE_V1',
    'invoice',jsonb_build_object(
      'id',a.id,'type',a.type,'invoice_no',a.invoice_no,'client_id',a.client_id,
      'issued_at_utc',a.issued_at_utc,'due_at_utc',a.due_at_utc,
      'paid_at_utc',a.paid_at_utc,'status',a.status,
      'status_date_utc',a.status_date_utc,'subtotal_ex_vat',a.subtotal_ex_vat,
      'vat_amount',a.vat_amount,'total_inc_vat',a.total_inc_vat,
      'original_invoice_id',a.original_invoice_id,'notes',a.notes,
      'created_at',a.created_at,'updated_at',a.updated_at,
      'invoice_pdf_r2_key',a.invoice_pdf_r2_key,
      'invoice_pdf_generated_at_utc',a.invoice_pdf_generated_at_utc,
      'header_snapshot_json',a.business_header_snapshot,
      'on_hold_reason',a.on_hold_reason,'do_not_send',a.do_not_send,
      'credit_note_created_at_utc',a.credit_note_created_at_utc,
      'document_revision',a.document_revision,'document_state',a.document_state,
      'preview_document_version_id',a.preview_document_version_id,
      'issued_document_version_id',a.issued_document_version_id,
      'active_document_operation_id',a.active_document_operation_id,
      'issue_state',a.issue_state,
      'active_issue_operation_id',a.active_issue_operation_id,
      'last_document_error_json',a.last_document_error_json),
    'items',a.line_rows,'lines',a.line_rows,
    'header_snapshot_json',coalesce(a.business_header_snapshot,'{}'::jsonb),
    'attach_policy',a.business_header_snapshot->'attach_policy',
    'evidence',a.evidence,'timesheet_evidence',a.timesheet_evidence,
    'evidence_other',a.evidence_other,
    'hr_source_rows_cache',a.hr_rows,
    'tsfin_external_source_rows',a.tsfin_rows,
    'segments_on_invoice_by_timesheet',a.segments,
    'segments_by_timesheet',a.segments,
    'history',a.history_rows,
    'tsfin_id_by_timesheet_id',a.tsfin_map,
    'reference_rows',a.reference_rows,
    'timesheet_reference_sources_by_id',a.reference_sources,
    'email_summary',jsonb_build_object(
      'emailed_once',(select count(*)>0 from invoice_mail),
      'email_count',(select count(*) from invoice_mail),
      'last_email_at_utc',(select max(created_at_utc) from invoice_mail)),
    'financial_validation',jsonb_build_object(
      'stored_net',round(coalesce(a.subtotal_ex_vat,0),2),
      'calculated_net',a.net,'stored_vat',round(coalesce(a.vat_amount,0),2),
      'calculated_vat',a.vat,'stored_gross',round(coalesce(a.total_inc_vat,0),2),
      'calculated_gross',a.gross),
    'document_readiness',jsonb_build_object(
      'revision',a.document_revision,'state',a.document_state,
      'preview',a.preview,'issued',a.issued,'latest',a.latest,
      'operation_id',a.document_operation_id,'operation_status',a.document_operation_status,
      'operation_phase',a.document_operation_phase,'operation_progress',a.document_operation_progress,
      'operation_error',coalesce(a.document_operation_error,a.last_document_error_json),
      'change_seq',a.document_change_seq,
      'electronic_timesheet_count',a.electronic_timesheet_count,
      'electronic_timesheet_not_ready_count',a.electronic_timesheet_not_ready_count,
      'manual_timesheet_count',a.manual_timesheet_count,
      'manual_timesheet_not_ready_count',a.manual_timesheet_not_ready_count,
      'evidence_count',a.evidence_count,
      'evidence_unregistered_count',a.evidence_unregistered_count,
      'evidence_not_ready_count',a.evidence_not_ready_count,
      'evidence_failed_count',a.evidence_failed_count,
      'healthroster_support_count',(select count(*) from public.invoice_hr_source_rows h
        where h.invoice_id=p_invoice_id and upper(coalesce(h.source_system,''))='HEALTHROSTER'),
      'nhsp_support_count',(select count(*) from public.invoice_hr_source_rows h
        where h.invoice_id=p_invoice_id and upper(coalesce(h.source_system,''))='NHSP'),
      'higher_rate_support_count',(select count(*) from lines l
        where upper(coalesce(l.meta_json->>'line_type','')) like '%HIGHER_RATE%'),
      'estimated_supporting_pages',a.estimated_timesheet_pages+a.estimated_evidence_pages),
    'issue',jsonb_build_object(
      'state',a.issue_state,'operation_id',a.issue_operation_id,
      'chunk_id',a.issue_chunk_id,'status',a.issue_status,'phase',a.issue_phase,
      'progress',a.issue_progress,'error',a.issue_error,'change_seq',a.issue_change_seq,
      'validation_detail',a.detail_json,
      'document_dependencies',coalesce(a.document_dependency_codes,'[]'::jsonb),
      'delivery_blockers',coalesce(a.delivery_blocker_codes,'[]'::jsonb),
      'route_policy',coalesce(a.route_policy_result,'{}'::jsonb),
      'warnings',coalesce(a.warning_codes,'[]'::jsonb)),
    'blocker_codes',coalesce(a.blocker_codes,'[]'::jsonb),
    'source_edit_blocker_codes',coalesce(a.source_edit_blocker_codes,'[]'::jsonb),
    'source_edit_will_replace_preview',coalesce(a.source_edit_will_replace_preview,false),
    'actions',jsonb_build_object(
      'can_edit',a.status='DRAFT' and a.issue_operation_id is null,
      'can_edit_source',coalesce(a.can_edit_source,false),
      'can_issue',coalesce(a.can_issue_only,false)
        and a.issue_operation_id is null,
      'can_issue_only',coalesce(a.can_issue_only,false)
        and a.issue_operation_id is null,
      'can_issue_and_deliver',coalesce(a.can_issue_and_deliver,false)
        and a.issue_operation_id is null,
      'can_unissue',a.status='ISSUED',
      'can_retry_document',a.document_state='FAILED',
      'can_retry_issue',coalesce(
        a.issue_status in('BLOCKED','FAILED','DEAD_LETTER'),false))
  ) into v_result
  from allowlisted_snapshot a;

  return v_result;
end;
$function$;

-- invoice_eligible_timesheets_for_invoice(uuid)
CREATE OR REPLACE FUNCTION public.invoice_eligible_timesheets_for_invoice(p_invoice_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_client_id uuid;
  v_invoice_week_start date;
  v_invoice_week_end date;

  -- =====================================================
  -- DEBUG (invoice_debug): single audit row per RPC call
  -- =====================================================
  v_invoice_debug boolean := false;
  v_dbg_started_at timestamptz := now();
  v_dbg_steps jsonb := '[]'::jsonb;
  v_dbg_sqlstate text := null;
  v_dbg_error text := null;
  v_dbg_payload jsonb := '{}'::jsonb;

  v_out jsonb := null;
begin
  -- Load invoice_debug flag (safe even if column not yet present)
  begin
    select coalesce(sd.invoice_debug, false)
    into v_invoice_debug
    from public.settings_defaults sd
    limit 1;
  exception when undefined_column then
    v_invoice_debug := false;
  end;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','start',
        'at_utc', public._inv_iso_utc(v_dbg_started_at),
        'invoice_id', coalesce(p_invoice_id::text,'')
      )
    );
  end if;

  -- Load invoice context (client + invoice week)
  select
    i.client_id,
    nullif(btrim(coalesce(i.header_snapshot_json #>> '{meta,invoice_week_start}', '')), '')::date
  into
    v_client_id,
    v_invoice_week_start
  from public.invoices i
  where i.id = p_invoice_id
  limit 1;

  if not found then
    raise exception 'invoice_eligible_timesheets_for_invoice: invoice % not found', p_invoice_id;
  end if;

  if v_invoice_week_start is null then
    raise exception 'invoice_eligible_timesheets_for_invoice: invoice % missing header_snapshot_json.meta.invoice_week_start', p_invoice_id;
  end if;

  v_invoice_week_end := v_invoice_week_start + 6;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','invoice_loaded',
        'invoice_id', p_invoice_id::text,
        'client_id', coalesce(v_client_id::text,''),
        'invoice_week_start', v_invoice_week_start::text,
        'invoice_week_ending', v_invoice_week_end::text
      )
    );
  end if;

  -- =====================================================
  -- MAIN RETURN (UNCHANGED OUTPUT SHAPE; SEGMENTS-empty supported)
  -- =====================================================
  v_out := (
    with base as (
      select
        tf.id as tsfin_id,
        tf.timesheet_id,
        tf.client_id,
        tf.candidate_id,
        ts.week_ending_date::date as timesheet_week_ending_date,
        (ts.week_ending_date::date - 6) as timesheet_week_start,
        ts.hospital_norm,
        ts.submission_mode,
        tf.basis,
        tf.total_hours as tsfin_total_hours,
        tf.total_charge_ex_vat as tsfin_total_charge_ex_vat,
        tf.invoice_breakdown_json,
        upper(coalesce(tf.invoice_breakdown_json->>'mode','')) as invoice_breakdown_mode,
        case
          when upper(coalesce(tf.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
           and jsonb_typeof(tf.invoice_breakdown_json->'segments') = 'array'
            then jsonb_array_length(tf.invoice_breakdown_json->'segments')
          else 0
        end as segments_len,
        s.client_name,
        s.candidate_name,
        s.validation_status,
        coalesce(s.hr_validation_required_for_invoice, false) as hr_validation_required_for_invoice
      from public.timesheets_financials tf
      join public.timesheets ts
        on ts.timesheet_id = tf.timesheet_id
       and ts.is_current = true
      join public.v_ts_invoice_precheck pc
        on pc.timesheet_id = tf.timesheet_id
      left join public.v_timesheets_summary_base s
        on s.timesheet_id = tf.timesheet_id
      where tf.is_current = true
        and tf.client_id = v_client_id
        and tf.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
        and tf.locked_by_invoice_id is null
        and ts.revoked_at is null
        and upper(coalesce(pc.precheck_status, '')) = 'OK'
    ),
    seg_agg as (
      select
        b.timesheet_id,
        sum((coalesce(nullif(seg->>'charge_amount', ''), '0'))::numeric) as invoiceable_charge_ex_vat,
        sum(
            (coalesce(nullif(seg->>'hours_day', ''), '0'))::numeric
          + (coalesce(nullif(seg->>'hours_night', ''), '0'))::numeric
          + (coalesce(nullif(seg->>'hours_sat', ''), '0'))::numeric
          + (coalesce(nullif(seg->>'hours_sun', ''), '0'))::numeric
          + (coalesce(nullif(seg->>'hours_bh', ''), '0'))::numeric
        ) as invoiceable_hours,
        count(*)::int as invoiceable_segments_count
      from base b
      cross join lateral jsonb_array_elements(coalesce(b.invoice_breakdown_json->'segments', '[]'::jsonb)) seg
      where b.invoice_breakdown_mode = 'SEGMENTS'
        -- ✅ Defensive: ignore invalid segment entries (json null/non-object/missing segment_id)
        and jsonb_typeof(seg) = 'object'
        and nullif(btrim(coalesce(seg->>'segment_id','')), '') is not null
        and nullif(btrim(coalesce(seg->>'invoice_locked_invoice_id', '')), '') is null
        and coalesce(
              nullif(btrim(coalesce(seg->>'invoice_target_week_start', '')), '')::date,
              b.timesheet_week_start
            ) = v_invoice_week_start
      group by b.timesheet_id
    ),
    seg_list as (
      select
        b.timesheet_id,
        coalesce(
          jsonb_agg(
            jsonb_build_object(
              'segment_id', coalesce(nullif(btrim(coalesce(seg->>'segment_id','')), ''), null),
              'date',        coalesce(nullif(btrim(coalesce(seg->>'date','')), ''), null),
              'start_utc',   coalesce(nullif(btrim(coalesce(seg->>'start_utc','')), ''), null),
              'end_utc',     coalesce(nullif(btrim(coalesce(seg->>'end_utc','')), ''), null),
              'break_mins',  (coalesce(nullif(seg->>'break_mins',''), nullif(seg->>'break_minutes',''), '0'))::numeric,
              'ref_num',     coalesce(nullif(btrim(coalesce(seg->>'ref_num','')), ''), null),
              'charge_amount', (coalesce(nullif(seg->>'charge_amount',''), '0'))::numeric,
              'pay_amount',    (coalesce(nullif(seg->>'pay_amount',''), '0'))::numeric,
              'hours_day',   (coalesce(nullif(seg->>'hours_day',''), '0'))::numeric,
              'hours_night', (coalesce(nullif(seg->>'hours_night',''), '0'))::numeric,
              'hours_sat',   (coalesce(nullif(seg->>'hours_sat',''), '0'))::numeric,
              'hours_sun',   (coalesce(nullif(seg->>'hours_sun',''), '0'))::numeric,
              'hours_bh',    (coalesce(nullif(seg->>'hours_bh',''), '0'))::numeric,
              'invoice_target_week_start', coalesce(nullif(btrim(coalesce(seg->>'invoice_target_week_start','')), ''), null)
            )
            order by
              coalesce(seg->>'date','') asc,
              coalesce(seg->>'start_utc','') asc,
              coalesce(seg->>'end_utc','') asc,
              coalesce(seg->>'segment_id','') asc
          ),
          '[]'::jsonb
        ) as eligible_segments
      from base b
      cross join lateral jsonb_array_elements(coalesce(b.invoice_breakdown_json->'segments', '[]'::jsonb)) seg
      where b.invoice_breakdown_mode = 'SEGMENTS'
        -- ✅ Defensive: ignore invalid segment entries (json null/non-object/missing segment_id)
        and jsonb_typeof(seg) = 'object'
        and nullif(btrim(coalesce(seg->>'segment_id','')), '') is not null
        and nullif(btrim(coalesce(seg->>'invoice_locked_invoice_id', '')), '') is null
        and coalesce(
              nullif(btrim(coalesce(seg->>'invoice_target_week_start', '')), '')::date,
              b.timesheet_week_start
            ) = v_invoice_week_start
      group by b.timesheet_id
    ),
    eligible as (
      select
        b.tsfin_id,
        b.timesheet_id,
        b.client_id,
        b.candidate_id,
        b.client_name,
        b.candidate_name,
        b.hospital_norm,
        b.submission_mode,
        b.basis,
        b.validation_status,
        b.hr_validation_required_for_invoice,
        (
          b.hr_validation_required_for_invoice
          and b.validation_status is distinct from 'VALIDATION_OK'::public.validation_status_enum
          and b.validation_status is distinct from 'OVERRIDDEN'::public.validation_status_enum
        ) as blocked_by_hr_validation,
        b.invoice_breakdown_mode as invoice_breakdown_mode,
        case
          when b.invoice_breakdown_mode = 'SEGMENTS' and b.segments_len > 0
            then coalesce(sa.invoiceable_hours, 0)
          else coalesce(b.tsfin_total_hours, 0)
        end::numeric as invoiceable_hours,
        case
          when b.invoice_breakdown_mode = 'SEGMENTS' and b.segments_len > 0
            then coalesce(sa.invoiceable_charge_ex_vat, 0)
          else coalesce(b.tsfin_total_charge_ex_vat, 0)
        end::numeric as invoiceable_charge_ex_vat,
        case
          when b.invoice_breakdown_mode = 'SEGMENTS' and b.segments_len > 0
            then coalesce(sa.invoiceable_segments_count, 0)
          else 0
        end as invoiceable_segments_count,
        case
          when b.invoice_breakdown_mode = 'SEGMENTS' and b.segments_len > 0
            then coalesce(sl.eligible_segments, '[]'::jsonb)
          else '[]'::jsonb
        end as eligible_segments,
        b.timesheet_week_ending_date
      from base b
      left join seg_agg sa on sa.timesheet_id = b.timesheet_id
      left join seg_list sl on sl.timesheet_id = b.timesheet_id
      where
        (
          -- SEGMENTS with real segments (existing behaviour)
          (
            b.invoice_breakdown_mode = 'SEGMENTS'
            and b.segments_len > 0
            and (coalesce(sa.invoiceable_hours, 0) <> 0 or coalesce(sa.invoiceable_charge_ex_vat, 0) <> 0)
          )
          -- ✅ NEW: SEGMENTS-empty fallback (expense-only / no segments)
          or
          (
            b.invoice_breakdown_mode = 'SEGMENTS'
            and b.segments_len = 0
            and b.timesheet_week_start = v_invoice_week_start
            and (coalesce(b.tsfin_total_hours, 0) <> 0 or coalesce(b.tsfin_total_charge_ex_vat, 0) <> 0)
          )
          -- Non-SEGMENTS (existing behaviour)
          or
          (
            b.invoice_breakdown_mode <> 'SEGMENTS'
            and b.timesheet_week_start = v_invoice_week_start
          )
        )
        and not (
          b.hr_validation_required_for_invoice
          and b.validation_status is distinct from 'VALIDATION_OK'::public.validation_status_enum
          and b.validation_status is distinct from 'OVERRIDDEN'::public.validation_status_enum
        )
    )

    select jsonb_build_object(
      'invoice_id', p_invoice_id,
      'client_id', v_client_id,
      'invoice_week_start', to_char(v_invoice_week_start, 'YYYY-MM-DD'),
      'invoice_week_ending', to_char(v_invoice_week_end, 'YYYY-MM-DD'),
      'timesheets', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'timesheet_id', e.timesheet_id::text,
            'tsfin_id', case when e.tsfin_id is null then null else e.tsfin_id::text end,
            'candidate_id', e.candidate_id::text,
            'client_name', e.client_name,
            'candidate_name', e.candidate_name,
            'hospital_norm', e.hospital_norm,
            'submission_mode', e.submission_mode,
            'basis', e.basis,

            'invoice_week_start', to_char(v_invoice_week_start, 'YYYY-MM-DD'),
            'invoice_week_ending', to_char(v_invoice_week_end, 'YYYY-MM-DD'),

            'source_week_ending_date', case when e.timesheet_week_ending_date is null then null else to_char(e.timesheet_week_ending_date, 'YYYY-MM-DD') end,

            'invoiceable_hours', round(e.invoiceable_hours, 2),
            'invoiceable_charge_ex_vat', round(e.invoiceable_charge_ex_vat, 2),
            'invoice_breakdown_mode', e.invoice_breakdown_mode,
            'invoiceable_segments_count', e.invoiceable_segments_count,

            'eligible_segments', e.eligible_segments,

            'hr_validation_required_for_invoice', e.hr_validation_required_for_invoice,
            'blocked_by_hr_validation', e.blocked_by_hr_validation,
            'validation_status', e.validation_status
          )
          order by e.candidate_name nulls last, e.timesheet_week_ending_date asc nulls last, e.timesheet_id::text
        )
        from (
          select * from eligible
        ) e
      ), '[]'::jsonb)
    )
  );

  -- =====================================================
  -- DEBUG AUDIT (NO OUTPUT CHANGES)
  -- =====================================================
  if v_invoice_debug then
    begin
      with base as (
        select
          tf.id as tsfin_id,
          tf.timesheet_id,
          tf.client_id,
          tf.candidate_id,
          ts.week_ending_date::date as timesheet_week_ending_date,
          (ts.week_ending_date::date - 6) as timesheet_week_start,
          tf.total_hours as tsfin_total_hours,
          tf.total_charge_ex_vat as tsfin_total_charge_ex_vat,
          tf.invoice_breakdown_json,
          upper(coalesce(tf.invoice_breakdown_json->>'mode','')) as invoice_breakdown_mode,
          case
            when upper(coalesce(tf.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
             and jsonb_typeof(tf.invoice_breakdown_json->'segments') = 'array'
              then jsonb_array_length(tf.invoice_breakdown_json->'segments')
            else 0
          end as segments_len,
          s.client_name,
          s.candidate_name,
          s.validation_status,
          coalesce(s.hr_validation_required_for_invoice, false) as hr_validation_required_for_invoice
        from public.timesheets_financials tf
        join public.timesheets ts
          on ts.timesheet_id = tf.timesheet_id
         and ts.is_current = true
        join public.v_ts_invoice_precheck pc
          on pc.timesheet_id = tf.timesheet_id
        left join public.v_timesheets_summary_base s
          on s.timesheet_id = tf.timesheet_id
        where tf.is_current = true
          and tf.client_id = v_client_id
          and tf.processing_status = 'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
          and tf.locked_by_invoice_id is null
          and ts.revoked_at is null
          and upper(coalesce(pc.precheck_status, '')) = 'OK'
      ),
      seg_stats as (
        select
          b.timesheet_id,
          count(*)::int as seg_total,
          count(*) filter (
            where nullif(btrim(coalesce(seg->>'invoice_locked_invoice_id','')), '') is null
          )::int as seg_unlocked_total,
          count(*) filter (
            where nullif(btrim(coalesce(seg->>'invoice_locked_invoice_id','')), '') is null
              and coalesce(
                    nullif(btrim(coalesce(seg->>'invoice_target_week_start','')), '')::date,
                    b.timesheet_week_start
                  ) = v_invoice_week_start
          )::int as seg_unlocked_for_invoice_week,
          count(*) filter (
            where nullif(btrim(coalesce(seg->>'invoice_locked_invoice_id','')), '') is null
              and coalesce(
                    nullif(btrim(coalesce(seg->>'invoice_target_week_start','')), '')::date,
                    b.timesheet_week_start
                  ) <> v_invoice_week_start
          )::int as seg_unlocked_other_week
        from base b
        cross join lateral jsonb_array_elements(coalesce(b.invoice_breakdown_json->'segments','[]'::jsonb)) seg
        where b.invoice_breakdown_mode = 'SEGMENTS'
          -- ✅ Defensive (debug too): only count valid segment objects
          and jsonb_typeof(seg) = 'object'
          and nullif(btrim(coalesce(seg->>'segment_id','')), '') is not null
        group by b.timesheet_id
      ),
      seg_agg as (
        select
          b.timesheet_id,
          sum((coalesce(nullif(seg->>'charge_amount', ''), '0'))::numeric) as invoiceable_charge_ex_vat,
          sum(
              (coalesce(nullif(seg->>'hours_day', ''), '0'))::numeric
            + (coalesce(nullif(seg->>'hours_night', ''), '0'))::numeric
            + (coalesce(nullif(seg->>'hours_sat', ''), '0'))::numeric
            + (coalesce(nullif(seg->>'hours_sun', ''), '0'))::numeric
            + (coalesce(nullif(seg->>'hours_bh', ''), '0'))::numeric
          ) as invoiceable_hours,
          count(*)::int as invoiceable_segments_count
        from base b
        cross join lateral jsonb_array_elements(coalesce(b.invoice_breakdown_json->'segments', '[]'::jsonb)) seg
        where b.invoice_breakdown_mode = 'SEGMENTS'
          -- ✅ Defensive (debug too): ignore invalid segment entries
          and jsonb_typeof(seg) = 'object'
          and nullif(btrim(coalesce(seg->>'segment_id','')), '') is not null
          and nullif(btrim(coalesce(seg->>'invoice_locked_invoice_id', '')), '') is null
          and coalesce(
                nullif(btrim(coalesce(seg->>'invoice_target_week_start', '')), '')::date,
                b.timesheet_week_start
              ) = v_invoice_week_start
        group by b.timesheet_id
      ),
      eligible_ids as (
        select b.timesheet_id
        from base b
        left join seg_agg sa on sa.timesheet_id = b.timesheet_id
        where
          (
            (
              b.invoice_breakdown_mode = 'SEGMENTS'
              and b.segments_len > 0
              and (coalesce(sa.invoiceable_hours, 0) <> 0 or coalesce(sa.invoiceable_charge_ex_vat, 0) <> 0)
            )
            or
            (
              b.invoice_breakdown_mode = 'SEGMENTS'
              and b.segments_len = 0
              and b.timesheet_week_start = v_invoice_week_start
              and (coalesce(b.tsfin_total_hours, 0) <> 0 or coalesce(b.tsfin_total_charge_ex_vat, 0) <> 0)
            )
            or
            (
              b.invoice_breakdown_mode <> 'SEGMENTS'
              and b.timesheet_week_start = v_invoice_week_start
            )
          )
          and not (
            b.hr_validation_required_for_invoice
            and b.validation_status is distinct from 'VALIDATION_OK'::public.validation_status_enum
            and b.validation_status is distinct from 'OVERRIDDEN'::public.validation_status_enum
          )
      ),
      excluded as (
        select
          b.timesheet_id,
          b.tsfin_id,
          b.candidate_id,
          b.candidate_name,
          b.client_name,
          b.invoice_breakdown_mode,
          b.segments_len,
          b.timesheet_week_start,
          b.timesheet_week_ending_date,
          b.hr_validation_required_for_invoice,
          b.validation_status,
          coalesce(ss.seg_total,0) as seg_total,
          coalesce(ss.seg_unlocked_total,0) as seg_unlocked_total,
          coalesce(ss.seg_unlocked_for_invoice_week,0) as seg_unlocked_for_invoice_week,
          coalesce(ss.seg_unlocked_other_week,0) as seg_unlocked_other_week,
          case
            when (
              b.hr_validation_required_for_invoice
              and b.validation_status is distinct from 'VALIDATION_OK'::public.validation_status_enum
              and b.validation_status is distinct from 'OVERRIDDEN'::public.validation_status_enum
            ) then 'HR_VALIDATION_BLOCKED'
            when b.invoice_breakdown_mode = 'SEGMENTS' and b.segments_len = 0
              and b.timesheet_week_start <> v_invoice_week_start then 'SEGMENTS_EMPTY_WEEK_MISMATCH'
            when b.invoice_breakdown_mode = 'SEGMENTS' and b.segments_len = 0
              and (coalesce(b.tsfin_total_hours,0) = 0 and coalesce(b.tsfin_total_charge_ex_vat,0) = 0) then 'SEGMENTS_EMPTY_ZERO_TOTALS'
            when b.invoice_breakdown_mode = 'SEGMENTS'
              and coalesce(ss.seg_unlocked_for_invoice_week,0) = 0 then 'NO_UNLOCKED_SEGMENTS_FOR_INVOICE_WEEK'
            when b.invoice_breakdown_mode <> 'SEGMENTS'
              and b.timesheet_week_start <> v_invoice_week_start then 'NON_SEGMENTS_WEEK_MISMATCH'
            else 'OTHER'
          end as exclude_reason
        from base b
        left join seg_stats ss on ss.timesheet_id = b.timesheet_id
        left join eligible_ids ei on ei.timesheet_id = b.timesheet_id
        where ei.timesheet_id is null
      )
      select jsonb_build_object(
        'invoice_id', p_invoice_id::text,
        'client_id', coalesce(v_client_id::text,''),
        'invoice_week_start', v_invoice_week_start::text,
        'invoice_week_ending', v_invoice_week_end::text,
        'counts', jsonb_build_object(
          'base_timesheets', (select count(*)::int from base),
          'eligible_timesheets', (select count(*)::int from eligible_ids),
          'returned_timesheets', case
            when v_out is not null and jsonb_typeof(v_out->'timesheets')='array' then jsonb_array_length(v_out->'timesheets')
            else 0
          end,
          'excluded_timesheets', (select count(*)::int from excluded)
        ),
        'excluded_samples', coalesce(
          (
            select jsonb_agg(
              jsonb_build_object(
                'timesheet_id', e.timesheet_id::text,
                'tsfin_id', case when e.tsfin_id is null then null else e.tsfin_id::text end,
                'candidate_id', case when e.candidate_id is null then null else e.candidate_id::text end,
                'candidate_name', e.candidate_name,
                'client_name', e.client_name,
                'invoice_breakdown_mode', e.invoice_breakdown_mode,
                'segments_len', e.segments_len,
                'timesheet_week_start', case when e.timesheet_week_start is null then null else e.timesheet_week_start::text end,
                'timesheet_week_ending', case when e.timesheet_week_ending_date is null then null else e.timesheet_week_ending_date::text end,
                'hr_validation_required_for_invoice', coalesce(e.hr_validation_required_for_invoice,false),
                'validation_status', case when e.validation_status is null then null else e.validation_status::text end,
                'seg_total', e.seg_total,
                'seg_unlocked_total', e.seg_unlocked_total,
                'seg_unlocked_for_invoice_week', e.seg_unlocked_for_invoice_week,
                'seg_unlocked_other_week', e.seg_unlocked_other_week,
                'exclude_reason', e.exclude_reason
              )
            )
            from (
              select *
              from excluded
              order by candidate_name nulls last, timesheet_week_ending_date asc nulls last, timesheet_id::text
              limit 50
            ) e
          ),
          '[]'::jsonb
        )
      )
      into v_dbg_payload;

      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','finish',
          'at_utc', public._inv_iso_utc(now()),
          'debug', coalesce(v_dbg_payload,'{}'::jsonb)
        )
      );

      perform public._inv_write_audit(
        null,
        'INVOICE_ELIGIBLE_TIMESHEETS_DEBUG',
        jsonb_build_object(
          'invoice_id', p_invoice_id::text,
          'debug', coalesce(v_dbg_payload,'{}'::jsonb),
          'steps', v_dbg_steps
        ),
        'invoices',
        p_invoice_id::text,
        null,
        'INVOICE_DEBUG',
        null, null, null
      );
    exception when others then
      null;
    end;
  end if;

  return v_out;

exception when others then
  v_dbg_sqlstate := SQLSTATE;
  v_dbg_error := SQLERRM;

  if v_invoice_debug then
    begin
      perform public._inv_write_audit(
        null,
        'INVOICE_ELIGIBLE_TIMESHEETS_ERROR',
        jsonb_build_object(
          'invoice_id', coalesce(p_invoice_id::text,''),
          'sqlstate', v_dbg_sqlstate,
          'error', v_dbg_error,
          'steps', v_dbg_steps
        ),
        'invoices',
        coalesce(p_invoice_id::text,''),
        null,
        'INVOICE_DEBUG',
        null, null, null
      );
    exception when others then
      null;
    end;
  end if;

  raise;
end;
$function$;

