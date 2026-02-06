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
      null::text as ref_num,
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




create or replace function public.hr_weekly_phase3_apply_adjustment_truth(
  p_import_id uuid,
  p_selected_external_row_keys text[],
  p_decisions jsonb,
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

  v_correction_id text;
  v_kind text;

  v_old_start_utc timestamptz;
  v_old_end_utc   timestamptz;
  v_new_start_utc timestamptz;
  v_new_end_utc   timestamptz;
  v_old_break_mins int;
  v_new_break_mins int;

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
$$;

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
  v_decisions jsonb := coalesce(v_payload->'decisions', '{}'::jsonb);
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

  -- Phase 3
  v_phase3_proceed_keys text[] := array[]::text[];
  v_phase3_skip_keys text[] := array[]::text[];

  v_phase3_result jsonb := null;

  -- Phase 1 / 1.5
  v_phase1_result jsonb := null;
  v_phase15_ok int := 0;
  v_phase15_updated int := 0;

  -- cancellations
  v_cancel_actions jsonb := '[]'::jsonb;
  v_cancellations_result jsonb := null;

  -- mirror (Mode A)
  v_mirror_result jsonb := null;

  -- validation (Mode A)
  v_weekly_val_payload jsonb := null;
  v_validations_upserted int := 0;
  v_mismatched_tsids uuid[] := array[]::uuid[];

  -- email (Mode A best-effort later; log is transactional here)
  v_email_jobs jsonb := '[]'::jsonb;

  -- affected timesheets for TSFIN drain (Strategy A done in Worker post-commit)
  v_affected_timesheet_ids uuid[] := array[]::uuid[];

  -- policy A replacement-day
  v_selected_cancel_shift_id_set text[] := array[]::text[];

begin
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

  -- ─────────────────────────────────────────────
  -- 1) Parse and normalize selection payload (ROW:/CANCEL:)
  -- ─────────────────────────────────────────────
  if jsonb_typeof(v_actions_json) <> 'array' then
    raise exception 'hr_weekly_apply_transactional: selected_action_ids must be a JSON array.';
  end if;

  if jsonb_typeof(v_actions2_json) <> 'array' then
    raise exception 'hr_weekly_apply_transactional: selected_actions must be a JSON array.';
  end if;

  if jsonb_typeof(v_decisions) <> 'object' then
    raise exception 'hr_weekly_apply_transactional: decisions must be a JSON object.';
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

  -- Validate action_id format
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

  -- Group map (MODE derived per group)
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
    select distinct p2.contract_id, p2.candidate_id, p2.client_id, p2.week_ending_date
    from tmp_p2_ok p2
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
    p2.external_row_key,
    p2.candidate_id,
    p2.client_id,
    p2.contract_id,
    p2.week_ending_date,
    p2.work_date,
    gm.group_id,
    gm.mode
  from tmp_p2_ok p2
  join tmp_group_mode gm
    on gm.contract_id = p2.contract_id
   and gm.candidate_id = p2.candidate_id
   and gm.week_ending_date = p2.week_ending_date;

  select coalesce(array_agg(distinct m.external_row_key order by m.external_row_key), array[]::text[])
  into v_mode_a_external_keys
  from tmp_p2_ok_mode m
  where m.mode = 'MODE_A';

  select coalesce(array_agg(distinct m.external_row_key order by m.external_row_key), array[]::text[])
  into v_mode_b_external_keys
  from tmp_p2_ok_mode m
  where m.mode = 'MODE_B';

  -- Validate any selected truth keys must be MODE_B
  if exists (
    select 1
    from unnest(v_selected_truth_keys) as k(external_row_key)
    left join (select distinct m.external_row_key from tmp_p2_ok_mode m where m.mode='MODE_B') as mb
      on mb.external_row_key = k.external_row_key
    where mb.external_row_key is null
  ) then
    raise exception 'hr_weekly_apply_transactional: selection includes ROW:<external_row_key> that is not MODE_B (timesheet required).';
  end if;

  select coalesce(array_agg(k.external_row_key order by k.external_row_key), array[]::text[])
  into v_selected_truth_keys_mode_b
  from (
    select distinct k.external_row_key
    from unnest(v_selected_truth_keys) as k(external_row_key)
    join (select distinct m.external_row_key from tmp_p2_ok_mode m where m.mode='MODE_B') as mb
      on mb.external_row_key = k.external_row_key
  ) as k;

  -- ─────────────────────────────────────────────
  -- 3) Policy A replacement-day enforcement (MODE_B only)
  -- ─────────────────────────────────────────────
  if array_length(v_selected_truth_keys_mode_b, 1) is not null then
    create temporary table tmp_sel_truth_p2 on commit drop as
    select
      m.external_row_key,
      m.candidate_id,
      m.client_id,
      m.work_date as import_work_date
    from tmp_p2_ok_mode m
    where m.mode = 'MODE_B'
      and m.external_row_key = any(v_selected_truth_keys_mode_b);

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
      and ns.external_row_key = any(v_selected_truth_keys_mode_b)
      and ns.work_date is not null
    order by ns.external_row_key, ns.updated_at desc nulls last, ns.created_at desc nulls last;

    create temporary table tmp_selected_replacement_keys on commit drop as
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

    -- selected cancel ids set (as text) for membership checks
    select coalesce(array_agg(x::text), array[]::text[])
    into v_selected_cancel_shift_id_set
    from unnest(v_selected_cancel_shift_ids) as x;

    if exists (select 1 from tmp_selected_replacement_keys) then
      create temporary table tmp_required_cancel_ids on commit drop as
      select distinct
        rk.replacement_day_key,
        ns2.id as shift_id
      from tmp_selected_replacement_keys rk
      join public.nhsp_shifts ns2
        on ns2.source_system = 'HEALTHROSTER'::public.hr_source_enum
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
        raise exception 'hr_weekly_apply_transactional: Policy A violation (replacement-day selected without selecting all required cancellations).';
      end if;
    end if;
  end if;

  -- ─────────────────────────────────────────────
  -- 4) Phase 3 decisions validation (selection-aware) + derive force/skip keys
  -- ─────────────────────────────────────────────
  create temporary table tmp_p3_sel on commit drop as
  select *
  from public.weekly_import_changed_hours_phase3(p_import_id := p_import_id, p_system_type := 'HEALTHROSTER')
  where external_row_key = any(v_selected_truth_keys_mode_b);

  -- Reject decision keys that are not present in phase3 rows (within selected truth keys)
  create temporary table tmp_decision_keys on commit drop as
  select k.key_text as external_row_key
  from (
    select jsonb_object_keys(v_decisions) as key_text
  ) as k
  where k.key_text is not null and btrim(k.key_text) <> '';

  if exists (
    select 1
    from tmp_decision_keys dk
    left join tmp_p3_sel p3
      on p3.external_row_key = dk.external_row_key
    where p3.external_row_key is null
  ) then
    raise exception 'hr_weekly_apply_transactional: decisions include unknown external_row_key(s) for this selection (no phase3 row).';
  end if;

  create temporary table tmp_phase3_keys on commit drop as
  select
    p3.external_row_key,
    (p3.requires_any_decision is true) as requires_any_decision,
    (p3.is_invoiced is true) as is_invoiced
  from tmp_p3_sel p3;

  if exists (
    select 1
    from tmp_phase3_keys pk
    where pk.requires_any_decision is true
      and (v_decisions ? pk.external_row_key) is not true
  ) then
    raise exception 'hr_weekly_apply_transactional: missing decision object for one or more selected decision-required rows.';
  end if;

  create temporary table tmp_decision_eval on commit drop as
  select
    pk.external_row_key,
    pk.is_invoiced,
    case
      when pk.requires_any_decision is not true then null
      else
        case
          when jsonb_typeof(v_decisions->pk.external_row_key) <> 'object' then null
          else
            case
              when lower(coalesce((v_decisions->pk.external_row_key)->>'skip','')) in ('true','1') then true
              when lower(coalesce((v_decisions->pk.external_row_key)->>'skip','')) in ('false','0') then false
              else null
            end
        end
    end as skip_bool,
    nullif(btrim((v_decisions->pk.external_row_key)->>'credit_week_start'), '') as credit_week_start,
    nullif(btrim((v_decisions->pk.external_row_key)->>'reinvoice_week_start'), '') as reinvoice_week_start
  from tmp_phase3_keys pk;

  if exists (
    select 1
    from tmp_decision_eval de
    where de.skip_bool is null
      and exists (select 1 from tmp_phase3_keys pk2 where pk2.external_row_key = de.external_row_key and pk2.requires_any_decision is true)
  ) then
    raise exception 'hr_weekly_apply_transactional: invalid decision skip boolean for one or more keys.';
  end if;

  if exists (
    select 1
    from tmp_decision_eval de
    where de.is_invoiced is true
      and de.skip_bool is false
      and (de.credit_week_start is null or de.reinvoice_week_start is null)
  ) then
    raise exception 'hr_weekly_apply_transactional: missing credit_week_start/reinvoice_week_start for invoiced PROCEED decision.';
  end if;

  if exists (
    select 1
    from tmp_decision_eval de
    where de.is_invoiced is true
      and de.skip_bool is false
      and extract(isodow from de.credit_week_start::date) <> 1
  ) then
    raise exception 'hr_weekly_apply_transactional: credit_week_start must be a Monday for invoiced PROCEED decision.';
  end if;

  if exists (
    select 1
    from tmp_decision_eval de
    where de.is_invoiced is true
      and de.skip_bool is false
      and extract(isodow from de.reinvoice_week_start::date) <> 1
  ) then
    raise exception 'hr_weekly_apply_transactional: reinvoice_week_start must be a Monday for invoiced PROCEED decision.';
  end if;

  select coalesce(array_agg(de.external_row_key order by de.external_row_key), array[]::text[])
  into v_phase3_skip_keys
  from tmp_decision_eval de
  where de.skip_bool is true;

  select coalesce(array_agg(k.external_row_key order by k.external_row_key), array[]::text[])
  into v_force_keys_final
  from (
    select distinct st.external_row_key
    from unnest(v_selected_truth_keys_mode_b) as st(external_row_key)
    left join unnest(v_phase3_skip_keys) as sk(external_row_key)
      on sk.external_row_key = st.external_row_key
    where sk.external_row_key is null
  ) as k;

  select coalesce(array_agg(x.external_row_key order by x.external_row_key), array[]::text[])
  into v_skip_keys_final
  from (
    select distinct allk.external_row_key
    from (
      select unnest(coalesce(v_mode_a_external_keys, array[]::text[])) as external_row_key
      union all
      select unnest(coalesce(v_mode_b_external_keys, array[]::text[])) as external_row_key
    ) as allk
    left join unnest(coalesce(v_force_keys_final, array[]::text[])) as fk(external_row_key)
      on fk.external_row_key = allk.external_row_key
    where fk.external_row_key is null
  ) as x;

  select coalesce(array_agg(de.external_row_key order by de.external_row_key), array[]::text[])
  into v_phase3_proceed_keys
  from tmp_decision_eval de
  where de.skip_bool is false;

  -- ─────────────────────────────────────────────
  -- 5) Apply Phase 3 adjustment truth (selected PROCEED keys only)
  -- ─────────────────────────────────────────────
  if array_length(v_phase3_proceed_keys, 1) is not null then
    select public.hr_weekly_phase3_apply_adjustment_truth(
      p_import_id := p_import_id,
      p_selected_external_row_keys := v_phase3_proceed_keys,
      p_decisions := v_decisions,
      p_actor_user_id := p_actor_user_id
    )
    into v_phase3_result;
  end if;

  -- ─────────────────────────────────────────────
  -- 6) Mode B truth apply pipeline (Phase 1 + Phase 1.5)
  -- ─────────────────────────────────────────────
  if array_length(v_force_keys_final, 1) is not null then
    select public.hr_autoprocess_apply_phase1(
      import_id := p_import_id,
      selected_group_ids := array[]::text[],
      p_skip_external_row_keys := v_skip_keys_final,
      p_force_overwrite_external_row_keys := v_force_keys_final
    )
    into v_phase1_result;

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
  end if;

  -- ─────────────────────────────────────────────
  -- 7) Apply selected cancellations (explicit shift_id only)
  -- ─────────────────────────────────────────────
  if array_length(v_selected_cancel_shift_ids, 1) is not null then
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
    from (
      select
        (t.candidate_id::text || '|' || t.client_id::text || '|' || t.old_work_date::text) as replacement_day_key
      from (
        select distinct
          ns2.candidate_id,
          ns2.client_id,
          ns2.work_date as old_work_date
        from tmp_existing_by_key ebk
        join tmp_sel_truth_p2 st2
          on st2.external_row_key = ebk.external_row_key
        join public.nhsp_shifts ns2
          on ns2.external_row_key = ebk.external_row_key
         and ns2.id = ebk.shift_id
        where ebk.old_work_date is not null
          and st2.import_work_date is not null
          and ebk.old_work_date <> st2.import_work_date
      ) as t
    ) as rk;

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
  end if;

  -- ─────────────────────────────────────────────
  -- 8) Mode A mirror ingestion (deterministic only; timesheet_id forced NULL inside mirror RPC)
  -- ─────────────────────────────────────────────
  if array_length(v_mode_a_external_keys, 1) is not null then
    select public.hr_weekly_mirror_upsert_deterministic(
      p_import_id := p_import_id,
      p_external_row_keys := v_mode_a_external_keys,
      p_actor_user_id := p_actor_user_id
    )
    into v_mirror_result;
  end if;

  -- ─────────────────────────────────────────────
  -- 9) Mode A weekly validation upserts (required, transactional)
  --     FIX: Mode A classification must use the candidate_id/contract_id/week_ending_date
  --     from hr_weekly_validation_preview rows, joined directly to tmp_group_mode.
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

  -- ─────────────────────────────────────────────
  -- 10) Transactional hr_issue_emails upsert for requested email_actions
  --      (Email/Re-email state; actual send is post-commit)
  -- ─────────────────────────────────────────────
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

  -- ─────────────────────────────────────────────
  -- 11) Compute affected_timesheet_ids for TSFIN Strategy A drain (Worker drains to completion)
  -- ─────────────────────────────────────────────
  create temporary table tmp_aff_ts on commit drop as
  select distinct t.timesheet_id
  from (
    select (x.value)::uuid as timesheet_id
    from jsonb_array_elements_text(coalesce(v_cancellations_result->'affected_timesheet_ids', '[]'::jsonb)) as x(value)

    union all

    select (x2.value)::uuid as timesheet_id
    from jsonb_array_elements_text(coalesce(v_phase3_result->'created_timesheet_ids', '[]'::jsonb)) as x2(value)

    union all

    select (x3.value)::uuid as timesheet_id
    from jsonb_array_elements_text(coalesce(v_phase3_result->'updated_timesheet_ids', '[]'::jsonb)) as x3(value)

    union all

    select ns.timesheet_id as timesheet_id
    from public.nhsp_shifts ns
    where ns.source_system = 'HEALTHROSTER'::public.hr_source_enum
      and ns.client_id = v_import_client_id
      and ns.cancelled_at_utc is null
      and ns.external_row_key = any(coalesce(v_force_keys_final, array[]::text[]))
      and ns.timesheet_id is not null
  ) as t
  where t.timesheet_id is not null;

  select coalesce(array_agg(distinct a.timesheet_id order by a.timesheet_id), array[]::uuid[])
  into v_affected_timesheet_ids
  from tmp_aff_ts a;

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

  v_timesheet_id uuid;
  v_invoice_id uuid;

  v_shift_source_system text;
  v_shift_candidate_id uuid;
  v_shift_client_id uuid;
  v_shift_work_date date;
  v_shift_cancelled_at timestamptz;

  v_has_any_rows_on_date boolean;
  v_has_candidate_rows_on_date boolean;
  v_ambiguous_rows_on_date int;

  v_import_has_any_mapped_rows_for_client boolean;

  v_timesheet_ids uuid[] := array[]::uuid[];
  v_invoice_ids uuid[] := array[]::uuid[];
  v_credit_note_ids uuid[] := array[]::uuid[];
  v_pdf_jobs_enqueued int := 0;

begin
  -- Validate import exists and is NHSP
  select
    upper(coalesce(hi.source_system, '')),
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

  -- Preload Phase2 mapping joined to hr_rows.date_local so we can enforce:
  -- - "day empty in file" using hr_rows file truth
  -- - unresolved mapping on that day => ambiguous => reject cancellation
  create temporary table tmp_p2_join on commit drop as
  select
    r.id as hr_row_id,
    r.date_local as date_local,
    p2.candidate_id as candidate_id,
    p2.client_id as client_id,
    p2.work_date as work_date,
    upper(coalesce(p2.action::text, '')) as action
  from public.hr_rows r
  left join public.weekly_import_phase2(
    p_import_id := p_import_id,
    p_system_type := 'NHSP'
  ) as p2
    on p2.hr_row_id = r.id
  where r.import_id = p_import_id;

  create temporary table tmp_date_stats on commit drop as
  select
    t.date_local as date_local,
    count(*)::int as file_rows_count,
    count(*) filter (
      where (t.candidate_id is null) or (t.client_id is null) or (t.work_date is null)
    )::int as ambiguous_rows_count
  from tmp_p2_join t
  group by t.date_local;

  -- Apply is transactional: any invalid selected row fails the whole apply.
  for v_idx, v_item in
    select (e.ord)::int, e.value
    from jsonb_array_elements(v_actions) with ordinality as e(value, ord)
  loop
    -- Extract shift_id and reason
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

    if v_reason is null then
      v_reason := 'MISSING_FROM_IMPORT';
    end if;

    -- Load shift with row lock
    select
      ns.timesheet_id,
      ns.invoice_id,
      upper(coalesce(ns.source_system::text,'')) as shift_source_system,
      ns.candidate_id,
      ns.client_id,
      ns.work_date,
      ns.cancelled_at_utc
    into
      v_timesheet_id,
      v_invoice_id,
      v_shift_source_system,
      v_shift_candidate_id,
      v_shift_client_id,
      v_shift_work_date,
      v_shift_cancelled_at
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

    if v_shift_candidate_id is null or v_shift_client_id is null or v_shift_work_date is null then
      raise exception 'nhsp_weekly_apply_cancellations: item % shift % missing candidate_id/client_id/work_date; cannot enforce day-empty policy.',
        v_idx, v_shift_id;
    end if;

    -- Import scope safety (NHSP imports may span multiple clients; do not rely on hr_imports.client_id):
    -- Require the import has at least one deterministically-mapped row for this client_id anywhere in the file.
    select exists (
      select 1
      from tmp_p2_join pj
      where pj.client_id = v_shift_client_id
        and pj.candidate_id is not null
        and pj.work_date is not null
    )
    into v_import_has_any_mapped_rows_for_client;

    if not v_import_has_any_mapped_rows_for_client then
      raise exception
        'nhsp_weekly_apply_cancellations: item % shift % client_id=% is out-of-scope for this import (no deterministically mapped rows for this client in file).',
        v_idx, v_shift_id, v_shift_client_id;
    end if;

    -- Day-empty enforcement (file truth):
    -- Allow cancellation only if we can deterministically prove the import contains zero shifts
    -- for this candidate+client+work_date. If mapping for that date is incomplete (ambiguous), reject.
    select exists (
      select 1
      from tmp_date_stats ds
      where ds.date_local = v_shift_work_date
        and ds.file_rows_count > 0
    )
    into v_has_any_rows_on_date;

    if v_has_any_rows_on_date then
      -- If the file has any deterministically-mapped row for this candidate+client+date => not empty => reject.
      select exists (
        select 1
        from tmp_p2_join pj
        where pj.date_local = v_shift_work_date
          and pj.candidate_id = v_shift_candidate_id
          and pj.client_id = v_shift_client_id
      )
      into v_has_candidate_rows_on_date;

      if v_has_candidate_rows_on_date then
        raise exception
          'nhsp_weekly_apply_cancellations: item % shift % violates day-empty policy: import contains shift rows for candidate+client+date.',
          v_idx, v_shift_id;
      end if;

      -- If any row on that date is not deterministically mapped (candidate/client/work_date missing), cancellation is ambiguous => reject.
      select coalesce(ds.ambiguous_rows_count, 0)
      into v_ambiguous_rows_on_date
      from tmp_date_stats ds
      where ds.date_local = v_shift_work_date
      limit 1;

      if v_ambiguous_rows_on_date > 0 then
        raise exception
          'nhsp_weekly_apply_cancellations: item % shift % violates day-empty policy: import has % ambiguous row(s) on that date; resolve mappings first.',
          v_idx, v_shift_id, v_ambiguous_rows_on_date;
      end if;
    end if;

    -- Always record affected ids for return payload
    if v_timesheet_id is not null then
      v_timesheet_ids := array_append(v_timesheet_ids, v_timesheet_id);
    end if;
    if v_invoice_id is not null then
      v_invoice_ids := array_append(v_invoice_ids, v_invoice_id);
    end if;

    -- Apply cancellation data state: mark cancelled + detach (no deletions)
    update public.nhsp_shifts ns2
    set
      cancelled_at_utc = v_now,
      cancelled_by_import_id = p_import_id,
      cancelled_reason = v_reason,
      timesheet_id = null
    where ns2.id = v_shift_id;

    v_cancelled_count := v_cancelled_count + 1;

    -- TSFIN recompute is mandatory for any cancel/detach/change.
    -- Mark current TSFIN stale and enqueue priority recompute.
    if v_timesheet_id is not null then
      update public.timesheets_financials tf
      set
        is_stale = true,
        stale_reason = 'IMPORT_CANCEL_DETACH',
        updated_at = v_now
      where tf.is_current = true
        and tf.timesheet_id = v_timesheet_id;

      perform public.enqueue_ts_financials_priority(array[v_timesheet_id]::uuid[], 'CONTEXT_CHANGED'::public.ts_fin_reason_enum);
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

  -- No credit notes are created by this RPC under locked policy
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
end;
$$;

create or replace function public.nhsp_weekly_phase3_apply_adjustment_truth(
  p_import_id uuid,
  p_selected_external_row_keys text[],
  p_decisions jsonb,
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

  v_correction_id text;
  v_kind text;

  v_old_start_utc timestamptz;
  v_old_end_utc   timestamptz;
  v_new_start_utc timestamptz;
  v_new_end_utc   timestamptz;
  v_old_break_mins int;
  v_new_break_mins int;

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

  if jsonb_typeof(coalesce(p_decisions,'{}'::jsonb)) <> 'object' then
    raise exception 'nhsp_weekly_phase3_apply_adjustment_truth: p_decisions must be a JSON object.';
  end if;

  -- ---- Load Phase 3 rows as JSONB (schema-safe) ----
  select coalesce(jsonb_agg(to_jsonb(r)), '[]'::jsonb)
  into v_phase3_rows
  from public.weekly_import_changed_hours_phase3(
    p_import_id := p_import_id,
    p_system_type := 'NHSP'
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
      raise exception 'nhsp_weekly_phase3_apply_adjustment_truth: Phase 3 row not found for selected external_row_key=%', v_key;
    end if;

    -- Validate decision object exists
    v_decision := coalesce(p_decisions->v_key, null);

    if v_decision is null or jsonb_typeof(v_decision) <> 'object' then
      raise exception 'nhsp_weekly_phase3_apply_adjustment_truth: Missing/invalid decision object for external_row_key=%', v_key;
    end if;

    v_skip_text := v_decision->>'skip';
    if v_skip_text is null then
      raise exception 'nhsp_weekly_phase3_apply_adjustment_truth: Decision missing skip boolean for external_row_key=%', v_key;
    end if;

    v_skip :=
      case
        when lower(v_skip_text) in ('true','1') then true
        when lower(v_skip_text) in ('false','0') then false
        else null
      end;

    if v_skip is null then
      raise exception 'nhsp_weekly_phase3_apply_adjustment_truth: Decision skip is not boolean-like for external_row_key=% (skip=%)', v_key, v_skip_text;
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
        raise exception 'nhsp_weekly_phase3_apply_adjustment_truth: Missing credit/reinvoice weeks for invoiced external_row_key=%', v_key;
      end if;

      begin
        if extract(isodow from (v_credit_week_start::date)) <> 1 then
          raise exception 'credit_week_start must be Monday';
        end if;
      exception when others then
        raise exception 'nhsp_weekly_phase3_apply_adjustment_truth: Invalid credit_week_start for external_row_key=% (value=%)', v_key, v_credit_week_start;
      end;

      begin
        if extract(isodow from (v_reinvoice_week_start::date)) <> 1 then
          raise exception 'reinvoice_week_start must be Monday';
        end if;
      exception when others then
        raise exception 'nhsp_weekly_phase3_apply_adjustment_truth: Invalid reinvoice_week_start for external_row_key=% (value=%)', v_key, v_reinvoice_week_start;
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
      raise exception 'nhsp_weekly_phase3_apply_adjustment_truth: Phase 3 row missing contract_id/candidate_id/client_id/week_ending_date for external_row_key=%', v_key;
    end;

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

    -- Booking base follows existing scheme (stable + deterministic)
    v_candidate_norm := lower(coalesce(v_candidate_tms_ref, v_candidate_display_name, v_candidate_id::text));
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

    -- Hash booking base (sha256) like makeBookingId; keep first 16 hex
    v_hash_hex := substring(encode(digest(v_booking_base, 'sha256'), 'hex') from 1 for 16);
    v_booking_id := 'bk_' || v_hash_hex;

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

    -- Two correction kinds per proceed: reversal + replacement
    foreach v_kind in array array['CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT'] loop

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

      v_shift_label := 'weekly-correction-' || lower(v_kind) || '-' || v_correction_id;

      v_shift_label_norm :=
        regexp_replace(
          regexp_replace(lower(trim(v_shift_label)), '\s+', ' ', 'g'),
          '[^\w\s\-@&\/,:]',
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
            update public.contract_weeks cw2
            set
              is_adjustment = true,
              status = 'SUBMITTED'::public.contract_week_status_enum,
              updated_at = v_now
            where cw2.id = v_existing_cw_id;
          end if;

          -- Update timesheet schedule + hint only; keep idempotent identity
          update public.timesheets t2
          set
            actual_schedule_json = v_schedule,
            qr_payload_json = v_hint,
            updated_at = v_now
          where t2.timesheet_id = v_existing_ts_id;

          v_upd_count := v_upd_count + 1;
          v_updated_ts_ids := array_append(v_updated_ts_ids, v_existing_ts_id);

          continue;
        end if;

        -- If we have an existing correction timesheet but no linked contract_week, create one.
        v_next_additional_seq := null;

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
          updated_at = v_now
        where t2b.timesheet_id = v_existing_ts_id;

        v_upd_count := v_upd_count + 1;
        v_updated_ts_ids := array_append(v_updated_ts_ids, v_existing_ts_id);

        continue;
      end if;

      -- No existing correction timesheet: create new adjustment contract_week + timesheet
      v_ts_id := null;
      v_cw_id := null;

      -- Allocate additional_seq with retry to avoid collisions
      for v_try in 1..5 loop
        select coalesce(max(cw4.additional_seq), 0) + 1
        into v_next_additional_seq
        from public.contract_weeks cw4
        where cw4.contract_id = v_contract_id
          and cw4.week_ending_date = v_week_ending_date
          and cw4.is_adjustment is true
        for update;

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
            'MANUAL'::public.timesheet_submission_mode_enum,
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
            null,
            coalesce(v_candidate_display_name, v_candidate_tms_ref, v_candidate_id::text),
            v_correction_id,
            v_kind,
            'IMPORT_CORRECTION'
          )
          returning timesheet_id into v_ts_id;

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
          )
          returning id into v_cw_id;

          v_ins_count := v_ins_count + 1;
          v_created_ts_ids := array_append(v_created_ts_ids, v_ts_id);

          exit;
        exception
          when unique_violation then
            -- retry allocation
            v_ts_id := null;
            v_cw_id := null;
        end;

        exit when v_ts_id is not null;
      end loop;

      if v_ts_id is null then
        raise exception 'nhsp_weekly_phase3_apply_adjustment_truth: Failed to allocate correction timesheet/contract_week after retries (external_row_key=% kind=%)', v_key, v_kind;
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
  v_decisions jsonb := coalesce(v_payload->'decisions', '{}'::jsonb);

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

  -- phase3 decisions (selection-aware)
  v_phase3_skip_keys text[] := array[]::text[];
  v_phase3_proceed_keys text[] := array[]::text[];
  v_phase3_result jsonb := null;

  -- phase1 / phase1.5
  v_phase1_result jsonb := null;
  v_phase15_ok int := 0;
  v_phase15_updated int := 0;

  -- policy A replacement-day enforcement
  v_selected_cancel_shift_id_set text[] := array[]::text[];

  -- cancellations
  v_cancel_actions jsonb := '[]'::jsonb;
  v_cancellations_result jsonb := null;

  -- affected timesheets
  v_affected_timesheet_ids uuid[] := array[]::uuid[];

begin
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

  -- ─────────────────────────────────────────────
  -- 1) Parse and normalize selection payload (ROW:/CANCEL:)
  -- ─────────────────────────────────────────────
  if jsonb_typeof(v_actions_json) <> 'array' then
    raise exception 'nhsp_weekly_apply_transactional: selected_action_ids must be a JSON array.';
  end if;

  if jsonb_typeof(v_actions2_json) <> 'array' then
    raise exception 'nhsp_weekly_apply_transactional: selected_actions must be a JSON array.';
  end if;

  if jsonb_typeof(v_decisions) <> 'object' then
    raise exception 'nhsp_weekly_apply_transactional: decisions must be a JSON object.';
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
    raise exception 'nhsp_weekly_apply_transactional: invalid action_id in selection (expected ROW:<external_row_key> or CANCEL:<shift_id>).';
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
    p2.candidate_id,
    p2.client_id,
    p2.contract_id,
    p2.week_ending_date,
    upper(coalesce(p2.action::text,'')) as action
  from tmp_p2_all p2
  where upper(coalesce(p2.action::text,'')) = 'OK'
    and p2.external_row_key is not null
    and p2.candidate_id is not null
    and p2.client_id is not null
    and p2.contract_id is not null
    and p2.week_ending_date is not null
    and p2.work_date is not null;

  select coalesce(array_agg(distinct p2.external_row_key order by p2.external_row_key), array[]::text[])
  into v_all_ok_external_keys
  from tmp_p2_ok p2;

  -- selected truth keys must be present in OK universe
  if exists (
    select 1
    from unnest(v_selected_truth_keys) as k(external_row_key)
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
    from unnest(v_selected_truth_keys) as k(external_row_key)
    join (select distinct p2.external_row_key from tmp_p2_ok p2) as okk
      on okk.external_row_key = k.external_row_key
  ) as k;

  -- ─────────────────────────────────────────────
  -- 3) Phase 3 decision validation (selection-aware) + derive skip/proceed keys
  --     NOTE: weekly_import_changed_hours_phase3 is already filtered to requires_any_decision=true rows.
  -- ─────────────────────────────────────────────
  create temporary table tmp_p3_sel on commit drop as
  select *
  from public.weekly_import_changed_hours_phase3(p_import_id := p_import_id, p_system_type := 'NHSP')
  where external_row_key = any(v_selected_truth_keys_ok);

  -- Reject decision keys that are not present in phase3 rows within the selected truth keys
  create temporary table tmp_decision_keys on commit drop as
  select k.key_text as external_row_key
  from (
    select jsonb_object_keys(v_decisions) as key_text
  ) as k
  where k.key_text is not null and btrim(k.key_text) <> '';

  if exists (
    select 1
    from tmp_decision_keys dk
    left join tmp_p3_sel p3
      on p3.external_row_key = dk.external_row_key
    where p3.external_row_key is null
  ) then
    raise exception 'nhsp_weekly_apply_transactional: decisions include unknown external_row_key(s) for this selection (no phase3 row).';
  end if;

  -- For every phase3 row in selection, require a decision object
  if exists (
    select 1
    from tmp_p3_sel p3req
    where (v_decisions ? p3req.external_row_key) is not true
  ) then
    raise exception 'nhsp_weekly_apply_transactional: missing decision object for one or more selected decision-required rows.';
  end if;

  create temporary table tmp_decision_eval on commit drop as
  select
    p3.external_row_key,
    (p3.is_invoiced is true) as is_invoiced,
    case
      when jsonb_typeof(v_decisions->p3.external_row_key) <> 'object' then null
      else
        case
          when lower(coalesce((v_decisions->p3.external_row_key)->>'skip','')) in ('true','1') then true
          when lower(coalesce((v_decisions->p3.external_row_key)->>'skip','')) in ('false','0') then false
          else null
        end
    end as skip_bool,
    nullif(btrim((v_decisions->p3.external_row_key)->>'credit_week_start'), '') as credit_week_start,
    nullif(btrim((v_decisions->p3.external_row_key)->>'reinvoice_week_start'), '') as reinvoice_week_start
  from tmp_p3_sel p3;

  if exists (
    select 1
    from tmp_decision_eval de
    where de.skip_bool is null
  ) then
    raise exception 'nhsp_weekly_apply_transactional: invalid decision skip boolean for one or more keys.';
  end if;

  if exists (
    select 1
    from tmp_decision_eval de
    where de.is_invoiced is true
      and de.skip_bool is false
      and (de.credit_week_start is null or de.reinvoice_week_start is null)
  ) then
    raise exception 'nhsp_weekly_apply_transactional: missing credit_week_start/reinvoice_week_start for invoiced PROCEED decision.';
  end if;

  if exists (
    select 1
    from tmp_decision_eval de
    where de.is_invoiced is true
      and de.skip_bool is false
      and extract(isodow from de.credit_week_start::date) <> 1
  ) then
    raise exception 'nhsp_weekly_apply_transactional: credit_week_start must be a Monday for invoiced PROCEED decision.';
  end if;

  if exists (
    select 1
    from tmp_decision_eval de
    where de.is_invoiced is true
      and de.skip_bool is false
      and extract(isodow from de.reinvoice_week_start::date) <> 1
  ) then
    raise exception 'nhsp_weekly_apply_transactional: reinvoice_week_start must be a Monday for invoiced PROCEED decision.';
  end if;

  select coalesce(array_agg(de.external_row_key order by de.external_row_key), array[]::text[])
  into v_phase3_skip_keys
  from tmp_decision_eval de
  where de.skip_bool is true;

  select coalesce(array_agg(de.external_row_key order by de.external_row_key), array[]::text[])
  into v_phase3_proceed_keys
  from tmp_decision_eval de
  where de.skip_bool is false;

  -- ─────────────────────────────────────────────
  -- 4) Compute force/skip keys for Phase 1 (tick-only + skip decisions)
  --     - force keys = selected ROW keys minus phase3 skip keys
  --     - skip keys  = all OK keys in import minus force keys
  -- ─────────────────────────────────────────────
  select coalesce(array_agg(k.external_row_key order by k.external_row_key), array[]::text[])
  into v_force_keys_final
  from (
    select distinct st.external_row_key
    from unnest(v_selected_truth_keys_ok) as st(external_row_key)
    left join unnest(v_phase3_skip_keys) as sk(external_row_key)
      on sk.external_row_key = st.external_row_key
    where sk.external_row_key is null
  ) as k;

  select coalesce(array_agg(x.external_row_key order by x.external_row_key), array[]::text[])
  into v_skip_keys_final
  from (
    select distinct okk.external_row_key
    from unnest(coalesce(v_all_ok_external_keys, array[]::text[])) as okk(external_row_key)
    left join unnest(coalesce(v_force_keys_final, array[]::text[])) as fk(external_row_key)
      on fk.external_row_key = okk.external_row_key
    where fk.external_row_key is null
  ) as x;

  -- ─────────────────────────────────────────────
  -- 5) Policy A replacement-day enforcement (NHSP)
  --     If any selected PROCEED truth row moves date, require all cancels for old day.
  -- ─────────────────────────────────────────────
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

    create temporary table tmp_selected_replacement_keys on commit drop as
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
    from unnest(v_selected_cancel_shift_ids) as x;

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

  -- ─────────────────────────────────────────────
  -- 6) Apply Phase 3 adjustment truth (selected PROCEED keys only; NHSP)
  -- ─────────────────────────────────────────────
  if array_length(v_phase3_proceed_keys, 1) is not null then
    select public.nhsp_weekly_phase3_apply_adjustment_truth(
      p_import_id := p_import_id,
      p_selected_external_row_keys := v_phase3_proceed_keys,
      p_decisions := v_decisions,
      p_actor_user_id := p_actor_user_id
    )
    into v_phase3_result;
  end if;

  -- ─────────────────────────────────────────────
  -- 7) Phase 1 upsert (NHSP) with tick-only skip/force
  -- ─────────────────────────────────────────────
  if array_length(v_all_ok_external_keys, 1) is not null then
    select public.nhsp_apply_import_phase1(
      p_import_id := p_import_id,
      p_selected_group_ids := array[]::text[],
      p_skip_external_row_keys := v_skip_keys_final,
      p_force_overwrite_external_row_keys := v_force_keys_final
    )
    into v_phase1_result;
  end if;

  -- ─────────────────────────────────────────────
  -- 8) Phase 1.5 repair (NHSP)
  -- ─────────────────────────────────────────────
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

  -- ─────────────────────────────────────────────
  -- 9) Apply selected cancellations (explicit shift_id only; NHSP)
  -- ─────────────────────────────────────────────
  if array_length(v_selected_cancel_shift_ids, 1) is not null then
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
    from (
      select
        (t.candidate_id::text || '|' || t.client_id::text || '|' || t.old_work_date::text) as replacement_day_key
      from (
        select distinct
          rk2.candidate_id,
          rk2.client_id,
          rk2.old_work_date
        from tmp_selected_replacement_keys rk2
      ) as t
    ) as rk;

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
  end if;

  -- ─────────────────────────────────────────────
  -- 10) Compute affected_timesheet_ids for TSFIN Strategy A drain (Worker drains to completion)
  -- ─────────────────────────────────────────────
  create temporary table tmp_aff_ts on commit drop as
  select distinct t.timesheet_id
  from (
    select (x.value)::uuid as timesheet_id
    from jsonb_array_elements_text(coalesce(v_cancellations_result->'affected_timesheet_ids', '[]'::jsonb)) as x(value)

    union all

    select (x2.value)::uuid as timesheet_id
    from jsonb_array_elements_text(coalesce(v_phase3_result->'created_timesheet_ids', '[]'::jsonb)) as x2(value)

    union all

    select (x3.value)::uuid as timesheet_id
    from jsonb_array_elements_text(coalesce(v_phase3_result->'updated_timesheet_ids', '[]'::jsonb)) as x3(value)

    union all

    select ns.timesheet_id as timesheet_id
    from public.nhsp_shifts ns
    where ns.source_system = 'NHSP'::public.hr_source_enum
      and ns.cancelled_at_utc is null
      and ns.external_row_key = any(coalesce(v_force_keys_final, array[]::text[]))
      and ns.timesheet_id is not null
  ) as t
  where t.timesheet_id is not null;

  select coalesce(array_agg(distinct a.timesheet_id order by a.timesheet_id), array[]::uuid[])
  into v_affected_timesheet_ids
  from tmp_aff_ts a;

  if array_length(v_affected_timesheet_ids, 1) is not null then
    perform public.enqueue_ts_financials_priority(v_affected_timesheet_ids, 'CONTEXT_CHANGED'::public.ts_fin_reason_enum);
  end if;

  -- ─────────────────────────────────────────────
  -- 11) Mark import applied (inside transaction)
  -- ─────────────────────────────────────────────
  update public.hr_imports hi3
  set
    import_scope = 'NHSP',
    applied_at = v_now
  where hi3.id = p_import_id;

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
