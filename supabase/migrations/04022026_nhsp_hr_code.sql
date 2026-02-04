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

  v_next_additional_seq int;
  v_cw_id uuid;

  v_existing_ts_id uuid;
  v_existing_booking_id text;

  v_ts_id uuid;

  v_ins_count int := 0;
  v_upd_count int := 0;
  v_skipped_count int := 0;

  v_created_ts_ids uuid[] := '{}';
  v_updated_ts_ids uuid[] := '{}';

  v_err text;

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
      raise exception 'Phase 3 row not found for selected external_row_key=%', v_key;
    end if;

    -- Validate decision object exists
    v_decision := coalesce(p_decisions->v_key, null);

    if v_decision is null or jsonb_typeof(v_decision) <> 'object' then
      raise exception 'Missing/invalid decision object for external_row_key=%', v_key;
    end if;

    v_skip_text := v_decision->>'skip';
    if v_skip_text is null then
      raise exception 'Decision missing skip boolean for external_row_key=%', v_key;
    end if;

    v_skip :=
      case
        when lower(v_skip_text) in ('true','1') then true
        when lower(v_skip_text) in ('false','0') then false
        else null
      end;

    if v_skip is null then
      raise exception 'Decision skip is not boolean-like for external_row_key=% (skip=%)', v_key, v_skip_text;
    end if;

    if v_skip then
      v_skipped_count := v_skipped_count + 1;
      continue;
    end if;

    -- Determine invoiced flag (be defensive)
    v_is_invoiced :=
      case
        when lower(coalesce(v_row->>'is_invoiced','')) in ('true','1') then true
        when lower(coalesce(v_row->>'locked_by_invoice_id','')) in ('true','1') then true
        when (v_row ? 'locked_by_invoice_id') and nullif(v_row->>'locked_by_invoice_id','') is not null then true
        else false
      end;

    if v_is_invoiced then
      v_credit_week_start := nullif(btrim(v_decision->>'credit_week_start'), '');
      v_reinvoice_week_start := nullif(btrim(v_decision->>'reinvoice_week_start'), '');

      if v_credit_week_start is null or v_reinvoice_week_start is null then
        raise exception 'Missing credit/reinvoice weeks for invoiced external_row_key=%', v_key;
      end if;

      -- Validate date format + Monday (ISO dow = 1)
      begin
        if extract(isodow from (v_credit_week_start::date)) <> 1 then
          raise exception 'credit_week_start must be Monday for external_row_key=% (value=%)', v_key, v_credit_week_start;
        end if;
      exception when others then
        raise exception 'Invalid credit_week_start for external_row_key=% (value=%)', v_key, v_credit_week_start;
      end;

      begin
        if extract(isodow from (v_reinvoice_week_start::date)) <> 1 then
          raise exception 'reinvoice_week_start must be Monday for external_row_key=% (value=%)', v_key, v_reinvoice_week_start;
        end if;
      exception when others then
        raise exception 'Invalid reinvoice_week_start for external_row_key=% (value=%)', v_key, v_reinvoice_week_start;
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
      raise exception 'Phase 3 row missing contract_id/candidate_id/client_id/week_ending_date for external_row_key=%', v_key;
    end;

    -- Extract old/new shift times and break mins
    begin
      v_old_start_utc := nullif(v_row->>'old_start_utc','')::timestamptz;
      v_old_end_utc   := nullif(v_row->>'old_end_utc','')::timestamptz;
      v_new_start_utc := nullif(v_row->>'new_start_utc','')::timestamptz;
      v_new_end_utc   := nullif(v_row->>'new_end_utc','')::timestamptz;
    exception when others then
      raise exception 'Phase 3 row has invalid timestamp fields for external_row_key=%', v_key;
    end;

    if v_old_start_utc is null or v_old_end_utc is null or v_new_start_utc is null or v_new_end_utc is null then
      raise exception 'Phase 3 row missing old/new start/end timestamps for external_row_key=%', v_key;
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

    v_fnv_h := 2166136261; -- 0x811c9dc5
    for v_fnv_i in 1..char_length(v_fnv_s) loop
      v_fnv_h := (v_fnv_h # ascii(substring(v_fnv_s from v_fnv_i for 1)));
      v_fnv_h := (v_fnv_h * 16777619) % 4294967296; -- 0x01000193, keep uint32
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

    if v_contract_display_site is null then
      v_contract_display_site := v_client_id::text;
    end if;
    if v_contract_ward_hint is null then
      v_contract_ward_hint := 'contract';
    end if;
    if v_contract_role is null then
      v_contract_role := 'weekly';
    end if;

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

    -- Build norms exactly like Worker "norm()" (trim->lower->collapse ws->strip chars)
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

      -- Shift date = start time date in Europe/London (cross-midnight belongs to start date)
      v_shift_date_ymd := to_char((v_seg_start_utc at time zone 'Europe/London')::date, 'YYYY-MM-DD');

      -- Store credit/reinvoice weeks as metadata only (NOT used to generate invoices)
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

      -- Check if correction timesheet already exists (idempotent rerun safety)
      select t.timesheet_id, t.booking_id
      into v_existing_ts_id, v_existing_booking_id
      from public.timesheets t
      where t.correction_id = v_correction_id
        and t.correction_kind = v_kind
        and t.is_current = true
      limit 1;

      v_schedule := jsonb_build_array(
        jsonb_build_object(
          'date', v_shift_date_ymd,
          'ward', nullif(btrim(v_contract_ward_hint), ''),
          'start_utc', v_seg_start_utc::text,
          'end_utc', v_seg_end_utc::text,
          'break_mins', v_seg_break_mins
        )
      );

      if v_existing_ts_id is not null then
        update public.timesheets t
          set actual_schedule_json = v_schedule,
              candidate_id = v_candidate_id,
              client_id = v_client_id,
              contract_id = v_contract_id,
              week_ending_date = v_week_ending_date,
              is_adjustment = true,
              adjustment_origin = 'IMPORT_CORRECTION',
              candidate_hint_text = v_hint,
              updated_at = v_now
        where t.timesheet_id = v_existing_ts_id;

        v_upd_count := v_upd_count + 1;
        v_updated_ts_ids := array_append(v_updated_ts_ids, v_existing_ts_id);

      else
        -- Create contract_week adjustment row
        select coalesce(max(cw.additional_seq), 0) + 1
        into v_next_additional_seq
        from public.contract_weeks cw
        where cw.contract_id = v_contract_id
          and cw.week_ending_date = v_week_ending_date;

        insert into public.contract_weeks(
          contract_id,
          week_ending_date,
          additional_seq,
          status,
          submission_mode_snapshot,
          timesheet_id,
          uploaded_pdf_r2_key,
          day_entries_json,
          totals_json,
          created_at,
          updated_at,
          planned_schedule_json,
          is_adjustment
        )
        values (
          v_contract_id,
          v_week_ending_date,
          v_next_additional_seq,
          'SUBMITTED'::public.contract_week_status_enum,
          'MANUAL'::public.submission_mode_enum,
          null,
          null,
          null,
          null,
          v_now,
          v_now,
          null,
          true
        )
        returning id into v_cw_id;

        -- booking_id must match makeWeeklyBookingId + correction_id influence (no suffix scheme)
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

        -- Insert timesheet (weekly manual adjustment)
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
          candidate_id,
          client_id,
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
          lower(coalesce(v_contract_ward_hint, 'contract')),
          lower(coalesce(v_contract_role, 'weekly')),
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
          v_candidate_id,
          v_client_id,
          v_hint,
          v_correction_id,
          v_kind,
          'IMPORT_CORRECTION'
        )
        returning timesheet_id into v_ts_id;

        -- Link contract_week -> timesheet (mirror worker behaviour)
        update public.contract_weeks cw
          set timesheet_id = v_ts_id,
              status = 'SUBMITTED'::public.contract_week_status_enum,
              updated_at = v_now
        where cw.id = v_cw_id;

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



