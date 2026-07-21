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

-- Backward-compatible _imp_debug_audit overloads (10-arg + 11-arg)
-- Purpose: allow legacy callers that pass extra trailing args, without ambiguity.
-- Safe to rerun: drops only the 10/11-arg overloads (if present), then recreates them.

-- Drop any existing 10/11-arg overloads (including earlier DEFAULT-based versions)
DROP FUNCTION IF EXISTS public._imp_debug_audit(
  uuid, text, jsonb, text, text, jsonb, text, text, text, text
);

DROP FUNCTION IF EXISTS public._imp_debug_audit(
  uuid, text, jsonb, text, text, jsonb, text, text, text, text, text
);

-- 10-arg overload (NO DEFAULTS to avoid ambiguity)
CREATE OR REPLACE FUNCTION public._imp_debug_audit(
  p_actor_user_id uuid,
  p_action text,
  p_after_json jsonb,
  p_entity text,
  p_subject_id text,
  p_before_json jsonb,
  p_ip text,
  p_user_agent text,
  p_correlation_id text,
  p_unused_1 text
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
begin
  -- Forward to the canonical 9-arg implementation (which applies invoice_debug gating)
  perform public._imp_debug_audit(
    p_actor_user_id,
    p_action,
    p_after_json,
    p_entity,
    p_subject_id,
    p_before_json,
    p_ip,
    p_user_agent,
    p_correlation_id
  );
end;
$function$;

-- 11-arg overload (NO DEFAULTS to avoid ambiguity)
CREATE OR REPLACE FUNCTION public._imp_debug_audit(
  p_actor_user_id uuid,
  p_action text,
  p_after_json jsonb,
  p_entity text,
  p_subject_id text,
  p_before_json jsonb,
  p_ip text,
  p_user_agent text,
  p_correlation_id text,
  p_unused_1 text,
  p_unused_2 text
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
begin
  -- Forward to the canonical 9-arg implementation (which applies invoice_debug gating)
  perform public._imp_debug_audit(
    p_actor_user_id,
    p_action,
    p_after_json,
    p_entity,
    p_subject_id,
    p_before_json,
    p_ip,
    p_user_agent,
    p_correlation_id
  );
end;
$function$;



-- ============================================================
-- CloudTMS: TSFIN wake-up on validation changes (idempotent)
-- Purpose:
--   Any INSERT/UPDATE to public.timesheet_validations should enqueue a
--   priority TSFIN recompute (CONTEXT_CHANGED) for that timesheet_id,
--   but ONLY when the timesheet is not invoice-locked/issued/paid.
--
-- Safe to re-run in migrations:
--   - CREATE OR REPLACE FUNCTION is idempotent
--   - DROP TRIGGER IF EXISTS is idempotent
--   - CREATE TRIGGER recreates deterministically
-- ============================================================

create or replace function public.trg_tsfin_timesheet_validations_wakeup()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  v_ts_id uuid;
  v_skip boolean := false;
begin
  v_ts_id := coalesce(new.timesheet_id, old.timesheet_id);

  if v_ts_id is null then
    return coalesce(new, old);
  end if;

  -- ------------------------------------------------------------
  -- Skip if timesheet is invoice-locked (issued/paid etc), or revoked, or missing
  -- We gate on timesheets_financials.is_current + locked_by_invoice_id
  -- and also ensure the timesheet exists and is current.
  -- ------------------------------------------------------------
  select
    (
      tf.timesheet_id is null
      or tf.is_current is not true
      or tf.locked_by_invoice_id is not null
      or ts.timesheet_id is null
      or ts.is_current is not true
      or ts.revoked_at is not null
    ) as should_skip
  into v_skip
  from public.timesheets ts
  left join public.timesheets_financials tf
    on tf.timesheet_id = ts.timesheet_id
   and tf.is_current = true
  where ts.timesheet_id = v_ts_id
  limit 1;

  if coalesce(v_skip, true) then
    return coalesce(new, old);
  end if;

  -- Priority TSFIN enqueue (idempotent on (timesheet_id, reason))
  perform public.enqueue_ts_financials_priority(
    array[v_ts_id],
    'CONTEXT_CHANGED'::public.ts_fin_reason_enum
  );

  return coalesce(new, old);
end;
$$;

drop trigger if exists trg_tsfin_timesheet_validations_wakeup
  on public.timesheet_validations;

create trigger trg_tsfin_timesheet_validations_wakeup
after insert or update
on public.timesheet_validations
for each row
execute function public.trg_tsfin_timesheet_validations_wakeup();
