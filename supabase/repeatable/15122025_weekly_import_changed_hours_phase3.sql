

-- ---------------------------------------------------------
-- Helper: HH:MM(:SS) -> minutes since midnight
-- ---------------------------------------------------------
create or replace function public._wkimp_hhmm_to_min(p text)
returns int
language sql
immutable
as $$
  select
    case
      when p is null or btrim(p) = '' then 0
      else
        (split_part(p, ':', 1)::int * 60) +
        (split_part(p, ':', 2)::int)
    end;
$$;

-- ---------------------------------------------------------
-- Window helper: returns [s1,e1,s2,e2] in minutes-of-day
-- - full day if ws=0 and we=0 -> [0,1440,-1,-1]
-- - non-wrap: [ws,we,-1,-1]
-- - wrap: [ws,1440,0,we]
-- ---------------------------------------------------------
create or replace function public._wkimp_win_parts(ws int, we int)
returns int[]
language plpgsql
immutable
as $$
declare
  a int := greatest(0, least(1440, coalesce(ws,0)));
  b int := greatest(0, least(1440, coalesce(we,0)));
begin
  if a = 0 and b = 0 then
    return array[0,1440,-1,-1];
  end if;

  if a < b then
    return array[a,b,-1,-1];
  end if;

  return array[a,1440,0,b];
end;
$$;

-- ---------------------------------------------------------
-- Overlap of [a0,b0) with [s0,e0) (minutes)
-- ---------------------------------------------------------
create or replace function public._wkimp_overlap_range(a0 int, b0 int, s0 int, e0 int)
returns int
language plpgsql
immutable
as $$
declare
  x1 int;
  x2 int;
begin
  if a0 is null or b0 is null or s0 is null or e0 is null then
    return 0;
  end if;

  if a0 >= b0 then
    return 0;
  end if;

  if s0 < 0 or e0 < 0 then
    return 0;
  end if;

  x1 := greatest(a0, s0);
  x2 := least(b0, e0);
  if x1 < x2 then
    return x2 - x1;
  end if;

  return 0;
end;
$$;

-- ---------------------------------------------------------
-- Overlap of [a0,b0) with a window ws->we (wrap-aware)
-- ---------------------------------------------------------
create or replace function public._wkimp_overlap_window(a0 int, b0 int, ws int, we int)
returns int
language plpgsql
immutable
as $$
declare
  parts int[];
  s1 int; e1 int; s2 int; e2 int;
begin
  if a0 is null or b0 is null then return 0; end if;
  if a0 >= b0 then return 0; end if;

  parts := public._wkimp_win_parts(ws,we);
  s1 := parts[1]; e1 := parts[2]; s2 := parts[3]; e2 := parts[4];

  return
    public._wkimp_overlap_range(a0,b0,s1,e1) +
    public._wkimp_overlap_range(a0,b0,s2,e2);
end;
$$;

-- ---------------------------------------------------------
-- Overlap of [a0,b0) with (window1 ∩ window2)
-- ---------------------------------------------------------
create or replace function public._wkimp_overlap_intersection2(
  a0 int, b0 int,
  ws1 int, we1 int,
  ws2 int, we2 int
)
returns int
language plpgsql
immutable
as $$
declare
  p1 int[]; p2 int[];
  a1 int; b1 int; a2 int; b2 int;
  i1 int; i2 int;
  s int; e int;
  out int := 0;
begin
  if a0 is null or b0 is null then return 0; end if;
  if a0 >= b0 then return 0; end if;

  p1 := public._wkimp_win_parts(ws1,we1);
  p2 := public._wkimp_win_parts(ws2,we2);

  for i1 in 0..1 loop
    if i1=0 then a1 := p1[1]; b1 := p1[2]; else a1 := p1[3]; b1 := p1[4]; end if;
    if a1 < 0 or b1 < 0 then continue; end if;

    for i2 in 0..1 loop
      if i2=0 then a2 := p2[1]; b2 := p2[2]; else a2 := p2[3]; b2 := p2[4]; end if;
      if a2 < 0 or b2 < 0 then continue; end if;

      s := greatest(a1,a2);
      e := least(b1,b2);
      out := out + public._wkimp_overlap_range(a0,b0,s,e);
    end loop;
  end loop;

  return out;
end;
$$;

-- ---------------------------------------------------------
-- Overlap of [a0,b0) with (window1 ∩ window2 ∩ window3)
-- ---------------------------------------------------------
create or replace function public._wkimp_overlap_intersection3(
  a0 int, b0 int,
  ws1 int, we1 int,
  ws2 int, we2 int,
  ws3 int, we3 int
)
returns int
language plpgsql
immutable
as $$
declare
  p1 int[]; p2 int[]; p3 int[];
  a1 int; b1 int; a2 int; b2 int; a3 int; b3 int;
  i1 int; i2 int; i3 int;
  s int; e int;
  out int := 0;
begin
  if a0 is null or b0 is null then return 0; end if;
  if a0 >= b0 then return 0; end if;

  p1 := public._wkimp_win_parts(ws1,we1);
  p2 := public._wkimp_win_parts(ws2,we2);
  p3 := public._wkimp_win_parts(ws3,we3);

  for i1 in 0..1 loop
    if i1=0 then a1 := p1[1]; b1 := p1[2]; else a1 := p1[3]; b1 := p1[4]; end if;
    if a1 < 0 or b1 < 0 then continue; end if;

    for i2 in 0..1 loop
      if i2=0 then a2 := p2[1]; b2 := p2[2]; else a2 := p2[3]; b2 := p2[4]; end if;
      if a2 < 0 or b2 < 0 then continue; end if;

      for i3 in 0..1 loop
        if i3=0 then a3 := p3[1]; b3 := p3[2]; else a3 := p3[3]; b3 := p3[4]; end if;
        if a3 < 0 or b3 < 0 then continue; end if;

        s := greatest(greatest(a1,a2),a3);
        e := least(least(b1,b2),b3);
        out := out + public._wkimp_overlap_range(a0,b0,s,e);
      end loop;
    end loop;
  end loop;

  return out;
end;
$$;

-- ---------------------------------------------------------
-- Helper: allocate bucket HOURS for one shift using policy_snapshot_json
-- Mirrors backend approach:
-- - timezone-aware day splitting
-- - break mins removed from the middle (subtractBreak)
-- - precedence BH > Sun > Sat > Night > Day
-- - "0/0" windows treated as FULL DAY
-- ---------------------------------------------------------


create or replace function public._wkimp_bucket_hours_from_policy(
  p_policy jsonb,
  p_start_utc timestamptz,
  p_end_utc   timestamptz,
  p_break_mins int
)
returns table (
  hours_day   numeric,
  hours_night numeric,
  hours_sat   numeric,
  hours_sun   numeric,
  hours_bh    numeric,
  total_hours numeric
)
language plpgsql
stable
as $$
declare
  tz text := coalesce(nullif(p_policy->>'timezone_id',''), 'Europe/London');

  day_start int := public._wkimp_hhmm_to_min(p_policy->>'day_start');
  day_end   int := public._wkimp_hhmm_to_min(p_policy->>'day_end');
  sat_start int := public._wkimp_hhmm_to_min(p_policy->>'sat_start');
  sat_end   int := public._wkimp_hhmm_to_min(p_policy->>'sat_end');
  sun_start int := public._wkimp_hhmm_to_min(p_policy->>'sun_start');
  sun_end   int := public._wkimp_hhmm_to_min(p_policy->>'sun_end');
  bh_start  int := public._wkimp_hhmm_to_min(p_policy->>'bh_start');
  bh_end    int := public._wkimp_hhmm_to_min(p_policy->>'bh_end');

  bh_list text[] := (
    select coalesce(array_agg(x), '{}'::text[])
    from jsonb_array_elements_text(coalesce(p_policy->'bh_list','[]'::jsonb)) as t(x)
  );

  m_day   int := 0;
  m_night int := 0;
  m_sat   int := 0;
  m_sun   int := 0;
  m_bh    int := 0;

  total_mins int;
  br int := greatest(coalesce(p_break_mins,0), 0);
  start_cut int;

  seg1_start timestamptz;
  seg1_end   timestamptz;
  seg2_start timestamptz;
  seg2_end   timestamptz;
  has_seg2   boolean := false;

  cur_start timestamptz;
  cur_end   timestamptz;

  slice_start timestamptz;
  slice_end   timestamptz;

  local_date date;
  next_midnight_utc timestamptz;

  a0 int;
  b0 int;
  slice_len int;

  dow int;
  is_bh boolean;

  bh_raw int;
  day_raw int;
  sun_raw int;
  sat_raw int;

  day_eff int;
  sun_eff int;
  sat_eff int;
  night_eff int;

  minute_cursor timestamptz;
  local_ts timestamp;
  local_minute int;
begin
  hours_day := 0; hours_night := 0; hours_sat := 0; hours_sun := 0; hours_bh := 0; total_hours := 0;

  if p_start_utc is null or p_end_utc is null then return; end if;
  if p_end_utc <= p_start_utc then return; end if;

  total_mins := floor(extract(epoch from (p_end_utc - p_start_utc)) / 60);
  if total_mins <= 0 then return; end if;
  if br >= total_mins then return; end if;

  -- subtractBreak: remove break mins from middle
  if br > 0 then
    start_cut := floor((total_mins - br) / 2.0);
    seg1_start := p_start_utc;
    seg1_end   := p_start_utc + make_interval(mins => start_cut);
    seg2_start := seg1_end + make_interval(mins => br);
    seg2_end   := p_end_utc;
    has_seg2 := true;
  else
    seg1_start := p_start_utc;
    seg1_end   := p_end_utc;
    has_seg2 := false;
  end if;

  for cur_start, cur_end in
    select seg1_start, seg1_end
    union all
    select seg2_start, seg2_end
    where has_seg2
  loop
    if cur_start is null or cur_end is null then continue; end if;
    if cur_end <= cur_start then continue; end if;

    slice_start := cur_start;

    while slice_start < cur_end loop
      local_date := (slice_start at time zone tz)::date;
      next_midnight_utc := ((local_date + 1)::timestamp at time zone tz);
      if next_midnight_utc is null then
        next_midnight_utc := date_trunc('day', slice_start) + interval '1 day';
      end if;
      slice_end := least(cur_end, next_midnight_utc);
      if slice_end <= slice_start then
        slice_start := next_midnight_utc;
        continue;
      end if;

      dow := extract(dow from local_date)::int; -- 0=Sun..6=Sat
      is_bh := (local_date::text = any(bh_list));

      minute_cursor := slice_start;

      while (minute_cursor + interval '1 minute') <= slice_end loop
        local_ts := (minute_cursor at time zone tz);
        local_minute := floor(extract(epoch from (local_ts - (local_date::timestamp))) / 60)::int;
        local_minute := greatest(0, least(1439, local_minute));

        if is_bh and (
          (bh_start = bh_end) or
          (bh_start < bh_end and local_minute >= bh_start and local_minute < bh_end) or
          (bh_start > bh_end and (local_minute >= bh_start or local_minute < bh_end))
        ) then
          m_bh := m_bh + 1;

        elsif dow = 0 and (
          (sun_start = sun_end) or
          (sun_start < sun_end and local_minute >= sun_start and local_minute < sun_end) or
          (sun_start > sun_end and (local_minute >= sun_start or local_minute < sun_end))
        ) then
          m_sun := m_sun + 1;

        elsif dow = 6 and (
          (sat_start = sat_end) or
          (sat_start < sat_end and local_minute >= sat_start and local_minute < sat_end) or
          (sat_start > sat_end and (local_minute >= sat_start or local_minute < sat_end))
        ) then
          m_sat := m_sat + 1;

        elsif
          (day_start = day_end) or
          (day_start < day_end and local_minute >= day_start and local_minute < day_end) or
          (day_start > day_end and (local_minute >= day_start or local_minute < day_end))
        then
          m_day := m_day + 1;

        else
          m_night := m_night + 1;
        end if;

        minute_cursor := minute_cursor + interval '1 minute';
      end loop;

      slice_start := slice_end;
    end loop;
  end loop;

  hours_day   := round((m_day::numeric   / 60.0), 2);
  hours_night := round((m_night::numeric / 60.0), 2);
  hours_sat   := round((m_sat::numeric   / 60.0), 2);
  hours_sun   := round((m_sun::numeric   / 60.0), 2);
  hours_bh    := round((m_bh::numeric    / 60.0), 2);
  total_hours := round((hours_day + hours_night + hours_sat + hours_sun + hours_bh), 2);

  return;
end;
$$;

CREATE OR REPLACE FUNCTION public._pay_finance_protected_recovery_allocate(
  p_recovery_rows jsonb,
  p_run_earnings_headroom numeric,
  p_run_take_home_headroom numeric DEFAULT NULL::numeric,
  p_default_take_home_floor numeric DEFAULT NULL::numeric
)
RETURNS TABLE(
  sort_order integer,
  finance_case_id uuid,
  case_type public.pay_finance_case_type_enum,
  payout_status public.pay_advance_payout_status_enum,
  nominal_due_amount numeric,
  minimum_earnings_threshold numeric,
  effective_take_home_floor numeric,
  headroom_before numeric,
  take_home_before numeric,
  threshold_cap_amount numeric,
  take_home_cap_amount numeric,
  protected_recoverable_amount numeric,
  headroom_after numeric,
  take_home_after numeric
)
LANGUAGE plpgsql
IMMUTABLE
SET search_path TO 'public'
AS $function$
DECLARE
  v_row record;
  v_remaining_headroom numeric(12,2) := round(greatest(coalesce(p_run_earnings_headroom, 0), 0), 2)::numeric(12,2);
  v_remaining_take_home numeric(12,2) := CASE
    WHEN p_run_take_home_headroom IS NULL THEN NULL
    ELSE round(greatest(p_run_take_home_headroom, 0), 2)::numeric(12,2)
  END;
  v_default_take_home_floor numeric(12,2) := CASE
    WHEN p_default_take_home_floor IS NULL THEN NULL
    ELSE round(greatest(p_default_take_home_floor, 0), 2)::numeric(12,2)
  END;
  v_case_type_text text := NULL;
  v_payout_status_text text := NULL;
  v_nominal_due_amount numeric(12,2) := 0;
  v_minimum_earnings_threshold numeric(12,2) := NULL;
  v_effective_take_home_floor_local numeric(12,2) := NULL;
  v_headroom_before_local numeric(12,2) := 0;
  v_take_home_before_local numeric(12,2) := NULL;
  v_threshold_cap_amount_local numeric(12,2) := 0;
  v_take_home_cap_amount_local numeric(12,2) := NULL;
  v_effective_cap_amount_local numeric(12,2) := 0;
  v_protected_recoverable_amount_local numeric(12,2) := 0;
BEGIN
  IF p_recovery_rows IS NULL OR jsonb_typeof(p_recovery_rows) <> 'array' THEN
    RAISE EXCEPTION '%', jsonb_build_object(
      'error', 'PAY_FINANCE_PROTECTED_RECOVERY_ALLOCATE',
      'code', 'RECOVERY_ROWS_ARRAY_REQUIRED',
      'message', '_pay_finance_protected_recovery_allocate: p_recovery_rows must be a JSON array'
    )::text;
  END IF;

  FOR v_row IN
    WITH parsed_rows AS (
      SELECT
        CASE
          WHEN nullif(btrim(elem.value->>'sort_order'), '') IS NULL THEN elem.ordinality::integer
          ELSE (elem.value->>'sort_order')::integer
        END AS sort_order,
        elem.ordinality::integer AS input_ordinality,
        CASE
          WHEN nullif(btrim(elem.value->>'finance_case_id'), '') IS NULL THEN NULL::uuid
          ELSE (elem.value->>'finance_case_id')::uuid
        END AS finance_case_id,
        nullif(btrim(elem.value->>'case_type'), '') AS case_type_text,
        nullif(btrim(elem.value->>'payout_status'), '') AS payout_status_text,
        round(
          greatest(
            coalesce(
              CASE
                WHEN nullif(btrim(elem.value->>'nominal_due_amount'), '') IS NULL THEN 0::numeric
                ELSE (elem.value->>'nominal_due_amount')::numeric
              END,
              0::numeric
            ),
            0::numeric
          ),
          2
        )::numeric(12,2) AS nominal_due_amount,
        CASE
          WHEN nullif(btrim(elem.value->>'minimum_earnings_threshold'), '') IS NULL THEN NULL::numeric(12,2)
          ELSE round(greatest((elem.value->>'minimum_earnings_threshold')::numeric, 0::numeric), 2)::numeric(12,2)
        END AS minimum_earnings_threshold,
        CASE
          WHEN nullif(btrim(elem.value->>'take_home_floor_override'), '') IS NULL THEN NULL::numeric(12,2)
          ELSE round(greatest((elem.value->>'take_home_floor_override')::numeric, 0::numeric), 2)::numeric(12,2)
        END AS take_home_floor_override
      FROM jsonb_array_elements(p_recovery_rows) WITH ORDINALITY AS elem(value, ordinality)
    )
    SELECT
      pr.sort_order,
      pr.input_ordinality,
      pr.finance_case_id,
      pr.case_type_text,
      pr.payout_status_text,
      pr.nominal_due_amount,
      pr.minimum_earnings_threshold,
      pr.take_home_floor_override
    FROM parsed_rows AS pr
    ORDER BY pr.sort_order, pr.input_ordinality, pr.finance_case_id
  LOOP
    IF v_row.finance_case_id IS NULL THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_FINANCE_PROTECTED_RECOVERY_ALLOCATE',
        'code', 'FINANCE_CASE_ID_REQUIRED',
        'message', '_pay_finance_protected_recovery_allocate: each recovery row must include finance_case_id',
        'sort_order', v_row.sort_order,
        'input_ordinality', v_row.input_ordinality
      )::text;
    END IF;

    v_case_type_text := upper(coalesce(v_row.case_type_text, ''));

    IF v_case_type_text NOT IN ('PAYMENT_ADVANCE', 'MANUAL_DEBT_ADJUSTMENT') THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_FINANCE_PROTECTED_RECOVERY_ALLOCATE',
        'code', 'UNSUPPORTED_CASE_TYPE',
        'message', '_pay_finance_protected_recovery_allocate: only PAYMENT_ADVANCE repayments and MANUAL_DEBT_ADJUSTMENT recoveries are supported',
        'finance_case_id', v_row.finance_case_id::text,
        'case_type', v_row.case_type_text
      )::text;
    END IF;

    v_payout_status_text := upper(coalesce(v_row.payout_status_text, ''));
    IF v_case_type_text = 'PAYMENT_ADVANCE' AND v_payout_status_text <> 'PAID' THEN
      RAISE EXCEPTION '%', jsonb_build_object(
        'error', 'PAY_FINANCE_PROTECTED_RECOVERY_ALLOCATE',
        'code', 'PAYMENT_ADVANCE_NOT_REPAYABLE',
        'message', '_pay_finance_protected_recovery_allocate: PAYMENT_ADVANCE rows must represent paid advances when allocating recoveries',
        'finance_case_id', v_row.finance_case_id::text,
        'payout_status', v_row.payout_status_text
      )::text;
    END IF;

    v_nominal_due_amount := round(greatest(coalesce(v_row.nominal_due_amount, 0), 0), 2)::numeric(12,2);
    v_minimum_earnings_threshold := v_row.minimum_earnings_threshold;
    v_effective_take_home_floor_local := coalesce(v_row.take_home_floor_override, v_default_take_home_floor);

    v_headroom_before_local := v_remaining_headroom;
    v_take_home_before_local := v_remaining_take_home;

    v_threshold_cap_amount_local := round(
      CASE
        WHEN v_minimum_earnings_threshold IS NULL THEN v_headroom_before_local
        ELSE greatest(v_headroom_before_local - v_minimum_earnings_threshold, 0)
      END,
      2
    )::numeric(12,2);

    v_take_home_cap_amount_local := CASE
      WHEN v_take_home_before_local IS NULL OR v_effective_take_home_floor_local IS NULL THEN NULL
      ELSE round(greatest(v_take_home_before_local - v_effective_take_home_floor_local, 0), 2)::numeric(12,2)
    END;

    v_effective_cap_amount_local := round(
      least(
        v_headroom_before_local,
        v_threshold_cap_amount_local,
        coalesce(v_take_home_cap_amount_local, v_headroom_before_local)
      ),
      2
    )::numeric(12,2);

    v_protected_recoverable_amount_local := round(
      least(v_nominal_due_amount, greatest(v_effective_cap_amount_local, 0)),
      2
    )::numeric(12,2);

    v_remaining_headroom := round(
      greatest(v_remaining_headroom - v_protected_recoverable_amount_local, 0),
      2
    )::numeric(12,2);

    IF v_remaining_take_home IS NOT NULL THEN
      v_remaining_take_home := round(
        greatest(v_remaining_take_home - v_protected_recoverable_amount_local, 0),
        2
      )::numeric(12,2);
    END IF;

    sort_order := v_row.sort_order;
    finance_case_id := v_row.finance_case_id;
    case_type := v_case_type_text::public.pay_finance_case_type_enum;
    payout_status := CASE
      WHEN v_case_type_text = 'PAYMENT_ADVANCE' THEN 'PAID'::public.pay_advance_payout_status_enum
      ELSE NULL::public.pay_advance_payout_status_enum
    END;
    nominal_due_amount := v_nominal_due_amount;
    minimum_earnings_threshold := v_minimum_earnings_threshold;
    effective_take_home_floor := v_effective_take_home_floor_local;
    headroom_before := v_headroom_before_local;
    take_home_before := v_take_home_before_local;
    threshold_cap_amount := v_threshold_cap_amount_local;
    take_home_cap_amount := v_take_home_cap_amount_local;
    protected_recoverable_amount := v_protected_recoverable_amount_local;
    headroom_after := v_remaining_headroom;
    take_home_after := v_remaining_take_home;

    RETURN NEXT;
  END LOOP;

  RETURN;
END;
$function$;




