-- 15122025_weekly_import_changed_hours_phase3.sql
--
-- FULL REPLACEMENT (compiles):
-- - Removes illegal nested helper functions inside plpgsql.
-- - Promotes helper functions to top-level public._wkimp_* helpers.
-- - Keeps the same RPC signature:
--     public.weekly_import_changed_hours_phase3(p_import_id uuid, p_system_type text)
-- - NHSP-only invoice_lines fallback:
--     meta_json->>'nhsp_shift_id' is only consulted when source_system='NHSP'.
--     HealthRoster relies on TSFIN segment charge/pay amounts (segment_id = 'nhsp:'||shift_id).

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

      a0 := floor(extract(epoch from ((slice_start at time zone tz) - (local_date::timestamp))) / 60)::int;
      b0 := floor(extract(epoch from ((slice_end   at time zone tz) - (local_date::timestamp))) / 60)::int;

      a0 := greatest(0, least(1440, a0));
      b0 := greatest(0, least(1440, b0));

      slice_len := greatest(0, floor(extract(epoch from (slice_end - slice_start)) / 60)::int);
      if slice_len <= 0 or a0 >= b0 then
        slice_start := slice_end;
        continue;
      end if;

      dow := extract(dow from local_date)::int; -- 0=Sun..6=Sat
      is_bh := (local_date::text = any(bh_list));

      -- BH (highest priority)
      bh_raw := case when is_bh then public._wkimp_overlap_window(a0,b0,bh_start,bh_end) else 0 end;

      -- Sun / Sat (excluding BH overlap)
      if dow = 0 then
        sun_raw := public._wkimp_overlap_window(a0,b0,sun_start,sun_end);
        sun_eff := greatest(0, sun_raw - case when is_bh then public._wkimp_overlap_intersection2(a0,b0,sun_start,sun_end,bh_start,bh_end) else 0 end);
      else
        sun_eff := 0;
      end if;

      if dow = 6 then
        sat_raw := public._wkimp_overlap_window(a0,b0,sat_start,sat_end);
        sat_eff := greatest(0, sat_raw - case when is_bh then public._wkimp_overlap_intersection2(a0,b0,sat_start,sat_end,bh_start,bh_end) else 0 end);
      else
        sat_eff := 0;
      end if;

      -- Day (exclude BH + weekend windows with inclusion-exclusion)
      day_raw := public._wkimp_overlap_window(a0,b0,day_start,day_end);
      day_eff := day_raw;

      if is_bh then
        day_eff := day_eff - public._wkimp_overlap_intersection2(a0,b0,day_start,day_end,bh_start,bh_end);
      end if;

      if dow = 0 then
        day_eff := day_eff - public._wkimp_overlap_intersection2(a0,b0,day_start,day_end,sun_start,sun_end);
        if is_bh then
          day_eff := day_eff + public._wkimp_overlap_intersection3(a0,b0,day_start,day_end,bh_start,bh_end,sun_start,sun_end);
        end if;
      elsif dow = 6 then
        day_eff := day_eff - public._wkimp_overlap_intersection2(a0,b0,day_start,day_end,sat_start,sat_end);
        if is_bh then
          day_eff := day_eff + public._wkimp_overlap_intersection3(a0,b0,day_start,day_end,bh_start,bh_end,sat_start,sat_end);
        end if;
      end if;

      if day_eff < 0 then day_eff := 0; end if;

      -- Night remainder
      night_eff := slice_len - bh_raw - sun_eff - sat_eff - day_eff;
      if night_eff < 0 then night_eff := 0; end if;

      m_bh    := m_bh    + bh_raw;
      m_sun   := m_sun   + sun_eff;
      m_sat   := m_sat   + sat_eff;
      m_day   := m_day   + day_eff;
      m_night := m_night + night_eff;

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

-- ---------------------------------------------------------
-- PHASE 3 RPC: preview "changed hours" rows (read-only)
-- ---------------------------------------------------------
create or replace function public.weekly_import_changed_hours_phase3(
  p_import_id uuid,
  p_system_type text
)
returns table (
  hr_row_id uuid,
  external_row_key text,

  shift_id uuid,
  source_system text,

  candidate_id uuid,
  client_id uuid,
  contract_id uuid,
  timesheet_id uuid,

  work_date date,
  week_ending_date date,

  old_start_utc timestamptz,
  old_end_utc timestamptz,
  old_break_mins int,

  new_start_utc timestamptz,
  new_end_utc timestamptz,
  new_break_mins int,

  old_paid_minutes int,
  new_paid_minutes int,

  is_changed_hours boolean,

  is_paid boolean,
  is_invoiced boolean,

  old_pay_ex numeric,
  old_charge_ex numeric,

  new_pay_ex numeric,
  new_charge_ex numeric,

  delta_pay_ex numeric,
  delta_charge_ex numeric,

  requires_pay_decision boolean,
  requires_invoice_decision boolean,
  requires_any_decision boolean
)
language sql
stable
as $$
with wanted as (
  select
    case
      when upper(coalesce($2,'')) = 'NHSP' then 'NHSP'::hr_source_enum
      when upper(coalesce($2,'')) = 'HEALTHROSTER' then 'HEALTHROSTER'::hr_source_enum
      else null::hr_source_enum
    end as sys
),
rows_in as (
  select
    r.id as hr_row_id,
    r.external_row_key,
    r.date_local as work_date,
    (r.payload_json->>'start_utc')::timestamptz as new_start_utc,
    (r.payload_json->>'end_utc')::timestamptz   as new_end_utc,
    coalesce((r.payload_json->>'break_mins')::int, 0) as new_break_mins
  from public.hr_rows r
  where r.import_id = $1
    and r.external_row_key is not null
),
matched as (
  select
    ri.*,
    s.id as shift_id,
    s.source_system::text as source_system,
    s.candidate_id,
    s.client_id,
    s.contract_id,
    s.timesheet_id,
    s.week_ending_date,
    s.work_date as old_work_date,

    s.start_utc as old_start_utc,
    s.end_utc   as old_end_utc,
    coalesce(s.break_mins,0) as old_break_mins,
    coalesce(s.pay_minutes,0) as old_paid_minutes
  from rows_in ri
  join wanted w on true
  left join public.nhsp_shifts s
    on s.external_row_key = ri.external_row_key
   and (w.sys is null or s.source_system = w.sys)
),
fin as (
  select
    m.*,
    tf.id as tsfin_id,
    tf.paid_at_utc,
    tf.locked_by_invoice_id,
    tf.invoice_breakdown_json,
    tf.policy_snapshot_json,
    tf.pay_day, tf.pay_night, tf.pay_sat, tf.pay_sun, tf.pay_bh,
    tf.charge_day, tf.charge_night, tf.charge_sat, tf.charge_sun, tf.charge_bh
  from matched m
  left join public.timesheets_financials tf
    on tf.timesheet_id = m.timesheet_id
   and tf.is_current = true
),
seg_old as (
  select
    f.*,
    (seg->>'pay_amount')::numeric     as seg_old_pay_ex,
    (seg->>'charge_amount')::numeric  as seg_old_charge_ex,
    nullif(seg->>'invoice_locked_invoice_id','')::uuid as seg_invoice_id
  from fin f
  left join lateral (
    select seg
    from jsonb_array_elements(coalesce(f.invoice_breakdown_json->'segments','[]'::jsonb)) as t(seg)
    where (seg->>'segment_id') = ('nhsp:' || f.shift_id::text)
    limit 1
  ) x on true
),
invline_old as (
  select
    s.*,
    -- NHSP invoice run writes invoice_lines.meta_json.nhsp_shift_id
    -- Guard: only attempt this fallback for NHSP.
    case
      when s.source_system = 'NHSP' then (
        select max(il.total_charge_ex_vat)
        from public.invoice_lines il
        where il.meta_json->>'nhsp_shift_id' = s.shift_id::text
      )
      else null
    end as invline_old_charge_ex
  from seg_old s
),
new_hours as (
  select
    a.*,
    h.hours_day, h.hours_night, h.hours_sat, h.hours_sun, h.hours_bh, h.total_hours,
    greatest(
      0,
      (extract(epoch from (a.new_end_utc - a.new_start_utc))/60)::int - coalesce(a.new_break_mins,0)
    ) as new_paid_minutes
  from invline_old a
  left join lateral public._wkimp_bucket_hours_from_policy(
    coalesce(a.policy_snapshot_json, '{}'::jsonb),
    a.new_start_utc,
    a.new_end_utc,
    a.new_break_mins
  ) h on true
),
amounts as (
  select
    n.*,

    -- OLD amounts:
    -- pay: prefer TSFIN segment pay_amount
    -- charge: prefer TSFIN segment charge_amount; for NHSP fallback to invoice_lines charge
    coalesce(n.seg_old_pay_ex, null) as old_pay_ex,
    coalesce(n.seg_old_charge_ex, n.invline_old_charge_ex, null) as old_charge_ex,

    -- NEW amounts from policy+rates (when policy + rate buckets exist)
    case
      when n.policy_snapshot_json is null then null
      else round(
        coalesce(n.hours_day,0)   * coalesce(n.pay_day,0) +
        coalesce(n.hours_night,0) * coalesce(n.pay_night,0) +
        coalesce(n.hours_sat,0)   * coalesce(n.pay_sat,0) +
        coalesce(n.hours_sun,0)   * coalesce(n.pay_sun,0) +
        coalesce(n.hours_bh,0)    * coalesce(n.pay_bh,0)
      , 2)
    end as new_pay_ex,

    case
      when n.policy_snapshot_json is null then null
      else round(
        coalesce(n.hours_day,0)   * coalesce(n.charge_day,0) +
        coalesce(n.hours_night,0) * coalesce(n.charge_night,0) +
        coalesce(n.hours_sat,0)   * coalesce(n.charge_sat,0) +
        coalesce(n.hours_sun,0)   * coalesce(n.charge_sun,0) +
        coalesce(n.hours_bh,0)    * coalesce(n.charge_bh,0)
      , 2)
    end as new_charge_ex
  from new_hours n
)
select
  a.hr_row_id,
  a.external_row_key,

  a.shift_id,
  a.source_system,

  a.candidate_id,
  a.client_id,
  a.contract_id,
  a.timesheet_id,

  coalesce(a.old_work_date, a.work_date) as work_date,
  a.week_ending_date,

  a.old_start_utc,
  a.old_end_utc,
  a.old_break_mins,

  a.new_start_utc,
  a.new_end_utc,
  a.new_break_mins,

  a.old_paid_minutes,
  a.new_paid_minutes,

  (
    a.shift_id is not null
    and (
      a.old_start_utc is distinct from a.new_start_utc
      or a.old_end_utc is distinct from a.new_end_utc
      or coalesce(a.old_break_mins,0) <> coalesce(a.new_break_mins,0)
    )
  ) as is_changed_hours,

  (a.paid_at_utc is not null) as is_paid,

  (
    -- invoiced if:
    -- - segment-level invoice lock exists, OR
    -- - timesheet-level lock exists, OR
    -- - shift row has invoice_id (NHSP invoice-run / any shift-based invoice runner)
    a.seg_invoice_id is not null
    or a.locked_by_invoice_id is not null
    or exists (select 1 from public.nhsp_shifts s2 where s2.id = a.shift_id and s2.invoice_id is not null)
  ) as is_invoiced,

  a.old_pay_ex,
  a.old_charge_ex,

  a.new_pay_ex,
  a.new_charge_ex,

  case when a.new_pay_ex is null or a.old_pay_ex is null then null else round(a.new_pay_ex - a.old_pay_ex, 2) end as delta_pay_ex,
  case when a.new_charge_ex is null or a.old_charge_ex is null then null else round(a.new_charge_ex - a.old_charge_ex, 2) end as delta_charge_ex,

  (
    (a.paid_at_utc is not null)
    and (
      a.shift_id is not null
      and (
        a.old_start_utc is distinct from a.new_start_utc
        or a.old_end_utc is distinct from a.new_end_utc
        or coalesce(a.old_break_mins,0) <> coalesce(a.new_break_mins,0)
      )
    )
  ) as requires_pay_decision,

  (
    (
      a.seg_invoice_id is not null
      or a.locked_by_invoice_id is not null
      or exists (select 1 from public.nhsp_shifts s2 where s2.id = a.shift_id and s2.invoice_id is not null)
    )
    and (
      a.shift_id is not null
      and (
        a.old_start_utc is distinct from a.new_start_utc
        or a.old_end_utc is distinct from a.new_end_utc
        or coalesce(a.old_break_mins,0) <> coalesce(a.new_break_mins,0)
      )
    )
  ) as requires_invoice_decision,

  (
    (
      (a.paid_at_utc is not null)
      or
      (a.seg_invoice_id is not null or a.locked_by_invoice_id is not null or exists (select 1 from public.nhsp_shifts s2 where s2.id = a.shift_id and s2.invoice_id is not null))
    )
    and (
      a.shift_id is not null
      and (
        a.old_start_utc is distinct from a.new_start_utc
        or a.old_end_utc is distinct from a.new_end_utc
        or coalesce(a.old_break_mins,0) <> coalesce(a.new_break_mins,0)
      )
    )
  ) as requires_any_decision
from amounts a
order by work_date asc, external_row_key asc;
$$;
