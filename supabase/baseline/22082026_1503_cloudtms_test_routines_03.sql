-- Immutable CloudTMS TEST function snapshot, page 03.
-- Generated from pg_get_functiondef; definitions only, with function body checks deferred for forward references.
-- Do not edit an applied baseline page. Add or replace routine authority in supabase/repeatable.

\set ON_ERROR_STOP on
set check_function_bodies = off;
set search_path = pg_catalog, public, extensions;

-- _trg_candidates_set_bank_hash()
CREATE OR REPLACE FUNCTION public._trg_candidates_set_bank_hash()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
begin
  new.bank_details_hash := public._bank_hash(new.sort_code, new.account_number, new.account_holder);
  return new;
end $function$;

-- _trg_change_bump()
CREATE OR REPLACE FUNCTION public._trg_change_bump()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_entity_key text;
begin
  v_entity_key := nullif(btrim(coalesce(tg_argv[0], '')), '');

  perform public._change_bump(v_entity_key);

  if tg_op = 'DELETE' then
    return old;
  else
    return new;
  end if;
end;
$function$;

-- _trg_touch_client_from_client_settings()
CREATE OR REPLACE FUNCTION public._trg_touch_client_from_client_settings()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_client_id uuid;
begin
  v_client_id := coalesce(new.client_id, old.client_id);

  if v_client_id is not null then
    update public.clients cl
       set rev = coalesce(cl.rev, 0) + 1,
           updated_at = now()
     where cl.id = v_client_id;
  end if;

  if tg_op = 'DELETE' then
    return old;
  else
    return new;
  end if;
end;
$function$;

-- _trg_umbrellas_set_bank_hash()
CREATE OR REPLACE FUNCTION public._trg_umbrellas_set_bank_hash()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
begin
  -- umbrellas schema has no account_holder; use umbrella.name as holder identity
  new.bank_details_hash := public._bank_hash(new.sort_code, new.account_number, new.name);
  return new;
end $function$;

-- _tsfin_invalid_segment_count(jsonb)
CREATE OR REPLACE FUNCTION public._tsfin_invalid_segment_count(invoice_breakdown_json jsonb)
 RETURNS integer
 LANGUAGE sql
 IMMUTABLE
AS $function$
  select case
    when invoice_breakdown_json is null then 0

    -- invoice_breakdown_json should always be an object; if it's not, it's structurally invalid
    when jsonb_typeof(invoice_breakdown_json) <> 'object' then 1

    -- only validate segments in SEGMENTS mode
    when upper(coalesce(invoice_breakdown_json->>'mode','')) <> 'SEGMENTS' then 0

    -- SEGMENTS mode must have a segments array
    when jsonb_typeof(invoice_breakdown_json->'segments') <> 'array' then 1

    -- count invalid segment elements (JSON nulls or non-objects, or missing/blank segment_id)
    else (
      select count(*)::int
      from jsonb_array_elements(invoice_breakdown_json->'segments') as seg(value)
      where jsonb_typeof(seg.value) <> 'object'
         or nullif(btrim(coalesce(seg.value->>'segment_id','')), '') is null
    )
  end;
$function$;

-- _wkimp_bucket_hours_from_policy(jsonb,timestamp with time zone,timestamp with time zone,integer)
CREATE OR REPLACE FUNCTION public._wkimp_bucket_hours_from_policy(p_policy jsonb, p_start_utc timestamp with time zone, p_end_utc timestamp with time zone, p_break_mins integer)
 RETURNS TABLE(hours_day numeric, hours_night numeric, hours_sat numeric, hours_sun numeric, hours_bh numeric, total_hours numeric)
 LANGUAGE plpgsql
 STABLE
AS $function$
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

  return next;
  return;
end;
$function$;

-- _wkimp_hhmm_to_min(text)
CREATE OR REPLACE FUNCTION public._wkimp_hhmm_to_min(p text)
 RETURNS integer
 LANGUAGE sql
 IMMUTABLE
AS $function$
  select
    case
      when p is null or btrim(p) = '' then 0
      else
        (split_part(p, ':', 1)::int * 60) +
        (split_part(p, ':', 2)::int)
    end;
$function$;

-- _wkimp_overlap_intersection2(integer,integer,integer,integer,integer,integer)
CREATE OR REPLACE FUNCTION public._wkimp_overlap_intersection2(a0 integer, b0 integer, ws1 integer, we1 integer, ws2 integer, we2 integer)
 RETURNS integer
 LANGUAGE plpgsql
 IMMUTABLE
AS $function$
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
$function$;

-- _wkimp_overlap_intersection3(integer,integer,integer,integer,integer,integer,integer,integer)
CREATE OR REPLACE FUNCTION public._wkimp_overlap_intersection3(a0 integer, b0 integer, ws1 integer, we1 integer, ws2 integer, we2 integer, ws3 integer, we3 integer)
 RETURNS integer
 LANGUAGE plpgsql
 IMMUTABLE
AS $function$
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
$function$;

-- _wkimp_overlap_range(integer,integer,integer,integer)
CREATE OR REPLACE FUNCTION public._wkimp_overlap_range(a0 integer, b0 integer, s0 integer, e0 integer)
 RETURNS integer
 LANGUAGE plpgsql
 IMMUTABLE
AS $function$
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
$function$;

-- _wkimp_overlap_window(integer,integer,integer,integer)
CREATE OR REPLACE FUNCTION public._wkimp_overlap_window(a0 integer, b0 integer, ws integer, we integer)
 RETURNS integer
 LANGUAGE plpgsql
 IMMUTABLE
AS $function$
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
$function$;

-- _wkimp_win_parts(integer,integer)
CREATE OR REPLACE FUNCTION public._wkimp_win_parts(ws integer, we integer)
 RETURNS integer[]
 LANGUAGE plpgsql
 IMMUTABLE
AS $function$
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
$function$;

-- audit_events_list(text,text,text,text,integer,integer,text,text)
CREATE OR REPLACE FUNCTION public.audit_events_list(p_search text DEFAULT NULL::text, p_action text DEFAULT NULL::text, p_object_type text DEFAULT NULL::text, p_actor_display text DEFAULT NULL::text, p_limit integer DEFAULT 50, p_offset integer DEFAULT 0, p_sort_by text DEFAULT 'ts_utc'::text, p_sort_dir text DEFAULT 'desc'::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_search text := NULLIF(BTRIM(p_search), '');
  v_action text := NULLIF(BTRIM(p_action), '');
  v_object_type text := NULLIF(BTRIM(p_object_type), '');
  v_actor_display text := NULLIF(BTRIM(p_actor_display), '');
  v_limit integer := LEAST(GREATEST(COALESCE(p_limit, 50), 1), 500);
  v_offset integer := GREATEST(COALESCE(p_offset, 0), 0);
  v_sort_by text := LOWER(COALESCE(NULLIF(BTRIM(p_sort_by), ''), 'ts_utc'));
  v_sort_dir text := LOWER(COALESCE(NULLIF(BTRIM(p_sort_dir), ''), 'desc'));
  v_items jsonb := '[]'::jsonb;
  v_total bigint := 0;
BEGIN
  -- Hash-ledger deployment probe: behaviour intentionally unchanged.
  IF v_sort_by NOT IN ('ts_utc', 'action', 'actor_display', 'object_type', 'object_id_text', 'correlation_id') THEN
    v_sort_by := 'ts_utc';
  END IF;

  IF v_sort_dir NOT IN ('asc', 'desc') THEN
    v_sort_dir := 'desc';
  END IF;

  WITH filtered AS (
    SELECT
      audit_row.id,
      audit_row.ts_utc,
      audit_row.actor_user_id,
      audit_row.actor_display,
      audit_row.actor_role_at_time,
      audit_row.object_type,
      audit_row.object_id_text,
      audit_row.action,
      audit_row.before_json,
      audit_row.after_json,
      audit_row.reason,
      audit_row.ip,
      audit_row.user_agent,
      audit_row.correlation_id
    FROM public.audit_events AS audit_row
    WHERE (
      v_search IS NULL
      OR CONCAT_WS(
        ' ',
        COALESCE(audit_row.actor_display, ''),
        COALESCE(audit_row.actor_role_at_time, ''),
        COALESCE(audit_row.object_type, ''),
        COALESCE(audit_row.object_id_text, ''),
        COALESCE(audit_row.action, ''),
        COALESCE(audit_row.reason, ''),
        COALESCE(audit_row.correlation_id, '')
      ) ILIKE ('%' || v_search || '%')
    )
      AND (v_action IS NULL OR UPPER(COALESCE(audit_row.action, '')) = UPPER(v_action))
      AND (v_object_type IS NULL OR UPPER(COALESCE(audit_row.object_type, '')) = UPPER(v_object_type))
      AND (v_actor_display IS NULL OR COALESCE(audit_row.actor_display, '') ILIKE ('%' || v_actor_display || '%'))
  ), ordered_rows AS (
    SELECT
      filtered_row.*,
      ROW_NUMBER() OVER (
        ORDER BY
          CASE WHEN v_sort_by = 'ts_utc' AND v_sort_dir = 'asc' THEN filtered_row.ts_utc END ASC NULLS LAST,
          CASE WHEN v_sort_by = 'ts_utc' AND v_sort_dir = 'desc' THEN filtered_row.ts_utc END DESC NULLS LAST,
          CASE WHEN v_sort_by = 'action' AND v_sort_dir = 'asc' THEN LOWER(filtered_row.action) END ASC NULLS LAST,
          CASE WHEN v_sort_by = 'action' AND v_sort_dir = 'desc' THEN LOWER(filtered_row.action) END DESC NULLS LAST,
          CASE WHEN v_sort_by = 'actor_display' AND v_sort_dir = 'asc' THEN LOWER(filtered_row.actor_display) END ASC NULLS LAST,
          CASE WHEN v_sort_by = 'actor_display' AND v_sort_dir = 'desc' THEN LOWER(filtered_row.actor_display) END DESC NULLS LAST,
          CASE WHEN v_sort_by = 'object_type' AND v_sort_dir = 'asc' THEN LOWER(filtered_row.object_type) END ASC NULLS LAST,
          CASE WHEN v_sort_by = 'object_type' AND v_sort_dir = 'desc' THEN LOWER(filtered_row.object_type) END DESC NULLS LAST,
          CASE WHEN v_sort_by = 'object_id_text' AND v_sort_dir = 'asc' THEN LOWER(filtered_row.object_id_text) END ASC NULLS LAST,
          CASE WHEN v_sort_by = 'object_id_text' AND v_sort_dir = 'desc' THEN LOWER(filtered_row.object_id_text) END DESC NULLS LAST,
          CASE WHEN v_sort_by = 'correlation_id' AND v_sort_dir = 'asc' THEN LOWER(filtered_row.correlation_id) END ASC NULLS LAST,
          CASE WHEN v_sort_by = 'correlation_id' AND v_sort_dir = 'desc' THEN LOWER(filtered_row.correlation_id) END DESC NULLS LAST,
          filtered_row.ts_utc DESC,
          filtered_row.id DESC
      ) AS sort_ordinal
    FROM filtered AS filtered_row
  ), page_rows AS (
    SELECT ordered_row.*
    FROM ordered_rows AS ordered_row
    WHERE ordered_row.sort_ordinal > v_offset
      AND ordered_row.sort_ordinal <= v_offset + v_limit
  ), page_aggregate AS (
    SELECT COALESCE(
      JSONB_AGG(
        JSONB_BUILD_OBJECT(
          'id', page_row.id,
          'ts_utc', page_row.ts_utc,
          'actor_user_id', page_row.actor_user_id,
          'actor_display', page_row.actor_display,
          'actor_role_at_time', page_row.actor_role_at_time,
          'object_type', page_row.object_type,
          'object_id_text', page_row.object_id_text,
          'action', page_row.action,
          'before_json', page_row.before_json,
          'after_json', page_row.after_json,
          'reason', page_row.reason,
          'ip', page_row.ip,
          'user_agent', page_row.user_agent,
          'correlation_id', page_row.correlation_id
        )
        ORDER BY page_row.sort_ordinal
      ),
      '[]'::jsonb
    ) AS items
    FROM page_rows AS page_row
  )
  SELECT page_aggregate.items,
         (SELECT COUNT(*)::bigint FROM filtered)
  INTO v_items, v_total
  FROM page_aggregate;

  RETURN JSONB_BUILD_OBJECT(
    'ok', true,
    'items', COALESCE(v_items, '[]'::jsonb),
    'total_count', COALESCE(v_total, 0),
    'limit', v_limit,
    'offset', v_offset,
    'sort_by', v_sort_by,
    'sort_dir', v_sort_dir
  );
END;
$function$;

-- bank_name_check_clear_override(text,text,text,uuid,uuid,text)
CREATE OR REPLACE FUNCTION public.bank_name_check_clear_override(p_provider text, p_env text, p_entity_kind text, p_entity_id uuid, p_actor_user_id uuid, p_bank_details_hash text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_provider text := upper(btrim(coalesce(p_provider, '')));
  v_env text := upper(btrim(coalesce(p_env, '')));
  v_kind text := upper(btrim(coalesce(p_entity_kind, '')));
  v_requested_hash text := nullif(btrim(coalesce(p_bank_details_hash, '')), '');

  v_now timestamptz := now();
  v_entity_exists boolean := false;
  v_current_hash text := null;
  v_target_hash text := null;
  v_target_is_valid_oneoff boolean := false;

  v_updated integer := 0;
  v_row public.bank_name_checks%rowtype;
begin
  if v_provider = '' then
    raise exception '%', jsonb_build_object('error', 'PROVIDER_REQUIRED')::text;
  end if;
  if v_env = '' then
    raise exception '%', jsonb_build_object('error', 'ENV_REQUIRED')::text;
  end if;
  if v_kind not in ('CANDIDATE', 'UMBRELLA') then
    raise exception '%', jsonb_build_object('error', 'INVALID_ENTITY_KIND', 'expected', 'CANDIDATE|UMBRELLA')::text;
  end if;
  if p_entity_id is null then
    raise exception '%', jsonb_build_object('error', 'ENTITY_ID_REQUIRED')::text;
  end if;

  if v_kind = 'CANDIDATE' then
    perform 1
    from public.candidates c
    where c.id = p_entity_id;

    v_entity_exists := found;

    if v_entity_exists then
      select c.bank_details_hash
      into v_current_hash
      from public.candidates c
      where c.id = p_entity_id
      limit 1;
    end if;
  else
    perform 1
    from public.umbrellas u
    where u.id = p_entity_id;

    v_entity_exists := found;

    if v_entity_exists then
      select u.bank_details_hash
      into v_current_hash
      from public.umbrellas u
      where u.id = p_entity_id
      limit 1;
    end if;
  end if;

  if not v_entity_exists then
    raise exception '%', jsonb_build_object('error', 'ENTITY_NOT_FOUND_OR_NO_HASH', 'entity_kind', v_kind)::text;
  end if;

  if v_requested_hash is null then
    v_target_hash := v_current_hash;
  elsif v_current_hash is not null and v_current_hash = v_requested_hash then
    v_target_hash := v_current_hash;
  elsif v_kind = 'CANDIDATE' then
    select exists (
      select 1
      from public.v_finance_cases_register vfcr
      where vfcr.candidate_id = p_entity_id
        and vfcr.routing_kind::text = 'ONE_OFF_SPECIFIED_BANK_ACCOUNT'
        and coalesce(vfcr.edit_bank_details_allowed, false) = true
        and coalesce(vfcr.oneoff_bank_details_present, false) = true
        and vfcr.oneoff_bank_details_hash = v_requested_hash
    )
    into v_target_is_valid_oneoff;

    if v_target_is_valid_oneoff then
      v_target_hash := v_requested_hash;
    end if;
  end if;

  if v_target_hash is null then
    if v_requested_hash is null then
      raise exception '%', jsonb_build_object('error', 'ENTITY_NOT_FOUND_OR_NO_HASH', 'entity_kind', v_kind)::text;
    end if;

    raise exception '%', jsonb_build_object(
      'error', 'INVALID_TARGET_BANK_DETAILS_HASH',
      'entity_kind', v_kind,
      'entity_id', p_entity_id::text,
      'bank_details_hash', v_requested_hash
    )::text;
  end if;

  update public.bank_name_checks bnc
  set
    override_reason = null,
    override_by_user_id = null,
    override_at_utc = null,
    override_hash = null,
    updated_at_utc = v_now
  where bnc.rail_provider = v_provider
    and bnc.rail_env = v_env
    and bnc.entity_kind = v_kind
    and bnc.entity_id = p_entity_id
    and bnc.bank_details_hash = v_target_hash;

  get diagnostics v_updated = row_count;

  if v_updated = 0 then
    return jsonb_build_object(
      'ok', true,
      'did_update', false,
      'row', null
    );
  end if;

  select bnc2.*
  into v_row
  from public.bank_name_checks bnc2
  where bnc2.rail_provider = v_provider
    and bnc2.rail_env = v_env
    and bnc2.entity_kind = v_kind
    and bnc2.entity_id = p_entity_id
    and bnc2.bank_details_hash = v_target_hash
  limit 1;

  return jsonb_build_object(
    'ok', true,
    'did_update', true,
    'row', jsonb_build_object(
      'rail_provider', v_row.rail_provider,
      'rail_env', v_row.rail_env,
      'entity_kind', v_row.entity_kind,
      'entity_id', v_row.entity_id::text,
      'bank_details_hash', v_row.bank_details_hash,
      'status', v_row.status,
      'checked_at_utc', v_row.checked_at_utc,
      'result_json', v_row.result_json,
      'override_reason', v_row.override_reason,
      'override_by_user_id', case when v_row.override_by_user_id is null then null else v_row.override_by_user_id::text end,
      'override_at_utc', v_row.override_at_utc,
      'override_hash', v_row.override_hash,
      'created_at_utc', v_row.created_at_utc,
      'updated_at_utc', v_row.updated_at_utc
    )
  );
end;
$function$;

-- bank_name_check_record_result(text,text,text,uuid,text,text,jsonb,timestamp with time zone,uuid)
CREATE OR REPLACE FUNCTION public.bank_name_check_record_result(p_provider text, p_env text, p_entity_kind text, p_entity_id uuid, p_bank_details_hash text, p_status text, p_result_json jsonb, p_checked_at_utc timestamp with time zone, p_actor_user_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_provider text := upper(btrim(coalesce(p_provider, '')));
  v_env text := upper(btrim(coalesce(p_env, '')));
  v_kind text := upper(btrim(coalesce(p_entity_kind, '')));
  v_status text := upper(btrim(coalesce(p_status, '')));
  v_provided_hash text := btrim(coalesce(p_bank_details_hash, ''));

  v_entity_exists boolean := false;
  v_current_hash text := null;
  v_hash_is_valid boolean := false;
  v_now timestamptz := now();

  v_inserted boolean := false;
  v_action text := null;
  v_row_json jsonb;
begin
  if v_provider = '' then
    raise exception '%', jsonb_build_object('error', 'PROVIDER_REQUIRED')::text;
  end if;
  if v_env = '' then
    raise exception '%', jsonb_build_object('error', 'ENV_REQUIRED')::text;
  end if;
  if v_kind not in ('CANDIDATE', 'UMBRELLA') then
    raise exception '%', jsonb_build_object('error', 'INVALID_ENTITY_KIND', 'expected', 'CANDIDATE|UMBRELLA')::text;
  end if;
  if p_entity_id is null then
    raise exception '%', jsonb_build_object('error', 'ENTITY_ID_REQUIRED')::text;
  end if;
  if v_status not in ('UNVERIFIED', 'PASS', 'NEAR_MATCH', 'FAIL', 'UNAVAILABLE') then
    raise exception '%', jsonb_build_object('error', 'INVALID_STATUS')::text;
  end if;
  if v_provided_hash = '' then
    raise exception '%', jsonb_build_object('error', 'BANK_DETAILS_HASH_REQUIRED')::text;
  end if;

  if v_kind = 'CANDIDATE' then
    perform 1
    from public.candidates c
    where c.id = p_entity_id;

    v_entity_exists := found;

    if v_entity_exists then
      select c.bank_details_hash
      into v_current_hash
      from public.candidates c
      where c.id = p_entity_id
      limit 1;
    end if;
  else
    perform 1
    from public.umbrellas u
    where u.id = p_entity_id;

    v_entity_exists := found;

    if v_entity_exists then
      select u.bank_details_hash
      into v_current_hash
      from public.umbrellas u
      where u.id = p_entity_id
      limit 1;
    end if;
  end if;

  if not v_entity_exists then
    raise exception '%', jsonb_build_object('error', 'ENTITY_NOT_FOUND_OR_NO_HASH', 'entity_kind', v_kind)::text;
  end if;

  v_hash_is_valid := (v_current_hash is not null and v_current_hash = v_provided_hash);

  if not v_hash_is_valid and v_kind = 'CANDIDATE' then
    select exists (
      select 1
      from public.v_finance_cases_register vfcr
      where vfcr.candidate_id = p_entity_id
        and vfcr.routing_kind::text = 'ONE_OFF_SPECIFIED_BANK_ACCOUNT'
        and coalesce(vfcr.edit_bank_details_allowed, false) = true
        and coalesce(vfcr.oneoff_bank_details_present, false) = true
        and vfcr.oneoff_bank_details_hash = v_provided_hash
    )
    into v_hash_is_valid;
  end if;

  if not v_hash_is_valid then
    return jsonb_build_object(
      'ok', true,
      'action', 'ignored_stale_hash',
      'ignored', true,
      'reason', 'STALE_HASH',
      'entity_kind', v_kind,
      'entity_id', p_entity_id::text,
      'current_bank_details_hash', v_current_hash,
      'provided_bank_details_hash', v_provided_hash
    );
  end if;

  with upserted as (
    insert into public.bank_name_checks (
      rail_provider,
      rail_env,
      entity_kind,
      entity_id,
      bank_details_hash,
      status,
      checked_at_utc,
      result_json,
      created_at_utc,
      updated_at_utc,
      override_reason,
      override_by_user_id,
      override_at_utc,
      override_hash
    )
    values (
      v_provider,
      v_env,
      v_kind,
      p_entity_id,
      v_provided_hash,
      v_status,
      coalesce(p_checked_at_utc, v_now),
      p_result_json,
      v_now,
      v_now,
      null::text,
      null::uuid,
      null::timestamptz,
      null::text
    )
    on conflict (rail_provider, rail_env, entity_kind, entity_id, bank_details_hash)
    do update set
      status = excluded.status,
      checked_at_utc = excluded.checked_at_utc,
      result_json = excluded.result_json,
      updated_at_utc = v_now,
      override_reason = case
        when excluded.status = 'PASS' then null::text
        else public.bank_name_checks.override_reason
      end,
      override_by_user_id = case
        when excluded.status = 'PASS' then null::uuid
        else public.bank_name_checks.override_by_user_id
      end,
      override_at_utc = case
        when excluded.status = 'PASS' then null::timestamptz
        else public.bank_name_checks.override_at_utc
      end,
      override_hash = case
        when excluded.status = 'PASS' then null::text
        else public.bank_name_checks.override_hash
      end
    returning
      public.bank_name_checks.*,
      (xmax = 0) as inserted_flag
  )
  select
    u.inserted_flag,
    jsonb_build_object(
      'id', u.id::text,
      'rail_provider', u.rail_provider,
      'rail_env', u.rail_env,
      'entity_kind', u.entity_kind,
      'entity_id', u.entity_id::text,
      'bank_details_hash', u.bank_details_hash,
      'status', u.status,
      'checked_at_utc', u.checked_at_utc,
      'result_json', u.result_json,
      'override_reason', u.override_reason,
      'override_by_user_id', case when u.override_by_user_id is null then null else u.override_by_user_id::text end,
      'override_at_utc', u.override_at_utc,
      'override_hash', u.override_hash,
      'created_at_utc', u.created_at_utc,
      'updated_at_utc', u.updated_at_utc
    )
  into
    v_inserted,
    v_row_json
  from upserted u
  limit 1;

  v_action := case when v_inserted then 'inserted' else 'updated' end;

  return jsonb_build_object(
    'ok', true,
    'action', v_action,
    'ignored', false,
    'row', v_row_json
  );
end;
$function$;

-- bank_name_check_set_override(text,text,text,uuid,text,uuid,text)
CREATE OR REPLACE FUNCTION public.bank_name_check_set_override(p_provider text, p_env text, p_entity_kind text, p_entity_id uuid, p_reason text, p_actor_user_id uuid, p_bank_details_hash text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_provider text := upper(btrim(coalesce(p_provider, '')));
  v_env text := upper(btrim(coalesce(p_env, '')));
  v_kind text := upper(btrim(coalesce(p_entity_kind, '')));
  v_reason text := nullif(btrim(coalesce(p_reason, '')), '');
  v_requested_hash text := nullif(btrim(coalesce(p_bank_details_hash, '')), '');
  v_now timestamptz := now();

  v_entity_exists boolean := false;
  v_current_hash text := null;
  v_target_hash text := null;
  v_target_is_valid_oneoff boolean := false;

  v_before public.bank_name_checks%rowtype;
  v_row public.bank_name_checks%rowtype;

  v_before_json jsonb := null;
  v_after_json jsonb := null;

  v_inserted boolean := false;
  v_action text := null;
begin
  if v_provider = '' then
    raise exception '%', jsonb_build_object('error', 'PROVIDER_REQUIRED')::text;
  end if;
  if v_env = '' then
    raise exception '%', jsonb_build_object('error', 'ENV_REQUIRED')::text;
  end if;
  if v_kind not in ('CANDIDATE', 'UMBRELLA') then
    raise exception '%', jsonb_build_object('error', 'INVALID_ENTITY_KIND', 'expected', 'CANDIDATE|UMBRELLA')::text;
  end if;
  if p_entity_id is null then
    raise exception '%', jsonb_build_object('error', 'ENTITY_ID_REQUIRED')::text;
  end if;
  if v_reason is null then
    raise exception '%', jsonb_build_object('error', 'REASON_REQUIRED')::text;
  end if;

  if v_kind = 'CANDIDATE' then
    perform 1
    from public.candidates c
    where c.id = p_entity_id;

    v_entity_exists := found;

    if v_entity_exists then
      select c.bank_details_hash
      into v_current_hash
      from public.candidates c
      where c.id = p_entity_id
      limit 1;
    end if;
  else
    perform 1
    from public.umbrellas u
    where u.id = p_entity_id;

    v_entity_exists := found;

    if v_entity_exists then
      select u.bank_details_hash
      into v_current_hash
      from public.umbrellas u
      where u.id = p_entity_id
      limit 1;
    end if;
  end if;

  if not v_entity_exists then
    raise exception '%', jsonb_build_object('error', 'ENTITY_NOT_FOUND_OR_NO_HASH', 'entity_kind', v_kind)::text;
  end if;

  if v_requested_hash is null then
    v_target_hash := v_current_hash;
  elsif v_current_hash is not null and v_current_hash = v_requested_hash then
    v_target_hash := v_current_hash;
  elsif v_kind = 'CANDIDATE' then
    select exists (
      select 1
      from public.v_finance_cases_register vfcr
      where vfcr.candidate_id = p_entity_id
        and vfcr.routing_kind::text = 'ONE_OFF_SPECIFIED_BANK_ACCOUNT'
        and coalesce(vfcr.edit_bank_details_allowed, false) = true
        and coalesce(vfcr.oneoff_bank_details_present, false) = true
        and vfcr.oneoff_bank_details_hash = v_requested_hash
    )
    into v_target_is_valid_oneoff;

    if v_target_is_valid_oneoff then
      v_target_hash := v_requested_hash;
    end if;
  end if;

  if v_target_hash is null then
    if v_requested_hash is null then
      raise exception '%', jsonb_build_object('error', 'ENTITY_NOT_FOUND_OR_NO_HASH', 'entity_kind', v_kind)::text;
    end if;

    raise exception '%', jsonb_build_object(
      'error', 'INVALID_TARGET_BANK_DETAILS_HASH',
      'entity_kind', v_kind,
      'entity_id', p_entity_id::text,
      'bank_details_hash', v_requested_hash
    )::text;
  end if;

  select bnc.*
  into v_before
  from public.bank_name_checks bnc
  where bnc.rail_provider = v_provider
    and bnc.rail_env = v_env
    and bnc.entity_kind = v_kind
    and bnc.entity_id = p_entity_id
    and bnc.bank_details_hash = v_target_hash
  limit 1;

  if v_before.id is not null then
    v_before_json := jsonb_build_object(
      'id', v_before.id::text,
      'rail_provider', v_before.rail_provider,
      'rail_env', v_before.rail_env,
      'entity_kind', v_before.entity_kind,
      'entity_id', v_before.entity_id::text,
      'bank_details_hash', v_before.bank_details_hash,
      'status', v_before.status,
      'checked_at_utc', v_before.checked_at_utc,
      'result_json', v_before.result_json,
      'override_reason', v_before.override_reason,
      'override_by_user_id', case when v_before.override_by_user_id is null then null else v_before.override_by_user_id::text end,
      'override_at_utc', v_before.override_at_utc,
      'override_hash', v_before.override_hash,
      'created_at_utc', v_before.created_at_utc,
      'updated_at_utc', v_before.updated_at_utc
    );
  end if;

  with upserted as (
    insert into public.bank_name_checks (
      rail_provider,
      rail_env,
      entity_kind,
      entity_id,
      bank_details_hash,
      status,
      checked_at_utc,
      result_json,
      override_reason,
      override_by_user_id,
      override_at_utc,
      override_hash,
      created_at_utc,
      updated_at_utc
    )
    values (
      v_provider,
      v_env,
      v_kind,
      p_entity_id,
      v_target_hash,
      'UNVERIFIED',
      null,
      null,
      v_reason,
      p_actor_user_id,
      v_now,
      v_target_hash,
      v_now,
      v_now
    )
    on conflict (rail_provider, rail_env, entity_kind, entity_id, bank_details_hash)
    do update set
      override_reason = excluded.override_reason,
      override_by_user_id = excluded.override_by_user_id,
      override_at_utc = excluded.override_at_utc,
      override_hash = excluded.override_hash,
      updated_at_utc = v_now
    returning public.bank_name_checks.*
  )
  select u.*
  into v_row
  from upserted u
  limit 1;

  v_inserted := (v_row.created_at_utc is not null and v_row.updated_at_utc is not null and v_row.created_at_utc = v_row.updated_at_utc);
  v_action := case when v_inserted then 'inserted' else 'updated' end;

  v_after_json := jsonb_build_object(
    'id', v_row.id::text,
    'rail_provider', v_row.rail_provider,
    'rail_env', v_row.rail_env,
    'entity_kind', v_row.entity_kind,
    'entity_id', v_row.entity_id::text,
    'bank_details_hash', v_row.bank_details_hash,
    'status', v_row.status,
    'checked_at_utc', v_row.checked_at_utc,
    'result_json', v_row.result_json,
    'override_reason', v_row.override_reason,
    'override_by_user_id', case when v_row.override_by_user_id is null then null else v_row.override_by_user_id::text end,
    'override_at_utc', v_row.override_at_utc,
    'override_hash', v_row.override_hash,
    'created_at_utc', v_row.created_at_utc,
    'updated_at_utc', v_row.updated_at_utc
  );

  perform public._audit_insert(
    'bank_name_checks',
    v_row.id::text,
    'BANK_NAME_CHECK_OVERRIDE_SET',
    v_before_json,
    v_after_json,
    v_reason,
    p_actor_user_id
  );

  return jsonb_build_object(
    'ok', true,
    'action', v_action,
    'row', v_after_json
  );
end;
$function$;

-- bank_payee_map_upsert(text,text,text,uuid,text,text,text,jsonb,uuid)
CREATE OR REPLACE FUNCTION public.bank_payee_map_upsert(p_provider text, p_env text, p_entity_kind text, p_entity_id uuid, p_bank_details_hash text, p_payee_id text, p_payee_account_id text, p_meta_json jsonb, p_actor_user_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_provider text := upper(btrim(coalesce(p_provider,'')));
  v_env text := upper(btrim(coalesce(p_env,'')));
  v_kind text := upper(btrim(coalesce(p_entity_kind,'')));

  v_hash text := nullif(btrim(coalesce(p_bank_details_hash,'')), '');
  v_payee_id text := nullif(btrim(coalesce(p_payee_id,'')), '');
  v_payee_account_id text := nullif(btrim(coalesce(p_payee_account_id,'')), '');

  v_now timestamptz := now();

  v_inserted boolean := false;
  v_action text := null;

  v_row_json jsonb;
  v_out_payee_id text;
  v_out_payee_account_id text;
begin
  if v_provider = '' then
    raise exception '%', jsonb_build_object('error','PROVIDER_REQUIRED')::text;
  end if;

  if v_env = '' then
    raise exception '%', jsonb_build_object('error','ENV_REQUIRED')::text;
  end if;

  if v_kind not in ('CANDIDATE','UMBRELLA') then
    raise exception '%', jsonb_build_object('error','INVALID_ENTITY_KIND','expected','CANDIDATE|UMBRELLA')::text;
  end if;

  if p_entity_id is null then
    raise exception '%', jsonb_build_object('error','ENTITY_ID_REQUIRED')::text;
  end if;

  if v_hash is null then
    raise exception '%', jsonb_build_object('error','BANK_DETAILS_HASH_REQUIRED')::text;
  end if;

  if v_payee_id is null then
    raise exception '%', jsonb_build_object('error','PAYEE_ID_REQUIRED')::text;
  end if;

  -- ✅ Approach A: single statement consumes the CTE and captures inserted_flag + output fields.
  with upserted as (
    insert into public.bank_payee_map (
      rail_provider,
      rail_env,
      entity_kind,
      entity_id,
      bank_details_hash,
      payee_id,
      payee_account_id,
      meta_json,
      created_at_utc,
      updated_at_utc
    )
    values (
      v_provider,
      v_env,
      v_kind,
      p_entity_id,
      v_hash,
      v_payee_id,
      v_payee_account_id,
      p_meta_json,
      v_now,
      v_now
    )
    on conflict (rail_provider, rail_env, entity_kind, entity_id, bank_details_hash)
    do update set
      payee_id = excluded.payee_id,
      payee_account_id = excluded.payee_account_id,
      meta_json = excluded.meta_json,
      updated_at_utc = v_now
    returning
      public.bank_payee_map.*,
      (xmax = 0) as inserted_flag
  )
  select
    u.inserted_flag,
    u.payee_id,
    u.payee_account_id,
    jsonb_build_object(
      'rail_provider', u.rail_provider,
      'rail_env', u.rail_env,
      'entity_kind', u.entity_kind,
      'entity_id', u.entity_id::text,
      'bank_details_hash', u.bank_details_hash,
      'payee_id', u.payee_id,
      'payee_account_id', u.payee_account_id,
      'meta_json', u.meta_json,
      'created_at_utc', u.created_at_utc,
      'updated_at_utc', u.updated_at_utc
    )
  into
    v_inserted,
    v_out_payee_id,
    v_out_payee_account_id,
    v_row_json
  from upserted u
  limit 1;

  v_action := case when v_inserted then 'inserted' else 'updated' end;

  return jsonb_build_object(
    'ok', true,
    'action', v_action,
    'payee_id', v_out_payee_id,
    'payee_account_id', v_out_payee_account_id,
    'row', v_row_json
  );
end;
$function$;

-- bank_readiness_lock(text,text,text,uuid,text,text)
CREATE OR REPLACE FUNCTION public.bank_readiness_lock(p_provider text, p_env text, p_entity_kind text, p_entity_id uuid, p_bank_details_hash text, p_lock_kind text DEFAULT 'READINESS'::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_provider text;
  v_env text;
  v_entity_kind text;
  v_bank_hash text;
  v_lock_kind text;
  v_key text;

  v_h1 int;
  v_h2 int;
  v_lock_key bigint;
begin
  v_provider := upper(btrim(coalesce(p_provider,'')));
  v_env := upper(btrim(coalesce(p_env,'')));
  v_entity_kind := upper(btrim(coalesce(p_entity_kind,'')));
  v_bank_hash := btrim(coalesce(p_bank_details_hash,''));
  v_lock_kind := upper(btrim(coalesce(p_lock_kind,'READINESS')));

  if v_provider = '' then
    raise exception 'provider is required';
  end if;

  if v_env = '' then
    raise exception 'env is required';
  end if;

  if v_entity_kind = '' then
    raise exception 'entity_kind is required';
  end if;

  if p_entity_id is null then
    raise exception 'entity_id is required';
  end if;

  if v_bank_hash = '' then
    raise exception 'bank_details_hash is required';
  end if;

  if v_lock_kind = '' then
    raise exception 'lock_kind is required';
  end if;

  v_key := v_lock_kind
           || '|' || v_provider
           || '|' || v_env
           || '|' || v_entity_kind
           || '|' || p_entity_id::text
           || '|' || v_bank_hash;

  -- Build a stable 64-bit advisory lock key from two 32-bit hashes.
  v_h1 := hashtext(v_key);
  v_h2 := hashtext(v_key || '|2');

  v_lock_key :=
    (( (v_h1::bigint & 4294967295) << 32 )
      | (v_h2::bigint & 4294967295));

  perform pg_advisory_xact_lock(v_lock_key);

  return jsonb_build_object(
    'ok', true
  );
end;
$function$;

-- banking_alert_acknowledge_all_current(uuid,text)
CREATE OR REPLACE FUNCTION public.banking_alert_acknowledge_all_current(p_actor_user_id uuid, p_note text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_active_json jsonb := '{}'::jsonb;
  v_remaining_json jsonb := '{}'::jsonb;
  v_active_count integer := 0;
  v_created_count integer := 0;
  v_already_acknowledged_count integer := 0;
  v_acknowledged_count integer := 0;
  v_remaining_count integer := 0;
  v_acknowledged_alerts jsonb := '[]'::jsonb;
  v_signal_json jsonb := '{}'::jsonb;
BEGIN
  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'BANKING_ALERT_ACKNOWLEDGE_ALL_CURRENT_ACTOR_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'BANKING_ALERT_ACKNOWLEDGE_ALL_CURRENT_ACTOR_REQUIRED')::text;
  END IF;

  PERFORM pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtext('banking_alert_acknowledge_all_current'),
    pg_catalog.hashtext(p_actor_user_id::text || ':USER')
  );

  v_active_json := public.banking_alerts_active_for_user(
    p_actor_user_id,
    NULL::text,
    NULL::uuid,
    false,
    0,
    'ALERT_MANAGEMENT'
  );

  WITH active_alert_rows AS (
    SELECT
      active_alert_element.alert_json ->> 'alert_fingerprint' AS active_alert_fingerprint,
      active_alert_element.alert_json ->> 'alert_kind' AS active_alert_kind,
      active_alert_element.alert_json ->> 'entity_kind' AS active_entity_kind,
      (active_alert_element.alert_json ->> 'entity_id')::uuid AS active_entity_id,
      COALESCE(active_alert_element.alert_json -> 'payload_json', '{}'::jsonb) AS active_payload_json
    FROM jsonb_array_elements(COALESCE(v_active_json -> 'alerts', '[]'::jsonb)) AS active_alert_element(alert_json)
    WHERE NULLIF(BTRIM(COALESCE(active_alert_element.alert_json ->> 'alert_fingerprint', '')), '') IS NOT NULL
      AND NULLIF(BTRIM(COALESCE(active_alert_element.alert_json ->> 'alert_kind', '')), '') IS NOT NULL
      AND NULLIF(BTRIM(COALESCE(active_alert_element.alert_json ->> 'entity_kind', '')), '') IS NOT NULL
      AND NULLIF(BTRIM(COALESCE(active_alert_element.alert_json ->> 'entity_id', '')), '') IS NOT NULL
      AND COALESCE((active_alert_element.alert_json ->> 'acknowledged_for_current_user')::boolean, false) = false
  ),
  deduped_active_alert_rows AS (
    SELECT DISTINCT ON (active_alert_rows.active_alert_fingerprint)
      active_alert_rows.active_alert_fingerprint,
      upper(btrim(active_alert_rows.active_alert_kind)) AS active_alert_kind,
      lower(btrim(active_alert_rows.active_entity_kind)) AS active_entity_kind,
      active_alert_rows.active_entity_id,
      COALESCE(active_alert_rows.active_payload_json, '{}'::jsonb) AS active_payload_json
    FROM active_alert_rows
    ORDER BY active_alert_rows.active_alert_fingerprint ASC
  ),
  upserted_acknowledgements AS (
    INSERT INTO public.banking_alert_acknowledgements AS banking_alert_acknowledgement_upsert (
      alert_fingerprint,
      alert_kind,
      entity_kind,
      entity_id,
      acknowledged_by_user_id,
      acknowledged_at_utc,
      acknowledge_scope,
      note,
      alert_payload_json,
      resolved_at_ack
    )
    SELECT
      deduped_active_alert_rows.active_alert_fingerprint,
      deduped_active_alert_rows.active_alert_kind,
      deduped_active_alert_rows.active_entity_kind,
      deduped_active_alert_rows.active_entity_id,
      p_actor_user_id,
      now(),
      'USER',
      NULLIF(BTRIM(COALESCE(p_note, '')), ''),
      deduped_active_alert_rows.active_payload_json,
      false
    FROM deduped_active_alert_rows
    WHERE true
    ON CONFLICT (alert_fingerprint, acknowledged_by_user_id, acknowledge_scope) DO UPDATE
    SET
      note = COALESCE(EXCLUDED.note, banking_alert_acknowledgement_upsert.note),
      alert_payload_json = CASE
        WHEN EXCLUDED.alert_payload_json = '{}'::jsonb THEN banking_alert_acknowledgement_upsert.alert_payload_json
        ELSE EXCLUDED.alert_payload_json
      END
    RETURNING
      banking_alert_acknowledgement_upsert.alert_fingerprint,
      banking_alert_acknowledgement_upsert.alert_kind,
      banking_alert_acknowledgement_upsert.entity_kind,
      banking_alert_acknowledgement_upsert.entity_id,
      banking_alert_acknowledgement_upsert.acknowledged_at_utc,
      (banking_alert_acknowledgement_upsert.xmax = 0) AS created
  ),
  acknowledgement_summary AS (
    SELECT
      COALESCE(count(*) FILTER (WHERE upserted_acknowledgements.created = true), 0)::integer AS created_alert_count,
      COALESCE(count(*) FILTER (WHERE upserted_acknowledgements.created = false), 0)::integer AS already_acknowledged_alert_count,
      COALESCE(count(*), 0)::integer AS acknowledged_alert_count,
      COALESCE(jsonb_agg(jsonb_build_object(
        'alert_fingerprint', upserted_acknowledgements.alert_fingerprint,
        'alert_kind', upserted_acknowledgements.alert_kind,
        'entity_kind', upserted_acknowledgements.entity_kind,
        'entity_id', upserted_acknowledgements.entity_id::text,
        'acknowledged_at_utc', upserted_acknowledgements.acknowledged_at_utc::text,
        'created', upserted_acknowledgements.created,
        'already_acknowledged', NOT upserted_acknowledgements.created
      ) ORDER BY upserted_acknowledgements.acknowledged_at_utc DESC, upserted_acknowledgements.alert_fingerprint ASC), '[]'::jsonb) AS acknowledged_alerts_json
    FROM upserted_acknowledgements
  ),
  counted_active_alerts AS (
    SELECT COUNT(*)::integer AS active_alert_count
    FROM deduped_active_alert_rows
  )
  SELECT
    COALESCE(counted_active_alerts.active_alert_count, 0),
    COALESCE(acknowledgement_summary.created_alert_count, 0),
    COALESCE(acknowledgement_summary.already_acknowledged_alert_count, 0),
    COALESCE(acknowledgement_summary.acknowledged_alert_count, 0),
    COALESCE(acknowledgement_summary.acknowledged_alerts_json, '[]'::jsonb)
  INTO
    v_active_count,
    v_created_count,
    v_already_acknowledged_count,
    v_acknowledged_count,
    v_acknowledged_alerts
  FROM counted_active_alerts
  CROSS JOIN acknowledgement_summary;

  v_remaining_json := public.banking_alerts_active_for_user(
    p_actor_user_id,
    NULL::text,
    NULL::uuid,
    false,
    25,
    'ALERT_MANAGEMENT'
  );

  v_signal_json := public.banking_alert_signal_for_user(
    p_actor_user_id,
    NULL::text,
    'ALERT_MANAGEMENT'
  );

  v_remaining_count := COALESCE((v_signal_json ->> 'banking_unacknowledged_alert_count')::integer, 0);

  v_remaining_json := v_remaining_json || jsonb_build_object(
    'banking_alert_hash', COALESCE(v_signal_json ->> 'banking_alert_hash', 'banking_alert_signal:v2:' || MD5('')),
    'banking_unacknowledged_alert_count', v_remaining_count,
    'banking_highest_alert_severity', COALESCE(v_signal_json ->> 'banking_highest_alert_severity', ''),
    'banking_highest_alert_label', COALESCE(v_signal_json ->> 'banking_highest_alert_label', ''),
    'unacknowledged_count', v_remaining_count,
    'highest_severity', NULLIF(COALESCE(v_signal_json ->> 'banking_highest_alert_severity', ''), ''),
    'highest_label', NULLIF(COALESCE(v_signal_json ->> 'banking_highest_alert_label', ''), '')
  );

  RETURN jsonb_build_object(
    'ok', true,
    'mode', 'clear_all',
    'active_count_before_acknowledge', COALESCE(v_active_count, 0),
    'requested_count', COALESCE(v_active_count, 0),
    'created_count', COALESCE(v_created_count, 0),
    'already_acknowledged_count', COALESCE(v_already_acknowledged_count, 0),
    'acknowledged_count', COALESCE(v_acknowledged_count, 0),
    'remaining_unacknowledged_count', COALESCE(v_remaining_count, 0),
    'banking_alert_hash', COALESCE(v_signal_json ->> 'banking_alert_hash', 'banking_alert_signal:v2:' || MD5('')),
    'banking_unacknowledged_alert_count', COALESCE(v_remaining_count, 0),
    'banking_highest_alert_severity', COALESCE(v_signal_json ->> 'banking_highest_alert_severity', ''),
    'banking_highest_alert_label', COALESCE(v_signal_json ->> 'banking_highest_alert_label', ''),
    'ignored_count', GREATEST(COALESCE(v_active_count, 0) - COALESCE(v_acknowledged_count, 0), 0),
    'acknowledged_alerts', COALESCE(v_acknowledged_alerts, '[]'::jsonb),
    'remaining_alert_summary', v_remaining_json,
    'alert_summary', v_remaining_json
  );
END;
$function$;

-- banking_alert_acknowledge_many(uuid,jsonb,text)
CREATE OR REPLACE FUNCTION public.banking_alert_acknowledge_many(p_actor_user_id uuid, p_alerts_json jsonb, p_note text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_requested_count integer := 0;
  v_created_count integer := 0;
  v_already_acknowledged_count integer := 0;
  v_acknowledged_count integer := 0;
  v_ignored_count integer := 0;
  v_remaining_count integer := 0;
  v_active_json jsonb := '{}'::jsonb;
  v_remaining_json jsonb := '{}'::jsonb;
  v_acknowledged_alerts jsonb := '[]'::jsonb;
  v_signal_json jsonb := '{}'::jsonb;
BEGIN
  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'BANKING_ALERT_ACKNOWLEDGE_MANY_ACTOR_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'BANKING_ALERT_ACKNOWLEDGE_MANY_ACTOR_REQUIRED')::text;
  END IF;

  IF p_alerts_json IS NULL OR coalesce(jsonb_typeof(p_alerts_json), 'null') <> 'array' THEN
    RAISE EXCEPTION 'BANKING_ALERT_ACKNOWLEDGE_MANY_ALERTS_ARRAY_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'BANKING_ALERT_ACKNOWLEDGE_MANY_ALERTS_ARRAY_REQUIRED')::text;
  END IF;

  PERFORM pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtext('banking_alert_acknowledge_many'),
    pg_catalog.hashtext(p_actor_user_id::text || ':USER')
  );

  WITH requested_fingerprints_for_count AS (
    SELECT DISTINCT
      nullif(btrim(coalesce(
        CASE
          WHEN jsonb_typeof(requested_alerts_for_count.requested_alert_json) = 'string'
            THEN trim(both '"' from requested_alerts_for_count.requested_alert_json::text)
          WHEN jsonb_typeof(requested_alerts_for_count.requested_alert_json) = 'object'
            THEN COALESCE(
              requested_alerts_for_count.requested_alert_json ->> 'alert_fingerprint',
              requested_alerts_for_count.requested_alert_json ->> 'fingerprint'
            )
          ELSE NULL::text
        END,
        ''
      )), '') AS alert_fingerprint
    FROM jsonb_array_elements(p_alerts_json) AS requested_alerts_for_count(requested_alert_json)
  )
  SELECT count(*)::integer
  INTO v_requested_count
  FROM requested_fingerprints_for_count
  WHERE requested_fingerprints_for_count.alert_fingerprint IS NOT NULL;

  v_active_json := public.banking_alerts_active_for_user(
    p_actor_user_id,
    NULL::text,
    NULL::uuid,
    false,
    0,
    'ALERT_MANAGEMENT'
  );

  WITH requested_fingerprints AS (
    SELECT DISTINCT
      nullif(btrim(coalesce(
        CASE
          WHEN jsonb_typeof(requested_alerts.requested_alert_json) = 'string'
            THEN trim(both '"' from requested_alerts.requested_alert_json::text)
          WHEN jsonb_typeof(requested_alerts.requested_alert_json) = 'object'
            THEN COALESCE(
              requested_alerts.requested_alert_json ->> 'alert_fingerprint',
              requested_alerts.requested_alert_json ->> 'fingerprint'
            )
          ELSE NULL::text
        END,
        ''
      )), '') AS alert_fingerprint
    FROM jsonb_array_elements(p_alerts_json) AS requested_alerts(requested_alert_json)
  ),
  active_alerts AS (
    SELECT
      active_alert.alert_json ->> 'alert_fingerprint' AS alert_fingerprint,
      active_alert.alert_json ->> 'alert_kind' AS alert_kind,
      active_alert.alert_json ->> 'entity_kind' AS entity_kind,
      (active_alert.alert_json ->> 'entity_id')::uuid AS entity_id,
      active_alert.alert_json AS alert_json,
      coalesce(active_alert.alert_json -> 'payload_json', '{}'::jsonb) AS payload_json
    FROM jsonb_array_elements(coalesce(v_active_json -> 'alerts', '[]'::jsonb)) AS active_alert(alert_json)
    WHERE nullif(btrim(coalesce(active_alert.alert_json ->> 'alert_fingerprint', '')), '') IS NOT NULL
      AND nullif(btrim(coalesce(active_alert.alert_json ->> 'alert_kind', '')), '') IS NOT NULL
      AND nullif(btrim(coalesce(active_alert.alert_json ->> 'entity_kind', '')), '') IS NOT NULL
      AND nullif(btrim(coalesce(active_alert.alert_json ->> 'entity_id', '')), '') IS NOT NULL
  ),
  matched_active_alerts AS (
    SELECT DISTINCT ON (active_alerts.alert_fingerprint)
      active_alerts.alert_fingerprint,
      upper(btrim(active_alerts.alert_kind)) AS alert_kind,
      lower(btrim(active_alerts.entity_kind)) AS entity_kind,
      active_alerts.entity_id,
      active_alerts.alert_json,
      coalesce(active_alerts.payload_json, '{}'::jsonb) AS payload_json
    FROM active_alerts
    JOIN requested_fingerprints
      ON requested_fingerprints.alert_fingerprint = active_alerts.alert_fingerprint
    WHERE requested_fingerprints.alert_fingerprint IS NOT NULL
    ORDER BY active_alerts.alert_fingerprint ASC
  ),
  upserted_acknowledgements AS (
    INSERT INTO public.banking_alert_acknowledgements AS banking_alert_acknowledgement_upsert (
      alert_fingerprint,
      alert_kind,
      entity_kind,
      entity_id,
      acknowledged_by_user_id,
      acknowledged_at_utc,
      acknowledge_scope,
      note,
      alert_payload_json,
      resolved_at_ack
    )
    SELECT
      matched_active_alerts.alert_fingerprint,
      matched_active_alerts.alert_kind,
      matched_active_alerts.entity_kind,
      matched_active_alerts.entity_id,
      p_actor_user_id,
      now(),
      'USER',
      nullif(btrim(coalesce(p_note, '')), ''),
      matched_active_alerts.payload_json,
      false
    FROM matched_active_alerts
    WHERE true
    ON CONFLICT (alert_fingerprint, acknowledged_by_user_id, acknowledge_scope) DO UPDATE
    SET
      note = COALESCE(EXCLUDED.note, banking_alert_acknowledgement_upsert.note),
      alert_payload_json = CASE
        WHEN EXCLUDED.alert_payload_json = '{}'::jsonb THEN banking_alert_acknowledgement_upsert.alert_payload_json
        ELSE EXCLUDED.alert_payload_json
      END
    RETURNING
      banking_alert_acknowledgement_upsert.alert_fingerprint,
      banking_alert_acknowledgement_upsert.alert_kind,
      banking_alert_acknowledgement_upsert.entity_kind,
      banking_alert_acknowledgement_upsert.entity_id,
      banking_alert_acknowledgement_upsert.acknowledged_at_utc,
      (banking_alert_acknowledgement_upsert.xmax = 0) AS created
  ),
  already_existing_requested_acknowledgements AS (
    SELECT DISTINCT ON (public.banking_alert_acknowledgements.alert_fingerprint)
      public.banking_alert_acknowledgements.alert_fingerprint,
      public.banking_alert_acknowledgements.alert_kind,
      public.banking_alert_acknowledgements.entity_kind,
      public.banking_alert_acknowledgements.entity_id,
      public.banking_alert_acknowledgements.acknowledged_at_utc,
      false AS created
    FROM public.banking_alert_acknowledgements
    JOIN requested_fingerprints
      ON requested_fingerprints.alert_fingerprint = public.banking_alert_acknowledgements.alert_fingerprint
    LEFT JOIN upserted_acknowledgements
      ON upserted_acknowledgements.alert_fingerprint = public.banking_alert_acknowledgements.alert_fingerprint
    WHERE requested_fingerprints.alert_fingerprint IS NOT NULL
      AND upserted_acknowledgements.alert_fingerprint IS NULL
      AND public.banking_alert_acknowledgements.acknowledged_by_user_id = p_actor_user_id
      AND upper(btrim(coalesce(public.banking_alert_acknowledgements.acknowledge_scope, 'USER'))) = 'USER'
    ORDER BY public.banking_alert_acknowledgements.alert_fingerprint ASC,
             public.banking_alert_acknowledgements.acknowledged_at_utc ASC,
             public.banking_alert_acknowledgements.id ASC
  ),
  changed_acknowledgements AS (
    SELECT
      upserted_acknowledgements.alert_fingerprint,
      upserted_acknowledgements.alert_kind,
      upserted_acknowledgements.entity_kind,
      upserted_acknowledgements.entity_id,
      upserted_acknowledgements.acknowledged_at_utc,
      upserted_acknowledgements.created
    FROM upserted_acknowledgements

    UNION ALL

    SELECT
      already_existing_requested_acknowledgements.alert_fingerprint,
      already_existing_requested_acknowledgements.alert_kind,
      already_existing_requested_acknowledgements.entity_kind,
      already_existing_requested_acknowledgements.entity_id,
      already_existing_requested_acknowledgements.acknowledged_at_utc,
      already_existing_requested_acknowledgements.created
    FROM already_existing_requested_acknowledgements
  )
  SELECT
    coalesce(count(*) FILTER (WHERE changed_acknowledgements.created = true), 0)::integer,
    coalesce(count(*) FILTER (WHERE changed_acknowledgements.created = false), 0)::integer,
    coalesce(count(*), 0)::integer,
    coalesce(jsonb_agg(jsonb_build_object(
      'alert_fingerprint', changed_acknowledgements.alert_fingerprint,
      'alert_kind', changed_acknowledgements.alert_kind,
      'entity_kind', changed_acknowledgements.entity_kind,
      'entity_id', changed_acknowledgements.entity_id::text,
      'acknowledged_at_utc', changed_acknowledgements.acknowledged_at_utc::text,
      'created', changed_acknowledgements.created,
      'already_acknowledged', NOT changed_acknowledgements.created
    ) ORDER BY changed_acknowledgements.acknowledged_at_utc DESC, changed_acknowledgements.alert_fingerprint ASC), '[]'::jsonb)
  INTO
    v_created_count,
    v_already_acknowledged_count,
    v_acknowledged_count,
    v_acknowledged_alerts
  FROM changed_acknowledgements;

  v_ignored_count := greatest(coalesce(v_requested_count, 0) - coalesce(v_acknowledged_count, 0), 0);

  v_remaining_json := public.banking_alerts_active_for_user(
    p_actor_user_id,
    NULL::text,
    NULL::uuid,
    false,
    25,
    'ALERT_MANAGEMENT'
  );

  v_signal_json := public.banking_alert_signal_for_user(
    p_actor_user_id,
    NULL::text,
    'ALERT_MANAGEMENT'
  );

  v_remaining_count := COALESCE((v_signal_json ->> 'banking_unacknowledged_alert_count')::integer, 0);

  v_remaining_json := v_remaining_json || jsonb_build_object(
    'banking_alert_hash', COALESCE(v_signal_json ->> 'banking_alert_hash', 'banking_alert_signal:v2:' || MD5('')),
    'banking_unacknowledged_alert_count', v_remaining_count,
    'banking_highest_alert_severity', COALESCE(v_signal_json ->> 'banking_highest_alert_severity', ''),
    'banking_highest_alert_label', COALESCE(v_signal_json ->> 'banking_highest_alert_label', ''),
    'unacknowledged_count', v_remaining_count,
    'highest_severity', NULLIF(COALESCE(v_signal_json ->> 'banking_highest_alert_severity', ''), ''),
    'highest_label', NULLIF(COALESCE(v_signal_json ->> 'banking_highest_alert_label', ''), '')
  );

  RETURN jsonb_build_object(
    'ok', true,
    'requested_count', coalesce(v_requested_count, 0),
    'created_count', coalesce(v_created_count, 0),
    'already_acknowledged_count', coalesce(v_already_acknowledged_count, 0),
    'acknowledged_count', coalesce(v_acknowledged_count, 0),
    'remaining_unacknowledged_count', coalesce(v_remaining_count, 0),
    'banking_alert_hash', COALESCE(v_signal_json ->> 'banking_alert_hash', 'banking_alert_signal:v2:' || MD5('')),
    'banking_unacknowledged_alert_count', coalesce(v_remaining_count, 0),
    'banking_highest_alert_severity', COALESCE(v_signal_json ->> 'banking_highest_alert_severity', ''),
    'banking_highest_alert_label', COALESCE(v_signal_json ->> 'banking_highest_alert_label', ''),
    'ignored_count', coalesce(v_ignored_count, 0),
    'acknowledged_alerts', coalesce(v_acknowledged_alerts, '[]'::jsonb),
    'remaining_alert_summary', v_remaining_json,
    'alert_summary', v_remaining_json
  );
END;
$function$;

-- banking_alert_acknowledge(text,text,text,uuid,uuid,text,jsonb)
CREATE OR REPLACE FUNCTION public.banking_alert_acknowledge(p_alert_fingerprint text, p_alert_kind text, p_entity_kind text, p_entity_id uuid, p_actor_user_id uuid, p_note text DEFAULT NULL::text, p_alert_payload_json jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_alert_fingerprint text := nullif(btrim(coalesce(p_alert_fingerprint, '')), '');
  v_alert_kind text := upper(nullif(btrim(coalesce(p_alert_kind, '')), ''));
  v_entity_kind text := lower(nullif(btrim(coalesce(p_entity_kind, '')), ''));
  v_alert_payload_json jsonb := '{}'::jsonb;
  v_active_json jsonb := '{}'::jsonb;
  v_remaining_json jsonb := '{}'::jsonb;
  v_active_alert_json jsonb := NULL::jsonb;
  v_current_alert_fingerprint text := NULL::text;
  v_existing_id uuid := NULL::uuid;
  v_existing_alert_kind text := NULL::text;
  v_existing_entity_kind text := NULL::text;
  v_existing_entity_id uuid := NULL::uuid;
  v_existing_alert_payload_json jsonb := '{}'::jsonb;
  v_ack_id uuid;
  v_ack_alert_fingerprint text;
  v_ack_alert_kind text;
  v_ack_entity_kind text;
  v_ack_entity_id uuid;
  v_acknowledged_by_user_id uuid;
  v_acknowledged_at_utc timestamptz;
  v_acknowledge_scope text;
  v_ack_note text;
  v_ack_alert_payload_json jsonb;
  v_ack_resolved_at_ack boolean;
  v_created boolean := false;
  v_upsert_inserted boolean := false;
  v_remaining_count integer := 0;
  v_signal_json jsonb := '{}'::jsonb;
BEGIN
  IF v_alert_fingerprint IS NULL THEN
    RAISE EXCEPTION 'BANKING_ALERT_ACKNOWLEDGE_FINGERPRINT_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'BANKING_ALERT_ACKNOWLEDGE_FINGERPRINT_REQUIRED')::text;
  END IF;

  IF v_alert_kind IS NULL THEN
    RAISE EXCEPTION 'BANKING_ALERT_ACKNOWLEDGE_KIND_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'BANKING_ALERT_ACKNOWLEDGE_KIND_REQUIRED')::text;
  END IF;

  IF v_entity_kind IS NULL THEN
    RAISE EXCEPTION 'BANKING_ALERT_ACKNOWLEDGE_ENTITY_KIND_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'BANKING_ALERT_ACKNOWLEDGE_ENTITY_KIND_REQUIRED')::text;
  END IF;

  IF p_entity_id IS NULL THEN
    RAISE EXCEPTION 'BANKING_ALERT_ACKNOWLEDGE_ENTITY_ID_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'BANKING_ALERT_ACKNOWLEDGE_ENTITY_ID_REQUIRED')::text;
  END IF;

  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'BANKING_ALERT_ACKNOWLEDGE_ACTOR_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'BANKING_ALERT_ACKNOWLEDGE_ACTOR_REQUIRED')::text;
  END IF;

  IF p_alert_payload_json IS NOT NULL AND coalesce(jsonb_typeof(p_alert_payload_json), 'null') <> 'object' THEN
    RAISE EXCEPTION 'BANKING_ALERT_ACKNOWLEDGE_PAYLOAD_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'BANKING_ALERT_ACKNOWLEDGE_PAYLOAD_MUST_BE_OBJECT',
              'alert_fingerprint', v_alert_fingerprint
            )::text;
  END IF;

  PERFORM pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtext('banking_alert_acknowledge:' || v_alert_fingerprint),
    pg_catalog.hashtext(p_actor_user_id::text || ':USER')
  );

  v_alert_payload_json := coalesce(p_alert_payload_json, '{}'::jsonb);

  SELECT
    public.banking_alert_acknowledgements.id,
    public.banking_alert_acknowledgements.alert_kind,
    public.banking_alert_acknowledgements.entity_kind,
    public.banking_alert_acknowledgements.entity_id,
    COALESCE(public.banking_alert_acknowledgements.alert_payload_json, '{}'::jsonb)
  INTO
    v_existing_id,
    v_existing_alert_kind,
    v_existing_entity_kind,
    v_existing_entity_id,
    v_existing_alert_payload_json
  FROM public.banking_alert_acknowledgements
  WHERE public.banking_alert_acknowledgements.alert_fingerprint = v_alert_fingerprint
    AND public.banking_alert_acknowledgements.acknowledged_by_user_id = p_actor_user_id
    AND upper(btrim(coalesce(public.banking_alert_acknowledgements.acknowledge_scope, 'USER'))) = 'USER'
  ORDER BY public.banking_alert_acknowledgements.acknowledged_at_utc ASC, public.banking_alert_acknowledgements.id ASC
  LIMIT 1
  FOR UPDATE;

  IF v_existing_id IS NOT NULL THEN
    IF v_alert_payload_json = '{}'::jsonb THEN
      v_alert_payload_json := COALESCE(v_existing_alert_payload_json, '{}'::jsonb);
    END IF;
  ELSE
    v_active_json := public.banking_alerts_active_for_user(
      p_actor_user_id,
      v_entity_kind,
      p_entity_id,
      false,
      0,
      'ALERT_MANAGEMENT'
    );

    SELECT active_alert.alert_json
    INTO v_active_alert_json
    FROM jsonb_array_elements(coalesce(v_active_json -> 'alerts', '[]'::jsonb)) AS active_alert(alert_json)
    WHERE active_alert.alert_json ->> 'alert_fingerprint' = v_alert_fingerprint
      AND upper(btrim(coalesce(active_alert.alert_json ->> 'alert_kind', ''))) = v_alert_kind
      AND lower(btrim(coalesce(active_alert.alert_json ->> 'entity_kind', ''))) = v_entity_kind
      AND nullif(btrim(coalesce(active_alert.alert_json ->> 'entity_id', '')), '') IS NOT NULL
      AND (active_alert.alert_json ->> 'entity_id')::uuid = p_entity_id
    LIMIT 1;

    IF v_active_alert_json IS NULL THEN
      SELECT
        public.banking_alert_acknowledgements.id,
        public.banking_alert_acknowledgements.alert_kind,
        public.banking_alert_acknowledgements.entity_kind,
        public.banking_alert_acknowledgements.entity_id,
        COALESCE(public.banking_alert_acknowledgements.alert_payload_json, '{}'::jsonb)
      INTO
        v_existing_id,
        v_existing_alert_kind,
        v_existing_entity_kind,
        v_existing_entity_id,
        v_existing_alert_payload_json
      FROM public.banking_alert_acknowledgements
      WHERE public.banking_alert_acknowledgements.alert_fingerprint = v_alert_fingerprint
        AND public.banking_alert_acknowledgements.acknowledged_by_user_id = p_actor_user_id
        AND upper(btrim(coalesce(public.banking_alert_acknowledgements.acknowledge_scope, 'USER'))) = 'USER'
      ORDER BY public.banking_alert_acknowledgements.acknowledged_at_utc ASC, public.banking_alert_acknowledgements.id ASC
      LIMIT 1;

      IF v_existing_id IS NOT NULL THEN
        IF v_alert_payload_json = '{}'::jsonb THEN
          v_alert_payload_json := COALESCE(v_existing_alert_payload_json, '{}'::jsonb);
        END IF;
      ELSE
        SELECT active_alert.alert_json ->> 'alert_fingerprint'
        INTO v_current_alert_fingerprint
        FROM jsonb_array_elements(coalesce(v_active_json -> 'alerts', '[]'::jsonb)) AS active_alert(alert_json)
        WHERE upper(btrim(coalesce(active_alert.alert_json ->> 'alert_kind', ''))) = v_alert_kind
          AND lower(btrim(coalesce(active_alert.alert_json ->> 'entity_kind', ''))) = v_entity_kind
          AND nullif(btrim(coalesce(active_alert.alert_json ->> 'entity_id', '')), '') IS NOT NULL
          AND (active_alert.alert_json ->> 'entity_id')::uuid = p_entity_id
        ORDER BY active_alert.alert_json ->> 'alert_fingerprint'
        LIMIT 1;

        v_remaining_json := public.banking_alerts_active_for_user(
          p_actor_user_id,
          NULL::text,
          NULL::uuid,
          false,
          25,
          'ALERT_MANAGEMENT'
        );

        v_signal_json := public.banking_alert_signal_for_user(
          p_actor_user_id,
          NULL::text,
          'ALERT_MANAGEMENT'
        );

        v_remaining_json := v_remaining_json || jsonb_build_object(
          'banking_alert_hash', COALESCE(v_signal_json ->> 'banking_alert_hash', 'banking_alert_signal:v2:' || MD5('')),
          'banking_unacknowledged_alert_count', COALESCE((v_signal_json ->> 'banking_unacknowledged_alert_count')::integer, 0),
          'banking_highest_alert_severity', COALESCE(v_signal_json ->> 'banking_highest_alert_severity', ''),
          'banking_highest_alert_label', COALESCE(v_signal_json ->> 'banking_highest_alert_label', ''),
          'unacknowledged_count', COALESCE((v_signal_json ->> 'banking_unacknowledged_alert_count')::integer, 0),
          'highest_severity', NULLIF(COALESCE(v_signal_json ->> 'banking_highest_alert_severity', ''), ''),
          'highest_label', NULLIF(COALESCE(v_signal_json ->> 'banking_highest_alert_label', ''), '')
        );

        RETURN jsonb_build_object(
          'ok', true,
          'created', false,
          'already_acknowledged', false,
          'acknowledged', false,
          'ignored', true,
          'ignored_count', 1,
          'reason', 'ALERT_NOT_CURRENT',
          'code', 'BANKING_ALERT_ACKNOWLEDGE_ALERT_NOT_ACTIVE',
          'alert_fingerprint', v_alert_fingerprint,
          'alert_kind', v_alert_kind,
          'entity_kind', v_entity_kind,
          'entity_id', p_entity_id::text,
          'current_alert_fingerprint', v_current_alert_fingerprint,
          'entity_alert_summary', v_active_json,
          'banking_alert_hash', COALESCE(v_signal_json ->> 'banking_alert_hash', 'banking_alert_signal:v2:' || MD5('')),
          'banking_unacknowledged_alert_count', COALESCE((v_signal_json ->> 'banking_unacknowledged_alert_count')::integer, 0),
          'banking_highest_alert_severity', COALESCE(v_signal_json ->> 'banking_highest_alert_severity', ''),
          'banking_highest_alert_label', COALESCE(v_signal_json ->> 'banking_highest_alert_label', ''),
          'remaining_alert_summary', v_remaining_json,
          'alert_summary', v_remaining_json
        );
      END IF;
    END IF;

    IF v_existing_id IS NULL AND v_alert_payload_json = '{}'::jsonb THEN
      v_alert_payload_json := coalesce(v_active_alert_json -> 'payload_json', '{}'::jsonb);
    END IF;
  END IF;

  INSERT INTO public.banking_alert_acknowledgements AS banking_alert_acknowledgement_upsert (
    alert_fingerprint,
    alert_kind,
    entity_kind,
    entity_id,
    acknowledged_by_user_id,
    acknowledged_at_utc,
    acknowledge_scope,
    note,
    alert_payload_json,
    resolved_at_ack
  )
  VALUES (
    v_alert_fingerprint,
    v_alert_kind,
    v_entity_kind,
    p_entity_id,
    p_actor_user_id,
    now(),
    'USER',
    nullif(btrim(coalesce(p_note, '')), ''),
    v_alert_payload_json,
    false
  )
  ON CONFLICT (alert_fingerprint, acknowledged_by_user_id, acknowledge_scope) DO UPDATE
  SET
    note = COALESCE(EXCLUDED.note, banking_alert_acknowledgement_upsert.note),
    alert_payload_json = CASE
      WHEN EXCLUDED.alert_payload_json = '{}'::jsonb THEN banking_alert_acknowledgement_upsert.alert_payload_json
      ELSE EXCLUDED.alert_payload_json
    END
  RETURNING
    banking_alert_acknowledgement_upsert.id,
    banking_alert_acknowledgement_upsert.alert_fingerprint,
    banking_alert_acknowledgement_upsert.alert_kind,
    banking_alert_acknowledgement_upsert.entity_kind,
    banking_alert_acknowledgement_upsert.entity_id,
    banking_alert_acknowledgement_upsert.acknowledged_by_user_id,
    banking_alert_acknowledgement_upsert.acknowledged_at_utc,
    banking_alert_acknowledgement_upsert.acknowledge_scope,
    banking_alert_acknowledgement_upsert.note,
    banking_alert_acknowledgement_upsert.alert_payload_json,
    banking_alert_acknowledgement_upsert.resolved_at_ack,
    (banking_alert_acknowledgement_upsert.xmax = 0)
  INTO
    v_ack_id,
    v_ack_alert_fingerprint,
    v_ack_alert_kind,
    v_ack_entity_kind,
    v_ack_entity_id,
    v_acknowledged_by_user_id,
    v_acknowledged_at_utc,
    v_acknowledge_scope,
    v_ack_note,
    v_ack_alert_payload_json,
    v_ack_resolved_at_ack,
    v_upsert_inserted;

  v_created := coalesce(v_upsert_inserted, false);

  v_remaining_json := public.banking_alerts_active_for_user(
    p_actor_user_id,
    NULL::text,
    NULL::uuid,
    false,
    25,
    'ALERT_MANAGEMENT'
  );

  v_signal_json := public.banking_alert_signal_for_user(
    p_actor_user_id,
    NULL::text,
    'ALERT_MANAGEMENT'
  );

  v_remaining_count := COALESCE((v_signal_json ->> 'banking_unacknowledged_alert_count')::integer, 0);

  v_remaining_json := v_remaining_json || jsonb_build_object(
    'banking_alert_hash', COALESCE(v_signal_json ->> 'banking_alert_hash', 'banking_alert_signal:v2:' || MD5('')),
    'banking_unacknowledged_alert_count', v_remaining_count,
    'banking_highest_alert_severity', COALESCE(v_signal_json ->> 'banking_highest_alert_severity', ''),
    'banking_highest_alert_label', COALESCE(v_signal_json ->> 'banking_highest_alert_label', ''),
    'unacknowledged_count', v_remaining_count,
    'highest_severity', NULLIF(COALESCE(v_signal_json ->> 'banking_highest_alert_severity', ''), ''),
    'highest_label', NULLIF(COALESCE(v_signal_json ->> 'banking_highest_alert_label', ''), '')
  );

  RETURN jsonb_build_object(
    'ok', true,
    'created', v_created,
    'already_acknowledged', NOT v_created,
    'acknowledged', true,
    'ignored', false,
    'ignored_count', 0,
    'remaining_unacknowledged_count', v_remaining_count,
    'banking_alert_hash', COALESCE(v_signal_json ->> 'banking_alert_hash', 'banking_alert_signal:v2:' || MD5('')),
    'banking_unacknowledged_alert_count', v_remaining_count,
    'banking_highest_alert_severity', COALESCE(v_signal_json ->> 'banking_highest_alert_severity', ''),
    'banking_highest_alert_label', COALESCE(v_signal_json ->> 'banking_highest_alert_label', ''),
    'remaining_alert_summary', v_remaining_json,
    'alert_summary', v_remaining_json,
    'acknowledgement', jsonb_build_object(
      'id', v_ack_id::text,
      'alert_fingerprint', v_ack_alert_fingerprint,
      'alert_kind', v_ack_alert_kind,
      'entity_kind', v_ack_entity_kind,
      'entity_id', v_ack_entity_id::text,
      'acknowledged_by_user_id', v_acknowledged_by_user_id::text,
      'acknowledged_at_utc', v_acknowledged_at_utc::text,
      'acknowledge_scope', v_acknowledge_scope,
      'note', v_ack_note,
      'alert_payload_json', v_ack_alert_payload_json,
      'resolved_at_ack', v_ack_resolved_at_ack
    )
  );
END;
$function$;

-- banking_alert_display_summary_refresh_for_user(uuid,text)
CREATE OR REPLACE FUNCTION public.banking_alert_display_summary_refresh_for_user(p_actor_user_id uuid, p_context text DEFAULT 'EXPLICIT_ALERT_REFRESH'::text)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_context text := UPPER(REPLACE(NULLIF(BTRIM(COALESCE(p_context, 'EXPLICIT_ALERT_REFRESH')), ''), '-', '_'));
  v_active_json jsonb := '{}'::jsonb;
  v_alert_hash text := NULL::text;
  v_summary_hash text := NULL::text;
  v_unacknowledged_count integer := 0;
  v_highest_severity text := NULL::text;
  v_highest_label text := NULL::text;
BEGIN
  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'BANKING_ALERT_DISPLAY_SUMMARY_REFRESH_ACTOR_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'BANKING_ALERT_DISPLAY_SUMMARY_REFRESH_ACTOR_REQUIRED'
            )::text;
  END IF;

  IF v_context IN (
    'BATCH_LIST',
    'PAY_BATCH_LIST',
    'LIST',
    'BOOTSTRAP',
    'BATCH_BOOTSTRAP',
    'PAY_BATCH_BOOTSTRAP',
    'DISPLAY',
    'BATCH_OPEN',
    'MODAL_OPEN',
    'PAY_BATCH_GET',
    'PAY_BATCH_GET_BOOTSTRAP_ONLY',
    'SECTION_PAGE',
    'OVERVIEW',
    'OVERVIEW_PAGE',
    'OPERATION_PROGRESS',
    'PREVIEW_PROGRESS',
    'WATCH_SIGNAL',
    'LIVE_WATCH',
    'RPC_CHANGES_PING',
    'CHANGES_PING'
  ) THEN
    RAISE EXCEPTION 'BANKING_ALERT_DISPLAY_SUMMARY_REFRESH_CONTEXT_NOT_ALLOWED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'BANKING_ALERT_DISPLAY_SUMMARY_REFRESH_CONTEXT_NOT_ALLOWED',
              'context', v_context
            )::text;
  END IF;

  PERFORM public.banking_pay_hot_path_budget_apply(
    CASE WHEN v_context = 'ALERT_REFRESH_JOB' THEN 'ALERT_REFRESH_JOB' ELSE 'EXPLICIT_DIAGNOSTIC' END
  );

  v_active_json := public.banking_alerts_active_for_user(
    p_actor_user_id,
    NULL::text,
    NULL::uuid,
    false,
    100,
    v_context
  );

  v_alert_hash := COALESCE(
    NULLIF(BTRIM(COALESCE(v_active_json ->> 'banking_alert_hash', '')), ''),
    'banking_alert_signal:v3:' || MD5('')
  );

  v_summary_hash := COALESCE(
    NULLIF(BTRIM(COALESCE(v_active_json ->> 'banking_alert_summary_signature', '')), ''),
    'banking_alert_summary:v3:' || MD5('')
  );

  BEGIN
    v_unacknowledged_count := COALESCE((v_active_json ->> 'banking_unacknowledged_alert_count')::integer, 0);
  EXCEPTION
    WHEN OTHERS THEN
      v_unacknowledged_count := 0;
  END;

  v_highest_severity := NULLIF(BTRIM(COALESCE(v_active_json ->> 'banking_highest_alert_severity', v_active_json ->> 'highest_severity', '')), '');
  v_highest_label := NULLIF(BTRIM(COALESCE(v_active_json ->> 'banking_highest_alert_label', v_active_json ->> 'highest_label', '')), '');

  INSERT INTO public.banking_alert_display_summary (
    actor_user_id,
    alert_hash,
    summary_hash,
    unacknowledged_count,
    highest_severity,
    highest_label,
    summary_json,
    updated_at_utc
  )
  VALUES (
    p_actor_user_id,
    v_alert_hash,
    v_summary_hash,
    v_unacknowledged_count,
    v_highest_severity,
    v_highest_label,
    v_active_json,
    now()
  )
  ON CONFLICT (actor_user_id) DO UPDATE
  SET
    alert_hash = EXCLUDED.alert_hash,
    summary_hash = EXCLUDED.summary_hash,
    unacknowledged_count = EXCLUDED.unacknowledged_count,
    highest_severity = EXCLUDED.highest_severity,
    highest_label = EXCLUDED.highest_label,
    summary_json = EXCLUDED.summary_json,
    updated_at_utc = now();
END;
$function$;

-- banking_alert_display_summary_touch(uuid,text,text,integer,text,text,jsonb)
CREATE OR REPLACE FUNCTION public.banking_alert_display_summary_touch(p_actor_user_id uuid, p_alert_hash text DEFAULT NULL::text, p_summary_hash text DEFAULT NULL::text, p_unacknowledged_count integer DEFAULT NULL::integer, p_highest_severity text DEFAULT NULL::text, p_highest_label text DEFAULT NULL::text, p_summary_json jsonb DEFAULT NULL::jsonb)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
BEGIN
  INSERT INTO public.banking_alert_display_summary (
    actor_user_id,
    alert_hash,
    summary_hash,
    unacknowledged_count,
    highest_severity,
    highest_label,
    summary_json,
    updated_at_utc
  )
  VALUES (
    p_actor_user_id,
    p_alert_hash,
    p_summary_hash,
    COALESCE(p_unacknowledged_count, 0),
    p_highest_severity,
    p_highest_label,
    COALESCE(p_summary_json, '{}'::jsonb),
    now()
  )
  ON CONFLICT (actor_user_id) DO UPDATE
  SET
    alert_hash = COALESCE(EXCLUDED.alert_hash, public.banking_alert_display_summary.alert_hash),
    summary_hash = COALESCE(EXCLUDED.summary_hash, public.banking_alert_display_summary.summary_hash),
    unacknowledged_count = COALESCE(EXCLUDED.unacknowledged_count, public.banking_alert_display_summary.unacknowledged_count),
    highest_severity = COALESCE(EXCLUDED.highest_severity, public.banking_alert_display_summary.highest_severity),
    highest_label = COALESCE(EXCLUDED.highest_label, public.banking_alert_display_summary.highest_label),
    summary_json = COALESCE(EXCLUDED.summary_json, public.banking_alert_display_summary.summary_json),
    updated_at_utc = now();
END;
$function$;

-- banking_alert_fingerprint(text,text,uuid,jsonb)
CREATE OR REPLACE FUNCTION public.banking_alert_fingerprint(p_alert_kind text, p_entity_kind text, p_entity_id uuid, p_payload jsonb DEFAULT '{}'::jsonb)
 RETURNS text
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_alert_kind text;
  v_entity_kind text;
  v_payload jsonb;
  v_stable_key text := NULL::text;
  v_payload_stable_key text := NULL::text;
  v_payload_dedupe_key text := NULL::text;
  v_pay_batch_id text := NULL::text;
  v_issue_key text := NULL::text;
  v_provider_key text := NULL::text;
  v_rail_env text := NULL::text;
  v_retry_operation_id text := NULL::text;
  v_outage_window text := NULL::text;
  v_correction_request_id text := NULL::text;
  v_cancellation_operation_id text := NULL::text;
  v_transfer_id text := NULL::text;
  v_manual_blocker_key text := NULL::text;
  v_provider_event_key text := NULL::text;
  v_issue_window text := NULL::text;
  v_terminal_failure_group_key text := NULL::text;
BEGIN
  v_alert_kind := UPPER(NULLIF(BTRIM(COALESCE(p_alert_kind, '')), ''));
  v_entity_kind := LOWER(NULLIF(BTRIM(COALESCE(p_entity_kind, '')), ''));

  IF v_alert_kind IS NULL THEN
    RAISE EXCEPTION 'BANKING_ALERT_FINGERPRINT_KIND_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'BANKING_ALERT_FINGERPRINT_KIND_REQUIRED')::text;
  END IF;

  IF v_entity_kind IS NULL THEN
    RAISE EXCEPTION 'BANKING_ALERT_FINGERPRINT_ENTITY_KIND_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'BANKING_ALERT_FINGERPRINT_ENTITY_KIND_REQUIRED')::text;
  END IF;

  IF p_entity_id IS NULL THEN
    RAISE EXCEPTION 'BANKING_ALERT_FINGERPRINT_ENTITY_ID_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'BANKING_ALERT_FINGERPRINT_ENTITY_ID_REQUIRED')::text;
  END IF;

  IF p_payload IS NOT NULL AND COALESCE(jsonb_typeof(p_payload), 'null') <> 'object' THEN
    RAISE EXCEPTION 'BANKING_ALERT_FINGERPRINT_PAYLOAD_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'BANKING_ALERT_FINGERPRINT_PAYLOAD_MUST_BE_OBJECT',
              'alert_kind', v_alert_kind,
              'entity_kind', v_entity_kind,
              'entity_id', p_entity_id::text
            )::text;
  END IF;

  v_payload := COALESCE(p_payload, '{}'::jsonb);

  v_payload_stable_key := NULLIF(BTRIM(COALESCE(v_payload->>'stable_issue_key', '')), '');
  v_payload_dedupe_key := NULLIF(BTRIM(COALESCE(v_payload->>'dedupe_key', '')), '');

  v_pay_batch_id := COALESCE(
    NULLIF(BTRIM(v_payload->>'pay_batch_id'), ''),
    CASE WHEN v_entity_kind IN ('pay_batch', 'pay_batches') THEN p_entity_id::text ELSE NULL END
  );

  v_provider_key := COALESCE(
    NULLIF(BTRIM(v_payload->>'provider'), ''),
    NULLIF(BTRIM(v_payload->>'rail'), ''),
    NULLIF(BTRIM(v_payload->>'rail_provider'), ''),
    NULLIF(BTRIM(v_payload->>'provider_key'), ''),
    'UNKNOWN_PROVIDER'
  );

  v_rail_env := COALESCE(
    NULLIF(BTRIM(v_payload->>'rail_env'), ''),
    NULLIF(BTRIM(v_payload->>'provider_env'), ''),
    NULLIF(BTRIM(v_payload->>'environment'), ''),
    'UNKNOWN_ENV'
  );

  v_retry_operation_id := COALESCE(
    NULLIF(BTRIM(v_payload->>'retry_operation_id'), ''),
    NULLIF(BTRIM(v_payload->>'retry_operation_key'), '')
  );

  v_outage_window := COALESCE(
    NULLIF(BTRIM(v_payload->>'outage_window'), ''),
    NULLIF(BTRIM(v_payload->>'retry_window'), ''),
    NULLIF(BTRIM(v_payload->>'provider_outage_window'), '')
  );

  v_correction_request_id := NULLIF(BTRIM(COALESCE(v_payload->>'correction_request_id', '')), '');
  v_cancellation_operation_id := COALESCE(
    NULLIF(BTRIM(v_payload->>'cancellation_operation_id'), ''),
    NULLIF(BTRIM(v_payload->>'correction_request_id'), '')
  );
  v_transfer_id := NULLIF(BTRIM(COALESCE(v_payload->>'pay_bank_transfer_id', '')), '');
  v_manual_blocker_key := COALESCE(
    NULLIF(BTRIM(v_payload->>'carry_forward_id'), ''),
    NULLIF(BTRIM(v_payload->>'source_pay_batch_item_id'), '')
  );

  v_provider_event_key := COALESCE(
    NULLIF(BTRIM(v_payload->>'provider_event_key'), ''),
    NULLIF(BTRIM(v_payload->>'provider_webhook_event_key'), ''),
    NULLIF(BTRIM(v_payload->>'provider_transaction_id'), ''),
    NULLIF(BTRIM(v_payload->>'provider_request_id'), '')
  );

  v_issue_window := COALESCE(
    NULLIF(BTRIM(v_payload->>'issue_window'), ''),
    NULLIF(BTRIM(v_payload->>'outcome_unknown_window'), ''),
    NULLIF(BTRIM(v_payload->>'outage_window'), ''),
    NULLIF(BTRIM(v_payload->>'retry_window'), '')
  );

  v_terminal_failure_group_key := COALESCE(
    NULLIF(BTRIM(v_payload->>'terminal_failure_group_key'), ''),
    NULLIF(BTRIM(v_payload->>'provider_failure_reason_group'), ''),
    NULLIF(BTRIM(v_payload->>'failure_reason_group'), ''),
    NULLIF(BTRIM(v_payload->>'provider_event_key'), ''),
    NULLIF(BTRIM(v_payload->>'dedupe_key'), '')
  );

  v_issue_key := CASE
    WHEN v_alert_kind = 'PROVIDER_OUTAGE_RETRY_LATER' THEN CONCAT_WS(
      ':',
      COALESCE(v_pay_batch_id, p_entity_id::text),
      v_alert_kind,
      v_provider_key,
      v_rail_env,
      COALESCE(v_retry_operation_id, v_outage_window, v_issue_window, 'NO_WINDOW')
    )
    WHEN v_alert_kind = 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER' THEN CONCAT_WS(
      ':',
      COALESCE(v_pay_batch_id, p_entity_id::text),
      v_alert_kind,
      v_provider_key,
      COALESCE(v_issue_window, 'NO_WINDOW')
    )
    WHEN v_alert_kind = 'WEBHOOK_UNMATCHED_REVIEW_REQUIRED' THEN CONCAT_WS(
      ':',
      COALESCE(v_pay_batch_id, 'NO_BATCH'),
      v_alert_kind,
      v_provider_key,
      COALESCE(v_provider_event_key, 'NO_PROVIDER_EVENT_KEY')
    )
    WHEN v_alert_kind = 'AUTO_UNWIND_PROGRESS'
      AND v_correction_request_id IS NOT NULL THEN CONCAT_WS(
        ':',
        COALESCE(v_pay_batch_id, p_entity_id::text),
        v_alert_kind,
        v_correction_request_id
      )
    WHEN v_alert_kind = 'WHOLE_BATCH_CANCELLATION_PROGRESS'
      AND v_cancellation_operation_id IS NOT NULL THEN CONCAT_WS(
        ':',
        COALESCE(v_pay_batch_id, p_entity_id::text),
        v_alert_kind,
        v_cancellation_operation_id
      )
    WHEN v_alert_kind = 'TERMINAL_NO_MONEY_REWIND_AVAILABLE' THEN CONCAT_WS(
      ':',
      COALESCE(v_pay_batch_id, p_entity_id::text),
      v_alert_kind,
      COALESCE(v_correction_request_id, v_terminal_failure_group_key, v_transfer_id, 'NO_TERMINAL_FAILURE_KEY')
    )
    WHEN v_alert_kind IN (
      'PAID_SETTLED_RECOVERY_REQUIRED',
      'CANCELLATION_RACED_WITH_PROVIDER_SUBMIT'
    )
      AND COALESCE(v_transfer_id, v_correction_request_id, v_terminal_failure_group_key) IS NOT NULL THEN CONCAT_WS(
        ':',
        COALESCE(v_pay_batch_id, p_entity_id::text),
        v_alert_kind,
        COALESCE(v_transfer_id, v_correction_request_id, v_terminal_failure_group_key)
      )
    WHEN v_alert_kind = 'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS'
      AND v_manual_blocker_key IS NOT NULL THEN CONCAT_WS(
        ':',
        COALESCE(v_pay_batch_id, p_entity_id::text),
        v_alert_kind,
        v_manual_blocker_key
      )
    ELSE NULL::text
  END;

  v_stable_key := COALESCE(
    v_payload_stable_key,
    v_payload_dedupe_key,
    v_issue_key,
    CONCAT_WS(':', COALESCE(v_pay_batch_id, p_entity_id::text), v_alert_kind)
  );

  RETURN 'banking_alert:v2:' || MD5(CONCAT_WS(':', v_entity_kind, v_stable_key));
END;
$function$;

-- banking_alert_payload_for_pay_batch(text,uuid,text,uuid)
CREATE OR REPLACE FUNCTION public.banking_alert_payload_for_pay_batch(p_alert_kind text, p_pay_batch_id uuid, p_source_kind text DEFAULT NULL::text, p_source_id uuid DEFAULT NULL::uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_alert_kind text;
  v_source_kind text;
  v_batch record;
  v_funds_check_json jsonb := '{}'::jsonb;
  v_required_gbp_text text := NULL;
  v_available_gbp_text text := NULL;
  v_required_gbp numeric := NULL;
  v_available_gbp numeric := NULL;
  v_sufficient_text text := NULL;
  v_sufficient boolean := NULL;
  v_funding_account_ref text := NULL;
  v_rail_provider text := NULL;
  v_rail_env text := NULL;
  v_funds_check_checked_at_utc text := NULL;
  v_last_funds_check_at_utc_text text := NULL;
  v_execution_committed_at_utc_text text := NULL;
  v_last_status_checked_at_utc_text text := NULL;
  v_latest_transfer jsonb := NULL;
  v_latest_transfer_event jsonb := NULL;
  v_latest_correction_request jsonb := NULL;
  v_latest_correction_work_item jsonb := NULL;
  v_latest_remittance_failure jsonb := NULL;
  v_provider_submit_diagnostic_result jsonb := '{}'::jsonb;
  v_provider_submit_diagnostic jsonb := '{}'::jsonb;
  v_provider_submission_status text := NULL;
  v_review_reason_code text := NULL;
  v_provider_operation_id uuid := NULL::uuid;
  v_provider_title text := NULL;
  v_provider_description text := NULL;
  v_provider_action_label text := NULL;
  v_grouped_alert_payload jsonb := '{}'::jsonb;
  v_provider_failure_reason_code text := NULL::text;
  v_provider_failure_reason_group text := NULL::text;
  v_provider_failure_reason_label text := NULL::text;
  v_provider_event_key text := NULL::text;
  v_webhook_receipt_id uuid := NULL::uuid;
BEGIN
  v_alert_kind := UPPER(NULLIF(BTRIM(COALESCE(p_alert_kind, '')), ''));
  v_source_kind := lower(nullif(btrim(coalesce(p_source_kind, '')), ''));

  IF v_alert_kind IS NULL THEN
    RAISE EXCEPTION 'BANKING_ALERT_PAYLOAD_KIND_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PAYLOAD_KIND_REQUIRED')::text;
  END IF;

  IF p_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'BANKING_ALERT_PAYLOAD_PAY_BATCH_ID_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PAYLOAD_PAY_BATCH_ID_REQUIRED')::text;
  END IF;

  v_alert_kind := CASE v_alert_kind
    WHEN 'BLOCKED_FUNDS' THEN 'PROVIDER_OUTAGE_RETRY_LATER'
    WHEN 'BANK_REJECTED_PAYMENT' THEN 'TERMINAL_NO_MONEY_REWIND_AVAILABLE'
    WHEN 'BANK_RETURNED_PAYMENT' THEN 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER'
    WHEN 'RAIL_SUBMISSION_UNKNOWN_OR_TIMEOUT' THEN 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER'
    WHEN 'AMBIGUOUS_PAYMENT_REVIEW_REQUIRED' THEN 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER'
    WHEN 'PAYMENT_PROVIDER_SUBMIT_REVIEW' THEN 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER'
    WHEN 'PAYMENT_CORRECTION_FAILED' THEN 'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS'
    WHEN 'PAYMENT_CORRECTION_BLOCKED' THEN 'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS'
    WHEN 'PAYMENT_CORRECTION_AWAITING_APPROVAL' THEN 'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS'
    ELSE v_alert_kind
  END;

  SELECT
    public.pay_batches.id,
    public.pay_batches.status,
    public.pay_batches.pay_date,
    public.pay_batches.bulk_reference,
    public.pay_batches.rail_provider_snapshot,
    public.pay_batches.rail_env_snapshot,
    public.pay_batches.funding_account_ref,
    public.pay_batches.last_funds_check_at_utc,
    public.pay_batches.last_funds_check_json,
    public.pay_batches.execution_commit_state,
    public.pay_batches.execution_commit_ref,
    public.pay_batches.execution_committed_at_utc,
    public.pay_batches.last_status_checked_at_utc,
    public.pay_batches.cancelled_at_utc,
    public.pay_batches.completed_at_utc
  INTO v_batch
  FROM public.pay_batches
  WHERE public.pay_batches.id = p_pay_batch_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'BANKING_ALERT_PAYLOAD_PAY_BATCH_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'BANKING_ALERT_PAYLOAD_PAY_BATCH_NOT_FOUND',
              'pay_batch_id', p_pay_batch_id::text
            )::text;
  END IF;

  v_funds_check_json := COALESCE(v_batch.last_funds_check_json, '{}'::jsonb);

  v_required_gbp_text := COALESCE(
    NULLIF(BTRIM(v_funds_check_json #>> '{required_gbp}'), ''),
    NULLIF(BTRIM(v_funds_check_json #>> '{required}'), ''),
    NULLIF(BTRIM(v_funds_check_json #>> '{required_amount_gbp}'), ''),
    NULLIF(BTRIM(v_funds_check_json #>> '{required_amount}'), '')
  );

  IF v_required_gbp_text IS NOT NULL AND v_required_gbp_text ~ '^-?[0-9]+(\.[0-9]+)?$' THEN
    v_required_gbp := v_required_gbp_text::numeric;
  END IF;

  v_available_gbp_text := COALESCE(
    NULLIF(BTRIM(v_funds_check_json #>> '{available_gbp}'), ''),
    NULLIF(BTRIM(v_funds_check_json #>> '{available}'), ''),
    NULLIF(BTRIM(v_funds_check_json #>> '{available_amount_gbp}'), ''),
    NULLIF(BTRIM(v_funds_check_json #>> '{available_amount}'), '')
  );

  IF v_available_gbp_text IS NOT NULL AND v_available_gbp_text ~ '^-?[0-9]+(\.[0-9]+)?$' THEN
    v_available_gbp := v_available_gbp_text::numeric;
  END IF;

  v_sufficient_text := COALESCE(
    NULLIF(BTRIM(v_funds_check_json #>> '{sufficient}'), ''),
    NULLIF(BTRIM(v_funds_check_json #>> '{is_sufficient}'), ''),
    NULLIF(BTRIM(v_funds_check_json #>> '{funds_sufficient}'), '')
  );

  IF v_sufficient_text IS NOT NULL THEN
    IF LOWER(v_sufficient_text) IN ('true', 't', '1', 'yes', 'y', 'on') THEN
      v_sufficient := true;
    ELSIF LOWER(v_sufficient_text) IN ('false', 'f', '0', 'no', 'n', 'off') THEN
      v_sufficient := false;
    END IF;
  END IF;

  v_funding_account_ref := COALESCE(
    NULLIF(BTRIM(v_funds_check_json #>> '{funding_account_ref}'), ''),
    NULLIF(BTRIM(v_funds_check_json #>> '{account_ref}'), ''),
    NULLIF(BTRIM(v_funds_check_json #>> '{funding_account_id}'), ''),
    NULLIF(BTRIM(v_batch.funding_account_ref), '')
  );

  v_rail_provider := COALESCE(
    NULLIF(BTRIM(v_funds_check_json #>> '{rail_provider}'), ''),
    NULLIF(BTRIM(v_funds_check_json #>> '{provider}'), ''),
    NULLIF(BTRIM(v_batch.rail_provider_snapshot), '')
  );

  v_rail_env := COALESCE(
    NULLIF(BTRIM(v_funds_check_json #>> '{rail_env}'), ''),
    NULLIF(BTRIM(v_funds_check_json #>> '{env}'), ''),
    NULLIF(BTRIM(v_batch.rail_env_snapshot), '')
  );

  v_last_funds_check_at_utc_text := CASE
    WHEN v_batch.last_funds_check_at_utc IS NULL THEN NULL
    ELSE TO_CHAR(v_batch.last_funds_check_at_utc AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
  END;

  v_execution_committed_at_utc_text := CASE
    WHEN v_batch.execution_committed_at_utc IS NULL THEN NULL
    ELSE TO_CHAR(v_batch.execution_committed_at_utc AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
  END;

  v_last_status_checked_at_utc_text := CASE
    WHEN v_batch.last_status_checked_at_utc IS NULL THEN NULL
    ELSE TO_CHAR(v_batch.last_status_checked_at_utc AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
  END;

  v_funds_check_checked_at_utc := COALESCE(
    NULLIF(BTRIM(v_funds_check_json #>> '{checked_at_utc}'), ''),
    NULLIF(BTRIM(v_funds_check_json #>> '{checked_at}'), ''),
    NULLIF(BTRIM(v_funds_check_json #>> '{created_at_utc}'), ''),
    NULLIF(BTRIM(v_funds_check_json #>> '{timestamp_utc}'), ''),
    v_last_funds_check_at_utc_text
  );

  SELECT
    NULLIF(BTRIM(COALESCE(provider_failure_event.provider_failure_reason_code, '')), ''),
    NULLIF(BTRIM(COALESCE(provider_failure_event.provider_failure_reason_group, '')), ''),
    NULLIF(BTRIM(COALESCE(provider_failure_event.provider_failure_reason_group, '')), ''),
    NULLIF(BTRIM(COALESCE(provider_failure_event.provider_event_key, '')), ''),
    provider_failure_event.provider_webhook_receipt_id
  INTO
    v_provider_failure_reason_code,
    v_provider_failure_reason_group,
    v_provider_failure_reason_label,
    v_provider_event_key,
    v_webhook_receipt_id
  FROM public.pay_bank_transfer_events AS provider_failure_event
  WHERE provider_failure_event.pay_batch_id = p_pay_batch_id
    AND (
      v_source_kind IS DISTINCT FROM 'pay_bank_transfer_event'
      OR p_source_id IS NULL
      OR provider_failure_event.id = p_source_id
    )
    AND (
      NULLIF(BTRIM(COALESCE(provider_failure_event.provider_failure_reason_group, '')), '') IS NOT NULL
      OR NULLIF(BTRIM(COALESCE(provider_failure_event.provider_event_key, '')), '') IS NOT NULL
      OR provider_failure_event.provider_webhook_receipt_id IS NOT NULL
      OR UPPER(BTRIM(COALESCE(provider_failure_event.mapping_status, ''))) IN ('UNMATCHED','NO_MATCH','MULTIPLE_MATCHES')
    )
  ORDER BY COALESCE(provider_failure_event.event_time_utc, provider_failure_event.received_at_utc, provider_failure_event.created_at_utc) DESC NULLS LAST,
           provider_failure_event.id DESC
  LIMIT 1;

  IF v_provider_failure_reason_group IS NOT NULL THEN
    v_provider_failure_reason_label := initcap(replace(lower(v_provider_failure_reason_group), '_', ' '));
  END IF;

  IF v_alert_kind IN (
    'PROVIDER_OUTAGE_RETRY_LATER',
    'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER',
    'TERMINAL_NO_MONEY_REWIND_AVAILABLE',
    'AUTO_UNWIND_PROGRESS',
    'WHOLE_BATCH_CANCELLATION_PROGRESS',
    'MANUAL_ADJUSTMENTS_CARRIED_FORWARD',
    'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS',
    'PAID_SETTLED_RECOVERY_REQUIRED',
    'CANCELLATION_RACED_WITH_PROVIDER_SUBMIT',
    'WEBHOOK_UNMATCHED_REVIEW_REQUIRED'
  ) THEN
    WITH retry_plan_scope AS (
      SELECT COALESCE(public.pay_payment_correction_plan(
        p_pay_batch_id,
        jsonb_build_object(
          'scope_type', 'BATCH',
          'source_context', 'BANKING_ALERT_PAYLOAD',
          'requested_action', 'RETRY_PROVIDER_LATER'
        ),
        NULL::uuid
      ), '{}'::jsonb) AS retry_plan_json
    ),
    retry_operation_scope AS (
      SELECT
        retry_operation.id AS retry_operation_id,
        retry_operation.status AS retry_operation_status
      FROM public.banking_pay_operations AS retry_operation
      WHERE retry_operation.pay_batch_id = p_pay_batch_id
        AND UPPER(BTRIM(COALESCE(retry_operation.operation_type, ''))) = 'PAYMENT_RETRY_BLOCKED_FUNDS'
        AND UPPER(BTRIM(COALESCE(retry_operation.status, ''))) NOT IN ('COMPLETE','COMPLETED','SUCCEEDED','SUCCESS','FAILED','FAILED_FINAL','CANCELLED','CANCELED')
      ORDER BY COALESCE(retry_operation.updated_at_utc, retry_operation.created_at_utc) DESC NULLS LAST,
               retry_operation.created_at_utc DESC NULLS LAST,
               retry_operation.id DESC
      LIMIT 1
    ),
    retry_plan_counts AS (
      SELECT
        CASE
          WHEN COALESCE(retry_plan_scope.retry_plan_json->>'retry_eligible_count', '') ~ '^\d+$' THEN (retry_plan_scope.retry_plan_json->>'retry_eligible_count')::integer
          ELSE 0
        END AS retry_eligible_count,
        CASE
          WHEN COALESCE(retry_plan_scope.retry_plan_json->>'retry_ineligible_count', '') ~ '^\d+$' THEN (retry_plan_scope.retry_plan_json->>'retry_ineligible_count')::integer
          ELSE 0
        END AS retry_ineligible_count
      FROM retry_plan_scope
    ),
    item_counts AS (
      SELECT
        COUNT(DISTINCT pbi.id)::integer AS affected_payment_count,
        COUNT(DISTINCT pbc.candidate_id)::integer AS affected_candidate_count,
        COUNT(DISTINCT pbi.pay_bank_transfer_id) FILTER (WHERE pbi.pay_bank_transfer_id IS NOT NULL)::integer AS affected_transfer_count,
        COUNT(DISTINCT pbi.timesheet_id) FILTER (WHERE pbi.timesheet_id IS NOT NULL)::integer AS affected_timesheet_count,
        ROUND(COALESCE(SUM(COALESCE(pbi.amount_inc_vat, pbi.amount_ex_vat, 0)), 0), 2)::numeric AS amount_affected
      FROM public.pay_batch_candidates AS pbc
      JOIN public.pay_batch_items AS pbi
        ON pbi.pay_batch_candidate_id = pbc.id
      WHERE pbc.pay_batch_id = p_pay_batch_id
        AND COALESCE(pbi.is_voided, false) = false
        AND pbi.item_type <> 'DEBT_CREATED'
    ),
    correction_scope AS (
      SELECT
        (array_agg(ppcr.id ORDER BY ppcr.updated_at_utc DESC NULLS LAST, ppcr.created_at_utc DESC, ppcr.id DESC))[1] AS correction_request_id,
        COUNT(ppwi.id) FILTER (WHERE ppwi.status IN ('APPLIED','DONE','COMPLETE','COMPLETED'))::integer AS progress_completed,
        COUNT(ppwi.id)::integer AS progress_total,
        bool_or(ppcr.status IN ('AUTHORISED','EXPANDED','PROCESSING')) AS correction_running
      FROM public.pay_payment_correction_requests AS ppcr
      LEFT JOIN public.pay_payment_correction_work_items AS ppwi
        ON ppwi.correction_request_id = ppcr.id
      WHERE ppcr.pay_batch_id = p_pay_batch_id
        AND (
          (v_alert_kind = 'AUTO_UNWIND_PROGRESS' AND ppcr.correction_kind IN ('NO_MONEY_UNWIND','MANUAL_EVIDENCE_NO_MONEY'))
          OR (v_alert_kind = 'WHOLE_BATCH_CANCELLATION_PROGRESS' AND ppcr.correction_kind = 'PRE_BANK_CANCEL')
          OR v_alert_kind NOT IN ('AUTO_UNWIND_PROGRESS','WHOLE_BATCH_CANCELLATION_PROGRESS')
        )
    ),
    carry_forward_scope AS (
      SELECT
        COUNT(*)::integer AS carry_forward_count,
        (array_agg(cf.id ORDER BY cf.id DESC))[1] AS carry_forward_id,
        (array_agg(cf.source_pay_batch_item_id ORDER BY cf.id DESC))[1] AS source_pay_batch_item_id
      FROM public.pay_manual_adjustment_carry_forwards AS cf
      WHERE cf.source_pay_batch_id = p_pay_batch_id
         OR cf.target_pay_batch_id = p_pay_batch_id
    ),
    single_transfer_scope AS (
      SELECT pbt.id AS pay_bank_transfer_id
      FROM public.pay_bank_transfers AS pbt
      WHERE pbt.pay_batch_id = p_pay_batch_id
        AND v_source_kind = 'pay_bank_transfer'
        AND pbt.id = p_source_id
      LIMIT 1
    )
    SELECT jsonb_strip_nulls(
      jsonb_build_object(
        'stable_issue_key', CASE
          WHEN v_alert_kind = 'PROVIDER_OUTAGE_RETRY_LATER' THEN p_pay_batch_id::text || ':' || v_alert_kind || ':' || COALESCE(v_rail_provider, 'UNKNOWN') || ':' || COALESCE(v_rail_env, 'UNKNOWN') || ':' || COALESCE(v_last_status_checked_at_utc_text, v_last_funds_check_at_utc_text, v_execution_committed_at_utc_text, (SELECT ros.retry_operation_id::text FROM retry_operation_scope AS ros), 'NO_WINDOW')
          WHEN v_alert_kind = 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER' THEN p_pay_batch_id::text || ':' || v_alert_kind || ':' || COALESCE(v_rail_provider, 'UNKNOWN') || ':' || COALESCE(v_rail_env, 'UNKNOWN') || ':' || COALESCE(v_last_status_checked_at_utc_text, v_execution_committed_at_utc_text, 'NO_WINDOW')
          WHEN v_alert_kind = 'WEBHOOK_UNMATCHED_REVIEW_REQUIRED' THEN COALESCE(p_pay_batch_id::text, 'NO_BATCH') || ':' || v_alert_kind || ':' || COALESCE(v_rail_provider, 'UNKNOWN') || ':' || COALESCE(v_rail_env, 'UNKNOWN') || ':' || COALESCE(v_provider_event_key, 'NO_PROVIDER_EVENT_KEY')
          WHEN v_alert_kind = 'TERMINAL_NO_MONEY_REWIND_AVAILABLE' THEN p_pay_batch_id::text || ':' || v_alert_kind || ':' || COALESCE((SELECT cs.correction_request_id::text FROM correction_scope AS cs), v_provider_failure_reason_group, v_provider_event_key, 'NO_TERMINAL_FAILURE_KEY')
          WHEN v_alert_kind = 'AUTO_UNWIND_PROGRESS' THEN p_pay_batch_id::text || ':' || v_alert_kind || ':' || COALESCE((SELECT cs.correction_request_id::text FROM correction_scope AS cs), 'NO_CORRECTION_REQUEST')
          WHEN v_alert_kind = 'WHOLE_BATCH_CANCELLATION_PROGRESS' THEN p_pay_batch_id::text || ':' || v_alert_kind || ':' || COALESCE((SELECT cs.correction_request_id::text FROM correction_scope AS cs), 'NO_CORRECTION_REQUEST')
          WHEN v_alert_kind = 'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS' THEN p_pay_batch_id::text || ':' || v_alert_kind || ':' || COALESCE((SELECT cfs.carry_forward_id::text FROM carry_forward_scope AS cfs), (SELECT cfs.source_pay_batch_item_id::text FROM carry_forward_scope AS cfs), 'NO_BLOCKER_KEY')
          WHEN v_alert_kind IN ('PAID_SETTLED_RECOVERY_REQUIRED','CANCELLATION_RACED_WITH_PROVIDER_SUBMIT') THEN p_pay_batch_id::text || ':' || v_alert_kind || ':' || COALESCE((SELECT sts.pay_bank_transfer_id::text FROM single_transfer_scope AS sts), (SELECT cs.correction_request_id::text FROM correction_scope AS cs), v_provider_event_key, 'NO_SCOPE_KEY')
          ELSE p_pay_batch_id::text || ':' || v_alert_kind
        END,
        'dedupe_key', CASE
          WHEN v_alert_kind = 'PROVIDER_OUTAGE_RETRY_LATER' THEN p_pay_batch_id::text || ':' || v_alert_kind || ':' || COALESCE(v_rail_provider, 'UNKNOWN') || ':' || COALESCE(v_rail_env, 'UNKNOWN') || ':' || COALESCE(v_last_status_checked_at_utc_text, v_last_funds_check_at_utc_text, v_execution_committed_at_utc_text, (SELECT ros.retry_operation_id::text FROM retry_operation_scope AS ros), 'NO_WINDOW')
          WHEN v_alert_kind = 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER' THEN p_pay_batch_id::text || ':' || v_alert_kind || ':' || COALESCE(v_rail_provider, 'UNKNOWN') || ':' || COALESCE(v_rail_env, 'UNKNOWN') || ':' || COALESCE(v_last_status_checked_at_utc_text, v_execution_committed_at_utc_text, 'NO_WINDOW')
          WHEN v_alert_kind = 'WEBHOOK_UNMATCHED_REVIEW_REQUIRED' THEN COALESCE(p_pay_batch_id::text, 'NO_BATCH') || ':' || v_alert_kind || ':' || COALESCE(v_rail_provider, 'UNKNOWN') || ':' || COALESCE(v_rail_env, 'UNKNOWN') || ':' || COALESCE(v_provider_event_key, 'NO_PROVIDER_EVENT_KEY')
          WHEN v_alert_kind = 'TERMINAL_NO_MONEY_REWIND_AVAILABLE' THEN p_pay_batch_id::text || ':' || v_alert_kind || ':' || COALESCE((SELECT cs.correction_request_id::text FROM correction_scope AS cs), v_provider_failure_reason_group, v_provider_event_key, 'NO_TERMINAL_FAILURE_KEY')
          WHEN (SELECT cs.correction_request_id FROM correction_scope AS cs) IS NOT NULL THEN p_pay_batch_id::text || ':' || v_alert_kind || ':' || (SELECT cs.correction_request_id::text FROM correction_scope AS cs)
          WHEN (SELECT sts.pay_bank_transfer_id FROM single_transfer_scope AS sts) IS NOT NULL THEN p_pay_batch_id::text || ':' || v_alert_kind || ':' || (SELECT sts.pay_bank_transfer_id::text FROM single_transfer_scope AS sts)
          WHEN (SELECT cfs.carry_forward_id FROM carry_forward_scope AS cfs) IS NOT NULL AND v_alert_kind = 'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS' THEN p_pay_batch_id::text || ':' || v_alert_kind || ':' || (SELECT cfs.carry_forward_id::text FROM carry_forward_scope AS cfs)
          ELSE p_pay_batch_id::text || ':' || v_alert_kind
        END,
        'alert_kind', v_alert_kind,
        'issue_kind', v_alert_kind,
        'alert_severity', CASE
          WHEN v_alert_kind IN ('AUTO_UNWIND_PROGRESS','WHOLE_BATCH_CANCELLATION_PROGRESS') THEN 'PROGRESS'
          WHEN v_alert_kind = 'MANUAL_ADJUSTMENTS_CARRIED_FORWARD' THEN 'INFO'
          ELSE 'ACTION_REQUIRED'
        END,
        'provider_failure_reason_code', v_provider_failure_reason_code,
        'provider_failure_reason_group', CASE WHEN v_alert_kind = 'WEBHOOK_UNMATCHED_REVIEW_REQUIRED' THEN COALESCE(v_provider_failure_reason_group, 'WEBHOOK_UNMATCHED') ELSE v_provider_failure_reason_group END,
        'provider_failure_reason_label', CASE WHEN v_alert_kind = 'WEBHOOK_UNMATCHED_REVIEW_REQUIRED' THEN COALESCE(v_provider_failure_reason_label, 'Webhook unmatched') ELSE v_provider_failure_reason_label END,
        'provider_event_key', v_provider_event_key,
        'provider_webhook_receipt_id', CASE WHEN v_webhook_receipt_id IS NULL THEN NULL ELSE v_webhook_receipt_id::text END,
        'pay_batch_id', p_pay_batch_id::text,
        'provider', v_rail_provider,
        'rail', v_rail_provider,
        'rail_env', v_rail_env,
        'outage_window', CASE WHEN v_alert_kind = 'PROVIDER_OUTAGE_RETRY_LATER' THEN COALESCE(v_last_status_checked_at_utc_text, v_last_funds_check_at_utc_text, v_execution_committed_at_utc_text) ELSE NULL END
      )
      || jsonb_build_object(
        'retry_operation_id', CASE WHEN (SELECT ros.retry_operation_id FROM retry_operation_scope AS ros) IS NULL THEN NULL ELSE (SELECT ros.retry_operation_id::text FROM retry_operation_scope AS ros) END,
        'retry_operation_status', (SELECT ros.retry_operation_status FROM retry_operation_scope AS ros),
        'retry_eligible_count', COALESCE((SELECT rpc.retry_eligible_count FROM retry_plan_counts AS rpc), 0),
        'retry_ineligible_count', COALESCE((SELECT rpc.retry_ineligible_count FROM retry_plan_counts AS rpc), 0),
        'correction_request_id', CASE WHEN (SELECT cs.correction_request_id FROM correction_scope AS cs) IS NULL THEN NULL ELSE (SELECT cs.correction_request_id::text FROM correction_scope AS cs) END,
        'cancellation_operation_id', CASE WHEN v_alert_kind = 'WHOLE_BATCH_CANCELLATION_PROGRESS' THEN CASE WHEN (SELECT cs.correction_request_id FROM correction_scope AS cs) IS NULL THEN NULL ELSE (SELECT cs.correction_request_id::text FROM correction_scope AS cs) END ELSE NULL END,
        'pay_bank_transfer_id', CASE WHEN (SELECT sts.pay_bank_transfer_id FROM single_transfer_scope AS sts) IS NULL THEN NULL ELSE (SELECT sts.pay_bank_transfer_id::text FROM single_transfer_scope AS sts) END,
        'carry_forward_id', CASE WHEN (SELECT cfs.carry_forward_id FROM carry_forward_scope AS cfs) IS NULL THEN NULL ELSE (SELECT cfs.carry_forward_id::text FROM carry_forward_scope AS cfs) END,
        'source_pay_batch_item_id', CASE WHEN (SELECT cfs.source_pay_batch_item_id FROM carry_forward_scope AS cfs) IS NULL THEN NULL ELSE (SELECT cfs.source_pay_batch_item_id::text FROM carry_forward_scope AS cfs) END,
        'affected_payment_count', COALESCE((SELECT ic.affected_payment_count FROM item_counts AS ic), 0),
        'affected_candidate_count', COALESCE((SELECT ic.affected_candidate_count FROM item_counts AS ic), 0),
        'affected_transfer_count', COALESCE((SELECT ic.affected_transfer_count FROM item_counts AS ic), 0),
        'affected_timesheet_count', COALESCE((SELECT ic.affected_timesheet_count FROM item_counts AS ic), 0),
        'amount_affected', COALESCE((SELECT ic.amount_affected FROM item_counts AS ic), 0),
        'current_status', UPPER(BTRIM(COALESCE(v_batch.status, ''))),
        'required_user_action', CASE v_alert_kind
          WHEN 'PROVIDER_OUTAGE_RETRY_LATER' THEN 'Retry unsent payments from Banking Pay Overview.'
          WHEN 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER' THEN 'Open Current Payment Status and check the provider outcome.'
          WHEN 'TERMINAL_NO_MONEY_REWIND_AVAILABLE' THEN 'Open Current Payment Status and rewind financials where no money moved.'
          WHEN 'AUTO_UNWIND_PROGRESS' THEN 'Monitor rewind progress.'
          WHEN 'WHOLE_BATCH_CANCELLATION_PROGRESS' THEN 'Monitor cancellation progress in Banking Pay Overview.'
          WHEN 'MANUAL_ADJUSTMENTS_CARRIED_FORWARD' THEN 'Review carried-forward manual adjustments in the next pay run.'
          WHEN 'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS' THEN 'Open Current Payment Status and review ambiguous manual adjustment blockers.'
          WHEN 'PAID_SETTLED_RECOVERY_REQUIRED' THEN 'Open Current Payment Status and recover overpayment in next pay run.'
          WHEN 'CANCELLATION_RACED_WITH_PROVIDER_SUBMIT' THEN 'Open Current Payment Status and check provider submission before continuing.'
          WHEN 'WEBHOOK_UNMATCHED_REVIEW_REQUIRED' THEN 'Review the unmatched provider webhook and link it to the correct payment if safe.'
          ELSE 'Open Banking Pay.'
        END,
        'auto_unwind_running', v_alert_kind = 'AUTO_UNWIND_PROGRESS' AND COALESCE((SELECT cs.correction_running FROM correction_scope AS cs), false),
        'manual_rewind_required', v_alert_kind = 'TERMINAL_NO_MONEY_REWIND_AVAILABLE',
        'progress_completed', COALESCE((SELECT cs.progress_completed FROM correction_scope AS cs), 0),
        'progress_total', COALESCE((SELECT cs.progress_total FROM correction_scope AS cs), 0)
      )
      || jsonb_build_object(
        'link_target', 'banking_pay_batch',
        'link_tab', CASE
          WHEN v_alert_kind IN ('PROVIDER_OUTAGE_RETRY_LATER','AUTO_UNWIND_PROGRESS','WHOLE_BATCH_CANCELLATION_PROGRESS') THEN 'overview'
          WHEN v_alert_kind = 'WEBHOOK_UNMATCHED_REVIEW_REQUIRED' THEN 'current_payment_status'
          ELSE 'current_payment_status'
        END,
        'user_label', CASE v_alert_kind
          WHEN 'PROVIDER_OUTAGE_RETRY_LATER' THEN CASE WHEN (SELECT ros.retry_operation_id FROM retry_operation_scope AS ros) IS NOT NULL THEN 'Retrying unsent payments — ' || COALESCE(NULLIF((SELECT rpc.retry_eligible_count FROM retry_plan_counts AS rpc), 0), COALESCE((SELECT ic.affected_payment_count FROM item_counts AS ic), 0))::text || ' payments in progress' ELSE 'Bank unavailable — ' || COALESCE(NULLIF((SELECT rpc.retry_eligible_count FROM retry_plan_counts AS rpc), 0), COALESCE((SELECT ic.affected_payment_count FROM item_counts AS ic), 0))::text || ' unsent payments can be retried' END
          WHEN 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER' THEN 'Provider outcome unknown — ' || COALESCE((SELECT ic.affected_payment_count FROM item_counts AS ic), 0)::text || ' payments need checking'
          WHEN 'TERMINAL_NO_MONEY_REWIND_AVAILABLE' THEN 'Failed payments — Rewind financials available for ' || COALESCE((SELECT ic.affected_payment_count FROM item_counts AS ic), 0)::text || ' payments'
          WHEN 'AUTO_UNWIND_PROGRESS' THEN 'Rewinding failed payments — ' || COALESCE((SELECT cs.progress_completed FROM correction_scope AS cs), 0)::text || ' of ' || COALESCE(NULLIF((SELECT cs.progress_total FROM correction_scope AS cs), 0), COALESCE((SELECT ic.affected_payment_count FROM item_counts AS ic), 0))::text || ' complete'
          WHEN 'WHOLE_BATCH_CANCELLATION_PROGRESS' THEN 'Cancelling scheduled batch — ' || COALESCE((SELECT cs.progress_completed FROM correction_scope AS cs), 0)::text || ' of ' || COALESCE(NULLIF((SELECT cs.progress_total FROM correction_scope AS cs), 0), COALESCE((SELECT ic.affected_payment_count FROM item_counts AS ic), 0))::text || ' payment scopes complete'
          WHEN 'MANUAL_ADJUSTMENTS_CARRIED_FORWARD' THEN 'Manual adjustments carried forward — ' || COALESCE((SELECT cfs.carry_forward_count FROM carry_forward_scope AS cfs), 0)::text || ' adjustments will appear in the next pay run'
          WHEN 'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS' THEN 'Manual adjustment blockers — review required'
          WHEN 'PAID_SETTLED_RECOVERY_REQUIRED' THEN 'Paid — recovery required'
          WHEN 'CANCELLATION_RACED_WITH_PROVIDER_SUBMIT' THEN 'Cancellation conflict — provider submission already started'
          WHEN 'WEBHOOK_UNMATCHED_REVIEW_REQUIRED' THEN 'Unmatched bank webhook — review required'
          ELSE initcap(replace(lower(v_alert_kind), '_', ' '))
        END,
        'user_description', CASE v_alert_kind
          WHEN 'PROVIDER_OUTAGE_RETRY_LATER' THEN CASE WHEN (SELECT ros.retry_operation_id FROM retry_operation_scope AS ros) IS NOT NULL THEN 'CloudTMS is retrying payments confirmed as not sent. You can continue using CloudTMS while this runs.' ELSE 'The bank/provider was unavailable before the payment request was sent. Retry unsent payments from Banking Pay Overview.' END
          WHEN 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER' THEN 'A provider request may have been sent, but the outcome is not confirmed. Check provider state before retrying or unwinding.'
          WHEN 'TERMINAL_NO_MONEY_REWIND_AVAILABLE' THEN 'The provider/bank outcome indicates no money moved. Financials can be rewound from Current Payment Status.'
          WHEN 'AUTO_UNWIND_PROGRESS' THEN 'Automatic no-money unwind is running and this alert updates progress instead of creating per-payment alerts.'
          WHEN 'WHOLE_BATCH_CANCELLATION_PROGRESS' THEN 'Scheduled local cancellation is running in chunks and this alert updates progress.'
          WHEN 'MANUAL_ADJUSTMENTS_CARRIED_FORWARD' THEN 'Safe source-less manual adjustments were carried forward and will be included in the next pay run.'
          WHEN 'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS' THEN 'One or more manual adjustments cannot be safely carried forward automatically.'
          WHEN 'PAID_SETTLED_RECOVERY_REQUIRED' THEN 'Money appears to have moved. Amend the timesheet and recover the overpayment in the next pay run rather than unwinding the payment.'
          WHEN 'CANCELLATION_RACED_WITH_PROVIDER_SUBMIT' THEN 'Cancellation could not proceed because provider submission had already started.'
          WHEN 'WEBHOOK_UNMATCHED_REVIEW_REQUIRED' THEN 'A verified provider webhook was received but could not be matched safely to a payment.'
          ELSE 'Open Banking Pay for details.'
        END,
        'manual_adjustments_carried_forward_count', COALESCE((SELECT cfs.carry_forward_count FROM carry_forward_scope AS cfs), 0),
        'carry_forward_count', COALESCE((SELECT cfs.carry_forward_count FROM carry_forward_scope AS cfs), 0),
        'remittance_already_sent', EXISTS (
          SELECT 1
          FROM public.pay_batch_candidates AS remittance_candidate
          WHERE remittance_candidate.pay_batch_id = p_pay_batch_id
            AND remittance_candidate.remittance_sent_at_utc IS NOT NULL
        ),
        'payload_is_grouped', true
      )
    )
    INTO v_grouped_alert_payload;

    RETURN v_grouped_alert_payload;
  END IF;

  IF v_alert_kind = 'BLOCKED_FUNDS' THEN
    RETURN jsonb_strip_nulls(jsonb_build_object(
      'pay_batch_id', v_batch.id::text,
      'source_kind', v_source_kind,
      'source_id', CASE WHEN p_source_id IS NULL THEN NULL ELSE p_source_id::text END,
      'issue_kind', 'BLOCKED_FUNDS',
      'batch_status', UPPER(BTRIM(COALESCE(v_batch.status, ''))),
      'execution_commit_state', UPPER(BTRIM(COALESCE(v_batch.execution_commit_state, 'NOT_SUBMITTED'))),
      'execution_commit_ref', NULLIF(BTRIM(COALESCE(v_batch.execution_commit_ref, '')), ''),
      'execution_committed_at_utc', v_execution_committed_at_utc_text,
      'last_funds_check_at_utc', v_last_funds_check_at_utc_text,
      'funds_check_checked_at_utc', v_funds_check_checked_at_utc,
      'required_gbp', v_required_gbp,
      'available_gbp', v_available_gbp,
      'sufficient', v_sufficient,
      'funding_account_ref', v_funding_account_ref,
      'rail_provider', v_rail_provider,
      'rail_env', v_rail_env
    ));
  END IF;

  IF v_alert_kind IN ('BANK_REJECTED_PAYMENT', 'BANK_RETURNED_PAYMENT', 'AMBIGUOUS_PAYMENT_REVIEW_REQUIRED', 'RAIL_SUBMISSION_UNKNOWN_OR_TIMEOUT') THEN
    SELECT jsonb_strip_nulls(jsonb_build_object(
      'pay_bank_transfer_id', public.pay_bank_transfers.id::text,
      'candidate_id', CASE WHEN public.pay_bank_transfers.candidate_id IS NULL THEN NULL ELSE public.pay_bank_transfers.candidate_id::text END,
      'umbrella_id', CASE WHEN public.pay_bank_transfers.umbrella_id IS NULL THEN NULL ELSE public.pay_bank_transfers.umbrella_id::text END,
      'transfer_group_key', NULLIF(BTRIM(COALESCE(public.pay_bank_transfers.transfer_group_key, '')), ''),
      'amount', public.pay_bank_transfers.amount,
      'currency', NULLIF(BTRIM(COALESCE(public.pay_bank_transfers.currency, '')), ''),
      'status', NULLIF(BTRIM(COALESCE(public.pay_bank_transfers.status, '')), ''),
      'rail_state', NULLIF(BTRIM(COALESCE(public.pay_bank_transfers.rail_state, '')), ''),
      'rail_provider', NULLIF(BTRIM(COALESCE(public.pay_bank_transfers.rail_provider, '')), ''),
      'rail_env', NULLIF(BTRIM(COALESCE(public.pay_bank_transfers.rail_env, '')), ''),
      'request_id', NULLIF(BTRIM(COALESCE(public.pay_bank_transfers.request_id, '')), ''),
      'rail_tx_id', NULLIF(BTRIM(COALESCE(public.pay_bank_transfers.rail_tx_id, '')), ''),
      'payment_reference', NULLIF(BTRIM(COALESCE(public.pay_bank_transfers.payment_reference, '')), ''),
      'completed_at_utc', CASE
        WHEN public.pay_bank_transfers.completed_at_utc IS NULL THEN NULL
        ELSE TO_CHAR(public.pay_bank_transfers.completed_at_utc AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
      END,
      'created_at_utc', CASE
        WHEN public.pay_bank_transfers.created_at_utc IS NULL THEN NULL
        ELSE TO_CHAR(public.pay_bank_transfers.created_at_utc AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
      END,
      'failed_reason_hash', CASE
        WHEN NULLIF(BTRIM(COALESCE(public.pay_bank_transfers.failed_reason, '')), '') IS NULL THEN NULL
        ELSE MD5(public.pay_bank_transfers.failed_reason)
      END,
      'provider_submission_id', COALESCE(
        NULLIF(BTRIM(public.pay_bank_transfers.rail_meta_json #>> '{provider_submission_id}'), ''),
        NULLIF(BTRIM(public.pay_bank_transfers.rail_meta_json #>> '{submission_id}'), ''),
        NULLIF(BTRIM(public.pay_bank_transfers.rail_meta_json #>> '{rail_submission_id}'), '')
      ),
      'provider_payment_id', COALESCE(
        NULLIF(BTRIM(public.pay_bank_transfers.rail_meta_json #>> '{provider_payment_id}'), ''),
        NULLIF(BTRIM(public.pay_bank_transfers.rail_meta_json #>> '{payment_id}'), ''),
        NULLIF(BTRIM(public.pay_bank_transfers.rail_meta_json #>> '{external_payment_id}'), ''),
        NULLIF(BTRIM(public.pay_bank_transfers.rail_meta_json #>> '{revolut_payment_id}'), ''),
        NULLIF(BTRIM(public.pay_bank_transfers.rail_meta_json #>> '{payment,id}'), '')
      ),
      'provider_transfer_id', COALESCE(
        NULLIF(BTRIM(public.pay_bank_transfers.rail_meta_json #>> '{provider_transfer_id}'), ''),
        NULLIF(BTRIM(public.pay_bank_transfers.rail_meta_json #>> '{transfer_id}'), ''),
        NULLIF(BTRIM(public.pay_bank_transfers.rail_meta_json #>> '{external_transfer_id}'), ''),
        NULLIF(BTRIM(public.pay_bank_transfers.rail_meta_json #>> '{revolut_transfer_id}'), ''),
        NULLIF(BTRIM(public.pay_bank_transfers.rail_meta_json #>> '{transfer,id}'), '')
      ),
      'provider_transaction_id', COALESCE(
        NULLIF(BTRIM(public.pay_bank_transfers.rail_meta_json #>> '{provider_transaction_id}'), ''),
        NULLIF(BTRIM(public.pay_bank_transfers.rail_meta_json #>> '{transaction_id}'), ''),
        NULLIF(BTRIM(public.pay_bank_transfers.rail_meta_json #>> '{external_transaction_id}'), ''),
        NULLIF(BTRIM(public.pay_bank_transfers.rail_meta_json #>> '{revolut_transaction_id}'), ''),
        NULLIF(BTRIM(public.pay_bank_transfers.rail_meta_json #>> '{transaction,id}'), '')
      ),
      'provider_response_id', COALESCE(
        NULLIF(BTRIM(public.pay_bank_transfers.rail_meta_json #>> '{provider,response,id}'), ''),
        NULLIF(BTRIM(public.pay_bank_transfers.rail_meta_json #>> '{response,id}'), '')
      )
    ))
    INTO v_latest_transfer
    FROM public.pay_bank_transfers
    WHERE public.pay_bank_transfers.pay_batch_id = p_pay_batch_id
      AND (
        v_source_kind IS DISTINCT FROM 'pay_bank_transfer'
        OR public.pay_bank_transfers.id = p_source_id
      )
      AND (
        (v_alert_kind = 'BANK_REJECTED_PAYMENT'
          AND (
            UPPER(BTRIM(COALESCE(public.pay_bank_transfers.status, ''))) IN ('FAILED', 'DECLINED', 'REJECTED', 'CANCELLED', 'CANCELED', 'SUBMISSION_FAILED', 'FAILED_BEFORE_COMMIT')
            OR UPPER(BTRIM(COALESCE(public.pay_bank_transfers.rail_state, ''))) IN ('FAILED', 'DECLINED', 'REJECTED', 'CANCELLED', 'CANCELED', 'SUBMISSION_FAILED', 'FAILED_BEFORE_COMMIT')
            OR NULLIF(BTRIM(COALESCE(public.pay_bank_transfers.failed_reason, '')), '') IS NOT NULL
          ))
        OR (v_alert_kind = 'BANK_RETURNED_PAYMENT'
          AND (
            UPPER(BTRIM(COALESCE(public.pay_bank_transfers.status, ''))) IN ('RETURNED', 'REVERTED')
            OR UPPER(BTRIM(COALESCE(public.pay_bank_transfers.rail_state, ''))) IN ('RETURNED', 'REVERTED')
          ))
        OR (v_alert_kind = 'RAIL_SUBMISSION_UNKNOWN_OR_TIMEOUT'
          AND (
            UPPER(BTRIM(COALESCE(public.pay_bank_transfers.status, ''))) IN ('UNKNOWN', 'TIMEOUT', 'TIMED_OUT', 'PENDING_REVIEW')
            OR UPPER(BTRIM(COALESCE(public.pay_bank_transfers.status, ''))) LIKE 'CREATE_ERROR%'
            OR UPPER(BTRIM(COALESCE(public.pay_bank_transfers.rail_state, ''))) IN ('UNKNOWN', 'TIMEOUT', 'TIMED_OUT', 'PENDING_REVIEW')
            OR UPPER(BTRIM(COALESCE(public.pay_bank_transfers.rail_state, ''))) LIKE 'CREATE_ERROR%'
          ))
        OR (v_alert_kind = 'AMBIGUOUS_PAYMENT_REVIEW_REQUIRED')
      )
    ORDER BY COALESCE(public.pay_bank_transfers.completed_at_utc, public.pay_bank_transfers.created_at_utc) DESC NULLS LAST,
             public.pay_bank_transfers.id DESC
    LIMIT 1;
    SELECT jsonb_strip_nulls(jsonb_build_object(
      'pay_bank_transfer_event_id', public.pay_bank_transfer_events.id::text,
      'pay_bank_transfer_id', CASE WHEN public.pay_bank_transfer_events.pay_bank_transfer_id IS NULL THEN NULL ELSE public.pay_bank_transfer_events.pay_bank_transfer_id::text END,
      'provider_key', public.pay_bank_transfer_events.provider_key,
      'provider_event_id', public.pay_bank_transfer_events.provider_event_id,
      'provider_reference', public.pay_bank_transfer_events.provider_reference,
      'provider_state', public.pay_bank_transfer_events.provider_state,
      'normalised_state', public.pay_bank_transfer_events.normalised_state,
      'mapping_status', public.pay_bank_transfer_events.mapping_status,
      'movement_classification', public.pay_bank_transfer_events.movement_classification,
      'correction_disposition', public.pay_bank_transfer_events.correction_disposition,
      'event_source', public.pay_bank_transfer_events.event_source,
      'event_time_utc', CASE
        WHEN public.pay_bank_transfer_events.event_time_utc IS NULL THEN NULL
        ELSE TO_CHAR(public.pay_bank_transfer_events.event_time_utc AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
      END,
      'received_at_utc', CASE
        WHEN public.pay_bank_transfer_events.received_at_utc IS NULL THEN NULL
        ELSE TO_CHAR(public.pay_bank_transfer_events.received_at_utc AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
      END
    ))
    INTO v_latest_transfer_event
    FROM public.pay_bank_transfer_events
    WHERE public.pay_bank_transfer_events.pay_batch_id = p_pay_batch_id
      AND (
        v_source_kind IS DISTINCT FROM 'pay_bank_transfer_event'
        OR public.pay_bank_transfer_events.id = p_source_id
      )
      AND (
        (v_alert_kind = 'BANK_REJECTED_PAYMENT'
          AND (
            UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.normalised_state, ''))) IN ('FAILED', 'DECLINED', 'REJECTED', 'CANCELLED', 'CANCELED', 'SUBMISSION_FAILED', 'FAILED_BEFORE_COMMIT')
            OR UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.provider_state, ''))) IN ('FAILED', 'DECLINED', 'REJECTED', 'CANCELLED', 'CANCELED', 'SUBMISSION_FAILED', 'FAILED_BEFORE_COMMIT')
            OR UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.movement_classification, ''))) IN ('NO_MONEY_UNWIND', 'PRE_BANK_CANCEL')
          ))
        OR (v_alert_kind = 'BANK_RETURNED_PAYMENT'
          AND (
            UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.normalised_state, ''))) IN ('RETURNED', 'REVERTED')
            OR UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.provider_state, ''))) IN ('RETURNED', 'REVERTED')
          ))
        OR (v_alert_kind = 'AMBIGUOUS_PAYMENT_REVIEW_REQUIRED'
          AND (
            UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.mapping_status, ''))) IN ('AMBIGUOUS', 'UNMATCHED', 'NO_MATCH', 'MULTIPLE_MATCHES')
            OR UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.correction_disposition, ''))) IN ('AMBIGUOUS', 'ACTION_REQUIRED')
            OR UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.movement_classification, ''))) IN ('AMBIGUOUS_REVIEW_REQUIRED')
          ))
        OR (v_alert_kind = 'RAIL_SUBMISSION_UNKNOWN_OR_TIMEOUT'
          AND (
            UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.normalised_state, ''))) IN ('UNKNOWN', 'TIMEOUT', 'TIMED_OUT', 'PENDING_REVIEW')
            OR UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.normalised_state, ''))) LIKE 'CREATE_ERROR%'
            OR UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.provider_state, ''))) IN ('UNKNOWN', 'TIMEOUT', 'TIMED_OUT', 'PENDING_REVIEW')
            OR UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.provider_state, ''))) LIKE 'CREATE_ERROR%'
          ))
      )
    ORDER BY COALESCE(public.pay_bank_transfer_events.event_time_utc, public.pay_bank_transfer_events.received_at_utc, public.pay_bank_transfer_events.created_at_utc) DESC NULLS LAST,
             public.pay_bank_transfer_events.id DESC
    LIMIT 1;

    RETURN jsonb_strip_nulls(jsonb_build_object(
      'pay_batch_id', v_batch.id::text,
      'source_kind', v_source_kind,
      'source_id', CASE WHEN p_source_id IS NULL THEN NULL ELSE p_source_id::text END,
      'issue_kind', v_alert_kind,
      'batch_status', UPPER(BTRIM(COALESCE(v_batch.status, ''))),
      'rail_provider', NULLIF(BTRIM(COALESCE(v_batch.rail_provider_snapshot, '')), ''),
      'rail_env', NULLIF(BTRIM(COALESCE(v_batch.rail_env_snapshot, '')), ''),
      'execution_commit_state', UPPER(BTRIM(COALESCE(v_batch.execution_commit_state, 'NOT_SUBMITTED'))),
      'execution_commit_ref', NULLIF(BTRIM(COALESCE(v_batch.execution_commit_ref, '')), ''),
      'execution_committed_at_utc', v_execution_committed_at_utc_text,
      'last_status_checked_at_utc', v_last_status_checked_at_utc_text,
      'latest_transfer', v_latest_transfer,
      'latest_transfer_event', v_latest_transfer_event
    ));
  END IF;

  IF v_alert_kind IN ('PAYMENT_CORRECTION_FAILED', 'PAYMENT_CORRECTION_BLOCKED', 'PAYMENT_CORRECTION_AWAITING_APPROVAL') THEN
    IF v_source_kind = 'pay_bank_transfer_event' AND p_source_id IS NOT NULL THEN
      SELECT jsonb_strip_nulls(jsonb_build_object(
        'pay_bank_transfer_event_id', public.pay_bank_transfer_events.id::text,
        'pay_bank_transfer_id', CASE WHEN public.pay_bank_transfer_events.pay_bank_transfer_id IS NULL THEN NULL ELSE public.pay_bank_transfer_events.pay_bank_transfer_id::text END,
        'provider_key', public.pay_bank_transfer_events.provider_key,
        'provider_event_id', public.pay_bank_transfer_events.provider_event_id,
        'provider_reference', public.pay_bank_transfer_events.provider_reference,
        'provider_state', public.pay_bank_transfer_events.provider_state,
        'normalised_state', public.pay_bank_transfer_events.normalised_state,
        'mapping_status', public.pay_bank_transfer_events.mapping_status,
        'movement_classification', public.pay_bank_transfer_events.movement_classification,
        'correction_disposition', public.pay_bank_transfer_events.correction_disposition,
        'event_source', public.pay_bank_transfer_events.event_source,
        'event_time_utc', CASE
          WHEN public.pay_bank_transfer_events.event_time_utc IS NULL THEN NULL
          ELSE TO_CHAR(public.pay_bank_transfer_events.event_time_utc AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
        END,
        'received_at_utc', CASE
          WHEN public.pay_bank_transfer_events.received_at_utc IS NULL THEN NULL
          ELSE TO_CHAR(public.pay_bank_transfer_events.received_at_utc AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
        END,
        'created_at_utc', CASE
          WHEN public.pay_bank_transfer_events.created_at_utc IS NULL THEN NULL
          ELSE TO_CHAR(public.pay_bank_transfer_events.created_at_utc AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
        END
      ))
      INTO v_latest_transfer_event
      FROM public.pay_bank_transfer_events
      WHERE public.pay_bank_transfer_events.pay_batch_id = p_pay_batch_id
        AND public.pay_bank_transfer_events.id = p_source_id
      LIMIT 1;
    END IF;

    SELECT jsonb_strip_nulls(jsonb_build_object(
      'correction_request_id', public.pay_payment_correction_requests.id::text,
      'correction_kind', public.pay_payment_correction_requests.correction_kind,
      'status', public.pay_payment_correction_requests.status,
      'source_bank_event_id', CASE WHEN public.pay_payment_correction_requests.source_bank_event_id IS NULL THEN NULL ELSE public.pay_payment_correction_requests.source_bank_event_id::text END,
      'requested_at_utc', CASE
        WHEN public.pay_payment_correction_requests.requested_at_utc IS NULL THEN NULL
        ELSE TO_CHAR(public.pay_payment_correction_requests.requested_at_utc AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
      END,
      'updated_at_utc', CASE
        WHEN public.pay_payment_correction_requests.updated_at_utc IS NULL THEN NULL
        ELSE TO_CHAR(public.pay_payment_correction_requests.updated_at_utc AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
      END,
      'authorised_at_utc', CASE
        WHEN public.pay_payment_correction_requests.authorised_at_utc IS NULL THEN NULL
        ELSE TO_CHAR(public.pay_payment_correction_requests.authorised_at_utc AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
      END,
      'applied_at_utc', CASE
        WHEN public.pay_payment_correction_requests.applied_at_utc IS NULL THEN NULL
        ELSE TO_CHAR(public.pay_payment_correction_requests.applied_at_utc AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
      END,
      'cancelled_at_utc', CASE
        WHEN public.pay_payment_correction_requests.cancelled_at_utc IS NULL THEN NULL
        ELSE TO_CHAR(public.pay_payment_correction_requests.cancelled_at_utc AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
      END
    ))
    INTO v_latest_correction_request
    FROM public.pay_payment_correction_requests
    WHERE public.pay_payment_correction_requests.pay_batch_id = p_pay_batch_id
      AND (
        v_source_kind IS DISTINCT FROM 'pay_payment_correction_request'
        OR public.pay_payment_correction_requests.id = p_source_id
      )
      AND (
        (v_alert_kind = 'PAYMENT_CORRECTION_FAILED'
          AND UPPER(BTRIM(COALESCE(public.pay_payment_correction_requests.status, ''))) IN ('FAILED', 'FAILED_RETRYABLE', 'FAILED_FINAL'))
        OR (v_alert_kind = 'PAYMENT_CORRECTION_BLOCKED'
          AND UPPER(BTRIM(COALESCE(public.pay_payment_correction_requests.status, ''))) IN ('BLOCKED', 'APPLIED_WITH_BLOCKERS'))
        OR (v_alert_kind = 'PAYMENT_CORRECTION_AWAITING_APPROVAL'
          AND UPPER(BTRIM(COALESCE(public.pay_payment_correction_requests.status, ''))) IN ('REQUESTED', 'AWAITING_AUTHORISATION', 'AWAITING_AUTHORIZATION', 'PENDING_APPROVAL'))
      )
    ORDER BY COALESCE(public.pay_payment_correction_requests.updated_at_utc, public.pay_payment_correction_requests.requested_at_utc, public.pay_payment_correction_requests.created_at_utc) DESC NULLS LAST,
             public.pay_payment_correction_requests.id DESC
    LIMIT 1;

    SELECT jsonb_strip_nulls(jsonb_build_object(
      'correction_work_item_id', public.pay_payment_correction_work_items.id::text,
      'correction_request_id', public.pay_payment_correction_work_items.correction_request_id::text,
      'pay_bank_transfer_id', CASE WHEN public.pay_payment_correction_work_items.pay_bank_transfer_id IS NULL THEN NULL ELSE public.pay_payment_correction_work_items.pay_bank_transfer_id::text END,
      'work_kind', public.pay_payment_correction_work_items.work_kind,
      'status', public.pay_payment_correction_work_items.status,
      'attempt_count', public.pay_payment_correction_work_items.attempt_count,
      'last_error_hash', CASE
        WHEN NULLIF(BTRIM(COALESCE(public.pay_payment_correction_work_items.last_error, '')), '') IS NULL THEN NULL
        ELSE MD5(public.pay_payment_correction_work_items.last_error)
      END,
      'created_at_utc', CASE
        WHEN public.pay_payment_correction_work_items.created_at_utc IS NULL THEN NULL
        ELSE TO_CHAR(public.pay_payment_correction_work_items.created_at_utc AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
      END,
      'processed_at_utc', CASE
        WHEN public.pay_payment_correction_work_items.processed_at_utc IS NULL THEN NULL
        ELSE TO_CHAR(public.pay_payment_correction_work_items.processed_at_utc AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
      END
    ))
    INTO v_latest_correction_work_item
    FROM public.pay_payment_correction_work_items
    WHERE public.pay_payment_correction_work_items.pay_batch_id = p_pay_batch_id
      AND (
        v_source_kind IS DISTINCT FROM 'pay_payment_correction_work_item'
        OR public.pay_payment_correction_work_items.id = p_source_id
      )
      AND (
        (v_alert_kind = 'PAYMENT_CORRECTION_FAILED'
          AND UPPER(BTRIM(COALESCE(public.pay_payment_correction_work_items.status, ''))) IN ('FAILED', 'FAILED_RETRYABLE', 'FAILED_FINAL'))
        OR (v_alert_kind = 'PAYMENT_CORRECTION_BLOCKED'
          AND UPPER(BTRIM(COALESCE(public.pay_payment_correction_work_items.status, ''))) IN ('BLOCKED', 'APPLIED_WITH_BLOCKERS'))
        OR (v_alert_kind = 'PAYMENT_CORRECTION_AWAITING_APPROVAL'
          AND UPPER(BTRIM(COALESCE(public.pay_payment_correction_work_items.status, ''))) IN ('REQUESTED', 'AWAITING_AUTHORISATION', 'AWAITING_AUTHORIZATION', 'PENDING_APPROVAL'))
      )
    ORDER BY COALESCE(public.pay_payment_correction_work_items.processed_at_utc, public.pay_payment_correction_work_items.created_at_utc) DESC NULLS LAST,
             public.pay_payment_correction_work_items.id DESC
    LIMIT 1;

    RETURN jsonb_strip_nulls(jsonb_build_object(
      'pay_batch_id', v_batch.id::text,
      'source_kind', v_source_kind,
      'source_id', CASE WHEN p_source_id IS NULL THEN NULL ELSE p_source_id::text END,
      'issue_kind', v_alert_kind,
      'batch_status', UPPER(BTRIM(COALESCE(v_batch.status, ''))),
      'rail_provider', NULLIF(BTRIM(COALESCE(v_batch.rail_provider_snapshot, '')), ''),
      'rail_env', NULLIF(BTRIM(COALESCE(v_batch.rail_env_snapshot, '')), ''),
      'latest_correction_request', v_latest_correction_request,
      'latest_correction_work_item', v_latest_correction_work_item,
      'latest_transfer_event', v_latest_transfer_event
    ));
  END IF;


  IF v_alert_kind = 'PAYMENT_PROVIDER_SUBMIT_REVIEW' THEN
    IF v_source_kind = 'banking_pay_operation' AND p_source_id IS NOT NULL THEN
      v_provider_operation_id := p_source_id;
    ELSE
      v_provider_operation_id := NULL::uuid;
    END IF;

    v_provider_submit_diagnostic_result := public.pay_provider_submit_diagnostic_get(
      p_pay_batch_id := p_pay_batch_id,
      p_operation_id := v_provider_operation_id,
      p_transfer_id := NULL::uuid,
      p_chunk_id := NULL::uuid,
      p_counts_only := false
    );
    v_provider_submit_diagnostic := COALESCE(v_provider_submit_diagnostic_result->'provider_submit_diagnostic', '{}'::jsonb);
    v_provider_submission_status := UPPER(BTRIM(COALESCE(v_provider_submit_diagnostic_result->>'provider_submission_status', v_provider_submit_diagnostic->>'provider_submission_status', '')));
    v_review_reason_code := COALESCE(NULLIF(BTRIM(COALESCE(v_provider_submit_diagnostic_result->>'review_reason_code', v_provider_submit_diagnostic->>'review_reason_code', '')), ''), 'PAYMENT_PROVIDER_SUBMIT_REVIEW');

    v_provider_title := CASE v_provider_submission_status
      WHEN 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK' THEN 'Provider submission outcome unknown'
      WHEN 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME' THEN 'Provider response missing'
      WHEN 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE' THEN 'Provider response unusable'
      WHEN 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID' THEN 'Provider acceptance evidence missing'
      WHEN 'PROVIDER_SUBMISSION_REJECTED' THEN 'Provider outcome needs review'
      WHEN 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL' THEN 'Provider was not called'
      WHEN 'PROVIDER_SUBMISSION_ACCEPTED' THEN 'Provider acceptance evidence present'
      ELSE 'Provider submission needs review'
    END;

    v_provider_description := CASE v_provider_submission_status
      WHEN 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK' THEN 'Submit chunk became stale with no provider response, transfer event, rail transaction ID, or rail state.'
      WHEN 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME' THEN 'A provider request may have been sent, but no usable provider response was recorded.'
      WHEN 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE' THEN 'Provider returned an unusable response. Manual reconciliation is required before retry.'
      WHEN 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID' THEN 'Provider response/status was recorded, but no usable external provider transaction/reference was stored.'
      WHEN 'PROVIDER_SUBMISSION_REJECTED' THEN 'Provider rejected the payment submission.'
      WHEN 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL' THEN 'Provider submission failed before the provider payment request was sent.'
      WHEN 'PROVIDER_SUBMISSION_ACCEPTED' THEN 'Provider acceptance evidence exists and retry is unsafe until reconciliation is complete.'
      ELSE 'Provider submission requires review before retry.'
    END;

    v_provider_action_label := COALESCE(NULLIF(BTRIM(v_provider_submit_diagnostic->>'recommended_action'), ''), 'Check Revolut/bank before retry. If no payment was made, record manual no-payment confirmation.');

    RETURN jsonb_strip_nulls(jsonb_build_object(
      'alert_kind', 'PAYMENT_PROVIDER_SUBMIT_REVIEW',
      'issue_kind', 'PAYMENT_PROVIDER_SUBMIT_REVIEW',
      'title', v_provider_title,
      'description', v_provider_description,
      'action_label', v_provider_action_label,
      'recommended_action', v_provider_action_label,
      'provider_submission_status', v_provider_submission_status,
      'review_reason_code', v_review_reason_code,
      'manual_resolution_required', lower(BTRIM(COALESCE(v_provider_submit_diagnostic->>'manual_resolution_required', v_provider_submit_diagnostic_result->>'manual_resolution_required', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on'),
      'safe_retry_available', lower(BTRIM(COALESCE(v_provider_submit_diagnostic->>'safe_retry_available', v_provider_submit_diagnostic_result->>'safe_retry_available', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on'),
      'provider_acceptance_evidence_present', lower(BTRIM(COALESCE(v_provider_submit_diagnostic->>'provider_acceptance_evidence_present', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on'),
      'provider_response_present', lower(BTRIM(COALESCE(v_provider_submit_diagnostic->>'provider_response_present', v_provider_submit_diagnostic->>'provider_response_received', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on'),
      'pay_batch_id', v_batch.id::text,
      'operation_id', NULLIF(BTRIM(COALESCE(v_provider_submit_diagnostic->>'operation_id', CASE WHEN v_provider_operation_id IS NULL THEN NULL ELSE v_provider_operation_id::text END, '')), ''),
      'chunk_id', NULLIF(BTRIM(COALESCE(v_provider_submit_diagnostic->>'chunk_id', '')), ''),
      'transfer_id', NULLIF(BTRIM(COALESCE(v_provider_submit_diagnostic->>'transfer_id', '')), ''),
      'transfer_scope_id', NULLIF(BTRIM(COALESCE(v_provider_submit_diagnostic->>'transfer_scope_id', '')), ''),
      'auth_request_id', NULLIF(BTRIM(COALESCE(v_provider_submit_diagnostic->>'auth_request_id', '')), ''),
      'rail_provider', NULLIF(BTRIM(COALESCE(v_provider_submit_diagnostic->>'rail_provider', v_batch.rail_provider_snapshot, '')), ''),
      'rail_env', NULLIF(BTRIM(COALESCE(v_provider_submit_diagnostic->>'rail_env', v_batch.rail_env_snapshot, '')), ''),
      'provider_submit_diagnostic', v_provider_submit_diagnostic - 'provider_response_redacted' - 'provider_error_redacted' - 'raw_payload' - 'provider_raw_response' - 'raw_provider_response' - 'provider_raw_error' - 'raw_provider_error'
    ));
  END IF;

  IF v_alert_kind = 'REMITTANCE_SEND_FAILED' THEN
    SELECT jsonb_strip_nulls(jsonb_build_object(
      'mail_outbox_id', public.mail_outbox.id::text,
      'type', public.mail_outbox.type,
      'status', public.mail_outbox.status::text,
      'reference', public.mail_outbox.reference,
      'recipient_kind', public.mail_outbox.recipient_kind,
      'recipient_id', CASE WHEN public.mail_outbox.recipient_id IS NULL THEN NULL ELSE public.mail_outbox.recipient_id::text END,
      'last_error_hash', CASE
        WHEN NULLIF(BTRIM(COALESCE(public.mail_outbox.last_error, '')), '') IS NULL THEN NULL
        ELSE MD5(public.mail_outbox.last_error)
      END,
      'created_at_utc', CASE
        WHEN public.mail_outbox.created_at_utc IS NULL THEN NULL
        ELSE TO_CHAR(public.mail_outbox.created_at_utc AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
      END,
      'failed_at', CASE
        WHEN public.mail_outbox.failed_at IS NULL THEN NULL
        ELSE TO_CHAR(public.mail_outbox.failed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
      END
    ))
    INTO v_latest_remittance_failure
    FROM public.mail_outbox
    WHERE public.mail_outbox.context_kind = 'pay_batches'
      AND public.mail_outbox.context_id = p_pay_batch_id
      AND (
        v_source_kind IS DISTINCT FROM 'mail_outbox'
        OR public.mail_outbox.id = p_source_id
      )
      AND UPPER(BTRIM(COALESCE(public.mail_outbox.type, ''))) = 'REMITTANCE'
      AND (
        UPPER(BTRIM(COALESCE(public.mail_outbox.status::text, ''))) = 'FAILED'
        OR public.mail_outbox.failed_at IS NOT NULL
        OR NULLIF(BTRIM(COALESCE(public.mail_outbox.last_error, '')), '') IS NOT NULL
      )
    ORDER BY COALESCE(public.mail_outbox.failed_at, public.mail_outbox.created_at_utc) DESC NULLS LAST,
             public.mail_outbox.id DESC
    LIMIT 1;

    RETURN jsonb_strip_nulls(jsonb_build_object(
      'pay_batch_id', v_batch.id::text,
      'source_kind', v_source_kind,
      'source_id', CASE WHEN p_source_id IS NULL THEN NULL ELSE p_source_id::text END,
      'issue_kind', 'REMITTANCE_SEND_FAILED',
      'batch_status', UPPER(BTRIM(COALESCE(v_batch.status, ''))),
      'rail_provider', NULLIF(BTRIM(COALESCE(v_batch.rail_provider_snapshot, '')), ''),
      'rail_env', NULLIF(BTRIM(COALESCE(v_batch.rail_env_snapshot, '')), ''),
      'latest_remittance_failure', v_latest_remittance_failure
    ));
  END IF;

  RAISE EXCEPTION 'BANKING_ALERT_PAYLOAD_KIND_NOT_SUPPORTED'
    USING ERRCODE = 'P0001',
          DETAIL = jsonb_build_object(
            'code', 'BANKING_ALERT_PAYLOAD_KIND_NOT_SUPPORTED',
            'alert_kind', v_alert_kind,
            'pay_batch_id', p_pay_batch_id::text
          )::text;
END;
$function$;

-- banking_alert_preferences_get(uuid)
CREATE OR REPLACE FUNCTION public.banking_alert_preferences_get(p_user_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_preferences public.banking_alert_user_preferences%ROWTYPE;
  v_defaults_applied boolean := false;
  v_enabled boolean := true;
  v_alert_kind_allowlist jsonb := NULL::jsonb;
  v_alert_kind_blocklist jsonb := '[]'::jsonb;
  v_failure_reason_allowlist jsonb := NULL::jsonb;
  v_failure_reason_blocklist jsonb := '[]'::jsonb;
  v_include_action_required boolean := true;
  v_include_progress_alerts boolean := true;
  v_include_informational_alerts boolean := false;
  v_include_success_alerts boolean := true;
  v_severity_min text := 'ACTION_REQUIRED';
  v_muted_provider_keys jsonb := '[]'::jsonb;
  v_muted_pay_batch_ids jsonb := '[]'::jsonb;
  v_snoozed_until_utc_text text := NULL::text;
  v_mode text := 'ALL_ACTION_REQUIRED';
  v_failure_reason_groups jsonb := '[]'::jsonb;
  v_informational_alert_kinds jsonb := '[]'::jsonb;
  v_allowed_alert_kinds jsonb := to_jsonb(ARRAY[
    'PROVIDER_OUTAGE_RETRY_LATER',
    'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER',
    'TERMINAL_NO_MONEY_REWIND_AVAILABLE',
    'AUTO_UNWIND_PROGRESS',
    'WHOLE_BATCH_CANCELLATION_PROGRESS',
    'MANUAL_ADJUSTMENTS_CARRIED_FORWARD',
    'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS',
    'PAID_SETTLED_RECOVERY_REQUIRED',
    'CANCELLATION_RACED_WITH_PROVIDER_SUBMIT',
    'WEBHOOK_UNMATCHED_REVIEW_REQUIRED',
    'BATCH_SCHEDULED_SUCCESS',
    'BATCH_SETTLED_SUCCESS'
  ]::text[]);
  v_allowed_failure_reason_groups jsonb := to_jsonb(ARRAY[
    'INSUFFICIENT_FUNDS',
    'UNKNOWN_RECIPIENT',
    'INVALID_ACCOUNT',
    'ACCOUNT_CLOSED',
    'BANK_REJECTED',
    'PROVIDER_OUTAGE',
    'PROVIDER_UNKNOWN',
    'COMPLIANCE_REVIEW',
    'DUPLICATE_RISK',
    'PAID_RECOVERY_REQUIRED',
    'MANUAL_ADJUSTMENT_BLOCKER',
    'WEBHOOK_UNMATCHED',
    'PROVIDER_FAILED_UNSPECIFIED'
  ]::text[]);
  v_progress_alert_kinds jsonb := to_jsonb(ARRAY[
    'AUTO_UNWIND_PROGRESS',
    'WHOLE_BATCH_CANCELLATION_PROGRESS'
  ]::text[]);
  v_default_informational_alert_kinds jsonb := to_jsonb(ARRAY[
    'MANUAL_ADJUSTMENTS_CARRIED_FORWARD'
  ]::text[]);
  v_effective_options_json jsonb := '{}'::jsonb;
BEGIN
  IF p_user_id IS NULL THEN
    RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_GET_USER_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_GET_USER_REQUIRED')::text;
  END IF;

  SELECT preference_row.*
  INTO v_preferences
  FROM public.banking_alert_user_preferences AS preference_row
  WHERE preference_row.user_id = p_user_id;

  IF FOUND THEN
    v_enabled := COALESCE(v_preferences.enabled, true);
    v_alert_kind_allowlist := CASE WHEN jsonb_typeof(v_preferences.alert_kind_allowlist) = 'array' THEN v_preferences.alert_kind_allowlist ELSE NULL::jsonb END;
    v_alert_kind_blocklist := COALESCE(v_preferences.alert_kind_blocklist, '[]'::jsonb);
    v_failure_reason_allowlist := CASE WHEN jsonb_typeof(v_preferences.failure_reason_allowlist) = 'array' THEN v_preferences.failure_reason_allowlist ELSE NULL::jsonb END;
    v_failure_reason_blocklist := COALESCE(v_preferences.failure_reason_blocklist, '[]'::jsonb);
    v_include_action_required := COALESCE(v_preferences.include_action_required, true);
    v_include_progress_alerts := COALESCE(v_preferences.include_progress_alerts, true);
    v_include_informational_alerts := COALESCE(v_preferences.include_informational_alerts, false);
    v_include_success_alerts := COALESCE(v_preferences.include_success_alerts, true);
    v_severity_min := COALESCE(NULLIF(BTRIM(v_preferences.severity_min), ''), 'ACTION_REQUIRED');
    v_muted_provider_keys := COALESCE(v_preferences.muted_provider_keys, '[]'::jsonb);
    v_muted_pay_batch_ids := COALESCE(v_preferences.muted_pay_batch_ids, '[]'::jsonb);
    v_snoozed_until_utc_text := CASE WHEN v_preferences.snoozed_until_utc IS NULL THEN NULL ELSE v_preferences.snoozed_until_utc::text END;
  ELSE
    v_defaults_applied := true;
  END IF;

  v_failure_reason_groups := CASE
    WHEN jsonb_typeof(v_failure_reason_allowlist) = 'array' THEN v_failure_reason_allowlist
    ELSE '[]'::jsonb
  END;

  v_mode := CASE
    WHEN COALESCE(v_enabled, true) IS NOT TRUE THEN 'NO_BANKING_PAY_ALERTS'
    WHEN jsonb_typeof(v_failure_reason_allowlist) = 'array' AND jsonb_array_length(v_failure_reason_allowlist) > 0 THEN 'SELECTED_FAILURE_REASONS'
    ELSE 'ALL_ACTION_REQUIRED'
  END;

  v_informational_alert_kinds := '[]'::jsonb;
  IF COALESCE(v_include_progress_alerts, true) THEN
    v_informational_alert_kinds := v_informational_alert_kinds || v_progress_alert_kinds;
  END IF;
  IF COALESCE(v_include_informational_alerts, false) THEN
    v_informational_alert_kinds := v_informational_alert_kinds || v_default_informational_alert_kinds;
  END IF;

  v_effective_options_json := jsonb_build_object(
    'modes', to_jsonb(ARRAY['ALL_ACTION_REQUIRED','SELECTED_FAILURE_REASONS','NO_BANKING_PAY_ALERTS']::text[]),
    'alert_kinds', v_allowed_alert_kinds,
    'failure_reason_groups', v_allowed_failure_reason_groups,
    'progress_alert_kinds', v_progress_alert_kinds,
    'informational_alert_kinds', v_default_informational_alert_kinds,
        'success_alerts_available', true,
    'default_mode', 'ALL_ACTION_REQUIRED'
  );

  RETURN jsonb_build_object(
    'ok', true,
    'user_id', p_user_id::text,
    'enabled', v_enabled,
    'mode', v_mode,
    'alert_kind_allowlist', v_alert_kind_allowlist,
    'alert_kind_blocklist', v_alert_kind_blocklist,
    'failure_reason_allowlist', v_failure_reason_allowlist,
    'failure_reason_blocklist', v_failure_reason_blocklist,
    'failure_reason_groups', v_failure_reason_groups,
    'include_action_required', v_include_action_required,
    'include_progress_alerts', v_include_progress_alerts,
    'include_informational_alerts', v_include_informational_alerts,
    'include_success_alerts', v_include_success_alerts,
    'informational_alert_kinds', v_informational_alert_kinds,
    'severity_min', v_severity_min,
    'muted_provider_keys', v_muted_provider_keys,
    'muted_pay_batch_ids', v_muted_pay_batch_ids,
    'snoozed_until_utc', v_snoozed_until_utc_text,
    'effective_defaults_applied', v_defaults_applied,
    'effective_options_json', v_effective_options_json
  );
END;
$function$;

-- banking_alert_preferences_update(uuid,jsonb)
CREATE OR REPLACE FUNCTION public.banking_alert_preferences_update(p_user_id uuid, p_preferences_json jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_payload jsonb := '{}'::jsonb;
  v_current jsonb := '{}'::jsonb;
  v_enabled boolean := true;
  v_alert_kind_allowlist jsonb := NULL::jsonb;
  v_alert_kind_blocklist jsonb := '[]'::jsonb;
  v_failure_reason_allowlist jsonb := NULL::jsonb;
  v_failure_reason_blocklist jsonb := '[]'::jsonb;
  v_include_action_required boolean := true;
  v_include_progress_alerts boolean := true;
  v_include_informational_alerts boolean := false;
  v_include_success_alerts boolean := true;
  v_severity_min text := 'ACTION_REQUIRED';
  v_muted_provider_keys jsonb := '[]'::jsonb;
  v_muted_pay_batch_ids jsonb := '[]'::jsonb;
  v_snoozed_until_utc timestamptz := NULL::timestamptz;
  v_mode text := NULL::text;
  v_informational_alert_kinds jsonb := '[]'::jsonb;
  v_allowed_alert_kinds text[] := ARRAY[
    'PROVIDER_OUTAGE_RETRY_LATER',
    'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER',
    'TERMINAL_NO_MONEY_REWIND_AVAILABLE',
    'AUTO_UNWIND_PROGRESS',
    'WHOLE_BATCH_CANCELLATION_PROGRESS',
    'MANUAL_ADJUSTMENTS_CARRIED_FORWARD',
    'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS',
    'PAID_SETTLED_RECOVERY_REQUIRED',
    'CANCELLATION_RACED_WITH_PROVIDER_SUBMIT',
    'WEBHOOK_UNMATCHED_REVIEW_REQUIRED',
    'BATCH_SCHEDULED_SUCCESS',
    'BATCH_SETTLED_SUCCESS'
  ];
  v_allowed_failure_reasons text[] := ARRAY[
    'INSUFFICIENT_FUNDS',
    'UNKNOWN_RECIPIENT',
    'INVALID_ACCOUNT',
    'ACCOUNT_CLOSED',
    'BANK_REJECTED',
    'PROVIDER_OUTAGE',
    'PROVIDER_UNKNOWN',
    'COMPLIANCE_REVIEW',
    'DUPLICATE_RISK',
    'PAID_RECOVERY_REQUIRED',
    'MANUAL_ADJUSTMENT_BLOCKER',
    'WEBHOOK_UNMATCHED',
    'PROVIDER_FAILED_UNSPECIFIED'
  ];
  v_value_text text;
  v_preference_id uuid;
  v_result jsonb := '{}'::jsonb;
BEGIN
  IF p_user_id IS NULL THEN
    RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_UPDATE_USER_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_UPDATE_USER_REQUIRED')::text;
  END IF;

  IF p_preferences_json IS NOT NULL AND COALESCE(jsonb_typeof(p_preferences_json), 'null') <> 'object' THEN
    RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_UPDATE_PAYLOAD_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_UPDATE_PAYLOAD_MUST_BE_OBJECT')::text;
  END IF;

  v_payload := COALESCE(p_preferences_json, '{}'::jsonb);
  v_current := public.banking_alert_preferences_get(p_user_id);

  v_enabled := COALESCE((v_current ->> 'enabled')::boolean, true);
  v_alert_kind_allowlist := CASE WHEN jsonb_typeof(v_current -> 'alert_kind_allowlist') = 'array' THEN v_current -> 'alert_kind_allowlist' ELSE NULL::jsonb END;
  v_alert_kind_blocklist := CASE WHEN jsonb_typeof(v_current -> 'alert_kind_blocklist') = 'array' THEN v_current -> 'alert_kind_blocklist' ELSE '[]'::jsonb END;
  v_failure_reason_allowlist := CASE WHEN jsonb_typeof(v_current -> 'failure_reason_allowlist') = 'array' THEN v_current -> 'failure_reason_allowlist' ELSE NULL::jsonb END;
  v_failure_reason_blocklist := CASE WHEN jsonb_typeof(v_current -> 'failure_reason_blocklist') = 'array' THEN v_current -> 'failure_reason_blocklist' ELSE '[]'::jsonb END;
  v_include_action_required := COALESCE((v_current ->> 'include_action_required')::boolean, true);
  v_include_progress_alerts := COALESCE((v_current ->> 'include_progress_alerts')::boolean, true);
  v_include_informational_alerts := COALESCE((v_current ->> 'include_informational_alerts')::boolean, false);
  v_include_success_alerts := COALESCE((v_current ->> 'include_success_alerts')::boolean, true);
  v_severity_min := COALESCE(NULLIF(BTRIM(v_current ->> 'severity_min'), ''), 'ACTION_REQUIRED');
  v_muted_provider_keys := COALESCE(v_current -> 'muted_provider_keys', '[]'::jsonb);
  v_muted_pay_batch_ids := COALESCE(v_current -> 'muted_pay_batch_ids', '[]'::jsonb);

  IF NULLIF(BTRIM(COALESCE(v_current ->> 'snoozed_until_utc', '')), '') IS NOT NULL THEN
    BEGIN
      v_snoozed_until_utc := (v_current ->> 'snoozed_until_utc')::timestamptz;
    EXCEPTION WHEN OTHERS THEN
      v_snoozed_until_utc := NULL::timestamptz;
    END;
  END IF;

  IF v_payload ? 'mode' THEN
    IF COALESCE(jsonb_typeof(v_payload -> 'mode'), 'null') <> 'string' THEN
      RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_MODE_MUST_BE_STRING'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_MODE_MUST_BE_STRING')::text;
    END IF;

    v_mode := UPPER(NULLIF(BTRIM(COALESCE(v_payload ->> 'mode', '')), ''));
    IF v_mode = 'NONE' THEN
      v_mode := 'NO_BANKING_PAY_ALERTS';
    END IF;

    IF v_mode NOT IN ('NO_BANKING_PAY_ALERTS', 'ALL_ACTION_REQUIRED', 'SELECTED_FAILURE_REASONS') THEN
      RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_INVALID_MODE'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_INVALID_MODE', 'mode', COALESCE(v_payload ->> 'mode', NULL))::text;
    END IF;

    IF v_mode = 'NO_BANKING_PAY_ALERTS' THEN
      v_enabled := false;
    ELSIF v_mode = 'ALL_ACTION_REQUIRED' THEN
      v_enabled := true;
      v_include_action_required := true;
      IF NOT (v_payload ? 'failure_reason_allowlist') AND NOT (v_payload ? 'failure_reason_groups') THEN
        v_failure_reason_allowlist := NULL::jsonb;
      END IF;
    ELSIF v_mode = 'SELECTED_FAILURE_REASONS' THEN
      v_enabled := true;
      v_include_action_required := true;
      IF NOT (v_payload ? 'failure_reason_allowlist') AND NOT (v_payload ? 'failure_reason_groups') THEN
        v_failure_reason_allowlist := '[]'::jsonb;
      END IF;
    END IF;
  END IF;

  IF v_payload ? 'failure_reason_groups' THEN
    IF COALESCE(jsonb_typeof(v_payload -> 'failure_reason_groups'), 'null') <> 'array' THEN
      RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_REASON_GROUPS_MUST_BE_ARRAY'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_REASON_GROUPS_MUST_BE_ARRAY')::text;
    END IF;

    SELECT COALESCE(jsonb_agg(to_jsonb(normalised_reason.failure_reason_group) ORDER BY normalised_reason.failure_reason_group), '[]'::jsonb)
    INTO v_failure_reason_allowlist
    FROM (
      SELECT DISTINCT CASE UPPER(BTRIM(reason_value.value))
        WHEN 'PROVIDER_OUTCOME_UNKNOWN' THEN 'PROVIDER_UNKNOWN'
        WHEN 'UNSPECIFIED_PROVIDER_FAILURE' THEN 'PROVIDER_FAILED_UNSPECIFIED'
        ELSE UPPER(BTRIM(reason_value.value))
      END AS failure_reason_group
      FROM jsonb_array_elements_text(v_payload -> 'failure_reason_groups') AS reason_value(value)
      WHERE NULLIF(BTRIM(reason_value.value), '') IS NOT NULL
    ) AS normalised_reason;
  END IF;

  IF v_payload ? 'informational_alert_kinds' THEN
    IF COALESCE(jsonb_typeof(v_payload -> 'informational_alert_kinds'), 'null') <> 'array' THEN
      RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_INFORMATIONAL_KINDS_MUST_BE_ARRAY'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_INFORMATIONAL_KINDS_MUST_BE_ARRAY')::text;
    END IF;

    SELECT COALESCE(jsonb_agg(to_jsonb(UPPER(BTRIM(informational_kind.value))) ORDER BY UPPER(BTRIM(informational_kind.value))), '[]'::jsonb)
    INTO v_informational_alert_kinds
    FROM jsonb_array_elements_text(v_payload -> 'informational_alert_kinds') AS informational_kind(value)
    WHERE NULLIF(BTRIM(informational_kind.value), '') IS NOT NULL;

    v_include_informational_alerts := EXISTS (
      SELECT 1
      FROM jsonb_array_elements_text(v_informational_alert_kinds) AS informational_kind(value)
      WHERE informational_kind.value IN ('MANUAL_ADJUSTMENTS_CARRIED_FORWARD')
    );

    IF EXISTS (
      SELECT 1
      FROM jsonb_array_elements_text(v_informational_alert_kinds) AS progress_kind(value)
      WHERE progress_kind.value IN ('AUTO_UNWIND_PROGRESS', 'WHOLE_BATCH_CANCELLATION_PROGRESS')
    ) THEN
      v_include_progress_alerts := true;
    END IF;
  END IF;

  FOR v_value_text IN
    SELECT informational_kind_entry.value
    FROM jsonb_array_elements_text(COALESCE(v_informational_alert_kinds, '[]'::jsonb)) AS informational_kind_entry(value)
  LOOP
    IF NOT (v_value_text = ANY(v_allowed_alert_kinds)) THEN
      RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_INVALID_INFORMATIONAL_ALERT_KIND'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_INVALID_INFORMATIONAL_ALERT_KIND', 'alert_kind', v_value_text)::text;
    END IF;
  END LOOP;

  IF v_payload ? 'enabled' THEN
    IF COALESCE(jsonb_typeof(v_payload -> 'enabled'), 'null') <> 'boolean' THEN
      RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_ENABLED_MUST_BE_BOOLEAN'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_ENABLED_MUST_BE_BOOLEAN')::text;
    END IF;
    v_enabled := (v_payload ->> 'enabled')::boolean;
  END IF;

  IF v_payload ? 'include_action_required' THEN
    IF COALESCE(jsonb_typeof(v_payload -> 'include_action_required'), 'null') <> 'boolean' THEN
      RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_ACTION_REQUIRED_MUST_BE_BOOLEAN'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_ACTION_REQUIRED_MUST_BE_BOOLEAN')::text;
    END IF;
    v_include_action_required := (v_payload ->> 'include_action_required')::boolean;
  END IF;

  IF v_payload ? 'include_progress_alerts' THEN
    IF COALESCE(jsonb_typeof(v_payload -> 'include_progress_alerts'), 'null') <> 'boolean' THEN
      RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_PROGRESS_MUST_BE_BOOLEAN'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_PROGRESS_MUST_BE_BOOLEAN')::text;
    END IF;
    v_include_progress_alerts := (v_payload ->> 'include_progress_alerts')::boolean;
  END IF;

  IF v_payload ? 'include_informational_alerts' THEN
    IF COALESCE(jsonb_typeof(v_payload -> 'include_informational_alerts'), 'null') <> 'boolean' THEN
      RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_INFORMATIONAL_MUST_BE_BOOLEAN'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_INFORMATIONAL_MUST_BE_BOOLEAN')::text;
    END IF;
    v_include_informational_alerts := (v_payload ->> 'include_informational_alerts')::boolean;
  END IF;

  IF v_payload ? 'include_success_alerts' THEN
    IF COALESCE(jsonb_typeof(v_payload -> 'include_success_alerts'), 'null') <> 'boolean' THEN
      RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_SUCCESS_MUST_BE_BOOLEAN'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_SUCCESS_MUST_BE_BOOLEAN')::text;
    END IF;
    v_include_success_alerts := (v_payload ->> 'include_success_alerts')::boolean;
  END IF;

  IF v_payload ? 'severity_min' THEN
    v_severity_min := UPPER(NULLIF(BTRIM(COALESCE(v_payload ->> 'severity_min', '')), ''));
    IF v_severity_min IS NULL OR v_severity_min NOT IN ('INFO','PROGRESS','ACTION_REQUIRED','CRITICAL') THEN
      RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_INVALID_SEVERITY'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'BANKING_ALERT_PREFERENCES_INVALID_SEVERITY',
                'severity_min', COALESCE(v_payload ->> 'severity_min', NULL)
              )::text;
    END IF;
  END IF;

  IF v_payload ? 'alert_kind_allowlist' THEN
    IF COALESCE(jsonb_typeof(v_payload -> 'alert_kind_allowlist'), 'null') = 'null' THEN
      v_alert_kind_allowlist := NULL::jsonb;
    ELSIF jsonb_typeof(v_payload -> 'alert_kind_allowlist') = 'array' THEN
      SELECT COALESCE(jsonb_agg(to_jsonb(normalised_alert_kind.alert_kind) ORDER BY normalised_alert_kind.alert_kind), '[]'::jsonb)
      INTO v_alert_kind_allowlist
      FROM (
        SELECT DISTINCT UPPER(BTRIM(alert_kind_value.value)) AS alert_kind
        FROM jsonb_array_elements_text(v_payload -> 'alert_kind_allowlist') AS alert_kind_value(value)
        WHERE NULLIF(BTRIM(alert_kind_value.value), '') IS NOT NULL
      ) AS normalised_alert_kind;
    ELSE
      RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_ALERT_ALLOWLIST_MUST_BE_ARRAY'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_ALERT_ALLOWLIST_MUST_BE_ARRAY')::text;
    END IF;
  END IF;

  IF v_payload ? 'alert_kind_blocklist' THEN
    IF COALESCE(jsonb_typeof(v_payload -> 'alert_kind_blocklist'), 'null') <> 'array' THEN
      RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_ALERT_BLOCKLIST_MUST_BE_ARRAY'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_ALERT_BLOCKLIST_MUST_BE_ARRAY')::text;
    END IF;
    SELECT COALESCE(jsonb_agg(to_jsonb(normalised_alert_kind.alert_kind) ORDER BY normalised_alert_kind.alert_kind), '[]'::jsonb)
    INTO v_alert_kind_blocklist
    FROM (
      SELECT DISTINCT UPPER(BTRIM(alert_kind_value.value)) AS alert_kind
      FROM jsonb_array_elements_text(v_payload -> 'alert_kind_blocklist') AS alert_kind_value(value)
      WHERE NULLIF(BTRIM(alert_kind_value.value), '') IS NOT NULL
    ) AS normalised_alert_kind;
  END IF;

  FOR v_value_text IN
    SELECT alert_kind_entry.value
    FROM jsonb_array_elements_text(COALESCE(v_alert_kind_allowlist, '[]'::jsonb)) AS alert_kind_entry(value)
  LOOP
    IF NOT (v_value_text = ANY(v_allowed_alert_kinds)) THEN
      RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_INVALID_ALERT_KIND'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_INVALID_ALERT_KIND', 'alert_kind', v_value_text)::text;
    END IF;
  END LOOP;

  FOR v_value_text IN
    SELECT alert_kind_entry.value
    FROM jsonb_array_elements_text(COALESCE(v_alert_kind_blocklist, '[]'::jsonb)) AS alert_kind_entry(value)
  LOOP
    IF NOT (v_value_text = ANY(v_allowed_alert_kinds)) THEN
      RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_INVALID_ALERT_KIND'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_INVALID_ALERT_KIND', 'alert_kind', v_value_text)::text;
    END IF;
  END LOOP;

  IF v_payload ? 'failure_reason_allowlist' THEN
    IF COALESCE(jsonb_typeof(v_payload -> 'failure_reason_allowlist'), 'null') = 'null' THEN
      v_failure_reason_allowlist := NULL::jsonb;
    ELSIF jsonb_typeof(v_payload -> 'failure_reason_allowlist') = 'array' THEN
      SELECT COALESCE(jsonb_agg(to_jsonb(normalised_reason.failure_reason_group) ORDER BY normalised_reason.failure_reason_group), '[]'::jsonb)
      INTO v_failure_reason_allowlist
      FROM (
        SELECT DISTINCT CASE UPPER(BTRIM(reason_value.value))
        WHEN 'PROVIDER_OUTCOME_UNKNOWN' THEN 'PROVIDER_UNKNOWN'
        WHEN 'UNSPECIFIED_PROVIDER_FAILURE' THEN 'PROVIDER_FAILED_UNSPECIFIED'
        ELSE UPPER(BTRIM(reason_value.value))
      END AS failure_reason_group
        FROM jsonb_array_elements_text(v_payload -> 'failure_reason_allowlist') AS reason_value(value)
        WHERE NULLIF(BTRIM(reason_value.value), '') IS NOT NULL
      ) AS normalised_reason;
    ELSE
      RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_REASON_ALLOWLIST_MUST_BE_ARRAY'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_REASON_ALLOWLIST_MUST_BE_ARRAY')::text;
    END IF;
  END IF;

  IF v_payload ? 'failure_reason_blocklist' THEN
    IF COALESCE(jsonb_typeof(v_payload -> 'failure_reason_blocklist'), 'null') <> 'array' THEN
      RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_REASON_BLOCKLIST_MUST_BE_ARRAY'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_REASON_BLOCKLIST_MUST_BE_ARRAY')::text;
    END IF;
    SELECT COALESCE(jsonb_agg(to_jsonb(normalised_reason.failure_reason_group) ORDER BY normalised_reason.failure_reason_group), '[]'::jsonb)
    INTO v_failure_reason_blocklist
    FROM (
      SELECT DISTINCT CASE UPPER(BTRIM(reason_value.value))
        WHEN 'PROVIDER_OUTCOME_UNKNOWN' THEN 'PROVIDER_UNKNOWN'
        WHEN 'UNSPECIFIED_PROVIDER_FAILURE' THEN 'PROVIDER_FAILED_UNSPECIFIED'
        ELSE UPPER(BTRIM(reason_value.value))
      END AS failure_reason_group
      FROM jsonb_array_elements_text(v_payload -> 'failure_reason_blocklist') AS reason_value(value)
      WHERE NULLIF(BTRIM(reason_value.value), '') IS NOT NULL
    ) AS normalised_reason;
  END IF;

  FOR v_value_text IN
    SELECT reason_entry.value
    FROM jsonb_array_elements_text(COALESCE(v_failure_reason_allowlist, '[]'::jsonb)) AS reason_entry(value)
  LOOP
    IF NOT (v_value_text = ANY(v_allowed_failure_reasons)) THEN
      RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_INVALID_FAILURE_REASON'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_INVALID_FAILURE_REASON', 'failure_reason_group', v_value_text)::text;
    END IF;
  END LOOP;

  FOR v_value_text IN
    SELECT reason_entry.value
    FROM jsonb_array_elements_text(COALESCE(v_failure_reason_blocklist, '[]'::jsonb)) AS reason_entry(value)
  LOOP
    IF NOT (v_value_text = ANY(v_allowed_failure_reasons)) THEN
      RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_INVALID_FAILURE_REASON'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_INVALID_FAILURE_REASON', 'failure_reason_group', v_value_text)::text;
    END IF;
  END LOOP;

  IF v_payload ? 'muted_provider_keys' THEN
    IF COALESCE(jsonb_typeof(v_payload -> 'muted_provider_keys'), 'null') <> 'array' THEN
      RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_MUTED_PROVIDERS_MUST_BE_ARRAY'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_MUTED_PROVIDERS_MUST_BE_ARRAY')::text;
    END IF;
    SELECT COALESCE(jsonb_agg(to_jsonb(provider_value.provider_key) ORDER BY provider_value.provider_key), '[]'::jsonb)
    INTO v_muted_provider_keys
    FROM (
      SELECT DISTINCT UPPER(BTRIM(muted_provider.value)) AS provider_key
      FROM jsonb_array_elements_text(v_payload -> 'muted_provider_keys') AS muted_provider(value)
      WHERE NULLIF(BTRIM(muted_provider.value), '') IS NOT NULL
    ) AS provider_value;
  END IF;

  IF v_payload ? 'muted_pay_batch_ids' THEN
    IF COALESCE(jsonb_typeof(v_payload -> 'muted_pay_batch_ids'), 'null') <> 'array' THEN
      RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_MUTED_BATCHES_MUST_BE_ARRAY'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_MUTED_BATCHES_MUST_BE_ARRAY')::text;
    END IF;
    SELECT COALESCE(jsonb_agg(to_jsonb(batch_value.batch_id_text) ORDER BY batch_value.batch_id_text), '[]'::jsonb)
    INTO v_muted_pay_batch_ids
    FROM (
      SELECT DISTINCT LOWER(BTRIM(muted_batch.value)) AS batch_id_text
      FROM jsonb_array_elements_text(v_payload -> 'muted_pay_batch_ids') AS muted_batch(value)
      WHERE NULLIF(BTRIM(muted_batch.value), '') IS NOT NULL
    ) AS batch_value;
  END IF;

  FOR v_value_text IN
    SELECT muted_batch_entry.value
    FROM jsonb_array_elements_text(COALESCE(v_muted_pay_batch_ids, '[]'::jsonb)) AS muted_batch_entry(value)
  LOOP
    IF v_value_text !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
      RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_INVALID_MUTED_BATCH_ID'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_INVALID_MUTED_BATCH_ID', 'pay_batch_id', v_value_text)::text;
    END IF;
  END LOOP;

  IF v_payload ? 'snoozed_until_utc' THEN
    IF COALESCE(jsonb_typeof(v_payload -> 'snoozed_until_utc'), 'null') = 'null' THEN
      v_snoozed_until_utc := NULL::timestamptz;
    ELSIF jsonb_typeof(v_payload -> 'snoozed_until_utc') = 'string' THEN
      BEGIN
        v_snoozed_until_utc := NULLIF(BTRIM(v_payload ->> 'snoozed_until_utc'), '')::timestamptz;
      EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_INVALID_SNOOZE_TIMESTAMP'
          USING ERRCODE = 'P0001',
                DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_INVALID_SNOOZE_TIMESTAMP')::text;
      END;
    ELSE
      RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_SNOOZE_MUST_BE_STRING_OR_NULL'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_SNOOZE_MUST_BE_STRING_OR_NULL')::text;
    END IF;

    IF v_snoozed_until_utc IS NOT NULL AND v_snoozed_until_utc > now() + interval '366 days' THEN
      RAISE EXCEPTION 'BANKING_ALERT_PREFERENCES_SNOOZE_TOO_FAR_IN_FUTURE'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'BANKING_ALERT_PREFERENCES_SNOOZE_TOO_FAR_IN_FUTURE')::text;
    END IF;
  END IF;

  IF v_mode = 'NO_BANKING_PAY_ALERTS' THEN
    v_enabled := false;
  ELSIF v_mode = 'ALL_ACTION_REQUIRED' THEN
    v_enabled := true;
    v_include_action_required := true;
    IF NOT (v_payload ? 'failure_reason_allowlist') AND NOT (v_payload ? 'failure_reason_groups') THEN
      v_failure_reason_allowlist := NULL::jsonb;
    END IF;
  ELSIF v_mode = 'SELECTED_FAILURE_REASONS' THEN
    v_enabled := true;
    v_include_action_required := true;
    IF NOT (v_payload ? 'failure_reason_allowlist') AND NOT (v_payload ? 'failure_reason_groups') THEN
      v_failure_reason_allowlist := COALESCE(v_failure_reason_allowlist, '[]'::jsonb);
    END IF;
  END IF;

  INSERT INTO public.banking_alert_user_preferences AS preference_target (
    user_id,
    enabled,
    alert_kind_allowlist,
    alert_kind_blocklist,
    failure_reason_allowlist,
    failure_reason_blocklist,
    include_action_required,
    include_progress_alerts,
    include_informational_alerts,
    include_success_alerts,
    severity_min,
    muted_provider_keys,
    muted_pay_batch_ids,
    snoozed_until_utc,
    created_at_utc,
    updated_at_utc
  )
  VALUES (
    p_user_id,
    v_enabled,
    v_alert_kind_allowlist,
    v_alert_kind_blocklist,
    v_failure_reason_allowlist,
    v_failure_reason_blocklist,
    v_include_action_required,
    v_include_progress_alerts,
    v_include_informational_alerts,
    v_include_success_alerts,
    v_severity_min,
    v_muted_provider_keys,
    v_muted_pay_batch_ids,
    v_snoozed_until_utc,
    now(),
    now()
  )
  ON CONFLICT (user_id) DO UPDATE
  SET
    enabled = EXCLUDED.enabled,
    alert_kind_allowlist = EXCLUDED.alert_kind_allowlist,
    alert_kind_blocklist = EXCLUDED.alert_kind_blocklist,
    failure_reason_allowlist = EXCLUDED.failure_reason_allowlist,
    failure_reason_blocklist = EXCLUDED.failure_reason_blocklist,
    include_action_required = EXCLUDED.include_action_required,
    include_progress_alerts = EXCLUDED.include_progress_alerts,
    include_informational_alerts = EXCLUDED.include_informational_alerts,
    include_success_alerts = EXCLUDED.include_success_alerts,
    severity_min = EXCLUDED.severity_min,
    muted_provider_keys = EXCLUDED.muted_provider_keys,
    muted_pay_batch_ids = EXCLUDED.muted_pay_batch_ids,
    snoozed_until_utc = EXCLUDED.snoozed_until_utc,
    updated_at_utc = now()
  RETURNING id
  INTO v_preference_id;

  v_result := public.banking_alert_preferences_get(p_user_id);

  RETURN v_result || jsonb_build_object(
    'updated', true,
    'preference_id', v_preference_id::text,
    'nav_refresh_recommended', true
  );
END;
$function$;

-- banking_alert_signal_for_user(uuid,text,text)
CREATE OR REPLACE FUNCTION public.banking_alert_signal_for_user(p_actor_user_id uuid, p_last_alert_hash text DEFAULT NULL::text, p_alert_context text DEFAULT 'CACHED'::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_last_alert_hash text := NULLIF(BTRIM(COALESCE(p_last_alert_hash, '')), '');
  v_alert_context text := UPPER(REPLACE(NULLIF(BTRIM(COALESCE(p_alert_context, 'CACHED')), ''), '-', '_'));
  v_alert_hash text := 'banking_alert_signal:v3:' || MD5('');
  v_summary_hash text := 'banking_alert_summary:v3:' || MD5('');
  v_unacknowledged_count integer := 0;
  v_highest_severity text := NULL::text;
  v_highest_label text := NULL::text;
  v_summary_json jsonb := '{}'::jsonb;
  v_live_json jsonb := NULL::jsonb;
BEGIN
  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'BANKING_ALERT_SIGNAL_FOR_USER_ACTOR_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'BANKING_ALERT_SIGNAL_FOR_USER_ACTOR_REQUIRED')::text;
  END IF;

  IF v_alert_context IN (
    'ALERT_PANEL',
    'ALERTS_PANEL',
    'ALERT_MANAGEMENT',
    'ALERT_REFRESH_JOB',
    'EXPLICIT_ALERT_REFRESH',
    'USER_TRIGGERED_ALERTS'
  ) THEN
    v_live_json := public.banking_alerts_refresh_for_user(
      p_actor_user_id,
      v_alert_context,
      100
    );

    v_alert_hash := COALESCE(NULLIF(BTRIM(COALESCE(v_live_json->>'banking_alert_hash', '')), ''), v_alert_hash);
    v_summary_hash := COALESCE(NULLIF(BTRIM(COALESCE(v_live_json->>'banking_alert_summary_signature', '')), ''), v_summary_hash);

    BEGIN
      v_unacknowledged_count := COALESCE((v_live_json->>'banking_unacknowledged_alert_count')::integer, 0);
    EXCEPTION WHEN OTHERS THEN
      v_unacknowledged_count := 0;
    END;

    v_highest_severity := NULLIF(BTRIM(COALESCE(v_live_json->>'banking_highest_alert_severity', v_live_json->>'highest_severity', '')), '');
    v_highest_label := NULLIF(BTRIM(COALESCE(v_live_json->>'banking_highest_alert_label', v_live_json->>'highest_label', '')), '');
    v_summary_json := COALESCE(v_live_json, '{}'::jsonb);
  ELSE
    SELECT
      COALESCE(alert_summary.alert_hash, v_alert_hash),
      COALESCE(alert_summary.summary_hash, v_summary_hash),
      COALESCE(alert_summary.unacknowledged_count, 0),
      alert_summary.highest_severity,
      alert_summary.highest_label,
      COALESCE(alert_summary.summary_json, '{}'::jsonb)
    INTO
      v_alert_hash,
      v_summary_hash,
      v_unacknowledged_count,
      v_highest_severity,
      v_highest_label,
      v_summary_json
    FROM public.banking_alert_display_summary AS alert_summary
    WHERE alert_summary.actor_user_id = p_actor_user_id;

    IF NOT FOUND THEN
      v_alert_hash := 'banking_alert_signal:v3:' || MD5('');
      v_summary_hash := 'banking_alert_summary:v3:' || MD5('');
      v_unacknowledged_count := 0;
      v_highest_severity := NULL::text;
      v_highest_label := NULL::text;
      v_summary_json := '{}'::jsonb;
    END IF;
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'generated_at_utc', now()::text,
    'actor_user_id', p_actor_user_id::text,
    'cached', v_live_json IS NULL,
    'alert_context', v_alert_context,
    'banking_alert_hash', v_alert_hash,
    'banking_alert_summary_signature', v_summary_hash,
    'banking_alert_summary_changed', COALESCE(v_last_alert_hash IS DISTINCT FROM v_alert_hash, true),
    'banking_unacknowledged_alert_count', COALESCE(v_unacknowledged_count, 0),
    'grouped_banking_unacknowledged_alert_count', COALESCE(v_unacknowledged_count, 0),
    'banking_highest_alert_severity', COALESCE(v_highest_severity, ''),
    'banking_highest_alert_label', COALESCE(v_highest_label, ''),
    'highest_severity', COALESCE(v_highest_severity, ''),
    'highest_label', COALESCE(v_highest_label, ''),
    'summary_is_grouped', true,
    'summary_json', COALESCE(v_summary_json, '{}'::jsonb)
  );
END;
$function$;

-- banking_alert_success_event_capture_pay_batch()
CREATE OR REPLACE FUNCTION public.banking_alert_success_event_capture_pay_batch()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_status text := UPPER(BTRIM(COALESCE(NEW.status, '')));
  v_schedule_kind text := UPPER(BTRIM(COALESCE(NEW.schedule_kind, '')));
  v_execution_mode text := UPPER(BTRIM(COALESCE(
    NEW.execution_intent_json ->> 'execution_mode',
    NEW.bank_csv_export_json ->> 'execution_mode',
    NEW.settlement_confirmation_json ->> 'execution_mode',
    ''
  )));
  v_is_csv_settlement boolean := false;
  v_became_future_scheduled boolean := false;
  v_became_settled boolean := false;
  v_total_bank_out numeric(14,2) := 0::numeric;
  v_individual_payment_count integer := 0;
  v_event_key text := NULL::text;
  v_event_at_utc timestamptz := now();
  v_local_at timestamp without time zone := NULL::timestamp;
  v_local_date_label text := NULL::text;
  v_local_time_label text := NULL::text;
  v_amount_label text := '£0.00'::text;
  v_payment_count_label text := '0 individual payments'::text;
  v_user_label text := NULL::text;
  v_user_description text := NULL::text;
  v_alert_kind text := NULL::text;
BEGIN
  v_is_csv_settlement := v_execution_mode LIKE 'CSV%'
    OR COALESCE(jsonb_typeof(NEW.bank_csv_export_json), 'null') = 'object'
       AND NEW.bank_csv_export_json <> '{}'::jsonb;

  v_became_future_scheduled := v_status = 'SCHEDULED'
    AND v_schedule_kind = 'SCHEDULED'
    AND NEW.scheduled_at_utc IS NOT NULL
    AND NEW.scheduled_at_utc > now()
    AND v_is_csv_settlement IS NOT TRUE
    AND (
      TG_OP = 'INSERT'
      OR CASE WHEN TG_OP = 'UPDATE' THEN
        UPPER(BTRIM(COALESCE(OLD.status, ''))) IS DISTINCT FROM v_status
        OR UPPER(BTRIM(COALESCE(OLD.schedule_kind, ''))) IS DISTINCT FROM v_schedule_kind
        OR OLD.scheduled_at_utc IS DISTINCT FROM NEW.scheduled_at_utc
      ELSE false END
    );

  v_became_settled := v_status = 'SETTLED'
    AND (
      TG_OP = 'INSERT'
      OR CASE WHEN TG_OP = 'UPDATE' THEN
        UPPER(BTRIM(COALESCE(OLD.status, ''))) IS DISTINCT FROM 'SETTLED'
      ELSE false END
    );

  IF v_became_future_scheduled IS NOT TRUE AND v_became_settled IS NOT TRUE THEN
    RETURN NEW;
  END IF;

  SELECT
    COALESCE(
      NULLIF(NEW.total_bank_out, 0::numeric),
      NULLIF(display_summary.total_bank_out, 0::numeric),
      transfer_summary.total_bank_out,
      0::numeric
    )::numeric(14,2),
    COALESCE(
      NULLIF(display_summary.transfer_count, 0),
      transfer_summary.transfer_count,
      0
    )::integer
  INTO
    v_total_bank_out,
    v_individual_payment_count
  FROM (SELECT 1) AS one_row
  LEFT JOIN public.pay_batch_display_summary AS display_summary
    ON display_summary.pay_batch_id = NEW.id
  LEFT JOIN LATERAL (
    SELECT
      COUNT(*)::integer AS transfer_count,
      COALESCE(SUM(pay_bank_transfer.amount), 0::numeric)::numeric(14,2) AS total_bank_out
    FROM public.pay_bank_transfers AS pay_bank_transfer
    WHERE pay_bank_transfer.pay_batch_id = NEW.id
  ) AS transfer_summary ON true;

  v_amount_label := '£' || TO_CHAR(COALESCE(v_total_bank_out, 0::numeric), 'FM999,999,999,990.00');
  v_payment_count_label := COALESCE(v_individual_payment_count, 0)::text
    || CASE WHEN COALESCE(v_individual_payment_count, 0) = 1 THEN ' individual payment' ELSE ' individual payments' END;

  IF v_became_future_scheduled THEN
    v_alert_kind := 'BATCH_SCHEDULED_SUCCESS';
    v_event_at_utc := now();
    v_event_key := 'SCHEDULED:' || TO_CHAR(NEW.scheduled_at_utc AT TIME ZONE 'UTC', 'YYYYMMDDHH24MISSUS');
    v_local_at := NEW.scheduled_at_utc AT TIME ZONE 'Europe/London';
    v_local_date_label := TO_CHAR(v_local_at, 'FMDD') || ' ' || TO_CHAR(v_local_at, 'FMMonth') || ' ' || TO_CHAR(v_local_at, 'YYYY');
    v_local_time_label := TO_CHAR(v_local_at, 'HH24:MI');
    v_user_label := 'Future payment batch scheduled';
    v_user_description := 'Payment batch scheduled successfully. '
      || v_amount_label || ' will be paid across ' || v_payment_count_label
      || ' on ' || v_local_date_label || ' at ' || v_local_time_label || ' (UK Time).';
  ELSE
    v_alert_kind := 'BATCH_SETTLED_SUCCESS';
    v_event_at_utc := COALESCE(NEW.completed_at_utc, now());
    v_event_key := 'SETTLED:' || TO_CHAR(v_event_at_utc AT TIME ZONE 'UTC', 'YYYYMMDDHH24MISSUS');
    v_local_at := v_event_at_utc AT TIME ZONE 'Europe/London';
    v_local_date_label := TO_CHAR(v_local_at, 'FMDD') || ' ' || TO_CHAR(v_local_at, 'FMMonth') || ' ' || TO_CHAR(v_local_at, 'YYYY');
    v_local_time_label := TO_CHAR(v_local_at, 'HH24:MI');
    IF v_is_csv_settlement THEN
      v_user_label := 'CSV settlement recorded';
      v_user_description := 'CSV settlement recorded successfully. ' || v_amount_label || ' across '
        || v_payment_count_label || ' was marked settled on ' || v_local_date_label || ' at '
        || v_local_time_label || ' (UK Time). This records settlement only; CloudTMS did not transfer the money.';
    ELSE
      v_user_label := 'Payment batch settled';
      v_user_description := 'Payment batch settled successfully. ' || v_amount_label || ' across '
        || v_payment_count_label || ' completed on ' || v_local_date_label || ' at '
        || v_local_time_label || ' (UK Time).';
    END IF;
  END IF;

  INSERT INTO public.banking_alert_success_events (
    pay_batch_id,
    alert_kind,
    event_key,
    payload_json,
    occurred_at_utc,
    expires_at_utc
  )
  VALUES (
    NEW.id,
    v_alert_kind,
    v_event_key,
    jsonb_strip_nulls(jsonb_build_object(
      'alert_kind', v_alert_kind,
      'issue_kind', v_alert_kind,
      'alert_severity', 'info',
      'severity', 'info',
      'alert_candidate_is_success_only', true,
      'is_success_only', true,
      'stable_issue_key', NEW.id::text || ':' || v_alert_kind || ':' || v_event_key,
      'dedupe_key', NEW.id::text || ':' || v_alert_kind || ':' || v_event_key,
      'pay_batch_id', NEW.id::text,
      'entity_kind', 'pay_batch',
      'entity_id', NEW.id::text,
      'payment_lifecycle_state', CASE WHEN v_alert_kind = 'BATCH_SCHEDULED_SUCCESS' THEN 'SCHEDULED' ELSE 'SETTLED' END,
      'current_status', v_status,
      'schedule_kind', v_schedule_kind,
      'scheduled_at_utc', CASE WHEN NEW.scheduled_at_utc IS NULL THEN NULL::text ELSE NEW.scheduled_at_utc::text END,
      'settled_at_utc', CASE WHEN v_alert_kind = 'BATCH_SETTLED_SUCCESS' THEN v_event_at_utc::text ELSE NULL::text END,
      'execution_mode', NULLIF(v_execution_mode, ''),
      'csv_settlement', v_is_csv_settlement,
      'amount_gbp', v_total_bank_out,
      'individual_payment_count', v_individual_payment_count,
      'uk_date_label', v_local_date_label,
      'uk_time_label', v_local_time_label,
      'user_label', v_user_label,
      'user_description', v_user_description,
      'required_user_action', 'Review or clear this Banking alert.',
      'link_target', 'banking_pay_batch',
      'link_tab', 'current_payment_status',
      'policy_x_source', 'FROZEN_BATCH_ARTIFACTS'
    )),
    v_event_at_utc,
    v_event_at_utc + interval '365 days'
  )
  ON CONFLICT (pay_batch_id, alert_kind, event_key) DO NOTHING;

  RETURN NEW;
EXCEPTION
  WHEN OTHERS THEN
    RAISE WARNING 'BANKING_ALERT_SUCCESS_EVENT_CAPTURE_FAILED [%]', SQLSTATE;
    RETURN NEW;
END;
$function$;

-- banking_alerts_active_for_user(uuid,text,uuid,boolean,integer,text)
CREATE OR REPLACE FUNCTION public.banking_alerts_active_for_user(p_actor_user_id uuid, p_entity_kind text DEFAULT NULL::text, p_entity_id uuid DEFAULT NULL::uuid, p_include_acknowledged boolean DEFAULT false, p_limit integer DEFAULT 100, p_alert_context text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_entity_kind text := LOWER(NULLIF(BTRIM(COALESCE(p_entity_kind, '')), ''));
  v_limit integer := NULL::integer;
  v_result jsonb := '{}'::jsonb;
  v_alert_context text := UPPER(REPLACE(NULLIF(BTRIM(COALESCE(p_alert_context, '')), ''), '-', '_'));
BEGIN
  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'BANKING_ALERTS_ACTIVE_FOR_USER_ACTOR_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'BANKING_ALERTS_ACTIVE_FOR_USER_ACTOR_REQUIRED')::text;
  END IF;

  IF v_alert_context IS NULL THEN
    RAISE EXCEPTION 'BANKING_ALERTS_ACTIVE_FOR_USER_CONTEXT_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'BANKING_ALERTS_ACTIVE_FOR_USER_CONTEXT_REQUIRED')::text;
  END IF;

  IF v_alert_context IN (
    'LIST','BATCH_LIST','PAY_BATCHES_LIST','BOOTSTRAP','BATCH_BOOTSTRAP','PAY_BATCH_GET_BOOTSTRAP_ONLY',
    'WATCH','WATCH_SIGNAL','LIVE_WATCH','PROGRESS','PROGRESS_POLLING','PREVIEW','PREVIEW_OPEN',
    'PREVIEW_PROGRESS','OPERATION_GET','OPERATION_PROGRESS','RPC_CHANGES_PING','CHANGES_PING'
  ) OR v_alert_context NOT IN (
    'ALERT_PANEL','ALERTS_PANEL','ALERT_MANAGEMENT','ALERT_REFRESH_JOB','EXPLICIT_ALERT_REFRESH','USER_TRIGGERED_ALERTS'
  ) THEN
    RAISE EXCEPTION 'BANKING_ALERTS_ACTIVE_FOR_USER_CONTEXT_NOT_ALLOWED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'BANKING_ALERTS_ACTIVE_FOR_USER_CONTEXT_NOT_ALLOWED', 'context', v_alert_context)::text;
  END IF;

  IF p_limit IS NULL THEN
    v_limit := 100;
  ELSIF p_limit = 0 THEN
    v_limit := NULL::integer;
  ELSE
    v_limit := LEAST(GREATEST(p_limit, 1), 500);
  END IF;

  WITH blocked_funds_base AS (
    SELECT
      blocked_funds_pay_batches.id AS pay_batch_id,
      blocked_funds_pay_batches.status AS batch_status,
      blocked_funds_pay_batches.execution_commit_state AS execution_commit_state,
      blocked_funds_pay_batches.execution_commit_ref AS execution_commit_ref,
      blocked_funds_pay_batches.execution_committed_at_utc AS execution_committed_at_utc,
      blocked_funds_pay_batches.last_funds_check_at_utc AS last_funds_check_at_utc,
      blocked_funds_pay_batches.last_funds_check_json AS last_funds_check_json,
      blocked_funds_pay_batches.funding_account_ref AS funding_account_ref,
      blocked_funds_pay_batches.rail_provider_snapshot AS rail_provider_snapshot,
      blocked_funds_pay_batches.rail_env_snapshot AS rail_env_snapshot,
      COALESCE(
        NULLIF(BTRIM(blocked_funds_pay_batches.last_funds_check_json #>> '{required_gbp}'), ''),
        NULLIF(BTRIM(blocked_funds_pay_batches.last_funds_check_json #>> '{required}'), ''),
        NULLIF(BTRIM(blocked_funds_pay_batches.last_funds_check_json #>> '{required_amount_gbp}'), ''),
        NULLIF(BTRIM(blocked_funds_pay_batches.last_funds_check_json #>> '{required_amount}'), '')
      ) AS required_gbp_text,
      COALESCE(
        NULLIF(BTRIM(blocked_funds_pay_batches.last_funds_check_json #>> '{available_gbp}'), ''),
        NULLIF(BTRIM(blocked_funds_pay_batches.last_funds_check_json #>> '{available}'), ''),
        NULLIF(BTRIM(blocked_funds_pay_batches.last_funds_check_json #>> '{available_amount_gbp}'), ''),
        NULLIF(BTRIM(blocked_funds_pay_batches.last_funds_check_json #>> '{available_amount}'), '')
      ) AS available_gbp_text,
      COALESCE(
        NULLIF(BTRIM(blocked_funds_pay_batches.last_funds_check_json #>> '{funding_account_ref}'), ''),
        NULLIF(BTRIM(blocked_funds_pay_batches.last_funds_check_json #>> '{account_ref}'), ''),
        NULLIF(BTRIM(blocked_funds_pay_batches.last_funds_check_json #>> '{funding_account_id}'), ''),
        NULLIF(BTRIM(blocked_funds_pay_batches.funding_account_ref), '')
      ) AS resolved_funding_account_ref,
      COALESCE(
        NULLIF(BTRIM(blocked_funds_pay_batches.last_funds_check_json #>> '{rail_provider}'), ''),
        NULLIF(BTRIM(blocked_funds_pay_batches.last_funds_check_json #>> '{provider}'), ''),
        NULLIF(BTRIM(blocked_funds_pay_batches.rail_provider_snapshot), '')
      ) AS resolved_rail_provider,
      COALESCE(
        NULLIF(BTRIM(blocked_funds_pay_batches.last_funds_check_json #>> '{rail_env}'), ''),
        NULLIF(BTRIM(blocked_funds_pay_batches.last_funds_check_json #>> '{env}'), ''),
        NULLIF(BTRIM(blocked_funds_pay_batches.rail_env_snapshot), '')
      ) AS resolved_rail_env
    FROM public.pay_batches AS blocked_funds_pay_batches
    WHERE UPPER(BTRIM(COALESCE(blocked_funds_pay_batches.status, ''))) = 'BLOCKED_FUNDS'
      AND UPPER(BTRIM(COALESCE(blocked_funds_pay_batches.execution_commit_state, 'NOT_SUBMITTED'))) = 'NOT_SUBMITTED'
      AND NULLIF(BTRIM(COALESCE(blocked_funds_pay_batches.execution_commit_ref, '')), '') IS NULL
      AND blocked_funds_pay_batches.execution_committed_at_utc IS NULL
      AND blocked_funds_pay_batches.cancelled_at_utc IS NULL
      AND EXISTS (
        SELECT 1
        FROM public.pay_batch_candidates AS blocked_funds_candidate_exists
        JOIN public.pay_batch_items AS blocked_funds_item_exists
          ON blocked_funds_item_exists.pay_batch_candidate_id = blocked_funds_candidate_exists.id
        WHERE blocked_funds_candidate_exists.pay_batch_id = blocked_funds_pay_batches.id
          AND COALESCE(blocked_funds_item_exists.is_voided, false) = false
      )
  ),
  blocked_funds_alerts AS MATERIALIZED (
    SELECT
      'PROVIDER_OUTAGE_RETRY_LATER'::text AS alert_kind,
      'critical'::text AS severity,
      100::integer AS severity_rank,
      'pay_batch'::text AS entity_kind,
      blocked_funds_base.pay_batch_id AS entity_id,
      blocked_funds_base.pay_batch_id AS pay_batch_id,
      NULL::text AS payload_source_kind,
      NULL::uuid AS payload_source_id,
      jsonb_strip_nulls(jsonb_build_object(
        'fingerprint_source_kind', 'pay_batch',
        'fingerprint_source_id', blocked_funds_base.pay_batch_id::text,
        'issue_kind', 'PROVIDER_OUTAGE_RETRY_LATER',
        'pay_batch_id', blocked_funds_base.pay_batch_id::text,
        'batch_status', UPPER(BTRIM(COALESCE(blocked_funds_base.batch_status, ''))),
        'execution_commit_state', UPPER(BTRIM(COALESCE(blocked_funds_base.execution_commit_state, 'NOT_SUBMITTED'))),
        'execution_commit_ref', NULLIF(BTRIM(COALESCE(blocked_funds_base.execution_commit_ref, '')), ''),
        'execution_committed_at_utc', CASE
          WHEN blocked_funds_base.execution_committed_at_utc IS NULL THEN NULL::text
          ELSE TO_CHAR(blocked_funds_base.execution_committed_at_utc AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
        END,
        'last_funds_check_at_utc', CASE
          WHEN blocked_funds_base.last_funds_check_at_utc IS NULL THEN NULL::text
          ELSE TO_CHAR(blocked_funds_base.last_funds_check_at_utc AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
        END,
        'required_gbp', CASE
          WHEN blocked_funds_base.required_gbp_text ~ '^-?[0-9]+(\.[0-9]+)?$' THEN blocked_funds_base.required_gbp_text::numeric
          ELSE NULL::numeric
        END,
        'available_gbp', CASE
          WHEN blocked_funds_base.available_gbp_text ~ '^-?[0-9]+(\.[0-9]+)?$' THEN blocked_funds_base.available_gbp_text::numeric
          ELSE NULL::numeric
        END,
        'funding_account_ref', blocked_funds_base.resolved_funding_account_ref,
        'rail_provider', blocked_funds_base.resolved_rail_provider,
        'rail_env', blocked_funds_base.resolved_rail_env
      )) AS fingerprint_payload_json,
      'Bank unavailable — unsent payments can be retried'::text AS label,
      'Bank unavailable — unsent payments can be retried'::text AS title,
      ('Bank unavailable — required '
        || COALESCE(NULLIF(BTRIM(blocked_funds_base.required_gbp_text), ''), '—')
        || ', available '
        || COALESCE(NULLIF(BTRIM(blocked_funds_base.available_gbp_text), ''), '—')
        || '. Payments were not sent to the bank/provider and can be retried from Overview.')::text AS description,
      'Open Banking Pay Overview and retry unsent payments.'::text AS action_guidance,
      blocked_funds_base.last_funds_check_at_utc AS sort_at_utc
    FROM blocked_funds_base
  ),
  bank_event_base AS MATERIALIZED (
    SELECT
      CASE
        WHEN (
            UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.provider_failure_reason_group, ''))) = 'WEBHOOK_UNMATCHED'
            OR (
              UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.mapping_status, ''))) IN ('UNMATCHED','NO_MATCH')
              AND UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.provider_event_transport, public.pay_bank_transfer_events.event_source, ''))) IN ('PROVIDER_WEBHOOK','FAILED_WEBHOOK_REPLAY','WEBHOOK')
            )
          )
          THEN 'WEBHOOK_UNMATCHED_REVIEW_REQUIRED'
        WHEN UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.mapping_status, ''))) IN ('AMBIGUOUS','UNMATCHED','NO_MATCH','MULTIPLE_MATCHES')
          OR UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.correction_disposition, ''))) IN ('AMBIGUOUS','ACTION_REQUIRED')
          THEN 'AMBIGUOUS_PAYMENT_REVIEW_REQUIRED'
        WHEN UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.correction_disposition, ''))) = 'FAILED'
          THEN 'PAYMENT_CORRECTION_FAILED'
        WHEN UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.correction_disposition, ''))) = 'BLOCKED'
          THEN 'PAYMENT_CORRECTION_BLOCKED'
        WHEN UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.normalised_state, ''))) IN ('RETURNED','REVERTED')
          OR UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.provider_state, ''))) IN ('RETURNED','REVERTED')
          THEN 'BANK_RETURNED_PAYMENT'
        WHEN UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.normalised_state, ''))) IN ('FAILED','DECLINED','REJECTED','CANCELLED','CANCELED','SUBMISSION_FAILED')
          OR UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.provider_state, ''))) IN ('FAILED','DECLINED','REJECTED','CANCELLED','CANCELED','SUBMISSION_FAILED')
          THEN 'BANK_REJECTED_PAYMENT'
        WHEN UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.normalised_state, ''))) IN ('UNKNOWN','TIMEOUT','TIMED_OUT','PENDING_REVIEW')
          OR UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.normalised_state, ''))) LIKE 'CREATE_ERROR%'
          OR UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.provider_state, ''))) IN ('UNKNOWN','TIMEOUT','TIMED_OUT','PENDING_REVIEW')
          OR UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.provider_state, ''))) LIKE 'CREATE_ERROR%'
          THEN 'RAIL_SUBMISSION_UNKNOWN_OR_TIMEOUT'
        ELSE NULL::text
      END AS alert_kind,
      public.pay_bank_transfer_events.pay_batch_id AS pay_batch_id,
      public.pay_bank_transfer_events.id AS source_id,
      public.pay_bank_transfer_events.normalised_state AS normalised_state,
      public.pay_bank_transfer_events.provider_state AS provider_state,
      public.pay_bank_transfer_events.mapping_status AS mapping_status,
      public.pay_bank_transfer_events.correction_disposition AS correction_disposition,
      public.pay_bank_transfer_events.event_time_utc AS event_time_utc,
      public.pay_bank_transfer_events.received_at_utc AS received_at_utc,
      public.pay_bank_transfer_events.created_at_utc AS created_at_utc,
      COALESCE(NULLIF(BTRIM(COALESCE(public.pay_bank_transfer_events.provider_key, '')), ''), NULLIF(BTRIM(COALESCE(bank_event_batches.rail_provider_snapshot, '')), ''), 'UNKNOWN_PROVIDER') AS provider_key,
      COALESCE(NULLIF(BTRIM(COALESCE(public.pay_bank_transfer_events.rail_env, '')), ''), NULLIF(BTRIM(COALESCE(bank_event_batches.rail_env_snapshot, '')), ''), 'PROD') AS rail_env,
      NULLIF(BTRIM(COALESCE(public.pay_bank_transfer_events.provider_event_key, '')), '') AS provider_event_key,
      NULLIF(BTRIM(COALESCE(public.pay_bank_transfer_events.provider_failure_reason_code, '')), '') AS provider_failure_reason_code,
      NULLIF(BTRIM(COALESCE(public.pay_bank_transfer_events.provider_failure_reason_group, '')), '') AS provider_failure_reason_group,
      public.pay_bank_transfer_events.provider_webhook_receipt_id AS provider_webhook_receipt_id,
      COALESCE(public.pay_bank_transfer_events.event_time_utc, public.pay_bank_transfer_events.received_at_utc, public.pay_bank_transfer_events.created_at_utc) AS sort_at_utc
    FROM public.pay_bank_transfer_events
    JOIN public.pay_batches AS bank_event_batches
      ON bank_event_batches.id = public.pay_bank_transfer_events.pay_batch_id
    WHERE UPPER(BTRIM(COALESCE(bank_event_batches.status, ''))) NOT IN ('CANCELLED','CANCELED')
      AND (
        UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.normalised_state, ''))) IN ('FAILED','DECLINED','REJECTED','CANCELLED','CANCELED','SUBMISSION_FAILED','RETURNED','REVERTED','UNKNOWN','TIMEOUT','TIMED_OUT','PENDING_REVIEW')
        OR UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.normalised_state, ''))) LIKE 'CREATE_ERROR%'
        OR UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.provider_state, ''))) IN ('FAILED','DECLINED','REJECTED','CANCELLED','CANCELED','SUBMISSION_FAILED','RETURNED','REVERTED','UNKNOWN','TIMEOUT','TIMED_OUT','PENDING_REVIEW')
        OR UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.provider_state, ''))) LIKE 'CREATE_ERROR%'
        OR UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.mapping_status, ''))) IN ('AMBIGUOUS','UNMATCHED','NO_MATCH','MULTIPLE_MATCHES')
        OR UPPER(BTRIM(COALESCE(public.pay_bank_transfer_events.correction_disposition, ''))) IN ('AMBIGUOUS','ACTION_REQUIRED','BLOCKED','FAILED')
      )
      AND NOT EXISTS (
        SELECT 1
        FROM public.pay_payment_correction_requests AS resolved_event_corrections
        WHERE resolved_event_corrections.source_bank_event_id = public.pay_bank_transfer_events.id
          AND UPPER(BTRIM(COALESCE(resolved_event_corrections.status, ''))) IN ('APPLIED','RESOLVED')
      )
  ),
  bank_event_alerts AS MATERIALIZED (
    SELECT
      bank_event_base.alert_kind,
      'critical'::text AS severity,
      CASE bank_event_base.alert_kind
        WHEN 'WEBHOOK_UNMATCHED_REVIEW_REQUIRED' THEN 96
        WHEN 'BANK_RETURNED_PAYMENT' THEN 95
        WHEN 'BANK_REJECTED_PAYMENT' THEN 94
        WHEN 'AMBIGUOUS_PAYMENT_REVIEW_REQUIRED' THEN 90
        WHEN 'PAYMENT_CORRECTION_FAILED' THEN 88
        WHEN 'PAYMENT_CORRECTION_BLOCKED' THEN 87
        ELSE 80
      END::integer AS severity_rank,
      'pay_batch'::text AS entity_kind,
      bank_event_base.pay_batch_id AS entity_id,
      bank_event_base.pay_batch_id AS pay_batch_id,
      'pay_bank_transfer_event'::text AS payload_source_kind,
      bank_event_base.source_id AS payload_source_id,
      jsonb_strip_nulls(jsonb_build_object(
        'fingerprint_source_kind', 'pay_bank_transfer_event',
        'fingerprint_source_id', bank_event_base.source_id::text,
        'issue_kind', bank_event_base.alert_kind,
        'alert_kind', bank_event_base.alert_kind,
        'pay_batch_id', bank_event_base.pay_batch_id::text,
        'provider_key', bank_event_base.provider_key,
        'rail_provider', bank_event_base.provider_key,
        'rail_env', bank_event_base.rail_env,
        'provider_event_key', bank_event_base.provider_event_key,
        'provider_webhook_receipt_id', CASE WHEN bank_event_base.provider_webhook_receipt_id IS NULL THEN NULL ELSE bank_event_base.provider_webhook_receipt_id::text END,
        'provider_failure_reason_code', bank_event_base.provider_failure_reason_code,
        'provider_failure_reason_group', CASE WHEN bank_event_base.alert_kind = 'WEBHOOK_UNMATCHED_REVIEW_REQUIRED' THEN COALESCE(bank_event_base.provider_failure_reason_group, 'WEBHOOK_UNMATCHED') ELSE bank_event_base.provider_failure_reason_group END,
        'normalised_state', NULLIF(BTRIM(COALESCE(bank_event_base.normalised_state, '')), ''),
        'provider_state', NULLIF(BTRIM(COALESCE(bank_event_base.provider_state, '')), ''),
        'mapping_status', NULLIF(BTRIM(COALESCE(bank_event_base.mapping_status, '')), ''),
        'correction_disposition', NULLIF(BTRIM(COALESCE(bank_event_base.correction_disposition, '')), ''),
        'event_time_utc', CASE WHEN bank_event_base.event_time_utc IS NULL THEN NULL::text ELSE TO_CHAR(bank_event_base.event_time_utc AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') END,
        'received_at_utc', CASE WHEN bank_event_base.received_at_utc IS NULL THEN NULL::text ELSE TO_CHAR(bank_event_base.received_at_utc AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') END
      )) AS fingerprint_payload_json,
      CASE bank_event_base.alert_kind
        WHEN 'WEBHOOK_UNMATCHED_REVIEW_REQUIRED' THEN 'Unmatched bank webhook'
        WHEN 'BANK_RETURNED_PAYMENT' THEN 'Provider outcome unknown — check provider'
        WHEN 'BANK_REJECTED_PAYMENT' THEN 'Failed payments — Rewind financials available'
        WHEN 'AMBIGUOUS_PAYMENT_REVIEW_REQUIRED' THEN 'Payment needs review'
        WHEN 'PAYMENT_CORRECTION_FAILED' THEN 'Payment correction failed'
        WHEN 'PAYMENT_CORRECTION_BLOCKED' THEN 'Payment correction blocked'
        ELSE 'Rail submission needs review'
      END::text AS label,
      CASE bank_event_base.alert_kind
        WHEN 'WEBHOOK_UNMATCHED_REVIEW_REQUIRED' THEN 'Unmatched provider webhook needs review'
        WHEN 'BANK_RETURNED_PAYMENT' THEN 'Provider outcome unknown — check provider'
        WHEN 'BANK_REJECTED_PAYMENT' THEN 'Failed payments — Rewind financials available'
        WHEN 'AMBIGUOUS_PAYMENT_REVIEW_REQUIRED' THEN 'Payment event needs review'
        WHEN 'PAYMENT_CORRECTION_FAILED' THEN 'Payment correction failed'
        WHEN 'PAYMENT_CORRECTION_BLOCKED' THEN 'Payment correction blocked'
        ELSE 'Rail submission needs review'
      END::text AS title,
      CASE bank_event_base.alert_kind
        WHEN 'WEBHOOK_UNMATCHED_REVIEW_REQUIRED' THEN 'A verified provider webhook could not be matched safely and needs review.'
        WHEN 'BANK_RETURNED_PAYMENT' THEN 'Provider returned/reverted status requires checking before any financial correction.'
        WHEN 'BANK_REJECTED_PAYMENT' THEN 'The provider/bank outcome indicates no money moved. Open Current Payment Status and rewind financials where safe.'
        WHEN 'AMBIGUOUS_PAYMENT_REVIEW_REQUIRED' THEN 'A bank payment event could not be matched safely and needs review.'
        WHEN 'PAYMENT_CORRECTION_FAILED' THEN 'CloudTMS could not complete a payment correction automatically.'
        WHEN 'PAYMENT_CORRECTION_BLOCKED' THEN 'A payment correction is blocked and needs review.'
        ELSE 'CloudTMS could not confirm the final bank submission state.'
      END::text AS description,
      CASE bank_event_base.alert_kind
        WHEN 'WEBHOOK_UNMATCHED_REVIEW_REQUIRED' THEN 'Review the unmatched provider webhook.'
        WHEN 'BANK_RETURNED_PAYMENT' THEN 'Open Current Payment Status and review the payment status.'
        WHEN 'BANK_REJECTED_PAYMENT' THEN 'Open Current Payment Status and rewind financials where no money moved.'
        WHEN 'AMBIGUOUS_PAYMENT_REVIEW_REQUIRED' THEN 'Review and resolve the ambiguous bank event.'
        ELSE 'Open Current Payment Status.'
      END::text AS action_guidance,
      bank_event_base.sort_at_utc
    FROM bank_event_base
    WHERE bank_event_base.alert_kind IS NOT NULL
  ),
  transfer_base AS MATERIALIZED (
    SELECT
      CASE
        WHEN UPPER(BTRIM(COALESCE(public.pay_bank_transfers.status, ''))) IN ('RETURNED','REVERTED')
          OR UPPER(BTRIM(COALESCE(public.pay_bank_transfers.rail_state, ''))) IN ('RETURNED','REVERTED') THEN 'BANK_RETURNED_PAYMENT'
        WHEN UPPER(BTRIM(COALESCE(public.pay_bank_transfers.status, ''))) IN ('FAILED','DECLINED','REJECTED','CANCELLED','CANCELED','SUBMISSION_FAILED','FAILED_BEFORE_COMMIT')
          OR UPPER(BTRIM(COALESCE(public.pay_bank_transfers.rail_state, ''))) IN ('FAILED','DECLINED','REJECTED','CANCELLED','CANCELED','SUBMISSION_FAILED','FAILED_BEFORE_COMMIT')
          OR NULLIF(BTRIM(COALESCE(public.pay_bank_transfers.failed_reason, '')), '') IS NOT NULL THEN 'BANK_REJECTED_PAYMENT'
        WHEN UPPER(BTRIM(COALESCE(public.pay_bank_transfers.status, ''))) IN ('UNKNOWN','TIMEOUT','TIMED_OUT','PENDING_REVIEW')
          OR UPPER(BTRIM(COALESCE(public.pay_bank_transfers.status, ''))) LIKE 'CREATE_ERROR%'
          OR UPPER(BTRIM(COALESCE(public.pay_bank_transfers.rail_state, ''))) IN ('UNKNOWN','TIMEOUT','TIMED_OUT','PENDING_REVIEW')
          OR UPPER(BTRIM(COALESCE(public.pay_bank_transfers.rail_state, ''))) LIKE 'CREATE_ERROR%' THEN 'RAIL_SUBMISSION_UNKNOWN_OR_TIMEOUT'
        ELSE NULL::text
      END AS alert_kind,
      public.pay_bank_transfers.pay_batch_id AS pay_batch_id,
      public.pay_bank_transfers.id AS source_id,
      public.pay_bank_transfers.status AS transfer_status,
      public.pay_bank_transfers.rail_state AS rail_state,
      public.pay_bank_transfers.failed_reason AS failed_reason,
      public.pay_bank_transfers.completed_at_utc AS completed_at_utc,
      public.pay_bank_transfers.created_at_utc AS created_at_utc,
      COALESCE(public.pay_bank_transfers.completed_at_utc, public.pay_bank_transfers.created_at_utc) AS sort_at_utc
    FROM public.pay_bank_transfers
    JOIN public.pay_batches AS transfer_batches
      ON transfer_batches.id = public.pay_bank_transfers.pay_batch_id
    WHERE UPPER(BTRIM(COALESCE(transfer_batches.status, ''))) NOT IN ('CANCELLED','CANCELED')
      AND (
        UPPER(BTRIM(COALESCE(public.pay_bank_transfers.status, ''))) IN ('FAILED','DECLINED','REJECTED','CANCELLED','CANCELED','SUBMISSION_FAILED','FAILED_BEFORE_COMMIT','RETURNED','REVERTED','UNKNOWN','TIMEOUT','TIMED_OUT','PENDING_REVIEW')
        OR UPPER(BTRIM(COALESCE(public.pay_bank_transfers.status, ''))) LIKE 'CREATE_ERROR%'
        OR UPPER(BTRIM(COALESCE(public.pay_bank_transfers.rail_state, ''))) IN ('FAILED','DECLINED','REJECTED','CANCELLED','CANCELED','SUBMISSION_FAILED','FAILED_BEFORE_COMMIT','RETURNED','REVERTED','UNKNOWN','TIMEOUT','TIMED_OUT','PENDING_REVIEW')
        OR UPPER(BTRIM(COALESCE(public.pay_bank_transfers.rail_state, ''))) LIKE 'CREATE_ERROR%'
        OR NULLIF(BTRIM(COALESCE(public.pay_bank_transfers.failed_reason, '')), '') IS NOT NULL
      )
      AND NOT EXISTS (
        SELECT 1
        FROM public.pay_bank_transfer_events AS transfer_resolution_events
        WHERE transfer_resolution_events.pay_bank_transfer_id = public.pay_bank_transfers.id
          AND UPPER(BTRIM(COALESCE(transfer_resolution_events.correction_disposition, ''))) IN ('NO_CORRECTION_REQUIRED','AUTO_APPLIED','APPLIED','RESOLVED')
      )
      AND NOT EXISTS (
        SELECT 1
        FROM public.pay_payment_correction_work_items AS transfer_resolved_work_items
        JOIN public.pay_payment_correction_requests AS transfer_resolved_requests
          ON transfer_resolved_requests.id = transfer_resolved_work_items.correction_request_id
        WHERE transfer_resolved_work_items.pay_bank_transfer_id = public.pay_bank_transfers.id
          AND UPPER(BTRIM(COALESCE(transfer_resolved_requests.status, ''))) IN ('APPLIED','RESOLVED')
      )
  ),
  transfer_alerts AS MATERIALIZED (
    SELECT
      transfer_base.alert_kind,
      'critical'::text AS severity,
      CASE transfer_base.alert_kind
        WHEN 'BANK_RETURNED_PAYMENT' THEN 93
        WHEN 'BANK_REJECTED_PAYMENT' THEN 92
        ELSE 79
      END::integer AS severity_rank,
      'pay_batch'::text AS entity_kind,
      transfer_base.pay_batch_id AS entity_id,
      transfer_base.pay_batch_id AS pay_batch_id,
      'pay_bank_transfer'::text AS payload_source_kind,
      transfer_base.source_id AS payload_source_id,
      jsonb_strip_nulls(jsonb_build_object(
        'fingerprint_source_kind', 'pay_bank_transfer',
        'fingerprint_source_id', transfer_base.source_id::text,
        'issue_kind', transfer_base.alert_kind,
        'pay_batch_id', transfer_base.pay_batch_id::text,
        'status', NULLIF(BTRIM(COALESCE(transfer_base.transfer_status, '')), ''),
        'rail_state', NULLIF(BTRIM(COALESCE(transfer_base.rail_state, '')), ''),
        'failed_reason_hash', CASE WHEN NULLIF(BTRIM(COALESCE(transfer_base.failed_reason, '')), '') IS NULL THEN NULL::text ELSE MD5(transfer_base.failed_reason) END,
        'completed_at_utc', CASE WHEN transfer_base.completed_at_utc IS NULL THEN NULL::text ELSE TO_CHAR(transfer_base.completed_at_utc AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') END,
        'created_at_utc', CASE WHEN transfer_base.created_at_utc IS NULL THEN NULL::text ELSE TO_CHAR(transfer_base.created_at_utc AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') END
      )) AS fingerprint_payload_json,
      CASE transfer_base.alert_kind
        WHEN 'BANK_RETURNED_PAYMENT' THEN 'Provider outcome unknown — check provider'
        WHEN 'BANK_REJECTED_PAYMENT' THEN 'Failed payments — Rewind financials available'
        ELSE 'Rail submission needs review'
      END::text AS label,
      CASE transfer_base.alert_kind
        WHEN 'BANK_RETURNED_PAYMENT' THEN 'Provider outcome unknown — check provider'
        WHEN 'BANK_REJECTED_PAYMENT' THEN 'Failed payments — Rewind financials available'
        ELSE 'Rail submission needs review'
      END::text AS title,
      CASE transfer_base.alert_kind
        WHEN 'BANK_RETURNED_PAYMENT' THEN 'Provider returned/reverted status requires checking before any financial correction.'
        WHEN 'BANK_REJECTED_PAYMENT' THEN 'The bank rejected a payment transfer.'
        ELSE 'CloudTMS could not confirm the final bank submission state.'
      END::text AS description,
      CASE transfer_base.alert_kind
        WHEN 'BANK_RETURNED_PAYMENT' THEN 'Open Current Payment Status and review the payment status.'
        ELSE 'Open Current Payment Status.'
      END::text AS action_guidance,
      transfer_base.sort_at_utc
    FROM transfer_base
    WHERE transfer_base.alert_kind IS NOT NULL
  ),
  correction_request_base AS MATERIALIZED (
    SELECT
      CASE
        WHEN UPPER(BTRIM(COALESCE(public.pay_payment_correction_requests.status, ''))) IN ('FAILED','FAILED_RETRYABLE','FAILED_FINAL') THEN 'PAYMENT_CORRECTION_FAILED'
        WHEN UPPER(BTRIM(COALESCE(public.pay_payment_correction_requests.status, ''))) IN ('BLOCKED','APPLIED_WITH_BLOCKERS') THEN 'PAYMENT_CORRECTION_BLOCKED'
        WHEN UPPER(BTRIM(COALESCE(public.pay_payment_correction_requests.status, ''))) IN ('REQUESTED','AWAITING_AUTHORISATION','AWAITING_AUTHORIZATION','PENDING_APPROVAL') THEN 'PAYMENT_CORRECTION_AWAITING_APPROVAL'
        ELSE NULL::text
      END AS alert_kind,
      public.pay_payment_correction_requests.pay_batch_id AS pay_batch_id,
      public.pay_payment_correction_requests.id AS source_id,
      public.pay_payment_correction_requests.status AS request_status,
      public.pay_payment_correction_requests.correction_kind AS correction_kind,
      public.pay_payment_correction_requests.selection_hash AS selection_hash,
      public.pay_payment_correction_requests.updated_at_utc AS updated_at_utc,
      public.pay_payment_correction_requests.created_at_utc AS created_at_utc,
      public.pay_payment_correction_requests.requested_at_utc AS requested_at_utc,
      COALESCE(public.pay_payment_correction_requests.updated_at_utc, public.pay_payment_correction_requests.created_at_utc, public.pay_payment_correction_requests.requested_at_utc) AS sort_at_utc
    FROM public.pay_payment_correction_requests
    JOIN public.pay_batches AS correction_request_batches
      ON correction_request_batches.id = public.pay_payment_correction_requests.pay_batch_id
    WHERE UPPER(BTRIM(COALESCE(correction_request_batches.status, ''))) NOT IN ('CANCELLED','CANCELED')
      AND UPPER(BTRIM(COALESCE(public.pay_payment_correction_requests.status, ''))) IN ('FAILED','FAILED_RETRYABLE','FAILED_FINAL','BLOCKED','APPLIED_WITH_BLOCKERS','REQUESTED','AWAITING_AUTHORISATION','AWAITING_AUTHORIZATION','PENDING_APPROVAL')
  ),
  correction_request_alerts AS MATERIALIZED (
    SELECT
      correction_request_base.alert_kind,
      'critical'::text AS severity,
      CASE correction_request_base.alert_kind
        WHEN 'PAYMENT_CORRECTION_FAILED' THEN 88
        WHEN 'PAYMENT_CORRECTION_BLOCKED' THEN 87
        ELSE 75
      END::integer AS severity_rank,
      'pay_batch'::text AS entity_kind,
      correction_request_base.pay_batch_id AS entity_id,
      correction_request_base.pay_batch_id AS pay_batch_id,
      'pay_payment_correction_request'::text AS payload_source_kind,
      correction_request_base.source_id AS payload_source_id,
      jsonb_strip_nulls(jsonb_build_object(
        'fingerprint_source_kind', 'pay_payment_correction_request',
        'fingerprint_source_id', correction_request_base.source_id::text,
        'issue_kind', correction_request_base.alert_kind,
        'pay_batch_id', correction_request_base.pay_batch_id::text,
        'status', NULLIF(BTRIM(COALESCE(correction_request_base.request_status, '')), ''),
        'correction_kind', NULLIF(BTRIM(COALESCE(correction_request_base.correction_kind, '')), ''),
        'selection_hash', NULLIF(BTRIM(COALESCE(correction_request_base.selection_hash, '')), '')
      )) AS fingerprint_payload_json,
      CASE correction_request_base.alert_kind
        WHEN 'PAYMENT_CORRECTION_FAILED' THEN 'Payment correction failed'
        WHEN 'PAYMENT_CORRECTION_BLOCKED' THEN 'Payment correction blocked'
        ELSE 'Payment correction awaiting approval'
      END::text AS label,
      CASE correction_request_base.alert_kind
        WHEN 'PAYMENT_CORRECTION_FAILED' THEN 'Payment correction failed'
        WHEN 'PAYMENT_CORRECTION_BLOCKED' THEN 'Payment correction blocked'
        ELSE 'Payment correction awaiting approval'
      END::text AS title,
      CASE correction_request_base.alert_kind
        WHEN 'PAYMENT_CORRECTION_FAILED' THEN 'CloudTMS could not complete a payment correction automatically.'
        WHEN 'PAYMENT_CORRECTION_BLOCKED' THEN 'A payment correction is blocked and needs review.'
        ELSE 'A payment correction is awaiting approval.'
      END::text AS description,
      CASE correction_request_base.alert_kind
        WHEN 'PAYMENT_CORRECTION_AWAITING_APPROVAL' THEN 'Review and approve or reject the correction request.'
        ELSE 'Open Current Payment Status.'
      END::text AS action_guidance,
      correction_request_base.sort_at_utc
    FROM correction_request_base
    WHERE correction_request_base.alert_kind IS NOT NULL
  ),
  correction_work_base AS MATERIALIZED (
    SELECT
      CASE
        WHEN UPPER(BTRIM(COALESCE(public.pay_payment_correction_work_items.status, ''))) IN ('FAILED','FAILED_RETRYABLE','FAILED_FINAL') THEN 'PAYMENT_CORRECTION_FAILED'
        WHEN UPPER(BTRIM(COALESCE(public.pay_payment_correction_work_items.status, ''))) IN ('BLOCKED','APPLIED_WITH_BLOCKERS') THEN 'PAYMENT_CORRECTION_BLOCKED'
        ELSE NULL::text
      END AS alert_kind,
      public.pay_payment_correction_work_items.pay_batch_id AS pay_batch_id,
      public.pay_payment_correction_work_items.id AS source_id,
      public.pay_payment_correction_work_items.status AS work_status,
      public.pay_payment_correction_work_items.work_kind AS work_kind,
      public.pay_payment_correction_work_items.selection_hash AS selection_hash,
      public.pay_payment_correction_work_items.processed_at_utc AS processed_at_utc,
      public.pay_payment_correction_work_items.created_at_utc AS created_at_utc,
      COALESCE(public.pay_payment_correction_work_items.processed_at_utc, public.pay_payment_correction_work_items.created_at_utc) AS sort_at_utc
    FROM public.pay_payment_correction_work_items
    JOIN public.pay_batches AS correction_work_batches
      ON correction_work_batches.id = public.pay_payment_correction_work_items.pay_batch_id
    WHERE UPPER(BTRIM(COALESCE(correction_work_batches.status, ''))) NOT IN ('CANCELLED','CANCELED')
      AND UPPER(BTRIM(COALESCE(public.pay_payment_correction_work_items.status, ''))) IN ('FAILED','FAILED_RETRYABLE','FAILED_FINAL','BLOCKED','APPLIED_WITH_BLOCKERS')
  ),
  correction_work_alerts AS MATERIALIZED (
    SELECT
      correction_work_base.alert_kind,
      'critical'::text AS severity,
      CASE correction_work_base.alert_kind
        WHEN 'PAYMENT_CORRECTION_FAILED' THEN 86
        ELSE 85
      END::integer AS severity_rank,
      'pay_batch'::text AS entity_kind,
      correction_work_base.pay_batch_id AS entity_id,
      correction_work_base.pay_batch_id AS pay_batch_id,
      'pay_payment_correction_work_item'::text AS payload_source_kind,
      correction_work_base.source_id AS payload_source_id,
      jsonb_strip_nulls(jsonb_build_object(
        'fingerprint_source_kind', 'pay_payment_correction_work_item',
        'fingerprint_source_id', correction_work_base.source_id::text,
        'issue_kind', correction_work_base.alert_kind,
        'pay_batch_id', correction_work_base.pay_batch_id::text,
        'status', NULLIF(BTRIM(COALESCE(correction_work_base.work_status, '')), ''),
        'work_kind', NULLIF(BTRIM(COALESCE(correction_work_base.work_kind, '')), ''),
        'selection_hash', NULLIF(BTRIM(COALESCE(correction_work_base.selection_hash, '')), '')
      )) AS fingerprint_payload_json,
      CASE correction_work_base.alert_kind
        WHEN 'PAYMENT_CORRECTION_FAILED' THEN 'Payment correction failed'
        ELSE 'Payment correction blocked'
      END::text AS label,
      CASE correction_work_base.alert_kind
        WHEN 'PAYMENT_CORRECTION_FAILED' THEN 'Payment correction failed'
        ELSE 'Payment correction blocked'
      END::text AS title,
      CASE correction_work_base.alert_kind
        WHEN 'PAYMENT_CORRECTION_FAILED' THEN 'CloudTMS could not complete a payment correction work item automatically.'
        ELSE 'A payment correction work item is blocked and needs review.'
      END::text AS description,
      'Open Current Payment Status.'::text AS action_guidance,
      correction_work_base.sort_at_utc
    FROM correction_work_base
    WHERE correction_work_base.alert_kind IS NOT NULL
  ),
  remittance_base AS MATERIALIZED (
    SELECT
      public.mail_outbox.context_id AS pay_batch_id,
      public.mail_outbox.id AS source_id,
      public.mail_outbox.reference AS reference,
      public.mail_outbox.failed_at AS failed_at,
      public.mail_outbox.created_at_utc AS created_at_utc,
      COALESCE(public.mail_outbox.failed_at, public.mail_outbox.created_at_utc) AS sort_at_utc
    FROM public.mail_outbox
    JOIN public.pay_batches AS remittance_batches
      ON remittance_batches.id = public.mail_outbox.context_id
    WHERE UPPER(BTRIM(COALESCE(remittance_batches.status, ''))) NOT IN ('CANCELLED','CANCELED')
      AND UPPER(BTRIM(COALESCE(public.mail_outbox.type, ''))) = 'REMITTANCE'
      AND LOWER(BTRIM(COALESCE(public.mail_outbox.context_kind, ''))) IN ('pay_batch','pay_batches')
      AND public.mail_outbox.context_id IS NOT NULL
      AND UPPER(BTRIM(COALESCE(public.mail_outbox.status::text, ''))) = 'FAILED'
      AND NOT EXISTS (
        SELECT 1
        FROM public.mail_outbox AS remittance_success_outbox
        WHERE UPPER(BTRIM(COALESCE(remittance_success_outbox.type, ''))) = 'REMITTANCE'
          AND LOWER(BTRIM(COALESCE(remittance_success_outbox.context_kind, ''))) IN ('pay_batch','pay_batches')
          AND remittance_success_outbox.context_id = public.mail_outbox.context_id
          AND COALESCE(NULLIF(BTRIM(remittance_success_outbox.reference), ''), remittance_success_outbox.id::text) = COALESCE(NULLIF(BTRIM(public.mail_outbox.reference), ''), public.mail_outbox.id::text)
          AND UPPER(BTRIM(COALESCE(remittance_success_outbox.status::text, ''))) = 'SENT'
          AND COALESCE(remittance_success_outbox.sent_at, remittance_success_outbox.created_at_utc) >= COALESCE(public.mail_outbox.failed_at, public.mail_outbox.created_at_utc)
      )
  ),
  remittance_alerts AS MATERIALIZED (
    SELECT
      'REMITTANCE_SEND_FAILED'::text AS alert_kind,
      'critical'::text AS severity,
      60::integer AS severity_rank,
      'pay_batch'::text AS entity_kind,
      remittance_base.pay_batch_id AS entity_id,
      remittance_base.pay_batch_id AS pay_batch_id,
      'mail_outbox'::text AS payload_source_kind,
      remittance_base.source_id AS payload_source_id,
      jsonb_strip_nulls(jsonb_build_object(
        'fingerprint_source_kind', 'mail_outbox',
        'fingerprint_source_id', remittance_base.source_id::text,
        'issue_kind', 'REMITTANCE_SEND_FAILED',
        'pay_batch_id', remittance_base.pay_batch_id::text,
        'reference', NULLIF(BTRIM(COALESCE(remittance_base.reference, '')), ''),
        'failed_at_utc', CASE WHEN remittance_base.failed_at IS NULL THEN NULL::text ELSE TO_CHAR(remittance_base.failed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') END
      )) AS fingerprint_payload_json,
      'Remittance failed'::text AS label,
      'Remittance send failed'::text AS title,
      'A remittance email failed to send and requires review.'::text AS description,
      'Review or resend the remittance from the batch.'::text AS action_guidance,
      remittance_base.sort_at_utc
    FROM remittance_base
  ),

  provider_submit_review_scope AS MATERIALIZED (
    SELECT DISTINCT
      provider_operation.id AS operation_id,
      provider_operation.pay_batch_id AS pay_batch_id,
      provider_operation.status AS operation_status,
      provider_operation.phase AS operation_phase,
      COALESCE(provider_operation.updated_at_utc, provider_operation.created_at_utc, now()) AS sort_at_utc
    FROM public.banking_pay_operations AS provider_operation
    JOIN public.pay_batches AS provider_batch
      ON provider_batch.id = provider_operation.pay_batch_id
    WHERE provider_operation.operation_type IN ('PAYMENT_EXECUTE', 'PAYMENT_RETRY_BLOCKED_FUNDS')
      AND provider_operation.pay_batch_id IS NOT NULL
      AND UPPER(BTRIM(COALESCE(provider_batch.status, ''))) NOT IN ('CANCELLED', 'CANCELED')
      AND (
        UPPER(BTRIM(COALESCE(provider_operation.status, ''))) IN ('REVIEW_REQUIRED', 'FAILED')
        OR jsonb_typeof(provider_operation.progress_json->'provider_submit_diagnostic') = 'object'
        OR jsonb_typeof(provider_operation.result_json->'provider_submit_diagnostic') = 'object'
        OR jsonb_typeof(provider_operation.error_json->'provider_submit_diagnostic') = 'object'
      )
      AND (
        UPPER(BTRIM(COALESCE(provider_operation.phase, ''))) IN ('SUBMIT_PROVIDER_TRANSFERS', 'APPLY_RAIL_UPDATES', 'COMPLETE')
        OR jsonb_typeof(provider_operation.progress_json->'provider_submit_diagnostic') = 'object'
        OR jsonb_typeof(provider_operation.result_json->'provider_submit_diagnostic') = 'object'
        OR jsonb_typeof(provider_operation.error_json->'provider_submit_diagnostic') = 'object'
      )
  ),
  provider_submit_review_base AS MATERIALIZED (
    SELECT
      provider_submit_review_scope.pay_batch_id,
      provider_submit_review_scope.operation_id,
      provider_submit_review_scope.operation_status,
      provider_submit_review_scope.operation_phase,
      provider_submit_review_scope.sort_at_utc,
      provider_diagnostic.diagnostic_json AS diagnostic_result,
      COALESCE(provider_diagnostic.diagnostic_json->'provider_submit_diagnostic', '{}'::jsonb) AS provider_submit_diagnostic,
      UPPER(BTRIM(COALESCE(provider_diagnostic.diagnostic_json->>'provider_submission_status', provider_diagnostic.diagnostic_json #>> '{provider_submit_diagnostic,provider_submission_status}', ''))) AS provider_submission_status,
      COALESCE(NULLIF(BTRIM(COALESCE(provider_diagnostic.diagnostic_json->>'review_reason_code', provider_diagnostic.diagnostic_json #>> '{provider_submit_diagnostic,review_reason_code}', '')), ''), 'PAYMENT_PROVIDER_SUBMIT_REVIEW') AS review_reason_code,
      NULLIF(BTRIM(COALESCE(provider_diagnostic.diagnostic_json #>> '{provider_submit_diagnostic,chunk_id}', '')), '') AS chunk_id_text,
      NULLIF(BTRIM(COALESCE(provider_diagnostic.diagnostic_json #>> '{provider_submit_diagnostic,transfer_id}', '')), '') AS transfer_id_text,
      NULLIF(BTRIM(COALESCE(provider_diagnostic.diagnostic_json #>> '{provider_submit_diagnostic,transfer_scope_id}', '')), '') AS transfer_scope_id_text,
      NULLIF(BTRIM(COALESCE(provider_diagnostic.diagnostic_json #>> '{provider_submit_diagnostic,auth_request_id}', '')), '') AS auth_request_id_text,
      lower(BTRIM(COALESCE(provider_diagnostic.diagnostic_json->>'manual_resolution_required', provider_diagnostic.diagnostic_json #>> '{provider_submit_diagnostic,manual_resolution_required}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') AS manual_resolution_required,
      lower(BTRIM(COALESCE(provider_diagnostic.diagnostic_json->>'safe_retry_available', provider_diagnostic.diagnostic_json #>> '{provider_submit_diagnostic,safe_retry_available}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') AS safe_retry_available,
      lower(BTRIM(COALESCE(provider_diagnostic.diagnostic_json #>> '{provider_submit_diagnostic,provider_acceptance_evidence_present}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') AS provider_acceptance_evidence_present,
      lower(BTRIM(COALESCE(provider_diagnostic.diagnostic_json #>> '{provider_submit_diagnostic,provider_response_present}', provider_diagnostic.diagnostic_json #>> '{provider_submit_diagnostic,provider_response_received}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') AS provider_response_present
    FROM provider_submit_review_scope
    CROSS JOIN LATERAL public.pay_provider_submit_diagnostic_get(
      p_pay_batch_id := provider_submit_review_scope.pay_batch_id,
      p_operation_id := provider_submit_review_scope.operation_id,
      p_transfer_id := NULL::uuid,
      p_chunk_id := NULL::uuid,
      p_counts_only := false,
      p_provider_diagnostic_context := 'PAYMENT_ISSUES_PROVIDER_DIAGNOSTIC'
    ) AS provider_diagnostic(diagnostic_json)
    WHERE (
        (
          UPPER(BTRIM(COALESCE(provider_diagnostic.diagnostic_json->>'provider_submission_status', provider_diagnostic.diagnostic_json #>> '{provider_submit_diagnostic,provider_submission_status}', ''))) IN (
            'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK',
            'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME',
            'PROVIDER_SUBMISSION_MALFORMED_RESPONSE',
            'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID',
            'PROVIDER_SUBMISSION_REJECTED'
          )
          AND UPPER(BTRIM(COALESCE(provider_submit_review_scope.operation_status, ''))) IN ('REVIEW_REQUIRED', 'FAILED')
        )
        OR (
          UPPER(BTRIM(COALESCE(provider_diagnostic.diagnostic_json->>'provider_submission_status', provider_diagnostic.diagnostic_json #>> '{provider_submit_diagnostic,provider_submission_status}', ''))) = 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL'
          AND UPPER(BTRIM(COALESCE(provider_submit_review_scope.operation_status, ''))) IN ('REVIEW_REQUIRED', 'FAILED')
        )
        OR (
          UPPER(BTRIM(COALESCE(provider_diagnostic.diagnostic_json->>'provider_submission_status', provider_diagnostic.diagnostic_json #>> '{provider_submit_diagnostic,provider_submission_status}', ''))) = 'PROVIDER_SUBMISSION_ACCEPTED'
          AND UPPER(BTRIM(COALESCE(provider_submit_review_scope.operation_status, ''))) = 'REVIEW_REQUIRED'
          AND NOT EXISTS (
            SELECT 1
            FROM public.pay_batches AS accepted_review_batch
            WHERE accepted_review_batch.id = provider_submit_review_scope.pay_batch_id
              AND (
                UPPER(BTRIM(COALESCE(accepted_review_batch.status, ''))) IN ('SETTLED','COMMITTED')
                OR UPPER(BTRIM(COALESCE(accepted_review_batch.execution_commit_state, ''))) = 'COMMITTED'
              )
              AND EXISTS (
                SELECT 1
                FROM public.pay_bank_transfers AS accepted_review_transfer
                WHERE accepted_review_transfer.pay_batch_id = accepted_review_batch.id
                  AND (
                    UPPER(BTRIM(COALESCE(accepted_review_transfer.status, ''))) IN ('COMPLETED','SETTLED','PAID','COMMITTED')
                    OR UPPER(BTRIM(COALESCE(accepted_review_transfer.rail_state, ''))) IN ('COMPLETED','SETTLED','PAID','COMMITTED')
                    OR accepted_review_transfer.completed_at_utc IS NOT NULL
                  )
                  AND NULLIF(BTRIM(COALESCE(accepted_review_transfer.rail_tx_id, '')), '') IS NOT NULL
              )
          )
        )
      )
      AND UPPER(BTRIM(COALESCE(provider_diagnostic.diagnostic_json->>'provider_submission_status', provider_diagnostic.diagnostic_json #>> '{provider_submit_diagnostic,provider_submission_status}', ''))) <> 'MANUAL_RESOLVED_NO_PAYMENT_MADE'
  ),
  provider_submit_review_alerts AS MATERIALIZED (
    SELECT
      'PAYMENT_PROVIDER_SUBMIT_REVIEW'::text AS alert_kind,
      'critical'::text AS severity,
      91::integer AS severity_rank,
      'pay_batch'::text AS entity_kind,
      provider_submit_review_base.pay_batch_id AS entity_id,
      provider_submit_review_base.pay_batch_id AS pay_batch_id,
      'banking_pay_operation'::text AS payload_source_kind,
      provider_submit_review_base.operation_id AS payload_source_id,
      jsonb_strip_nulls(jsonb_build_object(
        'alert_kind', 'PAYMENT_PROVIDER_SUBMIT_REVIEW',
        'issue_kind', 'PAYMENT_PROVIDER_SUBMIT_REVIEW',
        'pay_batch_id', provider_submit_review_base.pay_batch_id::text,
        'operation_id', provider_submit_review_base.operation_id::text,
        'chunk_id', provider_submit_review_base.chunk_id_text,
        'review_reason_code', provider_submit_review_base.review_reason_code,
        'provider_submission_status', provider_submit_review_base.provider_submission_status
      )) AS fingerprint_payload_json,
      CASE provider_submit_review_base.provider_submission_status
        WHEN 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK' THEN 'Provider submission outcome unknown'
        WHEN 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME' THEN 'Provider response missing'
        WHEN 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE' THEN 'Provider response unusable'
        WHEN 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID' THEN 'Provider acceptance evidence missing'
        WHEN 'PROVIDER_SUBMISSION_REJECTED' THEN 'Provider submission failed'
        WHEN 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL' THEN 'Provider was not called'
        WHEN 'PROVIDER_SUBMISSION_ACCEPTED' THEN 'Provider acceptance evidence present'
        ELSE 'Provider submission needs review'
      END::text AS label,
      CASE provider_submit_review_base.provider_submission_status
        WHEN 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK' THEN 'Provider submission outcome unknown'
        WHEN 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME' THEN 'Provider response missing'
        WHEN 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE' THEN 'Provider response unusable'
        WHEN 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID' THEN 'Provider acceptance evidence missing'
        WHEN 'PROVIDER_SUBMISSION_REJECTED' THEN 'Provider submission failed'
        WHEN 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL' THEN 'Provider was not called'
        WHEN 'PROVIDER_SUBMISSION_ACCEPTED' THEN 'Provider acceptance evidence present'
        ELSE 'Provider submission needs review'
      END::text AS title,
      CASE provider_submit_review_base.provider_submission_status
        WHEN 'PROVIDER_SUBMISSION_UNKNOWN_STALE_CHUNK' THEN 'Submit chunk became stale with no provider response, transfer event, rail transaction ID, or rail state.'
        WHEN 'UNKNOWN_PROVIDER_SUBMISSION_OUTCOME' THEN 'A provider request may have been sent, but no usable provider response was recorded.'
        WHEN 'PROVIDER_SUBMISSION_MALFORMED_RESPONSE' THEN 'Provider returned an unusable response. Manual reconciliation is required before retry.'
        WHEN 'PROVIDER_SUBMISSION_ATTEMPTED_NO_EXTERNAL_ID' THEN 'Provider response/status was recorded, but no usable external provider transaction/reference was stored.'
        WHEN 'PROVIDER_SUBMISSION_REJECTED' THEN 'Provider rejected the payment submission.'
        WHEN 'PROVIDER_SUBMISSION_BLOCKED_PRE_CALL' THEN 'Provider submission failed before the provider payment request was sent.'
        WHEN 'PROVIDER_SUBMISSION_ACCEPTED' THEN 'Provider acceptance evidence exists and retry is unsafe until reconciliation is complete.'
        ELSE 'Provider submission requires review before retry.'
      END::text AS description,
      COALESCE(NULLIF(BTRIM(provider_submit_review_base.provider_submit_diagnostic->>'recommended_action'), ''), 'Check Revolut/bank before retry. If no payment was made, record manual no-payment confirmation.')::text AS action_guidance,
      provider_submit_review_base.sort_at_utc
    FROM provider_submit_review_base
    WHERE NOT EXISTS (
        SELECT 1 FROM blocked_funds_alerts AS stronger_blocked
        WHERE stronger_blocked.pay_batch_id = provider_submit_review_base.pay_batch_id
      )
      AND NOT EXISTS (
        SELECT 1 FROM bank_event_alerts AS stronger_bank_event
        WHERE stronger_bank_event.pay_batch_id = provider_submit_review_base.pay_batch_id
          AND stronger_bank_event.alert_kind IN ('BANK_REJECTED_PAYMENT', 'BANK_RETURNED_PAYMENT', 'RAIL_SUBMISSION_UNKNOWN_OR_TIMEOUT')
      )
      AND NOT EXISTS (
        SELECT 1 FROM transfer_alerts AS stronger_transfer
        WHERE stronger_transfer.pay_batch_id = provider_submit_review_base.pay_batch_id
          AND stronger_transfer.alert_kind IN ('BANK_REJECTED_PAYMENT', 'BANK_RETURNED_PAYMENT', 'RAIL_SUBMISSION_UNKNOWN_OR_TIMEOUT')
      )
  ),

  grouped_banking_pay_diagnostic_pay_batch_scope AS MATERIALIZED (
    SELECT DISTINCT candidate_scope.pay_batch_id
    FROM (
      SELECT scoped_specific_batch.id AS pay_batch_id
      FROM public.pay_batches AS scoped_specific_batch
      WHERE p_entity_id IS NOT NULL
        AND (v_entity_kind IS NULL OR v_entity_kind IN ('pay_batch', 'pay_batches'))
        AND scoped_specific_batch.id = p_entity_id

      UNION

      SELECT blocked_candidate_batch.id AS pay_batch_id
      FROM public.pay_batches AS blocked_candidate_batch
      WHERE p_entity_id IS NULL
        AND (v_entity_kind IS NULL OR v_entity_kind IN ('pay_batch', 'pay_batches'))
        AND UPPER(BTRIM(COALESCE(blocked_candidate_batch.status, ''))) = 'BLOCKED_FUNDS'
        AND UPPER(BTRIM(COALESCE(blocked_candidate_batch.execution_commit_state, 'NOT_SUBMITTED'))) = 'NOT_SUBMITTED'
        AND blocked_candidate_batch.cancelled_at_utc IS NULL

      UNION

      SELECT event_candidate.pay_batch_id
      FROM public.pay_bank_transfer_events AS event_candidate
      JOIN public.pay_batches AS event_candidate_batch
        ON event_candidate_batch.id = event_candidate.pay_batch_id
      WHERE p_entity_id IS NULL
        AND (v_entity_kind IS NULL OR v_entity_kind IN ('pay_batch', 'pay_batches'))
        AND event_candidate.pay_batch_id IS NOT NULL
        AND UPPER(BTRIM(COALESCE(event_candidate_batch.status, ''))) NOT IN ('CANCELLED','CANCELED')
        AND (
          UPPER(BTRIM(COALESCE(event_candidate.normalised_state, ''))) IN ('FAILED','DECLINED','REJECTED','CANCELLED','CANCELED','SUBMISSION_FAILED','RETURNED','REVERTED','UNKNOWN','TIMEOUT','TIMED_OUT','PENDING_REVIEW')
          OR UPPER(BTRIM(COALESCE(event_candidate.normalised_state, ''))) LIKE 'CREATE_ERROR%'
          OR UPPER(BTRIM(COALESCE(event_candidate.provider_state, ''))) IN ('FAILED','DECLINED','REJECTED','CANCELLED','CANCELED','SUBMISSION_FAILED','RETURNED','REVERTED','UNKNOWN','TIMEOUT','TIMED_OUT','PENDING_REVIEW')
          OR UPPER(BTRIM(COALESCE(event_candidate.provider_state, ''))) LIKE 'CREATE_ERROR%'
          OR UPPER(BTRIM(COALESCE(event_candidate.mapping_status, ''))) IN ('AMBIGUOUS','UNMATCHED','NO_MATCH','MULTIPLE_MATCHES')
          OR UPPER(BTRIM(COALESCE(event_candidate.correction_disposition, ''))) IN ('AMBIGUOUS','ACTION_REQUIRED','BLOCKED','FAILED')
          OR NULLIF(BTRIM(COALESCE(event_candidate.provider_failure_reason_group, '')), '') IS NOT NULL
        )

      UNION

      SELECT transfer_candidate.pay_batch_id
      FROM public.pay_bank_transfers AS transfer_candidate
      JOIN public.pay_batches AS transfer_candidate_batch
        ON transfer_candidate_batch.id = transfer_candidate.pay_batch_id
      WHERE p_entity_id IS NULL
        AND (v_entity_kind IS NULL OR v_entity_kind IN ('pay_batch', 'pay_batches'))
        AND transfer_candidate.pay_batch_id IS NOT NULL
        AND UPPER(BTRIM(COALESCE(transfer_candidate_batch.status, ''))) NOT IN ('CANCELLED','CANCELED')
        AND (
          UPPER(BTRIM(COALESCE(transfer_candidate.status, ''))) IN ('FAILED','DECLINED','REJECTED','CANCELLED','CANCELED','SUBMISSION_FAILED','FAILED_BEFORE_COMMIT','RETURNED','REVERTED','UNKNOWN','TIMEOUT','TIMED_OUT','PENDING_REVIEW')
          OR UPPER(BTRIM(COALESCE(transfer_candidate.status, ''))) LIKE 'CREATE_ERROR%'
          OR UPPER(BTRIM(COALESCE(transfer_candidate.rail_state, ''))) IN ('FAILED','DECLINED','REJECTED','CANCELLED','CANCELED','SUBMISSION_FAILED','FAILED_BEFORE_COMMIT','RETURNED','REVERTED','UNKNOWN','TIMEOUT','TIMED_OUT','PENDING_REVIEW')
          OR UPPER(BTRIM(COALESCE(transfer_candidate.rail_state, ''))) LIKE 'CREATE_ERROR%'
          OR NULLIF(BTRIM(COALESCE(transfer_candidate.failed_reason, '')), '') IS NOT NULL
        )

      UNION

      SELECT correction_request_candidate.pay_batch_id
      FROM public.pay_payment_correction_requests AS correction_request_candidate
      JOIN public.pay_batches AS correction_request_candidate_batch
        ON correction_request_candidate_batch.id = correction_request_candidate.pay_batch_id
      WHERE p_entity_id IS NULL
        AND (v_entity_kind IS NULL OR v_entity_kind IN ('pay_batch', 'pay_batches'))
        AND correction_request_candidate.pay_batch_id IS NOT NULL
        AND UPPER(BTRIM(COALESCE(correction_request_candidate_batch.status, ''))) NOT IN ('CANCELLED','CANCELED')
        AND UPPER(BTRIM(COALESCE(correction_request_candidate.status, ''))) NOT IN ('APPLIED','RESOLVED','COMPLETE','COMPLETED','CANCELLED','CANCELED')

      UNION

      SELECT correction_work_candidate.pay_batch_id
      FROM public.pay_payment_correction_work_items AS correction_work_candidate
      JOIN public.pay_batches AS correction_work_candidate_batch
        ON correction_work_candidate_batch.id = correction_work_candidate.pay_batch_id
      WHERE p_entity_id IS NULL
        AND (v_entity_kind IS NULL OR v_entity_kind IN ('pay_batch', 'pay_batches'))
        AND correction_work_candidate.pay_batch_id IS NOT NULL
        AND UPPER(BTRIM(COALESCE(correction_work_candidate_batch.status, ''))) NOT IN ('CANCELLED','CANCELED')
        AND UPPER(BTRIM(COALESCE(correction_work_candidate.status, ''))) IN ('PENDING','PROCESSING','BLOCKED','FAILED','FAILED_RETRYABLE','FAILED_FINAL','APPLIED_WITH_BLOCKERS')

      UNION

      SELECT operation_candidate.pay_batch_id
      FROM public.banking_pay_operations AS operation_candidate
      JOIN public.pay_batches AS operation_candidate_batch
        ON operation_candidate_batch.id = operation_candidate.pay_batch_id
      WHERE p_entity_id IS NULL
        AND (v_entity_kind IS NULL OR v_entity_kind IN ('pay_batch', 'pay_batches'))
        AND operation_candidate.pay_batch_id IS NOT NULL
        AND UPPER(BTRIM(COALESCE(operation_candidate_batch.status, ''))) NOT IN ('CANCELLED','CANCELED')
        AND (
          UPPER(BTRIM(COALESCE(operation_candidate.operation_type, ''))) = 'PAYMENT_RETRY_BLOCKED_FUNDS'
          OR UPPER(BTRIM(COALESCE(operation_candidate.status, ''))) IN ('REVIEW_REQUIRED','FAILED')
          OR jsonb_typeof(operation_candidate.progress_json->'provider_submit_diagnostic') = 'object'
          OR jsonb_typeof(operation_candidate.result_json->'provider_submit_diagnostic') = 'object'
          OR jsonb_typeof(operation_candidate.error_json->'provider_submit_diagnostic') = 'object'
        )

      UNION

      SELECT remittance_candidate.context_id AS pay_batch_id
      FROM public.mail_outbox AS remittance_candidate
      JOIN public.pay_batches AS remittance_candidate_batch
        ON remittance_candidate_batch.id = remittance_candidate.context_id
      WHERE p_entity_id IS NULL
        AND (v_entity_kind IS NULL OR v_entity_kind IN ('pay_batch', 'pay_batches'))
        AND remittance_candidate.context_id IS NOT NULL
        AND UPPER(BTRIM(COALESCE(remittance_candidate_batch.status, ''))) NOT IN ('CANCELLED','CANCELED')
        AND UPPER(BTRIM(COALESCE(remittance_candidate.type, ''))) = 'REMITTANCE'
        AND LOWER(BTRIM(COALESCE(remittance_candidate.context_kind, ''))) IN ('pay_batch','pay_batches')
        AND UPPER(BTRIM(COALESCE(remittance_candidate.status::text, ''))) = 'FAILED'

      UNION

      SELECT carry_forward_candidate.source_pay_batch_id AS pay_batch_id
      FROM public.pay_manual_adjustment_carry_forwards AS carry_forward_candidate
      JOIN public.pay_batches AS carry_forward_candidate_batch
        ON carry_forward_candidate_batch.id = carry_forward_candidate.source_pay_batch_id
      WHERE p_entity_id IS NULL
        AND (v_entity_kind IS NULL OR v_entity_kind IN ('pay_batch', 'pay_batches'))
        AND carry_forward_candidate.source_pay_batch_id IS NOT NULL
        AND UPPER(BTRIM(COALESCE(carry_forward_candidate_batch.status, ''))) NOT IN ('CANCELLED','CANCELED')
        AND UPPER(BTRIM(COALESCE(carry_forward_candidate.status, ''))) = 'PENDING_CARRY_FORWARD'
    ) AS candidate_scope
    WHERE candidate_scope.pay_batch_id IS NOT NULL
  ),
  grouped_banking_pay_diagnostic_scope AS MATERIALIZED (
    SELECT
      scoped_pay_batches.id AS pay_batch_id,
      scoped_pay_batches.status AS batch_status,
      scoped_pay_batches.rail_provider_snapshot,
      scoped_pay_batches.rail_env_snapshot,
      GREATEST(
        COALESCE(scoped_pay_batches.last_status_checked_at_utc, '-infinity'::timestamptz),
        COALESCE(scoped_pay_batches.last_funds_check_at_utc, '-infinity'::timestamptz),
        COALESCE(scoped_pay_batches.execution_committed_at_utc, '-infinity'::timestamptz),
        COALESCE(scoped_pay_batches.created_at_utc, '-infinity'::timestamptz)
      ) AS sort_at_utc,
      COALESCE(
        public.pay_payment_cancelability_diagnostic(
          scoped_pay_batches.id,
          jsonb_build_object('scope_type', 'BATCH'),
          p_actor_user_id,
          'PAYMENT_ISSUES_TAB'
        ),
        '{}'::jsonb
      ) AS diagnostic_json
    FROM grouped_banking_pay_diagnostic_pay_batch_scope AS diagnostic_scope
    JOIN public.pay_batches AS scoped_pay_batches
      ON scoped_pay_batches.id = diagnostic_scope.pay_batch_id
  ),
  grouped_banking_pay_correction_scope AS MATERIALIZED (
    SELECT
      public.pay_payment_correction_requests.pay_batch_id,
      public.pay_payment_correction_requests.id AS correction_request_id,
      UPPER(BTRIM(COALESCE(public.pay_payment_correction_requests.correction_kind, ''))) AS correction_kind,
      UPPER(BTRIM(COALESCE(public.pay_payment_correction_requests.status, ''))) AS correction_status,
      public.pay_payment_correction_requests.updated_at_utc,
      public.pay_payment_correction_requests.created_at_utc,
      COUNT(public.pay_payment_correction_work_items.id)::integer AS work_total,
      COUNT(public.pay_payment_correction_work_items.id) FILTER (
        WHERE UPPER(BTRIM(COALESCE(public.pay_payment_correction_work_items.status, ''))) IN ('APPLIED','DONE','COMPLETE','COMPLETED')
      )::integer AS work_completed
    FROM public.pay_payment_correction_requests
    LEFT JOIN public.pay_payment_correction_work_items
      ON public.pay_payment_correction_work_items.correction_request_id = public.pay_payment_correction_requests.id
    WHERE EXISTS (
      SELECT 1
      FROM grouped_banking_pay_diagnostic_scope
      WHERE grouped_banking_pay_diagnostic_scope.pay_batch_id = public.pay_payment_correction_requests.pay_batch_id
    )
      AND UPPER(BTRIM(COALESCE(public.pay_payment_correction_requests.status, ''))) NOT IN ('APPLIED','COMPLETE','COMPLETED','CANCELLED','CANCELED')
    GROUP BY
      public.pay_payment_correction_requests.pay_batch_id,
      public.pay_payment_correction_requests.id,
      public.pay_payment_correction_requests.correction_kind,
      public.pay_payment_correction_requests.status,
      public.pay_payment_correction_requests.updated_at_utc,
      public.pay_payment_correction_requests.created_at_utc
  ),
  grouped_banking_pay_carry_forward_scope AS MATERIALIZED (
    SELECT
      public.pay_manual_adjustment_carry_forwards.source_pay_batch_id AS pay_batch_id,
      COUNT(*)::integer AS carry_forward_count,
      MAX(grouped_banking_pay_diagnostic_scope.sort_at_utc) AS sort_at_utc
    FROM public.pay_manual_adjustment_carry_forwards
    JOIN grouped_banking_pay_diagnostic_scope
      ON grouped_banking_pay_diagnostic_scope.pay_batch_id = public.pay_manual_adjustment_carry_forwards.source_pay_batch_id
    WHERE public.pay_manual_adjustment_carry_forwards.source_pay_batch_id IS NOT NULL
      AND UPPER(BTRIM(COALESCE(public.pay_manual_adjustment_carry_forwards.status, ''))) = 'PENDING_CARRY_FORWARD'
    GROUP BY public.pay_manual_adjustment_carry_forwards.source_pay_batch_id
  ),
  grouped_banking_pay_alert_seeds AS MATERIALIZED (
    SELECT
      'PROVIDER_OUTAGE_RETRY_LATER'::text AS alert_kind,
      92::integer AS severity_rank,
      'ACTION_REQUIRED'::text AS severity,
      grouped_banking_pay_diagnostic_scope.pay_batch_id,
      NULL::text AS payload_source_kind,
      NULL::uuid AS payload_source_id,
      grouped_banking_pay_diagnostic_scope.sort_at_utc
    FROM grouped_banking_pay_diagnostic_scope
    WHERE COALESCE(NULLIF(BTRIM(grouped_banking_pay_diagnostic_scope.diagnostic_json->>'payment_lifecycle_state'), ''), '') = 'PROVIDER_OUTAGE_RETRY_LATER'
       OR COALESCE(NULLIF(BTRIM(grouped_banking_pay_diagnostic_scope.diagnostic_json->>'recommended_action'), ''), '') = 'RETRY_PROVIDER_LATER'
       OR LOWER(BTRIM(COALESCE(grouped_banking_pay_diagnostic_scope.diagnostic_json->>'requires_retry_later', 'false'))) IN ('true','t','1','yes','y','on')

    UNION ALL
    SELECT
      'PROVIDER_OUTAGE_RETRY_LATER'::text AS alert_kind,
      92::integer AS severity_rank,
      'PROGRESS'::text AS severity,
      retry_operation.pay_batch_id,
      'banking_pay_operation'::text AS payload_source_kind,
      retry_operation.id AS payload_source_id,
      COALESCE(retry_operation.updated_at_utc, retry_operation.created_at_utc, now()) AS sort_at_utc
    FROM public.banking_pay_operations AS retry_operation
    WHERE UPPER(BTRIM(COALESCE(retry_operation.operation_type, ''))) = 'PAYMENT_RETRY_BLOCKED_FUNDS'
      AND UPPER(BTRIM(COALESCE(retry_operation.status, ''))) NOT IN ('COMPLETE','COMPLETED','SUCCEEDED','SUCCESS','FAILED','FAILED_FINAL','CANCELLED','CANCELED')
      AND EXISTS (
        SELECT 1
        FROM grouped_banking_pay_diagnostic_scope
        WHERE grouped_banking_pay_diagnostic_scope.pay_batch_id = retry_operation.pay_batch_id
      )

    UNION ALL
    SELECT
      'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER',
      94,
      'ACTION_REQUIRED',
      grouped_banking_pay_diagnostic_scope.pay_batch_id,
      NULL::text,
      NULL::uuid,
      grouped_banking_pay_diagnostic_scope.sort_at_utc
    FROM grouped_banking_pay_diagnostic_scope
    WHERE (
      COALESCE(NULLIF(BTRIM(grouped_banking_pay_diagnostic_scope.diagnostic_json->>'payment_lifecycle_state'), ''), '') = 'PROVIDER_OUTCOME_UNKNOWN'
       OR COALESCE(NULLIF(BTRIM(grouped_banking_pay_diagnostic_scope.diagnostic_json->>'recommended_action'), ''), '') = 'CHECK_PROVIDER_STATUS'
       OR LOWER(BTRIM(COALESCE(grouped_banking_pay_diagnostic_scope.diagnostic_json->>'requires_bank_check', 'false'))) IN ('true','t','1','yes','y','on')
    )
      AND LOWER(BTRIM(COALESCE(grouped_banking_pay_diagnostic_scope.diagnostic_json->>'clean_paid_or_settled_success', 'false'))) NOT IN ('true','t','1','yes','y','on')
      AND NOT (
        COALESCE(NULLIF(BTRIM(grouped_banking_pay_diagnostic_scope.diagnostic_json->>'payment_lifecycle_state'), ''), '') = 'PAID_OR_SETTLED'
        AND LOWER(BTRIM(COALESCE(grouped_banking_pay_diagnostic_scope.diagnostic_json->>'has_actual_recovery_context', 'false'))) NOT IN ('true','t','1','yes','y','on')
        AND LOWER(BTRIM(COALESCE(grouped_banking_pay_diagnostic_scope.diagnostic_json->>'has_no_money_context', 'false'))) NOT IN ('true','t','1','yes','y','on')
        AND LOWER(BTRIM(COALESCE(grouped_banking_pay_diagnostic_scope.diagnostic_json->>'has_failed_remittance_context', 'false'))) NOT IN ('true','t','1','yes','y','on')
        AND LOWER(BTRIM(COALESCE(grouped_banking_pay_diagnostic_scope.diagnostic_json->>'has_unmatched_or_ambiguous_event_context', 'false'))) NOT IN ('true','t','1','yes','y','on')
      )

    UNION ALL
    SELECT
      'TERMINAL_NO_MONEY_REWIND_AVAILABLE',
      93,
      'ACTION_REQUIRED',
      grouped_banking_pay_diagnostic_scope.pay_batch_id,
      NULL::text,
      NULL::uuid,
      grouped_banking_pay_diagnostic_scope.sort_at_utc
    FROM grouped_banking_pay_diagnostic_scope
    WHERE COALESCE(NULLIF(BTRIM(grouped_banking_pay_diagnostic_scope.diagnostic_json->>'payment_lifecycle_state'), ''), '') IN ('PROVIDER_CANCELLED_NO_MONEY','PROVIDER_FAILED_NO_MONEY')
      AND LOWER(BTRIM(COALESCE(grouped_banking_pay_diagnostic_scope.diagnostic_json->>'can_no_money_unwind', 'false'))) IN ('true','t','1','yes','y','on')
      AND NOT EXISTS (
        SELECT 1
        FROM grouped_banking_pay_correction_scope
        WHERE grouped_banking_pay_correction_scope.pay_batch_id = grouped_banking_pay_diagnostic_scope.pay_batch_id
          AND grouped_banking_pay_correction_scope.correction_kind IN ('NO_MONEY_UNWIND','MANUAL_EVIDENCE_NO_MONEY')
      )

    UNION ALL
    SELECT
      'PAID_SETTLED_RECOVERY_REQUIRED',
      94,
      'ACTION_REQUIRED',
      grouped_banking_pay_diagnostic_scope.pay_batch_id,
      NULL::text,
      NULL::uuid,
      grouped_banking_pay_diagnostic_scope.sort_at_utc
    FROM grouped_banking_pay_diagnostic_scope
    WHERE (
      COALESCE(NULLIF(BTRIM(grouped_banking_pay_diagnostic_scope.diagnostic_json->>'recommended_action'), ''), '') = 'AMEND_AND_RECOVER_OVERPAYMENT'
       OR LOWER(BTRIM(COALESCE(grouped_banking_pay_diagnostic_scope.diagnostic_json->>'can_recover_overpayment', 'false'))) IN ('true','t','1','yes','y','on')
    )
      AND LOWER(BTRIM(COALESCE(grouped_banking_pay_diagnostic_scope.diagnostic_json->>'has_actual_recovery_context', 'false'))) IN ('true','t','1','yes','y','on')

    UNION ALL
    SELECT
      'CANCELLATION_RACED_WITH_PROVIDER_SUBMIT',
      94,
      'ACTION_REQUIRED',
      grouped_banking_pay_diagnostic_scope.pay_batch_id,
      NULL::text,
      NULL::uuid,
      grouped_banking_pay_diagnostic_scope.sort_at_utc
    FROM grouped_banking_pay_diagnostic_scope
    WHERE COALESCE(grouped_banking_pay_diagnostic_scope.diagnostic_json->'blockers', '[]'::jsonb)::text LIKE '%CANCELLATION_RACED_WITH_PROVIDER_SUBMIT%'
       OR COALESCE(grouped_banking_pay_diagnostic_scope.diagnostic_json->'blockers', '[]'::jsonb)::text LIKE '%PROVIDER_SUBMISSION_ALREADY_CLAIMED%'
       OR COALESCE(grouped_banking_pay_diagnostic_scope.diagnostic_json->'blockers', '[]'::jsonb)::text LIKE '%PROVIDER_SUBMISSION_IN_PROGRESS%'

    UNION ALL
    SELECT
      'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS',
      91,
      'ACTION_REQUIRED',
      grouped_banking_pay_diagnostic_scope.pay_batch_id,
      NULL::text,
      NULL::uuid,
      grouped_banking_pay_diagnostic_scope.sort_at_utc
    FROM grouped_banking_pay_diagnostic_scope
    WHERE CASE
      WHEN jsonb_typeof(COALESCE(grouped_banking_pay_diagnostic_scope.diagnostic_json->'carry_forward_blockers', '[]'::jsonb)) = 'array'
        THEN jsonb_array_length(COALESCE(grouped_banking_pay_diagnostic_scope.diagnostic_json->'carry_forward_blockers', '[]'::jsonb))
      ELSE 0
    END > 0
       OR COALESCE(grouped_banking_pay_diagnostic_scope.diagnostic_json->'blockers', '[]'::jsonb)::text LIKE '%SOURCE_LESS_MANUAL_ADJUSTMENT_AMBIGUOUS%'

    UNION ALL
    SELECT
      'AUTO_UNWIND_PROGRESS',
      85,
      'PROGRESS',
      grouped_banking_pay_correction_scope.pay_batch_id,
      'pay_payment_correction_request',
      grouped_banking_pay_correction_scope.correction_request_id,
      COALESCE(grouped_banking_pay_correction_scope.updated_at_utc, grouped_banking_pay_correction_scope.created_at_utc)
    FROM grouped_banking_pay_correction_scope
    WHERE grouped_banking_pay_correction_scope.correction_kind IN ('NO_MONEY_UNWIND','MANUAL_EVIDENCE_NO_MONEY')

    UNION ALL
    SELECT
      'WHOLE_BATCH_CANCELLATION_PROGRESS',
      85,
      'PROGRESS',
      grouped_banking_pay_correction_scope.pay_batch_id,
      'pay_payment_correction_request',
      grouped_banking_pay_correction_scope.correction_request_id,
      COALESCE(grouped_banking_pay_correction_scope.updated_at_utc, grouped_banking_pay_correction_scope.created_at_utc)
    FROM grouped_banking_pay_correction_scope
    WHERE grouped_banking_pay_correction_scope.correction_kind = 'PRE_BANK_CANCEL'

    UNION ALL
    SELECT
      'MANUAL_ADJUSTMENTS_CARRIED_FORWARD',
      70,
      'INFO',
      grouped_banking_pay_carry_forward_scope.pay_batch_id,
      NULL::text,
      NULL::uuid,
      grouped_banking_pay_carry_forward_scope.sort_at_utc
    FROM grouped_banking_pay_carry_forward_scope
    WHERE COALESCE(grouped_banking_pay_carry_forward_scope.carry_forward_count, 0) > 0
  ),
  grouped_banking_pay_alerts AS MATERIALIZED (
    SELECT
      grouped_banking_pay_alert_seeds.alert_kind,
      grouped_banking_pay_alert_seeds.severity,
      grouped_banking_pay_alert_seeds.severity_rank,
      'pay_batch'::text AS entity_kind,
      grouped_banking_pay_alert_seeds.pay_batch_id AS entity_id,
      grouped_banking_pay_alert_seeds.pay_batch_id,
      grouped_banking_pay_alert_seeds.payload_source_kind,
      grouped_banking_pay_alert_seeds.payload_source_id,
      jsonb_strip_nulls(
        jsonb_build_object(
          'payload_is_grouped', true,
          'alert_kind', grouped_banking_pay_alert_seeds.alert_kind,
          'issue_kind', grouped_banking_pay_alert_seeds.alert_kind,
          'legacy_alert_kind', grouped_banking_pay_alert_seeds.alert_kind,
          'pay_batch_id', grouped_banking_pay_alert_seeds.pay_batch_id::text,
          'link_target', 'banking_pay_batch',
          'link_tab', CASE
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PROVIDER_OUTAGE_RETRY_LATER' THEN 'overview'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'WHOLE_BATCH_CANCELLATION_PROGRESS' THEN 'overview'
            ELSE 'current_payment_status'
          END,
          'alert_severity', grouped_banking_pay_alert_seeds.severity,
          'severity', grouped_banking_pay_alert_seeds.severity
        )
        || jsonb_build_object(
          'stable_issue_key', CONCAT_WS(
            ':',
            grouped_banking_pay_alert_seeds.pay_batch_id::text,
            grouped_banking_pay_alert_seeds.alert_kind,
            CASE
              WHEN grouped_banking_pay_alert_seeds.alert_kind IN (
                'PROVIDER_OUTAGE_RETRY_LATER',
                'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER',
                'TERMINAL_NO_MONEY_REWIND_AVAILABLE',
                'PAID_SETTLED_RECOVERY_REQUIRED',
                'CANCELLATION_RACED_WITH_PROVIDER_SUBMIT',
                'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS',
                'MANUAL_ADJUSTMENTS_CARRIED_FORWARD'
              ) THEN 'GROUPED'
              ELSE COALESCE(NULLIF(BTRIM(grouped_banking_pay_alert_seeds.payload_source_kind), ''), 'BATCH')
            END,
            CASE
              WHEN grouped_banking_pay_alert_seeds.alert_kind IN (
                'PROVIDER_OUTAGE_RETRY_LATER',
                'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER',
                'TERMINAL_NO_MONEY_REWIND_AVAILABLE',
                'PAID_SETTLED_RECOVERY_REQUIRED',
                'CANCELLATION_RACED_WITH_PROVIDER_SUBMIT',
                'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS',
                'MANUAL_ADJUSTMENTS_CARRIED_FORWARD'
              ) THEN 'ACTIVE'
              ELSE COALESCE(grouped_banking_pay_alert_seeds.payload_source_id::text, 'NO_SOURCE')
            END
          ),
          'dedupe_key', CONCAT_WS(
            ':',
            grouped_banking_pay_alert_seeds.pay_batch_id::text,
            grouped_banking_pay_alert_seeds.alert_kind,
            CASE
              WHEN grouped_banking_pay_alert_seeds.alert_kind IN (
                'PROVIDER_OUTAGE_RETRY_LATER',
                'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER',
                'TERMINAL_NO_MONEY_REWIND_AVAILABLE',
                'PAID_SETTLED_RECOVERY_REQUIRED',
                'CANCELLATION_RACED_WITH_PROVIDER_SUBMIT',
                'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS',
                'MANUAL_ADJUSTMENTS_CARRIED_FORWARD'
              ) THEN 'GROUPED'
              ELSE COALESCE(grouped_banking_pay_alert_seeds.payload_source_id::text, 'NO_SOURCE')
            END
          ),
          'payload_source_kind', NULLIF(BTRIM(COALESCE(grouped_banking_pay_alert_seeds.payload_source_kind, '')), ''),
          'payload_source_id', CASE WHEN grouped_banking_pay_alert_seeds.payload_source_id IS NULL THEN NULL::text ELSE grouped_banking_pay_alert_seeds.payload_source_id::text END
        )
        || jsonb_build_object(
          'user_label', CASE
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PROVIDER_OUTAGE_RETRY_LATER'
              AND UPPER(BTRIM(COALESCE(grouped_banking_pay_alert_seeds.severity, ''))) = 'PROGRESS'
              THEN 'Retrying unsent payments'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PROVIDER_OUTAGE_RETRY_LATER'
              THEN 'Bank unavailable — unsent payments can be retried'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER'
              THEN 'Provider outcome unknown — check provider'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'TERMINAL_NO_MONEY_REWIND_AVAILABLE'
              THEN 'Failed payments — Rewind financials available'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'AUTO_UNWIND_PROGRESS'
              THEN 'Rewinding failed payments'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'WHOLE_BATCH_CANCELLATION_PROGRESS'
              THEN 'Cancelling scheduled batch'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'MANUAL_ADJUSTMENTS_CARRIED_FORWARD'
              THEN 'Manual adjustments carried forward'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS'
              THEN 'Manual adjustment blockers — review required'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PAID_SETTLED_RECOVERY_REQUIRED'
              THEN 'Paid — recovery required'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'CANCELLATION_RACED_WITH_PROVIDER_SUBMIT'
              THEN 'Cancellation conflict — provider submission already started'
            ELSE INITCAP(REPLACE(LOWER(grouped_banking_pay_alert_seeds.alert_kind), '_', ' '))
          END,
          'user_description', CASE
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PROVIDER_OUTAGE_RETRY_LATER'
              AND UPPER(BTRIM(COALESCE(grouped_banking_pay_alert_seeds.severity, ''))) = 'PROGRESS'
              THEN 'Retrying unsent payments is in progress. You can continue using CloudTMS while this runs.'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PROVIDER_OUTAGE_RETRY_LATER'
              THEN 'The bank/provider was unavailable before the payment request was sent. Retry unsent payments from Banking Pay Overview.'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER'
              THEN 'A provider request may have been sent, but the outcome is not confirmed. Open Current Payment Status and check the provider outcome.'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'TERMINAL_NO_MONEY_REWIND_AVAILABLE'
              THEN 'The provider/bank outcome indicates no money moved. Open Current Payment Status and rewind financials where safe.'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'AUTO_UNWIND_PROGRESS'
              THEN 'Automatic no-money unwind is running and this alert updates grouped progress.'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'WHOLE_BATCH_CANCELLATION_PROGRESS'
              THEN 'Scheduled local cancellation is running in chunks and this alert updates grouped progress.'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'MANUAL_ADJUSTMENTS_CARRIED_FORWARD'
              THEN 'Safe source-less manual adjustments were carried forward and will be included in the next pay run.'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS'
              THEN 'One or more manual adjustments cannot be safely carried forward automatically.'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PAID_SETTLED_RECOVERY_REQUIRED'
              THEN 'Money appears to have moved. Amend the timesheet and recover the overpayment in the next pay run rather than unwinding the payment.'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'CANCELLATION_RACED_WITH_PROVIDER_SUBMIT'
              THEN 'Cancellation could not proceed because provider submission had already started.'
            ELSE 'Open Banking Pay for details.'
          END,
          'required_user_action', CASE
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PROVIDER_OUTAGE_RETRY_LATER'
              AND UPPER(BTRIM(COALESCE(grouped_banking_pay_alert_seeds.severity, ''))) = 'PROGRESS'
              THEN 'Monitor retry progress from Banking Pay Overview.'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PROVIDER_OUTAGE_RETRY_LATER'
              THEN 'Retry unsent payments from Banking Pay Overview.'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER'
              THEN 'Open Current Payment Status and check the provider outcome.'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'TERMINAL_NO_MONEY_REWIND_AVAILABLE'
              THEN 'Open Current Payment Status and rewind financials where no money moved.'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'AUTO_UNWIND_PROGRESS'
              THEN 'Monitor rewind progress.'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'WHOLE_BATCH_CANCELLATION_PROGRESS'
              THEN 'Monitor cancellation progress in Banking Pay Overview.'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'MANUAL_ADJUSTMENTS_CARRIED_FORWARD'
              THEN 'Review carried-forward manual adjustments in the next pay run.'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS'
              THEN 'Open Current Payment Status and review ambiguous manual adjustment blockers.'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PAID_SETTLED_RECOVERY_REQUIRED'
              THEN 'Open Current Payment Status and recover overpayment in next pay run.'
            WHEN grouped_banking_pay_alert_seeds.alert_kind = 'CANCELLATION_RACED_WITH_PROVIDER_SUBMIT'
              THEN 'Open Current Payment Status and check provider submission before continuing.'
            ELSE 'Open Banking Pay.'
          END
        )
      ) AS fingerprint_payload_json,
      CASE
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PROVIDER_OUTAGE_RETRY_LATER'
          AND UPPER(BTRIM(COALESCE(grouped_banking_pay_alert_seeds.severity, ''))) = 'PROGRESS'
          THEN 'Retrying unsent payments'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PROVIDER_OUTAGE_RETRY_LATER'
          THEN 'Bank unavailable — unsent payments can be retried'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER'
          THEN 'Provider outcome unknown — check provider'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'TERMINAL_NO_MONEY_REWIND_AVAILABLE'
          THEN 'Failed payments — Rewind financials available'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'AUTO_UNWIND_PROGRESS'
          THEN 'Rewinding failed payments'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'WHOLE_BATCH_CANCELLATION_PROGRESS'
          THEN 'Cancelling scheduled batch'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'MANUAL_ADJUSTMENTS_CARRIED_FORWARD'
          THEN 'Manual adjustments carried forward'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS'
          THEN 'Manual adjustment blockers — review required'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PAID_SETTLED_RECOVERY_REQUIRED'
          THEN 'Paid — recovery required'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'CANCELLATION_RACED_WITH_PROVIDER_SUBMIT'
          THEN 'Cancellation conflict — provider submission already started'
        ELSE INITCAP(REPLACE(LOWER(grouped_banking_pay_alert_seeds.alert_kind), '_', ' '))
      END::text AS label,
      CASE
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PROVIDER_OUTAGE_RETRY_LATER'
          AND UPPER(BTRIM(COALESCE(grouped_banking_pay_alert_seeds.severity, ''))) = 'PROGRESS'
          THEN 'Retrying unsent payments'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PROVIDER_OUTAGE_RETRY_LATER'
          THEN 'Bank unavailable — unsent payments can be retried'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER'
          THEN 'Provider outcome unknown — check provider'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'TERMINAL_NO_MONEY_REWIND_AVAILABLE'
          THEN 'Failed payments — Rewind financials available'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'AUTO_UNWIND_PROGRESS'
          THEN 'Rewinding failed payments'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'WHOLE_BATCH_CANCELLATION_PROGRESS'
          THEN 'Cancelling scheduled batch'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'MANUAL_ADJUSTMENTS_CARRIED_FORWARD'
          THEN 'Manual adjustments carried forward'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS'
          THEN 'Manual adjustment blockers — review required'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PAID_SETTLED_RECOVERY_REQUIRED'
          THEN 'Paid — recovery required'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'CANCELLATION_RACED_WITH_PROVIDER_SUBMIT'
          THEN 'Cancellation conflict — provider submission already started'
        ELSE INITCAP(REPLACE(LOWER(grouped_banking_pay_alert_seeds.alert_kind), '_', ' '))
      END::text AS title,
      CASE
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PROVIDER_OUTAGE_RETRY_LATER'
          AND UPPER(BTRIM(COALESCE(grouped_banking_pay_alert_seeds.severity, ''))) = 'PROGRESS'
          THEN 'Retrying unsent payments is in progress. You can continue using CloudTMS while this runs.'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PROVIDER_OUTAGE_RETRY_LATER'
          THEN 'The bank/provider was unavailable before the payment request was sent. Retry unsent payments from Banking Pay Overview.'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER'
          THEN 'A provider request may have been sent, but the outcome is not confirmed. Open Current Payment Status and check the provider outcome.'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'TERMINAL_NO_MONEY_REWIND_AVAILABLE'
          THEN 'The provider/bank outcome indicates no money moved. Open Current Payment Status and rewind financials where safe.'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'AUTO_UNWIND_PROGRESS'
          THEN 'Automatic no-money unwind is running and this alert updates grouped progress.'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'WHOLE_BATCH_CANCELLATION_PROGRESS'
          THEN 'Scheduled local cancellation is running in chunks and this alert updates grouped progress.'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'MANUAL_ADJUSTMENTS_CARRIED_FORWARD'
          THEN 'Safe source-less manual adjustments were carried forward and will be included in the next pay run.'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS'
          THEN 'One or more manual adjustments cannot be safely carried forward automatically.'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PAID_SETTLED_RECOVERY_REQUIRED'
          THEN 'Money appears to have moved. Amend the timesheet and recover the overpayment in the next pay run rather than unwinding the payment.'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'CANCELLATION_RACED_WITH_PROVIDER_SUBMIT'
          THEN 'Cancellation could not proceed because provider submission had already started.'
        ELSE 'Open Banking Pay for details.'
      END::text AS description,
      CASE
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PROVIDER_OUTAGE_RETRY_LATER'
          AND UPPER(BTRIM(COALESCE(grouped_banking_pay_alert_seeds.severity, ''))) = 'PROGRESS'
          THEN 'Monitor retry progress from Banking Pay Overview.'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PROVIDER_OUTAGE_RETRY_LATER'
          THEN 'Retry unsent payments from Banking Pay Overview.'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER'
          THEN 'Open Current Payment Status and check the provider outcome.'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'TERMINAL_NO_MONEY_REWIND_AVAILABLE'
          THEN 'Open Current Payment Status and rewind financials where no money moved.'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'AUTO_UNWIND_PROGRESS'
          THEN 'Monitor rewind progress.'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'WHOLE_BATCH_CANCELLATION_PROGRESS'
          THEN 'Monitor cancellation progress in Banking Pay Overview.'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'MANUAL_ADJUSTMENTS_CARRIED_FORWARD'
          THEN 'Review carried-forward manual adjustments in the next pay run.'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS'
          THEN 'Open Current Payment Status and review ambiguous manual adjustment blockers.'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'PAID_SETTLED_RECOVERY_REQUIRED'
          THEN 'Open Current Payment Status and recover overpayment in next pay run.'
        WHEN grouped_banking_pay_alert_seeds.alert_kind = 'CANCELLATION_RACED_WITH_PROVIDER_SUBMIT'
          THEN 'Open Current Payment Status and check provider submission before continuing.'
        ELSE 'Open Banking Pay.'
      END::text AS action_guidance,
      grouped_banking_pay_alert_seeds.sort_at_utc
    FROM grouped_banking_pay_alert_seeds
  ),
  latest_success_events AS MATERIALIZED (
    SELECT DISTINCT ON (success_event.pay_batch_id, success_event.alert_kind)
      success_event.id,
      success_event.pay_batch_id,
      success_event.alert_kind,
      success_event.event_key,
      COALESCE(success_event.payload_json, '{}'::jsonb) AS payload_json,
      success_event.occurred_at_utc
    FROM public.banking_alert_success_events AS success_event
    WHERE success_event.expires_at_utc > now()
      AND UPPER(BTRIM(COALESCE(success_event.alert_kind, ''))) IN (
        'BATCH_SCHEDULED_SUCCESS',
        'BATCH_SETTLED_SUCCESS',
        'BATCH_CANCELLATION_SUCCESS'
      )
    ORDER BY
      success_event.pay_batch_id,
      success_event.alert_kind,
      success_event.occurred_at_utc DESC,
      success_event.id DESC
  ),
  success_event_alerts AS MATERIALIZED (
    SELECT
      UPPER(BTRIM(latest_success_events.alert_kind))::text AS alert_kind,
      'info'::text AS severity,
      10::integer AS severity_rank,
      'pay_batch'::text AS entity_kind,
      latest_success_events.pay_batch_id AS entity_id,
      latest_success_events.pay_batch_id,
      'banking_alert_success_event'::text AS payload_source_kind,
      latest_success_events.id AS payload_source_id,
      jsonb_strip_nulls(
        latest_success_events.payload_json
        || jsonb_build_object(
          'alert_kind', UPPER(BTRIM(latest_success_events.alert_kind)),
          'issue_kind', UPPER(BTRIM(latest_success_events.alert_kind)),
          'stable_issue_key', COALESCE(
            NULLIF(BTRIM(latest_success_events.payload_json ->> 'stable_issue_key'), ''),
            latest_success_events.pay_batch_id::text || ':' || UPPER(BTRIM(latest_success_events.alert_kind)) || ':' || latest_success_events.event_key
          ),
          'dedupe_key', COALESCE(
            NULLIF(BTRIM(latest_success_events.payload_json ->> 'dedupe_key'), ''),
            latest_success_events.pay_batch_id::text || ':' || UPPER(BTRIM(latest_success_events.alert_kind)) || ':' || latest_success_events.event_key
          ),
          'payload_source_kind', 'banking_alert_success_event',
          'payload_source_id', latest_success_events.id::text,
          'pay_batch_id', latest_success_events.pay_batch_id::text,
          'alert_candidate_is_success_only', true,
          'is_success_only', true
        )
      ) AS fingerprint_payload_json,
      COALESCE(
        NULLIF(BTRIM(latest_success_events.payload_json ->> 'user_label'), ''),
        CASE WHEN UPPER(BTRIM(latest_success_events.alert_kind)) = 'BATCH_SCHEDULED_SUCCESS'
          THEN 'Future payment batch scheduled'
          ELSE 'Payment batch settled'
        END
      )::text AS label,
      COALESCE(
        NULLIF(BTRIM(latest_success_events.payload_json ->> 'user_label'), ''),
        CASE WHEN UPPER(BTRIM(latest_success_events.alert_kind)) = 'BATCH_SCHEDULED_SUCCESS'
          THEN 'Future payment batch scheduled'
          ELSE 'Payment batch settled'
        END
      )::text AS title,
      COALESCE(
        NULLIF(BTRIM(latest_success_events.payload_json ->> 'user_description'), ''),
        'Payment batch lifecycle completed successfully.'
      )::text AS description,
      COALESCE(
        NULLIF(BTRIM(latest_success_events.payload_json ->> 'required_user_action'), ''),
        'Review or clear this Banking alert.'
      )::text AS action_guidance,
      latest_success_events.occurred_at_utc AS sort_at_utc
    FROM latest_success_events
  ),
  raw_current_alerts AS MATERIALIZED (
    SELECT * FROM blocked_funds_alerts
    UNION ALL
    SELECT bank_event_alerts.*
    FROM bank_event_alerts
    WHERE NOT EXISTS (
      SELECT 1
      FROM grouped_banking_pay_alerts AS grouped_replacement_alerts
      WHERE grouped_replacement_alerts.pay_batch_id = bank_event_alerts.pay_batch_id
        AND (
          (
            bank_event_alerts.alert_kind IN ('BANK_REJECTED_PAYMENT')
            AND grouped_replacement_alerts.alert_kind IN (
              'TERMINAL_NO_MONEY_REWIND_AVAILABLE',
              'AUTO_UNWIND_PROGRESS'
            )
          )
          OR (
            bank_event_alerts.alert_kind IN ('BANK_RETURNED_PAYMENT')
            AND grouped_replacement_alerts.alert_kind IN (
              'TERMINAL_NO_MONEY_REWIND_AVAILABLE',
              'AUTO_UNWIND_PROGRESS',
              'PAID_SETTLED_RECOVERY_REQUIRED'
            )
          )
          OR (
            bank_event_alerts.alert_kind IN ('RAIL_SUBMISSION_UNKNOWN_OR_TIMEOUT')
            AND grouped_replacement_alerts.alert_kind = 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER'
          )
          OR (
            bank_event_alerts.alert_kind IN ('AMBIGUOUS_PAYMENT_REVIEW_REQUIRED')
            AND grouped_replacement_alerts.alert_kind IN (
              'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER',
              'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS'
            )
          )
          OR (
            bank_event_alerts.alert_kind IN ('PAYMENT_CORRECTION_FAILED','PAYMENT_CORRECTION_BLOCKED')
            AND grouped_replacement_alerts.alert_kind IN (
              'AUTO_UNWIND_PROGRESS',
              'WHOLE_BATCH_CANCELLATION_PROGRESS',
              'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS',
              'CANCELLATION_RACED_WITH_PROVIDER_SUBMIT',
              'PAID_SETTLED_RECOVERY_REQUIRED'
            )
          )
        )
    )
    UNION ALL
    SELECT transfer_alerts.*
    FROM transfer_alerts
    WHERE NOT EXISTS (
      SELECT 1
      FROM grouped_banking_pay_alerts AS grouped_replacement_alerts
      WHERE grouped_replacement_alerts.pay_batch_id = transfer_alerts.pay_batch_id
        AND (
          (
            transfer_alerts.alert_kind IN ('BANK_REJECTED_PAYMENT')
            AND grouped_replacement_alerts.alert_kind IN (
              'TERMINAL_NO_MONEY_REWIND_AVAILABLE',
              'AUTO_UNWIND_PROGRESS'
            )
          )
          OR (
            transfer_alerts.alert_kind IN ('BANK_RETURNED_PAYMENT')
            AND grouped_replacement_alerts.alert_kind IN (
              'TERMINAL_NO_MONEY_REWIND_AVAILABLE',
              'AUTO_UNWIND_PROGRESS',
              'PAID_SETTLED_RECOVERY_REQUIRED'
            )
          )
          OR (
            transfer_alerts.alert_kind IN ('RAIL_SUBMISSION_UNKNOWN_OR_TIMEOUT')
            AND grouped_replacement_alerts.alert_kind = 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER'
          )
        )
    )
    UNION ALL
    SELECT correction_request_alerts.*
    FROM correction_request_alerts
    WHERE NOT EXISTS (
      SELECT 1
      FROM grouped_banking_pay_alerts AS grouped_replacement_alerts
      WHERE grouped_replacement_alerts.pay_batch_id = correction_request_alerts.pay_batch_id
        AND grouped_replacement_alerts.payload_source_kind = 'pay_payment_correction_request'
        AND grouped_replacement_alerts.payload_source_id = correction_request_alerts.payload_source_id
        AND grouped_replacement_alerts.alert_kind IN (
          'AUTO_UNWIND_PROGRESS',
          'WHOLE_BATCH_CANCELLATION_PROGRESS'
        )
    )
    UNION ALL
    SELECT correction_work_alerts.*
    FROM correction_work_alerts
    WHERE NOT EXISTS (
      SELECT 1
      FROM grouped_banking_pay_alerts AS grouped_replacement_alerts
      JOIN public.pay_payment_correction_work_items AS correction_work_group_lookup
        ON correction_work_group_lookup.id = correction_work_alerts.payload_source_id
       AND correction_work_group_lookup.correction_request_id = grouped_replacement_alerts.payload_source_id
      WHERE grouped_replacement_alerts.pay_batch_id = correction_work_alerts.pay_batch_id
        AND grouped_replacement_alerts.payload_source_kind = 'pay_payment_correction_request'
        AND grouped_replacement_alerts.alert_kind IN (
          'AUTO_UNWIND_PROGRESS',
          'WHOLE_BATCH_CANCELLATION_PROGRESS'
        )
    )
    UNION ALL
    SELECT * FROM remittance_alerts
    UNION ALL
    SELECT provider_submit_review_alerts.*
    FROM provider_submit_review_alerts
    WHERE NOT EXISTS (
      SELECT 1
      FROM grouped_banking_pay_alerts AS grouped_replacement_alerts
      WHERE grouped_replacement_alerts.pay_batch_id = provider_submit_review_alerts.pay_batch_id
        AND grouped_replacement_alerts.alert_kind IN (
          'PROVIDER_OUTAGE_RETRY_LATER',
          'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER',
          'CANCELLATION_RACED_WITH_PROVIDER_SUBMIT'
        )
    )
    UNION ALL
    SELECT * FROM grouped_banking_pay_alerts
    UNION ALL
    SELECT * FROM success_event_alerts
  ),
  normalised_current_alerts AS MATERIALIZED (
    SELECT
      mapped_alerts.mapped_alert_kind AS alert_kind,
      raw_current_alerts.severity,
      raw_current_alerts.severity_rank,
      raw_current_alerts.entity_kind,
      raw_current_alerts.entity_id,
      raw_current_alerts.pay_batch_id,
      raw_current_alerts.payload_source_kind,
      raw_current_alerts.payload_source_id,
      COALESCE(raw_current_alerts.fingerprint_payload_json, '{}'::jsonb) || jsonb_build_object(
        'legacy_alert_kind', raw_current_alerts.alert_kind,
        'alert_kind', mapped_alerts.mapped_alert_kind
      ) AS fingerprint_payload_json,
      CASE mapped_alerts.mapped_alert_kind
        WHEN 'PROVIDER_OUTAGE_RETRY_LATER' THEN 'Bank unavailable — unsent payments can be retried'
        WHEN 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER' THEN 'Provider outcome unknown — check provider'
        WHEN 'TERMINAL_NO_MONEY_REWIND_AVAILABLE' THEN 'Failed payments — Rewind financials available'
        WHEN 'AUTO_UNWIND_PROGRESS' THEN 'Rewinding failed payments'
        WHEN 'WHOLE_BATCH_CANCELLATION_PROGRESS' THEN 'Cancelling scheduled batch'
        WHEN 'MANUAL_ADJUSTMENTS_CARRIED_FORWARD' THEN 'Manual adjustments carried forward'
        WHEN 'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS' THEN 'Manual adjustment blockers — review required'
        WHEN 'PAID_SETTLED_RECOVERY_REQUIRED' THEN 'Paid — recovery required'
        WHEN 'CANCELLATION_RACED_WITH_PROVIDER_SUBMIT' THEN 'Cancellation conflict — provider submission already started'
        WHEN 'WEBHOOK_UNMATCHED_REVIEW_REQUIRED' THEN 'Unmatched bank webhook — review required'
        ELSE raw_current_alerts.label
      END AS label,
      CASE mapped_alerts.mapped_alert_kind
        WHEN 'PROVIDER_OUTAGE_RETRY_LATER' THEN 'Bank unavailable — unsent payments can be retried'
        WHEN 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER' THEN 'Provider outcome unknown — check provider'
        WHEN 'TERMINAL_NO_MONEY_REWIND_AVAILABLE' THEN 'Failed payments — Rewind financials available'
        WHEN 'AUTO_UNWIND_PROGRESS' THEN 'Rewinding failed payments'
        WHEN 'WHOLE_BATCH_CANCELLATION_PROGRESS' THEN 'Cancelling scheduled batch'
        WHEN 'MANUAL_ADJUSTMENTS_CARRIED_FORWARD' THEN 'Manual adjustments carried forward'
        WHEN 'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS' THEN 'Manual adjustment blockers — review required'
        WHEN 'PAID_SETTLED_RECOVERY_REQUIRED' THEN 'Paid — recovery required'
        WHEN 'CANCELLATION_RACED_WITH_PROVIDER_SUBMIT' THEN 'Cancellation conflict — provider submission already started'
        WHEN 'WEBHOOK_UNMATCHED_REVIEW_REQUIRED' THEN 'Unmatched bank webhook — review required'
        ELSE raw_current_alerts.title
      END AS title,
      CASE mapped_alerts.mapped_alert_kind
        WHEN 'PROVIDER_OUTAGE_RETRY_LATER' THEN 'The bank/provider was unavailable before the payment request was sent. Retry unsent payments from Banking Pay Overview.'
        WHEN 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER' THEN 'A provider request may have been sent, but the outcome is not confirmed. Open Current Payment Status and check the provider outcome.'
        WHEN 'TERMINAL_NO_MONEY_REWIND_AVAILABLE' THEN 'The provider/bank outcome indicates no money moved. Open Current Payment Status and rewind financials where safe.'
        WHEN 'AUTO_UNWIND_PROGRESS' THEN 'Automatic no-money unwind is running and this alert updates grouped progress.'
        WHEN 'WHOLE_BATCH_CANCELLATION_PROGRESS' THEN 'Scheduled local cancellation is running in chunks and this alert updates grouped progress.'
        WHEN 'MANUAL_ADJUSTMENTS_CARRIED_FORWARD' THEN 'Safe source-less manual adjustments were carried forward and will be included in the next pay run.'
        WHEN 'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS' THEN 'One or more manual adjustments cannot be safely carried forward automatically.'
        WHEN 'PAID_SETTLED_RECOVERY_REQUIRED' THEN 'Money appears to have moved. Amend the timesheet and recover the overpayment in the next pay run rather than unwinding the payment.'
        WHEN 'CANCELLATION_RACED_WITH_PROVIDER_SUBMIT' THEN 'Cancellation could not proceed because provider submission had already started.'
        WHEN 'WEBHOOK_UNMATCHED_REVIEW_REQUIRED' THEN 'A verified provider webhook was received but could not be matched safely to a payment.'
        ELSE raw_current_alerts.description
      END AS description,
      CASE mapped_alerts.mapped_alert_kind
        WHEN 'PROVIDER_OUTAGE_RETRY_LATER' THEN 'Open Banking Pay Overview and retry unsent payments.'
        WHEN 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER' THEN 'Open Current Payment Status and check the provider outcome.'
        WHEN 'TERMINAL_NO_MONEY_REWIND_AVAILABLE' THEN 'Open Current Payment Status and rewind financials where no money moved.'
        WHEN 'AUTO_UNWIND_PROGRESS' THEN 'Monitor rewind progress.'
        WHEN 'WHOLE_BATCH_CANCELLATION_PROGRESS' THEN 'Monitor cancellation progress in Banking Pay Overview.'
        WHEN 'MANUAL_ADJUSTMENTS_CARRIED_FORWARD' THEN 'Review carried-forward manual adjustments in the next pay run.'
        WHEN 'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS' THEN 'Open Current Payment Status and review ambiguous manual adjustment blockers.'
        WHEN 'PAID_SETTLED_RECOVERY_REQUIRED' THEN 'Open Current Payment Status and recover overpayment in next pay run.'
        WHEN 'CANCELLATION_RACED_WITH_PROVIDER_SUBMIT' THEN 'Open Current Payment Status and check provider submission before continuing.'
        WHEN 'WEBHOOK_UNMATCHED_REVIEW_REQUIRED' THEN 'Open Current Payment Status and review the unmatched provider webhook.'
        ELSE replace(COALESCE(raw_current_alerts.action_guidance, 'Open Banking Pay.'), 'Payment Issues', 'Current Payment Status')
      END AS action_guidance,
      raw_current_alerts.sort_at_utc
    FROM raw_current_alerts
    CROSS JOIN LATERAL (
      SELECT CASE raw_current_alerts.alert_kind
        WHEN 'BLOCKED_FUNDS' THEN 'PROVIDER_OUTAGE_RETRY_LATER'
        WHEN 'BANK_REJECTED_PAYMENT' THEN 'TERMINAL_NO_MONEY_REWIND_AVAILABLE'
        WHEN 'BANK_RETURNED_PAYMENT' THEN 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER'
        WHEN 'RAIL_SUBMISSION_UNKNOWN_OR_TIMEOUT' THEN 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER'
        WHEN 'AMBIGUOUS_PAYMENT_REVIEW_REQUIRED' THEN 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER'
        WHEN 'PAYMENT_PROVIDER_SUBMIT_REVIEW' THEN 'PROVIDER_OUTCOME_UNKNOWN_CHECK_PROVIDER'
        WHEN 'PAYMENT_CORRECTION_FAILED' THEN 'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS'
        WHEN 'PAYMENT_CORRECTION_BLOCKED' THEN 'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS'
        WHEN 'PAYMENT_CORRECTION_AWAITING_APPROVAL' THEN 'MANUAL_ADJUSTMENT_AMBIGUOUS_BLOCKERS'
        ELSE raw_current_alerts.alert_kind
      END AS mapped_alert_kind
    ) AS mapped_alerts
  ),
  current_alert_identities AS MATERIALIZED (
    SELECT
      normalised_current_alerts.alert_kind,
      normalised_current_alerts.severity,
      normalised_current_alerts.severity_rank,
      normalised_current_alerts.entity_kind,
      normalised_current_alerts.entity_id,
      normalised_current_alerts.pay_batch_id,
      normalised_current_alerts.payload_source_kind,
      normalised_current_alerts.payload_source_id,
      normalised_current_alerts.fingerprint_payload_json,
      public.banking_alert_fingerprint(
        normalised_current_alerts.alert_kind,
        normalised_current_alerts.entity_kind,
        normalised_current_alerts.entity_id,
        normalised_current_alerts.fingerprint_payload_json
      ) AS alert_fingerprint,
      normalised_current_alerts.label,
      normalised_current_alerts.title,
      normalised_current_alerts.description,
      normalised_current_alerts.action_guidance,
      normalised_current_alerts.sort_at_utc
    FROM normalised_current_alerts
    WHERE normalised_current_alerts.alert_kind IS NOT NULL
      AND normalised_current_alerts.entity_id IS NOT NULL
  ),
  filtered_current_alert_identities AS MATERIALIZED (
    SELECT current_alert_identities.*
    FROM current_alert_identities
    WHERE (v_entity_kind IS NULL OR current_alert_identities.entity_kind = v_entity_kind)
      AND (p_entity_id IS NULL OR current_alert_identities.entity_id = p_entity_id)
      AND public._banking_alert_user_filter_allows(
        p_actor_user_id,
        jsonb_strip_nulls(
          COALESCE(current_alert_identities.fingerprint_payload_json, '{}'::jsonb)
          || jsonb_build_object(
            'alert_kind', current_alert_identities.alert_kind,
            'alert_severity', current_alert_identities.severity,
            'severity', current_alert_identities.severity,
            'entity_kind', current_alert_identities.entity_kind,
            'entity_id', current_alert_identities.entity_id::text,
            'pay_batch_id', current_alert_identities.pay_batch_id::text,
            'alert_fingerprint', current_alert_identities.alert_fingerprint,
            'user_label', current_alert_identities.label,
            'user_description', current_alert_identities.description,
            'required_user_action', current_alert_identities.action_guidance
          )
        )
      )
  ),
  deduped_alerts AS MATERIALIZED (
    SELECT DISTINCT ON (filtered_current_alert_identities.alert_fingerprint)
      filtered_current_alert_identities.alert_kind,
      filtered_current_alert_identities.severity,
      filtered_current_alert_identities.severity_rank,
      filtered_current_alert_identities.entity_kind,
      filtered_current_alert_identities.entity_id,
      filtered_current_alert_identities.pay_batch_id,
      filtered_current_alert_identities.payload_source_kind,
      filtered_current_alert_identities.payload_source_id,
      filtered_current_alert_identities.fingerprint_payload_json,
      filtered_current_alert_identities.alert_fingerprint,
      filtered_current_alert_identities.label,
      filtered_current_alert_identities.title,
      filtered_current_alert_identities.description,
      filtered_current_alert_identities.action_guidance,
      filtered_current_alert_identities.sort_at_utc
    FROM filtered_current_alert_identities
    WHERE filtered_current_alert_identities.alert_fingerprint IS NOT NULL
    ORDER BY filtered_current_alert_identities.alert_fingerprint ASC,
             filtered_current_alert_identities.severity_rank DESC,
             filtered_current_alert_identities.sort_at_utc DESC NULLS LAST
  ),
  alert_rows AS MATERIALIZED (
    SELECT
      deduped_alerts.alert_kind,
      deduped_alerts.severity,
      deduped_alerts.severity_rank,
      deduped_alerts.entity_kind,
      deduped_alerts.entity_id,
      deduped_alerts.pay_batch_id,
      deduped_alerts.payload_source_kind,
      deduped_alerts.payload_source_id,
      deduped_alerts.fingerprint_payload_json,
      deduped_alerts.alert_fingerprint,
      deduped_alerts.label,
      deduped_alerts.title,
      deduped_alerts.description,
      deduped_alerts.action_guidance,
      deduped_alerts.sort_at_utc,
      alert_acknowledgements.id IS NOT NULL AS acknowledged_for_current_user,
      alert_acknowledgements.acknowledged_at_utc
    FROM deduped_alerts
    LEFT JOIN public.banking_alert_acknowledgements AS alert_acknowledgements
      ON alert_acknowledgements.acknowledged_by_user_id = p_actor_user_id
     AND UPPER(BTRIM(COALESCE(alert_acknowledgements.acknowledge_scope, 'USER'))) = 'USER'
     AND (
       alert_acknowledgements.alert_fingerprint = deduped_alerts.alert_fingerprint
       OR (
         UPPER(BTRIM(COALESCE(alert_acknowledgements.alert_kind, ''))) = UPPER(BTRIM(COALESCE(deduped_alerts.alert_kind, '')))
         AND LOWER(BTRIM(COALESCE(alert_acknowledgements.entity_kind, ''))) = LOWER(BTRIM(COALESCE(deduped_alerts.entity_kind, '')))
         AND alert_acknowledgements.entity_id = deduped_alerts.entity_id
         AND alert_acknowledgements.acknowledged_at_utc >= COALESCE(deduped_alerts.sort_at_utc, '-infinity'::timestamptz)
       )
     )
    WHERE COALESCE(p_include_acknowledged, false) = true
       OR alert_acknowledgements.id IS NULL
  ),
  counted_alerts AS MATERIALIZED (
    SELECT
      alert_rows.*,
      (COUNT(*) OVER ())::integer AS filtered_count,
      (COUNT(*) FILTER (WHERE alert_rows.acknowledged_for_current_user = false) OVER ())::integer AS unacknowledged_count
    FROM alert_rows
  ),
  limited_alerts AS MATERIALIZED (
    SELECT counted_alerts.*
    FROM counted_alerts
    ORDER BY counted_alerts.sort_at_utc DESC NULLS LAST,
             counted_alerts.severity_rank DESC,
             counted_alerts.alert_fingerprint ASC
    LIMIT v_limit
  ),
  detailed_alerts AS MATERIALIZED (
    SELECT
      limited_alerts.alert_kind,
      limited_alerts.severity,
      limited_alerts.severity_rank,
      limited_alerts.entity_kind,
      limited_alerts.entity_id,
      limited_alerts.pay_batch_id,
      limited_alerts.fingerprint_payload_json,
      limited_alerts.alert_fingerprint,
      limited_alerts.label,
      limited_alerts.title,
      limited_alerts.description,
      limited_alerts.action_guidance,
      limited_alerts.sort_at_utc,
      limited_alerts.acknowledged_for_current_user,
      limited_alerts.acknowledged_at_utc,
      limited_alerts.filtered_count,
      limited_alerts.unacknowledged_count,
      jsonb_strip_nulls(
        COALESCE(limited_alerts.fingerprint_payload_json, '{}'::jsonb)
        || jsonb_build_object(
          'alert_kind', limited_alerts.alert_kind,
          'issue_kind', limited_alerts.alert_kind,
          'pay_batch_id', limited_alerts.pay_batch_id::text,
          'entity_kind', limited_alerts.entity_kind,
          'entity_id', limited_alerts.entity_id::text,
          'alert_fingerprint', limited_alerts.alert_fingerprint,
          'user_label', limited_alerts.label,
          'user_description', limited_alerts.description,
          'required_user_action', limited_alerts.action_guidance,
          'link_target', 'banking_pay_batch',
          'link_tab', CASE
            WHEN limited_alerts.alert_kind = 'PROVIDER_OUTAGE_RETRY_LATER' THEN 'overview'
            WHEN limited_alerts.alert_kind = 'WHOLE_BATCH_CANCELLATION_PROGRESS' THEN 'overview'
            ELSE 'current_payment_status'
          END
        )
      ) AS payload_json
    FROM limited_alerts
  ),
  signal_aggregate AS MATERIALIZED (
    SELECT
      (COUNT(*) FILTER (WHERE alert_rows.acknowledged_for_current_user = false))::integer AS unacknowledged_count,
      (ARRAY_AGG(alert_rows.severity ORDER BY alert_rows.severity_rank DESC, alert_rows.sort_at_utc DESC NULLS LAST, alert_rows.alert_fingerprint ASC) FILTER (WHERE alert_rows.acknowledged_for_current_user = false))[1] AS highest_severity,
      (ARRAY_AGG(alert_rows.label ORDER BY alert_rows.severity_rank DESC, alert_rows.sort_at_utc DESC NULLS LAST, alert_rows.alert_fingerprint ASC) FILTER (WHERE alert_rows.acknowledged_for_current_user = false))[1] AS highest_label,
      'banking_alert_signal:v3:' || MD5(COALESCE(STRING_AGG(
        CONCAT_WS('|',
          alert_rows.alert_fingerprint,
          alert_rows.alert_kind,
          alert_rows.entity_kind,
          alert_rows.entity_id::text,
          alert_rows.severity
        ),
        CHR(10)
        ORDER BY alert_rows.alert_fingerprint ASC
      ) FILTER (WHERE alert_rows.acknowledged_for_current_user = false), '')) AS alert_hash,
      'banking_alert_summary:v3:' || MD5(COALESCE(STRING_AGG(
        alert_rows.alert_fingerprint,
        CHR(10)
        ORDER BY alert_rows.alert_fingerprint ASC
      ) FILTER (WHERE alert_rows.acknowledged_for_current_user = false), '')) AS summary_signature
    FROM alert_rows
  ),
  aggregate_result AS MATERIALIZED (
    SELECT
      COALESCE(JSONB_AGG(
        jsonb_build_object(
          'alert_kind', detailed_alerts.alert_kind,
          'severity', detailed_alerts.severity,
          'severity_rank', detailed_alerts.severity_rank,
          'entity_kind', detailed_alerts.entity_kind,
          'entity_id', detailed_alerts.entity_id::text,
          'pay_batch_id', detailed_alerts.pay_batch_id::text,
          'alert_fingerprint', detailed_alerts.alert_fingerprint,
          'label', detailed_alerts.label,
          'title', detailed_alerts.title,
          'description', detailed_alerts.description,
          'action_guidance', detailed_alerts.action_guidance,
          'acknowledged_for_current_user', detailed_alerts.acknowledged_for_current_user,
          'requires_attention_for_current_user', NOT detailed_alerts.acknowledged_for_current_user,
          'acknowledged_at_utc', CASE WHEN detailed_alerts.acknowledged_at_utc IS NULL THEN NULL::text ELSE detailed_alerts.acknowledged_at_utc::text END,
          'sort_at_utc', CASE WHEN detailed_alerts.sort_at_utc IS NULL THEN NULL::text ELSE detailed_alerts.sort_at_utc::text END,
          'payload_json', detailed_alerts.payload_json
        )
        ORDER BY detailed_alerts.sort_at_utc DESC NULLS LAST,
                 detailed_alerts.severity_rank DESC,
                 detailed_alerts.alert_fingerprint ASC
      ), '[]'::jsonb) AS alerts_json,
      COALESCE(MAX(detailed_alerts.filtered_count), 0)::integer AS filtered_count,
      (ARRAY_AGG(detailed_alerts.severity ORDER BY detailed_alerts.severity_rank DESC, detailed_alerts.sort_at_utc DESC NULLS LAST, detailed_alerts.alert_fingerprint ASC))[1] AS highest_severity,
      (ARRAY_AGG(detailed_alerts.label ORDER BY detailed_alerts.severity_rank DESC, detailed_alerts.sort_at_utc DESC NULLS LAST, detailed_alerts.alert_fingerprint ASC))[1] AS highest_label
    FROM detailed_alerts
  )
  SELECT jsonb_build_object(
    'ok', true,
    'generated_at_utc', now()::text,
    'actor_user_id', p_actor_user_id::text,
    'alert_context', v_alert_context,
    'include_acknowledged', COALESCE(p_include_acknowledged, false),
    'limit', COALESCE(v_limit, 0),
    'alerts', aggregate_result.alerts_json,
    'banking_alert_hash', COALESCE(signal_aggregate.alert_hash, 'banking_alert_signal:v3:' || MD5('')),
    'banking_alert_summary_signature', COALESCE(signal_aggregate.summary_signature, 'banking_alert_summary:v3:' || MD5('')),
    'banking_unacknowledged_alert_count', COALESCE(signal_aggregate.unacknowledged_count, 0),
    'banking_highest_alert_severity', CASE WHEN COALESCE(signal_aggregate.unacknowledged_count, 0) > 0 THEN COALESCE(signal_aggregate.highest_severity, '') ELSE '' END,
    'banking_highest_alert_label', CASE WHEN COALESCE(signal_aggregate.unacknowledged_count, 0) > 0 THEN COALESCE(signal_aggregate.highest_label, '') ELSE '' END,
    'unacknowledged_count', COALESCE(signal_aggregate.unacknowledged_count, 0),
    'filtered_count', COALESCE(aggregate_result.filtered_count, 0),
    'highest_severity', CASE WHEN COALESCE(aggregate_result.filtered_count, 0) > 0 THEN aggregate_result.highest_severity ELSE NULL::text END,
    'highest_label', CASE WHEN COALESCE(aggregate_result.filtered_count, 0) > 0 THEN aggregate_result.highest_label ELSE NULL::text END
  )
  INTO v_result
  FROM aggregate_result
  CROSS JOIN signal_aggregate;

  RETURN COALESCE(v_result, jsonb_build_object(
    'ok', true,
    'generated_at_utc', now()::text,
    'actor_user_id', p_actor_user_id::text,
    'alert_context', v_alert_context,
    'include_acknowledged', COALESCE(p_include_acknowledged, false),
    'limit', COALESCE(v_limit, 0),
    'alerts', '[]'::jsonb,
    'banking_alert_hash', 'banking_alert_signal:v3:' || MD5(''),
    'banking_alert_summary_signature', 'banking_alert_summary:v3:' || MD5(''),
    'banking_unacknowledged_alert_count', 0,
    'banking_highest_alert_severity', '',
    'banking_highest_alert_label', '',
    'unacknowledged_count', 0,
    'filtered_count', 0,
    'highest_severity', NULL::text,
    'highest_label', NULL::text
  ));
END;
$function$;

-- banking_alerts_refresh_for_user(uuid,text,integer)
CREATE OR REPLACE FUNCTION public.banking_alerts_refresh_for_user(p_actor_user_id uuid, p_alert_context text DEFAULT 'ALERT_PANEL'::text, p_limit integer DEFAULT 100)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_alert_context text := UPPER(REPLACE(NULLIF(BTRIM(COALESCE(p_alert_context, 'ALERT_PANEL')), ''), '-', '_'));
  v_active_json jsonb := '{}'::jsonb;
  v_cached_json jsonb := NULL::jsonb;
  v_cached_updated_at_utc timestamptz := NULL::timestamptz;
  v_alert_hash text := 'banking_alert_signal:v3:' || MD5('');
  v_summary_hash text := 'banking_alert_summary:v3:' || MD5('');
  v_unacknowledged_count integer := 0;
  v_highest_severity text := NULL::text;
  v_highest_label text := NULL::text;
BEGIN
  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'BANKING_ALERTS_REFRESH_FOR_USER_ACTOR_REQUIRED'
      USING ERRCODE = 'P0001',
             DETAIL = jsonb_build_object('code', 'BANKING_ALERTS_REFRESH_FOR_USER_ACTOR_REQUIRED')::text;
  END IF;

  -- Routine navigation/heartbeat reads must not rebuild the complete alert set
  -- on every request. A ten-minute actor-scoped recovery cache protects
  -- against an indefinitely stale row when no explicit invalidation path fires,
  -- without turning the multi-second live projection into background polling.
  -- Alert-panel opening and explicit alert-management/refresh contexts bypass
  -- this branch so user actions still observe current database truth promptly.
  IF v_alert_context IN ('ALERT_PANEL', 'ALERTS_PANEL', 'CACHED', 'CHANGES_PING', 'RPC_CHANGES_PING') THEN
    SELECT
      alert_summary.summary_json,
      alert_summary.updated_at_utc
    INTO
      v_cached_json,
      v_cached_updated_at_utc
    FROM public.banking_alert_display_summary AS alert_summary
    WHERE alert_summary.actor_user_id = p_actor_user_id;

    IF FOUND
       AND v_cached_updated_at_utc >= now() - INTERVAL '10 minutes'
       AND jsonb_typeof(COALESCE(v_cached_json, '{}'::jsonb)) = 'object' THEN
      RETURN COALESCE(v_cached_json, '{}'::jsonb) || jsonb_build_object(
        'cached', true,
        'cache_checked_at_utc', now()::text,
        'cache_updated_at_utc', v_cached_updated_at_utc::text,
        'alert_context', v_alert_context
      );
    END IF;
  END IF;

  PERFORM public.banking_pay_hot_path_budget_apply(
    CASE WHEN v_alert_context = 'ALERT_REFRESH_JOB' THEN 'ALERT_REFRESH_JOB' ELSE 'ALERT_PANEL' END
  );

  v_active_json := public.banking_alerts_active_for_user(
    p_actor_user_id,
    NULL::text,
    NULL::uuid,
    false,
    LEAST(GREATEST(COALESCE(p_limit, 100), 0), 100),
    v_alert_context
  );

  v_alert_hash := COALESCE(NULLIF(BTRIM(v_active_json ->> 'banking_alert_hash'), ''), v_alert_hash);
  v_summary_hash := COALESCE(NULLIF(BTRIM(v_active_json ->> 'banking_alert_summary_signature'), ''), v_summary_hash);
  BEGIN
    v_unacknowledged_count := COALESCE((v_active_json ->> 'banking_unacknowledged_alert_count')::integer, 0);
  EXCEPTION WHEN OTHERS THEN
    v_unacknowledged_count := 0;
  END;
  v_highest_severity := NULLIF(BTRIM(COALESCE(v_active_json ->> 'banking_highest_alert_severity', v_active_json ->> 'highest_severity', '')), '');
  v_highest_label := NULLIF(BTRIM(COALESCE(v_active_json ->> 'banking_highest_alert_label', v_active_json ->> 'highest_label', '')), '');

  INSERT INTO public.banking_alert_display_summary (
    actor_user_id,
    alert_hash,
    summary_hash,
    unacknowledged_count,
    highest_severity,
    highest_label,
    summary_json,
    updated_at_utc
  )
  VALUES (
    p_actor_user_id,
    v_alert_hash,
    v_summary_hash,
    v_unacknowledged_count,
    v_highest_severity,
    v_highest_label,
    v_active_json,
    now()
  )
  ON CONFLICT (actor_user_id) DO UPDATE
  SET
    alert_hash = EXCLUDED.alert_hash,
    summary_hash = EXCLUDED.summary_hash,
    unacknowledged_count = EXCLUDED.unacknowledged_count,
    highest_severity = EXCLUDED.highest_severity,
    highest_label = EXCLUDED.highest_label,
    summary_json = EXCLUDED.summary_json,
    updated_at_utc = now();

  RETURN v_active_json || jsonb_build_object(
    'cached', false,
    'cache_checked_at_utc', now()::text,
    'alert_context', v_alert_context
  );
END;
$function$;

-- banking_get_capabilities()
CREATE OR REPLACE FUNCTION public.banking_get_capabilities()
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_provider text;
  v_env text;

  v_supports_scheduling boolean;
  v_supports_name_check boolean;
  v_supports_auto_execute boolean;

  v_supports_csv_confirm boolean;

  -- ✅ C: include default funding account in capabilities output
  v_rail_default_funding_account_ref text;

  -- ✅ NEW: payroll testing flag (simulate payments; no real bank payments)
  v_payroll_testing boolean;

  -- ✅ NEW: PAYE remittances gate
  v_paye_remittances_enabled boolean;
begin
  -- settings_defaults is expected to have a single row; do not assume an id column.
  select
    sd.rail_provider_default,
    sd.rail_env_default,
    sd.rail_supports_scheduling,
    sd.rail_supports_name_check,
    sd.rail_supports_auto_execute,
    sd.rail_default_funding_account_ref,
    sd.payroll_testing,
    sd.paye_remittances_enabled
  into
    v_provider,
    v_env,
    v_supports_scheduling,
    v_supports_name_check,
    v_supports_auto_execute,
    v_rail_default_funding_account_ref,
    v_payroll_testing,
    v_paye_remittances_enabled
  from public.settings_defaults sd
  limit 1;

  v_provider := upper(btrim(coalesce(v_provider, 'CSV')));
  v_env := upper(btrim(coalesce(v_env, 'PROD')));

  v_supports_scheduling := coalesce(v_supports_scheduling, false);
  v_supports_name_check := coalesce(v_supports_name_check, false);
  v_supports_auto_execute := coalesce(v_supports_auto_execute, false);

  v_payroll_testing := coalesce(v_payroll_testing, false);
  v_paye_remittances_enabled := coalesce(v_paye_remittances_enabled, false);

  -- CSV rail implies manual bank confirmation (upload + confirm).
  v_supports_csv_confirm := (v_provider = 'CSV');

  return jsonb_build_object(
    'rail_provider', v_provider,
    'rail_env', v_env,
    'supports_scheduling', v_supports_scheduling,
    'supports_name_check', v_supports_name_check,
    'supports_auto_execute', v_supports_auto_execute,
    'supports_csv_confirm', v_supports_csv_confirm,
    'requires_manual_bank_confirm', v_supports_csv_confirm,

    -- ✅ NEW: surface test-mode switch for UI/backend consistency
    'payroll_testing', v_payroll_testing,

    -- ✅ NEW: PAYE remittance gate (UI can block PAYE remittance send)
    'paye_remittances_enabled', v_paye_remittances_enabled,

    -- ✅ C: surface saved default so UI can preselect consistently
    'rail_default_funding_account_ref', v_rail_default_funding_account_ref
  );
end;
$function$;

-- banking_pay_batch_signal_touch(uuid,text,text,jsonb,boolean,boolean,boolean,boolean)
CREATE OR REPLACE FUNCTION public.banking_pay_batch_signal_touch(p_pay_batch_id uuid, p_change_reason text, p_change_source text DEFAULT NULL::text, p_change_scope_json jsonb DEFAULT '{}'::jsonb, p_touch_payment_status boolean DEFAULT true, p_touch_correction_progress boolean DEFAULT false, p_touch_alerts boolean DEFAULT false, p_touch_overview boolean DEFAULT true)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_change_reason text := NULLIF(BTRIM(COALESCE(p_change_reason, '')), '');
  v_change_source text := NULLIF(BTRIM(COALESCE(p_change_source, '')), '');
  v_change_scope_json jsonb := '{}'::jsonb;
  v_touch_payment_status boolean := COALESCE(p_touch_payment_status, false);
  v_touch_correction_progress boolean := COALESCE(p_touch_correction_progress, false);
  v_touch_alerts boolean := COALESCE(p_touch_alerts, false);
  v_touch_overview boolean := COALESCE(p_touch_overview, false);
  v_last_changed_transfer_ids jsonb := '[]'::jsonb;
  v_last_changed_candidate_ids jsonb := '[]'::jsonb;
  v_last_changed_pay_batch_item_ids jsonb := '[]'::jsonb;
  v_changed_transfer_id_count integer := 0;
  v_changed_candidate_id_count integer := 0;
  v_changed_pay_batch_item_id_count integer := 0;
  v_changed_transfer_ids_hash text := 'changed_transfer_ids:v1:' || MD5('[]');
  v_changed_candidate_ids_hash text := 'changed_candidate_ids:v1:' || MD5('[]');
  v_changed_pay_batch_item_ids_hash text := 'changed_pay_batch_item_ids:v1:' || MD5('[]');
  v_signal_row public.banking_pay_batch_change_signals%ROWTYPE;
BEGIN
  PERFORM public.banking_pay_hot_path_budget_apply('WATCH_SIGNAL');

  IF p_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'BANKING_PAY_BATCH_SIGNAL_TOUCH_BATCH_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'BANKING_PAY_BATCH_SIGNAL_TOUCH_BATCH_REQUIRED')::text;
  END IF;

  IF v_change_reason IS NULL THEN
    RAISE EXCEPTION 'BANKING_PAY_BATCH_SIGNAL_TOUCH_REASON_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'BANKING_PAY_BATCH_SIGNAL_TOUCH_REASON_REQUIRED')::text;
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM public.pay_batches AS pay_batch_check
    WHERE pay_batch_check.id = p_pay_batch_id
  ) THEN
    RAISE EXCEPTION 'BANKING_PAY_BATCH_SIGNAL_TOUCH_BATCH_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'BANKING_PAY_BATCH_SIGNAL_TOUCH_BATCH_NOT_FOUND',
              'pay_batch_id', p_pay_batch_id::text
            )::text;
  END IF;

  IF p_change_scope_json IS NOT NULL AND COALESCE(jsonb_typeof(p_change_scope_json), 'null') = 'object' THEN
    v_change_scope_json := p_change_scope_json;
  END IF;

  v_last_changed_transfer_ids := COALESCE(
    CASE WHEN jsonb_typeof(v_change_scope_json -> 'last_changed_transfer_ids') = 'array' THEN v_change_scope_json -> 'last_changed_transfer_ids' END,
    CASE WHEN jsonb_typeof(v_change_scope_json -> 'changed_transfer_ids') = 'array' THEN v_change_scope_json -> 'changed_transfer_ids' END,
    CASE WHEN jsonb_typeof(v_change_scope_json -> 'pay_bank_transfer_ids') = 'array' THEN v_change_scope_json -> 'pay_bank_transfer_ids' END,
    CASE WHEN jsonb_typeof(v_change_scope_json -> 'transfer_ids') = 'array' THEN v_change_scope_json -> 'transfer_ids' END,
    '[]'::jsonb
  );

  v_last_changed_candidate_ids := COALESCE(
    CASE WHEN jsonb_typeof(v_change_scope_json -> 'last_changed_candidate_ids') = 'array' THEN v_change_scope_json -> 'last_changed_candidate_ids' END,
    CASE WHEN jsonb_typeof(v_change_scope_json -> 'changed_candidate_ids') = 'array' THEN v_change_scope_json -> 'changed_candidate_ids' END,
    CASE WHEN jsonb_typeof(v_change_scope_json -> 'candidate_ids') = 'array' THEN v_change_scope_json -> 'candidate_ids' END,
    '[]'::jsonb
  );

  v_last_changed_pay_batch_item_ids := COALESCE(
    CASE WHEN jsonb_typeof(v_change_scope_json -> 'last_changed_pay_batch_item_ids') = 'array' THEN v_change_scope_json -> 'last_changed_pay_batch_item_ids' END,
    CASE WHEN jsonb_typeof(v_change_scope_json -> 'changed_pay_batch_item_ids') = 'array' THEN v_change_scope_json -> 'changed_pay_batch_item_ids' END,
    CASE WHEN jsonb_typeof(v_change_scope_json -> 'pay_batch_item_ids') = 'array' THEN v_change_scope_json -> 'pay_batch_item_ids' END,
    CASE WHEN jsonb_typeof(v_change_scope_json -> 'item_ids') = 'array' THEN v_change_scope_json -> 'item_ids' END,
    '[]'::jsonb
  );

  v_changed_transfer_id_count := COALESCE(
    CASE
      WHEN COALESCE(v_change_scope_json->>'last_changed_transfer_id_count', v_change_scope_json->>'changed_transfer_id_count', v_change_scope_json->>'transfer_id_count', '') ~ '^[0-9]+$'
        THEN COALESCE(v_change_scope_json->>'last_changed_transfer_id_count', v_change_scope_json->>'changed_transfer_id_count', v_change_scope_json->>'transfer_id_count')::integer
      ELSE NULL::integer
    END,
    CASE WHEN jsonb_typeof(v_last_changed_transfer_ids) = 'array' THEN jsonb_array_length(v_last_changed_transfer_ids) ELSE 0 END
  );

  v_changed_candidate_id_count := COALESCE(
    CASE
      WHEN COALESCE(v_change_scope_json->>'last_changed_candidate_id_count', v_change_scope_json->>'changed_candidate_id_count', v_change_scope_json->>'candidate_id_count', '') ~ '^[0-9]+$'
        THEN COALESCE(v_change_scope_json->>'last_changed_candidate_id_count', v_change_scope_json->>'changed_candidate_id_count', v_change_scope_json->>'candidate_id_count')::integer
      ELSE NULL::integer
    END,
    CASE WHEN jsonb_typeof(v_last_changed_candidate_ids) = 'array' THEN jsonb_array_length(v_last_changed_candidate_ids) ELSE 0 END
  );

  v_changed_pay_batch_item_id_count := COALESCE(
    CASE
      WHEN COALESCE(v_change_scope_json->>'last_changed_pay_batch_item_id_count', v_change_scope_json->>'changed_pay_batch_item_id_count', v_change_scope_json->>'pay_batch_item_id_count', v_change_scope_json->>'item_id_count', '') ~ '^[0-9]+$'
        THEN COALESCE(v_change_scope_json->>'last_changed_pay_batch_item_id_count', v_change_scope_json->>'changed_pay_batch_item_id_count', v_change_scope_json->>'pay_batch_item_id_count', v_change_scope_json->>'item_id_count')::integer
      ELSE NULL::integer
    END,
    CASE WHEN jsonb_typeof(v_last_changed_pay_batch_item_ids) = 'array' THEN jsonb_array_length(v_last_changed_pay_batch_item_ids) ELSE 0 END
  );

  v_changed_transfer_ids_hash := COALESCE(
    NULLIF(BTRIM(COALESCE(v_change_scope_json->>'last_changed_transfer_ids_hash', v_change_scope_json->>'changed_transfer_ids_hash', v_change_scope_json->>'transfer_ids_hash', '')), ''),
    'changed_transfer_ids:v1:' || COALESCE(v_changed_transfer_id_count, 0)::text || ':' || MD5('[]')
  );
  v_changed_candidate_ids_hash := COALESCE(
    NULLIF(BTRIM(COALESCE(v_change_scope_json->>'last_changed_candidate_ids_hash', v_change_scope_json->>'changed_candidate_ids_hash', v_change_scope_json->>'candidate_ids_hash', '')), ''),
    'changed_candidate_ids:v1:' || COALESCE(v_changed_candidate_id_count, 0)::text || ':' || MD5('[]')
  );
  v_changed_pay_batch_item_ids_hash := COALESCE(
    NULLIF(BTRIM(COALESCE(v_change_scope_json->>'last_changed_pay_batch_item_ids_hash', v_change_scope_json->>'changed_pay_batch_item_ids_hash', v_change_scope_json->>'pay_batch_item_ids_hash', v_change_scope_json->>'item_ids_hash', '')), ''),
    'changed_pay_batch_item_ids:v1:' || COALESCE(v_changed_pay_batch_item_id_count, 0)::text || ':' || MD5('[]')
  );

  SELECT COALESCE(jsonb_agg(to_jsonb(transfer_sample.changed_id) ORDER BY transfer_sample.ordinality), '[]'::jsonb)
  INTO v_last_changed_transfer_ids
  FROM (
    SELECT changed_transfer_id_rows.changed_id, changed_transfer_id_rows.ordinality
    FROM jsonb_array_elements_text(
      CASE WHEN jsonb_typeof(v_last_changed_transfer_ids) = 'array' THEN v_last_changed_transfer_ids ELSE '[]'::jsonb END
    ) WITH ORDINALITY AS changed_transfer_id_rows(changed_id, ordinality)
    LIMIT 10
  ) AS transfer_sample;

  SELECT COALESCE(jsonb_agg(to_jsonb(candidate_sample.changed_id) ORDER BY candidate_sample.ordinality), '[]'::jsonb)
  INTO v_last_changed_candidate_ids
  FROM (
    SELECT changed_candidate_id_rows.changed_id, changed_candidate_id_rows.ordinality
    FROM jsonb_array_elements_text(
      CASE WHEN jsonb_typeof(v_last_changed_candidate_ids) = 'array' THEN v_last_changed_candidate_ids ELSE '[]'::jsonb END
    ) WITH ORDINALITY AS changed_candidate_id_rows(changed_id, ordinality)
    LIMIT 10
  ) AS candidate_sample;

  SELECT COALESCE(jsonb_agg(to_jsonb(item_sample.changed_id) ORDER BY item_sample.ordinality), '[]'::jsonb)
  INTO v_last_changed_pay_batch_item_ids
  FROM (
    SELECT changed_item_id_rows.changed_id, changed_item_id_rows.ordinality
    FROM jsonb_array_elements_text(
      CASE WHEN jsonb_typeof(v_last_changed_pay_batch_item_ids) = 'array' THEN v_last_changed_pay_batch_item_ids ELSE '[]'::jsonb END
    ) WITH ORDINALITY AS changed_item_id_rows(changed_id, ordinality)
    LIMIT 10
  ) AS item_sample;

  IF v_changed_transfer_ids_hash LIKE ('changed_transfer_ids:v1:' || COALESCE(v_changed_transfer_id_count, 0)::text || ':%') THEN
    v_changed_transfer_ids_hash := 'changed_transfer_ids:v1:' || COALESCE(v_changed_transfer_id_count, 0)::text || ':' || MD5(COALESCE(v_last_changed_transfer_ids, '[]'::jsonb)::text);
  END IF;

  IF v_changed_candidate_ids_hash LIKE ('changed_candidate_ids:v1:' || COALESCE(v_changed_candidate_id_count, 0)::text || ':%') THEN
    v_changed_candidate_ids_hash := 'changed_candidate_ids:v1:' || COALESCE(v_changed_candidate_id_count, 0)::text || ':' || MD5(COALESCE(v_last_changed_candidate_ids, '[]'::jsonb)::text);
  END IF;

  IF v_changed_pay_batch_item_ids_hash LIKE ('changed_pay_batch_item_ids:v1:' || COALESCE(v_changed_pay_batch_item_id_count, 0)::text || ':%') THEN
    v_changed_pay_batch_item_ids_hash := 'changed_pay_batch_item_ids:v1:' || COALESCE(v_changed_pay_batch_item_id_count, 0)::text || ':' || MD5(COALESCE(v_last_changed_pay_batch_item_ids, '[]'::jsonb)::text);
  END IF;

  v_change_scope_json := (
    v_change_scope_json
      - 'last_changed_transfer_ids'
      - 'changed_transfer_ids'
      - 'pay_bank_transfer_ids'
      - 'transfer_ids'
      - 'last_changed_candidate_ids'
      - 'changed_candidate_ids'
      - 'candidate_ids'
      - 'last_changed_pay_batch_item_ids'
      - 'changed_pay_batch_item_ids'
      - 'pay_batch_item_ids'
      - 'item_ids'
  ) || jsonb_build_object(
    'last_changed_transfer_ids_sample', COALESCE(v_last_changed_transfer_ids, '[]'::jsonb),
    'last_changed_transfer_id_count', COALESCE(v_changed_transfer_id_count, 0),
    'last_changed_transfer_ids_hash', v_changed_transfer_ids_hash,
    'last_changed_candidate_ids_sample', COALESCE(v_last_changed_candidate_ids, '[]'::jsonb),
    'last_changed_candidate_id_count', COALESCE(v_changed_candidate_id_count, 0),
    'last_changed_candidate_ids_hash', v_changed_candidate_ids_hash,
    'last_changed_pay_batch_item_ids_sample', COALESCE(v_last_changed_pay_batch_item_ids, '[]'::jsonb),
    'last_changed_pay_batch_item_id_count', COALESCE(v_changed_pay_batch_item_id_count, 0),
    'last_changed_pay_batch_item_ids_hash', v_changed_pay_batch_item_ids_hash,
    'changed_id_samples_are_capped', true,
    'changed_id_sample_cap', 10
  );

  IF v_touch_payment_status IS NOT TRUE
     AND v_touch_correction_progress IS NOT TRUE
     AND v_touch_alerts IS NOT TRUE
     AND v_touch_overview IS NOT TRUE THEN
    SELECT existing_signal.*
    INTO v_signal_row
    FROM public.banking_pay_batch_change_signals AS existing_signal
    WHERE existing_signal.pay_batch_id = p_pay_batch_id;

    IF NOT FOUND THEN
      RETURN jsonb_build_object(
        'ok', true,
        'changed', false,
        'pay_batch_id', p_pay_batch_id::text,
        'version', 0,
        'payment_status_version', 0,
        'correction_progress_version', 0,
        'alert_version', 0,
        'overview_version', 0
      );
    END IF;

    RETURN jsonb_build_object(
      'ok', true,
      'changed', false,
      'pay_batch_id', v_signal_row.pay_batch_id::text,
      'version', v_signal_row.version,
      'payment_status_version', v_signal_row.payment_status_version,
      'correction_progress_version', v_signal_row.correction_progress_version,
      'alert_version', v_signal_row.alert_version,
      'overview_version', v_signal_row.overview_version
    );
  END IF;

  INSERT INTO public.banking_pay_batch_change_signals AS signal_target (
    pay_batch_id,
    version,
    payment_status_version,
    correction_progress_version,
    alert_version,
    overview_version,
    last_changed_at_utc,
    last_payment_status_changed_at_utc,
    last_correction_progress_changed_at_utc,
    last_alert_changed_at_utc,
    last_change_reason,
    last_change_source,
    last_change_scope_json,
    last_changed_transfer_ids,
    last_changed_candidate_ids,
    last_changed_pay_batch_item_ids,
    last_status_hash,
    last_alert_hash,
    updated_at_utc
  )
  VALUES (
    p_pay_batch_id,
    1,
    CASE WHEN v_touch_payment_status THEN 1 ELSE 0 END,
    CASE WHEN v_touch_correction_progress THEN 1 ELSE 0 END,
    CASE WHEN v_touch_alerts THEN 1 ELSE 0 END,
    CASE WHEN v_touch_overview THEN 1 ELSE 0 END,
    v_now,
    CASE WHEN v_touch_payment_status THEN v_now ELSE NULL::timestamptz END,
    CASE WHEN v_touch_correction_progress THEN v_now ELSE NULL::timestamptz END,
    CASE WHEN v_touch_alerts THEN v_now ELSE NULL::timestamptz END,
    v_change_reason,
    v_change_source,
    v_change_scope_json,
    v_last_changed_transfer_ids,
    v_last_changed_candidate_ids,
    v_last_changed_pay_batch_item_ids,
    CASE WHEN v_touch_payment_status THEN 'payment_status:' || MD5(p_pay_batch_id::text || ':1:' || v_change_reason || ':' || v_change_scope_json::text) ELSE NULL::text END,
    CASE WHEN v_touch_alerts THEN 'alert:' || MD5(p_pay_batch_id::text || ':1:' || v_change_reason || ':' || v_change_scope_json::text) ELSE NULL::text END,
    v_now
  )
  ON CONFLICT (pay_batch_id) DO UPDATE
  SET
    version = signal_target.version + 1,
    payment_status_version = signal_target.payment_status_version + CASE WHEN v_touch_payment_status THEN 1 ELSE 0 END,
    correction_progress_version = signal_target.correction_progress_version + CASE WHEN v_touch_correction_progress THEN 1 ELSE 0 END,
    alert_version = signal_target.alert_version + CASE WHEN v_touch_alerts THEN 1 ELSE 0 END,
    overview_version = signal_target.overview_version + CASE WHEN v_touch_overview THEN 1 ELSE 0 END,
    last_changed_at_utc = v_now,
    last_payment_status_changed_at_utc = CASE WHEN v_touch_payment_status THEN v_now ELSE signal_target.last_payment_status_changed_at_utc END,
    last_correction_progress_changed_at_utc = CASE WHEN v_touch_correction_progress THEN v_now ELSE signal_target.last_correction_progress_changed_at_utc END,
    last_alert_changed_at_utc = CASE WHEN v_touch_alerts THEN v_now ELSE signal_target.last_alert_changed_at_utc END,
    last_change_reason = v_change_reason,
    last_change_source = v_change_source,
    last_change_scope_json = v_change_scope_json,
    last_changed_transfer_ids = v_last_changed_transfer_ids,
    last_changed_candidate_ids = v_last_changed_candidate_ids,
    last_changed_pay_batch_item_ids = v_last_changed_pay_batch_item_ids,
    last_status_hash = CASE
      WHEN v_touch_payment_status THEN 'payment_status:' || MD5(p_pay_batch_id::text || ':' || (signal_target.payment_status_version + 1)::text || ':' || v_change_reason || ':' || v_change_scope_json::text)
      ELSE signal_target.last_status_hash
    END,
    last_alert_hash = CASE
      WHEN v_touch_alerts THEN 'alert:' || MD5(p_pay_batch_id::text || ':' || (signal_target.alert_version + 1)::text || ':' || v_change_reason || ':' || v_change_scope_json::text)
      ELSE signal_target.last_alert_hash
    END,
    updated_at_utc = v_now
  RETURNING *
  INTO v_signal_row;

  BEGIN
    PERFORM public.pay_batch_display_summary_refresh(p_pay_batch_id);
    SELECT refreshed_signal.*
    INTO v_signal_row
    FROM public.banking_pay_batch_change_signals AS refreshed_signal
    WHERE refreshed_signal.pay_batch_id = p_pay_batch_id;
  EXCEPTION WHEN OTHERS THEN
    PERFORM public.pay_batch_display_summary_touch(p_pay_batch_id);
  END;

  RETURN jsonb_build_object(
    'ok', true,
    'changed', true,
    'pay_batch_id', v_signal_row.pay_batch_id::text,
    'version', v_signal_row.version,
    'payment_status_version', v_signal_row.payment_status_version,
    'correction_progress_version', v_signal_row.correction_progress_version,
    'alert_version', v_signal_row.alert_version,
    'overview_version', v_signal_row.overview_version,
    'last_changed_at_utc', v_signal_row.last_changed_at_utc,
    'last_change_reason', v_signal_row.last_change_reason,
    'last_change_source', v_signal_row.last_change_source,
    'last_change_scope_json', v_signal_row.last_change_scope_json,
    'last_changed_transfer_ids', COALESCE(v_signal_row.last_changed_transfer_ids, '[]'::jsonb),
    'last_changed_transfer_id_count', COALESCE(v_changed_transfer_id_count, 0),
    'last_changed_transfer_ids_hash', v_changed_transfer_ids_hash,
    'last_changed_candidate_ids', COALESCE(v_signal_row.last_changed_candidate_ids, '[]'::jsonb),
    'last_changed_candidate_id_count', COALESCE(v_changed_candidate_id_count, 0),
    'last_changed_candidate_ids_hash', v_changed_candidate_ids_hash,
    'last_changed_pay_batch_item_ids', COALESCE(v_signal_row.last_changed_pay_batch_item_ids, '[]'::jsonb),
    'last_changed_pay_batch_item_id_count', COALESCE(v_changed_pay_batch_item_id_count, 0),
    'last_changed_pay_batch_item_ids_hash', v_changed_pay_batch_item_ids_hash,
    'last_status_hash', v_signal_row.last_status_hash,
    'last_alert_hash', v_signal_row.last_alert_hash
  );
END;
$function$;

-- banking_pay_batch_watch_signal(uuid,uuid,bigint,bigint,bigint,bigint,bigint,text,jsonb)
CREATE OR REPLACE FUNCTION public.banking_pay_batch_watch_signal(p_pay_batch_id uuid, p_actor_user_id uuid, p_known_version bigint DEFAULT NULL::bigint, p_known_payment_status_version bigint DEFAULT NULL::bigint, p_known_correction_progress_version bigint DEFAULT NULL::bigint, p_known_alert_version bigint DEFAULT NULL::bigint, p_known_overview_version bigint DEFAULT NULL::bigint, p_current_tab text DEFAULT NULL::text, p_current_section_json jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_actor_is_valid boolean := false;
  v_current_tab text := LOWER(REPLACE(NULLIF(BTRIM(COALESCE(p_current_tab, '')), ''), '-', '_'));
  v_current_section_json jsonb := '{}'::jsonb;
  v_signal_row public.banking_pay_batch_change_signals%ROWTYPE;
  v_signal_found boolean := false;
  v_version bigint := 0;
  v_payment_status_version bigint := 0;
  v_correction_progress_version bigint := 0;
  v_alert_version bigint := 0;
  v_overview_version bigint := 0;
  v_payment_status_changed boolean := false;
  v_correction_progress_changed boolean := false;
  v_alert_changed boolean := false;
  v_overview_changed boolean := false;
  v_changed boolean := false;
  v_changed_areas text[] := ARRAY[]::text[];
  v_recommended_refresh text := 'NONE';
  v_display_summary_version bigint := 0;
  v_display_summary_updated_at_utc timestamptz := NULL::timestamptz;
  v_display_stale_summary_json jsonb := '{}'::jsonb;
  v_display_summary_refresh_required boolean := false;
  v_latest_operation_id uuid := NULL::uuid;
  v_latest_operation_status text := NULL::text;
  v_latest_operation_phase text := NULL::text;
  v_cached_alert_hash text := 'banking_alert_signal:v3:' || MD5('');
  v_cached_summary_hash text := 'banking_alert_summary:v3:' || MD5('');
  v_cached_unacknowledged_count integer := 0;
  v_cached_highest_severity text := NULL::text;
  v_cached_highest_label text := NULL::text;
  v_changed_transfer_ids_sample jsonb := '[]'::jsonb;
  v_changed_candidate_ids_sample jsonb := '[]'::jsonb;
  v_changed_pay_batch_item_ids_sample jsonb := '[]'::jsonb;
  v_changed_transfer_id_count integer := 0;
  v_changed_candidate_id_count integer := 0;
  v_changed_pay_batch_item_id_count integer := 0;
  v_changed_transfer_ids_hash text := 'changed_transfer_ids:v1:' || MD5('[]');
  v_changed_candidate_ids_hash text := 'changed_candidate_ids:v1:' || MD5('[]');
  v_changed_pay_batch_item_ids_hash text := 'changed_pay_batch_item_ids:v1:' || MD5('[]');
  v_last_change_scope_summary_json jsonb := '{}'::jsonb;
BEGIN
  PERFORM public.banking_pay_hot_path_budget_apply('WATCH_SIGNAL');

  IF p_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'BANKING_PAY_BATCH_WATCH_SIGNAL_BATCH_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'BANKING_PAY_BATCH_WATCH_SIGNAL_BATCH_REQUIRED')::text;
  END IF;

  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'BANKING_PAY_BATCH_WATCH_SIGNAL_ACTOR_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'BANKING_PAY_BATCH_WATCH_SIGNAL_ACTOR_REQUIRED')::text;
  END IF;

  IF v_current_tab IS NULL THEN
    RAISE EXCEPTION 'BANKING_PAY_BATCH_WATCH_SIGNAL_VISIBLE_TAB_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'BANKING_PAY_BATCH_WATCH_SIGNAL_VISIBLE_TAB_REQUIRED',
              'message', 'banking_pay_batch_watch_signal requires the currently visible tab/section context.'
            )::text;
  END IF;

  IF v_current_tab NOT IN (
    'overview',
    'current_payment_status',
    'payment_status',
    'payment_issues',
    'candidates',
    'items',
    'item_breakdowns',
    'transfers',
    'finance_case_groups',
    'remittances',
    'communications',
    'auth_history',
    'events'
  ) THEN
    RAISE EXCEPTION 'BANKING_PAY_BATCH_WATCH_SIGNAL_VISIBLE_TAB_NOT_ALLOWED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'BANKING_PAY_BATCH_WATCH_SIGNAL_VISIBLE_TAB_NOT_ALLOWED',
              'current_tab', v_current_tab
            )::text;
  END IF;

  SELECT EXISTS (
    SELECT 1
    FROM public.tms_users AS actor_user
    WHERE actor_user.id = p_actor_user_id
      AND COALESCE(actor_user.is_active, false) = true
  )
  INTO v_actor_is_valid;

  IF COALESCE(v_actor_is_valid, false) IS NOT TRUE THEN
    RAISE EXCEPTION 'BANKING_PAY_BATCH_WATCH_SIGNAL_ACTOR_NOT_ALLOWED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'BANKING_PAY_BATCH_WATCH_SIGNAL_ACTOR_NOT_ALLOWED',
              'actor_user_id', p_actor_user_id::text
            )::text;
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM public.pay_batches AS pay_batch_check
    WHERE pay_batch_check.id = p_pay_batch_id
  ) THEN
    RAISE EXCEPTION 'BANKING_PAY_BATCH_WATCH_SIGNAL_BATCH_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'BANKING_PAY_BATCH_WATCH_SIGNAL_BATCH_NOT_FOUND',
              'pay_batch_id', p_pay_batch_id::text
            )::text;
  END IF;

  IF p_current_section_json IS NOT NULL AND COALESCE(jsonb_typeof(p_current_section_json), 'null') = 'object' THEN
    v_current_section_json := p_current_section_json;
  END IF;

  SELECT batch_signal.*
  INTO v_signal_row
  FROM public.banking_pay_batch_change_signals AS batch_signal
  WHERE batch_signal.pay_batch_id = p_pay_batch_id;

  v_signal_found := FOUND;

  SELECT
    COALESCE(display_summary.summary_version, 0),
    display_summary.updated_at_utc,
    COALESCE(display_summary.stale_summary_json, '{}'::jsonb),
    display_summary.latest_operation_id,
    display_summary.latest_operation_status,
    display_summary.latest_operation_phase
  INTO
    v_display_summary_version,
    v_display_summary_updated_at_utc,
    v_display_stale_summary_json,
    v_latest_operation_id,
    v_latest_operation_status,
    v_latest_operation_phase
  FROM public.pay_batch_display_summary AS display_summary
  WHERE display_summary.pay_batch_id = p_pay_batch_id;

  IF NOT FOUND THEN
    v_display_summary_version := 0;
    v_display_summary_updated_at_utc := NULL::timestamptz;
    v_display_stale_summary_json := '{}'::jsonb;
    v_latest_operation_id := NULL::uuid;
    v_latest_operation_status := NULL::text;
    v_latest_operation_phase := NULL::text;
  END IF;

  SELECT
    COALESCE(alert_summary.alert_hash, v_cached_alert_hash),
    COALESCE(alert_summary.summary_hash, v_cached_summary_hash),
    COALESCE(alert_summary.unacknowledged_count, 0),
    alert_summary.highest_severity,
    alert_summary.highest_label
  INTO
    v_cached_alert_hash,
    v_cached_summary_hash,
    v_cached_unacknowledged_count,
    v_cached_highest_severity,
    v_cached_highest_label
  FROM public.banking_alert_display_summary AS alert_summary
  WHERE alert_summary.actor_user_id = p_actor_user_id;

  IF NOT FOUND THEN
    v_cached_alert_hash := 'banking_alert_signal:v3:' || MD5('');
    v_cached_summary_hash := 'banking_alert_summary:v3:' || MD5('');
    v_cached_unacknowledged_count := 0;
    v_cached_highest_severity := NULL::text;
    v_cached_highest_label := NULL::text;
  END IF;


  v_display_summary_refresh_required := LOWER(BTRIM(COALESCE(v_display_stale_summary_json->>'summary_refresh_required', 'false'))) IN ('true','1','yes','y','on');

  IF v_signal_found THEN
    v_version := COALESCE(v_signal_row.version, 0);
    v_payment_status_version := COALESCE(v_signal_row.payment_status_version, 0);
    v_correction_progress_version := COALESCE(v_signal_row.correction_progress_version, 0);
    v_alert_version := COALESCE(v_signal_row.alert_version, 0);
    v_overview_version := COALESCE(v_signal_row.overview_version, 0);

    v_changed_transfer_id_count := COALESCE(
      CASE
        WHEN COALESCE(v_signal_row.last_change_scope_json->>'last_changed_transfer_id_count', v_signal_row.last_change_scope_json->>'changed_transfer_id_count', '') ~ '^[0-9]+$'
          THEN COALESCE(v_signal_row.last_change_scope_json->>'last_changed_transfer_id_count', v_signal_row.last_change_scope_json->>'changed_transfer_id_count')::integer
        ELSE NULL::integer
      END,
      CASE
        WHEN jsonb_typeof(COALESCE(v_signal_row.last_changed_transfer_ids, '[]'::jsonb)) = 'array'
          THEN jsonb_array_length(COALESCE(v_signal_row.last_changed_transfer_ids, '[]'::jsonb))
        ELSE 0
      END
    );

    v_changed_candidate_id_count := COALESCE(
      CASE
        WHEN COALESCE(v_signal_row.last_change_scope_json->>'last_changed_candidate_id_count', v_signal_row.last_change_scope_json->>'changed_candidate_id_count', '') ~ '^[0-9]+$'
          THEN COALESCE(v_signal_row.last_change_scope_json->>'last_changed_candidate_id_count', v_signal_row.last_change_scope_json->>'changed_candidate_id_count')::integer
        ELSE NULL::integer
      END,
      CASE
        WHEN jsonb_typeof(COALESCE(v_signal_row.last_changed_candidate_ids, '[]'::jsonb)) = 'array'
          THEN jsonb_array_length(COALESCE(v_signal_row.last_changed_candidate_ids, '[]'::jsonb))
        ELSE 0
      END
    );

    v_changed_pay_batch_item_id_count := COALESCE(
      CASE
        WHEN COALESCE(v_signal_row.last_change_scope_json->>'last_changed_pay_batch_item_id_count', v_signal_row.last_change_scope_json->>'changed_pay_batch_item_id_count', '') ~ '^[0-9]+$'
          THEN COALESCE(v_signal_row.last_change_scope_json->>'last_changed_pay_batch_item_id_count', v_signal_row.last_change_scope_json->>'changed_pay_batch_item_id_count')::integer
        ELSE NULL::integer
      END,
      CASE
        WHEN jsonb_typeof(COALESCE(v_signal_row.last_changed_pay_batch_item_ids, '[]'::jsonb)) = 'array'
          THEN jsonb_array_length(COALESCE(v_signal_row.last_changed_pay_batch_item_ids, '[]'::jsonb))
        ELSE 0
      END
    );

    v_changed_transfer_ids_hash := NULLIF(BTRIM(COALESCE(v_signal_row.last_change_scope_json->>'last_changed_transfer_ids_hash', v_signal_row.last_change_scope_json->>'changed_transfer_ids_hash', '')), '');
    v_changed_candidate_ids_hash := NULLIF(BTRIM(COALESCE(v_signal_row.last_change_scope_json->>'last_changed_candidate_ids_hash', v_signal_row.last_change_scope_json->>'changed_candidate_ids_hash', '')), '');
    v_changed_pay_batch_item_ids_hash := NULLIF(BTRIM(COALESCE(v_signal_row.last_change_scope_json->>'last_changed_pay_batch_item_ids_hash', v_signal_row.last_change_scope_json->>'changed_pay_batch_item_ids_hash', '')), '');

    SELECT COALESCE(jsonb_agg(to_jsonb(transfer_sample.changed_id) ORDER BY transfer_sample.ordinality), '[]'::jsonb)
    INTO v_changed_transfer_ids_sample
    FROM (
      SELECT changed_transfer_id_rows.changed_id, changed_transfer_id_rows.ordinality
      FROM jsonb_array_elements_text(
        CASE
          WHEN jsonb_typeof(COALESCE(v_signal_row.last_changed_transfer_ids, '[]'::jsonb)) = 'array'
            THEN COALESCE(v_signal_row.last_changed_transfer_ids, '[]'::jsonb)
          ELSE '[]'::jsonb
        END
      ) WITH ORDINALITY AS changed_transfer_id_rows(changed_id, ordinality)
      LIMIT 10
    ) AS transfer_sample;

    SELECT COALESCE(jsonb_agg(to_jsonb(candidate_sample.changed_id) ORDER BY candidate_sample.ordinality), '[]'::jsonb)
    INTO v_changed_candidate_ids_sample
    FROM (
      SELECT changed_candidate_id_rows.changed_id, changed_candidate_id_rows.ordinality
      FROM jsonb_array_elements_text(
        CASE
          WHEN jsonb_typeof(COALESCE(v_signal_row.last_changed_candidate_ids, '[]'::jsonb)) = 'array'
            THEN COALESCE(v_signal_row.last_changed_candidate_ids, '[]'::jsonb)
          ELSE '[]'::jsonb
        END
      ) WITH ORDINALITY AS changed_candidate_id_rows(changed_id, ordinality)
      LIMIT 10
    ) AS candidate_sample;

    SELECT COALESCE(jsonb_agg(to_jsonb(item_sample.changed_id) ORDER BY item_sample.ordinality), '[]'::jsonb)
    INTO v_changed_pay_batch_item_ids_sample
    FROM (
      SELECT changed_item_id_rows.changed_id, changed_item_id_rows.ordinality
      FROM jsonb_array_elements_text(
        CASE
          WHEN jsonb_typeof(COALESCE(v_signal_row.last_changed_pay_batch_item_ids, '[]'::jsonb)) = 'array'
            THEN COALESCE(v_signal_row.last_changed_pay_batch_item_ids, '[]'::jsonb)
          ELSE '[]'::jsonb
        END
      ) WITH ORDINALITY AS changed_item_id_rows(changed_id, ordinality)
      LIMIT 10
    ) AS item_sample;

    IF v_changed_transfer_ids_hash IS NULL THEN
      v_changed_transfer_ids_hash := 'changed_transfer_ids:v1:' || COALESCE(v_changed_transfer_id_count, 0)::text || ':' || MD5(COALESCE(v_changed_transfer_ids_sample, '[]'::jsonb)::text);
    END IF;

    IF v_changed_candidate_ids_hash IS NULL THEN
      v_changed_candidate_ids_hash := 'changed_candidate_ids:v1:' || COALESCE(v_changed_candidate_id_count, 0)::text || ':' || MD5(COALESCE(v_changed_candidate_ids_sample, '[]'::jsonb)::text);
    END IF;

    IF v_changed_pay_batch_item_ids_hash IS NULL THEN
      v_changed_pay_batch_item_ids_hash := 'changed_pay_batch_item_ids:v1:' || COALESCE(v_changed_pay_batch_item_id_count, 0)::text || ':' || MD5(COALESCE(v_changed_pay_batch_item_ids_sample, '[]'::jsonb)::text);
    END IF;

    v_last_change_scope_summary_json := (
      COALESCE(v_signal_row.last_change_scope_json, '{}'::jsonb)
        - 'last_changed_transfer_ids'
        - 'changed_transfer_ids'
        - 'pay_bank_transfer_ids'
        - 'transfer_ids'
        - 'last_changed_candidate_ids'
        - 'changed_candidate_ids'
        - 'candidate_ids'
        - 'last_changed_pay_batch_item_ids'
        - 'changed_pay_batch_item_ids'
        - 'pay_batch_item_ids'
        - 'item_ids'
    ) || jsonb_build_object(
      'last_changed_transfer_ids_sample', COALESCE(v_changed_transfer_ids_sample, '[]'::jsonb),
      'last_changed_transfer_id_count', COALESCE(v_changed_transfer_id_count, 0),
      'last_changed_transfer_ids_hash', v_changed_transfer_ids_hash,
      'last_changed_candidate_ids_sample', COALESCE(v_changed_candidate_ids_sample, '[]'::jsonb),
      'last_changed_candidate_id_count', COALESCE(v_changed_candidate_id_count, 0),
      'last_changed_candidate_ids_hash', v_changed_candidate_ids_hash,
      'last_changed_pay_batch_item_ids_sample', COALESCE(v_changed_pay_batch_item_ids_sample, '[]'::jsonb),
      'last_changed_pay_batch_item_id_count', COALESCE(v_changed_pay_batch_item_id_count, 0),
      'last_changed_pay_batch_item_ids_hash', v_changed_pay_batch_item_ids_hash,
      'changed_id_samples_are_capped', true,
      'changed_id_sample_cap', 10
    );
  END IF;

  v_payment_status_changed := p_known_payment_status_version IS NULL OR p_known_payment_status_version IS DISTINCT FROM v_payment_status_version;
  v_correction_progress_changed := p_known_correction_progress_version IS NULL OR p_known_correction_progress_version IS DISTINCT FROM v_correction_progress_version;
  v_alert_changed := p_known_alert_version IS NULL OR p_known_alert_version IS DISTINCT FROM v_alert_version;
  v_overview_changed := p_known_overview_version IS NULL OR p_known_overview_version IS DISTINCT FROM v_overview_version;
  v_changed := p_known_version IS NULL OR p_known_version IS DISTINCT FROM v_version OR v_payment_status_changed OR v_correction_progress_changed OR v_alert_changed OR v_overview_changed;

  IF v_display_summary_refresh_required THEN
    v_changed := true;
    v_overview_changed := true;
    v_payment_status_changed := true;
  END IF;

  IF v_payment_status_changed THEN
    v_changed_areas := array_append(v_changed_areas, 'payment_status');
  END IF;

  IF v_correction_progress_changed THEN
    v_changed_areas := array_append(v_changed_areas, 'correction_progress');
  END IF;

  IF v_alert_changed THEN
    v_changed_areas := array_append(v_changed_areas, 'alerts');
  END IF;

  IF v_overview_changed THEN
    v_changed_areas := array_append(v_changed_areas, 'overview');
  END IF;

  IF v_changed IS NOT TRUE THEN
    v_recommended_refresh := 'NONE';
  ELSIF v_current_tab = 'overview' AND v_overview_changed THEN
    v_recommended_refresh := 'OVERVIEW_VISIBLE_PAGE';
  ELSIF v_current_tab IN ('current_payment_status', 'payment_status', 'payment_issues') AND (v_payment_status_changed OR v_correction_progress_changed) THEN
    v_recommended_refresh := 'CURRENT_PAYMENT_STATUS_VISIBLE_PAGE';
  ELSIF v_current_tab IN ('candidates', 'items', 'item_breakdowns', 'transfers', 'finance_case_groups', 'remittances', 'communications', 'auth_history', 'events') THEN
    v_recommended_refresh := 'VISIBLE_SECTION_PAGE';
  ELSIF v_alert_changed THEN
    v_recommended_refresh := 'ALERT_SIGNAL_ONLY';
  ELSE
    v_recommended_refresh := 'VISIBLE_CONTEXT_ONLY';
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'pay_batch_id', p_pay_batch_id::text,
    'server_time_utc', now(),
    'version', v_version,
    'display_summary_version', COALESCE(v_display_summary_version, 0),
    'display_summary_updated_at_utc', CASE WHEN v_display_summary_updated_at_utc IS NULL THEN NULL::text ELSE v_display_summary_updated_at_utc::text END,
    'display_summary_refresh_required', COALESCE(v_display_summary_refresh_required, false),
    'display_stale_summary_json', COALESCE(v_display_stale_summary_json, '{}'::jsonb),
    'latest_operation_id', CASE WHEN v_latest_operation_id IS NULL THEN NULL::text ELSE v_latest_operation_id::text END,
    'latest_operation_status', v_latest_operation_status,
    'latest_operation_phase', v_latest_operation_phase,
    'payment_status_version', v_payment_status_version,
    'correction_progress_version', v_correction_progress_version,
    'alert_version', v_alert_version,
    'overview_version', v_overview_version,
    'changed', COALESCE(v_changed, false),
    'changed_areas', to_jsonb(v_changed_areas),
    'recommended_refresh', v_recommended_refresh,
    'last_change_reason', CASE WHEN v_signal_found THEN v_signal_row.last_change_reason ELSE NULL::text END,
    'last_change_source', CASE WHEN v_signal_found THEN v_signal_row.last_change_source ELSE NULL::text END,
    'last_change_scope_json', CASE WHEN v_signal_found THEN COALESCE(v_last_change_scope_summary_json, '{}'::jsonb) ELSE '{}'::jsonb END,
    'last_changed_transfer_ids', COALESCE(v_changed_transfer_ids_sample, '[]'::jsonb),
    'last_changed_transfer_ids_sample', COALESCE(v_changed_transfer_ids_sample, '[]'::jsonb),
    'last_changed_transfer_id_count', COALESCE(v_changed_transfer_id_count, 0),
    'last_changed_transfer_ids_hash', v_changed_transfer_ids_hash,
    'last_changed_candidate_ids', COALESCE(v_changed_candidate_ids_sample, '[]'::jsonb),
    'last_changed_candidate_ids_sample', COALESCE(v_changed_candidate_ids_sample, '[]'::jsonb),
    'last_changed_candidate_id_count', COALESCE(v_changed_candidate_id_count, 0),
    'last_changed_candidate_ids_hash', v_changed_candidate_ids_hash,
    'last_changed_pay_batch_item_ids', COALESCE(v_changed_pay_batch_item_ids_sample, '[]'::jsonb),
    'last_changed_pay_batch_item_ids_sample', COALESCE(v_changed_pay_batch_item_ids_sample, '[]'::jsonb),
    'last_changed_pay_batch_item_id_count', COALESCE(v_changed_pay_batch_item_id_count, 0),
    'last_changed_pay_batch_item_ids_hash', v_changed_pay_batch_item_ids_hash,
    'last_status_hash', CASE WHEN v_signal_found THEN v_signal_row.last_status_hash ELSE NULL::text END,
    'last_alert_hash', COALESCE(CASE WHEN v_signal_found THEN v_signal_row.last_alert_hash ELSE NULL::text END, v_cached_alert_hash),
    'banking_alert_hash', v_cached_alert_hash,
    'banking_alert_summary_signature', v_cached_summary_hash,
    'banking_unacknowledged_alert_count', COALESCE(v_cached_unacknowledged_count, 0),
    'banking_highest_alert_severity', COALESCE(v_cached_highest_severity, ''),
    'banking_highest_alert_label', COALESCE(v_cached_highest_label, ''),
    'current_tab', v_current_tab,
    'current_section_json', v_current_section_json
  );
END;
$function$;

-- banking_pay_draft_create_step_v1(uuid,text,text,integer)
CREATE OR REPLACE FUNCTION public.banking_pay_draft_create_step_v1(p_operation_id uuid, p_worker_id text, p_expected_phase text, p_request_budget_ms integer DEFAULT 25000)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'pg_temp'
AS $function$
DECLARE
  v_started_at timestamptz:=pg_catalog.clock_timestamp();
  v_business_started_at timestamptz;
  v_now timestamptz:=pg_catalog.clock_timestamp();
  v_operation public.banking_pay_operations%ROWTYPE;
  v_chunk record;
  v_group record;
  v_phase text:=pg_catalog.upper(pg_catalog.btrim(COALESCE(p_expected_phase,'')));
  v_next_phase text;
  v_worker_id text:=NULLIF(pg_catalog.btrim(COALESCE(p_worker_id,'')),'');
  v_scope_ids jsonb:='[]'::jsonb;
  v_result jsonb:='{}'::jsonb;
  v_results jsonb:='[]'::jsonb;
  v_saved jsonb:='{}'::jsonb;
  v_finished jsonb:='{}'::jsonb;
  v_business_ms integer:=0;
  v_scope_count integer:=0;
  v_pay_date date;
  v_week_start date;
  v_enabled boolean:=false;
  v_processed_chunk_count integer:=0;
  v_processed_scope_count integer:=0;
  v_elapsed_ms integer:=0;
BEGIN
  PERFORM public.banking_pay_hot_path_budget_apply('WORKBENCH_CHUNK');

  SELECT COALESCE(setting.banking_pay_draft_step_rpc_v1_enabled,false)
  INTO v_enabled
  FROM public.settings_defaults AS setting
  WHERE setting.id=1;
  IF v_enabled IS NOT TRUE THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok',true,'handled',false,'code','DRAFT_CREATE_STEP_RPC_DISABLED'
    );
  END IF;

  IF p_operation_id IS NULL OR v_worker_id IS NULL
     OR v_phase NOT IN (
       'SEED_ALLOCATION_ROWS','INSERT_CANDIDATES','INSERT_ITEMS',
       'APPLY_FINANCE_ADJUSTMENTS','FINALISE_RESERVATIONS',
       'POPULATE_CANDIDATE_SUMMARIES','CREATE_TIMESHEET_SNAPSHOTS',
       'BUILD_ITEM_BREAKDOWNS'
     ) OR COALESCE(p_request_budget_ms,25000) NOT BETWEEN 1000 AND 25000 THEN
    RAISE EXCEPTION 'BANKING_PAY_DRAFT_CREATE_STEP_INPUT_INVALID'
      USING ERRCODE='22023',DETAIL=pg_catalog.jsonb_build_object(
        'code','BANKING_PAY_DRAFT_CREATE_STEP_INPUT_INVALID',
        'operation_id',p_operation_id,
        'expected_phase',v_phase
      )::text;
  END IF;

  SELECT operation_row.* INTO v_operation
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.id=p_operation_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RETURN pg_catalog.jsonb_build_object('ok',false,'handled',true,'code','DRAFT_CREATE_OPERATION_NOT_FOUND');
  END IF;
  IF pg_catalog.upper(pg_catalog.btrim(COALESCE(v_operation.operation_type,'')))<>'DRAFT_CREATE'
     OR pg_catalog.upper(pg_catalog.btrim(COALESCE(v_operation.status,''))) NOT IN ('RUNNING','CONTINUING','WAITING_RETRY') THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok',true,'handled',false,'code','DRAFT_CREATE_OPERATION_NOT_RUNNABLE',
      'operation',pg_catalog.to_jsonb(v_operation)
    );
  END IF;
  IF pg_catalog.upper(pg_catalog.btrim(COALESCE(v_operation.phase,'')))<>v_phase THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok',true,'handled',false,'code','DRAFT_CREATE_PHASE_MOVED',
      'operation',pg_catalog.to_jsonb(v_operation)
    );
  END IF;
  IF COALESCE(v_operation.lease_owner,v_operation.locked_by) IS NOT NULL
     AND COALESCE(v_operation.lease_owner,v_operation.locked_by)<>v_worker_id THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok',true,'handled',false,'code','DRAFT_CREATE_LEASE_OWNER_MISMATCH',
      'operation',pg_catalog.to_jsonb(v_operation)
    );
  END IF;
  IF COALESCE(v_operation.lease_expires_at_utc,v_operation.lock_expires_at_utc) IS NOT NULL
     AND COALESCE(v_operation.lease_expires_at_utc,v_operation.lock_expires_at_utc)<=v_now THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok',true,'handled',false,'code','DRAFT_CREATE_LEASE_EXPIRED',
      'operation',pg_catalog.to_jsonb(v_operation)
    );
  END IF;

  v_next_phase:=CASE v_phase
    WHEN 'SEED_ALLOCATION_ROWS' THEN 'CREATE_BATCH_SHELLS'
    WHEN 'INSERT_CANDIDATES' THEN 'INSERT_ITEMS'
    WHEN 'INSERT_ITEMS' THEN 'APPLY_FINANCE_ADJUSTMENTS'
    WHEN 'APPLY_FINANCE_ADJUSTMENTS' THEN 'FINALISE_RESERVATIONS'
    WHEN 'FINALISE_RESERVATIONS' THEN 'POPULATE_CANDIDATE_SUMMARIES'
    WHEN 'POPULATE_CANDIDATE_SUMMARIES' THEN 'CREATE_TIMESHEET_SNAPSHOTS'
    WHEN 'CREATE_TIMESHEET_SNAPSHOTS' THEN 'BUILD_ITEM_BREAKDOWNS'
    WHEN 'BUILD_ITEM_BREAKDOWNS' THEN 'ASSERT_INTEGRITY'
  END;

  LOOP
    v_scope_ids:='[]'::jsonb;
    v_result:='{}'::jsonb;
    v_results:='[]'::jsonb;
    v_finished:='{}'::jsonb;
    v_scope_count:=0;

    SELECT claimed_chunk.* INTO v_chunk
    FROM public.banking_pay_operation_claim_chunk(
      p_operation_id,v_phase,'CANDIDATE_SCOPE',v_worker_id,
      LEAST(GREATEST(COALESCE(v_operation.config_json->>'lock_seconds','60')::integer,5),3600)
    ) AS claimed_chunk
    LIMIT 1;

  IF NOT FOUND OR v_chunk.chunk_id IS NULL THEN
    SELECT pg_catalog.to_jsonb(saved_row) INTO v_saved
    FROM public.banking_pay_operation_save_progress(
      p_operation_id,'RUNNING',v_next_phase,NULL,NULL,NULL,NULL,NULL,
      pg_catalog.jsonb_build_object(
        'status_text',v_phase||' chunks complete.',
        'draft_step_rpc',true,
        'draft_step_phase_complete',true,
        'draft_step_round_trip_count',1,
        'draft_step_processed_chunk_count',v_processed_chunk_count,
        'draft_step_processed_scope_count',v_processed_scope_count,
        'draft_step_elapsed_ms',pg_catalog.floor(extract(epoch FROM (pg_catalog.clock_timestamp()-v_started_at))*1000)::integer
      ),NULL
    ) AS saved_row
    LIMIT 1;
    RETURN pg_catalog.jsonb_build_object(
      'ok',true,'handled',true,'phase_complete',true,
      'phase',v_phase,'next_phase',v_next_phase,'operation',v_saved,
      'round_trip_count',1,
      'processed_chunk_count',v_processed_chunk_count,
      'processed_scope_count',v_processed_scope_count,
      'elapsed_ms',pg_catalog.floor(extract(epoch FROM (pg_catalog.clock_timestamp()-v_started_at))*1000)::integer
    );
  END IF;

  IF COALESCE((v_chunk.payload_json->>'row_backed')::boolean,false) IS NOT TRUE
     OR COALESCE((v_chunk.payload_json->>'legacy_tiny_compat')::boolean,false) IS TRUE
     OR COALESCE(v_chunk.payload_json->>'source_table','')='diagnostic_legacy_units' THEN
    RAISE EXCEPTION 'DRAFT_CREATE_STEP_CHUNK_NOT_ROW_BACKED'
      USING ERRCODE='P0001',DETAIL=pg_catalog.jsonb_build_object(
        'code','DRAFT_CREATE_STEP_CHUNK_NOT_ROW_BACKED','chunk_id',v_chunk.chunk_id,'phase',v_phase
      )::text;
  END IF;

  SELECT COALESCE(pg_catalog.jsonb_agg(scope_id ORDER BY scope_id),'[]'::jsonb)
  INTO v_scope_ids
  FROM (
    SELECT DISTINCT value::uuid AS scope_id
    FROM pg_catalog.jsonb_array_elements_text(
      CASE
        WHEN pg_catalog.jsonb_typeof(v_chunk.payload_json->'units')='array' THEN v_chunk.payload_json->'units'
        WHEN pg_catalog.jsonb_typeof(v_chunk.payload_json->'candidate_scope_ids')='array' THEN v_chunk.payload_json->'candidate_scope_ids'
        WHEN pg_catalog.jsonb_typeof(v_chunk.payload_json->'scope_unit_ids')='array' THEN v_chunk.payload_json->'scope_unit_ids'
        ELSE '[]'::jsonb
      END
    ) AS unit(value)
    WHERE value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  ) AS scope_ids;
  v_scope_count:=pg_catalog.jsonb_array_length(v_scope_ids);
  IF COALESCE(v_chunk.unit_count,0)>0 AND v_scope_count=0 THEN
    RAISE EXCEPTION 'DRAFT_CREATE_STEP_SCOPE_IDS_MISSING'
      USING ERRCODE='P0001',DETAIL=pg_catalog.jsonb_build_object(
        'code','DRAFT_CREATE_STEP_SCOPE_IDS_MISSING','chunk_id',v_chunk.chunk_id,'phase',v_phase
      )::text;
  END IF;

  v_pay_date:=NULLIF(v_operation.input_json->>'pay_date','')::date;
  v_week_start:=NULLIF(v_operation.input_json->>'week_start','')::date;
  v_business_started_at:=pg_catalog.clock_timestamp();

  IF v_phase='SEED_ALLOCATION_ROWS' THEN
    SELECT pg_catalog.to_jsonb(seed_row) INTO v_result
    FROM public.pay_workbench_prepare_draft_allocation_rows_seed(p_operation_id,v_scope_ids) AS seed_row
    LIMIT 1;
    v_result:=COALESCE(v_result,'{}'::jsonb);
    v_results:=pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'candidate_scope_ids',v_scope_ids,'result',v_result
    ));
  ELSE
    FOR v_group IN
      SELECT scope.pay_batch_id,scope.pay_channel,
             pg_catalog.jsonb_agg(scope.id ORDER BY scope.id) AS scope_ids
      FROM public.banking_pay_operation_candidate_scope AS scope
      WHERE scope.operation_id=p_operation_id
        AND scope.id IN (
          SELECT value::uuid FROM pg_catalog.jsonb_array_elements_text(v_scope_ids) AS unit(value)
        )
      GROUP BY scope.pay_batch_id,scope.pay_channel
      ORDER BY scope.pay_batch_id,scope.pay_channel
    LOOP
      IF v_group.pay_batch_id IS NULL THEN
        RAISE EXCEPTION 'DRAFT_CREATE_STEP_BATCH_ID_MISSING' USING ERRCODE='P0001';
      END IF;
      v_result:=CASE v_phase
        WHEN 'INSERT_CANDIDATES' THEN public.pay_batch_insert_candidates_from_preview(
          v_group.pay_batch_id,v_operation.actor_user_id,p_operation_id,v_group.scope_ids)
        WHEN 'INSERT_ITEMS' THEN public.pay_batch_insert_items_from_preview(
          v_group.pay_batch_id,v_operation.actor_user_id,p_operation_id,v_group.scope_ids)
        WHEN 'APPLY_FINANCE_ADJUSTMENTS' THEN public.pay_batch_apply_finance_adjustments(
          v_group.pay_batch_id,v_group.pay_channel,v_operation.actor_user_id,NULL,NULL,p_operation_id,v_group.scope_ids)
        WHEN 'FINALISE_RESERVATIONS' THEN public.pay_batch_finalize_reservations_and_markers(
          v_group.pay_batch_id,v_group.pay_channel,v_operation.actor_user_id,v_pay_date,v_week_start,p_operation_id,v_group.scope_ids)
        WHEN 'POPULATE_CANDIDATE_SUMMARIES' THEN public.pay_batch_populate_candidate_summaries(
          v_group.pay_batch_id,v_group.pay_channel,v_operation.actor_user_id,p_operation_id,v_group.scope_ids)
        WHEN 'CREATE_TIMESHEET_SNAPSHOTS' THEN public.pay_batch_create_timesheet_snapshots(
          v_group.pay_batch_id,v_operation.actor_user_id,p_operation_id,v_group.scope_ids)
        WHEN 'BUILD_ITEM_BREAKDOWNS' THEN public.pay_batch_build_item_breakdowns(
          v_group.pay_batch_id,v_operation.actor_user_id,p_operation_id,v_group.scope_ids)
      END;
      IF COALESCE((v_result->>'ok')::boolean,true) IS NOT TRUE THEN
        RAISE EXCEPTION 'DRAFT_CREATE_STEP_BUSINESS_OWNER_REJECTED'
          USING ERRCODE='P0001',DETAIL=pg_catalog.jsonb_build_object(
            'code','DRAFT_CREATE_STEP_BUSINESS_OWNER_REJECTED','phase',v_phase,
            'chunk_id',v_chunk.chunk_id,'pay_batch_id',v_group.pay_batch_id,
            'result',v_result
          )::text;
      END IF;
      v_results:=v_results||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'pay_batch_id',v_group.pay_batch_id,'pay_channel',v_group.pay_channel,
        'candidate_scope_ids',v_group.scope_ids,'result',v_result
      ));
    END LOOP;
  END IF;

  v_business_ms:=pg_catalog.floor(extract(epoch FROM (pg_catalog.clock_timestamp()-v_business_started_at))*1000)::integer;

  SELECT pg_catalog.to_jsonb(finished_row) INTO v_finished
  FROM public.banking_pay_operation_finish_chunk(
    v_chunk.chunk_id,'COMPLETE',v_scope_count,0,
    pg_catalog.jsonb_build_object('phase',v_phase,'scope_ids',v_scope_ids,'results',v_results),NULL
  ) AS finished_row
  LIMIT 1;

  SELECT pg_catalog.to_jsonb(saved_row) INTO v_saved
  FROM public.banking_pay_operation_save_progress(
    p_operation_id,'RUNNING',v_phase,NULL,v_scope_count,0,v_chunk.sequence_no,NULL,
    pg_catalog.jsonb_build_object(
      'status_text','Processed one '||v_phase||' Draft chunk.',
      'draft_step_rpc',true,
      'draft_step_phase',v_phase,
      'draft_step_chunk_id',v_chunk.chunk_id,
      'draft_step_business_ms',v_business_ms,
      'draft_step_round_trip_count',1,
      'draft_step_processed_chunk_count',v_processed_chunk_count+1,
      'draft_step_processed_scope_count',v_processed_scope_count+v_scope_count,
      'draft_step_elapsed_ms',pg_catalog.floor(extract(epoch FROM (pg_catalog.clock_timestamp()-v_started_at))*1000)::integer
    ),NULL
  ) AS saved_row
  LIMIT 1;

  v_processed_chunk_count:=v_processed_chunk_count+1;
  v_processed_scope_count:=v_processed_scope_count+v_scope_count;
  v_elapsed_ms:=pg_catalog.floor(extract(epoch FROM (pg_catalog.clock_timestamp()-v_started_at))*1000)::integer;

  -- Keep consuming same-phase chunks while the caller's existing bounded
  -- request budget has safe headroom.  When the final chunk was just
  -- completed, the next loop observes no work and advances the phase inside
  -- this same RPC/transaction instead of requiring an otherwise empty HTTP
  -- round trip at 99%.
  IF v_elapsed_ms < GREATEST(500,p_request_budget_ms-1000) THEN
    CONTINUE;
  END IF;

  RETURN pg_catalog.jsonb_build_object(
    'ok',true,'handled',true,'phase_complete',false,
    'phase',v_phase,'next_phase',v_phase,'chunk_id',v_chunk.chunk_id,
    'scope_count',v_scope_count,'phase_result',v_results,
    'chunk',v_finished,'operation',v_saved,'business_ms',v_business_ms,
    'processed_chunk_count',v_processed_chunk_count,
    'processed_scope_count',v_processed_scope_count,
    'round_trip_count',1,
    'elapsed_ms',v_elapsed_ms
  );
  END LOOP;
END;
$function$;

-- banking_pay_hot_path_budget_apply(text)
CREATE OR REPLACE FUNCTION public.banking_pay_hot_path_budget_apply(p_route_class text DEFAULT 'DISPLAY'::text)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_route_class text := UPPER(REPLACE(NULLIF(BTRIM(COALESCE(p_route_class, 'DISPLAY')), ''), '-', '_'));
  v_statement_timeout_ms integer := 5000;
  v_lock_timeout_ms integer := 1000;
  v_idle_tx_timeout_ms integer := 15000;
  v_settings_statement_timeout_ms integer := NULL::integer;
  v_settings_lock_timeout_ms integer := NULL::integer;
  v_settings_idle_tx_timeout_ms integer := NULL::integer;
BEGIN
  IF v_route_class IN (
    'DISPLAY',
    'LIST',
    'BATCH_LIST',
    'BOOTSTRAP',
    'BATCH_BOOTSTRAP',
    'PROGRESS',
    'OPERATION_PROGRESS',
    'PREVIEW_PROGRESS',
    'WATCH',
    'WATCH_SIGNAL',
    'OVERVIEW',
    'OVERVIEW_PAGE',
    'RPC_CHANGES_PING',
    'CHANGES_PING'
  ) THEN
    v_statement_timeout_ms := 3000;
    v_lock_timeout_ms := 750;
    v_idle_tx_timeout_ms := 10000;

  ELSIF v_route_class IN (
    'PREVIEW_CHUNK',
    'WORKBENCH_CHUNK',
    'WORKBENCH_WORKER_CHUNK',
    'EXECUTION_CHUNK',
    'WORKER_CHUNK',
    'PROVIDER_CHUNK',
    'OPERATION_WORKER',
    'OPERATION_ADVANCE',
    'TRANSFER_SCOPE_CHUNK',
    'FRESHNESS_CHUNK'
  ) THEN
    v_statement_timeout_ms := 15000;
    v_lock_timeout_ms := 1500;
    v_idle_tx_timeout_ms := 30000;

  ELSIF v_route_class IN (
    'DIAGNOSTIC',
    'EXPLICIT_DIAGNOSTIC',
    'ALERT_PANEL',
    'ALERT_REFRESH_JOB',
    'EXPORT',
    'ADMIN'
  ) THEN
    v_statement_timeout_ms := 30000;
    v_lock_timeout_ms := 3000;
    v_idle_tx_timeout_ms := 60000;

  ELSE
    v_statement_timeout_ms := 5000;
    v_lock_timeout_ms := 1000;
    v_idle_tx_timeout_ms := 15000;
  END IF;

  IF v_route_class IN ('WORKBENCH_CHUNK', 'WORKBENCH_WORKER_CHUNK') THEN
    SELECT
      sd.banking_pay_workbench_db_statement_timeout_ms,
      sd.banking_pay_workbench_db_lock_timeout_ms,
      sd.banking_pay_workbench_db_idle_tx_timeout_ms
    INTO
      v_settings_statement_timeout_ms,
      v_settings_lock_timeout_ms,
      v_settings_idle_tx_timeout_ms
    FROM public.settings_defaults AS sd
    WHERE sd.id = 1
    LIMIT 1;

    v_statement_timeout_ms := LEAST(GREATEST(COALESCE(v_settings_statement_timeout_ms, v_statement_timeout_ms), 1000), 30000);
    v_lock_timeout_ms := LEAST(GREATEST(COALESCE(v_settings_lock_timeout_ms, v_lock_timeout_ms), 100), 5000);
    v_idle_tx_timeout_ms := LEAST(GREATEST(COALESCE(v_settings_idle_tx_timeout_ms, v_idle_tx_timeout_ms), 5000), 60000);
  END IF;

  PERFORM set_config('statement_timeout', v_statement_timeout_ms::text, true);
  PERFORM set_config('lock_timeout', v_lock_timeout_ms::text, true);
  PERFORM set_config('idle_in_transaction_session_timeout', v_idle_tx_timeout_ms::text, true);
END;
$function$;

-- banking_pay_operation_claim_chunk(uuid,text,text,text,integer)
CREATE OR REPLACE FUNCTION public.banking_pay_operation_claim_chunk(p_operation_id uuid, p_phase text, p_chunk_type text, p_lock_owner text, p_lock_seconds integer DEFAULT 60)
 RETURNS TABLE(chunk_id uuid, operation_id uuid, phase text, chunk_type text, sequence_no integer, status text, payload_json jsonb, result_json jsonb, error_json jsonb, unit_count integer, completed_count integer, failed_count integer, locked_by text, lock_expires_at_utc timestamp with time zone, created_at_utc timestamp with time zone, started_at_utc timestamp with time zone, completed_at_utc timestamp with time zone, updated_at_utc timestamp with time zone)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_phase text := nullif(btrim(coalesce(p_phase, '')), '');
  v_chunk_type text := upper(nullif(btrim(coalesce(p_chunk_type, '')), ''));
  v_lock_owner text := coalesce(nullif(btrim(coalesce(p_lock_owner, '')), ''), 'unknown');
  v_lock_seconds integer := LEAST(GREATEST(coalesce(p_lock_seconds, 60), 5), 3600);
  v_chunk public.banking_pay_operation_chunks%ROWTYPE;
BEGIN
  IF p_operation_id IS NULL THEN
    RAISE EXCEPTION 'banking_pay_operation_claim_chunk requires p_operation_id';
  END IF;

  IF v_phase IS NULL THEN
    RAISE EXCEPTION 'banking_pay_operation_claim_chunk requires p_phase';
  END IF;

  IF v_chunk_type IS NULL THEN
    RAISE EXCEPTION 'banking_pay_operation_claim_chunk requires p_chunk_type';
  END IF;

  IF v_chunk_type NOT IN (
    'CANDIDATE_SCOPE',
    'TSFIN',
    'PAYEE_READINESS',
    'TRANSFER_GROUP',
    'TRANSFER_SCOPE_ITEM_SEED',
    'TRANSFER_SCOPE_ROLLUP',
    'TRANSFER_SUBMIT',
    'RAIL_UPDATE',
    'SETTLEMENT',
    'REMITTANCE',
    'PAYOUT_NOTICE',
    'PREVIEW_PAGE',
    'FRESHNESS_VALIDATE'
  ) THEN
    RAISE EXCEPTION 'Unsupported banking pay operation chunk_type: %', v_chunk_type;
  END IF;

  WITH claimable_chunk AS (
    SELECT chunk_row.id
    FROM public.banking_pay_operation_chunks AS chunk_row
    WHERE chunk_row.operation_id = p_operation_id
      AND chunk_row.phase = v_phase
      AND chunk_row.chunk_type = v_chunk_type
      AND (
        chunk_row.status = 'PENDING'
        OR (
          chunk_row.status = 'RUNNING'
          AND (chunk_row.lock_expires_at_utc IS NULL OR chunk_row.lock_expires_at_utc <= now())
        )
      )
    ORDER BY chunk_row.sequence_no ASC, chunk_row.created_at_utc NULLS FIRST, chunk_row.id
    LIMIT 1
    FOR UPDATE SKIP LOCKED
  )
  UPDATE public.banking_pay_operation_chunks AS chunk_update
  SET status = 'RUNNING',
      locked_by = v_lock_owner,
      lock_expires_at_utc = now() + make_interval(secs => v_lock_seconds),
      started_at_utc = coalesce(chunk_update.started_at_utc, now()),
      updated_at_utc = now()
  FROM claimable_chunk
  WHERE chunk_update.id = claimable_chunk.id
  RETURNING chunk_update.* INTO v_chunk;

  IF NOT FOUND THEN
    RETURN;
  END IF;

  RETURN QUERY
  SELECT v_chunk.id,
         v_chunk.operation_id,
         v_chunk.phase,
         v_chunk.chunk_type,
         v_chunk.sequence_no,
         v_chunk.status,
         v_chunk.payload_json,
         v_chunk.result_json,
         v_chunk.error_json,
         v_chunk.unit_count,
         v_chunk.completed_count,
         v_chunk.failed_count,
         v_chunk.locked_by,
         v_chunk.lock_expires_at_utc,
         v_chunk.created_at_utc,
         v_chunk.started_at_utc,
         v_chunk.completed_at_utc,
         v_chunk.updated_at_utc;
END;
$function$;

-- banking_pay_operation_claim_next(uuid,uuid,text,integer,boolean,text[])
CREATE OR REPLACE FUNCTION public.banking_pay_operation_claim_next(p_operation_id uuid DEFAULT NULL::uuid, p_actor_user_id uuid DEFAULT NULL::uuid, p_lock_owner text DEFAULT NULL::text, p_lock_seconds integer DEFAULT 60, p_allow_backend_runner_owned boolean DEFAULT false, p_operation_types text[] DEFAULT NULL::text[])
 RETURNS TABLE(claimed boolean, not_claimed_reason text, operation_id uuid, operation_type text, status text, phase text, actor_user_id uuid, workbench_session_id uuid, pay_batch_id uuid, root_operation_id uuid, idempotency_key text, input_json jsonb, config_json jsonb, progress_json jsonb, result_json jsonb, error_json jsonb, total_units integer, completed_units integer, failed_units integer, current_chunk_index integer, chunk_count integer, locked_by text, lock_expires_at_utc timestamp with time zone, created_at_utc timestamp with time zone, started_at_utc timestamp with time zone, updated_at_utc timestamp with time zone, completed_at_utc timestamp with time zone, failed_at_utc timestamp with time zone)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'private', 'extensions', 'pg_temp'
AS $function$
DECLARE
    v_now timestamptz := now();
    v_operation public.banking_pay_operations%ROWTYPE;
    v_visible public.banking_pay_operations%ROWTYPE;
    v_lock_owner text := COALESCE(NULLIF(BTRIM(COALESCE(p_lock_owner, '')), ''), 'banking-runner:' || pg_backend_pid()::text);
    v_lock_seconds integer := LEAST(GREATEST(COALESCE(p_lock_seconds, 60), 10), 3600);
    v_not_claimed_reason text := NULL::text;
    v_allow_backend_runner_owned boolean := COALESCE(p_allow_backend_runner_owned, false);
    v_operation_types text[] := ARRAY[]::text[];
    v_visible_operation_type text := NULL::text;
    v_visible_backend_runner_claimable boolean := false;
    v_visible_actor_authorised boolean := false;
BEGIN
    PERFORM set_config('lock_timeout', '3s', true);

    SELECT COALESCE(array_agg(DISTINCT supplied_operation_type.normalized_operation_type) FILTER (WHERE supplied_operation_type.normalized_operation_type IS NOT NULL), ARRAY[]::text[])
    INTO v_operation_types
    FROM (
      SELECT NULLIF(upper(BTRIM(COALESCE(operation_type_value, ''))), '') AS normalized_operation_type
      FROM unnest(COALESCE(p_operation_types, ARRAY[]::text[])) AS supplied(operation_type_value)
    ) AS supplied_operation_type;

    IF COALESCE(array_length(v_operation_types, 1), 0) = 0 AND v_allow_backend_runner_owned IS TRUE THEN
      v_operation_types := ARRAY['DRAFT_CREATE', 'PAYMENT_EXECUTE', 'PAYMENT_RETRY_BLOCKED_FUNDS', 'PAYMENT_SETTLEMENT', 'REMITTANCE_QUEUE', 'PAYMENT_CORRECTION']::text[];
    END IF;

    WITH claimable AS (
      SELECT operation_row.id
      FROM public.banking_pay_operations AS operation_row
      WHERE (p_operation_id IS NULL OR operation_row.id = p_operation_id)
        AND (
          COALESCE(array_length(v_operation_types, 1), 0) = 0
          OR upper(BTRIM(COALESCE(operation_row.operation_type, ''))) = ANY(v_operation_types)
        )
        AND (
          p_actor_user_id IS NULL
          OR operation_row.actor_user_id IS NULL
          OR operation_row.actor_user_id = p_actor_user_id
          OR (
            v_allow_backend_runner_owned IS TRUE
            AND upper(BTRIM(COALESCE(operation_row.operation_type, ''))) = ANY(v_operation_types)
            AND (
              upper(BTRIM(COALESCE(operation_row.input_json->>'backend_runner_owned', operation_row.input_json->>'backendRunnerOwned', operation_row.config_json->>'backend_runner_owned', operation_row.config_json->>'backendRunnerOwned', operation_row.progress_json->>'backend_runner_owned', operation_row.progress_json->>'backendRunnerOwned', operation_row.progress_json->>'server_runnable', operation_row.progress_json->>'serverRunnable', 'false'))) IN ('TRUE', 'T', '1', 'YES', 'Y', 'ON')
              OR (
                operation_row.config_json ? 'frontend_completion_required'
                AND upper(BTRIM(COALESCE(operation_row.config_json->>'frontend_completion_required', ''))) IN ('FALSE', 'F', '0', 'NO', 'N', 'OFF')
              )
              OR (
                operation_row.config_json ? 'frontendCompletionRequired'
                AND upper(BTRIM(COALESCE(operation_row.config_json->>'frontendCompletionRequired', ''))) IN ('FALSE', 'F', '0', 'NO', 'N', 'OFF')
              )
            )
          )
        )
        AND (
          (
            upper(BTRIM(COALESCE(operation_row.status, ''))) = 'RUNNING'
            AND upper(BTRIM(COALESCE(operation_row.runner_state, ''))) IN ('RUNNABLE', 'RUNNING', 'IDLE')
          )
          OR (
            upper(BTRIM(COALESCE(operation_row.status, ''))) = 'WAITING'
            AND upper(BTRIM(COALESCE(operation_row.runner_state, ''))) = 'RUNNABLE'
          )
          OR (
            upper(BTRIM(COALESCE(operation_row.status, ''))) = 'WAITING'
            AND upper(BTRIM(COALESCE(operation_row.runner_state, ''))) = 'WAITING_CHILD'
            AND p_operation_id IS NOT NULL
            AND v_allow_backend_runner_owned IS TRUE
            AND COALESCE(operation_row.progress_json->>'child_operation_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
            AND EXISTS (
              SELECT 1
              FROM public.banking_pay_operations AS child_operation
              WHERE child_operation.id = (operation_row.progress_json->>'child_operation_id')::uuid
                AND child_operation.root_operation_id = operation_row.id
                AND upper(BTRIM(COALESCE(child_operation.status, ''))) IN ('COMPLETE', 'FAILED', 'CANCELLED', 'CANCELED', 'REVIEW_REQUIRED')
            )
          )
          OR (
            upper(BTRIM(COALESCE(operation_row.status, ''))) = 'WAITING_PROVIDER'
            AND v_allow_backend_runner_owned IS TRUE
            AND upper(BTRIM(COALESCE(operation_row.runner_state, ''))) IN ('WAITING_PROVIDER', 'RUNNABLE')
            AND (
              (
                upper(BTRIM(COALESCE(operation_row.operation_type, ''))) = 'PAYMENT_EXECUTE'
                AND (
                  upper(BTRIM(COALESCE(operation_row.phase, ''))) IN ('APPLY_RAIL_UPDATES', 'WAITING_PROVIDER', 'WAIT_PROVIDER', 'PROVIDER_WAIT', 'PROVIDER_WAITING', 'WAITING_PROVIDER_CONFIRMATION', 'POLL_PROVIDER', 'PROVIDER_POLL', 'APPLY_PROVIDER_UPDATES', 'CHECK_PROVIDER_OUTCOME')
                  OR upper(BTRIM(COALESCE(operation_row.progress_json->>'phase', operation_row.progress_json->>'operation_phase', operation_row.progress_json->>'operationPhase', operation_row.progress_json->>'next_phase', operation_row.progress_json->>'nextPhase', ''))) IN ('APPLY_RAIL_UPDATES', 'WAITING_PROVIDER', 'WAIT_PROVIDER', 'PROVIDER_WAIT', 'PROVIDER_WAITING', 'WAITING_PROVIDER_CONFIRMATION', 'POLL_PROVIDER', 'PROVIDER_POLL', 'APPLY_PROVIDER_UPDATES', 'CHECK_PROVIDER_OUTCOME')
                  OR upper(BTRIM(COALESCE(operation_row.resume_reason, operation_row.progress_json->>'resume_reason', operation_row.progress_json->>'resumeReason', ''))) IN ('AWAITING_PROVIDER_OUTCOME', 'WAITING_PROVIDER', 'WAIT_PROVIDER', 'PROVIDER_WAIT')
                )
              )
              OR (
                upper(BTRIM(COALESCE(operation_row.operation_type, ''))) = 'PAYMENT_SETTLEMENT'
                AND (
                  upper(BTRIM(COALESCE(operation_row.phase, ''))) = 'APPLY_SETTLEMENT_CHUNKS'
                  OR upper(BTRIM(COALESCE(operation_row.progress_json->>'phase', operation_row.progress_json->>'operation_phase', operation_row.progress_json->>'operationPhase', operation_row.progress_json->>'next_phase', operation_row.progress_json->>'nextPhase', ''))) = 'APPLY_SETTLEMENT_CHUNKS'
                )
              )
            )
          )
        )
        AND COALESCE(operation_row.requires_user_action, false) = false
        AND COALESCE(operation_row.run_after_utc, v_now) <= v_now
        AND (
          operation_row.lease_owner IS NULL
          OR operation_row.lease_expires_at_utc IS NULL
          OR operation_row.lease_expires_at_utc <= v_now
        )
        AND COALESCE(operation_row.attempt_count, 0) < COALESCE(operation_row.max_attempts, 10)
      ORDER BY COALESCE(operation_row.run_after_utc, operation_row.created_at_utc, v_now), operation_row.created_at_utc, operation_row.id
      LIMIT 1
      FOR UPDATE SKIP LOCKED
    )
    UPDATE public.banking_pay_operations AS operation_update
    SET status = CASE WHEN upper(BTRIM(COALESCE(operation_update.status, ''))) IN ('WAITING', 'WAITING_PROVIDER') THEN 'RUNNING' ELSE operation_update.status END,
        runner_state = 'RUNNING',
        lease_owner = v_lock_owner,
        lease_expires_at_utc = v_now + make_interval(secs => v_lock_seconds),
        heartbeat_at_utc = v_now,
        last_advanced_at_utc = v_now,
        started_at_utc = COALESCE(operation_update.started_at_utc, v_now),
        locked_by = v_lock_owner,
        lock_expires_at_utc = v_now + make_interval(secs => v_lock_seconds),
        progress_json = jsonb_strip_nulls(COALESCE(operation_update.progress_json, '{}'::jsonb) || jsonb_build_object(
          'last_claimed_at_utc', v_now::text,
          'last_claim_lease_owner', v_lock_owner,
          'last_claim_runner_actor_user_id', CASE WHEN p_actor_user_id IS NULL THEN NULL ELSE p_actor_user_id::text END,
          'last_claim_backend_runner_owned', v_allow_backend_runner_owned,
          'runner_state', 'RUNNING',
          'claim_count', (
            CASE
              WHEN COALESCE(operation_update.progress_json->>'claim_count', '') ~ '^[0-9]+$'
                THEN (operation_update.progress_json->>'claim_count')::integer
              ELSE 0
            END
          ) + 1,
          'backend_runner_claim', v_allow_backend_runner_owned,
          'runner_actor_user_id', CASE WHEN p_actor_user_id IS NULL THEN NULL ELSE p_actor_user_id::text END
        )),
        updated_at_utc = v_now
    FROM claimable
    WHERE operation_update.id = claimable.id
    RETURNING operation_update.* INTO v_operation;

    IF FOUND THEN
      RETURN QUERY
      SELECT
        true,
        NULL::text,
        v_operation.id,
        v_operation.operation_type,
        v_operation.status,
        v_operation.phase,
        v_operation.actor_user_id,
        v_operation.workbench_session_id,
        v_operation.pay_batch_id,
        v_operation.root_operation_id,
        v_operation.idempotency_key,
        v_operation.input_json,
        v_operation.config_json,
        jsonb_strip_nulls(COALESCE(v_operation.progress_json, '{}'::jsonb) || jsonb_build_object(
          'runner_state', v_operation.runner_state,
          'run_after_utc', CASE WHEN v_operation.run_after_utc IS NULL THEN NULL ELSE v_operation.run_after_utc::text END,
          'lease_owner', v_operation.lease_owner,
          'lease_expires_at_utc', CASE WHEN v_operation.lease_expires_at_utc IS NULL THEN NULL ELSE v_operation.lease_expires_at_utc::text END,
          'heartbeat_at_utc', CASE WHEN v_operation.heartbeat_at_utc IS NULL THEN NULL ELSE v_operation.heartbeat_at_utc::text END,
          'last_advanced_at_utc', CASE WHEN v_operation.last_advanced_at_utc IS NULL THEN NULL ELSE v_operation.last_advanced_at_utc::text END,
          'requires_user_action', COALESCE(v_operation.requires_user_action, false),
          'resume_reason', v_operation.resume_reason,
          'attempt_count', COALESCE(v_operation.attempt_count, 0),
          'max_attempts', v_operation.max_attempts
        )),
        v_operation.result_json,
        v_operation.error_json,
        v_operation.total_units,
        v_operation.completed_units,
        v_operation.failed_units,
        v_operation.current_chunk_index,
        v_operation.chunk_count,
        v_operation.lease_owner,
        v_operation.lease_expires_at_utc,
        v_operation.created_at_utc,
        v_operation.started_at_utc,
        v_operation.updated_at_utc,
        v_operation.completed_at_utc,
        v_operation.failed_at_utc;
      RETURN;
    END IF;

    IF p_operation_id IS NOT NULL THEN
      SELECT visible_operation.*
      INTO v_visible
      FROM public.banking_pay_operations AS visible_operation
      WHERE visible_operation.id = p_operation_id;

      IF FOUND THEN
        v_visible_operation_type := upper(BTRIM(COALESCE(v_visible.operation_type, '')));
        v_visible_backend_runner_claimable := v_allow_backend_runner_owned IS TRUE
          AND (
            COALESCE(array_length(v_operation_types, 1), 0) = 0
            OR v_visible_operation_type = ANY(v_operation_types)
          )
          AND (
            upper(BTRIM(COALESCE(v_visible.input_json->>'backend_runner_owned', v_visible.input_json->>'backendRunnerOwned', v_visible.config_json->>'backend_runner_owned', v_visible.config_json->>'backendRunnerOwned', v_visible.progress_json->>'backend_runner_owned', v_visible.progress_json->>'backendRunnerOwned', v_visible.progress_json->>'server_runnable', v_visible.progress_json->>'serverRunnable', 'false'))) IN ('TRUE', 'T', '1', 'YES', 'Y', 'ON')
            OR (
              v_visible.config_json ? 'frontend_completion_required'
              AND upper(BTRIM(COALESCE(v_visible.config_json->>'frontend_completion_required', ''))) IN ('FALSE', 'F', '0', 'NO', 'N', 'OFF')
            )
            OR (
              v_visible.config_json ? 'frontendCompletionRequired'
              AND upper(BTRIM(COALESCE(v_visible.config_json->>'frontendCompletionRequired', ''))) IN ('FALSE', 'F', '0', 'NO', 'N', 'OFF')
            )
          );
        v_visible_actor_authorised := p_actor_user_id IS NULL OR v_visible.actor_user_id IS NULL OR v_visible.actor_user_id = p_actor_user_id OR v_visible_backend_runner_claimable IS TRUE;

        IF v_visible_actor_authorised IS NOT TRUE THEN
          RETURN QUERY
          SELECT
            false,
            'ACTOR_MISMATCH'::text,
            p_operation_id,
            NULL::text,
            NULL::text,
            NULL::text,
            NULL::uuid,
            NULL::uuid,
            NULL::uuid,
            NULL::uuid,
            NULL::text,
            NULL::jsonb,
            NULL::jsonb,
            NULL::jsonb,
            NULL::jsonb,
            NULL::jsonb,
            NULL::integer,
            NULL::integer,
            NULL::integer,
            NULL::integer,
            NULL::integer,
            NULL::text,
            NULL::timestamptz,
            NULL::timestamptz,
            NULL::timestamptz,
            NULL::timestamptz,
            NULL::timestamptz,
            NULL::timestamptz;
          RETURN;
        END IF;

        v_not_claimed_reason := CASE
          WHEN COALESCE(array_length(v_operation_types, 1), 0) > 0 AND v_visible_operation_type <> ALL(v_operation_types) THEN 'OPERATION_TYPE_NOT_IN_SCOPE'
          WHEN upper(BTRIM(COALESCE(v_visible.status, ''))) IN ('WAITING_AUTHORISATION', 'REVIEW_REQUIRED', 'COMPLETE', 'FAILED', 'CANCELLED', 'CANCELED') THEN 'NOT_RUNNABLE_STATUS'
          WHEN COALESCE(v_visible.requires_user_action, false) THEN 'REQUIRES_USER_ACTION'
          WHEN COALESCE(v_visible.run_after_utc, v_now) > v_now THEN 'RUN_AFTER_NOT_DUE'
          WHEN v_visible.lease_owner IS NOT NULL AND v_visible.lease_expires_at_utc IS NOT NULL AND v_visible.lease_expires_at_utc > v_now THEN 'LEASE_ACTIVE'
          WHEN COALESCE(v_visible.attempt_count, 0) >= COALESCE(v_visible.max_attempts, 10) THEN 'MAX_ATTEMPTS_REACHED'
          WHEN upper(BTRIM(COALESCE(v_visible.status, ''))) = 'RUNNING'
           AND upper(BTRIM(COALESCE(v_visible.runner_state, ''))) NOT IN ('RUNNABLE', 'RUNNING', 'IDLE') THEN 'RUNNING_NOT_RUNNABLE'
          WHEN upper(BTRIM(COALESCE(v_visible.status, ''))) = 'WAITING'
           AND upper(BTRIM(COALESCE(v_visible.runner_state, ''))) = 'WAITING_CHILD' THEN 'WAITING_CHILD_NOT_TERMINAL'
          WHEN upper(BTRIM(COALESCE(v_visible.status, ''))) = 'WAITING'
           AND upper(BTRIM(COALESCE(v_visible.runner_state, ''))) <> 'RUNNABLE' THEN 'WAITING_NOT_RUNNABLE'
          WHEN upper(BTRIM(COALESCE(v_visible.status, ''))) = 'WAITING_PROVIDER'
           AND v_allow_backend_runner_owned IS NOT TRUE THEN 'WAITING_PROVIDER_BACKEND_RUNNER_REQUIRED'
          WHEN upper(BTRIM(COALESCE(v_visible.status, ''))) = 'WAITING_PROVIDER'
           AND upper(BTRIM(COALESCE(v_visible.operation_type, ''))) NOT IN ('PAYMENT_EXECUTE', 'PAYMENT_SETTLEMENT') THEN 'WAITING_PROVIDER_OPERATION_TYPE_NOT_CLAIMABLE'
          WHEN upper(BTRIM(COALESCE(v_visible.status, ''))) = 'WAITING_PROVIDER'
           AND upper(BTRIM(COALESCE(v_visible.runner_state, ''))) NOT IN ('WAITING_PROVIDER', 'RUNNABLE') THEN 'WAITING_PROVIDER_NOT_RUNNABLE'
          WHEN upper(BTRIM(COALESCE(v_visible.status, ''))) = 'WAITING_PROVIDER'
           AND NOT (
             (
               upper(BTRIM(COALESCE(v_visible.operation_type, ''))) = 'PAYMENT_EXECUTE'
               AND (
                 upper(BTRIM(COALESCE(v_visible.phase, ''))) IN ('APPLY_RAIL_UPDATES', 'WAITING_PROVIDER', 'WAIT_PROVIDER', 'PROVIDER_WAIT', 'PROVIDER_WAITING', 'WAITING_PROVIDER_CONFIRMATION', 'POLL_PROVIDER', 'PROVIDER_POLL', 'APPLY_PROVIDER_UPDATES', 'CHECK_PROVIDER_OUTCOME')
                 OR upper(BTRIM(COALESCE(v_visible.progress_json->>'phase', v_visible.progress_json->>'operation_phase', v_visible.progress_json->>'operationPhase', v_visible.progress_json->>'next_phase', v_visible.progress_json->>'nextPhase', ''))) IN ('APPLY_RAIL_UPDATES', 'WAITING_PROVIDER', 'WAIT_PROVIDER', 'PROVIDER_WAIT', 'PROVIDER_WAITING', 'WAITING_PROVIDER_CONFIRMATION', 'POLL_PROVIDER', 'PROVIDER_POLL', 'APPLY_PROVIDER_UPDATES', 'CHECK_PROVIDER_OUTCOME')
                 OR upper(BTRIM(COALESCE(v_visible.resume_reason, v_visible.progress_json->>'resume_reason', v_visible.progress_json->>'resumeReason', ''))) IN ('AWAITING_PROVIDER_OUTCOME', 'WAITING_PROVIDER', 'WAIT_PROVIDER', 'PROVIDER_WAIT')
               )
             )
             OR (
               upper(BTRIM(COALESCE(v_visible.operation_type, ''))) = 'PAYMENT_SETTLEMENT'
               AND (
                 upper(BTRIM(COALESCE(v_visible.phase, ''))) = 'APPLY_SETTLEMENT_CHUNKS'
                 OR upper(BTRIM(COALESCE(v_visible.progress_json->>'phase', v_visible.progress_json->>'operation_phase', v_visible.progress_json->>'operationPhase', v_visible.progress_json->>'next_phase', v_visible.progress_json->>'nextPhase', ''))) = 'APPLY_SETTLEMENT_CHUNKS'
               )
             )
           ) THEN 'WAITING_PROVIDER_NOT_RECHECK_PHASE'
          ELSE 'NOT_RUNNABLE'
        END;

        RETURN QUERY
        SELECT
          false,
          v_not_claimed_reason,
          v_visible.id,
          v_visible.operation_type,
          v_visible.status,
          v_visible.phase,
          v_visible.actor_user_id,
          v_visible.workbench_session_id,
          v_visible.pay_batch_id,
          v_visible.root_operation_id,
          v_visible.idempotency_key,
          v_visible.input_json,
          v_visible.config_json,
          jsonb_strip_nulls(COALESCE(v_visible.progress_json, '{}'::jsonb) || jsonb_build_object(
            'runner_state', v_visible.runner_state,
            'run_after_utc', CASE WHEN v_visible.run_after_utc IS NULL THEN NULL ELSE v_visible.run_after_utc::text END,
            'lease_owner', v_visible.lease_owner,
            'lease_expires_at_utc', CASE WHEN v_visible.lease_expires_at_utc IS NULL THEN NULL ELSE v_visible.lease_expires_at_utc::text END,
            'requires_user_action', COALESCE(v_visible.requires_user_action, false),
            'resume_reason', v_visible.resume_reason,
            'attempt_count', COALESCE(v_visible.attempt_count, 0),
            'max_attempts', v_visible.max_attempts,
            'backend_runner_claimable', v_visible_backend_runner_claimable
          )),
          v_visible.result_json,
          v_visible.error_json,
          v_visible.total_units,
          v_visible.completed_units,
          v_visible.failed_units,
          v_visible.current_chunk_index,
          v_visible.chunk_count,
          COALESCE(v_visible.lease_owner, v_visible.locked_by),
          COALESCE(v_visible.lease_expires_at_utc, v_visible.lock_expires_at_utc),
          v_visible.created_at_utc,
          v_visible.started_at_utc,
          v_visible.updated_at_utc,
          v_visible.completed_at_utc,
          v_visible.failed_at_utc;
        RETURN;
      END IF;
    END IF;

    RETURN QUERY
    SELECT
      false,
      CASE WHEN p_operation_id IS NULL THEN 'NO_RUNNABLE_OPERATION' ELSE 'NOT_FOUND_OR_NOT_AUTHORISED' END,
      p_operation_id,
      NULL::text,
      NULL::text,
      NULL::text,
      NULL::uuid,
      NULL::uuid,
      NULL::uuid,
      NULL::uuid,
      NULL::text,
      NULL::jsonb,
      NULL::jsonb,
      NULL::jsonb,
      NULL::jsonb,
      NULL::jsonb,
      NULL::integer,
      NULL::integer,
      NULL::integer,
      NULL::integer,
      NULL::integer,
      NULL::text,
      NULL::timestamptz,
      NULL::timestamptz,
      NULL::timestamptz,
      NULL::timestamptz,
      NULL::timestamptz,
      NULL::timestamptz;
END;
$function$;

-- banking_pay_operation_config_get(text,text,text)
CREATE OR REPLACE FUNCTION public.banking_pay_operation_config_get(p_operation_type text, p_phase text, p_chunk_type text)
 RETURNS TABLE(chunk_size integer, min_chunk_size integer, max_chunk_size integer, max_advance_ms integer, lock_seconds integer)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
    v_operation_type text;
    v_phase text;
    v_chunk_type text;
    v_default_chunk_size integer;
    v_default_min_chunk_size integer;
    v_default_max_chunk_size integer;
    v_default_max_advance_ms integer;
    v_default_lock_seconds integer;
begin
    v_operation_type := coalesce(nullif(btrim(p_operation_type), ''), 'ALL');
    v_phase := coalesce(nullif(btrim(p_phase), ''), 'ALL');
    v_chunk_type := coalesce(nullif(btrim(p_chunk_type), ''), 'CANDIDATE_SCOPE');

    if v_chunk_type in ('TRANSFER_SUBMIT', 'PAYEE_READINESS', 'FRESHNESS_VALIDATE') then
        v_default_chunk_size := 50;
        v_default_min_chunk_size := 1;
        v_default_max_chunk_size := 250;
    else
        v_default_chunk_size := 100;
        v_default_min_chunk_size := 1;
        v_default_max_chunk_size := 500;
    end if;

    v_default_max_advance_ms := 15000;
    v_default_lock_seconds := 60;

    return query
    with candidate_config as (
        select
            config_exact.default_chunk_size,
            config_exact.min_chunk_size,
            config_exact.max_chunk_size,
            config_exact.max_advance_ms,
            config_exact.lock_seconds,
            1 as priority_order
        from public.banking_pay_operation_config as config_exact
        where config_exact.enabled is true
          and config_exact.operation_type = v_operation_type
          and config_exact.phase = v_phase
          and config_exact.chunk_type = v_chunk_type

        union all

        select
            config_operation_default.default_chunk_size,
            config_operation_default.min_chunk_size,
            config_operation_default.max_chunk_size,
            config_operation_default.max_advance_ms,
            config_operation_default.lock_seconds,
            2 as priority_order
        from public.banking_pay_operation_config as config_operation_default
        where config_operation_default.enabled is true
          and config_operation_default.operation_type = v_operation_type
          and config_operation_default.phase = 'ALL'
          and config_operation_default.chunk_type = v_chunk_type

        union all

        select
            config_global_default.default_chunk_size,
            config_global_default.min_chunk_size,
            config_global_default.max_chunk_size,
            config_global_default.max_advance_ms,
            config_global_default.lock_seconds,
            3 as priority_order
        from public.banking_pay_operation_config as config_global_default
        where config_global_default.enabled is true
          and config_global_default.operation_type = 'ALL'
          and config_global_default.phase = 'ALL'
          and config_global_default.chunk_type = v_chunk_type
    )
    select
        selected_config.default_chunk_size,
        selected_config.min_chunk_size,
        selected_config.max_chunk_size,
        selected_config.max_advance_ms,
        selected_config.lock_seconds
    from candidate_config as selected_config
    order by selected_config.priority_order asc
    limit 1;

    if found then
        return;
    end if;

    return query
    select
        v_default_chunk_size,
        v_default_min_chunk_size,
        v_default_max_chunk_size,
        v_default_max_advance_ms,
        v_default_lock_seconds;
end;
$function$;

-- banking_pay_operation_continuation_recovery_due_v1(integer,integer,text[])
CREATE OR REPLACE FUNCTION public.banking_pay_operation_continuation_recovery_due_v1(p_limit integer DEFAULT 25, p_stale_after_seconds integer DEFAULT 90, p_operation_types text[] DEFAULT ARRAY['DRAFT_CREATE'::text, 'PAYMENT_EXECUTE'::text, 'PAYMENT_RETRY_BLOCKED_FUNDS'::text, 'PAYMENT_SETTLEMENT'::text, 'REMITTANCE_QUEUE'::text, 'PAYMENT_CORRECTION'::text])
 RETURNS jsonb
 LANGUAGE sql
 STABLE PARALLEL RESTRICTED SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'private', 'extensions', 'pg_temp'
 SET statement_timeout TO '5000ms'
AS $function$
WITH supplied_types AS (
  SELECT COALESCE(
    array_agg(DISTINCT upper(btrim(type_value)))
      FILTER (WHERE NULLIF(btrim(type_value), '') IS NOT NULL),
    ARRAY[]::text[]
  ) AS values
  FROM unnest(COALESCE(p_operation_types, ARRAY[]::text[])) AS supplied(type_value)
),
validated AS (
  SELECT
    LEAST(GREATEST(COALESCE(p_limit, 25), 1), 25) AS row_limit,
    LEAST(GREATEST(COALESCE(p_stale_after_seconds, 90), 90), 3600) AS caller_stale_seconds,
    supplied_types.values AS operation_types,
    clock_timestamp() AS checked_at_utc
  FROM supplied_types
),
eligible AS (
  SELECT
    operation_row.id AS operation_id,
    operation_row.operation_type,
    operation_row.pay_batch_id,
    operation_row.root_operation_id,
    operation_row.status,
    operation_row.phase,
    operation_row.run_after_utc,
    operation_row.requires_user_action,
    operation_row.runner_state,
    operation_row.resume_reason,
    operation_row.attempt_count,
    operation_row.max_attempts,
    operation_row.progress_json,
    validation.checked_at_utc,
    GREATEST(
      validation.caller_stale_seconds,
      COALESCE(operation_config.lock_seconds, NULLIF(operation_row.config_json->>'lock_seconds', '')::integer, 60)
        + CEIL(COALESCE(operation_config.max_advance_ms, NULLIF(operation_row.config_json->>'max_advance_ms', '')::integer, 7500) / 1000.0)::integer
        + 15
    ) AS effective_stale_seconds,
    COALESCE(
      NULLIF(operation_row.progress_json->>'continuation_witness_changed_at_utc', '')::timestamptz,
      operation_row.last_advanced_at_utc,
      operation_row.started_at_utc,
      operation_row.created_at_utc
    ) AS last_meaningful_activity_utc
  FROM public.banking_pay_operations AS operation_row
  CROSS JOIN validated AS validation
  LEFT JOIN LATERAL (
    SELECT config_row.lock_seconds, config_row.max_advance_ms
    FROM public.banking_pay_operation_config AS config_row
    WHERE upper(btrim(config_row.operation_type)) = upper(btrim(operation_row.operation_type))
      AND upper(btrim(config_row.phase)) IN (upper(btrim(operation_row.phase)), 'ALL')
    ORDER BY CASE WHEN upper(btrim(config_row.phase)) = upper(btrim(operation_row.phase)) THEN 0 ELSE 1 END,
             config_row.id
    LIMIT 1
  ) AS operation_config ON true
  WHERE COALESCE(array_length(validation.operation_types, 1), 0) > 0
    AND upper(btrim(operation_row.operation_type)) = ANY(validation.operation_types)
    AND (
      (
        upper(btrim(COALESCE(operation_row.status, ''))) = 'RUNNING'
        AND upper(btrim(COALESCE(operation_row.runner_state, ''))) IN ('RUNNABLE', 'RUNNING', 'IDLE')
      )
      OR (
        upper(btrim(COALESCE(operation_row.status, ''))) = 'WAITING'
        AND upper(btrim(COALESCE(operation_row.runner_state, ''))) = 'RUNNABLE'
      )
      OR (
        upper(btrim(COALESCE(operation_row.status, ''))) = 'WAITING'
        AND upper(btrim(COALESCE(operation_row.runner_state, ''))) = 'WAITING_CHILD'
        AND COALESCE(operation_row.progress_json->>'child_operation_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
        AND EXISTS (
          SELECT 1
          FROM public.banking_pay_operations AS child_operation
          WHERE child_operation.id = (operation_row.progress_json->>'child_operation_id')::uuid
            AND child_operation.root_operation_id = operation_row.id
            AND upper(btrim(COALESCE(child_operation.status, ''))) IN ('COMPLETE', 'FAILED', 'CANCELLED', 'CANCELED', 'REVIEW_REQUIRED')
        )
      )
      OR (
        upper(btrim(COALESCE(operation_row.status, ''))) = 'WAITING_PROVIDER'
        AND upper(btrim(COALESCE(operation_row.runner_state, ''))) IN ('WAITING_PROVIDER', 'RUNNABLE')
        AND (
          (
            upper(btrim(COALESCE(operation_row.operation_type, ''))) = 'PAYMENT_EXECUTE'
            AND (
              upper(btrim(COALESCE(operation_row.phase, ''))) IN ('APPLY_RAIL_UPDATES', 'WAITING_PROVIDER', 'WAIT_PROVIDER', 'PROVIDER_WAIT', 'PROVIDER_WAITING', 'WAITING_PROVIDER_CONFIRMATION', 'POLL_PROVIDER', 'PROVIDER_POLL', 'APPLY_PROVIDER_UPDATES', 'CHECK_PROVIDER_OUTCOME')
              OR upper(btrim(COALESCE(operation_row.progress_json->>'phase', operation_row.progress_json->>'operation_phase', operation_row.progress_json->>'operationPhase', operation_row.progress_json->>'next_phase', operation_row.progress_json->>'nextPhase', ''))) IN ('APPLY_RAIL_UPDATES', 'WAITING_PROVIDER', 'WAIT_PROVIDER', 'PROVIDER_WAIT', 'PROVIDER_WAITING', 'WAITING_PROVIDER_CONFIRMATION', 'POLL_PROVIDER', 'PROVIDER_POLL', 'APPLY_PROVIDER_UPDATES', 'CHECK_PROVIDER_OUTCOME')
              OR upper(btrim(COALESCE(operation_row.resume_reason, operation_row.progress_json->>'resume_reason', operation_row.progress_json->>'resumeReason', ''))) IN ('AWAITING_PROVIDER_OUTCOME', 'WAITING_PROVIDER', 'WAIT_PROVIDER', 'PROVIDER_WAIT')
            )
          )
          OR (
            upper(btrim(COALESCE(operation_row.operation_type, ''))) = 'PAYMENT_SETTLEMENT'
            AND (
              upper(btrim(COALESCE(operation_row.phase, ''))) = 'APPLY_SETTLEMENT_CHUNKS'
              OR upper(btrim(COALESCE(operation_row.progress_json->>'phase', operation_row.progress_json->>'operation_phase', operation_row.progress_json->>'operationPhase', operation_row.progress_json->>'next_phase', operation_row.progress_json->>'nextPhase', ''))) = 'APPLY_SETTLEMENT_CHUNKS'
            )
          )
        )
      )
    )
    AND COALESCE(operation_row.requires_user_action, false) IS FALSE
    AND COALESCE(operation_row.attempt_count, 0) < COALESCE(operation_row.max_attempts, 10)
    AND COALESCE(NULLIF(operation_row.progress_json->>'continuation_no_progress_count', '')::integer, 0) < 5
    AND (operation_row.lease_owner IS NULL OR operation_row.lease_expires_at_utc IS NULL OR operation_row.lease_expires_at_utc <= validation.checked_at_utc)
    AND (operation_row.run_after_utc IS NULL OR operation_row.run_after_utc <= validation.checked_at_utc)
    AND NOT (
      upper(btrim(operation_row.status)) = 'WAITING_PROVIDER'
      AND operation_row.run_after_utc IS NULL
    )
),
due AS (
  SELECT eligible.*
  FROM eligible
  CROSS JOIN validated AS validation
  WHERE eligible.last_meaningful_activity_utc
        <= eligible.checked_at_utc - make_interval(secs => eligible.effective_stale_seconds)
  ORDER BY eligible.last_meaningful_activity_utc, eligible.operation_id
  LIMIT (SELECT row_limit FROM validated)
),
descriptors AS (
  SELECT COALESCE(
    jsonb_agg(
      jsonb_strip_nulls(jsonb_build_object(
        'required', true,
        'operation_id', due.operation_id,
        'operation_type', due.operation_type,
        'pay_batch_id', due.pay_batch_id,
        'root_operation_id', due.root_operation_id,
        'phase', due.phase,
        'run_after_utc', due.run_after_utc,
        'reason', 'STRANDED_OPERATION_RECOVERY',
        'successor_relation', 'SELF',
        'requires_user_action', false,
        'terminal', false,
        'effective_stale_seconds', due.effective_stale_seconds,
        'last_meaningful_activity_utc', due.last_meaningful_activity_utc
      ))
      ORDER BY due.last_meaningful_activity_utc, due.operation_id
    ),
    '[]'::jsonb
  ) AS rows
  FROM due
)
SELECT jsonb_build_object(
  'ok', true,
  'checked_at_utc', (SELECT checked_at_utc FROM validated),
  'count', jsonb_array_length(descriptors.rows),
  'continuations', descriptors.rows,
  'code', 'BANKING_PAY_CONTINUATION_RECOVERY_DUE_OK'
)
FROM descriptors;
$function$;

-- banking_pay_operation_find_active_draft_create(uuid,uuid,boolean,integer)
CREATE OR REPLACE FUNCTION public.banking_pay_operation_find_active_draft_create(p_actor_user_id uuid, p_workbench_session_id uuid, p_include_recent_terminal boolean DEFAULT false, p_recent_terminal_minutes integer DEFAULT 60)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
    v_now timestamptz := now();
    v_recent_terminal_minutes integer := LEAST(GREATEST(COALESCE(p_recent_terminal_minutes, 60), 0), 10080);
    v_recent_terminal_after timestamptz := NULL::timestamptz;
    v_operation public.banking_pay_operations%ROWTYPE;
    v_progress jsonb := '{}'::jsonb;
    v_result jsonb := '{}'::jsonb;
    v_error jsonb := '{}'::jsonb;
    v_public_payload jsonb := '{}'::jsonb;
    v_terminal boolean := false;
    v_active boolean := false;
    v_heartbeat_age_seconds integer := NULL::integer;
    v_batch_ids jsonb := '[]'::jsonb;
    v_primary_pay_batch_id text := NULL::text;
    v_backend_runner_owned boolean := false;
    v_frontend_completion_required boolean := false;
    v_session_id_text text := NULL::text;
BEGIN
    PERFORM public.banking_pay_hot_path_budget_apply('PROGRESS');

    IF p_actor_user_id IS NULL THEN
        RETURN jsonb_build_object(
            'ok', false,
            'code', 'ACTOR_USER_ID_REQUIRED',
            'message', 'actor_user_id is required'
        );
    END IF;

    IF p_workbench_session_id IS NULL THEN
        RETURN jsonb_build_object(
            'ok', false,
            'code', 'WORKBENCH_SESSION_ID_REQUIRED',
            'message', 'workbench_session_id is required'
        );
    END IF;

    v_session_id_text := p_workbench_session_id::text;

    IF COALESCE(p_include_recent_terminal, false) IS TRUE AND v_recent_terminal_minutes > 0 THEN
        v_recent_terminal_after := v_now - make_interval(mins => v_recent_terminal_minutes);
    END IF;

    SELECT operation_row.*
    INTO v_operation
    FROM public.banking_pay_operations AS operation_row
    WHERE upper(BTRIM(COALESCE(operation_row.operation_type, ''))) = 'DRAFT_CREATE'
      AND (
          operation_row.workbench_session_id = p_workbench_session_id
          OR COALESCE(operation_row.input_json->>'workbench_session_id', '') = v_session_id_text
          OR COALESCE(operation_row.input_json->>'workbenchSessionId', '') = v_session_id_text
          OR COALESCE(operation_row.input_json->>'session_id', '') = v_session_id_text
          OR COALESCE(operation_row.input_json->>'source_session_id', '') = v_session_id_text
          OR COALESCE(operation_row.input_json->>'source_workbench_session_id', '') = v_session_id_text
          OR COALESCE(operation_row.progress_json->>'workbench_session_id', '') = v_session_id_text
          OR COALESCE(operation_row.progress_json->>'workbenchSessionId', '') = v_session_id_text
          OR COALESCE(operation_row.progress_json->>'session_id', '') = v_session_id_text
          OR COALESCE(operation_row.progress_json->>'source_session_id', '') = v_session_id_text
          OR COALESCE(operation_row.progress_json->>'source_workbench_session_id', '') = v_session_id_text
          OR COALESCE(operation_row.result_json->>'workbench_session_id', '') = v_session_id_text
          OR COALESCE(operation_row.result_json->>'workbenchSessionId', '') = v_session_id_text
          OR COALESCE(operation_row.result_json->>'session_id', '') = v_session_id_text
          OR COALESCE(operation_row.result_json->>'source_session_id', '') = v_session_id_text
          OR COALESCE(operation_row.result_json->>'source_workbench_session_id', '') = v_session_id_text
      )
      AND (
          upper(BTRIM(COALESCE(operation_row.status, ''))) IN ('QUEUED', 'RUNNING', 'WAITING', 'WAITING_AUTHORISATION', 'WAITING_AUTHORIZATION', 'WAITING_PROVIDER', 'CONTINUING', 'WAITING_RETRY')
          OR (
              v_recent_terminal_after IS NOT NULL
              AND upper(BTRIM(COALESCE(operation_row.status, ''))) IN ('COMPLETE', 'COMPLETED', 'FAILED', 'CANCELLED', 'CANCELED', 'ERROR', 'REVIEW_REQUIRED')
              AND COALESCE(operation_row.completed_at_utc, operation_row.failed_at_utc, operation_row.updated_at_utc, operation_row.created_at_utc) >= v_recent_terminal_after
          )
      )
    ORDER BY
      CASE
        WHEN upper(BTRIM(COALESCE(operation_row.status, ''))) IN ('QUEUED', 'RUNNING', 'WAITING', 'WAITING_AUTHORISATION', 'WAITING_AUTHORIZATION', 'WAITING_PROVIDER', 'CONTINUING', 'WAITING_RETRY') THEN 0
        ELSE 1
      END ASC,
      CASE upper(BTRIM(COALESCE(operation_row.status, '')))
        WHEN 'RUNNING' THEN 0
        WHEN 'WAITING' THEN 1
        WHEN 'QUEUED' THEN 2
        WHEN 'WAITING_AUTHORISATION' THEN 3
        WHEN 'WAITING_AUTHORIZATION' THEN 3
        WHEN 'WAITING_PROVIDER' THEN 4
        WHEN 'CONTINUING' THEN 5
        WHEN 'WAITING_RETRY' THEN 6
        ELSE 8
      END ASC,
      operation_row.updated_at_utc DESC NULLS LAST,
      operation_row.created_at_utc DESC NULLS LAST,
      operation_row.id DESC
    LIMIT 1;

    IF NOT FOUND THEN
        RETURN jsonb_build_object(
            'ok', true,
            'found', false,
            'operation', NULL::jsonb,
            'operation_id', NULL::text,
            'workbench_session_id', v_session_id_text,
            'operation_type', 'DRAFT_CREATE'
        );
    END IF;

    v_progress := COALESCE(v_operation.progress_json, '{}'::jsonb);
    v_result := COALESCE(v_operation.result_json, '{}'::jsonb);
    v_error := COALESCE(v_operation.error_json, '{}'::jsonb);
    v_terminal := upper(BTRIM(COALESCE(v_operation.status, ''))) IN ('COMPLETE', 'COMPLETED', 'FAILED', 'CANCELLED', 'CANCELED', 'ERROR', 'REVIEW_REQUIRED');
    v_active := upper(BTRIM(COALESCE(v_operation.status, ''))) IN ('QUEUED', 'RUNNING', 'WAITING', 'WAITING_AUTHORISATION', 'WAITING_AUTHORIZATION', 'WAITING_PROVIDER', 'CONTINUING', 'WAITING_RETRY') AND v_terminal IS NOT TRUE;
    v_heartbeat_age_seconds := CASE
        WHEN v_operation.heartbeat_at_utc IS NULL THEN NULL::integer
        ELSE GREATEST(0, EXTRACT(EPOCH FROM (v_now - v_operation.heartbeat_at_utc))::integer)
    END;

    v_backend_runner_owned := upper(BTRIM(COALESCE(
        v_operation.input_json->>'backend_runner_owned',
        v_operation.input_json->>'backendRunnerOwned',
        v_operation.config_json->>'backend_runner_owned',
        v_operation.config_json->>'backendRunnerOwned',
        v_progress->>'backend_runner_owned',
        v_progress->>'backendRunnerOwned',
        'false'
    ))) IN ('TRUE', 'T', '1', 'YES', 'Y', 'ON');

    v_frontend_completion_required := upper(BTRIM(COALESCE(
        v_operation.input_json->>'frontend_completion_required',
        v_operation.input_json->>'frontendCompletionRequired',
        v_operation.config_json->>'frontend_completion_required',
        v_operation.config_json->>'frontendCompletionRequired',
        v_progress->>'frontend_completion_required',
        v_progress->>'frontendCompletionRequired',
        'false'
    ))) IN ('TRUE', 'T', '1', 'YES', 'Y', 'ON');

    SELECT COALESCE(jsonb_agg(batch_id_dedup.batch_id_text ORDER BY batch_id_dedup.batch_id_text), '[]'::jsonb)
    INTO v_batch_ids
    FROM (
        SELECT DISTINCT NULLIF(BTRIM(batch_id_source.batch_id_text), '') AS batch_id_text
        FROM (
            SELECT CASE WHEN v_operation.pay_batch_id IS NULL THEN NULL::text ELSE v_operation.pay_batch_id::text END AS batch_id_text
            UNION ALL SELECT v_progress->>'pay_batch_id'
            UNION ALL SELECT v_progress->>'payBatchId'
            UNION ALL SELECT v_progress->>'primary_pay_batch_id'
            UNION ALL SELECT v_progress->>'primaryPayBatchId'
            UNION ALL SELECT v_result->>'pay_batch_id'
            UNION ALL SELECT v_result->>'payBatchId'
            UNION ALL SELECT v_result->>'primary_pay_batch_id'
            UNION ALL SELECT v_result->>'primaryPayBatchId'
            UNION ALL SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_progress->'pay_batch_ids') = 'array' THEN v_progress->'pay_batch_ids' ELSE '[]'::jsonb END)
            UNION ALL SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_progress->'payBatchIds') = 'array' THEN v_progress->'payBatchIds' ELSE '[]'::jsonb END)
            UNION ALL SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_progress->'created_pay_batch_ids') = 'array' THEN v_progress->'created_pay_batch_ids' ELSE '[]'::jsonb END)
            UNION ALL SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_progress->'createdPayBatchIds') = 'array' THEN v_progress->'createdPayBatchIds' ELSE '[]'::jsonb END)
            UNION ALL SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_result->'pay_batch_ids') = 'array' THEN v_result->'pay_batch_ids' ELSE '[]'::jsonb END)
            UNION ALL SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_result->'payBatchIds') = 'array' THEN v_result->'payBatchIds' ELSE '[]'::jsonb END)
            UNION ALL SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_result->'created_pay_batch_ids') = 'array' THEN v_result->'created_pay_batch_ids' ELSE '[]'::jsonb END)
            UNION ALL SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_result->'createdPayBatchIds') = 'array' THEN v_result->'createdPayBatchIds' ELSE '[]'::jsonb END)
        ) AS batch_id_source
    ) AS batch_id_dedup
    WHERE batch_id_dedup.batch_id_text IS NOT NULL;

    v_primary_pay_batch_id := COALESCE(
        CASE WHEN v_operation.pay_batch_id IS NULL THEN NULL::text ELSE v_operation.pay_batch_id::text END,
        NULLIF(BTRIM(v_progress->>'primary_pay_batch_id'), ''),
        NULLIF(BTRIM(v_progress->>'primaryPayBatchId'), ''),
        NULLIF(BTRIM(v_progress->>'pay_batch_id'), ''),
        NULLIF(BTRIM(v_progress->>'payBatchId'), ''),
        NULLIF(BTRIM(v_result->>'primary_pay_batch_id'), ''),
        NULLIF(BTRIM(v_result->>'primaryPayBatchId'), ''),
        NULLIF(BTRIM(v_result->>'pay_batch_id'), ''),
        NULLIF(BTRIM(v_result->>'payBatchId'), '')
    );

    v_public_payload := COALESCE(public.banking_pay_operation_get(v_operation.id, NULL::uuid, 'PROGRESS_LIGHT'), '{}'::jsonb);

    RETURN jsonb_strip_nulls(
        v_public_payload
        || jsonb_build_object(
            'ok', true,
            'found', true,
            'active_draft_create_operation', v_active,
            'recent_terminal_draft_create_operation', (v_terminal IS TRUE AND v_active IS NOT TRUE),
            'operation_id', v_operation.id::text,
            'id', v_operation.id::text,
            'operation_type', v_operation.operation_type,
            'status', v_operation.status,
            'phase', v_operation.phase,
            'runner_state', v_operation.runner_state,
            'actor_user_id', v_operation.actor_user_id::text,
            'workbench_session_id', COALESCE(CASE WHEN v_operation.workbench_session_id IS NULL THEN NULL::text ELSE v_operation.workbench_session_id::text END, v_session_id_text),
            'pay_batch_id', v_primary_pay_batch_id,
            'primary_pay_batch_id', v_primary_pay_batch_id,
            'root_operation_id', CASE WHEN v_operation.root_operation_id IS NULL THEN NULL ELSE v_operation.root_operation_id::text END,
            'idempotency_key', v_operation.idempotency_key,
            'backend_runner_owned', v_backend_runner_owned,
            'frontend_completion_required', v_frontend_completion_required,
            'terminal', v_terminal,
            'server_running', (v_operation.lease_owner IS NOT NULL AND v_operation.lease_expires_at_utc IS NOT NULL AND v_operation.lease_expires_at_utc > v_now),
            'heartbeat_age_seconds', v_heartbeat_age_seconds,
            'pay_batch_ids', v_batch_ids,
            'created_pay_batch_ids', v_batch_ids
        )
        || jsonb_build_object(
            'result_summary', jsonb_strip_nulls(jsonb_build_object(
                'pay_batch_id', v_primary_pay_batch_id,
                'primary_pay_batch_id', v_primary_pay_batch_id,
                'pay_batch_ids', v_batch_ids,
                'created_pay_batch_ids', v_batch_ids,
                'created_batch_count', CASE WHEN COALESCE(v_result->>'created_batch_count', '') ~ '^[0-9]+$' THEN (v_result->>'created_batch_count')::integer ELSE jsonb_array_length(v_batch_ids) END,
                'workbench_session_id', COALESCE(CASE WHEN v_operation.workbench_session_id IS NULL THEN NULL::text ELSE v_operation.workbench_session_id::text END, v_session_id_text),
                'source_session_discarded', v_result->>'source_session_discarded',
                'last_message', COALESCE(v_result->>'last_message', v_progress->>'last_message')
            )),
            'small_error_summary', CASE
                WHEN v_operation.error_json IS NULL THEN NULL::jsonb
                WHEN jsonb_typeof(v_error) = 'object' THEN jsonb_strip_nulls(jsonb_build_object(
                    'code', COALESCE(NULLIF(BTRIM(v_error->>'code'), ''), NULLIF(BTRIM(v_error->>'error_code'), '')),
                    'message', LEFT(COALESCE(NULLIF(BTRIM(v_error->>'message'), ''), NULLIF(BTRIM(v_error->>'error'), ''), v_error::text), 1000)
                ))
                ELSE jsonb_build_object('message', LEFT(v_error::text, 1000))
            END
        )
    );
END;
$function$;

-- banking_pay_operation_find_active(text,uuid,uuid,uuid)
CREATE OR REPLACE FUNCTION public.banking_pay_operation_find_active(p_operation_type text DEFAULT NULL::text, p_workbench_session_id uuid DEFAULT NULL::uuid, p_pay_batch_id uuid DEFAULT NULL::uuid, p_actor_user_id uuid DEFAULT NULL::uuid)
 RETURNS TABLE(operation_id uuid, operation_type text, status text, phase text, actor_user_id uuid, workbench_session_id uuid, pay_batch_id uuid, root_operation_id uuid, idempotency_key text, progress_json jsonb, total_units integer, completed_units integer, failed_units integer, current_chunk_index integer, chunk_count integer, locked_by text, lock_expires_at_utc timestamp with time zone, created_at_utc timestamp with time zone, started_at_utc timestamp with time zone, updated_at_utc timestamp with time zone)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
    v_operation_type text := upper(NULLIF(BTRIM(COALESCE(p_operation_type, '')), ''));
BEGIN
    IF p_workbench_session_id IS NULL AND p_pay_batch_id IS NULL THEN
        RAISE EXCEPTION 'BANKING_PAY_OPERATION_FIND_ACTIVE_SCOPE_REQUIRED'
          USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_OPERATION_FIND_ACTIVE_SCOPE_REQUIRED')::text;
    END IF;

    IF v_operation_type IS NOT NULL
       AND v_operation_type NOT IN (
           'DRAFT_CREATE',
           'PAYMENT_EXECUTE',
           'PAYMENT_RETRY_BLOCKED_FUNDS',
           'PAYMENT_SETTLEMENT',
           'REMITTANCE_QUEUE',
           'PREVIEW_REFRESH'
       ) THEN
        RAISE EXCEPTION 'BANKING_PAY_OPERATION_FIND_ACTIVE_TYPE_UNSUPPORTED'
          USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_OPERATION_FIND_ACTIVE_TYPE_UNSUPPORTED', 'operation_type', v_operation_type)::text;
    END IF;

    RETURN QUERY
    SELECT
        operation_row.id,
        operation_row.operation_type,
        operation_row.status,
        operation_row.phase,
        operation_row.actor_user_id,
        operation_row.workbench_session_id,
        operation_row.pay_batch_id,
        operation_row.root_operation_id,
        operation_row.idempotency_key,
        jsonb_strip_nulls(COALESCE(operation_row.progress_json, '{}'::jsonb) || jsonb_build_object(
            'runner_state', operation_row.runner_state,
            'run_after_utc', CASE WHEN operation_row.run_after_utc IS NULL THEN NULL ELSE operation_row.run_after_utc::text END,
            'requires_user_action', COALESCE(operation_row.requires_user_action, false),
            'resume_reason', operation_row.resume_reason,
            'heartbeat_at_utc', CASE WHEN operation_row.heartbeat_at_utc IS NULL THEN NULL ELSE operation_row.heartbeat_at_utc::text END,
            'last_advanced_at_utc', CASE WHEN operation_row.last_advanced_at_utc IS NULL THEN NULL ELSE operation_row.last_advanced_at_utc::text END,
            'attempt_count', COALESCE(operation_row.attempt_count, 0),
            'max_attempts', operation_row.max_attempts
        )),
        operation_row.total_units,
        operation_row.completed_units,
        operation_row.failed_units,
        operation_row.current_chunk_index,
        operation_row.chunk_count,
        COALESCE(operation_row.lease_owner, operation_row.locked_by),
        COALESCE(operation_row.lease_expires_at_utc, operation_row.lock_expires_at_utc),
        operation_row.created_at_utc,
        operation_row.started_at_utc,
        operation_row.updated_at_utc
    FROM public.banking_pay_operations AS operation_row
    WHERE upper(BTRIM(COALESCE(operation_row.status, ''))) IN (
            'QUEUED',
            'RUNNING',
            'WAITING',
            'RUNNABLE',
            'CONTINUING',
            'WAITING_RETRY',
            'WAITING_AUTHORISATION',
            'WAITING_AUTHORIZATION',
            'AWAITING_AUTHORISATION',
            'AWAITING_AUTHORIZATION',
            'WAITING_PROVIDER',
            'WAITING_FOR_PROVIDER',
            'AWAITING_PROVIDER',
            'WAITING_USER',
            'WAITING_USER_REVIEW',
            'REVIEW_REQUIRED'
          )
      AND (v_operation_type IS NULL OR upper(BTRIM(COALESCE(operation_row.operation_type, ''))) = v_operation_type)
      AND NOT (
        v_operation_type = 'PAYMENT_EXECUTE'
        AND upper(BTRIM(COALESCE(operation_row.operation_type, ''))) = 'PAYMENT_EXECUTE'
        AND (
          lower(BTRIM(COALESCE(operation_row.input_json->>'prepare_bank_csv_export_only', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
          OR upper(BTRIM(COALESCE(operation_row.input_json->>'execution_mode', ''))) = 'BANK_CSV_EXPORT_PREPARE'
          OR BTRIM(COALESCE(operation_row.input_json->>'source', '')) = 'handleBankingPayBatchExportCsv'
          OR lower(COALESCE(operation_row.idempotency_key, '')) LIKE 'bank-csv-export-prepare:%'
        )
      )
      AND (p_workbench_session_id IS NULL OR operation_row.workbench_session_id = p_workbench_session_id)
      AND (p_pay_batch_id IS NULL OR operation_row.pay_batch_id = p_pay_batch_id)
      AND (
          p_actor_user_id IS NULL
          OR operation_row.actor_user_id IS NULL
          OR operation_row.actor_user_id = p_actor_user_id
      )
    ORDER BY
      CASE upper(BTRIM(COALESCE(operation_row.status, '')))
        WHEN 'RUNNING' THEN 0
        WHEN 'WAITING' THEN 1
        WHEN 'RUNNABLE' THEN 2
        WHEN 'QUEUED' THEN 3
        WHEN 'CONTINUING' THEN 4
        WHEN 'WAITING_RETRY' THEN 5
        WHEN 'WAITING_AUTHORISATION' THEN 6
        WHEN 'WAITING_AUTHORIZATION' THEN 6
        WHEN 'AWAITING_AUTHORISATION' THEN 6
        WHEN 'AWAITING_AUTHORIZATION' THEN 6
        WHEN 'WAITING_PROVIDER' THEN 7
        WHEN 'WAITING_FOR_PROVIDER' THEN 7
        WHEN 'AWAITING_PROVIDER' THEN 7
        WHEN 'WAITING_USER' THEN 8
        WHEN 'WAITING_USER_REVIEW' THEN 8
        WHEN 'REVIEW_REQUIRED' THEN 9
        ELSE 10
      END,
      operation_row.updated_at_utc DESC NULLS LAST,
      operation_row.created_at_utc DESC NULLS LAST,
      operation_row.id DESC
    LIMIT 1;
END;
$function$;

-- banking_pay_operation_finish_chunk(uuid,text,integer,integer,jsonb,jsonb)
CREATE OR REPLACE FUNCTION public.banking_pay_operation_finish_chunk(p_chunk_id uuid, p_status text, p_completed_count integer DEFAULT NULL::integer, p_failed_count integer DEFAULT NULL::integer, p_result_json jsonb DEFAULT NULL::jsonb, p_error_json jsonb DEFAULT NULL::jsonb)
 RETURNS TABLE(finished boolean, not_finished_reason text, chunk_id uuid, operation_id uuid, phase text, chunk_type text, sequence_no integer, status text, payload_json jsonb, result_json jsonb, error_json jsonb, unit_count integer, completed_count integer, failed_count integer, locked_by text, lock_expires_at_utc timestamp with time zone, created_at_utc timestamp with time zone, started_at_utc timestamp with time zone, completed_at_utc timestamp with time zone, updated_at_utc timestamp with time zone)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
    v_now timestamptz := now();
    v_chunk public.banking_pay_operation_chunks%ROWTYPE;
    v_status text := upper(NULLIF(BTRIM(COALESCE(p_status, '')), ''));
    v_completed_count integer := 0;
    v_failed_count integer := 0;
    v_result_json jsonb := p_result_json;
    v_error_json jsonb := p_error_json;
    v_result_hash text := NULL::text;
    v_error_summary jsonb := NULL::jsonb;
BEGIN
    PERFORM set_config('lock_timeout', '3s', true);

    IF p_chunk_id IS NULL THEN
        RAISE EXCEPTION 'BANKING_PAY_OPERATION_FINISH_CHUNK_ID_REQUIRED'
          USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_OPERATION_FINISH_CHUNK_ID_REQUIRED')::text;
    END IF;

    IF v_status IS NULL OR v_status NOT IN ('PENDING', 'COMPLETE', 'FAILED', 'SKIPPED') THEN
        RAISE EXCEPTION 'BANKING_PAY_OPERATION_FINISH_CHUNK_STATUS_INVALID'
          USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_OPERATION_FINISH_CHUNK_STATUS_INVALID', 'chunk_id', p_chunk_id::text, 'status', p_status)::text;
    END IF;

    IF v_result_json IS NOT NULL AND jsonb_typeof(v_result_json) <> 'object' THEN
        RAISE EXCEPTION 'BANKING_PAY_OPERATION_FINISH_CHUNK_RESULT_MUST_BE_OBJECT'
          USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_OPERATION_FINISH_CHUNK_RESULT_MUST_BE_OBJECT', 'chunk_id', p_chunk_id::text)::text;
    END IF;

    IF v_error_json IS NOT NULL AND jsonb_typeof(v_error_json) <> 'object' THEN
        RAISE EXCEPTION 'BANKING_PAY_OPERATION_FINISH_CHUNK_ERROR_MUST_BE_OBJECT'
          USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_OPERATION_FINISH_CHUNK_ERROR_MUST_BE_OBJECT', 'chunk_id', p_chunk_id::text)::text;
    END IF;

    SELECT chunk_row.*
    INTO v_chunk
    FROM public.banking_pay_operation_chunks AS chunk_row
    WHERE chunk_row.id = p_chunk_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RETURN QUERY SELECT false, 'NOT_FOUND'::text, p_chunk_id, NULL::uuid, NULL::text, NULL::text, NULL::integer, NULL::text, NULL::jsonb, NULL::jsonb, NULL::jsonb, NULL::integer, NULL::integer, NULL::integer, NULL::text, NULL::timestamptz, NULL::timestamptz, NULL::timestamptz, NULL::timestamptz, NULL::timestamptz;
        RETURN;
    END IF;

    IF upper(BTRIM(COALESCE(v_chunk.status, ''))) IN ('COMPLETE', 'FAILED', 'SKIPPED') THEN
        RETURN QUERY SELECT false, 'ALREADY_TERMINAL'::text, v_chunk.id, v_chunk.operation_id, v_chunk.phase, v_chunk.chunk_type, v_chunk.sequence_no, v_chunk.status, v_chunk.payload_json, v_chunk.result_json, v_chunk.error_json, v_chunk.unit_count, v_chunk.completed_count, v_chunk.failed_count, v_chunk.locked_by, v_chunk.lock_expires_at_utc, v_chunk.created_at_utc, v_chunk.started_at_utc, v_chunk.completed_at_utc, v_chunk.updated_at_utc;
        RETURN;
    END IF;

    IF v_status = 'PENDING' THEN
        v_completed_count := COALESCE(p_completed_count, v_chunk.completed_count, 0);
        v_failed_count := COALESCE(p_failed_count, v_chunk.failed_count, 0);
    ELSIF v_status = 'COMPLETE' THEN
        v_completed_count := COALESCE(p_completed_count, v_chunk.unit_count, 1);
        v_failed_count := COALESCE(p_failed_count, 0);
    ELSIF v_status = 'FAILED' THEN
        v_completed_count := COALESCE(p_completed_count, 0);
        v_failed_count := COALESCE(p_failed_count, v_chunk.unit_count, 1);
    ELSE
        v_completed_count := COALESCE(p_completed_count, v_chunk.unit_count, 1);
        v_failed_count := COALESCE(p_failed_count, 0);
    END IF;

    v_completed_count := LEAST(GREATEST(v_completed_count, 0), GREATEST(COALESCE(v_chunk.unit_count, 0), 0));
    v_failed_count := LEAST(GREATEST(v_failed_count, 0), GREATEST(COALESCE(v_chunk.unit_count, 0) - v_completed_count, 0));
    v_result_hash := CASE WHEN v_result_json IS NULL THEN NULL ELSE md5(v_result_json::text) END;
    v_error_summary := CASE WHEN v_error_json IS NULL THEN NULL ELSE jsonb_strip_nulls(jsonb_build_object(
      'code', COALESCE(NULLIF(BTRIM(v_error_json->>'code'), ''), NULLIF(BTRIM(v_error_json->>'error_code'), '')),
      'message', LEFT(COALESCE(NULLIF(BTRIM(v_error_json->>'message'), ''), NULLIF(BTRIM(v_error_json->>'error'), ''), v_error_json::text), 1000)
    )) END;

    UPDATE public.banking_pay_operation_chunks AS chunk_update
    SET status = v_status,
        completed_count = v_completed_count,
        failed_count = v_failed_count,
        result_json = v_result_json,
        error_json = v_error_json,
        locked_by = NULL::text,
        lock_expires_at_utc = NULL::timestamptz,
        completed_at_utc = CASE WHEN v_status = 'PENDING' THEN NULL::timestamptz ELSE COALESCE(chunk_update.completed_at_utc, v_now) END,
        updated_at_utc = v_now
    WHERE chunk_update.id = v_chunk.id
    RETURNING chunk_update.* INTO v_chunk;

    IF v_status = 'PENDING' THEN
        UPDATE public.banking_pay_operations AS operation_update
        SET phase = v_chunk.phase,
            status = CASE WHEN upper(BTRIM(COALESCE(operation_update.status, ''))) IN ('QUEUED', 'RUNNING', 'CONTINUING', 'WAITING_RETRY') THEN 'RUNNING' ELSE operation_update.status END,
            runner_state = CASE WHEN upper(BTRIM(COALESCE(operation_update.status, ''))) IN ('QUEUED', 'RUNNING', 'CONTINUING', 'WAITING_RETRY') THEN 'RUNNABLE' ELSE operation_update.runner_state END,
            run_after_utc = CASE WHEN upper(BTRIM(COALESCE(operation_update.status, ''))) IN ('QUEUED', 'RUNNING', 'CONTINUING', 'WAITING_RETRY') THEN v_now ELSE operation_update.run_after_utc END,
            heartbeat_at_utc = v_now,
            last_advanced_at_utc = v_now,
            progress_json = jsonb_strip_nulls(COALESCE(operation_update.progress_json, '{}'::jsonb) || jsonb_build_object(
              'last_requeued_chunk_id', v_chunk.id::text,
              'last_requeued_chunk_phase', v_chunk.phase,
              'last_requeued_chunk_type', v_chunk.chunk_type,
              'last_requeued_chunk_status', v_chunk.status,
              'last_requeued_chunk_sequence_no', v_chunk.sequence_no,
              'last_requeued_chunk_completed_count', v_completed_count,
              'last_requeued_chunk_failed_count', v_failed_count,
              'last_requeued_chunk_result_hash', v_result_hash,
              'last_requeued_chunk_error_summary', v_error_summary,
              'last_advanced_at_utc', v_now::text
            )),
            updated_at_utc = v_now
        WHERE operation_update.id = v_chunk.operation_id
          AND upper(BTRIM(COALESCE(operation_update.status, ''))) NOT IN ('COMPLETE', 'FAILED', 'CANCELLED', 'CANCELED', 'REVIEW_REQUIRED');
    ELSE
        UPDATE public.banking_pay_operations AS operation_update
        SET phase = v_chunk.phase,
            status = CASE WHEN upper(BTRIM(COALESCE(operation_update.status, ''))) IN ('RUNNING', 'CONTINUING', 'WAITING_RETRY') THEN 'RUNNING' ELSE operation_update.status END,
            runner_state = CASE WHEN upper(BTRIM(COALESCE(operation_update.status, ''))) IN ('RUNNING', 'CONTINUING', 'WAITING_RETRY') THEN 'RUNNABLE' ELSE operation_update.runner_state END,
            run_after_utc = CASE WHEN upper(BTRIM(COALESCE(operation_update.status, ''))) IN ('RUNNING', 'CONTINUING', 'WAITING_RETRY') THEN v_now ELSE operation_update.run_after_utc END,
            heartbeat_at_utc = v_now,
            last_advanced_at_utc = v_now,
            current_chunk_index = COALESCE(operation_update.current_chunk_index, 0) + 1,
            completed_units = COALESCE(operation_update.completed_units, 0) + v_completed_count,
            failed_units = COALESCE(operation_update.failed_units, 0) + v_failed_count,
            progress_json = jsonb_strip_nulls(COALESCE(operation_update.progress_json, '{}'::jsonb) || jsonb_build_object(
              'last_finished_chunk_id', v_chunk.id::text,
              'last_finished_chunk_phase', v_chunk.phase,
              'last_finished_chunk_type', v_chunk.chunk_type,
              'last_finished_chunk_status', v_chunk.status,
              'last_finished_chunk_sequence_no', v_chunk.sequence_no,
              'last_finished_chunk_completed_count', v_completed_count,
              'last_finished_chunk_failed_count', v_failed_count,
              'last_finished_chunk_result_hash', v_result_hash,
              'last_finished_chunk_error_summary', v_error_summary,
              'last_advanced_at_utc', v_now::text
            )),
            updated_at_utc = v_now
        WHERE operation_update.id = v_chunk.operation_id
          AND upper(BTRIM(COALESCE(operation_update.status, ''))) NOT IN ('COMPLETE', 'FAILED', 'CANCELLED', 'CANCELED', 'REVIEW_REQUIRED');
    END IF;

    RETURN QUERY SELECT true, NULL::text, v_chunk.id, v_chunk.operation_id, v_chunk.phase, v_chunk.chunk_type, v_chunk.sequence_no, v_chunk.status, v_chunk.payload_json, v_chunk.result_json, v_chunk.error_json, v_chunk.unit_count, v_chunk.completed_count, v_chunk.failed_count, v_chunk.locked_by, v_chunk.lock_expires_at_utc, v_chunk.created_at_utc, v_chunk.started_at_utc, v_chunk.completed_at_utc, v_chunk.updated_at_utc;
END;
$function$;

-- banking_pay_operation_finish(uuid,text,jsonb,jsonb)
CREATE OR REPLACE FUNCTION public.banking_pay_operation_finish(p_operation_id uuid, p_status text, p_result_json jsonb DEFAULT NULL::jsonb, p_error_json jsonb DEFAULT NULL::jsonb)
 RETURNS TABLE(finished boolean, not_finished_reason text, operation_id uuid, operation_type text, status text, phase text, actor_user_id uuid, workbench_session_id uuid, pay_batch_id uuid, root_operation_id uuid, idempotency_key text, input_json jsonb, config_json jsonb, progress_json jsonb, result_json jsonb, error_json jsonb, total_units integer, completed_units integer, failed_units integer, current_chunk_index integer, chunk_count integer, locked_by text, lock_expires_at_utc timestamp with time zone, created_at_utc timestamp with time zone, started_at_utc timestamp with time zone, updated_at_utc timestamp with time zone, completed_at_utc timestamp with time zone, failed_at_utc timestamp with time zone)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
    v_now timestamptz := now();
    v_operation public.banking_pay_operations%ROWTYPE;
    v_status text := upper(NULLIF(BTRIM(COALESCE(p_status, '')), ''));
    v_result_json jsonb := p_result_json;
    v_error_json jsonb := p_error_json;
    v_runner_state text := NULL::text;
    v_requires_user_action boolean := false;
    v_resume_reason text := NULL::text;
    v_finish_scope_generation bigint := 0;
    v_finish_relevant_generation bigint := NULL::bigint;
    v_finish_unresolved_root_count integer := 0;
    v_finish_failed_root_count integer := 0;
    v_finish_scope_count integer := 0;
    v_finish_selected_count integer := 0;
    v_finish_scope_invalid_count integer := 0;
    v_finish_chunk_count integer := 0;
    v_finish_chunk_invalid_count integer := 0;
    v_finish_scope_hash text := NULL::text;
    v_finish_blocker jsonb := '{}'::jsonb;
    v_finish_scope_status text := 'NONE';
    v_finish_freshness_status text := 'VALID_AT_SCOPE_FREEZE';
    v_post_draft_authority_count integer := 0;
    v_source_publication_identity_enforce_enabled boolean := false;
    v_execution_overlay_chain_v2 jsonb := NULL::jsonb;
BEGIN
    PERFORM set_config('lock_timeout', '3s', true);

    SELECT COALESCE(setting.banking_pay_source_publication_identity_enforce_v1_enabled,false)
    INTO v_source_publication_identity_enforce_enabled
    FROM public.settings_defaults AS setting
    WHERE setting.id=1;

    IF p_operation_id IS NULL THEN
        RAISE EXCEPTION 'BANKING_PAY_OPERATION_FINISH_OPERATION_ID_REQUIRED'
          USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_OPERATION_FINISH_OPERATION_ID_REQUIRED')::text;
    END IF;

    IF v_status IS NULL OR v_status NOT IN ('COMPLETE', 'FAILED', 'CANCELLED', 'CANCELED', 'REVIEW_REQUIRED', 'WAITING_AUTHORISATION', 'WAITING_PROVIDER') THEN
        RAISE EXCEPTION 'BANKING_PAY_OPERATION_FINISH_STATUS_INVALID'
          USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_OPERATION_FINISH_STATUS_INVALID', 'operation_id', p_operation_id::text, 'status', p_status)::text;
    END IF;

    IF v_result_json IS NOT NULL AND jsonb_typeof(v_result_json) <> 'object' THEN
        RAISE EXCEPTION 'BANKING_PAY_OPERATION_FINISH_RESULT_MUST_BE_OBJECT'
          USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_OPERATION_FINISH_RESULT_MUST_BE_OBJECT', 'operation_id', p_operation_id::text)::text;
    END IF;

    IF v_error_json IS NOT NULL AND jsonb_typeof(v_error_json) <> 'object' THEN
        RAISE EXCEPTION 'BANKING_PAY_OPERATION_FINISH_ERROR_MUST_BE_OBJECT'
          USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_OPERATION_FINISH_ERROR_MUST_BE_OBJECT', 'operation_id', p_operation_id::text)::text;
    END IF;

    SELECT operation_row.*
    INTO v_operation
    FROM public.banking_pay_operations AS operation_row
    WHERE operation_row.id = p_operation_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RETURN QUERY SELECT false, 'NOT_FOUND'::text, p_operation_id, NULL::text, NULL::text, NULL::text, NULL::uuid, NULL::uuid, NULL::uuid, NULL::uuid, NULL::text, NULL::jsonb, NULL::jsonb, NULL::jsonb, NULL::jsonb, NULL::jsonb, NULL::integer, NULL::integer, NULL::integer, NULL::integer, NULL::integer, NULL::text, NULL::timestamptz, NULL::timestamptz, NULL::timestamptz, NULL::timestamptz, NULL::timestamptz, NULL::timestamptz;
        RETURN;
    END IF;

    IF upper(BTRIM(COALESCE(v_operation.status, ''))) IN ('COMPLETE', 'FAILED', 'CANCELLED', 'CANCELED') THEN
        RETURN QUERY SELECT false, 'ALREADY_TERMINAL'::text, v_operation.id, v_operation.operation_type, v_operation.status, v_operation.phase, v_operation.actor_user_id, v_operation.workbench_session_id, v_operation.pay_batch_id, v_operation.root_operation_id, v_operation.idempotency_key, v_operation.input_json, v_operation.config_json, v_operation.progress_json, v_operation.result_json, v_operation.error_json, v_operation.total_units, v_operation.completed_units, v_operation.failed_units, v_operation.current_chunk_index, v_operation.chunk_count, COALESCE(v_operation.lease_owner, v_operation.locked_by), COALESCE(v_operation.lease_expires_at_utc, v_operation.lock_expires_at_utc), v_operation.created_at_utc, v_operation.started_at_utc, v_operation.updated_at_utc, v_operation.completed_at_utc, v_operation.failed_at_utc;
        RETURN;
    END IF;

    IF UPPER(BTRIM(COALESCE(v_operation.operation_type, ''))) = 'DRAFT_CREATE'
       AND v_status = 'COMPLETE' THEN
      IF UPPER(BTRIM(COALESCE(v_operation.scope_freeze_status, ''))) <> 'FROZEN'
         OR NOT COALESCE(v_operation.source_scope_seed_complete, false)
         OR v_operation.frozen_scope_change_generation IS NULL
         OR v_operation.scope_frozen_at_utc IS NULL
         OR COALESCE(v_operation.frozen_candidate_scope_count, 0) <= 0
         OR COALESCE(v_operation.frozen_selected_row_count, 0) <= 0
         OR NULLIF(BTRIM(COALESCE(v_operation.frozen_operation_scope_hash, '')), '') IS NULL
         OR v_operation.frozen_source_session_version IS NULL
         OR v_operation.frozen_source_snapshot_run_id IS NULL THEN
        RAISE EXCEPTION 'DRAFT_CREATE_OPERATION_SCOPE_NOT_FROZEN'
          USING ERRCODE = 'P0001';
      END IF;

      SELECT
        (SELECT COUNT(*)::integer
         FROM public.banking_pay_operation_candidate_scope AS scope_count
         WHERE scope_count.operation_id = v_operation.id),
        (SELECT COUNT(DISTINCT selected_id.value)::integer
         FROM public.banking_pay_operation_candidate_scope AS selected_scope
         CROSS JOIN LATERAL jsonb_array_elements_text(
           CASE WHEN jsonb_typeof(selected_scope.selected_preview_row_ids_json) = 'array'
             THEN selected_scope.selected_preview_row_ids_json ELSE '[]'::jsonb END
         ) AS selected_id(value)
         WHERE selected_scope.operation_id = v_operation.id),
        (SELECT COUNT(*)::integer
         FROM public.banking_pay_operation_candidate_scope AS invalid_scope
         WHERE invalid_scope.operation_id = v_operation.id
           AND (
             invalid_scope.pay_batch_id IS NULL
             OR invalid_scope.workbench_session_id IS DISTINCT FROM v_operation.workbench_session_id
             OR invalid_scope.source_session_version IS DISTINCT FROM v_operation.frozen_source_session_version
             OR invalid_scope.source_snapshot_run_id IS DISTINCT FROM v_operation.frozen_source_snapshot_run_id
             OR UPPER(BTRIM(COALESCE(invalid_scope.status, ''))) NOT IN ('ALLOCATED', 'DRAFTED')
             OR NOT EXISTS (
               SELECT 1
               FROM public.pay_batches AS provenance_batch
               WHERE provenance_batch.id = invalid_scope.pay_batch_id
                 AND provenance_batch.source_scope_change_generation IS NOT DISTINCT FROM v_operation.frozen_scope_change_generation
                 AND provenance_batch.source_workbench_session_id IS NOT DISTINCT FROM v_operation.workbench_session_id
                 AND provenance_batch.source_session_version IS NOT DISTINCT FROM v_operation.frozen_source_session_version
                 AND provenance_batch.source_snapshot_run_id IS NOT DISTINCT FROM v_operation.frozen_source_snapshot_run_id
             )
           )),
        (SELECT md5(COALESCE(string_agg(
           hash_scope.candidate_id::text || ':' || hash_scope.pay_channel || ':' || hash_scope.scope_hash,
           '|' ORDER BY hash_scope.pay_channel, hash_scope.candidate_id
         ), ''))
         FROM public.banking_pay_operation_candidate_scope AS hash_scope
         WHERE hash_scope.operation_id = v_operation.id)
      INTO v_finish_scope_count, v_finish_selected_count, v_finish_scope_invalid_count, v_finish_scope_hash;

      IF v_finish_scope_count <> v_operation.frozen_candidate_scope_count
         OR v_finish_selected_count <> v_operation.frozen_selected_row_count
         OR v_finish_scope_invalid_count > 0
         OR v_finish_scope_hash IS DISTINCT FROM v_operation.frozen_operation_scope_hash THEN
        RAISE EXCEPTION 'DRAFT_CREATE_OPERATION_BATCH_PROVENANCE_MISMATCH'
          USING ERRCODE = 'P0001';
      END IF;

      SELECT COUNT(*)::integer,
             COUNT(*) FILTER (
               WHERE UPPER(BTRIM(COALESCE(operation_chunk.status, ''))) <> 'COMPLETE'
                  OR COALESCE(operation_chunk.completed_count, 0) <> COALESCE(operation_chunk.unit_count, 0)
                  OR COALESCE(operation_chunk.failed_count, 0) <> 0
             )::integer
      INTO v_finish_chunk_count, v_finish_chunk_invalid_count
      FROM public.banking_pay_operation_chunks AS operation_chunk
      WHERE operation_chunk.operation_id = v_operation.id
        AND UPPER(BTRIM(COALESCE(operation_chunk.chunk_type, ''))) = 'CANDIDATE_SCOPE';

      IF v_finish_chunk_count <= 0 OR v_finish_chunk_invalid_count > 0 THEN
        RAISE EXCEPTION 'DRAFT_CREATE_OPERATION_CHUNKS_INCOMPLETE'
          USING ERRCODE = 'P0001';
      END IF;

      -- Freeze the live candidate authority accepted immediately after the
      -- Draft has completed.  This is deliberately distinct from the
      -- pre-Draft source publication stored in allocation_basis_json: an
      -- untouched-Draft cancellation may restore that immutable V3 source
      -- only while this post-Draft sequence/generation pair is still live.
      -- Legacy or non-V3 scopes simply remain ineligible for the fast route.
      WITH authority_rows AS (
        SELECT
          draft_scope.id AS scope_id,
          draft_scope.candidate_id,
          draft_scope.pay_batch_id,
          draft_scope.workbench_session_id,
          draft_scope.source_session_version,
          draft_scope.source_snapshot_run_id,
          COALESCE(candidate_counter.seq, 0) AS source_change_seq,
          COALESCE(candidate_counter.scope_change_generation, 0) AS dirty_generation,
          draft_scope.allocation_basis_json->>'source_build_run_id' AS original_source_build_run_id,
          draft_scope.allocation_basis_json->>'source_publication_id' AS original_source_publication_id,
          draft_scope.allocation_basis_json->>'source_identity_digest' AS original_source_identity_digest,
          draft_scope.allocation_basis_json->>'semantic_proof_digest' AS original_semantic_proof_digest,
          draft_scope.allocation_basis_json->'source_publication_attestation' AS source_attestation
        FROM public.banking_pay_operation_candidate_scope AS draft_scope
        LEFT JOIN public.app_change_counters AS candidate_counter
          ON candidate_counter.entity_key = 'pay_candidate:' || draft_scope.candidate_id::text
        WHERE draft_scope.operation_id = v_operation.id
      ), eligible_authority AS (
        SELECT authority_rows.*,
          COALESCE(authority_rows.original_source_publication_id,'')
            ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            AS fast_reversion_eligible,
          md5(
            v_operation.id::text || '|' ||
            authority_rows.pay_batch_id::text || '|' ||
            authority_rows.workbench_session_id::text || '|' ||
            authority_rows.candidate_id::text || '|' ||
            authority_rows.source_change_seq::text || '|' ||
            authority_rows.dirty_generation::text || '|' ||
            authority_rows.source_session_version::text || '|' ||
            authority_rows.source_snapshot_run_id::text || '|' ||
            authority_rows.original_source_build_run_id || '|' ||
            COALESCE(authority_rows.original_source_publication_id,'') || '|' ||
            authority_rows.original_source_identity_digest || '|' ||
            authority_rows.original_semantic_proof_digest || '|' ||
            (COALESCE(authority_rows.original_source_publication_id,'')
              ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')::text ||
            '|POST_DRAFT_LIVE_AUTHORITY_V2'
          ) AS authority_digest
        FROM authority_rows
        WHERE COALESCE(authority_rows.source_attestation->>'attestation_version', '')
                = 'CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3'
          AND COALESCE(authority_rows.source_attestation->>'semantic_contract_version', '')
                = 'READY_TO_PAY_SEMANTIC_V2'
          AND COALESCE((authority_rows.source_attestation->>'semantic_ready')::boolean, false)
          AND COALESCE((authority_rows.source_attestation->>'parity_complete')::boolean, false)
          AND COALESCE(authority_rows.original_source_build_run_id, '')
                ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          AND NULLIF(BTRIM(COALESCE(authority_rows.original_source_identity_digest, '')), '') IS NOT NULL
          AND NULLIF(BTRIM(COALESCE(authority_rows.original_semantic_proof_digest, '')), '') IS NOT NULL
      ), frozen_authority AS (
        UPDATE public.banking_pay_operation_candidate_scope AS draft_scope
        SET allocation_basis_json = COALESCE(draft_scope.allocation_basis_json, '{}'::jsonb)
              || jsonb_build_object(
                'post_draft_authority',
                jsonb_build_object(
                  'contract_version', 'POST_DRAFT_LIVE_AUTHORITY_V2',
                  'draft_operation_id', v_operation.id,
                  'pay_batch_id', eligible_authority.pay_batch_id,
                  'workbench_session_id', eligible_authority.workbench_session_id,
                  'candidate_id', eligible_authority.candidate_id,
                  'source_change_seq', eligible_authority.source_change_seq,
                  'dirty_generation', eligible_authority.dirty_generation,
                  'source_session_version', eligible_authority.source_session_version,
                  'source_snapshot_run_id', eligible_authority.source_snapshot_run_id,
                  'original_source_build_run_id', eligible_authority.original_source_build_run_id,
                  'original_source_publication_id', eligible_authority.original_source_publication_id,
                  'fast_reversion_eligible', eligible_authority.fast_reversion_eligible,
                  'fast_reversion_ineligible_reason', CASE
                    WHEN eligible_authority.fast_reversion_eligible THEN NULL
                    ELSE 'LEGACY_PHYSICAL_PUBLICATION_MISSING'
                  END,
                  'original_source_identity_digest', eligible_authority.original_source_identity_digest,
                  'original_semantic_proof_digest', eligible_authority.original_semantic_proof_digest,
                  'authority_digest', eligible_authority.authority_digest,
                  'captured_at_utc', v_now,
                  'policy_x_authority', 'FROZEN_PRE_DRAFT_SOURCE_PLUS_POST_DRAFT_LIVE_FENCE'
                )
              ),
            updated_at_utc = v_now
        FROM eligible_authority
        WHERE draft_scope.id = eligible_authority.scope_id
        RETURNING draft_scope.id
      )
      SELECT COUNT(*)::integer
      INTO v_post_draft_authority_count
      FROM frozen_authority;

      v_result_json := COALESCE(v_result_json, '{}'::jsonb)
        || jsonb_build_object(
          'post_draft_authority_contract_version', 'POST_DRAFT_LIVE_AUTHORITY_V2',
          'post_draft_authority_count', v_post_draft_authority_count,
          'post_draft_authority_candidate_count', v_finish_scope_count,
          'post_draft_fast_reversion_eligible_count', (
            SELECT COUNT(*)::integer FROM public.banking_pay_operation_candidate_scope AS scope_row
            WHERE scope_row.operation_id=v_operation.id
              AND COALESCE((scope_row.allocation_basis_json->'post_draft_authority'->>'fast_reversion_eligible')::boolean,false)
          )
        );
    END IF;

    v_runner_state := CASE
      WHEN v_status = 'COMPLETE' THEN 'COMPLETE'
      WHEN v_status IN ('CANCELLED', 'CANCELED') THEN 'CANCELLED'
      WHEN v_status = 'FAILED' THEN 'FAILED'
      WHEN v_status = 'REVIEW_REQUIRED' THEN 'WAITING_USER_REVIEW'
      WHEN v_status = 'WAITING_AUTHORISATION' THEN 'WAITING_USER'
      WHEN v_status = 'WAITING_PROVIDER' THEN 'WAITING_PROVIDER'
      ELSE v_operation.runner_state
    END;

    v_requires_user_action := v_status IN ('FAILED', 'REVIEW_REQUIRED', 'WAITING_AUTHORISATION');
    v_resume_reason := CASE
      WHEN v_status = 'COMPLETE' THEN 'OPERATION_COMPLETE'
      WHEN v_status = 'REVIEW_REQUIRED' THEN 'REVIEW_REQUIRED'
      WHEN v_status = 'WAITING_AUTHORISATION' THEN 'AWAITING_PAYMENT_AUTHORISATION'
      WHEN v_status = 'WAITING_PROVIDER' THEN 'AWAITING_PROVIDER_OUTCOME'
      WHEN v_status = 'FAILED' THEN 'OPERATION_FAILED'
      ELSE v_operation.resume_reason
    END;

    -- DRAFT_CREATE completion records post-freeze evidence without aborting the
    -- frozen operation. Existing central freshness gates remain authoritative
    -- for every consequential post-draft action.
    IF UPPER(BTRIM(COALESCE(v_operation.operation_type, ''))) = 'DRAFT_CREATE'
       AND v_status = 'COMPLETE'
       AND v_operation.scope_freeze_status = 'FROZEN'
       AND v_operation.frozen_scope_change_generation IS NOT NULL THEN
      SELECT COALESCE(change_counter.seq, 0)
      INTO v_finish_scope_generation
      FROM public.app_change_counters AS change_counter
      WHERE change_counter.entity_key = 'pay_candidate_scope_generation';

      SELECT MAX(candidate_counter.scope_change_generation)
      INTO v_finish_relevant_generation
      FROM public.banking_pay_operation_candidate_scope AS frozen_scope
      JOIN public.app_change_counters AS candidate_counter
        ON candidate_counter.entity_key = 'pay_candidate:' || frozen_scope.candidate_id::text
      WHERE frozen_scope.operation_id = v_operation.id
        AND candidate_counter.scope_change_generation > v_operation.frozen_scope_change_generation
        AND candidate_counter.scope_change_generation <= v_finish_scope_generation;

      v_finish_blocker := public.pay_workbench_scope_blocker_state_v1(
        v_operation.workbench_session_id,
        v_finish_scope_generation,
        v_operation.id
      );
      v_finish_unresolved_root_count := COALESCE((v_finish_blocker->>'upstream_active_count')::integer, 0);
      v_finish_failed_root_count := COALESCE((v_finish_blocker->>'upstream_unresolved_failure_count')::integer, 0);

      IF v_finish_scope_generation = v_operation.frozen_scope_change_generation THEN
        v_finish_scope_status := 'NONE';
        v_finish_freshness_status := 'VALID_AT_SCOPE_FREEZE';
      ELSIF v_finish_relevant_generation IS NOT NULL THEN
        v_finish_scope_status := 'RELEVANT';
        v_finish_freshness_status := 'STALE_POST_SCOPE_FREEZE';
      ELSIF v_finish_failed_root_count > 0 THEN
        v_finish_scope_status := 'PENDING_RELEVANCE';
        v_finish_freshness_status := 'PENDING_SCOPE_CHANGE_RELEVANCE_FAILED';
      ELSIF v_finish_unresolved_root_count > 0 THEN
        v_finish_scope_status := 'PENDING_RELEVANCE';
        v_finish_freshness_status := 'PENDING_SCOPE_CHANGE_RELEVANCE';
      ELSE
        v_finish_scope_status := 'IRRELEVANT';
        v_finish_freshness_status := 'VALID_AT_SCOPE_FREEZE';
      END IF;

      UPDATE public.pay_batches AS operation_batch
      SET freshness_validation_status = v_finish_freshness_status,
          freshness_checked_at_utc = v_now,
          scope_generation_observed_at_shell = GREATEST(
            COALESCE(operation_batch.scope_generation_observed_at_shell, 0),
            v_finish_scope_generation
          ),
          freshness_result_json = COALESCE(operation_batch.freshness_result_json, '{}'::jsonb)
            || jsonb_strip_nulls(jsonb_build_object(
              'post_freeze_scope_status', v_finish_scope_status,
              'scope_generation_observed_at_operation_finish', v_finish_scope_generation,
              'post_freeze_relevant_generation', v_finish_relevant_generation,
              'unresolved_broad_root_count', v_finish_unresolved_root_count,
              'failed_broad_root_count', v_finish_failed_root_count,
              'scope_blocker_failure_sample', COALESCE(v_finish_blocker->'failure_sample', '[]'::jsonb),
              'checked_at_utc', v_now::text,
              'policy_x_authority', 'FROZEN_OPERATION_SCOPE'
            ))
      WHERE operation_batch.id IN (
        SELECT DISTINCT candidate_scope.pay_batch_id
        FROM public.banking_pay_operation_candidate_scope AS candidate_scope
        WHERE candidate_scope.operation_id = v_operation.id
          AND candidate_scope.pay_batch_id IS NOT NULL
      );
    END IF;

    -- PAYMENT_EXECUTE may legitimately create more than one finalized dirty
    -- generation while preparing a local unsent transfer and committing its
    -- frozen reservation/audit overlay.  Seal that exact bounded chain before
    -- exposing the operation as COMPLETE.  An unprovable chain never blocks
    -- execution; it is retained as a typed rejection and cancellation safely
    -- falls back to the ordinary current-authority route.
    IF UPPER(BTRIM(COALESCE(v_operation.operation_type, ''))) = 'PAYMENT_EXECUTE'
       AND v_status = 'COMPLETE'
       AND v_operation.pay_batch_id IS NOT NULL THEN
      v_execution_overlay_chain_v2 :=
        private.pay_workbench_execution_unsent_overlay_chain_seal_v2(
          v_operation.id,v_operation.pay_batch_id,'{}'::jsonb);
      v_result_json := COALESCE(v_result_json,'{}'::jsonb)
        || jsonb_build_object(
          'execution_unsent_overlay_chain_v2',v_execution_overlay_chain_v2
        );
    END IF;

    UPDATE public.banking_pay_operations AS operation_update
    SET status = v_status,
        runner_state = v_runner_state,
        requires_user_action = v_requires_user_action,
        resume_reason = v_resume_reason,
        result_json = v_result_json,
        error_json = v_error_json,
        lease_owner = NULL::text,
        lease_expires_at_utc = NULL::timestamptz,
        locked_by = NULL::text,
        lock_expires_at_utc = NULL::timestamptz,
        run_after_utc = CASE WHEN v_status IN ('WAITING_PROVIDER') THEN operation_update.run_after_utc ELSE NULL::timestamptz END,
        heartbeat_at_utc = v_now,
        last_advanced_at_utc = v_now,
        completed_at_utc = CASE WHEN v_status IN ('COMPLETE', 'CANCELLED', 'CANCELED') THEN COALESCE(operation_update.completed_at_utc, v_now) ELSE operation_update.completed_at_utc END,
        failed_at_utc = CASE WHEN v_status = 'FAILED' THEN COALESCE(operation_update.failed_at_utc, v_now) ELSE operation_update.failed_at_utc END,
        progress_json = jsonb_strip_nulls(COALESCE(operation_update.progress_json, '{}'::jsonb) || jsonb_build_object(
          'finished_at_utc', v_now::text,
          'finish_status', v_status,
          'runner_state', v_runner_state,
          'requires_user_action', v_requires_user_action,
          'resume_reason', v_resume_reason,
          'execution_unsent_overlay_chain_v2',v_execution_overlay_chain_v2
        )),
        post_freeze_scope_status = CASE
          WHEN UPPER(BTRIM(COALESCE(operation_update.operation_type, ''))) = 'DRAFT_CREATE'
           AND v_status = 'COMPLETE'
           AND operation_update.scope_freeze_status = 'FROZEN'
            THEN v_finish_scope_status
          ELSE operation_update.post_freeze_scope_status
        END,
        post_freeze_observed_generation = CASE
          WHEN UPPER(BTRIM(COALESCE(operation_update.operation_type, ''))) = 'DRAFT_CREATE'
           AND v_status = 'COMPLETE'
           AND operation_update.scope_freeze_status = 'FROZEN'
            THEN v_finish_scope_generation
          ELSE operation_update.post_freeze_observed_generation
        END,
        post_freeze_relevant_generation = CASE
          WHEN UPPER(BTRIM(COALESCE(operation_update.operation_type, ''))) = 'DRAFT_CREATE'
           AND v_status = 'COMPLETE'
           AND operation_update.scope_freeze_status = 'FROZEN'
            THEN v_finish_relevant_generation
          ELSE operation_update.post_freeze_relevant_generation
        END,
        post_freeze_scope_checked_at_utc = CASE
          WHEN UPPER(BTRIM(COALESCE(operation_update.operation_type, ''))) = 'DRAFT_CREATE'
           AND v_status = 'COMPLETE'
           AND operation_update.scope_freeze_status = 'FROZEN'
            THEN v_now
          ELSE operation_update.post_freeze_scope_checked_at_utc
        END,
        updated_at_utc = v_now
    WHERE operation_update.id = v_operation.id
    RETURNING operation_update.* INTO v_operation;

    RETURN QUERY SELECT true, NULL::text, v_operation.id, v_operation.operation_type, v_operation.status, v_operation.phase, v_operation.actor_user_id, v_operation.workbench_session_id, v_operation.pay_batch_id, v_operation.root_operation_id, v_operation.idempotency_key, v_operation.input_json, v_operation.config_json, v_operation.progress_json, v_operation.result_json, v_operation.error_json, v_operation.total_units, v_operation.completed_units, v_operation.failed_units, v_operation.current_chunk_index, v_operation.chunk_count, COALESCE(v_operation.lease_owner, v_operation.locked_by), COALESCE(v_operation.lease_expires_at_utc, v_operation.lock_expires_at_utc), v_operation.created_at_utc, v_operation.started_at_utc, v_operation.updated_at_utc, v_operation.completed_at_utc, v_operation.failed_at_utc;
END;
$function$;

-- banking_pay_operation_get(uuid,uuid,text)
CREATE OR REPLACE FUNCTION public.banking_pay_operation_get(p_operation_id uuid, p_actor_user_id uuid DEFAULT NULL::uuid, p_mode text DEFAULT 'PROGRESS_LIGHT'::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
    v_now timestamptz := now();
    v_mode text := upper(BTRIM(COALESCE(p_mode, 'PROGRESS_LIGHT')));
    v_operation public.banking_pay_operations%ROWTYPE;
    v_progress jsonb := '{}'::jsonb;
    v_result jsonb := '{}'::jsonb;
    v_error_summary jsonb := NULL::jsonb;
    v_heartbeat_age_seconds integer := NULL::integer;
    v_terminal boolean := false;
    v_failed boolean := false;
    v_review_required boolean := false;
    v_status_upper text := NULL::text;
    v_phase_upper text := NULL::text;
    v_operation_type_upper text := NULL::text;
    v_error_code text := NULL::text;
    v_error_message text := NULL::text;
    v_status_text text := NULL::text;
    v_draft_creation_failed_partial boolean := false;
    v_batch_action_blocked boolean := false;
    v_failed_partial_cleanup_status text := NULL::text;
    v_backend_runner_owned boolean := false;
    v_frontend_completion_required boolean := false;
    v_batch_ids jsonb := '[]'::jsonb;
    v_primary_pay_batch_id text := NULL::text;
    v_result_summary jsonb := NULL::jsonb;
BEGIN
    PERFORM public.banking_pay_hot_path_budget_apply('PROGRESS');

    IF p_operation_id IS NULL THEN
      RETURN jsonb_build_object('ok', false, 'code', 'OPERATION_ID_REQUIRED');
    END IF;

    IF v_mode IS NULL OR v_mode = '' THEN
      v_mode := 'PROGRESS_LIGHT';
    END IF;

    IF v_mode NOT IN ('PROGRESS_LIGHT', 'LIGHT') THEN
      RAISE EXCEPTION 'BANKING_PAY_OPERATION_GET_MODE_FORBIDDEN'
        USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_OPERATION_GET_MODE_FORBIDDEN', 'operation_id', p_operation_id::text, 'mode', p_mode)::text;
    END IF;

    SELECT operation_row.*
    INTO v_operation
    FROM public.banking_pay_operations AS operation_row
    WHERE operation_row.id = p_operation_id
      AND (
          p_actor_user_id IS NULL
          OR operation_row.actor_user_id IS NULL
          OR operation_row.actor_user_id = p_actor_user_id
      );

    IF NOT FOUND THEN
      RETURN jsonb_build_object('ok', false, 'code', 'OPERATION_NOT_FOUND', 'operation_id', p_operation_id::text);
    END IF;

    v_progress := COALESCE(v_operation.progress_json, '{}'::jsonb);
    v_result := COALESCE(v_operation.result_json, '{}'::jsonb);
    v_status_upper := UPPER(BTRIM(COALESCE(v_operation.status, '')));
    v_phase_upper := UPPER(BTRIM(COALESCE(v_operation.phase, '')));
    v_operation_type_upper := UPPER(BTRIM(COALESCE(v_operation.operation_type, '')));
    v_terminal := v_status_upper IN (
      'COMPLETE',
      'COMPLETED',
      'SUCCESS',
      'SUCCEEDED',
      'DONE',
      'FAILED',
      'ERROR',
      'CANCELLED',
      'CANCELED',
      'REVIEW_REQUIRED',
      'NEEDS_REVIEW',
      'REVIEW'
    );
    v_failed := v_status_upper IN ('FAILED', 'ERROR');
    v_review_required := v_status_upper IN ('REVIEW_REQUIRED', 'NEEDS_REVIEW', 'REVIEW') OR COALESCE(v_operation.requires_user_action, false);
    v_heartbeat_age_seconds := CASE WHEN v_operation.heartbeat_at_utc IS NULL THEN NULL ELSE GREATEST(0, EXTRACT(EPOCH FROM (v_now - v_operation.heartbeat_at_utc))::integer) END;

    v_error_summary := CASE
      WHEN v_operation.error_json IS NULL THEN NULL::jsonb
      WHEN jsonb_typeof(v_operation.error_json) = 'object' THEN jsonb_strip_nulls(jsonb_build_object(
        'code', COALESCE(NULLIF(BTRIM(v_operation.error_json->>'code'), ''), NULLIF(BTRIM(v_operation.error_json->>'error_code'), '')),
        'message', LEFT(COALESCE(NULLIF(BTRIM(v_operation.error_json->>'message'), ''), NULLIF(BTRIM(v_operation.error_json->>'error'), ''), v_operation.error_json::text), 1000),
        'review_reason_code', COALESCE(NULLIF(BTRIM(v_operation.error_json->>'review_reason_code'), ''), NULLIF(BTRIM(v_operation.error_json->>'provider_submit_review_reason_code'), '')),
        'provider_submission_status', NULLIF(BTRIM(v_operation.error_json->>'provider_submission_status'), '')
      ))
      ELSE jsonb_build_object('message', LEFT(v_operation.error_json::text, 1000))
    END;

    v_error_code := COALESCE(
      NULLIF(BTRIM(COALESCE(v_error_summary->>'code', '')), ''),
      NULLIF(BTRIM(COALESCE(v_operation.error_json->>'code', '')), ''),
      NULLIF(BTRIM(COALESCE(v_operation.error_json->>'error_code', '')), ''),
      NULLIF(BTRIM(COALESCE(v_progress->>'error_code', '')), ''),
      NULLIF(BTRIM(COALESCE(v_progress->>'code', '')), ''),
      NULLIF(BTRIM(COALESCE(v_result->>'error_code', '')), ''),
      NULLIF(BTRIM(COALESCE(v_result->>'code', '')), '')
    );

    v_error_message := COALESCE(
      NULLIF(BTRIM(COALESCE(v_error_summary->>'message', '')), ''),
      NULLIF(BTRIM(COALESCE(v_operation.error_json->>'message', '')), ''),
      NULLIF(BTRIM(COALESCE(v_operation.error_json->>'error', '')), ''),
      NULLIF(BTRIM(COALESCE(v_progress->>'error_message', '')), ''),
      NULLIF(BTRIM(COALESCE(v_progress->>'message', '')), ''),
      NULLIF(BTRIM(COALESCE(v_result->>'error_message', '')), ''),
      NULLIF(BTRIM(COALESCE(v_result->>'message', '')), '')
    );

    v_status_text := COALESCE(
      NULLIF(BTRIM(COALESCE(v_progress->>'status_text', '')), ''),
      NULLIF(BTRIM(COALESCE(v_progress->>'last_message', '')), ''),
      NULLIF(BTRIM(COALESCE(v_result->>'status_text', '')), ''),
      NULLIF(BTRIM(COALESCE(v_result->>'last_message', '')), ''),
      CASE
        WHEN v_failed THEN COALESCE(v_error_message, CASE WHEN v_operation_type_upper = 'DRAFT_CREATE' THEN 'Draft creation failed.' ELSE 'Operation failed.' END)
        WHEN v_review_required THEN 'Review required.'
        WHEN v_status_upper IN ('COMPLETE', 'COMPLETED', 'SUCCESS', 'SUCCEEDED', 'DONE') THEN 'Operation complete.'
        ELSE NULL::text
      END
    );

    v_draft_creation_failed_partial := UPPER(BTRIM(COALESCE(
      v_progress->>'draft_creation_failed_partial',
      v_result->>'draft_creation_failed_partial',
      v_operation.error_json->>'draft_creation_failed_partial',
      'false'
    ))) IN ('TRUE', 'T', '1', 'YES', 'Y', 'ON');

    v_batch_action_blocked := UPPER(BTRIM(COALESCE(
      v_progress->>'batch_action_blocked',
      v_result->>'batch_action_blocked',
      v_operation.error_json->>'batch_action_blocked',
      v_progress->>'normal_draft_actions_blocked',
      v_result->>'normal_draft_actions_blocked',
      v_operation.error_json->>'normal_draft_actions_blocked',
      'false'
    ))) IN ('TRUE', 'T', '1', 'YES', 'Y', 'ON');

    v_failed_partial_cleanup_status := COALESCE(
      NULLIF(BTRIM(COALESCE(v_progress->>'failed_partial_cleanup_status', '')), ''),
      NULLIF(BTRIM(COALESCE(v_result->>'failed_partial_cleanup_status', '')), ''),
      NULLIF(BTRIM(COALESCE(v_operation.error_json->>'failed_partial_cleanup_status', '')), '')
    );

    v_backend_runner_owned := CASE
      WHEN v_operation_type_upper = 'DRAFT_CREATE' THEN true
      ELSE upper(BTRIM(COALESCE(
        v_operation.input_json->>'backend_runner_owned',
        v_operation.input_json->>'backendRunnerOwned',
        v_operation.config_json->>'backend_runner_owned',
        v_operation.config_json->>'backendRunnerOwned',
        v_progress->>'backend_runner_owned',
        v_progress->>'backendRunnerOwned',
        'false'
      ))) IN ('TRUE', 'T', '1', 'YES', 'Y', 'ON')
    END;

    v_frontend_completion_required := CASE
      WHEN v_operation_type_upper = 'DRAFT_CREATE' THEN false
      ELSE upper(BTRIM(COALESCE(
        v_operation.input_json->>'frontend_completion_required',
        v_operation.input_json->>'frontendCompletionRequired',
        v_operation.config_json->>'frontend_completion_required',
        v_operation.config_json->>'frontendCompletionRequired',
        v_progress->>'frontend_completion_required',
        v_progress->>'frontendCompletionRequired',
        'false'
      ))) IN ('TRUE', 'T', '1', 'YES', 'Y', 'ON')
    END;

    SELECT COALESCE(jsonb_agg(batch_id_dedup.batch_id_text ORDER BY batch_id_dedup.batch_id_text), '[]'::jsonb)
    INTO v_batch_ids
    FROM (
      SELECT DISTINCT NULLIF(BTRIM(batch_id_source.batch_id_text), '') AS batch_id_text
      FROM (
        SELECT CASE WHEN v_operation.pay_batch_id IS NULL THEN NULL::text ELSE v_operation.pay_batch_id::text END AS batch_id_text
        UNION ALL SELECT v_progress->>'pay_batch_id'
        UNION ALL SELECT v_progress->>'payBatchId'
        UNION ALL SELECT v_progress->>'primary_pay_batch_id'
        UNION ALL SELECT v_progress->>'primaryPayBatchId'
        UNION ALL SELECT v_result->>'pay_batch_id'
        UNION ALL SELECT v_result->>'payBatchId'
        UNION ALL SELECT v_result->>'primary_pay_batch_id'
        UNION ALL SELECT v_result->>'primaryPayBatchId'
        UNION ALL SELECT v_operation.error_json->>'pay_batch_id'
        UNION ALL SELECT v_operation.error_json->>'payBatchId'
        UNION ALL SELECT v_operation.error_json->>'primary_pay_batch_id'
        UNION ALL SELECT v_operation.error_json->>'primaryPayBatchId'
        UNION ALL SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_progress->'pay_batch_ids') = 'array' THEN v_progress->'pay_batch_ids' ELSE '[]'::jsonb END)
        UNION ALL SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_progress->'payBatchIds') = 'array' THEN v_progress->'payBatchIds' ELSE '[]'::jsonb END)
        UNION ALL SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_progress->'created_pay_batch_ids') = 'array' THEN v_progress->'created_pay_batch_ids' ELSE '[]'::jsonb END)
        UNION ALL SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_progress->'createdPayBatchIds') = 'array' THEN v_progress->'createdPayBatchIds' ELSE '[]'::jsonb END)
        UNION ALL SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_result->'pay_batch_ids') = 'array' THEN v_result->'pay_batch_ids' ELSE '[]'::jsonb END)
        UNION ALL SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_result->'payBatchIds') = 'array' THEN v_result->'payBatchIds' ELSE '[]'::jsonb END)
        UNION ALL SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_result->'created_pay_batch_ids') = 'array' THEN v_result->'created_pay_batch_ids' ELSE '[]'::jsonb END)
        UNION ALL SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_result->'createdPayBatchIds') = 'array' THEN v_result->'createdPayBatchIds' ELSE '[]'::jsonb END)
        UNION ALL SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_operation.error_json->'pay_batch_ids') = 'array' THEN v_operation.error_json->'pay_batch_ids' ELSE '[]'::jsonb END)
        UNION ALL SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_operation.error_json->'payBatchIds') = 'array' THEN v_operation.error_json->'payBatchIds' ELSE '[]'::jsonb END)
        UNION ALL SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_operation.error_json->'created_pay_batch_ids') = 'array' THEN v_operation.error_json->'created_pay_batch_ids' ELSE '[]'::jsonb END)
        UNION ALL SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_operation.error_json->'createdPayBatchIds') = 'array' THEN v_operation.error_json->'createdPayBatchIds' ELSE '[]'::jsonb END)
        UNION ALL SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_operation.error_json#>'{created_batch_cleanup,discovered_pay_batch_ids}') = 'array' THEN v_operation.error_json#>'{created_batch_cleanup,discovered_pay_batch_ids}' ELSE '[]'::jsonb END)
      ) AS batch_id_source
    ) AS batch_id_dedup
    WHERE batch_id_dedup.batch_id_text IS NOT NULL;

    v_primary_pay_batch_id := COALESCE(
      CASE WHEN v_operation.pay_batch_id IS NULL THEN NULL::text ELSE v_operation.pay_batch_id::text END,
      NULLIF(BTRIM(v_progress->>'primary_pay_batch_id'), ''),
      NULLIF(BTRIM(v_progress->>'primaryPayBatchId'), ''),
      NULLIF(BTRIM(v_progress->>'pay_batch_id'), ''),
      NULLIF(BTRIM(v_progress->>'payBatchId'), ''),
      NULLIF(BTRIM(v_result->>'primary_pay_batch_id'), ''),
      NULLIF(BTRIM(v_result->>'primaryPayBatchId'), ''),
      NULLIF(BTRIM(v_result->>'pay_batch_id'), ''),
      NULLIF(BTRIM(v_result->>'payBatchId'), ''),
      NULLIF(BTRIM(v_operation.error_json->>'primary_pay_batch_id'), ''),
      NULLIF(BTRIM(v_operation.error_json->>'primaryPayBatchId'), ''),
      NULLIF(BTRIM(v_operation.error_json->>'pay_batch_id'), ''),
      NULLIF(BTRIM(v_operation.error_json->>'payBatchId'), '')
    );

    v_result_summary := jsonb_strip_nulls(jsonb_build_object(
      'operation_type', v_operation.operation_type,
      'workbench_session_id', CASE WHEN v_operation.workbench_session_id IS NULL THEN NULL ELSE v_operation.workbench_session_id::text END,
      'pay_batch_id', v_primary_pay_batch_id,
      'primary_pay_batch_id', v_primary_pay_batch_id,
      'pay_batch_ids', v_batch_ids,
      'created_pay_batch_ids', v_batch_ids,
      'created_batch_count', CASE WHEN COALESCE(v_result->>'created_batch_count', '') ~ '^[0-9]+$' THEN (v_result->>'created_batch_count')::integer ELSE jsonb_array_length(v_batch_ids) END,
      'source_session_discarded', COALESCE(v_result->>'source_session_discarded', v_progress->>'source_session_discarded'),
      'last_message', COALESCE(v_result->>'last_message', v_progress->>'last_message', v_progress->>'status_text')
    ) || jsonb_build_object(
      'draft_creation_failed_partial', v_draft_creation_failed_partial,
      'batch_action_blocked', v_batch_action_blocked,
      'failed_partial_cleanup_status', v_failed_partial_cleanup_status
    ));

    RETURN jsonb_strip_nulls(
      jsonb_build_object(
        'ok', true,
        'mode', 'PROGRESS_LIGHT',
        'operation_id', v_operation.id::text,
        'id', v_operation.id::text,
        'pay_batch_id', COALESCE(CASE WHEN v_operation.pay_batch_id IS NULL THEN NULL ELSE v_operation.pay_batch_id::text END, v_primary_pay_batch_id),
        'primary_pay_batch_id', v_primary_pay_batch_id,
        'pay_batch_ids', v_batch_ids,
        'created_pay_batch_ids', v_batch_ids,
        'operation_type', v_operation.operation_type,
        'workbench_session_id', CASE WHEN v_operation.workbench_session_id IS NULL THEN NULL ELSE v_operation.workbench_session_id::text END,
        'root_operation_id', CASE WHEN v_operation.root_operation_id IS NULL THEN NULL ELSE v_operation.root_operation_id::text END,
        'idempotency_key', v_operation.idempotency_key,
        'backend_runner_owned', v_backend_runner_owned,
        'frontend_completion_required', v_frontend_completion_required,
        'status', v_operation.status,
        'phase', v_operation.phase,
        'runner_state', v_operation.runner_state,
        'run_after_utc', CASE WHEN v_operation.run_after_utc IS NULL THEN NULL ELSE v_operation.run_after_utc::text END,
        'lease_owner', v_operation.lease_owner,
        'lease_expires_at_utc', CASE WHEN v_operation.lease_expires_at_utc IS NULL THEN NULL ELSE v_operation.lease_expires_at_utc::text END,
        'heartbeat_at_utc', CASE WHEN v_operation.heartbeat_at_utc IS NULL THEN NULL ELSE v_operation.heartbeat_at_utc::text END,
        'heartbeat_age_seconds', v_heartbeat_age_seconds,
        'last_advanced_at_utc', CASE WHEN v_operation.last_advanced_at_utc IS NULL THEN NULL ELSE v_operation.last_advanced_at_utc::text END
      )
      || jsonb_build_object(
        'attempt_count', COALESCE(v_operation.attempt_count, 0),
        'max_attempts', v_operation.max_attempts,
        'requires_user_action', COALESCE(v_operation.requires_user_action, false),
        'resume_reason', v_operation.resume_reason,
        'terminal', v_terminal,
        'failed', v_failed,
        'error_code', v_error_code,
        'error_message', v_error_message,
        'status_text', v_status_text,
        'phase_index', CASE WHEN COALESCE(v_progress->>'phase_index', '') ~ '^[0-9]+$' THEN (v_progress->>'phase_index')::integer ELSE NULL::integer END,
        'phase_total', CASE WHEN COALESCE(v_progress->>'phase_total', '') ~ '^[0-9]+$' THEN (v_progress->>'phase_total')::integer ELSE NULL::integer END,
        'total_units', COALESCE(v_operation.total_units, 0),
        'completed_units', COALESCE(v_operation.completed_units, 0),
        'failed_units', COALESCE(v_operation.failed_units, 0),
        'current_chunk_index', COALESCE(v_operation.current_chunk_index, 0),
        'chunk_count', COALESCE(v_operation.chunk_count, 0),
        'draft_creation_failed_partial', v_draft_creation_failed_partial,
        'batch_action_blocked', v_batch_action_blocked,
        'failed_partial_cleanup_status', v_failed_partial_cleanup_status,
        'server_running', (v_operation.lease_owner IS NOT NULL AND v_operation.lease_expires_at_utc IS NOT NULL AND v_operation.lease_expires_at_utc > v_now),
        'waiting', v_status_upper IN ('WAITING_AUTHORISATION', 'WAITING_PROVIDER', 'WAITING_RETRY'),
        'review_required', v_review_required,
        'counters', jsonb_build_object(
          'total_units', COALESCE(v_operation.total_units, 0),
          'completed_units', COALESCE(v_operation.completed_units, 0),
          'failed_units', COALESCE(v_operation.failed_units, 0),
          'current_chunk_index', COALESCE(v_operation.current_chunk_index, 0),
          'chunk_count', COALESCE(v_operation.chunk_count, 0)
        ),
        'result_summary', CASE WHEN upper(BTRIM(COALESCE(v_operation.operation_type, ''))) = 'DRAFT_CREATE' THEN v_result_summary ELSE NULL::jsonb END,
        'small_error_summary', v_error_summary
      )
      || jsonb_build_object(
        'stale_summary', jsonb_strip_nulls(jsonb_build_object(
          'freshness_status', v_progress->>'freshness_status',
          'freshness_result_hash', v_progress->>'freshness_result_hash',
          'freshness_scope_hash', v_progress->>'freshness_scope_hash',
          'freshness_stale_units', v_progress->>'freshness_stale_units',
          'freshness_pending_units', v_progress->>'freshness_pending_units'
        )),
        'proof_hashes', jsonb_strip_nulls(jsonb_build_object(
          'freshness_result_hash', v_progress->>'freshness_result_hash',
          'freshness_scope_hash', v_progress->>'freshness_scope_hash',
          'prepared_transfer_proof_hash', COALESCE(v_progress->>'prepared_transfer_proof_hash', v_progress#>>'{prepare,prepared_transfer_proof_hash}'),
          'prepared_scope_hash', v_progress->>'prepared_scope_hash',
          'provider_scope_hash', v_progress->>'provider_scope_hash'
        )),
        'provider_counters', jsonb_strip_nulls(jsonb_build_object(
          'submitted', COALESCE(v_progress->>'provider_submitted_count', v_progress->>'submitted_count'),
          'accepted', COALESCE(v_progress->>'provider_accepted_count', v_progress->>'accepted_count'),
          'failed', COALESCE(v_progress->>'provider_failed_count', v_progress->>'failed_count'),
          'unknown', COALESCE(v_progress->>'provider_unknown_count', v_progress->>'unknown_count'),
          'review_required', COALESCE(v_progress->>'provider_review_required_count', v_progress->>'review_required_count'),
          'remaining_ready', COALESCE(v_progress->>'provider_remaining_ready_count', v_progress->>'remaining_ready_count'),
          'last_provider_message', COALESCE(v_progress->>'last_provider_message', v_progress#>>'{provider,last_message}')
        )),
        'last_message', COALESCE(v_progress->>'last_message', v_progress->>'status_text')
      )
      || jsonb_build_object(
        'created_at_utc', CASE WHEN v_operation.created_at_utc IS NULL THEN NULL ELSE v_operation.created_at_utc::text END,
        'started_at_utc', CASE WHEN v_operation.started_at_utc IS NULL THEN NULL ELSE v_operation.started_at_utc::text END,
        'updated_at_utc', CASE WHEN v_operation.updated_at_utc IS NULL THEN NULL ELSE v_operation.updated_at_utc::text END,
        'completed_at_utc', CASE WHEN v_operation.completed_at_utc IS NULL THEN NULL ELSE v_operation.completed_at_utc::text END,
        'failed_at_utc', CASE WHEN v_operation.failed_at_utc IS NULL THEN NULL ELSE v_operation.failed_at_utc::text END
      )
    );
END;
$function$;

-- banking_pay_operation_lease_heartbeat(uuid,text,integer,uuid)
CREATE OR REPLACE FUNCTION public.banking_pay_operation_lease_heartbeat(p_operation_id uuid, p_lease_owner text, p_extend_seconds integer DEFAULT 60, p_actor_user_id uuid DEFAULT NULL::uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_extend_seconds integer := LEAST(GREATEST(COALESCE(p_extend_seconds, 60), 10), 300);
  v_operation_row public.banking_pay_operations%ROWTYPE;
  v_new_lease_expires_at_utc timestamptz;
BEGIN
  IF p_operation_id IS NULL THEN
    RAISE EXCEPTION 'BANKING_PAY_OPERATION_HEARTBEAT_OPERATION_ID_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_OPERATION_HEARTBEAT_OPERATION_ID_REQUIRED')::text;
  END IF;

  IF NULLIF(BTRIM(COALESCE(p_lease_owner, '')), '') IS NULL THEN
    RAISE EXCEPTION 'BANKING_PAY_OPERATION_HEARTBEAT_LEASE_OWNER_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_OPERATION_HEARTBEAT_LEASE_OWNER_REQUIRED', 'operation_id', p_operation_id::text)::text;
  END IF;

  SELECT operation_row.*
  INTO v_operation_row
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.id = p_operation_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'BANKING_PAY_OPERATION_HEARTBEAT_OPERATION_NOT_FOUND'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_OPERATION_HEARTBEAT_OPERATION_NOT_FOUND', 'operation_id', p_operation_id::text)::text;
  END IF;

  IF NULLIF(BTRIM(COALESCE(v_operation_row.lease_owner, '')), '') IS NULL
     OR v_operation_row.lease_owner <> p_lease_owner THEN
    RAISE EXCEPTION 'BANKING_PAY_OPERATION_HEARTBEAT_LEASE_OWNER_MISMATCH'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_OPERATION_HEARTBEAT_LEASE_OWNER_MISMATCH', 'operation_id', p_operation_id::text, 'expected_lease_owner', v_operation_row.lease_owner, 'actual_lease_owner', p_lease_owner)::text;
  END IF;

  IF v_operation_row.lease_expires_at_utc IS NULL OR v_operation_row.lease_expires_at_utc <= v_now THEN
    RAISE EXCEPTION 'BANKING_PAY_OPERATION_HEARTBEAT_LEASE_EXPIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_OPERATION_HEARTBEAT_LEASE_EXPIRED', 'operation_id', p_operation_id::text, 'lease_expires_at_utc', CASE WHEN v_operation_row.lease_expires_at_utc IS NULL THEN NULL ELSE v_operation_row.lease_expires_at_utc::text END)::text;
  END IF;

  v_new_lease_expires_at_utc := v_now + make_interval(secs => v_extend_seconds);

  UPDATE public.banking_pay_operations AS operation_update
  SET heartbeat_at_utc = v_now,
      lease_expires_at_utc = v_new_lease_expires_at_utc,
      updated_at_utc = v_now
  WHERE operation_update.id = p_operation_id;

  RETURN jsonb_build_object(
    'ok', true,
    'operation_id', p_operation_id::text,
    'lease_owner', p_lease_owner,
    'heartbeat_at_utc', v_now::text,
    'lease_expires_at_utc', v_new_lease_expires_at_utc::text,
    'status', v_operation_row.status,
    'runner_state', v_operation_row.runner_state
  );
END;
$function$;

-- banking_pay_operation_provider_attempt_record(uuid,uuid,uuid,uuid,text,text,text,text,text,text,jsonb,jsonb,jsonb,uuid)
CREATE OR REPLACE FUNCTION public.banking_pay_operation_provider_attempt_record(p_operation_id uuid, p_pay_batch_id uuid DEFAULT NULL::uuid, p_transfer_scope_id uuid DEFAULT NULL::uuid, p_provider_chunk_id uuid DEFAULT NULL::uuid, p_idempotency_key text DEFAULT NULL::text, p_request_id text DEFAULT NULL::text, p_provider_transaction_id text DEFAULT NULL::text, p_previous_state text DEFAULT NULL::text, p_new_state text DEFAULT NULL::text, p_lease_owner text DEFAULT NULL::text, p_request_json jsonb DEFAULT NULL::jsonb, p_response_json jsonb DEFAULT NULL::jsonb, p_error_json jsonb DEFAULT NULL::jsonb, p_actor_user_id uuid DEFAULT NULL::uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_operation_row public.banking_pay_operations%ROWTYPE;
  v_effective_pay_batch_id uuid := p_pay_batch_id;
  v_request_hash text := NULL::text;
  v_response_hash text := NULL::text;
  v_error_summary jsonb := NULL::jsonb;
  v_attempt_id uuid := NULL::uuid;
BEGIN
  IF p_operation_id IS NULL THEN
    RAISE EXCEPTION 'BANKING_PAY_PROVIDER_ATTEMPT_OPERATION_ID_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_PROVIDER_ATTEMPT_OPERATION_ID_REQUIRED')::text;
  END IF;

  IF NULLIF(BTRIM(COALESCE(p_new_state, '')), '') IS NULL THEN
    RAISE EXCEPTION 'BANKING_PAY_PROVIDER_ATTEMPT_NEW_STATE_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_PROVIDER_ATTEMPT_NEW_STATE_REQUIRED', 'operation_id', p_operation_id::text)::text;
  END IF;

  SELECT operation_row.*
  INTO v_operation_row
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.id = p_operation_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'BANKING_PAY_PROVIDER_ATTEMPT_OPERATION_NOT_FOUND'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_PROVIDER_ATTEMPT_OPERATION_NOT_FOUND', 'operation_id', p_operation_id::text)::text;
  END IF;

  v_effective_pay_batch_id := COALESCE(v_effective_pay_batch_id, v_operation_row.pay_batch_id);

  IF p_transfer_scope_id IS NOT NULL THEN
    PERFORM 1
    FROM public.banking_pay_operation_transfer_scope AS transfer_scope_check
    WHERE transfer_scope_check.id = p_transfer_scope_id
      AND transfer_scope_check.operation_id = p_operation_id
      AND (v_effective_pay_batch_id IS NULL OR transfer_scope_check.pay_batch_id = v_effective_pay_batch_id);

    IF NOT FOUND THEN
      RAISE EXCEPTION 'BANKING_PAY_PROVIDER_ATTEMPT_TRANSFER_SCOPE_NOT_FOUND'
        USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_PROVIDER_ATTEMPT_TRANSFER_SCOPE_NOT_FOUND', 'operation_id', p_operation_id::text, 'transfer_scope_id', p_transfer_scope_id::text)::text;
    END IF;
  END IF;

  IF p_provider_chunk_id IS NOT NULL THEN
    PERFORM 1
    FROM public.banking_pay_operation_chunks AS provider_chunk_check
    WHERE provider_chunk_check.id = p_provider_chunk_id
      AND provider_chunk_check.operation_id = p_operation_id;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'BANKING_PAY_PROVIDER_ATTEMPT_CHUNK_NOT_FOUND'
        USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_PROVIDER_ATTEMPT_CHUNK_NOT_FOUND', 'operation_id', p_operation_id::text, 'provider_chunk_id', p_provider_chunk_id::text)::text;
    END IF;
  END IF;

  IF p_request_json IS NOT NULL AND jsonb_typeof(p_request_json) <> 'object' THEN
    RAISE EXCEPTION 'BANKING_PAY_PROVIDER_ATTEMPT_REQUEST_JSON_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_PROVIDER_ATTEMPT_REQUEST_JSON_MUST_BE_OBJECT', 'operation_id', p_operation_id::text)::text;
  END IF;

  IF p_response_json IS NOT NULL AND jsonb_typeof(p_response_json) <> 'object' THEN
    RAISE EXCEPTION 'BANKING_PAY_PROVIDER_ATTEMPT_RESPONSE_JSON_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_PROVIDER_ATTEMPT_RESPONSE_JSON_MUST_BE_OBJECT', 'operation_id', p_operation_id::text)::text;
  END IF;

  IF p_error_json IS NOT NULL AND jsonb_typeof(p_error_json) <> 'object' THEN
    RAISE EXCEPTION 'BANKING_PAY_PROVIDER_ATTEMPT_ERROR_JSON_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_PROVIDER_ATTEMPT_ERROR_JSON_MUST_BE_OBJECT', 'operation_id', p_operation_id::text)::text;
  END IF;

  v_request_hash := CASE WHEN p_request_json IS NULL THEN NULL ELSE md5(p_request_json::text) END;
  v_response_hash := CASE WHEN p_response_json IS NULL THEN NULL ELSE md5(p_response_json::text) END;
  v_error_summary := CASE
    WHEN p_error_json IS NULL THEN NULL::jsonb
    ELSE jsonb_strip_nulls(jsonb_build_object(
      'code', COALESCE(NULLIF(BTRIM(p_error_json->>'code'), ''), NULLIF(BTRIM(p_error_json->>'error_code'), '')),
      'message', LEFT(COALESCE(NULLIF(BTRIM(p_error_json->>'message'), ''), NULLIF(BTRIM(p_error_json->>'error'), ''), p_error_json::text), 1000),
      'sqlstate', NULLIF(BTRIM(p_error_json->>'sqlstate'), ''),
      'provider_status', NULLIF(BTRIM(p_error_json->>'provider_status'), '')
    ))
  END;

  INSERT INTO public.banking_pay_operation_provider_attempts (
    operation_id,
    pay_batch_id,
    transfer_scope_id,
    provider_chunk_id,
    provider_idempotency_key,
    provider_request_id,
    provider_transaction_id,
    previous_state,
    new_state,
    lease_owner,
    compact_request_hash,
    compact_response_hash,
    compact_error_summary_json,
    created_at_utc
  )
  VALUES (
    p_operation_id,
    v_effective_pay_batch_id,
    p_transfer_scope_id,
    p_provider_chunk_id,
    NULLIF(BTRIM(COALESCE(p_idempotency_key, '')), ''),
    NULLIF(BTRIM(COALESCE(p_request_id, '')), ''),
    NULLIF(BTRIM(COALESCE(p_provider_transaction_id, '')), ''),
    NULLIF(BTRIM(COALESCE(p_previous_state, '')), ''),
    UPPER(BTRIM(COALESCE(p_new_state, ''))),
    NULLIF(BTRIM(COALESCE(p_lease_owner, '')), ''),
    v_request_hash,
    v_response_hash,
    v_error_summary,
    v_now
  )
  RETURNING id INTO v_attempt_id;

  UPDATE public.banking_pay_operations AS operation_update
  SET progress_json = jsonb_strip_nulls(
        COALESCE(operation_update.progress_json, '{}'::jsonb)
        || jsonb_build_object(
          'last_provider_attempt', jsonb_strip_nulls(jsonb_build_object(
            'attempt_id', CASE WHEN v_attempt_id IS NULL THEN NULL ELSE v_attempt_id::text END,
            'recorded_at_utc', v_now::text,
            'transfer_scope_id', CASE WHEN p_transfer_scope_id IS NULL THEN NULL ELSE p_transfer_scope_id::text END,
            'provider_chunk_id', CASE WHEN p_provider_chunk_id IS NULL THEN NULL ELSE p_provider_chunk_id::text END,
            'previous_state', NULLIF(BTRIM(COALESCE(p_previous_state, '')), ''),
            'new_state', UPPER(BTRIM(COALESCE(p_new_state, ''))),
            'provider_transaction_id_present', NULLIF(BTRIM(COALESCE(p_provider_transaction_id, '')), '') IS NOT NULL,
            'provider_request_id_present', NULLIF(BTRIM(COALESCE(p_request_id, '')), '') IS NOT NULL,
            'provider_idempotency_key_present', NULLIF(BTRIM(COALESCE(p_idempotency_key, '')), '') IS NOT NULL,
            'error_present', p_error_json IS NOT NULL
          ))
        )
      ),
      updated_at_utc = v_now
  WHERE operation_update.id = p_operation_id;

  RETURN jsonb_build_object(
    'ok', true,
    'attempt_id', CASE WHEN v_attempt_id IS NULL THEN NULL ELSE v_attempt_id::text END,
    'operation_id', p_operation_id::text,
    'pay_batch_id', CASE WHEN v_effective_pay_batch_id IS NULL THEN NULL ELSE v_effective_pay_batch_id::text END,
    'transfer_scope_id', CASE WHEN p_transfer_scope_id IS NULL THEN NULL ELSE p_transfer_scope_id::text END,
    'provider_chunk_id', CASE WHEN p_provider_chunk_id IS NULL THEN NULL ELSE p_provider_chunk_id::text END,
    'previous_state', NULLIF(BTRIM(COALESCE(p_previous_state, '')), ''),
    'new_state', UPPER(BTRIM(COALESCE(p_new_state, ''))),
    'created_at_utc', v_now::text,
    'compact_request_hash', v_request_hash,
    'compact_response_hash', v_response_hash,
    'error_present', p_error_json IS NOT NULL
  );
END;
$function$;

-- banking_pay_operation_release_lease(uuid,text,text,integer,jsonb,jsonb,jsonb,text,uuid)
CREATE OR REPLACE FUNCTION public.banking_pay_operation_release_lease(p_operation_id uuid, p_lease_owner text, p_release_state text DEFAULT 'MORE_WORK'::text, p_run_after_delay_seconds integer DEFAULT 0, p_progress_patch_json jsonb DEFAULT '{}'::jsonb, p_result_patch_json jsonb DEFAULT NULL::jsonb, p_error_json jsonb DEFAULT NULL::jsonb, p_resume_reason text DEFAULT NULL::text, p_actor_user_id uuid DEFAULT NULL::uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'private', 'extensions', 'pg_temp'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_release_state text := upper(BTRIM(COALESCE(p_release_state, 'MORE_WORK')));
  v_delay_seconds integer := LEAST(GREATEST(COALESCE(p_run_after_delay_seconds, 0), 0), 3600);
  v_operation_row public.banking_pay_operations%ROWTYPE;
  v_operation_type text := NULL::text;
  v_next_status text := 'RUNNING';
  v_next_runner_state text := 'RUNNABLE';
  v_next_run_after_utc timestamptz := now();
  v_next_requires_user_action boolean := false;
  v_next_resume_reason text := NULL::text;
  v_completed_at_utc timestamptz := NULL::timestamptz;
  v_failed_at_utc timestamptz := NULL::timestamptz;
  v_retry_failure boolean := false;
  v_next_attempt_count integer := NULL::integer;
  v_attempt_limit_reached boolean := false;
  v_raw_progress_patch_json jsonb := '{}'::jsonb;
  v_progress_patch_json jsonb := '{}'::jsonb;
  v_progress_patch_key text := NULL::text;
  v_progress_patch_value jsonb := NULL::jsonb;
  v_progress_patch_array_count integer := 0;
  v_progress_patch_key_count integer := 0;
  v_result_patch_key_count integer := 0;
  v_progress_patch_bytes integer := 0;
  v_compact_progress_patch_bytes integer := 0;
  v_result_patch_bytes integer := NULL::integer;
  v_error_json_bytes integer := NULL::integer;
  v_existing_progress_json_bytes integer := 0;
  v_existing_result_json_bytes integer := NULL::integer;
  v_release_diag_json jsonb := '{}'::jsonb;
  v_clear_retryable_pre_provider_error boolean := false;
  v_cleared_retryable_pre_provider_progress_error boolean := false;
  v_previous_phase text := NULL::text;
  v_requested_phase text := NULL::text;
  v_phase_for_update text := NULL::text;
  v_execution_overlay_chain_v2 jsonb := NULL::jsonb;
BEGIN
  PERFORM set_config('lock_timeout', '3s', true);

  IF p_operation_id IS NULL THEN
    RAISE EXCEPTION 'BANKING_PAY_OPERATION_RELEASE_OPERATION_ID_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_OPERATION_RELEASE_OPERATION_ID_REQUIRED')::text;
  END IF;

  IF NULLIF(BTRIM(COALESCE(p_lease_owner, '')), '') IS NULL THEN
    RAISE EXCEPTION 'BANKING_PAY_OPERATION_RELEASE_LEASE_OWNER_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_OPERATION_RELEASE_LEASE_OWNER_REQUIRED', 'operation_id', p_operation_id::text)::text;
  END IF;

  v_raw_progress_patch_json := COALESCE(p_progress_patch_json, '{}'::jsonb);

  IF p_progress_patch_json IS NOT NULL AND jsonb_typeof(p_progress_patch_json) <> 'object' THEN
    RAISE EXCEPTION 'BANKING_PAY_OPERATION_RELEASE_PROGRESS_PATCH_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_OPERATION_RELEASE_PROGRESS_PATCH_MUST_BE_OBJECT', 'operation_id', p_operation_id::text)::text;
  END IF;

  IF p_result_patch_json IS NOT NULL AND jsonb_typeof(p_result_patch_json) <> 'object' THEN
    RAISE EXCEPTION 'BANKING_PAY_OPERATION_RELEASE_RESULT_PATCH_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_OPERATION_RELEASE_RESULT_PATCH_MUST_BE_OBJECT', 'operation_id', p_operation_id::text)::text;
  END IF;

  SELECT count(*)::integer
  INTO v_progress_patch_key_count
  FROM jsonb_object_keys(v_raw_progress_patch_json) AS progress_patch_keys(progress_key);

  SELECT count(*)::integer
  INTO v_result_patch_key_count
  FROM jsonb_object_keys(COALESCE(p_result_patch_json, '{}'::jsonb)) AS result_patch_keys(result_key);

  v_progress_patch_bytes := pg_column_size(v_raw_progress_patch_json);
  v_result_patch_bytes := CASE WHEN p_result_patch_json IS NULL THEN NULL::integer ELSE pg_column_size(p_result_patch_json) END;
  v_error_json_bytes := CASE WHEN p_error_json IS NULL THEN NULL::integer ELSE pg_column_size(p_error_json) END;

  FOR v_progress_patch_key, v_progress_patch_value IN
    SELECT progress_patch_entry.key, progress_patch_entry.value
    FROM jsonb_each(v_raw_progress_patch_json) AS progress_patch_entry(key, value)
  LOOP
    v_progress_patch_array_count := 0;
    IF v_progress_patch_key IN (
      'candidate_ids',
      'pending_candidate_ids',
      'failed_candidate_ids',
      'scope_ids',
      'transfer_ids',
      'transfer_scope_ids',
      'pay_batch_item_ids',
      'provider_events',
      'rows',
      'row_errors',
      'diagnostic_rows',
      'status_rows',
      'items',
      'proof_rows',
      'transfers',
      'session_progress',
      'recent_jobs',
      'transfer_scope_rollup_proofs',
      'transfer_scope_item_seed_proofs',
      'canonical_preview_lines_json',
      'full_preview_json',
      'provider_payload',
      'provider_response_payload',
      'full_provider_payload',
      'full_payload',
      'raw_payload'
    ) AND jsonb_typeof(v_progress_patch_value) IN ('array', 'object') THEN
      IF jsonb_typeof(v_progress_patch_value) = 'array' THEN
        v_progress_patch_array_count := jsonb_array_length(v_progress_patch_value);
      END IF;

      v_progress_patch_json := v_progress_patch_json || jsonb_build_object(
        v_progress_patch_key,
        jsonb_build_object(
          'omitted_heavy_release_progress_field', true,
          'json_type', jsonb_typeof(v_progress_patch_value),
          'array_count', CASE WHEN jsonb_typeof(v_progress_patch_value) = 'array' THEN v_progress_patch_array_count ELSE NULL END,
          'object_key_count_omitted', CASE WHEN jsonb_typeof(v_progress_patch_value) = 'object' THEN true ELSE NULL END,
          'value_bytes', pg_column_size(v_progress_patch_value)
        )
      );
    ELSIF jsonb_typeof(v_progress_patch_value) = 'array' THEN
      v_progress_patch_array_count := jsonb_array_length(v_progress_patch_value);

      IF v_progress_patch_array_count > 25 THEN
        v_progress_patch_json := v_progress_patch_json || jsonb_build_object(
          v_progress_patch_key,
          jsonb_build_object(
            'omitted_large_release_progress_array', true,
            'array_count', v_progress_patch_array_count,
            'value_bytes', pg_column_size(v_progress_patch_value)
          )
        );
      ELSE
        v_progress_patch_json := v_progress_patch_json || jsonb_build_object(v_progress_patch_key, v_progress_patch_value);
      END IF;
    ELSE
      v_progress_patch_json := v_progress_patch_json || jsonb_build_object(v_progress_patch_key, v_progress_patch_value);
    END IF;
  END LOOP;

  v_compact_progress_patch_bytes := pg_column_size(v_progress_patch_json);

  SELECT operation_row.*
  INTO v_operation_row
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.id = p_operation_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'BANKING_PAY_OPERATION_RELEASE_OPERATION_NOT_FOUND'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_OPERATION_RELEASE_OPERATION_NOT_FOUND', 'operation_id', p_operation_id::text)::text;
  END IF;

  v_operation_type := upper(BTRIM(COALESCE(v_operation_row.operation_type, '')));
  v_previous_phase := NULLIF(UPPER(BTRIM(COALESCE(v_operation_row.phase, ''))), '');
  v_existing_progress_json_bytes := pg_column_size(COALESCE(v_operation_row.progress_json, '{}'::jsonb));
  v_existing_result_json_bytes := CASE WHEN v_operation_row.result_json IS NULL THEN NULL::integer ELSE pg_column_size(v_operation_row.result_json) END;
  v_requested_phase := UPPER(BTRIM(COALESCE(
    NULLIF(BTRIM(COALESCE(v_raw_progress_patch_json->>'phase', '')), ''),
    NULLIF(BTRIM(COALESCE(v_raw_progress_patch_json->>'next_phase', '')), ''),
    NULLIF(BTRIM(COALESCE(v_raw_progress_patch_json->>'next_required_phase', '')), ''),
    NULLIF(BTRIM(COALESCE(v_raw_progress_patch_json->>'operation_phase', '')), ''),
    ''
  )));
  v_requested_phase := NULLIF(v_requested_phase, '');

  IF NULLIF(BTRIM(COALESCE(v_operation_row.lease_owner, '')), '') IS NULL
     OR v_operation_row.lease_owner <> p_lease_owner THEN
    RAISE EXCEPTION 'BANKING_PAY_OPERATION_RELEASE_LEASE_OWNER_MISMATCH'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_OPERATION_RELEASE_LEASE_OWNER_MISMATCH', 'operation_id', p_operation_id::text, 'expected_lease_owner', v_operation_row.lease_owner, 'actual_lease_owner', p_lease_owner)::text;
  END IF;

  IF v_operation_row.lease_expires_at_utc IS NULL OR v_operation_row.lease_expires_at_utc <= v_now THEN
    RAISE EXCEPTION 'BANKING_PAY_OPERATION_RELEASE_LEASE_EXPIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_OPERATION_RELEASE_LEASE_EXPIRED', 'operation_id', p_operation_id::text, 'lease_expires_at_utc', CASE WHEN v_operation_row.lease_expires_at_utc IS NULL THEN NULL ELSE v_operation_row.lease_expires_at_utc::text END)::text;
  END IF;

  IF v_release_state IN ('MORE_WORK', 'RUNNABLE', 'RUNNING', 'CONTINUE', 'CONTINUING') THEN
    v_next_status := 'RUNNING';
    v_next_runner_state := 'RUNNABLE';
    v_next_run_after_utc := v_now + make_interval(secs => v_delay_seconds);
    v_next_requires_user_action := false;
    v_next_resume_reason := COALESCE(NULLIF(BTRIM(COALESCE(p_resume_reason, '')), ''), 'MORE_WORK_REMAINS');
  ELSIF v_release_state IN ('WAITING_RETRY', 'RETRYABLE_ERROR', 'RETRYABLE_FAILURE', 'RETRY', 'TRANSIENT_ERROR') THEN
    v_retry_failure := true;
    v_next_attempt_count := COALESCE(v_operation_row.attempt_count, 0) + 1;
    v_attempt_limit_reached := v_next_attempt_count >= COALESCE(v_operation_row.max_attempts, 10);

    IF v_attempt_limit_reached IS TRUE THEN
      v_next_status := 'FAILED';
      v_next_runner_state := 'FAILED';
      v_next_run_after_utc := NULL::timestamptz;
      v_next_requires_user_action := false;
      v_next_resume_reason := COALESCE(NULLIF(BTRIM(COALESCE(p_resume_reason, '')), ''), CASE WHEN v_operation_type = 'DRAFT_CREATE' THEN 'DRAFT_CREATE_ATTEMPT_LIMIT_EXHAUSTED' ELSE 'OPERATION_ATTEMPT_LIMIT_EXHAUSTED' END);
      v_failed_at_utc := v_now;
    ELSE
      v_next_status := 'WAITING';
      v_next_runner_state := 'RUNNABLE';
      v_next_run_after_utc := v_now + make_interval(secs => GREATEST(v_delay_seconds, 1));
      v_next_requires_user_action := false;
      v_next_resume_reason := COALESCE(NULLIF(BTRIM(COALESCE(p_resume_reason, '')), ''), 'WAITING_RETRY');
    END IF;
  ELSIF v_release_state IN ('WAITING_AUTHORISATION', 'WAITING_AUTHORIZATION', 'WAIT_AUTHORISATION', 'WAIT_AUTHORIZATION') THEN
    v_next_status := 'WAITING_AUTHORISATION';
    v_next_runner_state := 'WAITING_USER';
    v_next_run_after_utc := NULL::timestamptz;
    v_next_requires_user_action := true;
    v_next_resume_reason := COALESCE(NULLIF(BTRIM(COALESCE(p_resume_reason, '')), ''), 'AWAITING_PAYMENT_AUTHORISATION');
  ELSIF v_release_state IN ('WAITING_CHILD', 'WAIT_CHILD') THEN
    IF COALESCE(p_progress_patch_json->>'child_operation_id', '') !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' THEN
      RAISE EXCEPTION 'BANKING_PAY_OPERATION_CHILD_ID_REQUIRED'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'BANKING_PAY_OPERATION_CHILD_ID_REQUIRED',
                'operation_id', p_operation_id::text
              )::text;
    END IF;

    IF NOT EXISTS (
      SELECT 1
      FROM public.banking_pay_operations AS child_operation
      WHERE child_operation.id = (p_progress_patch_json->>'child_operation_id')::uuid
        AND child_operation.root_operation_id = p_operation_id
    ) THEN
      RAISE EXCEPTION 'BANKING_PAY_OPERATION_CHILD_MISMATCH'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'BANKING_PAY_OPERATION_CHILD_MISMATCH',
                'operation_id', p_operation_id::text,
                'child_operation_id', p_progress_patch_json->>'child_operation_id'
              )::text;
    END IF;

    v_next_status := 'WAITING';
    v_next_runner_state := 'WAITING_CHILD';
    v_next_run_after_utc := NULL::timestamptz;
    v_next_requires_user_action := false;
    v_next_resume_reason := COALESCE(NULLIF(BTRIM(COALESCE(p_resume_reason, '')), ''), 'AWAITING_CHILD_OPERATION');
  ELSIF v_release_state IN ('WAITING_PROVIDER', 'WAIT_PROVIDER') THEN
    v_next_status := 'WAITING_PROVIDER';
    v_next_runner_state := 'WAITING_PROVIDER';
    v_next_run_after_utc := v_now + make_interval(secs => GREATEST(v_delay_seconds, 60));
    v_next_requires_user_action := false;
    v_next_resume_reason := COALESCE(NULLIF(BTRIM(COALESCE(p_resume_reason, '')), ''), 'AWAITING_PROVIDER_OUTCOME');
  ELSIF v_release_state IN ('REVIEW_REQUIRED', 'UNSAFE', 'AMBIGUOUS') THEN
    v_next_status := 'REVIEW_REQUIRED';
    v_next_runner_state := 'WAITING_USER_REVIEW';
    v_next_run_after_utc := NULL::timestamptz;
    v_next_requires_user_action := true;
    v_next_resume_reason := COALESCE(NULLIF(BTRIM(COALESCE(p_resume_reason, '')), ''), 'REVIEW_REQUIRED');
  ELSIF v_release_state IN ('COMPLETE', 'COMPLETED', 'DONE') THEN
    v_next_status := 'COMPLETE';
    v_next_runner_state := 'COMPLETE';
    v_next_run_after_utc := NULL::timestamptz;
    v_next_requires_user_action := false;
    v_next_resume_reason := COALESCE(NULLIF(BTRIM(COALESCE(p_resume_reason, '')), ''), 'OPERATION_COMPLETE');
    v_completed_at_utc := v_now;
  ELSIF v_release_state IN ('FAILED', 'ERROR') THEN
    v_next_status := 'FAILED';
    v_next_runner_state := 'FAILED';
    v_next_run_after_utc := NULL::timestamptz;
    v_next_requires_user_action := CASE WHEN v_operation_type = 'DRAFT_CREATE' THEN false ELSE true END;
    v_next_resume_reason := COALESCE(NULLIF(BTRIM(COALESCE(p_resume_reason, '')), ''), CASE WHEN v_operation_type = 'DRAFT_CREATE' THEN 'DRAFT_CREATE_OPERATION_FAILED' ELSE 'OPERATION_FAILED' END);
    v_failed_at_utc := v_now;
  ELSE
    RAISE EXCEPTION 'BANKING_PAY_OPERATION_RELEASE_STATE_INVALID'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_OPERATION_RELEASE_STATE_INVALID', 'operation_id', p_operation_id::text, 'release_state', p_release_state)::text;
  END IF;

  IF v_requested_phase IS NOT NULL THEN
    v_phase_for_update := v_requested_phase;
  ELSIF v_next_status = 'COMPLETE' THEN
    v_phase_for_update := 'COMPLETE';
  ELSIF v_next_status = 'WAITING_AUTHORISATION' THEN
    v_phase_for_update := 'WAITING_AUTHORISATION';
  ELSE
    v_phase_for_update := NULL::text;
  END IF;

  v_clear_retryable_pre_provider_error := (
    v_release_state IN ('MORE_WORK', 'RUNNABLE', 'RUNNING', 'CONTINUE', 'CONTINUING')
    AND v_next_status IN ('RUNNING', 'WAITING')
    AND v_next_runner_state = 'RUNNABLE'
    AND v_next_requires_user_action IS FALSE
    AND (
      UPPER(BTRIM(COALESCE(v_next_resume_reason, p_resume_reason, ''))) = 'RELEASE_TIMEOUT_BEFORE_PROVIDER_CALL_RETRYABLE'
      OR (
        LOWER(BTRIM(COALESCE(v_raw_progress_patch_json->>'retryable_orchestration_issue', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        AND LOWER(BTRIM(COALESCE(v_raw_progress_patch_json->>'release_timeout_before_provider_call', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        AND LOWER(BTRIM(COALESCE(v_raw_progress_patch_json->>'provider_ambiguity', 'true'))) IN ('false', 'f', '0', 'no', 'n', 'off')
      )
    )
  );

  v_cleared_retryable_pre_provider_progress_error := (
    v_clear_retryable_pre_provider_error IS TRUE
    AND (
      COALESCE(v_operation_row.progress_json, '{}'::jsonb) ? 'error'
      OR v_progress_patch_json ? 'error'
    )
  );

  -- The canonical PAYMENT_EXECUTE runner terminalises through this lease
  -- release function rather than banking_pay_operation_finish.  Seal and
  -- retain the exact provider-unsubmitted execution-owned dirty chain before
  -- exposing the operation as COMPLETE.  A rejected receipt is diagnostic
  -- evidence only; it never blocks a valid execution and cancellation will
  -- continue through its safe fallback route.
  IF v_operation_type = 'PAYMENT_EXECUTE'
     AND v_next_status = 'COMPLETE'
     AND v_operation_row.pay_batch_id IS NOT NULL THEN
    v_execution_overlay_chain_v2 :=
      private.pay_workbench_execution_unsent_overlay_chain_seal_v2(
        v_operation_row.id,
        v_operation_row.pay_batch_id,
        '{}'::jsonb
      );
  END IF;

  v_release_diag_json := jsonb_build_object(
    'function_name', 'banking_pay_operation_release_lease',
    'operation_id', p_operation_id::text,
    'release_state', v_release_state,
    'previous_phase', v_previous_phase,
    'requested_phase', v_requested_phase,
    'next_phase', COALESCE(v_phase_for_update, v_operation_row.phase),
    'next_status', v_next_status,
    'next_runner_state', v_next_runner_state,
    'run_after_delay_seconds', v_delay_seconds,
    'progress_patch_bytes', v_progress_patch_bytes,
    'compact_progress_patch_bytes', v_compact_progress_patch_bytes,
    'progress_patch_key_count', v_progress_patch_key_count,
    'result_patch_bytes', v_result_patch_bytes,
    'result_patch_key_count', v_result_patch_key_count,
    'error_json_bytes', v_error_json_bytes,
    'existing_progress_json_bytes', v_existing_progress_json_bytes,
    'existing_result_json_bytes', v_existing_result_json_bytes,
    'actor_user_id', CASE WHEN p_actor_user_id IS NULL THEN NULL ELSE p_actor_user_id::text END,
    'cleared_retryable_pre_provider_error', v_clear_retryable_pre_provider_error,
    'cleared_retryable_pre_provider_progress_error', v_cleared_retryable_pre_provider_progress_error,
    'existing_progress_compaction_applied', false,
    'existing_progress_compaction_scope', 'diagnostic_only'
  );

  UPDATE public.banking_pay_operations AS operation_update
  SET status = v_next_status,
      phase = COALESCE(v_phase_for_update, operation_update.phase),
      runner_state = v_next_runner_state,
      run_after_utc = v_next_run_after_utc,
      requires_user_action = v_next_requires_user_action,
      resume_reason = v_next_resume_reason,
      attempt_count = CASE WHEN v_retry_failure IS TRUE THEN COALESCE(v_next_attempt_count, operation_update.attempt_count) ELSE operation_update.attempt_count END,
      lease_owner = NULL::text,
      lease_expires_at_utc = NULL::timestamptz,
      locked_by = NULL::text,
      lock_expires_at_utc = NULL::timestamptz,
      heartbeat_at_utc = v_now,
      last_advanced_at_utc = v_now,
      progress_json = jsonb_strip_nulls(
        CASE
          WHEN v_clear_retryable_pre_provider_error IS TRUE THEN COALESCE(operation_update.progress_json, '{}'::jsonb) - 'error'
          ELSE COALESCE(operation_update.progress_json, '{}'::jsonb)
        END
        || CASE
          WHEN v_clear_retryable_pre_provider_error IS TRUE THEN v_progress_patch_json - 'error'
          ELSE v_progress_patch_json
        END
        || jsonb_build_object(
          'execution_unsent_overlay_chain_v2', v_execution_overlay_chain_v2,
          'last_release', jsonb_build_object(
            'released_at_utc', v_now::text,
            'release_state', v_release_state,
            'next_status', v_next_status,
            'runner_state', v_next_runner_state,
            'run_after_utc', CASE WHEN v_next_run_after_utc IS NULL THEN NULL ELSE v_next_run_after_utc::text END,
            'requires_user_action', v_next_requires_user_action,
            'resume_reason', v_next_resume_reason,
            'previous_phase', v_previous_phase,
            'requested_phase', v_requested_phase,
            'next_phase', COALESCE(v_phase_for_update, operation_update.phase),
            'phase_persisted', v_phase_for_update IS NOT NULL,
            'retry_failure', v_retry_failure,
            'attempt_count', CASE WHEN v_retry_failure IS TRUE THEN v_next_attempt_count ELSE operation_update.attempt_count END,
            'max_attempts', operation_update.max_attempts,
            'attempt_limit_reached', v_attempt_limit_reached,
            'actor_user_id', CASE WHEN p_actor_user_id IS NULL THEN NULL ELSE p_actor_user_id::text END,
            'release_diag', v_release_diag_json
          )
        )
      ),
      result_json = CASE
        WHEN p_result_patch_json IS NULL AND v_execution_overlay_chain_v2 IS NULL
          THEN operation_update.result_json
        ELSE jsonb_strip_nulls(
          COALESCE(operation_update.result_json, '{}'::jsonb)
          || COALESCE(p_result_patch_json, '{}'::jsonb)
          || jsonb_build_object(
            'execution_unsent_overlay_chain_v2', v_execution_overlay_chain_v2
          )
        )
      END,
      error_json = CASE
        WHEN v_clear_retryable_pre_provider_error IS TRUE THEN NULL::jsonb
        WHEN p_error_json IS NULL AND v_attempt_limit_reached IS NOT TRUE THEN operation_update.error_json
        ELSE jsonb_strip_nulls(
          COALESCE(p_error_json, '{}'::jsonb)
          || CASE
            WHEN v_attempt_limit_reached IS TRUE THEN jsonb_build_object(
              'code', v_next_resume_reason,
              'message', 'Operation retry attempt limit was exhausted.',
              'operation_id', p_operation_id::text,
              'attempt_count', v_next_attempt_count,
              'max_attempts', operation_update.max_attempts
            )
            ELSE '{}'::jsonb
          END
        )
      END,
      completed_at_utc = CASE WHEN v_completed_at_utc IS NULL THEN operation_update.completed_at_utc ELSE COALESCE(operation_update.completed_at_utc, v_completed_at_utc) END,
      failed_at_utc = CASE
        WHEN v_clear_retryable_pre_provider_error IS TRUE THEN NULL::timestamptz
        WHEN v_failed_at_utc IS NULL THEN operation_update.failed_at_utc
        ELSE COALESCE(operation_update.failed_at_utc, v_failed_at_utc)
      END,
      updated_at_utc = v_now
  WHERE operation_update.id = p_operation_id;

  RETURN jsonb_build_object(
    'ok', true,
    'operation_id', p_operation_id::text,
    'released_lease_owner', p_lease_owner,
    'status', v_next_status,
    'runner_state', v_next_runner_state,
    'phase', COALESCE(v_phase_for_update, v_operation_row.phase),
    'previous_phase', v_previous_phase,
    'phase_persisted', v_phase_for_update IS NOT NULL,
    'run_after_utc', CASE WHEN v_next_run_after_utc IS NULL THEN NULL ELSE v_next_run_after_utc::text END,
    'requires_user_action', v_next_requires_user_action,
    'resume_reason', v_next_resume_reason,
    'released_at_utc', v_now::text,
    'retry_failure', v_retry_failure,
    'attempt_count', CASE WHEN v_retry_failure IS TRUE THEN v_next_attempt_count ELSE v_operation_row.attempt_count END,
    'max_attempts', v_operation_row.max_attempts,
    'attempt_limit_reached', v_attempt_limit_reached,
    'cleared_retryable_pre_provider_error', v_clear_retryable_pre_provider_error,
    'cleared_retryable_pre_provider_progress_error', v_cleared_retryable_pre_provider_progress_error,
    'release_diag', v_release_diag_json
  );
END;
$function$;

-- banking_pay_operation_save_progress(uuid,text,text,integer,integer,integer,integer,integer,jsonb,integer)
CREATE OR REPLACE FUNCTION public.banking_pay_operation_save_progress(p_operation_id uuid, p_status text DEFAULT NULL::text, p_phase text DEFAULT NULL::text, p_total_units integer DEFAULT NULL::integer, p_completed_units integer DEFAULT NULL::integer, p_failed_units integer DEFAULT NULL::integer, p_current_chunk_index integer DEFAULT NULL::integer, p_chunk_count integer DEFAULT NULL::integer, p_progress_json jsonb DEFAULT '{}'::jsonb, p_extend_lock_seconds integer DEFAULT NULL::integer)
 RETURNS TABLE(saved boolean, not_saved_reason text, operation_id uuid, operation_type text, status text, phase text, actor_user_id uuid, workbench_session_id uuid, pay_batch_id uuid, root_operation_id uuid, idempotency_key text, input_json jsonb, config_json jsonb, progress_json jsonb, result_json jsonb, error_json jsonb, total_units integer, completed_units integer, failed_units integer, current_chunk_index integer, chunk_count integer, locked_by text, lock_expires_at_utc timestamp with time zone, created_at_utc timestamp with time zone, started_at_utc timestamp with time zone, updated_at_utc timestamp with time zone, completed_at_utc timestamp with time zone, failed_at_utc timestamp with time zone)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
    v_now timestamptz := now();
    v_operation public.banking_pay_operations%ROWTYPE;
    v_status text;
    v_phase text;
    v_total_units integer;
    v_completed_units integer;
    v_failed_units integer;
    v_current_chunk_index integer;
    v_chunk_count integer;
    v_progress_json jsonb := COALESCE(p_progress_json, '{}'::jsonb);
    v_compact_progress_json jsonb := '{}'::jsonb;
    v_extend_lock_seconds integer;
    v_key text;
    v_value jsonb;
    v_runner_state text;
    v_run_after_utc timestamptz;
    v_requires_user_action boolean;
    v_resume_reason text;
BEGIN
    PERFORM set_config('lock_timeout', '3s', true);

    IF p_operation_id IS NULL THEN
        RAISE EXCEPTION 'BANKING_PAY_OPERATION_SAVE_PROGRESS_OPERATION_ID_REQUIRED'
          USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_OPERATION_SAVE_PROGRESS_OPERATION_ID_REQUIRED')::text;
    END IF;

    IF jsonb_typeof(v_progress_json) <> 'object' THEN
        RAISE EXCEPTION 'BANKING_PAY_OPERATION_SAVE_PROGRESS_PROGRESS_MUST_BE_OBJECT'
          USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_OPERATION_SAVE_PROGRESS_PROGRESS_MUST_BE_OBJECT', 'operation_id', p_operation_id::text)::text;
    END IF;

    SELECT operation_row.*
    INTO v_operation
    FROM public.banking_pay_operations AS operation_row
    WHERE operation_row.id = p_operation_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RETURN QUERY
        SELECT false, 'NOT_FOUND'::text, p_operation_id, NULL::text, NULL::text, NULL::text, NULL::uuid, NULL::uuid, NULL::uuid, NULL::uuid, NULL::text, NULL::jsonb, NULL::jsonb, NULL::jsonb, NULL::jsonb, NULL::jsonb, NULL::integer, NULL::integer, NULL::integer, NULL::integer, NULL::integer, NULL::text, NULL::timestamptz, NULL::timestamptz, NULL::timestamptz, NULL::timestamptz, NULL::timestamptz, NULL::timestamptz;
        RETURN;
    END IF;

    IF upper(BTRIM(COALESCE(v_operation.status, ''))) IN ('COMPLETE', 'FAILED', 'CANCELLED', 'CANCELED') THEN
        RETURN QUERY
        SELECT false, 'TERMINAL'::text, v_operation.id, v_operation.operation_type, v_operation.status, v_operation.phase, v_operation.actor_user_id, v_operation.workbench_session_id, v_operation.pay_batch_id, v_operation.root_operation_id, v_operation.idempotency_key, v_operation.input_json, v_operation.config_json, v_operation.progress_json, v_operation.result_json, v_operation.error_json, v_operation.total_units, v_operation.completed_units, v_operation.failed_units, v_operation.current_chunk_index, v_operation.chunk_count, COALESCE(v_operation.lease_owner, v_operation.locked_by), COALESCE(v_operation.lease_expires_at_utc, v_operation.lock_expires_at_utc), v_operation.created_at_utc, v_operation.started_at_utc, v_operation.updated_at_utc, v_operation.completed_at_utc, v_operation.failed_at_utc;
        RETURN;
    END IF;

    FOR v_key, v_value IN SELECT key, value FROM jsonb_each(v_progress_json)
    LOOP
        IF jsonb_typeof(v_value) = 'array' AND jsonb_array_length(v_value) > 25 THEN
            v_compact_progress_json := v_compact_progress_json || jsonb_build_object(v_key, jsonb_build_object('omitted_large_array', true, 'count', jsonb_array_length(v_value)));
        ELSIF v_key IN ('candidate_ids', 'pending_candidate_ids', 'failed_candidate_ids', 'scope_ids', 'pay_batch_item_ids', 'transfer_ids', 'transfer_scope_ids', 'provider_events', 'rows', 'status_rows', 'canonical_preview_lines_json', 'full_preview_json')
              AND jsonb_typeof(v_value) IN ('array', 'object') THEN
            v_compact_progress_json := v_compact_progress_json || jsonb_build_object(v_key, jsonb_build_object('omitted_heavy_field', true, 'json_type', jsonb_typeof(v_value)));
        ELSE
            v_compact_progress_json := v_compact_progress_json || jsonb_build_object(v_key, v_value);
        END IF;
    END LOOP;

    v_status := upper(COALESCE(NULLIF(BTRIM(COALESCE(p_status, '')), ''), v_operation.status, 'RUNNING'));
    v_phase := upper(COALESCE(NULLIF(BTRIM(COALESCE(p_phase, '')), ''), v_operation.phase, 'INITIALISE'));

    IF v_status NOT IN ('RUNNING', 'CONTINUING', 'WAITING_RETRY', 'WAITING_AUTHORISATION', 'WAITING_PROVIDER', 'REVIEW_REQUIRED', 'COMPLETE', 'FAILED', 'CANCELLED', 'CANCELED') THEN
        RAISE EXCEPTION 'BANKING_PAY_OPERATION_SAVE_PROGRESS_STATUS_UNSUPPORTED'
          USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_OPERATION_SAVE_PROGRESS_STATUS_UNSUPPORTED', 'operation_id', p_operation_id::text, 'status', v_status)::text;
    END IF;

    v_total_units := GREATEST(COALESCE(p_total_units, v_operation.total_units, 0), 0);
    v_completed_units := GREATEST(COALESCE(p_completed_units, v_operation.completed_units, 0), 0);
    v_failed_units := GREATEST(COALESCE(p_failed_units, v_operation.failed_units, 0), 0);
    v_current_chunk_index := GREATEST(COALESCE(p_current_chunk_index, v_operation.current_chunk_index, 0), 0);
    v_chunk_count := GREATEST(COALESCE(p_chunk_count, v_operation.chunk_count, 0), 0);

    v_runner_state := upper(COALESCE(NULLIF(BTRIM(COALESCE(v_compact_progress_json->>'runner_state', '')), ''), CASE
      WHEN v_status = 'REVIEW_REQUIRED' THEN 'WAITING_USER_REVIEW'
      WHEN v_status = 'WAITING_AUTHORISATION' THEN 'WAITING_USER'
      WHEN v_status = 'WAITING_PROVIDER' THEN 'WAITING_PROVIDER'
      WHEN v_status IN ('RUNNING', 'CONTINUING', 'WAITING_RETRY') THEN 'RUNNABLE'
      WHEN v_status IN ('COMPLETE', 'FAILED', 'CANCELLED', 'CANCELED') THEN v_status
      ELSE COALESCE(v_operation.runner_state, 'RUNNABLE')
    END));

    v_requires_user_action := CASE
      WHEN v_status IN ('REVIEW_REQUIRED', 'WAITING_AUTHORISATION') THEN true
      WHEN v_compact_progress_json ? 'requires_user_action' THEN lower(BTRIM(COALESCE(v_compact_progress_json->>'requires_user_action', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
      ELSE COALESCE(v_operation.requires_user_action, false)
    END;

    v_resume_reason := COALESCE(NULLIF(BTRIM(COALESCE(v_compact_progress_json->>'resume_reason', '')), ''), v_operation.resume_reason);

    IF NULLIF(BTRIM(COALESCE(v_compact_progress_json->>'run_after_utc', '')), '') IS NOT NULL THEN
      BEGIN
        v_run_after_utc := (v_compact_progress_json->>'run_after_utc')::timestamptz;
      EXCEPTION WHEN OTHERS THEN
        v_run_after_utc := v_operation.run_after_utc;
      END;
    ELSE
      v_run_after_utc := CASE WHEN v_status IN ('RUNNING', 'CONTINUING', 'WAITING_RETRY') THEN COALESCE(v_operation.run_after_utc, v_now) ELSE v_operation.run_after_utc END;
    END IF;

    IF p_extend_lock_seconds IS NOT NULL AND p_extend_lock_seconds > 0 THEN
        v_extend_lock_seconds := LEAST(GREATEST(p_extend_lock_seconds, 10), 3600);
    ELSE
        v_extend_lock_seconds := NULL;
    END IF;

    UPDATE public.banking_pay_operations AS operation_update
    SET status = v_status,
        phase = v_phase,
        total_units = v_total_units,
        completed_units = v_completed_units,
        failed_units = v_failed_units,
        current_chunk_index = v_current_chunk_index,
        chunk_count = v_chunk_count,
        progress_json = jsonb_strip_nulls(COALESCE(operation_update.progress_json, '{}'::jsonb) || v_compact_progress_json || jsonb_build_object(
          'last_progress_saved_at_utc', v_now::text,
          'phase', v_phase,
          'status', v_status,
          'runner_state', v_runner_state,
          'requires_user_action', v_requires_user_action,
          'resume_reason', v_resume_reason
        )),
        runner_state = v_runner_state,
        run_after_utc = v_run_after_utc,
        requires_user_action = v_requires_user_action,
        resume_reason = v_resume_reason,
        heartbeat_at_utc = v_now,
        last_advanced_at_utc = v_now,
        lease_expires_at_utc = CASE WHEN v_extend_lock_seconds IS NULL THEN operation_update.lease_expires_at_utc ELSE v_now + make_interval(secs => v_extend_lock_seconds) END,
        lock_expires_at_utc = CASE WHEN v_extend_lock_seconds IS NULL THEN operation_update.lock_expires_at_utc ELSE v_now + make_interval(secs => v_extend_lock_seconds) END,
        updated_at_utc = v_now
    WHERE operation_update.id = v_operation.id
    RETURNING operation_update.* INTO v_operation;

    RETURN QUERY
    SELECT true, NULL::text, v_operation.id, v_operation.operation_type, v_operation.status, v_operation.phase, v_operation.actor_user_id, v_operation.workbench_session_id, v_operation.pay_batch_id, v_operation.root_operation_id, v_operation.idempotency_key, v_operation.input_json, v_operation.config_json, v_operation.progress_json, v_operation.result_json, v_operation.error_json, v_operation.total_units, v_operation.completed_units, v_operation.failed_units, v_operation.current_chunk_index, v_operation.chunk_count, COALESCE(v_operation.lease_owner, v_operation.locked_by), COALESCE(v_operation.lease_expires_at_utc, v_operation.lock_expires_at_utc), v_operation.created_at_utc, v_operation.started_at_utc, v_operation.updated_at_utc, v_operation.completed_at_utc, v_operation.failed_at_utc;
END;
$function$;

-- banking_pay_operation_seed_chunks(uuid,text,text,integer,jsonb)
CREATE OR REPLACE FUNCTION public.banking_pay_operation_seed_chunks(p_operation_id uuid, p_phase text, p_chunk_type text, p_chunk_size integer, p_units_json jsonb)
 RETURNS TABLE(total_units integer, chunk_count integer, existing_chunk_count integer, new_chunk_count integer)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_phase text := nullif(btrim(coalesce(p_phase, '')), '');
  v_chunk_type text := upper(nullif(btrim(coalesce(p_chunk_type, '')), ''));
  v_chunk_size integer := LEAST(GREATEST(coalesce(p_chunk_size, 100), 1), 100);
  v_units_json jsonb := coalesce(p_units_json, '[]'::jsonb);
  v_operation_row public.banking_pay_operations%ROWTYPE;
  v_operation_type text := NULL::text;
  v_total_units integer := 0;
  v_chunk_count integer := 0;
  v_existing_chunk_count integer := 0;
  v_new_chunk_count integer := 0;
  v_repaired_chunk_count integer := 0;
  v_mismatch_count integer := 0;
  v_frozen_scope_count integer := 0;
  v_frozen_selected_count integer := 0;
  v_frozen_invalid_count integer := 0;
  v_recomputed_scope_hash text := NULL::text;
  v_legacy_mode boolean := false;
BEGIN
  PERFORM public.banking_pay_hot_path_budget_apply('WORKER_CHUNK');

  IF p_operation_id IS NULL THEN
    RAISE EXCEPTION 'banking_pay_operation_seed_chunks requires p_operation_id';
  END IF;

  IF v_phase IS NULL THEN
    RAISE EXCEPTION 'banking_pay_operation_seed_chunks requires p_phase';
  END IF;

  IF v_chunk_type IS NULL THEN
    RAISE EXCEPTION 'banking_pay_operation_seed_chunks requires p_chunk_type';
  END IF;

  IF v_chunk_type NOT IN (
    'CANDIDATE_SCOPE',
    'TSFIN',
    'PAYEE_READINESS',
    'TRANSFER_GROUP',
    'TRANSFER_SCOPE_ITEM_SEED',
    'TRANSFER_SCOPE_ROLLUP',
    'TRANSFER_SUBMIT',
    'RAIL_UPDATE',
    'SETTLEMENT',
    'REMITTANCE',
    'PAYOUT_NOTICE',
    'PREVIEW_PAGE',
    'FRESHNESS_VALIDATE'
  ) THEN
    RAISE EXCEPTION 'Unsupported banking pay operation chunk_type: %', v_chunk_type;
  END IF;

  IF p_units_json IS NOT NULL AND jsonb_typeof(p_units_json) <> 'array' THEN
    RAISE EXCEPTION 'banking_pay_operation_seed_chunks requires p_units_json to be a JSON array when supplied';
  END IF;

  SELECT operation_row.*
  INTO v_operation_row
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.id = p_operation_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'banking_pay_operation_seed_chunks operation not found: %', p_operation_id;
  END IF;

  IF upper(btrim(coalesce(v_operation_row.status, ''))) IN ('COMPLETE', 'COMPLETED', 'FAILED', 'CANCELLED', 'CANCELED', 'REVIEW_REQUIRED') THEN
    RAISE EXCEPTION 'banking_pay_operation_seed_chunks cannot seed chunks for terminal operation % with status %', p_operation_id, v_operation_row.status;
  END IF;

  v_operation_type := upper(btrim(coalesce(v_operation_row.operation_type, '')));

  IF v_operation_type = 'DRAFT_CREATE' AND v_chunk_type = 'CANDIDATE_SCOPE' THEN
    IF UPPER(BTRIM(COALESCE(v_operation_row.scope_freeze_status, ''))) <> 'FROZEN'
       OR NOT COALESCE(v_operation_row.source_scope_seed_complete, false)
       OR v_operation_row.frozen_scope_change_generation IS NULL
       OR v_operation_row.scope_frozen_at_utc IS NULL
       OR COALESCE(v_operation_row.frozen_candidate_scope_count, 0) <= 0
       OR COALESCE(v_operation_row.frozen_selected_row_count, 0) <= 0
       OR NULLIF(BTRIM(COALESCE(v_operation_row.frozen_operation_scope_hash, '')), '') IS NULL
       OR v_operation_row.frozen_source_session_version IS NULL
       OR v_operation_row.frozen_source_snapshot_run_id IS NULL THEN
      RAISE EXCEPTION 'DRAFT_CREATE_OPERATION_SCOPE_NOT_FROZEN'
        USING ERRCODE = 'P0001';
    END IF;

    SELECT
      (SELECT COUNT(*)::integer
       FROM public.banking_pay_operation_candidate_scope AS scope_count
       WHERE scope_count.operation_id = p_operation_id),
      (SELECT COUNT(DISTINCT selected_id.value)::integer
       FROM public.banking_pay_operation_candidate_scope AS selected_scope
       CROSS JOIN LATERAL jsonb_array_elements_text(
         CASE
           WHEN jsonb_typeof(selected_scope.selected_preview_row_ids_json) = 'array'
             THEN selected_scope.selected_preview_row_ids_json
           ELSE '[]'::jsonb
         END
       ) AS selected_id(value)
       WHERE selected_scope.operation_id = p_operation_id),
      (SELECT COUNT(*)::integer
       FROM public.banking_pay_operation_candidate_scope AS invalid_scope
       WHERE invalid_scope.operation_id = p_operation_id
         AND (
           invalid_scope.workbench_session_id IS DISTINCT FROM v_operation_row.workbench_session_id
           OR invalid_scope.source_session_version IS DISTINCT FROM v_operation_row.frozen_source_session_version
           OR invalid_scope.source_snapshot_run_id IS DISTINCT FROM v_operation_row.frozen_source_snapshot_run_id
           OR UPPER(BTRIM(COALESCE(invalid_scope.status, ''))) IN ('FAILED', 'CANCELLED', 'CANCELED', 'ERROR')
         )),
      (SELECT md5(COALESCE(string_agg(
         hash_scope.candidate_id::text || ':' || hash_scope.pay_channel || ':' || hash_scope.scope_hash,
         '|' ORDER BY hash_scope.pay_channel, hash_scope.candidate_id
       ), ''))
       FROM public.banking_pay_operation_candidate_scope AS hash_scope
       WHERE hash_scope.operation_id = p_operation_id)
    INTO v_frozen_scope_count, v_frozen_selected_count, v_frozen_invalid_count, v_recomputed_scope_hash;

    IF v_frozen_scope_count <> v_operation_row.frozen_candidate_scope_count
       OR v_frozen_selected_count <> v_operation_row.frozen_selected_row_count
       OR v_frozen_invalid_count > 0
       OR v_recomputed_scope_hash IS DISTINCT FROM v_operation_row.frozen_operation_scope_hash THEN
      RAISE EXCEPTION 'DRAFT_CREATE_FROZEN_SCOPE_PROVENANCE_MISMATCH'
        USING ERRCODE = 'P0001';
    END IF;
  END IF;

  v_legacy_mode := p_units_json IS NOT NULL
    AND jsonb_array_length(v_units_json) > 0
    AND NOT (
      (v_operation_type = 'DRAFT_CREATE' AND v_chunk_type = 'CANDIDATE_SCOPE')
      OR (v_operation_type = 'REMITTANCE_QUEUE' AND v_chunk_type IN ('REMITTANCE', 'PAYOUT_NOTICE'))
    );

  IF v_legacy_mode AND jsonb_array_length(v_units_json) > 25 THEN
    RAISE EXCEPTION 'BANKING_PAY_OPERATION_SEED_CHUNKS_LEGACY_UNITS_TOO_LARGE'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_OPERATION_SEED_CHUNKS_LEGACY_UNITS_TOO_LARGE', 'operation_id', p_operation_id::text, 'chunk_type', v_chunk_type, 'unit_count', jsonb_array_length(v_units_json), 'limit', 25)::text;
  END IF;

  IF v_legacy_mode
     AND upper(btrim(coalesce(v_operation_row.operation_type, ''))) IN ('PAYMENT_EXECUTE', 'PAYMENT_RETRY_BLOCKED_FUNDS') THEN
    RAISE EXCEPTION 'BANKING_PAY_OPERATION_SEED_CHUNKS_LEGACY_MODE_DISALLOWED_FOR_EXECUTION'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_OPERATION_SEED_CHUNKS_LEGACY_MODE_DISALLOWED_FOR_EXECUTION', 'operation_id', p_operation_id::text, 'chunk_type', v_chunk_type)::text;
  END IF;

  DROP TABLE IF EXISTS pg_temp.tmp_operation_seed_units;
  CREATE TEMPORARY TABLE pg_temp.tmp_operation_seed_units (
    unit_ordinal bigint NOT NULL,
    candidate_scope_id uuid NULL,
    transfer_scope_id uuid NULL,
    remittance_scope_id uuid NULL,
    scope_unit_id uuid NULL,
    pay_bank_transfer_id uuid NULL,
    unit_key text NULL,
    unit_payload_json jsonb NOT NULL
  ) ON COMMIT DROP;

  IF v_operation_type = 'DRAFT_CREATE' AND v_chunk_type = 'CANDIDATE_SCOPE' THEN
    IF p_units_json IS NULL OR jsonb_array_length(v_units_json) = 0 THEN
      RAISE EXCEPTION 'DRAFT_CREATE_CHUNK_UNITS_REQUIRED'
        USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'DRAFT_CREATE_CHUNK_UNITS_REQUIRED', 'operation_id', p_operation_id::text, 'phase', v_phase, 'chunk_type', v_chunk_type)::text;
    END IF;

    IF jsonb_array_length(v_units_json) > 100 THEN
      RAISE EXCEPTION 'DRAFT_CREATE_CHUNK_UNITS_TOO_LARGE'
        USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'DRAFT_CREATE_CHUNK_UNITS_TOO_LARGE', 'operation_id', p_operation_id::text, 'phase', v_phase, 'chunk_type', v_chunk_type, 'unit_count', jsonb_array_length(v_units_json), 'limit', 100)::text;
    END IF;

    IF EXISTS (
      SELECT 1
      FROM jsonb_array_elements_text(v_units_json) AS supplied_candidate_scope(candidate_scope_id_text)
      WHERE BTRIM(supplied_candidate_scope.candidate_scope_id_text) !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    ) THEN
      RAISE EXCEPTION 'DRAFT_CREATE_CHUNK_UNITS_MUST_BE_UUIDS'
        USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'DRAFT_CREATE_CHUNK_UNITS_MUST_BE_UUIDS', 'operation_id', p_operation_id::text, 'phase', v_phase, 'chunk_type', v_chunk_type)::text;
    END IF;

    IF EXISTS (
      SELECT 1
      FROM jsonb_array_elements_text(v_units_json) AS supplied_candidate_scope(candidate_scope_id_text)
      WHERE NOT EXISTS (
        SELECT 1
        FROM public.banking_pay_operation_candidate_scope AS candidate_scope_row
        WHERE candidate_scope_row.operation_id = p_operation_id
          AND candidate_scope_row.id = BTRIM(supplied_candidate_scope.candidate_scope_id_text)::uuid
          AND upper(btrim(coalesce(candidate_scope_row.status, ''))) NOT IN ('FAILED', 'CANCELLED', 'CANCELED', 'ERROR')
      )
    ) THEN
      RAISE EXCEPTION 'DRAFT_CREATE_CHUNK_SCOPE_ID_MISMATCH'
        USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'DRAFT_CREATE_CHUNK_SCOPE_ID_MISMATCH', 'operation_id', p_operation_id::text, 'phase', v_phase, 'chunk_type', v_chunk_type)::text;
    END IF;

    INSERT INTO pg_temp.tmp_operation_seed_units (unit_ordinal, candidate_scope_id, unit_key, unit_payload_json)
    SELECT supplied_scope.ordinality::bigint,
           candidate_scope_row.id,
           candidate_scope_row.candidate_id::text || ':' || candidate_scope_row.pay_channel,
           jsonb_strip_nulls(jsonb_build_object(
             'candidate_scope_id', candidate_scope_row.id::text,
             'pay_batch_id', CASE WHEN candidate_scope_row.pay_batch_id IS NULL THEN NULL ELSE candidate_scope_row.pay_batch_id::text END,
             'candidate_id', candidate_scope_row.candidate_id::text,
             'pay_channel', candidate_scope_row.pay_channel,
             'chunk_sequence', candidate_scope_row.chunk_sequence,
             'status', candidate_scope_row.status
           ))
    FROM jsonb_array_elements_text(v_units_json) WITH ORDINALITY AS supplied_scope(candidate_scope_id_text, ordinality)
    JOIN public.banking_pay_operation_candidate_scope AS candidate_scope_row
      ON candidate_scope_row.operation_id = p_operation_id
     AND candidate_scope_row.id = BTRIM(supplied_scope.candidate_scope_id_text)::uuid
    ORDER BY supplied_scope.ordinality;
  ELSIF v_operation_type = 'REMITTANCE_QUEUE' AND v_chunk_type IN ('REMITTANCE', 'PAYOUT_NOTICE') THEN
    IF p_units_json IS NOT NULL AND jsonb_array_length(v_units_json) > 0 THEN
      IF EXISTS (
        SELECT 1
        FROM jsonb_array_elements_text(v_units_json) AS supplied_remittance_scope(remittance_scope_id_text)
        WHERE BTRIM(supplied_remittance_scope.remittance_scope_id_text) !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      ) THEN
        RAISE EXCEPTION 'REMITTANCE_CHUNK_UNITS_MUST_BE_UUIDS'
          USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'REMITTANCE_CHUNK_UNITS_MUST_BE_UUIDS', 'operation_id', p_operation_id::text, 'phase', v_phase, 'chunk_type', v_chunk_type)::text;
      END IF;

      IF EXISTS (
        SELECT 1
        FROM jsonb_array_elements_text(v_units_json) AS supplied_remittance_scope(remittance_scope_id_text)
        WHERE NOT EXISTS (
          SELECT 1
          FROM public.banking_pay_operation_remittance_scope AS remittance_scope_row
          WHERE remittance_scope_row.operation_id = p_operation_id
            AND remittance_scope_row.id = BTRIM(supplied_remittance_scope.remittance_scope_id_text)::uuid
            AND upper(btrim(coalesce(remittance_scope_row.status, ''))) IN ('PENDING', 'FAILED')
            AND (
              (
                v_chunk_type = 'PAYOUT_NOTICE'
                AND (
                  upper(btrim(coalesce(remittance_scope_row.remittance_type, ''))) LIKE '%PAYOUT%'
                  OR upper(btrim(coalesce(remittance_scope_row.remittance_type, ''))) LIKE '%NOTICE%'
                )
              )
              OR (
                v_chunk_type = 'REMITTANCE'
                AND upper(btrim(coalesce(remittance_scope_row.remittance_type, ''))) NOT LIKE '%PAYOUT%'
                AND upper(btrim(coalesce(remittance_scope_row.remittance_type, ''))) NOT LIKE '%NOTICE%'
              )
            )
        )
      ) THEN
        RAISE EXCEPTION 'REMITTANCE_CHUNK_SCOPE_ID_MISMATCH'
          USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'REMITTANCE_CHUNK_SCOPE_ID_MISMATCH', 'operation_id', p_operation_id::text, 'phase', v_phase, 'chunk_type', v_chunk_type)::text;
      END IF;

      INSERT INTO pg_temp.tmp_operation_seed_units (unit_ordinal, remittance_scope_id, unit_key, unit_payload_json)
      SELECT supplied_remittance_scope.ordinality::bigint,
             remittance_scope_row.id,
             remittance_scope_row.deterministic_outbox_key,
             jsonb_strip_nulls(jsonb_build_object(
               'remittance_scope_id', remittance_scope_row.id::text,
               'pay_batch_id', remittance_scope_row.pay_batch_id::text,
               'pay_batch_candidate_id', CASE WHEN remittance_scope_row.pay_batch_candidate_id IS NULL THEN NULL ELSE remittance_scope_row.pay_batch_candidate_id::text END,
               'candidate_id', CASE WHEN remittance_scope_row.candidate_id IS NULL THEN NULL ELSE remittance_scope_row.candidate_id::text END,
               'recipient_kind', remittance_scope_row.recipient_kind,
               'recipient_id', CASE WHEN remittance_scope_row.recipient_id IS NULL THEN NULL ELSE remittance_scope_row.recipient_id::text END,
               'remittance_type', remittance_scope_row.remittance_type,
               'status', remittance_scope_row.status,
               'deterministic_outbox_key', remittance_scope_row.deterministic_outbox_key,
               'outbox_id', CASE WHEN remittance_scope_row.outbox_id IS NULL THEN NULL ELSE remittance_scope_row.outbox_id::text END
             ))
      FROM jsonb_array_elements_text(v_units_json) WITH ORDINALITY AS supplied_remittance_scope(remittance_scope_id_text, ordinality)
      JOIN public.banking_pay_operation_remittance_scope AS remittance_scope_row
        ON remittance_scope_row.operation_id = p_operation_id
       AND remittance_scope_row.id = BTRIM(supplied_remittance_scope.remittance_scope_id_text)::uuid
      WHERE upper(btrim(coalesce(remittance_scope_row.status, ''))) IN ('PENDING', 'FAILED')
        AND (
          (
            v_chunk_type = 'PAYOUT_NOTICE'
            AND (
              upper(btrim(coalesce(remittance_scope_row.remittance_type, ''))) LIKE '%PAYOUT%'
              OR upper(btrim(coalesce(remittance_scope_row.remittance_type, ''))) LIKE '%NOTICE%'
            )
          )
          OR (
            v_chunk_type = 'REMITTANCE'
            AND upper(btrim(coalesce(remittance_scope_row.remittance_type, ''))) NOT LIKE '%PAYOUT%'
            AND upper(btrim(coalesce(remittance_scope_row.remittance_type, ''))) NOT LIKE '%NOTICE%'
          )
        )
      ORDER BY supplied_remittance_scope.ordinality;
    ELSE
      INSERT INTO pg_temp.tmp_operation_seed_units (unit_ordinal, remittance_scope_id, unit_key, unit_payload_json)
      SELECT row_number() OVER (ORDER BY remittance_scope_row.created_at_utc NULLS FIRST, remittance_scope_row.id)::bigint AS unit_ordinal,
             remittance_scope_row.id,
             remittance_scope_row.deterministic_outbox_key,
             jsonb_strip_nulls(jsonb_build_object(
               'remittance_scope_id', remittance_scope_row.id::text,
               'pay_batch_id', remittance_scope_row.pay_batch_id::text,
               'pay_batch_candidate_id', CASE WHEN remittance_scope_row.pay_batch_candidate_id IS NULL THEN NULL ELSE remittance_scope_row.pay_batch_candidate_id::text END,
               'candidate_id', CASE WHEN remittance_scope_row.candidate_id IS NULL THEN NULL ELSE remittance_scope_row.candidate_id::text END,
               'recipient_kind', remittance_scope_row.recipient_kind,
               'recipient_id', CASE WHEN remittance_scope_row.recipient_id IS NULL THEN NULL ELSE remittance_scope_row.recipient_id::text END,
               'remittance_type', remittance_scope_row.remittance_type,
               'status', remittance_scope_row.status,
               'deterministic_outbox_key', remittance_scope_row.deterministic_outbox_key,
               'outbox_id', CASE WHEN remittance_scope_row.outbox_id IS NULL THEN NULL ELSE remittance_scope_row.outbox_id::text END
             ))
      FROM public.banking_pay_operation_remittance_scope AS remittance_scope_row
      WHERE remittance_scope_row.operation_id = p_operation_id
        AND upper(btrim(coalesce(remittance_scope_row.status, ''))) IN ('PENDING', 'FAILED')
        AND (
          (
            v_chunk_type = 'PAYOUT_NOTICE'
            AND (
              upper(btrim(coalesce(remittance_scope_row.remittance_type, ''))) LIKE '%PAYOUT%'
              OR upper(btrim(coalesce(remittance_scope_row.remittance_type, ''))) LIKE '%NOTICE%'
            )
          )
          OR (
            v_chunk_type = 'REMITTANCE'
            AND upper(btrim(coalesce(remittance_scope_row.remittance_type, ''))) NOT LIKE '%PAYOUT%'
            AND upper(btrim(coalesce(remittance_scope_row.remittance_type, ''))) NOT LIKE '%NOTICE%'
          )
        )
      ORDER BY remittance_scope_row.created_at_utc NULLS FIRST, remittance_scope_row.id
      LIMIT 100;
    END IF;

  ELSIF v_legacy_mode THEN
    INSERT INTO pg_temp.tmp_operation_seed_units (unit_ordinal, unit_payload_json)
    SELECT unit_values.ordinality::bigint,
           jsonb_strip_nulls(jsonb_build_object('legacy_unit', unit_values.value, 'diagnostic_tiny_compat', true))
    FROM jsonb_array_elements(v_units_json) WITH ORDINALITY AS unit_values(value, ordinality)
    ORDER BY unit_values.ordinality;
  ELSIF v_chunk_type = 'FRESHNESS_VALIDATE' THEN
    INSERT INTO pg_temp.tmp_operation_seed_units (unit_ordinal, scope_unit_id, unit_key, unit_payload_json)
    SELECT scope_unit.unit_ordinal,
           scope_unit.id,
           scope_unit.unit_key,
           jsonb_build_object('scope_unit_id', scope_unit.id::text, 'unit_key', scope_unit.unit_key)
    FROM public.banking_pay_operation_scope_units AS scope_unit
    WHERE scope_unit.operation_id = p_operation_id
      AND scope_unit.phase = 'FRESHNESS'
      AND upper(btrim(coalesce(scope_unit.status, ''))) IN ('PENDING', 'QUEUED', 'READY', 'ERROR')
    ORDER BY scope_unit.unit_ordinal, scope_unit.id
    LIMIT 100;
  ELSIF v_chunk_type IN ('TRANSFER_GROUP', 'TRANSFER_SCOPE_ITEM_SEED', 'TRANSFER_SCOPE_ROLLUP') THEN
    INSERT INTO pg_temp.tmp_operation_seed_units (unit_ordinal, transfer_scope_id, pay_bank_transfer_id, unit_key, unit_payload_json)
    SELECT row_number() OVER (ORDER BY scope_row.created_at_utc NULLS FIRST, scope_row.id)::bigint AS unit_ordinal,
           scope_row.id,
           scope_row.pay_bank_transfer_id,
           scope_row.transfer_group_key,
           jsonb_strip_nulls(jsonb_build_object(
             'transfer_scope_id', scope_row.id::text,
             'pay_batch_id', scope_row.pay_batch_id::text,
             'pay_channel', scope_row.pay_channel,
             'transfer_group_key', scope_row.transfer_group_key,
             'pay_bank_transfer_id', CASE WHEN scope_row.pay_bank_transfer_id IS NULL THEN NULL ELSE scope_row.pay_bank_transfer_id::text END
           ))
    FROM public.banking_pay_operation_transfer_scope AS scope_row
    WHERE scope_row.operation_id = p_operation_id
      AND (
        (v_chunk_type = 'TRANSFER_GROUP' AND upper(btrim(coalesce(scope_row.status, ''))) IN ('PENDING', 'FAILED'))
        OR (v_chunk_type = 'TRANSFER_SCOPE_ITEM_SEED' AND upper(btrim(coalesce(scope_row.provider_submit_state, 'NOT_READY'))) IN ('', 'NOT_READY', 'REVIEW_REQUIRED'))
        OR (v_chunk_type = 'TRANSFER_SCOPE_ROLLUP' AND EXISTS (
          SELECT 1
          FROM public.banking_pay_operation_transfer_scope_items AS scope_item
          WHERE scope_item.operation_id = p_operation_id
            AND scope_item.transfer_scope_id = scope_row.id
            AND upper(btrim(coalesce(scope_item.rollup_status, ''))) IN ('PENDING', 'ERROR')
        ))
      )
    ORDER BY scope_row.created_at_utc NULLS FIRST, scope_row.id
    LIMIT 100;
  ELSIF v_chunk_type = 'TRANSFER_SUBMIT' THEN
    INSERT INTO pg_temp.tmp_operation_seed_units (unit_ordinal, transfer_scope_id, pay_bank_transfer_id, unit_key, unit_payload_json)
    SELECT row_number() OVER (ORDER BY scope_row.updated_at_utc NULLS FIRST, scope_row.id)::bigint AS unit_ordinal,
           scope_row.id,
           scope_row.pay_bank_transfer_id,
           scope_row.transfer_group_key,
           jsonb_strip_nulls(jsonb_build_object(
             'transfer_scope_id', scope_row.id::text,
             'pay_bank_transfer_id', CASE WHEN scope_row.pay_bank_transfer_id IS NULL THEN NULL ELSE scope_row.pay_bank_transfer_id::text END,
             'provider_idempotency_key', scope_row.provider_idempotency_key,
             'provider_request_id', scope_row.provider_request_id
           ))
    FROM public.banking_pay_operation_transfer_scope AS scope_row
    WHERE scope_row.operation_id = p_operation_id
      AND upper(btrim(coalesce(scope_row.status, ''))) = 'PREPARED'
      AND coalesce(scope_row.provider_submit_ready, false) = true
      AND upper(btrim(coalesce(scope_row.provider_submit_state, ''))) = 'READY'
      AND scope_row.pay_bank_transfer_id IS NOT NULL
    ORDER BY scope_row.updated_at_utc NULLS FIRST, scope_row.id
    LIMIT 100;
  ELSE
    RETURN QUERY SELECT 0::integer, 0::integer, 0::integer, 0::integer;
    RETURN;
  END IF;

  SELECT count(*)::integer
  INTO v_total_units
  FROM pg_temp.tmp_operation_seed_units AS seed_unit;

  IF v_total_units <= 0 THEN
    RETURN QUERY SELECT 0::integer, 0::integer, 0::integer, 0::integer;
    RETURN;
  END IF;

  v_chunk_count := ((v_total_units + v_chunk_size - 1) / v_chunk_size)::integer;

  WITH expected_chunks AS (
    SELECT (((seed_unit.unit_ordinal - 1) / v_chunk_size) + 1)::integer AS sequence_no,
           jsonb_strip_nulls(jsonb_build_object(
             'row_backed', NOT v_legacy_mode,
             'legacy_tiny_compat', v_legacy_mode,
             'source_table', CASE
               WHEN v_operation_type = 'DRAFT_CREATE' AND v_chunk_type = 'CANDIDATE_SCOPE' THEN 'banking_pay_operation_candidate_scope'
               WHEN v_operation_type = 'REMITTANCE_QUEUE' AND v_chunk_type IN ('REMITTANCE', 'PAYOUT_NOTICE') THEN 'banking_pay_operation_remittance_scope'
               WHEN v_chunk_type = 'FRESHNESS_VALIDATE' THEN 'banking_pay_operation_scope_units'
               WHEN v_chunk_type IN ('TRANSFER_GROUP', 'TRANSFER_SCOPE_ITEM_SEED', 'TRANSFER_SCOPE_ROLLUP', 'TRANSFER_SUBMIT') THEN 'banking_pay_operation_transfer_scope'
               ELSE 'diagnostic_legacy_units'
             END,
             'unit_count', count(*)::integer,
             'unit_ordinal_min', min(seed_unit.unit_ordinal),
             'unit_ordinal_max', max(seed_unit.unit_ordinal),
             'units', CASE
               WHEN v_operation_type = 'DRAFT_CREATE' AND v_chunk_type = 'CANDIDATE_SCOPE' THEN coalesce(jsonb_agg(to_jsonb(seed_unit.candidate_scope_id::text) ORDER BY seed_unit.unit_ordinal) FILTER (WHERE seed_unit.candidate_scope_id IS NOT NULL), '[]'::jsonb)
               WHEN v_operation_type = 'REMITTANCE_QUEUE' AND v_chunk_type IN ('REMITTANCE', 'PAYOUT_NOTICE') THEN coalesce(jsonb_agg(to_jsonb(seed_unit.remittance_scope_id::text) ORDER BY seed_unit.unit_ordinal) FILTER (WHERE seed_unit.remittance_scope_id IS NOT NULL), '[]'::jsonb)
               ELSE '[]'::jsonb
             END,
             'candidate_scope_ids', CASE
               WHEN v_operation_type = 'DRAFT_CREATE' AND v_chunk_type = 'CANDIDATE_SCOPE' THEN coalesce(jsonb_agg(to_jsonb(seed_unit.candidate_scope_id::text) ORDER BY seed_unit.unit_ordinal) FILTER (WHERE seed_unit.candidate_scope_id IS NOT NULL), '[]'::jsonb)
               ELSE '[]'::jsonb
             END,
             'pay_batch_ids', coalesce(jsonb_agg(DISTINCT to_jsonb((seed_unit.unit_payload_json->>'pay_batch_id'))) FILTER (WHERE NULLIF(seed_unit.unit_payload_json->>'pay_batch_id', '') IS NOT NULL), '[]'::jsonb),
             'transfer_scope_ids', coalesce(jsonb_agg(to_jsonb(seed_unit.transfer_scope_id::text) ORDER BY seed_unit.unit_ordinal) FILTER (WHERE seed_unit.transfer_scope_id IS NOT NULL), '[]'::jsonb),
             'remittance_scope_ids', CASE
               WHEN v_operation_type = 'REMITTANCE_QUEUE' AND v_chunk_type IN ('REMITTANCE', 'PAYOUT_NOTICE') THEN coalesce(jsonb_agg(to_jsonb(seed_unit.remittance_scope_id::text) ORDER BY seed_unit.unit_ordinal) FILTER (WHERE seed_unit.remittance_scope_id IS NOT NULL), '[]'::jsonb)
               ELSE '[]'::jsonb
             END,
             'scope_unit_ids', CASE
               WHEN v_operation_type = 'DRAFT_CREATE' AND v_chunk_type = 'CANDIDATE_SCOPE' THEN coalesce(jsonb_agg(to_jsonb(seed_unit.candidate_scope_id::text) ORDER BY seed_unit.unit_ordinal) FILTER (WHERE seed_unit.candidate_scope_id IS NOT NULL), '[]'::jsonb)
               WHEN v_operation_type = 'REMITTANCE_QUEUE' AND v_chunk_type IN ('REMITTANCE', 'PAYOUT_NOTICE') THEN coalesce(jsonb_agg(to_jsonb(seed_unit.remittance_scope_id::text) ORDER BY seed_unit.unit_ordinal) FILTER (WHERE seed_unit.remittance_scope_id IS NOT NULL), '[]'::jsonb)
               ELSE coalesce(jsonb_agg(to_jsonb(seed_unit.scope_unit_id::text) ORDER BY seed_unit.unit_ordinal) FILTER (WHERE seed_unit.scope_unit_id IS NOT NULL), '[]'::jsonb)
             END,
             'unit_keys_sample', coalesce(jsonb_agg(to_jsonb(seed_unit.unit_key) ORDER BY seed_unit.unit_ordinal) FILTER (WHERE seed_unit.unit_key IS NOT NULL), '[]'::jsonb)
           )) AS payload_json,
           count(*)::integer AS unit_count
    FROM pg_temp.tmp_operation_seed_units AS seed_unit
    GROUP BY (((seed_unit.unit_ordinal - 1) / v_chunk_size) + 1)::integer
  ), repaired_chunks AS (
    UPDATE public.banking_pay_operation_chunks AS chunk_update
    SET status = 'PENDING',
        payload_json = expected_chunks.payload_json,
        result_json = NULL::jsonb,
        error_json = NULL::jsonb,
        unit_count = expected_chunks.unit_count,
        completed_count = 0,
        failed_count = 0,
        locked_by = NULL,
        lock_expires_at_utc = NULL,
        started_at_utc = NULL,
        completed_at_utc = NULL,
        updated_at_utc = v_now
    FROM expected_chunks
    WHERE chunk_update.operation_id = p_operation_id
      AND chunk_update.phase = v_phase
      AND chunk_update.chunk_type = v_chunk_type
      AND chunk_update.sequence_no = expected_chunks.sequence_no
      AND v_operation_type = 'REMITTANCE_QUEUE'
      AND v_chunk_type IN ('REMITTANCE', 'PAYOUT_NOTICE')
      AND chunk_update.status IN ('PENDING', 'FAILED')
      AND (
        chunk_update.unit_count <> expected_chunks.unit_count
        OR chunk_update.payload_json IS DISTINCT FROM expected_chunks.payload_json
      )
      AND (
        chunk_update.payload_json->>'source_table' = 'diagnostic_legacy_units'
        OR lower(coalesce(chunk_update.payload_json->>'row_backed', 'false')) <> 'true'
        OR lower(coalesce(chunk_update.payload_json->>'legacy_tiny_compat', 'false')) = 'true'
      )
    RETURNING 1
  )
  SELECT count(*)::integer
  INTO v_repaired_chunk_count
  FROM repaired_chunks;

  WITH expected_chunks AS (
    SELECT (((seed_unit.unit_ordinal - 1) / v_chunk_size) + 1)::integer AS sequence_no,
           jsonb_strip_nulls(jsonb_build_object(
             'row_backed', NOT v_legacy_mode,
             'legacy_tiny_compat', v_legacy_mode,
             'source_table', CASE
               WHEN v_operation_type = 'DRAFT_CREATE' AND v_chunk_type = 'CANDIDATE_SCOPE' THEN 'banking_pay_operation_candidate_scope'
               WHEN v_operation_type = 'REMITTANCE_QUEUE' AND v_chunk_type IN ('REMITTANCE', 'PAYOUT_NOTICE') THEN 'banking_pay_operation_remittance_scope'
               WHEN v_chunk_type = 'FRESHNESS_VALIDATE' THEN 'banking_pay_operation_scope_units'
               WHEN v_chunk_type IN ('TRANSFER_GROUP', 'TRANSFER_SCOPE_ITEM_SEED', 'TRANSFER_SCOPE_ROLLUP', 'TRANSFER_SUBMIT') THEN 'banking_pay_operation_transfer_scope'
               ELSE 'diagnostic_legacy_units'
             END,
             'unit_count', count(*)::integer,
             'unit_ordinal_min', min(seed_unit.unit_ordinal),
             'unit_ordinal_max', max(seed_unit.unit_ordinal),
             'units', CASE
               WHEN v_operation_type = 'DRAFT_CREATE' AND v_chunk_type = 'CANDIDATE_SCOPE' THEN coalesce(jsonb_agg(to_jsonb(seed_unit.candidate_scope_id::text) ORDER BY seed_unit.unit_ordinal) FILTER (WHERE seed_unit.candidate_scope_id IS NOT NULL), '[]'::jsonb)
               WHEN v_operation_type = 'REMITTANCE_QUEUE' AND v_chunk_type IN ('REMITTANCE', 'PAYOUT_NOTICE') THEN coalesce(jsonb_agg(to_jsonb(seed_unit.remittance_scope_id::text) ORDER BY seed_unit.unit_ordinal) FILTER (WHERE seed_unit.remittance_scope_id IS NOT NULL), '[]'::jsonb)
               ELSE '[]'::jsonb
             END,
             'candidate_scope_ids', CASE
               WHEN v_operation_type = 'DRAFT_CREATE' AND v_chunk_type = 'CANDIDATE_SCOPE' THEN coalesce(jsonb_agg(to_jsonb(seed_unit.candidate_scope_id::text) ORDER BY seed_unit.unit_ordinal) FILTER (WHERE seed_unit.candidate_scope_id IS NOT NULL), '[]'::jsonb)
               ELSE '[]'::jsonb
             END,
             'pay_batch_ids', coalesce(jsonb_agg(DISTINCT to_jsonb((seed_unit.unit_payload_json->>'pay_batch_id'))) FILTER (WHERE NULLIF(seed_unit.unit_payload_json->>'pay_batch_id', '') IS NOT NULL), '[]'::jsonb),
             'transfer_scope_ids', coalesce(jsonb_agg(to_jsonb(seed_unit.transfer_scope_id::text) ORDER BY seed_unit.unit_ordinal) FILTER (WHERE seed_unit.transfer_scope_id IS NOT NULL), '[]'::jsonb),
             'remittance_scope_ids', CASE
               WHEN v_operation_type = 'REMITTANCE_QUEUE' AND v_chunk_type IN ('REMITTANCE', 'PAYOUT_NOTICE') THEN coalesce(jsonb_agg(to_jsonb(seed_unit.remittance_scope_id::text) ORDER BY seed_unit.unit_ordinal) FILTER (WHERE seed_unit.remittance_scope_id IS NOT NULL), '[]'::jsonb)
               ELSE '[]'::jsonb
             END,
             'scope_unit_ids', CASE
               WHEN v_operation_type = 'DRAFT_CREATE' AND v_chunk_type = 'CANDIDATE_SCOPE' THEN coalesce(jsonb_agg(to_jsonb(seed_unit.candidate_scope_id::text) ORDER BY seed_unit.unit_ordinal) FILTER (WHERE seed_unit.candidate_scope_id IS NOT NULL), '[]'::jsonb)
               WHEN v_operation_type = 'REMITTANCE_QUEUE' AND v_chunk_type IN ('REMITTANCE', 'PAYOUT_NOTICE') THEN coalesce(jsonb_agg(to_jsonb(seed_unit.remittance_scope_id::text) ORDER BY seed_unit.unit_ordinal) FILTER (WHERE seed_unit.remittance_scope_id IS NOT NULL), '[]'::jsonb)
               ELSE coalesce(jsonb_agg(to_jsonb(seed_unit.scope_unit_id::text) ORDER BY seed_unit.unit_ordinal) FILTER (WHERE seed_unit.scope_unit_id IS NOT NULL), '[]'::jsonb)
             END,
             'unit_keys_sample', coalesce(jsonb_agg(to_jsonb(seed_unit.unit_key) ORDER BY seed_unit.unit_ordinal) FILTER (WHERE seed_unit.unit_key IS NOT NULL), '[]'::jsonb)
           )) AS payload_json,
           count(*)::integer AS unit_count
    FROM pg_temp.tmp_operation_seed_units AS seed_unit
    GROUP BY (((seed_unit.unit_ordinal - 1) / v_chunk_size) + 1)::integer
  )
  SELECT count(*)::integer
  INTO v_mismatch_count
  FROM expected_chunks AS expected_chunk
  JOIN public.banking_pay_operation_chunks AS existing_chunk
    ON existing_chunk.operation_id = p_operation_id
   AND existing_chunk.phase = v_phase
   AND existing_chunk.chunk_type = v_chunk_type
   AND existing_chunk.sequence_no = expected_chunk.sequence_no
  WHERE existing_chunk.unit_count <> expected_chunk.unit_count
     OR existing_chunk.payload_json IS DISTINCT FROM expected_chunk.payload_json;

  IF coalesce(v_mismatch_count, 0) > 0 THEN
    RAISE EXCEPTION 'CHUNK_SCOPE_MISMATCH'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'CHUNK_SCOPE_MISMATCH', 'operation_id', p_operation_id::text, 'phase', v_phase, 'chunk_type', v_chunk_type, 'mismatched_chunk_count', v_mismatch_count)::text;
  END IF;

  WITH expected_chunks AS (
    SELECT (((seed_unit.unit_ordinal - 1) / v_chunk_size) + 1)::integer AS sequence_no,
           jsonb_strip_nulls(jsonb_build_object(
             'row_backed', NOT v_legacy_mode,
             'legacy_tiny_compat', v_legacy_mode,
             'source_table', CASE
               WHEN v_operation_type = 'DRAFT_CREATE' AND v_chunk_type = 'CANDIDATE_SCOPE' THEN 'banking_pay_operation_candidate_scope'
               WHEN v_operation_type = 'REMITTANCE_QUEUE' AND v_chunk_type IN ('REMITTANCE', 'PAYOUT_NOTICE') THEN 'banking_pay_operation_remittance_scope'
               WHEN v_chunk_type = 'FRESHNESS_VALIDATE' THEN 'banking_pay_operation_scope_units'
               WHEN v_chunk_type IN ('TRANSFER_GROUP', 'TRANSFER_SCOPE_ITEM_SEED', 'TRANSFER_SCOPE_ROLLUP', 'TRANSFER_SUBMIT') THEN 'banking_pay_operation_transfer_scope'
               ELSE 'diagnostic_legacy_units'
             END,
             'unit_count', count(*)::integer,
             'unit_ordinal_min', min(seed_unit.unit_ordinal),
             'unit_ordinal_max', max(seed_unit.unit_ordinal),
             'units', CASE
               WHEN v_operation_type = 'DRAFT_CREATE' AND v_chunk_type = 'CANDIDATE_SCOPE' THEN coalesce(jsonb_agg(to_jsonb(seed_unit.candidate_scope_id::text) ORDER BY seed_unit.unit_ordinal) FILTER (WHERE seed_unit.candidate_scope_id IS NOT NULL), '[]'::jsonb)
               WHEN v_operation_type = 'REMITTANCE_QUEUE' AND v_chunk_type IN ('REMITTANCE', 'PAYOUT_NOTICE') THEN coalesce(jsonb_agg(to_jsonb(seed_unit.remittance_scope_id::text) ORDER BY seed_unit.unit_ordinal) FILTER (WHERE seed_unit.remittance_scope_id IS NOT NULL), '[]'::jsonb)
               ELSE '[]'::jsonb
             END,
             'candidate_scope_ids', CASE
               WHEN v_operation_type = 'DRAFT_CREATE' AND v_chunk_type = 'CANDIDATE_SCOPE' THEN coalesce(jsonb_agg(to_jsonb(seed_unit.candidate_scope_id::text) ORDER BY seed_unit.unit_ordinal) FILTER (WHERE seed_unit.candidate_scope_id IS NOT NULL), '[]'::jsonb)
               ELSE '[]'::jsonb
             END,
             'pay_batch_ids', coalesce(jsonb_agg(DISTINCT to_jsonb((seed_unit.unit_payload_json->>'pay_batch_id'))) FILTER (WHERE NULLIF(seed_unit.unit_payload_json->>'pay_batch_id', '') IS NOT NULL), '[]'::jsonb),
             'transfer_scope_ids', coalesce(jsonb_agg(to_jsonb(seed_unit.transfer_scope_id::text) ORDER BY seed_unit.unit_ordinal) FILTER (WHERE seed_unit.transfer_scope_id IS NOT NULL), '[]'::jsonb),
             'remittance_scope_ids', CASE
               WHEN v_operation_type = 'REMITTANCE_QUEUE' AND v_chunk_type IN ('REMITTANCE', 'PAYOUT_NOTICE') THEN coalesce(jsonb_agg(to_jsonb(seed_unit.remittance_scope_id::text) ORDER BY seed_unit.unit_ordinal) FILTER (WHERE seed_unit.remittance_scope_id IS NOT NULL), '[]'::jsonb)
               ELSE '[]'::jsonb
             END,
             'scope_unit_ids', CASE
               WHEN v_operation_type = 'DRAFT_CREATE' AND v_chunk_type = 'CANDIDATE_SCOPE' THEN coalesce(jsonb_agg(to_jsonb(seed_unit.candidate_scope_id::text) ORDER BY seed_unit.unit_ordinal) FILTER (WHERE seed_unit.candidate_scope_id IS NOT NULL), '[]'::jsonb)
               WHEN v_operation_type = 'REMITTANCE_QUEUE' AND v_chunk_type IN ('REMITTANCE', 'PAYOUT_NOTICE') THEN coalesce(jsonb_agg(to_jsonb(seed_unit.remittance_scope_id::text) ORDER BY seed_unit.unit_ordinal) FILTER (WHERE seed_unit.remittance_scope_id IS NOT NULL), '[]'::jsonb)
               ELSE coalesce(jsonb_agg(to_jsonb(seed_unit.scope_unit_id::text) ORDER BY seed_unit.unit_ordinal) FILTER (WHERE seed_unit.scope_unit_id IS NOT NULL), '[]'::jsonb)
             END,
             'unit_keys_sample', coalesce(jsonb_agg(to_jsonb(seed_unit.unit_key) ORDER BY seed_unit.unit_ordinal) FILTER (WHERE seed_unit.unit_key IS NOT NULL), '[]'::jsonb)
           )) AS payload_json,
           count(*)::integer AS unit_count
    FROM pg_temp.tmp_operation_seed_units AS seed_unit
    GROUP BY (((seed_unit.unit_ordinal - 1) / v_chunk_size) + 1)::integer
  ), inserted_chunks AS (
    INSERT INTO public.banking_pay_operation_chunks (
      operation_id,
      phase,
      chunk_type,
      sequence_no,
      status,
      payload_json,
      result_json,
      error_json,
      unit_count,
      completed_count,
      failed_count,
      created_at_utc,
      updated_at_utc
    )
    SELECT p_operation_id,
           v_phase,
           v_chunk_type,
           expected_chunks.sequence_no,
           'PENDING',
           expected_chunks.payload_json,
           NULL::jsonb,
           NULL::jsonb,
           expected_chunks.unit_count,
           0,
           0,
           v_now,
           v_now
    FROM expected_chunks
    ORDER BY expected_chunks.sequence_no
    ON CONFLICT (operation_id, phase, chunk_type, sequence_no) DO NOTHING
    RETURNING 1
  )
  SELECT count(*)::integer
  INTO v_new_chunk_count
  FROM inserted_chunks;

  v_existing_chunk_count := v_chunk_count - coalesce(v_new_chunk_count, 0);

  UPDATE public.banking_pay_operations AS operation_update
  SET chunk_count = GREATEST(COALESCE(operation_update.chunk_count, 0), v_chunk_count),
      total_units = GREATEST(COALESCE(operation_update.total_units, 0), v_total_units),
      progress_json = jsonb_strip_nulls(COALESCE(operation_update.progress_json, '{}'::jsonb) || jsonb_build_object(
        'last_chunk_seed', jsonb_build_object(
          'phase', v_phase,
          'chunk_type', v_chunk_type,
          'seeded_at_utc', v_now::text,
          'total_units', v_total_units,
          'chunk_count', v_chunk_count,
          'new_chunk_count', COALESCE(v_new_chunk_count, 0),
          'repaired_chunk_count', COALESCE(v_repaired_chunk_count, 0),
          'row_backed', NOT v_legacy_mode,
          'source_table', CASE
            WHEN v_operation_type = 'DRAFT_CREATE' AND v_chunk_type = 'CANDIDATE_SCOPE' THEN 'banking_pay_operation_candidate_scope'
            WHEN v_operation_type = 'REMITTANCE_QUEUE' AND v_chunk_type IN ('REMITTANCE', 'PAYOUT_NOTICE') THEN 'banking_pay_operation_remittance_scope'
            WHEN v_chunk_type = 'FRESHNESS_VALIDATE' THEN 'banking_pay_operation_scope_units'
            WHEN v_chunk_type IN ('TRANSFER_GROUP', 'TRANSFER_SCOPE_ITEM_SEED', 'TRANSFER_SCOPE_ROLLUP', 'TRANSFER_SUBMIT') THEN 'banking_pay_operation_transfer_scope'
            ELSE 'diagnostic_legacy_units'
          END
        )
      )),
      updated_at_utc = v_now
  WHERE operation_update.id = p_operation_id;

  RETURN QUERY SELECT v_total_units, v_chunk_count, v_existing_chunk_count, coalesce(v_new_chunk_count, 0);
END;
$function$;

