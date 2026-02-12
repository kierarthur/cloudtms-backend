create or replace function public.weekly_import_phase2(
  p_import_id uuid,
  p_system_type text
)
returns table (
  hr_row_id         uuid,
  external_row_key  text,
  work_date         date,
  incoming_code     text,
  candidate_id      uuid,
  client_id         uuid,
  week_ending_date  date,
  contract_id       uuid,
  action            text,
  reason            text,

  -- ✅ NEW: existing-match + diff fields (for preview classification)
  matched_shift_id     uuid,
  old_start_utc        timestamptz,
  old_end_utc          timestamptz,
  old_break_mins       int,
  old_paid_minutes     int,
  old_cancelled_at_utc timestamptz,

  new_start_utc       timestamptz,
  new_end_utc         timestamptz,
  new_break_mins      int,
  new_paid_minutes    int,

  delta_paid_minutes  int,
  is_new              boolean,
  is_noop             boolean,
  is_changed          boolean
)
language plpgsql
as $$
declare
  v_sys text := upper(trim(coalesce(p_system_type,'')));
  v_src public.hr_source_enum;

  -- debug (invoice_debug-gated)
  v_invoice_debug boolean := false;
  v_dbg jsonb := null;
  v_rows_out int := 0;

  v_sqlstate text;
  v_err text;
begin
  if v_sys not in ('NHSP','HR_WEEKLY') then
    raise exception 'weekly_import_phase2: invalid p_system_type=% (expected NHSP or HR_WEEKLY)', p_system_type;
  end if;

  v_src :=
    case
      when v_sys = 'NHSP' then 'NHSP'::public.hr_source_enum
      else 'HEALTHROSTER'::public.hr_source_enum
    end;

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

  return query
  with imp as (
    select
      hi.id,
      hi.source_system,
      hi.client_id as hr_client_id
    from public.hr_imports hi
    where hi.id = p_import_id
    limit 1
  ),
  raw as (
    select
      r.id as hr_row_id,
      r.external_row_key,
      r.date_local as work_date,

      -- staff_name
      coalesce(
        nullif((r.payload_json ->> 'staff_name'), ''),
        nullif((r.payload_json ->> 'worker_name'), ''),
        nullif((r.payload_json ->> 'name'), ''),
        nullif(r.staff_raw, ''),
        nullif(r.staff_norm, '')
      ) as staff_name,

      -- ward/unit (for display / debugging only)
      coalesce(
        nullif((r.payload_json ->> 'ward'), ''),
        nullif((r.payload_json ->> 'unit'), ''),
        nullif(r.unit_hint, ''),
        nullif(r.unit_raw, '')
      ) as ward,

      -- trust_raw (NHSP only)
      coalesce(
        nullif((r.payload_json ->> 'trust'), ''),
        nullif((r.payload_json ->> 'hospital_or_trust'), ''),
        nullif(r.unit_raw, '')
      ) as trust_raw,

      -- ✅ minute-truncated new values (stable comparisons)
      date_trunc('minute', (r.payload_json ->> 'start_utc')::timestamptz) as new_start_utc,
      date_trunc('minute', (r.payload_json ->> 'end_utc')::timestamptz)   as new_end_utc,

      -- ✅ FIX: HealthRoster uses Actual Break as authoritative (priority: actual_break_* then break_* then 0)
      greatest(
        0,
        coalesce(
          nullif(btrim(coalesce(r.payload_json ->> 'actual_break_mins','')), '')::int,
          nullif(btrim(coalesce(r.payload_json ->> 'actual_break_minutes','')), '')::int,
          nullif(btrim(coalesce(r.payload_json ->> 'break_mins','')), '')::int,
          nullif(btrim(coalesce(r.payload_json ->> 'break_minutes','')), '')::int,
          0
        )
      ) as new_break_mins,

      greatest(
        0,
        (floor(extract(epoch from (
          date_trunc('minute', (r.payload_json ->> 'end_utc')::timestamptz)
          -
          date_trunc('minute', (r.payload_json ->> 'start_utc')::timestamptz)
        )) / 60.0))::int
        -
        greatest(
          0,
          coalesce(
            nullif(btrim(coalesce(r.payload_json ->> 'actual_break_mins','')), '')::int,
            nullif(btrim(coalesce(r.payload_json ->> 'actual_break_minutes','')), '')::int,
            nullif(btrim(coalesce(r.payload_json ->> 'break_mins','')), '')::int,
            nullif(btrim(coalesce(r.payload_json ->> 'break_minutes','')), '')::int,
            0
          )
        )
      ) as new_paid_minutes,

      -- incoming_code depends on system_type:
      case
        when v_sys = 'NHSP' then coalesce(
          nullif((r.payload_json ->> 'assignment_code'), ''),
          nullif((r.payload_json ->> 'assignment'), ''),
          nullif((r.payload_json ->> 'Request_Grade'), ''),
          nullif(r.assignment_grade_norm, '')
        )
        else coalesce(
          nullif((r.payload_json ->> 'grade_raw'), ''),
          nullif((r.payload_json ->> 'Grade'), ''),
          nullif((r.payload_json ->> 'Request_Grade'), ''),
          nullif(r.assignment_grade_norm, '')
        )
      end as incoming_code_raw

    from public.hr_rows r
    join imp
      on imp.id = r.import_id
    where r.import_id = p_import_id
      and r.date_local is not null
      and (r.payload_json ->> 'start_utc') is not null
      and (r.payload_json ->> 'end_utc')   is not null
  ),
  resolved_ids as (
    select
      src.*,
      n.staff_lc,
      n.staff_key,
      n.trust_lc,
      n.trust_key,

      -- candidate mapping precedence:
      coalesce(
        cand_alias.id,
        cand_map.candidate_id,
        cand_exact_unique.candidate_id
      ) as candidate_id,

      -- client mapping:
      case
        when v_sys = 'NHSP' then coalesce(cli_alias.client_id, cli_name.client_id)
        else imp.hr_client_id
      end as client_id

    from raw src
    join imp on true

    cross join lateral (
      select
        nullif(lower(trim(coalesce(src.staff_name,''))), '') as staff_lc,
        nullif(regexp_replace(lower(coalesce(src.staff_name,'')), '[^a-z0-9]+', '', 'g'), '') as staff_key,
        nullif(lower(trim(coalesce(src.trust_raw,''))), '') as trust_lc,
        nullif(regexp_replace(lower(coalesce(src.trust_raw,'')), '[^a-z0-9]+', '', 'g'), '') as trust_key
    ) n

    left join lateral (
      select c.id
      from public.candidates c
      where c.nhsp_hr_name_aliases is not null
        and (
          (n.staff_lc  is not null and c.nhsp_hr_name_aliases @> to_jsonb(array[n.staff_lc]::text[]))
          or
          (n.staff_key is not null and c.nhsp_hr_name_aliases @> to_jsonb(array[n.staff_key]::text[]))
        )
      limit 1
    ) cand_alias on true

    left join lateral (
      select hm.candidate_id
      from public.hr_name_mappings hm
      where hm.active = true
        and (
          (n.staff_lc  is not null and hm.hr_name_norm = n.staff_lc)
          or
          (n.staff_key is not null and hm.hr_name_norm = n.staff_key)
        )
      order by hm.created_at desc
      limit 1
    ) cand_map on (cand_alias.id is null)

    left join lateral (
      with matches as (
        select c.id as cid
        from public.candidates c
        where c.active = true
          and n.staff_key is not null
          and (
            regexp_replace(lower(coalesce(c.first_name,'') || coalesce(c.last_name,'')), '[^a-z0-9]+', '', 'g') = n.staff_key
            or
            regexp_replace(lower(coalesce(c.last_name,'')  || coalesce(c.first_name,'')), '[^a-z0-9]+', '', 'g') = n.staff_key
          )
      )
      select
        case
          when count(*) = 1
            then (array_agg(cid order by cid::text))[1]
        end as candidate_id
      from matches
    ) cand_exact_unique on (cand_alias.id is null and cand_map.candidate_id is null)

    left join lateral (
      select ch.client_id
      from public.client_hospitals ch
      where v_sys = 'NHSP'
        and ch.hospital_name_norm is not null
        and (
          (n.trust_lc  is not null and ch.hospital_name_norm @> to_jsonb(array[n.trust_lc]::text[]))
          or
          (n.trust_key is not null and ch.hospital_name_norm @> to_jsonb(array[n.trust_key]::text[]))
        )
      limit 1
    ) cli_alias on (v_sys = 'NHSP')

    left join lateral (
      with matches as (
        select cl.id as clid
        from public.clients cl
        where v_sys = 'NHSP'
          and n.trust_key is not null
          and regexp_replace(lower(coalesce(cl.name,'')), '[^a-z0-9]+', '', 'g') = n.trust_key
      )
      select
        case
          when count(*) = 1
            then (array_agg(clid order by clid::text))[1]
        end as client_id
      from matches
    ) cli_name on (v_sys = 'NHSP' and cli_alias.client_id is null)
  ),
  with_we as (
    select
      r.*,

      coalesce(cs.week_ending_weekday, 0)::int as we_dow,

      (r.work_date
        + (
            (coalesce(cs.week_ending_weekday, 0)::int - extract(dow from r.work_date)::int + 7) % 7
          )
      )::date as week_ending_date,

      lower(trim(coalesce(r.incoming_code_raw,''))) as code_norm

    from resolved_ids r
    left join lateral (
      select cs.week_ending_weekday
      from public.client_settings cs
      where cs.client_id = r.client_id
      order by cs.effective_from desc, cs.created_at desc
      limit 1
    ) cs on true
  ),
  in_range_counts as (
    select
      w.*,
      coalesce(cr.in_range_count, 0) as in_range_count
    from with_we w
    left join lateral (
      select count(*)::int as in_range_count
      from public.contracts c
      where c.candidate_id = w.candidate_id
        and c.client_id    = w.client_id
        and c.start_date <= w.work_date
        and (c.end_date is null or c.end_date >= w.work_date)
    ) cr on true
  ),
  chosen_maps as (
    select
      w.*,
      m.spec as map_spec,
      m.patterns as band_patterns
    from in_range_counts w
    left join lateral (
      with maps as (
        -- ✅ NEW: candidate + client mapping (highest precedence)
        select abm.band_match_pattern, 3 as spec
        from public.assignment_band_mappings abm
        where abm.active = true
          and upper(trim(abm.system_type)) = v_sys
          and lower(trim(abm.incoming_code)) = w.code_norm
          and w.candidate_id is not null
          and w.client_id is not null
          and abm.candidate_id = w.candidate_id
          and abm.client_id = w.client_id

        union all
        -- candidate-only
        select abm.band_match_pattern, 2 as spec
        from public.assignment_band_mappings abm
        where abm.active = true
          and upper(trim(abm.system_type)) = v_sys
          and lower(trim(abm.incoming_code)) = w.code_norm
          and w.candidate_id is not null
          and abm.candidate_id = w.candidate_id
          and abm.client_id is null

        union all
        -- client-only
        select abm.band_match_pattern, 1 as spec
        from public.assignment_band_mappings abm
        where abm.active = true
          and upper(trim(abm.system_type)) = v_sys
          and lower(trim(abm.incoming_code)) = w.code_norm
          and w.client_id is not null
          and abm.candidate_id is null
          and abm.client_id = w.client_id

        union all
        -- global
        select abm.band_match_pattern, 0 as spec
        from public.assignment_band_mappings abm
        where abm.active = true
          and upper(trim(abm.system_type)) = v_sys
          and lower(trim(abm.incoming_code)) = w.code_norm
          and abm.candidate_id is null
          and abm.client_id is null
      ),
      mx as (select max(maps.spec) as m from maps)
      select
        (select mx.m from mx) as spec,
        (select array_agg(lower(trim(maps2.band_match_pattern)))
         from maps maps2
         where maps2.spec = (select mx.m from mx)
        ) as patterns
    ) m on true
  ),
  chosen_contract as (
    select
      w.*,
      cc.contract_id
    from chosen_maps w
    left join lateral (
      select c.id as contract_id
      from public.contracts c
      where c.candidate_id = w.candidate_id
        and c.client_id    = w.client_id
        and c.start_date <= w.work_date
        and (c.end_date is null or c.end_date >= w.work_date)
        and w.band_patterns is not null
        and exists (
          select 1
          from unnest(w.band_patterns) p
          where position(lower(p) in lower(coalesce(c.band,''))) > 0
        )
      order by c.start_date desc nulls last, c.id desc
      limit 1
    ) cc on true
  ),
  matched_shift as (
    select
      w.*,

      s.id as matched_shift_id,
      date_trunc('minute', s.start_utc) as old_start_utc,
      date_trunc('minute', s.end_utc)   as old_end_utc,
      coalesce(s.break_mins, 0)::int    as old_break_mins,
      coalesce(s.pay_minutes, 0)::int   as old_paid_minutes,
      s.cancelled_at_utc               as old_cancelled_at_utc,

      s.timesheet_id                   as old_timesheet_id,
      (t.timesheet_id is not null)     as old_timesheet_exists

    from chosen_contract w
    left join public.nhsp_shifts s
      on s.external_row_key = w.external_row_key
     and s.source_system = v_src
    left join public.timesheets t
      on t.timesheet_id = s.timesheet_id
  )
  select
    ms.hr_row_id,
    ms.external_row_key,
    ms.work_date,
    nullif(trim(coalesce(ms.incoming_code_raw,'')),'') as incoming_code,
    ms.candidate_id,
    ms.client_id,
    ms.week_ending_date,
    ms.contract_id,

    case
      when (select count(*) from imp) = 0 then 'REJECT_IMPORT_NOT_FOUND'
      when v_sys = 'NHSP' and (select imp.source_system from imp) <> 'NHSP'::public.hr_source_enum then 'REJECT_SOURCE_SYSTEM_MISMATCH'
      when v_sys = 'HR_WEEKLY' and (select imp.source_system from imp) <> 'HEALTHROSTER'::public.hr_source_enum then 'REJECT_SOURCE_SYSTEM_MISMATCH'
      when ms.candidate_id is null then 'REJECT_NO_CANDIDATE'
      when ms.client_id is null then 'REJECT_NO_CLIENT'
      when ms.code_norm = '' then 'REJECT_BAD_ROW'
      when ms.in_range_count = 0 then 'REJECT_NO_CONTRACT'
      when ms.band_patterns is null then 'REJECT_NO_CONTRACT_BAND_MISMATCH'
      when ms.contract_id is null then 'REJECT_NO_CONTRACT_BAND_MISMATCH'
      else 'OK'
    end as action,

    case
      when (select count(*) from imp) = 0 then 'Import not found'
      when v_sys = 'NHSP' and (select imp.source_system from imp) <> 'NHSP'::public.hr_source_enum
        then 'Import source_system is not NHSP'
      when v_sys = 'HR_WEEKLY' and (select imp.source_system from imp) <> 'HEALTHROSTER'::public.hr_source_enum
        then 'Import source_system is not HEALTHROSTER'
      when ms.candidate_id is null then 'No candidate mapping found for staff name'
      when ms.client_id is null then 'No client mapping found'
      when ms.code_norm = '' then 'Missing incoming_code (assignment/grade)'
      when ms.in_range_count = 0 then 'No active contract for candidate/client on this date'
      when ms.band_patterns is null
        then 'No band mapping rows exist for this incoming_code at candidate+client/candidate/client/global scope'
      when ms.contract_id is null
        then 'No contract band matches incoming_code according to mapping table'
      else ''
    end as reason,

    -- diff fields
    ms.matched_shift_id,
    ms.old_start_utc,
    ms.old_end_utc,
    ms.old_break_mins,
    ms.old_paid_minutes,
    ms.old_cancelled_at_utc,

    ms.new_start_utc,
    ms.new_end_utc,
    ms.new_break_mins,
    ms.new_paid_minutes,

    case
      when ms.matched_shift_id is null then null::int
      else (ms.new_paid_minutes - ms.old_paid_minutes)
    end as delta_paid_minutes,

    (ms.matched_shift_id is null) as is_new,

    (
      ms.matched_shift_id is not null
      and ms.old_cancelled_at_utc is null
      and ms.old_start_utc = ms.new_start_utc
      and ms.old_end_utc   = ms.new_end_utc
      and ms.old_break_mins = ms.new_break_mins
      and (
        v_sys <> 'NHSP'
        or (ms.old_timesheet_id is not null and ms.old_timesheet_exists is true)
      )
    ) as is_noop,

    (
      ms.matched_shift_id is not null
      and not (
        ms.matched_shift_id is not null
        and ms.old_cancelled_at_utc is null
        and ms.old_start_utc = ms.new_start_utc
        and ms.old_end_utc   = ms.new_end_utc
        and ms.old_break_mins = ms.new_break_mins
        and (
          v_sys <> 'NHSP'
          or (ms.old_timesheet_id is not null and ms.old_timesheet_exists is true)
        )
      )
    ) as is_changed

  from matched_shift ms;

  get diagnostics v_rows_out = row_count;

  -- Debug summary (invoice_debug only). Note: actor_user_id is not available here, so we log with NULL actor.
  if v_invoice_debug then
    begin
      with final_rows as (
        with imp as (
          select
            hi.id,
            hi.source_system,
            hi.client_id as hr_client_id
          from public.hr_imports hi
          where hi.id = p_import_id
          limit 1
        ),
        raw as (
          select
            r.id as hr_row_id,
            r.external_row_key,
            r.date_local as work_date,
            coalesce(
              nullif((r.payload_json ->> 'staff_name'), ''),
              nullif((r.payload_json ->> 'worker_name'), ''),
              nullif((r.payload_json ->> 'name'), ''),
              nullif(r.staff_raw, ''),
              nullif(r.staff_norm, '')
            ) as staff_name,
            coalesce(
              nullif((r.payload_json ->> 'ward'), ''),
              nullif((r.payload_json ->> 'unit'), ''),
              nullif(r.unit_hint, ''),
              nullif(r.unit_raw, '')
            ) as ward,
            coalesce(
              nullif((r.payload_json ->> 'trust'), ''),
              nullif((r.payload_json ->> 'hospital_or_trust'), ''),
              nullif(r.unit_raw, '')
            ) as trust_raw,
            date_trunc('minute', (r.payload_json ->> 'start_utc')::timestamptz) as new_start_utc,
            date_trunc('minute', (r.payload_json ->> 'end_utc')::timestamptz)   as new_end_utc,

            -- ✅ keep debug aligned with live logic: actual_break_* then break_* then 0
            greatest(
              0,
              coalesce(
                nullif(btrim(coalesce(r.payload_json ->> 'actual_break_mins','')), '')::int,
                nullif(btrim(coalesce(r.payload_json ->> 'actual_break_minutes','')), '')::int,
                nullif(btrim(coalesce(r.payload_json ->> 'break_mins','')), '')::int,
                nullif(btrim(coalesce(r.payload_json ->> 'break_minutes','')), '')::int,
                0
              )
            ) as new_break_mins,

            greatest(
              0,
              (floor(extract(epoch from (
                date_trunc('minute', (r.payload_json ->> 'end_utc')::timestamptz)
                -
                date_trunc('minute', (r.payload_json ->> 'start_utc')::timestamptz)
              )) / 60.0))::int
              -
              greatest(
                0,
                coalesce(
                  nullif(btrim(coalesce(r.payload_json ->> 'actual_break_mins','')), '')::int,
                  nullif(btrim(coalesce(r.payload_json ->> 'actual_break_minutes','')), '')::int,
                  nullif(btrim(coalesce(r.payload_json ->> 'break_mins','')), '')::int,
                  nullif(btrim(coalesce(r.payload_json ->> 'break_minutes','')), '')::int,
                  0
                )
              )
            ) as new_paid_minutes,

            case
              when v_sys = 'NHSP' then coalesce(
                nullif((r.payload_json ->> 'assignment_code'), ''),
                nullif((r.payload_json ->> 'assignment'), ''),
                nullif((r.payload_json ->> 'Request_Grade'), ''),
                nullif(r.assignment_grade_norm, '')
              )
              else coalesce(
                nullif((r.payload_json ->> 'grade_raw'), ''),
                nullif((r.payload_json ->> 'Grade'), ''),
                nullif((r.payload_json ->> 'Request_Grade'), ''),
                nullif(r.assignment_grade_norm, '')
              )
            end as incoming_code_raw
          from public.hr_rows r
          join imp
            on imp.id = r.import_id
          where r.import_id = p_import_id
            and r.date_local is not null
            and (r.payload_json ->> 'start_utc') is not null
            and (r.payload_json ->> 'end_utc')   is not null
        ),
        resolved_ids as (
          select
            src.*,
            n.staff_lc,
            n.staff_key,
            n.trust_lc,
            n.trust_key,
            coalesce(
              cand_alias.id,
              cand_map.candidate_id,
              cand_exact_unique.candidate_id
            ) as candidate_id,
            case
              when v_sys = 'NHSP' then coalesce(cli_alias.client_id, cli_name.client_id)
              else imp.hr_client_id
            end as client_id
          from raw src
          join imp on true
          cross join lateral (
            select
              nullif(lower(trim(coalesce(src.staff_name,''))), '') as staff_lc,
              nullif(regexp_replace(lower(coalesce(src.staff_name,'')), '[^a-z0-9]+', '', 'g'), '') as staff_key,
              nullif(lower(trim(coalesce(src.trust_raw,''))), '') as trust_lc,
              nullif(regexp_replace(lower(coalesce(src.trust_raw,'')), '[^a-z0-9]+', '', 'g'), '') as trust_key
          ) n
          left join lateral (
            select c.id
            from public.candidates c
            where c.nhsp_hr_name_aliases is not null
              and (
                (n.staff_lc  is not null and c.nhsp_hr_name_aliases @> to_jsonb(array[n.staff_lc]::text[]))
                or
                (n.staff_key is not null and c.nhsp_hr_name_aliases @> to_jsonb(array[n.staff_key]::text[]))
              )
            limit 1
          ) cand_alias on true
          left join lateral (
            select hm.candidate_id
            from public.hr_name_mappings hm
            where hm.active = true
              and (
                (n.staff_lc  is not null and hm.hr_name_norm = n.staff_lc)
                or
                (n.staff_key is not null and hm.hr_name_norm = n.staff_key)
              )
            order by hm.created_at desc
            limit 1
          ) cand_map on (cand_alias.id is null)
          left join lateral (
            with matches as (
              select c.id as cid
              from public.candidates c
              where c.active = true
                and n.staff_key is not null
                and (
                  regexp_replace(lower(coalesce(c.first_name,'') || coalesce(c.last_name,'')), '[^a-z0-9]+', '', 'g') = n.staff_key
                  or
                  regexp_replace(lower(coalesce(c.last_name,'')  || coalesce(c.first_name,'')), '[^a-z0-9]+', '', 'g') = n.staff_key
                )
            )
            select
              case
                when count(*) = 1
                  then (array_agg(cid order by cid::text))[1]
              end as candidate_id
            from matches
          ) cand_exact_unique on (cand_alias.id is null and cand_map.candidate_id is null)
          left join lateral (
            select ch.client_id
            from public.client_hospitals ch
            where v_sys = 'NHSP'
              and ch.hospital_name_norm is not null
              and (
                (n.trust_lc  is not null and ch.hospital_name_norm @> to_jsonb(array[n.trust_lc]::text[]))
                or
                (n.trust_key is not null and ch.hospital_name_norm @> to_jsonb(array[n.trust_key]::text[]))
              )
            limit 1
          ) cli_alias on (v_sys = 'NHSP')
          left join lateral (
            with matches as (
              select cl.id as clid
              from public.clients cl
              where v_sys = 'NHSP'
                and n.trust_key is not null
                and regexp_replace(lower(coalesce(cl.name,'')), '[^a-z0-9]+', '', 'g') = n.trust_key
            )
            select
              case
                when count(*) = 1
                  then (array_agg(clid order by clid::text))[1]
              end as client_id
            from matches
          ) cli_name on (v_sys = 'NHSP' and cli_alias.client_id is null)
        ),
        with_we as (
          select
            r.*,
            (r.work_date
              + (
                  (coalesce(cs.week_ending_weekday, 0)::int - extract(dow from r.work_date)::int + 7) % 7
                )
            )::date as week_ending_date,
            lower(trim(coalesce(r.incoming_code_raw,''))) as code_norm
          from resolved_ids r
          left join lateral (
            select cs.week_ending_weekday
            from public.client_settings cs
            where cs.client_id = r.client_id
            order by cs.effective_from desc, cs.created_at desc
            limit 1
          ) cs on true
        ),
        in_range_counts as (
          select
            w.*,
            coalesce(cr.in_range_count, 0) as in_range_count
          from with_we w
          left join lateral (
            select count(*)::int as in_range_count
            from public.contracts c
            where c.candidate_id = w.candidate_id
              and c.client_id    = w.client_id
              and c.start_date <= w.work_date
              and (c.end_date is null or c.end_date >= w.work_date)
          ) cr on true
        ),
        chosen_maps as (
          select
            w.*,
            m.patterns as band_patterns
          from in_range_counts w
          left join lateral (
            with maps as (
              select abm.band_match_pattern, 3 as spec
              from public.assignment_band_mappings abm
              where abm.active = true
                and upper(trim(abm.system_type)) = v_sys
                and lower(trim(abm.incoming_code)) = w.code_norm
                and w.candidate_id is not null
                and w.client_id is not null
                and abm.candidate_id = w.candidate_id
                and abm.client_id = w.client_id
              union all
              select abm.band_match_pattern, 2 as spec
              from public.assignment_band_mappings abm
              where abm.active = true
                and upper(trim(abm.system_type)) = v_sys
                and lower(trim(abm.incoming_code)) = w.code_norm
                and w.candidate_id is not null
                and abm.candidate_id = w.candidate_id
                and abm.client_id is null
              union all
              select abm.band_match_pattern, 1 as spec
              from public.assignment_band_mappings abm
              where abm.active = true
                and upper(trim(abm.system_type)) = v_sys
                and lower(trim(abm.incoming_code)) = w.code_norm
                and w.client_id is not null
                and abm.candidate_id is null
                and abm.client_id = w.client_id
              union all
              select abm.band_match_pattern, 0 as spec
              from public.assignment_band_mappings abm
              where abm.active = true
                and upper(trim(abm.system_type)) = v_sys
                and lower(trim(abm.incoming_code)) = w.code_norm
                and abm.candidate_id is null
                and abm.client_id is null
            ),
            mx as (select max(maps.spec) as m from maps)
            select
              (select array_agg(lower(trim(maps2.band_match_pattern)))
               from maps maps2
               where maps2.spec = (select mx.m from mx)
              ) as patterns
          ) m on true
        ),
        chosen_contract as (
          select
            w.*,
            cc.contract_id
          from chosen_maps w
          left join lateral (
            select c.id as contract_id
            from public.contracts c
            where c.candidate_id = w.candidate_id
              and c.client_id    = w.client_id
              and c.start_date <= w.work_date
              and (c.end_date is null or c.end_date >= w.work_date)
              and w.band_patterns is not null
              and exists (
                select 1
                from unnest(w.band_patterns) p
                where position(lower(p) in lower(coalesce(c.band,''))) > 0
              )
            order by c.start_date desc nulls last, c.id desc
            limit 1
          ) cc on true
        ),
        matched_shift as (
          select
            w.*,
            s.id as matched_shift_id,
            s.timesheet_id as old_timesheet_id,
            (t.timesheet_id is not null) as old_timesheet_exists,
            s.cancelled_at_utc as old_cancelled_at_utc,
            date_trunc('minute', s.start_utc) as old_start_utc,
            date_trunc('minute', s.end_utc) as old_end_utc,
            coalesce(s.break_mins,0)::int as old_break_mins
          from chosen_contract w
          left join public.nhsp_shifts s
            on s.external_row_key = w.external_row_key
           and s.source_system = v_src
          left join public.timesheets t
            on t.timesheet_id = s.timesheet_id
        )
        select
          ms.hr_row_id,
          ms.external_row_key,
          ms.work_date,
          ms.candidate_id,
          ms.client_id,
          ms.week_ending_date,
          ms.contract_id,
          case
            when (select count(*) from imp) = 0 then 'REJECT_IMPORT_NOT_FOUND'
            when v_sys = 'NHSP' and (select imp.source_system from imp) <> 'NHSP'::public.hr_source_enum then 'REJECT_SOURCE_SYSTEM_MISMATCH'
            when v_sys = 'HR_WEEKLY' and (select imp.source_system from imp) <> 'HEALTHROSTER'::public.hr_source_enum then 'REJECT_SOURCE_SYSTEM_MISMATCH'
            when ms.candidate_id is null then 'REJECT_NO_CANDIDATE'
            when ms.client_id is null then 'REJECT_NO_CLIENT'
            when ms.code_norm = '' then 'REJECT_BAD_ROW'
            when ms.in_range_count = 0 then 'REJECT_NO_CONTRACT'
            when ms.band_patterns is null then 'REJECT_NO_CONTRACT_BAND_MISMATCH'
            when ms.contract_id is null then 'REJECT_NO_CONTRACT_BAND_MISMATCH'
            else 'OK'
          end as action,
          ms.matched_shift_id,
          ms.old_timesheet_id,
          ms.old_timesheet_exists,
          (ms.matched_shift_id is null) as is_new,
          (
            ms.matched_shift_id is not null
            and ms.old_cancelled_at_utc is null
            and ms.old_start_utc = ms.new_start_utc
            and ms.old_end_utc   = ms.new_end_utc
            and ms.old_break_mins = ms.new_break_mins
            and (
              v_sys <> 'NHSP'
              or (ms.old_timesheet_id is not null and ms.old_timesheet_exists is true)
            )
          ) as is_noop,
          (
            ms.matched_shift_id is not null
            and not (
              ms.matched_shift_id is not null
              and ms.old_cancelled_at_utc is null
              and ms.old_start_utc = ms.new_start_utc
              and ms.old_end_utc   = ms.new_end_utc
              and ms.old_break_mins = ms.new_break_mins
              and (
                v_sys <> 'NHSP'
                or (ms.old_timesheet_id is not null and ms.old_timesheet_exists is true)
              )
            )
          ) as is_changed
        from matched_shift ms
      )
      select jsonb_build_object(
        'import_id', p_import_id::text,
        'system_type', v_sys,
        'rows_returned', v_rows_out,

        'rows_total', count(*)::int,
        'ok_rows', count(*) filter (where fr.action = 'OK')::int,
        'reject_rows', count(*) filter (where fr.action <> 'OK')::int,

        'ok_new_rows', count(*) filter (where fr.action = 'OK' and fr.is_new is true)::int,
        'ok_noop_rows', count(*) filter (where fr.action = 'OK' and fr.is_noop is true)::int,
        'ok_changed_rows', count(*) filter (where fr.action = 'OK' and fr.is_changed is true)::int,

        -- ✅ NEW debug: attach-needed rows (NHSP) are those that would otherwise be noop but linkage is missing
        'ok_attach_needed_rows',
          count(*) filter (
            where fr.action = 'OK'
              and v_sys = 'NHSP'
              and fr.is_new is false
              and fr.is_noop is false
              and fr.is_changed is true
              and fr.matched_shift_id is not null
              and (
                fr.old_timesheet_id is null
                or fr.old_timesheet_exists is false
              )
          )::int,

        'missing_external_row_key_rows', count(*) filter (where fr.external_row_key is null)::int,
        'missing_candidate_rows', count(*) filter (where fr.candidate_id is null)::int,
        'missing_client_rows', count(*) filter (where fr.client_id is null)::int,
        'missing_contract_rows', count(*) filter (where fr.contract_id is null)::int,

        'sample_ok_changed_external_row_keys',
          coalesce(
            (
              select jsonb_agg(x.external_row_key)
              from (
                select fr2.external_row_key
                from final_rows fr2
                where fr2.action = 'OK'
                  and fr2.is_changed is true
                  and fr2.external_row_key is not null
                order by fr2.external_row_key
                limit 20
              ) as x
            ),
            '[]'::jsonb
          ),

        'sample_ok_new_external_row_keys',
          coalesce(
            (
              select jsonb_agg(x2.external_row_key)
              from (
                select fr3.external_row_key
                from final_rows fr3
                where fr3.action = 'OK'
                  and fr3.is_new is true
                  and fr3.external_row_key is not null
                order by fr3.external_row_key
                limit 20
              ) as x2
            ),
            '[]'::jsonb
          ),

        'reject_action_counts',
          coalesce(
            (
              select jsonb_agg(jsonb_build_object('action', y.action, 'count', y.cnt) order by y.cnt desc, y.action asc)
              from (
                select fr4.action, count(*)::int as cnt
                from final_rows fr4
                where fr4.action <> 'OK'
                group by fr4.action
                order by count(*) desc, fr4.action asc
                limit 30
              ) as y
            ),
            '[]'::jsonb
          )
      )
      into v_dbg
      from final_rows fr;

      perform public._imp_debug_audit(
        null,
        'WEEKLY_IMPORT_PHASE2_DEBUG',
        v_dbg,
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
  end if;

exception when others then
  get stacked diagnostics v_sqlstate = returned_sqlstate, v_err = message_text;

  begin
    perform public._imp_debug_audit(
      null,
      'WEEKLY_IMPORT_PHASE2_ERROR',
      jsonb_build_object(
        'import_id', p_import_id::text,
        'system_type', v_sys,
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
