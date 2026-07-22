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
      h.hr_break_mins
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
      and t.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum
      and ct.client_id = v_client_id
      and t.week_ending_date is not null
      and t.week_ending_date between v_we_min and v_we_max
      and coalesce(ce.eff_autoprocess_hr,false) = true
      and coalesce(ce.eff_requires_hr,false) = true
      and coalesce(ce.eff_no_timesheet_required,false) = false
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
        when ce2.contract_id is null then null::uuid
        else tmr.raw_timesheet_id
      end as timesheet_id,
      case
        when tmr.raw_timesheet_id is null then false
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
    from hr_entries_flat hf
    join ts_universe tu
      on tu.candidate_id = hf.candidate_id
     and tu.week_ending_date = hf.week_ending_date
    left join pairing_counts pc
      on pc.timesheet_id = tu.timesheet_id
     and pc.work_date = hf.work_date
     and pc.match_count = 1
     and pc.matched_hr_row_id = hf.hr_row_id
    where pc.matched_hr_row_id is null
  ),

  comparisons_worker as (
    select
      pc.timesheet_id,
      pc.candidate_id,
      pc.candidate_name,
      pc.week_ending_date,
      pc.work_date,

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
          'work_date', ce.work_date::text,

          'timesheet_start', ce.ts_start_hhmm,
          'timesheet_end', ce.ts_end_hhmm,
          'timesheet_break_mins', ce.ts_break_mins,

          'healthroster_start', ce.hr_start_hhmm,
          'healthroster_end', ce.hr_end_hhmm,
          'healthroster_break_mins', ce.hr_break_mins,

          -- stable key for FE checkbox state
          'comparison_key',
            (
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
    from hr_entries_flat hf
    join ts_universe tu
      on tu.candidate_id = hf.candidate_id
     and tu.week_ending_date = hf.week_ending_date
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
    left join hr_day_totals hdt
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
      coalesce(cbg.any_invoice_locked,false) as any_invoice_locked,
      coalesce(cbg.any_locked_ref_change,false) as any_locked_ref_change,
      coalesce(cbg.any_locked_time_mismatch,false) as any_locked_time_mismatch
    from per_ts p
    left join comparisons_by_group cbg
      on cbg.candidate_id = p.candidate_id
     and cbg.week_ending_date = p.week_ending_date
     and cbg.timesheet_id = p.timesheet_id
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

        'days', '[]'::jsonb,
        'comparisons', '[]'::jsonb
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

ALTER FUNCTION public.hr_weekly_validation_preview(uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.hr_weekly_validation_preview(uuid) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.hr_weekly_validation_preview(uuid) TO postgres, service_role;
