create or replace function public.timesheet_import_rows_for_timesheet_current(
  p_timesheet_id uuid,
  p_include_excluded boolean default true,
  p_import_id uuid default null,
  p_shift_id uuid default null
)
returns table (
  requested_timesheet_id uuid,
  current_timesheet_id uuid,
  source_system text,
  import_id uuid,
  filename text,
  uploaded_at_utc timestamptz,
  file_r2_key text,
  header_rows jsonb,
  header_columns jsonb,
  rows jsonb
)
language sql
stable
security definer
set search_path = public
as $$
  with req as (
    select
      t.timesheet_id as requested_timesheet_id,
      t.booking_id
    from public.timesheets t
    where t.timesheet_id = p_timesheet_id
    limit 1
  ),
  cur as (
    select
      t.timesheet_id as current_timesheet_id
    from public.timesheets t
    join req
      on req.booking_id = t.booking_id
    where t.is_current = true
    order by t.version desc
    limit 1
  ),
  use_id as (
    select
      (select r.requested_timesheet_id from req r) as requested_timesheet_id,
      (select c.current_timesheet_id  from cur c) as current_timesheet_id
  ),

  -- Existing evidence source: attached shifts (unchanged)
  sh as (
    select
      ns.source_system    as source_system,
      ns.latest_import_id as import_id,
      ns.external_row_key as external_row_key,
      ns.invoice_status   as invoice_status
    from public.nhsp_shifts ns
    where ns.timesheet_id = (select u.current_timesheet_id from use_id u)
      and ns.latest_import_id is not null
      and ns.external_row_key is not null
      and (p_shift_id is null or ns.id = p_shift_id)
      and (p_import_id is null or ns.latest_import_id = p_import_id)
      and (
        p_include_excluded is true
        or coalesce(ns.invoice_status,'') <> 'DEFERRED'
      )
  ),

  -- Daily evidence is server-owned once an import review exists.  Historical
  -- pre-review imports retain the legacy payload link as a compatibility read.
  daily as (
    select
      hr.import_id         as import_id,
      hr.external_row_key  as external_row_key,
      'HEALTHROSTER_DAILY'::text as source_system_hint
    from public.hr_rows hr
    join public.hr_imports hi
      on hi.id = hr.import_id
    left join public.import_review_daily_timesheet_resolutions rr
      on rr.import_id=hr.import_id and rr.hr_row_id=hr.id and rr.status='APPLIED'
    where hi.source_system = 'HEALTHROSTER_DAILY'::public.hr_source_enum
      and hr.import_id is not null
      and hr.external_row_key is not null
      and (
        rr.resolved_timesheet_id=(select u.current_timesheet_id from use_id u)
        or (rr.id is null and not exists(select 1 from public.import_review_states s where s.import_id=hr.import_id)
          and (hr.payload_json->>'resolved_timesheet_id')=(select u.current_timesheet_id::text from use_id u))
      )
      and (p_import_id is null or hr.import_id = p_import_id)
      and (p_shift_id is null)  -- shift_id filter only applies to shift-based evidence sources
  ),

  -- Existing evidence source: schedule entries (correction timesheets)
  -- Extract (import_id, external_row_key) pairs from timesheets.actual_schedule_json
  sched_raw as (
    select
      s.elem as seg
    from public.timesheets t
    join use_id u
      on u.current_timesheet_id = t.timesheet_id
    cross join lateral jsonb_array_elements(
      case
        when jsonb_typeof(t.actual_schedule_json) = 'array' then t.actual_schedule_json
        else '[]'::jsonb
      end
    ) as s(elem)
  ),
  sched_keys as (
    select
      case
        when (sr.seg ? 'import_id')
         and (sr.seg->>'import_id') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          then (sr.seg->>'import_id')::uuid
        else null
      end as import_id,
      nullif(btrim(coalesce(sr.seg->>'external_row_key','')), '') as external_row_key,
      case
        when (sr.seg ? 'shift_id')
         and (sr.seg->>'shift_id') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          then (sr.seg->>'shift_id')::uuid
        else null
      end as shift_id
    from sched_raw sr
  ),
  sched_filtered as (
    select
      sk.import_id,
      sk.external_row_key
    from sched_keys sk
    where sk.import_id is not null
      and sk.external_row_key is not null
      and (p_import_id is null or sk.import_id = p_import_id)
      and (p_shift_id is null or sk.shift_id = p_shift_id)
  ),

  -- Combine evidence keys from all sources and de-dupe by (import_id, external_row_key)
  keys_union as (
    select
      sh0.import_id as import_id,
      sh0.external_row_key as external_row_key,
      sh0.source_system::text as source_system_hint
    from sh sh0

    union all

    select
      d0.import_id as import_id,
      d0.external_row_key as external_row_key,
      d0.source_system_hint as source_system_hint
    from daily d0

    union all

    select
      sf.import_id as import_id,
      sf.external_row_key as external_row_key,
      null::text as source_system_hint
    from sched_filtered sf
  ),
  keys as (
    select distinct on (ku.import_id, ku.external_row_key)
      ku.import_id,
      ku.external_row_key,
      ku.source_system_hint
    from keys_union ku
    where ku.import_id is not null
      and ku.external_row_key is not null
    order by ku.import_id, ku.external_row_key, (ku.source_system_hint is null) asc
  ),

  -- Import header per import_id (source_system is taken from hr_imports where possible)
  imp as (
    select
      coalesce(hi.source_system::text, k.any_source_system) as source_system,
      k.import_id as import_id,
      hi.filename as filename,
      hi.uploaded_at_utc as uploaded_at_utc,
      hi.file_r2_key as file_r2_key,

      case
        when jsonb_typeof(hi.parse_summary_json->'header_rows') = 'array'
          then (hi.parse_summary_json->'header_rows')
        when jsonb_typeof(hi.parse_summary_json->'header_columns') = 'array'
          and jsonb_array_length(hi.parse_summary_json->'header_columns') > 0
          then jsonb_build_array(hi.parse_summary_json->'header_columns')
        else '[]'::jsonb
      end as header_rows,

      case
        when jsonb_typeof(hi.parse_summary_json->'header_columns') = 'array'
          then (hi.parse_summary_json->'header_columns')
        else '[]'::jsonb
      end as header_columns
    from (
      select
        k0.import_id,
        min(k0.source_system_hint) as any_source_system
      from keys k0
      group by k0.import_id
    ) as k
    left join public.hr_imports hi
      on hi.id = k.import_id
  ),

  -- Rows per import_id, joining hr_rows by (import_id, external_row_key)
  r as (
    select
      k.import_id as import_id,
      jsonb_agg(
        jsonb_build_object(
          'raw_columns', hr.payload_json->'raw_columns',
          'payload',     hr.payload_json
        )
        order by hr.id
      ) as rows
    from keys k
    join public.hr_rows hr
      on hr.import_id = k.import_id
     and hr.external_row_key = k.external_row_key
    group by k.import_id
  )

  select
    (select u.requested_timesheet_id from use_id u) as requested_timesheet_id,
    (select u.current_timesheet_id from use_id u)   as current_timesheet_id,
    i.source_system,
    i.import_id,
    i.filename,
    i.uploaded_at_utc,
    i.file_r2_key,
    i.header_rows,
    i.header_columns,
    coalesce(r.rows, '[]'::jsonb) as rows
  from imp i
  left join r
    on r.import_id = i.import_id
  order by i.source_system, i.uploaded_at_utc nulls last, i.import_id;
$$;

ALTER FUNCTION public.timesheet_import_rows_for_timesheet_current(uuid,boolean,uuid,uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.timesheet_import_rows_for_timesheet_current(uuid,boolean,uuid,uuid) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.timesheet_import_rows_for_timesheet_current(uuid,boolean,uuid,uuid) TO postgres, service_role;
