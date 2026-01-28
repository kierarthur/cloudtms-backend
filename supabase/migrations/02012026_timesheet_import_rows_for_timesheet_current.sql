drop function if exists public.timesheet_import_rows_for_timesheet_current(uuid, boolean, uuid, uuid);

create function public.timesheet_import_rows_for_timesheet_current(
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
      (select req.requested_timesheet_id from req) as requested_timesheet_id,
      (select cur.current_timesheet_id from cur)   as current_timesheet_id
  ),
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
  imp as (
    select
      s.source_system::text as source_system,
      s.import_id           as import_id,
      hi.filename           as filename,
      hi.uploaded_at_utc    as uploaded_at_utc,
      hi.file_r2_key        as file_r2_key,

      -- Prefer multi-row headers if stored; else wrap single-row header_columns (only if non-empty); else []
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
    from (select distinct sh0.source_system, sh0.import_id from sh sh0) s
    left join public.hr_imports hi
      on hi.id = s.import_id
  ),
  r as (
    select
      s.source_system::text as source_system,
      s.import_id           as import_id,
      jsonb_agg(
        jsonb_build_object(
          'raw_columns', hr.payload_json->'raw_columns',
          'payload',     hr.payload_json
        )
        order by hr.id
      ) as rows
    from sh s
    join public.hr_rows hr
      on hr.import_id = s.import_id
     and hr.external_row_key = s.external_row_key
    group by s.source_system::text, s.import_id
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
    on r.source_system = i.source_system
   and r.import_id     = i.import_id
  order by i.source_system, i.uploaded_at_utc nulls last, i.import_id;
$$;
