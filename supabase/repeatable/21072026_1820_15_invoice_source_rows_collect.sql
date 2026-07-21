create or replace function public.invoice_source_rows_collect(
  p_invoice_id uuid,
  p_force_refresh boolean default true
)
returns table (
  source_system text,
  import_id uuid,
  header_rows jsonb,
  header_columns jsonb,
  rows_json jsonb
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_has_cache boolean := false;
  v_requires_hr boolean := false;
  v_hr_attach_to_invoice boolean := true;
  v_hr_allowed boolean := false;
  v_invoice_status public.invoice_status_enum;
  v_invoice_issued_at_utc timestamptz;
begin
  if p_invoice_id is null then
    raise exception 'invoice_id is required';
  end if;

  -- Attachment policy gating (must match generator policy):
  -- - NHSP rows are always eligible.
  -- - HEALTHROSTER rows are eligible only when (requires_hr = true AND hr_attach_to_invoice = true).
  select
    coalesce((i.header_snapshot_json #>> '{attach_policy,requires_hr}')::boolean, false) as requires_hr,
    coalesce((i.header_snapshot_json #>> '{attach_policy,hr_attach_to_invoice}')::boolean, true) as hr_attach_to_invoice,
    i.status,i.issued_at_utc
  into v_requires_hr, v_hr_attach_to_invoice,v_invoice_status,v_invoice_issued_at_utc
  from public.invoices i
  where i.id = p_invoice_id;

  if not found then
    raise exception 'invoice not found';
  end if;

  v_hr_allowed := coalesce(v_requires_hr,false) = true
                  and coalesce(v_hr_attach_to_invoice,false) = true;

  -- Issued/paid/on-hold invoice evidence is frozen.  A force flag may refresh
  -- a draft only; it never rewrites or back-fills an already-issued artefact.
  select exists(
    select 1
    from public.invoice_hr_source_rows r
    where r.invoice_id = p_invoice_id
  ) into v_has_cache;

  if v_has_cache and (coalesce(p_force_refresh,false) = false
      or v_invoice_status<>'DRAFT'::public.invoice_status_enum or v_invoice_issued_at_utc is not null) then
    return query
    select
      r.source_system,
      r.import_id,
      r.header_rows,
      r.header_columns,
      r.rows_json
    from public.invoice_hr_source_rows r
    where r.invoice_id = p_invoice_id
    order by r.source_system, r.import_id;
    return;
  end if;
  if not v_has_cache and (v_invoice_status<>'DRAFT'::public.invoice_status_enum or v_invoice_issued_at_utc is not null) then
    raise exception 'INVOICE_EVIDENCE_FROZEN_CACHE_MISSING' using errcode='55000';
  end if;

  -- Recompute + refresh cache (safe even if cache is empty)
  delete from public.invoice_hr_source_rows r
  where r.invoice_id = p_invoice_id;

  with lines as (
    select
      l.timesheet_id,
      l.meta_json
    from public.invoice_lines l
    where l.invoice_id = p_invoice_id
  ),
  -- ✅ FIX: derive timesheet_ids from either invoice_lines.timesheet_id OR meta_json.timesheet_id (UUID validated)
  ts_ids as (
    select distinct
      case
        when ln.timesheet_id is not null then ln.timesheet_id
        when ln.meta_json is not null
          and nullif(btrim(coalesce(ln.meta_json->>'timesheet_id','')), '') is not null
          and nullif(btrim(coalesce(ln.meta_json->>'timesheet_id','')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
        then nullif(btrim(coalesce(ln.meta_json->>'timesheet_id','')), '')::uuid
        else null
      end as timesheet_id
    from lines ln
    where
      ln.timesheet_id is not null
      or (
        ln.meta_json is not null
        and nullif(btrim(coalesce(ln.meta_json->>'timesheet_id','')), '') is not null
        and nullif(btrim(coalesce(ln.meta_json->>'timesheet_id','')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
      )
  ),
  fin as (
    select
      tf.timesheet_id,
      tf.invoice_breakdown_json
    from public.timesheets_financials tf
    where tf.is_current = true
      and tf.timesheet_id in (select t.timesheet_id from ts_ids t)
  ),
  segs as (
    select
      upper(coalesce(seg->>'source_system','')) as source_system,
      nullif(btrim(coalesce(seg->>'invoice_locked_invoice_id','')), '') as locked_invoice_id_text,
      nullif(btrim(coalesce(seg->>'nhsp_shift_id','')), '') as nhsp_shift_id_text
    from fin f
    cross join lateral jsonb_array_elements(coalesce(f.invoice_breakdown_json->'segments','[]'::jsonb)) seg
    where jsonb_typeof(seg) = 'object'
  ),
  shift_ids as (
    select distinct (sg.nhsp_shift_id_text)::uuid as shift_id
    from segs sg
    where sg.nhsp_shift_id_text is not null
      -- ✅ Critical: ONLY rows/segments locked to THIS invoice
      and sg.locked_invoice_id_text = p_invoice_id::text
      -- Source-system gating: NHSP always; HealthRoster only when allowed by policy
      and (
        sg.source_system = 'NHSP'
        or (sg.source_system = 'HEALTHROSTER' and v_hr_allowed = true)
      )
      and sg.nhsp_shift_id_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  ),

  -- Shift-based evidence (NHSP + weekly HealthRoster via nhsp_shifts)
  useful_shift as (
    select
      upper(coalesce(s.source_system::text,'UNKNOWN')) as source_system,
      s.latest_import_id as import_id,
      s.external_row_key as external_row_key
    from public.nhsp_shifts s
    where s.id in (select sh.shift_id from shift_ids sh)
      and s.latest_import_id is not null
      and s.external_row_key is not null
  ),

  -- Applied review resolutions are the Daily evidence authority. Historical
  -- imports without a review state retain the legacy payload link as read-only
  -- compatibility evidence.
  useful_daily as (
    select
      'HEALTHROSTER_DAILY'::text as source_system,
      hr.import_id as import_id,
      hr.external_row_key as external_row_key
    from public.hr_rows hr
    join public.hr_imports hi
      on hi.id = hr.import_id
    left join public.import_review_daily_timesheet_resolutions rr
      on rr.import_id=hr.import_id and rr.hr_row_id=hr.id and rr.status='APPLIED'
    where v_hr_allowed = true
      and hi.source_system = 'HEALTHROSTER_DAILY'::public.hr_source_enum
      and hr.import_id is not null
      and hr.external_row_key is not null
      and (
        rr.resolved_timesheet_id in (select t.timesheet_id from ts_ids t where t.timesheet_id is not null)
        or (rr.id is null and not exists(select 1 from public.import_review_states s where s.import_id=hr.import_id)
          and (hr.payload_json->>'resolved_timesheet_id') in
            (select t.timesheet_id::text from ts_ids t where t.timesheet_id is not null))
      )
  ),

  -- Unified evidence set
  useful as (
    select
      us.source_system,
      us.import_id,
      us.external_row_key
    from useful_shift us

    union all

    select
      ud.source_system,
      ud.import_id,
      ud.external_row_key
    from useful_daily ud
  ),

  grouped as (
    select
      u.source_system,
      u.import_id,
      jsonb_agg(distinct u.external_row_key) as keys_json
    from useful u
    group by u.source_system, u.import_id
  ),
  hdr as (
    select
      g.source_system,
      g.import_id,
      -- Prefer multi-row header if stored; else wrap single-row header_columns; else []
      case
        when jsonb_typeof(hi.parse_summary_json->'header_rows') = 'array'
          then (hi.parse_summary_json->'header_rows')
        when jsonb_typeof(hi.parse_summary_json->'header_columns') = 'array'
          then jsonb_build_array(hi.parse_summary_json->'header_columns')
        else '[]'::jsonb
      end as header_rows,
      case
        when jsonb_typeof(hi.parse_summary_json->'header_columns') = 'array'
          then (hi.parse_summary_json->'header_columns')
        else '[]'::jsonb
      end as header_columns,
      g.keys_json
    from grouped g
    join public.hr_imports hi
      on hi.id = g.import_id
  ),
  rows_agg as (
    select
      h.source_system,
      h.import_id,
      h.header_rows,
      h.header_columns,
      (
        select coalesce(jsonb_agg(r.payload_json order by r.id), '[]'::jsonb)
        from public.hr_rows r
        where r.import_id = h.import_id
          and r.external_row_key in (
            select jsonb_array_elements_text(h.keys_json)
          )
      ) as rows_json
    from hdr h
  )
  insert into public.invoice_hr_source_rows(
    invoice_id,
    source_system,
    import_id,
    header_rows,
    header_columns,
    rows_json
  )
  select
    p_invoice_id,
    ra.source_system,
    ra.import_id,
    ra.header_rows,
    ra.header_columns,
    ra.rows_json
  from rows_agg ra;

  -- Return refreshed cache
  return query
  select
    r.source_system,
    r.import_id,
    r.header_rows,
    r.header_columns,
    r.rows_json
  from public.invoice_hr_source_rows r
  where r.invoice_id = p_invoice_id
  order by r.source_system, r.import_id;

end;
$$;

ALTER FUNCTION public.invoice_source_rows_collect(uuid,boolean) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.invoice_source_rows_collect(uuid,boolean) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.invoice_source_rows_collect(uuid,boolean) TO postgres, service_role;
