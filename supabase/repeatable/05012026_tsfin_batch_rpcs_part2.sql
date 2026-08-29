-- ============================================================
-- 2.5: Batch context loader (one row per requested timesheet_id)
-- SAFE TO RE-RUN: CREATE OR REPLACE
--
-- ✅ Includes new TSFIN category columns automatically via:
--     out_cur_fin = to_jsonb(tf)
-- because tf is a row from timesheets_financials and to_jsonb(row) includes all columns.
-- ============================================================

create or replace function public._tsfin_invalid_segment_count(invoice_breakdown_json jsonb)
returns int
language sql
immutable
as $$
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
$$;

select pg_notify('pgrst', 'reload schema');

select pg_notify('pgrst', 'reload schema');



