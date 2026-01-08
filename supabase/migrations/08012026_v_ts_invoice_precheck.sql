-- ============================================================
-- CloudTMS: v_ts_invoice_precheck (VIEW) — client-led attach flags
-- Safe to re-run (keeps existing column order, appends new columns)
--
-- Existing columns (UNCHANGED order):
--   1) timesheet_id
--   2) week_ending_date
--   3) submission_mode
--   4) manual_pdf_r2_key
--   5) reference_number
--   6) require_reference_to_invoice
--   7) precheck_status
--
-- Appended columns (NEW):
--   8) effective_ts_attach_to_invoice   (client_settings.ts_attach_to_invoice, default TRUE)
--   9) effective_hr_attach_to_invoice   (client_settings.hr_attach_to_invoice, default TRUE)
--  10) effective_auto_invoice_default   (client_settings.auto_invoice_default, default FALSE)
--
-- BLOCK_NO_PDF logic (client-led, only when effective_ts_attach_to_invoice = TRUE):
--   MANUAL     => manual_pdf_r2_key IS NULL
--   NON-MANUAL (incl NULL/ELECTRONIC) => generated_pdf_at_utc IS NULL
--
-- NOTE:
--   - timesheets has no client_id, so client_id is derived from timesheets_financials
--     (is_current = true) for this timesheet_id.
--   - Anchor date for picking client_settings row: (now() Europe/London)::date
-- ============================================================

create or replace view public.v_ts_invoice_precheck as
with anchor as (
  select (now() at time zone 'Europe/London')::date as anchor_ymd
)
select
  ts.timesheet_id,
  ts.week_ending_date,
  ts.submission_mode,
  ts.manual_pdf_r2_key,
  ts.reference_number,
  c.require_reference_to_invoice,

  case
    -- -----------------------------
    -- PDF gating (client-led)
    -- -----------------------------
    when coalesce(cs.ts_attach_to_invoice, true) = true
     and (
       (ts.submission_mode = 'MANUAL'::submission_mode_enum and ts.manual_pdf_r2_key is null)
       or
       (ts.submission_mode is distinct from 'MANUAL'::submission_mode_enum and ts.generated_pdf_at_utc is null)
     )
      then 'BLOCK_NO_PDF'::text

    -- -----------------------------
    -- Reference/PO gating (contract-led, unchanged)
    -- -----------------------------
    when coalesce(c.require_reference_to_invoice, false) = true
     and (
       (
         ts.sheet_scope = 'DAILY'::timesheet_scope_enum
         and (ts.reference_number is null or length(btrim(ts.reference_number)) = 0)
       )
       or
       (
         ts.sheet_scope = 'WEEKLY'::timesheet_scope_enum
         and ts.submission_mode = 'MANUAL'::submission_mode_enum
         and (
           ts.actual_schedule_json is null
           or jsonb_typeof(ts.actual_schedule_json) <> 'array'::text
           or (
             jsonb_typeof(ts.actual_schedule_json) = 'array'::text
             and (
               jsonb_array_length(ts.actual_schedule_json) = 0
               or (
                 exists (
                   select 1
                   from jsonb_array_elements(ts.actual_schedule_json) seg(value)
                   where coalesce(btrim(seg.value ->> 'start'::text), ''::text) <> ''::text
                     and coalesce(btrim(seg.value ->> 'end'::text), ''::text) <> ''::text
                     and coalesce(btrim(seg.value ->> 'ref_num'::text), ''::text) = ''::text
                 )
               )
             )
           )
         )
       )
       or
       (
         ts.sheet_scope = 'WEEKLY'::timesheet_scope_enum
         and ts.submission_mode <> 'MANUAL'::submission_mode_enum
         and (ts.reference_number is null or length(btrim(ts.reference_number)) = 0)
         and (
           ts.day_references_json is null
           or ts.day_references_json = '{}'::jsonb
           or not exists (
             select 1
             from jsonb_each_text(ts.day_references_json) j(k, v)
             where btrim(j.v) <> ''::text
           )
         )
       )
     )
      then 'BLOCK_NO_REFERENCE'::text

    else 'OK'::text
  end as precheck_status,

  -- ------------------------------------------------------------
  -- NEW appended columns (client-led effective flags)
  -- ------------------------------------------------------------
  coalesce(cs.ts_attach_to_invoice, true)  as effective_ts_attach_to_invoice,
  coalesce(cs.hr_attach_to_invoice, true)  as effective_hr_attach_to_invoice,
  coalesce(cs.auto_invoice_default, false) as effective_auto_invoice_default

from public.timesheets ts
left join public.contract_weeks cw
  on cw.timesheet_id = ts.timesheet_id
left join public.contracts c
  on c.id = coalesce(ts.contract_id, cw.contract_id)

-- derive client_id from the current TSFIN snapshot
left join lateral (
  select tf0.client_id
  from public.timesheets_financials tf0
  where tf0.timesheet_id = ts.timesheet_id
    and tf0.is_current = true
  order by tf0.updated_at desc nulls last
  limit 1
) tf on true

-- client_settings chosen by London "anchor" date
left join lateral (
  select
    cs0.client_id,
    cs0.auto_invoice_default,
    cs0.hr_attach_to_invoice,
    cs0.ts_attach_to_invoice
  from public.client_settings cs0
  cross join anchor a
  where cs0.client_id = tf.client_id
    and (cs0.effective_from <= a.anchor_ymd or cs0.effective_from is null)
  order by cs0.effective_from desc nulls last
  limit 1
) cs on true;
