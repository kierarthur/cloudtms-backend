-- NOTE: This must run AFTER your migrations that add:
--   public.contracts.is_ad_hoc
--   public.contract_weeks.enforce_day_partition, allowed_days_mask, split_boundary_date, worker_note, split_group_key
-- It is safe to re-run (CREATE OR REPLACE), and it preserves the existing column order by appending new columns at the end.


CREATE OR REPLACE VIEW public.v_contract_weeks_enriched AS
SELECT
  cw.id,
  cw.contract_id,
  cw.week_ending_date,
  cw.additional_seq,
  cw.status,
  cw.submission_mode_snapshot,
  cw.timesheet_id,
  cw.uploaded_pdf_r2_key,
  cw.day_entries_json,
  cw.totals_json,
  cw.created_at,
  cw.updated_at,
  c.candidate_id,
  c.client_id,
  c.require_reference_to_pay,
  c.require_reference_to_invoice,

  -- ✅ appended columns (do NOT disturb existing positions)
  cw.planned_schedule_json,
  cw.enforce_day_partition,
  cw.allowed_days_mask,
  cw.split_boundary_date,
  cw.worker_note,
  cw.split_group_key,
  c.is_ad_hoc,
  cw.is_adjustment

FROM public.contract_weeks AS cw
JOIN public.contracts      AS c
  ON c.id = cw.contract_id;
