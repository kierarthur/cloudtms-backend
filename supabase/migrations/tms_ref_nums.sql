-- =========================================================
-- 1) Candidate TMS Ref numeric sort helper (Option 3)
-- =========================================================

ALTER TABLE public.candidates
  ADD COLUMN IF NOT EXISTS tms_ref_num integer
  GENERATED ALWAYS AS (
    NULLIF(regexp_replace(tms_ref, '[^0-9]', '', 'g'), '')::integer
  ) STORED;

CREATE INDEX IF NOT EXISTS idx_candidates_tms_ref_num
  ON public.candidates (tms_ref_num, tms_ref);


-- =========================================================
-- 2) NEW RPC: Atomic PAYE<->UMBRELLA change apply (no partials)
--    Verified tables/columns used:
--      candidates(id,pay_method,umbrella_id)
--      contracts(38 cols)
--      contract_weeks(id,contract_id,week_ending_date,timesheet_id,updated_at)
--      timesheets(timesheet_id,contract_id,is_current)
--      timesheets_financials(timesheet_id,is_current,locked_by_invoice_id,paid_at_utc)
--      ts_financials_outbox(timesheet_id,reason,attempt_count,next_attempt_at,last_error,created_at)
--      audit via public._audit_insert(...)
--      change counters via public._change_bump(...)
-- =========================================================
