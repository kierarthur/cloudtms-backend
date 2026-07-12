BEGIN;

CREATE TABLE IF NOT EXISTS public.timesheet_financial_retention (
  timesheet_id uuid PRIMARY KEY,
  first_retained_at_utc timestamptz NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT timesheet_financial_retention_timesheet_fk
    FOREIGN KEY (timesheet_id)
    REFERENCES public.timesheets(timesheet_id)
    ON UPDATE RESTRICT
    ON DELETE RESTRICT
);

COMMENT ON TABLE public.timesheet_financial_retention IS
  'Sticky exact-identity authority: a row means the timesheet crossed a durable financial-retention boundary and cannot be physically deleted or Unprocessed.';
COMMENT ON COLUMN public.timesheet_financial_retention.timesheet_id IS
  'Exact authoritative timesheet identity. Primary-key lookup is the Delete/Unprocess hot path.';
COMMENT ON COLUMN public.timesheet_financial_retention.first_retained_at_utc IS
  'First marker creation time. Marker writers use ON CONFLICT DO NOTHING; this value is never refreshed.';

REVOKE ALL ON TABLE public.timesheet_financial_retention FROM PUBLIC;
REVOKE ALL ON TABLE public.timesheet_financial_retention FROM anon;
REVOKE ALL ON TABLE public.timesheet_financial_retention FROM authenticated;
REVOKE ALL ON TABLE public.timesheet_financial_retention FROM service_role;

COMMIT;

BEGIN;

-- INSERT and UPDATE are deliberately separate because PostgreSQL transition

-- relations are event-specific.  The trigger function performs only transition
-- filtering, one distinct-array aggregation, and at most one helper call.

DROP TRIGGER IF EXISTS trg_retention_capture_timesheets_financials_insert ON public.timesheets_financials;
CREATE TRIGGER trg_retention_capture_timesheets_financials_insert
AFTER INSERT ON public.timesheets_financials
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION public.timesheet_financial_retention_capture_trigger_v1();
DROP TRIGGER IF EXISTS trg_retention_capture_timesheets_financials_update ON public.timesheets_financials;
CREATE TRIGGER trg_retention_capture_timesheets_financials_update
AFTER UPDATE ON public.timesheets_financials
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION public.timesheet_financial_retention_capture_trigger_v1();

DROP TRIGGER IF EXISTS trg_retention_capture_pay_batch_items_insert ON public.pay_batch_items;
CREATE TRIGGER trg_retention_capture_pay_batch_items_insert
AFTER INSERT ON public.pay_batch_items
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION public.timesheet_financial_retention_capture_trigger_v1();
DROP TRIGGER IF EXISTS trg_retention_capture_pay_batch_items_update ON public.pay_batch_items;
CREATE TRIGGER trg_retention_capture_pay_batch_items_update
AFTER UPDATE ON public.pay_batch_items
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION public.timesheet_financial_retention_capture_trigger_v1();

DROP TRIGGER IF EXISTS trg_retention_capture_pay_batches_insert ON public.pay_batches;
CREATE TRIGGER trg_retention_capture_pay_batches_insert
AFTER INSERT ON public.pay_batches
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION public.timesheet_financial_retention_capture_trigger_v1();
DROP TRIGGER IF EXISTS trg_retention_capture_pay_batches_update ON public.pay_batches;
CREATE TRIGGER trg_retention_capture_pay_batches_update
AFTER UPDATE ON public.pay_batches
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION public.timesheet_financial_retention_capture_trigger_v1();

DROP TRIGGER IF EXISTS trg_retention_capture_pay_batch_timesheet_snapshots_insert ON public.pay_batch_timesheet_snapshots;
CREATE TRIGGER trg_retention_capture_pay_batch_timesheet_snapshots_insert
AFTER INSERT ON public.pay_batch_timesheet_snapshots
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION public.timesheet_financial_retention_capture_trigger_v1();

DROP TRIGGER IF EXISTS trg_retention_capture_pay_batch_timesheet_snapshots_update ON public.pay_batch_timesheet_snapshots;
CREATE TRIGGER trg_retention_capture_pay_batch_timesheet_snapshots_update
AFTER UPDATE ON public.pay_batch_timesheet_snapshots
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION public.timesheet_financial_retention_capture_trigger_v1();

DROP TRIGGER IF EXISTS trg_retention_capture_timesheet_pay_state_insert ON public.timesheet_pay_state;
CREATE TRIGGER trg_retention_capture_timesheet_pay_state_insert
AFTER INSERT ON public.timesheet_pay_state
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION public.timesheet_financial_retention_capture_trigger_v1();
DROP TRIGGER IF EXISTS trg_retention_capture_timesheet_pay_state_update ON public.timesheet_pay_state;
CREATE TRIGGER trg_retention_capture_timesheet_pay_state_update
AFTER UPDATE ON public.timesheet_pay_state
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION public.timesheet_financial_retention_capture_trigger_v1();

DROP TRIGGER IF EXISTS trg_retention_capture_timesheet_pay_state_history_insert ON public.timesheet_pay_state_history;
CREATE TRIGGER trg_retention_capture_timesheet_pay_state_history_insert
AFTER INSERT ON public.timesheet_pay_state_history
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION public.timesheet_financial_retention_capture_trigger_v1();

DROP TRIGGER IF EXISTS trg_retention_capture_timesheet_pay_state_history_update ON public.timesheet_pay_state_history;
CREATE TRIGGER trg_retention_capture_timesheet_pay_state_history_update
AFTER UPDATE ON public.timesheet_pay_state_history
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION public.timesheet_financial_retention_capture_trigger_v1();

DROP TRIGGER IF EXISTS trg_retention_capture_ts_pay_adjustments_insert ON public.ts_pay_adjustments;
CREATE TRIGGER trg_retention_capture_ts_pay_adjustments_insert
AFTER INSERT ON public.ts_pay_adjustments
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION public.timesheet_financial_retention_capture_trigger_v1();

DROP TRIGGER IF EXISTS trg_retention_capture_ts_pay_adjustments_update ON public.ts_pay_adjustments;
CREATE TRIGGER trg_retention_capture_ts_pay_adjustments_update
AFTER UPDATE ON public.ts_pay_adjustments
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION public.timesheet_financial_retention_capture_trigger_v1();

DROP TRIGGER IF EXISTS trg_retention_capture_pay_finance_case_components_insert ON public.pay_finance_case_components;
CREATE TRIGGER trg_retention_capture_pay_finance_case_components_insert
AFTER INSERT ON public.pay_finance_case_components
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION public.timesheet_financial_retention_capture_trigger_v1();
DROP TRIGGER IF EXISTS trg_retention_capture_pay_finance_case_components_update ON public.pay_finance_case_components;
CREATE TRIGGER trg_retention_capture_pay_finance_case_components_update
AFTER UPDATE ON public.pay_finance_case_components
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION public.timesheet_financial_retention_capture_trigger_v1();

DROP TRIGGER IF EXISTS trg_retention_capture_pay_advances_insert ON public.pay_advances;
CREATE TRIGGER trg_retention_capture_pay_advances_insert
AFTER INSERT ON public.pay_advances
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION public.timesheet_financial_retention_capture_trigger_v1();
DROP TRIGGER IF EXISTS trg_retention_capture_pay_advances_update ON public.pay_advances;
CREATE TRIGGER trg_retention_capture_pay_advances_update
AFTER UPDATE ON public.pay_advances
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION public.timesheet_financial_retention_capture_trigger_v1();

DROP TRIGGER IF EXISTS trg_retention_capture_pay_payment_correction_items_insert ON public.pay_payment_correction_items;
CREATE TRIGGER trg_retention_capture_pay_payment_correction_items_insert
AFTER INSERT ON public.pay_payment_correction_items
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION public.timesheet_financial_retention_capture_trigger_v1();
DROP TRIGGER IF EXISTS trg_retention_capture_pay_payment_correction_items_update ON public.pay_payment_correction_items;
CREATE TRIGGER trg_retention_capture_pay_payment_correction_items_update
AFTER UPDATE ON public.pay_payment_correction_items
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION public.timesheet_financial_retention_capture_trigger_v1();

DROP TRIGGER IF EXISTS trg_retention_capture_pay_manual_adjustment_carry_forwards_insert ON public.pay_manual_adjustment_carry_forwards;
CREATE TRIGGER trg_retention_capture_pay_manual_adjustment_carry_forwards_insert
AFTER INSERT ON public.pay_manual_adjustment_carry_forwards
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION public.timesheet_financial_retention_capture_trigger_v1();
DROP TRIGGER IF EXISTS trg_retention_capture_pay_manual_adjustment_carry_forwards_update ON public.pay_manual_adjustment_carry_forwards;
CREATE TRIGGER trg_retention_capture_pay_manual_adjustment_carry_forwards_update
AFTER UPDATE ON public.pay_manual_adjustment_carry_forwards
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION public.timesheet_financial_retention_capture_trigger_v1();

DROP TRIGGER IF EXISTS trg_retention_capture_timesheet_payment_overrides_insert ON public.timesheet_payment_overrides;
CREATE TRIGGER trg_retention_capture_timesheet_payment_overrides_insert
AFTER INSERT ON public.timesheet_payment_overrides
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION public.timesheet_financial_retention_capture_trigger_v1();
DROP TRIGGER IF EXISTS trg_retention_capture_timesheet_payment_overrides_update ON public.timesheet_payment_overrides;
CREATE TRIGGER trg_retention_capture_timesheet_payment_overrides_update
AFTER UPDATE ON public.timesheet_payment_overrides
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION public.timesheet_financial_retention_capture_trigger_v1();

COMMIT;

