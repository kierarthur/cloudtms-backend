-- CANDIDATE MIGRATION AMENDED AND RETURNED
-- The timesheets guard also covers INSERT so archive metadata cannot be
-- manufactured at row creation and bypass the authoritative Archive RPC.
BEGIN;

DROP TRIGGER IF EXISTS trg_timesheet_archive_row_guard_v1 ON public.timesheets;
CREATE TRIGGER trg_timesheet_archive_row_guard_v1
BEFORE INSERT OR UPDATE OR DELETE ON public.timesheets
FOR EACH ROW
EXECUTE FUNCTION public.timesheet_archive_row_guard_v1();

DROP TRIGGER IF EXISTS trg_timesheet_archived_evidence_guard_v1 ON public.timesheet_evidence;
CREATE TRIGGER trg_timesheet_archived_evidence_guard_v1
BEFORE INSERT OR UPDATE OR DELETE ON public.timesheet_evidence
FOR EACH ROW
EXECUTE FUNCTION public.timesheet_archived_evidence_guard_v1();

DROP TRIGGER IF EXISTS trg_invoice_line_archived_timesheet_guard_v1 ON public.invoice_lines;
CREATE TRIGGER trg_invoice_line_archived_timesheet_guard_v1
BEFORE INSERT OR UPDATE ON public.invoice_lines
FOR EACH ROW
EXECUTE FUNCTION public.invoice_line_archived_timesheet_guard_v1();

COMMIT;
