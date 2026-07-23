-- CloudTMS Stage 1 controlled TEST cutover.
-- Do not apply with the base schema/repeatables. Apply only after SQL acceptance.
-- This transaction removes the legacy row-level PDF enqueue triggers before it
-- activates the replacement statement-level invalidation triggers.
begin;

do $check$
begin
  if to_regprocedure('public.trg_invoice_document_invalidate()') is null
     or to_regprocedure('public.trg_timesheet_document_invalidate()') is null then
    raise exception using errcode='55000',
      message='STAGE1_INVALIDATION_FUNCTIONS_NOT_INSTALLED';
  end if;
end;
$check$;

drop trigger if exists trg_timesheets_enqueue_pdf_regen_on_refs_change
  on public.timesheets;
drop trigger if exists trg_tsfin_enqueue_tspdf_on_refs_change_ai
  on public.timesheets_financials;
drop trigger if exists trg_tsfin_enqueue_tspdf_on_refs_change_au
  on public.timesheets_financials;

drop trigger if exists trg_invoice_document_invalidate_u on public.invoices;
create trigger trg_invoice_document_invalidate_u
after update on public.invoices
referencing old table as old_rows new table as new_rows
for each statement execute function public.trg_invoice_document_invalidate();

drop trigger if exists trg_invoice_lines_document_invalidate_i
  on public.invoice_lines;
create trigger trg_invoice_lines_document_invalidate_i
after insert on public.invoice_lines
referencing new table as new_rows
for each statement execute function public.trg_invoice_document_invalidate();

drop trigger if exists trg_invoice_lines_document_invalidate_u
  on public.invoice_lines;
create trigger trg_invoice_lines_document_invalidate_u
after update on public.invoice_lines
referencing old table as old_rows new table as new_rows
for each statement execute function public.trg_invoice_document_invalidate();

drop trigger if exists trg_invoice_lines_document_invalidate_d
  on public.invoice_lines;
create trigger trg_invoice_lines_document_invalidate_d
after delete on public.invoice_lines
referencing old table as old_rows
for each statement execute function public.trg_invoice_document_invalidate();

drop trigger if exists trg_invoice_hr_source_document_invalidate_i
  on public.invoice_hr_source_rows;
create trigger trg_invoice_hr_source_document_invalidate_i
after insert on public.invoice_hr_source_rows
referencing new table as new_rows
for each statement execute function public.trg_invoice_document_invalidate();

drop trigger if exists trg_invoice_hr_source_document_invalidate_u
  on public.invoice_hr_source_rows;
create trigger trg_invoice_hr_source_document_invalidate_u
after update on public.invoice_hr_source_rows
referencing old table as old_rows new table as new_rows
for each statement execute function public.trg_invoice_document_invalidate();

drop trigger if exists trg_invoice_hr_source_document_invalidate_d
  on public.invoice_hr_source_rows;
create trigger trg_invoice_hr_source_document_invalidate_d
after delete on public.invoice_hr_source_rows
referencing old table as old_rows
for each statement execute function public.trg_invoice_document_invalidate();

drop trigger if exists trg_timesheets_document_invalidate_u
  on public.timesheets;
create trigger trg_timesheets_document_invalidate_u
after update on public.timesheets
referencing old table as old_rows new table as new_rows
for each statement execute function public.trg_timesheet_document_invalidate();

drop trigger if exists trg_tsfin_document_invalidate_i
  on public.timesheets_financials;
create trigger trg_tsfin_document_invalidate_i
after insert on public.timesheets_financials
referencing new table as new_rows
for each statement execute function public.trg_timesheet_document_invalidate();

drop trigger if exists trg_tsfin_document_invalidate_u
  on public.timesheets_financials;
create trigger trg_tsfin_document_invalidate_u
after update on public.timesheets_financials
referencing old table as old_rows new table as new_rows
for each statement execute function public.trg_timesheet_document_invalidate();

drop trigger if exists trg_tsfin_document_invalidate_d
  on public.timesheets_financials;
create trigger trg_tsfin_document_invalidate_d
after delete on public.timesheets_financials
referencing old table as old_rows
for each statement execute function public.trg_timesheet_document_invalidate();

drop trigger if exists trg_timesheet_evidence_document_invalidate_i
  on public.timesheet_evidence;
create trigger trg_timesheet_evidence_document_invalidate_i
after insert on public.timesheet_evidence
referencing new table as new_rows
for each statement execute function public.trg_timesheet_document_invalidate();

drop trigger if exists trg_timesheet_evidence_document_invalidate_u
  on public.timesheet_evidence;
create trigger trg_timesheet_evidence_document_invalidate_u
after update on public.timesheet_evidence
referencing old table as old_rows new table as new_rows
for each statement execute function public.trg_timesheet_document_invalidate();

drop trigger if exists trg_timesheet_evidence_document_invalidate_d
  on public.timesheet_evidence;
create trigger trg_timesheet_evidence_document_invalidate_d
after delete on public.timesheet_evidence
referencing old table as old_rows
for each statement execute function public.trg_timesheet_document_invalidate();

commit;
