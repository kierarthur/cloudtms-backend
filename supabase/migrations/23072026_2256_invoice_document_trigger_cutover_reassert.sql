-- Stage 1 trigger cutover drift guard.
-- Retires the legacy row-level PDF enqueue triggers if an older repeatable
-- was replayed after the primary statement-level trigger cutover.

begin;

set local lock_timeout = '5s';

 drop trigger if exists trg_timesheets_enqueue_pdf_regen_on_refs_change
   on public.timesheets;
 drop trigger if exists trg_tsfin_enqueue_tspdf_on_refs_change_ai
   on public.timesheets_financials;
 drop trigger if exists trg_tsfin_enqueue_tspdf_on_refs_change_au
   on public.timesheets_financials;

commit;