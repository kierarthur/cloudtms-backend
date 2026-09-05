-- Create Draft V8 finalizer lookup support.
-- Runtime authority is Miget TEST; the supabase/ directory name is historical.
-- This index changes no selection, amount, finance, tax, VAT, reservation,
-- grouping, payment or Policy X decision. It gives the existing bounded
-- finalizer an indexed path to the exact operation/Candidate-scope/batch items
-- whose Timesheet summaries it already validates and refreshes.

CREATE INDEX IF NOT EXISTS idx_bpay_operation_allocation_scope_batch_item_v8
  ON public.banking_pay_operation_candidate_allocation_rows (
    operation_id,
    candidate_scope_id,
    pay_batch_id,
    pay_batch_item_id
  );
