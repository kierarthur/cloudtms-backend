-- Bounded lookup support for constituent-level Draft parity.
-- Runtime authority is Miget TEST. The `supabase` directory name is historical.
--
-- The parity owner already matches a frozen constituent to its persisted
-- allocation by operation plus the producer-owned preview identity. Without
-- this lookup PostgreSQL scans every allocation in the operation once per
-- constituent. This index changes no row, policy decision, amount, category,
-- channel, tax/VAT treatment, reservation, settlement or provider behaviour.

CREATE INDEX IF NOT EXISTS idx_bpay_operation_allocation_preview_row_v8
  ON public.banking_pay_operation_candidate_allocation_rows (
    operation_id,
    (
      COALESCE(
        NULLIF(allocation_basis_json->>'preview_row_id', ''),
        NULLIF(allocation_basis_json#>>'{line,preview_row_id}', ''),
        NULLIF(allocation_basis_json#>>'{line,preview_row_pk}', '')
      )
    )
  );
