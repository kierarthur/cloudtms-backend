\set ON_ERROR_STOP on

-- Extends the minimal V8 transport fixture with only the immutable certificate facts
-- and existing Draft artifact columns needed to exercise the H2 parity comparator.
ALTER TABLE private.banking_pay_workbench_settled_certificate_entries_v8
  ADD COLUMN materialised_preview_row_id uuid,
  ADD COLUMN row_key text,
  ADD COLUMN timesheet_id uuid,
  ADD COLUMN semantic_kind text,
  ADD COLUMN economic_key_timesheet_id uuid,
  ADD COLUMN economic_key_type text NOT NULL DEFAULT 'TS_TOTAL',
  ADD COLUMN economic_key_value text NOT NULL DEFAULT 'TOTAL',
  ADD COLUMN expected_allocation_basis_kind text,
  ADD COLUMN expected_allocated_recovery_amount_ex_vat text,
  ADD COLUMN expected_allocation_result text,
  ADD COLUMN expected_allocation_source_digest_sha256 text,
  ADD COLUMN expected_item_semantic_kind text,
  ADD COLUMN expected_item_source_identity_digest_sha256 text,
  ADD COLUMN expected_item_amount_ex_vat text,
  ADD COLUMN expected_item_source_digest_sha256 text,
  ADD COLUMN expected_reservation_applicability text,
  ADD COLUMN expected_reservation_amount_ex_vat text,
  ADD COLUMN expected_reservation_source_digest_sha256 text,
  ADD COLUMN constituent_digest_sha256 text;

CREATE TABLE private.banking_pay_workbench_settled_cert_source_reservations_v8 (
  certificate_uuid uuid NOT NULL,
  constituent_ordinal integer NOT NULL,
  reservation_ordinal integer NOT NULL,
  source_reservation_id text NOT NULL,
  PRIMARY KEY (certificate_uuid, constituent_ordinal, reservation_ordinal),
  FOREIGN KEY (certificate_uuid, constituent_ordinal)
    REFERENCES private.banking_pay_workbench_settled_certificate_entries_v8(certificate_uuid, constituent_ordinal)
);

CREATE TABLE public.pay_batch_candidates (
  id uuid PRIMARY KEY,
  pay_batch_id uuid NOT NULL REFERENCES public.pay_batches(id),
  candidate_id uuid NOT NULL
);

CREATE TABLE public.pay_batch_items (
  id uuid PRIMARY KEY,
  pay_batch_candidate_id uuid NOT NULL REFERENCES public.pay_batch_candidates(id),
  item_type text NOT NULL,
  timesheet_id uuid,
  source_ref text,
  amount_ex_vat numeric,
  amount_vat numeric,
  amount_inc_vat numeric,
  pay_channel text NOT NULL,
  is_voided boolean NOT NULL DEFAULT false,
  finance_case_id uuid,
  reservation_id uuid,
  paye_treatment text,
  finance_component_id uuid,
  frozen_component_classification text,
  frozen_component_key_type text,
  frozen_component_key_value text,
  frozen_source_pay_method text,
  frozen_target_pay_method text,
  frozen_source_amount numeric,
  operation_source_key text
);

CREATE TABLE public.pay_advance_reservations (
  id uuid PRIMARY KEY,
  finance_case_id uuid NOT NULL,
  pay_batch_id uuid NOT NULL REFERENCES public.pay_batches(id),
  pay_batch_candidate_id uuid,
  pay_batch_item_id uuid,
  reserved_amount numeric NOT NULL,
  status text NOT NULL,
  finance_component_id uuid,
  frozen_component_classification text,
  frozen_component_key_type text,
  frozen_component_key_value text,
  reserved_source_amount numeric,
  frozen_rounded_target_amount numeric
);

ALTER TABLE public.pay_batch_items
  ADD CONSTRAINT pay_batch_items_reservation_fixture_fk
  FOREIGN KEY (reservation_id) REFERENCES public.pay_advance_reservations(id);

CREATE TABLE public.banking_pay_operation_candidate_allocation_rows (
  id uuid PRIMARY KEY,
  operation_id uuid NOT NULL REFERENCES public.banking_pay_operations(id),
  candidate_scope_id uuid NOT NULL REFERENCES public.banking_pay_operation_candidate_scope(id),
  pay_batch_id uuid,
  candidate_id uuid NOT NULL,
  pay_channel text NOT NULL,
  finance_case_id uuid,
  finance_component_id uuid,
  allocation_type text NOT NULL,
  source_ref text,
  operation_source_key text NOT NULL,
  allocated_amount numeric NOT NULL,
  allocation_basis_json jsonb NOT NULL,
  sort_order integer NOT NULL,
  status text NOT NULL,
  pay_batch_item_id uuid REFERENCES public.pay_batch_items(id),
  UNIQUE (operation_id, operation_source_key)
);

CREATE OR REPLACE FUNCTION public._pay_batch_item_source_reservation_amount_ex_vat(p_pay_batch_item_id uuid)
RETURNS numeric
LANGUAGE sql
STABLE
SECURITY INVOKER
SET search_path TO ''
AS $function$
  SELECT item.frozen_source_amount
  FROM public.pay_batch_items AS item
  WHERE item.id = p_pay_batch_item_id
$function$;
