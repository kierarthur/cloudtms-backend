-- Banking Pay bounded-scope Version 1.2.8 operational state.
--
-- This relation is non-economic queue coordination only. It gives each worker
-- lane a durable keyset cursor which advances independently of a job row that
-- may itself be locked. No public role can read or write it directly.

CREATE TABLE IF NOT EXISTS private.banking_pay_workbench_queue_scan_state (
  lane_identity text NOT NULL,
  scan_kind text NOT NULL,
  scan_scope_key text NOT NULL,
  cursor_generation bigint NULL,
  cursor_chain_rank smallint NULL,
  cursor_priority integer NULL,
  cursor_due_at timestamptz NULL,
  cursor_created_at timestamptz NULL,
  cursor_object_id uuid NULL,
  sweep_generation bigint NOT NULL DEFAULT 0,
  created_at_utc timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at_utc timestamptz NOT NULL DEFAULT clock_timestamp(),

  CONSTRAINT bpay_wb_queue_scan_state_pkey
    PRIMARY KEY (lane_identity,scan_kind,scan_scope_key),
  CONSTRAINT bpay_wb_queue_scan_state_identity_chk CHECK (
    NULLIF(btrim(lane_identity),'') IS NOT NULL
    AND char_length(lane_identity) <= 200
    AND scan_kind IN ('CLAIM','RECOVERY')
    AND NULLIF(btrim(scan_scope_key),'') IS NOT NULL
    AND char_length(scan_scope_key) <= 160
  ),
  CONSTRAINT bpay_wb_queue_scan_state_cursor_chk CHECK (
    (
      cursor_object_id IS NULL
      AND cursor_generation IS NULL
      AND cursor_chain_rank IS NULL
      AND cursor_priority IS NULL
      AND cursor_due_at IS NULL
      AND cursor_created_at IS NULL
    )
    OR (
      scan_kind = 'CLAIM'
      AND cursor_object_id IS NOT NULL
      AND cursor_generation IS NOT NULL
      AND cursor_generation >= 0
      AND cursor_chain_rank IN (0,1)
      AND cursor_priority IS NOT NULL
      AND cursor_due_at IS NOT NULL
      AND cursor_created_at IS NOT NULL
    )
    OR (
      scan_kind = 'RECOVERY'
      AND cursor_object_id IS NOT NULL
      AND cursor_generation IS NOT NULL
      AND cursor_generation >= 0
      AND cursor_chain_rank IS NULL
      AND cursor_priority IS NULL
      AND cursor_due_at IS NOT NULL
      AND cursor_created_at IS NULL
    )
  ),
  CONSTRAINT bpay_wb_queue_scan_state_sweep_chk CHECK (sweep_generation >= 0)
);

ALTER TABLE private.banking_pay_workbench_queue_scan_state OWNER TO postgres;
REVOKE ALL ON TABLE private.banking_pay_workbench_queue_scan_state
  FROM PUBLIC,anon,authenticated,service_role;
GRANT ALL ON TABLE private.banking_pay_workbench_queue_scan_state TO postgres;

CREATE INDEX IF NOT EXISTS idx_bpay_wb_batch_candidates_candidate_id
  ON public.pay_batch_candidates (candidate_id,id);

CREATE INDEX IF NOT EXISTS idx_bpay_wb_finance_item_authority_page
  ON public.pay_batch_items (pay_batch_candidate_id,id)
  WHERE item_type IN ('OVERPAYMENT_RECOVERY','UNDERPAYMENT_PAYMENT')
    AND COALESCE(is_voided,false) IS FALSE;

CREATE INDEX IF NOT EXISTS idx_bpay_wb_finance_item_timesheet_page
  ON public.pay_batch_items (timesheet_id,id)
  WHERE item_type IN ('OVERPAYMENT_RECOVERY','UNDERPAYMENT_PAYMENT')
    AND COALESCE(is_voided,false) IS FALSE;

CREATE INDEX IF NOT EXISTS idx_bpay_wb_finance_item_case_page
  ON public.pay_batch_items (finance_case_id,id)
  WHERE item_type IN ('OVERPAYMENT_RECOVERY','UNDERPAYMENT_PAYMENT')
    AND COALESCE(is_voided,false) IS FALSE;

CREATE INDEX IF NOT EXISTS idx_bpay_wb_finance_item_component_page
  ON public.pay_batch_items (finance_component_id,id)
  WHERE item_type IN ('OVERPAYMENT_RECOVERY','UNDERPAYMENT_PAYMENT')
    AND COALESCE(is_voided,false) IS FALSE;

CREATE INDEX IF NOT EXISTS idx_bpay_wb_finance_item_frozen_basis
  ON public.pay_batch_items USING gin (frozen_source_basis_json jsonb_path_ops)
  WHERE item_type IN ('OVERPAYMENT_RECOVERY','UNDERPAYMENT_PAYMENT')
    AND COALESCE(is_voided,false) IS FALSE;

CREATE INDEX IF NOT EXISTS idx_bpay_wb_finance_item_frozen_snapshot
  ON public.pay_batch_items USING gin (frozen_component_snapshot_json jsonb_path_ops)
  WHERE item_type IN ('OVERPAYMENT_RECOVERY','UNDERPAYMENT_PAYMENT')
    AND COALESCE(is_voided,false) IS FALSE;

-- FINANCE_ITEM_AUTHORITY is a build-global physical input stream. It is
-- collected once by item-id keyset before per-unit settled derivation, so a
-- candidate's finance history is never rediscovered for every projection.
ALTER TABLE private.banking_pay_workbench_economic_build_facts
  DROP CONSTRAINT IF EXISTS bpay_wb_economic_build_facts_family_chk;
ALTER TABLE private.banking_pay_workbench_economic_build_facts
  ADD CONSTRAINT bpay_wb_economic_build_facts_family_chk CHECK (fact_family IN (
    'DEPENDENCY_EDGE','FINANCE_ITEM_AUTHORITY','FROZEN_SETTLED_COMPONENT',
    'LIVE_ENTITLEMENT_INPUT','ENTITLEMENT_COMPONENT','RESERVATION_COMPONENT',
    'PAY_STATE_FALLBACK','FINANCE_CASE_IDENTITY','FINANCE_COMPONENT_IDENTITY',
    'PROTECTION_EVIDENCE','PAYEE_BASELINE_INPUT','ALLOCATION_INPUT','CANONICAL_INPUT'
  ));

ALTER TABLE private.banking_pay_workbench_economic_build_facts
  DROP CONSTRAINT IF EXISTS bpay_wb_economic_build_facts_unit_chk;
ALTER TABLE private.banking_pay_workbench_economic_build_facts
  ADD CONSTRAINT bpay_wb_economic_build_facts_unit_chk CHECK (
    (fact_family = 'DEPENDENCY_EDGE' AND (
      dependency_unit_key IS NULL
      OR (dependency_unit_key <> 'GLOBAL' AND btrim(dependency_unit_key) <> '')
    ))
    OR (fact_family IN (
      'FINANCE_ITEM_AUTHORITY','RESERVATION_COMPONENT','FINANCE_CASE_IDENTITY',
      'FINANCE_COMPONENT_IDENTITY','PROTECTION_EVIDENCE','ALLOCATION_INPUT'
    ) AND dependency_unit_key = 'GLOBAL')
    OR (fact_family IN (
      'FROZEN_SETTLED_COMPONENT','LIVE_ENTITLEMENT_INPUT','ENTITLEMENT_COMPONENT',
      'PAY_STATE_FALLBACK','PAYEE_BASELINE_INPUT','CANONICAL_INPUT'
    ) AND NULLIF(btrim(dependency_unit_key),'') IS NOT NULL
      AND dependency_unit_key <> 'GLOBAL')
  );

ALTER TABLE private.banking_pay_workbench_economic_build_facts
  DROP CONSTRAINT IF EXISTS bpay_wb_economic_build_facts_authority_chk;
ALTER TABLE private.banking_pay_workbench_economic_build_facts
  ADD CONSTRAINT bpay_wb_economic_build_facts_authority_chk CHECK (
    (fact_family <> 'FINANCE_ITEM_AUTHORITY' OR source_id IS NOT NULL)
    AND (fact_family <> 'FROZEN_SETTLED_COMPONENT' OR (
      economic_key_type IS NOT NULL AND (amount_ex_vat IS NOT NULL OR amount_inc_vat IS NOT NULL)
    ))
    AND (fact_family <> 'ENTITLEMENT_COMPONENT' OR (
      economic_key_type IS NOT NULL
      AND (truth_ex_vat IS NOT NULL OR truth_inc_vat IS NOT NULL)
      AND (baseline_ex_vat IS NOT NULL OR baseline_inc_vat IS NOT NULL)
    ))
    AND (fact_family <> 'RESERVATION_COMPONENT' OR (
      reservation_id IS NOT NULL AND economic_key_type IS NOT NULL
      AND reserved_source_amount IS NOT NULL
    ))
    AND (fact_family <> 'FINANCE_CASE_IDENTITY' OR finance_case_id IS NOT NULL)
    AND (fact_family <> 'FINANCE_COMPONENT_IDENTITY' OR (
      finance_case_id IS NOT NULL AND finance_component_id IS NOT NULL
    ))
    AND (fact_family <> 'PROTECTION_EVIDENCE' OR (
      finance_case_id IS NOT NULL OR finance_component_id IS NOT NULL
      OR reservation_id IS NOT NULL OR source_id IS NOT NULL
    ))
    AND (fact_family <> 'PAYEE_BASELINE_INPUT' OR (
      economic_key_type IS NOT NULL
      AND (truth_ex_vat IS NOT NULL OR truth_inc_vat IS NOT NULL
        OR baseline_ex_vat IS NOT NULL OR baseline_inc_vat IS NOT NULL)
    ))
    AND (fact_family <> 'ALLOCATION_INPUT' OR (
      economic_key_type IS NOT NULL
      AND (amount_ex_vat IS NOT NULL OR amount_inc_vat IS NOT NULL
        OR reserved_source_amount IS NOT NULL)
    ))
    AND (fact_family <> 'CANONICAL_INPUT' OR (
      economic_key_type IS NOT NULL
      AND (amount_ex_vat IS NOT NULL OR amount_inc_vat IS NOT NULL
        OR truth_ex_vat IS NOT NULL OR truth_inc_vat IS NOT NULL
        OR baseline_ex_vat IS NOT NULL OR baseline_inc_vat IS NOT NULL)
    ))
  );
