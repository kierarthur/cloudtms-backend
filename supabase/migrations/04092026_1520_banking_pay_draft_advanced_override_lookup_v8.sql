-- Bounded lookup support for the unchanged ADVANCE_THIS_PAYMENT integrity rule.
-- Runtime authority is Miget TEST. The `supabase` directory name is historical.
--
-- The Draft integrity owner only needs selected rows which actually carry an
-- advance marker. Keeping those rows in a partial index prevents an ordinary
-- large Draft from decoding every frozen payload during the final assertion.

CREATE INDEX IF NOT EXISTS banking_pay_draft_frozen_payloads_v8_advanced_override_idx
  ON private.banking_pay_draft_frozen_constituent_payloads_v8(
    operation_id,
    candidate_id,
    resolved_pay_channel,
    constituent_ordinal
  )
  WHERE (
    LOWER(COALESCE(payload_json->>'is_advanced', 'false')) IN ('true', 't', '1', 'yes', 'y')
    OR NULLIF(BTRIM(COALESCE(payload_json->>'advanced_override_id', '')), '') IS NOT NULL
  );

