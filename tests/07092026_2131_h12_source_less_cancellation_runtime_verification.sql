\set ON_ERROR_STOP on

-- H12 source-less manual-adjustment cancellation fixture.
--
-- This file is intentionally not a release verifier.  Its runner permits it
-- only in a task-owned disposable clone and removes the clone after each run.
-- It adds frozen test items; it never calls a provider, settlement, remittance,
-- email or background drain owner.

SELECT pg_catalog.set_config('h12.batch_id', :'h12_batch_id', false);
SELECT pg_catalog.set_config('h12.batch_channel', :'h12_batch_channel', false);
SELECT pg_catalog.set_config('h12.actor_id', :'h12_actor_id', false);
SELECT pg_catalog.set_config('h12.fixture_mode', :'h12_fixture_mode', false);

DO $h12_guard$
BEGIN
  IF pg_catalog.current_database() !~ '^(h2_cancel_v8|h12_rg5_builder_cancel)_pg(17|18)$' THEN
    RAISE EXCEPTION 'H12_DISPOSABLE_DATABASE_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = pg_catalog.jsonb_build_object(
              'code', 'H12_DISPOSABLE_DATABASE_REQUIRED',
              'database', pg_catalog.current_database()
            )::text;
  END IF;

  IF pg_catalog.current_setting('h12.batch_channel') NOT IN ('PAYE', 'UMBRELLA') THEN
    RAISE EXCEPTION 'H12_BATCH_CHANNEL_INVALID'
      USING ERRCODE = 'P0001';
  END IF;

  IF pg_catalog.current_setting('h12.fixture_mode') NOT IN ('FULL', 'MAIL_ONLY') THEN
    RAISE EXCEPTION 'H12_FIXTURE_MODE_INVALID'
      USING ERRCODE = 'P0001';
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM public.pay_batches AS batch_row
    WHERE batch_row.id = pg_catalog.current_setting('h12.batch_id')::uuid
      AND batch_row.status IN ('DRAFT', 'SCHEDULED')
      AND batch_row.batch_kind_fixed = pg_catalog.current_setting('h12.batch_channel')
  ) THEN
    RAISE EXCEPTION 'H12_CANCELLABLE_BATCH_FIXTURE_NOT_FOUND'
      USING ERRCODE = 'P0001';
  END IF;

  IF pg_catalog.to_regclass('private.h12_source_less_cancellation_fixture_v1') IS NOT NULL THEN
    RAISE EXCEPTION 'H12_FIXTURE_ALREADY_EXISTS'
      USING ERRCODE = 'P0001';
  END IF;

  IF pg_catalog.to_regclass('private.h12_source_less_cancellation_mail_fixture_v1') IS NOT NULL THEN
    RAISE EXCEPTION 'H12_MAIL_FIXTURE_ALREADY_EXISTS'
      USING ERRCODE = 'P0001';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM public.pay_payment_correction_requests AS old_request
    WHERE (
        old_request.plan_json->>'candidate_scope_contract_version' = '1'
        OR (
          old_request.plan_json->>'candidate_scope_contract_version' = '2'
          AND old_request.plan_json->>'communication_cleanup_contract_version' = '1'
        )
      )
      AND (
        old_request.status IS NULL
        OR old_request.status IN (
          'PLANNING', 'PLANNED', 'REQUESTED', 'AWAITING_AUTHORISATION',
          'AUTHORISED', 'EXPANDED', 'PROCESSING'
        )
        OR old_request.status NOT IN (
          'PLANNING', 'PLANNED', 'REQUESTED', 'AWAITING_AUTHORISATION',
          'AUTHORISED', 'EXPANDED', 'PROCESSING', 'APPLIED',
          'APPLIED_WITH_BLOCKERS', 'BLOCKED', 'FAILED', 'REJECTED', 'CANCELLED'
        )
        OR EXISTS (
          SELECT 1
          FROM public.banking_pay_operations AS linked_operation
          WHERE linked_operation.operation_type = 'PAYMENT_CORRECTION'
            AND linked_operation.input_json->>'correction_request_id' = old_request.id::text
            AND (
              linked_operation.status IS NULL
              OR linked_operation.status IN (
                'QUEUED', 'RUNNING', 'WAITING', 'WAITING_AUTHORISATION',
                'WAITING_PROVIDER', 'REVIEW_REQUIRED'
              )
              OR linked_operation.status NOT IN (
                'QUEUED', 'RUNNING', 'WAITING', 'WAITING_AUTHORISATION',
                'WAITING_PROVIDER', 'REVIEW_REQUIRED', 'COMPLETE', 'FAILED', 'CANCELLED'
              )
            )
        )
      )
  ) THEN
    RAISE EXCEPTION 'H12_ACTIVE_OLD_COMMUNICATION_CONTRACT_RELEASE_PRECONDITION_FAILED'
      USING ERRCODE = 'P0001',
            DETAIL = pg_catalog.jsonb_build_object(
              'code', 'H12_ACTIVE_OLD_COMMUNICATION_CONTRACT_RELEASE_PRECONDITION_FAILED',
              'action', 'STOP_RELEASE_AND_REVIEW_EXISTING_NONTERMINAL_REQUESTS'
            )::text;
  END IF;
END;
$h12_guard$;

-- The cancellation owners persist the acting user on investigation evidence.
-- The established V8 fixture uses a synthetic UUID that is intentionally not
-- present in the disposable clone's minimal auth.users relation, so install
-- that exact fixture identity before any cancellation work is attempted.
INSERT INTO auth.users (id)
VALUES (pg_catalog.current_setting('h12.actor_id')::uuid)
ON CONFLICT (id) DO NOTHING;

CREATE TABLE private.h12_source_less_cancellation_fixture_v1 (
  batch_id uuid NOT NULL,
  pay_batch_candidate_id uuid NOT NULL,
  candidate_id uuid NOT NULL,
  pay_batch_item_id uuid NOT NULL,
  fixture_kind text NOT NULL CHECK (fixture_kind IN ('ORDINARY_SOURCE_BACKED', 'SOURCE_LESS_SAFE', 'SOURCE_LESS_AMBIGUOUS')),
  ambiguity_reason text NULL,
  expected_pay_channel text NOT NULL,
  expected_description text NULL,
  expected_amount_ex_vat numeric NULL,
  expected_amount_vat numeric NULL,
  expected_amount_inc_vat numeric NULL,
  expected_paye_treatment text NULL,
  PRIMARY KEY (pay_batch_item_id)
);

WITH target_candidates AS (
  SELECT
    candidate_row.id AS pay_batch_candidate_id,
    candidate_row.candidate_id,
    source_item.id AS ordinary_item_id,
    source_item.description AS ordinary_description,
    source_item.amount_ex_vat AS ordinary_amount_ex_vat,
    source_item.amount_vat AS ordinary_amount_vat,
    source_item.amount_inc_vat AS ordinary_amount_inc_vat,
    source_item.paye_treatment AS ordinary_paye_treatment
  FROM public.pay_batch_candidates AS candidate_row
  JOIN LATERAL (
    SELECT
      item_row.id,
      item_row.description,
      item_row.amount_ex_vat,
      item_row.amount_vat,
      item_row.amount_inc_vat,
      item_row.paye_treatment
    FROM public.pay_batch_items AS item_row
    WHERE item_row.pay_batch_candidate_id = candidate_row.id
      AND COALESCE(item_row.is_voided, false) IS FALSE
      AND item_row.timesheet_id IS NOT NULL
    ORDER BY item_row.id
    LIMIT 1
  ) AS source_item ON true
  WHERE candidate_row.pay_batch_id = :'h12_batch_id'::uuid
), recorded AS (
  INSERT INTO private.h12_source_less_cancellation_fixture_v1 (
    batch_id,
    pay_batch_candidate_id,
    candidate_id,
    pay_batch_item_id,
    fixture_kind,
    ambiguity_reason,
    expected_pay_channel,
    expected_description,
    expected_amount_ex_vat,
    expected_amount_vat,
    expected_amount_inc_vat,
    expected_paye_treatment
  )
  SELECT
    :'h12_batch_id'::uuid,
    target_candidate.pay_batch_candidate_id,
    target_candidate.candidate_id,
    target_candidate.ordinary_item_id,
    'ORDINARY_SOURCE_BACKED',
    NULL,
    :'h12_batch_channel',
    target_candidate.ordinary_description,
    target_candidate.ordinary_amount_ex_vat,
    target_candidate.ordinary_amount_vat,
    target_candidate.ordinary_amount_inc_vat,
    target_candidate.ordinary_paye_treatment
  FROM target_candidates AS target_candidate
  RETURNING pay_batch_item_id
)
SELECT pg_catalog.count(*)
FROM recorded;

-- Mail delivery state is deliberately outside financial cancellation
-- authority.  These four shapes are adapted from immutable pre-change fixture
-- 07092026_1700_h12_unpaid_cancellation_unsafe_mail_prechange_runtime.sql.
-- No sender or provider routine is called.  The related unsafe rows prove that
-- queued/claimed mail cannot veto a confirmed-unpaid cancellation; the
-- mismatched and already-sent rows prove all mail evidence remains untouched.
CREATE TABLE private.h12_source_less_cancellation_mail_fixture_v1 (
  mail_outbox_id uuid PRIMARY KEY,
  batch_id uuid NOT NULL,
  pay_batch_candidate_id uuid NOT NULL,
  candidate_id uuid NOT NULL,
  fixture_kind text NOT NULL CHECK (fixture_kind IN (
    'UNSAFE_RELATED_QUEUED',
    'UNSAFE_RELATED_CLAIMED',
    'MISMATCHED_QUEUED',
    'RELATED_SENT'
  )),
  expected_status text NOT NULL,
  expected_attempt_lease_token text NULL,
  immutable_row_md5 text NOT NULL
);

WITH target_candidates AS (
  SELECT candidate_row.id AS pay_batch_candidate_id,
         candidate_row.candidate_id
  FROM public.pay_batch_candidates AS candidate_row
  WHERE candidate_row.pay_batch_id = :'h12_batch_id'::uuid
), mail_shapes AS (
  SELECT *
  FROM (VALUES
    ('UNSAFE_RELATED_QUEUED'::text, 1),
    ('UNSAFE_RELATED_CLAIMED'::text, 2),
    ('MISMATCHED_QUEUED'::text, 3),
    ('RELATED_SENT'::text, 4)
  ) AS shape_row(fixture_kind, ordinal)
), inserted AS (
  INSERT INTO public.mail_outbox (
    id,
    type,
    subject,
    "to",
    status,
    context_kind,
    context_id,
    payment_scope_json,
    created_at_utc,
    scheduled_for_utc,
    next_attempt_at_utc,
    sent_at,
    provider_message_id,
    provider_status,
    attempt_lease_token,
    attempt_leased_at_utc,
    attempt_lease_expires_at_utc
  )
  SELECT
    pg_catalog.md5(
      'h12-source-less-mail:' ||
      target_candidate.pay_batch_candidate_id::text || ':' ||
      mail_shape.fixture_kind
    )::uuid,
    'TSO_FAILURE',
    'H12 cancellation mail-independence fixture',
    'h12-no-delivery@example.invalid',
    CASE WHEN mail_shape.fixture_kind = 'RELATED_SENT'
      THEN 'SENT'::public.mail_status_enum
      ELSE 'QUEUED'::public.mail_status_enum
    END,
    'pay_batches',
    CASE WHEN mail_shape.fixture_kind = 'MISMATCHED_QUEUED'
      THEN pg_catalog.md5(
        'h12-source-less-mail-other-batch:' ||
        target_candidate.pay_batch_candidate_id::text
      )::uuid
      ELSE :'h12_batch_id'::uuid
    END,
    CASE WHEN mail_shape.fixture_kind = 'MISMATCHED_QUEUED' THEN
      pg_catalog.jsonb_build_object(
        'pay_batch_id', pg_catalog.md5(
          'h12-source-less-mail-other-batch:' ||
          target_candidate.pay_batch_candidate_id::text
        )::uuid,
        'candidate_id', pg_catalog.md5(
          'h12-source-less-mail-other-candidate:' ||
          target_candidate.candidate_id::text
        )::uuid,
        'fixture_contract', 'H12_SOURCE_LESS_CANCELLATION_RUNTIME_MATRIX_V1',
        'mail_shape', mail_shape.fixture_kind
      )
    ELSE
      pg_catalog.jsonb_build_object(
        'pay_batch_id', :'h12_batch_id'::uuid,
        'candidate_id', target_candidate.candidate_id,
        'fixture_contract', 'H12_SOURCE_LESS_CANCELLATION_RUNTIME_MATRIX_V1',
        'mail_shape', mail_shape.fixture_kind,
        'binding_intentionally_incomplete', true
      )
    END,
    TIMESTAMPTZ '2026-09-07 12:00:00+00' +
      (mail_shape.ordinal * interval '1 second'),
    CASE WHEN mail_shape.fixture_kind = 'RELATED_SENT'
      THEN TIMESTAMPTZ '2026-09-07 12:00:00+00'
      ELSE TIMESTAMPTZ '2100-01-01 00:00:00+00'
    END,
    CASE WHEN mail_shape.fixture_kind = 'RELATED_SENT'
      THEN NULL::timestamptz
      ELSE TIMESTAMPTZ '2100-01-01 00:00:00+00'
    END,
    CASE WHEN mail_shape.fixture_kind = 'RELATED_SENT'
      THEN TIMESTAMPTZ '2026-09-07 12:01:00+00'
      ELSE NULL::timestamptz
    END,
    CASE WHEN mail_shape.fixture_kind = 'RELATED_SENT'
      THEN 'h12-simulated-prior-provider-id'
      ELSE NULL::text
    END,
    CASE WHEN mail_shape.fixture_kind = 'RELATED_SENT'
      THEN 'ACCEPTED'
      ELSE NULL::text
    END,
    CASE WHEN mail_shape.fixture_kind = 'UNSAFE_RELATED_CLAIMED'
      THEN 'h12-mail-claimed-before-cancel'
      ELSE NULL::text
    END,
    CASE WHEN mail_shape.fixture_kind = 'UNSAFE_RELATED_CLAIMED'
      THEN TIMESTAMPTZ '2026-09-07 12:02:00+00'
      ELSE NULL::timestamptz
    END,
    CASE WHEN mail_shape.fixture_kind = 'UNSAFE_RELATED_CLAIMED'
      THEN TIMESTAMPTZ '2100-01-01 00:00:00+00'
      ELSE NULL::timestamptz
    END
  FROM target_candidates AS target_candidate
  CROSS JOIN mail_shapes AS mail_shape
  RETURNING *
), recorded AS (
  INSERT INTO private.h12_source_less_cancellation_mail_fixture_v1 (
    mail_outbox_id,
    batch_id,
    pay_batch_candidate_id,
    candidate_id,
    fixture_kind,
    expected_status,
    expected_attempt_lease_token,
    immutable_row_md5
  )
  SELECT
    inserted.id,
    :'h12_batch_id'::uuid,
    target_candidate.pay_batch_candidate_id,
    target_candidate.candidate_id,
    inserted.payment_scope_json->>'mail_shape',
    inserted.status::text,
    inserted.attempt_lease_token,
    pg_catalog.md5(pg_catalog.to_jsonb(inserted)::text)
  FROM inserted
  JOIN target_candidates AS target_candidate
    ON inserted.id = pg_catalog.md5(
      'h12-source-less-mail:' ||
      target_candidate.pay_batch_candidate_id::text || ':' ||
      (inserted.payment_scope_json->>'mail_shape')
    )::uuid
  RETURNING mail_outbox_id
)
SELECT pg_catalog.count(*)
FROM recorded;

DO $h12_mail_fixture_proof$
DECLARE
  expected_candidate_count integer;
  actual_mail_count integer;
  actual_mail_kind_count integer;
BEGIN
  SELECT pg_catalog.count(*)::integer
  INTO expected_candidate_count
  FROM public.pay_batch_candidates AS candidate_row
  WHERE candidate_row.pay_batch_id = pg_catalog.current_setting('h12.batch_id')::uuid;

  SELECT pg_catalog.count(*)::integer,
         pg_catalog.count(DISTINCT fixture_row.fixture_kind)::integer
  INTO actual_mail_count, actual_mail_kind_count
  FROM private.h12_source_less_cancellation_mail_fixture_v1 AS fixture_row;

  IF actual_mail_count <> expected_candidate_count * 4
     OR actual_mail_kind_count <> 4 THEN
    RAISE EXCEPTION 'H12_MAIL_FIXTURE_CARDINALITY_MISMATCH'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'candidate_count', expected_candidate_count,
        'mail_count', actual_mail_count,
        'mail_kind_count', actual_mail_kind_count
      )::text;
  END IF;
END;
$h12_mail_fixture_proof$;

WITH target_candidates AS (
  SELECT
    candidate_row.id AS pay_batch_candidate_id,
    candidate_row.candidate_id,
    candidate_record.umbrella_id
  FROM public.pay_batch_candidates AS candidate_row
  JOIN public.candidates AS candidate_record
    ON candidate_record.id = candidate_row.candidate_id
  WHERE candidate_row.pay_batch_id = :'h12_batch_id'::uuid
    AND pg_catalog.current_setting('h12.fixture_mode') = 'FULL'
), inserted AS (
  INSERT INTO public.pay_batch_items (
    id,
    pay_batch_candidate_id,
    item_type,
    timesheet_id,
    segment_key,
    source_ref,
    description,
    amount_ex_vat,
    amount_vat,
    amount_inc_vat,
    pay_channel,
    umbrella_id,
    pay_bank_transfer_id,
    is_voided,
    finance_case_id,
    reservation_id,
    paye_treatment,
    finance_component_id,
    frozen_source_basis_json,
    operation_source_key
  )
  SELECT
    pg_catalog.md5(
      'h12-source-less-safe:' ||
      target_candidate.pay_batch_candidate_id::text || ':' ||
      :'h12_batch_channel'
    )::uuid,
    target_candidate.pay_batch_candidate_id,
    'ADJUSTMENT_DELTA',
    NULL,
    NULL,
    NULL,
    CASE :'h12_batch_channel'
      WHEN 'PAYE' THEN 'H12 exact frozen PAYE manual deduction'
      ELSE 'H12 exact frozen Umbrella manual credit'
    END,
    CASE :'h12_batch_channel' WHEN 'PAYE' THEN -0.25 ELSE 12.00 END,
    CASE :'h12_batch_channel' WHEN 'PAYE' THEN 0.00 ELSE 2.40 END,
    CASE :'h12_batch_channel' WHEN 'PAYE' THEN -0.25 ELSE 14.40 END,
    :'h12_batch_channel',
    CASE :'h12_batch_channel'
      WHEN 'UMBRELLA' THEN target_candidate.umbrella_id
      ELSE NULL::uuid
    END,
    NULL,
    false,
    NULL,
    NULL,
    CASE :'h12_batch_channel' WHEN 'PAYE' THEN 'NET_DEDUCT' ELSE NULL::text END,
    NULL,
    pg_catalog.jsonb_build_object(
      'fixture_contract', 'H12_SOURCE_LESS_CANCELLATION_RUNTIME_MATRIX_V1',
      'frozen_evidence_only', true
    ),
    NULL
  FROM target_candidates AS target_candidate
  RETURNING *
), recorded AS (
  INSERT INTO private.h12_source_less_cancellation_fixture_v1 (
    batch_id,
    pay_batch_candidate_id,
    candidate_id,
    pay_batch_item_id,
    fixture_kind,
    ambiguity_reason,
    expected_pay_channel,
    expected_description,
    expected_amount_ex_vat,
    expected_amount_vat,
    expected_amount_inc_vat,
    expected_paye_treatment
  )
  SELECT
    :'h12_batch_id'::uuid,
    inserted.pay_batch_candidate_id,
    candidate_row.candidate_id,
    inserted.id,
    'SOURCE_LESS_SAFE',
    NULL,
    inserted.pay_channel,
    inserted.description,
    inserted.amount_ex_vat,
    inserted.amount_vat,
    inserted.amount_inc_vat,
    inserted.paye_treatment
  FROM inserted
  JOIN public.pay_batch_candidates AS candidate_row
    ON candidate_row.id = inserted.pay_batch_candidate_id
  RETURNING pay_batch_item_id
)
SELECT pg_catalog.count(*)
FROM recorded;

WITH reason_rows AS (
  SELECT reason_row.reason, reason_row.ordinal
  FROM (
    VALUES
      ('MISSING_AMOUNT_INC_VAT'::text, 1),
      ('MISSING_DESCRIPTION'::text, 2),
      ('ZERO_AMOUNT'::text, 3)
  ) AS reason_row(reason, ordinal)
  WHERE :'h12_batch_channel' = 'PAYE'

  UNION ALL

  SELECT reason_row.reason, reason_row.ordinal
  FROM (
    VALUES
      ('MISSING_AMOUNT_EX_VAT'::text, 1),
      ('MISSING_AMOUNT_VAT'::text, 2)
  ) AS reason_row(reason, ordinal)
  WHERE :'h12_batch_channel' = 'UMBRELLA'
), target_candidates AS (
  SELECT
    candidate_row.id AS pay_batch_candidate_id,
    candidate_row.candidate_id,
    candidate_record.umbrella_id
  FROM public.pay_batch_candidates AS candidate_row
  JOIN public.candidates AS candidate_record
    ON candidate_record.id = candidate_row.candidate_id
  WHERE candidate_row.pay_batch_id = :'h12_batch_id'::uuid
    AND pg_catalog.current_setting('h12.fixture_mode') = 'FULL'
), inserted AS (
  INSERT INTO public.pay_batch_items (
    id,
    pay_batch_candidate_id,
    item_type,
    timesheet_id,
    segment_key,
    source_ref,
    description,
    amount_ex_vat,
    amount_vat,
    amount_inc_vat,
    pay_channel,
    umbrella_id,
    pay_bank_transfer_id,
    is_voided,
    finance_case_id,
    reservation_id,
    paye_treatment,
    finance_component_id,
    frozen_source_basis_json,
    operation_source_key
  )
  SELECT
    pg_catalog.md5(
      'h12-source-less-ambiguous:' ||
      target_candidate.pay_batch_candidate_id::text || ':' ||
      reason_row.reason
    )::uuid,
    target_candidate.pay_batch_candidate_id,
    'ADJUSTMENT_DELTA',
    NULL,
    NULL,
    NULL,
    -- Missing/blank presentation evidence is activated only after the
    -- Candidate has passed the unchanged Current Payment Status admission.
    -- The final frozen value is still proved before any cancellation apply.
    'H12 pre-activation manual adjustment: ' || reason_row.reason,
    CASE
      WHEN reason_row.reason = 'ZERO_AMOUNT' THEN 0.00
      WHEN reason_row.reason = 'MISSING_AMOUNT_EX_VAT' THEN 0.00
      WHEN :'h12_batch_channel' = 'PAYE' THEN -0.10
      ELSE 7.50
    END,
    CASE
      WHEN reason_row.reason = 'ZERO_AMOUNT' THEN 0.00
      WHEN reason_row.reason = 'MISSING_AMOUNT_VAT' THEN 0.00
      WHEN :'h12_batch_channel' = 'PAYE' THEN 0.00
      ELSE 1.50
    END,
    CASE
      -- 0.004 hashes to the same zero-pence value as NULL under the frozen
      -- candidate-scope contract but does not collapse the Candidate total.
      WHEN reason_row.reason = 'MISSING_AMOUNT_INC_VAT' THEN 0.004
      WHEN reason_row.reason = 'ZERO_AMOUNT' THEN 0.00
      WHEN :'h12_batch_channel' = 'PAYE' THEN -0.10
      ELSE 9.00
    END,
    :'h12_batch_channel',
    CASE WHEN :'h12_batch_channel' = 'UMBRELLA' THEN target_candidate.umbrella_id ELSE NULL::uuid END,
    NULL,
    false,
    NULL,
    NULL,
    CASE :'h12_batch_channel' WHEN 'PAYE' THEN 'NET_DEDUCT' ELSE NULL::text END,
    NULL,
    pg_catalog.jsonb_build_object(
      'fixture_contract', 'H12_SOURCE_LESS_CANCELLATION_RUNTIME_MATRIX_V1',
      'frozen_evidence_only', true,
      'ambiguity_reason', reason_row.reason,
      'ordinal', reason_row.ordinal
    ),
    NULL
  FROM target_candidates AS target_candidate
  CROSS JOIN reason_rows AS reason_row
  RETURNING *
), recorded AS (
  INSERT INTO private.h12_source_less_cancellation_fixture_v1 (
    batch_id,
    pay_batch_candidate_id,
    candidate_id,
    pay_batch_item_id,
    fixture_kind,
    ambiguity_reason,
    expected_pay_channel,
    expected_description,
    expected_amount_ex_vat,
    expected_amount_vat,
    expected_amount_inc_vat,
    expected_paye_treatment
  )
  SELECT
    :'h12_batch_id'::uuid,
    inserted.pay_batch_candidate_id,
    candidate_row.candidate_id,
    inserted.id,
    'SOURCE_LESS_AMBIGUOUS',
    inserted.frozen_source_basis_json->>'ambiguity_reason',
    inserted.pay_channel,
    inserted.description,
    inserted.amount_ex_vat,
    inserted.amount_vat,
    inserted.amount_inc_vat,
    inserted.paye_treatment
  FROM inserted
  JOIN public.pay_batch_candidates AS candidate_row
    ON candidate_row.id = inserted.pay_batch_candidate_id
  RETURNING pay_batch_item_id
)
SELECT pg_catalog.count(*)
FROM recorded;

DO $h12_detector_proof$
DECLARE
  candidate_scope record;
  detector_result jsonb;
  source_backed_count integer;
  safe_fixture_count integer;
  ambiguous_fixture_count integer;
BEGIN
  IF pg_catalog.current_setting('h12.fixture_mode') = 'MAIL_ONLY' THEN
    FOR candidate_scope IN
      SELECT
        fixture_row.pay_batch_candidate_id,
        pg_catalog.array_agg(fixture_row.pay_batch_item_id ORDER BY fixture_row.pay_batch_item_id) AS item_ids
      FROM private.h12_source_less_cancellation_fixture_v1 AS fixture_row
      GROUP BY fixture_row.pay_batch_candidate_id
      ORDER BY fixture_row.pay_batch_candidate_id
    LOOP
      detector_result := public._pay_detect_manual_adjustments_for_carry_forward(
        pg_catalog.current_setting('h12.batch_id')::uuid,
        pg_catalog.jsonb_build_object(
          'pay_batch_item_ids', pg_catalog.to_jsonb(candidate_scope.item_ids)
        ),
        pg_catalog.current_setting('h12.actor_id')::uuid
      );

      IF COALESCE((detector_result->>'source_backed_count')::integer, 0) < 1
         OR COALESCE((detector_result->>'source_less_safe_count')::integer, 0) <> 0
         OR COALESCE((detector_result->>'source_less_ambiguous_count')::integer, 0) <> 0 THEN
        RAISE EXCEPTION 'H12_MAIL_ONLY_FIXTURE_CLASSIFICATION_MISMATCH'
          USING ERRCODE = 'P0001', DETAIL = detector_result::text;
      END IF;
    END LOOP;
    RETURN;
  END IF;

  SELECT
    pg_catalog.count(*) FILTER (WHERE fixture_kind = 'ORDINARY_SOURCE_BACKED')::integer,
    pg_catalog.count(*) FILTER (WHERE fixture_kind = 'SOURCE_LESS_SAFE')::integer,
    pg_catalog.count(*) FILTER (WHERE fixture_kind = 'SOURCE_LESS_AMBIGUOUS')::integer
  INTO source_backed_count, safe_fixture_count, ambiguous_fixture_count
  FROM private.h12_source_less_cancellation_fixture_v1;

  IF source_backed_count < 1
     OR safe_fixture_count < 1
     OR ambiguous_fixture_count <> (
       CASE pg_catalog.current_setting('h12.batch_channel')
         WHEN 'PAYE' THEN 6
         WHEN 'UMBRELLA' THEN 4
         ELSE -1
       END
     ) THEN
    RAISE EXCEPTION 'H12_PREACTIVATION_FIXTURE_CARDINALITY_MISMATCH'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'source_backed_count', source_backed_count,
        'safe_fixture_count', safe_fixture_count,
        'ambiguous_fixture_count', ambiguous_fixture_count
      )::text;
  END IF;
END;
$h12_detector_proof$;

SELECT pg_catalog.jsonb_build_object(
  'contract', 'H12_SOURCE_LESS_CANCELLATION_RUNTIME_FIXTURE_V1',
  'ok', true,
  'database', pg_catalog.current_database(),
  'batch_id', :'h12_batch_id'::uuid,
  'batch_channel', :'h12_batch_channel',
  'fixture_mode', pg_catalog.current_setting('h12.fixture_mode'),
  'candidate_count', pg_catalog.count(DISTINCT fixture_row.pay_batch_candidate_id),
  'ordinary_source_backed_count', pg_catalog.count(*) FILTER (
    WHERE fixture_row.fixture_kind = 'ORDINARY_SOURCE_BACKED'
  ),
  'source_less_safe_count', pg_catalog.count(*) FILTER (
    WHERE fixture_row.fixture_kind = 'SOURCE_LESS_SAFE'
  ),
  'source_less_ambiguous_count', pg_catalog.count(*) FILTER (
    WHERE fixture_row.fixture_kind = 'SOURCE_LESS_AMBIGUOUS'
  ),
  'ambiguity_reasons', pg_catalog.jsonb_agg(
    DISTINCT fixture_row.ambiguity_reason
  ) FILTER (WHERE fixture_row.ambiguity_reason IS NOT NULL),
  'mail_fixture_count', (
    SELECT pg_catalog.count(*)
    FROM private.h12_source_less_cancellation_mail_fixture_v1
  ),
  'active_old_communication_contract_request_count_before_test', 0,
  'mail_sender_or_provider_invoked', false,
  'external_side_effects_invoked', false
)::text
FROM private.h12_source_less_cancellation_fixture_v1 AS fixture_row;
