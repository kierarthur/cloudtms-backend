-- Exact installed TEST rollback definition and ACL captured before Banking Pay Stage 1 integration on 2026-08-04.
-- Installed definition MD5: d91fbb2396f9663d050b164bf1c11ec5

CREATE OR REPLACE FUNCTION public.pay_payment_cancel_not_sent_and_recalculate(p_pay_batch_id uuid, p_selection_json jsonb DEFAULT '{}'::jsonb, p_actor_user_id uuid DEFAULT NULL::uuid, p_reason text DEFAULT NULL::text, p_idempotency_key text DEFAULT NULL::text, p_confirmation_json jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_uuid_regex text := '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$';
  v_selection_json jsonb := '{}'::jsonb;
  v_confirmation_json jsonb := '{}'::jsonb;
  v_diagnostic_json jsonb := '{}'::jsonb;
  v_resolved_scope_json jsonb := '{}'::jsonb;
  v_lifecycle_state text := NULL::text;
  v_recommended_action text := NULL::text;
  v_blockers jsonb := '[]'::jsonb;
  v_race_blockers jsonb := '[]'::jsonb;
  v_effective_selection_json jsonb := '{}'::jsonb;
  v_selection_hash text := NULL::text;
  v_plan_json jsonb := '{}'::jsonb;
  v_plan_hash text := NULL::text;
  v_accepted_resolution_json jsonb := '{}'::jsonb;
  v_accepted_resolution_hash text := NULL::text;
  v_required_quantity integer := 1;
  v_reason text := NULL::text;
  v_existing_request public.pay_payment_correction_requests%ROWTYPE;
  v_request public.pay_payment_correction_requests%ROWTYPE;
  v_expand_result jsonb := '{}'::jsonb;
  v_process_result jsonb := '{}'::jsonb;
  v_signal_result jsonb := '{}'::jsonb;
  v_idempotency_key text := NULL::text;
  v_scope_type text := NULL::text;
  v_ui_mode text := NULL::text;
  v_batch_status text := NULL::text;
  v_execution_commit_state text := NULL::text;
  v_execution_commit_ref text := NULL::text;
  v_execution_committed_at_utc timestamptz := NULL::timestamptz;
  v_active_unsafe_operation_count integer := 0;
  v_active_unsafe_operation_ids jsonb := '[]'::jsonb;
  v_actor_valid boolean := false;
  v_is_complete boolean := false;
  v_grouped_alert_updates jsonb := '[]'::jsonb;
  v_process_total integer := 0;
  v_process_applied integer := 0;
  v_process_skipped integer := 0;
  v_process_blocked integer := 0;
  v_process_failed_retryable integer := 0;
  v_process_failed_final integer := 0;
  v_process_pending integer := 0;
  v_process_processing integer := 0;
  v_process_cancelled integer := 0;
  v_process_parent_status text := NULL::text;
  v_process_failed boolean := false;
  v_process_error_code text := NULL::text;
  v_process_error_message text := NULL::text;
  v_process_voided_item_count integer := 0;
  v_request_changed_scope_json jsonb := '{}'::jsonb;
  v_resume_request_count integer := 0;
  v_resume_existing_request boolean := false;
  v_resume_blocked_reset_count integer := 0;
  v_resume_expected_json_count integer := 0;
  v_resume_expected_valid_count integer := 0;
  v_resume_expected_belongs_count integer := 0;
  v_resume_expected_active_count integer := 0;
  v_resume_expected_same_request_voided_count integer := 0;
  v_resume_expected_disallowed_state_count integer := 0;
  v_resume_active_outside_original_count integer := 0;
  v_resume_applied_work_item_count integer := 0;
  v_resume_blocked_work_item_count integer := 0;
  v_resume_invalid_blocked_work_item_count integer := 0;
BEGIN
  IF p_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'PAY_PAYMENT_CANCEL_NOT_SENT_BATCH_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_PAYMENT_CANCEL_NOT_SENT_BATCH_REQUIRED')::text;
  END IF;

  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'PAY_PAYMENT_CANCEL_NOT_SENT_ACTOR_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_PAYMENT_CANCEL_NOT_SENT_ACTOR_REQUIRED')::text;
  END IF;

  IF p_selection_json IS NOT NULL AND COALESCE(jsonb_typeof(p_selection_json), 'null') <> 'object' THEN
    RAISE EXCEPTION 'PAY_PAYMENT_CANCEL_NOT_SENT_SELECTION_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_PAYMENT_CANCEL_NOT_SENT_SELECTION_MUST_BE_OBJECT')::text;
  END IF;

  IF p_confirmation_json IS NOT NULL AND COALESCE(jsonb_typeof(p_confirmation_json), 'null') <> 'object' THEN
    RAISE EXCEPTION 'PAY_PAYMENT_CANCEL_NOT_SENT_CONFIRMATION_MUST_BE_OBJECT'
      USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'PAY_PAYMENT_CANCEL_NOT_SENT_CONFIRMATION_MUST_BE_OBJECT')::text;
  END IF;

  SELECT EXISTS (
    SELECT 1
    FROM public.tms_users AS actor_user
    WHERE actor_user.id = p_actor_user_id
      AND COALESCE(actor_user.is_active, false) = true
  )
  INTO v_actor_valid;

  IF COALESCE(v_actor_valid, false) IS NOT TRUE THEN
    RAISE EXCEPTION 'PAY_PAYMENT_CANCEL_NOT_SENT_ACTOR_NOT_ALLOWED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'PAY_PAYMENT_CANCEL_NOT_SENT_ACTOR_NOT_ALLOWED', 'actor_user_id', p_actor_user_id::text)::text;
  END IF;

  v_selection_json := COALESCE(p_selection_json, '{}'::jsonb);
  v_confirmation_json := COALESCE(p_confirmation_json, '{}'::jsonb);
  v_reason := NULLIF(BTRIM(COALESCE(p_reason, '')), '');
  v_scope_type := COALESCE(NULLIF(BTRIM(UPPER(COALESCE(v_selection_json ->> 'scope_type', v_selection_json ->> 'scopeType', ''))), ''), 'BATCH');
  v_ui_mode := UPPER(REPLACE(NULLIF(BTRIM(COALESCE(v_confirmation_json ->> 'ui_mode', v_selection_json ->> 'ui_mode', '')), ''), '-', '_'));

  SELECT batch_rows.status,
         batch_rows.execution_commit_state,
         batch_rows.execution_commit_ref,
         batch_rows.execution_committed_at_utc
  INTO v_batch_status,
       v_execution_commit_state,
       v_execution_commit_ref,
       v_execution_committed_at_utc
  FROM public.pay_batches AS batch_rows
  WHERE batch_rows.id = p_pay_batch_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAY_PAYMENT_CANCEL_NOT_SENT_BATCH_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'PAY_PAYMENT_CANCEL_NOT_SENT_BATCH_NOT_FOUND', 'pay_batch_id', p_pay_batch_id::text)::text;
  END IF;

  IF UPPER(COALESCE(v_execution_commit_state, '')) <> 'NOT_SUBMITTED'
     OR NULLIF(BTRIM(COALESCE(v_execution_commit_ref, '')), '') IS NOT NULL
     OR v_execution_committed_at_utc IS NOT NULL THEN
    RAISE EXCEPTION 'PAY_PAYMENT_CANCEL_NOT_SENT_COMMIT_BOUNDARY_CROSSED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_PAYMENT_CANCEL_NOT_SENT_COMMIT_BOUNDARY_CROSSED',
              'pay_batch_id', p_pay_batch_id::text,
              'status', v_batch_status,
              'execution_commit_state', v_execution_commit_state,
              'has_execution_commit_ref', NULLIF(BTRIM(COALESCE(v_execution_commit_ref, '')), '') IS NOT NULL,
              'has_execution_committed_at_utc', v_execution_committed_at_utc IS NOT NULL
            )::text;
  END IF;

  IF v_ui_mode = 'DRAFT_DELETE' AND UPPER(COALESCE(v_batch_status, '')) NOT IN ('DRAFT', 'DRAFT_CREATED') THEN
    RAISE EXCEPTION 'PAY_PAYMENT_CANCEL_NOT_SENT_DRAFT_DELETE_NOT_AVAILABLE'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_PAYMENT_CANCEL_NOT_SENT_DRAFT_DELETE_NOT_AVAILABLE',
              'pay_batch_id', p_pay_batch_id::text,
              'status', v_batch_status,
              'execution_commit_state', v_execution_commit_state
            )::text;
  END IF;

  IF v_ui_mode = 'DRAFT_DELETE' AND v_scope_type NOT IN ('BATCH', 'WHOLE_BATCH', 'ALL', 'PAY_BATCH') THEN
    RAISE EXCEPTION 'PAY_PAYMENT_CANCEL_NOT_SENT_DRAFT_DELETE_REQUIRES_BATCH_SCOPE'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_PAYMENT_CANCEL_NOT_SENT_DRAFT_DELETE_REQUIRES_BATCH_SCOPE',
              'pay_batch_id', p_pay_batch_id::text,
              'scope_type', v_scope_type
            )::text;
  END IF;

  IF v_ui_mode = 'DRAFT_DELETE'
     OR v_scope_type IN ('BATCH', 'WHOLE_BATCH', 'ALL', 'PAY_BATCH') THEN
    SELECT count(*)::integer,
           COALESCE(jsonb_agg(active_operation_rows.id::text ORDER BY active_operation_rows.created_at_utc, active_operation_rows.id), '[]'::jsonb)
    INTO v_active_unsafe_operation_count,
         v_active_unsafe_operation_ids
    FROM public.banking_pay_operations AS active_operation_rows
    WHERE active_operation_rows.pay_batch_id = p_pay_batch_id
      AND UPPER(COALESCE(active_operation_rows.operation_type::text, '')) IN (
        'DRAFT_CREATE',
        'PAYMENT_DRAFT_CREATE',
        'PAY_BATCH_DRAFT_CREATE',
        'CREATE_DRAFT',
        'PAYMENT_EXECUTE',
        'PAY_EXECUTE',
        'PROVIDER_SUBMIT',
        'PAYMENT_PROVIDER_SUBMIT',
        'PAYMENT_SUBMISSION',
        'RAIL_SUBMIT',
        'BANK_SUBMISSION',
        'PAYMENT_AUTHORISE',
        'PAYMENT_AUTHORIZE',
        'PAYMENT_AUTH_START',
        'PAYMENT_RETRY_BLOCKED_FUNDS',
        'PAYMENT_RETRY_UNSENT_PAYMENTS',
        'PAYMENT_RETRY_UNSENT',
        'PAYMENT_SETTLEMENT'
      )
      AND UPPER(COALESCE(active_operation_rows.status::text, '')) NOT IN (
        'COMPLETE',
        'COMPLETED',
        'SUCCESS',
        'SUCCEEDED',
        'DONE',
        'FAILED',
        'ERROR',
        'ERRORED',
        'REVIEW_REQUIRED',
        'CANCELLED',
        'CANCELED',
        'ABORTED'
      )
      AND (
        UPPER(COALESCE(active_operation_rows.status::text, '')) IN (
          'QUEUED',
          'RUNNABLE',
          'RUNNING',
          'CONTINUING',
          'WAITING_RETRY',
          'LEASED',
          'ADVANCING',
          'PENDING',
          'STARTED',
          'INITIALISING',
          'INITIALIZING',
          'PROCESSING',
          'CLAIMED',
          'IN_PROGRESS',
          'WAITING_PROVIDER',
          'WAITING_FOR_PROVIDER',
          'PROVIDER_WAIT',
          'PROVIDER_WAITING',
          'WAITING_PROVIDER_CONFIRMATION',
          'WAITING_AUTHORISATION',
          'WAITING_AUTHORIZATION',
          'AWAITING_AUTHORISATION',
          'AWAITING_AUTHORIZATION',
          'AUTHORISATION_REQUIRED',
          'AUTHORIZATION_REQUIRED',
          'WAITING_BANK_CONFIRM',
          'SUBMITTED_NOT_COMMITTED'
        )
        OR UPPER(COALESCE(active_operation_rows.runner_state::text, '')) IN (
          'RUNNABLE',
          'RUNNING',
          'CONTINUING',
          'WAITING_RETRY',
          'LEASED',
          'ADVANCING',
          'PENDING',
          'STARTED',
          'INITIALISING',
          'INITIALIZING',
          'PROCESSING',
          'CLAIMED',
          'IN_PROGRESS',
          'WAITING_PROVIDER',
          'WAITING_FOR_PROVIDER',
          'PROVIDER_WAIT',
          'PROVIDER_WAITING',
          'WAITING_PROVIDER_CONFIRMATION',
          'WAITING_AUTHORISATION',
          'WAITING_AUTHORIZATION',
          'AWAITING_AUTHORISATION',
          'AWAITING_AUTHORIZATION',
          'AUTHORISATION_REQUIRED',
          'AUTHORIZATION_REQUIRED',
          'WAITING_BANK_CONFIRM'
        )
        OR (
          NULLIF(BTRIM(COALESCE(active_operation_rows.lease_owner, '')), '') IS NOT NULL
          AND COALESCE(active_operation_rows.lease_expires_at_utc, now()) > now()
        )
        OR (
          NULLIF(BTRIM(COALESCE(active_operation_rows.locked_by, '')), '') IS NOT NULL
          AND COALESCE(active_operation_rows.lock_expires_at_utc, now()) > now()
        )
      );

    IF COALESCE(v_active_unsafe_operation_count, 0) > 0 THEN
      RAISE EXCEPTION 'PAY_PAYMENT_CANCEL_NOT_SENT_DRAFT_DELETE_OPERATION_IN_PROGRESS'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_PAYMENT_CANCEL_NOT_SENT_DRAFT_DELETE_OPERATION_IN_PROGRESS',
                'pay_batch_id', p_pay_batch_id::text,
                'active_operation_count', COALESCE(v_active_unsafe_operation_count, 0),
                'active_operation_ids', COALESCE(v_active_unsafe_operation_ids, '[]'::jsonb)
              )::text;
    END IF;
  END IF;

  SELECT count(*)::integer
  INTO v_resume_request_count
  FROM public.pay_payment_correction_requests AS resumable_request_rows
  WHERE resumable_request_rows.pay_batch_id = p_pay_batch_id
    AND resumable_request_rows.correction_kind = 'PRE_BANK_CANCEL'
    AND resumable_request_rows.status = 'APPLIED_WITH_BLOCKERS'
    AND UPPER(BTRIM(COALESCE(resumable_request_rows.selection_json->>'scope_type', ''))) IN ('BATCH', 'WHOLE_BATCH', 'ALL', 'PAY_BATCH')
    AND EXISTS (
      SELECT 1
      FROM public.pay_payment_correction_work_items AS resumable_applied_items
      WHERE resumable_applied_items.correction_request_id = resumable_request_rows.id
        AND resumable_applied_items.work_kind = 'PRE_BANK_CANCEL'
        AND resumable_applied_items.status = 'APPLIED'
    )
    AND EXISTS (
      SELECT 1
      FROM public.pay_payment_correction_work_items AS resumable_blocked_items
      WHERE resumable_blocked_items.correction_request_id = resumable_request_rows.id
        AND resumable_blocked_items.work_kind = 'PRE_BANK_CANCEL'
        AND resumable_blocked_items.status = 'BLOCKED'
        AND COALESCE(
          resumable_blocked_items.result_json#>>'{blocker,code}',
          resumable_blocked_items.result_json->>'error_code',
          ''
        ) = 'PRE_BANK_CANCEL_CLASSIFICATION_REQUIRED'
    )
    AND NOT EXISTS (
      SELECT 1
      FROM public.pay_payment_correction_work_items AS resumable_invalid_items
      WHERE resumable_invalid_items.correction_request_id = resumable_request_rows.id
        AND resumable_invalid_items.status NOT IN ('APPLIED', 'BLOCKED')
    );

  IF v_resume_request_count = 1 THEN
    SELECT resumable_request_rows.*
    INTO v_existing_request
    FROM public.pay_payment_correction_requests AS resumable_request_rows
    WHERE resumable_request_rows.pay_batch_id = p_pay_batch_id
      AND resumable_request_rows.correction_kind = 'PRE_BANK_CANCEL'
      AND resumable_request_rows.status = 'APPLIED_WITH_BLOCKERS'
      AND UPPER(BTRIM(COALESCE(resumable_request_rows.selection_json->>'scope_type', ''))) IN ('BATCH', 'WHOLE_BATCH', 'ALL', 'PAY_BATCH')
      AND EXISTS (
        SELECT 1
        FROM public.pay_payment_correction_work_items AS resumable_applied_items
        WHERE resumable_applied_items.correction_request_id = resumable_request_rows.id
          AND resumable_applied_items.work_kind = 'PRE_BANK_CANCEL'
          AND resumable_applied_items.status = 'APPLIED'
      )
      AND EXISTS (
        SELECT 1
        FROM public.pay_payment_correction_work_items AS resumable_blocked_items
        WHERE resumable_blocked_items.correction_request_id = resumable_request_rows.id
          AND resumable_blocked_items.work_kind = 'PRE_BANK_CANCEL'
          AND resumable_blocked_items.status = 'BLOCKED'
          AND COALESCE(
            resumable_blocked_items.result_json#>>'{blocker,code}',
            resumable_blocked_items.result_json->>'error_code',
            ''
          ) = 'PRE_BANK_CANCEL_CLASSIFICATION_REQUIRED'
      )
      AND NOT EXISTS (
        SELECT 1
        FROM public.pay_payment_correction_work_items AS resumable_invalid_items
        WHERE resumable_invalid_items.correction_request_id = resumable_request_rows.id
          AND resumable_invalid_items.status NOT IN ('APPLIED', 'BLOCKED')
      )
    ORDER BY resumable_request_rows.created_at_utc DESC, resumable_request_rows.id
    LIMIT 1
    FOR UPDATE;

    v_diagnostic_json := public.pay_payment_cancelability_diagnostic(
      p_pay_batch_id,
      v_existing_request.selection_json,
      p_actor_user_id,
      'CANCEL_WHOLE_BATCH_ACTION'
    );

    WITH original_expected_raw AS (
      SELECT original_values.raw_value
      FROM jsonb_array_elements_text(
        CASE
          WHEN COALESCE(jsonb_typeof(v_existing_request.selection_json->'expected_pay_batch_item_ids'), 'null') = 'array'
            THEN v_existing_request.selection_json->'expected_pay_batch_item_ids'
          ELSE '[]'::jsonb
        END
      ) AS original_values(raw_value)
    ),
    original_expected AS (
      SELECT DISTINCT
        original_expected_raw.raw_value,
        CASE
          WHEN original_expected_raw.raw_value ~* v_uuid_regex
            THEN original_expected_raw.raw_value::uuid
          ELSE NULL::uuid
        END AS pay_batch_item_id
      FROM original_expected_raw
    ),
    original_expected_valid AS (
      SELECT original_expected.pay_batch_item_id
      FROM original_expected
      WHERE original_expected.pay_batch_item_id IS NOT NULL
    ),
    original_item_state AS (
      SELECT
        original_expected_valid.pay_batch_item_id,
        batch_items.id IS NOT NULL
          AND batch_candidates.pay_batch_id = p_pay_batch_id AS belongs_to_batch,
        COALESCE(batch_items.is_voided, false) AS is_voided,
        EXISTS (
          SELECT 1
          FROM public.pay_payment_correction_items AS same_request_correction_items
          WHERE same_request_correction_items.correction_request_id = v_existing_request.id
            AND same_request_correction_items.pay_batch_item_id = original_expected_valid.pay_batch_item_id
            AND same_request_correction_items.correction_item_kind = 'PRE_BANK_CANCEL'
            AND same_request_correction_items.status = 'APPLIED'
        ) AS voided_by_same_request,
        EXISTS (
          SELECT 1
          FROM public.pay_payment_correction_items AS any_applied_correction_items
          WHERE any_applied_correction_items.pay_batch_item_id = original_expected_valid.pay_batch_item_id
            AND any_applied_correction_items.status = 'APPLIED'
        ) AS has_any_applied_correction
      FROM original_expected_valid
      LEFT JOIN public.pay_batch_items AS batch_items
        ON batch_items.id = original_expected_valid.pay_batch_item_id
      LEFT JOIN public.pay_batch_candidates AS batch_candidates
        ON batch_candidates.id = batch_items.pay_batch_candidate_id
    )
    SELECT
      jsonb_array_length(
        CASE
          WHEN COALESCE(jsonb_typeof(v_existing_request.selection_json->'expected_pay_batch_item_ids'), 'null') = 'array'
            THEN v_existing_request.selection_json->'expected_pay_batch_item_ids'
          ELSE '[]'::jsonb
        END
      )::integer,
      (SELECT count(*)::integer FROM original_expected_valid),
      (SELECT count(*)::integer FROM original_item_state WHERE original_item_state.belongs_to_batch),
      (SELECT count(*)::integer FROM original_item_state WHERE original_item_state.belongs_to_batch AND NOT original_item_state.is_voided),
      (SELECT count(*)::integer FROM original_item_state WHERE original_item_state.belongs_to_batch AND original_item_state.is_voided AND original_item_state.voided_by_same_request),
      (
        SELECT count(*)::integer
        FROM original_item_state
        WHERE original_item_state.belongs_to_batch
          AND NOT (
            (
              NOT original_item_state.is_voided
              AND NOT original_item_state.has_any_applied_correction
            )
            OR (
              original_item_state.is_voided
              AND original_item_state.voided_by_same_request
            )
          )
      ),
      (
        SELECT count(*)::integer
        FROM public.pay_batch_items AS active_outside_items
        JOIN public.pay_batch_candidates AS active_outside_candidates
          ON active_outside_candidates.id = active_outside_items.pay_batch_candidate_id
        WHERE active_outside_candidates.pay_batch_id = p_pay_batch_id
          AND COALESCE(active_outside_items.is_voided, false) = false
          AND NOT EXISTS (
            SELECT 1
            FROM original_expected_valid
            WHERE original_expected_valid.pay_batch_item_id = active_outside_items.id
          )
      ),
      (
        SELECT count(*)::integer
        FROM public.pay_payment_correction_work_items AS applied_request_items
        WHERE applied_request_items.correction_request_id = v_existing_request.id
          AND applied_request_items.work_kind = 'PRE_BANK_CANCEL'
          AND applied_request_items.status = 'APPLIED'
      ),
      (
        SELECT count(*)::integer
        FROM public.pay_payment_correction_work_items AS blocked_request_items
        WHERE blocked_request_items.correction_request_id = v_existing_request.id
          AND blocked_request_items.work_kind = 'PRE_BANK_CANCEL'
          AND blocked_request_items.status = 'BLOCKED'
      ),
      (
        SELECT count(*)::integer
        FROM public.pay_payment_correction_work_items AS invalid_blocked_request_items
        WHERE invalid_blocked_request_items.correction_request_id = v_existing_request.id
          AND invalid_blocked_request_items.status = 'BLOCKED'
          AND COALESCE(
            invalid_blocked_request_items.result_json#>>'{blocker,code}',
            invalid_blocked_request_items.result_json->>'error_code',
            ''
          ) <> 'PRE_BANK_CANCEL_CLASSIFICATION_REQUIRED'
      )
    INTO
      v_resume_expected_json_count,
      v_resume_expected_valid_count,
      v_resume_expected_belongs_count,
      v_resume_expected_active_count,
      v_resume_expected_same_request_voided_count,
      v_resume_expected_disallowed_state_count,
      v_resume_active_outside_original_count,
      v_resume_applied_work_item_count,
      v_resume_blocked_work_item_count,
      v_resume_invalid_blocked_work_item_count;

    v_resume_existing_request := (
      v_existing_request.requested_by_user_id = p_actor_user_id
      AND COALESCE(v_existing_request.selection_json->>'correction_kind', 'PRE_BANK_CANCEL') = 'PRE_BANK_CANCEL'
      AND COALESCE(v_diagnostic_json->>'payment_lifecycle_state', '') IN (
        'PARTIALLY_CANCELLED_BEFORE_BANK_SUBMISSION',
        'CANCELLED_BEFORE_BANK_SUBMISSION'
      )
      AND COALESCE(jsonb_array_length(COALESCE(v_diagnostic_json->'blockers', '[]'::jsonb)), 0) = 0
      AND COALESCE(jsonb_array_length(COALESCE(v_diagnostic_json->'race_or_submission_blockers', '[]'::jsonb)), 0) = 0
      AND COALESCE(NULLIF(v_diagnostic_json#>>'{blocking_paid_evidence_json,has_paid_or_settled}', '')::boolean, false) IS NOT TRUE
      AND COALESCE(NULLIF(v_diagnostic_json#>>'{pending_provider_evidence_json,has_provider_pending}', '')::boolean, false) IS NOT TRUE
      AND COALESCE(NULLIF(v_diagnostic_json#>>'{provider_outcome_unknown_evidence_json,has_provider_unknown}', '')::boolean, false) IS NOT TRUE
      AND COALESCE(v_diagnostic_json#>>'{provider_evidence_summary_json,evidence_class}', '') = 'NO_PROVIDER_EVIDENCE'
      AND COALESCE(NULLIF(v_diagnostic_json#>>'{provider_evidence_summary_json,provider_submitted}', '')::boolean, false) IS NOT TRUE
      AND COALESCE(NULLIF(v_diagnostic_json#>>'{provider_evidence_summary_json,provider_request_sent}', '')::boolean, false) IS NOT TRUE
      AND COALESCE(NULLIF(v_diagnostic_json#>>'{provider_evidence_summary_json,provider_response_present}', '')::boolean, false) IS NOT TRUE
      AND COALESCE(NULLIF(v_diagnostic_json#>>'{provider_evidence_summary_json,provider_event_present}', '')::boolean, false) IS NOT TRUE
      AND COALESCE(NULLIF(v_diagnostic_json#>>'{provider_evidence_summary_json,provider_external_id_present}', '')::boolean, false) IS NOT TRUE
      AND v_resume_expected_json_count > 0
      AND v_resume_expected_valid_count = v_resume_expected_json_count
      AND v_resume_expected_belongs_count = v_resume_expected_json_count
      AND v_resume_expected_active_count > 0
      AND v_resume_expected_same_request_voided_count > 0
      AND v_resume_expected_active_count + v_resume_expected_same_request_voided_count = v_resume_expected_json_count
      AND v_resume_expected_disallowed_state_count = 0
      AND v_resume_active_outside_original_count = 0
      AND v_resume_applied_work_item_count > 0
      AND v_resume_blocked_work_item_count > 0
      AND v_resume_invalid_blocked_work_item_count = 0
    );

    IF v_resume_existing_request THEN
      v_selection_json := v_existing_request.selection_json;
      v_reason := COALESCE(v_reason, v_existing_request.reason);
      v_scope_type := 'BATCH';
    END IF;
  END IF;

  IF NOT v_resume_existing_request THEN
  v_diagnostic_json := public.pay_payment_cancelability_diagnostic(
    p_pay_batch_id,
    v_selection_json,
    p_actor_user_id,
    CASE
      WHEN v_scope_type IN ('BATCH', 'WHOLE_BATCH', 'ALL', 'PAY_BATCH') THEN 'CANCEL_WHOLE_BATCH_ACTION'
      ELSE 'CANCEL_PAYMENT_ACTION'
    END
  );
  END IF;

  v_lifecycle_state := v_diagnostic_json ->> 'payment_lifecycle_state';
  v_recommended_action := v_diagnostic_json ->> 'recommended_action';
  v_blockers := COALESCE(v_diagnostic_json -> 'blockers', '[]'::jsonb);
  v_race_blockers := COALESCE(v_diagnostic_json -> 'race_or_submission_blockers', '[]'::jsonb);
  v_resolved_scope_json := COALESCE(v_diagnostic_json -> 'resolved_full_payment_scope_json', '{}'::jsonb);
  v_scope_type := COALESCE(NULLIF(BTRIM(v_resolved_scope_json ->> 'scope_type'), ''), 'BATCH');

  IF (
    NOT v_resume_existing_request
    AND (
      v_lifecycle_state NOT IN ('LOCAL_PREPARED_NOT_SENT', 'SCHEDULED_LOCAL_NOT_SENT')
      OR v_recommended_action IS DISTINCT FROM 'PRE_PROVIDER_CANCEL_AND_RECALCULATE'
    )
  ) OR (
    v_resume_existing_request
    AND v_lifecycle_state NOT IN (
      'PARTIALLY_CANCELLED_BEFORE_BANK_SUBMISSION',
      'CANCELLED_BEFORE_BANK_SUBMISSION'
    )
  ) THEN
    RAISE EXCEPTION 'PAY_PAYMENT_CANCEL_NOT_SENT_NOT_AVAILABLE'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_PAYMENT_CANCEL_NOT_SENT_NOT_AVAILABLE',
              'pay_batch_id', p_pay_batch_id::text,
              'payment_lifecycle_state', v_lifecycle_state,
              'recommended_action', v_recommended_action,
              'diagnostic_json', v_diagnostic_json
            )::text;
  END IF;

  IF COALESCE(jsonb_array_length(v_blockers), 0) > 0 OR COALESCE(jsonb_array_length(v_race_blockers), 0) > 0 THEN
    RAISE EXCEPTION 'PAY_PAYMENT_CANCEL_NOT_SENT_BLOCKED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_PAYMENT_CANCEL_NOT_SENT_BLOCKED',
              'pay_batch_id', p_pay_batch_id::text,
              'blockers', v_blockers,
              'race_or_submission_blockers', v_race_blockers
            )::text;
  END IF;

  IF v_resume_existing_request THEN
    v_effective_selection_json := v_existing_request.selection_json;
    v_selection_hash := v_existing_request.selection_hash;
    v_plan_json := v_existing_request.plan_json;
    v_plan_hash := v_existing_request.plan_hash;
    v_accepted_resolution_json := COALESCE(v_existing_request.accepted_resolution_json, '{}'::jsonb);
    v_accepted_resolution_hash := v_existing_request.accepted_resolution_hash;
    v_required_quantity := GREATEST(COALESCE(v_existing_request.required_quantity, 1), 1);
    v_idempotency_key := COALESCE(
      NULLIF(BTRIM(COALESCE(v_existing_request.plan_json->>'idempotency_key', '')), ''),
      NULLIF(BTRIM(COALESCE(p_idempotency_key, '')), ''),
      md5(jsonb_build_object(
        'pay_batch_id', p_pay_batch_id::text,
        'correction_request_id', v_existing_request.id::text,
        'action', 'RESUME_PRE_PROVIDER_CANCEL_AND_RECALCULATE'
      )::text)
    );
  ELSE
  v_effective_selection_json := v_selection_json
    || jsonb_build_object(
      'requested_action', 'PRE_PROVIDER_CANCEL_AND_RECALCULATE',
      'correction_kind', 'PRE_BANK_CANCEL',
      'scope_type', v_scope_type,
      'work_unit', CASE
        WHEN v_scope_type IN ('BATCH', 'WHOLE_BATCH', 'ALL', 'PAY_BATCH') THEN 'BATCH'
        WHEN jsonb_array_length(COALESCE(v_resolved_scope_json -> 'pay_bank_transfer_ids', '[]'::jsonb)) > 0 THEN 'TRANSFER'
        ELSE 'CANDIDATE'
      END,
      'pay_batch_id', p_pay_batch_id::text,
      'pay_batch_item_ids', COALESCE(v_resolved_scope_json -> 'pay_batch_item_ids', '[]'::jsonb),
      'expected_pay_batch_item_ids', COALESCE(v_resolved_scope_json -> 'pay_batch_item_ids', '[]'::jsonb),
      'pay_bank_transfer_ids', COALESCE(v_resolved_scope_json -> 'pay_bank_transfer_ids', '[]'::jsonb),
      'pay_batch_candidate_ids', COALESCE(v_resolved_scope_json -> 'pay_batch_candidate_ids', '[]'::jsonb)
    )
    || jsonb_build_object(
      'candidate_ids', COALESCE(v_resolved_scope_json -> 'candidate_ids', '[]'::jsonb),
      'finance_case_ids', COALESCE(v_resolved_scope_json -> 'finance_case_ids', '[]'::jsonb),
      'finance_component_ids', COALESCE(v_resolved_scope_json -> 'finance_component_ids', '[]'::jsonb),
      'reservation_ids', COALESCE(v_resolved_scope_json -> 'reservation_ids', '[]'::jsonb),
      'manual_adjustments_to_carry_forward', COALESCE(v_diagnostic_json -> 'manual_adjustments_to_carry_forward', '[]'::jsonb),
      'manual_adjustments_carried_forward_existing', COALESCE(v_diagnostic_json -> 'manual_adjustments_carried_forward_existing', '[]'::jsonb)
    );

  v_selection_hash := md5(v_effective_selection_json::text);
  v_idempotency_key := COALESCE(NULLIF(BTRIM(COALESCE(p_idempotency_key, '')), ''), md5(jsonb_build_object(
    'pay_batch_id', p_pay_batch_id::text,
    'selection_hash', v_selection_hash,
    'action', 'PRE_PROVIDER_CANCEL_AND_RECALCULATE',
    'actor_user_id', p_actor_user_id::text
  )::text));

  SELECT GREATEST(COALESCE(settings_row.payment_authoriser_quantity, 1), 1)
  INTO v_required_quantity
  FROM public.settings_defaults AS settings_row
  ORDER BY settings_row.id
  LIMIT 1;

  v_required_quantity := GREATEST(COALESCE(v_required_quantity, 1), 1);

  v_plan_json := jsonb_build_object(
    'classification', v_lifecycle_state,
    'payment_lifecycle_state', v_lifecycle_state,
    'recommended_action', v_recommended_action,
    'resolved_full_payment_scope_json', v_resolved_scope_json,
    'finance_scope_json', COALESCE(v_diagnostic_json -> 'finance_scope_json', '{}'::jsonb),
    'provider_evidence', COALESCE(v_diagnostic_json -> 'provider_evidence_summary_json', '{}'::jsonb),
    'manual_adjustments_to_carry_forward', COALESCE(v_diagnostic_json -> 'manual_adjustments_to_carry_forward', '[]'::jsonb),
    'manual_adjustments_carried_forward_existing', COALESCE(v_diagnostic_json -> 'manual_adjustments_carried_forward_existing', '[]'::jsonb),
    'work_expansion_plan', jsonb_build_object(
      'work_unit',
      CASE
        WHEN v_scope_type IN ('BATCH', 'WHOLE_BATCH', 'ALL', 'PAY_BATCH') THEN 'BATCH'
        WHEN jsonb_array_length(COALESCE(v_resolved_scope_json -> 'pay_bank_transfer_ids', '[]'::jsonb)) > 0 THEN 'TRANSFER'
        ELSE 'CANDIDATE_PAYEE'
      END,
      'chunk_size',
      50
    ),
    'idempotency_key', v_idempotency_key
  );

  v_plan_hash := md5(v_plan_json::text);

  v_accepted_resolution_json := jsonb_build_object(
    'accepted_action', 'PRE_PROVIDER_CANCEL_AND_RECALCULATE',
    'accepted_at_utc', now(),
    'accepted_by_user_id', p_actor_user_id::text,
    'reason', v_reason,
    'confirmation_json', v_confirmation_json,
    'diagnostic_json', v_diagnostic_json,
    'idempotency_key', v_idempotency_key
  );

  v_accepted_resolution_hash := md5(v_accepted_resolution_json::text);
  END IF;

  SELECT request_rows.*
  INTO v_existing_request
  FROM public.pay_payment_correction_requests AS request_rows
  WHERE request_rows.pay_batch_id = p_pay_batch_id
    AND request_rows.selection_hash = v_selection_hash
    AND request_rows.correction_kind = 'PRE_BANK_CANCEL'
    AND request_rows.status IN ('REQUESTED', 'AWAITING_AUTHORISATION', 'AUTHORISED', 'EXPANDED', 'PROCESSING', 'APPLIED_WITH_BLOCKERS')
  ORDER BY request_rows.created_at_utc
  LIMIT 1
  FOR UPDATE;

  IF FOUND THEN
    IF v_resume_existing_request THEN
      UPDATE public.pay_payment_correction_work_items AS resumable_blocked_items
      SET
        status = 'PENDING',
        processed_at_utc = NULL,
        processed_by_user_id = NULL,
        locked_at_utc = NULL,
        locked_by = NULL,
        last_error = NULL,
        result_json = (
          COALESCE(resumable_blocked_items.result_json, '{}'::jsonb)
          - 'blocker'
          - 'classification_result'
          - 'error_message'
          - 'sqlstate'
          - 'failed_at_utc'
          - 'status'
          - 'ok'
        ) || jsonb_build_object(
          'resumed_same_request_pre_bank_cancel', true,
          'resumed_at_utc', now(),
          'resumed_by_user_id', p_actor_user_id::text,
          'resume_reason', 'SAFE_SAME_REQUEST_PARTIAL_PRE_BANK_CANCEL_CONTINUATION'
        )
      WHERE resumable_blocked_items.correction_request_id = v_existing_request.id
        AND resumable_blocked_items.work_kind = 'PRE_BANK_CANCEL'
        AND resumable_blocked_items.status = 'BLOCKED'
        AND COALESCE(
          resumable_blocked_items.result_json#>>'{blocker,code}',
          resumable_blocked_items.result_json->>'error_code',
          ''
        ) = 'PRE_BANK_CANCEL_CLASSIFICATION_REQUIRED';

      GET DIAGNOSTICS v_resume_blocked_reset_count = ROW_COUNT;

      IF v_resume_blocked_reset_count <= 0 THEN
        RAISE EXCEPTION 'PAY_PAYMENT_CANCEL_NOT_SENT_RESUME_WORK_NOT_FOUND'
          USING ERRCODE = 'P0001',
                DETAIL = jsonb_build_object(
                  'code', 'PAY_PAYMENT_CANCEL_NOT_SENT_RESUME_WORK_NOT_FOUND',
                  'pay_batch_id', p_pay_batch_id::text,
                  'correction_request_id', v_existing_request.id::text
                )::text;
      END IF;

      UPDATE public.pay_payment_correction_requests AS request_to_resume
      SET
        status = 'EXPANDED',
        applied_at_utc = NULL,
        approved_count = GREATEST(COALESCE(request_to_resume.approved_count, 0), COALESCE(request_to_resume.required_quantity, v_required_quantity, 1)),
        authorised_at_utc = COALESCE(request_to_resume.authorised_at_utc, now()),
        updated_at_utc = now()
      WHERE request_to_resume.id = v_existing_request.id
      RETURNING request_to_resume.*
      INTO v_request;

      INSERT INTO public.pay_payment_correction_actions (
        correction_request_id,
        pay_batch_id,
        actor_kind,
        actor_user_id,
        action,
        action_at_utc,
        note,
        before_json,
        after_json,
        metadata_json
      )
      VALUES (
        v_request.id,
        p_pay_batch_id,
        'USER',
        p_actor_user_id,
        'RETRY',
        now(),
        'Resume the remaining frozen scope of the same whole-batch pre-bank cancellation request.',
        to_jsonb(v_existing_request),
        to_jsonb(v_request),
        jsonb_build_object(
          'resume_reason', 'SAFE_SAME_REQUEST_PARTIAL_PRE_BANK_CANCEL_CONTINUATION',
          'resume_blocked_work_item_count', v_resume_blocked_reset_count,
          'expected_item_count', v_resume_expected_json_count,
          'active_expected_item_count', v_resume_expected_active_count,
          'same_request_voided_item_count', v_resume_expected_same_request_voided_count,
          'applied_work_item_count', v_resume_applied_work_item_count,
          'blocked_work_item_count', v_resume_blocked_work_item_count
        )
      );
    ELSE
      UPDATE public.pay_payment_correction_requests AS request_to_authorise
      SET status = CASE WHEN request_to_authorise.status IN ('REQUESTED', 'AWAITING_AUTHORISATION') THEN 'AUTHORISED' ELSE request_to_authorise.status END,
          approved_count = GREATEST(COALESCE(request_to_authorise.approved_count, 0), COALESCE(request_to_authorise.required_quantity, v_required_quantity, 1)),
          authorised_at_utc = COALESCE(request_to_authorise.authorised_at_utc, now()),
          accepted_resolution_json = COALESCE(request_to_authorise.accepted_resolution_json, v_accepted_resolution_json),
          accepted_resolution_hash = COALESCE(request_to_authorise.accepted_resolution_hash, v_accepted_resolution_hash),
          updated_at_utc = now()
      WHERE request_to_authorise.id = v_existing_request.id
      RETURNING request_to_authorise.*
      INTO v_request;
    END IF;
  ELSE
    BEGIN
      INSERT INTO public.pay_payment_correction_requests (
        pay_batch_id,
        correction_kind,
        status,
        requested_by_user_id,
        requested_at_utc,
        required_quantity,
        approved_count,
        golden_key_used,
        golden_key_user_id,
        reason,
        selection_json,
        selection_hash,
        plan_json,
        plan_hash,
        accepted_resolution_json,
        accepted_resolution_hash,
        source_bank_event_id,
        auto_requested,
        created_at_utc,
        authorised_at_utc,
        applied_at_utc,
        cancelled_at_utc,
        updated_at_utc
      )
      VALUES (
        p_pay_batch_id,
        'PRE_BANK_CANCEL',
        'AUTHORISED',
        p_actor_user_id,
        now(),
        v_required_quantity,
        v_required_quantity,
        false,
        NULL::uuid,
        v_reason,
        v_effective_selection_json,
        v_selection_hash,
        v_plan_json,
        v_plan_hash,
        v_accepted_resolution_json,
        v_accepted_resolution_hash,
        NULL::uuid,
        false,
        now(),
        now(),
        NULL::timestamptz,
        NULL::timestamptz,
        now()
      )
      RETURNING public.pay_payment_correction_requests.*
      INTO v_request;

      INSERT INTO public.pay_payment_correction_actions (
        correction_request_id,
        pay_batch_id,
        actor_kind,
        actor_user_id,
        action,
        action_at_utc,
        note,
        before_json,
        after_json,
        metadata_json
      )
      VALUES (
        v_request.id,
        p_pay_batch_id,
        'USER',
        p_actor_user_id,
        'REQUEST',
        now(),
        v_reason,
        NULL::jsonb,
        to_jsonb(v_request),
        jsonb_build_object(
          'requested_action', 'PRE_PROVIDER_CANCEL_AND_RECALCULATE',
          'correction_kind', 'PRE_BANK_CANCEL',
          'selection_hash', v_selection_hash,
          'idempotency_key', v_idempotency_key
        )
      );
    EXCEPTION WHEN unique_violation THEN
      SELECT request_rows.*
      INTO v_request
      FROM public.pay_payment_correction_requests AS request_rows
      WHERE request_rows.pay_batch_id = p_pay_batch_id
        AND request_rows.selection_hash = v_selection_hash
        AND request_rows.correction_kind = 'PRE_BANK_CANCEL'
        AND request_rows.status IN ('REQUESTED', 'AWAITING_AUTHORISATION', 'AUTHORISED', 'EXPANDED', 'PROCESSING', 'APPLIED_WITH_BLOCKERS')
      ORDER BY request_rows.created_at_utc
      LIMIT 1
      FOR UPDATE;
    END;
  END IF;

  v_expand_result := public.pay_payment_correction_expand_work(v_request.id, p_actor_user_id);
  v_process_result := public.pay_payment_correction_process_chunk(v_request.id, 50, 'cancel-not-sent-recalculate', p_actor_user_id);
  v_grouped_alert_updates := COALESCE(v_process_result -> 'grouped_alert_updates', '[]'::jsonb);
  v_is_complete := COALESCE((v_process_result ->> 'complete')::boolean, false);

  v_process_total := COALESCE(NULLIF(v_process_result #>> '{totals,total}', '')::integer, NULLIF(v_process_result ->> 'progress_total', '')::integer, 0);
  v_process_applied := COALESCE(NULLIF(v_process_result #>> '{totals,applied}', '')::integer, NULLIF(v_process_result ->> 'applied', '')::integer, 0);
  v_process_skipped := COALESCE(NULLIF(v_process_result #>> '{totals,skipped}', '')::integer, NULLIF(v_process_result ->> 'skipped', '')::integer, 0);
  v_process_blocked := COALESCE(NULLIF(v_process_result #>> '{totals,blocked}', '')::integer, NULLIF(v_process_result ->> 'blocked', '')::integer, 0);
  v_process_failed_retryable := COALESCE(NULLIF(v_process_result #>> '{totals,failed_retryable}', '')::integer, NULLIF(v_process_result ->> 'failed_retryable', '')::integer, 0);
  v_process_failed_final := COALESCE(NULLIF(v_process_result #>> '{totals,failed_final}', '')::integer, NULLIF(v_process_result ->> 'failed_final', '')::integer, 0);
  v_process_pending := COALESCE(NULLIF(v_process_result #>> '{totals,pending}', '')::integer, NULLIF(v_process_result ->> 'pending', '')::integer, 0);
  v_process_processing := COALESCE(NULLIF(v_process_result #>> '{totals,processing}', '')::integer, NULLIF(v_process_result ->> 'processing', '')::integer, 0);
  v_process_cancelled := COALESCE(NULLIF(v_process_result #>> '{totals,cancelled}', '')::integer, NULLIF(v_process_result ->> 'cancelled', '')::integer, 0);
  v_process_parent_status := UPPER(NULLIF(BTRIM(COALESCE(v_process_result ->> 'parent_status', '')), ''));

  SELECT jsonb_build_object(
    'pay_batch_id', p_pay_batch_id::text,
    'correction_request_id', v_request.id::text,
    'change_kind', 'PRE_BANK_CANCEL',
    'changed_pay_batch_item_ids', COALESCE(
      jsonb_agg(DISTINCT applied_scope_item.pay_batch_item_id::text ORDER BY applied_scope_item.pay_batch_item_id::text)
        FILTER (WHERE applied_scope_item.pay_batch_item_id IS NOT NULL),
      '[]'::jsonb
    ),
    'changed_pay_batch_candidate_ids', COALESCE(
      jsonb_agg(DISTINCT applied_scope_item.pay_batch_candidate_id::text ORDER BY applied_scope_item.pay_batch_candidate_id::text)
        FILTER (WHERE applied_scope_item.pay_batch_candidate_id IS NOT NULL),
      '[]'::jsonb
    ),
    'changed_candidate_ids', COALESCE(
      jsonb_agg(DISTINCT applied_scope_item.candidate_id::text ORDER BY applied_scope_item.candidate_id::text)
        FILTER (WHERE applied_scope_item.candidate_id IS NOT NULL),
      '[]'::jsonb
    ),
    'changed_transfer_ids', COALESCE(
      jsonb_agg(DISTINCT applied_scope_item.pay_bank_transfer_id::text ORDER BY applied_scope_item.pay_bank_transfer_id::text)
        FILTER (WHERE applied_scope_item.pay_bank_transfer_id IS NOT NULL),
      '[]'::jsonb
    ),
    'changed_finance_case_ids', COALESCE(
      jsonb_agg(DISTINCT applied_scope_item.finance_case_id::text ORDER BY applied_scope_item.finance_case_id::text)
        FILTER (WHERE applied_scope_item.finance_case_id IS NOT NULL),
      '[]'::jsonb
    ),
    'changed_finance_component_ids', COALESCE(
      jsonb_agg(DISTINCT applied_scope_item.finance_component_id::text ORDER BY applied_scope_item.finance_component_id::text)
        FILTER (WHERE applied_scope_item.finance_component_id IS NOT NULL),
      '[]'::jsonb
    ),
    'changed_reservation_ids', COALESCE(
      jsonb_agg(DISTINCT applied_scope_item.reservation_id::text ORDER BY applied_scope_item.reservation_id::text)
        FILTER (WHERE applied_scope_item.reservation_id IS NOT NULL),
      '[]'::jsonb
    ),
    'applied_correction_item_count', count(*)::integer
  )
  INTO v_request_changed_scope_json
  FROM public.pay_payment_correction_items AS applied_scope_item
  WHERE applied_scope_item.correction_request_id = v_request.id
    AND applied_scope_item.correction_item_kind = 'PRE_BANK_CANCEL'
    AND applied_scope_item.status = 'APPLIED';

  v_request_changed_scope_json := COALESCE(v_request_changed_scope_json, jsonb_build_object(
    'pay_batch_id', p_pay_batch_id::text,
    'correction_request_id', v_request.id::text,
    'change_kind', 'PRE_BANK_CANCEL',
    'changed_pay_batch_item_ids', '[]'::jsonb,
    'changed_pay_batch_candidate_ids', '[]'::jsonb,
    'changed_candidate_ids', '[]'::jsonb,
    'changed_transfer_ids', '[]'::jsonb,
    'changed_finance_case_ids', '[]'::jsonb,
    'changed_finance_component_ids', '[]'::jsonb,
    'changed_reservation_ids', '[]'::jsonb,
    'applied_correction_item_count', 0
  ));

  v_process_voided_item_count := COALESCE(
    jsonb_array_length(
      CASE
        WHEN COALESCE(jsonb_typeof(v_request_changed_scope_json->'changed_pay_batch_item_ids'), 'null') = 'array'
          THEN v_request_changed_scope_json->'changed_pay_batch_item_ids'
        ELSE '[]'::jsonb
      END
    ),
    0
  );

  v_process_failed := (
    COALESCE((v_process_result ->> 'ok')::boolean, true) IS NOT TRUE
    OR COALESCE(v_process_failed_final, 0) > 0
    OR COALESCE(v_process_failed_retryable, 0) > 0
    OR COALESCE(v_process_blocked, 0) > 0
    OR v_process_parent_status IN ('FAILED', 'BLOCKED', 'APPLIED_WITH_BLOCKERS')
    OR (
      COALESCE(v_process_total, 0) > 0
      AND COALESCE(v_is_complete, false) = true
      AND COALESCE(v_process_applied, 0) + COALESCE(v_process_skipped, 0) + COALESCE(v_process_cancelled, 0) <= 0
    )
  );

  v_process_error_code := CASE
    WHEN COALESCE(v_process_failed_final, 0) > 0 OR v_process_parent_status = 'FAILED' THEN 'PAY_PAYMENT_CANCEL_NOT_SENT_PROCESS_FAILED'
    WHEN COALESCE(v_process_failed_retryable, 0) > 0 THEN 'PAY_PAYMENT_CANCEL_NOT_SENT_PROCESS_RETRYABLE'
    WHEN COALESCE(v_process_blocked, 0) > 0 OR v_process_parent_status IN ('BLOCKED', 'APPLIED_WITH_BLOCKERS') THEN 'PAY_PAYMENT_CANCEL_NOT_SENT_PROCESS_BLOCKED'
    WHEN COALESCE(v_process_failed, false) THEN 'PAY_PAYMENT_CANCEL_NOT_SENT_NOT_APPLIED'
    ELSE NULL::text
  END;

  v_process_error_message := CASE
    WHEN COALESCE(v_process_failed_final, 0) > 0 OR v_process_parent_status = 'FAILED' THEN 'Cancel Draft Batch could not be completed because the cancellation work item failed.'
    WHEN COALESCE(v_process_failed_retryable, 0) > 0 THEN 'Cancel Draft Batch could not be completed because the cancellation work item needs to be retried.'
    WHEN COALESCE(v_process_blocked, 0) > 0 OR v_process_parent_status IN ('BLOCKED', 'APPLIED_WITH_BLOCKERS') THEN 'Cancel Draft Batch could not be completed because the cancellation work item was blocked.'
    WHEN COALESCE(v_process_failed, false) THEN 'Cancel Draft Batch could not be completed because no cancellation work item was applied.'
    ELSE NULL::text
  END;

  v_signal_result := public.banking_pay_batch_signal_touch(
    p_pay_batch_id,
    'PAYMENT_CANCEL_NOT_SENT_AND_RECALCULATE',
    'pay_payment_cancel_not_sent_and_recalculate',
    COALESCE(v_request_changed_scope_json, '{}'::jsonb) || jsonb_build_object(
      'correction_request_id', v_request.id::text,
      'selection_hash', v_selection_hash,
      'payment_lifecycle_state', v_lifecycle_state,
      'resumed_existing_correction_request', v_resume_existing_request,
      'resume_blocked_work_item_count', v_resume_blocked_reset_count,
      'progress', v_process_result
    ),
    true,
    true,
    COALESCE(jsonb_array_length(v_grouped_alert_updates), 0) > 0,
    true
  );

  IF COALESCE(v_process_failed, false) THEN
    RETURN jsonb_build_object(
      'ok', false,
      'error_code', v_process_error_code,
      'code', v_process_error_code,
      'message', v_process_error_message,
      'user_message', v_process_error_message,
      'pay_batch_id', p_pay_batch_id::text,
      'correction_request_id', v_request.id::text,
      'resumed_existing_correction_request', v_resume_existing_request,
      'resume_blocked_work_item_count', v_resume_blocked_reset_count,
      'payment_lifecycle_state', v_lifecycle_state,
      'recommended_action', v_recommended_action,
      'next_step', NULL::text,
      'progress_completed', COALESCE(v_process_applied, 0) + COALESCE(v_process_skipped, 0) + COALESCE(v_process_cancelled, 0),
      'progress_total', COALESCE(v_process_total, 0),
      'is_complete', v_is_complete,
      'voided_transfer_count', COALESCE((v_process_result ->> 'updated_transfer_count')::integer, 0),
      'voided_item_count', COALESCE(v_process_voided_item_count, 0),
      'released_reservation_count', COALESCE((v_process_result ->> 'released_reservations')::integer, 0),
      'restored_finance_component_count', COALESCE((v_process_result ->> 'restored_finance_components')::integer, 0),
      'carry_forward_created_count', COALESCE((v_process_result ->> 'carry_forward_created')::integer, 0),
      'carry_forward_existing_count', COALESCE((v_process_result ->> 'carry_forward_existing')::integer, 0),
      'carry_forward_released_count', COALESCE((v_process_result ->> 'carry_forward_released')::integer, 0),
      'freshness_dirty_result', COALESCE(v_process_result -> 'freshness_dirty_result', '{}'::jsonb),
      'changed_scope_json', COALESCE(v_request_changed_scope_json, '{}'::jsonb),
      'diagnostic_json', v_diagnostic_json,
      'expand_result', v_expand_result,
      'process_result', v_process_result,
      'live_signal', v_signal_result,
      'grouped_alert_summary_impact', v_grouped_alert_updates,
      'failure_summary', jsonb_build_object(
        'parent_status', v_process_parent_status,
        'total', COALESCE(v_process_total, 0),
        'applied', COALESCE(v_process_applied, 0),
        'skipped', COALESCE(v_process_skipped, 0),
        'blocked', COALESCE(v_process_blocked, 0),
        'failed_retryable', COALESCE(v_process_failed_retryable, 0),
        'failed_final', COALESCE(v_process_failed_final, 0),
        'pending', COALESCE(v_process_pending, 0),
        'processing', COALESCE(v_process_processing, 0),
        'cancelled', COALESCE(v_process_cancelled, 0)
      )
    );
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'pay_batch_id', p_pay_batch_id::text,
    'correction_request_id', v_request.id::text,
    'resumed_existing_correction_request', v_resume_existing_request,
    'resume_blocked_work_item_count', v_resume_blocked_reset_count,
    'payment_lifecycle_state', v_lifecycle_state,
    'recommended_action', v_recommended_action,
    'next_step', 'AMEND_TIMESHEET_AND_RECALCULATE',
    'progress_completed', COALESCE((v_process_result -> 'totals' ->> 'applied')::integer, COALESCE((v_process_result ->> 'applied')::integer, 0)),
    'progress_total', COALESCE((v_process_result -> 'totals' ->> 'total')::integer, 0),
    'is_complete', v_is_complete,
    'voided_transfer_count', COALESCE((v_process_result ->> 'updated_transfer_count')::integer, 0),
    'voided_item_count', COALESCE(v_process_voided_item_count, 0),
    'released_reservation_count', COALESCE((v_process_result ->> 'released_reservations')::integer, 0),
    'restored_finance_component_count', COALESCE((v_process_result ->> 'restored_finance_components')::integer, 0),
    'carry_forward_created_count', COALESCE((v_process_result ->> 'carry_forward_created')::integer, 0),
    'carry_forward_existing_count', COALESCE((v_process_result ->> 'carry_forward_existing')::integer, 0),
    'carry_forward_released_count', COALESCE((v_process_result ->> 'carry_forward_released')::integer, 0),
    'freshness_dirty_result', COALESCE(v_process_result -> 'freshness_dirty_result', '{}'::jsonb),
    'changed_scope_json', COALESCE(v_request_changed_scope_json, '{}'::jsonb),
    'diagnostic_json', v_diagnostic_json,
    'expand_result', v_expand_result,
    'process_result', v_process_result,
    'live_signal', v_signal_result,
    'grouped_alert_summary_impact', v_grouped_alert_updates
  );
END;
$function$;

ALTER FUNCTION pay_payment_cancel_not_sent_and_recalculate(uuid,jsonb,uuid,text,text,jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION pay_payment_cancel_not_sent_and_recalculate(uuid,jsonb,uuid,text,text,jsonb) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION pay_payment_cancel_not_sent_and_recalculate(uuid,jsonb,uuid,text,text,jsonb) TO postgres;
GRANT EXECUTE ON FUNCTION pay_payment_cancel_not_sent_and_recalculate(uuid,jsonb,uuid,text,text,jsonb) TO authenticated;
GRANT EXECUTE ON FUNCTION pay_payment_cancel_not_sent_and_recalculate(uuid,jsonb,uuid,text,text,jsonb) TO service_role;
