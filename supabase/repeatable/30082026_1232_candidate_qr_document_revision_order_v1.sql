-- AV-981: Candidate PAPER preparation must build the official QR document
-- from the final revision after its authorised AWAITING_MANUAL_SIGNATURE
-- transition. This replacement preserves every existing gate, identity,
-- finance, document, mail and idempotency authority.
CREATE OR REPLACE FUNCTION public.timesheet_qr_send_enqueue_v1(
  p_timesheet_id uuid,
  p_expected_timesheet_id uuid DEFAULT NULL,
  p_actor_user_id uuid DEFAULT NULL,
  p_idempotency_key text DEFAULT NULL,
  p_now_utc timestamp with time zone DEFAULT now()
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamp with time zone := COALESCE(p_now_utc, now());
  v_requested_timesheet_id uuid := NULL;
  v_requested_booking_id text := NULL;
  v_current_timesheet_id uuid := NULL;
  v_current_version integer := NULL;
  v_sheet_scope text := NULL;
  v_submission_mode text := NULL;
  v_contract_id uuid := NULL;
  v_week_ending_date date := NULL;
  v_worked_start_iso timestamp with time zone := NULL;
  v_worked_end_iso timestamp with time zone := NULL;
  v_actual_schedule_json jsonb := NULL;
  v_qr_status text := NULL;
  v_qr_token text := NULL;
  v_effective_qr_token text := NULL;
  v_qr_payload_json jsonb := '{}'::jsonb;
  v_payload_qr_token text := NULL;
  v_qr_generated_at timestamp with time zone := NULL;
  v_qr_scanned_at timestamp with time zone := NULL;
  v_qr_signed_hash text := NULL;
  v_qr_signed_at_utc timestamp with time zone := NULL;
  v_document_revision bigint := NULL;
  v_document_state text := NULL;
  v_updated_at timestamp with time zone := NULL;
  v_tsfin_id uuid := NULL;
  v_tsfin_processing_status text := NULL;
  v_locked_by_invoice_id uuid := NULL;
  v_paid_at_utc timestamp with time zone := NULL;
  v_invoice_breakdown_json jsonb := NULL;
  v_has_segment_invoice_lock boolean := FALSE;
  v_has_hours_for_send boolean := FALSE;
  v_candidate_id uuid := NULL;
  v_client_id uuid := NULL;
  v_candidate_email text := NULL;
  v_candidate_name text := NULL;
  v_candidate_opt_in_email boolean := TRUE;
  v_recipient_available boolean := FALSE;
  v_contract_candidate_id uuid := NULL;
  v_contract_client_id uuid := NULL;
  v_idempotency_key text := NULL;
  v_client_idempotency_key text := NULL;
  v_recipient_namespace text := NULL;
  v_mail_held_until_pdf_rendered boolean := TRUE;
  v_mail_hold_reason text := NULL;
  v_mail_scope_json jsonb := '{}'::jsonb;
  v_existing_mail_id uuid := NULL;
  v_existing_mail_status text := NULL;
  v_existing_mail_scope_json jsonb := '{}'::jsonb;
  v_mail_job_id uuid := NULL;
  v_pdf_job_id uuid := NULL;
  v_existing_pdf_job_id uuid := NULL;
  v_job_id uuid := NULL;
  v_send_state text := NULL;
  v_pdf_key text := NULL;
  v_document_idempotency text := NULL;
  v_document_operation_id uuid := NULL;
  v_document_version_id uuid := NULL;
  v_document_version_status text := NULL;
  v_snapshot_json jsonb := '{}'::jsonb;
  v_snapshot_model jsonb := '{}'::jsonb;
  v_snapshot_hash text := NULL;
  v_snapshot_valid boolean := FALSE;
  v_snapshot_error_code text := NULL;
  v_week_period_hash text := NULL;
  v_schedule_hash text := NULL;
  v_reference_signature text := NULL;
  v_additional_units_hash text := NULL;
  v_presentation_settings_hash text := NULL;
  v_qr_payload_hash text := NULL;
  v_complete_printable_content_hash text := NULL;
  v_issue_type text := NULL;
  v_rotate_token boolean := FALSE;
  v_mail_subject text := NULL;
  v_mail_body_text text := NULL;
  v_mail_body_html text := NULL;
  v_mail_reference text := NULL;
  v_mail_scheduled_for_utc timestamp with time zone := NULL;
  v_created_by_user_id uuid := NULL;
  v_post_row jsonb := NULL;
  v_row_key text := NULL;
  v_storage_key text := NULL;
  v_row_signature text := NULL;
  v_candidate_paper_workflow_count integer := 0;
  v_candidate_paper_workflow_id uuid := NULL;
  v_candidate_paper_workflow_generation integer := NULL;
  v_candidate_paper_manifest_sha256 text := NULL;
  v_candidate_paper_binding_json jsonb := '{}'::jsonb;
  v_candidate_paper_exact_mail_exists boolean := FALSE;
BEGIN
  IF p_timesheet_id IS NULL THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'TIMESHEET_ID_REQUIRED',
      'message', 'p_timesheet_id is required.',
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  SELECT requested_ts.timesheet_id,
         requested_ts.booking_id
    INTO v_requested_timesheet_id,
         v_requested_booking_id
  FROM public.timesheets AS requested_ts
  WHERE requested_ts.timesheet_id = p_timesheet_id
  LIMIT 1;

  IF v_requested_timesheet_id IS NULL THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'TIMESHEET_NOT_FOUND',
      'message', 'Timesheet was not found.',
      'requested_timesheet_id', p_timesheet_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  SELECT current_ts.timesheet_id,
         current_ts.version
    INTO v_current_timesheet_id,
         v_current_version
  FROM public.timesheets AS current_ts
  WHERE current_ts.booking_id = v_requested_booking_id
    AND current_ts.is_current = TRUE
  ORDER BY current_ts.version DESC NULLS LAST,
           current_ts.updated_at DESC NULLS LAST,
           current_ts.created_at DESC NULLS LAST,
           current_ts.timesheet_id DESC
  LIMIT 1;

  IF v_current_timesheet_id IS NULL THEN
    v_current_timesheet_id := v_requested_timesheet_id;
  END IF;

  IF p_expected_timesheet_id IS NOT NULL AND p_expected_timesheet_id IS DISTINCT FROM v_current_timesheet_id THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'TIMESHEET_MOVED',
      'message', 'Timesheet has moved to a newer current row.',
      'requested_timesheet_id', p_timesheet_id,
      'expected_timesheet_id', p_expected_timesheet_id,
      'current_timesheet_id', v_current_timesheet_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  IF v_requested_timesheet_id IS DISTINCT FROM v_current_timesheet_id THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'TIMESHEET_MOVED',
      'message', 'timesheet_qr_send_enqueue_v1 requires the current timesheet row.',
      'requested_timesheet_id', p_timesheet_id,
      'expected_timesheet_id', COALESCE(p_expected_timesheet_id, p_timesheet_id),
      'current_timesheet_id', v_current_timesheet_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  SELECT ts_current.sheet_scope::text,
         ts_current.submission_mode::text,
         ts_current.contract_id,
         ts_current.week_ending_date,
         ts_current.worked_start_iso,
         ts_current.worked_end_iso,
         ts_current.actual_schedule_json,
         ts_current.qr_status::text,
         ts_current.qr_token,
         ts_current.qr_payload_json,
         ts_current.qr_generated_at,
         ts_current.qr_scanned_at,
         ts_current.qr_signed_hash,
         ts_current.qr_signed_at_utc,
         ts_current.document_revision,
         ts_current.document_state::text,
         ts_current.version,
         ts_current.updated_at
    INTO v_sheet_scope,
         v_submission_mode,
         v_contract_id,
         v_week_ending_date,
         v_worked_start_iso,
         v_worked_end_iso,
         v_actual_schedule_json,
         v_qr_status,
         v_qr_token,
         v_qr_payload_json,
         v_qr_generated_at,
         v_qr_scanned_at,
         v_qr_signed_hash,
         v_qr_signed_at_utc,
         v_document_revision,
         v_document_state,
         v_current_version,
         v_updated_at
  FROM public.timesheets AS ts_current
  WHERE ts_current.timesheet_id = v_current_timesheet_id
    AND ts_current.is_current = TRUE
  LIMIT 1
  FOR UPDATE;

  IF v_sheet_scope IS NULL THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'CURRENT_TIMESHEET_NOT_FOUND',
      'message', 'Current timesheet was not found.',
      'current_timesheet_id', v_current_timesheet_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  IF UPPER(COALESCE(v_sheet_scope, '')) NOT IN ('DAILY', 'WEEKLY') THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'UNSUPPORTED_SHEET_SCOPE',
      'message', 'QR send is only supported for DAILY or WEEKLY timesheets.',
      'current_timesheet_id', v_current_timesheet_id,
      'sheet_scope', v_sheet_scope,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  IF UPPER(COALESCE(v_submission_mode, '')) <> 'MANUAL' THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'NOT_MANUAL_QR_ROUTE',
      'message', 'QR send requires a MANUAL submission-mode timesheet with QR enabled.',
      'current_timesheet_id', v_current_timesheet_id,
      'submission_mode', v_submission_mode,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  IF UPPER(COALESCE(v_qr_status, '')) <> 'PENDING' THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'QR_NOT_PENDING',
      'message', 'QR send requires qr_status=PENDING.',
      'current_timesheet_id', v_current_timesheet_id,
      'qr_status', v_qr_status,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  IF v_qr_scanned_at IS NOT NULL OR NULLIF(BTRIM(COALESCE(v_qr_signed_hash, '')), '') IS NOT NULL OR v_qr_signed_at_utc IS NOT NULL THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'QR_ALREADY_SIGNED',
      'message', 'Cannot queue QR send: timesheet already has signed QR markers.',
      'current_timesheet_id', v_current_timesheet_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  WITH locked_candidate_paper AS MATERIALIZED (
    SELECT workflow.id,
           workflow.generation,
           workflow.paper_return_manifest_sha256
    FROM public.candidate_submission_workflows AS workflow
    WHERE workflow.route = 'PAPER'
      AND workflow.state = 'AWAITING_PAPER_RETURN'
      AND (
        workflow.target_timesheet_id = v_current_timesheet_id
        OR workflow.anchor_timesheet_id = v_current_timesheet_id
      )
    ORDER BY workflow.id
    FOR UPDATE
  )
  SELECT count(*)::integer,
         (array_agg(locked_candidate_paper.id ORDER BY locked_candidate_paper.id))[1],
         (array_agg(locked_candidate_paper.generation ORDER BY locked_candidate_paper.id))[1],
         (array_agg(encode(locked_candidate_paper.paper_return_manifest_sha256, 'hex')
                    ORDER BY locked_candidate_paper.id))[1]
    INTO v_candidate_paper_workflow_count,
         v_candidate_paper_workflow_id,
         v_candidate_paper_workflow_generation,
         v_candidate_paper_manifest_sha256
  FROM locked_candidate_paper;

  IF v_candidate_paper_workflow_count > 1 THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'CANDIDATE_PAPER_WORKFLOW_CONFLICT',
      'message', 'More than one active Candidate PAPER workflow targets this timesheet.',
      'current_timesheet_id', v_current_timesheet_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  IF v_candidate_paper_workflow_count = 1 THEN
    IF v_candidate_paper_workflow_generation IS NULL
       OR v_candidate_paper_workflow_generation < 1
       OR COALESCE(v_candidate_paper_manifest_sha256, '') !~ '^[0-9a-f]{64}$' THEN
      RETURN jsonb_build_object(
        'ok', FALSE,
        'queued', FALSE,
        'operation', 'timesheet_qr_send_enqueue',
        'error_code', 'CANDIDATE_PAPER_RETURN_MANIFEST_STALE',
        'message', 'The Candidate PAPER workflow does not have a valid frozen return manifest.',
        'current_timesheet_id', v_current_timesheet_id,
        'recipient_available', FALSE,
        'send_state', 'REJECTED'
      );
    END IF;
    v_candidate_paper_binding_json := jsonb_build_object(
      'candidate_mail_authority', 'CANDIDATE_PAPER_V1',
      'candidate_workflow_id', v_candidate_paper_workflow_id,
      'candidate_workflow_generation', v_candidate_paper_workflow_generation,
      'paper_return_manifest_sha256', v_candidate_paper_manifest_sha256,
      'candidate_paper_pack_ready', FALSE,
      'candidate_paper_pack_retryable', FALSE,
      'candidate_paper_pack_failure_class', NULL,
      'candidate_paper_pack_failure_code', NULL,
      'candidate_paper_pack_failure_contract_version', NULL,
      'candidate_paper_pack_failed_at_utc', NULL,
      'candidate_paper_pack_preparation_started_at_utc', p_now_utc,
      'candidate_paper_pack_preparation_deadline_at_utc', p_now_utc + interval '15 minutes',
      'candidate_paper_pack_attempt_count', 0,
      'candidate_paper_pack_attempt_token', NULL,
      'candidate_paper_pack_attempt_expires_at_utc', NULL,
      'candidate_paper_pack_next_retry_at_utc', NULL
    );
  END IF;

  SELECT tf_current.id,
         tf_current.processing_status::text,
         tf_current.locked_by_invoice_id,
         tf_current.paid_at_utc,
         tf_current.invoice_breakdown_json,
         tf_current.candidate_id,
         tf_current.client_id
    INTO v_tsfin_id,
         v_tsfin_processing_status,
         v_locked_by_invoice_id,
         v_paid_at_utc,
         v_invoice_breakdown_json,
         v_candidate_id,
         v_client_id
  FROM public.timesheets_financials AS tf_current
  WHERE tf_current.timesheet_id = v_current_timesheet_id
    AND tf_current.is_current = TRUE
  ORDER BY tf_current.computed_at_utc DESC NULLS LAST,
           tf_current.created_at DESC NULLS LAST,
           tf_current.updated_at DESC NULLS LAST,
           tf_current.id DESC
  LIMIT 1
  FOR UPDATE;

  IF v_tsfin_id IS NULL THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'NO_TSFIN',
      'message', 'No current financial snapshot exists for this timesheet.',
      'current_timesheet_id', v_current_timesheet_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  SELECT EXISTS (
    SELECT 1
    FROM jsonb_array_elements(
      CASE
        WHEN v_invoice_breakdown_json IS NULL THEN '[]'::jsonb
        WHEN jsonb_typeof(v_invoice_breakdown_json) = 'array' THEN v_invoice_breakdown_json
        WHEN jsonb_typeof(v_invoice_breakdown_json) = 'object'
         AND jsonb_typeof(v_invoice_breakdown_json->'segments') = 'array' THEN v_invoice_breakdown_json->'segments'
        ELSE '[]'::jsonb
      END
    ) AS invoice_segment(segment_json)
    WHERE NULLIF(BTRIM(COALESCE(invoice_segment.segment_json->>'invoice_locked_invoice_id', '')), '') IS NOT NULL
  ) INTO v_has_segment_invoice_lock;

  IF v_locked_by_invoice_id IS NOT NULL OR v_paid_at_utc IS NOT NULL OR v_has_segment_invoice_lock = TRUE THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'TIMESHEET_LOCKED_OR_PAID',
      'message', 'Cannot queue QR send: timesheet is locked, invoice-locked, or paid.',
      'current_timesheet_id', v_current_timesheet_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  IF UPPER(COALESCE(v_sheet_scope, '')) = 'WEEKLY' THEN
    SELECT EXISTS (
      SELECT 1
      FROM jsonb_array_elements(
        CASE
          WHEN jsonb_typeof(COALESCE(v_actual_schedule_json, '[]'::jsonb)) = 'array' THEN COALESCE(v_actual_schedule_json, '[]'::jsonb)
          ELSE '[]'::jsonb
        END
      ) AS schedule_segment(segment_json)
      WHERE NULLIF(BTRIM(COALESCE(schedule_segment.segment_json->>'start', schedule_segment.segment_json->>'start_utc', '')), '') IS NOT NULL
        AND NULLIF(BTRIM(COALESCE(schedule_segment.segment_json->>'end', schedule_segment.segment_json->>'end_utc', '')), '') IS NOT NULL
    ) OR EXISTS (
      SELECT 1
      FROM jsonb_array_elements(
        CASE
          WHEN v_invoice_breakdown_json IS NULL THEN '[]'::jsonb
          WHEN jsonb_typeof(v_invoice_breakdown_json) = 'array' THEN v_invoice_breakdown_json
          WHEN jsonb_typeof(v_invoice_breakdown_json) = 'object'
           AND jsonb_typeof(v_invoice_breakdown_json->'segments') = 'array' THEN v_invoice_breakdown_json->'segments'
          ELSE '[]'::jsonb
        END
      ) AS invoice_segment(segment_json)
    ) INTO v_has_hours_for_send;
  ELSE
    v_has_hours_for_send := v_worked_start_iso IS NOT NULL AND v_worked_end_iso IS NOT NULL;
  END IF;

  IF COALESCE(v_has_hours_for_send, FALSE) = FALSE THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'NO_HOURS_RECORDED',
      'message', 'Cannot queue QR send: no hours are recorded yet.',
      'current_timesheet_id', v_current_timesheet_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  IF v_contract_id IS NOT NULL THEN
    SELECT contract_row.candidate_id,
           contract_row.client_id
      INTO v_contract_candidate_id,
           v_contract_client_id
    FROM public.contracts AS contract_row
    WHERE contract_row.id = v_contract_id
    LIMIT 1;
  END IF;

  v_candidate_id := COALESCE(v_contract_candidate_id, v_candidate_id);
  v_client_id := COALESCE(v_contract_client_id, v_client_id);

  IF v_candidate_id IS NULL THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', 'CANDIDATE_NOT_FOUND',
      'message', 'Cannot queue QR send: candidate could not be resolved.',
      'current_timesheet_id', v_current_timesheet_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  SELECT NULLIF(BTRIM(candidate_row.email), ''),
         COALESCE(NULLIF(BTRIM(candidate_row.display_name), ''), NULLIF(BTRIM(CONCAT_WS(' ', NULLIF(BTRIM(candidate_row.first_name), ''), NULLIF(BTRIM(candidate_row.last_name), ''))), '')),
         COALESCE(candidate_row.opt_in_email, TRUE)
    INTO v_candidate_email,
         v_candidate_name,
         v_candidate_opt_in_email
  FROM public.candidates AS candidate_row
  WHERE candidate_row.id = v_candidate_id
  LIMIT 1;

  v_recipient_available := NULLIF(BTRIM(COALESCE(v_candidate_email, '')), '') IS NOT NULL AND COALESCE(v_candidate_opt_in_email, TRUE) = TRUE;

  IF v_recipient_available = FALSE THEN
    RETURN jsonb_build_object(
      'ok', FALSE,
      'queued', FALSE,
      'operation', 'timesheet_qr_send_enqueue',
      'error_code', CASE WHEN NULLIF(BTRIM(COALESCE(v_candidate_email, '')), '') IS NULL THEN 'CANDIDATE_EMAIL_MISSING' ELSE 'CANDIDATE_EMAIL_OPTED_OUT' END,
      'message', CASE WHEN NULLIF(BTRIM(COALESCE(v_candidate_email, '')), '') IS NULL THEN 'Cannot queue QR send: candidate email is missing.' ELSE 'Cannot queue QR send: candidate has opted out of email.' END,
      'current_timesheet_id', v_current_timesheet_id,
      'candidate_id', v_candidate_id,
      'recipient_available', FALSE,
      'send_state', 'REJECTED'
    );
  END IF;

  v_payload_qr_token := CASE
    WHEN jsonb_typeof(v_qr_payload_json) = 'object' THEN NULLIF(BTRIM(COALESCE(v_qr_payload_json->>'tok', '')), '')
    ELSE NULL
  END;
  IF v_candidate_paper_workflow_count = 1 THEN
    SELECT EXISTS(
      SELECT 1
      FROM public.mail_outbox candidate_mail
      WHERE candidate_mail.type='TIMESHEET_QR'
        AND candidate_mail.context_kind='timesheets'
        AND candidate_mail.context_id=v_current_timesheet_id
        AND candidate_mail.payment_scope_json->>'candidate_workflow_id'=v_candidate_paper_workflow_id::text
        AND candidate_mail.payment_scope_json->>'candidate_workflow_generation'=v_candidate_paper_workflow_generation::text
        AND lower(coalesce(candidate_mail.payment_scope_json->>'paper_return_manifest_sha256',''))
              =v_candidate_paper_manifest_sha256
        AND lower(coalesce(candidate_mail.payment_scope_json->>'candidate_paper_generation_retired','false'))
              IN ('false','f','0','no')
    ) INTO v_candidate_paper_exact_mail_exists;
  END IF;
  v_rotate_token :=
    NULLIF(BTRIM(COALESCE(v_qr_token, '')), '') IS NULL
    OR UPPER(COALESCE(v_document_state, 'STALE')) IN ('STALE', 'FAILED')
    OR (v_candidate_paper_workflow_count = 1 AND NOT v_candidate_paper_exact_mail_exists);
  v_effective_qr_token := CASE
    WHEN v_rotate_token THEN gen_random_uuid()::text
    ELSE COALESCE(NULLIF(BTRIM(COALESCE(v_qr_token, '')), ''),
      v_payload_qr_token, gen_random_uuid()::text)
  END;
  v_qr_payload_json := jsonb_build_object('v', 1, 'tok', v_effective_qr_token);
  v_issue_type := CASE
    WHEN NULLIF(BTRIM(COALESCE(v_qr_token, '')), '') IS NULL THEN 'NEW_ISSUE'
    WHEN v_rotate_token THEN 'REISSUED_CHANGED'
    ELSE 'RESENT_UNCHANGED'
  END;

  UPDATE public.timesheets AS ts_qr_update
     SET qr_token = v_effective_qr_token,
         qr_payload_json = v_qr_payload_json,
         qr_generated_at = CASE
           WHEN v_rotate_token THEN v_now
           ELSE COALESCE(ts_qr_update.qr_generated_at, v_now) END,
         qr_r2_key = NULL,
         qr_scanned_at = NULL,
         qr_scan_info_json = NULL,
         updated_at = v_now
   WHERE ts_qr_update.timesheet_id = v_current_timesheet_id
     AND ts_qr_update.is_current = TRUE
  RETURNING ts_qr_update.qr_generated_at,
            ts_qr_update.updated_at,
            ts_qr_update.document_revision
       INTO v_qr_generated_at,
            v_updated_at,
            v_document_revision;

  IF UPPER(COALESCE(v_tsfin_processing_status, '')) <> 'AWAITING_MANUAL_SIGNATURE' THEN
    UPDATE public.timesheets_financials AS tf_update
       SET processing_status = 'AWAITING_MANUAL_SIGNATURE'::public.ts_fin_processing_status_enum,
           updated_at = v_now
     WHERE tf_update.id = v_tsfin_id
       AND tf_update.is_current = TRUE;
  END IF;

  -- The processing-state update above legitimately invokes the central
  -- Timesheet document invalidation trigger. Capture the authoritative
  -- revision only after that update, so the queued QR document and the
  -- Timesheet current-document pointer can never be one revision apart.
  SELECT t.document_revision
    INTO v_document_revision
  FROM public.timesheets t
  WHERE t.timesheet_id=v_current_timesheet_id AND t.is_current
  LIMIT 1;

  SELECT tms_user_check.id
    INTO v_created_by_user_id
  FROM public.tms_users AS tms_user_check
  WHERE tms_user_check.id = p_actor_user_id
  LIMIT 1;

  v_client_idempotency_key := NULLIF(
    REGEXP_REPLACE(
      LOWER(BTRIM(COALESCE(p_idempotency_key, ''))),
      '[^a-z0-9._:-]+',
      '-',
      'g'
    ),
    ''
  );

  IF v_client_idempotency_key IS NULL THEN
    v_client_idempotency_key := 'auto:' || md5(CONCAT_WS('|',
      v_current_timesheet_id::text,
      COALESCE(v_current_version::text, ''),
      LOWER(COALESCE(v_candidate_email, ''))
    ));
  END IF;

  v_recipient_namespace := md5(LOWER(COALESCE(v_candidate_email, '')));

  v_idempotency_key := CASE
    WHEN v_candidate_paper_workflow_count = 1 THEN
      'candidate_paper_send:'||v_candidate_paper_workflow_id::text
      ||':g'||v_candidate_paper_workflow_generation::text
      ||':manifest:'||v_candidate_paper_manifest_sha256
      ||':timesheet:'||v_current_timesheet_id::text
      ||':recipient:'||v_recipient_namespace
    ELSE
      'timesheet_qr_send:'||v_current_timesheet_id::text
      ||':v'||COALESCE(v_current_version::text,'0')
      ||':recipient:'||v_recipient_namespace
      ||':key:'||md5(v_client_idempotency_key)
    END;

  PERFORM pg_advisory_xact_lock(hashtext('timesheet_qr_send:' || v_current_timesheet_id::text));
  PERFORM pg_advisory_xact_lock(hashtext(v_idempotency_key));

  v_pdf_key := 'docs-pdf/timesheets/ts_' || v_current_timesheet_id::text || '.pdf';
  v_mail_reference := v_idempotency_key;
  v_mail_scheduled_for_utc := TIMESTAMPTZ '9999-12-31 00:00:00+00';
  v_mail_subject := CASE
    WHEN UPPER(COALESCE(v_sheet_scope, '')) = 'WEEKLY' THEN 'Weekly QR timesheet – week ending ' || COALESCE(v_week_ending_date::text, '(unknown)')
    ELSE 'Daily QR timesheet – ' || COALESCE((v_worked_start_iso AT TIME ZONE 'Europe/London')::date::text, '(unknown date)')
  END;
  v_mail_body_text := CONCAT_WS(E'\n',
    'Please print the attached timesheet, ask the ward manager to sign it,',
    'and then upload the signed copy via the app.',
    CASE
      WHEN private._candidate_feature_enabled_current_v1('candidate_paper_qr')
       AND COALESCE(v_current_version,1)>1
      THEN 'Please remember to sign the replacement timesheet before returning it.'
      ELSE NULL
    END,
    '',
    CASE WHEN UPPER(COALESCE(v_sheet_scope, '')) = 'WEEKLY' THEN 'Week ending: ' || COALESCE(v_week_ending_date::text, '(unknown)') ELSE 'Date: ' || COALESCE((v_worked_start_iso AT TIME ZONE 'Europe/London')::date::text, '(unknown date)') END,
    'Timesheet ID: ' || v_current_timesheet_id::text
  );
  v_mail_body_html := '<p>Please print the attached timesheet, ask the ward manager to sign it, and then upload the signed copy via the app.<br/>'
    || CASE
      WHEN private._candidate_feature_enabled_current_v1('candidate_paper_qr')
       AND COALESCE(v_current_version,1)>1
      THEN 'Please remember to sign the replacement timesheet before returning it.<br/>'
      ELSE ''
    END
    || '<br/>'
    || CASE WHEN UPPER(COALESCE(v_sheet_scope, '')) = 'WEEKLY' THEN 'Week ending: ' || COALESCE(v_week_ending_date::text, '(unknown)') ELSE 'Date: ' || COALESCE((v_worked_start_iso AT TIME ZONE 'Europe/London')::date::text, '(unknown date)') END
    || '<br/>Timesheet ID: ' || v_current_timesheet_id::text || '</p>';

  SELECT p.presentation_model,p.snapshot_json,p.snapshot_hash,p.valid,p.error_code
    INTO v_snapshot_model,v_snapshot_json,v_snapshot_hash,
      v_snapshot_valid,v_snapshot_error_code
  FROM private._invoice_presentation_snapshot_batch(
    jsonb_build_array(jsonb_build_object(
      'request_key','timesheet-qr:'||v_current_timesheet_id::text,
      'entity_type','TIMESHEET',
      'entity_id',v_current_timesheet_id,
      'purpose','TIMESHEET',
      'template_version','timesheet-professional-v2')),
    v_now
  ) p
  LIMIT 1;

  IF NOT COALESCE(v_snapshot_valid,FALSE)
     OR v_snapshot_model->>'schema_version'<>'TIMESHEET_RENDER_MODEL_V2'
     OR v_snapshot_model->>'form_variant'<>'QR_UNSIGNED' THEN
    RETURN jsonb_build_object(
      'ok',FALSE,'queued',FALSE,
      'operation','timesheet_qr_send_enqueue',
      'error_code',coalesce(v_snapshot_error_code,
        'TIMESHEET_PRESENTATION_SNAPSHOT_INVALID'),
      'message','The official QR timesheet could not be frozen safely.',
      'current_timesheet_id',v_current_timesheet_id,
      'recipient_available',TRUE,'send_state','REJECTED');
  END IF;

  v_week_period_hash := coalesce(
    nullif(v_snapshot_model->>'week_period_hash',''),
    encode(extensions.digest(convert_to(coalesce(
      v_snapshot_model->'week_period','{}'::jsonb)::text,'UTF8'),'sha256'),'hex'));
  v_schedule_hash := encode(extensions.digest(convert_to(
    coalesce(v_snapshot_model#>'{week_period,days}','[]'::jsonb)::text,
    'UTF8'),'sha256'),'hex');
  v_reference_signature := coalesce(
    nullif(v_snapshot_model->>'reference_signature',''),
    encode(extensions.digest(convert_to(coalesce(v_snapshot_model#>'{week_period,days}',
      '[]'::jsonb)::text,'UTF8'),'sha256'),'hex'));
  v_additional_units_hash := coalesce(
    nullif(v_snapshot_model->>'additional_units_hash',''),
    encode(extensions.digest(convert_to(coalesce(v_snapshot_model#>'{additional_units_section,rows}',
      '[]'::jsonb)::text,'UTF8'),'sha256'),'hex'));
  v_presentation_settings_hash := coalesce(
    nullif(v_snapshot_model->>'presentation_settings_hash',''),
    encode(extensions.digest(convert_to(jsonb_build_object(
      'branding',v_snapshot_model->'branding',
      'wording',v_snapshot_model->'wording')::text,'UTF8'),'sha256'),'hex'));
  v_qr_payload_hash := encode(extensions.digest(
    convert_to(v_qr_payload_json::text,'UTF8'),'sha256'),'hex');
  v_complete_printable_content_hash := encode(extensions.digest(convert_to(
    concat_ws('|',v_snapshot_hash,v_week_period_hash,v_schedule_hash,
      v_reference_signature,v_additional_units_hash,
      v_presentation_settings_hash,v_qr_payload_hash,
      'timesheet-professional-v2'),'UTF8'),'sha256'),'hex');
  v_document_idempotency := encode(extensions.digest(convert_to(concat_ws('|',
    'BUILD_DOCUMENT','TIMESHEET',v_current_timesheet_id::text,'TIMESHEET',
    v_document_revision::text,'timesheet-professional-v2',
    v_qr_payload_hash,v_complete_printable_content_hash),'UTF8'),'sha256'),'hex');

  SELECT v.id,v.operation_id,v.status::text,v.r2_key
    INTO v_document_version_id,v_document_operation_id,
      v_document_version_status,v_pdf_key
  FROM public.invoice_document_versions v
  WHERE v.entity_type='TIMESHEET' AND v.entity_id=v_current_timesheet_id
    AND v.purpose='TIMESHEET'
    AND v.source_revision=v_document_revision::text
    AND v.template_version='timesheet-professional-v2'
    AND v.status IN(
      'PLANNING','WAITING_FOR_INPUTS','RENDERING',
      'ASSEMBLING','VERIFYING','READY')
  ORDER BY (v.status='READY') DESC,v.created_at_utc DESC,v.id DESC
  LIMIT 1;

  IF v_document_version_id IS NULL THEN
    INSERT INTO public.invoice_operations(
      operation_type,entity_type,entity_id,actor_user_id,idempotency_key,
      status,phase,priority,source_revision,template_version,input_json,
      config_json,progress_json,total_units,chunk_count,control_version,
      change_seq,created_at_utc,updated_at_utc
    ) VALUES(
      'BUILD_DOCUMENT','TIMESHEET',v_current_timesheet_id,p_actor_user_id,
      v_document_idempotency,'QUEUED','BUILD_MANIFEST',550,
      v_document_revision::text,'timesheet-professional-v2',
      jsonb_build_object(
        'entity_type','TIMESHEET','entity_id',v_current_timesheet_id,
        'purpose','TIMESHEET','source_revision',v_document_revision,
        'template_version','timesheet-professional-v2',
        'qr_payload_hash',v_qr_payload_hash,
        'printable_content_hash',v_complete_printable_content_hash),
      jsonb_build_object('processor_policy',private._invoice_processor_limits()),
      jsonb_build_object('status_message','Official QR timesheet queued'),
      1,1,1,nextval('public.invoice_operation_change_seq'),v_now,v_now
    )
    ON CONFLICT DO NOTHING
    RETURNING id INTO v_document_operation_id;

    IF v_document_operation_id IS NULL THEN
      SELECT o.id INTO v_document_operation_id
      FROM public.invoice_operations o
      WHERE o.idempotency_key=v_document_idempotency
        AND o.status IN('QUEUED','RUNNING','WAITING','RETRY_WAIT')
      ORDER BY o.created_at_utc DESC,o.id DESC
      LIMIT 1;
    END IF;

    INSERT INTO public.invoice_document_versions(
      entity_type,entity_id,purpose,operation_id,source_revision,
      template_version,status,snapshot_json,snapshot_hash,
      manifest_json,manifest_hash,created_at_utc
    ) VALUES(
      'TIMESHEET',v_current_timesheet_id,'TIMESHEET',
      v_document_operation_id,v_document_revision::text,
      'timesheet-professional-v2','PLANNING',
      v_snapshot_json,v_snapshot_hash,'[]'::jsonb,
      encode(extensions.digest(convert_to('[]','UTF8'),'sha256'),'hex'),v_now
    )
    ON CONFLICT(entity_type,entity_id,purpose,source_revision,template_version)
      WHERE purpose IN('DRAFT_PREVIEW','TIMESHEET')
        AND status IN(
          'PLANNING','WAITING_FOR_INPUTS','RENDERING',
          'ASSEMBLING','VERIFYING','READY')
    DO NOTHING
    RETURNING id,status::text,r2_key
      INTO v_document_version_id,v_document_version_status,v_pdf_key;

    IF v_document_version_id IS NULL THEN
      SELECT v.id,v.operation_id,v.status::text,v.r2_key
        INTO v_document_version_id,v_document_operation_id,
          v_document_version_status,v_pdf_key
      FROM public.invoice_document_versions v
      WHERE v.entity_type='TIMESHEET' AND v.entity_id=v_current_timesheet_id
        AND v.purpose='TIMESHEET'
        AND v.source_revision=v_document_revision::text
        AND v.template_version='timesheet-professional-v2'
        AND v.status IN(
          'PLANNING','WAITING_FOR_INPUTS','RENDERING',
          'ASSEMBLING','VERIFYING','READY')
      ORDER BY (v.status='READY') DESC,v.created_at_utc DESC,v.id DESC
      LIMIT 1;
    END IF;

    INSERT INTO public.invoice_operation_chunks(
      operation_id,chunk_type,phase,work_key,sequence_no,entity_type,entity_id,
      document_version_id,status,priority,run_after_utc,payload_json,
      operation_control_version,created_at_utc,updated_at_utc
    )
    SELECT v_document_operation_id,'DOCUMENT_PLAN','BUILD_MANIFEST',
      encode(extensions.digest(convert_to(concat_ws('|','DOCUMENT_PLAN',
        v_document_version_id::text,v_document_revision::text,
        'timesheet-professional-v2','1'),'UTF8'),'sha256'),'hex'),
      0,'TIMESHEET',v_current_timesheet_id,v_document_version_id,
      'QUEUED',550,v_now,
      jsonb_build_object(
        'purpose','TIMESHEET','source_revision',v_document_revision,
        'template_version','timesheet-professional-v2',
        'qr_payload_hash',v_qr_payload_hash,
        'printable_content_hash',v_complete_printable_content_hash),
      o.control_version,v_now,v_now
    FROM public.invoice_operations o
    WHERE o.id=v_document_operation_id
    ON CONFLICT(operation_id,chunk_type,level_no,sequence_no,work_key)
      DO NOTHING;
  END IF;

  UPDATE public.timesheets t
  SET current_document_version_id=v_document_version_id,
      active_document_operation_id=case
        when v_document_version_status='READY' then null
        else v_document_operation_id end,
      document_state=case
        when v_document_version_status='READY' then 'READY'
        else 'QUEUED' end,
      last_document_error_json=null,updated_at=v_now
  WHERE t.timesheet_id=v_current_timesheet_id AND t.is_current
    AND t.document_revision=v_document_revision;

  v_pdf_job_id := v_document_operation_id;
  v_mail_held_until_pdf_rendered :=
    v_candidate_paper_workflow_count = 1
    OR COALESCE(v_document_version_status,'')<>'READY';
  v_mail_hold_reason := CASE
    WHEN v_candidate_paper_workflow_count = 1 THEN 'CANDIDATE_PAPER_PACK_PENDING'
    WHEN v_mail_held_until_pdf_rendered THEN 'PDF_RENDER_PENDING'
    ELSE NULL
  END;
  v_mail_scheduled_for_utc := CASE
    WHEN v_mail_held_until_pdf_rendered THEN
      TIMESTAMPTZ '9999-12-31 00:00:00+00'
    ELSE v_now
  END;

  v_mail_scope_json := jsonb_build_object(
    'job_kind', 'TIMESHEET_QR_SEND',
    'document_operation_id',v_document_operation_id,
    'document_version_id',v_document_version_id,
    'document_revision',v_document_revision,
    'template_version','timesheet-professional-v2',
    'idempotency_key', v_idempotency_key,
    'client_idempotency_key', v_client_idempotency_key,
    'requires_pdf_render', TRUE,
    'release_mail_after_pdf_render', TRUE,
    'mail_delayed_for_pdf_render',v_mail_held_until_pdf_rendered,
    'mail_held_until_pdf_rendered',v_mail_held_until_pdf_rendered,
    'mail_hold_reason',v_mail_hold_reason,
    'pdf_storage_key', v_pdf_key,
    'current_timesheet_id', v_current_timesheet_id::text,
    'current_version', v_current_version,
    'qr_token_hash',encode(extensions.digest(
      convert_to(v_effective_qr_token,'UTF8'),'sha256'),'hex'),
    'qr_payload_hash',v_qr_payload_hash,
    'week_period_hash',v_week_period_hash,
    'schedule_hash',v_schedule_hash,
    'reference_signature',v_reference_signature,
    'additional_units_hash',v_additional_units_hash,
    'presentation_settings_hash',v_presentation_settings_hash,
    'printable_content_hash',v_complete_printable_content_hash,
    'recipient_email', v_candidate_email
  ) || v_candidate_paper_binding_json;

  SELECT mail_existing.id,
         mail_existing.status::text,
         COALESCE(mail_existing.payment_scope_json, '{}'::jsonb)
    INTO v_existing_mail_id,
         v_existing_mail_status,
         v_existing_mail_scope_json
  FROM public.mail_outbox AS mail_existing
  WHERE mail_existing.type = 'TIMESHEET_QR'
    AND mail_existing.reference = v_mail_reference
    AND mail_existing.context_kind = 'timesheets'
    AND mail_existing.context_id = v_current_timesheet_id
    AND mail_existing."to" = v_candidate_email
  ORDER BY mail_existing.created_at_utc DESC,
           mail_existing.id DESC
  LIMIT 1;

  IF v_existing_mail_id IS NOT NULL THEN
    v_mail_job_id := v_existing_mail_id;

    IF v_candidate_paper_workflow_count = 1
       AND NULLIF(BTRIM(COALESCE(v_existing_mail_scope_json->>'candidate_workflow_id', '')), '') IS NOT NULL
       AND (
         v_existing_mail_scope_json->>'candidate_workflow_id' IS DISTINCT FROM v_candidate_paper_workflow_id::text
         OR COALESCE(v_existing_mail_scope_json->>'candidate_workflow_generation', '') IS DISTINCT FROM v_candidate_paper_workflow_generation::text
         OR LOWER(COALESCE(v_existing_mail_scope_json->>'paper_return_manifest_sha256', ''))
              IS DISTINCT FROM v_candidate_paper_manifest_sha256
       ) THEN
      RETURN jsonb_build_object(
        'ok', FALSE,
        'queued', FALSE,
        'operation', 'timesheet_qr_send_enqueue',
        'error_code', 'CANDIDATE_PAPER_OUTBOX_IDENTITY_CONFLICT',
        'message', 'The existing QR email is bound to a different Candidate PAPER workflow.',
        'current_timesheet_id', v_current_timesheet_id,
        'mail_outbox_id', v_existing_mail_id,
        'recipient_available', FALSE,
        'send_state', 'REJECTED'
      );
    ELSIF UPPER(COALESCE(v_existing_mail_status, '')) = 'SENT' THEN
      v_send_state := 'ALREADY_SENT';
    ELSIF v_candidate_paper_workflow_count = 1
          AND UPPER(COALESCE(v_existing_mail_status, '')) = 'FAILED' THEN
      v_send_state := 'CANDIDATE_PAPER_MAIL_FAILED';
    ELSIF NULLIF(BTRIM(COALESCE(v_existing_mail_scope_json->>'candidate_workflow_id', '')), '') IS NOT NULL
          AND (
            LOWER(COALESCE(v_existing_mail_scope_json->>'candidate_paper_pack_ready', 'false'))
              IN ('true','t','1','yes')
            OR v_candidate_paper_workflow_count = 0
          ) THEN
      -- A Candidate complete pack (or a historical Candidate binding) must never
      -- be replaced by the ordinary one-page QR attachment on enqueue replay.
      v_send_state := 'CANDIDATE_PAPER_OUTBOX_PRESERVED';
      v_mail_held_until_pdf_rendered := LOWER(COALESCE(
        v_existing_mail_scope_json->>'mail_held_until_pdf_rendered', 'false'))
        IN ('true','t','1','yes');
      v_mail_hold_reason := NULLIF(BTRIM(COALESCE(
        v_existing_mail_scope_json->>'mail_hold_reason', '')), '');
    ELSE
      UPDATE public.mail_outbox AS mail_update
         SET status = 'QUEUED'::public.mail_status_enum,
             subject = v_mail_subject,
             body_html = v_mail_body_html,
             body_text = v_mail_body_text,
             attachments = CASE
               WHEN v_mail_held_until_pdf_rendered THEN '[]'::jsonb
               ELSE jsonb_build_array(jsonb_build_object(
                 'r2_key',v_pdf_key,
                 'filename','Timesheet_'||COALESCE(
                   v_week_ending_date::text,v_current_timesheet_id::text)||'.pdf'))
             END,
             last_error = NULL,
             failed_at = NULL,
             scheduled_for_utc = v_mail_scheduled_for_utc,
             next_attempt_at_utc = v_mail_scheduled_for_utc,
             provider_status = NULL,
             provider_message_id = NULL,
             attempt_lease_token = NULL,
             attempt_leased_at_utc = NULL,
             attempt_lease_expires_at_utc = NULL,
             payment_scope_json = v_mail_scope_json
       WHERE mail_update.id = v_existing_mail_id;

      v_send_state := CASE
        WHEN v_candidate_paper_workflow_count = 1 THEN 'CANDIDATE_PAPER_PACK_MAIL_HELD'
        WHEN NOT v_mail_held_until_pdf_rendered THEN 'DOCUMENT_READY_MAIL_QUEUED'
        WHEN UPPER(COALESCE(v_existing_mail_status,''))='FAILED'
          THEN 'DOCUMENT_REQUEUED_MAIL_HELD'
        ELSE 'ALREADY_QUEUED' END;
    END IF;
  ELSE
    INSERT INTO public.mail_outbox AS mail_insert (
      type,
      "to",
      cc,
      bcc,
      reply_to,
      importance,
      email_type,
      subject,
      body_html,
      body_text,
      attachments,
      status,
      last_error,
      created_at_utc,
      sent_at,
      created_by,
      reference,
      recipient_kind,
      recipient_id,
      context_kind,
      context_id,
      mailshot_run_id,
      document_template_id,
      provider_status,
      delivered_at,
      read_at,
      scheduled_for_utc,
      next_attempt_at_utc,
      payment_scope_json
    ) VALUES (
      'TIMESHEET_QR',
      v_candidate_email,
      NULL,
      NULL,
      NULL,
      'Normal',
      'html',
      v_mail_subject,
      v_mail_body_html,
      v_mail_body_text,
      CASE
        WHEN v_mail_held_until_pdf_rendered THEN '[]'::jsonb
        ELSE jsonb_build_array(jsonb_build_object(
          'r2_key',v_pdf_key,
          'filename','Timesheet_'||COALESCE(
            v_week_ending_date::text,v_current_timesheet_id::text)||'.pdf'))
      END,
      'QUEUED'::public.mail_status_enum,
      NULL,
      v_now,
      NULL,
      v_created_by_user_id,
      v_mail_reference,
      'candidate',
      v_candidate_id,
      'timesheets',
      v_current_timesheet_id,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      v_mail_scheduled_for_utc,
      v_mail_scheduled_for_utc,
      v_mail_scope_json
    )
    RETURNING id INTO v_mail_job_id;

    v_send_state := CASE
      WHEN v_candidate_paper_workflow_count = 1 THEN 'CANDIDATE_PAPER_PACK_MAIL_HELD'
      WHEN v_mail_held_until_pdf_rendered THEN 'DOCUMENT_QUEUED_MAIL_HELD'
      ELSE 'DOCUMENT_READY_MAIL_QUEUED' END;
  END IF;

  v_job_id := v_mail_job_id;

  SELECT decision_result.row_json
    INTO v_post_row
  FROM public.bulk_timesheet_row_decision_v1(jsonb_build_object(
    'dataset_mode', 'process',
    'timesheet_id', v_current_timesheet_id::text
  )) AS decision_result(row_json)
  LIMIT 1;

  v_row_key := NULLIF(BTRIM(COALESCE(v_post_row->>'row_key', '')), '');
  v_storage_key := NULLIF(BTRIM(COALESCE(v_post_row->>'primary_artifact_storage_key', '')), '');
  v_row_signature := NULLIF(BTRIM(COALESCE(v_post_row->>'row_signature', '')), '');

  INSERT INTO public.audit_events AS audit_insert (
    ts_utc,
    actor_user_id,
    object_type,
    object_id_text,
    action,
    before_json,
    after_json,
    reason
  ) VALUES (
    v_now,
    p_actor_user_id,
    'timesheets',
    v_current_timesheet_id::text,
    'TIMESHEET_QR_SEND_QUEUED',
    NULL,
    jsonb_build_object(
      'timesheet_id', v_current_timesheet_id,
      'job_id', v_job_id,
      'mail_outbox_id', v_mail_job_id,
      'pdf_job_id', v_pdf_job_id,
      'idempotency_key', v_idempotency_key,
      'send_state', v_send_state,
      'recipient_email', v_candidate_email,
      'scheduled_for_utc', v_mail_scheduled_for_utc,
      'mail_held_until_pdf_rendered', v_mail_held_until_pdf_rendered,
      'mail_delayed_for_pdf_render',v_mail_held_until_pdf_rendered,
      'mail_hold_reason',v_mail_hold_reason,
      'pdf_storage_key', v_pdf_key,
      'document_operation_id',v_document_operation_id,
      'document_version_id',v_document_version_id,
      'issue_type',v_issue_type,
      'client_idempotency_key', v_client_idempotency_key,
      'recipient_namespace', v_recipient_namespace
    ),
    'Bulk QR send enqueue'
  );

  RETURN jsonb_build_object(
    'ok', TRUE,
    'queued', TRUE,
    'operation', 'timesheet_qr_send_enqueue',
    'job_id', v_job_id,
    'mail_outbox_id', v_mail_job_id,
    'pdf_job_id', v_pdf_job_id,
    'idempotency_key', v_idempotency_key,
    'current_timesheet_id', v_current_timesheet_id,
    'timesheet_id', v_current_timesheet_id,
    'expected_timesheet_id', v_current_timesheet_id,
    'current_version', v_current_version,
    'recipient_available', TRUE,
    'recipient_email', v_candidate_email,
    'recipient_name', v_candidate_name,
    'send_state', v_send_state,
    'scheduled_for_utc', v_mail_scheduled_for_utc,
    'mail_held_until_pdf_rendered', v_mail_held_until_pdf_rendered,
    'mail_delayed_for_pdf_render',v_mail_held_until_pdf_rendered,
    'mail_hold_reason',v_mail_hold_reason,
    'candidate_paper_pack_ready',CASE
      WHEN NULLIF(BTRIM(COALESCE(v_existing_mail_scope_json->>'candidate_workflow_id', '')), '') IS NOT NULL
        THEN LOWER(COALESCE(v_existing_mail_scope_json->>'candidate_paper_pack_ready', 'false'))
          IN ('true','t','1','yes')
      WHEN v_candidate_paper_workflow_count = 1 THEN FALSE
      ELSE NULL
    END,
    'pdf_storage_key', v_pdf_key,
    'document_operation_id',v_document_operation_id,
    'document_version_id',v_document_version_id,
    'document_revision',v_document_revision,
    'template_version','timesheet-professional-v2',
    'issue_type',v_issue_type,
    'client_idempotency_key', v_client_idempotency_key,
    'recipient_namespace', v_recipient_namespace,
    'row_patch', COALESCE(v_post_row->'row_patch', jsonb_build_object()),
    'data_row', COALESCE(v_post_row, jsonb_build_object()),
    'row', COALESCE(v_post_row, jsonb_build_object()),
    'cache_invalidation_hints', jsonb_build_object(
      'row_keys', jsonb_build_array(COALESCE(v_row_key, 'timesheet:' || v_current_timesheet_id::text)),
      'timesheet_ids', jsonb_build_array(v_current_timesheet_id),
      'storage_keys', jsonb_build_array(v_storage_key, v_pdf_key),
      'datasets', jsonb_build_array('bulk_process', 'bulk_authorise'),
      'row_signature', v_row_signature,
      'invalidate_context', TRUE,
      'invalidate_preview', TRUE
    ),
    'cache_invalidation', jsonb_build_object(
      'rows', jsonb_build_array(jsonb_build_object(
        'row_key', COALESCE(v_row_key, 'timesheet:' || v_current_timesheet_id::text),
        'timesheet_id', v_current_timesheet_id,
        'new_row_signature', v_row_signature
      )),
      'artifacts', jsonb_build_array(jsonb_build_object(
        'timesheet_id', v_current_timesheet_id,
        'storage_key', COALESCE(v_storage_key, v_pdf_key),
        'pdf_storage_key', v_pdf_key,
        'changed', TRUE
      )),
      'datasets', jsonb_build_array('bulk_process', 'bulk_authorise')
    )
  );
END;
$function$;

notify pgrst, 'reload schema';
