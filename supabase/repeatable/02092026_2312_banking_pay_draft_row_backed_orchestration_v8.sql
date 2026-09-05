-- Certified Create Draft row-backed orchestration adapters.
-- Runtime authority is Miget TEST. The `supabase` directory name is historical only.
-- Transport/resumability only: no payment-policy or economic calculation lives here.

CREATE OR REPLACE FUNCTION private.pay_workbench_draft_scope_line_rows_v8(
  p_candidate_scope_id uuid,
  p_selected_lines jsonb,
  p_effective_lines jsonb
)
RETURNS TABLE(value jsonb, ordinality bigint)
LANGUAGE plpgsql
SECURITY INVOKER
STABLE
SET search_path TO ''
AS $function$
DECLARE
  v_public_scope public.banking_pay_operation_candidate_scope%ROWTYPE;
  v_frozen_scope private.banking_pay_draft_frozen_candidate_scopes_v8%ROWTYPE;
  v_expected_count integer;
  v_actual_count integer;
BEGIN
  IF p_candidate_scope_id IS NULL THEN
    RAISE EXCEPTION 'DRAFT_SCOPE_MEMBER_GAP'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'DRAFT_SCOPE_MEMBER_GAP',
        'candidate_scope_id', p_candidate_scope_id
      )::text;
  END IF;

  SELECT scope_row.*
  INTO v_public_scope
  FROM public.banking_pay_operation_candidate_scope AS scope_row
  WHERE scope_row.id = p_candidate_scope_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'DRAFT_SCOPE_MEMBER_GAP'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'DRAFT_SCOPE_MEMBER_GAP',
        'candidate_scope_id', p_candidate_scope_id
      )::text;
  END IF;

  SELECT frozen_scope.*
  INTO v_frozen_scope
  FROM private.banking_pay_draft_frozen_candidate_scopes_v8 AS frozen_scope
  JOIN private.banking_pay_draft_frozen_certificate_scopes_v8 AS certificate_scope
    ON certificate_scope.operation_id = frozen_scope.operation_id
   AND certificate_scope.freeze_state = 'FROZEN'
  WHERE frozen_scope.operation_id = v_public_scope.operation_id
    AND frozen_scope.candidate_id = v_public_scope.candidate_id
    AND frozen_scope.resolved_pay_channel = v_public_scope.pay_channel
    AND frozen_scope.scope_digest_sha256 = v_public_scope.scope_hash
    AND frozen_scope.scope_state IN ('FROZEN', 'BATCH_LINKED', 'COMPLETE');

  IF FOUND THEN
    IF v_public_scope.selected_preview_row_ids_json <> '[]'::jsonb
       OR v_public_scope.selected_timesheet_ids_json <> '[]'::jsonb
       OR v_public_scope.selected_finance_case_ids_json <> '[]'::jsonb
       OR v_public_scope.selected_canonical_preview_lines_json <> '[]'::jsonb
       OR v_public_scope.effective_canonical_preview_lines_json <> '[]'::jsonb THEN
      RAISE EXCEPTION 'DRAFT_SCOPE_ALREADY_CONFLICTING'
        USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
          'code', 'DRAFT_SCOPE_ALREADY_CONFLICTING',
          'candidate_scope_id', p_candidate_scope_id,
          'reason', 'ROW_BACKED_SCOPE_CONTAINS_LEGACY_ARRAYS'
        )::text;
    END IF;

    v_expected_count := v_frozen_scope.constituent_count;
    SELECT pg_catalog.count(*)::integer
    INTO v_actual_count
    FROM private.banking_pay_draft_frozen_candidate_scope_members_v8 AS member
    JOIN private.banking_pay_draft_frozen_constituent_payloads_v8 AS payload
      ON payload.operation_id = member.operation_id
     AND payload.constituent_ordinal = member.constituent_ordinal
     AND payload.certificate_uuid = member.certificate_uuid
     AND payload.candidate_id = v_frozen_scope.candidate_id
     AND payload.resolved_pay_channel = v_frozen_scope.resolved_pay_channel
    WHERE member.operation_id = v_frozen_scope.operation_id
      AND member.candidate_scope_ordinal = v_frozen_scope.candidate_scope_ordinal;

    IF v_actual_count IS DISTINCT FROM v_expected_count THEN
      RAISE EXCEPTION 'DRAFT_SCOPE_MEMBER_GAP'
        USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
          'code', 'DRAFT_SCOPE_MEMBER_GAP',
          'candidate_scope_id', p_candidate_scope_id,
          'expected_count', v_expected_count,
          'actual_count', v_actual_count
        )::text;
    END IF;

    RETURN QUERY
    SELECT payload.payload_json,
      (member.member_ordinal + 1)::bigint
    FROM private.banking_pay_draft_frozen_candidate_scope_members_v8 AS member
    JOIN private.banking_pay_draft_frozen_constituent_payloads_v8 AS payload
      ON payload.operation_id = member.operation_id
     AND payload.constituent_ordinal = member.constituent_ordinal
     AND payload.certificate_uuid = member.certificate_uuid
     AND payload.candidate_id = v_frozen_scope.candidate_id
     AND payload.resolved_pay_channel = v_frozen_scope.resolved_pay_channel
    WHERE member.operation_id = v_frozen_scope.operation_id
      AND member.candidate_scope_ordinal = v_frozen_scope.candidate_scope_ordinal
    ORDER BY member.member_ordinal;
    RETURN;
  END IF;

  RETURN QUERY
  SELECT line.value, line.ordinality
  FROM pg_catalog.jsonb_array_elements(
    CASE
      WHEN pg_catalog.jsonb_typeof(p_selected_lines) = 'array'
        AND pg_catalog.jsonb_array_length(p_selected_lines) > 0
        THEN p_selected_lines
      WHEN pg_catalog.jsonb_typeof(p_effective_lines) = 'array'
        AND pg_catalog.jsonb_array_length(p_effective_lines) > 0
        THEN p_effective_lines
      ELSE '[]'::jsonb
    END
  ) WITH ORDINALITY AS line(value, ordinality);
END;
$function$;

ALTER FUNCTION private.pay_workbench_draft_scope_line_rows_v8(uuid,jsonb,jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_draft_scope_line_rows_v8(uuid,jsonb,jsonb) FROM PUBLIC, anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION public.pay_workbench_settled_certificate_reference_validate_v8(
  p_operation_id uuid,
  p_expected_certification_id text,
  p_expected_overall_digest_sha256 text,
  p_validation_stage text
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
VOLATILE
SET search_path TO ''
AS $function$
DECLARE
  v_stage text := upper(btrim(COALESCE(p_validation_stage, '')));
  v_operation public.banking_pay_operations%ROWTYPE;
  v_link private.banking_pay_workbench_settled_certificate_operation_links_v8%ROWTYPE;
  v_certificate private.banking_pay_workbench_settled_certificates_v8%ROWTYPE;
  v_manifest private.banking_pay_workbench_settled_certificate_channel_manifests_v8%ROWTYPE;
  v_scope private.banking_pay_draft_frozen_certificate_scopes_v8%ROWTYPE;
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_error_code text;
BEGIN
  PERFORM public.banking_pay_hot_path_budget_apply('WORKBENCH_CHUNK');

  IF p_operation_id IS NULL
     OR NULLIF(btrim(COALESCE(p_expected_certification_id, '')), '') IS NULL
     OR COALESCE(p_expected_overall_digest_sha256, '') !~ '^[0-9a-f]{64}$'
     OR v_stage NOT IN ('PAGE', 'FINAL_FREEZE') THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_IDENTITY_MISMATCH'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'WORKBENCH_CERTIFICATE_IDENTITY_MISMATCH',
        'operation_id', p_operation_id,
        'validation_stage', NULLIF(v_stage, '')
      )::text;
  END IF;

  SELECT operation_row.*
  INTO v_operation
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.id = p_operation_id
  FOR UPDATE;

  IF NOT FOUND
     OR upper(btrim(COALESCE(v_operation.operation_type, ''))) <> 'DRAFT_CREATE'
     OR upper(btrim(COALESCE(v_operation.status, ''))) IN ('COMPLETE', 'FAILED', 'CANCELLED', 'REVIEW_REQUIRED') THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_NOT_FOUND'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'WORKBENCH_CERTIFICATE_NOT_FOUND', 'operation_id', p_operation_id
      )::text;
  END IF;

  SELECT link_row.*
  INTO v_link
  FROM private.banking_pay_workbench_settled_certificate_operation_links_v8 AS link_row
  WHERE link_row.operation_id = p_operation_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_NOT_FOUND'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'WORKBENCH_CERTIFICATE_NOT_FOUND', 'operation_id', p_operation_id
      )::text;
  END IF;

  IF v_link.certification_id IS DISTINCT FROM p_expected_certification_id
     OR v_link.overall_digest_sha256 IS DISTINCT FROM p_expected_overall_digest_sha256
     OR v_link.certification_id IS DISTINCT FROM
       ('WORKBENCH_SETTLED_CERTIFICATION_V2:' || v_link.overall_digest_sha256) THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_IDENTITY_MISMATCH'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'WORKBENCH_CERTIFICATE_IDENTITY_MISMATCH', 'operation_id', p_operation_id
      )::text;
  END IF;

  SELECT certificate_row.*
  INTO v_certificate
  FROM private.banking_pay_workbench_settled_certificates_v8 AS certificate_row
  WHERE certificate_row.certificate_uuid = v_link.certificate_uuid;

  IF NOT FOUND
     OR v_certificate.certification_id IS DISTINCT FROM v_link.certification_id
     OR v_certificate.overall_digest_sha256 IS DISTINCT FROM v_link.overall_digest_sha256 THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_TAMPERED'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'WORKBENCH_CERTIFICATE_TAMPERED', 'operation_id', p_operation_id
      )::text;
  END IF;

  IF v_certificate.lifecycle <> 'SEALED_CURRENT' THEN
    v_error_code := CASE
      WHEN v_certificate.lifecycle = 'REVOKED_CORRUPT_OR_SECURITY' THEN 'WORKBENCH_CERTIFICATE_REVOKED'
      ELSE 'WORKBENCH_CERTIFICATE_NOT_CURRENT'
    END;
    RAISE EXCEPTION '%', v_error_code USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
      'code', v_error_code,
      'operation_id', p_operation_id,
      'lifecycle', v_certificate.lifecycle
    )::text;
  END IF;

  SELECT manifest_row.*
  INTO v_manifest
  FROM private.banking_pay_workbench_settled_certificate_channel_manifests_v8 AS manifest_row
  WHERE manifest_row.certificate_uuid = v_link.certificate_uuid
    AND manifest_row.pay_channel_scope = v_link.pay_channel_scope;

  SELECT scope_row.*
  INTO v_scope
  FROM private.banking_pay_draft_frozen_certificate_scopes_v8 AS scope_row
  WHERE scope_row.operation_id = p_operation_id
  FOR UPDATE;

  IF NOT FOUND
     OR v_manifest.certificate_uuid IS NULL
     OR v_scope.certificate_uuid IS DISTINCT FROM v_link.certificate_uuid
     OR v_scope.pay_channel_scope IS DISTINCT FROM v_link.pay_channel_scope
     OR v_scope.constituent_count IS DISTINCT FROM v_manifest.constituent_count
     OR v_scope.partition_count IS DISTINCT FROM v_manifest.partition_count
     OR v_scope.canonical_amount_ex_vat_total IS DISTINCT FROM v_manifest.canonical_amount_ex_vat_total
     OR v_scope.selected_constituents_digest_sha256 IS DISTINCT FROM v_manifest.selected_constituents_digest_sha256
     OR v_scope.selected_partitions_digest_sha256 IS DISTINCT FROM v_manifest.selected_partitions_digest_sha256
     OR v_scope.manifest_digest_sha256 IS DISTINCT FROM v_manifest.manifest_digest_sha256 THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_MANIFEST_MISMATCH'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'WORKBENCH_CERTIFICATE_MANIFEST_MISMATCH',
        'operation_id', p_operation_id,
        'pay_channel_scope', v_link.pay_channel_scope
      )::text;
  END IF;

  IF (v_stage = 'PAGE' AND v_scope.freeze_state <> 'STAGING')
     OR (v_stage = 'FINAL_FREEZE' AND v_scope.freeze_state NOT IN ('STAGING', 'FROZEN')) THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_STAGE_ALREADY_FROZEN'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'WORKBENCH_CERTIFICATE_STAGE_ALREADY_FROZEN',
        'operation_id', p_operation_id,
        'freeze_state', v_scope.freeze_state,
        'validation_stage', v_stage
      )::text;
  END IF;

  -- The exact session row is the sole mutation fence. No Candidate or publication
  -- row is locked, so a 50,000-constituent Draft never takes 50,000 locks.
  SELECT session_row.*
  INTO v_session
  FROM public.banking_pay_workbench_sessions AS session_row
  WHERE session_row.id = v_certificate.workbench_session_id
  FOR UPDATE;

  IF NOT FOUND
     OR v_session.discarded_at_utc IS NOT NULL
     OR v_session.replacement_session_id IS NOT NULL
     OR upper(btrim(COALESCE(v_session.status, ''))) <> 'OPEN'
     OR upper(btrim(COALESCE(v_session.progress_state, ''))) <> 'READY'
     OR v_session.version IS DISTINCT FROM v_certificate.session_version
     OR v_session.progress_counter_version IS DISTINCT FROM v_certificate.progress_counter_version
     OR v_session.source_snapshot_run_id IS DISTINCT FROM v_certificate.source_snapshot_run_id
     OR v_session.session_signature IS DISTINCT FROM v_certificate.session_signature
     OR v_session.scope_change_generation_target IS DISTINCT FROM v_certificate.scope_change_generation_target
     OR v_session.scope_change_generation_applied IS DISTINCT FROM v_certificate.scope_change_generation_applied
     OR v_session.scope_change_generation_shadow_checked IS DISTINCT FROM v_certificate.scope_change_generation_shadow_checked
     OR v_session.authority_fence_generation IS DISTINCT FROM v_certificate.authority_fence_generation THEN
    v_error_code := CASE
      WHEN v_stage = 'FINAL_FREEZE' THEN 'WORKBENCH_CERTIFICATE_FINAL_FREEZE_STALE'
      ELSE 'WORKBENCH_CERTIFICATE_SESSION_SUPERSEDED'
    END;
    RAISE EXCEPTION '%', v_error_code USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
      'code', v_error_code,
      'operation_id', p_operation_id,
      'validation_stage', v_stage
    )::text;
  END IF;

  RETURN pg_catalog.jsonb_build_object(
    'ok', true,
    'operation_id', p_operation_id,
    'certificate_uuid', v_certificate.certificate_uuid,
    'certification_id', v_certificate.certification_id,
    'overall_digest_sha256', v_certificate.overall_digest_sha256,
    'pay_channel_scope', v_link.pay_channel_scope,
    'manifest_digest_sha256', v_manifest.manifest_digest_sha256,
    'constituent_count', v_manifest.constituent_count,
    'partition_count', v_manifest.partition_count,
    'canonical_amount_ex_vat_total', v_manifest.canonical_amount_ex_vat_total,
    'validation_stage', v_stage,
    'session_lock_held', true,
    'candidate_locks_taken', 0
  );
END;
$function$;

ALTER FUNCTION public.pay_workbench_settled_certificate_reference_validate_v8(uuid,text,text,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_settled_certificate_reference_validate_v8(uuid,text,text,text) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.pay_workbench_settled_certificate_reference_validate_v8(uuid,text,text,text) TO service_role;

CREATE OR REPLACE FUNCTION public.banking_pay_draft_phase_units_seed_v8(
  p_operation_id uuid,
  p_after_candidate_scope_ordinal integer DEFAULT NULL,
  p_limit integer DEFAULT 256,
  p_expected_previous_receipt_sha256 text DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
VOLATILE
SET search_path TO ''
AS $function$
DECLARE
  v_operation public.banking_pay_operations%ROWTYPE;
  v_scope private.banking_pay_draft_frozen_certificate_scopes_v8%ROWTYPE;
  v_existing private.banking_pay_draft_frozen_stage_receipts_v8%ROWTYPE;
  v_previous private.banking_pay_draft_frozen_stage_receipts_v8%ROWTYPE;
  v_limit integer := COALESCE(p_limit, 256);
  v_phase text;
  v_stage_kind text;
  v_page_sequence integer;
  v_row_count integer;
  v_has_more boolean;
  v_next_after integer;
  v_canonical_bytes integer;
  v_request_text text;
  v_request_digest text;
  v_receipt_text text;
  v_receipt_digest text;
  v_new_unit_count integer;
  v_total_unit_count integer;
  v_conflict_count integer;
BEGIN
  PERFORM public.banking_pay_hot_path_budget_apply('WORKBENCH_CHUNK');

  IF p_operation_id IS NULL THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_NOT_FOUND' USING ERRCODE = 'P0001';
  END IF;
  IF v_limit < 1 OR v_limit > 256 THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_PAGE_LIMIT_INVALID'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'WORKBENCH_CERTIFICATE_PAGE_LIMIT_INVALID', 'limit', v_limit
      )::text;
  END IF;
  IF p_after_candidate_scope_ordinal IS NOT NULL AND p_after_candidate_scope_ordinal < 0 THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_PAGE_CURSOR_INVALID' USING ERRCODE = 'P0001';
  END IF;
  IF p_expected_previous_receipt_sha256 IS NOT NULL
     AND p_expected_previous_receipt_sha256 !~ '^[0-9a-f]{64}$' THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_PAGE_RECEIPT_MISMATCH' USING ERRCODE = 'P0001';
  END IF;

  SELECT operation_row.*
  INTO v_operation
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.id = p_operation_id
  FOR UPDATE;

  IF NOT FOUND
     OR upper(btrim(COALESCE(v_operation.operation_type, ''))) <> 'DRAFT_CREATE'
     OR upper(btrim(COALESCE(v_operation.status, ''))) IN ('COMPLETE', 'FAILED', 'CANCELLED', 'REVIEW_REQUIRED') THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_NOT_FOUND' USING ERRCODE = 'P0001';
  END IF;

  v_phase := upper(btrim(COALESCE(v_operation.phase, '')));
  IF v_phase NOT IN (
    'SEED_ALLOCATION_ROWS', 'INSERT_CANDIDATES', 'INSERT_ITEMS',
    'APPLY_FINANCE_ADJUSTMENTS', 'FINALISE_RESERVATIONS',
    'POPULATE_CANDIDATE_SUMMARIES', 'CREATE_TIMESHEET_SNAPSHOTS',
    'BUILD_ITEM_BREAKDOWNS'
  ) THEN
    RAISE EXCEPTION 'DRAFT_CREATE_PHASE_UNITS_PHASE_INVALID'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'DRAFT_CREATE_PHASE_UNITS_PHASE_INVALID',
        'operation_id', p_operation_id, 'phase', v_phase
      )::text;
  END IF;
  v_stage_kind := 'PHASE_UNITS:' || v_phase;

  SELECT scope_row.*
  INTO v_scope
  FROM private.banking_pay_draft_frozen_certificate_scopes_v8 AS scope_row
  WHERE scope_row.operation_id = p_operation_id
  FOR SHARE;

  IF NOT FOUND OR v_scope.freeze_state <> 'FROZEN' THEN
    RAISE EXCEPTION 'DRAFT_PARITY_NOT_FROZEN'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'DRAFT_PARITY_NOT_FROZEN', 'operation_id', p_operation_id
      )::text;
  END IF;

  -- Flat JCS/stableStringify preimage: keys are emitted in lexical order and
  -- nulls remain explicit. No complete constituent or Candidate array is built.
  v_request_text := '{'
    || '"after_ordinal":' || COALESCE(p_after_candidate_scope_ordinal::text, 'null') || ','
    || '"expected_previous_receipt_sha256":' || COALESCE(pg_catalog.to_jsonb(p_expected_previous_receipt_sha256)::text, 'null') || ','
    || '"operation_id":' || pg_catalog.to_jsonb(p_operation_id::text)::text || ','
    || '"pay_channel_scope":' || pg_catalog.to_jsonb(v_scope.pay_channel_scope)::text || ','
    || '"requested_limit":' || v_limit::text || ','
    || '"stage_kind":' || pg_catalog.to_jsonb(v_stage_kind)::text
    || '}';
  v_request_digest := pg_catalog.encode(
    extensions.digest(pg_catalog.convert_to(v_request_text, 'UTF8'), 'sha256'), 'hex'
  );

  SELECT receipt_row.*
  INTO v_existing
  FROM private.banking_pay_draft_frozen_stage_receipts_v8 AS receipt_row
  WHERE receipt_row.operation_id = p_operation_id
    AND receipt_row.stage_kind = v_stage_kind
    AND receipt_row.after_ordinal IS NOT DISTINCT FROM p_after_candidate_scope_ordinal;

  IF FOUND THEN
    IF v_existing.requested_limit IS DISTINCT FROM v_limit
       OR v_existing.expected_previous_receipt_sha256 IS DISTINCT FROM p_expected_previous_receipt_sha256
       OR v_existing.request_preimage_digest_sha256 IS DISTINCT FROM v_request_digest THEN
      RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_PAGE_REQUEST_CONFLICT'
        USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
          'code', 'WORKBENCH_CERTIFICATE_PAGE_REQUEST_CONFLICT',
          'operation_id', p_operation_id, 'stage_kind', v_stage_kind
        )::text;
    END IF;
    SELECT count(*)::integer
    INTO v_total_unit_count
    FROM private.banking_pay_draft_phase_units_v1 AS unit_row
    WHERE unit_row.operation_id = p_operation_id
      AND unit_row.phase = v_phase;
    RETURN pg_catalog.jsonb_build_object(
      'ok', true, 'operation_id', p_operation_id, 'stage_kind', v_stage_kind,
      'page_sequence', v_existing.page_sequence, 'after_ordinal', v_existing.after_ordinal,
      'row_count', v_existing.row_count, 'canonical_byte_count', v_existing.canonical_byte_count,
      'page_receipt_digest_sha256', v_existing.receipt_digest_sha256,
      'next_after_ordinal', v_existing.next_after_ordinal, 'has_more', v_existing.has_more,
      'terminal_sentinel_present', v_existing.terminal_sentinel_present,
      'stage_total_count', v_scope.partition_count,
      'stage_total_digest_sha256', v_scope.selected_partitions_digest_sha256,
      'phase_unit_count', v_total_unit_count, 'replayed', true
    );
  END IF;

  IF p_after_candidate_scope_ordinal IS NULL THEN
    IF p_expected_previous_receipt_sha256 IS NOT NULL THEN
      RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_PAGE_RECEIPT_MISMATCH' USING ERRCODE = 'P0001';
    END IF;
    v_page_sequence := 0;
  ELSE
    SELECT receipt_row.*
    INTO v_previous
    FROM private.banking_pay_draft_frozen_stage_receipts_v8 AS receipt_row
    WHERE receipt_row.operation_id = p_operation_id
      AND receipt_row.stage_kind = v_stage_kind
      AND receipt_row.receipt_digest_sha256 = p_expected_previous_receipt_sha256
      AND receipt_row.next_after_ordinal = p_after_candidate_scope_ordinal;
    IF NOT FOUND OR NOT v_previous.has_more THEN
      RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_PAGE_RECEIPT_MISMATCH'
        USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
          'code', 'WORKBENCH_CERTIFICATE_PAGE_RECEIPT_MISMATCH',
          'operation_id', p_operation_id, 'stage_kind', v_stage_kind
        )::text;
    END IF;
    v_page_sequence := v_previous.page_sequence + 1;
  END IF;

  CREATE TEMPORARY TABLE IF NOT EXISTS pg_temp.h2_v8_phase_scope_page (
    candidate_scope_ordinal integer PRIMARY KEY,
    certificate_uuid uuid NOT NULL,
    candidate_id uuid NOT NULL,
    resolved_pay_channel text NOT NULL,
    scope_digest_sha256 text NOT NULL,
    row_number_in_page integer NOT NULL
  ) ON COMMIT DROP;
  TRUNCATE TABLE pg_temp.h2_v8_phase_scope_page;

  INSERT INTO pg_temp.h2_v8_phase_scope_page(
    candidate_scope_ordinal, certificate_uuid, candidate_id,
    resolved_pay_channel, scope_digest_sha256, row_number_in_page
  )
  SELECT candidate_scope.candidate_scope_ordinal,
    candidate_scope.certificate_uuid,
    candidate_scope.candidate_id,
    candidate_scope.resolved_pay_channel,
    candidate_scope.scope_digest_sha256,
    row_number() OVER (ORDER BY candidate_scope.candidate_scope_ordinal)::integer
  FROM private.banking_pay_draft_frozen_candidate_scopes_v8 AS candidate_scope
  WHERE candidate_scope.operation_id = p_operation_id
    AND candidate_scope.scope_state IN ('FROZEN', 'BATCH_LINKED', 'COMPLETE')
    AND candidate_scope.candidate_scope_ordinal > COALESCE(p_after_candidate_scope_ordinal, -1)
  ORDER BY candidate_scope.candidate_scope_ordinal
  LIMIT (v_limit + 1);

  SELECT LEAST(count(*)::integer, v_limit), count(*) > v_limit
  INTO v_row_count, v_has_more
  FROM pg_temp.h2_v8_phase_scope_page;

  SELECT max(page_row.candidate_scope_ordinal)
  INTO v_next_after
  FROM pg_temp.h2_v8_phase_scope_page AS page_row
  WHERE page_row.row_number_in_page <= v_limit;

  SELECT COALESCE(sum(pg_catalog.octet_length(
    pg_catalog.jsonb_build_object(
      'candidate_scope_ordinal', page_row.candidate_scope_ordinal,
      'certificate_uuid', page_row.certificate_uuid,
      'candidate_id', page_row.candidate_id,
      'resolved_pay_channel', page_row.resolved_pay_channel,
      'scope_digest_sha256', page_row.scope_digest_sha256
    )::text
  )), 0)::integer
  INTO v_canonical_bytes
  FROM pg_temp.h2_v8_phase_scope_page AS page_row
  WHERE page_row.row_number_in_page <= v_limit;

  IF v_canonical_bytes > 524288 THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_PAGE_BYTES_EXCEEDED'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'WORKBENCH_CERTIFICATE_PAGE_BYTES_EXCEEDED',
        'operation_id', p_operation_id, 'stage_kind', v_stage_kind,
        'canonical_byte_count', v_canonical_bytes, 'limit', 524288
      )::text;
  END IF;

  INSERT INTO private.banking_pay_draft_phase_units_v1(
    operation_id, phase, candidate_scope_ordinal, unit_state,
    next_owner_iteration, last_owner_receipt_sha256, completed_at_utc
  )
  SELECT p_operation_id, v_phase, page_row.candidate_scope_ordinal,
    'PENDING', 0, NULL, NULL
  FROM pg_temp.h2_v8_phase_scope_page AS page_row
  WHERE page_row.row_number_in_page <= v_limit
  ON CONFLICT (operation_id, phase, candidate_scope_ordinal) DO NOTHING;
  GET DIAGNOSTICS v_new_unit_count = ROW_COUNT;

  SELECT count(*)::integer
  INTO v_conflict_count
  FROM pg_temp.h2_v8_phase_scope_page AS page_row
  LEFT JOIN private.banking_pay_draft_phase_units_v1 AS unit_row
    ON unit_row.operation_id = p_operation_id
   AND unit_row.phase = v_phase
   AND unit_row.candidate_scope_ordinal = page_row.candidate_scope_ordinal
  WHERE page_row.row_number_in_page <= v_limit
    AND (
      unit_row.operation_id IS NULL
      OR unit_row.unit_state NOT IN ('PENDING', 'RUNNING', 'WAITING_CONTINUATION', 'COMPLETE')
    );
  IF v_conflict_count <> 0 THEN
    RAISE EXCEPTION 'DRAFT_SCOPE_DIGEST_MISMATCH'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'DRAFT_SCOPE_DIGEST_MISMATCH', 'operation_id', p_operation_id,
        'phase', v_phase, 'conflict_count', v_conflict_count
      )::text;
  END IF;

  v_receipt_text := '{'
    || '"after_ordinal":' || COALESCE(p_after_candidate_scope_ordinal::text, 'null') || ','
    || '"canonical_byte_count":' || v_canonical_bytes::text || ','
    || '"expected_previous_receipt_sha256":' || COALESCE(pg_catalog.to_jsonb(p_expected_previous_receipt_sha256)::text, 'null') || ','
    || '"has_more":' || CASE WHEN v_has_more THEN 'true' ELSE 'false' END || ','
    || '"next_after_ordinal":' || COALESCE(v_next_after::text, 'null') || ','
    || '"operation_id":' || pg_catalog.to_jsonb(p_operation_id::text)::text || ','
    || '"page_sequence":' || v_page_sequence::text || ','
    || '"request_preimage_digest_sha256":' || pg_catalog.to_jsonb(v_request_digest)::text || ','
    || '"row_count":' || v_row_count::text || ','
    || '"stage_kind":' || pg_catalog.to_jsonb(v_stage_kind)::text || ','
    || '"stage_total_count":' || v_scope.partition_count::text || ','
    || '"stage_total_digest_sha256":' || pg_catalog.to_jsonb(v_scope.selected_partitions_digest_sha256)::text || ','
    || '"terminal_sentinel_present":true'
    || '}';
  v_receipt_digest := pg_catalog.encode(
    extensions.digest(pg_catalog.convert_to(v_receipt_text, 'UTF8'), 'sha256'), 'hex'
  );

  INSERT INTO private.banking_pay_draft_frozen_stage_receipts_v8(
    operation_id, stage_kind, page_sequence, after_ordinal, requested_limit,
    expected_previous_receipt_sha256, request_preimage_digest_sha256,
    row_count, canonical_byte_count, next_after_ordinal, has_more,
    terminal_sentinel_present, receipt_digest_sha256, stage_status
  ) VALUES (
    p_operation_id, v_stage_kind, v_page_sequence,
    p_after_candidate_scope_ordinal, v_limit,
    p_expected_previous_receipt_sha256, v_request_digest,
    v_row_count, v_canonical_bytes, v_next_after, v_has_more,
    true, v_receipt_digest, CASE WHEN v_has_more THEN 'COMMITTED' ELSE 'TERMINAL' END
  );

  SELECT count(*)::integer
  INTO v_total_unit_count
  FROM private.banking_pay_draft_phase_units_v1 AS unit_row
  WHERE unit_row.operation_id = p_operation_id
    AND unit_row.phase = v_phase;

  IF NOT v_has_more AND v_total_unit_count <> v_scope.partition_count THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_STAGE_INCOMPLETE'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'WORKBENCH_CERTIFICATE_STAGE_INCOMPLETE',
        'operation_id', p_operation_id, 'phase', v_phase,
        'expected_count', v_scope.partition_count, 'actual_count', v_total_unit_count
      )::text;
  END IF;

  RETURN pg_catalog.jsonb_build_object(
    'ok', true, 'operation_id', p_operation_id, 'stage_kind', v_stage_kind,
    'page_sequence', v_page_sequence, 'after_ordinal', p_after_candidate_scope_ordinal,
    'row_count', v_row_count, 'canonical_byte_count', v_canonical_bytes,
    'page_receipt_digest_sha256', v_receipt_digest,
    'next_after_ordinal', v_next_after, 'has_more', v_has_more,
    'terminal_sentinel_present', true,
    'stage_total_count', v_scope.partition_count,
    'stage_total_digest_sha256', v_scope.selected_partitions_digest_sha256,
    'new_phase_unit_count', v_new_unit_count,
    'phase_unit_count', v_total_unit_count,
    'financial_owner_chunk_limit', 100,
    'replayed', false
  );
END;
$function$;

ALTER FUNCTION public.banking_pay_draft_phase_units_seed_v8(uuid,integer,integer,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.banking_pay_draft_phase_units_seed_v8(uuid,integer,integer,text) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.banking_pay_draft_phase_units_seed_v8(uuid,integer,integer,text) TO service_role;

-- Exact current-owner replacement: only the selected-line transport gains the
-- certified V8 row-backed branch. All allocation/recovery/headroom equations,
-- guards and legacy-array behaviour below remain byte-equivalent.
CREATE OR REPLACE FUNCTION public.pay_workbench_prepare_draft_allocation_rows_seed(p_operation_id uuid, p_candidate_scope_ids jsonb DEFAULT NULL::jsonb)
 RETURNS TABLE(candidate_scopes_processed integer, allocation_rows_inserted integer, allocation_rows_reused integer, failures integer)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_operation public.banking_pay_operations%ROWTYPE;
  v_scope_ids jsonb := COALESCE(p_candidate_scope_ids, '[]'::jsonb);
  v_scope_id_count integer := 0;
  v_candidate_scopes_processed integer := 0;
  v_expected_count integer := 0;
  v_inserted integer := 0;
  v_reused integer := 0;
  v_malformed_allocation_preview_rows jsonb := '[]'::jsonb;
  v_synthetic_total_allocation_preview_rows jsonb := '[]'::jsonb;
  v_semantic_draft_guard_enabled boolean := false;
  v_source_publication_identity_enforce_enabled boolean := false;
  v_semantic_draft_failures jsonb := '[]'::jsonb;
  v_semantic_source_failure_count integer := 0;
  v_plan_scope_ids jsonb := '[]'::jsonb;
  v_plan_drift_details jsonb := '[]'::jsonb;
BEGIN
  PERFORM public.banking_pay_hot_path_budget_apply('WORKBENCH_CHUNK');

  IF p_operation_id IS NULL THEN
    RAISE EXCEPTION 'pay_workbench_prepare_draft_allocation_rows_seed: p_operation_id is required';
  END IF;

  IF p_candidate_scope_ids IS NOT NULL AND jsonb_typeof(p_candidate_scope_ids) <> 'array' THEN
    RAISE EXCEPTION 'pay_workbench_prepare_draft_allocation_rows_seed requires p_candidate_scope_ids to be null or a JSON array';
  END IF;

  IF p_candidate_scope_ids IS NOT NULL THEN
    v_scope_id_count := jsonb_array_length(v_scope_ids);
    IF v_scope_id_count = 0 THEN
      RAISE EXCEPTION 'DRAFT_ALLOCATION_SCOPE_EMPTY'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object('code', 'DRAFT_ALLOCATION_SCOPE_EMPTY', 'operation_id', p_operation_id::text, 'message', 'DRAFT_CREATE allocation seeding requires at least one candidate scope id.')::text;
    END IF;
    IF v_scope_id_count > 100 THEN
      RAISE EXCEPTION 'pay_workbench_prepare_draft_allocation_rows_seed candidate scope id array exceeds the 100 row cap: %', v_scope_id_count;
    END IF;
    IF EXISTS (
      SELECT 1
      FROM jsonb_array_elements(v_scope_ids) AS supplied_scope(scope_value)
      WHERE NOT ((supplied_scope.scope_value #>> '{}') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
    ) THEN
      RAISE EXCEPTION 'pay_workbench_prepare_draft_allocation_rows_seed requires candidate scope ids to be UUID strings';
    END IF;
  END IF;

  SELECT operation_row.*
  INTO v_operation
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.id = p_operation_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'pay_workbench_prepare_draft_allocation_rows_seed operation not found: %', p_operation_id;
  END IF;

  IF UPPER(BTRIM(COALESCE(v_operation.operation_type, ''))) <> 'DRAFT_CREATE' THEN
    RAISE EXCEPTION 'pay_workbench_prepare_draft_allocation_rows_seed expected DRAFT_CREATE operation %, got %', p_operation_id, v_operation.operation_type;
  END IF;

  IF UPPER(BTRIM(COALESCE(v_operation.status, ''))) IN ('COMPLETE', 'FAILED', 'CANCELLED', 'CANCELED', 'REVIEW_REQUIRED') THEN
    RAISE EXCEPTION 'pay_workbench_prepare_draft_allocation_rows_seed cannot seed allocation rows for terminal operation % with status %', p_operation_id, v_operation.status;
  END IF;

  SELECT COALESCE(settings_row.banking_pay_workbench_semantic_ready_draft_guard_v2_enabled, false),
         COALESCE(settings_row.banking_pay_source_publication_identity_enforce_v1_enabled, false)
  INTO v_semantic_draft_guard_enabled,v_source_publication_identity_enforce_enabled
  FROM public.settings_defaults AS settings_row
  WHERE settings_row.id = 1;

  DROP TABLE IF EXISTS pg_temp.tmp_pay_workbench_allocation_scope_rows;
  CREATE TEMPORARY TABLE pg_temp.tmp_pay_workbench_allocation_scope_rows ON COMMIT DROP AS
  SELECT scope_row.*
  FROM public.banking_pay_operation_candidate_scope AS scope_row
  WHERE scope_row.operation_id = p_operation_id
    AND (
      (
        p_candidate_scope_ids IS NULL
        AND UPPER(BTRIM(COALESCE(scope_row.status, ''))) IN ('PENDING', 'SCOPED')
      )
      OR (
        p_candidate_scope_ids IS NOT NULL
        AND scope_row.id IN (
          SELECT (supplied_scope.scope_value #>> '{}')::uuid
          FROM jsonb_array_elements(v_scope_ids) AS supplied_scope(scope_value)
        )
      )
    )
  ORDER BY scope_row.chunk_sequence NULLS LAST, scope_row.pay_channel, scope_row.candidate_id, scope_row.id
  LIMIT 100;

  SELECT COUNT(*)::integer
  INTO v_candidate_scopes_processed
  FROM pg_temp.tmp_pay_workbench_allocation_scope_rows AS selected_scope;

  IF p_candidate_scope_ids IS NOT NULL AND COALESCE(v_candidate_scopes_processed, 0) <> COALESCE(v_scope_id_count, 0) THEN
    RAISE EXCEPTION 'pay_workbench_prepare_draft_allocation_rows_seed one or more candidate scope ids do not belong to operation %', p_operation_id;
  END IF;

  IF COALESCE(v_candidate_scopes_processed, 0) = 0 THEN
    RETURN QUERY SELECT 0::integer, 0::integer, 0::integer, 0::integer;
    RETURN;
  END IF;

  SELECT COALESCE(jsonb_agg(selected_scope.id::text ORDER BY selected_scope.id::text), '[]'::jsonb)
  INTO v_plan_scope_ids
  FROM pg_temp.tmp_pay_workbench_allocation_scope_rows AS selected_scope;

  DROP TABLE IF EXISTS pg_temp.tmp_pay_workbench_allocation_preview_candidates;
  CREATE TEMPORARY TABLE pg_temp.tmp_pay_workbench_allocation_preview_candidates ON COMMIT DROP AS
  WITH scope_selected_lines AS (
    SELECT
      selected_scope.operation_id,
      selected_scope.id AS candidate_scope_id,
      selected_scope.pay_batch_id,
      selected_scope.workbench_session_id,
      selected_scope.source_session_version,
      selected_scope.candidate_id,
      selected_scope.pay_channel,
      selected_scope.candidate_totals_json,
      selected_scope.allocation_basis_json AS scope_allocation_basis_json,
      line_values.value AS line_json,
      line_values.ordinality::bigint AS line_ordinal
    FROM pg_temp.tmp_pay_workbench_allocation_scope_rows AS selected_scope
    CROSS JOIN LATERAL private.pay_workbench_draft_scope_line_rows_v8(
      selected_scope.id,
      selected_scope.selected_canonical_preview_lines_json,
      selected_scope.effective_canonical_preview_lines_json
    ) AS line_values(value, ordinality)
  ), normalised_scope_lines_raw AS (
    SELECT
      scope_selected_lines.operation_id,
      scope_selected_lines.candidate_scope_id,
      scope_selected_lines.pay_batch_id,
      scope_selected_lines.workbench_session_id,
      scope_selected_lines.source_session_version,
      scope_selected_lines.candidate_id,
      scope_selected_lines.pay_channel,
      CASE
        WHEN NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json->>'preview_row_id', scope_selected_lines.line_json->>'preview_row_pk', scope_selected_lines.line_json->>'row_id', '')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json->>'preview_row_id', scope_selected_lines.line_json->>'preview_row_pk', scope_selected_lines.line_json->>'row_id', '')), '')::uuid
        ELSE NULL::uuid
      END AS preview_row_id,
      NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json->>'row_key', scope_selected_lines.line_json->>'source_ref', '')), '') AS row_key,
      CASE WHEN COALESCE(scope_selected_lines.line_json->>'row_ordinal', '') ~ '^[0-9]+$' THEN (scope_selected_lines.line_json->>'row_ordinal')::bigint ELSE scope_selected_lines.line_ordinal END AS row_ordinal,
      scope_selected_lines.line_json AS row_json,
      CASE
        WHEN NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json#>>'{economic_key,timesheet_id}', '')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json#>>'{economic_key,timesheet_id}', '')), '')::uuid
        ELSE NULL::uuid
      END AS economic_key_timesheet_id,
      CASE
        WHEN NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json->>'timesheet_id', scope_selected_lines.line_json->>'real_business_timesheet_id', '')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json->>'timesheet_id', scope_selected_lines.line_json->>'real_business_timesheet_id', '')), '')::uuid
        ELSE NULL::uuid
      END AS top_level_timesheet_id,
      COALESCE(NULLIF(BTRIM(scope_selected_lines.line_json->>'section'), ''), 'canonical_preview_lines') AS section,
      UPPER(NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json#>>'{economic_key,key_type}', scope_selected_lines.line_json->>'key_type', scope_selected_lines.line_json->>'component_key_type', '')), '')) AS key_type,
      NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json#>>'{economic_key,key_value}', scope_selected_lines.line_json->>'key_value', scope_selected_lines.line_json->>'component_key_value', '')), '') AS key_value,
      UPPER(NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json->>'selection_state', 'SELECTED')), '')) AS selection_state,
      UPPER(NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json->>'status', 'READY')), '')) AS line_status,
      LOWER(BTRIM(COALESCE(scope_selected_lines.line_json->>'selected', 'true'))) IN ('true', 't', '1', 'yes', 'y', 'on') AS line_selected,
      (
        COALESCE(scope_selected_lines.line_json->>'projection_path', '') = 'WORKBENCH_CANDIDATE_DELTA_REFRESH'
        OR scope_selected_lines.line_json ? 'projection_run_id'
      ) AS is_delta_projection,
      LOWER(BTRIM(COALESCE(scope_selected_lines.line_json->>'projection_certified', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') AS projection_certified,
      COALESCE(
        NULLIF(BTRIM(scope_selected_lines.line_json->>'policy_x_authority_scope'), ''),
        NULLIF(BTRIM(scope_selected_lines.line_json#>>'{contract_json,policy_x_authority_scope}'), ''),
        NULLIF(BTRIM(scope_selected_lines.line_json#>>'{contract,policy_x_authority_scope}'), ''),
        ''
      ) AS policy_x_authority_scope,
      (
        LOWER(BTRIM(COALESCE(scope_selected_lines.line_json->>'post_draft_overlay_applied', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        AND (
          UPPER(BTRIM(COALESCE(scope_selected_lines.line_json->>'post_draft_overlay_operation_type', ''))) IN ('DRAFT_CREATE', 'PAYMENT_EXECUTE', 'PAYMENT_SETTLE')
          OR LOWER(BTRIM(COALESCE(scope_selected_lines.line_json->>'selected', 'true'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
          OR UPPER(BTRIM(COALESCE(scope_selected_lines.line_json->>'selection_state', 'SELECTED'))) <> 'SELECTED'
          OR UPPER(BTRIM(COALESCE(scope_selected_lines.line_json->>'status', 'READY'))) <> 'READY'
        )
      ) AS post_draft_overlay_unavailable,
      CASE WHEN NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json->>'finance_case_id', '')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json->>'finance_case_id', '')), '')::uuid ELSE NULL::uuid END AS finance_case_id,
      CASE WHEN NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json->>'finance_component_id', '')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json->>'finance_component_id', '')), '')::uuid ELSE NULL::uuid END AS finance_component_id,
      UPPER(COALESCE(NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json->>'line_type', '')), ''), NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json->>'case_type', '')), ''), 'PREVIEW_ROW')) AS allocation_type,
      UPPER(NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json->>'item_direction', scope_selected_lines.line_json->>'direction', '')), '')) AS item_direction,
      component_probe.single_fixed_reimbursement_key_type,
      component_probe.single_fixed_reimbursement_key_value,
      NULLIF(BTRIM(COALESCE(scope_selected_lines.line_json->>'source_ref', scope_selected_lines.line_json->>'row_key', '')), '') AS source_ref,
      CASE
        WHEN COALESCE(scope_selected_lines.line_json->>'amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
          THEN ROUND((scope_selected_lines.line_json->>'amount_ex_vat')::numeric, 2)
        WHEN COALESCE(scope_selected_lines.line_json->>'preview_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
          THEN ROUND((scope_selected_lines.line_json->>'preview_amount_ex_vat')::numeric, 2)
        WHEN COALESCE(scope_selected_lines.line_json->>'allocated_amount', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
          THEN ROUND((scope_selected_lines.line_json->>'allocated_amount')::numeric, 2)
        ELSE NULL::numeric
      END AS allocated_amount
    FROM scope_selected_lines
    CROSS JOIN LATERAL (
      SELECT
        CASE
          WHEN component_counts.object_component_count = 1
           AND component_counts.fixed_reimbursement_component_count = 1 THEN component_counts.fixed_reimbursement_key_type
          ELSE NULL::text
        END AS single_fixed_reimbursement_key_type,
        CASE
          WHEN component_counts.object_component_count = 1
           AND component_counts.fixed_reimbursement_component_count = 1 THEN component_counts.fixed_reimbursement_key_value
          ELSE NULL::text
        END AS single_fixed_reimbursement_key_value
      FROM (
        SELECT
          (COUNT(*) FILTER (
            WHERE component_element.value IS NOT NULL
              AND jsonb_typeof(component_element.value) = 'object'
          ))::integer AS object_component_count,
          (COUNT(*) FILTER (
            WHERE component_element.value IS NOT NULL
              AND jsonb_typeof(component_element.value) = 'object'
              AND UPPER(NULLIF(BTRIM(COALESCE(component_element.value->>'classification', '')), '')) = 'REIMBURSEMENT_GROSS_FIXED'
              AND UPPER(NULLIF(BTRIM(COALESCE(component_element.value->>'component_key_type', '')), '')) IN ('EXPENSE_CODE', 'ADDITIONAL_CODE')
              AND NULLIF(BTRIM(COALESCE(component_element.value->>'component_key_value', '')), '') IS NOT NULL
          ))::integer AS fixed_reimbursement_component_count,
          MAX(UPPER(NULLIF(BTRIM(COALESCE(component_element.value->>'component_key_type', '')), ''))) FILTER (
            WHERE component_element.value IS NOT NULL
              AND jsonb_typeof(component_element.value) = 'object'
              AND UPPER(NULLIF(BTRIM(COALESCE(component_element.value->>'classification', '')), '')) = 'REIMBURSEMENT_GROSS_FIXED'
              AND UPPER(NULLIF(BTRIM(COALESCE(component_element.value->>'component_key_type', '')), '')) IN ('EXPENSE_CODE', 'ADDITIONAL_CODE')
              AND NULLIF(BTRIM(COALESCE(component_element.value->>'component_key_value', '')), '') IS NOT NULL
          ) AS fixed_reimbursement_key_type,
          MAX(NULLIF(BTRIM(COALESCE(component_element.value->>'component_key_value', '')), '')) FILTER (
            WHERE component_element.value IS NOT NULL
              AND jsonb_typeof(component_element.value) = 'object'
              AND UPPER(NULLIF(BTRIM(COALESCE(component_element.value->>'classification', '')), '')) = 'REIMBURSEMENT_GROSS_FIXED'
              AND UPPER(NULLIF(BTRIM(COALESCE(component_element.value->>'component_key_type', '')), '')) IN ('EXPENSE_CODE', 'ADDITIONAL_CODE')
              AND NULLIF(BTRIM(COALESCE(component_element.value->>'component_key_value', '')), '') IS NOT NULL
          ) AS fixed_reimbursement_key_value
        FROM jsonb_array_elements(
          CASE
            WHEN jsonb_typeof(scope_selected_lines.line_json->'case_components') = 'array' THEN scope_selected_lines.line_json->'case_components'
            ELSE '[]'::jsonb
          END
        ) AS component_element(value)
      ) AS component_counts
    ) AS component_probe
  ), allocation_timesheet_refs AS (
    SELECT normalised_scope_lines_raw.economic_key_timesheet_id AS timesheet_id
    FROM normalised_scope_lines_raw
    WHERE normalised_scope_lines_raw.economic_key_timesheet_id IS NOT NULL
    UNION
    SELECT normalised_scope_lines_raw.top_level_timesheet_id AS timesheet_id
    FROM normalised_scope_lines_raw
    WHERE normalised_scope_lines_raw.top_level_timesheet_id IS NOT NULL
  ), allocation_timesheet_id_array AS (
    SELECT COALESCE(
      array_agg(DISTINCT allocation_timesheet_refs.timesheet_id ORDER BY allocation_timesheet_refs.timesheet_id),
      array[]::uuid[]
    ) AS timesheet_ids
    FROM allocation_timesheet_refs
  ), allocation_rotation_scope AS (
    SELECT DISTINCT ON (rotation_scope.requested_timesheet_id)
      rotation_scope.requested_timesheet_id,
      COALESCE(rotation_scope.canonical_timesheet_id, rotation_scope.requested_timesheet_id) AS canonical_timesheet_id,
      (rotation_scope.family_timesheet_id IS NOT NULL AND rotation_scope.family_is_current IS NOT NULL) AS rotation_scope_resolved
    FROM allocation_timesheet_id_array
    JOIN public._pay_timesheet_rotation_scope(allocation_timesheet_id_array.timesheet_ids) AS rotation_scope
      ON true
    ORDER BY
      rotation_scope.requested_timesheet_id,
      rotation_scope.family_is_current DESC NULLS LAST,
      rotation_scope.family_version DESC NULLS LAST,
      rotation_scope.family_timesheet_id
  ), normalised_scope_lines AS (
    SELECT
      normalised_scope_lines_raw.operation_id,
      normalised_scope_lines_raw.candidate_scope_id,
      normalised_scope_lines_raw.pay_batch_id,
      normalised_scope_lines_raw.workbench_session_id,
      normalised_scope_lines_raw.source_session_version,
      normalised_scope_lines_raw.candidate_id,
      normalised_scope_lines_raw.pay_channel,
      normalised_scope_lines_raw.preview_row_id,
      normalised_scope_lines_raw.row_key,
      normalised_scope_lines_raw.row_ordinal,
      jsonb_strip_nulls(
        normalised_scope_lines_raw.row_json
        || CASE
          WHEN COALESCE(economic_key_rotation.canonical_timesheet_id, top_level_rotation.canonical_timesheet_id) IS NULL THEN '{}'::jsonb
          ELSE jsonb_build_object(
            'timesheet_id', COALESCE(economic_key_rotation.canonical_timesheet_id, top_level_rotation.canonical_timesheet_id)::text,
            'real_business_timesheet_id', COALESCE(economic_key_rotation.canonical_timesheet_id, top_level_rotation.canonical_timesheet_id)::text,
            'economic_key', COALESCE(normalised_scope_lines_raw.row_json->'economic_key', '{}'::jsonb)
              || jsonb_build_object('timesheet_id', COALESCE(economic_key_rotation.canonical_timesheet_id, top_level_rotation.canonical_timesheet_id)::text)
          )
        END
        || CASE
          WHEN normalised_scope_lines_raw.top_level_timesheet_id IS NOT NULL
           AND normalised_scope_lines_raw.top_level_timesheet_id IS DISTINCT FROM COALESCE(economic_key_rotation.canonical_timesheet_id, top_level_rotation.canonical_timesheet_id)
          THEN jsonb_build_object('rotation_requested_timesheet_id', normalised_scope_lines_raw.top_level_timesheet_id::text)
          ELSE '{}'::jsonb
        END
      ) AS row_json,
      COALESCE(economic_key_rotation.canonical_timesheet_id, top_level_rotation.canonical_timesheet_id, normalised_scope_lines_raw.economic_key_timesheet_id, normalised_scope_lines_raw.top_level_timesheet_id) AS timesheet_id,
      normalised_scope_lines_raw.section,
      normalised_scope_lines_raw.key_type,
      normalised_scope_lines_raw.key_value,
      normalised_scope_lines_raw.selection_state,
      normalised_scope_lines_raw.line_status,
      normalised_scope_lines_raw.line_selected,
      normalised_scope_lines_raw.is_delta_projection,
      normalised_scope_lines_raw.projection_certified,
      normalised_scope_lines_raw.policy_x_authority_scope,
      normalised_scope_lines_raw.post_draft_overlay_unavailable,
      (
        normalised_scope_lines_raw.preview_row_id IS NULL
        OR backing_preview_row.id IS NULL
        OR UPPER(BTRIM(COALESCE(backing_preview_row.status, ''))) <> 'READY'
        OR COALESCE(backing_preview_row.selected, false) IS NOT TRUE
        OR UPPER(BTRIM(COALESCE(backing_preview_row.selection_state, ''))) <> 'SELECTED'
        OR backing_preview_row.candidate_id IS DISTINCT FROM normalised_scope_lines_raw.candidate_id
        OR private.pay_workbench_preview_effective_section_v1(
             backing_preview_row.section,
             backing_preview_row.row_json
           ) IS DISTINCT FROM COALESCE(NULLIF(BTRIM(normalised_scope_lines_raw.section), ''), 'canonical_preview_lines')
        OR NULLIF(BTRIM(backing_preview_row.row_key), '') IS DISTINCT FROM NULLIF(BTRIM(normalised_scope_lines_raw.row_key), '')
        OR (
          normalised_scope_lines_raw.finance_case_id IS NULL
          AND backing_preview_row.timesheet_id IS NULL
        )
        OR (
          normalised_scope_lines_raw.finance_case_id IS NULL
          AND backing_preview_row.timesheet_id IS DISTINCT FROM COALESCE(economic_key_rotation.canonical_timesheet_id, top_level_rotation.canonical_timesheet_id, normalised_scope_lines_raw.economic_key_timesheet_id, normalised_scope_lines_raw.top_level_timesheet_id)
        )
        OR UPPER(NULLIF(BTRIM(COALESCE(backing_preview_row.key_type, backing_preview_row.row_json#>>'{economic_key,key_type}', backing_preview_row.row_json->>'component_key_type', '')), '')) IS DISTINCT FROM normalised_scope_lines_raw.key_type
        OR NULLIF(BTRIM(COALESCE(backing_preview_row.key_value, backing_preview_row.row_json#>>'{economic_key,key_value}', backing_preview_row.row_json->>'component_key_value', '')), '') IS DISTINCT FROM normalised_scope_lines_raw.key_value
        OR COALESCE(
          NULLIF(BTRIM(backing_preview_row.row_json->>'policy_x_authority_scope'), ''),
          NULLIF(BTRIM(backing_preview_row.row_json#>>'{contract_json,policy_x_authority_scope}'), ''),
          NULLIF(BTRIM(backing_preview_row.row_json#>>'{contract,policy_x_authority_scope}'), ''),
          ''
        ) <> 'PRE_DRAFT_LIVE_TRUTH'
        OR (
          (
            COALESCE(backing_preview_row.row_json->>'projection_path', '') = 'WORKBENCH_CANDIDATE_DELTA_REFRESH'
            OR backing_preview_row.row_json ? 'projection_run_id'
          )
          AND LOWER(BTRIM(COALESCE(backing_preview_row.row_json->>'projection_certified', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
        )
        OR (
          backing_preview_row.row_json ? 'selected'
          AND (LOWER(BTRIM(COALESCE(backing_preview_row.row_json->>'selected', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')) IS DISTINCT FROM COALESCE(backing_preview_row.selected, false)
        )
        OR (
          backing_preview_row.row_json ? 'selection_state'
          AND UPPER(BTRIM(COALESCE(backing_preview_row.row_json->>'selection_state', ''))) IS DISTINCT FROM UPPER(BTRIM(COALESCE(backing_preview_row.selection_state, '')))
        )
        OR (
          backing_preview_row.row_json ? 'status'
          AND UPPER(BTRIM(COALESCE(backing_preview_row.row_json->>'status', ''))) IS DISTINCT FROM UPPER(BTRIM(COALESCE(backing_preview_row.status, '')))
        )
        OR (
          LOWER(BTRIM(COALESCE(backing_preview_row.row_json->>'post_draft_overlay_applied', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
          AND (
            UPPER(BTRIM(COALESCE(backing_preview_row.row_json->>'post_draft_overlay_operation_type', ''))) IN ('DRAFT_CREATE', 'PAYMENT_EXECUTE', 'PAYMENT_SETTLE')
            OR LOWER(BTRIM(COALESCE(backing_preview_row.row_json->>'selected', 'true'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
            OR UPPER(BTRIM(COALESCE(backing_preview_row.row_json->>'selection_state', backing_preview_row.selection_state, ''))) <> 'SELECTED'
            OR UPPER(BTRIM(COALESCE(backing_preview_row.row_json->>'status', backing_preview_row.status, ''))) <> 'READY'
          )
        )
      ) AS backing_table_state_invalid,
      CASE
        WHEN normalised_scope_lines_raw.preview_row_id IS NULL THEN 'ALLOCATION_BACKING_PREVIEW_ROW_ID_MISSING'
        WHEN backing_preview_row.id IS NULL THEN 'ALLOCATION_BACKING_PREVIEW_ROW_NOT_CURRENT'
        WHEN UPPER(BTRIM(COALESCE(backing_preview_row.status, ''))) <> 'READY' THEN 'ALLOCATION_BACKING_PREVIEW_ROW_NOT_READY'
        WHEN COALESCE(backing_preview_row.selected, false) IS NOT TRUE THEN 'ALLOCATION_BACKING_PREVIEW_ROW_NOT_SELECTED'
        WHEN UPPER(BTRIM(COALESCE(backing_preview_row.selection_state, ''))) <> 'SELECTED' THEN 'ALLOCATION_BACKING_PREVIEW_ROW_SELECTION_STATE_INVALID'
        WHEN backing_preview_row.candidate_id IS DISTINCT FROM normalised_scope_lines_raw.candidate_id THEN 'ALLOCATION_BACKING_PREVIEW_ROW_CANDIDATE_MISMATCH'
        WHEN private.pay_workbench_preview_effective_section_v1(
               backing_preview_row.section,
               backing_preview_row.row_json
             ) IS DISTINCT FROM COALESCE(NULLIF(BTRIM(normalised_scope_lines_raw.section), ''), 'canonical_preview_lines') THEN 'ALLOCATION_BACKING_PREVIEW_ROW_SECTION_MISMATCH'
        WHEN NULLIF(BTRIM(backing_preview_row.row_key), '') IS DISTINCT FROM NULLIF(BTRIM(normalised_scope_lines_raw.row_key), '') THEN 'ALLOCATION_BACKING_PREVIEW_ROW_KEY_MISMATCH'
        WHEN normalised_scope_lines_raw.finance_case_id IS NULL AND backing_preview_row.timesheet_id IS NULL THEN 'ALLOCATION_BACKING_PREVIEW_ROW_ECONOMIC_KEY_MISSING'
        WHEN normalised_scope_lines_raw.finance_case_id IS NULL AND backing_preview_row.timesheet_id IS DISTINCT FROM COALESCE(economic_key_rotation.canonical_timesheet_id, top_level_rotation.canonical_timesheet_id, normalised_scope_lines_raw.economic_key_timesheet_id, normalised_scope_lines_raw.top_level_timesheet_id) THEN 'ALLOCATION_BACKING_PREVIEW_ROW_TIMESHEET_MISMATCH'
        WHEN UPPER(NULLIF(BTRIM(COALESCE(backing_preview_row.key_type, backing_preview_row.row_json#>>'{economic_key,key_type}', backing_preview_row.row_json->>'component_key_type', '')), '')) IS DISTINCT FROM normalised_scope_lines_raw.key_type THEN 'ALLOCATION_BACKING_PREVIEW_ROW_KEY_TYPE_MISMATCH'
        WHEN NULLIF(BTRIM(COALESCE(backing_preview_row.key_value, backing_preview_row.row_json#>>'{economic_key,key_value}', backing_preview_row.row_json->>'component_key_value', '')), '') IS DISTINCT FROM normalised_scope_lines_raw.key_value THEN 'ALLOCATION_BACKING_PREVIEW_ROW_KEY_VALUE_MISMATCH'
        WHEN COALESCE(NULLIF(BTRIM(backing_preview_row.row_json->>'policy_x_authority_scope'), ''), NULLIF(BTRIM(backing_preview_row.row_json#>>'{contract_json,policy_x_authority_scope}'), ''), NULLIF(BTRIM(backing_preview_row.row_json#>>'{contract,policy_x_authority_scope}'), ''), '') <> 'PRE_DRAFT_LIVE_TRUTH' THEN 'ALLOCATION_BACKING_PREVIEW_ROW_POLICY_X_AUTHORITY_INVALID'
        WHEN (COALESCE(backing_preview_row.row_json->>'projection_path', '') = 'WORKBENCH_CANDIDATE_DELTA_REFRESH' OR backing_preview_row.row_json ? 'projection_run_id') AND LOWER(BTRIM(COALESCE(backing_preview_row.row_json->>'projection_certified', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on') THEN 'ALLOCATION_BACKING_PREVIEW_ROW_DELTA_NOT_CERTIFIED'
        WHEN LOWER(BTRIM(COALESCE(backing_preview_row.row_json->>'post_draft_overlay_applied', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN 'ALLOCATION_BACKING_PREVIEW_ROW_POST_DRAFT_OVERLAY_UNAVAILABLE'
        WHEN (
          (backing_preview_row.row_json ? 'selected' AND (LOWER(BTRIM(COALESCE(backing_preview_row.row_json->>'selected', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')) IS DISTINCT FROM COALESCE(backing_preview_row.selected, false))
          OR (backing_preview_row.row_json ? 'selection_state' AND UPPER(BTRIM(COALESCE(backing_preview_row.row_json->>'selection_state', ''))) IS DISTINCT FROM UPPER(BTRIM(COALESCE(backing_preview_row.selection_state, ''))))
          OR (backing_preview_row.row_json ? 'status' AND UPPER(BTRIM(COALESCE(backing_preview_row.row_json->>'status', ''))) IS DISTINCT FROM UPPER(BTRIM(COALESCE(backing_preview_row.status, ''))))
        ) THEN 'ALLOCATION_BACKING_PREVIEW_ROW_TABLE_JSON_CONFLICT'
        ELSE NULL::text
      END AS backing_table_state_failure_reason,
      normalised_scope_lines_raw.finance_case_id,
      normalised_scope_lines_raw.finance_component_id,
      normalised_scope_lines_raw.allocation_type,
      normalised_scope_lines_raw.item_direction,
      normalised_scope_lines_raw.single_fixed_reimbursement_key_type,
      normalised_scope_lines_raw.single_fixed_reimbursement_key_value,
      normalised_scope_lines_raw.source_ref,
      normalised_scope_lines_raw.allocated_amount,
      CASE
        WHEN normalised_scope_lines_raw.economic_key_timesheet_id IS NULL AND normalised_scope_lines_raw.top_level_timesheet_id IS NULL THEN false
        WHEN normalised_scope_lines_raw.economic_key_timesheet_id IS NOT NULL AND COALESCE(economic_key_rotation.rotation_scope_resolved, false) = false THEN true
        WHEN normalised_scope_lines_raw.top_level_timesheet_id IS NOT NULL AND COALESCE(top_level_rotation.rotation_scope_resolved, false) = false THEN true
        WHEN economic_key_rotation.canonical_timesheet_id IS NOT NULL
         AND top_level_rotation.canonical_timesheet_id IS NOT NULL
         AND economic_key_rotation.canonical_timesheet_id IS DISTINCT FROM top_level_rotation.canonical_timesheet_id THEN true
        ELSE false
      END AS rotation_validation_failed,
      CASE
        WHEN normalised_scope_lines_raw.economic_key_timesheet_id IS NULL AND normalised_scope_lines_raw.top_level_timesheet_id IS NULL THEN NULL::text
        WHEN normalised_scope_lines_raw.economic_key_timesheet_id IS NOT NULL AND COALESCE(economic_key_rotation.rotation_scope_resolved, false) = false THEN 'ALLOCATION_ECONOMIC_KEY_TIMESHEET_ROTATION_SCOPE_UNRESOLVED'
        WHEN normalised_scope_lines_raw.top_level_timesheet_id IS NOT NULL AND COALESCE(top_level_rotation.rotation_scope_resolved, false) = false THEN 'ALLOCATION_LINE_TIMESHEET_ROTATION_SCOPE_UNRESOLVED'
        WHEN economic_key_rotation.canonical_timesheet_id IS NOT NULL
         AND top_level_rotation.canonical_timesheet_id IS NOT NULL
         AND economic_key_rotation.canonical_timesheet_id IS DISTINCT FROM top_level_rotation.canonical_timesheet_id THEN 'ALLOCATION_LINE_AND_ECONOMIC_KEY_CANONICAL_TIMESHEET_MISMATCH'
        ELSE NULL::text
      END AS rotation_failure_reason
    FROM normalised_scope_lines_raw
    LEFT JOIN allocation_rotation_scope AS economic_key_rotation
      ON economic_key_rotation.requested_timesheet_id = normalised_scope_lines_raw.economic_key_timesheet_id
    LEFT JOIN allocation_rotation_scope AS top_level_rotation
      ON top_level_rotation.requested_timesheet_id = normalised_scope_lines_raw.top_level_timesheet_id
    LEFT JOIN public.banking_pay_workbench_preview_rows AS backing_preview_row
      ON backing_preview_row.id = normalised_scope_lines_raw.preview_row_id
     AND (
       normalised_scope_lines_raw.workbench_session_id IS NULL
       OR backing_preview_row.session_id = normalised_scope_lines_raw.workbench_session_id
     )
     AND (
       normalised_scope_lines_raw.source_session_version IS NULL
       OR backing_preview_row.session_version = normalised_scope_lines_raw.source_session_version
     )
  )
  SELECT
    normalised_scope_lines.*,
    jsonb_strip_nulls(jsonb_build_object(
      'timesheet_id', CASE WHEN normalised_scope_lines.timesheet_id IS NULL THEN NULL ELSE normalised_scope_lines.timesheet_id::text END,
      'key_type', normalised_scope_lines.key_type,
      'key_value', normalised_scope_lines.key_value
    )) AS economic_key_json,
    public.pay_workbench_preview_line_contract_ok(
      p_line_json => jsonb_strip_nulls(
        normalised_scope_lines.row_json
        || jsonb_build_object(
          'row_key', normalised_scope_lines.row_key,
          'preview_row_pk', CASE WHEN normalised_scope_lines.preview_row_id IS NULL THEN NULL ELSE normalised_scope_lines.preview_row_id::text END,
          'selection_state', normalised_scope_lines.selection_state
        )
      ),
      p_economic_key_json => jsonb_strip_nulls(jsonb_build_object(
        'timesheet_id', CASE WHEN normalised_scope_lines.timesheet_id IS NULL THEN NULL ELSE normalised_scope_lines.timesheet_id::text END,
        'key_type', normalised_scope_lines.key_type,
        'key_value', normalised_scope_lines.key_value
      )),
      p_target_section => normalised_scope_lines.section
    ) AS preview_contract_json
  FROM normalised_scope_lines
  ORDER BY normalised_scope_lines.pay_channel, normalised_scope_lines.candidate_id, normalised_scope_lines.row_ordinal, normalised_scope_lines.preview_row_id;

  SELECT COALESCE(jsonb_agg(jsonb_build_object(
           'preview_row_id', invalid_rows.preview_row_id::text,
           'row_key', invalid_rows.row_key,
           'candidate_id', invalid_rows.candidate_id::text,
           'candidate_scope_id', invalid_rows.candidate_scope_id::text,
           'reasons', COALESCE(invalid_rows.preview_contract_json->'reasons', '[]'::jsonb)
             || CASE WHEN invalid_rows.rotation_validation_failed IS TRUE THEN jsonb_build_array(invalid_rows.rotation_failure_reason) ELSE '[]'::jsonb END
             || CASE WHEN invalid_rows.backing_table_state_invalid IS TRUE THEN jsonb_build_array(COALESCE(invalid_rows.backing_table_state_failure_reason, 'ALLOCATION_BACKING_PREVIEW_ROW_STATE_INVALID')) ELSE '[]'::jsonb END
             || CASE
               WHEN invalid_rows.single_fixed_reimbursement_key_type IS NOT NULL
                AND (
                  invalid_rows.key_type IS DISTINCT FROM invalid_rows.single_fixed_reimbursement_key_type
                  OR invalid_rows.key_value IS DISTINCT FROM invalid_rows.single_fixed_reimbursement_key_value
                ) THEN jsonb_build_array('POLICY_X_REIMBURSEMENT_KEY_MISMATCH')
               ELSE '[]'::jsonb
             END
             || CASE
               WHEN ROUND(COALESCE(invalid_rows.allocated_amount, 0), 2) < 0
                AND NOT (
                  invalid_rows.allocation_type IN ('OVERPAYMENT_RECOVERY', 'MANUAL_DEBT_RECOVERY', 'PAYMENT_ADVANCE_REPAYMENT', 'LOAN_REPAYMENT')
                  AND invalid_rows.finance_case_id IS NOT NULL
                  AND (invalid_rows.item_direction IS NULL OR invalid_rows.item_direction = 'DEDUCTION')
                ) THEN jsonb_build_array('NEGATIVE_ENTITLEMENT_MUST_ROUTE_TO_FINANCE_CASE')
               ELSE '[]'::jsonb
             END
         ) ORDER BY invalid_rows.row_ordinal, invalid_rows.preview_row_id), '[]'::jsonb)
  INTO v_malformed_allocation_preview_rows
  FROM (
    SELECT candidate_row.*
    FROM pg_temp.tmp_pay_workbench_allocation_preview_candidates AS candidate_row
    WHERE candidate_row.rotation_validation_failed IS TRUE
       OR LOWER(BTRIM(COALESCE(candidate_row.preview_contract_json->>'ok', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
       OR LOWER(BTRIM(COALESCE(candidate_row.preview_contract_json->>'selection_allowed', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
       OR candidate_row.preview_row_id IS NULL
       OR COALESCE(candidate_row.backing_table_state_invalid, false) IS TRUE
       OR lower(COALESCE(candidate_row.section, '')) <> 'canonical_preview_lines'
       OR UPPER(BTRIM(COALESCE(candidate_row.selection_state, ''))) <> 'SELECTED'
       OR UPPER(BTRIM(COALESCE(candidate_row.line_status, ''))) <> 'READY'
       OR COALESCE(candidate_row.line_selected, false) IS NOT TRUE
       OR (COALESCE(candidate_row.is_delta_projection, false) IS TRUE AND COALESCE(candidate_row.projection_certified, false) IS NOT TRUE)
       OR COALESCE(candidate_row.policy_x_authority_scope, '') <> 'PRE_DRAFT_LIVE_TRUTH'
       OR (candidate_row.timesheet_id IS NULL AND candidate_row.finance_case_id IS NULL)
       OR COALESCE(candidate_row.post_draft_overlay_unavailable, false) IS TRUE
       OR candidate_row.pay_channel NOT IN ('PAYE', 'UMBRELLA')
       OR (
         candidate_row.key_type NOT IN ('TS_DAY','TS_TOTAL','ADDITIONAL_CODE','ADJUSTMENT_CODE','EXPENSE_CODE','MANUAL_CARRY_FORWARD')
         AND NOT (candidate_row.finance_case_id IS NOT NULL AND candidate_row.key_type IN ('CASE_TOTAL','FINANCE_COMPONENT'))
       )
       OR candidate_row.key_value IS NULL
       OR (
         candidate_row.single_fixed_reimbursement_key_type IS NOT NULL
         AND (
           candidate_row.key_type IS DISTINCT FROM candidate_row.single_fixed_reimbursement_key_type
           OR candidate_row.key_value IS DISTINCT FROM candidate_row.single_fixed_reimbursement_key_value
         )
       )
       OR (candidate_row.key_type = 'TS_DAY' AND candidate_row.key_value !~ '^\d{4}-\d{2}-\d{2}$')
       OR candidate_row.allocated_amount IS NULL
       OR ROUND(COALESCE(candidate_row.allocated_amount, 0), 2) = 0
       OR (
         ROUND(COALESCE(candidate_row.allocated_amount, 0), 2) < 0
         AND NOT (
           candidate_row.allocation_type IN ('OVERPAYMENT_RECOVERY', 'MANUAL_DEBT_RECOVERY', 'PAYMENT_ADVANCE_REPAYMENT', 'LOAN_REPAYMENT')
           AND candidate_row.finance_case_id IS NOT NULL
           AND (candidate_row.item_direction IS NULL OR candidate_row.item_direction = 'DEDUCTION')
         )
       )
    ORDER BY candidate_row.row_ordinal, candidate_row.preview_row_id
    LIMIT 25
  ) AS invalid_rows;

  IF jsonb_array_length(COALESCE(v_malformed_allocation_preview_rows, '[]'::jsonb)) > 0 THEN
    RAISE EXCEPTION 'MALFORMED_PREVIEW_ROW_NOT_DRAFTABLE'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'MALFORMED_PREVIEW_ROW_NOT_DRAFTABLE',
              'operation_id', p_operation_id::text,
              'malformed_selected_preview_rows', COALESCE(v_malformed_allocation_preview_rows, '[]'::jsonb),
              'message', 'Selected preview rows cannot be converted into allocation rows because they are not valid draftable Ready to Pay rows.'
            )::text;
  END IF;

  -- A set of individually valid deduction rows is not by itself a valid Draft
  -- candidate.  Headroom is candidate-local and is calculated only from the
  -- positive ordinary rows actually selected into this Draft operation.
  IF COALESCE(v_semantic_draft_guard_enabled, false) THEN
    SELECT COUNT(*)::integer
    INTO v_semantic_source_failure_count
    FROM pg_temp.tmp_pay_workbench_allocation_scope_rows AS selected_scope
    WHERE selected_scope.allocation_basis_json#>>'{source_publication_attestation,attestation_version}'
            IS DISTINCT FROM 'CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3'
       OR selected_scope.allocation_basis_json#>>'{source_publication_attestation,semantic_contract_version}'
            IS DISTINCT FROM 'READY_TO_PAY_SEMANTIC_V2'
       OR COALESCE((selected_scope.allocation_basis_json#>>'{source_publication_attestation,semantic_ready}')::boolean,false)
            IS NOT TRUE
       OR NULLIF(selected_scope.allocation_basis_json->>'semantic_proof_digest','') IS NULL
       OR NULLIF(selected_scope.allocation_basis_json->>'source_build_run_id','') IS NULL
       OR (
         v_source_publication_identity_enforce_enabled
         AND COALESCE(selected_scope.allocation_basis_json->>'source_publication_id','')
           !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       )
       OR (
         COALESCE(selected_scope.allocation_basis_json->>'source_publication_id','')
           ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
         AND NOT EXISTS (
           SELECT 1
           FROM public.banking_pay_workbench_session_scope AS source_scope
           WHERE source_scope.session_id=selected_scope.workbench_session_id
             AND source_scope.candidate_id=selected_scope.candidate_id
             AND source_scope.certified_preview_publication_parity_ok IS TRUE
             AND source_scope.certified_preview_publication_session_version=selected_scope.source_session_version
             AND source_scope.certified_preview_publication_source_publication_id=
                   (selected_scope.allocation_basis_json->>'source_publication_id')::uuid
             AND source_scope.certified_preview_publication_attestation_json->>'source_publication_id'=
                   selected_scope.allocation_basis_json->>'source_publication_id'
         )
       );

    IF v_semantic_source_failure_count > 0 THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_DRAFT_SOURCE_SEMANTIC_CERTIFICATION_REQUIRED'
        USING ERRCODE='P0001',DETAIL=jsonb_build_object(
          'code','PAY_WORKBENCH_DRAFT_SOURCE_SEMANTIC_CERTIFICATION_REQUIRED',
          'candidate_scope_count',v_semantic_source_failure_count,
          'message','Allocation seeding requires the V3 semantic source frozen by Draft scope creation.'
        )::text;
    END IF;

    WITH candidate_semantics AS (
      SELECT
        candidate_row.candidate_id,
        candidate_row.candidate_scope_id,
        ROUND(COALESCE(SUM(candidate_row.allocated_amount) FILTER (
          WHERE candidate_row.allocated_amount > 0
            AND candidate_row.allocation_type = 'TIMESHEET_PAYMENT'
            AND COALESCE((candidate_row.preview_contract_json->>'ok')::boolean, false)
            AND COALESCE((candidate_row.preview_contract_json->>'selection_allowed')::boolean, false)
        ), 0), 2) AS selected_positive_ordinary_amount,
        ROUND(COALESCE(SUM(candidate_row.allocated_amount) FILTER (
          WHERE candidate_row.allocated_amount < 0
            AND candidate_row.allocation_type IN (
              'OVERPAYMENT_RECOVERY',
              'MANUAL_DEBT_RECOVERY',
              'PAYMENT_ADVANCE_REPAYMENT',
              'LOAN_REPAYMENT'
            )
            AND candidate_row.finance_case_id IS NOT NULL
            AND (candidate_row.item_direction IS NULL OR candidate_row.item_direction = 'DEDUCTION')
            AND COALESCE((candidate_row.preview_contract_json->>'ok')::boolean, false)
            AND COALESCE((candidate_row.preview_contract_json->>'selection_allowed')::boolean, false)
        ), 0), 2) AS selected_deduction_amount
      FROM pg_temp.tmp_pay_workbench_allocation_preview_candidates AS candidate_row
      WHERE candidate_row.line_selected IS TRUE
        AND UPPER(COALESCE(candidate_row.selection_state, '')) = 'SELECTED'
        AND UPPER(COALESCE(candidate_row.line_status, '')) = 'READY'
      GROUP BY candidate_row.candidate_id, candidate_row.candidate_scope_id
    ), failed AS (
      SELECT
        candidate_semantics.*,
        ROUND(candidate_semantics.selected_positive_ordinary_amount
          + candidate_semantics.selected_deduction_amount, 2) AS selected_candidate_result,
        CASE
          WHEN candidate_semantics.selected_deduction_amount < 0
           AND candidate_semantics.selected_positive_ordinary_amount <= 0
            THEN 'PAY_WORKBENCH_DRAFT_RECOVERY_WITHOUT_POSITIVE_HEADROOM'
          WHEN -candidate_semantics.selected_deduction_amount
             > candidate_semantics.selected_positive_ordinary_amount
            THEN 'PAY_WORKBENCH_DRAFT_DEDUCTION_EXCEEDS_SELECTED_HEADROOM'
          WHEN candidate_semantics.selected_positive_ordinary_amount
             + candidate_semantics.selected_deduction_amount < 0
            THEN 'PAY_WORKBENCH_DRAFT_CANDIDATE_RESULT_NEGATIVE'
          ELSE NULL
        END AS failure_code
      FROM candidate_semantics
    )
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
      'candidate_id', failed.candidate_id::text,
      'candidate_scope_id', failed.candidate_scope_id::text,
      'selected_positive_ordinary_amount', failed.selected_positive_ordinary_amount,
      'selected_deduction_amount', failed.selected_deduction_amount,
      'selected_candidate_result', failed.selected_candidate_result,
      'code', failed.failure_code
    ) ORDER BY failed.candidate_id), '[]'::jsonb)
    INTO v_semantic_draft_failures
    FROM failed
    WHERE failed.failure_code IS NOT NULL;

    IF jsonb_array_length(COALESCE(v_semantic_draft_failures, '[]'::jsonb)) > 0 THEN
      RAISE EXCEPTION 'PAY_WORKBENCH_DRAFT_SELECTED_SEMANTIC_PROOF_FAILED'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAY_WORKBENCH_DRAFT_SELECTED_SEMANTIC_PROOF_FAILED',
                'operation_id', p_operation_id::text,
                'candidate_failures', v_semantic_draft_failures,
                'message', 'Draft selection requires positive ordinary entitlement for the same candidate, deductions within selected headroom, and a non-negative candidate result.'
              )::text;
    END IF;
  END IF;


  SELECT COALESCE(jsonb_agg(jsonb_build_object(
           'preview_row_id', CASE WHEN synthetic_total_rows.preview_row_id IS NULL THEN NULL ELSE synthetic_total_rows.preview_row_id::text END,
           'row_key', synthetic_total_rows.row_key,
           'candidate_id', synthetic_total_rows.candidate_id::text,
           'candidate_scope_id', synthetic_total_rows.candidate_scope_id::text,
           'timesheet_id', CASE WHEN synthetic_total_rows.timesheet_id IS NULL THEN NULL ELSE synthetic_total_rows.timesheet_id::text END,
           'key_type', synthetic_total_rows.key_type,
           'key_value', synthetic_total_rows.key_value,
           'allocated_amount', synthetic_total_rows.allocated_amount,
           'reason', 'RESOLVED_SYNTHETIC_TOTAL_ROW_ALLOCATION_BLOCKED'
         ) ORDER BY synthetic_total_rows.row_ordinal, synthetic_total_rows.preview_row_id), '[]'::jsonb)
  INTO v_synthetic_total_allocation_preview_rows
  FROM pg_temp.tmp_pay_workbench_allocation_preview_candidates AS synthetic_total_rows
  WHERE UPPER(BTRIM(COALESCE(synthetic_total_rows.key_type, synthetic_total_rows.row_json#>>'{economic_key,key_type}', ''))) = 'TS_TOTAL'
    AND UPPER(BTRIM(COALESCE(synthetic_total_rows.key_value, synthetic_total_rows.row_json#>>'{economic_key,key_value}', ''))) = 'TOTAL'
    AND LOWER(BTRIM(COALESCE(synthetic_total_rows.source_ref, synthetic_total_rows.row_key, synthetic_total_rows.row_json->>'row_key', synthetic_total_rows.row_json->>'line_key', synthetic_total_rows.row_json->>'source_ref', ''))) LIKE '%:non_segment:total%'
    AND (
      lower(btrim(coalesce(synthetic_total_rows.row_json->>'resolved_segment_rows_replace_source_total', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
      OR lower(btrim(coalesce(synthetic_total_rows.row_json->>'has_resolved_rate', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
      OR lower(btrim(coalesce(synthetic_total_rows.row_json#>>'{case_resolution_summary,has_resolved_rate}', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
      OR lower(btrim(coalesce(synthetic_total_rows.row_json#>>'{case_resolution_summary,resolved_rate_applied}', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
      OR lower(btrim(coalesce(synthetic_total_rows.row_json#>>'{case_resolution_summary,resolved_rate_active}', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
      OR (
        COALESCE(synthetic_total_rows.row_json#>>'{case_resolution_summary,resolved_rate_component_count}', '') ~ '^[0-9]+$'
        AND (synthetic_total_rows.row_json#>>'{case_resolution_summary,resolved_rate_component_count}')::integer > 0
      )
      OR EXISTS (
        SELECT 1
        FROM pg_temp.tmp_pay_workbench_allocation_preview_candidates AS sibling_segment
        WHERE sibling_segment.candidate_id = synthetic_total_rows.candidate_id
          AND sibling_segment.candidate_scope_id = synthetic_total_rows.candidate_scope_id
          AND sibling_segment.timesheet_id IS NOT DISTINCT FROM synthetic_total_rows.timesheet_id
          AND UPPER(BTRIM(COALESCE(sibling_segment.key_type, sibling_segment.row_json#>>'{economic_key,key_type}', ''))) = 'TS_DAY'
          AND LOWER(BTRIM(COALESCE(sibling_segment.source_ref, sibling_segment.row_key, sibling_segment.row_json->>'row_key', sibling_segment.row_json->>'line_key', sibling_segment.row_json->>'source_ref', ''))) LIKE '%:segment:%'
      )
    );

  IF jsonb_array_length(COALESCE(v_synthetic_total_allocation_preview_rows, '[]'::jsonb)) > 0 THEN
    RAISE EXCEPTION 'RESOLVED_SYNTHETIC_TOTAL_ROW_ALLOCATION_BLOCKED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'RESOLVED_SYNTHETIC_TOTAL_ROW_ALLOCATION_BLOCKED',
              'operation_id', p_operation_id::text,
              'synthetic_total_allocation_preview_rows', COALESCE(v_synthetic_total_allocation_preview_rows, '[]'::jsonb),
              'message', 'A stale resolved-timesheet synthetic total row reached allocation scope. Refresh Banking Pay and try Create Draft again.'
            )::text;
  END IF;

  DROP TABLE IF EXISTS pg_temp.tmp_pay_workbench_allocation_expected_rows;
  CREATE TEMPORARY TABLE pg_temp.tmp_pay_workbench_allocation_expected_rows ON COMMIT DROP AS
  WITH selected_preview_rows AS (
    SELECT candidate_row.*
    FROM pg_temp.tmp_pay_workbench_allocation_preview_candidates AS candidate_row
    WHERE candidate_row.rotation_validation_failed IS NOT TRUE
      AND LOWER(BTRIM(COALESCE(candidate_row.preview_contract_json->>'ok', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
      AND LOWER(BTRIM(COALESCE(candidate_row.preview_contract_json->>'selection_allowed', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
      AND candidate_row.preview_row_id IS NOT NULL
      AND COALESCE(candidate_row.backing_table_state_invalid, false) IS NOT TRUE
      AND lower(COALESCE(candidate_row.section, '')) = 'canonical_preview_lines'
      AND UPPER(BTRIM(COALESCE(candidate_row.selection_state, ''))) = 'SELECTED'
      AND UPPER(BTRIM(COALESCE(candidate_row.line_status, ''))) = 'READY'
      AND COALESCE(candidate_row.line_selected, false) IS TRUE
      AND (COALESCE(candidate_row.is_delta_projection, false) IS NOT TRUE OR COALESCE(candidate_row.projection_certified, false) IS TRUE)
      AND COALESCE(candidate_row.policy_x_authority_scope, '') = 'PRE_DRAFT_LIVE_TRUTH'
      AND (candidate_row.timesheet_id IS NOT NULL OR candidate_row.finance_case_id IS NOT NULL)
      AND COALESCE(candidate_row.post_draft_overlay_unavailable, false) IS NOT TRUE
      AND candidate_row.pay_channel IN ('PAYE', 'UMBRELLA')
      AND (
        candidate_row.key_type IN ('TS_DAY','TS_TOTAL','ADDITIONAL_CODE','ADJUSTMENT_CODE','EXPENSE_CODE','MANUAL_CARRY_FORWARD')
        OR (candidate_row.finance_case_id IS NOT NULL AND candidate_row.key_type IN ('CASE_TOTAL','FINANCE_COMPONENT'))
      )
      AND candidate_row.key_value IS NOT NULL
      AND NOT (
        candidate_row.single_fixed_reimbursement_key_type IS NOT NULL
        AND (
          candidate_row.key_type IS DISTINCT FROM candidate_row.single_fixed_reimbursement_key_type
          OR candidate_row.key_value IS DISTINCT FROM candidate_row.single_fixed_reimbursement_key_value
        )
      )
      AND NOT (candidate_row.key_type = 'TS_DAY' AND candidate_row.key_value !~ '^\d{4}-\d{2}-\d{2}$')
      AND candidate_row.allocated_amount IS NOT NULL
      AND ROUND(COALESCE(candidate_row.allocated_amount, 0), 2) <> 0
      AND (
        ROUND(COALESCE(candidate_row.allocated_amount, 0), 2) > 0
        OR (
          candidate_row.allocation_type IN ('OVERPAYMENT_RECOVERY', 'MANUAL_DEBT_RECOVERY', 'PAYMENT_ADVANCE_REPAYMENT', 'LOAN_REPAYMENT')
          AND candidate_row.finance_case_id IS NOT NULL
          AND (candidate_row.item_direction IS NULL OR candidate_row.item_direction = 'DEDUCTION')
        )
      )
  )
  , finance_component_source_rows AS (
    SELECT
      selected_preview_rows.*,
      component_element.ordinality::integer AS finance_component_ordinality,
      component_element.value AS finance_component_json,
      CASE
        WHEN NULLIF(BTRIM(COALESCE(component_element.value->>'finance_component_id', '')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN NULLIF(BTRIM(COALESCE(component_element.value->>'finance_component_id', '')), '')::uuid
        ELSE selected_preview_rows.finance_component_id
      END AS effective_finance_component_id,
      UPPER(COALESCE(
        NULLIF(BTRIM(component_element.value->>'component_key_type'), ''),
        selected_preview_rows.key_type,
        'CASE_TOTAL'
      )) AS effective_key_type,
      COALESCE(
        NULLIF(BTRIM(component_element.value->>'component_key_value'), ''),
        selected_preview_rows.key_value,
        'TOTAL'
      ) AS effective_key_value,
      ROUND(COALESCE(
        CASE WHEN COALESCE(component_element.value->>'preview_due_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ABS((component_element.value->>'preview_due_amount_ex_vat')::numeric) ELSE NULL::numeric END,
        CASE WHEN COALESCE(component_element.value->>'allocated_source_due_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ABS((component_element.value->>'allocated_source_due_amount_ex_vat')::numeric) ELSE NULL::numeric END,
        CASE WHEN COALESCE(component_element.value->>'target_pay_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ABS((component_element.value->>'target_pay_ex_vat')::numeric) ELSE NULL::numeric END,
        CASE WHEN COALESCE(component_element.value->>'ready_preview_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ABS((component_element.value->>'ready_preview_amount_ex_vat')::numeric) ELSE NULL::numeric END,
        CASE WHEN COALESCE(component_element.value->>'preview_component_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ABS((component_element.value->>'preview_component_amount_ex_vat')::numeric) ELSE NULL::numeric END,
        CASE WHEN COALESCE(component_element.value->>'component_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ABS((component_element.value->>'component_amount_ex_vat')::numeric) ELSE NULL::numeric END,
        ABS(COALESCE(selected_preview_rows.allocated_amount, 0))
      ), 2) AS raw_component_abs_amount,
      ROW_NUMBER() OVER (
        PARTITION BY selected_preview_rows.operation_id, selected_preview_rows.candidate_scope_id, selected_preview_rows.preview_row_id
        ORDER BY component_element.ordinality
      ) AS component_rn,
      COUNT(*) OVER (
        PARTITION BY selected_preview_rows.operation_id, selected_preview_rows.candidate_scope_id, selected_preview_rows.preview_row_id
      ) AS component_count,
      ROUND(SUM(ROUND(COALESCE(
        CASE WHEN COALESCE(component_element.value->>'preview_due_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ABS((component_element.value->>'preview_due_amount_ex_vat')::numeric) ELSE NULL::numeric END,
        CASE WHEN COALESCE(component_element.value->>'allocated_source_due_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ABS((component_element.value->>'allocated_source_due_amount_ex_vat')::numeric) ELSE NULL::numeric END,
        CASE WHEN COALESCE(component_element.value->>'target_pay_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ABS((component_element.value->>'target_pay_ex_vat')::numeric) ELSE NULL::numeric END,
        CASE WHEN COALESCE(component_element.value->>'ready_preview_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ABS((component_element.value->>'ready_preview_amount_ex_vat')::numeric) ELSE NULL::numeric END,
        CASE WHEN COALESCE(component_element.value->>'preview_component_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ABS((component_element.value->>'preview_component_amount_ex_vat')::numeric) ELSE NULL::numeric END,
        CASE WHEN COALESCE(component_element.value->>'component_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ABS((component_element.value->>'component_amount_ex_vat')::numeric) ELSE NULL::numeric END,
        ABS(COALESCE(selected_preview_rows.allocated_amount, 0))
      ), 2)) OVER (
        PARTITION BY selected_preview_rows.operation_id, selected_preview_rows.candidate_scope_id, selected_preview_rows.preview_row_id
      ), 2) AS component_abs_amount_sum,
      ROUND(COALESCE(SUM(ROUND(COALESCE(
        CASE WHEN COALESCE(component_element.value->>'preview_due_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ABS((component_element.value->>'preview_due_amount_ex_vat')::numeric) ELSE NULL::numeric END,
        CASE WHEN COALESCE(component_element.value->>'allocated_source_due_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ABS((component_element.value->>'allocated_source_due_amount_ex_vat')::numeric) ELSE NULL::numeric END,
        CASE WHEN COALESCE(component_element.value->>'target_pay_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ABS((component_element.value->>'target_pay_ex_vat')::numeric) ELSE NULL::numeric END,
        CASE WHEN COALESCE(component_element.value->>'ready_preview_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ABS((component_element.value->>'ready_preview_amount_ex_vat')::numeric) ELSE NULL::numeric END,
        CASE WHEN COALESCE(component_element.value->>'preview_component_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ABS((component_element.value->>'preview_component_amount_ex_vat')::numeric) ELSE NULL::numeric END,
        CASE WHEN COALESCE(component_element.value->>'component_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ABS((component_element.value->>'component_amount_ex_vat')::numeric) ELSE NULL::numeric END,
        ABS(COALESCE(selected_preview_rows.allocated_amount, 0))
      ), 2)) OVER (
        PARTITION BY selected_preview_rows.operation_id, selected_preview_rows.candidate_scope_id, selected_preview_rows.preview_row_id
        ORDER BY component_element.ordinality
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
      ), 0), 2) AS preceding_component_abs_amount
    FROM selected_preview_rows
    CROSS JOIN LATERAL jsonb_array_elements(
      CASE
        WHEN selected_preview_rows.finance_case_id IS NOT NULL
         AND jsonb_typeof(selected_preview_rows.row_json->'case_components') = 'array'
         AND jsonb_array_length(selected_preview_rows.row_json->'case_components') > 0
          THEN selected_preview_rows.row_json->'case_components'
        ELSE '[]'::jsonb
      END
    ) WITH ORDINALITY AS component_element(value, ordinality)
    WHERE selected_preview_rows.finance_case_id IS NOT NULL
      AND jsonb_typeof(component_element.value) = 'object'
  ), allocation_expanded_rows AS (
    SELECT
      selected_preview_rows.operation_id,
      selected_preview_rows.candidate_scope_id,
      selected_preview_rows.pay_batch_id,
      selected_preview_rows.candidate_id,
      selected_preview_rows.preview_row_id,
      selected_preview_rows.row_key,
      selected_preview_rows.timesheet_id,
      selected_preview_rows.key_type,
      selected_preview_rows.key_value,
      selected_preview_rows.pay_channel,
      selected_preview_rows.finance_case_id,
      selected_preview_rows.finance_component_id,
      selected_preview_rows.allocation_type,
      selected_preview_rows.source_ref,
      selected_preview_rows.operation_id::text || ':allocation:' || selected_preview_rows.candidate_scope_id::text || ':' || selected_preview_rows.preview_row_id::text AS operation_source_key,
      selected_preview_rows.allocated_amount,
      selected_preview_rows.preview_contract_json,
      selected_preview_rows.item_direction,
      selected_preview_rows.row_json,
      selected_preview_rows.row_ordinal,
      NULL::jsonb AS finance_component_json,
      NULL::integer AS finance_component_ordinality
    FROM selected_preview_rows
    WHERE selected_preview_rows.finance_case_id IS NULL
       OR NOT EXISTS (
         SELECT 1
         FROM finance_component_source_rows AS component_probe
         WHERE component_probe.operation_id = selected_preview_rows.operation_id
           AND component_probe.candidate_scope_id = selected_preview_rows.candidate_scope_id
           AND component_probe.preview_row_id = selected_preview_rows.preview_row_id
       )

    UNION ALL

    SELECT
      finance_component_source_rows.operation_id,
      finance_component_source_rows.candidate_scope_id,
      finance_component_source_rows.pay_batch_id,
      finance_component_source_rows.candidate_id,
      finance_component_source_rows.preview_row_id,
      finance_component_source_rows.row_key,
      finance_component_source_rows.timesheet_id,
      finance_component_source_rows.effective_key_type AS key_type,
      finance_component_source_rows.effective_key_value AS key_value,
      finance_component_source_rows.pay_channel,
      finance_component_source_rows.finance_case_id,
      finance_component_source_rows.effective_finance_component_id AS finance_component_id,
      finance_component_source_rows.allocation_type,
      finance_component_source_rows.source_ref,
      finance_component_source_rows.operation_id::text || ':allocation:' || finance_component_source_rows.candidate_scope_id::text || ':' || finance_component_source_rows.preview_row_id::text || ':component:' || COALESCE(finance_component_source_rows.effective_finance_component_id::text, md5(finance_component_source_rows.finance_component_json::text)) AS operation_source_key,
      CASE
        WHEN ROUND(COALESCE(finance_component_source_rows.allocated_amount, 0), 2) < 0 THEN -1
        ELSE 1
      END * ROUND(
        LEAST(
          COALESCE(finance_component_source_rows.raw_component_abs_amount, 0),
          GREATEST(
            ABS(COALESCE(finance_component_source_rows.allocated_amount, 0))
              - COALESCE(finance_component_source_rows.preceding_component_abs_amount, 0),
            0
          )
        ),
        2
      ) AS allocated_amount,
      finance_component_source_rows.preview_contract_json,
      finance_component_source_rows.item_direction,
      jsonb_strip_nulls(
        finance_component_source_rows.row_json
        || jsonb_build_object(
          'finance_component_id', CASE WHEN finance_component_source_rows.effective_finance_component_id IS NULL THEN NULL ELSE finance_component_source_rows.effective_finance_component_id::text END,
          'component_key_type', finance_component_source_rows.effective_key_type,
          'component_key_value', finance_component_source_rows.effective_key_value,
          'key_type', finance_component_source_rows.effective_key_type,
          'key_value', finance_component_source_rows.effective_key_value,
          'component', finance_component_source_rows.finance_component_json
        )
      ) AS row_json,
      (finance_component_source_rows.row_ordinal * 1000 + finance_component_source_rows.finance_component_ordinality)::bigint AS row_ordinal,
      finance_component_source_rows.finance_component_json,
      finance_component_source_rows.finance_component_ordinality
    FROM finance_component_source_rows
    WHERE ROUND(COALESCE(finance_component_source_rows.raw_component_abs_amount, 0), 2) > 0
  )
  SELECT
    allocation_expanded_rows.operation_id,
    allocation_expanded_rows.candidate_scope_id,
    allocation_expanded_rows.pay_batch_id,
    allocation_expanded_rows.candidate_id,
    allocation_expanded_rows.preview_row_id,
    allocation_expanded_rows.row_key,
    allocation_expanded_rows.timesheet_id,
    allocation_expanded_rows.key_type,
    allocation_expanded_rows.key_value,
    allocation_expanded_rows.pay_channel,
    allocation_expanded_rows.finance_case_id,
    allocation_expanded_rows.finance_component_id,
    allocation_expanded_rows.allocation_type,
    allocation_expanded_rows.source_ref,
    allocation_expanded_rows.operation_source_key,
    allocation_expanded_rows.allocated_amount,
    jsonb_build_object(
      'source', 'banking_pay_workbench_preview_rows',
      'preview_row_id', allocation_expanded_rows.preview_row_id::text,
      'row_key', allocation_expanded_rows.row_key,
      'row_ordinal', allocation_expanded_rows.row_ordinal,
      'timesheet_id', CASE WHEN allocation_expanded_rows.timesheet_id IS NULL THEN NULL ELSE allocation_expanded_rows.timesheet_id::text END,
      'key_type', allocation_expanded_rows.key_type,
      'key_value', allocation_expanded_rows.key_value,
      'allocation_type', allocation_expanded_rows.allocation_type,
      'item_direction', allocation_expanded_rows.item_direction,
      'finance_case_id', CASE WHEN allocation_expanded_rows.finance_case_id IS NULL THEN NULL ELSE allocation_expanded_rows.finance_case_id::text END,
      'finance_component_id', CASE WHEN allocation_expanded_rows.finance_component_id IS NULL THEN NULL ELSE allocation_expanded_rows.finance_component_id::text END,
      'finance_component', allocation_expanded_rows.finance_component_json,
      'economic_key', jsonb_strip_nulls(jsonb_build_object(
        'timesheet_id', CASE WHEN allocation_expanded_rows.timesheet_id IS NULL THEN NULL ELSE allocation_expanded_rows.timesheet_id::text END,
        'key_type', allocation_expanded_rows.key_type,
        'key_value', allocation_expanded_rows.key_value
      )),
      'preview_contract', allocation_expanded_rows.preview_contract_json,
      'line', jsonb_strip_nulls(
        allocation_expanded_rows.row_json
        || jsonb_build_object(
          'row_key', allocation_expanded_rows.row_key,
          'preview_row_pk', allocation_expanded_rows.preview_row_id::text,
          'selection_state', 'SELECTED'
        )
      )
    ) AS allocation_basis_json,
    row_number() OVER (
      PARTITION BY
        allocation_expanded_rows.operation_id,
        allocation_expanded_rows.candidate_scope_id
      ORDER BY
        allocation_expanded_rows.row_ordinal,
        allocation_expanded_rows.preview_row_id,
        allocation_expanded_rows.operation_source_key
    )::integer AS sort_order
  FROM allocation_expanded_rows
  WHERE ROUND(COALESCE(allocation_expanded_rows.allocated_amount, 0), 2) <> 0;


  SELECT COALESCE(jsonb_agg(jsonb_build_object(
           'preview_row_id', synthetic_total_rows.preview_row_id::text,
           'row_key', synthetic_total_rows.row_key,
           'candidate_id', synthetic_total_rows.candidate_id::text,
           'candidate_scope_id', synthetic_total_rows.candidate_scope_id::text,
           'timesheet_id', CASE WHEN synthetic_total_rows.timesheet_id IS NULL THEN NULL ELSE synthetic_total_rows.timesheet_id::text END,
           'key_type', synthetic_total_rows.key_type,
           'key_value', synthetic_total_rows.key_value,
           'allocated_amount', synthetic_total_rows.allocated_amount,
           'reason', 'RESOLVED_SYNTHETIC_TOTAL_ROW_ALLOCATION_BLOCKED'
         ) ORDER BY synthetic_total_rows.sort_order, synthetic_total_rows.preview_row_id), '[]'::jsonb)
  INTO v_synthetic_total_allocation_preview_rows
  FROM pg_temp.tmp_pay_workbench_allocation_expected_rows AS synthetic_total_rows
  WHERE UPPER(BTRIM(COALESCE(synthetic_total_rows.key_type, synthetic_total_rows.allocation_basis_json#>>'{economic_key,key_type}', ''))) = 'TS_TOTAL'
    AND UPPER(BTRIM(COALESCE(synthetic_total_rows.key_value, synthetic_total_rows.allocation_basis_json#>>'{economic_key,key_value}', ''))) = 'TOTAL'
    AND LOWER(BTRIM(COALESCE(synthetic_total_rows.source_ref, synthetic_total_rows.row_key, synthetic_total_rows.allocation_basis_json#>>'{line,row_key}', synthetic_total_rows.allocation_basis_json#>>'{line,line_key}', synthetic_total_rows.allocation_basis_json#>>'{line,source_ref}', ''))) LIKE '%:non_segment:total%'
    AND (
      lower(btrim(coalesce(synthetic_total_rows.allocation_basis_json#>>'{line,resolved_segment_rows_replace_source_total}', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
      OR lower(btrim(coalesce(synthetic_total_rows.allocation_basis_json#>>'{line,has_resolved_rate}', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
      OR lower(btrim(coalesce(synthetic_total_rows.allocation_basis_json#>>'{line,case_resolution_summary,has_resolved_rate}', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
      OR lower(btrim(coalesce(synthetic_total_rows.allocation_basis_json#>>'{line,case_resolution_summary,resolved_rate_applied}', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
      OR lower(btrim(coalesce(synthetic_total_rows.allocation_basis_json#>>'{line,case_resolution_summary,resolved_rate_active}', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
      OR (
        COALESCE(synthetic_total_rows.allocation_basis_json#>>'{line,case_resolution_summary,resolved_rate_component_count}', '') ~ '^[0-9]+$'
        AND (synthetic_total_rows.allocation_basis_json#>>'{line,case_resolution_summary,resolved_rate_component_count}')::integer > 0
      )
      OR EXISTS (
        SELECT 1
        FROM pg_temp.tmp_pay_workbench_allocation_expected_rows AS sibling_segment
        WHERE sibling_segment.candidate_id = synthetic_total_rows.candidate_id
          AND sibling_segment.candidate_scope_id = synthetic_total_rows.candidate_scope_id
          AND sibling_segment.timesheet_id IS NOT DISTINCT FROM synthetic_total_rows.timesheet_id
          AND UPPER(BTRIM(COALESCE(sibling_segment.key_type, sibling_segment.allocation_basis_json#>>'{economic_key,key_type}', ''))) = 'TS_DAY'
          AND LOWER(BTRIM(COALESCE(sibling_segment.source_ref, sibling_segment.row_key, sibling_segment.allocation_basis_json#>>'{line,row_key}', sibling_segment.allocation_basis_json#>>'{line,line_key}', sibling_segment.allocation_basis_json#>>'{line,source_ref}', ''))) LIKE '%:segment:%'
      )
    );

  IF jsonb_array_length(COALESCE(v_synthetic_total_allocation_preview_rows, '[]'::jsonb)) > 0 THEN
    RAISE EXCEPTION 'RESOLVED_SYNTHETIC_TOTAL_ROW_ALLOCATION_BLOCKED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'RESOLVED_SYNTHETIC_TOTAL_ROW_ALLOCATION_BLOCKED',
              'operation_id', p_operation_id::text,
              'synthetic_total_allocation_preview_rows', COALESCE(v_synthetic_total_allocation_preview_rows, '[]'::jsonb),
              'message', 'A stale resolved-timesheet synthetic total row reached allocation. Refresh Banking Pay and try Create Draft again.'
            )::text;
  END IF;

  SELECT COUNT(*)::integer
  INTO v_expected_count
  FROM pg_temp.tmp_pay_workbench_allocation_expected_rows AS expected_row_count;

  IF COALESCE(v_expected_count, 0) = 0 AND EXISTS (
    SELECT 1
    FROM pg_temp.tmp_pay_workbench_allocation_scope_rows AS selected_scope
    WHERE COALESCE(selected_scope.candidate_totals_json->>'selected_row_count_seeded_in_page', '0') ~ '^[0-9]+$'
      AND (selected_scope.candidate_totals_json->>'selected_row_count_seeded_in_page')::integer > 0
      AND NOT EXISTS (
        SELECT 1
        FROM public.banking_pay_operation_candidate_allocation_rows AS existing_row
        WHERE existing_row.operation_id = p_operation_id
          AND existing_row.candidate_scope_id = selected_scope.id
      )
  ) THEN
    RAISE EXCEPTION 'DRAFT_ALLOCATION_ROWS_EMPTY'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'DRAFT_ALLOCATION_ROWS_EMPTY',
              'operation_id', p_operation_id::text,
              'candidate_scopes_processed', COALESCE(v_candidate_scopes_processed, 0),
              'message', 'Row-backed candidate scope did not contain draftable selected preview lines for allocation.'
            )::text;
  END IF;

  SELECT COUNT(*)::integer
  INTO v_reused
  FROM public.banking_pay_operation_candidate_allocation_rows AS existing_row
  WHERE existing_row.operation_id = p_operation_id
    AND existing_row.candidate_scope_id IN (SELECT selected_scope.id FROM pg_temp.tmp_pay_workbench_allocation_scope_rows AS selected_scope);

  IF COALESCE(v_expected_count, 0) > 0 THEN
    WITH inserted_rows AS (
      INSERT INTO public.banking_pay_operation_candidate_allocation_rows (
        operation_id,
        candidate_scope_id,
        pay_batch_id,
        candidate_id,
        pay_channel,
        finance_case_id,
        finance_component_id,
        allocation_type,
        source_ref,
        operation_source_key,
        allocated_amount,
        allocation_basis_json,
        sort_order,
        status,
        created_at_utc,
        updated_at_utc
      )
      SELECT
        expected_row.operation_id,
        expected_row.candidate_scope_id,
        expected_row.pay_batch_id,
        expected_row.candidate_id,
        expected_row.pay_channel,
        expected_row.finance_case_id,
        expected_row.finance_component_id,
        expected_row.allocation_type,
        expected_row.source_ref,
        expected_row.operation_source_key,
        expected_row.allocated_amount,
        expected_row.allocation_basis_json,
        expected_row.sort_order,
        'PENDING',
        v_now,
        v_now
      FROM pg_temp.tmp_pay_workbench_allocation_expected_rows AS expected_row
      ON CONFLICT (operation_id, operation_source_key) DO NOTHING
      RETURNING public.banking_pay_operation_candidate_allocation_rows.id
    )
    SELECT COUNT(*)::integer
    INTO v_inserted
    FROM inserted_rows;
  ELSE
    v_inserted := 0;
  END IF;

  SELECT COALESCE(jsonb_agg(jsonb_build_object(
           'allocation_source_key', plan_row.allocation_source_key,
           'existing_plan_digest', existing_row.allocation_basis_json#>>'{draft_finance_item_plan,plan_digest}',
           'current_plan_digest', plan_row.plan_digest
         ) ORDER BY plan_row.allocation_source_key), '[]'::jsonb)
  INTO v_plan_drift_details
  FROM private.pay_workbench_draft_finance_item_plan_v1(p_operation_id, v_plan_scope_ids) AS plan_row
  JOIN public.banking_pay_operation_candidate_allocation_rows AS existing_row
    ON existing_row.operation_id = plan_row.operation_id
   AND existing_row.operation_source_key = plan_row.allocation_source_key
  WHERE existing_row.allocation_basis_json ? 'draft_finance_item_plan'
    AND NULLIF(existing_row.allocation_basis_json#>>'{draft_finance_item_plan,plan_digest}', '')
        IS DISTINCT FROM plan_row.plan_digest;

  IF jsonb_array_length(COALESCE(v_plan_drift_details, '[]'::jsonb)) > 0 THEN
    RAISE EXCEPTION 'DRAFT_FINANCE_ITEM_PLAN_DRIFT'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'DRAFT_FINANCE_ITEM_PLAN_DRIFT',
              'operation_id', p_operation_id::text,
              'mismatches', v_plan_drift_details,
              'message', 'Frozen Draft finance allocation evidence no longer matches its canonical item plan.'
            )::text;
  END IF;

  WITH current_plan AS (
    SELECT *
    FROM private.pay_workbench_draft_finance_item_plan_v1(p_operation_id, v_plan_scope_ids)
  )
  UPDATE public.banking_pay_operation_candidate_allocation_rows AS allocation_update
  SET allocation_basis_json = COALESCE(allocation_update.allocation_basis_json, '{}'::jsonb)
        || jsonb_build_object(
             'draft_finance_item_plan',
             jsonb_build_object(
               'contract_version', 1,
               'planned_item_key', current_plan.planned_item_key,
               'planned_item_type', current_plan.planned_item_type,
               'contribution_amount', current_plan.contribution_amount,
               'planned_item_amount', current_plan.planned_item_amount,
               'contribution_count', current_plan.contribution_count,
               'plan_digest', current_plan.plan_digest
             )
           ),
      updated_at_utc = v_now
  FROM current_plan
  WHERE allocation_update.operation_id = current_plan.operation_id
    AND allocation_update.operation_source_key = current_plan.allocation_source_key
    AND allocation_update.allocation_basis_json#>>'{draft_finance_item_plan,plan_digest}' IS DISTINCT FROM current_plan.plan_digest;

  UPDATE public.banking_pay_operation_candidate_scope AS scope_update
  SET status = CASE
        WHEN EXISTS (
          SELECT 1
          FROM public.banking_pay_operation_candidate_allocation_rows AS existing_row
          WHERE existing_row.operation_id = scope_update.operation_id
            AND existing_row.candidate_scope_id = scope_update.id
        ) THEN 'ALLOCATED'
        ELSE scope_update.status
      END,
      updated_at_utc = v_now
  WHERE scope_update.operation_id = p_operation_id
    AND EXISTS (
      SELECT 1
      FROM pg_temp.tmp_pay_workbench_allocation_scope_rows AS selected_scope
      WHERE selected_scope.id = scope_update.id
    );

  UPDATE public.banking_pay_operations AS operation_update
  SET updated_at_utc = v_now
  WHERE operation_update.id = p_operation_id;

  RETURN QUERY
  SELECT
    COALESCE(v_candidate_scopes_processed, 0),
    COALESCE(v_inserted, 0),
    COALESCE(v_reused, 0),
    0::integer;
END;
$function$;


ALTER FUNCTION public.pay_workbench_prepare_draft_allocation_rows_seed(uuid,jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_prepare_draft_allocation_rows_seed(uuid,jsonb) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.pay_workbench_prepare_draft_allocation_rows_seed(uuid,jsonb) TO service_role;

-- Exact current-owner replacement: only the selected-line transport gains the
-- certified V8 row-backed branch. Context derivation, PAYE/Umbrella handling,
-- finance facts, settings and every legacy-array outcome remain unchanged.
create or replace function public.pay_batch_stage_operation_candidate_chunk_context(
    p_operation_id uuid,
    p_pay_batch_id uuid,
    p_candidate_scope_ids jsonb,
    p_actor_user_id uuid
)
returns table (
    candidate_count integer,
    selected_row_count integer,
    timesheet_snapshot_count integer,
    finance_component_count integer
)
language plpgsql
security definer
volatile
set search_path = public, pg_temp
as $$
declare
    v_operation public.banking_pay_operations%rowtype;
    v_pay_batch public.pay_batches%rowtype;
    v_scope_ids jsonb;
    v_candidate_count integer;
    v_selected_row_count integer;
    v_timesheet_snapshot_count integer;
    v_finance_component_count integer;
    v_settings_bank_system text;
    v_settings_external_paye_system text;
    v_settings_rail_provider text;
    v_settings_rail_env text;
    v_need_name_check boolean;
    v_requires_payee_map boolean;
    v_vat_rate_pct numeric;
    v_erni_pct numeric;
    v_week_start date;
    v_scope text;
begin
    v_scope_ids := coalesce(p_candidate_scope_ids, '[]'::jsonb);

    if p_actor_user_id is null then
        raise exception 'pay_batch_stage_operation_candidate_chunk_context requires p_actor_user_id';
    end if;

    perform 1
    from public.tms_users as actor_user
    where actor_user.id = p_actor_user_id;

    if not found then
        raise exception 'pay_batch_stage_operation_candidate_chunk_context tms_users row not found: %', p_actor_user_id;
    end if;

    if jsonb_typeof(v_scope_ids) <> 'array' then
        raise exception 'pay_batch_stage_operation_candidate_chunk_context requires p_candidate_scope_ids to be a JSON array';
    end if;

    if jsonb_array_length(v_scope_ids) = 0 then
        raise exception 'pay_batch_stage_operation_candidate_chunk_context requires at least one candidate scope id';
    end if;

    if exists (
        select 1
        from jsonb_array_elements(v_scope_ids) as supplied_scope(scope_value)
        where not ((supplied_scope.scope_value #>> '{}') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
    ) then
        raise exception 'pay_batch_stage_operation_candidate_chunk_context requires candidate scope ids to be UUID strings';
    end if;

    select operation_row.*
    into v_operation
    from public.banking_pay_operations as operation_row
    where operation_row.id = p_operation_id
    for update;

    if not found then
        raise exception 'pay_batch_stage_operation_candidate_chunk_context operation not found: %', p_operation_id;
    end if;

    if v_operation.operation_type <> 'DRAFT_CREATE' then
        raise exception 'pay_batch_stage_operation_candidate_chunk_context expected DRAFT_CREATE operation %, got %', p_operation_id, v_operation.operation_type;
    end if;

    if v_operation.status in ('COMPLETE', 'FAILED', 'CANCELLED', 'REVIEW_REQUIRED') then
        raise exception 'pay_batch_stage_operation_candidate_chunk_context cannot stage terminal operation % with status %', p_operation_id, v_operation.status;
    end if;

    if v_operation.actor_user_id is not null and v_operation.actor_user_id <> p_actor_user_id then
        raise exception 'pay_batch_stage_operation_candidate_chunk_context operation % belongs to a different actor', p_operation_id;
    end if;

    select batch_row.*
    into v_pay_batch
    from public.pay_batches as batch_row
    where batch_row.id = p_pay_batch_id
    for update;

    if not found then
        raise exception 'pay_batch_stage_operation_candidate_chunk_context pay batch not found: %', p_pay_batch_id;
    end if;

    if v_pay_batch.status <> 'DRAFT' then
        raise exception 'pay_batch_stage_operation_candidate_chunk_context pay batch % is not DRAFT: %', p_pay_batch_id, v_pay_batch.status;
    end if;

    if v_pay_batch.source_workbench_session_id is not null
       and v_operation.workbench_session_id is not null
       and v_pay_batch.source_workbench_session_id <> v_operation.workbench_session_id then
        raise exception 'pay_batch_stage_operation_candidate_chunk_context pay batch % belongs to a different workbench session', p_pay_batch_id;
    end if;

    drop table if exists pg_temp.tmp_pay_build_operation_candidate_scope;
    create temporary table pg_temp.tmp_pay_build_operation_candidate_scope as
    select scope_row.*
    from public.banking_pay_operation_candidate_scope as scope_row
    where scope_row.operation_id = p_operation_id
      and scope_row.pay_batch_id = p_pay_batch_id
      and scope_row.id in (
          select (supplied_scope.scope_value #>> '{}')::uuid
          from jsonb_array_elements(v_scope_ids) as supplied_scope(scope_value)
      )
    order by scope_row.chunk_sequence nulls last, scope_row.pay_channel, scope_row.candidate_id;

    select count(*)::integer
    into v_candidate_count
    from pg_temp.tmp_pay_build_operation_candidate_scope as staged_scope;

    if coalesce(v_candidate_count, 0) = 0 then
        raise exception 'pay_batch_stage_operation_candidate_chunk_context found no candidate scope rows for operation %, batch %', p_operation_id, p_pay_batch_id;
    end if;

    if coalesce(v_candidate_count, 0) <> jsonb_array_length(v_scope_ids) then
        raise exception 'pay_batch_stage_operation_candidate_chunk_context one or more candidate scope ids do not belong to operation % and batch %', p_operation_id, p_pay_batch_id;
    end if;

    select upper(btrim(coalesce(staged_scope.pay_channel, '')))
    into v_scope
    from pg_temp.tmp_pay_build_operation_candidate_scope as staged_scope
    where upper(btrim(coalesce(staged_scope.pay_channel, ''))) in ('PAYE', 'UMBRELLA')
    group by upper(btrim(coalesce(staged_scope.pay_channel, '')))
    order by upper(btrim(coalesce(staged_scope.pay_channel, '')))
    limit 1;

    if v_scope is null then
        raise exception 'pay_batch_stage_operation_candidate_chunk_context cannot determine pay channel scope for operation %', p_operation_id;
    end if;

    if (
        select count(*)::integer
        from (
            select distinct upper(btrim(coalesce(staged_scope.pay_channel, ''))) as pay_channel_scope
            from pg_temp.tmp_pay_build_operation_candidate_scope as staged_scope
            where upper(btrim(coalesce(staged_scope.pay_channel, ''))) in ('PAYE', 'UMBRELLA')
        ) as pay_channel_scope_rows
    ) <> 1 then
        raise exception 'pay_batch_stage_operation_candidate_chunk_context candidate scope chunk must contain exactly one pay channel scope';
    end if;

    v_week_start := public._pay_week_start_monday(v_pay_batch.pay_date);

    select coalesce(nullif(v_pay_batch.banking_system_snapshot, ''), settings_row.banking_system),
           coalesce(nullif(v_pay_batch.external_paye_system_snapshot, ''), settings_row.external_paye_system),
           coalesce(nullif(v_pay_batch.rail_provider_snapshot, ''), settings_row.rail_provider_default),
           coalesce(nullif(v_pay_batch.rail_env_snapshot, ''), settings_row.rail_env_default),
           (coalesce(settings_row.rail_supports_name_check, false) = true and upper(btrim(coalesce(coalesce(nullif(v_pay_batch.rail_provider_snapshot, ''), settings_row.rail_provider_default), ''))) <> 'CSV') as need_name_check,
           (upper(btrim(coalesce(coalesce(nullif(v_pay_batch.rail_provider_snapshot, ''), settings_row.rail_provider_default), ''))) <> 'CSV') as requires_payee_map
    into v_settings_bank_system,
         v_settings_external_paye_system,
         v_settings_rail_provider,
         v_settings_rail_env,
         v_need_name_check,
         v_requires_payee_map
    from public.settings_defaults as settings_row
    order by settings_row.id asc
    limit 1;

    if v_settings_bank_system is null or v_settings_external_paye_system is null then
        raise exception 'pay_batch_stage_operation_candidate_chunk_context settings_defaults missing banking_system/external_paye_system';
    end if;

    if v_settings_rail_provider is null or v_settings_rail_env is null then
        raise exception 'pay_batch_stage_operation_candidate_chunk_context settings_defaults missing rail provider/environment';
    end if;

    select finance_window.vat_rate_pct,
           finance_window.erni_pct
    into v_vat_rate_pct,
         v_erni_pct
    from public.settings_finance_windows as finance_window
    where v_pay_batch.pay_date >= finance_window.date_from
      and v_pay_batch.pay_date <= coalesce(finance_window.date_to, 'infinity'::date)
    order by finance_window.date_from desc
    limit 1;

    if v_vat_rate_pct is null or v_erni_pct is null then
        raise exception 'pay_batch_stage_operation_candidate_chunk_context no matching finance window for pay date %', v_pay_batch.pay_date;
    end if;

    create temporary table if not exists pg_temp.tmp_pay_build_settings_context (
        banking_system text,
        external_paye_system text,
        rail_provider_default text,
        rail_env_default text,
        need_name_check boolean,
        requires_payee_map boolean,
        vat_rate_pct numeric,
        erni_pct numeric,
        pay_week_start date
    ) on commit drop;
    truncate table pg_temp.tmp_pay_build_settings_context;

    insert into pg_temp.tmp_pay_build_settings_context (
        banking_system,
        external_paye_system,
        rail_provider_default,
        rail_env_default,
        need_name_check,
        requires_payee_map,
        vat_rate_pct,
        erni_pct,
        pay_week_start
    ) values (
        v_settings_bank_system,
        v_settings_external_paye_system,
        v_settings_rail_provider,
        v_settings_rail_env,
        v_need_name_check,
        v_requires_payee_map,
        v_vat_rate_pct,
        v_erni_pct,
        v_week_start
    );

    create temporary table if not exists pg_temp.tmp_pay_build_preview_candidates (
        cand jsonb not null
    ) on commit drop;
    truncate table pg_temp.tmp_pay_build_preview_candidates;

    insert into pg_temp.tmp_pay_build_preview_candidates(cand)
    select candidate_json.cand
    from (
        select staged_scope.effective_paye_candidate_json as cand
        from pg_temp.tmp_pay_build_operation_candidate_scope as staged_scope
        where jsonb_typeof(staged_scope.effective_paye_candidate_json) = 'object'
          and nullif(btrim(coalesce(staged_scope.effective_paye_candidate_json->>'candidate_id', '')), '') is not null
        union all
        select staged_scope.effective_non_paye_payee_json as cand
        from pg_temp.tmp_pay_build_operation_candidate_scope as staged_scope
        where jsonb_typeof(staged_scope.effective_non_paye_payee_json) = 'object'
          and nullif(btrim(coalesce(staged_scope.effective_non_paye_payee_json->>'candidate_id', '')), '') is not null
    ) as candidate_json;

    create temporary table if not exists pg_temp.tmp_pay_build_selected_preview_rows (
        preview_row_id text primary key,
        candidate_id uuid not null,
        finance_case_id uuid null,
        timesheet_id uuid null,
        client_id uuid null,
        line_type text null,
        case_type text null,
        pay_channel text null,
        paye_treatment text null,
        routing_kind text null,
        destination_label text null,
        taxability text null,
        beneficiary_name text null,
        masked_bank_account text null,
        bank_details_hash text null,
        item_direction text null,
        item_type_label text null,
        source_ref text null,
        preview_amount_ex_vat numeric(12,2) null,
        draftable boolean not null,
        snooze_allowed boolean not null,
        is_candidate_directed_oneoff_payout boolean not null,
        appears_on_umbrella_remittance boolean not null,
        generates_candidate_payment_advice boolean not null,
        case_components_json jsonb null
    ) on commit drop;
    truncate table pg_temp.tmp_pay_build_selected_preview_rows;

    insert into pg_temp.tmp_pay_build_selected_preview_rows (
        preview_row_id,
        candidate_id,
        finance_case_id,
        timesheet_id,
        client_id,
        line_type,
        case_type,
        pay_channel,
        paye_treatment,
        routing_kind,
        destination_label,
        taxability,
        beneficiary_name,
        masked_bank_account,
        bank_details_hash,
        item_direction,
        item_type_label,
        source_ref,
        preview_amount_ex_vat,
        draftable,
        snooze_allowed,
        is_candidate_directed_oneoff_payout,
        appears_on_umbrella_remittance,
        generates_candidate_payment_advice,
        case_components_json
    )
    select distinct on (line_rows.preview_row_id)
        line_rows.preview_row_id,
        line_rows.candidate_id,
        line_rows.finance_case_id,
        line_rows.timesheet_id,
        line_rows.client_id,
        line_rows.line_type,
        line_rows.case_type,
        line_rows.pay_channel,
        line_rows.paye_treatment,
        line_rows.routing_kind,
        line_rows.destination_label,
        line_rows.taxability,
        line_rows.beneficiary_name,
        line_rows.masked_bank_account,
        line_rows.bank_details_hash,
        line_rows.item_direction,
        line_rows.item_type_label,
        line_rows.source_ref,
        line_rows.preview_amount_ex_vat,
        line_rows.draftable,
        line_rows.snooze_allowed,
        line_rows.is_candidate_directed_oneoff_payout,
        line_rows.appears_on_umbrella_remittance,
        line_rows.generates_candidate_payment_advice,
        line_rows.case_components_json
    from (
        select
            coalesce(nullif(btrim(coalesce(line_element.value->>'preview_row_id', '')), ''), nullif(btrim(coalesce(line_element.value->>'line_id', '')), ''), nullif(btrim(coalesce(line_element.value->>'row_id', '')), ''), nullif(btrim(coalesce(line_element.value->>'id', '')), '')) as preview_row_id,
            case when coalesce(line_element.value->>'candidate_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then (line_element.value->>'candidate_id')::uuid else staged_scope.candidate_id end as candidate_id,
            case when coalesce(line_element.value->>'finance_case_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then (line_element.value->>'finance_case_id')::uuid else null::uuid end as finance_case_id,
            case when coalesce(line_element.value->>'timesheet_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then (line_element.value->>'timesheet_id')::uuid else null::uuid end as timesheet_id,
            case when coalesce(line_element.value->>'client_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then (line_element.value->>'client_id')::uuid else null::uuid end as client_id,
            nullif(btrim(coalesce(line_element.value->>'line_type', '')), '') as line_type,
            nullif(btrim(coalesce(line_element.value->>'case_type', '')), '') as case_type,
            upper(btrim(coalesce(line_element.value->>'pay_channel', staged_scope.pay_channel, ''))) as pay_channel,
            upper(btrim(coalesce(line_element.value->>'paye_treatment', ''))) as paye_treatment,
            nullif(btrim(coalesce(line_element.value->>'routing_kind', '')), '') as routing_kind,
            nullif(btrim(coalesce(line_element.value->>'destination_label', '')), '') as destination_label,
            nullif(btrim(coalesce(line_element.value->>'taxability', '')), '') as taxability,
            nullif(btrim(coalesce(line_element.value->>'beneficiary_name', '')), '') as beneficiary_name,
            nullif(btrim(coalesce(line_element.value->>'masked_bank_account', '')), '') as masked_bank_account,
            nullif(btrim(coalesce(line_element.value->>'bank_details_hash', '')), '') as bank_details_hash,
            nullif(btrim(coalesce(line_element.value->>'item_direction', '')), '') as item_direction,
            nullif(btrim(coalesce(line_element.value->>'item_type_label', '')), '') as item_type_label,
            nullif(btrim(coalesce(line_element.value->>'source_ref', '')), '') as source_ref,
            round(coalesce(
                case when coalesce(line_element.value->>'amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then (line_element.value->>'amount_ex_vat')::numeric else null::numeric end,
                case when coalesce(line_element.value->>'preview_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then (line_element.value->>'preview_amount_ex_vat')::numeric else null::numeric end,
                0::numeric
            ), 2)::numeric(12,2) as preview_amount_ex_vat,
            coalesce(case when lower(coalesce(line_element.value->>'draftable', '')) in ('true', 'false') then (line_element.value->>'draftable')::boolean else null::boolean end, true) as draftable,
            coalesce(case when lower(coalesce(line_element.value->>'snooze_allowed', '')) in ('true', 'false') then (line_element.value->>'snooze_allowed')::boolean else null::boolean end, false) as snooze_allowed,
            coalesce(case when lower(coalesce(line_element.value->>'is_candidate_directed_oneoff_payout', '')) in ('true', 'false') then (line_element.value->>'is_candidate_directed_oneoff_payout')::boolean else null::boolean end, false) as is_candidate_directed_oneoff_payout,
            coalesce(case when lower(coalesce(line_element.value->>'appears_on_umbrella_remittance', '')) in ('true', 'false') then (line_element.value->>'appears_on_umbrella_remittance')::boolean else null::boolean end, false) as appears_on_umbrella_remittance,
            coalesce(case when lower(coalesce(line_element.value->>'generates_candidate_payment_advice', '')) in ('true', 'false') then (line_element.value->>'generates_candidate_payment_advice')::boolean else null::boolean end, false) as generates_candidate_payment_advice,
            case
              when upper(btrim(coalesce(line_element.value->>'line_type', ''))) in ('OVERPAYMENT_RECOVERY', 'UNDERPAYMENT_PAYMENT')
               and jsonb_typeof(coalesce(line_element.value->'case_components', line_element.value->'components')) = 'array'
               and jsonb_array_length(coalesce(line_element.value->'case_components', line_element.value->'components')) = 1
              then (
                select jsonb_build_array(
                  component_element.value
                  || coalesce(line_element.value->'frozen_component_snapshot_json', '{}'::jsonb)
                  || jsonb_strip_nulls(jsonb_build_object(
                    'finance_component_id', line_element.value->>'finance_component_id',
                    'component_key_type', coalesce(
                      line_element.value->>'frozen_component_key_type',
                      line_element.value->>'component_key_type'
                    ),
                    'component_key_value', coalesce(
                      line_element.value->>'frozen_component_key_value',
                      line_element.value->>'component_key_value'
                    ),
                    'classification', coalesce(
                      line_element.value->>'frozen_component_classification',
                      line_element.value->>'classification'
                    ),
                    'source_pay_method', coalesce(
                      line_element.value#>>'{frozen_component_snapshot_json,source_pay_method}',
                      line_element.value->>'frozen_source_pay_method'
                    ),
                    'source_basis_json', coalesce(
                      line_element.value->'frozen_source_basis_json',
                      line_element.value->'source_basis_json'
                    ),
                    'saved_resolution_mode', coalesce(
                      line_element.value->>'frozen_resolution_mode',
                      line_element.value->>'saved_resolution_mode'
                    ),
                    'saved_resolution_payload_json', coalesce(
                      line_element.value->'frozen_resolution_payload_json',
                      line_element.value->'saved_resolution_payload_json',
                      line_element.value->'resolution_payload_json'
                    ),
                    'saved_resolution_result_json', coalesce(
                      line_element.value->'frozen_resolution_result_json',
                      line_element.value->'saved_resolution_result_json',
                      line_element.value->'resolution_result_json'
                    ),
                    'source_amount', coalesce(
                      line_element.value->>'frozen_source_amount',
                      line_element.value->>'source_amount_ex_vat',
                      line_element.value->>'source_amount'
                    ),
                    'remaining_source_amount', coalesce(
                      component_element.value->>'remaining_source_amount',
                      line_element.value->>'remaining_source_amount',
                      line_element.value->>'frozen_source_amount',
                      line_element.value->>'source_amount_ex_vat'
                    ),
                    'preview_due_amount_ex_vat', coalesce(
                      component_element.value->>'preview_due_amount_ex_vat',
                      line_element.value->>'preview_amount_ex_vat',
                      line_element.value->>'amount_ex_vat'
                    ),
                    'target_pay_ex_vat', coalesce(
                      component_element.value->>'target_pay_ex_vat',
                      line_element.value->>'target_pay_ex_vat',
                      line_element.value->>'preview_amount_ex_vat',
                      line_element.value->>'amount_ex_vat'
                    )
                  ))
                )
                from jsonb_array_elements(
                  coalesce(line_element.value->'case_components', line_element.value->'components')
                ) as component_element(value)
              )
              else coalesce(line_element.value->'case_components', line_element.value->'components', '[]'::jsonb)
            end as case_components_json,
            line_element.ord as line_ordinal
        from pg_temp.tmp_pay_build_operation_candidate_scope as staged_scope
        cross join lateral private.pay_workbench_draft_scope_line_rows_v8(
          staged_scope.id,
          staged_scope.selected_canonical_preview_lines_json,
          staged_scope.effective_canonical_preview_lines_json
        ) as line_element(value, ord)
        where jsonb_typeof(line_element.value) = 'object'
    ) as line_rows
    where line_rows.preview_row_id is not null
      and line_rows.candidate_id is not null
      and line_rows.draftable = true
    order by line_rows.preview_row_id, line_rows.line_ordinal;

    select count(*)::integer
    into v_selected_row_count
    from pg_temp.tmp_pay_build_selected_preview_rows as selected_row;

    if coalesce(v_selected_row_count, 0) = 0 then
        raise exception 'pay_batch_stage_operation_candidate_chunk_context staged no selected preview rows for operation %, batch %', p_operation_id, p_pay_batch_id;
    end if;

    create temporary table if not exists pg_temp.tmp_pay_build_hidden_template_input_rows (
        candidate_id uuid null,
        finance_case_id uuid null,
        recovery_family text null,
        paye_treatment text null,
        raw_pay_channel text null,
        normalized_pay_channel text null,
        umbrella_id uuid null,
        source_ref text null,
        finance_component_id uuid null,
        frozen_component_key_type text null,
        frozen_component_key_value text null,
        frozen_component_snapshot_json jsonb null,
        frozen_component_classification public.pay_finance_component_classification_enum null,
        frozen_source_basis_json jsonb null,
        frozen_source_pay_method text null,
        frozen_target_pay_method text null,
        frozen_resolution_mode public.pay_finance_component_resolution_mode_enum null,
        frozen_resolution_payload_json jsonb null,
        frozen_resolution_result_json jsonb null,
        frozen_source_amount numeric(12,2) null,
        frozen_outstanding_amount numeric(12,2) null,
        weekly_due numeric(12,2) null,
        minimum_earnings_threshold numeric null,
        take_home_floor_override numeric null,
        default_take_home_floor numeric null,
        payout_status text null,
        next_due_week_start date null,
        sort_order integer null,
        required_identifiers_present boolean not null,
        candidate_selected boolean not null,
        pay_channel_matches_scope boolean not null
    ) on commit drop;
    truncate table pg_temp.tmp_pay_build_hidden_template_input_rows;

    insert into pg_temp.tmp_pay_build_hidden_template_input_rows (
        candidate_id,
        finance_case_id,
        recovery_family,
        paye_treatment,
        raw_pay_channel,
        normalized_pay_channel,
        umbrella_id,
        source_ref,
        finance_component_id,
        frozen_component_key_type,
        frozen_component_key_value,
        frozen_component_snapshot_json,
        frozen_component_classification,
        frozen_source_basis_json,
        frozen_source_pay_method,
        frozen_target_pay_method,
        frozen_resolution_mode,
        frozen_resolution_payload_json,
        frozen_resolution_result_json,
        frozen_source_amount,
        frozen_outstanding_amount,
        weekly_due,
        minimum_earnings_threshold,
        take_home_floor_override,
        default_take_home_floor,
        payout_status,
        next_due_week_start,
        sort_order,
        required_identifiers_present,
        candidate_selected,
        pay_channel_matches_scope
    )
    select
        case when coalesce(template_element.value->>'candidate_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then (template_element.value->>'candidate_id')::uuid else staged_scope.candidate_id end,
        case when coalesce(template_element.value->>'finance_case_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then (template_element.value->>'finance_case_id')::uuid else null::uuid end,
        upper(btrim(coalesce(template_element.value->>'recovery_family', ''))),
        upper(nullif(btrim(coalesce(template_element.value->>'paye_treatment', '')), '')),
        upper(nullif(btrim(coalesce(template_element.value->>'pay_channel', '')), '')),
        case
            when upper(nullif(btrim(coalesce(template_element.value->>'pay_channel', '')), '')) in ('PAYE', 'UMBRELLA') then upper(nullif(btrim(coalesce(template_element.value->>'pay_channel', '')), ''))
            when coalesce(btrim(coalesce(template_element.value->>'pay_channel', '')), '') = '' and v_scope = 'PAYE' then 'PAYE'
            else null::text
        end,
        case when coalesce(template_element.value->>'umbrella_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then (template_element.value->>'umbrella_id')::uuid else null::uuid end,
        nullif(btrim(coalesce(template_element.value->>'source_ref', '')), ''),
        case when coalesce(template_element.value->>'finance_component_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then (template_element.value->>'finance_component_id')::uuid else null::uuid end,
        nullif(btrim(coalesce(template_element.value->>'frozen_component_key_type', '')), ''),
        nullif(btrim(coalesce(template_element.value->>'frozen_component_key_value', '')), ''),
        case when jsonb_typeof(template_element.value->'frozen_component_snapshot_json') = 'object' then template_element.value->'frozen_component_snapshot_json' else null::jsonb end,
        case
            when upper(btrim(coalesce(template_element.value->>'frozen_component_classification', ''))) = 'TAXABLE_CHANNEL_SENSITIVE' then 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
            when upper(btrim(coalesce(template_element.value->>'frozen_component_classification', ''))) = 'REIMBURSEMENT_GROSS_FIXED' then 'REIMBURSEMENT_GROSS_FIXED'::public.pay_finance_component_classification_enum
            when upper(btrim(coalesce(template_element.value->>'frozen_component_classification', ''))) = 'NET_PAY_FIXED_RECOVERY' then 'NET_PAY_FIXED_RECOVERY'::public.pay_finance_component_classification_enum
            else null::public.pay_finance_component_classification_enum
        end,
        case when jsonb_typeof(template_element.value->'frozen_source_basis_json') = 'object' then template_element.value->'frozen_source_basis_json' else null::jsonb end,
        upper(nullif(btrim(coalesce(template_element.value->>'frozen_source_pay_method', '')), '')),
        upper(nullif(btrim(coalesce(template_element.value->>'frozen_target_pay_method', '')), '')),
        case
            when upper(btrim(coalesce(template_element.value->>'frozen_resolution_mode', ''))) = 'SUGGESTED_EQUIVALENT_BASIS' then 'SUGGESTED_EQUIVALENT_BASIS'::public.pay_finance_component_resolution_mode_enum
            when upper(btrim(coalesce(template_element.value->>'frozen_resolution_mode', ''))) = 'MANUAL_REPLACEMENT_RATE' then 'MANUAL_REPLACEMENT_RATE'::public.pay_finance_component_resolution_mode_enum
            when upper(btrim(coalesce(template_element.value->>'frozen_resolution_mode', ''))) = 'MANUAL_AMOUNT' then 'MANUAL_AMOUNT'::public.pay_finance_component_resolution_mode_enum
            else null::public.pay_finance_component_resolution_mode_enum
        end,
        case when jsonb_typeof(template_element.value->'frozen_resolution_payload_json') = 'object' then template_element.value->'frozen_resolution_payload_json' else null::jsonb end,
        case when jsonb_typeof(template_element.value->'frozen_resolution_result_json') = 'object' then template_element.value->'frozen_resolution_result_json' else null::jsonb end,
        case when coalesce(template_element.value->>'frozen_source_amount', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then round((template_element.value->>'frozen_source_amount')::numeric, 2)::numeric(12,2) else null::numeric(12,2) end,
        case when coalesce(template_element.value->>'frozen_outstanding_amount', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then round((template_element.value->>'frozen_outstanding_amount')::numeric, 2)::numeric(12,2) else null::numeric(12,2) end,
        case when coalesce(template_element.value->>'weekly_due', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then round((template_element.value->>'weekly_due')::numeric, 2)::numeric(12,2) else null::numeric(12,2) end,
        case when coalesce(template_element.value->>'minimum_earnings_threshold', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then (template_element.value->>'minimum_earnings_threshold')::numeric else null::numeric end,
        case when coalesce(template_element.value->>'take_home_floor_override', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then (template_element.value->>'take_home_floor_override')::numeric else null::numeric end,
        case when coalesce(template_element.value->>'default_take_home_floor', '') ~ '^-?[0-9]+(\.[0-9]+)?$' then (template_element.value->>'default_take_home_floor')::numeric else null::numeric end,
        nullif(btrim(coalesce(template_element.value->>'payout_status', '')), ''),
        case when coalesce(template_element.value->>'next_due_week_start', '') ~ '^\d{4}-\d{2}-\d{2}$' then (template_element.value->>'next_due_week_start')::date else null::date end,
        case when coalesce(template_element.value->>'sort_order', '') ~ '^-?[0-9]+$' then (template_element.value->>'sort_order')::integer else template_element.ord::integer end,
        (
            (case when coalesce(template_element.value->>'candidate_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then (template_element.value->>'candidate_id')::uuid else staged_scope.candidate_id end) is not null
            and coalesce(template_element.value->>'finance_case_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            and upper(btrim(coalesce(template_element.value->>'recovery_family', ''))) in ('MANUAL_DEBT_RECOVERY', 'LOAN_REPAYMENT', 'OVERPAYMENT_RECOVERY')
        ),
        true,
        case
            when upper(nullif(btrim(coalesce(template_element.value->>'pay_channel', '')), '')) in ('PAYE', 'UMBRELLA') then upper(nullif(btrim(coalesce(template_element.value->>'pay_channel', '')), '')) = v_scope
            when coalesce(btrim(coalesce(template_element.value->>'pay_channel', '')), '') = '' and v_scope = 'PAYE' then true
            else false
        end
    from pg_temp.tmp_pay_build_operation_candidate_scope as staged_scope
    cross join lateral jsonb_array_elements(staged_scope.hidden_recovery_template_lines_json) with ordinality as template_element(value, ord)
    where jsonb_typeof(template_element.value) = 'object';

    create temporary table if not exists pg_temp.tmp_pay_build_recovery_template_rows (
        candidate_id uuid not null,
        finance_case_id uuid not null,
        recovery_family text not null,
        paye_treatment text null,
        pay_channel text null,
        umbrella_id uuid null,
        source_ref text null,
        finance_component_id uuid null,
        frozen_component_key_type text null,
        frozen_component_key_value text null,
        frozen_component_snapshot_json jsonb null,
        frozen_component_classification public.pay_finance_component_classification_enum null,
        frozen_source_basis_json jsonb null,
        frozen_source_pay_method text null,
        frozen_target_pay_method text null,
        frozen_resolution_mode public.pay_finance_component_resolution_mode_enum null,
        frozen_resolution_payload_json jsonb null,
        frozen_resolution_result_json jsonb null,
        frozen_source_amount numeric(12,2) null,
        frozen_outstanding_amount numeric(12,2) null,
        weekly_due numeric(12,2) null,
        minimum_earnings_threshold numeric null,
        take_home_floor_override numeric null,
        default_take_home_floor numeric null,
        payout_status text null,
        next_due_week_start date null,
        sort_order integer null
    ) on commit drop;
    truncate table pg_temp.tmp_pay_build_recovery_template_rows;

    insert into pg_temp.tmp_pay_build_recovery_template_rows (
        candidate_id,
        finance_case_id,
        recovery_family,
        paye_treatment,
        pay_channel,
        umbrella_id,
        source_ref,
        finance_component_id,
        frozen_component_key_type,
        frozen_component_key_value,
        frozen_component_snapshot_json,
        frozen_component_classification,
        frozen_source_basis_json,
        frozen_source_pay_method,
        frozen_target_pay_method,
        frozen_resolution_mode,
        frozen_resolution_payload_json,
        frozen_resolution_result_json,
        frozen_source_amount,
        frozen_outstanding_amount,
        weekly_due,
        minimum_earnings_threshold,
        take_home_floor_override,
        default_take_home_floor,
        payout_status,
        next_due_week_start,
        sort_order
    )
    select distinct on (
        hidden_input.candidate_id,
        hidden_input.finance_case_id,
        hidden_input.recovery_family,
        coalesce(hidden_input.finance_component_id, '00000000-0000-0000-0000-000000000000'::uuid),
        coalesce(hidden_input.source_ref, ''),
        coalesce(hidden_input.normalized_pay_channel, '')
    )
        hidden_input.candidate_id,
        hidden_input.finance_case_id,
        hidden_input.recovery_family,
        hidden_input.paye_treatment,
        hidden_input.normalized_pay_channel,
        hidden_input.umbrella_id,
        hidden_input.source_ref,
        hidden_input.finance_component_id,
        hidden_input.frozen_component_key_type,
        hidden_input.frozen_component_key_value,
        hidden_input.frozen_component_snapshot_json,
        hidden_input.frozen_component_classification,
        hidden_input.frozen_source_basis_json,
        hidden_input.frozen_source_pay_method,
        hidden_input.frozen_target_pay_method,
        hidden_input.frozen_resolution_mode,
        hidden_input.frozen_resolution_payload_json,
        hidden_input.frozen_resolution_result_json,
        hidden_input.frozen_source_amount,
        hidden_input.frozen_outstanding_amount,
        hidden_input.weekly_due,
        hidden_input.minimum_earnings_threshold,
        hidden_input.take_home_floor_override,
        hidden_input.default_take_home_floor,
        hidden_input.payout_status,
        hidden_input.next_due_week_start,
        hidden_input.sort_order
    from pg_temp.tmp_pay_build_hidden_template_input_rows as hidden_input
    where hidden_input.candidate_selected = true
      and hidden_input.required_identifiers_present = true
      and hidden_input.pay_channel_matches_scope = true
    order by
        hidden_input.candidate_id,
        hidden_input.finance_case_id,
        hidden_input.recovery_family,
        coalesce(hidden_input.finance_component_id, '00000000-0000-0000-0000-000000000000'::uuid),
        coalesce(hidden_input.source_ref, ''),
        coalesce(hidden_input.normalized_pay_channel, ''),
        hidden_input.sort_order;

    create temporary table if not exists pg_temp.tmp_pay_build_candidates_ctx (
        id uuid primary key,
        tms_ref text,
        display_name text,
        pay_method text,
        umbrella_id uuid,
        first_name text,
        last_name text,
        account_holder text,
        sort_code text,
        account_number text,
        bank_details_hash text,
        payee_id text,
        payee_account_id text,
        umbrella_vat_chargeable boolean
    ) on commit drop;
    truncate table pg_temp.tmp_pay_build_candidates_ctx;

    create temporary table if not exists pg_temp.tmp_pay_build_umbrellas_ctx (
        id uuid primary key,
        name text,
        vat_chargeable boolean,
        sort_code text,
        account_number text,
        bank_details_hash text,
        payee_id text,
        payee_account_id text
    ) on commit drop;
    truncate table pg_temp.tmp_pay_build_umbrellas_ctx;

    insert into pg_temp.tmp_pay_build_candidates_ctx (
        id,
        tms_ref,
        display_name,
        pay_method,
        umbrella_id,
        first_name,
        last_name,
        account_holder,
        sort_code,
        account_number,
        bank_details_hash,
        payee_id,
        payee_account_id,
        umbrella_vat_chargeable
    )
    with preview_candidate_rows as (
        select distinct on (preview_candidate_data.preview_candidate_id)
            preview_candidate_data.preview_candidate_id,
            preview_candidate_data.preview_tms_ref,
            preview_candidate_data.preview_display_name,
            preview_candidate_data.preview_pay_method,
            preview_candidate_data.preview_umbrella_id,
            preview_candidate_data.preview_umbrella_vat_chargeable,
            preview_candidate_data.preview_candidate_bank_hash
        from (
            select
                case when coalesce(preview_candidate.cand->>'candidate_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then (preview_candidate.cand->>'candidate_id')::uuid else null::uuid end as preview_candidate_id,
                nullif(btrim(coalesce(preview_candidate.cand->>'tms_ref', '')), '') as preview_tms_ref,
                nullif(btrim(coalesce(preview_candidate.cand->>'display_name', '')), '') as preview_display_name,
                upper(nullif(btrim(coalesce(preview_candidate.cand->>'current_pay_method', '')), '')) as preview_pay_method,
                case when coalesce(preview_candidate.cand->>'umbrella_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then (preview_candidate.cand->>'umbrella_id')::uuid else null::uuid end as preview_umbrella_id,
                coalesce(case when lower(coalesce(preview_candidate.cand->>'umbrella_vat_chargeable', '')) in ('true', 'false') then (preview_candidate.cand->>'umbrella_vat_chargeable')::boolean else null::boolean end, false) as preview_umbrella_vat_chargeable,
                nullif(btrim(coalesce(preview_candidate.cand->>'candidate_bank_hash', '')), '') as preview_candidate_bank_hash
            from pg_temp.tmp_pay_build_preview_candidates as preview_candidate
        ) as preview_candidate_data
        where preview_candidate_data.preview_candidate_id is not null
        order by preview_candidate_data.preview_candidate_id
    )
    select
        candidate_row.id,
        coalesce(preview_candidate_rows.preview_tms_ref, candidate_row.tms_ref),
        coalesce(preview_candidate_rows.preview_display_name, candidate_row.display_name),
        coalesce(preview_candidate_rows.preview_pay_method, upper(coalesce(candidate_row.pay_method, ''))),
        coalesce(preview_candidate_rows.preview_umbrella_id, candidate_row.umbrella_id),
        candidate_row.first_name,
        candidate_row.last_name,
        candidate_row.account_holder,
        regexp_replace(coalesce(candidate_row.sort_code, ''), '[^0-9]', '', 'g'),
        nullif(regexp_replace(coalesce(candidate_row.account_number, ''), '[^0-9]', '', 'g'), ''),
        coalesce(preview_candidate_rows.preview_candidate_bank_hash, candidate_row.bank_details_hash),
        candidate_payee_map.payee_id,
        candidate_payee_map.payee_account_id,
        coalesce(preview_candidate_rows.preview_umbrella_vat_chargeable, umbrella_row.vat_chargeable, false)
    from (
        select distinct selected_row.candidate_id
        from pg_temp.tmp_pay_build_selected_preview_rows as selected_row
    ) as selected_candidate
    join public.candidates as candidate_row
      on candidate_row.id = selected_candidate.candidate_id
    left join preview_candidate_rows
      on preview_candidate_rows.preview_candidate_id = candidate_row.id
    left join public.umbrellas as umbrella_row
      on umbrella_row.id = coalesce(preview_candidate_rows.preview_umbrella_id, candidate_row.umbrella_id)
    left join public.bank_payee_map as candidate_payee_map
      on upper(coalesce(candidate_payee_map.rail_provider, '')) = upper(btrim(coalesce(v_settings_rail_provider, '')))
     and upper(coalesce(candidate_payee_map.rail_env, '')) = upper(btrim(coalesce(v_settings_rail_env, '')))
     and upper(coalesce(candidate_payee_map.entity_kind, '')) = 'CANDIDATE'
     and candidate_payee_map.entity_id = candidate_row.id
     and candidate_payee_map.bank_details_hash = coalesce(preview_candidate_rows.preview_candidate_bank_hash, candidate_row.bank_details_hash);

    insert into pg_temp.tmp_pay_build_umbrellas_ctx (
        id,
        name,
        vat_chargeable,
        sort_code,
        account_number,
        bank_details_hash,
        payee_id,
        payee_account_id
    )
    select
        umbrella_row.id,
        umbrella_row.name,
        coalesce(umbrella_row.vat_chargeable, false),
        regexp_replace(coalesce(umbrella_row.sort_code, ''), '[^0-9]', '', 'g'),
        nullif(regexp_replace(coalesce(umbrella_row.account_number, ''), '[^0-9]', '', 'g'), ''),
        umbrella_row.bank_details_hash,
        umbrella_payee_map.payee_id,
        umbrella_payee_map.payee_account_id
    from public.umbrellas as umbrella_row
    join (
        select distinct candidate_ctx.umbrella_id
        from pg_temp.tmp_pay_build_candidates_ctx as candidate_ctx
        where candidate_ctx.umbrella_id is not null
    ) as selected_umbrella
      on selected_umbrella.umbrella_id = umbrella_row.id
    left join public.bank_payee_map as umbrella_payee_map
      on upper(coalesce(umbrella_payee_map.rail_provider, '')) = upper(btrim(coalesce(v_settings_rail_provider, '')))
     and upper(coalesce(umbrella_payee_map.rail_env, '')) = upper(btrim(coalesce(v_settings_rail_env, '')))
     and upper(coalesce(umbrella_payee_map.entity_kind, '')) = 'UMBRELLA'
     and umbrella_payee_map.entity_id = umbrella_row.id
     and umbrella_payee_map.bank_details_hash = umbrella_row.bank_details_hash;

    create temporary table if not exists pg_temp.tmp_pay_build_timesheet_snapshots_ctx (
        timesheet_id uuid primary key,
        candidate_id uuid not null,
        client_id uuid null,
        week_ending_date date null,
        source_pay_method text null,
        candidate_pay_method text null,
        baseline_signature text null,
        base_snapshot_json jsonb not null,
        target_snapshot_json jsonb not null,
        target_signature text null
    ) on commit drop;
    truncate table pg_temp.tmp_pay_build_timesheet_snapshots_ctx;

    insert into pg_temp.tmp_pay_build_timesheet_snapshots_ctx (
        timesheet_id,
        candidate_id,
        client_id,
        week_ending_date,
        source_pay_method,
        candidate_pay_method,
        baseline_signature,
        base_snapshot_json,
        target_snapshot_json,
        target_signature
    )
    select distinct on (snapshot_row.timesheet_id)
        snapshot_row.timesheet_id,
        snapshot_row.candidate_id,
        snapshot_row.client_id,
        snapshot_row.week_ending_date,
        snapshot_row.source_pay_method,
        snapshot_row.candidate_pay_method,
        snapshot_row.baseline_signature,
        snapshot_row.base_snapshot_json,
        snapshot_row.target_snapshot_json,
        snapshot_row.target_signature
    from (
        select
            case when coalesce(snapshot_element.value->>'timesheet_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then (snapshot_element.value->>'timesheet_id')::uuid else null::uuid end as timesheet_id,
            case when coalesce(snapshot_element.value->>'candidate_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then (snapshot_element.value->>'candidate_id')::uuid else staged_scope.candidate_id end as candidate_id,
            case when coalesce(snapshot_element.value->>'client_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then (snapshot_element.value->>'client_id')::uuid else null::uuid end as client_id,
            case when coalesce(snapshot_element.value->>'week_ending_date', '') ~ '^\d{4}-\d{2}-\d{2}$' then (snapshot_element.value->>'week_ending_date')::date else null::date end as week_ending_date,
            nullif(btrim(coalesce(snapshot_element.value->>'source_pay_method', '')), '') as source_pay_method,
            nullif(btrim(coalesce(snapshot_element.value->>'candidate_pay_method', '')), '') as candidate_pay_method,
            nullif(btrim(coalesce(snapshot_element.value->>'baseline_signature', '')), '') as baseline_signature,
            coalesce(snapshot_element.value->'base_snapshot_json', '{}'::jsonb) as base_snapshot_json,
            coalesce(snapshot_element.value->'target_snapshot_json', '{}'::jsonb) as target_snapshot_json,
            nullif(btrim(coalesce(snapshot_element.value->>'target_signature', '')), '') as target_signature
        from pg_temp.tmp_pay_build_operation_candidate_scope as staged_scope
        cross join lateral jsonb_array_elements(
            case
                when jsonb_typeof(staged_scope.effective_candidate_fragment_json->'timesheet_snapshots_json') = 'array' then coalesce(staged_scope.effective_candidate_fragment_json->'timesheet_snapshots_json', '[]'::jsonb)
                when jsonb_typeof(staged_scope.effective_candidate_fragment_json->'timesheet_snapshots') = 'array' then coalesce(staged_scope.effective_candidate_fragment_json->'timesheet_snapshots', '[]'::jsonb)
                else '[]'::jsonb
            end
        ) as snapshot_element(value)
        where jsonb_typeof(snapshot_element.value) = 'object'
    ) as snapshot_row
    where snapshot_row.timesheet_id is not null
      and exists (
          select 1
          from pg_temp.tmp_pay_build_selected_preview_rows as selected_row
          where selected_row.timesheet_id = snapshot_row.timesheet_id
            and selected_row.candidate_id = snapshot_row.candidate_id
      )
    order by snapshot_row.timesheet_id, snapshot_row.week_ending_date desc nulls last;

    create temporary table if not exists pg_temp.tmp_pay_build_oneoff_payout_bank_details_ctx (
        finance_case_id uuid primary key,
        beneficiary_name text,
        sort_code text,
        account_number text,
        bank_details_hash text,
        created_by_user_id uuid,
        updated_by_user_id uuid,
        note text
    ) on commit drop;
    truncate table pg_temp.tmp_pay_build_oneoff_payout_bank_details_ctx;

    insert into pg_temp.tmp_pay_build_oneoff_payout_bank_details_ctx (
        finance_case_id,
        beneficiary_name,
        sort_code,
        account_number,
        bank_details_hash,
        created_by_user_id,
        updated_by_user_id,
        note
    )
    select
        oneoff_details.finance_case_id,
        oneoff_details.beneficiary_name,
        regexp_replace(coalesce(oneoff_details.sort_code, ''), '[^0-9]', '', 'g'),
        nullif(regexp_replace(coalesce(oneoff_details.account_number, ''), '[^0-9]', '', 'g'), ''),
        oneoff_details.bank_details_hash,
        oneoff_details.created_by_user_id,
        oneoff_details.updated_by_user_id,
        oneoff_details.note
    from public.pay_finance_case_oneoff_payout_bank_details as oneoff_details
    where oneoff_details.finance_case_id in (
        select distinct selected_row.finance_case_id
        from pg_temp.tmp_pay_build_selected_preview_rows as selected_row
        where selected_row.finance_case_id is not null
    );

    create temporary table if not exists pg_temp.tmp_pay_build_finance_case_components_ctx (
        id uuid primary key,
        finance_case_id uuid not null,
        candidate_id uuid null,
        linked_timesheet_id uuid null,
        component_key_type text null,
        component_key_value text null,
        classification public.pay_finance_component_classification_enum null,
        source_pay_method text null,
        source_basis_json jsonb null,
        saved_target_pay_method text null,
        saved_resolution_mode public.pay_finance_component_resolution_mode_enum null,
        saved_resolution_payload_json jsonb null,
        saved_resolution_result_json jsonb null,
        source_amount numeric(12,2) null,
        remaining_source_amount numeric(12,2) null,
        allocation_priority_group integer null,
        allocation_priority_order integer null,
        created_at_utc timestamptz null
    ) on commit drop;
    truncate table pg_temp.tmp_pay_build_finance_case_components_ctx;

    -- Operation-mode staging must use the frozen operation candidate/allocation scope.
    -- Do not rebuild finance component identity from live public.pay_finance_case_components here.
    -- The child batch-builder functions read this temp table, so expose allocation rows in the same shape.
    insert into pg_temp.tmp_pay_build_finance_case_components_ctx (
        id,
        finance_case_id,
        candidate_id,
        linked_timesheet_id,
        component_key_type,
        component_key_value,
        classification,
        source_pay_method,
        source_basis_json,
        saved_target_pay_method,
        saved_resolution_mode,
        saved_resolution_payload_json,
        saved_resolution_result_json,
        source_amount,
        remaining_source_amount,
        allocation_priority_group,
        allocation_priority_order,
        created_at_utc
    )
    select distinct on (coalesce(allocation_row.finance_component_id, allocation_row.id))
        coalesce(allocation_row.finance_component_id, allocation_row.id) as id,
        allocation_row.finance_case_id,
        allocation_row.candidate_id,
        case
            when coalesce(allocation_row.allocation_basis_json #>> '{line,timesheet_id}', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                then (allocation_row.allocation_basis_json #>> '{line,timesheet_id}')::uuid
            else null::uuid
        end as linked_timesheet_id,
        coalesce(
            nullif(btrim(allocation_row.allocation_basis_json #>> '{line,frozen_component_key_type}'), ''),
            nullif(btrim(allocation_row.allocation_basis_json #>> '{line,component_key_type}'), ''),
            nullif(btrim(allocation_row.allocation_basis_json #>> '{line,key_type}'), ''),
            nullif(btrim(allocation_row.allocation_basis_json #>> '{line,economic_key_type}'), ''),
            nullif(btrim(allocation_row.allocation_basis_json #>> '{component,frozen_component_key_type}'), ''),
            nullif(btrim(allocation_row.allocation_basis_json #>> '{component,component_key_type}'), ''),
            nullif(btrim(allocation_row.allocation_type), '')
        ) as component_key_type,
        coalesce(
            nullif(btrim(allocation_row.allocation_basis_json #>> '{line,frozen_component_key_value}'), ''),
            nullif(btrim(allocation_row.allocation_basis_json #>> '{line,component_key_value}'), ''),
            nullif(btrim(allocation_row.allocation_basis_json #>> '{line,key_value}'), ''),
            nullif(btrim(allocation_row.allocation_basis_json #>> '{line,economic_key_value}'), ''),
            nullif(btrim(allocation_row.allocation_basis_json #>> '{component,frozen_component_key_value}'), ''),
            nullif(btrim(allocation_row.allocation_basis_json #>> '{component,component_key_value}'), ''),
            allocation_row.operation_source_key
        ) as component_key_value,
        case
            when upper(coalesce(allocation_row.allocation_basis_json #>> '{line,frozen_component_classification}', allocation_row.allocation_basis_json #>> '{component,classification}', '')) = 'TAXABLE_CHANNEL_SENSITIVE'
                then 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
            when upper(coalesce(allocation_row.allocation_basis_json #>> '{line,frozen_component_classification}', allocation_row.allocation_basis_json #>> '{component,classification}', '')) = 'REIMBURSEMENT_GROSS_FIXED'
                then 'REIMBURSEMENT_GROSS_FIXED'::public.pay_finance_component_classification_enum
            when upper(coalesce(allocation_row.allocation_basis_json #>> '{line,frozen_component_classification}', allocation_row.allocation_basis_json #>> '{component,classification}', '')) = 'NET_PAY_FIXED_RECOVERY'
                then 'NET_PAY_FIXED_RECOVERY'::public.pay_finance_component_classification_enum
            else null::public.pay_finance_component_classification_enum
        end as classification,
        upper(nullif(btrim(coalesce(
            allocation_row.allocation_basis_json #>> '{line,frozen_source_pay_method}',
            allocation_row.allocation_basis_json #>> '{line,source_pay_method}',
            allocation_row.allocation_basis_json #>> '{component,source_pay_method}',
            allocation_row.pay_channel
        )), '')) as source_pay_method,
        coalesce(
            nullif(allocation_row.allocation_basis_json->'line'->'frozen_source_basis_json', 'null'::jsonb),
            nullif(allocation_row.allocation_basis_json->'line'->'source_basis_json', 'null'::jsonb),
            nullif(allocation_row.allocation_basis_json->'component'->'source_basis_json', 'null'::jsonb),
            allocation_row.allocation_basis_json
        ) as source_basis_json,
        upper(nullif(btrim(coalesce(
            allocation_row.allocation_basis_json #>> '{line,frozen_target_pay_method}',
            allocation_row.allocation_basis_json #>> '{line,target_pay_method}',
            allocation_row.allocation_basis_json #>> '{component,saved_target_pay_method}',
            allocation_row.pay_channel
        )), '')) as saved_target_pay_method,
        case
            when upper(coalesce(allocation_row.allocation_basis_json #>> '{line,frozen_resolution_mode}', allocation_row.allocation_basis_json #>> '{component,saved_resolution_mode}', '')) = 'SUGGESTED_EQUIVALENT_BASIS'
                then 'SUGGESTED_EQUIVALENT_BASIS'::public.pay_finance_component_resolution_mode_enum
            when upper(coalesce(allocation_row.allocation_basis_json #>> '{line,frozen_resolution_mode}', allocation_row.allocation_basis_json #>> '{component,saved_resolution_mode}', '')) = 'MANUAL_REPLACEMENT_RATE'
                then 'MANUAL_REPLACEMENT_RATE'::public.pay_finance_component_resolution_mode_enum
            when upper(coalesce(allocation_row.allocation_basis_json #>> '{line,frozen_resolution_mode}', allocation_row.allocation_basis_json #>> '{component,saved_resolution_mode}', '')) = 'MANUAL_AMOUNT'
                then 'MANUAL_AMOUNT'::public.pay_finance_component_resolution_mode_enum
            else null::public.pay_finance_component_resolution_mode_enum
        end as saved_resolution_mode,
        coalesce(
            nullif(allocation_row.allocation_basis_json->'line'->'frozen_resolution_payload_json', 'null'::jsonb),
            nullif(allocation_row.allocation_basis_json->'line'->'resolution_payload_json', 'null'::jsonb),
            nullif(allocation_row.allocation_basis_json->'component'->'saved_resolution_payload_json', 'null'::jsonb),
            '{}'::jsonb
        ) as saved_resolution_payload_json,
        coalesce(
            nullif(allocation_row.allocation_basis_json->'line'->'frozen_resolution_result_json', 'null'::jsonb),
            nullif(allocation_row.allocation_basis_json->'line'->'resolution_result_json', 'null'::jsonb),
            nullif(allocation_row.allocation_basis_json->'component'->'saved_resolution_result_json', 'null'::jsonb),
            jsonb_build_object('operation_source_key', allocation_row.operation_source_key, 'allocated_amount', allocation_row.allocated_amount)
        ) as saved_resolution_result_json,
        round(abs(coalesce(allocation_row.allocated_amount, 0)), 2)::numeric(12,2) as source_amount,
        round(abs(coalesce(allocation_row.allocated_amount, 0)), 2)::numeric(12,2) as remaining_source_amount,
        0::integer as allocation_priority_group,
        coalesce(allocation_row.sort_order, 0)::integer as allocation_priority_order,
        coalesce(allocation_row.created_at_utc, now()) as created_at_utc
    from public.banking_pay_operation_candidate_allocation_rows as allocation_row
    where allocation_row.operation_id = p_operation_id
      and allocation_row.pay_batch_id = p_pay_batch_id
      and allocation_row.candidate_scope_id in (
          select staged_scope.id
          from pg_temp.tmp_pay_build_operation_candidate_scope as staged_scope
      )
      and allocation_row.finance_case_id is not null
    order by coalesce(allocation_row.finance_component_id, allocation_row.id), allocation_row.sort_order nulls last, allocation_row.id;

    select count(*)::integer
    into v_timesheet_snapshot_count
    from pg_temp.tmp_pay_build_timesheet_snapshots_ctx as snapshot_ctx;

    select count(*)::integer
    into v_finance_component_count
    from pg_temp.tmp_pay_build_finance_case_components_ctx as component_ctx;

    return query
    select
        coalesce(v_candidate_count, 0),
        coalesce(v_selected_row_count, 0),
        coalesce(v_timesheet_snapshot_count, 0),
        coalesce(v_finance_component_count, 0);
end;
$$;

ALTER FUNCTION public.pay_batch_stage_operation_candidate_chunk_context(uuid,uuid,jsonb,uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_batch_stage_operation_candidate_chunk_context(uuid,uuid,jsonb,uuid) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.pay_batch_stage_operation_candidate_chunk_context(uuid,uuid,jsonb,uuid) TO service_role;

