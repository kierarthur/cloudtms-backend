-- Certified row-backed Candidate/channel scope construction for Create Draft.
-- Runtime authority is Miget TEST. The `supabase` directory name is historical only.
-- This owner transports the already-certified selection. It does not decide eligibility,
-- amounts, gross/net, tax, VAT, channel, payee, headroom, finance or settlement policy.

CREATE OR REPLACE FUNCTION private.pay_workbench_prepare_draft_scope_from_certificate_partition_v8(
  p_operation_id uuid,
  p_certificate_uuid uuid,
  p_partition_ordinal integer,
  p_request_preimage_digest_sha256 text
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY INVOKER
VOLATILE
SET search_path TO ''
AS $function$
DECLARE
  v_frozen_scope private.banking_pay_draft_frozen_certificate_scopes_v8%ROWTYPE;
  v_certificate private.banking_pay_workbench_settled_certificates_v8%ROWTYPE;
  v_partition private.banking_pay_workbench_settled_certificate_partitions_v8%ROWTYPE;
  v_candidate_input private.banking_pay_draft_frozen_candidate_inputs_v8%ROWTYPE;
  v_existing_scope private.banking_pay_draft_frozen_candidate_scopes_v8%ROWTYPE;
  v_public_scope public.banking_pay_operation_candidate_scope%ROWTYPE;
  v_member_count integer;
  v_distinct_constituent_count integer;
  v_min_member_ordinal integer;
  v_max_member_ordinal integer;
  v_frozen_reference_count integer;
  v_frozen_payload_count integer;
  v_identity_mismatch_count integer;
  v_candidate_channel_mismatch_count integer;
  v_amount_total numeric;
  v_scope_digest text;
BEGIN
  IF p_operation_id IS NULL
     OR p_certificate_uuid IS NULL
     OR p_partition_ordinal IS NULL
     OR p_partition_ordinal < 0
     OR COALESCE(p_request_preimage_digest_sha256, '') !~ '^[0-9a-f]{64}$' THEN
    RAISE EXCEPTION 'DRAFT_SCOPE_PARTITION_MISMATCH'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'DRAFT_SCOPE_PARTITION_MISMATCH',
        'operation_id', p_operation_id,
        'partition_ordinal', p_partition_ordinal
      )::text;
  END IF;

  SELECT frozen_scope.*
  INTO v_frozen_scope
  FROM private.banking_pay_draft_frozen_certificate_scopes_v8 AS frozen_scope
  WHERE frozen_scope.operation_id = p_operation_id
    AND frozen_scope.certificate_uuid = p_certificate_uuid
  FOR UPDATE;

  IF NOT FOUND OR v_frozen_scope.freeze_state <> 'STAGING' THEN
    RAISE EXCEPTION 'DRAFT_SCOPE_NOT_FROZEN'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'DRAFT_SCOPE_NOT_STAGING', 'operation_id', p_operation_id
      )::text;
  END IF;

  SELECT certificate_row.*
  INTO v_certificate
  FROM private.banking_pay_workbench_settled_certificates_v8 AS certificate_row
  WHERE certificate_row.certificate_uuid = p_certificate_uuid;

  SELECT partition_row.*
  INTO v_partition
  FROM private.banking_pay_workbench_settled_certificate_partitions_v8 AS partition_row
  JOIN private.banking_pay_draft_frozen_partition_refs_v8 AS frozen_partition
    ON frozen_partition.operation_id = p_operation_id
   AND frozen_partition.certificate_uuid = partition_row.certificate_uuid
   AND frozen_partition.partition_ordinal = partition_row.partition_ordinal
  WHERE partition_row.certificate_uuid = p_certificate_uuid
    AND partition_row.partition_ordinal = p_partition_ordinal
    AND (
      v_frozen_scope.pay_channel_scope = 'ALL'
      OR partition_row.resolved_pay_channel = v_frozen_scope.pay_channel_scope
    );

  IF NOT FOUND OR v_certificate.certificate_uuid IS NULL THEN
    RAISE EXCEPTION 'DRAFT_SCOPE_PARTITION_MISMATCH'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'DRAFT_SCOPE_PARTITION_MISMATCH',
        'operation_id', p_operation_id,
        'partition_ordinal', p_partition_ordinal
      )::text;
  END IF;

  SELECT input_row.*
  INTO v_candidate_input
  FROM private.banking_pay_draft_frozen_candidate_inputs_v8 AS input_row
  WHERE input_row.operation_id = p_operation_id
    AND input_row.certificate_uuid = p_certificate_uuid
    AND input_row.partition_ordinal = p_partition_ordinal
    AND input_row.candidate_id = v_partition.candidate_id
    AND input_row.resolved_pay_channel = v_partition.resolved_pay_channel;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'DRAFT_SCOPE_PARTITION_MISMATCH'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'DRAFT_SCOPE_PARTITION_MISMATCH',
        'operation_id', p_operation_id,
        'partition_ordinal', p_partition_ordinal,
        'reason', 'FROZEN_CANDIDATE_INPUT_MISSING'
      )::text;
  END IF;

  SELECT
    pg_catalog.count(*)::integer,
    pg_catalog.count(DISTINCT member.constituent_ordinal)::integer,
    pg_catalog.min(member.member_ordinal),
    pg_catalog.max(member.member_ordinal),
    pg_catalog.count(frozen_ref.constituent_ordinal)::integer,
    pg_catalog.count(frozen_payload.constituent_ordinal)::integer,
    pg_catalog.count(*) FILTER (
      WHERE member.stable_identity_digest_sha256 IS DISTINCT FROM entry.constituent_digest_sha256
    )::integer,
    pg_catalog.count(*) FILTER (
      WHERE entry.candidate_id IS DISTINCT FROM v_partition.candidate_id
         OR entry.resolved_pay_channel IS DISTINCT FROM v_partition.resolved_pay_channel
    )::integer,
    ROUND(COALESCE(pg_catalog.sum(entry.canonical_amount_ex_vat::numeric), 0), 2)
  INTO
    v_member_count,
    v_distinct_constituent_count,
    v_min_member_ordinal,
    v_max_member_ordinal,
    v_frozen_reference_count,
    v_frozen_payload_count,
    v_identity_mismatch_count,
    v_candidate_channel_mismatch_count,
    v_amount_total
  FROM private.banking_pay_workbench_settled_certificate_partition_members_v8 AS member
  JOIN private.banking_pay_workbench_settled_certificate_entries_v8 AS entry
    ON entry.certificate_uuid = member.certificate_uuid
   AND entry.constituent_ordinal = member.constituent_ordinal
  LEFT JOIN private.banking_pay_draft_frozen_constituent_refs_v8 AS frozen_ref
    ON frozen_ref.operation_id = p_operation_id
   AND frozen_ref.certificate_uuid = member.certificate_uuid
   AND frozen_ref.constituent_ordinal = member.constituent_ordinal
  LEFT JOIN private.banking_pay_draft_frozen_constituent_payloads_v8 AS frozen_payload
    ON frozen_payload.operation_id = p_operation_id
   AND frozen_payload.certificate_uuid = member.certificate_uuid
   AND frozen_payload.constituent_ordinal = member.constituent_ordinal
  WHERE member.certificate_uuid = p_certificate_uuid
    AND member.partition_ordinal = p_partition_ordinal;

  IF v_member_count IS DISTINCT FROM v_partition.constituent_count
     OR v_distinct_constituent_count IS DISTINCT FROM v_partition.constituent_count
     OR v_frozen_reference_count IS DISTINCT FROM v_partition.constituent_count
     OR v_frozen_payload_count IS DISTINCT FROM v_partition.constituent_count
     OR v_min_member_ordinal IS DISTINCT FROM 0
     OR v_max_member_ordinal IS DISTINCT FROM v_partition.constituent_count - 1
     OR v_identity_mismatch_count <> 0
     OR v_candidate_channel_mismatch_count <> 0
     OR v_amount_total IS DISTINCT FROM v_partition.canonical_amount_ex_vat_total::numeric THEN
    RAISE EXCEPTION 'DRAFT_SCOPE_MEMBER_GAP'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'DRAFT_SCOPE_MEMBER_GAP',
        'operation_id', p_operation_id,
        'partition_ordinal', p_partition_ordinal,
        'expected_member_count', v_partition.constituent_count,
        'actual_member_count', v_member_count,
        'frozen_reference_count', v_frozen_reference_count,
        'frozen_payload_count', v_frozen_payload_count,
        'identity_mismatch_count', v_identity_mismatch_count,
        'candidate_channel_mismatch_count', v_candidate_channel_mismatch_count
      )::text;
  END IF;

  v_scope_digest := v_partition.partition_digest_sha256;

  INSERT INTO private.banking_pay_draft_frozen_candidate_scopes_v8(
    operation_id, candidate_scope_ordinal, certificate_uuid, partition_ordinal,
    candidate_id, resolved_pay_channel, constituent_count,
    canonical_amount_ex_vat_total, scope_digest_sha256, scope_state
  ) VALUES (
    p_operation_id, p_partition_ordinal, p_certificate_uuid, p_partition_ordinal,
    v_partition.candidate_id, v_partition.resolved_pay_channel,
    v_partition.constituent_count, v_partition.canonical_amount_ex_vat_total,
    v_scope_digest, 'STAGED'
  )
  ON CONFLICT (operation_id, candidate_scope_ordinal) DO NOTHING;

  SELECT candidate_scope.*
  INTO v_existing_scope
  FROM private.banking_pay_draft_frozen_candidate_scopes_v8 AS candidate_scope
  WHERE candidate_scope.operation_id = p_operation_id
    AND candidate_scope.candidate_scope_ordinal = p_partition_ordinal
  FOR UPDATE;

  IF v_existing_scope.certificate_uuid IS DISTINCT FROM p_certificate_uuid
     OR v_existing_scope.partition_ordinal IS DISTINCT FROM p_partition_ordinal
     OR v_existing_scope.candidate_id IS DISTINCT FROM v_partition.candidate_id
     OR v_existing_scope.resolved_pay_channel IS DISTINCT FROM v_partition.resolved_pay_channel
     OR v_existing_scope.constituent_count IS DISTINCT FROM v_partition.constituent_count
     OR v_existing_scope.canonical_amount_ex_vat_total IS DISTINCT FROM v_partition.canonical_amount_ex_vat_total
     OR v_existing_scope.scope_digest_sha256 IS DISTINCT FROM v_scope_digest
     OR v_existing_scope.scope_state NOT IN ('STAGED', 'FROZEN') THEN
    RAISE EXCEPTION 'DRAFT_SCOPE_ALREADY_CONFLICTING'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'DRAFT_SCOPE_ALREADY_CONFLICTING',
        'operation_id', p_operation_id,
        'partition_ordinal', p_partition_ordinal
      )::text;
  END IF;

  INSERT INTO public.banking_pay_operation_candidate_scope(
    operation_id, workbench_session_id, source_snapshot_run_id, source_session_version,
    candidate_state_id, candidate_id, pay_channel, pay_batch_id,
    selected_preview_row_ids_json, selected_timesheet_ids_json,
    selected_finance_case_ids_json, effective_candidate_fragment_json,
    effective_summary_fragment_json, effective_paye_candidate_json,
    effective_non_paye_payee_json, effective_payees_json,
    effective_case_resolution_states_json, effective_canonical_preview_lines_json,
    selected_canonical_preview_lines_json, baseline_component_rows_json,
    hidden_recovery_template_lines_json, candidate_totals_json,
    allocation_basis_json, scope_hash, chunk_sequence, status
  ) VALUES (
    p_operation_id, v_certificate.workbench_session_id, v_certificate.source_snapshot_run_id,
    v_certificate.session_version, v_candidate_input.candidate_state_id, v_partition.candidate_id,
    v_partition.resolved_pay_channel, NULL,
    '[]'::jsonb, '[]'::jsonb, '[]'::jsonb,
    v_candidate_input.effective_candidate_fragment_json,
    v_candidate_input.effective_summary_fragment_json,
    v_candidate_input.effective_paye_candidate_json,
    v_candidate_input.effective_non_paye_payee_json,
    v_candidate_input.effective_payees_json,
    v_candidate_input.effective_case_resolution_states_json,
    '[]'::jsonb, '[]'::jsonb, '[]'::jsonb, '[]'::jsonb,
    pg_catalog.jsonb_build_object(
      'selected_row_count', v_partition.constituent_count,
      'selected_amount_ex_vat', v_partition.canonical_amount_ex_vat_total::numeric,
      'selected_constituents_digest_sha256', v_partition.partition_digest_sha256,
      'row_backed_draft_scope', true,
      'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
    ),
    pg_catalog.jsonb_build_object(
      'source', 'WORKBENCH_SETTLED_CERTIFICATION_V2',
      'certificate_uuid', p_certificate_uuid::text,
      'partition_ordinal', p_partition_ordinal,
      'constituent_count', v_partition.constituent_count,
      'canonical_amount_ex_vat_total', v_partition.canonical_amount_ex_vat_total,
      'partition_digest_sha256', v_partition.partition_digest_sha256,
      'semantic_contract_version', v_candidate_input.source_publication_attestation_json->>'semantic_contract_version',
      'semantic_proof_digest', v_candidate_input.source_publication_attestation_json->>'semantic_proof_digest',
      'source_build_run_id', v_candidate_input.source_build_run_id::text,
      'source_publication_attestation', v_candidate_input.source_publication_attestation_json,
      'source_publication_id', v_candidate_input.source_publication_id::text,
      'row_backed_draft_scope', true,
      'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
    ),
    v_scope_digest, p_partition_ordinal, 'SCOPED'
  )
  ON CONFLICT (operation_id, candidate_id, pay_channel) DO NOTHING;

  SELECT public_scope.*
  INTO v_public_scope
  FROM public.banking_pay_operation_candidate_scope AS public_scope
  WHERE public_scope.operation_id = p_operation_id
    AND public_scope.candidate_id = v_partition.candidate_id
    AND public_scope.pay_channel = v_partition.resolved_pay_channel
  FOR UPDATE;

  IF v_public_scope.workbench_session_id IS DISTINCT FROM v_certificate.workbench_session_id
     OR v_public_scope.source_snapshot_run_id IS DISTINCT FROM v_certificate.source_snapshot_run_id
     OR v_public_scope.source_session_version IS DISTINCT FROM v_certificate.session_version
     OR v_public_scope.scope_hash IS DISTINCT FROM v_scope_digest
     OR v_public_scope.chunk_sequence IS DISTINCT FROM p_partition_ordinal
     OR v_public_scope.candidate_state_id IS DISTINCT FROM v_candidate_input.candidate_state_id
     OR v_public_scope.pay_batch_id IS NOT NULL
     OR v_public_scope.selected_preview_row_ids_json <> '[]'::jsonb
     OR v_public_scope.selected_timesheet_ids_json <> '[]'::jsonb
     OR v_public_scope.selected_finance_case_ids_json <> '[]'::jsonb
     OR v_public_scope.effective_canonical_preview_lines_json <> '[]'::jsonb
     OR v_public_scope.selected_canonical_preview_lines_json <> '[]'::jsonb
     OR v_public_scope.baseline_component_rows_json <> '[]'::jsonb
     OR v_public_scope.hidden_recovery_template_lines_json <> '[]'::jsonb
     OR v_public_scope.effective_candidate_fragment_json IS DISTINCT FROM v_candidate_input.effective_candidate_fragment_json
     OR v_public_scope.effective_summary_fragment_json IS DISTINCT FROM v_candidate_input.effective_summary_fragment_json
     OR v_public_scope.effective_paye_candidate_json IS DISTINCT FROM v_candidate_input.effective_paye_candidate_json
     OR v_public_scope.effective_non_paye_payee_json IS DISTINCT FROM v_candidate_input.effective_non_paye_payee_json
     OR v_public_scope.effective_payees_json IS DISTINCT FROM v_candidate_input.effective_payees_json
     OR v_public_scope.effective_case_resolution_states_json IS DISTINCT FROM v_candidate_input.effective_case_resolution_states_json
     OR v_public_scope.status <> 'SCOPED' THEN
    RAISE EXCEPTION 'DRAFT_SCOPE_ALREADY_CONFLICTING'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'DRAFT_SCOPE_ALREADY_CONFLICTING',
        'operation_id', p_operation_id,
        'partition_ordinal', p_partition_ordinal
      )::text;
  END IF;

  INSERT INTO private.banking_pay_draft_frozen_candidate_scope_members_v8(
    operation_id, candidate_scope_ordinal, member_ordinal, certificate_uuid,
    partition_ordinal, constituent_ordinal, stable_identity_digest_sha256
  )
  SELECT
    p_operation_id,
    p_partition_ordinal,
    member.member_ordinal,
    member.certificate_uuid,
    member.partition_ordinal,
    member.constituent_ordinal,
    member.stable_identity_digest_sha256
  FROM private.banking_pay_workbench_settled_certificate_partition_members_v8 AS member
  JOIN private.banking_pay_draft_frozen_constituent_refs_v8 AS frozen_ref
    ON frozen_ref.operation_id = p_operation_id
   AND frozen_ref.certificate_uuid = member.certificate_uuid
   AND frozen_ref.constituent_ordinal = member.constituent_ordinal
  WHERE member.certificate_uuid = p_certificate_uuid
    AND member.partition_ordinal = p_partition_ordinal
  ORDER BY member.member_ordinal
  ON CONFLICT (operation_id, candidate_scope_ordinal, member_ordinal) DO NOTHING;

  SELECT pg_catalog.count(*)::integer
  INTO v_member_count
  FROM private.banking_pay_draft_frozen_candidate_scope_members_v8 AS frozen_member
  WHERE frozen_member.operation_id = p_operation_id
    AND frozen_member.candidate_scope_ordinal = p_partition_ordinal;

  IF v_member_count IS DISTINCT FROM v_partition.constituent_count THEN
    RAISE EXCEPTION 'DRAFT_SCOPE_DIGEST_MISMATCH'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'DRAFT_SCOPE_DIGEST_MISMATCH',
        'operation_id', p_operation_id,
        'partition_ordinal', p_partition_ordinal,
        'expected_member_count', v_partition.constituent_count,
        'actual_member_count', v_member_count
      )::text;
  END IF;

  UPDATE private.banking_pay_draft_frozen_candidate_scopes_v8 AS candidate_scope
  SET scope_state = 'FROZEN'
  WHERE candidate_scope.operation_id = p_operation_id
    AND candidate_scope.candidate_scope_ordinal = p_partition_ordinal
    AND candidate_scope.scope_state = 'STAGED';

  RETURN pg_catalog.jsonb_build_object(
    'operation_id', p_operation_id,
    'certificate_uuid', p_certificate_uuid,
    'candidate_scope_ordinal', p_partition_ordinal,
    'partition_ordinal', p_partition_ordinal,
    'candidate_id', v_partition.candidate_id,
    'resolved_pay_channel', v_partition.resolved_pay_channel,
    'constituent_count', v_partition.constituent_count,
    'scope_digest_sha256', v_scope_digest,
    'public_candidate_scope_id', v_public_scope.id,
    'request_preimage_digest_sha256', p_request_preimage_digest_sha256,
    'row_backed', true
  );
END;
$function$;

ALTER FUNCTION private.pay_workbench_prepare_draft_scope_from_certificate_partition_v8(uuid,uuid,integer,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_prepare_draft_scope_from_certificate_partition_v8(uuid,uuid,integer,text) FROM PUBLIC, anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION public.pay_workbench_prepare_draft_scope_from_frozen_page_v8(
  p_operation_id uuid,
  p_after_partition_ordinal integer DEFAULT NULL,
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
  v_link private.banking_pay_workbench_settled_certificate_operation_links_v8%ROWTYPE;
  v_reference_result jsonb;
  v_existing private.banking_pay_draft_frozen_stage_receipts_v8%ROWTYPE;
  v_previous private.banking_pay_draft_frozen_stage_receipts_v8%ROWTYPE;
  v_partition record;
  v_stage_kind constant text := 'CANDIDATE_SCOPE';
  v_limit integer := COALESCE(p_limit, 256);
  v_page_sequence integer;
  v_candidate_count integer;
  v_row_count integer;
  v_next_after integer;
  v_has_more boolean;
  v_page_preimage text;
  v_page_digest text;
  v_request_preimage text;
  v_request_digest text;
  v_receipt_preimage text;
  v_receipt_digest text;
  v_canonical_bytes integer;
  v_total_scope_count integer;
  v_total_member_count integer;
BEGIN
  PERFORM public.banking_pay_hot_path_budget_apply('WORKBENCH_CHUNK');

  IF p_operation_id IS NULL THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_OPERATION_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = '{"code":"WORKBENCH_CERTIFICATE_OPERATION_REQUIRED"}';
  END IF;
  IF v_limit < 1 OR v_limit > 256 THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_PAGE_LIMIT_INVALID'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'WORKBENCH_CERTIFICATE_PAGE_LIMIT_INVALID', 'limit', v_limit
      )::text;
  END IF;
  IF p_after_partition_ordinal IS NOT NULL AND p_after_partition_ordinal < 0 THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_PAGE_CURSOR_INVALID'
      USING ERRCODE = 'P0001', DETAIL = '{"code":"WORKBENCH_CERTIFICATE_PAGE_CURSOR_INVALID"}';
  END IF;
  IF p_expected_previous_receipt_sha256 IS NOT NULL
     AND p_expected_previous_receipt_sha256 !~ '^[0-9a-f]{64}$' THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_PAGE_RECEIPT_MISMATCH'
      USING ERRCODE = 'P0001', DETAIL = '{"code":"WORKBENCH_CERTIFICATE_PAGE_RECEIPT_MISMATCH"}';
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
      USING ERRCODE = 'P0001', DETAIL = '{"code":"WORKBENCH_CERTIFICATE_NOT_FOUND"}';
  END IF;

  SELECT frozen_scope.*
  INTO v_scope
  FROM private.banking_pay_draft_frozen_certificate_scopes_v8 AS frozen_scope
  WHERE frozen_scope.operation_id = p_operation_id
  FOR UPDATE;

  IF NOT FOUND OR v_scope.freeze_state <> 'STAGING' THEN
    RAISE EXCEPTION 'DRAFT_SCOPE_NOT_STAGING'
      USING ERRCODE = 'P0001', DETAIL = '{"code":"DRAFT_SCOPE_NOT_STAGING"}';
  END IF;

  SELECT link_row.*
  INTO v_link
  FROM private.banking_pay_workbench_settled_certificate_operation_links_v8 AS link_row
  WHERE link_row.operation_id = p_operation_id;
  IF NOT FOUND OR v_link.certificate_uuid IS DISTINCT FROM v_scope.certificate_uuid THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_IDENTITY_MISMATCH'
      USING ERRCODE = 'P0001', DETAIL = '{"code":"WORKBENCH_CERTIFICATE_IDENTITY_MISMATCH"}';
  END IF;

  -- Every pre-freeze scope page shares the exact Workbench session row fence.
  -- No Candidate row is locked and no financial object exists at this stage.
  v_reference_result := public.pay_workbench_settled_certificate_reference_validate_v8(
    p_operation_id, v_link.certification_id, v_link.overall_digest_sha256, 'PAGE');
  IF COALESCE((v_reference_result->>'ok')::boolean, false) IS NOT TRUE THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_SESSION_SUPERSEDED'
      USING ERRCODE = 'P0001', DETAIL = '{"code":"WORKBENCH_CERTIFICATE_SESSION_SUPERSEDED"}';
  END IF;

  v_request_preimage := '{"after_partition_ordinal":'
    || COALESCE(pg_catalog.to_jsonb(p_after_partition_ordinal)::text, 'null')
    || ',"expected_previous_receipt_sha256":'
    || COALESCE(pg_catalog.to_jsonb(p_expected_previous_receipt_sha256)::text, 'null')
    || ',"operation_id":' || pg_catalog.to_jsonb(p_operation_id::text)::text
    || ',"requested_limit":' || v_limit::text
    || ',"stage_kind":' || pg_catalog.to_jsonb(v_stage_kind)::text || '}';
  v_request_digest := pg_catalog.encode(
    extensions.digest(pg_catalog.convert_to(v_request_preimage, 'UTF8'), 'sha256'), 'hex');

  SELECT receipt_row.*
  INTO v_existing
  FROM private.banking_pay_draft_frozen_stage_receipts_v8 AS receipt_row
  WHERE receipt_row.operation_id = p_operation_id
    AND receipt_row.stage_kind = v_stage_kind
    AND receipt_row.after_ordinal IS NOT DISTINCT FROM p_after_partition_ordinal;

  IF FOUND THEN
    IF v_existing.requested_limit IS DISTINCT FROM v_limit
       OR v_existing.expected_previous_receipt_sha256 IS DISTINCT FROM p_expected_previous_receipt_sha256
       OR v_existing.request_preimage_digest_sha256 IS DISTINCT FROM v_request_digest THEN
      RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_PAGE_REQUEST_CONFLICT'
        USING ERRCODE = 'P0001', DETAIL = '{"code":"WORKBENCH_CERTIFICATE_PAGE_REQUEST_CONFLICT"}';
    END IF;
    RETURN pg_catalog.jsonb_build_object(
      'operation_id', p_operation_id,
      'stage_kind', v_stage_kind,
      'page_sequence', v_existing.page_sequence,
      'after_partition_ordinal', v_existing.after_ordinal,
      'row_count', v_existing.row_count,
      'canonical_byte_count', v_existing.canonical_byte_count,
      'page_receipt_digest_sha256', v_existing.receipt_digest_sha256,
      'next_after_partition_ordinal', v_existing.next_after_ordinal,
      'has_more', v_existing.has_more,
      'terminal_sentinel_present', v_existing.terminal_sentinel_present,
      'stage_total_count', v_scope.partition_count,
      'stage_total_digest_sha256', v_scope.selected_partitions_digest_sha256,
      'replayed', true
    );
  END IF;

  SELECT COALESCE(pg_catalog.max(receipt_row.page_sequence), -1) + 1
  INTO v_page_sequence
  FROM private.banking_pay_draft_frozen_stage_receipts_v8 AS receipt_row
  WHERE receipt_row.operation_id = p_operation_id
    AND receipt_row.stage_kind = v_stage_kind;

  IF v_page_sequence = 0 THEN
    IF p_after_partition_ordinal IS NOT NULL OR p_expected_previous_receipt_sha256 IS NOT NULL THEN
      RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_PAGE_CURSOR_INVALID'
        USING ERRCODE = 'P0001', DETAIL = '{"code":"WORKBENCH_CERTIFICATE_PAGE_CURSOR_INVALID"}';
    END IF;
  ELSE
    SELECT receipt_row.*
    INTO v_previous
    FROM private.banking_pay_draft_frozen_stage_receipts_v8 AS receipt_row
    WHERE receipt_row.operation_id = p_operation_id
      AND receipt_row.stage_kind = v_stage_kind
      AND receipt_row.page_sequence = v_page_sequence - 1;
    IF NOT FOUND
       OR NOT v_previous.has_more
       OR v_previous.next_after_ordinal IS DISTINCT FROM p_after_partition_ordinal
       OR v_previous.receipt_digest_sha256 IS DISTINCT FROM p_expected_previous_receipt_sha256 THEN
      RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_PAGE_RECEIPT_MISMATCH'
        USING ERRCODE = 'P0001', DETAIL = '{"code":"WORKBENCH_CERTIFICATE_PAGE_RECEIPT_MISMATCH"}';
    END IF;
  END IF;

  WITH page_with_sentinel AS (
    SELECT frozen_partition.partition_ordinal
    FROM private.banking_pay_draft_frozen_partition_refs_v8 AS frozen_partition
    WHERE frozen_partition.operation_id = p_operation_id
      AND frozen_partition.partition_ordinal > COALESCE(p_after_partition_ordinal, -1)
    ORDER BY frozen_partition.partition_ordinal
    LIMIT (v_limit + 1)
  )
  SELECT pg_catalog.count(*)::integer
  INTO v_candidate_count
  FROM page_with_sentinel;

  v_row_count := LEAST(v_candidate_count, v_limit);
  v_has_more := v_candidate_count > v_limit;

  WITH page_rows AS (
    SELECT frozen_partition.partition_ordinal, partition_row.partition_digest_sha256
    FROM private.banking_pay_draft_frozen_partition_refs_v8 AS frozen_partition
    JOIN private.banking_pay_workbench_settled_certificate_partitions_v8 AS partition_row
      ON partition_row.certificate_uuid = frozen_partition.certificate_uuid
     AND partition_row.partition_ordinal = frozen_partition.partition_ordinal
    WHERE frozen_partition.operation_id = p_operation_id
      AND frozen_partition.partition_ordinal > COALESCE(p_after_partition_ordinal, -1)
    ORDER BY frozen_partition.partition_ordinal
    LIMIT v_limit
  )
  SELECT pg_catalog.max(page_row.partition_ordinal),
         '[' || COALESCE(pg_catalog.string_agg(
           '{"partition_digest_sha256":' || pg_catalog.to_jsonb(page_row.partition_digest_sha256)::text
             || ',"partition_ordinal":' || page_row.partition_ordinal::text || '}',
           ',' ORDER BY page_row.partition_ordinal), '') || ']'
  INTO v_next_after, v_page_preimage
  FROM page_rows AS page_row;

  v_canonical_bytes := pg_catalog.octet_length(v_page_preimage);
  IF v_canonical_bytes > 524288 THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_PAGE_BYTES_EXCEEDED'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'WORKBENCH_CERTIFICATE_PAGE_BYTES_EXCEEDED',
        'canonical_byte_count', v_canonical_bytes
      )::text;
  END IF;
  v_page_digest := pg_catalog.encode(
    extensions.digest(pg_catalog.convert_to(v_page_preimage, 'UTF8'), 'sha256'), 'hex');

  FOR v_partition IN
    SELECT frozen_partition.partition_ordinal
    FROM private.banking_pay_draft_frozen_partition_refs_v8 AS frozen_partition
    WHERE frozen_partition.operation_id = p_operation_id
      AND frozen_partition.partition_ordinal > COALESCE(p_after_partition_ordinal, -1)
    ORDER BY frozen_partition.partition_ordinal
    LIMIT v_limit
  LOOP
    PERFORM private.pay_workbench_prepare_draft_scope_from_certificate_partition_v8(
      p_operation_id,
      v_scope.certificate_uuid,
      v_partition.partition_ordinal,
      v_request_digest
    );
  END LOOP;

  IF NOT v_has_more THEN
    SELECT pg_catalog.count(*)::integer
    INTO v_total_scope_count
    FROM private.banking_pay_draft_frozen_candidate_scopes_v8 AS candidate_scope
    WHERE candidate_scope.operation_id = p_operation_id
      AND candidate_scope.scope_state = 'FROZEN';

    SELECT pg_catalog.count(*)::integer
    INTO v_total_member_count
    FROM private.banking_pay_draft_frozen_candidate_scope_members_v8 AS member
    WHERE member.operation_id = p_operation_id;

    IF v_total_scope_count IS DISTINCT FROM v_scope.partition_count
       OR v_total_member_count IS DISTINCT FROM v_scope.constituent_count THEN
      RAISE EXCEPTION 'DRAFT_SCOPE_DIGEST_MISMATCH'
        USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
          'code', 'DRAFT_SCOPE_DIGEST_MISMATCH',
          'expected_scope_count', v_scope.partition_count,
          'actual_scope_count', v_total_scope_count,
          'expected_member_count', v_scope.constituent_count,
          'actual_member_count', v_total_member_count
      )::text;
    END IF;

  END IF;

  v_receipt_preimage := '{"after_partition_ordinal":'
    || COALESCE(pg_catalog.to_jsonb(p_after_partition_ordinal)::text, 'null')
    || ',"has_more":' || pg_catalog.to_jsonb(v_has_more)::text
    || ',"next_after_partition_ordinal":' || COALESCE(pg_catalog.to_jsonb(v_next_after)::text, 'null')
    || ',"operation_id":' || pg_catalog.to_jsonb(p_operation_id::text)::text
    || ',"page_digest_sha256":' || pg_catalog.to_jsonb(v_page_digest)::text
    || ',"page_sequence":' || v_page_sequence::text
    || ',"request_preimage_digest_sha256":' || pg_catalog.to_jsonb(v_request_digest)::text
    || ',"requested_limit":' || v_limit::text
    || ',"row_count":' || v_row_count::text
    || ',"stage_kind":' || pg_catalog.to_jsonb(v_stage_kind)::text || '}';
  v_receipt_digest := pg_catalog.encode(
    extensions.digest(pg_catalog.convert_to(v_receipt_preimage, 'UTF8'), 'sha256'), 'hex');

  INSERT INTO private.banking_pay_draft_frozen_stage_receipts_v8(
    operation_id, stage_kind, page_sequence, after_ordinal, requested_limit,
    expected_previous_receipt_sha256, request_preimage_digest_sha256, row_count,
    canonical_byte_count, next_after_ordinal, has_more, terminal_sentinel_present,
    receipt_digest_sha256, stage_status
  ) VALUES (
    p_operation_id, v_stage_kind, v_page_sequence, p_after_partition_ordinal, v_limit,
    p_expected_previous_receipt_sha256, v_request_digest, v_row_count,
    v_canonical_bytes, v_next_after, v_has_more, true,
    v_receipt_digest, CASE WHEN v_has_more THEN 'COMMITTED' ELSE 'TERMINAL' END
  );

  RETURN pg_catalog.jsonb_build_object(
    'operation_id', p_operation_id,
    'stage_kind', v_stage_kind,
    'page_sequence', v_page_sequence,
    'after_partition_ordinal', p_after_partition_ordinal,
    'row_count', v_row_count,
    'canonical_byte_count', v_canonical_bytes,
    'page_receipt_digest_sha256', v_receipt_digest,
    'next_after_partition_ordinal', v_next_after,
    'has_more', v_has_more,
    'terminal_sentinel_present', true,
    'stage_total_count', v_scope.partition_count,
    'stage_total_digest_sha256', v_scope.selected_partitions_digest_sha256,
    'frozen_candidate_scope_count', v_total_scope_count,
    'frozen_constituent_count', v_total_member_count,
    'replayed', false
  );
END;
$function$;

ALTER FUNCTION public.pay_workbench_prepare_draft_scope_from_frozen_page_v8(uuid,integer,integer,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_prepare_draft_scope_from_frozen_page_v8(uuid,integer,integer,text) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.pay_workbench_prepare_draft_scope_from_frozen_page_v8(uuid,integer,integer,text) TO service_role;
