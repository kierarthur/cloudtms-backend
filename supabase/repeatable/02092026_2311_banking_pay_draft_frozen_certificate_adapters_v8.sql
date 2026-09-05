-- H2 bounded certificate-reference staging and final freeze.
-- Runtime authority is Miget TEST. This file changes transport only: all payment,
-- tax, VAT, channel, payee, finance and settlement policy remains with its current owner.

CREATE OR REPLACE FUNCTION private.banking_pay_draft_frozen_certificate_scope_initialise_v8(
  p_operation_id uuid
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
VOLATILE
SET search_path TO ''
AS $function$
DECLARE
  v_link private.banking_pay_workbench_settled_certificate_operation_links_v8%ROWTYPE;
  v_certificate private.banking_pay_workbench_settled_certificates_v8%ROWTYPE;
  v_manifest private.banking_pay_workbench_settled_certificate_channel_manifests_v8%ROWTYPE;
  v_scope private.banking_pay_draft_frozen_certificate_scopes_v8%ROWTYPE;
  v_inserted boolean := false;
BEGIN
  IF p_operation_id IS NULL THEN
    RAISE EXCEPTION 'DRAFT_CERTIFICATE_SCOPE_INITIALISE_INPUT_INVALID'
      USING ERRCODE = '22023',
            DETAIL = '{"code":"DRAFT_CERTIFICATE_SCOPE_INITIALISE_INPUT_INVALID"}';
  END IF;

  SELECT link_row.*
  INTO v_link
  FROM private.banking_pay_workbench_settled_certificate_operation_links_v8 AS link_row
  WHERE link_row.operation_id = p_operation_id
  FOR SHARE;

  IF NOT FOUND OR v_link.link_state <> 'STAGING' THEN
    RAISE EXCEPTION 'DRAFT_CERTIFICATE_SCOPE_INITIALISE_LINK_INVALID'
      USING ERRCODE = '55000',
            DETAIL = pg_catalog.jsonb_build_object(
              'code', 'DRAFT_CERTIFICATE_SCOPE_INITIALISE_LINK_INVALID',
              'link_state', v_link.link_state
            )::text;
  END IF;

  SELECT certificate_row.*
  INTO v_certificate
  FROM private.banking_pay_workbench_settled_certificates_v8 AS certificate_row
  WHERE certificate_row.certificate_uuid = v_link.certificate_uuid
    AND certificate_row.certification_id = v_link.certification_id
    AND certificate_row.overall_digest_sha256 = v_link.overall_digest_sha256
  FOR SHARE;

  IF NOT FOUND OR v_certificate.lifecycle <> 'SEALED_CURRENT' THEN
    RAISE EXCEPTION 'DRAFT_CERTIFICATE_SCOPE_INITIALISE_CERTIFICATE_INVALID'
      USING ERRCODE = '55000',
            DETAIL = '{"code":"DRAFT_CERTIFICATE_SCOPE_INITIALISE_CERTIFICATE_INVALID"}';
  END IF;

  SELECT manifest_row.*
  INTO v_manifest
  FROM private.banking_pay_workbench_settled_certificate_channel_manifests_v8 AS manifest_row
  WHERE manifest_row.certificate_uuid = v_link.certificate_uuid
    AND manifest_row.pay_channel_scope = v_link.pay_channel_scope;

  IF NOT FOUND
     OR v_manifest.manifest_digest_sha256 IS DISTINCT FROM v_link.channel_manifest_digest_sha256
     OR v_manifest.constituent_count NOT BETWEEN 1 AND 50000
     OR v_manifest.partition_count < 1 THEN
    RAISE EXCEPTION 'DRAFT_CERTIFICATE_SCOPE_INITIALISE_MANIFEST_INVALID'
      USING ERRCODE = '55000',
            DETAIL = '{"code":"DRAFT_CERTIFICATE_SCOPE_INITIALISE_MANIFEST_INVALID"}';
  END IF;

  INSERT INTO private.banking_pay_draft_frozen_certificate_scopes_v8(
    operation_id,
    certificate_uuid,
    pay_channel_scope,
    constituent_count,
    partition_count,
    canonical_amount_ex_vat_total,
    selected_constituents_digest_sha256,
    selected_partitions_digest_sha256,
    manifest_digest_sha256,
    freeze_state,
    frozen_at_utc
  ) VALUES (
    p_operation_id,
    v_manifest.certificate_uuid,
    v_manifest.pay_channel_scope,
    v_manifest.constituent_count,
    v_manifest.partition_count,
    v_manifest.canonical_amount_ex_vat_total,
    v_manifest.selected_constituents_digest_sha256,
    v_manifest.selected_partitions_digest_sha256,
    v_manifest.manifest_digest_sha256,
    'STAGING',
    NULL
  )
  ON CONFLICT (operation_id) DO NOTHING;
  v_inserted := FOUND;

  SELECT scope_row.*
  INTO STRICT v_scope
  FROM private.banking_pay_draft_frozen_certificate_scopes_v8 AS scope_row
  WHERE scope_row.operation_id = p_operation_id
  FOR UPDATE;

  IF v_scope.certificate_uuid IS DISTINCT FROM v_manifest.certificate_uuid
     OR v_scope.pay_channel_scope IS DISTINCT FROM v_manifest.pay_channel_scope
     OR v_scope.constituent_count IS DISTINCT FROM v_manifest.constituent_count
     OR v_scope.partition_count IS DISTINCT FROM v_manifest.partition_count
     OR v_scope.canonical_amount_ex_vat_total IS DISTINCT FROM v_manifest.canonical_amount_ex_vat_total
     OR v_scope.selected_constituents_digest_sha256 IS DISTINCT FROM v_manifest.selected_constituents_digest_sha256
     OR v_scope.selected_partitions_digest_sha256 IS DISTINCT FROM v_manifest.selected_partitions_digest_sha256
     OR v_scope.manifest_digest_sha256 IS DISTINCT FROM v_manifest.manifest_digest_sha256
     OR v_scope.freeze_state <> 'STAGING'
     OR v_scope.frozen_at_utc IS NOT NULL THEN
    RAISE EXCEPTION 'DRAFT_CERTIFICATE_SCOPE_INITIALISE_REPLAY_CONFLICT'
      USING ERRCODE = '23505',
            DETAIL = '{"code":"DRAFT_CERTIFICATE_SCOPE_INITIALISE_REPLAY_CONFLICT"}';
  END IF;

  RETURN pg_catalog.jsonb_build_object(
    'ok', true,
    'operation_id', p_operation_id,
    'certificate_uuid', v_scope.certificate_uuid,
    'pay_channel_scope', v_scope.pay_channel_scope,
    'constituent_count', v_scope.constituent_count,
    'partition_count', v_scope.partition_count,
    'manifest_digest_sha256', v_scope.manifest_digest_sha256,
    'freeze_state', v_scope.freeze_state,
    'replayed', NOT v_inserted
  );
END;
$function$;

ALTER FUNCTION private.banking_pay_draft_frozen_certificate_scope_initialise_v8(uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.banking_pay_draft_frozen_certificate_scope_initialise_v8(uuid)
  FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION private.banking_pay_draft_frozen_certificate_scope_initialise_v8(uuid)
  TO postgres;

CREATE OR REPLACE FUNCTION public.pay_workbench_draft_certificate_constituent_ref_page_v8(
  p_operation_id uuid,
  p_after_ordinal integer DEFAULT NULL,
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
  v_scope private.banking_pay_draft_frozen_certificate_scopes_v8%ROWTYPE;
  v_link private.banking_pay_workbench_settled_certificate_operation_links_v8%ROWTYPE;
  v_reference_result jsonb;
  v_existing private.banking_pay_draft_frozen_stage_receipts_v8%ROWTYPE;
  v_previous private.banking_pay_draft_frozen_stage_receipts_v8%ROWTYPE;
  v_stage_kind constant text := 'CERTIFICATE_CONSTITUENT_REFS';
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
BEGIN
  PERFORM public.banking_pay_hot_path_budget_apply('WORKBENCH_CHUNK');

  IF p_operation_id IS NULL THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_OPERATION_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = '{"code":"WORKBENCH_CERTIFICATE_OPERATION_REQUIRED"}';
  END IF;
  IF v_limit < 1 OR v_limit > 256 THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_PAGE_LIMIT_INVALID'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'WORKBENCH_CERTIFICATE_PAGE_LIMIT_INVALID', 'limit', v_limit)::text;
  END IF;
  IF p_after_ordinal IS NOT NULL AND p_after_ordinal < 0 THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_PAGE_CURSOR_INVALID'
      USING ERRCODE = 'P0001', DETAIL = '{"code":"WORKBENCH_CERTIFICATE_PAGE_CURSOR_INVALID"}';
  END IF;
  IF p_expected_previous_receipt_sha256 IS NOT NULL
     AND p_expected_previous_receipt_sha256 !~ '^[0-9a-f]{64}$' THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_PAGE_RECEIPT_MISMATCH'
      USING ERRCODE = 'P0001', DETAIL = '{"code":"WORKBENCH_CERTIFICATE_PAGE_RECEIPT_MISMATCH"}';
  END IF;

  SELECT scope_row.*
  INTO v_scope
  FROM private.banking_pay_draft_frozen_certificate_scopes_v8 AS scope_row
  WHERE scope_row.operation_id = p_operation_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_NOT_FOUND'
      USING ERRCODE = 'P0001', DETAIL = '{"code":"WORKBENCH_CERTIFICATE_NOT_FOUND"}';
  END IF;
  IF v_scope.freeze_state <> 'STAGING' THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_STAGE_ALREADY_FROZEN'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'WORKBENCH_CERTIFICATE_STAGE_ALREADY_FROZEN', 'freeze_state', v_scope.freeze_state)::text;
  END IF;

  SELECT link_row.*
  INTO v_link
  FROM private.banking_pay_workbench_settled_certificate_operation_links_v8 AS link_row
  WHERE link_row.operation_id = p_operation_id;
  IF NOT FOUND OR v_link.certificate_uuid IS DISTINCT FROM v_scope.certificate_uuid THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_IDENTITY_MISMATCH'
      USING ERRCODE = 'P0001', DETAIL = '{"code":"WORKBENCH_CERTIFICATE_IDENTITY_MISMATCH"}';
  END IF;
  v_reference_result := public.pay_workbench_settled_certificate_reference_validate_v8(
    p_operation_id, v_link.certification_id, v_link.overall_digest_sha256, 'PAGE');
  IF COALESCE((v_reference_result->>'ok')::boolean, false) IS NOT TRUE THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_SESSION_SUPERSEDED'
      USING ERRCODE = 'P0001', DETAIL = '{"code":"WORKBENCH_CERTIFICATE_SESSION_SUPERSEDED"}';
  END IF;

  v_request_preimage := '{"after_ordinal":' || COALESCE(pg_catalog.to_jsonb(p_after_ordinal)::text, 'null')
    || ',"expected_previous_receipt_sha256":' || COALESCE(pg_catalog.to_jsonb(p_expected_previous_receipt_sha256)::text, 'null')
    || ',"operation_id":' || pg_catalog.to_jsonb(p_operation_id::text)::text
    || ',"pay_channel_scope":' || pg_catalog.to_jsonb(v_scope.pay_channel_scope)::text
    || ',"requested_limit":' || v_limit::text
    || ',"stage_kind":' || pg_catalog.to_jsonb(v_stage_kind)::text || '}';
  v_request_digest := pg_catalog.encode(
    extensions.digest(pg_catalog.convert_to(v_request_preimage, 'UTF8'), 'sha256'), 'hex');

  SELECT receipt_row.*
  INTO v_existing
  FROM private.banking_pay_draft_frozen_stage_receipts_v8 AS receipt_row
  WHERE receipt_row.operation_id = p_operation_id
    AND receipt_row.stage_kind = v_stage_kind
    AND receipt_row.after_ordinal IS NOT DISTINCT FROM p_after_ordinal;

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
      'after_ordinal', v_existing.after_ordinal,
      'row_count', v_existing.row_count,
      'canonical_byte_count', v_existing.canonical_byte_count,
      'page_receipt_digest_sha256', v_existing.receipt_digest_sha256,
      'next_after_ordinal', v_existing.next_after_ordinal,
      'has_more', v_existing.has_more,
      'terminal_sentinel_present', v_existing.terminal_sentinel_present,
      'stage_total_count', v_scope.constituent_count,
      'stage_total_digest_sha256', v_scope.selected_constituents_digest_sha256,
      'replayed', true
    );
  END IF;

  SELECT COALESCE(pg_catalog.max(receipt_row.page_sequence), -1) + 1
  INTO v_page_sequence
  FROM private.banking_pay_draft_frozen_stage_receipts_v8 AS receipt_row
  WHERE receipt_row.operation_id = p_operation_id
    AND receipt_row.stage_kind = v_stage_kind;

  IF v_page_sequence = 0 THEN
    IF p_after_ordinal IS NOT NULL OR p_expected_previous_receipt_sha256 IS NOT NULL THEN
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
       OR v_previous.next_after_ordinal IS DISTINCT FROM p_after_ordinal
       OR v_previous.receipt_digest_sha256 IS DISTINCT FROM p_expected_previous_receipt_sha256 THEN
      RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_PAGE_RECEIPT_MISMATCH'
        USING ERRCODE = 'P0001', DETAIL = '{"code":"WORKBENCH_CERTIFICATE_PAGE_RECEIPT_MISMATCH"}';
    END IF;
  END IF;

  WITH page_with_sentinel AS (
    SELECT entry.constituent_ordinal
    FROM private.banking_pay_workbench_settled_certificate_entries_v8 AS entry
    WHERE entry.certificate_uuid = v_scope.certificate_uuid
      AND (v_scope.pay_channel_scope = 'ALL' OR entry.resolved_pay_channel = v_scope.pay_channel_scope)
      AND entry.constituent_ordinal > COALESCE(p_after_ordinal, -1)
    ORDER BY entry.constituent_ordinal
    LIMIT (v_limit + 1)
  )
  SELECT pg_catalog.count(*)::integer
  INTO v_candidate_count
  FROM page_with_sentinel;

  v_row_count := LEAST(v_candidate_count, v_limit);
  v_has_more := v_candidate_count > v_limit;

  WITH page_rows AS (
    SELECT entry.constituent_ordinal
    FROM private.banking_pay_workbench_settled_certificate_entries_v8 AS entry
    WHERE entry.certificate_uuid = v_scope.certificate_uuid
      AND (v_scope.pay_channel_scope = 'ALL' OR entry.resolved_pay_channel = v_scope.pay_channel_scope)
      AND entry.constituent_ordinal > COALESCE(p_after_ordinal, -1)
    ORDER BY entry.constituent_ordinal
    LIMIT v_limit
  )
  SELECT pg_catalog.max(page_row.constituent_ordinal),
         '[' || COALESCE(pg_catalog.string_agg(
           '{"constituent_ordinal":' || page_row.constituent_ordinal::text || '}',
           ',' ORDER BY page_row.constituent_ordinal), '') || ']'
  INTO v_next_after, v_page_preimage
  FROM page_rows AS page_row;

  v_canonical_bytes := pg_catalog.octet_length(v_page_preimage);
  IF v_canonical_bytes > 524288 THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_PAGE_BYTES_EXCEEDED'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'WORKBENCH_CERTIFICATE_PAGE_BYTES_EXCEEDED', 'canonical_byte_count', v_canonical_bytes)::text;
  END IF;
  v_page_digest := pg_catalog.encode(
    extensions.digest(pg_catalog.convert_to(v_page_preimage, 'UTF8'), 'sha256'), 'hex');

  INSERT INTO private.banking_pay_draft_frozen_constituent_refs_v8(
    operation_id, certificate_uuid, constituent_ordinal, staged_page_sequence
  )
  SELECT p_operation_id, v_scope.certificate_uuid, entry.constituent_ordinal, v_page_sequence
  FROM private.banking_pay_workbench_settled_certificate_entries_v8 AS entry
  WHERE entry.certificate_uuid = v_scope.certificate_uuid
    AND (v_scope.pay_channel_scope = 'ALL' OR entry.resolved_pay_channel = v_scope.pay_channel_scope)
    AND entry.constituent_ordinal > COALESCE(p_after_ordinal, -1)
  ORDER BY entry.constituent_ordinal
  LIMIT v_limit;

  WITH selected_entries AS (
    SELECT entry.*
    FROM private.banking_pay_workbench_settled_certificate_entries_v8 AS entry
    WHERE entry.certificate_uuid = v_scope.certificate_uuid
      AND (v_scope.pay_channel_scope = 'ALL' OR entry.resolved_pay_channel = v_scope.pay_channel_scope)
      AND entry.constituent_ordinal > COALESCE(p_after_ordinal, -1)
    ORDER BY entry.constituent_ordinal
    LIMIT v_limit
  ), frozen_payloads AS (
    SELECT entry.constituent_ordinal,
      entry.certificate_uuid,
      entry.materialised_preview_row_id AS preview_row_id,
      entry.candidate_id,
      entry.resolved_pay_channel,
      entry.timesheet_id,
      NULLIF(pg_catalog.btrim(COALESCE(preview_row.row_json->>'finance_case_id', '')), '')::uuid AS finance_case_id,
      entry.row_key,
      preview_row.row_ordinal,
      pg_catalog.jsonb_strip_nulls(
        preview_row.row_json
        || pg_catalog.jsonb_build_object(
          'source', 'banking_pay_workbench_preview_rows',
          'preview_row_id', entry.materialised_preview_row_id::text,
          'preview_row_pk', entry.materialised_preview_row_id::text,
          'row_key', entry.row_key,
          'row_ordinal', preview_row.row_ordinal,
          'candidate_id', entry.candidate_id::text,
          'timesheet_id', CASE WHEN entry.timesheet_id IS NULL THEN NULL ELSE entry.timesheet_id::text END,
          'finance_case_id', preview_row.row_json->>'finance_case_id',
          'finance_component_id', preview_row.row_json->>'finance_component_id',
          'line_type', preview_row.row_json->>'line_type',
          'case_type', preview_row.row_json->>'case_type',
          'item_direction', preview_row.row_json->>'item_direction',
          'pay_channel', entry.resolved_pay_channel,
          'section', 'canonical_preview_lines',
          'selection_state', 'SELECTED',
          'status', 'READY',
          'amount_ex_vat', entry.canonical_amount_ex_vat::numeric,
          'key_type', entry.economic_key_type,
          'key_value', entry.economic_key_value,
          'economic_key', pg_catalog.jsonb_build_object(
            'timesheet_id', CASE WHEN entry.economic_key_timesheet_id IS NULL THEN NULL ELSE entry.economic_key_timesheet_id::text END,
            'key_type', entry.economic_key_type,
            'key_value', entry.economic_key_value
          ),
          'preview_contract', COALESCE(preview_row.row_json->'preview_contract', '{}'::jsonb),
          'workbench_settled_certificate_binding_v8', pg_catalog.jsonb_build_object(
            'binding_contract_version', 'WORKBENCH_SETTLED_CERTIFICATE_BINDING_V8',
            'certificate_uuid', entry.certificate_uuid::text,
            'constituent_digest_sha256', entry.constituent_digest_sha256,
            'constituent_ordinal', entry.constituent_ordinal,
            'source_identity_digest_sha256', entry.source_identity_digest_sha256
          ),
          'draftable', true,
          'row_backed_draft_scope', true,
          'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH',
          'effective', true
        )
      ) AS payload_json
    FROM selected_entries AS entry
    JOIN private.banking_pay_workbench_settled_certificates_v8 AS certificate_row
      ON certificate_row.certificate_uuid = entry.certificate_uuid
    JOIN public.banking_pay_workbench_preview_rows AS preview_row
      ON preview_row.id = entry.materialised_preview_row_id
     AND preview_row.session_id = certificate_row.workbench_session_id
     AND preview_row.candidate_id = entry.candidate_id
     AND preview_row.row_key = entry.row_key
     AND preview_row.section = 'canonical_preview_lines'
     AND preview_row.selected IS TRUE
     AND preview_row.selection_state = 'SELECTED'
     AND preview_row.status = 'READY'
     AND preview_row.session_version = certificate_row.session_version
     AND preview_row.timesheet_id IS NOT DISTINCT FROM entry.timesheet_id
     AND preview_row.key_type = entry.economic_key_type
     AND preview_row.key_value = entry.economic_key_value
  )
  INSERT INTO private.banking_pay_draft_frozen_constituent_payloads_v8(
    operation_id, constituent_ordinal, certificate_uuid, preview_row_id,
    candidate_id, resolved_pay_channel, timesheet_id, finance_case_id,
    row_key, row_ordinal, payload_json, payload_digest_sha256
  )
  SELECT p_operation_id, payload.constituent_ordinal, payload.certificate_uuid,
    payload.preview_row_id, payload.candidate_id, payload.resolved_pay_channel,
    payload.timesheet_id, payload.finance_case_id, payload.row_key, payload.row_ordinal,
    payload.payload_json,
    pg_catalog.encode(extensions.digest(pg_catalog.convert_to(payload.payload_json::text, 'UTF8'), 'sha256'), 'hex')
  FROM frozen_payloads AS payload
  ON CONFLICT (operation_id, constituent_ordinal) DO NOTHING;

  IF (
    SELECT pg_catalog.count(*)
    FROM private.banking_pay_draft_frozen_constituent_payloads_v8 AS payload
    JOIN private.banking_pay_draft_frozen_constituent_refs_v8 AS ref
      ON ref.operation_id = payload.operation_id
     AND ref.constituent_ordinal = payload.constituent_ordinal
    WHERE payload.operation_id = p_operation_id
      AND ref.staged_page_sequence = v_page_sequence
  ) <> v_row_count THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_TAMPERED'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'WORKBENCH_CERTIFICATE_TAMPERED',
        'operation_id', p_operation_id,
        'stage_kind', v_stage_kind,
        'reason', 'FROZEN_CONSTITUENT_PAYLOAD_COUNT_MISMATCH'
      )::text;
  END IF;

  v_receipt_preimage := '{"after_ordinal":' || COALESCE(pg_catalog.to_jsonb(p_after_ordinal)::text, 'null')
    || ',"has_more":' || pg_catalog.to_jsonb(v_has_more)::text
    || ',"next_after_ordinal":' || COALESCE(pg_catalog.to_jsonb(v_next_after)::text, 'null')
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
    p_operation_id, v_stage_kind, v_page_sequence, p_after_ordinal, v_limit,
    p_expected_previous_receipt_sha256, v_request_digest, v_row_count,
    v_canonical_bytes, v_next_after, v_has_more, true,
    v_receipt_digest, CASE WHEN v_has_more THEN 'COMMITTED' ELSE 'TERMINAL' END
  );

  RETURN pg_catalog.jsonb_build_object(
    'operation_id', p_operation_id,
    'stage_kind', v_stage_kind,
    'page_sequence', v_page_sequence,
    'after_ordinal', p_after_ordinal,
    'row_count', v_row_count,
    'canonical_byte_count', v_canonical_bytes,
    'page_receipt_digest_sha256', v_receipt_digest,
    'next_after_ordinal', v_next_after,
    'has_more', v_has_more,
    'terminal_sentinel_present', true,
    'stage_total_count', v_scope.constituent_count,
    'stage_total_digest_sha256', v_scope.selected_constituents_digest_sha256,
    'replayed', false
  );
END;
$function$;

ALTER FUNCTION public.pay_workbench_draft_certificate_constituent_ref_page_v8(uuid,integer,integer,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_draft_certificate_constituent_ref_page_v8(uuid,integer,integer,text) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.pay_workbench_draft_certificate_constituent_ref_page_v8(uuid,integer,integer,text) TO service_role;

CREATE OR REPLACE FUNCTION public.pay_workbench_draft_certificate_partition_ref_page_v8(
  p_operation_id uuid,
  p_after_ordinal integer DEFAULT NULL,
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
  v_scope private.banking_pay_draft_frozen_certificate_scopes_v8%ROWTYPE;
  v_link private.banking_pay_workbench_settled_certificate_operation_links_v8%ROWTYPE;
  v_reference_result jsonb;
  v_existing private.banking_pay_draft_frozen_stage_receipts_v8%ROWTYPE;
  v_previous private.banking_pay_draft_frozen_stage_receipts_v8%ROWTYPE;
  v_stage_kind constant text := 'CERTIFICATE_PARTITION_REFS';
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
BEGIN
  PERFORM public.banking_pay_hot_path_budget_apply('WORKBENCH_CHUNK');

  IF p_operation_id IS NULL THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_OPERATION_REQUIRED'
      USING ERRCODE = 'P0001', DETAIL = '{"code":"WORKBENCH_CERTIFICATE_OPERATION_REQUIRED"}';
  END IF;
  IF v_limit < 1 OR v_limit > 256 THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_PAGE_LIMIT_INVALID'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'WORKBENCH_CERTIFICATE_PAGE_LIMIT_INVALID', 'limit', v_limit)::text;
  END IF;
  IF p_after_ordinal IS NOT NULL AND p_after_ordinal < 0 THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_PAGE_CURSOR_INVALID'
      USING ERRCODE = 'P0001', DETAIL = '{"code":"WORKBENCH_CERTIFICATE_PAGE_CURSOR_INVALID"}';
  END IF;
  IF p_expected_previous_receipt_sha256 IS NOT NULL
     AND p_expected_previous_receipt_sha256 !~ '^[0-9a-f]{64}$' THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_PAGE_RECEIPT_MISMATCH'
      USING ERRCODE = 'P0001', DETAIL = '{"code":"WORKBENCH_CERTIFICATE_PAGE_RECEIPT_MISMATCH"}';
  END IF;

  SELECT scope_row.*
  INTO v_scope
  FROM private.banking_pay_draft_frozen_certificate_scopes_v8 AS scope_row
  WHERE scope_row.operation_id = p_operation_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_NOT_FOUND'
      USING ERRCODE = 'P0001', DETAIL = '{"code":"WORKBENCH_CERTIFICATE_NOT_FOUND"}';
  END IF;
  IF v_scope.freeze_state <> 'STAGING' THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_STAGE_ALREADY_FROZEN'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'WORKBENCH_CERTIFICATE_STAGE_ALREADY_FROZEN', 'freeze_state', v_scope.freeze_state)::text;
  END IF;

  SELECT link_row.*
  INTO v_link
  FROM private.banking_pay_workbench_settled_certificate_operation_links_v8 AS link_row
  WHERE link_row.operation_id = p_operation_id;
  IF NOT FOUND OR v_link.certificate_uuid IS DISTINCT FROM v_scope.certificate_uuid THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_IDENTITY_MISMATCH'
      USING ERRCODE = 'P0001', DETAIL = '{"code":"WORKBENCH_CERTIFICATE_IDENTITY_MISMATCH"}';
  END IF;
  v_reference_result := public.pay_workbench_settled_certificate_reference_validate_v8(
    p_operation_id, v_link.certification_id, v_link.overall_digest_sha256, 'PAGE');
  IF COALESCE((v_reference_result->>'ok')::boolean, false) IS NOT TRUE THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_SESSION_SUPERSEDED'
      USING ERRCODE = 'P0001', DETAIL = '{"code":"WORKBENCH_CERTIFICATE_SESSION_SUPERSEDED"}';
  END IF;

  v_request_preimage := '{"after_ordinal":' || COALESCE(pg_catalog.to_jsonb(p_after_ordinal)::text, 'null')
    || ',"expected_previous_receipt_sha256":' || COALESCE(pg_catalog.to_jsonb(p_expected_previous_receipt_sha256)::text, 'null')
    || ',"operation_id":' || pg_catalog.to_jsonb(p_operation_id::text)::text
    || ',"pay_channel_scope":' || pg_catalog.to_jsonb(v_scope.pay_channel_scope)::text
    || ',"requested_limit":' || v_limit::text
    || ',"stage_kind":' || pg_catalog.to_jsonb(v_stage_kind)::text || '}';
  v_request_digest := pg_catalog.encode(
    extensions.digest(pg_catalog.convert_to(v_request_preimage, 'UTF8'), 'sha256'), 'hex');

  SELECT receipt_row.*
  INTO v_existing
  FROM private.banking_pay_draft_frozen_stage_receipts_v8 AS receipt_row
  WHERE receipt_row.operation_id = p_operation_id
    AND receipt_row.stage_kind = v_stage_kind
    AND receipt_row.after_ordinal IS NOT DISTINCT FROM p_after_ordinal;

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
      'after_ordinal', v_existing.after_ordinal,
      'row_count', v_existing.row_count,
      'canonical_byte_count', v_existing.canonical_byte_count,
      'page_receipt_digest_sha256', v_existing.receipt_digest_sha256,
      'next_after_ordinal', v_existing.next_after_ordinal,
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
    IF p_after_ordinal IS NOT NULL OR p_expected_previous_receipt_sha256 IS NOT NULL THEN
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
       OR v_previous.next_after_ordinal IS DISTINCT FROM p_after_ordinal
       OR v_previous.receipt_digest_sha256 IS DISTINCT FROM p_expected_previous_receipt_sha256 THEN
      RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_PAGE_RECEIPT_MISMATCH'
        USING ERRCODE = 'P0001', DETAIL = '{"code":"WORKBENCH_CERTIFICATE_PAGE_RECEIPT_MISMATCH"}';
    END IF;
  END IF;

  WITH page_with_sentinel AS (
    SELECT partition_row.partition_ordinal
    FROM private.banking_pay_workbench_settled_certificate_partitions_v8 AS partition_row
    WHERE partition_row.certificate_uuid = v_scope.certificate_uuid
      AND (v_scope.pay_channel_scope = 'ALL' OR partition_row.resolved_pay_channel = v_scope.pay_channel_scope)
      AND partition_row.partition_ordinal > COALESCE(p_after_ordinal, -1)
    ORDER BY partition_row.partition_ordinal
    LIMIT (v_limit + 1)
  )
  SELECT pg_catalog.count(*)::integer
  INTO v_candidate_count
  FROM page_with_sentinel;

  v_row_count := LEAST(v_candidate_count, v_limit);
  v_has_more := v_candidate_count > v_limit;

  WITH page_rows AS (
    SELECT partition_row.partition_ordinal
    FROM private.banking_pay_workbench_settled_certificate_partitions_v8 AS partition_row
    WHERE partition_row.certificate_uuid = v_scope.certificate_uuid
      AND (v_scope.pay_channel_scope = 'ALL' OR partition_row.resolved_pay_channel = v_scope.pay_channel_scope)
      AND partition_row.partition_ordinal > COALESCE(p_after_ordinal, -1)
    ORDER BY partition_row.partition_ordinal
    LIMIT v_limit
  )
  SELECT pg_catalog.max(page_row.partition_ordinal),
         '[' || COALESCE(pg_catalog.string_agg(
           '{"partition_ordinal":' || page_row.partition_ordinal::text || '}',
           ',' ORDER BY page_row.partition_ordinal), '') || ']'
  INTO v_next_after, v_page_preimage
  FROM page_rows AS page_row;

  v_canonical_bytes := pg_catalog.octet_length(v_page_preimage);
  IF v_canonical_bytes > 524288 THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_PAGE_BYTES_EXCEEDED'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'WORKBENCH_CERTIFICATE_PAGE_BYTES_EXCEEDED', 'canonical_byte_count', v_canonical_bytes)::text;
  END IF;
  v_page_digest := pg_catalog.encode(
    extensions.digest(pg_catalog.convert_to(v_page_preimage, 'UTF8'), 'sha256'), 'hex');

  INSERT INTO private.banking_pay_draft_frozen_partition_refs_v8(
    operation_id, certificate_uuid, partition_ordinal, staged_page_sequence
  )
  SELECT p_operation_id, v_scope.certificate_uuid, partition_row.partition_ordinal, v_page_sequence
  FROM private.banking_pay_workbench_settled_certificate_partitions_v8 AS partition_row
  WHERE partition_row.certificate_uuid = v_scope.certificate_uuid
    AND (v_scope.pay_channel_scope = 'ALL' OR partition_row.resolved_pay_channel = v_scope.pay_channel_scope)
    AND partition_row.partition_ordinal > COALESCE(p_after_ordinal, -1)
  ORDER BY partition_row.partition_ordinal
  LIMIT v_limit;

  WITH selected_partitions AS (
    SELECT partition_row.*
    FROM private.banking_pay_workbench_settled_certificate_partitions_v8 AS partition_row
    WHERE partition_row.certificate_uuid = v_scope.certificate_uuid
      AND (v_scope.pay_channel_scope = 'ALL' OR partition_row.resolved_pay_channel = v_scope.pay_channel_scope)
      AND partition_row.partition_ordinal > COALESCE(p_after_ordinal, -1)
    ORDER BY partition_row.partition_ordinal
    LIMIT v_limit
  ), candidate_input_values AS (
    SELECT partition_row.certificate_uuid,
      partition_row.partition_ordinal,
      candidate_state.id AS candidate_state_id,
      partition_row.candidate_id,
      partition_row.resolved_pay_channel,
      candidate_state.source_change_seq,
      candidate_state.session_version,
      publication_row.source_build_run_id,
      publication_row.source_publication_id,
      session_scope.certified_preview_publication_attestation_json AS source_publication_attestation_json,
      candidate_state.effective_candidate_fragment_json,
      candidate_state.effective_summary_fragment_json,
      COALESCE(candidate_state.effective_paye_candidate_json, '{}'::jsonb) AS effective_paye_candidate_json,
      COALESCE(candidate_state.effective_non_paye_payee_json, '{}'::jsonb) AS effective_non_paye_payee_json,
      candidate_state.effective_payees_json,
      pg_catalog.jsonb_strip_nulls(pg_catalog.jsonb_build_object(
        'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH',
        'rows', COALESCE((
          SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_strip_nulls(pg_catalog.jsonb_build_object(
            'case_resolution_summary', payload.payload_json->'case_resolution_summary',
            'finance_case_id', CASE WHEN payload.finance_case_id IS NULL THEN NULL ELSE payload.finance_case_id::text END,
            'preview_row_id', payload.preview_row_id::text,
            'row_key', payload.row_key
          )) ORDER BY payload.row_ordinal, payload.preview_row_id)
          FROM private.banking_pay_draft_frozen_constituent_payloads_v8 AS payload
          WHERE payload.operation_id = p_operation_id
            AND payload.candidate_id = partition_row.candidate_id
            AND payload.resolved_pay_channel = partition_row.resolved_pay_channel
            AND (payload.finance_case_id IS NOT NULL OR payload.payload_json ? 'case_resolution_summary')
        ), '[]'::jsonb),
        'same_week_paye_override', COALESCE((
          SELECT operation_row.input_json->'same_week_paye_override'
          FROM public.banking_pay_operations AS operation_row
          WHERE operation_row.id = p_operation_id
        ), '{}'::jsonb),
        'source', 'WORKBENCH_SETTLED_CERTIFICATION_V2'
      )) AS effective_case_resolution_states_json
    FROM selected_partitions AS partition_row
    JOIN private.banking_pay_workbench_settled_certificates_v8 AS certificate_row
      ON certificate_row.certificate_uuid = partition_row.certificate_uuid
    JOIN public.banking_pay_workbench_session_candidate_state AS candidate_state
      ON candidate_state.session_id = certificate_row.workbench_session_id
     AND candidate_state.candidate_id = partition_row.candidate_id
     AND candidate_state.status = 'READY'
     AND candidate_state.session_version = certificate_row.session_version
    JOIN private.banking_pay_workbench_settled_certificate_publications_v8 AS publication_row
      ON publication_row.certificate_uuid = partition_row.certificate_uuid
     AND publication_row.candidate_id = partition_row.candidate_id
     AND publication_row.candidate_state_id = candidate_state.id
     AND publication_row.candidate_state_status = 'READY'
     AND publication_row.source_change_seq = candidate_state.source_change_seq
     AND publication_row.certified_publication_session_version = candidate_state.session_version
    JOIN public.banking_pay_workbench_session_scope AS session_scope
      ON session_scope.session_id = certificate_row.workbench_session_id
     AND session_scope.candidate_id = partition_row.candidate_id
     AND session_scope.certified_preview_publication_parity_ok IS TRUE
     AND session_scope.certified_preview_publication_session_version = certificate_row.session_version
     AND session_scope.certified_preview_publication_source_build_run_id = publication_row.source_build_run_id
     AND session_scope.certified_preview_publication_source_publication_id = publication_row.source_publication_id
     AND session_scope.certified_preview_publication_attestation_json->>'source_publication_id' = publication_row.source_publication_id::text
  ), candidate_inputs AS (
    SELECT input_value.*,
      pg_catalog.jsonb_build_object(
        'candidate_id', input_value.candidate_id::text,
        'candidate_state_id', input_value.candidate_state_id::text,
        'effective_candidate_fragment_json', input_value.effective_candidate_fragment_json,
        'effective_case_resolution_states_json', input_value.effective_case_resolution_states_json,
        'effective_non_paye_payee_json', input_value.effective_non_paye_payee_json,
        'effective_payees_json', input_value.effective_payees_json,
        'effective_paye_candidate_json', input_value.effective_paye_candidate_json,
        'effective_summary_fragment_json', input_value.effective_summary_fragment_json,
        'partition_ordinal', input_value.partition_ordinal,
        'resolved_pay_channel', input_value.resolved_pay_channel,
        'source_build_run_id', input_value.source_build_run_id::text,
        'source_change_seq', input_value.source_change_seq,
        'source_publication_attestation_json', input_value.source_publication_attestation_json,
        'source_publication_id', input_value.source_publication_id::text,
        'source_session_version', input_value.session_version
      ) AS input_preimage_json
    FROM candidate_input_values AS input_value
  )
  INSERT INTO private.banking_pay_draft_frozen_candidate_inputs_v8(
    operation_id, partition_ordinal, certificate_uuid, candidate_state_id,
    candidate_id, resolved_pay_channel, source_change_seq, source_session_version,
    source_build_run_id, source_publication_id, source_publication_attestation_json,
    effective_candidate_fragment_json, effective_summary_fragment_json,
    effective_paye_candidate_json, effective_non_paye_payee_json,
    effective_payees_json, effective_case_resolution_states_json,
    input_digest_sha256
  )
  SELECT p_operation_id, input_row.partition_ordinal, input_row.certificate_uuid,
    input_row.candidate_state_id, input_row.candidate_id, input_row.resolved_pay_channel,
    input_row.source_change_seq, input_row.session_version,
    input_row.source_build_run_id, input_row.source_publication_id,
    input_row.source_publication_attestation_json,
    input_row.effective_candidate_fragment_json, input_row.effective_summary_fragment_json,
    input_row.effective_paye_candidate_json, input_row.effective_non_paye_payee_json,
    input_row.effective_payees_json, input_row.effective_case_resolution_states_json,
    pg_catalog.encode(extensions.digest(pg_catalog.convert_to(input_row.input_preimage_json::text, 'UTF8'), 'sha256'), 'hex')
  FROM candidate_inputs AS input_row
  ON CONFLICT (operation_id, partition_ordinal) DO NOTHING;

  IF (
    SELECT pg_catalog.count(*)
    FROM private.banking_pay_draft_frozen_candidate_inputs_v8 AS input_row
    JOIN private.banking_pay_draft_frozen_partition_refs_v8 AS ref
      ON ref.operation_id = input_row.operation_id
     AND ref.partition_ordinal = input_row.partition_ordinal
    WHERE input_row.operation_id = p_operation_id
      AND ref.staged_page_sequence = v_page_sequence
  ) <> v_row_count THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_TAMPERED'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'WORKBENCH_CERTIFICATE_TAMPERED',
        'operation_id', p_operation_id,
        'stage_kind', v_stage_kind,
        'reason', 'FROZEN_CANDIDATE_INPUT_COUNT_MISMATCH'
      )::text;
  END IF;

  v_receipt_preimage := '{"after_ordinal":' || COALESCE(pg_catalog.to_jsonb(p_after_ordinal)::text, 'null')
    || ',"has_more":' || pg_catalog.to_jsonb(v_has_more)::text
    || ',"next_after_ordinal":' || COALESCE(pg_catalog.to_jsonb(v_next_after)::text, 'null')
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
    p_operation_id, v_stage_kind, v_page_sequence, p_after_ordinal, v_limit,
    p_expected_previous_receipt_sha256, v_request_digest, v_row_count,
    v_canonical_bytes, v_next_after, v_has_more, true,
    v_receipt_digest, CASE WHEN v_has_more THEN 'COMMITTED' ELSE 'TERMINAL' END
  );

  RETURN pg_catalog.jsonb_build_object(
    'operation_id', p_operation_id,
    'stage_kind', v_stage_kind,
    'page_sequence', v_page_sequence,
    'after_ordinal', p_after_ordinal,
    'row_count', v_row_count,
    'canonical_byte_count', v_canonical_bytes,
    'page_receipt_digest_sha256', v_receipt_digest,
    'next_after_ordinal', v_next_after,
    'has_more', v_has_more,
    'terminal_sentinel_present', true,
    'stage_total_count', v_scope.partition_count,
    'stage_total_digest_sha256', v_scope.selected_partitions_digest_sha256,
    'replayed', false
  );
END;
$function$;

ALTER FUNCTION public.pay_workbench_draft_certificate_partition_ref_page_v8(uuid,integer,integer,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_draft_certificate_partition_ref_page_v8(uuid,integer,integer,text) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.pay_workbench_draft_certificate_partition_ref_page_v8(uuid,integer,integer,text) TO service_role;

CREATE OR REPLACE FUNCTION public.pay_workbench_draft_certificate_final_freeze_v8(
  p_operation_id uuid,
  p_expected_last_stage_receipt_sha256 text
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
  v_constituent_terminal private.banking_pay_draft_frozen_stage_receipts_v8%ROWTYPE;
  v_partition_terminal private.banking_pay_draft_frozen_stage_receipts_v8%ROWTYPE;
  v_candidate_scope_terminal private.banking_pay_draft_frozen_stage_receipts_v8%ROWTYPE;
  v_certificate private.banking_pay_workbench_settled_certificates_v8%ROWTYPE;
  v_reference_result jsonb;
  v_constituent_count integer;
  v_partition_count integer;
  v_payload_count integer;
  v_candidate_input_count integer;
  v_receipt_preimage text;
  v_receipt_digest text;
BEGIN
  PERFORM public.banking_pay_hot_path_budget_apply('WORKBENCH_CHUNK');

  IF p_operation_id IS NULL
     OR p_expected_last_stage_receipt_sha256 IS NULL
     OR p_expected_last_stage_receipt_sha256 !~ '^[0-9a-f]{64}$' THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_PAGE_RECEIPT_MISMATCH'
      USING ERRCODE = 'P0001', DETAIL = '{"code":"WORKBENCH_CERTIFICATE_PAGE_RECEIPT_MISMATCH"}';
  END IF;

  SELECT operation_row.*
  INTO v_operation
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.id = p_operation_id
  FOR UPDATE;
  IF NOT FOUND OR pg_catalog.upper(pg_catalog.btrim(COALESCE(v_operation.operation_type, ''))) <> 'DRAFT_CREATE' THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_NOT_FOUND'
      USING ERRCODE = 'P0001', DETAIL = '{"code":"WORKBENCH_CERTIFICATE_NOT_FOUND"}';
  END IF;

  SELECT scope_row.*
  INTO v_scope
  FROM private.banking_pay_draft_frozen_certificate_scopes_v8 AS scope_row
  WHERE scope_row.operation_id = p_operation_id
  FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_NOT_FOUND'
      USING ERRCODE = 'P0001', DETAIL = '{"code":"WORKBENCH_CERTIFICATE_NOT_FOUND"}';
  END IF;

  SELECT link_row.*
  INTO v_link
  FROM private.banking_pay_workbench_settled_certificate_operation_links_v8 AS link_row
  WHERE link_row.operation_id = p_operation_id;
  IF NOT FOUND OR v_link.certificate_uuid IS DISTINCT FROM v_scope.certificate_uuid THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_IDENTITY_MISMATCH'
      USING ERRCODE = 'P0001', DETAIL = '{"code":"WORKBENCH_CERTIFICATE_IDENTITY_MISMATCH"}';
  END IF;

  SELECT certificate_row.*
  INTO v_certificate
  FROM private.banking_pay_workbench_settled_certificates_v8 AS certificate_row
  WHERE certificate_row.certificate_uuid = v_scope.certificate_uuid;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_NOT_FOUND'
      USING ERRCODE = 'P0001', DETAIL = '{"code":"WORKBENCH_CERTIFICATE_NOT_FOUND"}';
  END IF;

  v_receipt_preimage := '{"certificate_uuid":' || pg_catalog.to_jsonb(v_scope.certificate_uuid::text)::text
    || ',"constituent_count":' || v_scope.constituent_count::text
    || ',"manifest_digest_sha256":' || pg_catalog.to_jsonb(v_scope.manifest_digest_sha256)::text
    || ',"operation_id":' || pg_catalog.to_jsonb(p_operation_id::text)::text
    || ',"partition_count":' || v_scope.partition_count::text
    || ',"selected_constituents_digest_sha256":' || pg_catalog.to_jsonb(v_scope.selected_constituents_digest_sha256)::text
    || ',"selected_partitions_digest_sha256":' || pg_catalog.to_jsonb(v_scope.selected_partitions_digest_sha256)::text || '}';
  v_receipt_digest := pg_catalog.encode(
    extensions.digest(pg_catalog.convert_to(v_receipt_preimage, 'UTF8'), 'sha256'), 'hex');

  IF v_scope.freeze_state = 'FROZEN' THEN
    RETURN pg_catalog.jsonb_build_object(
      'operation_id', p_operation_id,
      'freeze_state', 'FROZEN',
      'frozen_at_utc', v_scope.frozen_at_utc,
      'constituent_count', v_scope.constituent_count,
      'partition_count', v_scope.partition_count,
      'manifest_digest_sha256', v_scope.manifest_digest_sha256,
      'frozen_receipt_sha256', v_receipt_digest,
      'replayed', true
    );
  END IF;
  IF v_scope.freeze_state <> 'STAGING' THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_STAGE_ALREADY_FROZEN'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object('code', 'WORKBENCH_CERTIFICATE_STAGE_ALREADY_FROZEN', 'freeze_state', v_scope.freeze_state)::text;
  END IF;

  SELECT receipt_row.*
  INTO v_constituent_terminal
  FROM private.banking_pay_draft_frozen_stage_receipts_v8 AS receipt_row
  WHERE receipt_row.operation_id = p_operation_id
    AND receipt_row.stage_kind = 'CERTIFICATE_CONSTITUENT_REFS'
    AND receipt_row.stage_status = 'TERMINAL'
    AND NOT receipt_row.has_more;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_STAGE_INCOMPLETE'
      USING ERRCODE = 'P0001', DETAIL = '{"code":"WORKBENCH_CERTIFICATE_STAGE_INCOMPLETE","stage":"CERTIFICATE_CONSTITUENT_REFS"}';
  END IF;

  SELECT receipt_row.*
  INTO v_partition_terminal
  FROM private.banking_pay_draft_frozen_stage_receipts_v8 AS receipt_row
  WHERE receipt_row.operation_id = p_operation_id
    AND receipt_row.stage_kind = 'CERTIFICATE_PARTITION_REFS'
    AND receipt_row.stage_status = 'TERMINAL'
    AND NOT receipt_row.has_more;
  IF NOT FOUND OR v_partition_terminal.receipt_digest_sha256 IS DISTINCT FROM p_expected_last_stage_receipt_sha256 THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_STAGE_INCOMPLETE'
      USING ERRCODE = 'P0001', DETAIL = '{"code":"WORKBENCH_CERTIFICATE_STAGE_INCOMPLETE","stage":"CERTIFICATE_PARTITION_REFS"}';
  END IF;

  SELECT receipt_row.*
  INTO v_candidate_scope_terminal
  FROM private.banking_pay_draft_frozen_stage_receipts_v8 AS receipt_row
  WHERE receipt_row.operation_id = p_operation_id
    AND receipt_row.stage_kind = 'CANDIDATE_SCOPE'
    AND receipt_row.stage_status = 'TERMINAL'
    AND NOT receipt_row.has_more;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_STAGE_INCOMPLETE'
      USING ERRCODE = 'P0001', DETAIL = '{"code":"WORKBENCH_CERTIFICATE_STAGE_INCOMPLETE","stage":"CANDIDATE_SCOPE"}';
  END IF;

  SELECT pg_catalog.count(*)::integer
  INTO v_constituent_count
  FROM private.banking_pay_draft_frozen_constituent_refs_v8 AS ref
  WHERE ref.operation_id = p_operation_id;
  SELECT pg_catalog.count(*)::integer
  INTO v_partition_count
  FROM private.banking_pay_draft_frozen_partition_refs_v8 AS ref
  WHERE ref.operation_id = p_operation_id;
  SELECT pg_catalog.count(*)::integer
  INTO v_payload_count
  FROM private.banking_pay_draft_frozen_constituent_payloads_v8 AS payload
  WHERE payload.operation_id = p_operation_id;
  SELECT pg_catalog.count(*)::integer
  INTO v_candidate_input_count
  FROM private.banking_pay_draft_frozen_candidate_inputs_v8 AS input_row
  WHERE input_row.operation_id = p_operation_id;

  IF v_constituent_count <> v_scope.constituent_count
     OR v_payload_count <> v_scope.constituent_count
     OR v_partition_count <> v_scope.partition_count
     OR v_candidate_input_count <> v_scope.partition_count THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_STAGE_INCOMPLETE'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'WORKBENCH_CERTIFICATE_STAGE_INCOMPLETE',
        'expected_constituent_count', v_scope.constituent_count,
        'actual_constituent_count', v_constituent_count,
        'actual_constituent_payload_count', v_payload_count,
        'expected_partition_count', v_scope.partition_count,
        'actual_partition_count', v_partition_count,
        'actual_candidate_input_count', v_candidate_input_count
      )::text;
  END IF;

  v_reference_result := public.pay_workbench_settled_certificate_reference_validate_v8(
    p_operation_id, v_link.certification_id, v_link.overall_digest_sha256, 'FINAL_FREEZE');
  IF COALESCE((v_reference_result->>'ok')::boolean, false) IS NOT TRUE THEN
    RAISE EXCEPTION 'WORKBENCH_CERTIFICATE_FINAL_FREEZE_STALE'
      USING ERRCODE = 'P0001', DETAIL = '{"code":"WORKBENCH_CERTIFICATE_FINAL_FREEZE_STALE"}';
  END IF;

  UPDATE private.banking_pay_draft_frozen_certificate_scopes_v8
  SET freeze_state = 'FROZEN',
      frozen_at_utc = pg_catalog.clock_timestamp()
  WHERE operation_id = p_operation_id
  RETURNING * INTO v_scope;

  UPDATE private.banking_pay_workbench_settled_certificate_operation_links_v8
  SET link_state = 'FROZEN'
  WHERE operation_id = p_operation_id;

  -- Publish the compact legacy operation facts required by the unchanged
  -- business owners.  The selected rows remain normalized and are never
  -- reconstructed as one operation/Worker JSON array.
  UPDATE public.banking_pay_operations AS operation_update
  SET progress_json = pg_catalog.jsonb_strip_nulls(
        COALESCE(operation_update.progress_json, '{}'::jsonb)
        || pg_catalog.jsonb_build_object(
          'draft_scope_last_seeded_at_utc', pg_catalog.clock_timestamp()::text,
          'draft_scope_last_selected_row_count', v_scope.constituent_count,
          'draft_scope_last_candidate_scope_count', v_scope.partition_count,
          'draft_scope_selection_resolved_to_current', true,
          'draft_scope_row_backed_v8', true,
          'draft_scope_certificate_uuid', v_scope.certificate_uuid,
          'draft_scope_manifest_digest_sha256', v_scope.manifest_digest_sha256
        )
      ),
      scope_freeze_status = 'FROZEN',
      frozen_scope_change_generation = v_certificate.scope_change_generation_applied,
      scope_frozen_at_utc = COALESCE(operation_update.scope_frozen_at_utc, pg_catalog.clock_timestamp()),
      source_scope_seed_complete = true,
      frozen_candidate_scope_count = v_scope.partition_count,
      frozen_selected_row_count = v_scope.constituent_count,
      frozen_operation_scope_hash = (
        SELECT pg_catalog.md5(COALESCE(pg_catalog.string_agg(
          scope_hash_row.candidate_id::text || ':' || scope_hash_row.pay_channel || ':' || scope_hash_row.scope_hash,
          '|' ORDER BY scope_hash_row.pay_channel, scope_hash_row.candidate_id
        ), ''))
        FROM public.banking_pay_operation_candidate_scope AS scope_hash_row
        WHERE scope_hash_row.operation_id = p_operation_id
      ),
      frozen_source_session_version = v_certificate.session_version,
      frozen_source_snapshot_run_id = v_certificate.source_snapshot_run_id,
      post_freeze_scope_status = 'NONE',
      post_freeze_observed_generation = NULL::bigint,
      post_freeze_relevant_generation = NULL::bigint,
      post_freeze_scope_checked_at_utc = NULL::timestamptz,
      updated_at_utc = pg_catalog.clock_timestamp()
  WHERE operation_update.id = p_operation_id;

  RETURN pg_catalog.jsonb_build_object(
    'operation_id', p_operation_id,
    'freeze_state', 'FROZEN',
    'frozen_at_utc', v_scope.frozen_at_utc,
    'constituent_count', v_scope.constituent_count,
    'partition_count', v_scope.partition_count,
    'manifest_digest_sha256', v_scope.manifest_digest_sha256,
    'frozen_receipt_sha256', v_receipt_digest,
    'replayed', false
  );
END;
$function$;

ALTER FUNCTION public.pay_workbench_draft_certificate_final_freeze_v8(uuid,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_draft_certificate_final_freeze_v8(uuid,text) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.pay_workbench_draft_certificate_final_freeze_v8(uuid,text) TO service_role;
