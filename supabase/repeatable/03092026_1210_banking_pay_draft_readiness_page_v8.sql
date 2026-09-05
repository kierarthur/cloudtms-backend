-- Bounded row-backed Create Draft readiness transport.
-- Runtime authority is Miget TEST. The `supabase` directory name is historical only.
-- This routine pages already-certified Timesheet/payee identities. It owns no
-- selection, amount, gross/net, tax, VAT, channel, payee or payment policy.

CREATE OR REPLACE FUNCTION public.banking_pay_draft_readiness_page_v8(
  p_operation_id uuid,
  p_worker_id text,
  p_readiness_kind text,
  p_after_ordinal integer DEFAULT NULL::integer,
  p_limit integer DEFAULT 100
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
VOLATILE
PARALLEL UNSAFE
SET search_path TO ''
AS $function$
DECLARE
  v_operation public.banking_pay_operations%ROWTYPE;
  v_scope private.banking_pay_draft_frozen_certificate_scopes_v8%ROWTYPE;
  v_kind text := pg_catalog.upper(pg_catalog.btrim(COALESCE(p_readiness_kind, '')));
  v_limit integer;
  v_phase text;
  v_items jsonb := '[]'::jsonb;
  v_row_count integer := 0;
  v_next_after integer := NULL::integer;
  v_has_more boolean := false;
  v_payload_bytes integer := 0;
  v_payload_digest text;
BEGIN
  PERFORM public.banking_pay_hot_path_budget_apply('WORKBENCH_CHUNK');

  IF p_operation_id IS NULL
     OR NULLIF(pg_catalog.btrim(COALESCE(p_worker_id, '')), '') IS NULL
     OR pg_catalog.octet_length(p_worker_id) > 200
     OR v_kind NOT IN ('TIMESHEET', 'PAYEE')
     OR (p_after_ordinal IS NOT NULL AND p_after_ordinal < -1) THEN
    RAISE EXCEPTION 'DRAFT_READINESS_PAGE_INPUT_INVALID'
      USING ERRCODE = '22023', DETAIL = '{"code":"DRAFT_READINESS_PAGE_INPUT_INVALID"}';
  END IF;

  v_limit := CASE v_kind
    WHEN 'TIMESHEET' THEN LEAST(GREATEST(COALESCE(p_limit, 100), 1), 100)
    ELSE LEAST(GREATEST(COALESCE(p_limit, 25), 1), 25)
  END;
  v_phase := CASE v_kind WHEN 'TIMESHEET' THEN 'DRAIN_TSFIN' ELSE 'ENSURE_PAYEE_READINESS' END;

  -- The operation row is the sole safety fence. No Candidate or publication
  -- row is locked, and each response is capped by both row count and bytes.
  SELECT operation_row.*
  INTO v_operation
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.id = p_operation_id
  FOR UPDATE;

  IF NOT FOUND
     OR pg_catalog.upper(pg_catalog.btrim(COALESCE(v_operation.operation_type, ''))) <> 'DRAFT_CREATE'
     OR pg_catalog.upper(pg_catalog.btrim(COALESCE(v_operation.status, ''))) NOT IN ('RUNNING','CONTINUING','WAITING_RETRY')
     OR pg_catalog.upper(pg_catalog.btrim(COALESCE(v_operation.phase, ''))) <> v_phase THEN
    RAISE EXCEPTION 'DRAFT_READINESS_PAGE_OPERATION_NOT_RUNNABLE'
      USING ERRCODE = '55000', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'DRAFT_READINESS_PAGE_OPERATION_NOT_RUNNABLE',
        'expected_phase', v_phase,
        'actual_phase', CASE WHEN v_operation.id IS NULL THEN NULL ELSE v_operation.phase END
      )::text;
  END IF;
  IF COALESCE(v_operation.lease_owner, v_operation.locked_by) IS NOT NULL
     AND COALESCE(v_operation.lease_owner, v_operation.locked_by) <> p_worker_id THEN
    RAISE EXCEPTION 'DRAFT_READINESS_PAGE_LEASE_OWNER_MISMATCH'
      USING ERRCODE = '55000', DETAIL = '{"code":"DRAFT_READINESS_PAGE_LEASE_OWNER_MISMATCH"}';
  END IF;
  IF COALESCE(v_operation.lease_expires_at_utc, v_operation.lock_expires_at_utc) IS NOT NULL
     AND COALESCE(v_operation.lease_expires_at_utc, v_operation.lock_expires_at_utc) <= pg_catalog.clock_timestamp() THEN
    RAISE EXCEPTION 'DRAFT_READINESS_PAGE_LEASE_EXPIRED'
      USING ERRCODE = '55000', DETAIL = '{"code":"DRAFT_READINESS_PAGE_LEASE_EXPIRED"}';
  END IF;

  SELECT scope_row.*
  INTO v_scope
  FROM private.banking_pay_draft_frozen_certificate_scopes_v8 AS scope_row
  WHERE scope_row.operation_id = p_operation_id;
  IF NOT FOUND OR v_scope.freeze_state <> 'FROZEN' THEN
    RAISE EXCEPTION 'DRAFT_READINESS_PAGE_SCOPE_NOT_FROZEN'
      USING ERRCODE = '55000', DETAIL = '{"code":"DRAFT_READINESS_PAGE_SCOPE_NOT_FROZEN"}';
  END IF;

  IF v_kind = 'TIMESHEET' THEN
    WITH distinct_identities AS (
      SELECT payload.timesheet_id,
             pg_catalog.min(payload.constituent_ordinal)::integer AS source_ordinal
      FROM private.banking_pay_draft_frozen_constituent_payloads_v8 AS payload
      WHERE payload.operation_id = p_operation_id
        AND payload.timesheet_id IS NOT NULL
      GROUP BY payload.timesheet_id
    ), bounded AS (
      SELECT identity_row.timesheet_id, identity_row.source_ordinal
      FROM distinct_identities AS identity_row
      WHERE identity_row.source_ordinal > COALESCE(p_after_ordinal, -1)
      ORDER BY identity_row.source_ordinal, identity_row.timesheet_id
      LIMIT v_limit + 1
    ), numbered AS (
      SELECT bounded.*, pg_catalog.row_number() OVER (
        ORDER BY bounded.source_ordinal, bounded.timesheet_id)::integer AS page_row_number
      FROM bounded
    )
    SELECT COALESCE(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
             'source_ordinal', numbered.source_ordinal,
             'timesheet_id', numbered.timesheet_id::text
           ) ORDER BY numbered.source_ordinal, numbered.timesheet_id)
             FILTER (WHERE numbered.page_row_number <= v_limit), '[]'::jsonb),
           pg_catalog.count(*) FILTER (WHERE numbered.page_row_number <= v_limit)::integer,
           pg_catalog.max(numbered.source_ordinal) FILTER (WHERE numbered.page_row_number <= v_limit)::integer,
           COALESCE(pg_catalog.bool_or(numbered.page_row_number = v_limit + 1), false)
    INTO v_items, v_row_count, v_next_after, v_has_more
    FROM numbered;
  ELSE
    WITH bounded AS (
      SELECT input_row.partition_ordinal,
             input_row.candidate_id,
             input_row.resolved_pay_channel,
             input_row.effective_payees_json
      FROM private.banking_pay_draft_frozen_candidate_inputs_v8 AS input_row
      WHERE input_row.operation_id = p_operation_id
        AND input_row.partition_ordinal > COALESCE(p_after_ordinal, -1)
      ORDER BY input_row.partition_ordinal
      LIMIT v_limit + 1
    ), numbered AS (
      SELECT bounded.*, pg_catalog.row_number() OVER (
        ORDER BY bounded.partition_ordinal)::integer AS page_row_number
      FROM bounded
    )
    SELECT COALESCE(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
             'candidate_id', numbered.candidate_id::text,
             'effective_payees', numbered.effective_payees_json,
             'partition_ordinal', numbered.partition_ordinal,
             'resolved_pay_channel', numbered.resolved_pay_channel
           ) ORDER BY numbered.partition_ordinal)
             FILTER (WHERE numbered.page_row_number <= v_limit), '[]'::jsonb),
           pg_catalog.count(*) FILTER (WHERE numbered.page_row_number <= v_limit)::integer,
           pg_catalog.max(numbered.partition_ordinal) FILTER (WHERE numbered.page_row_number <= v_limit)::integer,
           COALESCE(pg_catalog.bool_or(numbered.page_row_number = v_limit + 1), false)
    INTO v_items, v_row_count, v_next_after, v_has_more
    FROM numbered;
  END IF;

  v_payload_bytes := pg_catalog.octet_length(v_items::text);
  IF v_payload_bytes > 524288 THEN
    RAISE EXCEPTION 'DRAFT_READINESS_PAGE_BYTES_EXCEEDED'
      USING ERRCODE = '54000', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'DRAFT_READINESS_PAGE_BYTES_EXCEEDED',
        'readiness_kind', v_kind,
        'payload_byte_count', v_payload_bytes,
        'maximum_payload_byte_count', 524288
      )::text;
  END IF;
  v_payload_digest := pg_catalog.encode(extensions.digest(
    pg_catalog.convert_to(v_items::text, 'UTF8'), 'sha256'), 'hex');

  RETURN pg_catalog.jsonb_build_object(
    'contract', 'BANKING_PAY_DRAFT_READINESS_PAGE_V8',
    'ok', true,
    'operation_id', p_operation_id,
    'readiness_kind', v_kind,
    'after_ordinal', p_after_ordinal,
    'requested_limit', v_limit,
    'row_count', v_row_count,
    'items', v_items,
    'next_after_ordinal', v_next_after,
    'has_more', v_has_more,
    'terminal_sentinel_present', true,
    'payload_byte_count', v_payload_bytes,
    'payload_digest_sha256', v_payload_digest
  );
END;
$function$;

ALTER FUNCTION public.banking_pay_draft_readiness_page_v8(uuid,text,text,integer,integer) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.banking_pay_draft_readiness_page_v8(uuid,text,text,integer,integer)
  FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.banking_pay_draft_readiness_page_v8(uuid,text,text,integer,integer)
  TO service_role;

NOTIFY pgrst, 'reload schema';
