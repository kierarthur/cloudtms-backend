-- Bounded certificate-to-Draft staging coordinator.
-- Runtime authority is Miget TEST. The `supabase` directory name is historical only.
-- One call commits at most one 256-row transport page or the final freeze.
-- It transports certified facts only and owns no eligibility, amount, gross/net,
-- tax, VAT, channel, payee, headroom, finance, settlement or provider policy.

CREATE OR REPLACE FUNCTION public.banking_pay_draft_certificate_stage_advance_v8(
  p_operation_id uuid,
  p_worker_id text
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
VOLATILE
PARALLEL UNSAFE
SET search_path TO ''
AS $function$
DECLARE
  v_started_at timestamptz := pg_catalog.clock_timestamp();
  v_operation public.banking_pay_operations%ROWTYPE;
  v_scope private.banking_pay_draft_frozen_certificate_scopes_v8%ROWTYPE;
  v_last private.banking_pay_draft_frozen_stage_receipts_v8%ROWTYPE;
  v_partition_terminal private.banking_pay_draft_frozen_stage_receipts_v8%ROWTYPE;
  v_result jsonb;
  v_stage text;
  v_next_stage text;
BEGIN
  -- Preserve the established Workbench/Draft statement, lock and idle budgets.
  -- This coordinator must never make a long RPC pass by widening them.
  PERFORM public.banking_pay_hot_path_budget_apply('WORKBENCH_CHUNK');

  IF p_operation_id IS NULL
     OR NULLIF(pg_catalog.btrim(COALESCE(p_worker_id, '')), '') IS NULL
     OR pg_catalog.octet_length(p_worker_id) > 200 THEN
    RAISE EXCEPTION 'DRAFT_CERTIFICATE_STAGE_ADVANCE_INPUT_INVALID'
      USING ERRCODE = '22023',
            DETAIL = '{"code":"DRAFT_CERTIFICATE_STAGE_ADVANCE_INPUT_INVALID"}';
  END IF;

  -- This is the sole concurrency fence. No Candidate/publication row is locked.
  SELECT operation_row.*
  INTO v_operation
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.id = p_operation_id
  FOR UPDATE;

  IF NOT FOUND
     OR pg_catalog.upper(pg_catalog.btrim(COALESCE(v_operation.operation_type, ''))) <> 'DRAFT_CREATE'
     OR pg_catalog.upper(pg_catalog.btrim(COALESCE(v_operation.status, ''))) NOT IN
        ('PENDING', 'RUNNING', 'CONTINUING', 'WAITING_RETRY') THEN
    RAISE EXCEPTION 'DRAFT_CERTIFICATE_STAGE_ADVANCE_OPERATION_NOT_RUNNABLE'
      USING ERRCODE = 'P0001',
            DETAIL = '{"code":"DRAFT_CERTIFICATE_STAGE_ADVANCE_OPERATION_NOT_RUNNABLE"}';
  END IF;

  IF COALESCE(v_operation.lease_owner, v_operation.locked_by) IS NOT NULL
     AND COALESCE(v_operation.lease_owner, v_operation.locked_by) <> p_worker_id THEN
    RAISE EXCEPTION 'DRAFT_CERTIFICATE_STAGE_ADVANCE_LEASE_OWNER_MISMATCH'
      USING ERRCODE = 'P0001',
            DETAIL = '{"code":"DRAFT_CERTIFICATE_STAGE_ADVANCE_LEASE_OWNER_MISMATCH"}';
  END IF;
  IF COALESCE(v_operation.lease_expires_at_utc, v_operation.lock_expires_at_utc) IS NOT NULL
     AND COALESCE(v_operation.lease_expires_at_utc, v_operation.lock_expires_at_utc)
         <= pg_catalog.clock_timestamp() THEN
    RAISE EXCEPTION 'DRAFT_CERTIFICATE_STAGE_ADVANCE_LEASE_EXPIRED'
      USING ERRCODE = 'P0001',
            DETAIL = '{"code":"DRAFT_CERTIFICATE_STAGE_ADVANCE_LEASE_EXPIRED"}';
  END IF;

  -- Admission deliberately owns only the immutable Workbench certificate link.
  -- The first H2 staging call materialises the compact row-backed Draft scope
  -- from that link and its exact channel manifest.  Exact replay is idempotent;
  -- a changed or partially initialised scope fails closed before financial work.
  IF pg_catalog.upper(pg_catalog.btrim(COALESCE(v_operation.phase, ''))) = 'INITIALISE' THEN
    PERFORM private.banking_pay_draft_frozen_certificate_scope_initialise_v8(
      p_operation_id
    );
  END IF;

  SELECT scope_row.*
  INTO v_scope
  FROM private.banking_pay_draft_frozen_certificate_scopes_v8 AS scope_row
  WHERE scope_row.operation_id = p_operation_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'DRAFT_CERTIFICATE_STAGE_ADVANCE_SCOPE_MISSING'
      USING ERRCODE = 'P0001',
            DETAIL = '{"code":"DRAFT_CERTIFICATE_STAGE_ADVANCE_SCOPE_MISSING"}';
  END IF;

  IF v_scope.freeze_state = 'FROZEN' THEN
    SELECT receipt_row.*
    INTO v_partition_terminal
    FROM private.banking_pay_draft_frozen_stage_receipts_v8 AS receipt_row
    WHERE receipt_row.operation_id = p_operation_id
      AND receipt_row.stage_kind = 'CERTIFICATE_PARTITION_REFS'
      AND receipt_row.stage_status = 'TERMINAL'
      AND NOT receipt_row.has_more
    ORDER BY receipt_row.page_sequence DESC
    LIMIT 1;

    RETURN pg_catalog.jsonb_build_object(
      'ok', true,
      'handled', true,
      'terminal', false,
      'stage', 'CERTIFICATE_FROZEN',
      'next_stage', 'DRAFT_BUSINESS_PHASES',
      'work_kind', 'CERTIFICATE_STAGE_ALREADY_COMPLETE',
      'operation_id', p_operation_id,
      'constituent_count', v_scope.constituent_count,
      'partition_count', v_scope.partition_count,
      'partition_terminal_receipt_sha256', v_partition_terminal.receipt_digest_sha256,
      'replayed', true,
      'page_call_count', 0,
      'freeze_call_count', 0,
      'elapsed_ms', pg_catalog.floor(EXTRACT(EPOCH FROM
        (pg_catalog.clock_timestamp() - v_started_at)) * 1000)::integer
    );
  END IF;

  IF v_scope.freeze_state <> 'STAGING' THEN
    RAISE EXCEPTION 'DRAFT_CERTIFICATE_STAGE_ADVANCE_SCOPE_STATE_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = pg_catalog.jsonb_build_object(
              'code', 'DRAFT_CERTIFICATE_STAGE_ADVANCE_SCOPE_STATE_INVALID',
              'freeze_state', v_scope.freeze_state
            )::text;
  END IF;

  IF pg_catalog.upper(pg_catalog.btrim(COALESCE(v_operation.phase, ''))) NOT IN (
    'INITIALISE',
    'CERTIFICATE_CONSTITUENT_REFS',
    'CERTIFICATE_PARTITION_REFS',
    'CANDIDATE_SCOPE',
    'CERTIFICATE_FINAL_FREEZE'
  ) THEN
    RAISE EXCEPTION 'DRAFT_CERTIFICATE_STAGE_ADVANCE_PHASE_MISMATCH'
      USING ERRCODE = 'P0001',
            DETAIL = pg_catalog.jsonb_build_object(
              'code', 'DRAFT_CERTIFICATE_STAGE_ADVANCE_PHASE_MISMATCH',
              'operation_phase', v_operation.phase,
              'freeze_state', v_scope.freeze_state
            )::text;
  END IF;

  -- Stage constituents first. A response-loss retry observes the committed last
  -- receipt and safely advances to the next page without recreating earlier work.
  SELECT receipt_row.*
  INTO v_last
  FROM private.banking_pay_draft_frozen_stage_receipts_v8 AS receipt_row
  WHERE receipt_row.operation_id = p_operation_id
    AND receipt_row.stage_kind = 'CERTIFICATE_CONSTITUENT_REFS'
  ORDER BY receipt_row.page_sequence DESC
  LIMIT 1;

  IF NOT FOUND OR v_last.has_more THEN
    v_stage := 'CERTIFICATE_CONSTITUENT_REFS';
    v_result := public.pay_workbench_draft_certificate_constituent_ref_page_v8(
      p_operation_id,
      CASE WHEN v_last.operation_id IS NULL THEN NULL ELSE v_last.next_after_ordinal END,
      256,
      CASE WHEN v_last.operation_id IS NULL THEN NULL ELSE v_last.receipt_digest_sha256 END
    );
    v_next_stage := CASE WHEN COALESCE((v_result->>'has_more')::boolean, false)
      THEN v_stage ELSE 'CERTIFICATE_PARTITION_REFS' END;
  ELSE
    -- Constituents are terminal; stage partitions.
    v_last := NULL;
    SELECT receipt_row.*
    INTO v_last
    FROM private.banking_pay_draft_frozen_stage_receipts_v8 AS receipt_row
    WHERE receipt_row.operation_id = p_operation_id
      AND receipt_row.stage_kind = 'CERTIFICATE_PARTITION_REFS'
    ORDER BY receipt_row.page_sequence DESC
    LIMIT 1;

    IF NOT FOUND OR v_last.has_more THEN
      v_stage := 'CERTIFICATE_PARTITION_REFS';
      v_result := public.pay_workbench_draft_certificate_partition_ref_page_v8(
        p_operation_id,
        CASE WHEN v_last.operation_id IS NULL THEN NULL ELSE v_last.next_after_ordinal END,
        256,
        CASE WHEN v_last.operation_id IS NULL THEN NULL ELSE v_last.receipt_digest_sha256 END
      );
      v_next_stage := CASE WHEN COALESCE((v_result->>'has_more')::boolean, false)
        THEN v_stage ELSE 'CANDIDATE_SCOPE' END;
    ELSE
      -- Partitions are terminal; build one Candidate/channel scope per call.
      v_partition_terminal := v_last;
      v_last := NULL;
      SELECT receipt_row.*
      INTO v_last
      FROM private.banking_pay_draft_frozen_stage_receipts_v8 AS receipt_row
      WHERE receipt_row.operation_id = p_operation_id
        AND receipt_row.stage_kind = 'CANDIDATE_SCOPE'
      ORDER BY receipt_row.page_sequence DESC
      LIMIT 1;

      IF NOT FOUND OR v_last.has_more THEN
        v_stage := 'CANDIDATE_SCOPE';
        v_result := public.pay_workbench_prepare_draft_scope_from_frozen_page_v8(
          p_operation_id,
          CASE WHEN v_last.operation_id IS NULL THEN NULL ELSE v_last.next_after_ordinal END,
          256,
          CASE WHEN v_last.operation_id IS NULL THEN NULL ELSE v_last.receipt_digest_sha256 END
        );
        v_next_stage := CASE WHEN COALESCE((v_result->>'has_more')::boolean, false)
          THEN v_stage ELSE 'CERTIFICATE_FINAL_FREEZE' END;
      ELSE
        -- The sole non-page call. It revalidates currentness and completeness in
        -- the same transaction and publishes the immutable Policy X boundary.
        v_stage := 'CERTIFICATE_FINAL_FREEZE';
        v_result := public.pay_workbench_draft_certificate_final_freeze_v8(
          p_operation_id,
          v_partition_terminal.receipt_digest_sha256
        );
        v_next_stage := 'DRAFT_BUSINESS_PHASES';

        UPDATE public.banking_pay_operations AS operation_row
        SET phase = 'DRAIN_TSFIN',
            progress_json = COALESCE(operation_row.progress_json, '{}'::jsonb)
              || pg_catalog.jsonb_build_object(
                'status_text', 'Certified payment selection frozen; preparing Timesheet financials.',
                'draft_v8_certificate_stage_complete', true,
                'draft_v8_certificate_partition_receipt_sha256',
                  v_partition_terminal.receipt_digest_sha256
              ),
            updated_at_utc = pg_catalog.clock_timestamp()
        WHERE operation_row.id = p_operation_id;

        RETURN pg_catalog.jsonb_build_object(
          'ok', true,
          'handled', true,
          'terminal', false,
          'stage', v_stage,
          'next_stage', v_next_stage,
          'work_kind', 'CERTIFICATE_FINAL_FREEZE',
          'operation_id', p_operation_id,
          'partition_terminal_receipt_sha256', v_partition_terminal.receipt_digest_sha256,
          'owner_result', v_result,
          'replayed', COALESCE((v_result->>'replayed')::boolean, false),
          'page_call_count', 0,
          'freeze_call_count', 1,
          'immediate_continue', true,
          'elapsed_ms', pg_catalog.floor(EXTRACT(EPOCH FROM
            (pg_catalog.clock_timestamp() - v_started_at)) * 1000)::integer
        );
      END IF;
    END IF;
  END IF;

  UPDATE public.banking_pay_operations AS operation_row
  SET phase = v_next_stage,
      progress_json = COALESCE(operation_row.progress_json, '{}'::jsonb)
        || pg_catalog.jsonb_build_object(
          'status_text', 'Staged one bounded certified Draft page.',
          'draft_v8_certificate_stage', v_stage,
          'draft_v8_certificate_next_stage', v_next_stage,
          'draft_v8_certificate_last_page_receipt_sha256',
            v_result->>'page_receipt_digest_sha256',
          'draft_v8_certificate_last_page_sequence',
            NULLIF(v_result->>'page_sequence', '')::integer,
          'draft_v8_certificate_last_page_row_count',
            NULLIF(v_result->>'row_count', '')::integer
        ),
      updated_at_utc = pg_catalog.clock_timestamp()
  WHERE operation_row.id = p_operation_id;

  RETURN pg_catalog.jsonb_build_object(
    'ok', true,
    'handled', true,
    'terminal', false,
    'stage', v_stage,
    'next_stage', v_next_stage,
    'work_kind', 'CERTIFICATE_STAGE_PAGE',
    'operation_id', p_operation_id,
    'page_receipt_digest_sha256', v_result->>'page_receipt_digest_sha256',
    'page_sequence', NULLIF(v_result->>'page_sequence', '')::integer,
    'row_count', NULLIF(v_result->>'row_count', '')::integer,
    'has_more', COALESCE((v_result->>'has_more')::boolean, false),
    'replayed', COALESCE((v_result->>'replayed')::boolean, false),
    'page_call_count', 1,
    'freeze_call_count', 0,
    'immediate_continue', true,
    'elapsed_ms', pg_catalog.floor(EXTRACT(EPOCH FROM
      (pg_catalog.clock_timestamp() - v_started_at)) * 1000)::integer
  );
END;
$function$;

ALTER FUNCTION public.banking_pay_draft_certificate_stage_advance_v8(uuid,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.banking_pay_draft_certificate_stage_advance_v8(uuid,text)
  FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.banking_pay_draft_certificate_stage_advance_v8(uuid,text)
  TO service_role;
