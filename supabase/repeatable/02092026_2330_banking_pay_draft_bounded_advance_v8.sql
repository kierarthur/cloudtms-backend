-- Bounded Banking Pay Draft business advancement for the certified V8 route.
-- Runtime authority is Miget TEST. Each public call performs at most one
-- existing-owner business page. The selected universe remains row-backed and
-- every financial, PAYE, Umbrella, VAT, recovery, reservation and item decision
-- remains owned by the unchanged Banking Pay functions called below.

CREATE OR REPLACE FUNCTION private.banking_pay_draft_owner_receipt_record_v8(
  p_operation_id uuid,
  p_phase text,
  p_candidate_scope_ordinal integer,
  p_delegated_owner_identity text,
  p_owner_result_json jsonb,
  p_owner_has_more boolean DEFAULT false,
  p_owner_processed_count integer DEFAULT 1,
  p_owner_remaining_count integer DEFAULT 0
)
RETURNS text
LANGUAGE plpgsql
SECURITY INVOKER
VOLATILE
SET search_path TO ''
AS $function$
DECLARE
  v_phase text := pg_catalog.upper(pg_catalog.btrim(COALESCE(p_phase, '')));
  v_unit private.banking_pay_draft_phase_units_v1%ROWTYPE;
  v_result jsonb := COALESCE(p_owner_result_json, '{}'::jsonb);
  v_request_digest text;
  v_result_digest text;
  v_receipt_digest text;
BEGIN
  IF p_operation_id IS NULL
     OR p_candidate_scope_ordinal IS NULL OR p_candidate_scope_ordinal < 0
     OR NULLIF(pg_catalog.btrim(COALESCE(p_delegated_owner_identity, '')), '') IS NULL
     OR pg_catalog.jsonb_typeof(v_result) <> 'object'
     OR COALESCE(p_owner_processed_count, -1) NOT BETWEEN 0 AND 100
     OR COALESCE(p_owner_remaining_count, -1) < 0 THEN
    RAISE EXCEPTION 'DRAFT_OWNER_RECEIPT_INPUT_INVALID'
      USING ERRCODE = '22023', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'DRAFT_OWNER_RECEIPT_INPUT_INVALID',
        'operation_id', p_operation_id,
        'phase', NULLIF(v_phase, ''),
        'candidate_scope_ordinal', p_candidate_scope_ordinal
      )::text;
  END IF;

  SELECT phase_unit.*
  INTO v_unit
  FROM private.banking_pay_draft_phase_units_v1 AS phase_unit
  WHERE phase_unit.operation_id = p_operation_id
    AND phase_unit.phase = v_phase
    AND phase_unit.candidate_scope_ordinal = p_candidate_scope_ordinal
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'DRAFT_PHASE_UNIT_NOT_FOUND'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'DRAFT_PHASE_UNIT_NOT_FOUND',
        'operation_id', p_operation_id,
        'phase', v_phase,
        'candidate_scope_ordinal', p_candidate_scope_ordinal
      )::text;
  END IF;

  IF v_unit.unit_state = 'COMPLETE' THEN
    IF COALESCE(p_owner_has_more, false) OR COALESCE(p_owner_remaining_count, 0) <> 0 THEN
      RAISE EXCEPTION 'DRAFT_PHASE_UNIT_REPLAY_CONFLICT'
        USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
          'code', 'DRAFT_PHASE_UNIT_REPLAY_CONFLICT',
          'operation_id', p_operation_id,
          'phase', v_phase,
          'candidate_scope_ordinal', p_candidate_scope_ordinal
        )::text;
    END IF;
    RETURN v_unit.last_owner_receipt_sha256;
  END IF;

  v_request_digest := pg_catalog.encode(extensions.digest(pg_catalog.convert_to(
    pg_catalog.jsonb_build_object(
      'candidate_scope_ordinal', p_candidate_scope_ordinal,
      'delegated_owner_identity', p_delegated_owner_identity,
      'operation_id', p_operation_id,
      'owner_iteration', v_unit.next_owner_iteration,
      'phase', v_phase,
      'previous_receipt_sha256', v_unit.last_owner_receipt_sha256
    )::text, 'UTF8'), 'sha256'), 'hex');
  v_result_digest := pg_catalog.encode(extensions.digest(
    pg_catalog.convert_to(v_result::text, 'UTF8'), 'sha256'), 'hex');
  v_receipt_digest := pg_catalog.encode(extensions.digest(pg_catalog.convert_to(
    pg_catalog.jsonb_build_object(
      'owner_has_more', COALESCE(p_owner_has_more, false),
      'owner_processed_count', COALESCE(p_owner_processed_count, 0),
      'owner_remaining_count', COALESCE(p_owner_remaining_count, 0),
      'owner_result_digest_sha256', v_result_digest,
      'request_digest_sha256', v_request_digest
    )::text, 'UTF8'), 'sha256'), 'hex');

  INSERT INTO private.banking_pay_draft_owner_receipts_v1(
    operation_id, phase, candidate_scope_ordinal, owner_iteration,
    request_digest_sha256, previous_receipt_sha256, delegated_owner_identity,
    owner_result_json, owner_result_digest_sha256, owner_has_more,
    owner_processed_count, owner_after_ordinal, owner_next_after_ordinal,
    owner_remaining_count, terminal_sentinel_present, receipt_digest_sha256,
    completed_at_utc
  ) VALUES (
    p_operation_id, v_phase, p_candidate_scope_ordinal, v_unit.next_owner_iteration,
    v_request_digest, v_unit.last_owner_receipt_sha256, p_delegated_owner_identity,
    v_result, v_result_digest, COALESCE(p_owner_has_more, false),
    COALESCE(p_owner_processed_count, 0),
    CASE WHEN v_unit.next_owner_iteration = 0 THEN NULL ELSE v_unit.next_owner_iteration - 1 END,
    CASE WHEN COALESCE(p_owner_has_more, false) THEN v_unit.next_owner_iteration ELSE NULL END,
    COALESCE(p_owner_remaining_count, 0), true, v_receipt_digest,
    pg_catalog.clock_timestamp()
  );

  UPDATE private.banking_pay_draft_phase_units_v1 AS phase_unit
  SET unit_state = CASE WHEN COALESCE(p_owner_has_more, false)
        THEN 'WAITING_CONTINUATION' ELSE 'COMPLETE' END,
      next_owner_iteration = v_unit.next_owner_iteration + 1,
      last_owner_receipt_sha256 = v_receipt_digest,
      completed_at_utc = CASE WHEN COALESCE(p_owner_has_more, false)
        THEN NULL ELSE pg_catalog.clock_timestamp() END
  WHERE phase_unit.operation_id = p_operation_id
    AND phase_unit.phase = v_phase
    AND phase_unit.candidate_scope_ordinal = p_candidate_scope_ordinal;

  RETURN v_receipt_digest;
END;
$function$;

ALTER FUNCTION private.banking_pay_draft_owner_receipt_record_v8(uuid,text,integer,text,jsonb,boolean,integer,integer) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.banking_pay_draft_owner_receipt_record_v8(uuid,text,integer,text,jsonb,boolean,integer,integer)
  FROM PUBLIC, anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION public.banking_pay_draft_advance_bounded_v8(
  p_operation_id uuid,
  p_worker_id text,
  p_expected_partition_stage_receipt_sha256 text
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
  v_terminal private.banking_pay_draft_operation_terminal_results_v8%ROWTYPE;
  v_scope private.banking_pay_draft_frozen_certificate_scopes_v8%ROWTYPE;
  v_last_seed private.banking_pay_draft_frozen_stage_receipts_v8%ROWTYPE;
  v_freeze jsonb;
  v_seed jsonb;
  v_shell record;
  v_group record;
  v_result jsonb := '{}'::jsonb;
  v_integrity jsonb;
  v_parity jsonb;
  v_phase text;
  v_next_phase text;
  v_owner text;
  v_scope_ids jsonb := '[]'::jsonb;
  v_scope_ordinals integer[] := ARRAY[]::integer[];
  v_scope_ordinal integer;
  v_page_size integer;
  v_pre_remaining integer := 0;
  v_post_remaining integer := 0;
  v_processed integer := 0;
  v_result_reservation_remaining integer := 0;
  v_summary_processed integer := 0;
  v_summary_remaining integer := 0;
  v_has_more boolean := false;
  v_transition_count integer := 0;
  v_pay_date date;
  v_week_start date;
  v_batch_kind text;
  v_receipt text;
  v_parity_count integer;
  v_mismatch_count integer;
  v_integrity_code text;
  v_integrity_pass_count integer;
  v_integrity_skipped_count integer;
  v_patch jsonb;
  v_patch_code text;
  v_created_batch_ids jsonb;
  v_skipped_batch_ids jsonb;
  v_cancelled_batch_ids jsonb;
  v_replacement_idempotency_key text;
BEGIN
  -- This installs the existing 15s/1.5s/30s Workbench-chunk safety budgets.
  -- V8 neither raises nor disables them and performs only one business page.
  PERFORM public.banking_pay_hot_path_budget_apply('WORKBENCH_CHUNK');

  IF p_operation_id IS NULL
     OR NULLIF(pg_catalog.btrim(COALESCE(p_worker_id, '')), '') IS NULL
     OR COALESCE(p_expected_partition_stage_receipt_sha256, '') !~ '^[0-9a-f]{64}$' THEN
    RAISE EXCEPTION 'DRAFT_BOUNDED_ADVANCE_INPUT_INVALID'
      USING ERRCODE = '22023', DETAIL = '{"code":"DRAFT_BOUNDED_ADVANCE_INPUT_INVALID"}';
  END IF;

  SELECT terminal_row.*
  INTO v_terminal
  FROM private.banking_pay_draft_operation_terminal_results_v8 AS terminal_row
  WHERE terminal_row.operation_id = p_operation_id;
  IF FOUND THEN
    RETURN v_terminal.legacy_terminal_result_json
      || v_terminal.additive_certificate_diagnostics_json
      || pg_catalog.jsonb_build_object('replayed', true, 'bounded_advance_v8', true);
  END IF;

  SELECT operation_row.*
  INTO v_operation
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.id = p_operation_id
  FOR UPDATE;

  IF NOT FOUND
     OR pg_catalog.upper(pg_catalog.btrim(COALESCE(v_operation.operation_type, ''))) <> 'DRAFT_CREATE'
     OR pg_catalog.upper(pg_catalog.btrim(COALESCE(v_operation.status, ''))) NOT IN ('RUNNING', 'CONTINUING', 'WAITING_RETRY') THEN
    RAISE EXCEPTION 'DRAFT_BOUNDED_ADVANCE_OPERATION_NOT_RUNNABLE'
      USING ERRCODE = 'P0001', DETAIL = '{"code":"DRAFT_BOUNDED_ADVANCE_OPERATION_NOT_RUNNABLE"}';
  END IF;
  IF COALESCE(v_operation.lease_owner, v_operation.locked_by) IS NOT NULL
     AND COALESCE(v_operation.lease_owner, v_operation.locked_by) <> p_worker_id THEN
    RAISE EXCEPTION 'DRAFT_BOUNDED_ADVANCE_LEASE_OWNER_MISMATCH'
      USING ERRCODE = 'P0001', DETAIL = '{"code":"DRAFT_BOUNDED_ADVANCE_LEASE_OWNER_MISMATCH"}';
  END IF;
  IF COALESCE(v_operation.lease_expires_at_utc, v_operation.lock_expires_at_utc) IS NOT NULL
     AND COALESCE(v_operation.lease_expires_at_utc, v_operation.lock_expires_at_utc) <= pg_catalog.clock_timestamp() THEN
    RAISE EXCEPTION 'DRAFT_BOUNDED_ADVANCE_LEASE_EXPIRED'
      USING ERRCODE = 'P0001', DETAIL = '{"code":"DRAFT_BOUNDED_ADVANCE_LEASE_EXPIRED"}';
  END IF;

  SELECT scope_row.*
  INTO v_scope
  FROM private.banking_pay_draft_frozen_certificate_scopes_v8 AS scope_row
  WHERE scope_row.operation_id = p_operation_id
  FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'DRAFT_BOUNDED_ADVANCE_CERTIFICATE_SCOPE_MISSING'
      USING ERRCODE = 'P0001';
  END IF;

  IF v_scope.freeze_state = 'STAGING' THEN
    RAISE EXCEPTION 'DRAFT_BOUNDED_ADVANCE_CERTIFICATE_NOT_FROZEN'
      USING ERRCODE = '55000', DETAIL = '{"code":"DRAFT_BOUNDED_ADVANCE_CERTIFICATE_NOT_FROZEN"}';
  END IF;
  IF v_scope.freeze_state <> 'FROZEN' THEN
    RAISE EXCEPTION 'DRAFT_BOUNDED_ADVANCE_FREEZE_STATE_INVALID'
      USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'DRAFT_BOUNDED_ADVANCE_FREEZE_STATE_INVALID',
        'freeze_state', v_scope.freeze_state
      )::text;
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM private.banking_pay_draft_frozen_stage_receipts_v8 AS receipt_row
    WHERE receipt_row.operation_id = p_operation_id
      AND receipt_row.stage_kind = 'CERTIFICATE_PARTITION_REFS'
      AND receipt_row.stage_status = 'TERMINAL'
      AND receipt_row.has_more IS FALSE
      AND receipt_row.receipt_digest_sha256 = p_expected_partition_stage_receipt_sha256
  ) THEN
    RAISE EXCEPTION 'DRAFT_BOUNDED_ADVANCE_PARTITION_RECEIPT_MISMATCH'
      USING ERRCODE = '55000', DETAIL = '{"code":"DRAFT_BOUNDED_ADVANCE_PARTITION_RECEIPT_MISMATCH"}';
  END IF;

  v_phase := pg_catalog.upper(pg_catalog.btrim(COALESCE(v_operation.phase, '')));
  IF v_phase NOT IN (
    'SEED_ALLOCATION_ROWS','CREATE_BATCH_SHELLS','INSERT_CANDIDATES','INSERT_ITEMS',
    'APPLY_FINANCE_ADJUSTMENTS','FINALISE_RESERVATIONS','POPULATE_CANDIDATE_SUMMARIES',
    'CREATE_TIMESHEET_SNAPSHOTS','BUILD_ITEM_BREAKDOWNS','ASSERT_INTEGRITY',
    'CONSTITUENT_PARITY','POST_CREATE_REFRESH'
  ) THEN
    RAISE EXCEPTION 'DRAFT_BOUNDED_ADVANCE_PHASE_INVALID'
      USING ERRCODE = '55000', DETAIL = pg_catalog.jsonb_build_object(
        'code', 'DRAFT_BOUNDED_ADVANCE_PHASE_INVALID',
        'phase', v_phase
      )::text;
  END IF;

  v_pay_date := NULLIF(v_operation.input_json->'same_week_paye_override'->>'pay_date', '')::date;
  v_week_start := NULLIF(v_operation.input_json->'same_week_paye_override'->>'pay_week_start', '')::date;
  IF v_pay_date IS NULL OR v_week_start IS NULL THEN
    RAISE EXCEPTION 'DRAFT_BOUNDED_ADVANCE_PAY_PERIOD_MISSING'
      USING ERRCODE = '55000', DETAIL = '{"code":"DRAFT_BOUNDED_ADVANCE_PAY_PERIOD_MISSING"}';
  END IF;
  v_batch_kind := COALESCE(NULLIF(v_operation.input_json->>'batch_kind', ''), 'STANDARD_PAYRUN');

  -- Only empty stage transitions may loop. One invocation below calls at most
  -- one business owner page and returns immediately with a durable receipt.
  LOOP
    v_transition_count := v_transition_count + 1;
    IF v_transition_count > 12 THEN
      RAISE EXCEPTION 'DRAFT_BOUNDED_ADVANCE_TRANSITION_LOOP_INVALID'
        USING ERRCODE = 'P0001';
    END IF;

    IF v_phase = 'POST_CREATE_REFRESH' THEN
      -- The patch owner is part of the durable Draft contract: it marks the
      -- selected rows no longer payable and enqueues any targeted rebuild.
      -- Invoke it once per successful pay-channel batch and persist its exact
      -- result. The Worker wake is intentionally not awaited here; it is only
      -- a post-terminal nudge for work already durably owned by the database.
      INSERT INTO private.banking_pay_draft_phase_units_v1(
        operation_id, phase, candidate_scope_ordinal, unit_state
      )
      SELECT p_operation_id, 'POST_CREATE_REFRESH', created_batch.candidate_scope_ordinal, 'PENDING'
      FROM private.banking_pay_draft_operation_created_batches_v8 AS created_batch
      WHERE created_batch.operation_id = p_operation_id
        AND created_batch.integrity_state = 'PASS'
      ON CONFLICT (operation_id, phase, candidate_scope_ordinal) DO NOTHING;

      SELECT created_batch.pay_batch_id, created_batch.candidate_scope_ordinal
      INTO v_group
      FROM private.banking_pay_draft_operation_created_batches_v8 AS created_batch
      JOIN private.banking_pay_draft_phase_units_v1 AS unit_row
        ON unit_row.operation_id = created_batch.operation_id
       AND unit_row.phase = 'POST_CREATE_REFRESH'
       AND unit_row.candidate_scope_ordinal = created_batch.candidate_scope_ordinal
      WHERE created_batch.operation_id = p_operation_id
        AND created_batch.integrity_state = 'PASS'
        AND created_batch.post_refresh_state IN ('PENDING','RETRY_REQUIRED')
        AND unit_row.unit_state <> 'COMPLETE'
      ORDER BY created_batch.batch_ordinal
      LIMIT 1;

      IF FOUND THEN
        SELECT COALESCE(pg_catalog.jsonb_agg(created_batch.pay_batch_id ORDER BY created_batch.batch_ordinal), '[]'::jsonb)
        INTO v_created_batch_ids
        FROM private.banking_pay_draft_operation_created_batches_v8 AS created_batch
        WHERE created_batch.operation_id = p_operation_id
          AND created_batch.integrity_state = 'PASS';
        SELECT COALESCE(pg_catalog.jsonb_agg(created_batch.pay_batch_id ORDER BY created_batch.batch_ordinal), '[]'::jsonb)
        INTO v_skipped_batch_ids
        FROM private.banking_pay_draft_operation_created_batches_v8 AS created_batch
        WHERE created_batch.operation_id = p_operation_id
          AND created_batch.integrity_state IN ('SKIPPED_EMPTY_RESERVED','CANCELLED_EMPTY_RESERVED');
        SELECT COALESCE(pg_catalog.jsonb_agg(created_batch.pay_batch_id ORDER BY created_batch.batch_ordinal), '[]'::jsonb)
        INTO v_cancelled_batch_ids
        FROM private.banking_pay_draft_operation_created_batches_v8 AS created_batch
        WHERE created_batch.operation_id = p_operation_id
          AND created_batch.integrity_state = 'CANCELLED_EMPTY_RESERVED';
        SELECT 'DRAFT_CREATE:' || p_operation_id::text || ':BATCHES:' || COALESCE(
          pg_catalog.string_agg(created_batch.pay_batch_id::text, ',' ORDER BY created_batch.batch_ordinal), 'NONE')
        INTO v_replacement_idempotency_key
        FROM private.banking_pay_draft_operation_created_batches_v8 AS created_batch
        WHERE created_batch.operation_id = p_operation_id
          AND created_batch.integrity_state = 'PASS';

        v_patch := public.pay_workbench_patch_preview_after_batch_mutation(
          v_operation.workbench_session_id,
          v_group.pay_batch_id,
          'DRAFT_CREATE',
          v_operation.actor_user_id,
          pg_catalog.jsonb_build_object(
            'source', 'banking_pay_draft_advance_bounded_v8',
            'operation_id', p_operation_id,
            'operation_type', 'DRAFT_CREATE',
            'post_action_phase', 'POST_CREATE_REFRESH',
            'created_pay_batch_ids', v_created_batch_ids,
            'skipped_empty_pay_batch_ids', v_skipped_batch_ids,
            'cancelled_empty_pay_batch_ids', v_cancelled_batch_ids,
            'expected_source_session_version', v_operation.frozen_source_session_version,
            'source_session_version', v_operation.frozen_source_session_version,
            'replacement_idempotency_key', v_replacement_idempotency_key
          )
        );
        v_patch_code := pg_catalog.upper(pg_catalog.btrim(COALESCE(
          v_patch->>'fallback_reason', v_patch->>'code', '')));

        IF COALESCE((v_patch->>'replacement_session_required')::boolean, false) THEN
          UPDATE private.banking_pay_draft_operation_created_batches_v8
          SET post_refresh_state = 'REPLACEMENT_REQUIRED',
              post_refresh_attempt_count = post_refresh_attempt_count + 1,
              post_refresh_result_json = v_patch,
              post_refresh_result_digest_sha256 = pg_catalog.encode(extensions.digest(
                pg_catalog.convert_to(v_patch::text, 'UTF8'), 'sha256'), 'hex'),
              post_refresh_checked_at_utc = pg_catalog.clock_timestamp()
          WHERE operation_id = p_operation_id AND pay_batch_id = v_group.pay_batch_id;
          v_receipt := private.banking_pay_draft_owner_receipt_record_v8(
            p_operation_id, v_phase, v_group.candidate_scope_ordinal,
            'public.pay_workbench_patch_preview_after_batch_mutation(uuid,uuid,text,uuid,jsonb)',
            v_patch, false, 1, 0);
          RETURN pg_catalog.jsonb_build_object(
            'ok', true, 'handled', true, 'terminal', false,
            'phase', v_phase, 'work_kind', 'BUSINESS_OWNER_PAGE',
            'owner', 'public.pay_workbench_patch_preview_after_batch_mutation(uuid,uuid,text,uuid,jsonb)',
            'post_refresh_state', 'REPLACEMENT_REQUIRED',
            'pay_batch_id', v_group.pay_batch_id, 'receipt_sha256', v_receipt,
            'immediate_continue', true, 'business_owner_call_count', 1
          );
        END IF;

        IF COALESCE((v_patch->>'ok')::boolean, false)
           AND COALESCE((v_patch->>'patch_applied')::boolean, false) THEN
          UPDATE private.banking_pay_draft_operation_created_batches_v8
          SET post_refresh_state = 'APPLIED',
              post_refresh_attempt_count = post_refresh_attempt_count + 1,
              post_refresh_result_json = v_patch,
              post_refresh_result_digest_sha256 = pg_catalog.encode(extensions.digest(
                pg_catalog.convert_to(v_patch::text, 'UTF8'), 'sha256'), 'hex'),
              post_refresh_checked_at_utc = pg_catalog.clock_timestamp()
          WHERE operation_id = p_operation_id AND pay_batch_id = v_group.pay_batch_id;
          v_receipt := private.banking_pay_draft_owner_receipt_record_v8(
            p_operation_id, v_phase, v_group.candidate_scope_ordinal,
            'public.pay_workbench_patch_preview_after_batch_mutation(uuid,uuid,text,uuid,jsonb)',
            v_patch, false, 1, 0);
          RETURN pg_catalog.jsonb_build_object(
            'ok', true, 'handled', true, 'terminal', false,
            'phase', v_phase, 'work_kind', 'BUSINESS_OWNER_PAGE',
            'owner', 'public.pay_workbench_patch_preview_after_batch_mutation(uuid,uuid,text,uuid,jsonb)',
            'pay_batch_id', v_group.pay_batch_id, 'post_refresh_state', 'APPLIED',
            'receipt_sha256', v_receipt, 'immediate_continue', true,
            'business_owner_call_count', 1,
            'elapsed_ms', pg_catalog.floor(EXTRACT(EPOCH FROM
              (pg_catalog.clock_timestamp() - v_started_at)) * 1000)::integer
          );
        END IF;

        UPDATE private.banking_pay_draft_operation_created_batches_v8
        SET post_refresh_state = 'RETRY_REQUIRED',
            post_refresh_attempt_count = post_refresh_attempt_count + 1,
            post_refresh_result_json = v_patch,
            post_refresh_result_digest_sha256 = pg_catalog.encode(extensions.digest(
              pg_catalog.convert_to(v_patch::text, 'UTF8'), 'sha256'), 'hex'),
            post_refresh_checked_at_utc = pg_catalog.clock_timestamp()
        WHERE operation_id = p_operation_id AND pay_batch_id = v_group.pay_batch_id;
        v_receipt := private.banking_pay_draft_owner_receipt_record_v8(
          p_operation_id, v_phase, v_group.candidate_scope_ordinal,
          'public.pay_workbench_patch_preview_after_batch_mutation(uuid,uuid,text,uuid,jsonb)',
          v_patch, true, 1, 1);
        RETURN pg_catalog.jsonb_build_object(
          'ok', true, 'handled', true, 'terminal', false,
          'phase', v_phase, 'work_kind', 'POST_CREATE_REFRESH_RETRY_REQUIRED',
          'code', COALESCE(NULLIF(v_patch_code, ''), 'DRAFT_CREATE_POST_ACTION_PATCH_RPC_FAILED'),
          'pay_batch_id', v_group.pay_batch_id, 'receipt_sha256', v_receipt,
          'immediate_continue', false, 'business_owner_call_count', 1,
          'elapsed_ms', pg_catalog.floor(EXTRACT(EPOCH FROM
            (pg_catalog.clock_timestamp() - v_started_at)) * 1000)::integer
        );
      END IF;

      IF EXISTS (
        SELECT 1
        FROM private.banking_pay_draft_operation_created_batches_v8 AS created_batch
        WHERE created_batch.operation_id = p_operation_id
          AND created_batch.integrity_state = 'PASS'
          AND created_batch.post_refresh_state NOT IN ('APPLIED','REPLACEMENT_REQUIRED','APPLIED_REPLACEMENT')
      ) THEN
        RAISE EXCEPTION 'DRAFT_POST_CREATE_REFRESH_INCOMPLETE'
          USING ERRCODE = 'P0001', DETAIL = '{"code":"DRAFT_POST_CREATE_REFRESH_INCOMPLETE"}';
      END IF;

      RETURN pg_catalog.jsonb_build_object(
        'ok', true, 'handled', true, 'terminal', false,
        'phase', v_phase, 'work_kind', 'READY_FOR_TERMINAL_FINISH',
        'business_artifacts_complete', true, 'constituent_parity_complete', true,
        'post_create_patch_complete', true, 'worker_wake_required_after_terminal', true,
        'terminal_prepare', private.banking_pay_draft_terminal_context_build_v8(p_operation_id),
        'immediate_continue', true, 'business_owner_call_count', 0,
        'elapsed_ms', pg_catalog.floor(EXTRACT(EPOCH FROM
          (pg_catalog.clock_timestamp() - v_started_at)) * 1000)::integer
      );
    END IF;

    IF v_phase = 'CREATE_BATCH_SHELLS' THEN
      SELECT candidate_scope.resolved_pay_channel AS pay_channel,
             pg_catalog.min(candidate_scope.candidate_scope_ordinal)::integer AS candidate_scope_ordinal
      INTO v_group
      FROM private.banking_pay_draft_frozen_candidate_scopes_v8 AS candidate_scope
      WHERE candidate_scope.operation_id = p_operation_id
        AND candidate_scope.pay_batch_id IS NULL
      GROUP BY candidate_scope.resolved_pay_channel
      ORDER BY candidate_scope.resolved_pay_channel
      LIMIT 1;

      IF FOUND THEN
        SELECT shell_row.*
        INTO v_shell
        FROM public.pay_batch_shell_ensure_from_operation(
          p_operation_id, v_operation.workbench_session_id, v_operation.actor_user_id,
          v_batch_kind, v_group.pay_channel, v_operation.input_json
        ) AS shell_row
        LIMIT 1;
        IF v_shell.pay_batch_id IS NULL THEN
          RAISE EXCEPTION 'DRAFT_BOUNDED_ADVANCE_BATCH_SHELL_FAILED'
            USING ERRCODE = 'P0001';
        END IF;
        IF NOT EXISTS (
          SELECT 1
          FROM public.pay_batches AS batch_row
          WHERE batch_row.id = v_shell.pay_batch_id
            AND pg_catalog.upper(pg_catalog.btrim(COALESCE(batch_row.rail_provider_snapshot, '')))
                IS NOT DISTINCT FROM pg_catalog.upper(pg_catalog.btrim(COALESCE(v_operation.input_json->>'rail_provider_snapshot', '')))
            AND pg_catalog.upper(pg_catalog.btrim(COALESCE(batch_row.rail_env_snapshot, '')))
                IS NOT DISTINCT FROM pg_catalog.upper(pg_catalog.btrim(COALESCE(v_operation.input_json->>'rail_env_snapshot', '')))
        ) THEN
          RAISE EXCEPTION 'DRAFT_BATCH_SHELL_PAYMENT_RAIL_SNAPSHOT_MISMATCH'
            USING ERRCODE = '55000', DETAIL = '{"code":"DRAFT_BATCH_SHELL_PAYMENT_RAIL_SNAPSHOT_MISMATCH"}';
        END IF;

        UPDATE private.banking_pay_draft_frozen_candidate_scopes_v8 AS frozen_scope
        SET pay_batch_id = public_scope.pay_batch_id,
            scope_state = 'BATCH_LINKED'
        FROM public.banking_pay_operation_candidate_scope AS public_scope
        WHERE frozen_scope.operation_id = p_operation_id
          AND frozen_scope.resolved_pay_channel = v_group.pay_channel
          AND public_scope.operation_id = frozen_scope.operation_id
          AND public_scope.candidate_id = frozen_scope.candidate_id
          AND public_scope.pay_channel = frozen_scope.resolved_pay_channel
          AND public_scope.pay_batch_id = v_shell.pay_batch_id;

        IF EXISTS (
          SELECT 1
          FROM private.banking_pay_draft_frozen_candidate_scopes_v8 AS frozen_scope
          WHERE frozen_scope.operation_id = p_operation_id
            AND frozen_scope.resolved_pay_channel = v_group.pay_channel
            AND frozen_scope.pay_batch_id IS DISTINCT FROM v_shell.pay_batch_id
        ) THEN
          RAISE EXCEPTION 'DRAFT_BOUNDED_ADVANCE_BATCH_LINK_INCOMPLETE'
            USING ERRCODE = 'P0001';
        END IF;

        INSERT INTO private.banking_pay_draft_operation_created_batches_v8(
          operation_id, batch_ordinal, pay_batch_id, candidate_scope_ordinal
        )
        SELECT p_operation_id,
               COALESCE((SELECT pg_catalog.max(existing.batch_ordinal) + 1
                 FROM private.banking_pay_draft_operation_created_batches_v8 AS existing
                 WHERE existing.operation_id = p_operation_id), 0),
               v_shell.pay_batch_id, v_group.candidate_scope_ordinal
        ON CONFLICT (operation_id, pay_batch_id) DO NOTHING;

        INSERT INTO private.banking_pay_draft_phase_units_v1(
          operation_id, phase, candidate_scope_ordinal, unit_state
        ) VALUES (p_operation_id, 'CREATE_BATCH_SHELLS', v_group.candidate_scope_ordinal, 'PENDING')
        ON CONFLICT (operation_id, phase, candidate_scope_ordinal) DO NOTHING;
        v_receipt := private.banking_pay_draft_owner_receipt_record_v8(
          p_operation_id, 'CREATE_BATCH_SHELLS', v_group.candidate_scope_ordinal,
          'public.pay_batch_shell_ensure_from_operation(uuid,uuid,uuid,text,text,jsonb)',
          pg_catalog.jsonb_build_object(
            'ok', true, 'pay_batch_id', v_shell.pay_batch_id,
            'pay_channel', v_group.pay_channel
          ), false, 1, 0);

        RETURN pg_catalog.jsonb_build_object(
          'ok', true, 'handled', true, 'terminal', false,
          'phase', v_phase, 'work_kind', 'BUSINESS_OWNER_PAGE',
          'owner', 'public.pay_batch_shell_ensure_from_operation(uuid,uuid,uuid,text,text,jsonb)',
          'pay_channel', v_group.pay_channel, 'pay_batch_id', v_shell.pay_batch_id,
          'receipt_sha256', v_receipt, 'immediate_continue', true,
          'business_owner_call_count', 1,
          'elapsed_ms', pg_catalog.floor(EXTRACT(EPOCH FROM
            (pg_catalog.clock_timestamp() - v_started_at)) * 1000)::integer
        );
      END IF;

      v_next_phase := 'INSERT_CANDIDATES';
      UPDATE public.banking_pay_operations AS operation_row
      SET phase = v_next_phase,
          progress_json = COALESCE(operation_row.progress_json, '{}'::jsonb)
            || pg_catalog.jsonb_build_object(
              'status_text', 'Draft batch shells complete.',
              'draft_v8_last_completed_phase', v_phase
            ),
          updated_at_utc = pg_catalog.clock_timestamp()
      WHERE operation_row.id = p_operation_id;
      v_phase := v_next_phase;
      CONTINUE;
    END IF;

    IF v_phase = 'ASSERT_INTEGRITY' THEN
      INSERT INTO private.banking_pay_draft_phase_units_v1(
        operation_id, phase, candidate_scope_ordinal, unit_state
      )
      SELECT p_operation_id, 'ASSERT_INTEGRITY', created_batch.candidate_scope_ordinal, 'PENDING'
      FROM private.banking_pay_draft_operation_created_batches_v8 AS created_batch
      WHERE created_batch.operation_id = p_operation_id
      ON CONFLICT (operation_id, phase, candidate_scope_ordinal) DO NOTHING;

      SELECT created_batch.pay_batch_id, created_batch.candidate_scope_ordinal
      INTO v_group
      FROM private.banking_pay_draft_operation_created_batches_v8 AS created_batch
      JOIN private.banking_pay_draft_phase_units_v1 AS unit_row
        ON unit_row.operation_id = created_batch.operation_id
       AND unit_row.phase = 'ASSERT_INTEGRITY'
       AND unit_row.candidate_scope_ordinal = created_batch.candidate_scope_ordinal
      WHERE created_batch.operation_id = p_operation_id
        AND unit_row.unit_state <> 'COMPLETE'
        AND created_batch.integrity_state = 'PENDING'
      ORDER BY created_batch.batch_ordinal
      LIMIT 1;
      IF FOUND THEN
        v_integrity := public.pay_batch_assert_integrity(
          v_group.pay_batch_id, v_operation.actor_user_id, p_operation_id);
        v_integrity_code := pg_catalog.upper(pg_catalog.btrim(COALESCE(
          v_integrity->>'error', v_integrity->>'code', '')));
        IF COALESCE((v_integrity->>'ok')::boolean, true) IS NOT TRUE
           OR COALESCE((v_integrity->>'pass')::boolean, true) IS NOT TRUE THEN
          IF v_integrity_code <> 'PAY_DRAFT_ALL_SELECTED_PAYMENTS_ALREADY_RESERVED' THEN
            UPDATE private.banking_pay_draft_operation_created_batches_v8
            SET integrity_state = 'FAILED',
                integrity_result_json = v_integrity,
                integrity_result_digest_sha256 = pg_catalog.encode(extensions.digest(
                  pg_catalog.convert_to(v_integrity::text, 'UTF8'), 'sha256'), 'hex'),
                integrity_checked_at_utc = pg_catalog.clock_timestamp()
            WHERE operation_id = p_operation_id AND pay_batch_id = v_group.pay_batch_id;
            RAISE EXCEPTION 'DRAFT_BOUNDED_ADVANCE_INTEGRITY_FAILED'
              USING ERRCODE = 'P0001', DETAIL = v_integrity::text;
          END IF;
          UPDATE private.banking_pay_draft_operation_created_batches_v8
          SET integrity_state = 'SKIPPED_EMPTY_RESERVED',
              integrity_result_json = v_integrity,
              integrity_result_digest_sha256 = pg_catalog.encode(extensions.digest(
                pg_catalog.convert_to(v_integrity::text, 'UTF8'), 'sha256'), 'hex'),
              integrity_checked_at_utc = pg_catalog.clock_timestamp()
          WHERE operation_id = p_operation_id AND pay_batch_id = v_group.pay_batch_id;
        ELSE
          UPDATE private.banking_pay_draft_operation_created_batches_v8
          SET integrity_state = 'PASS',
              integrity_result_json = v_integrity,
              integrity_result_digest_sha256 = pg_catalog.encode(extensions.digest(
                pg_catalog.convert_to(v_integrity::text, 'UTF8'), 'sha256'), 'hex'),
              integrity_checked_at_utc = pg_catalog.clock_timestamp()
          WHERE operation_id = p_operation_id AND pay_batch_id = v_group.pay_batch_id;
        END IF;
        v_receipt := private.banking_pay_draft_owner_receipt_record_v8(
          p_operation_id, v_phase, v_group.candidate_scope_ordinal,
          'public.pay_batch_assert_integrity(uuid,uuid,uuid)',
          v_integrity, false, 1, 0);
        RETURN pg_catalog.jsonb_build_object(
          'ok', true, 'handled', true, 'terminal', false,
          'phase', v_phase, 'work_kind', 'BUSINESS_OWNER_PAGE',
          'owner', 'public.pay_batch_assert_integrity(uuid,uuid,uuid)',
          'pay_batch_id', v_group.pay_batch_id,
          'integrity_state', CASE
            WHEN v_integrity_code = 'PAY_DRAFT_ALL_SELECTED_PAYMENTS_ALREADY_RESERVED'
              THEN 'SKIPPED_EMPTY_RESERVED' ELSE 'PASS' END,
          'receipt_sha256', v_receipt, 'immediate_continue', true,
          'business_owner_call_count', 1,
          'elapsed_ms', pg_catalog.floor(EXTRACT(EPOCH FROM
            (pg_catalog.clock_timestamp() - v_started_at)) * 1000)::integer
        );
      END IF;

      -- Reuse the current Worker cleanup owner. The database only records the
      -- established empty-reserved disposition after the actual batch is
      -- demonstrably CANCELLED; it never deletes or repairs financial rows.
      UPDATE private.banking_pay_draft_operation_created_batches_v8 AS created_batch
      SET integrity_state = 'CANCELLED_EMPTY_RESERVED'
      FROM public.pay_batches AS batch_row
      WHERE created_batch.operation_id = p_operation_id
        AND created_batch.integrity_state = 'SKIPPED_EMPTY_RESERVED'
        AND batch_row.id = created_batch.pay_batch_id
        AND pg_catalog.upper(pg_catalog.btrim(COALESCE(batch_row.status, ''))) IN ('CANCELLED','CANCELED');

      SELECT pg_catalog.count(*) FILTER (WHERE integrity_state = 'PASS')::integer,
             pg_catalog.count(*) FILTER (
               WHERE integrity_state IN ('SKIPPED_EMPTY_RESERVED','CANCELLED_EMPTY_RESERVED'))::integer
      INTO v_integrity_pass_count, v_integrity_skipped_count
      FROM private.banking_pay_draft_operation_created_batches_v8
      WHERE operation_id = p_operation_id;

      IF EXISTS (
        SELECT 1 FROM private.banking_pay_draft_operation_created_batches_v8
        WHERE operation_id = p_operation_id
          AND integrity_state = 'SKIPPED_EMPTY_RESERVED'
      ) THEN
        RETURN pg_catalog.jsonb_build_object(
          'ok', true, 'handled', true, 'terminal', false,
          'phase', v_phase, 'work_kind', 'EMPTY_RESERVED_BATCH_CLEANUP_REQUIRED',
          'cleanup_owner', 'existing Worker cancelSkippedEmptyReservedDraftBatches',
          'skipped_empty_pay_batch_ids', (
            SELECT COALESCE(pg_catalog.jsonb_agg(created_batch.pay_batch_id ORDER BY created_batch.batch_ordinal), '[]'::jsonb)
            FROM private.banking_pay_draft_operation_created_batches_v8 AS created_batch
            WHERE created_batch.operation_id = p_operation_id
              AND created_batch.integrity_state = 'SKIPPED_EMPTY_RESERVED'
          ),
          'immediate_continue', false, 'business_owner_call_count', 0
        );
      END IF;

      IF COALESCE(v_integrity_pass_count, 0) = 0
         AND COALESCE(v_integrity_skipped_count, 0) > 0 THEN
        RETURN pg_catalog.jsonb_build_object(
          'ok', false, 'handled', true, 'terminal', false,
          'phase', v_phase, 'work_kind', 'TERMINAL_FAILURE_REQUIRED',
          'code', 'PAY_DRAFT_ALL_SELECTED_PAYMENTS_ALREADY_RESERVED',
          'message', 'No draft was created because all selected payments are already reserved in another active draft batch.',
          'business_owner_call_count', 0
        );
      END IF;
      v_next_phase := 'CONSTITUENT_PARITY';
      UPDATE public.banking_pay_operations SET phase = v_next_phase,
        updated_at_utc = pg_catalog.clock_timestamp() WHERE id = p_operation_id;
      v_phase := v_next_phase;
      CONTINUE;
    END IF;

    IF v_phase = 'CONSTITUENT_PARITY' THEN
      SELECT receipt_row.*
      INTO v_last_seed
      FROM private.banking_pay_draft_frozen_stage_receipts_v8 AS receipt_row
      WHERE receipt_row.operation_id = p_operation_id
        AND receipt_row.stage_kind = 'CONSTITUENT_PARITY'
      ORDER BY receipt_row.page_sequence DESC
      LIMIT 1;
      IF NOT FOUND OR v_last_seed.has_more THEN
        v_parity := public.pay_workbench_draft_constituent_parity_page_v8(
          p_operation_id,
          CASE WHEN FOUND THEN v_last_seed.next_after_ordinal ELSE NULL END,
          256,
          CASE WHEN FOUND THEN v_last_seed.receipt_digest_sha256 ELSE NULL END
        );
        IF COALESCE((v_parity->>'mismatch_count')::integer, 0) <> 0 THEN
          RAISE EXCEPTION 'DRAFT_BOUNDED_ADVANCE_PARITY_FAILED'
            USING ERRCODE = 'P0001', DETAIL = v_parity::text;
        END IF;
        RETURN pg_catalog.jsonb_build_object(
          'ok', true, 'handled', true, 'terminal', false,
          'phase', v_phase, 'work_kind', 'CONSTITUENT_PARITY_PAGE',
          'row_count', COALESCE((v_parity->>'row_count')::integer, 0),
          'has_more', COALESCE((v_parity->>'has_more')::boolean, false),
          'receipt_sha256', v_parity->>'page_receipt_digest_sha256',
          'immediate_continue', true, 'business_owner_call_count', 1,
          'elapsed_ms', pg_catalog.floor(EXTRACT(EPOCH FROM
            (pg_catalog.clock_timestamp() - v_started_at)) * 1000)::integer
        );
      END IF;

      SELECT pg_catalog.count(*)::integer,
             pg_catalog.count(*) FILTER (WHERE parity_row.comparison_status <> 'MATCH')::integer
      INTO v_parity_count, v_mismatch_count
      FROM private.banking_pay_draft_constituent_parity_results_v8 AS parity_row
      WHERE parity_row.operation_id = p_operation_id;
      IF v_parity_count IS DISTINCT FROM v_operation.frozen_selected_row_count
         OR v_mismatch_count <> 0 THEN
        RAISE EXCEPTION 'DRAFT_BOUNDED_ADVANCE_PARITY_INCOMPLETE'
          USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
            'code', 'DRAFT_BOUNDED_ADVANCE_PARITY_INCOMPLETE',
            'expected_count', v_operation.frozen_selected_row_count,
            'actual_count', v_parity_count,
            'mismatch_count', v_mismatch_count
          )::text;
      END IF;
      v_next_phase := 'POST_CREATE_REFRESH';
      UPDATE public.banking_pay_operations AS operation_row
      SET phase = v_next_phase,
          progress_json = COALESCE(operation_row.progress_json, '{}'::jsonb)
            || pg_catalog.jsonb_build_object(
              'status_text', 'Draft business artifacts and constituent parity complete.',
              'draft_v8_business_artifacts_complete', true,
              'draft_v8_constituent_parity_complete', true
            ),
          updated_at_utc = pg_catalog.clock_timestamp()
      WHERE operation_row.id = p_operation_id;
      v_phase := v_next_phase;
      CONTINUE;
    END IF;

    -- Regular Candidate-scope phases seed at most one bounded scope page when
    -- no previously seeded unit is waiting. No complete selected array exists.
    IF v_phase NOT IN (
      'SEED_ALLOCATION_ROWS','INSERT_CANDIDATES','INSERT_ITEMS',
      'APPLY_FINANCE_ADJUSTMENTS','FINALISE_RESERVATIONS',
      'POPULATE_CANDIDATE_SUMMARIES','CREATE_TIMESHEET_SNAPSHOTS',
      'BUILD_ITEM_BREAKDOWNS'
    ) THEN
      RAISE EXCEPTION 'DRAFT_BOUNDED_ADVANCE_PHASE_INVALID'
        USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
          'code', 'DRAFT_BOUNDED_ADVANCE_PHASE_INVALID', 'phase', v_phase
        )::text;
    END IF;

    IF NOT EXISTS (
      SELECT 1 FROM private.banking_pay_draft_phase_units_v1 AS unit_row
      WHERE unit_row.operation_id = p_operation_id
        AND unit_row.phase = v_phase
        AND unit_row.unit_state <> 'COMPLETE'
    ) THEN
      SELECT receipt_row.*
      INTO v_last_seed
      FROM private.banking_pay_draft_frozen_stage_receipts_v8 AS receipt_row
      WHERE receipt_row.operation_id = p_operation_id
        AND receipt_row.stage_kind = 'PHASE_UNITS:' || v_phase
      ORDER BY receipt_row.page_sequence DESC
      LIMIT 1;
      IF NOT FOUND OR v_last_seed.has_more THEN
        v_seed := public.banking_pay_draft_phase_units_seed_v8(
          p_operation_id,
          CASE WHEN FOUND THEN v_last_seed.next_after_ordinal ELSE NULL END,
          256,
          CASE WHEN FOUND THEN v_last_seed.receipt_digest_sha256 ELSE NULL END
        );
      END IF;
    END IF;

    IF NOT EXISTS (
      SELECT 1 FROM private.banking_pay_draft_phase_units_v1 AS unit_row
      WHERE unit_row.operation_id = p_operation_id
        AND unit_row.phase = v_phase
        AND unit_row.unit_state <> 'COMPLETE'
    ) THEN
      SELECT receipt_row.*
      INTO v_last_seed
      FROM private.banking_pay_draft_frozen_stage_receipts_v8 AS receipt_row
      WHERE receipt_row.operation_id = p_operation_id
        AND receipt_row.stage_kind = 'PHASE_UNITS:' || v_phase
      ORDER BY receipt_row.page_sequence DESC
      LIMIT 1;
      IF NOT FOUND OR v_last_seed.has_more THEN
        RETURN pg_catalog.jsonb_build_object(
          'ok', true, 'handled', true, 'terminal', false,
          'phase', v_phase, 'work_kind', 'PHASE_UNIT_SEED_PAGE',
          'immediate_continue', true, 'business_owner_call_count', 0,
          'seed_result', COALESCE(v_seed, '{}'::jsonb)
        );
      END IF;

      v_next_phase := CASE v_phase
        WHEN 'SEED_ALLOCATION_ROWS' THEN 'CREATE_BATCH_SHELLS'
        WHEN 'INSERT_CANDIDATES' THEN 'INSERT_ITEMS'
        WHEN 'INSERT_ITEMS' THEN 'APPLY_FINANCE_ADJUSTMENTS'
        WHEN 'APPLY_FINANCE_ADJUSTMENTS' THEN 'FINALISE_RESERVATIONS'
        WHEN 'FINALISE_RESERVATIONS' THEN 'POPULATE_CANDIDATE_SUMMARIES'
        WHEN 'POPULATE_CANDIDATE_SUMMARIES' THEN 'CREATE_TIMESHEET_SNAPSHOTS'
        WHEN 'CREATE_TIMESHEET_SNAPSHOTS' THEN 'BUILD_ITEM_BREAKDOWNS'
        WHEN 'BUILD_ITEM_BREAKDOWNS' THEN 'ASSERT_INTEGRITY'
      END;
      UPDATE public.banking_pay_operations AS operation_row
      SET phase = v_next_phase,
          progress_json = COALESCE(operation_row.progress_json, '{}'::jsonb)
            || pg_catalog.jsonb_build_object(
              'status_text', v_phase || ' complete.',
              'draft_v8_last_completed_phase', v_phase
            ),
          updated_at_utc = pg_catalog.clock_timestamp()
      WHERE operation_row.id = p_operation_id;
      v_phase := v_next_phase;
      CONTINUE;
    END IF;

    v_page_size := CASE
      WHEN v_phase IN ('SEED_ALLOCATION_ROWS','APPLY_FINANCE_ADJUSTMENTS') THEN 50
      WHEN v_phase = 'FINALISE_RESERVATIONS' THEN 1
      ELSE 100
    END;

    IF v_phase = 'SEED_ALLOCATION_ROWS' THEN
      SELECT pg_catalog.jsonb_agg(page_row.public_scope_id ORDER BY page_row.candidate_scope_ordinal),
             pg_catalog.array_agg(page_row.candidate_scope_ordinal ORDER BY page_row.candidate_scope_ordinal)
      INTO v_scope_ids, v_scope_ordinals
      FROM (
        SELECT public_scope.id AS public_scope_id, unit_row.candidate_scope_ordinal
        FROM private.banking_pay_draft_phase_units_v1 AS unit_row
        JOIN private.banking_pay_draft_frozen_candidate_scopes_v8 AS frozen_scope
          ON frozen_scope.operation_id = unit_row.operation_id
         AND frozen_scope.candidate_scope_ordinal = unit_row.candidate_scope_ordinal
        JOIN public.banking_pay_operation_candidate_scope AS public_scope
          ON public_scope.operation_id = frozen_scope.operation_id
         AND public_scope.candidate_id = frozen_scope.candidate_id
         AND public_scope.pay_channel = frozen_scope.resolved_pay_channel
        WHERE unit_row.operation_id = p_operation_id
          AND unit_row.phase = v_phase
          AND unit_row.unit_state <> 'COMPLETE'
        ORDER BY unit_row.candidate_scope_ordinal
        LIMIT v_page_size
      ) AS page_row;
      SELECT NULL::uuid AS pay_batch_id, NULL::text AS pay_channel
      INTO v_group;
    ELSE
      SELECT frozen_scope.pay_batch_id, frozen_scope.resolved_pay_channel AS pay_channel
      INTO v_group
      FROM private.banking_pay_draft_phase_units_v1 AS unit_row
      JOIN private.banking_pay_draft_frozen_candidate_scopes_v8 AS frozen_scope
        ON frozen_scope.operation_id = unit_row.operation_id
       AND frozen_scope.candidate_scope_ordinal = unit_row.candidate_scope_ordinal
      WHERE unit_row.operation_id = p_operation_id
        AND unit_row.phase = v_phase
        AND unit_row.unit_state <> 'COMPLETE'
      ORDER BY unit_row.candidate_scope_ordinal
      LIMIT 1;
      IF v_group.pay_batch_id IS NULL THEN
        RAISE EXCEPTION 'DRAFT_BOUNDED_ADVANCE_BATCH_ID_MISSING' USING ERRCODE = 'P0001';
      END IF;
      SELECT pg_catalog.jsonb_agg(page_row.public_scope_id ORDER BY page_row.candidate_scope_ordinal),
             pg_catalog.array_agg(page_row.candidate_scope_ordinal ORDER BY page_row.candidate_scope_ordinal)
      INTO v_scope_ids, v_scope_ordinals
      FROM (
        SELECT public_scope.id AS public_scope_id, unit_row.candidate_scope_ordinal
        FROM private.banking_pay_draft_phase_units_v1 AS unit_row
        JOIN private.banking_pay_draft_frozen_candidate_scopes_v8 AS frozen_scope
          ON frozen_scope.operation_id = unit_row.operation_id
         AND frozen_scope.candidate_scope_ordinal = unit_row.candidate_scope_ordinal
        JOIN public.banking_pay_operation_candidate_scope AS public_scope
          ON public_scope.operation_id = frozen_scope.operation_id
         AND public_scope.candidate_id = frozen_scope.candidate_id
         AND public_scope.pay_channel = frozen_scope.resolved_pay_channel
        WHERE unit_row.operation_id = p_operation_id
          AND unit_row.phase = v_phase
          AND unit_row.unit_state <> 'COMPLETE'
          AND frozen_scope.pay_batch_id = v_group.pay_batch_id
          AND frozen_scope.resolved_pay_channel = v_group.pay_channel
        ORDER BY unit_row.candidate_scope_ordinal
        LIMIT v_page_size
      ) AS page_row;
    END IF;

    IF COALESCE(pg_catalog.jsonb_array_length(v_scope_ids), 0) = 0
       OR COALESCE(pg_catalog.cardinality(v_scope_ordinals), 0) = 0 THEN
      RAISE EXCEPTION 'DRAFT_BOUNDED_ADVANCE_SCOPE_PAGE_EMPTY' USING ERRCODE = 'P0001';
    END IF;

    UPDATE private.banking_pay_draft_phase_units_v1 AS unit_row
    SET unit_state = 'RUNNING'
    WHERE unit_row.operation_id = p_operation_id
      AND unit_row.phase = v_phase
      AND unit_row.candidate_scope_ordinal = ANY(v_scope_ordinals)
      AND unit_row.unit_state IN ('PENDING','WAITING_CONTINUATION','RUNNING');

    IF v_phase = 'FINALISE_RESERVATIONS' THEN
      SELECT pg_catalog.count(*)::integer
      INTO v_pre_remaining
      FROM public.pay_batch_items AS item_row
      JOIN public.pay_batch_candidates AS batch_candidate
        ON batch_candidate.id = item_row.pay_batch_candidate_id
      JOIN public.banking_pay_operation_candidate_allocation_rows AS allocation_row
        ON allocation_row.pay_batch_item_id = item_row.id
       AND allocation_row.operation_id = p_operation_id
      WHERE batch_candidate.pay_batch_id = v_group.pay_batch_id
        AND allocation_row.candidate_scope_id IN (
          SELECT value::uuid FROM pg_catalog.jsonb_array_elements_text(v_scope_ids) AS scope_id(value))
        AND COALESCE(item_row.is_voided, false) = false
        AND item_row.finance_case_id IS NOT NULL
        AND item_row.reservation_id IS NULL;
    END IF;

    v_result := CASE v_phase
      WHEN 'SEED_ALLOCATION_ROWS' THEN pg_catalog.to_jsonb((
        SELECT seed_row FROM public.pay_workbench_prepare_draft_allocation_rows_seed(
          p_operation_id, v_scope_ids) AS seed_row LIMIT 1))
      WHEN 'INSERT_CANDIDATES' THEN public.pay_batch_insert_candidates_from_preview(
        v_group.pay_batch_id, v_operation.actor_user_id, p_operation_id, v_scope_ids)
      WHEN 'INSERT_ITEMS' THEN public.pay_batch_insert_items_from_preview(
        v_group.pay_batch_id, v_operation.actor_user_id, p_operation_id, v_scope_ids)
      WHEN 'APPLY_FINANCE_ADJUSTMENTS' THEN public.pay_batch_apply_finance_adjustments(
        v_group.pay_batch_id, v_group.pay_channel, v_operation.actor_user_id,
        NULL, NULL, p_operation_id, v_scope_ids)
      WHEN 'FINALISE_RESERVATIONS' THEN public.pay_batch_finalize_reservations_and_markers(
        v_group.pay_batch_id, v_group.pay_channel, v_operation.actor_user_id,
        v_pay_date, v_week_start, p_operation_id, v_scope_ids)
      WHEN 'POPULATE_CANDIDATE_SUMMARIES' THEN public.pay_batch_populate_candidate_summaries(
        v_group.pay_batch_id, v_group.pay_channel, v_operation.actor_user_id,
        p_operation_id, v_scope_ids)
      WHEN 'CREATE_TIMESHEET_SNAPSHOTS' THEN public.pay_batch_create_timesheet_snapshots(
        v_group.pay_batch_id, v_operation.actor_user_id, p_operation_id, v_scope_ids)
      WHEN 'BUILD_ITEM_BREAKDOWNS' THEN public.pay_batch_build_item_breakdowns(
        v_group.pay_batch_id, v_operation.actor_user_id, p_operation_id, v_scope_ids)
    END;
    v_result := COALESCE(v_result, '{}'::jsonb);

    IF COALESCE((v_result->>'ok')::boolean, true) IS NOT TRUE
       OR COALESCE((v_result->>'failed_count')::integer,
                   (v_result->>'failed_item_rows')::integer, 0) <> 0
       OR COALESCE((v_result->>'failures')::integer, 0) <> 0 THEN
      RAISE EXCEPTION 'DRAFT_BOUNDED_ADVANCE_OWNER_FAILED'
        USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
          'code', 'DRAFT_BOUNDED_ADVANCE_OWNER_FAILED',
          'phase', v_phase, 'owner_result', v_result
        )::text;
    END IF;

    v_has_more := COALESCE((v_result->>'has_more')::boolean, false);
    IF v_phase = 'FINALISE_RESERVATIONS' THEN
      SELECT pg_catalog.count(*)::integer
      INTO v_post_remaining
      FROM public.pay_batch_items AS item_row
      JOIN public.pay_batch_candidates AS batch_candidate
        ON batch_candidate.id = item_row.pay_batch_candidate_id
      JOIN public.banking_pay_operation_candidate_allocation_rows AS allocation_row
        ON allocation_row.pay_batch_item_id = item_row.id
       AND allocation_row.operation_id = p_operation_id
      WHERE batch_candidate.pay_batch_id = v_group.pay_batch_id
        AND allocation_row.candidate_scope_id IN (
          SELECT value::uuid FROM pg_catalog.jsonb_array_elements_text(v_scope_ids) AS scope_id(value))
        AND COALESCE(item_row.is_voided, false) = false
        AND item_row.finance_case_id IS NOT NULL
        AND item_row.reservation_id IS NULL;
      IF COALESCE((v_result->>'bounded_summary_page')::boolean, false) THEN
        v_result_reservation_remaining := COALESCE((v_result->>'reservation_remaining_count')::integer, -1);
        v_summary_processed := COALESCE((v_result->>'summary_timesheets_refreshed')::integer, -1);
        v_summary_remaining := COALESCE((v_result->>'summary_remaining_count')::integer, -1);

        IF v_result_reservation_remaining IS DISTINCT FROM v_post_remaining
           OR v_summary_processed NOT BETWEEN 0 AND 25
           OR v_summary_remaining < 0
           OR v_has_more IS DISTINCT FROM ((v_post_remaining + v_summary_remaining) > 0) THEN
          RAISE EXCEPTION 'DRAFT_BOUNDED_ADVANCE_FINALIZER_PROGRESS_INVALID'
            USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
              'code', 'DRAFT_BOUNDED_ADVANCE_FINALIZER_PROGRESS_INVALID',
              'reason', 'BOUNDED_SUMMARY_RESULT_INVALID',
              'reservation_before', v_pre_remaining,
              'reservation_after', v_post_remaining,
              'result_reservation_remaining', v_result_reservation_remaining,
              'summary_processed', v_summary_processed,
              'summary_remaining', v_summary_remaining,
              'has_more', v_has_more
            )::text;
        END IF;

        IF v_pre_remaining > 0 THEN
          v_processed := v_pre_remaining - v_post_remaining;
          IF v_processed NOT BETWEEN 1 AND 100
             OR v_summary_processed <> 0 THEN
            RAISE EXCEPTION 'DRAFT_BOUNDED_ADVANCE_FINALIZER_PROGRESS_INVALID'
              USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
                'code', 'DRAFT_BOUNDED_ADVANCE_FINALIZER_PROGRESS_INVALID',
                'reason', 'RESERVATION_AND_SUMMARY_PAGE_MUST_NOT_MIX',
                'reservation_before', v_pre_remaining,
                'reservation_processed', v_processed,
                'reservation_after', v_post_remaining,
                'summary_processed', v_summary_processed
              )::text;
          END IF;
          v_post_remaining := v_post_remaining + v_summary_remaining;
        ELSE
          IF v_post_remaining <> 0
             OR (v_has_more AND (v_summary_processed <= 0 OR v_summary_remaining <= 0))
             OR (NOT v_has_more AND v_summary_remaining <> 0) THEN
            RAISE EXCEPTION 'DRAFT_BOUNDED_ADVANCE_FINALIZER_PROGRESS_INVALID'
              USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
                'code', 'DRAFT_BOUNDED_ADVANCE_FINALIZER_PROGRESS_INVALID',
                'reason', 'SUMMARY_PAGE_DID_NOT_ADVANCE',
                'summary_processed', v_summary_processed,
                'summary_remaining', v_summary_remaining,
                'has_more', v_has_more
              )::text;
          END IF;
          v_processed := v_summary_processed;
          v_post_remaining := v_summary_remaining;
        END IF;
      ELSE
        v_processed := v_pre_remaining - v_post_remaining;
        IF v_processed NOT BETWEEN 0 AND 100
           OR (v_has_more AND (v_processed <= 0 OR v_post_remaining <= 0))
           OR (NOT v_has_more AND v_post_remaining <> 0) THEN
          RAISE EXCEPTION 'DRAFT_BOUNDED_ADVANCE_FINALIZER_PROGRESS_INVALID'
            USING ERRCODE = 'P0001', DETAIL = pg_catalog.jsonb_build_object(
              'code', 'DRAFT_BOUNDED_ADVANCE_FINALIZER_PROGRESS_INVALID',
              'before', v_pre_remaining, 'processed', v_processed,
              'after', v_post_remaining, 'has_more', v_has_more
            )::text;
        END IF;
      END IF;
    ELSE
      v_processed := 1;
      v_post_remaining := CASE WHEN v_has_more THEN 1 ELSE 0 END;
    END IF;

    v_owner := CASE v_phase
      WHEN 'SEED_ALLOCATION_ROWS' THEN 'public.pay_workbench_prepare_draft_allocation_rows_seed(uuid,jsonb)'
      WHEN 'INSERT_CANDIDATES' THEN 'public.pay_batch_insert_candidates_from_preview(uuid,uuid,uuid,jsonb)'
      WHEN 'INSERT_ITEMS' THEN 'public.pay_batch_insert_items_from_preview(uuid,uuid,uuid,jsonb)'
      WHEN 'APPLY_FINANCE_ADJUSTMENTS' THEN 'public.pay_batch_apply_finance_adjustments(uuid,text,uuid,numeric,date,uuid,jsonb)'
      WHEN 'FINALISE_RESERVATIONS' THEN 'public.pay_batch_finalize_reservations_and_markers(uuid,text,uuid,date,date,uuid,jsonb)'
      WHEN 'POPULATE_CANDIDATE_SUMMARIES' THEN 'public.pay_batch_populate_candidate_summaries(uuid,text,uuid,uuid,jsonb)'
      WHEN 'CREATE_TIMESHEET_SNAPSHOTS' THEN 'public.pay_batch_create_timesheet_snapshots(uuid,uuid,uuid,jsonb)'
      WHEN 'BUILD_ITEM_BREAKDOWNS' THEN 'public.pay_batch_build_item_breakdowns(uuid,uuid,uuid,jsonb)'
    END;
    FOREACH v_scope_ordinal IN ARRAY v_scope_ordinals LOOP
      v_receipt := private.banking_pay_draft_owner_receipt_record_v8(
        p_operation_id, v_phase, v_scope_ordinal, v_owner, v_result,
        v_has_more, LEAST(GREATEST(v_processed, 0), 100), v_post_remaining);
    END LOOP;

    UPDATE public.banking_pay_operations AS operation_row
    SET progress_json = COALESCE(operation_row.progress_json, '{}'::jsonb)
          || pg_catalog.jsonb_build_object(
            'status_text', 'Processed one bounded ' || v_phase || ' Draft page.',
            'draft_v8_bounded_advance', true,
            'draft_v8_last_owner', v_owner,
            'draft_v8_last_owner_receipt_sha256', v_receipt,
            'draft_v8_last_scope_count', pg_catalog.cardinality(v_scope_ordinals)
          ),
        updated_at_utc = pg_catalog.clock_timestamp()
    WHERE operation_row.id = p_operation_id;

    RETURN pg_catalog.jsonb_build_object(
      'ok', true, 'handled', true, 'terminal', false,
      'phase', v_phase, 'work_kind', 'BUSINESS_OWNER_PAGE',
      'owner', v_owner, 'scope_count', pg_catalog.cardinality(v_scope_ordinals),
      'owner_has_more', v_has_more, 'receipt_sha256', v_receipt,
      'immediate_continue', true, 'business_owner_call_count', 1,
      'elapsed_ms', pg_catalog.floor(EXTRACT(EPOCH FROM
        (pg_catalog.clock_timestamp() - v_started_at)) * 1000)::integer
    );
  END LOOP;
END;
$function$;

ALTER FUNCTION public.banking_pay_draft_advance_bounded_v8(uuid,text,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.banking_pay_draft_advance_bounded_v8(uuid,text,text)
  FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.banking_pay_draft_advance_bounded_v8(uuid,text,text)
  TO service_role;
