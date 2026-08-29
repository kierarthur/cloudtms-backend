-- Canonical later authority for Draft completion's frozen post-Draft live fence.
-- The historical omnibus remains complete; this repeatable is replayed after it.

\ir 13082026_0122_pay_workbench_execution_unsent_overlay_chain_seal_v2.sql

CREATE OR REPLACE FUNCTION public.banking_pay_operation_finish(
    p_operation_id uuid,
    p_status text,
    p_result_json jsonb DEFAULT NULL::jsonb,
    p_error_json jsonb DEFAULT NULL::jsonb
)
RETURNS TABLE (
    finished boolean,
    not_finished_reason text,
    operation_id uuid,
    operation_type text,
    status text,
    phase text,
    actor_user_id uuid,
    workbench_session_id uuid,
    pay_batch_id uuid,
    root_operation_id uuid,
    idempotency_key text,
    input_json jsonb,
    config_json jsonb,
    progress_json jsonb,
    result_json jsonb,
    error_json jsonb,
    total_units integer,
    completed_units integer,
    failed_units integer,
    current_chunk_index integer,
    chunk_count integer,
    locked_by text,
    lock_expires_at_utc timestamptz,
    created_at_utc timestamptz,
    started_at_utc timestamptz,
    updated_at_utc timestamptz,
    completed_at_utc timestamptz,
    failed_at_utc timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
VOLATILE
SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
    v_now timestamptz := now();
    v_operation public.banking_pay_operations%ROWTYPE;
    v_status text := upper(NULLIF(BTRIM(COALESCE(p_status, '')), ''));
    v_result_json jsonb := p_result_json;
    v_error_json jsonb := p_error_json;
    v_runner_state text := NULL::text;
    v_requires_user_action boolean := false;
    v_resume_reason text := NULL::text;
    v_finish_scope_generation bigint := 0;
    v_finish_relevant_generation bigint := NULL::bigint;
    v_finish_unresolved_root_count integer := 0;
    v_finish_failed_root_count integer := 0;
    v_finish_scope_count integer := 0;
    v_finish_selected_count integer := 0;
    v_finish_scope_invalid_count integer := 0;
    v_finish_chunk_count integer := 0;
    v_finish_chunk_invalid_count integer := 0;
    v_finish_scope_hash text := NULL::text;
    v_finish_blocker jsonb := '{}'::jsonb;
    v_finish_scope_status text := 'NONE';
    v_finish_freshness_status text := 'VALID_AT_SCOPE_FREEZE';
    v_post_draft_authority_count integer := 0;
    v_source_publication_identity_enforce_enabled boolean := false;
    v_execution_overlay_chain_v2 jsonb := NULL::jsonb;
BEGIN
    PERFORM set_config('lock_timeout', '3s', true);

    SELECT COALESCE(setting.banking_pay_source_publication_identity_enforce_v1_enabled,false)
    INTO v_source_publication_identity_enforce_enabled
    FROM public.settings_defaults AS setting
    WHERE setting.id=1;

    IF p_operation_id IS NULL THEN
        RAISE EXCEPTION 'BANKING_PAY_OPERATION_FINISH_OPERATION_ID_REQUIRED'
          USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_OPERATION_FINISH_OPERATION_ID_REQUIRED')::text;
    END IF;

    IF v_status IS NULL OR v_status NOT IN ('COMPLETE', 'FAILED', 'CANCELLED', 'CANCELED', 'REVIEW_REQUIRED', 'WAITING_AUTHORISATION', 'WAITING_PROVIDER') THEN
        RAISE EXCEPTION 'BANKING_PAY_OPERATION_FINISH_STATUS_INVALID'
          USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_OPERATION_FINISH_STATUS_INVALID', 'operation_id', p_operation_id::text, 'status', p_status)::text;
    END IF;

    IF v_result_json IS NOT NULL AND jsonb_typeof(v_result_json) <> 'object' THEN
        RAISE EXCEPTION 'BANKING_PAY_OPERATION_FINISH_RESULT_MUST_BE_OBJECT'
          USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_OPERATION_FINISH_RESULT_MUST_BE_OBJECT', 'operation_id', p_operation_id::text)::text;
    END IF;

    IF v_error_json IS NOT NULL AND jsonb_typeof(v_error_json) <> 'object' THEN
        RAISE EXCEPTION 'BANKING_PAY_OPERATION_FINISH_ERROR_MUST_BE_OBJECT'
          USING ERRCODE = 'P0001', DETAIL = jsonb_build_object('code', 'BANKING_PAY_OPERATION_FINISH_ERROR_MUST_BE_OBJECT', 'operation_id', p_operation_id::text)::text;
    END IF;

    SELECT operation_row.*
    INTO v_operation
    FROM public.banking_pay_operations AS operation_row
    WHERE operation_row.id = p_operation_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RETURN QUERY SELECT false, 'NOT_FOUND'::text, p_operation_id, NULL::text, NULL::text, NULL::text, NULL::uuid, NULL::uuid, NULL::uuid, NULL::uuid, NULL::text, NULL::jsonb, NULL::jsonb, NULL::jsonb, NULL::jsonb, NULL::jsonb, NULL::integer, NULL::integer, NULL::integer, NULL::integer, NULL::integer, NULL::text, NULL::timestamptz, NULL::timestamptz, NULL::timestamptz, NULL::timestamptz, NULL::timestamptz, NULL::timestamptz;
        RETURN;
    END IF;

    IF upper(BTRIM(COALESCE(v_operation.status, ''))) IN ('COMPLETE', 'FAILED', 'CANCELLED', 'CANCELED') THEN
        RETURN QUERY SELECT false, 'ALREADY_TERMINAL'::text, v_operation.id, v_operation.operation_type, v_operation.status, v_operation.phase, v_operation.actor_user_id, v_operation.workbench_session_id, v_operation.pay_batch_id, v_operation.root_operation_id, v_operation.idempotency_key, v_operation.input_json, v_operation.config_json, v_operation.progress_json, v_operation.result_json, v_operation.error_json, v_operation.total_units, v_operation.completed_units, v_operation.failed_units, v_operation.current_chunk_index, v_operation.chunk_count, COALESCE(v_operation.lease_owner, v_operation.locked_by), COALESCE(v_operation.lease_expires_at_utc, v_operation.lock_expires_at_utc), v_operation.created_at_utc, v_operation.started_at_utc, v_operation.updated_at_utc, v_operation.completed_at_utc, v_operation.failed_at_utc;
        RETURN;
    END IF;

    IF UPPER(BTRIM(COALESCE(v_operation.operation_type, ''))) = 'DRAFT_CREATE'
       AND v_status = 'COMPLETE' THEN
      IF UPPER(BTRIM(COALESCE(v_operation.scope_freeze_status, ''))) <> 'FROZEN'
         OR NOT COALESCE(v_operation.source_scope_seed_complete, false)
         OR v_operation.frozen_scope_change_generation IS NULL
         OR v_operation.scope_frozen_at_utc IS NULL
         OR COALESCE(v_operation.frozen_candidate_scope_count, 0) <= 0
         OR COALESCE(v_operation.frozen_selected_row_count, 0) <= 0
         OR NULLIF(BTRIM(COALESCE(v_operation.frozen_operation_scope_hash, '')), '') IS NULL
         OR v_operation.frozen_source_session_version IS NULL
         OR v_operation.frozen_source_snapshot_run_id IS NULL THEN
        RAISE EXCEPTION 'DRAFT_CREATE_OPERATION_SCOPE_NOT_FROZEN'
          USING ERRCODE = 'P0001';
      END IF;

      SELECT
        (SELECT COUNT(*)::integer
         FROM public.banking_pay_operation_candidate_scope AS scope_count
         WHERE scope_count.operation_id = v_operation.id),
        (SELECT COUNT(DISTINCT selected_id.value)::integer
         FROM public.banking_pay_operation_candidate_scope AS selected_scope
         CROSS JOIN LATERAL jsonb_array_elements_text(
           CASE WHEN jsonb_typeof(selected_scope.selected_preview_row_ids_json) = 'array'
             THEN selected_scope.selected_preview_row_ids_json ELSE '[]'::jsonb END
         ) AS selected_id(value)
         WHERE selected_scope.operation_id = v_operation.id),
        (SELECT COUNT(*)::integer
         FROM public.banking_pay_operation_candidate_scope AS invalid_scope
         WHERE invalid_scope.operation_id = v_operation.id
           AND (
             invalid_scope.pay_batch_id IS NULL
             OR invalid_scope.workbench_session_id IS DISTINCT FROM v_operation.workbench_session_id
             OR invalid_scope.source_session_version IS DISTINCT FROM v_operation.frozen_source_session_version
             OR invalid_scope.source_snapshot_run_id IS DISTINCT FROM v_operation.frozen_source_snapshot_run_id
             OR UPPER(BTRIM(COALESCE(invalid_scope.status, ''))) NOT IN ('ALLOCATED', 'DRAFTED')
             OR NOT EXISTS (
               SELECT 1
               FROM public.pay_batches AS provenance_batch
               WHERE provenance_batch.id = invalid_scope.pay_batch_id
                 AND provenance_batch.source_scope_change_generation IS NOT DISTINCT FROM v_operation.frozen_scope_change_generation
                 AND provenance_batch.source_workbench_session_id IS NOT DISTINCT FROM v_operation.workbench_session_id
                 AND provenance_batch.source_session_version IS NOT DISTINCT FROM v_operation.frozen_source_session_version
                 AND provenance_batch.source_snapshot_run_id IS NOT DISTINCT FROM v_operation.frozen_source_snapshot_run_id
             )
           )),
        (SELECT md5(COALESCE(string_agg(
           hash_scope.candidate_id::text || ':' || hash_scope.pay_channel || ':' || hash_scope.scope_hash,
           '|' ORDER BY hash_scope.pay_channel, hash_scope.candidate_id
         ), ''))
         FROM public.banking_pay_operation_candidate_scope AS hash_scope
         WHERE hash_scope.operation_id = v_operation.id)
      INTO v_finish_scope_count, v_finish_selected_count, v_finish_scope_invalid_count, v_finish_scope_hash;

      IF v_finish_scope_count <> v_operation.frozen_candidate_scope_count
         OR v_finish_selected_count <> v_operation.frozen_selected_row_count
         OR v_finish_scope_invalid_count > 0
         OR v_finish_scope_hash IS DISTINCT FROM v_operation.frozen_operation_scope_hash THEN
        RAISE EXCEPTION 'DRAFT_CREATE_OPERATION_BATCH_PROVENANCE_MISMATCH'
          USING ERRCODE = 'P0001';
      END IF;

      SELECT COUNT(*)::integer,
             COUNT(*) FILTER (
               WHERE UPPER(BTRIM(COALESCE(operation_chunk.status, ''))) <> 'COMPLETE'
                  OR COALESCE(operation_chunk.completed_count, 0) <> COALESCE(operation_chunk.unit_count, 0)
                  OR COALESCE(operation_chunk.failed_count, 0) <> 0
             )::integer
      INTO v_finish_chunk_count, v_finish_chunk_invalid_count
      FROM public.banking_pay_operation_chunks AS operation_chunk
      WHERE operation_chunk.operation_id = v_operation.id
        AND UPPER(BTRIM(COALESCE(operation_chunk.chunk_type, ''))) = 'CANDIDATE_SCOPE';

      IF v_finish_chunk_count <= 0 OR v_finish_chunk_invalid_count > 0 THEN
        RAISE EXCEPTION 'DRAFT_CREATE_OPERATION_CHUNKS_INCOMPLETE'
          USING ERRCODE = 'P0001';
      END IF;

      -- Freeze the live candidate authority accepted immediately after the
      -- Draft has completed.  This is deliberately distinct from the
      -- pre-Draft source publication stored in allocation_basis_json: an
      -- untouched-Draft cancellation may restore that immutable V3 source
      -- only while this post-Draft sequence/generation pair is still live.
      -- Legacy or non-V3 scopes simply remain ineligible for the fast route.
      WITH authority_rows AS (
        SELECT
          draft_scope.id AS scope_id,
          draft_scope.candidate_id,
          draft_scope.pay_batch_id,
          draft_scope.workbench_session_id,
          draft_scope.source_session_version,
          draft_scope.source_snapshot_run_id,
          COALESCE(candidate_counter.seq, 0) AS source_change_seq,
          COALESCE(candidate_counter.scope_change_generation, 0) AS dirty_generation,
          draft_scope.allocation_basis_json->>'source_build_run_id' AS original_source_build_run_id,
          draft_scope.allocation_basis_json->>'source_publication_id' AS original_source_publication_id,
          draft_scope.allocation_basis_json->>'source_identity_digest' AS original_source_identity_digest,
          draft_scope.allocation_basis_json->>'semantic_proof_digest' AS original_semantic_proof_digest,
          draft_scope.allocation_basis_json->'source_publication_attestation' AS source_attestation
        FROM public.banking_pay_operation_candidate_scope AS draft_scope
        LEFT JOIN public.app_change_counters AS candidate_counter
          ON candidate_counter.entity_key = 'pay_candidate:' || draft_scope.candidate_id::text
        WHERE draft_scope.operation_id = v_operation.id
      ), eligible_authority AS (
        SELECT authority_rows.*,
          COALESCE(authority_rows.original_source_publication_id,'')
            ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            AS fast_reversion_eligible,
          md5(
            v_operation.id::text || '|' ||
            authority_rows.pay_batch_id::text || '|' ||
            authority_rows.workbench_session_id::text || '|' ||
            authority_rows.candidate_id::text || '|' ||
            authority_rows.source_change_seq::text || '|' ||
            authority_rows.dirty_generation::text || '|' ||
            authority_rows.source_session_version::text || '|' ||
            authority_rows.source_snapshot_run_id::text || '|' ||
            authority_rows.original_source_build_run_id || '|' ||
            COALESCE(authority_rows.original_source_publication_id,'') || '|' ||
            authority_rows.original_source_identity_digest || '|' ||
            authority_rows.original_semantic_proof_digest || '|' ||
            (COALESCE(authority_rows.original_source_publication_id,'')
              ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')::text ||
            '|POST_DRAFT_LIVE_AUTHORITY_V2'
          ) AS authority_digest
        FROM authority_rows
        WHERE COALESCE(authority_rows.source_attestation->>'attestation_version', '')
                = 'CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3'
          AND COALESCE(authority_rows.source_attestation->>'semantic_contract_version', '')
                = 'READY_TO_PAY_SEMANTIC_V2'
          AND COALESCE((authority_rows.source_attestation->>'semantic_ready')::boolean, false)
          AND COALESCE((authority_rows.source_attestation->>'parity_complete')::boolean, false)
          AND COALESCE(authority_rows.original_source_build_run_id, '')
                ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          AND NULLIF(BTRIM(COALESCE(authority_rows.original_source_identity_digest, '')), '') IS NOT NULL
          AND NULLIF(BTRIM(COALESCE(authority_rows.original_semantic_proof_digest, '')), '') IS NOT NULL
      ), frozen_authority AS (
        UPDATE public.banking_pay_operation_candidate_scope AS draft_scope
        SET allocation_basis_json = COALESCE(draft_scope.allocation_basis_json, '{}'::jsonb)
              || jsonb_build_object(
                'post_draft_authority',
                jsonb_build_object(
                  'contract_version', 'POST_DRAFT_LIVE_AUTHORITY_V2',
                  'draft_operation_id', v_operation.id,
                  'pay_batch_id', eligible_authority.pay_batch_id,
                  'workbench_session_id', eligible_authority.workbench_session_id,
                  'candidate_id', eligible_authority.candidate_id,
                  'source_change_seq', eligible_authority.source_change_seq,
                  'dirty_generation', eligible_authority.dirty_generation,
                  'source_session_version', eligible_authority.source_session_version,
                  'source_snapshot_run_id', eligible_authority.source_snapshot_run_id,
                  'original_source_build_run_id', eligible_authority.original_source_build_run_id,
                  'original_source_publication_id', eligible_authority.original_source_publication_id,
                  'fast_reversion_eligible', eligible_authority.fast_reversion_eligible,
                  'fast_reversion_ineligible_reason', CASE
                    WHEN eligible_authority.fast_reversion_eligible THEN NULL
                    ELSE 'LEGACY_PHYSICAL_PUBLICATION_MISSING'
                  END,
                  'original_source_identity_digest', eligible_authority.original_source_identity_digest,
                  'original_semantic_proof_digest', eligible_authority.original_semantic_proof_digest,
                  'authority_digest', eligible_authority.authority_digest,
                  'captured_at_utc', v_now,
                  'policy_x_authority', 'FROZEN_PRE_DRAFT_SOURCE_PLUS_POST_DRAFT_LIVE_FENCE'
                )
              ),
            updated_at_utc = v_now
        FROM eligible_authority
        WHERE draft_scope.id = eligible_authority.scope_id
        RETURNING draft_scope.id
      )
      SELECT COUNT(*)::integer
      INTO v_post_draft_authority_count
      FROM frozen_authority;

      v_result_json := COALESCE(v_result_json, '{}'::jsonb)
        || jsonb_build_object(
          'post_draft_authority_contract_version', 'POST_DRAFT_LIVE_AUTHORITY_V2',
          'post_draft_authority_count', v_post_draft_authority_count,
          'post_draft_authority_candidate_count', v_finish_scope_count,
          'post_draft_fast_reversion_eligible_count', (
            SELECT COUNT(*)::integer FROM public.banking_pay_operation_candidate_scope AS scope_row
            WHERE scope_row.operation_id=v_operation.id
              AND COALESCE((scope_row.allocation_basis_json->'post_draft_authority'->>'fast_reversion_eligible')::boolean,false)
          )
        );
    END IF;

    v_runner_state := CASE
      WHEN v_status = 'COMPLETE' THEN 'COMPLETE'
      WHEN v_status IN ('CANCELLED', 'CANCELED') THEN 'CANCELLED'
      WHEN v_status = 'FAILED' THEN 'FAILED'
      WHEN v_status = 'REVIEW_REQUIRED' THEN 'WAITING_USER_REVIEW'
      WHEN v_status = 'WAITING_AUTHORISATION' THEN 'WAITING_USER'
      WHEN v_status = 'WAITING_PROVIDER' THEN 'WAITING_PROVIDER'
      ELSE v_operation.runner_state
    END;

    v_requires_user_action := v_status IN ('FAILED', 'REVIEW_REQUIRED', 'WAITING_AUTHORISATION');
    v_resume_reason := CASE
      WHEN v_status = 'COMPLETE' THEN 'OPERATION_COMPLETE'
      WHEN v_status = 'REVIEW_REQUIRED' THEN 'REVIEW_REQUIRED'
      WHEN v_status = 'WAITING_AUTHORISATION' THEN 'AWAITING_PAYMENT_AUTHORISATION'
      WHEN v_status = 'WAITING_PROVIDER' THEN 'AWAITING_PROVIDER_OUTCOME'
      WHEN v_status = 'FAILED' THEN 'OPERATION_FAILED'
      ELSE v_operation.resume_reason
    END;

    -- DRAFT_CREATE completion records post-freeze evidence without aborting the
    -- frozen operation. Existing central freshness gates remain authoritative
    -- for every consequential post-draft action.
    IF UPPER(BTRIM(COALESCE(v_operation.operation_type, ''))) = 'DRAFT_CREATE'
       AND v_status = 'COMPLETE'
       AND v_operation.scope_freeze_status = 'FROZEN'
       AND v_operation.frozen_scope_change_generation IS NOT NULL THEN
      SELECT COALESCE(change_counter.seq, 0)
      INTO v_finish_scope_generation
      FROM public.app_change_counters AS change_counter
      WHERE change_counter.entity_key = 'pay_candidate_scope_generation';

      SELECT MAX(candidate_counter.scope_change_generation)
      INTO v_finish_relevant_generation
      FROM public.banking_pay_operation_candidate_scope AS frozen_scope
      JOIN public.app_change_counters AS candidate_counter
        ON candidate_counter.entity_key = 'pay_candidate:' || frozen_scope.candidate_id::text
      WHERE frozen_scope.operation_id = v_operation.id
        AND candidate_counter.scope_change_generation > v_operation.frozen_scope_change_generation
        AND candidate_counter.scope_change_generation <= v_finish_scope_generation;

      v_finish_blocker := public.pay_workbench_scope_blocker_state_v1(
        v_operation.workbench_session_id,
        v_finish_scope_generation,
        v_operation.id
      );
      v_finish_unresolved_root_count := COALESCE((v_finish_blocker->>'upstream_active_count')::integer, 0);
      v_finish_failed_root_count := COALESCE((v_finish_blocker->>'upstream_unresolved_failure_count')::integer, 0);

      IF v_finish_scope_generation = v_operation.frozen_scope_change_generation THEN
        v_finish_scope_status := 'NONE';
        v_finish_freshness_status := 'VALID_AT_SCOPE_FREEZE';
      ELSIF v_finish_relevant_generation IS NOT NULL THEN
        v_finish_scope_status := 'RELEVANT';
        v_finish_freshness_status := 'STALE_POST_SCOPE_FREEZE';
      ELSIF v_finish_failed_root_count > 0 THEN
        v_finish_scope_status := 'PENDING_RELEVANCE';
        v_finish_freshness_status := 'PENDING_SCOPE_CHANGE_RELEVANCE_FAILED';
      ELSIF v_finish_unresolved_root_count > 0 THEN
        v_finish_scope_status := 'PENDING_RELEVANCE';
        v_finish_freshness_status := 'PENDING_SCOPE_CHANGE_RELEVANCE';
      ELSE
        v_finish_scope_status := 'IRRELEVANT';
        v_finish_freshness_status := 'VALID_AT_SCOPE_FREEZE';
      END IF;

      UPDATE public.pay_batches AS operation_batch
      SET freshness_validation_status = v_finish_freshness_status,
          freshness_checked_at_utc = v_now,
          scope_generation_observed_at_shell = GREATEST(
            COALESCE(operation_batch.scope_generation_observed_at_shell, 0),
            v_finish_scope_generation
          ),
          freshness_result_json = COALESCE(operation_batch.freshness_result_json, '{}'::jsonb)
            || jsonb_strip_nulls(jsonb_build_object(
              'post_freeze_scope_status', v_finish_scope_status,
              'scope_generation_observed_at_operation_finish', v_finish_scope_generation,
              'post_freeze_relevant_generation', v_finish_relevant_generation,
              'unresolved_broad_root_count', v_finish_unresolved_root_count,
              'failed_broad_root_count', v_finish_failed_root_count,
              'scope_blocker_failure_sample', COALESCE(v_finish_blocker->'failure_sample', '[]'::jsonb),
              'checked_at_utc', v_now::text,
              'policy_x_authority', 'FROZEN_OPERATION_SCOPE'
            ))
      WHERE operation_batch.id IN (
        SELECT DISTINCT candidate_scope.pay_batch_id
        FROM public.banking_pay_operation_candidate_scope AS candidate_scope
        WHERE candidate_scope.operation_id = v_operation.id
          AND candidate_scope.pay_batch_id IS NOT NULL
      );
    END IF;

    -- PAYMENT_EXECUTE may legitimately create more than one finalized dirty
    -- generation while preparing a local unsent transfer and committing its
    -- frozen reservation/audit overlay.  Seal that exact bounded chain before
    -- exposing the operation as COMPLETE.  An unprovable chain never blocks
    -- execution; it is retained as a typed rejection and cancellation safely
    -- falls back to the ordinary current-authority route.
    IF UPPER(BTRIM(COALESCE(v_operation.operation_type, ''))) = 'PAYMENT_EXECUTE'
       AND v_status = 'COMPLETE'
       AND v_operation.pay_batch_id IS NOT NULL THEN
      v_execution_overlay_chain_v2 :=
        private.pay_workbench_execution_unsent_overlay_chain_seal_v2(
          v_operation.id,v_operation.pay_batch_id,'{}'::jsonb);
      v_result_json := COALESCE(v_result_json,'{}'::jsonb)
        || jsonb_build_object(
          'execution_unsent_overlay_chain_v2',v_execution_overlay_chain_v2
        );
    END IF;

    UPDATE public.banking_pay_operations AS operation_update
    SET status = v_status,
        runner_state = v_runner_state,
        requires_user_action = v_requires_user_action,
        resume_reason = v_resume_reason,
        result_json = v_result_json,
        error_json = v_error_json,
        lease_owner = NULL::text,
        lease_expires_at_utc = NULL::timestamptz,
        locked_by = NULL::text,
        lock_expires_at_utc = NULL::timestamptz,
        run_after_utc = CASE WHEN v_status IN ('WAITING_PROVIDER') THEN operation_update.run_after_utc ELSE NULL::timestamptz END,
        heartbeat_at_utc = v_now,
        last_advanced_at_utc = v_now,
        completed_at_utc = CASE WHEN v_status IN ('COMPLETE', 'CANCELLED', 'CANCELED') THEN COALESCE(operation_update.completed_at_utc, v_now) ELSE operation_update.completed_at_utc END,
        failed_at_utc = CASE WHEN v_status = 'FAILED' THEN COALESCE(operation_update.failed_at_utc, v_now) ELSE operation_update.failed_at_utc END,
        progress_json = jsonb_strip_nulls(COALESCE(operation_update.progress_json, '{}'::jsonb) || jsonb_build_object(
          'finished_at_utc', v_now::text,
          'finish_status', v_status,
          'runner_state', v_runner_state,
          'requires_user_action', v_requires_user_action,
          'resume_reason', v_resume_reason,
          'execution_unsent_overlay_chain_v2',v_execution_overlay_chain_v2
        )),
        post_freeze_scope_status = CASE
          WHEN UPPER(BTRIM(COALESCE(operation_update.operation_type, ''))) = 'DRAFT_CREATE'
           AND v_status = 'COMPLETE'
           AND operation_update.scope_freeze_status = 'FROZEN'
            THEN v_finish_scope_status
          ELSE operation_update.post_freeze_scope_status
        END,
        post_freeze_observed_generation = CASE
          WHEN UPPER(BTRIM(COALESCE(operation_update.operation_type, ''))) = 'DRAFT_CREATE'
           AND v_status = 'COMPLETE'
           AND operation_update.scope_freeze_status = 'FROZEN'
            THEN v_finish_scope_generation
          ELSE operation_update.post_freeze_observed_generation
        END,
        post_freeze_relevant_generation = CASE
          WHEN UPPER(BTRIM(COALESCE(operation_update.operation_type, ''))) = 'DRAFT_CREATE'
           AND v_status = 'COMPLETE'
           AND operation_update.scope_freeze_status = 'FROZEN'
            THEN v_finish_relevant_generation
          ELSE operation_update.post_freeze_relevant_generation
        END,
        post_freeze_scope_checked_at_utc = CASE
          WHEN UPPER(BTRIM(COALESCE(operation_update.operation_type, ''))) = 'DRAFT_CREATE'
           AND v_status = 'COMPLETE'
           AND operation_update.scope_freeze_status = 'FROZEN'
            THEN v_now
          ELSE operation_update.post_freeze_scope_checked_at_utc
        END,
        updated_at_utc = v_now
    WHERE operation_update.id = v_operation.id
    RETURNING operation_update.* INTO v_operation;

    RETURN QUERY SELECT true, NULL::text, v_operation.id, v_operation.operation_type, v_operation.status, v_operation.phase, v_operation.actor_user_id, v_operation.workbench_session_id, v_operation.pay_batch_id, v_operation.root_operation_id, v_operation.idempotency_key, v_operation.input_json, v_operation.config_json, v_operation.progress_json, v_operation.result_json, v_operation.error_json, v_operation.total_units, v_operation.completed_units, v_operation.failed_units, v_operation.current_chunk_index, v_operation.chunk_count, COALESCE(v_operation.lease_owner, v_operation.locked_by), COALESCE(v_operation.lease_expires_at_utc, v_operation.lock_expires_at_utc), v_operation.created_at_utc, v_operation.started_at_utc, v_operation.updated_at_utc, v_operation.completed_at_utc, v_operation.failed_at_utc;
END;
$function$;



