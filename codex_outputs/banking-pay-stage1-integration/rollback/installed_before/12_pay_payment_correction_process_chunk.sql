-- Exact installed TEST rollback definition captured before Banking Pay Stage 1 integration on 2026-08-04.
-- Installed definition MD5: 45927f8bdd588b9707ed3c5a611f1ab5

CREATE OR REPLACE FUNCTION public.pay_payment_correction_process_chunk(p_correction_request_id uuid DEFAULT NULL::uuid, p_limit integer DEFAULT 50, p_worker_id text DEFAULT NULL::text, p_actor_user_id uuid DEFAULT NULL::uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_limit integer := LEAST(GREATEST(COALESCE(p_limit, 50), 1), 100);
  v_worker_id text := COALESCE(NULLIF(btrim(COALESCE(p_worker_id, '')), ''), 'payment-correction-worker');
  v_now timestamptz := now();
  v_claimed_count integer := 0;
  v_processed_count integer := 0;
  v_applied_count integer := 0;
  v_blocked_count integer := 0;
  v_failed_retryable_count integer := 0;
  v_failed_final_count integer := 0;
  v_skipped_count integer := 0;
  v_parent_status text := NULL;
  v_parent_request_ids uuid[] := ARRAY[]::uuid[];
  v_totals jsonb := '{}'::jsonb;
  v_result jsonb := '{}'::jsonb;
  v_result_ok boolean := false;
  v_result_status text := NULL;
  v_failure_status text := NULL;
  v_work_row record;
  v_request_status public.pay_payment_correction_requests.status%TYPE;
  v_work_selection_json jsonb := '{}'::jsonb;
  v_expected_item_count integer := NULL;
  v_expected_item_ids jsonb := '[]'::jsonb;
  v_resolved_item_count integer := 0;
  v_resolved_item_ids jsonb := '[]'::jsonb;
  v_selection_drift boolean := false;
  v_selection_drift_result jsonb := '{}'::jsonb;
  v_effective_actor_user_id uuid := NULL::uuid;
  v_processing_actor_kind text := 'SYSTEM';
  v_source_bank_event_id uuid := NULL::uuid;
  v_source_event_disposition text := NULL::text;
  v_source_event_update jsonb := NULL::jsonb;
  v_source_event_updates jsonb := '[]'::jsonb;
  v_requires_user_action boolean := false;
  v_processing_continues boolean := false;
  v_chunk_carry_forward_created_count integer := 0;
  v_chunk_carry_forward_existing_count integer := 0;
  v_chunk_carry_forward_released_count integer := 0;
  v_chunk_released_reservation_count integer := 0;
  v_chunk_restored_component_count integer := 0;
  v_chunk_workbench_refresh_results jsonb := '[]'::jsonb;
  v_chunk_workbench_refresh_job_ids jsonb := '[]'::jsonb;
  v_chunk_workbench_refresh_queued_count integer := 0;
  v_chunk_workbench_refresh_deferred_count integer := 0;
  v_chunk_workbench_refresh_failed_count integer := 0;
  v_chunk_workbench_refresh_status text := 'NOT_REQUIRED';
  v_chunk_requires_workbench_session boolean := false;
  v_result_workbench_refresh jsonb := NULL::jsonb;
  v_chunk_ambiguous_manual_blocker_count integer := 0;
  v_chunk_grouped_alert_updates jsonb := '[]'::jsonb;
  v_chunk_changed_scope_items jsonb := '[]'::jsonb;
  v_chunk_changed_scope_json jsonb := '{}'::jsonb;
  v_live_signal_updates jsonb := '[]'::jsonb;
  v_signal_request record;
  v_signal_changed_scope_json jsonb := '{}'::jsonb;
  v_signal_result jsonb := '{}'::jsonb;
BEGIN
  PERFORM public._imp_debug_audit(
    p_actor_user_id,
    'PAYMENT_CORRECTION_PROCESS_CHUNK_START',
    jsonb_build_object(
      'correction_request_id', p_correction_request_id,
      'requested_limit', p_limit,
      'effective_limit', v_limit,
      'worker_id', v_worker_id,
      'actor_user_id', p_actor_user_id,
      'actor_supplied', p_actor_user_id IS NOT NULL
    ),
    'pay_payment_correction',
    COALESCE(p_correction_request_id::text, 'ALL_REQUESTS'),
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  IF p_limit IS NOT NULL AND p_limit < 1 THEN
    v_limit := 1;
  END IF;

  IF p_limit IS NOT NULL AND p_limit > 100 THEN
    v_limit := 100;
  END IF;

  IF p_correction_request_id IS NOT NULL THEN
    SELECT public.pay_payment_correction_requests.status
    INTO v_request_status
    FROM public.pay_payment_correction_requests
    WHERE public.pay_payment_correction_requests.id = p_correction_request_id;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'PAYMENT_CORRECTION_REQUEST_NOT_FOUND'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'PAYMENT_CORRECTION_REQUEST_NOT_FOUND',
                'correction_request_id', p_correction_request_id
              )::text;
    END IF;

    IF v_request_status IN ('APPLIED', 'APPLIED_WITH_BLOCKERS', 'FAILED', 'REJECTED', 'CANCELLED') THEN
      SELECT jsonb_build_object(
        'total', count(*)::integer,
        'pending', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'PENDING')::integer,
        'processing', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'PROCESSING')::integer,
        'applied', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'APPLIED')::integer,
        'skipped', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'SKIPPED')::integer,
        'blocked', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'BLOCKED')::integer,
        'failed_retryable', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'FAILED_RETRYABLE')::integer,
        'failed_final', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'FAILED_FINAL')::integer,
        'cancelled', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'CANCELLED')::integer
      )
      INTO v_totals
      FROM public.pay_payment_correction_work_items
      WHERE public.pay_payment_correction_work_items.correction_request_id = p_correction_request_id;


      DROP TABLE IF EXISTS pg_temp._tmp_payment_correction_source_event_updates;
      CREATE TEMP TABLE _tmp_payment_correction_source_event_updates ON COMMIT DROP AS
      WITH request_totals AS (
        SELECT
          public.pay_payment_correction_requests.id AS correction_request_id,
          public.pay_payment_correction_requests.source_bank_event_id,
          public.pay_payment_correction_requests.status::text AS parent_status,
          COALESCE((v_totals->>'total')::integer, 0) AS total_count,
          COALESCE((v_totals->>'pending')::integer, 0) AS pending_count,
          COALESCE((v_totals->>'processing')::integer, 0) AS processing_count,
          COALESCE((v_totals->>'applied')::integer, 0) AS applied_count,
          COALESCE((v_totals->>'skipped')::integer, 0) AS skipped_count,
          COALESCE((v_totals->>'blocked')::integer, 0) AS blocked_count,
          COALESCE((v_totals->>'failed_retryable')::integer, 0) AS failed_retryable_count,
          COALESCE((v_totals->>'failed_final')::integer, 0) AS failed_final_count,
          COALESCE((v_totals->>'cancelled')::integer, 0) AS cancelled_count
        FROM public.pay_payment_correction_requests
        WHERE public.pay_payment_correction_requests.id = p_correction_request_id
          AND public.pay_payment_correction_requests.source_bank_event_id IS NOT NULL
          AND public.pay_payment_correction_requests.status IN ('APPLIED', 'APPLIED_WITH_BLOCKERS', 'BLOCKED', 'FAILED', 'PROCESSING', 'EXPANDED', 'AUTHORISED')
      ), derived_event_status AS (
        SELECT
          request_totals.correction_request_id,
          request_totals.source_bank_event_id,
          request_totals.parent_status,
          request_totals.total_count,
          request_totals.pending_count,
          request_totals.processing_count,
          request_totals.applied_count,
          request_totals.skipped_count,
          request_totals.blocked_count,
          request_totals.failed_retryable_count,
          request_totals.failed_final_count,
          request_totals.cancelled_count,
          CASE
            WHEN COALESCE(request_totals.total_count, 0) <= 0 THEN 'FAILED'
            WHEN COALESCE(request_totals.failed_retryable_count, 0) > 0 OR COALESCE(request_totals.failed_final_count, 0) > 0 OR request_totals.parent_status = 'FAILED' THEN 'FAILED'
            WHEN COALESCE(request_totals.blocked_count, 0) > 0 OR request_totals.parent_status IN ('APPLIED_WITH_BLOCKERS', 'BLOCKED') THEN 'BLOCKED'
            WHEN request_totals.parent_status = 'APPLIED'
                 AND COALESCE(request_totals.applied_count, 0) + COALESCE(request_totals.skipped_count, 0) = COALESCE(request_totals.total_count, 0) THEN 'AUTO_APPLIED'
            WHEN request_totals.parent_status IN ('PROCESSING', 'EXPANDED', 'AUTHORISED')
                 AND COALESCE(request_totals.pending_count, 0) + COALESCE(request_totals.processing_count, 0) > 0
                 AND COALESCE(request_totals.blocked_count, 0) = 0
                 AND COALESCE(request_totals.failed_retryable_count, 0) = 0
                 AND COALESCE(request_totals.failed_final_count, 0) = 0 THEN 'AUTO_PROCESSING'
            WHEN COALESCE(request_totals.pending_count, 0) + COALESCE(request_totals.processing_count, 0) > 0
                 AND COALESCE(request_totals.blocked_count, 0) = 0
                 AND COALESCE(request_totals.failed_retryable_count, 0) = 0
                 AND COALESCE(request_totals.failed_final_count, 0) = 0 THEN 'AUTO_PROCESSING'
            ELSE 'FAILED'
          END AS correction_disposition,
          jsonb_build_object(
            'total', COALESCE(request_totals.total_count, 0),
            'pending', COALESCE(request_totals.pending_count, 0),
            'processing', COALESCE(request_totals.processing_count, 0),
            'applied', COALESCE(request_totals.applied_count, 0),
            'skipped', COALESCE(request_totals.skipped_count, 0),
            'blocked', COALESCE(request_totals.blocked_count, 0),
            'failed_retryable', COALESCE(request_totals.failed_retryable_count, 0),
            'failed_final', COALESCE(request_totals.failed_final_count, 0),
            'cancelled', COALESCE(request_totals.cancelled_count, 0)
          ) AS totals_json
        FROM request_totals
      ), updated_events AS (
        UPDATE public.pay_bank_transfer_events AS bank_event_to_update
        SET
          correction_disposition = derived_event_status.correction_disposition
        FROM derived_event_status
        WHERE bank_event_to_update.id = derived_event_status.source_bank_event_id
        RETURNING
          derived_event_status.correction_request_id,
          bank_event_to_update.id AS source_bank_event_id,
          derived_event_status.correction_disposition,
          derived_event_status.parent_status,
          derived_event_status.totals_json,
          (derived_event_status.correction_disposition IN ('BLOCKED', 'FAILED')) AS requires_user_action,
          (derived_event_status.correction_disposition = 'AUTO_PROCESSING') AS processing_continues
      )
      SELECT
        updated_events.correction_request_id,
        updated_events.source_bank_event_id,
        updated_events.correction_disposition,
        updated_events.parent_status,
        updated_events.totals_json,
        updated_events.requires_user_action,
        updated_events.processing_continues
      FROM updated_events;

      SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'correction_request_id', source_event_updates.correction_request_id,
        'event_id', source_event_updates.source_bank_event_id,
        'correction_disposition', source_event_updates.correction_disposition,
        'parent_status', source_event_updates.parent_status,
        'totals', source_event_updates.totals_json,
        'requires_user_action', source_event_updates.requires_user_action,
        'processing_continues', source_event_updates.processing_continues
      ) ORDER BY source_event_updates.correction_request_id), '[]'::jsonb)
      INTO v_source_event_updates
      FROM pg_temp._tmp_payment_correction_source_event_updates AS source_event_updates;

      SELECT
        jsonb_build_object(
          'correction_request_id', source_event_updates.correction_request_id,
          'event_id', source_event_updates.source_bank_event_id,
          'correction_disposition', source_event_updates.correction_disposition,
          'parent_status', source_event_updates.parent_status,
          'totals', source_event_updates.totals_json,
          'requires_user_action', source_event_updates.requires_user_action,
          'processing_continues', source_event_updates.processing_continues
        ),
        source_event_updates.requires_user_action,
        source_event_updates.processing_continues
      INTO
        v_source_event_update,
        v_requires_user_action,
        v_processing_continues
      FROM pg_temp._tmp_payment_correction_source_event_updates AS source_event_updates
      ORDER BY source_event_updates.correction_request_id
      LIMIT 1;

      RETURN jsonb_build_object(
        'ok', true,
        'processed', 0,
        'applied', 0,
        'blocked', 0,
        'failed_retryable', 0,
        'failed_final', 0,
        'pending', COALESCE((v_totals->>'pending')::integer, 0),
        'processing', COALESCE((v_totals->>'processing')::integer, 0),
        'progress_completed', COALESCE((v_totals->>'applied')::integer, 0) + COALESCE((v_totals->>'skipped')::integer, 0) + COALESCE((v_totals->>'blocked')::integer, 0) + COALESCE((v_totals->>'failed_final')::integer, 0) + COALESCE((v_totals->>'cancelled')::integer, 0),
        'progress_total', COALESCE((v_totals->>'total')::integer, 0),
        'parent_status', v_request_status,
        'totals', v_totals,
        'carry_forward_created', 0,
        'carry_forward_existing', 0,
        'carry_forward_released', 0,
        'carry_forward_created_count', 0,
        'carry_forward_existing_count', 0,
        'carry_forward_released_count', 0,
        'ambiguous_manual_blockers', 0,
        'released_reservations', 0,
        'restored_finance_components', 0,
        'changed_scope_json', '{}'::jsonb,
        'grouped_alert_updates', '[]'::jsonb,
        'live_signal_updates', '[]'::jsonb,
        'live_signal', '[]'::jsonb,
        'complete', true,
        'requires_user_action', (
          COALESCE(v_requires_user_action, false)
          OR COALESCE((v_totals->>'blocked')::integer, 0) > 0
          OR COALESCE((v_totals->>'failed_retryable')::integer, 0) > 0
          OR COALESCE((v_totals->>'failed_final')::integer, 0) > 0
        ),
        'processing_continues', (
          COALESCE(v_processing_continues, false)
          OR COALESCE((v_totals->>'pending')::integer, 0) > 0
          OR COALESCE((v_totals->>'processing')::integer, 0) > 0
          OR COALESCE((v_totals->>'failed_retryable')::integer, 0) > 0
        ),
        'source_bank_event_update', v_source_event_update,
        'source_bank_event_updates', v_source_event_updates,
        'terminal_request', true,
        'processing_actor_kind', CASE WHEN p_actor_user_id IS NULL THEN 'SYSTEM' ELSE 'USER' END,
        'processing_actor_user_id', CASE WHEN p_actor_user_id IS NULL THEN NULL ELSE p_actor_user_id::text END
      );
    END IF;
  END IF;

  DROP TABLE IF EXISTS pg_temp._tmp_payment_correction_claimed_work;
  CREATE TEMP TABLE _tmp_payment_correction_claimed_work ON COMMIT DROP AS
  WITH claimable_work AS (
    SELECT claimable_items.id
    FROM public.pay_payment_correction_work_items AS claimable_items
    JOIN public.pay_payment_correction_requests AS claimable_requests
      ON claimable_requests.id = claimable_items.correction_request_id
    WHERE claimable_items.status IN ('PENDING', 'FAILED_RETRYABLE')
      AND claimable_requests.status IN ('AUTHORISED', 'EXPANDED', 'PROCESSING')
      AND (p_correction_request_id IS NULL OR claimable_items.correction_request_id = p_correction_request_id)
    ORDER BY
      claimable_items.created_at_utc,
      claimable_items.id
    FOR UPDATE OF claimable_items SKIP LOCKED
    LIMIT v_limit
  ),
  updated_work AS (
    UPDATE public.pay_payment_correction_work_items AS work_to_claim
    SET
      status = 'PROCESSING',
      attempt_count = COALESCE(work_to_claim.attempt_count, 0) + 1,
      locked_at_utc = v_now,
      locked_by = v_worker_id,
      processed_by_user_id = COALESCE(p_actor_user_id, work_to_claim.processed_by_user_id),
      result_json = COALESCE(work_to_claim.result_json, '{}'::jsonb) || jsonb_build_object(
        'claimed_at_utc', v_now,
        'claimed_by', v_worker_id,
        'previous_status', work_to_claim.status,
        'claim_actor_user_id', CASE WHEN p_actor_user_id IS NULL THEN NULL ELSE p_actor_user_id::text END
      )
    FROM claimable_work
    WHERE work_to_claim.id = claimable_work.id
    RETURNING
      work_to_claim.id,
      work_to_claim.correction_request_id,
      work_to_claim.pay_batch_id,
      work_to_claim.work_kind,
      work_to_claim.attempt_count,
      work_to_claim.selection_json
  )
  SELECT
    updated_work.id,
    updated_work.correction_request_id,
    updated_work.pay_batch_id,
    updated_work.work_kind,
    updated_work.attempt_count,
    updated_work.selection_json
  FROM updated_work;

  SELECT count(*)::integer
  INTO v_claimed_count
  FROM pg_temp._tmp_payment_correction_claimed_work AS claimed_work_count;

  SELECT COALESCE(array_agg(DISTINCT claimed_work_request_ids.correction_request_id), ARRAY[]::uuid[])
  INTO v_parent_request_ids
  FROM pg_temp._tmp_payment_correction_claimed_work AS claimed_work_request_ids;

  IF v_claimed_count = 0 THEN
    IF p_correction_request_id IS NOT NULL THEN
      SELECT jsonb_build_object(
        'total', count(*)::integer,
        'pending', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'PENDING')::integer,
        'processing', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'PROCESSING')::integer,
        'applied', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'APPLIED')::integer,
        'skipped', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'SKIPPED')::integer,
        'blocked', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'BLOCKED')::integer,
        'failed_retryable', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'FAILED_RETRYABLE')::integer,
        'failed_final', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'FAILED_FINAL')::integer,
        'cancelled', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'CANCELLED')::integer
      )
      INTO v_totals
      FROM public.pay_payment_correction_work_items
      WHERE public.pay_payment_correction_work_items.correction_request_id = p_correction_request_id;

      SELECT public.pay_payment_correction_requests.status
      INTO v_parent_status
      FROM public.pay_payment_correction_requests
      WHERE public.pay_payment_correction_requests.id = p_correction_request_id;
    ELSE
      v_totals := '{}'::jsonb;
      v_parent_status := NULL;
    END IF;

    IF p_correction_request_id IS NOT NULL THEN

      DROP TABLE IF EXISTS pg_temp._tmp_payment_correction_source_event_updates;
      CREATE TEMP TABLE _tmp_payment_correction_source_event_updates ON COMMIT DROP AS
      WITH request_totals AS (
        SELECT
          public.pay_payment_correction_requests.id AS correction_request_id,
          public.pay_payment_correction_requests.source_bank_event_id,
          public.pay_payment_correction_requests.status::text AS parent_status,
          COALESCE((v_totals->>'total')::integer, 0) AS total_count,
          COALESCE((v_totals->>'pending')::integer, 0) AS pending_count,
          COALESCE((v_totals->>'processing')::integer, 0) AS processing_count,
          COALESCE((v_totals->>'applied')::integer, 0) AS applied_count,
          COALESCE((v_totals->>'skipped')::integer, 0) AS skipped_count,
          COALESCE((v_totals->>'blocked')::integer, 0) AS blocked_count,
          COALESCE((v_totals->>'failed_retryable')::integer, 0) AS failed_retryable_count,
          COALESCE((v_totals->>'failed_final')::integer, 0) AS failed_final_count,
          COALESCE((v_totals->>'cancelled')::integer, 0) AS cancelled_count
        FROM public.pay_payment_correction_requests
        WHERE public.pay_payment_correction_requests.id = p_correction_request_id
          AND public.pay_payment_correction_requests.source_bank_event_id IS NOT NULL
          AND public.pay_payment_correction_requests.status IN ('APPLIED', 'APPLIED_WITH_BLOCKERS', 'BLOCKED', 'FAILED', 'PROCESSING', 'EXPANDED', 'AUTHORISED')
      ), derived_event_status AS (
        SELECT
          request_totals.correction_request_id,
          request_totals.source_bank_event_id,
          request_totals.parent_status,
          request_totals.total_count,
          request_totals.pending_count,
          request_totals.processing_count,
          request_totals.applied_count,
          request_totals.skipped_count,
          request_totals.blocked_count,
          request_totals.failed_retryable_count,
          request_totals.failed_final_count,
          request_totals.cancelled_count,
          CASE
            WHEN COALESCE(request_totals.total_count, 0) <= 0 THEN 'FAILED'
            WHEN COALESCE(request_totals.failed_retryable_count, 0) > 0 OR COALESCE(request_totals.failed_final_count, 0) > 0 OR request_totals.parent_status = 'FAILED' THEN 'FAILED'
            WHEN COALESCE(request_totals.blocked_count, 0) > 0 OR request_totals.parent_status IN ('APPLIED_WITH_BLOCKERS', 'BLOCKED') THEN 'BLOCKED'
            WHEN request_totals.parent_status = 'APPLIED'
                 AND COALESCE(request_totals.applied_count, 0) + COALESCE(request_totals.skipped_count, 0) = COALESCE(request_totals.total_count, 0) THEN 'AUTO_APPLIED'
            WHEN request_totals.parent_status IN ('PROCESSING', 'EXPANDED', 'AUTHORISED')
                 AND COALESCE(request_totals.pending_count, 0) + COALESCE(request_totals.processing_count, 0) > 0
                 AND COALESCE(request_totals.blocked_count, 0) = 0
                 AND COALESCE(request_totals.failed_retryable_count, 0) = 0
                 AND COALESCE(request_totals.failed_final_count, 0) = 0 THEN 'AUTO_PROCESSING'
            WHEN COALESCE(request_totals.pending_count, 0) + COALESCE(request_totals.processing_count, 0) > 0
                 AND COALESCE(request_totals.blocked_count, 0) = 0
                 AND COALESCE(request_totals.failed_retryable_count, 0) = 0
                 AND COALESCE(request_totals.failed_final_count, 0) = 0 THEN 'AUTO_PROCESSING'
            ELSE 'FAILED'
          END AS correction_disposition,
          jsonb_build_object(
            'total', COALESCE(request_totals.total_count, 0),
            'pending', COALESCE(request_totals.pending_count, 0),
            'processing', COALESCE(request_totals.processing_count, 0),
            'applied', COALESCE(request_totals.applied_count, 0),
            'skipped', COALESCE(request_totals.skipped_count, 0),
            'blocked', COALESCE(request_totals.blocked_count, 0),
            'failed_retryable', COALESCE(request_totals.failed_retryable_count, 0),
            'failed_final', COALESCE(request_totals.failed_final_count, 0),
            'cancelled', COALESCE(request_totals.cancelled_count, 0)
          ) AS totals_json
        FROM request_totals
      ), updated_events AS (
        UPDATE public.pay_bank_transfer_events AS bank_event_to_update
        SET
          correction_disposition = derived_event_status.correction_disposition
        FROM derived_event_status
        WHERE bank_event_to_update.id = derived_event_status.source_bank_event_id
        RETURNING
          derived_event_status.correction_request_id,
          bank_event_to_update.id AS source_bank_event_id,
          derived_event_status.correction_disposition,
          derived_event_status.parent_status,
          derived_event_status.totals_json,
          (derived_event_status.correction_disposition IN ('BLOCKED', 'FAILED')) AS requires_user_action,
          (derived_event_status.correction_disposition = 'AUTO_PROCESSING') AS processing_continues
      )
      SELECT
        updated_events.correction_request_id,
        updated_events.source_bank_event_id,
        updated_events.correction_disposition,
        updated_events.parent_status,
        updated_events.totals_json,
        updated_events.requires_user_action,
        updated_events.processing_continues
      FROM updated_events;

      SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'correction_request_id', source_event_updates.correction_request_id,
        'event_id', source_event_updates.source_bank_event_id,
        'correction_disposition', source_event_updates.correction_disposition,
        'parent_status', source_event_updates.parent_status,
        'totals', source_event_updates.totals_json,
        'requires_user_action', source_event_updates.requires_user_action,
        'processing_continues', source_event_updates.processing_continues
      ) ORDER BY source_event_updates.correction_request_id), '[]'::jsonb)
      INTO v_source_event_updates
      FROM pg_temp._tmp_payment_correction_source_event_updates AS source_event_updates;

      SELECT
        jsonb_build_object(
          'correction_request_id', source_event_updates.correction_request_id,
          'event_id', source_event_updates.source_bank_event_id,
          'correction_disposition', source_event_updates.correction_disposition,
          'parent_status', source_event_updates.parent_status,
          'totals', source_event_updates.totals_json,
          'requires_user_action', source_event_updates.requires_user_action,
          'processing_continues', source_event_updates.processing_continues
        ),
        source_event_updates.requires_user_action,
        source_event_updates.processing_continues
      INTO
        v_source_event_update,
        v_requires_user_action,
        v_processing_continues
      FROM pg_temp._tmp_payment_correction_source_event_updates AS source_event_updates
      ORDER BY source_event_updates.correction_request_id
      LIMIT 1;

    END IF;

    PERFORM public._imp_debug_audit(
      p_actor_user_id,
      'PAYMENT_CORRECTION_PROCESS_CHUNK_NO_WORK',
      jsonb_build_object(
        'correction_request_id', p_correction_request_id,
        'effective_limit', v_limit,
        'worker_id', v_worker_id,
        'parent_status', v_parent_status,
        'totals', v_totals
      ),
      'pay_payment_correction',
      COALESCE(p_correction_request_id::text, 'ALL_REQUESTS'),
      NULL::jsonb,
      NULL::text,
      NULL::text,
      NULL::text
    );

    RETURN jsonb_build_object(
      'ok', true,
      'processed', 0,
      'applied', 0,
      'blocked', 0,
      'failed_retryable', 0,
      'failed_final', 0,
      'pending', COALESCE((v_totals->>'pending')::integer, 0),
      'processing', COALESCE((v_totals->>'processing')::integer, 0),
      'progress_completed', COALESCE((v_totals->>'applied')::integer, 0) + COALESCE((v_totals->>'skipped')::integer, 0) + COALESCE((v_totals->>'blocked')::integer, 0) + COALESCE((v_totals->>'failed_final')::integer, 0) + COALESCE((v_totals->>'cancelled')::integer, 0),
      'progress_total', COALESCE((v_totals->>'total')::integer, 0),
      'parent_status', v_parent_status,
      'totals', v_totals,
      'complete', COALESCE((v_totals->>'pending')::integer, 0) = 0 AND COALESCE((v_totals->>'processing')::integer, 0) = 0,
      'carry_forward_created', v_chunk_carry_forward_created_count,
      'carry_forward_existing', v_chunk_carry_forward_existing_count,
      'carry_forward_released', v_chunk_carry_forward_released_count,
      'carry_forward_created_count', v_chunk_carry_forward_created_count,
      'carry_forward_existing_count', v_chunk_carry_forward_existing_count,
      'carry_forward_released_count', v_chunk_carry_forward_released_count,
      'ambiguous_manual_blockers', v_chunk_ambiguous_manual_blocker_count,
      'released_reservations', v_chunk_released_reservation_count,
      'restored_finance_components', v_chunk_restored_component_count,
      'grouped_alert_updates', v_chunk_grouped_alert_updates,
      'requires_user_action', (
        COALESCE(v_requires_user_action, false)
        OR COALESCE((v_totals->>'blocked')::integer, 0) > 0
        OR COALESCE((v_totals->>'failed_retryable')::integer, 0) > 0
        OR COALESCE((v_totals->>'failed_final')::integer, 0) > 0
      ),
      'processing_continues', (
        COALESCE(v_processing_continues, false)
        OR COALESCE((v_totals->>'pending')::integer, 0) > 0
        OR COALESCE((v_totals->>'processing')::integer, 0) > 0
        OR COALESCE((v_totals->>'failed_retryable')::integer, 0) > 0
      ),
      'source_bank_event_update', v_source_event_update,
      'source_bank_event_updates', v_source_event_updates,
      'changed_scope_json', COALESCE(v_chunk_changed_scope_json, '{}'::jsonb),
      'live_signal_updates', COALESCE(v_live_signal_updates, '[]'::jsonb),
      'live_signal', CASE WHEN jsonb_array_length(COALESCE(v_live_signal_updates, '[]'::jsonb)) = 1 THEN v_live_signal_updates->0 ELSE COALESCE(v_live_signal_updates, '[]'::jsonb) END,
      'processing_actor_kind', CASE WHEN p_actor_user_id IS NULL THEN 'SYSTEM' ELSE 'USER' END,
      'processing_actor_user_id', CASE WHEN p_actor_user_id IS NULL THEN NULL ELSE p_actor_user_id::text END
    );
  END IF;

  FOR v_work_row IN
    SELECT
      claimed_work.id,
      claimed_work.correction_request_id,
      claimed_work.pay_batch_id,
      claimed_work.work_kind,
      claimed_work.attempt_count,
      claimed_work.selection_json
    FROM pg_temp._tmp_payment_correction_claimed_work AS claimed_work
    ORDER BY claimed_work.id
  LOOP
    v_processed_count := v_processed_count + 1;
    v_result := '{}'::jsonb;
    v_result_ok := false;
    v_result_status := NULL;
    v_work_selection_json := COALESCE(v_work_row.selection_json, '{}'::jsonb);
    v_expected_item_count := CASE
      WHEN COALESCE(v_work_selection_json->>'expected_item_count', '') ~ '^[0-9]+$'
        THEN (v_work_selection_json->>'expected_item_count')::integer
      ELSE NULL::integer
    END;
    v_expected_item_ids := CASE
      WHEN jsonb_typeof(v_work_selection_json->'expected_pay_batch_item_ids') = 'array'
        THEN COALESCE(v_work_selection_json->'expected_pay_batch_item_ids', '[]'::jsonb)
      WHEN jsonb_typeof(v_work_selection_json->'pay_batch_item_ids') = 'array'
        THEN COALESCE(v_work_selection_json->'pay_batch_item_ids', '[]'::jsonb)
      ELSE '[]'::jsonb
    END;

    SELECT COALESCE(jsonb_agg(DISTINCT expected_item_values.expected_item_id ORDER BY expected_item_values.expected_item_id), '[]'::jsonb)
    INTO v_expected_item_ids
    FROM jsonb_array_elements_text(v_expected_item_ids) AS expected_item_values(expected_item_id);

    SELECT
      COALESCE(p_actor_user_id, latest_authorising_action.actor_user_id, CASE WHEN COALESCE(parent_request.auto_requested, false) THEN NULL::uuid ELSE parent_request.requested_by_user_id END),
      CASE
        WHEN COALESCE(p_actor_user_id, latest_authorising_action.actor_user_id, CASE WHEN COALESCE(parent_request.auto_requested, false) THEN NULL::uuid ELSE parent_request.requested_by_user_id END) IS NULL THEN 'SYSTEM'
        ELSE 'USER'
      END
    INTO
      v_effective_actor_user_id,
      v_processing_actor_kind
    FROM public.pay_payment_correction_requests AS parent_request
    LEFT JOIN LATERAL (
      SELECT authorising_actions.actor_user_id
      FROM public.pay_payment_correction_actions AS authorising_actions
      WHERE authorising_actions.correction_request_id = parent_request.id
        AND authorising_actions.action IN ('AUTHORISE', 'USE_GOLDEN_KEY')
        AND authorising_actions.actor_user_id IS NOT NULL
      ORDER BY authorising_actions.action_at_utc DESC, authorising_actions.id DESC
      LIMIT 1
    ) AS latest_authorising_action ON true
    WHERE parent_request.id = v_work_row.correction_request_id;

    SELECT
      count(*)::integer,
      COALESCE(jsonb_agg(DISTINCT resolved_items.pay_batch_item_id::text ORDER BY resolved_items.pay_batch_item_id::text), '[]'::jsonb)
    INTO
      v_resolved_item_count,
      v_resolved_item_ids
    FROM public._pay_payment_correction_selected_items(
      v_work_row.pay_batch_id,
      v_work_selection_json,
      false
    ) AS resolved_items;

    v_selection_drift := (
      (v_expected_item_count IS NOT NULL AND v_expected_item_count <> v_resolved_item_count)
      OR (jsonb_array_length(v_expected_item_ids) > 0 AND v_expected_item_ids <> v_resolved_item_ids)
    );

    IF v_selection_drift THEN
      v_selection_drift_result := jsonb_build_object(
        'ok', false,
        'status', 'BLOCKED',
        'blocker', jsonb_build_object(
          'code', 'WORK_SELECTION_DRIFT',
          'message', 'The correction work item selection no longer resolves to the expected selected pay_batch_items.',
          'expected_item_count', v_expected_item_count,
          'resolved_item_count', v_resolved_item_count,
          'expected_pay_batch_item_ids', v_expected_item_ids,
          'resolved_pay_batch_item_ids', v_resolved_item_ids
        ),
        'processed_by_chunk', true,
        'processed_worker_id', v_worker_id,
        'processing_actor_kind', v_processing_actor_kind,
        'processing_actor_user_id', CASE WHEN v_effective_actor_user_id IS NULL THEN NULL ELSE v_effective_actor_user_id::text END,
        'processed_at_utc', now()
      );

      UPDATE public.pay_payment_correction_work_items AS selection_drift_work
      SET
        status = 'BLOCKED',
        processed_at_utc = COALESCE(selection_drift_work.processed_at_utc, now()),
        locked_at_utc = NULL,
        locked_by = NULL,
        processed_by_user_id = COALESCE(v_effective_actor_user_id, selection_drift_work.processed_by_user_id),
        last_error = 'WORK_SELECTION_DRIFT',
        result_json = COALESCE(selection_drift_work.result_json, '{}'::jsonb) || v_selection_drift_result
      WHERE selection_drift_work.id = v_work_row.id;

      v_blocked_count := v_blocked_count + 1;
      CONTINUE;
    END IF;

    BEGIN
      IF v_work_row.work_kind = 'PRE_BANK_CANCEL' THEN
        v_result := public.pay_pre_bank_cancel_apply_work_item(v_work_row.id, v_effective_actor_user_id);
      ELSIF v_work_row.work_kind = 'NO_MONEY_UNWIND' THEN
        v_result := public.pay_no_money_unwind_apply_work_item(v_work_row.id, v_effective_actor_user_id);
      ELSIF v_work_row.work_kind = 'SETTLED_REVERSAL' THEN
        v_result := jsonb_build_object(
          'ok', false,
          'status', 'BLOCKED',
          'blocker', jsonb_build_object(
            'code', 'PAID_SETTLED_RECOVERY_REQUIRED',
            'message', 'Paid/settled Banking Pay corrections must be handled through overpayment/recovery, not settled reversal work items.'
          ),
          'blockers', jsonb_build_array(jsonb_build_object(
            'code', 'PAID_SETTLED_RECOVERY_REQUIRED',
            'message', 'Paid/settled Banking Pay corrections must be handled through overpayment/recovery, not settled reversal work items.'
          ))
        );
      ELSE
        v_result := jsonb_build_object(
          'ok', false,
          'status', 'BLOCKED',
          'blocker', jsonb_build_object(
            'code', 'UNKNOWN_WORK_KIND',
            'message', 'Unknown payment correction work kind.',
            'work_kind', v_work_row.work_kind
          )
        );
      END IF;

      v_result_ok := COALESCE((v_result->>'ok')::boolean, false);
      v_result_status := upper(nullif(btrim(COALESCE(v_result->>'status', '')), ''));
      v_chunk_carry_forward_created_count := v_chunk_carry_forward_created_count + COALESCE(NULLIF(v_result->>'carry_forward_created_count', '')::integer, 0);
      v_chunk_carry_forward_existing_count := v_chunk_carry_forward_existing_count + COALESCE(NULLIF(v_result->>'carry_forward_existing_count', '')::integer, 0);
      v_chunk_carry_forward_released_count := v_chunk_carry_forward_released_count + COALESCE(NULLIF(v_result->>'carry_forward_released_count', '')::integer, 0);
      v_chunk_released_reservation_count := v_chunk_released_reservation_count + COALESCE(NULLIF(v_result->>'released_reservation_count', '')::integer, 0);
      v_chunk_restored_component_count := v_chunk_restored_component_count + COALESCE(NULLIF(v_result->>'restored_component_count', '')::integer, 0);


      v_result_workbench_refresh := CASE
        WHEN jsonb_typeof(v_result->'workbench_refresh') = 'object' THEN v_result->'workbench_refresh'
        WHEN NULLIF(BTRIM(COALESCE(v_result->>'workbench_refresh_status', '')), '') IS NOT NULL THEN jsonb_build_object(
          'status', v_result->>'workbench_refresh_status',
          'queued_count', COALESCE(NULLIF(v_result->>'workbench_refresh_queued_count', '')::integer, 0),
          'deferred_count', COALESCE(NULLIF(v_result->>'workbench_refresh_deferred_count', '')::integer, 0),
          'failed_count', COALESCE(NULLIF(v_result->>'workbench_refresh_failed_count', '')::integer, 0),
          'job_ids', COALESCE(v_result->'workbench_refresh_job_ids', '[]'::jsonb),
          'requires_workbench_session', lower(BTRIM(COALESCE(v_result->>'requires_workbench_session', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        )
        ELSE NULL::jsonb
      END;

      IF v_result_workbench_refresh IS NOT NULL THEN
        v_chunk_workbench_refresh_results := COALESCE(v_chunk_workbench_refresh_results, '[]'::jsonb) || jsonb_build_array(v_result_workbench_refresh || jsonb_build_object('work_item_id', v_work_row.id::text));
        v_chunk_workbench_refresh_queued_count := v_chunk_workbench_refresh_queued_count + COALESCE(NULLIF(v_result_workbench_refresh->>'queued_count', '')::integer, 0);
        v_chunk_workbench_refresh_deferred_count := v_chunk_workbench_refresh_deferred_count + COALESCE(NULLIF(v_result_workbench_refresh->>'deferred_count', '')::integer, 0);
        v_chunk_workbench_refresh_failed_count := v_chunk_workbench_refresh_failed_count + COALESCE(NULLIF(v_result_workbench_refresh->>'failed_count', '')::integer, 0);
        IF lower(BTRIM(COALESCE(v_result_workbench_refresh->>'requires_workbench_session', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on') THEN
          v_chunk_requires_workbench_session := true;
        END IF;
        IF jsonb_typeof(v_result_workbench_refresh->'job_ids') = 'array' THEN
          v_chunk_workbench_refresh_job_ids := COALESCE(v_chunk_workbench_refresh_job_ids, '[]'::jsonb) || COALESCE(v_result_workbench_refresh->'job_ids', '[]'::jsonb);
        ELSIF NULLIF(BTRIM(COALESCE(v_result_workbench_refresh->>'job_id', '')), '') IS NOT NULL THEN
          v_chunk_workbench_refresh_job_ids := COALESCE(v_chunk_workbench_refresh_job_ids, '[]'::jsonb) || jsonb_build_array(v_result_workbench_refresh->>'job_id');
        END IF;
      END IF;

      IF EXISTS (
        SELECT 1
        FROM jsonb_array_elements(COALESCE(v_result->'blockers', CASE WHEN v_result ? 'blocker' THEN jsonb_build_array(v_result->'blocker') ELSE '[]'::jsonb END)) AS result_blockers(blocker_value)
        WHERE COALESCE(result_blockers.blocker_value->>'code', '') = 'SOURCE_LESS_MANUAL_ADJUSTMENT_AMBIGUOUS'
      ) THEN
        v_chunk_ambiguous_manual_blocker_count := v_chunk_ambiguous_manual_blocker_count + 1;
      END IF;

      IF v_result ? 'grouped_alert_summary_impact' THEN
        v_chunk_grouped_alert_updates := v_chunk_grouped_alert_updates || jsonb_build_array(v_result->'grouped_alert_summary_impact');
      END IF;

      IF v_result ? 'changed_scope_json'
         AND COALESCE(jsonb_typeof(v_result->'changed_scope_json'), 'null') = 'object' THEN
        v_chunk_changed_scope_items := v_chunk_changed_scope_items || jsonb_build_array(v_result->'changed_scope_json');
      END IF;

      IF v_result_ok THEN
        UPDATE public.pay_payment_correction_work_items AS processed_work_success
        SET
          status = 'APPLIED',
          processed_at_utc = COALESCE(processed_work_success.processed_at_utc, now()),
          locked_at_utc = NULL,
          locked_by = NULL,
          processed_by_user_id = COALESCE(v_effective_actor_user_id, processed_work_success.processed_by_user_id),
          result_json = COALESCE(processed_work_success.result_json, '{}'::jsonb) || v_result || jsonb_build_object(
            'processed_by_chunk', true,
            'processed_worker_id', v_worker_id,
            'processing_actor_kind', v_processing_actor_kind,
            'processing_actor_user_id', CASE WHEN v_effective_actor_user_id IS NULL THEN NULL ELSE v_effective_actor_user_id::text END,
            'processed_at_utc', now()
          )
        WHERE processed_work_success.id = v_work_row.id;

        v_applied_count := v_applied_count + 1;
      ELSIF v_result_status = 'SKIPPED' THEN
        UPDATE public.pay_payment_correction_work_items AS processed_work_skipped
        SET
          status = 'SKIPPED',
          processed_at_utc = COALESCE(processed_work_skipped.processed_at_utc, now()),
          locked_at_utc = NULL,
          locked_by = NULL,
          processed_by_user_id = COALESCE(v_effective_actor_user_id, processed_work_skipped.processed_by_user_id),
          result_json = COALESCE(processed_work_skipped.result_json, '{}'::jsonb) || v_result || jsonb_build_object(
            'processed_by_chunk', true,
            'processed_worker_id', v_worker_id,
            'processing_actor_kind', v_processing_actor_kind,
            'processing_actor_user_id', CASE WHEN v_effective_actor_user_id IS NULL THEN NULL ELSE v_effective_actor_user_id::text END,
            'processed_at_utc', now()
          )
        WHERE processed_work_skipped.id = v_work_row.id;

        v_skipped_count := v_skipped_count + 1;
      ELSE
        UPDATE public.pay_payment_correction_work_items AS processed_work_blocked
        SET
          status = 'BLOCKED',
          processed_at_utc = COALESCE(processed_work_blocked.processed_at_utc, now()),
          locked_at_utc = NULL,
          locked_by = NULL,
          last_error = COALESCE(v_result->>'error_message', v_result->>'message', v_result#>>'{blocker,message}', 'Payment correction work item blocked.'),
          result_json = COALESCE(processed_work_blocked.result_json, '{}'::jsonb) || v_result || jsonb_build_object(
            'processed_by_chunk', true,
            'processed_worker_id', v_worker_id,
            'processed_at_utc', now()
          )
        WHERE processed_work_blocked.id = v_work_row.id;

        v_blocked_count := v_blocked_count + 1;
      END IF;

    EXCEPTION
      WHEN OTHERS THEN
        v_failure_status := CASE
          WHEN SQLSTATE IN ('40001', '40P01', '55P03', '57014') THEN 'FAILED_RETRYABLE'
          WHEN SQLSTATE = 'P0001'
            AND (
              upper(SQLERRM) LIKE '%BLOCK%'
              OR upper(SQLERRM) LIKE '%STALE%'
              OR upper(SQLERRM) LIKE '%AMBIGUOUS%'
              OR upper(SQLERRM) LIKE '%NOT_SAFE%'
              OR upper(SQLERRM) LIKE '%CLASSIFICATION%'
              OR upper(SQLERRM) LIKE '%SETTLED%'
              OR upper(SQLERRM) LIKE '%AUTHORIS%'
              OR upper(SQLERRM) LIKE '%SELECTION%'
              OR upper(SQLERRM) LIKE '%DRIFT%'
              OR upper(SQLERRM) LIKE '%SCOPE%'
            ) THEN 'BLOCKED'
          ELSE 'FAILED_FINAL'
        END;

        UPDATE public.pay_payment_correction_work_items AS failed_work_item
        SET
          status = v_failure_status,
          locked_at_utc = NULL,
          locked_by = NULL,
          last_error = SQLERRM,
          processed_at_utc = CASE WHEN v_failure_status = 'FAILED_RETRYABLE' THEN NULL ELSE now() END,
          processed_by_user_id = COALESCE(v_effective_actor_user_id, failed_work_item.processed_by_user_id),
          result_json = COALESCE(failed_work_item.result_json, '{}'::jsonb) || jsonb_build_object(
            'ok', false,
            'status', v_failure_status,
            'sqlstate', SQLSTATE,
            'error_message', SQLERRM,
            'failed_at_utc', now(),
            'processed_worker_id', v_worker_id,
            'processing_actor_kind', v_processing_actor_kind,
            'processing_actor_user_id', CASE WHEN v_effective_actor_user_id IS NULL THEN NULL ELSE v_effective_actor_user_id::text END,
            'attempt_count', v_work_row.attempt_count
          )
        WHERE failed_work_item.id = v_work_row.id;

        IF v_failure_status = 'BLOCKED' THEN
          v_blocked_count := v_blocked_count + 1;
        ELSIF v_failure_status = 'FAILED_RETRYABLE' THEN
          v_failed_retryable_count := v_failed_retryable_count + 1;
        ELSE
          v_failed_final_count := v_failed_final_count + 1;
        END IF;

        PERFORM public._imp_debug_audit(
          v_effective_actor_user_id,
          'PAYMENT_CORRECTION_PROCESS_CHUNK_WORK_ITEM_ERROR',
          jsonb_build_object(
            'work_item_id', v_work_row.id,
            'correction_request_id', v_work_row.correction_request_id,
            'pay_batch_id', v_work_row.pay_batch_id,
            'work_kind', v_work_row.work_kind,
            'failure_status', v_failure_status,
            'sqlstate', SQLSTATE,
            'error_message', SQLERRM,
            'attempt_count', v_work_row.attempt_count
          ),
          'pay_payment_correction',
          v_work_row.id::text,
          NULL::jsonb,
          NULL::text,
          NULL::text,
          NULL::text
        );
    END;
  END LOOP;

  DROP TABLE IF EXISTS pg_temp._tmp_payment_correction_request_statuses;
  CREATE TEMP TABLE _tmp_payment_correction_request_statuses ON COMMIT DROP AS
  SELECT DISTINCT claimed_request_ids.correction_request_id
  FROM pg_temp._tmp_payment_correction_claimed_work AS claimed_request_ids;

  UPDATE public.pay_payment_correction_requests AS processing_request
  SET
    status = derived_request_status.derived_status,
    applied_at_utc = CASE
      WHEN derived_request_status.derived_status IN ('APPLIED', 'APPLIED_WITH_BLOCKERS')
        THEN COALESCE(processing_request.applied_at_utc, now())
      ELSE processing_request.applied_at_utc
    END,
    updated_at_utc = now()
  FROM (
    SELECT
      request_statuses.correction_request_id,
      CASE
        WHEN work_totals.total_count = 0 THEN 'EXPANDED'
        WHEN work_totals.cancelled_count = work_totals.total_count THEN 'CANCELLED'
        WHEN work_totals.applied_count + work_totals.skipped_count = work_totals.total_count THEN 'APPLIED'
        WHEN work_totals.failed_final_count = work_totals.total_count THEN 'FAILED'
        WHEN work_totals.pending_count + work_totals.processing_count + work_totals.failed_retryable_count > 0 THEN 'PROCESSING'
        WHEN work_totals.blocked_count + work_totals.failed_final_count > 0 THEN 'APPLIED_WITH_BLOCKERS'
        ELSE 'PROCESSING'
      END AS derived_status
    FROM pg_temp._tmp_payment_correction_request_statuses AS request_statuses
    CROSS JOIN LATERAL (
      SELECT
        count(*)::integer AS total_count,
        count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'PENDING')::integer AS pending_count,
        count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'PROCESSING')::integer AS processing_count,
        count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'APPLIED')::integer AS applied_count,
        count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'SKIPPED')::integer AS skipped_count,
        count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'BLOCKED')::integer AS blocked_count,
        count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'FAILED_RETRYABLE')::integer AS failed_retryable_count,
        count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'FAILED_FINAL')::integer AS failed_final_count,
        count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'CANCELLED')::integer AS cancelled_count
      FROM public.pay_payment_correction_work_items
      WHERE public.pay_payment_correction_work_items.correction_request_id = request_statuses.correction_request_id
    ) AS work_totals
  ) AS derived_request_status
  WHERE processing_request.id = derived_request_status.correction_request_id
    AND processing_request.status NOT IN ('REJECTED', 'CANCELLED', 'APPLIED', 'APPLIED_WITH_BLOCKERS', 'FAILED');

  UPDATE public.pay_batches AS completed_local_cancel_batch
  SET
    status = 'CANCELLED',
    cancelled_at_utc = COALESCE(completed_local_cancel_batch.cancelled_at_utc, v_now),
    cancel_reason = COALESCE(completed_local_cancel_batch.cancel_reason, 'Whole-batch pre-bank cancellation completed through payment correction work items.')
  FROM public.pay_payment_correction_requests AS completed_cancel_request
  WHERE completed_cancel_request.id = p_correction_request_id
    AND completed_cancel_request.pay_batch_id = completed_local_cancel_batch.id
    AND completed_cancel_request.correction_kind = 'PRE_BANK_CANCEL'
    AND completed_cancel_request.status = 'APPLIED'
    AND upper(btrim(COALESCE(completed_cancel_request.selection_json->>'scope_type', ''))) = 'BATCH'
    AND NOT EXISTS (
      SELECT 1
      FROM public.pay_batch_items AS remaining_cancel_items
      JOIN public.pay_batch_candidates AS remaining_cancel_candidates
        ON remaining_cancel_candidates.id = remaining_cancel_items.pay_batch_candidate_id
      WHERE remaining_cancel_candidates.pay_batch_id = completed_cancel_request.pay_batch_id
        AND COALESCE(remaining_cancel_items.is_voided, false) = false
        AND remaining_cancel_items.item_type <> 'DEBT_CREATED'
    )
    AND completed_local_cancel_batch.status <> 'CANCELLED';

  IF p_correction_request_id IS NOT NULL THEN
    SELECT jsonb_build_object(
      'total', count(*)::integer,
      'pending', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'PENDING')::integer,
      'processing', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'PROCESSING')::integer,
      'applied', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'APPLIED')::integer,
      'skipped', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'SKIPPED')::integer,
      'blocked', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'BLOCKED')::integer,
      'failed_retryable', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'FAILED_RETRYABLE')::integer,
      'failed_final', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'FAILED_FINAL')::integer,
      'cancelled', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'CANCELLED')::integer
    )
    INTO v_totals
    FROM public.pay_payment_correction_work_items
    WHERE public.pay_payment_correction_work_items.correction_request_id = p_correction_request_id;

    SELECT public.pay_payment_correction_requests.status
    INTO v_parent_status
    FROM public.pay_payment_correction_requests
    WHERE public.pay_payment_correction_requests.id = p_correction_request_id;
  ELSE
    SELECT jsonb_build_object(
      'total', count(*)::integer,
      'pending', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'PENDING')::integer,
      'processing', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'PROCESSING')::integer,
      'applied', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'APPLIED')::integer,
      'skipped', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'SKIPPED')::integer,
      'blocked', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'BLOCKED')::integer,
      'failed_retryable', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'FAILED_RETRYABLE')::integer,
      'failed_final', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'FAILED_FINAL')::integer,
      'cancelled', count(*) FILTER (WHERE public.pay_payment_correction_work_items.status = 'CANCELLED')::integer
    )
    INTO v_totals
    FROM public.pay_payment_correction_work_items
    WHERE public.pay_payment_correction_work_items.id IN (
      SELECT claimed_work_ids.id
      FROM pg_temp._tmp_payment_correction_claimed_work AS claimed_work_ids
    );

    v_parent_status := NULL;
  END IF;


  DROP TABLE IF EXISTS pg_temp._tmp_payment_correction_source_event_updates;
  CREATE TEMP TABLE _tmp_payment_correction_source_event_updates ON COMMIT DROP AS
  WITH request_scope AS (
    SELECT public.pay_payment_correction_requests.id AS correction_request_id
    FROM public.pay_payment_correction_requests
    WHERE public.pay_payment_correction_requests.source_bank_event_id IS NOT NULL
      AND public.pay_payment_correction_requests.status IN ('APPLIED', 'APPLIED_WITH_BLOCKERS', 'BLOCKED', 'FAILED', 'PROCESSING', 'EXPANDED', 'AUTHORISED')
      AND (
        (p_correction_request_id IS NOT NULL AND public.pay_payment_correction_requests.id = p_correction_request_id)
        OR (
          p_correction_request_id IS NULL
          AND public.pay_payment_correction_requests.id = ANY(v_parent_request_ids)
        )
      )
  ), request_totals AS (
    SELECT
      public.pay_payment_correction_requests.id AS correction_request_id,
      public.pay_payment_correction_requests.source_bank_event_id,
      public.pay_payment_correction_requests.status::text AS parent_status,
      count(public.pay_payment_correction_work_items.id)::integer AS total_count,
      count(public.pay_payment_correction_work_items.id) FILTER (WHERE public.pay_payment_correction_work_items.status = 'PENDING')::integer AS pending_count,
      count(public.pay_payment_correction_work_items.id) FILTER (WHERE public.pay_payment_correction_work_items.status = 'PROCESSING')::integer AS processing_count,
      count(public.pay_payment_correction_work_items.id) FILTER (WHERE public.pay_payment_correction_work_items.status = 'APPLIED')::integer AS applied_count,
      count(public.pay_payment_correction_work_items.id) FILTER (WHERE public.pay_payment_correction_work_items.status = 'SKIPPED')::integer AS skipped_count,
      count(public.pay_payment_correction_work_items.id) FILTER (WHERE public.pay_payment_correction_work_items.status = 'BLOCKED')::integer AS blocked_count,
      count(public.pay_payment_correction_work_items.id) FILTER (WHERE public.pay_payment_correction_work_items.status = 'FAILED_RETRYABLE')::integer AS failed_retryable_count,
      count(public.pay_payment_correction_work_items.id) FILTER (WHERE public.pay_payment_correction_work_items.status = 'FAILED_FINAL')::integer AS failed_final_count,
      count(public.pay_payment_correction_work_items.id) FILTER (WHERE public.pay_payment_correction_work_items.status = 'CANCELLED')::integer AS cancelled_count
    FROM request_scope
    JOIN public.pay_payment_correction_requests
      ON public.pay_payment_correction_requests.id = request_scope.correction_request_id
    LEFT JOIN public.pay_payment_correction_work_items
      ON public.pay_payment_correction_work_items.correction_request_id = public.pay_payment_correction_requests.id
    GROUP BY
      public.pay_payment_correction_requests.id,
      public.pay_payment_correction_requests.source_bank_event_id,
      public.pay_payment_correction_requests.status
  ), derived_event_status AS (
    SELECT
      request_totals.correction_request_id,
      request_totals.source_bank_event_id,
      request_totals.parent_status,
      request_totals.total_count,
      request_totals.pending_count,
      request_totals.processing_count,
      request_totals.applied_count,
      request_totals.skipped_count,
      request_totals.blocked_count,
      request_totals.failed_retryable_count,
      request_totals.failed_final_count,
      request_totals.cancelled_count,
      CASE
        WHEN COALESCE(request_totals.total_count, 0) <= 0 THEN 'FAILED'
        WHEN COALESCE(request_totals.failed_retryable_count, 0) > 0 OR COALESCE(request_totals.failed_final_count, 0) > 0 OR request_totals.parent_status = 'FAILED' THEN 'FAILED'
        WHEN COALESCE(request_totals.blocked_count, 0) > 0 OR request_totals.parent_status IN ('APPLIED_WITH_BLOCKERS', 'BLOCKED') THEN 'BLOCKED'
        WHEN request_totals.parent_status = 'APPLIED'
             AND COALESCE(request_totals.applied_count, 0) + COALESCE(request_totals.skipped_count, 0) = COALESCE(request_totals.total_count, 0) THEN 'AUTO_APPLIED'
        WHEN request_totals.parent_status IN ('PROCESSING', 'EXPANDED', 'AUTHORISED')
             AND COALESCE(request_totals.pending_count, 0) + COALESCE(request_totals.processing_count, 0) > 0
             AND COALESCE(request_totals.blocked_count, 0) = 0
             AND COALESCE(request_totals.failed_retryable_count, 0) = 0
             AND COALESCE(request_totals.failed_final_count, 0) = 0 THEN 'AUTO_PROCESSING'
        WHEN COALESCE(request_totals.pending_count, 0) + COALESCE(request_totals.processing_count, 0) > 0
             AND COALESCE(request_totals.blocked_count, 0) = 0
             AND COALESCE(request_totals.failed_retryable_count, 0) = 0
             AND COALESCE(request_totals.failed_final_count, 0) = 0 THEN 'AUTO_PROCESSING'
        ELSE 'FAILED'
      END AS correction_disposition,
      jsonb_build_object(
        'total', COALESCE(request_totals.total_count, 0),
        'pending', COALESCE(request_totals.pending_count, 0),
        'processing', COALESCE(request_totals.processing_count, 0),
        'applied', COALESCE(request_totals.applied_count, 0),
        'skipped', COALESCE(request_totals.skipped_count, 0),
        'blocked', COALESCE(request_totals.blocked_count, 0),
        'failed_retryable', COALESCE(request_totals.failed_retryable_count, 0),
        'failed_final', COALESCE(request_totals.failed_final_count, 0),
        'cancelled', COALESCE(request_totals.cancelled_count, 0)
      ) AS totals_json
    FROM request_totals
  ), updated_events AS (
    UPDATE public.pay_bank_transfer_events AS bank_event_to_update
    SET
      correction_disposition = derived_event_status.correction_disposition
    FROM derived_event_status
    WHERE bank_event_to_update.id = derived_event_status.source_bank_event_id
    RETURNING
      derived_event_status.correction_request_id,
      bank_event_to_update.id AS source_bank_event_id,
      derived_event_status.correction_disposition,
      derived_event_status.parent_status,
      derived_event_status.totals_json,
      (derived_event_status.correction_disposition IN ('BLOCKED', 'FAILED')) AS requires_user_action,
      (derived_event_status.correction_disposition = 'AUTO_PROCESSING') AS processing_continues
  )
  SELECT
    updated_events.correction_request_id,
    updated_events.source_bank_event_id,
    updated_events.correction_disposition,
    updated_events.parent_status,
    updated_events.totals_json,
    updated_events.requires_user_action,
    updated_events.processing_continues
  FROM updated_events;

  SELECT COALESCE(jsonb_agg(jsonb_build_object(
    'correction_request_id', source_event_updates.correction_request_id,
    'event_id', source_event_updates.source_bank_event_id,
    'correction_disposition', source_event_updates.correction_disposition,
    'parent_status', source_event_updates.parent_status,
    'totals', source_event_updates.totals_json,
    'requires_user_action', source_event_updates.requires_user_action,
    'processing_continues', source_event_updates.processing_continues
  ) ORDER BY source_event_updates.correction_request_id), '[]'::jsonb)
  INTO v_source_event_updates
  FROM pg_temp._tmp_payment_correction_source_event_updates AS source_event_updates;

  IF p_correction_request_id IS NOT NULL THEN
    SELECT
      jsonb_build_object(
        'correction_request_id', source_event_updates.correction_request_id,
        'event_id', source_event_updates.source_bank_event_id,
        'correction_disposition', source_event_updates.correction_disposition,
        'parent_status', source_event_updates.parent_status,
        'totals', source_event_updates.totals_json,
        'requires_user_action', source_event_updates.requires_user_action,
        'processing_continues', source_event_updates.processing_continues
      ),
      source_event_updates.source_bank_event_id,
      source_event_updates.correction_disposition,
      source_event_updates.requires_user_action,
      source_event_updates.processing_continues
    INTO
      v_source_event_update,
      v_source_bank_event_id,
      v_source_event_disposition,
      v_requires_user_action,
      v_processing_continues
    FROM pg_temp._tmp_payment_correction_source_event_updates AS source_event_updates
    WHERE source_event_updates.correction_request_id = p_correction_request_id
    ORDER BY source_event_updates.correction_request_id
    LIMIT 1;
  ELSE
    v_source_event_update := NULL::jsonb;
    SELECT
      COALESCE(bool_or(source_event_updates.requires_user_action), false),
      COALESCE(bool_or(source_event_updates.processing_continues), false)
    INTO
      v_requires_user_action,
      v_processing_continues
    FROM pg_temp._tmp_payment_correction_source_event_updates AS source_event_updates;
  END IF;

  SELECT COALESCE(jsonb_agg(DISTINCT job_id_values.value ORDER BY job_id_values.value), '[]'::jsonb)
  INTO v_chunk_workbench_refresh_job_ids
  FROM jsonb_array_elements_text(COALESCE(v_chunk_workbench_refresh_job_ids, '[]'::jsonb)) AS job_id_values(value)
  WHERE NULLIF(BTRIM(job_id_values.value), '') IS NOT NULL;

  v_chunk_workbench_refresh_status := CASE
    WHEN COALESCE(v_chunk_workbench_refresh_failed_count, 0) > 0 THEN 'ERROR'
    WHEN COALESCE(v_chunk_workbench_refresh_queued_count, 0) > 0 THEN 'QUEUED'
    WHEN COALESCE(v_chunk_workbench_refresh_deferred_count, 0) > 0 OR COALESCE(v_chunk_requires_workbench_session, false) THEN 'DEFERRED_REQUIRES_WORKBENCH_SESSION'
    ELSE 'NOT_REQUIRED'
  END;

  SELECT jsonb_build_object(
    'changed_scope_items', COALESCE(v_chunk_changed_scope_items, '[]'::jsonb),
    'changed_pay_batch_item_ids', COALESCE((
      SELECT jsonb_agg(DISTINCT changed_item_values.value_text ORDER BY changed_item_values.value_text)
      FROM jsonb_array_elements(COALESCE(v_chunk_changed_scope_items, '[]'::jsonb)) AS changed_scope_item(scope_json)
      CROSS JOIN LATERAL jsonb_array_elements_text(CASE WHEN jsonb_typeof(changed_scope_item.scope_json->'changed_pay_batch_item_ids') = 'array' THEN changed_scope_item.scope_json->'changed_pay_batch_item_ids' WHEN jsonb_typeof(changed_scope_item.scope_json->'pay_batch_item_ids') = 'array' THEN changed_scope_item.scope_json->'pay_batch_item_ids' ELSE '[]'::jsonb END) AS changed_item_values(value_text)
      WHERE NULLIF(btrim(COALESCE(changed_item_values.value_text, '')), '') IS NOT NULL
    ), '[]'::jsonb),
    'changed_pay_batch_candidate_ids', COALESCE((
      SELECT jsonb_agg(DISTINCT changed_candidate_values.value_text ORDER BY changed_candidate_values.value_text)
      FROM jsonb_array_elements(COALESCE(v_chunk_changed_scope_items, '[]'::jsonb)) AS changed_scope_item(scope_json)
      CROSS JOIN LATERAL jsonb_array_elements_text(CASE WHEN jsonb_typeof(changed_scope_item.scope_json->'changed_pay_batch_candidate_ids') = 'array' THEN changed_scope_item.scope_json->'changed_pay_batch_candidate_ids' WHEN jsonb_typeof(changed_scope_item.scope_json->'pay_batch_candidate_ids') = 'array' THEN changed_scope_item.scope_json->'pay_batch_candidate_ids' ELSE '[]'::jsonb END) AS changed_candidate_values(value_text)
      WHERE NULLIF(btrim(COALESCE(changed_candidate_values.value_text, '')), '') IS NOT NULL
    ), '[]'::jsonb),
    'changed_candidate_ids', COALESCE((
      SELECT jsonb_agg(DISTINCT changed_candidate_values.value_text ORDER BY changed_candidate_values.value_text)
      FROM jsonb_array_elements(COALESCE(v_chunk_changed_scope_items, '[]'::jsonb)) AS changed_scope_item(scope_json)
      CROSS JOIN LATERAL jsonb_array_elements_text(CASE WHEN jsonb_typeof(changed_scope_item.scope_json->'changed_candidate_ids') = 'array' THEN changed_scope_item.scope_json->'changed_candidate_ids' WHEN jsonb_typeof(changed_scope_item.scope_json->'candidate_ids') = 'array' THEN changed_scope_item.scope_json->'candidate_ids' ELSE '[]'::jsonb END) AS changed_candidate_values(value_text)
      WHERE NULLIF(btrim(COALESCE(changed_candidate_values.value_text, '')), '') IS NOT NULL
    ), '[]'::jsonb),
    'changed_transfer_ids', COALESCE((
      SELECT jsonb_agg(DISTINCT changed_transfer_values.value_text ORDER BY changed_transfer_values.value_text)
      FROM jsonb_array_elements(COALESCE(v_chunk_changed_scope_items, '[]'::jsonb)) AS changed_scope_item(scope_json)
      CROSS JOIN LATERAL jsonb_array_elements_text(CASE WHEN jsonb_typeof(changed_scope_item.scope_json->'changed_transfer_ids') = 'array' THEN changed_scope_item.scope_json->'changed_transfer_ids' WHEN jsonb_typeof(changed_scope_item.scope_json->'pay_bank_transfer_ids') = 'array' THEN changed_scope_item.scope_json->'pay_bank_transfer_ids' ELSE '[]'::jsonb END) AS changed_transfer_values(value_text)
      WHERE NULLIF(btrim(COALESCE(changed_transfer_values.value_text, '')), '') IS NOT NULL
    ), '[]'::jsonb),
    'carry_forward_created_count', COALESCE(v_chunk_carry_forward_created_count, 0),
    'carry_forward_existing_count', COALESCE(v_chunk_carry_forward_existing_count, 0),
    'carry_forward_released_count', COALESCE(v_chunk_carry_forward_released_count, 0),
    'released_reservation_count', COALESCE(v_chunk_released_reservation_count, 0),
    'restored_component_count', COALESCE(v_chunk_restored_component_count, 0),
    'workbench_refresh_status', COALESCE(v_chunk_workbench_refresh_status, 'NOT_REQUIRED'),
    'workbench_refresh_queued_count', COALESCE(v_chunk_workbench_refresh_queued_count, 0),
    'workbench_refresh_deferred_count', COALESCE(v_chunk_workbench_refresh_deferred_count, 0),
    'workbench_refresh_failed_count', COALESCE(v_chunk_workbench_refresh_failed_count, 0),
    'workbench_refresh_job_ids', COALESCE(v_chunk_workbench_refresh_job_ids, '[]'::jsonb),
    'requires_workbench_session', COALESCE(v_chunk_requires_workbench_session, false),
    'processed', COALESCE(v_processed_count, 0),
    'applied', COALESCE(v_applied_count, 0),
    'blocked', COALESCE(v_blocked_count, 0),
    'failed_retryable', COALESCE(v_failed_retryable_count, 0),
    'failed_final', COALESCE(v_failed_final_count, 0),
    'grouped_alert_updates', COALESCE(v_chunk_grouped_alert_updates, '[]'::jsonb)
  )
  INTO v_chunk_changed_scope_json;

  FOR v_signal_request IN
    SELECT DISTINCT
      request_rows.id AS correction_request_id,
      request_rows.pay_batch_id AS pay_batch_id,
      request_rows.correction_kind AS correction_kind
    FROM public.pay_payment_correction_requests AS request_rows
    WHERE request_rows.id = ANY(v_parent_request_ids)
      AND request_rows.pay_batch_id IS NOT NULL
    ORDER BY request_rows.pay_batch_id, request_rows.id
  LOOP
    v_signal_changed_scope_json := COALESCE(v_chunk_changed_scope_json, '{}'::jsonb) || jsonb_build_object(
      'correction_request_id', v_signal_request.correction_request_id::text,
      'correction_kind', v_signal_request.correction_kind,
      'parent_status', v_parent_status,
      'totals', COALESCE(v_totals, '{}'::jsonb)
    );

    v_signal_result := public.banking_pay_batch_signal_touch(
      p_pay_batch_id := v_signal_request.pay_batch_id,
      p_change_reason := 'PAYMENT_CORRECTION_PROCESS_CHUNK',
      p_change_source := 'pay_payment_correction_process_chunk',
      p_change_scope_json := v_signal_changed_scope_json,
      p_touch_payment_status := COALESCE(v_processed_count, 0) > 0,
      p_touch_correction_progress := true,
      p_touch_alerts := (
        jsonb_array_length(COALESCE(v_chunk_grouped_alert_updates, '[]'::jsonb)) > 0
        OR COALESCE(v_blocked_count, 0) > 0
        OR COALESCE(v_failed_retryable_count, 0) > 0
        OR COALESCE(v_failed_final_count, 0) > 0
      ),
      p_touch_overview := true
    );

    v_live_signal_updates := v_live_signal_updates || jsonb_build_array(v_signal_result);
  END LOOP;

  PERFORM public._imp_debug_audit(
    p_actor_user_id,
    'PAYMENT_CORRECTION_PROCESS_CHUNK_RESULT',
    jsonb_build_object(
      'correction_request_id', p_correction_request_id,
      'processed', v_processed_count,
      'applied', v_applied_count,
      'skipped', v_skipped_count,
      'blocked', v_blocked_count,
      'failed_retryable', v_failed_retryable_count,
      'failed_final', v_failed_final_count,
      'parent_status', v_parent_status,
      'totals', v_totals
    ),
    'pay_payment_correction',
    COALESCE(p_correction_request_id::text, 'ALL_REQUESTS'),
    NULL::jsonb,
    NULL::text,
    NULL::text,
    NULL::text
  );

  RETURN jsonb_build_object(
    'ok', true,
    'processed', v_processed_count,
    'applied', v_applied_count,
    'skipped', v_skipped_count,
    'blocked', v_blocked_count,
    'failed_retryable', v_failed_retryable_count,
    'failed_final', v_failed_final_count,
    'pending', COALESCE((v_totals->>'pending')::integer, 0),
    'processing', COALESCE((v_totals->>'processing')::integer, 0),
    'progress_completed', COALESCE((v_totals->>'applied')::integer, 0) + COALESCE((v_totals->>'skipped')::integer, 0) + COALESCE((v_totals->>'blocked')::integer, 0) + COALESCE((v_totals->>'failed_final')::integer, 0) + COALESCE((v_totals->>'cancelled')::integer, 0),
    'progress_total', COALESCE((v_totals->>'total')::integer, 0),
    'parent_status', v_parent_status,
    'totals', v_totals,
    'complete', COALESCE((v_totals->>'pending')::integer, 0) = 0 AND COALESCE((v_totals->>'processing')::integer, 0) = 0 AND COALESCE((v_totals->>'failed_retryable')::integer, 0) = 0,
    'carry_forward_created', v_chunk_carry_forward_created_count,
    'carry_forward_existing', v_chunk_carry_forward_existing_count,
    'carry_forward_released', v_chunk_carry_forward_released_count,
    'carry_forward_created_count', v_chunk_carry_forward_created_count,
    'carry_forward_existing_count', v_chunk_carry_forward_existing_count,
    'carry_forward_released_count', v_chunk_carry_forward_released_count,
    'ambiguous_manual_blockers', v_chunk_ambiguous_manual_blocker_count,
    'released_reservations', v_chunk_released_reservation_count,
    'restored_finance_components', v_chunk_restored_component_count,
    'grouped_alert_updates', v_chunk_grouped_alert_updates,
    'workbench_refresh_status', COALESCE(v_chunk_workbench_refresh_status, 'NOT_REQUIRED'),
    'workbench_refresh_queued_count', COALESCE(v_chunk_workbench_refresh_queued_count, 0),
    'workbench_refresh_deferred_count', COALESCE(v_chunk_workbench_refresh_deferred_count, 0),
    'workbench_refresh_failed_count', COALESCE(v_chunk_workbench_refresh_failed_count, 0),
    'workbench_refresh_job_ids', COALESCE(v_chunk_workbench_refresh_job_ids, '[]'::jsonb),
    'requires_workbench_session', COALESCE(v_chunk_requires_workbench_session, false),
    'workbench_refresh', jsonb_build_object(
      'status', COALESCE(v_chunk_workbench_refresh_status, 'NOT_REQUIRED'),
      'queued_count', COALESCE(v_chunk_workbench_refresh_queued_count, 0),
      'deferred_count', COALESCE(v_chunk_workbench_refresh_deferred_count, 0),
      'failed_count', COALESCE(v_chunk_workbench_refresh_failed_count, 0),
      'job_ids', COALESCE(v_chunk_workbench_refresh_job_ids, '[]'::jsonb),
      'results', COALESCE(v_chunk_workbench_refresh_results, '[]'::jsonb),
      'requires_workbench_session', COALESCE(v_chunk_requires_workbench_session, false)
    ),
    'requires_user_action', (
      COALESCE(v_requires_user_action, false)
      OR COALESCE((v_totals->>'blocked')::integer, 0) > 0
      OR COALESCE((v_totals->>'failed_retryable')::integer, 0) > 0
      OR COALESCE((v_totals->>'failed_final')::integer, 0) > 0
    ),
    'processing_continues', (
      COALESCE(v_processing_continues, false)
      OR COALESCE((v_totals->>'pending')::integer, 0) > 0
      OR COALESCE((v_totals->>'processing')::integer, 0) > 0
      OR COALESCE((v_totals->>'failed_retryable')::integer, 0) > 0
    ),
    'source_bank_event_update', v_source_event_update,
    'source_bank_event_updates', v_source_event_updates,
    'changed_scope_json', COALESCE(v_chunk_changed_scope_json, '{}'::jsonb),
    'live_signal_updates', COALESCE(v_live_signal_updates, '[]'::jsonb),
    'live_signal', CASE WHEN jsonb_array_length(COALESCE(v_live_signal_updates, '[]'::jsonb)) = 1 THEN v_live_signal_updates->0 ELSE COALESCE(v_live_signal_updates, '[]'::jsonb) END,
    'processing_actor_kind', CASE WHEN p_actor_user_id IS NULL THEN 'SYSTEM' ELSE 'USER' END,
    'processing_actor_user_id', CASE WHEN p_actor_user_id IS NULL THEN NULL ELSE p_actor_user_id::text END
  );

EXCEPTION
  WHEN OTHERS THEN
    PERFORM public._imp_debug_audit(
      p_actor_user_id,
      'PAYMENT_CORRECTION_PROCESS_CHUNK_ERROR',
      jsonb_build_object(
        'correction_request_id', p_correction_request_id,
        'limit', p_limit,
        'worker_id', p_worker_id,
        'actor_user_id', p_actor_user_id,
        'sqlstate', SQLSTATE,
        'error_message', SQLERRM
      ),
      'pay_payment_correction',
      COALESCE(p_correction_request_id::text, 'ALL_REQUESTS'),
      NULL::jsonb,
      NULL::text,
      NULL::text,
      NULL::text
    );

    RAISE;
END;
$function$;

ALTER FUNCTION pay_payment_correction_process_chunk(uuid,integer,text,uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION pay_payment_correction_process_chunk(uuid,integer,text,uuid) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION pay_payment_correction_process_chunk(uuid,integer,text,uuid) TO postgres;
GRANT EXECUTE ON FUNCTION pay_payment_correction_process_chunk(uuid,integer,text,uuid) TO authenticated;
GRANT EXECUTE ON FUNCTION pay_payment_correction_process_chunk(uuid,integer,text,uuid) TO service_role;
