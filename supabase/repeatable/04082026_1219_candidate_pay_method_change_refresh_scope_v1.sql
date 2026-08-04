-- Banking Pay bounded-scope Version 1.2.4
-- Exact installed TEST baseline; intentionally replaced in place by exact identity.
-- Policy X: pre-draft freshness/orchestration only; frozen post-draft authority is unchanged.

-- -----------------------------------------------------------------------------
-- public.candidate_pay_method_change_refresh_scope_v1(p_candidate_id uuid, p_source_method text, p_target_method text)
-- Installed pg_get_functiondef MD5: ade7473eabd12525f5c93847181f90dd
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.candidate_pay_method_change_refresh_scope_v1(p_candidate_id uuid, p_source_method text, p_target_method text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_source_method text := UPPER(BTRIM(COALESCE(p_source_method, '')));
  v_target_method text := UPPER(BTRIM(COALESCE(p_target_method, '')));
  v_candidate_current_method text;
  v_latest_source_change_seq bigint := 0;
  v_authoritative_sessions_json jsonb := '[]'::jsonb;
  v_replaced_source_session_ids_json jsonb := '[]'::jsonb;
  v_represented_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_authorised_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_active_advance_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_retained_finance_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_targeted_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_target_details_json jsonb := '[]'::jsonb;
  v_preview_row_count integer := 0;
  v_source_target_mismatch_count integer := 0;
  v_uuid_re text := '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';
  v_route_operation_id_text text := NULLIF(BTRIM(COALESCE(current_setting('cloudtms.candidate_pay_method_change_operation_id', true), '')), '');
  v_route_actor_user_id_text text := NULLIF(BTRIM(COALESCE(current_setting('cloudtms.candidate_pay_method_change_actor_user_id', true), '')), '');
  v_route_reason text := NULLIF(BTRIM(COALESCE(current_setting('cloudtms.candidate_pay_method_change_reason', true), '')), '');
  v_route_operation_id uuid := NULL::uuid;
  v_route_actor_user_id uuid := NULL::uuid;
  v_route_actor_role text := NULL::text;
  v_route_job_id uuid := NULL::uuid;
  v_route_job_status text := NULL::text;
  v_route_job_payload jsonb := '{}'::jsonb;
  v_route_dedupe_key text := NULL::text;
  v_route_job_count integer := 0;
  v_route_existing_raw_count integer := 0;
  v_route_existing_invalid_count integer := 0;
  v_route_existing_duplicate_count integer := 0;
  v_route_existing_targeted_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_now timestamptz := clock_timestamp();
  v_scope_invalidation_result jsonb := '{}'::jsonb;
BEGIN
  IF p_candidate_id IS NULL THEN
    RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_CANDIDATE_REQUIRED'
      USING ERRCODE = '22023';
  END IF;

  IF v_source_method NOT IN ('PAYE', 'UMBRELLA') THEN
    RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_SOURCE_METHOD_INVALID'
      USING ERRCODE = '22023', DETAIL = COALESCE(p_source_method, '');
  END IF;

  IF v_target_method NOT IN ('PAYE', 'UMBRELLA') THEN
    RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_TARGET_METHOD_INVALID'
      USING ERRCODE = '22023', DETAIL = COALESCE(p_target_method, '');
  END IF;

  IF v_source_method = v_target_method THEN
    RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_METHODS_MUST_DIFFER'
      USING ERRCODE = '22023';
  END IF;

  SELECT UPPER(BTRIM(COALESCE(candidate_row.pay_method, '')))
  INTO v_candidate_current_method
  FROM public.candidates AS candidate_row
  WHERE candidate_row.id = p_candidate_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_CANDIDATE_NOT_FOUND'
      USING ERRCODE = 'P0002', DETAIL = p_candidate_id::text;
  END IF;

  -- Preview/plan calls must be bound to the candidate's current committed
  -- source route.  The dedicated apply transaction is the only exception:
  -- its trigger calls this helper after the candidate row has already moved
  -- to the target route and supplies the operation/actor context through
  -- transaction-local settings.
  IF v_route_operation_id_text IS NULL
     AND v_route_actor_user_id_text IS NULL
     AND v_candidate_current_method IS DISTINCT FROM v_source_method THEN
    RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_SOURCE_METHOD_STALE'
      USING ERRCODE = '40001',
            DETAIL = jsonb_build_object(
              'candidate_id', p_candidate_id::text,
              'expected_source_method', v_source_method,
              'current_candidate_method', v_candidate_current_method,
              'target_method', v_target_method,
              'refresh_required', true
            )::text;
  END IF;

  SELECT COALESCE(change_counter.seq, 0)
  INTO v_latest_source_change_seq
  FROM public.app_change_counters AS change_counter
  WHERE change_counter.entity_key = 'pay_candidate:' || p_candidate_id::text;

  v_latest_source_change_seq := COALESCE(v_latest_source_change_seq, 0);

  WITH candidate_open_scope AS (
    SELECT
      workbench_session.id AS session_id,
      workbench_session.actor_user_id,
      workbench_session.pay_date,
      workbench_session.week_ending_cutoff,
      workbench_session.version AS session_version,
      workbench_session.progress_state,
      workbench_session.progress_counter_version,
      workbench_session.updated_at_utc,
      MAX(workbench_session.pay_date) OVER (
        PARTITION BY workbench_session.actor_user_id
      ) AS latest_actor_open_pay_date
    FROM public.banking_pay_workbench_session_scope AS session_scope
    JOIN public.banking_pay_workbench_sessions AS workbench_session
      ON workbench_session.id = session_scope.session_id
    WHERE UPPER(BTRIM(COALESCE(workbench_session.status, ''))) = 'OPEN'
      AND workbench_session.discarded_at_utc IS NULL
      AND workbench_session.replacement_session_id IS NULL
      AND session_scope.candidate_id = p_candidate_id
  ),
  authoritative_sessions AS (
    SELECT DISTINCT
      candidate_scope.session_id,
      candidate_scope.actor_user_id,
      candidate_scope.pay_date,
      candidate_scope.week_ending_cutoff,
      candidate_scope.session_version,
      candidate_scope.progress_state,
      candidate_scope.progress_counter_version,
      candidate_scope.updated_at_utc
    FROM candidate_open_scope AS candidate_scope
    WHERE candidate_scope.pay_date = candidate_scope.latest_actor_open_pay_date
  ),
  retained_case_scope AS (
    SELECT
      finance_case.id AS finance_case_id,
      finance_case.linked_timesheet_id,
      finance_case.status,
      finance_case.outstanding_amount,
      finance_case.updated_at
    FROM public.pay_advances AS finance_case
    WHERE finance_case.candidate_id = p_candidate_id
      AND finance_case.written_off_at_utc IS NULL
      AND (
        finance_case.advance_kind IN (
          'OVERPAYMENT'::public.pay_advance_kind_enum,
          'UNDERPAYMENT'::public.pay_advance_kind_enum
        )
        OR finance_case.case_type IN (
          'OVERPAYMENT'::public.pay_finance_case_type_enum,
          'UNDERPAYMENT'::public.pay_finance_case_type_enum
        )
      )
  ),
  retained_finance_authority_rows AS (
    -- Current open finance authority.
    SELECT DISTINCT
      COALESCE(finance_component.linked_timesheet_id, retained_case.linked_timesheet_id) AS timesheet_id,
      NULLIF(UPPER(BTRIM(COALESCE(finance_component.source_pay_method, ''))), '') AS source_pay_method,
      10::integer AS authority_priority,
      COALESCE(finance_component.updated_at_utc, retained_case.updated_at) AS authority_at_utc,
      'OPEN_FINANCE_CASE_OR_COMPONENT'::text AS authority_source
    FROM retained_case_scope AS retained_case
    LEFT JOIN public.pay_finance_case_components AS finance_component
      ON finance_component.finance_case_id = retained_case.finance_case_id
     AND finance_component.candidate_id = p_candidate_id
    WHERE retained_case.status IN (
        'ACTIVE'::public.pay_advance_status_enum,
        'PAUSED'::public.pay_advance_status_enum
      )
      AND (
        ROUND(COALESCE(retained_case.outstanding_amount, 0), 2) > 0
        OR (
          finance_component.id IS NOT NULL
          AND finance_component.closed_at_utc IS NULL
          AND ROUND(COALESCE(finance_component.remaining_source_amount, 0), 2) > 0
        )
      )
      AND COALESCE(finance_component.linked_timesheet_id, retained_case.linked_timesheet_id) IS NOT NULL

    UNION ALL

    -- Frozen active Draft authority. SETTLED is deliberately excluded here;
    -- settled authority is proven from the frozen item and settlement evidence
    -- in the next branch, so it cannot be counted as both active and settled.
    SELECT DISTINCT
      COALESCE(finance_component.linked_timesheet_id, retained_case.linked_timesheet_id) AS timesheet_id,
      NULLIF(UPPER(BTRIM(COALESCE(
        active_reservation.frozen_source_pay_method,
        finance_component.source_pay_method,
        ''
      ))), '') AS source_pay_method,
      20::integer AS authority_priority,
      COALESCE(active_reservation.committed_at_utc, active_reservation.created_at_utc) AS authority_at_utc,
      'ACTIVE_FINANCE_RESERVATION'::text AS authority_source
    FROM public.pay_advance_reservations AS active_reservation
    JOIN retained_case_scope AS retained_case
      ON retained_case.finance_case_id = active_reservation.finance_case_id
    LEFT JOIN public.pay_finance_case_components AS finance_component
      ON finance_component.id = active_reservation.finance_component_id
     AND finance_component.finance_case_id = retained_case.finance_case_id
     AND finance_component.candidate_id = p_candidate_id
    WHERE UPPER(BTRIM(COALESCE(active_reservation.status, ''))) IN ('RESERVED', 'COMMITTED')
      AND active_reservation.released_at_utc IS NULL
      AND COALESCE(finance_component.linked_timesheet_id, retained_case.linked_timesheet_id) IS NOT NULL

    UNION ALL

    -- Immutable settled movements that still contribute to projected retained
    -- value. Voided/reversed no-money authority is excluded exactly once.
    SELECT DISTINCT
      COALESCE(batch_item.timesheet_id, finance_component.linked_timesheet_id, retained_case.linked_timesheet_id) AS timesheet_id,
      NULLIF(UPPER(BTRIM(COALESCE(
        batch_item.frozen_source_pay_method,
        finance_component.source_pay_method,
        batch_item.pay_channel,
        ''
      ))), '') AS source_pay_method,
      30::integer AS authority_priority,
      COALESCE(bank_transfer.completed_at_utc, batch_candidate.settled_at_utc, batch_item.created_at) AS authority_at_utc,
      'ACTIVE_SETTLED_FINANCE_MOVEMENT'::text AS authority_source
    FROM public.pay_batch_items AS batch_item
    JOIN public.pay_batch_candidates AS batch_candidate
      ON batch_candidate.id = batch_item.pay_batch_candidate_id
     AND batch_candidate.candidate_id = p_candidate_id
    JOIN retained_case_scope AS retained_case
      ON retained_case.finance_case_id = batch_item.finance_case_id
    LEFT JOIN public.pay_finance_case_components AS finance_component
      ON finance_component.id = batch_item.finance_component_id
     AND finance_component.finance_case_id = retained_case.finance_case_id
     AND finance_component.candidate_id = p_candidate_id
    LEFT JOIN public.pay_bank_transfers AS bank_transfer
      ON bank_transfer.id = batch_item.pay_bank_transfer_id
    WHERE batch_item.is_voided IS NOT TRUE
      AND UPPER(BTRIM(COALESCE(batch_item.item_type, ''))) IN ('OVERPAYMENT_RECOVERY', 'UNDERPAYMENT_PAYMENT')
      AND (
        UPPER(BTRIM(COALESCE(batch_candidate.settlement_status, ''))) = 'SETTLED'
        OR batch_candidate.settled_at_utc IS NOT NULL
        OR UPPER(BTRIM(COALESCE(bank_transfer.status, ''))) = 'COMPLETED'
        OR bank_transfer.completed_at_utc IS NOT NULL
      )
      AND NOT EXISTS (
        SELECT 1
        FROM public.pay_payment_correction_items AS applied_reversal
        WHERE applied_reversal.pay_batch_item_id = batch_item.id
          AND applied_reversal.status = 'APPLIED'
          AND applied_reversal.correction_item_kind IN ('PRE_BANK_CANCEL', 'NO_MONEY_UNWIND', 'SETTLED_REVERSAL')
      )
      AND COALESCE(batch_item.timesheet_id, finance_component.linked_timesheet_id, retained_case.linked_timesheet_id) IS NOT NULL

    UNION ALL

    -- Applied corrections are retained only where they are one of the exact
    -- authority-changing correction kinds and remain linked to this finance
    -- lineage. Generic historic events do not broaden route-change scope.
    SELECT DISTINCT
      COALESCE(correction_item.timesheet_id, finance_component.linked_timesheet_id, retained_case.linked_timesheet_id) AS timesheet_id,
      NULLIF(UPPER(BTRIM(COALESCE(finance_component.source_pay_method, ''))), '') AS source_pay_method,
      40::integer AS authority_priority,
      COALESCE(correction_item.applied_at_utc, correction_item.created_at_utc) AS authority_at_utc,
      'APPLIED_FINANCE_CORRECTION'::text AS authority_source
    FROM public.pay_payment_correction_items AS correction_item
    JOIN retained_case_scope AS retained_case
      ON retained_case.finance_case_id = correction_item.finance_case_id
    LEFT JOIN public.pay_finance_case_components AS finance_component
      ON finance_component.id = correction_item.finance_component_id
     AND finance_component.finance_case_id = retained_case.finance_case_id
     AND finance_component.candidate_id = p_candidate_id
    WHERE correction_item.candidate_id = p_candidate_id
      AND correction_item.status = 'APPLIED'
      AND correction_item.correction_item_kind IN ('PRE_BANK_CANCEL', 'NO_MONEY_UNWIND', 'SETTLED_REVERSAL')
      AND COALESCE(correction_item.timesheet_id, finance_component.linked_timesheet_id, retained_case.linked_timesheet_id) IS NOT NULL
  ),
  canonical_seed_ids AS (
    SELECT COALESCE(
      array_agg(DISTINCT seed.timesheet_id ORDER BY seed.timesheet_id),
      ARRAY[]::uuid[]
    ) AS timesheet_ids
    FROM (
      -- Ordinary pay-method invalidation begins from bounded active economic
      -- state.  It never rediscovers the candidate's lifetime timesheet set.
      SELECT scope_state.timesheet_id
      FROM private.banking_pay_workbench_timesheet_scope_state AS scope_state
      WHERE scope_state.candidate_id=p_candidate_id
        AND scope_state.economic_state IN ('DIRTY','LIVE')

      UNION

      SELECT source_owner.timesheet_id
      FROM public.banking_pay_workbench_candidate_source_lines AS source_owner
      WHERE source_owner.candidate_id=p_candidate_id
        AND source_owner.status IN ('CURRENT','DIRTY')
        AND source_owner.timesheet_id IS NOT NULL

      UNION

      SELECT payment_override.timesheet_id
      FROM public.timesheet_payment_overrides AS payment_override
      WHERE payment_override.candidate_id = p_candidate_id
        AND UPPER(BTRIM(COALESCE(payment_override.override_type, ''))) = 'ADVANCE_THIS_PAYMENT'
        AND payment_override.cleared_at_utc IS NULL
        AND payment_override.consumed_at_utc IS NULL
        AND payment_override.consumed_by_pay_batch_id IS NULL
        AND payment_override.timesheet_id IS NOT NULL

      UNION

      SELECT retained_authority.timesheet_id
      FROM retained_finance_authority_rows AS retained_authority
      WHERE retained_authority.timesheet_id IS NOT NULL
    ) AS seed
  ),
  rotation_scope AS (
    SELECT
      rotation_row.requested_timesheet_id,
      rotation_row.booking_id,
      rotation_row.canonical_timesheet_id,
      rotation_row.family_timesheet_id,
      rotation_row.family_is_current,
      rotation_row.family_version,
      rotation_row.requested_is_canonical
    FROM public._pay_timesheet_rotation_scope(
      COALESCE((SELECT canonical_seed_ids.timesheet_ids FROM canonical_seed_ids), ARRAY[]::uuid[])
    ) AS rotation_row
  ),
  canonical_ids AS (
    SELECT DISTINCT
      COALESCE(rotation_row.canonical_timesheet_id, seed_id.timesheet_id) AS timesheet_id
    FROM unnest(
      COALESCE((SELECT canonical_seed_ids.timesheet_ids FROM canonical_seed_ids), ARRAY[]::uuid[])
    ) AS seed_id(timesheet_id)
    LEFT JOIN rotation_scope AS rotation_row
      ON rotation_row.requested_timesheet_id = seed_id.timesheet_id
    WHERE COALESCE(rotation_row.canonical_timesheet_id, seed_id.timesheet_id) IS NOT NULL
  ),
  canonical_qualification AS (
    SELECT
      canonical_id.timesheet_id,
      (
        current_timesheet.archived_at_utc IS NULL
        AND current_timesheet.revoked_at IS NULL
        AND current_timesheet.is_current IS TRUE
        AND current_timesheet.authorised_at_server IS NOT NULL
      ) AS is_authorised,
      EXISTS (
        SELECT 1
        FROM public.timesheet_payment_overrides AS payment_override
        WHERE payment_override.candidate_id = p_candidate_id
          AND UPPER(BTRIM(COALESCE(payment_override.override_type, ''))) = 'ADVANCE_THIS_PAYMENT'
          AND payment_override.cleared_at_utc IS NULL
          AND payment_override.consumed_at_utc IS NULL
          AND payment_override.consumed_by_pay_batch_id IS NULL
          AND (
            payment_override.timesheet_id = canonical_id.timesheet_id
            OR payment_override.timesheet_id IN (
              SELECT family_row.family_timesheet_id
              FROM rotation_scope AS family_row
              WHERE family_row.canonical_timesheet_id = canonical_id.timesheet_id
            )
          )
      ) AS has_active_advance,
      EXISTS (
        SELECT 1
        FROM retained_finance_authority_rows AS retained_authority
        WHERE retained_authority.timesheet_id = canonical_id.timesheet_id
           OR retained_authority.timesheet_id IN (
             SELECT family_row.family_timesheet_id
             FROM rotation_scope AS family_row
             WHERE family_row.canonical_timesheet_id = canonical_id.timesheet_id
           )
      ) AS has_retained_finance_authority,
      UPPER(BTRIM(COALESCE(
        current_financial.pay_method,
        retained_finance_authority.source_pay_method,
        current_contract.pay_method_snapshot,
        ''
      ))) AS source_pay_method
    FROM canonical_ids AS canonical_id
    LEFT JOIN public.timesheets AS current_timesheet
      ON current_timesheet.timesheet_id = canonical_id.timesheet_id
     AND current_timesheet.is_current IS TRUE
    LEFT JOIN LATERAL (
      SELECT financial_row.pay_method
      FROM public.timesheets_financials AS financial_row
      WHERE financial_row.timesheet_id = canonical_id.timesheet_id
        AND financial_row.is_current IS TRUE
        AND financial_row.candidate_id = p_candidate_id
      ORDER BY financial_row.timesheet_version DESC, financial_row.id DESC
      LIMIT 1
    ) AS current_financial ON TRUE
    LEFT JOIN LATERAL (
      SELECT retained_authority.source_pay_method
      FROM retained_finance_authority_rows AS retained_authority
      WHERE (
          retained_authority.timesheet_id = canonical_id.timesheet_id
          OR retained_authority.timesheet_id IN (
            SELECT family_row.family_timesheet_id
            FROM rotation_scope AS family_row
            WHERE family_row.canonical_timesheet_id = canonical_id.timesheet_id
          )
        )
        AND retained_authority.source_pay_method IN ('PAYE', 'UMBRELLA')
      ORDER BY
        retained_authority.authority_priority,
        retained_authority.authority_at_utc DESC NULLS LAST,
        retained_authority.source_pay_method
      LIMIT 1
    ) AS retained_finance_authority ON TRUE
    LEFT JOIN public.contracts AS current_contract
      ON current_contract.id = current_timesheet.contract_id
     AND current_contract.candidate_id = p_candidate_id
  ),
  qualifying_timesheets AS (
    SELECT
      qualification.timesheet_id,
      qualification.is_authorised,
      qualification.has_active_advance,
      qualification.has_retained_finance_authority,
      qualification.source_pay_method
    FROM canonical_qualification AS qualification
    WHERE qualification.is_authorised IS TRUE
       OR qualification.has_active_advance IS TRUE
       OR qualification.has_retained_finance_authority IS TRUE
  ),
  current_preview_rows AS (
    SELECT DISTINCT
      authoritative_session.session_id,
      authoritative_session.session_version,
      preview_row.id AS preview_row_id,
      preview_row.timesheet_id AS requested_timesheet_id,
      COALESCE(current_preview_timesheet.timesheet_id, preview_row.timesheet_id) AS canonical_timesheet_id
    FROM authoritative_sessions AS authoritative_session
    JOIN public.banking_pay_workbench_preview_rows AS preview_row
      ON preview_row.session_id = authoritative_session.session_id
     AND preview_row.candidate_id = p_candidate_id
     AND preview_row.session_version = authoritative_session.session_version
    LEFT JOIN public.timesheets AS requested_preview_timesheet
      ON requested_preview_timesheet.timesheet_id = preview_row.timesheet_id
    LEFT JOIN LATERAL (
      SELECT candidate_current_timesheet.timesheet_id
      FROM public.timesheets AS candidate_current_timesheet
      WHERE requested_preview_timesheet.booking_id IS NOT NULL
        AND candidate_current_timesheet.booking_id = requested_preview_timesheet.booking_id
        AND candidate_current_timesheet.is_current IS TRUE
      ORDER BY candidate_current_timesheet.version DESC NULLS LAST,
               candidate_current_timesheet.updated_at DESC NULLS LAST,
               candidate_current_timesheet.created_at DESC NULLS LAST,
               candidate_current_timesheet.timesheet_id DESC
      LIMIT 1
    ) AS current_preview_timesheet ON TRUE
    WHERE preview_row.timesheet_id IS NOT NULL
  ),
  represented_ids AS (
    SELECT COALESCE(
      array_agg(DISTINCT preview_row.canonical_timesheet_id ORDER BY preview_row.canonical_timesheet_id),
      ARRAY[]::uuid[]
    ) AS timesheet_ids
    FROM current_preview_rows AS preview_row
    WHERE preview_row.canonical_timesheet_id IS NOT NULL
  ),
  session_rollup AS (
    SELECT
      authoritative_session.session_id,
      authoritative_session.actor_user_id,
      authoritative_session.pay_date,
      authoritative_session.week_ending_cutoff,
      authoritative_session.session_version,
      authoritative_session.progress_state,
      authoritative_session.progress_counter_version,
      authoritative_session.updated_at_utc,
      COUNT(DISTINCT current_preview.preview_row_id)::integer AS preview_row_count,
      COALESCE(
        array_agg(DISTINCT current_preview.canonical_timesheet_id ORDER BY current_preview.canonical_timesheet_id)
          FILTER (WHERE current_preview.canonical_timesheet_id IS NOT NULL),
        ARRAY[]::uuid[]
      ) AS represented_timesheet_ids,
      COALESCE(
        (SELECT array_agg(qualifying.timesheet_id ORDER BY qualifying.timesheet_id) FROM qualifying_timesheets AS qualifying),
        ARRAY[]::uuid[]
      ) AS qualifying_timesheet_ids
    FROM authoritative_sessions AS authoritative_session
    LEFT JOIN current_preview_rows AS current_preview
      ON current_preview.session_id = authoritative_session.session_id
     AND current_preview.session_version = authoritative_session.session_version
    GROUP BY
      authoritative_session.session_id,
      authoritative_session.actor_user_id,
      authoritative_session.pay_date,
      authoritative_session.week_ending_cutoff,
      authoritative_session.session_version,
      authoritative_session.progress_state,
      authoritative_session.progress_counter_version,
      authoritative_session.updated_at_utc
  )
  SELECT
    COALESCE((
      SELECT jsonb_agg(
        jsonb_build_object(
          'session_id', session_row.session_id::text,
          'actor_user_id', session_row.actor_user_id::text,
          'pay_date', session_row.pay_date::text,
          'week_ending_cutoff', session_row.week_ending_cutoff::text,
          'session_version', session_row.session_version,
          'progress_state', session_row.progress_state,
          'progress_counter_version', session_row.progress_counter_version,
          'preview_row_count', session_row.preview_row_count,
          'represented_timesheet_ids', to_jsonb(session_row.represented_timesheet_ids),
          'qualifying_timesheet_ids', to_jsonb(session_row.qualifying_timesheet_ids),
          'updated_at_utc', session_row.updated_at_utc::text
        )
        ORDER BY session_row.updated_at_utc DESC, session_row.session_id
      )
      FROM session_rollup AS session_row
    ), '[]'::jsonb),
    COALESCE((SELECT COUNT(*)::integer FROM current_preview_rows), 0),
    COALESCE((SELECT represented_ids.timesheet_ids FROM represented_ids), ARRAY[]::uuid[]),
    COALESCE((
      SELECT array_agg(qualification.timesheet_id ORDER BY qualification.timesheet_id)
      FROM canonical_qualification AS qualification
      WHERE qualification.is_authorised IS TRUE
    ), ARRAY[]::uuid[]),
    COALESCE((
      SELECT array_agg(qualification.timesheet_id ORDER BY qualification.timesheet_id)
      FROM canonical_qualification AS qualification
      WHERE qualification.has_active_advance IS TRUE
    ), ARRAY[]::uuid[]),
    COALESCE((
      SELECT array_agg(qualification.timesheet_id ORDER BY qualification.timesheet_id)
      FROM canonical_qualification AS qualification
      WHERE qualification.has_retained_finance_authority IS TRUE
    ), ARRAY[]::uuid[]),
    COALESCE((
      SELECT array_agg(qualifying.timesheet_id ORDER BY qualifying.timesheet_id)
      FROM qualifying_timesheets AS qualifying
    ), ARRAY[]::uuid[]),
    COALESCE((
      SELECT jsonb_agg(
        jsonb_build_object(
          'timesheet_id', qualifying.timesheet_id::text,
          'is_authorised', qualifying.is_authorised,
          'has_active_advance', qualifying.has_active_advance,
          'has_retained_finance_authority', qualifying.has_retained_finance_authority,
          'source_pay_method', NULLIF(qualifying.source_pay_method, ''),
          'target_pay_method', v_target_method,
          'source_target_mismatch', (
            qualifying.source_pay_method IN ('PAYE', 'UMBRELLA')
            AND qualifying.source_pay_method <> v_target_method
          )
        )
        ORDER BY qualifying.timesheet_id
      )
      FROM qualifying_timesheets AS qualifying
    ), '[]'::jsonb),
    COALESCE((
      SELECT COUNT(*)::integer
      FROM qualifying_timesheets AS qualifying
      WHERE qualifying.source_pay_method IN ('PAYE', 'UMBRELLA')
        AND qualifying.source_pay_method <> v_target_method
    ), 0)
  INTO
    v_authoritative_sessions_json,
    v_preview_row_count,
    v_represented_timesheet_ids,
    v_authorised_timesheet_ids,
    v_active_advance_timesheet_ids,
    v_retained_finance_timesheet_ids,
    v_targeted_timesheet_ids,
    v_target_details_json,
    v_source_target_mismatch_count;

  v_scope_invalidation_result:=private.pay_workbench_scope_invalidate_v1(
    CASE WHEN cardinality(v_targeted_timesheet_ids)=0 THEN ARRAY[p_candidate_id]
      ELSE array_fill(p_candidate_id,ARRAY[cardinality(v_targeted_timesheet_ids)]) END,
    CASE WHEN cardinality(v_targeted_timesheet_ids)=0 THEN ARRAY[NULL::uuid]
      ELSE v_targeted_timesheet_ids END,
    'CANDIDATE_PAY_METHOD_CHANGED',NULL,
    jsonb_build_object('source_pay_method',v_source_method,'target_pay_method',v_target_method)
  );

  WITH authoritative_session_ids AS (
    SELECT (session_value->>'session_id')::uuid AS session_id
    FROM jsonb_array_elements(v_authoritative_sessions_json) AS session_values(session_value)
    WHERE COALESCE(session_value->>'session_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  )
  SELECT COALESCE(
    jsonb_agg(source_session.id::text ORDER BY source_session.updated_at_utc DESC, source_session.id),
    '[]'::jsonb
  )
  INTO v_replaced_source_session_ids_json
  FROM public.banking_pay_workbench_sessions AS source_session
  WHERE source_session.replacement_session_id IN (
    SELECT authoritative_session.session_id
    FROM authoritative_session_ids AS authoritative_session
  )
    AND (
      EXISTS (
        SELECT 1
        FROM public.banking_pay_workbench_session_scope AS source_scope
        WHERE source_scope.session_id = source_session.id
          AND source_scope.candidate_id = p_candidate_id
      )
      OR EXISTS (
        SELECT 1
        FROM public.banking_pay_workbench_preview_rows AS source_preview
        WHERE source_preview.session_id = source_session.id
          AND source_preview.candidate_id = p_candidate_id
      )
    );

  IF v_route_operation_id_text IS NOT NULL OR v_route_actor_user_id_text IS NOT NULL THEN
    IF v_route_operation_id_text IS NULL OR v_route_operation_id_text !~* v_uuid_re THEN
      RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_OPERATION_CONTEXT_REQUIRED'
        USING ERRCODE = '22023';
    END IF;

    IF v_route_actor_user_id_text IS NULL OR v_route_actor_user_id_text !~* v_uuid_re THEN
      RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_ACTOR_CONTEXT_REQUIRED'
        USING ERRCODE = '22023';
    END IF;

    v_route_operation_id := v_route_operation_id_text::uuid;
    v_route_actor_user_id := v_route_actor_user_id_text::uuid;

    SELECT LOWER(BTRIM(COALESCE(actor_row.role, '')))
    INTO v_route_actor_role
    FROM public.tms_users AS actor_row
    WHERE actor_row.id = v_route_actor_user_id
      AND actor_row.is_active IS TRUE;

    IF NOT FOUND OR v_route_actor_role <> 'admin' THEN
      RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_ADMIN_REQUIRED'
        USING ERRCODE = '42501', DETAIL = v_route_actor_user_id::text;
    END IF;

    IF v_candidate_current_method IS DISTINCT FROM v_target_method THEN
      RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_TARGET_NOT_COMMITTED'
        USING ERRCODE = '40001',
              DETAIL = jsonb_build_object(
                'candidate_id', p_candidate_id::text,
                'candidate_current_method', v_candidate_current_method,
                'expected_target_method', v_target_method,
                'operation_id', v_route_operation_id::text
              )::text;
    END IF;

    v_route_dedupe_key := 'CANDIDATE_PAY_METHOD_CHANGE:' || p_candidate_id::text || ':' || v_route_operation_id::text;
    PERFORM pg_advisory_xact_lock(hashtext('candidate_pay_method_change_job:' || v_route_operation_id::text));

    SELECT COUNT(*)::integer
    INTO v_route_job_count
    FROM public.banking_pay_workbench_jobs AS existing_job
    WHERE existing_job.dedupe_key = v_route_dedupe_key;

    IF v_route_job_count > 1 THEN
      RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_DURABLE_QUEUE_DUPLICATED'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'candidate_id', p_candidate_id::text,
                'operation_id', v_route_operation_id::text,
                'dedupe_key', v_route_dedupe_key,
                'job_count', v_route_job_count
              )::text;
    END IF;

    SELECT
      existing_job.id,
      existing_job.status,
      COALESCE(existing_job.payload_json, '{}'::jsonb)
    INTO
      v_route_job_id,
      v_route_job_status,
      v_route_job_payload
    FROM public.banking_pay_workbench_jobs AS existing_job
    WHERE existing_job.dedupe_key = v_route_dedupe_key
    ORDER BY existing_job.updated_at_utc DESC, existing_job.id DESC
    LIMIT 1
    FOR UPDATE;

    IF v_route_job_id IS NOT NULL THEN
      IF COALESCE(v_route_job_payload->>'candidate_id', '') <> p_candidate_id::text
         OR COALESCE(v_route_job_payload->>'route_change_operation_id', '') <> v_route_operation_id::text
         OR UPPER(BTRIM(COALESCE(v_route_job_payload->>'route_change_source_method', ''))) <> v_source_method
         OR UPPER(BTRIM(COALESCE(v_route_job_payload->>'route_change_target_method', ''))) <> v_target_method THEN
        RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_DURABLE_QUEUE_AUTHORITY_MISMATCH'
          USING ERRCODE = 'P0001', DETAIL = v_route_job_id::text;
      END IF;

      SELECT
        COUNT(*)::integer,
        COUNT(*) FILTER (WHERE raw_target.value !~* v_uuid_re)::integer,
        COALESCE(
          array_agg(DISTINCT raw_target.value::uuid ORDER BY raw_target.value::uuid)
            FILTER (WHERE raw_target.value ~* v_uuid_re),
          ARRAY[]::uuid[]
        )
      INTO
        v_route_existing_raw_count,
        v_route_existing_invalid_count,
        v_route_existing_targeted_timesheet_ids
      FROM jsonb_array_elements_text(
        CASE
          WHEN jsonb_typeof(v_route_job_payload->'targeted_timesheet_ids') = 'array'
            THEN v_route_job_payload->'targeted_timesheet_ids'
          ELSE '[]'::jsonb
        END
      ) AS raw_target(value);

      v_route_existing_duplicate_count := GREATEST(
        COALESCE(v_route_existing_raw_count, 0)
          - COALESCE(v_route_existing_invalid_count, 0)
          - COALESCE(array_length(v_route_existing_targeted_timesheet_ids, 1), 0),
        0
      );

      IF COALESCE(v_route_existing_invalid_count, 0) <> 0
         OR COALESCE(v_route_existing_duplicate_count, 0) <> 0
         OR v_route_existing_targeted_timesheet_ids IS DISTINCT FROM v_targeted_timesheet_ids
         OR LOWER(BTRIM(COALESCE(v_route_job_payload->>'exact_target_scope', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
         OR UPPER(BTRIM(COALESCE(v_route_job_payload->>'refresh_scope_kind', ''))) <> 'TARGETED_TIMESHEETS' THEN
        RAISE EXCEPTION 'CANDIDATE_PAY_METHOD_CHANGE_DURABLE_QUEUE_SET_MISMATCH'
          USING ERRCODE = 'P0001',
                DETAIL = jsonb_build_object(
                  'job_id', v_route_job_id::text,
                  'expected_targeted_timesheet_ids', to_jsonb(v_targeted_timesheet_ids),
                  'persisted_targeted_timesheet_ids', to_jsonb(v_route_existing_targeted_timesheet_ids),
                  'invalid_target_count', v_route_existing_invalid_count,
                  'effective_duplicate_count', v_route_existing_duplicate_count
                )::text;
      END IF;

      v_latest_source_change_seq := COALESCE(
        CASE
          WHEN COALESCE(v_route_job_payload->>'source_change_seq', '') ~ '^[0-9]{1,18}$'
            THEN (v_route_job_payload->>'source_change_seq')::bigint
          ELSE v_latest_source_change_seq
        END,
        v_latest_source_change_seq,
        0
      );
    ELSE
      PERFORM public._change_bump('pay_candidate:' || p_candidate_id::text);

      SELECT COALESCE(change_counter.seq, 0)
      INTO v_latest_source_change_seq
      FROM public.app_change_counters AS change_counter
      WHERE change_counter.entity_key = 'pay_candidate:' || p_candidate_id::text;

      v_latest_source_change_seq := COALESCE(v_latest_source_change_seq, 0);

      v_route_job_payload := (
        jsonb_build_object(
          'job_type', 'WORKBENCH_CANDIDATE_DIRTY_APPLY',
          'scope_kind', 'CANDIDATE',
          'scope_id', p_candidate_id::text,
          'candidate_id', p_candidate_id::text,
          'reason', COALESCE(v_route_reason, 'PAY_METHOD_CHANGE'),
          'dirty_reason', COALESCE(v_route_reason, 'PAY_METHOD_CHANGE'),
          'candidate_pay_method_change', true,
          'candidate_payment_status_changed', true,
          'pay_method_changed', true,
          'prospective_only', true,
          'old_pay_method', v_source_method,
          'new_pay_method', v_target_method,
          'route_change_operation_id', v_route_operation_id::text,
          'route_change_actor_user_id', v_route_actor_user_id::text,
          'actor_user_id', v_route_actor_user_id::text,
          'route_change_reason', COALESCE(v_route_reason, 'PAY_METHOD_CHANGE'),
          'route_change_source_method', v_source_method,
          'route_change_target_method', v_target_method,
          'latest_source_change_seq', v_latest_source_change_seq,
          'source_change_seq', v_latest_source_change_seq,
          'source_change_sequence', v_latest_source_change_seq
        )
        || jsonb_build_object(
          'refresh_scope_kind', 'TARGETED_TIMESHEETS',
          'targeted_timesheet_ids', to_jsonb(v_targeted_timesheet_ids),
          'targeted_scope_is_empty', COALESCE(array_length(v_targeted_timesheet_ids, 1), 0) = 0,
          'exact_target_scope', true,
          'coverage_basis', 'CANONICAL_TIMESHEETS_WITH_RETAINED_FINANCE_AUTHORITY',
          'coverage_complete', true,
          'authorised_timesheet_ids', to_jsonb(v_authorised_timesheet_ids),
          'authorised_timesheet_count', COALESCE(array_length(v_authorised_timesheet_ids, 1), 0),
          'active_advance_timesheet_ids', to_jsonb(v_active_advance_timesheet_ids),
          'active_advance_timesheet_count', COALESCE(array_length(v_active_advance_timesheet_ids, 1), 0),
          'retained_finance_timesheet_ids', to_jsonb(v_retained_finance_timesheet_ids),
          'retained_finance_timesheet_count', COALESCE(array_length(v_retained_finance_timesheet_ids, 1), 0),
          'authoritative_sessions', v_authoritative_sessions_json,
          'replaced_source_session_ids', v_replaced_source_session_ids_json,
          'target_details', v_target_details_json,
          'source_target_mismatch_count', COALESCE(v_source_target_mismatch_count, 0),
          'source_build_required', COALESCE(array_length(v_targeted_timesheet_ids, 1), 0) > 0,
          'line_work_required', COALESCE(array_length(v_targeted_timesheet_ids, 1), 0) > 0,
          'line_work_only', false,
          'line_work_action', 'SOURCE_BUILD',
          'rerun_required', false
        )
        || jsonb_build_object(
          'contracts_changed', 0,
          'contract_weeks_changed', 0,
          'timesheets_changed', 0,
          'rates_changed', 0,
          'tsfin_repricing_rows', 0,
          'source_records_mutated', false,
          'economic_truth_mutation_allowed', false,
          'refresh_completion_requires_terminal_exact_scope', true,
          'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH',
          'policy_x_dirtying_only', true
        )
      );

      INSERT INTO public.banking_pay_workbench_jobs (
        job_type,
        status,
        priority,
        run_at_utc,
        attempt_count,
        max_attempts,
        dedupe_key,
        snapshot_run_id,
        session_id,
        candidate_id,
        payload_json,
        created_at_utc,
        updated_at_utc,
        started_at_utc,
        completed_at_utc,
        failed_at_utc,
        last_error_json
      )
      VALUES (
        'WORKBENCH_CANDIDATE_DIRTY_APPLY',
        CASE WHEN COALESCE(array_length(v_targeted_timesheet_ids, 1), 0) = 0 THEN 'SUCCEEDED' ELSE 'QUEUED' END,
        -1000,
        v_now,
        0,
        8,
        v_route_dedupe_key,
        NULL,
        NULL,
        p_candidate_id,
        v_route_job_payload,
        v_now,
        v_now,
        NULL,
        CASE WHEN COALESCE(array_length(v_targeted_timesheet_ids, 1), 0) = 0 THEN v_now ELSE NULL END,
        NULL,
        NULL
      )
      RETURNING id, status, payload_json
      INTO v_route_job_id, v_route_job_status, v_route_job_payload;
    END IF;
  END IF;

  RETURN (
    jsonb_build_object(
      'ok', true,
      'candidate_id', p_candidate_id::text,
      'candidate_current_method', v_candidate_current_method,
      'source_method', v_source_method,
      'target_method', v_target_method,
      'latest_source_change_seq', v_latest_source_change_seq,
      'source_change_seq', v_latest_source_change_seq,
      'durable_job_id', CASE WHEN v_route_job_id IS NULL THEN NULL ELSE v_route_job_id::text END,
      'durable_job_status', v_route_job_status,
      'durable_job_dedupe_key', v_route_dedupe_key,
      'durable_scope_persisted', v_route_job_id IS NOT NULL,
      'coverage_basis', 'CANONICAL_TIMESHEETS_WITH_RETAINED_FINANCE_AUTHORITY',
      'coverage_complete', true,
      'exact_scope', true,
      'authoritative_sessions', v_authoritative_sessions_json,
      'replaced_source_session_ids', v_replaced_source_session_ids_json,
      'represented_timesheet_ids', to_jsonb(v_represented_timesheet_ids),
      'authorised_timesheet_ids', to_jsonb(v_authorised_timesheet_ids),
      'active_advance_timesheet_ids', to_jsonb(v_active_advance_timesheet_ids),
      'retained_finance_timesheet_ids', to_jsonb(v_retained_finance_timesheet_ids)
    )
    || jsonb_build_object(
      'targeted_timesheet_ids', to_jsonb(v_targeted_timesheet_ids),
      'targeted_scope_is_empty', COALESCE(array_length(v_targeted_timesheet_ids, 1), 0) = 0,
      'authoritative_session_count', jsonb_array_length(v_authoritative_sessions_json),
      'preview_row_count', v_preview_row_count,
      'represented_timesheet_count', COALESCE(array_length(v_represented_timesheet_ids, 1), 0),
      'authorised_timesheet_count', COALESCE(array_length(v_authorised_timesheet_ids, 1), 0),
      'active_advance_timesheet_count', COALESCE(array_length(v_active_advance_timesheet_ids, 1), 0),
      'retained_finance_timesheet_count', COALESCE(array_length(v_retained_finance_timesheet_ids, 1), 0),
      'targeted_timesheet_count', COALESCE(array_length(v_targeted_timesheet_ids, 1), 0),
      'source_target_mismatch_count', COALESCE(v_source_target_mismatch_count, 0),
      'target_details', v_target_details_json,
      'contracts_changed', 0,
      'contract_weeks_changed', 0,
      'timesheets_changed', 0,
      'rates_changed', 0,
      'tsfin_repricing_rows', 0,
      'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH',
      'policy_x_dirtying_only', true
    )
  );
END;
$function$;

ALTER FUNCTION public.candidate_pay_method_change_refresh_scope_v1(p_candidate_id uuid, p_source_method text, p_target_method text) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.candidate_pay_method_change_refresh_scope_v1(p_candidate_id uuid, p_source_method text, p_target_method text) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.candidate_pay_method_change_refresh_scope_v1(p_candidate_id uuid, p_source_method text, p_target_method text) TO postgres;
GRANT EXECUTE ON FUNCTION public.candidate_pay_method_change_refresh_scope_v1(p_candidate_id uuid, p_source_method text, p_target_method text) TO service_role;
