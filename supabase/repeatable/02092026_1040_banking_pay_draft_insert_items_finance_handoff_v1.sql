CREATE OR REPLACE FUNCTION public.pay_batch_insert_items_from_preview(
  p_pay_batch_id uuid,
  p_actor_user_id uuid DEFAULT NULL::uuid,
  p_operation_id uuid DEFAULT NULL::uuid,
  p_candidate_scope_ids jsonb DEFAULT NULL::jsonb
)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := now();
  v_scope_ids jsonb := COALESCE(p_candidate_scope_ids, '[]'::jsonb);
  v_scope_id_count integer := 0;
  v_inserted_count integer := 0;
  v_reused_count integer := 0;
  v_failed_count integer := 0;
  v_expected_allocation_row_count integer := 0;
  v_page_allocation_row_count integer := 0;
  v_ordinary_page_allocation_row_count integer := 0;
  v_deferred_finance_adjustment_row_count integer := 0;
  v_linked_allocation_row_count integer := 0;
  v_scope_expects_item_count integer := 0;
  v_candidate_scope_ids jsonb := '[]'::jsonb;
  v_repaired_existing_item_link_count integer := 0;
  v_synthetic_total_item_rows jsonb := '[]'::jsonb;
  v_pay_date date := NULL::date;
  v_vat_rate_pct numeric := NULL::numeric;
  v_workbench_session_id uuid := NULL::uuid;
  v_operation_type text := NULL::text;
  v_operation_status text := NULL::text;
  v_operation_phase text := NULL::text;
  v_operation_actor_user_id uuid := NULL::uuid;
  v_session_status text := NULL::text;
  v_session_discarded_at_utc timestamptz := NULL::timestamptz;
  v_session_replacement_session_id uuid := NULL::uuid;
  v_session_version bigint := NULL::bigint;
  v_session_source_snapshot_run_id uuid := NULL::uuid;
  v_batch_status text := NULL::text;
  v_batch_execution_commit_state text := NULL::text;
  v_batch_source_workbench_session_id uuid := NULL::uuid;
  v_batch_source_snapshot_run_id uuid := NULL::uuid;
  v_batch_source_session_version bigint := NULL::bigint;
  v_locked_candidate_scope_count integer := 0;
  v_snooze_guard_json jsonb := '{}'::jsonb;
  v_snooze_guard_selected_line_count integer := 0;
  v_active_snooze_count integer := 0;
  v_retention_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_retention_mark_json jsonb := '{}'::jsonb;
  v_locked_allocation_row_count integer := 0;
  -- All six visible finance aliases must first match the producer-owned case,
  -- component, source, amount and readiness identity.  Their materialisation
  -- disposition is deliberately not uniform.  MANUAL_DEBT_RECOVERY remains on
  -- the established INSERT_ITEMS path: for PAYE NET_DEDUCT it must exist as a
  -- frozen Draft item before the PAYE Worksheet net is supplied, after which
  -- pay_set_paye_net_manual/Sage applies the existing net-headroom policy.
  v_certified_finance_identity_aliases constant text[] := ARRAY[
    'OVERPAYMENT_RECOVERY',
    'MANUAL_DEBT_RECOVERY',
    'PAYMENT_ADVANCE_REPAYMENT',
    'LOAN_PAYOUT',
    'UNDERPAYMENT_PAYMENT',
    'MANUAL_CREDIT_ADJUSTMENT_PAYMENT'
  ]::text[];
  v_deferred_finance_aliases constant text[] := ARRAY[
    'OVERPAYMENT_RECOVERY',
    'PAYMENT_ADVANCE_REPAYMENT',
    'LOAN_PAYOUT',
    'UNDERPAYMENT_PAYMENT',
    'MANUAL_CREDIT_ADJUSTMENT_PAYMENT'
  ]::text[];
BEGIN
  PERFORM public.banking_pay_hot_path_budget_apply('WORKBENCH_CHUNK');

  IF p_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'pay_batch_id is required';
  END IF;

  IF p_operation_id IS NULL THEN
    RAISE EXCEPTION 'p_operation_id is required for row-backed draft item insertion';
  END IF;

  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'p_actor_user_id is required for row-backed draft item insertion';
  END IF;

  IF p_candidate_scope_ids IS NULL OR jsonb_typeof(v_scope_ids) <> 'array' OR jsonb_array_length(v_scope_ids) = 0 THEN
    RAISE EXCEPTION 'p_candidate_scope_ids must be a non-empty JSON array';
  END IF;

  v_scope_id_count := jsonb_array_length(v_scope_ids);

  IF v_scope_id_count > 100 THEN
    RAISE EXCEPTION 'p_candidate_scope_ids exceeds the 100 row cap: %', v_scope_id_count;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM jsonb_array_elements(v_scope_ids) AS supplied_scope(scope_value)
    WHERE NOT ((supplied_scope.scope_value #>> '{}') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
  ) THEN
    RAISE EXCEPTION 'p_candidate_scope_ids must contain UUID strings only';
  END IF;

  SELECT COALESCE(jsonb_agg(candidate_scope_source.candidate_scope_id_text ORDER BY candidate_scope_source.candidate_scope_id_text), '[]'::jsonb)
  INTO v_candidate_scope_ids
  FROM (
    SELECT DISTINCT BTRIM(supplied_scope.scope_value #>> '{}') AS candidate_scope_id_text
    FROM jsonb_array_elements(v_scope_ids) AS supplied_scope(scope_value)
  ) AS candidate_scope_source;

  IF jsonb_array_length(v_candidate_scope_ids) IS DISTINCT FROM v_scope_id_count THEN
    RAISE EXCEPTION 'DRAFT_ITEM_CANDIDATE_SCOPE_DUPLICATE'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'DRAFT_ITEM_CANDIDATE_SCOPE_DUPLICATE',
              'operation_id', p_operation_id::text,
              'supplied_scope_count', v_scope_id_count,
              'distinct_scope_count', jsonb_array_length(v_candidate_scope_ids),
              'message', 'Candidate scope identifiers must be unique for draft item insertion.'
            )::text;
  END IF;

  PERFORM 1
  FROM public.tms_users AS actor_user
  WHERE actor_user.id = p_actor_user_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'DRAFT_ITEM_ACTOR_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'DRAFT_ITEM_ACTOR_NOT_FOUND',
              'actor_user_id', p_actor_user_id::text,
              'message', 'The draft operation actor could not be verified.'
            )::text;
  END IF;

  -- Establish and hold the operation/session authority before taking candidate,
  -- allocation or timesheet locks. A stale source operation is never rebound to
  -- a replacement session inside this independently callable child writer.
  SELECT UPPER(BTRIM(COALESCE(operation_row.operation_type, ''))),
         UPPER(BTRIM(COALESCE(operation_row.status, ''))),
         UPPER(BTRIM(COALESCE(operation_row.phase, ''))),
         operation_row.actor_user_id,
         operation_row.workbench_session_id
  INTO v_operation_type,
       v_operation_status,
       v_operation_phase,
       v_operation_actor_user_id,
       v_workbench_session_id
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.id = p_operation_id
  FOR UPDATE OF operation_row;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'DRAFT_ITEM_OPERATION_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'DRAFT_ITEM_OPERATION_NOT_FOUND',
              'operation_id', p_operation_id::text,
              'message', 'The DRAFT_CREATE operation could not be found.'
            )::text;
  END IF;

  IF v_operation_type <> 'DRAFT_CREATE'
     OR v_operation_status <> 'RUNNING'
     OR v_operation_phase <> 'INSERT_ITEMS'
     OR v_operation_actor_user_id IS DISTINCT FROM p_actor_user_id
     OR v_workbench_session_id IS NULL THEN
    RAISE EXCEPTION 'DRAFT_ITEM_OPERATION_AUTHORITY_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'DRAFT_ITEM_OPERATION_AUTHORITY_INVALID',
              'operation_id', p_operation_id::text,
              'operation_type', v_operation_type,
              'operation_status', v_operation_status,
              'operation_phase', v_operation_phase,
              'operation_actor_user_id', CASE
                WHEN v_operation_actor_user_id IS NULL THEN NULL
                ELSE v_operation_actor_user_id::text
              END,
              'supplied_actor_user_id', p_actor_user_id::text,
              'workbench_session_id', CASE
                WHEN v_workbench_session_id IS NULL THEN NULL
                ELSE v_workbench_session_id::text
              END,
              'message', 'The operation is not the active INSERT_ITEMS DRAFT_CREATE operation for this actor and Workbench session.'
            )::text;
  END IF;

  SELECT UPPER(BTRIM(COALESCE(session_row.status, ''))),
         session_row.discarded_at_utc,
         session_row.replacement_session_id,
         session_row.version,
         session_row.source_snapshot_run_id
  INTO v_session_status,
       v_session_discarded_at_utc,
       v_session_replacement_session_id,
       v_session_version,
       v_session_source_snapshot_run_id
  FROM public.banking_pay_workbench_sessions AS session_row
  WHERE session_row.id = v_workbench_session_id
  FOR UPDATE OF session_row;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'DRAFT_ITEM_WORKBENCH_SESSION_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'DRAFT_ITEM_WORKBENCH_SESSION_NOT_FOUND',
              'operation_id', p_operation_id::text,
              'workbench_session_id', v_workbench_session_id::text,
              'message', 'The operation Workbench session could not be found.'
            )::text;
  END IF;

  IF v_session_status <> 'OPEN'
     OR v_session_discarded_at_utc IS NOT NULL
     OR v_session_replacement_session_id IS NOT NULL THEN
    RAISE EXCEPTION 'DRAFT_ITEM_WORKBENCH_SESSION_STALE'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'DRAFT_ITEM_WORKBENCH_SESSION_STALE',
              'operation_id', p_operation_id::text,
              'workbench_session_id', v_workbench_session_id::text,
              'session_status', v_session_status,
              'discarded_at_utc', CASE
                WHEN v_session_discarded_at_utc IS NULL THEN NULL
                ELSE v_session_discarded_at_utc::text
              END,
              'replacement_session_id', CASE
                WHEN v_session_replacement_session_id IS NULL THEN NULL
                ELSE v_session_replacement_session_id::text
              END,
              'refresh_required', true,
              'next_action', 'REFRESH_WORKBENCH',
              'message', 'The draft operation belongs to a stale or replaced Workbench session. Refresh Banking Pay before creating items.'
            )::text;
  END IF;

  SELECT COUNT(*)::integer
  INTO v_locked_candidate_scope_count
  FROM (
    SELECT scope_row.id
    FROM public.banking_pay_operation_candidate_scope AS scope_row
    WHERE scope_row.id IN (
      SELECT supplied_scope.candidate_scope_id_text::uuid
      FROM jsonb_array_elements_text(v_candidate_scope_ids)
        AS supplied_scope(candidate_scope_id_text)
    )
    ORDER BY scope_row.id
    FOR UPDATE OF scope_row
  ) AS locked_candidate_scopes;

  IF v_locked_candidate_scope_count IS DISTINCT FROM v_scope_id_count THEN
    RAISE EXCEPTION 'DRAFT_ITEM_CANDIDATE_SCOPE_TARGET_SET_CHANGED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'DRAFT_ITEM_CANDIDATE_SCOPE_TARGET_SET_CHANGED',
              'operation_id', p_operation_id::text,
              'expected_scope_count', v_scope_id_count,
              'locked_scope_count', v_locked_candidate_scope_count,
              'message', 'One or more candidate scopes no longer exist. Refresh Banking Pay before creating items.'
            )::text;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM public.banking_pay_operation_candidate_scope AS scope_row
    WHERE scope_row.id IN (
      SELECT supplied_scope.candidate_scope_id_text::uuid
      FROM jsonb_array_elements_text(v_candidate_scope_ids)
        AS supplied_scope(candidate_scope_id_text)
    )
      AND (
        scope_row.operation_id IS DISTINCT FROM p_operation_id
        OR scope_row.workbench_session_id IS DISTINCT FROM v_workbench_session_id
        OR scope_row.pay_batch_id IS DISTINCT FROM p_pay_batch_id
        OR UPPER(BTRIM(COALESCE(scope_row.status, ''))) NOT IN ('SCOPED', 'ALLOCATED', 'DRAFTED')
        OR scope_row.source_session_version IS DISTINCT FROM v_session_version
        OR scope_row.source_snapshot_run_id IS DISTINCT FROM v_session_source_snapshot_run_id
      )
  ) THEN
    RAISE EXCEPTION 'DRAFT_ITEM_CANDIDATE_SCOPE_AUTHORITY_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'DRAFT_ITEM_CANDIDATE_SCOPE_AUTHORITY_INVALID',
              'operation_id', p_operation_id::text,
              'workbench_session_id', v_workbench_session_id::text,
              'pay_batch_id', p_pay_batch_id::text,
              'candidate_scope_ids', v_candidate_scope_ids,
              'refresh_required', true,
              'next_action', 'REFRESH_WORKBENCH',
              'message', 'One or more candidate scopes no longer belong to the active operation, Workbench session or draft batch.'
            )::text;
  END IF;

  SELECT batch_row.pay_date,
         UPPER(BTRIM(COALESCE(batch_row.status, ''))),
         UPPER(BTRIM(COALESCE(batch_row.execution_commit_state, ''))),
         batch_row.source_workbench_session_id,
         batch_row.source_snapshot_run_id,
         batch_row.source_session_version
  INTO v_pay_date,
       v_batch_status,
       v_batch_execution_commit_state,
       v_batch_source_workbench_session_id,
       v_batch_source_snapshot_run_id,
       v_batch_source_session_version
  FROM public.pay_batches AS batch_row
  WHERE batch_row.id = p_pay_batch_id
  FOR UPDATE OF batch_row;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'DRAFT_ITEM_PAY_BATCH_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'DRAFT_ITEM_PAY_BATCH_NOT_FOUND',
              'pay_batch_id', p_pay_batch_id::text,
              'message', 'The draft pay batch could not be found.'
            )::text;
  END IF;

  IF v_pay_date IS NULL THEN
    RAISE EXCEPTION 'DRAFT_ITEM_PAY_BATCH_PAY_DATE_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'DRAFT_ITEM_PAY_BATCH_PAY_DATE_REQUIRED',
              'pay_batch_id', p_pay_batch_id::text,
              'message', 'The draft pay batch has no pay date.'
            )::text;
  END IF;

  IF v_batch_status <> 'DRAFT'
     OR v_batch_execution_commit_state <> 'NOT_SUBMITTED'
     OR v_batch_source_workbench_session_id IS DISTINCT FROM v_workbench_session_id
     OR v_batch_source_snapshot_run_id IS DISTINCT FROM v_session_source_snapshot_run_id
     OR v_batch_source_session_version IS DISTINCT FROM v_session_version THEN
    RAISE EXCEPTION 'DRAFT_ITEM_PAY_BATCH_AUTHORITY_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'DRAFT_ITEM_PAY_BATCH_AUTHORITY_INVALID',
              'pay_batch_id', p_pay_batch_id::text,
              'batch_status', v_batch_status,
              'execution_commit_state', v_batch_execution_commit_state,
              'batch_source_workbench_session_id', CASE
                WHEN v_batch_source_workbench_session_id IS NULL THEN NULL
                ELSE v_batch_source_workbench_session_id::text
              END,
              'operation_workbench_session_id', v_workbench_session_id::text,
              'batch_source_session_version', v_batch_source_session_version,
              'operation_session_version', v_session_version,
              'refresh_required', true,
              'next_action', 'REFRESH_WORKBENCH',
              'message', 'The draft batch no longer belongs to the active operation Workbench authority.'
            )::text;
  END IF;

  v_snooze_guard_json := private.pay_workbench_operation_active_snoozes_v8(
    p_operation_id,
    v_workbench_session_id,
    v_candidate_scope_ids
  );

  IF LOWER(BTRIM(COALESCE(v_snooze_guard_json->>'ok', 'false'))) NOT IN ('true', 't', '1') THEN
    RAISE EXCEPTION 'DRAFT_ITEM_SNOOZE_GUARD_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'DRAFT_ITEM_SNOOZE_GUARD_INVALID',
              'operation_id', p_operation_id::text,
              'workbench_session_id', v_workbench_session_id::text,
              'message', 'The live snooze authority returned an invalid result.'
            )::text;
  END IF;

  v_snooze_guard_selected_line_count := CASE
    WHEN COALESCE(v_snooze_guard_json->>'selected_line_count', '') ~ '^[0-9]{1,9}$'
      THEN (v_snooze_guard_json->>'selected_line_count')::integer
    ELSE 0
  END;
  v_active_snooze_count := CASE
    WHEN COALESCE(v_snooze_guard_json->>'active_snooze_count', '') ~ '^[0-9]{1,9}$'
      THEN (v_snooze_guard_json->>'active_snooze_count')::integer
    ELSE 0
  END;

  IF v_active_snooze_count > 0 THEN
    RAISE EXCEPTION 'ACTIVE_SNOOZE_PREVENTS_DRAFT'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_strip_nulls(
              jsonb_build_object(
                'code', 'ACTIVE_SNOOZE_PREVENTS_DRAFT',
                'message', 'One or more selected payments are currently snoozed. Banking Pay has been refreshed so you can review the updated selection.',
                'operation_id', p_operation_id::text,
                'workbench_session_id', v_workbench_session_id::text,
                'pay_batch_id', p_pay_batch_id::text,
                'refresh_required', true,
                'next_action', 'REFRESH_WORKBENCH'
              )
              || COALESCE(v_snooze_guard_json, '{}'::jsonb)
            )::text;
  END IF;

  SELECT finance_window.vat_rate_pct
  INTO v_vat_rate_pct
  FROM public.settings_finance_windows AS finance_window
  WHERE v_pay_date >= finance_window.date_from
    AND v_pay_date <= COALESCE(finance_window.date_to, 'infinity'::date)
  ORDER BY finance_window.date_from DESC, finance_window.id DESC
  LIMIT 1;

  IF v_vat_rate_pct IS NULL THEN
    RAISE EXCEPTION 'VAT rate not found for pay_batch % pay_date %', p_pay_batch_id, v_pay_date;
  END IF;

  SELECT COALESCE(jsonb_agg(jsonb_build_object(
           'allocation_row_id', allocation_row.id::text,
           'candidate_scope_id', allocation_row.candidate_scope_id::text,
           'source_ref', allocation_row.source_ref,
           'timesheet_id', allocation_identity.timesheet_id_text,
           'key_type', allocation_identity.key_type,
           'key_value', allocation_identity.key_value,
           'allocated_amount', allocation_row.allocated_amount,
           'reason', 'RESOLVED_SYNTHETIC_TOTAL_ROW_ITEM_BLOCKED'
         ) ORDER BY allocation_row.sort_order, allocation_row.id), '[]'::jsonb)
  INTO v_synthetic_total_item_rows
  FROM public.banking_pay_operation_candidate_allocation_rows AS allocation_row
  CROSS JOIN LATERAL (
    SELECT
      NULLIF(BTRIM(COALESCE(
        allocation_row.allocation_basis_json#>>'{economic_key,timesheet_id}',
        allocation_row.allocation_basis_json->>'timesheet_id',
        allocation_row.allocation_basis_json#>>'{line,timesheet_id}',
        allocation_row.allocation_basis_json#>>'{line,real_business_timesheet_id}',
        ''
      )), '') AS timesheet_id_text,
      UPPER(NULLIF(BTRIM(COALESCE(
        allocation_row.allocation_basis_json#>>'{economic_key,key_type}',
        allocation_row.allocation_basis_json->>'key_type',
        allocation_row.allocation_basis_json#>>'{line,economic_key,key_type}',
        allocation_row.allocation_basis_json#>>'{line,key_type}',
        allocation_row.allocation_basis_json#>>'{line,component_key_type}',
        ''
      )), '')) AS key_type,
      NULLIF(BTRIM(COALESCE(
        allocation_row.allocation_basis_json#>>'{economic_key,key_value}',
        allocation_row.allocation_basis_json->>'key_value',
        allocation_row.allocation_basis_json#>>'{line,economic_key,key_value}',
        allocation_row.allocation_basis_json#>>'{line,key_value}',
        allocation_row.allocation_basis_json#>>'{line,component_key_value}',
        ''
      )), '') AS key_value,
      LOWER(BTRIM(COALESCE(
        allocation_row.source_ref,
        allocation_row.allocation_basis_json#>>'{line,row_key}',
        allocation_row.allocation_basis_json#>>'{line,line_key}',
        allocation_row.allocation_basis_json#>>'{line,source_ref}',
        allocation_row.allocation_basis_json->>'row_key',
        allocation_row.allocation_basis_json->>'line_key',
        allocation_row.allocation_basis_json->>'source_ref',
        ''
      ))) AS identity_text
  ) AS allocation_identity
  WHERE allocation_row.operation_id = p_operation_id
    AND allocation_row.candidate_scope_id IN (
      SELECT supplied_scope.candidate_scope_id_text::uuid
      FROM jsonb_array_elements_text(v_candidate_scope_ids) AS supplied_scope(candidate_scope_id_text)
    )
    AND UPPER(BTRIM(COALESCE(allocation_row.status, ''))) NOT IN ('FAILED', 'ERROR', 'CANCELLED', 'CANCELED', 'SKIPPED', 'VOIDED')
    AND allocation_identity.key_type = 'TS_TOTAL'
    AND UPPER(BTRIM(COALESCE(allocation_identity.key_value, ''))) = 'TOTAL'
    AND allocation_identity.identity_text LIKE '%:non_segment:total%'
    AND (
      lower(btrim(coalesce(allocation_row.allocation_basis_json->>'resolved_segment_rows_replace_source_total', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
      OR lower(btrim(coalesce(allocation_row.allocation_basis_json#>>'{line,resolved_segment_rows_replace_source_total}', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
      OR lower(btrim(coalesce(allocation_row.allocation_basis_json->>'has_resolved_rate', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
      OR lower(btrim(coalesce(allocation_row.allocation_basis_json#>>'{line,has_resolved_rate}', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
      OR lower(btrim(coalesce(allocation_row.allocation_basis_json#>>'{line,case_resolution_summary,has_resolved_rate}', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
      OR lower(btrim(coalesce(allocation_row.allocation_basis_json#>>'{line,case_resolution_summary,resolved_rate_applied}', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
      OR lower(btrim(coalesce(allocation_row.allocation_basis_json#>>'{line,case_resolution_summary,resolved_rate_active}', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
      OR (
        COALESCE(allocation_row.allocation_basis_json#>>'{line,case_resolution_summary,resolved_rate_component_count}', '') ~ '^[0-9]+$'
        AND (allocation_row.allocation_basis_json#>>'{line,case_resolution_summary,resolved_rate_component_count}')::integer > 0
      )
      OR EXISTS (
        SELECT 1
        FROM public.banking_pay_operation_candidate_allocation_rows AS sibling_allocation_row
        CROSS JOIN LATERAL (
          SELECT
            NULLIF(BTRIM(COALESCE(
              sibling_allocation_row.allocation_basis_json#>>'{economic_key,timesheet_id}',
              sibling_allocation_row.allocation_basis_json->>'timesheet_id',
              sibling_allocation_row.allocation_basis_json#>>'{line,timesheet_id}',
              sibling_allocation_row.allocation_basis_json#>>'{line,real_business_timesheet_id}',
              ''
            )), '') AS timesheet_id_text,
            UPPER(NULLIF(BTRIM(COALESCE(
              sibling_allocation_row.allocation_basis_json#>>'{economic_key,key_type}',
              sibling_allocation_row.allocation_basis_json->>'key_type',
              sibling_allocation_row.allocation_basis_json#>>'{line,economic_key,key_type}',
              sibling_allocation_row.allocation_basis_json#>>'{line,key_type}',
              sibling_allocation_row.allocation_basis_json#>>'{line,component_key_type}',
              ''
            )), '')) AS key_type,
            LOWER(BTRIM(COALESCE(
              sibling_allocation_row.source_ref,
              sibling_allocation_row.allocation_basis_json#>>'{line,row_key}',
              sibling_allocation_row.allocation_basis_json#>>'{line,line_key}',
              sibling_allocation_row.allocation_basis_json#>>'{line,source_ref}',
              sibling_allocation_row.allocation_basis_json->>'row_key',
              sibling_allocation_row.allocation_basis_json->>'line_key',
              sibling_allocation_row.allocation_basis_json->>'source_ref',
              ''
            ))) AS identity_text
        ) AS sibling_identity
        WHERE sibling_allocation_row.operation_id = allocation_row.operation_id
          AND sibling_allocation_row.candidate_scope_id = allocation_row.candidate_scope_id
          AND sibling_allocation_row.id IS DISTINCT FROM allocation_row.id
          AND UPPER(BTRIM(COALESCE(sibling_allocation_row.status, ''))) NOT IN ('FAILED', 'ERROR', 'CANCELLED', 'CANCELED', 'SKIPPED', 'VOIDED')
          AND sibling_identity.key_type = 'TS_DAY'
          AND sibling_identity.identity_text LIKE '%:segment:%'
          AND sibling_identity.timesheet_id_text IS NOT DISTINCT FROM allocation_identity.timesheet_id_text
      )
    );

  IF jsonb_array_length(COALESCE(v_synthetic_total_item_rows, '[]'::jsonb)) > 0 THEN
    RAISE EXCEPTION 'RESOLVED_SYNTHETIC_TOTAL_ROW_ITEM_BLOCKED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'RESOLVED_SYNTHETIC_TOTAL_ROW_ITEM_BLOCKED',
              'operation_id', p_operation_id::text,
              'pay_batch_id', p_pay_batch_id::text,
              'synthetic_total_item_rows', COALESCE(v_synthetic_total_item_rows, '[]'::jsonb),
              'message', 'A stale resolved-timesheet synthetic total row reached item creation. Refresh Banking Pay and try Create Draft again.'
            )::text;
  END IF;

  DROP TABLE IF EXISTS pg_temp.tmp_pay_batch_insert_items_certified_finance;
  CREATE TEMPORARY TABLE pg_temp.tmp_pay_batch_insert_items_certified_finance ON COMMIT DROP AS
  SELECT
    allocation_row.id AS allocation_row_id,
    allocation_row.pay_batch_item_id,
    allocation_row.candidate_id,
    UPPER(BTRIM(COALESCE(allocation_row.pay_channel, ''))) AS pay_channel,
    UPPER(BTRIM(COALESCE(allocation_row.allocation_type, ''))) AS visible_alias,
    CASE UPPER(BTRIM(COALESCE(allocation_row.allocation_type, '')))
      WHEN 'OVERPAYMENT_RECOVERY' THEN 'OVERPAYMENT_RECOVERY'
      WHEN 'MANUAL_DEBT_RECOVERY' THEN 'MANUAL_DEBT_RECOVERY'
      WHEN 'PAYMENT_ADVANCE_REPAYMENT' THEN 'LOAN_REPAYMENT'
      WHEN 'LOAN_PAYOUT' THEN 'LOAN_PAYOUT'
      WHEN 'UNDERPAYMENT_PAYMENT' THEN 'UNDERPAYMENT_PAYMENT'
      WHEN 'MANUAL_CREDIT_ADJUSTMENT_PAYMENT' THEN 'MANUAL_CREDIT_PAYOUT'
      ELSE NULL::text
    END AS expected_item_type,
    allocation_row.finance_case_id,
    allocation_row.finance_component_id,
    allocation_row.source_ref,
    allocation_row.operation_source_key,
    ROUND(COALESCE(allocation_row.allocated_amount, 0), 2)::numeric(12,2) AS allocated_amount,
    (
      allocation_row.finance_case_id IS NOT NULL
      AND allocation_row.finance_component_id IS NOT NULL
      AND NULLIF(BTRIM(COALESCE(allocation_row.operation_source_key, '')), '') IS NOT NULL
      AND allocation_row.source_ref = 'advance:' || allocation_row.finance_case_id::text
      AND jsonb_typeof(allocation_row.allocation_basis_json->'line') = 'object'
      AND jsonb_typeof(allocation_row.allocation_basis_json->'finance_component') = 'object'
      AND UPPER(BTRIM(COALESCE(allocation_row.allocation_basis_json#>>'{line,line_type}', ''))) = UPPER(BTRIM(COALESCE(allocation_row.allocation_type, '')))
      AND NULLIF(BTRIM(COALESCE(allocation_row.allocation_basis_json#>>'{line,finance_case_id}', '')), '') = allocation_row.finance_case_id::text
      AND NULLIF(BTRIM(COALESCE(allocation_row.allocation_basis_json#>>'{line,finance_component_id}', '')), '') = allocation_row.finance_component_id::text
      AND NULLIF(BTRIM(COALESCE(allocation_row.allocation_basis_json#>>'{finance_component,finance_component_id}', '')), '') = allocation_row.finance_component_id::text
      AND NULLIF(BTRIM(COALESCE(allocation_row.allocation_basis_json#>>'{line,source_ref}', '')), '') = allocation_row.source_ref
      AND LOWER(NULLIF(BTRIM(COALESCE(allocation_row.allocation_basis_json#>>'{line,candidate_id}', '')), '')) = allocation_row.candidate_id::text
      AND UPPER(NULLIF(BTRIM(COALESCE(allocation_row.allocation_basis_json#>>'{line,pay_channel}', '')), '')) = UPPER(BTRIM(COALESCE(allocation_row.pay_channel, '')))
      AND UPPER(BTRIM(COALESCE(allocation_row.allocation_basis_json#>>'{line,presentation_section}', ''))) = 'READY_TO_PAY'
      AND LOWER(BTRIM(COALESCE(allocation_row.allocation_basis_json#>>'{line,draftable}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
      AND LOWER(BTRIM(COALESCE(allocation_row.allocation_basis_json#>>'{line,is_ready_for_draft}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
      AND LOWER(BTRIM(COALESCE(allocation_row.allocation_basis_json#>>'{line,selection_allowed}', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
      AND ROUND(COALESCE(allocation_row.allocated_amount, 0), 2) <> 0
      AND COALESCE(allocation_row.allocation_basis_json#>>'{line,amount_ex_vat}', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
      AND ROUND((allocation_row.allocation_basis_json#>>'{line,amount_ex_vat}')::numeric, 2) = ROUND(COALESCE(allocation_row.allocated_amount, 0), 2)
      AND EXISTS (
        SELECT 1
        FROM jsonb_array_elements(
          CASE
            WHEN jsonb_typeof(allocation_row.allocation_basis_json#>'{line,case_components}') = 'array'
              THEN allocation_row.allocation_basis_json#>'{line,case_components}'
            ELSE '[]'::jsonb
          END
        ) AS source_component(component_json)
        WHERE NULLIF(BTRIM(COALESCE(source_component.component_json->>'finance_component_id', '')), '') = allocation_row.finance_component_id::text
      )
    ) AS certification_ok
  FROM public.banking_pay_operation_candidate_allocation_rows AS allocation_row
  WHERE allocation_row.operation_id = p_operation_id
    AND allocation_row.candidate_scope_id IN (
      SELECT supplied_scope.candidate_scope_id_text::uuid
      FROM jsonb_array_elements_text(v_candidate_scope_ids) AS supplied_scope(candidate_scope_id_text)
    )
    AND UPPER(BTRIM(COALESCE(allocation_row.allocation_type, ''))) = ANY(v_certified_finance_identity_aliases)
    AND UPPER(BTRIM(COALESCE(allocation_row.status, ''))) NOT IN ('FAILED', 'ERROR', 'CANCELLED', 'CANCELED', 'SKIPPED', 'VOIDED');

  PERFORM 1
  FROM pg_temp.tmp_pay_batch_insert_items_certified_finance AS finance_row
  WHERE finance_row.certification_ok IS NOT TRUE
  LIMIT 1;

  IF FOUND THEN
    RAISE EXCEPTION 'DRAFT_FINANCE_CONSTITUENT_HANDOFF_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'DRAFT_FINANCE_CONSTITUENT_HANDOFF_INVALID',
              'operation_id', p_operation_id::text,
              'pay_batch_id', p_pay_batch_id::text,
              'message', 'A certified finance constituent did not match its producer-owned case, component, source, amount or readiness identity.'
            )::text;
  END IF;

  PERFORM 1
  FROM pg_temp.tmp_pay_batch_insert_items_certified_finance AS finance_row
  WHERE (
      finance_row.pay_batch_item_id IS NOT NULL
      OR EXISTS (
        SELECT 1
        FROM public.pay_batch_candidates AS collision_candidate
        JOIN public.pay_batch_items AS collision_item
          ON collision_item.pay_batch_candidate_id = collision_candidate.id
         AND collision_item.operation_source_key = finance_row.operation_source_key
         AND COALESCE(collision_item.is_voided, false) = false
        WHERE collision_candidate.pay_batch_id = p_pay_batch_id
          AND collision_candidate.candidate_id = finance_row.candidate_id
      )
    )
    AND NOT EXISTS (
      SELECT 1
      FROM public.pay_batch_candidates AS exact_candidate
      JOIN public.pay_batch_items AS exact_item
        ON exact_item.pay_batch_candidate_id = exact_candidate.id
       AND COALESCE(exact_item.is_voided, false) = false
      WHERE exact_candidate.pay_batch_id = p_pay_batch_id
        AND exact_candidate.candidate_id = finance_row.candidate_id
        AND (exact_item.id = finance_row.pay_batch_item_id OR exact_item.operation_source_key = finance_row.operation_source_key)
        AND exact_item.operation_source_key = finance_row.operation_source_key
        AND exact_item.item_type = finance_row.expected_item_type
        AND exact_item.finance_case_id IS NOT DISTINCT FROM finance_row.finance_case_id
        AND exact_item.finance_component_id IS NOT DISTINCT FROM finance_row.finance_component_id
        AND exact_item.source_ref IS NOT DISTINCT FROM finance_row.source_ref
        AND UPPER(BTRIM(COALESCE(exact_item.pay_channel::text, ''))) = finance_row.pay_channel
        AND ROUND(COALESCE(exact_item.amount_ex_vat, 0), 2) = finance_row.allocated_amount
    )
  LIMIT 1;

  IF FOUND THEN
    RAISE EXCEPTION 'DRAFT_FINANCE_PREEXISTING_ITEM_LINK_MISMATCH'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'DRAFT_FINANCE_PREEXISTING_ITEM_LINK_MISMATCH',
              'operation_id', p_operation_id::text,
              'pay_batch_id', p_pay_batch_id::text,
              'message', 'A certified finance allocation is linked to an item that does not match the existing finance owner identity.'
            )::text;
  END IF;

  WITH existing_linked_items AS (
    SELECT
      allocation_row.id AS allocation_row_id,
      existing_item.id AS pay_batch_item_id
    FROM public.banking_pay_operation_candidate_allocation_rows AS allocation_row
    JOIN public.pay_batch_candidates AS existing_batch_candidate
      ON existing_batch_candidate.pay_batch_id = p_pay_batch_id
     AND existing_batch_candidate.candidate_id = allocation_row.candidate_id
    JOIN public.pay_batch_items AS existing_item
      ON existing_item.pay_batch_candidate_id = existing_batch_candidate.id
     AND existing_item.operation_source_key = allocation_row.operation_source_key
     AND COALESCE(existing_item.is_voided, false) = false
    WHERE allocation_row.operation_id = p_operation_id
      AND allocation_row.candidate_scope_id IN (
        SELECT supplied_scope.candidate_scope_id_text::uuid
        FROM jsonb_array_elements_text(v_candidate_scope_ids) AS supplied_scope(candidate_scope_id_text)
      )
      AND allocation_row.operation_source_key IS NOT NULL
      AND (
        (
          UPPER(BTRIM(COALESCE(allocation_row.allocation_type, ''))) = ANY(v_certified_finance_identity_aliases)
          AND EXISTS (
            SELECT 1
            FROM pg_temp.tmp_pay_batch_insert_items_certified_finance AS finance_row
            WHERE finance_row.allocation_row_id = allocation_row.id
              AND existing_item.item_type = finance_row.expected_item_type
              AND existing_item.finance_case_id IS NOT DISTINCT FROM finance_row.finance_case_id
              AND existing_item.finance_component_id IS NOT DISTINCT FROM finance_row.finance_component_id
              AND existing_item.source_ref IS NOT DISTINCT FROM finance_row.source_ref
              AND UPPER(BTRIM(COALESCE(existing_item.pay_channel::text, ''))) = finance_row.pay_channel
              AND ROUND(COALESCE(existing_item.amount_ex_vat, 0), 2) = finance_row.allocated_amount
          )
        )
        OR (
          NOT (UPPER(BTRIM(COALESCE(allocation_row.allocation_type, ''))) = ANY(v_certified_finance_identity_aliases))
          AND existing_item.item_type <> 'OVERPAYMENT_RECOVERY'
        )
      )
      AND UPPER(BTRIM(COALESCE(allocation_row.status, ''))) NOT IN ('FAILED', 'ERROR', 'CANCELLED', 'CANCELED', 'SKIPPED', 'VOIDED')
  ), repaired_existing_links AS (
    UPDATE public.banking_pay_operation_candidate_allocation_rows AS allocation_update
    SET status = 'ITEM_CREATED',
        pay_batch_id = p_pay_batch_id,
        pay_batch_item_id = existing_linked_items.pay_batch_item_id,
        updated_at_utc = v_now
    FROM existing_linked_items
    WHERE allocation_update.id = existing_linked_items.allocation_row_id
      AND (
        allocation_update.pay_batch_id IS DISTINCT FROM p_pay_batch_id
        OR allocation_update.pay_batch_item_id IS DISTINCT FROM existing_linked_items.pay_batch_item_id
        OR UPPER(BTRIM(COALESCE(allocation_update.status, ''))) <> 'ITEM_CREATED'
      )
    RETURNING allocation_update.id
  )
  SELECT COUNT(*)::integer
  INTO v_repaired_existing_item_link_count
  FROM repaired_existing_links;

  SELECT COUNT(*)::integer,
         COUNT(*) FILTER (
           WHERE allocation_row.pay_batch_id = p_pay_batch_id
             AND allocation_row.pay_batch_item_id IS NOT NULL
             AND EXISTS (
               SELECT 1
               FROM public.pay_batch_items AS existing_item
               JOIN public.pay_batch_candidates AS existing_batch_candidate
                 ON existing_batch_candidate.id = existing_item.pay_batch_candidate_id
               WHERE existing_item.id = allocation_row.pay_batch_item_id
                 AND existing_batch_candidate.pay_batch_id = p_pay_batch_id
                 AND existing_batch_candidate.candidate_id = allocation_row.candidate_id
                 AND COALESCE(existing_item.is_voided, false) = false
                 AND (
                   (
                     UPPER(BTRIM(COALESCE(allocation_row.allocation_type, ''))) = ANY(v_certified_finance_identity_aliases)
                     AND EXISTS (
                       SELECT 1
                       FROM pg_temp.tmp_pay_batch_insert_items_certified_finance AS finance_row
                       WHERE finance_row.allocation_row_id = allocation_row.id
                         AND existing_item.operation_source_key = finance_row.operation_source_key
                         AND existing_item.item_type = finance_row.expected_item_type
                         AND existing_item.finance_case_id IS NOT DISTINCT FROM finance_row.finance_case_id
                         AND existing_item.finance_component_id IS NOT DISTINCT FROM finance_row.finance_component_id
                         AND existing_item.source_ref IS NOT DISTINCT FROM finance_row.source_ref
                         AND UPPER(BTRIM(COALESCE(existing_item.pay_channel::text, ''))) = finance_row.pay_channel
                         AND ROUND(COALESCE(existing_item.amount_ex_vat, 0), 2) = finance_row.allocated_amount
                     )
                   )
                   OR (
                     NOT (UPPER(BTRIM(COALESCE(allocation_row.allocation_type, ''))) = ANY(v_certified_finance_identity_aliases))
                     AND existing_item.item_type <> 'OVERPAYMENT_RECOVERY'
                   )
                 )
             )
         )::integer
  INTO v_expected_allocation_row_count, v_linked_allocation_row_count
  FROM public.banking_pay_operation_candidate_allocation_rows AS allocation_row
  WHERE allocation_row.operation_id = p_operation_id
    AND allocation_row.candidate_scope_id IN (
      SELECT supplied_scope.candidate_scope_id_text::uuid
      FROM jsonb_array_elements_text(v_candidate_scope_ids) AS supplied_scope(candidate_scope_id_text)
    )
    AND UPPER(BTRIM(COALESCE(allocation_row.status, ''))) NOT IN ('FAILED', 'ERROR', 'CANCELLED', 'CANCELED', 'SKIPPED', 'VOIDED');

  SELECT COUNT(*)::integer
  INTO v_scope_expects_item_count
  FROM public.banking_pay_operation_candidate_scope AS scope_row
  WHERE scope_row.operation_id = p_operation_id
    AND scope_row.id IN (
      SELECT supplied_scope.candidate_scope_id_text::uuid
      FROM jsonb_array_elements_text(v_candidate_scope_ids) AS supplied_scope(candidate_scope_id_text)
    )
    AND lower(coalesce(scope_row.allocation_basis_json #>> '{reservation_availability,all_selected_rows_unavailable}', 'false')) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
    AND (
      CASE
        WHEN COALESCE(scope_row.candidate_totals_json->>'selected_row_count_seeded_in_page', '') ~ '^[0-9]+$'
          THEN (scope_row.candidate_totals_json->>'selected_row_count_seeded_in_page')::integer
        WHEN COALESCE(scope_row.candidate_totals_json->>'selected_preview_row_count', '') ~ '^[0-9]+$'
          THEN (scope_row.candidate_totals_json->>'selected_preview_row_count')::integer
        WHEN COALESCE(scope_row.candidate_totals_json->>'selected_row_count', '') ~ '^[0-9]+$'
          THEN (scope_row.candidate_totals_json->>'selected_row_count')::integer
        ELSE 0
      END > 0
      OR (
        jsonb_typeof(scope_row.selected_canonical_preview_lines_json) = 'array'
        AND jsonb_array_length(scope_row.selected_canonical_preview_lines_json) > 0
      )
      OR (
        jsonb_typeof(scope_row.effective_canonical_preview_lines_json) = 'array'
        AND jsonb_array_length(scope_row.effective_canonical_preview_lines_json) > 0
      )
      OR EXISTS (
        SELECT 1
        FROM private.banking_pay_draft_frozen_candidate_scopes_v8 AS frozen_scope
        JOIN private.banking_pay_draft_frozen_certificate_scopes_v8 AS certificate_scope
          ON certificate_scope.operation_id = frozen_scope.operation_id
         AND certificate_scope.freeze_state = 'FROZEN'
        WHERE frozen_scope.operation_id = scope_row.operation_id
          AND frozen_scope.candidate_id = scope_row.candidate_id
          AND frozen_scope.resolved_pay_channel = scope_row.pay_channel
          AND frozen_scope.scope_digest_sha256 = scope_row.scope_hash
          AND frozen_scope.scope_state IN ('FROZEN', 'BATCH_LINKED', 'COMPLETE')
      )
    );

  DROP TABLE IF EXISTS pg_temp.tmp_pay_batch_item_allocation_page;
  CREATE TEMPORARY TABLE pg_temp.tmp_pay_batch_item_allocation_page ON COMMIT DROP AS
  WITH allocation_page_raw AS (
    SELECT
      allocation_row.id,
      allocation_row.operation_id,
      allocation_row.candidate_scope_id,
      allocation_row.pay_batch_id,
      allocation_row.candidate_id,
      allocation_row.pay_channel,
      allocation_row.finance_case_id,
      allocation_row.finance_component_id,
      allocation_row.allocation_type,
      allocation_row.source_ref,
      allocation_row.operation_source_key,
      allocation_row.allocated_amount,
      allocation_row.allocation_basis_json,
      allocation_row.sort_order,
      allocation_row.status
    FROM public.banking_pay_operation_candidate_allocation_rows AS allocation_row
    WHERE allocation_row.operation_id = p_operation_id
      AND allocation_row.candidate_scope_id IN (
        SELECT (supplied_scope.scope_value #>> '{}')::uuid
        FROM jsonb_array_elements(v_scope_ids) AS supplied_scope(scope_value)
      )
      AND UPPER(BTRIM(COALESCE(allocation_row.status, ''))) IN ('PENDING', 'ITEM_PENDING')
    ORDER BY
      CASE
        WHEN UPPER(BTRIM(COALESCE(allocation_row.allocation_type, ''))) = ANY(v_deferred_finance_aliases) THEN 1
        ELSE 0
      END,
      allocation_row.candidate_scope_id,
      allocation_row.sort_order,
      allocation_row.id
    LIMIT 100
  ), allocation_page_keys AS (
    SELECT
      allocation_page_raw.*,
      CASE
        WHEN NULLIF(BTRIM(COALESCE(allocation_page_raw.allocation_basis_json#>>'{economic_key,timesheet_id}', allocation_page_raw.allocation_basis_json->>'timesheet_id', '')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN NULLIF(BTRIM(COALESCE(allocation_page_raw.allocation_basis_json#>>'{economic_key,timesheet_id}', allocation_page_raw.allocation_basis_json->>'timesheet_id', '')), '')::uuid
        ELSE NULL::uuid
      END AS economic_key_timesheet_id,
      CASE
        WHEN NULLIF(BTRIM(COALESCE(allocation_page_raw.allocation_basis_json#>>'{line,timesheet_id}', allocation_page_raw.allocation_basis_json#>>'{line,real_business_timesheet_id}', '')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN NULLIF(BTRIM(COALESCE(allocation_page_raw.allocation_basis_json#>>'{line,timesheet_id}', allocation_page_raw.allocation_basis_json#>>'{line,real_business_timesheet_id}', '')), '')::uuid
        ELSE NULL::uuid
      END AS line_timesheet_id,
      UPPER(NULLIF(BTRIM(COALESCE(allocation_page_raw.allocation_basis_json#>>'{economic_key,key_type}', allocation_page_raw.allocation_basis_json->>'key_type', '')), '')) AS allocation_key_type,
      NULLIF(BTRIM(COALESCE(allocation_page_raw.allocation_basis_json#>>'{economic_key,key_value}', allocation_page_raw.allocation_basis_json->>'key_value', '')), '') AS allocation_key_value
    FROM allocation_page_raw
  ), allocation_timesheet_refs AS (
    SELECT allocation_page_keys.economic_key_timesheet_id AS timesheet_id
    FROM allocation_page_keys
    WHERE allocation_page_keys.economic_key_timesheet_id IS NOT NULL
    UNION
    SELECT allocation_page_keys.line_timesheet_id AS timesheet_id
    FROM allocation_page_keys
    WHERE allocation_page_keys.line_timesheet_id IS NOT NULL
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
      (rotation_scope.family_timesheet_id IS NOT NULL AND rotation_scope.family_is_current IS NOT NULL) AS rotation_scope_resolved,
      COALESCE(rotation_scope.requested_is_canonical, false) AS requested_is_canonical
    FROM allocation_timesheet_id_array
    JOIN public._pay_timesheet_rotation_scope(allocation_timesheet_id_array.timesheet_ids) AS rotation_scope
      ON true
    ORDER BY
      rotation_scope.requested_timesheet_id,
      rotation_scope.family_is_current DESC NULLS LAST,
      rotation_scope.family_version DESC NULLS LAST,
      rotation_scope.family_timesheet_id
  ), allocation_page_scopes AS MATERIALIZED (
    SELECT DISTINCT
      allocation_page_keys.operation_id,
      allocation_page_keys.candidate_scope_id
    FROM allocation_page_keys
  ), allocation_page_scope_lines AS MATERIALIZED (
    SELECT
      scope_row.id AS candidate_scope_id,
      scope_line.line_json,
      scope_line.line_ordinal
    FROM allocation_page_scopes AS page_scope
    JOIN public.banking_pay_operation_candidate_scope AS scope_row
      ON scope_row.id = page_scope.candidate_scope_id
     AND scope_row.operation_id = page_scope.operation_id
    CROSS JOIN LATERAL private.pay_workbench_draft_scope_line_rows_v8(
      scope_row.id,
      scope_row.selected_canonical_preview_lines_json,
      scope_row.effective_canonical_preview_lines_json
    ) AS scope_line(line_json, line_ordinal)
  ), scope_canonical_line_matches AS (
    SELECT DISTINCT
      allocation_page_keys.id AS allocation_row_id,
      COALESCE(economic_key_rotation.canonical_timesheet_id, line_rotation.canonical_timesheet_id) AS canonical_timesheet_id
    FROM allocation_page_keys
    JOIN allocation_page_scope_lines AS scope_line
      ON scope_line.candidate_scope_id = allocation_page_keys.candidate_scope_id
    LEFT JOIN allocation_rotation_scope AS economic_key_rotation
      ON economic_key_rotation.requested_timesheet_id = allocation_page_keys.economic_key_timesheet_id
    LEFT JOIN allocation_rotation_scope AS line_rotation
      ON line_rotation.requested_timesheet_id = allocation_page_keys.line_timesheet_id
    WHERE COALESCE(economic_key_rotation.canonical_timesheet_id, line_rotation.canonical_timesheet_id) IS NOT NULL
      AND NULLIF(BTRIM(COALESCE(scope_line.line_json#>>'{economic_key,timesheet_id}', scope_line.line_json->>'timesheet_id', '')), '') = COALESCE(economic_key_rotation.canonical_timesheet_id, line_rotation.canonical_timesheet_id)::text
      AND UPPER(NULLIF(BTRIM(COALESCE(scope_line.line_json#>>'{economic_key,key_type}', scope_line.line_json->>'key_type', scope_line.line_json->>'component_key_type', '')), '')) IS NOT DISTINCT FROM allocation_page_keys.allocation_key_type
      AND NULLIF(BTRIM(COALESCE(scope_line.line_json#>>'{economic_key,key_value}', scope_line.line_json->>'key_value', scope_line.line_json->>'component_key_value', '')), '') IS NOT DISTINCT FROM allocation_page_keys.allocation_key_value
      AND NULLIF(BTRIM(COALESCE(scope_line.line_json->>'row_key', scope_line.line_json->>'source_ref', '')), '') IS NOT DISTINCT FROM NULLIF(BTRIM(COALESCE(allocation_page_keys.allocation_basis_json->>'row_key', allocation_page_keys.allocation_basis_json#>>'{line,row_key}', allocation_page_keys.source_ref, '')), '')
  )
  SELECT
    allocation_page_keys.id,
    allocation_page_keys.operation_id,
    allocation_page_keys.candidate_scope_id,
    allocation_page_keys.pay_batch_id,
    allocation_page_keys.candidate_id,
    allocation_page_keys.pay_channel,
    allocation_page_keys.finance_case_id,
    allocation_page_keys.finance_component_id,
    allocation_page_keys.allocation_type,
    allocation_page_keys.source_ref,
    allocation_page_keys.operation_source_key,
    allocation_page_keys.allocated_amount,
    allocation_page_keys.allocation_basis_json AS source_allocation_basis_json,
    jsonb_strip_nulls(
      allocation_page_keys.allocation_basis_json
      || CASE
        WHEN COALESCE(economic_key_rotation.canonical_timesheet_id, line_rotation.canonical_timesheet_id) IS NULL THEN '{}'::jsonb
        ELSE jsonb_build_object(
          'timesheet_id', COALESCE(economic_key_rotation.canonical_timesheet_id, line_rotation.canonical_timesheet_id)::text,
          'economic_key', COALESCE(allocation_page_keys.allocation_basis_json->'economic_key', '{}'::jsonb)
            || jsonb_build_object(
              'timesheet_id', COALESCE(economic_key_rotation.canonical_timesheet_id, line_rotation.canonical_timesheet_id)::text,
              'key_type', allocation_page_keys.allocation_key_type,
              'key_value', allocation_page_keys.allocation_key_value
            ),
          'line', COALESCE(allocation_page_keys.allocation_basis_json->'line', '{}'::jsonb)
            || jsonb_build_object(
              'timesheet_id', COALESCE(economic_key_rotation.canonical_timesheet_id, line_rotation.canonical_timesheet_id)::text,
              'real_business_timesheet_id', COALESCE(economic_key_rotation.canonical_timesheet_id, line_rotation.canonical_timesheet_id)::text,
              'economic_key', COALESCE(allocation_page_keys.allocation_basis_json#>'{line,economic_key}', '{}'::jsonb)
                || jsonb_build_object(
                  'timesheet_id', COALESCE(economic_key_rotation.canonical_timesheet_id, line_rotation.canonical_timesheet_id)::text,
                  'key_type', allocation_page_keys.allocation_key_type,
                  'key_value', allocation_page_keys.allocation_key_value
                )
            )
        )
      END
      || CASE
        WHEN allocation_page_keys.economic_key_timesheet_id IS NOT NULL
         AND allocation_page_keys.economic_key_timesheet_id IS DISTINCT FROM COALESCE(economic_key_rotation.canonical_timesheet_id, line_rotation.canonical_timesheet_id) THEN jsonb_build_object(
          'rotation_requested_timesheet_id', allocation_page_keys.economic_key_timesheet_id::text
        )
        ELSE '{}'::jsonb
      END
    ) AS allocation_basis_json,
    allocation_page_keys.sort_order,
    allocation_page_keys.status,
    allocation_page_keys.economic_key_timesheet_id,
    allocation_page_keys.line_timesheet_id,
    COALESCE(economic_key_rotation.canonical_timesheet_id, line_rotation.canonical_timesheet_id) AS canonical_timesheet_id,
    allocation_page_keys.allocation_key_type,
    allocation_page_keys.allocation_key_value,
    CASE
      WHEN allocation_page_keys.economic_key_timesheet_id IS NULL AND allocation_page_keys.line_timesheet_id IS NULL THEN true
      WHEN allocation_page_keys.economic_key_timesheet_id IS NOT NULL AND COALESCE(economic_key_rotation.rotation_scope_resolved, false) = false THEN false
      WHEN allocation_page_keys.line_timesheet_id IS NOT NULL AND COALESCE(line_rotation.rotation_scope_resolved, false) = false THEN false
      WHEN economic_key_rotation.canonical_timesheet_id IS NOT NULL
       AND line_rotation.canonical_timesheet_id IS NOT NULL
       AND economic_key_rotation.canonical_timesheet_id IS DISTINCT FROM line_rotation.canonical_timesheet_id THEN false
      WHEN COALESCE(economic_key_rotation.requested_is_canonical, line_rotation.requested_is_canonical, true) = false
       AND scope_canonical_line_matches.allocation_row_id IS NULL THEN false
      ELSE true
    END AS rotation_validation_ok,
    CASE
      WHEN allocation_page_keys.economic_key_timesheet_id IS NULL AND allocation_page_keys.line_timesheet_id IS NULL THEN NULL::text
      WHEN allocation_page_keys.economic_key_timesheet_id IS NOT NULL AND COALESCE(economic_key_rotation.rotation_scope_resolved, false) = false THEN 'ALLOCATION_ECONOMIC_KEY_TIMESHEET_ROTATION_SCOPE_UNRESOLVED'
      WHEN allocation_page_keys.line_timesheet_id IS NOT NULL AND COALESCE(line_rotation.rotation_scope_resolved, false) = false THEN 'ALLOCATION_LINE_TIMESHEET_ROTATION_SCOPE_UNRESOLVED'
      WHEN economic_key_rotation.canonical_timesheet_id IS NOT NULL
       AND line_rotation.canonical_timesheet_id IS NOT NULL
       AND economic_key_rotation.canonical_timesheet_id IS DISTINCT FROM line_rotation.canonical_timesheet_id THEN 'ALLOCATION_LINE_AND_ECONOMIC_KEY_CANONICAL_TIMESHEET_MISMATCH'
      WHEN COALESCE(economic_key_rotation.requested_is_canonical, line_rotation.requested_is_canonical, true) = false
       AND scope_canonical_line_matches.allocation_row_id IS NULL THEN 'ALLOCATION_TIMESHEET_ROTATED_WITHOUT_VALIDATED_CANONICAL_SCOPE_ROW'
      ELSE NULL::text
    END AS rotation_validation_failure_reason
  FROM allocation_page_keys
  LEFT JOIN allocation_rotation_scope AS economic_key_rotation
    ON economic_key_rotation.requested_timesheet_id = allocation_page_keys.economic_key_timesheet_id
  LEFT JOIN allocation_rotation_scope AS line_rotation
    ON line_rotation.requested_timesheet_id = allocation_page_keys.line_timesheet_id
  LEFT JOIN scope_canonical_line_matches
    ON scope_canonical_line_matches.allocation_row_id = allocation_page_keys.id;

  SELECT COUNT(*)::integer,
         COUNT(*) FILTER (WHERE NOT (UPPER(BTRIM(COALESCE(allocation_page.allocation_type, ''))) = ANY(v_deferred_finance_aliases)))::integer,
         COUNT(*) FILTER (WHERE UPPER(BTRIM(COALESCE(allocation_page.allocation_type, ''))) = ANY(v_deferred_finance_aliases))::integer
  INTO v_page_allocation_row_count,
       v_ordinary_page_allocation_row_count,
       v_deferred_finance_adjustment_row_count
  FROM pg_temp.tmp_pay_batch_item_allocation_page AS allocation_page;

  PERFORM 1
  FROM pg_temp.tmp_pay_batch_item_allocation_page AS allocation_page
  WHERE allocation_page.rotation_validation_ok IS NOT TRUE
  LIMIT 1;

  IF FOUND THEN
    RAISE EXCEPTION 'DRAFT_ITEM_ROTATED_ALLOCATION_STALE'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'DRAFT_ITEM_ROTATED_ALLOCATION_STALE',
              'pay_batch_id', p_pay_batch_id::text,
              'operation_id', p_operation_id::text,
              'message', 'One or more allocation rows no longer resolve to a validated current canonical timesheet. Refresh draft scope before creating batch items.'
            )::text;
  END IF;

  IF COALESCE(v_ordinary_page_allocation_row_count, 0) <= 0 THEN
    IF COALESCE(v_expected_allocation_row_count, 0) > 0
       AND COALESCE(v_linked_allocation_row_count, 0) = COALESCE(v_expected_allocation_row_count, 0) THEN
      UPDATE public.banking_pay_operation_candidate_scope AS scope_update
      SET pay_batch_id = p_pay_batch_id,
          status = 'DRAFTED',
          updated_at_utc = v_now
      WHERE scope_update.operation_id = p_operation_id
        AND scope_update.id IN (
          SELECT supplied_scope.candidate_scope_id_text::uuid
          FROM jsonb_array_elements_text(v_candidate_scope_ids) AS supplied_scope(candidate_scope_id_text)
        );

      RETURN jsonb_build_object(
        'ok', true,
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', p_operation_id::text,
        'candidate_scope_count', v_scope_id_count,
        'candidate_scope_ids', COALESCE(v_candidate_scope_ids, '[]'::jsonb),
        'allocation_row_count', COALESCE(v_expected_allocation_row_count, 0),
        'page_allocation_row_count', COALESCE(v_page_allocation_row_count, 0),
        'ordinary_page_allocation_row_count', COALESCE(v_ordinary_page_allocation_row_count, 0),
        'deferred_finance_adjustment_rows', COALESCE(v_deferred_finance_adjustment_row_count, 0),
        'linked_allocation_rows', COALESCE(v_linked_allocation_row_count, 0),
        'repaired_existing_item_links', COALESCE(v_repaired_existing_item_link_count, 0),
        'inserted_item_rows', 0,
        'reused_item_rows', COALESCE(v_linked_allocation_row_count, 0),
        'skipped_item_rows', 0,
        'failed_item_rows', 0,
        'has_more', false
      );
    END IF;

    IF COALESCE(v_deferred_finance_adjustment_row_count, 0) > 0 THEN
      RETURN jsonb_build_object(
        'ok', true,
        'pay_batch_id', p_pay_batch_id::text,
        'operation_id', p_operation_id::text,
        'candidate_scope_count', v_scope_id_count,
        'candidate_scope_ids', COALESCE(v_candidate_scope_ids, '[]'::jsonb),
        'allocation_row_count', COALESCE(v_expected_allocation_row_count, 0),
        'page_allocation_row_count', COALESCE(v_page_allocation_row_count, 0),
        'ordinary_page_allocation_row_count', COALESCE(v_ordinary_page_allocation_row_count, 0),
        'deferred_finance_adjustment_rows', COALESCE(v_deferred_finance_adjustment_row_count, 0),
        'linked_allocation_rows', COALESCE(v_linked_allocation_row_count, 0),
        'repaired_existing_item_links', COALESCE(v_repaired_existing_item_link_count, 0),
        'inserted_item_rows', 0,
        'reused_item_rows', 0,
        'skipped_item_rows', COALESCE(v_deferred_finance_adjustment_row_count, 0),
        'failed_item_rows', 0,
        'has_more', false
      );
    END IF;

    IF COALESCE(v_scope_expects_item_count, 0) > 0
       OR COALESCE(v_expected_allocation_row_count, 0) > 0 THEN
      RAISE EXCEPTION 'DRAFT_ITEM_ALLOCATION_ROWS_EMPTY'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'DRAFT_ITEM_ALLOCATION_ROWS_EMPTY',
                'pay_batch_id', p_pay_batch_id::text,
                'operation_id', p_operation_id::text,
                'candidate_scope_count', v_scope_id_count,
                'candidate_scope_ids', COALESCE(v_candidate_scope_ids, '[]'::jsonb),
                'allocation_row_count', COALESCE(v_expected_allocation_row_count, 0),
                'page_allocation_row_count', COALESCE(v_page_allocation_row_count, 0),
                'linked_allocation_rows', COALESCE(v_linked_allocation_row_count, 0),
                'scope_expects_item_count', COALESCE(v_scope_expects_item_count, 0),
                'message', 'No pending allocation rows were available and not all expected row-backed allocation rows were already linked to draft items.'
              )::text;
    END IF;

    RETURN jsonb_build_object(
      'ok', true,
      'pay_batch_id', p_pay_batch_id::text,
      'operation_id', p_operation_id::text,
      'candidate_scope_count', v_scope_id_count,
      'candidate_scope_ids', COALESCE(v_candidate_scope_ids, '[]'::jsonb),
      'allocation_row_count', COALESCE(v_expected_allocation_row_count, 0),
      'page_allocation_row_count', COALESCE(v_page_allocation_row_count, 0),
      'ordinary_page_allocation_row_count', COALESCE(v_ordinary_page_allocation_row_count, 0),
      'deferred_finance_adjustment_rows', COALESCE(v_deferred_finance_adjustment_row_count, 0),
      'linked_allocation_rows', COALESCE(v_linked_allocation_row_count, 0),
      'repaired_existing_item_links', COALESCE(v_repaired_existing_item_link_count, 0),
      'inserted_item_rows', 0,
      'reused_item_rows', 0,
      'skipped_item_rows', 0,
      'failed_item_rows', 0,
      'has_more', false
    );
  END IF;


  SELECT COALESCE(jsonb_agg(jsonb_build_object(
           'allocation_row_id', allocation_row.id::text,
           'candidate_scope_id', allocation_row.candidate_scope_id::text,
           'source_ref', allocation_row.source_ref,
           'timesheet_id', allocation_row.allocation_basis_json#>>'{economic_key,timesheet_id}',
           'key_type', COALESCE(allocation_row.allocation_basis_json#>>'{economic_key,key_type}', allocation_row.allocation_basis_json->>'key_type'),
           'key_value', COALESCE(allocation_row.allocation_basis_json#>>'{economic_key,key_value}', allocation_row.allocation_basis_json->>'key_value'),
           'allocated_amount', allocation_row.allocated_amount,
           'reason', 'RESOLVED_SYNTHETIC_TOTAL_ROW_ITEM_BLOCKED'
         ) ORDER BY allocation_row.sort_order, allocation_row.id), '[]'::jsonb)
  INTO v_synthetic_total_item_rows
  FROM pg_temp.tmp_pay_batch_item_allocation_page AS allocation_row
  WHERE UPPER(BTRIM(COALESCE(allocation_row.allocation_basis_json#>>'{economic_key,key_type}', allocation_row.allocation_basis_json->>'key_type', ''))) = 'TS_TOTAL'
    AND UPPER(BTRIM(COALESCE(allocation_row.allocation_basis_json#>>'{economic_key,key_value}', allocation_row.allocation_basis_json->>'key_value', ''))) = 'TOTAL'
    AND LOWER(BTRIM(COALESCE(allocation_row.source_ref, allocation_row.allocation_basis_json#>>'{line,row_key}', allocation_row.allocation_basis_json#>>'{line,line_key}', allocation_row.allocation_basis_json#>>'{line,source_ref}', allocation_row.allocation_basis_json->>'row_key', allocation_row.allocation_basis_json->>'line_key', ''))) LIKE '%:non_segment:total%'
    AND (
      lower(btrim(coalesce(allocation_row.allocation_basis_json->>'resolved_segment_rows_replace_source_total', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
      OR lower(btrim(coalesce(allocation_row.allocation_basis_json#>>'{line,resolved_segment_rows_replace_source_total}', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
      OR lower(btrim(coalesce(allocation_row.allocation_basis_json->>'has_resolved_rate', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
      OR lower(btrim(coalesce(allocation_row.allocation_basis_json#>>'{line,has_resolved_rate}', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
      OR lower(btrim(coalesce(allocation_row.allocation_basis_json#>>'{line,case_resolution_summary,has_resolved_rate}', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
      OR lower(btrim(coalesce(allocation_row.allocation_basis_json#>>'{line,case_resolution_summary,resolved_rate_applied}', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
      OR lower(btrim(coalesce(allocation_row.allocation_basis_json#>>'{line,case_resolution_summary,resolved_rate_active}', 'false'))) in ('true', 't', '1', 'yes', 'y', 'on')
      OR (
        COALESCE(allocation_row.allocation_basis_json#>>'{line,case_resolution_summary,resolved_rate_component_count}', '') ~ '^[0-9]+$'
        AND (allocation_row.allocation_basis_json#>>'{line,case_resolution_summary,resolved_rate_component_count}')::integer > 0
      )
      OR EXISTS (
        SELECT 1
        FROM public.banking_pay_operation_candidate_scope AS scope_row
        CROSS JOIN LATERAL private.pay_workbench_draft_scope_line_rows_v8(
          scope_row.id,
          scope_row.selected_canonical_preview_lines_json,
          scope_row.effective_canonical_preview_lines_json
        ) AS selected_line(line_json, line_ordinal)
        WHERE scope_row.operation_id = p_operation_id
          AND scope_row.id = allocation_row.candidate_scope_id
          AND UPPER(BTRIM(COALESCE(selected_line.line_json#>>'{economic_key,key_type}', selected_line.line_json->>'key_type', ''))) = 'TS_DAY'
          AND LOWER(BTRIM(COALESCE(selected_line.line_json->>'row_key', selected_line.line_json->>'line_key', selected_line.line_json->>'source_ref', ''))) LIKE '%:segment:%'
          AND COALESCE(selected_line.line_json#>>'{economic_key,timesheet_id}', selected_line.line_json->>'timesheet_id', selected_line.line_json->>'real_business_timesheet_id', '') = COALESCE(allocation_row.allocation_basis_json#>>'{economic_key,timesheet_id}', allocation_row.allocation_basis_json#>>'{line,timesheet_id}', allocation_row.allocation_basis_json#>>'{line,real_business_timesheet_id}', '')
      )
    );

  IF jsonb_array_length(COALESCE(v_synthetic_total_item_rows, '[]'::jsonb)) > 0 THEN
    RAISE EXCEPTION 'RESOLVED_SYNTHETIC_TOTAL_ROW_ITEM_BLOCKED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'RESOLVED_SYNTHETIC_TOTAL_ROW_ITEM_BLOCKED',
              'operation_id', p_operation_id::text,
              'pay_batch_id', p_pay_batch_id::text,
              'synthetic_total_item_rows', COALESCE(v_synthetic_total_item_rows, '[]'::jsonb),
              'message', 'A stale resolved-timesheet synthetic total row reached item creation. Refresh Banking Pay and try Create Draft again.'
            )::text;
  END IF;

  PERFORM 1
  FROM pg_temp.tmp_pay_batch_item_allocation_page AS allocation_row
  WHERE NOT (UPPER(BTRIM(COALESCE(allocation_row.allocation_type, ''))) = ANY(v_deferred_finance_aliases))
    AND (
      LOWER(BTRIM(COALESCE(allocation_row.allocation_basis_json#>>'{line,row_key}', allocation_row.allocation_basis_json#>>'{line,line_key}', allocation_row.allocation_basis_json->>'row_key', allocation_row.allocation_basis_json->>'line_key', ''))) LIKE 'timesheet_snapshot:%'
      OR UPPER(BTRIM(COALESCE(allocation_row.allocation_basis_json#>>'{line,source_kind}', allocation_row.allocation_basis_json#>>'{line,source_type}', allocation_row.allocation_basis_json->>'source_kind', allocation_row.allocation_basis_json->>'source_type', ''))) IN (
          'TIMESHEET_SNAPSHOT',
          'TIMESHEET_SNAPSHOT_EVIDENCE',
          'RAW_TIMESHEET_SNAPSHOT',
          'INTERNAL_ONLY',
          'NO_DELTA',
          'EXCLUDED'
        )
     OR UPPER(BTRIM(COALESCE(allocation_row.allocation_basis_json#>>'{line,presentation_section}', allocation_row.allocation_basis_json->>'presentation_section', ''))) <> 'READY_TO_PAY'
     OR LOWER(BTRIM(COALESCE(allocation_row.allocation_basis_json#>>'{line,draftable}', allocation_row.allocation_basis_json->>'draftable', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
     OR LOWER(BTRIM(COALESCE(allocation_row.allocation_basis_json#>>'{line,is_ready_for_draft}', allocation_row.allocation_basis_json->>'is_ready_for_draft', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
     OR (
       UPPER(BTRIM(COALESCE(allocation_row.allocation_basis_json#>>'{economic_key,key_type}', allocation_row.allocation_basis_json->>'key_type', ''))) NOT IN ('TS_DAY','TS_TOTAL','ADDITIONAL_CODE','ADJUSTMENT_CODE','EXPENSE_CODE','MANUAL_CARRY_FORWARD')
       AND NOT EXISTS (
         SELECT 1
         FROM pg_temp.tmp_pay_batch_insert_items_certified_finance AS finance_row
         WHERE finance_row.allocation_row_id = allocation_row.id
           AND finance_row.visible_alias = 'MANUAL_DEBT_RECOVERY'
           AND finance_row.certification_ok
       )
     )
     OR NULLIF(BTRIM(COALESCE(allocation_row.allocation_basis_json#>>'{economic_key,key_value}', allocation_row.allocation_basis_json->>'key_value', '')), '') IS NULL
     OR ROUND(COALESCE(allocation_row.allocated_amount, 0), 2) = 0
     OR (
       ROUND(COALESCE(allocation_row.allocated_amount, 0), 2) < 0
       AND NOT (
         (
           UPPER(BTRIM(COALESCE(allocation_row.allocation_type, ''))) = 'OVERPAYMENT_RECOVERY'
           AND allocation_row.finance_case_id IS NOT NULL
         )
         OR EXISTS (
           SELECT 1
           FROM pg_temp.tmp_pay_batch_insert_items_certified_finance AS finance_row
           WHERE finance_row.allocation_row_id = allocation_row.id
             AND finance_row.visible_alias = 'MANUAL_DEBT_RECOVERY'
             AND finance_row.certification_ok
         )
       )
     )
      OR (
        ROUND(COALESCE(allocation_row.allocated_amount, 0), 2) <= 0
        AND NOT (UPPER(BTRIM(COALESCE(allocation_row.allocation_type, ''))) = ANY(v_deferred_finance_aliases))
        AND NOT EXISTS (
          SELECT 1
          FROM pg_temp.tmp_pay_batch_insert_items_certified_finance AS finance_row
          WHERE finance_row.allocation_row_id = allocation_row.id
            AND finance_row.visible_alias = 'MANUAL_DEBT_RECOVERY'
            AND finance_row.certification_ok
        )
      )
    )
  LIMIT 1;

  IF FOUND THEN
    RAISE EXCEPTION 'MALFORMED_PREVIEW_ALLOCATION_ROW_NOT_DRAFTABLE'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'MALFORMED_PREVIEW_ALLOCATION_ROW_NOT_DRAFTABLE',
              'operation_id', p_operation_id::text,
              'pay_batch_id', p_pay_batch_id::text,
              'message', 'Allocation rows from the preview are not valid draftable Ready to Pay rows.'
            )::text;
  END IF;

  DROP TABLE IF EXISTS pg_temp.tmp_pay_batch_item_allocation_outstanding_components;
  CREATE TEMPORARY TABLE pg_temp.tmp_pay_batch_item_allocation_outstanding_components ON COMMIT DROP AS
  SELECT
    outstanding_components.timesheet_id,
    outstanding_components.key_type,
    outstanding_components.key_value,
    outstanding_components.outstanding_ex_vat
  FROM public._pay_outstanding_components(
    ARRAY(
      SELECT DISTINCT allocation_page.canonical_timesheet_id
      FROM pg_temp.tmp_pay_batch_item_allocation_page AS allocation_page
      WHERE allocation_page.canonical_timesheet_id IS NOT NULL
    ),
    p_pay_batch_id
  ) AS outstanding_components;

  PERFORM 1
  FROM pg_temp.tmp_pay_batch_item_allocation_page AS allocation_row
  CROSS JOIN LATERAL (
    SELECT
      CASE WHEN NULLIF(BTRIM(COALESCE(allocation_row.allocation_basis_json#>>'{economic_key,timesheet_id}', allocation_row.allocation_basis_json->>'timesheet_id', allocation_row.allocation_basis_json#>>'{line,timesheet_id}', '')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        THEN NULLIF(BTRIM(COALESCE(allocation_row.allocation_basis_json#>>'{economic_key,timesheet_id}', allocation_row.allocation_basis_json->>'timesheet_id', allocation_row.allocation_basis_json#>>'{line,timesheet_id}', '')), '')::uuid
        ELSE NULL::uuid
      END AS timesheet_id,
      UPPER(NULLIF(BTRIM(COALESCE(allocation_row.allocation_basis_json#>>'{economic_key,key_type}', allocation_row.allocation_basis_json->>'key_type', '')), '')) AS key_type,
      NULLIF(BTRIM(COALESCE(allocation_row.allocation_basis_json#>>'{economic_key,key_value}', allocation_row.allocation_basis_json->>'key_value', '')), '') AS key_value
  ) AS allocation_key
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
          WHEN jsonb_typeof(allocation_row.allocation_basis_json#>'{line,case_components}') = 'array' THEN allocation_row.allocation_basis_json#>'{line,case_components}'
          ELSE '[]'::jsonb
        END
      ) AS component_element(value)
    ) AS component_counts
  ) AS component_probe
  WHERE NOT (UPPER(BTRIM(COALESCE(allocation_row.allocation_type, ''))) = ANY(v_deferred_finance_aliases))
    AND component_probe.single_fixed_reimbursement_key_type IS NOT NULL
    AND (
      allocation_key.key_type IS DISTINCT FROM component_probe.single_fixed_reimbursement_key_type
      OR allocation_key.key_value IS DISTINCT FROM component_probe.single_fixed_reimbursement_key_value
    )
  LIMIT 1;

  IF FOUND THEN
    RAISE EXCEPTION 'PREVIEW_ALLOCATION_REIMBURSEMENT_KEY_MISMATCH'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PREVIEW_ALLOCATION_REIMBURSEMENT_KEY_MISMATCH',
              'operation_id', p_operation_id::text,
              'pay_batch_id', p_pay_batch_id::text,
              'message', 'A fixed reimbursement preview allocation did not use its EXPENSE_CODE/ADDITIONAL_CODE economic key.'
            )::text;
  END IF;

  PERFORM 1
  FROM pg_temp.tmp_pay_batch_item_allocation_page AS allocation_row
  CROSS JOIN LATERAL (
    SELECT
      CASE WHEN NULLIF(BTRIM(COALESCE(allocation_row.allocation_basis_json#>>'{economic_key,timesheet_id}', allocation_row.allocation_basis_json->>'timesheet_id', allocation_row.allocation_basis_json#>>'{line,timesheet_id}', '')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        THEN NULLIF(BTRIM(COALESCE(allocation_row.allocation_basis_json#>>'{economic_key,timesheet_id}', allocation_row.allocation_basis_json->>'timesheet_id', allocation_row.allocation_basis_json#>>'{line,timesheet_id}', '')), '')::uuid
        ELSE NULL::uuid
      END AS timesheet_id,
      UPPER(NULLIF(BTRIM(COALESCE(allocation_row.allocation_basis_json#>>'{economic_key,key_type}', allocation_row.allocation_basis_json->>'key_type', '')), '')) AS key_type,
      NULLIF(BTRIM(COALESCE(allocation_row.allocation_basis_json#>>'{economic_key,key_value}', allocation_row.allocation_basis_json->>'key_value', '')), '') AS key_value
  ) AS allocation_key
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
          WHEN jsonb_typeof(allocation_row.allocation_basis_json#>'{line,case_components}') = 'array' THEN allocation_row.allocation_basis_json#>'{line,case_components}'
          ELSE '[]'::jsonb
        END
      ) AS component_element(value)
    ) AS component_counts
  ) AS component_probe
  LEFT JOIN pg_temp.tmp_pay_batch_item_allocation_outstanding_components AS outstanding_component
    ON outstanding_component.timesheet_id = allocation_key.timesheet_id
   AND outstanding_component.key_type = allocation_key.key_type
   AND outstanding_component.key_value = allocation_key.key_value
  WHERE NOT (UPPER(BTRIM(COALESCE(allocation_row.allocation_type, ''))) = ANY(v_deferred_finance_aliases))
    AND component_probe.single_fixed_reimbursement_key_type IS NOT NULL
    AND allocation_key.key_type = component_probe.single_fixed_reimbursement_key_type
    AND allocation_key.key_value = component_probe.single_fixed_reimbursement_key_value
    AND (
      outstanding_component.outstanding_ex_vat IS NULL
      OR ROUND(COALESCE(outstanding_component.outstanding_ex_vat, 0), 2) <= 0
      OR ROUND(COALESCE(allocation_row.allocated_amount, 0), 2) <= 0
      OR ROUND(COALESCE(allocation_row.allocated_amount, 0), 2) > ROUND(COALESCE(outstanding_component.outstanding_ex_vat, 0), 2)
    )
  LIMIT 1;

  IF FOUND THEN
    RAISE EXCEPTION 'PREVIEW_ALLOCATION_REIMBURSEMENT_OUTSTANDING_NOT_AVAILABLE'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PREVIEW_ALLOCATION_REIMBURSEMENT_OUTSTANDING_NOT_AVAILABLE',
              'operation_id', p_operation_id::text,
              'pay_batch_id', p_pay_batch_id::text,
              'message', 'A selected fixed reimbursement preview allocation no longer has enough outstanding entitlement to draft.'
            )::text;
  END IF;

  PERFORM 1
  FROM pg_temp.tmp_pay_batch_item_allocation_page AS allocation_row
  WHERE UPPER(BTRIM(COALESCE(allocation_row.allocation_basis_json#>>'{economic_key,key_type}', allocation_row.allocation_basis_json->>'key_type', ''))) = 'TS_DAY'
    AND NULLIF(BTRIM(COALESCE(allocation_row.allocation_basis_json#>>'{economic_key,key_value}', allocation_row.allocation_basis_json->>'key_value', '')), '') !~ '^\d{4}-\d{2}-\d{2}$'
  LIMIT 1;

  IF FOUND THEN
    RAISE EXCEPTION 'Selected preview rows did not resolve TS_DAY economic keys to YYYY-MM-DD date buckets';
  END IF;

  DROP TABLE IF EXISTS pg_temp.tmp_pay_batch_item_normalised_rows;
  CREATE TEMPORARY TABLE tmp_pay_batch_item_normalised_rows ON COMMIT DROP AS
  WITH prepared_rows AS (
    SELECT
      allocation_row.id AS allocation_row_id,
      allocation_row.candidate_scope_id,
      allocation_row.operation_id,
      allocation_row.pay_batch_id,
      allocation_row.candidate_id,
      allocation_row.pay_channel,
      allocation_row.finance_case_id,
      allocation_row.finance_component_id,
      allocation_row.source_ref,
      allocation_row.allocation_type,
      allocation_row.operation_source_key,
      allocation_row.allocated_amount,
      allocation_row.allocation_basis_json,
      COALESCE(allocation_row.allocation_basis_json->'line', '{}'::jsonb) AS line_json,
      COALESCE(allocation_row.allocation_basis_json->'finance_component', '{}'::jsonb) AS finance_component_json,
      CASE WHEN NULLIF(BTRIM(COALESCE(allocation_row.allocation_basis_json#>>'{economic_key,timesheet_id}', allocation_row.allocation_basis_json->>'timesheet_id', '')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN NULLIF(BTRIM(COALESCE(allocation_row.allocation_basis_json#>>'{economic_key,timesheet_id}', allocation_row.allocation_basis_json->>'timesheet_id', '')), '')::uuid ELSE NULL::uuid END AS timesheet_id,
      UPPER(NULLIF(BTRIM(COALESCE(allocation_row.allocation_basis_json#>>'{economic_key,key_type}', allocation_row.allocation_basis_json->>'key_type', '')), '')) AS key_type,
      NULLIF(BTRIM(COALESCE(allocation_row.allocation_basis_json#>>'{economic_key,key_value}', allocation_row.allocation_basis_json->>'key_value', '')), '') AS key_value
    FROM pg_temp.tmp_pay_batch_item_allocation_page AS allocation_row
    WHERE NOT (UPPER(BTRIM(COALESCE(allocation_row.allocation_type, ''))) = ANY(v_deferred_finance_aliases))
  ), normalised_source_rows AS (
    SELECT
      prepared_rows.*,
      CASE
        WHEN UPPER(COALESCE(prepared_rows.line_json->>'line_type', prepared_rows.allocation_type, '')) IN ('MANUAL_ADJUSTMENT_CARRY_FORWARD', 'MANUAL_CREDIT_PAYOUT') THEN 'MANUAL_CREDIT_PAYOUT'
        WHEN UPPER(COALESCE(prepared_rows.line_json->>'line_type', prepared_rows.allocation_type, '')) = 'MANUAL_DEBT_RECOVERY' THEN 'MANUAL_DEBT_RECOVERY'
        WHEN prepared_rows.key_type IN ('TS_DAY', 'TS_TOTAL') THEN 'SEGMENT_DELTA'
        WHEN prepared_rows.key_type = 'ADJUSTMENT_CODE' THEN 'ADJUSTMENT_DELTA'
        WHEN prepared_rows.key_type = 'EXPENSE_CODE' AND UPPER(COALESCE(prepared_rows.key_value, '')) = 'MILEAGE' THEN 'MILEAGE_DELTA'
        ELSE 'EXPENSE_DELTA'
      END AS item_type,
      CASE WHEN prepared_rows.key_type IN ('TS_DAY', 'TS_TOTAL') THEN NULLIF(BTRIM(COALESCE(prepared_rows.line_json#>>'{source_basis_json,segment_key}', prepared_rows.line_json#>>'{source_basis,segment_key}', prepared_rows.line_json->>'segment_key', '')), '') ELSE NULL::text END AS segment_key,
      ROUND(COALESCE(prepared_rows.allocated_amount, 0), 2)::numeric(12,2) AS amount_ex_vat,
      CASE
        WHEN UPPER(COALESCE(prepared_rows.finance_component_json->>'classification', prepared_rows.line_json->>'classification', prepared_rows.line_json->>'frozen_component_classification', '')) = 'REIMBURSEMENT_GROSS_FIXED' THEN 'REIMBURSEMENT_GROSS_FIXED'::public.pay_finance_component_classification_enum
        WHEN UPPER(COALESCE(prepared_rows.finance_component_json->>'classification', prepared_rows.line_json->>'classification', prepared_rows.line_json->>'frozen_component_classification', '')) = 'NET_PAY_FIXED_RECOVERY' THEN 'NET_PAY_FIXED_RECOVERY'::public.pay_finance_component_classification_enum
        ELSE 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
      END AS component_classification,
      CASE
        WHEN UPPER(COALESCE(prepared_rows.line_json->>'line_type', prepared_rows.allocation_type, '')) = 'MANUAL_DEBT_RECOVERY'
          THEN UPPER(NULLIF(BTRIM(COALESCE(prepared_rows.line_json->>'paye_treatment', '')), ''))
        ELSE NULL::text
      END AS paye_treatment,
      CASE
        WHEN UPPER(COALESCE(prepared_rows.finance_component_json->>'saved_resolution_mode', prepared_rows.line_json->>'resolution_mode', prepared_rows.line_json->>'saved_resolution_mode', '')) = 'SUGGESTED_EQUIVALENT_BASIS' THEN 'SUGGESTED_EQUIVALENT_BASIS'::public.pay_finance_component_resolution_mode_enum
        WHEN UPPER(COALESCE(prepared_rows.finance_component_json->>'saved_resolution_mode', prepared_rows.line_json->>'resolution_mode', prepared_rows.line_json->>'saved_resolution_mode', '')) = 'MANUAL_REPLACEMENT_RATE' THEN 'MANUAL_REPLACEMENT_RATE'::public.pay_finance_component_resolution_mode_enum
        WHEN UPPER(COALESCE(prepared_rows.finance_component_json->>'saved_resolution_mode', prepared_rows.line_json->>'resolution_mode', prepared_rows.line_json->>'saved_resolution_mode', '')) = 'MANUAL_AMOUNT' THEN 'MANUAL_AMOUNT'::public.pay_finance_component_resolution_mode_enum
        ELSE NULL::public.pay_finance_component_resolution_mode_enum
      END AS resolution_mode,
      CASE
        WHEN ROUND(COALESCE(prepared_rows.allocated_amount, 0), 2) > 0
         AND NOT (UPPER(BTRIM(COALESCE(prepared_rows.allocation_type, ''))) = ANY(v_deferred_finance_aliases))
         AND UPPER(COALESCE(prepared_rows.line_json->>'line_type', prepared_rows.line_json->>'item_type', prepared_rows.allocation_type, '')) NOT IN ('DEBT_CREATED', 'LOAN_REPAYMENT', 'OVERPAYMENT_RECOVERY', 'LOAN_PAYOUT')
         AND (
           prepared_rows.key_type IN ('TS_DAY', 'TS_TOTAL', 'ADDITIONAL_CODE', 'ADJUSTMENT_CODE', 'EXPENSE_CODE')
           OR UPPER(COALESCE(prepared_rows.line_json->>'line_type', prepared_rows.line_json->>'item_type', prepared_rows.allocation_type, '')) IN ('TIMESHEET_PAYMENT', 'SEGMENT_DELTA', 'EXPENSE_DELTA', 'MILEAGE_DELTA', 'ADJUSTMENT_DELTA')
         ) THEN ROUND(COALESCE(
          (
            SELECT ABS((bucket_resolution.bucket_json->>'source_pay_ex_vat')::numeric)
            FROM jsonb_array_elements(
              CASE
                WHEN jsonb_typeof(prepared_rows.line_json#>'{correction_chain_component,resolution_rows}') = 'array'
                  THEN prepared_rows.line_json#>'{correction_chain_component,resolution_rows}'
                ELSE '[]'::jsonb
              END
            ) AS canonical_resolution(resolution_json)
            CROSS JOIN LATERAL jsonb_array_elements(
              CASE
                WHEN jsonb_typeof(canonical_resolution.resolution_json#>'{payload_json,bucket_resolutions}') = 'array'
                  THEN canonical_resolution.resolution_json#>'{payload_json,bucket_resolutions}'
                ELSE '[]'::jsonb
              END
            ) AS bucket_resolution(bucket_json)
            WHERE UPPER(BTRIM(COALESCE(bucket_resolution.bucket_json->>'component_key_type', ''))) = prepared_rows.key_type
              AND BTRIM(COALESCE(bucket_resolution.bucket_json->>'component_key_value', '')) = prepared_rows.key_value
              AND COALESCE(bucket_resolution.bucket_json->>'source_pay_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
              AND ROUND(ABS((bucket_resolution.bucket_json->>'source_pay_ex_vat')::numeric), 2) > 0
            ORDER BY canonical_resolution.resolution_json->>'resolution_id',
                     bucket_resolution.bucket_json->>'component_key_type',
                     bucket_resolution.bucket_json->>'component_key_value'
            LIMIT 1
          ),
          CASE WHEN COALESCE(prepared_rows.finance_component_json->>'allocated_source_due_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' AND ROUND(ABS((prepared_rows.finance_component_json->>'allocated_source_due_amount_ex_vat')::numeric), 2) > 0 THEN ABS((prepared_rows.finance_component_json->>'allocated_source_due_amount_ex_vat')::numeric) ELSE NULL::numeric END,
          CASE WHEN COALESCE(prepared_rows.finance_component_json->>'remaining_source_amount', '') ~ '^-?[0-9]+(\.[0-9]+)?$' AND ROUND(ABS((prepared_rows.finance_component_json->>'remaining_source_amount')::numeric), 2) > 0 THEN ABS((prepared_rows.finance_component_json->>'remaining_source_amount')::numeric) ELSE NULL::numeric END,
          CASE WHEN COALESCE(prepared_rows.line_json->>'source_reservation_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' AND ROUND(ABS((prepared_rows.line_json->>'source_reservation_amount_ex_vat')::numeric), 2) > 0 THEN ABS((prepared_rows.line_json->>'source_reservation_amount_ex_vat')::numeric) ELSE NULL::numeric END,
          CASE WHEN COALESCE(prepared_rows.line_json->>'source_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' AND ROUND(ABS((prepared_rows.line_json->>'source_amount_ex_vat')::numeric), 2) > 0 THEN ABS((prepared_rows.line_json->>'source_amount_ex_vat')::numeric) ELSE NULL::numeric END,
          CASE WHEN COALESCE(prepared_rows.line_json->>'source_entitlement_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' AND ROUND(ABS((prepared_rows.line_json->>'source_entitlement_amount_ex_vat')::numeric), 2) > 0 THEN ABS((prepared_rows.line_json->>'source_entitlement_amount_ex_vat')::numeric) ELSE NULL::numeric END,
          CASE WHEN COALESCE(prepared_rows.line_json->>'frozen_source_amount', '') ~ '^-?[0-9]+(\.[0-9]+)?$' AND ROUND(ABS((prepared_rows.line_json->>'frozen_source_amount')::numeric), 2) > 0 THEN ABS((prepared_rows.line_json->>'frozen_source_amount')::numeric) ELSE NULL::numeric END,
          CASE WHEN COALESCE(prepared_rows.line_json->>'pay_outstanding_clamped_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' AND ROUND(ABS((prepared_rows.line_json->>'pay_outstanding_clamped_amount_ex_vat')::numeric), 2) > 0 THEN ABS((prepared_rows.line_json->>'pay_outstanding_clamped_amount_ex_vat')::numeric) ELSE NULL::numeric END,
          CASE WHEN COALESCE(prepared_rows.line_json->>'pay_outstanding_available_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' AND ROUND(ABS((prepared_rows.line_json->>'pay_outstanding_available_ex_vat')::numeric), 2) > 0 THEN ABS((prepared_rows.line_json->>'pay_outstanding_available_ex_vat')::numeric) ELSE NULL::numeric END,
          CASE WHEN COALESCE(prepared_rows.line_json#>>'{source_basis_json,source_reservation_amount_ex_vat}', '') ~ '^-?[0-9]+(\.[0-9]+)?$' AND ROUND(ABS((prepared_rows.line_json#>>'{source_basis_json,source_reservation_amount_ex_vat}')::numeric), 2) > 0 THEN ABS((prepared_rows.line_json#>>'{source_basis_json,source_reservation_amount_ex_vat}')::numeric) ELSE NULL::numeric END,
          CASE WHEN COALESCE(prepared_rows.line_json#>>'{source_basis_json,source_amount_ex_vat}', '') ~ '^-?[0-9]+(\.[0-9]+)?$' AND ROUND(ABS((prepared_rows.line_json#>>'{source_basis_json,source_amount_ex_vat}')::numeric), 2) > 0 THEN ABS((prepared_rows.line_json#>>'{source_basis_json,source_amount_ex_vat}')::numeric) ELSE NULL::numeric END,
          CASE WHEN COALESCE(prepared_rows.line_json#>>'{source_basis_json,source_entitlement_amount_ex_vat}', '') ~ '^-?[0-9]+(\.[0-9]+)?$' AND ROUND(ABS((prepared_rows.line_json#>>'{source_basis_json,source_entitlement_amount_ex_vat}')::numeric), 2) > 0 THEN ABS((prepared_rows.line_json#>>'{source_basis_json,source_entitlement_amount_ex_vat}')::numeric) ELSE NULL::numeric END,
          CASE WHEN COALESCE(prepared_rows.line_json#>>'{source_basis_json,source_pay_ex_vat}', '') ~ '^-?[0-9]+(\.[0-9]+)?$' AND ROUND(ABS((prepared_rows.line_json#>>'{source_basis_json,source_pay_ex_vat}')::numeric), 2) > 0 THEN ABS((prepared_rows.line_json#>>'{source_basis_json,source_pay_ex_vat}')::numeric) ELSE NULL::numeric END,
          CASE WHEN COALESCE(prepared_rows.line_json#>>'{frozen_source_basis_json,source_reservation_amount_ex_vat}', '') ~ '^-?[0-9]+(\.[0-9]+)?$' AND ROUND(ABS((prepared_rows.line_json#>>'{frozen_source_basis_json,source_reservation_amount_ex_vat}')::numeric), 2) > 0 THEN ABS((prepared_rows.line_json#>>'{frozen_source_basis_json,source_reservation_amount_ex_vat}')::numeric) ELSE NULL::numeric END,
          CASE WHEN COALESCE(prepared_rows.line_json#>>'{frozen_source_basis_json,source_pay_ex_vat}', '') ~ '^-?[0-9]+(\.[0-9]+)?$' AND ROUND(ABS((prepared_rows.line_json#>>'{frozen_source_basis_json,source_pay_ex_vat}')::numeric), 2) > 0 THEN ABS((prepared_rows.line_json#>>'{frozen_source_basis_json,source_pay_ex_vat}')::numeric) ELSE NULL::numeric END,
          CASE WHEN COALESCE(prepared_rows.line_json#>>'{frozen_component_snapshot_json,source_reservation_amount_ex_vat}', '') ~ '^-?[0-9]+(\.[0-9]+)?$' AND ROUND(ABS((prepared_rows.line_json#>>'{frozen_component_snapshot_json,source_reservation_amount_ex_vat}')::numeric), 2) > 0 THEN ABS((prepared_rows.line_json#>>'{frozen_component_snapshot_json,source_reservation_amount_ex_vat}')::numeric) ELSE NULL::numeric END,
          CASE WHEN COALESCE(prepared_rows.line_json#>>'{frozen_component_snapshot_json,source_pay_ex_vat}', '') ~ '^-?[0-9]+(\.[0-9]+)?$' AND ROUND(ABS((prepared_rows.line_json#>>'{frozen_component_snapshot_json,source_pay_ex_vat}')::numeric), 2) > 0 THEN ABS((prepared_rows.line_json#>>'{frozen_component_snapshot_json,source_pay_ex_vat}')::numeric) ELSE NULL::numeric END,
          CASE WHEN COALESCE(prepared_rows.line_json#>>'{frozen_component_snapshot_json,source_basis_json,source_pay_ex_vat}', '') ~ '^-?[0-9]+(\.[0-9]+)?$' AND ROUND(ABS((prepared_rows.line_json#>>'{frozen_component_snapshot_json,source_basis_json,source_pay_ex_vat}')::numeric), 2) > 0 THEN ABS((prepared_rows.line_json#>>'{frozen_component_snapshot_json,source_basis_json,source_pay_ex_vat}')::numeric) ELSE NULL::numeric END,
          ABS(COALESCE(prepared_rows.allocated_amount, 0))
        ), 2)::numeric(12,2)
        ELSE ROUND(COALESCE(
          CASE WHEN COALESCE(prepared_rows.line_json->>'source_reservation_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ABS((prepared_rows.line_json->>'source_reservation_amount_ex_vat')::numeric) ELSE NULL::numeric END,
          CASE WHEN COALESCE(prepared_rows.line_json->>'source_entitlement_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$' THEN ABS((prepared_rows.line_json->>'source_entitlement_amount_ex_vat')::numeric) ELSE NULL::numeric END,
          ABS(COALESCE(prepared_rows.allocated_amount, 0))
        ), 2)::numeric(12,2)
      END AS frozen_source_amount
    FROM prepared_rows
  ), normalised_rows AS (
    SELECT
      normalised_source_rows.*,
      payee_resolution.resolved_umbrella_id,
      COALESCE(umbrella_row.vat_chargeable, false) AS umbrella_vat_chargeable,
      payout_route.routing_kind,
      payout_route.payee_entity_kind,
      payout_route.payee_entity_id,
      payout_route.beneficiary_name,
      payout_route.destination_label,
      payout_route.sort_code,
      payout_route.account_number,
      payout_route.bank_details_hash,
      payout_route.week_ending_bucket,
      payout_route.payout_instruction_error_code,
      jsonb_strip_nulls(jsonb_build_object(
        'routing_kind', payout_route.routing_kind,
        'payee_entity_kind', payout_route.payee_entity_kind,
        'payee_entity_id', CASE WHEN payout_route.payee_entity_id IS NULL THEN NULL ELSE payout_route.payee_entity_id::text END,
        'umbrella_id', CASE WHEN payout_route.payee_entity_kind = 'UMBRELLA' AND payee_resolution.resolved_umbrella_id IS NOT NULL THEN payee_resolution.resolved_umbrella_id::text ELSE NULL END,
        'beneficiary_name', payout_route.beneficiary_name,
        'payee_name', payout_route.beneficiary_name,
        'sort_code', payout_route.sort_code,
        'account_number', payout_route.account_number,
        'bank_details_hash', payout_route.bank_details_hash,
        'destination_label', payout_route.destination_label,
        'week_ending_bucket', CASE WHEN payout_route.week_ending_bucket IS NULL THEN NULL ELSE payout_route.week_ending_bucket::text END
      ) || CASE
        WHEN normalised_source_rows.item_type = 'MANUAL_DEBT_RECOVERY' THEN jsonb_build_object(
          'taxability', normalised_source_rows.line_json->'taxability',
          'routing_kind', normalised_source_rows.line_json->'routing_kind',
          'destination_label', normalised_source_rows.line_json->'destination_label',
          'pay_channel', to_jsonb(UPPER(BTRIM(normalised_source_rows.pay_channel))),
          'beneficiary_name', normalised_source_rows.line_json->'beneficiary_name',
          'masked_bank_account', normalised_source_rows.line_json->'masked_bank_account',
          'bank_details_hash', normalised_source_rows.line_json->'bank_details_hash',
          'appears_on_umbrella_remittance', normalised_source_rows.line_json->'appears_on_umbrella_remittance',
          'generates_candidate_payment_advice', normalised_source_rows.line_json->'generates_candidate_payment_advice',
          'is_candidate_directed_oneoff_payout', normalised_source_rows.line_json->'is_candidate_directed_oneoff_payout'
        )
        ELSE '{}'::jsonb
      END) AS payout_instruction_snapshot_json,
      ROUND(COALESCE((vat_calculation.vat_json->>'vat')::numeric, 0), 2)::numeric(12,2) AS amount_vat,
      ROUND(COALESCE((vat_calculation.vat_json->>'inc')::numeric, normalised_source_rows.amount_ex_vat, 0), 2)::numeric(12,2) AS amount_inc_vat
    FROM normalised_source_rows
    JOIN public.candidates AS candidate_row
      ON candidate_row.id = normalised_source_rows.candidate_id
    LEFT JOIN public.banking_pay_operation_candidate_scope AS scope_row
      ON scope_row.id = normalised_source_rows.candidate_scope_id
     AND scope_row.operation_id = p_operation_id
    CROSS JOIN LATERAL (
      SELECT
        CASE
          WHEN UPPER(BTRIM(COALESCE(normalised_source_rows.pay_channel, ''))) = 'UMBRELLA' THEN
            COALESCE(
              CASE
                WHEN NULLIF(BTRIM(COALESCE(scope_row.effective_non_paye_payee_json->>'umbrella_id', '')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                  THEN NULLIF(BTRIM(COALESCE(scope_row.effective_non_paye_payee_json->>'umbrella_id', '')), '')::uuid
                ELSE NULL::uuid
              END,
              candidate_row.umbrella_id
            )
          ELSE NULL::uuid
        END AS resolved_umbrella_id
    ) AS payee_resolution
    LEFT JOIN public.umbrellas AS umbrella_row
      ON umbrella_row.id = payee_resolution.resolved_umbrella_id
    CROSS JOIN LATERAL (
      SELECT
        UPPER(BTRIM(COALESCE(normalised_source_rows.pay_channel, ''))) AS channel_upper,
        NULLIF(BTRIM(COALESCE(umbrella_row.name, '')), '') AS umbrella_name,
        NULLIF(BTRIM(COALESCE(umbrella_row.sort_code, '')), '') AS umbrella_sort_code,
        NULLIF(BTRIM(COALESCE(umbrella_row.account_number, '')), '') AS umbrella_account_number,
        NULLIF(BTRIM(COALESCE(umbrella_row.bank_details_hash, '')), '') AS umbrella_bank_details_hash,
        COALESCE(
          NULLIF(BTRIM(candidate_row.account_holder), ''),
          NULLIF(BTRIM(candidate_row.display_name), ''),
          NULLIF(BTRIM(CONCAT_WS(' ', NULLIF(BTRIM(candidate_row.first_name), ''), NULLIF(BTRIM(candidate_row.last_name), ''))), '')
        ) AS candidate_payee_name,
        NULLIF(BTRIM(COALESCE(candidate_row.sort_code, '')), '') AS candidate_sort_code,
        NULLIF(BTRIM(COALESCE(candidate_row.account_number, '')), '') AS candidate_account_number,
        NULLIF(BTRIM(COALESCE(candidate_row.bank_details_hash, '')), '') AS candidate_bank_details_hash,
        CASE
          WHEN NULLIF(BTRIM(COALESCE(
            normalised_source_rows.line_json->>'week_ending_bucket',
            normalised_source_rows.line_json->>'week_ending_date',
            normalised_source_rows.line_json#>>'{timesheet,week_ending_date}',
            normalised_source_rows.allocation_basis_json->>'week_ending_bucket',
            normalised_source_rows.allocation_basis_json->>'week_ending_date',
            normalised_source_rows.allocation_basis_json#>>'{timesheet,week_ending_date}',
            ''
          )), '') ~ '^\d{4}-\d{2}-\d{2}$'
            THEN NULLIF(BTRIM(COALESCE(
              normalised_source_rows.line_json->>'week_ending_bucket',
              normalised_source_rows.line_json->>'week_ending_date',
              normalised_source_rows.line_json#>>'{timesheet,week_ending_date}',
              normalised_source_rows.allocation_basis_json->>'week_ending_bucket',
              normalised_source_rows.allocation_basis_json->>'week_ending_date',
              normalised_source_rows.allocation_basis_json#>>'{timesheet,week_ending_date}',
              ''
            )), '')::date
          ELSE NULL::date
        END AS resolved_week_ending_bucket
    ) AS payout_source
    CROSS JOIN LATERAL (
      SELECT
        CASE WHEN payout_source.channel_upper = 'UMBRELLA' THEN 'UMBRELLA_COMPANY' ELSE 'NORMAL_PAY_ROUTE' END AS routing_kind,
        CASE WHEN payout_source.channel_upper = 'UMBRELLA' THEN 'UMBRELLA' ELSE 'CANDIDATE' END AS payee_entity_kind,
        CASE WHEN payout_source.channel_upper = 'UMBRELLA' THEN payee_resolution.resolved_umbrella_id ELSE candidate_row.id END AS payee_entity_id,
        CASE WHEN payout_source.channel_upper = 'UMBRELLA' THEN payout_source.umbrella_name ELSE payout_source.candidate_payee_name END AS beneficiary_name,
        CASE WHEN payout_source.channel_upper = 'UMBRELLA' THEN payout_source.umbrella_name ELSE payout_source.candidate_payee_name END AS destination_label,
        CASE WHEN payout_source.channel_upper = 'UMBRELLA' THEN payout_source.umbrella_sort_code ELSE payout_source.candidate_sort_code END AS sort_code,
        CASE WHEN payout_source.channel_upper = 'UMBRELLA' THEN payout_source.umbrella_account_number ELSE payout_source.candidate_account_number END AS account_number,
        CASE WHEN payout_source.channel_upper = 'UMBRELLA' THEN payout_source.umbrella_bank_details_hash ELSE payout_source.candidate_bank_details_hash END AS bank_details_hash,
        payout_source.resolved_week_ending_bucket AS week_ending_bucket,
        CASE
          WHEN payout_source.channel_upper = 'UMBRELLA' AND payee_resolution.resolved_umbrella_id IS NULL THEN 'PAYOUT_INSTRUCTION_UMBRELLA_REQUIRED'
          WHEN payout_source.channel_upper = 'UMBRELLA' AND umbrella_row.id IS NULL THEN 'PAYOUT_INSTRUCTION_UMBRELLA_NOT_FOUND'
          WHEN payout_source.channel_upper = 'UMBRELLA' AND COALESCE(umbrella_row.enabled, false) IS NOT TRUE THEN 'PAYOUT_INSTRUCTION_UMBRELLA_DISABLED'
          WHEN payout_source.channel_upper = 'UMBRELLA' AND (
            payout_source.umbrella_name IS NULL
            OR length(regexp_replace(COALESCE(payout_source.umbrella_sort_code, ''), '[^0-9]', '', 'g')) <> 6
            OR NULLIF(regexp_replace(COALESCE(payout_source.umbrella_account_number, ''), '[^0-9]', '', 'g'), '') IS NULL
            OR payout_source.umbrella_bank_details_hash IS NULL
          ) THEN 'PAYOUT_INSTRUCTION_UMBRELLA_BANK_DETAILS_MISSING'
          WHEN payout_source.channel_upper = 'PAYE' AND (
            payout_source.candidate_payee_name IS NULL
            OR length(regexp_replace(COALESCE(payout_source.candidate_sort_code, ''), '[^0-9]', '', 'g')) <> 6
            OR NULLIF(regexp_replace(COALESCE(payout_source.candidate_account_number, ''), '[^0-9]', '', 'g'), '') IS NULL
            OR payout_source.candidate_bank_details_hash IS NULL
          ) THEN 'PAYOUT_INSTRUCTION_PAYE_BANK_DETAILS_MISSING'
          WHEN payout_source.channel_upper NOT IN ('PAYE', 'UMBRELLA') THEN 'PAYOUT_INSTRUCTION_PAY_CHANNEL_UNSUPPORTED'
          ELSE NULL::text
        END AS payout_instruction_error_code
    ) AS payout_route
    CROSS JOIN LATERAL (
      SELECT public._pay_umbrella_vat_calc(
        normalised_source_rows.amount_ex_vat,
        v_vat_rate_pct,
        payout_source.channel_upper = 'UMBRELLA'
          AND COALESCE(umbrella_row.vat_chargeable, false)
          AND (
            normalised_source_rows.item_type <> 'MANUAL_DEBT_RECOVERY'
            OR normalised_source_rows.component_classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
          )
      ) AS vat_json
    ) AS vat_calculation
  )
  SELECT normalised_rows.*
  FROM normalised_rows;

  IF EXISTS (
    SELECT 1
    FROM pg_temp.tmp_pay_batch_item_normalised_rows AS policy_row
    WHERE policy_row.item_type = 'MANUAL_DEBT_RECOVERY'
      AND (
        UPPER(BTRIM(COALESCE(policy_row.allocation_type, ''))) <> 'MANUAL_DEBT_RECOVERY'
        OR UPPER(BTRIM(COALESCE(policy_row.line_json->>'line_type', ''))) <> 'MANUAL_DEBT_RECOVERY'
        OR UPPER(BTRIM(COALESCE(policy_row.line_json->>'pay_channel', ''))) <> UPPER(BTRIM(COALESCE(policy_row.pay_channel, '')))
        OR UPPER(BTRIM(COALESCE(policy_row.line_json->>'taxability', ''))) NOT IN ('TAXABLE', 'NON_TAXABLE')
        OR UPPER(BTRIM(COALESCE(policy_row.finance_component_json->>'classification', ''))) NOT IN (
          'TAXABLE_CHANNEL_SENSITIVE',
          'NET_PAY_FIXED_RECOVERY',
          'REIMBURSEMENT_GROSS_FIXED'
        )
        OR (
          UPPER(BTRIM(COALESCE(policy_row.finance_component_json->>'classification', ''))) = 'TAXABLE_CHANNEL_SENSITIVE'
          AND UPPER(BTRIM(COALESCE(policy_row.line_json->>'taxability', ''))) <> 'TAXABLE'
        )
        OR (
          UPPER(BTRIM(COALESCE(policy_row.finance_component_json->>'classification', ''))) IN ('NET_PAY_FIXED_RECOVERY', 'REIMBURSEMENT_GROSS_FIXED')
          AND UPPER(BTRIM(COALESCE(policy_row.line_json->>'taxability', ''))) <> 'NON_TAXABLE'
        )
        OR UPPER(BTRIM(COALESCE(policy_row.finance_component_json->>'source_pay_method', ''))) NOT IN ('PAYE', 'UMBRELLA')
        OR jsonb_typeof(policy_row.finance_component_json->'source_basis_json') IS DISTINCT FROM 'object'
        OR policy_row.paye_treatment IS DISTINCT FROM CASE
          WHEN UPPER(BTRIM(COALESCE(policy_row.pay_channel, ''))) <> 'PAYE' THEN 'NONE'
          WHEN policy_row.component_classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum THEN 'GROSS_DEDUCT'
          ELSE 'NET_DEDUCT'
        END
        OR UPPER(BTRIM(COALESCE(policy_row.line_json->>'routing_kind', ''))) IS DISTINCT FROM CASE
          WHEN UPPER(BTRIM(COALESCE(policy_row.pay_channel, ''))) = 'UMBRELLA' THEN 'UMBRELLA_COMPANY'
          ELSE 'NORMAL_PAY_ROUTE'
        END
        OR jsonb_typeof(policy_row.line_json->'appears_on_umbrella_remittance') IS DISTINCT FROM 'boolean'
        OR jsonb_typeof(policy_row.line_json->'generates_candidate_payment_advice') IS DISTINCT FROM 'boolean'
        OR jsonb_typeof(policy_row.line_json->'is_candidate_directed_oneoff_payout') IS DISTINCT FROM 'boolean'
        OR policy_row.line_json->'appears_on_umbrella_remittance' IS DISTINCT FROM to_jsonb(UPPER(BTRIM(COALESCE(policy_row.pay_channel, ''))) = 'UMBRELLA')
        OR policy_row.line_json->'generates_candidate_payment_advice' IS DISTINCT FROM 'false'::jsonb
        OR policy_row.line_json->'is_candidate_directed_oneoff_payout' IS DISTINCT FROM 'false'::jsonb
      )
  ) THEN
    RAISE EXCEPTION 'DRAFT_MANUAL_DEBT_POLICY_TRANSPORT_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'DRAFT_MANUAL_DEBT_POLICY_TRANSPORT_INVALID',
              'operation_id', p_operation_id::text,
              'pay_batch_id', p_pay_batch_id::text,
              'message', 'A selected Manual Debt Recovery did not carry the exact producer-owned tax, channel and component policy into Draft item creation.'
            )::text;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM pg_temp.tmp_pay_batch_item_normalised_rows AS validation_rows
    WHERE validation_rows.payout_instruction_error_code IS NOT NULL
  ) THEN
    RAISE EXCEPTION 'PAYOUT_INSTRUCTION_FREEZE_FAILED'
      USING ERRCODE = 'P0001',
            DETAIL = (
              SELECT jsonb_build_object(
                'code', 'PAYOUT_INSTRUCTION_FREEZE_FAILED',
                'message', COALESCE(
                  MIN(failure_rows.failure_message) FILTER (WHERE failure_rows.failure_message IS NOT NULL),
                  'Draft payout instructions could not be frozen from the resolved payee bank details.'
                ),
                'pay_batch_id', p_pay_batch_id::text,
                'operation_id', p_operation_id::text,
                'first_failure_code', MIN(failure_rows.payout_instruction_error_code) FILTER (WHERE failure_rows.payout_instruction_error_code IS NOT NULL),
                'first_failure_message', MIN(failure_rows.failure_message) FILTER (WHERE failure_rows.failure_message IS NOT NULL),
                'failures', COALESCE(jsonb_agg(jsonb_build_object(
                  'allocation_row_id', failure_rows.allocation_row_id::text,
                  'candidate_id', failure_rows.candidate_id::text,
                  'pay_channel', failure_rows.pay_channel,
                  'umbrella_id', CASE WHEN failure_rows.resolved_umbrella_id IS NULL THEN NULL ELSE failure_rows.resolved_umbrella_id::text END,
                  'code', failure_rows.payout_instruction_error_code,
                  'message', failure_rows.failure_message
                ) ORDER BY failure_rows.allocation_row_id), '[]'::jsonb)
              )::text
              FROM (
                SELECT validation_rows.allocation_row_id,
                       validation_rows.candidate_id,
                       validation_rows.pay_channel,
                       validation_rows.resolved_umbrella_id,
                       validation_rows.payout_instruction_error_code,
                       CASE validation_rows.payout_instruction_error_code
                         WHEN 'PAYOUT_INSTRUCTION_UMBRELLA_REQUIRED' THEN 'Umbrella payment cannot be drafted because the candidate has no resolved umbrella payee.'
                         WHEN 'PAYOUT_INSTRUCTION_UMBRELLA_NOT_FOUND' THEN 'Umbrella payment cannot be drafted because the resolved umbrella payee was not found.'
                         WHEN 'PAYOUT_INSTRUCTION_UMBRELLA_DISABLED' THEN 'Umbrella payment cannot be drafted because the resolved umbrella is disabled.'
                         WHEN 'PAYOUT_INSTRUCTION_UMBRELLA_BANK_DETAILS_MISSING' THEN 'Umbrella payment cannot be drafted because the enabled umbrella is missing mandatory bank details.'
                         WHEN 'PAYOUT_INSTRUCTION_PAYE_BANK_DETAILS_MISSING' THEN 'PAYE payment cannot be drafted because the candidate is missing mandatory bank details.'
                         WHEN 'PAYOUT_INSTRUCTION_PAY_CHANNEL_UNSUPPORTED' THEN 'Payment cannot be drafted because the pay channel is unsupported for bank payout freezing.'
                         ELSE 'Draft payout instructions could not be frozen from the resolved payee bank details.'
                       END AS failure_message
                FROM pg_temp.tmp_pay_batch_item_normalised_rows AS validation_rows
                WHERE validation_rows.payout_instruction_error_code IS NOT NULL
                ORDER BY validation_rows.allocation_row_id
                LIMIT 25
              ) AS failure_rows
            );
  END IF;

  -- This child writer is independently callable. Lock and revalidate the exact
  -- allocation page, then establish the sticky retention marker transactionally
  -- before any durable pay_batch_items row can be created.
  SELECT COUNT(*)::integer
  INTO v_locked_allocation_row_count
  FROM (
    SELECT allocation_current.id
    FROM public.banking_pay_operation_candidate_allocation_rows AS allocation_current
    JOIN pg_temp.tmp_pay_batch_item_normalised_rows AS normalised_rows
      ON normalised_rows.allocation_row_id = allocation_current.id
    ORDER BY allocation_current.id
    FOR UPDATE OF allocation_current
  ) AS locked_allocations;

  IF v_locked_allocation_row_count IS DISTINCT FROM (
    SELECT COUNT(*)::integer
    FROM pg_temp.tmp_pay_batch_item_normalised_rows
  ) THEN
    RAISE EXCEPTION 'DRAFT_ITEM_ALLOCATION_TARGET_SET_CHANGED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'DRAFT_ITEM_ALLOCATION_TARGET_SET_CHANGED',
              'operation_id', p_operation_id::text,
              'pay_batch_id', p_pay_batch_id::text,
              'message', 'The exact draft allocation target set changed before durable item creation. Refresh Banking Pay and try again.'
            )::text;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM pg_temp.tmp_pay_batch_item_normalised_rows AS normalised_rows
    JOIN pg_temp.tmp_pay_batch_item_allocation_page AS allocation_snapshot
      ON allocation_snapshot.id = normalised_rows.allocation_row_id
    JOIN public.banking_pay_operation_candidate_allocation_rows AS allocation_current
      ON allocation_current.id = normalised_rows.allocation_row_id
    WHERE allocation_current.operation_id IS DISTINCT FROM allocation_snapshot.operation_id
       OR allocation_current.candidate_scope_id IS DISTINCT FROM allocation_snapshot.candidate_scope_id
       OR allocation_current.pay_batch_id IS DISTINCT FROM allocation_snapshot.pay_batch_id
       OR allocation_current.candidate_id IS DISTINCT FROM allocation_snapshot.candidate_id
       OR allocation_current.pay_channel IS DISTINCT FROM allocation_snapshot.pay_channel
       OR allocation_current.finance_case_id IS DISTINCT FROM allocation_snapshot.finance_case_id
       OR allocation_current.finance_component_id IS DISTINCT FROM allocation_snapshot.finance_component_id
       OR allocation_current.allocation_type IS DISTINCT FROM allocation_snapshot.allocation_type
       OR allocation_current.source_ref IS DISTINCT FROM allocation_snapshot.source_ref
       OR allocation_current.operation_source_key IS DISTINCT FROM allocation_snapshot.operation_source_key
       OR allocation_current.allocated_amount IS DISTINCT FROM allocation_snapshot.allocated_amount
       OR allocation_current.allocation_basis_json IS DISTINCT FROM allocation_snapshot.source_allocation_basis_json
       OR allocation_current.status IS DISTINCT FROM allocation_snapshot.status
       OR UPPER(BTRIM(COALESCE(allocation_current.status, ''))) NOT IN ('PENDING', 'ITEM_PENDING')
       OR allocation_current.operation_id IS DISTINCT FROM p_operation_id
       OR allocation_current.candidate_scope_id IS DISTINCT FROM normalised_rows.candidate_scope_id
       OR allocation_current.candidate_id IS DISTINCT FROM normalised_rows.candidate_id
       OR allocation_current.operation_source_key IS DISTINCT FROM normalised_rows.operation_source_key
       OR allocation_snapshot.canonical_timesheet_id IS DISTINCT FROM normalised_rows.timesheet_id
  ) THEN
    RAISE EXCEPTION 'DRAFT_ITEM_ALLOCATION_IDENTITY_CHANGED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'DRAFT_ITEM_ALLOCATION_IDENTITY_CHANGED',
              'operation_id', p_operation_id::text,
              'pay_batch_id', p_pay_batch_id::text,
              'message', 'A selected draft allocation changed identity or state before durable item creation. Refresh Banking Pay and try again.'
            )::text;
  END IF;

  -- Every exact ordinary allocation candidate/scope must have been represented
  -- in the authoritative snooze guard before retention or durable item creation.
  IF EXISTS (
    SELECT 1
    FROM pg_temp.tmp_pay_batch_item_normalised_rows AS normalised_rows
    WHERE NOT EXISTS (
      SELECT 1
      FROM pg_temp._bpay_draft_snooze_guard_scope AS guard_scope
      WHERE guard_scope.candidate_scope_id = normalised_rows.candidate_scope_id
        AND guard_scope.candidate_id = normalised_rows.candidate_id
    )
  ) THEN
    RAISE EXCEPTION 'DRAFT_ITEM_SNOOZE_GUARD_SCOPE_MISMATCH'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'DRAFT_ITEM_SNOOZE_GUARD_SCOPE_MISMATCH',
              'operation_id', p_operation_id::text,
              'workbench_session_id', v_workbench_session_id::text,
              'pay_batch_id', p_pay_batch_id::text,
              'guarded_selected_line_count', v_snooze_guard_selected_line_count,
              'refresh_required', true,
              'next_action', 'REFRESH_WORKBENCH',
              'message', 'One or more final allocation candidates were not covered by the live snooze authority. Refresh Banking Pay before creating items.'
            )::text;
  END IF;

  SELECT COALESCE(
           ARRAY_AGG(DISTINCT normalised_rows.timesheet_id ORDER BY normalised_rows.timesheet_id),
           ARRAY[]::uuid[]
         )
  INTO v_retention_timesheet_ids
  FROM pg_temp.tmp_pay_batch_item_normalised_rows AS normalised_rows
  WHERE normalised_rows.timesheet_id IS NOT NULL;

  IF COALESCE(ARRAY_LENGTH(v_retention_timesheet_ids, 1), 0) > 0 THEN
    -- Re-resolve after the allocation-row lock in case that lock acquisition waited.
    IF EXISTS (
      WITH requested_timesheets AS (
        SELECT requested.timesheet_id
        FROM unnest(v_retention_timesheet_ids) AS requested(timesheet_id)
      ), current_rotation AS (
        SELECT DISTINCT ON (rotation_scope.requested_timesheet_id)
          rotation_scope.requested_timesheet_id,
          rotation_scope.canonical_timesheet_id,
          rotation_scope.family_timesheet_id,
          rotation_scope.family_is_current,
          rotation_scope.requested_is_canonical
        FROM public._pay_timesheet_rotation_scope(v_retention_timesheet_ids) AS rotation_scope
        ORDER BY
          rotation_scope.requested_timesheet_id,
          rotation_scope.family_is_current DESC NULLS LAST,
          rotation_scope.family_version DESC NULLS LAST,
          rotation_scope.family_timesheet_id
      )
      SELECT 1
      FROM requested_timesheets AS requested
      LEFT JOIN current_rotation AS rotation_scope
        ON rotation_scope.requested_timesheet_id = requested.timesheet_id
      WHERE rotation_scope.requested_timesheet_id IS NULL
         OR rotation_scope.canonical_timesheet_id IS DISTINCT FROM requested.timesheet_id
         OR COALESCE(rotation_scope.requested_is_canonical, false) IS NOT TRUE
         OR rotation_scope.family_timesheet_id IS NULL
         OR rotation_scope.family_is_current IS NULL
    ) THEN
      RAISE EXCEPTION 'DRAFT_ITEM_ROTATED_ALLOCATION_STALE'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'DRAFT_ITEM_ROTATED_ALLOCATION_STALE',
                'pay_batch_id', p_pay_batch_id::text,
                'operation_id', p_operation_id::text,
                'message', 'One or more allocation rows no longer resolve to the same current canonical timesheet. Refresh draft scope before creating batch items.'
              )::text;
    END IF;

    -- The helper deterministically locks every exact persisted timesheet identity
    -- and inserts the sticky marker in this same transaction.
    v_retention_mark_json := public.timesheet_financial_retention_mark_v1(v_retention_timesheet_ids);

    -- The marker helper may have waited on a timesheet lock. Revalidate current
    -- rotation after that wait; any failure rolls back both marker and item work.
    IF EXISTS (
      WITH requested_timesheets AS (
        SELECT requested.timesheet_id
        FROM unnest(v_retention_timesheet_ids) AS requested(timesheet_id)
      ), current_rotation AS (
        SELECT DISTINCT ON (rotation_scope.requested_timesheet_id)
          rotation_scope.requested_timesheet_id,
          rotation_scope.canonical_timesheet_id,
          rotation_scope.family_timesheet_id,
          rotation_scope.family_is_current,
          rotation_scope.requested_is_canonical
        FROM public._pay_timesheet_rotation_scope(v_retention_timesheet_ids) AS rotation_scope
        ORDER BY
          rotation_scope.requested_timesheet_id,
          rotation_scope.family_is_current DESC NULLS LAST,
          rotation_scope.family_version DESC NULLS LAST,
          rotation_scope.family_timesheet_id
      )
      SELECT 1
      FROM requested_timesheets AS requested
      LEFT JOIN current_rotation AS rotation_scope
        ON rotation_scope.requested_timesheet_id = requested.timesheet_id
      WHERE rotation_scope.requested_timesheet_id IS NULL
         OR rotation_scope.canonical_timesheet_id IS DISTINCT FROM requested.timesheet_id
         OR COALESCE(rotation_scope.requested_is_canonical, false) IS NOT TRUE
         OR rotation_scope.family_timesheet_id IS NULL
         OR rotation_scope.family_is_current IS NULL
    ) THEN
      RAISE EXCEPTION 'DRAFT_ITEM_ROTATED_ALLOCATION_STALE'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'DRAFT_ITEM_ROTATED_ALLOCATION_STALE',
                'pay_batch_id', p_pay_batch_id::text,
                'operation_id', p_operation_id::text,
                'message', 'One or more allocation rows changed current canonical timesheet while retention was being established. Refresh draft scope before creating batch items.'
              )::text;
    END IF;
  END IF;

  -- Revalidate operation, session, batch and candidate-scope authority after
  -- allocation and timesheet/retention waits. These rows were locked earlier;
  -- this check also protects against any change made by this transaction.
  IF NOT EXISTS (
    SELECT 1
    FROM public.banking_pay_operations AS operation_row
    WHERE operation_row.id = p_operation_id
      AND UPPER(BTRIM(COALESCE(operation_row.operation_type, ''))) = 'DRAFT_CREATE'
      AND UPPER(BTRIM(COALESCE(operation_row.status, ''))) = 'RUNNING'
      AND UPPER(BTRIM(COALESCE(operation_row.phase, ''))) = 'INSERT_ITEMS'
      AND operation_row.actor_user_id = p_actor_user_id
      AND operation_row.workbench_session_id = v_workbench_session_id
  ) THEN
    RAISE EXCEPTION 'DRAFT_ITEM_OPERATION_AUTHORITY_CHANGED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'DRAFT_ITEM_OPERATION_AUTHORITY_CHANGED',
              'operation_id', p_operation_id::text,
              'workbench_session_id', v_workbench_session_id::text,
              'message', 'The active DRAFT_CREATE operation authority changed before durable item creation.'
            )::text;
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM public.banking_pay_workbench_sessions AS session_row
    WHERE session_row.id = v_workbench_session_id
      AND UPPER(BTRIM(COALESCE(session_row.status, ''))) = 'OPEN'
      AND session_row.discarded_at_utc IS NULL
      AND session_row.replacement_session_id IS NULL
      AND session_row.version IS NOT DISTINCT FROM v_session_version
      AND session_row.source_snapshot_run_id IS NOT DISTINCT FROM v_session_source_snapshot_run_id
  ) THEN
    RAISE EXCEPTION 'DRAFT_ITEM_WORKBENCH_SESSION_AUTHORITY_CHANGED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'DRAFT_ITEM_WORKBENCH_SESSION_AUTHORITY_CHANGED',
              'operation_id', p_operation_id::text,
              'workbench_session_id', v_workbench_session_id::text,
              'refresh_required', true,
              'next_action', 'REFRESH_WORKBENCH',
              'message', 'The Workbench session authority changed before durable item creation.'
            )::text;
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM public.pay_batches AS batch_row
    WHERE batch_row.id = p_pay_batch_id
      AND UPPER(BTRIM(COALESCE(batch_row.status, ''))) = 'DRAFT'
      AND UPPER(BTRIM(COALESCE(batch_row.execution_commit_state, ''))) = 'NOT_SUBMITTED'
      AND batch_row.source_workbench_session_id = v_workbench_session_id
      AND batch_row.source_snapshot_run_id IS NOT DISTINCT FROM v_session_source_snapshot_run_id
      AND batch_row.source_session_version IS NOT DISTINCT FROM v_session_version
  ) THEN
    RAISE EXCEPTION 'DRAFT_ITEM_PAY_BATCH_AUTHORITY_CHANGED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'DRAFT_ITEM_PAY_BATCH_AUTHORITY_CHANGED',
              'operation_id', p_operation_id::text,
              'pay_batch_id', p_pay_batch_id::text,
              'message', 'The draft pay batch authority changed before durable item creation.'
            )::text;
  END IF;

  SELECT COUNT(*)::integer
  INTO v_locked_candidate_scope_count
  FROM public.banking_pay_operation_candidate_scope AS scope_row
  WHERE scope_row.id IN (
    SELECT supplied_scope.candidate_scope_id_text::uuid
    FROM jsonb_array_elements_text(v_candidate_scope_ids)
      AS supplied_scope(candidate_scope_id_text)
  )
    AND scope_row.operation_id = p_operation_id
    AND scope_row.workbench_session_id = v_workbench_session_id
    AND scope_row.pay_batch_id = p_pay_batch_id
    AND UPPER(BTRIM(COALESCE(scope_row.status, ''))) IN ('SCOPED', 'ALLOCATED', 'DRAFTED')
    AND scope_row.source_session_version IS NOT DISTINCT FROM v_session_version
    AND scope_row.source_snapshot_run_id IS NOT DISTINCT FROM v_session_source_snapshot_run_id;

  IF v_locked_candidate_scope_count IS DISTINCT FROM v_scope_id_count THEN
    RAISE EXCEPTION 'DRAFT_ITEM_CANDIDATE_SCOPE_AUTHORITY_CHANGED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'DRAFT_ITEM_CANDIDATE_SCOPE_AUTHORITY_CHANGED',
              'operation_id', p_operation_id::text,
              'workbench_session_id', v_workbench_session_id::text,
              'pay_batch_id', p_pay_batch_id::text,
              'candidate_scope_ids', v_candidate_scope_ids,
              'refresh_required', true,
              'next_action', 'REFRESH_WORKBENCH',
              'message', 'Candidate-scope authority changed before durable item creation.'
            )::text;
  END IF;

  -- Option B: repeat the authoritative snooze check after retention marking and
  -- all post-wait identity validation, immediately before the first item insert.
  -- Any conflict rolls the retention marker and item work back together.
  v_snooze_guard_json := private.pay_workbench_operation_active_snoozes_v8(
    p_operation_id,
    v_workbench_session_id,
    v_candidate_scope_ids
  );

  IF LOWER(BTRIM(COALESCE(v_snooze_guard_json->>'ok', 'false'))) NOT IN ('true', 't', '1') THEN
    RAISE EXCEPTION 'DRAFT_ITEM_FINAL_SNOOZE_GUARD_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'DRAFT_ITEM_FINAL_SNOOZE_GUARD_INVALID',
              'operation_id', p_operation_id::text,
              'workbench_session_id', v_workbench_session_id::text,
              'message', 'The final live snooze authority returned an invalid result.'
            )::text;
  END IF;

  v_snooze_guard_selected_line_count := CASE
    WHEN COALESCE(v_snooze_guard_json->>'selected_line_count', '') ~ '^[0-9]{1,9}$'
      THEN (v_snooze_guard_json->>'selected_line_count')::integer
    ELSE 0
  END;
  v_active_snooze_count := CASE
    WHEN COALESCE(v_snooze_guard_json->>'active_snooze_count', '') ~ '^[0-9]{1,9}$'
      THEN (v_snooze_guard_json->>'active_snooze_count')::integer
    ELSE 0
  END;

  IF v_active_snooze_count > 0 THEN
    RAISE EXCEPTION 'ACTIVE_SNOOZE_PREVENTS_DRAFT'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_strip_nulls(
              jsonb_build_object(
                'code', 'ACTIVE_SNOOZE_PREVENTS_DRAFT',
                'message', 'One or more selected payments are currently snoozed. Banking Pay has been refreshed so you can review the updated selection.',
                'operation_id', p_operation_id::text,
                'workbench_session_id', v_workbench_session_id::text,
                'pay_batch_id', p_pay_batch_id::text,
                'refresh_required', true,
                'next_action', 'REFRESH_WORKBENCH'
              )
              || COALESCE(v_snooze_guard_json, '{}'::jsonb)
            )::text;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM pg_temp.tmp_pay_batch_item_normalised_rows AS normalised_rows
    WHERE NOT EXISTS (
      SELECT 1
      FROM pg_temp._bpay_draft_snooze_guard_scope AS guard_scope
      WHERE guard_scope.candidate_scope_id = normalised_rows.candidate_scope_id
        AND guard_scope.candidate_id = normalised_rows.candidate_id
    )
  ) THEN
    RAISE EXCEPTION 'DRAFT_ITEM_FINAL_SNOOZE_GUARD_SCOPE_MISMATCH'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'DRAFT_ITEM_FINAL_SNOOZE_GUARD_SCOPE_MISMATCH',
              'operation_id', p_operation_id::text,
              'workbench_session_id', v_workbench_session_id::text,
              'pay_batch_id', p_pay_batch_id::text,
              'guarded_selected_line_count', v_snooze_guard_selected_line_count,
              'refresh_required', true,
              'next_action', 'REFRESH_WORKBENCH',
              'message', 'One or more final allocation candidates were not covered by the final live snooze authority.'
            )::text;
  END IF;

  PERFORM private.pay_workbench_draft_expected_effects_v1(
    p_operation_id,'INSERT_ITEMS','REGISTER',
    jsonb_build_array(
      jsonb_build_object('relation_name','pay_batch_items','operation','INSERT'),
      jsonb_build_object('relation_name','pay_batch_items','operation','UPDATE')
    ),jsonb_build_object('pay_batch_id',p_pay_batch_id)
  );

  WITH inserted_items AS (
    INSERT INTO public.pay_batch_items(
      pay_batch_candidate_id,
      item_type,
      timesheet_id,
      segment_key,
      source_ref,
      description,
      amount_ex_vat,
      amount_vat,
      amount_inc_vat,
      pay_channel,
      umbrella_id,
      is_mismatch,
      finance_case_id,
      finance_component_id,
      frozen_component_snapshot_json,
      frozen_component_key_type,
      frozen_component_key_value,
      frozen_component_classification,
      frozen_source_basis_json,
      frozen_source_pay_method,
      frozen_target_pay_method,
      frozen_resolution_mode,
      frozen_resolution_payload_json,
      frozen_resolution_result_json,
      frozen_source_amount,
      frozen_target_amount_ex_vat,
      frozen_target_amount_vat,
      frozen_target_amount_inc_vat,
      payout_instruction_snapshot_json,
      paye_treatment,
      operation_source_key
    )
    SELECT
      pay_batch_candidate.id,
      normalised_rows.item_type,
      normalised_rows.timesheet_id,
      normalised_rows.segment_key,
      COALESCE(NULLIF(BTRIM(normalised_rows.source_ref), ''), normalised_rows.allocation_basis_json->>'row_key', normalised_rows.operation_source_key),
      COALESCE(NULLIF(BTRIM(normalised_rows.line_json->>'description'), ''), NULLIF(BTRIM(normalised_rows.line_json->>'label'), ''), normalised_rows.item_type),
      normalised_rows.amount_ex_vat,
      normalised_rows.amount_vat,
      normalised_rows.amount_inc_vat,
      normalised_rows.pay_channel,
      normalised_rows.resolved_umbrella_id,
      CASE
        WHEN normalised_rows.item_type = 'MANUAL_DEBT_RECOVERY'
         AND normalised_rows.component_classification = 'TAXABLE_CHANNEL_SENSITIVE'::public.pay_finance_component_classification_enum
          THEN UPPER(BTRIM(COALESCE(normalised_rows.finance_component_json->>'source_pay_method', ''))) <> UPPER(BTRIM(COALESCE(normalised_rows.pay_channel, '')))
        ELSE false
      END,
      normalised_rows.finance_case_id,
      normalised_rows.finance_component_id,
      jsonb_strip_nulls(
        CASE
          WHEN normalised_rows.item_type = 'MANUAL_DEBT_RECOVERY' THEN normalised_rows.finance_component_json
          ELSE COALESCE(normalised_rows.line_json->'frozen_component_snapshot_json', normalised_rows.line_json->'component_snapshot_json', '{}'::jsonb)
        END
        || jsonb_build_object(
          'source', 'banking_pay_operation_candidate_allocation_rows',
          'allocation_row_id', normalised_rows.allocation_row_id::text,
          'operation_source_key', normalised_rows.operation_source_key,
          'timesheet_id', CASE WHEN normalised_rows.timesheet_id IS NULL THEN NULL ELSE normalised_rows.timesheet_id::text END,
          'component_key_type', normalised_rows.key_type,
          'component_key_value', normalised_rows.key_value,
          'source_family_key', NULLIF(BTRIM(COALESCE(normalised_rows.line_json->>'source_family_key', '')), ''),
          'correction_chain_residual', CASE
            WHEN NULLIF(BTRIM(COALESCE(normalised_rows.line_json->>'source_family_key', '')), '') LIKE 'correction-chain:%'
             AND jsonb_typeof(normalised_rows.line_json->'correction_chain_residual') = 'object'
              THEN normalised_rows.line_json->'correction_chain_residual'
            ELSE NULL
          END
        )
        || CASE
          WHEN normalised_rows.item_type = 'MANUAL_DEBT_RECOVERY' THEN jsonb_build_object(
            'frozen_target_pay_method', normalised_rows.pay_channel,
            'frozen_source_amount', normalised_rows.frozen_source_amount,
            'frozen_target_amount_ex_vat', normalised_rows.amount_ex_vat,
            'frozen_target_amount_vat', normalised_rows.amount_vat,
            'frozen_target_amount_inc_vat', normalised_rows.amount_inc_vat,
            'selected_preview_due_amount_ex_vat', ABS((normalised_rows.line_json->>'amount_ex_vat')::numeric),
            'component_preview_due_amount_ex_vat', ABS(normalised_rows.amount_ex_vat)
          )
          ELSE '{}'::jsonb
        END
        || CASE
          WHEN normalised_rows.item_type IN ('SEGMENT_DELTA', 'EXPENSE_DELTA', 'ADJUSTMENT_DELTA', 'MILEAGE_DELTA')
           AND ROUND(COALESCE(normalised_rows.amount_ex_vat, 0), 2) > 0
           AND ROUND(ABS(COALESCE(normalised_rows.frozen_source_amount, 0)), 2) > 0 THEN jsonb_build_object(
            'source_pay_ex_vat', normalised_rows.frozen_source_amount,
            'source_amount_ex_vat', normalised_rows.frozen_source_amount,
            'source_entitlement_amount_ex_vat', normalised_rows.frozen_source_amount,
            'source_reservation_amount_ex_vat', normalised_rows.frozen_source_amount,
            'frozen_source_amount', normalised_rows.frozen_source_amount
          )
          ELSE '{}'::jsonb
        END
        || CASE
          WHEN normalised_rows.item_type IN ('SEGMENT_DELTA', 'EXPENSE_DELTA', 'ADJUSTMENT_DELTA', 'MILEAGE_DELTA')
           AND ROUND(COALESCE(normalised_rows.amount_ex_vat, 0), 2) > 0
           AND ROUND(ABS(COALESCE(normalised_rows.frozen_source_amount, 0)), 2) > 0 THEN jsonb_build_object(
            'source_basis_json',
            COALESCE(
              normalised_rows.line_json#>'{frozen_component_snapshot_json,source_basis_json}',
              normalised_rows.line_json#>'{component_snapshot_json,source_basis_json}',
              normalised_rows.line_json->'source_basis_json',
              '{}'::jsonb
            )
            || jsonb_build_object(
              'source_pay_ex_vat', normalised_rows.frozen_source_amount,
              'source_amount_ex_vat', normalised_rows.frozen_source_amount,
              'source_entitlement_amount_ex_vat', normalised_rows.frozen_source_amount,
              'source_reservation_amount_ex_vat', normalised_rows.frozen_source_amount
            )
          )
          ELSE '{}'::jsonb
        END
      ),
      normalised_rows.key_type,
      normalised_rows.key_value,
      normalised_rows.component_classification,
      jsonb_strip_nulls(
        CASE
          WHEN normalised_rows.item_type = 'MANUAL_DEBT_RECOVERY'
            THEN COALESCE(normalised_rows.finance_component_json->'source_basis_json', '{}'::jsonb)
          ELSE COALESCE(normalised_rows.line_json->'source_basis_json', normalised_rows.line_json#>'{economic_key}', '{}'::jsonb)
        END
        || jsonb_build_object(
          'source', 'banking_pay_workbench_preview_rows',
          'allocation_row_id', normalised_rows.allocation_row_id::text,
          'timesheet_id', CASE WHEN normalised_rows.timesheet_id IS NULL THEN NULL ELSE normalised_rows.timesheet_id::text END,
          'key_type', normalised_rows.key_type,
          'key_value', normalised_rows.key_value,
          'source_family_key', NULLIF(BTRIM(COALESCE(normalised_rows.line_json->>'source_family_key', '')), ''),
          'correction_chain_residual', CASE
            WHEN NULLIF(BTRIM(COALESCE(normalised_rows.line_json->>'source_family_key', '')), '') LIKE 'correction-chain:%'
             AND jsonb_typeof(normalised_rows.line_json->'correction_chain_residual') = 'object'
              THEN normalised_rows.line_json->'correction_chain_residual'
            ELSE NULL
          END
        )
        || CASE
          WHEN normalised_rows.item_type IN ('SEGMENT_DELTA', 'EXPENSE_DELTA', 'ADJUSTMENT_DELTA', 'MILEAGE_DELTA')
           AND ROUND(COALESCE(normalised_rows.amount_ex_vat, 0), 2) > 0
           AND ROUND(ABS(COALESCE(normalised_rows.frozen_source_amount, 0)), 2) > 0 THEN jsonb_build_object(
            'source_pay_ex_vat', normalised_rows.frozen_source_amount,
            'source_amount_ex_vat', normalised_rows.frozen_source_amount,
            'source_entitlement_amount_ex_vat', normalised_rows.frozen_source_amount,
            'source_reservation_amount_ex_vat', normalised_rows.frozen_source_amount
          )
          ELSE '{}'::jsonb
        END
      ),
      CASE
        WHEN normalised_rows.item_type = 'MANUAL_DEBT_RECOVERY'
          THEN UPPER(NULLIF(BTRIM(normalised_rows.finance_component_json->>'source_pay_method'), ''))
        ELSE COALESCE(NULLIF(UPPER(BTRIM(normalised_rows.line_json->>'source_pay_method')), ''), normalised_rows.pay_channel)
      END,
      normalised_rows.pay_channel,
      normalised_rows.resolution_mode,
      CASE
        WHEN normalised_rows.item_type = 'MANUAL_DEBT_RECOVERY'
         AND jsonb_typeof(normalised_rows.finance_component_json->'saved_resolution_payload_json') = 'object'
          THEN normalised_rows.finance_component_json->'saved_resolution_payload_json'
        WHEN normalised_rows.item_type = 'MANUAL_DEBT_RECOVERY' THEN NULL::jsonb
        WHEN jsonb_typeof(normalised_rows.line_json->'resolution_payload_json') = 'object' THEN normalised_rows.line_json->'resolution_payload_json'
        ELSE normalised_rows.line_json
      END,
      jsonb_strip_nulls(
        CASE
          WHEN normalised_rows.item_type = 'MANUAL_DEBT_RECOVERY'
            THEN COALESCE(normalised_rows.finance_component_json->'saved_resolution_result_json', '{}'::jsonb)
          ELSE COALESCE(normalised_rows.line_json->'resolution_result_json', '{}'::jsonb)
        END
        || jsonb_build_object(
          'source_amount_ex_vat', normalised_rows.frozen_source_amount,
          'target_amount_ex_vat', normalised_rows.amount_ex_vat,
          'target_amount_vat', normalised_rows.amount_vat,
          'target_amount_inc_vat', normalised_rows.amount_inc_vat
        )
      ),
      normalised_rows.frozen_source_amount,
      normalised_rows.amount_ex_vat,
      normalised_rows.amount_vat,
      normalised_rows.amount_inc_vat,
      normalised_rows.payout_instruction_snapshot_json,
      normalised_rows.paye_treatment,
      normalised_rows.operation_source_key
    FROM pg_temp.tmp_pay_batch_item_normalised_rows AS normalised_rows
    JOIN public.pay_batch_candidates AS pay_batch_candidate
      ON pay_batch_candidate.pay_batch_id = p_pay_batch_id
     AND pay_batch_candidate.candidate_id = normalised_rows.candidate_id
    ON CONFLICT (pay_batch_candidate_id, operation_source_key) WHERE operation_source_key IS NOT NULL DO NOTHING
    RETURNING public.pay_batch_items.id, public.pay_batch_items.operation_source_key
  )
  SELECT COUNT(*)::integer
  INTO v_inserted_count
  FROM inserted_items;

  WITH matched_items AS (
    SELECT allocation_row.id AS allocation_row_id,
           pay_batch_item.id AS pay_batch_item_id
    FROM pg_temp.tmp_pay_batch_item_allocation_page AS allocation_row
    JOIN public.banking_pay_operation_candidate_scope AS scope_row
      ON scope_row.id = allocation_row.candidate_scope_id
    JOIN public.pay_batch_candidates AS pay_batch_candidate
      ON pay_batch_candidate.pay_batch_id = p_pay_batch_id
     AND pay_batch_candidate.candidate_id = allocation_row.candidate_id
    JOIN public.pay_batch_items AS pay_batch_item
      ON pay_batch_item.pay_batch_candidate_id = pay_batch_candidate.id
     AND pay_batch_item.operation_source_key = allocation_row.operation_source_key
     AND COALESCE(pay_batch_item.is_voided, false) = false
     AND NOT (UPPER(BTRIM(COALESCE(allocation_row.allocation_type, ''))) = ANY(v_deferred_finance_aliases))
  ), repaired_item_snapshots AS (
    UPDATE public.pay_batch_items AS item_update
    SET payout_instruction_snapshot_json = normalised_rows.payout_instruction_snapshot_json,
        updated_at = v_now
    FROM matched_items
    JOIN pg_temp.tmp_pay_batch_item_normalised_rows AS normalised_rows
      ON normalised_rows.allocation_row_id = matched_items.allocation_row_id
    WHERE item_update.id = matched_items.pay_batch_item_id
      AND item_update.payout_instruction_snapshot_json IS NULL
    RETURNING item_update.id
  ), updated_allocations AS (
    UPDATE public.banking_pay_operation_candidate_allocation_rows AS allocation_update
    SET status = 'ITEM_CREATED',
        pay_batch_id = p_pay_batch_id,
        pay_batch_item_id = matched_items.pay_batch_item_id,
        updated_at_utc = v_now
    FROM matched_items
    WHERE allocation_update.id = matched_items.allocation_row_id
    RETURNING allocation_update.id
  )
  SELECT COUNT(*)::integer,
         GREATEST(COUNT(*)::integer - COALESCE(v_inserted_count, 0), 0)
  INTO v_linked_allocation_row_count, v_reused_count
  FROM updated_allocations;

  IF COALESCE(v_linked_allocation_row_count, 0) <= 0
     AND COALESCE(v_ordinary_page_allocation_row_count, 0) > 0 THEN
    RAISE EXCEPTION 'DRAFT_ITEM_INSERTION_EMPTY'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'DRAFT_ITEM_INSERTION_EMPTY',
              'pay_batch_id', p_pay_batch_id::text,
              'operation_id', p_operation_id::text,
              'candidate_scope_count', v_scope_id_count,
              'candidate_scope_ids', COALESCE(v_candidate_scope_ids, '[]'::jsonb),
              'allocation_row_count', COALESCE(v_expected_allocation_row_count, 0),
              'page_allocation_row_count', COALESCE(v_page_allocation_row_count, 0),
              'linked_allocation_rows', COALESCE(v_linked_allocation_row_count, 0),
              'repaired_existing_item_links', COALESCE(v_repaired_existing_item_link_count, 0),
              'inserted_item_rows', COALESCE(v_inserted_count, 0),
              'reused_item_rows', COALESCE(v_reused_count, 0),
              'message', 'Draft item insertion produced no linked batch items for row-backed selected allocation rows.'
            )::text;
  END IF;

  IF COALESCE(v_linked_allocation_row_count, 0) <> COALESCE(v_ordinary_page_allocation_row_count, 0) THEN
    RAISE EXCEPTION 'DRAFT_ITEM_LINKAGE_INCOMPLETE'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'DRAFT_ITEM_LINKAGE_INCOMPLETE',
              'pay_batch_id', p_pay_batch_id::text,
              'operation_id', p_operation_id::text,
              'candidate_scope_count', v_scope_id_count,
              'candidate_scope_ids', COALESCE(v_candidate_scope_ids, '[]'::jsonb),
              'allocation_row_count', COALESCE(v_expected_allocation_row_count, 0),
              'page_allocation_row_count', COALESCE(v_page_allocation_row_count, 0),
              'linked_allocation_rows', COALESCE(v_linked_allocation_row_count, 0),
              'repaired_existing_item_links', COALESCE(v_repaired_existing_item_link_count, 0),
              'inserted_item_rows', COALESCE(v_inserted_count, 0),
              'reused_item_rows', COALESCE(v_reused_count, 0),
              'message', 'One or more row-backed allocation rows were not linked to draft items.'
            )::text;
  END IF;

  UPDATE public.banking_pay_operation_candidate_scope AS scope_update
  SET pay_batch_id = p_pay_batch_id,
      status = CASE
        WHEN NOT EXISTS (
          SELECT 1
          FROM public.banking_pay_operation_candidate_allocation_rows AS allocation_remaining
          WHERE allocation_remaining.candidate_scope_id = scope_update.id
            AND UPPER(BTRIM(COALESCE(allocation_remaining.status, ''))) IN ('PENDING', 'ITEM_PENDING')
        ) THEN 'DRAFTED'
        ELSE scope_update.status
      END,
      updated_at_utc = v_now
  WHERE scope_update.operation_id = p_operation_id
    AND scope_update.id IN (
      SELECT (supplied_scope.scope_value #>> '{}')::uuid
      FROM jsonb_array_elements(v_scope_ids) AS supplied_scope(scope_value)
    );

  PERFORM private.pay_workbench_draft_expected_effects_v1(
    p_operation_id,'INSERT_ITEMS','ASSERT_COMPLETE','[]'::jsonb,
    jsonb_build_object('pay_batch_id',p_pay_batch_id)
  );

  RETURN jsonb_build_object(
    'ok', true,
    'pay_batch_id', p_pay_batch_id::text,
    'operation_id', p_operation_id::text,
    'candidate_scope_count', v_scope_id_count,
    'candidate_scope_ids', COALESCE(v_candidate_scope_ids, '[]'::jsonb),
    'allocation_row_count', COALESCE(v_expected_allocation_row_count, 0),
    'page_allocation_row_count', COALESCE(v_page_allocation_row_count, 0),
    'ordinary_page_allocation_row_count', COALESCE(v_ordinary_page_allocation_row_count, 0),
    'deferred_finance_adjustment_rows', COALESCE(v_deferred_finance_adjustment_row_count, 0),
    'linked_allocation_rows', COALESCE(v_linked_allocation_row_count, 0),
    'repaired_existing_item_links', COALESCE(v_repaired_existing_item_link_count, 0),
    'inserted_item_rows', COALESCE(v_inserted_count, 0),
    'reused_item_rows', COALESCE(v_reused_count, 0),
    'skipped_item_rows', COALESCE(v_deferred_finance_adjustment_row_count, 0),
    'failed_item_rows', COALESCE(v_failed_count, 0),
    'retention_mark', COALESCE(v_retention_mark_json, '{}'::jsonb),
    'has_more', EXISTS (
      SELECT 1
      FROM public.banking_pay_operation_candidate_allocation_rows AS remaining_row
      WHERE remaining_row.operation_id = p_operation_id
        AND remaining_row.candidate_scope_id IN (
          SELECT (supplied_scope.scope_value #>> '{}')::uuid
          FROM jsonb_array_elements(v_scope_ids) AS supplied_scope(scope_value)
        )
        AND UPPER(BTRIM(COALESCE(remaining_row.status, ''))) IN ('PENDING', 'ITEM_PENDING')
        AND NOT (UPPER(BTRIM(COALESCE(remaining_row.allocation_type, ''))) = ANY(v_deferred_finance_aliases))
    )
  );
END;
$function$;





REVOKE ALL ON FUNCTION public.pay_batch_insert_items_from_preview(uuid, uuid, uuid, jsonb)
  FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.pay_batch_insert_items_from_preview(uuid, uuid, uuid, jsonb)
  TO service_role;
