CREATE OR REPLACE FUNCTION public.pay_workbench_revalidate_zero_retained_recovery_headroom_v1(
  p_session_id uuid,
  p_candidate_id uuid
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamptz := clock_timestamp();
  v_session_row public.banking_pay_workbench_sessions%ROWTYPE;
  v_nonterminal_line_count integer := 0;
  v_retained_positive_headroom numeric(12,2) := 0;
  v_recovery_group_count integer := 0;
  v_demoted_count integer := 0;
  v_superseded_count integer := 0;
  v_line_work_revalidated_count integer := 0;
  v_selected_preview_row_ids jsonb := '[]'::jsonb;
  v_selected_row_count integer := 0;
  v_selection_intent_mode text := '';
  v_progress_json jsonb := '{}'::jsonb;
BEGIN
  PERFORM public.banking_pay_hot_path_budget_apply('WORKBENCH_CHUNK');

  IF p_session_id IS NULL THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_RECOVERY_HEADROOM_SESSION_ID_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'PAY_WORKBENCH_RECOVERY_HEADROOM_SESSION_ID_REQUIRED')::text;
  END IF;

  IF p_candidate_id IS NULL THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_RECOVERY_HEADROOM_CANDIDATE_ID_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object('code', 'PAY_WORKBENCH_RECOVERY_HEADROOM_CANDIDATE_ID_REQUIRED')::text;
  END IF;

  SELECT session_row.*
  INTO v_session_row
  FROM public.banking_pay_workbench_sessions AS session_row
  WHERE session_row.id = p_session_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_RECOVERY_HEADROOM_SESSION_NOT_FOUND'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_RECOVERY_HEADROOM_SESSION_NOT_FOUND',
              'session_id', p_session_id::text
            )::text;
  END IF;

  IF UPPER(BTRIM(COALESCE(v_session_row.status, ''))) <> 'OPEN'
     OR v_session_row.discarded_at_utc IS NOT NULL THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_RECOVERY_HEADROOM_SESSION_NOT_OPEN'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_RECOVERY_HEADROOM_SESSION_NOT_OPEN',
              'session_id', p_session_id::text,
              'status', v_session_row.status
            )::text;
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM public.banking_pay_workbench_session_scope AS scope_row
    WHERE scope_row.session_id = p_session_id
      AND scope_row.candidate_id = p_candidate_id
  ) THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_RECOVERY_HEADROOM_CANDIDATE_NOT_IN_SCOPE'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'code', 'PAY_WORKBENCH_RECOVERY_HEADROOM_CANDIDATE_NOT_IN_SCOPE',
              'session_id', p_session_id::text,
              'candidate_id', p_candidate_id::text
            )::text;
  END IF;

  SELECT COUNT(*)::integer
  INTO v_nonterminal_line_count
  FROM public.banking_pay_workbench_candidate_line_work AS line_work
  WHERE line_work.session_id = p_session_id
    AND line_work.candidate_id = p_candidate_id
    AND UPPER(BTRIM(COALESCE(line_work.status, ''))) NOT IN (
      'MATERIALISED',
      'SKIPPED',
      'ERROR',
      'FAILED',
      'CANCELLED',
      'CANCELED',
      'SUPERSEDED',
      'RETIRED'
    );

  IF COALESCE(v_nonterminal_line_count, 0) > 0 THEN
    RETURN jsonb_build_object(
      'ok', true,
      'action', 'DEFERRED_UNTIL_FINAL_MATERIALISATION',
      'session_id', p_session_id::text,
      'candidate_id', p_candidate_id::text,
      'nonterminal_line_count', v_nonterminal_line_count,
      'policy_x_authority_scope', 'PRE_DRAFT_LIVE_WORKBENCH_ONLY'
    );
  END IF;

  SELECT ROUND(COALESCE(SUM(
           CASE
             WHEN COALESCE(preview_row.row_json->>'amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$'
               THEN GREATEST((preview_row.row_json->>'amount_ex_vat')::numeric, 0)
             ELSE 0::numeric
           END
         ), 0), 2)::numeric(12,2)
  INTO v_retained_positive_headroom
  FROM public.banking_pay_workbench_preview_rows AS preview_row
  WHERE preview_row.session_id = p_session_id
    AND preview_row.session_version = COALESCE(v_session_row.version, 1)
    AND preview_row.candidate_id = p_candidate_id
    AND LOWER(BTRIM(COALESCE(preview_row.section, ''))) = 'canonical_preview_lines'
    AND UPPER(BTRIM(COALESCE(preview_row.status, ''))) = 'READY'
    AND LOWER(BTRIM(COALESCE(preview_row.row_json->>'draftable', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
    AND LOWER(BTRIM(COALESCE(preview_row.row_json->>'is_ready_for_draft', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
    AND UPPER(BTRIM(COALESCE(preview_row.row_json->>'line_type', preview_row.row_json->>'item_type', ''))) NOT IN (
      'MANUAL_DEBT_RECOVERY',
      'OVERPAYMENT_RECOVERY',
      'LOAN_REPAYMENT',
      'PAYMENT_ADVANCE_REPAYMENT'
    );

  IF COALESCE(v_retained_positive_headroom, 0) > 0 THEN
    RETURN jsonb_build_object(
      'ok', true,
      'action', 'RETAINED_POSITIVE_PAY_PRESENT',
      'session_id', p_session_id::text,
      'candidate_id', p_candidate_id::text,
      'retained_positive_headroom_ex_vat', v_retained_positive_headroom,
      'demoted_recovery_count', 0,
      'policy_x_authority_scope', 'PRE_DRAFT_LIVE_WORKBENCH_ONLY'
    );
  END IF;

  DROP TABLE IF EXISTS pg_temp._tmp_pay_wb_zero_headroom_recovery;
  CREATE TEMP TABLE _tmp_pay_wb_zero_headroom_recovery ON COMMIT DROP AS
  SELECT DISTINCT ON (
           ready_row.row_json->>'finance_case_id',
           UPPER(BTRIM(COALESCE(ready_row.row_json->>'line_type', ready_row.row_json->>'item_type', '')))
         )
         ready_row.row_json->>'finance_case_id' AS finance_case_id_text,
         UPPER(BTRIM(COALESCE(ready_row.row_json->>'line_type', ready_row.row_json->>'item_type', ''))) AS line_type,
         ready_row.row_ordinal,
         ready_row.timesheet_id,
         ready_row.key_type,
         ready_row.key_value,
         COALESCE(blocked_template.row_key,
           'finance:' || (ready_row.row_json->>'finance_case_id') || ':' ||
           LOWER(BTRIM(COALESCE(ready_row.row_json->>'line_type', ready_row.row_json->>'item_type', ''))) || ':parent'
         ) AS blocked_row_key,
         COALESCE(blocked_template.row_json, ready_row.row_json, '{}'::jsonb) AS base_row_json,
         CASE
           WHEN COALESCE(ready_row.row_json->>'nominal_due_amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$'
             THEN ROUND(ABS((ready_row.row_json->>'nominal_due_amount_ex_vat')::numeric), 2)
           WHEN COALESCE(ready_row.row_json->>'amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$'
             THEN ROUND(ABS((ready_row.row_json->>'amount_ex_vat')::numeric), 2)
           ELSE 0::numeric
         END::numeric(12,2) AS nominal_due_amount_ex_vat
  FROM public.banking_pay_workbench_preview_rows AS ready_row
  LEFT JOIN LATERAL (
    SELECT blocked_row.row_key,
           blocked_row.row_json
    FROM public.banking_pay_workbench_preview_rows AS blocked_row
    WHERE blocked_row.session_id = ready_row.session_id
      AND blocked_row.session_version = ready_row.session_version
      AND blocked_row.candidate_id = ready_row.candidate_id
      AND LOWER(BTRIM(COALESCE(blocked_row.section, ''))) = 'blocked_for_pay'
      AND blocked_row.row_json->>'finance_case_id' = ready_row.row_json->>'finance_case_id'
      AND UPPER(BTRIM(COALESCE(blocked_row.row_json->>'line_type', blocked_row.row_json->>'item_type', ''))) =
          UPPER(BTRIM(COALESCE(ready_row.row_json->>'line_type', ready_row.row_json->>'item_type', '')))
    ORDER BY CASE WHEN UPPER(BTRIM(COALESCE(blocked_row.status, ''))) = 'READY' THEN 0 ELSE 1 END,
             blocked_row.updated_at_utc DESC,
             blocked_row.id
    LIMIT 1
  ) AS blocked_template ON true
  WHERE ready_row.session_id = p_session_id
    AND ready_row.session_version = COALESCE(v_session_row.version, 1)
    AND ready_row.candidate_id = p_candidate_id
    AND LOWER(BTRIM(COALESCE(ready_row.section, ''))) = 'canonical_preview_lines'
    AND UPPER(BTRIM(COALESCE(ready_row.status, ''))) = 'READY'
    AND ready_row.row_json->>'finance_case_id' ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    AND UPPER(BTRIM(COALESCE(ready_row.row_json->>'line_type', ready_row.row_json->>'item_type', ''))) IN (
      'MANUAL_DEBT_RECOVERY',
      'OVERPAYMENT_RECOVERY',
      'LOAN_REPAYMENT',
      'PAYMENT_ADVANCE_REPAYMENT'
    )
    AND COALESCE(ready_row.row_json->>'amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$'
    AND ROUND((ready_row.row_json->>'amount_ex_vat')::numeric, 2) < 0
  ORDER BY ready_row.row_json->>'finance_case_id',
           UPPER(BTRIM(COALESCE(ready_row.row_json->>'line_type', ready_row.row_json->>'item_type', ''))),
           ready_row.row_ordinal,
           ready_row.id;

  SELECT COUNT(*)::integer
  INTO v_recovery_group_count
  FROM pg_temp._tmp_pay_wb_zero_headroom_recovery;

  IF COALESCE(v_recovery_group_count, 0) = 0 THEN
    RETURN jsonb_build_object(
      'ok', true,
      'action', 'NO_READY_RECOVERY_TO_REVALIDATE',
      'session_id', p_session_id::text,
      'candidate_id', p_candidate_id::text,
      'retained_positive_headroom_ex_vat', v_retained_positive_headroom,
      'demoted_recovery_count', 0,
      'policy_x_authority_scope', 'PRE_DRAFT_LIVE_WORKBENCH_ONLY'
    );
  END IF;

  INSERT INTO public.banking_pay_workbench_preview_rows (
    session_id,
    candidate_id,
    section,
    row_key,
    row_ordinal,
    row_json,
    timesheet_id,
    key_type,
    key_value,
    selected,
    selection_state,
    status,
    session_version,
    created_at_utc,
    updated_at_utc
  )
  SELECT
    p_session_id,
    p_candidate_id,
    'blocked_for_pay',
    recovery_row.blocked_row_key,
    recovery_row.row_ordinal,
    (
      recovery_row.base_row_json
      - 'source_basis_json'
      - 'frozen_source_basis_json'
      - 'frozen_component_snapshot_json'
      - 'finance_component_id'
      - 'materialised_from_line_work_id'
    )
    || jsonb_build_object(
      'line_key', recovery_row.blocked_row_key,
      'row_key', recovery_row.blocked_row_key,
      'presentation_line_id', recovery_row.blocked_row_key,
      'presentation_parent_line_id', NULL,
      'section', 'blocked_for_pay',
      'target_section', 'blocked_for_pay',
      'readiness_state', 'BLOCKED_FOR_PAY',
      'presentation_section', 'BLOCKED_FOR_PAY',
      'presentation_reason', 'NO_PAY_HEADROOM',
      'amount_ex_vat', 0,
      'preview_amount_ex_vat', 0,
      'amount_display', 0,
      'section_amount_ex_vat', 0,
      'section_amount_display', 0,
      'target_pay_ex_vat', 0,
      'ready_preview_amount_ex_vat', 0,
      'preview_component_amount_ex_vat', 0,
      'source_amount_ex_vat', 0,
      'source_reservation_amount_ex_vat', 0,
      'frozen_source_amount', 0,
      'frozen_target_amount_ex_vat', 0,
      'nominal_due_amount_ex_vat', recovery_row.nominal_due_amount_ex_vat,
      'recoverable_this_pay_run_ex_vat', 0,
      'draftable', false,
      'is_ready_for_draft', false,
      'selection_allowed', false,
      'is_excluded_from_allocation', true,
      'selected', false,
      'selection_state', 'NOT_SELECTABLE',
      'blocked_reason_codes', CASE
        WHEN jsonb_typeof(COALESCE(recovery_row.base_row_json->'blocked_reason_codes', '[]'::jsonb)) <> 'array'
          THEN jsonb_build_array('NO_PAY_HEADROOM')
        WHEN COALESCE(recovery_row.base_row_json->'blocked_reason_codes', '[]'::jsonb) @> jsonb_build_array('NO_PAY_HEADROOM')
          THEN COALESCE(recovery_row.base_row_json->'blocked_reason_codes', '[]'::jsonb)
        ELSE COALESCE(recovery_row.base_row_json->'blocked_reason_codes', '[]'::jsonb) || jsonb_build_array('NO_PAY_HEADROOM')
      END,
      'materialisation_recovery_headroom_revalidated', true,
      'materialisation_recovery_headroom_revalidated_at_utc', v_now::text,
      'retained_positive_headroom_ex_vat', 0,
      'policy_x_authority_scope', 'PRE_DRAFT_LIVE_WORKBENCH_ONLY'
    )
    || jsonb_build_object(
      'preview_contract', COALESCE(recovery_row.base_row_json->'preview_contract', '{}'::jsonb)
        || jsonb_build_object(
          'line_key', recovery_row.blocked_row_key,
          'amount_ex_vat', 0,
          'draftable', false,
          'is_ready_for_draft', false,
          'selection_allowed', false,
          'is_excluded_from_allocation', true,
          'target_section', 'blocked_for_pay',
          'presentation_section', 'BLOCKED_FOR_PAY'
        ),
      'case_resolution_summary', CASE
        WHEN jsonb_typeof(COALESCE(recovery_row.base_row_json->'case_resolution_summary', '{}'::jsonb)) = 'object'
          THEN COALESCE(recovery_row.base_row_json->'case_resolution_summary', '{}'::jsonb)
            || jsonb_build_object('due_amount_ex_vat', 0)
        ELSE jsonb_build_object('due_amount_ex_vat', 0)
      END
    ),
    recovery_row.timesheet_id,
    recovery_row.key_type,
    recovery_row.key_value,
    false,
    'NOT_SELECTABLE',
    'READY',
    COALESCE(v_session_row.version, 1),
    v_now,
    v_now
  FROM pg_temp._tmp_pay_wb_zero_headroom_recovery AS recovery_row
  ON CONFLICT (session_id, section, candidate_id, row_key)
  DO UPDATE
  SET row_ordinal = EXCLUDED.row_ordinal,
      row_json = EXCLUDED.row_json,
      timesheet_id = EXCLUDED.timesheet_id,
      key_type = EXCLUDED.key_type,
      key_value = EXCLUDED.key_value,
      selected = false,
      selection_state = 'NOT_SELECTABLE',
      status = 'READY',
      session_version = EXCLUDED.session_version,
      updated_at_utc = v_now;

  GET DIAGNOSTICS v_demoted_count = ROW_COUNT;

  UPDATE public.banking_pay_workbench_preview_rows AS ready_recovery_row
  SET status = 'SUPERSEDED',
      selected = false,
      selection_state = 'SUPERSEDED',
      row_json = COALESCE(ready_recovery_row.row_json, '{}'::jsonb)
        || jsonb_build_object(
          'selected', false,
          'selection_state', 'SUPERSEDED',
          'materialisation_recovery_headroom_revalidated', true,
          'materialisation_recovery_headroom_revalidated_at_utc', v_now::text,
          'retained_positive_headroom_ex_vat', 0,
          'superseded_reason', 'NO_PAY_HEADROOM_AFTER_FINAL_MATERIALISATION',
          'policy_x_authority_scope', 'PRE_DRAFT_LIVE_WORKBENCH_ONLY'
        ),
      updated_at_utc = v_now
  WHERE ready_recovery_row.session_id = p_session_id
    AND ready_recovery_row.session_version = COALESCE(v_session_row.version, 1)
    AND ready_recovery_row.candidate_id = p_candidate_id
    AND LOWER(BTRIM(COALESCE(ready_recovery_row.section, ''))) = 'canonical_preview_lines'
    AND UPPER(BTRIM(COALESCE(ready_recovery_row.status, ''))) = 'READY'
    AND EXISTS (
      SELECT 1
      FROM pg_temp._tmp_pay_wb_zero_headroom_recovery AS recovery_row
      WHERE recovery_row.finance_case_id_text = ready_recovery_row.row_json->>'finance_case_id'
        AND recovery_row.line_type = UPPER(BTRIM(COALESCE(ready_recovery_row.row_json->>'line_type', ready_recovery_row.row_json->>'item_type', '')))
    );

  GET DIAGNOSTICS v_superseded_count = ROW_COUNT;

  UPDATE public.banking_pay_workbench_candidate_line_work AS line_work
  SET result_row_json = COALESCE(line_work.result_row_json, '{}'::jsonb)
        || jsonb_build_object(
          'section', 'blocked_for_pay',
          'target_section', 'blocked_for_pay',
          'readiness_state', 'BLOCKED_FOR_PAY',
          'presentation_section', 'BLOCKED_FOR_PAY',
          'presentation_reason', 'NO_PAY_HEADROOM',
          'amount_ex_vat', 0,
          'preview_amount_ex_vat', 0,
          'amount_display', 0,
          'target_pay_ex_vat', 0,
          'recoverable_this_pay_run_ex_vat', 0,
          'draftable', false,
          'is_ready_for_draft', false,
          'selection_allowed', false,
          'is_excluded_from_allocation', true,
          'selected', false,
          'selection_state', 'NOT_SELECTABLE',
          'blocked_reason_codes', CASE
            WHEN jsonb_typeof(COALESCE(line_work.result_row_json->'blocked_reason_codes', '[]'::jsonb)) <> 'array'
              THEN jsonb_build_array('NO_PAY_HEADROOM')
            WHEN COALESCE(line_work.result_row_json->'blocked_reason_codes', '[]'::jsonb) @> jsonb_build_array('NO_PAY_HEADROOM')
              THEN COALESCE(line_work.result_row_json->'blocked_reason_codes', '[]'::jsonb)
            ELSE COALESCE(line_work.result_row_json->'blocked_reason_codes', '[]'::jsonb) || jsonb_build_array('NO_PAY_HEADROOM')
          END,
          'materialisation_recovery_headroom_revalidated', true,
          'materialisation_recovery_headroom_revalidated_at_utc', v_now::text,
          'retained_positive_headroom_ex_vat', 0,
          'policy_x_authority_scope', 'PRE_DRAFT_LIVE_WORKBENCH_ONLY'
        ),
      updated_at_utc = v_now
  WHERE line_work.session_id = p_session_id
    AND line_work.candidate_id = p_candidate_id
    AND EXISTS (
      SELECT 1
      FROM pg_temp._tmp_pay_wb_zero_headroom_recovery AS recovery_row
      WHERE recovery_row.finance_case_id_text = line_work.result_row_json->>'finance_case_id'
        AND recovery_row.line_type = UPPER(BTRIM(COALESCE(line_work.result_row_json->>'line_type', line_work.result_row_json->>'item_type', '')))
    );

  GET DIAGNOSTICS v_line_work_revalidated_count = ROW_COUNT;

  v_progress_json := public.pay_workbench_session_recompute_progress_counters(
    p_session_id,
    true,
    'POST_MATERIALISATION_RECOVERY_HEADROOM_REVALIDATION',
    false
  );

  SELECT COALESCE(jsonb_agg(to_jsonb(selected_row.id::text) ORDER BY selected_row.row_ordinal, selected_row.id), '[]'::jsonb),
         COUNT(*)::integer
  INTO v_selected_preview_row_ids,
       v_selected_row_count
  FROM public.banking_pay_workbench_preview_rows AS selected_row
  WHERE selected_row.session_id = p_session_id
    AND selected_row.session_version = COALESCE(v_session_row.version, 1)
    AND LOWER(BTRIM(COALESCE(selected_row.section, ''))) = 'canonical_preview_lines'
    AND UPPER(BTRIM(COALESCE(selected_row.status, ''))) = 'READY'
    AND COALESCE(selected_row.selected, false) = true
    AND UPPER(BTRIM(COALESCE(selected_row.selection_state, ''))) = 'SELECTED';

  v_selection_intent_mode := UPPER(BTRIM(COALESCE(
    v_session_row.progress_json#>>'{selection_intent_v1,canonical_preview_lines,mode}',
    ''
  )));

  UPDATE public.banking_pay_workbench_sessions AS session_row
  SET selected_row_count = COALESCE(v_selected_row_count, 0),
      server_selected_preview_row_ids = CASE
        WHEN v_selection_intent_mode = 'EXPLICIT_INCLUDE'
          OR COALESCE(session_row.server_selected_preview_row_ids_provided, false) IS TRUE
          THEN COALESCE(v_selected_preview_row_ids, '[]'::jsonb)
        ELSE session_row.server_selected_preview_row_ids
      END,
      progress_json = COALESCE(session_row.progress_json, '{}'::jsonb)
        || jsonb_build_object(
          'last_recovery_headroom_revalidation_at_utc', v_now::text,
          'last_recovery_headroom_revalidation_action', 'DEMOTED_ZERO_RETAINED_HEADROOM',
          'last_recovery_headroom_revalidation_candidate_id', p_candidate_id::text,
          'last_recovery_headroom_revalidation_demoted_count', COALESCE(v_demoted_count, 0),
          'policy_x_authority_scope', 'PRE_DRAFT_LIVE_WORKBENCH_ONLY'
        ),
      updated_at_utc = v_now
  WHERE session_row.id = p_session_id;

  RETURN jsonb_build_object(
    'ok', true,
    'action', 'DEMOTED_ZERO_RETAINED_HEADROOM',
    'session_id', p_session_id::text,
    'candidate_id', p_candidate_id::text,
    'retained_positive_headroom_ex_vat', v_retained_positive_headroom,
    'recovery_group_count', COALESCE(v_recovery_group_count, 0),
    'demoted_recovery_count', COALESCE(v_demoted_count, 0),
    'superseded_ready_recovery_count', COALESCE(v_superseded_count, 0),
    'line_work_revalidated_count', COALESCE(v_line_work_revalidated_count, 0),
    'selected_row_count', COALESCE(v_selected_row_count, 0),
    'progress_recomputed', COALESCE(v_progress_json, '{}'::jsonb),
    'policy_x_authority_scope', 'PRE_DRAFT_LIVE_WORKBENCH_ONLY',
    'post_draft_artifacts_touched', false,
    'payment_execution_started', false
  );
END;
$function$;

REVOKE ALL ON FUNCTION public.pay_workbench_revalidate_zero_retained_recovery_headroom_v1(uuid, uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.pay_workbench_revalidate_zero_retained_recovery_headroom_v1(uuid, uuid) FROM anon;
REVOKE ALL ON FUNCTION public.pay_workbench_revalidate_zero_retained_recovery_headroom_v1(uuid, uuid) FROM authenticated;
GRANT EXECUTE ON FUNCTION public.pay_workbench_revalidate_zero_retained_recovery_headroom_v1(uuid, uuid) TO service_role;


CREATE OR REPLACE FUNCTION public.pay_workbench_worker_drain_chunk_revalidated_v1(
  p_limit integer DEFAULT 5,
  p_now_utc timestamptz DEFAULT NULL,
  p_session_id uuid DEFAULT NULL,
  p_candidate_id uuid DEFAULT NULL,
  p_allowed_job_types text[] DEFAULT NULL,
  p_worker_id text DEFAULT NULL,
  p_lease_seconds integer DEFAULT 180
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  v_drain_result jsonb := '{}'::jsonb;
  v_revalidation_results jsonb := '[]'::jsonb;
  v_revalidation_result jsonb := '{}'::jsonb;
  v_target record;
  v_target_count integer := 0;
  v_demoted_count integer := 0;
BEGIN
  -- Preserve the existing aggregate workbench worker unchanged, then apply the
  -- candidate-level invariant only to terminal materialisation jobs returned by
  -- this drain. Both functions run in the same transaction, so a revalidation
  -- error fails closed and rolls the claimed work back for a safe retry.
  v_drain_result := public.pay_workbench_worker_drain_chunk(
    p_limit,
    p_now_utc,
    p_session_id,
    p_candidate_id,
    p_allowed_job_types,
    p_worker_id,
    p_lease_seconds
  );

  FOR v_target IN
    SELECT DISTINCT
      completed_job.session_id,
      completed_job.candidate_id
    FROM jsonb_array_elements(
      CASE
        WHEN jsonb_typeof(COALESCE(v_drain_result->'jobs', '[]'::jsonb)) = 'array'
          THEN COALESCE(v_drain_result->'jobs', '[]'::jsonb)
        ELSE '[]'::jsonb
      END
    ) AS returned_job(job_item)
    JOIN public.banking_pay_workbench_jobs AS completed_job
      ON COALESCE(job_item->>'job_id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
     AND completed_job.id = (job_item->>'job_id')::uuid
    WHERE completed_job.session_id IS NOT NULL
      AND completed_job.candidate_id IS NOT NULL
      AND UPPER(BTRIM(COALESCE(job_item->>'status', ''))) = 'SUCCEEDED'
      AND UPPER(BTRIM(COALESCE(job_item->>'canonical_job_type', job_item->>'job_type', job_item->>'type', ''))) IN (
        'WORKBENCH_PREVIEW_ROWS_MATERIALISE',
        'WORKBENCH_PREVIEW_ROWS_MATERIALIZE',
        'WORKBENCH_PREVIEW_ROWS_MATERIALISE_CHUNK',
        'WORKBENCH_PREVIEW_ROWS_MATERIALIZE_CHUNK',
        'PREVIEW_ROWS_MATERIALISE',
        'PREVIEW_ROWS_MATERIALIZE',
        'PREVIEW_ROWS_MATERIALISE_CHUNK',
        'PREVIEW_ROWS_MATERIALIZE_CHUNK',
        'PREVIEW_ROW_MATERIALISE_CHUNK',
        'PREVIEW_ROW_MATERIALIZE_CHUNK'
      )
  LOOP
    v_target_count := v_target_count + 1;
    v_revalidation_result := public.pay_workbench_revalidate_zero_retained_recovery_headroom_v1(
      v_target.session_id,
      v_target.candidate_id
    );

    v_demoted_count := v_demoted_count + COALESCE(
      CASE
        WHEN COALESCE(v_revalidation_result->>'demoted_recovery_count', '') ~ '^[0-9]+$'
          THEN (v_revalidation_result->>'demoted_recovery_count')::integer
        ELSE 0
      END,
      0
    );

    v_revalidation_results := v_revalidation_results || jsonb_build_array(
      jsonb_build_object(
        'session_id', v_target.session_id::text,
        'candidate_id', v_target.candidate_id::text,
        'result', COALESCE(v_revalidation_result, '{}'::jsonb)
      )
    );
  END LOOP;

  RETURN COALESCE(v_drain_result, '{}'::jsonb) || jsonb_build_object(
    'recovery_headroom_revalidation_completed', true,
    'recovery_headroom_revalidation_target_count', v_target_count,
    'recovery_headroom_revalidation_demoted_count', v_demoted_count,
    'recovery_headroom_revalidation_results', v_revalidation_results,
    'policy_x_authority_scope', 'PRE_DRAFT_LIVE_WORKBENCH_ONLY',
    'post_draft_artifacts_touched', false,
    'payment_execution_started', false
  );
END;
$function$;

REVOKE ALL ON FUNCTION public.pay_workbench_worker_drain_chunk_revalidated_v1(integer, timestamptz, uuid, uuid, text[], text, integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.pay_workbench_worker_drain_chunk_revalidated_v1(integer, timestamptz, uuid, uuid, text[], text, integer) FROM anon;
REVOKE ALL ON FUNCTION public.pay_workbench_worker_drain_chunk_revalidated_v1(integer, timestamptz, uuid, uuid, text[], text, integer) FROM authenticated;
GRANT EXECUTE ON FUNCTION public.pay_workbench_worker_drain_chunk_revalidated_v1(integer, timestamptz, uuid, uuid, text[], text, integer) TO service_role;
