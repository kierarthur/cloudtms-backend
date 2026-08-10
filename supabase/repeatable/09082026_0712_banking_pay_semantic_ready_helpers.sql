-- Banking Pay semantic Ready-to-Pay contract helpers.
--
-- This helper deliberately evaluates the public READY projection rather than
-- inventing another economic-key or payment-formula owner.  It composes the
-- existing strict per-line contract with a candidate-set invariant:
--
--   * deductions can use only positive ordinary entitlement for the same
--     candidate as headroom;
--   * a recovery-only selectable population is not semantically ready;
--   * a selected Draft candidate may not have a negative result.
--
-- It is bounded to 100 candidates and performs no financial mutation.

CREATE OR REPLACE FUNCTION private.pay_workbench_semantic_ready_proof_page_v1(
  p_session_id uuid,
  p_candidate_ids uuid[],
  p_source_run_by_candidate_json jsonb DEFAULT '{}'::jsonb,
  p_selected_preview_row_ids_by_candidate_json jsonb DEFAULT NULL::jsonb,
  p_mode text DEFAULT 'PUBLICATION'::text,
  p_options_json jsonb DEFAULT '{}'::jsonb
) RETURNS jsonb
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path TO ''
AS $function$
DECLARE
  v_mode text := pg_catalog.upper(pg_catalog.btrim(COALESCE(p_mode, 'PUBLICATION')));
  v_candidate_ids uuid[] := COALESCE(p_candidate_ids, ARRAY[]::uuid[]);
  v_candidate_count integer := 0;
  v_result jsonb := '[]'::jsonb;
  v_semantic_ready boolean := false;
  v_publication_identity_enforce_enabled boolean := false;
BEGIN
  IF p_session_id IS NULL THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_SEMANTIC_READY_SESSION_REQUIRED'
      USING ERRCODE = '22023';
  END IF;

  IF v_mode NOT IN ('PUBLICATION', 'DRAFT_SELECTION', 'CANCELLATION_REVERSION', 'OBSERVE_ONLY') THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_SEMANTIC_READY_MODE_INVALID: %', v_mode
      USING ERRCODE = '22023';
  END IF;

  IF p_source_run_by_candidate_json IS NOT NULL
     AND pg_catalog.jsonb_typeof(p_source_run_by_candidate_json) <> 'object' THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_SEMANTIC_READY_SOURCE_RUN_MAP_INVALID'
      USING ERRCODE = '22023';
  END IF;

  IF p_selected_preview_row_ids_by_candidate_json IS NOT NULL
     AND pg_catalog.jsonb_typeof(p_selected_preview_row_ids_by_candidate_json) <> 'object' THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_SEMANTIC_READY_SELECTION_MAP_INVALID'
      USING ERRCODE = '22023';
  END IF;

  IF p_options_json IS NULL OR pg_catalog.jsonb_typeof(p_options_json) <> 'object' THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_SEMANTIC_READY_OPTIONS_INVALID'
      USING ERRCODE = '22023';
  END IF;

  SELECT pg_catalog.count(*)::integer
  INTO v_candidate_count
  FROM (
    SELECT DISTINCT candidate_id
    FROM pg_catalog.unnest(v_candidate_ids) AS candidate_id
    WHERE candidate_id IS NOT NULL
  ) AS distinct_candidates;

  IF v_candidate_count < 1 OR v_candidate_count > 100 THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_SEMANTIC_READY_CANDIDATE_LIMIT: %', v_candidate_count
      USING ERRCODE = '22023';
  END IF;

  WITH requested_candidates AS (
    SELECT DISTINCT candidate_id
    FROM pg_catalog.unnest(v_candidate_ids) AS candidate_id
    WHERE candidate_id IS NOT NULL
  ), selected_ids AS (
    SELECT
      requested.candidate_id,
      selected_id.value::uuid AS preview_row_id
    FROM requested_candidates AS requested
    CROSS JOIN LATERAL pg_catalog.jsonb_array_elements_text(
      CASE
        WHEN pg_catalog.jsonb_typeof(p_selected_preview_row_ids_by_candidate_json -> requested.candidate_id::text) = 'array'
          THEN p_selected_preview_row_ids_by_candidate_json -> requested.candidate_id::text
        ELSE '[]'::jsonb
      END
    ) AS selected_id(value)
    WHERE selected_id.value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  ), ready_rows AS (
    SELECT
      preview_row.candidate_id,
      preview_row.id AS preview_row_id,
      preview_row.row_key,
      preview_row.row_json,
      preview_row.timesheet_id,
      preview_row.key_type,
      preview_row.key_value,
      preview_row.selected,
      preview_row.selection_state,
      public.pay_workbench_preview_line_contract_ok(
        p_line_json => preview_row.row_json
          || pg_catalog.jsonb_build_object(
            'row_key', preview_row.row_key,
            'selected', preview_row.selected,
            'selection_state', preview_row.selection_state,
            'status', preview_row.status
          ),
        p_economic_key_json => pg_catalog.jsonb_strip_nulls(pg_catalog.jsonb_build_object(
          'timesheet_id', CASE WHEN preview_row.timesheet_id IS NULL THEN NULL ELSE preview_row.timesheet_id::text END,
          'key_type', preview_row.key_type,
          'key_value', preview_row.key_value
        )),
        p_target_section => CASE
          WHEN COALESCE(
                 preview_row.row_json#>>'{selection_recovery_headroom_v1,contract_version}',
                 ''
               ) = '1'
            THEN COALESCE(
              NULLIF(pg_catalog.btrim(
                preview_row.row_json#>>'{selection_recovery_headroom_v1,effective_section}'
              ), ''),
              NULLIF(pg_catalog.btrim(preview_row.section), ''),
              'canonical_preview_lines'
            )
          ELSE COALESCE(
            NULLIF(pg_catalog.btrim(preview_row.section), ''),
            'canonical_preview_lines'
          )
        END
      ) AS line_contract,
      CASE
        WHEN COALESCE(preview_row.row_json->>'amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
          THEN pg_catalog.round((preview_row.row_json->>'amount_ex_vat')::numeric, 2)
        WHEN COALESCE(preview_row.row_json->>'preview_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
          THEN pg_catalog.round((preview_row.row_json->>'preview_amount_ex_vat')::numeric, 2)
        ELSE 0::numeric
      END AS amount_ex_vat,
      CASE
        WHEN v_mode = 'DRAFT_SELECTION'
          THEN EXISTS (
            SELECT 1
            FROM selected_ids AS selected_id
            WHERE selected_id.candidate_id = preview_row.candidate_id
              AND selected_id.preview_row_id = preview_row.id
          )
        ELSE true
      END AS included_in_proof
    FROM public.banking_pay_workbench_preview_rows AS preview_row
    JOIN requested_candidates AS requested
      ON requested.candidate_id = preview_row.candidate_id
    WHERE preview_row.session_id = p_session_id
      AND preview_row.status = 'READY'
  ), classified AS (
    SELECT
      ready_row.*,
      pg_catalog.lower(pg_catalog.btrim(COALESCE(ready_row.line_contract->>'ok', 'false')))
        IN ('true', 't', '1', 'yes', 'y', 'on') AS contract_ok,
      pg_catalog.lower(pg_catalog.btrim(COALESCE(ready_row.line_contract->>'selection_allowed', 'false')))
        IN ('true', 't', '1', 'yes', 'y', 'on') AS contract_selection_allowed,
      pg_catalog.lower(pg_catalog.btrim(COALESCE(ready_row.line_contract->>'is_recognised_finance_deduction', 'false')))
        IN ('true', 't', '1', 'yes', 'y', 'on') AS recognised_deduction,
      pg_catalog.upper(pg_catalog.btrim(COALESCE(ready_row.row_json->>'line_type', ready_row.row_json->>'case_type', ''))) AS line_type,
      pg_catalog.upper(pg_catalog.btrim(COALESCE(ready_row.row_json->>'presentation_role', ''))) AS presentation_role
    FROM ready_rows AS ready_row
  ), source_authority AS (
    SELECT
      requested.candidate_id,
      pg_catalog.count(DISTINCT source_row.source_publication_id)::integer AS source_publication_count,
      pg_catalog.min(source_row.source_publication_id::text)::uuid AS source_publication_id
    FROM requested_candidates AS requested
    LEFT JOIN public.banking_pay_workbench_candidate_source_lines AS source_row
      ON source_row.session_id=p_session_id
     AND source_row.candidate_id=requested.candidate_id
     AND source_row.status='CURRENT'
    GROUP BY requested.candidate_id
  ), candidate_rollup AS (
    SELECT
      requested.candidate_id,
      source_authority.source_publication_count,
      source_authority.source_publication_id,
      pg_catalog.count(classified.preview_row_id) FILTER (
        WHERE classified.included_in_proof
          AND classified.contract_ok
          AND classified.contract_selection_allowed
          AND NOT classified.recognised_deduction
          AND classified.amount_ex_vat > 0
          AND classified.line_type = 'TIMESHEET_PAYMENT'
          AND classified.presentation_role = 'ALLOCATION_COMPONENT'
      )::integer AS ordinary_positive_selectable_count,
      pg_catalog.round(COALESCE(pg_catalog.sum(classified.amount_ex_vat) FILTER (
        WHERE classified.included_in_proof
          AND classified.contract_ok
          AND classified.contract_selection_allowed
          AND NOT classified.recognised_deduction
          AND classified.amount_ex_vat > 0
          AND classified.line_type = 'TIMESHEET_PAYMENT'
          AND classified.presentation_role = 'ALLOCATION_COMPONENT'
      ), 0), 2) AS ordinary_positive_amount,
      pg_catalog.count(classified.preview_row_id) FILTER (
        WHERE classified.included_in_proof
          AND classified.contract_ok
          AND classified.contract_selection_allowed
          AND classified.recognised_deduction
          AND classified.amount_ex_vat < 0
      )::integer AS recognised_deduction_count,
      pg_catalog.round(COALESCE(pg_catalog.sum(classified.amount_ex_vat) FILTER (
        WHERE classified.included_in_proof
          AND classified.contract_ok
          AND classified.contract_selection_allowed
          AND classified.recognised_deduction
          AND classified.amount_ex_vat < 0
      ), 0), 2) AS recognised_deduction_amount,
      pg_catalog.count(classified.preview_row_id) FILTER (
        WHERE classified.included_in_proof
          AND (
            (classified.contract_selection_allowed AND NOT classified.contract_ok)
            OR (
              pg_catalog.upper(COALESCE(classified.selection_state, '')) <> 'NOT_SELECTABLE'
              AND NOT classified.contract_ok
            )
          )
      )::integer AS invalid_selectable_row_count,
      pg_catalog.count(classified.preview_row_id) FILTER (
        WHERE pg_catalog.lower(COALESCE(classified.row_json->>'draftable', 'false')) NOT IN ('true','t','1','yes','y','on')
      )::integer AS context_row_count,
      pg_catalog.count(classified.preview_row_id) FILTER (
        WHERE pg_catalog.upper(COALESCE(classified.row_json->>'presentation_section', classified.row_json->>'readiness_state', '')) = 'BLOCKED_FOR_PAY'
      )::integer AS blocked_row_count,
      pg_catalog.count(classified.preview_row_id) FILTER (
        WHERE classified.line_type = 'TIMESHEET_PAYMENT'
          AND classified.presentation_role = 'PARENT'
          AND classified.amount_ex_vat < 0
          AND pg_catalog.upper(COALESCE(classified.row_json->>'presentation_section', classified.row_json->>'readiness_state', '')) = 'READY_TO_PAY'
      )::integer AS invalid_ready_negative_parent_count,
      pg_catalog.count(classified.preview_row_id) FILTER (
        WHERE classified.line_type = 'TIMESHEET_PAYMENT'
          AND classified.presentation_role = 'PARENT'
          AND classified.amount_ex_vat < 0
          AND pg_catalog.upper(COALESCE(classified.row_json->>'presentation_section', classified.row_json->>'readiness_state', '')) = 'BLOCKED_FOR_PAY'
      )::integer AS negative_ordinary_blocked_count,
      pg_catalog.count(classified.preview_row_id) FILTER (
        WHERE classified.line_type = 'TIMESHEET_PAYMENT'
          AND classified.presentation_role = 'PARENT'
          AND classified.amount_ex_vat < 0
          AND pg_catalog.upper(COALESCE(classified.row_json->>'presentation_section', classified.row_json->>'readiness_state', '')) = 'CASES_RESOLUTIONS'
      )::integer AS negative_ordinary_cases_count,
      pg_catalog.md5(COALESCE(pg_catalog.string_agg(
        classified.preview_row_id::text || ':' ||
        pg_catalog.upper(COALESCE(classified.row_json->>'presentation_section', classified.row_json->>'readiness_state', '')),
        '|' ORDER BY classified.preview_row_id
      ), '')) AS presentation_section_digest,
      pg_catalog.count(classified.preview_row_id) FILTER (WHERE classified.included_in_proof)::integer AS proof_row_count
    FROM requested_candidates AS requested
    JOIN source_authority ON source_authority.candidate_id=requested.candidate_id
    LEFT JOIN classified
      ON classified.candidate_id = requested.candidate_id
    GROUP BY requested.candidate_id,source_authority.source_publication_count,
      source_authority.source_publication_id
  ), candidate_proof AS (
    SELECT
      rollup.*,
      pg_catalog.round(LEAST(
        GREATEST(rollup.ordinary_positive_amount, 0),
        GREATEST(-rollup.recognised_deduction_amount, 0)
      ), 2) AS usable_same_candidate_headroom,
      pg_catalog.round(rollup.ordinary_positive_amount + rollup.recognised_deduction_amount, 2) AS candidate_ready_amount,
      (
        rollup.invalid_selectable_row_count = 0
        AND (
          v_publication_identity_enforce_enabled IS NOT TRUE
          OR (rollup.source_publication_count=1 AND rollup.source_publication_id IS NOT NULL)
        )
        AND rollup.invalid_ready_negative_parent_count = 0
        AND rollup.ordinary_positive_amount >= 0
        AND (
          rollup.recognised_deduction_amount = 0
          OR (
            rollup.ordinary_positive_amount > 0
            AND -rollup.recognised_deduction_amount <= rollup.ordinary_positive_amount
          )
        )
        AND rollup.ordinary_positive_amount + rollup.recognised_deduction_amount >= 0
      ) AS semantic_ready
    FROM candidate_rollup AS rollup
  ), proof_json AS (
    SELECT
      proof.candidate_id,
      proof.semantic_ready,
      pg_catalog.jsonb_build_object(
        'candidate_id', proof.candidate_id::text,
        'mode', v_mode,
        'semantic_contract_version', 'READY_TO_PAY_SEMANTIC_V2',
        'source_publication_id', proof.source_publication_id,
        'source_publication_count', proof.source_publication_count,
        'ordinary_positive_selectable_count', proof.ordinary_positive_selectable_count,
        'ordinary_positive_amount', proof.ordinary_positive_amount,
        'recognised_deduction_count', proof.recognised_deduction_count,
        'recognised_deduction_amount', proof.recognised_deduction_amount,
        'usable_same_candidate_headroom', proof.usable_same_candidate_headroom,
        'candidate_ready_amount', proof.candidate_ready_amount,
        'context_row_count', proof.context_row_count,
        'blocked_row_count', proof.blocked_row_count,
        'invalid_ready_negative_parent_count', proof.invalid_ready_negative_parent_count,
        'negative_ordinary_blocked_count', proof.negative_ordinary_blocked_count,
        'negative_ordinary_cases_count', proof.negative_ordinary_cases_count,
        'presentation_section_digest', proof.presentation_section_digest,
        'invalid_selectable_row_count', proof.invalid_selectable_row_count,
        'proof_row_count', proof.proof_row_count,
        'negative_or_recovery_only', (
          proof.recognised_deduction_amount < 0
          AND proof.ordinary_positive_amount <= 0
        ),
        'cross_candidate_headroom_used', false,
        'semantic_ready', proof.semantic_ready,
        'source_run_hint', p_source_run_by_candidate_json -> proof.candidate_id::text
      ) AS proof_without_digest
    FROM candidate_proof AS proof
  )
  SELECT
    COALESCE(
      pg_catalog.jsonb_agg(
        proof.proof_without_digest
        || pg_catalog.jsonb_build_object(
          'semantic_proof_digest', pg_catalog.md5(
            (proof.proof_without_digest - 'mode' - 'source_run_hint')::text
          )
        )
        ORDER BY proof.candidate_id
      ),
      '[]'::jsonb
    ),
    pg_catalog.bool_and(proof.semantic_ready)
  INTO v_result, v_semantic_ready
  FROM proof_json AS proof;

  RETURN pg_catalog.jsonb_build_object(
    'ok', true,
    'proof_version', 1,
    'semantic_contract_version', 'READY_TO_PAY_SEMANTIC_V2',
    'mode', v_mode,
    'session_id', p_session_id::text,
    'candidate_count', v_candidate_count,
    'semantic_ready', COALESCE(v_semantic_ready, false),
    'candidate_results', COALESCE(v_result, '[]'::jsonb)
  );
END;
$function$;

ALTER FUNCTION private.pay_workbench_semantic_ready_proof_page_v1(
  uuid, uuid[], jsonb, jsonb, text, jsonb
) OWNER TO postgres;

REVOKE ALL ON FUNCTION private.pay_workbench_semantic_ready_proof_page_v1(
  uuid, uuid[], jsonb, jsonb, text, jsonb
) FROM PUBLIC, anon, authenticated, service_role;

GRANT EXECUTE ON FUNCTION private.pay_workbench_semantic_ready_proof_page_v1(
  uuid, uuid[], jsonb, jsonb, text, jsonb
) TO postgres;


-- Selection-dependent recovery projection for semantically certified V3
-- sources.  The certified source identity remains immutable: this helper does
-- not change the physical preview section, row key, ordinal, source lineage or
-- preview contract.  It updates only the pre-Draft public selection overlay so
-- a recovery becomes usable when the same candidate has actually selected
-- positive pay in the same channel.
CREATE OR REPLACE FUNCTION private.pay_workbench_recovery_selection_overlay_apply_v1(
  p_session_id uuid,
  p_candidate_id uuid,
  p_options_json jsonb DEFAULT '{}'::jsonb
) RETURNS jsonb
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path TO ''
AS $function$
DECLARE
  v_now timestamptz := pg_catalog.clock_timestamp();
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_scope public.banking_pay_workbench_session_scope%ROWTYPE;
  v_force_v3 boolean := false;
  v_reason text := 'SELECTION_CHANGED';
  v_unknown_option_count integer := 0;
  v_is_v3 boolean := false;
  v_recovery_row_count integer := 0;
  v_ready_recovery_count integer := 0;
  v_blocked_recovery_count integer := 0;
  v_forced_deselected_count integer := 0;
  v_updated_count integer := 0;
  v_selected_positive_amount numeric := 0;
  v_ready_recovery_amount numeric := 0;
  v_selected_preview_row_ids jsonb := '[]'::jsonb;
  v_selected_row_count integer := 0;
  v_overlay_digest text := NULL::text;
  v_duplicate_recovery_identity_count integer := 0;
  v_session_ready boolean := false;
  v_draft_blocker_codes jsonb := '[]'::jsonb;
BEGIN
  IF p_session_id IS NULL OR p_candidate_id IS NULL THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_RECOVERY_SELECTION_OVERLAY_SCOPE_REQUIRED'
      USING ERRCODE = '22023';
  END IF;

  IF p_options_json IS NULL OR pg_catalog.jsonb_typeof(p_options_json) <> 'object' THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_RECOVERY_SELECTION_OVERLAY_OPTIONS_INVALID'
      USING ERRCODE = '22023';
  END IF;

  SELECT pg_catalog.count(*)::integer
  INTO v_unknown_option_count
  FROM pg_catalog.jsonb_object_keys(p_options_json) AS supplied_option(option_key)
  WHERE supplied_option.option_key NOT IN ('force_v3', 'reason');

  IF v_unknown_option_count > 0 THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_RECOVERY_SELECTION_OVERLAY_OPTIONS_UNKNOWN'
      USING ERRCODE = '22023';
  END IF;

  v_force_v3 := pg_catalog.lower(pg_catalog.btrim(COALESCE(p_options_json->>'force_v3', 'false')))
    IN ('true', 't', '1', 'yes', 'y', 'on');
  v_reason := pg_catalog.upper(pg_catalog.btrim(COALESCE(NULLIF(p_options_json->>'reason', ''), 'SELECTION_CHANGED')));

  SELECT session_row.*
  INTO v_session
  FROM public.banking_pay_workbench_sessions AS session_row
  WHERE session_row.id = p_session_id
  FOR UPDATE;

  IF NOT FOUND
     OR pg_catalog.upper(pg_catalog.btrim(COALESCE(v_session.status, ''))) <> 'OPEN'
     OR v_session.discarded_at_utc IS NOT NULL THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_RECOVERY_SELECTION_OVERLAY_SESSION_NOT_OPEN'
      USING ERRCODE = 'P0001';
  END IF;

  SELECT scope_row.*
  INTO v_scope
  FROM public.banking_pay_workbench_session_scope AS scope_row
  WHERE scope_row.session_id = p_session_id
    AND scope_row.candidate_id = p_candidate_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_RECOVERY_SELECTION_OVERLAY_CANDIDATE_NOT_IN_SCOPE'
      USING ERRCODE = 'P0001';
  END IF;

  v_is_v3 := v_force_v3 OR (
    v_scope.certified_preview_publication_attestation_json->>'attestation_version'
      = 'CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3'
    AND v_scope.certified_preview_publication_attestation_json->>'contract_version' = '3'
    AND v_scope.certified_preview_publication_attestation_json->>'semantic_contract_version'
      = 'READY_TO_PAY_SEMANTIC_V2'
  );

  IF v_is_v3 IS NOT TRUE THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok', true,
      'action', 'NOT_V3_SEMANTIC_SOURCE',
      'session_id', p_session_id::text,
      'candidate_id', p_candidate_id::text,
      'updated_count', 0,
      'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
    );
  END IF;

  DROP TABLE IF EXISTS pg_temp._bpay_recovery_selection_overlay;
  CREATE TEMPORARY TABLE pg_temp._bpay_recovery_selection_overlay ON COMMIT DROP AS
  WITH selected_positive_by_channel AS (
    SELECT
      pg_catalog.upper(pg_catalog.btrim(COALESCE(
        positive_row.row_json->>'pay_channel',
        positive_row.row_json->>'candidate_pay_method',
        positive_row.row_json->>'current_pay_method',
        ''
      ))) AS pay_channel,
      pg_catalog.round(COALESCE(pg_catalog.sum(
        CASE
          WHEN COALESCE(positive_row.row_json->>'amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
            THEN GREATEST((positive_row.row_json->>'amount_ex_vat')::numeric, 0)
          ELSE 0::numeric
        END
      ), 0), 2) AS selected_positive_headroom_ex_vat
    FROM public.banking_pay_workbench_preview_rows AS positive_row
    WHERE positive_row.session_id = p_session_id
      AND positive_row.session_version = v_session.version
      AND positive_row.candidate_id = p_candidate_id
      AND positive_row.status = 'READY'
      AND positive_row.selected IS TRUE
      AND pg_catalog.upper(pg_catalog.btrim(COALESCE(positive_row.selection_state, ''))) = 'SELECTED'
      AND pg_catalog.upper(pg_catalog.btrim(COALESCE(positive_row.row_json->>'line_type', ''))) = 'TIMESHEET_PAYMENT'
      AND pg_catalog.upper(pg_catalog.btrim(COALESCE(positive_row.row_json->>'presentation_role', ''))) = 'ALLOCATION_COMPONENT'
      AND pg_catalog.lower(pg_catalog.btrim(COALESCE(positive_row.row_json->>'draftable', 'false')))
        IN ('true', 't', '1', 'yes', 'y', 'on')
      AND pg_catalog.lower(pg_catalog.btrim(COALESCE(positive_row.row_json->>'is_ready_for_draft', 'false')))
        IN ('true', 't', '1', 'yes', 'y', 'on')
      AND pg_catalog.lower(pg_catalog.btrim(COALESCE(positive_row.row_json->>'selection_allowed', 'false')))
        IN ('true', 't', '1', 'yes', 'y', 'on')
      AND COALESCE(positive_row.row_json->>'amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
      AND (positive_row.row_json->>'amount_ex_vat')::numeric > 0
    GROUP BY pg_catalog.upper(pg_catalog.btrim(COALESCE(
      positive_row.row_json->>'pay_channel',
      positive_row.row_json->>'candidate_pay_method',
      positive_row.row_json->>'current_pay_method',
      ''
    )))
  ), recovery_base AS (
    SELECT
      recovery_row.id,
      recovery_row.row_ordinal,
      recovery_row.section AS physical_section,
      recovery_row.row_key,
      recovery_row.selected AS current_selected,
      recovery_row.selection_state AS current_selection_state,
      COALESCE(recovery_row.row_json, '{}'::jsonb) AS base_row_json,
      pg_catalog.upper(pg_catalog.btrim(COALESCE(
        recovery_row.row_json->>'selection_user_override',
        ''
      ))) = 'UNSELECTED' AS explicitly_user_unselected,
      pg_catalog.upper(pg_catalog.btrim(COALESCE(
        recovery_row.row_json->>'pay_channel',
        recovery_row.row_json->>'candidate_pay_method',
        recovery_row.row_json->>'current_pay_method',
        ''
      ))) AS pay_channel,
      pg_catalog.round(COALESCE(
        CASE
          WHEN COALESCE(recovery_row.row_json->>'nominal_due_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
            THEN pg_catalog.abs((recovery_row.row_json->>'nominal_due_amount_ex_vat')::numeric)
          ELSE NULL::numeric
        END,
        CASE
          WHEN COALESCE((
            SELECT pg_catalog.sum(
              CASE
                WHEN COALESCE(component.value->>'preview_due_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
                  THEN pg_catalog.abs((component.value->>'preview_due_amount_ex_vat')::numeric)
                ELSE 0::numeric
              END
            )
            FROM pg_catalog.jsonb_array_elements(
              CASE
                WHEN pg_catalog.jsonb_typeof(recovery_row.row_json->'case_components') = 'array'
                  THEN recovery_row.row_json->'case_components'
                ELSE '[]'::jsonb
              END
            ) AS component(value)
          ), 0) > 0 THEN (
            SELECT pg_catalog.sum(pg_catalog.abs((component.value->>'preview_due_amount_ex_vat')::numeric))
            FROM pg_catalog.jsonb_array_elements(recovery_row.row_json->'case_components') AS component(value)
            WHERE COALESCE(component.value->>'preview_due_amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
          )
          ELSE NULL::numeric
        END,
        CASE
          WHEN COALESCE(recovery_row.row_json->>'amount_ex_vat', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
            THEN pg_catalog.abs((recovery_row.row_json->>'amount_ex_vat')::numeric)
          ELSE 0::numeric
        END
      ), 2) AS nominal_due_amount_ex_vat,
      COALESCE(
        recovery_row.row_json#>'{selection_recovery_headroom_v1,base_blocked_reason_codes}',
        (
          SELECT COALESCE(pg_catalog.jsonb_agg(reason.value ORDER BY reason.ordinality), '[]'::jsonb)
          FROM pg_catalog.jsonb_array_elements_text(
            CASE
              WHEN pg_catalog.jsonb_typeof(recovery_row.row_json->'blocked_reason_codes') = 'array'
                THEN recovery_row.row_json->'blocked_reason_codes'
              ELSE '[]'::jsonb
            END
          ) WITH ORDINALITY AS reason(value, ordinality)
          WHERE pg_catalog.upper(pg_catalog.btrim(reason.value)) <> 'NO_PAY_HEADROOM'
        ),
        '[]'::jsonb
      ) AS base_blocked_reason_codes,
      COALESCE(
        NULLIF(recovery_row.row_json#>>'{selection_recovery_headroom_v1,base_presentation_reason}', ''),
        CASE
          WHEN pg_catalog.upper(COALESCE(recovery_row.row_json->>'presentation_reason', '')) = 'NO_PAY_HEADROOM'
            THEN 'READY_TO_PAY'
          ELSE NULLIF(recovery_row.row_json->>'presentation_reason', '')
        END,
        'READY_TO_PAY'
      ) AS base_presentation_reason,
      (
        recovery_row.row_json->>'finance_case_id'
          ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        AND pg_catalog.upper(pg_catalog.btrim(COALESCE(recovery_row.row_json->>'line_type', ''))) IN (
          'OVERPAYMENT_RECOVERY', 'MANUAL_DEBT_RECOVERY',
          'PAYMENT_ADVANCE_REPAYMENT', 'LOAN_REPAYMENT'
        )
        AND pg_catalog.lower(pg_catalog.btrim(COALESCE(recovery_row.row_json->>'post_draft_overlay_applied', 'false')))
          NOT IN ('true', 't', '1', 'yes', 'y', 'on')
        AND pg_catalog.lower(pg_catalog.btrim(COALESCE(recovery_row.row_json->>'case_needs_resolution_now', 'false')))
          NOT IN ('true', 't', '1', 'yes', 'y', 'on')
        AND pg_catalog.upper(pg_catalog.btrim(COALESCE(recovery_row.row_json#>>'{snooze_state,state}', 'NONE'))) = 'NONE'
        AND NOT EXISTS (
          SELECT 1
          FROM pg_catalog.jsonb_array_elements_text(
            COALESCE(
              recovery_row.row_json#>'{selection_recovery_headroom_v1,base_blocked_reason_codes}',
              CASE
                WHEN pg_catalog.jsonb_typeof(recovery_row.row_json->'blocked_reason_codes') = 'array'
                  THEN recovery_row.row_json->'blocked_reason_codes'
                ELSE '[]'::jsonb
              END
            )
          ) AS blocker(value)
          WHERE pg_catalog.upper(pg_catalog.btrim(blocker.value)) <> 'NO_PAY_HEADROOM'
        )
      ) AS static_recovery_eligible
    FROM public.banking_pay_workbench_preview_rows AS recovery_row
    WHERE recovery_row.session_id = p_session_id
      AND recovery_row.session_version = v_session.version
      AND recovery_row.candidate_id = p_candidate_id
      AND recovery_row.status = 'READY'
      AND pg_catalog.upper(pg_catalog.btrim(COALESCE(
        recovery_row.row_json->>'line_type',
        recovery_row.row_json->>'case_type',
        ''
      ))) IN (
        'OVERPAYMENT_RECOVERY', 'MANUAL_DEBT_RECOVERY',
        'PAYMENT_ADVANCE_REPAYMENT', 'LOAN_REPAYMENT'
      )
  ), ranked_recovery AS (
    SELECT
      recovery_base.*,
      COALESCE(selected_headroom.selected_positive_headroom_ex_vat, 0)::numeric AS selected_positive_headroom_ex_vat,
      COALESCE(pg_catalog.sum(
        CASE
          WHEN recovery_base.static_recovery_eligible
            THEN recovery_base.nominal_due_amount_ex_vat
          ELSE 0::numeric
        END
      ) OVER (
        PARTITION BY recovery_base.pay_channel
        ORDER BY recovery_base.row_ordinal, recovery_base.id
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
      ), 0)::numeric AS prior_nominal_due_amount_ex_vat
    FROM recovery_base
    LEFT JOIN selected_positive_by_channel AS selected_headroom
      ON selected_headroom.pay_channel = recovery_base.pay_channel
  ), allocated_recovery AS (
    SELECT
      ranked_recovery.*,
      CASE
        WHEN ranked_recovery.static_recovery_eligible
         AND ranked_recovery.pay_channel IN ('PAYE', 'UMBRELLA')
          THEN pg_catalog.round(LEAST(
            ranked_recovery.nominal_due_amount_ex_vat,
            GREATEST(
              ranked_recovery.selected_positive_headroom_ex_vat
                - ranked_recovery.prior_nominal_due_amount_ex_vat,
              0
            )
          ), 2)
        ELSE 0::numeric
      END AS recoverable_amount_ex_vat
    FROM ranked_recovery
  )
  SELECT
    allocated_recovery.*,
    CASE
      WHEN allocated_recovery.static_recovery_eligible
       AND allocated_recovery.recoverable_amount_ex_vat > 0
        THEN 'canonical_preview_lines'
      ELSE 'blocked_for_pay'
    END AS effective_section,
    pg_catalog.md5(pg_catalog.jsonb_build_object(
      'contract_version', 1,
      'candidate_id', p_candidate_id::text,
      'pay_channel', allocated_recovery.pay_channel,
      'row_key', allocated_recovery.row_key,
      'selected_positive_headroom_ex_vat', allocated_recovery.selected_positive_headroom_ex_vat,
      'nominal_due_amount_ex_vat', allocated_recovery.nominal_due_amount_ex_vat,
      'recoverable_amount_ex_vat', allocated_recovery.recoverable_amount_ex_vat,
      'effective_section', CASE
        WHEN allocated_recovery.static_recovery_eligible
         AND allocated_recovery.recoverable_amount_ex_vat > 0
          THEN 'canonical_preview_lines'
        ELSE 'blocked_for_pay'
      END
    )::text) AS row_overlay_digest
  FROM allocated_recovery;

  SELECT pg_catalog.count(*)::integer
  INTO v_duplicate_recovery_identity_count
  FROM (
    SELECT
      COALESCE(NULLIF(overlay_row.base_row_json->>'finance_case_id', ''), overlay_row.row_key) AS recovery_identity,
      pg_catalog.upper(pg_catalog.btrim(COALESCE(overlay_row.base_row_json->>'line_type', ''))) AS recovery_family
    FROM pg_temp._bpay_recovery_selection_overlay AS overlay_row
    GROUP BY
      COALESCE(NULLIF(overlay_row.base_row_json->>'finance_case_id', ''), overlay_row.row_key),
      pg_catalog.upper(pg_catalog.btrim(COALESCE(overlay_row.base_row_json->>'line_type', '')))
    HAVING pg_catalog.count(*) > 1
  ) AS duplicate_identity;

  IF v_duplicate_recovery_identity_count > 0 THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_RECOVERY_SELECTION_OVERLAY_DUPLICATE_IDENTITY'
      USING ERRCODE = 'P0001';
  END IF;

  SELECT pg_catalog.count(*)::integer,
         pg_catalog.count(*) FILTER (WHERE overlay_row.effective_section = 'canonical_preview_lines')::integer,
         pg_catalog.count(*) FILTER (WHERE overlay_row.effective_section = 'blocked_for_pay')::integer,
         (
           SELECT pg_catalog.round(COALESCE(pg_catalog.sum(channel_headroom.selected_positive_headroom_ex_vat), 0), 2)
           FROM (
             SELECT overlay_channel.pay_channel,
                    pg_catalog.max(overlay_channel.selected_positive_headroom_ex_vat) AS selected_positive_headroom_ex_vat
             FROM pg_temp._bpay_recovery_selection_overlay AS overlay_channel
             GROUP BY overlay_channel.pay_channel
           ) AS channel_headroom
         ),
         pg_catalog.round(COALESCE(pg_catalog.sum(overlay_row.recoverable_amount_ex_vat), 0), 2),
         pg_catalog.md5(COALESCE(pg_catalog.string_agg(
           overlay_row.row_key || E'\x1f' || overlay_row.row_overlay_digest,
           E'\x1e' ORDER BY overlay_row.row_ordinal, overlay_row.id
         ), ''))
  INTO v_recovery_row_count,
       v_ready_recovery_count,
       v_blocked_recovery_count,
       v_selected_positive_amount,
       v_ready_recovery_amount,
       v_overlay_digest
  FROM pg_temp._bpay_recovery_selection_overlay AS overlay_row;

  WITH updated_rows AS (
    UPDATE public.banking_pay_workbench_preview_rows AS recovery_row
    SET selected = CASE
          WHEN overlay_row.effective_section = 'canonical_preview_lines'
            THEN overlay_row.explicitly_user_unselected IS NOT TRUE
          ELSE false
        END,
        selection_state = CASE
          WHEN overlay_row.effective_section = 'canonical_preview_lines'
            THEN CASE
              WHEN overlay_row.explicitly_user_unselected IS TRUE THEN 'UNSELECTED'
              ELSE 'SELECTED'
            END
          ELSE 'NOT_SELECTABLE'
        END,
        row_json = pg_catalog.jsonb_strip_nulls(
          overlay_row.base_row_json
          || pg_catalog.jsonb_build_object(
            'amount_ex_vat', -overlay_row.recoverable_amount_ex_vat,
            'preview_amount_ex_vat', -overlay_row.recoverable_amount_ex_vat,
            'amount_display', -overlay_row.recoverable_amount_ex_vat,
            'section_amount_ex_vat', -overlay_row.recoverable_amount_ex_vat,
            'section_amount_display', -overlay_row.recoverable_amount_ex_vat,
            'target_pay_ex_vat', -overlay_row.recoverable_amount_ex_vat,
            'ready_preview_amount_ex_vat', -overlay_row.recoverable_amount_ex_vat,
            'preview_component_amount_ex_vat', -overlay_row.recoverable_amount_ex_vat,
            'nominal_due_amount_ex_vat', overlay_row.nominal_due_amount_ex_vat,
            'recoverable_this_pay_run_ex_vat', overlay_row.recoverable_amount_ex_vat,
            'semantic_ordinary_positive_headroom_ex_vat', overlay_row.selected_positive_headroom_ex_vat,
            'retained_positive_headroom_ex_vat', overlay_row.selected_positive_headroom_ex_vat,
            'semantic_recovery_headroom_capped',
              overlay_row.recoverable_amount_ex_vat < overlay_row.nominal_due_amount_ex_vat,
            'readiness_state', CASE
              WHEN overlay_row.effective_section = 'canonical_preview_lines' THEN 'READY_TO_PAY'
              ELSE 'BLOCKED_FOR_PAY'
            END,
            'presentation_section', CASE
              WHEN overlay_row.effective_section = 'canonical_preview_lines' THEN 'READY_TO_PAY'
              ELSE 'BLOCKED_FOR_PAY'
            END,
            'presentation_reason', CASE
              WHEN overlay_row.effective_section = 'canonical_preview_lines'
                THEN overlay_row.base_presentation_reason
              WHEN overlay_row.static_recovery_eligible THEN 'NO_PAY_HEADROOM'
              ELSE overlay_row.base_presentation_reason
            END,
            'draftable', overlay_row.effective_section = 'canonical_preview_lines',
            'is_ready_for_draft', overlay_row.effective_section = 'canonical_preview_lines',
            'selection_allowed', overlay_row.effective_section = 'canonical_preview_lines',
            'is_excluded_from_allocation', overlay_row.effective_section <> 'canonical_preview_lines',
            'selected', CASE
              WHEN overlay_row.effective_section = 'canonical_preview_lines'
                THEN overlay_row.explicitly_user_unselected IS NOT TRUE
              ELSE false
            END,
            'selection_state', CASE
              WHEN overlay_row.effective_section = 'canonical_preview_lines'
               AND overlay_row.explicitly_user_unselected IS NOT TRUE
                THEN 'SELECTED'
              WHEN overlay_row.effective_section = 'canonical_preview_lines' THEN 'UNSELECTED'
              ELSE 'NOT_SELECTABLE'
            END,
            'selection_origin', CASE
              WHEN overlay_row.effective_section = 'canonical_preview_lines'
               AND overlay_row.explicitly_user_unselected IS TRUE
                THEN 'USER_EXPLICIT_DESELECT'
              WHEN overlay_row.effective_section = 'canonical_preview_lines'
                THEN 'SERVER_DEFAULT_NEWLY_ELIGIBLE'
              ELSE COALESCE(overlay_row.base_row_json->>'selection_origin', 'SERVER_HEADROOM_BLOCKED')
            END,
            'blocked_reason_codes', CASE
              WHEN overlay_row.effective_section = 'canonical_preview_lines'
                THEN overlay_row.base_blocked_reason_codes
              WHEN overlay_row.static_recovery_eligible
                THEN overlay_row.base_blocked_reason_codes || pg_catalog.jsonb_build_array('NO_PAY_HEADROOM')
              ELSE overlay_row.base_blocked_reason_codes
            END,
            'case_is_blocked', overlay_row.effective_section <> 'canonical_preview_lines',
            'selection_recovery_headroom_v1', pg_catalog.jsonb_build_object(
              'contract_version', 1,
              'candidate_id', p_candidate_id::text,
              'pay_channel', overlay_row.pay_channel,
              'physical_section', overlay_row.physical_section,
              'effective_section', overlay_row.effective_section,
              'selected_positive_headroom_ex_vat', overlay_row.selected_positive_headroom_ex_vat,
              'nominal_due_amount_ex_vat', overlay_row.nominal_due_amount_ex_vat,
              'recoverable_amount_ex_vat', overlay_row.recoverable_amount_ex_vat,
              'base_blocked_reason_codes', overlay_row.base_blocked_reason_codes,
              'base_presentation_reason', overlay_row.base_presentation_reason,
              'static_recovery_eligible', overlay_row.static_recovery_eligible,
              'overlay_digest', overlay_row.row_overlay_digest,
              'reason', v_reason,
              'updated_at_utc', v_now::text,
              'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
            ),
            'case_resolution_summary', CASE
              WHEN pg_catalog.jsonb_typeof(overlay_row.base_row_json->'case_resolution_summary') = 'object'
                THEN overlay_row.base_row_json->'case_resolution_summary'
                  || pg_catalog.jsonb_build_object('due_amount_ex_vat', overlay_row.recoverable_amount_ex_vat)
              ELSE pg_catalog.jsonb_build_object('due_amount_ex_vat', overlay_row.recoverable_amount_ex_vat)
            END
          )
        ),
        updated_at_utc = v_now
    FROM pg_temp._bpay_recovery_selection_overlay AS overlay_row
    WHERE recovery_row.id = overlay_row.id
    RETURNING recovery_row.id,
              overlay_row.effective_section,
              overlay_row.current_selected,
              recovery_row.selected
  )
  SELECT pg_catalog.count(*)::integer,
         pg_catalog.count(*) FILTER (
           WHERE updated_rows.current_selected IS TRUE
             AND updated_rows.selected IS FALSE
         )::integer
  INTO v_updated_count, v_forced_deselected_count
  FROM updated_rows;

  SELECT COALESCE(
           pg_catalog.jsonb_agg(selected_row.id::text ORDER BY selected_row.row_ordinal, selected_row.id),
           '[]'::jsonb
         ),
         pg_catalog.count(*)::integer
  INTO v_selected_preview_row_ids,
       v_selected_row_count
  FROM public.banking_pay_workbench_preview_rows AS selected_row
  WHERE selected_row.session_id = p_session_id
    AND selected_row.session_version = v_session.version
    AND selected_row.status = 'READY'
    AND selected_row.selected IS TRUE
    AND pg_catalog.upper(pg_catalog.btrim(COALESCE(selected_row.selection_state, ''))) = 'SELECTED';

  v_session_ready := pg_catalog.lower(pg_catalog.btrim(COALESCE(
    v_session.progress_json->>'ready',
    v_session.progress_json->>'session_ready',
    v_session.progress_json->>'ready_flag',
    'false'
  ))) IN ('true', 't', '1', 'yes', 'y', 'on');

  SELECT COALESCE(
           pg_catalog.jsonb_agg(pg_catalog.to_jsonb(blocker.value) ORDER BY blocker.ordinality),
           '[]'::jsonb
         )
  INTO v_draft_blocker_codes
  FROM pg_catalog.jsonb_array_elements_text(
    CASE
      WHEN pg_catalog.jsonb_typeof(v_session.progress_json->'draft_blocker_codes') = 'array'
        THEN v_session.progress_json->'draft_blocker_codes'
      ELSE '[]'::jsonb
    END
  ) WITH ORDINALITY AS blocker(value, ordinality)
  WHERE pg_catalog.upper(pg_catalog.btrim(blocker.value)) <> 'NO_SELECTED_ROWS';

  IF v_session_ready AND COALESCE(v_selected_row_count, 0) = 0 THEN
    v_draft_blocker_codes := COALESCE(v_draft_blocker_codes, '[]'::jsonb)
      || pg_catalog.jsonb_build_array('NO_SELECTED_ROWS');
  END IF;

  UPDATE public.banking_pay_workbench_sessions AS session_update
  SET selected_row_count = COALESCE(v_selected_row_count, 0),
      server_selected_preview_row_ids = CASE
        WHEN COALESCE(session_update.server_selected_preview_row_ids_provided, false)
          THEN COALESCE(v_selected_preview_row_ids, '[]'::jsonb)
        ELSE session_update.server_selected_preview_row_ids
      END,
      progress_json = COALESCE(session_update.progress_json, '{}'::jsonb)
        || pg_catalog.jsonb_build_object(
          'selection_recovery_headroom_v1', pg_catalog.jsonb_build_object(
            'contract_version', 1,
            'candidate_id', p_candidate_id::text,
            'selected_positive_headroom_ex_vat', v_selected_positive_amount,
            'ready_recovery_count', v_ready_recovery_count,
            'blocked_recovery_count', v_blocked_recovery_count,
            'recoverable_amount_ex_vat', v_ready_recovery_amount,
            'overlay_digest', v_overlay_digest,
            'reason', v_reason,
            'updated_at_utc', v_now::text
          ),
          'selected_row_count', COALESCE(v_selected_row_count, 0),
          'selected_eligible_ready_row_count', COALESCE(v_selected_row_count, 0),
          'selected_rows_available', COALESCE(v_selected_row_count, 0) > 0,
          'ready_for_draft', v_session_ready AND COALESCE(v_selected_row_count, 0) > 0,
          'can_create_draft', v_session_ready AND COALESCE(v_selected_row_count, 0) > 0,
          'draft_blocker_codes', COALESCE(v_draft_blocker_codes, '[]'::jsonb),
          'blocker_codes', COALESCE(v_draft_blocker_codes, '[]'::jsonb)
        ),
      updated_at_utc = v_now
  WHERE session_update.id = p_session_id;

  UPDATE public.banking_pay_workbench_session_scope AS scope_update
  SET certified_preview_publication_attestation_json = CASE
        WHEN v_scope.certified_preview_publication_attestation_json->>'attestation_version'
             = 'CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3'
          THEN COALESCE(scope_update.certified_preview_publication_attestation_json, '{}'::jsonb)
            || pg_catalog.jsonb_build_object(
              'selection_recovery_headroom_v1', pg_catalog.jsonb_build_object(
                'contract_version', 1,
                'selected_positive_headroom_ex_vat', v_selected_positive_amount,
                'ready_recovery_count', v_ready_recovery_count,
                'blocked_recovery_count', v_blocked_recovery_count,
                'recoverable_amount_ex_vat', v_ready_recovery_amount,
                'overlay_digest', v_overlay_digest,
                'reason', v_reason,
                'updated_at_utc', v_now::text
              )
            )
        ELSE scope_update.certified_preview_publication_attestation_json
      END,
      updated_at_utc = v_now
  WHERE scope_update.id = v_scope.id;

  RETURN pg_catalog.jsonb_build_object(
    'ok', true,
    'action', 'SELECTION_DEPENDENT_RECOVERY_HEADROOM_APPLIED',
    'session_id', p_session_id::text,
    'candidate_id', p_candidate_id::text,
    'recovery_row_count', v_recovery_row_count,
    'ready_recovery_count', v_ready_recovery_count,
    'blocked_recovery_count', v_blocked_recovery_count,
    'forced_deselected_count', v_forced_deselected_count,
    'updated_count', v_updated_count,
    'selected_positive_headroom_ex_vat', v_selected_positive_amount,
    'recoverable_amount_ex_vat', v_ready_recovery_amount,
    'overlay_digest', v_overlay_digest,
    'physical_sections_changed', false,
    'source_rows_changed', false,
    'post_draft_artifacts_touched', false,
    'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
  );
END;
$function$;

ALTER FUNCTION private.pay_workbench_recovery_selection_overlay_apply_v1(
  uuid, uuid, jsonb
) OWNER TO postgres;

REVOKE ALL ON FUNCTION private.pay_workbench_recovery_selection_overlay_apply_v1(
  uuid, uuid, jsonb
) FROM PUBLIC, anon, authenticated, service_role;

GRANT EXECUTE ON FUNCTION private.pay_workbench_recovery_selection_overlay_apply_v1(
  uuid, uuid, jsonb
) TO postgres;


-- Bounded V3 publication core.  Descriptor JSON is transport only; the
-- single-candidate publisher locks and rederives every durable authority.
CREATE OR REPLACE FUNCTION private.pay_workbench_publish_certified_source_preview_page_v1(
  p_session_id uuid,
  p_candidate_authorities_json jsonb,
  p_publication_options_json jsonb DEFAULT '{}'::jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
PARALLEL UNSAFE
SECURITY INVOKER
SET search_path TO ''
AS $function$
DECLARE
  v_descriptor jsonb;
  v_ordinality bigint;
  v_candidate_id uuid;
  v_result jsonb;
  v_results jsonb := '[]'::jsonb;
  v_count integer := 0;
  v_descriptor_options jsonb := '{}'::jsonb;
  v_authority_kind text;
  v_original_source_build_run_id uuid;
  v_original_source_publication_id uuid;
  v_source_publication_id uuid;
  v_publication_identity_write_enabled boolean := false;
  v_publication_identity_enforce_enabled boolean := false;
  v_current_source_change_seq bigint;
  v_current_dirty_generation bigint;
  v_expected_source_count integer;
  v_staged_source_count integer;
  v_expected_source_identity_digest text;
  v_staged_source_identity_digest text;
  v_progress_result jsonb := '{}'::jsonb;
BEGIN
  IF p_session_id IS NULL
     OR pg_catalog.jsonb_typeof(COALESCE(p_candidate_authorities_json, '[]'::jsonb)) <> 'array'
     OR pg_catalog.jsonb_array_length(COALESCE(p_candidate_authorities_json, '[]'::jsonb)) > 100
     OR pg_catalog.jsonb_typeof(COALESCE(p_publication_options_json, '{}'::jsonb)) <> 'object' THEN
    RAISE EXCEPTION 'CERTIFIED_SOURCE_PREVIEW_PAGE_ARGUMENT_INVALID'
      USING ERRCODE='P0001', DETAIL=pg_catalog.jsonb_build_object(
        'code','CERTIFIED_SOURCE_PREVIEW_PAGE_ARGUMENT_INVALID'
      )::text;
  END IF;

  SELECT COALESCE(setting.banking_pay_source_publication_identity_enforce_v1_enabled,false)
  INTO v_publication_identity_enforce_enabled
  FROM public.settings_defaults AS setting
  WHERE setting.id=1;

  SELECT COALESCE(setting.banking_pay_source_publication_identity_write_v1_enabled,false),
         COALESCE(setting.banking_pay_source_publication_identity_enforce_v1_enabled,false)
  INTO v_publication_identity_write_enabled,v_publication_identity_enforce_enabled
  FROM public.settings_defaults AS setting
  WHERE setting.id=1;

  IF (
    SELECT pg_catalog.count(*)
    FROM pg_catalog.jsonb_array_elements(COALESCE(p_candidate_authorities_json,'[]'::jsonb)) AS descriptor(value)
  ) IS DISTINCT FROM (
    SELECT pg_catalog.count(DISTINCT descriptor.value->>'candidate_id')
    FROM pg_catalog.jsonb_array_elements(COALESCE(p_candidate_authorities_json,'[]'::jsonb)) AS descriptor(value)
  ) THEN
    RAISE EXCEPTION 'CERTIFIED_SOURCE_PREVIEW_PAGE_DUPLICATE_CANDIDATE'
      USING ERRCODE='P0001',DETAIL=pg_catalog.jsonb_build_object(
        'code','CERTIFIED_SOURCE_PREVIEW_PAGE_DUPLICATE_CANDIDATE'
      )::text;
  END IF;

  FOR v_descriptor, v_ordinality IN
    SELECT descriptor.value, descriptor.ordinality
    FROM pg_catalog.jsonb_array_elements(COALESCE(p_candidate_authorities_json, '[]'::jsonb))
      WITH ORDINALITY AS descriptor(value, ordinality)
    ORDER BY descriptor.value->>'candidate_id', descriptor.ordinality
  LOOP
    IF pg_catalog.jsonb_typeof(v_descriptor) <> 'object'
       OR COALESCE(v_descriptor->>'candidate_id','')
            !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       OR COALESCE(v_descriptor->>'economic_build_id','')
            !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       OR COALESCE(v_descriptor->>'source_build_run_id','')
            !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       OR COALESCE(v_descriptor->>'completion_job_id','')
            !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       OR COALESCE(v_descriptor->>'source_change_seq','') !~ '^[0-9]{1,18}$'
       OR COALESCE(v_descriptor->>'session_version','') !~ '^[1-9][0-9]{0,18}$' THEN
      RAISE EXCEPTION 'CERTIFIED_SOURCE_PREVIEW_PAGE_DESCRIPTOR_INVALID'
        USING ERRCODE='P0001', DETAIL=pg_catalog.jsonb_build_object(
          'code','CERTIFIED_SOURCE_PREVIEW_PAGE_DESCRIPTOR_INVALID',
          'ordinality',v_ordinality
        )::text;
    END IF;

    v_candidate_id := (v_descriptor->>'candidate_id')::uuid;

    -- The page is sorted by candidate UUID, so candidate serial locks are
    -- acquired in one deterministic global order.  This makes source-run
    -- adoption safe against full builds, clone finalisation and dirty apply.
    PERFORM pg_catalog.pg_advisory_xact_lock(
      pg_catalog.hashtextextended(
        public._pay_workbench_candidate_serial_key(v_candidate_id),
        24062027
      )
    );

    PERFORM 1
    FROM public.candidates AS candidate_row
    WHERE candidate_row.id=v_candidate_id
    FOR UPDATE;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'CERTIFIED_SOURCE_PREVIEW_PAGE_CANDIDATE_MISSING'
        USING ERRCODE='P0001',DETAIL=pg_catalog.jsonb_build_object(
          'code','CERTIFIED_SOURCE_PREVIEW_PAGE_CANDIDATE_MISSING',
          'candidate_id',v_candidate_id
        )::text;
    END IF;

    PERFORM 1
    FROM private.banking_pay_workbench_candidate_scope_registry AS locked_registry
    WHERE locked_registry.candidate_id=v_candidate_id
    FOR UPDATE;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'CERTIFIED_SOURCE_PREVIEW_PAGE_REGISTRY_MISSING'
        USING ERRCODE='P0001',DETAIL=pg_catalog.jsonb_build_object(
          'code','CERTIFIED_SOURCE_PREVIEW_PAGE_REGISTRY_MISSING',
          'candidate_id',v_candidate_id
        )::text;
    END IF;

    v_descriptor_options := COALESCE(p_publication_options_json,'{}'::jsonb)
      || CASE WHEN pg_catalog.jsonb_typeof(v_descriptor->'publication_options_json')='object'
        THEN v_descriptor->'publication_options_json' ELSE '{}'::jsonb END;
    v_authority_kind := pg_catalog.upper(pg_catalog.btrim(COALESCE(
      v_descriptor_options->>'authority_kind','BOUNDED_FULL_SOURCE_BUILD'
    )));

    IF v_authority_kind='CERTIFIED_CANCELLATION_REVERSION' THEN
      IF COALESCE(v_descriptor_options->>'original_source_build_run_id','')
           !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
         OR COALESCE(v_descriptor_options->>'original_source_publication_id','')
              !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
         OR COALESCE(v_descriptor_options->>'source_session_id','') IS DISTINCT FROM p_session_id::text
         OR COALESCE(v_descriptor_options->>'source_count','') !~ '^[0-9]{1,9}$'
         OR COALESCE(v_descriptor->>'source_identity_digest','') !~ '^[0-9a-f]{32}$' THEN
        RAISE EXCEPTION 'CERTIFIED_CANCELLATION_REVERSION_SOURCE_INVALID'
          USING ERRCODE='P0001',DETAIL=pg_catalog.jsonb_build_object(
            'code','CERTIFIED_CANCELLATION_REVERSION_SOURCE_INVALID',
            'candidate_id',v_candidate_id
          )::text;
      END IF;

      v_original_source_build_run_id := (v_descriptor_options->>'original_source_build_run_id')::uuid;
      v_original_source_publication_id := CASE
        WHEN COALESCE(v_descriptor_options->>'original_source_publication_id','')
          ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        THEN (v_descriptor_options->>'original_source_publication_id')::uuid
        ELSE NULL::uuid
      END;
      v_current_source_change_seq := (v_descriptor->>'source_change_seq')::bigint;
      v_expected_source_count := (v_descriptor_options->>'source_count')::integer;
      v_expected_source_identity_digest := v_descriptor->>'source_identity_digest';
      v_source_publication_id := CASE WHEN v_publication_identity_write_enabled
        THEN private.pay_workbench_source_publication_identity_v1(
          p_session_id,v_candidate_id,(v_descriptor->>'session_version')::bigint,
          v_current_source_change_seq,(v_descriptor->>'source_build_run_id')::uuid
        ) ELSE NULL::uuid END;

      SELECT COALESCE(change_counter.scope_change_generation,0)
      INTO v_current_dirty_generation
      FROM public.app_change_counters AS change_counter
      WHERE change_counter.entity_key='pay_candidate:'||v_candidate_id::text;

      UPDATE private.banking_pay_workbench_candidate_scope_registry AS registry
      SET current_source_change_seq=v_current_source_change_seq,
          evaluated_generation=GREATEST(COALESCE(registry.evaluated_generation,0),v_current_dirty_generation),
          current_build_id=(v_descriptor->>'economic_build_id')::uuid,
          initialisation_status='READY',
          last_evaluated_at_utc=pg_catalog.clock_timestamp(),
          failure_json='{}'::jsonb,
          updated_at_utc=pg_catalog.clock_timestamp()
      WHERE registry.candidate_id=v_candidate_id
        AND registry.dirty_generation=v_current_dirty_generation
        AND registry.current_source_change_seq <= v_current_source_change_seq;

      IF NOT FOUND THEN
        RAISE EXCEPTION 'CERTIFIED_CANCELLATION_REVERSION_REGISTRY_STALE'
          USING ERRCODE='P0001',DETAIL=pg_catalog.jsonb_build_object(
            'code','CERTIFIED_CANCELLATION_REVERSION_REGISTRY_STALE',
            'candidate_id',v_candidate_id
          )::text;
      END IF;

      UPDATE public.banking_pay_workbench_session_candidate_state AS candidate_state
      SET source_change_seq=v_current_source_change_seq,
          session_version=(v_descriptor->>'session_version')::bigint,
          status='PENDING',
          pending_job_id=NULL,
          last_error_json='{}'::jsonb,
          updated_at_utc=pg_catalog.clock_timestamp()
      WHERE candidate_state.session_id=p_session_id
        AND candidate_state.candidate_id=v_candidate_id;

      INSERT INTO public.banking_pay_workbench_candidate_source_lines(
        session_id,candidate_id,session_version,source_change_seq,source_build_run_id,source_publication_id,
        source_ordinal,line_key,parent_line_key,split_suffix,timesheet_id,section,
        source_row_json,economic_key_json,contract_json,pay_channel_scope,
        refresh_scope_kind,status,created_at_utc,updated_at_utc
      )
      SELECT
        p_session_id,source_row.candidate_id,(v_descriptor->>'session_version')::bigint,
        v_current_source_change_seq,(v_descriptor->>'source_build_run_id')::uuid,
        v_source_publication_id,
        source_row.source_ordinal,source_row.line_key,source_row.parent_line_key,
        source_row.split_suffix,source_row.timesheet_id,source_row.section,
        source_row.source_row_json || pg_catalog.jsonb_build_object(
          'cancellation_reversion_run_id',v_descriptor->>'source_build_run_id',
          'original_source_build_run_id',v_original_source_build_run_id::text,
          'policy_x_authority_scope','PRE_DRAFT_LIVE_TRUTH'
        ),
        source_row.economic_key_json,
        COALESCE(source_row.contract_json,'{}'::jsonb) || pg_catalog.jsonb_build_object(
          'authority_kind','CERTIFIED_CANCELLATION_REVERSION',
          'cancellation_request_id',v_descriptor_options->>'cancellation_request_id',
          'cancellation_work_item_id',v_descriptor_options->>'cancellation_work_item_id',
          'financial_reversion_digest',v_descriptor_options->>'financial_reversion_digest'
        ),
        source_row.pay_channel_scope,'CANDIDATE_FULL_LIVE','DIRTY',
        pg_catalog.clock_timestamp(),pg_catalog.clock_timestamp()
      FROM public.banking_pay_workbench_candidate_source_lines AS source_row
      WHERE source_row.session_id=p_session_id
        AND source_row.candidate_id=v_candidate_id
        AND source_row.source_build_run_id=v_original_source_build_run_id
        AND source_row.source_publication_id=v_original_source_publication_id
        AND NOT EXISTS (
          SELECT 1
          FROM public.banking_pay_workbench_candidate_source_lines AS replay_row
          WHERE replay_row.session_id=p_session_id
            AND replay_row.candidate_id=v_candidate_id
            AND replay_row.source_build_run_id=(v_descriptor->>'source_build_run_id')::uuid
            AND replay_row.source_change_seq=v_current_source_change_seq
            AND replay_row.source_publication_id IS NOT DISTINCT FROM v_source_publication_id
            AND replay_row.source_ordinal=source_row.source_ordinal
            AND replay_row.line_key=source_row.line_key
        );

      SELECT pg_catalog.count(*)::integer,
             pg_catalog.md5(COALESCE(pg_catalog.string_agg(
               public.pay_workbench_preview_section_from_line_json(staged_row.source_row_json)
                 || E'\x1f' || staged_row.line_key || E'\x1f' || staged_row.source_ordinal::text,
               E'\x1e' ORDER BY staged_row.source_ordinal,
                 public.pay_workbench_preview_section_from_line_json(staged_row.source_row_json),
                 staged_row.line_key
             ),''))
      INTO v_staged_source_count,v_staged_source_identity_digest
      FROM public.banking_pay_workbench_candidate_source_lines AS staged_row
      WHERE staged_row.session_id=p_session_id
        AND staged_row.candidate_id=v_candidate_id
        AND staged_row.source_build_run_id=(v_descriptor->>'source_build_run_id')::uuid
        AND staged_row.source_change_seq=v_current_source_change_seq
        AND staged_row.source_publication_id IS NOT DISTINCT FROM v_source_publication_id
        AND staged_row.status IN ('DIRTY','CURRENT');

      IF v_staged_source_count IS DISTINCT FROM v_expected_source_count THEN
        RAISE EXCEPTION 'CERTIFIED_CANCELLATION_REVERSION_SOURCE_COUNT_MISMATCH'
          USING ERRCODE='P0001',DETAIL=pg_catalog.jsonb_build_object(
            'code','CERTIFIED_CANCELLATION_REVERSION_SOURCE_COUNT_MISMATCH',
            'candidate_id',v_candidate_id,
            'expected_count',v_expected_source_count,
            'staged_count',v_staged_source_count
          )::text;
      END IF;
      IF v_staged_source_identity_digest IS DISTINCT FROM v_expected_source_identity_digest THEN
        RAISE EXCEPTION 'CERTIFIED_CANCELLATION_REVERSION_SOURCE_REPLAY_CONFLICT'
          USING ERRCODE='23514',DETAIL=pg_catalog.jsonb_build_object(
            'code','CERTIFIED_CANCELLATION_REVERSION_SOURCE_REPLAY_CONFLICT',
            'candidate_id',v_candidate_id,
            'expected_digest',v_expected_source_identity_digest,
            'staged_digest',v_staged_source_identity_digest
          )::text;
      END IF;

      UPDATE public.banking_pay_workbench_candidate_source_lines AS old_current
      SET status='SUPERSEDED',updated_at_utc=pg_catalog.clock_timestamp()
      WHERE old_current.session_id=p_session_id
        AND old_current.candidate_id=v_candidate_id
        AND old_current.status='CURRENT'
        AND old_current.source_build_run_id IS DISTINCT FROM (v_descriptor->>'source_build_run_id')::uuid;

      UPDATE public.banking_pay_workbench_candidate_source_lines AS staged_row
      SET status='CURRENT',updated_at_utc=pg_catalog.clock_timestamp()
      WHERE staged_row.session_id=p_session_id
        AND staged_row.candidate_id=v_candidate_id
        AND staged_row.source_build_run_id=(v_descriptor->>'source_build_run_id')::uuid
        AND staged_row.source_change_seq=v_current_source_change_seq
        AND staged_row.source_publication_id IS NOT DISTINCT FROM v_source_publication_id
        AND staged_row.status='DIRTY';
    END IF;

    v_result := private.pay_workbench_publish_certified_source_preview_v1(
      p_session_id,
      v_candidate_id,
      (v_descriptor->>'economic_build_id')::uuid,
      (v_descriptor->>'source_build_run_id')::uuid,
      (v_descriptor->>'source_change_seq')::bigint,
      (v_descriptor->>'session_version')::bigint,
      (v_descriptor->>'completion_job_id')::uuid,
      COALESCE(v_descriptor->>'refresh_scope_kind','CANDIDATE_FULL_LIVE'),
      CASE WHEN pg_catalog.jsonb_typeof(v_descriptor->'targeted_timesheet_ids')='array'
        THEN v_descriptor->'targeted_timesheet_ids' ELSE '[]'::jsonb END,
      CASE WHEN pg_catalog.jsonb_typeof(v_descriptor->'linked_timesheet_ids')='array'
        THEN v_descriptor->'linked_timesheet_ids' ELSE '[]'::jsonb END,
      v_descriptor_options
    );

    IF v_authority_kind='CERTIFIED_CANCELLATION_REVERSION' THEN
      UPDATE public.banking_pay_workbench_session_scope AS completed_scope
      SET status=CASE WHEN v_expected_source_count=0 THEN 'SOURCE_EMPTY' ELSE 'MATERIALISED' END,
          pending_job_id=NULL,dirty=false,error_json='{}'::jsonb,
          updated_at_utc=pg_catalog.clock_timestamp()
      WHERE completed_scope.session_id=p_session_id
        AND completed_scope.candidate_id=v_candidate_id
        AND completed_scope.certified_preview_publication_parity_ok IS TRUE
        AND completed_scope.certified_preview_publication_source_build_run_id
          =(v_descriptor->>'source_build_run_id')::uuid;

      UPDATE public.banking_pay_workbench_session_candidate_state AS completed_state
      SET status='READY',pending_job_id=NULL,last_error_json='{}'::jsonb,
          last_recomputed_at_utc=pg_catalog.clock_timestamp(),
          updated_at_utc=pg_catalog.clock_timestamp()
      WHERE completed_state.session_id=p_session_id
        AND completed_state.candidate_id=v_candidate_id
        AND completed_state.source_change_seq=v_current_source_change_seq;
    END IF;

    v_results := v_results || pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object('candidate_id',v_candidate_id::text,'publication',v_result)
    );
    v_count := v_count + 1;
  END LOOP;

  IF v_count > 0 THEN
    v_progress_result := public.pay_workbench_session_recompute_progress_counters(
      p_session_id,true,'CERTIFIED_SOURCE_PREVIEW_PUBLICATION_PAGE_V1',true
    );
  END IF;

  RETURN pg_catalog.jsonb_build_object(
    'ok',true,
    'contract_version','CERTIFIED_SOURCE_PREVIEW_PUBLICATION_PAGE_V1',
    'session_id',p_session_id::text,
    'candidate_count',v_count,
    'candidate_results',v_results,
    'progress',v_progress_result
  );
END;
$function$;

ALTER FUNCTION private.pay_workbench_publish_certified_source_preview_page_v1(uuid,jsonb,jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_publish_certified_source_preview_page_v1(uuid,jsonb,jsonb)
  FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION private.pay_workbench_publish_certified_source_preview_page_v1(uuid,jsonb,jsonb)
  TO postgres;


-- Read-only admission for exact untouched-Draft and post-execution/pre-provider
-- cancellation reversion.  It never calculates pay and never changes finance
-- or Workbench state.  The completed Draft freezes both the immutable V3
-- source to restore and the live sequence/generation accepted after Draft
-- effects.  A later reversion is admitted only while those fences remain exact.
CREATE OR REPLACE FUNCTION private.pay_workbench_cancel_reversion_admission_page_v1(
  p_correction_request_id uuid,
  p_operation_id uuid,
  p_session_id uuid,
  p_work_item_ids uuid[],
  p_options_json jsonb DEFAULT '{}'::jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
PARALLEL UNSAFE
SECURITY DEFINER
SET search_path TO ''
AS $function$
DECLARE
  v_count integer := 0;
  v_results jsonb := '[]'::jsonb;
  v_publication_identity_enforce_enabled boolean := false;
  v_mode text := pg_catalog.upper(pg_catalog.btrim(COALESCE(
    p_options_json->>'mode','POST_FINANCIAL'
  )));
BEGIN
  SELECT COALESCE(setting.banking_pay_source_publication_identity_enforce_v1_enabled,false)
  INTO v_publication_identity_enforce_enabled
  FROM public.settings_defaults AS setting
  WHERE setting.id=1;

  IF p_correction_request_id IS NULL OR p_operation_id IS NULL OR p_session_id IS NULL
     OR p_work_item_ids IS NULL OR pg_catalog.cardinality(p_work_item_ids) > 100
     OR v_mode NOT IN ('DRAFT_OVERLAY_PREFLIGHT','PRE_FINANCIAL','POST_FINANCIAL','OBSERVE_ONLY')
     OR pg_catalog.jsonb_typeof(COALESCE(p_options_json,'{}'::jsonb)) <> 'object' THEN
    RAISE EXCEPTION 'CANCELLATION_REVERSION_ADMISSION_ARGUMENT_INVALID'
      USING ERRCODE='P0001', DETAIL=pg_catalog.jsonb_build_object(
        'code','CANCELLATION_REVERSION_ADMISSION_ARGUMENT_INVALID'
      )::text;
  END IF;

  IF EXISTS (
    SELECT 1 FROM pg_catalog.jsonb_object_keys(COALESCE(p_options_json,'{}'::jsonb)) AS option_key(key)
    WHERE option_key.key NOT IN ('mode','candidate_ids')
  ) OR (
    v_mode='DRAFT_OVERLAY_PREFLIGHT'
    AND (
      pg_catalog.jsonb_typeof(COALESCE(p_options_json->'candidate_ids','[]'::jsonb))<>'array'
      OR pg_catalog.jsonb_array_length(COALESCE(p_options_json->'candidate_ids','[]'::jsonb))<1
      OR pg_catalog.jsonb_array_length(COALESCE(p_options_json->'candidate_ids','[]'::jsonb))>100
    )
  ) OR (
    v_mode<>'DRAFT_OVERLAY_PREFLIGHT' AND pg_catalog.cardinality(p_work_item_ids)<1
  ) THEN
    RAISE EXCEPTION 'CANCELLATION_REVERSION_ADMISSION_ARGUMENT_INVALID'
      USING ERRCODE='P0001', DETAIL=pg_catalog.jsonb_build_object(
        'code','CANCELLATION_REVERSION_ADMISSION_ARGUMENT_INVALID','mode',v_mode
      )::text;
  END IF;

  WITH requested_scope AS (
    SELECT
      candidate_value.value::uuid AS requested_candidate_id,
      NULL::uuid AS work_id,
      candidate_value.ordinality
    FROM pg_catalog.jsonb_array_elements_text(
      CASE WHEN v_mode='DRAFT_OVERLAY_PREFLIGHT'
        THEN COALESCE(p_options_json->'candidate_ids','[]'::jsonb)
        ELSE '[]'::jsonb END
    ) WITH ORDINALITY AS candidate_value(value,ordinality)
    WHERE candidate_value.value
      ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'

    UNION ALL

    SELECT NULL::uuid,work_scope.work_id,work_scope.ordinality
    FROM pg_catalog.unnest(
      CASE WHEN v_mode<>'DRAFT_OVERLAY_PREFLIGHT' THEN p_work_item_ids ELSE ARRAY[]::uuid[] END
    ) WITH ORDINALITY AS work_scope(work_id,ordinality)
  ), authority AS (
    SELECT
      requested_scope.ordinality,
      work_row.id AS work_item_id,
      COALESCE(work_row.candidate_id,batch_candidate.candidate_id) AS candidate_id,
      COALESCE(work_row.pay_batch_candidate_id,batch_candidate.id) AS pay_batch_candidate_id,
      COALESCE(work_row.pay_batch_id,request_row.pay_batch_id) AS pay_batch_id,
      work_row.status AS work_status,
      work_row.result_json AS work_result,
      request_row.plan_json,
      request_row.selection_json,
      request_row.created_at_utc AS request_created_at_utc,
      request_row.selection_json->'draft_overlay_fast_pre_request_authorities'
        ->COALESCE(work_row.candidate_id,batch_candidate.candidate_id)::text
        AS draft_overlay_pre_request_authority,
      correction_operation.input_json->'draft_overlay_fast_start_authorities'
        ->COALESCE(work_row.candidate_id,batch_candidate.candidate_id)::text
        AS draft_overlay_start_authority,
      batch_row.source_workbench_session_id,
      batch_row.source_session_version,
      batch_row.source_snapshot_run_id,
      pg_catalog.upper(pg_catalog.btrim(COALESCE(batch_row.execution_commit_state,'NOT_SUBMITTED'))) AS execution_commit_state,
      batch_candidate.settlement_status,
      batch_candidate.settled_at_utc,
      draft_operation.id AS draft_operation_id,
      draft_scope.allocation_basis_json,
      draft_scope.allocation_basis_json->'source_publication_attestation' AS frozen_attestation,
      draft_scope.allocation_basis_json->>'source_build_run_id' AS frozen_source_build_run_id,
      draft_scope.allocation_basis_json->>'source_publication_id' AS frozen_source_publication_id,
      draft_scope.allocation_basis_json->'post_draft_authority' AS post_draft_authority,
      COALESCE(change_counter.seq,0) AS live_source_change_seq,
      COALESCE(change_counter.scope_change_generation,0) AS live_dirty_generation,
      registry.current_source_change_seq AS registry_source_change_seq,
      registry.dirty_generation AS registry_dirty_generation,
      candidate_state.source_change_seq AS candidate_state_source_change_seq,
      candidate_state.session_version AS candidate_state_session_version,
      request_owned_dirty.job_id AS current_request_owned_dirty_job_id
    FROM requested_scope
    LEFT JOIN public.pay_payment_correction_work_items AS work_row
      ON work_row.id=requested_scope.work_id
     AND work_row.correction_request_id=p_correction_request_id
    JOIN public.pay_payment_correction_requests AS request_row
      ON request_row.id=p_correction_request_id
    JOIN public.banking_pay_operations AS correction_operation
      ON correction_operation.id=p_operation_id
     AND correction_operation.operation_type='PAYMENT_CORRECTION'
     AND correction_operation.input_json->>'correction_request_id'=request_row.id::text
    JOIN public.pay_batch_candidates AS batch_candidate
      ON batch_candidate.pay_batch_id=request_row.pay_batch_id
     AND batch_candidate.candidate_id=COALESCE(work_row.candidate_id,requested_scope.requested_candidate_id)
    JOIN public.pay_payment_correction_request_candidates AS request_candidate
      ON request_candidate.correction_request_id=request_row.id
     AND request_candidate.pay_batch_candidate_id=batch_candidate.id
    JOIN public.pay_batches AS batch_row ON batch_row.id=request_row.pay_batch_id
    LEFT JOIN LATERAL (
      SELECT operation_row.id
      FROM public.banking_pay_operations AS operation_row
      WHERE operation_row.operation_type='DRAFT_CREATE'
        AND operation_row.status='COMPLETE'
        AND (
          operation_row.pay_batch_id=request_row.pay_batch_id
          OR EXISTS (
            SELECT 1
            FROM public.banking_pay_operation_candidate_scope AS operation_scope_link
            WHERE operation_scope_link.operation_id=operation_row.id
              AND operation_scope_link.candidate_id=batch_candidate.candidate_id
              AND operation_scope_link.pay_batch_id=request_row.pay_batch_id
              AND operation_scope_link.status NOT IN ('FAILED','CANCELLED','SUPERSEDED')
          )
        )
      ORDER BY operation_row.completed_at_utc DESC NULLS LAST, operation_row.created_at_utc DESC
      LIMIT 1
    ) AS draft_operation ON true
    LEFT JOIN public.banking_pay_operation_candidate_scope AS draft_scope
      ON draft_scope.operation_id=draft_operation.id
      AND draft_scope.candidate_id=batch_candidate.candidate_id
      AND draft_scope.pay_batch_id=request_row.pay_batch_id
     AND draft_scope.status NOT IN ('FAILED','CANCELLED','SUPERSEDED')
    LEFT JOIN public.app_change_counters AS change_counter
      ON change_counter.entity_key='pay_candidate:'||batch_candidate.candidate_id::text
    LEFT JOIN private.banking_pay_workbench_candidate_scope_registry AS registry
      ON registry.candidate_id=batch_candidate.candidate_id
    LEFT JOIN public.banking_pay_workbench_session_candidate_state AS candidate_state
      ON candidate_state.session_id=p_session_id
     AND candidate_state.candidate_id=batch_candidate.candidate_id
    LEFT JOIN LATERAL (
      SELECT dirty_job.id AS job_id
      FROM public.banking_pay_workbench_jobs AS dirty_job
      WHERE dirty_job.id=CASE
          WHEN COALESCE(
            correction_operation.input_json->'draft_overlay_fast_start_authorities'
              ->COALESCE(work_row.candidate_id,batch_candidate.candidate_id)::text
              ->>'request_owned_dirty_job_id',''
          ) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN (
            correction_operation.input_json->'draft_overlay_fast_start_authorities'
              ->COALESCE(work_row.candidate_id,batch_candidate.candidate_id)::text
              ->>'request_owned_dirty_job_id'
          )::uuid
          ELSE NULL::uuid END
        AND dirty_job.candidate_id=COALESCE(work_row.candidate_id,batch_candidate.candidate_id)
        -- The transition-trigger job is normally candidate-global
        -- (session_id NULL).  If a session-scoped owner is supplied it must
        -- belong to this exact source Workbench session.
        AND (
          dirty_job.session_id IS NULL
          OR dirty_job.session_id=p_session_id
        )
        AND dirty_job.job_type='WORKBENCH_CANDIDATE_DIRTY_APPLY'
        AND (
          dirty_job.status IN ('QUEUED','RUNNING')
          OR (
            dirty_job.status='SUCCEEDED'
            AND COALESCE(
              (dirty_job.payload_json->>'coalesced_to_current_refresh_authority')::boolean,
              false
            )
            AND COALESCE(dirty_job.payload_json->>'coalesced_owner_resolution','')
                  ='COMPLETE_CURRENT_AUTHORITY'
            AND COALESCE(dirty_job.payload_json->>'actual_refresh_scope_status','')
                  ='MATERIALISED'
          )
        )
        AND dirty_job.created_at_utc>=request_row.created_at_utc
        AND dirty_job.scope_change_generation=COALESCE(change_counter.scope_change_generation,0)
        AND COALESCE(dirty_job.payload_json->>'source_change_seq','')
              =COALESCE(change_counter.seq,0)::text
        AND COALESCE((dirty_job.payload_json->>'policy_x_dirtying_only')::boolean,false)
        AND COALESCE((dirty_job.payload_json->>'economic_truth_mutation_allowed')::boolean,true) IS FALSE
        AND pg_catalog.jsonb_typeof(COALESCE(dirty_job.payload_json->'reasons','[]'::jsonb))='array'
        AND pg_catalog.jsonb_array_length(COALESCE(dirty_job.payload_json->'reasons','[]'::jsonb))>0
        AND NOT EXISTS (
          SELECT 1
          FROM pg_catalog.jsonb_array_elements_text(
            COALESCE(dirty_job.payload_json->'reasons','[]'::jsonb)
          ) AS dirty_reason(value)
          WHERE pg_catalog.upper(pg_catalog.btrim(dirty_reason.value)) NOT IN (
            'DIRTY_TRIGGER:PAY_PAYMENT_CORRECTION_REQUESTS:INSERT',
            'DIRTY_TRIGGER:PAY_PAYMENT_CORRECTION_REQUESTS:UPDATE',
            'DIRTY_TRIGGER:PAY_BATCHES:UPDATE'
          )
        )
      LIMIT 1
    ) AS request_owned_dirty ON true
  ), source_proof AS (
    SELECT
      authority.*,
      source_rows.source_row_count,
      source_rows.source_identity_digest,
      pg_catalog.md5(
        authority.draft_operation_id::text||'|'||authority.pay_batch_id::text||'|'||
        p_session_id::text||'|'||authority.candidate_id::text||'|'||
        COALESCE(authority.post_draft_authority->>'source_change_seq','')||'|'||
        COALESCE(authority.post_draft_authority->>'dirty_generation','')||'|'||
        authority.source_session_version::text||'|'||authority.source_snapshot_run_id::text||'|'||
        COALESCE(authority.frozen_source_build_run_id,'')||'|'||
        COALESCE(authority.frozen_source_publication_id,'')||'|'||
        COALESCE(authority.allocation_basis_json->>'source_identity_digest','')||'|'||
        COALESCE(authority.allocation_basis_json->>'semantic_proof_digest','')||
        '|'||COALESCE((authority.post_draft_authority->>'fast_reversion_eligible')::boolean,false)::text||
        '|POST_DRAFT_LIVE_AUTHORITY_V2'
      ) AS recomputed_post_draft_authority_digest,
      CASE
        WHEN v_mode<>'DRAFT_OVERLAY_PREFLIGHT' AND authority.work_item_id IS NULL THEN 'WORK_ITEM_NOT_EXACT'
        WHEN v_mode='PRE_FINANCIAL' AND pg_catalog.upper(COALESCE(authority.work_status,''))<>'PROCESSING'
          THEN 'FINANCIAL_REVERSION_NOT_CLAIMED'
        WHEN v_mode IN ('POST_FINANCIAL','OBSERVE_ONLY')
          AND pg_catalog.upper(COALESCE(authority.work_status,''))<>'APPLIED'
          THEN 'FINANCIAL_REVERSION_NOT_APPLIED'
        WHEN v_mode IN ('POST_FINANCIAL','OBSERVE_ONLY')
          AND COALESCE(authority.work_result->>'result_code','')<>'APPLIED'
          THEN 'FINANCIAL_REVERSION_RESULT_NOT_EXACT'
        WHEN authority.source_workbench_session_id IS DISTINCT FROM p_session_id THEN 'SOURCE_SESSION_NOT_EXACT'
        WHEN authority.draft_operation_id IS NULL OR authority.allocation_basis_json IS NULL THEN 'COMPLETED_DRAFT_SCOPE_MISSING'
        WHEN COALESCE(authority.frozen_attestation->>'attestation_version','') <> 'CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3'
          OR COALESCE(authority.frozen_attestation->>'semantic_contract_version','') <> 'READY_TO_PAY_SEMANTIC_V2'
          OR COALESCE((authority.frozen_attestation->>'semantic_ready')::boolean,false) IS NOT TRUE
          OR COALESCE((authority.frozen_attestation->>'parity_complete')::boolean,false) IS NOT TRUE
          THEN 'LEGACY_OR_SEMANTICALLY_UNCERTIFIED_SOURCE'
        WHEN authority.frozen_source_build_run_id
          !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          OR COALESCE(authority.frozen_attestation->>'economic_build_id','')
          !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN 'ORIGINAL_BUILD_LINEAGE_MISSING'
        WHEN COALESCE(authority.frozen_source_publication_id,'')
            !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN 'LEGACY_PHYSICAL_PUBLICATION_MISSING'
        WHEN COALESCE((authority.post_draft_authority->>'fast_reversion_eligible')::boolean,false) IS NOT TRUE
          THEN COALESCE(NULLIF(authority.post_draft_authority->>'fast_reversion_ineligible_reason',''),
                        'LEGACY_PHYSICAL_PUBLICATION_MISSING')
        WHEN COALESCE(authority.post_draft_authority->>'contract_version','')
               <>'POST_DRAFT_LIVE_AUTHORITY_V2'
          OR COALESCE(authority.post_draft_authority->>'draft_operation_id','')
               IS DISTINCT FROM authority.draft_operation_id::text
          OR COALESCE(authority.post_draft_authority->>'pay_batch_id','')
               IS DISTINCT FROM authority.pay_batch_id::text
          OR COALESCE(authority.post_draft_authority->>'workbench_session_id','')
               IS DISTINCT FROM p_session_id::text
          OR COALESCE(authority.post_draft_authority->>'candidate_id','')
               IS DISTINCT FROM authority.candidate_id::text
          OR COALESCE(authority.post_draft_authority->>'source_session_version','')
               IS DISTINCT FROM authority.source_session_version::text
          OR COALESCE(authority.post_draft_authority->>'source_snapshot_run_id','')
               IS DISTINCT FROM authority.source_snapshot_run_id::text
          OR COALESCE(authority.post_draft_authority->>'original_source_build_run_id','')
               IS DISTINCT FROM authority.frozen_source_build_run_id
          OR COALESCE(authority.post_draft_authority->>'original_source_publication_id','')
               IS DISTINCT FROM COALESCE(authority.frozen_source_publication_id,'')
          OR COALESCE(authority.post_draft_authority->>'original_source_identity_digest','')
               IS DISTINCT FROM authority.allocation_basis_json->>'source_identity_digest'
          OR COALESCE(authority.post_draft_authority->>'original_semantic_proof_digest','')
               IS DISTINCT FROM authority.allocation_basis_json->>'semantic_proof_digest'
          OR COALESCE(authority.post_draft_authority->>'authority_digest','')
               IS DISTINCT FROM pg_catalog.md5(
                 authority.draft_operation_id::text||'|'||authority.pay_batch_id::text||'|'||
                 p_session_id::text||'|'||authority.candidate_id::text||'|'||
                 COALESCE(authority.post_draft_authority->>'source_change_seq','')||'|'||
                 COALESCE(authority.post_draft_authority->>'dirty_generation','')||'|'||
                 authority.source_session_version::text||'|'||authority.source_snapshot_run_id::text||'|'||
                 COALESCE(authority.frozen_source_build_run_id,'')||'|'||
                 COALESCE(authority.frozen_source_publication_id,'')||'|'||
                 COALESCE(authority.allocation_basis_json->>'source_identity_digest','')||'|'||
                 COALESCE(authority.allocation_basis_json->>'semantic_proof_digest','')||
                 '|'||COALESCE((authority.post_draft_authority->>'fast_reversion_eligible')::boolean,false)::text||
                 '|POST_DRAFT_LIVE_AUTHORITY_V2'
               )
          THEN 'POST_DRAFT_AUTHORITY_MISSING_OR_MISMATCH'
        WHEN authority.execution_commit_state <> 'NOT_SUBMITTED' THEN 'PROVIDER_OR_EXECUTION_COMMIT_PRESENT'
        WHEN pg_catalog.upper(COALESCE(authority.settlement_status,''))='SETTLED'
          OR authority.settled_at_utc IS NOT NULL THEN 'SETTLEMENT_EVIDENCE_PRESENT'
        WHEN v_mode='DRAFT_OVERLAY_PREFLIGHT'
          AND (
            COALESCE(authority.draft_overlay_pre_request_authority->>'contract_version','')
              <>'DRAFT_OVERLAY_FAST_PRE_REQUEST_AUTHORITY_V1'
            OR COALESCE(authority.draft_overlay_pre_request_authority->>'candidate_id','')
              IS DISTINCT FROM authority.candidate_id::text
            OR COALESCE(
              (authority.draft_overlay_pre_request_authority->>'pre_request_exact')::boolean,
              false
            ) IS NOT TRUE
            OR COALESCE(authority.draft_overlay_pre_request_authority->>'source_change_seq','')
              !~ '^[0-9]{1,18}$'
            OR COALESCE(authority.draft_overlay_pre_request_authority->>'dirty_generation','')
              !~ '^[0-9]{1,18}$'
            OR NULLIF(COALESCE(authority.draft_overlay_pre_request_authority->>'fence_digest',''),'')
              IS NULL
            OR COALESCE(authority.draft_overlay_pre_request_authority->>'post_draft_authority_digest','')
              IS DISTINCT FROM authority.post_draft_authority->>'authority_digest'
          ) THEN 'PRE_REQUEST_ECONOMIC_AUTHORITY_NOT_CURRENT'
        WHEN v_mode='DRAFT_OVERLAY_PREFLIGHT'
          AND (
            COALESCE(authority.draft_overlay_start_authority->>'contract_version','')
              <>'DRAFT_OVERLAY_FAST_START_AUTHORITY_V1'
            OR COALESCE(authority.draft_overlay_start_authority->>'correction_request_id','')
              IS DISTINCT FROM p_correction_request_id::text
            OR COALESCE(authority.draft_overlay_start_authority->>'operation_id','')
              IS DISTINCT FROM p_operation_id::text
            OR COALESCE(authority.draft_overlay_start_authority->>'candidate_id','')
              IS DISTINCT FROM authority.candidate_id::text
            OR COALESCE((authority.draft_overlay_start_authority->>'start_exact')::boolean,false)
              IS NOT TRUE
            OR COALESCE(authority.draft_overlay_start_authority->>'source_change_seq','')
              !~ '^[0-9]{1,18}$'
            OR COALESCE(authority.draft_overlay_start_authority->>'dirty_generation','')
              !~ '^[0-9]{1,18}$'
            OR (
              COALESCE(
                (authority.draft_overlay_start_authority->>'request_owned_dirty_proven')::boolean,
                false
              )
              AND (
                COALESCE(authority.draft_overlay_start_authority->>'request_owned_dirty_job_id','')
                  !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                OR authority.current_request_owned_dirty_job_id IS DISTINCT FROM
                  (authority.draft_overlay_start_authority->>'request_owned_dirty_job_id')::uuid
              )
            )
            OR COALESCE(authority.draft_overlay_start_authority->>'pre_request_fence_digest','')
              IS DISTINCT FROM authority.draft_overlay_pre_request_authority->>'fence_digest'
            OR COALESCE(authority.draft_overlay_start_authority->>'fence_digest','')
              IS DISTINCT FROM pg_catalog.md5(
                p_correction_request_id::text||'|'||p_operation_id::text||'|'||
                authority.candidate_id::text||'|'||
                COALESCE(authority.draft_overlay_start_authority->>'source_change_seq','')||'|'||
                COALESCE(authority.draft_overlay_start_authority->>'dirty_generation','')||'|'||
                COALESCE(authority.draft_overlay_start_authority->>'pre_request_fence_digest','')||'|'||
                COALESCE(authority.draft_overlay_start_authority->>'request_owned_dirty_job_id','')||'|'||
                COALESCE(
                  (authority.draft_overlay_start_authority->>'request_owned_dirty_proven')::boolean,
                  false
                )::text||'|'||
                COALESCE((authority.draft_overlay_start_authority->>'start_exact')::boolean,false)::text||
                '|DRAFT_OVERLAY_FAST_START_AUTHORITY_V1'
              )
          ) THEN 'CANCELLATION_START_AUTHORITY_MISSING_OR_MISMATCH'
        WHEN v_mode='DRAFT_OVERLAY_PREFLIGHT'
          AND (
            authority.live_source_change_seq IS DISTINCT FROM
              (authority.draft_overlay_start_authority->>'source_change_seq')::bigint
            OR authority.live_dirty_generation IS DISTINCT FROM
              (authority.draft_overlay_start_authority->>'dirty_generation')::bigint
          )
          AND authority.current_request_owned_dirty_job_id IS NULL
          THEN 'CURRENT_ECONOMIC_AUTHORITY_CHANGED'
        WHEN v_mode='PRE_FINANCIAL'
          AND (
            COALESCE(authority.post_draft_authority->>'source_change_seq','') !~ '^[0-9]{1,18}$'
            OR COALESCE(authority.post_draft_authority->>'dirty_generation','') !~ '^[0-9]{1,18}$'
            OR authority.live_source_change_seq
                 IS DISTINCT FROM (authority.post_draft_authority->>'source_change_seq')::bigint
            OR authority.live_dirty_generation
                 IS DISTINCT FROM (authority.post_draft_authority->>'dirty_generation')::bigint
          ) THEN 'CURRENT_ECONOMIC_AUTHORITY_CHANGED'
        WHEN v_mode IN ('POST_FINANCIAL','OBSERVE_ONLY')
          AND (
            COALESCE(authority.work_result->'cancellation_reversion_preflight'->>'admitted','false')::boolean IS NOT TRUE
            OR COALESCE(authority.work_result->'cancellation_reversion_preflight'->>'post_draft_authority_digest','')
                 IS DISTINCT FROM authority.post_draft_authority->>'authority_digest'
            OR COALESCE(authority.work_result->'cancellation_reversion_post_financial_authority'->>'source_change_seq','')
                 !~ '^[0-9]{1,18}$'
            OR COALESCE(authority.work_result->'cancellation_reversion_post_financial_authority'->>'dirty_generation','')
                 !~ '^[0-9]{1,18}$'
            OR authority.live_source_change_seq IS DISTINCT FROM
                 (authority.work_result->'cancellation_reversion_post_financial_authority'->>'source_change_seq')::bigint
            OR authority.live_dirty_generation IS DISTINCT FROM
                 (authority.work_result->'cancellation_reversion_post_financial_authority'->>'dirty_generation')::bigint
          ) THEN 'POST_FINANCIAL_AUTHORITY_CHANGED'
        WHEN v_mode='DRAFT_OVERLAY_PREFLIGHT'
          AND (
            NOT (
              authority.registry_source_change_seq IS NOT DISTINCT FROM
                (authority.draft_overlay_pre_request_authority->>'source_change_seq')::bigint
              OR authority.registry_source_change_seq IS NOT DISTINCT FROM authority.live_source_change_seq
            )
            OR NOT (
              authority.registry_dirty_generation IS NOT DISTINCT FROM
                (authority.draft_overlay_pre_request_authority->>'dirty_generation')::bigint
              OR authority.registry_dirty_generation IS NOT DISTINCT FROM authority.live_dirty_generation
            )
            OR NOT (
              authority.candidate_state_source_change_seq IS NOT DISTINCT FROM
                (authority.draft_overlay_pre_request_authority->>'source_change_seq')::bigint
              OR authority.candidate_state_source_change_seq IS NOT DISTINCT FROM authority.live_source_change_seq
            )
            OR authority.candidate_state_session_version IS DISTINCT FROM authority.source_session_version
          )
          AND authority.current_request_owned_dirty_job_id IS NULL
          THEN 'WORKBENCH_AUTHORITY_NOT_FROZEN_DRAFT_BASELINE'
        WHEN v_mode<>'DRAFT_OVERLAY_PREFLIGHT'
          AND (authority.registry_source_change_seq IS DISTINCT FROM authority.live_source_change_seq
          OR authority.registry_dirty_generation IS DISTINCT FROM authority.live_dirty_generation
          OR authority.candidate_state_source_change_seq IS DISTINCT FROM authority.live_source_change_seq
          OR authority.candidate_state_session_version IS DISTINCT FROM authority.source_session_version)
          THEN 'WORKBENCH_AUTHORITY_NOT_CURRENT'
        WHEN source_rows.source_row_count
             IS DISTINCT FROM COALESCE(NULLIF(authority.frozen_attestation->>'source_row_count','')::integer,-1)
          OR source_rows.source_identity_digest
             IS DISTINCT FROM authority.frozen_attestation->>'source_identity_digest'
          THEN 'ORIGINAL_SOURCE_DIGEST_MISMATCH'
        ELSE NULL
      END AS rejection_reason
    FROM authority
    LEFT JOIN LATERAL (
      SELECT
        pg_catalog.count(*)::integer AS source_row_count,
        pg_catalog.md5(COALESCE(pg_catalog.string_agg(
          public.pay_workbench_preview_section_from_line_json(source_row.source_row_json)
            || E'\x1f' || source_row.line_key || E'\x1f' || source_row.source_ordinal::text,
          E'\x1e' ORDER BY source_row.source_ordinal,
            public.pay_workbench_preview_section_from_line_json(source_row.source_row_json),
            source_row.line_key
        ),'')) AS source_identity_digest
      FROM public.banking_pay_workbench_candidate_source_lines AS source_row
      WHERE source_row.session_id=p_session_id
        AND source_row.candidate_id=authority.candidate_id
        AND source_row.session_version=authority.source_session_version
        AND source_row.source_build_run_id=CASE
          WHEN authority.frozen_source_build_run_id
            ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN authority.frozen_source_build_run_id::uuid ELSE NULL::uuid END
        AND authority.frozen_source_publication_id
              ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        AND source_row.source_publication_id=authority.frozen_source_publication_id::uuid
    ) AS source_rows ON true
  ), result_rows AS (
    SELECT
      source_proof.ordinality,
      pg_catalog.jsonb_strip_nulls(pg_catalog.jsonb_build_object(
        'work_item_id',source_proof.work_item_id,
        'candidate_id',source_proof.candidate_id,
        'pay_batch_candidate_id',source_proof.pay_batch_candidate_id,
        'pay_batch_id',source_proof.pay_batch_id,
        'admitted',source_proof.rejection_reason IS NULL,
        'fast_reversion_eligible',source_proof.rejection_reason IS NULL,
        'rejection_reason',source_proof.rejection_reason,
        'draft_operation_id',source_proof.draft_operation_id,
        'original_economic_build_id',source_proof.frozen_attestation->>'economic_build_id',
        'original_source_build_run_id',source_proof.frozen_source_build_run_id,
        'original_source_publication_id',source_proof.frozen_source_publication_id,
        'original_source_change_seq',source_proof.frozen_attestation->>'source_change_seq',
        'current_source_change_seq',source_proof.live_source_change_seq,
        'current_dirty_generation',source_proof.live_dirty_generation,
        'session_version',source_proof.source_session_version,
        'source_count',source_proof.source_row_count,
        'source_identity_digest',source_proof.source_identity_digest,
        'semantic_proof_digest',source_proof.frozen_attestation->>'semantic_proof_digest',
        'post_draft_authority_digest',source_proof.post_draft_authority->>'authority_digest',
        'recomputed_post_draft_authority_digest',source_proof.recomputed_post_draft_authority_digest,
        'pre_request_source_change_seq',source_proof.draft_overlay_pre_request_authority->>'source_change_seq',
        'pre_request_dirty_generation',source_proof.draft_overlay_pre_request_authority->>'dirty_generation',
        'pre_request_fence_digest',source_proof.draft_overlay_pre_request_authority->>'fence_digest',
        'cancellation_start_source_change_seq',source_proof.draft_overlay_start_authority->>'source_change_seq',
        'cancellation_start_dirty_generation',source_proof.draft_overlay_start_authority->>'dirty_generation',
        'cancellation_start_fence_digest',source_proof.draft_overlay_start_authority->>'fence_digest',
        'request_owned_dirty_proven',source_proof.current_request_owned_dirty_job_id IS NOT NULL,
        'request_owned_dirty_job_id',source_proof.current_request_owned_dirty_job_id,
        'financial_reversion_digest',pg_catalog.md5(
          (COALESCE(source_proof.work_result,'{}'::jsonb)
            - 'applied_at_utc' - 'processed_at_utc')::text
        )
      )) AS result_json
    FROM source_proof
  )
  SELECT COALESCE(pg_catalog.jsonb_agg(result_rows.result_json ORDER BY result_rows.ordinality),'[]'::jsonb),
         pg_catalog.count(*)::integer
  INTO v_results,v_count
  FROM result_rows;

  RETURN pg_catalog.jsonb_build_object(
    'ok',true,
    'contract_version','CERTIFIED_CANCELLATION_REVERSION_ADMISSION_V1',
    'mode',v_mode,
    'correction_request_id',p_correction_request_id,
    'operation_id',p_operation_id,
    'session_id',p_session_id,
    'candidate_count',v_count,
    'admitted_count',(SELECT pg_catalog.count(*) FROM pg_catalog.jsonb_array_elements(v_results) AS r(value)
      WHERE COALESCE((r.value->>'admitted')::boolean,false)),
    'candidate_results',v_results,
    'policy_x_authority_scope','POST_DRAFT_FROZEN_PAYMENT_PROOF_PLUS_PRE_DRAFT_REVERSION'
  );
END;
$function$;

ALTER FUNCTION private.pay_workbench_cancel_reversion_admission_page_v1(uuid,uuid,uuid,uuid[],jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_cancel_reversion_admission_page_v1(uuid,uuid,uuid,uuid[],jsonb)
  FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION private.pay_workbench_cancel_reversion_admission_page_v1(uuid,uuid,uuid,uuid[],jsonb)
  TO postgres;


-- Bounded compatibility page for the installed financial cancellation owner.
-- Each item retains the exact single-item financial transaction semantics and
-- audit contract.  Per-item subtransactions prevent one rejected candidate
-- from rolling back other independent candidates in the page.
CREATE OR REPLACE FUNCTION private.pay_pre_bank_cancel_apply_work_page_v1(
  p_correction_request_id uuid,
  p_work_item_ids uuid[],
  p_actor_user_id uuid DEFAULT NULL::uuid,
  p_options_json jsonb DEFAULT '{}'::jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
PARALLEL UNSAFE
SECURITY DEFINER
SET search_path TO ''
AS $function$
DECLARE
  v_work record;
  v_result jsonb := '{}'::jsonb;
  v_results jsonb := '[]'::jsonb;
  v_error_message text;
  v_error_state text;
  v_count integer := 0;
  v_operation_id uuid := NULL::uuid;
  v_session_id uuid := NULL::uuid;
  v_preflight jsonb := pg_catalog.jsonb_build_object(
    'ok',true,'mode','PRE_FINANCIAL','candidate_results','[]'::jsonb
  );
  v_candidate_preflight jsonb := '{}'::jsonb;
  v_post_source_change_seq bigint := 0;
  v_post_dirty_generation bigint := 0;
  v_batch_id uuid := NULL::uuid;
  v_now timestamptz := pg_catalog.clock_timestamp();
  v_fast_count integer := 0;
  v_mutation_guard jsonb := '{}'::jsonb;
  v_set_page_enabled boolean := false;
BEGIN
  IF p_correction_request_id IS NULL
     OR p_work_item_ids IS NULL
     OR pg_catalog.cardinality(p_work_item_ids) < 1
     OR pg_catalog.cardinality(p_work_item_ids) > 100
     OR pg_catalog.jsonb_typeof(COALESCE(p_options_json,'{}'::jsonb)) <> 'object' THEN
    RAISE EXCEPTION 'PRE_BANK_CANCEL_APPLY_PAGE_ARGUMENT_INVALID'
      USING ERRCODE='P0001',DETAIL=pg_catalog.jsonb_build_object(
        'code','PRE_BANK_CANCEL_APPLY_PAGE_ARGUMENT_INVALID'
      )::text;
  END IF;

  SELECT COALESCE(setting.banking_pay_pre_bank_cancel_set_page_v1_enabled,false)
  INTO v_set_page_enabled
  FROM public.settings_defaults AS setting
  ORDER BY setting.id
  LIMIT 1;

  SELECT operation_row.id,batch_row.source_workbench_session_id,request_row.pay_batch_id
  INTO v_operation_id,v_session_id,v_batch_id
  FROM public.pay_payment_correction_requests AS request_row
  JOIN public.pay_batches AS batch_row ON batch_row.id=request_row.pay_batch_id
  JOIN public.banking_pay_operations AS operation_row
    ON operation_row.operation_type='PAYMENT_CORRECTION'
   AND operation_row.pay_batch_id=request_row.pay_batch_id
   AND operation_row.input_json->>'correction_request_id'=request_row.id::text
  WHERE request_row.id=p_correction_request_id
  ORDER BY operation_row.created_at_utc
  LIMIT 1;

  IF v_set_page_enabled
     AND v_operation_id IS NOT NULL AND v_session_id IS NOT NULL THEN
    v_preflight := private.pay_workbench_cancel_reversion_admission_page_v1(
      p_correction_request_id,v_operation_id,v_session_id,p_work_item_ids,
      pg_catalog.jsonb_build_object('mode','PRE_FINANCIAL')
    );
  END IF;

  -- Elect the closed-world no-provider page.  Admission is based on the exact
  -- V3 post-Draft authority; this extra fence deliberately narrows the bulk
  -- path to candidate-complete local/no-bank scopes.  Any ambiguity remains on
  -- the established single-item owner below.
  DROP TABLE IF EXISTS pg_temp._bpay_pre_cancel_fast_work;
  CREATE TEMP TABLE pg_temp._bpay_pre_cancel_fast_work ON COMMIT DROP AS
  WITH requested AS (
    SELECT DISTINCT requested_id.work_id
    FROM pg_catalog.unnest(p_work_item_ids) AS requested_id(work_id)
    WHERE requested_id.work_id IS NOT NULL
      AND v_set_page_enabled
  ), preflight AS (
    SELECT
      (entry.value->>'work_item_id')::uuid AS work_item_id,
      (entry.value->>'candidate_id')::uuid AS candidate_id,
      entry.value AS proof
    FROM pg_catalog.jsonb_array_elements(
      COALESCE(v_preflight->'candidate_results','[]'::jsonb)
    ) AS entry(value)
    WHERE COALESCE((entry.value->>'admitted')::boolean,false)
      AND COALESCE(entry.value->>'work_item_id','')
        ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      AND COALESCE(entry.value->>'candidate_id','')
        ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  ), exact_work AS (
    SELECT
      work_row.id AS work_item_id,
      work_row.candidate_id,
      work_row.pay_batch_candidate_id,
      work_row.pay_batch_id,
      work_row.selection_json,
      work_row.selection_hash,
      work_row.attempt_count,
      membership.pay_batch_item_ids,
      membership.active_item_count,
      membership.candidate_scope_hash,
      preflight.proof
    FROM requested
    JOIN public.pay_payment_correction_work_items AS work_row
      ON work_row.id=requested.work_id
     AND work_row.correction_request_id=p_correction_request_id
     AND work_row.pay_batch_id=v_batch_id
     AND work_row.work_kind='PRE_BANK_CANCEL'
     AND work_row.status='PROCESSING'
    JOIN public.pay_payment_correction_request_candidates AS membership
      ON membership.correction_request_id=p_correction_request_id
     AND membership.pay_batch_candidate_id=work_row.pay_batch_candidate_id
    JOIN preflight ON preflight.work_item_id=work_row.id
      AND preflight.candidate_id=work_row.candidate_id
    WHERE membership.active_item_count BETWEEN 1 AND 128
      AND pg_catalog.cardinality(membership.pay_batch_item_ids)=membership.active_item_count
      AND work_row.selection_hash IS NOT DISTINCT FROM membership.candidate_scope_hash
      AND pg_catalog.upper(pg_catalog.btrim(COALESCE(work_row.selection_json->>'scope_type','')))
            IN ('CANDIDATES','BATCH')
  ), item_proof AS (
    SELECT
      exact_work.work_item_id,
      pg_catalog.count(item_row.id)::integer AS resolved_item_count,
      pg_catalog.count(*) FILTER (WHERE COALESCE(item_row.is_voided,false))::integer AS voided_count,
      pg_catalog.count(*) FILTER (
        WHERE item_row.pay_batch_candidate_id IS DISTINCT FROM exact_work.pay_batch_candidate_id
      )::integer AS wrong_candidate_count,
      pg_catalog.count(*) FILTER (
        WHERE item_row.pay_bank_transfer_id IS NOT NULL
      )::integer AS provider_shape_count,
      pg_catalog.count(*) FILTER (
        WHERE pg_catalog.upper(COALESCE(item_row.item_type,'')) IN (
          'MANUAL_ADJUSTMENT','MANUAL_CREDIT_PAYOUT','MANUAL_DEBT_RECOVERY'
        )
      )::integer AS manual_shape_count
    FROM exact_work
    CROSS JOIN LATERAL pg_catalog.unnest(exact_work.pay_batch_item_ids) AS member(item_id)
    LEFT JOIN public.pay_batch_items AS item_row ON item_row.id=member.item_id
    GROUP BY exact_work.work_item_id
  )
  SELECT exact_work.*
  FROM exact_work
  JOIN item_proof ON item_proof.work_item_id=exact_work.work_item_id
  JOIN public.pay_batch_candidates AS batch_candidate
    ON batch_candidate.id=exact_work.pay_batch_candidate_id
   AND batch_candidate.pay_batch_id=v_batch_id
   AND batch_candidate.candidate_id=exact_work.candidate_id
  WHERE item_proof.resolved_item_count=exact_work.active_item_count
    AND item_proof.voided_count=0
    AND item_proof.wrong_candidate_count=0
    AND item_proof.provider_shape_count=0
    AND item_proof.manual_shape_count=0
    AND batch_candidate.settled_at_utc IS NULL
    AND pg_catalog.upper(COALESCE(batch_candidate.settlement_status,''))<>'SETTLED'
    AND NOT EXISTS (
      SELECT 1
      FROM public.pay_batch_items AS outside_item
      WHERE outside_item.pay_batch_candidate_id=exact_work.pay_batch_candidate_id
        AND COALESCE(outside_item.is_voided,false)=false
        AND NOT (outside_item.id=ANY(exact_work.pay_batch_item_ids))
    )
    AND NOT EXISTS (
      SELECT 1
      FROM public.pay_manual_adjustment_carry_forwards AS carry_forward
      WHERE carry_forward.source_pay_batch_item_id=ANY(exact_work.pay_batch_item_ids)
         OR carry_forward.target_pay_batch_item_id=ANY(exact_work.pay_batch_item_ids)
    )
    AND NOT EXISTS (
      SELECT 1
      FROM public.pay_payment_correction_items AS prior_correction
      WHERE prior_correction.pay_batch_item_id=ANY(exact_work.pay_batch_item_ids)
        AND prior_correction.status='APPLIED'
        AND prior_correction.correction_request_id IS DISTINCT FROM p_correction_request_id
    )
    AND NOT EXISTS (
      SELECT 1
      FROM public.pay_advances AS payout_case
      JOIN public.pay_batch_items AS finance_item
        ON finance_item.finance_case_id=payout_case.id
      WHERE finance_item.id=ANY(exact_work.pay_batch_item_ids)
        AND (
          payout_case.payout_transfer_id IS NOT NULL
          OR pg_catalog.upper(COALESCE(payout_case.payout_status::text,''))='PAID'
        )
    )
    AND NOT EXISTS (
      SELECT 1
      FROM public.pay_bank_transfers AS transfer
      WHERE transfer.pay_batch_id=v_batch_id
        AND transfer.candidate_id=exact_work.candidate_id
    )
    AND NOT EXISTS (
      SELECT 1
      FROM public.pay_bank_transfer_events AS transfer_event
      WHERE transfer_event.pay_batch_id=v_batch_id
        AND transfer_event.candidate_id=exact_work.candidate_id
    )
    AND NOT EXISTS (
      SELECT 1
      FROM public.banking_pay_operation_transfer_scope AS transfer_scope
      WHERE transfer_scope.pay_batch_id=v_batch_id
        AND transfer_scope.candidate_id=exact_work.candidate_id
    )
    AND NOT EXISTS (
      SELECT 1
      FROM public.mail_outbox AS queued_mail
      WHERE pg_catalog.upper(COALESCE(queued_mail.status::text,''))='QUEUED'
        AND (
          queued_mail.context_id=v_batch_id
          OR queued_mail.recipient_id=exact_work.candidate_id
          OR queued_mail.reference ILIKE '%'||v_batch_id::text||'%'
        )
        AND pg_catalog.lower(pg_catalog.concat_ws('|',queued_mail.type,queued_mail.email_type,
          queued_mail.context_kind,queued_mail.reference,
          COALESCE(queued_mail.payment_scope_json::text,'{}')))
          LIKE ANY(ARRAY['%remittance%','%payout%','%pay_batch%','%finance_payout%'])
    );

  SELECT pg_catalog.count(*)::integer INTO v_fast_count
  FROM pg_temp._bpay_pre_cancel_fast_work;

  IF v_fast_count>0 THEN
    v_mutation_guard:=private.pay_payment_mutation_guard_v1(
      v_batch_id,p_correction_request_id,'CORRECTION_APPLY'
    );
    IF COALESCE((v_mutation_guard->>'ok')::boolean,false) IS NOT TRUE THEN
      RAISE EXCEPTION '%',COALESCE(v_mutation_guard->>'code','PAYMENT_CORRECTION_GATE_OWNER_MISMATCH')
        USING ERRCODE='P0001',DETAIL=v_mutation_guard::text;
    END IF;

    -- Candidate serial authority is the only deliberate page loop.  Economic
    -- DML below is set-wise and candidates are locked in deterministic order.
    FOR v_work IN
      SELECT candidate_id
      FROM pg_temp._bpay_pre_cancel_fast_work
      ORDER BY candidate_id
    LOOP
      PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtext('PAY_WORKBENCH_CANDIDATE_SERIAL_V1'),
        pg_catalog.hashtext(v_work.candidate_id::text)
      );
    END LOOP;

    PERFORM 1 FROM public.pay_payment_correction_requests AS request_lock
      WHERE request_lock.id=p_correction_request_id FOR UPDATE;
    PERFORM 1 FROM public.pay_batches AS batch_lock
      WHERE batch_lock.id=v_batch_id
        AND pg_catalog.upper(pg_catalog.btrim(COALESCE(batch_lock.execution_commit_state,'NOT_SUBMITTED')))
              ='NOT_SUBMITTED'
        AND batch_lock.execution_commit_ref IS NULL
        AND batch_lock.execution_committed_at_utc IS NULL
      FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'PRE_BANK_CANCEL_FAST_PAGE_EXECUTION_AUTHORITY_CHANGED'
        USING ERRCODE='P0001';
    END IF;

    DROP TABLE IF EXISTS pg_temp._bpay_pre_cancel_fast_items;
    CREATE TEMP TABLE pg_temp._bpay_pre_cancel_fast_items ON COMMIT DROP AS
    SELECT
      fast_work.work_item_id,fast_work.candidate_id,fast_work.proof,item_row.*
    FROM pg_temp._bpay_pre_cancel_fast_work AS fast_work
    CROSS JOIN LATERAL pg_catalog.unnest(fast_work.pay_batch_item_ids) AS member(item_id)
    JOIN public.pay_batch_items AS item_row ON item_row.id=member.item_id
    ORDER BY fast_work.candidate_id,item_row.id
    FOR UPDATE OF item_row;

    -- A local scheduled execution is only a durable wait.  The admission and
    -- batch locks above prove no provider boundary was crossed before retiring
    -- that wait for the whole page.
    UPDATE public.banking_pay_operations AS scheduled_local_operation
    SET status='CANCELLED',phase='COMPLETE',runner_state='CANCELLED',
        requires_user_action=false,
        result_json=COALESCE(scheduled_local_operation.result_json,'{}'::jsonb)
          || pg_catalog.jsonb_build_object(
            'code','SCHEDULED_LOCAL_EXECUTION_CANCELLED_BEFORE_APPLY',
            'correction_request_id',p_correction_request_id,
            'work_item_ids',(SELECT pg_catalog.jsonb_agg(work_item_id ORDER BY work_item_id)
              FROM pg_temp._bpay_pre_cancel_fast_work),
            'provider_submission_attempted',false,'submitted_to_bank',false),
        completed_at_utc=COALESCE(scheduled_local_operation.completed_at_utc,v_now),
        lease_owner=NULL,lease_expires_at_utc=NULL,locked_by=NULL,
        lock_expires_at_utc=NULL,run_after_utc=NULL,updated_at_utc=v_now,
        resume_reason='SCHEDULED_LOCAL_EXECUTION_CANCELLED_BEFORE_APPLY'
    WHERE scheduled_local_operation.pay_batch_id=v_batch_id
      AND scheduled_local_operation.operation_type='PAYMENT_EXECUTE'
      AND scheduled_local_operation.status IN ('QUEUED','RUNNING','PROCESSING','CLAIMED','IN_PROGRESS')
      AND scheduled_local_operation.phase='SCHEDULE_PAYMENT'
      AND scheduled_local_operation.resume_reason IN (
        'WAIT_FOR_SCHEDULED_NO_BANK_PAYMENT','WAIT_FOR_SCHEDULED_LOCAL_MANUAL_SETTLEMENT'
      );

    INSERT INTO public.pay_payment_correction_items(
      correction_request_id,pay_batch_id,pay_batch_candidate_id,candidate_id,
      pay_batch_item_id,pay_bank_transfer_id,timesheet_id,finance_case_id,
      finance_component_id,reservation_id,item_type,correction_item_kind,
      source_amount,amount_ex_vat,amount_vat,amount_inc_vat,
      economic_key_type,economic_key_value,before_snapshot_json,
      after_snapshot_json,status,created_at_utc,applied_at_utc
    )
    SELECT
      p_correction_request_id,v_batch_id,fast_item.pay_batch_candidate_id,
      fast_item.candidate_id,fast_item.id,fast_item.pay_bank_transfer_id,
      fast_item.timesheet_id,fast_item.finance_case_id,fast_item.finance_component_id,
      fast_item.reservation_id,fast_item.item_type,'PRE_BANK_CANCEL',
      economic_component.source_amount_ex_vat,fast_item.amount_ex_vat,
      fast_item.amount_vat,COALESCE(economic_component.source_amount_inc_vat,fast_item.amount_inc_vat),
      economic_component.key_type,economic_component.key_value,to_jsonb(fast_item),
      to_jsonb(fast_item)||pg_catalog.jsonb_build_object(
        'is_voided',true,'updated_at',v_now,'set_based_page',true,
        'work_item_id',fast_item.work_item_id),
      'APPLIED',v_now,v_now
    FROM pg_temp._bpay_pre_cancel_fast_items AS fast_item
    LEFT JOIN LATERAL (
      SELECT component.key_type,component.key_value,
             component.source_amount_ex_vat,component.source_amount_inc_vat
      FROM public._pay_batch_item_economic_components(
        NULL::uuid,ARRAY[fast_item.id]::uuid[]
      ) AS component
      LIMIT 1
    ) AS economic_component ON true
    ON CONFLICT (pay_batch_item_id,correction_item_kind)
      WHERE status='APPLIED' AND pay_batch_item_id IS NOT NULL DO NOTHING;

    UPDATE public.pay_batch_items AS item_to_void
    SET is_voided=true,updated_at=v_now
    FROM pg_temp._bpay_pre_cancel_fast_items AS fast_item
    WHERE item_to_void.id=fast_item.id AND COALESCE(item_to_void.is_voided,false)=false;

    DROP TABLE IF EXISTS pg_temp._bpay_pre_cancel_fast_reservations;
    CREATE TEMP TABLE pg_temp._bpay_pre_cancel_fast_reservations ON COMMIT DROP AS
    WITH candidate AS (
      SELECT DISTINCT reservation.id,reservation.finance_case_id,
        reservation.finance_component_id,reservation.pay_batch_item_id,
        reservation.reserved_amount,reservation.reserved_source_amount,
        fast_item.work_item_id,fast_item.candidate_id
      FROM public.pay_advance_reservations AS reservation
      JOIN pg_temp._bpay_pre_cancel_fast_items AS fast_item
        ON fast_item.reservation_id=reservation.id
        OR fast_item.id=reservation.pay_batch_item_id
      WHERE reservation.pay_batch_id=v_batch_id
        AND (
          pg_catalog.upper(pg_catalog.btrim(COALESCE(reservation.status,'')))='RESERVED'
          OR (pg_catalog.upper(pg_catalog.btrim(COALESCE(reservation.status,'')))='COMMITTED'
              AND reservation.settled_at_utc IS NULL)
        )
    ), released AS (
      UPDATE public.pay_advance_reservations AS reservation
      SET status='RELEASED',released_at_utc=COALESCE(reservation.released_at_utc,v_now),
          released_reason='PRE_BANK_CANCEL',updated_by_user_id=p_actor_user_id
      FROM candidate
      WHERE reservation.id=candidate.id
      RETURNING reservation.id
    )
    SELECT candidate.* FROM candidate JOIN released ON released.id=candidate.id;

    DROP TABLE IF EXISTS pg_temp._bpay_pre_cancel_fast_components;
    CREATE TEMP TABLE pg_temp._bpay_pre_cancel_fast_components ON COMMIT DROP AS
    SELECT component.id AS finance_component_id,component.finance_case_id,
      component.remaining_source_amount AS remaining_before,
      COALESCE(component.remaining_source_amount,0) AS remaining_after,
      pg_catalog.round(pg_catalog.sum(pg_catalog.abs(COALESCE(
        reservation.reserved_source_amount,
        public._pay_batch_item_source_reservation_amount_ex_vat(item_row.id),
        item_row.frozen_source_amount,reservation.reserved_amount,
        item_row.amount_ex_vat,item_row.amount_inc_vat,0
      ))),2)::numeric AS restore_source_amount,
      (pg_catalog.array_agg(reservation.work_item_id ORDER BY reservation.work_item_id))[1] AS work_item_id
    FROM pg_temp._bpay_pre_cancel_fast_reservations AS reservation
    LEFT JOIN public.pay_batch_items AS item_row ON item_row.id=reservation.pay_batch_item_id
    JOIN public.pay_finance_case_components AS component
      ON component.id=COALESCE(reservation.finance_component_id,item_row.finance_component_id)
    GROUP BY component.id,component.finance_case_id,component.remaining_source_amount;

    UPDATE public.pay_finance_case_components AS component
    SET remaining_source_amount=fast_component.remaining_after,
        resolved_at_utc=CASE WHEN fast_component.remaining_after>0 THEN NULL ELSE component.resolved_at_utc END,
        closed_at_utc=NULL,updated_at_utc=v_now
    FROM pg_temp._bpay_pre_cancel_fast_components AS fast_component
    WHERE component.id=fast_component.finance_component_id;

    INSERT INTO public.pay_finance_case_events(
      finance_case_id,finance_component_id,event_type,event_at_utc,actor_user_id,
      pay_batch_id,reservation_id,before_json,after_json,reason,note
    )
    SELECT finance_case_id,finance_component_id,'COMPONENT_RESTORED',v_now,
      p_actor_user_id,v_batch_id,NULL::uuid,
      pg_catalog.jsonb_build_object('remaining_source_amount',remaining_before),
      pg_catalog.jsonb_build_object('remaining_source_amount',remaining_after,
        'restored_source_amount',restore_source_amount,'correction_kind','PRE_BANK_CANCEL',
        'work_item_id',work_item_id,'set_based_page',true),
      'PRE_BANK_CANCEL',
      'Set-based pre-bank cancellation released the reservation without changing live outstanding balance.'
    FROM pg_temp._bpay_pre_cancel_fast_components;

    INSERT INTO public.pay_finance_case_events(
      finance_case_id,finance_component_id,event_type,event_at_utc,actor_user_id,
      pay_batch_id,reservation_id,before_json,after_json,reason,note
    )
    SELECT finance_case_id,finance_component_id,'RESERVATION_RELEASED',v_now,
      p_actor_user_id,v_batch_id,id,
      pg_catalog.jsonb_build_object('reservation_status','RESERVED_OR_COMMITTED'),
      pg_catalog.jsonb_build_object('reservation_status','RELEASED',
        'released_reason','PRE_BANK_CANCEL','work_item_id',work_item_id,'set_based_page',true),
      'PRE_BANK_CANCEL','Set-based pre-bank cancellation released reservation.'
    FROM pg_temp._bpay_pre_cancel_fast_reservations
    WHERE finance_case_id IS NOT NULL;

    UPDATE public.pay_advances AS payout_case
    SET payout_status='PENDING',payout_pay_batch_id=NULL,payout_transfer_id=NULL,updated_at=v_now
    WHERE payout_case.id IN (
      SELECT DISTINCT fast_item.finance_case_id
      FROM pg_temp._bpay_pre_cancel_fast_items AS fast_item
      WHERE fast_item.finance_case_id IS NOT NULL
    )
      AND COALESCE(payout_case.payout_status::text,'')<>'PAID'
      AND payout_case.payout_pay_batch_id=v_batch_id;

    -- Recalculate every affected candidate once and the batch once.
    WITH affected AS (
      SELECT DISTINCT pay_batch_candidate_id
      FROM pg_temp._bpay_pre_cancel_fast_work
    ), sums AS (
      SELECT candidate.id,
        pg_catalog.count(item.id) FILTER (WHERE COALESCE(item.is_voided,false)=false)::integer AS active_count,
        pg_catalog.bool_or(COALESCE(item.is_voided,false)=false AND pg_catalog.upper(COALESCE(item.pay_channel,''))='PAYE') AS has_paye,
        pg_catalog.bool_or(COALESCE(item.is_voided,false)=false AND pg_catalog.upper(COALESCE(item.pay_channel,''))='UMBRELLA') AS has_umbrella,
        pg_catalog.round(COALESCE(pg_catalog.sum(CASE WHEN COALESCE(item.is_voided,false)=false
          AND item.item_type NOT IN ('OVERPAYMENT_RECOVERY','UNDERPAYMENT_PAYMENT','LOAN_REPAYMENT','MANUAL_DEBT_RECOVERY','MANUAL_CREDIT_PAYOUT','LOAN_PAYOUT','DEBT_CREATED')
          THEN COALESCE(item.amount_ex_vat,0) ELSE 0 END),0),2)::numeric(12,2) AS earnings_ex,
        pg_catalog.round(COALESCE(pg_catalog.sum(CASE WHEN COALESCE(item.is_voided,false)=false AND item.item_type<>'DEBT_CREATED'
          THEN COALESCE(item.amount_ex_vat,0) ELSE 0 END),0),2)::numeric(12,2) AS earnings_inc,
        pg_catalog.round(COALESCE(pg_catalog.sum(CASE WHEN COALESCE(item.is_voided,false)=false
          AND COALESCE(item.paye_treatment,'NONE') IN ('GROSS_ADD','GROSS_DEDUCT') THEN COALESCE(item.amount_ex_vat,0) ELSE 0 END),0),2)::numeric(12,2) AS gross_adj,
        pg_catalog.round(COALESCE(pg_catalog.sum(CASE WHEN COALESCE(item.is_voided,false)=false
          AND COALESCE(item.paye_treatment,'NONE') IN ('NET_ADD','NET_DEDUCT') THEN COALESCE(item.amount_ex_vat,0) ELSE 0 END),0),2)::numeric(12,2) AS net_adj,
        pg_catalog.round(COALESCE(pg_catalog.sum(CASE WHEN COALESCE(item.is_voided,false)=false AND item.item_type='OVERPAYMENT_RECOVERY'
          THEN -COALESCE(item.amount_ex_vat,0) ELSE 0 END),0),2)::numeric(12,2) AS recovery,
        pg_catalog.round(COALESCE(pg_catalog.sum(CASE WHEN COALESCE(item.is_voided,false)=false AND item.item_type='LOAN_REPAYMENT'
          THEN -COALESCE(item.amount_ex_vat,0) ELSE 0 END),0),2)::numeric(12,2) AS loan,
        pg_catalog.round(COALESCE(pg_catalog.sum(CASE WHEN COALESCE(item.is_voided,false)=false AND item.item_type='DEBT_CREATED'
          THEN COALESCE(item.amount_inc_vat,item.amount_ex_vat,0) ELSE 0 END),0),2)::numeric(12,2) AS debt
      FROM affected JOIN public.pay_batch_candidates AS candidate ON candidate.id=affected.pay_batch_candidate_id
      LEFT JOIN public.pay_batch_items AS item ON item.pay_batch_candidate_id=candidate.id
      GROUP BY candidate.id
    )
    UPDATE public.pay_batch_candidates AS candidate
    SET awaiting_net_amount=CASE WHEN sums.active_count=0 THEN false WHEN COALESCE(sums.has_paye,false)
          THEN NOT EXISTS (SELECT 1 FROM public.pay_batch_paye_net_inputs AS net_input WHERE net_input.pay_batch_candidate_id=candidate.id)
          ELSE false END,
        gross_preview=CASE WHEN sums.active_count=0 THEN 0 WHEN COALESCE(sums.has_paye,false)
          THEN pg_catalog.round(COALESCE(sums.earnings_ex,0)+COALESCE(sums.gross_adj,0),2)
          ELSE GREATEST(COALESCE(sums.earnings_inc,0),0) END,
        net_bank_amount=CASE WHEN sums.active_count=0 THEN 0 WHEN COALESCE(sums.has_paye,false)
          THEN CASE WHEN NOT EXISTS (SELECT 1 FROM public.pay_batch_paye_net_inputs AS net_input WHERE net_input.pay_batch_candidate_id=candidate.id)
            THEN NULL::numeric ELSE GREATEST(pg_catalog.round(COALESCE((SELECT net_amount FROM public.pay_batch_paye_net_inputs WHERE pay_batch_candidate_id=candidate.id),0)+COALESCE(sums.net_adj,0),2),0) END
          ELSE GREATEST(COALESCE(sums.earnings_inc,0),0) END,
        debt_created=COALESCE(sums.debt,0),overpayment_recovery_taken=COALESCE(sums.recovery,0),
        loan_repayment_taken=COALESCE(sums.loan,0),mismatch_settlement_choice=NULL,updated_at=v_now
    FROM sums WHERE candidate.id=sums.id AND candidate.pay_batch_id=v_batch_id;

    UPDATE public.pay_batches AS batch
    SET total_bank_out=COALESCE((SELECT pg_catalog.round(COALESCE(pg_catalog.sum(COALESCE(candidate.net_bank_amount,0)),0),2)
          FROM public.pay_batch_candidates AS candidate WHERE candidate.pay_batch_id=v_batch_id),0),
        total_debt_created=COALESCE((SELECT pg_catalog.round(COALESCE(pg_catalog.sum(COALESCE(candidate.debt_created,0)),0),2)
          FROM public.pay_batch_candidates AS candidate WHERE candidate.pay_batch_id=v_batch_id),0)
    WHERE batch.id=v_batch_id;

    INSERT INTO public.app_change_counters(entity_key,seq,updated_at)
    SELECT 'pay_candidate:'||fast_work.candidate_id::text,1,v_now
    FROM pg_temp._bpay_pre_cancel_fast_work AS fast_work
    ON CONFLICT (entity_key) DO UPDATE
      SET seq=public.app_change_counters.seq+1,updated_at=v_now;

    DROP TABLE IF EXISTS pg_temp._bpay_pre_cancel_fast_results;
    CREATE TEMP TABLE pg_temp._bpay_pre_cancel_fast_results ON COMMIT DROP AS
    WITH per_work AS (
      SELECT fast_work.work_item_id,fast_work.candidate_id,fast_work.proof,
        pg_catalog.count(DISTINCT fast_item.id)::integer AS selected_item_count,
        pg_catalog.count(DISTINCT reservation.id)::integer AS released_reservation_count,
        pg_catalog.count(DISTINCT component.finance_component_id)::integer AS restored_component_count,
        COALESCE(change_counter.seq,0) AS source_change_seq,
        COALESCE(change_counter.scope_change_generation,0) AS dirty_generation,
        pg_catalog.jsonb_build_object(
          'pay_batch_id',v_batch_id::text,'correction_request_id',p_correction_request_id::text,
          'work_item_id',fast_work.work_item_id::text,'change_kind','PRE_BANK_CANCEL',
          'changed_pay_batch_item_ids',pg_catalog.jsonb_agg(DISTINCT fast_item.id::text ORDER BY fast_item.id::text),
          'changed_pay_batch_candidate_ids',pg_catalog.jsonb_build_array(fast_work.pay_batch_candidate_id::text),
          'changed_candidate_ids',pg_catalog.jsonb_build_array(fast_work.candidate_id::text),
          'changed_finance_case_ids',COALESCE(pg_catalog.jsonb_agg(DISTINCT fast_item.finance_case_id::text ORDER BY fast_item.finance_case_id::text)
            FILTER (WHERE fast_item.finance_case_id IS NOT NULL),'[]'::jsonb),
          'changed_finance_component_ids',COALESCE(pg_catalog.jsonb_agg(DISTINCT fast_item.finance_component_id::text ORDER BY fast_item.finance_component_id::text)
            FILTER (WHERE fast_item.finance_component_id IS NOT NULL),'[]'::jsonb),
          'changed_reservation_ids',COALESCE(pg_catalog.jsonb_agg(DISTINCT fast_item.reservation_id::text ORDER BY fast_item.reservation_id::text)
            FILTER (WHERE fast_item.reservation_id IS NOT NULL),'[]'::jsonb),
          'set_based_page',true
        ) AS changed_scope_json
      FROM pg_temp._bpay_pre_cancel_fast_work AS fast_work
      JOIN pg_temp._bpay_pre_cancel_fast_items AS fast_item ON fast_item.work_item_id=fast_work.work_item_id
      LEFT JOIN pg_temp._bpay_pre_cancel_fast_reservations AS reservation ON reservation.work_item_id=fast_work.work_item_id
      LEFT JOIN pg_temp._bpay_pre_cancel_fast_components AS component ON component.work_item_id=fast_work.work_item_id
      LEFT JOIN public.app_change_counters AS change_counter
        ON change_counter.entity_key='pay_candidate:'||fast_work.candidate_id::text
      GROUP BY fast_work.work_item_id,fast_work.candidate_id,fast_work.pay_batch_candidate_id,
        fast_work.proof,change_counter.seq,change_counter.scope_change_generation
    ), result AS (
      SELECT per_work.*,
        pg_catalog.jsonb_build_object(
          'ok',true,'status','APPLIED','result_code','APPLIED','work_item_id',per_work.work_item_id,
          'candidate_id',per_work.candidate_id,'correction_request_id',p_correction_request_id,
          'pay_batch_id',v_batch_id,'correction_item_kind','PRE_BANK_CANCEL',
          'selected_item_count',per_work.selected_item_count,'expected_item_count',per_work.selected_item_count,
          'applied_item_count',per_work.selected_item_count,'voided_item_count',per_work.selected_item_count,
          'released_reservation_count',per_work.released_reservation_count,
          'released_reservations',per_work.released_reservation_count,
          'restored_component_count',per_work.restored_component_count,
          'restored_finance_components',per_work.restored_component_count,
          'changed_scope_json',per_work.changed_scope_json,'set_based_page',true,
          'refresh_result',pg_catalog.jsonb_build_object('status','PENDING_OPERATION_REFRESH','owner','pay_payment_correction_process_chunk'),
          'applied_at_utc',v_now
        ) AS base_result
      FROM per_work
    )
    SELECT result.*,
      result.base_result||pg_catalog.jsonb_build_object(
        'cancellation_reversion_preflight',result.proof,
        'cancellation_reversion_post_financial_authority',pg_catalog.jsonb_build_object(
          'contract_version','POST_FINANCIAL_CANCELLATION_AUTHORITY_V1',
          'source_change_seq',result.source_change_seq,'dirty_generation',result.dirty_generation,
          'authority_digest',pg_catalog.md5(
            COALESCE(result.proof->>'post_draft_authority_digest','')||'|'||result.work_item_id::text||'|'||
            result.source_change_seq::text||'|'||result.dirty_generation::text||'|'||
            pg_catalog.md5((result.base_result-'applied_at_utc'-'processed_at_utc')::text)||
            '|POST_FINANCIAL_CANCELLATION_AUTHORITY_V1'
          )
        )
      ) AS final_result
    FROM result;

    UPDATE public.pay_payment_correction_work_items AS work_row
    SET status='APPLIED',locked_at_utc=NULL,locked_by=NULL,
        processed_at_utc=COALESCE(work_row.processed_at_utc,v_now),last_error=NULL,
        result_json=COALESCE(work_row.result_json,'{}'::jsonb)||fast_result.final_result
    FROM pg_temp._bpay_pre_cancel_fast_results AS fast_result
    WHERE work_row.id=fast_result.work_item_id;

    SELECT COALESCE(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
      'work_item_id',fast_result.work_item_id::text,
      'candidate_id',fast_result.candidate_id::text,'result',fast_result.final_result
    ) ORDER BY fast_result.candidate_id,fast_result.work_item_id),'[]'::jsonb)
    INTO v_results
    FROM pg_temp._bpay_pre_cancel_fast_results AS fast_result;

    v_count:=v_fast_count;
  END IF;

  FOR v_work IN
    SELECT work_row.id,work_row.candidate_id,work_row.attempt_count
    FROM public.pay_payment_correction_work_items AS work_row
    JOIN (
      SELECT DISTINCT work_id
      FROM pg_catalog.unnest(p_work_item_ids) AS requested(work_id)
      WHERE work_id IS NOT NULL
    ) AS requested_work ON requested_work.work_id=work_row.id
    WHERE work_row.correction_request_id=p_correction_request_id
      AND work_row.work_kind='PRE_BANK_CANCEL'
      AND NOT EXISTS (
        SELECT 1 FROM pg_temp._bpay_pre_cancel_fast_work AS fast_work
        WHERE fast_work.work_item_id=work_row.id
      )
    ORDER BY work_row.candidate_id,work_row.id
  LOOP
    BEGIN
      SELECT result_row.value
      INTO v_candidate_preflight
      FROM pg_catalog.jsonb_array_elements(
        COALESCE(v_preflight->'candidate_results','[]'::jsonb)
      ) AS result_row(value)
      WHERE result_row.value->>'candidate_id'=v_work.candidate_id::text
      LIMIT 1;

      v_result := public.pay_pre_bank_cancel_apply_work_item(v_work.id,p_actor_user_id);

      SELECT COALESCE(change_counter.seq,0),COALESCE(change_counter.scope_change_generation,0)
      INTO v_post_source_change_seq,v_post_dirty_generation
      FROM public.app_change_counters AS change_counter
      WHERE change_counter.entity_key='pay_candidate:'||v_work.candidate_id::text;

      v_result := COALESCE(v_result,'{}'::jsonb)
        || pg_catalog.jsonb_build_object(
          'cancellation_reversion_preflight',COALESCE(v_candidate_preflight,'{}'::jsonb),
          'cancellation_reversion_post_financial_authority',
          pg_catalog.jsonb_build_object(
            'contract_version','POST_FINANCIAL_CANCELLATION_AUTHORITY_V1',
            'source_change_seq',v_post_source_change_seq,
            'dirty_generation',v_post_dirty_generation,
            'authority_digest',pg_catalog.md5(
              COALESCE(v_candidate_preflight->>'post_draft_authority_digest','')||'|'||
              v_work.id::text||'|'||v_post_source_change_seq::text||'|'||
              v_post_dirty_generation::text||'|'||
              pg_catalog.md5((COALESCE(v_result,'{}'::jsonb)
                - 'applied_at_utc' - 'processed_at_utc')::text)||
              '|POST_FINANCIAL_CANCELLATION_AUTHORITY_V1'
            )
          )
        );
    EXCEPTION WHEN OTHERS THEN
      GET STACKED DIAGNOSTICS
        v_error_message=MESSAGE_TEXT,
        v_error_state=RETURNED_SQLSTATE;

      UPDATE public.pay_payment_correction_work_items AS failed_work
      SET status=CASE WHEN failed_work.attempt_count>=5 THEN 'FAILED_FINAL' ELSE 'FAILED_RETRYABLE' END,
          locked_at_utc=NULL,locked_by=NULL,
          processed_at_utc=CASE WHEN failed_work.attempt_count>=5
            THEN pg_catalog.clock_timestamp() ELSE NULL END,
          last_error=v_error_message,
          result_json=COALESCE(failed_work.result_json,'{}'::jsonb)
            || pg_catalog.jsonb_build_object(
              'code',CASE WHEN failed_work.attempt_count>=5 THEN 'FAILED_FINAL' ELSE 'FAILED_RETRYABLE' END,
              'sqlstate',v_error_state,'message',v_error_message
            )
      WHERE failed_work.id=v_work.id;

      v_result := pg_catalog.jsonb_build_object(
        'ok',false,
        'status',CASE WHEN v_work.attempt_count>=5 THEN 'FAILED_FINAL' ELSE 'FAILED_RETRYABLE' END,
        'code',CASE WHEN v_work.attempt_count>=5 THEN 'FAILED_FINAL' ELSE 'FAILED_RETRYABLE' END,
        'sqlstate',v_error_state,
        'message',v_error_message
      );
    END;

    v_results := v_results || pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object(
        'work_item_id',v_work.id::text,
        'candidate_id',CASE WHEN v_work.candidate_id IS NULL THEN NULL ELSE v_work.candidate_id::text END,
        'result',v_result
      )
    );
    v_count := v_count+1;
  END LOOP;

  IF v_count IS DISTINCT FROM (
    SELECT pg_catalog.count(DISTINCT requested.work_id)::integer
    FROM pg_catalog.unnest(p_work_item_ids) AS requested(work_id)
    WHERE requested.work_id IS NOT NULL
  ) THEN
    RAISE EXCEPTION 'PRE_BANK_CANCEL_APPLY_PAGE_MEMBERSHIP_MISMATCH'
      USING ERRCODE='P0001',DETAIL=pg_catalog.jsonb_build_object(
        'code','PRE_BANK_CANCEL_APPLY_PAGE_MEMBERSHIP_MISMATCH',
        'requested_count',pg_catalog.cardinality(p_work_item_ids),
        'resolved_count',v_count
      )::text;
  END IF;

  RETURN pg_catalog.jsonb_build_object(
    'ok',true,
    'contract_version','PRE_BANK_CANCEL_APPLY_PAGE_V1',
    'correction_request_id',p_correction_request_id::text,
    'work_item_count',v_count,
    'set_based_work_item_count',v_fast_count,
    'compatibility_work_item_count',v_count-v_fast_count,
    'candidate_results',v_results
  );
END;
$function$;

ALTER FUNCTION private.pay_pre_bank_cancel_apply_work_page_v1(uuid,uuid[],uuid,jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_pre_bank_cancel_apply_work_page_v1(uuid,uuid[],uuid,jsonb)
  FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION private.pay_pre_bank_cancel_apply_work_page_v1(uuid,uuid[],uuid,jsonb)
  TO postgres;


-- Untouched Draft cancellation page.  Draft reservation effects may change the
-- live recovery headroom, so this route restores the exact immutable V3 source
-- frozen before Draft only after a post-Draft sequence/generation fence proves
-- that no unrelated candidate change occurred.  Rejected pages mutate nothing.
CREATE OR REPLACE FUNCTION private.pay_workbench_draft_overlay_remove_page_v1(
  p_correction_request_id uuid,
  p_pay_batch_id uuid,
  p_session_id uuid,
  p_after_candidate_id uuid DEFAULT NULL::uuid,
  p_candidate_limit integer DEFAULT 100,
  p_finance_row_limit integer DEFAULT 500,
  p_actor_user_id uuid DEFAULT NULL::uuid,
  p_options_json jsonb DEFAULT '{}'::jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
PARALLEL UNSAFE
SECURITY DEFINER
SET search_path TO ''
AS $function$
DECLARE
  v_now timestamptz := pg_catalog.clock_timestamp();
  v_batch public.pay_batches%ROWTYPE;
  v_candidate_count integer := 0;
  v_item_count integer := 0;
  v_reservation_count integer := 0;
  v_last_candidate_id uuid := p_after_candidate_id;
  v_has_more boolean := false;
  v_candidate_ids jsonb := '[]'::jsonb;
  v_item_ids jsonb := '[]'::jsonb;
  v_patch jsonb := '{}'::jsonb;
  v_operation_id uuid := NULL::uuid;
  v_admission jsonb := '{}'::jsonb;
  v_admitted_count integer := 0;
  v_reversion_descriptors jsonb := '[]'::jsonb;
  v_publication jsonb := '{}'::jsonb;
  v_candidate_lock record;
BEGIN
  IF p_correction_request_id IS NULL OR p_pay_batch_id IS NULL OR p_session_id IS NULL
     OR p_candidate_limit IS NULL OR p_candidate_limit<1 OR p_candidate_limit>100
     OR p_finance_row_limit IS NULL OR p_finance_row_limit<1 OR p_finance_row_limit>500
     OR pg_catalog.jsonb_typeof(COALESCE(p_options_json,'{}'::jsonb))<>'object' THEN
    RAISE EXCEPTION 'DRAFT_OVERLAY_FAST_ARGUMENT_INVALID' USING ERRCODE='P0001';
  END IF;

  PERFORM pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtext('PAYMENT_DRAFT_OVERLAY_FAST_CANCEL'),
    pg_catalog.hashtext(p_pay_batch_id::text)
  );

  SELECT batch_row.* INTO v_batch
  FROM public.pay_batches AS batch_row
  WHERE batch_row.id=p_pay_batch_id
  FOR UPDATE;

  IF NOT FOUND
     OR pg_catalog.upper(pg_catalog.btrim(COALESCE(v_batch.status,''))) NOT IN ('DRAFT','DRAFT_CREATED','CANCELLED')
     OR pg_catalog.upper(pg_catalog.btrim(COALESCE(v_batch.execution_commit_state,'NOT_SUBMITTED'))) <> 'NOT_SUBMITTED'
     OR NULLIF(pg_catalog.btrim(COALESCE(v_batch.execution_commit_ref,'')),'') IS NOT NULL
     OR v_batch.execution_committed_at_utc IS NOT NULL
     OR EXISTS (
       SELECT 1 FROM public.banking_pay_operations AS unsafe_operation
       WHERE unsafe_operation.pay_batch_id=p_pay_batch_id
         AND unsafe_operation.operation_type IN ('PAYMENT_EXECUTE','PAYMENT_SETTLEMENT','REMITTANCE_QUEUE')
         AND unsafe_operation.status IN (
           'QUEUED','RUNNING','PROCESSING','CLAIMED','IN_PROGRESS','COMPLETE','COMPLETED','SUCCEEDED'
         )
     ) THEN
    RAISE EXCEPTION 'DRAFT_OVERLAY_FAST_NOT_UNTOUCHED'
      USING ERRCODE='P0001',DETAIL=pg_catalog.jsonb_build_object(
        'code','DRAFT_OVERLAY_FAST_NOT_UNTOUCHED','pay_batch_id',p_pay_batch_id
      )::text;
  END IF;

  SELECT operation_row.id
  INTO v_operation_id
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.operation_type='PAYMENT_CORRECTION'
    AND operation_row.pay_batch_id=p_pay_batch_id
    AND operation_row.input_json->>'correction_request_id'=p_correction_request_id::text
  ORDER BY operation_row.created_at_utc
  LIMIT 1;

  IF v_operation_id IS NULL THEN
    RAISE EXCEPTION 'DRAFT_OVERLAY_FAST_OPERATION_MISSING'
      USING ERRCODE='P0001',DETAIL=pg_catalog.jsonb_build_object(
        'code','DRAFT_OVERLAY_FAST_OPERATION_MISSING','correction_request_id',p_correction_request_id
      )::text;
  END IF;

  DROP TABLE IF EXISTS pg_temp._bpay_draft_overlay_candidate_page;
  CREATE TEMP TABLE pg_temp._bpay_draft_overlay_candidate_page ON COMMIT DROP AS
  SELECT request_candidate.pay_batch_candidate_id,candidate_row.candidate_id
  FROM public.pay_payment_correction_request_candidates AS request_candidate
  JOIN public.pay_batch_candidates AS candidate_row
    ON candidate_row.id=request_candidate.pay_batch_candidate_id
   AND candidate_row.pay_batch_id=p_pay_batch_id
  WHERE request_candidate.correction_request_id=p_correction_request_id
    AND (p_after_candidate_id IS NULL OR candidate_row.candidate_id>p_after_candidate_id)
  ORDER BY candidate_row.candidate_id
  LIMIT p_candidate_limit;

  SELECT pg_catalog.count(*)::integer,
         pg_catalog.max(candidate_page.candidate_id::text)::uuid,
         COALESCE(pg_catalog.jsonb_agg(candidate_page.candidate_id::text
           ORDER BY candidate_page.candidate_id),'[]'::jsonb)
  INTO v_candidate_count,v_last_candidate_id,v_candidate_ids
  FROM pg_temp._bpay_draft_overlay_candidate_page AS candidate_page;

  IF v_candidate_count=0 THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok',true,'status','DRAFT_OVERLAY_FAST_COMPLETE','candidate_count',0,
      'has_more',false,'full_build_count',0,'reconciliation_count',0
    );
  END IF;

  -- Complete the global lock prefix before reading the frozen/live fence.
  FOR v_candidate_lock IN
    SELECT candidate_page.candidate_id
    FROM pg_temp._bpay_draft_overlay_candidate_page AS candidate_page
    ORDER BY candidate_page.candidate_id
  LOOP
    PERFORM pg_catalog.pg_advisory_xact_lock(
      pg_catalog.hashtextextended(
        public._pay_workbench_candidate_serial_key(v_candidate_lock.candidate_id),
        24062027
      )
    );
  END LOOP;

  v_admission := private.pay_workbench_cancel_reversion_admission_page_v1(
    p_correction_request_id,v_operation_id,p_session_id,ARRAY[]::uuid[],
    pg_catalog.jsonb_build_object(
      'mode','DRAFT_OVERLAY_PREFLIGHT','candidate_ids',v_candidate_ids
    )
  );
  v_admitted_count := COALESCE((v_admission->>'admitted_count')::integer,0);

  IF v_admitted_count IS DISTINCT FROM v_candidate_count THEN
    RETURN pg_catalog.jsonb_build_object(
      'ok',true,'status','DRAFT_OVERLAY_FAST_REJECTED','fast_route_eligible',false,
      'candidate_count',v_candidate_count,'candidate_ids',v_candidate_ids,
      'admission',v_admission,'has_more',false,'full_build_count',0,
      'reconciliation_count',0,'financial_work_item_count',0,
      'policy_x_authority_scope','SAFE_FALLBACK_BEFORE_DRAFT_MUTATION'
    );
  END IF;

  IF (
    SELECT pg_catalog.count(*)
    FROM public.pay_batch_items AS item_row
    JOIN pg_temp._bpay_draft_overlay_candidate_page AS candidate_page
      ON candidate_page.pay_batch_candidate_id=item_row.pay_batch_candidate_id
    WHERE COALESCE(item_row.is_voided,false) IS NOT TRUE
  ) > p_finance_row_limit THEN
    RAISE EXCEPTION 'DRAFT_OVERLAY_FAST_FINANCE_ROW_LIMIT'
      USING ERRCODE='P0001',DETAIL=pg_catalog.jsonb_build_object(
        'code','DRAFT_OVERLAY_FAST_FINANCE_ROW_LIMIT','limit',p_finance_row_limit
      )::text;
  END IF;

  WITH voided AS (
    UPDATE public.pay_batch_items AS item_row
    SET is_voided=true,updated_at=v_now
    FROM pg_temp._bpay_draft_overlay_candidate_page AS candidate_page
    WHERE item_row.pay_batch_candidate_id=candidate_page.pay_batch_candidate_id
      AND COALESCE(item_row.is_voided,false) IS NOT TRUE
    RETURNING item_row.id
  )
  SELECT pg_catalog.count(*)::integer,
         COALESCE(pg_catalog.jsonb_agg(voided.id::text ORDER BY voided.id),'[]'::jsonb)
  INTO v_item_count,v_item_ids FROM voided;

  WITH released AS (
    UPDATE public.pay_advance_reservations AS reservation_row
    SET status='RELEASED',released_at_utc=COALESCE(reservation_row.released_at_utc,v_now),
        released_reason='DRAFT_OVERLAY_FAST_CANCEL',updated_by_user_id=p_actor_user_id
    FROM pg_temp._bpay_draft_overlay_candidate_page AS candidate_page
    WHERE reservation_row.pay_batch_id=p_pay_batch_id
      AND reservation_row.pay_batch_candidate_id=candidate_page.pay_batch_candidate_id
      AND pg_catalog.upper(pg_catalog.btrim(COALESCE(reservation_row.status,'')))='RESERVED'
    RETURNING reservation_row.id
  ) SELECT pg_catalog.count(*)::integer INTO v_reservation_count FROM released;

  UPDATE public.pay_batch_candidates AS candidate_row
  SET gross_preview=0,net_bank_amount=0,debt_created=0,
      loan_repayment_taken=0,overpayment_recovery_taken=0,
      awaiting_net_amount=false,updated_at=v_now
  FROM pg_temp._bpay_draft_overlay_candidate_page AS candidate_page
  WHERE candidate_row.id=candidate_page.pay_batch_candidate_id;

  v_patch := public.pay_workbench_patch_preview_after_batch_mutation(
    p_session_id,p_pay_batch_id,'DRAFT_CANCEL',p_actor_user_id,
    pg_catalog.jsonb_build_object(
      'candidate_ids',v_candidate_ids,
      'changed_pay_batch_item_ids',v_item_ids,
      'defer_complex_enqueue',true,
      'maximum_candidate_count',p_candidate_limit
    )
  );

  IF COALESCE((v_patch->>'ok')::boolean,false) IS NOT TRUE THEN
    RAISE EXCEPTION 'DRAFT_OVERLAY_FAST_WORKBENCH_PATCH_FAILED'
      USING ERRCODE='P0001',DETAIL=pg_catalog.jsonb_build_object(
        'code','DRAFT_OVERLAY_FAST_WORKBENCH_PATCH_FAILED','patch',v_patch
      )::text;
  END IF;

  WITH admitted AS (
    SELECT result_row.value,
           change_counter.seq AS current_source_change_seq,
           change_counter.scope_change_generation AS current_dirty_generation,
           pg_catalog.md5(
             p_correction_request_id::text||':'||(result_row.value->>'candidate_id')||':'||
             v_operation_id::text||':'||change_counter.seq::text||':'||
             change_counter.scope_change_generation::text||':DRAFT_OVERLAY_FAST_V1'
           ) AS run_hash,
           pg_catalog.md5(
             COALESCE(result_row.value->>'post_draft_authority_digest','')||'|'||
             p_correction_request_id::text||'|'||(result_row.value->>'candidate_id')||'|'||
             change_counter.seq::text||'|'||change_counter.scope_change_generation::text||'|'||
             COALESCE(item_proof.item_digest,'')||'|'||COALESCE(reservation_proof.reservation_digest,'')||
             '|DRAFT_OVERLAY_FAST_REVERSION_V1'
           ) AS reversion_digest
    FROM pg_catalog.jsonb_array_elements(
      COALESCE(v_admission->'candidate_results','[]'::jsonb)
    ) AS result_row(value)
    JOIN public.app_change_counters AS change_counter
      ON change_counter.entity_key='pay_candidate:'||(result_row.value->>'candidate_id')
    LEFT JOIN LATERAL (
      SELECT pg_catalog.md5(COALESCE(pg_catalog.string_agg(
        item_row.id::text||':'||COALESCE(item_row.is_voided,false)::text,
        '|' ORDER BY item_row.id
      ),'')) AS item_digest
      FROM public.pay_batch_items AS item_row
      JOIN public.pay_batch_candidates AS batch_candidate
        ON batch_candidate.id=item_row.pay_batch_candidate_id
      WHERE batch_candidate.pay_batch_id=p_pay_batch_id
        AND batch_candidate.candidate_id=(result_row.value->>'candidate_id')::uuid
    ) AS item_proof ON true
    LEFT JOIN LATERAL (
      SELECT pg_catalog.md5(COALESCE(pg_catalog.string_agg(
        reservation_row.id::text||':'||COALESCE(reservation_row.status,''),
        '|' ORDER BY reservation_row.id
      ),'')) AS reservation_digest
      FROM public.pay_advance_reservations AS reservation_row
      JOIN public.pay_batch_candidates AS batch_candidate
        ON batch_candidate.id=reservation_row.pay_batch_candidate_id
      WHERE reservation_row.pay_batch_id=p_pay_batch_id
        AND batch_candidate.candidate_id=(result_row.value->>'candidate_id')::uuid
    ) AS reservation_proof ON true
    WHERE COALESCE((result_row.value->>'admitted')::boolean,false)
  ), descriptors AS (
    SELECT pg_catalog.jsonb_build_object(
      'candidate_id',admitted.value->>'candidate_id',
      'economic_build_id',admitted.value->>'original_economic_build_id',
      'source_build_run_id',pg_catalog.substr(admitted.run_hash,1,8)||'-'||
        pg_catalog.substr(admitted.run_hash,9,4)||'-'||pg_catalog.substr(admitted.run_hash,13,4)||'-'||
        pg_catalog.substr(admitted.run_hash,17,4)||'-'||pg_catalog.substr(admitted.run_hash,21,12),
      'source_change_seq',admitted.current_source_change_seq,
      'source_identity_digest',admitted.value->>'source_identity_digest',
      'session_version',admitted.value->>'session_version',
      'completion_job_id',v_operation_id,
      'refresh_scope_kind','CANDIDATE_FULL_LIVE',
      'publication_options_json',pg_catalog.jsonb_build_object(
        'contract_version',3,'semantic_contract_version','READY_TO_PAY_SEMANTIC_V2',
        'authority_kind','CERTIFIED_CANCELLATION_REVERSION',
        'invocation_kind','CANCELLATION_REVERSION_FINALISE','final_state',
          CASE WHEN COALESCE((admitted.value->>'source_count')::integer,0)=0
            THEN 'SOURCE_EMPTY' ELSE 'READY' END,
        'source_session_id',p_session_id,
        'original_economic_build_id',admitted.value->>'original_economic_build_id',
        'original_source_build_run_id',admitted.value->>'original_source_build_run_id',
        'original_source_publication_id',admitted.value->>'original_source_publication_id',
        'cancellation_request_id',p_correction_request_id,
        'cancellation_operation_id',v_operation_id,
        'pay_batch_id',p_pay_batch_id,
        'cancellation_reversion_run_id',pg_catalog.substr(admitted.run_hash,1,8)||'-'||
          pg_catalog.substr(admitted.run_hash,9,4)||'-'||pg_catalog.substr(admitted.run_hash,13,4)||'-'||
          pg_catalog.substr(admitted.run_hash,17,4)||'-'||pg_catalog.substr(admitted.run_hash,21,12),
        'cancellation_route','DRAFT_OVERLAY_FAST',
        'financial_reversion_digest',admitted.reversion_digest,
        'original_semantic_proof_digest',admitted.value->>'semantic_proof_digest',
        'source_count',admitted.value->>'source_count'
      )
    ) AS descriptor
    FROM admitted
  )
  SELECT COALESCE(pg_catalog.jsonb_agg(descriptors.descriptor
    ORDER BY descriptors.descriptor->>'candidate_id'),'[]'::jsonb)
  INTO v_reversion_descriptors
  FROM descriptors;

  IF pg_catalog.jsonb_array_length(v_reversion_descriptors) IS DISTINCT FROM v_candidate_count THEN
    RAISE EXCEPTION 'DRAFT_OVERLAY_FAST_REVERSION_DESCRIPTOR_MISMATCH'
      USING ERRCODE='P0001',DETAIL=pg_catalog.jsonb_build_object(
        'code','DRAFT_OVERLAY_FAST_REVERSION_DESCRIPTOR_MISMATCH'
      )::text;
  END IF;

  v_publication := private.pay_workbench_publish_certified_source_preview_page_v1(
    p_session_id,v_reversion_descriptors,'{}'::jsonb
  );

  IF COALESCE((v_publication->>'ok')::boolean,false) IS NOT TRUE
     OR COALESCE((v_publication->>'candidate_count')::integer,0) IS DISTINCT FROM v_candidate_count THEN
    RAISE EXCEPTION 'DRAFT_OVERLAY_FAST_REVERSION_PUBLICATION_FAILED'
      USING ERRCODE='P0001',DETAIL=pg_catalog.jsonb_build_object(
        'code','DRAFT_OVERLAY_FAST_REVERSION_PUBLICATION_FAILED'
      )::text;
  END IF;

  v_has_more := EXISTS (
    SELECT 1
    FROM public.pay_payment_correction_request_candidates AS request_candidate
    JOIN public.pay_batch_candidates AS candidate_row
      ON candidate_row.id=request_candidate.pay_batch_candidate_id
     AND candidate_row.pay_batch_id=p_pay_batch_id
    WHERE request_candidate.correction_request_id=p_correction_request_id
      AND candidate_row.candidate_id>v_last_candidate_id
  );

  IF NOT v_has_more THEN
    UPDATE public.pay_batches AS batch_row
    SET status='CANCELLED',cancelled_at_utc=COALESCE(batch_row.cancelled_at_utc,v_now),
        cancelled_by_user_id=COALESCE(batch_row.cancelled_by_user_id,p_actor_user_id),
        cancel_reason=COALESCE(batch_row.cancel_reason,'DRAFT_OVERLAY_FAST_CANCEL'),
        total_bank_out=0,total_debt_created=0
    WHERE batch_row.id=p_pay_batch_id;
  END IF;

  RETURN pg_catalog.jsonb_build_object(
    'ok',true,'status',CASE WHEN v_has_more THEN 'DRAFT_OVERLAY_FAST_PAGE' ELSE 'DRAFT_OVERLAY_FAST_COMPLETE' END,
    'candidate_count',v_candidate_count,'voided_item_count',v_item_count,
    'released_reservation_count',v_reservation_count,'candidate_ids',v_candidate_ids,
    'last_candidate_id',v_last_candidate_id,'has_more',v_has_more,
    'fast_route_eligible',true,'admission',v_admission,
    'workbench_patch',v_patch,'reversion_publication',v_publication,
    'financial_work_item_count',0,
    'full_build_count',0,'reconciliation_count',0,
    'policy_x_authority_scope','POST_DRAFT_FROZEN_OVERLAY_REMOVAL'
  );
END;
$function$;

ALTER FUNCTION private.pay_workbench_draft_overlay_remove_page_v1(uuid,uuid,uuid,uuid,integer,integer,uuid,jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_draft_overlay_remove_page_v1(uuid,uuid,uuid,uuid,integer,integer,uuid,jsonb)
  FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION private.pay_workbench_draft_overlay_remove_page_v1(uuid,uuid,uuid,uuid,integer,integer,uuid,jsonb)
  TO postgres;
