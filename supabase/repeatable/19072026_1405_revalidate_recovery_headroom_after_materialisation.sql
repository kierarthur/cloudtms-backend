CREATE OR REPLACE FUNCTION public.pay_workbench_revalidate_zero_retained_recovery_headroom_v1(
  p_session_id uuid,
  p_candidate_id uuid
)
RETURNS jsonb
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path TO 'public'
SET plpgsql_check.mode TO 'disabled'
SET plpgsql_check.profiler TO 'off'
SET plpgsql_check.tracer TO 'off'
SET plpgsql_check.constants_tracing TO 'off'
SET plpgsql_check.cursors_leaks TO 'off'
SET plpgsql_check.strict_cursors_leaks TO 'off'
SET plpgsql_check.fatal_errors TO 'off'
AS $function$
DECLARE
  v_now timestamptz := clock_timestamp();
  v_session_row public.banking_pay_workbench_sessions%ROWTYPE;
  v_nonterminal_line_count integer := 0;
  v_retained_positive_headroom numeric(12,2) := 0;
  v_recovery_group_count integer := 0;
  v_promoted_count integer := 0;
  v_promoted_selected_count integer := 0;
  v_repaired_authority_scope_count integer := 0;
  v_ready_recovery_capped_count integer := 0;
  v_ready_recovery_blocked_count integer := 0;
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

  v_selection_intent_mode := UPPER(BTRIM(COALESCE(
    v_session_row.progress_json#>>'{selection_intent_v1,canonical_preview_lines,mode}',
    CASE
      WHEN COALESCE(v_session_row.server_selected_preview_row_ids_provided, false)
        THEN 'EXPLICIT_INCLUDE'
      ELSE 'IMPLICIT_ALL'
    END
  )));
  IF v_selection_intent_mode NOT IN ('IMPLICIT_ALL', 'EXPLICIT_INCLUDE') THEN
    v_selection_intent_mode := CASE
      WHEN COALESCE(v_session_row.server_selected_preview_row_ids_provided, false)
        THEN 'EXPLICIT_INCLUDE'
      ELSE 'IMPLICIT_ALL'
    END;
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
    AND LOWER(BTRIM(COALESCE(preview_row.row_json->>'post_draft_overlay_applied', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
    AND (
      v_selection_intent_mode <> 'EXPLICIT_INCLUDE'
      OR (
        COALESCE(preview_row.selected, false) IS TRUE
        AND UPPER(BTRIM(COALESCE(preview_row.selection_state, ''))) = 'SELECTED'
      )
    )
    AND UPPER(BTRIM(COALESCE(preview_row.row_json->>'line_type', preview_row.row_json->>'item_type', ''))) NOT IN (
      'MANUAL_DEBT_RECOVERY',
      'OVERPAYMENT_RECOVERY',
      'LOAN_REPAYMENT',
      'PAYMENT_ADVANCE_REPAYMENT'
    );

  IF COALESCE(v_retained_positive_headroom, 0) > 0 THEN
    /*
     * A recovery line may have been materialised before the candidate's final
     * positive correction carrier. In that order it is correctly emitted as
     * NO_PAY_HEADROOM, but the terminal candidate view is then stale once the
     * positive carrier exists. Reconcile the other direction here as well:
     * promote only rows whose sole blocker is NO_PAY_HEADROOM, allocate against
     * retained positive pay in the same pay channel, and leave every financial
     * amount sourced from the already-materialised pre-draft authority.
     */
    DROP TABLE IF EXISTS pg_temp._tmp_pay_wb_ready_recovery_allocation;
    CREATE TEMP TABLE _tmp_pay_wb_ready_recovery_allocation ON COMMIT DROP AS
    WITH positive_by_channel AS (
      SELECT
        UPPER(BTRIM(COALESCE(
          positive_row.row_json->>'pay_channel',
          positive_row.row_json->>'current_pay_method',
          positive_row.row_json->>'candidate_pay_method',
          ''
        ))) AS pay_channel,
        ROUND(COALESCE(SUM(
          CASE
            WHEN COALESCE(positive_row.row_json->>'amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$'
              THEN GREATEST((positive_row.row_json->>'amount_ex_vat')::numeric, 0)
            ELSE 0::numeric
          END
        ), 0), 2)::numeric(12,2) AS positive_headroom_ex_vat
      FROM public.banking_pay_workbench_preview_rows AS positive_row
      WHERE positive_row.session_id = p_session_id
        AND positive_row.session_version = COALESCE(v_session_row.version, 1)
        AND positive_row.candidate_id = p_candidate_id
        AND LOWER(BTRIM(COALESCE(positive_row.section, ''))) = 'canonical_preview_lines'
        AND UPPER(BTRIM(COALESCE(positive_row.status, ''))) = 'READY'
        AND LOWER(BTRIM(COALESCE(positive_row.row_json->>'draftable', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        AND LOWER(BTRIM(COALESCE(positive_row.row_json->>'is_ready_for_draft', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        AND LOWER(BTRIM(COALESCE(positive_row.row_json->>'post_draft_overlay_applied', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
        AND (
          v_selection_intent_mode <> 'EXPLICIT_INCLUDE'
          OR (
            COALESCE(positive_row.selected, false) IS TRUE
            AND UPPER(BTRIM(COALESCE(positive_row.selection_state, ''))) = 'SELECTED'
          )
        )
        AND UPPER(BTRIM(COALESCE(positive_row.row_json->>'line_type', positive_row.row_json->>'item_type', ''))) NOT IN (
          'MANUAL_DEBT_RECOVERY',
          'OVERPAYMENT_RECOVERY',
          'LOAN_REPAYMENT',
          'PAYMENT_ADVANCE_REPAYMENT'
        )
      GROUP BY UPPER(BTRIM(COALESCE(
        positive_row.row_json->>'pay_channel',
        positive_row.row_json->>'current_pay_method',
        positive_row.row_json->>'candidate_pay_method',
        ''
      )))
    ), ready_recovery AS (
      SELECT
        ready_row.id AS ready_preview_row_id,
        ready_row.row_key,
        ready_row.row_ordinal,
        ready_row.timesheet_id,
        ready_row.key_type,
        ready_row.key_value,
        ready_row.selected,
        ready_row.selection_state,
        COALESCE(ready_row.row_json, '{}'::jsonb) AS base_row_json,
        UPPER(BTRIM(COALESCE(
          ready_row.row_json->>'pay_channel',
          ready_row.row_json->>'current_pay_method',
          ready_row.row_json->>'candidate_pay_method',
          ''
        ))) AS pay_channel,
        ROUND(
          CASE
            WHEN COALESCE((
              SELECT SUM(
                CASE
                  WHEN COALESCE(component_row.value->>'preview_due_amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$'
                    THEN ABS((component_row.value->>'preview_due_amount_ex_vat')::numeric)
                  ELSE 0::numeric
                END
              )
              FROM jsonb_array_elements(
                CASE
                  WHEN jsonb_typeof(ready_row.row_json->'case_components') = 'array'
                    THEN ready_row.row_json->'case_components'
                  ELSE '[]'::jsonb
                END
              ) AS component_row(value)
            ), 0) > 0
              THEN (
                SELECT SUM(ABS((component_row.value->>'preview_due_amount_ex_vat')::numeric))
                FROM jsonb_array_elements(ready_row.row_json->'case_components') AS component_row(value)
                WHERE COALESCE(component_row.value->>'preview_due_amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$'
              )
            WHEN COALESCE(ready_row.row_json->>'amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$'
             AND ABS((ready_row.row_json->>'amount_ex_vat')::numeric) > 0
              THEN ABS((ready_row.row_json->>'amount_ex_vat')::numeric)
            WHEN COALESCE(ready_row.row_json#>>'{case_resolution_summary,due_amount_ex_vat}', '') ~ '^-?[0-9]+([.][0-9]+)?$'
              THEN ABS((ready_row.row_json#>>'{case_resolution_summary,due_amount_ex_vat}')::numeric)
            WHEN COALESCE(ready_row.row_json->>'nominal_due_amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$'
              THEN ABS((ready_row.row_json->>'nominal_due_amount_ex_vat')::numeric)
            ELSE 0::numeric
          END,
          2
        )::numeric(12,2) AS nominal_due_amount_ex_vat
      FROM public.banking_pay_workbench_preview_rows AS ready_row
      WHERE ready_row.session_id = p_session_id
        AND ready_row.session_version = COALESCE(v_session_row.version, 1)
        AND ready_row.candidate_id = p_candidate_id
        AND LOWER(BTRIM(COALESCE(ready_row.section, ''))) = 'canonical_preview_lines'
        AND UPPER(BTRIM(COALESCE(ready_row.status, ''))) = 'READY'
        AND LOWER(BTRIM(COALESCE(ready_row.row_json->>'post_draft_overlay_applied', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
        AND UPPER(BTRIM(COALESCE(ready_row.row_json->>'line_type', ready_row.row_json->>'item_type', ''))) IN (
          'MANUAL_DEBT_RECOVERY',
          'OVERPAYMENT_RECOVERY',
          'LOAN_REPAYMENT',
          'PAYMENT_ADVANCE_REPAYMENT'
        )
        AND COALESCE(ready_row.row_json->>'amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$'
        AND ROUND((ready_row.row_json->>'amount_ex_vat')::numeric, 2) < 0
    ), ranked_recovery AS (
      SELECT
        ready_recovery.*,
        COALESCE(positive_by_channel.positive_headroom_ex_vat, 0)::numeric(12,2) AS positive_headroom_ex_vat,
        COALESCE(SUM(ready_recovery.nominal_due_amount_ex_vat) OVER (
          PARTITION BY ready_recovery.pay_channel
          ORDER BY ready_recovery.row_ordinal, ready_recovery.ready_preview_row_id
          ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0)::numeric(12,2) AS prior_nominal_due_amount_ex_vat
      FROM ready_recovery
      LEFT JOIN positive_by_channel
        ON positive_by_channel.pay_channel = ready_recovery.pay_channel
      WHERE ready_recovery.pay_channel IN ('PAYE', 'UMBRELLA')
        AND ready_recovery.nominal_due_amount_ex_vat > 0
    ), allocated_recovery AS (
      SELECT
        ranked_recovery.*,
        ROUND(LEAST(
          ranked_recovery.nominal_due_amount_ex_vat,
          GREATEST(
            ranked_recovery.positive_headroom_ex_vat - ranked_recovery.prior_nominal_due_amount_ex_vat,
            0
          )
        ), 2)::numeric(12,2) AS recoverable_amount_ex_vat
      FROM ranked_recovery
    )
    SELECT
      allocated_recovery.*,
      ROUND(SUM(allocated_recovery.recoverable_amount_ex_vat) OVER (
        PARTITION BY allocated_recovery.pay_channel
      ), 2)::numeric(12,2) AS channel_recoverable_amount_ex_vat
    FROM allocated_recovery;

    WITH capped_source AS (
      SELECT
        allocation.ready_preview_row_id,
        allocation.base_row_json
          || jsonb_build_object(
            'amount_ex_vat', -allocation.recoverable_amount_ex_vat,
            'preview_amount_ex_vat', -allocation.recoverable_amount_ex_vat,
            'amount_display', -allocation.recoverable_amount_ex_vat,
            'section_amount_ex_vat', -allocation.recoverable_amount_ex_vat,
            'section_amount_display', -allocation.recoverable_amount_ex_vat,
            'target_pay_ex_vat', -allocation.recoverable_amount_ex_vat,
            'ready_preview_amount_ex_vat', -allocation.recoverable_amount_ex_vat,
            'preview_component_amount_ex_vat', -allocation.recoverable_amount_ex_vat,
            'nominal_due_amount_ex_vat', allocation.nominal_due_amount_ex_vat,
            'recoverable_this_pay_run_ex_vat', allocation.recoverable_amount_ex_vat,
            'recovery_group_recoverable_this_pay_run_ex_vat', allocation.channel_recoverable_amount_ex_vat,
            'materialisation_recovery_headroom_revalidated', true,
            'materialisation_recovery_headroom_revalidated_at_utc', v_now::text,
            'retained_positive_headroom_ex_vat', allocation.positive_headroom_ex_vat,
            'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH',
            'case_resolution_summary', CASE
              WHEN jsonb_typeof(COALESCE(allocation.base_row_json->'case_resolution_summary', '{}'::jsonb)) = 'object'
                THEN COALESCE(allocation.base_row_json->'case_resolution_summary', '{}'::jsonb)
                  || jsonb_build_object('due_amount_ex_vat', allocation.recoverable_amount_ex_vat)
              ELSE jsonb_build_object('due_amount_ex_vat', allocation.recoverable_amount_ex_vat)
            END
          ) AS capped_row_json
      FROM pg_temp._tmp_pay_wb_ready_recovery_allocation AS allocation
      WHERE allocation.recoverable_amount_ex_vat > 0
    ), contracted_source AS (
      SELECT
        capped_source.ready_preview_row_id,
        capped_source.capped_row_json,
        public.pay_workbench_preview_line_contract_ok(
          p_line_json => capped_source.capped_row_json,
          p_economic_key_json => COALESCE(capped_source.capped_row_json->'economic_key', '{}'::jsonb),
          p_target_section => 'canonical_preview_lines'
        ) AS capped_contract_json
      FROM capped_source
    )
    UPDATE public.banking_pay_workbench_preview_rows AS ready_row
    SET row_json = contracted_source.capped_row_json
          || jsonb_build_object(
            'preview_contract', contracted_source.capped_contract_json,
            'selection_allowed', true
          ),
        updated_at_utc = v_now
    FROM contracted_source
    WHERE ready_row.id = contracted_source.ready_preview_row_id
      AND LOWER(BTRIM(COALESCE(contracted_source.capped_contract_json->>'ok', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
      AND LOWER(BTRIM(COALESCE(contracted_source.capped_contract_json->>'selection_allowed', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');

    GET DIAGNOSTICS v_ready_recovery_capped_count = ROW_COUNT;

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
      allocation.row_key,
      allocation.row_ordinal,
      (
        allocation.base_row_json
        - 'source_basis_json'
        - 'frozen_source_basis_json'
        - 'frozen_component_snapshot_json'
        - 'finance_component_id'
        - 'materialised_from_line_work_id'
      )
      || jsonb_build_object(
        'line_key', allocation.row_key,
        'row_key', allocation.row_key,
        'presentation_line_id', allocation.row_key,
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
        'recoverable_this_pay_run_ex_vat', 0,
        'nominal_due_amount_ex_vat', allocation.nominal_due_amount_ex_vat,
        'draftable', false,
        'is_ready_for_draft', false,
        'selection_allowed', false,
        'is_excluded_from_allocation', true,
        'selected', false,
        'selection_state', 'NOT_SELECTABLE',
        'blocked_reason_codes', jsonb_build_array('NO_PAY_HEADROOM'),
        'materialisation_recovery_headroom_revalidated', true,
        'materialisation_recovery_headroom_revalidated_at_utc', v_now::text,
        'retained_positive_headroom_ex_vat', allocation.positive_headroom_ex_vat,
        'policy_x_authority_scope', 'PRE_DRAFT_LIVE_WORKBENCH_ONLY',
        'case_resolution_summary', CASE
          WHEN jsonb_typeof(COALESCE(allocation.base_row_json->'case_resolution_summary', '{}'::jsonb)) = 'object'
            THEN COALESCE(allocation.base_row_json->'case_resolution_summary', '{}'::jsonb)
              || jsonb_build_object('due_amount_ex_vat', 0)
          ELSE jsonb_build_object('due_amount_ex_vat', 0)
        END
      )
      || jsonb_build_object(
        'preview_contract', COALESCE(allocation.base_row_json->'preview_contract', '{}'::jsonb)
          || jsonb_build_object(
            'line_key', allocation.row_key,
            'amount_ex_vat', 0,
            'draftable', false,
            'is_ready_for_draft', false,
            'selection_allowed', false,
            'is_excluded_from_allocation', true,
            'target_section', 'blocked_for_pay',
            'presentation_section', 'BLOCKED_FOR_PAY'
          )
      ),
      allocation.timesheet_id,
      allocation.key_type,
      allocation.key_value,
      false,
      'NOT_SELECTABLE',
      'READY',
      COALESCE(v_session_row.version, 1),
      v_now,
      v_now
    FROM pg_temp._tmp_pay_wb_ready_recovery_allocation AS allocation
    WHERE allocation.recoverable_amount_ex_vat <= 0
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

    GET DIAGNOSTICS v_ready_recovery_blocked_count = ROW_COUNT;

    UPDATE public.banking_pay_workbench_preview_rows AS ready_row
    SET status = 'SUPERSEDED',
        selected = false,
        selection_state = 'SUPERSEDED',
        row_json = COALESCE(ready_row.row_json, '{}'::jsonb)
          || jsonb_build_object(
            'selected', false,
            'selection_state', 'SUPERSEDED',
            'superseded_reason', 'RECOVERY_EXCEEDS_SELECTED_POSITIVE_HEADROOM',
            'materialisation_recovery_headroom_revalidated', true,
            'materialisation_recovery_headroom_revalidated_at_utc', v_now::text,
            'policy_x_authority_scope', 'PRE_DRAFT_LIVE_WORKBENCH_ONLY'
          ),
        updated_at_utc = v_now
    WHERE ready_row.id IN (
      SELECT allocation.ready_preview_row_id
      FROM pg_temp._tmp_pay_wb_ready_recovery_allocation AS allocation
      WHERE allocation.recoverable_amount_ex_vat <= 0
    );

    IF COALESCE(v_ready_recovery_capped_count, 0) > 0
       OR COALESCE(v_ready_recovery_blocked_count, 0) > 0 THEN
      UPDATE public.banking_pay_workbench_candidate_line_work AS line_work
      SET result_row_json = active_row.row_json,
          updated_at_utc = v_now
      FROM (
        SELECT ready_row.row_key, ready_row.row_json
        FROM public.banking_pay_workbench_preview_rows AS ready_row
        WHERE ready_row.session_id = p_session_id
          AND ready_row.session_version = COALESCE(v_session_row.version, 1)
          AND ready_row.candidate_id = p_candidate_id
          AND LOWER(BTRIM(COALESCE(ready_row.section, ''))) = 'canonical_preview_lines'
          AND UPPER(BTRIM(COALESCE(ready_row.status, ''))) = 'READY'
        UNION ALL
        SELECT blocked_row.row_key, blocked_row.row_json
        FROM public.banking_pay_workbench_preview_rows AS blocked_row
        WHERE blocked_row.session_id = p_session_id
          AND blocked_row.session_version = COALESCE(v_session_row.version, 1)
          AND blocked_row.candidate_id = p_candidate_id
          AND LOWER(BTRIM(COALESCE(blocked_row.section, ''))) = 'blocked_for_pay'
          AND UPPER(BTRIM(COALESCE(blocked_row.status, ''))) = 'READY'
      ) AS active_row
      WHERE line_work.session_id = p_session_id
        AND line_work.candidate_id = p_candidate_id
        AND line_work.line_key = active_row.row_key
        AND EXISTS (
          SELECT 1
          FROM pg_temp._tmp_pay_wb_ready_recovery_allocation AS allocation
          WHERE allocation.row_key = line_work.line_key
        );
    END IF;

    WITH repair_source AS (
      SELECT
        ready_recovery_row.id,
        ready_recovery_row.row_json
          || jsonb_build_object(
            'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH',
            'materialisation_recovery_headroom_revalidated', true,
            'materialisation_recovery_headroom_revalidated_at_utc', v_now::text
          ) AS repaired_row_json
      FROM public.banking_pay_workbench_preview_rows AS ready_recovery_row
      WHERE ready_recovery_row.session_id = p_session_id
        AND ready_recovery_row.session_version = COALESCE(v_session_row.version, 1)
        AND ready_recovery_row.candidate_id = p_candidate_id
        AND LOWER(BTRIM(COALESCE(ready_recovery_row.section, ''))) = 'canonical_preview_lines'
        AND UPPER(BTRIM(COALESCE(ready_recovery_row.status, ''))) = 'READY'
        AND LOWER(BTRIM(COALESCE(ready_recovery_row.row_json->>'draftable', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        AND LOWER(BTRIM(COALESCE(ready_recovery_row.row_json->>'is_ready_for_draft', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        AND LOWER(BTRIM(COALESCE(ready_recovery_row.row_json->>'post_draft_overlay_applied', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
        AND LOWER(BTRIM(COALESCE(ready_recovery_row.row_json->>'materialisation_recovery_headroom_revalidated', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        AND UPPER(BTRIM(COALESCE(ready_recovery_row.row_json->>'line_type', ready_recovery_row.row_json->>'item_type', ''))) IN (
          'MANUAL_DEBT_RECOVERY',
          'OVERPAYMENT_RECOVERY',
          'LOAN_REPAYMENT',
          'PAYMENT_ADVANCE_REPAYMENT'
        )
        AND UPPER(BTRIM(COALESCE(ready_recovery_row.row_json->>'policy_x_authority_scope', ''))) = 'PRE_DRAFT_LIVE_WORKBENCH_ONLY'
    ), contracted_repair AS (
      SELECT
        repair_source.id,
        repair_source.repaired_row_json,
        public.pay_workbench_preview_line_contract_ok(
          p_line_json => repair_source.repaired_row_json,
          p_economic_key_json => COALESCE(repair_source.repaired_row_json->'economic_key', '{}'::jsonb),
          p_target_section => 'canonical_preview_lines'
        ) AS repaired_contract_json
      FROM repair_source
    )
    UPDATE public.banking_pay_workbench_preview_rows AS ready_recovery_row
    SET row_json = contracted_repair.repaired_row_json
          || jsonb_build_object(
            'preview_contract', contracted_repair.repaired_contract_json,
            'selection_allowed', true
          ),
        updated_at_utc = v_now
    FROM contracted_repair
    WHERE ready_recovery_row.id = contracted_repair.id
      AND LOWER(BTRIM(COALESCE(contracted_repair.repaired_contract_json->>'ok', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
      AND LOWER(BTRIM(COALESCE(contracted_repair.repaired_contract_json->>'selection_allowed', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on');

    GET DIAGNOSTICS v_repaired_authority_scope_count = ROW_COUNT;

    IF COALESCE(v_repaired_authority_scope_count, 0) > 0 THEN
      UPDATE public.banking_pay_workbench_candidate_line_work AS line_work
      SET result_row_json = ready_recovery_row.row_json,
          updated_at_utc = v_now
      FROM public.banking_pay_workbench_preview_rows AS ready_recovery_row
      WHERE line_work.session_id = p_session_id
        AND line_work.candidate_id = p_candidate_id
        AND ready_recovery_row.session_id = p_session_id
        AND ready_recovery_row.session_version = COALESCE(v_session_row.version, 1)
        AND ready_recovery_row.candidate_id = p_candidate_id
        AND ready_recovery_row.updated_at_utc = v_now
        AND LOWER(BTRIM(COALESCE(ready_recovery_row.section, ''))) = 'canonical_preview_lines'
        AND UPPER(BTRIM(COALESCE(ready_recovery_row.status, ''))) = 'READY'
        AND UPPER(BTRIM(COALESCE(ready_recovery_row.row_json->>'policy_x_authority_scope', ''))) = 'PRE_DRAFT_LIVE_TRUTH'
        AND line_work.line_key = ready_recovery_row.row_key;
    END IF;

    DROP TABLE IF EXISTS pg_temp._tmp_pay_wb_positive_headroom_recovery;
    CREATE TEMP TABLE _tmp_pay_wb_positive_headroom_recovery ON COMMIT DROP AS
    WITH gross_positive_by_channel AS (
      SELECT
        UPPER(BTRIM(COALESCE(
          preview_row.row_json->>'pay_channel',
          preview_row.row_json->>'current_pay_method',
          preview_row.row_json->>'candidate_pay_method',
          ''
        ))) AS pay_channel,
        ROUND(COALESCE(SUM(
          CASE
            WHEN COALESCE(preview_row.row_json->>'amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$'
              THEN GREATEST((preview_row.row_json->>'amount_ex_vat')::numeric, 0)
            ELSE 0::numeric
          END
        ), 0), 2)::numeric(12,2) AS positive_headroom_ex_vat
      FROM public.banking_pay_workbench_preview_rows AS preview_row
      WHERE preview_row.session_id = p_session_id
        AND preview_row.session_version = COALESCE(v_session_row.version, 1)
        AND preview_row.candidate_id = p_candidate_id
        AND LOWER(BTRIM(COALESCE(preview_row.section, ''))) = 'canonical_preview_lines'
        AND UPPER(BTRIM(COALESCE(preview_row.status, ''))) = 'READY'
        AND LOWER(BTRIM(COALESCE(preview_row.row_json->>'draftable', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        AND LOWER(BTRIM(COALESCE(preview_row.row_json->>'is_ready_for_draft', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        AND LOWER(BTRIM(COALESCE(preview_row.row_json->>'post_draft_overlay_applied', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
        AND (
          v_selection_intent_mode <> 'EXPLICIT_INCLUDE'
          OR (
            COALESCE(preview_row.selected, false) IS TRUE
            AND UPPER(BTRIM(COALESCE(preview_row.selection_state, ''))) = 'SELECTED'
          )
        )
        AND UPPER(BTRIM(COALESCE(preview_row.row_json->>'line_type', preview_row.row_json->>'item_type', ''))) NOT IN (
          'MANUAL_DEBT_RECOVERY',
          'OVERPAYMENT_RECOVERY',
          'LOAN_REPAYMENT',
          'PAYMENT_ADVANCE_REPAYMENT'
        )
      GROUP BY UPPER(BTRIM(COALESCE(
        preview_row.row_json->>'pay_channel',
        preview_row.row_json->>'current_pay_method',
        preview_row.row_json->>'candidate_pay_method',
        ''
      )))
    ), ready_recovery_by_channel AS (
      SELECT
        UPPER(BTRIM(COALESCE(
          ready_recovery_row.row_json->>'pay_channel',
          ready_recovery_row.row_json->>'current_pay_method',
          ready_recovery_row.row_json->>'candidate_pay_method',
          ''
        ))) AS pay_channel,
        ROUND(COALESCE(SUM(
          CASE
            WHEN COALESCE(ready_recovery_row.row_json->>'amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$'
              THEN ABS(LEAST((ready_recovery_row.row_json->>'amount_ex_vat')::numeric, 0))
            ELSE 0::numeric
          END
        ), 0), 2)::numeric(12,2) AS allocated_recovery_ex_vat
      FROM public.banking_pay_workbench_preview_rows AS ready_recovery_row
      WHERE ready_recovery_row.session_id = p_session_id
        AND ready_recovery_row.session_version = COALESCE(v_session_row.version, 1)
        AND ready_recovery_row.candidate_id = p_candidate_id
        AND LOWER(BTRIM(COALESCE(ready_recovery_row.section, ''))) = 'canonical_preview_lines'
        AND UPPER(BTRIM(COALESCE(ready_recovery_row.status, ''))) = 'READY'
        AND UPPER(BTRIM(COALESCE(ready_recovery_row.row_json->>'line_type', ready_recovery_row.row_json->>'item_type', ''))) IN (
          'MANUAL_DEBT_RECOVERY',
          'OVERPAYMENT_RECOVERY',
          'LOAN_REPAYMENT',
          'PAYMENT_ADVANCE_REPAYMENT'
        )
      GROUP BY UPPER(BTRIM(COALESCE(
        ready_recovery_row.row_json->>'pay_channel',
        ready_recovery_row.row_json->>'current_pay_method',
        ready_recovery_row.row_json->>'candidate_pay_method',
        ''
      )))
    ), positive_by_channel AS (
      SELECT
        gross_positive.pay_channel,
        ROUND(GREATEST(
          gross_positive.positive_headroom_ex_vat
            - COALESCE(ready_recovery.allocated_recovery_ex_vat, 0),
          0
        ), 2)::numeric(12,2) AS positive_headroom_ex_vat
      FROM gross_positive_by_channel AS gross_positive
      LEFT JOIN ready_recovery_by_channel AS ready_recovery
        ON ready_recovery.pay_channel = gross_positive.pay_channel
    ), blocked_recovery AS (
      SELECT
        blocked_row.id AS blocked_preview_row_id,
        blocked_row.candidate_id,
        blocked_row.row_key,
        blocked_row.row_ordinal,
        blocked_row.timesheet_id,
        blocked_row.key_type,
        blocked_row.key_value,
        blocked_row.row_json AS base_row_json,
        UPPER(BTRIM(COALESCE(
          blocked_row.row_json->>'pay_channel',
          blocked_row.row_json->>'current_pay_method',
          blocked_row.row_json->>'candidate_pay_method',
          ''
        ))) AS pay_channel,
        ROUND(
          CASE
            WHEN COALESCE((
              SELECT SUM(
                CASE
                  WHEN COALESCE(component_row.value->>'preview_due_amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$'
                    THEN ABS((component_row.value->>'preview_due_amount_ex_vat')::numeric)
                  ELSE 0::numeric
                END
              )
              FROM jsonb_array_elements(
                CASE
                  WHEN jsonb_typeof(blocked_row.row_json->'case_components') = 'array'
                    THEN blocked_row.row_json->'case_components'
                  ELSE '[]'::jsonb
                END
              ) AS component_row(value)
            ), 0) > 0
              THEN (
                SELECT SUM(ABS((component_row.value->>'preview_due_amount_ex_vat')::numeric))
                FROM jsonb_array_elements(blocked_row.row_json->'case_components') AS component_row(value)
                WHERE COALESCE(component_row.value->>'preview_due_amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$'
              )
            WHEN COALESCE(blocked_row.row_json->>'amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$'
             AND ABS((blocked_row.row_json->>'amount_ex_vat')::numeric) > 0
              THEN ABS((blocked_row.row_json->>'amount_ex_vat')::numeric)
            WHEN COALESCE(blocked_row.row_json#>>'{case_resolution_summary,due_amount_ex_vat}', '') ~ '^-?[0-9]+([.][0-9]+)?$'
              THEN ABS((blocked_row.row_json#>>'{case_resolution_summary,due_amount_ex_vat}')::numeric)
            WHEN COALESCE(blocked_row.row_json->>'nominal_due_amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$'
              THEN ABS((blocked_row.row_json->>'nominal_due_amount_ex_vat')::numeric)
            ELSE 0::numeric
          END,
          2
        )::numeric(12,2) AS nominal_due_amount_ex_vat
      FROM public.banking_pay_workbench_preview_rows AS blocked_row
      WHERE blocked_row.session_id = p_session_id
        AND blocked_row.session_version = COALESCE(v_session_row.version, 1)
        AND blocked_row.candidate_id = p_candidate_id
        AND LOWER(BTRIM(COALESCE(blocked_row.section, ''))) = 'blocked_for_pay'
        AND UPPER(BTRIM(COALESCE(blocked_row.status, ''))) = 'READY'
        AND UPPER(BTRIM(COALESCE(blocked_row.row_json->>'line_type', blocked_row.row_json->>'item_type', ''))) IN (
          'MANUAL_DEBT_RECOVERY',
          'OVERPAYMENT_RECOVERY',
          'LOAN_REPAYMENT',
          'PAYMENT_ADVANCE_REPAYMENT'
        )
        AND (
          UPPER(BTRIM(COALESCE(blocked_row.row_json->>'presentation_reason', ''))) = 'NO_PAY_HEADROOM'
          OR COALESCE(blocked_row.row_json->'blocked_reason_codes', '[]'::jsonb) @> jsonb_build_array('NO_PAY_HEADROOM')
        )
        AND LOWER(BTRIM(COALESCE(blocked_row.row_json->>'case_is_blocked', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
        AND LOWER(BTRIM(COALESCE(blocked_row.row_json->>'case_needs_resolution_now', 'false'))) NOT IN ('true', 't', '1', 'yes', 'y', 'on')
        AND (
          LOWER(BTRIM(COALESCE(blocked_row.row_json->>'case_resolution_satisfied_now', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
          OR UPPER(BTRIM(COALESCE(blocked_row.row_json->>'resolution_state', ''))) = 'RESOLVED'
          OR LOWER(BTRIM(COALESCE(blocked_row.row_json->>'is_case_resolution_satisfied', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        )
        AND NOT EXISTS (
          SELECT 1
          FROM jsonb_array_elements_text(
            CASE
              WHEN jsonb_typeof(blocked_row.row_json->'blocked_reason_codes') = 'array'
                THEN blocked_row.row_json->'blocked_reason_codes'
              ELSE '[]'::jsonb
            END
          ) AS blocked_code(value)
          WHERE UPPER(BTRIM(blocked_code.value)) <> 'NO_PAY_HEADROOM'
        )
    ), ranked_recovery AS (
      SELECT
        blocked_recovery.*,
        COALESCE(positive_by_channel.positive_headroom_ex_vat, 0)::numeric(12,2) AS positive_headroom_ex_vat,
        COALESCE(SUM(blocked_recovery.nominal_due_amount_ex_vat) OVER (
          PARTITION BY blocked_recovery.pay_channel
          ORDER BY blocked_recovery.row_ordinal, blocked_recovery.blocked_preview_row_id
          ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0)::numeric(12,2) AS prior_nominal_due_amount_ex_vat
      FROM blocked_recovery
      LEFT JOIN positive_by_channel
        ON positive_by_channel.pay_channel = blocked_recovery.pay_channel
      WHERE blocked_recovery.pay_channel IN ('PAYE', 'UMBRELLA')
        AND ROUND(COALESCE(blocked_recovery.nominal_due_amount_ex_vat, 0), 2) > 0
    ), allocated_recovery AS (
      SELECT
        ranked_recovery.*,
        ROUND(LEAST(
          ranked_recovery.nominal_due_amount_ex_vat,
          GREATEST(
            ranked_recovery.positive_headroom_ex_vat - ranked_recovery.prior_nominal_due_amount_ex_vat,
            0
          )
        ), 2)::numeric(12,2) AS recoverable_amount_ex_vat
      FROM ranked_recovery
    )
    SELECT
      allocated_recovery.*,
      COALESCE(existing_ready.id, gen_random_uuid()) AS promoted_preview_row_id,
      CASE
        WHEN UPPER(BTRIM(COALESCE(
          v_session_row.progress_json#>>'{selection_intent_v1,canonical_preview_lines,mode}',
          ''
        ))) = 'EXPLICIT_INCLUDE'
          OR COALESCE(v_session_row.server_selected_preview_row_ids_provided, false) IS TRUE
        THEN EXISTS (
          SELECT 1
          FROM jsonb_array_elements_text(
            CASE
              WHEN jsonb_typeof(COALESCE(v_session_row.server_selected_preview_row_ids, '[]'::jsonb)) = 'array'
                THEN COALESCE(v_session_row.server_selected_preview_row_ids, '[]'::jsonb)
              ELSE '[]'::jsonb
            END
          ) AS selected_id(value)
          WHERE BTRIM(selected_id.value) IN (
            allocated_recovery.blocked_preview_row_id::text,
            existing_ready.id::text
          )
        )
        ELSE true
      END AS promoted_selected
    FROM allocated_recovery
    LEFT JOIN public.banking_pay_workbench_preview_rows AS existing_ready
      ON existing_ready.session_id = p_session_id
     AND existing_ready.session_version = COALESCE(v_session_row.version, 1)
     AND existing_ready.candidate_id = p_candidate_id
     AND LOWER(BTRIM(COALESCE(existing_ready.section, ''))) = 'canonical_preview_lines'
     AND existing_ready.row_key = allocated_recovery.row_key
    WHERE allocated_recovery.recoverable_amount_ex_vat > 0;

    SELECT COUNT(*)::integer,
           COUNT(*) FILTER (WHERE promoted_selected)::integer
    INTO v_promoted_count,
         v_promoted_selected_count
    FROM pg_temp._tmp_pay_wb_positive_headroom_recovery;

    IF COALESCE(v_promoted_count, 0) > 0 THEN
      WITH promoted_payload AS (
        SELECT
          promotion.*,
          jsonb_strip_nulls(
            (
              promotion.base_row_json
              - 'blocked_reason_codes'
              - 'superseded_reason'
              - 'materialisation_recovery_headroom_revalidated'
              - 'materialisation_recovery_headroom_revalidated_at_utc'
            )
            || jsonb_build_object(
              'preview_row_id', promotion.promoted_preview_row_id::text,
              'line_id', promotion.promoted_preview_row_id::text,
              'section', 'canonical_preview_lines',
              'target_section', 'canonical_preview_lines',
              'readiness_state', 'READY',
              'presentation_section', 'READY_TO_PAY',
              'presentation_reason', NULL,
              'amount_ex_vat', -promotion.recoverable_amount_ex_vat,
              'preview_amount_ex_vat', -promotion.recoverable_amount_ex_vat,
              'amount_display', -promotion.recoverable_amount_ex_vat,
              'section_amount_ex_vat', -promotion.recoverable_amount_ex_vat,
              'section_amount_display', -promotion.recoverable_amount_ex_vat,
              'target_pay_ex_vat', -promotion.recoverable_amount_ex_vat,
              'ready_preview_amount_ex_vat', -promotion.recoverable_amount_ex_vat,
              'preview_component_amount_ex_vat', -promotion.recoverable_amount_ex_vat,
              'nominal_due_amount_ex_vat', promotion.nominal_due_amount_ex_vat,
              'recoverable_this_pay_run_ex_vat', promotion.recoverable_amount_ex_vat,
              'draftable', true,
              'is_ready_for_draft', true,
              'selection_allowed', true,
              'is_excluded_from_allocation', false,
              'selected', promotion.promoted_selected,
              'selection_state', CASE WHEN promotion.promoted_selected THEN 'SELECTED' ELSE 'UNSELECTED' END,
              'materialisation_recovery_headroom_revalidated', true,
              'materialisation_recovery_headroom_revalidated_at_utc', v_now::text,
              'retained_positive_headroom_ex_vat', promotion.positive_headroom_ex_vat,
              'policy_x_authority_scope', 'PRE_DRAFT_LIVE_TRUTH'
            )
            || jsonb_build_object(
              'case_resolution_summary', CASE
                WHEN jsonb_typeof(COALESCE(promotion.base_row_json->'case_resolution_summary', '{}'::jsonb)) = 'object'
                  THEN COALESCE(promotion.base_row_json->'case_resolution_summary', '{}'::jsonb)
                    || jsonb_build_object('due_amount_ex_vat', promotion.recoverable_amount_ex_vat)
                ELSE jsonb_build_object('due_amount_ex_vat', promotion.recoverable_amount_ex_vat)
              END
            )
          ) AS promoted_row_json
        FROM pg_temp._tmp_pay_wb_positive_headroom_recovery AS promotion
      ), contracted_payload AS (
        SELECT
          promoted_payload.*,
          public.pay_workbench_preview_line_contract_ok(
            p_line_json => promoted_payload.promoted_row_json,
            p_economic_key_json => COALESCE(promoted_payload.promoted_row_json->'economic_key', '{}'::jsonb),
            p_target_section => 'canonical_preview_lines'
          ) AS promoted_contract_json
        FROM promoted_payload
      )
      INSERT INTO public.banking_pay_workbench_preview_rows (
        id,
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
        contracted_payload.promoted_preview_row_id,
        p_session_id,
        p_candidate_id,
        'canonical_preview_lines',
        contracted_payload.row_key,
        contracted_payload.row_ordinal,
        contracted_payload.promoted_row_json
          || jsonb_build_object(
            'preview_contract', contracted_payload.promoted_contract_json,
            'selection_allowed', true
          ),
        contracted_payload.timesheet_id,
        contracted_payload.key_type,
        contracted_payload.key_value,
        contracted_payload.promoted_selected,
        CASE WHEN contracted_payload.promoted_selected THEN 'SELECTED' ELSE 'UNSELECTED' END,
        'READY',
        COALESCE(v_session_row.version, 1),
        v_now,
        v_now
      FROM contracted_payload
      WHERE LOWER(BTRIM(COALESCE(contracted_payload.promoted_contract_json->>'ok', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
        AND LOWER(BTRIM(COALESCE(contracted_payload.promoted_contract_json->>'selection_allowed', 'false'))) IN ('true', 't', '1', 'yes', 'y', 'on')
      ON CONFLICT (session_id, section, candidate_id, row_key)
      DO UPDATE
      SET row_ordinal = EXCLUDED.row_ordinal,
          row_json = EXCLUDED.row_json,
          timesheet_id = EXCLUDED.timesheet_id,
          key_type = EXCLUDED.key_type,
          key_value = EXCLUDED.key_value,
          selected = EXCLUDED.selected,
          selection_state = EXCLUDED.selection_state,
          status = 'READY',
          session_version = EXCLUDED.session_version,
          updated_at_utc = v_now;

      UPDATE public.banking_pay_workbench_preview_rows AS blocked_row
      SET status = 'SUPERSEDED',
          selected = false,
          selection_state = 'SUPERSEDED',
          row_json = COALESCE(blocked_row.row_json, '{}'::jsonb)
            || jsonb_build_object(
              'selected', false,
              'selection_state', 'SUPERSEDED',
              'superseded_reason', 'POSITIVE_PAY_HEADROOM_AVAILABLE_AFTER_FINAL_MATERIALISATION',
              'materialisation_recovery_headroom_revalidated', true,
              'materialisation_recovery_headroom_revalidated_at_utc', v_now::text,
              'policy_x_authority_scope', 'PRE_DRAFT_LIVE_WORKBENCH_ONLY'
            ),
          updated_at_utc = v_now
      WHERE blocked_row.id IN (
        SELECT promotion.blocked_preview_row_id
        FROM pg_temp._tmp_pay_wb_positive_headroom_recovery AS promotion
      );

      UPDATE public.banking_pay_workbench_candidate_line_work AS line_work
      SET result_row_json = ready_row.row_json,
          updated_at_utc = v_now
      FROM public.banking_pay_workbench_preview_rows AS ready_row
      WHERE line_work.session_id = p_session_id
        AND line_work.candidate_id = p_candidate_id
        AND ready_row.session_id = p_session_id
        AND ready_row.session_version = COALESCE(v_session_row.version, 1)
        AND ready_row.candidate_id = p_candidate_id
        AND LOWER(BTRIM(COALESCE(ready_row.section, ''))) = 'canonical_preview_lines'
        AND UPPER(BTRIM(COALESCE(ready_row.status, ''))) = 'READY'
        AND ready_row.row_key = line_work.line_key
        AND EXISTS (
          SELECT 1
          FROM pg_temp._tmp_pay_wb_positive_headroom_recovery AS promotion
          WHERE promotion.row_key = line_work.line_key
        );

      v_progress_json := public.pay_workbench_session_recompute_progress_counters(
        p_session_id,
        true,
        'POST_MATERIALISATION_RECOVERY_HEADROOM_PROMOTION',
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
              'last_recovery_headroom_revalidation_action', 'PROMOTED_RECOVERY_WITH_RETAINED_POSITIVE_PAY',
              'last_recovery_headroom_revalidation_candidate_id', p_candidate_id::text,
              'last_recovery_headroom_revalidation_promoted_count', COALESCE(v_promoted_count, 0),
              'policy_x_authority_scope', 'PRE_DRAFT_LIVE_WORKBENCH_ONLY'
            ),
          updated_at_utc = v_now
      WHERE session_row.id = p_session_id;

      RETURN jsonb_build_object(
        'ok', true,
        'action', 'PROMOTED_RECOVERY_WITH_RETAINED_POSITIVE_PAY',
        'session_id', p_session_id::text,
        'candidate_id', p_candidate_id::text,
        'retained_positive_headroom_ex_vat', v_retained_positive_headroom,
        'promoted_recovery_count', COALESCE(v_promoted_count, 0),
        'promoted_selected_count', COALESCE(v_promoted_selected_count, 0),
        'repaired_authority_scope_count', COALESCE(v_repaired_authority_scope_count, 0),
        'capped_ready_recovery_count', COALESCE(v_ready_recovery_capped_count, 0),
        'blocked_excess_recovery_count', COALESCE(v_ready_recovery_blocked_count, 0),
        'selected_row_count', COALESCE(v_selected_row_count, 0),
        'progress_recomputed', COALESCE(v_progress_json, '{}'::jsonb),
        'demoted_recovery_count', COALESCE(v_ready_recovery_blocked_count, 0),
        'policy_x_authority_scope', 'PRE_DRAFT_LIVE_WORKBENCH_ONLY',
        'post_draft_artifacts_touched', false,
        'payment_execution_started', false
      );
    END IF;

    IF COALESCE(v_ready_recovery_capped_count, 0) > 0
       OR COALESCE(v_ready_recovery_blocked_count, 0) > 0 THEN
      v_progress_json := public.pay_workbench_session_recompute_progress_counters(
        p_session_id,
        true,
        'POST_MATERIALISATION_RECOVERY_HEADROOM_CAP',
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
        AND COALESCE(selected_row.selected, false) IS TRUE
        AND UPPER(BTRIM(COALESCE(selected_row.selection_state, ''))) = 'SELECTED';

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
              'last_recovery_headroom_revalidation_action', 'CAPPED_READY_RECOVERY_TO_POSITIVE_HEADROOM',
              'last_recovery_headroom_revalidation_candidate_id', p_candidate_id::text,
              'last_recovery_headroom_revalidation_capped_count', COALESCE(v_ready_recovery_capped_count, 0),
              'last_recovery_headroom_revalidation_blocked_excess_count', COALESCE(v_ready_recovery_blocked_count, 0),
              'policy_x_authority_scope', 'PRE_DRAFT_LIVE_WORKBENCH_ONLY'
            ),
          updated_at_utc = v_now
      WHERE session_row.id = p_session_id;
    END IF;

    RETURN jsonb_build_object(
      'ok', true,
      'action', CASE
        WHEN COALESCE(v_repaired_authority_scope_count, 0) > 0
          THEN 'REPAIRED_EXISTING_RECOVERY_AUTHORITY_SCOPE'
        ELSE 'RETAINED_POSITIVE_PAY_PRESENT'
      END,
      'session_id', p_session_id::text,
      'candidate_id', p_candidate_id::text,
      'retained_positive_headroom_ex_vat', v_retained_positive_headroom,
      'promoted_recovery_count', 0,
      'repaired_authority_scope_count', COALESCE(v_repaired_authority_scope_count, 0),
      'capped_ready_recovery_count', COALESCE(v_ready_recovery_capped_count, 0),
      'blocked_excess_recovery_count', COALESCE(v_ready_recovery_blocked_count, 0),
      'selected_row_count', COALESCE(v_selected_row_count, 0),
      'progress_recomputed', COALESCE(v_progress_json, '{}'::jsonb),
      'demoted_recovery_count', COALESCE(v_ready_recovery_blocked_count, 0),
      'policy_x_authority_scope', 'PRE_DRAFT_LIVE_WORKBENCH_ONLY',
      'post_draft_artifacts_touched', false,
      'payment_execution_started', false
    );
  END IF;

  DROP TABLE IF EXISTS pg_temp._tmp_pay_wb_zero_headroom_recovery;
  CREATE TEMP TABLE _tmp_pay_wb_zero_headroom_recovery ON COMMIT DROP AS
  SELECT ready_row.id AS ready_preview_row_id,
         ready_row.row_key AS ready_row_key,
         ready_row.row_json->>'finance_case_id' AS finance_case_id_text,
         UPPER(BTRIM(COALESCE(ready_row.row_json->>'line_type', ready_row.row_json->>'item_type', ''))) AS line_type,
         ready_row.row_ordinal,
         ready_row.timesheet_id,
         ready_row.key_type,
         ready_row.key_value,
         ready_row.row_key AS blocked_row_key,
         COALESCE(blocked_template.row_json, ready_row.row_json, '{}'::jsonb) AS base_row_json,
         CASE
           WHEN COALESCE(ready_row.row_json->>'amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$'
            AND ABS((ready_row.row_json->>'amount_ex_vat')::numeric) > 0
             THEN ROUND(ABS((ready_row.row_json->>'amount_ex_vat')::numeric), 2)
           WHEN COALESCE((
             SELECT SUM(
               CASE
                 WHEN COALESCE(component_row.value->>'preview_due_amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$'
                   THEN ABS((component_row.value->>'preview_due_amount_ex_vat')::numeric)
                 ELSE 0::numeric
               END
             )
             FROM jsonb_array_elements(
               CASE
                 WHEN jsonb_typeof(ready_row.row_json->'case_components') = 'array'
                   THEN ready_row.row_json->'case_components'
                 ELSE '[]'::jsonb
               END
             ) AS component_row(value)
           ), 0) > 0
             THEN ROUND((
               SELECT SUM(ABS((component_row.value->>'preview_due_amount_ex_vat')::numeric))
               FROM jsonb_array_elements(ready_row.row_json->'case_components') AS component_row(value)
               WHERE COALESCE(component_row.value->>'preview_due_amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$'
             ), 2)
           WHEN COALESCE(ready_row.row_json->>'nominal_due_amount_ex_vat', '') ~ '^-?[0-9]+([.][0-9]+)?$'
             THEN ROUND(ABS((ready_row.row_json->>'nominal_due_amount_ex_vat')::numeric), 2)
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
      AND blocked_row.row_key = ready_row.row_key
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
  ORDER BY ready_row.row_ordinal,
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
      WHERE recovery_row.ready_preview_row_id = ready_recovery_row.id
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
      WHERE recovery_row.ready_row_key = line_work.line_key
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
SET plpgsql_check.mode TO 'disabled'
AS $function$
DECLARE
  v_drain_result jsonb := '{}'::jsonb;
  v_revalidation_results jsonb := '[]'::jsonb;
  v_revalidation_result jsonb := '{}'::jsonb;
  v_target record;
  v_target_count integer := 0;
  v_demoted_count integer := 0;
  v_scope_ensure_result jsonb := '{}'::jsonb;
  v_scope_reconcile_result jsonb := '{}'::jsonb;
BEGIN
  -- Continuous candidate-scope maintenance shares this aggregate drain RPC.
  -- Ensure never locks the global generation row and the bounded coordinator
  -- page runs before the established queue claim, so there is no RPC fan-out.
  v_scope_ensure_result := public.pay_workbench_scope_reconcile_ensure_v1();
  v_scope_reconcile_result := public.pay_workbench_scope_reconcile_drain_one_v1(
    25,
    COALESCE(NULLIF(BTRIM(p_worker_id), ''), 'WORKBENCH_REVALIDATED') || ':SCOPE'
  );

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
    'scope_reconcile_ensure', COALESCE(v_scope_ensure_result, '{}'::jsonb),
    'scope_reconcile_page', COALESCE(v_scope_reconcile_result, '{}'::jsonb),
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
