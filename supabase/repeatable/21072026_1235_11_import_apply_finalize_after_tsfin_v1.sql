-- ============================================================================
-- 05_import_apply_finalize_after_tsfin_v1.txt
--
-- Durable financialisation/finalisation state authority.
--
-- This function does not call lifecycle RPCs and does not directly authorise
-- timesheets. It:
--   - locks and verifies the operation and exact timesheet target set;
--   - verifies current TSFIN readiness and correction-pair completeness;
--   - returns canonical bulk-authorisation items when authorisation is needed;
--   - verifies completed authorisation on a later call;
--   - advances import_apply_operations safely and idempotently.
--
-- This design preserves no-RPC-fanout:
--   calculate/write TSFIN
--   -> call this function
--   -> if requested, backend calls the existing bulk authorise RPC once
--   -> call this function again to verify and mark COMPLETE.
--
-- Limits:
--   7 arguments.
--   Maximum timesheets: 100.
--   Response patch JSON: object, <= 128 KiB.
-- ============================================================================

CREATE OR REPLACE FUNCTION public.import_apply_finalize_after_tsfin_v1(
  p_operation_id uuid,
  p_actor_user_id uuid,
  p_expected_timesheet_ids uuid[],
  p_expected_preflight_fingerprint text DEFAULT NULL::text,
  p_response_patch_json jsonb DEFAULT '{}'::jsonb,
  p_now_utc timestamptz DEFAULT now(),
  p_max_timesheets integer DEFAULT 100
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
DECLARE
  v_now timestamptz := COALESCE(p_now_utc, now());
  v_expected_preflight_fingerprint text :=
    NULLIF(BTRIM(COALESCE(p_expected_preflight_fingerprint, '')), '');
  v_response_patch jsonb := COALESCE(p_response_patch_json, '{}'::jsonb);

  v_operation public.import_apply_operations%ROWTYPE;
  v_before_operation jsonb;
  v_expected_ids uuid[] := ARRAY[]::uuid[];
  v_raw_expected_count integer := 0;
  v_expected_count integer := 0;

  v_preflight jsonb;
  v_current_preflight_fingerprint text;
  v_member_count integer := 0;
  v_current_tsfin_count integer := 0;
  v_core_financial_ready_count integer := 0;
  v_financial_ready_count integer := 0;
  v_authorised_count integer := 0;
  v_unauthorised_count integer := 0;
  v_stale_count integer := 0;
  v_blocking_batch_count integer := 0;
  v_pair_scope_error_count integer := 0;
  v_pair_anchor_mismatch_count integer := 0;
  v_historical_anchor_mismatch_count integer := 0;

  v_authorisation_items jsonb := '[]'::jsonb;
  v_timesheet_rows jsonb := '[]'::jsonb;
  v_errors jsonb := '[]'::jsonb;

  v_new_state text;
  v_result_code text;
  v_continuation_required boolean := false;
  v_complete boolean := false;
  v_state_changed boolean := false;
BEGIN
  IF p_operation_id IS NULL THEN
    RAISE EXCEPTION 'IMPORT_FINALIZE_OPERATION_ID_REQUIRED'
      USING ERRCODE = '22023';
  END IF;

  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'IMPORT_FINALIZE_ACTOR_REQUIRED'
      USING ERRCODE = '22023';
  END IF;

  IF p_max_timesheets < 1 OR p_max_timesheets > 100 THEN
    RAISE EXCEPTION 'IMPORT_FINALIZE_MAX_TIMESHEETS_OUT_OF_RANGE'
      USING ERRCODE = '22023';
  END IF;

  IF jsonb_typeof(v_response_patch) <> 'object' THEN
    RAISE EXCEPTION 'IMPORT_FINALIZE_RESPONSE_PATCH_MUST_BE_OBJECT'
      USING ERRCODE = '22023';
  END IF;

  IF octet_length(v_response_patch::text) > 131072 THEN
    RAISE EXCEPTION 'IMPORT_FINALIZE_RESPONSE_PATCH_TOO_LARGE'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object(
              'max_bytes', 131072
            )::text;
  END IF;

  IF v_expected_preflight_fingerprint IS NOT NULL
     AND char_length(v_expected_preflight_fingerprint) > 256 THEN
    RAISE EXCEPTION 'IMPORT_FINALIZE_PREFLIGHT_FINGERPRINT_TOO_LONG'
      USING ERRCODE = '22023';
  END IF;

  v_raw_expected_count := COALESCE(cardinality(p_expected_timesheet_ids), 0);
  IF v_raw_expected_count < 1 OR v_raw_expected_count > p_max_timesheets THEN
    RAISE EXCEPTION 'IMPORT_FINALIZE_RAW_TIMESHEET_COUNT_OUT_OF_RANGE'
      USING ERRCODE = '22023';
  END IF;

  SELECT COALESCE(
    array_agg(DISTINCT expected_id ORDER BY expected_id),
    ARRAY[]::uuid[]
  )
  INTO v_expected_ids
  FROM unnest(COALESCE(
    p_expected_timesheet_ids,
    ARRAY[]::uuid[]
  )) AS expected_timesheet(expected_id)
  WHERE expected_id IS NOT NULL;

  v_expected_count := cardinality(v_expected_ids);

  IF v_expected_count < 1 OR v_expected_count > p_max_timesheets THEN
    RAISE EXCEPTION 'IMPORT_FINALIZE_TIMESHEET_COUNT_OUT_OF_RANGE'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object(
              'expected_count', v_expected_count,
              'min', 1,
              'max', p_max_timesheets
            )::text;
  END IF;

  SELECT operation_row.*
  INTO v_operation
  FROM public.import_apply_operations AS operation_row
  WHERE operation_row.id = p_operation_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'IMPORT_FINALIZE_OPERATION_NOT_FOUND'
      USING ERRCODE = 'P0002';
  END IF;

  IF v_operation.actor_user_id IS DISTINCT FROM p_actor_user_id THEN
    RAISE EXCEPTION 'IMPORT_FINALIZE_OPERATION_ACTOR_MISMATCH'
      USING ERRCODE = '42501';
  END IF;

  IF v_operation.state = 'COMPLETE' THEN
    IF COALESCE(v_operation.response_json -> 'expected_timesheet_ids','[]'::jsonb)
         IS DISTINCT FROM to_jsonb(v_expected_ids)
       OR (
         v_expected_preflight_fingerprint IS NOT NULL
         AND COALESCE(
           v_operation.response_json ->> 'preflight_fingerprint',
           v_operation.response_json ->> 'expected_preflight_fingerprint'
         ) IS DISTINCT FROM v_expected_preflight_fingerprint
       ) THEN
      RAISE EXCEPTION 'IMPORT_FINALIZE_COMPLETE_REPLAY_CONFLICT'
        USING ERRCODE = '40001',
              DETAIL = jsonb_build_object(
                'operation_id', p_operation_id,
                'expected_timesheet_ids', to_jsonb(v_expected_ids)
              )::text;
    END IF;
    RETURN jsonb_build_object(
      'ok', true,
      'replay', true,
      'complete', true,
      'continuation_required', false,
      'operation_id', p_operation_id::text,
      'state', v_operation.state,
      'result_code', 'IDEMPOTENT_REPLAY',
      'response_json', v_operation.response_json,
      'finalised_at_utc', v_operation.finalised_at_utc
    );
  END IF;

  IF v_operation.state NOT IN (
    'PREPARED',
    'SOURCE_COMMITTED_TSFIN_PENDING',
    'FINANCIALISED_PENDING_FINALISATION'
  ) THEN
    RAISE EXCEPTION 'IMPORT_FINALIZE_OPERATION_STATE_INVALID'
      USING ERRCODE = 'P0001',
            DETAIL = jsonb_build_object(
              'operation_id', p_operation_id::text,
              'state', v_operation.state
            )::text;
  END IF;

  v_before_operation := to_jsonb(v_operation);

  v_preflight := public.import_timesheet_financial_preflight_v1(
    v_expected_ids,
    'IMPORT_FINALIZE_AFTER_TSFIN',
    p_actor_user_id,
    '{}'::jsonb,
    true,
    p_max_timesheets
  );

  v_current_preflight_fingerprint :=
    v_preflight ->> 'preflight_fingerprint';

  IF v_expected_preflight_fingerprint IS NOT NULL
     AND v_current_preflight_fingerprint
         IS DISTINCT FROM v_expected_preflight_fingerprint THEN
    v_errors := v_errors || jsonb_build_array(
      jsonb_build_object(
        'code', 'IMPORT_FINALIZE_PREFLIGHT_FINGERPRINT_MISMATCH',
        'expected_preflight_fingerprint',
          v_expected_preflight_fingerprint,
        'actual_preflight_fingerprint',
          v_current_preflight_fingerprint
      )
    );
  END IF;


  IF jsonb_typeof(v_preflight -> 'errors') = 'array'
     AND jsonb_array_length(v_preflight -> 'errors') > 0 THEN
    v_errors := v_errors || (v_preflight -> 'errors');
  END IF;

  v_member_count :=
    COALESCE((v_preflight ->> 'member_count')::integer, 0);

  IF EXISTS (
    SELECT 1
    FROM unnest(v_expected_ids) AS expected_scope(timesheet_id)
    WHERE NOT EXISTS (
      SELECT 1
      FROM jsonb_array_elements_text(
        COALESCE(
          v_preflight -> 'member_timesheet_ids',
          '[]'::jsonb
        )
      ) AS actual_scope(timesheet_id_text)
      WHERE actual_scope.timesheet_id_text =
            expected_scope.timesheet_id::text
    )
  ) THEN
    v_errors := v_errors || jsonb_build_array(
      jsonb_build_object(
        'code', 'IMPORT_FINALIZE_EXPECTED_SCOPE_NOT_IN_PREFLIGHT',
        'expected_timesheet_ids', to_jsonb(v_expected_ids),
        'actual_member_timesheet_ids',
          COALESCE(
            v_preflight -> 'member_timesheet_ids',
            '[]'::jsonb
          )
      )
    );
  END IF;

  v_blocking_batch_count :=
    COALESCE(
      (v_preflight ->> 'blocking_batch_count')::integer,
      0
    );

  IF v_blocking_batch_count > 0 THEN
    v_errors := v_errors || jsonb_build_array(
      jsonb_build_object(
        'code', 'BLOCKED_ACTIVE_PAY_DRAFT',
        'blocking_batches',
          COALESCE(v_preflight -> 'blocking_batches', '[]'::jsonb)
      )
    );
  END IF;

  WITH current_state_raw AS (
    SELECT
      timesheet_row.timesheet_id,
      timesheet_row.booking_id,
      timesheet_row.version,
      timesheet_row.correction_id,
      timesheet_row.correction_kind,
      timesheet_row.parent_timesheet_id,
      timesheet_row.authorised_at_server,

      current_financial.id AS tsfin_id,
      current_financial.processing_status,
      current_financial.is_stale,
      current_financial.stale_reason,
      current_financial.candidate_id,
      current_financial.client_id,
      current_financial.pay_method,
      current_financial.has_rate_issue,
      current_financial.has_pay_channel_issue,
      current_financial.authorised_at_utc,
      current_financial.computed_at_utc,
      current_financial.policy_snapshot_json,
      current_financial.rate_source_refs_json,
      current_financial.pay_vat_rate_pct_snapshot,

      chain_scope.chain_json ->> 'root_timesheet_id'
        AS root_timesheet_id_text,
      COALESCE(
        (chain_scope.chain_json ->>
          'correction_financials_policy_envelope_required')::boolean,
        false
      ) AS correction_financials_policy_envelope_required,
      chain_scope.chain_json -> 'correction_financials_policy_envelope'
        AS expected_correction_financials_policy_envelope,
      NULLIF(
        BTRIM(COALESCE(
          chain_scope.chain_json ->>
            'correction_financials_policy_envelope_fingerprint',
          ''
        )),
        ''
      ) AS expected_correction_financials_policy_envelope_fingerprint,
      policy_leg.expected_leg AS expected_correction_policy_leg,
      policy_leg.expected_leg ->> 'leg_fingerprint'
        AS expected_correction_leg_fingerprint,
      policy_leg.expected_leg #>> '{tsfin_policy,tsfin_policy_fingerprint}'
        AS expected_correction_tsfin_policy_fingerprint,
      policy_leg.expected_leg #>> '{invoice_policy,invoice_policy_fingerprint}'
        AS expected_correction_invoice_policy_fingerprint,

      COALESCE(
        current_financial.policy_snapshot_json #>
          '{correction_financials_policy_envelope}',
        current_financial.rate_source_refs_json #>
          '{correction_financials_policy_envelope}'
      ) AS actual_correction_financials_policy_envelope,
      NULLIF(
        BTRIM(COALESCE(
          current_financial.policy_snapshot_json ->>
            'correction_financials_policy_envelope_fingerprint',
          current_financial.policy_snapshot_json #>>
            '{correction_financials_policy_envelope,envelope_fingerprint}',
          current_financial.rate_source_refs_json ->>
            'correction_financials_policy_envelope_fingerprint',
          current_financial.rate_source_refs_json #>>
            '{correction_financials_policy_envelope,envelope_fingerprint}',
          ''
        )),
        ''
      ) AS actual_correction_financials_policy_envelope_fingerprint,
      COALESCE(
        current_financial.policy_snapshot_json ->> 'correction_leg_fingerprint',
        current_financial.rate_source_refs_json ->> 'correction_leg_fingerprint'
      ) AS actual_correction_leg_fingerprint,
      COALESCE(
        current_financial.policy_snapshot_json ->> 'correction_tsfin_policy_fingerprint',
        current_financial.rate_source_refs_json ->> 'correction_tsfin_policy_fingerprint'
      ) AS actual_correction_tsfin_policy_fingerprint,
      COALESCE(
        current_financial.policy_snapshot_json ->> 'correction_invoice_policy_fingerprint',
        current_financial.rate_source_refs_json ->> 'correction_invoice_policy_fingerprint'
      ) AS actual_correction_invoice_policy_fingerprint,

      current_financial.policy_snapshot_json ->> 'erni_pct'
        AS actual_erni_pct_text,
      current_financial.policy_snapshot_json ->> 'apply_erni_to'
        AS actual_apply_erni_to,
      COALESCE(
        current_financial.pay_vat_rate_pct_snapshot::text,
        current_financial.policy_snapshot_json ->> 'vat_rate_pct'
      ) AS actual_pay_vat_rate_pct_text,

      v_preflight #>> ARRAY[
        'timesheet_signatures',
        timesheet_row.timesheet_id::text
      ] AS row_signature
    FROM public.timesheets AS timesheet_row
    LEFT JOIN LATERAL (
      SELECT financial_row.*
      FROM public.timesheets_financials AS financial_row
      WHERE financial_row.timesheet_id = timesheet_row.timesheet_id
        AND financial_row.is_current = true
      ORDER BY financial_row.computed_at_utc DESC, financial_row.id DESC
      LIMIT 1
    ) AS current_financial
      ON true
    LEFT JOIN LATERAL (
      SELECT public.timesheet_correction_chain_scope_v1(
        timesheet_row.timesheet_id, false, 32, p_max_timesheets
      ) AS chain_json
    ) AS chain_scope ON true
    LEFT JOIN LATERAL (
      SELECT public._ctms_correction_policy_leg_read_v1(
        timesheet_row.timesheet_id
      ) AS expected_leg
      WHERE COALESCE(
        (chain_scope.chain_json ->>
          'correction_financials_policy_envelope_required')::boolean,
        false
      ) = true
    ) AS policy_leg ON true
    WHERE timesheet_row.timesheet_id = ANY(v_expected_ids)
  ),
  current_state AS (
    SELECT
      raw_state.*,
      CASE
        WHEN jsonb_typeof(
          raw_state.actual_correction_financials_policy_envelope
        ) = 'object'
          THEN encode(
            extensions.digest(
              convert_to(
                (
                  raw_state.actual_correction_financials_policy_envelope
                  - 'envelope_fingerprint'
                )::text,
                'UTF8'
              ),
              'sha256'
            ),
            'hex'
          )
        ELSE NULL::text
      END AS recomputed_correction_financials_policy_envelope_fingerprint,
      CASE
        WHEN raw_state.actual_erni_pct_text ~
          '^[+-]?([0-9]+([.][0-9]+)?|[.][0-9]+)$'
          THEN raw_state.actual_erni_pct_text::numeric
        ELSE NULL::numeric
      END AS actual_erni_pct,
      CASE
        WHEN raw_state.actual_pay_vat_rate_pct_text ~
          '^[+-]?([0-9]+([.][0-9]+)?|[.][0-9]+)$'
          THEN raw_state.actual_pay_vat_rate_pct_text::numeric
        ELSE NULL::numeric
      END AS actual_pay_vat_rate_pct,
      CASE
        WHEN raw_state.expected_correction_policy_leg #>>
          '{tsfin_policy,erni_pct}' ~
          '^[+-]?([0-9]+([.][0-9]+)?|[.][0-9]+)$'
          THEN (
            raw_state.expected_correction_policy_leg #>>
              '{tsfin_policy,erni_pct}'
          )::numeric
        ELSE NULL::numeric
      END AS expected_erni_pct,
      raw_state.expected_correction_policy_leg #>>
        '{tsfin_policy,apply_erni_to}' AS expected_apply_erni_to,
      CASE
        WHEN raw_state.expected_correction_policy_leg #>>
          '{tsfin_policy,applied_pay_vat_rate_pct}' ~
          '^[+-]?([0-9]+([.][0-9]+)?|[.][0-9]+)$'
          THEN (
            raw_state.expected_correction_policy_leg #>>
              '{tsfin_policy,applied_pay_vat_rate_pct}'
          )::numeric
        ELSE NULL::numeric
      END AS expected_pay_vat_rate_pct
    FROM current_state_raw AS raw_state
  ),
  readiness AS (
    SELECT
      state_row.*,
      (
        state_row.tsfin_id IS NOT NULL
        AND COALESCE(state_row.is_stale, false) = false
        AND state_row.candidate_id IS NOT NULL
        AND state_row.client_id IS NOT NULL
        AND NULLIF(BTRIM(COALESCE(
          state_row.pay_method,
          ''
        )), '') IS NOT NULL
        AND COALESCE(state_row.has_rate_issue, false) = false
        AND COALESCE(
          state_row.has_pay_channel_issue,
          false
        ) = false
        AND state_row.processing_status NOT IN (
          'UNASSIGNED'::public.ts_fin_processing_status_enum,
          'CLIENT_UNRESOLVED'::public.ts_fin_processing_status_enum,
          'RATE_MISSING'::public.ts_fin_processing_status_enum,
          'PAY_CHANNEL_MISSING'::public.ts_fin_processing_status_enum
        )
      ) AS core_financial_ready,
      (
        NOT state_row.correction_financials_policy_envelope_required
        OR (
          state_row.expected_correction_financials_policy_envelope_fingerprint
            IS NOT NULL
          AND jsonb_typeof(
            state_row.expected_correction_financials_policy_envelope
          ) = 'object'
          AND jsonb_typeof(
            state_row.actual_correction_financials_policy_envelope
          ) = 'object'
          AND state_row.actual_correction_financials_policy_envelope =
              state_row.expected_correction_financials_policy_envelope
          AND state_row.actual_correction_financials_policy_envelope_fingerprint =
              state_row.expected_correction_financials_policy_envelope_fingerprint
          AND state_row.recomputed_correction_financials_policy_envelope_fingerprint =
              state_row.expected_correction_financials_policy_envelope_fingerprint
          AND state_row.actual_correction_leg_fingerprint =
              state_row.expected_correction_leg_fingerprint
          AND state_row.actual_correction_tsfin_policy_fingerprint =
              state_row.expected_correction_tsfin_policy_fingerprint
          AND state_row.actual_correction_invoice_policy_fingerprint =
              state_row.expected_correction_invoice_policy_fingerprint
          AND state_row.actual_erni_pct
              IS NOT DISTINCT FROM state_row.expected_erni_pct
          AND UPPER(BTRIM(COALESCE(
                state_row.actual_apply_erni_to,
                ''
              ))) =
              UPPER(BTRIM(COALESCE(
                state_row.expected_apply_erni_to,
                ''
              )))
          AND state_row.actual_pay_vat_rate_pct
              IS NOT DISTINCT FROM
                state_row.expected_pay_vat_rate_pct
        )
      ) AS correction_financials_policy_envelope_ready
    FROM current_state AS state_row
  ),
  correction_pair_rollup AS (
    SELECT
      state_row.expected_correction_financials_policy_envelope #>>
        '{operation,operation_id}' AS correction_operation_id,
      min(state_row.correction_id) AS correction_id,
      count(distinct state_row.correction_id)::integer AS correction_id_count,
      min(state_row.expected_correction_financials_policy_envelope ->>
        'correction_shape') AS expected_correction_shape,
      min((state_row.expected_correction_financials_policy_envelope ->>
        'expected_member_count')::integer) AS expected_member_count,
      count(*)::integer AS pair_member_count,
      count(*) FILTER (
        WHERE UPPER(BTRIM(COALESCE(
          state_row.correction_kind,
          ''
        ))) IN ('CHANGED_HOURS_REVERSAL','CANCELLATION_REVERSAL')
      )::integer AS reversal_count,
      count(*) FILTER (
        WHERE UPPER(BTRIM(COALESCE(
          state_row.correction_kind,
          ''
        ))) IN ('CHANGED_HOURS_REPLACEMENT','CANCELLATION_REPLACEMENT')
      )::integer AS replacement_count,
      count(DISTINCT state_row.parent_timesheet_id)::integer
        AS distinct_parent_count,
      count(DISTINCT
        state_row.expected_correction_financials_policy_envelope_fingerprint
      ) FILTER (
        WHERE state_row.correction_financials_policy_envelope_required
      )::integer AS distinct_expected_anchor_count,
      count(DISTINCT
        state_row.actual_correction_financials_policy_envelope_fingerprint
      ) FILTER (
        WHERE state_row.tsfin_id IS NOT NULL
      )::integer AS distinct_actual_anchor_count,
      count(*) FILTER (
        WHERE state_row.correction_financials_policy_envelope_required
          AND state_row.correction_financials_policy_envelope_ready
      )::integer AS matching_anchor_member_count,
      count(*) FILTER (
        WHERE state_row.correction_financials_policy_envelope_required
      )::integer AS required_anchor_member_count
    FROM readiness AS state_row
    WHERE state_row.correction_id IS NOT NULL
       OR state_row.correction_kind IS NOT NULL
    GROUP BY state_row.expected_correction_financials_policy_envelope #>>
      '{operation,operation_id}'
  )
  SELECT
    (SELECT count(*) FROM readiness)::integer,
    (SELECT count(*) FROM readiness WHERE tsfin_id IS NOT NULL)::integer,
    (SELECT count(*) FROM readiness WHERE core_financial_ready)::integer,
    (SELECT count(*) FROM readiness WHERE
      core_financial_ready
      AND correction_financials_policy_envelope_ready
    )::integer,
    (SELECT count(*) FROM readiness WHERE
      authorised_at_server IS NOT NULL
      AND authorised_at_utc IS NOT NULL
    )::integer,
    (SELECT count(*) FROM readiness WHERE
      authorised_at_server IS NULL
      AND authorised_at_utc IS NULL
    )::integer,
    (SELECT count(*) FROM readiness WHERE
      COALESCE(is_stale, false)
    )::integer,
    (SELECT count(*) FROM correction_pair_rollup WHERE
      correction_operation_id IS NULL
      OR correction_id_count <> 1
      OR expected_correction_shape NOT IN ('REVERSAL_ONLY','REVERSAL_REPLACEMENT')
      OR pair_member_count <> expected_member_count
      OR reversal_count <> 1
      OR replacement_count <> CASE
           WHEN expected_correction_shape='REVERSAL_ONLY' THEN 0 ELSE 1 END
      OR distinct_parent_count <> 1
    )::integer,
    (SELECT count(*) FROM correction_pair_rollup WHERE
      required_anchor_member_count <> pair_member_count
      OR distinct_expected_anchor_count <> 1
      OR distinct_actual_anchor_count <> 1
      OR matching_anchor_member_count <>
         required_anchor_member_count
    )::integer,
    (SELECT count(*) FROM readiness WHERE
      correction_financials_policy_envelope_required
      AND NOT correction_financials_policy_envelope_ready
    )::integer,
    COALESCE(
      (
        SELECT jsonb_agg(
          jsonb_strip_nulls(
            jsonb_build_object(
              'timesheet_id', state_row.timesheet_id::text,
              'booking_id', state_row.booking_id,
              'version', state_row.version,
              'root_timesheet_id',
                state_row.root_timesheet_id_text,
              'correction_id', state_row.correction_id,
              'correction_kind', state_row.correction_kind,
              'parent_timesheet_id', CASE
                WHEN state_row.parent_timesheet_id IS NULL THEN NULL
                ELSE state_row.parent_timesheet_id::text
              END,
              'tsfin_id', CASE
                WHEN state_row.tsfin_id IS NULL THEN NULL
                ELSE state_row.tsfin_id::text
              END,
              'processing_status', CASE
                WHEN state_row.processing_status IS NULL THEN NULL
                ELSE state_row.processing_status::text
              END,
              'is_stale', state_row.is_stale,
              'stale_reason', state_row.stale_reason,
              'timesheet_authorised',
                state_row.authorised_at_server IS NOT NULL,
              'tsfin_authorised',
                state_row.authorised_at_utc IS NOT NULL,
              'core_financial_ready',
                state_row.core_financial_ready,
              'correction_financials_policy_envelope_required',
                state_row.correction_financials_policy_envelope_required,
              'correction_financials_policy_envelope_ready',
                state_row.correction_financials_policy_envelope_ready,
              'expected_correction_financials_policy_envelope_fingerprint',
                state_row.expected_correction_financials_policy_envelope_fingerprint,
              'actual_correction_financials_policy_envelope_fingerprint',
                state_row.actual_correction_financials_policy_envelope_fingerprint,
              'recomputed_correction_financials_policy_envelope_fingerprint',
                state_row.recomputed_correction_financials_policy_envelope_fingerprint,
              'expected_erni_pct', state_row.expected_erni_pct,
              'actual_erni_pct', state_row.actual_erni_pct,
              'expected_apply_erni_to',
                state_row.expected_apply_erni_to,
              'actual_apply_erni_to',
                state_row.actual_apply_erni_to,
              'expected_pay_vat_rate_pct',
                state_row.expected_pay_vat_rate_pct,
              'actual_pay_vat_rate_pct',
                state_row.actual_pay_vat_rate_pct,
              'row_signature', state_row.row_signature
            )
          )
          ORDER BY state_row.timesheet_id
        )
        FROM readiness AS state_row
      ),
      '[]'::jsonb
    ),
    COALESCE(
      (
        SELECT jsonb_agg(
          jsonb_strip_nulls(
            jsonb_build_object(
              'timesheet_id', state_row.timesheet_id::text,
              'expected_timesheet_id',
                state_row.timesheet_id::text,
              'expected_row_signature',
                state_row.row_signature,
              'correction_financials_policy_envelope_required',
                state_row.correction_financials_policy_envelope_required,
              'expected_correction_financials_policy_envelope_fingerprint',
                state_row.expected_correction_financials_policy_envelope_fingerprint
            )
          )
          ORDER BY state_row.timesheet_id
        )
        FROM readiness AS state_row
        WHERE state_row.authorised_at_server IS NULL
          AND state_row.authorised_at_utc IS NULL
          AND state_row.core_financial_ready
          AND state_row.correction_financials_policy_envelope_ready
      ),
      '[]'::jsonb
    )
  INTO
    v_member_count,
    v_current_tsfin_count,
    v_core_financial_ready_count,
    v_financial_ready_count,
    v_authorised_count,
    v_unauthorised_count,
    v_stale_count,
    v_pair_scope_error_count,
    v_pair_anchor_mismatch_count,
    v_historical_anchor_mismatch_count,
    v_timesheet_rows,
    v_authorisation_items;

  IF v_member_count <> v_expected_count THEN
    v_errors := v_errors || jsonb_build_array(
      jsonb_build_object(
        'code', 'IMPORT_FINALIZE_TIMESHEET_SCOPE_MISMATCH',
        'expected_count', v_expected_count,
        'actual_count', v_member_count
      )
    );
  END IF;

  IF v_pair_scope_error_count > 0 THEN
    v_errors := v_errors || jsonb_build_array(
      jsonb_build_object(
        'code', 'IMPORT_FINALIZE_CORRECTION_PAIR_INCOMPLETE',
        'pair_error_count', v_pair_scope_error_count
      )
    );
  END IF;

  IF v_pair_anchor_mismatch_count > 0 THEN
    v_errors := v_errors || jsonb_build_array(
      jsonb_build_object(
        'code',
          'IMPORT_FINALIZE_CORRECTION_PAIR_HISTORICAL_ANCHOR_MISMATCH',
        'pair_anchor_mismatch_count',
          v_pair_anchor_mismatch_count
      )
    );
  END IF;

  IF v_historical_anchor_mismatch_count > 0 THEN
    v_errors := v_errors || jsonb_build_array(
      jsonb_build_object(
        'code',
          'IMPORT_FINALIZE_CORRECTION_FINANCIALS_POLICY_ENVELOPE_MISMATCH',
        'mismatching_timesheet_count',
          v_historical_anchor_mismatch_count
      )
    );
  END IF;

  IF jsonb_array_length(v_errors) > 0 THEN
    v_new_state := 'FINANCIALISED_PENDING_FINALISATION';
    v_result_code := CASE
      WHEN v_blocking_batch_count > 0
        THEN 'BLOCKED_ACTIVE_PAY_DRAFT'
      WHEN v_pair_anchor_mismatch_count > 0
        OR v_historical_anchor_mismatch_count > 0
        THEN 'BLOCKED_CORRECTION_FINANCIALS_POLICY_ENVELOPE_MISMATCH'
      ELSE 'FINANCIALISED_FINALISATION_BLOCKED'
    END;
    v_continuation_required := true;
    v_complete := false;
  ELSIF v_current_tsfin_count <> v_expected_count
     OR v_core_financial_ready_count <> v_expected_count
     OR v_stale_count > 0 THEN
    v_new_state := 'SOURCE_COMMITTED_TSFIN_PENDING';
    v_result_code := 'APPLIED_FINANCIALISATION_PENDING';
    v_continuation_required := true;
    v_complete := false;
  ELSIF v_financial_ready_count <> v_expected_count THEN
    v_new_state := 'FINANCIALISED_PENDING_FINALISATION';
    v_result_code := 'BLOCKED_CORRECTION_FINANCIALS_POLICY_ENVELOPE_MISMATCH';
    v_continuation_required := true;
    v_complete := false;
  ELSIF v_authorised_count = v_expected_count THEN
    v_new_state := 'COMPLETE';
    v_result_code := 'APPLIED_AND_FINANCIALISED';
    v_continuation_required := false;
    v_complete := true;
  ELSE
    v_new_state := 'FINANCIALISED_PENDING_FINALISATION';
    v_result_code := 'FINANCIALISED_AUTHORISATION_REQUIRED';
    v_continuation_required := true;
    v_complete := false;
  END IF;

  UPDATE public.import_apply_operations AS operation_row
  SET
    state = v_new_state,
    response_json =
      COALESCE(operation_row.response_json, '{}'::jsonb)
      || v_response_patch
      || jsonb_build_object(
        'operation_id', p_operation_id::text,
        'state', v_new_state,
        'result_code', v_result_code,
        'expected_timesheet_ids', to_jsonb(v_expected_ids),
        'preflight_fingerprint',
          v_current_preflight_fingerprint,
        'current_tsfin_count', v_current_tsfin_count,
        'core_financial_ready_count',
          v_core_financial_ready_count,
        'financial_ready_count', v_financial_ready_count,
        'correction_financials_policy_envelope_fingerprints',
          COALESCE(
            v_preflight ->
              'correction_financials_policy_envelope_fingerprints',
            '{}'::jsonb
          ),
        'authorised_count', v_authorised_count,
        'unauthorised_count', v_unauthorised_count,
        'continuation_required', v_continuation_required,
        'complete', v_complete,
        'errors', v_errors
      ),
    committed_at_utc = COALESCE(
      operation_row.committed_at_utc,
      v_now
    ),
    financialised_at_utc = CASE
      WHEN v_financial_ready_count = v_expected_count
       AND jsonb_array_length(v_errors) = 0
        THEN COALESCE(operation_row.financialised_at_utc, v_now)
      ELSE operation_row.financialised_at_utc
    END,
    finalised_at_utc = CASE
      WHEN v_complete
        THEN COALESCE(operation_row.finalised_at_utc, v_now)
      ELSE operation_row.finalised_at_utc
    END,
    updated_at_utc = v_now
  WHERE operation_row.id = p_operation_id
  RETURNING *
  INTO v_operation;

  v_state_changed :=
    v_before_operation ->> 'state'
      IS DISTINCT FROM v_operation.state;

  IF v_state_changed OR v_complete THEN
    PERFORM public._inv_write_audit(
      p_actor_user_id,
      CASE
        WHEN v_complete THEN 'IMPORT_FINANCIALISATION_COMPLETE'
        ELSE 'IMPORT_FINANCIALISATION_STATE_UPDATED'
      END,
      jsonb_build_object(
        'operation_id', p_operation_id::text,
        'state', v_operation.state,
        'result_code', v_result_code,
        'timesheet_ids', to_jsonb(v_expected_ids),
        'core_financial_ready_count',
          v_core_financial_ready_count,
        'financial_ready_count', v_financial_ready_count,
        'correction_financials_policy_envelope_fingerprints',
          COALESCE(
            v_preflight ->
              'correction_financials_policy_envelope_fingerprints',
            '{}'::jsonb
          ),
        'authorised_count', v_authorised_count,
        'errors', v_errors
      ),
      'import_apply_operation',
      p_operation_id::text,
      v_before_operation,
      'Import apply financialisation/finalisation verification',
      NULL::text,
      NULL::text,
      'import-operation:' || p_operation_id::text
    );
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'replay', false,
    'operation_id', p_operation_id::text,
    'state', v_operation.state,
    'result_code', v_result_code,
    'complete', v_complete,
    'continuation_required', v_continuation_required,
    'requires_authorisation',
      v_result_code = 'FINANCIALISED_AUTHORISATION_REQUIRED',
    'required_backend_rpc', CASE
      WHEN v_result_code = 'FINANCIALISED_AUTHORISATION_REQUIRED'
        THEN 'timesheet_authorise_bulk_atomic'
      ELSE NULL
    END,
    'expected_timesheet_ids', to_jsonb(v_expected_ids),
    'timesheets', v_timesheet_rows,
    'authorisation_items', v_authorisation_items,
    'preflight_fingerprint',
      v_current_preflight_fingerprint,
    'correction_financials_policy_envelopes',
      COALESCE(
        v_preflight -> 'correction_financials_policy_envelopes',
        '{}'::jsonb
      ),
    'correction_financials_policy_envelope_fingerprints',
      COALESCE(
        v_preflight ->
          'correction_financials_policy_envelope_fingerprints',
        '{}'::jsonb
      ),
    'current_tsfin_count', v_current_tsfin_count,
    'core_financial_ready_count',
      v_core_financial_ready_count,
    'financial_ready_count', v_financial_ready_count,
    'historical_anchor_mismatch_count',
      v_historical_anchor_mismatch_count,
    'pair_anchor_mismatch_count',
      v_pair_anchor_mismatch_count,
    'authorised_count', v_authorised_count,
    'unauthorised_count', v_unauthorised_count,
    'blocking_batches',
      COALESCE(v_preflight -> 'blocking_batches', '[]'::jsonb),
    'errors', v_errors,
    'response_json', v_operation.response_json,
    'committed_at_utc', v_operation.committed_at_utc,
    'financialised_at_utc', v_operation.financialised_at_utc,
    'finalised_at_utc', v_operation.finalised_at_utc
  );
END;
$function$;

COMMENT ON FUNCTION public.import_apply_finalize_after_tsfin_v1(
  uuid,
  uuid,
  uuid[],
  text,
  jsonb,
  timestamptz,
  integer
)
IS 'Verifies bounded import TSFIN readiness, exact root historical ERNI/VAT anchor fingerprints and complete correction pairs before returning bulk authorisation items or advancing durable import operation state idempotently.';

REVOKE ALL ON FUNCTION public.import_apply_finalize_after_tsfin_v1(
  uuid,
  uuid,
  uuid[],
  text,
  jsonb,
  timestamptz,
  integer
) FROM PUBLIC, anon, authenticated;

GRANT EXECUTE ON FUNCTION public.import_apply_finalize_after_tsfin_v1(
  uuid,
  uuid,
  uuid[],
  text,
  jsonb,
  timestamptz,
  integer
) TO service_role;
