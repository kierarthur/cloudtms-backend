-- ============================================================================
-- 05_import_timesheet_financial_preflight_v1.txt
-- New bounded, lock-aware import/correction and correction-policy-envelope preflight.
--
-- Limits:
--   6 arguments.
--   Input timesheets: 1..100.
--   Expanded correction scope: 1..100.
--   Expected-state JSON: object, <= 256 KiB.
--   Blocking batch sample: max 100.
--   Correction recursion delegated to function 04: max depth 32.
--
-- This function does not change timesheet, TSFIN, invoice, or payment economics.
-- It may acquire transaction-scoped advisory locks and row locks when requested.
-- ============================================================================

CREATE OR REPLACE FUNCTION public.import_timesheet_financial_preflight_v1(
  p_timesheet_ids uuid[],
  p_action text,
  p_actor_user_id uuid,
  p_expected_state_json jsonb DEFAULT '{}'::jsonb,
  p_lock_rows boolean DEFAULT true,
  p_max_scope integer DEFAULT 100
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
DECLARE
  v_action text := UPPER(BTRIM(COALESCE(p_action, '')));
  v_expected_state jsonb := COALESCE(p_expected_state_json, '{}'::jsonb);

  v_input_ids uuid[] := ARRAY[]::uuid[];
  v_root_ids uuid[] := ARRAY[]::uuid[];
  v_member_ids uuid[] := ARRAY[]::uuid[];

  v_raw_input_count integer := 0;
  v_input_count integer := 0;
  v_root_count integer := 0;
  v_member_count integer := 0;

  v_input_id uuid;
  v_root_id uuid;
  v_member_id uuid;
  v_member_text text;

  v_chain jsonb;
  v_chains_json jsonb := '[]'::jsonb;
  v_initial_chain_fingerprints jsonb := '{}'::jsonb;
  v_initial_anchor_fingerprints jsonb := '{}'::jsonb;
  v_correction_financials_policy_envelopes jsonb := '{}'::jsonb;
  v_correction_financials_policy_envelope_fingerprints jsonb := '{}'::jsonb;
  v_chain_fingerprint text;
  v_anchor_fingerprint text;
  v_expected_fingerprint text;
  v_expected_anchor_fingerprint text;

  v_timesheet_signatures jsonb := '{}'::jsonb;
  v_member_summaries jsonb := '[]'::jsonb;
  v_errors_json jsonb := '[]'::jsonb;
  v_blocking_batches_json jsonb := '[]'::jsonb;

  v_member_row record;
  v_signature_payload jsonb;
  v_timesheet_signature text;
  v_expected_timesheet_signature text;

  v_blocking_batch_count integer := 0;
  v_authorised_count integer := 0;
  v_processed_count integer := 0;
  v_paid_count integer := 0;
  v_invoice_lined_count integer := 0;
  v_stale_tsfin_count integer := 0;

  v_lock_acquired boolean := false;
  v_all_locks_acquired boolean := false;
  v_allowed boolean := false;
  v_required_path text;
  v_preflight_fingerprint text;
BEGIN
  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION 'IMPORT_PREFLIGHT_ACTOR_REQUIRED'
      USING ERRCODE = '22023';
  END IF;

  PERFORM 1
  FROM public.tms_users AS actor_row
  WHERE actor_row.id = p_actor_user_id
    AND COALESCE(actor_row.is_active, false) = true;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'IMPORT_PREFLIGHT_ACTOR_INVALID'
      USING ERRCODE = '42501',
            DETAIL = jsonb_build_object(
              'code', 'IMPORT_PREFLIGHT_ACTOR_INVALID',
              'actor_user_id', p_actor_user_id::text
            )::text;
  END IF;

  IF v_action = '' OR char_length(v_action) > 64 THEN
    RAISE EXCEPTION 'IMPORT_PREFLIGHT_ACTION_INVALID'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object(
              'code', 'IMPORT_PREFLIGHT_ACTION_INVALID',
              'max_characters', 64
            )::text;
  END IF;

  IF p_max_scope < 1 OR p_max_scope > 100 THEN
    RAISE EXCEPTION 'IMPORT_PREFLIGHT_MAX_SCOPE_OUT_OF_RANGE'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object(
              'code', 'IMPORT_PREFLIGHT_MAX_SCOPE_OUT_OF_RANGE',
              'min', 1,
              'max', 100,
              'supplied', p_max_scope
            )::text;
  END IF;

  IF jsonb_typeof(v_expected_state) <> 'object' THEN
    RAISE EXCEPTION 'IMPORT_PREFLIGHT_EXPECTED_STATE_MUST_BE_OBJECT'
      USING ERRCODE = '22023';
  END IF;

  IF octet_length(v_expected_state::text) > 262144 THEN
    RAISE EXCEPTION 'IMPORT_PREFLIGHT_EXPECTED_STATE_TOO_LARGE'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object(
              'code', 'IMPORT_PREFLIGHT_EXPECTED_STATE_TOO_LARGE',
              'max_bytes', 262144
            )::text;
  END IF;

  -- Enforce the limit before unnest/dedup so duplicate-heavy input cannot
  -- bypass the public RPC cardinality bound.
  v_raw_input_count := COALESCE(cardinality(p_timesheet_ids), 0);
  IF v_raw_input_count < 1 OR v_raw_input_count > 100 THEN
    RAISE EXCEPTION 'IMPORT_PREFLIGHT_RAW_INPUT_COUNT_OUT_OF_RANGE'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object(
              'code', 'IMPORT_PREFLIGHT_RAW_INPUT_COUNT_OUT_OF_RANGE',
              'min', 1, 'max', 100, 'supplied_raw_count', v_raw_input_count
            )::text;
  END IF;

  SELECT COALESCE(
    array_agg(DISTINCT supplied_id ORDER BY supplied_id),
    ARRAY[]::uuid[]
  )
  INTO v_input_ids
  FROM unnest(COALESCE(p_timesheet_ids, ARRAY[]::uuid[]))
    AS supplied_timesheet(supplied_id)
  WHERE supplied_id IS NOT NULL;

  v_input_count := cardinality(v_input_ids);

  IF v_input_count < 1 OR v_input_count > 100 THEN
    RAISE EXCEPTION 'IMPORT_PREFLIGHT_INPUT_COUNT_OUT_OF_RANGE'
      USING ERRCODE = '22023',
            DETAIL = jsonb_build_object(
              'code', 'IMPORT_PREFLIGHT_INPUT_COUNT_OUT_OF_RANGE',
              'min', 1,
              'max', 100,
              'supplied_distinct_count', v_input_count
            )::text;
  END IF;

  FOREACH v_input_id IN ARRAY v_input_ids LOOP
    v_chain := public.timesheet_correction_chain_scope_v1(
      v_input_id,
      false,
      32,
      100
    );

    IF COALESCE((v_chain ->> 'ok')::boolean, false) IS NOT TRUE THEN
      RAISE EXCEPTION 'IMPORT_PREFLIGHT_CHAIN_SCOPE_FAILED'
        USING ERRCODE = 'P0001',
              DETAIL = jsonb_build_object(
                'code', 'IMPORT_PREFLIGHT_CHAIN_SCOPE_FAILED',
                'timesheet_id', v_input_id::text
              )::text;
    END IF;

    v_root_id := NULLIF(v_chain ->> 'root_timesheet_id', '')::uuid;

    IF v_root_id IS NULL THEN
      RAISE EXCEPTION 'IMPORT_PREFLIGHT_CHAIN_ROOT_MISSING'
        USING ERRCODE = 'P0001';
    END IF;

    v_root_ids := array_append(v_root_ids, v_root_id);

    FOR v_member_text IN
      SELECT member_value
      FROM jsonb_array_elements_text(
        COALESCE(v_chain -> 'member_timesheet_ids', '[]'::jsonb)
      ) AS member_element(member_value)
    LOOP
      v_member_ids := array_append(v_member_ids, v_member_text::uuid);
    END LOOP;

    v_initial_chain_fingerprints :=
      v_initial_chain_fingerprints
      || jsonb_build_object(
        v_root_id::text,
        v_chain ->> 'chain_fingerprint'
      );

    v_initial_anchor_fingerprints :=
      v_initial_anchor_fingerprints
      || jsonb_build_object(
        v_root_id::text,
        v_chain ->> 'correction_financials_policy_envelope_fingerprint'
      );
  END LOOP;

  SELECT COALESCE(
    array_agg(DISTINCT root_id ORDER BY root_id),
    ARRAY[]::uuid[]
  )
  INTO v_root_ids
  FROM unnest(v_root_ids) AS root_scope(root_id);

  SELECT COALESCE(
    array_agg(DISTINCT member_id ORDER BY member_id),
    ARRAY[]::uuid[]
  )
  INTO v_member_ids
  FROM unnest(v_member_ids) AS member_scope(member_id);

  v_root_count := cardinality(v_root_ids);
  v_member_count := cardinality(v_member_ids);

  IF v_member_count < 1 OR v_member_count > p_max_scope THEN
    RAISE EXCEPTION 'IMPORT_PREFLIGHT_EXPANDED_SCOPE_OUT_OF_RANGE'
      USING ERRCODE = '54001',
            DETAIL = jsonb_build_object(
              'code', 'IMPORT_PREFLIGHT_EXPANDED_SCOPE_OUT_OF_RANGE',
              'expanded_member_count', v_member_count,
              'max_scope', p_max_scope
            )::text;
  END IF;

  IF COALESCE(p_lock_rows, true) THEN
    FOREACH v_root_id IN ARRAY v_root_ids LOOP
      v_lock_acquired := pg_try_advisory_xact_lock(
        hashtextextended(
          'TIMESHEET_CORRECTION_CHAIN|' || v_root_id::text,
          24062026
        )
      );

      IF NOT v_lock_acquired THEN
        RAISE EXCEPTION 'IMPORT_PREFLIGHT_LOCK_BUSY'
          USING ERRCODE = '55P03',
                DETAIL = jsonb_build_object(
                  'code', 'IMPORT_PREFLIGHT_LOCK_BUSY',
                  'root_timesheet_id', v_root_id::text,
                  'retryable', true
                )::text;
      END IF;
    END LOOP;

    PERFORM 1
    FROM public.timesheets AS lock_timesheet
    WHERE lock_timesheet.timesheet_id = ANY(v_member_ids)
    ORDER BY lock_timesheet.timesheet_id
    FOR UPDATE;

    PERFORM 1
    FROM public.timesheets_financials AS lock_financial
    WHERE lock_financial.timesheet_id = ANY(v_member_ids)
      AND lock_financial.is_current = true
    ORDER BY lock_financial.timesheet_id, lock_financial.id
    FOR UPDATE;

    v_all_locks_acquired := true;
  END IF;

  FOREACH v_root_id IN ARRAY v_root_ids LOOP
    v_chain := public.timesheet_correction_chain_scope_v1(
      v_root_id,
      false,
      32,
      100
    );

    v_chain_fingerprint := v_chain ->> 'chain_fingerprint';

    IF v_chain_fingerprint IS DISTINCT FROM
       (v_initial_chain_fingerprints ->> v_root_id::text) THEN
      v_errors_json := v_errors_json || jsonb_build_array(
        jsonb_build_object(
          'code', 'STALE_CORRECTION_CHAIN',
          'root_timesheet_id', v_root_id::text,
          'expected_fingerprint',
            v_initial_chain_fingerprints ->> v_root_id::text,
          'actual_fingerprint', v_chain_fingerprint
        )
      );
    END IF;


    v_anchor_fingerprint :=
      NULLIF(v_chain ->> 'correction_financials_policy_envelope_fingerprint', '');

    IF v_anchor_fingerprint IS DISTINCT FROM
       NULLIF(v_initial_anchor_fingerprints ->> v_root_id::text, '') THEN
      v_errors_json := v_errors_json || jsonb_build_array(
        jsonb_build_object(
          'code', 'STALE_CORRECTION_FINANCE_ANCHOR',
          'root_timesheet_id', v_root_id::text,
          'expected_anchor_fingerprint',
            NULLIF(v_initial_anchor_fingerprints ->> v_root_id::text, ''),
          'actual_anchor_fingerprint', v_anchor_fingerprint
        )
      );
    END IF;

    IF COALESCE(
         (v_chain ->> 'correction_financials_policy_envelope_required')::boolean,
         false
       )
       AND v_anchor_fingerprint IS NULL THEN
      v_errors_json := v_errors_json || jsonb_build_array(
        jsonb_build_object(
          'code', 'CORRECTION_FINANCE_ANCHOR_UNRESOLVED',
          'root_timesheet_id', v_root_id::text,
          'details', COALESCE(v_chain -> 'errors', '[]'::jsonb)
        )
      );
    END IF;

    v_expected_anchor_fingerprint := COALESCE(
      v_expected_state #>> ARRAY[
        'correction_financials_policy_envelope_fingerprints',
        v_root_id::text
      ],
      v_expected_state #>> ARRAY[
        'anchor_fingerprints',
        v_root_id::text
      ]
    );

    IF v_expected_anchor_fingerprint IS NOT NULL
       AND v_expected_anchor_fingerprint
             IS DISTINCT FROM v_anchor_fingerprint THEN
      v_errors_json := v_errors_json || jsonb_build_array(
        jsonb_build_object(
          'code', 'EXPECTED_CORRECTION_FINANCE_ANCHOR_MISMATCH',
          'root_timesheet_id', v_root_id::text,
          'expected_anchor_fingerprint', v_expected_anchor_fingerprint,
          'actual_anchor_fingerprint', v_anchor_fingerprint
        )
      );
    END IF;

    v_correction_financials_policy_envelope_fingerprints :=
      v_correction_financials_policy_envelope_fingerprints
      || jsonb_build_object(
        v_root_id::text,
        v_anchor_fingerprint
      );

    v_correction_financials_policy_envelopes :=
      v_correction_financials_policy_envelopes
      || jsonb_build_object(
        v_root_id::text,
        v_chain -> 'correction_financials_policy_envelope'
      );

    v_expected_fingerprint :=
      v_expected_state #>> ARRAY[
        'chain_fingerprints',
        v_root_id::text
      ];

    IF v_expected_fingerprint IS NOT NULL
       AND v_expected_fingerprint IS DISTINCT FROM v_chain_fingerprint THEN
      v_errors_json := v_errors_json || jsonb_build_array(
        jsonb_build_object(
          'code', 'EXPECTED_CORRECTION_CHAIN_FINGERPRINT_MISMATCH',
          'root_timesheet_id', v_root_id::text,
          'expected_fingerprint', v_expected_fingerprint,
          'actual_fingerprint', v_chain_fingerprint
        )
      );
    END IF;

    IF COALESCE((v_chain ->> 'valid')::boolean, false) IS NOT TRUE THEN
      v_errors_json := v_errors_json || jsonb_build_array(
        jsonb_build_object(
          'code', 'CORRECTION_CHAIN_INCOMPLETE',
          'root_timesheet_id', v_root_id::text,
          'details', COALESCE(v_chain -> 'errors', '[]'::jsonb)
        )
      );
    END IF;

    v_chains_json := v_chains_json || jsonb_build_array(v_chain);
  END LOOP;

  FOR v_member_row IN
    SELECT
      timesheet_row.timesheet_id,
      timesheet_row.booking_id,
      timesheet_row.version,
      timesheet_row.is_current,
      timesheet_row.parent_timesheet_id,
      timesheet_row.correction_id,
      timesheet_row.correction_kind,
      timesheet_row.adjustment_origin,
      timesheet_row.contract_id,
      timesheet_row.week_ending_date,
      timesheet_row.actual_schedule_json,
      timesheet_row.additional_units_week,
      timesheet_row.additional_units_per_day,
      timesheet_row.reference_number,
      timesheet_row.authorised_at_server,
      timesheet_row.status,
      timesheet_row.archived_at_utc,

      current_financial.id AS current_tsfin_id,
      current_financial.is_stale AS current_tsfin_is_stale,
      current_financial.processing_status,
      current_financial.processed_at_utc,
      current_financial.authorised_at_utc,
      current_financial.candidate_id,
      current_financial.client_id,
      current_financial.pay_method,
      current_financial.policy_snapshot_json,
      current_financial.rate_source_refs_json,
      current_financial.actual_schedule_json AS tsfin_actual_schedule_json,
      current_financial.total_hours,
      current_financial.total_pay_ex_vat,
      current_financial.total_charge_ex_vat,
      current_financial.pay_vat_rate_pct_snapshot,
      current_financial.pay_vat_amount_snapshot,
      current_financial.pay_total_inc_vat_snapshot,
      COALESCE(
        current_financial.policy_snapshot_json ->>
          'correction_financials_policy_envelope_fingerprint',
        current_financial.policy_snapshot_json #>>
          '{correction_financials_policy_envelope,envelope_fingerprint}',
        current_financial.rate_source_refs_json ->>
          'correction_financials_policy_envelope_fingerprint',
        current_financial.rate_source_refs_json #>>
          '{correction_financials_policy_envelope,envelope_fingerprint}'
      ) AS current_correction_financials_policy_envelope_fingerprint,
      current_financial.computed_at_utc,
      current_financial.locked_by_invoice_id,
      current_financial.paid_at_utc,

      EXISTS (
        SELECT 1
        FROM public.timesheets_financials AS paid_financial
        WHERE paid_financial.timesheet_id = timesheet_row.timesheet_id
          AND paid_financial.paid_at_utc IS NOT NULL
      )
      OR EXISTS (
        SELECT 1
        FROM public.pay_batch_items AS settled_item
        JOIN public.pay_batch_candidates AS settled_candidate
          ON settled_candidate.id = settled_item.pay_batch_candidate_id
        WHERE settled_item.timesheet_id = timesheet_row.timesheet_id
          AND COALESCE(settled_item.is_voided, false) = false
          AND (
            UPPER(BTRIM(COALESCE(
              settled_candidate.settlement_status,
              ''
            ))) = 'SETTLED'
            OR settled_candidate.settled_at_utc IS NOT NULL
          )
      ) AS has_paid_evidence,

      EXISTS (
        SELECT 1
        FROM public.invoice_lines AS invoice_line
        WHERE invoice_line.timesheet_id = timesheet_row.timesheet_id
      )
      OR current_financial.locked_by_invoice_id IS NOT NULL
      OR EXISTS (
        SELECT 1
        FROM public.nhsp_shifts AS invoice_shift
        WHERE invoice_shift.timesheet_id = timesheet_row.timesheet_id
          AND invoice_shift.invoice_id IS NOT NULL
      )
      OR EXISTS (
        SELECT 1
        FROM jsonb_array_elements(
          CASE
            WHEN jsonb_typeof(current_financial.invoice_breakdown_json) = 'array'
              THEN current_financial.invoice_breakdown_json
            WHEN jsonb_typeof(current_financial.invoice_breakdown_json) = 'object'
                 AND jsonb_typeof(current_financial.invoice_breakdown_json -> 'segments') = 'array'
              THEN current_financial.invoice_breakdown_json -> 'segments'
            ELSE '[]'::jsonb
          END
        ) AS invoice_segment(segment)
        WHERE NULLIF(BTRIM(COALESCE(
          invoice_segment.segment ->> 'invoice_locked_invoice_id',
          ''
        )), '') IS NOT NULL
      )
        AS has_invoice_evidence
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
    WHERE timesheet_row.timesheet_id = ANY(v_member_ids)
    ORDER BY timesheet_row.timesheet_id
  LOOP
    v_signature_payload := jsonb_strip_nulls(
      jsonb_build_object(
        'timesheet_id', v_member_row.timesheet_id::text,
        'booking_id', v_member_row.booking_id,
        'version', v_member_row.version,
        'is_current', v_member_row.is_current,
        'parent_timesheet_id', CASE
          WHEN v_member_row.parent_timesheet_id IS NULL THEN NULL
          ELSE v_member_row.parent_timesheet_id::text
        END,
        'correction_id', v_member_row.correction_id,
        'correction_kind', v_member_row.correction_kind,
        'adjustment_origin', v_member_row.adjustment_origin,
        'contract_id', CASE
          WHEN v_member_row.contract_id IS NULL THEN NULL
          ELSE v_member_row.contract_id::text
        END,
        'week_ending_date', v_member_row.week_ending_date,
        'actual_schedule_json', COALESCE(
          v_member_row.actual_schedule_json,
          '[]'::jsonb
        ),
        'additional_units_week', COALESCE(
          v_member_row.additional_units_week,
          '{}'::jsonb
        ),
        'additional_units_per_day', COALESCE(
          v_member_row.additional_units_per_day,
          '{}'::jsonb
        ),
        'reference_number', v_member_row.reference_number,
        'authorised_at_server', v_member_row.authorised_at_server,
        'status', v_member_row.status::text,
        'archived_at_utc', v_member_row.archived_at_utc,
        'current_tsfin_id', CASE
          WHEN v_member_row.current_tsfin_id IS NULL THEN NULL
          ELSE v_member_row.current_tsfin_id::text
        END,
        'current_tsfin_is_stale', v_member_row.current_tsfin_is_stale,
        'processing_status', CASE
          WHEN v_member_row.processing_status IS NULL THEN NULL
          ELSE v_member_row.processing_status::text
        END,
        'processed_at_utc', v_member_row.processed_at_utc,
        'tsfin_authorised_at_utc', v_member_row.authorised_at_utc,
        'candidate_id', CASE
          WHEN v_member_row.candidate_id IS NULL THEN NULL
          ELSE v_member_row.candidate_id::text
        END,
        'client_id', CASE
          WHEN v_member_row.client_id IS NULL THEN NULL
          ELSE v_member_row.client_id::text
        END,
        'pay_method', v_member_row.pay_method,
        'policy_snapshot_json', COALESCE(
          v_member_row.policy_snapshot_json,
          '{}'::jsonb
        ),
        'rate_source_refs_json', COALESCE(
          v_member_row.rate_source_refs_json,
          '{}'::jsonb
        ),
        'tsfin_actual_schedule_json', COALESCE(
          v_member_row.tsfin_actual_schedule_json,
          '[]'::jsonb
        ),
        'total_hours', v_member_row.total_hours,
        'total_pay_ex_vat', v_member_row.total_pay_ex_vat,
        'total_charge_ex_vat', v_member_row.total_charge_ex_vat,
        'pay_vat_rate_pct_snapshot',
          v_member_row.pay_vat_rate_pct_snapshot,
        'pay_vat_amount_snapshot',
          v_member_row.pay_vat_amount_snapshot,
        'pay_total_inc_vat_snapshot',
          v_member_row.pay_total_inc_vat_snapshot,
        'correction_financials_policy_envelope_fingerprint',
          v_member_row.current_correction_financials_policy_envelope_fingerprint,
        'computed_at_utc', v_member_row.computed_at_utc,
        'locked_by_invoice_id', CASE
          WHEN v_member_row.locked_by_invoice_id IS NULL THEN NULL
          ELSE v_member_row.locked_by_invoice_id::text
        END,
        'paid_at_utc', v_member_row.paid_at_utc
      )
    );

    v_timesheet_signature := encode(
      extensions.digest(
        convert_to(v_signature_payload::text, 'UTF8'),
        'sha256'
      ),
      'hex'
    );

    v_timesheet_signatures :=
      v_timesheet_signatures
      || jsonb_build_object(
        v_member_row.timesheet_id::text,
        v_timesheet_signature
      );

    v_expected_timesheet_signature :=
      v_expected_state #>> ARRAY[
        'timesheet_signatures',
        v_member_row.timesheet_id::text
      ];

    IF v_expected_timesheet_signature IS NOT NULL
       AND v_expected_timesheet_signature
           IS DISTINCT FROM v_timesheet_signature THEN
      v_errors_json := v_errors_json || jsonb_build_array(
        jsonb_build_object(
          'code', 'EXPECTED_TIMESHEET_SIGNATURE_MISMATCH',
          'timesheet_id', v_member_row.timesheet_id::text,
          'expected_signature', v_expected_timesheet_signature,
          'actual_signature', v_timesheet_signature
        )
      );
    END IF;

    IF v_member_row.authorised_at_server IS NOT NULL
       OR v_member_row.authorised_at_utc IS NOT NULL THEN
      v_authorised_count := v_authorised_count + 1;
    END IF;

    IF v_member_row.processed_at_utc IS NOT NULL
       OR UPPER(BTRIM(COALESCE(
         v_member_row.processing_status::text,
         ''
       ))) = 'PROCESSED' THEN
      v_processed_count := v_processed_count + 1;
    END IF;

    IF COALESCE(v_member_row.has_paid_evidence, false) THEN
      v_paid_count := v_paid_count + 1;
    END IF;

    IF COALESCE(v_member_row.has_invoice_evidence, false) THEN
      v_invoice_lined_count := v_invoice_lined_count + 1;
    END IF;

    IF COALESCE(v_member_row.current_tsfin_is_stale, false) THEN
      v_stale_tsfin_count := v_stale_tsfin_count + 1;
    END IF;

    v_member_summaries := v_member_summaries || jsonb_build_array(
      jsonb_strip_nulls(
        jsonb_build_object(
          'timesheet_id', v_member_row.timesheet_id::text,
          'timesheet_signature', v_timesheet_signature,
          'authorised',
            v_member_row.authorised_at_server IS NOT NULL
            OR v_member_row.authorised_at_utc IS NOT NULL,
          'processed',
            v_member_row.processed_at_utc IS NOT NULL
            OR UPPER(BTRIM(COALESCE(
              v_member_row.processing_status::text,
              ''
            ))) = 'PROCESSED',
          'paid', COALESCE(v_member_row.has_paid_evidence, false),
          'invoice_lined',
            COALESCE(v_member_row.has_invoice_evidence, false),
          'current_tsfin_id', CASE
            WHEN v_member_row.current_tsfin_id IS NULL THEN NULL
            ELSE v_member_row.current_tsfin_id::text
          END,
          'current_tsfin_is_stale',
            v_member_row.current_tsfin_is_stale,
          'candidate_id', CASE
            WHEN v_member_row.candidate_id IS NULL THEN NULL
            ELSE v_member_row.candidate_id::text
          END,
          'client_id', CASE
            WHEN v_member_row.client_id IS NULL THEN NULL
            ELSE v_member_row.client_id::text
          END,
          'pay_method', v_member_row.pay_method,
          'correction_financials_policy_envelope_fingerprint',
            v_member_row.current_correction_financials_policy_envelope_fingerprint
        )
      )
    );
  END LOOP;

  WITH blocking_source AS (
    SELECT
      batch_row.id AS pay_batch_id,
      batch_row.status,
      batch_row.pay_date,
      batch_row.bulk_reference,
      'PAY_BATCH_ITEM'::text AS blocker_source
    FROM public.pay_batch_items AS batch_item
    JOIN public.pay_batch_candidates AS batch_candidate
      ON batch_candidate.id = batch_item.pay_batch_candidate_id
    JOIN public.pay_batches AS batch_row
      ON batch_row.id = batch_candidate.pay_batch_id
    WHERE batch_item.timesheet_id = ANY(v_member_ids)
      AND COALESCE(batch_item.is_voided, false) = false
      AND public._pay_batch_status_is_active_reservation(batch_row.status)

    UNION ALL

    SELECT
      batch_row.id,
      batch_row.status,
      batch_row.pay_date,
      batch_row.bulk_reference,
      'PAY_BATCH_TIMESHEET_SNAPSHOT'
    FROM public.pay_batch_timesheet_snapshots AS batch_snapshot
    JOIN public.pay_batches AS batch_row
      ON batch_row.id = batch_snapshot.pay_batch_id
    WHERE batch_snapshot.timesheet_id = ANY(v_member_ids)
      AND public._pay_batch_status_is_active_reservation(batch_row.status)

    UNION ALL

    SELECT
      batch_row.id,
      batch_row.status,
      batch_row.pay_date,
      batch_row.bulk_reference,
      'PAYMENT_CORRECTION_ITEM'
    FROM public.pay_payment_correction_items AS correction_item
    JOIN public.pay_batches AS batch_row
      ON batch_row.id = correction_item.pay_batch_id
    WHERE correction_item.timesheet_id = ANY(v_member_ids)
      AND UPPER(BTRIM(COALESCE(correction_item.status, '')))
          = 'APPLIED'
      AND public._pay_batch_status_is_active_reservation(batch_row.status)

    UNION ALL

    SELECT
      batch_row.id,
      batch_row.status,
      batch_row.pay_date,
      batch_row.bulk_reference,
      'PAYMENT_OVERRIDE'
    FROM public.timesheet_payment_overrides AS payment_override
    JOIN public.pay_batches AS batch_row
      ON batch_row.id = payment_override.consumed_by_pay_batch_id
    WHERE payment_override.timesheet_id = ANY(v_member_ids)
      AND payment_override.cleared_at_utc IS NULL
      AND public._pay_batch_status_is_active_reservation(batch_row.status)

    UNION ALL

    SELECT
      batch_row.id,
      batch_row.status,
      batch_row.pay_date,
      batch_row.bulk_reference,
      'FINANCE_CASE_RESERVATION'
    FROM public.pay_advances AS finance_case
    JOIN public.pay_advance_reservations AS reservation_row
      ON reservation_row.finance_case_id = finance_case.id
    JOIN public.pay_batches AS batch_row
      ON batch_row.id = reservation_row.pay_batch_id
    WHERE finance_case.linked_timesheet_id = ANY(v_member_ids)
      AND UPPER(BTRIM(COALESCE(reservation_row.status, '')))
          IN ('RESERVED', 'COMMITTED')
      AND reservation_row.released_at_utc IS NULL
      AND reservation_row.settled_at_utc IS NULL
      AND public._pay_batch_status_is_active_reservation(batch_row.status)

    UNION ALL

    SELECT
      batch_row.id,
      batch_row.status,
      batch_row.pay_date,
      batch_row.bulk_reference,
      'FINANCE_COMPONENT_RESERVATION'
    FROM public.pay_finance_case_components AS finance_component
    JOIN public.pay_advance_reservations AS reservation_row
      ON reservation_row.finance_component_id = finance_component.id
    JOIN public.pay_batches AS batch_row
      ON batch_row.id = reservation_row.pay_batch_id
    WHERE finance_component.linked_timesheet_id = ANY(v_member_ids)
      AND UPPER(BTRIM(COALESCE(reservation_row.status, '')))
          IN ('RESERVED', 'COMMITTED')
      AND reservation_row.released_at_utc IS NULL
      AND reservation_row.settled_at_utc IS NULL
      AND public._pay_batch_status_is_active_reservation(batch_row.status)
  ),
  blocking_rollup AS (
    SELECT
      blocker.pay_batch_id,
      min(blocker.status) AS status,
      min(blocker.pay_date) AS pay_date,
      min(blocker.bulk_reference) AS bulk_reference,
      array_agg(DISTINCT blocker.blocker_source ORDER BY blocker.blocker_source)
        AS blocker_sources
    FROM blocking_source AS blocker
    GROUP BY blocker.pay_batch_id
  )
  SELECT
    count(*)::integer,
    COALESCE(
      (
        SELECT jsonb_agg(
          jsonb_build_object(
            'pay_batch_id', limited_blocker.pay_batch_id::text,
            'status', limited_blocker.status,
            'pay_date', limited_blocker.pay_date,
            'bulk_reference', limited_blocker.bulk_reference,
            'sources', to_jsonb(limited_blocker.blocker_sources)
          )
          ORDER BY limited_blocker.pay_date, limited_blocker.pay_batch_id
        )
        FROM (
          SELECT *
          FROM blocking_rollup
          ORDER BY pay_date, pay_batch_id
          LIMIT 100
        ) AS limited_blocker
      ),
      '[]'::jsonb
    )
  INTO v_blocking_batch_count, v_blocking_batches_json
  FROM blocking_rollup;

  IF v_blocking_batch_count > 100 THEN
    v_errors_json := v_errors_json || jsonb_build_array(
      jsonb_build_object(
        'code', 'IMPORT_PREFLIGHT_BLOCKER_LIMIT_EXCEEDED',
        'blocking_batch_count', v_blocking_batch_count,
        'max_reported', 100
      )
    );
  END IF;

  v_allowed :=
    v_blocking_batch_count = 0
    AND jsonb_array_length(v_errors_json) = 0;

  v_required_path := CASE
    WHEN v_blocking_batch_count > 0
      THEN 'BLOCKED_ACTIVE_PAY_DRAFT'
    WHEN jsonb_array_length(v_errors_json) > 0
      THEN 'BLOCKED_STALE_OR_INVALID_SCOPE'
    WHEN v_invoice_lined_count > 0
      THEN 'CREATE_OR_UPDATE_CORRECTION_CHAIN'
    WHEN v_paid_count > 0
      THEN 'PAID_UNINVOICED_ROLLOVER'
    WHEN v_authorised_count > 0
      THEN 'UNAUTHORISE_AMEND_RECALCULATE_REAUTHORISE'
    ELSE 'DIRECT_AMEND_RECALCULATE'
  END;

  v_preflight_fingerprint := encode(
    extensions.digest(
      convert_to(
        jsonb_build_object(
          'action', v_action,
          'input_timesheet_ids', to_jsonb(v_input_ids),
          'root_timesheet_ids', to_jsonb(v_root_ids),
          'member_timesheet_ids', to_jsonb(v_member_ids),
          'chain_fingerprints',
            COALESCE(
              (
                SELECT jsonb_object_agg(
                  chain_element.value ->> 'root_timesheet_id',
                  chain_element.value ->> 'chain_fingerprint'
                )
                FROM jsonb_array_elements(v_chains_json)
                  AS chain_element(value)
              ),
              '{}'::jsonb
            ),
          'correction_financials_policy_envelope_fingerprints',
            v_correction_financials_policy_envelope_fingerprints,
          'timesheet_signatures', v_timesheet_signatures,
          'blocking_batches', v_blocking_batches_json
        )::text,
        'UTF8'
      ),
      'sha256'
    ),
    'hex'
  );

  RETURN jsonb_build_object(
    'ok', true,
    'allowed', v_allowed,
    'action', v_action,
    'required_path', v_required_path,
    'locks_requested', COALESCE(p_lock_rows, true),
    'locks_acquired', v_all_locks_acquired,
    'raw_input_count', v_raw_input_count,
    'input_count', v_input_count,
    'root_count', v_root_count,
    'member_count', v_member_count,
    'input_timesheet_ids', to_jsonb(v_input_ids),
    'root_timesheet_ids', to_jsonb(v_root_ids),
    'member_timesheet_ids', to_jsonb(v_member_ids),
    'chains', v_chains_json,
    'correction_financials_policy_envelopes', v_correction_financials_policy_envelopes,
    'correction_financials_policy_envelope_fingerprints',
      v_correction_financials_policy_envelope_fingerprints,
    'members', v_member_summaries,
    'timesheet_signatures', v_timesheet_signatures,
    'authorised_count', v_authorised_count,
    'processed_count', v_processed_count,
    'paid_count', v_paid_count,
    'invoice_lined_count', v_invoice_lined_count,
    'stale_tsfin_count', v_stale_tsfin_count,
    'blocking_batch_count', v_blocking_batch_count,
    'blocking_batches', v_blocking_batches_json,
    'errors', v_errors_json,
    'preflight_fingerprint', v_preflight_fingerprint
  );
END;
$function$;

COMMENT ON FUNCTION public.import_timesheet_financial_preflight_v1(
  uuid[],
  text,
  uuid,
  jsonb,
  boolean,
  integer
)
IS 'Bounded import/correction preflight. Expands correction chains, locks deterministically, includes original historical ERNI/VAT anchor evidence in freshness, detects active pay drafts, and classifies lifecycle state without changing economics.';

REVOKE ALL ON FUNCTION public.import_timesheet_financial_preflight_v1(
  uuid[],
  text,
  uuid,
  jsonb,
  boolean,
  integer
) FROM PUBLIC, anon, authenticated;

GRANT EXECUTE ON FUNCTION public.import_timesheet_financial_preflight_v1(
  uuid[],
  text,
  uuid,
  jsonb,
  boolean,
  integer
) TO service_role;
