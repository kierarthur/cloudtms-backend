-- Read-only, service-only presentation of the exact frozen payment units that
-- failed central freshness validation. This is deliberately not a second
-- freshness decision engine: status/reason/diff authority remains the existing
-- operation scope-unit validation result.

CREATE OR REPLACE FUNCTION public.pay_batch_freshness_user_failures_v1(
  p_operation_id uuid,
  p_pay_batch_id uuid,
  p_actor_user_id uuid DEFAULT NULL::uuid,
  p_limit integer DEFAULT 50
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path TO 'pg_catalog', 'public', 'extensions', 'pg_temp'
SET statement_timeout TO '2000ms'
AS $function$
DECLARE
  v_operation public.banking_pay_operations%ROWTYPE;
  v_limit integer := LEAST(GREATEST(COALESCE(p_limit, 50), 1), 100);
  v_actor_is_valid boolean := true;
  v_failure_count integer := 0;
  v_failures jsonb := '[]'::jsonb;
BEGIN
  IF p_operation_id IS NULL OR p_pay_batch_id IS NULL THEN
    RAISE EXCEPTION 'PAY_BATCH_FRESHNESS_USER_FAILURES_IDENTIFIERS_REQUIRED'
      USING ERRCODE = 'P0001',
            DETAIL = pg_catalog.jsonb_build_object(
              'code', 'PAY_BATCH_FRESHNESS_USER_FAILURES_IDENTIFIERS_REQUIRED'
            )::text;
  END IF;

  SELECT operation_row.*
  INTO v_operation
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.id = p_operation_id;

  IF NOT FOUND OR v_operation.pay_batch_id IS DISTINCT FROM p_pay_batch_id THEN
    RAISE EXCEPTION 'PAY_BATCH_FRESHNESS_USER_FAILURES_OPERATION_BATCH_MISMATCH'
      USING ERRCODE = 'P0001',
            DETAIL = pg_catalog.jsonb_build_object(
              'code', 'PAY_BATCH_FRESHNESS_USER_FAILURES_OPERATION_BATCH_MISMATCH',
              'operation_id', p_operation_id,
              'pay_batch_id', p_pay_batch_id
            )::text;
  END IF;

  IF p_actor_user_id IS NOT NULL THEN
    SELECT EXISTS (
      SELECT 1
      FROM public.tms_users AS actor_user
      WHERE actor_user.id = p_actor_user_id
        AND COALESCE(actor_user.is_active, false) = true
    )
    INTO v_actor_is_valid;

    IF COALESCE(v_actor_is_valid, false) IS NOT TRUE THEN
      RAISE EXCEPTION 'PAY_BATCH_FRESHNESS_USER_FAILURES_ACTOR_NOT_ALLOWED'
        USING ERRCODE = 'P0001',
              DETAIL = pg_catalog.jsonb_build_object(
                'code', 'PAY_BATCH_FRESHNESS_USER_FAILURES_ACTOR_NOT_ALLOWED',
                'actor_user_id', p_actor_user_id
              )::text;
    END IF;
  END IF;

  SELECT pg_catalog.count(*)::integer
  INTO v_failure_count
  FROM public.banking_pay_operation_scope_units AS failed_unit
  WHERE failed_unit.operation_id = p_operation_id
    AND failed_unit.pay_batch_id = p_pay_batch_id
    AND failed_unit.phase = 'FRESHNESS'
    AND failed_unit.unit_type = 'PAY_BATCH_ITEM'
    AND pg_catalog.upper(pg_catalog.btrim(COALESCE(failed_unit.status, ''))) IN ('STALE', 'ERROR');

  WITH failed_units AS (
    SELECT
      failed_unit.id,
      failed_unit.unit_ordinal,
      failed_unit.status,
      failed_unit.unit_payload_json,
      failed_unit.error_json,
      CASE
        WHEN COALESCE(failed_unit.unit_payload_json->>'pay_batch_candidate_id', '')
             ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN (failed_unit.unit_payload_json->>'pay_batch_candidate_id')::uuid
        ELSE NULL::uuid
      END AS pay_batch_candidate_id,
      CASE
        WHEN COALESCE(failed_unit.unit_payload_json->>'timesheet_id', '')
             ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          THEN (failed_unit.unit_payload_json->>'timesheet_id')::uuid
        ELSE NULL::uuid
      END AS timesheet_id
    FROM public.banking_pay_operation_scope_units AS failed_unit
    WHERE failed_unit.operation_id = p_operation_id
      AND failed_unit.pay_batch_id = p_pay_batch_id
      AND failed_unit.phase = 'FRESHNESS'
      AND failed_unit.unit_type = 'PAY_BATCH_ITEM'
      AND pg_catalog.upper(pg_catalog.btrim(COALESCE(failed_unit.status, ''))) IN ('STALE', 'ERROR')
    ORDER BY failed_unit.unit_ordinal, failed_unit.id
    LIMIT v_limit
  ), failure_rows AS (
    SELECT
      failed_unit.unit_ordinal,
      failed_unit.id,
      pg_catalog.jsonb_strip_nulls(pg_catalog.jsonb_build_object(
        'status', pg_catalog.upper(pg_catalog.btrim(COALESCE(failed_unit.status, ''))),
        'candidate_id', failed_unit.unit_payload_json->>'candidate_id',
        'candidate_tms_ref', batch_candidate.candidate_tms_ref,
        'candidate_display_name', batch_candidate.candidate_display_name,
        'client_name', timesheet_display.client_name,
        'week_ending_date', timesheet_display.week_ending_date,
        'payment_amount', payment_display.payment_amount,
        'payment_amount_pence', pg_catalog.round(payment_display.payment_amount * 100)::bigint,
        'currency', 'GBP',
        'pay_batch_candidate_id', failed_unit.unit_payload_json->>'pay_batch_candidate_id',
        'pay_batch_item_id', failed_unit.unit_payload_json->>'pay_batch_item_id',
        'timesheet_id', failed_unit.unit_payload_json->>'timesheet_id',
        'item_type', failed_unit.unit_payload_json->>'item_type',
        'description', failed_unit.unit_payload_json->>'description',
        'source_ref', failed_unit.unit_payload_json->>'source_ref',
        'pay_channel', failed_unit.unit_payload_json->>'pay_channel',
        'reason_codes', COALESCE(reason_values.reason_codes, '[]'::jsonb),
        'diff', CASE
          WHEN pg_catalog.jsonb_typeof(failed_unit.unit_payload_json #> '{validation_result,diff}') = 'object'
            THEN failed_unit.unit_payload_json #> '{validation_result,diff}'
          ELSE NULL::jsonb
        END
      )) AS failure_json
    FROM failed_units AS failed_unit
    LEFT JOIN public.pay_batch_candidates AS batch_candidate
      ON batch_candidate.id = failed_unit.pay_batch_candidate_id
     AND batch_candidate.pay_batch_id = p_pay_batch_id
    LEFT JOIN LATERAL (
      SELECT
        NULLIF(pg_catalog.btrim(COALESCE(timesheet_snapshot.target_snapshot_json->>'client_name', '')), '') AS client_name,
        NULLIF(pg_catalog.btrim(COALESCE(timesheet_snapshot.target_snapshot_json->>'week_ending_date', '')), '') AS week_ending_date
      FROM public.pay_batch_timesheet_snapshots AS timesheet_snapshot
      WHERE timesheet_snapshot.pay_batch_id = p_pay_batch_id
        AND timesheet_snapshot.timesheet_id = failed_unit.timesheet_id
      ORDER BY timesheet_snapshot.created_at_utc DESC, timesheet_snapshot.id DESC
      LIMIT 1
    ) AS timesheet_display ON true
    LEFT JOIN LATERAL (
      SELECT COALESCE(
        (
          SELECT member_row.active_amount
          FROM public.pay_payment_correction_request_candidates AS member_row
          JOIN public.pay_payment_correction_requests AS member_request
            ON member_request.id = member_row.correction_request_id
          WHERE member_request.pay_batch_id = p_pay_batch_id
            AND member_row.pay_batch_candidate_id = batch_candidate.id
          ORDER BY
            CASE WHEN member_request.status IN ('APPLIED', 'APPLIED_WITH_BLOCKERS') THEN 0 ELSE 1 END,
            member_request.updated_at_utc DESC,
            member_request.id DESC
          LIMIT 1
        ),
        CASE
          WHEN EXISTS (
            SELECT 1
            FROM public.pay_batch_items AS active_paye_item
            WHERE active_paye_item.pay_batch_candidate_id = batch_candidate.id
              AND COALESCE(active_paye_item.is_voided, false) IS NOT TRUE
              AND pg_catalog.upper(pg_catalog.btrim(COALESCE(active_paye_item.pay_channel, ''))) = 'PAYE'
          ) THEN
            COALESCE((
              SELECT paye_input.net_amount
              FROM public.pay_batch_paye_net_inputs AS paye_input
              WHERE paye_input.pay_batch_candidate_id = batch_candidate.id
              ORDER BY paye_input.imported_at_utc DESC, paye_input.id DESC
              LIMIT 1
            ), 0)
            + COALESCE((
              SELECT pg_catalog.sum(active_umbrella_item.amount_inc_vat)
              FROM public.pay_batch_items AS active_umbrella_item
              WHERE active_umbrella_item.pay_batch_candidate_id = batch_candidate.id
                AND COALESCE(active_umbrella_item.is_voided, false) IS NOT TRUE
                AND pg_catalog.upper(pg_catalog.btrim(COALESCE(active_umbrella_item.pay_channel, ''))) = 'UMBRELLA'
            ), 0)
          ELSE batch_candidate.net_bank_amount
        END,
        0
      )::numeric(14,2) AS payment_amount
    ) AS payment_display ON true
    LEFT JOIN LATERAL (
      SELECT COALESCE(pg_catalog.jsonb_agg(
        reason_set.reason_code ORDER BY reason_set.reason_code
      ), '[]'::jsonb) AS reason_codes
      FROM (
        SELECT DISTINCT NULLIF(pg_catalog.btrim(reason_text.reason_code), '') AS reason_code
        FROM (
          SELECT stale_reason.value AS reason_code
          FROM pg_catalog.jsonb_array_elements_text(
            CASE
              WHEN pg_catalog.jsonb_typeof(failed_unit.unit_payload_json #> '{validation_result,stale_reasons}') = 'array'
                THEN failed_unit.unit_payload_json #> '{validation_result,stale_reasons}'
              ELSE '[]'::jsonb
            END
          ) AS stale_reason(value)

          UNION ALL
          SELECT failed_unit.error_json->>'code'
          UNION ALL
          SELECT failed_unit.error_json->>'reason'
        ) AS reason_text(reason_code)
        WHERE NULLIF(pg_catalog.btrim(reason_text.reason_code), '') IS NOT NULL
      ) AS reason_set
    ) AS reason_values ON true
  )
  SELECT COALESCE(pg_catalog.jsonb_agg(
    failure_rows.failure_json ORDER BY failure_rows.unit_ordinal, failure_rows.id
  ), '[]'::jsonb)
  INTO v_failures
  FROM failure_rows;

  RETURN pg_catalog.jsonb_build_object(
    'ok', true,
    'contract_version', 'PAY_BATCH_FRESHNESS_USER_FAILURES_V1',
    'operation_id', p_operation_id,
    'pay_batch_id', p_pay_batch_id,
    'failure_count', COALESCE(v_failure_count, 0),
    'returned_count', pg_catalog.jsonb_array_length(COALESCE(v_failures, '[]'::jsonb)),
    'truncated', COALESCE(v_failure_count, 0) > v_limit,
    'all_failures_listed', COALESCE(v_failure_count, 0) <= v_limit,
    'failures', COALESCE(v_failures, '[]'::jsonb),
    'instruction_code', 'REMOVE_OR_RESOLVE_STALE_PAYMENTS_THEN_REAUTHORISE',
    'instruction', 'Remove or resolve every payment listed, then reauthorise the remaining batch. If these are the only freshness failures, reauthorisation can proceed.',
    'decision_authority', 'EXISTING_FRESHNESS_SCOPE_UNIT_RESULTS',
    'policy_x_economics_changed', false
  );
END;
$function$;

ALTER FUNCTION public.pay_batch_freshness_user_failures_v1(uuid, uuid, uuid, integer) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_batch_freshness_user_failures_v1(uuid, uuid, uuid, integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.pay_batch_freshness_user_failures_v1(uuid, uuid, uuid, integer) FROM anon;
REVOKE ALL ON FUNCTION public.pay_batch_freshness_user_failures_v1(uuid, uuid, uuid, integer) FROM authenticated;
REVOKE ALL ON FUNCTION public.pay_batch_freshness_user_failures_v1(uuid, uuid, uuid, integer) FROM service_role;
GRANT EXECUTE ON FUNCTION public.pay_batch_freshness_user_failures_v1(uuid, uuid, uuid, integer) TO service_role;
