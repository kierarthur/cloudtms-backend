-- Profile-2 reconciliation-only exact-input cache.
--
-- Correction residuals are evaluated once without a Workbench session and
-- once with the current Workbench session.  Their session-specific resolution
-- work must remain independent, but the live entitlement input for the same
-- canonical timesheet-id array is identical inside the same database
-- transaction.  This helper caches only the unmodified result of the existing
-- Policy X authority for that exact array and build token.

CREATE OR REPLACE FUNCTION private.pay_workbench_current_entitlement_components_cached_v1(
  p_timesheet_ids uuid[]
)
RETURNS TABLE(
  timesheet_id uuid,
  key_type text,
  key_value text,
  truth_ex_vat numeric,
  baseline_ex_vat numeric,
  truth_inc_vat numeric,
  baseline_inc_vat numeric
)
LANGUAGE plpgsql
VOLATILE
SECURITY INVOKER
SET search_path = ''
SET "plpgsql_check.mode" = 'disabled'
AS $function$
DECLARE
  v_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_sync_token_text text := NULL::text;
  v_sync_token uuid := NULL::uuid;
  v_cache_key text := NULL::text;
  v_cache_enabled boolean := false;
BEGIN
  SELECT COALESCE(
    ARRAY_AGG(DISTINCT input_id ORDER BY input_id),
    ARRAY[]::uuid[]
  )
  INTO v_timesheet_ids
  FROM UNNEST(COALESCE(p_timesheet_ids, ARRAY[]::uuid[])) AS input_rows(input_id)
  WHERE input_id IS NOT NULL;

  v_sync_token_text := NULLIF(BTRIM(COALESCE(
    current_setting('cloudtms.pay_workbench_overpayment_sync_token', true),
    ''
  )), '');

  IF COALESCE(
       current_setting('cloudtms.pay_workbench_execution_profile_version', true),
       ''
     ) = '2'
     AND v_sync_token_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    v_sync_token := v_sync_token_text::uuid;

    SELECT EXISTS (
      SELECT 1
      FROM private.banking_pay_workbench_economic_builds AS build_row
      WHERE build_row.build_token = v_sync_token
        AND build_row.status = 'RECONCILING'
    )
    INTO v_cache_enabled;
  END IF;

  IF NOT COALESCE(v_cache_enabled, false) THEN
    RETURN QUERY
    SELECT component_row.*
    FROM public._pay_current_timesheet_entitlement_components(
      v_timesheet_ids
    ) AS component_row;
    RETURN;
  END IF;

  v_cache_key := md5(array_to_string(v_timesheet_ids, ','));

  CREATE TEMPORARY TABLE IF NOT EXISTS pg_temp._bpay_wb_live_entitlement_cache_v2 (
    sync_token uuid NOT NULL,
    cache_key text NOT NULL,
    input_timesheet_ids uuid[] NOT NULL,
    timesheet_id uuid NOT NULL,
    key_type text NOT NULL,
    key_value text NOT NULL,
    truth_ex_vat numeric NOT NULL,
    baseline_ex_vat numeric NOT NULL,
    truth_inc_vat numeric NOT NULL,
    baseline_inc_vat numeric NOT NULL,
    PRIMARY KEY(sync_token, cache_key, timesheet_id, key_type, key_value)
  ) ON COMMIT DROP;

  CREATE TEMPORARY TABLE IF NOT EXISTS pg_temp._bpay_wb_live_entitlement_cache_keys_v2 (
    sync_token uuid NOT NULL,
    cache_key text NOT NULL,
    input_timesheet_ids uuid[] NOT NULL,
    PRIMARY KEY(sync_token, cache_key)
  ) ON COMMIT DROP;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_temp._bpay_wb_live_entitlement_cache_keys_v2 AS cache_key_row
    WHERE cache_key_row.sync_token = v_sync_token
      AND cache_key_row.cache_key = v_cache_key
      AND cache_key_row.input_timesheet_ids = v_timesheet_ids
  ) THEN
    INSERT INTO pg_temp._bpay_wb_live_entitlement_cache_v2(
      sync_token,
      cache_key,
      input_timesheet_ids,
      timesheet_id,
      key_type,
      key_value,
      truth_ex_vat,
      baseline_ex_vat,
      truth_inc_vat,
      baseline_inc_vat
    )
    SELECT
      v_sync_token,
      v_cache_key,
      v_timesheet_ids,
      component_row.timesheet_id,
      component_row.key_type,
      component_row.key_value,
      component_row.truth_ex_vat,
      component_row.baseline_ex_vat,
      component_row.truth_inc_vat,
      component_row.baseline_inc_vat
    FROM public._pay_current_timesheet_entitlement_components(
      v_timesheet_ids
    ) AS component_row;

    INSERT INTO pg_temp._bpay_wb_live_entitlement_cache_keys_v2(
      sync_token,
      cache_key,
      input_timesheet_ids
    )
    VALUES (
      v_sync_token,
      v_cache_key,
      v_timesheet_ids
    );
  END IF;

  RETURN QUERY
  SELECT
    cached_component.timesheet_id,
    cached_component.key_type,
    cached_component.key_value,
    cached_component.truth_ex_vat,
    cached_component.baseline_ex_vat,
    cached_component.truth_inc_vat,
    cached_component.baseline_inc_vat
  FROM pg_temp._bpay_wb_live_entitlement_cache_v2 AS cached_component
  WHERE cached_component.sync_token = v_sync_token
    AND cached_component.cache_key = v_cache_key
    AND cached_component.input_timesheet_ids = v_timesheet_ids
  ORDER BY
    cached_component.timesheet_id,
    cached_component.key_type,
    cached_component.key_value;
END;
$function$;

REVOKE ALL ON FUNCTION private.pay_workbench_current_entitlement_components_cached_v1(
  uuid[]
) FROM PUBLIC, anon, authenticated, service_role;

GRANT EXECUTE ON FUNCTION private.pay_workbench_current_entitlement_components_cached_v1(
  uuid[]
) TO postgres;

COMMENT ON FUNCTION private.pay_workbench_current_entitlement_components_cached_v1(
  uuid[]
) IS
  'Profile-2 transaction-local exact-input cache over the unchanged current-timesheet entitlement authority; no economic derivation or cross-input reuse.';
