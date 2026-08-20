-- Pure validation for a published pre-Draft overpayment-recovery residual.
--
-- Policy X boundary:
--   * the publisher supplies the source/reservation/residual arithmetic;
--   * readers supply only a set-wise current reservation total and frozen-item
--     overlap fact;
--   * this helper reads no table and creates no economic authority.
CREATE OR REPLACE FUNCTION private.pay_workbench_preview_recovery_residual_is_current_v1(
  p_row_status text,
  p_row_candidate_id uuid,
  p_expected_candidate_id uuid,
  p_finance_case_id uuid,
  p_row_json jsonb,
  p_current_active_reserved_ex_vat numeric,
  p_current_active_reservation_count integer,
  p_exact_active_item_overlap boolean
) RETURNS boolean
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
SECURITY INVOKER
SET search_path TO ''
AS $function$
DECLARE
  v_source_outstanding_ex_vat numeric;
  v_embedded_active_reserved_ex_vat numeric;
  v_residual_outstanding_ex_vat numeric;
  v_nominal_due_amount_ex_vat numeric;
BEGIN
  IF pg_catalog.upper(pg_catalog.btrim(COALESCE(p_row_status, ''))) <> 'READY'
     OR p_row_candidate_id IS NULL
     OR p_expected_candidate_id IS NULL
     OR p_row_candidate_id <> p_expected_candidate_id
     OR p_finance_case_id IS NULL
     OR pg_catalog.jsonb_typeof(COALESCE(p_row_json, '{}'::jsonb)) <> 'object'
     OR pg_catalog.upper(pg_catalog.btrim(COALESCE(
          p_row_json->>'line_type',
          p_row_json#>>'{preview_contract,line_type}',
          ''
        ))) <> 'OVERPAYMENT_RECOVERY'
     OR pg_catalog.jsonb_typeof(p_row_json->'recovery_residual_contract_version') <> 'number'
     OR pg_catalog.jsonb_typeof(p_row_json->'recovery_source_outstanding_ex_vat') <> 'number'
     OR pg_catalog.jsonb_typeof(p_row_json->'recovery_active_reserved_ex_vat') <> 'number'
     OR pg_catalog.jsonb_typeof(p_row_json->'recovery_residual_outstanding_ex_vat') <> 'number'
     OR pg_catalog.jsonb_typeof(p_row_json->'nominal_due_amount_ex_vat') <> 'number'
     OR NULLIF(pg_catalog.btrim(COALESCE(p_row_json->>'finance_case_id', '')), '')
          !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
     OR COALESCE(p_current_active_reservation_count, 0) < 0
     OR p_current_active_reserved_ex_vat IS NULL
     OR p_current_active_reserved_ex_vat < 0
  THEN
    RETURN false;
  END IF;

  BEGIN
    IF (p_row_json->>'recovery_residual_contract_version')::numeric <> 1
       OR (p_row_json->>'finance_case_id')::uuid <> p_finance_case_id
    THEN
      RETURN false;
    END IF;

    v_source_outstanding_ex_vat := pg_catalog.round(
      (p_row_json->>'recovery_source_outstanding_ex_vat')::numeric,
      2
    );
    v_embedded_active_reserved_ex_vat := pg_catalog.round(
      (p_row_json->>'recovery_active_reserved_ex_vat')::numeric,
      2
    );
    v_residual_outstanding_ex_vat := pg_catalog.round(
      (p_row_json->>'recovery_residual_outstanding_ex_vat')::numeric,
      2
    );
    v_nominal_due_amount_ex_vat := pg_catalog.round(
      (p_row_json->>'nominal_due_amount_ex_vat')::numeric,
      2
    );
  EXCEPTION
    WHEN invalid_text_representation OR numeric_value_out_of_range THEN
      RETURN false;
  END;

  RETURN v_source_outstanding_ex_vat > 0
    AND v_embedded_active_reserved_ex_vat >= 0
    AND v_embedded_active_reserved_ex_vat < v_source_outstanding_ex_vat
    AND v_residual_outstanding_ex_vat > 0
    AND pg_catalog.round(
      greatest(
        v_source_outstanding_ex_vat - v_embedded_active_reserved_ex_vat,
        0
      ),
      2
    ) = v_residual_outstanding_ex_vat
    AND v_residual_outstanding_ex_vat = v_nominal_due_amount_ex_vat
    AND pg_catalog.round(p_current_active_reserved_ex_vat, 2)
      = v_embedded_active_reserved_ex_vat
    AND (
      v_embedded_active_reserved_ex_vat = 0
      OR COALESCE(p_current_active_reservation_count, 0) > 0
    )
    AND (
      COALESCE(p_exact_active_item_overlap, false) IS NOT TRUE
      OR v_embedded_active_reserved_ex_vat > 0
    );
EXCEPTION
  WHEN OTHERS THEN
    -- Reader safety: malformed published data must fail closed, never make the
    -- whole Workbench page RPC fail.
    RETURN false;
END;
$function$;

ALTER FUNCTION private.pay_workbench_preview_recovery_residual_is_current_v1(
  text,uuid,uuid,uuid,jsonb,numeric,integer,boolean
) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_preview_recovery_residual_is_current_v1(
  text,uuid,uuid,uuid,jsonb,numeric,integer,boolean
) FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION private.pay_workbench_preview_recovery_residual_is_current_v1(
  text,uuid,uuid,uuid,jsonb,numeric,integer,boolean
) TO postgres;
