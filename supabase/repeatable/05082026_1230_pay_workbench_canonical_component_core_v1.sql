-- Banking Pay bounded-scope Version 1.2.8.
-- Normalize an economic component independently of presentation enrichment.

CREATE OR REPLACE FUNCTION private.pay_workbench_canonical_component_core_v1(
  p_component_json jsonb
)
RETURNS jsonb
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
SECURITY DEFINER
SET search_path = ''
AS $function$
  SELECT CASE
    WHEN jsonb_typeof(COALESCE(p_component_json,'null'::jsonb))<>'object' THEN NULL::jsonb
    ELSE jsonb_strip_nulls(jsonb_build_object(
      'component_key_type',UPPER(NULLIF(BTRIM(p_component_json->>'component_key_type'),'')),
      'component_key_value',NULLIF(BTRIM(p_component_json->>'component_key_value'),''),
      'component_amount_ex_vat',CASE
        WHEN COALESCE(p_component_json->>'component_amount_ex_vat','') ~ '^-?\d+(\.\d+)?$'
          THEN to_jsonb(ROUND((p_component_json->>'component_amount_ex_vat')::numeric,2))
        ELSE NULL::jsonb END,
      'authoritative_truth_ex_vat',CASE
        WHEN COALESCE(p_component_json->>'authoritative_truth_ex_vat','') ~ '^-?\d+(\.\d+)?$'
          THEN to_jsonb(ROUND((p_component_json->>'authoritative_truth_ex_vat')::numeric,2))
        ELSE NULL::jsonb END,
      'authoritative_baseline_ex_vat',CASE
        WHEN COALESCE(p_component_json->>'authoritative_baseline_ex_vat','') ~ '^-?\d+(\.\d+)?$'
          THEN to_jsonb(ROUND((p_component_json->>'authoritative_baseline_ex_vat')::numeric,2))
        ELSE NULL::jsonb END,
      'authoritative_reserved_ex_vat',CASE
        WHEN COALESCE(p_component_json->>'authoritative_reserved_ex_vat','') ~ '^-?\d+(\.\d+)?$'
          THEN to_jsonb(ROUND((p_component_json->>'authoritative_reserved_ex_vat')::numeric,2))
        ELSE NULL::jsonb END,
      'authoritative_outstanding_ex_vat',CASE
        WHEN COALESCE(p_component_json->>'authoritative_outstanding_ex_vat','') ~ '^-?\d+(\.\d+)?$'
          THEN to_jsonb(ROUND((p_component_json->>'authoritative_outstanding_ex_vat')::numeric,2))
        ELSE NULL::jsonb END,
      'overpayment_component_authority',NULLIF(BTRIM(p_component_json->>'overpayment_component_authority'),''),
      'source_pay_method',UPPER(NULLIF(BTRIM(p_component_json->>'source_pay_method'),'')),
      'source_family_key',NULLIF(BTRIM(p_component_json->>'source_family_key'),''),
      'source_basis_identity',jsonb_strip_nulls(jsonb_build_object(
        'build_id',NULLIF(BTRIM(p_component_json#>>'{source_basis_json,build_id}'),''),
        'linked_timesheet_id',COALESCE(
          NULLIF(BTRIM(p_component_json#>>'{source_basis_json,linked_timesheet_id}'),''),
          NULLIF(BTRIM(p_component_json#>>'{source_basis_json,timesheet_id}'),'')),
        'component_key_type',UPPER(NULLIF(BTRIM(
          p_component_json#>>'{source_basis_json,component_key_type}'),'')),
        'component_key_value',NULLIF(BTRIM(
          p_component_json#>>'{source_basis_json,component_key_value}'),''),
        'authority',NULLIF(BTRIM(p_component_json#>>'{source_basis_json,authority}'),'')
      ))
    ))
  END;
$function$;

ALTER FUNCTION private.pay_workbench_canonical_component_core_v1(jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_canonical_component_core_v1(jsonb)
  FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION private.pay_workbench_canonical_component_core_v1(jsonb) TO postgres;
