-- Compact presentation of the existing selection owner's complete movement
-- receipt. No identity, eligibility, amount or movement is calculated here.

\set ON_ERROR_STOP on

begin;

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_movement_envelope_v2(p_movements jsonb)
RETURNS jsonb LANGUAGE plpgsql IMMUTABLE SECURITY INVOKER SET search_path TO ''
AS $function$
DECLARE v_count bigint;v_distinct bigint;v_inline boolean;
BEGIN
  IF jsonb_typeof(p_movements) IS DISTINCT FROM 'array' THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_MOVEMENT' USING ERRCODE='22023';
  END IF;
  IF EXISTS(SELECT 1 FROM jsonb_array_elements(p_movements) m(value) WHERE
    jsonb_typeof(m.value) IS DISTINCT FROM 'object'
    OR COALESCE(m.value->>'identity','') !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    OR COALESCE(m.value->>'candidate_id','') !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    OR jsonb_typeof(m.value->'row_key') IS DISTINCT FROM 'string' OR NULLIF(m.value->>'row_key','') IS NULL
    OR jsonb_typeof(m.value->'from') IS DISTINCT FROM 'string' OR NULLIF(m.value->>'from','') IS NULL
    OR jsonb_typeof(m.value->'to') IS DISTINCT FROM 'string' OR NULLIF(m.value->>'to','') IS NULL
    OR m.value->>'from'=m.value->>'to' OR jsonb_typeof(m.value->'selected') IS DISTINCT FROM 'boolean') THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_MOVEMENT' USING ERRCODE='22023';
  END IF;
  SELECT count(*),count(DISTINCT lower(m.value->>'identity')) INTO v_count,v_distinct
    FROM jsonb_array_elements(p_movements) m(value);
  IF v_count<>v_distinct THEN RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_MOVEMENT' USING ERRCODE='22023';END IF;
  v_inline:=octet_length(convert_to(p_movements::text,'UTF8'))<=8192;
  -- The complete original array remains in the current server receipt. The
  -- false completeness flag below is mandatory: [] never means no movement
  -- when only a compact count/digest was returned. All prior detail caches
  -- must be discarded, not selectively patched from an incomplete array.
  RETURN jsonb_build_object('movements',CASE WHEN v_inline THEN p_movements ELSE '[]'::jsonb END,
    'movements_complete',v_inline,'movement_count',v_count,
    'movement_digest',encode(extensions.digest(convert_to(p_movements::text,'UTF8'),'sha256'),'hex'),
    'invalidations',jsonb_build_object('scope','ALL_PREVIOUS_DETAILS','ready',true,'actions',true,'updating',true,'blocked',true));
END;
$function$;
ALTER FUNCTION private.pay_workbench_modal_movement_envelope_v2(jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_movement_envelope_v2(jsonb) FROM PUBLIC, anon, authenticated, service_role;

commit;
