-- Banking Pay bounded-scope Version 1.2.10.
-- Keep current-projection physical exhaustion distinct from whole-family work.

CREATE OR REPLACE FUNCTION private.pay_workbench_physical_source_continuation_v1(
  p_raw_source_has_more boolean,
  p_raw_source_exhausted boolean,
  p_another_projection_exists boolean
)
RETURNS boolean
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
SECURITY DEFINER
SET search_path = ''
AS $function$
DECLARE
  v_raw_has_more boolean:=COALESCE(p_raw_source_has_more,false);
  v_raw_exhausted boolean:=COALESCE(p_raw_source_exhausted,false);
  v_another_projection boolean:=COALESCE(p_another_projection_exists,false);
BEGIN
  IF v_raw_exhausted IS DISTINCT FROM NOT v_raw_has_more THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_PHYSICAL_SOURCE_EVIDENCE_INVALID'
      USING ERRCODE='23514';
  END IF;
  RETURN v_raw_has_more OR v_another_projection;
END;
$function$;

ALTER FUNCTION private.pay_workbench_physical_source_continuation_v1(
  boolean,boolean,boolean) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_physical_source_continuation_v1(
  boolean,boolean,boolean) FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION private.pay_workbench_physical_source_continuation_v1(
  boolean,boolean,boolean) TO postgres;
