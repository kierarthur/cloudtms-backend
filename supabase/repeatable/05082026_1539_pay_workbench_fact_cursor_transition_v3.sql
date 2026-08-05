-- Banking Pay bounded-scope Version 1.2.10.
-- Produce the exact 25-member cursor used at every fact-family/unit boundary.
-- The producer and Version-2 consumer therefore share one executable contract.

CREATE OR REPLACE FUNCTION private.pay_workbench_fact_cursor_transition_v3(
  p_current_cursor jsonb,
  p_dependency_unit_key text,
  p_fact_family text,
  p_input_phase text,
  p_input_projection_id uuid DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
SECURITY DEFINER
SET search_path = ''
AS $function$
DECLARE
  v_current jsonb:=private.pay_workbench_fact_cursor_preserve_v2(p_current_cursor);
  v_unit_key text:=NULLIF(BTRIM(COALESCE(p_dependency_unit_key,'')),'');
  v_fact_family text:=UPPER(NULLIF(BTRIM(COALESCE(p_fact_family,'')),''));
  v_input_phase text:=UPPER(NULLIF(BTRIM(COALESCE(p_input_phase,'')),''));
  v_result jsonb;
BEGIN
  IF v_unit_key IS NULL OR v_fact_family IS NULL OR v_input_phase IS NULL
     OR v_input_phase NOT IN ('PHYSICAL_SOURCE','PROJECTION','COMPONENTS')
     OR (v_unit_key='GLOBAL' AND (
       v_input_phase<>'PHYSICAL_SOURCE' OR p_input_projection_id IS NOT NULL))
     OR (v_unit_key<>'GLOBAL' AND v_fact_family='LIVE_ENTITLEMENT_INPUT'
       AND v_input_phase<>'PROJECTION')
     OR (v_unit_key<>'GLOBAL' AND v_fact_family<>'LIVE_ENTITLEMENT_INPUT'
       AND v_input_phase<>'COMPONENTS') THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_FACT_CURSOR_TRANSITION_INVALID'
      USING ERRCODE='22023';
  END IF;

  v_result:=jsonb_build_object(
    'cursor_kind','WORKSPACE_FACT',
    'cursor_version',2,
    'build_id',v_current->'build_id',
    'candidate_id',v_current->'candidate_id',
    'captured_candidate_generation',v_current->'captured_candidate_generation',
    'captured_source_change_seq',v_current->'captured_source_change_seq',
    'dependency_unit_key',v_unit_key,
    'fact_family',v_fact_family,
    'page_number',1,
    'last_source_key',NULL,
    'previous_page_digest',NULL,
    'cumulative_fact_count',0,
    'cumulative_digest',md5('BPAY_FACT_STREAM_V2'),
    'terminal',false,
    'raw_physical_source_count',0,
    'resolved_physical_source_count',0,
    'failed_physical_source_count',0,
    'raw_physical_amount_ex_vat',0,
    'resolved_physical_amount_ex_vat',0,
    'last_raw_physical_source_key',NULL,
    'source_exhausted',false,
    'raw_terminal_source_key',NULL,
    'raw_page_evidence_digest',NULL,
    'input_phase',v_input_phase,
    'input_projection_id',p_input_projection_id);

  -- Both functions validate the exact member set.  This is the producer-side
  -- assertion that the generated cursor is directly consumable.
  RETURN private.pay_workbench_fact_cursor_preserve_v2(v_result);
END;
$function$;

ALTER FUNCTION private.pay_workbench_fact_cursor_transition_v3(
  jsonb,text,text,text,uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_fact_cursor_transition_v3(
  jsonb,text,text,text,uuid) FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION private.pay_workbench_fact_cursor_transition_v3(
  jsonb,text,text,text,uuid) TO postgres;
