-- Read-only presentation of the existing case-resolution button guards.
-- Oracle: unchanged buildCaseResolutionDisplayState/renderCaseActionButtons.
-- No resolution is saved, restored, cancelled or recalculated here. Actual
-- actions continue through their current authenticated mutation authorities.

\set ON_ERROR_STOP on

begin;

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_case_first_v2(p_values jsonb[], p_kind text)
RETURNS jsonb LANGUAGE plpgsql IMMUTABLE SECURITY INVOKER SET search_path TO ''
AS $function$
DECLARE v_value jsonb; v_objects jsonb;
BEGIN
  FOREACH v_value IN ARRAY p_values LOOP
    IF v_value IS NULL OR v_value='null'::jsonb THEN CONTINUE; END IF;
    CASE p_kind
      WHEN 'OBJECT' THEN IF jsonb_typeof(v_value)='object' THEN RETURN v_value; END IF;
      WHEN 'OBJECTS' THEN
        IF jsonb_typeof(v_value)='array' THEN
          SELECT jsonb_agg(value ORDER BY ord) INTO v_objects
          FROM jsonb_array_elements(v_value) WITH ORDINALITY AS a(value,ord)
          WHERE jsonb_typeof(value)='object';
          IF v_objects IS NOT NULL THEN RETURN v_objects; END IF;
        END IF;
      WHEN 'TEXT' THEN
        IF BTRIM(v_value #>> '{}')<>'' THEN RETURN to_jsonb(BTRIM(v_value #>> '{}')); END IF;
      WHEN 'DEFINED' THEN
        IF jsonb_typeof(v_value)<>'string' OR BTRIM(v_value #>> '{}')<>'' THEN RETURN v_value; END IF;
      ELSE RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_CASE_PROJECTION' USING ERRCODE='22023';
    END CASE;
  END LOOP;
  RETURN NULL;
END;
$function$;
ALTER FUNCTION private.pay_workbench_modal_case_first_v2(jsonb[],text) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_case_first_v2(jsonb[],text) FROM PUBLIC, anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_case_basis_number_v2(p_value jsonb)
RETURNS double precision LANGUAGE plpgsql IMMUTABLE SECURITY INVOKER SET search_path TO ''
AS $function$
DECLARE v_text text; v_number double precision;
BEGIN
  -- Mirror Number()/Number.isFinite for canonical source-unit/rate fields.
  -- These numbers only decide whether the existing manual-rate button exists;
  -- they never supply a payment display, recovery cap or saved resolution.
  IF p_value IS NULL THEN RETURN NULL; END IF;
  IF p_value='null'::jsonb OR p_value='false'::jsonb THEN RETURN 0; END IF;
  IF p_value='true'::jsonb THEN RETURN 1; END IF;
  IF jsonb_typeof(p_value) NOT IN ('string','number') THEN RETURN NULL; END IF;
  v_text := BTRIM(p_value #>> '{}');
  IF v_text='' THEN RETURN 0; END IF;
  IF v_text !~ '^[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)([eE][+-]?[0-9]+)?$' THEN RETURN NULL; END IF;
  BEGIN v_number:=v_text::double precision;
  EXCEPTION WHEN numeric_value_out_of_range OR invalid_text_representation THEN RETURN NULL; END;
  IF v_number IN ('Infinity'::double precision,'-Infinity'::double precision,'NaN'::double precision) THEN RETURN NULL; END IF;
  RETURN v_number;
END;
$function$;
ALTER FUNCTION private.pay_workbench_modal_case_basis_number_v2(jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_case_basis_number_v2(jsonb) FROM PUBLIC, anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_case_actionable_v2(p_components jsonb)
RETURNS boolean LANGUAGE plpgsql IMMUTABLE SECURITY INVOKER SET search_path TO ''
AS $function$
DECLARE v_component jsonb; v_explicit jsonb; v_payload jsonb; v_result jsonb;
  v_key text; v_bucket text; v_units double precision; v_rate double precision; v_charge double precision;
BEGIN
  FOR v_component IN SELECT value FROM jsonb_array_elements(
    CASE WHEN jsonb_typeof(p_components)='array' THEN p_components ELSE '[]'::jsonb END
  ) LOOP
    IF jsonb_typeof(v_component)<>'object' THEN CONTINUE; END IF;
    IF LOWER(BTRIM(COALESCE(private.pay_workbench_modal_case_first_v2(ARRAY[v_component->'requires_resolution',
      v_component->'needs_resolution',v_component->'resolution_required'],'DEFINED') #>> '{}','')))
      NOT IN ('true','1','yes','y','on') THEN CONTINUE; END IF;
    v_explicit:=private.pay_workbench_modal_case_first_v2(ARRAY[v_component->'is_actionable_resolution_row',
      v_component->'actionable_resolution_row',v_component->'is_actionable'],'DEFINED');
    IF v_explicit IS NOT NULL AND LOWER(BTRIM(v_explicit #>> '{}')) NOT IN ('true','1','yes','y','on') THEN CONTINUE; END IF;
    v_payload:=private.pay_workbench_modal_case_first_v2(ARRAY[v_component->'suggested_resolution_payload_json',
      v_component->'suggested_resolution_payload',v_component->'suggested_payload_json'],'OBJECT');
    v_result:=private.pay_workbench_modal_case_first_v2(ARRAY[v_component->'suggested_resolution_result_json',
      v_component->'suggested_resolution_result',v_component->'suggested_result_json'],'OBJECT');
    -- The legacy flag alone never grants this action; even an empty object is
    -- an explicitly supplied suggestion and satisfies the existing predicate.
    IF v_payload IS NOT NULL OR v_result IS NOT NULL THEN RETURN true; END IF;
    v_key:=UPPER(BTRIM(COALESCE(v_component->>'component_key_type','')));
    v_bucket:=UPPER(BTRIM(COALESCE(NULLIF(v_component->>'bucket_code',''),v_component#>>'{source_basis_json,bucket_code}','')));
    v_units:=private.pay_workbench_modal_case_basis_number_v2(v_component->'source_units');
    v_rate:=private.pay_workbench_modal_case_basis_number_v2(v_component->'source_rate');
    v_charge:=private.pay_workbench_modal_case_basis_number_v2(v_component->'source_charge_rate');
    IF v_key IN ('TS_DAY','TS_TOTAL','ADDITIONAL_CODE','PAY_CODE','ADDITIONAL_PAY_CODE','ADDITIONAL_UNIT','ADDITIONAL_UNITS')
      AND v_units IS NOT NULL AND ABS(v_units)>0.000000001 AND v_rate IS NOT NULL AND v_charge IS NOT NULL
      AND (v_key NOT IN ('TS_DAY','TS_TOTAL') OR v_bucket<>'') THEN RETURN true; END IF;
  END LOOP;
  RETURN false;
END;
$function$;
ALTER FUNCTION private.pay_workbench_modal_case_actionable_v2(jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_case_actionable_v2(jsonb) FROM PUBLIC, anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_case_meta_v2(p_row jsonb)
RETURNS jsonb LANGUAGE plpgsql IMMUTABLE SECURITY INVOKER SET search_path TO ''
AS $function$
DECLARE v_nested jsonb; v_raw jsonb; v_summary jsonb; v_line_summary jsonb;
  v_components jsonb; v_line_components jsonb[]; v_candidate text; v_case text; v_finance text;
  v_timesheet text; v_family text; v_needs jsonb; v_satisfied jsonb; v_excluded jsonb;
  v_line_raw jsonb; v_line_candidate text; v_line_case text; v_line_finance text; v_line_timesheet text;
BEGIN
  IF jsonb_typeof(p_row) IS DISTINCT FROM 'object' THEN RETURN NULL; END IF;
  v_nested:=private.pay_workbench_modal_case_first_v2(ARRAY[p_row->'case',p_row->'case_state',p_row->'state'],'OBJECT');
  v_raw:=private.pay_workbench_modal_case_first_v2(ARRAY[p_row->'raw_case',v_nested->'raw_case'],'OBJECT');
  IF jsonb_typeof(v_raw->'case')='object' THEN v_raw:=v_raw->'case'; END IF;
  v_summary:=private.pay_workbench_modal_case_first_v2(ARRAY[p_row->'case_resolution_summary',p_row->'caseResolutionSummary',
    v_nested->'case_resolution_summary',v_nested->'caseResolutionSummary',v_raw->'case_resolution_summary',v_raw->'caseResolutionSummary'],'OBJECT');
  v_candidate:=COALESCE(private.pay_workbench_modal_case_first_v2(ARRAY[p_row->'candidate_id',v_nested->'candidate_id',v_raw->'candidate_id',v_summary->'candidate_id'],'TEXT') #>> '{}','');
  v_case:=COALESCE(private.pay_workbench_modal_case_first_v2(ARRAY[p_row->'case_key',v_nested->'case_key',v_raw->'case_key',v_summary->'case_key'],'TEXT') #>> '{}','');
  v_finance:=COALESCE(private.pay_workbench_modal_case_first_v2(ARRAY[p_row->'finance_case_id',v_nested->'finance_case_id',v_raw->'finance_case_id',v_summary->'finance_case_id'],'TEXT') #>> '{}','');
  v_timesheet:=COALESCE(private.pay_workbench_modal_case_first_v2(ARRAY[p_row->'linked_timesheet_id',p_row->'timesheet_id',
    v_nested->'linked_timesheet_id',v_nested->'timesheet_id',v_raw->'linked_timesheet_id',v_raw->'timesheet_id',
    v_summary->'linked_timesheet_id',v_summary->'timesheet_id'],'TEXT') #>> '{}','');
  IF v_candidate='' OR (v_case='' AND v_finance='' AND v_timesheet='') THEN RETURN NULL; END IF;
  v_family:=UPPER(COALESCE(private.pay_workbench_modal_case_first_v2(ARRAY[p_row->'resolution_family',v_nested->'resolution_family',v_raw->'resolution_family',v_summary->'resolution_family'],'TEXT') #>> '{}',''));
  v_needs:=private.pay_workbench_modal_case_first_v2(ARRAY[p_row->'case_needs_resolution',v_nested->'case_needs_resolution',v_raw->'case_needs_resolution',v_summary->'case_needs_resolution',
    p_row->'needs_resolution',v_nested->'needs_resolution',v_raw->'needs_resolution',v_summary->'needs_resolution'],'DEFINED');
  v_satisfied:=private.pay_workbench_modal_case_first_v2(ARRAY[p_row->'case_resolution_satisfied_now',v_nested->'case_resolution_satisfied_now',v_raw->'case_resolution_satisfied_now',v_summary->'case_resolution_satisfied_now'],'DEFINED');
  v_excluded:=private.pay_workbench_modal_case_first_v2(ARRAY[p_row->'excluded_from_run',p_row->'exclude_from_run',v_nested->'excluded_from_run',v_nested->'exclude_from_run',
    v_raw->'excluded_from_run',v_raw->'exclude_from_run',v_summary->'excluded_from_run'],'DEFINED');
  v_components:=private.pay_workbench_modal_case_first_v2(ARRAY[
    p_row->'components',p_row->'case_components',p_row->'caseComponents',p_row->'component_states',p_row->'component_resolution_states',p_row->'preview_components',
    v_nested->'components',v_nested->'case_components',v_nested->'caseComponents',v_nested->'component_states',v_nested->'component_resolution_states',v_nested->'preview_components',
    v_raw->'components',v_raw->'case_components',v_raw->'caseComponents',v_raw->'component_states',v_raw->'component_resolution_states',v_raw->'preview_components'],'OBJECTS');

  -- Apply the canonical current preview-line overlay, including explicit empty
  -- component arrays. Falling back past an explicit empty array invents a button.
  v_line_summary:=private.pay_workbench_modal_case_first_v2(ARRAY[p_row->'case_resolution_summary',p_row->'caseResolutionSummary',
    p_row#>'{row_json,case_resolution_summary}',p_row#>'{row_json,caseResolutionSummary}',
    p_row#>'{rowJson,case_resolution_summary}',p_row#>'{rowJson,caseResolutionSummary}'],'OBJECT');
  -- A stored case-state envelope alone does not prove the current preview line
  -- joined that case. Preserve the legacy display join; unmatched rows must be
  -- surfaced as a contract problem by their caller, never given invented actions.
  v_line_raw:=private.pay_workbench_modal_case_first_v2(ARRAY[p_row->'raw_case',p_row->'case'],'OBJECT');
  IF jsonb_typeof(v_line_raw->'case')='object' THEN v_line_raw:=v_line_raw->'case'; END IF;
  v_line_candidate:=COALESCE(private.pay_workbench_modal_case_first_v2(ARRAY[p_row->'candidate_id',v_line_raw->'candidate_id',v_line_summary->'candidate_id'],'TEXT') #>> '{}','');
  v_line_case:=COALESCE(private.pay_workbench_modal_case_first_v2(ARRAY[p_row->'case_key',v_line_raw->'case_key',v_line_summary->'case_key'],'TEXT') #>> '{}','');
  v_line_finance:=COALESCE(private.pay_workbench_modal_case_first_v2(ARRAY[p_row->'finance_case_id',v_line_raw->'finance_case_id',v_line_summary->'finance_case_id'],'TEXT') #>> '{}','');
  v_line_timesheet:=COALESCE(private.pay_workbench_modal_case_first_v2(ARRAY[p_row->'linked_timesheet_id',p_row->'timesheet_id',
    v_line_raw->'linked_timesheet_id',v_line_raw->'timesheet_id',v_line_summary->'linked_timesheet_id',v_line_summary->'timesheet_id'],'TEXT') #>> '{}','');
  IF NOT ((v_line_case<>'' AND v_line_case=v_case) OR (v_line_finance<>'' AND v_line_finance=v_finance)
    OR (v_line_candidate=v_candidate AND v_line_timesheet<>'' AND v_line_timesheet=v_timesheet)) THEN RETURN NULL; END IF;
  IF v_line_summary IS NOT NULL THEN
    v_family:=UPPER(COALESCE(private.pay_workbench_modal_case_first_v2(ARRAY[p_row->'resolution_family',v_line_summary->'resolution_family',to_jsonb(v_family)],'TEXT') #>> '{}',''));
    v_needs:=private.pay_workbench_modal_case_first_v2(ARRAY[p_row->'case_needs_resolution',v_line_summary->'case_needs_resolution',v_needs],'DEFINED');
    v_satisfied:=private.pay_workbench_modal_case_first_v2(ARRAY[p_row->'case_resolution_satisfied_now',v_line_summary->'case_resolution_satisfied_now',v_satisfied],'DEFINED');
  END IF;
  v_line_components:=ARRAY[p_row->'case_components',p_row->'caseComponents',p_row#>'{row_json,case_components}',
    p_row#>'{row_json,caseComponents}',p_row#>'{rowJson,case_components}',p_row#>'{rowJson,caseComponents}'];
  IF EXISTS (SELECT 1 FROM unnest(v_line_components) AS a(value) WHERE jsonb_typeof(value)='array') THEN
    v_components:=private.pay_workbench_modal_case_first_v2(v_line_components,'OBJECTS');
  END IF;
  RETURN jsonb_build_object('candidate_id',v_candidate,'finance_case_id',v_finance,'case_key',v_case,'linked_timesheet_id',v_timesheet,
    'resolution_family',v_family,'case_needs_resolution',LOWER(BTRIM(COALESCE(v_needs #>> '{}',''))) IN ('true','1','yes','y','on'),
    'case_resolution_satisfied_now',LOWER(BTRIM(COALESCE(v_satisfied #>> '{}',''))) IN ('true','1','yes','y','on'),
    'excluded_from_run',LOWER(BTRIM(COALESCE(v_excluded #>> '{}',''))) IN ('true','1','yes','y','on'),
    'has_actionable_suggested_resolution',private.pay_workbench_modal_case_actionable_v2(v_components),
    'resolution_action_requires_actionable_components',v_line_summary IS NOT NULL AND v_family='BUCKETED',
    'components',COALESCE(v_components,'[]'::jsonb));
END;
$function$;
ALTER FUNCTION private.pay_workbench_modal_case_meta_v2(jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_case_meta_v2(jsonb) FROM PUBLIC, anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_case_actions_v2(p_meta jsonb)
RETURNS jsonb LANGUAGE plpgsql IMMUTABLE SECURITY INVOKER SET search_path TO ''
AS $function$
DECLARE v_actions jsonb:='[]'::jsonb;
BEGIN
  IF p_meta->'case_needs_resolution'='true'::jsonb AND p_meta->'case_resolution_satisfied_now'='false'::jsonb
    AND (p_meta->'resolution_action_requires_actionable_components'='false'::jsonb OR p_meta->'has_actionable_suggested_resolution'='true'::jsonb) THEN
    v_actions:=v_actions || jsonb_build_array(CASE p_meta->>'resolution_family'
      WHEN 'TAXABLE_CHANNEL_RESTRUCTURE' THEN 'banking:pay:openTaxableFinanceCaseRestructure'
      WHEN 'NON_BUCKET' THEN 'banking:pay:openNonBucketResolution'
      ELSE 'banking:pay:openBucketedResolution' END);
  END IF;
  IF BTRIM(COALESCE(p_meta->>'linked_timesheet_id',''))<>'' THEN
    v_actions:=v_actions || jsonb_build_array('banking:pay:toggleExcludeTimesheet');
  END IF;
  RETURN v_actions;
END;
$function$;
ALTER FUNCTION private.pay_workbench_modal_case_actions_v2(jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_case_actions_v2(jsonb) FROM PUBLIC, anon, authenticated, service_role;

commit;
