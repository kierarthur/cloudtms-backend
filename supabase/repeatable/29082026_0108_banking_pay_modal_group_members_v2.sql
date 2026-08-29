-- Repeatable CloudTMS function/view authority: banking_pay_modal_group_members_v2
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

-- Exact JavaScript String(value) projection for the old `a || b` identity
-- fields. This is identity/display grouping only, never a financial scalar.
CREATE OR REPLACE FUNCTION private.pay_workbench_modal_js_string_v2(p_value jsonb)
RETURNS text LANGUAGE plpgsql IMMUTABLE SECURITY INVOKER SET search_path TO ''
AS $function$
DECLARE v_type text; v_result text;
BEGIN
  IF p_value IS NULL OR p_value='null'::jsonb THEN RETURN ''; END IF;
  v_type:=jsonb_typeof(p_value);
  IF v_type='string' THEN RETURN p_value #>> '{}'; END IF;
  IF v_type IN ('number','boolean') THEN RETURN p_value #>> '{}'; END IF;
  IF v_type='object' THEN RETURN '[object Object]'; END IF;
  IF v_type='array' THEN
    SELECT string_agg(private.pay_workbench_modal_js_string_v2(item.value),',' ORDER BY item.ordinality)
    INTO v_result FROM jsonb_array_elements(p_value) WITH ORDINALITY AS item(value,ordinality);
    RETURN COALESCE(v_result,'');
  END IF;
  RETURN '';
END;
$function$;
ALTER FUNCTION private.pay_workbench_modal_js_string_v2(jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_js_string_v2(jsonb) FROM PUBLIC, anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_js_first_truthy_text_v2(p_values jsonb[])
RETURNS text LANGUAGE plpgsql IMMUTABLE SECURITY INVOKER SET search_path TO ''
AS $function$
DECLARE v jsonb; v_type text;
BEGIN
  FOREACH v IN ARRAY p_values LOOP
    IF v IS NULL OR v='null'::jsonb THEN CONTINUE; END IF;
    v_type:=jsonb_typeof(v);
    IF v_type='boolean' AND v='false'::jsonb THEN CONTINUE; END IF;
    IF v_type='number' AND (v #>> '{}')::numeric=0 THEN CONTINUE; END IF;
    IF v_type='string' AND length(v #>> '{}')=0 THEN CONTINUE; END IF;
    RETURN BTRIM(private.pay_workbench_modal_js_string_v2(v));
  END LOOP;
  RETURN '';
END;
$function$;
ALTER FUNCTION private.pay_workbench_modal_js_first_truthy_text_v2(jsonb[]) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_js_first_truthy_text_v2(jsonb[]) FROM PUBLIC, anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_ready_group_key_v2(p_payload jsonb,p_kind text)
RETURNS text LANGUAGE plpgsql IMMUTABLE SECURITY INVOKER SET search_path TO ''
AS $function$
DECLARE v_nested jsonb:='{}'::jsonb;v_economic jsonb:='{}'::jsonb;
 v_candidate text;v_timesheet text;v_finance text;v_parent text;v_line_type text;
BEGIN
  IF jsonb_typeof(p_payload)<>'object' OR p_kind NOT IN ('TIMESHEET','OVERPAYMENT') THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_GROUP_INPUT_INVALID' USING ERRCODE='22023';
  END IF;
  v_nested:=CASE WHEN jsonb_typeof(p_payload->'row_json')='object' THEN p_payload->'row_json'
    WHEN jsonb_typeof(p_payload->'rowJson')='object' THEN p_payload->'rowJson' ELSE '{}'::jsonb END;
  v_economic:=CASE WHEN jsonb_typeof(p_payload->'economic_key')='object' THEN p_payload->'economic_key'
    WHEN jsonb_typeof(p_payload->'economicKey')='object' THEN p_payload->'economicKey' ELSE '{}'::jsonb END;
  IF p_kind='TIMESHEET' THEN
    v_candidate:=private.pay_workbench_modal_js_first_truthy_text_v2(ARRAY[p_payload->'candidate_id',p_payload->'candidateId',v_nested->'candidate_id',v_nested->'candidateId']);
    v_timesheet:=private.pay_workbench_modal_js_first_truthy_text_v2(ARRAY[p_payload->'timesheet_id',p_payload->'timesheetId',
      p_payload->'linked_timesheet_id',p_payload->'linkedTimesheetId',v_economic->'timesheet_id',v_economic->'timesheetId',
      v_nested->'timesheet_id',v_nested->'timesheetId',v_nested->'linked_timesheet_id',v_nested->'linkedTimesheetId']);
    RETURN CASE WHEN v_candidate<>'' AND v_timesheet<>'' THEN 'READY_TO_PAY|'||v_candidate||'|'||v_timesheet ELSE '' END;
  END IF;
  v_line_type:=UPPER(private.pay_workbench_modal_js_first_truthy_text_v2(ARRAY[p_payload->'line_type',p_payload->'lineType',
    v_nested->'line_type',v_nested->'lineType']));
  IF v_line_type<>'OVERPAYMENT_RECOVERY' THEN RETURN ''; END IF;
  v_finance:=private.pay_workbench_modal_js_first_truthy_text_v2(ARRAY[p_payload->'finance_case_id',p_payload->'financeCaseId',v_nested->'finance_case_id',v_nested->'financeCaseId']);
  IF v_finance<>'' THEN RETURN 'finance:'||v_finance||':overpayment_recovery'; END IF;
  v_parent:=private.pay_workbench_modal_js_first_truthy_text_v2(ARRAY[p_payload->'presentation_parent_line_id',p_payload->'presentationParentLineId',
    v_nested->'presentation_parent_line_id',v_nested->'presentationParentLineId']);
  RETURN v_parent;
END;
$function$;
ALTER FUNCTION private.pay_workbench_modal_ready_group_key_v2(jsonb,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_ready_group_key_v2(jsonb,text) FROM PUBLIC, anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_ready_group_members_v2(
  p_session public.banking_pay_workbench_sessions,p_channel text,p_candidate_id uuid
) RETURNS TABLE(row_id uuid,group_kind text,group_key text,selected boolean)
LANGUAGE sql STABLE SECURITY INVOKER SET search_path TO ''
AS $function$
  WITH eligible AS MATERIALIZED (
    SELECT r.*,private.pay_workbench_modal_row_payload_v2(r) AS payload
    FROM private.pay_workbench_modal_eligible_rows_v2(p_session.id,p_session.version,'canonical_preview_lines') AS r
    JOIN private.pay_workbench_modal_source_progress_facts_v2(p_session.id,p_session.version) AS scope ON scope.candidate_id=r.candidate_id
    WHERE scope.source_state='CURRENT' AND r.candidate_id=p_candidate_id
      AND NOT private.pay_workbench_modal_hidden_v2(r.row_json)
  ), selection AS MATERIALIZED (
    SELECT * FROM private.pay_workbench_modal_selection_rows_v2(p_session.id,p_session.version) WHERE is_selectable IS TRUE
  ), classified AS MATERIALIZED (
    SELECT r.id,r.selected,r.payload,UPPER(private.pay_workbench_modal_js_first_truthy_text_v2(ARRAY[
      r.payload->'line_type',r.payload->'lineType',r.payload#>'{row_json,line_type}',r.payload#>'{row_json,lineType}',
      r.payload#>'{rowJson,line_type}',r.payload#>'{rowJson,lineType}'])) AS line_type
    FROM eligible AS r JOIN selection ON selection.id=r.id
    WHERE private.pay_workbench_modal_row_matches_scope_v2(r.payload,p_session.filters_json,p_channel,'canonical_preview_lines')
  ), grouped AS (
    SELECT id,selected,CASE WHEN line_type='TIMESHEET_PAYMENT' THEN 'TIMESHEET'
      WHEN line_type='OVERPAYMENT_RECOVERY' THEN 'OVERPAYMENT' ELSE NULL END AS kind,payload FROM classified
  )
  SELECT id,kind,private.pay_workbench_modal_ready_group_key_v2(payload,kind),selected
  FROM grouped WHERE kind IS NOT NULL AND private.pay_workbench_modal_ready_group_key_v2(payload,kind)<>'';
$function$;
ALTER FUNCTION private.pay_workbench_modal_ready_group_members_v2(public.banking_pay_workbench_sessions,text,uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_ready_group_members_v2(public.banking_pay_workbench_sessions,text,uuid) FROM PUBLIC, anon, authenticated, service_role;

commit;
