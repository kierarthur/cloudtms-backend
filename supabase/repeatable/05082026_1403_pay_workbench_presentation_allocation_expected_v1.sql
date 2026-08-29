-- Banking Pay bounded-scope Version 1.2.10.
-- Derive the protected public-timesheet presentation contract from one sealed
-- canonical presentation-state row.  This is independent of the renderer's
-- final line table and deliberately excludes presentation-only decoration.

CREATE OR REPLACE FUNCTION private.pay_workbench_presentation_allocation_expected_v1(
  p_state_json jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
SECURITY DEFINER
SET search_path = ''
AS $function$
DECLARE
  v_state jsonb:=COALESCE(p_state_json,'{}'::jsonb);
  v_timesheet_id text:=NULLIF(BTRIM(COALESCE(v_state->>'timesheet_id','')),'');
  v_candidate_id text:=NULLIF(BTRIM(COALESCE(v_state->>'candidate_id','')),'');
  v_pay_channel text:=UPPER(NULLIF(BTRIM(COALESCE(v_state->>'candidate_pay_method','')),''));
  v_has_whole_snooze boolean:=COALESCE((v_state->>'has_active_timesheet_snooze')::boolean,false);
  v_snooze_until_date text:=NULLIF(BTRIM(COALESCE(v_state->>'snooze_until_date','')),'');
  v_whole_snooze_indefinite boolean:=false;
  v_total_amount numeric:=ROUND(COALESCE((v_state->>'amount_ex_vat')::numeric,0),2);
  v_case_blocked boolean:=COALESCE((v_state->>'case_is_blocked')::boolean,false);
  v_has_ready boolean:=false;
  v_has_blocked boolean:=false;
  v_has_case boolean:=false;
  v_ready_for_draft boolean:=COALESCE((v_state->>'is_ready_for_draft')::boolean,false);
  v_partial_ready boolean:=false;
  v_non_resolution_block boolean:=false;
  v_blocked_segment_count integer:=GREATEST(COALESCE((v_state->>'blocked_visible_segment_count')::integer,0),0);
  v_blocked_expense_count integer:=GREATEST(COALESCE((v_state->>'blocked_expense_count')::integer,0),0);
  v_ready_segment_count integer:=GREATEST(COALESCE((v_state->>'ready_segment_count')::integer,0),0);
  v_ready_amount numeric:=ROUND(COALESCE((v_state->>'ready_section_amount_ex_vat')::numeric,0),2);
  v_case_amount numeric:=ROUND(COALESCE((v_state->>'case_resolution_section_amount_ex_vat')::numeric,0),2);
  v_case_needs_resolution boolean:=false;
  v_has_actionable_component boolean:=false;
  v_has_payee_readiness_block boolean:=COALESCE(
    (v_state->>'has_payee_readiness_block')::boolean,false);
  v_result jsonb:='[]'::jsonb;
  v_line_id text;
BEGIN
  IF jsonb_typeof(v_state)<>'object'
     OR v_timesheet_id IS NULL
     OR v_timesheet_id !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
     OR v_candidate_id IS NULL
     OR v_candidate_id !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
     OR v_pay_channel IS NULL THEN
    RAISE EXCEPTION 'PAY_WORKBENCH_PRESENTATION_STATE_INVALID' USING ERRCODE='22023';
  END IF;

  v_case_needs_resolution:=COALESCE(
    (v_state#>>'{case_resolution_summary_json,case_needs_resolution}')::boolean,false);
  v_whole_snooze_indefinite:=v_has_whole_snooze AND v_snooze_until_date IS NULL;
  SELECT EXISTS(
    SELECT 1
    FROM jsonb_array_elements(CASE
      WHEN jsonb_typeof(v_state->'case_components_json')='array'
        THEN v_state->'case_components_json' ELSE '[]'::jsonb END) component(value)
    WHERE COALESCE((component.value->>'requires_resolution')::boolean,false)
      AND COALESCE((component.value->>'is_actionable_resolution_row')::boolean,false)
  ) INTO v_has_actionable_component;
  -- A component-bearing zero-net target remains privately attested but does
  -- not invent a public economic row.
  v_has_ready:=v_total_amount<>0 AND (v_ready_amount<>0 OR v_ready_segment_count>0);
  v_non_resolution_block:=NOT v_has_whole_snooze AND NOT v_ready_for_draft
    AND NOT v_case_needs_resolution
    AND (NOT v_case_blocked OR v_has_payee_readiness_block)
    AND v_has_ready;
  -- An indefinite whole-timesheet snooze is completely hidden.  A dated whole
  -- snooze retains the existing BLOCKED_FOR_PAY presentation.
  v_has_blocked:=v_total_amount<>0 AND (
    (v_has_whole_snooze AND NOT v_whole_snooze_indefinite)
    OR v_blocked_segment_count>0 OR v_blocked_expense_count>0
    OR v_non_resolution_block);
  v_has_case:=NOT v_has_whole_snooze AND v_case_blocked AND v_case_needs_resolution
    AND (v_case_amount<>0 OR v_has_actionable_component)
    AND v_blocked_segment_count=0 AND NOT v_non_resolution_block;
  v_partial_ready:=NOT v_has_whole_snooze AND NOT v_case_blocked AND v_has_ready
    AND (v_blocked_segment_count>0 OR v_blocked_expense_count>0);

  IF NOT v_has_whole_snooze AND NOT v_case_blocked AND v_has_ready AND v_ready_for_draft THEN
    v_line_id:=CASE WHEN v_partial_ready THEN v_timesheet_id||':01:ready' ELSE v_timesheet_id END;
    v_result:=v_result||jsonb_build_array(jsonb_build_object(
      'candidate_id',v_candidate_id,'timesheet_id',v_timesheet_id,
      'line_identity',v_line_id,'parent_line_identity',v_timesheet_id,
      'presentation_section','READY_TO_PAY','pay_channel',v_pay_channel,
      'amount_ex_vat',ROUND(COALESCE((v_state->>'ready_section_amount_ex_vat')::numeric,0),2),
      'draftable',v_ready_for_draft,'excluded_from_allocation',false,
      'section_non_segment_amount_ex_vat',ROUND(COALESCE(
        (v_state->>'non_segment_amount_ex_vat')::numeric,0),2),
      'section_segment_rows',CASE WHEN jsonb_typeof(v_state->'ready_segment_rows_json')='array'
        THEN v_state->'ready_segment_rows_json' ELSE '[]'::jsonb END,
      'resolved_segment_rows_replace_source_total',COALESCE(
        (v_state->>'resolved_segment_rows_replace_source_total')::boolean,false)));
  END IF;

  IF v_has_case THEN
    v_result:=v_result||jsonb_build_array(jsonb_build_object(
      'candidate_id',v_candidate_id,'timesheet_id',v_timesheet_id,
      'line_identity',v_timesheet_id||':03:case','parent_line_identity',v_timesheet_id,
      'presentation_section','CASES_RESOLUTIONS','pay_channel',v_pay_channel,
      'amount_ex_vat',ROUND(COALESCE(
        (v_state->>'case_resolution_section_amount_ex_vat')::numeric,0),2),
      'draftable',false,'excluded_from_allocation',false,
      'section_non_segment_amount_ex_vat',ROUND(COALESCE(
        (v_state->>'case_resolution_section_amount_ex_vat')::numeric,0),2),
      'section_segment_rows','[]'::jsonb,
      'resolved_segment_rows_replace_source_total',false));
  END IF;

  IF v_has_blocked AND (
       v_has_whole_snooze OR v_non_resolution_block OR v_blocked_segment_count>0
       OR v_blocked_expense_count>0
     ) THEN
    v_line_id:=CASE WHEN NOT v_has_whole_snooze AND NOT v_case_blocked
      AND v_has_ready AND v_blocked_segment_count>0
      THEN v_timesheet_id||':02:blocked' ELSE v_timesheet_id END;
    v_result:=v_result||jsonb_build_array(jsonb_build_object(
      'candidate_id',v_candidate_id,'timesheet_id',v_timesheet_id,
      'line_identity',v_line_id,'parent_line_identity',v_timesheet_id,
      'presentation_section','BLOCKED_FOR_PAY','pay_channel',v_pay_channel,
      'amount_ex_vat',ROUND(CASE WHEN v_has_whole_snooze OR v_non_resolution_block
        THEN COALESCE((v_state->>'amount_ex_vat')::numeric,0)
        ELSE COALESCE((v_state->>'blocked_section_amount_ex_vat')::numeric,0) END,2),
      'draftable',CASE WHEN v_has_whole_snooze THEN v_ready_for_draft ELSE false END,
      'excluded_from_allocation',v_has_whole_snooze,
      'section_non_segment_amount_ex_vat',ROUND(CASE
        WHEN v_has_whole_snooze OR v_non_resolution_block
          THEN COALESCE((v_state->>'non_segment_amount_ex_vat')::numeric,0)
        ELSE COALESCE((v_state->>'blocked_expense_amount_ex_vat')::numeric,0) END,2),
      'section_segment_rows',CASE
        WHEN v_has_whole_snooze OR v_non_resolution_block THEN CASE
          WHEN jsonb_typeof(v_state->'visible_segment_rows_json')='array'
            THEN v_state->'visible_segment_rows_json' ELSE '[]'::jsonb END
        ELSE CASE WHEN jsonb_typeof(v_state->'blocked_visible_segment_rows_json')='array'
          THEN v_state->'blocked_visible_segment_rows_json' ELSE '[]'::jsonb END END,
      'resolved_segment_rows_replace_source_total',false));
  END IF;

  RETURN v_result;
END;
$function$;

ALTER FUNCTION private.pay_workbench_presentation_allocation_expected_v1(jsonb)
  OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_presentation_allocation_expected_v1(jsonb)
  FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION private.pay_workbench_presentation_allocation_expected_v1(jsonb)
  TO postgres;
