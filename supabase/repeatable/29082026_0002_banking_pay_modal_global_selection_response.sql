-- Compact response adapter for the existing filtered all-page Ready intent.
-- Current Workbench and Draft owners remain the sole financial authority.
\set ON_ERROR_STOP on
begin;
CREATE OR REPLACE FUNCTION public.pay_workbench_session_set_filtered_ready_selection_v1(
  p_session_id uuid,p_options_json jsonb,p_actor_user_id uuid,
  p_action text,p_request_id uuid,p_expected_view_digest text
) RETURNS jsonb LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path TO ''
AS $function$
DECLARE v_result jsonb;v_options jsonb;v_summary jsonb;v_reply jsonb;
BEGIN
  v_result:=public.pay_workbench_session_set_selected_rows(p_session_id,jsonb_build_object(
    'modal_global_intent_v2',jsonb_build_object('request_id',p_request_id,'action',p_action,'options',p_options_json,
      'presentation_v2',jsonb_build_object('view_digest',p_expected_view_digest,'open_ready',NULL))),p_actor_user_id);
  IF v_result->>'ok' IS DISTINCT FROM 'true'
     OR v_result#>>'{presentation_before,view_digest}' IS DISTINCT FROM p_expected_view_digest THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_DEPENDENCY_UNAVAILABLE' USING ERRCODE='P0001';
  END IF;
  v_options:=p_options_json||jsonb_build_object('expected_progress_counter_version',v_result->'progress_counter_version');
  v_summary:=public.pay_workbench_session_get_candidate_summary_page_v1(
    p_session_id,v_options,p_actor_user_id,'CANDIDATE','ASC',NULL,1);
  v_reply:=jsonb_build_object('ok',true,'contract','BANKING_PAY_MODAL_STRUCTURE_V2','contract_version',1,
    'session_id',p_session_id,'session_version',v_summary->'session_version',
    'progress_counter_version',v_summary->'progress_counter_version','scope_hash',v_options->>'scope_hash',
    'request_id',p_request_id,'state_changed',v_result->'state_changed','selection_scope','FILTERED_READY',
    'before_view_digest',p_expected_view_digest,'view_digest',v_summary->>'view_digest',
    'requires_summary_refresh',true,'global',v_summary->'global','rail',v_summary->'rail')
    || private.pay_workbench_modal_movement_envelope_v2(v_result->'movements');
  IF octet_length(convert_to(v_reply::text,'UTF8'))>32*1024 THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_SELECTION_TOO_LARGE' USING ERRCODE='P0001';
  END IF;
  PERFORM private.pay_workbench_modal_context_v2(p_session_id,v_options,p_actor_user_id);
  RETURN v_reply;
END;
$function$;
ALTER FUNCTION public.pay_workbench_session_set_filtered_ready_selection_v1(uuid,jsonb,uuid,text,uuid,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_session_set_filtered_ready_selection_v1(uuid,jsonb,uuid,text,uuid,text) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.pay_workbench_session_set_filtered_ready_selection_v1(uuid,jsonb,uuid,text,uuid,text) TO service_role;
NOTIFY pgrst, 'reload schema';
commit;
