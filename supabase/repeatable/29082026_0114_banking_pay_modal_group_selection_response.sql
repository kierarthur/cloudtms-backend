-- Repeatable CloudTMS function/view authority: banking_pay_modal_group_selection_response
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

CREATE OR REPLACE FUNCTION public.pay_workbench_session_set_ready_group_v1(
  p_session_id uuid,p_candidate_id uuid,p_options_json jsonb,p_actor_user_id uuid,
  p_group_kind text,p_group_key text,p_selected boolean,p_request_id uuid,p_expected_view_digest text,
  p_open_ready_json jsonb DEFAULT NULL
) RETURNS jsonb LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path TO ''
AS $function$
DECLARE
  v_session public.banking_pay_workbench_sessions%ROWTYPE;v_result jsonb;
  v_reply jsonb;v_member_count integer;
BEGIN
  IF p_candidate_id IS NULL OR p_request_id IS NULL OR p_selected IS NULL
     OR p_group_kind NOT IN ('TIMESHEET','OVERPAYMENT')
     OR length(COALESCE(p_group_key,'')) NOT BETWEEN 1 AND 512
     OR p_group_key ~ '[[:cntrl:]]'
     OR COALESCE(p_expected_view_digest,'') !~ '^[a-f0-9]{64}$' THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_INPUT' USING ERRCODE='22023';
  END IF;
  IF p_open_ready_json IS NOT NULL AND p_open_ready_json IS DISTINCT FROM 'null'::jsonb THEN
    IF jsonb_typeof(p_open_ready_json) IS DISTINCT FROM 'object'
       OR NOT(p_open_ready_json ? 'cursor') OR jsonb_typeof(p_open_ready_json->'limit') IS DISTINCT FROM 'number'
       OR COALESCE(p_open_ready_json->>'limit','') !~ '^(100|[1-9][0-9]?)$'
       OR EXISTS(SELECT 1 FROM jsonb_object_keys(p_open_ready_json) k(value) WHERE value NOT IN('cursor','limit'))
       OR (p_open_ready_json->'cursor' IS DISTINCT FROM 'null'::jsonb AND
         (jsonb_typeof(p_open_ready_json->'cursor') IS DISTINCT FROM 'string'
          OR length(p_open_ready_json->>'cursor') NOT BETWEEN 1 AND 4096
          OR (p_open_ready_json->>'cursor') !~ '^[A-Za-z0-9_-]+$')) THEN
      RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_INPUT' USING ERRCODE='22023';
    END IF;
  END IF;
  PERFORM public.banking_pay_hot_path_budget_apply('WORKBENCH_CHUNK');
  PERFORM private.pay_workbench_modal_context_v2(p_session_id,p_options_json,p_actor_user_id);
  PERFORM 1 FROM public.banking_pay_workbench_sessions WHERE id=p_session_id FOR UPDATE;
  v_session:=private.pay_workbench_modal_context_v2(p_session_id,p_options_json,p_actor_user_id);
  IF private.pay_workbench_modal_candidate_state_v2(v_session,p_options_json->>'pay_channel_scope',p_candidate_id)->>'view_digest'
     IS DISTINCT FROM p_expected_view_digest THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_STALE_VIEW' USING ERRCODE='P0001';
  END IF;
  SELECT count(*)::integer INTO v_member_count
  FROM private.pay_workbench_modal_ready_group_members_v2(v_session,p_options_json->>'pay_channel_scope',p_candidate_id)
  WHERE group_kind=p_group_kind AND group_key=p_group_key;
  v_member_count:=COALESCE(v_member_count,0);
  IF v_member_count=0 THEN RAISE EXCEPTION 'BANKING_PAY_V2_GROUP_NOT_SELECTABLE' USING ERRCODE='P0001'; END IF;
  v_result:=public.pay_workbench_session_set_selected_rows(p_session_id,jsonb_build_object(
    'modal_group_intent_v2',jsonb_build_object('candidate_id',p_candidate_id,'request_id',p_request_id,
      'action',CASE WHEN p_selected THEN 'SELECT_ALL_READY' ELSE 'CLEAR_ALL_READY' END,
      'group_kind',p_group_kind,'group_key',p_group_key,'options',p_options_json,
      'presentation_v2',jsonb_build_object('view_digest',p_expected_view_digest,'open_ready',p_open_ready_json))),p_actor_user_id);
  v_reply:=private.pay_workbench_modal_selection_response_finish_v2(p_session_id,p_candidate_id,p_options_json,p_actor_user_id,
    p_request_id,p_expected_view_digest,p_open_ready_json,v_result)||jsonb_build_object(
      'selection_scope','COMPLETE_READY_GROUP','group_kind',p_group_kind,'group_key',p_group_key,
      'group_member_count',v_member_count,'owner_call_count',1);
  IF octet_length(convert_to((v_reply-'ready_page')::text,'UTF8'))>32*1024
    OR octet_length(convert_to(v_reply::text,'UTF8'))>544*1024 THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_SELECTION_TOO_LARGE' USING ERRCODE='P0001';
  END IF;
  RETURN v_reply;
END;
$function$;
ALTER FUNCTION public.pay_workbench_session_set_ready_group_v1(uuid,uuid,jsonb,uuid,text,text,boolean,uuid,text,jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_session_set_ready_group_v1(uuid,uuid,jsonb,uuid,text,text,boolean,uuid,text,jsonb) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.pay_workbench_session_set_ready_group_v1(uuid,uuid,jsonb,uuid,text,text,boolean,uuid,text,jsonb) TO service_role;
NOTIFY pgrst, 'reload schema';

commit;
