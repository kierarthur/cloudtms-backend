-- Internal candidate-response assembly. The service-callable adapter remains
-- separately gated until complete output, rollback and browser parity pass.

\set ON_ERROR_STOP on

begin;

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_selection_response_finish_v2(
  p_session_id uuid,p_candidate_id uuid,p_options_json jsonb,p_actor_user_id uuid,
  p_request_id uuid,p_expected_view_digest text,p_open_ready_json jsonb,p_result jsonb
) RETURNS jsonb LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path TO ''
AS $function$
DECLARE
  v_result jsonb:=p_result;v_before jsonb;v_after jsonb;v_summary jsonb;v_reply jsonb;v_ready jsonb;v_anchor jsonb;
  v_options jsonb;v_session public.banking_pay_workbench_sessions%ROWTYPE;
BEGIN
  v_before:=v_result->'presentation_before';
  IF v_result->>'ok' IS DISTINCT FROM 'true' OR jsonb_typeof(v_before) IS DISTINCT FROM 'object'
     OR v_before->>'view_digest' IS DISTINCT FROM p_expected_view_digest THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_DEPENDENCY_UNAVAILABLE' USING ERRCODE='P0001';
  END IF;
  v_options:=p_options_json||jsonb_build_object('expected_progress_counter_version',v_result->'progress_counter_version');
  v_session:=private.pay_workbench_modal_context_v2(p_session_id,v_options,p_actor_user_id);
  v_after:=private.pay_workbench_modal_candidate_state_v2(v_session,v_options->>'pay_channel_scope',p_candidate_id);
  -- The existing complete summary still owns the selected headline, issue
  -- counts and Draft gate. Its one-row page is discarded, not used for totals.
  v_summary:=public.pay_workbench_session_get_candidate_summary_page_v1(
    p_session_id,v_options,p_actor_user_id,'CANDIDATE','ASC',NULL,1);
  IF v_after->>'view_digest' IS DISTINCT FROM v_summary->>'view_digest' THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_STALE_VIEW' USING ERRCODE='P0001';
  END IF;
  v_reply:=jsonb_build_object(
    'ok',true,'contract','BANKING_PAY_MODAL_STRUCTURE_V2','contract_version',1,
    'session_id',p_session_id,'candidate_id',p_candidate_id,'session_version',v_session.version,
    'progress_counter_version',v_session.progress_counter_version,'scope_hash',v_options->>'scope_hash',
    'request_id',p_request_id,'state_changed',v_result->'state_changed',
    'candidate',v_after->'candidate','candidate_absent',v_after->'candidate'='null'::jsonb,
    'view_digest',v_after->>'view_digest','global',v_summary->'global','rail',v_summary->'rail',
    'retention',jsonb_build_object('before_view_digest',v_before->>'view_digest',
      'other_candidates_unchanged',v_before->>'other_candidates_digest'=v_after->>'other_candidates_digest',
      'membership_unchanged',v_before->>'membership_digest'=v_after->>'membership_digest',
      'candidate_sort_unchanged',
        v_before#>'{candidate,candidate_sort_name}' IS NOT DISTINCT FROM v_after#>'{candidate,candidate_sort_name}'
        AND v_before#>'{candidate,candidate_sort_reference}' IS NOT DISTINCT FROM v_after#>'{candidate,candidate_sort_reference}',
      'amount_sort_unchanged',v_before#>'{candidate,selected_display_amount}' IS NOT DISTINCT FROM v_after#>'{candidate,selected_display_amount}',
      'deduction_sort_unchanged',v_before#>'{candidate,selected_deduction_exists}' IS NOT DISTINCT FROM v_after#>'{candidate,selected_deduction_exists}'))
    || private.pay_workbench_modal_movement_envelope_v2(v_result->'movements');
  IF octet_length(convert_to(v_reply::text,'UTF8'))>32*1024 THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_SELECTION_TOO_LARGE' USING ERRCODE='P0001';
  END IF;
  IF p_open_ready_json IS NOT NULL AND p_open_ready_json IS DISTINCT FROM 'null'::jsonb THEN
    -- A pre-selection ordinary cursor must never be reused after mutation.
    -- Only the independently typed position anchor may renew this read.
    IF p_open_ready_json->>'cursor' IS NOT NULL THEN
      v_anchor:=private.pay_workbench_modal_cursor_decode_v2(p_open_ready_json->>'cursor',jsonb_build_object(
        'contract','BANKING_PAY_MODAL_STRUCTURE_V2','kind','READY_PAGE_ANCHOR',
        'session_id',p_session_id,'candidate_id',p_candidate_id,'session_version',v_session.version,
        'scope_hash',v_options->>'scope_hash','page_limit',p_open_ready_json->'limit'));
      IF COALESCE(v_anchor->>'progress_counter_version','') !~ '^[0-9]{1,16}$'
         OR (v_anchor->>'progress_counter_version')::bigint>(p_options_json->>'expected_progress_counter_version')::bigint THEN
        RAISE EXCEPTION 'BANKING_PAY_V2_STALE_CURSOR' USING ERRCODE='P0001';
      END IF;
    END IF;
    v_ready:=public.pay_workbench_session_get_candidate_ready_page_v1(p_session_id,p_candidate_id,v_options,p_actor_user_id,
      p_open_ready_json->>'cursor',(p_open_ready_json->>'limit')::integer);
    IF v_ready->'candidate' IS DISTINCT FROM v_after->'candidate'
       OR v_ready->>'progress_counter_version' IS DISTINCT FROM v_reply->>'progress_counter_version' THEN
      RAISE EXCEPTION 'BANKING_PAY_V2_STALE_VIEW' USING ERRCODE='P0001';
    END IF;
    IF octet_length(convert_to(v_ready::text,'UTF8'))>512*1024 THEN
      RAISE EXCEPTION 'BANKING_PAY_V2_READY_TOO_LARGE' USING ERRCODE='P0001';
    END IF;
    v_reply:=v_reply||jsonb_build_object('ready_page',v_ready);
    IF octet_length(convert_to(v_reply::text,'UTF8'))>544*1024 THEN
      RAISE EXCEPTION 'BANKING_PAY_V2_SELECTION_TOO_LARGE' USING ERRCODE='P0001';
    END IF;
  END IF;
  PERFORM private.pay_workbench_modal_context_v2(p_session_id,v_options,p_actor_user_id);
  RETURN v_reply;
END;
$function$;
ALTER FUNCTION private.pay_workbench_modal_selection_response_finish_v2(uuid,uuid,jsonb,uuid,uuid,text,jsonb,jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_selection_response_finish_v2(uuid,uuid,jsonb,uuid,uuid,text,jsonb,jsonb) FROM PUBLIC, anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_candidate_selection_response_v2(
  p_session_id uuid,p_candidate_id uuid,p_options_json jsonb,p_actor_user_id uuid,
  p_action text,p_request_id uuid,p_expected_view_digest text,p_open_ready_json jsonb DEFAULT NULL
) RETURNS jsonb LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path TO ''
AS $function$
DECLARE
  v_result jsonb;
BEGIN
  -- One existing mutation entry point. Its exact intent fingerprint also binds
  -- presentation metadata, and its original receipt retains the before-state.
  v_result:=public.pay_workbench_session_set_selected_rows(p_session_id,jsonb_build_object(
    'modal_candidate_intent_v2',jsonb_build_object('candidate_id',p_candidate_id,'request_id',p_request_id,
      'action',p_action,'options',p_options_json,'presentation_v2',jsonb_build_object(
        'view_digest',p_expected_view_digest,'open_ready',p_open_ready_json))),p_actor_user_id);
  RETURN private.pay_workbench_modal_selection_response_finish_v2(p_session_id,p_candidate_id,p_options_json,p_actor_user_id,
    p_request_id,p_expected_view_digest,p_open_ready_json,v_result);
END;
$function$;
ALTER FUNCTION private.pay_workbench_modal_candidate_selection_response_v2(uuid,uuid,jsonb,uuid,text,uuid,text,jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_candidate_selection_response_v2(uuid,uuid,jsonb,uuid,text,uuid,text,jsonb) FROM PUBLIC, anon, authenticated, service_role;

-- Thin service boundary only. Authentication, scope, locks, replay and recovery
-- remain inside the existing owners composed by the private response helper.
CREATE OR REPLACE FUNCTION public.pay_workbench_session_set_candidate_ready_selection_v1(
  p_session_id uuid,p_candidate_id uuid,p_options_json jsonb,p_actor_user_id uuid,
  p_action text,p_request_id uuid,p_expected_view_digest text,p_open_ready_json jsonb DEFAULT NULL
) RETURNS jsonb LANGUAGE sql VOLATILE SECURITY DEFINER SET search_path TO ''
AS $function$
  SELECT private.pay_workbench_modal_candidate_selection_response_v2(
    p_session_id,p_candidate_id,p_options_json,p_actor_user_id,p_action,p_request_id,
    p_expected_view_digest,p_open_ready_json);
$function$;
ALTER FUNCTION public.pay_workbench_session_set_candidate_ready_selection_v1(uuid,uuid,jsonb,uuid,text,uuid,text,jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_session_set_candidate_ready_selection_v1(uuid,uuid,jsonb,uuid,text,uuid,text,jsonb) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.pay_workbench_session_set_candidate_ready_selection_v1(uuid,uuid,jsonb,uuid,text,uuid,text,jsonb) TO service_role;
NOTIFY pgrst, 'reload schema';

commit;
