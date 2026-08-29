-- One bounded public candidate summary. All financial values come from the
-- certified candidate projection; current Draft readiness remains its owner.

\set ON_ERROR_STOP on

begin;

CREATE OR REPLACE FUNCTION public.pay_workbench_session_get_candidate_summary_page_v1(
  p_session_id uuid,p_options_json jsonb,p_actor_user_id uuid,
  p_sort_key text DEFAULT 'CANDIDATE',p_sort_direction text DEFAULT 'ASC',
  p_cursor text DEFAULT NULL,p_limit integer DEFAULT 100
) RETURNS jsonb LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path TO ''
AS $function$
DECLARE v_session public.banking_pay_workbench_sessions%ROWTYPE;v_after public.banking_pay_workbench_sessions%ROWTYPE;
  v_options jsonb:=p_options_json;v_page jsonb;v_draft jsonb;v_counts jsonb;v_reply jsonb;
  v_provider text;v_environment text;v_count bigint;v_distinct bigint;
BEGIN
  PERFORM public.banking_pay_hot_path_budget_apply('PREVIEW_PROGRESS');
  v_session:=private.pay_workbench_modal_summary_context_v2(p_session_id,p_options_json,p_actor_user_id,p_cursor);
  v_options:=p_options_json||jsonb_build_object('scope_hash',
    private.pay_workbench_modal_scope_hash_v2(v_session,p_options_json->>'pay_channel_scope'));
  v_page:=private.pay_workbench_modal_candidate_page_v2(v_session,v_options,p_sort_key,p_sort_direction,p_cursor,p_limit);
  v_draft:=private.pay_workbench_modal_draft_gate_v2(p_session_id,(v_page#>>'{ready_global,selected_ready_count}')::bigint);
  -- Same settings owner as current bank checks and certified reuse. No rail is
  -- selected by browser input and no provider operation is performed here.
  SELECT UPPER(BTRIM(COALESCE(s.rail_provider_default,'CSV'))),UPPER(BTRIM(COALESCE(s.rail_env_default,'PROD')))
    INTO v_provider,v_environment FROM public.settings_defaults s WHERE s.id=1;
  IF NOT FOUND OR NULLIF(v_provider,'') IS NULL OR NULLIF(v_environment,'') IS NULL THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_DEPENDENCY_UNAVAILABLE' USING ERRCODE='P0001';
  END IF;
  -- The strict context owner already proved an active admin. These booleans
  -- expose existing editor/Refresh navigation, not permission to mutate data.
  SELECT jsonb_build_object('action_required_count',count(*) FILTER(WHERE i.issue_state='ACTION_REQUIRED'),
    'updating_count',count(*) FILTER(WHERE i.issue_state='UPDATING'),
    'blocked_count',count(*) FILTER(WHERE i.issue_state='BLOCKED')),
    count(*),count(DISTINCT i.identity)
  INTO v_counts,v_count,v_distinct
  FROM private.pay_workbench_modal_issue_index_v2(v_session,v_options->>'pay_channel_scope',v_provider,v_environment,v_draft,true,true) i;
  IF v_count<>v_distinct THEN RAISE EXCEPTION 'BANKING_PAY_V2_DUPLICATE_IDENTITY' USING ERRCODE='P0001';END IF;
  v_after:=private.pay_workbench_modal_context_v2(p_session_id,v_options,p_actor_user_id);
  v_reply:=(v_page-'ready_global')||jsonb_build_object(
    'ok',true,'contract','BANKING_PAY_MODAL_STRUCTURE_V2','contract_version',1,
    'session_id',p_session_id,'session_version',v_after.version,'progress_counter_version',v_after.progress_counter_version,
    'scope_hash',v_options->>'scope_hash',
    'global',v_page->'ready_global'||v_counts||jsonb_build_object('draft',v_draft),
    'rail',jsonb_build_object('provider_default',v_provider,'env_default',v_environment));
  IF octet_length(convert_to(v_reply::text,'UTF8'))>128*1024 THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_SUMMARY_TOO_LARGE' USING ERRCODE='P0001';
  END IF;
  RETURN v_reply;
END;
$function$;
ALTER FUNCTION public.pay_workbench_session_get_candidate_summary_page_v1(uuid,jsonb,uuid,text,text,text,integer) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_session_get_candidate_summary_page_v1(uuid,jsonb,uuid,text,text,text,integer) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.pay_workbench_session_get_candidate_summary_page_v1(uuid,jsonb,uuid,text,text,text,integer) TO service_role;
NOTIFY pgrst, 'reload schema';

commit;
