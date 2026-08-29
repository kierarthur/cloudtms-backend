-- Complete exact-key detail over existing issue owners. Original member payloads
-- are not rewritten. Paging is read-only and never becomes selection authority.

\set ON_ERROR_STOP on

begin;

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_issue_detail_members_v2(
 p_session public.banking_pay_workbench_sessions,p_channel text,p_provider text,p_environment text,
 p_progress jsonb,p_identity text,p_actions boolean
) RETURNS TABLE(identity text,candidate_id uuid,source_kind text,preview_row_id uuid,
 context_only boolean,affected_by_task boolean,payment_membership_known boolean,payload jsonb,task_meta jsonb,bank_row jsonb)
LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path TO ''
AS $function$
DECLARE v_ref record;
BEGIN
 SELECT i.*,count(*) OVER() AS matches INTO v_ref
 FROM private.pay_workbench_modal_issue_index_v2(p_session,p_channel,p_provider,p_environment,p_progress,true,true) i
 WHERE i.identity=p_identity AND CASE WHEN p_actions THEN i.issue_state IN ('ACTION_REQUIRED','UPDATING') ELSE i.issue_state='BLOCKED' END;
 IF NOT FOUND THEN RAISE EXCEPTION 'BANKING_PAY_V2_ITEM_NOT_CURRENT' USING ERRCODE='P0001';END IF;
 IF v_ref.matches<>1 THEN RAISE EXCEPTION 'BANKING_PAY_V2_DUPLICATE_IDENTITY' USING ERRCODE='P0001';END IF;
 IF v_ref.task_family IN ('FINANCE_CASE','FINANCE_COMPONENT') THEN
  RETURN QUERY
  SELECT encode(extensions.digest(convert_to(jsonb_build_array(p_identity,f.candidate_id,'PREVIEW_ROW',f.preview_row_id)::text,'UTF8'),'sha256'),'hex'),
   f.candidate_id,'PREVIEW_ROW'::text,f.preview_row_id,COALESCE(f.task_json->'context_only'='true'::jsonb,false),
   NOT COALESCE(f.task_json->'context_only'='true'::jsonb,false),true,f.row_payload,f.task_json,NULL::jsonb
  FROM private.pay_workbench_modal_finance_task_members_v2(p_session,p_channel) f WHERE f.task_key=v_ref.task_key;
 ELSIF v_ref.task_family='BANK_ACCOUNT' THEN
  RETURN QUERY
  SELECT encode(extensions.digest(convert_to(jsonb_build_array(p_identity,b.candidate_id,b.source_kind,b.preview_row_id,
    b.task_json->'source_ordinal')::text,'UTF8'),'sha256'),'hex'),
   b.candidate_id,b.source_kind,b.preview_row_id,b.context_only,b.affected_by_task,
   COALESCE(b.task_json->'payment_membership_known'='true'::jsonb,true),b.source_payload,b.task_json,b.bank_row
  FROM private.pay_workbench_modal_bank_task_members_v2(p_session,p_channel,p_provider,p_environment,true) b
  WHERE b.task_key=v_ref.task_key AND (p_actions OR b.candidate_id=v_ref.candidate_id);
 ELSIF v_ref.task_family='SOURCE_PROGRESS' THEN
  RETURN QUERY
  SELECT encode(extensions.digest(convert_to(jsonb_build_array(p_identity,s.candidate_id,s.source_kind,s.preview_row_id)::text,'UTF8'),'sha256'),'hex'),
   s.candidate_id,s.source_kind,s.preview_row_id,COALESCE(s.task_json->'context_only'='true'::jsonb,false),false,false,
   s.source_payload,s.task_json,NULL::jsonb
  FROM private.pay_workbench_modal_source_issue_members_v2(p_session,p_channel,p_progress,true) s
  WHERE s.task_key=v_ref.task_key AND (p_actions OR s.candidate_id=v_ref.candidate_id);
 ELSIF v_ref.task_family='PASSIVE_PAYMENT' THEN
  RETURN QUERY
  SELECT encode(extensions.digest(convert_to(jsonb_build_array(p_identity,r.candidate_id,'PREVIEW_ROW',r.id)::text,'UTF8'),'sha256'),'hex'),
   r.candidate_id,'PREVIEW_ROW'::text,r.id,false,true,true,private.pay_workbench_modal_row_payload_v2(r),
   jsonb_build_object('task_family','PASSIVE_PAYMENT','state','BLOCKED'),NULL::jsonb
  FROM public.banking_pay_workbench_preview_rows r WHERE r.id=v_ref.preview_row_id
   AND r.session_id=p_session.id AND r.session_version=p_session.version;
 ELSE RAISE EXCEPTION 'BANKING_PAY_V2_DEPENDENCY_UNAVAILABLE' USING ERRCODE='P0001';
 END IF;
END;
$function$;
ALTER FUNCTION private.pay_workbench_modal_issue_detail_members_v2(public.banking_pay_workbench_sessions,text,text,text,jsonb,text,boolean) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_issue_detail_members_v2(public.banking_pay_workbench_sessions,text,text,text,jsonb,text,boolean) FROM PUBLIC, anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_issue_detail_v2(
 p_session_id uuid,p_options_json jsonb,p_actor_user_id uuid,p_identity text,p_actions boolean,p_cursor text,p_limit integer
) RETURNS jsonb LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path TO ''
AS $function$
DECLARE s public.banking_pay_workbench_sessions%ROWTYPE;v_after public.banking_pay_workbench_sessions%ROWTYPE;
 v_binding jsonb;v_cursor jsonb;v_provider text;v_environment text;v_progress jsonb;v_page jsonb;v_reply jsonb;v_last text;
BEGIN
 IF p_identity IS NULL OR p_identity !~ '^[a-f0-9]{64}$' OR p_actions IS NULL OR p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 100 THEN
  RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_INPUT' USING ERRCODE='22023';END IF;
 PERFORM public.banking_pay_hot_path_budget_apply('PREVIEW_PROGRESS');
 s:=private.pay_workbench_modal_context_v2(p_session_id,p_options_json,p_actor_user_id);
 v_binding:=jsonb_build_object('kind',CASE WHEN p_actions THEN 'ACTION_DETAIL' ELSE 'BLOCKED_DETAIL' END,
  'session_id',s.id,'session_version',s.version,'progress_counter_version',s.progress_counter_version,
  'scope_hash',p_options_json->>'scope_hash','issue_identity',p_identity,'limit',p_limit);
 v_cursor:=private.pay_workbench_modal_cursor_decode_v2(p_cursor,v_binding);
 IF v_cursor IS NOT NULL AND (jsonb_typeof(v_cursor->'last_identity') IS DISTINCT FROM 'string'
  OR v_cursor->>'last_identity' !~ '^[a-f0-9]{64}$') THEN
  RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_CURSOR' USING ERRCODE='22023';END IF;
 v_last:=v_cursor->>'last_identity';
 SELECT UPPER(BTRIM(COALESCE(d.rail_provider_default,'CSV'))),UPPER(BTRIM(COALESCE(d.rail_env_default,'PROD')))
 INTO v_provider,v_environment FROM public.settings_defaults d WHERE d.id=1;
 IF NOT FOUND OR NULLIF(v_provider,'') IS NULL OR NULLIF(v_environment,'') IS NULL THEN
  RAISE EXCEPTION 'BANKING_PAY_V2_DEPENDENCY_UNAVAILABLE' USING ERRCODE='P0001';END IF;
 -- Only the progress recommendation is consumed here; no Draft gate or
 -- selected amount/count is derived from this detail's limited members.
 v_progress:=private.pay_workbench_modal_draft_gate_v2(p_session_id,0);
 WITH members AS MATERIALIZED (
  SELECT m.* FROM private.pay_workbench_modal_issue_detail_members_v2(s,p_options_json->>'pay_channel_scope',
   v_provider,v_environment,v_progress,p_identity,p_actions) m
 ), ordered AS MATERIALIZED (
  SELECT m.*,row_number() OVER(ORDER BY m.identity) AS ordinal FROM members m
 ), position AS (
  SELECT CASE WHEN v_last IS NULL THEN 0::bigint ELSE (SELECT m.ordinal FROM ordered m WHERE m.identity=v_last) END AS n
 ), stats AS (
  SELECT count(*) AS total,count(DISTINCT m.identity) AS unique_count,count(DISTINCT m.candidate_id) AS candidates,
   bool_and(m.payment_membership_known) AS complete,
   count(DISTINCT m.preview_row_id) FILTER(WHERE m.affected_by_task) AS payments
  FROM members m
 ), page AS MATERIALIZED (
  SELECT m.* FROM ordered m WHERE v_last IS NULL OR m.identity>v_last ORDER BY m.identity LIMIT p_limit
 )
 SELECT jsonb_build_object(
  'total_count',t.total,'affected_candidate_count',t.candidates,'affected_payment_count_complete',COALESCE(t.complete,false),
  'affected_payment_count',CASE WHEN t.complete THEN t.payments END,
  'page_number',p.n/p_limit+1,'has_previous',p.n>0,'has_more',p.n+p_limit<t.total,
  'previous_cursor',CASE WHEN p.n>p_limit THEN (SELECT private.pay_workbench_modal_cursor_encode_v2(v_binding||
    jsonb_build_object('last_identity',m.identity)) FROM ordered m WHERE m.ordinal=p.n-p_limit) END,
  'next_cursor',CASE WHEN p.n+p_limit<t.total THEN (SELECT private.pay_workbench_modal_cursor_encode_v2(v_binding||
    jsonb_build_object('last_identity',m.identity)) FROM page m ORDER BY m.identity DESC LIMIT 1) END,
  'rows',COALESCE((SELECT jsonb_agg(jsonb_build_object('identity',m.identity,'candidate_id',m.candidate_id,
    'source_kind',m.source_kind,'preview_row_id',m.preview_row_id,'context_only',m.context_only,
    'affected_by_task',m.affected_by_task,'payload',m.payload,'task_meta',m.task_meta,'bank_row',m.bank_row) ORDER BY m.identity) FROM page m),'[]'::jsonb),
  'unique_members',t.unique_count=t.total,
  'valid_position',p.n IS NOT NULL AND p.n%p_limit=0 AND (p.n=0 OR p.n<t.total))
 INTO v_page FROM stats t CROSS JOIN position p;
 IF v_page->'unique_members' IS DISTINCT FROM 'true'::jsonb THEN
  RAISE EXCEPTION 'BANKING_PAY_V2_DUPLICATE_IDENTITY' USING ERRCODE='P0001';END IF;
 IF v_page->>'total_count'='0' THEN RAISE EXCEPTION 'BANKING_PAY_V2_ITEM_NOT_CURRENT' USING ERRCODE='P0001';END IF;
 IF v_page->'valid_position' IS DISTINCT FROM 'true'::jsonb THEN
  RAISE EXCEPTION 'BANKING_PAY_V2_STALE_CURSOR' USING ERRCODE='P0001';END IF;
 v_after:=private.pay_workbench_modal_context_v2(p_session_id,p_options_json,p_actor_user_id);
 v_reply:=(v_page-'unique_members'-'valid_position')||jsonb_build_object(
  'ok',true,'contract','BANKING_PAY_MODAL_STRUCTURE_V2','contract_version',1,
  'session_id',s.id,'session_version',v_after.version,'progress_counter_version',v_after.progress_counter_version,
  'scope_hash',p_options_json->>'scope_hash',CASE WHEN p_actions THEN 'task_key' ELSE 'blocker_key' END,p_identity);
 IF octet_length(convert_to(v_reply::text,'UTF8'))>256*1024 THEN
  RAISE EXCEPTION 'BANKING_PAY_V2_DETAIL_TOO_LARGE' USING ERRCODE='P0001';END IF;
 RETURN v_reply;
END;
$function$;
ALTER FUNCTION private.pay_workbench_modal_issue_detail_v2(uuid,jsonb,uuid,text,boolean,text,integer) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_issue_detail_v2(uuid,jsonb,uuid,text,boolean,text,integer) FROM PUBLIC, anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION public.pay_workbench_session_get_action_required_detail_v1(
 p_session_id uuid,p_options_json jsonb,p_actor_user_id uuid,p_task_key text,p_cursor text DEFAULT NULL,p_limit integer DEFAULT 100
) RETURNS jsonb LANGUAGE sql VOLATILE SECURITY DEFINER SET search_path TO ''
AS $function$
 SELECT private.pay_workbench_modal_issue_detail_v2(p_session_id,p_options_json,p_actor_user_id,p_task_key,true,p_cursor,p_limit);
$function$;
ALTER FUNCTION public.pay_workbench_session_get_action_required_detail_v1(uuid,jsonb,uuid,text,text,integer) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_session_get_action_required_detail_v1(uuid,jsonb,uuid,text,text,integer) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.pay_workbench_session_get_action_required_detail_v1(uuid,jsonb,uuid,text,text,integer) TO service_role;

CREATE OR REPLACE FUNCTION public.pay_workbench_session_get_blocked_detail_v1(
 p_session_id uuid,p_options_json jsonb,p_actor_user_id uuid,p_blocker_key text,p_cursor text DEFAULT NULL,p_limit integer DEFAULT 100
) RETURNS jsonb LANGUAGE sql VOLATILE SECURITY DEFINER SET search_path TO ''
AS $function$
 SELECT private.pay_workbench_modal_issue_detail_v2(p_session_id,p_options_json,p_actor_user_id,p_blocker_key,false,p_cursor,p_limit);
$function$;
ALTER FUNCTION public.pay_workbench_session_get_blocked_detail_v1(uuid,jsonb,uuid,text,text,integer) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_session_get_blocked_detail_v1(uuid,jsonb,uuid,text,text,integer) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.pay_workbench_session_get_blocked_detail_v1(uuid,jsonb,uuid,text,text,integer) TO service_role;
NOTIFY pgrst,'reload schema';

commit;
