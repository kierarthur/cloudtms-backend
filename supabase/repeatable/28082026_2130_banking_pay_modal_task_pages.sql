-- Complete task sort/search before bounded paging. Updating stays separate
-- from human action and has explicit continuation, never a silently cut array.

\set ON_ERROR_STOP on

begin;

CREATE OR REPLACE FUNCTION public.pay_workbench_session_get_action_required_page_v1(
 p_session_id uuid,p_options_json jsonb,p_actor_user_id uuid,
 p_sort_key text DEFAULT 'TITLE',p_sort_direction text DEFAULT 'ASC',p_cursor text DEFAULT NULL,
 p_limit integer DEFAULT 100,p_search text DEFAULT '',p_view text DEFAULT 'ACTION_REQUIRED'
) RETURNS jsonb LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path TO ''
AS $function$
DECLARE s public.banking_pay_workbench_sessions%ROWTYPE;v_after public.banking_pay_workbench_sessions%ROWTYPE;
 v_binding jsonb;v_cursor jsonb;v_last text;v_page jsonb;v_reply jsonb;v_progress jsonb;
 v_provider text;v_environment text;
BEGIN
 PERFORM public.banking_pay_hot_path_budget_apply('PREVIEW_PROGRESS');
 IF p_view IS NULL OR p_view NOT IN ('ACTION_REQUIRED','UPDATING') OR p_sort_key IS NULL
  OR p_sort_key NOT IN ('TITLE','CANDIDATES','PAYMENTS') OR p_sort_direction IS NULL
  OR p_sort_direction NOT IN ('ASC','DESC') OR p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 100
  OR p_search IS NULL OR char_length(p_search)>200 OR p_search<>btrim(p_search)
  OR (p_view='UPDATING' AND (p_sort_key<>'TITLE' OR p_sort_direction<>'ASC' OR p_search<>'')) THEN
  RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_INPUT' USING ERRCODE='22023';END IF;
 -- Exact UTF-16 length/control boundary used by the existing browser contract.
 IF char_length(p_search)+(SELECT count(*) FROM generate_series(1,char_length(p_search)) n
    WHERE ascii(substr(p_search,n,1))>65535)>200
  OR EXISTS(SELECT 1 FROM generate_series(1,char_length(p_search)) n WHERE ascii(substr(p_search,n,1)) BETWEEN 1 AND 31
    OR ascii(substr(p_search,n,1))=127) THEN
  RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_INPUT' USING ERRCODE='22023';END IF;
 s:=private.pay_workbench_modal_context_v2(p_session_id,p_options_json,p_actor_user_id);
 v_binding:=jsonb_build_object('kind','ACTION_TASKS','view',p_view,'session_id',s.id,'session_version',s.version,
  'progress_counter_version',s.progress_counter_version,'scope_hash',p_options_json->>'scope_hash',
  'sort_key',p_sort_key,'sort_direction',p_sort_direction,'search',p_search,'limit',p_limit);
 v_cursor:=private.pay_workbench_modal_cursor_decode_v2(p_cursor,v_binding);
 IF v_cursor IS NOT NULL AND (jsonb_typeof(v_cursor->'last_identity') IS DISTINCT FROM 'string'
   OR v_cursor->>'last_identity' !~ '^[a-f0-9]{64}$') THEN
  RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_CURSOR' USING ERRCODE='22023';END IF;
 v_last:=v_cursor->>'last_identity';
 SELECT UPPER(BTRIM(COALESCE(d.rail_provider_default,'CSV'))),UPPER(BTRIM(COALESCE(d.rail_env_default,'PROD')))
 INTO v_provider,v_environment FROM public.settings_defaults d WHERE d.id=1;
 IF NOT FOUND OR NULLIF(v_provider,'') IS NULL OR NULLIF(v_environment,'') IS NULL THEN
  RAISE EXCEPTION 'BANKING_PAY_V2_DEPENDENCY_UNAVAILABLE' USING ERRCODE='P0001';END IF;
 v_progress:=private.pay_workbench_modal_draft_gate_v2(p_session_id,0);
 WITH tasks AS MATERIALIZED (
  SELECT t.* FROM private.pay_workbench_modal_task_summaries_v2(s,p_options_json->>'pay_channel_scope',
   v_provider,v_environment,v_progress,true,true) t
 ), facts AS MATERIALIZED (
  SELECT t.*,lower(t.title) COLLATE "C" AS sort_text,
   CASE p_sort_key WHEN 'CANDIDATES' THEN t.affected_candidate_count WHEN 'PAYMENTS' THEN t.affected_payment_count END AS sort_number
  FROM tasks t WHERE t.issue_state=p_view AND (p_search='' OR position(lower(p_search) IN t.search_text)>0)
 ), ranked AS MATERIALIZED (
  SELECT f.*,row_number() OVER(ORDER BY
   (p_sort_key<>'TITLE' AND f.sort_number IS NULL),
   CASE WHEN p_sort_key='TITLE' AND p_sort_direction='ASC' THEN f.sort_text END COLLATE "C" ASC,
   CASE WHEN p_sort_key='TITLE' AND p_sort_direction='DESC' THEN f.sort_text END COLLATE "C" DESC,
   CASE WHEN p_sort_direction='ASC' THEN f.sort_number END ASC,
   CASE WHEN p_sort_direction='DESC' THEN f.sort_number END DESC,f.identity COLLATE "C") AS ordinal
  FROM facts f
 ), boundary AS (
  SELECT b.* FROM ranked b WHERE b.identity=v_last
 ), page AS MATERIALIZED (
  SELECT r.* FROM ranked r WHERE v_last IS NULL OR EXISTS(SELECT 1 FROM boundary b WHERE
   CASE WHEN p_sort_key='TITLE' THEN
    (CASE WHEN p_sort_direction='ASC' THEN r.sort_text>b.sort_text ELSE r.sort_text<b.sort_text END)
     OR (r.sort_text=b.sort_text AND r.identity COLLATE "C">b.identity COLLATE "C")
   ELSE
    (r.sort_number IS NULL AND b.sort_number IS NOT NULL)
    OR (r.sort_number IS NOT NULL AND b.sort_number IS NOT NULL AND
      CASE WHEN p_sort_direction='ASC' THEN r.sort_number>b.sort_number ELSE r.sort_number<b.sort_number END)
    OR (r.sort_number IS NOT DISTINCT FROM b.sort_number AND r.identity COLLATE "C">b.identity COLLATE "C")
   END)
  ORDER BY r.ordinal LIMIT p_limit
 ), updating AS MATERIALIZED (
  SELECT t.*,row_number() OVER(ORDER BY lower(t.title) COLLATE "C",t.identity COLLATE "C") AS ordinal
  FROM tasks t WHERE t.issue_state='UPDATING'
 ), stats AS (
  SELECT (SELECT count(*) FROM facts) AS total,(SELECT count(*) FROM tasks t WHERE t.issue_state=p_view) AS scope_count,
   (SELECT count(*) FROM updating) AS updating_count,
   CASE WHEN v_last IS NULL THEN 0::bigint ELSE (SELECT b.ordinal FROM boundary b) END AS position
 )
 SELECT jsonb_build_object('view',p_view,'search',p_search,'sort_key',p_sort_key,'sort_direction',p_sort_direction,
  'total_count',x.total,'scope_count',x.scope_count,'page_number',CASE WHEN x.total=0 THEN 0 ELSE x.position/p_limit+1 END,
  'has_previous',x.position>0,'has_more',x.position+p_limit<x.total,
  'previous_cursor',CASE WHEN x.position>p_limit THEN (SELECT private.pay_workbench_modal_cursor_encode_v2(
    v_binding||jsonb_build_object('last_identity',r.identity)) FROM ranked r WHERE r.ordinal=x.position-p_limit) END,
  'next_cursor',CASE WHEN x.position+p_limit<x.total THEN (SELECT private.pay_workbench_modal_cursor_encode_v2(
    v_binding||jsonb_build_object('last_identity',r.identity)) FROM page r ORDER BY r.ordinal DESC LIMIT 1) END,
  'rows',COALESCE((SELECT jsonb_agg(jsonb_build_object('identity',r.identity,'issue_state',r.issue_state,
    'title_message_id',r.title_message_id,'title',r.title,'affected_candidate_count',r.affected_candidate_count,
    'affected_payment_count',r.affected_payment_count,'affected_payment_count_complete',r.affected_payment_count_complete)
    ORDER BY r.ordinal) FROM page r),'[]'::jsonb),
  'updating_count',x.updating_count,'updating_has_more',x.updating_count>100,
  'updating_next_cursor',CASE WHEN x.updating_count>100 THEN (SELECT private.pay_workbench_modal_cursor_encode_v2(
    v_binding||jsonb_build_object('view','UPDATING','search','','sort_key','TITLE','sort_direction','ASC','limit',100,
      'last_identity',u.identity)) FROM updating u WHERE u.ordinal=100) END,
  'updating',COALESCE((SELECT jsonb_agg(jsonb_build_object('identity',u.identity,'issue_state','UPDATING',
    'title_message_id',u.title_message_id,'title',u.title,'affected_candidate_count',u.affected_candidate_count,
    'affected_payment_count',u.affected_payment_count,'affected_payment_count_complete',u.affected_payment_count_complete)
    ORDER BY u.ordinal) FROM updating u WHERE u.ordinal<=100),'[]'::jsonb),
  'valid_position',x.position IS NOT NULL AND x.position%p_limit=0
    AND (v_last IS NULL OR (x.position>0 AND x.position<x.total)),
  'valid_tasks',(SELECT count(*)=count(DISTINCT t.identity) AND COALESCE(bool_and((
    t.identity ~ '^[a-f0-9]{64}$' AND NULLIF(btrim(t.title),'') IS NOT NULL AND t.affected_candidate_count>0
    AND t.affected_payment_count_complete IS NOT NULL
    AND CASE WHEN t.affected_payment_count_complete THEN t.affected_payment_count>=0 ELSE t.affected_payment_count IS NULL END) IS TRUE),true) FROM tasks t))
 INTO v_page FROM stats x;
 IF v_page->'valid_position' IS DISTINCT FROM 'true'::jsonb THEN
  RAISE EXCEPTION 'BANKING_PAY_V2_STALE_CURSOR' USING ERRCODE='P0001';END IF;
 IF v_page->'valid_tasks' IS DISTINCT FROM 'true'::jsonb THEN
  RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_TASK_SUMMARY' USING ERRCODE='P0001';END IF;
 v_after:=private.pay_workbench_modal_context_v2(p_session_id,p_options_json,p_actor_user_id);
 v_reply:=(v_page-'valid_position'-'valid_tasks')||jsonb_build_object('ok',true,'contract','BANKING_PAY_MODAL_STRUCTURE_V2',
  'contract_version',1,'session_id',s.id,'session_version',v_after.version,'progress_counter_version',v_after.progress_counter_version,
  'scope_hash',p_options_json->>'scope_hash');
 IF octet_length(convert_to(v_reply::text,'UTF8'))>256*1024 THEN
  RAISE EXCEPTION 'BANKING_PAY_V2_ISSUES_TOO_LARGE' USING ERRCODE='P0001';END IF;
 RETURN v_reply;
END;
$function$;
ALTER FUNCTION public.pay_workbench_session_get_action_required_page_v1(uuid,jsonb,uuid,text,text,text,integer,text,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_session_get_action_required_page_v1(uuid,jsonb,uuid,text,text,text,integer,text,text) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.pay_workbench_session_get_action_required_page_v1(uuid,jsonb,uuid,text,text,text,integer,text,text) TO service_role;
NOTIFY pgrst,'reload schema';

commit;
