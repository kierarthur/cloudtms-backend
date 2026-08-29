-- Exact passive issue membership and compact server paging. This read grants
-- no action and does not change the original payload, economics or selection.

\set ON_ERROR_STOP on

begin;

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_blocked_summaries_v2(
 p_session public.banking_pay_workbench_sessions,p_channel text,p_provider text,p_environment text,
 p_progress jsonb,p_can_open_owner boolean,p_can_refresh boolean
) RETURNS TABLE(identity text,candidate_id uuid,candidate_name text,candidate_reference text,
 source_kind text,preview_row_id uuid,reason text,reason_message_id text,affected_display_amount text,
 search_text text)
LANGUAGE sql STABLE SECURITY INVOKER SET search_path TO ''
AS $function$
 WITH refs AS MATERIALIZED (
  SELECT i.* FROM private.pay_workbench_modal_issue_index_v2(p_session,p_channel,p_provider,p_environment,
   p_progress,p_can_open_owner,p_can_refresh) i WHERE i.issue_state='BLOCKED'
 ), bank AS MATERIALIZED (
  SELECT b.* FROM private.pay_workbench_modal_bank_issue_members_v2(p_session,p_channel,p_provider,p_environment,p_can_open_owner) b
  WHERE EXISTS(SELECT 1 FROM refs r WHERE r.task_family='BANK_ACCOUNT' AND r.task_key=b.task_key)
 ), source AS MATERIALIZED (
  SELECT s.* FROM private.pay_workbench_modal_source_issue_members_v2(p_session,p_channel,p_progress,p_can_refresh) s
  WHERE s.source_kind='SOURCE_PROGRESS'
 ), inputs AS MATERIALIZED (
  SELECT r.*,COALESCE(NULLIF(btrim(c.display_name),''),NULLIF(btrim(c.tms_ref),''),c.id::text) AS candidate_name,
   COALESCE(c.tms_ref,'') AS candidate_reference,
   CASE r.task_family WHEN 'BANK_ACCOUNT' THEN b.source_payload WHEN 'SOURCE_PROGRESS' THEN s.source_payload
    WHEN 'PASSIVE_PAYMENT' THEN private.pay_workbench_modal_row_payload_v2(p) END AS payload,
   CASE r.task_family WHEN 'BANK_ACCOUNT' THEN b.task_json WHEN 'SOURCE_PROGRESS' THEN s.task_json
    WHEN 'PASSIVE_PAYMENT' THEN '{}'::jsonb END AS meta
  FROM refs r JOIN public.candidates c ON c.id=r.candidate_id
  LEFT JOIN public.banking_pay_workbench_preview_rows p ON r.task_family='PASSIVE_PAYMENT' AND p.id=r.preview_row_id
   AND p.session_id=p_session.id AND p.session_version=p_session.version
  LEFT JOIN bank b ON r.task_family='BANK_ACCOUNT' AND b.task_key=r.task_key AND b.candidate_id=r.candidate_id
   AND b.source_kind=r.source_kind AND b.preview_row_id IS NOT DISTINCT FROM r.preview_row_id
   AND (b.task_json->>'source_ordinal')::bigint IS NOT DISTINCT FROM r.source_ordinal
  LEFT JOIN source s ON r.task_family='SOURCE_PROGRESS' AND s.task_key=r.task_key AND s.candidate_id=r.candidate_id
 ), displayed AS MATERIALIZED (
  SELECT i.*,private.pay_workbench_modal_blocked_presentation_v2(i.payload,i.meta) AS display FROM inputs i
 )
 SELECT d.identity,d.candidate_id,d.candidate_name,d.candidate_reference,d.source_kind,d.preview_row_id,
  d.display->>'reason',d.display->>'reason_message_id',CASE WHEN d.preview_row_id IS NOT NULL THEN d.display->>'affected_display_amount' END,
  lower(d.candidate_name||chr(10)||d.candidate_reference||chr(10)||COALESCE(d.display->>'reason',''))
 FROM displayed d;
$function$;
ALTER FUNCTION private.pay_workbench_modal_blocked_summaries_v2(public.banking_pay_workbench_sessions,text,text,text,jsonb,boolean,boolean) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_blocked_summaries_v2(public.banking_pay_workbench_sessions,text,text,text,jsonb,boolean,boolean) FROM PUBLIC, anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION public.pay_workbench_session_get_blocked_page_v1(
 p_session_id uuid,p_options_json jsonb,p_actor_user_id uuid,p_sort_key text DEFAULT 'CANDIDATE',
 p_sort_direction text DEFAULT 'ASC',p_cursor text DEFAULT NULL,p_limit integer DEFAULT 100,p_search text DEFAULT ''
) RETURNS jsonb LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path TO ''
AS $function$
DECLARE s public.banking_pay_workbench_sessions%ROWTYPE;v_after public.banking_pay_workbench_sessions%ROWTYPE;
 v_binding jsonb;v_cursor jsonb;v_last text;v_page jsonb;v_reply jsonb;v_progress jsonb;v_provider text;v_environment text;
BEGIN
 PERFORM public.banking_pay_hot_path_budget_apply('PREVIEW_PROGRESS');
 IF p_sort_key IS NULL OR p_sort_key NOT IN ('CANDIDATE','REASON','AMOUNT') OR p_sort_direction IS NULL
  OR p_sort_direction NOT IN ('ASC','DESC') OR p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 100 OR p_search IS NULL
  OR char_length(p_search)>200 OR p_search<>btrim(p_search) THEN
  RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_INPUT' USING ERRCODE='22023';END IF;
 IF char_length(p_search)+(SELECT count(*) FROM generate_series(1,char_length(p_search)) n WHERE ascii(substr(p_search,n,1))>65535)>200
  OR EXISTS(SELECT 1 FROM generate_series(1,char_length(p_search)) n WHERE ascii(substr(p_search,n,1)) BETWEEN 1 AND 31
   OR ascii(substr(p_search,n,1))=127) THEN RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_INPUT' USING ERRCODE='22023';END IF;
 s:=private.pay_workbench_modal_context_v2(p_session_id,p_options_json,p_actor_user_id);
 v_binding:=jsonb_build_object('kind','BLOCKED_ITEMS','session_id',s.id,'session_version',s.version,
  'progress_counter_version',s.progress_counter_version,'scope_hash',p_options_json->>'scope_hash',
  'sort_key',p_sort_key,'sort_direction',p_sort_direction,'search',p_search,'limit',p_limit);
 v_cursor:=private.pay_workbench_modal_cursor_decode_v2(p_cursor,v_binding);
 IF v_cursor IS NOT NULL AND (jsonb_typeof(v_cursor->'last_identity') IS DISTINCT FROM 'string'
  OR v_cursor->>'last_identity' !~ '^[a-f0-9]{64}$') THEN RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_CURSOR' USING ERRCODE='22023';END IF;
 v_last:=v_cursor->>'last_identity';
 SELECT UPPER(BTRIM(COALESCE(d.rail_provider_default,'CSV'))),UPPER(BTRIM(COALESCE(d.rail_env_default,'PROD')))
 INTO v_provider,v_environment FROM public.settings_defaults d WHERE d.id=1;
 IF NOT FOUND OR NULLIF(v_provider,'') IS NULL OR NULLIF(v_environment,'') IS NULL THEN
  RAISE EXCEPTION 'BANKING_PAY_V2_DEPENDENCY_UNAVAILABLE' USING ERRCODE='P0001';END IF;
 -- Consume only the original progress recommendation, never zero-count Draft
 -- readiness or an amount inferred from this limited list.
 v_progress:=private.pay_workbench_modal_draft_gate_v2(p_session_id,0);
 WITH items AS MATERIALIZED (
  SELECT i.* FROM private.pay_workbench_modal_blocked_summaries_v2(s,p_options_json->>'pay_channel_scope',
   v_provider,v_environment,v_progress,true,true) i
 ), facts AS MATERIALIZED (
  SELECT i.*,CASE p_sort_key WHEN 'CANDIDATE' THEN lower(i.candidate_name)||chr(10)||lower(i.candidate_reference)
    WHEN 'REASON' THEN lower(i.reason) END COLLATE "C" AS sort_text,
   CASE WHEN p_sort_key='AMOUNT' THEN i.affected_display_amount::numeric END AS sort_number
  FROM items i WHERE p_search='' OR position(lower(p_search) IN i.search_text)>0
 ), ranked AS MATERIALIZED (
  SELECT f.*,row_number() OVER(ORDER BY (p_sort_key='AMOUNT' AND f.sort_number IS NULL),
   CASE WHEN p_sort_direction='ASC' THEN f.sort_text END COLLATE "C" ASC,
   CASE WHEN p_sort_direction='DESC' THEN f.sort_text END COLLATE "C" DESC,
   CASE WHEN p_sort_direction='ASC' THEN f.sort_number END ASC,
   CASE WHEN p_sort_direction='DESC' THEN f.sort_number END DESC,f.identity COLLATE "C") AS ordinal
  FROM facts f
 ), boundary AS (
  SELECT r.* FROM ranked r WHERE r.identity=v_last
 ), page AS MATERIALIZED (
  SELECT r.* FROM ranked r WHERE v_last IS NULL OR EXISTS(SELECT 1 FROM boundary b WHERE
   CASE WHEN p_sort_key<>'AMOUNT' THEN
    (CASE WHEN p_sort_direction='ASC' THEN r.sort_text>b.sort_text ELSE r.sort_text<b.sort_text END)
     OR (r.sort_text=b.sort_text AND r.identity COLLATE "C">b.identity COLLATE "C")
   ELSE
    (r.sort_number IS NULL AND b.sort_number IS NOT NULL)
    OR (r.sort_number IS NOT NULL AND b.sort_number IS NOT NULL AND
     CASE WHEN p_sort_direction='ASC' THEN r.sort_number>b.sort_number ELSE r.sort_number<b.sort_number END)
    OR (r.sort_number IS NOT DISTINCT FROM b.sort_number AND r.identity COLLATE "C">b.identity COLLATE "C") END)
  ORDER BY r.ordinal LIMIT p_limit
 ), stats AS (
  SELECT (SELECT count(*) FROM facts) AS total,(SELECT count(*) FROM items) AS scope_count,
   CASE WHEN v_last IS NULL THEN 0::bigint ELSE (SELECT b.ordinal FROM boundary b) END AS position
 )
 SELECT jsonb_build_object('search',p_search,'sort_key',p_sort_key,'sort_direction',p_sort_direction,
  'total_count',x.total,'scope_count',x.scope_count,'page_number',CASE WHEN x.total=0 THEN 0 ELSE x.position/p_limit+1 END,
  'has_previous',x.position>0,'has_more',x.position+p_limit<x.total,
  'previous_cursor',CASE WHEN x.position>p_limit THEN (SELECT private.pay_workbench_modal_cursor_encode_v2(
    v_binding||jsonb_build_object('last_identity',r.identity)) FROM ranked r WHERE r.ordinal=x.position-p_limit) END,
  'next_cursor',CASE WHEN x.position+p_limit<x.total THEN (SELECT private.pay_workbench_modal_cursor_encode_v2(
    v_binding||jsonb_build_object('last_identity',r.identity)) FROM page r ORDER BY r.ordinal DESC LIMIT 1) END,
  'rows',COALESCE((SELECT jsonb_agg(jsonb_build_object('identity',r.identity,'candidate_id',r.candidate_id,
    'candidate_name',r.candidate_name,'candidate_reference',r.candidate_reference,'reason',r.reason,
    'reason_message_id',r.reason_message_id,'affected_display_amount',r.affected_display_amount,
    'source_kind',r.source_kind,'preview_row_id',r.preview_row_id) ORDER BY r.ordinal) FROM page r),'[]'::jsonb),
  'valid_position',x.position IS NOT NULL AND x.position%p_limit=0 AND (v_last IS NULL OR (x.position>0 AND x.position<x.total)),
  'valid_items',(SELECT count(*)=count(DISTINCT i.identity) AND COALESCE(bool_and((
    i.identity ~ '^[a-f0-9]{64}$' AND i.candidate_id IS NOT NULL AND NULLIF(btrim(i.candidate_name),'') IS NOT NULL
    AND i.candidate_reference IS NOT NULL AND NULLIF(btrim(i.reason),'') IS NOT NULL
    AND (i.affected_display_amount IS NULL OR i.affected_display_amount ~ '^-?[0-9]{1,16}[.][0-9]{2}$')
    AND CASE WHEN i.source_kind='PREVIEW_ROW' THEN i.preview_row_id IS NOT NULL
      ELSE i.source_kind IN ('STORED_PAYEE','SOURCE_PROGRESS') AND i.preview_row_id IS NULL END) IS TRUE),true) FROM items i))
 INTO v_page FROM stats x;
 IF v_page->'valid_position' IS DISTINCT FROM 'true'::jsonb THEN RAISE EXCEPTION 'BANKING_PAY_V2_STALE_CURSOR' USING ERRCODE='P0001';END IF;
 IF v_page->'valid_items' IS DISTINCT FROM 'true'::jsonb THEN RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_BLOCKED_SUMMARY' USING ERRCODE='P0001';END IF;
 v_after:=private.pay_workbench_modal_context_v2(p_session_id,p_options_json,p_actor_user_id);
 v_reply:=(v_page-'valid_position'-'valid_items')||jsonb_build_object('ok',true,'contract','BANKING_PAY_MODAL_STRUCTURE_V2',
  'contract_version',1,'session_id',s.id,'session_version',v_after.version,'progress_counter_version',v_after.progress_counter_version,
  'scope_hash',p_options_json->>'scope_hash');
 IF octet_length(convert_to(v_reply::text,'UTF8'))>256*1024 THEN RAISE EXCEPTION 'BANKING_PAY_V2_ISSUES_TOO_LARGE' USING ERRCODE='P0001';END IF;
 RETURN v_reply;
END;
$function$;
ALTER FUNCTION public.pay_workbench_session_get_blocked_page_v1(uuid,jsonb,uuid,text,text,text,integer,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_session_get_blocked_page_v1(uuid,jsonb,uuid,text,text,text,integer,text) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.pay_workbench_session_get_blocked_page_v1(uuid,jsonb,uuid,text,text,text,integer,text) TO service_role;
NOTIFY pgrst,'reload schema';

commit;
