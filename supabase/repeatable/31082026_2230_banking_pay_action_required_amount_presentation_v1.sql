-- Correct the read-only Action Required amount for a finance case whose
-- current-run recoverable value is zero but whose existing unresolved amount
-- still needs a decision. This does not change recovery, selection, Draft or
-- payment economics.

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
  OR p_sort_key NOT IN ('TITLE','CANDIDATES','PAYMENTS','AMOUNT') OR p_sort_direction IS NULL
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
 ), issue_refs AS MATERIALIZED (
  SELECT i.identity,i.task_key FROM private.pay_workbench_modal_issue_index_v2(s,p_options_json->>'pay_channel_scope',
   v_provider,v_environment,v_progress,true,true) i WHERE i.issue_state IN ('ACTION_REQUIRED','UPDATING')
 ), members AS MATERIALIZED (
  SELECT f.task_key,f.candidate_id,f.preview_row_id,
   f.task_json->'context_only' IS DISTINCT FROM 'true'::jsonb AS affected,f.row_payload AS payload,
   COALESCE(NULLIF(f.task_json->>'task_family',''),NULLIF(f.task_json->>'family','')) AS task_family
  FROM private.pay_workbench_modal_finance_task_members_v2(s,p_options_json->>'pay_channel_scope') f
  UNION ALL
  SELECT b.task_key,b.candidate_id,b.preview_row_id,b.affected_by_task,b.source_payload,NULL::text AS task_family
  FROM private.pay_workbench_modal_bank_task_members_v2(s,p_options_json->>'pay_channel_scope',
   v_provider,v_environment,true) b
  UNION ALL
  SELECT x.task_key,x.candidate_id,x.preview_row_id,false,x.source_payload,NULL::text AS task_family
  FROM private.pay_workbench_modal_source_issue_members_v2(s,p_options_json->>'pay_channel_scope',v_progress,true) x
 ), presentation AS MATERIALIZED (
  SELECT i.identity,
   CASE WHEN t.affected_candidate_count=1 AND count(DISTINCT m.candidate_id)=1
    THEN min(COALESCE(NULLIF(btrim(c.display_name),''),NULLIF(btrim(c.tms_ref),''),c.id::text)) END AS candidate_name,
   CASE WHEN t.affected_candidate_count=1 AND count(DISTINCT m.candidate_id)=1
    THEN min(COALESCE(c.tms_ref,'')) END AS candidate_reference,
   CASE WHEN t.affected_payment_count_complete AND t.affected_payment_count=1
     AND count(DISTINCT m.preview_row_id) FILTER(WHERE m.affected)=1 THEN
    CASE WHEN max(NULLIF(m.payload->>'timesheet_id','')) FILTER(WHERE m.affected) IS NOT NULL
      THEN 'Timesheet payment' ELSE 'Payment' END END AS payment_label,
   CASE WHEN t.affected_payment_count_complete AND t.affected_payment_count=1
     AND count(DISTINCT m.preview_row_id) FILTER(WHERE m.affected)=1 THEN max(COALESCE(
    NULLIF(m.payload->>'week_ending_date',''),NULLIF(m.payload->>'linked_shift_date',''),
    NULLIF(m.payload->>'shift_date',''),NULLIF(m.payload->>'work_date',''))) FILTER(WHERE m.affected) END AS payment_date,
   CASE WHEN t.affected_payment_count_complete AND t.affected_payment_count=1
     AND count(DISTINCT m.preview_row_id) FILTER(WHERE m.affected)=1 THEN
    to_char(max(NULLIF(ROUND(CASE
      WHEN UPPER(COALESCE(m.task_family,''))='FINANCE_CASE'
       AND COALESCE(m.payload->>'amount_display','') ~ '^-?[0-9]{1,16}([.][0-9]{1,12})?$'
       AND ROUND(ABS((m.payload->>'amount_display')::numeric),2)=0
      THEN ABS(COALESCE(private.pay_workbench_modal_first_display_number_v2(
       ARRAY[
        m.payload->'case_resolution_summary'->'unresolved_taxable_amount_ex_vat',
        m.payload->'case_resolution_summary'->'unresolvedTaxableAmountExVat',
        m.payload->'case_resolution_summary_json'->'unresolved_taxable_amount_ex_vat',
        m.payload->'case_resolution_summary_json'->'unresolvedTaxableAmountExVat',
        m.payload->'row_json'->'case_resolution_summary'->'unresolved_taxable_amount_ex_vat',
        m.payload->'row_json'->'case_resolution_summary'->'unresolvedTaxableAmountExVat'
       ] || private.pay_workbench_modal_display_fields_v2(m.payload,ARRAY[
        'nominal_due_amount_ex_vat','nominalDueAmountExVat'
       ]) || ARRAY[
        m.payload->'case_resolution_summary'->'blocked_case_amount_ex_vat',
        m.payload->'case_resolution_summary'->'blockedCaseAmountExVat',
        m.payload->'case_resolution_summary'->'safe_amount_ex_vat',
        m.payload->'case_resolution_summary'->'safeAmountExVat'
       ],true),0))
      WHEN COALESCE(m.payload->>'amount_display','') ~ '^-?[0-9]{1,16}([.][0-9]{1,12})?$'
      THEN (m.payload->>'amount_display')::numeric
     END,2),0)) FILTER(WHERE m.affected),'FM9999999999999990.00') END AS affected_display_amount,
   CASE WHEN t.affected_payment_count_complete AND t.affected_payment_count=1
     AND count(DISTINCT m.preview_row_id) FILTER(WHERE m.affected)=1 THEN
    max(NULLIF(m.payload->>'timesheet_id','')) FILTER(WHERE m.affected) END AS linked_timesheet_id
  FROM issue_refs i JOIN tasks t ON t.identity=i.identity JOIN members m ON m.task_key=i.task_key
  JOIN public.candidates c ON c.id=m.candidate_id
  GROUP BY i.identity,t.affected_candidate_count,t.affected_payment_count_complete,t.affected_payment_count
 ), facts AS MATERIALIZED (
  SELECT t.*,p.candidate_name,p.candidate_reference,p.payment_label,
   CASE WHEN p.payment_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN p.payment_date END AS payment_date,
   CASE WHEN p.affected_display_amount ~ '^-?[0-9]{1,16}[.][0-9]{2}$' AND p.affected_display_amount<>'-0.00'
    THEN p.affected_display_amount END AS affected_display_amount,
   CASE WHEN p.linked_timesheet_id ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    THEN p.linked_timesheet_id END AS linked_timesheet_id,
   CASE p_sort_key
    WHEN 'TITLE' THEN lower(t.title) COLLATE "C"
    WHEN 'CANDIDATES' THEN CASE WHEN p.candidate_name IS NOT NULL
      THEN ('0|'||lower(p.candidate_name)||'|'||lower(COALESCE(p.candidate_reference,''))) COLLATE "C"
      ELSE '1|'||lpad(t.affected_candidate_count::text,20,'0') END
   END AS sort_text,
   CASE p_sort_key
    WHEN 'PAYMENTS' THEN t.affected_payment_count::numeric
    WHEN 'AMOUNT' THEN CASE WHEN p.affected_display_amount ~ '^-?[0-9]{1,16}[.][0-9]{2}$'
      AND p.affected_display_amount<>'-0.00' THEN p.affected_display_amount::numeric END
   END AS sort_number
  FROM tasks t LEFT JOIN presentation p ON p.identity=t.identity
  WHERE t.issue_state=p_view AND (p_search='' OR position(lower(p_search) IN t.search_text)>0)
 ), ranked AS MATERIALIZED (
  SELECT f.*,row_number() OVER(ORDER BY
   (CASE WHEN p_sort_key IN ('TITLE','CANDIDATES') THEN f.sort_text IS NULL ELSE f.sort_number IS NULL END) ASC,
   CASE WHEN p_sort_key IN ('TITLE','CANDIDATES') AND p_sort_direction='ASC' THEN f.sort_text END COLLATE "C" ASC,
   CASE WHEN p_sort_key IN ('TITLE','CANDIDATES') AND p_sort_direction='DESC' THEN f.sort_text END COLLATE "C" DESC,
   CASE WHEN p_sort_direction='ASC' THEN f.sort_number END ASC,
   CASE WHEN p_sort_direction='DESC' THEN f.sort_number END DESC,
   CASE WHEN p_sort_direction='ASC' THEN f.identity END COLLATE "C" ASC,
   CASE WHEN p_sort_direction='DESC' THEN f.identity END COLLATE "C" DESC) AS ordinal
  FROM facts f
 ), boundary AS (SELECT b.* FROM ranked b WHERE b.identity=v_last), page AS MATERIALIZED (
  SELECT r.* FROM ranked r WHERE v_last IS NULL OR EXISTS(SELECT 1 FROM boundary b WHERE r.ordinal>b.ordinal)
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
    'affected_payment_count',r.affected_payment_count,'affected_payment_count_complete',r.affected_payment_count_complete,
    'candidate_name',r.candidate_name,'candidate_reference',r.candidate_reference,'payment_label',r.payment_label,
    'payment_date',r.payment_date,'affected_display_amount',r.affected_display_amount,
    'linked_timesheet_id',r.linked_timesheet_id) ORDER BY r.ordinal) FROM page r),'[]'::jsonb),
  'updating_count',x.updating_count,'updating_has_more',x.updating_count>100,
  'updating_next_cursor',CASE WHEN x.updating_count>100 THEN (SELECT private.pay_workbench_modal_cursor_encode_v2(
    v_binding||jsonb_build_object('view','UPDATING','search','','sort_key','TITLE','sort_direction','ASC','limit',100,
      'last_identity',u.identity)) FROM updating u WHERE u.ordinal=100) END,
  'updating',COALESCE((SELECT jsonb_agg(jsonb_build_object('identity',u.identity,'issue_state','UPDATING',
    'title_message_id',u.title_message_id,'title',u.title,'affected_candidate_count',u.affected_candidate_count,
    'affected_payment_count',u.affected_payment_count,'affected_payment_count_complete',u.affected_payment_count_complete)
    ORDER BY u.ordinal) FROM updating u WHERE u.ordinal<=100),'[]'::jsonb),
  'valid_position',x.position IS NOT NULL AND x.position%p_limit=0 AND (v_last IS NULL OR (x.position>0 AND x.position<x.total)),
  'valid_tasks',(SELECT count(*)=count(DISTINCT t.identity) AND COALESCE(bool_and((
    t.identity ~ '^[a-f0-9]{64}$' AND NULLIF(btrim(t.title),'') IS NOT NULL AND t.affected_candidate_count>0
    AND t.affected_payment_count_complete IS NOT NULL
    AND CASE WHEN t.affected_payment_count_complete THEN t.affected_payment_count>=0 ELSE t.affected_payment_count IS NULL END
    ) IS TRUE),true) FROM tasks t),
  'valid_presentation',(SELECT COALESCE(bool_and((
    (t.candidate_name IS NULL OR (t.affected_candidate_count=1 AND NULLIF(btrim(t.candidate_name),'') IS NOT NULL))
    AND (t.candidate_reference IS NULL OR t.affected_candidate_count=1)
    AND (t.payment_label IS NULL OR (t.affected_payment_count_complete AND t.affected_payment_count=1
      AND NULLIF(btrim(t.payment_label),'') IS NOT NULL))
    AND (t.payment_date IS NULL OR (t.affected_payment_count_complete AND t.affected_payment_count=1
      AND t.payment_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'))
    AND (t.affected_display_amount IS NULL OR (t.affected_payment_count_complete AND t.affected_payment_count=1
      AND t.affected_display_amount ~ '^-?[0-9]{1,16}[.][0-9]{2}$' AND t.affected_display_amount<>'-0.00'))
    AND (t.linked_timesheet_id IS NULL OR (t.affected_payment_count_complete AND t.affected_payment_count=1
      AND t.linked_timesheet_id ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'))
    ) IS TRUE),true) FROM facts t))
 INTO v_page FROM stats x;
 IF v_page->'valid_position' IS DISTINCT FROM 'true'::jsonb THEN
  RAISE EXCEPTION 'BANKING_PAY_V2_STALE_CURSOR' USING ERRCODE='P0001';END IF;
 IF v_page->'valid_tasks' IS DISTINCT FROM 'true'::jsonb THEN
  RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_TASK_SUMMARY' USING ERRCODE='P0001';END IF;
 IF v_page->'valid_presentation' IS DISTINCT FROM 'true'::jsonb THEN
  RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_TASK_PRESENTATION' USING ERRCODE='P0001';END IF;
 v_after:=private.pay_workbench_modal_context_v2(p_session_id,p_options_json,p_actor_user_id);
 v_reply:=(v_page-'valid_position'-'valid_tasks'-'valid_presentation')||jsonb_build_object('ok',true,'contract','BANKING_PAY_MODAL_STRUCTURE_V2',
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
