-- Repeatable CloudTMS function authority: complete group facts on Candidate Banking rows.
-- This is presentation/selection identity only. Existing Ready, selection, amount,
-- recovery and Draft authorities remain unchanged.

\set ON_ERROR_STOP on

begin;

CREATE OR REPLACE FUNCTION public.pay_workbench_session_get_candidate_ready_page_v1(
  p_session_id uuid, p_candidate_id uuid, p_options_json jsonb, p_actor_user_id uuid,
  p_cursor text DEFAULT NULL, p_limit integer DEFAULT 100
) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER
SET search_path TO '' SET statement_timeout TO '3s' SET lock_timeout TO '1s'
AS $function$
DECLARE
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_limit integer := LEAST(GREATEST(COALESCE(p_limit, 100), 1), 100);
  v_binding jsonb;
  v_cursor jsonb;
  v_last_id uuid;
  v_last_ordinal bigint;
  v_page jsonb;
  v_candidate jsonb;
  v_after public.banking_pay_workbench_sessions%ROWTYPE;
  v_scope public.banking_pay_workbench_session_scope%ROWTYPE;
  v_anchor boolean := false;
BEGIN
  PERFORM public.banking_pay_hot_path_budget_apply('PREVIEW_PROGRESS');
  v_session := private.pay_workbench_modal_context_v2(p_session_id, p_options_json, p_actor_user_id);
  IF p_candidate_id IS NULL OR p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 100 THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_INPUT' USING ERRCODE = '22023';
  END IF;
  SELECT * INTO v_scope FROM public.banking_pay_workbench_session_scope
  WHERE session_id = p_session_id AND candidate_id = p_candidate_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_CANDIDATE_NOT_CURRENT' USING ERRCODE = 'P0001';
  END IF;
  IF NOT EXISTS(SELECT 1 FROM private.pay_workbench_modal_source_progress_facts_v2(p_session_id,v_session.version) f
    WHERE f.candidate_id=p_candidate_id AND f.source_state='CURRENT') THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_NOT_READY' USING ERRCODE = 'P0001';
  END IF;
  v_binding := jsonb_build_object('contract', 'BANKING_PAY_MODAL_STRUCTURE_V2', 'kind', 'READY',
    'session_id', p_session_id, 'candidate_id', p_candidate_id, 'session_version', v_session.version,
    'progress_counter_version', v_session.progress_counter_version, 'scope_hash', p_options_json->>'scope_hash');
  SELECT private.pay_workbench_modal_candidate_row_v2(to_jsonb(f),v_binding-'kind'-'candidate_id')
    INTO v_candidate
    FROM private.pay_workbench_modal_candidate_facts_v2(v_session,p_options_json->>'pay_channel_scope') f
    WHERE f.candidate_id=p_candidate_id;
  v_cursor := private.pay_workbench_modal_cursor_decode_v2(p_cursor, '{}'::jsonb);
  IF v_cursor->>'kind'='READY_PAGE_ANCHOR' THEN
    v_anchor:=true;
    v_cursor:=private.pay_workbench_modal_cursor_decode_v2(p_cursor,
      (v_binding-'progress_counter_version') || jsonb_build_object('kind','READY_PAGE_ANCHOR','page_limit',v_limit));
    IF COALESCE(v_cursor->>'progress_counter_version','') !~ '^[0-9]{1,16}$'
      OR (v_cursor->>'progress_counter_version')::bigint>v_session.progress_counter_version THEN
      RAISE EXCEPTION 'BANKING_PAY_V2_STALE_CURSOR' USING ERRCODE='P0001';
    END IF;
  ELSE
    v_cursor:=private.pay_workbench_modal_cursor_decode_v2(p_cursor,v_binding);
  END IF;
  IF v_cursor IS NOT NULL THEN
    IF COALESCE(v_cursor->>'last_id', '') !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       OR COALESCE(v_cursor->>'last_ordinal', '') !~ '^[0-9]{1,18}$' THEN
      RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_CURSOR' USING ERRCODE = '22023';
    END IF;
    v_last_id := (v_cursor->>'last_id')::uuid;
    v_last_ordinal := (v_cursor->>'last_ordinal')::bigint;
  END IF;
  WITH scoped_rows AS MATERIALIZED (
    SELECT r.* FROM private.pay_workbench_modal_eligible_rows_v2(p_session_id, v_session.version, 'canonical_preview_lines') AS r
    WHERE r.candidate_id = p_candidate_id AND v_candidate IS NOT NULL
      AND private.pay_workbench_modal_row_matches_scope_v2(r.row_json || jsonb_build_object('candidate_id',r.candidate_id), v_session.filters_json,
        p_options_json->>'pay_channel_scope', 'canonical_preview_lines')
      AND NOT private.pay_workbench_modal_hidden_v2(r.row_json)
  ), group_members AS MATERIALIZED (
    SELECT g.* FROM private.pay_workbench_modal_ready_group_members_v2(
      v_session,p_options_json->>'pay_channel_scope',p_candidate_id) AS g
  ), group_facts AS MATERIALIZED (
    SELECT g.group_kind,g.group_key,count(*)::integer AS member_count,
      count(*) FILTER (WHERE g.selected)::integer AS selected_count,
      sum(private.pay_workbench_modal_line_display_amount_v2(private.pay_workbench_modal_row_payload_v2(
        r::public.banking_pay_workbench_preview_rows))) AS full_display_amount,
      COALESCE(sum(private.pay_workbench_modal_line_display_amount_v2(private.pay_workbench_modal_row_payload_v2(
        r::public.banking_pay_workbench_preview_rows))) FILTER (WHERE g.selected),0) AS selected_display_amount
    FROM group_members g JOIN scoped_rows r ON r.id=g.row_id GROUP BY g.group_kind,g.group_key
  ), ranked AS MATERIALIZED (
    SELECT r.id,r.row_ordinal,r::public.banking_pay_workbench_preview_rows AS source_row,
      g.group_kind,g.group_key,f.member_count,f.selected_count,f.full_display_amount,f.selected_display_amount,
      row_number() OVER (ORDER BY r.row_ordinal,r.id) AS page_order
    FROM scoped_rows r
    LEFT JOIN group_members g ON g.row_id=r.id
    LEFT JOIN group_facts f ON f.group_kind=g.group_kind AND f.group_key=g.group_key
  ), anchor_page AS (
    SELECT ((COALESCE((SELECT page_order FROM ranked WHERE id=v_last_id),
      (SELECT min(page_order) FROM ranked WHERE (row_ordinal,id)>(v_last_ordinal,v_last_id)),
      (SELECT max(page_order) FROM ranked),1)-1)/v_limit)*v_limit+1 AS first_order
  ), page_rows AS MATERIALIZED (
    SELECT r.* FROM ranked r CROSS JOIN anchor_page a
    WHERE (v_anchor AND r.page_order>=a.first_order)
      OR (NOT v_anchor AND (v_last_id IS NULL OR (r.row_ordinal,r.id)>(v_last_ordinal,v_last_id)))
    ORDER BY r.page_order LIMIT (v_limit + 1)
  ), limited_rows AS MATERIALIZED (
    SELECT r.* FROM page_rows AS r ORDER BY r.row_ordinal, r.id LIMIT v_limit
  )
  SELECT jsonb_build_object(
    'rows', COALESCE((SELECT jsonb_agg(private.pay_workbench_modal_row_payload_v2(r.source_row)
      || jsonb_build_object('identity',r.id::text,
        'selection_group_kind',r.group_kind,
        'selection_group_key',r.group_key,
        'selection_group_member_count',COALESCE(r.member_count,0),
        'selection_group_selected_count',COALESCE(r.selected_count,0),
        'selection_group_display_amount',CASE WHEN r.group_kind IS NOT NULL THEN to_char(r.full_display_amount,'FM999999999999999990.00') END,
        'selection_group_selected_display_amount',CASE WHEN r.group_kind IS NOT NULL THEN to_char(r.selected_display_amount,'FM999999999999999990.00') END,
        'selection_group_state',CASE WHEN r.group_kind IS NULL THEN NULL
          WHEN r.selected_count=0 THEN 'NONE'
          WHEN r.selected_count=r.member_count THEN 'ALL' ELSE 'SOME' END)
      ORDER BY r.row_ordinal,r.id) FROM limited_rows AS r), '[]'::jsonb),
    'total_count', (SELECT count(*) FROM scoped_rows),
    'has_more', (SELECT count(*) > v_limit FROM page_rows),
    'next_cursor', CASE WHEN (SELECT count(*) > v_limit FROM page_rows) THEN (
      SELECT private.pay_workbench_modal_cursor_encode_v2(v_binding || jsonb_build_object('last_id',r.id,'last_ordinal',r.row_ordinal))
      FROM limited_rows AS r ORDER BY r.row_ordinal DESC,r.id DESC LIMIT 1
    ) ELSE NULL END,
    'page_number',COALESCE((SELECT ((min(page_order)-1)/v_limit)+1 FROM limited_rows),0),
    'has_previous',COALESCE((SELECT min(page_order)>1 FROM limited_rows),false),
    'previous_cursor',(
      SELECT private.pay_workbench_modal_cursor_encode_v2(v_binding || jsonb_build_object('last_id',r.id,'last_ordinal',r.row_ordinal))
      FROM ranked r WHERE r.page_order=(SELECT min(page_order) FROM limited_rows)-v_limit-1
    ),
    'page_anchor',(
      SELECT private.pay_workbench_modal_cursor_encode_v2(v_binding || jsonb_build_object('kind','READY_PAGE_ANCHOR',
        'page_limit',v_limit,'last_id',r.id,'last_ordinal',r.row_ordinal))
      FROM limited_rows r ORDER BY r.page_order LIMIT 1
    ),
    'cursor_identity_current',v_last_id IS NULL OR v_anchor OR EXISTS (
      SELECT 1 FROM scoped_rows AS r WHERE r.id=v_last_id AND r.row_ordinal=v_last_ordinal
    )
  ) INTO v_page;
  IF v_page->>'cursor_identity_current'='false'
     OR ((v_page->>'total_count')::bigint>0 AND jsonb_array_length(v_page->'rows')=0) THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_STALE_CURSOR' USING ERRCODE='P0001';
  END IF;
  v_after:=private.pay_workbench_modal_context_v2(p_session_id,p_options_json,p_actor_user_id);
  v_page:=(v_page-'cursor_identity_current') || jsonb_build_object(
    'ok',true,'contract','BANKING_PAY_MODAL_STRUCTURE_V2','contract_version',1,
    'session_id',p_session_id,'candidate_id',p_candidate_id,'candidate',v_candidate,
    'session_version',v_after.version,'progress_counter_version',v_after.progress_counter_version,
    'scope_hash',p_options_json->>'scope_hash'
  );
  IF octet_length(v_page::text)>512*1024 THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_READY_TOO_LARGE' USING ERRCODE='P0001';
  END IF;
  RETURN v_page;
END;
$function$;
ALTER FUNCTION public.pay_workbench_session_get_candidate_ready_page_v1(uuid,uuid,jsonb,uuid,text,integer) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_session_get_candidate_ready_page_v1(uuid,uuid,jsonb,uuid,text,integer) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.pay_workbench_session_get_candidate_ready_page_v1(uuid,uuid,jsonb,uuid,text,integer) TO service_role;

NOTIFY pgrst, 'reload schema';

commit;
