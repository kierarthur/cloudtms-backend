-- Bounded presentation adapter over the unchanged exact-row selection owner.
\set ON_ERROR_STOP on
begin;

CREATE OR REPLACE FUNCTION public.pay_workbench_session_set_ready_rows_v1(
  p_session_id uuid,p_candidate_id uuid,p_options_json jsonb,p_actor_user_id uuid,
  p_preview_row_ids jsonb,p_selected boolean,p_request_id uuid,p_expected_view_digest text,
  p_open_ready_json jsonb DEFAULT NULL
) RETURNS jsonb LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path TO ''
AS $function$
DECLARE
  v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_before jsonb;v_result jsonb;v_movements jsonb;v_reply jsonb;v_ids uuid[];
BEGIN
  IF p_candidate_id IS NULL OR p_request_id IS NULL OR p_selected IS NULL
     OR COALESCE(p_expected_view_digest,'') !~ '^[a-f0-9]{64}$'
     OR jsonb_typeof(p_preview_row_ids) IS DISTINCT FROM 'array' THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_INPUT' USING ERRCODE='22023';
  END IF;
  IF jsonb_array_length(p_preview_row_ids) NOT BETWEEN 1 AND 100 OR EXISTS(
    SELECT 1 FROM jsonb_array_elements(p_preview_row_ids) x(value)
    WHERE jsonb_typeof(value) IS DISTINCT FROM 'string'
      OR (value#>>'{}') !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$') THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_INPUT' USING ERRCODE='22023';
  END IF;
  SELECT array_agg(DISTINCT value::uuid ORDER BY value::uuid) INTO v_ids FROM jsonb_array_elements_text(p_preview_row_ids) x(value);
  IF cardinality(v_ids) IS DISTINCT FROM jsonb_array_length(p_preview_row_ids) THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_INPUT' USING ERRCODE='22023';
  END IF;
  IF p_open_ready_json IS NOT NULL AND p_open_ready_json IS DISTINCT FROM 'null'::jsonb THEN
    IF jsonb_typeof(p_open_ready_json) IS DISTINCT FROM 'object' THEN
      RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_INPUT' USING ERRCODE='22023';
    END IF;
    IF NOT(p_open_ready_json ? 'cursor') OR jsonb_typeof(p_open_ready_json->'limit') IS DISTINCT FROM 'number'
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
  -- Validate the existing admin/context boundary before taking the session
  -- lock. Re-prove that same context after waiting; never use the earlier copy.
  PERFORM private.pay_workbench_modal_context_v2(p_session_id,p_options_json,p_actor_user_id);
  PERFORM 1 FROM public.banking_pay_workbench_sessions WHERE id=p_session_id FOR UPDATE;
  v_session:=private.pay_workbench_modal_context_v2(p_session_id,p_options_json,p_actor_user_id);
  v_before:=private.pay_workbench_modal_candidate_state_v2(v_session,p_options_json->>'pay_channel_scope',p_candidate_id);
  IF v_before->>'view_digest' IS DISTINCT FROM p_expected_view_digest THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_STALE_VIEW' USING ERRCODE='P0001';
  END IF;
  IF cardinality(v_ids) IS DISTINCT FROM (SELECT count(*) FROM private.pay_workbench_modal_ready_members_v2(
    v_session,p_options_json->>'pay_channel_scope') r WHERE r.candidate_id=p_candidate_id AND r.row_id=ANY(v_ids)) THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_ROW_NOT_SELECTABLE' USING ERRCODE='P0001';
  END IF;
  DROP TABLE IF EXISTS pg_temp._bpay_modal_row_before_v2;
  CREATE TEMPORARY TABLE pg_temp._bpay_modal_row_before_v2 ON COMMIT DROP AS
    SELECT r.id,private.pay_workbench_preview_effective_section_v1(r.section,r.row_json) AS effective_section
    FROM public.banking_pay_workbench_preview_rows r WHERE r.session_id=p_session_id AND r.session_version=v_session.version;
  -- Exactly the old ROW_PATCH shape and original owner: no replacement
  -- selection algorithm, extra receipt, audit or per-candidate mutation loop.
  v_result:=public.pay_workbench_session_set_selected_rows(p_session_id,jsonb_build_object(
    'section','canonical_preview_lines','expected_session_version',v_session.version,
    'expected_progress_counter_version',v_session.progress_counter_version,
    CASE WHEN p_selected THEN 'select_preview_row_ids' ELSE 'deselect_preview_row_ids' END,p_preview_row_ids),p_actor_user_id);
  IF v_result->>'ok' IS DISTINCT FROM 'true'
    OR COALESCE(v_result->>'progress_counter_version','') !~ '^[0-9]{1,16}$'
    OR (v_result->>'progress_counter_version')::bigint<=v_session.progress_counter_version THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_DEPENDENCY_UNAVAILABLE' USING ERRCODE='P0001';
  END IF;
  SELECT COALESCE(jsonb_agg(jsonb_build_object('identity',r.id,'candidate_id',r.candidate_id,
    'row_key',r.row_key,'key_type',r.key_type,'key_value',r.key_value,'from',b.effective_section,
    'to',private.pay_workbench_preview_effective_section_v1(r.section,r.row_json),'selected',r.selected)
    ORDER BY r.row_ordinal,r.id),'[]'::jsonb) INTO v_movements
    FROM public.banking_pay_workbench_preview_rows r JOIN pg_temp._bpay_modal_row_before_v2 b ON b.id=r.id
    WHERE r.session_id=p_session_id AND r.session_version=v_session.version
      AND private.pay_workbench_preview_effective_section_v1(r.section,r.row_json) IS DISTINCT FROM b.effective_section;
  -- Even an already-selected original ROW_PATCH advances audit/revision.
  -- Correlation is not a new idempotency receipt: old-context repeats reject.
  v_result:=v_result||jsonb_build_object('presentation_before',v_before,'state_changed',true,'movements',v_movements);
  v_reply:=private.pay_workbench_modal_selection_response_finish_v2(p_session_id,p_candidate_id,p_options_json,p_actor_user_id,
    p_request_id,p_expected_view_digest,p_open_ready_json,v_result)||jsonb_build_object('selection_scope','EXACT_READY_ROWS');
  IF octet_length(convert_to((v_reply-'ready_page')::text,'UTF8'))>32*1024
    OR octet_length(convert_to(v_reply::text,'UTF8'))>544*1024 THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_SELECTION_TOO_LARGE' USING ERRCODE='P0001';
  END IF;
  RETURN v_reply;
END;
$function$;
ALTER FUNCTION public.pay_workbench_session_set_ready_rows_v1(uuid,uuid,jsonb,uuid,jsonb,boolean,uuid,text,jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_session_set_ready_rows_v1(uuid,uuid,jsonb,uuid,jsonb,boolean,uuid,text,jsonb) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.pay_workbench_session_set_ready_rows_v1(uuid,uuid,jsonb,uuid,jsonb,boolean,uuid,text,jsonb) TO service_role;
NOTIFY pgrst, 'reload schema';
commit;
