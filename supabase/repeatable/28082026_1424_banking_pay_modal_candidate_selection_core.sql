\set ON_ERROR_STOP on
\ir 28082026_1427_banking_pay_modal_recovery_channel_scope.sql
\ir 28082026_2245_banking_pay_modal_candidate_state.sql
begin;
-- Internal branch of pay_workbench_session_set_selected_rows, not an alternate
-- financial/selection API. Public v2 response assembly remains separately gated.
-- Candidate and filtered-header intent share this one implementation. The
-- initial internal name is retained; no candidate-call fan-out is introduced.
CREATE OR REPLACE FUNCTION private.pay_workbench_modal_candidate_selection_apply_v2(
  p_session_id uuid, p_input jsonb, p_actor_user_id uuid
) RETURNS jsonb LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path TO ''
AS $function$
DECLARE
  v_group boolean:=COALESCE(p_input ? 'modal_group_intent_v2',false);
  v_global boolean:=NOT COALESCE(p_input ? 'modal_group_intent_v2',false)
    AND COALESCE(p_input ? 'modal_global_intent_v2',false);
  v_intent_key text:=CASE WHEN v_group THEN 'modal_group_intent_v2'
    WHEN v_global THEN 'modal_global_intent_v2' ELSE 'modal_candidate_intent_v2' END;
  v_intent jsonb:=p_input->v_intent_key;
  v_options jsonb; v_session public.banking_pay_workbench_sessions%ROWTYPE;
  v_scope public.banking_pay_workbench_session_scope%ROWTYPE;
  v_candidate uuid; v_request uuid; v_action text; v_channel text; v_selected boolean; v_state text;
  v_now timestamptz:=clock_timestamp(); v_fingerprint text; v_prior_receipt jsonb; v_result jsonb;
  v_revalidation jsonb; v_before_revision bigint; v_eligible_count bigint; v_change_count bigint;
  v_selected_ids jsonb; v_identities jsonb; v_selected_count bigint; v_session_ready boolean; v_blockers jsonb;
  v_movements jsonb; v_selection_changed bigint; v_final_count bigint;
  v_revalidation_candidate uuid; v_revalidation_count bigint:=0;
  v_presentation jsonb; v_open_ready jsonb; v_before_state jsonb;
  v_group_kind text;v_group_key text;v_selection_scope text;v_selection_origin text;
BEGIN
  IF p_session_id IS NULL OR jsonb_typeof(p_input) IS DISTINCT FROM 'object'
     OR jsonb_typeof(v_intent) IS DISTINCT FROM 'object'
     OR EXISTS(SELECT 1 FROM jsonb_object_keys(p_input) k(value) WHERE k.value<>v_intent_key)
     OR EXISTS(SELECT 1 FROM jsonb_object_keys(v_intent) k(value)
       WHERE k.value NOT IN ('request_id','action','options','presentation_v2')
         AND NOT(NOT v_global AND k.value='candidate_id')
         AND NOT(v_group AND k.value IN ('group_kind','group_key')))
     OR (NOT v_global AND COALESCE(v_intent->>'candidate_id','') !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
     OR COALESCE(v_intent->>'request_id','') !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
     OR COALESCE(v_intent->>'action','') NOT IN ('SELECT_ALL_READY','CLEAR_ALL_READY')
     OR (v_group AND (COALESCE(v_intent->>'group_kind','') NOT IN ('TIMESHEET','OVERPAYMENT')
       OR length(COALESCE(v_intent->>'group_key','')) NOT BETWEEN 1 AND 512
       OR (v_intent->>'group_key') ~ '[[:cntrl:]]')) THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_INPUT' USING ERRCODE='22023';
  END IF;
  IF v_intent ? 'presentation_v2' THEN
    v_presentation:=v_intent->'presentation_v2';
    IF jsonb_typeof(v_presentation) IS DISTINCT FROM 'object' THEN
      RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_INPUT' USING ERRCODE='22023';
    END IF;
    IF jsonb_typeof(v_presentation->'view_digest') IS DISTINCT FROM 'string'
       OR COALESCE(v_presentation->>'view_digest','') !~ '^[a-f0-9]{64}$'
       OR NOT (v_presentation ? 'open_ready')
       OR EXISTS(SELECT 1 FROM jsonb_object_keys(v_presentation) k(value) WHERE k.value NOT IN ('view_digest','open_ready')) THEN
      RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_INPUT' USING ERRCODE='22023';
    END IF;
    v_open_ready:=v_presentation->'open_ready';
    IF v_global AND v_open_ready IS DISTINCT FROM 'null'::jsonb THEN
      RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_INPUT' USING ERRCODE='22023';
    END IF;
    IF v_open_ready IS DISTINCT FROM 'null'::jsonb THEN
      IF jsonb_typeof(v_open_ready) IS DISTINCT FROM 'object' THEN
        RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_INPUT' USING ERRCODE='22023';
      END IF;
      IF NOT (v_open_ready ? 'cursor') OR jsonb_typeof(v_open_ready->'limit') IS DISTINCT FROM 'number'
         OR COALESCE(v_open_ready->>'limit','') !~ '^(100|[1-9][0-9]?)$'
         OR EXISTS(SELECT 1 FROM jsonb_object_keys(v_open_ready) k(value) WHERE k.value NOT IN ('cursor','limit'))
         OR (v_open_ready->'cursor' IS DISTINCT FROM 'null'::jsonb AND
           (jsonb_typeof(v_open_ready->'cursor') IS DISTINCT FROM 'string'
            OR length(v_open_ready->>'cursor') NOT BETWEEN 1 AND 4096
            OR (v_open_ready->>'cursor') !~ '^[A-Za-z0-9_-]+$')) THEN
        RAISE EXCEPTION 'BANKING_PAY_V2_INVALID_INPUT' USING ERRCODE='22023';
      END IF;
    END IF;
  END IF;
  IF NOT EXISTS(SELECT 1 FROM public.tms_users u WHERE u.id=p_actor_user_id AND u.is_active IS TRUE AND lower(u.role)='admin') THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_UNAUTHORISED' USING ERRCODE='42501';
  END IF;
  PERFORM public.banking_pay_hot_path_budget_apply('WORKBENCH_CHUNK');
  v_candidate:=(v_intent->>'candidate_id')::uuid;
  v_request:=(v_intent->>'request_id')::uuid;
  v_action:=v_intent->>'action'; v_options:=v_intent->'options';
  v_group_kind:=CASE WHEN v_group THEN v_intent->>'group_kind' END;
  v_group_key:=CASE WHEN v_group THEN v_intent->>'group_key' END;
  v_channel:=v_options->>'pay_channel_scope';
  v_selected:=v_action='SELECT_ALL_READY'; v_state:=CASE WHEN v_selected THEN 'SELECTED' ELSE 'UNSELECTED' END;
  v_selection_scope:=CASE WHEN v_group THEN 'COMPLETE_READY_GROUP' WHEN v_global THEN 'FILTERED_READY' ELSE 'CANDIDATE_READY' END;
  v_selection_origin:=CASE WHEN v_group THEN CASE WHEN v_selected THEN 'USER_READY_GROUP_SELECT_ALL' ELSE 'USER_READY_GROUP_CLEAR_ALL' END
    WHEN v_global THEN CASE WHEN v_selected THEN 'USER_GLOBAL_SELECT_ALL' ELSE 'USER_GLOBAL_CLEAR' END
    ELSE CASE WHEN v_selected THEN 'USER_CANDIDATE_SELECT_ALL' ELSE 'USER_CANDIDATE_CLEAR_ALL' END END;
  SELECT * INTO v_session FROM public.banking_pay_workbench_sessions WHERE id=p_session_id FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'WORKBENCH_SESSION_NOT_FOUND' USING ERRCODE='P0001'; END IF;
  -- Validate all current ownership/scope/contract guards even on a duplicate.
  -- Only the progress expectation is temporarily replaced for receipt lookup;
  -- a new command is checked against its ORIGINAL expected revision below.
  PERFORM private.pay_workbench_modal_context_v2(p_session_id,
    v_options || jsonb_build_object('expected_progress_counter_version',v_session.progress_counter_version),p_actor_user_id);
  v_fingerprint:=encode(extensions.digest(convert_to(jsonb_build_object(
    'session_id',p_session_id,'actor_id',p_actor_user_id,'intent',v_intent)::text,'UTF8'),'sha256'),'hex');
  v_prior_receipt:=v_session.progress_json->'candidate_selection_receipt_v2';
  IF v_prior_receipt->>'request_id'=v_request::text THEN
    IF v_prior_receipt->>'request_fingerprint' IS DISTINCT FROM v_fingerprint THEN
      RAISE EXCEPTION 'BANKING_PAY_V2_REQUEST_CONFLICT' USING ERRCODE='P0001';
    END IF;
    IF (v_prior_receipt->>'progress_counter_version')::bigint IS DISTINCT FROM v_session.progress_counter_version THEN
      RAISE EXCEPTION 'BANKING_PAY_V2_STALE_REVISION' USING ERRCODE='P0001';
    END IF;
    RETURN v_prior_receipt->'result';
  END IF;
  v_session:=private.pay_workbench_modal_context_v2(p_session_id,v_options,p_actor_user_id);
  v_before_revision:=v_session.progress_counter_version;
  IF v_global THEN
    PERFORM 1 FROM public.banking_pay_workbench_session_scope
      WHERE session_id=p_session_id ORDER BY candidate_id FOR UPDATE;
  ELSE
    SELECT * INTO v_scope FROM public.banking_pay_workbench_session_scope
      WHERE session_id=p_session_id AND candidate_id=v_candidate FOR UPDATE;
    IF NOT FOUND OR NOT EXISTS(SELECT 1 FROM private.pay_workbench_modal_source_progress_facts_v2(p_session_id,v_session.version) f
       WHERE f.candidate_id=v_candidate AND f.source_state='CURRENT') THEN
      RAISE EXCEPTION 'BANKING_PAY_V2_NOT_READY' USING ERRCODE='P0001';
    END IF;
  END IF;
  -- Same session->scope->row lock order as current Workbench owners.
  PERFORM 1 FROM public.banking_pay_workbench_preview_rows r
    WHERE r.session_id=p_session_id AND r.session_version=v_session.version AND (v_global OR r.candidate_id=v_candidate)
    ORDER BY r.candidate_id,r.id FOR UPDATE;
  IF v_presentation IS NOT NULL THEN
    -- The complete previously displayed content, not a visible-page count.
    -- Replay already returned its original receipt above without another write.
    v_before_state:=private.pay_workbench_modal_candidate_state_v2(v_session,v_channel,v_candidate);
    IF v_before_state->>'view_digest' IS DISTINCT FROM v_presentation->>'view_digest' THEN
      RAISE EXCEPTION 'BANKING_PAY_V2_STALE_VIEW' USING ERRCODE='P0001';
    END IF;
  END IF;
  DROP TABLE IF EXISTS pg_temp._bpay_modal_candidate_before_v2;
  CREATE TEMPORARY TABLE pg_temp._bpay_modal_candidate_before_v2 ON COMMIT DROP AS
    SELECT r.id,r.row_key,r.key_type,r.key_value,r.timesheet_id,r.selected,r.selection_state,
      private.pay_workbench_preview_effective_section_v1(r.section,r.row_json) AS effective_section
    FROM public.banking_pay_workbench_preview_rows r
    WHERE r.session_id=p_session_id AND r.session_version=v_session.version AND (v_global OR r.candidate_id=v_candidate);
  DROP TABLE IF EXISTS pg_temp._bpay_modal_candidate_ready_v2;
  CREATE TEMPORARY TABLE pg_temp._bpay_modal_candidate_ready_v2 ON COMMIT DROP AS
    SELECT m.row_id,m.candidate_id FROM private.pay_workbench_modal_ready_members_v2(v_session,v_channel) m
    WHERE NOT v_group AND (v_global OR m.candidate_id=v_candidate)
    UNION ALL
    SELECT g.row_id,v_candidate FROM private.pay_workbench_modal_ready_group_members_v2(v_session,v_channel,v_candidate) g
    WHERE v_group AND g.group_kind=v_group_kind AND g.group_key=v_group_key;
  SELECT count(*) INTO v_eligible_count FROM pg_temp._bpay_modal_candidate_ready_v2;
  IF v_eligible_count=0 AND v_group THEN RAISE EXCEPTION 'BANKING_PAY_V2_GROUP_NOT_SELECTABLE' USING ERRCODE='P0001'; END IF;
  IF v_eligible_count=0 AND NOT v_global THEN RAISE EXCEPTION 'BANKING_PAY_V2_CANDIDATE_NOT_CURRENT' USING ERRCODE='P0001'; END IF;
  SELECT count(*) INTO v_change_count FROM public.banking_pay_workbench_preview_rows r
    JOIN pg_temp._bpay_modal_candidate_ready_v2 m ON m.row_id=r.id
    WHERE r.selected IS DISTINCT FROM v_selected OR r.selection_state IS DISTINCT FROM v_state
      OR r.row_json->>'selection_user_override' IS DISTINCT FROM v_state;
  IF v_change_count=0 THEN
    RETURN jsonb_build_object('ok',true,'request_id',v_request,'state_changed',false,
      'session_id',p_session_id,'candidate_id',v_candidate,'session_version',v_session.version,
      'progress_counter_version',v_session.progress_counter_version,'scope_hash',v_options->>'scope_hash',
      'action',v_action,'selection_scope',v_selection_scope,
      'changed_row_count',0,'movements','[]'::jsonb,'recovery_revalidation_count',0)
      || CASE WHEN v_presentation IS NULL THEN '{}'::jsonb ELSE jsonb_build_object('presentation_before',v_before_state) END;
  END IF;
  UPDATE public.banking_pay_workbench_preview_rows r
    SET selected=v_selected,selection_state=v_state,row_json=COALESCE(r.row_json,'{}'::jsonb) || jsonb_build_object(
      'selected',v_selected,'selection_state',v_state,'selection_user_override',v_state,
      'selection_origin',v_selection_origin,
      'selection_user_override_at_utc',v_now::text),updated_at_utc=v_now
    FROM pg_temp._bpay_modal_candidate_ready_v2 m
    WHERE r.id=m.row_id;
  -- One canonical revalidation per affected candidate, as in the legacy global
  -- owner; one for a candidate intent. No repeated selection-owner invocation.
  -- It owns every amount, allocation and movement. Final publication is below
  -- this loop, so failure in any candidate rolls the whole SQL operation back.
  FOR v_revalidation_candidate IN
    SELECT DISTINCT candidate_id FROM pg_temp._bpay_modal_candidate_ready_v2 ORDER BY candidate_id
  LOOP
    v_revalidation:=public.pay_workbench_revalidate_zero_retained_recovery_headroom_v1(
      p_session_id,v_revalidation_candidate,jsonb_build_object('pay_channel_scope',v_channel));
    IF v_revalidation->>'ok' IS DISTINCT FROM 'true'
       OR v_revalidation->>'action'='DEFERRED_UNTIL_FINAL_MATERIALISATION' THEN
      RAISE EXCEPTION 'BANKING_PAY_V2_NOT_READY' USING ERRCODE='P0001';
    END IF;
    v_revalidation_count:=v_revalidation_count+1;
  END LOOP;
  SELECT * INTO v_session FROM public.banking_pay_workbench_sessions WHERE id=p_session_id;
  IF v_session.progress_counter_version IS DISTINCT FROM v_before_revision THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_DEPENDENCY_UNAVAILABLE' USING ERRCODE='P0001';
  END IF;
  -- Re-read the FINAL eligible set. This changes selection/explicit intent only:
  -- a newly promoted recovery is now Ready, so SELECT all must override an old
  -- individual unselection without running a second deduction calculation.
  DROP TABLE IF EXISTS pg_temp._bpay_modal_candidate_final_ready_v2;
  CREATE TEMPORARY TABLE pg_temp._bpay_modal_candidate_final_ready_v2 ON COMMIT DROP AS
    SELECT m.row_id FROM private.pay_workbench_modal_ready_members_v2(v_session,v_channel) m
    WHERE NOT v_group AND (v_global OR m.candidate_id=v_candidate)
    UNION ALL
    SELECT g.row_id FROM private.pay_workbench_modal_ready_group_members_v2(v_session,v_channel,v_candidate) g
    WHERE v_group AND g.group_kind=v_group_kind AND g.group_key=v_group_key;
  UPDATE public.banking_pay_workbench_preview_rows r
    SET selected=v_selected,selection_state=v_state,row_json=COALESCE(r.row_json,'{}'::jsonb) || jsonb_build_object(
      'selected',v_selected,'selection_state',v_state,'selection_user_override',v_state,
      'selection_origin',v_selection_origin,
      'selection_user_override_at_utc',v_now::text),updated_at_utc=v_now
    FROM pg_temp._bpay_modal_candidate_final_ready_v2 m WHERE r.id=m.row_id;
  SELECT count(*) INTO v_final_count FROM pg_temp._bpay_modal_candidate_final_ready_v2;
  IF EXISTS(SELECT 1 FROM pg_temp._bpay_modal_candidate_final_ready_v2 m
    JOIN public.banking_pay_workbench_preview_rows r ON r.id=m.row_id
    WHERE r.selected IS DISTINCT FROM v_selected OR r.selection_state IS DISTINCT FROM v_state
      OR r.row_json->>'selection_user_override' IS DISTINCT FROM v_state) THEN
    RAISE EXCEPTION 'BANKING_PAY_V2_SELECTION_DID_NOT_SETTLE' USING ERRCODE='P0001';
  END IF;
  -- Preserve the existing full-session selected identity and Draft-input owner.
  -- This is the same certified predicate, not a candidate-page count or sum.
  SELECT COALESCE(jsonb_agg(r.id::text ORDER BY r.row_ordinal,r.id),'[]'::jsonb),count(*),
    COALESCE(jsonb_agg(jsonb_build_object('candidate_id',r.candidate_id,'row_key',r.row_key,
      'timesheet_id',r.timesheet_id,'key_type',r.key_type,'key_value',r.key_value)
      ORDER BY r.candidate_id,r.row_key,r.id),'[]'::jsonb)
    INTO v_selected_ids,v_selected_count,v_identities
    FROM private.pay_workbench_modal_selection_rows_v2(p_session_id,v_session.version) m
    JOIN public.banking_pay_workbench_preview_rows r ON r.id=m.id
    WHERE m.is_selectable IS TRUE AND r.selected IS TRUE AND upper(btrim(r.selection_state))='SELECTED';
  v_session_ready:=lower(btrim(COALESCE(v_session.progress_json->>'ready',v_session.progress_json->>'session_ready',
    v_session.progress_json->>'ready_flag','false'))) IN ('true','t','1','yes','y','on');
  SELECT COALESCE(jsonb_agg(b.value ORDER BY b.n),'[]'::jsonb) INTO v_blockers
    FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(v_session.progress_json->'draft_blocker_codes')='array'
      THEN v_session.progress_json->'draft_blocker_codes' ELSE '[]'::jsonb END) WITH ORDINALITY b(value,n)
    WHERE upper(btrim(b.value))<>'NO_SELECTED_ROWS';
  IF v_session_ready AND v_selected_count=0 THEN v_blockers:=v_blockers || jsonb_build_array('NO_SELECTED_ROWS'); END IF;
  SELECT COALESCE(jsonb_agg(jsonb_build_object('identity',r.id,'candidate_id',r.candidate_id,
    'row_key',r.row_key,'key_type',r.key_type,'key_value',r.key_value,'from',b.effective_section,
    'to',private.pay_workbench_preview_effective_section_v1(r.section,r.row_json),'selected',r.selected)
    ORDER BY r.row_ordinal,r.id),'[]'::jsonb) INTO v_movements
    FROM public.banking_pay_workbench_preview_rows r JOIN pg_temp._bpay_modal_candidate_before_v2 b ON b.id=r.id
    WHERE private.pay_workbench_preview_effective_section_v1(r.section,r.row_json) IS DISTINCT FROM b.effective_section;
  SELECT count(*) INTO v_selection_changed
    FROM public.banking_pay_workbench_preview_rows r JOIN pg_temp._bpay_modal_candidate_before_v2 b ON b.id=r.id
    WHERE r.selected IS DISTINCT FROM b.selected OR r.selection_state IS DISTINCT FROM b.selection_state;
  v_result:=jsonb_build_object('ok',true,'request_id',v_request,'state_changed',true,
    'session_id',p_session_id,'candidate_id',v_candidate,'session_version',v_session.version,
    'progress_counter_version',v_session.progress_counter_version+1,'scope_hash',v_options->>'scope_hash',
    'action',v_action,'selection_scope',v_selection_scope,
    'changed_row_count',v_selection_changed,'intent_row_count',v_change_count,
    'final_ready_count',v_final_count,'movements',v_movements,'recovery_revalidation_count',v_revalidation_count)
    || CASE WHEN v_presentation IS NULL THEN '{}'::jsonb ELSE jsonb_build_object('presentation_before',v_before_state) END;
  UPDATE public.banking_pay_workbench_sessions s SET selected_row_count=v_selected_count,
    server_selected_preview_row_ids='[]'::jsonb,server_selected_preview_row_ids_provided=false,
    progress_counter_version=v_session.progress_counter_version+1,progress_updated_at_utc=v_now,updated_at_utc=v_now,
    progress_json=COALESCE(s.progress_json,'{}'::jsonb) || jsonb_build_object(
      'last_selection_update_at_utc',v_now::text,'last_selection_mode',CASE WHEN v_group THEN 'COMPLETE_READY_GROUP_SCOPE_V2'
        WHEN v_global THEN 'FILTERED_READY_SCOPE_V2' ELSE 'CANDIDATE_READY_SCOPE_V2' END,
      'last_selection_action',v_action,'last_selection_candidate_id',v_candidate,
      'selected_row_count',v_selected_count,'selected_eligible_ready_row_count',v_selected_count,
      'selected_rows_available',v_selected_count>0,'ready_for_draft',v_session_ready AND v_selected_count>0,
      'can_create_draft',v_session_ready AND v_selected_count>0,'draft_blocker_codes',v_blockers,'blocker_codes',v_blockers,
      'selection_intent_v1',COALESCE(s.progress_json->'selection_intent_v1','{}'::jsonb) || jsonb_build_object(
        'canonical_preview_lines',jsonb_build_object('mode','IMPLICIT_ALL','section','canonical_preview_lines',
          'identity','preview_row_id_with_session_section_candidate_row_key_conflict_identity',
          'updated_at_utc',v_now::text,'updated_by_user_id',p_actor_user_id::text,
          'source_selection_mode',CASE WHEN v_group THEN 'COMPLETE_READY_GROUP_SCOPE_V2'
            WHEN v_global THEN 'FILTERED_READY_SCOPE_V2' ELSE 'CANDIDATE_READY_SCOPE_V2' END,'source_selection_action',v_action,
          'server_selected_preview_row_ids_provided',false,'selected_row_count',v_selected_count,
          'selected_economic_identities',v_identities,'identity_contract_version',2)),
      'candidate_selection_receipt_v2',jsonb_build_object('request_id',v_request,'request_fingerprint',v_fingerprint,
        'progress_counter_version',v_session.progress_counter_version+1,'result',v_result))
    WHERE s.id=p_session_id;
  PERFORM public._audit_insert('banking_pay_workbench_session',p_session_id::text,
    CASE WHEN v_group THEN 'SESSION_READY_GROUP_SELECTION' WHEN v_global THEN 'SESSION_FILTERED_READY_SELECTION' ELSE 'SESSION_CANDIDATE_READY_SELECTION' END,NULL::jsonb,
    jsonb_build_object('request_id',v_request,'candidate_id',v_candidate,'action',v_action,'pay_channel_scope',v_channel,
      'group_kind',v_group_kind,'group_key',v_group_key,
      'scope_hash',v_options->>'scope_hash','before_progress_counter_version',v_before_revision,
      'progress_counter_version',v_before_revision+1,'intent_row_count',v_change_count,
      'changed_row_count',v_selection_changed,'final_ready_count',v_final_count,'movement_count',jsonb_array_length(v_movements)),
    CASE WHEN v_group THEN 'SESSION_READY_GROUP_SELECTION' WHEN v_global THEN 'SESSION_FILTERED_READY_SELECTION' ELSE 'SESSION_CANDIDATE_READY_SELECTION' END,p_actor_user_id);
  RETURN v_result;
END;
$function$;
ALTER FUNCTION private.pay_workbench_modal_candidate_selection_apply_v2(uuid,jsonb,uuid) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_candidate_selection_apply_v2(uuid,jsonb,uuid) FROM PUBLIC, anon, authenticated, service_role;
commit;
