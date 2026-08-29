BEGIN;SET LOCAL client_min_messages='warning';SET LOCAL statement_timeout='30s';
/*FIXTURE*/
UPDATE public.banking_pay_workbench_sessions SET scope_seed_complete=true WHERE id='10000000-0000-4000-8000-000000000005';
CREATE FUNCTION pg_temp.row_snapshot_v2(p_id uuid) RETURNS jsonb LANGUAGE sql AS $snapshot$
 SELECT jsonb_build_object('session',(SELECT to_jsonb(s) FROM public.banking_pay_workbench_sessions s WHERE id=p_id),
 'rows',(SELECT jsonb_agg(to_jsonb(r) ORDER BY id) FROM public.banking_pay_workbench_preview_rows r WHERE session_id=p_id),
 'scopes',(SELECT jsonb_agg(to_jsonb(r) ORDER BY id) FROM public.banking_pay_workbench_session_scope r WHERE session_id=p_id),
 'audits',(SELECT count(*) FROM public.audit_events WHERE object_id_text=p_id::text),
 'counter',(SELECT to_jsonb(c) FROM public.app_change_counters c WHERE entity_key='banking_pay_workbench_session:'||p_id::text));
$snapshot$;
CREATE FUNCTION pg_temp.row_diff_paths_v2(a jsonb,b jsonb,p_path text DEFAULT '') RETURNS text[] LANGUAGE plpgsql AS $diff$
DECLARE k text;n integer;result text[]:=ARRAY[]::text[];
BEGIN
 IF a IS NOT DISTINCT FROM b THEN RETURN result;END IF;
 IF jsonb_typeof(a)='object' AND jsonb_typeof(b)='object' THEN
  FOR k IN SELECT jsonb_object_keys(a||b) LOOP result:=result||pg_temp.row_diff_paths_v2(a->k,b->k,p_path||'/'||k);END LOOP;
 ELSIF jsonb_typeof(a)='array' AND jsonb_typeof(b)='array' AND jsonb_array_length(a)=jsonb_array_length(b) THEN
  FOR n IN 0..jsonb_array_length(a)-1 LOOP result:=result||pg_temp.row_diff_paths_v2(a->n,b->n,p_path||'/'||n);END LOOP;
 ELSE result:=ARRAY[p_path];END IF;
 RETURN result;
END;$diff$;
CREATE TEMP TABLE row_responses(label text, before_summary jsonb, args jsonb,payload jsonb) ON COMMIT DROP;
DO $proof$
DECLARE s public.banking_pay_workbench_sessions%ROWTYPE;opts jsonb;before_summary jsonb;ready jsonb;open_ready jsonb;
 before_snapshot jsonb;baseline_after jsonb;baseline_result jsonb;actual_after jsonb;result jsonb;ids jsonb;old_args jsonb;selected boolean;
 diff_path text;json_path text[];before_time timestamptz;after_time timestamptz;bad jsonb;
 candidate uuid:='10000000-0000-4000-8000-000000000002';request uuid:='10000000-0000-4000-8000-000000009996';case_no integer;
BEGIN
 IF current_setting('server_version_num')::integer NOT BETWEEN 170000 AND 179999 THEN RAISE EXCEPTION 'PG17_REQUIRED';END IF;
 IF has_function_privilege('anon','public.pay_workbench_session_set_ready_rows_v1(uuid,uuid,jsonb,uuid,jsonb,boolean,uuid,text,jsonb)','EXECUTE')
  OR has_function_privilege('authenticated','public.pay_workbench_session_set_ready_rows_v1(uuid,uuid,jsonb,uuid,jsonb,boolean,uuid,text,jsonb)','EXECUTE')
  OR NOT has_function_privilege('service_role','public.pay_workbench_session_set_ready_rows_v1(uuid,uuid,jsonb,uuid,jsonb,boolean,uuid,text,jsonb)','EXECUTE')
  THEN RAISE EXCEPTION 'ROW_SELECTION_ACL';END IF;
 FOR case_no IN 1..4 LOOP
  SELECT * INTO STRICT s FROM public.banking_pay_workbench_sessions WHERE id='10000000-0000-4000-8000-000000000005';
  opts:=jsonb_build_object('expected_session_version',s.version,'expected_progress_counter_version',s.progress_counter_version,
   'scope_hash',private.pay_workbench_modal_scope_hash_v2(s,'ALL'),'pay_channel_scope','ALL');
  before_summary:=public.pay_workbench_session_get_candidate_summary_page_v1(s.id,opts,s.actor_user_id,'CANDIDATE','ASC',NULL,100);
  ready:=public.pay_workbench_session_get_candidate_ready_page_v1(s.id,candidate,opts,s.actor_user_id,NULL,100);
  open_ready:=jsonb_build_object('cursor',ready->'page_anchor','limit',100);
  SELECT jsonb_agg(('10000000-0000-4000-8000-'||lpad((1000+n)::text,12,'0'))) INTO ids FROM generate_series(1,CASE WHEN case_no=1 THEN 1 ELSE 100 END)n;
  selected:=(case_no>=3);
  old_args:=jsonb_build_object('section','canonical_preview_lines','expected_session_version',s.version,
   'expected_progress_counter_version',s.progress_counter_version,
   CASE WHEN selected THEN 'select_preview_row_ids' ELSE 'deselect_preview_row_ids' END,ids);
  before_snapshot:=pg_temp.row_snapshot_v2(s.id);
  BEGIN
   baseline_result:=public.pay_workbench_session_set_selected_rows(s.id,old_args,s.actor_user_id);
   baseline_after:=pg_temp.row_snapshot_v2(s.id);
   RAISE EXCEPTION 'ROLLBACK_ORIGINAL_ONLY' USING ERRCODE='ZP001';
  EXCEPTION WHEN SQLSTATE 'ZP001' THEN IF SQLERRM IS DISTINCT FROM 'ROLLBACK_ORIGINAL_ONLY' THEN RAISE;END IF;END;
  IF pg_temp.row_snapshot_v2(s.id) IS DISTINCT FROM before_snapshot THEN RAISE EXCEPTION 'BASELINE_NOT_RESTORED';END IF;
  SET LOCAL ROLE service_role;
  result:=public.pay_workbench_session_set_ready_rows_v1(s.id,candidate,opts,s.actor_user_id,ids,selected,request,before_summary->>'view_digest',open_ready);
  RESET ROLE;
  actual_after:=pg_temp.row_snapshot_v2(s.id);
  -- The unchanged recovery owner deliberately uses clock_timestamp(), unlike
  -- the row owner's transaction time. Compare every other field exactly;
  -- those precise clock fields must remain valid, current and monotonic.
  FOREACH diff_path IN ARRAY pg_temp.row_diff_paths_v2(baseline_after,actual_after) LOOP
   IF diff_path !~ '^/(rows/[0-9]+/row_json/selection_recovery_headroom_v1/updated_at_utc|session/progress_json/selection_recovery_headroom_v1/updated_at_utc|session/updated_at_utc|scopes/[0-9]+/certified_preview_publication_attestation_json/selection_recovery_headroom_v1/updated_at_utc|scopes/[0-9]+/updated_at_utc)$' THEN
    RAISE EXCEPTION 'ROW_OWNER_EFFECTS_CHANGED_CASE_%',case_no USING DETAIL=diff_path;
   END IF;
   json_path:=string_to_array(substr(diff_path,2),'/');
   before_time:=(baseline_after#>>json_path)::timestamptz;after_time:=(actual_after#>>json_path)::timestamptz;
   IF before_time IS NULL OR after_time IS NULL OR before_time<transaction_timestamp() OR after_time<before_time OR after_time>clock_timestamp() THEN
    RAISE EXCEPTION 'RECOVERY_CLOCK_SEMANTICS_CHANGED' USING DETAIL=diff_path;
   END IF;
  END LOOP;
  IF result->>'progress_counter_version' IS DISTINCT FROM baseline_result->>'progress_counter_version'
   OR result->>'state_changed' IS DISTINCT FROM 'true' OR result->>'selection_scope' IS DISTINCT FROM 'EXACT_READY_ROWS'
   OR result#>>'{ready_page,progress_counter_version}' IS DISTINCT FROM result->>'progress_counter_version'
   OR result#>>'{retention,before_view_digest}' IS DISTINCT FROM before_summary->>'view_digest' THEN RAISE EXCEPTION 'ROW_RESPONSE_AUTHORITY';END IF;
  INSERT INTO row_responses VALUES('case_'||case_no,before_summary,jsonb_build_object(
   'p_session_id',s.id,'p_candidate_id',candidate,'p_options_json',opts,'p_actor_user_id',s.actor_user_id,
   'p_preview_row_ids',ids,'p_selected',selected,'p_request_id',request,'p_expected_view_digest',before_summary->>'view_digest','p_open_ready_json',open_ready),result);
  before_snapshot:=pg_temp.row_snapshot_v2(s.id);
  BEGIN
   PERFORM public.pay_workbench_session_set_ready_rows_v1(s.id,candidate,opts,s.actor_user_id,ids,selected,request,before_summary->>'view_digest',open_ready);
   RAISE EXCEPTION 'OLD_ROW_CONTEXT_ACCEPTED';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN IF SQLERRM IS DISTINCT FROM 'BANKING_PAY_V2_STALE_REVISION' THEN RAISE;END IF;END;
  IF pg_temp.row_snapshot_v2(s.id) IS DISTINCT FROM before_snapshot THEN RAISE EXCEPTION 'ROW_REPEAT_WROTE';END IF;
 END LOOP;
 -- Failed requests cannot change the original row/session/scope/audit state.
 SELECT * INTO STRICT s FROM public.banking_pay_workbench_sessions WHERE id=s.id;
 opts:=jsonb_set(opts,'{expected_progress_counter_version}',to_jsonb(s.progress_counter_version));
 before_summary:=public.pay_workbench_session_get_candidate_summary_page_v1(s.id,opts,s.actor_user_id,'CANDIDATE','ASC',NULL,100);
 ids:='["10000000-0000-4000-8000-000000001001"]'::jsonb;
 before_snapshot:=pg_temp.row_snapshot_v2(s.id);
 FOR bad IN SELECT value FROM jsonb_array_elements(jsonb_build_array(
  jsonb_build_object('rows','null'::jsonb,'code','BANKING_PAY_V2_INVALID_INPUT'),
  jsonb_build_object('rows','[]'::jsonb,'code','BANKING_PAY_V2_INVALID_INPUT'),
  jsonb_build_object('rows',ids||ids,'code','BANKING_PAY_V2_INVALID_INPUT'),
  jsonb_build_object('rows',(SELECT jsonb_agg('10000000-0000-4000-8000-'||lpad(n::text,12,'0')) FROM generate_series(1,101)n),'code','BANKING_PAY_V2_INVALID_INPUT'),
  jsonb_build_object('rows','[12]'::jsonb,'code','BANKING_PAY_V2_INVALID_INPUT'),
  jsonb_build_object('rows','["not-a-uuid"]'::jsonb,'code','BANKING_PAY_V2_INVALID_INPUT'),
  jsonb_build_object('rows','["10000000-0000-4000-8000-000000001108"]'::jsonb,'code','BANKING_PAY_V2_ROW_NOT_SELECTABLE'),
  jsonb_build_object('rows','["10000000-0000-4000-8000-000000002003"]'::jsonb,'code','BANKING_PAY_V2_ROW_NOT_SELECTABLE'),
  jsonb_build_object('selected',NULL,'code','BANKING_PAY_V2_INVALID_INPUT'),
  jsonb_build_object('view',repeat('e',64),'code','BANKING_PAY_V2_STALE_VIEW'),
  jsonb_build_object('view',NULL,'code','BANKING_PAY_V2_INVALID_INPUT'),
  jsonb_build_object('open','[]'::jsonb,'code','BANKING_PAY_V2_INVALID_INPUT'),
  jsonb_build_object('open',jsonb_build_object('cursor',NULL,'limit','100'),'code','BANKING_PAY_V2_INVALID_INPUT'),
  jsonb_build_object('open',jsonb_build_object('cursor','../invalid','limit',100),'code','BANKING_PAY_V2_INVALID_INPUT')
 )) LOOP
  BEGIN
   PERFORM public.pay_workbench_session_set_ready_rows_v1(s.id,candidate,opts,s.actor_user_id,
    CASE WHEN bad ? 'rows' THEN bad->'rows' ELSE ids END,
    CASE WHEN bad ? 'selected' THEN (bad->>'selected')::boolean ELSE false END,request,
    CASE WHEN bad ? 'view' THEN bad->>'view' ELSE before_summary->>'view_digest' END,
    CASE WHEN bad ? 'open' THEN bad->'open' ELSE NULL END);
   RAISE EXCEPTION 'BAD_ROW_REQUEST_ACCEPTED';
  EXCEPTION WHEN OTHERS THEN IF SQLERRM IS DISTINCT FROM bad->>'code' THEN RAISE;END IF;END;
  IF pg_temp.row_snapshot_v2(s.id) IS DISTINCT FROM before_snapshot THEN RAISE EXCEPTION 'BAD_ROW_REQUEST_WROTE';END IF;
 END LOOP;
 -- A response-limit error occurs after the original mutation and must roll it
 -- back; oversized data cannot silently hide the row or report partial success.
 UPDATE public.candidates SET display_name=repeat('x',40000) WHERE id=candidate;
 SELECT * INTO STRICT s FROM public.banking_pay_workbench_sessions WHERE id=s.id;
 opts:=jsonb_set(opts,'{expected_progress_counter_version}',to_jsonb(s.progress_counter_version));
 before_summary:=private.pay_workbench_modal_candidate_state_v2(s,'ALL',candidate);
 before_snapshot:=pg_temp.row_snapshot_v2(s.id);
 BEGIN
  PERFORM public.pay_workbench_session_set_ready_rows_v1(s.id,candidate,opts,s.actor_user_id,ids,false,request,before_summary->>'view_digest',NULL);
  RAISE EXCEPTION 'OVERSIZED_ROW_RESPONSE_ACCEPTED';
 EXCEPTION WHEN SQLSTATE 'P0001' THEN IF SQLERRM IS DISTINCT FROM 'BANKING_PAY_V2_SELECTION_TOO_LARGE' THEN RAISE;END IF;END;
 IF pg_temp.row_snapshot_v2(s.id) IS DISTINCT FROM before_snapshot THEN RAISE EXCEPTION 'OVERSIZED_ROW_RESPONSE_WROTE';END IF;
END;$proof$;
SELECT jsonb_build_object('label',label,'before_summary',before_summary,'args',args,'payload',payload) FROM row_responses ORDER BY label;
ROLLBACK;
