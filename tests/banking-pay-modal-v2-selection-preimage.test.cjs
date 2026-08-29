const assert=require('node:assert/strict');
const test=require('node:test');
const fs=require('node:fs');
const path=require('node:path');
const {spawnSync}=require('node:child_process');
const root=path.resolve(__dirname,'..');
const read=name=>fs.readFileSync(path.join(root,name),'utf8');

test('candidate preimage belongs to the existing receipt and cannot add another session write',()=>{
 const source=read('supabase/repeatable/28082026_1424_banking_pay_modal_candidate_selection_core.sql');
 assert.ok(source.includes('presentation_v2'));
 assert.ok(source.includes('presentation_before'));
 assert.ok(source.includes('BANKING_PAY_V2_STALE_VIEW'));
 assert.equal((source.match(/UPDATE public\.banking_pay_workbench_sessions/g)||[]).length,1);
 assert.equal((source.match(/public\._audit_insert\(/g)||[]).length,1);
 assert.ok(source.indexOf('BANKING_PAY_V2_STALE_VIEW')<source.indexOf('UPDATE public.banking_pay_workbench_preview_rows'));
 assert.ok(source.indexOf('BANKING_PAY_V2_STALE_VIEW')>source.indexOf('ORDER BY r.candidate_id,r.id FOR UPDATE'));
 assert.match(source,/'session_id',p_session_id,'actor_id',p_actor_user_id,'intent',v_intent/);
});

test('actual preimage rejection, selection, replay and no-op preserve existing notifications with no extra writes',
 {skip:!process.env.BANKING_MODAL_LOCAL_PSQL},()=>{
 const setup=read('tests/fixtures/28082026_1429_banking_pay_selection_setup.sql');
 const sql=`BEGIN; SET LOCAL statement_timeout='30s'; SET LOCAL client_min_messages='warning';
 DO $target$ BEGIN IF current_database()<>'banking_modal_v2_test'
  OR current_setting('server_version_num')::integer NOT BETWEEN 170000 AND 179999 THEN RAISE EXCEPTION 'WRONG_LOCAL_TARGET';END IF;END;$target$;
 ${setup}
 CREATE FUNCTION pg_temp.preimage_snapshot_v2(session_id uuid) RETURNS jsonb LANGUAGE sql AS $snapshot$
  SELECT jsonb_build_object(
   'session',(SELECT to_jsonb(s) FROM public.banking_pay_workbench_sessions s WHERE s.id=session_id),
   'rows',(SELECT jsonb_agg(to_jsonb(r) ORDER BY r.id) FROM public.banking_pay_workbench_preview_rows r WHERE r.session_id=preimage_snapshot_v2.session_id),
   'audits',(SELECT count(*) FROM public.audit_events a WHERE a.object_id_text=session_id::text),
   'change',(SELECT to_jsonb(c) FROM public.app_change_counters c WHERE c.entity_key='banking_pay_workbench_session:'||session_id::text));
 $snapshot$;
 DO $proof$
 DECLARE s public.banking_pay_workbench_sessions%ROWTYPE;before_state jsonb;after_state jsonb;options jsonb;input jsonb;
  before_snapshot jsonb;after_snapshot jsonb;result jsonb;again jsonb;bad jsonb;metadata jsonb;seq_before bigint;audits_before bigint;
  baseline_after jsonb;baseline_result jsonb;baseline_change_count bigint;
  candidate uuid:='10000000-0000-4000-8000-000000000002';
 BEGIN
  SELECT * INTO STRICT s FROM public.banking_pay_workbench_sessions WHERE id='10000000-0000-4000-8000-000000000005';
  before_state:=private.pay_workbench_modal_candidate_state_v2(s,'ALL',candidate);
  options:=jsonb_build_object('expected_session_version',s.version,'expected_progress_counter_version',s.progress_counter_version,
   'pay_channel_scope','ALL','scope_hash',private.pay_workbench_modal_scope_hash_v2(s,'ALL'));
  input:=jsonb_build_object('modal_candidate_intent_v2',jsonb_build_object('candidate_id',candidate,
   'request_id','10000000-0000-4000-8000-000000009998','action','CLEAR_ALL_READY','options',options,
   'presentation_v2',jsonb_build_object('view_digest',before_state->>'view_digest','open_ready',NULL)));
  before_snapshot:=pg_temp.preimage_snapshot_v2(s.id);
  -- Characterize the ORIGINAL intent on exactly the same fixture, then roll
  -- it back. Recovery already emits its own change notification; presentation
  -- must add none, not redefine how many the existing owners emit.
  BEGIN
   baseline_result:=public.pay_workbench_session_set_selected_rows(s.id,
    input#-'{modal_candidate_intent_v2,presentation_v2}',s.actor_user_id);
   baseline_after:=pg_temp.preimage_snapshot_v2(s.id);
   RAISE EXCEPTION 'ROLLBACK_BASELINE_ONLY' USING ERRCODE='ZP001';
  EXCEPTION WHEN SQLSTATE 'ZP001' THEN
   IF SQLERRM IS DISTINCT FROM 'ROLLBACK_BASELINE_ONLY' THEN RAISE;END IF;
  END;
  IF pg_temp.preimage_snapshot_v2(s.id) IS DISTINCT FROM before_snapshot
   OR baseline_result ? 'presentation_before' OR baseline_result->>'progress_counter_version' IS DISTINCT FROM '5' THEN
   RAISE EXCEPTION 'ORIGINAL_INTENT_BASELINE_NOT_RESTORED';END IF;
  baseline_change_count:=(baseline_after#>>'{change,seq}')::bigint-(before_snapshot#>>'{change,seq}')::bigint;
  IF baseline_change_count IS NULL OR baseline_change_count<1 THEN RAISE EXCEPTION 'BASELINE_NOTIFICATION_MISSING';END IF;
  FOR metadata IN SELECT value FROM jsonb_array_elements(jsonb_build_array(
   'null'::jsonb,'[]'::jsonb,'true'::jsonb,'{}'::jsonb,
   jsonb_build_object('view_digest',before_state->>'view_digest'),
   jsonb_build_object('view_digest','bad','open_ready',NULL),
   jsonb_build_object('view_digest',before_state->>'view_digest','open_ready',NULL,'extra',true),
   jsonb_build_object('view_digest',before_state->>'view_digest','open_ready',jsonb_build_object('cursor',NULL,'limit',101)),
   jsonb_build_object('view_digest',before_state->>'view_digest','open_ready',jsonb_build_object('cursor',NULL,'limit','100')),
   jsonb_build_object('view_digest',before_state->>'view_digest','open_ready',jsonb_build_object('cursor',NULL)),
   jsonb_build_object('view_digest',before_state->>'view_digest','open_ready',jsonb_build_object('cursor','../wrong','limit',100))
  )) LOOP
   bad:=jsonb_set(input,'{modal_candidate_intent_v2,presentation_v2}',metadata);
   BEGIN
    PERFORM public.pay_workbench_session_set_selected_rows(s.id,bad,s.actor_user_id);
    RAISE EXCEPTION 'BAD_PRESENTATION_ACCEPTED';
   EXCEPTION WHEN invalid_parameter_value THEN
    IF SQLERRM IS DISTINCT FROM 'BANKING_PAY_V2_INVALID_INPUT' THEN RAISE;END IF;
   END;
   IF pg_temp.preimage_snapshot_v2(s.id) IS DISTINCT FROM before_snapshot THEN RAISE EXCEPTION 'BAD_PRESENTATION_WROTE';END IF;
  END LOOP;
  bad:=jsonb_set(input,'{modal_candidate_intent_v2,presentation_v2,view_digest}',to_jsonb(repeat('0',64)));
  BEGIN
   PERFORM public.pay_workbench_session_set_selected_rows(s.id,bad,s.actor_user_id);
   RAISE EXCEPTION 'STALE_PRESENTATION_ACCEPTED';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN
   IF SQLERRM IS DISTINCT FROM 'BANKING_PAY_V2_STALE_VIEW' THEN RAISE;END IF;
  END;
  IF pg_temp.preimage_snapshot_v2(s.id) IS DISTINCT FROM before_snapshot THEN RAISE EXCEPTION 'STALE_PRESENTATION_WROTE';END IF;
  seq_before:=(before_snapshot#>>'{change,seq}')::bigint;audits_before:=(before_snapshot->>'audits')::bigint;
  result:=public.pay_workbench_session_set_selected_rows(s.id,input,s.actor_user_id);
  after_snapshot:=pg_temp.preimage_snapshot_v2(s.id);
  IF result->'presentation_before' IS DISTINCT FROM before_state OR result->>'state_changed' IS DISTINCT FROM 'true'
   OR result->>'progress_counter_version' IS DISTINCT FROM '5'
   OR (after_snapshot#>>'{change,seq}')::bigint IS DISTINCT FROM seq_before+baseline_change_count
   OR (after_snapshot->>'audits')::bigint IS DISTINCT FROM audits_before+1 THEN
   RAISE EXCEPTION 'PRESENTATION_CHANGED_SINGLE_SETTLEMENT_CONTRACT before_matches%, changed%, progress%, seq_before%, seq_after%, audits_before%, audits_after%',
    result->'presentation_before' IS NOT DISTINCT FROM before_state,result->>'state_changed',result->>'progress_counter_version',
    seq_before,after_snapshot#>>'{change,seq}',audits_before,after_snapshot->>'audits';END IF;
  again:=public.pay_workbench_session_set_selected_rows(s.id,input,s.actor_user_id);
  IF again IS DISTINCT FROM result OR pg_temp.preimage_snapshot_v2(s.id) IS DISTINCT FROM after_snapshot THEN
   RAISE EXCEPTION 'PRESENTATION_REPLAY_CHANGED_OR_WROTE';END IF;
  FOR bad IN SELECT value FROM jsonb_array_elements(jsonb_build_array(
   jsonb_set(input,'{modal_candidate_intent_v2,presentation_v2,view_digest}',to_jsonb(repeat('0',64))),
   jsonb_set(input,'{modal_candidate_intent_v2,presentation_v2,open_ready}','{"cursor":null,"limit":100}'::jsonb)
  )) LOOP
   BEGIN
    PERFORM public.pay_workbench_session_set_selected_rows(s.id,bad,s.actor_user_id);
    RAISE EXCEPTION 'CHANGED_REPLAY_PRESENTATION_ACCEPTED';
   EXCEPTION WHEN SQLSTATE 'P0001' THEN
    IF SQLERRM IS DISTINCT FROM 'BANKING_PAY_V2_REQUEST_CONFLICT' THEN RAISE;END IF;
   END;
   IF pg_temp.preimage_snapshot_v2(s.id) IS DISTINCT FROM after_snapshot THEN RAISE EXCEPTION 'CONFLICT_WROTE';END IF;
  END LOOP;
  SELECT * INTO STRICT s FROM public.banking_pay_workbench_sessions WHERE id=s.id;
  after_state:=private.pay_workbench_modal_candidate_state_v2(s,'ALL',candidate);
  input:=jsonb_set(jsonb_set(jsonb_set(input,'{modal_candidate_intent_v2,request_id}',
   '"10000000-0000-4000-8000-000000009999"'),'{modal_candidate_intent_v2,options,expected_progress_counter_version}','5'),
   '{modal_candidate_intent_v2,presentation_v2,view_digest}',after_state->'view_digest');
  result:=public.pay_workbench_session_set_selected_rows(s.id,input,s.actor_user_id);
  IF result->>'state_changed' IS DISTINCT FROM 'false' OR result->'presentation_before' IS DISTINCT FROM after_state
   OR pg_temp.preimage_snapshot_v2(s.id) IS DISTINCT FROM after_snapshot THEN RAISE EXCEPTION 'PRESENTATION_NOOP_WROTE';END IF;
  SET LOCAL client_min_messages='notice';
  RAISE NOTICE 'PASS: current preimage and original single-write receipt; stale/malformed no writes; exact replay; bound metadata; genuine noop.';
 END;$proof$;ROLLBACK;`;
 const r=spawnSync(process.env.BANKING_MODAL_LOCAL_PSQL,['-X','-q','-h','127.0.0.1','-p','55441','-U','postgres',
  '-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1'],{input:sql,encoding:'utf8',timeout:60000,maxBuffer:1024*1024});
 assert.equal(r.status,0,r.error?.message||r.stderr);assert.match(r.stderr,/PASS: current preimage/);
});
