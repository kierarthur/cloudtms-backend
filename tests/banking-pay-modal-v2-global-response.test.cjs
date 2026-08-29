const assert=require('node:assert/strict');const test=require('node:test');const fs=require('node:fs');
const path=require('node:path');const {spawnSync}=require('node:child_process');const root=path.resolve(__dirname,'..');
const sqlName='29082026_0002_banking_pay_modal_global_selection_response.sql';
const read=p=>fs.readFileSync(path.join(root,p),'utf8');
test('global response uses the original whole-scope owner once with service-only access and no new financial calculation',()=>{
 const sql=read('supabase/repeatable/'+sqlName);
 assert.equal((sql.match(/public\.pay_workbench_session_set_selected_rows\(/g)||[]).length,1);
 assert.match(sql,/modal_global_intent_v2/);assert.match(sql,/presentation_v2/);assert.match(sql,/requires_summary_refresh/);
 assert.match(sql,/public\.pay_workbench_session_get_candidate_summary_page_v1/);
 assert.match(sql,/SECURITY DEFINER SET search_path TO ''/);assert.match(sql,/32\*1024/);
 assert.doesNotMatch(sql,/\b(?:LOOP|UPDATE|INSERT INTO|DELETE FROM|SUM|ROUND|ABS)\b|set_candidate_ready_selection_v1/i);
 assert.match(sql,/FROM PUBLIC, anon, authenticated/);assert.match(sql,/TO service_role/);
});
test('real public filtered header preserves106-candidate scope, exact replay/no-op and view-failure rollback',
 {skip:!process.env.BANKING_MODAL_LOCAL_PSQL},async()=>{
 const prefix=read('tests/28082026_1755_banking_pay_global_selection_runtime.sql').split('DO $global_selection$')[0]
  .replace('\\ir fixtures/28082026_1429_banking_pay_selection_setup.sql',()=>read('tests/fixtures/28082026_1429_banking_pay_selection_setup.sql'));
 const sql=prefix+`
 UPDATE public.banking_pay_workbench_sessions SET scope_seed_complete=true WHERE id='10000000-0000-4000-8000-000000000005';
 CREATE TEMP TABLE global_replies(label text,args jsonb,before_summary jsonb,after_summary jsonb,payload jsonb) ON COMMIT DROP;
 DO $global$
 DECLARE s public.banking_pay_workbench_sessions%ROWTYPE;opts jsonb;state jsonb;before_summary jsonb;after_summary jsonb;
  reply jsonb;again jsonb;args jsonb;rows_before jsonb;session_before jsonb;other_partition jsonb;failure text;
  request uuid:='40000000-0000-4000-8000-000000000001';
 BEGIN
  IF current_database()<>'banking_modal_v2_test' OR current_setting('server_version_num')::integer NOT BETWEEN 170000 AND 179999 THEN
   RAISE EXCEPTION 'LOCAL_PG17_ONLY';END IF;
  SELECT * INTO STRICT s FROM public.banking_pay_workbench_sessions WHERE id='10000000-0000-4000-8000-000000000005';
  opts:=jsonb_build_object('expected_session_version',s.version,'expected_progress_counter_version',s.progress_counter_version,
   'scope_hash',private.pay_workbench_modal_scope_hash_v2(s,'PAYE'),'pay_channel_scope','PAYE');
  state:=private.pay_workbench_modal_candidate_state_v2(s,'PAYE',NULL);
  before_summary:=public.pay_workbench_session_get_candidate_summary_page_v1(s.id,opts,s.actor_user_id,'CANDIDATE','ASC',NULL,100);
  SELECT jsonb_agg(to_jsonb(r) ORDER BY id) INTO rows_before FROM public.banking_pay_workbench_preview_rows r WHERE session_id=s.id;
  session_before:=to_jsonb(s);
  BEGIN
   PERFORM public.pay_workbench_session_set_filtered_ready_selection_v1(s.id,opts,s.actor_user_id,'CLEAR_ALL_READY',request,repeat('0',64));
   RAISE EXCEPTION 'STALE_GLOBAL_VIEW_ACCEPTED';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN GET STACKED DIAGNOSTICS failure=MESSAGE_TEXT;
   IF failure<>'BANKING_PAY_V2_STALE_VIEW' THEN RAISE;END IF;
  END;
  IF rows_before IS DISTINCT FROM (SELECT jsonb_agg(to_jsonb(r) ORDER BY id) FROM public.banking_pay_workbench_preview_rows r WHERE session_id=s.id)
   OR session_before IS DISTINCT FROM (SELECT to_jsonb(q) FROM public.banking_pay_workbench_sessions q WHERE q.id=s.id) THEN RAISE EXCEPTION 'STALE_GLOBAL_WROTE';END IF;
  BEGIN
   PERFORM public.pay_workbench_session_set_selected_rows(s.id,jsonb_build_object('modal_global_intent_v2',
    jsonb_build_object('request_id',request,'action','CLEAR_ALL_READY','options',opts,'presentation_v2',
     jsonb_build_object('view_digest',state->>'view_digest','open_ready',jsonb_build_object('cursor',NULL,'limit',100)))),s.actor_user_id);
   RAISE EXCEPTION 'GLOBAL_CHILD_ACCEPTED';
  EXCEPTION WHEN SQLSTATE '22023' THEN NULL;END;
  SELECT jsonb_agg(to_jsonb(r) ORDER BY id) INTO other_partition FROM public.banking_pay_workbench_preview_rows r WHERE session_id=s.id AND row_json->>'pay_channel'='UMBRELLA';
  IF has_function_privilege('anon','public.pay_workbench_session_set_filtered_ready_selection_v1(uuid,jsonb,uuid,text,uuid,text)','EXECUTE')
   OR has_function_privilege('authenticated','public.pay_workbench_session_set_filtered_ready_selection_v1(uuid,jsonb,uuid,text,uuid,text)','EXECUTE') THEN RAISE EXCEPTION 'GLOBAL_BROWSER_GRANT';END IF;
  args:=jsonb_build_object('p_session_id',s.id,'p_options_json',opts,'p_actor_user_id',s.actor_user_id,
   'p_action','CLEAR_ALL_READY','p_request_id',request,'p_expected_view_digest',state->>'view_digest');
  SET LOCAL ROLE service_role;
  reply:=public.pay_workbench_session_set_filtered_ready_selection_v1(s.id,opts,s.actor_user_id,'CLEAR_ALL_READY',request,state->>'view_digest');
  RESET ROLE;
  SELECT * INTO STRICT s FROM public.banking_pay_workbench_sessions WHERE id=s.id;
  IF reply->>'state_changed' IS DISTINCT FROM 'true' OR reply->>'requires_summary_refresh' IS DISTINCT FROM 'true'
   OR reply#>>'{global,candidate_count}' IS DISTINCT FROM '106' OR reply#>>'{global,selected_ready_count}' IS DISTINCT FROM '0'
   OR EXISTS(SELECT 1 FROM private.pay_workbench_modal_ready_members_v2(s,'PAYE') WHERE selected)
   OR other_partition IS DISTINCT FROM (SELECT jsonb_agg(to_jsonb(r) ORDER BY id) FROM public.banking_pay_workbench_preview_rows r WHERE session_id=s.id AND row_json->>'pay_channel'='UMBRELLA')
   THEN RAISE EXCEPTION 'GLOBAL_RESPONSE_SCOPE_MISMATCH';END IF;
  rows_before:=(SELECT jsonb_agg(to_jsonb(r) ORDER BY id) FROM public.banking_pay_workbench_preview_rows r WHERE session_id=s.id);
  session_before:=to_jsonb(s);
  again:=public.pay_workbench_session_set_filtered_ready_selection_v1(s.id,opts,s.actor_user_id,'CLEAR_ALL_READY',request,state->>'view_digest');
  IF again IS DISTINCT FROM reply OR session_before IS DISTINCT FROM (SELECT to_jsonb(q) FROM public.banking_pay_workbench_sessions q WHERE q.id=s.id)
   OR rows_before IS DISTINCT FROM (SELECT jsonb_agg(to_jsonb(r) ORDER BY id) FROM public.banking_pay_workbench_preview_rows r WHERE session_id=s.id)
   THEN RAISE EXCEPTION 'GLOBAL_RESPONSE_REPLAY_CHANGED';END IF;
  opts:=jsonb_set(opts,'{expected_progress_counter_version}',to_jsonb(s.progress_counter_version));
  after_summary:=public.pay_workbench_session_get_candidate_summary_page_v1(s.id,opts,s.actor_user_id,'CANDIDATE','ASC',before_summary->>'page_anchor',100);
  IF after_summary->'global' IS DISTINCT FROM reply->'global' OR after_summary->>'view_digest' IS DISTINCT FROM reply->>'view_digest' THEN RAISE EXCEPTION 'GLOBAL_SUMMARY_MISMATCH';END IF;
  INSERT INTO global_replies VALUES('clear',args,before_summary,after_summary,reply);
  request:='40000000-0000-4000-8000-000000000002';
  args:=args||jsonb_build_object('p_options_json',opts,'p_request_id',request,'p_expected_view_digest',reply->>'view_digest');
  reply:=public.pay_workbench_session_set_filtered_ready_selection_v1(s.id,opts,s.actor_user_id,'CLEAR_ALL_READY',request,reply->>'view_digest');
  IF reply->>'state_changed' IS DISTINCT FROM 'false' OR session_before IS DISTINCT FROM (SELECT to_jsonb(q) FROM public.banking_pay_workbench_sessions q WHERE q.id=s.id)
   THEN RAISE EXCEPTION 'GLOBAL_NOOP_WROTE';END IF;
  INSERT INTO global_replies VALUES('noop',args,after_summary,after_summary,reply);
 END;$global$;
 SELECT jsonb_build_object('label',label,'args',args,'before_summary',before_summary,'after_summary',after_summary,'payload',payload) FROM global_replies ORDER BY label;
 ROLLBACK;`;
 const r=spawnSync(process.env.BANKING_MODAL_LOCAL_PSQL,['-X','-q','-A','-t','-h','127.0.0.1','-p','55441','-U','postgres','-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1'],
  {input:sql,encoding:'utf8',cwd:root,timeout:60000,maxBuffer:2*1024*1024});
 assert.equal(r.status,0,r.error?.message||r.stderr);
 assert.ok(process.env.BANKING_MODAL_FRONTEND_ROOT,'Actual frontend source is required');
 const {validateBankingPayModalEnvelope}=await import('../broker/src/banking-pay-modal-v2.js');
 const {reconcileGlobalSelection}=require(path.join(process.env.BANKING_MODAL_FRONTEND_ROOT,'js/banking-pay-modal-v2-mutation.js'));
 const replies=r.stdout.trim().split(/\r?\n/).map(JSON.parse);assert.equal(replies.length,2);
 for(const {args,payload,before_summary,after_summary} of replies){
  validateBankingPayModalEnvelope(payload,'globalSelection',args);
  let reads=0;
  const next=await reconcileGlobalSelection({summary:before_summary,ui:{surface:'main'}},payload,{
   ...args.p_options_json,session_id:args.p_session_id,request_id:args.p_request_id,expected_view_digest:args.p_expected_view_digest
  },async query=>{reads++;assert.equal(query.cursor,before_summary.page_anchor);return after_summary;});
  assert.equal(reads,1);assert.equal(next.summary,after_summary);assert.equal(next.summary.rows.length,100);
  assert.deepEqual(payload.global,after_summary.global);assert.equal(payload.view_digest,after_summary.view_digest);
  assert.equal(payload.requires_summary_refresh,true);assert.equal('candidate' in payload,false);assert.equal('ready_page' in payload,false);
  assert.ok(Buffer.byteLength(JSON.stringify(payload),'utf8')<=32*1024);
 }
});
