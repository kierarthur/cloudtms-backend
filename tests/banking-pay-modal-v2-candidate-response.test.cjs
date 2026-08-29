const assert=require('node:assert/strict');const test=require('node:test');const fs=require('node:fs');const path=require('node:path');
const {spawnSync}=require('node:child_process');const root=path.resolve(__dirname,'..');
const source=()=>fs.readFileSync(path.join(root,'supabase/repeatable/28082026_2313_banking_pay_modal_candidate_selection_response.sql'),'utf8');
test('candidate response internal owner is private and composes existing owners once without another write',()=>{
 const sql=source().split('CREATE OR REPLACE FUNCTION public.pay_workbench_session_set_candidate_ready_selection_v1')[0];
 assert.match(sql,/CREATE OR REPLACE FUNCTION private\.pay_workbench_modal_candidate_selection_response_v2/);
 assert.equal((sql.match(/public\.pay_workbench_session_set_selected_rows\(/g)||[]).length,1);
 assert.match(sql,/public\.pay_workbench_session_get_candidate_summary_page_v1/);
 assert.match(sql,/public\.pay_workbench_session_get_candidate_ready_page_v1/);
 assert.match(sql,/private\.pay_workbench_modal_movement_envelope_v2/);
 assert.match(sql,/presentation_before/);assert.match(sql,/other_candidates_unchanged/);
 assert.match(sql,/32\*1024/);assert.match(sql,/512\*1024/);assert.match(sql,/544\*1024/);
 assert.doesNotMatch(sql,/\b(?:UPDATE|INSERT INTO|DELETE FROM)\s+public\.|\b(?:SUM|ROUND|ABS)\s*\(|GRANT EXECUTE/i);
 assert.match(sql,/VOLATILE SECURITY INVOKER SET search_path TO ''/);
});
test('actual complete candidate response settles the open Ready page and unchanged global Draft authority, with replay and no-op',
 {skip:!process.env.BANKING_MODAL_LOCAL_PSQL},async()=>{
 const setup=fs.readFileSync(path.join(__dirname,'fixtures/28082026_1429_banking_pay_selection_setup.sql'),'utf8');
 const sql=`BEGIN;SET LOCAL statement_timeout='30s';SET LOCAL client_min_messages='warning';
 ${setup}
 UPDATE public.banking_pay_workbench_sessions SET scope_seed_complete=true WHERE id='10000000-0000-4000-8000-000000000005';
 CREATE TEMP TABLE candidate_responses(label text,before_summary jsonb,args jsonb,payload jsonb) ON COMMIT DROP;
 DO $response$
 DECLARE s public.banking_pay_workbench_sessions%ROWTYPE;opts jsonb;before_state jsonb;after_state jsonb;ready jsonb;reply jsonb;again jsonb;summary jsonb;
  open_ready jsonb;args jsonb;initial_summary jsonb;after_session jsonb;after_rows jsonb;request uuid:='10000000-0000-4000-8000-000000009999';
  candidate uuid:='10000000-0000-4000-8000-000000000002';
 BEGIN
  IF current_setting('server_version_num')::integer NOT BETWEEN 170000 AND 179999 THEN RAISE EXCEPTION 'LOCAL_PG17_ONLY';END IF;
  SELECT * INTO STRICT s FROM public.banking_pay_workbench_sessions WHERE id='10000000-0000-4000-8000-000000000005';
  opts:=jsonb_build_object('expected_session_version',s.version,'expected_progress_counter_version',s.progress_counter_version,
   'scope_hash',private.pay_workbench_modal_scope_hash_v2(s,'ALL'),'pay_channel_scope','ALL');
  before_state:=private.pay_workbench_modal_candidate_state_v2(s,'ALL',candidate);
  initial_summary:=public.pay_workbench_session_get_candidate_summary_page_v1(s.id,opts,s.actor_user_id,'CANDIDATE','ASC',NULL,100);
  ready:=public.pay_workbench_session_get_candidate_ready_page_v1(s.id,candidate,opts,s.actor_user_id,NULL,100);
  open_ready:=jsonb_build_object('cursor',ready->'page_anchor','limit',100);
  args:=jsonb_build_object('p_session_id',s.id,'p_candidate_id',candidate,'p_options_json',opts,'p_actor_user_id',s.actor_user_id,
   'p_action','CLEAR_ALL_READY','p_request_id',request,'p_expected_view_digest',before_state->>'view_digest','p_open_ready_json',open_ready);
  IF has_function_privilege('anon','public.pay_workbench_session_set_candidate_ready_selection_v1(uuid,uuid,jsonb,uuid,text,uuid,text,jsonb)','EXECUTE')
   OR has_function_privilege('authenticated','public.pay_workbench_session_set_candidate_ready_selection_v1(uuid,uuid,jsonb,uuid,text,uuid,text,jsonb)','EXECUTE')
   OR NOT has_function_privilege('service_role','public.pay_workbench_session_set_candidate_ready_selection_v1(uuid,uuid,jsonb,uuid,text,uuid,text,jsonb)','EXECUTE')
   OR has_function_privilege('service_role','private.pay_workbench_modal_candidate_selection_response_v2(uuid,uuid,jsonb,uuid,text,uuid,text,jsonb)','EXECUTE')
   THEN RAISE EXCEPTION 'SELECTION_ACL_BOUNDARY_CHANGED';END IF;
  SET LOCAL ROLE service_role;
  reply:=public.pay_workbench_session_set_candidate_ready_selection_v1(s.id,candidate,opts,s.actor_user_id,'CLEAR_ALL_READY',request,before_state->>'view_digest',open_ready);
  RESET ROLE;
  SELECT * INTO STRICT s FROM public.banking_pay_workbench_sessions WHERE id=s.id;
  after_state:=private.pay_workbench_modal_candidate_state_v2(s,'ALL',candidate);
  summary:=public.pay_workbench_session_get_candidate_summary_page_v1(s.id,jsonb_set(opts,'{expected_progress_counter_version}',to_jsonb(s.progress_counter_version)),s.actor_user_id,'CANDIDATE','ASC',NULL,100);
  IF reply->'global' IS DISTINCT FROM summary->'global' OR reply->'candidate' IS DISTINCT FROM after_state->'candidate'
   OR reply->>'view_digest' IS DISTINCT FROM after_state->>'view_digest'
   OR reply#>>'{retention,before_view_digest}' IS DISTINCT FROM before_state->>'view_digest'
   OR reply#>>'{retention,other_candidates_unchanged}' IS DISTINCT FROM 'true'
   OR reply#>>'{retention,membership_unchanged}' IS DISTINCT FROM 'true'
   OR reply#>'{ready_page,candidate}' IS DISTINCT FROM reply->'candidate'
   OR reply#>>'{ready_page,progress_counter_version}' IS DISTINCT FROM reply->>'progress_counter_version'
   OR reply#>>'{candidate,selection_state}' IS DISTINCT FROM 'NONE'
   OR reply#>>'{candidate,selected_display_amount}' IS DISTINCT FROM '0.00'
   OR jsonb_array_length(reply#>'{ready_page,rows}') IS DISTINCT FROM 100 THEN RAISE EXCEPTION 'CANDIDATE_RESPONSE_NOT_ATOMIC';END IF;
  after_session:=to_jsonb(s);
  SELECT jsonb_agg(to_jsonb(r) ORDER BY id) INTO after_rows FROM public.banking_pay_workbench_preview_rows r WHERE session_id=s.id;
  again:=public.pay_workbench_session_set_candidate_ready_selection_v1(s.id,candidate,opts,s.actor_user_id,'CLEAR_ALL_READY',request,before_state->>'view_digest',open_ready);
  IF again IS DISTINCT FROM reply OR after_session IS DISTINCT FROM (SELECT to_jsonb(q) FROM public.banking_pay_workbench_sessions q WHERE q.id=s.id)
   OR after_rows IS DISTINCT FROM (SELECT jsonb_agg(to_jsonb(r) ORDER BY id) FROM public.banking_pay_workbench_preview_rows r WHERE session_id=s.id) THEN
   RAISE EXCEPTION 'CANDIDATE_RESPONSE_REPLAY_WROTE_OR_CHANGED';END IF;
  INSERT INTO candidate_responses VALUES('open',initial_summary,args,reply);
  opts:=jsonb_set(opts,'{expected_progress_counter_version}',to_jsonb(s.progress_counter_version));
  request:='10000000-0000-4000-8000-000000009998';
  args:=jsonb_build_object('p_session_id',s.id,'p_candidate_id',candidate,'p_options_json',opts,'p_actor_user_id',s.actor_user_id,
   'p_action','CLEAR_ALL_READY','p_request_id',request,'p_expected_view_digest',after_state->>'view_digest','p_open_ready_json',NULL);
  reply:=public.pay_workbench_session_set_candidate_ready_selection_v1(s.id,candidate,opts,s.actor_user_id,'CLEAR_ALL_READY',request,after_state->>'view_digest',NULL);
  IF reply->>'state_changed' IS DISTINCT FROM 'false' OR reply ? 'ready_page'
   OR after_session IS DISTINCT FROM (SELECT to_jsonb(q) FROM public.banking_pay_workbench_sessions q WHERE q.id=s.id) THEN
   RAISE EXCEPTION 'CANDIDATE_RESPONSE_NOOP_WROTE';END IF;
  INSERT INTO candidate_responses VALUES('noop',summary,args,reply);
 END;$response$;
 SELECT jsonb_build_object('label',label,'before_summary',before_summary,'args',args,'payload',payload) FROM candidate_responses ORDER BY label;
 ROLLBACK;`;
 const r=spawnSync(process.env.BANKING_MODAL_LOCAL_PSQL,['-X','-q','-A','-t','-h','127.0.0.1','-p','55441','-U','postgres','-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1'],
  {input:sql,encoding:'utf8',timeout:60000,maxBuffer:2*1024*1024});
 assert.equal(r.status,0,r.error?.message||r.stderr);
 const {validateBankingPayModalEnvelope}=await import('../broker/src/banking-pay-modal-v2.js');
 assert.ok(process.env.BANKING_MODAL_FRONTEND_ROOT,'Actual saved frontend checkout is required');
 const {reconcileCandidateSelection}=require(path.join(process.env.BANKING_MODAL_FRONTEND_ROOT,'js/banking-pay-modal-v2-mutation.js'));
 const replies=r.stdout.trim().split(/\r?\n/).map(JSON.parse);assert.equal(replies.length,2);
 for(const {label,before_summary,args,payload} of replies){validateBankingPayModalEnvelope(payload,'selection',args);
  let reads=0;
  const staged=await reconcileCandidateSelection({summary:before_summary,ui:{surface:label==='open'?'candidate':'main'},
   ready:label==='open'?{candidate_id:args.p_candidate_id}:null},payload,{
    ...args.p_options_json,session_id:args.p_session_id,candidate_id:args.p_candidate_id,request_id:args.p_request_id,
    expected_view_digest:args.p_expected_view_digest,open_ready:args.p_open_ready_json
   },async()=>{reads++;throw Error('UNEXPECTED_EXTRA_SUMMARY');});
  assert.equal(reads,0);assert.equal(staged.summary.global,payload.global);assert.equal(staged.summary.view_digest,payload.view_digest);
  assert.equal(staged.ready,payload.ready_page||null);assert.equal(staged.actions,null);assert.equal(staged.blocked,null);
  const base={...payload};delete base.ready_page;assert.ok(Buffer.byteLength(JSON.stringify(base),'utf8')<=32*1024,label);
  if(payload.ready_page)assert.ok(Buffer.byteLength(JSON.stringify(payload.ready_page),'utf8')<=512*1024,label);
 }
});

test('stale view, wrong Ready anchor and oversized replies roll back all candidate selection effects',
 {skip:!process.env.BANKING_MODAL_LOCAL_PSQL},()=>{
 const setup=fs.readFileSync(path.join(__dirname,'fixtures/28082026_1429_banking_pay_selection_setup.sql'),'utf8');
 const sql=`BEGIN;SET LOCAL statement_timeout='30s';SET LOCAL client_min_messages='warning';
 ${setup}
 UPDATE public.banking_pay_workbench_sessions SET scope_seed_complete=true WHERE id='10000000-0000-4000-8000-000000000005';
 CREATE FUNCTION pg_temp.response_snapshot_v2(p_id uuid) RETURNS jsonb LANGUAGE sql AS $snapshot$
  SELECT jsonb_build_object('session',(SELECT to_jsonb(s) FROM public.banking_pay_workbench_sessions s WHERE id=p_id),
   'rows',(SELECT jsonb_agg(to_jsonb(r) ORDER BY id) FROM public.banking_pay_workbench_preview_rows r WHERE session_id=p_id),
   'audit_count',(SELECT count(*) FROM public.audit_events WHERE object_id_text=p_id::text),
   'counter',(SELECT to_jsonb(c) FROM public.app_change_counters c WHERE entity_key='banking_pay_workbench_session:'||p_id::text));
 $snapshot$;
 DO $negative$
 DECLARE s public.banking_pay_workbench_sessions%ROWTYPE;options jsonb;state jsonb;before_snapshot jsonb;ready jsonb;
  open_ready jsonb;digest text;expected text;n integer;old_name text;old_json jsonb;payment_id uuid;
  candidate uuid:='10000000-0000-4000-8000-000000000002';
 BEGIN
  IF current_setting('server_version_num')::integer NOT BETWEEN 170000 AND 179999 THEN RAISE EXCEPTION 'LOCAL_PG17_ONLY';END IF;
  SELECT * INTO STRICT s FROM public.banking_pay_workbench_sessions WHERE id='10000000-0000-4000-8000-000000000005';
  options:=jsonb_build_object('expected_session_version',s.version,'expected_progress_counter_version',s.progress_counter_version,
   'scope_hash',private.pay_workbench_modal_scope_hash_v2(s,'ALL'),'pay_channel_scope','ALL');
  ready:=public.pay_workbench_session_get_candidate_ready_page_v1(s.id,candidate,options,s.actor_user_id,NULL,100);
  SELECT display_name INTO STRICT old_name FROM public.candidates WHERE id=candidate;
  SELECT id,row_json INTO STRICT payment_id,old_json FROM public.banking_pay_workbench_preview_rows WHERE session_id=s.id AND row_ordinal=1;
  FOR n IN 1..4 LOOP
   IF n=3 THEN UPDATE public.candidates SET display_name=repeat('Z',20000) WHERE id=candidate;END IF;
   IF n=4 THEN
    UPDATE public.candidates SET display_name=old_name WHERE id=candidate;
    UPDATE public.banking_pay_workbench_preview_rows SET row_json=old_json||jsonb_build_object('synthetic_oversized_detail',repeat('£',270000)) WHERE id=payment_id;
   END IF;
   state:=private.pay_workbench_modal_candidate_state_v2(s,'ALL',candidate);
   digest:=CASE WHEN n=1 THEN repeat('0',64) ELSE state->>'view_digest' END;
   open_ready:=CASE WHEN n=2 THEN jsonb_build_object('cursor',ready->'next_cursor','limit',100)
     WHEN n=4 THEN jsonb_build_object('cursor',NULL,'limit',1) ELSE NULL END;
   expected:=CASE n WHEN 1 THEN 'BANKING_PAY_V2_STALE_VIEW' WHEN 2 THEN 'BANKING_PAY_V2_STALE_CURSOR'
     WHEN 3 THEN 'BANKING_PAY_V2_SELECTION_TOO_LARGE' ELSE 'BANKING_PAY_V2_READY_TOO_LARGE' END;
   before_snapshot:=pg_temp.response_snapshot_v2(s.id);
   BEGIN
    PERFORM public.pay_workbench_session_set_candidate_ready_selection_v1(s.id,candidate,options,s.actor_user_id,
     'CLEAR_ALL_READY','10000000-0000-4000-8000-000000009999',digest,open_ready);
    RAISE EXCEPTION 'INVALID_CANDIDATE_RESPONSE_ACCEPTED %',n;
   EXCEPTION WHEN SQLSTATE 'P0001' THEN
    IF SQLERRM IS DISTINCT FROM expected THEN RAISE;END IF;
   END;
   IF pg_temp.response_snapshot_v2(s.id) IS DISTINCT FROM before_snapshot THEN RAISE EXCEPTION 'RESPONSE_FAILURE_LEFT_SELECTION_WRITES %',n;END IF;
  END LOOP;
  SET LOCAL client_min_messages='notice';
  RAISE NOTICE 'PASS: stale view and non-anchor plus oversized base and multibyte Ready page all roll back rows/session/audit/change counters.';
 END;$negative$;ROLLBACK;`;
 const r=spawnSync(process.env.BANKING_MODAL_LOCAL_PSQL,['-X','-q','-h','127.0.0.1','-p','55441','-U','postgres','-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1'],
  {input:sql,encoding:'utf8',timeout:60000,maxBuffer:1024*1024});
 assert.equal(r.status,0,r.error?.message||r.stderr);assert.match(r.stderr,/PASS: stale view and non-anchor/);
});

test('actual promotion and last-selectable recovery demotion return complete current child or an explicit absent candidate',
 {skip:!process.env.BANKING_MODAL_LOCAL_PSQL},async()=>{
 const setup=fs.readFileSync(path.join(__dirname,'fixtures/28082026_1429_banking_pay_selection_setup.sql'),'utf8');
 const sql=`BEGIN;SET LOCAL statement_timeout='30s';SET LOCAL client_min_messages='warning';
 ${setup}
 UPDATE public.banking_pay_workbench_sessions SET scope_seed_complete=true WHERE id='10000000-0000-4000-8000-000000000005';
 CREATE TEMP TABLE edge_responses(label text,before_summary jsonb,after_summary jsonb,args jsonb,payload jsonb) ON COMMIT DROP;
 DO $edge$
 DECLARE s public.banking_pay_workbench_sessions%ROWTYPE;opts jsonb;state jsonb;reply jsonb;args jsonb;n integer;action text;
  before_summary jsonb;after_summary jsonb;
  candidate uuid:='10000000-0000-4000-8000-000000000002';request uuid;
 BEGIN
  IF current_setting('server_version_num')::integer NOT BETWEEN 170000 AND 179999 THEN RAISE EXCEPTION 'LOCAL_PG17_ONLY';END IF;
  FOR n IN 1..2 LOOP
   SELECT * INTO STRICT s FROM public.banking_pay_workbench_sessions WHERE id='10000000-0000-4000-8000-000000000005';
   IF n=2 THEN
    -- Preserve the real recovery produced by the original owner. Make its
    -- positive payments nonselectable through the existing canonical flags;
    -- CLEAR must then demote that last selectable recovery and remove the row.
    UPDATE public.banking_pay_workbench_preview_rows SET selected=false,selection_state='NOT_SELECTABLE',
     row_json=row_json||'{"selection_allowed":false,"draftable":false,"is_ready_for_draft":false,"selected":false,"selection_state":"NOT_SELECTABLE"}'::jsonb
     WHERE session_id=s.id AND candidate_id=candidate AND row_ordinal<=107;
   END IF;
   opts:=jsonb_build_object('expected_session_version',s.version,'expected_progress_counter_version',s.progress_counter_version,
    'scope_hash',private.pay_workbench_modal_scope_hash_v2(s,'ALL'),'pay_channel_scope','ALL');
   state:=private.pay_workbench_modal_candidate_state_v2(s,'ALL',candidate);
   before_summary:=public.pay_workbench_session_get_candidate_summary_page_v1(s.id,opts,s.actor_user_id,'CANDIDATE','ASC',NULL,100);
   IF state->'candidate' IS NOT DISTINCT FROM 'null'::jsonb THEN RAISE EXCEPTION 'EDGE_FIXTURE_HAS_NO_CURRENT_CANDIDATE';END IF;
   action:=CASE n WHEN 1 THEN 'SELECT_ALL_READY' ELSE 'CLEAR_ALL_READY' END;
   request:=CASE n WHEN 1 THEN '10000000-0000-4000-8000-000000009998'::uuid ELSE '10000000-0000-4000-8000-000000009999'::uuid END;
   args:=jsonb_build_object('p_session_id',s.id,'p_candidate_id',candidate,'p_options_json',opts,'p_actor_user_id',s.actor_user_id,
    'p_action',action,'p_request_id',request,'p_expected_view_digest',state->>'view_digest','p_open_ready_json','{"cursor":null,"limit":100}'::jsonb);
   reply:=public.pay_workbench_session_set_candidate_ready_selection_v1(s.id,candidate,opts,s.actor_user_id,action,request,state->>'view_digest','{"cursor":null,"limit":100}'::jsonb);
   IF reply->>'state_changed' IS DISTINCT FROM 'true' OR COALESCE((reply->>'movement_count')::bigint,0)<1 THEN
    RAISE EXCEPTION 'EDGE_RESPONSE_DID_NOT_MOVE_RECOVERY';END IF;
   IF n=1 AND (reply#>>'{candidate,selection_state}' IS DISTINCT FROM 'ALL'
    OR reply#>>'{candidate,selected_deduction_exists}' IS DISTINCT FROM 'true'
    OR reply#>>'{candidate,selected_display_amount}' IS DISTINCT FROM '1040.00'
    OR reply#>>'{ready_page,total_count}' IS DISTINCT FROM '109') THEN RAISE EXCEPTION 'PROMOTED_RESPONSE_WRONG';END IF;
   IF n=2 AND (reply->>'candidate_absent' IS DISTINCT FROM 'true' OR reply->'candidate' IS DISTINCT FROM 'null'::jsonb
    OR reply#>>'{retention,membership_unchanged}' IS DISTINCT FROM 'false'
    OR reply#>>'{ready_page,total_count}' IS DISTINCT FROM '0' OR reply#>'{ready_page,rows}' IS DISTINCT FROM '[]'::jsonb
    OR reply#>'{ready_page,candidate}' IS DISTINCT FROM 'null'::jsonb) THEN RAISE EXCEPTION 'DEPARTED_CANDIDATE_NOT_EXPLICIT';END IF;
   after_summary:=public.pay_workbench_session_get_candidate_summary_page_v1(s.id,
    opts||jsonb_build_object('expected_progress_counter_version',reply->'progress_counter_version'),s.actor_user_id,'CANDIDATE','ASC',NULL,100);
   INSERT INTO edge_responses VALUES(n::text,before_summary,after_summary,args,reply);
  END LOOP;
 END;$edge$;
 SELECT jsonb_build_object('label',label,'before_summary',before_summary,'after_summary',after_summary,'args',args,'payload',payload) FROM edge_responses ORDER BY label;
 ROLLBACK;`;
 const r=spawnSync(process.env.BANKING_MODAL_LOCAL_PSQL,['-X','-q','-A','-t','-h','127.0.0.1','-p','55441','-U','postgres','-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1'],
  {input:sql,encoding:'utf8',timeout:60000,maxBuffer:2*1024*1024});
 assert.equal(r.status,0,r.error?.message||r.stderr);
 const {validateBankingPayModalEnvelope}=await import('../broker/src/banking-pay-modal-v2.js');
 const {reconcileCandidateSelection}=require(path.join(process.env.BANKING_MODAL_FRONTEND_ROOT,'js/banking-pay-modal-v2-mutation.js'));
 const replies=r.stdout.trim().split(/\r?\n/).map(JSON.parse);assert.equal(replies.length,2);
 for(const {args,payload,before_summary,after_summary} of replies){validateBankingPayModalEnvelope(payload,'selection',args);
  let reads=0;
  const staged=await reconcileCandidateSelection({summary:before_summary,ui:{surface:'candidate'},ready:{candidate_id:args.p_candidate_id}},payload,{
   ...args.p_options_json,session_id:args.p_session_id,candidate_id:args.p_candidate_id,request_id:args.p_request_id,
   expected_view_digest:args.p_expected_view_digest,open_ready:args.p_open_ready_json
  },async query=>{reads++;assert.equal(query.cursor,before_summary.page_anchor);return after_summary;});
  assert.equal(reads,payload.candidate_absent?1:0);assert.equal(staged.summary.view_digest,after_summary.view_digest);
  assert.equal(staged.ready,payload.ready_page);
 }
});
