const assert=require('node:assert/strict');const fs=require('node:fs');const path=require('node:path');const test=require('node:test');
const {spawnSync}=require('node:child_process');const {pathToFileURL}=require('node:url');const root=path.resolve(__dirname,'..');
test('public issue detail preserves existing member owners and has no write or financial equation',()=>{
 const sql=fs.readFileSync(path.join(root,'supabase/repeatable/28082026_2046_banking_pay_modal_issue_detail.sql'),'utf8');
 for(const family of ['finance_task_members','bank_task_members','source_issue_members','issue_index','context','cursor_decode'])
  assert.ok(sql.includes('private.pay_workbench_modal_'+family+'_v2'),family);
 for(const name of ['pay_workbench_session_get_action_required_detail_v1','pay_workbench_session_get_blocked_detail_v1'])
  assert.ok(sql.includes('FUNCTION public.'+name),name);
 assert.match(sql,/FROM PUBLIC, anon, authenticated/);assert.match(sql,/TO service_role/);
 assert.doesNotMatch(sql,/\b(?:SUM\s*\(|INSERT INTO|UPDATE public\.|DELETE FROM|pay_workbench_prepare_draft\s*\()/i);
 assert.doesNotMatch(sql,/pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
});
test('actual bounded issue details pass frontend and Worker and preserve every original payment payload',
 {skip:!process.env.BANKING_MODAL_LOCAL_PSQL||!process.env.BANKING_MODAL_FRONTEND_ROOT},async()=>{
 const result=spawnSync(process.env.BANKING_MODAL_LOCAL_PSQL,['-X','-q','-A','-t','-h','127.0.0.1','-p','55441','-U','postgres',
  '-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1','-f','tests/28082026_2046_banking_pay_issue_detail_runtime.sql'],
  {cwd:root,encoding:'utf8',timeout:90000,maxBuffer:5*1024*1024});
 assert.equal(result.status,0,result.stderr||result.error?.message);
 assert.match(result.stderr,/PASS: public issue detail/);
 const worker=await import(pathToFileURL(path.join(root,'broker/src/banking-pay-modal-v2.js')).href);
 const issues=require(path.join(process.env.BANKING_MODAL_FRONTEND_ROOT,'js/banking-pay-modal-v2-issues.js'));
 const results=result.stdout.trim().split(/\r?\n/).filter(Boolean).map(line=>JSON.parse(line));assert.equal(results.length,5);
 for(const {kind,key,cursor,limit,payload,summary} of results){
  assert.doesNotThrow(()=>issues.validateDetail(payload,summary,kind,key,cursor,limit));
  const args={p_session_id:payload.session_id,p_options_json:{expected_session_version:payload.session_version,
   expected_progress_counter_version:payload.progress_counter_version,scope_hash:payload.scope_hash,pay_channel_scope:'ALL'},
   p_limit:limit,p_cursor:cursor,[kind==='actions'?'p_task_key':'p_blocker_key']:key};
  assert.doesNotThrow(()=>worker.validateBankingPayModalEnvelope(payload,kind==='actions'?'actionDetail':'blockedDetail',args));
  assert.ok(Buffer.byteLength(JSON.stringify(payload),'utf8')<=256*1024);
 }
});
