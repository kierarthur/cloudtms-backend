const assert=require('node:assert/strict');const fs=require('node:fs');const path=require('node:path');const test=require('node:test');
const {spawnSync}=require('node:child_process');const root=path.resolve(__dirname,'..');
const {pathToFileURL}=require('node:url');
test('Action and Updating paging is one bounded read over complete tasks, not payment selection',()=>{
 const sql=fs.readFileSync(path.join(root,'supabase/repeatable/29082026_1831_banking_pay_action_list_presentation_v1.sql'),'utf8');
 for(const owner of ['task_summaries','context','draft_gate','cursor_decode','cursor_encode'])
  assert.ok(sql.includes(`private.pay_workbench_modal_${owner}_v2`),owner);
 assert.match(sql,/public\.pay_workbench_session_get_action_required_page_v1/);
 assert.match(sql,/p_view NOT IN \('ACTION_REQUIRED','UPDATING'\)/);
 assert.match(sql,/p_sort_key NOT IN \('TITLE','CANDIDATES','PAYMENTS','AMOUNT'\)/);
 for(const field of ['candidate_name','candidate_reference','payment_label','payment_date','affected_display_amount','linked_timesheet_id'])
  assert.match(sql,new RegExp(field),field);
 assert.match(sql,/FROM tasks t\)\s*,\s*'valid_presentation'/,
  'the original complete-task validation remains authoritative');
 assert.match(sql,/BANKING_PAY_V2_INVALID_TASK_PRESENTATION/,
  'new display facts have their own fail-closed validation');
 assert.match(sql,/p_limit NOT BETWEEN 1 AND 100/);assert.match(sql,/256\*1024/);
 assert.match(sql,/SECURITY DEFINER SET search_path TO ''/);assert.match(sql,/TO service_role/);
 assert.match(sql,/NOTIFY pgrst/);assert.doesNotMatch(sql,/\b(?:OFFSET|INSERT INTO|UPDATE public\.|DELETE FROM|EXECUTE|LOOP)\b(?! ON)/i);
 assert.doesNotMatch(sql,/pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
});
test('actual Action pages retain complete tasks and reject changed cursor scope',
 {skip:!process.env.BANKING_MODAL_LOCAL_PSQL||!process.env.BANKING_MODAL_FRONTEND_ROOT},async()=>{
 const r=spawnSync(process.env.BANKING_MODAL_LOCAL_PSQL,['-X','-q','-A','-t','-h','127.0.0.1','-p','55441','-U','postgres',
  '-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1','-f','tests/28082026_2130_banking_pay_task_pages_runtime.sql'],
  {cwd:root,encoding:'utf8',timeout:60000,maxBuffer:4*1024*1024});
 assert.equal(r.status,0,r.stderr||r.error?.message);assert.match(r.stderr,/PASS: actual Action task paging/);
 const worker=await import(pathToFileURL(path.join(root,'broker/src/banking-pay-modal-v2.js')).href);
 const issues=require(path.join(process.env.BANKING_MODAL_FRONTEND_ROOT,'js/banking-pay-modal-v2-issues.js'));
 const results=r.stdout.trim().split(/\r?\n/).filter(line=>line.startsWith('{')).map(line=>JSON.parse(line));
 assert.equal(results.length,19);
 for(const {label,payload,summary} of results){
  const limit=label.startsWith('forty_')?40:100,cursor=payload.page_number>1?'current_boundary':null;
  const args={p_session_id:payload.session_id,p_options_json:{expected_session_version:payload.session_version,
   expected_progress_counter_version:payload.progress_counter_version,scope_hash:payload.scope_hash,pay_channel_scope:'ALL'},
   p_sort_key:payload.sort_key,p_sort_direction:payload.sort_direction,p_view:payload.view,p_search:payload.search,p_limit:limit,p_cursor:cursor};
  assert.doesNotThrow(()=>worker.validateBankingPayModalEnvelope(payload,'actions',args),label);
  assert.doesNotThrow(()=>issues.validate(payload,summary,'actions',cursor,limit,payload.view),label);
 }
 const updating=results.find(result=>result.label==='updating_first');
 assert.ok(updating,'updating_first');
 const mutations=[
  row=>{row.identity='0'.repeat(64);},
  row=>{row.title_message_id=`${row.title_message_id||'UPDATING'}_CHANGED`;},
  row=>{row.title=`${row.title} changed`;},
  row=>{row.affected_candidate_count+=1;},
  row=>{row.affected_payment_count_complete=true;row.affected_payment_count=0;}
 ];
 for(const mutate of mutations){
  const changed=structuredClone(updating.payload);mutate(changed.rows[0]);
  const args={p_session_id:changed.session_id,p_options_json:{expected_session_version:changed.session_version,
   expected_progress_counter_version:changed.progress_counter_version,scope_hash:changed.scope_hash,pay_channel_scope:'ALL'},
   p_sort_key:'TITLE',p_sort_direction:'ASC',p_view:'UPDATING',p_search:'',p_limit:100,p_cursor:null};
  assert.throws(()=>worker.validateBankingPayModalEnvelope(changed,'actions',args),/BANKING_PAY_V2_INVALID_RESPONSE/);
  assert.throws(()=>issues.validate(changed,updating.summary,'actions',null,100,'UPDATING'),/BANKING_PAY_V2_INVALID_RESPONSE/);
 }
});
