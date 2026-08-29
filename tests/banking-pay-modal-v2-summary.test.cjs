const assert=require('node:assert/strict');const fs=require('node:fs');const path=require('node:path');const test=require('node:test');
const {spawnSync}=require('node:child_process');const {pathToFileURL}=require('node:url');const root=path.resolve(__dirname,'..');
test('public summary composes existing value and Draft owners without new arithmetic or mutation',()=>{
 const sql=fs.readFileSync(path.join(root,'supabase/repeatable/28082026_2035_banking_pay_modal_summary.sql'),'utf8');
 for(const name of ['summary_context','candidate_page','draft_gate','issue_index','context'])assert.ok(sql.includes(`private.pay_workbench_modal_${name}_v2`));
 assert.match(sql,/SECURITY DEFINER SET search_path TO ''/);
 assert.match(sql,/FROM PUBLIC, anon, authenticated/);assert.match(sql,/TO service_role/);
 assert.match(sql,/128\*1024/);
 assert.doesNotMatch(sql,/\b(?:SUM\s*\(|INSERT INTO|UPDATE public\.|DELETE FROM|pay_workbench_prepare_draft\s*\()/i);
 assert.doesNotMatch(sql,/pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
});
test('real public summaries pass both current Worker and frontend contracts after all-page selection',
 {skip:!process.env.BANKING_MODAL_LOCAL_PSQL||!process.env.BANKING_MODAL_FRONTEND_ROOT},async()=>{
 const result=spawnSync(process.env.BANKING_MODAL_LOCAL_PSQL,['-X','-q','-A','-t','-h','127.0.0.1','-p','55441','-U','postgres',
  '-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1','-f','tests/28082026_2038_banking_pay_summary_runtime.sql'],
  {cwd:root,encoding:'utf8',timeout:60000,maxBuffer:4*1024*1024});
 assert.equal(result.status,0,result.stderr||result.error?.message);
 assert.match(result.stderr,/PASS: actual public summary105 candidates/);
 const worker=await import(pathToFileURL(path.join(root,'broker/src/banking-pay-modal-v2.js')).href);
 const table=require(path.join(process.env.BANKING_MODAL_FRONTEND_ROOT,'js/banking-pay-modal-v2-table.js'));
 const rows=result.stdout.trim().split(/\r?\n/).filter(Boolean).map(line=>JSON.parse(line));assert.equal(rows.length,11);
 for(const {label,payload} of rows){
  assert.doesNotThrow(()=>table.validateSummary(payload),label);
  const args={p_session_id:payload.session_id,p_options_json:{expected_session_version:payload.session_version,
   expected_progress_counter_version:payload.progress_counter_version,scope_hash:payload.scope_hash,pay_channel_scope:'ALL'},
   p_sort_key:payload.sort_key,p_sort_direction:payload.sort_direction,p_limit:100,p_cursor:/next|previous/.test(label)?'fixture_cursor':null};
  assert.doesNotThrow(()=>worker.validateBankingPayModalEnvelope(payload,'summary',args),label);
  assert.ok(Buffer.byteLength(JSON.stringify(payload),'utf8')<=128*1024,label);
 }
});
