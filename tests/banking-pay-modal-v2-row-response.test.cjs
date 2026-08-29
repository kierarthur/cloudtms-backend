'use strict';
const assert=require('node:assert/strict');const test=require('node:test');const fs=require('node:fs');const path=require('node:path');
const {spawnSync}=require('node:child_process');const root=path.resolve(__dirname,'..');
function rowSource(){const folder=path.join(root,'supabase/repeatable');const files=fs.readdirSync(folder).filter(n=>/^\d{8}_\d{4}_banking_pay_modal_row_selection_response\.sql$/.test(n));
 assert.equal(files.length,1,'one exact row response definition required');return fs.readFileSync(path.join(folder,files[0]),'utf8');}
test('row response delegates one unchanged bounded ROW_PATCH and creates no financial or receipt owner',()=>{
 const sql=rowSource();assert.match(sql,/CREATE OR REPLACE FUNCTION public\.pay_workbench_session_set_ready_rows_v1/);
 assert.equal((sql.match(/public\.pay_workbench_session_set_selected_rows\(/g)||[]).length,1);
 assert.match(sql,/private\.pay_workbench_modal_selection_response_finish_v2/);
 assert.match(sql,/private\.pay_workbench_modal_ready_members_v2/);
 assert.match(sql,/'section','canonical_preview_lines'/);assert.match(sql,/'select_preview_row_ids'/);assert.match(sql,/'deselect_preview_row_ids'/);
 assert.match(sql,/FOR UPDATE/);assert.match(sql,/100/);assert.match(sql,/SECURITY DEFINER SET search_path TO ''/);
 assert.doesNotMatch(sql,/modal_candidate_intent_v2|modal_global_intent_v2|_audit_insert|candidate_selection_receipt_v2|\b(?:UPDATE|INSERT INTO|DELETE FROM)\s+public\.|\b(?:SUM|ROUND|ABS)\s*\(/i);
 assert.doesNotMatch(sql,/pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
 assert.match(sql,/REVOKE ALL[^;]+FROM PUBLIC, anon, authenticated/);
 assert.match(sql,/GRANT EXECUTE[^;]+TO service_role/);
});
function run(sql){assert.ok(process.env.BANKING_MODAL_LOCAL_PSQL);const r=spawnSync(process.env.BANKING_MODAL_LOCAL_PSQL,
 ['-X','-q','-A','-t','-h','127.0.0.1','-p','55441','-U','postgres','-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1'],
 {input:sql,encoding:'utf8',timeout:60000,maxBuffer:4*1024*1024});
 assert.equal(r.status,0,r.error?.message||r.stderr);return r.stdout.trim().split(/\r?\n/).filter(Boolean).map(JSON.parse);}
test('actual row response preserves exact original selection, notifications, audit, no-op and recovery behavior',
 {skip:!process.env.BANKING_MODAL_LOCAL_PSQL},async()=>{
 const setup=fs.readFileSync(path.join(__dirname,'fixtures/28082026_1429_banking_pay_selection_setup.sql'),'utf8');
 const sql=fs.readFileSync(path.join(__dirname,'29082026_0017_banking_pay_row_response_runtime.sql'),'utf8').replace('/*FIXTURE*/',()=>setup);
 const values=run(sql);assert.equal(values.length,4);
 const {validateBankingPayModalEnvelope}=await import('../broker/src/banking-pay-modal-v2.js');
 assert.ok(process.env.BANKING_MODAL_FRONTEND_ROOT,'Actual frontend checkout required');
 const {reconcileCandidateSelection}=require(path.join(process.env.BANKING_MODAL_FRONTEND_ROOT,'js/banking-pay-modal-v2-mutation.js'));
 for(const {payload,args,before_summary} of values){
  assert.equal(payload.ready_page.rows.length,100);assert.equal(payload.selection_scope,'EXACT_READY_ROWS');
  validateBankingPayModalEnvelope(payload,'rowSelection',args);let reads=0;
  const staged=await reconcileCandidateSelection({summary:before_summary,ui:{surface:'candidate'},ready:{candidate_id:args.p_candidate_id}},payload,
   {...args.p_options_json,session_id:args.p_session_id,candidate_id:args.p_candidate_id,request_id:args.p_request_id,
    preview_row_ids:args.p_preview_row_ids,selected:args.p_selected,expected_view_digest:args.p_expected_view_digest,open_ready:args.p_open_ready_json},
   async()=>{reads++;throw Error('UNEXPECTED_EXTRA_SUMMARY_READ');});
  assert.equal(reads,0);assert.equal(staged.ready,payload.ready_page);assert.equal(staged.summary.global,payload.global);
 }
});
