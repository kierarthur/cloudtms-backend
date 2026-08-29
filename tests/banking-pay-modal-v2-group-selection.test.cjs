const assert=require('node:assert/strict');const fs=require('node:fs');const path=require('node:path');const test=require('node:test');const {spawnSync}=require('node:child_process');
const root=path.resolve(__dirname,'..');const groupFile='supabase/repeatable/29082026_0108_banking_pay_modal_group_members_v2.sql';
const coreFile='supabase/repeatable/28082026_1424_banking_pay_modal_candidate_selection_core.sql';
const bridgeFile='supabase/repeatable/28082026_1424_banking_pay_modal_selection_owner_bridge.sql';
const responseFile='supabase/repeatable/29082026_0114_banking_pay_modal_group_selection_response.sql';
const read=file=>fs.readFileSync(path.join(root,file),'utf8');const definitions=file=>{const sql=read(file);return sql.slice(sql.indexOf('CREATE OR REPLACE FUNCTION'),sql.lastIndexOf('commit;'));};
test('complete group response is one service route over one set-wise original-owner intent',()=>{const sql=read(responseFile);
 assert.match(sql,/FUNCTION public\.pay_workbench_session_set_ready_group_v1/);assert.match(sql,/private\.pay_workbench_modal_ready_group_members_v2/);
 assert.equal((sql.match(/public\.pay_workbench_session_set_selected_rows\(/g)||[]).length,1);assert.match(sql,/modal_group_intent_v2/);
 assert.doesNotMatch(sql,/WHILE|select_preview_row_ids|deselect_preview_row_ids/);assert.match(sql,/selection_scope','COMPLETE_READY_GROUP/);
 assert.doesNotMatch(sql,/\b(?:pay_workbench_prepare_draft|pay_batch_items|SUM\s*\(|amount_display\s*:=)\b/i);
 assert.match(sql,/FROM PUBLIC, anon, authenticated/);assert.match(sql,/TO service_role/);
});
test('actual107-row complete group selection is atomic and missing group is write-free',{skip:!process.env.BANKING_MODAL_LOCAL_PSQL},()=>{
 const runtime=read('tests/29082026_0121_banking_pay_group_selection_runtime.sql')
  .replace('BEGIN;',()=>`BEGIN;DO $g$ BEGIN IF current_database()<>'banking_modal_v2_test' OR current_setting('server_version_num')::int NOT BETWEEN 170000 AND 179999 THEN RAISE EXCEPTION 'LOCAL_PG17_ONLY';END IF;END $g$;${definitions(groupFile)}${definitions(coreFile)}${definitions(bridgeFile)}${definitions(responseFile)}`)
  .replace('\\ir fixtures/28082026_1429_banking_pay_selection_setup.sql',()=>read('tests/fixtures/28082026_1429_banking_pay_selection_setup.sql'));
 assert.match(runtime,/ROLLBACK;\s*$/);assert.doesNotMatch(runtime,/^\s*COMMIT\s*;/im);
 const result=spawnSync(process.env.BANKING_MODAL_LOCAL_PSQL,['-X','-q','-h','127.0.0.1','-p','55441','-U','postgres','-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1'],{cwd:root,input:runtime,encoding:'utf8',timeout:60000,maxBuffer:2*1024*1024});
 assert.equal(result.status,0,result.error?.message||result.stderr);assert.match(result.stderr,/PASS: complete107-row group/);
});
test('actual overpayment group settles in one atomic original-owner call',{skip:!process.env.BANKING_MODAL_LOCAL_PSQL},()=>{
 const runtime=read('tests/29082026_0136_banking_pay_overpayment_group_runtime.sql')
  .replace('BEGIN;',()=>`BEGIN;DO $g$ BEGIN IF current_database()<>'banking_modal_v2_test' OR current_setting('server_version_num')::int NOT BETWEEN 170000 AND 179999 THEN RAISE EXCEPTION 'LOCAL_PG17_ONLY';END IF;END $g$;${definitions(groupFile)}${definitions(coreFile)}${definitions(bridgeFile)}${definitions(responseFile)}`)
  .replace('\\ir fixtures/28082026_1429_banking_pay_selection_setup.sql',()=>read('tests/fixtures/28082026_1429_banking_pay_selection_setup.sql'));
 assert.match(runtime,/ROLLBACK;\s*$/);assert.doesNotMatch(runtime,/^\s*COMMIT\s*;/im);
 const result=spawnSync(process.env.BANKING_MODAL_LOCAL_PSQL,['-X','-q','-h','127.0.0.1','-p','55441','-U','postgres','-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1'],{cwd:root,input:runtime,encoding:'utf8',timeout:60000,maxBuffer:2*1024*1024});
 assert.equal(result.status,0,result.error?.message||result.stderr);assert.match(result.stderr,/PASS: real overpayment group/);
});
