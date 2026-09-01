const assert=require('node:assert/strict');const fs=require('node:fs');const path=require('node:path');const test=require('node:test');const {spawnSync}=require('node:child_process');
const root=path.resolve(__dirname,'..');const files=['supabase/repeatable/29082026_0108_banking_pay_modal_group_members_v2.sql','supabase/repeatable/01092026_1058_banking_pay_candidate_current_payable_groups_v2.sql'];
const read=file=>fs.readFileSync(path.join(root,file),'utf8');const definitions=file=>{const sql=read(file);return sql.slice(sql.indexOf('CREATE OR REPLACE FUNCTION'),sql.lastIndexOf('commit;'));};
test('Ready reader joins one set-wise current-payable group projection before physical paging',()=>{const sql=read(files[1]);
 const outer=sql.slice(sql.indexOf('CREATE OR REPLACE FUNCTION public.pay_workbench_session_get_candidate_ready_page_v1('),sql.indexOf('CREATE OR REPLACE FUNCTION public.pay_workbench_session_get_candidate_ready_group_page_v1('));
 assert.equal((outer.match(/private\.pay_workbench_modal_ready_group_members_v2\(/g)||[]).length,1);
 assert.match(outer,/group_members AS MATERIALIZED/);assert.match(outer,/group_facts AS MATERIALIZED/);
 assert.match(outer,/WHERE r\.presentation_group_kind='ROW' OR f\.group_kind IS NOT NULL/);
 assert.equal((outer.match(/sum\(private\.pay_workbench_modal_line_display_amount_v2\(r\.payload\)/g)||[]).length,2);
 for(const key of ['selection_group_kind','selection_group_key','selection_group_member_count','selection_group_selected_count','selection_group_state','selection_group_display_amount','selection_group_selected_display_amount'])assert.match(sql,new RegExp("'"+key+"'"));
 assert.doesNotMatch(sql,/\b(?:pay_workbench_prepare_draft|pay_workbench_session_set_selected_rows|UPDATE|INSERT|DELETE)\b/i);
 assert.doesNotMatch(sql,/->>\s*'(?:amount|section_amount|amount_ex_vat|section_amount_ex_vat)'/i);
});
test('actual two-page 107-line Ready group has identical complete facts and no writes',{skip:!process.env.BANKING_MODAL_LOCAL_PSQL},()=>{
 const runtime=read('tests/29082026_0128_banking_pay_ready_group_facts_runtime.sql')
  .replace('BEGIN;',()=>`BEGIN;DO $g$ BEGIN IF current_database()<>'banking_modal_v2_test' OR current_setting('server_version_num')::int NOT BETWEEN 170000 AND 179999 THEN RAISE EXCEPTION 'LOCAL_PG17_ONLY';END IF;END $g$;${files.map(definitions).join('')}`)
  .replace('\\ir fixtures/28082026_1429_banking_pay_selection_setup.sql',()=>read('tests/fixtures/28082026_1429_banking_pay_selection_setup.sql'));
 assert.match(runtime,/ROLLBACK;\s*$/);assert.doesNotMatch(runtime,/^\s*COMMIT\s*;/im);
 const result=spawnSync(process.env.BANKING_MODAL_LOCAL_PSQL,['-X','-q','-h','127.0.0.1','-p','55441','-U','postgres','-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1'],{cwd:root,input:runtime,encoding:'utf8',timeout:60000,maxBuffer:2*1024*1024});
 assert.equal(result.status,0,result.error?.message||result.stderr);assert.match(result.stderr,/PASS: one outer payment group carries complete 107-member authority/);
});
