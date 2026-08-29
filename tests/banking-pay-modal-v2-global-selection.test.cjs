'use strict';
const assert=require('node:assert/strict');
const test=require('node:test');
const fs=require('node:fs');
const path=require('node:path');
const {spawnSync}=require('node:child_process');
const root=path.resolve(__dirname,'..');
const read=file=>fs.readFileSync(path.join(root,file),'utf8').replaceAll('\r\n','\n');

test('filtered header intent enters the same private selection branch, never a candidate-call loop',()=>{
  const bridge=read('supabase/repeatable/28082026_1424_banking_pay_modal_selection_owner_bridge.sql');
  const core=read('supabase/repeatable/28082026_1424_banking_pay_modal_candidate_selection_core.sql');
  assert.ok(/p_selected_preview_row_ids \? 'modal_global_intent_v2'/.test(bridge),'the global v2 dispatch is required');
  assert.match(core,/v_global boolean/);
  assert.equal((core.match(/CREATE OR REPLACE FUNCTION/g)||[]).length,1);
  assert.equal((core.match(/public\._audit_insert\(/g)||[]).length,1);
  assert.equal((core.match(/public\.pay_workbench_revalidate_zero_retained_recovery_headroom_v1\(/g)||[]).length,1);
  assert.doesNotMatch(core,/PERFORM\s+(?:private\.pay_workbench_modal_candidate_selection_apply_v2|public\.pay_workbench_session_set_selected_rows)\(/i);
  assert.doesNotMatch(core,/pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(|\b(?:sum|round|abs)\s*\(/i);
});

for(const [file,label,marker] of [
 ['28082026_1755_banking_pay_global_selection_runtime.sql','covers all pages and is transactionally indivisible','GLOBAL_FILTERED_SELECTION_PASS'],
 ['28082026_1759_banking_pay_global_selection_filters.sql','preserves channel Candidate Client hidden and Updating scopes','GLOBAL_FILTER_MATRIX_PASS']
]) test(`real local PG17 filtered header selection ${label}`,{skip:!process.env.BANKING_MODAL_LOCAL_PSQL},()=>{
  const result=spawnSync(process.env.BANKING_MODAL_LOCAL_PSQL,
    ['-X','-q','-h','127.0.0.1','-p','55441','-U','postgres','-d','banking_modal_v2_test',
      '-v','ON_ERROR_STOP=1','-f',`tests/${file}`],
    {cwd:root,encoding:'utf8',timeout:60000,maxBuffer:2*1024*1024});
  assert.equal(result.status,0,result.error?.message||result.stderr);
  assert.ok(result.stdout.includes(marker),'complete fixture must reach its final proof marker');
});
