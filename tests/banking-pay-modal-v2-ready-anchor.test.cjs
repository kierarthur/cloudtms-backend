const assert=require('node:assert/strict');
const test=require('node:test');
const fs=require('node:fs');
const path=require('node:path');
const {spawnSync}=require('node:child_process');
const root=path.resolve(__dirname,'..');
const source=fs.readFileSync(path.join(root,'supabase/repeatable/28082026_1159_banking_pay_modal_structure_v2.sql'),'utf8');
const marker='CREATE OR REPLACE FUNCTION public.pay_workbench_session_get_candidate_ready_page_v1(';
const reader=source.slice(source.indexOf(marker));
test('Ready position is separately typed and cannot relax an ordinary financial cursor',()=>{
  assert.match(reader,/READY_PAGE_ANCHOR/);
  assert.match(reader,/page_limit/);
  assert.match(reader,/'previous_cursor'/);
  assert.match(reader,/'page_anchor'/);
  assert.equal((reader.match(/private\.pay_workbench_modal_context_v2\(/g)||[]).length,2);
  assert.doesNotMatch(reader,/pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
  assert.doesNotMatch(reader,/\bOFFSET\b|\b(?:UPDATE|INSERT INTO|DELETE FROM)\s+public\./i);
});
test('Ready anchors preserve current complete rows after real selection and bounded section movement',
  {skip:!process.env.BANKING_MODAL_LOCAL_PSQL},()=>{
  const fixture=fs.readFileSync(path.join(__dirname,'28082026_1254_banking_pay_modal_ready_page_runtime.sql'),'utf8');
  const setup=fixture.slice(0,fixture.indexOf('DO $ready_read_proof$'));
  const sql=setup+fs.readFileSync(path.join(__dirname,'28082026_1856_banking_pay_ready_anchor_runtime.sql'),'utf8');
  const result=spawnSync(process.env.BANKING_MODAL_LOCAL_PSQL,['-X','-q','-h','127.0.0.1','-p','55441','-U','postgres',
    '-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1'],{input:sql,cwd:root,encoding:'utf8',timeout:60000,maxBuffer:2*1024*1024});
  assert.equal(result.status,0,result.error?.message||result.stderr);
  assert.match(result.stderr,/PASS: Ready anchor renewal/);
});
