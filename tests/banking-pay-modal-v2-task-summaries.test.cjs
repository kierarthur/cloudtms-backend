const assert=require('node:assert/strict');const fs=require('node:fs');const path=require('node:path');const test=require('node:test');
const {spawnSync}=require('node:child_process');const root=path.resolve(__dirname,'..');
test('compact task summaries reuse complete owners without financial calculations or per-task reads',()=>{
 const sql=fs.readFileSync(path.join(root,'supabase/repeatable/28082026_2122_banking_pay_modal_task_summaries.sql'),'utf8');
 for(const owner of ['issue_index','finance_task_members','bank_task_members','source_issue_members'])
  assert.ok(sql.includes(`private.pay_workbench_modal_${owner}_v2`),owner);
 assert.match(sql,/STABLE SECURITY INVOKER SET search_path TO ''/);
 assert.match(sql,/count\(DISTINCT m\.preview_row_id\) FILTER\(WHERE m\.affected\)/);
 assert.doesNotMatch(sql,/\b(?:SUM\s*\(|LOOP|INSERT INTO|UPDATE public\.|DELETE FROM|GRANT EXECUTE|SECURITY DEFINER|LIMIT\s+100)/i);
 assert.doesNotMatch(sql,/issue_detail_members_v2|pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
});
test('server task titles are exactly the approved frontend wording',
 {skip:!process.env.BANKING_MODAL_LOCAL_PSQL||!process.env.BANKING_MODAL_FRONTEND_ROOT},()=>{
 const query=require('./fixtures/banking-pay-local-query.cjs');
 const copy=require(path.join(process.env.BANKING_MODAL_FRONTEND_ROOT,'js/banking-pay-modal-v2-copy.js'));
 const ids=['MSG-044','MSG-045','MSG-046','MSG-060','MSG-062','MSG-064','MSG-066','MSG-067','MSG-068','MSG-071','MSG-072','MSG-073','MSG-077'];
 const rows=query(`SELECT jsonb_build_object('id',id,'title',private.pay_workbench_modal_task_title_v2(id))
  FROM unnest(ARRAY[${ids.map(id=>"'"+id+"'").join(',')}]) id;`);
 assert.equal(rows.length,ids.length);for(const row of rows)assert.equal(row.title,copy.message(row.id));
 assert.deepEqual(query("SELECT jsonb_build_object('title',private.pay_workbench_modal_task_title_v2('not_approved'));"),[{title:null}]);
});
test('real complete task counts and current source ownership are retained',
 {skip:!process.env.BANKING_MODAL_LOCAL_PSQL},()=>{
 const r=spawnSync(process.env.BANKING_MODAL_LOCAL_PSQL,['-X','-q','-h','127.0.0.1','-p','55441','-U','postgres',
  '-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1','-f','tests/28082026_2122_banking_pay_task_summaries_runtime.sql'],
  {cwd:root,encoding:'utf8',timeout:60000,maxBuffer:2*1024*1024});
 assert.equal(r.status,0,r.stderr||r.error?.message);assert.match(r.stderr,/PASS: complete task summaries/);
});
