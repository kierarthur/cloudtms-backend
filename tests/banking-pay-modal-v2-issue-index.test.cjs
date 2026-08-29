const assert=require('node:assert/strict');const fs=require('node:fs');const path=require('node:path');const test=require('node:test');
const root=path.resolve(__dirname,'..');
test('complete issue references use existing scoped authorities without payment arithmetic or write authority',()=>{
 const sql=fs.readFileSync(path.join(root,'supabase/repeatable/28082026_2032_banking_pay_modal_issue_index.sql'),'utf8');
 assert.match(sql,/STABLE SECURITY INVOKER SET search_path TO ''/);
 for(const owner of ['source_issue_members','finance_task_members','bank_issue_members','source_progress_facts','eligible_rows'])
  assert.ok(sql.includes(`private.pay_workbench_modal_${owner}_v2`));
 assert.doesNotMatch(sql,/\b(?:SUM\s*\(|INSERT INTO|UPDATE public\.|DELETE FROM|GRANT EXECUTE|SECURITY DEFINER|LIMIT\s+100)/i);
 assert.doesNotMatch(sql,/pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
 assert.match(sql,/FROM PUBLIC, anon, authenticated, service_role/);
});
