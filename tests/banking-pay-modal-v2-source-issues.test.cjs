const assert=require('node:assert/strict');const test=require('node:test');
const fs=require('node:fs');const path=require('node:path');const {spawnSync}=require('node:child_process');
const root=path.resolve(__dirname,'..');
test('source problems reuse current progress and only its published refresh action',()=>{
 const sql=fs.readFileSync(path.join(root,'supabase/repeatable/28082026_2019_banking_pay_modal_source_issues.sql'),'utf8');
 assert.match(sql,/STABLE SECURITY INVOKER SET search_path TO ''/);
 assert.match(sql,/FROM PUBLIC, anon, authenticated, service_role/);
 assert.match(sql,/'REFRESH_OR_RETRY','RETRY_OR_REFRESH','OPEN_NEW_SESSION'/);
 assert.match(sql,/'affected_payment_count_complete',false/);
 assert.match(sql,/IF NOT EXISTS\s*\(\s*SELECT 1 FROM private\.pay_workbench_modal_source_progress_facts_v2\(p_session\.id,p_session\.version\) f\s*WHERE f\.source_state<>'CURRENT'\s*\) THEN RETURN; END IF;/);
 assert.doesNotMatch(sql,/\b(?:INSERT INTO|UPDATE public\.|DELETE FROM|GRANT EXECUTE|SECURITY DEFINER)\b/i);
 assert.doesNotMatch(sql,/pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
});
test('source issue membership and Ready exclusion are exercised in a rollback-only real fixture',
 {skip:!process.env.BANKING_MODAL_LOCAL_PSQL},()=>{
 const result=spawnSync(process.env.BANKING_MODAL_LOCAL_PSQL,['-X','-q','-h','127.0.0.1','-p','55441','-U','postgres',
  '-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1','-f','tests/28082026_2024_banking_pay_source_issues_runtime.sql'],
  {cwd:root,encoding:'utf8',timeout:60000});
 assert.equal(result.status,0,result.stderr||result.error?.message);
 assert.match(result.stderr,/PASS: complete source issues and 110-member details/);
});
