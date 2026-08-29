const assert=require('node:assert/strict');
const test=require('node:test');
const fs=require('node:fs');
const path=require('node:path');
const {spawnSync}=require('node:child_process');
const root=path.resolve(__dirname,'..');
test('bank job evidence cannot enqueue work or accept a stale word from a preview row',()=>{
 const source=fs.readFileSync(path.join(root,'supabase/repeatable/28082026_1935_banking_pay_modal_bank_jobs.sql'),'utf8');
 assert.match(source,/STABLE SECURITY INVOKER SET search_path TO ''/);
 for(const name of ['banking_pay_workbench_jobs','app_change_counters','readiness_fingerprint','completed_at_utc','source_change_seq'])assert.ok(source.includes(name));
 assert.match(source,/payee_count BETWEEN 1 AND 25/);
 assert.doesNotMatch(source,/\b(?:INSERT INTO|UPDATE public\.|DELETE FROM|SECURITY DEFINER|GRANT EXECUTE|http_post|net\.)\b/i);
 assert.doesNotMatch(source,/pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
});
test('current bank job, failure, completed successor, exact target and every stale binding are proved with real SQL',
 {skip:!process.env.BANKING_MODAL_LOCAL_PSQL},()=>{
 const result=spawnSync(process.env.BANKING_MODAL_LOCAL_PSQL,['-X','-q','-h','127.0.0.1','-p','55441','-U','postgres',
  '-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1','-f','tests/28082026_1935_banking_pay_bank_jobs_runtime.sql'],
  {cwd:root,encoding:'utf8',timeout:60000});
 assert.equal(result.status,0,result.stderr||result.error?.message);
 assert.match(result.stderr,/PASS: bank jobs require exact current ownership/);
});
