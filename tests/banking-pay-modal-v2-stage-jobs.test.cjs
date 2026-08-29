const assert=require('node:assert/strict');
const test=require('node:test');
const fs=require('node:fs');
const path=require('node:path');
const {spawnSync}=require('node:child_process');
const root=path.resolve(__dirname,'..');
test('stage observations cannot enqueue, retry, execute or change finance authority',()=>{
 const source=fs.readFileSync(path.join(root,'supabase/repeatable/28082026_2008_banking_pay_modal_stage_jobs.sql'),'utf8');
 assert.match(source,/STABLE SECURITY INVOKER SET search_path TO ''/);
 assert.match(source,/FROM PUBLIC, anon, authenticated, service_role/);
 assert.doesNotMatch(source,/\b(?:INSERT INTO|UPDATE public\.|DELETE FROM|SECURITY DEFINER|GRANT EXECUTE|http_post|net\.)\b/i);
 assert.doesNotMatch(source,/pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
 assert.match(source,/j\.payload_json->>'source_change_seq' IS NULL OR j\.source_seq>=COALESCE\(c\.seq,0\)/);
 assert.ok(source.includes('economic_build_id')&&source.includes('completed_at_utc')&&source.includes('session_signature'));
});
test('real rollback-only stage continuation and stale/terminal/clone negative coverage',
 {skip:!process.env.BANKING_MODAL_LOCAL_PSQL},()=>{
 const result=spawnSync(process.env.BANKING_MODAL_LOCAL_PSQL,['-X','-q','-h','127.0.0.1','-p','55441','-U','postgres',
  '-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1','-f','tests/28082026_2014_banking_pay_stage_jobs_runtime.sql'],
  {cwd:root,encoding:'utf8',timeout:60000});
 assert.equal(result.status,0,result.stderr||result.error?.message);
 assert.match(result.stderr,/PASS: seven actual continuation families/);
});
