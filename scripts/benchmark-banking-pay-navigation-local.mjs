// Rollback-only diagnosis of the exact full summary/sort/navigation fixture.
// No hosted target, configuration change, financial execution or timeout waiver.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import {spawnSync} from 'node:child_process';
const mode=process.argv[2]||'unset';
const profile=process.argv[3]==='profile';
const mechanism=process.argv[4]||'unchanged';
assert.ok(['unchanged','eligible_materialized'].includes(mechanism));
assert.ok(['unset','auto','force_custom_plan'].includes(mode));
assert.ok(process.env.BANKING_MODAL_LOCAL_PSQL);
const setup=fs.readFileSync('tests/fixtures/28082026_1429_banking_pay_selection_setup.sql','utf8');
assert.ok(setup.includes("current_database()<>'banking_modal_v2_test'"));
let sql=fs.readFileSync('tests/28082026_2038_banking_pay_summary_runtime.sql','utf8')
 .replace('\\ir fixtures/28082026_1429_banking_pay_selection_setup.sql',setup);
assert.ok(!sql.includes('\\ir'));assert.match(sql,/ROLLBACK;\s*$/);assert.doesNotMatch(sql,/\bCOMMIT;/);
const marker='CREATE TEMP TABLE modal_summary_results';assert.equal(sql.split(marker).length,2);
sql=sql.replace(marker,(mode==='unset'?'':`ALTER FUNCTION private.pay_workbench_modal_candidate_page_v2(public.banking_pay_workbench_sessions,jsonb,text,text,text,integer) SET plan_cache_mode='${mode}';\n`)+`\\timing on\n${marker}`);
if(profile){
 sql=sql.replace("SET LOCAL client_min_messages='warning';","SET LOCAL client_min_messages='notice';");
 const metrics=`SELECT jsonb_agg(to_jsonb(metric)) FROM (
  SELECT schemaname||'.'||funcname AS function,calls,round(total_time::numeric,2) AS total_ms,
   round(self_time::numeric,2) AS self_ms FROM pg_stat_xact_user_functions WHERE calls>0 ORDER BY total_time DESC LIMIT 12
 ) metric`;
 sql=sql.replace(marker,"SET LOCAL track_functions='all';\n"+marker);
 assert.equal(sql.split('END;\n$proof$;').length,2);
 sql=sql.replace('END;\n$proof$;',`EXCEPTION WHEN query_canceled THEN
  RAISE NOTICE 'PROFILE_FAILED %',(${metrics});RAISE;
 END;\n$proof$;`);
 sql=sql.replace(/(SELECT pg_temp\.verify_summary_sort\('[A-Z_]+','[A-Z]+'\);)/g,
  `$1\nSELECT jsonb_build_object('diagnostic_metrics',(${metrics}));`);
}
if(mechanism==='eligible_materialized'){
 const source=fs.readFileSync('supabase/repeatable/28082026_1232_banking_pay_modal_certified_projection.sql','utf8');
 const definition=source.slice(source.indexOf('CREATE OR REPLACE FUNCTION private.pay_workbench_modal_eligible_rows_v2('),
  source.indexOf('ALTER FUNCTION private.pay_workbench_modal_eligible_rows_v2('))
  .replace('), eligible_rows AS MATERIALIZED (','), eligible_rows AS (');
 assert.equal(definition.split('), eligible_rows AS (').length,2);
 sql=sql.replace(marker,()=>definition.replace('), eligible_rows AS (','), eligible_rows AS MATERIALIZED (')+marker);
}
const start=performance.now();
const result=spawnSync(process.env.BANKING_MODAL_LOCAL_PSQL,['-X','-q','-A','-t','-h','127.0.0.1','-p','55441','-U','postgres',
 '-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1'],{input:sql,encoding:'utf8',timeout:60000,maxBuffer:4*1024*1024});
const timing=result.stdout.split(/\r?\n/).filter(line=>line.startsWith('Time:'));
console.log(JSON.stringify({mode,mechanism,elapsed_ms:Math.round(performance.now()-start),status:result.status,timing,
 metrics:profile?result.stdout.split(/\r?\n/).filter(line=>line.includes('"diagnostic_metrics"')).map(line=>JSON.parse(line)):undefined,
 errors:result.stderr.split(/\r?\n/).filter(line=>/ERROR:|NOTICE:/.test(line)),
 error_context:result.status===0?undefined:result.stderr.split(/\r?\n/).slice(0,45)}));
process.exitCode=result.status===0?0:1;
