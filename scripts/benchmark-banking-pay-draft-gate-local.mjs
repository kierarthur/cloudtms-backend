// Disposable local component timing only; no hosted connection or financial
// execution. All synthetic records and settings roll back. No timeout increase.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import {spawnSync} from 'node:child_process';
assert.ok(process.env.BANKING_MODAL_LOCAL_PSQL);
const fixture=fs.readFileSync('tests/28082026_2038_banking_pay_summary_runtime.sql','utf8');
const marker='DO $summary$';assert.equal(fixture.split(marker).length,2);
const setup=fs.readFileSync('tests/fixtures/28082026_1429_banking_pay_selection_setup.sql','utf8');
assert.ok(setup.includes("current_database()<>'banking_modal_v2_test'"));
const prefix=fixture.slice(0,fixture.indexOf(marker)).replace('\\ir fixtures/28082026_1429_banking_pay_selection_setup.sql',()=>setup);
assert.doesNotMatch(prefix,/^\s*COMMIT;/im);
const sql=prefix+`
DO $guard$ BEGIN
IF current_setting('server_version_num')::int NOT BETWEEN 170000 AND 179999 THEN RAISE EXCEPTION 'LOCAL_PG17_ONLY'; END IF;
END $guard$;
SET LOCAL statement_timeout='3s';
CREATE FUNCTION pg_temp.bpay_gate_timing(n integer) RETURNS jsonb LANGUAGE plpgsql AS $proof$
DECLARE s uuid:='10000000-0000-4000-8000-000000000005';t timestamptz;p jsonb;g jsonb;c numeric;r numeric;a numeric;
BEGIN
t:=clock_timestamp();p:=public.pay_workbench_session_recompute_progress_counters(s,false,'BANKING_PAY_MODAL_STRUCTURE_V2',false);
c:=extract(epoch FROM clock_timestamp()-t)*1000;
t:=clock_timestamp();p:=public.pay_workbench_scope_progress_v1(s);
r:=extract(epoch FROM clock_timestamp()-t)*1000;
t:=clock_timestamp();g:=private.pay_workbench_modal_draft_gate_v2(s,212);
a:=extract(epoch FROM clock_timestamp()-t)*1000;
IF g->'draft_safe' IS DISTINCT FROM p->'draft_safe' THEN RAISE EXCEPTION 'PROFILE_SCOPE_PARITY'; END IF;
RETURN jsonb_build_object('sample',n,'counter_ms',round(c,3),'scope_ms',round(r,3),'combined_gate_ms',round(a,3),'gate_bytes',octet_length(g::text));
END;
$proof$;
${Array.from({length:10},(_,i)=>`SELECT pg_temp.bpay_gate_timing(${i+1});`).join('\n')}
ROLLBACK;`;
const result=spawnSync(process.env.BANKING_MODAL_LOCAL_PSQL,['-X','-q','-A','-t','-h','127.0.0.1','-p','55441','-U','postgres',
  '-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1'],{input:sql,encoding:'utf8',timeout:45000,maxBuffer:1024*1024});
assert.equal(result.status,0,result.error?.message||result.stderr);
const rows=result.stdout.trim().split(/\r?\n/).filter(Boolean).map(row=>JSON.parse(row));assert.equal(rows.length,10);
console.log(JSON.stringify({target:'disposable_local_pg17',fixture_candidates:105,samples:rows}));
