// Reproducible component timing only. Synthetic local fixtures always roll back;
// this is not a deployed UI or representative whole-session performance verdict.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {spawnSync} from 'node:child_process';
const fixture=fs.readFileSync('tests/28082026_1709_banking_pay_bank_sources_runtime.sql','utf8');
const prefix=fixture.slice(0,fixture.indexOf('CREATE FUNCTION pg_temp.bank_source_count'));
assert.ok(prefix.includes('\\ir fixtures/28082026_1429_banking_pay_selection_setup.sql'));
assert.ok(fs.readFileSync('tests/fixtures/28082026_1429_banking_pay_selection_setup.sql','utf8').includes("current_database()<>'banking_modal_v2_test'"));
assert.ok(process.env.BANKING_MODAL_LOCAL_PSQL);
const sql=prefix+`SET LOCAL client_min_messages='notice'; DO $profile$
DECLARE s public.banking_pay_workbench_sessions%ROWTYPE; stamp timestamptz; n integer; members integer;
BEGIN SELECT * INTO s FROM public.banking_pay_workbench_sessions WHERE id='10000000-0000-4000-8000-000000000005';
FOR n IN 1..5 LOOP stamp:=clock_timestamp(); SELECT count(*) INTO members FROM private.pay_workbench_modal_bank_sources_v2(s,'ALL');
IF members<>105 THEN RAISE EXCEPTION 'BANK_SOURCE_BENCHMARK_WRONG_SCOPE'; END IF;
RAISE NOTICE 'BANK_SOURCE_PROFILE ms=%',round(extract(epoch FROM clock_timestamp()-stamp)*1000,2); END LOOP;
END $profile$; ROLLBACK;`;
const result=spawnSync(process.env.BANKING_MODAL_LOCAL_PSQL,['-X','-q','-h','127.0.0.1','-p','55441','-U','postgres','-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1'],
  {input:sql,cwd:path.resolve('tests'),encoding:'utf8',timeout:45000});
assert.equal(result.status,0,result.stderr||result.error?.message);
const elapsed=[...result.stderr.matchAll(/BANK_SOURCE_PROFILE ms=([0-9.]+)/g)].map(match=>Number(match[1]));
assert.equal(elapsed.length,5);
console.log(JSON.stringify({scope:'105 synthetic candidates; component only',iterations_ms:elapsed,
  median_ms:[...elapsed].sort((a,b)=>a-b)[2],hosted_mutations:0,whole_ui_acceptance:false}));
