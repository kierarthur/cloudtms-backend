// Synthetic disposable PG17 only. No hosted connection or data is accepted.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { spawnSync } from 'node:child_process';
const file = fs.readFileSync('tests/28082026_1254_banking_pay_modal_ready_page_runtime.sql','utf8');
const prefix = file.slice(0,file.indexOf('DO $ready_read_proof$'));
assert.ok(prefix.includes("current_database() <> 'banking_modal_v2_test'"));
assert.ok(process.env.BANKING_MODAL_LOCAL_PSQL);
const sql = `${prefix}
SET LOCAL statement_timeout='45s';
DO $benchmark$
DECLARE v_session public.banking_pay_workbench_sessions%ROWTYPE; v_at timestamptz; v_jit text;
BEGIN
  SELECT * INTO v_session FROM public.banking_pay_workbench_sessions WHERE id='00000000-0000-4000-8000-000000000005';
  FOREACH v_jit IN ARRAY ARRAY['on','off'] LOOP
    PERFORM set_config('jit',v_jit,true);
    v_at:=clock_timestamp();
    PERFORM * FROM private.pay_workbench_modal_eligible_rows_v2(v_session.id,v_session.version,'canonical_preview_lines');
    RAISE NOTICE 'PROFILE jit=% eligible_ms=%',v_jit,round(extract(epoch FROM clock_timestamp()-v_at)*1000,2);
    v_at:=clock_timestamp();
    PERFORM * FROM private.pay_workbench_modal_selection_rows_v2(v_session.id,v_session.version);
    RAISE NOTICE 'PROFILE jit=% selection_ms=%',v_jit,round(extract(epoch FROM clock_timestamp()-v_at)*1000,2);
    v_at:=clock_timestamp();
    PERFORM private.pay_workbench_modal_row_payload_v2(r) FROM public.banking_pay_workbench_preview_rows AS r WHERE session_id=v_session.id;
    RAISE NOTICE 'PROFILE jit=% payload_ms=%',v_jit,round(extract(epoch FROM clock_timestamp()-v_at)*1000,2);
    v_at:=clock_timestamp();
    PERFORM private.pay_workbench_modal_row_matches_scope_v2(r.row_json,'{}'::jsonb,'ALL','canonical_preview_lines') FROM public.banking_pay_workbench_preview_rows AS r WHERE session_id=v_session.id;
    RAISE NOTICE 'PROFILE jit=% filter_ms=%',v_jit,round(extract(epoch FROM clock_timestamp()-v_at)*1000,2);
    v_at:=clock_timestamp();
    PERFORM * FROM private.pay_workbench_modal_ready_members_v2(v_session,'ALL');
    RAISE NOTICE 'PROFILE jit=% members_ms=%',v_jit,round(extract(epoch FROM clock_timestamp()-v_at)*1000,2);
    v_at:=clock_timestamp();
    PERFORM * FROM private.pay_workbench_modal_candidate_facts_v2(v_session,'ALL');
    RAISE NOTICE 'PROFILE jit=% candidates_ms=%',v_jit,round(extract(epoch FROM clock_timestamp()-v_at)*1000,2);
  END LOOP;
END
$benchmark$;
ROLLBACK;`;
const result = spawnSync(process.env.BANKING_MODAL_LOCAL_PSQL, ['-X','-q','-h','127.0.0.1','-p','55441','-U','postgres','-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1'],
  { input:sql,encoding:'utf8',timeout:60000,maxBuffer:1024*1024 });
assert.equal(result.status,0,result.error?.message||result.stderr);
console.log(result.stderr.split(/\r?\n/).filter(line=>line.includes('PROFILE')).join('\n'));
