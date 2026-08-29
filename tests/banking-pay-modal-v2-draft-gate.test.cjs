const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const { spawnSync } = require('node:child_process');
const source = () => fs.readFileSync(path.resolve(__dirname, '../supabase/repeatable/28082026_1451_banking_pay_modal_draft_gate.sql'), 'utf8');

test('Draft display gate calls the existing counter owner explicitly in read-only mode once', () => {
  const sql = source();
  assert.match(sql, /FUNCTION private\.pay_workbench_modal_draft_gate_v2/);
  assert.equal((sql.match(/public\.pay_workbench_session_recompute_progress_counters\(/g) || []).length, 1);
  assert.match(sql, /public\.pay_workbench_session_recompute_progress_counters\(\s*p_session_id,\s*false,\s*'BANKING_PAY_MODAL_STRUCTURE_V2',\s*false\s*\)/);
  assert.doesNotMatch(sql, /\b(?:INSERT INTO|UPDATE public\.|DELETE FROM|pay_workbench_prepare_draft\s*\(|pay_batch_items|pay_advances)\b/i);
  assert.doesNotMatch(sql, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
});

test('Draft helper retains current blockers and cannot enable a zero-selected active filter', () => {
  const sql = source();
  assert.match(sql, /p_selected_ready_count\s*>\s*0/);
  assert.match(sql, /v_progress->'can_create_draft'\s*=\s*'true'::jsonb/);
  assert.match(sql, /v_progress->'draft_blocker_codes'/);
  assert.match(sql, /NO_SELECTED_ROWS/);
  assert.doesNotMatch(sql, /v_ready\s*:=|status_key\s+IN|candidate_ready_amount|source_change_seq/);
  assert.match(sql, /SECURITY INVOKER SET search_path TO ''/);
  assert.match(sql, /REVOKE ALL ON FUNCTION .* FROM PUBLIC, anon, authenticated, service_role/);
});

test('Draft display retains the original continuous-scope owner and original complete-session counts', () => {
  const sql=source();
  assert.equal((sql.match(/public\.pay_workbench_scope_progress_v1\(/g)||[]).length,1);
  assert.match(sql,/v_scope->'draft_safe'\s*=\s*'true'::jsonb/);
  for(const key of ['display_ready','draft_safe','draft_block_reason_code','selected_row_count','selected_eligible_ready_row_count'])
    assert.ok(sql.includes(`->'${key}'`),key);
  assert.match(sql,/octet_length\(v_gate::text\)>2048/);
  assert.doesNotMatch(sql,/\b(?:scope_change_generation_applied|settings_defaults|banking_pay_workbench_jobs)\b/);
});

test('actual pending Draft adapter preserves both unchanged owners across every readiness state',
  {skip:!process.env.BANKING_MODAL_LOCAL_PSQL},()=>{
  const root=path.resolve(__dirname,'..');
  const sql=source();
  const start=sql.indexOf('CREATE OR REPLACE FUNCTION private.pay_workbench_modal_draft_gate_v2(');
  const end=sql.indexOf('$function$;',start);
  assert.ok(start>=0&&end>start);
  const definition=sql.slice(start,end+'$function$;'.length);
  assert.doesNotMatch(definition,/^\s*(?:begin|commit|rollback)\s*;/im);
  const setup=fs.readFileSync(path.join(__dirname,'fixtures/28082026_1429_banking_pay_selection_setup.sql'),'utf8');
  const runtime=fs.readFileSync(path.join(__dirname,'28082026_1453_banking_pay_draft_gate_runtime.sql'),'utf8')
    .replace('BEGIN;',()=>`BEGIN;\nDO $guard$ BEGIN\nIF current_database()<>'banking_modal_v2_test' OR current_setting('server_version_num')::int NOT BETWEEN 170000 AND 179999 THEN RAISE EXCEPTION 'LOCAL_PG17_ONLY'; END IF;\nEND $guard$;\n${definition}`)
    .replace('\\ir fixtures/28082026_1429_banking_pay_selection_setup.sql',()=>setup);
  assert.match(runtime,/ROLLBACK;\s*$/);
  assert.doesNotMatch(runtime,/^\s*COMMIT\s*;/im);
  const result=spawnSync(process.env.BANKING_MODAL_LOCAL_PSQL,['-X','-q','-h','127.0.0.1','-p','55441','-U','postgres',
    '-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1'],{cwd:root,input:runtime,encoding:'utf8',timeout:60000,maxBuffer:2*1024*1024});
  assert.equal(result.status,0,result.error?.message||result.stderr);
  assert.match(result.stderr,/PASS: .*current-owner Draft display states/);
});
