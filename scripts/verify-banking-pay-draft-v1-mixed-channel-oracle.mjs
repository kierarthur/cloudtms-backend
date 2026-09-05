import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import { fileURLToPath, pathToFileURL } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const args = new Map();
for (let index = 2; index < process.argv.length; index += 2) {
  const key = process.argv[index];
  const value = process.argv[index + 1];
  if (!key?.startsWith('--') || !value) throw new Error(`Invalid argument at position ${index}`);
  args.set(key.slice(2), value);
}

const container = args.get('container');
const database = args.get('database');
const postgrestOrigin = String(args.get('postgrest') || '').replace(/\/$/, '');
const oracleRoot = path.resolve(args.get('oracle-root') || '');
const targetCount = Number(args.get('target') || 8);
const runId = String(args.get('run-id') || 'baseline-1');
if (!container || !/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$/.test(container)) throw new Error('Safe --container is required');
if (!database || !/^[a-zA-Z_][a-zA-Z0-9_]{0,62}$/.test(database)) throw new Error('Safe --database is required');
if (!/^http:\/\/127\.0\.0\.1:\d+$/.test(postgrestOrigin)) throw new Error('Local --postgrest origin is required');
if (!fs.existsSync(path.join(oracleRoot, 'broker', 'src', 'index.js'))) throw new Error('Exact --oracle-root is required');
if (!Number.isInteger(targetCount) || targetCount < 2 || targetCount > 100) throw new Error('--target must be between 2 and 100');
if (!/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}$/.test(runId)) throw new Error('Safe --run-id is required');

const SESSION_ID = '20000000-0000-4000-8000-000000000001';
const ACTOR_ID = '10000000-0000-4000-8000-000000000001';
const basePath = path.join(root, 'tests', '03092026_1010_banking_pay_workbench_settled_certificate_v8_scale_verification.sql');
const brokerPath = path.join(oracleRoot, 'broker', 'src', 'index.js');
const draftInsertItemsHelperPath = path.join(oracleRoot, 'broker', 'src', 'banking-pay-draft-insert-items.js');

function replaceOnce(text, needle, replacement, label) {
  const pieces = text.split(needle);
  if (pieces.length !== 2) throw new Error(`${label} expected once; found ${pieces.length - 1}`);
  return `${pieces[0]}${replacement}${pieces[1]}`;
}

function run(command, commandArgs, options = {}) {
  const result = spawnSync(command, commandArgs, {
    encoding: 'utf8',
    maxBuffer: 64 * 1024 * 1024,
    ...options
  });
  if (result.status !== 0) {
    throw new Error(`${command} failed (${result.status}):\n${result.stderr || result.stdout}`);
  }
  return options.includeStderr ? `${result.stdout || ''}\n${result.stderr || ''}` : (result.stdout || '');
}

function buildSeedSql() {
  let sql = fs.readFileSync(basePath, 'utf8').replaceAll('\r\n', '\n');
  sql = replaceOnce(
    sql,
    '  IF v_target NOT IN (101,1001,2000,5000) THEN',
    '  IF v_target NOT IN (8,101,1001,2000,5000) THEN',
    'bounded oracle target allowlist'
  );
  sql = replaceOnce(
    sql,
    `  CREATE TEMP TABLE h1_v8_scale_rows AS
  SELECT row_ordinal,
         ((row_ordinal-1) % v_candidate_count)+1 AS candidate_seq,
         format('70000000-0000-0000-0000-%s',lpad(row_ordinal::text,12,'0'))::uuid AS source_line_id,
         format('80000000-0000-0000-0000-%s',lpad(row_ordinal::text,12,'0'))::uuid AS preview_row_id
  FROM pg_catalog.generate_series(1,v_target) row_ordinal;`,
    `  CREATE TEMP TABLE h1_v8_scale_rows AS
  WITH row_seed AS (
    SELECT row_ordinal,
           ((row_ordinal-1) % v_candidate_count)+1 AS candidate_seq,
           pg_catalog.floor((row_ordinal-1)/v_candidate_count)::integer+1 AS candidate_local_ordinal
    FROM pg_catalog.generate_series(1,v_target) row_ordinal
  )
  SELECT row_ordinal,candidate_seq,candidate_local_ordinal,candidate_local_ordinal AS candidate_timesheet_ordinal,
         format('70000000-0000-0000-0000-%s',lpad(row_ordinal::text,12,'0'))::uuid AS source_line_id,
         format('80000000-0000-0000-0000-%s',lpad(row_ordinal::text,12,'0'))::uuid AS preview_row_id,
         format('72000000-0000-0000-%s-%s',lpad(candidate_seq::text,4,'0'),lpad(candidate_local_ordinal::text,12,'0'))::uuid AS timesheet_id,
         format('73000000-0000-0000-0000-%s',lpad(row_ordinal::text,12,'0'))::uuid AS adjustment_id
  FROM row_seed;`,
    'row-backed source identities'
  );
  sql = replaceOnce(
    sql,
    `  INSERT INTO public.banking_pay_snapshot_runs(`,
    `  INSERT INTO public.app_change_counters(entity_key,seq)
  VALUES ('pay_candidate_scope_generation',1)
  ON CONFLICT (entity_key) DO UPDATE SET seq=GREATEST(public.app_change_counters.seq,EXCLUDED.seq);

  INSERT INTO public.umbrellas(
    id,name,remittance_email,bank_name,sort_code,account_number,
    vat_chargeable,enabled,bank_details_hash
  ) VALUES (
    '65000000-0000-0000-0000-000000000001',v_prefix||':UMBRELLA',
    'h2-v1-oracle@example.invalid','H2 V1 Oracle Bank','112233','87654321',
    true,true,v_prefix||':UMBRELLA:BANK'
  );

  INSERT INTO public.settings_finance_windows(
    id,date_from,date_to,vat_rate_pct,erni_pct,holiday_pay_pct,
    apply_holiday_to,apply_erni_to,margin_includes
  ) VALUES (
    '65000000-0000-0000-0000-000000000002',DATE '2099-01-01',DATE '2099-12-31',
    20,0,0,'NONE','NONE','[]'::jsonb
  );

  INSERT INTO public.clients(id,cli_ref,name,vat_chargeable,payment_terms_days)
  VALUES (v_client_id,v_prefix||':CLIENT',v_prefix||':CLIENT',true,30);

  INSERT INTO public.banking_pay_snapshot_runs(`,
    'provider prerequisites'
  );
  sql = replaceOnce(
    sql,
    `  INSERT INTO public.candidates(id,display_name,tms_ref,pay_method)
  SELECT candidate_id,v_prefix||':CANDIDATE:'||candidate_seq::text,
         'CCR-'||(99000000+candidate_seq)::text,pay_channel
  FROM h1_v8_scale_candidates ORDER BY candidate_seq;`,
    `  INSERT INTO public.candidates(
    id,display_name,tms_ref,pay_method,umbrella_id,active,
    account_holder,sort_code,account_number,bank_details_hash
  )
  SELECT candidate_id,v_prefix||':CANDIDATE:'||candidate_seq::text,
         'CCR-'||(99000000+candidate_seq)::text,pay_channel,
         CASE WHEN pay_channel='UMBRELLA' THEN '65000000-0000-0000-0000-000000000001'::uuid END,
         true,v_prefix||':CANDIDATE:'||candidate_seq::text,'010203',
         pg_catalog.lpad((12000000+candidate_seq)::text,8,'0'),v_prefix||':BANK:'||candidate_seq::text
  FROM h1_v8_scale_candidates ORDER BY candidate_seq;`,
    'candidate payee setup'
  );
  sql = replaceOnce(
    sql,
    `  INSERT INTO public.timesheets(
    timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,week_ending_date
  ) VALUES (v_timesheet_id,v_prefix,v_prefix,'H1','H1','H1',DATE '2099-03-29');`,
    `  PERFORM pg_catalog.set_config('cloudtms.lifecycle_mutation_context','manual_timesheet_save',true);
  INSERT INTO public.timesheets(
    timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
    week_ending_date,sheet_scope,authorised_at_server,is_current,version,is_adjustment
  )
  SELECT row.timesheet_id,v_prefix||':TS:'||row.candidate_seq::text||':'||row.candidate_timesheet_ordinal::text,
         v_prefix||':OCC:'||row.candidate_seq::text||':'||row.candidate_timesheet_ordinal::text,
         'H2 V1 Oracle Hospital','H2 V1 Oracle Ward','H2 V1 Oracle Role',DATE '2099-03-29',
         'DAILY',pg_catalog.clock_timestamp(),true,1,false
  FROM h1_v8_scale_rows row;
  PERFORM pg_catalog.set_config('cloudtms.lifecycle_mutation_context','',true);

  INSERT INTO public.timesheets_financials(
    id,timesheet_id,timesheet_version,basis,is_current,is_stale,
    worked_start_iso,worked_end_iso,actual_schedule_json,candidate_id,client_id,role,
    pay_method,occupant_key_norm,candidate_assignment,processing_status,
    hours_day,pay_day,charge_day,total_hours,total_pay_ex_vat,total_charge_ex_vat,
    margin_ex_vat,pay_on_hold,has_rate_issue,has_pay_channel_issue
  )
  SELECT format('74000000-0000-0000-%s-%s',pg_catalog.lpad(row.candidate_seq::text,4,'0'),
                pg_catalog.lpad(row.candidate_timesheet_ordinal::text,12,'0'))::uuid,
         row.timesheet_id,1,'SELF_REPORTED',true,false,
         '2099-03-23 08:00:00+00','2099-03-23 16:00:00+00',
         '{"date":"2099-03-23","start":"08:00","end":"16:00","break_minutes":0}'::jsonb,
         candidate.candidate_id,v_client_id,'H2 V1 Oracle Role',candidate.pay_channel,
         v_prefix||':OCC:'||row.candidate_seq::text||':'||row.candidate_timesheet_ordinal::text,
         'ASSIGNED','READY_FOR_INVOICE',0,0,0,0,1,1,0,false,false,false
  FROM h1_v8_scale_rows row JOIN h1_v8_scale_candidates candidate USING(candidate_seq);

  INSERT INTO public.ts_pay_adjustments(
    id,timesheet_id,candidate_id,client_id,week_ending_date,delta_pay_ex_vat,
    reason,as_advance,meta_json
  )
  SELECT row.adjustment_id,row.timesheet_id,candidate.candidate_id,v_client_id,DATE '2099-03-29',
         1.00,v_prefix||':ADJUSTMENT:'||row.row_ordinal::text,false,
         pg_catalog.jsonb_build_object('h2_fixture',true,'row_ordinal',row.row_ordinal)
  FROM h1_v8_scale_rows row JOIN h1_v8_scale_candidates candidate USING(candidate_seq);`,
    'row-backed Timesheet setup'
  );
  sql = sql.replaceAll('v_timesheet_id,ARRAY[v_timesheet_id]', 'row.timesheet_id,ARRAY[row.timesheet_id]');
  sql = sql.replaceAll("v_prefix||':LINE:'||row.row_ordinal::text,v_timesheet_id,'canonical_preview_lines'", "v_prefix||':LINE:'||row.row_ordinal::text,row.timesheet_id,'canonical_preview_lines'");
  sql = sql.replaceAll("'H1SCALE:'||row.row_ordinal::text", 'row.adjustment_id::text');
  sql = replaceOnce(
    sql,
    `  SELECT row.preview_row_id,v_session_id,candidate.candidate_id,v_timesheet_id,`,
    `  SELECT row.preview_row_id,v_session_id,candidate.candidate_id,row.timesheet_id,`,
    'preview Timesheet identity'
  );
  sql = replaceOnce(
    sql,
    `'client_id',v_client_id::text,'timesheet_id',v_timesheet_id::text,`,
    `'client_id',v_client_id::text,'timesheet_id',row.timesheet_id::text,
           'presentation_section','READY_TO_PAY','readiness_state','READY_TO_PAY',
           'source_function','pay_workbench_candidate_source_build_chunk','source_kind','VALID_PREVIEW_LINE',
           'session_id',v_session_id::text,'session_version',1,'candidate_id',candidate.candidate_id::text,
           'source_ordinal',row.row_ordinal,'line_key',v_prefix||':LINE:'||row.row_ordinal::text,
           'line_ordinal',row.row_ordinal,'target_section','canonical_preview_lines','section','canonical_preview_lines',
           'economic_key',pg_catalog.jsonb_build_object('timesheet_id',row.timesheet_id::text,
             'key_type','ADJUSTMENT_CODE','key_value',row.adjustment_id::text),
           'dependency_family_kind','TIMESHEET','dependency_family_key','timesheet:'||row.timesheet_id::text,
           'refresh_scope_kind','CANDIDATE_FULL_LIVE','requested_refresh_scope_kind','CANDIDATE_FULL_LIVE',
           'actual_refresh_scope_kind','CANDIDATE_FULL_LIVE','pay_channel_scope','ALL',
           'policy_x_authority_scope','PRE_DRAFT_LIVE_TRUTH',`,
    'certified preview identity'
  );
  sql = replaceOnce(
    sql,
    `'preview_contract',pg_catalog.jsonb_build_object('ok',true,'selection_allowed',true),`,
    `'preview_contract',pg_catalog.jsonb_build_object(
             'ok',true,'materialisable',true,'target_section','canonical_preview_lines',
             'presentation_section','READY_TO_PAY','selection_allowed',true),`,
    'preview materialisation contract'
  );
  sql = replaceOnce(
    sql,
    `  PERFORM pg_catalog.set_config('client_min_messages','notice',true);`,
    `  DELETE FROM public.banking_pay_workbench_jobs
  WHERE dedupe_key='DIRTY_TRIGGER:CONTRACT_CLIENT_DIRTY_FANOUT:UMBRELLA:65000000-0000-0000-0000-000000000001';

  UPDATE public.banking_pay_workbench_sessions
  SET scope_change_generation_target=public.pay_workbench_scope_current_generation_v1(),
      scope_change_generation_applied=public.pay_workbench_scope_current_generation_v1(),
      scope_change_generation_shadow_checked=public.pay_workbench_scope_current_generation_v1()
  WHERE id=v_session_id;

  PERFORM pg_catalog.set_config('client_min_messages','notice',true);`,
    'settled generation sync'
  );
  return sql;
}

async function requestJson(url, init = {}) {
  const response = await fetch(url, init);
  const text = await response.text();
  if (!response.ok) throw Object.assign(new Error(`POSTGREST_${response.status}:${text.slice(0, 2000)}`), { status: response.status });
  return text ? JSON.parse(text) : null;
}

function unwrap(value) {
  return Array.isArray(value) && value.length === 1 ? value[0] : value;
}

const version = run('docker', ['exec', container, 'psql', '-X', '-U', 'postgres', '-d', database, '-Atc', 'select version()']).trim();
if (!/^PostgreSQL (17\.11|18\.6)\b/.test(version)) throw new Error(`Unsupported engine: ${version}`);
const existingSeedCount = Number(run(
  'docker',
  ['exec', container, 'psql', '-X', '-U', 'postgres', '-d', database, '-Atc', `select count(*) from public.banking_pay_workbench_sessions where id='${SESSION_ID}'::uuid`]
).trim());
if (existingSeedCount === 0) {
  const seedOutput = run(
    'docker',
    ['exec', '-i', '-e', 'PGOPTIONS=-c jit=off', container, 'psql', '-X', '-v', 'ON_ERROR_STOP=1', '-v', `target_count=${targetCount}`, '-v', 'worker_seed_only=true', '-v', 'h2_transport=false', '-U', 'postgres', '-d', database],
    { input: buildSeedSql(), includeStderr: true }
  );
  if (!seedOutput.includes(`H1_V8_WORKER_POSTGREST_SEED_READY target=${targetCount}`)) throw new Error(`Seed marker missing:\n${seedOutput.slice(-4000)}`);
} else if (existingSeedCount !== 1) {
  throw new Error(`Unexpected existing seed cardinality: ${existingSeedCount}`);
}

run('docker', ['exec', container, 'psql', '-X', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', database, '-c', `
  DELETE FROM public.banking_pay_workbench_jobs
  WHERE dedupe_key='DIRTY_TRIGGER:CONTRACT_CLIENT_DIRTY_FANOUT:UMBRELLA:65000000-0000-0000-0000-000000000001'
     OR (
       job_type='WORKBENCH_CANDIDATE_DIRTY_APPLY'
       AND candidate_id IN (
         '60000000-0000-0000-0000-000000000001'::uuid,
         '60000000-0000-0000-0000-000000000002'::uuid,
         '60000000-0000-0000-0000-000000000003'::uuid,
         '60000000-0000-0000-0000-000000000004'::uuid
       )
       AND dedupe_key LIKE 'DIRTY_TRIGGER:WORKBENCH_CANDIDATE_DIRTY_APPLY:CANDIDATE:60000000-0000-0000-0000-%'
     );
  UPDATE public.banking_pay_workbench_session_candidate_state AS candidate_state
  SET source_change_seq=COALESCE(change_counter.seq,0),
      updated_at_utc=pg_catalog.clock_timestamp(),
      last_recomputed_at_utc=pg_catalog.clock_timestamp()
  FROM public.app_change_counters AS change_counter
  WHERE candidate_state.session_id='${SESSION_ID}'::uuid
    AND change_counter.entity_key='pay_candidate:'||candidate_state.candidate_id::text;
  UPDATE public.banking_pay_workbench_sessions
  SET scope_change_generation_target=public.pay_workbench_scope_current_generation_v1(),
      scope_change_generation_applied=public.pay_workbench_scope_current_generation_v1(),
      scope_change_generation_shadow_checked=public.pay_workbench_scope_current_generation_v1()
  WHERE id='${SESSION_ID}'::uuid;
`]);

const previewRows = await requestJson(`${postgrestOrigin}/banking_pay_workbench_preview_rows?session_id=eq.${SESSION_ID}&selected=is.true&select=id,candidate_id,timesheet_id,key_type,key_value,row_ordinal&order=row_ordinal.asc`);
if (!Array.isArray(previewRows) || previewRows.length !== targetCount) throw new Error(`Selected-row count mismatch: ${previewRows?.length}`);
const selectedIds = previewRows.map(row => row.id);
const selectedTimesheetIds = [...new Set(previewRows.map(row => row.timesheet_id))];
const inputJson = {
  source: 'H2_V1_MIXED_CHANNEL_ORACLE',
  row_backed_preview_required: true,
  frozen_economic_keys_required: true,
  backend_runner_owned: true,
  frontend_completion_required: false,
  workbench_session_id: SESSION_ID,
  session_id: SESSION_ID,
  pay_date: '2099-04-03',
  week_ending_cutoff_date: '2099-03-29',
  week_ending_cutoff: '2099-03-29',
  pay_channel_scope: 'ALL',
  draft_scope: 'ALL',
  same_week_paye_override: { used: false },
  same_week_paye_override_json: { used: false },
  selected_rows_total: targetCount,
  selected_preview_row_count: targetCount,
  selected_preview_row_ids: selectedIds,
  draft_selected_preview_row_ids: selectedIds,
  selected_timesheet_ids: selectedTimesheetIds,
  expected_workbench_selected_preview_row_ids: selectedIds,
  expected_workbench_progress_counter_version: 1,
  selection_review_contract_version: 1,
  selection_reviewed_by_user_id: ACTOR_ID,
  workbench_readiness_snapshot: {
    session_id: SESSION_ID,
    session_version: 1,
    progress_counter_version: 1,
    expected_workbench_selected_preview_row_ids: selectedIds,
    expected_workbench_progress_counter_version: 1,
    selection_review_contract_version: 1,
    selection_reviewed_by_user_id: ACTOR_ID,
    session_ready: true,
    ready_for_draft: true,
    selected_eligible_ready_row_count: targetCount,
    blocker_codes: []
  },
  selected_preview_row_mode: 'SERVER_SESSION_SELECTION',
  selection_session_version: 1,
  source_session_version: 1,
  source_snapshot_run_id: '30000000-0000-0000-0000-000000000001',
  rail_env_snapshot: 'TEST',
  policy_x_authority: 'PRE_DRAFT_ROW_BACKED_PREVIEW_TO_POST_DRAFT_FROZEN_ARTEFACTS',
  economic_keyspace: 'timesheet_id,key_type,key_value',
  draft_source: 'banking_pay_workbench_preview_rows',
  final_result_shape: 'handleBankingPayCreateDraft'
};
const start = unwrap(await requestJson(`${postgrestOrigin}/rpc/banking_pay_operation_start`, {
  method: 'POST',
  headers: { 'content-type': 'application/json', accept: 'application/json' },
  body: JSON.stringify({
    p_operation_type: 'DRAFT_CREATE',
    p_actor_user_id: ACTOR_ID,
    p_idempotency_key: `h2-v1-mixed-${targetCount}-${runId}`,
    p_workbench_session_id: SESSION_ID,
    p_pay_batch_id: null,
    p_root_operation_id: null,
    p_input_json: inputJson,
    p_config_json: {
      backend_runner_owned: true,
      frontend_completion_required: false,
      chunk_size: 100,
      scope_seed_limit: 100,
      allocation_seed_limit: 100,
      candidate_insert_limit: 100,
      item_insert_limit: 100,
      breakdown_limit: 100,
      snapshot_limit: 100,
      runner_state: 'RUNNABLE'
    }
  })
}));
if (!start?.operation_id) throw new Error(`V1 operation start failed: ${JSON.stringify(start)}`);
const operationId = start.operation_id;

const broker = fs.readFileSync(brokerPath, 'utf8');
const {
  isCertifiedDeferredFinanceOnlyInsertItemsResult,
  isCertifiedEmptyTimesheetSnapshotsResult
} = await import(`${pathToFileURL(draftInsertItemsHelperPath).href}?h2_v1_oracle=${encodeURIComponent(runId)}`);
const functionStart = broker.indexOf('async function advanceBankingPayDraftCreateOperation');
const functionEnd = broker.indexOf('async function handleBankingPaySnoozeValidate', functionStart);
if (functionStart < 0 || functionEnd <= functionStart) throw new Error('V1 Worker function boundary missing');
const sbRpc = async (_env, name, rpcArgs) => requestJson(`${postgrestOrigin}/rpc/${encodeURIComponent(name)}`, {
  method: 'POST',
  headers: { 'content-type': 'application/json', accept: 'application/json' },
  body: JSON.stringify(rpcArgs || {})
});
const sbFetch = async (_env, address) => {
  const source = new URL(address);
  const relative = source.pathname.replace(/^\/rest\/v1/, '');
  const rows = await requestJson(`${postgrestOrigin}${relative}${source.search}`, { headers: { accept: 'application/json' } });
  return { rows: Array.isArray(rows) ? rows : [] };
};
const context = vm.createContext({
  assertBankingPayWorkbenchContract: async () => true,
  buildBankingPayOperationPublicPayload: row => JSON.parse(JSON.stringify(row || {})),
  console,
  crypto,
  Date,
  encodeURIComponent,
  fetch,
  isCertifiedDeferredFinanceOnlyInsertItemsResult,
  isCertifiedEmptyTimesheetSnapshotsResult,
  Map,
  revolutEnsurePayeesReadyFromPreview: async () => ({ ok: true, remaining: 0, failed: 0, next_cursor: null }),
  sbFetch,
  sbRpc,
  scheduleBankingPayWorkbenchDrainWithDurableWake: async () => ({ ok: true, scheduled: false, skipped: true }),
  Set,
  structuredClone,
  tsfinBestEffortMakeReadyForDraft: async () => ({ ok: true, remaining: 0, made_ready: 0, failed: 0, still_pending: 0, next_cursor: null }),
  URL,
  URLSearchParams,
  WeakSet
});
vm.runInContext(`${broker.slice(functionStart, functionEnd)}\nglobalThis.advanceDraft=advanceBankingPayDraftCreateOperation;`, context);

const fetchOperation = async () => {
  const rows = await requestJson(`${postgrestOrigin}/banking_pay_operations?id=eq.${operationId}&select=*`);
  if (!Array.isArray(rows) || rows.length !== 1) throw new Error(`Operation row missing: ${operationId}`);
  return rows[0];
};
const phaseHistory = [];
let operation = await fetchOperation();
for (let call = 1; call <= 500; call += 1) {
  const before = String(operation.phase || '').toUpperCase();
  const startedAt = Date.now();
  const result = await context.advanceDraft(
    { SUPABASE_URL: postgrestOrigin },
    operation,
    { id: ACTOR_ID },
    { singleStep: true, maxPhaseUnits: 1, maxChunksPerCall: 1, requestBudgetMs: 15000, lockOwner: `H2_V1_ORACLE:${targetCount}` }
  );
  operation = await fetchOperation();
  phaseHistory.push({ call, before, after: String(operation.phase || '').toUpperCase(), status: String(operation.status || '').toUpperCase(), elapsed_ms: Date.now() - startedAt });
  if (['COMPLETE', 'FAILED', 'ERROR', 'REVIEW_REQUIRED', 'CANCELLED'].includes(String(operation.status || '').toUpperCase())) break;
  if (result?.error || result?.error_json) throw new Error(`V1 Worker returned error: ${JSON.stringify(result).slice(0, 4000)}`);
}

const summarySql = `
with operation_batches as (
  select batch_id.value::uuid as pay_batch_id
  from public.banking_pay_operations operation
  cross join lateral pg_catalog.jsonb_array_elements_text(operation.result_json->'created_pay_batch_ids') batch_id(value)
  where operation.id='${operationId}'::uuid
)
select pg_catalog.jsonb_build_object(
  'operation_id','${operationId}'::uuid,
  'operation_status',(select status from public.banking_pay_operations where id='${operationId}'::uuid),
  'operation_phase',(select phase from public.banking_pay_operations where id='${operationId}'::uuid),
  'batch_count',(select count(*) from operation_batches),
  'paye_batch_count',(select count(*) from operation_batches operation_batch join public.pay_batches b on b.id=operation_batch.pay_batch_id where upper(btrim(b.batch_kind_fixed))='PAYE'),
  'umbrella_batch_count',(select count(*) from operation_batches operation_batch join public.pay_batches b on b.id=operation_batch.pay_batch_id where upper(btrim(b.batch_kind_fixed))='UMBRELLA'),
  'candidate_count',(select count(*) from public.pay_batch_candidates c join operation_batches operation_batch on operation_batch.pay_batch_id=c.pay_batch_id),
  'item_count',(select count(*) from public.pay_batch_items i join public.pay_batch_candidates c on c.id=i.pay_batch_candidate_id join operation_batches operation_batch on operation_batch.pay_batch_id=c.pay_batch_id where coalesce(i.is_voided,false)=false),
  'paye_item_count',(select count(*) from public.pay_batch_items i join public.pay_batch_candidates c on c.id=i.pay_batch_candidate_id join operation_batches operation_batch on operation_batch.pay_batch_id=c.pay_batch_id where i.pay_channel='PAYE' and coalesce(i.is_voided,false)=false),
  'umbrella_item_count',(select count(*) from public.pay_batch_items i join public.pay_batch_candidates c on c.id=i.pay_batch_candidate_id join operation_batches operation_batch on operation_batch.pay_batch_id=c.pay_batch_id where i.pay_channel='UMBRELLA' and coalesce(i.is_voided,false)=false),
  'total_ex_vat',(select coalesce(sum(i.amount_ex_vat),0)::text from public.pay_batch_items i join public.pay_batch_candidates c on c.id=i.pay_batch_candidate_id join operation_batches operation_batch on operation_batch.pay_batch_id=c.pay_batch_id where coalesce(i.is_voided,false)=false),
  'external_action_count',(
    (select count(*) from public.pay_bank_transfers t join operation_batches operation_batch on operation_batch.pay_batch_id=t.pay_batch_id)
    +(select count(*) from public.banking_pay_operation_provider_attempts a join operation_batches operation_batch on operation_batch.pay_batch_id=a.pay_batch_id)
  )
)::text;`;
const summary = JSON.parse(run('docker', ['exec', container, 'psql', '-X', '-U', 'postgres', '-d', database, '-Atc', summarySql]).trim());
if (summary.operation_status !== 'COMPLETE') throw new Error(`V1 route did not complete: ${JSON.stringify({ summary, phaseHistory }, null, 2)}`);
if (Number(summary.batch_count) !== 2 || Number(summary.paye_batch_count) !== 1 || Number(summary.umbrella_batch_count) !== 1) {
  throw new Error(`V1 mixed PAYE/Umbrella split changed: ${JSON.stringify(summary)}`);
}
if (Number(summary.item_count) !== targetCount || Number(summary.paye_item_count) <= 0 || Number(summary.umbrella_item_count) <= 0) {
  throw new Error(`V1 mixed PAYE/Umbrella item membership changed: ${JSON.stringify(summary)}`);
}
if (Number(summary.external_action_count) !== 0) throw new Error(`V1 oracle crossed external action boundary: ${JSON.stringify(summary)}`);

process.stdout.write(`${JSON.stringify({
  contract: 'BANKING_PAY_DRAFT_V1_MIXED_CHANNEL_ORACLE_RESULT_V1',
  oracle_source_head: run('git', ['rev-parse', 'HEAD'], { cwd: oracleRoot }).trim(),
  engine: version,
  target_count: targetCount,
  paye_and_umbrella_created_together: true,
  summary,
  phase_call_count: phaseHistory.length,
  maximum_call_ms: Math.max(...phaseHistory.map(row => row.elapsed_ms)),
  phase_history: phaseHistory,
  external_payment_actions: 0
}, null, 2)}\n`);
