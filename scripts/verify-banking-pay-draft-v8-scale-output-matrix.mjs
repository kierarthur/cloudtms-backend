import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import process from 'node:process';
import { fileURLToPath } from 'node:url';

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
const onlyCase = args.get('case');
const profileNestedSql = args.get('profile') === 'true';
const persistDisposable = args.get('persist-disposable') === 'true';
if (!container || !/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$/.test(container)) {
  throw new Error('A safe --container value is required');
}
if (!database || !/^[a-zA-Z_][a-zA-Z0-9_]{0,62}$/.test(database)) {
  throw new Error('A safe --database value is required');
}
if (persistDisposable && (
  onlyCase !== 'MIXED_PAYE_UMBRELLA_ORACLE_8'
  || !/^h12-v8-restart-pg(?:17|18)$/.test(container)
  || database !== 'banking_modal_v2_test'
)) {
  throw new Error('Disposable persistence is limited to the exact task-owned mixed-channel oracle database');
}

const basePath = 'tests/03092026_1010_banking_pay_workbench_settled_certificate_v8_scale_verification.sql';
const casesPath = 'tests/fixtures/banking-pay-draft-v8-scale-output-cases-v1.json';
const read = relativePath => fs.readFileSync(path.join(root, relativePath), 'utf8');
const sha256 = value => createHash('sha256').update(value).digest('hex');
const baseRaw = read(basePath);
const base = baseRaw.replaceAll('\r\n', '\n');
const casesText = read(casesPath);
const caseContract = JSON.parse(casesText);

function replaceOnce(text, needle, replacement, label) {
  const parts = text.split(needle);
  if (parts.length !== 2) throw new Error(`${label} expected exactly once; observed ${parts.length - 1}`);
  return `${parts[0]}${replacement}${parts[1]}`;
}

const versionResult = spawnSync(
  'docker',
  ['exec', container, 'psql', '-X', '-U', 'postgres', '-d', database, '-Atc', 'select version()'],
  { encoding: 'utf8', maxBuffer: 1024 * 1024 }
);
if (versionResult.status !== 0) throw new Error(`Unable to read disposable PostgreSQL identity: ${versionResult.stderr.trim()}`);
const engine = versionResult.stdout.trim();
if (!/^PostgreSQL (17\.11|18\.6)\b/.test(engine)) throw new Error(`Unsupported disposable PostgreSQL engine: ${engine}`);

const selectedCases = onlyCase
  ? caseContract.cases.filter(testCase => testCase.case_id === onlyCase)
  : caseContract.cases;
if (onlyCase && selectedCases.length !== 1) throw new Error(`Unknown --case ${onlyCase}`);

const results = [];
for (const testCase of selectedCases) {
  let sql = base;
  const transactionOutcome = persistDisposable ? 'COMMIT_TASK_OWNED_DISPOSABLE' : 'ROLLBACK';
  sql = replaceOnce(
    sql,
    '  IF v_target NOT IN (101,1001,2000,5000) THEN',
    '  IF v_target NOT IN (8,101,1001,2000,5000) THEN',
    'mixed-channel oracle target allowlist'
  );
  if (profileNestedSql) {
    sql = replaceOnce(
      sql,
      `BEGIN;\nSET LOCAL statement_timeout = '60min';`,
      `BEGIN;\nLOAD 'auto_explain';\nSET LOCAL auto_explain.log_min_duration = '250ms';\nSET LOCAL auto_explain.log_analyze = on;\nSET LOCAL auto_explain.log_buffers = on;\nSET LOCAL auto_explain.log_timing = off;\nSET LOCAL auto_explain.log_nested_statements = on;\nSET LOCAL auto_explain.log_level = log;\nSET LOCAL statement_timeout = '60min';`,
      'optional nested SQL profiling preamble'
    );
  }
  const rowsBlock = `  CREATE TEMP TABLE h1_v8_scale_rows AS
  SELECT row_ordinal,
         ((row_ordinal-1) % v_candidate_count)+1 AS candidate_seq,
         format('70000000-0000-0000-0000-%s',lpad(row_ordinal::text,12,'0'))::uuid AS source_line_id,
         format('80000000-0000-0000-0000-%s',lpad(row_ordinal::text,12,'0'))::uuid AS preview_row_id
  FROM pg_catalog.generate_series(1,v_target) row_ordinal;`;
  const replacementRows = `  CREATE TEMP TABLE h1_v8_scale_rows AS
  WITH row_seed AS (
    SELECT row_ordinal,
           ((row_ordinal-1) % v_candidate_count)+1 AS candidate_seq,
           pg_catalog.floor((row_ordinal-1)/v_candidate_count)::integer+1 AS candidate_local_ordinal
    FROM pg_catalog.generate_series(1,v_target) row_ordinal
  ), grouped AS (
    SELECT row_seed.*,
           pg_catalog.ceil(candidate_local_ordinal::numeric/${testCase.segments_per_timesheet})::integer AS candidate_timesheet_ordinal
    FROM row_seed
  )
  SELECT row_ordinal,candidate_seq,candidate_local_ordinal,candidate_timesheet_ordinal,
         format('70000000-0000-0000-0000-%s',lpad(row_ordinal::text,12,'0'))::uuid AS source_line_id,
         format('80000000-0000-0000-0000-%s',lpad(row_ordinal::text,12,'0'))::uuid AS preview_row_id,
         format('72000000-0000-0000-%s-%s',lpad(candidate_seq::text,4,'0'),lpad(candidate_timesheet_ordinal::text,12,'0'))::uuid AS timesheet_id,
         format('73000000-0000-0000-0000-%s',lpad(row_ordinal::text,12,'0'))::uuid AS adjustment_id
  FROM grouped;`;
  sql = replaceOnce(sql, rowsBlock, replacementRows, 'scale row identity block');

  const setupStart = `  INSERT INTO public.banking_pay_snapshot_runs(`;
  const setupReplacement = `  INSERT INTO public.app_change_counters(entity_key,seq)
  VALUES ('pay_candidate_scope_generation',1)
  ON CONFLICT (entity_key) DO UPDATE
  SET seq=GREATEST(public.app_change_counters.seq,EXCLUDED.seq);

  INSERT INTO public.umbrellas(
    id,name,remittance_email,bank_name,sort_code,account_number,
    vat_chargeable,enabled,bank_details_hash
  ) VALUES (
    '65000000-0000-0000-0000-000000000001',v_prefix||':UMBRELLA',
    'h2-v8-scale@example.invalid','H2 Scale Bank','112233','87654321',
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

  INSERT INTO public.banking_pay_snapshot_runs(`;
  sql = replaceOnce(sql, setupStart, setupReplacement, 'scale prerequisites');

  const candidateInsert = `  INSERT INTO public.candidates(id,display_name,tms_ref,pay_method)
  SELECT candidate_id,v_prefix||':CANDIDATE:'||candidate_seq::text,
         'CCR-'||(99000000+candidate_seq)::text,pay_channel
  FROM h1_v8_scale_candidates ORDER BY candidate_seq;`;
  const candidateReplacement = `  INSERT INTO public.candidates(
    id,display_name,tms_ref,pay_method,umbrella_id,active,
    account_holder,sort_code,account_number,bank_details_hash
  )
  SELECT candidate_id,v_prefix||':CANDIDATE:'||candidate_seq::text,
         'CCR-'||(99000000+candidate_seq)::text,pay_channel,
         CASE WHEN pay_channel='UMBRELLA' THEN '65000000-0000-0000-0000-000000000001'::uuid END,
         true,v_prefix||':CANDIDATE:'||candidate_seq::text,'010203',
         pg_catalog.lpad((12000000+candidate_seq)::text,8,'0'),v_prefix||':BANK:'||candidate_seq::text
  FROM h1_v8_scale_candidates ORDER BY candidate_seq;`;
  sql = replaceOnce(sql, candidateInsert, candidateReplacement, 'scale candidate payee setup');

  const oneTimesheet = `  INSERT INTO public.timesheets(
    timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,week_ending_date
  ) VALUES (v_timesheet_id,v_prefix,v_prefix,'H1','H1','H1',DATE '2099-03-29');`;
  const rowBackedTimesheets = `  PERFORM pg_catalog.set_config('cloudtms.lifecycle_mutation_context','manual_timesheet_save',true);
  INSERT INTO public.timesheets(
    timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
    week_ending_date,sheet_scope,authorised_at_server,is_current,version,is_adjustment
  )
  SELECT row.timesheet_id,v_prefix||':TS:'||row.candidate_seq::text||':'||row.candidate_timesheet_ordinal::text,
         v_prefix||':OCC:'||row.candidate_seq::text||':'||row.candidate_timesheet_ordinal::text,
         'H2 Scale Hospital','H2 Scale Ward','H2 Scale Role',DATE '2099-03-29',
         'DAILY',pg_catalog.clock_timestamp(),true,1,false
  FROM h1_v8_scale_rows row
  GROUP BY row.timesheet_id,row.candidate_seq,row.candidate_timesheet_ordinal
  ORDER BY row.timesheet_id;
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
         candidate.candidate_id,v_client_id,'H2 Scale Role',candidate.pay_channel,
         v_prefix||':OCC:'||row.candidate_seq::text||':'||row.candidate_timesheet_ordinal::text,
         'ASSIGNED','READY_FOR_INVOICE',0,0,0,0,
         pg_catalog.count(*)::numeric,pg_catalog.count(*)::numeric,0,false,false,false
  FROM h1_v8_scale_rows row
  JOIN h1_v8_scale_candidates candidate USING(candidate_seq)
  GROUP BY row.timesheet_id,row.candidate_seq,row.candidate_timesheet_ordinal,candidate.candidate_id,candidate.pay_channel;

  INSERT INTO public.ts_pay_adjustments(
    id,timesheet_id,candidate_id,client_id,week_ending_date,delta_pay_ex_vat,
    reason,as_advance,meta_json
  )
  SELECT row.adjustment_id,row.timesheet_id,candidate.candidate_id,v_client_id,DATE '2099-03-29',
         1.00,v_prefix||':ADJUSTMENT:'||row.row_ordinal::text,false,
         pg_catalog.jsonb_build_object('h2_fixture',true,'row_ordinal',row.row_ordinal)
  FROM h1_v8_scale_rows row
  JOIN h1_v8_scale_candidates candidate USING(candidate_seq)
  ORDER BY row.row_ordinal;`;
  sql = replaceOnce(sql, oneTimesheet, rowBackedTimesheets, 'row-backed Timesheet setup');

  sql = sql.replaceAll('v_timesheet_id,ARRAY[v_timesheet_id]', 'row.timesheet_id,ARRAY[row.timesheet_id]');
  sql = sql.replaceAll("v_prefix||':LINE:'||row.row_ordinal::text,v_timesheet_id,'canonical_preview_lines'", "v_prefix||':LINE:'||row.row_ordinal::text,row.timesheet_id,'canonical_preview_lines'");
  sql = sql.replaceAll("'H1SCALE:'||row.row_ordinal::text", 'row.adjustment_id::text');
  sql = sql.replaceAll("v_prefix||':ROW:'||row.row_ordinal::text,row.row_ordinal,", "v_prefix||':ROW:'||row.row_ordinal::text,row.row_ordinal,");
  sql = replaceOnce(
    sql,
    `  SELECT row.preview_row_id,v_session_id,candidate.candidate_id,v_timesheet_id,`,
    `  SELECT row.preview_row_id,v_session_id,candidate.candidate_id,row.timesheet_id,`,
    'preview-row Timesheet identity'
  );

  const previewIdentity = `'client_id',v_client_id::text,'timesheet_id',v_timesheet_id::text,`;
  const certifiedPreviewIdentity = `'client_id',v_client_id::text,'timesheet_id',row.timesheet_id::text,
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
           'policy_x_authority_scope','PRE_DRAFT_LIVE_TRUTH',`;
  sql = replaceOnce(sql, previewIdentity, certifiedPreviewIdentity, 'certified preview identity');
  sql = replaceOnce(
    sql,
    `'preview_contract',pg_catalog.jsonb_build_object('ok',true,'selection_allowed',true),`,
    `'preview_contract',pg_catalog.jsonb_build_object(
             'ok',true,'materialisable',true,'target_section','canonical_preview_lines',
             'presentation_section','READY_TO_PAY','selection_allowed',true),`,
    'preview contract'
  );

  const noticePoint = `  PERFORM pg_catalog.set_config('client_min_messages','notice',true);`;
  const generationSync = `  UPDATE public.banking_pay_workbench_sessions
  SET scope_change_generation_target=public.pay_workbench_scope_current_generation_v1(),
      scope_change_generation_applied=public.pay_workbench_scope_current_generation_v1(),
      scope_change_generation_shadow_checked=public.pay_workbench_scope_current_generation_v1()
  WHERE id=v_session_id;

  PERFORM pg_catalog.set_config('client_min_messages','notice',true);`;
  sql = replaceOnce(sql, noticePoint, generationSync, 'settled generation sync');

  const transportEnd = `$h2_transport$;
\\endif`;
  const boundedAdvanceStatements = Array.from(
    { length: 1000 },
    (_, index) => `SELECT pg_temp.h2_v8_scale_advance_once(${index + 1});`
  ).join('\n');
  const fullOutput = `$h2_transport$;

CREATE TEMPORARY TABLE pg_temp.h2_v8_scale_advance_calls(
  call_ordinal integer PRIMARY KEY,
  phase_before text NOT NULL,
  result_json jsonb NOT NULL,
  elapsed_ms numeric NOT NULL
) ON COMMIT DROP;

CREATE OR REPLACE FUNCTION pg_temp.h2_v8_scale_advance_once(p_call_ordinal integer)
RETURNS void
LANGUAGE plpgsql
AS $h2_advance_once$
DECLARE
  v_operation_id uuid := (SELECT operation_id FROM h1_v8_scale_result);
  v_receipt text;
  v_result jsonb;
  v_started timestamptz;
  v_elapsed numeric;
  v_phase text;
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_temp.h2_v8_scale_advance_calls AS prior_call
    WHERE prior_call.result_json->>'work_kind'='READY_FOR_TERMINAL_FINISH'
  ) THEN
    RETURN;
  END IF;

  SELECT receipt_digest_sha256 INTO STRICT v_receipt
  FROM private.banking_pay_draft_frozen_stage_receipts_v8
  WHERE operation_id=v_operation_id AND stage_kind='CERTIFICATE_PARTITION_REFS'
    AND stage_status='TERMINAL' AND has_more=false
  ORDER BY page_sequence DESC LIMIT 1;

  SELECT operation_row.phase INTO STRICT v_phase
  FROM public.banking_pay_operations AS operation_row
  WHERE operation_row.id=v_operation_id;
  v_started:=pg_catalog.clock_timestamp();
  -- Match the real PostgREST/Worker call shape. An EXCEPTION block here would
  -- create an artificial PL/pgSQL subtransaction around the whole Draft page
  -- and materially distort the large-run timing and resource profile.
  v_result:=public.banking_pay_draft_advance_bounded_v8(
    v_operation_id,'H2_V8_SCALE_FULL_OUTPUT',v_receipt);
  v_elapsed:=EXTRACT(EPOCH FROM (pg_catalog.clock_timestamp()-v_started))*1000;
  IF v_elapsed>15000 THEN
    RAISE EXCEPTION 'H2_V8_SCALE_FULL_OUTPUT_CALL_OVER_BUDGET:%:%',v_elapsed,v_result;
  END IF;
  INSERT INTO pg_temp.h2_v8_scale_advance_calls(call_ordinal,phase_before,result_json,elapsed_ms)
  VALUES (p_call_ordinal,v_phase,v_result,v_elapsed);
END;
$h2_advance_once$;

DO $h2_full_output_start$
DECLARE
  v_operation_id uuid := (SELECT operation_id FROM h1_v8_scale_result);
BEGIN
  UPDATE public.banking_pay_operations SET phase='SEED_ALLOCATION_ROWS' WHERE id=v_operation_id;
END;
$h2_full_output_start$;

-- These are separate SQL statements in one rollback-contained transaction. That
-- mirrors separate Worker/PostgREST calls, so the unchanged per-call budget starts
-- afresh instead of one synthetic DO statement consuming the whole-run budget.
\\o /dev/null
${boundedAdvanceStatements}
\\o

-- The final assertion is test-harness work, not a production RPC. Each bounded
-- Draft call above independently proved the unchanged 15-second business
-- budget. Restore the rollback fixture's setup/assertion allowance before the
-- exhaustive 5,000-row comparison so test bookkeeping cannot be mistaken for
-- a production timeout.
SET LOCAL statement_timeout = '60min';

DO $h2_full_output$
DECLARE
  v_operation_id uuid := (SELECT operation_id FROM h1_v8_scale_result);
  v_receipt text;
  v_result jsonb;
  v_call_count integer;
  v_phase text;
  v_expected_timesheets integer;
  v_allocation_count integer;
  v_item_count integer;
  v_snapshot_count integer;
  v_breakdown_count integer;
  v_parity_count integer;
  v_batch_count integer;
  v_paye_batch_count integer;
  v_umbrella_batch_count integer;
  v_paye_item_count integer;
  v_umbrella_item_count integer;
  v_paye_total_ex_vat numeric;
  v_umbrella_total_ex_vat numeric;
  v_paye_total_vat numeric;
  v_umbrella_total_vat numeric;
  v_batch_candidate_count integer;
  v_unlinked_count integer;
  v_started timestamptz;
  v_elapsed numeric;
  v_max_call_ms numeric := 0;
  v_sum_call_ms numeric := 0;
  v_max_call_phase text;
  v_phase_timings jsonb := '{}'::jsonb;
BEGIN
  SELECT receipt_digest_sha256 INTO STRICT v_receipt
  FROM private.banking_pay_draft_frozen_stage_receipts_v8
  WHERE operation_id=v_operation_id AND stage_kind='CERTIFICATE_PARTITION_REFS'
    AND stage_status='TERMINAL' AND has_more=false
  ORDER BY page_sequence DESC LIMIT 1;

  SELECT pg_catalog.count(DISTINCT timesheet_id)::integer
  INTO v_expected_timesheets FROM h1_v8_scale_rows;
  SELECT pg_catalog.count(*)::integer,
         pg_catalog.max(call_row.elapsed_ms),
         pg_catalog.sum(call_row.elapsed_ms)
  INTO v_call_count,v_max_call_ms,v_sum_call_ms
  FROM pg_temp.h2_v8_scale_advance_calls AS call_row;
  SELECT call_row.phase_before
  INTO v_max_call_phase
  FROM pg_temp.h2_v8_scale_advance_calls AS call_row
  ORDER BY call_row.elapsed_ms DESC,call_row.call_ordinal
  LIMIT 1;
  SELECT coalesce(jsonb_object_agg(
           phase_timing.phase_before,
           jsonb_build_object(
             'call_count',phase_timing.call_count,
             'max_call_ms',round(phase_timing.max_call_ms,3),
             'sum_call_ms',round(phase_timing.sum_call_ms,3)
           )
         ),'{}'::jsonb)
  INTO v_phase_timings
  FROM (
    SELECT call_row.phase_before,
           count(*)::integer AS call_count,
           max(call_row.elapsed_ms) AS max_call_ms,
           sum(call_row.elapsed_ms) AS sum_call_ms
    FROM pg_temp.h2_v8_scale_advance_calls AS call_row
    GROUP BY call_row.phase_before
    ORDER BY call_row.phase_before
  ) AS phase_timing;
  SELECT call_row.result_json
  INTO v_result
  FROM pg_temp.h2_v8_scale_advance_calls AS call_row
  ORDER BY call_row.call_ordinal DESC
  LIMIT 1;
  SELECT phase INTO STRICT v_phase FROM public.banking_pay_operations WHERE id=v_operation_id;

  SELECT pg_catalog.count(*)::integer,
         pg_catalog.count(*) FILTER (WHERE pay_batch_item_id IS NULL)::integer
  INTO v_allocation_count,v_unlinked_count
  FROM public.banking_pay_operation_candidate_allocation_rows
  WHERE operation_id=v_operation_id;
  SELECT pg_catalog.count(DISTINCT item.id)::integer
  INTO v_item_count
  FROM public.pay_batch_items item
  JOIN public.pay_batch_candidates candidate ON candidate.id=item.pay_batch_candidate_id
  JOIN public.pay_batches batch ON batch.id=candidate.pay_batch_id
  WHERE batch.source_workbench_session_id='20000000-0000-4000-8000-000000000001'
    AND COALESCE(item.is_voided,false)=false;
  SELECT pg_catalog.count(*)::integer INTO v_snapshot_count
  FROM public.pay_batch_timesheet_snapshots snapshot
  JOIN public.pay_batches batch ON batch.id=snapshot.pay_batch_id
  WHERE batch.source_workbench_session_id='20000000-0000-4000-8000-000000000001';
  SELECT pg_catalog.count(*)::integer INTO v_breakdown_count
  FROM public.pay_batch_item_breakdowns breakdown
  JOIN public.pay_batch_items item ON item.id=breakdown.pay_batch_item_id
  JOIN public.pay_batch_candidates candidate ON candidate.id=item.pay_batch_candidate_id
  JOIN public.pay_batches batch ON batch.id=candidate.pay_batch_id
  WHERE batch.source_workbench_session_id='20000000-0000-4000-8000-000000000001';
  SELECT pg_catalog.count(*)::integer INTO v_parity_count
  FROM private.banking_pay_draft_constituent_parity_results_v8
  WHERE operation_id=v_operation_id AND comparison_status='MATCH';
  SELECT pg_catalog.count(*)::integer INTO v_batch_count
  FROM public.pay_batches WHERE source_workbench_session_id='20000000-0000-4000-8000-000000000001';
  SELECT pg_catalog.count(*) FILTER (WHERE UPPER(BTRIM(batch_kind_fixed))='PAYE')::integer,
         pg_catalog.count(*) FILTER (WHERE UPPER(BTRIM(batch_kind_fixed))='UMBRELLA')::integer
  INTO v_paye_batch_count,v_umbrella_batch_count
  FROM public.pay_batches WHERE source_workbench_session_id='20000000-0000-4000-8000-000000000001';
  SELECT pg_catalog.count(*) FILTER (WHERE item.pay_channel='PAYE')::integer,
         pg_catalog.count(*) FILTER (WHERE item.pay_channel='UMBRELLA')::integer,
         COALESCE(pg_catalog.sum(item.amount_ex_vat) FILTER (WHERE item.pay_channel='PAYE'),0),
         COALESCE(pg_catalog.sum(item.amount_ex_vat) FILTER (WHERE item.pay_channel='UMBRELLA'),0),
         COALESCE(pg_catalog.sum(item.amount_vat) FILTER (WHERE item.pay_channel='PAYE'),0),
         COALESCE(pg_catalog.sum(item.amount_vat) FILTER (WHERE item.pay_channel='UMBRELLA'),0)
  INTO v_paye_item_count,v_umbrella_item_count,
       v_paye_total_ex_vat,v_umbrella_total_ex_vat,v_paye_total_vat,v_umbrella_total_vat
  FROM public.pay_batch_items item
  JOIN public.pay_batch_candidates candidate ON candidate.id=item.pay_batch_candidate_id
  JOIN public.pay_batches batch ON batch.id=candidate.pay_batch_id
  WHERE batch.source_workbench_session_id='20000000-0000-4000-8000-000000000001'
    AND COALESCE(item.is_voided,false)=false;
  SELECT pg_catalog.count(*)::integer INTO v_batch_candidate_count
  FROM public.pay_batch_candidates candidate
  JOIN public.pay_batches batch ON batch.id=candidate.pay_batch_id
  WHERE batch.source_workbench_session_id='20000000-0000-4000-8000-000000000001';

  IF v_result->>'work_kind'<>'READY_FOR_TERMINAL_FINISH'
     OR v_phase<>'POST_CREATE_REFRESH'
     OR v_allocation_count<>(SELECT target_count FROM h1_v8_scale_result)
     OR v_unlinked_count<>0
     OR v_item_count<>(SELECT target_count FROM h1_v8_scale_result)
     OR v_snapshot_count<>v_expected_timesheets
     OR v_breakdown_count<>(SELECT target_count FROM h1_v8_scale_result)
     OR v_parity_count<>(SELECT target_count FROM h1_v8_scale_result)
     OR v_batch_count<>2
     OR v_paye_batch_count<>1
     OR v_umbrella_batch_count<>1
     OR v_paye_item_count=0
     OR v_umbrella_item_count=0
     OR v_batch_candidate_count<>(SELECT candidate_count FROM h1_v8_scale_result)
     OR EXISTS (
       SELECT 1
       FROM h1_v8_scale_rows row
       JOIN public.banking_pay_operation_candidate_allocation_rows allocation
         ON allocation.operation_id=v_operation_id
        AND allocation.allocation_basis_json#>>'{line,row_key}'=
            'H1-V8-SCALE-'||(SELECT target_count FROM h1_v8_scale_result)::text||':ROW:'||row.row_ordinal::text
       JOIN public.pay_batch_items item ON item.id=allocation.pay_batch_item_id
       JOIN h1_v8_scale_candidates expected_candidate USING(candidate_seq)
       WHERE allocation.status<>'ITEM_CREATED'
          OR ROUND(allocation.allocated_amount,2)<>1.00
          OR item.timesheet_id IS DISTINCT FROM row.timesheet_id
          OR item.pay_channel IS DISTINCT FROM expected_candidate.pay_channel
          OR item.item_type IS DISTINCT FROM 'ADJUSTMENT_DELTA'
          OR item.source_ref IS DISTINCT FROM
             'H1-V8-SCALE-'||(SELECT target_count FROM h1_v8_scale_result)::text||':ROW:'||row.row_ordinal::text
          OR item.amount_ex_vat IS DISTINCT FROM 1.00
          OR item.amount_vat IS DISTINCT FROM CASE WHEN expected_candidate.pay_channel='UMBRELLA' THEN 0.20 ELSE 0.00 END
          OR item.amount_inc_vat IS DISTINCT FROM CASE WHEN expected_candidate.pay_channel='UMBRELLA' THEN 1.20 ELSE 1.00 END
     ) THEN
    RAISE EXCEPTION 'H2_V8_SCALE_FULL_OUTPUT_MISMATCH:%',pg_catalog.jsonb_build_object(
      'phase',v_phase,'result',v_result,'allocation_count',v_allocation_count,
      'unlinked_count',v_unlinked_count,'item_count',v_item_count,
      'snapshot_count',v_snapshot_count,'expected_timesheets',v_expected_timesheets,
      'breakdown_count',v_breakdown_count,'parity_count',v_parity_count,
      'batch_count',v_batch_count,'paye_batch_count',v_paye_batch_count,
      'umbrella_batch_count',v_umbrella_batch_count,'batch_candidate_count',v_batch_candidate_count,
      'paye_item_count',v_paye_item_count,'umbrella_item_count',v_umbrella_item_count,
      'paye_total_ex_vat',v_paye_total_ex_vat,'umbrella_total_ex_vat',v_umbrella_total_ex_vat,
      'paye_total_vat',v_paye_total_vat,'umbrella_total_vat',v_umbrella_total_vat);
  END IF;

  -- Simulate loss of the final success response. Repeating the exact operation
  -- must return the same ready state and must not create another business row.
  v_result:=public.banking_pay_draft_advance_bounded_v8(
    v_operation_id,'H2_V8_SCALE_FULL_OUTPUT',v_receipt);
  IF v_result->>'work_kind'<>'READY_FOR_TERMINAL_FINISH'
     OR (SELECT pg_catalog.count(*) FROM public.banking_pay_operation_candidate_allocation_rows
         WHERE operation_id=v_operation_id)<>v_allocation_count
     OR (SELECT pg_catalog.count(*) FROM private.banking_pay_draft_constituent_parity_results_v8
         WHERE operation_id=v_operation_id)<>v_parity_count THEN
    RAISE EXCEPTION 'H2_V8_SCALE_FINAL_RESPONSE_LOSS_REPLAY_CHANGED:%',v_result;
  END IF;

  RAISE NOTICE 'H2_V8_SCALE_FULL_OUTPUT_PASS=%',pg_catalog.jsonb_build_object(
    'case_id','${testCase.case_id}','target_count',(SELECT target_count FROM h1_v8_scale_result),
    'timesheet_count',v_expected_timesheets,'candidate_count',(SELECT candidate_count FROM h1_v8_scale_result),
    'batch_count',v_batch_count,'batch_candidate_count',v_batch_candidate_count,
    'paye_batch_count',v_paye_batch_count,'umbrella_batch_count',v_umbrella_batch_count,
    'paye_item_count',v_paye_item_count,'umbrella_item_count',v_umbrella_item_count,
    'paye_total_ex_vat',ROUND(v_paye_total_ex_vat,2),
    'umbrella_total_ex_vat',ROUND(v_umbrella_total_ex_vat,2),
    'paye_total_vat',ROUND(v_paye_total_vat,2),
    'umbrella_total_vat',ROUND(v_umbrella_total_vat,2),
    'item_count',v_item_count,'allocation_count',v_allocation_count,
    'snapshot_count',v_snapshot_count,'breakdown_count',v_breakdown_count,
    'parity_count',v_parity_count,'bounded_advance_call_count',v_call_count,
    'final_response_loss_replay',true,
    'summed_bounded_call_ms',ROUND(v_sum_call_ms,3),
    'max_call_ms',ROUND(v_max_call_ms,3),'max_call_phase',v_max_call_phase,
    'phase_timings',v_phase_timings,'transaction_outcome','${transactionOutcome}');
END;
$h2_full_output$;
\\endif`;
  sql = replaceOnce(sql, transportEnd, fullOutput, 'full Draft scale output proof');

  // The PG17 disposable database already retains the earlier 101-row scale
  // evidence. Give the committed mixed-channel oracle its own deterministic
  // fixture namespace so neither proof has to delete or overwrite the other.
  if (persistDisposable && /^PostgreSQL 17\.11\b/.test(engine)) {
    const pg17MixedNamespace = new Map([
      ['20000000-0000-4000-8000-000000000001', '22000000-0000-4000-8000-000000000001'],
      ['30000000-0000-0000-0000-000000000001', '32000000-0000-0000-0000-000000000001'],
      ['40000000-0000-0000-0000-000000000001', '42000000-0000-0000-0000-000000000001'],
      ['50000000-0000-0000-0000-000000000001', '52000000-0000-0000-0000-000000000001'],
      ['60000000-0000-0000-0000-', '66000000-0000-0000-0000-'],
      ['61000000-0000-0000-0000-', '66100000-0000-0000-0000-'],
      ['62000000-0000-0000-0000-', '66200000-0000-0000-0000-'],
      ['63000000-0000-0000-0000-', '66300000-0000-0000-0000-'],
      ['64000000-0000-0000-0000-', '66400000-0000-0000-0000-'],
      ['65000000-0000-0000-0000-', '66500000-0000-0000-0000-'],
      ['70000000-0000-0000-0000-', '77000000-0000-0000-0000-'],
      ['72000000-0000-0000-', '77200000-0000-0000-'],
      ['73000000-0000-0000-0000-', '77300000-0000-0000-0000-'],
      ['74000000-0000-0000-', '77400000-0000-0000-'],
      ['80000000-0000-0000-0000-', '88000000-0000-0000-0000-']
    ]);
    for (const [sourceIdentity, isolatedIdentity] of pg17MixedNamespace) {
      sql = sql.replaceAll(sourceIdentity, isolatedIdentity);
    }
  }

  if (persistDisposable) {
    sql = replaceOnce(
      sql,
      `\\if :worker_seed_only
COMMIT;
\\else
ROLLBACK;
\\endif`,
      'COMMIT;',
      'task-owned disposable commit boundary'
    );
  } else if (!sql.endsWith('\n') || (sql.match(/^\s*ROLLBACK\s*;\s*$/gim) ?? []).length !== 1) {
    throw new Error(`${testCase.case_id} must preserve one final rollback`);
  }
  const started = performance.now();
  const execution = spawnSync(
    'docker',
    [
      'exec', '-i', '-e', 'PGOPTIONS=-c jit=off', container,
      'psql', '-X', '-v', 'ON_ERROR_STOP=1', '-v', `target_count=${testCase.target_count}`,
      '-v', 'h2_transport=true', '-U', 'postgres', '-d', database
    ],
    { input: sql, encoding: 'utf8', maxBuffer: 64 * 1024 * 1024 }
  );
  const output = `${execution.stdout ?? ''}\n${execution.stderr ?? ''}`;
  const passMarker = `H2_V8_SCALE_FULL_OUTPUT_PASS={"case_id": "${testCase.case_id}"`;
  if (execution.status !== 0 || !output.includes(passMarker)) {
    throw new Error(`${testCase.case_id} failed with exit ${execution.status}:\n${output.split(/\r?\n/).slice(-500).join('\n')}`);
  }
  const noticeLine = output.split(/\r?\n/).find(line => line.includes('H2_V8_SCALE_FULL_OUTPUT_PASS='));
  const outputLines = output.split(/\r?\n/);
  const profileTopStatements = profileNestedSql
    ? outputLines.flatMap((line, index) => {
        const match = line.match(/duration:\s+([0-9.]+)\s+ms\s+plan:/i);
        if (!match) return [];
        const durationMs = Number(match[1]);
        if (!Number.isFinite(durationMs) || durationMs < 1000) return [];
        const nearby = outputLines
          .slice(index + 1, index + 25)
          .find(candidate => /Query Text:|Function Scan|Seq Scan|Index Scan|Bitmap/i.test(candidate));
        return [{ duration_ms: durationMs, evidence_line: (nearby ?? line).trim().slice(0, 500) }];
      })
      .sort((left, right) => right.duration_ms - left.duration_ms)
      .slice(0, 25)
    : [];
  results.push({
    case_id: testCase.case_id,
    target_count: testCase.target_count,
    segments_per_timesheet: testCase.segments_per_timesheet,
    class_ids: testCase.class_ids,
    status: 'PASS',
    evidence_tier: 'FULL_TYPED_CURRENT_V8_DRAFT_OUTPUT',
    v1_v8_typed_parity_status: 'OPEN',
    transaction_outcome: transactionOutcome,
    elapsed_ms: Math.round((performance.now() - started) * 1000) / 1000,
    notice: noticeLine?.slice(noticeLine.indexOf('{')) ?? null,
    ...(profileNestedSql ? { profile_top_statements: profileTopStatements } : {})
  });
}

process.stdout.write(`${JSON.stringify({
  contract: 'BANKING_PAY_DRAFT_V8_SCALE_OUTPUT_MATRIX_RESULT_V1',
  engine,
  database,
  base_source_sha256: sha256(baseRaw),
  cases_sha256: sha256(casesText),
  configured_scalar_ceiling: caseContract.configured_scalar_ceiling,
  materialised_row_ceiling: caseContract.materialised_row_ceiling,
  materialised_50000_test_prohibited: true,
  unchanged_budgets: {
    statement_timeout_ms: 15000,
    lock_timeout_ms: 1500,
    idle_in_transaction_session_timeout_ms: 30000
  },
  case_count: results.length,
  pass_count: results.length,
  fail_count: 0,
  class_pass_count: new Set(results.flatMap(result => result.class_ids)).size,
  external_payment_actions: 0,
  transaction_outcome: persistDisposable ? 'COMMIT_TASK_OWNED_DISPOSABLE' : 'ROLLBACK',
  results
}, null, 2)}\n`);
