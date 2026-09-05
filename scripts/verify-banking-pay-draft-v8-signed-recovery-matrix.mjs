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
const expectedEngine = args.get('engine');
if (!container || !/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$/.test(container)) {
  throw new Error('A safe --container value is required');
}
if (!database || !/^[a-zA-Z_][a-zA-Z0-9_]{0,62}$/.test(database)) {
  throw new Error('A safe --database value is required');
}
if (expectedEngine && !/^PostgreSQL (17\.11|18\.6)\b/.test(expectedEngine)) {
  throw new Error('--engine must start with PostgreSQL 17.11 or PostgreSQL 18.6');
}

const read = relativePath => fs.readFileSync(path.join(root, relativePath), 'utf8');
const sha256 = value => createHash('sha256').update(value).digest('hex');
const runtimePath = 'tests/01092026_1511_banking_pay_signed_recovery_draft_runtime_verification.sql';
const classifierPath = 'supabase/verification/01092026_1648_banking_pay_signed_recovery_classifier_verification.sql';
const selectionPath = 'tests/fixtures/28082026_1429_banking_pay_selection_setup.sql';
const casesPath = 'tests/fixtures/banking-pay-draft-v8-signed-recovery-cases-v1.json';
const runtime = read(runtimePath);
const classifier = read(classifierPath);
const selection = read(selectionPath);
const caseText = read(casesPath);
const caseContract = JSON.parse(caseText);
const quoteLiteral = value => `'${String(value).replaceAll("'", "''")}'`;

function replaceOnce(text, needle, replacement, label) {
  const parts = text.split(needle);
  if (parts.length !== 2) throw new Error(`${label} expected exactly once; observed ${parts.length - 1}`);
  return `${parts[0]}${replacement}${parts[1]}`;
}

let selectionFixture = replaceOnce(
  selection,
  "current_database()<>'banking_modal_v2_test'",
  `current_database()<>${quoteLiteral(database)}`,
  'selection fixture database guard'
);
let sql = replaceOnce(
  runtime,
  '\\ir fixtures/28082026_1429_banking_pay_selection_setup.sql',
  selectionFixture,
  'selection fixture include'
);

const mixedPayload = String.raw`
do $h2_p5_mixed_payload$
declare
  signed_component jsonb;
  ordinary_component_a jsonb;
  ordinary_component_b jsonb;
begin
  select frozen_resolution_payload_json#>'{case_components,0}'
  into strict signed_component
  from public.pay_batch_items
  where id='10000000-0000-4000-8000-000000009105';
  ordinary_component_a:=jsonb_build_object(
    'component_key_type','TS_DAY','component_key_value','2026-03-13',
    'component_amount_ex_vat',52.17::numeric,'source_pay_ex_vat',52.17::numeric,
    'source_charge_ex_vat',90::numeric,'source_pay_method','PAYE',
    'current_target_pay_method','PAYE',
    'source_basis_json',jsonb_build_object('work_date','2026-03-13')
  );
  ordinary_component_b:=jsonb_build_object(
    'component_key_type','TS_DAY','component_key_value','2026-03-13',
    'component_amount_ex_vat',-17.39::numeric,'source_pay_ex_vat',-17.39::numeric,
    'source_charge_ex_vat',-30::numeric,'source_pay_method','PAYE',
    'current_target_pay_method','PAYE',
    'source_basis_json',jsonb_build_object(
      'component_fallback','WORKED_TIME_AMOUNT','work_date','2026-03-13'
    )
  );
  update public.pay_batch_items
  set frozen_resolution_payload_json=jsonb_build_object(
    'case_components',jsonb_build_array(ordinary_component_a,signed_component,ordinary_component_b)
  )
  where id='10000000-0000-4000-8000-000000009105';
end;
$h2_p5_mixed_payload$;
`;
const allocationMarker = 'insert into public.banking_pay_operation_candidate_allocation_rows(';
const allocationParts = sql.split(allocationMarker);
if (allocationParts.length !== 3) {
  throw new Error(`mixed same-key payload expected two allocation inserts; observed ${allocationParts.length - 1}`);
}
sql = `${allocationParts[0]}${mixedPayload}\n${allocationMarker}${allocationParts[1]}${allocationMarker}${allocationParts[2]}`;

const assertions = String.raw`
do $h2_p5_exact_assertions$
declare
  item_json jsonb;
  signed_component jsonb;
  ordinary_count integer;
  evidence jsonb;
  before_item_count integer;
  before_allocation_count integer;
  before_reservation_count integer;
  after_item_count integer;
  after_allocation_count integer;
  after_reservation_count integer;
  rejected_count integer:=0;
  candidate jsonb;
begin
  select to_jsonb(item),
         item.frozen_resolution_payload_json#>'{case_components,1}',
         jsonb_array_length(item.frozen_resolution_payload_json->'case_components')-1
  into strict item_json,signed_component,ordinary_count
  from public.pay_batch_items item
  where item.id='10000000-0000-4000-8000-000000009105';
  evidence:=private.pay_batch_signed_non_charge_recovery_evidence_v1(item_json);
  if ordinary_count<>2
     or evidence->>'contract'<>'SIGNED_NON_CHARGE_RECOVERY_DRAFT_V1'
     or evidence->>'economic_key_type'<>'TS_DAY'
     or evidence->>'economic_key_value'<>'2026-03-13'
     or (evidence->>'outstanding_ex_vat')::numeric<>37.39
     or not exists(
       select 1 from public.pay_batch_items
       where id='10000000-0000-4000-8000-000000009105'
         and amount_ex_vat=37.39 and amount_vat=0 and amount_inc_vat=37.39
         and frozen_component_key_type='TS_DAY'
         and frozen_component_key_value='2026-03-13'
     ) then
    raise exception 'H2_P5_SIGNED_MIXED_OUTPUT_MISMATCH:%',evidence;
  end if;
  select count(*) into before_item_count from public.pay_batch_items;
  select count(*) into before_allocation_count from public.banking_pay_operation_candidate_allocation_rows;
  select count(*) into before_reservation_count from public.pay_advance_reservations;

  candidate:=item_json||jsonb_build_object(
    'frozen_resolution_payload_json',jsonb_build_object(
      'case_components',(item_json#>'{frozen_resolution_payload_json,case_components}')||jsonb_build_array(signed_component)
    )
  );
  begin
    perform private.pay_batch_signed_non_charge_recovery_evidence_v1(candidate);
    raise exception 'H2_P5_TWO_DECISIVE_MATCHES_ACCEPTED';
  exception when sqlstate '23514' then
    if sqlerrm<>'PAY_BATCH_SIGNED_NON_CHARGE_RECOVERY_EVIDENCE_INVALID' then raise; end if;
    rejected_count:=rejected_count+1;
    raise notice 'H2_V8_SIGNED_RECOVERY_REJECT=signed_two_decisive_matches|23514|PAY_BATCH_SIGNED_NON_CHARGE_RECOVERY_EVIDENCE_INVALID';
  end;

  candidate:=jsonb_set(
    item_json,'{frozen_resolution_payload_json,case_components,1,sealed_evidence_digest}',
    to_jsonb('tampered'::text),false
  );
  begin
    perform private.pay_batch_signed_non_charge_recovery_evidence_v1(candidate);
    raise exception 'H2_P5_TAMPERED_SEAL_ACCEPTED';
  exception when sqlstate '23514' then
    if sqlerrm<>'PAY_BATCH_SIGNED_NON_CHARGE_RECOVERY_EVIDENCE_INVALID' then raise; end if;
    rejected_count:=rejected_count+1;
  end;

  candidate:=item_json #- '{frozen_resolution_payload_json,case_components,1,physical_bucket_digest}';
  begin
    perform private.pay_batch_signed_non_charge_recovery_evidence_v1(candidate);
    raise exception 'H2_P5_INCOMPLETE_SHAPE_ACCEPTED';
  exception when sqlstate '23514' then
    if sqlerrm<>'PAY_BATCH_SIGNED_NON_CHARGE_RECOVERY_EVIDENCE_INVALID' then raise; end if;
    rejected_count:=rejected_count+1;
  end;

  select count(*) into after_item_count from public.pay_batch_items;
  select count(*) into after_allocation_count from public.banking_pay_operation_candidate_allocation_rows;
  select count(*) into after_reservation_count from public.pay_advance_reservations;
  if rejected_count<>3
     or after_item_count<>before_item_count
     or after_allocation_count<>before_allocation_count
     or after_reservation_count<>before_reservation_count then
    raise exception 'H2_P5_REJECTION_MUTATED_DRAFT:%',jsonb_build_object(
      'rejected_count',rejected_count,
      'items_before',before_item_count,'items_after',after_item_count,
      'allocations_before',before_allocation_count,'allocations_after',after_allocation_count,
      'reservations_before',before_reservation_count,'reservations_after',after_reservation_count
    );
  end if;
  if current_setting('statement_timeout')::interval>'15 seconds'::interval
     or current_setting('lock_timeout')::interval>'1500 milliseconds'::interval
     or current_setting('idle_in_transaction_session_timeout')::interval>'30 seconds'::interval then
    raise exception 'H2_P5_BUDGET_RELAXATION:%',jsonb_build_object(
      'statement_timeout',current_setting('statement_timeout'),
      'lock_timeout',current_setting('lock_timeout'),
      'idle_in_transaction_session_timeout',current_setting('idle_in_transaction_session_timeout')
    );
  end if;
  raise notice 'H2_V8_SIGNED_RECOVERY_PASS=signed_positive_return';
  raise notice 'H2_V8_SIGNED_RECOVERY_PASS=signed_negative_recovery';
  raise notice 'H2_V8_SIGNED_RECOVERY_PASS=signed_mixed_ordinary_same_key';
  raise notice 'H2_V8_SIGNED_RECOVERY_REJECT=signed_tampered_or_incomplete|23514|PAY_BATCH_SIGNED_NON_CHARGE_RECOVERY_EVIDENCE_INVALID';
end;
$h2_p5_exact_assertions$;
`;
sql = replaceOnce(sql, '\nrollback;\n', `${assertions}\nrollback;\n`, 'terminal rollback');
if ((sql.match(/^\s*rollback\s*;\s*$/gim) ?? []).length !== 1 || !sql.endsWith('\n')) {
  throw new Error('Composed signed-recovery proof must contain exactly one terminal rollback');
}
if (!classifier.includes('BANKING_PAY_MIXED_SIGNED_RECOVERY_NOT_RECOGNISED')
    || !classifier.includes('BANKING_PAY_DUPLICATE_SIGNED_RECOVERY_ACCEPTED')) {
  throw new Error('Sealed classifier historical-shape verifier markers changed');
}
if (/generate_series\s*\(\s*1\s*,\s*50000\s*\)/i.test(sql)) {
  throw new Error('50,000-row materialisation is prohibited');
}

const version = spawnSync('docker', ['exec', container, 'psql', '-X', '-U', 'postgres', '-d', database, '-Atc', 'select version()'], {
  encoding: 'utf8', maxBuffer: 1024 * 1024
});
if (version.status !== 0) throw new Error(`Unable to read disposable PostgreSQL identity: ${version.stderr.trim()}`);
const engine = version.stdout.trim();
if (!/^PostgreSQL (17\.11|18\.6)\b/.test(engine)) throw new Error(`Unsupported disposable PostgreSQL engine: ${engine}`);
if (expectedEngine && !engine.startsWith(expectedEngine)) {
  throw new Error(`Disposable engine mismatch: expected ${expectedEngine}; observed ${engine}`);
}

const started = performance.now();
const execution = spawnSync('docker', [
  'exec','-i','-w','/repo/tests','-e','PGOPTIONS=-c jit=off',container,
  'psql','-X','-v','ON_ERROR_STOP=1','-U','postgres','-d',database
], { input: sql, encoding: 'utf8', maxBuffer: 64 * 1024 * 1024 });
const output = `${execution.stdout ?? ''}\n${execution.stderr ?? ''}`;
for (const row of caseContract.cases) {
  if (!output.includes(row.expected_marker)) {
    throw new Error(`${row.class_id} marker missing (exit ${execution.status}):\n${output.split(/\r?\n/).slice(-120).join('\n')}`);
  }
}
if (execution.status !== 0) {
  throw new Error(`Signed-recovery proof failed with exit ${execution.status}:\n${output.split(/\r?\n/).slice(-120).join('\n')}`);
}

process.stdout.write(`${JSON.stringify({
  contract: 'BANKING_PAY_DRAFT_V8_SIGNED_RECOVERY_RESULT_V1',
  status: 'PASS',engine,database,
  evidence_tier: 'CURRENT_V8_POLICY_OWNER_ONLY',
  v1_v8_typed_parity_status: 'OPEN_HISTORICAL_V1_HARNESS_NOT_EXECUTED',
  source_sha256: {
    [runtimePath]: sha256(runtime),[classifierPath]: sha256(classifier),
    [selectionPath]: sha256(selection),[casesPath]: sha256(caseText)
  },
  harness_isolation: {
    kind: 'EXACT_LOCAL_DATABASE_GUARD_SUBSTITUTION_ONLY',
    committed_fixtures_unchanged: true,pre_insert_collision_guard_preserved: true
  },
  unchanged_budgets: {
    statement_timeout_ms: 15000,lock_timeout_ms: 1500,
    idle_in_transaction_session_timeout_ms: 30000,jit: 'OFF'
  },
  case_count: caseContract.cases.length,pass_count: caseContract.cases.length,fail_count: 0,
  class_results: caseContract.cases.map(row => ({
    class_id: row.class_id,status: 'PASS',marker: row.expected_marker,
    proof_kind: row.class_id.startsWith('signed_two_') || row.class_id.startsWith('signed_tampered_')
      ? 'TYPED_FAIL_CLOSED_ZERO_WRITE'
      : 'FULL_TYPED_CURRENT_V8_POLICY_OWNER'
  })),
  elapsed_ms: Math.round((performance.now()-started)*1000)/1000,
  transaction_outcome: 'ROLLBACK',external_payment_actions: 0
}, null, 2)}\n`);
