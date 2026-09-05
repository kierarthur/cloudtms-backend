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
const database = args.get('database') ?? 'banking_modal_v2_test';
const expectedEngine = args.get('engine');
if (!container || !/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$/.test(container)) {
  throw new Error('A safe --container value is required');
}
if (!/^[a-zA-Z_][a-zA-Z0-9_]{0,62}$/.test(database)) {
  throw new Error('The --database value is invalid');
}
if (expectedEngine && !/^PostgreSQL (17\.11|18\.6)\b/.test(expectedEngine)) {
  throw new Error('--engine must start with PostgreSQL 17.11 or PostgreSQL 18.6');
}

const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), 'utf8');
const bridgePath = 'tests/03092026_1600_banking_pay_draft_v8_finance_category_runtime.sql';
const canonicalPath = 'tests/02092026_1042_banking_pay_draft_insert_items_finance_handoff_runtime_verification.sql';
const bridge = read(bridgePath);
const canonical = read(canonicalPath);
const bridgeMarker = '\\ir 02092026_1042_banking_pay_draft_insert_items_finance_handoff_runtime_verification.sql';
const canonicalMarker = '\\if true';
if (bridge.split(bridgeMarker).length !== 2 || canonical.split(canonicalMarker).length < 2) {
  throw new Error('The immutable finance fixture splice boundary changed');
}

const legacyScopeAssertion = /     or not exists \(\s*select 1\s*from public\.banking_pay_operation_candidate_scope as scope_row\s*cross join lateral jsonb_array_elements\(\s*scope_row\.selected_canonical_preview_lines_json\s*\) as selected\(line_json\)\s*where scope_row\.operation_id='43000000-0000-4000-8000-000000000100'\s*and selected\.line_json->>'finance_case_id'=selected_finance_row->>'finance_case_id'\s*and selected\.line_json->>'line_type'=selected_finance_row->>'item_type'\s*and selected\.line_json->>'row_key'=selected_finance_row->>'row_key'\s*\) then/s;
const normalizedScopeAssertion = `     or not exists (
       select 1
       from private.banking_pay_draft_frozen_constituent_payloads_v8 as payload
       where payload.operation_id='43000000-0000-4000-8000-000000000100'
         and payload.finance_case_id=(selected_finance_row->>'finance_case_id')::uuid
         and payload.payload_json->>'line_type'=selected_finance_row->>'item_type'
         and payload.payload_json->>'row_key'=selected_finance_row->>'row_key'
     ) then`;

const canonicalPrefix = canonical.split(canonicalMarker)[0];
const matches = canonicalPrefix.match(new RegExp(legacyScopeAssertion.source, 'gs')) ?? [];
if (matches.length !== 1) throw new Error(`Expected one legacy scope assertion, found ${matches.length}`);
let normalizedCanonicalPrefix = canonicalPrefix.replace(legacyScopeAssertion, normalizedScopeAssertion);

// The canonical producer requires the Timesheet settings authority to be
// frozen before collection.  Production freezes this at authorisation time;
// this rollback harness creates its own synthetic Timesheet rows, so it must
// perform the same existing-owner step rather than bypassing that gate.
const frozenSettingsAuthoritySql = String.raw`
insert into public.client_settings(
  id,client_id,effective_from,autoprocess_hr
) values (
  '43000000-0000-4000-8000-000000000021',
  '43000000-0000-4000-8000-000000000020','2026-01-01',false
);
do $h2_finance_matrix_freeze_settings_authority$
declare
  v_row record;
begin
  update public.timesheets_financials
  set processed_at_utc=coalesce(processed_at_utc,statement_timestamp())
  where client_id='43000000-0000-4000-8000-000000000020'
    and is_current=true;
  for v_row in
    select timesheet.timesheet_id
    from public.timesheets as timesheet
    join public.timesheets_financials as financial
      on financial.timesheet_id=timesheet.timesheet_id
     and financial.is_current=true
    where financial.client_id='43000000-0000-4000-8000-000000000020'
    order by timesheet.timesheet_id
  loop
    perform private._timesheet_settings_authority_frozen_v1(v_row.timesheet_id);
  end loop;
  if exists(
    select 1
    from public.timesheets as timesheet
    join public.timesheets_financials as financial
      on financial.timesheet_id=timesheet.timesheet_id
     and financial.is_current=true
    where financial.client_id='43000000-0000-4000-8000-000000000020'
      and timesheet.settings_authority_json='{}'::jsonb
  ) then
    raise exception 'H2_FINANCE_MATRIX_SETTINGS_AUTHORITY_NOT_FROZEN';
  end if;
end;
$h2_finance_matrix_freeze_settings_authority$;
`;
const settingsFreezeMarker = 'set constraints all immediate;';
if (normalizedCanonicalPrefix.split(settingsFreezeMarker).length !== 2) {
  throw new Error('The finance fixture settings-freeze insertion boundary changed');
}
normalizedCanonicalPrefix = normalizedCanonicalPrefix.replace(
  settingsFreezeMarker,
  `${frozenSettingsAuthoritySql}\n${settingsFreezeMarker}`
);

const suffix = String.raw`
DO $h2_v8_green_chain$
DECLARE
  v_result jsonb;
  v_partition_receipt text;
  v_phase text;
  v_unlinked integer;
  v_iteration integer;
  v_variant record;
  v_batch_id uuid;
  v_finance_item record;
  v_allocation record;
  v_active_item_count integer;
  v_finance_item_count integer;
  v_reservation_count integer;
  v_reserved_amount numeric;
  v_case_status text;
  v_payout_status text;
  v_expected_payout_status text;
  v_expected_umbrella_id uuid;
  v_expected_component_id uuid;
  v_timesheet_snapshot_count integer;
  v_item_breakdown_count integer;
BEGIN
  SELECT receipt_digest_sha256 INTO STRICT v_partition_receipt
  FROM private.banking_pay_draft_frozen_stage_receipts_v8
  WHERE operation_id='43000000-0000-4000-8000-000000000100'
    AND stage_kind='CERTIFICATE_PARTITION_REFS'
    AND stage_status='TERMINAL'
    AND has_more=false
  ORDER BY page_sequence DESC LIMIT 1;

  FOR v_iteration IN 1..100 LOOP
    v_result := public.banking_pay_draft_advance_bounded_v8(
      '43000000-0000-4000-8000-000000000100',
      'H2_V8_FINANCE_POLICY_MATRIX',
      v_partition_receipt
    );
    EXIT WHEN v_result->>'work_kind'='READY_FOR_TERMINAL_FINISH';
  END LOOP;

  SELECT phase INTO STRICT v_phase
  FROM public.banking_pay_operations
  WHERE id='43000000-0000-4000-8000-000000000100';
  SELECT count(*)::integer INTO v_unlinked
  FROM public.banking_pay_operation_candidate_allocation_rows
  WHERE operation_id='43000000-0000-4000-8000-000000000100'
    AND finance_case_id IS NOT NULL
    AND pay_batch_item_id IS NULL;

  IF v_result->>'work_kind'<>'READY_FOR_TERMINAL_FINISH'
     OR v_phase<>'POST_CREATE_REFRESH'
     OR v_unlinked<>0 THEN
    RAISE EXCEPTION 'H2_V8_FINANCE_POLICY_MATRIX_INCOMPLETE:%:%:%',
      v_phase,v_unlinked,v_result;
  END IF;

  SELECT variant.* INTO STRICT v_variant
  FROM pg_temp.h2_f013_canonical_variants AS variant;
  SELECT scope_row.pay_batch_id INTO STRICT v_batch_id
  FROM public.banking_pay_operation_candidate_scope AS scope_row
  WHERE scope_row.operation_id='43000000-0000-4000-8000-000000000100'
    AND scope_row.candidate_id=(
      '43000000-0000-4000-8000-'||lpad((10000+v_variant.ordinal)::text,12,'0')
    )::uuid;

  SELECT count(*)::integer,
         count(*) FILTER (WHERE item.finance_case_id IS NOT NULL)::integer
  INTO STRICT v_active_item_count,v_finance_item_count
  FROM public.pay_batch_items AS item
  JOIN public.pay_batch_candidates AS batch_candidate
    ON batch_candidate.id=item.pay_batch_candidate_id
  WHERE batch_candidate.pay_batch_id=v_batch_id
    AND COALESCE(item.is_voided,false)=false;

  SELECT item.* INTO STRICT v_finance_item
  FROM public.pay_batch_items AS item
  JOIN public.pay_batch_candidates AS batch_candidate
    ON batch_candidate.id=item.pay_batch_candidate_id
  WHERE batch_candidate.pay_batch_id=v_batch_id
    AND item.finance_case_id IS NOT NULL
    AND COALESCE(item.is_voided,false)=false;

  SELECT allocation.* INTO STRICT v_allocation
  FROM public.banking_pay_operation_candidate_allocation_rows AS allocation
  WHERE allocation.operation_id='43000000-0000-4000-8000-000000000100'
    AND allocation.finance_case_id=v_finance_item.finance_case_id;

  SELECT count(*)::integer,COALESCE(sum(reservation.reserved_amount),0)
  INTO STRICT v_reservation_count,v_reserved_amount
  FROM public.pay_advance_reservations AS reservation
  WHERE reservation.pay_batch_id=v_batch_id
    AND reservation.finance_case_id=v_finance_item.finance_case_id
    AND reservation.status IN ('RESERVED','COMMITTED');

  SELECT advance.status::text,advance.payout_status::text
  INTO STRICT v_case_status,v_payout_status
  FROM public.pay_advances AS advance
  WHERE advance.id=v_finance_item.finance_case_id;

  SELECT count(*)::integer INTO STRICT v_timesheet_snapshot_count
  FROM public.pay_batch_timesheet_snapshots AS snapshot
  WHERE snapshot.pay_batch_id=v_batch_id;
  SELECT count(*)::integer INTO STRICT v_item_breakdown_count
  FROM public.pay_batch_item_breakdowns AS breakdown
  JOIN public.pay_batch_items AS item ON item.id=breakdown.pay_batch_item_id
  JOIN public.pay_batch_candidates AS batch_candidate
    ON batch_candidate.id=item.pay_batch_candidate_id
  WHERE batch_candidate.pay_batch_id=v_batch_id;

  v_expected_umbrella_id:=CASE WHEN v_variant.pay_channel='UMBRELLA'
    THEN '41000000-0000-4000-8000-000000000010'::uuid ELSE NULL::uuid END;
  v_expected_component_id:=(
    '43000000-0000-4000-8000-'||lpad((80000+v_variant.ordinal)::text,12,'0')
  )::uuid;
  v_expected_payout_status:=CASE
    WHEN v_variant.visible_alias='PAYMENT_ADVANCE_REPAYMENT' THEN 'PAID'
    WHEN v_variant.visible_alias IN ('LOAN_PAYOUT','MANUAL_CREDIT_ADJUSTMENT_PAYMENT') THEN 'PENDING'
    ELSE NULL::text END;

  IF v_active_item_count<>2
     OR v_finance_item_count<>1
     OR v_finance_item.item_type<>v_variant.expected_final_item_type
     OR round(v_finance_item.amount_ex_vat,2)<>round(v_variant.expected_amount_ex_vat,2)
     OR round(v_finance_item.amount_vat,2)<>round(v_variant.expected_amount_vat,2)
     OR round(v_finance_item.amount_inc_vat,2)<>round(v_variant.expected_amount_inc_vat,2)
     OR v_finance_item.paye_treatment<>v_variant.expected_paye_treatment
     OR v_finance_item.pay_channel::text<>v_variant.pay_channel
     OR v_finance_item.umbrella_id IS DISTINCT FROM v_expected_umbrella_id
     OR v_finance_item.finance_component_id IS DISTINCT FROM v_expected_component_id
     OR v_allocation.status<>'ITEM_CREATED'
     OR v_allocation.pay_batch_item_id IS DISTINCT FROM v_finance_item.id
     OR round(v_allocation.allocated_amount,2)<>round(v_variant.expected_amount_ex_vat,2)
     OR v_reservation_count<>1
     OR round(v_reserved_amount,2)<>10.00
     OR v_case_status<>'ACTIVE'
     OR v_payout_status IS DISTINCT FROM v_expected_payout_status
     OR (v_finance_item.payout_instruction_snapshot_json->>'routing_kind') IS DISTINCT FROM v_variant.routing_kind
     OR (v_finance_item.payout_instruction_snapshot_json->>'pay_channel') IS DISTINCT FROM v_variant.pay_channel
     OR (v_finance_item.payout_instruction_snapshot_json->>'taxability') IS DISTINCT FROM v_variant.taxability
     OR v_timesheet_snapshot_count<>1
     OR v_item_breakdown_count<2 THEN
    RAISE EXCEPTION 'H2_V8_FINANCE_POLICY_ARTIFACT_PARITY_MISMATCH:%',
      pg_catalog.jsonb_build_object(
        'variant',to_jsonb(v_variant),
        'finance_item',to_jsonb(v_finance_item),
        'allocation',to_jsonb(v_allocation),
        'active_item_count',v_active_item_count,
        'finance_item_count',v_finance_item_count,
        'reservation_count',v_reservation_count,
        'reserved_amount',v_reserved_amount,
        'case_status',v_case_status,
        'payout_status',v_payout_status,
        'timesheet_snapshot_count',v_timesheet_snapshot_count,
        'item_breakdown_count',v_item_breakdown_count,
        'mismatch_flags',pg_catalog.jsonb_build_object(
          'active_item_count',v_active_item_count<>2,
          'finance_item_count',v_finance_item_count<>1,
          'item_type',v_finance_item.item_type IS DISTINCT FROM v_variant.expected_final_item_type,
          'amount_ex_vat',round(v_finance_item.amount_ex_vat,2) IS DISTINCT FROM round(v_variant.expected_amount_ex_vat,2),
          'amount_vat',round(v_finance_item.amount_vat,2) IS DISTINCT FROM round(v_variant.expected_amount_vat,2),
          'amount_inc_vat',round(v_finance_item.amount_inc_vat,2) IS DISTINCT FROM round(v_variant.expected_amount_inc_vat,2),
          'paye_treatment',v_finance_item.paye_treatment IS DISTINCT FROM v_variant.expected_paye_treatment,
          'pay_channel',v_finance_item.pay_channel::text IS DISTINCT FROM v_variant.pay_channel,
          'umbrella_id',v_finance_item.umbrella_id IS DISTINCT FROM v_expected_umbrella_id,
          'finance_component_id',v_finance_item.finance_component_id IS DISTINCT FROM v_expected_component_id,
          'allocation_status',v_allocation.status IS DISTINCT FROM 'ITEM_CREATED',
          'allocation_item_link',v_allocation.pay_batch_item_id IS DISTINCT FROM v_finance_item.id,
          'allocation_amount',round(v_allocation.allocated_amount,2) IS DISTINCT FROM round(v_variant.expected_amount_ex_vat,2),
          'reservation_count',v_reservation_count<>1,
          'reserved_amount',round(v_reserved_amount,2) IS DISTINCT FROM 10.00,
          'case_status',v_case_status IS DISTINCT FROM 'ACTIVE',
          'payout_status',v_payout_status IS DISTINCT FROM v_expected_payout_status,
          'payout_routing_kind',(v_finance_item.payout_instruction_snapshot_json->>'routing_kind') IS DISTINCT FROM v_variant.routing_kind,
          'payout_pay_channel',(v_finance_item.payout_instruction_snapshot_json->>'pay_channel') IS DISTINCT FROM v_variant.pay_channel,
          'payout_taxability',(v_finance_item.payout_instruction_snapshot_json->>'taxability') IS DISTINCT FROM v_variant.taxability,
          'timesheet_snapshot_count',v_timesheet_snapshot_count<>1,
          'item_breakdown_count',v_item_breakdown_count<2
        )
      );
  END IF;

  RAISE NOTICE 'H2_V8_FINANCE_POLICY_MATRIX_PASS=%|%|%|%|%|%',
    current_setting('cloudtms.h2_f013_variant_ordinal',true),v_phase,v_unlinked,
    v_active_item_count,v_timesheet_snapshot_count,v_item_breakdown_count;
END;
$h2_v8_green_chain$;
ROLLBACK;
`;

const sql = `${bridge.split(bridgeMarker)[0]}\n${normalizedCanonicalPrefix}\n${suffix}`;
if (!sql.endsWith('\n') || (sql.match(/\bROLLBACK\s*;/gi) ?? []).length !== 1) {
  throw new Error('Composed policy matrix must end in exactly one rollback');
}
if (/\b(?:provider|settlement|remittance)[a-z_]*\s*\(/i.test(suffix)) {
  throw new Error('Policy matrix suffix crossed a forbidden external action boundary');
}

const version = spawnSync('docker', [
  'exec', container, 'psql', '-X', '-U', 'postgres', '-d', database,
  '-Atc', 'select version()'
], { encoding: 'utf8', maxBuffer: 1024 * 1024 });
if (version.status !== 0) throw new Error(`Unable to read disposable PostgreSQL identity: ${version.stderr.trim()}`);
const engine = version.stdout.trim();
if (!/^PostgreSQL (17\.11|18\.6)\b/.test(engine)) {
  throw new Error(`Unsupported disposable PostgreSQL engine: ${engine}`);
}
if (expectedEngine && !engine.startsWith(expectedEngine)) {
  throw new Error(`Disposable engine mismatch: expected ${expectedEngine}; observed ${engine}`);
}

const results = [];
for (let ordinal = 1; ordinal <= 20; ordinal += 1) {
  const started = performance.now();
  const execution = spawnSync('docker', [
    'exec', '-i', '-w', '/repo/tests',
    '-e', `PGOPTIONS=-c cloudtms.h2_f013_variant_ordinal=${ordinal} -c jit=off`,
    container, 'psql', '-X', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', database
  ], { input: sql, encoding: 'utf8', maxBuffer: 32 * 1024 * 1024 });
  const output = `${execution.stdout ?? ''}\n${execution.stderr ?? ''}`;
  const marker = new RegExp(`H2_V8_FINANCE_POLICY_MATRIX_PASS=${ordinal}\\|POST_CREATE_REFRESH\\|0\\|2\\|1\\|[2-9][0-9]*`);
  if (execution.status !== 0 || !marker.test(output)) {
    const tail = output.split(/\r?\n/).slice(-80).join('\n');
    throw new Error(`Finance policy variant ${ordinal} failed with exit ${execution.status}:\n${tail}`);
  }
  results.push({ ordinal, status: 'PASS', elapsed_ms: Math.round((performance.now() - started) * 1000) / 1000 });
}

process.stdout.write(`${JSON.stringify({
  contract: 'BANKING_PAY_DRAFT_V8_FINANCE_POLICY_MATRIX_V1',
  engine,
  database,
  source_sha256: {
    [bridgePath]: createHash('sha256').update(bridge).digest('hex'),
    [canonicalPath]: createHash('sha256').update(canonical).digest('hex')
  },
  variant_count: results.length,
  pass_count: results.filter((result) => result.status === 'PASS').length,
  fail_count: 0,
  external_payment_actions: 0,
  transaction_outcome: 'ROLLBACK',
  results
}, null, 2)}\n`);
