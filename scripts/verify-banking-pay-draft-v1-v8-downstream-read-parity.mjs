import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';

const args = new Map();
for (let index = 2; index < process.argv.length; index += 2) {
  const key = process.argv[index];
  const value = process.argv[index + 1];
  if (!key?.startsWith('--') || !value) throw new Error(`Invalid argument at position ${index}`);
  args.set(key.slice(2), value);
}

const required = [
  'v1-container', 'v1-database', 'v1-operation',
  'v8-container', 'v8-database', 'v8-operation'
];
for (const key of required) {
  if (!args.get(key)) throw new Error(`--${key} is required`);
}
for (const key of ['v1-container', 'v8-container']) {
  if (!/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$/.test(args.get(key))) throw new Error(`Unsafe --${key}`);
}
for (const key of ['v1-database', 'v8-database']) {
  if (!/^[a-zA-Z_][a-zA-Z0-9_]{0,62}$/.test(args.get(key))) throw new Error(`Unsafe --${key}`);
}
for (const key of ['v1-operation', 'v8-operation']) {
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(args.get(key))) {
    throw new Error(`Unsafe --${key}`);
  }
}

const ACTOR_ID = '10000000-0000-4000-8000-000000000001';

function sqlFor(operationId) {
  return String.raw`
\set ON_ERROR_STOP on
\pset tuples_only on
\pset format unaligned
BEGIN;
SET LOCAL statement_timeout='15s';
SET LOCAL lock_timeout='1500ms';
SET LOCAL idle_in_transaction_session_timeout='30s';
SET LOCAL jit=off;

CREATE TEMP TABLE h2_route_batches ON COMMIT DROP AS
WITH operation_row AS (
  SELECT result_json
  FROM public.banking_pay_operations
  WHERE id='${operationId}'::uuid
    AND operation_type='DRAFT_CREATE'
    AND status='COMPLETE'
)
SELECT batch.id AS pay_batch_id, UPPER(BTRIM(batch.batch_kind_fixed)) AS pay_channel
FROM operation_row
CROSS JOIN LATERAL jsonb_array_elements_text(operation_row.result_json->'created_pay_batch_ids') batch_id(value)
JOIN public.pay_batches batch ON batch.id=batch_id.value::uuid;

DO $h2_assert_batches$
BEGIN
  IF (SELECT count(*) FROM h2_route_batches)<>2
     OR (SELECT count(*) FROM h2_route_batches WHERE pay_channel='PAYE')<>1
     OR (SELECT count(*) FROM h2_route_batches WHERE pay_channel='UMBRELLA')<>1 THEN
    RAISE EXCEPTION 'H2_DOWNSTREAM_MIXED_BATCH_SET_INVALID';
  END IF;
END;
$h2_assert_batches$;

CREATE TEMP TABLE h2_net_result(result_json jsonb) ON COMMIT DROP;
INSERT INTO h2_net_result(result_json)
SELECT public.pay_set_paye_net_manual(
  batch.pay_batch_id,
  (
    SELECT jsonb_agg(
      jsonb_build_object(
        'candidate_id', candidate.candidate_id::text,
        'net_amount', CASE WHEN right(candidate.candidate_id::text,1)='1' THEN '1.25' ELSE '1.50' END
      )
      ORDER BY candidate.candidate_id
    )
    FROM public.pay_batch_candidates candidate
    WHERE candidate.pay_batch_id=batch.pay_batch_id
  ),
  '${ACTOR_ID}'::uuid
)
FROM h2_route_batches batch
WHERE batch.pay_channel='PAYE';

WITH batch_rows AS (
  SELECT batch.pay_batch_id,batch.pay_channel,pb.status,pb.schedule_kind,
         pb.execution_commit_state,pb.cancelled_at_utc
  FROM h2_route_batches batch
  JOIN public.pay_batches pb ON pb.id=batch.pay_batch_id
),
candidate_rows AS (
  SELECT batch.pay_channel,candidate.candidate_id,
         (candidate.candidate_tms_ref IS NOT DISTINCT FROM source_candidate.tms_ref) candidate_tms_ref_matches_source,
         candidate.candidate_display_name,candidate.paye_state,
         candidate.awaiting_net_amount,round(candidate.gross_preview,2) gross_preview,
         round(candidate.net_bank_amount,2) net_bank_amount,
         round(candidate.debt_created,2) debt_created,
         round(candidate.loan_repayment_taken,2) loan_repayment_taken,
         round(candidate.overpayment_recovery_taken,2) overpayment_recovery_taken,
         candidate.settlement_status
  FROM h2_route_batches batch
  JOIN public.pay_batch_candidates candidate ON candidate.pay_batch_id=batch.pay_batch_id
  JOIN public.candidates source_candidate ON source_candidate.id=candidate.candidate_id
),
item_rows AS (
  SELECT batch.pay_channel,candidate.candidate_id,item.item_type,item.timesheet_id,
         item.segment_key,item.source_ref,item.description,
         round(item.amount_ex_vat,2) amount_ex_vat,
         round(item.amount_vat,2) amount_vat,
         round(item.amount_inc_vat,2) amount_inc_vat,
         item.paye_treatment,item.frozen_component_key_type,
         item.frozen_component_key_value,item.frozen_source_pay_method,
         item.frozen_target_pay_method,item.is_voided
  FROM h2_route_batches batch
  JOIN public.pay_batch_candidates candidate ON candidate.pay_batch_id=batch.pay_batch_id
  JOIN public.pay_batch_items item ON item.pay_batch_candidate_id=candidate.id
),
allocation_rows AS (
  SELECT batch.pay_channel,allocation.candidate_id,allocation.allocation_type,
         allocation.source_ref,
         (
           allocation.operation_source_key LIKE '${operationId}:allocation:%'
           AND allocation.operation_source_key ~ ':[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
         ) operation_source_key_is_operation_bound,
         round(allocation.allocated_amount,2) allocated_amount,
         allocation.status,
         (allocation.pay_batch_item_id IS NOT NULL) item_linked
  FROM h2_route_batches batch
  JOIN public.banking_pay_operation_candidate_allocation_rows allocation
    ON allocation.pay_batch_id=batch.pay_batch_id
),
snapshot_rows AS (
  SELECT batch.pay_channel,snapshot.candidate_id,snapshot.timesheet_id
  FROM h2_route_batches batch
  JOIN public.pay_batch_timesheet_snapshots snapshot ON snapshot.pay_batch_id=batch.pay_batch_id
),
breakdown_rows AS (
  SELECT batch.pay_channel,candidate.candidate_id,item.source_ref,
         breakdown.line_kind,breakdown.bucket_code,breakdown.unit_name,
         round(breakdown.units,4) units,round(breakdown.rate,4) rate,
         round(breakdown.amount_ex_vat,2) amount_ex_vat,
         round(breakdown.amount_vat,2) amount_vat,
         round(breakdown.amount_inc_vat,2) amount_inc_vat
  FROM h2_route_batches batch
  JOIN public.pay_batch_candidates candidate ON candidate.pay_batch_id=batch.pay_batch_id
  JOIN public.pay_batch_items item ON item.pay_batch_candidate_id=candidate.id
  JOIN public.pay_batch_item_breakdowns breakdown ON breakdown.pay_batch_item_id=item.id
),
projection_rows AS (
  SELECT batch.pay_channel,projection.candidate_id,projection.pay_channel projected_channel,
         projection.paye_net_required,projection.has_effective_paye_input,
         projection.effective_paye_net_input_source,
         round(projection.effective_paye_net_input_amount,2) effective_paye_net_input_amount,
         round(projection.final_frozen_bank_amount,2) final_frozen_bank_amount,
         projection.paye_net_classification,projection.is_paye_net_state_row,
         projection.is_positive_bank_payment,projection.payment_reference,
         projection.payee_name,regexp_replace(coalesce(projection.sort_code,''),'[^0-9]','','g') sort_code,
         regexp_replace(coalesce(projection.account_number,''),'[^0-9]','','g') account_number,
         round(projection.amount,2) amount,projection.currency,projection.rail_provider,
         projection.rail_env,projection.grouping_mode_used,projection.payee_entity_kind
  FROM h2_route_batches batch
  CROSS JOIN LATERAL public._pay_batch_bank_payment_projection_rows(batch.pay_batch_id,'ALL') projection
),
status_pages AS (
  SELECT batch.pay_channel,
         public.pay_batch_payment_status_page_v1(
           batch.pay_batch_id,'${ACTOR_ID}'::uuid,'{}'::jsonb,
           'CANDIDATE','ASC',100,NULL::jsonb
         ) result_json
  FROM h2_route_batches batch
),
status_rows AS (
  SELECT page.pay_channel,row_value->>'candidate_id' candidate_id,
         row_value->>'pay_channel' row_pay_channel,
         row_value->>'display_status' display_status,
         row_value->'available_actions' available_actions,
         (row_value->>'active_item_count')::integer active_item_count,
         (row_value->>'original_payment_amount_pence')::integer original_payment_amount_pence,
         (row_value->>'active_payment_amount_pence')::integer active_payment_amount_pence,
         (row_value->>'include_in_active_overview')::boolean include_in_active_overview,
         (row_value->>'include_in_active_paye_schedule')::boolean include_in_active_paye_schedule,
         (row_value->>'pre_provider_cancel_eligible')::boolean pre_provider_cancel_eligible,
         (row_value->>'complete_candidate_instruction_scope')::boolean complete_candidate_instruction_scope
  FROM status_pages page
  CROSS JOIN LATERAL jsonb_array_elements(page.result_json->'rows') row_value
),
summary_rows AS (
  SELECT batch.pay_channel,
         public.pay_batch_execution_summary_get(
           batch.pay_batch_id,'${ACTOR_ID}'::uuid,'LIGHT'
         ) result_json
  FROM h2_route_batches batch
),
csv_rows AS (
  SELECT batch.pay_channel,
         public.pay_bank_csv_export_summary_get(
           batch.pay_batch_id,'ALL','${ACTOR_ID}'::uuid
         ) result_json
  FROM h2_route_batches batch
)
SELECT '__H2_DOWNSTREAM_JSON__'||jsonb_build_object(
  'contract','BANKING_PAY_DRAFT_V1_V8_DOWNSTREAM_READ_PARITY_V1',
  'policy_change_count',0,
  'external_payment_action_count',0,
  'batch_rows',(SELECT jsonb_agg(jsonb_build_object(
    'pay_channel',pay_channel,'status',status,'schedule_kind',schedule_kind,
    'execution_commit_state',execution_commit_state,'cancelled',cancelled_at_utc IS NOT NULL
  ) ORDER BY pay_channel) FROM batch_rows),
  'candidate_rows',(SELECT jsonb_agg(to_jsonb(candidate_rows) ORDER BY pay_channel,candidate_id) FROM candidate_rows),
  'item_rows',(SELECT jsonb_agg(to_jsonb(item_rows) ORDER BY pay_channel,candidate_id,timesheet_id,source_ref) FROM item_rows),
  'allocation_rows',(SELECT jsonb_agg(to_jsonb(allocation_rows) ORDER BY pay_channel,candidate_id,source_ref) FROM allocation_rows),
  'snapshot_rows',(SELECT jsonb_agg(to_jsonb(snapshot_rows) ORDER BY pay_channel,candidate_id,timesheet_id) FROM snapshot_rows),
  'breakdown_rows',(SELECT jsonb_agg(to_jsonb(breakdown_rows) ORDER BY pay_channel,candidate_id,source_ref,line_kind,bucket_code) FROM breakdown_rows),
  'projection_rows',(SELECT jsonb_agg(to_jsonb(projection_rows) ORDER BY pay_channel,candidate_id) FROM projection_rows),
  'status_rows',(SELECT jsonb_agg(to_jsonb(status_rows) ORDER BY pay_channel,candidate_id) FROM status_rows),
  'status_page_summaries',(SELECT jsonb_agg(jsonb_build_object(
    'pay_channel',pay_channel,'ok',result_json->'ok','code',result_json->'code',
    'row_count',result_json->'row_count','total_matching_count',result_json->'total_matching_count',
    'eligible_matching_count',result_json->'eligible_matching_count','has_more',result_json->'has_more',
    'active_overview_amount_pence',result_json->'active_overview_amount_pence',
    'original_overview_amount_pence',result_json->'original_overview_amount_pence',
    'active_overview_candidate_count',result_json->'active_overview_candidate_count',
    'active_paye_schedule_line_count',result_json->'active_paye_schedule_line_count',
    'active_paye_schedule_amount_pence',result_json->'active_paye_schedule_amount_pence'
  ) ORDER BY pay_channel) FROM status_pages),
  'execution_summaries',(SELECT jsonb_agg(jsonb_build_object(
    'pay_channel',pay_channel,'ok',result_json->'ok','status',result_json->'status',
    'batch_status',result_json->'batch_status','batch_kind_fixed',result_json->'batch_kind_fixed',
    'candidate_count',result_json->'candidate_count','item_count',result_json->'item_count',
    'total_amount',result_json->'total_amount','current_bank_payment_total',result_json->'current_bank_payment_total',
    'current_bank_payment_row_count',result_json->'current_bank_payment_row_count',
    'paye_net_complete',result_json->'paye_net_complete',
    'global_missing_explicit_paye_input_count',result_json->'global_missing_explicit_paye_input_count',
    'global_explicit_zero_count',result_json->'global_explicit_zero_count',
    'global_positive_paye_count',result_json->'global_positive_paye_count',
    'global_positive_bank_payment_count',result_json->'global_positive_bank_payment_count',
    'global_positive_bank_payment_total',result_json->'global_positive_bank_payment_total',
    'global_invalid_payment_row_count',result_json->'global_invalid_payment_row_count',
    'has_positive_bank_payments',result_json->'has_positive_bank_payments',
    'no_bank_payment_execution_eligible',result_json->'no_bank_payment_execution_eligible',
    'execution_commit_state',result_json->'execution_commit_state',
    'rail_provider_snapshot',result_json->'rail_provider_snapshot',
    'rail_env_snapshot',result_json->'rail_env_snapshot',
    'transfer_count',result_json->'transfer_count'
  ) ORDER BY pay_channel) FROM summary_rows),
  'csv_summaries',(SELECT jsonb_agg(jsonb_build_object(
    'pay_channel',pay_channel,'ok',result_json->'ok','scope',result_json->'scope',
    'row_count',result_json->'row_count','total_amount',result_json->'total_amount',
    'pending_row_count',result_json->'pending_row_count',
    'pending_total_amount',result_json->'pending_total_amount',
    'stable_row_count',result_json->'stable_row_count','stable_total_amount',result_json->'stable_total_amount',
    'blocked_row_count',result_json->'blocked_row_count',
    'pre_execution_export',result_json->'pre_execution_export',
    'batch_status',result_json->'batch_status','execution_commit_state',result_json->'execution_commit_state'
  ) ORDER BY pay_channel) FROM csv_rows)
)::text;
ROLLBACK;
`;
}

function runRoute(label, container, database, operationId) {
  const result = spawnSync('docker', [
    'exec', '-i', '-e', 'PGOPTIONS=-c jit=off', container,
    'psql', '-X', '-U', 'postgres', '-d', database
  ], {
    input: sqlFor(operationId),
    encoding: 'utf8',
    maxBuffer: 64 * 1024 * 1024
  });
  if (result.status !== 0) {
    throw new Error(`${label} downstream read failed (${result.status}):\n${result.stderr || result.stdout}`);
  }
  const marker = '__H2_DOWNSTREAM_JSON__';
  const line = String(result.stdout || '').split(/\r?\n/).find(value => value.startsWith(marker));
  if (!line) throw new Error(`${label} downstream result marker missing:\n${result.stdout}\n${result.stderr}`);
  return JSON.parse(line.slice(marker.length));
}

function normalizeDeterministicFixtureNamespaces(value) {
  const namespaceMap = new Map([
    ['22000000-0000-4000-8000-', '20000000-0000-4000-8000-'],
    ['32000000-0000-0000-0000-', '30000000-0000-0000-0000-'],
    ['52000000-0000-0000-0000-', '50000000-0000-0000-0000-'],
    ['66000000-0000-0000-0000-', '60000000-0000-0000-0000-'],
    ['66100000-0000-0000-0000-', '61000000-0000-0000-0000-'],
    ['66200000-0000-0000-0000-', '62000000-0000-0000-0000-'],
    ['66300000-0000-0000-0000-', '63000000-0000-0000-0000-'],
    ['66400000-0000-0000-0000-', '64000000-0000-0000-0000-'],
    ['66500000-0000-0000-0000-', '65000000-0000-0000-0000-'],
    ['77000000-0000-0000-0000-', '70000000-0000-0000-0000-'],
    ['77200000-0000-0000-', '72000000-0000-0000-'],
    ['77300000-0000-0000-0000-', '73000000-0000-0000-0000-'],
    ['77400000-0000-0000-', '74000000-0000-0000-'],
    ['88000000-0000-0000-0000-', '80000000-0000-0000-0000-']
  ]);
  const visit = current => {
    if (Array.isArray(current)) return current.map(visit);
    if (current && typeof current === 'object') {
      return Object.fromEntries(Object.entries(current).map(([key, child]) => [key, visit(child)]));
    }
    if (typeof current !== 'string') return current;
    let normalized = current;
    for (const [isolatedNamespace, canonicalNamespace] of namespaceMap) {
      normalized = normalized.replaceAll(isolatedNamespace, canonicalNamespace);
    }
    return normalized;
  };
  return visit(value);
}

const v1 = normalizeDeterministicFixtureNamespaces(
  runRoute('V1', args.get('v1-container'), args.get('v1-database'), args.get('v1-operation'))
);
const v8 = normalizeDeterministicFixtureNamespaces(
  runRoute('V8', args.get('v8-container'), args.get('v8-database'), args.get('v8-operation'))
);
const v1Canonical = JSON.stringify(v1);
const v8Canonical = JSON.stringify(v8);
if (v1Canonical !== v8Canonical) {
  function firstDifference(left, right, path = '$') {
    if (Object.is(left, right)) return null;
    if (typeof left !== typeof right || left === null || right === null) {
      return { path, v1: left, v8: right };
    }
    if (Array.isArray(left) || Array.isArray(right)) {
      if (!Array.isArray(left) || !Array.isArray(right)) return { path, v1: left, v8: right };
      if (left.length !== right.length) return { path: `${path}.length`, v1: left.length, v8: right.length };
      for (let index = 0; index < left.length; index += 1) {
        const difference = firstDifference(left[index], right[index], `${path}[${index}]`);
        if (difference) return difference;
      }
      return { path, v1: left, v8: right };
    }
    if (typeof left === 'object') {
      const keys = [...new Set([...Object.keys(left), ...Object.keys(right)])].sort();
      for (const key of keys) {
        if (!Object.hasOwn(left, key) || !Object.hasOwn(right, key)) {
          return { path: `${path}.${key}`, v1: left[key], v8: right[key] };
        }
        const difference = firstDifference(left[key], right[key], `${path}.${key}`);
        if (difference) return difference;
      }
      return { path, v1: left, v8: right };
    }
    return { path, v1: left, v8: right };
  }
  throw new Error(`H2_V1_V8_DOWNSTREAM_READ_PARITY_MISMATCH:${JSON.stringify(firstDifference(v1, v8))}`);
}

const resultSha256 = createHash('sha256').update(v8Canonical, 'utf8').digest('hex');

process.stdout.write(`${JSON.stringify({
  contract: 'BANKING_PAY_DRAFT_V1_V8_DOWNSTREAM_READ_PARITY_RESULT_V1',
  status: 'PASS',
  policy_change_count: 0,
  external_payment_action_count: 0,
  compared_routes: ['HISTORICAL_V1_SOURCE_ORACLE', 'CURRENT_V8_LOCAL_CANDIDATE'],
  simultaneous_channels: ['PAYE', 'UMBRELLA'],
  gates_proved: [
    'PAYE_WORKSHEET_NET_ENTRY_AND_SAVED_SCALAR',
    'UMBRELLA_PAYEE_VAT_AND_CHANNEL',
    'CURRENT_PAYMENT_STATUS_ROWS_AMOUNTS_ACTIONS',
    'OVERVIEW_BENEFICIARY_AND_PAYMENT_TOTALS',
    'CSV_SETTLEMENT_PROJECTION_PRE_EXECUTION'
  ],
  normalized_result_sha256: resultSha256,
  compared_counts: {
    batches: v8.batch_rows?.length ?? 0,
    candidates: v8.candidate_rows?.length ?? 0,
    items: v8.item_rows?.length ?? 0,
    allocations: v8.allocation_rows?.length ?? 0,
    snapshots: v8.snapshot_rows?.length ?? 0,
    breakdowns: v8.breakdown_rows?.length ?? 0,
    bank_projections: v8.projection_rows?.length ?? 0,
    payment_status_rows: v8.status_rows?.length ?? 0
  }
}, null, 2)}\n`);
