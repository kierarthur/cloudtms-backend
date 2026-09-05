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

function sqlFor(draftOperationId) {
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
  WHERE id='${draftOperationId}'::uuid
    AND operation_type='DRAFT_CREATE'
    AND status='COMPLETE'
)
SELECT batch.id AS pay_batch_id, upper(btrim(batch.batch_kind_fixed)) AS pay_channel
FROM operation_row
CROSS JOIN LATERAL jsonb_array_elements_text(operation_row.result_json->'created_pay_batch_ids') batch_id(value)
JOIN public.pay_batches batch ON batch.id=batch_id.value::uuid;

DO $h2_assert_batches$
BEGIN
  IF (SELECT count(*) FROM h2_route_batches)<>2
     OR (SELECT count(*) FROM h2_route_batches WHERE pay_channel='PAYE')<>1
     OR (SELECT count(*) FROM h2_route_batches WHERE pay_channel='UMBRELLA')<>1 THEN
    RAISE EXCEPTION 'H2_EXECUTE_PREPARATION_MIXED_BATCH_SET_INVALID';
  END IF;
  IF EXISTS (
    SELECT 1
    FROM public.pay_bank_transfers transfer
    JOIN h2_route_batches batch ON batch.pay_batch_id=transfer.pay_batch_id
  ) THEN
    RAISE EXCEPTION 'H2_EXECUTE_PREPARATION_REQUIRES_UNTOUCHED_DRAFT';
  END IF;
END;
$h2_assert_batches$;

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

CREATE TEMP TABLE h2_execute_operations(
  pay_batch_id uuid PRIMARY KEY,
  pay_channel text NOT NULL,
  operation_id uuid NOT NULL UNIQUE,
  prepare_result jsonb NOT NULL,
  replay_result jsonb NOT NULL
) ON COMMIT DROP;

DO $h2_execute$
DECLARE
  v_actor uuid := '${ACTOR_ID}'::uuid;
  v_batch record;
  v_operation uuid;
  v_scope record;
  v_cursor jsonb;
  v_result jsonb;
  v_first_result jsonb;
BEGIN
  FOR v_batch IN SELECT * FROM h2_route_batches ORDER BY pay_channel LOOP
    SELECT started.operation_id
    INTO v_operation
    FROM public.banking_pay_operation_start(
      'PAYMENT_EXECUTE',
      v_actor,
      'h2-v1-v8-execute-preparation:' || gen_random_uuid()::text,
      NULL::uuid,
      v_batch.pay_batch_id,
      NULL::uuid,
      jsonb_build_object(
        'execution_mode','STANDARD_BANK',
        'pay_channel_scope','ALL',
        'schedule_kind','IMMEDIATE',
        'payment_date',current_date::text,
        'source','H2_ROLLBACK_ONLY_EXECUTE_PREPARATION'
      ),
      '{}'::jsonb
    ) AS started;

    UPDATE public.banking_pay_operations
    SET phase='FRESHNESS_SCOPE_SEED', updated_at_utc=now()
    WHERE id=v_operation;
    v_result := public.pay_batch_freshness_scope_seed(v_operation,v_batch.pay_batch_id,NULL::jsonb,100);
    IF v_result->>'ok' IS DISTINCT FROM 'true' THEN
      RAISE EXCEPTION 'H2_FRESHNESS_SCOPE_SEED_FAILED: %', v_result;
    END IF;

    UPDATE public.banking_pay_operations
    SET phase='FRESHNESS_VALIDATE', updated_at_utc=now()
    WHERE id=v_operation;
    v_cursor := NULL::jsonb;
    LOOP
      v_result := public.pay_batch_validate_freshness_chunk(v_operation,v_cursor,100);
      IF v_result->>'ok' IS DISTINCT FROM 'true' THEN
        RAISE EXCEPTION 'H2_FRESHNESS_VALIDATE_FAILED: %', v_result;
      END IF;
      EXIT WHEN coalesce((v_result->>'has_more')::boolean,false) IS NOT TRUE;
      v_cursor := v_result->'next_cursor';
      IF v_cursor IS NULL THEN
        RAISE EXCEPTION 'H2_FRESHNESS_VALIDATE_CURSOR_MISSING';
      END IF;
    END LOOP;

    UPDATE public.banking_pay_operations
    SET phase='FRESHNESS_RESULT', updated_at_utc=now()
    WHERE id=v_operation;
    v_result := public.pay_batch_freshness_result_get(v_operation,v_batch.pay_batch_id,v_actor);
    IF v_result->>'ok' IS DISTINCT FROM 'true'
       OR upper(coalesce(v_result->>'status','')) NOT IN ('PASSED','FRESH')
       OR coalesce((v_result->>'stale_count')::integer,0)<>0 THEN
      RAISE EXCEPTION 'H2_FRESHNESS_RESULT_FAILED: %', v_result;
    END IF;

    UPDATE public.banking_pay_operations
    SET phase='TRANSFER_SCOPE_SEED', updated_at_utc=now()
    WHERE id=v_operation;
    v_result := public.pay_execute_bank_transfer_scope_seed(v_operation,v_batch.pay_batch_id,'ALL',v_actor,false);
    IF v_result->>'ok' IS DISTINCT FROM 'true'
       OR coalesce((v_result->>'transfer_scope_count')::integer,0)<=0 THEN
      RAISE EXCEPTION 'H2_TRANSFER_SCOPE_SEED_FAILED: %', v_result;
    END IF;

    FOR v_scope IN
      SELECT id
      FROM public.banking_pay_operation_transfer_scope
      WHERE operation_id=v_operation
        AND pay_batch_id=v_batch.pay_batch_id
      ORDER BY id
    LOOP
      UPDATE public.banking_pay_operations
      SET phase='SEED_TRANSFER_SCOPE_ITEMS', updated_at_utc=now()
      WHERE id=v_operation;
      v_cursor := NULL::jsonb;
      LOOP
        v_result := public.pay_execute_bank_transfer_scope_items_seed_chunk(v_operation,v_scope.id,v_cursor,100,v_actor);
        IF v_result->>'ok' IS DISTINCT FROM 'true' THEN
          RAISE EXCEPTION 'H2_TRANSFER_SCOPE_ITEMS_SEED_FAILED: %', v_result;
        END IF;
        EXIT WHEN coalesce((v_result->>'has_more')::boolean,false) IS NOT TRUE;
        v_cursor := v_result->'next_cursor';
        IF v_cursor IS NULL THEN
          RAISE EXCEPTION 'H2_TRANSFER_SCOPE_ITEMS_CURSOR_MISSING';
        END IF;
      END LOOP;

      UPDATE public.banking_pay_operations
      SET phase='ROLLUP_TRANSFER_SCOPE_ITEMS', updated_at_utc=now()
      WHERE id=v_operation;
      v_cursor := NULL::jsonb;
      LOOP
        v_result := public.pay_execute_bank_transfer_scope_rollup_chunk(v_operation,v_scope.id,v_cursor,100,v_actor);
        IF v_result->>'ok' IS DISTINCT FROM 'true' THEN
          RAISE EXCEPTION 'H2_TRANSFER_SCOPE_ROLLUP_FAILED: %', v_result;
        END IF;
        EXIT WHEN coalesce((v_result->>'has_more')::boolean,false) IS NOT TRUE;
        v_cursor := v_result->'next_cursor';
        IF v_cursor IS NULL THEN
          RAISE EXCEPTION 'H2_TRANSFER_SCOPE_ROLLUP_CURSOR_MISSING';
        END IF;
      END LOOP;
    END LOOP;

    UPDATE public.banking_pay_operations
    SET phase='PREPARE_TRANSFER_CHUNKS', updated_at_utc=now()
    WHERE id=v_operation;
    v_result := public.pay_execute_bank_transfer_chunk_prepare(v_operation,v_batch.pay_batch_id,NULL::jsonb,v_actor);
    IF v_result->>'ok' IS DISTINCT FROM 'true'
       OR coalesce((v_result->>'failed_count')::integer,0)<>0
       OR coalesce((v_result->>'remaining_count')::integer,0)<>0
       OR coalesce((v_result->>'has_more')::boolean,false) IS TRUE
       OR upper(coalesce(v_result->>'next_required_phase',''))<>'PROVIDER_SUBMIT_CLAIM' THEN
      RAISE EXCEPTION 'H2_TRANSFER_PREPARE_FAILED: %', v_result;
    END IF;

    v_first_result := v_result;
    v_result := public.pay_execute_bank_transfer_chunk_prepare(v_operation,v_batch.pay_batch_id,NULL::jsonb,v_actor);
    IF v_result->>'ok' IS DISTINCT FROM 'true'
       OR coalesce((v_result->>'prepared_count')::integer,0)<>0
       OR coalesce((v_result->>'failed_count')::integer,0)<>0
       OR coalesce((v_result->>'remaining_count')::integer,0)<>0
       OR coalesce((v_result->>'has_more')::boolean,false) IS TRUE
       OR upper(coalesce(v_result->>'next_required_phase',''))<>'PROVIDER_SUBMIT_CLAIM' THEN
      RAISE EXCEPTION 'H2_TRANSFER_PREPARE_RESPONSE_LOSS_REPLAY_FAILED: %', v_result;
    END IF;

    INSERT INTO h2_execute_operations(pay_batch_id,pay_channel,operation_id,prepare_result,replay_result)
    VALUES(v_batch.pay_batch_id,v_batch.pay_channel,v_operation,v_first_result,v_result);
  END LOOP;
END;
$h2_execute$;

WITH prepare_rows AS (
  SELECT operation.pay_channel,
         jsonb_build_object(
           'ok',operation.prepare_result->'ok',
           'has_more',operation.prepare_result->'has_more',
           'prepared_count',operation.prepare_result->'prepared_count',
           'reused_count',operation.prepare_result->'reused_count',
           'failed_count',operation.prepare_result->'failed_count',
           'remaining_count',operation.prepare_result->'remaining_count',
           'execution_mode',operation.prepare_result->'execution_mode',
           'provider_submission_required',operation.prepare_result->'provider_submission_required',
           'provider_submission_attempted',operation.prepare_result->'provider_submission_attempted',
           'submitted_to_bank',operation.prepare_result->'submitted_to_bank',
           'next_required_phase',operation.prepare_result->'next_required_phase',
           'item_transfer_linked_count',operation.prepare_result->'item_transfer_linked_count',
           'item_transfer_reused_count',operation.prepare_result->'item_transfer_reused_count',
           'item_transfer_conflict_count',operation.prepare_result->'item_transfer_conflict_count'
         ) AS result_json,
         jsonb_build_object(
           'ok',operation.replay_result->'ok',
           'has_more',operation.replay_result->'has_more',
           'prepared_count',operation.replay_result->'prepared_count',
           'reused_count',operation.replay_result->'reused_count',
           'failed_count',operation.replay_result->'failed_count',
           'remaining_count',operation.replay_result->'remaining_count',
           'execution_mode',operation.replay_result->'execution_mode',
           'provider_submission_required',operation.replay_result->'provider_submission_required',
           'provider_submission_attempted',operation.replay_result->'provider_submission_attempted',
           'submitted_to_bank',operation.replay_result->'submitted_to_bank',
           'next_required_phase',operation.replay_result->'next_required_phase',
           'item_transfer_linked_count',operation.replay_result->'item_transfer_linked_count',
           'item_transfer_reused_count',operation.replay_result->'item_transfer_reused_count',
           'item_transfer_conflict_count',operation.replay_result->'item_transfer_conflict_count'
         ) AS replay_json
  FROM h2_execute_operations operation
),
scope_rows AS (
  SELECT operation.pay_channel AS batch_channel,
         scope.pay_channel,
         scope.transfer_group_key,
         scope.candidate_id,
         scope.umbrella_id,
         scope.payee_entity_kind,
         scope.payee_entity_id,
         scope.currency,
         round(scope.amount,2) amount,
         scope.payment_reference,
         scope.payee_name,
         regexp_replace(coalesce(scope.sort_code,''),'[^0-9]','','g') sort_code,
         regexp_replace(coalesce(scope.account_number,''),'[^0-9]','','g') account_number,
         scope.account_type,
         scope.bank_details_hash_snapshot,
         scope.grouping_mode_used,
         scope.week_ending_bucket,
         scope.status,
         scope.provider_submit_ready,
         scope.provider_submit_state,
         scope.provider_review_required,
         scope.provider_unsafe_reason,
         scope.prepared_item_count,
         round(scope.prepared_amount_total,2) prepared_amount_total,
         nullif(btrim(coalesce(scope.prepared_scope_hash,'')),'') IS NOT NULL prepared_scope_hash_present,
         nullif(btrim(coalesce(scope.prepared_result_hash,'')),'') IS NOT NULL prepared_result_hash_present
  FROM h2_execute_operations operation
  JOIN public.banking_pay_operation_transfer_scope scope ON scope.operation_id=operation.operation_id
),
scope_item_rows AS (
  SELECT operation.pay_channel AS batch_channel,
         scope.pay_channel,
         member.candidate_id,
         item.item_type,
         item.timesheet_id,
         item.segment_key,
         item.source_ref,
         member.item_status,
         member.item_ordinal>0 item_ordinal_valid,
         member.rollup_status
  FROM h2_execute_operations operation
  JOIN public.banking_pay_operation_transfer_scope scope ON scope.operation_id=operation.operation_id
  JOIN public.banking_pay_operation_transfer_scope_items member ON member.transfer_scope_id=scope.id
  JOIN public.pay_batch_items item ON item.id=member.pay_batch_item_id
),
scope_item_total_rows AS (
  SELECT operation.pay_channel AS batch_channel,
         scope.pay_channel,
         member.candidate_id,
         count(*)::integer item_count,
         round(sum(member.item_amount),2) item_amount_total
  FROM h2_execute_operations operation
  JOIN public.banking_pay_operation_transfer_scope scope ON scope.operation_id=operation.operation_id
  JOIN public.banking_pay_operation_transfer_scope_items member ON member.transfer_scope_id=scope.id
  GROUP BY operation.pay_channel,scope.pay_channel,member.candidate_id
),
transfer_rows AS (
  SELECT operation.pay_channel AS batch_channel,
         transfer.pay_channel,
         transfer.candidate_id,
         transfer.umbrella_id,
         transfer.payee_entity_kind,
         transfer.payee_entity_id,
         transfer.transfer_group_key,
         transfer.grouping_mode_used,
         transfer.week_ending_bucket,
         round(transfer.amount,2) amount,
         transfer.currency,
         transfer.status,
         transfer.payment_reference,
         transfer.payee_name,
         regexp_replace(coalesce(transfer.sort_code,''),'[^0-9]','','g') sort_code,
         regexp_replace(coalesce(transfer.account_number,''),'[^0-9]','','g') account_number,
         transfer.account_type,
         transfer.bank_details_hash_snapshot,
         transfer.rail_provider,
         transfer.rail_env,
         (
           transfer.request_id LIKE 'op:' || operation.operation_id::text || ':scope:%'
           AND transfer.request_id ~ ':scope:[0-9a-f]{32}$'
         ) request_id_is_operation_bound,
         transfer.rail_tx_id,
         transfer.rail_state,
         transfer.completed_at_utc IS NOT NULL completed,
         transfer.failed_reason
  FROM h2_execute_operations operation
  JOIN public.pay_bank_transfers transfer ON transfer.pay_batch_id=operation.pay_batch_id
),
item_link_rows AS (
  SELECT operation.pay_channel AS batch_channel,
         candidate.candidate_id,
         item.item_type,
         item.timesheet_id,
         item.segment_key,
         item.source_ref,
         round(item.amount_ex_vat,2) amount_ex_vat,
         transfer.pay_channel AS transfer_pay_channel,
         transfer.transfer_group_key,
         round(transfer.amount,2) transfer_amount
  FROM h2_execute_operations operation
  JOIN public.pay_batch_candidates candidate ON candidate.pay_batch_id=operation.pay_batch_id
  JOIN public.pay_batch_items item ON item.pay_batch_candidate_id=candidate.id
  JOIN public.pay_bank_transfers transfer ON transfer.id=item.pay_bank_transfer_id
  WHERE item.is_voided IS NOT TRUE
),
provider_counts AS (
  SELECT operation.pay_channel,
         (SELECT count(*) FROM public.banking_pay_operation_provider_attempts attempt WHERE attempt.operation_id=operation.operation_id) provider_attempt_count,
         (SELECT count(*) FROM public.pay_bank_transfer_events event WHERE event.pay_batch_id=operation.pay_batch_id) provider_event_count,
         (SELECT count(*) FROM public.banking_pay_operation_chunks chunk WHERE chunk.operation_id=operation.operation_id AND upper(coalesce(chunk.chunk_type,''))='TRANSFER_SUBMIT') provider_submit_chunk_count
  FROM h2_execute_operations operation
)
SELECT '__H2_EXECUTE_PREPARATION_JSON__'||jsonb_build_object(
  'contract','BANKING_PAY_DRAFT_V1_V8_EXECUTE_PREPARATION_PARITY_V1',
  'policy_change_count',0,
  'external_payment_action_count',0,
  'prepare_rows',(SELECT jsonb_agg(jsonb_build_object('pay_channel',pay_channel,'result',result_json,'response_loss_replay',replay_json) ORDER BY pay_channel) FROM prepare_rows),
  'scope_rows',(SELECT jsonb_agg(to_jsonb(scope_rows) ORDER BY batch_channel,pay_channel,candidate_id,umbrella_id,transfer_group_key) FROM scope_rows),
  'scope_item_rows',(SELECT jsonb_agg(to_jsonb(scope_item_rows) ORDER BY batch_channel,pay_channel,candidate_id,timesheet_id,source_ref) FROM scope_item_rows),
  'scope_item_total_rows',(SELECT jsonb_agg(to_jsonb(scope_item_total_rows) ORDER BY batch_channel,pay_channel,candidate_id) FROM scope_item_total_rows),
  'transfer_rows',(SELECT jsonb_agg(to_jsonb(transfer_rows) ORDER BY batch_channel,pay_channel,candidate_id,umbrella_id,transfer_group_key) FROM transfer_rows),
  'item_link_rows',(SELECT jsonb_agg(to_jsonb(item_link_rows) ORDER BY batch_channel,candidate_id,timesheet_id,source_ref) FROM item_link_rows),
  'provider_counts',(SELECT jsonb_agg(to_jsonb(provider_counts) ORDER BY pay_channel) FROM provider_counts)
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
    throw new Error(`${label} execute preparation failed (${result.status}):\n${result.stderr || result.stdout}`);
  }
  const marker = '__H2_EXECUTE_PREPARATION_JSON__';
  const line = String(result.stdout || '').split(/\r?\n/).find(value => value.startsWith(marker));
  if (!line) throw new Error(`${label} execute preparation marker missing:\n${result.stdout}\n${result.stderr}`);
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

function firstDifference(left, right, path = '$') {
  if (Object.is(left, right)) return null;
  if (Array.isArray(left) && Array.isArray(right)) {
    if (left.length !== right.length) return { path, left: `array(${left.length})`, right: `array(${right.length})` };
    for (let index = 0; index < left.length; index += 1) {
      const difference = firstDifference(left[index], right[index], `${path}[${index}]`);
      if (difference) return difference;
    }
    return null;
  }
  if (left && right && typeof left === 'object' && typeof right === 'object') {
    const keys = [...new Set([...Object.keys(left), ...Object.keys(right)])].sort();
    for (const key of keys) {
      if (!(key in left) || !(key in right)) return { path: `${path}.${key}`, left: left[key], right: right[key] };
      const difference = firstDifference(left[key], right[key], `${path}.${key}`);
      if (difference) return difference;
    }
    return null;
  }
  return { path, left, right };
}

const v1 = normalizeDeterministicFixtureNamespaces(
  runRoute('V1', args.get('v1-container'), args.get('v1-database'), args.get('v1-operation'))
);
const v8 = normalizeDeterministicFixtureNamespaces(
  runRoute('V8', args.get('v8-container'), args.get('v8-database'), args.get('v8-operation'))
);
const difference = firstDifference(v1, v8);
if (difference) {
  throw new Error(`V1/V8 execute-preparation parity mismatch at ${difference.path}: ${JSON.stringify(difference.left)} !== ${JSON.stringify(difference.right)}`);
}

for (const entry of v8.provider_counts || []) {
  if (Number(entry.provider_attempt_count) !== 0 || Number(entry.provider_event_count) !== 0 || Number(entry.provider_submit_chunk_count) !== 0) {
    throw new Error(`Provider/payment boundary was crossed for ${entry.pay_channel}: ${JSON.stringify(entry)}`);
  }
}

const counts = {
  batches: v8.prepare_rows?.length || 0,
  scopes: v8.scope_rows?.length || 0,
  scope_items: v8.scope_item_rows?.length || 0,
  transfers: v8.transfer_rows?.length || 0,
  linked_items: v8.item_link_rows?.length || 0
};
const fingerprint = createHash('sha256').update(JSON.stringify(v8)).digest('hex');
console.log(JSON.stringify({
  ok: true,
  contract: v8.contract,
  fingerprint_sha256: fingerprint,
  counts,
  provider_boundary_crossed: false,
  transaction_rolled_back: true,
  statement_timeout_ms: 15000,
  lock_timeout_ms: 1500,
  idle_in_transaction_session_timeout_ms: 30000
}, null, 2));
