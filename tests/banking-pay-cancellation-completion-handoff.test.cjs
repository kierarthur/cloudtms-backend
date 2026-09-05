const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), 'utf8').replace(/\r\n/g, '\n');
const historical = read('supabase/repeatable/04082026_1209_pay_payment_correction_process_chunk.sql');
const replacement = read('supabase/repeatable/04092026_2350_banking_pay_cancellation_completion_v1.sql');
const broker = read('broker/src/index.js');

const sha256 = (text) => crypto.createHash('sha256').update(text).digest('hex');

const oldHeader = [
  '-- CloudTMS Banking Pay cancellation — Stage 1 replacement.',
  '-- One public phase owner; every call advances exactly one bounded phase page.'
].join('\n');
const newHeader = [
  '-- CloudTMS Banking Pay cancellation completion authority.',
  '-- Exact additive replacement of pay_payment_correction_process_chunk.',
  '-- Financial cancellation remains frozen under Policy X. This owner only binds',
  '-- the existing post-commit Workbench authority into the fallback refresh and',
  '-- invokes the existing idempotent terminal cancellation-audit owner.'
].join('\n');
const oldFallback = [
  "          'changed_pay_batch_item_ids', v_refresh_pay_batch_item_ids,",
  "          'maximum_candidate_count', 100",
  '        )'
].join('\n');
const newFallback = [
  "          'changed_pay_batch_item_ids', v_refresh_pay_batch_item_ids,",
  "          'maximum_candidate_count', 100,",
  "          'defer_complex_enqueue', true,",
  "          'post_commit_authorities_v3', v_route_authorities_v3",
  '        )'
].join('\n');
const auditAnchor = [
  '    UPDATE public.banking_pay_operations AS refresh_operation',
  "    SET progress_json = COALESCE(refresh_operation.progress_json, '{}'::jsonb)"
].join('\n');
const terminalWholeBatchGuard = [
  "    IF v_requested_action = 'DRAFT_CANCEL'",
  '       AND v_refresh_has_more IS NOT TRUE',
  '       AND EXISTS (',
  '         SELECT 1',
  '         FROM public.pay_batches AS cancelled_batch',
  '         WHERE cancelled_batch.id = v_request.pay_batch_id',
  "           AND cancelled_batch.status = 'CANCELLED'",
  '       ) THEN'
].join('\n');
const auditInsertion = [
  '    -- DRAFT_CANCEL used to reach this established idempotent metadata/audit',
  '    -- owner only through the retired all-in-one wrapper. The current staged',
  '    -- route must complete the same durable contract before it reports COMPLETE.',
  terminalWholeBatchGuard,
  '      PERFORM public.pay_payment_cancel_finalise_metadata_v1(',
  '        v_request.pay_batch_id,',
  '        p_correction_request_id,',
  '        p_actor_user_id,',
  '        v_request.reason',
  '      );',
  '    END IF;',
  '',
  auditAnchor
].join('\n');

function expectedReplacement() {
  assert.equal(historical.split(oldHeader).length - 1, 1);
  assert.equal(historical.split(oldFallback).length - 1, 1);
  assert.equal(historical.split(auditAnchor).length - 1, 1);
  return historical
    .replace(oldHeader, newHeader)
    .replace(oldFallback, newFallback)
    .replace(auditAnchor, auditInsertion);
}

function completionViolations(source) {
  const required = [
    [newFallback, 'bounded committed-authority Workbench handoff'],
    [terminalWholeBatchGuard, 'Draft-only terminal whole-batch audit guard'],
    [[
      'PERFORM public.pay_payment_cancel_finalise_metadata_v1(',
      '        v_request.pay_batch_id,',
      '        p_correction_request_id,',
      '        p_actor_user_id,',
      '        v_request.reason',
      '      );'
    ].join('\n'), 'exact established audit-owner arguments']
  ];
  return required.filter(([needle]) => !source.includes(needle)).map(([, label]) => label);
}

test('historical owner remains byte-identical and replacement contains exactly the reviewed orchestration delta', () => {
  assert.equal(sha256(historical), '48e059cd9df6fbeafe3560c4408fb167117f5c0da1933f13f1174c6125a3ac2e');
  assert.equal(sha256(replacement), '60e5fc26fbd147991c16ada0aaf2a9691143ccf46aa6e3227dc78d823a7a7ce1');
  assert.equal(replacement.trimEnd(), expectedReplacement().trimEnd());
  assert.equal((replacement.match(/CREATE OR REPLACE FUNCTION /g) || []).length, 1);
  assert.equal((replacement.match(/CREATE OR REPLACE FUNCTION public\.pay_payment_correction_process_chunk\s*\(/g) || []).length, 1);
});

test('replacement preserves signature, limits, security metadata, owner and service-only ACL', () => {
  assert.match(replacement, /p_correction_request_id uuid DEFAULT NULL::uuid,[\s\S]*p_limit integer DEFAULT 50,[\s\S]*p_worker_id text DEFAULT NULL::text,[\s\S]*p_actor_user_id uuid DEFAULT NULL::uuid/);
  assert.match(replacement, /LANGUAGE plpgsql\s+VOLATILE\s+SECURITY DEFINER/);
  assert.match(replacement, /SET search_path TO pg_catalog, private, extensions, pg_temp/);
  assert.match(replacement, /SET statement_timeout TO '6000ms'/);
  assert.match(replacement, /SET lock_timeout TO '1000ms'/);
  assert.match(replacement, /ALTER FUNCTION public\.pay_payment_correction_process_chunk\(uuid,integer,text,uuid\) OWNER TO postgres/);
  assert.match(replacement, /REVOKE ALL ON FUNCTION public\.pay_payment_correction_process_chunk\(uuid,integer,text,uuid\) FROM PUBLIC/);
  assert.match(replacement, /REVOKE ALL ON FUNCTION public\.pay_payment_correction_process_chunk\(uuid,integer,text,uuid\) FROM anon/);
  assert.match(replacement, /REVOKE ALL ON FUNCTION public\.pay_payment_correction_process_chunk\(uuid,integer,text,uuid\) FROM authenticated/);
  assert.match(replacement, /GRANT EXECUTE ON FUNCTION public\.pay_payment_correction_process_chunk\(uuid,integer,text,uuid\) TO service_role/);
});

test('Workbench handoff consumes committed authority and preserves the existing bounded enqueue owner', () => {
  assert.deepEqual(completionViolations(replacement), []);
  assert.equal(replacement.split(newFallback).length - 1, 1);
  assert.equal(replacement.split(oldFallback).length - 1, 0);
  assert.match(replacement, /v_post_commit_authorities_v3:=private\.pay_workbench_correction_post_commit_authority_page_v1\([\s\S]*INTO v_route_authorities_v3[\s\S]*CANCELLATION_ROUTE_INPUT_V1/);
  assert.match(replacement, /public\.pay_workbench_patch_preview_after_batch_mutation_cancel_safe_v1\([\s\S]*'maximum_candidate_count', 100,[\s\S]*'defer_complex_enqueue', true,[\s\S]*'post_commit_authorities_v3', v_route_authorities_v3/);
});

test('terminal whole-Draft cancellation invokes the existing idempotent audit owner before COMPLETE', () => {
  const auditIndex = replacement.indexOf('PERFORM public.pay_payment_cancel_finalise_metadata_v1(');
  const operationCompleteIndex = replacement.indexOf('UPDATE public.banking_pay_operations AS refresh_operation', auditIndex);
  assert.ok(auditIndex > 0);
  assert.ok(operationCompleteIndex > auditIndex);
  assert.equal((replacement.match(/PERFORM public\.pay_payment_cancel_finalise_metadata_v1\(/g) || []).length, 1);
  assert.match(replacement.slice(auditIndex - 420, operationCompleteIndex), /DRAFT_CANCEL[\s\S]*v_refresh_has_more IS NOT TRUE[\s\S]*cancelled_batch\.status = 'CANCELLED'/);
});

test('current Worker uses the staged correction route and does not revive the retired all-in-one wrapper', () => {
  const handlerStart = broker.indexOf('async function handleBankingPayBatchCancelV1(');
  const handlerEnd = broker.indexOf('async function handleBankingPayPaymentStatusResolveV1(', handlerStart);
  const handler = broker.slice(handlerStart, handlerEnd);
  assert.ok(handlerStart > 0 && handlerEnd > handlerStart);
  assert.match(handler, /unwrapBankingPayCancellationRpc\(await sbRpc\(env, 'pay_batch_cancel'/);
  assert.match(handler, /startVerifiedDraftCancellationAfterPlanningV1/);
  assert.doesNotMatch(handler, /pay_payment_cancel_not_sent_and_recalculate_complete_v1/);
  assert.match(broker, /sbRpc\(env, 'pay_payment_correction_process_chunk'/);
});

test('each new completion boundary has an executable mutation killed by the validator', () => {
  const operators = [
    newFallback,
    terminalWholeBatchGuard,
    [
      'PERFORM public.pay_payment_cancel_finalise_metadata_v1(',
      '        v_request.pay_batch_id,',
      '        p_correction_request_id,',
      '        p_actor_user_id,',
      '        v_request.reason',
      '      );'
    ].join('\n')
  ];
  for (const operator of operators) {
    const mutated = replacement.replace(operator, `REMOVED_${crypto.randomUUID()}`);
    assert.notEqual(mutated, replacement, `mutation target missing: ${operator}`);
    assert.ok(completionViolations(mutated).length > 0, `mutation survived: ${operator}`);
  }
});
