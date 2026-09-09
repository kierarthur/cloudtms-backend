import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (...parts) => readFileSync(path.join(root, ...parts), 'utf8');
const migration = read('supabase', 'migrations', '07092026_2300_banking_pay_payment_cancellation_mail_outbox_v1.sql');
const owner = read('supabase', 'repeatable', '07092026_2301_banking_pay_payment_cancellation_notice_reconcile_v1.sql');
const verification = read('supabase', 'verification', '07092026_2302_banking_pay_payment_cancellation_notice_verification.sql');
const fixture = JSON.parse(read('tests', 'fixtures', '07092026_2303_banking_pay_payment_cancellation_notice_v1.json'));
const worker = read('broker', 'src', 'index.js');
const monolith = read('supabase', 'repeatable', '26052026_2100HRS_NEW_FUNCTIONS.sql');
const mailClaim = read('supabase', 'repeatable', '23072026_2207_email_outbox_claim_ready_batch.sql');
const manualRetry = read('supabase', 'repeatable', '04032026_mailshots.sql');

const count = (source, pattern) => (source.match(pattern) || []).length;

function workerHelpers(stubs = {}) {
  const start = worker.indexOf('function unwrapPaymentCancellationNoticeResult');
  const end = worker.indexOf('\nasync function advancePaymentCorrectionOperation', start);
  assert.ok(start >= 0 && end > start, 'cancellation-notice Worker helpers must be extractable');
  const warnings = [];
  const context = {
    sbRpc: stubs.sbRpc || (async () => ({})),
    console: { warn(value) { warnings.push(String(value)); } },
    Object,
    Array,
    String,
    Number,
    Math,
    Date,
    Error,
    Set,
    JSON
  };
  const helpers = vm.runInNewContext(
    `${worker.slice(start, end)}\n({` +
      'unwrapPaymentCancellationNoticeResult,' +
      'validatePaymentCancellationNoticePage,' +
      'reconcilePaymentCancellationNoticesBounded,' +
      'observePaymentCancellationNoticeReconcile,' +
      'schedulePaymentCancellationNoticeNudge' +
    '})',
    context,
    { filename: 'payment-cancellation-notice-worker-helpers.js' }
  );
  return { helpers, warnings };
}

function page(overrides = {}) {
  return {
    ok: true,
    mode: 'RECOVERY',
    template_version: 'PAYMENT_CANCELLATION_NOTICE_V1',
    examined: 1,
    eligible: 1,
    queued: 1,
    already_present: 0,
    skipped: 0,
    reason_counts: {},
    has_more: false,
    recovery_claim_contended: false,
    next_cursor: null,
    progress_owner: 'SERVER_ROW',
    ...overrides
  };
}

test('migration admits only the new mail type and installs exact event indexes in safe lock order', () => {
  const requestLock = migration.indexOf('lock table only public.pay_payment_correction_requests');
  const mailLock = migration.indexOf('lock table only public.mail_outbox');
  assert.ok(requestLock >= 0 && mailLock > requestLock, 'cutover must lock request before mail');
  assert.match(migration, /v_expected_preimage constant text/);
  assert.match(migration, /v_expected_postimage constant text/);
  assert.match(migration, /convalidated/);
  assert.equal(count(migration, /lock table only public\.mail_outbox/gi), 1);
  assert.equal(count(migration, /lock table only public\.pay_payment_correction_requests/gi), 1);
  assert.match(migration, /mail_outbox_payment_cancellation_sent_source_idx[\s\S]+\(context_id, created_at_utc, id\)/i);
  assert.match(migration, /mail_outbox_payment_cancellation_sent_candidate_source_idx[\s\S]+payment_scope_json->>'candidate_id'[\s\S]+payment_scope_json->>'pay_batch_candidate_id'/i);
  assert.match(migration, /mail_outbox_payment_cancellation_pending_idx[\s\S]+cancel_notice_tracked is true/i);
  assert.match(migration, /coalesce\(cancel_notice_next_attempt_at_utc, sent_at\)/i);
  assert.match(migration, /pay_payment_cancellation_notice_recovery_request_idx[\s\S]+cancel_notice_tracked is true[\s\S]+APPLIED_WITH_BLOCKERS/i);
  assert.match(migration, /coalesce\(cancel_notice_next_attempt_at_utc, applied_at_utc\)/i);
  assert.match(migration, /alter column cancel_notice_tracked set default true/i);
  assert.match(migration, /PAYMENT_CANCELLATION_NOTICE_CUTOVER_ACTIVE_REQUESTS/);
  assert.doesNotMatch(migration, /alter table public\.(pay_batches|pay_batch_items|pay_bank_transfers|pay_advances)/i);
});

test('reconciler has the exact service-only, fixed-budget and diagnostic-only boundary', () => {
  assert.equal(count(owner, /create or replace function public\.pay_payment_cancellation_notice_reconcile_v1/gi), 1);
  assert.match(owner, /security definer[\s\S]+set search_path to ''[\s\S]+set statement_timeout to '6000ms'[\s\S]+set lock_timeout to '1000ms'/i);
  assert.match(owner, /CREATE OR REPLACE preserves a function's existing ACL/);
  assert.match(owner, /pg_catalog\.aclexplode[\s\S]+acl_row\.grantee not in \([\s\S]+v_function_owner_oid,[\s\S]+v_service_role_oid/);
  assert.match(owner, /revoke all on function %s from public cascade/);
  assert.match(owner, /revoke all on function %s from %I cascade/);
  assert.match(owner, /acl_row\.is_grantable is true[\s\S]+revoke grant option for execute on function %s from %I cascade/);
  assert.match(owner, /grant execute on function %s to %I/);
  assert.match(verification, /v_execute_grantees is distinct from v_expected_execute_grantees/);
  assert.match(verification, /v_execute_grant_option is true/);
  assert.match(verification, /has_function_privilege\(v_function_owner, v_function_oid, 'EXECUTE'\)/);
  assert.doesNotMatch(verification, /has_function_privilege\('postgres', v_function_oid, 'EXECUTE'\)/);
  assert.equal(count(owner, /insert into public\./gi), 1);
  assert.match(owner, /insert into public\.mail_outbox/i);
  const updateTargets = [...owner.matchAll(/\bupdate\s+public\.([a-z0-9_]+)/gi)].map((match) => match[1]);
  assert.ok(updateTargets.length > 0);
  assert.deepEqual([...new Set(updateTargets)].sort(), [
    'mail_outbox',
    'pay_payment_correction_requests'
  ]);
  assert.doesNotMatch(owner, /\b(delete|truncate)\b[\s\S]{0,40}public\./i);
  assert.match(owner, /p_limit is null or p_limit < 1 or p_limit > 100/i);
  assert.match(owner, /jsonb_array_length\(v_item_json\) > 50000/i);
});

test('only exact provider-accepted payment mail and completed communication V2 qualify', () => {
  for (const required of [
    "type is distinct from 'REMITTANCE'",
    "status::text is distinct from 'SENT'",
    'sent_at is null',
    "provider_status is distinct from 'ACCEPTED'",
    "candidate_scope_contract_version' is distinct from '2'",
    "candidate_scope_hash_version' is distinct from '2'",
    "source_row_count_semantics' is distinct from 'FINANCIAL_ONLY'",
    "communication_cleanup_contract_version' is distinct from '2'",
    'applied_at_utc is null'
  ]) assert.ok(owner.toLowerCase().includes(required.toLowerCase()), `missing gate: ${required}`);
  assert.match(owner, /'CANDIDATE_REMITTANCE'/);
  assert.match(owner, /'UMBRELLA_REMITTANCE'/);
  assert.match(owner, /'PAYOUT_NOTICE_CANDIDATE'/);
  assert.match(owner, /ORIGINAL_RECIPIENT_IDENTITY_MISMATCH/);
  assert.match(owner, /ORIGINAL_PAY_BATCH_CANDIDATE_SCOPE_INVALID/);
  assert.match(owner, /correction_item\.pay_batch_candidate_id = any\(v_pay_batch_candidate_ids\)/i);
  assert.match(verification, /pay_payment_correction_items_applied_item_kind_uidx/);
  assert.match(verification, /PAYMENT_CANCELLATION_NOTICE_APPLIED_ITEM_UNIQUENESS_INVALID/);
});

test('notice copies recipients, is neutral, attachment-free and stores no item array', () => {
  assert.match(owner, /v_notice_subject := 'Payment cancelled';/);
  assert.doesNotMatch(owner, /v_notice_subject\s*:=\s*[^;]*v_original\.subject/i);
  assert.match(owner, /The payment described in our earlier email has been cancelled and will not be made as previously advised\./);
  assert.match(owner, /If a replacement payment is arranged, you will receive a separate notification\./);
  assert.match(owner, /'PAYMENT_CANCELLATION',[\s\S]+v_original\."to",[\s\S]+v_original\.cc,[\s\S]+v_original\.bcc,[\s\S]+v_original\.reply_to/i);
  assert.match(owner, /'\[\]'::jsonb,[\s\S]+'QUEUED'::public\.mail_status_enum/i);
  assert.match(owner, /'covered_item_count', pg_catalog\.cardinality\(v_item_ids\)/);
  assert.doesNotMatch(owner, /'pay_batch_item_ids',\s*v_item_ids/i);
  assert.equal(fixture.notice_contract.body_text, 'The payment described in our earlier email has been cancelled and will not be made as previously advised.\n\nIf a replacement payment is arranged, you will receive a separate notification.');
  assert.equal(fixture.notice_contract.subject, 'Payment cancelled');
  assert.equal(fixture.notice_contract.subject_is_fixed_and_does_not_copy_original, true);
});

test('exact deterministic replay is accepted but a tampered key collision is typed', () => {
  assert.match(owner, /on conflict \(deterministic_outbox_key\) do nothing/i);
  assert.match(owner, /EXISTING_NOTICE_IDENTITY_CONFLICT/);
  for (const binding of [
    'v_existing_notice."to" is not distinct from v_original."to"',
    'v_existing_notice.recipient_id is not distinct from v_original.recipient_id',
    "v_existing_notice.context_kind is not distinct from 'pay_payment_correction_requests'",
    'v_existing_notice.context_id is not distinct from v_request.id',
    'v_existing_notice.payment_scope_json is not distinct from v_notice_scope'
  ]) assert.ok(owner.includes(binding), `missing replay binding: ${binding}`);
  assert.match(owner, /v_examined <> v_eligible \+ v_skipped/);
  assert.match(owner, /v_eligible <> v_queued \+ v_already_present/);
  assert.match(owner, /v_reason_total <> v_skipped/);
});

test('recovery is row-event driven, server-paged and bounded before item expansion', () => {
  assert.match(owner, /Historical rows are absent from[\s\S]+cutover marker is NULL/i);
  assert.match(owner, /cancel_notice_tracked is true/);
  assert.match(owner, /claimed_source as materialized[\s\S]+for update skip locked[\s\S]+limit p_limit \+ 1[\s\S]+locked_source as materialized/i);
  const claimAt = owner.indexOf('with claimed_source as materialized');
  const lockAt = owner.indexOf('for update skip locked', claimAt);
  const windowAt = owner.indexOf('pg_catalog.row_number() over', lockAt);
  assert.ok(claimAt >= 0 && lockAt > claimAt && windowAt > lockAt);
  assert.match(owner, /weighted_source\.source_ordinal <= p_limit/);
  assert.doesNotMatch(owner, /weighted_source\.source_ordinal <= p_limit \+ 1/);
  assert.match(owner, /candidate_seed as materialized[\s\S]+pay_batch_item_id = v_item_ids\[1\][\s\S]+limit 3/i);
  assert.match(owner, /MAIL mode may resolve the exact pre-cutover APPLIED request[\s\S]{0,500}?request_row\.status in \('APPLIED', 'APPLIED_WITH_BLOCKERS'\)/i);
  assert.match(owner, /request_scope as materialized[\s\S]+candidate_source_per_scope as materialized[\s\S]+cross join lateral[\s\S]+payment_scope_json->>'candidate_id'[\s\S]+candidate_source as materialized[\s\S]+limit p_limit \+ 1/i);
  assert.match(owner, /order by coalesce\([\s\S]{0,500}?limit 1\s*\) as mail_event/i);
  assert.match(owner, /order by coalesce\([\s\S]{0,500}?limit 1\s*\) as request_event/i);
  assert.match(owner, /for update skip locked[\s\S]+busy oldest row must never make[\s\S]+for update skip locked/i);
  assert.match(owner, /Capture the current oldest due mail without a lock[\s\S]+v_recovery_selected_mail_id[\s\S]+not \([\s\S]+any\([\s\S]+v_recovery_claim_contended or exists/i);
  assert.match(owner, /'has_more', v_has_more or v_recovery_claim_contended/i);
  assert.match(owner, /PAYMENT_CANCELLATION_NOTICE_EXTERNAL_CURSOR_PROHIBITED/);
  assert.match(owner, /'progress_owner', 'SERVER_ROW'/);
  assert.match(owner, /'EXISTING_NOTICE_IDENTITY_CONFLICT'/);
  assert.equal(fixture.rpc_page.worker_recovery_pages_per_drain, 1);
  assert.equal(fixture.rpc_page.worker_absolute_page_cap, 1);
  assert.equal(fixture.rpc_page.caller_cursor_permitted, false);
});

test('Worker strictly validates the server-owned result envelope', () => {
  const { helpers } = workerHelpers();
  const valid = helpers.validatePaymentCancellationNoticePage(page(), 'RECOVERY', 50, null);
  assert.equal(valid.server_owned_progress, true);
  assert.equal(valid.has_more, false);
  assert.equal(valid.recovery_claim_contended, false);
  assert.equal(
    helpers.validatePaymentCancellationNoticePage(
      page({ has_more: true, recovery_claim_contended: true }),
      'RECOVERY',
      50,
      null
    ).recovery_claim_contended,
    true
  );
  assert.equal(
    helpers.validatePaymentCancellationNoticePage(
      page({ mode: 'CORRECTION', recovery_claim_contended: false }),
      'CORRECTION',
      50,
      null
    ).recovery_claim_contended,
    false
  );
  const correctionWithoutContentionField = page({ mode: 'CORRECTION' });
  delete correctionWithoutContentionField.recovery_claim_contended;
  assert.equal(
    helpers.validatePaymentCancellationNoticePage(
      correctionWithoutContentionField,
      'CORRECTION',
      50,
      null
    ).recovery_claim_contended,
    false
  );
  assert.equal(
    helpers.validatePaymentCancellationNoticePage(page({
      eligible: 0,
      queued: 0,
      skipped: 1,
      reason_counts: { ORIGINAL_NOT_FOUND: 1 }
    }), 'RECOVERY', 50, null).server_owned_progress,
    true
  );
  for (const invalid of [
    {},
    page({ ok: false }),
    page({ examined: 2 }),
    page({ eligible: 0 }),
    page({ reason_counts: { BAD: -1 } }),
    page({ reason_counts: { BAD: 0.5 } }),
    (() => {
      const missing = page();
      delete missing.recovery_claim_contended;
      return missing;
    })(),
    page({ recovery_claim_contended: 'false' }),
    page({ recovery_claim_contended: true, has_more: false }),
    page({ eligible: 0, queued: 0, skipped: 1, reason_counts: { UNKNOWN_REASON: 1 } }),
    page({ examined: 1, eligible: 0, queued: 1, skipped: 1, reason_counts: { BAD: 1 } }),
    page({ examined: 1, eligible: 1, queued: 0, already_present: 0 }),
    page({ skipped: 1, reason_counts: {} }),
    page({ examined: 51, eligible: 51, queued: 51 }),
    page({ has_more: false, next_cursor: {} }),
    page({ progress_owner: 'CALLER_CURSOR' })
  ]) assert.throws(() => helpers.validatePaymentCancellationNoticePage(invalid, 'RECOVERY', 50, null));
  assert.throws(() => helpers.validatePaymentCancellationNoticePage(
    page({ mode: 'CORRECTION', recovery_claim_contended: true, has_more: true }),
    'CORRECTION',
    50,
    null
  ));
  assert.throws(() => helpers.validatePaymentCancellationNoticePage(
    page({ mode: 'CORRECTION', recovery_claim_contended: 'false' }),
    'CORRECTION',
    50,
    null
  ));
  assert.throws(() => helpers.validatePaymentCancellationNoticePage(page(), 'RECOVERY', 50, {}));
});

test('Worker recovery makes exactly one bounded server-progress call per drain', async () => {
  const calls = [];
  const { helpers } = workerHelpers({
    async sbRpc(env, name, args) {
      calls.push({ env, name, args });
      return page({ has_more: true, recovery_claim_contended: true });
    }
  });
  const once = await helpers.reconcilePaymentCancellationNoticesBounded({ marker: true });
  assert.equal(once.calls, 1);
  assert.equal(calls.length, 1);
  assert.equal(once.has_more, true);
  assert.equal(once.recovery_claim_contended, true);
  assert.equal(calls[0].args.p_after_created_at_utc, null);
  assert.equal(calls[0].args.p_after_mail_outbox_id, null);
  assert.ok(calls.every((call) => call.name === 'pay_payment_cancellation_notice_reconcile_v1'));
});

test('detached notice failure is observable but can never reject cancellation', async () => {
  const { helpers, warnings } = workerHelpers({
    async sbRpc() { throw new Error('expected email-only failure'); }
  });
  let task;
  const accepted = helpers.schedulePaymentCancellationNoticeNudge({}, {
    waitUntil(value) { task = value; }
  }, '11111111-1111-4111-8111-111111111111');
  assert.equal(accepted, true);
  await task;
  assert.equal(warnings.length, 1);
  assert.match(warnings[0], /PAYMENT_CANCELLATION_NOTICE_NUDGE_FAILED/);
  assert.equal(JSON.parse(warnings[0]).calls, 1);
  assert.equal(helpers.schedulePaymentCancellationNoticeNudge({}, null, '11111111-1111-4111-8111-111111111111'), false);

  const rejected = workerHelpers();
  assert.equal(rejected.helpers.schedulePaymentCancellationNoticeNudge({}, {
    waitUntil() { throw new Error('expected waitUntil rejection'); }
  }, '11111111-1111-4111-8111-111111111111'), false);
  assert.equal(rejected.warnings.length, 1);
  assert.equal(JSON.parse(rejected.warnings[0]).calls, 1);
});

test('recovery contention is retained in bounded attention logging', () => {
  const { helpers, warnings } = workerHelpers();
  helpers.observePaymentCancellationNoticeReconcile('MAIL_OUTBOX_DRAIN_COMPLETE', {
    ok: true,
    calls: 1,
    queued: 0,
    already_present: 0,
    skipped: 0,
    has_more: true,
    recovery_claim_contended: true,
    reason_codes: []
  });
  assert.equal(warnings.length, 1);
  const warning = JSON.parse(warnings[0]);
  assert.equal(warning.recovery_claim_contended, true);
  assert.equal(warning.has_more, true);
});

test('Worker hooks cannot create per-email RPC fan-out or alter cancellation success', () => {
  const finishStart = worker.indexOf('const finishDrainReport = async (report) =>');
  const finishEnd = worker.indexOf('\n  const nowIsoUtc', finishStart);
  const finish = worker.slice(finishStart, finishEnd);
  assert.equal(count(finish, /reconcilePaymentCancellationNoticesBounded\(env/g), 1);
  assert.match(finish, /payment_cancellation_notice_recovery/);
  assert.match(finish, /PAYMENT_CANCELLATION_NOTICE_RECOVERY_FAILED'[\s\S]+calls: 1/);
  const sentPatchStart = worker.indexOf('const patchClaimedRowSent = async');
  const sentPatchEnd = worker.indexOf('\n  const patchClaimedRowDeferred', sentPatchStart);
  assert.doesNotMatch(worker.slice(sentPatchStart, sentPatchEnd), /pay_payment_cancellation_notice_reconcile_v1|reconcilePaymentCancellationNoticesBounded/);
  const advanceStart = worker.indexOf('async function advancePaymentCorrectionOperation');
  const advanceEnd = worker.indexOf('\nasync function ', advanceStart + 20);
  const advance = worker.slice(advanceStart, advanceEnd);
  assert.match(advance, /schedulePaymentCancellationNoticeNudge/);
  assert.match(advance, /throw error/);
});

test('current producer evidence binds three persisted shapes and established manual FAILED retry', () => {
  assert.match(monolith, /CREATE OR REPLACE FUNCTION public\.pay_operation_remittance_scope_seed/);
  assert.match(monolith, /'pay_batch_candidate_id', candidate_scope\.pay_batch_candidate_id::text/);
  assert.match(monolith, /'pay_batch_candidate_id', umbrella_scope\.pay_batch_candidate_id::text/);
  assert.match(monolith, /CREATE OR REPLACE FUNCTION public\.pay_finance_payout_notice_queue_commit_stage/);
  assert.match(monolith, /'pay_batch_candidate_id', candidate_scope\.pay_batch_candidate_id::text/);
  assert.match(monolith, /valid_targets\.payload_json[\s\S]+PAYOUT_NOTICE_CANDIDATE[\s\S]+'items', valid_targets\.items_json/i);
  assert.match(mailClaim, /where mo\.status='QUEUED' and mo\.sent_at is null/i);
  assert.doesNotMatch(mailClaim, /mo\.status\s+in\s*\([^)]*FAILED/i);
  assert.match(manualRetry, /create or replace function public\.outbox_unified_retry\(/i);
  assert.match(manualRetry, /if v_channel = 'EMAIL'[\s\S]+upper\(coalesce\(v_mail_row\.status::text,''\)\) not in \('QUEUED','FAILED'\)[\s\S]+status = 'QUEUED'/i);
  assert.match(owner, /outbox_unified_retry[\s\S]+must not invent an[\s\S]+automatic retry policy/i);
  assert.deepEqual(fixture.explicitly_non_reachable_persisted_shapes, [
    'CANDIDATE_UMBRELLA_COPY_REMITTANCE'
  ]);
});

test('current operation route does not persist a separate Candidate Umbrella copy message', () => {
  const seedStart = monolith.indexOf('CREATE OR REPLACE FUNCTION public.pay_operation_remittance_scope_seed(');
  const seedEnd = monolith.indexOf('CREATE OR REPLACE FUNCTION', seedStart + 20);
  const queueStart = monolith.indexOf('CREATE OR REPLACE FUNCTION public.pay_remittance_queue_commit_stage(');
  const queueEnd = monolith.indexOf('CREATE OR REPLACE FUNCTION', queueStart + 20);
  assert.ok(seedStart >= 0 && seedEnd > seedStart && queueStart >= 0 && queueEnd > queueStart);
  const seedOwner = monolith.slice(seedStart, seedEnd);
  const queueOwner = monolith.slice(queueStart, queueEnd);
  const operationStart = queueOwner.indexOf('IF v_operation_mode THEN');
  const legacyStart = queueOwner.indexOf('SELECT COUNT(*)::integer\n  INTO v_legacy_scope_count', operationStart);
  assert.ok(operationStart >= 0 && legacyStart > operationStart);
  const operationOwner = queueOwner.slice(operationStart, legacyStart);
  assert.match(seedOwner, /'CANDIDATE_REMITTANCE'::text AS remittance_type/i);
  assert.match(seedOwner, /'UMBRELLA_REMITTANCE'::text AS remittance_type/i);
  assert.doesNotMatch(seedOwner, /CANDIDATE_UMBRELLA_COPY_REMITTANCE/);
  assert.match(operationOwner, /insert into public\.mail_outbox/i);
  assert.match(operationOwner, /valid_targets\.remittance_type/i);
  assert.doesNotMatch(operationOwner, /CANDIDATE_UMBRELLA_COPY_REMITTANCE/);
  assert.match(queueOwner, /CANDIDATE_UMBRELLA_COPY_REMITTANCE/);
  assert.match(worker, /const allowLegacySmallCompatibility = readBool\([\s\S]{0,300}, false\);/);
  assert.match(worker, /if \(operationId\)[\s\S]+pay_remittance_queue_commit_stage[\s\S]+pay_finance_payout_notice_queue_commit_stage/);
  assert.doesNotMatch(worker, /allow(?:LegacySmallCompatibility|_legacy_small_compatibility|SynchronousSmallCompatibility|_synchronous_small_compatibility)\s*:/);
});

test('saved progress and terminal replay are identity-bound, size-bounded and fail closed', () => {
  for (const required of [
    "v_request_result_json->>'template_version'",
    "v_request_result_json->>'reconciliation_authority'",
    "v_request_result_json->'event_applied_at_utc'",
    "v_request_result_json->'last_source_created_at_utc'",
    "v_request_result_json->'last_source_mail_outbox_id'",
    'pg_catalog.pg_column_size(v_request_result_json) > 16384',
    'reason_entry.reason_code <> all(v_allowed_reason_codes)',
    'PAYMENT_CANCELLATION_NOTICE_TERMINAL_PROGRESS_CONFLICT',
    "'already_complete', true"
  ]) assert.ok(owner.includes(required), `missing saved/replay boundary: ${required}`);
  assert.match(owner, /coalesce\([\s\S]{0,250}?pg_catalog\.to_char\([\s\S]{0,250}?pg_catalog\.timezone\(\s*'UTC', v_request_progress_after_created_at_utc\s*\)[\s\S]{0,150}?'null'::jsonb/i);
  assert.match(owner, /case[\s\S]+jsonb_typeof\([\s\S]+reason_counts[\s\S]+then v_request_result_json->'reason_counts'[\s\S]+else '\{\}'::jsonb/i);
});

test('fixture freezes the complete positive, negative, race and scale inventory', () => {
  assert.deepEqual(fixture.positive_source_shapes, [
    'CANDIDATE_REMITTANCE',
    'UMBRELLA_REMITTANCE',
    'PAYOUT_NOTICE_CANDIDATE'
  ]);
  assert.deepEqual(fixture.stress_item_counts, [1, 101, 1001, 5000]);
  assert.equal(fixture.boundary_rejection_item_count, 50001);
  for (const required of [
    'ORIGINAL_QUEUED',
    'ORIGINAL_FAILED',
    'ORIGINAL_SENT_AT_ABSENT',
    'ORIGINAL_PROVIDER_NOT_ACCEPTED',
    'ORIGINAL_WRONG_RECIPIENT',
    'ORIGINAL_WRONG_PAY_BATCH_CANDIDATE',
    'ORIGINAL_ITEM_SCOPE_PARTIAL',
    'ORIGINAL_ITEM_SCOPE_EXTRA',
    'ORIGINAL_ITEM_SCOPE_DUPLICATE',
    'ORIGINAL_ITEM_COUNT_MISMATCH',
    'ORIGINAL_ITEM_SCOPE_50001',
    'EXISTING_NOTICE_IDENTITY_CONFLICT'
  ]) assert.ok(fixture.negative_cases.includes(required), `missing negative ${required}`);
  for (const required of [
    'EXACT_REPLAY_MAX_ONE_NOTICE',
    'CORRECTION_COMPLETE_BEFORE_ORIGINAL_SENT',
    'ORIGINAL_SENT_BEFORE_CORRECTION_COMPLETE',
    'CONCURRENT_CORRECTION_AND_RECOVERY',
    'BOUNDED_NOISE_INDEX_ISOLATION_DOES_NOT_STARVE_ELIGIBLE',
    'ONE_CORRECTION_COVERS_TWO_SEPARATELY_SENT_EMAILS',
    'REQUEST_PROGRESS_IS_SERVER_OWNED',
    'CUTOVER_UNTRACKED_APPLIED_REQUEST_WITH_LATER_SENT_TRACKED_MAIL_CONVERGES',
    'NO_CURSOR_UNEXPECTED_ERROR_RETRY_RESUMES',
    'CANCELLATION_RESULT_NEVER_DEPENDS_ON_EMAIL'
  ]) assert.ok(fixture.reliability_cases.includes(required), `missing reliability case ${required}`);
});

test('source mutations remove each critical boundary and are detected', () => {
  const required = [
    "v_original.status::text is distinct from 'SENT'",
    'v_original.sent_at is null',
    "v_original.provider_status is distinct from 'ACCEPTED'",
    'v_original.recipient_id is distinct from v_candidate_id',
    'correction_item.pay_batch_candidate_id = any(v_pay_batch_candidate_ids)',
    'correction_item.applied_at_utc is not null',
    "v_existing_notice.type is not distinct from 'PAYMENT_CANCELLATION'",
    'v_examined <> v_eligible + v_skipped',
    'v_prior_examined::bigint + v_examined::bigint > 50000',
    "message = 'PAYMENT_CANCELLATION_NOTICE_REQUEST_EVENT_CEILING_EXCEEDED'"
  ];
  for (const boundary of required) {
    assert.ok(owner.includes(boundary), `missing mutation target ${boundary}`);
    const mutated = owner.replaceAll(boundary, 'true');
    assert.notEqual(mutated, owner);
    assert.equal(mutated.includes(boundary), false);
  }
  assert.equal(count(owner, /'recovery_claim_contended', false/g), 3);
  assert.equal(count(owner, /'recovery_claim_contended', v_recovery_claim_contended/g), 3);
  assert.match(owner, /REQUEST_PROGRESS_CURSOR_CORRUPT[\s\S]{0,600}?'has_more', v_recovery_claim_contended[\s\S]{0,200}?'recovery_claim_contended', v_recovery_claim_contended/);
  assert.match(owner, /SAVED_PROGRESS_INVALID[\s\S]{0,600}?'has_more', v_recovery_claim_contended[\s\S]{0,200}?'recovery_claim_contended', v_recovery_claim_contended/);
  assert.match(verification, /PAYMENT_CANCELLATION_NOTICE_SOURCE_BOUNDARY_INVALID/);
  assert.match(verification, /PAYMENT_CANCELLATION_NOTICE_APPLIED_ITEM_UNIQUENESS_INVALID/);
  assert.match(verification, /PAYMENT_CANCELLATION_NOTICE_PENDING_REQUEST_INDEX_INVALID/);
  for (const aclBoundary of [
    'acl_row.grantee not in (',
    'acl_row.is_grantable is true',
    'v_execute_grantees is distinct from v_expected_execute_grantees',
    'v_execute_grant_option is true'
  ]) {
    const source = aclBoundary.startsWith('v_execute_') ? verification : owner;
    assert.ok(source.includes(aclBoundary), `missing ACL mutation target ${aclBoundary}`);
    assert.equal(source.replace(aclBoundary, 'false').includes(aclBoundary), false);
  }
});
