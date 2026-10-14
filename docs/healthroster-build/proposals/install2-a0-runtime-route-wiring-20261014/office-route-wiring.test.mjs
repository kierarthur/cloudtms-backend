import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { createTimesheetCorrectionOrchestrator } from './reference/broker/src/timesheet-correction-orchestrator.mjs';
import { assertTimesheetCorrectionOfficeActor, createTimesheetCorrectionLifecycleResolver,
  handleTimesheetCorrectionOfficeAction, matchTimesheetCorrectionOfficeRoute } from './forward/broker/src/timesheet-correction-office-routes.mjs';
import { id, I, H, copy, action, prepareFixture, replacementFixture, appliedFixture, cancelledFixture } from './unit-fixtures.mjs';
// Effect-free UNIT_ONLY. Actual complete saved factory hunk is evaluated with
// local typed native-owner stubs; fetch/DB/R2/provider/network are never mounted.
const reverse = JSON.parse(fs.readFileSync(new URL('./CURRENT_INDEX_FULL_REVERSE.json', import.meta.url)));
const wiring = reverse.hunks[1].replacement.split('\nasync function runTsfinWorkerOnce(')[0];
const env = { CANDIDATE_APP_ENVIRONMENT: 'TEST', MYTMS_OFFICE_AGENCY_ID: I.client, SUPABASE_URL: 'https://unit.invalid' };
const user = { id: I.actor, role: 'admin', is_active: true, tenant_id: I.client };
const send = { timesheet_id: I.original, expected_version: 7, proposal: { schedule: [] }, reason: 'UNIT_ONLY correction', request_key: 'UNIT_ONLY-exact' };
const request = (suffix, payload, options = {}) => new Request(`https://unit.invalid/api/timesheets/corrections/${suffix}`, {
  method: 'POST', headers: { 'content-type': 'application/json', ...options.headers },
  body: JSON.stringify(payload), ...options
});
function harness(options = {}) {
  const calls = [], users = options.users || [user]; let auth = 0;
  const requireUser = async (...args) => { calls.push(['requireUser', args[2]]); return users[Math.min(auth++, users.length - 1)]; };
  const sbRpc = async (_, name, args, transport) => {
    calls.push([name, copy(args), transport]);
    if (options.rpc) return options.rpc(name, args);
    if (name === 'timesheet_correction_send_v1') return { ok: true, request_id: I.request, state: 'WAITING', state_revision: 1,
      replacement_timesheet_id: I.p0, replacement_booking_id: 'unit:p0', target_revision: H, proposal_revision: 1 };
    if (name === 'timesheet_correction_request_authority_v1') return { contract_version: 'TIMESHEET_CORRECTION_REQUEST_AUTHORITY_V1',
      request_id: I.request, state: options.state || 'WAITING', state_revision: options.state === 'APPLIED' ? '2' : '1',
      replacement_timesheet_id: I.p0, return_invoice_id: null };
    if (name === 'timesheet_correction_cancel_atomic_v1') return cancelledFixture();
    if (name === 'timesheet_correction_prepare_v1') return options.prepared || prepareFixture();
    if (name === 'timesheet_correction_authorise_atomic_v1') return appliedFixture();
    if (name === 'timesheet_correction_return_to_draft_v1') return { ok: true, request_id: I.request, not_required: true };
    if (name === 'timesheet_authorise_bulk_atomic' || name === 'timesheet_unauthorise_bulk_atomic') return {
      ok: true, all_success: true, failure_count: 0, results: args.p_items.map(item => ({ success: true, current_timesheet_id: item.timesheet_id })) };
    throw new Error(`Unexpected UNIT_ONLY owner ${name}`);
  };
  const cleanup = async (_, result, opts) => {
    calls.push(['cleanup', copy(result), copy(opts)]);
    if (options.cleanupFail) throw new Error('UNIT_ONLY transient cleanup');
    return { ok: true, delete_operation_id: I.deleteOp, requested_timesheet_id: I.p0, deleted_timesheet_ids: [I.p0], requested_key_count: 1,
      attempted_key_count: 1, deleted_key_count: 1, already_missing_key_count: 0, deferred_key_count: 0,
      r2_cleanup_complete: true, durable_retry_recorded: true, deleted_keys: ['exact/p0/mutable.pdf'], deferred_keys: [] };
  };
  const owners = { createTimesheetCorrectionLifecycleResolver, assertTimesheetCorrectionOfficeActor,
    handleTimesheetCorrectionOfficeAction, requireUser, sbRpc,
    createTimesheetCorrectionNativeOrchestrator: (_, deps) => createTimesheetCorrectionOrchestrator({ ...deps,
      buildReplacementSnapshot: async () => replacementFixture() }),
    rpcTsfinLoadContextBatch: async (_, { timesheetIds }) => timesheetIds.map(timesheet_id => ({ effective_timesheet_id: timesheet_id,
      out_timesheet: { timesheet_id, is_current: true, archived_at_utc: null, version: 7, ...options.physical?.(timesheet_id) }, out_candidate: { id: I.candidate } })),
    deleteCommittedTimesheetR2Keys: cleanup,
    weeklySourceUnauthorisePairPreflight: async scope => { calls.push(['sourcePreflight', scope.timesheetIds]); return options.preflight || { ordinary: true }; },
    bpayNextOwnerStatusRpc: () => (() => {}), sbGetOne: async () => ({}), nowIso: () => '2026-10-14T12:00:00Z',
    requestTimesheetLifecycleWorkbenchWake: (_, __, wake) => { calls.push(['wake', wake]); if (options.wakeFail) throw new Error('UNIT_ONLY wake'); },
    timesheetLifecycleMutationChangedCount: result => result.results?.length || 0,
    JSON_HEADERS: { 'content-type': 'application/json' }, withCORS: (_, __, response) => response };
  const code = `const { ${Object.keys(owners).join(', ')} } = owners;\n${wiring}\n` +
    'return { createTimesheetCorrectionOfficeOrchestrator, handleTimesheetCorrectionOfficeRoute, timesheetCorrectionOfficeFailure };';
  const actual = Function('owners', code)(owners);
  return { calls, actual, rpc: sbRpc };
}
test('finite action map has no PREPARE, arbitrary owner, dynamic URL or GET mutation', () => {
  assert.equal(matchTimesheetCorrectionOfficeRoute('POST', '/api/timesheets/corrections/send'), 'send');
  for (const method of ['GET', 'DELETE', 'PUT']) assert.equal(matchTimesheetCorrectionOfficeRoute(method, '/api/timesheets/corrections/send'), null);
  for (const suffix of ['prepare', 'private-owner', 'send/extra']) assert.equal(matchTimesheetCorrectionOfficeRoute('POST', `/api/timesheets/corrections/${suffix}`), null);
});
test('actual saved index SEND factory forwards untouched closed payload and authenticated actor through current service owner', async () => {
  const h = harness(); const response = await h.actual.handleTimesheetCorrectionOfficeRoute(env, request('send', send), 'send');
  assert.equal(response.status, 200);
  const call = h.calls.find(call => call[0] === 'timesheet_correction_send_v1');
  assert.deepEqual(call[1], { p_request: send, p_actor_user_id: I.actor });
  assert.deepEqual(call[2], { timeoutMs: 20000 });
  assert.equal(h.calls.filter(call => call[0] === 'requireUser').length, 2);
  assert.equal(h.calls.filter(call => call[0] === 'cleanup').length, 0);
});
test('missing/inactive/non-admin/foreign tenant and non-TEST cannot create a native command', async () => {
  for (const denied of [null, { ...user, is_active: false }, { ...user, role: 'pay' }, { ...user, tenant_id: id(99) }]) {
    const h = harness({ users: [denied] });
    assert.equal((await h.actual.handleTimesheetCorrectionOfficeRoute(env, request('send', send), 'send')).status, 403);
    assert.equal(h.calls.filter(call => call[0].startsWith('timesheet_')).length, 0);
  }
  const h = harness(); assert.equal((await h.actual.handleTimesheetCorrectionOfficeRoute({ ...env, CANDIDATE_APP_ENVIRONMENT: 'LIVE' }, request('send', send), 'send')).status, 403);
  assert.equal(h.calls.filter(call => call[0].startsWith('timesheet_')).length, 0);
});
test('actor change after request read is denied at actual native factory recheck', async () => {
  const h = harness({ users: [user, { ...user, id: id(97) }] });
  assert.equal((await h.actual.handleTimesheetCorrectionOfficeRoute(env, request('send', send), 'send')).status, 403);
  assert.equal(h.calls.filter(call => call[0].startsWith('timesheet_')).length, 0);
});
test('browser actor/finance/prepared snapshot and query scope are refused before RPC', async () => {
  for (const extra of [{ actor_user_id: I.actor }, { environment: 'TEST' }, { prepared: {} }, { finance: {} }]) {
    const h = harness(); const response = await h.actual.handleTimesheetCorrectionOfficeRoute(env, request('send', { ...send, ...extra }), 'send');
    assert.equal(response.status, 400); assert.equal(h.calls.filter(call => call[0].startsWith('timesheet_')).length, 0);
  }
  const h = harness(); const response = await h.actual.handleTimesheetCorrectionOfficeRoute(env, request('send?actor=unit', send), 'send');
  assert.equal(response.status, 400); assert.equal(h.calls.length, 0);
});
test('actual streamed byte bound handles missing Content-Length and denies overlarge bodies', async () => {
  const h = harness(); const response = await h.actual.handleTimesheetCorrectionOfficeRoute(env,
    request('send', { ...send, reason: 'x'.repeat(262144) }), 'send');
  assert.equal(response.status, 413); assert.equal(h.calls.filter(call => call[0].startsWith('timesheet_')).length, 0);
});
test('native missing qualification/refusal stays closed and reveals no raw prepared/error details', async () => {
  const h = harness({ rpc: async () => { throw { code: '55000', status: 409,
    json: { code: '55000', message: 'WEEKLY_SOURCE_PROTECTED_CAPABILITY_RELEASE_UNQUALIFIED', details: 'UNIT_ONLY private context' } }; } });
  const response = await h.actual.handleTimesheetCorrectionOfficeRoute(env, request('send', send), 'send');
  assert.equal(response.status, 409);
  assert.deepEqual(await response.json(), { ok: false, error_code: 'WEEKLY_SOURCE_PROTECTED_CAPABILITY_RELEASE_UNQUALIFIED' });
});
test('pending Cancel binds exact native reader; committed singleton keys reach existing cleanup only after native receipt', async () => {
  const h = harness(); const response = await h.actual.handleTimesheetCorrectionOfficeRoute(env, request('cancel', action()), 'cancel');
  assert.equal(response.status, 200);
  const calls = h.calls.map(call => call[0]);
  assert.ok(calls.indexOf('timesheet_correction_request_authority_v1') < calls.indexOf('timesheet_correction_cancel_atomic_v1'));
  assert.ok(calls.indexOf('timesheet_correction_cancel_atomic_v1') < calls.indexOf('cleanup'));
  assert.ok(!calls.includes('timesheet_correction_prepare_v1'));
  assert.deepEqual(h.calls.find(call => call[0] === 'cleanup').slice(1), [cancelledFixture().lawful_removal,
    { deleteOperationId: I.deleteOp, requestedTimesheetId: I.p0 }]);
});
test('cleanup failure remains committed CANCELLED/pending and does not replay SQL', async () => {
  const h = harness({ cleanupFail: true }); const response = await h.actual.handleTimesheetCorrectionOfficeRoute(env, request('cancel', action()), 'cancel');
  const result = await response.json(); assert.equal(response.status, 200); assert.equal(result.state, 'CANCELLED');
  assert.equal(result.cleanup.cleanup_pending, true); assert.equal(h.calls.filter(call => call[0] === 'timesheet_correction_cancel_atomic_v1').length, 1);
});
test('Return uses exact terminal read CAS and existing owner without inventing native CAS argument', async () => {
  const h = harness({ state: 'APPLIED' }); const response = await h.actual.handleTimesheetCorrectionOfficeRoute(env, request('return-to-draft', action(2)), 'returnToDraft');
  assert.equal(response.status, 200);
  assert.deepEqual(h.calls.find(call => call[0] === 'timesheet_correction_return_to_draft_v1')[1], { p_request_id: I.request, p_actor_user_id: I.actor });
  const stale = harness({ state: 'APPLIED' }); assert.equal((await stale.actual.handleTimesheetCorrectionOfficeRoute(env, request('return-to-draft', action(1)), 'returnToDraft')).status, 409);
  assert.ok(!stale.calls.some(call => call[0] === 'timesheet_correction_return_to_draft_v1'));
});
test('actual AUTHORISE consumer awaits canonical producer/native atomic unit and exposes only its applied receipt/P0 wake', async () => {
  const h = harness(); const response = await h.actual.handleTimesheetCorrectionOfficeRoute(env, request('authorise', action()), 'authorise');
  const receipt = await response.json(); assert.equal(response.status, 200, JSON.stringify(receipt));
  assert.equal(receipt.state, 'APPLIED'); assert.equal(receipt.replacement_timesheet_id, I.p0);
  assert.ok(!Object.hasOwn(receipt, 'authority') && !Object.hasOwn(receipt, 'original_current_snapshot'));
  const atomic = h.calls.find(call => call[0] === 'timesheet_correction_authorise_atomic_v1');
  assert.equal(atomic[1].p_actor_user_id, I.actor); assert.equal(atomic[1].p_prepared.replacement_snapshot.timesheet_id, I.p0);
  assert.equal(atomic[1].p_prepared.reversal_snapshot.timesheet_id, I.r0);
  assert.ok(!Object.hasOwn(atomic[1], 'p_now_utc'));
  assert.equal(h.calls.find(call => call[0] === 'wake')[1].timesheetId, I.p0);
});
test('native APPLIED replay skips producer/mutation and new post-commit wake', async () => {
  const h = harness({ prepared: appliedFixture({ replayed: true }) });
  const response = await h.actual.handleTimesheetCorrectionOfficeRoute(env, request('authorise', action()), 'authorise');
  const receipt = await response.json(); assert.equal(response.status, 200, JSON.stringify(receipt)); assert.equal(receipt.replayed, true);
  assert.ok(!h.calls.some(call => call[0] === 'timesheet_correction_authorise_atomic_v1' || call[0] === 'wake'));
});
test('native fresh applied unit stays committed when existing post-commit wake fails', async () => {
  const h = harness({ wakeFail: true }); const response = await h.actual.handleTimesheetCorrectionOfficeRoute(env, request('authorise', action()), 'authorise');
  const receipt = await response.json(); assert.equal(response.status, 200, JSON.stringify(receipt)); assert.equal(receipt.state, 'APPLIED'); assert.equal(receipt.follow_up_pending, true);
  assert.equal(h.calls.filter(call => call[0] === 'timesheet_correction_authorise_atomic_v1').length, 1);
});
test('actual mixed ordinary lifecycle uses native bulk owner and retains Source/NEXT preflight before UNAUTHORISE', async () => {
  const h = harness({ rpc: async (name, args) => name === 'timesheet_correction_request_projection_v1' ? null : {
    ok: true, all_success: true, failure_count: 0, results: args.p_items.map(item => ({ success: true, current_timesheet_id: item.timesheet_id })) } });
  const response = await h.actual.handleTimesheetCorrectionOfficeRoute(env, request('lifecycle', { action: 'UNAUTHORISE', items: [{ timesheet_id: I.original }] }), 'runLifecycle');
  const receipt = await response.json(); assert.equal(response.status, 200, JSON.stringify(receipt)); assert.equal(receipt.all_success, true);
  const calls = h.calls.map(call => call[0]); assert.ok(calls.indexOf('sourcePreflight') < calls.indexOf('timesheet_unauthorise_bulk_atomic'));
  assert.deepEqual(h.calls.find(call => call[0] === 'timesheet_unauthorise_bulk_atomic')[1].p_items, [{ timesheet_id: I.original }]);
});

function resolverHarness(options = {}) {
  const ids = options.ids || [I.p0, I.original], log = [];
  const baseProjection = { schema_version: 1, request_id: I.request, root_timesheet_id: I.root,
    reversed_timesheet_id: I.original, replacement_timesheet_id: I.p0, candidate_id: I.candidate,
    correction_id: I.correction, state: options.state || 'WAITING', state_revision: options.state === 'APPLIED' ? 2 : 1 };
  const resolver = createTimesheetCorrectionLifecycleResolver({
    readContexts: async () => ids.map(timesheet_id => ({ effective_timesheet_id: timesheet_id,
      out_candidate: { id: I.candidate }, out_timesheet: { timesheet_id, is_current: true, archived_at_utc: null,
        correction_kind: timesheet_id === I.p0 ? 'WHOLE_TIMESHEET_REPLACEMENT' : timesheet_id === I.r0 ? 'WHOLE_TIMESHEET_REVERSAL' : null,
        correction_id: timesheet_id === I.original ? null : options.wrongIdentity ? 'UNIT_ONLY wrong' : I.correction } })),
    readProjection: async id => { log.push(['projection', id]); return options.projection === null ? null : { ...baseProjection, ...options.projection }; },
    readPrepare: async () => { log.push(['prepare']); return options.state === 'APPLIED' ? appliedFixture({ replayed: true }) : {
      ...prepareFixture(), original_authorisation_required: true }; },
    readTransition: async () => { log.push(['transition']); return { ok: true, valid: true, action_ready: true, action: 'AUTHORISE',
      correction_shape: 'WHOLE_REVERSAL_REPLACEMENT', request_id: I.request, root_timesheet_id: I.root,
      correction_id: I.correction, chain_fingerprint: H, expected_member_count: 2,
      transition_items: [{ timesheet_id: I.r0, expected_timesheet_id: I.r0, expected_version: 1 },
        { timesheet_id: I.p0, expected_timesheet_id: I.p0, expected_version: 3 }], ...options.transition }; }
  });
  const resolve = timesheetId => resolver({ timesheetId, selectedTimesheetIds: ids, action: 'AUTHORISE',
    actor: { actor_user_id: I.actor, environment: 'TEST' } });
  return { resolver, resolve, log };
}
test('real owner-shaped WAITING P0 selection joins exact prepared required A0; original alone remains ordinary', async () => {
  const h = resolverHarness(); const p0 = await h.resolve(I.p0), a0 = await h.resolve(I.original);
  assert.equal(p0.request_id, I.request); assert.equal(a0.request_id, I.request); assert.equal(a0.original_authorisation_required, true);
  assert.equal(h.log.filter(call => call[0] === 'prepare').length, 1);
  assert.equal(await resolverHarness({ ids: [I.original] }).resolve(I.original), null);
});
test('missing or mismatched real physical P0 projection refuses rather than falling back to ordinary lifecycle', async () => {
  for (const options of [{ projection: null }, { wrongIdentity: true }, { projection: { candidate_id: id(95) } },
    { projection: { state_revision: 9007199254740992 } }]) await assert.rejects(resolverHarness(options).resolve(I.p0), /CORRECTION_LIFECYCLE_SCOPE_UNPROVED/);
});
test('APPLIED native whole transition retains exact native order/versions and derives only existing approval group fields', async () => {
  const h = resolverHarness({ state: 'APPLIED', ids: [I.p0, I.r0] }); const result = await h.resolve(I.p0);
  assert.deepEqual(result.transition_items, [
    { timesheet_id: I.r0, expected_timesheet_id: I.r0, expected_version: 1, lifecycle_group_id: `correction:${I.correction}`, lifecycle_group_size: 2 },
    { timesheet_id: I.p0, expected_timesheet_id: I.p0, expected_version: 3, lifecycle_group_id: `correction:${I.correction}`, lifecycle_group_size: 2 }]);
  await h.resolve(I.r0); assert.equal(h.log.filter(call => call[0] === 'transition').length, 1);
});
test('foreign/stale/blocked APPLIED native transition cannot qualify the whole lifecycle', async () => {
  for (const transition of [{ request_id: id(98) }, { valid: false }, { correction_id: 'UNIT_ONLY foreign' }, { chain_fingerprint: null }])
    await assert.rejects(resolverHarness({ state: 'APPLIED', transition }).resolve(I.p0), /CORRECTION_LIFECYCLE_SCOPE_UNPROVED/);
});
test('actual saved factory APPLIED whole consumer sends all native items in owner order to unchanged bulk RPC', async () => {
  const h = harness({ physical: timesheet_id => ({ correction_id: I.correction,
    correction_kind: timesheet_id === I.p0 ? 'WHOLE_TIMESHEET_REPLACEMENT' : 'WHOLE_TIMESHEET_REVERSAL' }),
    rpc: async (name, args) => {
      if (name === 'timesheet_correction_request_projection_v1') return { schema_version: 1, request_id: I.request,
        root_timesheet_id: I.root, reversed_timesheet_id: I.original, replacement_timesheet_id: I.p0,
        candidate_id: I.candidate, correction_id: I.correction, state: 'APPLIED', state_revision: 2 };
      if (name === 'timesheet_correction_prepare_v1') return appliedFixture({ replayed: true });
      if (name === 'timesheet_correction_pair_transition_v1') return { ok: true, valid: true, action_ready: true, action: 'AUTHORISE',
        correction_shape: 'WHOLE_REVERSAL_REPLACEMENT', request_id: I.request, root_timesheet_id: I.root,
        correction_id: I.correction, chain_fingerprint: H, expected_member_count: 2,
        transition_items: [{ timesheet_id: I.r0, expected_timesheet_id: I.r0, expected_version: 1 },
          { timesheet_id: I.p0, expected_timesheet_id: I.p0, expected_version: 3 }] };
      if (name === 'timesheet_authorise_bulk_atomic') return { ok: true, all_success: true, failure_count: 0,
        results: args.p_items.map(item => ({ success: true, current_timesheet_id: item.timesheet_id })) };
      assert.fail(`Unexpected UNIT_ONLY owner ${name}`);
    } });
  const response = await h.actual.handleTimesheetCorrectionOfficeRoute(env, request('lifecycle', { action: 'AUTHORISE', items: [{ timesheet_id: I.p0 }] }), 'runLifecycle');
  const receipt = await response.json(); assert.equal(response.status, 200, JSON.stringify(receipt)); assert.equal(receipt.all_success, true);
  assert.deepEqual(h.calls.find(call => call[0] === 'timesheet_authorise_bulk_atomic')[1].p_items.map(item => item.timesheet_id), [I.r0, I.p0]);
  const read = h.calls.find(call => call[0] === 'timesheet_correction_pair_transition_v1')[1];
  assert.equal(read.p_lock_rows, false); assert.equal(read.p_actor_user_id, I.actor); assert.equal(read.p_operation_id, null);
});
test('actual factory ordinary preflight hold produces truthful failed unit, no bulk mutation and no wake', async () => {
  const h = harness({ preflight: { ordinary: false, status: 409, body: { error_code: 'WEEKLY_SOURCE_NEXT_BUSY' } },
    rpc: async name => name === 'timesheet_correction_request_projection_v1' ? null : assert.fail('No mutation allowed') });
  const response = await h.actual.handleTimesheetCorrectionOfficeRoute(env, request('lifecycle', { action: 'UNAUTHORISE', items: [{ timesheet_id: I.original }] }), 'runLifecycle');
  assert.equal(response.status, 207); const result = await response.json(); assert.equal(result.all_success, false);
  assert.equal(result.results[0].code, 'WEEKLY_SOURCE_NEXT_BUSY'); assert.ok(!h.calls.some(call => call[0] === 'wake'));
});
