import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import crypto from 'node:crypto';
import assert from 'node:assert/strict';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const read = p => fs.readFileSync(path.join(here, p), 'utf8');
const sha = b => crypto.createHash('sha256').update(b).digest('hex');
const reverse = JSON.parse(read('MODE_A_POLICY_COMPLETE_REVERSES.json'));
const source = read('25_complete_current_routes_with_native_mode_a_continuation.js');
function interval(first, last) {
  const start = source.indexOf(first);
  const end = source.indexOf(last, start + first.length);
  assert.ok(start >= 0 && end > start);
  return source.slice(start, end);
}
const functions = interval('async function dispatchModeA(', '/**')
  + interval('async function modeARpc(', '/**')
  + interval('async function runModeAAuthorisationFollowUp(', '// Only the named protected-action seam');
function caller(dispatched, applied = { ok: true, clients: [], operations: [] }, options = {}) {
  const calls = [];
  class WeeklySourceRouteError extends Error {}
  const context = vm.createContext({ Object, Number, String, Array, WeeklySourceRouteError,
    fail(code, message, status, details) { throw Object.assign(new Error(message), { code, status, details }); },
    async callRpc(dependencies, name, request) {
      calls.push({ name, request });
      if (name === 'weekly_source_mode_a_dispatch_atomic_v1') {
        if (options.dispatchError) throw options.dispatchError;
        return dispatched;
      }
      if (options.applyError) throw options.applyError;
      return applied;
    },
  });
  vm.runInContext(functions + '\nthis.dispatchModeA = dispatchModeA;', context);
  const dependencies = {};
  if (options.followUp) dependencies.runImportReviewPostCommit = async (env, request) => {
    calls.push({ name: 'native_post_commit', request });
    return options.followUp;
  };
  return { calls, run: () => context.dispatchModeA(dependencies, {}, 'actual-actor', 'actual-publication') };
}
const dispatch = { ok: true, dispatched: true, reference_apply_required: false, preset_units: 1,
  timesheet_authority_rows: 2, comparisons_written: 2 };

for (const edge of reverse.reverses) test('UNIT_ONLY exact full owner reverse: ' + edge.output, () => {
  let b = read(edge.output);
  assert.equal(sha(b), edge.forward_sha256);
  for (const h of [...edge.hunks].reverse()) {
    assert.equal(b.split(h.after).length, 2);
    b = b.replace(h.after, () => h.before);
  }
  assert.equal(sha(b), edge.input_sha256);
  assert.equal(b, read('before/' + edge.output + '.txt'));
});

test('UNIT_ONLY actual route: all preset checks have no reference/apply or import authorisation call', async () => {
  const c = caller(dispatch);
  const result = await c.run();
  assert.deepEqual(c.calls.map(c => c.name), ['weekly_source_mode_a_dispatch_atomic_v1']);
  assert.equal(result.preset_units, 1);
  assert.equal(result.reference_apply_operations, 0);
  assert.equal(result.auto_authorise_requested, 0);
  assert.equal(result.auto_authorise_follow_up.required, false);
});
test('UNIT_ONLY actual route: ordinary native continuation still calls the two existing owners', async () => {
  const c = caller({ ...dispatch, preset_units: 0, reference_apply_required: true },
    { ok: true, operations: [{ auto_authorise_requested: false }], clients: [] });
  const result = await c.run();
  assert.deepEqual(c.calls.map(c => c.name), ['weekly_source_mode_a_dispatch_atomic_v1', 'weekly_source_mode_a_reference_apply_atomic_v1']);
  assert.equal(result.reference_apply_operations, 1);
  assert.deepEqual(JSON.parse(JSON.stringify(c.calls[1].request)), {
    actor_user_id: 'actual-actor', publication_id: 'actual-publication',
  });
});
test('UNIT_ONLY actual mixed caller retains the real native ordinary follow-up handle', async () => {
  const c = caller({ ...dispatch, reference_apply_required: true }, {
    ok: true, publication_id: 'actual-publication', preset_units_refused: 1, operations: [{ auto_authorise_requested: true }],
    clients: [{ applied: true, import_id: 'real-import', operation_id: 'real-operation', request_hash: 'real-hash',
      auto_authorise_timesheet_ids: ['ordinary-target'], apply_result: { actual: 'native-result' } }],
  }, { followUp: { ok: true } });
  const result = await c.run();
  assert.equal(result.preset_units_refused, 1);
  assert.equal(result.auto_authorise_follow_up.completed, 1);
  assert.deepEqual(JSON.parse(JSON.stringify(c.calls[2].request)), {
    importId: 'real-import', operationId: 'real-operation', actorUserId: 'actual-actor', requestHash: 'real-hash',
    applyResult: { actual: 'native-result' },
  });
});
for (const value of [undefined, null, 'false', 0, [], {}]) test('UNIT_ONLY actual route refuses untyped native continuation ' + JSON.stringify(value), async () => {
  const c = caller({ ...dispatch, reference_apply_required: value });
  await assert.rejects(c.run(), e => e.code === 'WEEKLY_SOURCE_MODE_A_CONTINUATION_UNAVAILABLE' && e.status === 503);
  assert.equal(c.calls.length, 1);
});
for (const value of [undefined, null, -1, 1.5, '1', [], Number.MAX_SAFE_INTEGER + 1]) test('UNIT_ONLY actual route refuses native unit count ' + JSON.stringify(value), async () => {
  const c = caller({ ...dispatch, preset_units: value });
  await assert.rejects(c.run(), e => e.code === 'WEEKLY_SOURCE_MODE_A_CONTINUATION_UNAVAILABLE');
  assert.equal(c.calls.length, 1);
});
test('UNIT_ONLY native refusal remains causal and identifies the already published upload', async () => {
  const c = caller({ ...dispatch, reference_apply_required: true }, null, {
    applyError: Object.assign(new Error('AGENCY_INVOICE_USES_COMPLETENESS_WRITER'), { status: 409 }),
  });
  await assert.rejects(c.run(), e => e.code === 'WEEKLY_SOURCE_MODE_A_REFERENCE_APPLY_FAILED'
    && e.status === 409 && e.details.publication_id === 'actual-publication'
    && e.details.reason === 'AGENCY_INVOICE_USES_COMPLETENESS_WRITER');
});
test('UNIT_ONLY not-dispatched ordinary upload preserves the existing null continuation', async () => {
  const c = caller({ ok: true, dispatched: false });
  assert.equal(await c.run(), null);
  assert.equal(c.calls.length, 1);
});

// Reference model for the existing native COMPLETE Candidate/Client boundary.
// The predicate results are explicit UNIT inputs, not native qualification.
function units(rows) {
  const result = new Map();
  for (const r of rows) {
    const key = r.candidate_id + ':' + r.client_id;
    result.set(key, { has_preset: (result.get(key)?.has_preset || r.native_check_only || r.native_configured) === true });
  }
  return result;
}
function ready(decisions, rows) {
  const u = units(rows);
  return decisions.filter(d => d.selected && d.selectable && !d.blocking
    && !u.get(d.candidate_id + ':' + d.client_id)?.has_preset
    && !decisions.some(b => b.blocking && b.candidate_id === d.candidate_id && b.client_id === d.client_id)).map(d => d.action_id).sort();
}
const decision = (action_id, candidate_id, client_id, extra = {}) => ({ action_id, candidate_id, client_id, selected: true, selectable: true, blocking: false, ...extra });
test('UNIT_ONLY actual complete-unit model: mixed Clients retain ordinary native action IDs exactly', () => {
  assert.deepEqual(ready([decision('a', 'c1', 'preset'), decision('b', 'c2', 'ordinary')], [
    { candidate_id: 'c1', client_id: 'preset', native_check_only: true },
    { candidate_id: 'c2', client_id: 'ordinary', native_check_only: false, native_configured: false },
  ]), ['b']);
});
test('UNIT_ONLY actual complete-unit model: same Client different Candidates does not skip the Client', () => {
  assert.deepEqual(ready([decision('a', 'c1', 'same'), decision('b', 'c2', 'same')], [
    { candidate_id: 'c1', client_id: 'same', native_configured: true },
    { candidate_id: 'c2', client_id: 'same', native_configured: false },
  ]), ['b']);
});
test('UNIT_ONLY a same Candidate/Client unit straddling dates remains whole, never a fabricated subset envelope', () => {
  assert.deepEqual(ready([decision('old', 'c1', 'same'), decision('new', 'c1', 'same')], [
    { candidate_id: 'c1', client_id: 'same', native_configured: false },
    { candidate_id: 'c1', client_id: 'same', native_configured: true },
  ]), []);
});
test('UNIT_ONLY existing unresolved/blocked candidate unit does not freeze unrelated ordinary unit', () => {
  assert.deepEqual(ready([decision('a', 'c1', 'same'), decision('blocker', 'c1', 'same', { blocking: true }), decision('b', 'c2', 'same')], []), ['b']);
});
test('UNIT_ONLY pre-T0 configured preset is excluded without inventing a Timesheet', () => {
  assert.deepEqual(ready([decision('a', 'c1', 'same')], [{ candidate_id: 'c1', client_id: 'same', timesheet_id: null, native_configured: true }]), []);
});
test('UNIT_ONLY native census calls the exact existing preview/configured authorities and has no readiness recursion', () => {
  const helper = read('20_native_mode_a_unit_census.sqlinc');
  assert.match(helper, /preview:=public\.hr_weekly_validation_preview\(hi\.id\)/);
  assert.match(helper, /private\.weekly_source_configured_agency_invoice_v1\(n\.client,g\.id,g\.agency_id,g\.environment,n\.own_week\)/);
  assert.doesNotMatch(helper, /_import_review_ready_action|apply_envelope|set_config|insert into|update public|delete from/i);
  assert.match(helper, /revoke all .* from public,anon,authenticated,service_role/);
});
test('UNIT_ONLY existing native server envelope/hash and selection comparison are not reconstructed by caller', () => {
  const ref = read('23_complete_native_reference_apply_with_preset_row_refusal.sqlinc');
  assert.match(ref, /v_envelope:=public\._import_review_apply_envelope_core_v1\(v_import\.id\)/);
  assert.match(ref, /v_request_hash:=public\._import_review_hash_v1\(v_envelope::text\)/);
  assert.match(ref, /'review_selected_action_ids',coalesce\(v_envelope->'selected_action_ids'/);
  assert.doesNotMatch(ref, /update public\.import_review_decisions|set selected=/i);
});
test('UNIT_ONLY preset refusal precedes any reference manifest or auto-authorisation policy read', () => {
  const ref = read('23_complete_native_reference_apply_with_preset_row_refusal.sqlinc');
  const refusal = ref.indexOf("'AGENCY_INVOICE_USES_COMPLETENESS_WRITER'");
  assert.ok(refusal < ref.indexOf('v_manifest_hash:='));
  assert.ok(refusal < ref.indexOf('public.import_auto_authorise_policy_resolve_v2'));
});
test('UNIT_ONLY final native unit refusal runs before Mode A TV/mirror/email/auto-authorisation steps', () => {
  const hr = read('22_complete_native_mode_a_with_selected_preset_refusal.sqlinc');
  const final = hr.indexOf('Recheck the actual guard-admitted complete units');
  assert.ok(final > hr.indexOf('public.import_review_apply_guard_v1('));
  assert.ok(final < hr.indexOf('10) MODE_A weekly validation upserts'));
  assert.ok(final < hr.indexOf('v_auto_authorise_timesheet_ids:=public._import_review_auto_authorise_targets_core_v1('));
  assert.equal((hr.match(/raise exception 'AGENCY_INVOICE_USES_COMPLETENESS_WRITER'/g) || []).length, 2);
});
test('UNIT_ONLY genuine bridge/comparison write precedes sole validation continuation', () => {
  const d = read('24_complete_native_dispatch_with_separate_completeness_continuation.sqlinc');
  assert.ok(d.indexOf('private.weekly_source_agency_publication_validation_v1(v_publication_id)') > d.indexOf('insert into public.weekly_timesheet_source_comparisons('));
  assert.doesNotMatch(d, /hr_weekly_apply_transactional|timesheets_authorise|email_jobs|timesheet_validations/);
});
