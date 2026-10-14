import { readFileSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { createHash } from 'node:crypto';
import { spawnSync } from 'node:child_process';

const here = import.meta.dirname;
const lane = resolve(here, '../../../..');
const input = resolve(lane, '../healthroster-v2-backend-13102026');
const sha = bytes => createHash('sha256').update(bytes).digest('hex');
const nativePath = 'broker/src/timesheet-correction-orchestrator.mjs';
const nativeBytes = readFileSync(resolve(input, nativePath));
const nativeSha = '637c3b206b8e6700ce40e6ea9e5209bab8fb1b0702322375505a9750f1989f23';
if (sha(nativeBytes) !== nativeSha) throw new Error('Current controlling orchestrator moved; re-read before rebuilding proposal');
let proposed = nativeBytes.toString('utf8');
function replaceOnce(before, after) {
  if (proposed.split(before).length !== 2) throw new Error(`Source hunk not unique: ${before.slice(0,100)}`);
  proposed = proposed.replace(before, after);
}
replaceOnce("const HASH = /^[0-9a-f]{64}$/;", "const HASH = /^[0-9a-f]{64}$/;\nconst CORRECTION_ID = /^awc:([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}):([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$/;");
replaceOnce("function positive(value) { return typeof value === 'number' && Number.isSafeInteger(value) && value > 0; }", "function positive(value) { return typeof value === 'number' && Number.isSafeInteger(value) && value > 0; }\nfunction correctionIdentity(value, requestId, rootId = null) {\n  const match = typeof value === 'string' ? CORRECTION_ID.exec(value) : null;\n  if (!match || !sameUuid(match[2], requestId) || (rootId !== null && !sameUuid(match[1], rootId))) {\n    fail('CORRECTION_RECEIPT_IDENTITY_MISMATCH');\n  }\n  return value;\n}");
replaceOnce("['request_id', 'correction_id', 'reversed_timesheet_id', 'reversal_timesheet_id', 'replacement_timesheet_id',", "['request_id', 'reversed_timesheet_id', 'reversal_timesheet_id', 'replacement_timesheet_id',");
replaceOnce("'original_financial_id', 'reversal_financial_id', 'replacement_financial_id']) uuid(r[k], 'CORRECTION_RECEIPT_INVALID');", "'original_financial_id', 'reversal_financial_id', 'replacement_financial_id']) uuid(r[k], 'CORRECTION_RECEIPT_INVALID');\n  correctionIdentity(r.correction_id, r.request_id, prepare?.authority?.root_timesheet_id ?? null);");
replaceOnce("if (!positive(a.original_timesheet_version) || !positive(a.replacement_version) || !positive(a.target_revision) || a.proposal_revision !== 1 ||", "if (!positive(a.original_timesheet_version) || !positive(a.replacement_version) || typeof a.target_revision !== 'string' || !HASH.test(a.target_revision) || a.proposal_revision !== 1 ||");
replaceOnce("const p0 = object(value.replacement_timesheet, 'CORRECTION_PREPARE_AUTHORITY_INVALID');", "const p0 = object(value.replacement_timesheet, 'CORRECTION_PREPARE_AUTHORITY_INVALID');\n  correctionIdentity(p0.correction_id, payload.request_id, a.root_timesheet_id);");
replaceOnce("typeof r.replacement_booking_id !== 'string' || !r.replacement_booking_id.trim()))) {", "typeof r.replacement_booking_id !== 'string' || !r.replacement_booking_id.trim())) ||\n        (OWN(r, 'target_revision') && (typeof r.target_revision !== 'string' || !HASH.test(r.target_revision))) ||\n        (OWN(r, 'proposal_revision') && r.proposal_revision !== 1)) {");
replaceOnce("r.state_revision !== 1 || r.proposal_revision !== 1 || !positive(r.target_revision) ||", "r.state_revision !== 1 || r.proposal_revision !== 1 || typeof r.target_revision !== 'string' || !HASH.test(r.target_revision) ||");
writeFileSync(resolve(here, 'timesheet-correction-orchestrator.mjs'), proposed);

const testPath = 'tests/timesheet-correction-orchestrator.test.mjs';
const testBytes = readFileSync(resolve(input, testPath));
let tests = testBytes.toString('utf8');
tests = tests.replace("'../broker/src/timesheet-correction-orchestrator.mjs'", "'../docs/healthroster-build/proposals/f4-ai/timesheet-correction-orchestrator.mjs'");
tests = tests.replace('correction: id(12)', "correction: `awc:${id(8)}:${id(1)}`");
tests = tests.replaceAll('target_revision: 2', 'target_revision: H');
tests = tests.replace('correction: id(26)', "correction: `awc:${I.root}:${id(21)}`");
tests = tests.replace("replacement_signed_witness: { workflow_id: id(16), workflow_generation: 2, approval_id: id(17), review_manifest_hash: H }", "replacement_signed_witness: { workflow_id: id(16), workflow_generation: 2, approval_request_id: id(17),\n        hours_component_id: id(18), candidate_component_id: id(19), manager_component_id: id(20),\n        candidate_sha256: H, manager_sha256: FH, manager_approved_at_utc: '2026-10-13T11:20:00Z', document_version_id: null }");
tests += `
test('native digest target and request-bound awc text work for the unsigned agency origin without manager evidence invention', async () => {
  const p = prepareFixture(); p.authority.origin = 'AGENCY_INVOICE_CORRECTION'; p.authority.replacement_signed_witness = null;
  const h = harness({ prepare: p });
  const receipt = await h.adapter.authorise(action(), {});
  assert.equal(receipt.correction_id, I.correction);
  assert.equal(h.calls.filter(x => x[0] === 'timesheet_correction_authorise_atomic_v1').length, 1);
});

test('numeric, malformed, coercible and unknown-proposal witnesses refuse before canonical production or mutation', async () => {
  for (const value of [2, '2', H.toUpperCase(), H.slice(1), null, {}, [H]]) {
    const p = prepareFixture(); p.authority.target_revision = value;
    const h = harness({ prepare: p });
    await rejects(() => h.adapter.authorise(action(), {}), 'CORRECTION_PREPARE_AUTHORITY_INVALID');
    assert.deepEqual(h.calls.map(x => x[0]), ['actor','timesheet_correction_prepare_v1']);
  }
  for (const value of [0, 2, '1']) {
    const p = prepareFixture(); p.authority.proposal_revision = value;
    await rejects(() => harness({ prepare: p }).adapter.authorise(action(), {}), 'CORRECTION_PREPARE_AUTHORITY_INVALID');
  }
});

test('prepared P0 correction identity must name this request and exact immutable root', async () => {
  for (const correction_id of [id(12), 'awc:' + I.root + ':' + id(90), 'awc:' + id(90) + ':' + I.request]) {
    const p = prepareFixture(); p.replacement_timesheet.correction_id = correction_id;
    const h = harness({ prepare: p });
    await rejects(() => h.adapter.authorise(action(), {}), 'CORRECTION_RECEIPT_IDENTITY_MISMATCH');
    assert.deepEqual(h.calls.map(x => x[0]), ['actor','timesheet_correction_prepare_v1']);
  }
});

test('APPLIED replay rejects a fabricated UUID or another request awc identity without rebuilding finance', async () => {
  for (const correction_id of [id(12), 'awc:' + I.root + ':' + id(90)]) {
    const r = appliedFixture({ replayed: true, correction_id }); delete r.authorisation;
    const h = harness({ prepare: r });
    await rejects(() => h.adapter.authorise(action(), {}), 'CORRECTION_RECEIPT_IDENTITY_MISMATCH');
    assert.deepEqual(h.calls.map(x => x[0]), ['actor','timesheet_correction_prepare_v1']);
  }
});

test('SEND rejects fabricated numeric target and changed immutable proposal revision, including supplied terminal replay witnesses', async () => {
  const payload = { timesheet_id: I.original, expected_version: 7, request_key: 'native-shape-send', reason: 'Correct hours', proposal: { schedule: [] } };
  for (const fields of [{ target_revision: 2 }, { target_revision: [H] }, { proposal_revision: 2 },
    { replayed: true, state: 'APPLIED', state_revision: 2, target_revision: 2 },
    { replayed: true, state: 'APPLIED', state_revision: 2, proposal_revision: 2 }]) {
    const h = harness({ rpc: () => ({ ok:true, request_id:I.request, state:'WAITING', state_revision:1,
      replacement_timesheet_id:I.p0, replacement_booking_id:'bk_awc_exact_p0', target_revision:H, proposal_revision:1, ...fields }) });
    await rejects(() => h.adapter.send(payload, {}), 'CORRECTION_SEND_RECEIPT_INVALID');
    assert.equal(h.calls.length, 2);
  }
});
`;
writeFileSync(resolve(lane, 'tests/healthroster-f4-ai-correction-wire.test.mjs'), tests);

const diff = spawnSync('git', ['diff', '--no-index', '--', resolve(input, nativePath), resolve(here, 'timesheet-correction-orchestrator.mjs')], { encoding: 'utf8', windowsHide: true });
if (![0,1].includes(diff.status)) throw new Error('Could not create read-only source diff');
const lines = diff.stdout.split('\n');
const firstHunk = lines.findIndex(line => line.startsWith('@@'));
writeFileSync(resolve(here, 'CN_WIRE_ROOT.diff'), `--- a/${nativePath}\n+++ b/${nativePath}\n${lines.slice(firstHunk).join('\n')}`);

const otherInputs = [
  'supabase/repeatable/13102026_1127_healthroster_correction_requests/13102026_1127_request.sqlinc',
  'supabase/repeatable/13102026_1127_healthroster_correction_requests/13102026_1127_approval.sqlinc',
  'supabase/repeatable/13102026_1127_healthroster_correction_requests/13102026_1127_cancel.sqlinc',
  'supabase/repeatable/13102026_1127_healthroster_correction_requests/13102026_1127_draft.sqlinc',
  'supabase/repeatable/13102026_1127_healthroster_correction_requests/13102026_1127_context.sqlinc',
  'supabase/repeatable/13102026_1127_healthroster_correction_requests/13102026_1127_transition.sqlinc',
  'supabase/repeatable/13102026_1649_healthroster_correction_request_read_authority_v1.sql',
  'broker/src/candidate-correction-v1.js',
  'docs/healthroster-build/CORRECTION_CONTRACT.md',
];
const guard = { contract: 'HEALTHROSTER_F4_AI_CN_WIRE_PROPOSAL_INPUT_V1',
  source_only: true, source_worktree: 'healthroster-v2-backend-13102026',
  controlling_module: { path: nativePath, sha256: nativeSha },
  controlling_tests: { path: testPath, sha256: sha(testBytes), retained_existing_cases: 32 },
  other_inputs: otherInputs.map(path => ({ path, sha256: sha(readFileSync(resolve(input, path))) })),
  outputs: [
    { path: 'docs/healthroster-build/proposals/f4-ai/timesheet-correction-orchestrator.mjs', sha256: sha(Buffer.from(proposed)) },
    { path: 'tests/healthroster-f4-ai-correction-wire.test.mjs', sha256: sha(Buffer.from(tests)) },
  ],
  preserved_gates: ['Current1649 exact decimal-text request authority decoder', 'Missing-A0 original normalisation hold',
    'Native snapshot/booking/provenance/CAS/replay and complete-unit validation', 'Ordinary lifecycle delegation',
    'No SQL/Git/network/runtime mutation; root integration and native journeys remain required'],
};
writeFileSync(resolve(here, 'CN_WIRE_SOURCE_GUARD.json'), `${JSON.stringify(guard, null, 2)}\n`);
process.stdout.write('Complete CN wire proposal and native-shaped 32-case successor saved; root runtime untouched.\n');
