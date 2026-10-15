import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import vm from 'node:vm';
import assert from 'node:assert/strict';
import test from 'node:test';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { functionEvents } from './reference/pinned-source-parser.mjs';
import { validateFrozenTimesheetPresentationModel } from './forward/112_complete_worker_only_presentation_contract.js';
import { buildOfficialWeekPeriod, officialTimesheetNumber, renderOfficialTimesheetPdfBytes } from './forward/113_complete_worker_only_official_timesheet_pdf.js';

const here = path.dirname(fileURLToPath(import.meta.url));
const read = name => fs.readFileSync(path.join(here, name), 'utf8');
const sha = value => crypto.createHash('sha256').update(value).digest('hex');
const proof = JSON.parse(read('COMPLETE_REVERSES.json'));
for (const owner of proof.owners) test('SOURCE_ONLY complete actual owner reverse ' + owner.file, () => {
  let text = read('forward/' + owner.file);
  assert.equal(sha(text), owner.after_sha256);
  for (const hunk of [...owner.hunks].reverse()) {
    assert.equal(text.split(hunk.after).length, 2, hunk.purpose);
    text = text.replace(hunk.after, () => hunk.before);
  }
  assert.equal(text, read('before/' + owner.file));
  assert.equal(sha(text), owner.before_sha256);
  if (owner.file.endsWith('.sqlinc')) {
    const before = functionEvents(text), after = functionEvents(read('forward/' + owner.file));
    assert.deepEqual(after.events.map(event => event.id), before.events.map(event => event.id));
    assert.equal(after.events.length, 1);
    assert.deepEqual(after.unresolved, []);
  } else execFileSync(process.execPath, ['--check', path.join(here, 'forward', owner.file)]);
});
for (const input of proof.inputs) test('SOURCE_ONLY exact native input pin ' + input.reference, () => {
  const raw = execFileSync('git', ['show', `${input.pin}:${input.path}`], { maxBuffer: 16_000_000 });
  assert.equal(raw.length, input.raw_bytes);
  assert.equal(sha(raw), input.raw_sha256);
  assert.equal(sha(read(input.reference)), input.selected_definition_sha256);
  if (input.id) assert.ok(functionEvents(raw.toString('utf8')).events.some(event =>
    event.id === input.id && sha(event.definition) === input.selected_definition_sha256));
  else assert.equal(raw.toString('utf8'), read(input.reference));
});
const transition = read('forward/108_complete_worker_only_candidate_transition.sqlinc');
const render = read('forward/109_complete_worker_only_component_render_contract.sqlinc');
const finaliser = read('forward/110_complete_worker_only_candidate_finaliser.sqlinc');
const backend = read('forward/111_complete_worker_only_candidate_backend.js');

test('SOURCE_ONLY worker-only scope is exact WEEKLY CONTRACT_HOURS ELECTRONIC with strict false for absent policy', () => {
  for (const text of [transition, render, finaliser]) {
    assert.ok(text.includes("v_workflow.scope='WEEKLY'"));
    assert.ok(text.includes("v_workflow.workflow_kind='CONTRACT_HOURS'"));
    assert.ok(text.includes("v_workflow.route='ELECTRONIC'"));
    assert.ok(text.includes("'\"NOT_REQUIRED\"'::jsonb,false)"));
  }
  assert.ok(transition.includes("v_policy->>'policy_fingerprint'=v_workflow.policy_snapshot_json->>'policy_fingerprint'"));
  assert.ok(transition.indexOf('CANDIDATE_MANAGER_APPROVAL_NOT_REQUIRED') < transition.indexOf('select coalesce(max(request_generation),0)+1'));
});
test('SOURCE_ONLY genuine document render readiness precedes READY_TO_FINALISE without a fabricated manager claim', () => {
  assert.ok(transition.indexOf('CANDIDATE_WORKER_ONLY_DOCUMENT_INVALID') < transition.indexOf('final_signed_storage_key=review_storage_key'));
  assert.ok(transition.indexOf('final_signed_storage_key=review_storage_key') < transition.indexOf("state=case when v_worker_only_approval then 'READY_TO_FINALISE'"));
  assert.ok(transition.includes("v_receipt->>'candidate_signature_sha256' is distinct from encode(v_workflow.candidate_signature_sha256,'hex')"));
  assert.ok(render.includes("if v_phase='FINAL' and not v_worker_only_approval then"));
  assert.ok(render.includes("case when v_worker_only_approval then jsonb_build_object('worker_only_approval',true) else '{}'::jsonb end"));
});
test('SOURCE_ONLY finaliser keeps mandatory Candidate signature, policy freshness, native HOURS admission and Office authorisation', () => {
  assert.ok(finaliser.includes('v_candidate_signature.source_content_sha256 is distinct from v_workflow.candidate_signature_sha256'));
  assert.ok(finaliser.includes("v_current_policy->>'policy_fingerprint'"));
  assert.ok(finaliser.includes("v_worker_only_final_state->>'expected_line_type' is distinct from 'HOURS'"));
  assert.ok(finaliser.includes("v_worker_only_final_state->>'record_role'='IMPORT_HOURS'"));
  assert.ok(finaliser.includes("and v_workflow.route<>'PAPER' and not v_worker_only_approval"));
  const context = finaliser.indexOf("perform set_config('cloudtms.candidate_electronic_finalise',v_workflow.id::text");
  assert.ok(context < finaliser.indexOf('v_worker_only_final_state:=private._candidate_weekly_final_state_guard_v1'));
  assert.ok(finaliser.indexOf('CANDIDATE_FIXED_APPROVAL_HOURS_SCOPE_INVALID') < finaliser.indexOf('v_hours_result:=public.contract_week_manual_upsert_atomic'));
  assert.equal(finaliser.match(/set_config\(/g)?.length,
    read('before/110_complete_worker_only_candidate_finaliser.sqlinc').match(/set_config\(/g)?.length);
});
test('SOURCE_ONLY service identity retains actual policy, Candidate, manifest and replay binding; no new public signatures', () => {
  for (const name of ['candidate_signature_sha256_hex', 'policy_snapshot_sha256_hex', 'review_manifest_sha256_hex']) {
    assert.ok(finaliser.includes(name) && backend.includes(name));
  }
  for (const unchanged of ['WORKFLOW_VERSION_MISMATCH','CANDIDATE_IDEMPOTENCY_CONFLICT','CANDIDATE_IMMUTABLE_SUBMISSION_MISMATCH']) assert.ok(finaliser.includes(unchanged));
  assert.ok(finaliser.includes("approval.workflow_generation=v_workflow.generation and approval.state in ('PENDING','APPROVED')"));
  assert.deepEqual(proof.new_signatures, []);
  assert.deepEqual(proof.new_tables, []);
  assert.equal(proof.native, false);
  assert.ok(!backend.includes("'candidate_submission_finalize_atomic_v1'"));
  // The current verifier selects public SECURITY DEFINER identities by the
  // native %candidate% predicate; it does not enumerate function names.
  // Our three SQL owners replace those same signatures and add no public RPC.
  assert.ok(read('reference/actual_candidate_named_security_verifier.sql').includes("n.nspname='public' and p.prosecdef and p.proname ilike '%candidate%'"));
});

function actualFinaliser(workflow, approved = null) {
  const calls = [];
  const source = backend.slice(backend.indexOf('async function finaliseWorkflow('),
    backend.indexOf('\nexport async function recoverPendingCandidateManagerFinalisations('));
  class CandidateHttpError extends Error { constructor(status, code) { super(code); this.code = code; this.status = status; } }
  const context = {
    requireCandidateIdempotency: value => value,
    workflowRow: async () => workflow,
    probeFinalisationReplayByKey: async () => null,
    probeFinalisationReplay: async (...args) => { calls.push({ kind: 'replay', service: args[5] }); return null; },
    parseJson: (value, fallback) => typeof value === 'string' ? JSON.parse(value) : value ?? fallback,
    upper: value => String(value || '').toUpperCase(), text: value => String(value ?? ''),
    restOne: async (...args) => { calls.push({ kind: 'approval-read', args }); return approved; },
    environmentName: () => 'TEST', lifecycleSignature: async () => 'native-row-signature',
    safeFinalisationResult: value => value, CandidateHttpError,
    MANAGER_FINALISATION_RPC_TIMEOUT_MS: 20_000,
    rpcCall: async (_deps, name, payload, options) => { calls.push({ kind: 'rpc', name, payload, options }); return { ok: true, state: 'FINALISED' }; },
    officeAdapter: async () => { throw new Error('unexpected Office delegation'); }
  };
  return { run: vm.runInNewContext(`(${source})`, context), calls };
}
const workflow = {
  id: '00000000-0000-4000-8000-000000000101', generation: 3,
  scope: 'WEEKLY', workflow_kind: 'CONTRACT_HOURS', route: 'ELECTRONIC',
  policy_snapshot_json: { manager_approval_policy: { approval: 'NOT_REQUIRED' } },
  candidate_signature_sha256: '\\x' + 'a'.repeat(64),
  policy_snapshot_sha256: '\\x' + 'b'.repeat(64), review_manifest_sha256: '\\x' + 'c'.repeat(64)
};
test('ACTUAL_JS_UNIT worker-only finalisation uses only the bounded non-waiting wrapper and frozen fixture identity', async () => {
  const actual = actualFinaliser(workflow);
  assert.equal((await actual.run({}, {}, workflow.id, 3, 'worker-only-test-00000001')).ok, true);
  assert.equal(actual.calls.filter(call => call.kind === 'approval-read').length, 0);
  const rpc = actual.calls.find(call => call.kind === 'rpc');
  assert.equal(rpc.name, 'candidate_submission_finalize_single_flight_v1');
  assert.equal(rpc.options.timeoutMs, 20_000);
  assert.equal(rpc.payload.p_expected_row_signature, 'native-row-signature');
  const identity = rpc.payload.p_daily_materialisation_json.service_finalisation.finalisation_identity;
  assert.equal(identity.approval_method, 'ELECTRONIC');
  assert.equal(identity.approval_request_id, null);
  assert.equal(identity.candidate_signature_sha256_hex, 'a'.repeat(64));
  assert.equal(identity.policy_snapshot_sha256_hex, 'b'.repeat(64));
  assert.equal(identity.review_manifest_sha256_hex, 'c'.repeat(64));
});
for (const [name, overrides] of [
  ['DAILY', { scope: 'DAILY', workflow_kind: 'DAILY' }],
  ['expense', { workflow_kind: 'CONTRACT_EXPENSE' }],
  ['combined', { workflow_kind: 'CONTRACT_COMBINED' }],
  ['ordinary policy', { policy_snapshot_json: { manager_approval_policy: {} } }]
]) test('ACTUAL_JS_UNIT ' + name + ' retains manager approval despite a copied fixed key elsewhere', async () => {
  const actual = actualFinaliser({ ...workflow, ...overrides });
  await assert.rejects(actual.run({}, {}, workflow.id, 3, 'worker-only-test-00000001'), error => error.code === 'FINAL_SIGNED_DOCUMENT_NOT_READY');
  assert.equal(actual.calls.filter(call => call.kind === 'approval-read').length, 1);
  assert.equal(actual.calls.filter(call => call.kind === 'rpc').length, 0);
});
test('ACTUAL_JS_UNIT ordinary PHONE manager finalisation preserves its approval fixture identity', async () => {
  const approved = { id: '00000000-0000-4000-8000-000000000201', request_generation: 2, method: 'PHONE', review_manifest_sha256: '\\x' + 'd'.repeat(64) };
  const actual = actualFinaliser({ ...workflow, route: 'PHONE', policy_snapshot_json: { manager_approval_policy: {} } }, approved);
  await actual.run({}, {}, workflow.id, 3, 'manager-test-00000000001');
  const identity = actual.calls.find(call => call.kind === 'rpc').payload.p_daily_materialisation_json.service_finalisation.finalisation_identity;
  assert.equal(identity.approval_method, 'PHONE');
  assert.equal(identity.approval_request_id, approved.id);
  assert.equal(identity.approval_request_generation, 2);
  assert.equal(identity.review_manifest_sha256_hex, 'd'.repeat(64));
  assert.equal(Object.hasOwn(identity, 'candidate_signature_sha256_hex'), false);
});

const currentTests = read('reference/actual_official_pdf_tests.js');
const fixture = vm.runInNewContext(`(() => {${currentTests.slice(currentTests.indexOf('const TIMESHEET_ID ='), currentTests.indexOf("test('official week period"))}; return fixture;})()`,
  { buildOfficialWeekPeriod, officialTimesheetNumber });
const signature = { r2_key: 'candidate/test/signed.png', sha256: 'a'.repeat(64), media_type: 'image/png', size_bytes: 68 };
const assets = { candidate_signature: { data_url: 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Y9ZP8sAAAAASUVORK5CYII=' } };
test('ACTUAL_PDF worker-only official page contains only its Candidate declaration and immutable fixture signature', async () => {
  const output = process.env.CANDIDATE_WORKER_ONLY_PDF_PROOF_DIR;
  assert.ok(output && path.isAbsolute(output), 'Use an explicit ignored local proof directory');
  const model = fixture({ form_variant: 'ELECTRONIC_WORKER_SIGNED', signatures: { candidate: signature, authoriser: {} } });
  assert.equal(validateFrozenTimesheetPresentationModel(model), model);
  const rendered = await renderOfficialTimesheetPdfBytes(model, assets);
  assert.equal(rendered.page_count, 1);
  assert.equal(rendered.render_receipt.worker_signature_only, true);
  assert.equal(rendered.render_receipt.manager_signature_block_rendered, false);
  assert.equal(rendered.render_receipt.all_embedded_images_aspect_ratio_preserved, true);
  fs.mkdirSync(output, { recursive: true });
  fs.writeFileSync(path.join(output, 'worker-only.pdf'), rendered.pdf_bytes);
  const original = fixture({ form_variant: 'ELECTRONIC_MANAGER_REVIEW', signatures: { candidate: signature, authoriser: {} } });
  const normal = await renderOfficialTimesheetPdfBytes(original, assets);
  assert.equal(normal.page_count, 1);
  assert.equal(Object.hasOwn(normal.render_receipt, 'worker_signature_only'), false);
  fs.writeFileSync(path.join(output, 'manager-review.pdf'), normal.pdf_bytes);
});
test('ACTUAL_MODEL worker-only form rejects missing Candidate, any manager signature, authorisation and DAILY scope', () => {
  const base = fixture({ form_variant: 'ELECTRONIC_WORKER_SIGNED', signatures: { candidate: signature, authoriser: {} } });
  for (const invalid of [
    { signatures: { candidate: {}, authoriser: {} } },
    { signatures: { candidate: signature, authoriser: { ...signature, r2_key: 'manager.png' } } },
    { authorisation: { authorised: true, authorised_at_utc: '2026-10-15T00:00:00Z' } },
    { sheet_scope: 'DAILY' }
  ]) assert.throws(() => validateFrozenTimesheetPresentationModel({ ...base, ...invalid }));
});
