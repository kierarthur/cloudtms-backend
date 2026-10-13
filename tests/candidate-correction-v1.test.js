import assert from 'node:assert/strict';
import test from 'node:test';
import { readFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { PDFDocument, PDFRawStream, decodePDFRawStream } from 'pdf-lib';
import { candidateAppBackendInternals as core, handleCandidateAppRequest } from '../broker/src/candidate-app-backend.js';
import { correctionSchedule, serializeCandidateCorrection, serializeManagerCorrection, correctionPresentation,
  CORRECTION_CAPABILITY, CORRECTION_CAPABILITY_HEADER } from '../broker/src/candidate-correction-v1.js';
import { validateCandidateCorrectionV1, validateManagerCorrectionV1 } from '../broker/src/candidate-correction-v1.validator.js';
import { renderOfficialTimesheetPdfBytes } from '../broker/src/timesheet-official-pdf.js';
import { candidateBrokerInternals as broker, handleCandidateBrokerRequest } from '../candidate-broker/src/candidate-broker.js';

const id = value => `00000000-0000-4000-8000-${String(value).padStart(12, '0')}`;
const converters = { segmentBreak: core.segmentBreak, normaliseAdaptiveBreakEntry: core.normaliseAdaptiveBreakEntry };
const part = end => ({ date: '2026-09-06', segment_id: 'part-one', start: '08:00', end, break_minutes: 60 });
const projection = () => ({ schema_version: 1, request_id: id(1), correction_id: 'awc:source-fixture',
  origin: 'SIGNED_TIMESHEET_CORRECTION', state: 'WAITING', state_revision: 1, proposal_revision: 1,
  target_revision: 'a'.repeat(64), root_timesheet_id: id(2), reversed_timesheet_id: id(2),
  original_booking_id: 'original-business-booking', replacement_timesheet_id: id(4), replacement_booking_id: 'physical-p0-booking',
  reason: 'Please check the finish time.', changed_dates: ['2026-09-06'],
  original_schedule: [part('20:00')], proposed_schedule: [part('19:30')],
  draft_scope: { kind: 'TIMESHEET_CORRECTION', request_id: id(1), replacement_timesheet_id: id(4),
    allowed_timesheet_aliases: [id(4)], allowed_booking_aliases: ['physical-p0-booking'] },
  frozen_daily_receipt: { private_fact: 'must stay private' }, original_manager_approved_at_utc: '2026-09-07T12:00:00Z',
  frozen_daily_shift: { booking_id: 'original-business-booking', work_date: '2026-09-06', week_ending_date: '2026-09-06',
    hospital: 'Synthetic hospital', ward: 'Synthetic ward', job_title: 'Nurse', shift_type: 'Long day',
    shift_starts_at: '2026-09-06T07:00:00Z', shift_ends_at: '2026-09-06T19:00:00Z' } });
const capable = extra => ({ [CORRECTION_CAPABILITY_HEADER]: CORRECTION_CAPABILITY, ...extra });
const env = { CANDIDATE_APP_ENVIRONMENT: 'TEST', CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'source-fixture-only',
  SUPABASE_URL: 'https://source-fixture.invalid', SUPABASE_SERVICE_ROLE_KEY: 'source-fixture-only' };
const session = { id: id(20), session_id: id(20), account_id: id(21), selected_candidate_id: id(22),
  environment: 'TEST', status: 'ACTIVE', rotation: 1, expires_at_utc: '2099-01-01T00:00:00Z',
  absolute_expires_at_utc: '2099-01-02T00:00:00Z' };

test('purpose serializer closes internal facts, preserves aliases and does not mutate frozen raw source', () => {
  const raw = projection(); const before = structuredClone(raw);
  const output = serializeCandidateCorrection(raw, id(4), converters);
  assert.equal(validateCandidateCorrectionV1(output), true);
  assert.equal(output.original_schedule[0].break_entry.kind, 'DURATION_MINUTES');
  assert.equal(output.original_schedule[0].segment_id, 'part-one');
  assert.equal(output.original_schedule[0].start, '08:00');
  for (const key of ['frozen_daily_receipt', 'frozen_daily_shift', 'original_manager_approved_at_utc']) assert.equal(key in output, false);
  assert.deepEqual(raw, before);
});

test('changed dates come from normalized frozen parts, never the untrusted proposal label', () => {
  const raw = projection(); raw.changed_dates = ['2099-01-01'];
  assert.deepEqual(serializeCandidateCorrection(raw, id(4), converters).changed_dates, ['2026-09-06']);
  raw.proposed_schedule = structuredClone(raw.original_schedule);
  assert.deepEqual(serializeCandidateCorrection(raw, id(4), converters).changed_dates, []);
});

test('NONE, duration and overnight timed breaks use the existing canonical converters', () => {
  const examples = [
    { date: '2026-09-06', start: '22:00', end: '06:00', no_break: true, break_minutes: 0 },
    { date: '2026-09-06', start: '22:00', end: '06:00', break_minutes: 30 },
    { date: '2026-09-06', start: '22:00', end: '06:00', break_start: '23:45', break_end: '00:15', break_minutes: 30 }
  ];
  assert.deepEqual(correctionSchedule([examples[0]], converters)[0].break_entry, { kind: 'NO_BREAK', no_break: true, break_minutes: 0 });
  assert.deepEqual(correctionSchedule([examples[1]], converters)[0].break_entry, { kind: 'DURATION_MINUTES', break_minutes: 30 });
  assert.deepEqual(correctionSchedule([examples[2]], converters)[0].break_entry,
    { kind: 'START_END_TIMES', break_start: '23:45', break_end: '00:15', calculated_break_minutes: 30 });
  const iso = { ...examples[1], worked_start_iso: '2026-09-06T21:00:00Z', worked_end_iso: '2026-09-07T05:00:00Z' };
  assert.equal(correctionSchedule([iso], converters)[0].worked_end_iso, iso.worked_end_iso);
});

test('missing/contradictory breaks, part identities, UTC/local aliases and exact P0 aliases fail closed', () => {
  const mutations = [
    raw => { delete raw.original_schedule[0].break_minutes; },
    raw => { raw.proposed_schedule[0].break_start = '12:00'; },
    raw => { raw.proposed_schedule[0].break_start = '12:00'; raw.proposed_schedule[0].break_end = '12:30'; },
    raw => { raw.proposed_schedule[0].start_time = '09:00'; },
    raw => { raw.proposed_schedule[0].worked_start_iso = '2026-09-06T08:00:00Z'; },
    raw => { raw.original_schedule.push({ ...part('16:00') }); },
    raw => { delete raw.original_schedule[0].segment_id; raw.original_schedule.push({ ...part('16:00'), segment_id: undefined }); },
    raw => { raw.draft_scope.allowed_booking_aliases.push(raw.original_booking_id); },
    raw => { raw.draft_scope.request_id = id(99); },
    raw => { raw.state = 'CANCELLED'; },
    raw => { raw.proposal_revision = 2; }
  ];
  for (const mutate of mutations) { const raw = projection(); mutate(raw); assert.throws(() => serializeCandidateCorrection(raw, id(4), converters)); }
});

test('real authorized detail uses selected Candidate and returned physical P0, returns frozen Daily outside Rota', async () => {
  const token = await core.createAccessToken(env, session); const previousFetch = globalThis.fetch;
  const calls = []; const raw = projection();
  globalThis.fetch = async input => { assert.match(String(input), /candidate_app_sessions/); return Response.json([session]); };
  const deps = { routeAudience: 'PRIVATE', async rpc(name, args) {
    calls.push({ name, args });
    if (name === 'candidate_app_timesheet_detail_v2') return { timesheet: { id: id(4), sheet_scope: 'DAILY' }, hours: { total_hours: 0 },
      components: [], workflows: [], capabilities: {}, daily_shift: { booking_id: 'stale-rota', work_date: '2026-10-13' } };
    if (name === 'candidate_expense_component_projection_v1') return null;
    if (name === 'timesheet_correction_request_projection_v1') return structuredClone(raw);
    throw new Error(`Unexpected dependency ${name}`);
  } };
  try {
    const request = headers => new Request(`https://private.source.invalid/candidate-app/v1/timesheets/${id(4)}`, { headers: { authorization: `Bearer ${token}`, ...headers } });
    const old = await handleCandidateAppRequest(request({}), env, {}, deps);
    assert.equal(old.status, 409); assert.equal((await old.json()).error_code, 'CANDIDATE_CORRECTION_CLIENT_REQUIRED');
    const response = await handleCandidateAppRequest(request(capable()), env, {}, deps);
    assert.equal(response.status, 200); const body = await response.json();
    assert.equal(validateCandidateCorrectionV1(body.correction), true);
    assert.deepEqual(body.daily_shift, raw.frozen_daily_shift);
    assert.equal(JSON.stringify(body).includes('must stay private'), false);
    const projectionCall = calls.find(call => call.name === 'timesheet_correction_request_projection_v1');
    assert.deepEqual(projectionCall.args, { p_timesheet_id: id(4), p_candidate_id: session.selected_candidate_id });
    raw.frozen_daily_shift.work_date = null;
    assert.equal((await handleCandidateAppRequest(request(capable()), env, {}, deps)).status, 409);
  } finally { globalThis.fetch = previousFetch; }
});

test('capability never substitutes for Candidate authentication, and missing/foreign workflows keep SQL admission', async () => {
  const calls = [];
  const response = await handleCandidateAppRequest(new Request(`https://private.source.invalid/candidate-app/v1/timesheets/${id(4)}`,
    { headers: capable() }), env, {}, { routeAudience: 'PRIVATE', async rpc(name) { calls.push(name); return projection(); } });
  assert.equal(response.status, 401); assert.deepEqual(calls, []);
});

test('real workflow route refuses legacy actionable P0 and cancelled P0 before existing mutation', async () => {
  const token = await core.createAccessToken(env, session); const previousFetch = globalThis.fetch; let mutationCalls = 0;
  globalThis.fetch = async () => Response.json([session]);
  const raw = projection(); const deps = { routeAudience: 'PRIVATE', async rpc(name, args) {
    if (name === 'timesheet_correction_workflow_projection_v1') {
      assert.deepEqual(args, { p_workflow_id: id(30), p_candidate_id: session.selected_candidate_id }); return structuredClone(raw);
    }
    mutationCalls++; return { ok: true };
  } };
  try {
    const request = headers => new Request(`https://private.source.invalid/candidate-app/v1/workflows/${id(30)}/actions/cancel`,
      { method: 'POST', headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json', ...headers },
        body: JSON.stringify({ generation: 1, idempotency_key: 'source-correction-cancel', reason_note: 'Source fixture' }) });
    assert.equal((await handleCandidateAppRequest(request({}), env, {}, deps)).status, 409);
    raw.state = 'CANCELLED'; raw.draft_scope.allowed_timesheet_aliases = []; raw.draft_scope.allowed_booking_aliases = [];
    assert.equal((await handleCandidateAppRequest(request(capable()), env, {}, deps)).status, 409);
    assert.equal(mutationCalls, 0);
  } finally { globalThis.fetch = previousFetch; }
});

test('legacy P0 component preparation and authenticated upload refuse before any storage or mutation', async () => {
  const fixtureEnv = { ...env, CANDIDATE_PRIVATE_UPLOAD_TOKEN_SECRET: 'source-upload-fixture-only' };
  const token = await core.createAccessToken(fixtureEnv, session); const previousFetch = globalThis.fetch;
  globalThis.fetch = async () => Response.json([session]);
  let mutationCalls = 0;
  const deps = { routeAudience: 'PRIVATE', async rpc(name) {
    if (name === 'timesheet_correction_workflow_projection_v1') return projection();
    mutationCalls++; throw new Error(`Unexpected mutation ${name}`);
  } };
  try {
    const prepare = await handleCandidateAppRequest(new Request(`https://private.source.invalid/candidate-app/v1/workflows/${id(30)}/components/prepare`, {
      method: 'POST', headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
      body: JSON.stringify({ generation: 1, component_kind: 'CANDIDATE_SIGNATURE', media_type: 'image/png', byte_size: 3, idempotency_key: 'source-prepare-key' })
    }), fixtureEnv, {}, deps);
    assert.equal(prepare.status, 409); assert.equal((await prepare.json()).error_code, 'CANDIDATE_CORRECTION_CLIENT_REQUIRED');
    const ticket = await core.uploadTicket(fixtureEnv, { owner: 'candidate', authority_kind: 'CANDIDATE_SESSION',
      workflow_id: id(30), candidate_account_id: session.account_id, candidate_id: session.selected_candidate_id,
      owner_id: session.session_id, key: 'source-fixture/unused.png', media_type: 'image/png', byte_size: 3 });
    const upload = await handleCandidateAppRequest(new Request(`https://private.source.invalid/candidate-app/v1/uploads/${encodeURIComponent(ticket)}`, {
      method: 'PUT', headers: { authorization: `Bearer ${token}`, 'content-type': 'image/png' }, body: new Uint8Array([1, 2, 3])
    }), fixtureEnv, {}, deps);
    assert.equal(upload.status, 409); assert.equal((await upload.json()).error_code, 'CANDIDATE_CORRECTION_CLIENT_REQUIRED');
    assert.equal(mutationCalls, 0);
  } finally { globalThis.fetch = previousFetch; }
});

test('weekly CREATE with only the exact contract week refuses a legacy P0 before creating a workflow', async () => {
  const token = await core.createAccessToken(env, session); const previousFetch = globalThis.fetch;
  globalThis.fetch = async () => Response.json([session]); let createCalls = 0;
  const raw = projection();
  const deps = { routeAudience: 'PRIVATE', async rpc(name, args) {
    if (name === 'timesheet_correction_create_projection_v1') {
      assert.deepEqual(args, { p_candidate_id: session.selected_candidate_id, p_contract_week_id: id(40),
        p_target_timesheet_id: null, p_anchor_timesheet_id: null }); return structuredClone(raw);
    }
    assert.equal(name, 'candidate_workflow_transition_atomic_v1'); assert.equal(args.p_action, 'CREATE');
    createCalls++; return { ok: true, workflow_id: id(30), policy: {} };
  } };
  const request = headers => new Request('https://private.source.invalid/candidate-app/v1/workflows', {
    method: 'POST', headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json', ...headers },
    body: JSON.stringify({ workflow_id: id(30), idempotency_key: 'source-create-key', workflow: {
      workflow_kind: 'CONTRACT_HOURS', scope: 'WEEKLY', route: 'ELECTRONIC', contract_week_id: id(40) } })
  });
  try {
    const old = await handleCandidateAppRequest(request({}), env, {}, deps);
    assert.equal(old.status, 409); assert.equal(createCalls, 0);
    assert.equal((await handleCandidateAppRequest(request(capable()), env, {}, deps)).status, 201);
    assert.equal(createCalls, 1);
    raw.state = 'CANCELLED'; raw.draft_scope.allowed_timesheet_aliases = []; raw.draft_scope.allowed_booking_aliases = [];
    assert.equal((await handleCandidateAppRequest(request(capable()), env, {}, deps)).status, 409);
    assert.equal(createCalls, 1);
  } finally { globalThis.fetch = previousFetch; }
});

const managerResult = () => ({ workflow_id: id(30), workflow_generation: 1, approval_request_id: id(31), manifest_sha256: 'b'.repeat(64),
  ordered_components: [{ component_id: id(32), component_kind: 'HOURS_TIMESHEET', content_sha256: 'c'.repeat(64) }] });
const managerRaw = () => ({ schema_version: 1, request_id: id(1), correction_id: 'awc:source-fixture', reason: projection().reason,
  original_schedule: projection().original_schedule, reviewed_schedule: [part('19:15')],
  review_witness: { workflow_id: id(30), workflow_generation: 1, approval_request_id: id(31), manifest_sha256: 'b'.repeat(64),
    hours_component_id: id(32), review_render_input_sha256: 'd'.repeat(64), review_content_sha256: 'c'.repeat(64) } });

test('manager summary uses final worker schedule and every exact frozen review witness, never Office proposal', () => {
  const output = serializeManagerCorrection(managerRaw(), managerResult(), converters);
  assert.equal(validateManagerCorrectionV1(output), true); assert.equal(output.reviewed_schedule[0].end_time, '19:15');
  for (const key of ['workflow_id', 'workflow_generation', 'approval_request_id', 'manifest_sha256', 'hours_component_id', 'review_content_sha256']) {
    const raw = managerRaw(); raw.review_witness[key] = key.includes('sha256') ? 'e'.repeat(64) : key.includes('generation') ? 2 : id(99);
    assert.throws(() => serializeManagerCorrection(raw, managerResult(), converters));
  }
});

test('manager read refuses cancelled/changed/amended context and summary differs from frozen rendered presentation', async () => {
  const raw = managerRaw(); const result = managerResult();
  const frozen = correctionPresentation(projection(), raw.reviewed_schedule, converters);
  const previousFetch = globalThis.fetch;
  globalThis.fetch = async input => { assert.match(String(input), /candidate_submission_workflows/);
    return Response.json([{ id: id(30), generation: 1, immutable_submission_json: { official_presentation: { correction: frozen } } }]); };
  const deps = { async rpc(name, args) {
    assert.equal(name, 'timesheet_correction_manager_summary_v1'); assert.equal(args.p_manifest_sha256, result.manifest_sha256); return raw;
  } };
  const request = new Request('https://source.invalid/manager', { headers: capable() });
  try {
    const output = await core.enrichManagerCorrection(deps, env, result, request);
    assert.equal(output.correction.reviewed_schedule[0].end_time, '19:15');
    raw.reviewed_schedule = [part('19:30')];
    await assert.rejects(core.enrichManagerCorrection(deps, env, result, request), error => error.code === 'MANAGER_REVIEW_MANIFEST_MISMATCH');
    await assert.rejects(core.enrichManagerCorrection({ rpc: async () => null }, env, result, request), error => error.code === 'MANAGER_REVIEW_MANIFEST_MISMATCH');
    await assert.rejects(core.enrichManagerCorrection({ rpc: async () => { throw new Error('CORRECTION_MANAGER_REVIEW_INPUT_CHANGED'); } }, env, result, request), error => error.code === 'MANAGER_REVIEW_MANIFEST_MISMATCH');
  } finally { globalThis.fetch = previousFetch; }
});

test('existing official model/renderer freezes before and final worker after, and rejects later schedule change', async () => {
  const signatureBytes = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aU3sAAAAASUVORK5CYII=', 'base64');
  const signatureHash = createHash('sha256').update(signatureBytes).digest('hex');
  const renderEnv = { ...env, R2: { async get() {
    return { arrayBuffer: async () => signatureBytes, httpMetadata: { contentType: 'image/png' } };
  } } };
  const raw = projection(); const workflow = { id: id(30), generation: 1, candidate_id: session.selected_candidate_id,
    workflow_kind: 'CONTRACT_HOURS', scope: 'WEEKLY', week_ending_date: '2026-09-06', target_timesheet_id: id(4),
    candidate_signature_component_id: id(50), candidate_signed_at_utc: '2026-10-13T12:00:00Z' };
  const previousFetch = globalThis.fetch;
  globalThis.fetch = async input => {
    const path = new URL(String(input)).pathname;
    if (path.endsWith('/timesheets')) return Response.json([{ timesheet_id: id(4), sheet_scope: 'WEEKLY', band: '6' }]);
    if (path.endsWith('/candidates')) return Response.json([{ first_name: 'Source', last_name: 'Fixture' }]);
    if (path.endsWith('/timesheets_financials')) return Response.json([]);
    if (path.endsWith('/settings_defaults')) return Response.json([{ agency_name: 'Source Fixture Agency', agency_logo: null }]);
    if (path.endsWith('/candidate_submission_components')) return Response.json([{ storage_key: 'source-fixture/signature.png', source_content_sha256: signatureHash }]);
    throw new Error(`Unexpected render read ${path}`);
  };
  try {
    const immutable = await core.prepareImmutableSubmission(renderEnv, { rpc: async name => {
      assert.equal(name, 'timesheet_correction_request_projection_v1'); return raw;
    }, buildWeeklySubmission: async () => ({ timesheet_patch_json: { actual_schedule_json: [part('19:15')] } }) }, workflow, { immutable_submission: {} }, 'source-fixture-key');
    assert.equal(immutable.official_presentation.correction.reviewed_schedule[0].end_time, '19:15');
    assert.equal(immutable.official_presentation.wording.header.lines.some(line => line.includes('19:30')), false);
    const state = { workflow: { ...workflow, immutable_submission_json: immutable }, timesheet: { timesheet_id: id(4), version: 1 }, candidate: {}, client: {}, contract: {} };
    const { model, assets } = await core.buildOfficialCandidateModel(renderEnv, {}, state, 'REVIEW');
    assert.equal(model.form_variant, 'ELECTRONIC_MANAGER_REVIEW');
    assert.equal(model.wording.header.lines.some(line => line.includes('20:00')), true);
    assert.equal(model.wording.header.lines.some(line => line.includes('19:15')), true);
    const rendered = await renderOfficialTimesheetPdfBytes(model, assets);
    const bytes = rendered.pdf_bytes;
    assert.equal((await PDFDocument.load(bytes)).getPageCount(), 1);
    const parsed = await PDFDocument.load(bytes);
    const pdf = parsed.context.enumerateIndirectObjects().filter(([, object]) => object instanceof PDFRawStream)
      .map(([, stream]) => Buffer.from(decodePDFRawStream(stream).decode()).toString('latin1')).join('\n');
    for (const text of ['Corrected Timesheet', 'Previously approved hours:', 'Submitted corrected hours:']) {
      assert.ok(pdf.includes(Buffer.from(text).toString('hex').toUpperCase()));
    }
    state.workflow.immutable_submission_json.timesheet_patch_json.actual_schedule_json = [part('19:00')];
    await assert.rejects(core.buildOfficialCandidateModel(renderEnv, {}, state, 'REVIEW'), error => error.code === 'MANAGER_REVIEW_MANIFEST_MISMATCH');
  } finally { globalThis.fetch = previousFetch; }
});

test('backend validator matches the copied generated MyTMS schema-closure receipt', async () => {
  const backend = await readFile(new URL('../broker/src/candidate-correction-v1.validator.js', import.meta.url));
  const receipt = JSON.parse(await readFile(new URL('../broker/src/candidate-correction-v1.contract.json', import.meta.url), 'utf8'));
  assert.equal(createHash('sha256').update(backend.toString('utf8').replace(/\r\n/g, '\n')).digest('hex'), receipt.validatorSha256);
  assert.ok(backend.toString('utf8').includes(receipt.closureSha256));
  assert.deepEqual(receipt.schemaNames, ['CandidateTimesheetCorrectionV1', 'ManagerCorrectionSummaryV1']);
});

test('public boundary preserves ordinary bytes and validates only the optional new fragment', async () => {
  const operation = { operation_id: 'getCandidateTimesheet' };
  const request = new Request('https://public.source.invalid/candidate-app/v1/timesheets/id', { headers: capable() });
  const ordinary = new Response('{ "timesheet" : null }\n', { headers: { 'content-type': 'application/json' } });
  assert.equal(await broker.correctionPrivateResponse(ordinary, operation, request), ordinary);
  assert.equal(await ordinary.text(), '{ "timesheet" : null }\n');
  const payload = { timesheet: { id: id(4) }, correction: serializeCandidateCorrection(projection(), id(4), converters) };
  const valid = Response.json(payload);
  assert.equal(await broker.correctionPrivateResponse(valid, operation, request), valid);
  payload.correction.frozen_daily_receipt = { private: true };
  await assert.rejects(broker.correctionPrivateResponse(Response.json(payload), operation, request), error => error.code === 'CANDIDATE_PRIVATE_API_UNAVAILABLE');
  delete payload.correction.frozen_daily_receipt;
  await assert.rejects(broker.correctionPrivateResponse(Response.json(payload), operation, new Request(request.url)), error => error.code === 'CANDIDATE_CORRECTION_CLIENT_REQUIRED');
});

test('public CORS and signed private forwarding preserve exact capability and reject unknown versions', async () => {
  const publicEnv = { CANDIDATE_APP_ENVIRONMENT: 'TEST', CANDIDATE_ALLOWED_ORIGINS: 'https://app.source.invalid',
    CANDIDATE_PRIVATE_SERVICE_SECRET: 'source-only-service-fixture-key', CANDIDATE_PRIVATE_SERVICE_KEY_ID: 'source-fixture',
    CLOUDTMS_PRIVATE: { async fetch(request) {
      assert.equal(request.headers.get(CORRECTION_CAPABILITY_HEADER), CORRECTION_CAPABILITY);
      assert.equal(request.headers.get('authorization'), 'Bearer source-placeholder');
      return Response.json({ timesheet: null });
    } } };
  const preflight = await handleCandidateBrokerRequest(new Request('https://public.source.invalid/candidate-app/v1/timesheets', {
    method: 'OPTIONS', headers: { origin: 'https://app.source.invalid', 'access-control-request-method': 'GET',
      'access-control-request-headers': CORRECTION_CAPABILITY_HEADER }
  }), publicEnv);
  assert.equal(preflight.status, 204);
  await broker.forwardPrivate(new Request(`https://public.source.invalid/candidate-app/v1/timesheets/${id(4)}`, { headers: capable() }),
    publicEnv, { authorization: 'Bearer source-placeholder' });
  await assert.rejects(broker.forwardPrivate(new Request(`https://public.source.invalid/candidate-app/v1/timesheets/${id(4)}`,
    { headers: { [CORRECTION_CAPABILITY_HEADER]: 'HEALTHROSTER_CORRECTION_V2' } }), publicEnv), error => error.code === 'CANDIDATE_CORRECTION_CLIENT_REQUIRED');
});
