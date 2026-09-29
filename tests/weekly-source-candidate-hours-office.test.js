import assert from 'node:assert/strict';
import { createHash, webcrypto } from 'node:crypto';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const worker = readFileSync(new URL('../broker/src/index.js', import.meta.url), 'utf8');
const audit = readFileSync(new URL('../supabase/repeatable/29092026_1800_candidate_hours_received_audit_v1.sql', import.meta.url), 'utf8');
const evidenceSql = readFileSync(new URL('../supabase/repeatable/29092026_1810_weekly_source_candidate_hours_evidence_v1.sql', import.meta.url), 'utf8');
const acl = readFileSync(new URL('../supabase/repeatable/15092026_1534_weekly_source_acl_contract_v1.sql', import.meta.url), 'utf8');
const aclVerification = readFileSync(new URL('../supabase/verification/15092026_1534_weekly_source_acl_contract_v1.sql', import.meta.url), 'utf8');
const helperStart = worker.indexOf('async function weeklySourceCandidatePackEvidence(');
const downloadStart = worker.indexOf('\nasync function handleTimesheetCandidateHoursEvidence(', helperStart);
const listStart = worker.indexOf('\nasync function handleTimesheetEvidenceList(', downloadStart);
assert.ok(helperStart > 0 && downloadStart > helperStart && listStart > downloadStart);

const timesheetId = '10000000-0000-4000-8000-000000000001';
const eventId = '30000000-0000-4000-8000-000000000001';
const bytes = new TextEncoder().encode('%PDF-1.7\nCandidate signed hours');
const sha = createHash('sha256').update(bytes).digest('hex');

const validPack = () => ({ available: true, event_id: eventId,
  timesheet_id: timesheetId, r2_key: 'candidate/signed.pdf', sha256: sha,
  size_bytes: bytes.length, filename: 'signed-hours.pdf' });

function loadPack(result = validPack(), calls = []) {
  const rpc = async (_env, name, request) => { calls.push({ name, request }); return result; };
  return new Function('sbRpc', 'unwrapRpcJsonb',
    `${worker.slice(helperStart, downloadStart)}\nreturn weeklySourceCandidatePackEvidence;`)(rpc, value => value);
}

test('signed candidate-hours evidence uses only the actor-scoped service RPC', async () => {
  const calls = [];
  assert.equal((await loadPack(validPack(), calls)({}, timesheetId, eventId)).event_id, eventId);
  assert.deepEqual(calls, [{ name: 'weekly_source_candidate_hours_evidence_v1',
    request: { p_request: { actor_user_id: eventId, timesheet_id: timesheetId } } }]);
  assert.equal(await loadPack({ available: false })({}, timesheetId, eventId), null);
  await assert.rejects(() => loadPack({ ...validPack(), timesheet_id: eventId })({}, timesheetId, eventId),
    /LINEAGE_INCOMPLETE/);
  assert.doesNotMatch(worker.slice(helperStart, downloadStart), /\/rest\/v1\/(weekly_completed_pack_copy_events|mail_outbox|candidate_submission_workflows)/);
  assert.match(evidenceSql, /weekly_source_query_require_service_v1\(\)/);
  assert.match(evidenceSql, /weekly_source_office_authority_v1\(/);
  assert.match(evidenceSql, /v_mail\.attachment_total_bytes<>v_bytes/);
  assert.match(evidenceSql, /v_event\.final_document_hash/);
  assert.match(evidenceSql, /revoke all on function public\.weekly_source_candidate_hours_evidence_v1\(jsonb\)/);
  assert.match(acl, /public\.weekly_source_candidate_hours_evidence_v1\(jsonb\)/);
  assert.match(aclVerification, /public\.weekly_source_candidate_hours_evidence_v1\(jsonb\)/);
});

test('Office PDF download checks the event and bytes, and refuses unrelated or altered files', async () => {
  const pack = await loadPack()({}, timesheetId, eventId);
  const download = new Function('requireUser', 'resolveTimesheetToCurrent',
    'weeklySourceCandidatePackEvidence', 'withCORS', 'unauthorized', 'notFound',
    'serverError', 'crypto', 'Response',
    `${worker.slice(downloadStart, listStart)}\nreturn handleTimesheetCandidateHoursEvidence;`)(
    async () => ({ id: 'admin' }), async () => ({ current_timesheet_id: timesheetId }),
    async () => pack, (_env, _req, response) => response,
    () => new Response('', { status: 401 }), () => new Response('', { status: 404 }),
    () => new Response('', { status: 500 }), webcrypto, Response);
  const request = new Request('https://test.invalid');
  const env = { R2_BUCKET: { get: async () => ({ size: bytes.length,
    arrayBuffer: async () => bytes.buffer }) } };
  assert.equal((await download(env, request, timesheetId, 'other')).status, 404);
  const ok = await download(env, request, timesheetId, eventId);
  assert.equal(ok.status, 200);
  assert.equal(ok.headers.get('Cache-Control'), 'no-store');
  assert.deepEqual(new Uint8Array(await ok.arrayBuffer()), bytes);
  const altered = new TextEncoder().encode('%PDF-1.7\nAltered signed hours!');
  assert.equal((await download({ R2_BUCKET: { get: async () => ({ size: bytes.length,
    arrayBuffer: async () => altered.buffer }) } }, request, timesheetId, eventId)).status, 500);
});

test('accepted candidate submission writes a real, idempotent generation audit event', () => {
  assert.match(audit, /new\.state <> 'WORKER_SUBMITTED'/);
  assert.match(audit, /old\.generation = new\.generation/);
  assert.match(audit, /old\.candidate_signed_at_utc is not distinct from new\.candidate_signed_at_utc/);
  assert.match(audit, /'candidate-hours-received:'\|\|new\.id::text\|\|':'\|\|new\.generation::text/);
  assert.match(audit, /insert into public\.audit_events\(/);
  assert.match(audit, /'CANDIDATE_HOURS_RECEIVED'/);
  assert.match(audit, /after update of generation,state,worker_submitted_at_utc,candidate_signed_at_utc/);
});
