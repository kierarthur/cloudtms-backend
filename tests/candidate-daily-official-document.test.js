import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import test from 'node:test';
import { candidateAppBackendInternals } from '../broker/src/candidate-app-backend.js';

const { officialPresentationFromRows, buildOfficialPresentationSnapshot, buildOfficialCandidateModel } = candidateAppBackendInternals;
const timesheetId = '00000000-0000-4000-8000-000000000001';
const candidateId = '00000000-0000-4000-8000-000000000002';
const timesheet = {
  timesheet_id: timesheetId, sheet_scope: 'DAILY', version: 1,
  week_ending_date: '2026-08-30', hospital_norm: 'Booked Hospital',
  ward_norm: 'Booked Ward', job_title_norm: 'Registered Nurse'
};
const candidate = { first_name: 'Test', last_name: 'Worker' };
const brandingBase = { contract_version: 'CANDIDATE_DOCUMENT_BRANDING_V1', agency_name: 'Test Agency',
  logo_key: null, logo_sha256: null, logo_media_type: null };
const branding = { ...brandingBase,
  branding_contract_sha256: createHash('sha256').update(JSON.stringify(brandingBase)).digest('hex') };
const signatureBytes = Buffer.from('synthetic-signature-fixture');
const signatureHash = createHash('sha256').update(signatureBytes).digest('hex');
async function withSignatureFixture(run) {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (input) => {
    const url = new URL(String(input));
    assert.ok(url.pathname.endsWith('/candidate_submission_components'));
    return Response.json([{ storage_key: 'test/signature.png', source_content_sha256: signatureHash }]);
  };
  try {
    return await run({ SUPABASE_URL: 'https://miget.test.invalid', SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder',
      R2: { async get() { return { arrayBuffer: async () => signatureBytes,
        httpMetadata: { contentType: 'image/png' } }; } } });
  } finally { globalThis.fetch = originalFetch; }
}
const signedWorkflow = { candidate_signature_component_id: candidateId,
  manager_signature_component_id: candidateId, candidate_signed_at_utc: '2026-08-28T20:00:00Z',
  manager_approved_at_utc: '2026-08-28T20:05:00Z', manager_name: 'Test Manager', manager_position: 'Ward Manager' };

test('unresolved Daily presentation uses the booked hospital without inventing a Client', () => {
  const presentation = officialPresentationFromRows({ timesheet, candidate, client: null, contractRow: null, branding });
  assert.equal(presentation.client.name, 'Booked Hospital');
  assert.equal(presentation.client.hospital, 'Booked Hospital');
  assert.equal(presentation.client.site_ward, 'Booked Ward');
  assert.equal(presentation.worker.job_profile_title, 'Registered Nurse');
  assert.equal('client_id' in presentation.client, false);
});

test('resolved Daily presentation retains the distinct booked hospital and Client name', () => {
  const presentation = officialPresentationFromRows({ timesheet, candidate, client: { name: 'Mapped Trust' }, branding });
  assert.equal(presentation.client.name, 'Mapped Trust');
  assert.equal(presentation.client.hospital, 'Booked Hospital');
});

test('Daily snapshot actually selects the booked hospital from the saved receipt row', async () => {
  const originalFetch = globalThis.fetch;
  let selected;
  globalThis.fetch = async (input) => {
    const url = new URL(String(input));
    if (url.pathname.endsWith('/timesheets')) {
      selected = url.searchParams.get('select').split(',');
      return Response.json([Object.fromEntries(selected.filter(key => key in timesheet).map(key => [key, timesheet[key]]))]);
    }
    if (url.pathname.endsWith('/candidates')) return Response.json([candidate]);
    if (url.pathname.endsWith('/timesheets_financials')) return Response.json([]);
    if (url.pathname.endsWith('/settings_defaults')) return Response.json([{ agency_name: 'Test Agency', agency_logo: null }]);
    throw Error(`Unexpected read ${url.pathname}`);
  };
  try {
    const presentation = await buildOfficialPresentationSnapshot({
      SUPABASE_URL: 'https://miget.test.invalid', SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
    }, { scope: 'DAILY', candidate_id: candidateId, target_timesheet_id: timesheetId });
    assert.ok(selected.includes('hospital_norm'));
    assert.equal(presentation.client.hospital, 'Booked Hospital');
  } finally { globalThis.fetch = originalFetch; }
});

test('Daily official review and final models use the stored week ending, never the worked Friday', async () => {
  const presentation = officialPresentationFromRows({ timesheet, candidate, branding });
  const workflow = { ...signedWorkflow, scope: 'DAILY', target_timesheet_id: timesheetId, generation: 1,
    work_date: '2026-08-28', week_ending_date: null,
    immutable_submission_json: { official_presentation: presentation, hours_submission: {
      actual_schedule_json: [{ date: '2026-08-28', start_time: '07:30', end_time: '20:00',
        break_start: '13:00', break_end: '13:30', break_minutes: 30 }]
    } } };
  for (const phase of ['REVIEW', 'FINAL']) {
    const { model } = await withSignatureFixture(env => buildOfficialCandidateModel(env, {}, { workflow, timesheet, candidate }, phase));
    assert.equal(model.week_period.end_date, '2026-08-30');
    assert.equal(model.week_period.end_weekday_name, 'Sunday');
    assert.equal(model.week_period.days.find(day => day.date === '2026-08-28').shift_lines.length, 1);
    assert.equal(model.totals.paid_minutes, 720);
    assert.equal(model.client.hospital, 'Booked Hospital');
  }
});

test('Weekly official model keeps its configured non-Sunday period and frozen Client presentation', async () => {
  const presentation = officialPresentationFromRows({ candidate, branding,
    contractRow: { display_site: 'Contract Site', role: 'Nurse' }, client: { name: 'Contract Client' } });
  const workflow = { ...signedWorkflow, scope: 'WEEKLY', target_timesheet_id: timesheetId, generation: 1,
    week_ending_date: '2026-08-28', work_date: null,
    immutable_submission_json: { official_presentation: presentation, hours_submission: { actual_schedule_json: [] } } };
  const { model } = await withSignatureFixture(env => buildOfficialCandidateModel(env, {}, { workflow, timesheet, candidate }, 'REVIEW'));
  assert.equal(model.week_period.end_date, '2026-08-28');
  assert.equal(model.client.name, 'Contract Client');
  assert.equal(model.client.hospital, 'Contract Site');
});
