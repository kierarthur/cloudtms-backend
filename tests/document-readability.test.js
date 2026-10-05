import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { PDFDocument } from 'pdf-lib';
import jpeg from 'jpeg-js';
import { buildOfficialWeekPeriod, officialTimesheetNumber, renderOfficialTimesheetPdfBytes } from '../broker/src/timesheet-official-pdf.js';
import { candidateAppBackendInternals } from '../broker/src/candidate-app-backend.js';

export function readabilityModel() {
  // Reuse the existing canonical renderer fixture, not a parallel document model.
  const fixtureSource = readFileSync(new URL('./timesheet_official_pdf.test.js', import.meta.url), 'utf8');
  const fixture = fixtureSource.slice(fixtureSource.indexOf('function fixture('), fixtureSource.indexOf("test('official week period"));
  const model = Function('buildOfficialWeekPeriod', 'officialTimesheetNumber', 'TIMESHEET_ID',
    `${fixture}; return fixture();`)(buildOfficialWeekPeriod, officialTimesheetNumber, '00000000-0000-4000-8000-000000000101');
  model.worker = { first_name: 'Kier', surname: 'Arthur', job_profile_title: 'Community Psychiatric Nurse' };
  model.client = { name: 'Arthur Rai Medical Services', site_ward: 'Community nursing service' };
  model.week_period.days[1].shift_lines[0].break_display_mode = 'EXPLICIT_INTERVAL';
  model.week_period.days[1].shift_lines[0].break_start_local = '12:00';
  model.week_period.days[1].shift_lines[0].break_end_local = '12:30';
  return model;
}

export function readabilityWorkflow() {
  const base = { contract_version: 'CANDIDATE_DOCUMENT_BRANDING_V1',
    agency_name: 'Arthur Rai Medical Services', logo_key: null, logo_sha256: null, logo_media_type: null };
  const branding = { ...base, branding_contract_sha256: createHash('sha256').update(JSON.stringify(base)).digest('hex') };
  return { id: '00000000-0000-4000-8000-000000000493', week_ending_date: '2026-07-11',
    immutable_submission_json: { official_presentation: { branding,
      worker: { first_name: 'Kier', surname: 'Arthur' }, client: { name: 'Arthur Rai Medical Services' } },
    expense_submission: { canonical_tsfin_snapshot: { mileage_units: 10,
      travel_pay_ex_vat: 8.50, accommodation_pay_ex_vat: 20, other_pay_ex_vat: 4,
      expenses_pay_ex_vat: 37 } } } };
}

export async function readabilityReceiptState(source = null) {
  const pdf = await PDFDocument.create();
  const page = pdf.addPage([300, 400]);
  page.drawText('Receipt / photographed evidence', { x: 20, y: 365, size: 14 });
  page.drawText('Local visual QA sample - no application mutation', { x: 20, y: 340, size: 9 });
  page.drawText('Travel: GBP 8.50', { x: 20, y: 300, size: 13 });
  const bytes = source?.bytes || new Uint8Array(await pdf.save());
  const mediaType = source?.media_type || 'application/pdf';
  const digest = createHash('sha256').update(bytes).digest('hex');
  const component = { id: '00000000-0000-4000-8000-000000000492',
    component_kind: 'EXPENSE_EVIDENCE', document_role: 'SOURCE_EVIDENCE', expense_category: 'TRAVEL',
    review_ordinal: 1, storage_key: 'local-readability-receipt', media_type: mediaType,
    byte_size: bytes.byteLength, source_content_sha256: `\\x${digest}`, state: 'IMMUTABLE' };
  return { component, workflow: readabilityWorkflow(),
    contract: { review_ordinal: 1, render_input: { source_component_id: component.id, source_content_sha256: digest } },
    env: { R2: { async get(key) { assert.equal(key, component.storage_key); return {
      httpMetadata: { contentType: mediaType }, async arrayBuffer() {
        return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
      } }; } } } };
}

test('filled Timesheet values grow with cell height without changing frozen hours or identities', async () => {
  const model = readabilityModel();
  const frozen = JSON.stringify(model);
  const result = await renderOfficialTimesheetPdfBytes(model);
  assert.equal(result.page_count, 1);
  assert.equal(result.render_receipt.schedule_value_preferred_font_pt, 12);
  assert.equal(result.render_receipt.detail_value_preferred_font_pt, 11);
  assert.ok(result.render_receipt.total_value_preferred_font_pt >= 10);
  assert.equal(JSON.stringify(model), frozen);
});

test('long identities and large amounts render on expense review and unchanged physical QR pages', async () => {
  const state = await readabilityReceiptState();
  state.workflow.immutable_submission_json.official_presentation.client.name =
    'Berkshire Healthcare NHS Foundation Trust - Community Mental Health Service';
  state.workflow.immutable_submission_json.expense_submission.canonical_tsfin_snapshot.expenses_pay_ex_vat = 1234567.89;
  const original = JSON.stringify(state.workflow);
  for (const paper of [false, true]) {
    const result = await candidateAppBackendInternals.renderExpensePage(state.env,
      { ...state.contract, ...(paper ? { paper_return_qr_text: `TSQ2.${'a'.repeat(330)}` } : {}) }, state, 'REVIEW');
    assert.equal((await PDFDocument.load(result.pdf_bytes)).getPageCount(), 1);
    assert.equal(JSON.stringify(state.workflow), original);
  }
});

test('uploaded PNG and JPEG evidence remain digest-checked and fit both enlarged review layouts', async () => {
  const sources = [
    { media_type: 'image/png', bytes: Buffer.from(
      'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Y9ZP8sAAAAASUVORK5CYII=', 'base64') },
    { media_type: 'image/jpeg', bytes: jpeg.encode({
      data: Buffer.alloc(64 * 128 * 4, 255), width: 64, height: 128
    }, 92).data }
  ];
  for (const source of sources) {
    const digest = createHash('sha256').update(source.bytes).digest('hex');
    const state = await readabilityReceiptState(source);
    for (const paper of [false, true]) {
      const contract = { ...state.contract,
        ...(paper ? { paper_return_qr_text: `TSQ2.${'a'.repeat(330)}` } : {}) };
      const result = await candidateAppBackendInternals.renderExpensePage(state.env, contract, state, 'REVIEW');
      assert.equal((await PDFDocument.load(result.pdf_bytes)).getPageCount(), 1);
      assert.equal(createHash('sha256').update(source.bytes).digest('hex'), digest);
      await assert.rejects(candidateAppBackendInternals.renderExpensePage(state.env,
        { ...contract, render_input: { ...contract.render_input, source_content_sha256: '0'.repeat(64) } },
        state, 'REVIEW'), error => error.code === 'CANDIDATE_SOURCE_COMPONENT_NOT_ALLOWED');
    }
  }
});
