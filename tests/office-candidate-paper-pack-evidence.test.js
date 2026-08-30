import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

import { resolveOfficeCandidatePaperPackEvidenceKey } from '../broker/src/office-candidate-paper-pack-evidence.js';

const timesheetId = '312879f1-2291-5073-9793-b826d5bbfdfb';
const digest = 'a'.repeat(64);
const key = 'candidate-app/test/workflow/g2/paper-pack/complete.pdf';

function readyRow(overrides = {}) {
  const scope = {
    candidate_mail_authority: 'CANDIDATE_PAPER_V1',
    candidate_paper_pack_ready: true,
    mail_held_until_pdf_rendered: false,
    mail_hold_reason: null,
    candidate_complete_pack_storage_key: key,
    candidate_complete_pack_sha256: digest,
    candidate_complete_pack_size_bytes: 4096,
    candidate_complete_pack_page_count: 3,
    ...(overrides.scope || {})
  };
  const attachment = {
    r2_key: scope.candidate_complete_pack_storage_key,
    sha256: scope.candidate_complete_pack_sha256,
    size_bytes: scope.candidate_complete_pack_size_bytes,
    page_count: scope.candidate_complete_pack_page_count,
    content_type: 'application/pdf'
  };
  return {
    context_id: overrides.context_id || timesheetId,
    payment_scope_json: scope,
    attachments: overrides.attachments || [attachment]
  };
}

test('Office evidence resolves one exact ready Candidate paper pack', () => {
  assert.equal(
    resolveOfficeCandidatePaperPackEvidenceKey(
      { CANDIDATE_APP_ENVIRONMENT: 'TEST' },
      [readyRow()],
      timesheetId
    ),
    key
  );
});

test('Office evidence rejects retired, held, foreign and ambiguous packs', () => {
  const env = { CANDIDATE_APP_ENVIRONMENT: 'TEST' };
  assert.equal(resolveOfficeCandidatePaperPackEvidenceKey(env, [readyRow({
    scope: { candidate_paper_generation_retired: true }
  })], timesheetId), null);
  assert.equal(resolveOfficeCandidatePaperPackEvidenceKey(env, [readyRow({
    scope: { candidate_paper_pack_ready: false, mail_held_until_pdf_rendered: true }
  })], timesheetId), null);
  assert.equal(resolveOfficeCandidatePaperPackEvidenceKey(env, [readyRow({
    context_id: '11111111-1111-4111-8111-111111111111'
  })], timesheetId), null);
  assert.equal(resolveOfficeCandidatePaperPackEvidenceKey(env, [readyRow(), readyRow()], timesheetId), null);
});

test('Office evidence rejects inconsistent attachments and environment prefixes', () => {
  const env = { CANDIDATE_APP_ENVIRONMENT: 'TEST' };
  assert.equal(resolveOfficeCandidatePaperPackEvidenceKey(env, [readyRow({
    attachments: [{ r2_key: key, sha256: digest, size_bytes: 1, page_count: 3, content_type: 'application/pdf' }]
  })], timesheetId), null);
  assert.equal(resolveOfficeCandidatePaperPackEvidenceKey(env, [readyRow({
    scope: { candidate_complete_pack_storage_key: 'candidate-app/live/workflow/g2/paper-pack/complete.pdf' }
  })], timesheetId), null);
});

test('Office evidence prioritises the complete Candidate pack over the legacy PDF fallback', () => {
  const source = readFileSync(new URL('../broker/src/index.js', import.meta.url), 'utf8');
  assert.match(source, /candidatePaperPackStorageKey\s*\|\|\s*\(\(ts\?\.manual_pdf_r2_key/);
  assert.match(source, /type=eq\.TIMESHEET_QR[\s\S]*candidatePaperPackStorageKey/);
  assert.match(source, /complete_candidate_pack:\s*!!candidatePaperPackStorageKey/);
});
