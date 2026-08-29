import assert from 'node:assert/strict';
import test from 'node:test';

import { isOfficeFileDownloadKeyAllowed } from '../broker/src/index.js';

test('Office file downloads preserve the established document prefixes', () => {
  const env = { CANDIDATE_APP_ENVIRONMENT: 'TEST' };
  for (const key of [
    'files/evidence.pdf',
    'invoices/invoice.pdf',
    'remittances/remittance.pdf',
    'paper_ts/timesheet.pdf',
    'signatures/candidate.png',
    'docs/document.pdf',
    'docs-pdf/timesheets/example.pdf',
    'assets/logo.png',
    'Assets/logo.png',
    'mailshot-template-attachments/attachment.pdf'
  ]) {
    assert.equal(isOfficeFileDownloadKeyAllowed(env, key), true, key);
  }
});

test('Office file downloads allow only Candidate evidence for the exact Worker environment', () => {
  assert.equal(
    isOfficeFileDownloadKeyAllowed(
      { CANDIDATE_APP_ENVIRONMENT: 'TEST' },
      '/candidate-app/test/workflow/final/timesheet.pdf'
    ),
    true
  );
  assert.equal(
    isOfficeFileDownloadKeyAllowed(
      { CANDIDATE_APP_ENVIRONMENT: 'TEST' },
      'candidate-app/live/workflow/final/timesheet.pdf'
    ),
    false
  );
  assert.equal(
    isOfficeFileDownloadKeyAllowed(
      { CANDIDATE_APP_ENVIRONMENT: 'LIVE' },
      'candidate-app/test/workflow/final/timesheet.pdf'
    ),
    false
  );
});

test('Office file downloads fail closed for missing or malformed Candidate environments and unsafe keys', () => {
  for (const [env, key] of [
    [{}, 'candidate-app/test/workflow/final/timesheet.pdf'],
    [{ CANDIDATE_APP_ENVIRONMENT: '../test' }, 'candidate-app/test/workflow/final/timesheet.pdf'],
    [{ CANDIDATE_APP_ENVIRONMENT: 'TEST' }, 'candidate-app/test/../live/timesheet.pdf'],
    [{ CANDIDATE_APP_ENVIRONMENT: 'TEST' }, 'candidate-app/branding/logo.png'],
    [{ CANDIDATE_APP_ENVIRONMENT: 'TEST' }, 'unrelated/private.pdf'],
    [{ CANDIDATE_APP_ENVIRONMENT: 'TEST' }, '']
  ]) {
    assert.equal(isOfficeFileDownloadKeyAllowed(env, key), false, key);
  }
});
