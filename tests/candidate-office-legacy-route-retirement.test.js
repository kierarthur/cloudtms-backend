import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(here, '..');
const backend = fs.readFileSync(path.join(root, 'broker', 'src', 'index.js'), 'utf8');
const authority = fs.readFileSync(
  path.join(root, 'supabase', 'repeatable', '14082026_1310_timesheet_processing_status_and_authorise_authority_v1.sql'),
  'utf8'
);
const finalise = fs.readFileSync(
  path.join(root, 'supabase', 'repeatable', '07082026_2128_candidate_finalize_reject_no_work_rpcs_v1.sql'),
  'utf8'
);

function functionBlock(source, name, nextName) {
  const start = source.indexOf(`function ${name}`);
  assert.ok(start >= 0, `${name} is missing`);
  const end = nextName ? source.indexOf(`function ${nextName}`, start + 1) : source.length;
  assert.ok(end > start, `${name} boundary is missing`);
  return source.slice(start, end);
}

test('Office Authorise uses canonical processing authority and no legacy QR veto', () => {
  const eligibility = functionBlock(backend, 'buildBulkAuthoriseEligibility', 'handleBulkAuthoriseDataset');
  const patch = functionBlock(backend, 'buildTimesheetLifecyclePatchFromMutation', 'timesheetLifecycleMutationChangedCount');
  const authorise = functionBlock(backend, 'runTimesheetAuthoriseDecision', 'runTimesheetUnauthoriseDecision');
  const unauthorise = functionBlock(backend, 'runTimesheetUnauthoriseDecision', 'normaliseWeeklyDisplaySite');

  for (const source of [patch, authorise, unauthorise]) {
    assert.doesNotMatch(source, /AWAITING_SIGNED_QR|awaiting signed QR timesheet/i);
  }
  assert.doesNotMatch(eligibility, /canBulkAuthorise[^;]*(?:qrUnsignedBlocked|qrSignedReturned|qrBulkAuthoriseVisible)/);
  assert.doesNotMatch(eligibility, /canBulkUnauthorise[^;]*(?:qrUnsignedBlocked|qrSignedReturned|qrBulkAuthoriseVisible)/);
  assert.doesNotMatch(eligibility, /finalCanBulkAuthorise[^;]*(?:qrUnsignedBlocked|qrSignedReturned|qrBulkAuthoriseVisible)/);
  assert.doesNotMatch(patch, /!qrUnsignedBlocked/);
  assert.match(patch, /\['PENDING_AUTH', 'READY_FOR_HR'\]\.includes\(processingStatusUpper\)/);
  assert.match(eligibility, /requiresAuthorisation && !isAuthorised/);
});

test('Processing Status exposes only the agreed operational catalogue', () => {
  for (const label of [
    'Unprocessed', 'Processed', 'Authorised for Invoicing', 'Partially Invoiced',
    'Invoiced', 'Archived', 'Processing Delayed'
  ]) {
    assert.match(authority, new RegExp(`'${label.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}'`));
  }
  assert.doesNotMatch(authority, /Awaiting signed QR timesheet|Awaiting Authorisation|QR_NOT_ISSUED|QR_ISSUED_AWAITING_SIGNATURE/);
  assert.doesNotMatch(authority, /qr_unsigned_blocked_calc\s*=\s*FALSE/i);
  assert.doesNotMatch(authority, /qr_signed_returned_calc\s*=\s*TRUE/i);
});

test('complete signed QR return remains the prerequisite to Candidate finalisation', () => {
  assert.match(finalise, /v_workflow\.state<>\s*'RECEIVED'/);
  assert.match(finalise, /CANDIDATE_PAPER_RETURN_INCOMPLETE/);
  assert.match(finalise, /component_kind='SIGNED_RETURN'/);
  assert.match(finalise, /paper_return_page_key=expected_page->>'page_key'/);
  assert.match(finalise, /source_content_sha256 is not null/);
  assert.match(finalise, /state='FINALISED'/);
  assert.match(finalise, /PAPER_NEVER_AUTO_AUTHORISES/);
});
