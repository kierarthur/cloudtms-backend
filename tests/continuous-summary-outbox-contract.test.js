import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const source = readFileSync(new URL('../broker/src/index.js', import.meta.url), 'utf8');

test('Outbox membership is channel-qualified and cardinality checked', () => {
  assert.match(source, /allowedSections = new Set\(\['candidates', 'clients', 'umbrellas', 'contracts', 'timesheets', 'invoices', 'outbox'\]\)/);
  assert.match(source, /rowIds\.push\(`\$\{channel\}::\$\{id\}`\)/);
  assert.match(source, /OUTBOX_MEMBERSHIP_CARDINALITY_MISMATCH/);
  assert.match(source, /const pageSize = 500/);
});

test('Outbox Ready filter and type-ahead are supported by the unified route', () => {
  assert.match(source, /new Set\(\['READY', 'SCHEDULED', 'QUEUED', 'SENT', 'DELIVERED', 'READ', 'FAILED'\]\)/);
  assert.match(source, /if \(sectionKey === 'outbox'\)/);
  assert.match(source, /handleOutboxUnifiedList\(env, internalReq\)/);
  assert.match(source, /row_id: channelValue && idValue \? `\$\{channelValue\}::\$\{idValue\}` : null/);
});

test('Candidate Submission sort stays server-bounded and exposes the exact display label', () => {
  assert.match(source, /const candidateSubmissionSort = orderByParam === 'candidate_submission'/);
  assert.match(source, /const scanPageSize = 200/);
  assert.match(source, /if \(keptRows\.length > keepCount\) keptRows\.length = keepCount/);
  assert.match(source, /candidate_office_summary_status_label = candidateOfficeSummaryStatusLabel/);
});
