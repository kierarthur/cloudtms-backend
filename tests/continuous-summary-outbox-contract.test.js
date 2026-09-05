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
  const timesheetHandler = source.slice(
    source.indexOf('async function handleTimesheetsSummary'),
    source.indexOf('async function handleSummaryTypeAheadLookup')
  );
  const contractsHandler = source.slice(
    source.indexOf('async function handleContractsList'),
    source.indexOf('async function handleTimesheetsSummary')
  );
  assert.match(timesheetHandler, /const candidateSubmissionSort = orderByParam === 'candidate_submission'/);
  assert.doesNotMatch(contractsHandler, /candidateSubmissionSort/);
  assert.match(source, /const scanPageSize = 200/);
  assert.match(source, /if \(keptRows\.length > keepCount\) keptRows\.length = keepCount/);
  assert.match(source, /candidate_office_summary_status_label = candidateOfficeSummaryStatusLabel/);
});

test('Timesheet Route sort uses the final display label without projecting every scanned row', () => {
  const timesheetHandler = source.slice(
    source.indexOf('async function handleTimesheetsSummary'),
    source.indexOf('async function scheduleBankingPayOperationDrain')
  );
  assert.match(timesheetHandler, /const routeDisplaySort = orderByParam === 'route_type' \|\| orderByParam === 'route_display'/);
  assert.match(timesheetHandler, /candidateSubmissionSort \|\| routeDisplaySort/);
  assert.match(timesheetHandler, /const compareRows = routeDisplaySort[\s\S]*createTimesheetRouteComparator\(orderDir\)/);
  assert.match(timesheetHandler, /if \(keptRows\.length > keepCount\) keptRows\.length = keepCount/);
  assert.match(timesheetHandler, /const selectedRows = keptRows\.slice\(effOffset, effOffset \+ effLimit\)/);
  assert.match(timesheetHandler, /routeDisplaySort && includeCandidateProjection[\s\S]*attachCandidateOfficeSummaryProjections\(env, user\.id, selectedRows\)/);
  assert.match(source, /sectionKey === 'timesheets' && \(sortKey === 'route_type' \|\| sortKey === 'route_display'\)[\s\S]*\? 'route_display'/);
});

test('Outbox membership is count-only until full selection membership is requested', () => {
  assert.match(source, /if \(!explicitFullMembership\) \{/);
  assert.match(source, /limit: 1,[\s\S]*membership_deferred: true/);
  assert.match(source, /if \(normalizedRowIds\.length !== resolvedTotal\)/);
});
