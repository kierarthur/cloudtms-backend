import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const worker = readFileSync(new URL('../src/index.js', import.meta.url), 'utf8');

function functionBody(name) {
  const startMarker = `async function ${name}(`;
  const start = worker.indexOf(startMarker);
  assert.notEqual(start, -1, `${name} must exist`);
  const next = worker.indexOf('\nasync function ', start + startMarker.length);
  return worker.slice(start, next === -1 ? worker.length : next);
}

test('Worker module initialises with import-review dependencies resolved', async () => {
  const workerModule = await import(new URL('../src/index.js', import.meta.url));
  assert.equal(typeof workerModule.default?.fetch, 'function');
});

test('durable review dispatcher is registered before all legacy import routes', () => {
  const dispatcher = worker.indexOf('const importReviewResponse = await dispatchImportReviewRequest');
  const firstLegacyImportRoute = worker.indexOf("p === '/api/healthroster/autoprocess/import'");
  assert.ok(dispatcher > 0);
  assert.ok(firstLegacyImportRoute > dispatcher);
});

test('all retired import mutation routes return the hard-cutover response', () => {
  for (const routeFragment of [
    "'/api/healthroster/:import_id/autoprocess-apply'",
    "'/api/imports/hr-rota/:import_id/apply'",
    "'/api/hr/rota/tso-email'",
    "'/api/nhsp/:import_id/confirm'",
    "'/api/nhsp/:import_id/apply'"
  ]) {
    const location = worker.indexOf(routeFragment);
    assert.ok(location > 0, `${routeFragment} must remain explicitly retired`);
    assert.match(worker.slice(location, location + 650), /handleRetiredImportMutationRoute/);
  }
  for (const handler of ['handleNhspImportConfirm', 'handleNhspApply', 'handleHrAutoprocessApply', 'handleHrRotaQueueTsoEmail']) {
    const body = functionBody(handler);
    assert.match(body, /handleRetiredImportMutationRoute/);
    assert.doesNotMatch(body, /selected_action_ids|validation_rows|hr_issue_emails/);
  }
});

test('the Worker contains no caller for hard-retired database RPCs', () => {
  assert.doesNotMatch(worker, /hr_issue_emails_touch/);
  assert.doesNotMatch(worker, /hr_weekly_validation_apply_send_emails/);
  assert.doesNotMatch(worker, /import_apply_operation_claim_v1/);
  assert.doesNotMatch(worker, /import_review_follow_up_update_v1/);
});

test('post-commit orchestration is delegated to the component-aware runner', () => {
  assert.match(worker, /createImportReviewPostCommitRunner/);
  assert.match(worker, /runFollowUp:\s*runImportReviewPostCommit/);
});

test('Daily timesheet authority is no longer read from or written to hr_rows payload_json', () => {
  assert.doesNotMatch(worker, /payload(?:_json)?\??\.resolved_timesheet_id/);
  const resolutionBody = functionBody('handleHrRotaResolveMappings');
  assert.doesNotMatch(resolutionBody, /resolved_timesheet_id\s*:/);
  assert.match(resolutionBody, /HR_DAILY_TIMESHEET_RESOLUTION_ROUTE_REQUIRED/);
});

test('all three import staging routes persist bounded source evidence and parser versions', () => {
  const daily = functionBody('handleImportHrRotaParse');
  const weekly = functionBody('handleHrAutoprocessImport');
  const nhsp = functionBody('handleNhspImport');
  assert.match(daily, /importReviewSourceEvidenceFromR2[\s\S]*:HR_DAILY/);
  assert.match(weekly, /importReviewSourceEvidenceFromR2[\s\S]*:HR_WEEKLY/);
  assert.match(nhsp, /importReviewSourceEvidenceFromR2[\s\S]*:NHSP/);
  for (const body of [daily, weekly, nhsp]) {
    assert.match(body, /source_file_sha256/);
    assert.match(body, /parser_version/);
    assert.match(body, /findImportStageReplay/);
  }
});

test('timesheet-query history is marked only after the provider outbox row is SENT', () => {
  const drain = functionBody('drainEmailOutboxOnce');
  const sentUpdate = drain.indexOf("status: 'SENT'");
  const deliveryMark = drain.indexOf("'timesheet_query_email_delivery_mark_v1'");
  const deliveryReconcile = drain.indexOf("'timesheet_query_email_delivery_reconcile_v1'", deliveryMark);
  assert.ok(sentUpdate > 0);
  assert.ok(deliveryMark > sentUpdate);
  assert.ok(deliveryReconcile > deliveryMark);
});

test('contract email override is supported by every contract persistence path', () => {
  for (const name of [
    'handleContractsCreate',
    'handleContractsUpdate',
    'handleContractsReplace',
    'handleContractsDuplicate',
    'handleContractsCloneAndExtend'
  ]) {
    assert.match(functionBody(name), /send_ts_queries_to_different_email/);
    assert.match(functionBody(name), /ts_queries_alt_email_address/);
  }
});

test('global/client financial policy writes use the installed atomic CAS RPCs', () => {
  assert.match(functionBody('handleUpdateSettings'), /settings_defaults_import_financial_policy_update_v1/);
  const createClient = functionBody('handleCreateClient');
  assert.match(createClient, /createClientWithSettingsAtomic/);
  assert.doesNotMatch(createClient, /rest\/v1\/(clients|client_settings)/);
  assert.match(functionBody('handleUpdateClient'), /client_update_with_settings_v1/);
});

test('Banking Pay refresh remains outside the import-review change boundary', () => {
  const bankingRefresh = functionBody('handleBankingPayWorkbenchSessionRefresh');
  assert.doesNotMatch(bankingRefresh, /send_ts_queries_to_different_email|import_review|reversal_complete_financials_date/);
});
