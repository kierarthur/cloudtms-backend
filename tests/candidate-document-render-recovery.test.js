import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const recoverySql = fs.readFileSync(path.join(root,
  'supabase/repeatable/07092026_2315_candidate_review_render_recovery_v1.sql'), 'utf8');
const expenseRecoverySql = fs.readFileSync(path.join(root,
  'supabase/repeatable/08092026_1050_candidate_expense_render_recovery_without_operation_v1.sql'), 'utf8');
const workerSource = fs.readFileSync(path.join(root,
  'broker/src/candidate-private-worker.js'), 'utf8');
const configSource = fs.readFileSync(path.join(root,
  'candidate-private-api/wrangler.jsonc'), 'utf8');

test('review render recovery is service-only, exact-target capable and excludes expense updates', () => {
  assert.match(recoverySql, /security definer/i);
  assert.match(recoverySql, /workflow\.state='WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT'/);
  assert.match(recoverySql, /_candidate_render_contract_v1\([\s\S]*'ELECTRONIC_MANAGER_REVIEW'/);
  assert.match(recoverySql, /candidate_pending_expense_updates[\s\S]*'EDITING','RENDERING'/);
  assert.match(recoverySql, /p_workflow_id is not null or workflow\.updated_at_utc<=p_now_utc-interval '30 seconds'/);
  assert.match(recoverySql, /revoke all[\s\S]*from public,anon,authenticated,service_role/i);
  assert.match(recoverySql, /grant execute[\s\S]*to service_role/i);
  assert.doesNotMatch(recoverySql, /grant execute[\s\S]*to (?:anon|authenticated)/i);
});

test('one durable queue covers review renders, expense refreshes and manager final documents', () => {
  assert.match(configSource, /CANDIDATE_DOCUMENT_RENDER_QUEUE/);
  assert.match(configSource, /test-cloudtms-candidate-document-render-recovery/);
  assert.match(configSource, /test-cloudtms-candidate-document-render-recovery-dlq/);
  assert.match(workerSource, /CANDIDATE_REVIEW_RENDER_QUEUE_MESSAGE_V1/);
  assert.match(workerSource, /CANDIDATE_EXPENSE_RENDER_QUEUE_MESSAGE_V1/);
  assert.match(workerSource, /CANDIDATE_MANAGER_FINALISATION_QUEUE_MESSAGE_V1/);
  assert.match(workerSource, /resumePendingCandidateReviewRenders/);
  assert.match(workerSource, /resumePendingCandidateExpenseUpdateRenders/);
  assert.match(workerSource, /recoverPendingCandidateManagerFinalisations/);
});

test('expense render recovery includes ordinary updates without an expense operation', () => {
  assert.match(expenseRecoverySql, /left join public\.candidate_expense_operations operation/i);
  assert.match(expenseRecoverySql,
    /update_row\.operation_id is null or operation\.state='RENDERING'/i);
  assert.match(expenseRecoverySql, /security definer/i);
  assert.match(expenseRecoverySql, /revoke all[\s\S]*from public,anon,authenticated,service_role/i);
  assert.match(expenseRecoverySql, /grant execute[\s\S]*to service_role/i);
  assert.doesNotMatch(expenseRecoverySql, /grant execute[\s\S]*to (?:anon|authenticated)/i);
});
