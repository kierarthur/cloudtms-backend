import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';

const read = (path) => fs.readFileSync(new URL(`../${path}`, import.meta.url), 'utf8');
const sql = read('supabase/repeatable/30082026_0640_candidate_weekly_paper_prepare_state_adapter_v1.sql');
const backend = read('broker/src/candidate-app-backend.js');
const runtime = read('broker/src/index.js');
const adapter = read('broker/src/mytms-manager-control-adapter.js');
const verification = read('supabase/verification/30082026_0605_candidate_weekly_paper_target_prepare_verification.sql');
const release = read('supabase/release/current-release.json');
const contract = JSON.parse(read('supabase/release/current-contract.json'));
const ownerReassert = read('supabase/repeatable/30082026_1540_candidate_weekly_paper_prepare_owner_reassert_v1.sql');

test('weekly PAPER adapter accepts only the exact submitted and ready states', () => {
  assert.match(sql, /v_workflow\.state='READY_FOR_MANAGER_APPROVAL'/i);
  assert.match(sql, /set state='WORKER_SUBMITTED'/i);
  assert.match(sql, /state not in \('WORKER_SUBMITTED','AWAITING_PAPER_RETURN'\)/i);
  assert.match(sql, /scope<>'WEEKLY'/i);
  assert.match(sql, /workflow_kind not in \([\s\S]*'CONTRACT_HOURS'[\s\S]*'CONTRACT_COMBINED'[\s\S]*'CONTRACT_EXPENSE'/i);
  assert.match(sql, /set qr_status='PENDING'/i);
  assert.match(sql, /qr_status::text,''\)\) not in \('','PENDING'\)/i);
  assert.doesNotMatch(sql, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
});

test('state adaptation and authoritative PAPER transition share one transaction', () => {
  assert.match(sql, /update public\.candidate_submission_workflows[\s\S]*set state='WORKER_SUBMITTED'/i);
  assert.match(sql, /return public\.candidate_workflow_transition_atomic_v1\([\s\S]*p_action=>'PAPER_PREPARE'/i);
  assert.ok(sql.indexOf('pg_advisory_xact_lock') < sql.indexOf('from public.timesheets timesheet_row'));
  assert.ok(sql.indexOf('from public.timesheets timesheet_row') < sql.lastIndexOf('from public.candidate_submission_workflows workflow_row'));
  assert.doesNotMatch(sql, /exception when others/i);
});

test('PAPER dispatch uses the bounded adapter without changing other actions', () => {
  assert.match(backend, /dbAction === 'PAPER_PREPARE'[\s\S]*'candidate_weekly_paper_prepare_atomic_v1'[\s\S]*'candidate_workflow_transition_atomic_v1'/i);
});

test('private PAPER preparation immediately delegates document work to the normal backend', () => {
  const nudge = runtime.slice(
    runtime.indexOf('async function nudgeCandidateQrPackDocumentOperation'),
    runtime.indexOf('function hasExplicitCandidateZeroBreak')
  );
  assert.match(nudge, /nudgeInvoiceOperations\(/i);
  assert.match(nudge, /result\?\.scheduled !== true && result\?\.coalesced !== true/i);
  assert.match(nudge, /nudgeCandidatePaperDocumentViaAdapter\(/i);
  assert.match(adapter, /MYTMS_PAPER_DOCUMENT_NUDGE_ADAPTER_PATH/i);
  assert.match(adapter, /verifyCandidatePrivateRequest\(/i);
  assert.match(adapter, /consumeAdapterNonce\(/i);
  assert.match(adapter, /keys\.join\(','\) !== 'operation_id,timesheet_id'/i);
});

test('pending PAPER status resumes the existing document then atomically completes its exact ready pack', () => {
  const requeue = backend.slice(
    backend.indexOf('async function restartCandidatePaperSourceDocumentFromStatus'),
    backend.indexOf('async function handlePaperPackStatus')
  );
  const assembly = backend.slice(
    backend.indexOf('async function resumeCandidatePaperPackFromStatus'),
    backend.indexOf('async function handlePaperPackStatus')
  );
  const status = backend.slice(
    backend.indexOf('async function handlePaperPackStatus'),
    backend.indexOf('async function handlePaperPackDownload')
  );
  assert.match(backend, /document_revision,document_state,current_document_version_id,[\s\S]*active_document_operation_id,manual_pdf_r2_key/i);
  assert.match(requeue, /documentState !== 'STALE'/i);
  assert.match(requeue, /UUID_RE\.test\(activeOperationId\)/i);
  assert.match(requeue, /typeof deps\?\.enqueueQrPack !== 'function'/i);
  assert.match(requeue, /expectedTimesheetId: context\.id/i);
  assert.match(requeue, /candidate-paper-status:\$\{context\.workflow\.id\}:g\$\{generation\}:r\$\{documentRevision\}/i);
  assert.match(requeue, /CANDIDATE_PAPER_DOCUMENT_REQUEUE_INVALID/i);
  assert.match(status, /context\.state === 'PREPARING'/i);
  assert.match(status, /upper\(context\.timesheet\.document_state\) === 'STALE'/i);
  assert.match(status, /restartCandidatePaperSourceDocumentFromStatus\(/i);
  assert.match(status, /paper-source-status-requeue/i);
  assert.match(status, /UUID_RE\.test\(documentOperationId\)/i);
  assert.match(status, /document_operation_id: documentOperationId/i);
  assert.match(status, /current_timesheet_id: context\.id/i);
  assert.match(status, /context\.version\?\.r2_key && context\.outbox/i);
  assert.match(status, /resumeCandidatePaperPackFromStatus\(env, deps, context\)/i);
  assert.match(assembly, /claimCandidatePaperPackAttempt\(/i);
  assert.match(assembly, /claim\.claim_acquired_new !== true/i);
  assert.match(assembly, /assembleCandidatePaperPack\(/i);
  assert.match(assembly, /releaseCandidatePaperPack\(/i);
  assert.match(assembly, /recordCandidatePaperPackFailure\(/i);
  assert.doesNotMatch(status, /timesheet_qr_send_enqueue_v1|immutablePut/i);
  assert.doesNotMatch(assembly, /timesheet_qr_send_enqueue_v1/i);
  assert.doesNotMatch(requeue, /timesheet_qr_send_enqueue_v1|immutablePut/i);
});

test('an older mail attachment never projects the current Candidate pack as ready', () => {
  const executionState = backend.slice(
    backend.indexOf('function candidatePaperExecutionState'),
    backend.indexOf('async function candidatePaperPackContext')
  );
  assert.match(executionState, /if \(complete\?\.ready === true\)/i);
  assert.doesNotMatch(
    executionState,
    /complete\?\.ready === true\s*\|\|\s*candidateCompletePackAttachmentMatchesScope/i
  );
});

test('weekly PAPER adapter remains service-only', () => {
  assert.match(sql, /revoke all on function public\.candidate_weekly_paper_prepare_atomic_v1[\s\S]*from public,anon,authenticated/i);
  assert.match(sql, /grant execute on function public\.candidate_weekly_paper_prepare_atomic_v1[\s\S]*to service_role/i);
  assert.match(ownerReassert, /alter function public\.candidate_weekly_paper_prepare_atomic_v1\([\s\S]*owner to postgres/i);
  assert.match(ownerReassert, /revoke all on function public\.candidate_weekly_paper_prepare_atomic_v1[\s\S]*from public,anon,authenticated/i);
  assert.match(ownerReassert, /grant execute on function public\.candidate_weekly_paper_prepare_atomic_v1[\s\S]*to service_role/i);
});

test('mandatory rollback proof executes ready-state transition and exact replay', () => {
  assert.match(verification, /'READY_FOR_MANAGER_APPROVAL'/i);
  assert.match(verification, /candidate_weekly_paper_prepare_atomic_v1\(/i);
  assert.match(verification, /AWAITING_PAPER_RETURN/i);
  assert.match(verification, /count\(\*\) from public\.mail_outbox/i);
  assert.match(verification, /private\._invoice_document_advance_batch\(/i);
  assert.match(verification, /CANDIDATE_WEEKLY_QR_UNSIGNED_DOCUMENT_PLAN_FAILED/i);
  assert.match(verification, /AWAITING_MANUAL_SIGNATURE/i);
  assert.match(verification, /current_document\.source_revision=current_timesheet\.document_revision::text/i);
  assert.match(verification, /current_timesheet\.document_state='QUEUED'/i);
  assert.match(verification, /ELECTRONIC_TIMESHEET/i);
  assert.match(verification, /ORDINARY_MANUAL_MISSING_ASSET_NOT_BLOCKED/i);
  assert.match(verification, /MANUAL_TIMESHEET_ASSET_REQUIRED/i);
  assert.match(release, /30082026_0605_candidate_weekly_paper_target_prepare_verification\.sql/i);
});

test('generated contract contains the exact PAPER adapter authority', () => {
  const routine = contract.routines.find((item) => item.identity.startsWith(
    'candidate_weekly_paper_prepare_atomic_v1('
  ));
  assert.ok(routine);
  assert.equal(routine.owner, 'postgres');
  assert.deepEqual(routine.acl.map((item) => item.grantee), ['postgres', 'service_role']);
  assert.equal(
    routine.definition_sha256,
    'c9a3a73845d0ad85a443c332a3cfbd617ab4bbc2926678f824299bf68a1fa291'
  );
});
