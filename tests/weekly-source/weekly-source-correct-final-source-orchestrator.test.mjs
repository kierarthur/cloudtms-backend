import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

import {
  orchestrateWeeklyCorrectFinalApply,
  orchestrateWeeklyCorrectFinalPreview,
  WEEKLY_CORRECT_FINAL_SOURCE_ORCHESTRATION_CONTRACT,
  WeeklyCorrectFinalSourceError,
} from '../../broker/src/weekly-source/correct-final-source-orchestrator.mjs';
import { dispatchWeeklySourceRequest } from '../../broker/src/weekly-source/routes.js';

const ID = Object.freeze({
  actor: 'cf000000-0000-4000-8000-000000000001',
  cycle: 'cf000000-0000-4000-8000-000000000002',
  priorRevision: 'cf000000-0000-4000-8000-000000000003',
  session: 'cf000000-0000-4000-8000-000000000004',
  upload: 'cf000000-0000-4000-8000-000000000005',
  publication: 'cf000000-0000-4000-8000-000000000006',
  rebuiltPublication: 'cf000000-0000-4000-8000-00000000000b',
  preparedRevision: 'cf000000-0000-4000-8000-000000000007',
  appliedRevision: 'cf000000-0000-4000-8000-000000000008',
  rootA: 'cf000000-0000-4000-8000-000000000009',
  rootB: 'cf000000-0000-4000-8000-00000000000a',
});

const HASH = Object.freeze({
  prior: '1'.repeat(64),
  rows: '2'.repeat(64),
  comparison: '3'.repeat(64),
  issues: '4'.repeat(64),
  rebuiltComparison: '9'.repeat(64),
  rebuiltIssues: 'a'.repeat(64),
  rootA: '5'.repeat(64),
  rootB: '6'.repeat(64),
  preview: '7'.repeat(64),
});

const CONFIRMATION = 'I understand this replacement becomes the final source for this week.';

const officePreview = (overrides = {}) => ({
  changes: [{
    candidate: 'Alex Worker',
    day_date: 'Monday 14 September 2026',
    current_final: '09:00–17:00 (30 minute break)',
    replacement: '09:00–18:00 (30 minute break)',
    result: 'Approved hours will change to 8 hours 30 minutes.',
  }],
  blockers: [],
  confirmation_text: CONFIRMATION,
  preview_hash: HASH.preview,
  ...overrides,
});

const previewRequest = (overrides = {}) => ({
  actor_user_id: ID.actor,
  source_cycle_id: ID.cycle,
  authority_scope_kind: 'CYCLE',
  report_scope_id: null,
  expected_current_final_revision_id: ID.priorRevision,
  expected_final_manifest_hash: HASH.prior,
  idempotency_key: 'correct-final-source-test-0001',
  replacement_source: { file_key: 'weekly-source/new-replacement.xlsx' },
  ...overrides,
});

const preparedRoot = (rootId, preparedHash) => ({
  root_timesheet_id: rootId,
  prior_projection_receipt_id: 'cf100000-0000-4000-8000-000000000001',
  client_id: 'cf100000-0000-4000-8000-000000000002',
  source_profile_kind: 'ROSTER_COMPLETE_COVERAGE',
  source_mode: 'HEALTHROSTER_WEEKLY',
  source_units: [{ unit: 'DAY', hours: 7.5 }],
  expected_segments: [{ work_date: '2026-09-14', hours_day: 7.5 }],
  expected_actual_schedule: [{ date: '2026-09-14', start: '09:00', end: '17:00', break_minutes: 30 }],
  expected_source_expenses: [],
  expected_rate_source_refs: {
    schema_version: 'WEEKLY_SOURCE_FINAL_AUTHORITY_RATE_SOURCE_V1',
    root_timesheet_id: rootId,
  },
  root_state_hash: '8'.repeat(64),
  prepared_context_hash: preparedHash,
});

function serviceSnapshot(root) {
  return {
    schema_version: 'WEEKLY_SOURCE_ORDINARY_TSFIN_SERVICE_SNAPSHOT_V1',
    calculator_owner: 'buildWeeklyScheduleSegmentsSnapshot',
    source_actual_schedule_json: root.expected_actual_schedule,
    tsfin_snapshot_json: {
      timesheet_id: root.root_timesheet_id,
      rate_source_refs_json: root.expected_rate_source_refs,
      invoice_breakdown_json: { mode: 'SEGMENTS', segments: root.expected_segments },
    },
  };
}

function preparedResult(roots, overrides = {}) {
  return {
    ok: true,
    status: 'PREPARED',
    correction_session_id: ID.session,
    prior_final_revision_id: ID.priorRevision,
    final_revision_id: ID.preparedRevision,
    upload_id: ID.upload,
    version: 6,
    root_contexts: roots,
    office_preview: officePreview(),
    idempotent_replay: false,
    ...overrides,
  };
}

function reviewedResult(overrides = {}) {
  return {
    ok: true,
    status: 'READY_FOR_CONFIRMATION',
    correction_session_id: ID.session,
    prior_final_revision_id: ID.priorRevision,
    upload_id: ID.upload,
    version: 4,
    office_preview: officePreview(),
    idempotent_replay: false,
    ...overrides,
  };
}

function dependenciesFor(options = {}) {
  const calls = [];
  const roots = options.roots ?? [
    preparedRoot(ID.rootA, HASH.rootA),
    preparedRoot(ID.rootB, HASH.rootB),
  ];
  const prepared = options.prepared ?? preparedResult(roots, { idempotent_replay: options.replay === true });
  return {
    calls,
    dataRpc: async (name, args) => {
      calls.push([name, args]);
      if (name === 'weekly_source_correct_final_open_atomic_v1') {
        return options.opened ?? {
          ok: true,
          status: options.openStatus ?? 'DRAFT',
          correction_session_id: ID.session,
          version: 1,
          affected_root_count: roots.length,
          idempotent_replay: options.replay === true,
        };
      }
      if (name === 'weekly_source_correct_final_review_atomic_v1') {
        return options.reviewed ?? reviewedResult({ idempotent_replay: options.replay === true });
      }
      if (name === 'weekly_source_correct_final_prepare_atomic_v1') return prepared;
      if (name === 'weekly_source_correct_final_apply_atomic_v1') {
        return options.applied ?? {
          ok: true,
          status: 'CORRECTED',
          correction_session_id: ID.session,
          prior_final_revision_id: ID.priorRevision,
          final_revision_id: ID.appliedRevision,
          affected_root_count: roots.length,
          idempotent_replay: options.replay === true,
        };
      }
      throw new Error(`Unexpected RPC ${name}`);
    },
    stageReplacementSource: async (input) => {
      calls.push(['STAGE_REPLACEMENT', input]);
      return options.staged ?? {
        ok: true,
        status: 'CORRECTION_READY',
        correction_session_id: ID.session,
        replacement_upload_id: ID.upload,
        replacement_projection_publication_id: ID.publication,
        expected_authority_scope_version: 3,
        expected_row_manifest_hash: HASH.rows,
        expected_comparison_manifest_hash: HASH.comparison,
        expected_issue_set_hash: HASH.issues,
        version: 3,
      };
    },
    rebuildReplacementProjection: async (input) => {
      calls.push(['REBUILD_PROJECTION', input]);
      return options.rebuilt ?? {
        ok: true,
        status: 'CORRECTION_READY',
        correction_session_id: ID.session,
        replacement_upload_id: ID.upload,
        replacement_projection_publication_id: ID.rebuiltPublication,
        expected_authority_scope_version: 3,
        expected_row_manifest_hash: HASH.rows,
        expected_comparison_manifest_hash: HASH.rebuiltComparison,
        expected_issue_set_hash: HASH.rebuiltIssues,
        version: 5,
      };
    },
    buildOrdinaryServiceSnapshot: async (input) => {
      calls.push(['BUILD_ROOT', input]);
      return options.snapshotFor?.(input.root_context) ?? serviceSnapshot(input.root_context);
    },
  };
}

const preview = (dependencies, request = previewRequest()) => orchestrateWeeklyCorrectFinalPreview({
  request,
  actor: { id: ID.actor, role: 'admin' },
  env: { marker: 'TEST' },
  ctx: { marker: 'REQUEST' },
  dependencies,
});

const applyRequest = (review, overrides = {}) => ({
  actor_user_id: ID.actor,
  reason: 'The wrong source file was finalised.',
  confirmation_text: review.confirmation_text,
  correction_context: review.apply_context,
  idempotency_key: 'correct-final-source-apply-0001',
  ...overrides,
});

const apply = (dependencies, request) => orchestrateWeeklyCorrectFinalApply({
  request,
  actor: { id: ID.actor, role: 'admin' },
  env: { marker: 'TEST' },
  ctx: { marker: 'REQUEST' },
  dependencies,
});

test('preview uploads new bytes and returns a sealed review without materialising roots or applying authority', async () => {
  const dependencies = dependenciesFor();
  const review = await preview(dependencies);
  assert.equal(review.status, 'READY_FOR_CONFIRMATION');
  assert.deepEqual(review.changes, officePreview().changes);
  assert.deepEqual(review.blockers, []);
  assert.equal(review.confirmation_text, CONFIRMATION);
  assert.equal(review.apply_context.expected_preview_hash, HASH.preview);
  assert.equal(review.review_context.expected_session_version, 4);
  assert.deepEqual(dependencies.calls.map(([name]) => name), [
    'weekly_source_correct_final_open_atomic_v1',
    'STAGE_REPLACEMENT',
    'weekly_source_correct_final_review_atomic_v1',
  ]);
  assert.equal(dependencies.calls.some(([name]) => name.includes('apply_atomic')), false);
  assert.deepEqual(
    dependencies.calls.find(([name]) => name === 'STAGE_REPLACEMENT')[1].replacement_source,
    { file_key: 'weekly-source/new-replacement.xlsx' },
  );
  assert.equal(Object.hasOwn(previewRequest(), 'reason'), false);
  assert.equal(Object.hasOwn(previewRequest(), 'root_service_snapshots'), false);
});

test('a blocked preview returns exact blockers, no apply context, no calculation and never applies', async () => {
  const blockers = [{
    candidate: 'Alex Worker',
    day_date: 'Monday 14 September 2026',
    problem: 'A contract could not be matched.',
    action: 'Choose the correct contract before trying again.',
  }];
  const dependencies = dependenciesFor({
    reviewed: reviewedResult({
      status: 'BLOCKED',
      office_preview: officePreview({ blockers }),
    }),
  });
  const review = await preview(dependencies);
  assert.equal(review.status, 'BLOCKED');
  assert.deepEqual(review.blockers, blockers);
  assert.equal(review.apply_context, null);
  assert.ok(review.review_context);
  assert.equal(dependencies.calls.some(([name]) => name === 'BUILD_ROOT'), false);
  assert.equal(dependencies.calls.some(([name]) => name.includes('apply_atomic')), false);
});

test('apply reloads the exact staged preview, rebuilds service-owned snapshots and supplies the Office reason', async () => {
  const dependencies = dependenciesFor();
  const review = await preview(dependencies);
  dependencies.calls.length = 0;
  const result = await apply(dependencies, applyRequest(review));
  assert.equal(result.status, 'CORRECTED');
  assert.deepEqual(dependencies.calls.map(([name]) => name), [
    'weekly_source_correct_final_review_atomic_v1',
    'weekly_source_correct_final_prepare_atomic_v1',
    'BUILD_ROOT',
    'BUILD_ROOT',
    'weekly_source_correct_final_apply_atomic_v1',
  ]);
  const request = dependencies.calls.at(-1)[1].p_request;
  assert.equal(request.expected_preview_hash, HASH.preview);
  assert.equal(request.reason, 'The wrong source file was finalised.');
  assert.equal(request.confirmation_text, CONFIRMATION);
  assert.deepEqual(request.root_service_snapshots.map((entry) => entry.root_timesheet_id), [ID.rootA, ID.rootB]);
  const prepare = dependencies.calls.find(([name]) => name === 'weekly_source_correct_final_prepare_atomic_v1')[1].p_request;
  assert.equal(prepare.expected_preview_hash, HASH.preview);
  assert.equal(prepare.reason, 'The wrong source file was finalised.');
  assert.equal(prepare.confirmation_text, CONFIRMATION);
});

test('blocked Recheck rebuilds current mapping into a fresh publication without re-uploading or materialising', async () => {
  const blockers = [{
    candidate: 'Alex Worker',
    day_date: 'Monday 14 September 2026',
    problem: 'More than one contract matches',
    action: 'Choose contract',
  }];
  const firstDependencies = dependenciesFor({
    reviewed: reviewedResult({ status: 'BLOCKED', office_preview: officePreview({ blockers }) }),
  });
  const first = await preview(firstDependencies);
  const recheckDependencies = dependenciesFor({
    reviewed: reviewedResult({ version: 6 }),
  });
  const rechecked = await preview(recheckDependencies, {
    actor_user_id: ID.actor,
    correction_context: first.review_context,
    idempotency_key: 'correct-final-source-recheck-0001',
  });
  assert.equal(rechecked.status, 'READY_FOR_CONFIRMATION');
  assert.deepEqual(recheckDependencies.calls.map(([name]) => name), [
    'REBUILD_PROJECTION',
    'weekly_source_correct_final_review_atomic_v1',
  ]);
  const rebuild = recheckDependencies.calls[0][1];
  assert.equal(rebuild.replacement_upload_id, ID.upload);
  assert.equal(rebuild.replacement_projection_publication_id, ID.publication);
  assert.equal(Object.hasOwn(rebuild, 'replacement_source'), false);
  assert.equal(rechecked.apply_context.replacement_upload_id, ID.upload);
  assert.equal(
    rechecked.apply_context.replacement_projection_publication_id,
    ID.rebuiltPublication,
  );
  assert.equal(rechecked.apply_context.expected_comparison_manifest_hash, HASH.rebuiltComparison);
  assert.equal(rechecked.apply_context.expected_issue_set_hash, HASH.rebuiltIssues);
  assert.equal(rechecked.apply_context.expected_session_version, 6);
  assert.equal(rechecked.apply_context.review_expected_session_version, 5);
  assert.equal(recheckDependencies.calls.some(([name]) => name === 'STAGE_REPLACEMENT'), false);
  assert.equal(recheckDependencies.calls.some(([name]) => name === 'BUILD_ROOT'), false);
});

test('changed confirmation or stale preview fails before APPLY and leaves the prior authority untouched', async () => {
  const dependencies = dependenciesFor();
  const review = await preview(dependencies);
  dependencies.calls.length = 0;
  await assert.rejects(
    apply(dependencies, applyRequest(review, { confirmation_text: 'I confirm something else.' })),
    (error) => error instanceof WeeklyCorrectFinalSourceError
      && error.code === 'WEEKLY_SOURCE_CORRECTION_PREVIEW_STALE',
  );
  assert.equal(dependencies.calls.some(([name]) => name.includes('apply_atomic')), false);

  const staleDependencies = dependenciesFor();
  const alteredContext = { ...review.apply_context, expected_preview_hash: '9'.repeat(64) };
  await assert.rejects(
    apply(staleDependencies, applyRequest(review, { correction_context: alteredContext })),
    (error) => error instanceof WeeklyCorrectFinalSourceError
      && error.code === 'WEEKLY_SOURCE_CORRECTION_PREVIEW_STALE',
  );
  assert.equal(staleDependencies.calls.some(([name]) => name.includes('apply_atomic')), false);
});

test('browser financial snapshots, the former one-step request and wrong actor are refused', async () => {
  await assert.rejects(
    preview(dependenciesFor(), { ...previewRequest(), root_service_snapshots: [] }),
    (error) => error?.code === 'WEEKLY_SOURCE_CORRECTION_UNKNOWN_FIELD',
  );
  await assert.rejects(
    preview(dependenciesFor(), { ...previewRequest(), reason: 'Not accepted at preview.' }),
    (error) => error?.code === 'WEEKLY_SOURCE_CORRECTION_UNKNOWN_FIELD',
  );
  await assert.rejects(orchestrateWeeklyCorrectFinalPreview({
    request: previewRequest(),
    actor: { id: 'cf900000-0000-4000-8000-000000000002' },
    dependencies: dependenciesFor(),
  }), (error) => error?.code === 'WEEKLY_SOURCE_CORRECTION_ACTOR_MISMATCH');
});

test('Office routes expose separate preview and apply commands and reject the former one-step action', async () => {
  const dependencies = dependenciesFor({ roots: [preparedRoot(ID.rootA, HASH.rootA)] });
  const routeDependencies = {
    requireUser: async () => ({ id: ID.actor, role: 'admin' }),
    rpc: async () => { throw new Error('Direct route RPC must not run for Correct final source.'); },
    previewCorrectFinalSource: (input) => orchestrateWeeklyCorrectFinalPreview({ ...input, dependencies }),
    applyCorrectFinalSource: (input) => orchestrateWeeklyCorrectFinalApply({ ...input, dependencies }),
  };
  const command = async (action, payload) => dispatchWeeklySourceRequest(
    new Request('https://test.invalid/api/weekly-source/v1/commands', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ action, payload }),
    }),
    { marker: 'TEST' },
    { marker: 'REQUEST' },
    routeDependencies,
  );
  let response = await command('PREVIEW_CORRECT_FINAL_SOURCE', previewRequest({ actor_user_id: undefined }));
  assert.equal(response.status, 200);
  const review = await response.json();
  response = await command('APPLY_CORRECT_FINAL_SOURCE', applyRequest(review, { actor_user_id: undefined }));
  assert.equal(response.status, 200);
  assert.equal((await response.json()).status, 'CORRECTED');
  response = await command('CORRECT_FINAL_SOURCE', {});
  assert.equal(response.status, 400);
  assert.equal((await response.json()).error_code, 'WEEKLY_SOURCE_COMMAND_NOT_SUPPORTED');
});

test('the split boundary has no direct Workbench, Draft, Banking or invoice owner', () => {
  assert.deepEqual(WEEKLY_CORRECT_FINAL_SOURCE_ORCHESTRATION_CONTRACT.previewStages, [
    'OPEN', 'STAGE_REPLACEMENT', 'REVIEW_STAGED_SOURCE', 'RETURN_REVIEW',
  ]);
  assert.deepEqual(WEEKLY_CORRECT_FINAL_SOURCE_ORCHESTRATION_CONTRACT.applyStages, [
    'RELOAD_REVIEW', 'MATERIALISE_INACTIVE', 'BUILD_SERVICE_SNAPSHOTS', 'APPLY', 'VERIFY_RESULT',
  ]);
  assert.deepEqual(WEEKLY_CORRECT_FINAL_SOURCE_ORCHESTRATION_CONTRACT.recheckStages, [
    'REBUILD_STAGED_PROJECTION', 'REVIEW_STAGED_SOURCE', 'RETURN_REVIEW',
  ]);
  assert.equal(WEEKLY_CORRECT_FINAL_SOURCE_ORCHESTRATION_CONTRACT.acceptsBrowserFinancialSnapshots, false);
  assert.equal(WEEKLY_CORRECT_FINAL_SOURCE_ORCHESTRATION_CONTRACT.acceptsSavedReplacementUpload, false);
  assert.equal(WEEKLY_CORRECT_FINAL_SOURCE_ORCHESTRATION_CONTRACT.previewChangesAuthority, false);
  assert.equal(WEEKLY_CORRECT_FINAL_SOURCE_ORCHESTRATION_CONTRACT.mutatesBankingPay, false);
  assert.equal(WEEKLY_CORRECT_FINAL_SOURCE_ORCHESTRATION_CONTRACT.mutatesInvoices, false);

  const repositoryRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..');
  const source = fs.readFileSync(
    path.join(repositoryRoot, 'broker/src/weekly-source/correct-final-source-orchestrator.mjs'),
    'utf8',
  );
  assert.equal(/\bsbRpc\b|pay_workbench|pay_batch|create_draft|invoice_lines/i.test(source), false);
});
