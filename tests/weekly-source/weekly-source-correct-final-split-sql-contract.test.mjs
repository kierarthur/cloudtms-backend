import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(here, '..', '..');
const read = relativePath => fs.readFileSync(path.join(root, relativePath), 'utf8')
  .replaceAll('\r\n', '\n');

const owner = read('supabase/repeatable/15092026_1534_weekly_source_correct_final_source_v1.sql');
const verifier = read('supabase/verification/15092026_1534_weekly_source_correct_final_source_v1.sql');
const schema = read('supabase/migrations/15092026_1534_weekly_source_plan6_schema.sql');
const uploadPublication = read(
  'supabase/repeatable/15092026_1534_weekly_source_upload_publication_v1.sql',
);
const projectionBuild = read(
  'supabase/repeatable/15092026_1534_weekly_source_projection_build_v1.sql',
);
const uploadContext = read(
  'supabase/repeatable/15092026_1534_weekly_source_upload_context_v1.sql',
);
const finalisation = read(
  'supabase/repeatable/15092026_1534_weekly_source_finalisation_v1.sql',
);
const ordinaryProjection = read(
  'supabase/repeatable/15092026_1534_weekly_source_ordinary_pay_projection_v1.sql',
);
const ordinaryProjectionVerifier = read(
  'supabase/verification/15092026_1534_weekly_source_ordinary_pay_projection_v1.sql',
);

function sourceSection(source, startText, endText) {
  const start = source.indexOf(startText);
  const end = source.indexOf(endText, start + startText.length);
  assert.ok(start >= 0, `${startText} is missing`);
  assert.ok(end > start, `${endText} is missing after ${startText}`);
  return source.slice(start, end);
}

const section = (startText, endText) => sourceSection(owner, startText, endText);

const previewOwner = section(
  'create or replace function private.weekly_source_correct_final_office_preview_v1',
  'create or replace function public.weekly_source_correct_final_review_atomic_v1',
);
const reviewOwner = section(
  'create or replace function public.weekly_source_correct_final_review_atomic_v1',
  'create or replace function public.weekly_source_correct_final_prepare_atomic_v1',
);
const preparedContextOwner = section(
  'create or replace function private.weekly_source_correct_final_prepared_context_v1',
  'create or replace function private.weekly_source_correct_final_office_preview_v1',
);
const prepareOwner = section(
  'create or replace function public.weekly_source_correct_final_prepare_atomic_v1',
  'create or replace function public.weekly_source_correct_final_apply_atomic_v1',
);
const applyOwner = section(
  'create or replace function public.weekly_source_correct_final_apply_atomic_v1',
  'alter function private.weekly_source_correct_final_preconditions_v1',
);
const projectionBeginOwner = sourceSection(
  uploadPublication,
  'create or replace function public.weekly_source_projection_begin_atomic_v1',
  'create or replace function public.weekly_source_projection_publish_atomic_v1',
);
const projectionPublishOwner = sourceSection(
  uploadPublication,
  'create or replace function public.weekly_source_projection_publish_atomic_v1',
  'create or replace function private.weekly_source_current_publication_guard_v1',
);

test('Office preview is a private stable read owner with fixed plain-English Changes and Blocked fields', () => {
  assert.match(previewOwner, /language plpgsql stable security definer/i);
  assert.match(previewOwner, /'changes',v_changes,'blockers',v_blockers,'confirmation_text',v_confirmation/i);
  for (const field of ['candidate', 'day_date', 'current_final', 'replacement', 'result', 'problem', 'action']) {
    assert.ok(previewOwner.includes(`'${field}'`), `${field} is missing from the Office review`);
  }
  assert.match(previewOwner, /WEEKLY_SOURCE_CORRECT_FINAL_OFFICE_PREVIEW_V1/i);
  assert.doesNotMatch(previewOwner, /\b(insert|update|delete|truncate)\s+(into\s+|public\.|private\.)/i);
});

test('REVIEW seals Changes and Blocked while touching only its correction-session record', () => {
  assert.match(reviewOwner, /WEEKLY_SOURCE_CORRECT_FINAL_REVIEW_V1/i);
  assert.match(reviewOwner, /set state='REVIEWED'/i);
  assert.match(reviewOwner, /then 'BLOCKED' else 'READY_FOR_CONFIRMATION'/i);
  assert.doesNotMatch(reviewOwner, /(insert\s+into|update|delete\s+from)\s+public\.(timesheets|contract_weeks|weekly_source_row_timesheet_lineages|timesheets_financials|weekly_source_billing_movements|weekly_query_incidents|weekly_manager_route_receipts)\b/i);
  assert.doesNotMatch(reviewOwner, /weekly_source_finalise_engine_v1|weekly_source_timesheet_lineage_ensure_atomic_v1/i);
});

test('REVIEW reports a replacement source-family mismatch separately from stale identity', () => {
  const staleRaise = reviewOwner.indexOf(
    "raise exception 'WEEKLY_SOURCE_CORRECTION_REPLACEMENT_STALE'",
  );
  const staleStart = reviewOwner.lastIndexOf('if v_prior_revision.state', staleRaise);
  const familyCheck = reviewOwner.indexOf(
    'if v_replacement_profile.final_authority_kind is distinct from v_prior_profile.final_authority_kind',
    staleRaise,
  );
  const familyRaise = reviewOwner.indexOf(
    "raise exception 'WEEKLY_SOURCE_CORRECTION_SOURCE_FAMILY_MISMATCH'",
    familyCheck,
  );
  assert.ok(staleStart >= 0 && staleRaise > staleStart);
  assert.doesNotMatch(reviewOwner.slice(staleStart, staleRaise), /final_authority_kind/i);
  assert.ok(familyCheck > staleRaise && familyRaise > familyCheck);
  assert.match(reviewOwner.slice(familyRaise, familyRaise + 120), /errcode='55000'/i);
});

test('PREPARE requires the reviewed proof, reason and exact confirmation before inactive materialisation', () => {
  assert.ok(prepareOwner.includes("'expected_preview_hash'"));
  assert.ok(prepareOwner.includes("'reason','confirmation_text'"));
  assert.match(prepareOwner, /v_session\.state<>'REVIEWED'/i);
  const preview = prepareOwner.indexOf('v_office_preview:=private.weekly_source_correct_final_office_preview_v1');
  const exactReview = prepareOwner.indexOf("v_session.review_result_json->'office_preview' is distinct from v_office_preview", preview);
  const preparing = prepareOwner.indexOf("set state='PREPARING'", preview);
  const inactivePrepare = prepareOwner.indexOf('private.weekly_source_finalise_engine_v1', preparing);
  const sealedPreview = prepareOwner.lastIndexOf("'office_preview',v_office_preview");
  assert.ok(preview >= 0 && exactReview > preview && preparing > exactReview && inactivePrepare > preparing);
  assert.ok(sealedPreview > inactivePrepare);
  assert.match(prepareOwner, /v_session\.id,true/i);
});

test('prepared contexts cover prior-only, replacement-only, changed and unchanged roots', () => {
  assert.match(preparedContextOwner, /with prior_roots as/i);
  assert.match(preparedContextOwner, /replacement_roots as/i);
  assert.match(preparedContextOwner, /full join replacement_roots replacement using\(root_timesheet_id\)/i);
  assert.match(preparedContextOwner, /prior_projection_receipt_id',v_root\.prior_projection_receipt_id/i);
  assert.match(applyOwner, /when v_root\.prior_receipt_id is null\s+then 'PUBLISHED_REPLACEMENT_ONLY_ROOT'/i);
  assert.match(applyOwner, /REPROJECTED_WITHOUT_CURRENT_REVISION_MOVEMENTS/i);
  assert.match(applyOwner, /REPROJECTED_WITH_CURRENT_REVISION_MOVEMENTS/i);
});

test('APPLY proves exact reason, confirmation and preview before COMMITTING or changing authority', () => {
  assert.ok(applyOwner.includes("'expected_preview_hash'"));
  assert.ok(applyOwner.includes("'reason','confirmation_text'"));
  assert.match(applyOwner, /v_reason:=pg_catalog\.btrim/i);
  assert.match(applyOwner, /v_confirmation_text:=pg_catalog\.btrim/i);
  const livePreview = applyOwner.indexOf('v_live_preview:=private.weekly_source_correct_final_office_preview_v1');
  const committing = applyOwner.indexOf("set state='COMMITTING'", livePreview);
  const supersede = applyOwner.indexOf("set state='SUPERSEDED' where id=v_prior_revision.id", livePreview);
  const authority = applyOwner.indexOf('current_final_revision_id=v_prepared_revision.id', livePreview);
  assert.ok(livePreview >= 0 && committing > livePreview && supersede > committing && authority > supersede);
  assert.match(applyOwner, /v_session\.prepare_result_json->'office_preview' is distinct from v_live_preview/i);
  assert.match(applyOwner, /v_confirmation_text is distinct from v_live_preview->>'confirmation_text'/i);
  assert.match(applyOwner, /reason=v_reason/i);
  const unionRootPreflight = applyOwner.indexOf('v_root_preflight:=public.import_timesheet_financial_preflight_v1');
  assert.ok(unionRootPreflight > livePreview && committing > unionRootPreflight);
  assert.match(applyOwner, /array\[v_root\.root_timesheet_id\][\s\S]*'WEEKLY_SOURCE_CORRECT_FINAL_SOURCE'/i);
});

test('the rollback verifier submits the sealed preview proof for ordinary and NHSP apply paths', () => {
  assert.ok((verifier.match(/'expected_preview_hash'/g) || []).length >= 2);
  assert.ok((verifier.match(/'confirmation_text'/g) || []).length >= 2);
  assert.match(verifier, /'reason','Replace the mistaken same-cycle final source\.'/i);
  assert.match(verifier, /'reason','Replace the mistaken NHSP Trust backing report\.'/i);
  assert.match(verifier, /^rollback;$/m);
});

test('the rollback verifier exercises the full prior/replacement root union and a read-only REVIEW', () => {
  assert.match(verifier, /do \$review_union_side_effect_free\$/i);
  assert.match(verifier, /from public\.weekly_source_row_timesheet_lineages/i);
  assert.match(verifier, /union REVIEW created live Timesheet or lineage state/i);
  assert.match(verifier, /PUBLISHED_REPLACEMENT_ONLY_ROOT/i);
  assert.match(verifier, /REPROJECTED_WITHOUT_CURRENT_REVISION_MOVEMENTS/i);
  assert.match(verifier, /REPROJECTED_WITH_CURRENT_REVISION_MOVEMENTS/i);
  assert.match(verifier, /the four rebuilt roots must contain the corrected, removed, unchanged and added hours/i);
  assert.match(verifier, /'lifecycle','OPEN_REVIEW_PREPARE_APPLY'/i);
});

test('Recheck builds a fresh generation over the immutable correction upload and seals its request', () => {
  assert.match(schema, /projection_generation integer/i);
  assert.match(schema, /rebuild_idempotency_key text/i);
  assert.match(schema, /rebuild_request_hash bytea/i);
  assert.match(schema, /ready_session_version bigint/i);
  assert.match(projectionBeginOwner, /expected_projection_publication_id/i);
  assert.match(projectionBeginOwner, /expected_row_manifest_hash/i);
  assert.match(projectionBeginOwner, /expected_comparison_manifest_hash/i);
  assert.match(projectionBeginOwner, /expected_issue_set_hash/i);
  assert.match(projectionBeginOwner, /v_prior_publication\.upload_id is distinct from v_upload\.id/i);
  assert.match(projectionBeginOwner, /v_prior_publication\.comparison_manifest_hash is distinct from v_expected_comparison_hash/i);
  assert.match(projectionBeginOwner, /max\(coalesce\([\s\S]*projection_generation[\s\S]*authority_scope_version::integer/i);
  assert.match(projectionBeginOwner, /v_projection_generation:=coalesce\(v_projection_generation,0\)\+1/i);
  assert.match(projectionBeginOwner, /insert into public\.weekly_source_projection_publications\([\s\S]*projection_generation[\s\S]*rebuild_idempotency_key/i);
  assert.doesNotMatch(projectionBeginOwner, /(insert\s+into|update|delete\s+from)\s+public\.weekly_source_upload_rows\b/i);
});

test('fresh correction publication rows and every downstream review use its generation', () => {
  assert.match(projectionBuild, /v_generation:=coalesce\(\s*v_publication\.projection_generation,\s*v_publication\.authority_scope_version::integer\s*\)/i);
  assert.match(previewOwner, /resolution\.generation=coalesce\(\s*v_publication\.projection_generation,\s*v_publication\.authority_scope_version::integer\s*\)/i);
  assert.match(previewOwner, /charge\.generation=coalesce\(\s*v_publication\.projection_generation,\s*v_publication\.authority_scope_version::integer\s*\)/i);
  assert.match(finalisation, /v_generation:=coalesce\(\s*v_publication\.projection_generation,\s*v_scope_version::integer\s*\)/i);
  assert.doesNotMatch(projectionBuild, /delete\s+from\s+public\.weekly_source_(upload_rows|row_resolutions)\b/i);
});

test('a newest zero expense authority remains the tombstone but is not Timesheet expense evidence', () => {
  const currentExpenses = sourceSection(
    ordinaryProjection,
    'create or replace function private.weekly_source_ordinary_projection_current_expenses_v1',
    'create or replace function private.weekly_source_ordinary_projection_actual_schedule_v1',
  );
  const ranking = currentExpenses.indexOf('pg_catalog.row_number() over');
  const positiveManifest = currentExpenses.indexOf('and ranked.source_expense_pence>0', ranking);
  assert.ok(ranking >= 0 && positiveManifest > ranking);
  assert.match(
    currentExpenses,
    /where ranked\.event_rank=1\s+and ranked\.source_expense_pence>0/i,
  );
});

test('ordinary projection verifier snapshots clear superseded source-fixed expense evidence', () => {
  const requestBuilder = sourceSection(
    ordinaryProjectionVerifier,
    'create function pg_temp.ordinary_projection_request(',
    'create function pg_temp.target_zero_request(',
  );
  assert.match(
    requestBuilder,
    /elsif coalesce\(v_current\.expenses_evidence_manifest->>'schema_version',''\)=\s*'WEEKLY_SOURCE_FIXED_EXPENSE_TSFIN_LINEAGE_V1' then/i,
  );
  assert.match(
    requestBuilder,
    /v_expense_pay:=0;\s*v_expense_charge:=0;\s*v_expense_description:=null;\s*v_expense_evidence_r2_key:=null;\s*v_expense_evidence_manifest:='null'::jsonb;/i,
  );
});

test('ordinary verifier can build an explicit prior-only root snapshot without replacement movements', () => {
  const requestBuilder = sourceSection(
    ordinaryProjectionVerifier,
    'create function pg_temp.ordinary_projection_request(',
    'create function pg_temp.target_zero_request(',
  );
  assert.match(
    requestBuilder,
    /if p_root_timesheet_id is null then[\s\S]*from public\.weekly_source_billing_movements movement[\s\S]*else[\s\S]*v_root:=p_root_timesheet_id;[\s\S]*from public\.timesheets timesheet\s+join public\.contracts contract on contract\.id=timesheet\.contract_id[\s\S]*where timesheet\.timesheet_id=v_root;\s+end if;/i,
  );
});

test('four-root Correct Final proof uses isolated weeks outside the earlier happy-path root', () => {
  const unionPrior = sourceSection(
    verifier,
    'create function pg_temp.correct_final_seed_union_prior(',
    'create function pg_temp.correct_final_stage_union_replacement(',
  );
  const unionReplacement = sourceSection(
    verifier,
    'create function pg_temp.correct_final_stage_union_replacement(',
    'create function pg_temp.correct_final_open_request(',
  );
  for (const date of ['2027-03-01', '2027-03-08', '2027-03-15']) {
    assert.ok(unionPrior.includes(`'${date}'`), `${date} is missing from the prior union fixture`);
  }
  assert.match(unionReplacement, /'UNION-ADDED',[\s\S]*'2027-03-22'/i);
  assert.match(unionReplacement, /'2027-03-01','2027-03-22','2027-03-01','2027-03-22','Europe\/London'/i);
  assert.doesNotMatch(`${unionPrior}\n${unionReplacement}`, /2026-12-(07|14|21|28)/);
  assert.match(verifier, /week_ending_date='2027-03-28'[\s\S]*financial\.total_hours=7\.50/i);
  const contractExtension = verifier.indexOf("set end_date='2027-04-30'");
  const unionSeed = verifier.indexOf('select pg_temp.correct_final_seed_union_prior(');
  assert.ok(contractExtension >= 0 && unionSeed > contractExtension);
});

test('concurrent rebuild retries share scope-first locking and replay a completed row generation', () => {
  const advisory = projectionBuild.indexOf('pg_catalog.pg_advisory_xact_lock');
  const scopeLock = projectionBuild.indexOf('for update;', advisory);
  const publicationLock = projectionBuild.indexOf('where id=p_publication_id\n  for update;', scopeLock);
  assert.ok(advisory >= 0 && scopeLock > advisory && publicationLock > scopeLock);
  assert.match(projectionBuild, /if v_publication\.state='CORRECTION_READY' or v_existing>0 then/i);
  assert.match(projectionBuild, /'applied_row_count',v_existing,'idempotent',true/i);
  assert.match(projectionBuild, /source_row_fingerprint=source_row\.normalised_row_hash/i);
});

test('publish CAS-swaps only the correction review pointer and clears the superseded review proof', () => {
  assert.match(projectionPublishOwner, /set replacement_projection_publication_id=v_publication\.id,[\s\S]*state='READY',[\s\S]*review_idempotency_key=null,review_request_hash=null,[\s\S]*review_result_json=null,review_result_hash=null/i);
  assert.match(projectionPublishOwner, /where id=v_publication\.correction_session_id and state='REVIEWED'[\s\S]*and version=v_publication\.ready_session_version/i);
  assert.match(projectionPublishOwner, /and replacement_correction_upload_id=v_publication\.upload_id[\s\S]*and replacement_projection_publication_id is not null[\s\S]*and replacement_projection_publication_id<>v_publication\.id/i);
  assert.match(uploadContext, /publication\.id=correction\.replacement_projection_publication_id/i);
  const correctionReplay = projectionPublishOwner.indexOf("if v_publication.state='CORRECTION_READY' then");
  const replayHashGuard = projectionPublishOwner.indexOf(
    "raise exception 'WEEKLY_SOURCE_PUBLICATION_HASH_MISMATCH'",
    correctionReplay,
  );
  const replayReturn = projectionPublishOwner.indexOf('return pg_catalog.jsonb_build_object(', correctionReplay);
  assert.ok(correctionReplay >= 0 && replayHashGuard > correctionReplay && replayReturn > replayHashGuard);
  assert.doesNotMatch(projectionPublishOwner, /(insert\s+into|update|delete\s+from)\s+public\.(pay_batches|pay_batch_items|pay_workbench|banking_pay|invoices|invoice_lines)\b/i);
});

test('Correct final owner still has no Workbench, Draft, Banking or invoice mutations', () => {
  const executable = owner.replace(/--[^\n]*/g, '').replace(/\/\*[\s\S]*?\*\//g, '');
  assert.doesNotMatch(executable, /(insert\s+into|update|delete\s+from)\s+public\.(pay_batches|pay_batch_items|pay_workbench|banking_pay)\b/i);
  assert.doesNotMatch(executable, /(insert\s+into|update|delete\s+from)\s+public\.(invoices|invoice_lines)\b/i);
});
