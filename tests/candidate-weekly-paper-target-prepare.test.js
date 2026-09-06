import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';

const read = path => fs.readFileSync(new URL(`../${path}`, import.meta.url), 'utf8');
const sql = read('supabase/repeatable/30082026_0714_candidate_weekly_paper_target_ready_state_v1.sql');
const backend = read('broker/src/candidate-app-backend.js');
const release = JSON.parse(read('supabase/release/current-release.json'));
const proofPath = 'supabase/verification/30082026_0605_candidate_weekly_paper_target_prepare_verification.sql';
const proof = read(proofPath);

test('printed preparation materialises a missing weekly target through the existing canonical owner', () => {
  assert.match(sql, /create or replace function public\.candidate_weekly_paper_target_prepare_v1/i);
  assert.match(sql, /private\._candidate_session_context_v1\(/i);
  assert.match(sql, /v_workflow\.scope<>'WEEKLY'/i);
  assert.match(sql, /v_workflow\.workflow_kind not in \('CONTRACT_HOURS','CONTRACT_COMBINED'\)/i);
  assert.match(sql, /v_workflow\.state not in \('READY_FOR_MANAGER_APPROVAL','WORKER_SUBMITTED','AWAITING_PAPER_RETURN'\)/i);
  assert.match(sql, /candidate_paper_submission_allowed/i);
  assert.match(sql, /CANDIDATE_WEEKLY_CANONICAL_AUTHORITY_V1/i);
  assert.match(sql, /contract_week_manual_upsert_atomic\(/i);
  assert.match(sql, /p_materialise_staged_evidence=>false/i);
  assert.match(sql, /suppress_timesheet_evidence_materialisation/i);
  assert.match(sql, /target_timesheet_id=v_timesheet\.timesheet_id/i);
  assert.match(sql, /timesheet_id=v_timesheet\.timesheet_id[\s\S]*timesheet_id is null/i);
});

test('printed target preparation cannot authorise, duplicate, or cross workflow authority', () => {
  assert.match(sql, /pg_advisory_xact_lock/i);
  assert.match(sql, /for update/i);
  assert.match(sql, /WORKFLOW_GENERATION_CONFLICT/i);
  assert.match(sql, /CANDIDATE_IMMUTABLE_SUBMISSION_MISMATCH/i);
  assert.match(sql, /v_week\.timesheet_id is distinct from v_timesheet_id/i);
  assert.match(sql, /v_timesheet\.authorised_at_server is not null/i);
  assert.match(sql, /v_fin\.authorised_at_utc is not null/i);
  assert.match(sql, /v_fin\.paid_at_utc is not null/i);
  assert.match(sql, /v_fin\.locked_by_invoice_id is not null/i);
  assert.match(sql, /authorised_at_server','auth_name','auth_job_title','r2_auth_key'/i);
  assert.doesNotMatch(sql, /\b(?:delete from|truncate|drop table)\b/i);
});

test('private Worker prepares a durable worked target but preserves the established standalone expense PAPER path', () => {
  const prepare = backend.indexOf("if (dbAction === 'PAPER_PREPARE')");
  const transition = backend.indexOf("'candidate_workflow_transition_atomic_v1'", prepare);
  assert.ok(prepare > 0 && transition > prepare);
  const seam = backend.slice(prepare, transition);
  assert.match(seam, /workflowRow\(env,\s*workflowId\)/i);
  assert.match(seam, /workflow_kind\s*!==\s*'CONTRACT_EXPENSE'/i);
  assert.match(seam, /candidate_weekly_paper_target_prepare_v1/i);
  assert.match(seam, /p_expected_generation:\s*generation/i);
  assert.match(seam, /CANDIDATE_PAPER_TIMESHEET_NOT_READY/i);
  assert.match(backend, /workflow\.target_timesheet_id\s*\|\|\s*workflow\.anchor_timesheet_id/i);
  assert.match(backend, /candidatePaperReturnPackReceipts[\s\S]*finaliseWorkflow/i);
});

test('paper target preparation is service-only and its first-use proof is mandatory', () => {
  assert.match(sql, /revoke all on function public\.candidate_weekly_paper_target_prepare_v1[\s\S]*from public,anon,authenticated/i);
  assert.match(sql, /grant execute on function public\.candidate_weekly_paper_target_prepare_v1[\s\S]*to service_role/i);
  for (const list of [release.verificationFiles, release.newVerificationFiles]) {
    assert.ok(list.includes(proofPath));
  }
});

test('weekly PAPER first use derives one internally consistent fixed week', () => {
  assert.match(proof, /v_now timestamptz := '2026-08-30 06:05:00\+00'/i);
  assert.match(proof, /v_week_ending_date date := \('2026-08-30 06:05:00\+00'::timestamptz at time zone 'UTC'\)::date/i);
  assert.match(proof, /v_work_date date := \(\('2026-08-30 06:05:00\+00'::timestamptz at time zone 'UTC'\)::date-6\)/i);
  assert.match(proof, /'date',v_work_date,'day','MON'/i);
  assert.match(proof, /extract\(dow from v_week_ending_date\)::integer/i);
  assert.doesNotMatch(proof, /\bcurrent_date\b/i);
});
