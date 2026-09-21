import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const repositoryRoot = path.resolve(here, '..', '..');
const read = (relative) => fs.readFileSync(path.join(repositoryRoot, relative), 'utf8');

const weeklyApply = read('supabase/repeatable/21072026_1820_06_hr_weekly_apply_transactional.sql');
const weeklyPreview = read('supabase/repeatable/21072026_1820_13_hr_weekly_validation_preview.sql');
const catalogueOwner = read('supabase/repeatable/21072026_1820_00_import_review_internal_core.sql');
const catalogueCompatibilityOwner = read('supabase/repeatable/22082026_1706_daily_validation_compatibility_authorities_v1.sql');
const legacyResolution = read('supabase/repeatable/02082026_1626_hr_weekly_candidate_not_worked_resolution.sql');
const settingsResolver = read('supabase/repeatable/03092026_1641_contract_settings_effective_authority_v1.sql');
const dailyApply = read('supabase/repeatable/21072026_1820_08_hr_daily_apply_transactional.sql');
const followUp = read('broker/src/import-review-follow-up.js');
const broker = read('broker/src/index.js');

const weeklyCatalogueBranch = (source) => {
  const start = source.indexOf('-- Weekly validation-only issues use the installed comparison engine');
  assert.notEqual(start, -1, 'Weekly Timesheet-authority catalogue branch is missing');
  const end = source.indexOf('end if;', start);
  assert.notEqual(end, -1, 'Weekly Timesheet-authority catalogue branch is not bounded');
  return source.slice(start, end);
};

test('both installed catalogue roots carry the identical Weekly Timesheet-authority branch', () => {
  assert.equal(
    weeklyCatalogueBranch(catalogueCompatibilityOwner),
    weeklyCatalogueBranch(catalogueOwner)
  );
});

test('Weekly exact-match auto-target applies to the whole Timesheet and uses frozen reference policy', () => {
  assert.match(weeklyApply, /hi\.coverage_mode in \('COMPLETE_ALL','COMPLETE_SELECTED_CANDIDATES'\)/);
  assert.match(weeklyApply, /whole_timesheet\.segment_count=jsonb_array_length\(vr\.row_json->'comparisons'\)/);
  assert.match(weeklyApply, /not exists \([\s\S]*?from jsonb_array_elements\(vr\.row_json->'comparisons'\) comparison\(value\)/);
  assert.match(
    weeklyApply,
    /contract_settings_effective_get_v1\([\s\S]*?vr\.client_id[\s\S]*?vr\.contract_id[\s\S]*?'IMPORT'[\s\S]*?vr\.timesheet_id[\s\S]*?\) as payload/
  );
  assert.match(
    weeklyApply,
    /not coalesce\([\s\S]*?values,require_reference_to_pay[\s\S]*?or nullif\(btrim\(comparison\.value->>'ref_after'\),''\) is not null/
  );
  assert.match(weeklyApply, /from public\.nhsp_shifts matched_shift/);
  assert.match(weeklyApply, /matched_shift\.timesheet_id=vr\.timesheet_id/);
  assert.match(
    weeklyApply,
    /not coalesce\([\s\S]*?values,require_reference_to_pay[\s\S]*?or \([\s\S]*?matched_shift\.ref_num[\s\S]*?matched_shift\.hr_request_id/
  );
});

test('Weekly auto-authorisation reuses the central v2 policy resolver and ordinary bulk authoriser', () => {
  assert.match(catalogueOwner, /public\.import_auto_authorise_policy_resolve_v2\(/);
  assert.match(settingsResolver, /create or replace function public\.import_auto_authorise_policy_resolve_v2\(/i);
  assert.match(weeklyApply, /public\._import_review_auto_authorise_targets_core_v1\(/);
  // WP-51 / WP-48 F2.  The ordinary bulk authoriser is still the one owner this
  // path uses; what changed is that the call now goes through the established
  // guard-refusal recording funnel instead of a raw `sbRpc`, so an E29 refusal
  // is recorded as well as failed closed.  The assertion keeps its intent - the
  // ordinary bulk authoriser and nothing bespoke - and additionally pins the
  // funnel, so a silent return to the unrecorded call would fail here too.
  assert.match(followUp, /sbRpcRecordingGuardRefusal\(env, 'timesheet_authorise_bulk_atomic'/);
  assert.doesNotMatch(followUp, /[^g]sbRpc\(env, 'timesheet_authorise_bulk_atomic'/);
  assert.doesNotMatch(followUp, /weekly[_-]timesheet[_-]authority[_-]authorise/i);
});

for (const [name, source] of [
  ['canonical catalogue owner', catalogueOwner],
  ['compatibility catalogue owner', catalogueCompatibilityOwner]
]) {
  test(`${name} keeps incomplete Weekly Timesheets non-selectable and workflow-aware`, () => {
    const branch = weeklyCatalogueBranch(source);
    assert.match(branch, /from public\.candidate_submission_workflows w/);
    assert.match(branch, /w\.scope='WEEKLY'/);
    assert.match(branch, /w\.workflow_kind in \('CONTRACT_HOURS','CONTRACT_COMBINED'\)/);
    assert.match(branch, /WEEKLY_TIMESHEET_NOT_SUBMITTED/);
    assert.match(branch, /Waiting for candidate to submit/);
    assert.match(branch, /WEEKLY_TIMESHEET_AWAITING_MANAGER_APPROVAL/);
    assert.match(branch, /Waiting for manager approval/);
    assert.match(branch, /m\.missing_reason_code[\s\S]*?'ADVISORY','BLOCKED'/);
    assert.doesNotMatch(branch, /omitted_shifts as \(/);
  });

  test(`${name} routes every present Weekly mismatch, including roster-only, to existing manager email`, () => {
    const branch = weeklyCatalogueBranch(source);
    assert.match(branch, /coalesce\(cx\.value->>'match_status','MATCH'\) <> 'MATCH'/);
    assert.doesNotMatch(branch, /coalesce\(cx\.value->>'match_status','MATCH'\) <> 'HR_ONLY'/);
    assert.match(branch, /case when a\.issue_id is null then 'EMAIL_ISSUE' else 'EMAIL_REMINDER' end/);
    assert.match(branch, /case when coalesce\(\(a\.protection->>'active_pay_draft'\)::boolean,false\) then 'BLOCKED' else 'EMAIL' end/);
    assert.doesNotMatch(branch, /'WEEKLY_CANDIDATE_DID_NOT_WORK'[\s\S]*?'ADVISORY','PENDING'/);
    assert.doesNotMatch(branch, /candidate_query/i);
    assert.doesNotMatch(branch, /secure_manager/i);
  });
}

test('historical candidate-did-not-work evidence cannot suppress a current roster-only mismatch', () => {
  assert.match(
    weeklyPreview,
    /confirmed_hr_exceptions as \([\s\S]*?from hr_exception_evidence he[\s\S]*?where false[\s\S]*?\)/
  );
  assert.match(weeklyPreview, /match_status <> 'MATCH'/);
  assert.match(legacyResolution, /_import_review_assert_actor_v1[\s\S]*?HR_WEEKLY_CANDIDATE_NOT_WORKED_ROUTE_RETIRED/);
  assert.match(legacyResolution, /import_review_weekly_validation_resolutions/);
});

test('settings transport preserves existing global and Client auto-authorisation choices', () => {
  for (const field of [
    'healthroster_import_auto_authorise_default',
    'nhsp_import_auto_authorise_default',
    'auto_authorise_on_validation'
  ]) {
    assert.match(broker, new RegExp(`'${field}'`));
  }
  assert.match(
    broker,
    /\[\s*'healthroster_import_auto_authorise_default',[\s\S]*?'nhsp_import_auto_authorise_default',[\s\S]*?'auto_authorise_on_validation'[\s\S]*?\]\.includes\(k\)[\s\S]*?parseStrictBoolean\(data\[k\], k\)/
  );
  assert.match(broker, /'healthroster_import_auto_authorise','nhsp_import_auto_authorise'/);
  assert.match(broker, /csInput\.healthroster_import_auto_authorise[\s\S]*?csInput\.nhsp_import_auto_authorise/);
  assert.match(
    broker,
    /const BOOL_KEYS = \[[\s\S]*?'healthroster_import_auto_authorise',[\s\S]*?'nhsp_import_auto_authorise'/
  );
});

test('Daily remains on its existing owner and is not given Weekly policy or workflow rules', () => {
  assert.match(dailyApply, /t\.reference_number=u\.new_hr_request_id/);
  assert.match(
    dailyApply,
    /_import_review_auto_authorise_targets_core_v1\([\s\S]*?'HEALTHROSTER_DAILY'::public\.hr_source_enum,true/
  );
  assert.doesNotMatch(dailyApply, /contract_settings_effective_get_v1\(/);
  assert.doesNotMatch(dailyApply, /WEEKLY_TIMESHEET_NOT_SUBMITTED|WEEKLY_TIMESHEET_AWAITING_MANAGER_APPROVAL/);
});
