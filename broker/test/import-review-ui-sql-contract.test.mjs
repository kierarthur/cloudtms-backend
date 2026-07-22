import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const uiSql = readFileSync(new URL('../../supabase/repeatable/22072026_0052_import_review_ui_contract_v1.sql', import.meta.url), 'utf8');
const emailSql = readFileSync(new URL('../../supabase/repeatable/21072026_1820_04_timesheet_query_email_rpcs.sql', import.meta.url), 'utf8');
const lifecycleSql = readFileSync(new URL('../../supabase/repeatable/21072026_1820_01_import_review_lifecycle_rpcs.sql', import.meta.url), 'utf8');
const coreSql = readFileSync(new URL('../../supabase/repeatable/21072026_1820_00_import_review_internal_core.sql', import.meta.url), 'utf8');
const dailySql = readFileSync(new URL('../../supabase/repeatable/21072026_1820_03_import_review_daily_resolution_and_previews.sql', import.meta.url), 'utf8');
const weeklyApplySql = readFileSync(new URL('../../supabase/repeatable/21072026_1820_06_hr_weekly_apply_transactional.sql', import.meta.url), 'utf8');
const weeklyPreviewSql = readFileSync(new URL('../../supabase/repeatable/21072026_1820_13_hr_weekly_validation_preview.sql', import.meta.url), 'utf8');
const retirementSql = readFileSync(new URL('../../supabase/repeatable/21072026_1820_99_import_review_hard_cutover_retirements.sql', import.meta.url), 'utf8');
const incrementalMigration = readFileSync(new URL('../../supabase/migrations/22072026_1700_import_review_incremental_outcomes.sql', import.meta.url), 'utf8');

function functionBody(source, name) {
  const marker = `create or replace function public.${name}(`;
  const start = source.toLowerCase().indexOf(marker.toLowerCase());
  assert.notEqual(start, -1, `${name} must be defined`);
  const end = source.toLowerCase().indexOf('$function$;', start);
  assert.notEqual(end, -1, `${name} must have a complete body`);
  return source.slice(start, end + '$function$;'.length);
}

test('the fail-closed database contract exposes the bounded review UI and recipient grouping versions', () => {
  const body = functionBody(uiSql, 'import_review_contract_version_get_v1');
  assert.match(body, /incremental_apply_version','IMPORT_REVIEW_INCREMENTAL_APPLY_V1/);
  assert.match(body, /review_ui_contract_version','IMPORT_REVIEW_UI_V5/);
  assert.match(body, /email_grouping_version','TIMESHEET_QUERY_RECIPIENT_EMAIL_V1/);
  assert.match(body, /legacy_contracts_supported',false/);
  assert.match(lifecycleSql, /review_ui_contract_version','IMPORT_REVIEW_UI_V5/);
});

test('staged scope discovery is actor-bound, source-owned and bounded', () => {
  const body = functionBody(uiSql, 'import_review_staged_scope_get_v1');
  assert.match(body, /_import_review_assert_actor_v1/);
  assert.match(body, /v_row_count>500/);
  assert.match(body, /v_size not in \(25,50,75,100,500\)/);
  assert.match(body, /source_file_sha256/);
  assert.match(body, /coverage_start_date/);
  assert.match(body, /candidate_total_pages/);
  assert.match(body, /_import_review_overlap_preflight_core_v2/);
  assert.match(body, /overlapping_unfinished_reviews/);
  assert.match(body, /authority_mode/);
  assert.match(body, /authority_summary/);
  assert.match(body, /DAILY_EXISTING_TIMESHEET_VALIDATION/);
  assert.match(body, /_import_review_effective_authority_core_v1/);
  assert.match(body, /CURRENT_CLIENT_AND_CONTRACT_SETTINGS/);
  assert.match(body, /CURRENT_NHSP_SETTINGS/);
  assert.match(body, /settings_as_of_date/);
  assert.doesNotMatch(body, /effective_from<=ric\.week_ending_date/);
});

test('action paging supports only the approved sizes and deterministic server-side sorts', () => {
  const body = functionBody(uiSql, 'import_review_actions_page_v1');
  assert.match(body, /v_size not in \(25,50,75,100\)/);
  assert.match(body, /candidate_surname_sort/);
  assert.match(body, /action_id asc/);
  assert.match(body, /'has_previous'/);
  assert.match(body, /'has_next'/);
  assert.match(body, /'view_counts'/);
  for (const field of ['imported_evidence', 'current_evidence', 'difference_codes', 'evidence_rows', 'outcome_label', 'resolution_kind', 'resolution_options']) {
    assert.match(body, new RegExp(`'${field}'`));
  }
  assert.match(body, /'role',t\.tsfin_role,'band',t\.tsfin_band/);
  assert.match(body, /ct\.ts_queries_alt_email_address/);
  assert.match(body, /cl\.ts_queries_email/);
  assert.match(body, /RECIPIENT_UNAVAILABLE:/);
  assert.match(body, /current_weekly_options\.options/);
  assert.match(body, /option_contract\.candidate_id=d\.candidate_id/);
  assert.match(body, /option_contract\.client_id=d\.client_id/);
  assert.match(body, /option_authority\.route_eligible/);
  assert.match(body, /badges\.badge_code<>'NOT_IN_CLOUDTMS'/);
  assert.match(body, /other\.badge_code<>'NOT_IN_CLOUDTMS'/);
  assert.match(body, /Shift not in CloudTMS/);
  assert.doesNotMatch(body, /_timesheet_query_recipient_resolve_core_v1/);
});

test('weekly action classification consumes the established phase2 mapping authority', () => {
  const body = functionBody(coreSql, '_import_review_action_catalog_core_v1');
  assert.match(body, /weekly_phase as materialized/);
  assert.match(body, /public\.weekly_import_phase2/);
  assert.match(body, /assignment_band_mappings/);
  assert.match(body, /WEEKLY_ASSIGNMENT_CONTRACT/);
  assert.match(body, /CONTRACT_OUT_OF_SCOPE/);
  assert.match(body, /CONTRACT_RATES_INCOMPLETE/);
  assert.match(body, /weekly_mapping_evidence/);
  assert.match(body, /contract_rate_evidence/);
  assert.match(body, /import_authoritative/);
  assert.match(body, /evidenced as\s*\(\s*select c\.\*,[\s\S]*?from facts c/);
  assert.doesNotMatch(body, /evidenced as\s*\(\s*select f\.\*,[\s\S]*?from facts c/);
  assert.match(body, /when not coalesce\(f\.import_authoritative,false\) then 'NO_ACTION'/);
  assert.match(body, /Validate candidate timesheet/);
  assert.match(body, /WEEKLY_TIMESHEET_NOT_SUBMITTED/);
  assert.match(body, /WEEKLY_SHIFT_ABSENT_FROM_TIMESHEET/);
  assert.match(body, /DAILY_TIMESHEET_NOT_SUBMITTED/);
  assert.match(body, /DAILY_SHIFT_ABSENT_FROM_TIMESHEET/);
  assert.match(body, /Candidate timesheet states they did not work this shift/);
  assert.match(body, /match_status','MATCH'\)<>'HR_ONLY'/);
  assert.match(body, /validation-email-v2/);
  assert.match(body, /and not coalesce\(m\.contract_rate_complete,false\) then 'CONTRACT_RATES_INCOMPLETE'/);
  assert.match(body, /source_route_eligible',coalesce\(o\.route_eligible,false\)/);
  assert.match(body, /'selectable',coalesce\(o\.route_eligible,false\)/);
  assert.match(body, /CONTRACT_NOT_ELIGIBLE/);
  assert.match(body, /'evidence_rows'/);
  assert.match(body, /healthroster_start/);
  assert.match(body, /timesheet_start/);
});

test('a missing Weekly timesheet retains one server-owned imported-evidence action per shift', () => {
  const catalogue = functionBody(coreSql, '_import_review_action_catalog_core_v1');
  const preview = functionBody(weeklyPreviewSql, 'hr_weekly_validation_preview');
  assert.match(preview, /'day_status', 'TIMESHEET_NOT_SUBMITTED'/);
  assert.match(preview, /'comparison_key', 'hr-row:'\|\|hf\.hr_row_id::text/);
  assert.match(preview, /'healthroster_start', hf\.hr_start_hhmm/);
  assert.match(preview, /'healthroster_end', hf\.hr_end_hhmm/);
  assert.match(preview, /'healthroster_break_mins', hf\.hr_break_mins/);
  assert.match(preview, /'healthroster_paid_minutes', hf\.hr_paid_minutes/);
  assert.match(preview, /'ref_after', hf\.hr_request_id/);
  assert.match(catalogue, /d\.hr_row_id shift_hr_row_id/);
  assert.match(catalogue, /m\.shift_summary_json\|\|jsonb_build_object/);
  assert.match(catalogue, /weekly-timesheet-not-submitted-v2/);
  assert.match(catalogue, /'WEEKLY_TIMESHEET_NOT_SUBMITTED',m\.shift_hr_row_id/);
});

test('Daily automatic and saved resolution both enforce the active mapped role and band', () => {
  const catalogue = functionBody(coreSql, '_import_review_action_catalog_core_v1');
  const save = functionBody(dailySql, 'hr_daily_timesheet_resolution_save_v1');
  assert.match(catalogue, /lower\(btrim\(coalesce\(t\.tsfin_role,''\)\)\)=lower\(btrim\(coalesce\(dgm\.role_code,''\)\)\)/);
  assert.match(catalogue, /t\.tsfin_band/);
  assert.match(catalogue, /daily_mapping_updated_at/);
  assert.match(catalogue, /dcon\.contract_ids/);
  assert.match(catalogue, /ts\.contract_id=any\(coalesce\(dcon\.contract_ids,array\[\]::uuid\[\]\)\)/);
  assert.match(catalogue, /coalesce\(rtsx\.contract_id,dcon\.contract_id\)/);
  assert.match(save, /HR_DAILY_RESOLUTION_GRADE_MAPPING_STALE/);
  assert.match(save, /HR_DAILY_RESOLUTION_GRADE_ROLE_MISMATCH/);
  assert.match(save, /v_mapping\.role_code/);
  assert.match(save, /v_mapping\.band_norm/);
  assert.match(save, /mapping_evidence/);
  assert.match(save, /timesheet_evidence/);
  assert.match(save, /v_action\.contract_id is not null and v_contract_id is distinct from v_action\.contract_id/);
  assert.match(save, /HR_DAILY_RESOLUTION_CONTRACT_OUT_OF_SCOPE/);
});

test('one owner-only current-setting authority core governs catalogue and Weekly apply', () => {
  const authority = functionBody(coreSql, '_import_review_effective_authority_core_v1');
  const catalogue = functionBody(coreSql, '_import_review_action_catalog_core_v1');
  const apply = functionBody(weeklyApplySql, 'hr_weekly_apply_transactional');
  assert.match(authority, /effective_from<=x\.today_london/);
  assert.match(authority, /e\.autoprocess_hr and e\.no_timesheet_required/);
  assert.match(authority, /e\.autoprocess_hr and not e\.no_timesheet_required/);
  assert.match(authority, /when e\.route in \('HR_DAILY','HEALTHROSTER_DAILY'\) then e\.autoprocess_hr/);
  assert.match(coreSql, /revoke all on function public\._import_review_effective_authority_core_v1\(text,uuid,uuid,date\)[\s\S]*?service_role/);
  assert.match(catalogue, /cross join lateral public\._import_review_effective_authority_core_v1/);
  assert.match(apply, /case a\.authority_mode when 'AUTHORITATIVE' then 'MODE_B'/);
  assert.match(apply, /case aval\.authority_mode when 'VALIDATION_ONLY' then 'MODE_A'/);
  assert.doesNotMatch(apply, /effective_no_timesheet_required/);
});

test('overlap preflight is global for NHSP and client/route aware for HealthRoster', () => {
  const body = functionBody(coreSql, '_import_review_overlap_preflight_core_v2');
  assert.match(body, /hi\.source_system=p_source_system/);
  assert.match(body, /daterange\(hi\.coverage_start_date,hi\.coverage_end_date,'\[\]'\)/);
  assert.match(body, /current\.client_id is not null and other_client\.client_id=current\.client_id/);
  assert.match(body, /other_client\.source_client_key=current\.source_client_key/);
  assert.match(body, /'NHSP_PERIOD'/);
  assert.match(body, /p_source_system='NHSP'/);
  assert.match(body, /limit 20/);
});

test('review creation rejects overlaps and replacement is atomic', () => {
  const core = functionBody(lifecycleSql, '_import_review_create_core_v2');
  const create = functionBody(lifecycleSql, 'import_review_create_v1');
  const replace = functionBody(lifecycleSql, 'import_review_replace_v1');
  assert.match(core, /IMPORT_REVIEW_OVERLAP_CONFLICT/);
  assert.match(core, /IMPORT_REVIEW_REPLACE_TARGET_APPLYING/);
  assert.match(core, /status='SUPERSEDED'/);
  assert.match(core, /'atomic_replace',true/);
  assert.match(create, /_import_review_create_core_v2/);
  assert.match(replace, /_import_review_create_core_v2/);
  assert.match(retirementSql, /drop function if exists public\.import_review_supersede_v1/);
});

test('review get is authoritative for editability, commands, evidence and final confirmation', () => {
  const body = functionBody(lifecycleSql, 'import_review_get_v1');
  assert.match(body, /'editability'/);
  assert.match(body, /'allowed_commands'/);
  assert.match(body, /v_state\.status in \('APPLYING','APPLIED','ABANDONED','SUPERSEDED'\)/);
  assert.match(body, /'evidence'/);
  assert.match(body, /'confirmation_summary'/);
  assert.match(body, /selected_email_reminder_count/);
});

test('incremental apply is candidate/client scoped, immutable and transactionally bounded', () => {
  const envelope = functionBody(coreSql, '_import_review_apply_envelope_core_v1');
  const readyIds = functionBody(coreSql, '_import_review_ready_action_ids_core_v1');
  const guard = functionBody(lifecycleSql, 'import_review_apply_guard_v1');
  const complete = functionBody(lifecycleSql, '_import_review_apply_complete_core_v1');
  assert.match(incrementalMigration, /create table if not exists public\.import_review_action_outcomes/);
  assert.match(incrementalMigration, /enable row level security/);
  assert.match(incrementalMigration, /revoke all on table public\.import_review_action_outcomes from public,anon,authenticated,service_role/);
  assert.match(incrementalMigration, /trg_import_review_action_outcomes_immutable/);
  assert.match(readyIds, /selected_units/);
  assert.match(readyIds, /eligible_units/);
  assert.match(readyIds, /b\.candidate_id=u\.candidate_id/);
  assert.match(readyIds, /b\.client_id=u\.client_id/);
  assert.doesNotMatch(readyIds, /b\.candidate_id is null or/);
  assert.match(envelope, /_import_review_ready_action_ids_core_v1/);
  assert.match(envelope, /batch_scope_units/);
  assert.match(guard, /v_s\.status not in \('BLOCKED','READY'\)/);
  assert.match(guard, /d\.action_id=any\(v_ids\)/);
  assert.doesNotMatch(guard, /is_current and blocking\) then raise exception 'IMPORT_REVIEW_HAS_BLOCKERS'/);
  assert.match(complete, /insert into public\.import_review_action_outcomes/);
  assert.match(complete, /_import_review_refresh_core_v1/);
  assert.match(complete, /partial_application/);
  assert.match(complete, /remaining_blocker_count/);
});

test('all transactional apply routes restrict execution to the guarded batch action IDs', () => {
  const dailyApplySql = readFileSync(new URL('../../supabase/repeatable/21072026_1820_08_hr_daily_apply_transactional.sql', import.meta.url), 'utf8');
  const nhspApplySql = readFileSync(new URL('../../supabase/repeatable/21072026_1820_07_nhsp_weekly_apply_transactional.sql', import.meta.url), 'utf8');
  for (const source of [weeklyApplySql, dailyApplySql, nhspApplySql]) {
    assert.match(source, /jsonb_array_elements_text\(v_review_guard->'selected_action_ids'\)/);
    assert.doesNotMatch(source, /applied_at\s*=\s*v_now/);
  }
  assert.match(weeklyApplySql, /tmp_review_batch_units/);
  assert.match(weeklyApplySql, /p2m\.external_row_key = any\(coalesce\(v_mode_a_external_keys/);
  assert.match(weeklyApplySql, /bu\.candidate_id=nullif\(btrim\(r\.value->>'candidate_id'/);
});

test('failed-before-commit recovery proves no commit before reopening the review', () => {
  const body = functionBody(lifecycleSql, 'import_review_apply_failed_before_commit_recover_v1');
  assert.match(body, /o\.state<>'FAILED_BEFORE_COMMIT'/);
  assert.match(body, /o\.committed_at_utc is not null/);
  assert.match(body, /status=v_status/);
  assert.match(body, /'source_committed',false/);
  assert.match(lifecycleSql, /grant execute on function public\.import_review_apply_failed_before_commit_recover_v1/);
});

test('query email enqueue consolidates one outbox message per normalised recipient address', () => {
  const body = functionBody(emailSql, 'timesheet_query_email_enqueue_v1');
  assert.match(body, /group by lower\(route->>'recipient_email'\)/);
  assert.match(body, /RECIPIENT_EMAIL:/);
  assert.match(body, /business_route_count/);
  assert.match(body, /Items are grouped by client and contract/);
  assert.match(body, /TIMESHEET_QUERY_EMAIL/);
  assert.doesNotMatch(body, /group by route->>'recipient_scope',route->>'recipient_scope_key'/);
});

test('new public read RPCs are service-role-only', () => {
  for (const signature of [
    'import_review_staged_scope_get_v1\\(uuid,uuid,integer,integer\\)',
    'import_review_actions_page_v1\\(uuid,uuid,integer,integer,text,text,text\\)'
  ]) {
    assert.match(uiSql, new RegExp(`revoke all on function public\\.${signature} from public,anon,authenticated;`));
    assert.match(uiSql, new RegExp(`grant execute on function public\\.${signature} to service_role;`));
  }
});
