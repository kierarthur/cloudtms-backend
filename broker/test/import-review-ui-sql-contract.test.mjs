import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const uiSql = readFileSync(new URL('../../supabase/repeatable/22072026_0052_import_review_ui_contract_v1.sql', import.meta.url), 'utf8');
const emailSql = readFileSync(new URL('../../supabase/repeatable/21072026_1820_04_timesheet_query_email_rpcs.sql', import.meta.url), 'utf8');
const lifecycleSql = readFileSync(new URL('../../supabase/repeatable/21072026_1820_01_import_review_lifecycle_rpcs.sql', import.meta.url), 'utf8');
const coreSql = readFileSync(new URL('../../supabase/repeatable/21072026_1820_00_import_review_internal_core.sql', import.meta.url), 'utf8');
const dailySql = readFileSync(new URL('../../supabase/repeatable/21072026_1820_03_import_review_daily_resolution_and_previews.sql', import.meta.url), 'utf8');
const weeklyApplySql = readFileSync(new URL('../../supabase/repeatable/21072026_1820_06_hr_weekly_apply_transactional.sql', import.meta.url), 'utf8');
const nhspApplySql = readFileSync(new URL('../../supabase/repeatable/21072026_1820_07_nhsp_weekly_apply_transactional.sql', import.meta.url), 'utf8');
const dailyApplySql = readFileSync(new URL('../../supabase/repeatable/21072026_1820_08_hr_daily_apply_transactional.sql', import.meta.url), 'utf8');
const changedHoursSql = readFileSync(new URL('../../supabase/repeatable/21072026_1235_20_weekly_import_changed_hours_phase3.sql', import.meta.url), 'utf8');
const nhspPhase3Sql = readFileSync(new URL('../../supabase/repeatable/21072026_1235_26_nhsp_weekly_phase3_apply_adjustment_truth.sql', import.meta.url), 'utf8');
const weeklyPhase3Sql = readFileSync(new URL('../../supabase/repeatable/21072026_1235_24_hr_weekly_phase3_apply_adjustment_truth_3arg.sql', import.meta.url), 'utf8');
const correctionPolicySql = readFileSync(new URL('../../supabase/repeatable/21072026_1235_01_correction_financials_policy_resolve_v1.sql', import.meta.url), 'utf8');
const correctionGuardSql = readFileSync(new URL('../../supabase/repeatable/21072026_1235_00_import_correction_policy_helpers.sql', import.meta.url), 'utf8');
const correctionRuntimeGuardsSql = readFileSync(new URL('../../supabase/repeatable/21072026_1235_00b_import_correction_runtime_guards.sql', import.meta.url), 'utf8');
const candidateSourceBuildSql = readFileSync(new URL('../../supabase/repeatable/21072026_1235_39_pay_workbench_candidate_source_build_chunk.sql', import.meta.url), 'utf8');
const caseResolutionSql = readFileSync(new URL('../../supabase/repeatable/21072026_1235_41_pay_workbench_session_apply_case_resolution.sql', import.meta.url), 'utf8');
const clearCaseResolutionSql = readFileSync(new URL('../../supabase/repeatable/21072026_1235_42_pay_workbench_session_clear_case_resolution.sql', import.meta.url), 'utf8');
const correctionTransitionSql = readFileSync(new URL('../../supabase/repeatable/21072026_1235_08_timesheet_correction_pair_transition_v1.sql', import.meta.url), 'utf8');
const autoAuthorisePolicySql = readFileSync(new URL('../../supabase/repeatable/21072026_1235_02_import_auto_authorise_policy_resolve_v1.sql', import.meta.url), 'utf8');
const priorityEnqueueSql = readFileSync(new URL('../../supabase/repeatable/04012026_enqueue_ts_financials_priority.sql', import.meta.url), 'utf8');
const targetedDequeueSql = readFileSync(new URL('../../supabase/repeatable/05012026_tsfin_batch_rpcs_part1.sql', import.meta.url), 'utf8');
const weeklyPreviewSql = readFileSync(new URL('../../supabase/repeatable/21072026_1820_13_hr_weekly_validation_preview.sql', import.meta.url), 'utf8');
const retirementSql = readFileSync(new URL('../../supabase/repeatable/21072026_1820_99_import_review_hard_cutover_retirements.sql', import.meta.url), 'utf8');
const incrementalMigration = readFileSync(new URL('../../supabase/migrations/22072026_1700_import_review_incremental_outcomes.sql', import.meta.url), 'utf8');
const autoAuthoriseHierarchyMigration = readFileSync(new URL('../../supabase/migrations/22072026_2041_import_auto_authorise_hierarchy.sql', import.meta.url), 'utf8');

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
  assert.match(body, /review_ui_contract_version','IMPORT_REVIEW_UI_V6/);
  assert.match(body, /email_grouping_version','TIMESHEET_QUERY_RECIPIENT_EMAIL_V1/);
  assert.match(body, /legacy_contracts_supported',false/);
  assert.match(lifecycleSql, /review_ui_contract_version','IMPORT_REVIEW_UI_V6/);
});

test('staged scope discovery is actor-bound, source-owned and bounded', () => {
  const body = functionBody(uiSql, 'import_review_staged_scope_get_v1');
  assert.match(body, /_import_review_assert_actor_v1/);
  assert.match(body, /v_row_count>5000/);
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
  assert.match(body, /'confirmation_counts'/);
  for (const view of ['CONFIRM_STANDARD', 'CONFIRM_NON_STANDARD', 'CONFIRM_VALIDATION', 'CONFIRM_EMAIL', 'CONFIRM_REFERENCE']) {
    assert.match(body, new RegExp(view));
  }
  assert.match(body, /v_page>10000/);
  assert.match(body, /candidate_section_total_count/);
  assert.match(body, /client_section_total_count/);
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
  assert.match(body, /a\.summary_json->>'amendment_route'='AMEND_EXISTING_REPLACEMENT'/);
  assert.match(body, /TMS to amend replacement shift/);
  assert.match(body, /d\.summary_json->>'amendment_route' amendment_route/);
  assert.match(body, /amendment_route='AMEND_EXISTING_REPLACEMENT'/);
  assert.match(body, /amendment_route is distinct from 'AMEND_EXISTING_REPLACEMENT'/);
  assert.doesNotMatch(body, /_timesheet_query_recipient_resolve_core_v1/);
});

test('omitted HealthRoster breaks remain unknown while worked hours still validate', () => {
  const body = functionBody(coreSql, '_import_review_action_catalog_core_v1');
  const breakComparisons = [...body.matchAll(/and coalesce\(\(m\.payload_json->>'break_evidence_supplied'\)::boolean,false\)[\s\S]*?then 'BREAK_MINUTES'::text end/g)];
  assert.equal(breakComparisons.length, 2, 'Daily and Weekly comparisons must both ignore an omitted import break');
  assert.match(body, /coalesce\(\(r\.payload_json->>'break_evidence_supplied'\)::boolean,false\)[\s\S]*?then 'BREAK_MINUTES_MISMATCH'/);
  assert.match(body, /\(f\.payload_json->>'break_mins'\) is not null[\s\S]*?then 'APPLY_AMENDMENT'/);
  assert.match(body, /and abs\(\(m\.hours_worked\*60\)-greatest\(m\.worked_minutes-coalesce\(m\.ts_break_minutes,0\),0\)\)>1/);
  assert.match(body, /and abs\(\(m\.hours_worked\*60\)-m\.existing_shift_paid_minutes\)>1 then 'WORKED_HOURS'/);
  assert.match(body, /'elapsed_minutes',m\.worked_minutes[\s\S]*?'worked_minutes',greatest\(m\.worked_minutes-coalesce\(m\.ts_break_minutes,0\),0\)/);
  assert.match(body, /abs\(\(r\.hours_worked\*60\)-greatest\(r\.worked_minutes-coalesce\(r\.break_minutes,0\),0\)\)>1/);
});

test('Daily HealthRoster validation associates contract-free Daily timesheets', () => {
  const core = functionBody(coreSql, '_import_review_action_catalog_core_v1');
  const resolution = functionBody(dailySql, 'hr_daily_timesheet_resolution_save_v1');
  const apply = functionBody(dailyApplySql, 'hr_daily_apply_transactional');

  assert.doesNotMatch(core, /ts\.contract_id=any\(coalesce\(dcon\.contract_ids/);
  assert.match(core, /select distinct t\.tsfin_role role,t\.tsfin_band band[\s\S]*?from public\.v_timesheets_daily_match t/);
  assert.match(core, /when not f\.is_daily and coalesce\(f\.contract_count,0\)=0 then 'ADVISORY'/);
  assert.doesNotMatch(core, /when f\.is_daily and f\.contract_count>1/);
  assert.doesNotMatch(core, /when m\.is_daily and m\.contract_count>1 then 'CONTRACT_AMBIGUOUS'/);

  assert.match(resolution, /select \* into v_ts from public\.v_timesheets_daily_match/);
  assert.doesNotMatch(resolution, /HR_DAILY_RESOLUTION_CONTRACT_OUT_OF_SCOPE|HR_DAILY_RESOLUTION_CONTRACT_MISMATCH/);
  assert.doesNotMatch(resolution, /select \* into v_contract from public\.contracts/);

  assert.match(apply, /join public\.timesheets_financials scoped_financial[\s\S]*?scoped_financial\.candidate_id=scoped_candidate\.candidate_id/);
  assert.doesNotMatch(apply, /join public\.contracts scoped_contract/);
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
  assert.match(body, /match_status','MATCH'\) not in \('MATCH','HR_ONLY'\)/);
  assert.match(body, /validation-email-v2/);
  assert.match(body, /and not coalesce\(m\.contract_rate_complete,false\) then 'CONTRACT_RATES_INCOMPLETE'/);
  assert.match(body, /source_route_eligible',coalesce\(o\.route_eligible,false\)/);
  assert.match(body, /'selectable',coalesce\(o\.route_eligible,false\)/);
  assert.match(body, /CONTRACT_NOT_ELIGIBLE/);
  assert.match(body, /'evidence_rows'/);
  assert.match(body, /healthroster_start/);
  assert.match(body, /timesheet_start/);
});

test('HealthRoster query emails contain only shifts requiring Temporary Staffing attention', () => {
  const catalogue = functionBody(coreSql, '_import_review_action_catalog_core_v1');
  const enqueue = functionBody(emailSql, 'timesheet_query_email_enqueue_v1');

  assert.match(catalogue, /email_comparisons/);
  assert.match(catalogue, /coalesce\(cx\.value->>'match_status','MATCH'\) not in \('MATCH','HR_ONLY'\)/);
  assert.match(catalogue, /or coalesce\(\(cx\.value->>'ref_changed'\)::boolean,false\)/);
  assert.match(catalogue, /email_days/);
  assert.match(catalogue, /weekly-query-evidence-v2/);
  assert.match(catalogue, /'days',a\.email_days,'comparisons',a\.email_comparisons/);
  assert.match(
    catalogue,
    /validation-email-v2',\s*p\.timesheet_id,p\.row_json->>'week_ending_date',p\.email_comparisons::text/
  );

  // Daily validation already creates one email action only from a mismatch row.
  assert.match(catalogue, /from mismatch m where m\.reason_code is not null/);
  assert.match(
    catalogue,
    /'HEALTHROSTER_DAILY',m\.reason_code,m\.timesheet_id,m\.hr_request_id,[\s\S]*?m\.date_local,m\.start_time_local,m\.end_time_local,m\.hours_worked,m\.worked_minutes/
  );

  // The current evidence is part of each issue fingerprint. A changed shift
  // therefore becomes a new issue; it cannot inherit the old issue's reminder
  // history merely because the candidate, client or timesheet is unchanged.
  assert.match(catalogue, /left join public\.hr_issue_emails e on e\.issue_fingerprint=public\._import_review_hash_v1/);
  assert.match(catalogue, /from issues i left join public\.hr_issue_emails e on e\.issue_fingerprint=i\.issue_fingerprint/);

  // The final renderer independently fails closed if an older stored action
  // still contains matched comparison rows.
  assert.match(enqueue, /coalesce\(c\.value->>'match_status',''\)<>'MATCH'/);
  assert.match(enqueue, /coalesce\(c\.value->>'ref_before',''\)<>coalesce\(c\.value->>'ref_after',''\)/);
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
  assert.match(catalogue, /t\.candidate_id=m\.resolved_candidate_id and t\.client_id=m\.resolved_client_id/);
  assert.match(catalogue, /\(t\.worked_start_iso at time zone 'Europe\/London'\)::date=m\.date_local/);
  assert.doesNotMatch(catalogue, /ts\.contract_id=any\(coalesce\(dcon\.contract_ids,array\[\]::uuid\[\]\)\)/);
  assert.match(save, /HR_DAILY_RESOLUTION_GRADE_MAPPING_STALE/);
  assert.match(save, /HR_DAILY_RESOLUTION_GRADE_ROLE_MISMATCH/);
  assert.match(save, /v_mapping\.role_code/);
  assert.match(save, /v_mapping\.band_norm/);
  assert.match(save, /mapping_evidence/);
  assert.match(save, /timesheet_evidence/);
  assert.match(save, /v_ts\.candidate_id is distinct from v_action\.candidate_id/);
  assert.match(save, /v_ts\.client_id is distinct from v_action\.client_id/);
  assert.doesNotMatch(save, /HR_DAILY_RESOLUTION_CONTRACT_OUT_OF_SCOPE/);
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

test('every generated invoice line is protected correction evidence in preview and apply', () => {
  const protection = functionBody(coreSql, '_import_review_timesheet_protection_core_v1');
  const envelope = functionBody(coreSql, '_import_review_apply_envelope_core_v1');
  assert.match(protection, /coalesce\(tf\.locked_by_invoice_id is not null,false\)/);
  assert.match(protection, /from public\.invoice_lines il\s+where il\.timesheet_id=p_timesheet_id/);
  assert.doesNotMatch(protection, /join public\.invoices invoice_row/);
  assert.doesNotMatch(protection, /invoice_row\.issued_at_utc is not null/);
  assert.doesNotMatch(protection, /invoice_row\.paid_at_utc is not null/);
  assert.doesNotMatch(protection, /status::text[\s\S]*?\('ISSUED','PAID'\)/);
  assert.match(envelope, /pr\.protection->>'invoice_locked'/);
  assert.match(envelope, /'REVERSAL_REPLACEMENT'/);
  assert.match(changedHoursSql, /left join lateral \(\s+select il\.invoice_id\s+from public\.invoice_lines il/);
  assert.match(changedHoursSql, /or a\.invoice_line_invoice_id is not null\s+\)\s+as is_invoiced/);
  assert.doesNotMatch(changedHoursSql, /upper\(coalesce\(i\.status::text,''\)\) in \('ISSUED','PAID'\)/);
  assert.match(nhspApplySql, /weekly_import_changed_hours_phase3\(p_import_id := p_import_id, p_system_type := 'NHSP'\)/);
  assert.match(weeklyApplySql, /weekly_import_changed_hours_phase3\(p_import_id := p_import_id, p_system_type := 'HEALTHROSTER'\)/);
});

test('settled Banking Pay artifacts force the protected weekly correction pathway', () => {
  const protection = functionBody(coreSql, '_import_review_timesheet_protection_core_v1');
  assert.match(protection, /from public\.pay_batch_items settled_item/);
  assert.match(protection, /join public\.pay_batch_candidates settled_candidate/);
  assert.match(protection, /settled_candidate\.settlement_status[\s\S]*?'SETTLED'/);
  assert.match(
    protection,
    /with recursive correction_ancestry as[\s\S]*?into v_correction_family_ids,v_correction_root_id/,
  );
  assert.match(protection, /not parent_timesheet\.timesheet_id=any\(correction_ancestry\.visited_ids\)/);
  assert.match(protection, /settled_item\.timesheet_id=any\(v_correction_family_ids\)/);
  assert.match(
    protection,
    /settled_item\.frozen_component_snapshot_json->>'correction_root_id'=any\(v_correction_family_ids::text\[\]\)/,
  );
  assert.match(
    protection,
    /settled_item\.frozen_resolution_payload_json->>'correction_root_id'=any\(v_correction_family_ids::text\[\]\)/,
  );
  assert.match(changedHoursSql, /as has_paid_evidence/);
  assert.match(changedHoursSql, /a\.has_paid_evidence as is_paid/);
  assert.match(
    weeklyApplySql,
    /where cs\.is_invoiced is true\s+or cs\.is_paid is true/,
  );
  assert.match(
    weeklyApplySql,
    /where cs\.is_invoiced is false\s+and cs\.is_paid is false/,
  );
  assert.match(
    weeklyPhase3Sql,
    /_import_review_timesheet_protection_core_v1\(v_existing_pos_ts_id\)[\s\S]*?->>'paid'/,
  );
  assert.match(
    weeklyPhase3Sql,
    /_import_review_timesheet_protection_core_v1\(v_existing_neg_ts_id\)[\s\S]*?->>'paid'/,
  );
});

test('TSFIN correction-unit payload extraction accepts JSON whitespace without double escaping', () => {
  const extractor = functionBody(correctionGuardSql, '_ctms_payload_timesheet_ids_v1');
  assert.match(extractor, /"\[\[:space:\]\]\*:\[\[:space:\]\]\*"/);
  assert.doesNotMatch(extractor, /"\\\\s\*:\\\\s\*"/);
});

test('correction lifecycle expansion preserves the atomic expected-timesheet fence for every member', () => {
  const transition = functionBody(correctionTransitionSql, 'timesheet_correction_pair_transition_v1');
  const expander = functionBody(correctionGuardSql, '_ctms_expand_lifecycle_items_v1');
  assert.match(transition, /'timesheet_id',r\.timesheet_id,'expected_timesheet_id',r\.timesheet_id/);
  assert.match(expander, /v_transition -> 'transition_items'/);
});

test('protected amendments prove the new immutable import row before either Weekly route commits', () => {
  const policy = functionBody(correctionPolicySql, '_ctms_correction_financials_policy_build_v2');
  const resolver = functionBody(correctionPolicySql, 'correction_financials_policy_resolve_v1');
  assert.match(policy, /source_row\.import_id\s*=\s*p_import_id/);
  assert.match(policy, /source_row\.external_row_key\s*=\s*v_source_row_key/);
  assert.doesNotMatch(policy, /v_shift\.latest_import_id\s+is\s+distinct\s+from\s+p_import_id/);
  assert.match(resolver, /source_row\.import_id\s*=\s*v_operation\.import_id/);
  assert.match(resolver, /source_row\.external_row_key\s*=\s*p_source_row_key/);
  assert.doesNotMatch(resolver, /v_source_shift\.latest_import_id\s+is\s+distinct\s+from\s*v_operation\.import_id/);
  assert.match(nhspApplySql, /import_review_apply_guard_v1/);
  assert.match(weeklyApplySql, /import_review_apply_guard_v1/);
  assert.match(policy, /_import_review_effective_authority_core_v1\(/);
  assert.match(policy, /authority\.import_authoritative/);
  assert.doesNotMatch(
    policy,
    /coalesce\(v_settings\.requires_hr,\s*false\)[\s\S]*?coalesce\(v_settings\.no_timesheet_required,\s*false\)/,
  );
});

test('one complete contract-client-global hierarchy governs HealthRoster and NHSP auto-authorisation', () => {
  const resolver = functionBody(autoAuthorisePolicySql, 'import_auto_authorise_policy_resolve_v1');
  assert.match(autoAuthoriseHierarchyMigration, /add column if not exists healthroster_import_auto_authorise_default/);
  assert.match(autoAuthoriseHierarchyMigration, /add column if not exists nhsp_import_auto_authorise_default/);
  assert.match(autoAuthoriseHierarchyMigration, /add column if not exists healthroster_import_auto_authorise_override/);
  assert.match(autoAuthoriseHierarchyMigration, /add column if not exists nhsp_import_auto_authorise_override/);
  assert.match(resolver, /IF v_contract_override_value IS NOT NULL THEN/);
  assert.match(resolver, /ELSIF v_client_setting_found THEN/);
  assert.match(resolver, /GLOBAL_FALLBACK_CLIENT_SETTING_MISSING/);
  assert.match(resolver, /v_effective_value := v_effective_import_value/);
  assert.doesNotMatch(resolver, /v_effective_value := v_global\.auto_authorise_on_validation/);
});

test('configured auto-authorisation keeps mandatory corrections separate and admits only whole-timesheet validation matches', () => {
  const helper = functionBody(coreSql, '_import_review_auto_authorise_targets_core_v1');
  assert.match(helper, /import_auto_authorise_policy_resolve_v1/);
  assert.match(helper, /_import_review_timesheet_protection_core_v1/);
  assert.match(helper, /invoice_locked/);
  assert.match(helper, /active_pay_draft/);
  assert.match(helper, /is_adjustment,false[\s\S]*?correction_id is not null/);
  assert.match(nhspApplySql, /_import_review_auto_authorise_targets_core_v1\([\s\S]*?'NHSP'::public\.hr_source_enum,false/);
  assert.match(nhspApplySql, /'auto_authorise_timesheet_ids'/);
  assert.match(nhspApplySql, /correction\.correction_id is not null[\s\S]*?v_auto_authorise_timesheet_ids/);
  assert.match(weeklyApplySql, /v_authoritative_affected_timesheet_ids/);
  assert.match(weeklyApplySql, /_import_review_auto_authorise_targets_core_v1\([\s\S]*?'HEALTHROSTER'::public\.hr_source_enum,false/);
  assert.match(weeklyApplySql, /v_validation_auto_authorise_timesheet_ids/);
  assert.match(weeklyApplySql, /hi\.coverage_mode in \('COMPLETE_ALL','COMPLETE_SELECTED_CANDIDATES'\)/);
  assert.match(weeklyApplySql, /whole_timesheet\.segment_count=jsonb_array_length\(vr\.row_json->'comparisons'\)/);
  assert.match(weeklyApplySql, /time_match','false'/);
  assert.match(weeklyApplySql, /matched_shift\.ref_num=comparison\.value->>'ref_after'/);
  assert.match(weeklyApplySql, /'HEALTHROSTER'::public\.hr_source_enum,true/);
  assert.doesNotMatch(weeklyApplySql, /hi\.coverage_mode='PARTIAL'[\s\S]*?v_validation_auto_authorise_timesheet_ids/);
  assert.match(weeklyApplySql, /correction\.correction_id is not null[\s\S]*?v_auto_authorise_timesheet_ids/);
  assert.match(weeklyApplySql, /'auto_authorise_timesheet_ids'/);

  assert.match(dailyApplySql, /v_validation_eligible_timesheet_ids/);
  assert.match(dailyApplySql, /hi\.coverage_mode in \('COMPLETE_ALL','COMPLETE_SELECTED_CANDIDATES'\)/);
  assert.match(dailyApplySql, /whole_timesheet\.segment_count=\(\s*select count\(\*\)::integer\s*from tmp_val_raw/);
  assert.match(dailyApplySql, /not exists \([\s\S]*?tmp_val_raw raw_row[\s\S]*?VALIDATION_OK/);
  assert.match(dailyApplySql, /t\.reference_number=u\.new_hr_request_id/);
  assert.match(dailyApplySql, /'HEALTHROSTER_DAILY'::public\.hr_source_enum,true/);
});

test('weekly protected amendments refresh only the correction pair and keep the settled root frozen', () => {
  assert.match(
    weeklyApplySql,
    /ns\.external_row_key=any\(\s*coalesce\(v_force_keys_final,array\[\]::text\[\]\)\s*\)[\s\S]*?and not \(\s*ns\.external_row_key=any\(\s*coalesce\(v_invoiced_changed_keys,array\[\]::text\[\]\)\s*\)\s*\)/,
  );
  assert.match(
    weeklyApplySql,
    /jsonb_array_elements_text\(coalesce\(v_phase3_result->'created_timesheet_ids','\[\]'::jsonb\)\)/,
  );
  assert.match(
    weeklyApplySql,
    /jsonb_array_elements_text\(coalesce\(v_phase3_result->'updated_timesheet_ids','\[\]'::jsonb\)\)/,
  );
  assert.match(
    weeklyApplySql,
    /into v_protected_source_timesheet_ids[\s\S]*?cs\.is_invoiced is true or cs\.is_paid is true/,
  );
  assert.match(
    weeklyApplySql,
    /v_affected_timesheet_ids[\s\S]*?not \(\s*a\.timesheet_id=any\(\s*coalesce\(v_protected_source_timesheet_ids,array\[\]::uuid\[\]\)/,
  );
});

test('correction Suggested Rate uses one changed pay bucket and leaves mixed buckets to custom resolution', () => {
  const materialiser = functionBody(
    correctionRuntimeGuardsSql,
    '_ctms_materialise_candidate_correction_residuals_v1',
  );
  assert.match(materialiser, /with signed_source_buckets as/);
  assert.match(materialiser, /signed_bucket_source_pay/);
  assert.match(materialiser, /delta_component\.value->>'bucket_code'/);
  assert.match(materialiser, /sign\(suggestion_candidate\.signed_bucket_source_pay\)\s*=sign\(v_component_source_outstanding\)/);
  assert.match(materialiser, /count\(distinct eligible\.bucket_code\)::integer/);
  assert.match(materialiser, /v_suggestion_matching_bucket_count<>1/);
  assert.doesNotMatch(materialiser, /CORRECTION_CHAIN_SUGGESTED_RESOLUTION_BASIS_AMBIGUOUS/);
  assert.match(
    materialiser,
    /do not expose its target suggestion,[\s\S]*guess, average, or let an optional suggestion abort the whole/,
  );
  assert.match(
    materialiser,
    /v_suggestion_matching_bucket_count<>1[\s\S]*v_suggestion_rebased_units:=case[\s\S]*v_suggested_component:=v_suggested_component/,
  );
  assert.match(
    materialiser,
    /'resolution_action_label',case[\s\S]*when v_has_suggested_resolution then 'Suggested Rate'[\s\S]*else 'Custom Rate'/,
  );
  assert.match(
    materialiser,
    /'has_suggested_resolution',v_has_suggested_resolution/,
  );
  assert.match(
    materialiser,
    /v_suggestion_target_inc:=round\(\s*coalesce\(v_suggestion_target_ex,0\)\s*\+\s*coalesce\(v_suggestion_target_vat,0\)/,
  );

  const payloadEnricher = functionBody(
    correctionRuntimeGuardsSql,
    '_ctms_enrich_correction_resolution_payload_v1',
  );
  const residualLookup = payloadEnricher.indexOf(
    'v_residuals:=public._ctms_candidate_correction_residuals_v1',
  );
  const correctionClassification = payloadEnricher.indexOf(
    'public._ctms_import_correction_classify_v1(v_timesheet)',
  );
  assert.ok(residualLookup >= 0);
  assert.ok(correctionClassification > residualLookup);
  assert.match(payloadEnricher, /if v_residual is null[\s\S]*return v_payload;/i);
  assert.match(
    payloadEnricher,
    /'correction_root_id',v_residual->>'root_timesheet_id'/,
  );
});

test('a paged source build retries its first page when the continuation attestation cannot be persisted', () => {
  const sourceBuild = functionBody(
    candidateSourceBuildSql,
    'pay_workbench_candidate_source_build_chunk',
  );
  assert.match(
    sourceBuild,
    /v_first_source_page[\s\S]*v_has_more[\s\S]*v_sync_completed[\s\S]*v_session_progress_update_applied/,
  );
  assert.match(
    sourceBuild,
    /PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_ATTESTATION_PERSIST_DEFERRED/,
  );
  assert.match(sourceBuild, /'retry_safe', true/);
  assert.match(
    sourceBuild,
    /RETURN public\.pay_workbench_candidate_source_build_chunk\(\s*p_session_id,\s*p_candidate_id,\s*'\{\}'::jsonb/,
  );
  assert.match(sourceBuild, /'continuation_attestation_restart', true/);
  assert.doesNotMatch(
    sourceBuild,
    /RAISE EXCEPTION 'PAY_WORKBENCH_CANDIDATE_SOURCE_BUILD_CONTINUATION_ATTESTATION_STALE'/,
  );
});

test('read-only correction discovery binds its database freshness check without weakening Apply', () => {
  const body = functionBody(
    caseResolutionSql,
    'pay_workbench_session_apply_case_resolution',
  );
  assert.match(
    body,
    /IF v_operation = 'DISCOVER' THEN\s*v_expected_progress_counter_version :=\s*COALESCE\(v_session_row\.progress_counter_version, 0\);/,
  );
  assert.match(
    body,
    /IF COALESCE\(v_session_row\.progress_counter_version, 0\) IS DISTINCT FROM v_expected_progress_counter_version THEN\s*RAISE EXCEPTION 'WORKBENCH_SESSION_PROGRESS_CHANGED'/,
  );
  assert.match(
    body,
    /APPLY retains the caller's reviewed progress fence unchanged/,
  );
});

test('correction resolutions keep the canonical carrier identity through target materialisation', () => {
  const body = functionBody(
    caseResolutionSql,
    'pay_workbench_session_apply_case_resolution',
  );
  assert.match(
    body,
    /SELECT\s+input_bucket\.resolution_identity_key,\s+component_evidence\.candidate_id/i,
  );
  assert.match(
    body,
    /WHEN component_evidence\.source_family_key LIKE 'correction-chain:%'\s+THEN public\._ctms_correction_carrier_identity_v1\(/i,
  );
  assert.match(
    body,
    /component_evidence\.component_state_json->>'correction_root_id'/,
  );
});

test('case-resolution clear does not apply the resolution-only correction enricher', () => {
  const body = functionBody(
    clearCaseResolutionSql,
    'pay_workbench_session_clear_case_resolution',
  );
  assert.doesNotMatch(
    body,
    /_ctms_enrich_correction_resolution_payload_v1/,
  );
});

test('import-authoritative NHSP and HealthRoster block expense-occupied timesheets before mutation', () => {
  const expenseGuard = functionBody(coreSql, '_import_review_timesheet_has_calculated_expenses_core_v1');
  for (const field of [
    'expenses_pay_ex_vat', 'expenses_charge_ex_vat',
    'mileage_pay_ex_vat', 'mileage_charge_ex_vat',
    'travel_pay_ex_vat', 'travel_charge_ex_vat',
    'accommodation_pay_ex_vat', 'accommodation_charge_ex_vat',
    'other_pay_ex_vat', 'other_charge_ex_vat'
  ]) assert.match(expenseGuard, new RegExp(`tf\\.${field}`));
  assert.match(coreSql, /revoke all on function public\._import_review_timesheet_has_calculated_expenses_core_v1\(uuid\) from public,anon,authenticated,service_role/);
  assert.match(functionBody(coreSql, '_import_review_auto_authorise_targets_core_v1'), /_import_review_timesheet_has_calculated_expenses_core_v1/);
  const catalog = functionBody(coreSql, '_import_review_action_catalog_core_v1');
  assert.match(catalog, /authoritative_target_timesheet_id/);
  assert.match(catalog, /TIMESHEET_OCCUPIED_BY_EXPENSES/);
  assert.match(catalog, /Timesheet occupied by expenses/);
  assert.match(catalog, /occupied_timesheet_id/);
  assert.match(uiSql, /when 'TIMESHEET_OCCUPIED_BY_EXPENSES' then 'Timesheet occupied by expenses'/);

  for (const source of [nhspApplySql, weeklyApplySql]) {
    assert.match(source, /IMPORT_AUTHORITATIVE_EXPENSE_SEPARATION_REQUIRED/);
    assert.match(source, /Timesheet occupied by expenses/);
    assert.match(source, /no import mutation was applied/);
    assert.match(source, /coalesce\(cw\.additional_seq,0\)=0/);
    assert.match(source, /existing_import_shift\.external_row_key=p2\.external_row_key/);
    assert.doesNotMatch(source, /expense_separated_container_count/);
    assert.doesNotMatch(source, /expense_separated_from_timesheet_id/);
  }
  assert.match(nhspApplySql, /source_system='NHSP'::public\.hr_source_enum/);
  assert.match(weeklyApplySql, /source_system='HEALTHROSTER'::public\.hr_source_enum/);
  assert.match(weeklyApplySql, /p2\.mode='MODE_B'/);
  assert.doesNotMatch(dailyApplySql, /expense_separated_container_count/);
  assert.doesNotMatch(dailyApplySql, /IMPORT_AUTHORITATIVE_EXPENSE_SEPARATION_REQUIRED/);
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
  assert.match(body, /'last_operation_request_hash'/);
  assert.match(body, /o\.id=v_state\.last_operation_id and o\.import_id=p_import_id/);
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
  for (const source of [weeklyApplySql, dailyApplySql, nhspApplySql]) {
    assert.match(source, /jsonb_array_elements_text\(v_review_guard->'selected_action_ids'\)/);
    assert.doesNotMatch(source, /applied_at\s*=\s*v_now/);
  }
  assert.match(weeklyApplySql, /tmp_review_batch_units/);
  assert.match(weeklyApplySql, /p2m\.external_row_key = any\(coalesce\(v_mode_a_external_keys/);
  assert.match(weeklyApplySql, /bu\.candidate_id=nullif\(btrim\(r\.value->>'candidate_id'/);
});

test('authoritative Weekly apply preserves authorised lifecycle through TSFIN follow-up', () => {
  for (const source of [weeklyApplySql, nhspApplySql]) {
    assert.match(source, /v_reauthorise_timesheet_ids uuid\[\]/);
    assert.match(source, /timesheet_unauthorise_bulk_atomic\(v_lifecycle_items,p_actor_user_id,v_now\)/);
    assert.match(source, /post_commit_reauthorise_timesheet_ids/);
    assert.match(source, /CANONICAL_UNAUTHORISE_COMPLETE/);
  }
  assert.doesNotMatch(nhspApplySql, /if exists \([\s\S]{0,500}tmp_changed_sel[\s\S]{0,500}message='CANONICAL_UNAUTHORISE_REQUIRED'/);
  assert.doesNotMatch(weeklyApplySql, /if exists \([\s\S]{0,650}tmp_changed_sel[\s\S]{0,650}message = 'CANONICAL_UNAUTHORISE_REQUIRED'/);
  assert.match(nhspApplySql, /Phase 3 deliberately amends that pair's replacement[\s\S]*?correction_kind='CHANGED_HOURS_REPLACEMENT'/);
  assert.match(nhspApplySql, /actual_schedule_json @> jsonb_build_array\(jsonb_build_object\([\s\S]*?'shift_id',source_shift\.id::text,[\s\S]*?'external_row_key',cs\.external_row_key/);
  assert.match(nhspPhase3Sql, /amendment within the existing correction unit, not a new/);
  assert.match(nhspPhase3Sql, /v_existing_pos_hint#>>'\{correction_financials_policy_envelope,operation,operation_id\}'/);
  assert.match(nhspPhase3Sql, /EXISTING_CORRECTION_POLICY_ENVELOPE_INVALID/);
  assert.match(nhspPhase3Sql, /v_hint := v_existing_pos_hint \|\| jsonb_build_object/);
  assert.doesNotMatch(nhspPhase3Sql, /v_existing_pos_hint[\s\S]{0,800}update public\.timesheets[\s\S]{0,800}correction_financials_policy_envelope', v_correction_financials_policy_envelope/);
  assert.match(nhspApplySql, /existing_replacement\.timesheet_id[\s\S]*?_import_review_timesheet_protection_core_v1\(existing_replacement\.timesheet_id\)[\s\S]*?'invoice_locked'/);
  assert.doesNotMatch(weeklyApplySql, /Phase 3 deliberately amends that pair's replacement/);
  assert.match(nhspApplySql, /Persist and enqueue complete correction units/);
  assert.match(weeklyApplySql, /complete TSFIN\/lifecycle unit/);
  assert.match(priorityEnqueueSql, /TSFIN_PRIORITY_ENQUEUE_TARGET_LIMIT_EXCEEDED/);
  assert.match(priorityEnqueueSql, /partner\.correction_id = seed\.correction_id/);
  assert.match(priorityEnqueueSql, /HEALTHROSTER_CHANGED_HOURS/);
  assert.match(priorityEnqueueSql, /NHSP_CHANGED_HOURS/);
  const targetedDequeue = functionBody(targetedDequeueSql, 'tsfin_dequeue_specific');
  assert.match(targetedDequeue, /TSFIN_SPECIFIC_DEQUEUE_TARGET_LIMIT_EXCEEDED/);
  assert.match(targetedDequeue, /partner\.correction_id=seed\.correction_id/);
  assert.match(targetedDequeue, /limit 100/);
});

test('mutable NHSP and HealthRoster correction replays preserve one shared pair parent', () => {
  for (const source of [nhspPhase3Sql, weeklyPhase3Sql]) {
    const body = functionBody(source, source === nhspPhase3Sql
      ? 'nhsp_weekly_phase3_apply_adjustment_truth'
      : 'hr_weekly_phase3_apply_adjustment_truth');
    const lifecycleGuard = body.indexOf("message='CORRECTION_PAIR_LIFECYCLE_TRANSITION_REQUIRED'");
    const repair = body.indexOf('update public.timesheets pair_replacement');
    const replacementUpdate = body.indexOf('update public.timesheets tup', repair);
    const replacementUpdateEnd = body.indexOf('where tup.timesheet_id = v_existing_pos_ts_id', replacementUpdate);

    assert.notEqual(lifecycleGuard, -1);
    assert.notEqual(repair, -1);
    assert.ok(lifecycleGuard < repair, 'frozen/lifecycle evidence must be rejected before repair');
    assert.match(body, /v_existing_pair_parent_timesheet_id uuid := null/);
    assert.match(body, /pair_reversal\.correction_kind='CHANGED_HOURS_REVERSAL'/);
    assert.match(body, /pair_replacement\.parent_timesheet_id is distinct from v_existing_pair_parent_timesheet_id/);
    assert.match(body, /count\(distinct pair_check\.parent_timesheet_id\) = 1/);
    assert.match(body, /count\(pair_check\.parent_timesheet_id\) = 2/);
    assert.match(body, /CORRECTION_PAIR_PARENT_MISSING/);
    assert.doesNotMatch(
      body.slice(replacementUpdate, replacementUpdateEnd),
      /parent_timesheet_id\s*=\s*v_base_timesheet_id/
    );
  }
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
  assert.match(body, /v_operation public\.import_apply_operations%rowtype/);
  assert.match(body, /v_state\.status not in \('IN_REVIEW','BLOCKED','READY','APPLIED'\)/);
  assert.match(body, /v_operation\.committed_at_utc is null/);
  assert.match(body, /v_operation\.response_json->'post_commit_email_action_ids'/);
  assert.match(body, /public\.import_review_action_outcomes o/);
  assert.doesNotMatch(body, /v_state\.status<>'APPLIED'/);
  assert.match(body, /group by lower\(route->>'recipient_email'\)/);
  assert.match(body, /RECIPIENT_EMAIL:/);
  assert.match(body, /business_route_count/);
  assert.match(body, /Items are grouped by client and contract/);
  assert.match(body, /TIMESHEET_QUERY_EMAIL/);
  assert.doesNotMatch(body, /group by route->>'recipient_scope',route->>'recipient_scope_key'/);
  assert.match(body, /p_max_actions integer default 5000/);
});

test('large reviews remain hard bounded while confirmation details are server paged', () => {
  const catalogue = functionBody(coreSql, '_import_review_action_catalog_core_v1');
  const refresh = functionBody(coreSql, '_import_review_refresh_core_v1');
  const guard = functionBody(lifecycleSql, 'import_review_apply_guard_v1');
  assert.match(catalogue, /p_max_actions integer default 5000/);
  assert.match(catalogue, /p_max_actions>5000/);
  assert.match(refresh, /p_max_actions integer default 5000/);
  assert.match(guard, /p_selected_action_ids,'\[\]'.*?>5000/);
  assert.match(guard, />2097152/);
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
