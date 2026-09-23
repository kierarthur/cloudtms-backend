import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const read = (path) => readFileSync(new URL(path, import.meta.url), 'utf8');
const contextSql = read('../../supabase/repeatable/15092026_1534_weekly_source_upload_context_v1.sql');
const verifierSql = read('../../supabase/verification/15092026_1534_weekly_source_upload_context_v1.sql');
const publicationVerifierSql = read('../../supabase/verification/15092026_1534_weekly_source_upload_publication_v1.sql');
const publicationSql = read('../../supabase/repeatable/15092026_1534_weekly_source_upload_publication_v1.sql');
const ownerSource = read('../../broker/src/weekly-source/upload-publication-owner.mjs');
const adapterSource = read('../../broker/src/weekly-source/upload-staging-adapter.mjs');
const modeADispatchSql = read('../../supabase/repeatable/17092026_0800_weekly_source_mode_a_dispatch_v1.sql');

test('upload context is service-only and refuses browser-owned economic or identity payloads', () => {
  assert.match(contextSql, /request\.jwt\.claim\.role/);
  assert.match(contextSql, /WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED/);
  assert.match(contextSql, /revoke all on function public\.weekly_source_upload_context_v1\(jsonb\)[\s\S]*from public,anon,authenticated/i);
  assert.match(contextSql, /grant execute on function public\.weekly_source_upload_context_v1\(jsonb\) to service_role/i);
  assert.match(contextSql, /v_allowed constant text\[\]:=array\[[\s\S]*'operation'[\s\S]*'upload_id'[\s\S]*\]/);
  for (const forbidden of ['rates_json', 'pay_amount', 'charge_amount', 'candidate_id', 'contract_id']) {
    assert.doesNotMatch(contextSql.match(/v_allowed constant[\s\S]*?\]::text\[\];/)[0], new RegExp(`'${forbidden}'`));
  }
  assert.match(contextSql, /candidate\.tms_ref/);
  assert.match(contextSql, /candidate\.nhsp_hr_name_aliases/);
  assert.match(contextSql, /public\.hr_name_mappings/);
  assert.doesNotMatch(contextSql, /similarity\s*\(|levenshtein\s*\(|soundex\s*\(/i);
  assert.match(verifierSql, /not pg_catalog\.has_function_privilege\('anon'/);
  assert.match(verifierSql, /browser rate facts were accepted/);
});

test('correction-upload verifier fixtures satisfy the complete correction-session identity', () => {
  const fixtureColumnLists = [...publicationVerifierSql.matchAll(
    /insert into public\.weekly_final_source_correction_sessions\(\s*([\s\S]*?)\s*\) values/gi,
  )];
  assert.equal(fixtureColumnLists.length, 3);
  for (const [, columns] of fixtureColumnLists) {
    for (const required of [
      'expected_current_final_revision_id',
      'expected_final_manifest_hash',
      'idempotency_key',
      'request_hash',
      'guard_fingerprint',
    ]) assert.match(columns, new RegExp(`\\b${required}\\b`));
  }
});

test('publication owner uses the canonical qualification, calculation and snapshot modules only', () => {
  assert.match(ownerSource, /qualifyWeeklySourceContract/);
  assert.match(ownerSource, /canonicalWeeklyShiftFinancialSegment/);
  assert.match(ownerSource, /compareWeeklySourceShiftPrice/);
  assert.match(ownerSource, /buildWeeklySourceCanonicalEconomicSnapshot/);
  assert.match(ownerSource, /weekly_source_upload_stage_begin_atomic_v1/);
  assert.match(ownerSource, /weekly_source_upload_stage_rows_atomic_v1/);
  assert.match(ownerSource, /weekly_source_upload_seal_atomic_v1/);
  assert.match(ownerSource, /weekly_source_projection_begin_atomic_v1/);
  assert.match(ownerSource, /weekly_source_projection_rows_apply_atomic_v1/);
  assert.match(ownerSource, /weekly_source_projection_publish_atomic_v1/);
  for (const forbidden of [
    'banking_pay', 'pay_workbench', 'pay_create_draft', 'invoice_generate',
    'daily_validation', 'hr_weekly_apply_transactional',
  ]) assert.doesNotMatch(ownerSource.toLowerCase(), new RegExp(forbidden));
});

test('staging adapter explicitly owns physical, money, expense and workbook provenance', () => {
  assert.match(adapterSource, /physicalRows/);
  assert.match(adapterSource, /moneyEvidence/);
  assert.match(adapterSource, /expenseEvidence/);
  assert.match(adapterSource, /workbook_part_and_sheet_fingerprint/);
  assert.match(adapterSource, /source_kind: parsed\.sourceKind/);
  assert.match(publicationSql, /v_summary->>'source_kind'/);
  assert.match(publicationSql, /not in \('XLSX','HTML'\)/);
  assert.match(publicationSql, /v_upload\.parser_summary_json->>'source_kind'='HTML'[\s\S]*money\.source_kind='HTML_DECODED_TEXT'/);
  assert.match(adapterSource, /header_coordinate_map_json/);
  assert.match(adapterSource, /nhsp_report_number/);
  assert.match(adapterSource, /saved_finalisation_profile_map/);
  assert.match(adapterSource, /BOTTOM_TOTAL_COST/);
  assert.match(adapterSource, /SOURCE_FIXED_EXPENSE_PENCE_V1/);
});

test('HealthRoster signed-Timesheet bridge supplies an incoming contract code when the parser has no band column', () => {
  assert.match(modeADispatchSql, /join public\.contracts contract on contract\.id=resolution\.contract_id/i);
  assert.match(modeADispatchSql, /coalesce\(nullif\(pg_catalog\.btrim\(source_row\.role_band_source\),''\),\s*nullif\(pg_catalog\.btrim\(contract\.band\),''\),nullif\(pg_catalog\.btrim\(contract\.role\),''\)\)/i);
});
