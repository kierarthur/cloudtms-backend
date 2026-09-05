const test = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const repeatableDir = path.join(root, 'supabase', 'repeatable');
const monolithName = '26052026_2100HRS_NEW_FUNCTIONS.sql';
const reassertName = '08082026_0902_reassert_authorities_after_legacy_monolith.sql';
const currentClosureName = '29082026_0326_banking_pay_release_authority_repair_v1.sql';
const finalAuthorityClosureName = '05092026_1200_banking_pay_draft_v8_final_authority_closure.sql';
const normalizeLf = (value) => String(value || '').replace(/\r\n/g, '\n');
const monolith = normalizeLf(fs.readFileSync(path.join(repeatableDir, monolithName), 'utf8'));
const reassert = normalizeLf(fs.readFileSync(path.join(repeatableDir, reassertName), 'utf8'));
const currentClosure = normalizeLf(fs.readFileSync(path.join(repeatableDir, currentClosureName), 'utf8'));
const finalAuthorityClosure = normalizeLf(fs.readFileSync(path.join(repeatableDir, finalAuthorityClosureName), 'utf8'));
const targetedManifest = JSON.parse(fs.readFileSync(
  path.join(root, 'supabase', 'verification', 'banking_pay_targeted_fast_route_certified_reuse_catalog_manifest.json'),
  'utf8',
));
const criticalAuthorityFiles = [
  '07082026_1015_pay_sync_overpayments_from_workbench_workspace_v1.sql',
];
const finalAuthorityFiles = [
  '19072026_1816_cancel_refresh_supersede_finance_dirty.sql',
];
const incrementalReplayDependencies = [
  '21072026_1235_00b_import_correction_runtime_guards.sql',
];
const repairTargets = [
  ['bulk_authorise_dataset_v1', '14082026_1310_timesheet_processing_status_and_authorise_authority_v1.sql'],
  ['bulk_process_dataset_v1', '07082026_2224_candidate_app_weekly_office_replacements_v1.sql'],
  ['bulk_timesheet_row_patch_v1', '07082026_2224_candidate_app_weekly_office_replacements_v1.sql'],
  ['contract_week_manual_upsert_atomic', '27082026_2205_candidate_weekly_manager_finalisation_authority_v1.sql'],
  ['pay_workbench_mark_finance_case_dirty', '04082026_1219_pay_workbench_mark_finance_case_dirty.sql'],
  ['pay_workbench_enqueue_candidate_refresh', '07082026_1017_pay_workbench_enqueue_candidate_refresh.sql'],
  ['timesheet_daily_manual_process_atomic', '07082026_2224_candidate_app_weekly_office_replacements_v1.sql'],
];
const bankingV2PublicServiceIdentities = [
  'public.pay_timesheet_summary_pay_state_refresh_trigger()',
  'public.pay_workbench_contract_client_dirty_fanout_chunk(uuid,jsonb,integer)',
  'public.pay_workbench_enqueue_candidate_refresh(uuid,uuid,text,uuid,jsonb)',
  'public.pay_workbench_enqueue_stage_continuation(uuid,uuid,text,jsonb,uuid,jsonb,uuid,text,integer,integer)',
  'public.pay_workbench_revalidate_zero_retained_recovery_headroom_v1(uuid,uuid,jsonb)',
  'public.pay_workbench_session_get_action_required_detail_v1(uuid,jsonb,uuid,text,text,integer)',
  'public.pay_workbench_session_get_action_required_page_v1(uuid,jsonb,uuid,text,text,text,integer,text,text)',
  'public.pay_workbench_session_get_blocked_detail_v1(uuid,jsonb,uuid,text,text,integer)',
  'public.pay_workbench_session_get_blocked_page_v1(uuid,jsonb,uuid,text,text,text,integer,text)',
  'public.pay_workbench_session_get_candidate_ready_page_v1(uuid,uuid,jsonb,uuid,text,integer)',
  'public.pay_workbench_session_get_candidate_summary_page_v1(uuid,jsonb,uuid,text,text,text,integer)',
  'public.pay_workbench_session_get_selected_ready_timesheets_v1(uuid,uuid,jsonb,uuid,text)',
  'public.pay_workbench_session_set_candidate_ready_selection_v1(uuid,uuid,jsonb,uuid,text,uuid,text,jsonb)',
  'public.pay_workbench_session_set_filtered_ready_selection_v1(uuid,jsonb,uuid,text,uuid,text)',
  'public.pay_workbench_session_set_ready_group_v1(uuid,uuid,jsonb,uuid,text,text,boolean,uuid,text,jsonb)',
  'public.pay_workbench_session_set_ready_rows_v1(uuid,uuid,jsonb,uuid,jsonb,boolean,uuid,text,jsonb)',
];
const definition = (source, name) => {
  const token = `CREATE OR REPLACE FUNCTION public.${name}(`;
  const start = source.indexOf(token);
  assert.ok(start >= 0, `missing ${name}`);
  assert.equal(source.indexOf(token, start + token.length), -1, `ambiguous ${name}`);
  const terminator = '\n$function$;';
  const end = source.indexOf(terminator, start);
  assert.ok(end > start, `incomplete ${name}`);
  return source.slice(start, end + terminator.length).trim();
};

test('immutable legacy replay is followed by the provider-neutral repair and final current reassertion', () => {
  const monolithHash = crypto.createHash('sha256').update(monolith).digest('hex');
  assert.match(reassert, new RegExp(`legacy_monolith_sha256:\\s*\\n-- ${monolithHash}`));
  const historicalIncludedFiles = [...reassert.matchAll(/^\\ir\s+([^\s]+\.sql)\s*$/gmi)]
    .map((match) => match[1]);
  const currentIncludedFiles = [...currentClosure.matchAll(/^\\ir\s+([^\s]+\.sql)\s*$/gmi)]
    .map((match) => match[1]);
  assert.equal(new Set(historicalIncludedFiles).size, historicalIncludedFiles.length);
  assert.deepEqual(currentIncludedFiles, [], 'the current repair must not replay a broad authority file');
  assert.doesNotMatch(reassert, /\\ir\s+26052026_2100HRS_NEW_FUNCTIONS\.sql/i);
  assert.equal(
    crypto.createHash('sha256').update(reassert).digest('hex'),
    '3483e69bbc1ca13ba151b75b59e7b8e192f96f9df2627b829340b4d4e50d62c5',
    'the historical compatibility replay must remain byte-for-byte immutable',
  );
  assert.doesNotMatch(currentClosure, /plpgsql_check\./i);

  for (const [name, sourceName] of repairTargets) {
    const source = normalizeLf(fs.readFileSync(path.join(repeatableDir, sourceName), 'utf8'));
    if (name === 'contract_week_manual_upsert_atomic') {
      assert.notEqual(
        definition(currentClosure, name),
        definition(source, name),
        `${name} must retain the source-proved later current owner distinction`,
      );
      continue;
    }
    assert.equal(definition(currentClosure, name), definition(source, name), `${name} must be byte-identical`);
  }
  const f7Index = finalAuthorityClosure.indexOf('\\ir 30082026_2358_banking_pay_dirty_apply_family_authority_repair_v1.sql');
  const manualIndex = finalAuthorityClosure.indexOf('\\ir 27082026_2205_candidate_weekly_manager_finalisation_authority_v1.sql');
  assert.ok(f7Index >= 0 && manualIndex > f7Index,
    'the exact later current manual-upsert owner must reassert after the historical closure');
  const bulkAuthorise = definition(currentClosure, 'bulk_authorise_dataset_v1');
  assert.match(bulkAuthorise, /DUPLICATE_EXPENSE_REVIEW[\s\S]*can_bulk_authorise_calc/i);
  assert.match(bulkAuthorise, /processed_review_required/i);
  assert.match(bulkAuthorise, /bulk_authorise_block_code[\s\S]*DUPLICATE_EXPENSE_REVIEW_REQUIRED/i);
  const created = [...currentClosure.matchAll(/CREATE OR REPLACE FUNCTION\s+public\.([a-zA-Z0-9_]+)\s*\(/g)]
    .map((match) => match[1]);
  assert.deepEqual(created, [...repairTargets.map(([name]) => name), 'timesheet_daily_manual_unprocess_atomic']);
  assert.match(
    currentClosure,
    /RETURN public\.timesheet_daily_manual_unprocess_atomic\([\s\S]*p_expected_row_signature => NULL::text[\s\S]*\);/,
  );
  assert.equal((currentClosure.match(/REVOKE ALL ON FUNCTION/g) || []).length, 9);
  assert.equal((currentClosure.match(/GRANT EXECUTE ON FUNCTION/g) || []).length, 9);
  for (const identity of bankingV2PublicServiceIdentities) {
    assert.ok(currentClosure.includes(identity), `missing explicit v2 ACL: ${identity}`);
  }
});

test('changed earlier overlapping authorities force the final reassertion to replay', () => {
  for (const name of incrementalReplayDependencies) {
    const source = normalizeLf(fs.readFileSync(path.join(repeatableDir, name), 'utf8'));
    const sourceHash = crypto.createHash('sha256').update(source).digest('hex');
    const escapedName = name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    assert.match(
      reassert,
      new RegExp(`authority_dependency_sha256:\\s*${escapedName}\\s*\\n-- ${sourceHash}`),
    );
  }
});


test('reconciliation authorities are explicitly replayed and catalogued', () => {
  const includedFiles = [...reassert.matchAll(/^\\ir\s+([^\s]+\.sql)\s*$/gmi)]
    .map((match) => match[1]);
  assert.ok(includedFiles.includes(criticalAuthorityFiles[0]));
  assert.equal(targetedManifest.function_count, targetedManifest.functions.length);

  const mainOwner = targetedManifest.functions.find((entry) =>
    entry.schema === 'private' &&
    entry.name === 'pay_sync_overpayments_from_workbench_workspace_v1');
  assert.ok(mainOwner);
  assert.equal(mainOwner.security_definer, false);
  assert.deepEqual(mainOwner.proconfig, ['search_path=""', 'plpgsql_check.mode=disabled']);
  assert.deepEqual(mainOwner.source_files, [
    'supabase/repeatable/07082026_1015_pay_sync_overpayments_from_workbench_workspace_v1.sql',
  ]);

  const residualOwner = targetedManifest.functions.find((entry) =>
    entry.schema === 'public' &&
    entry.name === '_ctms_candidate_correction_residuals_v1');
  assert.ok(residualOwner);
  assert.equal(residualOwner.identity_arguments,
    'p_session_id uuid, p_candidate_id uuid, p_exclude_pay_batch_id uuid, p_context text');
  assert.equal(residualOwner.security_definer, true);
  assert.deepEqual(residualOwner.proconfig, [
    'search_path=public, extensions, pg_temp',
    'plpgsql_check.mode=disabled',
  ]);
  assert.deepEqual(residualOwner.expanded_acl, [{
    grantee: 'postgres',
    grantor: 'postgres',
    is_grantable: false,
    privilege_type: 'EXECUTE',
  }]);
  assert.deepEqual(residualOwner.source_files, [
    'supabase/repeatable/07082026_1015_pay_sync_overpayments_from_workbench_workspace_v1.sql',
  ]);
});
