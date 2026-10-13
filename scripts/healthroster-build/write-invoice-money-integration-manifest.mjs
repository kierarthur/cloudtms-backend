// Local source inventory only. No database, credentials, network or Git writes.
import fs from 'node:fs';
import crypto from 'node:crypto';
const files = [
  ...['economic-snapshot.js','healthroster-normalise.js','profiles.js','routes.js','source-price-comparator.js','upload-publication-owner.mjs','upload-staging-adapter.mjs','source-invoice-money.js','source-finalised-time.js'].map(f => `broker/src/weekly-source/${f}`),
  'supabase/migrations/13102026_1430_weekly_source_invoice_money_origins.sql',
  ...['1431_weekly_source_invoice_money_owners_v1','1432_weekly_source_invoice_money_publication_v1','1433_weekly_source_invoice_money_finalisation_v1','1434_weekly_source_invoice_money_admission_v1','1435_weekly_source_invoice_only_pay_isolation_v1','1436_weekly_source_invoice_money_acl_inventory_v1'].map(f => `supabase/repeatable/13102026_${f}.sql`),
  ...['1440_weekly_source_invoice_money_staging_hold_verification','1441_weekly_source_invoice_money_owner_acl_verification'].map(f => `supabase/verification/13102026_${f}.sql`),
  'tests/weekly-source/healthroster-invoice-money.test.mjs','tests/weekly-source/invoice-money-source-contract.test.mjs','tests/weekly-source/harness/f5-first-use-fixture.mjs',
  'scripts/healthroster-build/compose-invoice-money-source.mjs','scripts/healthroster-build/write-invoice-money-integration-manifest.mjs',
  'docs/healthroster-build/F5_ACL_ADDITIONS.json','docs/healthroster-build/F5_PAY_SOURCE_CENSUS.json','docs/healthroster-build/F5_PRODUCER_FIXTURE.json','docs/healthroster-build/F5_SOURCE_INPUTS.json','docs/healthroster-build/WP_F5_F6_IMPLEMENTATION.md',
];
const rows = files.map(file => {
  const data = fs.readFileSync(file), source = data.toString('utf8');
  return {file, bytes: data.length, sha256: crypto.createHash('sha256').update(data).digest('hex'),
    ...(file.includes('/repeatable/') ? {function_signatures: [...source.matchAll(/alter function ([\s\S]*?) owner to postgres;/g)].map(m => m[1].replace(/\s+/g, ' ').trim())} : {})};
});
const fixture = JSON.parse(fs.readFileSync('docs/healthroster-build/F5_PRODUCER_FIXTURE.json','utf8'));
const manifest = {
  contract: 'HEALTHROSTER_F5_F6_SOURCE_INTEGRATION_MANIFEST_V1', freeze: 'FROZEN_SOURCE_FOR_ROOT_REVIEW',
  branch: 'codex/healthroster-v2-money-build-13102026', baseline: 'e286533e90c0658c252ba1bcc909b9af909be785',
  files: rows, source_inputs: JSON.parse(fs.readFileSync('docs/healthroster-build/F5_SOURCE_INPUTS.json','utf8')),
  schema: {new_public_tables: [{name: 'weekly_source_invoice_hold_releases', record_class: 'IMMUTABLE_APPEND_ONLY'}],
    new_columns: {weekly_source_upload_rows: ['source_gross_pence bigint','source_vat_pence bigint','finalised_at_utc timestamptz'], weekly_source_billing_movements: ['self_bill_report_row_id uuid','vat_rounding_revision_id uuid'], weekly_source_invoice_presentation_lines: ['invoice_hold_release_id uuid']},
    triggers: ['weekly_source_invoice_placement_identity_guard on weekly_source_invoice_placements', 'weekly_source_immutable_record_guard on weekly_source_invoice_hold_releases', 'weekly_source_immutable_truncate_guard on weekly_source_invoice_hold_releases'],
    expected_class_count_before_report_f7: {total:103,immutable:38,lifecycle:19,stateful:46}},
  fixtures: {prefix: fixture.fixture_prefix, scope: fixture.scope, fixed_week:fixture.fixed_week_ending, source_sha256:fixture.source_sha256, source_bytes:fixture.byte_count,
    rows_by_exact_seed_or_parent: {tms_users:2,clients:1,client_settings:1,weekly_source_groups:1,weekly_source_group_clients:1,weekly_source_client_policies:1,weekly_source_cycles:1,
      weekly_source_rule_sets:1,weekly_source_rule_set_versions:1,weekly_source_group_rule_assignments:1,weekly_source_uploads:1,weekly_source_physical_rows:2,weekly_source_upload_rows:1,weekly_source_money_cell_evidence:2,weekly_source_invoice_hold_releases:1},
    financial_rows_expected:0, root_must_add_all_native_audit_attempt_or_generated_id_parent_counts:true},
  source_checks: {behavior_and_source_tests:67,passed:67,skipped:0,failed:0,node_syntax:'PASS',git_diff_check:'PASS',database_check:'Expected new migration unregistered stage; root registers/seals',hosted_database_proof:false},
  root_composition: ['F2+F1+F3+F8+Money complete FIN', 'Report arm in ONE four-fact self-bill helper', 'Acacium profile/normaliser dispatcher with HealthRoster hunks', 'full admin Source difference warning reader', 'invoice Report origins/admission/placement/renderer', 'Protection source G1 closure', 'final ACL/include/generated contract union'],
  engineering_holds: ['Earlier different OPEN window native routing', 'Post-final VAT_FIT with non-SUM S09 immutable rounding authority', 'Report protected/NEXT/proposed effective pay witness', 'Operational VAT rounding identity A/B actual native proof', 'Actual hosted financial/API/Office/PDF/concurrency/Bank-hook gates'],
  permissions: {database_calls:false,docker:false,commit:false,push:false,release:false,activation:false,bank_owners_edited:false},
};
fs.writeFileSync('docs/healthroster-build/F5_INTEGRATION_MANIFEST.json',JSON.stringify(manifest,null,2)+'\n');
process.stdout.write(JSON.stringify({files:rows.length,function_signatures:rows.flatMap(r=>r.function_signatures||[]).length,sql:rows.filter(r=>r.file.endsWith('.sql')).map(({file,bytes,sha256})=>({file,bytes,sha256}))})+'\n');
