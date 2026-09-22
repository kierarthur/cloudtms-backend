import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const repoRoot = new URL('..', import.meta.url);
const read = relativePath => readFileSync(new URL(relativePath, repoRoot), 'utf8');

const resolver = read(
  'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/'
  + '23072026_2207_private_invoice_generation_resolve_command_groups.sql',
);
const generateClassification = read(
  'supabase/repeatable/27072026_1042_invoice_async_v8/'
  + '27072026_1806_private_invoice_batch_generate_classification_v2.sql',
);
const issueValidation = read(
  'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/'
  + '23072026_2207_private_invoice_issue_validate_batch.sql',
);
const precheck = read('supabase/repeatable/08012026_v_ts_invoice_precheck.sql');

test('Batch Generate applies only the effective reference-to-invoice policy to positive hours', () => {
  assert.match(resolver, /tf\.total_hours/);
  assert.match(
    resolver,
    /when r\.blocker_code='INVOICE_REFERENCE_REQUIRED'[\s\S]*precheck\.require_reference_to_invoice[\s\S]*b\.total_hours,0\)>0[\s\S]*then null/i,
  );
  assert.match(
    resolver,
    /left join public\.v_ts_invoice_precheck precheck[\s\S]*precheck\.timesheet_id=b\.timesheet_id/i,
  );
  assert.match(
    generateClassification,
    /precheck\.require_reference_to_invoice,false\)[\s\S]*financial\.total_hours,0\)>0[\s\S]*reference\.reference_ready,false\)/i,
  );
  assert.doesNotMatch(resolver, /reference_number_required_to_issue_invoice|require_reference_to_pay/i);
});

test('Batch Issue applies only the effective reference-to-issue policy', () => {
  const start = issueValidation.indexOf('reference_checks as materialized');
  const end = issueValidation.indexOf('correction_checks as materialized', start);
  assert.ok(start >= 0 && end > start);
  const referenceChecks = issueValidation.slice(start, end);
  assert.match(
    referenceChecks,
    /precheck\.reference_number_required_to_issue_invoice,false\)/i,
  );
  assert.match(
    referenceChecks,
    /left join public\.v_ts_invoice_precheck precheck[\s\S]*precheck\.timesheet_id=ref\.timesheet_id/i,
  );
  assert.doesNotMatch(referenceChecks, /ref\.is_required|require_reference_to_invoice|require_reference_to_pay/i);
});

test('the canonical precheck retains frozen contract-over-client authority for each independent stage', () => {
  assert.match(
    precheck,
    /authority\.settings_json#>>'\{values,require_reference_to_invoice\}'\)::boolean,false\)[\s\S]*as require_reference_to_invoice/i,
  );
  assert.match(
    precheck,
    /authority\.settings_json#>>'\{values,reference_number_required_to_issue_invoice\}'\)::boolean,[\s\S]*false[\s\S]*as reference_number_required_to_issue_invoice/i,
  );
  assert.match(precheck, /private\._contract_settings_effective_core_v1/i);
  assert.doesNotMatch(precheck, /from\s+public\.client_settings|join\s+public\.client_settings/i);
});
