import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(here, '..', '..');
const read = relativePath => fs.readFileSync(path.join(root, relativePath), 'utf8')
  .replaceAll('\r\n', '\n');
const executableSql = source => source
  .replace(/--[^\n]*/g, '')
  .replace(/\/\*[\s\S]*?\*\//g, '');

const schema = read('supabase/migrations/15092026_1534_weekly_source_plan6_schema.sql');
const acceptance = read('supabase/repeatable/15092026_1534_weekly_source_charge_acceptance_v1.sql');
const workspace = read('supabase/repeatable/15092026_1534_weekly_source_read_projections_v1.sql');
const finalisation = read('supabase/repeatable/15092026_1534_weekly_source_finalisation_v1.sql');
const publication = read('supabase/repeatable/15092026_1534_weekly_source_upload_publication_v1.sql');
const projectionBuild = read('supabase/repeatable/15092026_1534_weekly_source_projection_build_v1.sql');
const rowAdmission = read('supabase/repeatable/17092026_1500_weekly_source_row_admission_guards_v1.sql');
const deliveryTargets = read('supabase/repeatable/15092026_2311_weekly_source_delivery_targets_v1.sql');
const issueValidator = read('supabase/repeatable/02092026_1833_weekly_source_invoice_issue_validator_v1.sql');
const invoiceAdmission = read('supabase/repeatable/15092026_1534_weekly_source_invoice_admission_v1.sql');

test('known NHSP price warnings have an immutable fingerprint-bound acceptance record', () => {
  assert.match(schema, /create table public\.weekly_source_charge_acceptances/);
  assert.match(schema, /acceptance_kind text not null check \(acceptance_kind in \('ACCEPTED_DISPARITY','ACCEPTED_ZERO'\)\)/);
  assert.match(schema, /source_upload_hash bytea not null/);
  assert.match(schema, /source_row_fingerprint bytea not null/);
  assert.match(schema, /contract_and_rate_fingerprint bytea not null/);
  assert.match(schema, /effective_policy_fingerprint bytea not null/);
  assert.match(schema, /charge_calculation_fingerprint bytea not null/);
  assert.match(schema, /acceptance_policy_fingerprint bytea not null/);
  assert.match(schema, /unique \(charge_check_id\)/);
});

test('Office accepts opaque warning keys under one current server proof, never browser financial facts', () => {
  assert.match(acceptance, /'warning_keys','selection_proof'/);
  assert.match(acceptance, /'NHSP_RATE_WARNING_SELECTION_V1'/);
  assert.match(acceptance, /'all-zero-source-charge'/);
  assert.match(acceptance, /'charge-check:'\|\|charge\.id::text/);
  assert.match(acceptance, /v_selected_keys<@v_eligible_keys/);
  assert.match(acceptance, /WEEKLY_SOURCE_CHARGE_ACCEPT_SELECTION_STALE/);
  assert.match(acceptance, /v_profile\.profile_code<>'NHSP_FINAL_BACKING_V1'/);
  assert.doesNotMatch(executableSql(acceptance), /insert\s+into\s+public\.timesheets\b/i);
  assert.doesNotMatch(executableSql(acceptance), /update\s+public\.timesheets\b/i);
  assert.doesNotMatch(executableSql(acceptance), /\bpay_batch\w*\b/i);
  assert.doesNotMatch(executableSql(acceptance), /\bbanking_pay\w*\b/i);
});

test('Weekly Source first-use paths do not schema-qualify PostgreSQL conditional expressions', () => {
  for (const source of [acceptance, deliveryTargets]) {
    assert.doesNotMatch(
      executableSql(source),
      /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i,
    );
  }
});

test('workspace groups zero-charge warnings and fails finalisation closed until current acceptance', () => {
  assert.match(workspace, /'contract','NHSP_RATE_WARNING_WORKSPACE_V1'/);
  assert.match(workspace, /'phase'.*'PREFINAL'[\s\S]*'FINAL_AWAITING_ACCEPTANCE'[\s\S]*'READY'/);
  assert.match(workspace, /'warning_key','all-zero-source-charge'/);
  assert.match(workspace, /'warning','Possible NHSP rate card issue'/);
  assert.match(workspace, /'warning','Rate card expired or wrong Contract rate'/);
  assert.match(workspace, /'title','Possible Trust rate card issue'/);
  assert.match(workspace, /'action','ACCEPT_NHSP_SOURCE_CHARGES'/);
  assert.match(workspace, /'key','warning_keys','proof_key','selection_proof'/);
  assert.match(workspace, /v_rate_warning_unaccepted_count=0/);
  assert.match(workspace, /'rate_warnings',v_rate_warnings/);
});

test('a valid zero-charge NHSP worked row remains publishable, positive for pay, and overlap checked', () => {
  const executablePublication = executableSql(publication);
  assert.doesNotMatch(
    executablePublication,
    /source_row\.source_shift_charge_pence\s*=\s*0/,
    'publication must not reject an otherwise valid £0 NHSP source value',
  );
  assert.match(
    executableSql(projectionBuild),
    /case when coalesce\(v_source_row\.source_shift_charge_pence,0\)<0 then -1 else 1 end/,
  );
  assert.match(
    executableSql(finalisation),
    /case when v_row\.source_shift_charge_pence<0 then -1 else 1 end/,
  );
  assert.match(
    executableSql(rowAdmission),
    /coalesce\(source_row\.source_shift_charge_pence,1\)>=0/,
  );
});

test('accepted warning state is sealed through movement, invoice presentation and issue validation', () => {
  assert.match(finalisation, /'EXACT','SOURCE_ROUNDING_EQUIVALENT','ACCEPTED_DISPARITY','ACCEPTED_ZERO'/);
  assert.match(finalisation, /weekly_source_charge_acceptance_policy_fingerprint_v1/);
  assert.match(finalisation, /p_charge_acceptance_id/);
  assert.match(invoiceAdmission, /charge_acceptance_id/);
  assert.match(issueValidator, /presentation\.charge_acceptance_id is distinct from movement\.charge_acceptance_id/);
  assert.match(issueValidator, /acceptance\.acceptance_kind=presentation\.price_check_result/);
  assert.match(issueValidator, /acceptance\.acceptance_policy_fingerprint=/);
});

test('initial pay withdrawal cannot remove immutable final-source invoice admission', () => {
  assert.equal(fs.existsSync(path.join(root, 'supabase/repeatable/19092026_0200_weekly_source_invoice_withdraw_admission_v1.sql')), false);
  assert.equal(fs.existsSync(path.join(root, 'supabase/migrations/19092026_0200_weekly_source_invoice_admission_withdrawal.sql')), false);
  assert.doesNotMatch(invoiceAdmission, /WITHDRAW_SOURCE_INVOICE/);
  assert.match(invoiceAdmission, /'ADMIT_SOURCE_INVOICE','MOVE_SOURCE_INVOICE'/);
});
