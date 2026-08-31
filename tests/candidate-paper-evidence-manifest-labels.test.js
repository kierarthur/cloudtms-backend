import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';

const read = relative => fs.readFileSync(new URL(`../${relative}`, import.meta.url), 'utf8');
const successorPath = 'supabase/repeatable/30082026_2156_candidate_paper_evidence_manifest_labels_v1.sql';
const verifierPath = 'supabase/verification/30082026_2157_candidate_paper_evidence_manifest_label_verification.sql';
const predecessor = read('supabase/repeatable/26082026_2225_candidate_expense_finalise_signature_recheck_v3.sql');
const successor = read(successorPath);
const verifier = read(verifierPath);
const runtimePath = 'tests/30082026_2207_candidate_paper_evidence_manifest_label_runtime_verification.sql';
const runtimeSql = read(runtimePath);
const runtimeWorkflow = read('.github/workflows/candidate-db-runtime.yml');
const compileFixture = read('tests/fixtures/07082026_2155_candidate_app_local_compile_base.sql');
const release = JSON.parse(read('supabase/release/current-release.json'));

test('PAPER evidence uses the exact server manifest display name with non-paper fallback preserved', () => {
  assert.match(successor, /case when v_is_paper then nullif\(v_paper_page->>'display_name',''\) end[\s\S]*nullif\(v_claim->>'evidence_display_name',''\)[\s\S]*'Candidate submission evidence'/i);
  assert.match(successor, /where expected_page->>'page_key'=v_component\.paper_return_page_key/i);
  assert.match(successor, /on conflict \(candidate_component_id\)[\s\S]*do nothing/i);
  assert.doesNotMatch(successor, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
});

test('the complete successor changes only the QR manifest label expression', () => {
  const predecessorBody = predecessor.slice(predecessor.indexOf('\\set ON_ERROR_STOP on'));
  const expectedBody = predecessorBody.replace(
    "v_target_timesheet_id,v_kind,coalesce(nullif(v_claim->>'evidence_display_name',''),'Candidate submission evidence'),",
    "v_target_timesheet_id,v_kind,coalesce(\n        case when v_is_paper then nullif(v_paper_page->>'display_name','') end,\n        nullif(v_claim->>'evidence_display_name',''),\n        'Candidate submission evidence'\n      ),"
  );
  assert.notEqual(expectedBody, predecessorBody);
  assert.equal(successor.slice(successor.indexOf('\\set ON_ERROR_STOP on')), expectedBody);
});

test('rollback-contained PAPER finalisation proves both named expense pages', () => {
  assert.match(compileFixture, /create table public\.tms_users[\s\S]*email text not null default 'candidate-runtime@example\.invalid'/i);
  assert.match(runtimeSql, /information_schema\.columns[\s\S]*column_name='password_hash'[\s\S]*insert into public\.tms_users\(id,email,password_hash,role,is_active\)[\s\S]*@example\.invalid[\s\S]*UNUSABLE_VERIFICATION_ONLY[\s\S]*'admin'[\s\S]*true[\s\S]*else[\s\S]*insert into public\.tms_users\(id,email,is_active\)/i);
  assert.match(runtimeSql, /insert into public\.contracts\([\s\S]*pay_method_snapshot,default_submission_mode[\s\S]*'PAYE','ELECTRONIC'\)/i);
  assert.match(runtimeSql, /insert into public\.timesheets\([\s\S]*booking_id,occupant_key_norm,hospital_norm,[\s\S]*ward_norm,job_title_norm,[\s\S]*sheet_scope,[\s\S]*'PAPER-COMPLETE-PACK-RUNTIME'[\s\S]*'GCK-PAPER-RUNTIME'[\s\S]*'WEEKLY','MANUAL'/i);
  assert.match(runtimeSql, /insert into public\.timesheets_financials\([\s\S]*timesheet_version[\s\S]*values\(v_timesheet,1,/i);
  assert.match(runtimeSql, /insert into public\.invoice_operations\([\s\S]*'BUILD_DOCUMENT','TIMESHEET',v_timesheet,v_actor[\s\S]*jsonb_build_object\('processor_policy',private\._invoice_processor_limits\(\)\)/i);
  assert.match(runtimeSql, /insert into public\.invoice_document_versions\([\s\S]*operation_id[\s\S]*snapshot_hash[\s\S]*manifest_hash[\s\S]*ready_at_utc,verified_at_utc[\s\S]*v_document_operation[\s\S]*encode\(extensions\.digest\('\{\}','sha256'\),'hex'\)[\s\S]*encode\(extensions\.digest\('\[\]','sha256'\),'hex'\)/i);
  assert.match(runtimeSql, /manifest_page->>'component_kind'='EXPENSE_SUMMARY'[\s\S]*manifest_page->>'display_name'='Expense summary'[\s\S]*evidence\.display_name=manifest_page->>'display_name'/i);
  assert.match(runtimeSql, /manifest_page->>'component_kind'='EXPENSE_EVIDENCE'[\s\S]*manifest_page->>'display_name'='Other 1'[\s\S]*evidence\.display_name=manifest_page->>'display_name'/i);
  assert.match(runtimeSql, /begin;[\s\S]*rollback;/i);
});

test('release and Candidate runtime install the successor before mandatory first use', () => {
  assert.match(runtimeWorkflow, /30082026_1903_candidate_expense_carrier_anchor_route_v1\.sql[\s\S]*30082026_1339_candidate_paper_manifest_page_qr_v2\.sql[\s\S]*30082026_2156_candidate_paper_evidence_manifest_labels_v1\.sql[\s\S]*30082026_2207_candidate_paper_evidence_manifest_label_runtime_verification\.sql/i);
  assert.ok(release.verificationFiles.includes(verifierPath));
  assert.ok(release.newVerificationFiles.includes(verifierPath));
  assert.ok(release.verificationFiles.includes(runtimePath));
  assert.ok(release.newVerificationFiles.includes(runtimePath));
  assert.match(verifier, /has_function_privilege\('service_role'/i);
  assert.match(verifier, /has_function_privilege\('anon'/i);
  assert.match(verifier, /has_function_privilege\('authenticated'/i);
  assert.match(verifier, /v_owner<>current_user/i);
  assert.match(verifier, /v_security_definer is not true/i);
  assert.match(verifier, /search_path=pg_catalog, public, private, pg_temp/i);
});
