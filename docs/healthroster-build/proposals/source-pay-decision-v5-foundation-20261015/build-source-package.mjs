import { createHash } from 'node:crypto';
import { readFileSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { exactAtomicBody, scanManagedSql } from '../../../../scripts/cloudtms-managed-release-sql.mjs';

// Packaging only. The managed scanner treats PL/pgSQL bodies as opaque; it is
// deliberately not described as PostgreSQL parsing or native acceptance proof.
const base = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(base, '../../../..');
const sha = text => createHash('sha256').update(text).digest('hex');
const stages = [
  { id: 'HEALTH_METADATA', requires: ['Current genuine native schema and WP-PP actor predicate'], files: [
    'one-time/01_complete_health_decision_metadata.sqlinc',
    'one-time/02_complete_retained_native_preparations.sqlinc'] },
  { id: 'HEALTH_APPLICATION_AFTER_BANK_CUT', requires: [
    'Banking real Source cut/cut-member schema with exact decision/assembly/review relationships'], files: [
    'one-time/03_complete_acceptance_applications_and_adoptions.sqlinc'] },
  { id: 'HEALTH_BRIDGE_AFTER_BANK_PUBLICATION', requires: [
    'Banking canonical Source publication parent and exact per-component publication member schema'], files: [
    'one-time/04_complete_canonical_publication_bridge.sqlinc',
    'one-time/05_complete_owner_only_rls.sqlinc'] },
  { id: 'HEALTH_OWNERS', requires: [
    'All eleven Health tables and real Banking cut/publication owners',
    'Physical H103 direct alias/placement/evidence/archive-consumption owners and genuine Banking move publication schema',
    'Invoice owner exact original-identity Source certificate/assert kernels and saved accepted move Source accessor'], files: [
    'forward/01_health_metadata_guards.sqlinc',
    'forward/02_atomic_source_run_birth.sqlinc',
    'forward/03_normal_native_source_gate.sqlinc',
    'forward/04_exact_publication_adoption_bridge.sqlinc',
    'forward/05_application_final_receipt.sqlinc',
    'forward/06_confirmation_run_preparation.sqlinc',
    'forward/07_exact_preparation_application.sqlinc',
    'forward/08_protected_current_placement.sqlinc',
    'forward/09_certified_native_signed_evidence.sqlinc',
    'forward/10_shared_native_context.sqlinc',
    'forward/11_native_source_observation.sqlinc',
    'forward/12_run_free_review_context.sqlinc',
    'forward/13_preview_quote_operands.sqlinc',
    'forward/14_native_preparation_request_scope.sqlinc',
    'forward/15_record_owned_native_adoption.sqlinc',
    'forward/16_native_next_implementation.sqlinc',
    'forward/17_source_composed_owner_route.sqlinc',
    'forward/18_current_native_owner_route.sqlinc',
    'forward/19_exact_local_head_capture.sqlinc',
    'forward/20_native_local_stage_implementation.sqlinc',
    'forward/21_native_local_complete_implementation.sqlinc',
    'forward/22_full_native_payload_preparation.sqlinc',
    'forward/23_same_transaction_native_dispatch.sqlinc',
    'forward/24_genuine_local_deferred_release_proof.sqlinc',
    'forward/25_actual_local_release_and_saved_status.sqlinc',
    'forward/26_confirmed_quote_operands.sqlinc',
    'forward/27_certified_current_native_scope.sqlinc',
    'forward/28_current_native_entitlement_projection.sqlinc',
    'forward/29_current_local_native_context.sqlinc',
    'forward/30_native_saved_placement_proof.sqlinc',
    'forward/31_current_financial_native_source.sqlinc'] },
  { id: 'CURRENT_NATIVE_ENTRY_FENCES', requires: [
    'private.bpay_next_source_decision_native_status_v1(uuid,uuid,uuid)',
    'Complete current native bodies pinned by NATIVE_ENTRY_RECEIPT.json; root final owner composition'], files: [
    'native-entry/weekly_exceptional_pay_next_publication_status_v1.sqlinc',
    'native-entry/owner_acl.sqlinc'] },
];
const manifest = {
  version: 'HEALTH_SOURCE_NATIVE_SOURCE_PACKAGE_V1',
  reviewed_pack_content_sha256: 'afaf162e59b2e7901ef0dbc3f484ce51c741f0971557f80fdc877bb8b9ecaa88',
  selected_current_native_commit: 'a934ee1d460b444939234e812babccb90dc3ddff',
  qualification: 'PARTIAL_SOURCE_NOT_INSTALLABLE_NATIVE_REQUIRED_NOT_RUN',
  scanner: 'scripts/cloudtms-managed-release-sql.mjs:scanManagedSql+exactAtomicBody',
  scanner_limit: 'Packaging statement boundaries only; function bodies opaque. No PostgreSQL/PLpgSQL compilation claim.',
  stages: [],
  runtime: [],
  open_health_owners: [
    'Public preview begin/quote/ready/seal/abandon and accept/read/resume implementation',
    'Actual preexisting immutable native receipt adoption and whitelisted native dependency rollback composition',
    'Current-B NEXT wrapper/producer integration beyond actual current Local writer/context consumers',
    'Full current-B Source Local native writer/qualification effects and historical read qualification remain REQUIRED_NOT_RUN',
    'Full binding/selected contact orchestration and final canonical Source receipt integration',
  ],
  external_dependencies: [
    { owner: 'Banking', abi: 'private.bpay_next_source_pay_decision_cut_v1(uuid,uuid,uuid,boolean)', state: 'REQUIRE_GENUINE_PRODUCER_RECEIPT' },
    { owner: 'Banking', abi: 'private.bpay_next_source_pay_decision_acceptance_begin_v1(uuid,uuid,uuid,bytea)', state: 'REQUIRE_GENUINE_PRODUCER_RECEIPT' },
    { owner: 'Banking', abi: 'private.bpay_next_source_pay_decision_stage_v1(uuid,uuid,bytea,uuid)', state: 'REQUIRE_GENUINE_PRODUCER_RECEIPT' },
    { owner: 'Banking', abi: 'private.bpay_next_source_decision_native_status_v1(uuid,uuid,uuid)', state: 'REQUIRE_GENUINE_PRODUCER_RECEIPT' },
    { owner: 'Banking existing protected producer', abi: 'private.bpay_next_stage_protected_source_v1(uuid,uuid,uuid,jsonb)',
      source: 'supabase/repeatable/04102026_0345_banking_pay_next_protected_source_v1.sql',
      case: 'Accepted A-to-B move: producer still validates original family/root Contract A; caller current-B qualification alone cannot satisfy that invariant.',
      state: 'MOVED_B_NEXT_REQUIRES_EXISTING_PRODUCER_QUALIFICATION_RECEIPT_NO_BANK_BODY_EDIT' },
    { owner: 'Invoice', abi: 'private.weekly_source_invoice_certificate_assert_v1(jsonb,boolean)', state: 'REQUIRE_GENUINE_PRODUCER_RECEIPT' },
    { owner: 'Invoice', abi: 'private.invoice_contract_move_source_certificate_v1(uuid,boolean)',
      source_commit: '330a4770a208f0c6a927fd3a7ebee366dbc96340', state: 'SOURCE_PUBLISHED_NATIVE_REQUIRED_NOT_RUN' },
    { owner: 'Physical', abi: 'private.timesheet_contract_current_placement_v1(uuid)', state: 'REQUIRE_GENUINE_PRODUCER_RECEIPT' },
  ],
  native_tests: 'REQUIRED_NOT_RUN',
  native_NP13: { state: 'REQUIRED_NOT_RUN', current_financial_source_first_use_negatives: [
    'Unmoved family cannot claim moved B source; missing real B financial is a genuine prerequisite refusal',
    'Wrong/inactive actor/client/family/event/source cycle/cutoff or mismatched expected placement digest refuses',
    'Wrong B Timesheet/Contract/current row/version/native row hash/source revision or extra service DTO keys refuses',
    'Original context timesheet/contract_record/root_financial/candidate_submission A retained unchanged',
    'Actual B stream source is sealed in full native payload; stage rehashes current actual B row/version before write',
    'Own native Local stage/complete uses B while original family/approval/source invoice/evidence A remains immutable',
    'Later B-to-C retains genuine accepted native Local B receipt via immutable stage/physical operation/member proof',
  ] },
  database_actions: 'NONE',
};
const declarations = new Set();
for (const stage of stages) {
  const output = { id: stage.id, requires: stage.requires, files: [] };
  for (const relative of stage.files) {
    const text = readFileSync(path.join(base, relative), 'utf8');
    const body = exactAtomicBody(text);
    const parsed = scanManagedSql(body);
    const functions = [];
    for (const match of text.matchAll(/create or replace function\s+([a-z_0-9.]+)\s*\(([^)]*)\)/gi)) {
      const signature = `${match[1]}(${match[2].replace(/\s+/g, ' ').trim()})`;
      if (declarations.has(signature)) throw new Error(`Duplicate owner ${signature}`);
      declarations.add(signature); functions.push(signature);
    }
    output.files.push({ path: relative, sha256: sha(text), bytes: Buffer.byteLength(text),
      packaging_statements: parsed.statements.length, functions });
  }
  manifest.stages.push(output);
}
for (const relative of [
  'broker/src/weekly-source/protected-action-orchestrator.mjs',
  'broker/src/weekly-source/protected-next-preview.mjs',
  'broker/src/weekly-source/source-native-contract.mjs',
  'broker/src/weekly-source/source-pay-decision-request.mjs',
  'broker/src/weekly-source/source-pay-decision-preparation.mjs',
  'broker/src/weekly-source/source-native-economic.mjs',
  'broker/src/weekly-source/source-unprotect-worker.mjs',
  'broker/src/weekly-source/source-current-financial.mjs',
  'broker/src/banking-pay/weekly-source-c1-components.mjs',
  'tests/weekly-source/protected-action-orchestrator.test.mjs',
  'tests/weekly-source/source-native-contract.test.mjs',
  'tests/weekly-source/source-pay-decision-request.test.mjs',
  'tests/weekly-source/source-pay-decision-preparation.test.mjs',
  'tests/weekly-source/source-protected-placement.test.mjs',
  'tests/weekly-source/source-native-package.test.mjs',
  'tests/weekly-source/source-native-economic.test.mjs',
  'tests/weekly-source/source-unprotect-worker.test.mjs',
  'tests/weekly-source/source-native-generators.test.mjs',
  'tests/weekly-source/source-current-financial.test.mjs',
]) {
  const text = readFileSync(path.join(root, relative), 'utf8');
  manifest.runtime.push({ path: relative, sha256: sha(text), bytes: Buffer.byteLength(text) });
}
writeFileSync(path.join(base, 'SOURCE_PACKAGE_RECEIPT.json'), JSON.stringify(manifest, null, 2) + '\n');
process.stdout.write(`Scanned ${manifest.stages.reduce((sum, s) => sum + s.files.length, 0)} SQL inputs; ${declarations.size} unique owner declarations. Native proof remains REQUIRED_NOT_RUN.\n`);
