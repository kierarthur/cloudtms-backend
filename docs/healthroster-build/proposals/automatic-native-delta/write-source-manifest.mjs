import { readFileSync, writeFileSync, readdirSync } from 'node:fs';
import { dirname, resolve, relative } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';
const out = dirname(fileURLToPath(import.meta.url));
const lane = resolve(out, '../../../..');
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
const paths = readdirSync(out).filter(name => name !== 'SOURCE_MANIFEST.json').sort().map(name => resolve(out, name));
paths.push(resolve(lane, 'tests/weekly-source/automatic-native-delta-proposal.test.mjs'));
const files = paths.map(path => { const bytes = readFileSync(path); return { path: relative(lane, path).replaceAll('\\', '/'),
  bytes: bytes.length, sha256: hash(bytes) }; });
const newPrivateSignatures = [
  'weekly_source_staged_source_documents_v1(uuid)', 'weekly_source_staged_publication_context_v1(uuid,uuid)',
  'weekly_source_approved_detail_evidence_v1(uuid,jsonb)', 'weekly_source_pay_query_local_receipt_event_v1(uuid,uuid,uuid,uuid)',
  'weekly_source_pay_query_c1_receipt_event_v1(uuid,uuid,uuid,uuid)', 'weekly_source_pay_query_next_receipt_event_v1(uuid,uuid,uuid,uuid)',
  'weekly_source_staged_prior_wait_receipt_v1(uuid,uuid)', 'weekly_source_expense_authority_evidence_v1(uuid,jsonb)',
  'weekly_source_approved_expense_evidence_v1(uuid,jsonb)', 'weekly_source_staged_entitlement_components_v1(uuid,uuid)',
  'weekly_source_automatic_reconcile_delta_assert_v1(uuid,uuid)',
].map(name=>'private.'+name);
const body = { formatVersion:1, status:'REVIEWED_SOURCE_WIP_NATIVE_ACCEPTANCE_HELD',
  branch:'codex/healthroster-v2-report-build-13102026', assignedBase:'83547d19',
  newPrivateSignatures, newPublicOrServiceSignatures:[], newTableCount:0, newIndexCount:0,
  changedExistingOwners:['private.weekly_source_pay_query_local_receipt_v1(uuid,uuid,uuid)',
    'private.weekly_source_pay_query_c1_receipt_v1(uuid,uuid,uuid)', 'private.weekly_source_pay_query_next_receipt_v1(uuid,uuid,uuid)',
    'public.weekly_exceptional_pay_complete_local_v1(jsonb)'],
  unchangedCompleteCallSiteInputs:['10_complete_stage_request.sqlinc','11_complete_automatic_reconcile.sqlinc'],
  sourceTests:{ command:'node --test tests/weekly-source/automatic-native-delta-proposal.test.mjs',passed:31,failed:0,
    qualification:'Independent synthetic full-component comparison + real existing native carrier + hash-bound complete-source/owner/sequence/ACL/parenthesis guards. No PostgreSQL semantic compilation or native financial/business/API/runtime acceptance.' },
  boundaries:{ sourceOnly:true, mounted:false, databaseCalls:false, docker:false, rootSharedEdits:false,
    frozenReportU3CNInputsEdited:false, publicProducerExposure:false,
    reviewedGitBackupAuthority:'SEND_5B_TO_HEALTHROSTER_FULL_REHEARSAL.md; own feature branch only' },
  runtimeGates:'Root complete native I1/detail/expense/vector/receipt/Source-CAS/concurrency/completion/ordinary-regression/ACL and integrated Bank core journeys remain mandatory; see NATIVE_PROOF_RECIPE.md.',
  files, packageSha256:hash(Buffer.from(files.map(f=>`${f.path}\0${f.sha256}\n`).join(''))),
};
const text=JSON.stringify(body,null,2)+'\n';writeFileSync(resolve(out,'SOURCE_MANIFEST.json'),text);
console.log(JSON.stringify({files:files.length, packageSha256:body.packageSha256,manifestSha256:hash(Buffer.from(text))}));
