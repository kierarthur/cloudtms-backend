import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {fileURLToPath} from 'node:url';
import {scanManagedSql} from '../../../../scripts/cloudtms-managed-release-sql.mjs';
const here=path.dirname(fileURLToPath(import.meta.url));
const read=file=>fs.readFileSync(path.join(here,file),'utf8');
const sql=file=>read('forward/'+file);
const manifest=JSON.parse(read('SOURCE_MANIFEST.json'));

test('recursive source checkpoint has complete, unchanged published bytes',()=>{
 assert.equal(manifest.source_pin,'a934ee1d460b444939234e812babccb90dc3ddff');
 assert.equal(manifest.native,'REQUIRED_NOT_RUN');
 for(const row of manifest.files){assert.equal(crypto.createHash('sha256').update(read(row.file)).digest('hex'),row.sha256,row.file);assert.ok(scanManagedSql(read(row.file)).statements.length,row.file);}
});
test('Source certificate is original invoice evidence, with unknown before-final money',()=>{
 const facts=sql('010_source_invoice_facts.sqlinc'),certificate=sql('015_source_certificate.sqlinc');
 assert.match(facts,/to_jsonb\(native_row\)-'canonical_pay_vector_json'-'total_pay_ex_vat'/);
 assert.doesNotMatch(facts,/private\.bpay_|weekly_source_expense_pay_materialisations/);
 assert.match(certificate,/v_phase='NO_FINAL_REPORT_YET' then null/);
 assert.match(certificate,/v_actual is distinct from p_expected/);
 assert.match(certificate,/original_invoice_timesheet_ids/);
 assert.doesNotMatch(certificate,/current_timesheet_id|current_contract_id|current_financial_cut/);
});
test('Source family is the exact accepted member, including retained superseded aliases',()=>{
 const family=sql('020_source_family.sqlinc');
 assert.match(family,/using\(family_root_id,member_key\)/);
 assert.match(family,/state in\('COMPLETE','SUPERSEDED'\)/);
 assert.match(family,/current_owner\.placement_receipt_json/);
 assert.match(sql('021_source_lineage.sqlinc'),/not v_timesheet\.is_current and not v_inherited_original/);
 assert.match(sql('022_source_presentation.sqlinc'),/scoped_credit_line_rows/);
 assert.match(sql('022_source_presentation.sqlinc'),/source_rounding_suppression_agg/);
 assert.match(sql('022_source_presentation.sqlinc'),/source_receipt_support/);
});
test('claim currentness uses actual component and workflow membership before native fallbacks',()=>{
 const claim=sql('025_claim_placement.sqlinc');
 assert.match(claim,/p_expense_component_id=any\(m\.expense_component_ids\)/);
 assert.match(claim,/w\.id=any\(m\.workflow_ids\)/);
 assert.match(claim,/CANDIDATE_EXPENSE_CURRENT_PLACEMENT_UNBOUND/);
 for(const file of ['026_claim_current.sqlinc','027_claim_owned.sqlinc']){
  const text=sql(file);assert.ok(text.indexOf('timesheet_contract_claim_placement_v1')<text.indexOf('booking_id=v_stored.booking_id'));
 }
 const lock=sql('028_claim_lock.sqlinc');assert.ok(lock.indexOf('timesheet_contract_move_family_lock_v1')<lock.indexOf('for update;'));
 assert.match(sql('029_claim_financial.sqlinc'),/else v_base_agency end/);
 assert.match(sql('030_claim_sync.sqlinc'),/v_component\.owning_timesheet_id,v_component\.expense_component_id/);
 assert.doesNotMatch(sql('030_claim_sync.sqlinc'),/update public\.candidate_submission_workflows/i);
});
test('CONTRACT_MOVE is a closed owner-only DRAFT_ONLY scope and never a fabricated signed correction',()=>{
 const credit=sql('050_credit_owner.sqlinc');
 assert.match(credit,/jsonb_object_keys\(v\)\)<>6/);
 assert.match(credit,/v->>'preparation_mode'<>'DRAFT_ONLY'/);
 assert.doesNotMatch(credit,/reversal_timesheet_id|timesheet_correction_requests|update public\.timesheets_financials/i);
 assert.match(credit,/-l\.total_charge_ex_vat/);
 assert.match(credit,/-l\.vat_amount/);
 assert.match(credit,/-l\.total_inc_vat/);
 assert.match(sql('081_issue_registration.sqlinc'),/invoice_contract_move_issue_commands_v1/);
 assert.match(sql('075_issue_owner.sqlinc'),/CONTRACT_MOVE_CREDIT_OWNER_ONLY/);
});
test('native automatic credit Issue and each Draft-document retry excludes only the owned purpose',()=>{
 const generation=sql('070_generation_native.sqlinc');
 const issuing=generation.slice(generation.indexOf('  credit_issue_ops as materialized ('),generation.indexOf('  selected_credit_issue as materialized ('));
 assert.match(issuing,/not private\.invoice_contract_move_draft_only_v1\(q\.chunk_id\)/);
 assert.equal((generation.match(/q\.command_type<>'GENERATE_CREDIT_NOTE' or private\.invoice_contract_move_draft_only_v1\(q\.chunk_id\)/g)||[]).length,2);
 assert.match(generation,/and not private\.invoice_contract_move_draft_only_v1\(c\.id\)/);
 assert.match(sql('071_generation_dispatch.sqlinc'),/invoice_contract_move_prepare_advance_v1/);
});
test('both FINALISE transitions and the final eligible UPDATE recheck actual issued credit receipts',()=>{
 const issue=sql('080_issue_native.sqlinc');
 assert.match(issue,/s\.exact_document_status='READY' and private\.invoice_contract_move_credit_ready_v1\(s\.invoice_id,true\) then 'FINALISE'/);
 assert.match(issue,/v\.status='READY' and private\.invoice_contract_move_credit_ready_v1\(c\.entity_id,true\) then 'FINALISE'/);
 assert.match(issue,/snapshot_hash'=v\.snapshot_hash\n      and private\.invoice_contract_move_credit_ready_v1\(i\.id,true\)/);
 assert.match(issue,/from eligible e where i\.id=e\.entity_id\n      and private\.invoice_contract_move_credit_ready_v1\(i\.id,true\)/);
 assert.match(sql('075_issue_owner.sqlinc'),/credit\.issued_at_utc is null/);
 assert.match(sql('075_issue_owner.sqlinc'),/c\.result_json->>'document_version_id'=credit\.issued_document_version_id::text/);
});
test('Office Unissue requires its real successful audit and current native Draft',()=>{
 const unissue=sql('024_line_admission.sqlinc');
 assert.match(unissue,/a\.action='INVOICE_UNISSUED'/);
 assert.match(unissue,/i\.issued_at_utc is null/);
 assert.match(unissue,/i\.issued_document_version_id is null/);
 assert.match(unissue,/historical_object_preserved/);
 assert.match(unissue,/later\.action='INVOICE_ISSUED'/);
 assert.doesNotMatch(sql('040_prepare.sqlinc'),/invoice_unissue_one|invoice_unissue_batch/);
});
test('known ordinary admission gap is surfaced before Ready and source/no-invoice creates none',()=>{
 assert.match(sql('035_preflight.sqlinc'),/INVOICE_CONTEXT_UNAVAILABLE/);
 assert.match(sql('035_preflight.sqlinc'),/ready_to_confirm',jsonb_array_length\(v_facts->'known_blockers'\)=0/);
 const preparation=sql('040_prepare.sqlinc');
 assert.ok(preparation.indexOf("'native_parent_operation_id',null")<preparation.indexOf('insert into public.invoice_operations'));
 assert.match(sql('045_preparation_native.sqlinc'),/CONTRACT_MOVE_UNREQUESTED_INVOICE_JOB/);
});
test('new private helpers have owner-only ACLs; no new queue, lease or capability family',()=>{
 const acl=sql('099_new_private_acl.sqlinc');
 assert.match(acl,/revoke all on function private\.weekly_source_invoice_certificate_assert_v1\(jsonb,boolean\) from public,anon,authenticated,service_role/);
 const schema=sql('001_billing_schema.sqlinc');
 assert.equal((schema.match(/create table /gi)||[]).length,6);
 assert.doesNotMatch(schema,/create table[^;]*(?:queue|lease|capabilit)/i);
 assert.doesNotMatch(acl,/grant .*to (?:anon|authenticated|public)/i);
 for(const row of manifest.files)assert.doesNotMatch(read(row.file),/pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i,row.file);
});
