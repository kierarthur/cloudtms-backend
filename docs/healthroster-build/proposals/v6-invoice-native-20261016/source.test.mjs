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

test('prospective grouping stays inside the native kernel and does not allocate a target',()=>{
 const prospective=sql('032_prospective_group.sqlinc');
 assert.match(prospective,/timesheet_contract_move_scope_v1\(o.id,false\)/);
 assert.doesNotMatch(prospective,/gen_random_uuid|insert into|update public\.|total_charge_ex_vat\s*[:=]/i);
 assert.match(sql('033_native_grouping.sqlinc'),/prospective\.fact->'settings_authority'/);
 assert.match(sql('081_issue_registration.sqlinc'),/CONTRACT_MOVE_PROSPECTIVE_INPUT_OWNER_ONLY/);
 assert.match(sql('040_prepare.sqlinc'),/CONTRACT_MOVE_NATIVE_GROUP_REVIEW_CHANGED/);
});

test('selected Drafts use native removal/addition and the actual attached financial guard',()=>{
 const selected=sql('037_selected_draft.sqlinc');
 assert.match(selected,/public\.invoice_apply_edits\(i.id/);
 assert.match(selected,/private\.timesheet_contract_move_financial_row_guard_v1/);
 assert.doesNotMatch(selected,/public\.timesheet_archived_evidence_guard_v1/);
 assert.match(selected,/CONTRACT_MOVE_NATIVE_SELECTED_DRAFT_RESULT_UNPROVED/);
 assert.match(selected,/v_unselected/);
 assert.match(sql('036_native_selected_edit.sqlinc'),/invoice_contract_move_selected_financial_authorise_v1\(to_jsonb\(tfu_lock\)/);
 assert.match(sql('036a_native_selected_unlock.sqlinc'),/limit 1 for update/);
 assert.doesNotMatch(sql('040_prepare.sqlinc'),/SELECTED_DRAFT_NATIVE_ADMISSION_UNQUALIFIED/);
});

test('repeat correction retains original issued credit ownership and uses direct current placement',()=>{
 const owner=sql('074_current_billing.sqlinc');
 assert.match(owner,/timesheet_contract_current_placements/);
 assert.match(owner,/s\.disposition='EDIT_DRAFT'/);
 assert.doesNotMatch(owner,/with recursive|created_at.*desc|updated_at.*desc/i);
 assert.match(sql('075_issue_owner.sqlinc'),/o\.state in\('COMPLETE','SUPERSEDED'\)/);
 assert.match(sql('075_issue_owner.sqlinc'),/Include retained old credits\/groups/);
 assert.match(sql('050_credit_owner.sqlinc'),/v is null.*SIGNED_CORRECTION/);
 assert.match(sql('050_credit_owner.sqlinc'),/o\.state in\('COMPLETE','SUPERSEDED'\)/);
});

test('Source B cannot enter ordinary collection and original A uses certified eligibility',()=>{
 assert.match(sql('033a_native_collection.sqlinc'),/not private\.timesheet_contract_source_pay_only_v1/);
 assert.match(sql('033_native_grouping.sqlinc'),/SOURCE_ORIGINAL_INVOICE_SUBJECT_REQUIRED/);
 assert.match(sql('070_generation_native.sqlinc'),/timesheet_contract_source_original_eligible_v1\(ts.timesheet_id\)/);
 assert.match(sql('018_source_original.sqlinc'),/timesheet_contract_move_accepting_v1/);
 assert.match(sql('021a_source_duty.sqlinc'),/timesheet_contract_source_original_eligible_v1\(v_root.timesheet_id\)/);
});

test('CM credit Issue validates frozen original inputs instead of requiring a current reversal row',()=>{
 const admission=sql('079_native_issue_admission.sqlinc');
 assert.match(admission,/cm_credits as materialized/);
 assert.match(admission,/invoice_contract_move_credit_invoice_valid_v1\(s.invoice_id,false\)/);
 assert.equal((admission.match(/not exists\(select 1 from cm_credits cm/g)||[]).length,3);
 assert.match(sql('076_credit_issue_admission.sqlinc'),/CONTRACT_MOVE_CREDIT_INVOICE_CHANGED/);
 assert.match(sql('055_credit_scope_projection.sqlinc'),/CONTRACT_MOVE_CREDIT_SCOPE_V1/);
});

test('legacy Source uses the one native projection, with a saved plan before any original-A line insertion',()=>{
 const proof=JSON.parse(read('SOURCE_PARSE_RESULT.json')).native_projection;
 assert.equal(proof.unchanged_native_projection,true);
 assert.ok(proof.operand_count>20);
 const core=sql('070_generation_native.sqlinc');
 assert.match(core,/native_planned_lines as materialized/);
 assert.match(core,/deferred_existing_source_headers as/);
 assert.match(core,/not exists\(select 1 from source_original_missing_witness missing where missing.chunk_id=e.chunk_id\)/);
 assert.match(core,/source_original_line_plan_sha256/);
 assert.match(core,/payload_json=c.payload_json-'source_original_native_claim'/);
 assert.match(sql('024_line_admission.sqlinc'),/invoice_source_original_line_allowed_v1\(p_new\)/);
});

test('native Source witness rejects stale owner, changed line/value, duplicate coverage and changed replay',()=>{
 const guard=sql('019_source_generation_witness.sqlinc');
 assert.match(guard,/s->>'backend_pid' is distinct from pg_backend_pid\(\)::text/);
 assert.match(guard,/s->>'transaction_id' is distinct from pg_current_xact_id\(\)::text/);
 assert.match(guard,/s->>'lease_token' is distinct from c.lease_token::text/);
 assert.match(guard,/s->>'fence_token' is distinct from c.fence_token::text/);
 assert.match(guard,/c.operation_control_version<>o.control_version/);
 assert.match(guard,/SOURCE_ORIGINAL_INTENDED_LINE_CHANGED/);
 assert.match(guard,/SOURCE_ORIGINAL_INVOICE_BASIS_CHANGED/);
 assert.match(guard,/SOURCE_ORIGINAL_DUPLICATE_COVERAGE/);
 assert.match(guard,/SOURCE_ORIGINAL_REPLAY_LINE_CHANGED/);
 assert.doesNotMatch(guard,/set_config|current_setting|INVOICE_SOURCE_ORIGINAL|config_json/);
});

test('synchronous Source admission preserves its native projection and proves one real header transaction',()=>{
 const proof=JSON.parse(read('SOURCE_PARSE_RESULT.json')).source_admission_projection;
 assert.equal(proof.unchanged_native_projection,true);assert.equal(proof.operand_count,27);
 const owner=sql('024a_source_native_admission.sqlinc'),guard=sql('019a_source_admission_witness.sqlinc');
 assert.ok(owner.indexOf('invoice_source_admission_line_enter_v1')<owner.indexOf('insert into public.invoice_lines('));
 assert.ok(owner.indexOf('invoice_source_admission_line_finish_v1')>owner.indexOf('returning id into v_invoice_line_id;'));
 for(const condition of ['SOURCE_ORIGINAL_ADMISSION_OWNER_STALE','SOURCE_ORIGINAL_ADMISSION_AUTHORITY_CHANGED',
  'SOURCE_ORIGINAL_ADMISSION_FINAL_STALE','SOURCE_ORIGINAL_ADMISSION_MOVEMENT_CHANGED',
  'SOURCE_ORIGINAL_ADMISSION_LINE_CHANGED','SOURCE_ORIGINAL_ADMISSION_DUPLICATE_LINE','SOURCE_ORIGINAL_ADMISSION_RESULT_UNPROVED'])assert.ok(guard.includes(condition));
 assert.match(guard,/s->>'backend_pid' is distinct from pg_backend_pid\(\)::text/);
 assert.match(guard,/s->>'transaction_id' is distinct from pg_current_xact_id\(\)::text/);
 assert.match(guard,/header_snapshot_json=header_snapshot_json-'source_original_native_admission'/);
 assert.doesNotMatch(guard,/set_config|current_setting|service_role|INVOICE_SOURCE_ORIGINAL/);
 assert.doesNotMatch(sql('024_line_admission.sqlinc'),/current_setting/);
});

test('native Source allocation retains exact whole rows except native invoice placement',()=>{
 const owner=sql('024b_source_native_allocation.sqlinc'),guard=sql('019b_source_allocation_witness.sqlinc');
 assert.match(owner,/perform private\.invoice_source_allocation_enter_v1/);
 assert.match(owner,/set invoice_id=v_destination_invoice_id/);
 assert.ok(owner.indexOf('invoice_source_allocation_finish_v1')<owner.indexOf('for v_binding in'));
 assert.match(guard,/SOURCE_ORIGINAL_ALLOCATION_IDLE_DRAFTS_CHANGED/);
 assert.match(guard,/SOURCE_ORIGINAL_ALLOCATION_BINDING_CHANGED/);
 assert.match(guard,/SOURCE_ORIGINAL_ALLOCATION_LINE_CHANGED_OR_DUPLICATE/);
 assert.match(guard,/to_jsonb\(old\)\|\|jsonb_build_object\('invoice_id'/);
 assert.match(guard,/header_snapshot_json=header_snapshot_json-'source_original_native_allocation'/);
 assert.doesNotMatch(guard,/set_config|current_setting|INVOICE_SOURCE_ORIGINAL|new_final/);
});

test('Daily native overlay keeps actual identity and compares real approved finance, without Contract aliases',()=>{
 const basis=sql('032a_daily_native_basis.sqlinc'),prepare=sql('040_prepare.sqlinc');
 assert.match(basis,/daily_rate_correction_normalized_snapshot_v1\(v_context,p_daily->'calculation_snapshot',p_daily->'rate_provenance',p_actor_user_id\)/);
 assert.match(basis,/DAILY_RATE_NORMALIZED_SNAPSHOT_MISMATCH/);
 assert.match(basis,/daily_rate_correction_accepting_v1\(o.id\)/);
 assert.match(basis,/m.from_timesheet_id is distinct from m.to_timesheet_id/);
 assert.match(basis,/m.to_financial_id is distinct from private.daily_rate_correction_financial_id_v1/);
 assert.match(basis,/DAILY_RATE_ACCEPTED_NATIVE_VALUATION_CHANGED/);
 assert.match(prepare,/o.purpose='CONTRACT_CORRECTION' and not exists/);
 assert.match(prepare,/slice->>'original_financial_id' is distinct from member.from_financial_id::text/);
 assert.doesNotMatch(basis,/insert into|create table|gen_random_uuid|daily_rate_calculator_round/);
 assert.match(sql('033_native_grouping.sqlinc'),/jsonb_populate_record\(original_tf,coalesce\(prospective.fact->'financial_snapshot'/);
 assert.match(sql('074_current_billing.sqlinc'),/f.rate_source_refs_json->>'correction_operation_id'=o.id::text/);
 const reserve=sql('045_preparation_native.sqlinc');
 assert.match(reserve,/current_finance.id=m.current_financial_id and current_finance.is_current/);
 assert.match(reserve,/m.current_timesheet_id::text\|\|':'\|\|m.current_financial_id::text/);
});

test('replacement documents read original signed evidence and keep current attachment policy',()=>{
 const snapshot=sql('022_source_presentation.sqlinc'),issue=sql('079_native_issue_admission.sqlinc');
 assert.match(snapshot,/invoice_evidence_subjects as materialized/);
 assert.match(snapshot,/original.source->>'original_timesheet_id'/);
 assert.match(snapshot,/v_ts_invoice_precheck pc on pc.timesheet_id=il.current_timesheet_id/);
 assert.match(snapshot,/current_policy on current_policy.timesheet_id=il.current_timesheet_id/);
 assert.match(issue,/timesheet_contract_move_evidence_source_v1\(s.timesheet_id\)/);
 assert.match(issue,/timesheet_contract_move_evidence_source_v1\(r.timesheet_id\)/);
 assert.match(sql('023a_native_document_source.sqlinc'),/invoice_contract_move_original_evidence_eligible_v1/);
 const known=sql('034a_document_preflight.sqlinc');
 for(const blocker of ['MANUAL_TIMESHEET_SOURCE_MISSING','QR_TIMESHEET_UNSIGNED','ASSET_WORKFLOW_MISSING','REQUIRED_ASSET_FAILED','TIMESHEET_DOCUMENT_FAILED'])assert.ok(known.includes(blocker));
 assert.doesNotMatch(known,/raise exception .*NOT_READY|TIMESHEET_DOCUMENT_NOT_READY|insert into|update public/);
 assert.match(known,/purpose','TIMESHEET'/);
});
