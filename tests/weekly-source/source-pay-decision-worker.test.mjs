import test from 'node:test';
import assert from 'node:assert/strict';
import {nativeSnapshotFixture} from './helpers/source-native-snapshot-fixture.mjs';
import { previewSourcePayDecision,confirmSourcePayDecision,abandonSourcePayDecisionPreview,readSourcePayDecisionSource } from '../../broker/src/weekly-source/source-pay-decision-worker.mjs';
import { decodeSourcePayDecisionReview,decodeSourcePayDecisionReceipt,decodeSourcePreviewBegin,decodeSourceInvoiceCertificate,decodeSourceQuantity,decodeSourcePayDecisionSourceRead } from '../../broker/src/weekly-source/source-pay-decision-contract.mjs';
const id=n=>`98000000-0000-4000-8000-${String(n).padStart(12,'0')}`;const hash='a'.repeat(64);
const actor=id(9);const quantity=()=>({version:'PAY_DISPOSITION_QUANTITY_V1',mode:'WHOLE_SHIFT',quantity_state:'PRE_FINANCIAL',slices:[]});
const invoice=()=>({phase:'NO_FINAL_REPORT_YET',hash,source_row_ids:[id(4)],accepted_final_member_ids:[],original_invoice_timesheet_ids:[],invoice_ids:[],net:null,vat:null,gross:null});
const preview=()=>({version:'SOURCE_PAY_DECISION_V1',decision_id:id(1),request_id:id(2),decision:'EXCLUDE_FUTURE_WORK_PAY',selection:[{
 scope_kind:'SOURCE_ROW',scope_mode:'WHOLE_SHIFT',family_root_id:null,source_work_event_id:null,source_row_id:id(4),component_keys:[],work_dates:['2026-10-12'],expected_work_revision:null}]});
const member=()=>({member_key:'actual:source-member',scope_kind:'SOURCE_ROW',scope_mode:'WHOLE_SHIFT',family_root_id:null,candidate_id:id(5),actual_client_id:id(6),source_row_id:id(4),
 source_work_event_id:null,component_keys:[],work_dates:['2026-10-12'],expected_work_revision:null,branch:'FUTURE_UNSIGNED',qualification:'NOT_APPROVED',
 supported_quantities:quantity(),excluded_quantities:quantity(),retained_commitments:[],invoice_certificate:invoice(),selected_issue_ids:[],next_actions:[]});
const review=()=>({version:'SOURCE_PAY_DECISION_REVIEW_V1',decision_id:id(1),request_id:id(2),review_id:id(3),review_digest:hash,state_revision:'9007199254740993',
 expected_pay_cut_hash:hash,expected_source_invoice_hash:hash,expected_issue_set_hash:hash,decision:preview().decision,supersedes_decision_id:null,prior_dispositions:[],prior_dispositions_sha256:hash,
 members:[member()],next_actions:[]});
const begin=()=>({version:'SOURCE_PAY_DECISION_PREVIEW_BEGIN_RESULT_V1',decision_id:id(1),request_id:id(2),assembly_id:id(7),assembly_state:'ASSEMBLING',cut_id:id(8),
 cut_no:'9007199254740993',expected_pay_cut_hash:hash,draft_scope_sha256:hash,expires_at_utc:'2099-10-16T17:00:00Z',required_quote_members:[],sealed_review:null});
const accept=()=>({version:'SOURCE_PAY_DECISION_V1',action:'ACCEPT',decision_id:id(1),request_id:id(10),review_id:id(3),review_digest:hash,decision:'ACCEPT_FINAL_SOURCE_PAY',
 supersedes_decision_id:null,expected_prior_dispositions:[],expected_prior_dispositions_sha256:hash,invoice_policy:'KEEP_SOURCE_INVOICE',
 contact_policy:'DO_NOT_QUERY_SELECTED_ISSUES',reason:'Office evidence reviewed',evidence_refs:[],expected_pay_cut_hash:hash,expected_source_invoice_hash:hash,expected_issue_set_hash:hash});
const receipt=()=>({version:'SOURCE_PAY_DECISION_RECEIPT_V1',decision_id:id(1),request_id:id(10),review_id:id(3),state_revision:'9007199254740994',
 accepted_at_utc:'2026-10-16T17:00:00Z',actor_user_id:actor,reason:accept().reason,decision:accept().decision,supersedes_decision_id:null,prior_dispositions:[],prior_dispositions_sha256:hash,
 members:[{member_key:member().member_key,scope_kind:'SOURCE_ROW',scope_mode:'WHOLE_SHIFT',family_root_id:null,source_row_id:id(4),source_work_event_id:null,
 pay_disposition_id:id(11),pay_result:'SOURCE_PAY_REVIEW_ACCEPTED',invoice_result:'NO_FINAL_REPORT_YET',invoice_certificate:invoice(),
 protection_release:{status:'NOT_REQUIRED',preparation_id:null,orchestration_run_id:null,native_receipt_hash:null,next_action:null},retired_issue_ids:[],next_actions:[]}],
 financial_publications:{decision_id:id(1),cut_id:id(8),commands:[{candidate_id:id(5),command_id:id(12),member_count:1,manifest_sha256:hash}],publication_digest:hash},
 read_result_url:`/api/weekly-source/pay-decisions/${id(1)}?request_id=${id(10)}`});
function calculator(){assert.fail('No genuine active protection member requires a calculator.');}
test('No-protection preview executes begin, exact-empty READY and atomic seal, preserving authentic NULL identities',async()=>{
 const calls=[];const dataRpc=async(name,args)=>{calls.push([name,args]);if(name.endsWith('begin_v1'))return begin();if(name.endsWith('ready_v1'))return {version:'SOURCE_PAY_DECISION_PREVIEW_READY_RESULT_V1',assembly_id:id(7),assembly_sha256:hash};if(name==='weekly_source_pay_decision_preview_v1')return review();assert.fail(name);};
 assert.deepEqual(await previewSourcePayDecision({request:preview(),actorUserId:actor,dataRpc,calculateWeeklySnapshot:calculator}),review());
 assert.deepEqual(calls.map(x=>x[0]),['weekly_source_pay_decision_preview_begin_v1','weekly_source_pay_decision_preview_ready_v1','weekly_source_pay_decision_preview_v1']);
 assert.deepEqual(calls[0][1].p_request.browser_request.evidence_refs,[]);
 assert.equal(calls[2][1].p_request.actor_user_id,actor);assert.deepEqual(calls[2][1].p_request.unprotect_preparations,[]);
 assert.equal(calls[2][1].p_request.selection[0].family_root_id,null);
});
test('Exact sealed replay returns saved review before READY, quotes or calculation, even after assembly expiry',async()=>{
 let calls=0;const result=await previewSourcePayDecision({request:preview(),actorUserId:actor,calculateWeeklySnapshot:calculator,dataRpc:async()=>{calls++;
 return {...begin(),assembly_state:'SEALED',expires_at_utc:'2020-01-01T00:00:00Z',sealed_review:review()};}});
 assert.deepEqual(result,review());assert.equal(calls,1);
});
test('Unknown seal is propagated unchanged; it never cancels, recalculates or guesses absence',async()=>{
 const unknown=Object.assign(new Error('Connection lost'),{outcome:'UNKNOWN'});const calls=[];
 await assert.rejects(previewSourcePayDecision({request:preview(),actorUserId:actor,calculateWeeklySnapshot:calculator,dataRpc:async name=>{
  calls.push(name);if(name.endsWith('begin_v1'))return begin();if(name.endsWith('ready_v1'))return {version:'SOURCE_PAY_DECISION_PREVIEW_READY_RESULT_V1',assembly_id:id(7),assembly_sha256:hash};throw unknown;
 }}),e=>e===unknown);assert.equal(calls.length,3);assert.ok(!calls.some(x=>x.includes('abandon')));
});
test('Changed assembly/cut/decision/request and positive extra protection stop before later RPCs',async()=>{
 const variants=[v=>v.cut_no=9007199254740993,v=>v.decision_id=id(99),v=>v.request_id=id(99),v=>v.extra='authority',v=>v.required_quote_members=[{member_key:'x',family_id:id(5),work_event_id:id(6)},{member_key:'x',family_id:id(7),work_event_id:id(8)}]];
 for(const mutate of variants){let calls=0;const v=begin();mutate(v);await assert.rejects(previewSourcePayDecision({request:preview(),actorUserId:actor,calculateWeeklySnapshot:calculator,dataRpc:async()=>{calls++;return v;}}));assert.equal(calls,1);}
 const v=begin();v.required_quote_members=[{member_key:'x',family_id:id(5),work_event_id:id(6)}];
 assert.throws(()=>decodeSourcePreviewBegin(v,{decision:'ACCEPT_FINAL_SOURCE_PAY'}));
});
test('Positive confirmation creates no Unprotect payload and strict-decodes the complete publication list',async()=>{
 const calls=[];const result=await confirmSourcePayDecision({request:accept(),actorUserId:actor,calculateWeeklySnapshot:calculator,dataRpc:async(name,args)=>{
  calls.push([name,args]);if(name.endsWith('confirmation_prepare_v1'))return {version:'SOURCE_PAY_DECISION_CONFIRMATION_PREPARED_V1',decision_id:id(1),review_id:id(3),request_id:id(10),confirmation_reason:accept().reason,members:[]};
  if(name.endsWith('accept_v1'))return receipt();assert.fail(name);
 }});assert.deepEqual(result,receipt());assert.equal(calls.length,2);assert.deepEqual(calls[1][1].p_request.unprotect_preparations,[]);
 assert.equal(calls[1][1].p_request.preview_assembly_ref,null);assert.equal(result.financial_publications.commands[0].command_id,id(12));
});
test('Public accepted receipt refuses mutable wrapper fields, malformed native publication and false protection success',()=>{
 const mutations=[v=>v.current_financial_result={},v=>v.financial_publications.commands[0].member_count='1',v=>v.financial_publications.commands[0].extra=true,
  v=>v.read_result_url='https://elsewhere.invalid',v=>v.members[0].protection_release.status='RELEASED',v=>v.members[0].invoice_certificate.net='0.00',
  v=>v.members.push(structuredClone(v.members[0])),v=>v.members[0].next_actions=[{kind:'READ_RESULT',target_type:'SOURCE_PAY_DECISION',target_id:id(1),caption:'Check saved result',url:'//external',enabled:true,disabled_reason:null}]];
 for(const mutate of mutations){const v=receipt();mutate(v);assert.throws(()=>decodeSourcePayDecisionReceipt(v));}
});
test('Review rejects opaque quantity, artifact, action and invoice child values',()=>{
 const mutations=[v=>v.members[0].supported_quantities={opaque:true},v=>v.members[0].retained_commitments=[{paid:true}],v=>v.members[0].invoice_certificate.phase='UNKNOWN',
  v=>v.members[0].next_actions=[{}],v=>v.members[0].expected_work_revision=2,v=>v.members[0].actual_client_id=null,v=>v.members[0].work_dates=['2026-02-30']];
 for(const mutate of mutations){const v=review();mutate(v);assert.throws(()=>decodeSourcePayDecisionReview(v));}
});
test('UNRESOLVED invoice retains known IDs/hash and NULL amounts; PRE_FINANCIAL never acquires fabricated units',()=>{
 const v={...invoice(),phase:'UNRESOLVED',accepted_final_member_ids:[id(20)]};assert.equal(decodeSourceInvoiceCertificate(v),v);
 assert.throws(()=>decodeSourceQuantity({...quantity(),slices:[{amount:'0.00'}]},'WHOLE_SHIFT'));
 assert.throws(()=>decodeSourceQuantity({...quantity(),quantity_state:'PRE_FINANCIAL'},'COMPONENT_QUANTITIES'));
});
test('Abandon preserves the actual SEALED response, and browser authority fields fail before RPC',async()=>{
 let calls=0;assert.deepEqual(await abandonSourcePayDecisionPreview({assemblyId:id(7),actorUserId:actor,dataRpc:async()=>{calls++;return {version:'SOURCE_PAY_DECISION_PREVIEW_ABANDON_RESULT_V1',assembly_id:id(7),state:'SEALED'};}}),
 {version:'SOURCE_PAY_DECISION_PREVIEW_ABANDON_RESULT_V1',assembly_id:id(7),state:'SEALED'});assert.equal(calls,1);
 await assert.rejects(previewSourcePayDecision({request:{...preview(),actor_user_id:actor},actorUserId:actor,calculateWeeklySnapshot:calculator,dataRpc:async()=>{assert.fail();}}));
});

test('Protected PREVIEW composes actual run-free calculation and quote before READY/seal, without native mutation RPCs',async()=>{
 const calls=[];const root=id(30),contractId=id(31),familyId=id(32),eventId=id(33),financialId=id(34);
 const timesheet={timesheet_id:root,contract_id:contractId,version:1,is_current:true,archived_at_utc:null};
 const contract={id:contractId,candidate_id:id(5),client_id:id(6)};const financial={id:financialId};
 const context={action:'WITHDRAW',family_id:familyId,family_bound_version:'2',current_target_vector_sha256:hash,
  source_cycle_id:id(35),source_group_id:id(36),agency_id:id(37),candidate_id:id(5),client_id:id(6),contract_id:contractId,contract_week_id:id(38),
  root_timesheet_id:root,work_event_id:eventId,week_ending_date:'2026-10-18',source_mode:'HEALTHROSTER_WEEKLY',policy:{},timesheet,
  contract_week:{id:id(38)},contract_record:contract,current_financial:financial,approved_component_identity_components:[],
  requires_zero_financial:false,protected_schedule:{work_date:'2026-10-12',start_at_local:'2026-10-12 09:00:00',end_at_local:'2026-10-12 17:00:00',break_minutes:30},
  source_segments:[],protected_decisions:[{work_event_id:eventId,state:'ACCEPTED_SOURCE',fixed_schedule:null}],
  source_proposal:{selected_work_event_id:eventId,source_present:false,source_minutes:0,source_revision:null,source_hash:null,source_segments:[]},
  client_sources:[],source_expenses:[],candidate_submission:null,current_comparison_revision_id:null,current_final_revision_id:null,root_financial:{},provider:{},
  expected_head_revision:'0',request_sequence:'1',zero_rate_source_refs:{},next_source_segments:[],
  protected_current_placement:{version:'PROTECTED_CURRENT_PLACEMENT_V1',moved:false,family_id:familyId,family_root_id:root,member_key:root,
   original_root_timesheet_id:root,original_contract_id:contractId,current_timesheet_id:root,current_contract_id:contractId,current_financial_id:financialId,
   current_timesheet_version:1,current_approval_work_id:null,original_canonical_work_id:null,move_operation_id:null,placement_revision:'0',
   immutable_original_evidence_sha256:hash,source_invoice_certificate_sha256:hash,placement_certificate_sha256:hash},
  current_entitlement:{timesheet,contract_record:contract,current_financial:financial,current_approval_work_id:null}};
 const dataRpc=async(name,args)=>{
  calls.push([name,args]);
  if(name.endsWith('begin_v1'))return {...begin(),required_quote_members:[{member_key:member().member_key,family_id:familyId,work_event_id:eventId}]};
  if(name==='weekly_source_pay_decision_preview_quote_context_v1')return {version:'SOURCE_PAY_DECISION_PREVIEW_QUOTE_CONTEXT_RESULT_V1',
   review_context:{version:'SOURCE_UNPROTECT_REVIEW_CONTEXT_RESULT_V1',assembly_id:id(7),decision_id:id(1),member_key:member().member_key,
    source_phase:'NO_FINAL_REPORT_YET',source_observation_sha256:hash,context_sha256:hash,context},prior_commitment_cut_sha256:hash,
   quantity_partition:[{cut_id:id(8),cut_member_no:'1',member_sha256:hash,family_root_id:root,source_row_id:id(4),source_work_event_id:eventId,
    canonical_work_id:null,canonical_component_key:null,scope_kind:'SOURCE_ROW',scope_mode:'WHOLE_SHIFT',work_dates:['2026-10-12'],supported_quantity_json:quantity(),excluded_quantity_json:quantity()}]};
  if(name==='weekly_source_pay_decision_preview_quote_v1'){
   const {hashSourceNativeJson}=await import('../../broker/src/weekly-source/source-native-economic.mjs');
   return {version:'SOURCE_PAY_DECISION_PREVIEW_QUOTE_RESULT_V1',assembly_id:id(7),member_key:member().member_key,quote_id:id(39),
    quote_sha256:await hashSourceNativeJson('SOURCE_UNPROTECT_PREVIEW_QUOTE_V1',args.p_request.quote),economic_target_sha256:args.p_request.quote.economic_target_sha256};
  }
  if(name.endsWith('ready_v1'))return {version:'SOURCE_PAY_DECISION_PREVIEW_READY_RESULT_V1',assembly_id:id(7),assembly_sha256:hash};
  if(name==='weekly_source_pay_decision_preview_v1')return review();
  assert.fail('Unexpected native mutation '+name);
 };
 const calculateWeeklySnapshot=async args=>{calls.push(['calculator',args]);return {ok:true,snapshot:nativeSnapshotFixture({timesheet_id:root,candidate_id:id(5),client_id:id(6),
  contract_id:args.context.contract_id,policy:args.context.policy})};};
 assert.deepEqual(await previewSourcePayDecision({request:preview(),actorUserId:actor,dataRpc,calculateWeeklySnapshot}),review());
 assert.deepEqual(calls.map(x=>x[0]),['weekly_source_pay_decision_preview_begin_v1','weekly_source_pay_decision_preview_quote_context_v1','calculator',
  'weekly_source_pay_decision_preview_quote_v1','weekly_source_pay_decision_preview_ready_v1','weekly_source_pay_decision_preview_v1']);
 const quote=calls[3][1].p_request.quote;assert.deepEqual(quote.economic_target.component_economics,[]);
 assert.equal(Object.hasOwn(quote,'orchestration_run_id'),false);assert.equal(quote.economic_target.quantity_partition[0].cut_id,id(8));
});

test('Internal Source read preserves immutable accepted/request receipts separately and injects only the actual current reader',async()=>{
 const source={version:'SOURCE_PAY_DECISION_INTERNAL_SOURCE_READ_V1',decision_id:id(1),request_id:id(10),accepted_receipt:receipt(),request_receipt:receipt(),
  state_revision:'9007199254740994',current_cut:{cut_id:id(8),cut_no:'2',review_id:id(3),qualification_state:'NOT_APPROVED',pay_cut_sha256:hash,source_invoice_sha256:hash,issue_set_sha256:hash},
  applications:[{application_id:id(50),request_id:id(10),review_id:id(3),cut_id:id(8),state:'COMMITTED',application_sha256:hash,completed_at_utc:'2026-10-16T17:00:00Z'}],native_members:[]};
 let sent;const result=await readSourcePayDecisionSource({request:{version:'SOURCE_PAY_DECISION_V1',action:'READ_RESULT',decision_id:id(1),request_id:id(10)},
  actorUserId:id(99),dataRpc:async(name,args)=>{assert.equal(name,'weekly_source_pay_decision_read_source_v1');sent=args;return source;}});
 assert.equal(sent.p_request.actor_user_id,id(99));assert.equal(result.accepted_receipt.actor_user_id,actor);assert.deepEqual(result.accepted_receipt,receipt());
 for(const mutate of [v=>v.current_cut.current_bank_json={},v=>v.applications[0].state='STAGED',v=>v.current_cut.cut_no=2,v=>v.request_receipt.request_id=id(98),
  v=>v.native_members=[{opaque:'bank JSON'}],v=>v.current_cut.cut_id=id(98)]){
  const v=structuredClone(source);mutate(v);assert.throws(()=>decodeSourcePayDecisionSourceRead(v));
 }
});
