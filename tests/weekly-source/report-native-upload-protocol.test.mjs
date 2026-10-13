// Executable offline protocol checks, not PostgreSQL/real native READY proof.
import test from 'node:test';
import assert from 'node:assert/strict';
import * as XLSX from 'xlsx';
import { parseWeeklySourceFile } from '../../broker/src/weekly-source/index.js';
import { adaptWeeklySourceParserOutput } from '../../broker/src/weekly-source/upload-staging-adapter.mjs';
import { createWeeklySourceUploadPublicationOwner, buildWeeklySourceProjectionRows } from '../../broker/src/weekly-source/upload-publication-owner.mjs';
import { REPORT_PROFILE, reportScopeRequest, selfBillReportRows, assertReportUploadRequest } from '../../broker/src/weekly-source/report-upload-provenance.mjs';
const id=n=>`20000000-0000-4000-8000-${String(n).padStart(12,'0')}`;
const I={actor:id(1),agency:id(2),group:id(3),cycle:id(4),client:id(5),scope:id(6),report:id(7),upload:id(8),publication:id(9),assignment:id(10),version:id(11)};
const H='a'.repeat(64),C='b'.repeat(64),Q='c'.repeat(64);
const values={invoice_sender:'CLIENT_SELF_BILL',self_bill_source:'SEPARATE_FINAL_REPORT',final_report_profile_kind:REPORT_PROFILE,
  invoice_membership:'FOLLOW_FINAL_REPORT_LINES',invoice_amount_basis:'SOURCE_AMOUNT',source_amount_basis:'REPORT_LINE_NET',
  line_vat_method:'LINE_VAT_FROM_REPORT',pay_membership:'ON_ACCEPTED_REPORT_OR_PROTECTED',invoice_vat_method:'ONCE_ON_TOTAL_HALF_UP'};
const context={ok:true,environment:'TEST',agency_id:I.agency,source_family:'ROSTER',source_group_id:I.group,
  source_cycle_id:I.cycle,client_id:I.client,client_name:'Synthetic Client',report_scope_id:null,authority_scope_kind:'CYCLE',authority_scope_version:1,
  effective_rule_set:{contract:'WEEKLY_SOURCE_EFFECTIVE_RULE_SET_V1',source_group_id:I.group,assignment_id:I.assignment,
    assignment_version:1,rule_set_version_id:I.version,values_sha256:H,values}};
const body={source_group_id:I.group,source_cycle_id:I.cycle,client_id:I.client,profile_id:REPORT_PROFILE,filename:'synthetic.xlsx'};
function bytes(layout='A',{overnight=false,negative=false,mismatch=false}={}){
  const common=['Request','Date','Start','End','Staff','Requested On','Invoicenumber','Invoicedate'];
  const money=layout==='A'?['Net','VAT','Total','MSP Fee','MSPVAT']:['Actual Cost','Net to Agency','VAT to Agency','Total','MSP Fee','ABI Fee'];
  const signs=negative?-1:1;
  const row=['000123','2025-04-01',overnight?'22:00':'08:00',overnight?'06:00':'16:00','Synthetic worker','2025-04-03','XYOXLR030425','03/04/25',
    ...(layout==='A'?[signs*222.31,signs*44.462,signs*266.772,'ignored','ignored']:['999.99',String(signs*263.4),String(signs*52.68),mismatch?'999.99':String(signs*316.08),'ignored','ignored'])];
  const book=XLSX.utils.book_new();XLSX.utils.book_append_sheet(book,XLSX.utils.aoa_to_sheet([[...common,...money],row]),'Allocate');
  return XLSX.write(book,{type:'buffer',bookType:'xlsx'});
}
async function parsed(layout='A',options={}){return parseWeeklySourceFile(bytes(layout,options),{profileId:REPORT_PROFILE,expectedClient:context.client_name,
  effectiveRuleSet:context.effective_rule_set,ruleValues:{...values,invoice_vat_method:layout==='B'?'ONCE_ON_TOTAL_TRUNCATE':values.invoice_vat_method}});}
function scoped(){return {...context,report_scope_id:I.scope,self_bill_report_id:I.report,authority_scope_kind:'SELF_BILL_REPORT_SCOPE'};}
function buildContext(state='BUILDING') {return {...scoped(),profile_id:REPORT_PROFILE,upload_id:I.upload,invoice_only:true,rows:[],
  projection_state:state,projection_publication_id:I.publication,row_manifest_hash:H,comparison_manifest_hash:C,issue_set_hash:Q};}
test('actual parser A literal amounts/leading zeros map to the native closed SQL row DTO',async()=>{
  const p=await parsed(),request=reportScopeRequest(p,context,I.actor),row=request.rows[0];
  assert.equal(row.external_source_key,'000123');assert.equal(row.source_shift_charge_pence,'22231');
  assert.equal(row.source_vat_pence,'4446');assert.equal(row.source_gross_pence,'26677');
  assert.deepEqual(Object.keys(row).sort(),['source_row_ordinal','external_source_key','source_candidate_identity','source_client_identity','work_date',
    'start_at_local','end_at_local','source_shift_charge_pence','source_vat_pence','source_gross_pence','bounded_raw_columns_json'].sort());
  assert.equal(row.bounded_raw_columns_json.components.net.sourceKind,'XLSX_NUMERIC_TOKEN');
  assert.equal(Object.hasOwn(row,'actual_net_minutes'),false);assert.equal(Object.hasOwn(row,'break_minutes'),false);
});
test('overnight report clocks are retained without computing worked hours/pay',async()=>{
  const p=await parsed('A',{overnight:true}),a=adaptWeeklySourceParserOutput(p,{...body,actor_user_id:I.actor},scoped());
  assert.equal(a.normalisedRows[0].start_at_local,'2025-04-01T22:00:00');assert.equal(a.normalisedRows[0].end_at_local,'2025-04-02T06:00:00');
  for(const field of ['break_minutes','actual_net_minutes','finalised_by','role_band_source','source_expense_pence']) assert.equal(a.normalisedRows[0][field],null);
  assert.equal(a.normalisedRows[0].row_finalisation_state,'SOURCE_REPORT_LINE');assert.deepEqual(a.expenseEvidence,[]);
  assert.equal(a.beginRequest.coverage_proof_kind,'SELF_BILL_REPORT_SCOPE');assert.equal(a.beginRequest.confirmed_coverage_start_local_date,null);
  assert.equal(a.beginRequest.import_use,undefined);assert.equal(a.beginRequest.file_metadata_json.import_use,'PREPARE_FINALISATION');
  assert.equal(a.beginRequest.money_lexical_authority_version,'SOURCE_INVOICE_MONEY_PENCE_V1');
});
test('B literal gross mismatch remains forensic warning and the adapter does not repair it',async()=>{
  const p=await parsed('B',{mismatch:true});assert.equal(p.ok,true);assert.ok(p.warnings.some(w=>w.code==='SELF_BILL_REPORT_ARITHMETIC_WARNING'));
  const row=selfBillReportRows(p,{...context,effective_rule_set:{...context.effective_rule_set,values:{...values,invoice_vat_method:'ONCE_ON_TOTAL_TRUNCATE'}}})[0];assert.equal(row.source_gross_pence,'99999');assert.equal(row.source_shift_charge_pence,'26340');
  assert.notEqual(BigInt(row.source_gross_pence),BigInt(row.source_shift_charge_pence)+BigInt(row.source_vat_pence));
});
test('negative literal Report money is signed while all staged pay facts remain absent',async()=>{
  const p=await parsed('A',{negative:true}),a=adaptWeeklySourceParserOutput(p,{...body,actor_user_id:I.actor},scoped());
  assert.equal(a.normalisedRows[0].source_shift_charge_pence,'-22231');assert.equal(a.normalisedRows[0].actual_net_minutes,null);
  assert.deepEqual(a.moneyEvidence.map(e=>e.money_field_kind),['SOURCE_NET','SOURCE_VAT','SOURCE_GROSS']);
  assert.deepEqual(a.moneyEvidence.map(e=>e.parsed_pence),['-22231','-4446','-26677']);
});
test('closed Report payload rejects browser actor/economics/feature/rule/target/coverage injection',()=>{
  for(const field of ['actor_user_id','finance','rows','report_number','invoice_date','self_bill_report_id','invoice_only','feature_enabled','effective_rule_set',
    'rule_set_version_id','contract_selections','target_timesheet_id','coverage','canonical_pay_vector_json']) {
    assert.throws(()=>assertReportUploadRequest({...body,[field]:{}}),e=>e.code==='WEEKLY_SOURCE_REPORT_REQUEST_UNKNOWN_FIELD');
  }
  assert.throws(()=>assertReportUploadRequest({...body,parser_options:{profileId:REPORT_PROFILE,ruleValues:values}}),e=>e.code==='WEEKLY_SOURCE_REPORT_PARSER_OPTIONS_FORBIDDEN');
  assert.throws(()=>assertReportUploadRequest({...body,purpose:'FINAL_SOURCE_CORRECTION'}),e=>e.code==='WEEKLY_SOURCE_SELF_BILL_REPORT_CORRECT_FINAL_UNSUPPORTED');
});
test('actual effective source assignment is required; empty/foreign/disabled truth cannot fabricate readiness',async()=>{
  const p=await parsed();
  for(const c of [{...context,effective_rule_set:null},{...context,effective_rule_set:{...context.effective_rule_set,source_group_id:id(99)}},
    {...context,effective_rule_set:{...context.effective_rule_set,values:{...values,self_bill_source:'SAME_FILE'}}}]) {
    assert.throws(()=>selfBillReportRows(p,c),e=>e.code==='WEEKLY_SOURCE_SELF_BILL_REPORT_RULES_NOT_ENABLED');
  }
});
test('source component/row/manifest tampering refuses before any staging',async()=>{
  for(const mutate of [p=>p.report.netPence='1',p=>p.rows[0].pricingEvidence.components.net.parsedPence='1',
    p=>p.rows[0].pricingEvidence.components.net.coordinate='J9',p=>p.rows[0].actual={start:'09:00'},
    p=>p.rows[0].requestId=123,p=>p.rows[0].reportTimes.overnight=true]) {
    const p=await parsed();mutate(p);assert.throws(()=>selfBillReportRows(p,context));
  }
});
test('native Report projection response requires empty rows and never invokes contract/price calculation',()=>{
  assert.deepEqual(buildWeeklySourceProjectionRows(buildContext()),[]);
  assert.throws(()=>buildWeeklySourceProjectionRows({...buildContext(),rows:[{actual_net_minutes:480,contracts:[]}]}),
    e=>e.code==='WEEKLY_SOURCE_REPORT_PUBLICATION_CONTEXT_INVALID');
});
function transport({failAt=null,drift=false,returnedFailure=false}={}) {
  const calls=[];let registered=false,published=false;
  const rpc=async(name,args)=>{
    calls.push([name,args]);if(name===failAt)throw new Error('OFFLINE_TRANSPORT_FAILURE');
    if(name==='weekly_source_upload_context_v1') {
      if(args.p_request.operation==='BUILD_PROJECTION')return buildContext(published?'CURRENT':'BUILDING');
      const c=registered?scoped():context;return drift&&registered?{...c,effective_rule_set:{...c.effective_rule_set,values_sha256:Q}}:c;
    }
    if(name==='weekly_source_self_bill_report_scope_resolve_atomic_v1'){
      registered=true;return {ok:true,contract:'WEEKLY_SOURCE_SELF_BILL_REPORT_SCOPE_V1',report_scope_id:I.scope,self_bill_report_id:I.report,
        source_group_id:I.group,source_cycle_id:I.cycle,client_id:I.client,client_name:context.client_name,
        authority_scope_version:1,cutoff_at_utc:'2026-10-11T16:00:00Z',row_manifest_hash:H,idempotent:false};
    }
    if(name==='weekly_source_upload_stage_begin_atomic_v1')return {ok:true,status:'STAGING',upload_id:I.upload};
    if(name==='weekly_source_upload_stage_rows_atomic_v1')return {ok:!returnedFailure,status:returnedFailure?'REFUSED':'STAGING',upload_id:I.upload,
      inserted:{physical_rows:2,normalised_rows:1,money_evidence:3,expense_evidence:0},stored:{physical_rows:2,normalised_rows:1,money_evidence:3,expense_evidence:0}};
    if(name==='weekly_source_upload_seal_atomic_v1')return {ok:true,status:'CURRENT',upload_id:I.upload,authority_scope_version:1,row_manifest_hash:H};
    if(name==='weekly_source_projection_begin_atomic_v1')return {ok:true,status:'BUILDING',publication_id:I.publication};
    if(name==='weekly_source_projection_rows_apply_atomic_v1')return {ok:true,status:'APPLIED',publication_id:I.publication,rows_applied:0,expected_rows:0,invoice_only:true};
    if(name==='weekly_source_projection_publish_atomic_v1'){published=true;return {ok:true,status:'CURRENT',publication_id:I.publication};}
    throw new Error('UNEXPECTED_RPC '+name);
  };
  return {calls,rpc};
}
test('offline sequencing covers real parser→scope manifest→stage→seal→empty projection→CURRENT proof',async()=>{
  const stub=transport(),owner=createWeeklySourceUploadPublicationOwner({rpc:stub.rpc});
  const result=await owner.acceptUpload({body,bytes:bytes(),actor:{id:I.actor},parseWeeklySourceFile});
  assert.equal(result.invoice_only,true);assert.equal(result.row_manifest_hash,H);assert.equal(result.authority_scope_kind,'SELF_BILL_REPORT_SCOPE');
  assert.deepEqual(stub.calls.map(([n])=>n),['weekly_source_upload_context_v1','weekly_source_self_bill_report_scope_resolve_atomic_v1',
    'weekly_source_upload_context_v1','weekly_source_upload_stage_begin_atomic_v1','weekly_source_upload_stage_rows_atomic_v1',
    'weekly_source_upload_seal_atomic_v1','weekly_source_projection_begin_atomic_v1','weekly_source_upload_context_v1',
    'weekly_source_projection_rows_apply_atomic_v1','weekly_source_projection_publish_atomic_v1','weekly_source_upload_context_v1']);
  const scope=stub.calls[1][1].p_request,stage=stub.calls[4][1].p_request;
  assert.equal(scope.actor_user_id,I.actor);assert.equal(scope.rows[0].external_source_key,'000123');assert.equal(stage.normalised_rows[0].actual_net_minutes,null);
  assert.deepEqual(stub.calls[8][1].p_rows,[]);assert.equal(stub.calls.some(([n])=>/pay_|authoris|tsfin|email|claim/.test(n)),false);
});
test('source authority drift or failed native phases cannot reach accepted publication',async()=>{
  for(const options of [{drift:true},{returnedFailure:true},{failAt:'weekly_source_upload_stage_rows_atomic_v1'},{failAt:'weekly_source_projection_rows_apply_atomic_v1'}]) {
    const stub=transport(options),owner=createWeeklySourceUploadPublicationOwner({rpc:stub.rpc});
    await assert.rejects(owner.acceptUpload({body,bytes:bytes(),actor:{id:I.actor},parseWeeklySourceFile}));
    assert.equal(stub.calls.some(([n])=>n==='weekly_source_projection_publish_atomic_v1'),false);
  }
});
