import assert from 'node:assert/strict';
import test from 'node:test';
import { decodeNextPaidEvidencePage, nextPaidSchedule, nextOriginalCashObligations, applyNextPaidEvidence }
  from '../../broker/src/weekly-source/next-paid-evidence.mjs';
const id=(n)=>`81000000-0000-4000-8000-${String(n).padStart(12,'0')}`;
const request=()=>({version:'SOURCE_PAID_EVIDENCE_V1',actor_user_id:id(1),root_timesheet_id:id(2),
  work_id:id(3),expected_revision_id:id(4),kind:'COMPONENTS',context_id:null,after:null,limit:100});
const originalCash=()=>({transfer_id:id(20),transfer_no:'1',scope:'WHOLE_ORIGINAL_TRANSFER',
  original_cash_amount:'160.00',original_settlement_posting_complete:true,
  original_return_command_id:id(21),return_posting_complete:true,return_cash_id:id(22),
  cash_owed:'160.00',cash_held:'0.00',cash_repaid:'0.00',cash_available:'160.00',
  pending_reissue_command_id:null,action_state:'CASH_AVAILABLE'});
const component=()=>({component_key:'worked-time:monday',source_component_id:id(5),detail_kind:'WORK',
  quantity_subject:true,applied_revision_id:id(4),collection_id:id(6),qualification_state:'READY',
  approved_source_ex_vat:'140.00',realised_source_disposition_ex_vat:'140.00',active_source_hold_ex_vat:'0.00',
  actual_recovered_source_ex_vat:'20.00',written_off_source_ex_vat:'0.00',active_recovery_hold_source_ex_vat:'0.00',
  original_effect_id:id(7),original_transfer_id:id(20),original_run_worker_id:id(8),original_positive_source_ex_vat:'160.00',
  only_original_positive_payroll:true,paid_quantity_certificate:'ORIGINAL_RETURN_ZERO',paid_quantity:'0',reason:null,
  original_cash:originalCash()});
function page(){return {version:'SOURCE_PAID_EVIDENCE_V1',ok:true,kind:'COMPONENTS',
  header:{root_timesheet_id:id(2),work_id:id(3),expected_revision_id:id(4),module_epoch:'1',
    current_origin_state:'APPLIED',financial_view_revision:'99',scope:'EXACT_SOURCE_WORK',
    quantity_authority_scope:'CURRENT',context:null},rows:[component()],complete:true,next_cursor:null,
  quantity_certificate:{scope:'EXACT_SOURCE_WORK',state:'ROOT_RETURN_ZERO',quantity:'0',unit:'HOURS',
    reason:null,component_set_complete:true},activity_coverage:{scope:'COMPONENT_ORIGINALS',complete_for_scope:true,
    page_row_count:'1',root_preparing_count:null,root_frozen_draft_count:null,root_inflight_count:null,
    reason:'ROOT_ACTIVITY_NOT_INDEXED'}};}
const invalid=(value,req=request())=>assert.throws(()=>decodeNextPaidEvidencePage(value,req),
  {code:'WEEKLY_SOURCE_NEXT_PAID_CONTRACT_INVALID'});
test('qualified aggregate zero stays separate from unknown activity and carries no invented shift/bucket/history',()=>{
  const value=page();
  assert.equal(decodeNextPaidEvidencePage(value,request()),value);
  assert.deepEqual(nextPaidSchedule(value,request()),{available:true,reason:null,
    source:'NEXT_ROOT_RETURN_ZERO',total_hours:'0',row_count:0,rows:[]});
  assert.equal(value.activity_coverage.root_inflight_count,null);
});
test('closed identity, nullable types, page bounds and cursor completeness reject invalid reads',()=>{
  for(const mutate of [v=>v.extra=true,v=>v.header.extra=true,v=>v.header.work_id=id(77),
    v=>v.header.module_epoch=1,v=>v.header.financial_view_revision=-1,
    v=>v.rows[0].approved_source_ex_vat=140,v=>v.rows[0].extra=true,
    v=>v.activity_coverage.root_inflight_count=0,v=>v.activity_coverage.page_row_count='2',
    v=>v.complete=false,v=>v.next_cursor={after_component_key:'x'},
    v=>v.rows[0].original_cash.cash_owed='Infinity',v=>v.rows[0].original_cash.transfer_id=id(30)]){
    const value=page();mutate(value);invalid(value);
  }
  invalid(page(),{...request(),limit:0});
  invalid(page(),{...request(),context_id:id(8)});
});
test('pending/historical/multipage/empty and missing positive originals never qualify current root zero',()=>{
  for(const mutate of [v=>v.header.current_origin_state='PENDING',
    v=>v.header.quantity_authority_scope='HISTORICAL_ONLY',v=>v.header.quantity_authority_scope='NONE',
    v=>v.quantity_certificate.component_set_complete=false,v=>v.rows[0].quantity_subject=null,
    v=>v.rows[0].original_positive_source_ex_vat='0',v=>v.rows[0].only_original_positive_payroll=false,
    v=>v.rows[0].original_cash.return_posting_complete=false,v=>v.rows[0].original_cash.cash_held='1',
    v=>v.rows[0].original_cash.pending_reissue_command_id=id(30),
    v=>{v.rows=[];v.activity_coverage.page_row_count='0';}]){
    const value=page();mutate(value);invalid(value);
  }
  invalid(page(),{...request(),after:{after_component_key:'earlier'}});
});
test('actual withholding stays unavailable rather than deriving nonzero or zero hours from money',()=>{
  const value=page();
  value.quantity_certificate={scope:'NONE',state:'POSITION_WITHHELD',quantity:null,unit:'HOURS',
    reason:'ADDITIONAL_POSITIVE_PAYROLL',component_set_complete:true};
  value.rows[0].paid_quantity_certificate='POSITION_WITHHELD';value.rows[0].paid_quantity=null;
  value.rows[0].reason='ADDITIONAL_POSITIVE_PAYROLL';value.rows[0].only_original_positive_payroll=false;
  const schedule=nextPaidSchedule(value,request());
  assert.equal(schedule.available,false);assert.equal(schedule.reason,'ADDITIONAL_POSITIVE_PAYROLL');
  assert(!Object.hasOwn(schedule,'total_hours'));assert(!Object.hasOwn(schedule,'hours_by_bucket'));
});
test('whole original cash is deduplicated with equality, not summed or allocated to each component',()=>{
  const value=page();value.rows.push({...component(),component_key:'worked-time:wednesday'});
  value.activity_coverage.page_row_count='2';
  assert.deepEqual(nextOriginalCashObligations(value,request()),[{cash:originalCash(),
    component_keys:['worked-time:monday','worked-time:wednesday']}]);
  value.rows[1].original_cash.cash_owed='159.00';
  assert.throws(()=>nextOriginalCashObligations(value,request()),{code:'WEEKLY_SOURCE_NEXT_PAID_CONTRACT_INVALID'});
});
test('bounded incomplete component pages remain usable without assembling a root certificate',()=>{
  const value=page();value.complete=false;value.next_cursor={after_component_key:'worked-time:monday'};
  value.quantity_certificate={scope:'NONE',state:'POSITION_WITHHELD',quantity:null,unit:'HOURS',
    reason:'COMPONENT_SET_NOT_ONE_SNAPSHOT',component_set_complete:false};
  value.activity_coverage.complete_for_scope=false;
  assert.equal(nextPaidSchedule(value,request()).available,false);
});

test('worker transfer reads retain actual INTERNAL_ZERO states without claiming bank payment',()=>{
  const req={...request(),kind:'WORKER_TRANSFERS',context_id:id(8)};
  const value=page();value.kind=req.kind;
  value.header.context={run_id:id(40),run_worker_id:id(8),run_status:'EXECUTING',worker_status:'ISSUED',
    preparation_created_at_utc:'2026-10-05T12:00:00.000000Z',preparation_deadline_utc:null,
    confirmed_at_utc:'2026-10-05T12:01:00.000000Z',captured_line_count:'1',captured_position_count:'1',
    realised_effect_count:null,active_case_hold_count:null};
  value.quantity_certificate={scope:'NONE',state:'POSITION_WITHHELD',quantity:null,unit:'HOURS',
    reason:'QUANTITY_MAPPING_UNSUPPORTED',component_set_complete:false};
  value.activity_coverage.scope='EXACT_RUN_WORKER';
  const transfer={transfer_id:id(20),transfer_no:'1',original_transfer_id:null,return_cash_id:null,
    execution_kind:'INTERNAL_ZERO',status:'INTERNAL_PROCESSING',cash_scope:'WHOLE_WORKER_LEG',cash_amount:'0.00',
    outcome_posting_complete:null,return_posting_complete:null,internal_posting_complete:false,reissue_action_state:null};
  value.rows=[transfer];
  for(const status of ['INTERNAL_PROCESSING','INTERNAL_SETTLED']) {
    transfer.status=status;assert.equal(decodeNextPaidEvidencePage(value,req),value);
  }
  transfer.execution_kind='BANK';transfer.cash_amount='10.00';invalid(value,req);
  transfer.execution_kind='INTERNAL_ZERO';transfer.status='SETTLED';transfer.cash_amount='0.00';invalid(value,req);
  transfer.status='INTERNAL_SETTLED';transfer.cash_amount='10.00';invalid(value,req);
});

function officePresentation() {
  const components=page();
  const holds=page();holds.kind='ACTIVE_HOLDS';holds.rows=[];
  holds.quantity_certificate={scope:'NONE',state:'POSITION_WITHHELD',quantity:null,unit:'HOURS',
    reason:'COMPONENT_SET_NOT_ONE_SNAPSHOT',component_set_complete:false};
  holds.activity_coverage.scope='ACTIVE_HOLDS_ONLY';holds.activity_coverage.page_row_count='0';
  const wrapper=(p)=>({contract:'WEEKLY_SOURCE_OFFICE_NEXT_PAGE_V1',available:true,reason:null,
    request:{...request(),kind:p.kind},page:p});
  return {record_version:'unchanged-source-command-authority',action_state:{authorise_allowed:true},
    lifecycle:{ok:false,server_phase:null,permitted_actions:[],schedules:{approved:{available:true,source:'ORIGINAL_I7'}}},
    next_paid_evidence:{contract:'WEEKLY_SOURCE_OFFICE_NEXT_EVIDENCE_V1',components:wrapper(components),active_holds:wrapper(holds)}};
}

test('Office NEXT consumer preserves command authority and independent I7 while fingerprinting only bounded information',async()=>{
  const supplied=officePresentation();const before=JSON.stringify(supplied);
  const first=await applyNextPaidEvidence(supplied,id(1),id(2));
  assert.equal(JSON.stringify(supplied),before);
  assert.equal(first.record_version,supplied.record_version);
  assert.deepEqual(first.action_state,supplied.action_state);
  assert.equal(first.lifecycle.schedules.approved,supplied.lifecycle.schedules.approved);
  assert.equal(first.lifecycle.ok,false);assert.equal(first.lifecycle.server_phase,null);
  assert.deepEqual(first.lifecycle.permitted_actions,[]);
  assert.equal(first.lifecycle.schedules.current_paid.total_hours,'0');
  assert.equal(first.lifecycle.schedules.processing.available,false);
  assert(!Object.hasOwn(first.lifecycle.schedules.processing,'total_hours'));
  assert.match(first.next_paid_information.read_fingerprint,/^[0-9a-f]{64}$/);
  supplied.next_paid_evidence.components.page.rows[0].original_cash.cash_owed='159.00';
  const later=await applyNextPaidEvidence(supplied,id(1),id(2));
  assert.notEqual(later.next_paid_information.read_fingerprint,first.next_paid_information.read_fingerprint);
  assert.equal(later.record_version,first.record_version);
  const legacy={lifecycle:{ok:true}};assert.equal(await applyNextPaidEvidence(legacy,id(1),id(2)),legacy);
});

test('Office contextual pages reject another actor/root, mismatched snapshot, extra fields and later cursors',async()=>{
  for(const mutate of [p=>p.next_paid_evidence.extra=true,
    p=>p.next_paid_evidence.components.request.actor_user_id=id(99),
    p=>p.next_paid_evidence.components.request.root_timesheet_id=id(99),
    p=>p.next_paid_evidence.active_holds.page.header.module_epoch='2',
    p=>p.next_paid_evidence.active_holds.page.header.financial_view_revision='100',
    p=>p.next_paid_evidence.active_holds.request.after={after_component_key:'x',after_hold_id:id(50)}]) {
    const supplied=officePresentation();mutate(supplied);
    await assert.rejects(applyNextPaidEvidence(supplied,id(1),id(2)),{code:'WEEKLY_SOURCE_NEXT_PAID_CONTRACT_INVALID'});
  }
});

test('known missing informational reader stays a bounded unavailable leaf without changing authorise',async()=>{
  const supplied=officePresentation();
  supplied.next_paid_evidence.components={contract:'WEEKLY_SOURCE_OFFICE_NEXT_PAGE_V1',available:false,
    reason:'NEXT_READER_UNAVAILABLE',request:null,page:null};
  const result=await applyNextPaidEvidence(supplied,id(1),id(2));
  assert.equal(result.action_state.authorise_allowed,true);
  assert.equal(result.lifecycle.schedules.current_paid.available,false);
  assert(!Object.hasOwn(result.lifecycle.schedules.current_paid,'total_hours'));
  assert.match(result.lifecycle.schedules.current_paid.reason_detail,/temporarily unavailable/);
});
