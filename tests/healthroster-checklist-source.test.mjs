import test from 'node:test';
import assert from 'node:assert/strict';
import {
  ChecklistSourceError, decodeChecklistHold, unavailableChecklistHold, projectChecklistItem,
  projectChecklistRow, createHealthRosterChecklistSourceReader, createChecklistSession
} from '../broker/src/weekly-source/healthroster-checklist-source.mjs';
const id = n => `00000000-0000-4000-8000-${String(n).padStart(12,'0')}`;
const scope = { source_group_id:id(1),client_id:id(2),source_cycle_id:id(3),week_ending:'2026-10-11' };
const actor = { actor_user_id:id(4),permission_view:'ADMIN',active:true };
const payActor = { ...actor,permission_view:'PAY_READONLY' };
const H = 'a'.repeat(64), R = 'b'.repeat(64);
const copy = value => JSON.parse(JSON.stringify(value));
const nativeStatus = (text='Nothing outstanding',tone='positive',subtext='') => ({text,tone,subtext,subtone:tone});
const zeroHold = () => ({holds_pay:false,holds_pay_scope:null,withheld_amount_pence:'0',holds_invoice:false,pending_decision:false,
  authority:{owner:'NEXT',hash:H,withholding_known:true}});
const cell = (value=true) => ({value,status:nativeStatus(),item_count:0,link:null});
function row(n=1) { return {row_id:`client-week:${id(n+20)}`,scope:copy(scope),client:'Client',group:'North · signed Timesheets',
  route:'SIGNED_TIMESHEETS',retention:{actual_timesheet_count:2,most_recent_ended_week:'2026-10-11',has_open_items:false,earlier_open_weeks:0},
  checked:cell(),completed:cell(),pay_completed:cell(),hold:zeroHold(),permitted_pay_item_count:1,admin_actions:[]}; }
function item(n=1) { return {item_id:`timesheet:${id(n+30)}`,item_kind:'ACTUAL_TIMESHEET',route:'SIGNED_TIMESHEETS',scope:copy(scope),worker:'Worker',work_date:'2026-10-07',sheet_scope:'WEEKLY',
  reason_code:'SIGNED_NOT_AUTHORISED',status:nativeStatus('Timesheet not authorised','danger'),hold:{...zeroHold(),holds_pay:true,
    holds_pay_scope:'TIMESHEET',withheld_amount_pence:'12500',holds_invoice:true},subject:'PAY_AND_INVOICE',permitted_pay_subject:true,
  action:{kind:'TIMESHEET',label:'Authorise in Timesheet ›',target:{timesheet_id:id(n+30)},native_allowed:true},
  counted_timesheet_id:id(n+30),correction_binding:null}; }
function frame(overrides={}) { return {contract:'WEEKLY_SOURCE_CHECKLIST_SOURCE_FRAME_V1',actor_user_id:actor.actor_user_id,
  permission_view:actor.permission_view,binding:H,read_revision:R,rows:[row()],items:[],has_more:false,next_cursor:null,
  refresh_required:false,anchor:null,native_preservation:false,...overrides}; }
function harness(options={}) {
  const calls=[]; const native={ok:true,rows:[],native_marker:'unchanged'};
  const reader=createHealthRosterChecklistSourceReader({
    assertActor:async()=>{calls.push('actor');return options.actor??actor;},
    readNativePage:async request=>{calls.push(['native',copy(request)]);return native;},
    loadAuthoritativeEvidence:async input=>{calls.push(['evidence',input.request.actor_user_id]);return options.frame??frame();},
    readNativeTimesheetPresentation:async input=>{calls.push(['timesheet',input.timesheetId]);return {ok:true,requested_timesheet_id:input.timesheetId,
      actor_user_id:actor.actor_user_id,root_timesheet_id:id(90),presentation:{lifecycle:{processing_status:'COMPLETE'},next_paid_evidence:{opaque:true}}};},
    applyNextPaidEvidence:async(presentation,actorId,rootId)=>{calls.push(['existing-next-decoder',actorId,rootId]);return {
      ...presentation,lifecycle:{...presentation.lifecycle,schedules:{paid_to_date:{available:false,reason:'POSITION_WITHHELD',rows:[]}}}};},
    ...options.dependencies
  }); return {reader,calls,native};
}
const request = () => ({command:'CHECKLIST',sort:'week_ending',direction:'DESC',limit:50});
const rejects=(fn,code)=>assert.rejects(fn,e=>e instanceof ChecklistSourceError&&e.code===code);

test('withheld unknown stays null, and false requires exact actual known zero evidence',()=>{
  const unknown=decodeChecklistHold(unavailableChecklistHold()); assert.equal(unknown.holds_pay,null); assert.equal(unknown.withheld_amount_pence,null); assert.equal(unknown.pending_decision,null);
  assert.throws(()=>decodeChecklistHold({...unavailableChecklistHold(),pending_decision:true}),/EVIDENCE_INVALID/);
  for(const h of [{...zeroHold(),withheld_amount_pence:null},{...zeroHold(),withheld_amount_pence:'125'},
    {...zeroHold(),authority:{owner:'UNAVAILABLE',hash:null,withholding_known:false}}]) {
    assert.throws(()=>decodeChecklistHold(h),ChecklistSourceError);
  }
  assert.equal(decodeChecklistHold(zeroHold()).holds_pay,false);
  assert.throws(()=>decodeChecklistHold({...zeroHold(),holds_pay:true,holds_pay_scope:'SHIFT'}),/CONTRADICTION/);
  assert.throws(()=>decodeChecklistHold({...unavailableChecklistHold(),holds_invoice:false,pending_decision:false}),/EVIDENCE_INVALID/);
});

test('unknown item is amber Decision needed without invented zero or pay-hold false',()=>{
  const i=item();i.hold=unavailableChecklistHold();i.status={text:'Ready',tone:'future-state'};
  const result=projectChecklistItem(i,actor);
  assert.equal(result.status.text,'Decision needed');assert.equal(result.status.tone,'warning');
  assert.equal(result.hold.holds_pay,null);assert.equal(result.hold.withheld_amount_pence,null);
});

test('pay authoriser sees permitted alignment/paired/return subjects as text despite invoice hold, hides invoice-only subject independently of badge',()=>{
  const alignment=item();alignment.reason_code='RECORDS_NOT_ALIGNED';alignment.hold={...zeroHold(),holds_invoice:true};
  alignment.action.label='Align and re-authorise in Timesheet ›';
  const result=projectChecklistItem(alignment,payActor);assert.ok(result);assert.equal(result.action,null);assert.equal(result.hold.holds_invoice,true);
  for(const reason of ['CORRECTION_PAIR_WAITING','SAME_DRAFT_RETURN_PENDING']) {
    const i=item();i.reason_code=reason;i.hold={...zeroHold(),holds_invoice:true};assert.ok(projectChecklistItem(i,payActor));
  }
  const invoice=item();invoice.subject='INVOICE_ONLY';invoice.permitted_pay_subject=false;invoice.hold=zeroHold();
  assert.equal(projectChecklistItem(invoice,payActor),null);
});

test('No one worked/Undo are admin-only native descriptors; reduced Checklist has no action links',()=>{
  const r=row();r.route='SELF_BILL';r.admin_actions=[{kind:'UNDO_NO_ONE_WORKED',label:'Undo',native_allowed:true,
    payload:{...scope,expected_cycle_version:3,completion_id:id(90)}}];delete r.admin_actions[0].payload.week_ending;
  r.checked.link={kind:'UPLOAD_DETAIL',label:'View upload',target:{upload_id:id(91)},native_allowed:true};
  assert.equal(projectChecklistRow(r,actor).admin_actions.length,1);
  const reduced=projectChecklistRow(r,payActor);
  assert.deepEqual(reduced.admin_actions,[]);assert.equal(reduced.checked.status.text,'—');assert.equal(reduced.completed.link,null);
});

test('native admin command payload cannot smuggle neighbouring scope or financial/origin fields',()=>{
  for(const mutation of [a=>{a.payload.client_id=id(99);},a=>{a.payload.net_pence='100';}]) {
    const r=row();r.admin_actions=[{kind:'NO_ONE_WORKED',label:'No one worked',native_allowed:true,
      payload:{source_group_id:scope.source_group_id,client_id:scope.client_id,source_cycle_id:scope.source_cycle_id,expected_cycle_version:3}}];
    mutation(r.admin_actions[0]);assert.throws(()=>projectChecklistRow(r,actor),ChecklistSourceError);
  }
});

test('signed latest ended row stays complete, older done week drops, earlier open stays, no-Timesheet row never appears',()=>{
  assert.ok(projectChecklistRow(row(),actor));
  const earlier=row();earlier.scope.week_ending='2026-09-27';assert.equal(projectChecklistRow(earlier,actor),null);
  earlier.retention.has_open_items=true;assert.ok(projectChecklistRow(earlier,actor));
  const empty=row();empty.retention.actual_timesheet_count=0;assert.equal(projectChecklistRow(empty,actor),null);
  const other=row();other.route='AGENCY_INVOICE';other.scope.week_ending='2026-09-27';assert.ok(projectChecklistRow(other,actor));
});

function sourceInformation() {
  const result=item();result.item_kind='SOURCE_INFORMATION';result.item_id=`native-action:${H}`;
  result.reason_code='WEEKLY_TIMESHEET_NOT_SUBMITTED';result.subject='INFORMATION';result.permitted_pay_subject=false;
  result.action=null;result.counted_timesheet_id=null;result.correction_binding=null;result.hold=zeroHold();
  result.source_information_witness={native_action_id:H,evidence_fingerprint:R,import_id:id(92),hr_row_id:id(93),
    existing_row_id:row().row_id,counts_for_row_admission:false};return result;
}
test('native source-only advisory is information inside an existing row, without Timesheet target or admission count',()=>{
  const result=projectChecklistItem(sourceInformation(),actor);
  assert.equal(result.counted_timesheet_id,null);assert.equal(result.action,null);
  assert.equal(result.source_information_witness.counts_for_row_admission,false);
  assert.equal(projectChecklistItem(sourceInformation(),payActor),null);
  const r=row();r.retention.actual_timesheet_count=0;
  assert.equal(projectChecklistRow(r,actor),null); // An advisory cannot resurrect/create its parent.
});
test('source-only information cannot acquire a fake Timesheet, correction, pay subject, action or neighbouring native witness',()=>{
  const mutations=[i=>{i.counted_timesheet_id=id(94);},i=>{i.permitted_pay_subject=true;},i=>{i.subject='PAY';},
    i=>{i.source_information_witness.counts_for_row_admission=true;},i=>{i.source_information_witness.native_action_id=R;},
    i=>{i.action={kind:'TIMESHEET',label:'Open',target:{timesheet_id:id(94)},native_allowed:true};}];
  for(const mutate of mutations){const i=sourceInformation();mutate(i);assert.throws(()=>projectChecklistItem(i,actor),ChecklistSourceError);}
});
test('signed issue itself cannot receive a Finalise action from an otherwise valid native scope',()=>{
  const i=item();i.action={kind:'FINALISE',label:'Finalise',target:{...scope},native_allowed:true};delete i.action.target.week_ending;
  assert.throws(()=>projectChecklistItem(i,actor),/SIGNED_FINALISE_NOT_ALLOWED/);
});
test('actual submitted Daily stays visible while native materialisation is pending, with complete immutable identity and no action',()=>{
  const i=item();i.sheet_scope='DAILY';i.action=null;i.status=nativeStatus('Submitted','warning','Native materialisation pending');
  i.submission_read={state:'SUBMITTED_PENDING_NATIVE_MATERIALISATION',timesheet_id:i.counted_timesheet_id,
    client_id:scope.client_id,candidate_id:id(95),work_date:i.work_date,workflow_id:id(96),workflow_generation:2,
    submission_sha256:H,actions_held:true};
  assert.ok(projectChecklistItem(i,actor));assert.equal(projectChecklistItem(i,actor).action,null);
  for(const mutate of [x=>{x.action={kind:'TIMESHEET',label:'Open',target:{timesheet_id:x.counted_timesheet_id},native_allowed:true};},
    x=>{x.submission_read.client_id=id(97);},x=>{x.submission_read.work_date='2026-10-06';},
    x=>{x.submission_read.timesheet_id=id(97);},x=>{x.submission_read.workflow_generation=0;},x=>{x.submission_read.actions_held=false;},
    x=>{x.status=nativeStatus('Ready','positive');}]) {
    const invalid=copy(i);mutate(invalid);assert.throws(()=>projectChecklistItem(invalid,actor),/PENDING_SUBMISSION_ACTION_UNAVAILABLE/);
  }
});

test('agency Completed may be true before invoicing; consumer draws native status rather than deriving invoice readiness',()=>{
  const r=row();r.route='AGENCY_INVOICE';r.completed.status=nativeStatus('Nothing outstanding');r.hold=zeroHold();
  const result=projectChecklistRow(r,actor);assert.equal(result.completed.value,true);assert.equal(result.completed.status.text,'Nothing outstanding');
  assert.equal(Object.hasOwn(result,'ready_to_invoice'),false);
});

test('Acacium pay-process caption is preserved without becoming paid quantity or Completed true',()=>{
  const r=row();r.route='SELF_BILL';r.completed={...cell(false),status:nativeStatus('To do','danger','Pay processing complete · Acacium report not yet in')};
  const result=projectChecklistRow(r,actor);assert.equal(result.completed.value,false);
  assert.equal(result.completed.status.subtext,r.completed.status.subtext);assert.equal(result.paid,undefined);
});

test('waiting correction counts T0 and permits exact P0 workspace action; after apply only P0 counts and R0 never does',()=>{
  const i=item();i.correction_binding={request_id:id(100),state:'WAITING',state_revision:1,original_timesheet_id:i.counted_timesheet_id,
    replacement_timesheet_id:id(101),reversal_timesheet_id:null};i.action.target.timesheet_id=id(101);i.action.label='Authorise both in Timesheet ›';
  assert.ok(projectChecklistItem(i,actor));
  i.action.label='Send reminder in Queries ›';assert.throws(()=>projectChecklistItem(i,actor),/REMINDER_NOT_ALLOWED/);
  i.action.label='Authorise both in Timesheet ›';i.correction_binding.state='APPLIED';i.correction_binding.state_revision=2;i.correction_binding.reversal_timesheet_id=id(102);
  assert.throws(()=>projectChecklistItem(i,actor),/MEMBER_SELECTION/);
  i.counted_timesheet_id=id(101);assert.ok(projectChecklistItem(i,actor));
  i.counted_timesheet_id=id(102);assert.throws(()=>projectChecklistItem(i,actor),/MEMBER_SELECTION/);
});

test('Daily is an item fact; client/group row stays same route and Source-declared own week',()=>{
  const i=item();i.sheet_scope='DAILY';i.status=nativeStatus('W7 · Tue 06/10 (daily) · waiting for manager','danger');
  const result=projectChecklistItem(i,actor);assert.equal(result.sheet_scope,'DAILY');assert.equal(result.scope.week_ending,'2026-10-11');
  assert.equal(projectChecklistRow(row(),actor).group,'North · signed Timesheets');
});

test('signed row cannot acquire Finalise, and exact action target/scope must stay in its one workspace',()=>{
  const r=row();r.completed.link={kind:'FINALISE',label:'Finalise',target:copy(scope),native_allowed:true};delete r.completed.link.target.week_ending;
  assert.throws(()=>projectChecklistRow(r,actor),/SIGNED_FINALISE/);
  const i=item();i.action.target.timesheet_id=id(999);assert.throws(()=>projectChecklistItem(i,actor),/ACTION_TARGET/);
  i.action={kind:'QUERIES',label:'Send reminder in Queries ›',target:{source_group_id:id(99),client_id:scope.client_id,source_cycle_id:scope.source_cycle_id,row_key:i.item_id},native_allowed:true};
  assert.throws(()=>projectChecklistItem(i,actor),/ACTION_SCOPE/);
});

test('closed user read payload rejects actor/origin/financial facts before any native callbacks',async()=>{
  const h=harness();for(const field of ['actor_user_id','permission_view','origin','holds_pay','withheld_amount_pence','replacement_timesheet_id','evidence']) {
    await rejects(()=>h.reader.read({...request(),[field]:'injected'},{}),'CHECKLIST_SOURCE_FIELD_NOT_ALLOWED');
  } assert.equal(h.calls.length,0);
});

test('read authenticates, calls native page, then authoritative Source evidence in sequence',async()=>{
  const h=harness();const result=await h.reader.read(request(),{});
  assert.deepEqual(h.calls.map(c=>typeof c==='string'?c:c[0]),['actor','native','evidence']);
  assert.equal(h.calls[1][1].actor_user_id,actor.actor_user_id);assert.equal(result.rows.length,1);
});

test('native pay/retention selection must precede cursor slicing; consumer refuses a hidden row rather than false end',async()=>{
  const hidden=row();hidden.permitted_pay_item_count=0;
  const h=harness({actor:payActor,frame:frame({permission_view:'PAY_READONLY',rows:[hidden]})});
  await rejects(()=>h.reader.read(request(),{}),'CHECKLIST_NATIVE_VIEW_FILTER_NOT_APPLIED');
});

test('wrong actor, duplicate identity, stale refresh or missing more cursor never publishes a valid page',async()=>{
  for(const f of [frame({actor_user_id:id(99)}),frame({rows:[row(),row()]}),frame({refresh_required:true}),frame({has_more:true,next_cursor:null})]) {
    await assert.rejects(()=>harness({frame:f}).reader.read(request(),{}),ChecklistSourceError);
  }
});

test('item expansion scope is exact and batch bound is internal 100, independent of main list',async()=>{
  const payload={command:'CHECKLIST_ISSUES',...scope,sort:'worker',direction:'ASC',limit:50};
  const h=harness({frame:frame({rows:[],items:[item()]})});assert.equal((await h.reader.read(payload,{})).items.length,1);
  const foreign=item();foreign.scope.client_id=id(99);
  await rejects(()=>harness({frame:frame({rows:[],items:[foreign]})}).reader.read(payload,{}),'CHECKLIST_SOURCE_SCOPE_CHANGED');
  await rejects(()=>h.reader.read({...payload,limit:101},{}),'CHECKLIST_SOURCE_REQUEST_INVALID');
});
test('main-list explicit filters stay scope-bound, and one page cannot mix main and expansion traversals',async()=>{
  await rejects(()=>harness().reader.read({...request(),client_id:id(99)},{}),'CHECKLIST_SOURCE_SCOPE_CHANGED');
  await rejects(()=>harness({frame:frame({items:[item()]})}).reader.read(request(),{}),'CHECKLIST_SOURCE_CONTRACT_INVALID');
});

test('NHSP/nonpreset native preservation is exact pass-through and never relabelled by Source consumer',async()=>{
  const h=harness({frame:frame({native_preservation:true})});assert.equal(await h.reader.read(request(),{}),h.native);
});

test('Timesheet information uses existing NEXT decoder; COMPLETE processing cannot fabricate paid hours',async()=>{
  const h=harness();const result=await h.reader.readTimesheetInformation({timesheet_id:id(80)},{});
  assert.deepEqual(h.calls.map(c=>typeof c==='string'?c:c[0]),['actor','timesheet','existing-next-decoder']);
  assert.equal(result.lifecycle.processing_status,'COMPLETE');assert.equal(result.lifecycle.schedules.paid_to_date.available,false);
  assert.deepEqual(result.lifecycle.schedules.paid_to_date.rows,[]);
});

test('500 client-week cache rows, insertions before and after consumed cursor, 20 closures, anchor return and duplicate-free merge',()=>{
  const session=createChecklistSession({compareRecords:(a,b)=>a.order-b.order});
  const page=records=>({rows:records,items:[],binding:H,read_revision:R,next_cursor:'opaque',has_more:true});
  const make=n=>({row_id:`row-${n}`,order:n});
  for(let p=0;p<10;p++) session.apply('main',page(Array.from({length:50},(_,i)=>make(p*50+i))));
  session.remember('main','row-180',1800);
  const refreshed={...page([make(-1),make(500)]),read_revision:H};
  const result=session.apply('main',refreshed,{anchoredRefresh:true,removedIds:Array.from({length:20},(_,i)=>`row-${i+170}`)});
  assert.equal(result.records.length,482);assert.equal(new Set(result.records.map(r=>r.row_id)).size,482);
  assert.equal(result.records[0].row_id,'row--1');assert.equal(result.records.at(-1).row_id,'row-500');
  assert.equal(result.anchor,'row-190');assert.equal(result.scrollTop,1800);assert.equal(result.hasMore,true);
  session.dispose();assert.equal(session.isLive('main'),false);
});

test('main and each expansion retain independent observers/cursors, collapse disconnects only its child, disposal blocks late activity',()=>{
  const calls=[];const session=createChecklistSession({compareRecords:(a,b)=>a.order-b.order});
  const observer=name=>({disconnect:()=>calls.push(name)});
  session.attachObserver('main',observer('main'));session.attachObserver('child-a',observer('a'));session.attachObserver('child-b',observer('b'));
  session.expand('main','row-a');session.collapse('main','row-a','child-a');assert.deepEqual(calls,['a']);
  assert.equal(session.isLive('main'),true);assert.equal(session.isLive('child-b'),true);
  session.dispose();assert.deepEqual(calls,['a','main','b']);
  assert.throws(()=>session.remember('main','row-a',0),/DISPOSED/);
});

test('changed permission/filter binding cannot reuse cache and changed revision requires explicit anchored refresh, never false end',()=>{
  const session=createChecklistSession({compareRecords:(a,b)=>a.order-b.order});
  const p={rows:[{row_id:'one',order:1}],items:[],binding:H,read_revision:R,next_cursor:'cursor',has_more:true};
  session.apply('main',p);
  assert.throws(()=>session.apply('main',{...p,binding:R}),/SCOPE_CHANGED/);
  assert.throws(()=>session.apply('main',{...p,read_revision:H,has_more:false,next_cursor:null}),/ANCHORED_REFRESH/);
});
