// Additive actual-owner callbacks, not another calculator/ledger/native driver.
// Import is inert. Root supplies the SAME isolated outer-transaction connection.
import assert from 'node:assert/strict';import fs from 'node:fs';import path from 'node:path';
import {randomUUID,createHash} from 'node:crypto';import {fileURLToPath} from 'node:url';
import {createActualCallbacks} from '../SOURCE_MOVE_JOINED_FIRST_USE_V11_20261007/actual-callbacks.mjs';
import {loadCurrentProducerOwners,pointExpression} from '../SOURCE_JOINED_NATIVE_CALLBACKS_V1_20261007/actual-callbacks.mjs';
import {produceParsedFinal,rawWorkedHtml as rawWorkedCsv} from '../SOURCE_MOVE_JOINED_FIRST_USE_V11_20261007/artifact-supplier.mjs';
import {withLocalProducerTransport} from '../MIXED300_FACTUAL_PRODUCER_V1_20261006/mixed300-fixture-v1-6.mjs';
const E=path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const sha=x=>createHash('sha256').update(x).digest('hex').toUpperCase();
const lit=x=>"'"+String(x).replaceAll("'","''")+"'";
const UUID=/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
export const PINS=Object.freeze({
 'SOURCE_JOINED_NATIVE_CALLBACKS_V1_20261007/actual-callbacks.mjs':'BBBBF8E17627BACD4A3011570179F1B8869FE04BB5373353F4F1CB12AD89A94A',
 'SOURCE_BANKING_JOINED_REVIEW_V2_20261007/joined-native-fixture.mjs':'C71B412241ED2C110AE7AD8C28264B52F444961A4410EB756EC6388A2934DF65',
 'BPAY_NEXT_LOCAL_PAID_RECOVERY_C2_TAIL_V3_20261006.mjs':'3B563836363F642D06476DCE66BC227AAFEDF7C8566DD6C99AC92C8ABF8898E4'
 ,'SOURCE_JOINED_FIXTURE_HELPER_SYNTAX_V1_20261007/actual-callbacks-helper-qualified.mjs':'AD90214930BC1964DF21A821D4F7B9040A97E7729E683A9F088F149FDE529E84'
 ,'SOURCE_JOINED_FIXTURE_HELPER_SYNTAX_V1_20261007/joined-native-owner-seams.sql':'3D74A986F9A98596A4DA26A38CC25A6DA39A1D60015B3E136B3B426625823628'
 ,'SOURCE_JOINED_FIXTURE_HELPER_SYNTAX_V1_20261007/FIXTURE_HELPER_MATERIAL.json':'DB9C69914896572521FC228E04082EDA9DDE6ADFD855B40AB39E13736DDBA411'
 ,'SOURCE_MOVE_JOINED_UM_MATERIAL_V2_20261007/SOURCE_MATERIAL.json':'B08EC0D0258143A46590AF69A1EC0BEE836A6065941FDEDA757B373110AD21F3'
 ,'SOURCE_MOVE_JOINED_UM_MATERIAL_V2_20261007/managed-overlay.sql':'6CF8093E9D651306CE284A729D4ACF7D902BF9D0B4CD9E122A84E5DD9AAD2530'
 ,'SOURCE_MOVE_JOINED_FIRST_USE_V11_20261007/actual-callbacks.mjs':'E06FEC072EB93BE3B5BD4CE57B7DD9AE535EEE90BC895A5F8B8D0683B5DDB2CE'
 ,'SOURCE_MOVE_JOINED_FIRST_USE_V11_20261007/artifact-supplier.mjs':'AAEDD3F44E2FF776DA7D5EC4B21644B8DAAFF7197BEC5E348D350F19EE49E8EB'
 ,'SOURCE_MOVE_JOINED_FIRST_USE_V11_20261007/fixture-readiness.mjs':'CD1DA4652B81DB0833A430FBB835B125D0360A4361876478C1F7F7BBF93979F8'
});
export function qualifySources(){for(const [f,pin]of Object.entries(PINS))assert.equal(sha(fs.readFileSync(path.join(E,f))),pin,'JMF_EXACT_FROZEN_SOURCE');}
const key=p=>{assert.match(p.canonical_work_id,UUID);assert.match(p.source_component_id,UUID);assert.equal(p.component_key,'SOURCE:'+p.source_component_id);
 return 'wc.work_id='+lit(p.canonical_work_id)+'::uuid and wc.component_key='+lit(p.component_key);};
// Immutable originals are POINT links. No history reconstruction or totals.
export function originalSnapshotExpression(p){return `(select jsonb_build_object(
 'origin',jsonb_build_object('id',wc.id,'work_id',wc.work_id,'component_key',wc.component_key,
 'effect_id',wc.original_effect_id,'run_line_id',wc.original_run_line_id,'revision_id',wc.original_revision_id,
 'approved_line_id',wc.original_approved_line_id,'transfer_id',wc.original_transfer_id,
 'source_channel',wc.original_source_pay_channel,'policy_id',wc.original_valuation_policy_id),
 'effect',to_jsonb(e),'approved_line',to_jsonb(a),'run_line',to_jsonb(l),'transfer',to_jsonb(t),
 'members',(select jsonb_agg(to_jsonb(m) order by m.member_no) from private.bpay_next_transfer_member m where m.transfer_id=t.id),
 'rates',(select jsonb_agg(to_jsonb(d) order by d.bucket) from private.bpay_next_rate_detail d where d.approved_line_id=a.id),
 'shifts',(select jsonb_agg(to_jsonb(d) order by d.detail_no) from private.bpay_next_shift_detail d where d.approved_line_id=a.id),
 'breaks',(select jsonb_agg(to_jsonb(b) order by d.detail_no,b.break_no) from private.bpay_next_shift_detail d
   join private.bpay_next_break_detail b on b.shift_detail_id=d.id where d.approved_line_id=a.id),
 'schedule',(select jsonb_agg(to_jsonb(s) order by s.rate_family,s.rate_code) from private.bpay_next_rate_schedule s where s.revision_id=wc.original_revision_id))
 from private.bpay_next_work_collection wc join private.bpay_next_financial_effect e on e.id=wc.original_effect_id
 join private.bpay_next_approved_line a on a.id=wc.original_approved_line_id
 join private.bpay_next_run_line l on l.id=wc.original_run_line_id
 join private.bpay_next_transfer t on t.id=wc.original_transfer_id where ${key(p)})`;}
export function casePointExpression(p){return `(select jsonb_build_object('collection_id',wc.id,'case_id',fc.id,
 'case_component_id',cc.id,'case_kind',fc.case_kind,'tax_treatment',fc.tax_treatment,'status',fc.status,
 'principal_approved',fc.principal_approved,'principal_recovered',fc.principal_recovered,
 'principal_written_off',fc.principal_written_off,'active_hold_amount',fc.active_hold_amount,
 'instruction_kind',cc.instruction_kind,'payroll_stage',cc.payroll_stage,'source_pay_channel',cc.source_pay_channel,
 'component_revision',cc.component_revision,'recovered_source_ex_vat',cc.recovered_source_ex_vat,
 'outstanding',fc.principal_approved-fc.principal_recovered-fc.principal_written_off)
 from private.bpay_next_work_collection wc join private.bpay_next_finance_case fc on fc.id=wc.case_id
 join private.bpay_next_case_component cc on cc.id=wc.case_component_id and cc.case_id=fc.id where ${key(p)})`;}
const cents=x=>{assert.equal(typeof x,'number');assert(Number.isFinite(x)&&Number.isInteger(x*100),'JMF_FINITE_FIXTURE_MONEY');return x.toFixed(2);};
export async function createActualFinancialCallbacks({c}){
 qualifySources();assert.equal(typeof c?.query,'function');assert.equal(typeof c?.sql,'function');
 const base=await createActualCallbacks({c}),pairs=[];let futureOwners=null;
 const original=async(pair,kind)=>{const p=await base.readPoint(pair,kind),x=await c.query(originalSnapshotExpression(p));
  assert.match(x?.origin?.effect_id??'',UUID,'JMF_REAL_ORIGINAL_REQUIRED');return x;};
 const preserve=async pair=>{const state=base.actualState(pair);assert(state.immutableOriginals,'JMF_ORIGINAL_SNAPSHOT_REQUIRED');
  for(const kind of ['SOURCE','OWN_B'])assert.deepEqual(await original(pair,kind),state.immutableOriginals[kind],'JMF_IMMUTABLE_ORIGINAL_UNCHANGED');};
 const current=async pair=>({SOURCE:await base.readPoint(pair,'SOURCE'),OWN_B:await base.readPoint(pair,'OWN_B')});
 const requireUMCase=async()=>{
  // Presence is an early refusal, NOT installed-body/catalogue qualification.
  // Root must bind the full independently reviewed53-unit material separately.
  assert.equal(await c.query("(select to_regprocedure('private.bpay_next_umbrella_case_completed_transfer_valid_v2(private.bpay_next_run_worker,private.bpay_next_transfer)') is not null)"),true,'JMF_UM53_CASE_PREREQUISITE');
 };
 const createFuture=async pair=>{
  const state=base.actualState(pair);assert(!state.futureEarning,'JMF_ONE_GENUINE_FUTURE_CAPACITY_ONLY');
  futureOwners??=await loadCurrentProducerOwners();const client=state.p.clients[1],contract=state.p.contracts[1];let root=null;
  const env={SUPABASE_URL:'https://mixed300.invalid',SUPABASE_SERVICE_ROLE_KEY:'synthetic-local-only'};
  for(const generation of [0,1]){
   const cycle=randomUUID(),week=generation===0?'2026-09-20':'2026-09-27';
   // Configuration/CSV inputs only. Economic/source/approval/financial facts
   // are authored by exactly the existing parsed Final/APP/payment owners.
   await c.sql(`insert into public.weekly_source_cycles(id,source_group_id,finalisation_week_ending,cutoff_at_utc,scope_client_id)
    values(${lit(cycle)}::uuid,${lit(state.p.groups[0].id)}::uuid,${lit(week)}::date,
    ((${lit(week)}::date-4)::text||' 18:00:00 Europe/London')::timestamptz,${lit(client.id)}::uuid);`);
   const fact={slot:'FUTURE',candidate:pair.candidate,contract:contract.id,client:client.id,date:'2026-09-15',week:'2026-09-20'};
   const artifact=rawWorkedCsv({requestId:'JMF-'+pair.candidate+'-FUTURE',staff:state.p.candidates[0].displayName,
    workDate:fact.date,bootstrap:generation===0});
   const produced=await withLocalProducerTransport(state.bridge.fetchImpl,()=>produceParsedFinal({p:state.p,
    b:{group:state.p.groups[0].id,cycle,client:client.id,rows:[fact]},artifact,owners:futureOwners,env,
    dataRpc:state.bridge.dataRpc,qualifyProducedRoot:state.bridge.qualifyProducedRoot,first:generation===0}));
   if(generation===0)root=produced.root_timesheet_id;
   else{assert.equal(produced.root_timesheet_id,root,'JMF_GENUINE_FUTURE_ROOT_REUSED');
    const app=await c.query(`pg_temp.jmv_approve_parsed_later_final(${lit(pair.actor.id)}::uuid,${lit(root)}::uuid,
     ${lit(produced.final_revision_id)}::uuid,${lit('jmf-app-future-'+pair.candidate)})`);assert.equal(app.reply.published,true);}
   const command=await c.query(`(select p.command_id from private.bpay_next_work w join private.bpay_next_publication p
    on p.work_id=w.id and p.revision_id=w.current_revision_id where w.original_timesheet_id=${lit(root)}::uuid)`);
   assert.match(command,UUID);await state.api.enrollCommand(command,'POSITION_APPLY');
  }
  const point=await c.query(pointExpression(root));assert.equal(point.binding_control_state,'APPLIED');assert.equal(point.binding_worker_state,'DONE');
  assert.equal(point.source_book_state,'READY');assert.equal(Number(point.original_bucket_units??0),0);
  assert.equal(point.source_book_seed_kind,'ZERO');assert.equal(point.source_book_seed_effect_id,null);
  for(const k of ['native_realised_paye','native_realised_umbrella','native_held_paye','native_held_umbrella'])assert.equal(Number(point[k]),0);
  assert.equal(point.approval_source_channel,state.spec.to);assert(Number(point.approved_source_ex_vat)>3,'JMF_REAL_EARNING_CAPACITY');
  state.futureEarning={root,point};await preserve(pair);return state.futureEarning;
 };
 const settle=async(pair,{negativeCapacity='CURRENT_ONLY'}={})=>{
  assert(['CURRENT_ONLY','GENUINE_FUTURE_SOURCE'].includes(negativeCapacity),'JMF_CLOSED_CAPACITY_SCOPE');
  const state=base.actualState(pair),before=await current(pair),a=before.SOURCE,b=before.OWN_B,channel=state.spec.to;
  // Minor units project ACTUAL penny amounts for assertions; they do not price,
  // convert channels, allocate capacity, infer units or round an economic input.
  const minor=value=>{const text=String(value);assert.match(text,/^-?\d+(?:\.\d{1,2})?$/,'JMF_EXACT_ACTUAL_PENNY_AMOUNT');
   const negative=text.startsWith('-'),parts=(negative?text.slice(1):text).split('.');
   const n=Number(parts[0])*100+Number((parts[1]??'').padEnd(2,'0'));assert(Number.isSafeInteger(n));return negative?-n:n;};
  const decimal=n=>{assert(Number.isSafeInteger(n));return (n<0?'-':'')+Math.floor(Math.abs(n)/100)+'.'+String(Math.abs(n)%100).padStart(2,'0');};
  await preserve(pair);for(const p of [a,b]){assert.equal(p.receipt_qualification_state,'READY');assert.equal(p.target_pay_channel,channel);
   for(const k of ['comparable_held_ex_vat','native_held_paye','native_held_umbrella'])assert.equal(minor(p[k]),0,'JMF_EXACT_NO_HELD_PREFIX_SCOPE');}
  const debts=[];
  for(const kind of ['SOURCE','OWN_B'])if(minor(before[kind].selected_residual_ex_vat)<0){
   const point=before[kind],debt=await c.query(casePointExpression(point));assert(debt,'JMF_EACH_NEGATIVE_COMPONENT_REAL_CASE');
   assert.equal(debt.case_kind,'OVERPAYMENT');assert.equal(debt.tax_treatment,'TAXABLE');assert.equal(debt.payroll_stage,'GROSS_DEDUCT');
   assert.equal(debt.instruction_kind,'RECOVERY');assert.equal(debt.source_pay_channel,channel);assert.equal(minor(debt.active_hold_amount),0);
   assert.equal(minor(debt.outstanding),-minor(point.selected_residual_ex_vat),'JMF_REAL_CURRENT_AUTOMATIC_COMPARISON_DEBT');
   assert.match(debt.collection_id,UUID);assert.match(debt.case_component_id,UUID);assert.match(debt.case_id,UUID);
   debts.push({kind,point,debt});
  }
  assert.equal(new Set(debts.map(x=>x.debt.case_component_id)).size,debts.length,'JMF_DISJOINT_A_B_CASE_COMPONENTS');
  if(debts.length&&channel==='UMBRELLA')await requireUMCase();
  let future=null;if(debts.length&&negativeCapacity==='GENUINE_FUTURE_SOURCE')future=await createFuture(pair);
  const positive=[a,b].filter(p=>minor(p.selected_residual_ex_vat)>0),earningsMinor=positive.reduce((n,p)=>n+minor(p.selected_residual_ex_vat),0)
   +(future?minor(future.point.approved_source_ex_vat):0);
  const exposure=(kind,point,debt)=>({kind,source_component_id:point.source_component_id,case_id:debt.case_id,
   case_component_id:debt.case_component_id,source_pay_channel:debt.source_pay_channel,target_pay_channel:channel,
   sourceCaseOutstandingExVat:Number(debt.outstanding),remainingTargetDebtExVat:Math.max(-minor(point.selected_residual_ex_vat),0)/100});
  if(earningsMinor===0){await preserve(pair);return {
   kind:debts.length?'UNFUNDED_NEGATIVE_EXPOSURES':'ZERO_DIFFERENCE_NO_NEW_TRANSFER',spec:state.spec.key,
   sourceResidual:Number(a.selected_residual_ex_vat),ownBResidual:Number(b.selected_residual_ex_vat),
   allCases:debts.map(x=>exposure(x.kind,x.point,x.debt)),openExposures:debts.map(x=>exposure(x.kind,x.point,x.debt)),
   paid:0,recoveredSourceExVat:0,deductedTargetExVat:0,bankCash:0,
   allocationResults:[],originalPaymentsUnchanged:true,actualFinancialExecution:false,exactReplay:null,
   execution:'NO_REAL_EARNINGS',providerCalls:0,fullFinancialMatrix:false};}
  const api=state.api,run=randomUUID(),command=randomUUID();await api.preparation('CREATE',{run_id:run,pay_date:'2026-10-09'});
  const works=[{work_id:b.approval_work_id,expected_revision_id:b.approval_revision_id,mode:'ALL'}];
  assert.equal(a.approval_work_id,b.approval_work_id,'JMF_REAL_COMPLETE_DESTINATION_WORK');
  if(future)works.push({work_id:future.point.approval_work_id,expected_revision_id:future.point.approval_revision_id,mode:'ALL'});
  const workPage={run_id:run,page_no:'1',choices:works};await api.preparation('WORK_PAGE',workPage);
  assert.equal((await api.preparation('WORK_PAGE',workPage)).replay,true);
  if(debts.length){const ids=debts.map(x=>x.debt.case_component_id),page={run_id:run,candidate_id:pair.candidate,
    selection_revision:'1',page_no:'1',request_id:randomUUID(),component_ids:ids,selected:ids.map(()=>true)};
   await api.preparation('CASE_PAGE',page);assert.equal((await api.preparation('CASE_PAGE',page)).replay,true);
   const sealed=await api.preparation('CASE_SEAL',{run_id:run,candidate_id:pair.candidate,selection_revision:'1',expected_pages:'1',expected_items:String(ids.length)});
   assert.equal(sealed.selected_count,String(ids.length));}
  await api.preparation('SEAL_ENQUEUE',{run_id:run,command_id:command,expected_pages:'1',expected_count:String(works.length)});
  await api.enrollCommand(command,'PREPARE');const review=await api.preparation('REVIEW',{run_id:run});assert.equal(review.all_ready,true);
  assert.deepEqual((await api.preparation('CONFIRM',{run_id:run,review_revision:review.review_revision})).drafts,[{run_id:run,pay_channel:channel}]);
  const page=await api.page('CHANNEL_WORKERS',{run_id:run,pay_channel:channel,limit:'100'});assert.equal(page.complete,true);assert.equal(page.rows.length,1);
  const worker=page.rows[0].run_worker_id;assert.equal(page.rows[0].candidate_id,pair.candidate);
  // Actual saved DRAFT results own ordering, thresholds, floors and per-Case
  // source/target allocation. Do not replace them with min(), FIFO or a quote.
  let allocations=[];
  if(debts.length)allocations=await c.query(`(select coalesce(jsonb_agg(jsonb_build_object(
    'instruction_id',i.id,'result_id',r.id,'case_id',i.case_id,'case_component_id',i.case_component_id,
    'source',r.allocated_source_ex_vat,'target',r.allocated_target_ex_vat,'vat',r.allocated_target_vat,'inclusive',r.allocated_target_inc_vat,
    'comparison_receipt_id',i.source_comparison_receipt_id,'collection_id',i.work_collection_id,
    'stage',i.payroll_stage,'pass',r.pass_kind,'source_channel',i.source_pay_channel,'target_channel',i.target_pay_channel)
    order by i.case_component_id),'[]'::jsonb)
    from private.bpay_next_run_case_instruction i join private.bpay_next_case_allocation_result r on r.instruction_id=i.id
    where i.run_worker_id=${lit(worker)}::uuid and i.case_component_id in (${debts.map(x=>lit(x.debt.case_component_id)+'::uuid').join(',')}) and r.pass_kind='DRAFT')`);
  assert(Array.isArray(allocations));assert.equal(allocations.length,debts.length,'JMF_EXACT_SELECTED_DRAFT_RESULT_SET');
  assert.deepEqual(allocations.map(x=>x.case_component_id).sort(),debts.map(x=>x.debt.case_component_id).sort());
  for(const allocation of allocations){const d=debts.find(x=>x.debt.case_component_id===allocation.case_component_id);
   assert.match(allocation.instruction_id,UUID);assert.match(allocation.result_id,UUID);
   assert.match(allocation.comparison_receipt_id??'',UUID,'JMF_REAL_FROZEN_COMPARISON_INSTRUCTION');
   assert.equal(allocation.collection_id,d.debt.collection_id);assert.equal(allocation.case_id,d.debt.case_id);
   assert.equal(allocation.stage,'GROSS_DEDUCT');assert.equal(allocation.pass,'DRAFT');
   assert.equal(allocation.source_channel,d.debt.source_pay_channel);assert.equal(allocation.target_channel,channel);
   assert(minor(allocation.source)>=0&&minor(allocation.source)<=minor(d.debt.outstanding));assert(minor(allocation.target)>=0);
   assert.equal(minor(allocation.vat),0,'JMF_EXACT_NO_VAT_FIXTURE');assert.equal(minor(allocation.inclusive),minor(allocation.target));}
  const recoveredSourceMinor=allocations.reduce((n,x)=>n+minor(x.source),0),deductedTargetMinor=allocations.reduce((n,x)=>n+minor(x.target),0),
   deductedInclusiveMinor=allocations.reduce((n,x)=>n+minor(x.inclusive),0),cashMinor=earningsMinor-deductedInclusiveMinor;
  assert(cashMinor>=0,'JMF_ACTUAL_ALLOCATION_WITHIN_GENUINE_EARNINGS');
  assert.equal(minor(page.rows[0].frozen_ex_vat),earningsMinor-deductedTargetMinor);assert.equal(minor(page.rows[0].frozen_vat),0);
  let net=null;if(channel==='PAYE'){
   net={command_id:randomUUID(),run_worker_id:worker,entered_paye_net:decimal(cashMinor)};
   if(debts.length){const status=await api.page('STATUS',{run_id:run,run_worker_id:worker,limit:'100'});
    net.expected_projection_revision=status.header.net_projection_revision;
    assert(typeof net.expected_projection_revision==='string'&&/^(?:0|[1-9]\d{0,18})$/.test(net.expected_projection_revision)&&BigInt(net.expected_projection_revision)<9223372036854775807n,'JMF_REAL_SELECTED_CASE_NET_PROJECTION');}
   await api.financial('PAYE_NET',net);await api.enrollCommand(net.command_id,'PAYE_NET_ENTRY');
  }else assert.equal(channel,'UMBRELLA'); // Never invent a UM NET request.
  const build={command_id:randomUUID(),run_worker_id:worker},transfer=await api.financial('TRANSFER_BEGIN',build);
  await api.enrollCommand(build.command_id,'TRANSFER_BUILD');
  const actualTransfer=await c.query(`(select jsonb_build_object('cash',cash_amount,'kind',execution_kind,'status',status)
   from private.bpay_next_transfer where id=${lit(transfer.transfer_id)}::uuid)`);
  assert.equal(minor(actualTransfer.cash),cashMinor);assert.equal(actualTransfer.status,'MEMBERS_READY');
  let issue=null,outcome;
  if(cashMinor===0){assert.equal(actualTransfer.kind,'INTERNAL_ZERO');outcome={command_id:randomUUID(),transfer_id:transfer.transfer_id};
   await api.financial('INTERNAL_SETTLE',outcome);await api.enrollCommand(outcome.command_id,'INTERNAL_SETTLEMENT');}
  else{issue={instruction_id:randomUUID(),transfer_id:transfer.transfer_id};const csv=await api.financial('CSV_ISSUE',issue);
   assert.equal(csv.phase,'ISSUED_CSV');assert.equal(csv.payment_recorded,false);
   outcome={command_id:randomUUID(),transfer_id:transfer.transfer_id,receipt_id:'JMF-LOCAL-'+randomUUID(),amount:decimal(cashMinor),occurred_at:'2026-10-09T12:00:00Z'};
   await api.financial('CSV_SETTLEMENT',outcome);await api.enrollCommand(outcome.command_id,'CSV_SETTLEMENT');}
  const after=await current(pair),caseAfter=[];
  for(const kind of ['SOURCE','OWN_B']){const prior=before[kind],now=after[kind],d=debts.find(x=>x.kind===kind),allocation=d?allocations.find(x=>x.case_component_id===d.debt.case_component_id):null,
    targetRecoveryMinor=allocation?minor(allocation.target):0,delta=Math.max(minor(prior.selected_residual_ex_vat),0)-targetRecoveryMinor;
   assert.equal(now.canonical_work_id,prior.canonical_work_id);assert.equal(now.source_component_id,prior.source_component_id);
   assert.equal(minor(now.comparable_paid_ex_vat),minor(prior.comparable_paid_ex_vat)+delta);
   assert.equal(minor(now.comparable_held_ex_vat),0);assert.equal(minor(now.selected_residual_ex_vat),minor(prior.selected_residual_ex_vat)-delta);
   const target=channel==='PAYE'?'native_realised_paye':'native_realised_umbrella',other=channel==='PAYE'?'native_realised_umbrella':'native_realised_paye';
   assert.equal(minor(now[target]),minor(prior[target])+delta);assert.equal(minor(now[other]),minor(prior[other]));
   assert.equal(now.receipt_qualification_state,'READY');
   if(d){const actual=await c.query(casePointExpression(prior));
    assert.equal(minor(actual.principal_recovered),minor(d.debt.principal_recovered)+minor(allocation.source));
    assert.equal(minor(actual.outstanding),minor(d.debt.outstanding)-minor(allocation.source));assert.equal(minor(actual.active_hold_amount),0);
    caseAfter.push({kind,debt:actual});}}
  let futureAfter=null,futureOriginal=null;
  if(future){futureAfter=await c.query(pointExpression(future.root));
   assert.equal(futureAfter.canonical_work_id,future.point.canonical_work_id);assert.equal(futureAfter.source_component_id,future.point.source_component_id);
   assert.equal(futureAfter.approval_work_id,future.point.approval_work_id);assert.equal(futureAfter.approval_revision_id,future.point.approval_revision_id);
   assert.equal(futureAfter.source_book_state,'READY');assert.equal(futureAfter.binding_control_state,'APPLIED');assert.equal(futureAfter.binding_worker_state,'DONE');
   assert.equal(minor(futureAfter.approved_source_ex_vat),minor(future.point.approved_source_ex_vat));
   const target=channel==='PAYE'?'native_realised_paye':'native_realised_umbrella',other=channel==='PAYE'?'native_realised_umbrella':'native_realised_paye';
   assert.equal(minor(futureAfter[target]),minor(future.point.approved_source_ex_vat),'JMF_REAL_FUTURE_FULL_TARGET_POST');assert.equal(minor(futureAfter[other]),0);
   assert.equal(minor(futureAfter.native_held_paye),0);assert.equal(minor(futureAfter.native_held_umbrella),0);
   assert.equal(futureAfter.original_paid_basis_qualified,true);assert.equal(minor(futureAfter.original_effect_source_ex_vat),minor(future.point.approved_source_ex_vat));
   assert.equal(minor(futureAfter.original_line_source_ex_vat),minor(future.point.approved_source_ex_vat));assert.equal(Number(futureAfter.original_bucket_units),1);
   futureOriginal=await c.query(originalSnapshotExpression(futureAfter));assert.match(futureOriginal?.origin?.effect_id??'',UUID);
   assert.equal(futureOriginal.origin.transfer_id,transfer.transfer_id);
  }
  await preserve(pair);if(net)assert.equal((await api.financial('PAYE_NET',net)).replay,true);
  assert.equal((await api.financial('TRANSFER_BEGIN',build)).replay,true);if(issue)assert.equal((await api.financial('CSV_ISSUE',issue)).replay,true);
  assert.equal((await api.financial(cashMinor===0?'INTERNAL_SETTLE':'CSV_SETTLEMENT',outcome)).replay,true);
  assert.deepEqual(await current(pair),after);for(const x of caseAfter)assert.deepEqual(await c.query(casePointExpression(before[x.kind])),x.debt);
  if(future){assert.deepEqual(await c.query(pointExpression(future.root)),futureAfter,'JMF_REAL_FUTURE_POST_REPLAY_POINT');
   assert.deepEqual(await c.query(originalSnapshotExpression(futureAfter)),futureOriginal,'JMF_REAL_FUTURE_ORIGINAL_REPLAY');}
  await preserve(pair);
  return {kind:debts.length?'ACTUAL_GROSS_CASE_RECOVERY':'ACTUAL_POSITIVE_DIFFERENCE_SETTLEMENT',spec:state.spec.key,
   run_id:run,run_worker_id:worker,transfer_id:transfer.transfer_id,outcome_command_id:outcome.command_id,
   originalPaymentsUnchanged:true,actualFinancialExecution:true,sourceResidual:Number(after.SOURCE.selected_residual_ex_vat),ownBResidual:Number(after.OWN_B.selected_residual_ex_vat),
   paid:earningsMinor/100,recoveredSourceExVat:recoveredSourceMinor/100,deductedTargetExVat:deductedTargetMinor/100,bankCash:cashMinor/100,
   allocationResults:allocations,allCases:caseAfter.map(x=>exposure(x.kind,after[x.kind],x.debt)),
   openExposures:caseAfter.filter(x=>minor(x.debt.outstanding)>0||minor(after[x.kind].selected_residual_ex_vat)<0)
    .map(x=>exposure(x.kind,after[x.kind],x.debt)),
   execution:actualTransfer.kind,futureGenuineSource:!!future,
   futurePayment:future?{root_timesheet_id:future.root,canonical_work_id:futureAfter.canonical_work_id,
    source_component_id:futureAfter.source_component_id,original_effect_id:futureAfter.original_effect_id,
    original_transfer_id:futureOriginal.origin.transfer_id,fullActualSourceExVat:Number(futureAfter.original_effect_source_ex_vat),
    actualFullTargetPost:true,exactPointAndOriginalReplay:true}:null,
   exactReplay:true,providerCalls:0,fullFinancialMatrix:false};
 };
 return {...base,
  async createActualPair(spec){const pair=await base.createActualPair(spec);pairs.push(pair);return pair;},
  async settleFirstPayment(pair,channel,amount){const result=await base.settleFirstPayment(pair,channel,amount),state=base.actualState(pair);
   state.immutableOriginals={SOURCE:await original(pair,'SOURCE'),OWN_B:await original(pair,'OWN_B')};return result;},
  preserveOriginalPayments:preserve,createGenuineFutureCapacity:createFuture,settleCurrentDifferences:settle,
  actualPairs:()=>[...pairs]
 };
}
