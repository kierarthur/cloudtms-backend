import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import crypto from 'node:crypto';
import {createNativeCorrectionSnapshotProducers,canonicalCorrectionWire} from '../docs/healthroster-build/proposals/cn-a0-native-normalisation/native-correction-snapshot-producers.mjs';
import {deriveNativeWeeklyCorrectionRoute,buildNativeWeeklyCorrectionSnapshot} from '../docs/healthroster-build/proposals/cn-a0-native-normalisation/native-weekly-correction-dispatch.mjs';
import {deriveCanonicalDailyFinancialContext,buildCanonicalDailyFinancialSnapshot} from '../docs/healthroster-build/proposals/cn-a0-native-normalisation/daily-canonical-authority.native-input.js';
import {normaliseRole,normaliseBand,parseCandidateRoleCodes} from '../docs/healthroster-build/proposals/cn-a0-native-normalisation/native-daily-role-helpers.mjs';
const id=n=>`00000000-0000-4000-8000-${String(n).padStart(12,'0')}`;
const I={original:id(3),p0:id(4),root:id(8),actor:id(2),candidate:id(6),client:id(7),contract:id(24),week:id(25),request:id(1)};
const actor={environment:'TEST',actor_user_id:I.actor},copy=x=>JSON.parse(JSON.stringify(x));
function setup({original=false,scope='WEEKLY',route='WEEKLY_MANUAL'}={}){
 const tid=original?I.original:I.p0,version=original?7:3,calls=[];
 const ts={timesheet_id:tid,version,booking_id:original?'business-original-TEXT':'bk_awc_exact_P0',sheet_scope:scope,
  is_current:true,archived_at_utc:null,authorised_at_server:null,contract_id:I.contract,
  actual_schedule_json:[{date:'2026-10-12',start:'20:00',end:'04:00',segment_id:'physical-part'}],
  settings_authority_json:{contract:'CONTRACT_SETTINGS_AUTHORITY_V1',values:{day_start_minutes:480}},
  worked_start_iso:'2026-10-12T20:00:00+01:00',worked_end_iso:'2026-10-13T04:00:00+01:00',break_minutes:15,
  job_title_norm:'NURSE',band:'6',week_ending_date:'2026-10-18',additional_units_week:{},additional_units_per_day:{},
  ...(original?{}:{parent_timesheet_id:I.root,correction_id:`awc:${I.root}:${I.request}`,correction_kind:'WHOLE_TIMESHEET_REPLACEMENT'})};
 const ctx={effective_timesheet_id:tid,out_timesheet:copy(ts),out_cur_fin:null,
  out_candidate:{id:I.candidate,pay_method:'PAYE',roles:[{code:'NURSE'}],sort_code:'00-00-00',account_number:'00000000'},
  out_umbrella:null,out_client_id:I.client,out_effective_flags:{route_type:route},
  out_policy:{erni_pct:0,apply_erni_to:'PAYE_ONLY',bh_list:[]}};
 const cw={id:I.week,contract_id:I.contract,timesheet_id:tid,week_ending_date:'2026-10-18'};
 const contract={id:I.contract,candidate_id:I.candidate,client_id:I.client,additional_rates_json:[]};
 const p={candidate_id:I.candidate,client_id:I.client,original_materialisation_required:original,original_authorisation_required:original,
  original_current_snapshot:null,authority:{request_id:I.request,root_timesheet_id:I.root,original_timesheet_id:I.original,original_timesheet_version:7,
    original_financial_id:null,original_financial_authority_hash:null,replacement_timesheet_id:I.p0,replacement_version:3,
    replacement_financial_id:null,replacement_schedule:copy(ts.actual_schedule_json)},frozen_original:{timesheet:copy(ts)},replacement_timesheet:copy(ts)};
 const snap={timesheet_id:tid,timesheet_version:version,candidate_id:I.candidate,client_id:I.client,pay_method:'PAYE',
  hours_day:8,hours_night:0,hours_sat:0,hours_sun:0,hours_bh:0,total_hours:8,
  pay_day:20,pay_night:20,pay_sat:20,pay_sun:20,pay_bh:20,charge_day:30,charge_night:30,charge_sat:30,charge_sun:30,charge_bh:30,
  additional_units_json:{},policy_snapshot_json:ctx.out_policy,invoice_breakdown_json:{mode:'SEGMENTS',segments:[]},processing_status:'PENDING_AUTH'};
 const deps={env:{},readContext:async ids=>{calls.push(['context',ids]);return [copy(ctx)];},
  readWeeklyContext:async ids=>{calls.push(['weekly-context',ids]);return [{timesheet_id:tid,out_cw:copy(cw),out_contract:copy(contract)}];},
  readShifts:async ids=>{calls.push(['shifts',ids]);return [{timesheet_id:tid,shifts:[]}];},
  readWeeklyValidation:async id=>{calls.push(['validation',id]);return [];},
  resolveRates:async items=>{calls.push(['rates',copy(items)]);return [{k:tid,pay_day:20,pay_night:20,pay_sat:20,pay_sun:20,pay_bh:20,
    charge_day:30,charge_night:30,charge_sat:30,charge_sun:30,charge_bh:30}];},
  buildWeekly:async input=>{calls.push(['native-weekly-calculator',input]);return copy(snap);},
  deriveDaily:deriveCanonicalDailyFinancialContext,buildDaily:buildCanonicalDailyFinancialSnapshot,
  toLocalParts:()=>({ymd:'2026-10-12'}),subtractBreak:segments=>segments,
  classifyMinutes:()=>({hours_day:8,hours_night:0,hours_sat:0,hours_sun:0,hours_bh:0,total_hours:8}),
  resolveEffectivePayChannel:({candidate})=>({ok:!!(candidate.sort_code&&candidate.account_number)}),
  anyMissingRates:(hours,pay,charge)=>Object.keys(hours).some(b=>hours[b]>0&&(pay[b]==null||charge[b]==null))};
 const build=()=>createNativeCorrectionSnapshotProducers(deps)[original?'buildOriginalSnapshot':'buildReplacementSnapshot']({prepare:p,actor});
 return {calls,ts,ctx,cw,contract,p,snap,deps,build,tid};
}
const refuses=(fn,code)=>assert.rejects(fn,e=>e.code===code);
test('actual physical P0 producer performs only scoped reads then unchanged weekly calculation',async()=>{
 const h=setup(),s=await h.build();assert.equal(s.has_rate_issue,false);assert.equal(s.has_pay_channel_issue,false);
 assert.equal(Object.hasOwn(s,'is_stale'),false);assert.deepEqual(h.calls.map(x=>x[0]),['context','weekly-context','shifts','validation','native-weekly-calculator']);
 const input=h.calls.at(-1)[1];assert.deepEqual(input.ts.actual_schedule_json,h.p.authority.replacement_schedule);
 assert.equal(input.ts.booking_id,'bk_awc_exact_P0');assert.equal(input.curFin,null);
});
test('missing Original producer selects exact frozen original identity, never P0, and preserves omitted metadata',async()=>{
 const h=setup({original:true});const s=await h.build();assert.equal(s.timesheet_id,I.original);
 assert.deepEqual(h.calls[0][1],[I.original]);assert.equal(Object.hasOwn(s,'is_stale'),false);
});
test('existing Original finance cannot enter missing-finance calculation callback',async()=>{
 const h=setup({original:true});h.p.original_materialisation_required=false;
 await refuses(h.build,'CORRECTION_CANONICAL_ORIGINAL_CONTEXT_INVALID');assert.equal(h.calls.length,0);
});
test('redirected, duplicate, foreign, stale-version or non-current context refuses before calculator',async()=>{
 for(const edit of [h=>{h.ctx.effective_timesheet_id=I.original;},h=>{h.ctx.out_candidate.id=id(40);},
   h=>{h.ctx.out_client_id=id(41);},h=>{h.ctx.out_timesheet.version=4;},h=>{h.ctx.out_timesheet.is_current=false;},
   h=>{h.deps.readContext=async()=>[h.ctx,h.ctx];}]){
  const h=setup();edit(h);await assert.rejects(h.build);assert.equal(h.calls.some(x=>x[0]==='native-weekly-calculator'),false);
 }
});
test('actual worker schedule and frozen settings drift are refused; Office proposal cannot substitute',async()=>{
 for(const field of ['actual_schedule_json','settings_authority_json','worked_start_iso','break_minutes','additional_units_week']){
  const h=setup();h.ctx.out_timesheet[field]=field==='break_minutes'?30:field==='worked_start_iso'?'2026-10-12T21:00:00+01:00':field==='additional_units_week'?{ONCALL:1}:{};
  await refuses(h.build,'CORRECTION_CANONICAL_SOURCE_CHANGED');
 }
});
test('P0 booking/root/request and concurrent materialisation mismatches refuse',async()=>{
 for(const edit of [h=>{h.ctx.out_timesheet.booking_id='original-business-booking';},h=>{h.ctx.out_timesheet.parent_timesheet_id=id(45);},
  h=>{h.ctx.out_timesheet.correction_id=`awc:${I.root}:${id(46)}`;},h=>{h.ctx.out_cur_fin={id:id(47)};}]){
  const h=setup();edit(h);await assert.rejects(h.build);
 }
 const h=setup({original:true});h.ctx.out_cur_fin={id:id(48)};await refuses(h.build,'CORRECTION_CANONICAL_ORIGINAL_ALREADY_MATERIALISED');
});
test('failed source read, missing/ambiguous imported row and duplicate validation cannot become empty defaults',async()=>{
 for(const edit of [h=>{h.deps.readShifts=async()=>{throw new Error('bounded dependency refusal');};},h=>{h.deps.readShifts=async()=>[];},
  h=>{h.deps.readShifts=async()=>[{timesheet_id:h.tid,shifts:'malformed'}];},
  h=>{h.deps.readWeeklyValidation=async()=>[{timesheet_id:h.tid},{timesheet_id:h.tid}];}]){
  const h=setup();edit(h);await assert.rejects(h.build);assert.equal(h.calls.some(x=>x[0]==='native-weekly-calculator'),false);
 }
});
test('used hourly rate absence and pay channel absence refuse without manufacturing false',async()=>{
 const h=setup();h.snap.pay_day=null;await refuses(h.build,'CORRECTION_CANONICAL_FINANCIAL_ADMISSION_BLOCKED');
 const bank=setup();delete bank.ctx.out_candidate.sort_code;await refuses(bank.build,'CORRECTION_CANONICAL_FINANCIAL_ADMISSION_BLOCKED');
});
test('native latest-validation ordering permits proved older history but refuses tied latest facts',async()=>{
 const h=setup();h.deps.readWeeklyValidation=async()=>[
  {timesheet_id:h.tid,status:'VALIDATION_OK',updated_at:'2026-10-13T13:00:00Z'},
  {timesheet_id:h.tid,status:'HOURS_MISMATCH',updated_at:'2026-10-12T13:00:00Z'}];
 await h.build();assert.equal(h.calls.at(-1)[1].validation.status,'VALIDATION_OK');
 const tied=setup();tied.deps.readWeeklyValidation=async()=>[
  {timesheet_id:tied.tid,status:'VALIDATION_OK',updated_at:'2026-10-13T13:00:00Z'},
  {timesheet_id:tied.tid,status:'HOURS_MISMATCH',updated_at:'2026-10-13T13:00:00Z'}];
 await refuses(tied.build,'CORRECTION_CANONICAL_VALIDATION_AMBIGUOUS');
});
test('real zero rate is retained; unused absent bucket does not invent a rate failure',async()=>{
 const h=setup();h.snap.pay_day=0;h.snap.pay_sun=null;const s=await h.build();assert.equal(s.pay_day,0);assert.equal(s.has_rate_issue,false);
});
test('nonzero additional claim requires exact unique native config rates, including explicit lawful zero',async()=>{
 for(const config of [[],[{code:'ONCALL',pay_rate:null,charge_rate:4}],[{code:'ONCALL',pay_rate:2,charge_rate:4},{code:'ONCALL',pay_rate:2,charge_rate:4}]]){
  const h=setup();h.ctx.out_timesheet.additional_units_week={ONCALL:1};h.p.replacement_timesheet.additional_units_week={ONCALL:1};
  h.contract.additional_rates_json=config;await refuses(h.build,'CORRECTION_CANONICAL_FINANCIAL_ADMISSION_BLOCKED');
 }
 const h=setup();h.ctx.out_timesheet.additional_units_week={ONCALL:1};h.p.replacement_timesheet.additional_units_week={ONCALL:1};
 h.contract.additional_rates_json=[{code:'ONCALL',pay_rate:0,charge_rate:0}];assert.equal((await h.build()).has_rate_issue,false);
});
test('native JSON wire omits only object undefined, retains decimal text/null, rejects invalid numbers/objects/arrays',()=>{
 assert.deepEqual(canonicalCorrectionWire({units:{ONCALL:{days:undefined,unit_count:'2.5000',pay_ex_vat:'12.3500'}},nil:null}),
  {nil:null,units:{ONCALL:{pay_ex_vat:'12.3500',unit_count:'2.5000'}}});
 for(const x of [NaN,Infinity,new Date(),[undefined],1n])assert.throws(()=>canonicalCorrectionWire(x));
});
test('ordinary DAILY role helper extraction retains exact normalisation and malformed role refusal',()=>{
 assert.equal(normaliseRole(' nurse  specialist '),'NURSE SPECIALIST');assert.equal(normaliseBand('b6'),'Band 6');
 assert.deepEqual(parseCandidateRoleCodes('[{"code":"Nurse"}]'),['NURSE']);assert.deepEqual(parseCandidateRoleCodes('bad'),[]);
});
test('DAILY callbacks execute unchanged native canonical module with exact rate request identity',async()=>{
 const h=setup({scope:'DAILY'}),s=await h.build();assert.deepEqual(h.calls.map(x=>x[0]),['context','rates']);
 assert.deepEqual(h.calls[1][1],[{k:I.p0,candidate_id:I.candidate,client_id:I.client,role:'NURSE',band:'Band 6',date:'2026-10-12',rate_type:'PAYE'}]);
 assert.equal(s.total_pay_ex_vat,160);assert.equal(s.total_charge_ex_vat,240);assert.equal(Object.hasOwn(s,'is_stale'),false);
 // These are canonical-body callback tests with controlled time buckets;
 // actual UK break/DST SQL/calculation acceptance remains the native journey.
});
test('DAILY duplicate/foreign rate key and unknown worked date refuse before financial approval',async()=>{
 for(const edit of [h=>{h.deps.resolveRates=async()=>[{k:I.original}];},h=>{h.deps.resolveRates=async()=>[];},
  h=>{h.deps.toLocalParts=()=>null;}]){const h=setup({scope:'DAILY'});edit(h);await assert.rejects(h.build);}
});
test('native weekly dispatch retains ordinary import/evidence/correction branch selection without queue work',async()=>{
 const base=setup();const choices=[];const owners={
  buildWeeklyScheduleSegmentsSnapshot:async(...args)=>{choices.push(['schedule',args.at(-1)]);return {ok:true,snapshot:base.snap};},
  buildNhspWeeklySnapshotCached:async(...args)=>{choices.push(['import',args.at(-1)]);return {ok:true,snapshot:base.snap};},
  rebuildFromExistingSegmentsEvidence:async(...args)=>{choices.push(['evidence',args.at(-1)]);return {ok:true,snapshot:base.snap};},
  enrichTsfinWithHrCrosscheck:async()=>{}};
 for(const [ts,flags,shifts,expected] of [
  [base.ts,{route_type:'WEEKLY_HEALTHROSTER'},[{id:id(60)}],'schedule'],
  [{...base.ts,correction_kind:null},{route_type:'WEEKLY_HEALTHROSTER'},[{id:id(60)}],'import'],
  [{...base.ts,correction_kind:null},{route_type:'WEEKLY_HEALTHROSTER'},[],'evidence'],
  [{...base.ts,correction_kind:null},{route_type:'WEEKLY_MANUAL'},[],'schedule']]){
  const r=deriveNativeWeeklyCorrectionRoute(ts,null,flags);await buildNativeWeeklyCorrectionSnapshot({env:{},ts,curFin:null,effFlags:flags,
    cw:base.cw,contract:base.contract,policySql:base.ctx.out_policy,shifts,validation:null,...r},owners);
  assert.equal(choices.at(-1)[0],expected);assert.equal(choices.at(-1)[1].write_now,false);assert.equal(choices.at(-1)[1].outbox_id,null);
 }
});
test('extracted native DAILY file is byte-identical to pinned controlling source and SQL only changes admission/reread',()=>{
 const folder=new URL('../docs/healthroster-build/proposals/cn-a0-native-normalisation/',import.meta.url);
 const guard=JSON.parse(fs.readFileSync(new URL('NATIVE_EXTRACTION_SOURCE_GUARD.json',folder)));
 assert.equal(crypto.createHash('sha256').update(fs.readFileSync(new URL('daily-canonical-authority.native-input.js',folder))).digest('hex'),guard.dailyNativeInput.sha256);
 const approval=fs.readFileSync(new URL('13102026_1127_approval.complete-proposed.sqlinc',folder),'utf8');
 assert.match(approval,/timesheet_correction_native_original_reversal_v1\(v_original,v_r\)/);
 assert.match(approval,/timesheet_correction_snapshot_identity_assert_v1\(v_original,v_request\.reversed_timesheet_id/);
 assert.match(approval,/timesheet_correction_snapshot_identity_assert_v1\(v_after_fin,v_p,v_version/);
 assert.match(approval,/tsfin_dequeue_specific\(array\[v_r\],1\)/);
 assert.match(approval,/'original_materialised_in_unit',\(v_prepare->>'original_materialisation_required'\)::boolean/);
});
