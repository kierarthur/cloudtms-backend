import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import {buildExactRequestA0WeeklyClockSnapshot} from '../docs/healthroster-build/proposals/cn-a0-native-normalisation/exact-request-a0-weekly-clock-arm.mjs';
import {createNativeCorrectionSnapshotProducers} from '../docs/healthroster-build/proposals/cn-a0-native-normalisation/native-correction-snapshot-producers.mjs';
import {deriveNativeWeeklyCorrectionRoute} from '../docs/healthroster-build/proposals/cn-a0-native-normalisation/native-weekly-correction-dispatch.mjs';
const id=n=>`00000000-0000-4000-8000-${String(n).padStart(12,'0')}`,copy=v=>JSON.parse(JSON.stringify(v));
const I={request:id(1),original:id(2),candidate:id(3),client:id(4),contract:id(5),week:id(6),actor:id(7)};
function fixture(){
 const calls=[];
 const ts={timesheet_id:I.original,version:4,booking_id:'business-original-TEXT',contract_id:I.contract,
  sheet_scope:'WEEKLY',is_current:true,archived_at_utc:null,authorised_at_server:null,week_ending_date:'2026-10-18',
  actual_schedule_json:[{date:'2026-10-12',start:'20:00',end:'04:00',break_mins:30,segment_id:'immutable-original-part'}],
  settings_authority_json:{authority_version:'CONTRACT_SETTINGS_AUTHORITY_V1',authority_fingerprint:'a'.repeat(64),
   client_id:I.client,contract_id:I.contract,values:{timezone_id:'Europe/London'},applicability:{configuration_valid:true,import_authoritative:false}}};
 const cw={id:I.week,contract_id:I.contract,timesheet_id:I.original,week_ending_date:'2026-10-18'};
 const contract={id:I.contract,candidate_id:I.candidate,client_id:I.client,start_date:'2026-10-01',end_date:'2026-10-31',
  pay_method_snapshot:'PAYE',rates_json:{paye_day:'20.0000',charge_day:'30.0000'},additional_rates_json:[]};
 // Synthetic closed native-wire shapes, not a real activation/release receipt.
 const scope={contract:'TIMESHEET_CORRECTION_A0_CLOCK_SCOPE_V1',request_id:I.request,original_timesheet_id:I.original,
  original_version:4,original_booking_id:ts.booking_id,candidate_id:I.candidate,client_id:I.client,contract_id:I.contract,
  week_ending_date:'2026-10-18',pricing_origin:'SIGNED_CHECK_ONLY_CONTRACT_RATES',rule_set_version_id:null,
  settings_authority_fingerprint:'a'.repeat(64),timesheet_authority_hash:'b'.repeat(64),contract_input:copy(contract),
  controlled_scope:{source_group_id:id(10),source_group_version:'2',membership_id:id(11),membership_valid_from:'2026-10-01',
   membership_valid_to:null,assignment_id:id(12),assignment_version:'3',activation_release_id:'synthetic-native-wire-only',
   activation_request_id:id(13),activation_request_sha256:'c'.repeat(64),activation_receipt_sha256:'d'.repeat(64)},scope_sha256:'e'.repeat(64)};
 const authority={request_id:I.request,origin:'SIGNED_TIMESHEET_CORRECTION',original_timesheet_id:I.original,original_timesheet_version:4,
  original_financial_id:null,original_financial_authority_hash:null,original_invoice_id:null,return_invoice_id:null,original_a0_clock_scope:scope};
 const snapshot={timesheet_id:I.original,timesheet_version:4,candidate_id:I.candidate,client_id:I.client,pay_method:'PAYE',
  hours_day:7.5,hours_night:0,hours_sat:0,hours_sun:0,hours_bh:0,pay_day:'20.0000',charge_day:'30.0000',additional_units_json:{}};
 const owners={buildWeeklyScheduleSegmentsSnapshot:async(...args)=>{calls.push(['schedule',args]);return {ok:true,snapshot:copy(snapshot)};},
  rebuildFromExistingSegmentsEvidence:async()=>{calls.push(['evidence']);throw new Error('NO_EXISTING_SEGMENTS_EVIDENCE');},
  buildNhspWeeklySnapshotCached:async()=>{calls.push(['import']);throw new Error('unexpected import calculator');},
  enrichTsfinWithHrCrosscheck:async(...args)=>{calls.push(['enrich',args]);}};
 const flags={route_type:'WEEKLY_HEALTHROSTER',client_autoprocess_hr:true};
 const input={env:{},ts,curFin:null,cw,contract,policySql:{erni_pct:'13.8000'},shifts:[],validation:null,effFlags:flags,
  correctionPurpose:'MISSING_ORIGINAL_A0',authority,frozenOriginal:{timesheet:copy(ts),manager_signed_witness:{workflow_id:id(20)}},
  candidateId:I.candidate,clientId:I.client,...deriveNativeWeeklyCorrectionRoute(ts,null,flags)};
 return {calls,ts,cw,contract,authority,scope,input,snapshot,owners,run:()=>buildExactRequestA0WeeklyClockSnapshot(input,owners)};
}
const refuses=fn=>assert.rejects(fn,e=>e.code==='CORRECTION_A0_NATIVE_CLOCK_SOURCE_UNPROVED');
test('exact missing-A0 native scope routes unchanged original clock producer with no writes/queue',async()=>{
 const h=fixture(),result=await h.run();assert.equal(result.timesheet_id,I.original);
 assert.deepEqual(h.calls.map(c=>c[0]),['schedule','enrich']);
 const args=h.calls[0][1];assert.equal(args[1],h.ts);assert.equal(args[2],h.cw);assert.equal(args[3],h.contract);assert.equal(args[4],null);
 assert.equal(args[5].write_now,false);assert.equal(args[5].outbox_id,null);assert.equal(args[5].policy_override,h.input.policySql);
 assert.deepEqual(args[5].hr_preloaded_shifts,[]);assert.equal(result.pay_day,'20.0000');
});
test('ordinary HR missing segments retains native refusal; a purpose field cannot replace physical P0 routing',async()=>{
 const h=fixture();delete h.input.correctionPurpose;
 await assert.rejects(h.run,/NO_EXISTING_SEGMENTS_EVIDENCE/);assert.deepEqual(h.calls.map(c=>c[0]),['evidence']);
 const p0=fixture();p0.input.correctionPurpose='REPLACEMENT_P0';p0.ts.correction_kind='WHOLE_TIMESHEET_REPLACEMENT';
 await p0.run();assert.deepEqual(p0.calls.map(c=>c[0]),['schedule','enrich']);
});
test('missing/foreign/extra scope, source pricing, non-NULL rules and altered rate input refuse before calculator',async()=>{
 for(const edit of [h=>delete h.authority.original_a0_clock_scope,h=>h.scope.request_id=id(50),
  h=>h.scope.original_booking_id='another-business-TEXT',h=>h.scope.candidate_id=id(51),h=>h.scope.client_id=id(52),
  h=>h.scope.contract_id=id(53),h=>h.scope.original_version=5,h=>h.scope.settings_authority_fingerprint='f'.repeat(64),
  h=>h.scope.pricing_origin='SOURCE_AMOUNT',h=>h.scope.rule_set_version_id=id(54),h=>h.scope.ready=true,
  h=>h.scope.contract_input.rates_json.paye_day='21.0000',h=>h.scope.controlled_scope.assignment_version=3,
  h=>h.scope.controlled_scope.source_group_version='0',h=>h.scope.controlled_scope.activation_request_sha256='invalid',
  h=>delete h.scope.controlled_scope.activation_receipt_sha256]){
  const h=fixture();edit(h);await refuses(h.run);assert.equal(h.calls.length,0);
 }
});
test('existing FIN/import, empty clock, agency issued parent or missing signed witness never enters new arm',async()=>{
 for(const edit of [h=>h.input.curFin={id:id(55)},h=>h.input.shifts=[{id:id(56)}],
  h=>{h.ts.actual_schedule_json=[];h.input.frozenOriginal.timesheet.actual_schedule_json=[];},
  h=>h.authority.origin='AGENCY_INVOICE_CORRECTION',h=>h.authority.original_invoice_id=id(57),
  h=>delete h.authority.return_invoice_id,h=>delete h.input.frozenOriginal.manager_signed_witness]){
  const h=fixture();edit(h);await refuses(h.run);assert.equal(h.calls.length,0);
 }
});
test('immutable original clock/settings mismatch refuses rather than substituting current Rota facts',async()=>{
 for(const edit of [h=>h.ts.actual_schedule_json[0].start='21:00',h=>h.ts.settings_authority_json.values.timezone_id='UTC']){
  const h=fixture();edit(h);await refuses(h.run);assert.equal(h.calls.length,0);
 }
});
test('native producer factory supplies server-owned original purpose and exact PREPARE scope to actual clock arm',async()=>{
 const h=fixture(),context={effective_timesheet_id:I.original,out_timesheet:h.ts,out_cur_fin:null,
  out_candidate:{id:I.candidate},out_client_id:I.client,out_effective_flags:h.input.effFlags,out_policy:h.input.policySql};
 const deps={env:{},readContext:async()=>[context],readWeeklyContext:async()=>[{timesheet_id:I.original,out_cw:h.cw,out_contract:h.contract}],
  readShifts:async()=>[{timesheet_id:I.original,shifts:[]}],readWeeklyValidation:async()=>[],resolveRates:async()=>[],
  buildWeekly:input=>buildExactRequestA0WeeklyClockSnapshot(input,h.owners),deriveDaily:()=>{},buildDaily:()=>{},
  toLocalParts:()=>{},subtractBreak:()=>{},classifyMinutes:()=>{},resolveEffectivePayChannel:()=>({ok:true}),anyMissingRates:()=>false};
 const prepare={candidate_id:I.candidate,client_id:I.client,authority:h.authority,frozen_original:h.input.frozenOriginal,
  original_materialisation_required:true,original_authorisation_required:true,original_current_snapshot:null};
 const result=await createNativeCorrectionSnapshotProducers(deps).buildOriginalSnapshot({prepare,actor:{environment:'TEST',actor_user_id:I.actor}});
 assert.equal(result.has_rate_issue,false);assert.equal(result.has_pay_channel_issue,false);assert.deepEqual(h.calls.map(c=>c[0]),['schedule','enrich']);
});
test('complete SQL scope reuses actual native activation/settings and absence owners; optional scope is in PREPARE CAS',()=>{
 const folder=new URL('../docs/healthroster-build/proposals/cn-a0-native-normalisation/',import.meta.url);
 const scope=fs.readFileSync(new URL('13102026_2002_a0_clock_scope.sqlinc',folder),'utf8');
 const approval=fs.readFileSync(new URL('13102026_1127_approval.complete-proposed.sqlinc',folder),'utf8');
 assert.match(scope,/private\.weekly_source_signed_check_only_v1\(v_ts.timesheet_id\) is distinct from true/);
 assert.match(scope,/private\._timesheet_settings_authority_frozen_v1\(v_ts.timesheet_id\)/);
 assert.match(scope,/exists\(select 1 from public\.timesheets_financials f where f.timesheet_id=v_ts.timesheet_id\)/);
 assert.match(scope,/a.feature_enabled or a.feature_release_id is not null or a.feature_changed_at_utc is not null/);
 assert.match(scope,/rel.status='VERIFIED'/);assert.match(scope,/gc.client_id=v_request.client_id/);
 assert.match(scope,/revoke all on function private.timesheet_correction_a0_clock_scope_v1\(uuid\) from public,anon,authenticated,service_role/);
 assert.doesNotMatch(scope,/pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
 assert.ok(approval.indexOf("'original_a0_clock_scope'")<approval.indexOf('v_hash:=encode(extensions.digest(v_context::text'));
 assert.match(approval,/v_prepare:=public.timesheet_correction_prepare_v1\(p_request_id,p_actor_user_id\)/);
});
