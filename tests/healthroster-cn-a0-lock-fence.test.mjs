import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';
import {createNativeCorrectionSnapshotProducers} from '../docs/healthroster-build/proposals/cn-a0-lock-fence/native-correction-snapshot-producers.mjs';
const id=n=>`00000000-0000-4000-8000-${String(n).padStart(12,'0')}`,copy=x=>JSON.parse(JSON.stringify(x));
const folder=new URL('../docs/healthroster-build/proposals/cn-a0-lock-fence/',import.meta.url);
function fixture({original=true,materialise=true}={}){
 const ids={original:id(1),p0:id(2),candidate:id(3),client:id(4),contract:id(5),request:id(6),actor:id(7),root:id(8)},tsid=original?ids.original:ids.p0;
 const ts={timesheet_id:tsid,version:2,booking_id:original?'actual-business-TEXT':'exact-P0-TEXT',sheet_scope:'WEEKLY',
  contract_id:ids.contract,is_current:true,archived_at_utc:null,authorised_at_server:null,
  actual_schedule_json:[{date:'2026-10-12',start:'08:00',end:'16:00'}],settings_authority_json:{values:{}},
  ...(original?{}:{parent_timesheet_id:ids.root,correction_id:`awc:${ids.root}:${ids.request}`,correction_kind:'WHOLE_TIMESHEET_REPLACEMENT'})};
 const ctx={effective_timesheet_id:tsid,out_timesheet:copy(ts),out_cur_fin:null,out_candidate:{id:ids.candidate,pay_method:'PAYE',sort_code:'00-00-00'},
  out_client_id:ids.client,out_effective_flags:{route_type:'WEEKLY_HEALTHROSTER'},out_policy:{erni_pct:'13.8'}};
 const second=copy(ctx);second.effective_timesheet_id=original?ids.p0:ids.original;second.out_timesheet.timesheet_id=second.effective_timesheet_id;
 const weekly={timesheet_id:tsid,out_cw:{timesheet_id:tsid,contract_id:ids.contract},
  out_contract:{id:ids.contract,candidate_id:ids.candidate,client_id:ids.client,additional_rates_json:[]}};
 const secondWeekly=copy(weekly);secondWeekly.timesheet_id=second.effective_timesheet_id;secondWeekly.out_cw.timesheet_id=second.effective_timesheet_id;
 const shifts={timesheet_id:tsid,shifts:[]},validations=[];
 const p={candidate_id:ids.candidate,client_id:ids.client,original_materialisation_required:materialise,original_authorisation_required:true,
  original_current_snapshot:null,authority:{request_id:ids.request,root_timesheet_id:ids.root,original_timesheet_id:ids.original,original_timesheet_version:2,
   original_financial_id:null,original_financial_authority_hash:null,replacement_timesheet_id:ids.p0,replacement_version:2,replacement_financial_id:null,
   replacement_schedule:copy(ts.actual_schedule_json),original_a0_native_input_witness:{contract:'TIMESHEET_CORRECTION_A0_NATIVE_INPUT_WITNESS_V1',
    request_id:ids.request,contexts:[copy(ctx),second],weekly_contexts:[copy(weekly),secondWeekly],
    import_contexts:[copy(shifts),{timesheet_id:second.effective_timesheet_id,shifts:[]}],
    validations:[{timesheet_id:tsid,rows:[]},{timesheet_id:second.effective_timesheet_id,rows:[]}],sha256:'a'.repeat(64)}},
  frozen_original:{timesheet:copy(ts)},replacement_timesheet:copy(ts)};
 let calculatorCalls=0;
 const deps={readContext:async()=>[ctx],readWeeklyContext:async()=>[weekly],
  readShifts:async()=>[shifts],readWeeklyValidation:async()=>validations,resolveRates:async()=>[],
  buildWeekly:async()=>{calculatorCalls++;return {timesheet_id:tsid,timesheet_version:2,candidate_id:ids.candidate,client_id:ids.client,
   hours_day:8,hours_night:0,hours_sat:0,hours_sun:0,hours_bh:0,pay_day:'20.00',charge_day:'30.00',additional_units_json:{}};},
  deriveDaily:()=>{},buildDaily:()=>{},toLocalParts:()=>{},subtractBreak:()=>{},classifyMinutes:()=>{},
  resolveEffectivePayChannel:()=>({ok:true}),anyMissingRates:()=>false};
 return {ids,ctx,weekly,shifts,validations,p,deps,calls:()=>calculatorCalls,run:()=>createNativeCorrectionSnapshotProducers(deps)[original?'buildOriginalSnapshot':'buildReplacementSnapshot'](
  {prepare:p,actor:{environment:'TEST',actor_user_id:ids.actor}})};
}
test('exact actual normal-reader contexts bind both original and P0 producer input before calculation',async()=>{
 for(const original of [true,false]){const h=fixture({original});const s=await h.run();assert.equal(h.calls(),1);assert.equal(s.timesheet_id,original?h.ids.original:h.ids.p0);}
});
test('CW, rates, additional configuration, imported rows and validation drift refuse before calculation',async()=>{
 for(const change of [h=>h.weekly.out_cw.additional_seq=1,h=>h.weekly.out_contract.pay_rates_json={day:'21.00'},
  h=>h.weekly.out_contract.additional_rates_json=[{code:'ACTUAL',pay_rate:'20.00',charge_rate:'30.00'}],
  h=>h.shifts.shifts.push({id:id(30),timesheet_id:h.ids.original,work_date:'2026-10-12'}),
  h=>h.validations.push({timesheet_id:h.ids.original,status:'VALIDATED',reason_code:null,validated_at_utc:null,updated_at:'2026-10-13T09:00:00+00:00'})]){
  const h=fixture();change(h);await assert.rejects(h.run,e=>e.code==='CORRECTION_A0_NATIVE_INPUT_CHANGED');assert.equal(h.calls(),0);
 }
});
test('Candidate/channel/policy/flags/current-finance drift refuses even if identity and clocks match',async()=>{
 for(const change of [h=>h.ctx.out_candidate.pay_method='UMBRELLA',h=>h.ctx.out_candidate.sort_code='11-11-11',
  h=>h.ctx.out_policy.erni_pct='15.0',h=>h.ctx.out_effective_flags.route_type='WEEKLY_MANUAL',h=>h.ctx.out_cur_fin={id:id(50)}]){
  const h=fixture();change(h);await assert.rejects(h.run,e=>e.code==='CORRECTION_A0_NATIVE_INPUT_CHANGED');assert.equal(h.calls(),0);
 }
});
test('missing, foreign, duplicate or client-like partial witness never becomes a usable lock/input proof',async()=>{
 for(const change of [h=>delete h.p.authority.original_a0_native_input_witness,h=>h.p.authority.original_a0_native_input_witness.request_id=id(40),
  h=>h.p.authority.original_a0_native_input_witness.contexts=[h.ctx,h.ctx],h=>h.p.authority.original_a0_native_input_witness.ready=true,
  h=>h.p.authority.original_a0_native_input_witness.contexts[0]={effective_timesheet_id:h.ids.original}]){
  const h=fixture();change(h);await assert.rejects(h.run);assert.equal(h.calls(),0);
 }
});
test('existing-finance P0 callback does not require the new missing-A0 witness',async()=>{
 const h=fixture({original:false,materialise:false});delete h.p.authority.original_a0_native_input_witness;await h.run();assert.equal(h.calls(),1);
});
test('complete owner has immutable dispatch and native family acquisition before every original business lock',()=>{
 const sql=fs.readFileSync(new URL('13102026_2034_approval.complete-proposed.sqlinc',folder),'utf8');
 const body=sql.slice(sql.indexOf('create or replace function public.timesheet_correction_authorise_atomic_v1'));
 assert.ok(body.indexOf('timesheet_correction_a0_native_lock_fence_v1')<body.indexOf('from public.invoices'));
 assert.match(body,/v_request.reversed_financial_id is null/);assert.match(body,/frozen_original_json#>>'\{timesheet,sheet_scope\}'='WEEKLY'/);
 assert.match(body,/v_prepare:=public.timesheet_correction_prepare_v1/);
 assert.match(sql,/'original_a0_native_input_witness',private.timesheet_correction_a0_native_input_witness_v1/);
 assert.match(sql,/timesheet_correction_native_original_reversal_v1\(v_original,v_r\)/);
});
test('native gate/I1 order, timeout restoration, try-chain and NOWAIT authority locks are explicit',()=>{
 const sql=fs.readFileSync(new URL('13102026_2034_a0_native_lock_fence.sqlinc',folder),'utf8');
 assert.ok(sql.indexOf('weekly_source_candidate_serial_gate_v1')<sql.indexOf('weekly_source_lock_family_rows_v1'));
 assert.ok(sql.indexOf('weekly_source_lock_family_rows_v1')<sql.indexOf('from public.weekly_source_groups g'));
 assert.match(sql,/'WORKBENCH_CANDIDATE_FIRST_AUTHORISATION'/);assert.match(sql,/set_config\('lock_timeout','1ms',true\)/);
 assert.equal((sql.match(/set_config\('lock_timeout',v_old_timeout,true\)/g)||[]).length,2);
 assert.match(sql,/pg_try_advisory_xact_lock\(hashtextextended\('CORRECTION_CHAIN\|'/);
 assert.equal((sql.match(/for (?:update|share) nowait/g)||[]).length,22);
 assert.doesNotMatch(sql,/LOCK TABLE|pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
});
test('fence includes inactive finance/import/expense history and exact native FK child INSERT/UPDATE checks',()=>{
 const sql=fs.readFileSync(new URL('13102026_2034_a0_native_lock_fence.sqlinc',folder),'utf8');
 for(const table of ['timesheets_financials','nhsp_shifts','candidate_expense_components','weekly_source_row_timesheet_lineages']){
  const line=sql.split('\n').find(l=>l.includes('perform 1 from public.'+table));assert.ok(line,table);
  assert.doesNotMatch(line,/is_current|cancelled_at_utc|removed_at_utc/,table);
 }
 assert.match(sql,/c.convalidated and not c.condeferrable/);assert.match(sql,/c.conkey=array/);assert.match(sql,/c.confkey=array/);
 assert.match(sql,/\(t.tgtype&4\)=4/);assert.match(sql,/\(t.tgtype&16\)=16/);
 assert.match(sql,/session_replication_role/);assert.match(sql,/v_groups_after is distinct from v_groups/);
});
test('unknown unowned native claims remain held instead of being normalized to zero',()=>{
 const sql=fs.readFileSync(new URL('13102026_2034_clock_scope.complete-proposed.sqlinc',folder),'utf8');
 assert.match(sql,/e.owning_timesheet_id is null and e.workflow_id in/);
 assert.match(sql,/w.target_timesheet_id in\(v_ts.timesheet_id,v_request.replacement_timesheet_id\)/);
});
