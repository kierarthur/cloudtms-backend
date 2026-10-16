import assert from 'node:assert/strict';
import test from 'node:test';
import {decodeSourceNativeCalculationSnapshot,SOURCE_NATIVE_SNAPSHOT_KEYS} from '../../broker/src/weekly-source/source-native-snapshot.mjs';
import {nativeSnapshotFixture,nativeSegmentFixture} from './helpers/source-native-snapshot-fixture.mjs';
const id=n=>`97000000-0000-4000-8000-${String(n).padStart(12,'0')}`;
const current=()=>({root_timesheet_id:id(1),timesheet:{version:1},candidate_id:id(2),client_id:id(3),contract_id:id(4),policy:{rate:1}});
const snapshot=()=>nativeSnapshotFixture({timesheet_id:id(1),candidate_id:id(2),client_id:id(3),contract_id:id(4),policy:{rate:1}});
const reject=value=>assert.throws(()=>decodeSourceNativeCalculationSnapshot(value,{current:current()}),{code:'SOURCE_NATIVE_ECONOMIC_SNAPSHOT_INVALID'});

for(const key of SOURCE_NATIVE_SNAPSHOT_KEYS)test(`Native snapshot requires actual calculator field ${key}`,()=>{
 const value=snapshot();delete value[key];reject(value);
});
test('snapshot preserves all native values and exact policy/rate refs without a financial calculation or normalization',()=>{
 const value=snapshot();value.mileage_pay_rate='1.00';value.expenses_evidence_manifest={version:'existing-native-evidence',receipts:[{key:'unchanged'}]};
 value.hr_crosscheck_status=null;value.hr_crosscheck_issues=null;value.external_source_rows_json={};
 const before=structuredClone(value);assert.equal(decodeSourceNativeCalculationSnapshot(value,{current:current()}),value);assert.deepEqual(value,before);
 for(const mutate of [v=>v.policy_snapshot_json.rate='1.00',v=>v.rate_source_refs_json.mode='guessed',v=>v.rate_source_refs_json.contract_id=id(9),
  v=>v.policy_snapshot_json.extra=true,v=>v.rate_source_refs_json.extra=true,v=>v.hours_day='0.00',v=>v.pay_day=Infinity,
  v=>v.total_pay_ex_vat=null,v=>v.invoice_breakdown_json.totals.total_pay_ex_vat=1,v=>v.native_receipt_id=id(9),
  v=>v.hr_crosscheck_issues={},v=>v.external_source_rows_json=[],v=>v.mileage_pay_rate='',v=>v.expenses_evidence_manifest='opaque authority']){
  const changed=snapshot();mutate(changed);reject(changed);
 }
});
test('complete native segment/identity/break/metadata children refuse extras, wrong owners and malformed values',()=>{
 const original=snapshot();original.invoice_breakdown_json.segments=[nativeSegmentFixture(id(5))];
 assert.equal(decodeSourceNativeCalculationSnapshot(original,{current:current()}),original);
 for(const mutate of [s=>delete s.hours_bh,s=>s.hours_day='7.50',s=>s.segment_id='guessed',s=>s.weekly_protected_component_identity.source_component_id=id(9),
  s=>s.weekly_protected_component_identity.work_event_id='bad',s=>s.weekly_protected_component_identity.schema_version='bad',
  s=>s.date='2026-02-30',s=>s.start_utc='not-an-instant',s=>s.exclude_from_pay='false',s=>s.break_mins=-1,
  s=>s.breaks=[{start:'25:00',end:'25:30'}],s=>s.breaks=[{start:'12:00',end:'12:30',bank_receipt:id(9)}],
  s=>s.pay_amount=NaN,s=>s.invoice_locked_invoice_id=null,s=>s.invoice_target_week_start='2026-99-99',s=>s.unreviewed_rate=100]){
  const value=structuredClone(original);mutate(value.invoice_breakdown_json.segments[0]);reject(value);
 }
 const duplicate=structuredClone(original);duplicate.invoice_breakdown_json.segments.push(duplicate.invoice_breakdown_json.segments[0]);reject(duplicate);
});
test('NEXT facts are closed and genuine WAIT proof is required while imported-source previews cannot claim it',()=>{
 const value=snapshot();const segment=nativeSegmentFixture(id(5));delete segment.weekly_protected_component_identity;
 const buckets=Object.fromEntries(['day','night','sat','sun','bh'].map(k=>[k,'0.000000']));
 Object.assign(segment,{work_event_id:id(5),protected_target_state:'WAIT',protected_next_preview:{schema_version:'WEEKLY_PROTECTED_NEXT_PREVIEW_V1',
  calculator_owner:'buildWeeklyScheduleSegmentsSnapshot',calculator_version:'WEEKLY_SHIFT_FINANCIAL_SEGMENT_V1',mode:'SPLIT_RATE_WINDOWS',category:null,sign:1,
  bucket_minutes:{day:0,night:0,sat:0,sun:0,bh:0},paid_minutes:0,break_minutes:0,hours:buckets,rates:buckets,charge_rates:buckets,pay_ex_vat:'0.00',charge_ex_vat:'0.00'}});
 value.invoice_breakdown_json.segments=[segment];assert.equal(decodeSourceNativeCalculationSnapshot(value,{current:current(),nextRoute:true}),value);
 for(const mutate of [s=>delete s.protected_next_preview,s=>s.protected_next_preview.quoted_money=1,s=>s.protected_next_preview.hours.day=1,
  s=>s.protected_next_preview.rates.extra='1',s=>s.protected_next_preview.calculator_owner='other',s=>s.protected_target_state='SOURCE']){
  const changed=structuredClone(value);mutate(changed.invoice_breakdown_json.segments[0]);
  assert.throws(()=>decodeSourceNativeCalculationSnapshot(changed,{current:current(),nextRoute:true}),{code:'SOURCE_NATIVE_ECONOMIC_SNAPSHOT_INVALID'});
 }
 segment.protected_target_state='SOURCE';delete segment.protected_next_preview;
 assert.equal(decodeSourceNativeCalculationSnapshot(value,{current:current(),nextRoute:true}),value);
});
