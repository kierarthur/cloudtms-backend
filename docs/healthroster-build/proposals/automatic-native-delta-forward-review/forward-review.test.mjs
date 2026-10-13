import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { compareCompleteNativeComponents } from './reference-comparison.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),prior=path.resolve(here,'../automatic-native-delta');
const lane=path.resolve(here,'../../../..');
const read=file=>fs.readFileSync(path.join(here,file),'utf8');
const component={component_id:'00000000-0000-4000-8000-000000000001',component_kind:'ADDITIONAL_UNIT',
  economic_key_type:'ADDITIONAL_CODE',economic_key_value:'ONCALL',component_member_identity:'additional:family:ONCALL',
  segment_id:null,segment_key:null,segment_stable_key:null,work_date:null,reference_number:null,
  additional_code_raw:'ONCALL',expense_code:null,exclude_from_pay:false,origin:'TIMESHEET_ADDITIONAL',movement_id:null,movement_group_id:null,
  hours_day:null,hours_night:null,hours_sat:null,hours_sun:null,hours_bh:null,unit_count:'1.000000',unit_pay_rate:'10.000000',
  unit_charge_rate:'20.000000',pay_ex_vat:'10.00',charge_ex_vat:'20.00'};
test('an independent segment_stable_key drift refuses even with all other identity and money unchanged',()=>{
  const before={...component,segment_stable_key:'saved-part-a'},next={...before,segment_stable_key:'saved-part-b'};
  assert.throws(()=>compareCompleteNativeComponents([before],[next],new Map()),e=>e.code==='COMPONENT_IDENTITY_CHANGED');
});
test('a missing stable key refuses; explicit same null remains valid native closure',()=>{
  assert.equal(compareCompleteNativeComponents([component],[structuredClone(component)],new Map()).component_count,1);
  const next={...component};delete next.segment_stable_key;
  assert.throws(()=>compareCompleteNativeComponents([component],[next],new Map()),e=>e.code==='INCOMPLETE_NATIVE_IDENTITY');
});
test('complete forward SQL changes exactly one identity key and retains owner/ACL/all guards',()=>{
  const sql=read('08_complete_automatic_reconcile_delta_forward.sqlinc');
  assert.equal(sql.replace("'segment_id','segment_key','segment_stable_key',",()=>"'segment_id','segment_key',"),fs.readFileSync(path.join(prior,'08_automatic_reconcile_delta.sqlinc'),'utf8'));
  assert.match(sql,/stable security definer/);assert.match(sql,/from public,anon,authenticated,service_role/);
  assert.match(sql,/v_cas is distinct from v_proof->'source_cas'/);
});
test('full current Source CAS excludes every direct own-stage write relation without a normalization waiver',()=>{
  const census=JSON.parse(read('SOURCE_CAS_STAGE_CENSUS.json'));
  assert.deepEqual(census.direct_relation_intersection,[]);assert.ok(census.stage_direct_write_relations.length>=10);
  assert.equal(census.facts_categories.length,39);assert.equal(new Set(census.facts_categories).size,39);
  assert.equal(census.money_callee_census.reachable_owner_count,202);assert.equal(census.money_callee_census.source_write_candidates.length,6);
  for(const category of ['report_bindings','economics','expense_authorities','invoice_hold_releases','financial_identities','root_identities'])assert.ok(census.facts_categories.includes(category),category);
  assert.equal(census.runtime_equality,'HELD_ROOT_NATIVE_C03');
});
test('all original and controlling bytes remain pinned; no frozen file is rewritten',()=>{
  for(const p of JSON.parse(read('CONTROLLING_INPUTS.json')).inputs){const base=p.authority==='FROZEN_AUTOMATIC23'?prior:p.authority.startsWith('CURRENT_ROOT')?path.resolve(lane,'../healthroster-v2-backend-13102026'):path.resolve(lane,'../healthroster-v2-money-build-13102026');
    const bytes=fs.readFileSync(path.join(base,p.path));assert.equal(bytes.length,p.bytes,p.path);assert.equal(crypto.createHash('sha256').update(bytes).digest('hex'),p.sha256,p.path);}
});
