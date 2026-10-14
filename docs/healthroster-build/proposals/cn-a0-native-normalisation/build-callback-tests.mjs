import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..');
const file=path.join(repo,'tests/healthroster-f4-ai-correction-wire.test.mjs');
const input=fs.readFileSync(file);
assert.equal(crypto.createHash('sha256').update(input).digest('hex'),'094fcead27788b3798de9a38e589b1506a9a7bf83b7e57fa0b57754bc7441c91','frozen retained CN fixture suite changed');
let s=input.toString('utf8').replace(/\r\n/g,'\n');
function replace(a,b){assert.equal(s.split(a).length,2,a);s=s.replace(a,b);}
replace("from '../docs/healthroster-build/proposals/f4-ai/timesheet-correction-orchestrator.mjs'", "from '../docs/healthroster-build/proposals/cn-a0-native-normalisation/timesheet-correction-orchestrator.mjs'");
replace("state_revision: 2, prepare_fingerprint: H, original_authorisation_required: false,", "state_revision: 2, prepare_fingerprint: H, original_authorisation_required: false, original_materialised_in_unit: false,");
replace(`test('missing A0 finance holds before any producer or mutation; calculator output is never assumed to equal stored native output', async () => {
  const p = prepareFixture(); p.original_materialisation_required = true; p.original_authorisation_required = true;
  p.original_current_snapshot = null; p.authority.original_financial_id = null; p.authority.original_financial_authority_hash = null;
  const { adapter, calls } = harness({ prepare: p });
  await rejects(() => adapter.authorise(action(), {}), 'CORRECTION_ORIGINAL_NORMALISATION_UNPROVED');
  assert.deepEqual(calls.map(x => x[0]), ['actor', 'timesheet_correction_prepare_v1']);
});`,
`function missingPrepare() {
  const p=prepareFixture();p.original_materialisation_required=true;p.original_authorisation_required=true;
  p.original_current_snapshot=null;p.authority.original_financial_id=null;p.authority.original_financial_authority_hash=null;
  p.frozen_original.financial_snapshot=null;p.frozen_original.financial_authority_hash=null;
  p.frozen_original.timesheet.authorised_at_server=null;return p;
}
function materialisedReceipt(overrides={}) {
  return appliedFixture({original_materialised_in_unit:true,original_authorisation_required:true,
    original_financial_id:id(23),original_financial_authority_hash:'c'.repeat(64),
    authorisation:{ok:true,all_success:true,success_count:3,failure_count:0,
      results:[I.original,I.r0,I.p0].map(current_timesheet_id=>({current_timesheet_id,action:'AUTHORISE',success:true}))},...overrides});
}
function originalInput() {
  const s=replacementFixture();s.timesheet_id=I.original;s.timesheet_version=7;delete s.is_stale;return s;
}
test('missing A0 finance without native original producer holds before P0 or any mutation', async () => {
  const {adapter,calls}=harness({prepare:missingPrepare()});
  await rejects(()=>adapter.authorise(action(),{}),'CORRECTION_CANONICAL_ORIGINAL_PRODUCER_UNAVAILABLE');
  assert.deepEqual(calls.map(x=>x[0]),['actor','timesheet_correction_prepare_v1']);
});
test('missing A0 supplies exact original/P0 inputs once; SQL owns stored normalisation and R0 sign-copy',async()=>{
  const events=[];const p=missingPrepare();const original=originalInput();
  const h=harness({prepare:p,applied:materialisedReceipt(),dependencies:{buildOriginalSnapshot:async ({prepare})=>{
    assert.equal(prepare.original_current_snapshot,null);events.push('original');return original;
  }}});
  const result=await h.adapter.authorise(action(),{});assert.deepEqual(events,['original']);
  const args=h.calls.at(-1)[1];assert.deepEqual(args.p_prepared.original_snapshot,original);
  assert.equal(Object.hasOwn(args.p_prepared,'reversal_snapshot'),false);
  assert.equal(Object.hasOwn(args.p_prepared.original_snapshot,'is_stale'),false);
  assert.equal(result.original_financial_id,id(23));assert.equal(result.original_financial_authority_hash,'c'.repeat(64));
  assert.equal(result.original_materialised_in_unit,true);assert.equal(result.authorisation.results.length,3);
  assert.equal(h.calls.filter(x=>x[0]==='timesheet_correction_authorise_atomic_v1').length,1);
  assert.equal(h.calls.some(x=>/enqueue|dequeue|write|bulk|notification/.test(x[0])),false);
});
test('missing A0 producer mutation, identity, issue and explicit stale failures stop before atomic approval',async()=>{
  for(const alter of [s=>{s.timesheet_id=I.p0;},s=>{s.has_rate_issue=true;},s=>{s.has_pay_channel_issue=true;},
    s=>{s.is_stale=true;},s=>{s.is_stale=null;}]){
    const s=originalInput();alter(s);const h=harness({prepare:missingPrepare(),dependencies:{buildOriginalSnapshot:async()=>s}});
    await rejects(()=>h.adapter.authorise(action(),{}),'CORRECTION_CANONICAL_SNAPSHOT_INVALID');
    assert.equal(h.calls.some(x=>x[0]==='timesheet_correction_authorise_atomic_v1'),false);
  }
});
test('missing A0 refuses contradictory nullable PREPARE facts rather than masking missing finance',async()=>{
  for(const alter of [p=>{p.authority.original_financial_authority_hash=H;},p=>{p.frozen_original.financial_snapshot=finance();},
    p=>{p.frozen_original.financial_authority_hash=FH;},p=>{p.frozen_original.timesheet.authorised_at_server='2026-10-13T12:00:00Z';}]){
    const p=missingPrepare();alter(p);await rejects(()=>harness({prepare:p}).adapter.authorise(action(),{}),'CORRECTION_PREPARE_AUTHORITY_INVALID');
  }
});
test('fresh materialisation fact and complete three-member approval are required on A0 APPLIED',async()=>{
  for(const patch of [{original_materialised_in_unit:false},{original_materialised_in_unit:undefined},
    {original_authorisation_required:false},{original_financial_id:I.rFin},
    {authorisation:{ok:true,all_success:true,success_count:2,failure_count:0,results:[I.r0,I.p0].map(current_timesheet_id=>({current_timesheet_id,action:'AUTHORISE',success:true}))}}]){
    const h=harness({prepare:missingPrepare(),applied:materialisedReceipt(patch),dependencies:{buildOriginalSnapshot:async()=>originalInput()}});
    await assert.rejects(()=>h.adapter.authorise(action(),{}),e=>e instanceof CorrectionOrchestrationError);
  }
});
test('existing Original never calls new original calculator and still fences original stored hash',async()=>{
  const h=harness({dependencies:{buildOriginalSnapshot:()=>assert.fail('Existing Original must not recalculate')}});
  await h.adapter.authorise(action(),{});
  await rejects(()=>harness({applied:appliedFixture({original_financial_authority_hash:H})}).adapter.authorise(action(),{}),'CORRECTION_RECEIPT_IDENTITY_MISMATCH');
  await rejects(()=>harness({applied:appliedFixture({original_materialised_in_unit:true})}).adapter.authorise(action(),{}),'CORRECTION_RECEIPT_IDENTITY_MISMATCH');
});`);
fs.writeFileSync(path.join(repo,'tests/healthroster-cn-a0-callbacks.test.mjs'),s);
console.log('Retained37 callback suite plus A0 actual-wire cases generated');
