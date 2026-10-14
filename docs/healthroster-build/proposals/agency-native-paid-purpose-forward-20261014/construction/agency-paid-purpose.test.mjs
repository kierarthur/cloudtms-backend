import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import assert from 'node:assert/strict';
import test from 'node:test';
import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url));
const sql=fs.readFileSync(path.join(dir,'28_agency_original_pay_identity.sqlinc'),'utf8');
const purpose=fs.readFileSync(path.join(dir,'29_agency_paid_write_purpose.sqlinc'),'utf8');
const authority=fs.readFileSync(path.join(dir,'31_complete_native_paid_financial_authority.sqlinc'),'utf8');
const sha=x=>crypto.createHash('sha256').update(x).digest('hex');
const canonical=x=>Array.isArray(x)?x.map(canonical):x&&typeof x==='object'
 ?Object.fromEntries(Object.entries(x).sort(([a],[b])=>a.localeCompare(b)).map(([k,v])=>[k,canonical(v)])):x;
const hash=x=>sha(JSON.stringify(canonical(x)));
const parseArray=s=>[...s.matchAll(/'([^']+)'/g)].map(x=>x[1]);
const deleted=label=>parseArray(sql.match(new RegExp(`${label}:=to_jsonb\\([^)]*\\)-array\\[([\\s\\S]*?)\\];`))[1]);
// UNIT_ONLY identity counterexamples from literal source projections. These
// exercise preservation requirements; they do not execute SQL or certify cash.
const financialRemoved=deleted('financial_identity'),timesheetRemoved=deleted('signed_identity');
function projection(row,removed,timestamps){const out=structuredClone(row);for(const k of removed)delete out[k];
 for(const key of timestamps)if(out[key]!==null&&out[key]!==undefined)out[key]=Date.parse(out[key])/1000;
 if(out.invoice_breakdown_json?.segments)out.invoice_breakdown_json.segments=out.invoice_breakdown_json.segments.map(s=>{
  const copy={...s};delete copy.invoice_locked_invoice_id;return copy;});return out;}
const financial={id:'original-f',timesheet_id:'T0',timesheet_version:1,is_current:true,
 paid_at_utc:'2026-10-10T09:00:00+01:00',computed_at_utc:'2026-10-07T12:00:00Z',updated_at:'2026-10-07T12:00:00Z',
 processing_status:'READY_FOR_INVOICE',authorised_by_user_id:'A',authorised_at_utc:'2026-10-07T13:00:00+01:00',
 locked_by_invoice_id:'D1',locked_at_utc:'2026-10-08T09:00:00Z',total_pay_ex_vat:100,
 policy_snapshot_json:{policy:'original'},invoice_breakdown_json:{mode:'SEGMENTS',segments:[
  {segment_id:'native-original',work_event_id:'event',exclude_from_pay:false,total_pay_ex_vat:100,invoice_locked_invoice_id:'D1'}]}};
const ts={timesheet_id:'T0',version:1,is_current:true,contract_id:'C',booking_id:'original',
 actual_schedule_json:[{date:'2026-10-07',start:'09:00',end:'17:00'}],authorised_at_server:'2026-10-07T12:00:00Z',
 revoked_at:null,revoked_reason:null,revoked_by:null,updated_at:'2026-10-07T12:00:00Z',
 qr_state:'USED',qr_scanned_at:'2026-10-07T13:00:00+01:00',signed_hash:'signed-original'};
test('native D1 removal/UNA/current retirement preserve economics and paid marker identity',()=>{
 const next=structuredClone(financial);Object.assign(next,{is_current:false,updated_at:'2026-10-14T12:00:00Z',
  processing_status:'PENDING_AUTH',authorised_by_user_id:null,authorised_at_utc:null,locked_by_invoice_id:null,locked_at_utc:null});
 next.invoice_breakdown_json.segments[0].invoice_locked_invoice_id=null;
 assert.equal(hash(projection(financial,financialRemoved,['paid_at_utc','computed_at_utc'])),
  hash(projection(next,financialRemoved,['paid_at_utc','computed_at_utc'])));
 assert.ok(!financialRemoved.includes('paid_at_utc'));
});
for(const [label,mutate] of [
 ['paid marker cleared',x=>x.paid_at_utc=null],['economic amount rotated',x=>x.total_pay_ex_vat=90],
 ['policy replaced',x=>x.policy_snapshot_json={policy:'foreign'}],
 ['segment amount replaced',x=>x.invoice_breakdown_json.segments[0].total_pay_ex_vat=90],
 ['segment source identity replaced',x=>x.invoice_breakdown_json.segments[0].work_event_id='foreign'],
 ['exclude flag silently changed',x=>x.invoice_breakdown_json.segments[0].exclude_from_pay=true]]){
 test(`Original identity refuses ${label}`,()=>{const next=structuredClone(financial);mutate(next);
  assert.notEqual(hash(projection(financial,financialRemoved,['paid_at_utc','computed_at_utc'])),
   hash(projection(next,financialRemoved,['paid_at_utc','computed_at_utc'])));});
}
test('same T0 Source schedule and genuine UNA changes preserve opaque signed identity',()=>{
 const next=structuredClone(ts);Object.assign(next,{actual_schedule_json:[],updated_at:'2026-10-14T12:00:00Z',
  authorised_at_server:null,revoked_at:'2026-10-14T12:00:00Z',revoked_reason:'TIMESHEET_UNAUTHORISE',revoked_by:'A'});
 assert.equal(hash(projection(ts,timesheetRemoved,['qr_scanned_at'])),hash(projection(next,timesheetRemoved,['qr_scanned_at'])));
});
for(const [label,key,value] of [['signature','signed_hash','foreign'],['QR','qr_state','UNUSED'],
 ['physical version','version',2],['physical target','timesheet_id','foreign'],['booking','booking_id','foreign']]){
 test(`signed identity refuses ${label} change`,()=>{const next={...ts,[key]:value};
  assert.notEqual(hash(projection(ts,timesheetRemoved,['qr_scanned_at'])),hash(projection(next,timesheetRemoved,['qr_scanned_at'])));});
}
test('equivalent native timestamp instants survive a different session timezone',()=>{
 const next={...financial,paid_at_utc:'2026-10-10T08:00:00Z'};
 assert.equal(hash(projection(financial,financialRemoved,['paid_at_utc','computed_at_utc'])),
  hash(projection(next,financialRemoved,['paid_at_utc','computed_at_utc'])));
 assert.match(sql,/column_type\.attrelid='public\.timesheets_financials'::regclass/);
 assert.match(sql,/column_type\.atttypid='pg_catalog\.timestamptz'::regtype/);
 assert.doesNotMatch(sql,/set_config\(/);
});
test('paid purpose has literal native fence with no invented lease and does not write ledgers',()=>{
 assert.match(sql,/bpay_next_assert_candidate_financial_fence_v1\(\s*\(scope->>'candidate_id'\)::uuid,null::uuid,null::uuid,null::bigint\)/);
 assert.match(sql,/from private\.bpay_next_original_reservation_v1\(transfer\.run_worker_id,transfer\.id\) native/);
 assert.doesNotMatch(sql+purpose,/\b(?:insert into|update|delete from)\s+(?:private\.bpay_next_|public\.timesheets)/i);
 assert.match(purpose,/AGENCY_CHANGE_ORIGINAL_PAY_IDENTITY_CHANGED/);
 assert.doesNotMatch(authority,/AGENCY_CHANGE_PAID_WRITER_PRODUCER_UNIMPLEMENTED/);
 assert.equal((authority.match(/weekly_source_agency_paid_write_purpose_v1\(r\.id\)/g)||[]).length,2);
});
test('uncertain/posting/unbound reservation is refused instead of zero or caller completeness',()=>{
 const states=parseArray(purpose.match(/reservation_state}' not in\(([\s\S]*?)\) then/)[1]);
 assert.deepEqual(states,['POSTED','RETURNED','CLOSED_INTERNAL','RESERVED','INTERNAL_RESERVED','ISSUED_AWAITING_RECEIPT']);
 for(const state of ['EXECUTION_UNCERTAIN','EXECUTED_PENDING_POSTING','INTERNAL_PENDING_POSTING','EVIDENCE_CONFLICT',null])assert.equal(states.includes(state),false);
 assert.doesNotMatch(purpose,/native_collection_observation.*complete.*true/);
 assert.match(authority,/original_submission_proved'='false'::jsonb/);
 assert.match(authority,/original_submission_sha256' is distinct from 'null'::jsonb/);
});
test('two exact composite-native reservation codec reversals preserve both scope owners',()=>{
 const receipt=JSON.parse(fs.readFileSync(path.join(dir,'NATIVE_RESERVATION_TABLE_JSON_FULL_REVERSE.json')));
 for(const r of receipt.full_forward_files){let s=fs.readFileSync(path.join(dir,r.after_path),'utf8');
  assert.match(s,/cross join lateral private\.bpay_next_original_reservation_v1/);
  assert.match(s,/select to_jsonb\(native\) into reservation/);
  for(const h of [...r.hunks].reverse()){assert.equal(s.split(h.after).length,2);s=s.replace(h.after,()=>h.before);}
  assert.equal(s,fs.readFileSync(path.join(dir,r.before_path),'utf8'));}
});
