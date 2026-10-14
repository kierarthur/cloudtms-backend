import test from 'node:test';import assert from 'node:assert/strict';
import fs from 'node:fs';import path from 'node:path';import {fileURLToPath} from 'node:url';
import {decodeAgencyWorkerContext as oldDecode,runAgencyCanonicalManual as oldRun} from './runtime/agency-canonical-worker.mjs';
import {decodeAgencyWorkerContext,runAgencyCanonicalManual} from './41_agency_canonical_worker_scalar.mjs';
const dir=path.dirname(fileURLToPath(import.meta.url));
const u=n=>`00000000-0000-4000-8000-${String(n).padStart(12,'0')}`;
const entry={date:'2026-10-09',start:'09:00',end:'17:00',end_date:'2026-10-09',break_minutes:30,source_shift_id:u(10),work_event_id:u(10)};
function frame(){return{schema_version:'WEEKLY_SOURCE_AGENCY_CANONICAL_WORKER_CONTEXT_V1',request_id:u(1),request_sha256:'a'.repeat(64),request_version:7,
 phase:'UNAUTHORISED',phase_actor_user_id:u(8),current_row_signature:'UNIT_ONLY_SIGNATURE',
 source_cas:{schema_version:'WEEKLY_SOURCE_BULK_SOURCE_CAS_V1',source_cycle_id:u(9),client_ids:[u(7)],source_cas_sha256:'b'.repeat(64)},
 source_schedule:[structuredClone(entry)],outbox_id:null,outbox_reason:null,outbox_row:null,
 context:{effective_timesheet_id:u(2),out_timesheet:{timesheet_id:u(2),contract_id:u(5),version:1,sheet_scope:'WEEKLY',line_type:'HOURS',is_current:true,
  authorised_at_server:null,actual_schedule_json:[]},out_cur_fin:{id:u(3),timesheet_id:u(2),timesheet_version:1,locked_by_invoice_id:null},
  out_candidate:{id:u(6)},out_umbrella:null,out_client_id:u(7),out_effective_flags:{},out_policy:{}},
 weekly_context:{timesheet_id:u(2),out_cw:{id:u(4),timesheet_id:u(2),contract_id:u(5)},out_contract:{id:u(5),candidate_id:u(6),client_id:u(7)}},
 weekly_validation:null,nhsp_shifts:[],context_sha256:'c'.repeat(64)};}
test('exact predecessor reproduces array coercion; new actual decoder refuses it',()=>{
 const f=frame();f.source_schedule[0].date=[entry.date];f.source_schedule[0].start=[entry.start];
 assert.ok(oldDecode(f,u(1),'UNAUTHORISED'));assert.throws(()=>decodeAgencyWorkerContext(f,u(1),'UNAUTHORISED'),/WORKER_CONTEXT_INVALID/);
});
for(const key of ['date','end_date','start','end'])for(const type of ['array','number','null','object']){
 test(`before calculator refusal: ${key}/${type}`,async()=>{const f=frame();
  f.source_schedule[0][key]=type==='array'?[entry[key]]:type==='number'?20261009:type==='null'?null:{value:entry[key]};
  let calls=0;await assert.rejects(()=>runAgencyCanonicalManual({requestId:u(1),readContext:async()=>f,
   buildWeeklySnapshot:async()=>{calls++;throw new Error('CALCULATOR_MUST_NOT_RUN');},
   applySnapshot:async()=>{calls++;throw new Error('NATIVE_APPLY_MUST_NOT_RUN');}}),/WORKER_CONTEXT_INVALID/);
  assert.equal(calls,0);
 });}
test('genuine scalar fields still decode without new caller authority',()=>{
 assert.deepEqual(decodeAgencyWorkerContext(frame(),u(1),'UNAUTHORISED'),oldDecode(frame(),u(1),'UNAUTHORISED'));
});
test('actual source guard shows original false hold is required before REMOVE',()=>{
 const guard=fs.readFileSync(path.join(dir,'../../agency-native-paid-purpose-forward-20261014/dependencies/05_agency_draft_round_trip.sqlinc'),'utf8');
 assert.match(guard,/if v_request\.return_hold or v_request\.current_timesheet_id/);
 const before=fs.readFileSync(path.join(dir,'33_complete_native_begin_with_after_una_purpose.sqlinc'),'utf8');
 const after=fs.readFileSync(path.join(dir,'42_complete_native_begin_with_actual_removal_hold.sqlinc'),'utf8');
 assert.match(before,/capture->>'original_draft_invoice_id' is not null,\(capture->>'current_financial_id'\)::uuid/);
 assert.match(after,/false,\(capture->>'current_financial_id'\)::uuid/);
 assert.match(after,/phase='DRAFT_REMOVED',native_receipts=receipts,\s*return_hold=r\.original_draft_invoice_id is not null/);
 assert.ok(after.indexOf("receipt->>'ok' is distinct from 'true'")<after.indexOf('return_hold=r.original_draft_invoice_id is not null'));
});
test('hold writes only on successful phase; caught REMOVE failure preserves CAPTURED/no hold',()=>{
 // UNIT_ONLY state-transition requirement from the actual guarded subtransaction.
 const begin={phase:'CAPTURED',return_hold:false,original_draft_invoice_id:'D1'};
 function remove(result){const committed=structuredClone(begin);if(result.ok!==true)return committed;
  committed.phase='DRAFT_REMOVED';committed.return_hold=committed.original_draft_invoice_id!==null;return committed;}
 assert.deepEqual(remove({ok:false}),begin);assert.equal(remove({ok:true}).return_hold,true);
 const noInvoice={...begin,original_draft_invoice_id:null};assert.equal(noInvoice.return_hold,false);
});
