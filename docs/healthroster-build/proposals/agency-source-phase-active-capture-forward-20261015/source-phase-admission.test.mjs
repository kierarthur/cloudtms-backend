// UNIT_ONLY source / relational protocol cases. No PostgreSQL or native claim.
import fs from 'node:fs';import assert from 'node:assert/strict';import test from 'node:test';import crypto from 'node:crypto';
import {scanManagedSql} from './reference/packaging-parser.mjs';
const read=n=>fs.readFileSync(new URL(n,import.meta.url),'utf8');
const sha=x=>crypto.createHash('sha256').update(x).digest('hex');
const reverses=JSON.parse(read('COMPLETE_SOURCE_PHASE_REVERSES.json'));
test('all five complete current-owner reverses restore pinned raw inputs without modifying old capture or public ABI',()=>{
 assert.equal(reverses.reverses.length,5);assert.equal(reverses.reverses.reduce((n,x)=>n+x.hunks.length,0),16);
 for(const r of reverses.reverses){let s=read(r.output);assert.equal(sha(s),r.after_sha256);
  for(const h of [...r.hunks].reverse()){assert.equal(s.split(h.after).length,2);s=s.replace(h.after,()=>h.before);}
  assert.equal(s,read('before/'+r.output+'.txt'));assert.equal(sha(s),r.before_sha256);}
 assert.deepEqual(reverses.public_signature_changes,[]);
});
test('literal actual Candidate admission precedes invoice/family, with no later Candidate reacquisition or browser gate',()=>{
 for(const name of ['89_complete_native_capture_source_phase_admission.sqlinc','91_complete_current_request_source_phase_resume.sqlinc']){
  const s=read(name);assert.equal((s.match(/native_admission:=private.weekly_source_agency_candidate_admit_v1/g)||[]).length,1);
  assert.ok(s.indexOf('native_admission:=')<s.indexOf('weekly_source_lock_family_rows_v1'));
  assert.match(s,/weekly_source_agency_source_phase_admit_v1[\s\S]*native_admission\)/);}
 const s=read('86_agency_source_phase_admission.sqlinc');assert.doesNotMatch(s,/gate:=private\.weekly_source_agency_candidate_admit_v1/);
 assert.match(s,/gate->>'candidate_id' is distinct from p_candidate_id::text/);
 assert.match(s,/from public\.contracts where id=t\.contract_id/);assert.doesNotMatch(s,/t\.client_id|t\.candidate_id/);
});
test('actual PHYSICAL/native family dependencies precede Source dependencies and full retained reread',()=>{
 const s=read('86_agency_source_phase_admission.sqlinc');
 assert.ok(s.indexOf('weekly_source_lock_family_rows_v1')<s.indexOf('timesheet_correction_a0_family_tokens_retain_v1'));
 assert.ok(s.indexOf('timesheet_correction_a0_family_tokens_retain_v1')<s.indexOf('before_footprint:='));
 assert.ok(s.indexOf("fence_acquire_v1(before_footprint->'tokens',true)")<s.indexOf('after_footprint:='));
 assert.match(s,/after_footprint is distinct from before_footprint/);assert.match(s,/family_after is distinct from family_before/);
 assert.match(s,/before_footprint->'source_cas' is distinct from p_expected_source_cas/);
 assert.equal((s.match(/timesheet_correction_a0_source_footprint_v1\(/g)||[]).length,2);
 assert.doesNotMatch(s,/replacement_timesheet_id|planned_reversal|new_generation|SOURCE_CORRECTION|set_config\('cloudtms/);
});
test('one original immutable token set and current monotonic tokens use the SAME planned request',()=>{
 const s=read('88_complete_same_request_store_source_tokens.sqlinc');
 assert.equal((s.match(/create table /gi)||[]).length,1);assert.match(s,/current_source_dependency_tokens @> source_dependency_tokens/);
 assert.doesNotMatch(s,/v_mutable[\s\S]*'source_dependency_tokens'/);assert.match(s,/not new.current_source_dependency_tokens @> old.current_source_dependency_tokens/);
 assert.match(s,/source_admission,transaction_id}' is distinct from pg_current_xact_id\(\)::text/);
 assert.match(s,/source_admission,backend_pid}' is distinct from pg_backend_pid\(\)::text/);
 assert.match(s,/source_admission,source_cas}' is distinct from old.source_cas/);
 assert.match(s,/original_capture' is distinct from old.native_receipts->'original_capture'/);
});
test('full token capture is moved to same row before small capture bound and original input digest',()=>{
 const s=read('90_complete_native_begin_immutable_source_tokens.sqlinc');
 assert.ok(s.indexOf("capture:=capture-'source_dependency_tokens'")<s.indexOf('49152'));
 assert.match(s,/source_cas,source_dependency_tokens,current_source_dependency_tokens,initial_timesheet_id/);
 assert.match(s,/capture->'source_cas',dependency_tokens,dependency_tokens/);
 assert.match(s,/request_sha256_v1\(p_contract_week_id,p_reason,[\s\S]*p_request_key,p_actor_user_id,capture\)/);
 assert.match(s,/original_capture',capture/);assert.match(s,/RESUME_CURRENT_SCOPED_ACTOR/);
});
test('current actor resume derives full Source scope without digest regeneration or stored gate replay',()=>{
 const s=read('91_complete_current_request_source_phase_resume.sqlinc');
 assert.match(s,/p_request_key,v_request.actor_user_id,v_original/);
 assert.match(s,/v_request.source_cas,native_admission\)/);assert.match(s,/CURRENT_SOURCE|ORIGINAL_SOURCE_CAS_CHANGED/);
 assert.match(s,/current_source_dependency_tokens\|\|\(source_admission->'source_dependency_tokens'\)/);
 assert.match(s,/NATIVE_SOURCE_FOOTPRINT_RETAINED/);assert.match(s,/current_resumer_user_id',p_actor_user_id/);
 assert.ok(s.indexOf("if v_request.state='COMPLETED'")<s.indexOf('source_admission:='));
 assert.match(s,/v_request.current_row_signature/);assert.match(s,/AGENCY_CHANGE_CURRENT_PHASE_SIGNATURE_CHANGED/);
});
test('source before guard has a single exact ordinary arbiter before any ACTIVE lookup and no AFTER reader',()=>{
 const s=read('92_complete_same_source_before_with_active_capture.sqlinc');
 assert.equal((s.match(/fence_acquire_v1\(tokens,false\)/g)||[]).length,1);
 assert.ok(s.indexOf('fence_acquire_v1(tokens,false)')<s.indexOf('fence_acquire_v1(nested_tokens,false)'));
 assert.ok(s.indexOf('fence_acquire_v1(nested_tokens,false)')<s.indexOf('weekly_source_agency_active_source_mutation_assert_v1'));
 assert.match(s,/to_jsonb\(new\) end,tokens\|\|nested_tokens/);
 assert.match(s,/case when tg_op in\('UPDATE','DELETE'\) then to_jsonb\(old\)/);
 assert.match(s,/case when tg_op in\('INSERT','UPDATE'\) then to_jsonb\(new\)/);
 const g=read('87_agency_active_source_mutation_guard.sqlinc');assert.doesNotMatch(g,/weekly_source_bulk_source_cas_v1|after update|fence_acquire|set_config/i);
 assert.match(g,/raw_tokens is distinct from p_raw_tokens/);assert.match(g,/request.state='ACTIVE'/);
 assert.match(g,/raw_tokens:=raw_tokens\|\|nested_tokens/);
 assert.match(g,/RESUME_SAME_REQUEST_WITH_CURRENT_SCOPED_ACTOR/);
 assert.doesNotMatch(g,/p_bypass|p_allow|current_setting|insert into|update private/);
});
test('native original actor/target/CAS/replay and actual Authorise/Return continuations stay required',()=>{
 for(const n of ['reference/46_agency_native_authorise_step.sqlinc','reference/53_agency_same_draft_return_step.sqlinc',
  'reference/19_agency_native_worker_apply.sqlinc'])assert.match(read(n),/weekly_source_agency_change_request_assert_v1|weekly_source_agency_change_worker_context_v1/);
 const s=read('91_complete_current_request_source_phase_resume.sqlinc');
 for(const literal of ['v_request.initial_row_signature','v_request.current_timesheet_version','v_request.current_row_signature',
  'v_request.original_draft_invoice_id','v_request.source_cas','v_request.rule_set_version_id'])assert.ok(s.includes(literal));
 assert.match(s,/weekly_source_pay_actor_allowed_v1\(p_actor_user_id\)/);assert.match(s,/VIEW_PAY_WORK/);
});
// Relational model only: the exact native SELECT and dirty/RR behavior remain
// mandatory PostgreSQL first-use tests. No model result is receipt evidence.
const object=(relation,id)=>({kind:'SOURCE_OBJECT',json:JSON.stringify([relation,id])});
const tuple=(kind,...values)=>({kind,json:JSON.stringify(values)});
const same=(a,b)=>JSON.stringify(a)===JSON.stringify(b);
const matches=(held,raw)=>held.some(h=>raw.some(r=>same(h,r)||h.kind==='SOURCE_CONTRACT_COVERAGE'&&r.kind===h.kind&&h.uuid===r.uuid&&
  (h.coverage[1]===null||r.coverage[0]===null||r.coverage[0]<=h.coverage[1])&&
  (r.coverage[1]===null||h.coverage[0]===null||h.coverage[0]<=r.coverage[1])));
const selected=(r,keys)=>Object.fromEntries(keys.map(k=>[k,r[k]]));
function mutationBasis(relation,row,comparisons={}){if(row===null)return null;
 const keys={'public.timesheets':['timesheet_id','contract_id','booking_id'],
  'public.timesheets_financials':['id','timesheet_id','client_id','candidate_id'],
  'public.weekly_issue_comparison_revisions':['id','incident_id','contract_id'],
  'public.weekly_discrepancy_incidents':['id','source_cycle_id','client_id','work_event_id']}[relation];
 const result=keys?selected(row,keys):row;
 if(relation==='public.weekly_discrepancy_incidents')result.source_contract_dependency=row.current_comparison_revision_id?
  comparisons[row.current_comparison_revision_id].contract_id:null;
 return result;}
const admits=(request,raw,relation,old,next,comparisons={})=>request.state!=='ACTIVE'||!matches(request.current,raw)||
 same(mutationBasis(relation,old,comparisons),mutationBasis(relation,next,comparisons));
for(const phase of ['CAPTURED','DRAFT_REMOVED','UNAUTHORISED','RECOMPUTE_PENDING','FINANCIAL_CURRENT','AUTHORISED'])
 test('UNIT_ONLY persisted '+phase+' refuses selected foreign publication/config in a later RPC',()=>{
  const current=[object('public.weekly_source_cycles','cycle'),object('public.weekly_source_rule_set_versions','rules')];
  const req={state:'ACTIVE',phase,current};
  assert.equal(admits(req,[current[0]],'public.weekly_source_cycles',{id:'cycle',current_projection_publication_id:'A'},
   {id:'cycle',current_projection_publication_id:'B'}),false);
  assert.equal(admits(req,[current[1]],'public.weekly_source_rule_set_versions',{id:'rules',values_json:{rate:1}},
   {id:'rules',values_json:{rate:2}}),false);
  assert.equal(admits({...req,state:'COMPLETED'},[current[0]],'public.weekly_source_cycles',{id:'cycle'},{id:'cycle',version:2}),true);
 });
test('UNIT_ONLY native processing/audit fields do not become Source identity locks',()=>{
 const current=[object('public.timesheets_financials','fin'),object('public.timesheets','T0')],r={state:'ACTIVE',current};
 const f={id:'fin',timesheet_id:'T0',client_id:'client',candidate_id:'candidate',processing_status:'PENDING_AUTH'};
 assert.equal(admits(r,[current[0]],'public.timesheets_financials',f,{...f,processing_status:'READY_FOR_HR',updated_at:'later'}),true);
 assert.equal(admits(r,[current[0]],'public.timesheets_financials',f,{...f,client_id:'foreign'}),false);
 const t={timesheet_id:'T0',contract_id:'contract',booking_id:' booking ',authorised_at_server:null};
 assert.equal(admits(r,[current[1]],'public.timesheets',t,{...t,authorised_at_server:'actual native time'}),true);
 assert.equal(admits(r,[current[1]],'public.timesheets',t,{...t,booking_id:'other'}),false);
});
test('UNIT_ONLY native query closure/new comparison wording retains SAME referenced Contract, foreign dependency refuses',()=>{
 const token=tuple('SOURCE_CYCLE_CLIENT','cycle','client'),r={state:'ACTIVE',current:[token]};
 const i={id:'incident',source_cycle_id:'cycle',client_id:'client',work_event_id:'event',current_comparison_revision_id:'old',state:'OPEN'};
 const comps={old:{contract_id:'contract'},new:{contract_id:'contract'},foreign:{contract_id:'other'}};
 assert.equal(admits(r,[token],'public.weekly_discrepancy_incidents',i,{...i,current_comparison_revision_id:'new',state:'CLOSED'},comps),true);
 assert.equal(admits(r,[token],'public.weekly_discrepancy_incidents',i,{...i,current_comparison_revision_id:'foreign'},comps),false);
});
test('UNIT_ONLY resolution conjunction excludes unrelated Client and generation without broad raw-parent locking',()=>{
 const held=[tuple('SOURCE_RESOLUTION_SLOT','row',4,'client')];
 for(const t of [tuple('SOURCE_RESOLUTION_SLOT','row',3,'client'),tuple('SOURCE_RESOLUTION_SLOT','row',4,'foreign'),
  tuple('SOURCE_RESOLUTION_SLOT','other',4,'client'),object('public.weekly_source_upload_rows','row')])assert.equal(matches(held,[t]),false);
 assert.equal(matches(held,[held[0]]),true);
});
test('UNIT_ONLY native coverage overlaps/open bounds and selected object IDs remain distinct from all-Client aliases',()=>{
 const held=[{kind:'SOURCE_CONTRACT_COVERAGE',uuid:'client',coverage:['2026-10-05','2026-10-11']}];
 assert.equal(matches(held,[{...held[0],coverage:['2026-10-11',null]}]),true);
 assert.equal(matches(held,[{...held[0],coverage:['2026-10-12',null]}]),false);
 assert.equal(matches(held,[{...held[0],uuid:'foreign',coverage:[null,null]}]),false);
 const exact=[object('public.contracts','referenced-contract')];assert.equal(matches(exact,[exact[0]]),true);
});
test('UNIT_ONLY foreign-Client snapshot still selects genuine group/event revision through NESTED union',()=>{
 const captured=[tuple('SOURCE_GROUP_REVISION_EVENT','group','selected-event')];
 const raw=[object('public.weekly_source_final_snapshot_lines','new-row'),tuple('SOURCE_REVISION_CLIENT','revision','foreign-client')];
 const nested=[tuple('SOURCE_GROUP_REVISION_EVENT','group','selected-event')];
 assert.equal(matches(captured,raw),false);assert.equal(matches(captured,[...raw,...nested]),true);
 const req={state:'ACTIVE',current:captured};
 assert.equal(admits(req,[...raw,...nested],'public.weekly_source_final_snapshot_lines',null,
  {id:'new-row',final_revision_id:'revision',client_id:'foreign-client',work_event_id:'selected-event'}),false);
});
test('all seven native pages are packaging-parseable and expose no new public helper/SQL/callback',()=>{
 for(const n of ['86_agency_source_phase_admission.sqlinc','87_agency_active_source_mutation_guard.sqlinc',
  '88_complete_same_request_store_source_tokens.sqlinc','89_complete_native_capture_source_phase_admission.sqlinc',
  '90_complete_native_begin_immutable_source_tokens.sqlinc','91_complete_current_request_source_phase_resume.sqlinc',
  '92_complete_same_source_before_with_active_capture.sqlinc'])assert.ok(scanManagedSql(read(n)).statements.length>=3,n);
 for(const n of ['86_agency_source_phase_admission.sqlinc','87_agency_active_source_mutation_guard.sqlinc']){
  assert.doesNotMatch(read(n),/grant execute|create or replace function public\./);assert.match(read(n),/from public,anon,authenticated,service_role/);}
});
