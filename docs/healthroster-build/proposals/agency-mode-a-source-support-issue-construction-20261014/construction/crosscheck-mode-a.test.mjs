import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import assert from 'node:assert/strict';import test from 'node:test';import vm from 'node:vm';import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),read=n=>fs.readFileSync(path.join(dir,n),'utf8');
const full=read('10_complete_worker_index_with_native_source_crosscheck.js'),mode=read('12_complete_native_mode_a_with_preset_reference_only.sqlinc');
const start=full.indexOf('async function enrichTsfinWithHrCrosscheck('),end=full.indexOf('// ============================================================\n// Helpers for weekly changed-hours correction workflow',start);
assert.ok(start>=0&&end>start);const actualFunction=full.slice(start,end),id=n=>`00000000-0000-0000-0000-${String(n).padStart(12,'0')}`;
const data=()=>({contract:'WEEKLY_SOURCE_AGENCY_CROSSCHECK_CONTEXT_V1',is_agency_invoice:true,timesheet_id:id(1),timesheet_version:7,
 source_cycle_id:id(2),projection_publication_id:id(3),upload_id:id(4),hr_import_id:id(5),facts_sha256:'a'.repeat(64),week_complete:true,
 validation_source_kind:'AGENCY_INVOICE_WEEK',validation_status:'VALIDATION_OK',validation_reason:null,crosscheck_status:'OK',crosscheck_issues:null,
 rows:[{date:'2026-10-09',source_system:'HEALTHROSTER',hr_request_id:'REAL-IMPORT-ROW',nhsp_shift_id:null,external_row_key:'REAL-EXPORT-KEY',
 ward:null,assignment_code:null,ref_num:'REF',start:'09:00',end:'17:00',break_mins:30,paid_minutes:450}]});
async function run(native=data(),{paid=false,locked=false}={}){
 const calls=[];const ctx=vm.createContext({structuredClone,console:{log(){}},sbRpc:async(env,name,args)=>{calls.push({name,args});if(native instanceof Error)throw native;return native;},Date,Intl});
 vm.runInContext(actualFunction+'\nthis.fn=enrichTsfinWithHrCrosscheck;',ctx);
 const ts={timesheet_id:id(1),version:7,sheet_scope:'WEEKLY',week_ending_date:'2026-10-11'};
 const fin={timesheet_id:id(1),client_id:id(6),candidate_id:id(7),paid_at_utc:paid?'2026-10-09T10:00:00Z':null,
  locked_by_invoice_id:locked?id(8):null,external_source_rows_json:{OTHER:[{actual:'kept'}]},nhsp_import_id:null};
 const before=structuredClone(fin),result=await ctx.fn({},ts,fin,{client_autoprocess_hr:false,client_no_timesheet_required:true},[],null);
 return{result:JSON.parse(JSON.stringify(result)),before,calls};
}
const scalarFields=['source_cycle_id','projection_publication_id','upload_id','hr_import_id','facts_sha256'];
test('actual whole enrichment attaches real bridge support even when unsigned preset flags would clear it',async()=>{
 const f=data(),r=await run(f);assert.equal(r.calls.length,1);assert.equal(r.calls[0].name,'weekly_source_agency_crosscheck_context_v1');
 assert.equal(r.result.nhsp_import_id,id(5));assert.deepEqual(r.result.external_source_rows_json.HR_WEEKLY,f.rows);
 assert.deepEqual(r.result.external_source_rows_json.OTHER,r.before.external_source_rows_json.OTHER);assert.equal(r.result.hr_crosscheck_status,'OK');assert.equal(r.result.hr_crosscheck_issues,null);
});
test('native protection/query validation hold is not fabricated as a clinical hours mismatch',async()=>{
 const f=data();f.week_complete=false;f.validation_status='VALIDATION_ERROR';f.validation_reason='PROTECTED_SHIFT_WAITING';
 const r=await run(f);assert.equal(r.result.hr_crosscheck_status,'OK');assert.equal(r.result.hr_crosscheck_issues,null);assert.ok(r.result.external_source_rows_json.HR_WEEKLY.length);
});
test('actual native HOURS_MISMATCH is copied separately from typed completeness',async()=>{
 const f=data();f.week_complete=false;f.validation_status='VALIDATION_ERROR';f.validation_reason='HOURS_DIFFER';f.crosscheck_status='HOURS_MISMATCH_HR';f.crosscheck_issues=['HOURS_MISMATCH_HR'];
 const r=await run(f);assert.equal(r.result.hr_crosscheck_status,'HOURS_MISMATCH_HR');assert.deepEqual(r.result.hr_crosscheck_issues,['HOURS_MISMATCH_HR']);
});
for(const opts of [{paid:true},{locked:true}])test('actual locked/paid early return preserves stored history without new read '+JSON.stringify(opts),async()=>{
 const r=await run(new Error('Must not read'),opts);assert.equal(r.calls.length,0);assert.deepEqual(r.result,r.before);
});
test('ordinary native false descriptor takes unchanged legacy clear branch',async()=>{
 const f={contract:'WEEKLY_SOURCE_AGENCY_CROSSCHECK_CONTEXT_V1',is_agency_invoice:false,timesheet_id:id(1),timesheet_version:7};
 const r=await run(f);assert.equal(r.result.nhsp_import_id,null);assert.equal(r.result.hr_crosscheck_status,null);assert.ok(!r.result.external_source_rows_json.HR_WEEKLY);
});
test('native read failure propagates in the actual enrichment rather than creating support',async()=>{await assert.rejects(()=>run(new Error('Native provenance unavailable')),/Native provenance unavailable/);});
for(const field of scalarFields)test('actual decoder refuses native '+field+' array coercion',async()=>{const f=data();f[field]=[f[field]];await assert.rejects(()=>run(f),/NATIVE_CONTEXT_INVALID/);});
for(const field of ['date','start','end'])test('actual decoder refuses source-row '+field+' array coercion',async()=>{const f=data();f.rows[0][field]=[f.rows[0][field]];await assert.rejects(()=>run(f),/NATIVE_ROW_INVALID/);});
test('actual decoder refuses invented NHSP shift identity and preserves real NULL',async()=>{const f=data();f.rows[0].nhsp_shift_id=id(4);await assert.rejects(()=>run(f),/NATIVE_ROW_INVALID/);});
for(const [name,change] of [
 ['foreign Timesheet',f=>{f.timesheet_id=id(99);} ],['stale physical version',f=>{f.timesheet_version=6;}],
 ['legacy marker',f=>{f.validation_source_kind='LEGACY';}],['override',f=>{f.validation_status='OVERRIDDEN';}],
 ['invented completion',f=>{f.week_complete=true;f.validation_status='VALIDATION_ERROR';}],
 ['empty support',f=>{f.rows=[];}],['unknown claim',f=>{f.native_paid=true;}],['fabricated crosscheck',f=>{f.crosscheck_status='READY_FOR_INVOICE';}],
 ['invalid clinical scalar',f=>{f.rows[0].paid_minutes='450';}],['null source identity',f=>{f.hr_import_id=null;}]
])test('actual enrichment refuses '+name,async()=>{const f=data();change(f);await assert.rejects(()=>run(f),/AGENCY_INVOICE_SUPPORT_NATIVE_/);});
test('complete Worker reverse restores exact accepted13; complete native ModeA reverse restores exact37/71',()=>{
 for(const [name,file] of [['CROSSCHECK_WORKER_FULL_REVERSE.json','10_complete_worker_index_with_native_source_crosscheck.js'],['MODE_A_REFERENCE_FULL_REVERSE.json','12_complete_native_mode_a_with_preset_reference_only.sqlinc']]){
  const r=JSON.parse(read(name));let s=read(file);assert.equal(crypto.createHash('sha256').update(s).digest('hex'),r.forward_sha256);
  for(const h of [...r.hunks].reverse()){assert.equal(s.split(h.after).length,2);s=s.replace(h.after,()=>h.before);}assert.equal(crypto.createHash('sha256').update(s).digest('hex'),r.input_sha256);
 }
});
test('actual native ModeA legacy upsert excludes only rederived preset target IDs',()=>{
 assert.ok(mode.includes("and not(vr.timesheet_id=any(v_agency_reference_targets));"));
 const upsert=mode.slice(mode.indexOf('  create temporary table tmp_val_upsert'),mode.indexOf('  -- ✅ UPDATED: include pre_validated'));
 assert.ok(upsert.includes("where vm.mode = 'MODE_A'"));
 const targets=[id(1)],ordinary=[id(1),id(2),id(3)].filter(t=>!targets.includes(t));assert.deepEqual(ordinary,[id(2),id(3)]);
});
test('preset loop uses genuine native guard-selected rows, existing main week and sole typed writer',()=>{
 assert.ok(mode.includes('select distinct main_week.timesheet_id from tmp_val_rows vr'));
 assert.ok(mode.includes('perform public.weekly_source_agency_week_validation_refresh_v1(v_agency_main_id,v_agency_publication_id)'));
 const auto=mode.slice(mode.indexOf('  into v_validation_auto_authorise_timesheet_ids'));assert.ok(auto.includes('join tmp_val_upsert vu'));
 assert.ok(!read('11_native_mode_a_preset_reference_admission.sqlinc').includes('set_config'));
});
test('genuine bridge admission rechecks publication/pointer/content/coverage/actor and legacy refusal',()=>{
 const s=read('11_native_mode_a_preset_reference_admission.sqlinc');for(const t of ["p.state is distinct from 'CURRENT'",'c.current_projection_publication_id is distinct from p.id',
  'source_file_sha256 is distinct from encode(u.content_sha256', 'coverage_operation_key is distinct from private.weekly_source_mode_a_operation_key_v1',
  'hi.coverage_start_date is distinct from u.confirmed_coverage_start_local_date',"p_actor_user_id,'RECHECK_SOURCE'",'public._import_review_assert_actor_v1(p_actor_user_id)',
  'private.weekly_source_agency_legacy_import_validation_admit_v1(p_import_id)',"a.authority_mode='VALIDATION_ONLY'"])assert.ok(s.includes(t),t);
});
test('native Source support reads actual finalised comparison/current generation/bridge row without mutation',()=>{
 const s=read('08_native_agency_crosscheck_context.sqlinc');for(const x of ["ur.row_finalisation_state='SOURCE_WORKED'",'comparison.timesheet_revision=t.version',
  "hr.payload_json->>'weekly_source_upload_row_id'=ur.id::text",'comparison.source_start_at_local is not distinct from ur.start_at_local',
  "source->>'upload_row_id'=ur.id::text",'source_count<>comparison_count',"validation.validation_source_kind is distinct from 'AGENCY_INVOICE_WEEK'"])assert.ok(s.includes(x),x);
 assert.equal(/\b(insert into|update public\.|delete from)\b/i.test(s),false);assert.ok(!s.includes('paid_at_utc=true'));
});
console.log('UNIT_ONLY actual Worker function / source-bound native branch vectors; no native SQL, actor or import operation executed.');
