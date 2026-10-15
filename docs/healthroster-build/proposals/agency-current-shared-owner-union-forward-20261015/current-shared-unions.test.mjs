import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import test from 'node:test';import crypto from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
import {scanManagedSql,exactAtomicBody} from './reference/pinned-source-parser.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),read=p=>fs.readFileSync(path.join(here,p),'utf8'),sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const inputs=JSON.parse(read('CURRENT_OWNER_INPUTS.json')),reverse=JSON.parse(read('COMPLETE_CURRENT_SHARED_REVERSES.json')),comparison=JSON.parse(read('BASELINE_COMPARISON.json'));
const root=execFileSync('git',['rev-parse','--show-toplevel'],{cwd:here,encoding:'utf8'}).trim();
function parsedDefinitions(s){const p=scanManagedSql(s);return p.statements.map(st=>p.source.slice(st.start,st.end)).filter(st=>/^create\s+(?:or\s+replace\s+)?function/i.test(st));}
function header(s){return s.slice(0,s.search(/\bas\s+\$[a-z0-9_]*\$/i)).replace(/\s+/g,' ').trim();}
for(const r of inputs.records)test('CURRENT_SOURCE_INPUT actual pinned raw file and extracted current function '+r.name,()=>{
 const b=execFileSync('git',['show',`${r.current.pin}:${r.current.path}`],{cwd:root,maxBuffer:32*1024*1024});assert.equal(b.length,r.current.raw_bytes);assert.equal(sha(b),r.current.raw_sha256);
 const defs=parsedDefinitions(b.toString('utf8'));assert.ok(defs.some(s=>sha(s)===r.current.definition_sha256));
 assert.equal(sha(read('before/'+r.slug+'.sqlinc').trimEnd()),r.current.definition_sha256);
 const raw=fs.readdirSync(path.join(here,'reference')).find(f=>f.startsWith('current_'+String(r.number).padStart(2,'0')+'_'));assert.ok(raw);assert.equal(sha(fs.readFileSync(path.join(here,'reference',raw))),r.current.raw_sha256);
});
for(const r of reverse.records)test('COMPLETE_CURRENT_REVERSE '+r.name,()=>{
 let current=read(r.source_forward).trimEnd();assert.equal(sha(current),r.forward_sha256);for(const h of r.full_function_hunks){assert.equal(current.split(h.after).length,2);current=current.replace(h.after,()=>h.before);}assert.equal(current,read(r.source_before).trimEnd());assert.equal(sha(current),r.current.definition_sha256);
 assert.equal(header(read(r.source_forward)),header(read(r.source_before)),'Function signature/security/config header stays exact');
});
test('PACKAGING_ONLY all seventeen selected shared files contain only their existing complete function definitions',()=>{
 const forwardFiles=fs.readdirSync(path.join(here,'forward')).filter(f=>f.endsWith('.sqlinc'));assert.equal(forwardFiles.length,17);
 let functions=0;for(const file of forwardFiles){const p=scanManagedSql(read('forward/'+file));assert.equal(p.meta.length,0);for(const stmt of p.statements){assert.deepEqual(stmt.tokens.slice(0,4),['CREATE','OR','REPLACE','FUNCTION']);functions++;}}
 assert.equal(functions,18);assert.equal(reverse.changed_function_owners,17);
 const expanded=read('00_current_agency_shared_owners.sql').replace(/^\\ir ([^\r\n]+)$/gm,(_,f)=>read(f));assert.ok(exactAtomicBody(expanded));
 assert.equal(scanManagedSql(expanded).statements.filter(s=>s.tokens[0]==='BEGIN').length,1);
});
test('SOURCE_ONLY first twelve current owner preimages reproduce accepted delta baselines exactly',()=>{
 assert.equal(comparison.filter(r=>r.number<=12).length,12);for(const r of comparison.filter(r=>r.number<=12))assert.equal(r.exact_current_preimage,true,r.name);
 for(const r of inputs.records.filter(r=>r.number<=12))assert.equal(read('before/'+r.slug+'.sqlinc'),read('accepted-before/'+r.slug+'.sqlinc'));
});
test('SOURCE_ONLY finite OFF union includes the accepted Money and physical predecessors plus exactly seven Agency operations',()=>{
 const p=JSON.parse(read('OFF_PREDECESSOR_PROVENANCE.json'));assert.equal(p.agency_operations.length,7);assert.equal(new Set(p.agency_operations).size,7);assert.equal(p.agency_operations_widened,false);
 const parent=read('forward/14_current_98_complete_off_parent_with_actual_agency.sqlinc');
 for(const expected of ['paid_overlap_target_work_event_id','decision_bundle_id','WEEKLY_SOURCE_MISSING_ACCEPT_PROPOSAL_PROOF_V1','contract.client_id is null or contract.candidate_id is null','private.weekly_source_off_rule_agency_parent_v1'])assert.ok(parent.includes(expected),expected);
 const command=read('forward/15_current_99_complete_off_command_with_literal_agency.sqlinc'),shape=read('forward/17_current_101_complete_off_setup_shape_with_finite_agency.sqlinc');
 for(const op of p.agency_operations){assert.ok(command.includes(op));assert.ok(shape.includes(op));}
 for(const op of ['weekly_source_missing_accept_context_v1','weekly_source_missing_accept_prepare_atomic_v1','weekly_source_later_change_decide_atomic_v1','weekly_source_office_recheck_begin_v1'])assert.ok(command.includes(op));
 assert.ok(command.includes("req->>'decision' is distinct from 'APPROVE_UPDATED_HOURS'"));
});
test('SOURCE_ONLY current financial writers retain managed/protected and invoice locks around the exact Agency native frame',()=>{
 for(const [file,marker]of [['01_current_11_complete_tsfin_prepare_write.sqlinc','E7'],['02_current_12_complete_tsfin_write_current_snapshot_single_bounded.sqlinc','E25'],['03_current_13_complete_tsfin_write_snapshots_and_complete.sqlinc','E26']]){
  const s=read('forward/'+file);assert.ok(s.includes('WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED'));assert.ok(s.includes('private.weekly_source_agency_financial_write_assert_v1'));assert.ok(s.includes('locked_by_invoice_id'));
  if(marker==='E7')assert.ok(!header(s).includes('SECURITY DEFINER'));else{assert.ok(header(s).includes('SECURITY DEFINER'));assert.ok(s.includes('private.weekly_source_agency_financial_write_observe_v1'));}
 }
});
test('SOURCE_ONLY actual publication callback remains in all three final/replay branches',()=>{
 const s=read('forward/05_current_70_complete_current_publication_with_agency_validation.sqlinc');assert.equal((s.match(/perform private.weekly_source_agency_publication_validation_v1\(v_publication.id\);/g)||[]).length,3);
});
test('SOURCE_ONLY actual direct and asynchronous issue owners retain native validation and callback gates',()=>{
 for(const file of ['07_current_16_complete_actual_direct_issue_with_agency_callbacks.sqlinc','08_current_17_complete_actual_async_issue_with_agency_callbacks.sqlinc']){
  const s=read('forward/'+file);assert.ok(s.includes('private.weekly_source_agency_issue_close_v1'));assert.ok(s.includes('private._invoice_issue_validate_batch'));assert.ok(s.includes('HEALTHROSTER_VALIDATION_REQUIRED'));
 }
 assert.ok(read('forward/06_current_14_complete_native_issue_validator_with_agency_requirements.sqlinc').includes('private.weekly_source_agency_issue_requirements_v1'));
});
test('SOURCE_ONLY selected Mode A paths preserve whole native Candidate/Client unit admission and separate completeness continuation',()=>{
 const apply=read('forward/10_current_22_complete_native_mode_a_with_selected_preset_refusal.sqlinc'),dispatch=read('forward/12_current_24_complete_native_dispatch_with_separate_completeness_continuation.sqlinc');
 assert.ok(apply.includes('private.weekly_source_agency_mode_a_units_v1'));assert.ok(apply.includes('AGENCY_INVOICE_USES_COMPLETENESS_WRITER'));
 assert.ok(!apply.includes('perform private.weekly_source_agency_legacy_import_validation_admit_v1(p_import_id);'));
 assert.ok(dispatch.includes('perform private.weekly_source_agency_publication_validation_v1(v_publication_id);'));
 assert.ok(read('forward/09_current_21_complete_native_ready_units_with_preset_exclusion.sqlinc').includes('has_preset'));
});
test('SOURCE_ONLY correction of121 preserves the actual existing E7 service helper grant and every other ACL expectation',()=>{
 const j=JSON.parse(read('SERVICE_INVOKER_HELPER_VERIFIER_REVERSE.json')),old=read('verification-before/121_current_agency_security.sql'),s=read(j.forward);assert.equal(sha(old),j.old_source.sha256);assert.equal(sha(s),j.forward_sha256);
 let reversed=s;for(const h of j.hunks){assert.equal(reversed.split(h.after).length,2);reversed=reversed.replace(h.after,h.before);}assert.equal(reversed,old);
 assert.equal(j.new_grant_added,false);assert.equal(j.new_public_rpc,false);
 const grantSource=execFileSync('git',['show',`${j.actual_grant_source.pin}:${j.actual_grant_source.path}`],{cwd:root,maxBuffer:4*1024*1024}).toString('utf8');
 const p=scanManagedSql(grantSource),grants=p.statements.map(st=>p.source.slice(st.start,st.end)).filter(st=>/^grant execute on function private\./i.test(st));
 assert.equal(grants.length,1);assert.match(grants[0],/weekly_source_agency_financial_write_assert_v1\(uuid,jsonb,uuid,uuid\) to service_role;/);
 assert.ok(s.includes("('private.weekly_source_agency_financial_write_assert_v1(uuid,jsonb,uuid,uuid)',true)"));
 for(const expected of ["has_function_privilege('anon'","has_function_privilege('authenticated'",'acl.grantee=0'])assert.ok(s.includes(expected));
});
test('SOURCE_ONLY approved schema packaging normalization preserves the exact raw88 concatenation',()=>{
 const j=JSON.parse(read('SCHEMA_SPLIT_PACKAGING_SUCCESSOR.json')),m=read('migration/01_same_agency_request_store.sqlinc'),identity=read('repeatable/02_same_request_identity_guard.sqlinc');
 assert.equal(sha(m),j.migration_after_sha256);assert.equal(sha(identity),j.identity_after_sha256);assert.equal(sha(m+identity),j.combined_bytes_sha256);
 assert.ok(!m.endsWith('\n\n'));assert.ok(identity.startsWith('\ncreate or replace function'));
});
test('PACKAGING_ONLY full current Agency recipe expands to88 unique existing/new definitions after prerequisite migrations',()=>{
 const sibling='../agency-current-source-callers-lifecycle-module-20261015/',base=path.join(root,'docs/healthroster-build/proposals/agency-current-source-callers-lifecycle-module-20261015');
 const source=read('01_selected_full_agency_sql_recipe.sql').replace(/^\\ir ([^\r\n]+)$/gm,(_,file)=>file.startsWith(sibling)?fs.readFileSync(path.join(base,file.slice(sibling.length)),'utf8'):read(file));
 assert.ok(exactAtomicBody(source));assert.doesNotMatch(source,/create table|alter table[^;]*add column/i);
 const definitions=parsedDefinitions(source),names=definitions.map(s=>s.match(/^create\s+(?:or\s+replace\s+)?function\s+([^\s(]+)/i)[1]);
 assert.equal(names.length,88);assert.equal(new Set(names).size,88);
 assert.ok(source.includes('perform private.weekly_source_agency_original_claim_recheck_v1(v_request.id);'));
});
test('SOURCE_ONLY final verifier recipe excludes broken121 and selects the exact122 successor',()=>{
 const selected=JSON.parse(read('SELECTED_CURRENT_MODULE_RECIPE.json')),s=read('02_selected_final_security_recipe.sql');
 assert.ok(!/^\\ir .*121_/m.test(s));assert.ok(s.includes('122_complete_current_agency_security_with_actual_invoker_helper_grant.sql'));
 assert.ok(selected.original_389_verifier_121.startsWith('FIX_REQUIRED'));assert.equal(selected.changed_shared_function_owners,17);assert.equal(selected.native,false);
});
