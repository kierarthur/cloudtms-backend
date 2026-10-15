import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import test from 'node:test';import crypto from 'node:crypto';
import {fileURLToPath} from 'node:url';import {execFileSync} from 'node:child_process';import {scanManagedSql,exactAtomicBody} from './reference/pinned-source-parser.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),read=p=>fs.readFileSync(path.join(here,p),'utf8'),sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const inputs=JSON.parse(read('SOURCE_INPUTS.json')),reverses=JSON.parse(read('COMPLETE_REVERSES.json')),recipe=JSON.parse(read('CURRENT_MODULE_RECIPE.json')),census=JSON.parse(read('CLAIM_AND_CALLER_CENSUS.json'));
function expand(file){return read(file).replace(/^\\ir ([^\r\n]+)$/gm,(_,relative)=>expand(path.posix.join(path.posix.dirname(file),relative)));}
for(const entry of inputs.entries)test('SAVED_SOURCE_INPUT exact raw receipt '+entry.reference,()=>{
 const b=fs.readFileSync(path.join(here,'reference',entry.reference));assert.equal(b.length,entry.bytes);assert.equal(sha(b),entry.sha256);
});
test('SOURCE_ONLY all 76 saved input receipts reproduce the exact committed Git objects',()=>{
 const root=execFileSync('git',['rev-parse','--show-toplevel'],{cwd:here,encoding:'utf8'}).trim();
 assert.equal(inputs.entries.length,76);for(const entry of inputs.entries){const b=execFileSync('git',['show',`${entry.commit}:${entry.path}`],{cwd:root,maxBuffer:64*1024*1024});assert.equal(sha(b),entry.sha256,entry.path);assert.equal(b.length,entry.bytes);}
});
for(const reverse of reverses)test('FULL_REVERSE exact complete owner '+reverse.destination,()=>{
 let s=read(reverse.destination);assert.equal(sha(s),reverse.after_sha256);
 for(const h of reverse.hunks){assert.equal(s.split(h.after).length,2);s=s.replace(h.after,h.before);}
 assert.equal(s,read(reverse.before));assert.equal(sha(s),reverse.before_sha256);
});
for(const file of ['00_agency_schema_once_only.sql','01_agency_source_core.sql','02_agency_current_callers.sql','03_agency_source_and_current_callers.sql','04_agency_fixed_approval_candidate_lifecycle.sql','05_agency_source_callers_and_lifecycle.sql'])test('PACKAGING_ONLY lexical atomic include closure '+file,()=>{
 const s=expand(file),parsed=scanManagedSql(s);assert.equal(parsed.meta.length,0);assert.ok(exactAtomicBody(s).length>0);
 assert.equal(parsed.statements.filter(r=>r.tokens[0]==='BEGIN').length,1);assert.equal(parsed.statements.filter(r=>r.tokens[0]==='COMMIT').length,1);
});
test('SOURCE_ONLY table/typed provenance DDL is only in once-only migrations',()=>{
 const s=expand('05_agency_source_callers_and_lifecycle.sql');assert.doesNotMatch(s,/create table|alter table[^;]*add column/i);
 assert.equal((read('migration/01_same_agency_request_store.sqlinc').match(/create table /g)||[]).length,1);
 assert.equal(read('migration/01_same_agency_request_store.sqlinc')+read('repeatable/02_same_request_identity_guard.sqlinc'),read('reference/88_same_request_schema_and_guard.sqlinc'));
 assert.ok(read('migration/02_typed_existing_validation_columns.sqlinc').includes("validation_source_kind text not null default 'LEGACY'"));
});
test('SOURCE_ONLY one current Agency owner per name; all Agency callees close over selected bodies/current configured predicate',()=>{
 assert.equal(census.functions.length,60);assert.equal(new Set(census.functions.map(f=>f.function)).size,60);
 assert.deepEqual(census.agency_dependencies_from_current_canonical,['private.weekly_source_agency_invoice_check_only_v1']);
 const names=census.functions.map(f=>f.function);for(const expected of ['private.weekly_source_agency_change_capture_v1','private.weekly_source_agency_change_begin_v1','private.weekly_source_agency_change_request_assert_v1','private.weekly_source_agency_original_claim_recheck_v1','private.weekly_source_agency_draft_round_trip_v1','public.weekly_source_agency_timesheet_change_v1'])assert.ok(names.includes(expected));
});
test('SOURCE_ONLY every request rowtype field referenced by actual selected bodies is declared on the one request store',()=>{
 const s=read('migration/01_same_agency_request_store.sqlinc');const fields=new Set([...s.matchAll(/^  ([a-z][a-z0-9_]*)\s+(?:uuid|text|bytea|date|jsonb|integer|boolean|bigint|timestamptz|public\.)/gm)].map(m=>m[1]));
 for(const fn of census.functions)for(const field of fn.request_fields)assert.ok(fields.has(field),fn.function+': '+field);
});
test('SOURCE_ONLY actual common advancing-phase guard rechecks native original claim after UNA scope and before Source CAS',()=>{
 const s=read('repeatable/115_complete_current_request_assert_with_original_claim_recheck.sqlinc'),call='perform private.weekly_source_agency_original_claim_recheck_v1(v_request.id);';
 assert.equal(s.split(call).length,2);assert.ok(s.indexOf(call)>s.indexOf("if v_ts.revoked_at is not null then"));
 assert.ok(s.indexOf(call)<s.indexOf('v_cas:=private.weekly_source_bulk_source_cas_v1'));
 assert.ok(s.indexOf(call)<s.indexOf('source_admission:=private.weekly_source_agency_source_phase_admit_v1'));
 assert.ok(s.indexOf('native_admission:=private.weekly_source_agency_candidate_admit_v1')<s.indexOf('from public.invoices where id='));
});
test('SOURCE_ONLY native original claim recheck retains genuine absence/unavailability and immutable Candidate witness checks',()=>{
 const s=read('repeatable/06_actual_original_claim_recheck.sqlinc');
 for(const expected of ["if stored->>'available' is distinct from 'true' then","'available',false","'NO_ORIGINAL_SUBMISSION'","workflow.worker_submitted_at_utc is not null","private._candidate_sha256_jsonb_v1(w.immutable_submission_json)","component.component_kind='HOURS_TIMESHEET'","sig.source_content_sha256 is distinct from w.candidate_signature_sha256","jsonb_array_length(schedule)>100","AGENCY_CHANGE_ORIGINAL_CLAIM_CHANGED"])assert.ok(s.includes(expected),expected);
 assert.equal(s,read('reference/15_agency_original_claim_recheck.sqlinc'));
});
test('SOURCE_ONLY both immutable claim owners and native transitive retention fields remain present',()=>{
 const agency=read('repeatable/02_same_request_identity_guard.sqlinc');
 assert.ok(agency.includes("new.native_receipts->'original_capture' is distinct from old.native_receipts->'original_capture'"));
 assert.ok(agency.includes('not new.current_source_dependency_tokens @> old.current_source_dependency_tokens'));
 assert.ok(agency.includes("'{source_admission,transaction_id}'"));assert.ok(agency.includes("'{source_admission,backend_pid}'"));
 const source=read('reference/canonical_claim_2_13102026_1127_invariants.sqlinc');
 const mutable=source.match(/to_jsonb\(new\)-array\[([^\]]+)\]/)[1];assert.ok(!mutable.includes('frozen_original_json'));assert.ok(!mutable.includes('proposal_json'));
 assert.ok(source.includes('CORRECTION_REQUEST_ORIGINAL_PROPOSAL_IMMUTABLE'));
 const retention=read('reference/canonical_claim_3_01_physical_token_and_retention_helpers.sqlinc');assert.ok(retention.includes('frozen_original_json'));
 assert.equal(recipe.claim_authorities[1].new_selector_kind,null);
});
test('SOURCE_ONLY exact current raw05 is unchanged except accepted BEFORE delta and final guard follows both raw/nested phases',()=>{
 const early=read('repeatable/116_complete_current_source_raw05_with_active_before_union.sqlinc'),final=read('repeatable/117_current_source_final_before.sqlinc');
 const prior=read('reference/root_current_raw05.sqlinc');assert.equal(early.slice(0,early.indexOf('create or replace function private.timesheet_correction_a0_source_reference_before_v1()')),prior.slice(0,prior.indexOf('create or replace function private.timesheet_correction_a0_source_reference_before_v1()')));
 for(const s of [early,final]){assert.ok(s.indexOf('perform private.weekly_source_agency_active_source_mutation_assert_v1')>s.indexOf('perform private.timesheet_correction_a0_fence_acquire_v1(nested_tokens,false)'));assert.ok(s.includes('tokens||nested_tokens'));}
 assert.equal(recipe.root_grammar_replaced,false);assert.equal(recipe.raw05_helpers_replaced,false);
});
test('SOURCE_ONLY corrected 76 early/final trigger verifier rejects column filtered UPDATE and no whole old104 file is mounted',()=>{
 assert.equal(read('repeatable/118_current_source_raw_and_final_76_registrations.sqlinc'),read('reference/root_corrected_76_registrations.sqlinc'));
 assert.equal(read('verification/119_current_source_raw_and_final_76_verifier.sqlinc'),read('reference/root_corrected_76_verifier.sqlinc'));
 assert.ok(read('verification/119_current_source_raw_and_final_76_verifier.sqlinc').includes('tgattr'));
 assert.ok(!read('05_agency_source_callers_and_lifecycle.sql').includes('104_'));
});
test('SOURCE_ONLY new-owner verifier enumerates the exact selected function ACL closure',()=>{
 const v=read('verification/121_current_agency_source_owner_security.sql');
 for(const fn of census.functions)assert.ok(v.includes("('"+fn.function+'('),fn.function);
 for(const expected of ["has_function_privilege('anon'","has_function_privilege('authenticated'","has_function_privilege('service_role'",'acl.grantee=0','p.prosecdef','search_path=pg_catalog'])assert.ok(v.includes(expected));
});
test('SOURCE_ONLY selected fixed-policy lifecycle uses exact accepted bodies and proposed Candidate native verifier',()=>{
 const s=expand('04_agency_fixed_approval_candidate_lifecycle.sql');
 for(const expected of ['CANDIDATE_MANAGER_CONTRACT_POLICY_FIELD_INVALID','CANDIDATE_FIXED_APPROVAL_HOURS_SCOPE_INVALID','ELECTRONIC_WORKER_SIGNED','contract_approval_changes','private._candidate_policy_resolve_v1'])assert.ok(s.includes(expected));
 assert.ok(read('verification/114_candidate_native_security_verifier.sql').includes('candidate_submission_finalize_single_flight_v1'));
});
test('SOURCE_ONLY final module preserves root mounted Candidate runtime files and does not widen the finite OFF Agency operations',()=>{
 const runtime=JSON.parse(read('MOUNTED_CANDIDATE_RUNTIME_PRESERVE.json'));assert.equal(runtime.entries.length,3);for(const item of runtime.entries)assert.equal(item.action,'PRESERVE_EXACT_CURRENT_CANONICAL_BYTES');
 assert.equal(runtime.entries[0].sha256,'8c35a472595ea3f8f24049e445697c939f291f42cb928d8186138e6a765cb6dd');
 assert.equal(recipe.off_literal_agency_operations,7);assert.equal(recipe.off_allowlist_widened,false);
 assert.ok(read('MANDATORY_SHARED_OWNER_UNIONS.json').includes('101_complete_off_setup_shape_with_finite_agency.sqlinc'));
 assert.equal(recipe.native,false);assert.equal(recipe.mounted,false);assert.equal(recipe.whole_agency_complete,false);
});
