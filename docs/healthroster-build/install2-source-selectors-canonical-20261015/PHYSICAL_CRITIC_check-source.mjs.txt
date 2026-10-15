// Read-only source review. No SQL, network, database, Docker or deployment calls.
// Run: node <this file> [I2 worktree] [A0 worktree]
import fs from 'node:fs';
import path from 'node:path';
import cp from 'node:child_process';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {fileURLToPath,pathToFileURL} from 'node:url';

const ownRoot=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'../../../..');
const root=path.resolve(process.argv[2]??path.join(ownRoot,'../healthroster-install2-14102026'));
const a0=path.resolve(process.argv[3]??path.join(ownRoot,'../healthroster-v2-protection-build-13102026'));
const commit='bdc1b7cb2a9b2f0067ed0ba6dc0c96d4b0858690';
const dir='docs/healthroster-build/install2-a0-physical-source-canonical-20261015';
const oldRoot='supabase/repeatable/14102026_0519_healthroster_a0_current_native_composition_v1.sql';
const leaf='supabase/repeatable/15102026_0216_healthroster_a0_physical_source_union/';
const kernel='supabase/repeatable/15102026_0205_healthroster_source_arbiter_kernel_v1.sql';
const verifier='supabase/verification/15102026_0216_healthroster_a0_physical_source_union_verification.sql';
const sha=b=>createHash('sha256').update(b).digest('hex');
const git=(args,repo=root)=>cp.execFileSync('git',['-C',repo,...args],{maxBuffer:16e6});
const blob=(file,ref=commit)=>git(['cat-file','blob',ref+':'+file]);
const checked=new Map(),passes=[];
function pinned(file,ref=commit,savedRoot=root){
 const raw=blob(file,ref),saved=fs.readFileSync(path.join(savedRoot,file));
 assert.deepEqual(saved,raw,'saved bytes differ: '+file+' @ '+ref);
 checked.set(ref+':'+file,{commit:ref,file,bytes:raw.length,sha256:sha(raw)});return raw;
}
const text=(file,ref=commit,savedRoot=root)=>pinned(file,ref,savedRoot).toString('utf8');
const json=(file,ref=commit,savedRoot=root)=>JSON.parse(text(file,ref,savedRoot));
function once(source,before,after,label){assert.equal(source.split(before).length,2,'unique hunk: '+label);return source.replace(before,after);}
function pass(label){passes.push(label);}
assert.equal(git(['rev-parse','HEAD']).toString().trim(),commit,'review target HEAD');
const composition=json(dir+'/COMPOSITION.json'),manifest=json(dir+'/SHA256_MANIFEST.json');
assert.equal(composition.source_only,true);assert.equal(composition.deployable,false);
assert.equal(manifest.artifacts.length,24);
assert.deepEqual(fs.readdirSync(path.join(root,dir)).sort(),[...manifest.artifacts.map(e=>e.file),'SHA256_MANIFEST.json'].sort());
for(const e of manifest.artifacts){const b=pinned(dir+'/'+e.file);assert.equal(b.length,e.bytes);assert.equal(sha(b),e.sha256,e.file);}
assert.equal(composition.outputs.length,9);assert.equal(composition.inputs.length,10);
for(const e of composition.outputs){const b=pinned(e.file);assert.equal(b.length,e.bytes);assert.equal(sha(b),e.sha256,e.file);}
const inputData=new Map();
for(const e of composition.inputs){const b=pinned(e.file,e.commit,e.commit===composition.physical_pin?a0:root);assert.equal(b.length,e.bytes);assert.equal(sha(b),e.sha256,e.file);inputData.set(e.file,b);}
pass('25 archive files, nine canonical outputs and ten exact raw Git/saved input pins');

// Imports are source-only parsers; verify their complete saved bytes before use.
const parser='docs/healthroster-build/proposals/a0-current-root-install2-forward/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
for(const file of [parser,'scripts/cloudtms-managed-release-sql.mjs','scripts/cloudtms-db-release-lib.mjs'])pinned(file);
const {functionEvents}=await import(pathToFileURL(path.join(root,parser)));
const {scanManagedSql,readManagedClosure,exactAtomicBody}=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-release-sql.mjs')));
const {mapExecutableSqlSource}=await import(pathToFileURL(path.join(root,'scripts/cloudtms-db-release-lib.mjs')));
const creates=s=>functionEvents(s).events.filter(e=>e.kind==='CREATE');
const base='docs/healthroster-build/proposals/a0-hidden-rr-owner-coverage-successor-20261014/';
const reverse=JSON.parse(inputData.get(base+'COMPLETE_CURRENT_FORWARD_AND_REVERSE.json'));
assert.equal(reverse.reversals.length,4);let hunkCount=0;
for(const r of reverse.reversals){
 const before=pinned(r.input),input=inputData.get(base+r.output);assert(input);
 assert.deepEqual(before,blob(r.input,composition.input_head),'original page was changed');
 assert.equal(sha(before),r.beforeSha256);assert.equal(sha(input),r.afterSha256);
 let forward=before.toString(),back=input.toString();
 for(const h of r.hunks)forward=once(forward,h.before,h.after,h.label);
 for(const h of [...r.hunks].reverse())back=once(back,h.after,h.before,h.label);
 assert.deepEqual(Buffer.from(forward),input,'whole forward page');assert.deepEqual(Buffer.from(back),before,'whole reverse page');
 assert.deepEqual(pinned(leaf+path.posix.basename(r.input)),input,'canonical complete physical page');hunkCount+=r.hunks.length;
}
assert.equal(hunkCount,16);
const prior=readManagedClosure(oldRoot,root);
for(const e of prior.ordered){pinned(e.path);assert.deepEqual(blob(e.path),blob(e.path,composition.input_head),'original0519 closure '+e.path);}
pass('four entire physical page forward/reverse proofs (16 hunks), original0519 closure untouched');

const physical=inputData.get(base+'sql/14102026_1216_a0_transaction_fence_helpers.sqlinc').toString();
const scalar=inputData.get('docs/healthroster-build/proposals/a0-source-policy-selector-successor-20261015/forward/02_complete_scalar_helpers.sqlinc').toString();
const core='private.timesheet_correction_a0_fence_scalar_acquire_core_v1';
const dispatcher='private.timesheet_correction_a0_fence_acquire_v1(jsonb,boolean)';
const selected=creates(scalar).filter(e=>!e.id.startsWith(core+'('));assert.equal(selected.length,4);
assert.deepEqual(selected.map(e=>e.id),composition.physical_private_helpers);
for(const e of selected)assert.equal(e.definition,creates(physical).find(x=>x.id===e.id)?.definition,'unchanged physical helper '+e.id);
const helpers=scanManagedSql(scalar).statements.map(s=>scalar.slice(s.start,s.end)).filter(s=>!s.includes(core+'(')).join('\n')+'\n';
assert.equal(text(leaf+'01_physical_token_and_retention_helpers.sqlinc'),helpers);
assert.deepEqual(pinned(leaf+'09_native_child_raw_intents.sqlinc'),inputData.get(base+'sql/14102026_1216_a0_native_child_raw_intents.sqlinc'));
const module=composition.outputs.find(e=>/_v1\.sql$/.test(e.file)).file;
const ordered=['01_physical_token_and_retention_helpers.sqlinc','05_native_creator_intents_and_triggers.sqlinc','06_send_family_fence.sqlinc','08_complete_current_request.sqlinc','04_complete_native_lock_fence.sqlinc','09_native_child_raw_intents.sqlinc'];
const priorSource=text(oldRoot),acl=priorSource.slice(priorSource.indexOf('alter function private.timesheet_correction_canonical_input_assert_v1')).replace(/commit;\s*$/,'');
assert(acl.length>5000);
assert.equal(text(module),'-- Canonical source union, not yet qualified for release.\nbegin;\n'+ordered.map(f=>'\\ir 15102026_0216_healthroster_a0_physical_source_union/'+f).join('\n')+'\n'+acl+'commit;\n');
const union=readManagedClosure(module,root),k=readManagedClosure(kernel,root);
for(const e of k.ordered)pinned(e.path);
for(const e of union.ordered)pinned(e.path);
assert.equal(union.closureHash,composition.repeatable_closure_sha256);
assert(!creates(union.expanded).some(e=>e.id===dispatcher||e.id.startsWith(core+'(')),'old dispatcher or core reintroduced');
const currentDispatcher=creates(k.expanded).find(e=>e.id===dispatcher);assert(currentDispatcher);
assert.equal(sha(currentDispatcher.body),'37eeb5ac3857fb815126e71b725eb264d20ec958ae7a4473248d1425c18abb1b');
assert.notEqual(sha(currentDispatcher.body),sha(creates(physical).find(e=>e.id===dispatcher).body));
assert.equal((union.expanded.match(/create trigger /gi)||[]).length,14);
assert.equal((text(leaf+'09_native_child_raw_intents.sqlinc').match(/create trigger /gi)||[]).length,9);
assert(!/pg_catalog\.(coalesce|nullif|least|greatest)\s*\(/i.test(union.expanded));
pass('four unchanged d48 helpers, exact nine-child guard, six-page order and complete original ACL tail; current Source dispatcher retained');

let contract=inputData.get(base+'sql/14102026_1216_a0_transaction_fence_source_contract.sql').toString();
let bodies=inputData.get(base+'sql/14102026_1216_a0_transaction_fence_reviewed_bodies.sqlinc').toString();
assert.equal(composition.contract_successor_changes.length,6);
for(const h of composition.contract_successor_changes){
 if(h.label==='use current Source dispatcher body, preserve all other forty expectations')bodies=once(bodies,h.before,h.after,h.label);
 else contract=once(contract,h.before,h.after,h.label);
}
const initial='-- Retains all physical-owner catalogue checks against the Source arbiter schema.\n-- The separate mandatory Source kernel verifier also proves its five helpers and coverage exclusion.\nbegin;\n'+contract+'\nrollback;\n';
assert.equal(text(dir+'/INITIAL_PHYSICAL_VERIFIER.sql.txt'),initial);
const fix=json(dir+'/NATIVE_BINDING_VERIFIER_FIX.json');assert.equal(fix.hunks.length,2);
assert.equal(sha(initial),fix.before_sha256);let final=initial;
for(const h of fix.hunks)final=once(final,h.before,h.after,h.label);
assert.equal(text(verifier),final);assert.equal(sha(final),fix.after_sha256);
for(const h of [...fix.hunks].reverse())final=once(final,h.after,h.before,h.label);
assert.equal(final,initial,'entire verifier reverse');
assert.equal(text('supabase/verification/15102026_0216_healthroster_a0_physical_source_union/02_reviewed_bodies.sqlinc'),bodies);
assert.equal([...bodies.matchAll(/\('([^']+)','([0-9a-f]{64})'\)/g)].length,41);
const fixedVerifier=text(verifier);
const rawSection=fixedVerifier.slice(fixedVerifier.indexOf("('timesheet_validations','zzzz_healthroster"),fixedVerifier.indexOf("('candidate_approval_requests','candidate_manager_request_mail_guard"));
assert.equal([...rawSection.matchAll(/\('[^']+','[^']+','[^']+'\)/g)].length,14);
const nativeSection=fixedVerifier.slice(fixedVerifier.indexOf("('candidate_approval_requests','candidate_manager_request_mail_guard"),fixedVerifier.indexOf(') e(table_name,trigger_name,signature,definition)'));
assert.equal([...nativeSection.matchAll(/\('([^']+)','([^']+)','([^']+)','CREATE TRIGGER /g)].length,31);
assert(fixedVerifier.includes('CORRECTION_A0_UNKNOWN_NATIVE_BEFORE_OWNER'));
const kernelVerifier='supabase/verification/15102026_0205_healthroster_source_arbiter_kernel_verification.sql';
const kernelProof=readManagedClosure(kernelVerifier,root);for(const e of kernelProof.ordered)pinned(e.path);
const beforeRelease=json(dir+'/CURRENT_RELEASE_BEFORE.json'),release=json('supabase/release/current-release.json');
assert.equal(sha(blob('supabase/release/current-release.json',composition.input_head)),composition.release.before_sha256);
assert.deepEqual(release,{...beforeRelease,verificationFiles:[...beforeRelease.verificationFiles,verifier],newVerificationFiles:[...beforeRelease.newVerificationFiles,verifier]});
assert.equal(release.verificationFiles.length,175);assert.equal(release.newVerificationFiles.length,176);
for(const key of ['verificationFiles','newVerificationFiles'])assert(release[key].includes(kernelVerifier));
const schema=text('supabase/migrations/15102026_0205_healthroster_source_arbiter_kernel.sql');
assert.equal((schema.match(/constraint timesheet_correction_a0_fences_\w+ exclude using gist/g)||[]).length,3);
assert(schema.includes('coverage_span daterange'));assert(fixedVerifier.includes('not a.attisdropped)<>7'));
assert(fixedVerifier.includes('where c.conrelid=v_oid)<>5'));
pass('complete physical verifier reconstruction, exact two-hunk reverse, all14 raw/31 native/41 body checks, both mandatory constructors and release prefixes');

const diag=json(dir+'/READONLY_BINDING_DIAG_RESULT.json');
assert(diag.accepted&&diag.rolledBack&&diag.rollbackVerification.connectedFresh);
assert.equal(diag.database,'cloudtms_test_clone');assert.equal(diag.statements.length,1);
const bindings=diag.statements[0].rows;assert.equal(bindings.length,31);assert(bindings.every(e=>e.function_matches===true));
assert.deepEqual(bindings.filter(e=>e.tgenabled!=='O').map(e=>[e.table_name,e.trigger_name,e.tgenabled]),[['timesheets','zz_bpay_timesheet_work_key_v1','A']]);
for(const r of bindings){let expected=r.expected.replace(' ON '+r.table_name+' ',' ON public.'+r.table_name+' ');
 if(r.trigger_name==='weekly_source_managed_root_authorisation_guard_bu')expected=expected.replace('WHEN (old.authorised_at_server IS DISTINCT FROM new.authorised_at_server)','WHEN ((old.authorised_at_server IS DISTINCT FROM new.authorised_at_server))');
 assert.equal(r.observed,expected,r.trigger_name);assert(nativeSection.includes("'"+expected.replace(' ON public.'+r.table_name+' ',' ON '+r.table_name+' ')+"'"));}
const alwaysSource=text('supabase/migrations/26092026_0200_banking_pay_stage2_work_reference_schema.sql');
assert.equal(alwaysSource.split('\n')[172],'ALTER TABLE public.timesheets ENABLE ALWAYS TRIGGER zz_bpay_timesheet_work_key_v1;');
pass('31 recorded native bindings agree exactly after representation fix; the sole Bank ALWAYS state is immutable source line173');

// Rebuild the entire finite trial text without executing it or its archived generator.
const trialManifest=json(dir+'/RETRY01_TRIAL_MANIFEST.json');
const closures=trialManifest.files.map(e=>{const c=readManagedClosure(e.file,root);for(const f of c.ordered)pinned(f.path);assert.equal(c.closureHash,e.closure_sha256);return {...c,file:e.file};});
const trialVerifiers=trialManifest.verifiers.map(e=>{const c=readManagedClosure(e.file,root);assert.equal(c.closureHash,e.closure_sha256);return c;});
const existingIds=new Set(closures.flatMap(c=>creates(c.expanded).map(e=>e.id)));
const prerequisites=creates(prior.expanded).filter(e=>!existingIds.has(e.id));assert.equal(prerequisites.length,10);
assert.deepEqual(prerequisites.map(e=>({signature:e.id,definition_sha256:sha(e.definition)})),trialManifest.prior_source.exact_complete_prerequisite_declarations);
let mapped;const priorOwner=process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER;
try{process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER='CURRENT_USER';mapped=closures.map(c=>mapExecutableSqlSource(exactAtomicBody(c.expanded),c.file));}
finally{if(priorOwner===undefined)delete process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER;else process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER=priorOwner;}
const probe=text(dir+'/ABSENCE.sql');
const vacancy="do $vacancy$ begin if to_regclass('private.timesheet_correction_requests') is not null or to_regclass('private.timesheet_correction_a0_transaction_fences') is not null then raise exception 'EXPECTED_NEW_A0_SCHEMA_NOT_VACANT';end if;end $vacancy$;\n";
const withoutOuter=sql=>scanManagedSql(sql).statements.filter(s=>!['BEGIN','COMMIT','ROLLBACK'].includes(s.tokens[0])).map(s=>sql.slice(s.start,s.end)).join('\n');
const trial='set local jit=off;\nset local pg_show_plans.is_enabled=off;\n'+probe+vacancy+mapped[0]+'\n'+prerequisites.map(e=>e.definition).join('\n')+'\n'+mapped.slice(1).join('\n')+'\n'+trialVerifiers.map(c=>withoutOuter(c.expanded)).join('\n')+'\nselect true as canonical_physical_module_and_complete_verifiers_passed;\n';
assert.equal(trial,text(dir+'/RETRY01_TRIAL.sql'));assert.equal(sha(trial),trialManifest.sql_sha256);
assert.equal(Buffer.byteLength(trial),214864);assert.equal(scanManagedSql(trial).statements.length,177);
assert.equal(sha(probe),trialManifest.verification_sha256);
for(const [name,accepted,length] of [['INITIAL_NATIVE_RESULT.json',false,175],['RETRY01_NATIVE_RESULT.json',true,177]]){
 const result=json(dir+'/'+name),stem=name.startsWith('INITIAL')?'INITIAL':'RETRY01',m=json(dir+'/'+stem+'_TRIAL_MANIFEST.json');
 assert.equal(result.accepted,accepted);assert.equal(result.database,'cloudtms_test_clone');assert.equal(result.sqlSha256,m.sql_sha256);assert.equal(result.statementCount,177);
 assert(result.rolledBack&&result.rollbackVerification.connectedFresh);assert.equal(result.rollbackVerification.database,'cloudtms_test_clone');assert.equal(result.statements.length,length);
 const before=result.statements[2].rows[0],after=result.rollbackVerification.statements[0].rows[0];assert.deepEqual(before,after);assert(before.request_absent&&before.arbiter_absent);
 const errors=result.statements.filter(e=>e.error);assert.equal(errors.length,accepted?0:1);
 if(accepted){assert.equal(result.statements[173].command,'DO');assert.equal(result.statements[174].command,'DO');assert.equal(result.statements[175].command,'DO');assert.equal(result.statements[176].rows[0].canonical_physical_module_and_complete_verifiers_passed,true);}
 else{assert.equal(errors[0].statement,175);assert.equal(errors[0].error.message,'CORRECTION_A0_NATIVE_BEFORE_OWNER_CHANGED');assert.equal(errors[0].error.code,'55000');}
}
const initialTrial=text(dir+'/INITIAL_TRIAL.sql');let priorTrial=trial;
for(const h of [...fix.hunks].reverse())priorTrial=once(priorTrial,h.after,h.before,h.label);
assert.equal(priorTrial,initialTrial,'trial delta is exactly the two verifier hunks');
const journal=text(dir+'/SHARED_TURN_JOURNAL.txt').trim().split('\n').map(s=>JSON.parse(s.slice(s.indexOf('{'))));
assert.equal(journal.length,6);
for(let i=0;i<6;i+=2){const taken=journal[i],released=journal[i+1];assert.equal(taken.status,'TAKEN');assert.equal(released.status,'RELEASED');assert.equal(taken.task,released.task);assert.equal(taken.takenAt,released.takenAt);assert.deepEqual(taken.key,[20261013,1]);assert(released.explicitUnlockConfirmed&&released.connectionClosed&&released.proved);}
pass('whole177-statement trial rebuilt exactly; initial failure, diagnostic, retry PASS, fresh restoration fingerprints and three released shared turns reconciled');

for(const e of [...checked.values()])pinned(e.file,e.commit,e.commit===composition.physical_pin?a0:root);
assert.equal(git(['rev-parse','HEAD']).toString().trim(),commit,'review target changed during check');
pass('all65 raw Git/saved pins and reviewed HEAD checked again after assertions');
console.log(JSON.stringify({verdict:'PASS_BOUNDED_SOURCE_REVIEW',reviewed_commit:commit,checks:passes,raw_saved_pins:checked.size,physical_page_hunks:hunkCount,artifacts:25,canonical_outputs:9,raw_guards:14,native_bindings:31,body_expectations:41,recorded_retry_statements:177,recorded_retry_bytes:214864,fresh_database_calls:0,full_rehearsal:false,final_part_b_verdict:false,source_pins:[...checked.values()]},null,2));
