// Source-only mocks: no native database, provider or runtime acceptance.
import test,{after} from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {execFileSync} from 'node:child_process';
import {prepareF5NativeSetup,f5NativeSetupDiscoverySql} from './forward/healthroster-f5-native-setup.mjs';
const here=path.dirname(fileURLToPath(import.meta.url));
const root='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/healthroster-install2-14102026';
let helperSource=fs.readFileSync(path.join(here,'forward/healthroster-f5-synthetic-fixture.mjs'),'utf8');
helperSource=helperSource.replace("'xlsx'",JSON.stringify(pathToFileURL(path.join(root,'node_modules/xlsx/xlsx.mjs')).href));
for(const name of ['index.js','upload-staging-adapter.mjs'])helperSource=helperSource.replace("'../broker/src/weekly-source/"+name+"'",JSON.stringify(pathToFileURL(path.join(root,'broker/src/weekly-source',name)).href));
const helperUrl='data:text/javascript;base64,'+Buffer.from(helperSource).toString('base64');
const {generateF5SyntheticFixture,compileF5SyntheticVerifier,assertF5SyntheticProof,f5ParserInputs}=await import(helperUrl);
// Test the proposed mount bytes with the actual unchanged protected owners.
let gateSource=fs.readFileSync(path.join(here,'forward/healthroster-f5-post-verified-gate.mjs'),'utf8');
for(const name of ['weekly-source-protected-execution-tree.mjs','weekly-source-post-verified-evidence-controller.mjs'])
 gateSource=gateSource.replace("'./"+name+"'",JSON.stringify(pathToFileURL(path.join(root,'scripts',name)).href));
gateSource=gateSource.replace("'./healthroster-f5-synthetic-fixture.mjs'",JSON.stringify(helperUrl));
const gateUrl='data:text/javascript;base64,'+Buffer.from(gateSource).toString('base64');
const {f5Verifier,partitionF5VerificationFiles,runF5PostVerifiedGate,assertF5AuthorityCompatibility}=await import(gateUrl);
const desktopSource=fs.readFileSync(path.join(here,'forward/healthroster-f5-prepublication.mjs'),'utf8')
 .replaceAll("'./healthroster-f5-post-verified-gate.mjs'",JSON.stringify(gateUrl))
 .replace("'./healthroster-f5-synthetic-fixture.mjs'",JSON.stringify(helperUrl));
const {validateF5PrepublicationReceipt,loadF5PrepublicationReceipt}=await import('data:text/javascript;base64,'+Buffer.from(desktopSource).toString('base64'));
const {proveCommittedSources,sourceInventory}=await import(pathToFileURL(path.join(root,'scripts/weekly-source-protected-execution-tree.mjs')).href);
const {domainHash,sha256}=await import(pathToFileURL(path.join(root,'scripts/weekly-source-protected-release-qualification.mjs')).href);
const id=n=>`f5520000-0000-4000-8000-${String(n).padStart(12,'0')}`;
const reportRoot=path.resolve(here,'../../../..'),unitBase=path.join(reportRoot,'.codex-tmp');fs.mkdirSync(unitBase,{recursive:true});
const executionRoot=fs.mkdtempSync(path.join(unitBase,'f5-gate-unit-'));
const unitGit=args=>execFileSync('git',['-c','user.name=F5 source fixture','-c','user.email=f5-source@example.invalid',...args],{cwd:executionRoot,encoding:'utf8',stdio:['ignore','pipe','pipe']}).trim();
unitGit(['init','--quiet']);const verifierFile=path.join(executionRoot,f5Verifier);fs.mkdirSync(path.dirname(verifierFile),{recursive:true});
fs.writeFileSync(verifierFile,fs.readFileSync(path.join(here,'fixtures/f5-post-verified.sql')));fs.writeFileSync(path.join(executionRoot,'.gitignore'),'.codex-tmp/\n');
unitGit(['add','.']);unitGit(['commit','-m','Explicit synthetic F5 gate source unit']);
after(()=>{const real=fs.realpathSync(executionRoot),base=fs.realpathSync(unitBase);if(path.dirname(real)!==base||!path.basename(real).startsWith('f5-gate-unit-'))throw Error('SOURCE_TEST_CLEANUP_SCOPE_INVALID');fs.rmSync(real,{recursive:true,force:true});});
const sourceCommit=unitGit(['rev-parse','HEAD']),contract='2'.repeat(64);
const fixtureContext={authorization:{schema_version:'WEEKLY_SOURCE_OFF_RULE_TEST_AUTHORIZATION_V1',purpose:'SOURCE_FIXTURE_SETUP',environment:'TEST',
 source_cycle_id:id(3),source_group_id:id(2),agency_id:id(4),journey_actor_user_id:id(1),week_ending:'2026-10-11',fixture_manifest:{client_ids:[id(5)]},
 allowed_operations:['weekly_source_upload_stage_begin_atomic_v1','weekly_source_upload_stage_rows_atomic_v1','weekly_source_upload_seal_atomic_v1','weekly_source_invoice_hold_release_atomic_v1']},
 observed_at_utc:'2026-10-16T12:00:00Z',cycle:{id:id(3),source_group_id:id(2),scope_client_id:id(5),finalisation_week_ending:'2026-10-11',
 cutoff_at_utc:'2026-10-14T22:59:00Z',previous_cutoff_at_utc:'2026-10-07T22:59:00Z',state:'OPEN',current_complete_upload_id:null,current_projection_publication_id:null,current_final_revision_id:null}};
const state={database:'cloudtms_test_clone',execution:{release_id:'synthetic-source-only-release',source_commit:sourceCommit,contract_sha256:contract},
 restoration_sha256:'3'.repeat(64),fixture_vacant:true,authority_registrations:[],f5_fixture_context:fixtureContext};
const sql=fs.readFileSync(path.join(here,'forward/f5-restoration.sqlinc'),'utf8');
const gateArgs={root:executionRoot,sourceCommit,expectedContractSha256:contract,restorationSql:sql};
test('both original mandatory lists retain F5 exactly once; only its execution phase changes',()=>{
 const actualRelease=JSON.parse(fs.readFileSync(path.join(root,'supabase/release/current-release.json')));
 for(const key of ['verificationFiles','newVerificationFiles'])assert.equal(actualRelease[key].filter(file=>file===f5Verifier).length,1);
 const release={verificationFiles:['before',f5Verifier,'after'],newVerificationFiles:[f5Verifier,'new']};
 assert.deepEqual(partitionF5VerificationFiles(release,'UPGRADE'),{preVerified:['before','after'],postVerified:[f5Verifier]});
 assert.deepEqual(partitionF5VerificationFiles(release,'NEW'),{preVerified:['new'],postVerified:[f5Verifier]});
 assert.equal(release.verificationFiles.includes(f5Verifier),true);
 for(const invalid of [{...release,newVerificationFiles:['new']},{...release,verificationFiles:[f5Verifier,f5Verifier]}])
  assert.throws(()=>partitionF5VerificationFiles(invalid,'UPGRADE'),/REGISTRATION_REQUIRED/);
});
test('mandatory post-VERIFIED native rollback file precedes success and fresh restoration',async()=>{
 const calls=[];const psql=arg=>{calls.push(arg);return arg.sql===sql?JSON.stringify(state):'controlled source-test SQL output';};
 const receipt=await runF5PostVerifiedGate({...gateArgs,psql});
 assert.deepEqual(calls.map(x=>Object.keys(x)[0]),['sql','sql','sql']);assert.ok(calls[1].sql.includes('actual stage begin'));
 assert.equal(receipt.qualification,'ROLLBACK_SQL_PASS');assert.equal(receipt.provider_or_runtime_acceptance,false);
});
for(const [name,change] of [['wrong database',{database:'uofvkfi5'}],['dirty fixture',{fixture_vacant:false}],
 ['stale source',{execution:{...state.execution,source_commit:'4'.repeat(40)}}],['stale contract',{execution:{...state.execution,contract_sha256:'5'.repeat(64)}}]]){
 test('publication gate refuses '+name+' before native positive execution',async()=>{
  let files=0;await assert.rejects(runF5PostVerifiedGate({...gateArgs,psql:arg=>{if(arg.sql!==sql)files++;return JSON.stringify({...state,...change});}}),/ACTUAL_VERIFIED_EXECUTION|DATABASE_SOURCE_AUTHORITY_CHANGED/);assert.equal(files,0);
 });
}
test('native positive failure cannot return success or proceed to publication',async()=>{
 await assert.rejects(runF5PostVerifiedGate({...gateArgs,psql:arg=>{if(arg.sql!==sql)throw Error('actual native assertion refused');return JSON.stringify(state);}}),/native assertion refused/);
});
test('fresh rollback restoration mismatch holds publication',async()=>{
 let reads=0;await assert.rejects(runF5PostVerifiedGate({...gateArgs,psql:arg=>arg.sql===sql?JSON.stringify(++reads===1?state:{...state,restoration_sha256:'f'.repeat(64)}):''}),/RESTORATION_FAILED/);
});
test('exact unchanged route separately binds actual distinct Git commits with equal managed authority',async()=>{
 // Real source-only receipt commits in this repository. Their authority trees
 // are measured by Git; the mock database result is explicitly synthetic.
 const publication='2c9452d03a37690daaeb79fa93fab9c5c5da56a7',installed='eba70f2a7bc2fba173a6644fb696cdae65c2b460';
 const result=assertF5AuthorityCompatibility(root,installed,publication);
 assert.match(result.sha256,/^[a-f0-9]{64}$/);assert.equal(result.registration,null);
});
const gate=await runF5PostVerifiedGate({...gateArgs,psql:arg=>arg.sql===sql?JSON.stringify(state):'source-only controlled result'});
const run={id:12345,head_sha:sourceCommit,event:'workflow_dispatch',status:'completed',conclusion:'success'};
const autoReceipt={formatVersion:1,environment:'TEST',database:'cloudtms_test_clone',status:'DATABASE_READY',commit:sourceCommit,
 completedAt:'2026-10-16T12:00:00Z',f5Gate:gate,databaseRelease:{release_id:gate.release_id,source_commit:gate.database_source_commit,contract_sha256:gate.contract_sha256}};
test('desktop validates the actual returned AUTO gate receipt and source authority',()=>{
 assert.equal(validateF5PrepublicationReceipt({root:executionRoot,run,automaticReceipt:autoReceipt,expectedCommit:sourceCommit,expectedContractSha256:contract}).protected_run_id,run.id);
});
for(const [name,value] of [['missing gate',{...autoReceipt,f5Gate:undefined}],['plan only',{...autoReceipt,status:'PLANNED'}],
 ['malformed restoration',{...autoReceipt,f5Gate:{...gate,restoration_sha256:'not-a-sha'}}],
 ['foreign source',{...autoReceipt,f5Gate:{...gate,source_commit:'f'.repeat(40)}}],
 ['foreign contract',{...autoReceipt,f5Gate:{...gate,contract_sha256:'f'.repeat(64)}}],
 ['foreign release',{...autoReceipt,f5Gate:{...gate,release_id:'another-native-release'}}],
 ['forged authority',{...autoReceipt,f5Gate:{...gate,database_authority_sha256:'f'.repeat(64)}}],
 ['unexpected gate key',{...autoReceipt,f5Gate:{...gate,skip:true}}]]){
 test('desktop prepublication refuses '+name,()=>{
  assert.throws(()=>validateF5PrepublicationReceipt({root:executionRoot,run,automaticReceipt:value,expectedCommit:sourceCommit,expectedContractSha256:contract}));
 });
}
test('desktop requires the selected successful source-bound dispatch and one genuine run artifact',async()=>{
 assert.throws(()=>validateF5PrepublicationReceipt({root:executionRoot,run:{...run,head_sha:'f'.repeat(40)},automaticReceipt:autoReceipt,expectedCommit:sourceCommit,expectedContractSha256:contract}));
 await assert.rejects(loadF5PrepublicationReceipt({root:executionRoot,command:async()=>'',api:async()=>({total_count:0,artifacts:[]}),gh:'source-only-gh',run,
  expectedCommit:sourceCommit,expectedContractSha256:contract}),/ONE_CURRENT_AUTO_ARTIFACT/);
});
test('expired or duplicate protected run artifacts cannot grant publication',async()=>{
 const name='automatic-test-release-'+sourceCommit;
 for(const artifacts of [[{id:1,name,expired:true}],[{id:1,name,expired:false},{id:2,name,expired:false}]]){
  await assert.rejects(loadF5PrepublicationReceipt({root:executionRoot,command:async()=>{throw Error('must not download');},
   api:async()=>({total_count:artifacts.length,artifacts}),gh:'source-only-gh',run,expectedCommit:sourceCommit,expectedContractSha256:contract}),/ONE_CURRENT_AUTO_ARTIFACT/);
 }
});
test('desktop retrieves the exact selected run artifact before accepting returned native proof',async()=>{
 const tempBase=path.join(executionRoot,'.codex-tmp');let destination;
 try{
  const calls=[];
  const proof=await loadF5PrepublicationReceipt({root:executionRoot,gh:'source-only-gh',run,expectedCommit:sourceCommit,expectedContractSha256:contract,
   api:async url=>{calls.push(url);return {total_count:1,artifacts:[{id:67890,name:'automatic-test-release-'+sourceCommit,expired:false}]};},
   command:async (binary,args)=>{calls.push([binary,args]);destination=args[args.indexOf('--dir')+1];fs.writeFileSync(path.join(destination,'automatic-test-release.json'),JSON.stringify(autoReceipt));}});
  assert.equal(proof.protected_artifact_id,67890);
  assert.ok(calls[0].includes('/actions/runs/'+run.id+'/artifacts'));
 }finally{
  if(destination){const resolved=fs.realpathSync(destination),base=fs.realpathSync(tempBase);
   if(path.dirname(resolved)!==base||!path.basename(resolved).startsWith('f5-prepublication-'+run.id+'-'))throw Error('SOURCE_TEST_CLEANUP_SCOPE_INVALID');
   fs.rmSync(resolved,{recursive:true,force:true});}
 }
});
const assignment={id:id(7),version:1,feature_enabled:false,rule_set_version_id:id(8),values_sha256:'b'.repeat(64),values_json:{
 checking_profile_kind:'HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1',invoice_sender:'CLIENT_SELF_BILL',invoice_amount_basis:'SOURCE_AMOUNT',
 source_amount_basis:'EXPORT_ACTUAL_COST_INC_VAT_NET_AGENCY_WORKER_PAY',invoice_membership:'FINALISED_IN_CYCLE_WINDOW',
 line_vat_method:'LINE_GROSS_MINUS_NET_FROM_SOURCE',rate_difference_policy:'FLAG_ALLOW_IGNORE'}};
const controls={schema_version:'WEEKLY_SOURCE_OFF_RULE_TEST_CONTROLS_V1',release_id:state.execution.release_id,source_commit:sourceCommit,
 contract_sha256:contract,banking_install_receipt_sha256:'c'.repeat(64),security_receipt_sha256:'d'.repeat(64),source_effects_review_sha256:'e'.repeat(64),
 banking_first_install_complete:true,effective_acl_pass:true,browser_denied:true,source_effects_no_external_calls:true};
const observed={execution:{...state.execution,environment:'TEST',database:'cloudtms_test_clone'},controls,
 expires_at_utc:'2026-10-16T12:00:00Z',agency_id:id(4),group_version:'1',client_id:id(5),group_code:'WSQ_TEST_F5_SOURCE_ONLY',assignment_count:1,
 week_ending:'2026-10-11',future_week_ending:'2026-11-01',future_assignment_count:0,future_assignment:null,
 assignment,assignment_version:'1',assignment_sha256:'a'.repeat(64),fixture_manifest:{group_code:'WSQ_TEST_F5_SOURCE_ONLY',client_ids:[id(5)],
 contract_ids:[id(6)],candidate_ids:[id(9)],client_refs:[{client_id:id(5),cli_ref:'CLI-91301301'}],candidate_refs:[{candidate_id:id(9),tms_ref:'CCR-91301301'}]}};
// Reuse the actual existing pure managed builder. Only its approved proxy import
// is redirected to the exact finite successor in this proposal.
let builderSource=fs.readFileSync(path.join(root,'scripts/weekly-source-off-rule-setup-evidence.mjs'),'utf8');
builderSource=builderSource.replace("'./weekly-source-protected-release-qualification.mjs'",JSON.stringify(pathToFileURL(path.join(root,'scripts/weekly-source-protected-release-qualification.mjs')).href))
 .replace("'../broker/src/weekly-source/off-rule-native-proxy.mjs'",JSON.stringify(pathToFileURL(path.join(here,'forward/off-rule-native-proxy.mjs')).href));
const {buildOffRuleTestAuthorization}=await import('data:text/javascript;base64,'+Buffer.from(builderSource).toString('base64'));
const setupArgs={actorUserId:id(1),sourceGroupId:id(2),sourceCycleId:id(3),buildAuthorization:buildOffRuleTestAuthorization,turnBackendPid:12345};
test('existing genuine OFF assignment capture feeds actual managed builder without activation',async()=>{
 const calls=[];const result=await prepareF5NativeSetup({...setupArgs,session:{execute:async query=>{calls.push(query);return query.startsWith('with native')?JSON.stringify(observed):'';}}});
 assert.equal(result.qualified,false);assert.equal(result.input.assignment_sha256,observed.assignment_sha256);
 assert.equal(result.authorization.schema_version,'WEEKLY_SOURCE_OFF_RULE_TEST_AUTHORIZATION_V1');
 assert.ok(result.input.allowed_operations.includes('weekly_source_invoice_hold_release_atomic_v1'));
 assert.equal(calls.some(q=>q.includes('feature_set_atomic')),false);
});
test('missing actual fixture participants never fabricates candidate/contract/assignment or authorisation',async()=>{
 const calls=[];await assert.rejects(prepareF5NativeSetup({...setupArgs,session:{execute:async query=>{calls.push(query);return '';}}}),/GENUINE_FIXTURE_PARTICIPANTS/);
 assert.equal(calls.some(q=>q.includes('version_save_atomic')||q.includes('assign_atomic')),false);
});
test('genuine future reference and native historical assignment owners use real turn authority and re-read state',async()=>{
 let reads=0;const calls=[];
 const result=await prepareF5NativeSetup({...setupArgs,session:{execute:async query=>{
  calls.push(query);if(query.startsWith('with native'))return JSON.stringify(++reads===1?{...observed,assignment_count:0,assignment:null}:
   reads===2?{...observed,assignment_count:0,assignment:null,future_assignment_count:1,future_assignment:assignment,future_assignment_sha256:'a'.repeat(64)}:observed);
  if(query.includes('version_save_atomic'))return JSON.stringify({rule_set_version_id:assignment.rule_set_version_id});
  if(query.includes('assign_atomic'))return JSON.stringify({assignment_id:assignment.id});return '';
 }}});
 assert.equal(reads,3);assert.equal(result.input.assignment_version,'1');
 assert.equal(calls.filter(q=>q.startsWith('select public.')).length,2);
 assert.equal(calls.filter(q=>q.startsWith('select private.weekly_source_historical_fixture_rule_assign_v1(')).length,1);
 assert.equal(calls.some(q=>/feature_enabled.*true|status='VERIFIED'|insert into public\.(candidates|contracts|audit_events)/i.test(q)),false);
});
test('past setup without the actual separate turn-lock PID stops before mutation',async()=>{
 const calls=[];await assert.rejects(prepareF5NativeSetup({...setupArgs,turnBackendPid:undefined,session:{execute:async q=>{
  calls.push(q);return q.startsWith('with native')?JSON.stringify({...observed,assignment_count:0,assignment:null}):'';
 }}}),/ACTUAL_SEPARATE_TURN_LOCK_PID/);
 assert.equal(calls.some(q=>q.includes('version_save_atomic')||q.includes('historical_fixture_rule_assign_v1(')),false);
});
test('fresh past-window workbook uses the real parser/adapter and preserves the original economic cases',async()=>{
 const generated=await generateF5SyntheticFixture(fixtureContext);
 assert.equal(assertF5SyntheticProof(generated.proof),generated.proof);
 assert.equal(generated.begin_request.content_sha256,generated.proof.source_file_sha256);
 assert.notEqual(generated.proof.source_file_sha256,'4b9aea615d5c56577f9e4e6b1281ce8d35e5e0f3ec9d69e2682a5acac2aa1c1d');
 assert.equal(generated.stage_request.normalised_rows[0].work_date,'2026-10-05');
 assert.equal(generated.stage_request.normalised_rows[0].source_gross_pence,'12002');
 assert.equal(generated.stage_request.money_evidence.length,2);
 const compiled=compileF5SyntheticVerifier({...gateArgs,verifier:f5Verifier,generated});
 assert.ok(compiled.sql.includes('source_bytes_base64'));assert.ok(compiled.sql.includes('exact hold replay'));
 const file=path.join(executionRoot,f5Verifier),original=fs.readFileSync(file);fs.appendFileSync(file,'\n-- altered native assertion');
 try{assert.throws(()=>compileF5SyntheticVerifier({...gateArgs,verifier:f5Verifier,generated}),/SOURCE_MISMATCH/);}finally{fs.writeFileSync(file,original);}
 for(const changed of [{...fixtureContext,observed_at_utc:'2026-10-01T00:00:00Z'},
  {...fixtureContext,cycle:{...fixtureContext.cycle,current_complete_upload_id:id(99)}},
  {...fixtureContext,cycle:{...fixtureContext.cycle,previous_cutoff_at_utc:fixtureContext.cycle.cutoff_at_utc}},
  {...fixtureContext,authorization:{...fixtureContext.authorization,week_ending:'2026-11-01'}}])
  await assert.rejects(generateF5SyntheticFixture(changed),/REAL_PAST_OPEN_CYCLE_REQUIRED/);
 assert.throws(()=>assertF5SyntheticProof({...generated.proof,source_file_sha256:'f'.repeat(64)}),/PROOF_INVALID/);
});
test('finite F5 authority inputs equal the actual parser/adapter relative dependency closure',()=>{
 const seen=new Set();function walk(file){if(seen.has(file))return;seen.add(file);
  for(const m of fs.readFileSync(path.join(root,file),'utf8').matchAll(/(?:from\s*|import\s*)['"]([^'"]+)['"]/g))
   if(m[1].startsWith('.'))walk(path.posix.normalize(path.posix.join(path.posix.dirname(file),m[1])));
 }
 walk('broker/src/weekly-source/index.js');walk('broker/src/weekly-source/upload-staging-adapter.mjs');
 assert.deepEqual([...seen].sort(),[...f5ParserInputs].sort());
});
test('enabled assignment and false native controls cannot mint an authorisation',async()=>{
 for(const value of [{...observed,assignment:{...assignment,feature_enabled:true}},{...observed,controls:{...controls,browser_denied:false}}]){
  await assert.rejects(prepareF5NativeSetup({...setupArgs,session:{execute:async q=>q.startsWith('with native')?JSON.stringify(value):''}}));
 }
 assert.throws(()=>f5NativeSetupDiscoverySql({...setupArgs,sourceCycleId:'not-a-uuid'}),/IDENTITIES_REQUIRED/);
});
test('all original native positive and negative F5 checks survive without feature enable or fake approval',()=>{
 const fixture=fs.readFileSync(path.join(here,'fixtures/f5-post-verified.sql'),'utf8');
 for(const assertion of ['actual stage begin','actual money evidence stage','actual immutable seal','native stored literal money and real timestamp',
  'native bounded default hold release','exact hold replay','WEEKLY_SOURCE_INVOICE_HOLD_RELEASE_REPLAY_CONFLICT',
  'WEEKLY_SOURCE_SETTINGS_ADMIN_REQUIRED','WEEKLY_SOURCE_INVOICE_HOLD_SCOPE_STALE','WEEKLY_SOURCE_IMMUTABLE_RECORD','two exact source evidence cells; no fee'])assert.ok(fixture.includes(assertion),assertion);
 assert.equal(fixture.includes("'feature_enabled',true"),false);assert.ok(fixture.trim().endsWith('rollback;'));
 assert.ok(fixture.includes('exact original TS/TF/workflow/approval/component/invoice/document equality'));
 assert.ok(fixture.includes('actual fixture remains operationally OFF'));
 assert.ok(!/update private\.cloudtms_database_releases|insert into public\.audit_events/i.test(fixture));
});
test('real Git evidence-registration commits use ONLY the existing complete protected normalization',()=>{
 // Real temporary Git commits and the real protected proof/receipt decoder;
 // the database audit response is explicitly synthetic source-test input.
 const reportRoot=path.resolve(here,'../../../..'),base=path.join(reportRoot,'.codex-tmp');
 fs.mkdirSync(base,{recursive:true});const temporary=fs.mkdtempSync(path.join(base,'f5-equivalence-unit-'));
 const git=args=>execFileSync('git',['-c','user.name=F5 source fixture','-c','user.email=f5-source@example.invalid',...args],
  {cwd:temporary,encoding:'utf8',stdio:['ignore','pipe','pipe']}).trim();
 const write=(file,bytes)=>{const target=path.join(temporary,file);fs.mkdirSync(path.dirname(target),{recursive:true});fs.writeFileSync(target,bytes);};
 const commit=()=>{git(['add','.']);git(['commit','-m','Explicit synthetic F5 source equivalence unit']);return git(['rev-parse','HEAD']);};
 try{
  git(['init','--quiet']);
  for(const file of sourceInventory.required)write(file,'SOURCE_UNIT_ONLY\n');
  write('.gitignore','.codex-tmp/\n');
  const authorities=['supabase/repeatable/source.sql',f5Verifier,'scripts/healthroster-f5-post-verified-gate.mjs','broker/src/weekly-source/index.js',
   'broker/src/weekly-source/off-rule-native-proxy.mjs'];
  for(const file of authorities)write(file,'SOURCE_UNIT_ONLY\n');
  const manifest={releaseId:'synthetic-source-only-release',verificationFiles:[f5Verifier],newVerificationFiles:[f5Verifier]};
  write(sourceInventory.manifest,JSON.stringify(manifest));const installed=commit();
  const bundlePath=sourceInventory.receiptBundles.weeklySourceProtectedTestSetup;
  const bundle=JSON.stringify({schema_version:'WEEKLY_SOURCE_PROTECTED_TEST_SETUP_INPUT_BUNDLE_V1',authorizations:[],controls:[]});
  write(bundlePath,bundle);write(sourceInventory.manifest,JSON.stringify({...manifest,weeklySourceProtectedTestSetup:{path:bundlePath,sha256:sha256(bundle)}}));
  const registrationCommit=commit(),proof=proveCommittedSources(temporary,installed,registrationCommit);
  const execution={release_id:manifest.releaseId,source_commit:installed,contract_sha256:contract,installation_id:id(90),environment:'TEST',
   database:'cloudtms_test_clone',managed_manifest_sha256:'9'.repeat(64),installed_catalogue_jsonb_sha256:'8'.repeat(64)};
  const artifacts=[{...buildOffRuleTestAuthorization({environment:'TEST',expectedDatabase:'cloudtms_test_clone',mode:'UPGRADE',releaseId:manifest.releaseId,
   gitCommit:installed,expectedHash:contract},{...(awaitlessSetupInput()),authorization_id:id(91)},
   {...controls,source_commit:installed})}];
  function awaitlessSetupInput(){return {authorization_id:id(91),scope_id:id(92),agency_id:id(4),source_group_id:id(2),source_cycle_id:id(3),
   setup_actor_user_id:id(1),journey_actor_user_id:id(1),purpose:'SOURCE_FIXTURE_SETUP',week_ending:'2026-11-01',expires_at_utc:'2026-10-16T12:00:00Z',
   fixture_namespace:'WSQ_TEST_',fixture_manifest:observed.fixture_manifest,assignment_id:id(7),assignment_version:'1',assignment_sha256:'a'.repeat(64),
   rule_set_version_id:id(8),rule_values_sha256:'b'.repeat(64),original_source_cas:null,
   allowed_operations:['weekly_source_upload_stage_begin_atomic_v1','weekly_source_upload_stage_rows_atomic_v1','weekly_source_upload_seal_atomic_v1','weekly_source_invoice_hold_release_atomic_v1'].sort(),
   banking_install_receipt_sha256:'c'.repeat(64),security_receipt_sha256:'d'.repeat(64),source_effects_review_sha256:'e'.repeat(64)};}
  const body={schema_version:'WEEKLY_SOURCE_POST_VERIFIED_EVIDENCE_RECEIPT_V1',execution,receipt_input_commit:registrationCommit,
   receipt_tree_sha256:proof.source_equivalence_sha256,artifact_slot:'weekly_source_protected_test_setup_authorizations',
   artifacts_sha256:domainHash('WEEKLY_SOURCE_POST_VERIFIED_ARTIFACT_SET_V1',artifacts),prior_evidence_sha256:'7'.repeat(64),evidence_sha256:'6'.repeat(64),
   actor_user_id:id(1),recorded_at_utc:'2026-10-16T11:00:00Z',replayed:false,operational_settings_changed:false};
  const record={receipt:{...body,receipt_sha256:domainHash(body.schema_version,body)},artifacts};
  const actual=assertF5AuthorityCompatibility(temporary,installed,registrationCommit,[record],execution);
  assert.deepEqual(actual.registration,record);assert.match(actual.sha256,/^[0-9a-f]{64}$/);
  assert.throws(()=>assertF5AuthorityCompatibility(temporary,installed,registrationCommit,[],execution));
  assert.throws(()=>assertF5AuthorityCompatibility(temporary,installed,registrationCommit,[{...record,receipt:{...record.receipt,receipt_tree_sha256:'f'.repeat(64)}}],execution));
  for(const file of [...authorities,'supabase/release/current-contract.json','.github/workflows/database-release.yml',sourceInventory.manifest]){
   const original=fs.readFileSync(path.join(temporary,file));write(file,Buffer.concat([original,Buffer.from('\nALTERED_AUTHORITY')]));const changed=commit();
   assert.throws(()=>assertF5AuthorityCompatibility(temporary,installed,changed,[record],execution),undefined,file);
   write(file,original);commit();
  }
 }finally{
  const real=fs.realpathSync(temporary),realBase=fs.realpathSync(base);
  if(path.dirname(real)!==realBase||!path.basename(real).startsWith('f5-equivalence-unit-'))throw Error('SOURCE_TEST_CLEANUP_SCOPE_INVALID');
  fs.rmSync(real,{recursive:true,force:true});
 }
});
