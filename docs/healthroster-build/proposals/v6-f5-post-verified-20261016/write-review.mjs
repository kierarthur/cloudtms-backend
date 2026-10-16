// Records source closure only. No database, release or runtime action.
import fs from 'node:fs';
import path from 'node:path';
import {createHash} from 'node:crypto';
import {fileURLToPath} from 'node:url';
import {f5NativeSetupDiscoverySql} from './forward/healthroster-f5-native-setup.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),sha=b=>createHash('sha256').update(b).digest('hex');
const input=JSON.parse(fs.readFileSync(path.join(here,'SOURCE_INPUTS.json'))),inputs=new Map(input.inputs.map(i=>[i.path,i.sha256]));
const write=(file,data)=>fs.writeFileSync(path.join(here,file),typeof data==='string'?data:JSON.stringify(data,null,2)+'\n');
const id=n=>`f5520000-0000-4000-8000-${String(n).padStart(12,'0')}`;
write('fixtures/discovery-parser-only.sql','-- OFFLINE GRAMMAR ONLY; synthetic IDs, NOT a native observation. DO NOT EXECUTE.\n'+f5NativeSetupDiscoverySql({actorUserId:id(1),sourceGroupId:id(2),sourceCycleId:id(3)})+'\n');
const base='supabase/repeatable/14102026_0430_healthroster_money_native_interfaces/';
const replace=[['forward/06_OFF_RULE_COMMAND.sqlinc',base+'06_OFF_RULE_COMMAND.sqlinc'],
 ['forward/21_SETUP_STATIC_SHAPE_ASSERT.sqlinc',base+'21_SETUP_STATIC_SHAPE_ASSERT.sqlinc'],
 ['forward/off-rule-native-proxy.mjs','broker/src/weekly-source/off-rule-native-proxy.mjs'],
 ['fixtures/f5-post-verified.sql','supabase/verification/13102026_1440_weekly_source_invoice_money_staging_hold_verification.sql'],
 ['forward/weekly-source-post-verified-evidence-controller.mjs','scripts/weekly-source-post-verified-evidence-controller.mjs']];
const add=['healthroster-f5-native-setup.mjs','healthroster-f5-post-verified-gate.mjs','healthroster-f5-prepublication.mjs','healthroster-f5-synthetic-fixture.mjs']
 .map(n=>['forward/'+n,'scripts/'+n]);add.push(['forward/f5-restoration.sqlinc','scripts/healthroster-f5-restoration.sql']);
write('MOUNT_RECIPE.json',{schema_version:'F5_POST_VERIFIED_SOURCE_MOUNT_V1',source_pin:input.integrator_commit,native:'NOT_RUN',root_only_integration:true,
 replacements:replace.map(([payload,target])=>({target,payload,expected_before_sha256:inputs.get(target),after_sha256:sha(fs.readFileSync(path.join(here,payload)))})),
 additions:add.map(([payload,target])=>({target,payload,sha256:sha(fs.readFileSync(path.join(here,payload)))})),
 finite_root_edits:[
 {target:'scripts/cloudtms-db-release.mjs',owner:'verificationFilesForMode/runVerifiers + applyManagedRelease caller',
  recipe:'Require partitionF5VerificationFiles(release,mode) for agency TEST NEW/UPGRADE. Execute only preVerified in preinstall phase; return verificationFiles only actually run, and mandatory_post_verified_verification_files:[original F5 path], phase REQUIRED_POST_VERIFIED. Pass mandatory postVerified callback using actual psql/root/gitCommit/expectedHash/restoration bytes; preserve all other assertions/order.'},
 {target:'scripts/cloudtms-managed-release.mjs',owner:'applyManagedRelease',
  recipe:'Require agency TEST postVerified callback before durable application. After the real VERIFIED commit and before return/writer unlock, await postVerified(); propagate failure and retain truthful VERIFIED (existing catch only updates APPLYING). No env/optional skip. Return actual F5 receipt.'},
 {target:'scripts/cloudtms-auto-test-release.mjs',owner:'authorityUnchanged before chooseTestDatabaseRoute + before DATABASE_READY',
  recipe:'Keep actual pending migration/repeatable/contract/identity guards. Replace raw authority diff admission with assertF5AuthorityCompatibility. Exact authority equality uses original paths plus proxy. Receipt-only normalization must read actual current audit/execution via readF5VerifiedState and use existing ALL_TRACKED proveCommittedSources + assertPostVerifiedReceipt; failure leaves authorityUnchanged=false. Before DATABASE_READY unconditionally await runF5PostVerifiedGate on FULL_UPGRADE, approved component and NO_DATABASE_CHANGE. Save returned f5Gate and databaseRelease{release_id,source_commit:database_source_commit,contract_sha256}. Preserve original artifact upload path/name.'},
 {target:'scripts/request-test-release.mjs',owner:"after stage('database',{status:'VERIFIED_OR_EXACT_UNCHANGED',...}) and before first targets for-loop",
  recipe:'Unconditionally await loadF5PrepublicationReceipt({root,command,api,gh,run,expectedCommit:sha,expectedContractSha256:canonicalContractHash(current-contract.json)}). Save actual returned receipt/stage before any Worker or Office publication. No dependency/env skip. Existing interruption/guard/publication ordering stays intact.'},
 {target:'root full-rehearsal constructor and source/current176 census',owner:'root release harness',
  recipe:'F5 path remains registered exactly once in both existing mandatory verifier lists and current census. Preinstall rollback cannot fabricate genuine VERIFIED/catalogue/OFF-rule authority. Record REQUIRED_POST_VERIFIED_NOT_EXECUTABLE_PREINSTALL, mandatory:true, positives_pending:true, provider/runtime:false. This is a required execution phase boundary, not a passed or optional skipped check. Preserve all 16 original safety exclusions and every original F5 positive/negative; actual post-VERIFIED/prepublication pass plus restoration is mandatory.'}],
 setup_owner:{function:'prepareF5NativeSetup({session,actorUserId,sourceGroupId,sourceCycleId,buildAuthorization,turnBackendPid})',
  registration:'existing committed setup bundle + recordPostVerifiedEvidence slot weekly_source_protected_test_setup_authorizations',
  past_assignment:'existing private.weekly_source_historical_fixture_rule_assign_v1(uuid,jsonb), actual separate native turn lock + future reference + full row hash/native audit',
  inputs_required:['genuine reserved TEST Client/ROSTER group/past open cycle','actual overlapping fixture Contracts/Candidates','real current native OFF-rule controls','real protected owner and separate turn PID','actual measured/committed/registered authorization'],
  observed_native_fixture_ids:false},database_calls:false,features_remain_off:true,query_checkpoint_unchanged:'2775cfd745c79197ddc9b36d1aac3df8b18a9ec3'});
const assertions=[
 ['future fixture date','temporary config date derives from actual native clock; fresh Source XLSX is generated for real past open window'],
 ['exact fixture vacancy','same original f551 vacancy'],['saved assignment remains OFF','same actual native rule-save/assignment positive'],
 ['actual latest release VERIFIED','actual post-VERIFIED release + completed/catalogue owner; never fabricated'],
 ['actual stage begin','same native stage-begin through exact registered OFF owner'],['actual money evidence stage','same native stage-rows'],
 ['actual immutable seal','same native seal'],['native stored literal money and real timestamp','same 10000/2002/12002; new parsed source finalisation inside actual cycle window'],
 ['WEEKLY_SOURCE_SETTINGS_ADMIN_REQUIRED','same exact negative'],['WEEKLY_SOURCE_INVOICE_HOLD_SCOPE_STALE','same exact negative'],
 ['native bounded default hold release','same 10000/2000/12000 BILL_AT_NET_PLUS_20'],['exact hold replay','same native replay'],
 ['WEEKLY_SOURCE_INVOICE_HOLD_RELEASE_REPLAY_CONFLICT','same conflicting replay refusal'],
 ['WEEKLY_SOURCE_IMMUTABLE_RECORD','both original UPDATE and DELETE refusals'],['two exact source evidence cells; no fee','same M2/N2 cells; fee unmapped'],
 ['no fabricated financial producer','same original assertion + actual authorized Client/cycle no final/billing publication'],
 ['exact original TS/TF/workflow/approval/component/invoice/document equality','additional actual protected row fingerprint before/after'],
 ['no new native Timesheet','additional exact scoped identity equality'],['features and ordinary effective rules remain OFF after native command','additional OFF/close/restoration']];
write('F5_ASSERTION_MAP.json',{schema_version:'F5_MANDATORY_ASSERTION_PHASE_MAP_V1',original_verifier:replace[3][1],original_sha256:inputs.get(replace[3][1]),
 original_positive_and_negative_assertions_retained:true,mandatory_phase:'POST_VERIFIED_PREPUBLICATION',preinstall_status:'REQUIRED_POST_VERIFIED_NOT_EXECUTABLE_PREINSTALL',
 native_status:'NOT_RUN',assertions:assertions.map(([original,mapping])=>({original,mapping,mandatory:true})),synthetic_source_file:'fresh actual XLSX/parser/adapter output; old Nov1 fixture unchanged historical',
 genuine_acceptance:false,provider_or_runtime_acceptance:false});
write('QUALIFICATION.json',{schema_version:'F5_SOURCE_QUALIFICATION_V1',source_tests:{cases:30,passed:30},offline_parser:'pglast v7.20 PostgreSQL17',
 native:'NOT_RUN',database_calls:false,actual_fixture_observations:false,root_integration:'NOT_MOUNTED',release:'NOT_RUN',
 current_inputs_required:true,features_operationally_enabled:false,schedules_changed:false,query_checkpoint_preserved:true});
const walk=dir=>fs.readdirSync(dir,{withFileTypes:true}).flatMap(e=>e.isDirectory()?walk(path.join(dir,e.name)):[path.join(dir,e.name)]);
const files=walk(here).filter(f=>!['SOURCE_MANIFEST.json','SOURCE_RELEASE_RECEIPT.json'].includes(path.basename(f)))
 .map(f=>({path:path.relative(here,f).replaceAll('\\','/'),bytes:fs.statSync(f).size,sha256:sha(fs.readFileSync(f))})).sort((a,b)=>a.path.localeCompare(b.path));
write('SOURCE_MANIFEST.json',{schema_version:'F5_POST_VERIFIED_SOURCE_MANIFEST_V1',files,closure_sha256:sha(JSON.stringify(files)),native:'NOT_RUN',database_calls:false});
console.log(JSON.stringify({files:files.length,closure_sha256:sha(JSON.stringify(files))}));
