import test from 'node:test';
import assert from 'node:assert/strict';
import {canonicalContractHash} from '../scripts/cloudtms-db-release-lib.mjs';
import {validateInstall2Predecessor,assertFreshInstall2NativeGate} from '../scripts/healthroster-install2-native-gate.mjs';
import {runFullRehearsalWithinOwnedTurn,INSTALL2_REHEARSAL_VERSION,identityProbeSql} from '../scripts/healthroster-install1-full-rehearsal.mjs';

// These are deterministic source-only gate models. No native observation or
// installed catalogue result is minted by this test.
const now=Date.parse('2026-10-14T21:00:00Z'),at=new Date(now-1000).toISOString();
const bank='a'.repeat(40),installed='b'.repeat(40),remote='c'.repeat(40),candidate='d'.repeat(40);
const candidateHash='e'.repeat(64),bindingHash='f'.repeat(64);
const contract={contract_version:5,enums:[],schemas:[],policies:[],routines:[],triggers:[],relations:[],extensions:[],default_acls:[]};
const nativeHash=canonicalContractHash(contract);
const migration={path:'supabase/migrations/26052026_1525_INDEX_OUTBOX.SQL',sha256:'1'.repeat(64)};
const repeatable={path:'supabase/repeatable/14102026_0044_weekly_source_workspace.sql',sha256:'2'.repeat(64)};
const inventory={migrations:[migration],repeatables:[repeatable]};
function model(){
 const latestRelease={status:'VERIFIED',commit:installed,releaseId:'model-installed-release',completedAt:at,
  repositoryContract:nativeHash,installedContract:nativeHash};
 const predecessorObservation={checkedAt:at,connectedFresh:true,physicalDatabase:'cloudtms_test_clone',identityTest:true,
  customerKeyNull:true,module:'LEGACY:1',applyingCount:0,latestRelease,contract,installed:inventory};
 const path='docs/healthroster-build/PUBLISHED_HELPERS.md',afterContentSha256='3'.repeat(64),beforeContentSha256='4'.repeat(64);
 const sourceRelationship={status:'FRESH_GIT_RELATIONSHIP_ONLY_NATIVE_PREDECESSOR_UNVERIFIED',checkedAt:at,repository:'kierarthur/cloudtms-backend',
  pushedCandidateCommit:candidate,observedRemoteTestCommit:remote,completedBankCommitSupplied:bank,installedSourceCommitSupplied:installed,
  fullGitClean:true,gitStatusEnvelope:[],ancestry:[[bank,installed],[installed,remote],[remote,candidate]].map(([ancestor,descendant])=>({ancestor,descendant,proved:true})),
  installedSourceToRemoteChangedPaths:[path],installedSourceToRemoteBlobs:[{path,beforeContentSha256,afterContentSha256}]};
 return {checkedAt:at,environment:'TEST',physicalDatabase:'cloudtms_test_clone',identityTest:true,customerKeyNull:true,module:'LEGACY:1',
  candidatePin:candidate,pushedCandidatePin:candidate,remoteTestPin:remote,cleanGit:true,sourceStaged:true,reviewedSqlRuntimeMatch:true,
  expectedContractHash:candidateHash,sourceBindingSha256:bindingHash,
  bank:{status:'COMPLETE',protectedConclusion:'success',sourcePin:bank,sharedUnlockConfirmed:true,recordHash:'5'.repeat(64)},
  latestRelease,predecessorObservation,reviewedInstalledSource:{commit:installed,contract,inventory},sourceRelationship,
  reviewedInstalledToRemotePaths:[{path,beforeContentSha256,afterContentSha256,decision:'INSTALL1_PUBLICATION_RECEIPT_ONLY'}],
  protectedRunsChecked:true,protectedActiveCount:0,applyingCount:0,phoneOrAcceptanceActive:false,
  route:'APPROVED_MACHINE_LOCAL_FALLBACK_PROCESS_ENV',borrowedLabCapability:false,secretsInArgvOrFiles:false,ownerNativeCallbackAuthorised:true};
}
const options={candidatePin:candidate,currentRemoteTestPin:remote,expectedContractHash:candidateHash,now};
const predecessor=g=>({observation:g.predecessorObservation,reviewedSource:g.reviewedInstalledSource,now});

test('second install distinguishes Banking, installed source, receipt-only test and candidate without relabelling the release',()=>{
 const g=model();assert.equal(assertFreshInstall2NativeGate(g,options),g);
 const p=validateInstall2Predecessor(predecessor(g));
 assert.equal(p.installedCommit,installed);assert.equal(p.installedContractHash,nativeHash);
 assert.notEqual(p.installedContractHash,candidateHash);assert.notEqual(g.latestRelease.commit,g.remoteTestPin);
});
test('failed, applying, incomplete or unrelated installed releases cannot qualify the predecessor',()=>{
 for(const delta of [{status:'FAILED'},{status:'APPLYING'},{commit:remote},{releaseId:''},{completedAt:null},{completedAt:new Date(now+1000).toISOString()}]){
  const g=model();g.predecessorObservation.latestRelease={...g.latestRelease,...delta};
  assert.throws(()=>validateInstall2Predecessor(predecessor(g)),/VERIFIED_RELEASE_REQUIRED/);
 }
});
test('native catalogue must match its own installed Git contract and both recorded contract hashes',()=>{
 for(const mutate of [g=>{g.predecessorObservation.contract={...contract,routines:[{name:'unexpected'}]};},
  g=>{g.predecessorObservation.latestRelease={...g.latestRelease,installedContract:candidateHash};},
  g=>{g.predecessorObservation.latestRelease={...g.latestRelease,repositoryContract:candidateHash};},
  g=>{g.predecessorObservation.contract={...contract,contract_version:6};},
  g=>{g.reviewedInstalledSource={...g.reviewedInstalledSource,contract:{}};}]){
  const g=model();mutate(g);assert.throws(()=>validateInstall2Predecessor(predecessor(g)),/CONTRACT_/);
 }
});
test('both entire ledgers reject missing, unknown, duplicate or changed entries while preserving uppercase SQL identity',()=>{
 for(const mutate of [v=>{v.migrations=[];},v=>{v.repeatables=[];},v=>{v.migrations.push({...migration});},
  v=>{v.repeatables[0].sha256='6'.repeat(64);},v=>{v.repeatables.push({path:'supabase/repeatable/14102026_unknown.sql',sha256:'7'.repeat(64)});},
  v=>{v.migrations[0].path='supabase/migrations/26052026_1525_index_outbox.sql';}]){
  const g=model();g.predecessorObservation.installed=structuredClone(inventory);mutate(g.predecessorObservation.installed);
  assert.throws(()=>validateInstall2Predecessor(predecessor(g)),/LEDGER_/);
 }
});
test('target, fresh connection and identity refusals remain mandatory for native predecessor observations',()=>{
 for(const delta of [{checkedAt:new Date(now-900000).toISOString()},{checkedAt:new Date(now+1).toISOString()},
  {connectedFresh:false},{physicalDatabase:'uofvkfi5'},{identityTest:false},{customerKeyNull:false},{module:'NEXT:2'},{applyingCount:1}]){
  const g=model();g.predecessorObservation={...g.predecessorObservation,...delta};
  assert.throws(()=>validateInstall2Predecessor(predecessor(g)),/REQUIRED|REFUSED/);
 }
});
test('receipt-gap dispositions must cover exactly the actual Git paths and raw before/after content hashes',()=>{
 for(const mutate of [g=>{g.reviewedInstalledToRemotePaths=[];},g=>{g.reviewedInstalledToRemotePaths[0].afterContentSha256='8'.repeat(64);},
  g=>{g.reviewedInstalledToRemotePaths[0].beforeContentSha256=null;},g=>{g.reviewedInstalledToRemotePaths[0].decision='GENERIC_DOCS_OK';},
  g=>{g.reviewedInstalledToRemotePaths.push({...g.reviewedInstalledToRemotePaths[0]});},
  g=>{g.sourceRelationship.installedSourceToRemoteBlobs=[];},
  g=>{const path='broker/src/index.js';g.sourceRelationship.installedSourceToRemoteChangedPaths=[path];g.sourceRelationship.installedSourceToRemoteBlobs[0].path=path;g.reviewedInstalledToRemotePaths[0].path=path;}]){
  const g=structuredClone(model());mutate(g);assert.throws(()=>assertFreshInstall2NativeGate(g,options),/RECEIPT_GAP/);
 }
});
test('retained drafts remain visible and prevent a clean release claim',()=>{
 const g=model();g.sourceRelationship.fullGitClean=false;g.sourceRelationship.gitStatusEnvelope=['?? retained-source-draft.txt'];
 assert.throws(()=>assertFreshInstall2NativeGate(g,options),/SOURCE_RELATIONSHIP/);
});
test('source relationship cannot swap predecessor, target, bank pin or any actual ancestry edge',()=>{
 for(const mutate of [g=>{g.sourceRelationship.installedSourceCommitSupplied=remote;},g=>{g.sourceRelationship.observedRemoteTestCommit=installed;},
  g=>{g.sourceRelationship.completedBankCommitSupplied=installed;},g=>{g.sourceRelationship.ancestry.pop();},
  g=>{g.sourceRelationship.ancestry[1].proved=false;},g=>{g.sourceRelationship.ancestry[0].ancestor=remote;},
  g=>{g.sourceRelationship.checkedAt=new Date(now-900000).toISOString();}]){
  const g=structuredClone(model());mutate(g);assert.throws(()=>assertFreshInstall2NativeGate(g,options),/RELATIONSHIP|RELEASE|ANCESTRY/);
 }
});
test('the common safety gates still hold runtime/Office before any publication callback',()=>{
 for(const delta of [{environment:'LIVE'},{physicalDatabase:'uofvkfi5'},{module:'NEXT:2'},{cleanGit:false},{pushedCandidatePin:remote},
  {sourceStaged:false},{reviewedSqlRuntimeMatch:false},{expectedContractHash:nativeHash},{sourceBindingSha256:'bad'},
  {protectedRunsChecked:false},{protectedActiveCount:1},{applyingCount:1},{phoneOrAcceptanceActive:true},
  {route:'ARTHUR_LAB'},{borrowedLabCapability:true},{secretsInArgvOrFiles:true},{ownerNativeCallbackAuthorised:false}]){
  assert.throws(()=>assertFreshInstall2NativeGate({...model(),...delta},options),/REFUSED/);
 }
});
test('the second marker can be probed truthfully and cannot be run through the first-install entry',async()=>{
 const marker='hr_install2_'+'9'.repeat(32);
 assert.ok(identityProbeSql(marker).includes(marker));
 await assert.rejects(runFullRehearsalWithinOwnedTurn({installNumber:1,marker}),/INSTALL_IDENTITY_REFUSED/);
 await assert.rejects(runFullRehearsalWithinOwnedTurn({installNumber:3,marker}),/INSTALL_IDENTITY_REFUSED/);
});
test('second-install callback source flow preserves identity and gates across rollback and publication',async()=>{
 const events=[],frames=[],marker='hr_install2_'+'9'.repeat(32),controller=new AbortController();
 const identity={physicalDatabase:'cloudtms_test_clone',databaseOid:123,backendPid:456,turnId:'hr_turn_'+'0'.repeat(32),key:[20261013,1]};
 const turn={identity,signal:controller.signal,assertHeld:async phase=>events.push(phase)};
 const g=model();const actualNow=Date.now();g.checkedAt=new Date(actualNow).toISOString();g.sourceRelationship.checkedAt=g.checkedAt;g.predecessorObservation.checkedAt=g.checkedAt;
 g.latestRelease.completedAt=new Date(actualNow-60000).toISOString();
 const snapshot={physicalDatabase:'cloudtms_test_clone',identityTest:true,customerKeyNull:true,module:'LEGACY:1',applyingCount:0,otherActivityCount:0,
  markerAbsent:true,ledgerFingerprint:bindingHash,catalogueFingerprint:bindingHash,businessCountsFingerprint:bindingHash,businessRowsFingerprint:bindingHash,relationCensus:bindingHash,installed:{}};
 const result=await runFullRehearsalWithinOwnedTurn({installNumber:2,turn,candidatePin:candidate,currentRemoteTestPin:remote,expectedContractHash:candidateHash,marker,review:{},
  compilePlan:()=>({version:INSTALL2_REHEARSAL_VERSION,sourceBinding:{sha256:bindingHash},sql:'begin; rollback;',sqlHash:bindingHash,lineMap:[],expectedContractHash:candidateHash,diagnosticCodes:[],currentCounts:{},pendingCounts:{},exclusions:[]}),
  callbacks:{freshGate:async()=>g,before:async()=>snapshot,executeSameSession:async()=>({sameSession:true,explicitRollback:true,exitCode:0,errorCollectionComplete:true,errors:[],installedContract:{matched:true,sha256:candidateHash}}),
   freshAfter:async()=>({...snapshot,connectedFresh:true,marker}),recordReceipt:async frame=>frames.push(frame),publishManaged:async()=>({status:'UNIT_CALLBACK_ONLY'})}});
 assert.equal(result.receipt.version,INSTALL2_REHEARSAL_VERSION);assert.ok(frames.every(x=>x.version===INSTALL2_REHEARSAL_VERSION));
 assert.ok(events.includes('install2-fresh'));assert.equal(events.at(-1),'install2-after-managed-release');assert.ok(events.every(x=>!x.startsWith('install1-')));
});
