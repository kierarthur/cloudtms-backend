// Importing opens no database, reads no credential and performs no publication.
// The root coordinator supplies fresh native observations under its owned turn.
import {canonicalContractHash} from './cloudtms-db-release-lib.mjs';
import {digest} from './cloudtms-managed-release-sql.mjs';

const sha=/^[a-f0-9]{40}$/;
const hash=/^[a-f0-9]{64}$/;
const requireThat=(ok,code)=>{if(!ok)throw Error(code);};
const fresh=(at,now)=>Number.isFinite(Date.parse(at??''))&&now-Date.parse(at)>=0&&now-Date.parse(at)<15*60*1000;
const contractSections=['default_acls','enums','extensions','policies','relations','routines','schemas','triggers'];
const contractHash=contract=>{
 requireThat(contract&&typeof contract==='object'&&!Array.isArray(contract)
  &&JSON.stringify(Object.keys(contract).sort())===JSON.stringify(['contract_version',...contractSections].sort())
  &&Number.isSafeInteger(contract.contract_version)&&contract.contract_version>0
  &&contractSections.every(key=>Array.isArray(contract[key])),'INSTALL2_PREDECESSOR_CONTRACT_ENVELOPE_REFUSED');
 return canonicalContractHash(contract);
};

function ledgerVector(rows){
 requireThat(Array.isArray(rows),'INSTALL2_PREDECESSOR_LEDGER_ENVELOPE_REFUSED');
 const seen=new Set();
 return rows.map(row=>{
  requireThat(typeof row.path==='string'&&/^supabase\/(?:migrations|repeatable)\/[A-Za-z0-9_./-]+\.(?:sql|SQL)$/.test(row.path)
   &&!row.path.split('/').includes('..')&&!row.path.includes('//')
   &&hash.test(row.sha256??'')&&!seen.has(row.path),'INSTALL2_PREDECESSOR_LEDGER_ENTRY_REFUSED');
  seen.add(row.path);return {path:row.path,sha256:row.sha256};
 }).sort((a,b)=>a.path.localeCompare(b.path));
}

export function validateInstall2Predecessor({observation,reviewedSource,now=Date.now()}){
 requireThat(fresh(observation?.checkedAt,now)&&observation.connectedFresh===true,'INSTALL2_PREDECESSOR_FRESH_PROBE_REQUIRED');
 requireThat(observation.physicalDatabase==='cloudtms_test_clone'&&observation.identityTest===true
  &&observation.customerKeyNull===true&&observation.module==='LEGACY:1'&&observation.applyingCount===0,
  'INSTALL2_PREDECESSOR_TARGET_REFUSED');
 const release=observation.latestRelease;
 requireThat(sha.test(reviewedSource?.commit??'')&&release?.status==='VERIFIED'&&release.commit===reviewedSource.commit
  &&typeof release.releaseId==='string'&&release.releaseId.length>0
  &&Number.isFinite(Date.parse(release.completedAt??''))&&Date.parse(release.completedAt)<=Date.parse(observation.checkedAt),
  'INSTALL2_PREDECESSOR_VERIFIED_RELEASE_REQUIRED');
 const expected=contractHash(reviewedSource.contract);
 requireThat(hash.test(expected)&&release.repositoryContract===expected&&release.installedContract===expected
  &&contractHash(observation.contract)===expected,'INSTALL2_PREDECESSOR_CONTRACT_DRIFT');
 for(const kind of ['migrations','repeatables']){
  const native=ledgerVector(observation.installed?.[kind]);
  const reviewed=ledgerVector(reviewedSource.inventory?.[kind]);
  requireThat(reviewed.length>0&&JSON.stringify(native)===JSON.stringify(reviewed),'INSTALL2_PREDECESSOR_LEDGER_DRIFT');
 }
 return {status:'FRESH_INSTALL2_PREDECESSOR_VALIDATED',checkedAt:observation.checkedAt,
  installedCommit:release.commit,releaseId:release.releaseId,installedContractHash:expected,
  observedCatalogueHash:contractHash(observation.contract),
  inventoryHash:digest(JSON.stringify({migrations:ledgerVector(observation.installed.migrations),repeatables:ledgerVector(observation.installed.repeatables)})),
  physicalDatabase:observation.physicalDatabase,module:observation.module,
  boundary:'Fresh native predecessor proof only. This is not the pending candidate contract or a rehearsal/install result.'};
}

export function assertFreshInstall2NativeGate(gate,{candidatePin,currentRemoteTestPin,expectedContractHash,now=Date.now()}={}){
 requireThat(sha.test(candidatePin??'')&&sha.test(currentRemoteTestPin??'')&&hash.test(expectedContractHash??''),
  'INSTALL2_GATE_ARGUMENTS_REFUSED');
 requireThat(fresh(gate?.checkedAt,now),'INSTALL2_GATE_STALE');
 requireThat(gate.environment==='TEST'&&gate.physicalDatabase==='cloudtms_test_clone'&&gate.identityTest===true
  &&gate.customerKeyNull===true&&gate.module==='LEGACY:1','INSTALL2_TEST_GATE_REFUSED');
 requireThat(gate.candidatePin===candidatePin&&gate.pushedCandidatePin===candidatePin&&gate.remoteTestPin===currentRemoteTestPin
  &&gate.cleanGit===true&&gate.sourceStaged===true&&gate.reviewedSqlRuntimeMatch===true,'INSTALL2_SOURCE_GATE_REFUSED');
 requireThat(gate.expectedContractHash===expectedContractHash&&hash.test(gate.sourceBindingSha256??''),'INSTALL2_CANDIDATE_CONTRACT_GATE_REFUSED');
 requireThat(gate.bank?.status==='COMPLETE'&&gate.bank.protectedConclusion==='success'&&sha.test(gate.bank.sourcePin??'')
  &&gate.bank.sharedUnlockConfirmed===true&&hash.test(gate.bank.recordHash??''),'INSTALL2_BANK_GATE_REFUSED');
 const relationship=gate.sourceRelationship;
 requireThat(fresh(relationship?.checkedAt,now)&&relationship.repository==='kierarthur/cloudtms-backend'
  &&relationship.pushedCandidateCommit===candidatePin&&relationship.observedRemoteTestCommit===currentRemoteTestPin
  &&relationship.completedBankCommitSupplied===gate.bank.sourcePin&&relationship.fullGitClean===true
  &&Array.isArray(relationship.gitStatusEnvelope)&&relationship.gitStatusEnvelope.length===0,
  'INSTALL2_SOURCE_RELATIONSHIP_REFUSED');
 const predecessor=validateInstall2Predecessor({observation:gate.predecessorObservation,reviewedSource:gate.reviewedInstalledSource,now});
 requireThat(relationship.installedSourceCommitSupplied===predecessor.installedCommit
  &&gate.latestRelease?.status==='VERIFIED'&&gate.latestRelease.commit===predecessor.installedCommit
  &&gate.latestRelease.releaseId===predecessor.releaseId,'INSTALL2_LATEST_RELEASE_GATE_REFUSED');
 const links=[[gate.bank.sourcePin,predecessor.installedCommit],[predecessor.installedCommit,currentRemoteTestPin],[currentRemoteTestPin,candidatePin]];
 requireThat(Array.isArray(relationship.ancestry)&&relationship.ancestry.length===links.length
  &&links.every(([ancestor,descendant],i)=>relationship.ancestry[i]?.ancestor===ancestor
   &&relationship.ancestry[i]?.descendant===descendant&&relationship.ancestry[i]?.proved===true),
  'INSTALL2_ANCESTRY_GATE_REFUSED');
 // The reviewed receipt-only gap is separate from the pending I2 source delta.
 // Every path gets an exact source disposition; no generic docs-path waiver.
 const changed=relationship.installedSourceToRemoteChangedPaths;
 const blobs=relationship.installedSourceToRemoteBlobs;
 const reviewed=gate.reviewedInstalledToRemotePaths;
 requireThat(Array.isArray(changed)&&Array.isArray(blobs)&&Array.isArray(reviewed)&&new Set(changed).size===changed.length
  &&new Set(blobs.map(row=>row.path)).size===blobs.length&&blobs.length===changed.length
  &&new Set(reviewed.map(row=>row.path)).size===reviewed.length&&changed.length===reviewed.length
  &&changed.every(file=>reviewed.some(row=>row.path===file&&row.decision==='INSTALL1_PUBLICATION_RECEIPT_ONLY'
   &&hash.test(row.afterContentSha256??'')
   &&blobs.some(blob=>blob.path===file&&blob.afterContentSha256===row.afterContentSha256
    &&blob.beforeContentSha256===row.beforeContentSha256&&(blob.beforeContentSha256===null||hash.test(blob.beforeContentSha256)))
   &&/^(?:docs\/healthroster-build\/(?:PUBLISHED_HELPERS\.md|TEST_INSTALLS\.md|install1-completed-20261014\/[A-Za-z0-9_.-]+))$/.test(file))),
  'INSTALL2_RECEIPT_GAP_REVIEW_REQUIRED');
 requireThat(gate.protectedRunsChecked===true&&gate.protectedActiveCount===0&&gate.applyingCount===0
  &&gate.phoneOrAcceptanceActive===false,'INSTALL2_ACTIVITY_GATE_REFUSED');
 requireThat(gate.route==='APPROVED_MACHINE_LOCAL_FALLBACK_PROCESS_ENV'&&gate.borrowedLabCapability===false
  &&gate.secretsInArgvOrFiles===false&&gate.ownerNativeCallbackAuthorised===true,'INSTALL2_ROUTE_GATE_REFUSED');
 return gate;
}
