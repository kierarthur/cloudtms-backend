"""Author-only additive source renderer. Public saved/Git inputs; no execution."""
from pathlib import Path
import json,hashlib,subprocess
D=Path(__file__).resolve().parent
ROOT=Path('D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026')
def sha(b):return hashlib.sha256(b).hexdigest()
def git(*args):return subprocess.check_output(['git','-c','gc.auto=0','-c','maintenance.auto=false','-C',str(ROOT),*args],stderr=subprocess.DEVNULL)
head=git('rev-parse','HEAD').decode().strip()
rows=[]
inputs=['scripts/healthroster-install1-native-coordinator.mjs','scripts/healthroster-install1-archive-preflight.mjs',
 'scripts/healthroster-install1-full-rehearsal.mjs','scripts/healthroster-install2-native-gate.mjs',
 'scripts/healthroster-install2-installed-source.mjs','scripts/healthroster-install2-source-relationship.mjs',
 'scripts/healthroster-rehearsal-turn.mjs','scripts/healthroster-turn-control.mjs',
 'scripts/healthroster-live-test-restoration.mjs','scripts/healthroster-receipt-journal.mjs',
 'scripts/healthroster-rehearsal-resource-guard.mjs','scripts/healthroster-rehearsal-stream-observer.mjs',
 'scripts/healthroster-f5-post-verified-gate.mjs','scripts/healthroster-f5-prepublication.mjs',
 'scripts/request-test-release.mjs','scripts/automatic-test-release-policy.mjs',
 'scripts/healthroster-install1-verifier-census.mjs','scripts/healthroster-source-check-environment.mjs',
 'scripts/cloudtms-db-release-lib.mjs','scripts/cloudtms-managed-release-sql.mjs',
 'supabase/release/current-release.json','supabase/release/current-contract.json','supabase/release/export_contract.sql',
 'docs/healthroster-build/install1-rehearsal/VERIFIER_SAFETY_REVIEW.json',
 'docs/AUTOMATIC_TEST_DEPLOYMENT.md','docs/DATABASE_RELEASE_BIBLE.md','docs/healthroster-build/TEST_INSTALL_PLAN.md',
 'docs/healthroster-build/rehearsals/13102026/prepare_full_upgrade_rehearsal.mjs',
 'docs/healthroster-build/rehearsals/13102026/run_full_upgrade_rehearsal.mjs',
 '.github/workflows/database-release.yml','AGENTS.md']
for file in inputs:
 b=(ROOT/file).read_bytes();assert b==git('show',':'+file)==git('show',head+':'+file)
 p=D/'parents'/file;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(b)
 rows.append(dict(file=file,root=ROOT.as_posix(),commit=head,bytes=len(b),sha256=sha(b),captured='parents/'+file))
assert git('rev-parse','HEAD').decode().strip()==head
(D/'PARENTS.json').write_text(json.dumps(dict(version='INSTALL2_ADDITIVE_NATIVE_ENTRY_PARENTS_V1',rows=rows),indent=2)+'\n')
old=(ROOT/inputs[0]).read_bytes().decode();s=old
def change(a,b):
 global s
 assert s.count(a)==1,repr(a)
 s=s.replace(a,b)
change('// Owner-authorised Install1 only.','// Owner-authorised Install2 only; additive entry, original Install1 remains unchanged.')
change("import {canonical,digest}","import {canonical,digest,scanManagedSql}")
change("import assert from 'node:assert/strict';", "import assert from 'node:assert/strict';\nimport {canonicalContractHash,inventory} from './cloudtms-db-release-lib.mjs';\nimport {assertReview} from './healthroster-install1-full-rehearsal.mjs';\nimport {assertFreshInstall2NativeGate} from './healthroster-install2-native-gate.mjs';\nimport {collectInstall2ReviewedInstalledSource} from './healthroster-install2-installed-source.mjs';\nimport {collectInstall2SourceRelationship} from './healthroster-install2-source-relationship.mjs';")
change("const predecessor='07e66fb2b2742fbe634a41582f2ad3e004eed3b9';\nconst contract='7e3c00b70366c2c036cfea419adf32c77df5b65070771591e75168b68f3336d6';", "const completedBankSource='07e66fb2b2742fbe634a41582f2ad3e004eed3b9';\nconst configFile='docs/healthroster-build/install2-rehearsal/NATIVE_COORDINATOR_CONFIG.json';")
change("const out=path.join(workspace,'cloudtms-backend/codex_outputs/claude-stage2-continuation-20260925/HEALTHROSTER_BUILD_13102026');", "const out=path.join(root,'.codex-tmp/healthroster-install2-receipts');")
change("const progressPath=path.join(workspace,'.codex-worktrees/healthroster-v2-backend-13102026/docs/healthroster-build/PROGRESS.md');", "const progressPath=path.join(root,'docs/healthroster-build/PROGRESS.md');")
change("text.includes(predecessor)","text.includes(completedBankSource)")
change("run.head_sha===predecessor","run.head_sha===completedBankSource")
change("sourcePin:predecessor,sharedUnlockConfirmed","sourcePin:completedBankSource,sharedUnlockConfirmed")
excluded=[r['file'] for r in json.loads((ROOT/inputs[23]).read_bytes())['verifiers'] if r['decision']=='EXCLUDE'];assert len(excluded)==16
helpers=r'''
export const INSTALL2_CONFIG_VERSION='HEALTHROSTER_INSTALL2_NATIVE_COORDINATOR_CONFIG_V1';
export const originalSafetyExclusions=Object.freeze(__EXCLUSIONS__);
export function validateInstall2Configuration(c){
 const keys=['version','installedSourceCommit','expectedRemoteTestPin','expectedContractHash','reviewedInstalledToRemotePaths','officePath'];
 requireThat(c&&typeof c==='object'&&!Array.isArray(c)&&JSON.stringify(Object.keys(c).sort())===JSON.stringify(keys.sort())
  &&c.version===INSTALL2_CONFIG_VERSION&&[c.installedSourceCommit,c.expectedRemoteTestPin].every(x=>/^[a-f0-9]{40}$/.test(x??''))
  &&/^[a-f0-9]{64}$/.test(c.expectedContractHash??''),'INSTALL2_CLOSED_CONFIGURATION_REQUIRED');
 requireThat(typeof c.officePath==='string'&&/^D:\/Development\/GitHub\/\.codex-worktrees\/[a-z0-9-]+$/.test(c.officePath),
  'INSTALL2_REVIEWED_D_OFFICE_PATH_REQUIRED');
 requireThat(Array.isArray(c.reviewedInstalledToRemotePaths)&&c.reviewedInstalledToRemotePaths.length<=100
  &&new Set(c.reviewedInstalledToRemotePaths.map(r=>r?.path)).size===c.reviewedInstalledToRemotePaths.length
  &&c.reviewedInstalledToRemotePaths.every(r=>r&&JSON.stringify(Object.keys(r).sort())===JSON.stringify(['path','beforeContentSha256','afterContentSha256','decision'].sort())
   &&/^(?:docs\/healthroster-build\/(?:PUBLISHED_HELPERS\.md|TEST_INSTALLS\.md|install1-completed-20261014\/[A-Za-z0-9_.-]+))$/.test(r.path)
   &&r.decision==='INSTALL1_PUBLICATION_RECEIPT_ONLY'&&(r.beforeContentSha256===null||/^[a-f0-9]{64}$/.test(r.beforeContentSha256??''))
   &&/^[a-f0-9]{64}$/.test(r.afterContentSha256??'')),'INSTALL2_EXACT_RECEIPT_GAP_REVIEW_REQUIRED');
 return c;
}
export function validateInstall2Review(review,release,current){
 assertReview(review,release); // Shared original review schema, not a fabricated Install2 schema.
 requireThat(review.sourceCounts?.migrations===current.migrations.length&&review.sourceCounts?.repeatables===current.repeatables.length
  &&review.sourceCounts?.verifiers===release.verificationFiles.length&&review.verifiers.every(r=>r.pendingIndependentReview!==true),
  'INSTALL2_CURRENT_COMPLETE_REVIEW_REQUIRED');
 const excluded=review.verifiers.filter(r=>r.decision==='EXCLUDE').map(r=>r.file).sort();
 requireThat(JSON.stringify(excluded)===JSON.stringify([...originalSafetyExclusions].sort()),'INSTALL2_ORIGINAL_SIXTEEN_EXCLUSIONS_REQUIRED');
 return review;
}
// One fresh RO/RR child returns identity, exact complete ledgers and the canonical
// exporter result from one snapshot. No caller-supplied SQL or catalogue adoption.
export function install2PredecessorProbeSql(marker){
 requireThat(arguments.length===1,'INSTALL2_FIXED_PREDECESSOR_ARGUMENT_REQUIRED');
 const source=canonical(fs.readFileSync(path.join(root,'supabase/release/export_contract.sql'),'utf8'));
 const parsed=scanManagedSql(source);
 requireThat(parsed.statements.length===2&&parsed.statements[0].tokens.join(' ')==='SET SEARCH_PATH = PG_CATALOG , PUBLIC'
  &&parsed.statements[1].tokens[0]==='WITH','INSTALL2_FIXED_EXPORTER_QUERY_REQUIRED');
 const query=source.slice(parsed.statements[1].start,parsed.statements[1].end).trim().replace(/;\s*$/,'');
 const identity=identityProbeSql(marker).trim().replace(/;\s*$/,'');
 return `set local search_path=pg_catalog,public;
 with native_identity(value) as (${identity}), native_contract(value) as (${query})
 select jsonb_build_object('serverVersionNum',current_setting('server_version_num')::integer,'identity',(select value::jsonb from native_identity),'contract',(select value::jsonb from native_contract),
 'installed',jsonb_build_object('migrations',(select coalesce(jsonb_agg(jsonb_build_object('path',path,'sha256',content_sha256) order by path),'[]'::jsonb) from private.cloudtms_migration_ledger),
 'repeatables',(select coalesce(jsonb_agg(jsonb_build_object('path',path,'sha256',closure_sha256) order by path),'[]'::jsonb) from private.cloudtms_repeatable_ledger)));`;
}
'''.replace('__EXCLUSIONS__',json.dumps(excluded))
change('export function parseOneJsonResult(stdout){',helpers+'\nexport function parseOneJsonResult(stdout){')
change(" requireThat(process.argv[2]==='--run-owner-authorised-install1','EXPLICIT_OWNER_INSTALL1_ENTRY_REQUIRED');", " requireThat(process.argv[2]==='--run-owner-authorised-install1'&&process.argv.length===3,'EXPLICIT_OWNER_INSTALL1_ENTRY_REQUIRED');\n requireThat(fs.realpathSync.native(root).replaceAll('\\\\','/')==='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026','INSTALL2_PHYSICAL_D_ROOT_REQUIRED');\n const config=validateInstall2Configuration(JSON.parse(fs.readFileSync(path.join(root,configFile),'utf8')));\n requireThat(fs.realpathSync.native(config.officePath).replaceAll('\\\\','/')===config.officePath,'INSTALL2_PHYSICAL_D_OFFICE_REQUIRED');\n const predecessor=config.expectedRemoteTestPin,contract=config.expectedContractHash;")
change(" requireThat(digest(JSON.stringify(JSON.parse(fs.readFileSync(path.join(root,'supabase/release/current-contract.json'),'utf8'))))===contract,'INSTALL1_SEALED_CONTRACT_CHANGED');", " requireThat(canonicalContractHash(JSON.parse(fs.readFileSync(path.join(root,'supabase/release/current-contract.json'),'utf8')))===contract,'INSTALL1_SEALED_CONTRACT_CHANGED');")
change(" const sourceBinding=rehearsalSourceBinding({review});", " validateInstall2Review(review,JSON.parse(fs.readFileSync(path.join(root,'supabase/release/current-release.json'),'utf8')),inventory());\n const reviewedInstalledSource=collectInstall2ReviewedInstalledSource({repositoryRoot:root,installedSourceCommit:config.installedSourceCommit});\n const sourceBinding=rehearsalSourceBinding({review});")
change("archiveProof.status==='SOURCE_PREFLIGHT_PASS'", "archiveProof.status==='SOURCE_PREFLIGHT_PASS'&&archiveProof.installNumber===2&&archiveProof.predecessor===predecessor")
change(" const turnJournal='D:/Development/Legacy-Work/C-tmp/healthroster-install1-turn-journal-20261014';", " const turnJournal=path.join(root,'.codex-tmp/healthroster-install2-turn-journal-20261017');")
change("    const freshGate=async()=>{", "    const freshGate=async()=>{") # Anchor check, later bounded block replacements.
change("     const identity=await probe(identityProbeSql(marker));", "     const sourceRelationship=collectInstall2SourceRelationship({repositoryRoot:root,installedSourceCommit:config.installedSourceCommit,completedBankCommit:bank.sourcePin,expectedCandidateCommit:candidatePin,expectedRemoteTestCommit:predecessor,featureBranch:'codex/healthroster-install2-14102026'});\n     const native=await probe(install2PredecessorProbeSql(marker));\n     requireThat(Number.isSafeInteger(native.serverVersionNum)&&native.serverVersionNum>=170000&&native.serverVersionNum<180000,'INSTALL2_PG17_SERVER_REQUIRED');\n     const identity=native.identity;\n     const predecessorObservation={...identity,contract:native.contract,installed:native.installed,connectedFresh:true,checkedAt:new Date().toISOString()};")
change("     return assertFreshNativeGate(observed,{candidatePin,currentRemoteTestPin:predecessor,expectedContractHash:contract});", "     return assertFreshInstall2NativeGate({...observed,predecessorObservation,reviewedInstalledSource,sourceRelationship,reviewedInstalledToRemotePaths:config.reviewedInstalledToRemotePaths},{candidatePin,currentRemoteTestPin:predecessor,expectedContractHash:contract});")
change("    return await runFullRehearsalWithinOwnedTurn({turn,candidatePin", "    return await runFullRehearsalWithinOwnedTurn({installNumber:2,turn,candidatePin")
change("connectionsProof:proofFile,cwd:root", "connectionsProof:proofFile,officePath:config.officePath,cwd:root")
s=s.replace('Install1','Install2').replace('INSTALL1_','INSTALL2_').replace('healthroster-install1-','healthroster-install2-').replace('healthroster_install1_','healthroster_install2_').replace('hr_install1_','hr_install2_').replace('install1-rehearsal','install2-rehearsal').replace('--run-owner-authorised-install1','--run-owner-authorised-install2')
# Shared owners and the historical receipt gap remain their actual names/ABI.
s=s.replace('./healthroster-install2-full-rehearsal.mjs','./healthroster-install1-full-rehearsal.mjs').replace("'INSTALL2_PUBLICATION_RECEIPT_ONLY'","'INSTALL1_PUBLICATION_RECEIPT_ONLY'")
s=s.replace('additive entry, original Install2 remains unchanged','additive entry, original Install1 remains unchanged').replace(',assertFreshNativeGate}', '}')
s=s.replace("const liveDirectory=fs.mkdtempSync('D:/Development/Legacy-Work/C-tmp/healthroster-install2-native-'+stamp+'-');", "fs.mkdirSync(path.join(root,'.codex-tmp'),{recursive:true});\n const liveDirectory=fs.mkdtempSync(path.join(root,'.codex-tmp/healthroster-install2-native-'+stamp+'-'));")
s=s.replace("try{fs.writeFileSync(path.join(out,'INSTALL2_FULL_REHEARSAL_'", "try{fs.mkdirSync(out,{recursive:true});fs.writeFileSync(path.join(out,'INSTALL2_FULL_REHEARSAL_'")
newfile='scripts/healthroster-install2-native-coordinator.mjs'
(D/'payload/scripts').mkdir(parents=True,exist_ok=True);(D/'payload'/newfile).write_bytes(s.encode())
forward=[dict(file=newfile,parentFile=inputs[0],beforeBytes=len(old.encode()),afterBytes=len(s.encode()),beforeSha256=sha(old.encode()),afterSha256=sha(s.encode()),wholeInverse=dict(before=old,after=s))]
# Additive exact-archive source preflight. It neither connects nor installs.
old2=(ROOT/inputs[1]).read_bytes().decode();t=old2
t=t.replace("const base='07e66fb2b2742fbe634a41582f2ad3e004eed3b9';", "const config=validateInstall2Configuration(JSON.parse(fs.readFileSync(path.join(root,'docs/healthroster-build/install2-rehearsal/NATIVE_COORDINATOR_CONFIG.json'),'utf8')));\nconst base=config.expectedRemoteTestPin;")
t=t.replace("import {sourceCheckEnvironment}","import {validateInstall2Configuration} from './healthroster-install2-native-coordinator.mjs';\nimport {sourceCheckEnvironment}")
t=t.replace('healthroster-install1-','healthroster-install2-')
t=t.replace("if(!/^[a-f0-9]{40}$/.test(sha)","if(process.argv.length!==3||process.argv[2]!=='--prepare-owner-authorised-install2')throw Error('EXPLICIT_INSTALL2_ARCHIVE_ENTRY_REQUIRED');\nif(fs.realpathSync.native(root).replaceAll('\\\\','/')!=='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026')throw Error('INSTALL2_PHYSICAL_D_ROOT_REQUIRED');\nif(git(['ls-remote','origin','refs/heads/test']).split(/\\s/)[0]!==base)throw Error('INSTALL2_REMOTE_TEST_CHANGED');\nif(!/^[a-f0-9]{40}$/.test(sha)")
entryGuard="if(process.argv.length!==3||process.argv[2]!=='--prepare-owner-authorised-install2')throw Error('EXPLICIT_INSTALL2_ARCHIVE_ENTRY_REQUIRED');\nif(fs.realpathSync.native(root).replaceAll('\\\\','/')!=='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026')throw Error('INSTALL2_PHYSICAL_D_ROOT_REQUIRED');\n"
assert t.count(entryGuard)==1;t=t.replace(entryGuard,'');t=t.replace("const config=validateInstall2Configuration",entryGuard+"const config=validateInstall2Configuration")
t=t.replace("status:'SOURCE_PREFLIGHT_RUNNING',commit:sha", "status:'SOURCE_PREFLIGHT_RUNNING',installNumber:2,commit:sha")
# Existing shared test names remain real; additionally test the two new entries.
t=t.replace('tests/healthroster-install2-full-rehearsal.test.mjs','tests/healthroster-install1-full-rehearsal.test.mjs')
t=t.replace("'tests/healthroster-install2-native-coordinator.test.mjs'", "'tests/healthroster-install1-native-coordinator.test.mjs','tests/healthroster-install2-native-coordinator.test.mjs','tests/healthroster-install2-native-gate.test.mjs'")
file2='scripts/healthroster-install2-archive-preflight.mjs';(D/'payload'/file2).write_bytes(t.encode())
forward.append(dict(file=file2,parentFile=inputs[1],beforeBytes=len(old2.encode()),afterBytes=len(t.encode()),beforeSha256=sha(old2.encode()),afterSha256=sha(t.encode()),wholeInverse=dict(before=old2,after=t)))
testFile='tests/healthroster-install2-native-coordinator.test.mjs'
testSource=(D/'coordinator.test.source.mjs').read_bytes();(D/'payload/tests').mkdir(parents=True,exist_ok=True)
(D/'payload'/testFile).write_bytes(testSource)
(D/'FULL_PARENT_FORWARD.json').write_text(json.dumps(dict(version='INSTALL2_ADDITIVE_ROOT_ENTRY_WHOLE_PARENTS_V1',targets=forward),indent=2)+'\n')
(D/'SELECTION.json').write_text(json.dumps(dict(version='INSTALL2_ADDITIVE_ENTRY_SELECTION_V1',targets=[dict(file=r['file'],bytes=r['afterBytes'],sha256=r['afterSha256'],mode='ADDITIVE_ONLY_PARENT_ENTRY_UNCHANGED') for r in forward]+[dict(file=testFile,bytes=len(testSource),sha256=sha(testSource),mode='ADDITIVE_TEST_ONLY')]),indent=2)+'\n')
print(json.dumps(dict(status='SOURCE_RENDERED_ONLY',capturedParents=len(rows),sourceOwners=len(forward),targets=len(forward)+1,rootHead=head,native='NOT_RUN')))
