// Exact immutable source selection only. Never writes another worktree.
import fs from 'node:fs';
import path from 'node:path';
import {spawnSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import {fileURLToPath,pathToFileURL} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url));
const repo=path.resolve(here,'../../../..');
const root=process.argv[2];if(!root)throw Error('READ_ONLY_ROOT_REQUIRED');
const {scanManagedSql}=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-release-sql.mjs')));
const {routineDefinitions}=await import(pathToFileURL(path.join(root,'scripts/bpay-next-final-definers.mjs')));
const sha=s=>createHash('sha256').update(s).digest('hex');
const commits={c86:'c86a1dfcf0098a26b6f0eac085c95c9b08f81074',sourceCertificate:'330a4770a208f0c6a927fd3a7ebee366dbc96340',rotation:'ffc317246e54fd2c943d65f7af653c4537dae80f',nativeIngress:'150ac08f4a9ae4196a6f6319f1478a693f70fd39',dailyIngress:'f0172356c678695a68db80cd066ed69e80648a29',pending:'b45eae27977007991ba449c1345890c7a4ee5f85'};
const base='docs/healthroster-build/proposals/';
const dirs={c86:base+'v6-invoice-native-20261016/',nativeIngress:base+'v6-expense-family-ingress-20261016/',dailyIngress:base+'v6-expense-daily-ingress-20261016/',pending:base+'v6-claim-native-pending-20261016/',rotation:base+'v6-claim-native-rotation-20261016/'};
function object(commit,file){const r=spawnSync('git',['-c','maintenance.auto=false','-c','gc.auto=0','show',commit+':'+file],{cwd:repo,maxBuffer:30_000_000});if(r.status!==0)throw Error(r.stderr.toString());return r.stdout;}
function describe(commit,file,kind){const bytes=object(commit,file),text=bytes.toString('utf8').replaceAll('\r\n','\n');
 const disk=fs.readFileSync(path.join(repo,file));
 const statements=scanManagedSql(text).statements.map(s=>text.slice(s.start,s.end).trim());
 const functions=[],effects=[];
 for(const statement of statements){const signatures=routineDefinitions(statement,file);if(signatures.length)for(const signature of signatures)functions.push({signature,statementSha256:sha(statement)});else effects.push({statementSha256:sha(statement),statement});}
 return {sourceCommit:commit,path:file,rawSha256:sha(bytes),canonicalLfSha256:sha(text),currentSavedDiskRawSha256:sha(disk),currentSavedDiskEqualsSelectedGitObject:sha(disk)===sha(bytes),kind,functions,nonFunctionEffects:effects};}
const recipePath=dirs.c86+'INTEGRATION_RECIPE_c86a1dfc.json';
const recipe=JSON.parse(fs.readFileSync(path.join(repo,recipePath),'utf8'));
const baseFiles=recipe.files.map(r=>describe(commits.c86,dirs.c86+r.file,r.kind));
for(let i=0;i<baseFiles.length;i++)if(baseFiles[i].canonicalLfSha256!==recipe.files[i].sha256)throw Error('C86_RECIPE_MISMATCH '+baseFiles[i].path);
if(baseFiles.length!==53)throw Error('C86_BASE_COUNT');
const selected=baseFiles.map(r=>{
 const leaf=path.basename(r.path);
 if(['015_source_certificate.sqlinc','035_preflight.sqlinc'].includes(leaf))return {...describe(commits.sourceCertificate,r.path,r.kind),supersedes:r};
 if(['025_claim_placement.sqlinc','026_claim_current.sqlinc','027_claim_owned.sqlinc','028_claim_lock.sqlinc','030_claim_sync.sqlinc'].includes(leaf))return {...describe(commits.pending,dirs.pending+'forward/'+leaf,r.kind),supersedes:r};
 return r;
});
// New pending helper must exist before the current/owned/lock/sync consumers.
selected.splice(selected.findIndex(r=>path.basename(r.path)==='025_claim_placement.sqlinc'),0,describe(commits.pending,dirs.pending+'forward/024_claim_pending.sqlinc','NEW_OWNER_ONLY_HELPER_WITH_ACL'));
const ingress=JSON.parse(fs.readFileSync(path.join(repo,dirs.nativeIngress+'MOUNT_RECIPE.json'),'utf8'));
const family=describe(commits.dailyIngress,dirs.dailyIngress+'forward/001_family_entry.sqlinc','NEW_LOCKING_ONLY_HELPERS_WITH_ACL');
selected.unshift(family);
const nativeOwners=ingress.native_replacements.map(r=>({...describe(commits.nativeIngress,dirs.nativeIngress+r.forward,'COMPLETE_EXISTING_NATIVE_INGRESS_OWNER'),nativePreimage:{path:dirs.nativeIngress+r.preimage,canonicalLfSha256:r.preimage_sha256_lf}}));
// Existing final source targets are replaced by signature; never mount the
// stale generic eight-argument workflow template accompanying ingress work.
selected.push(...nativeOwners);
const seen=new Map();
for(const file of selected){if(!file.currentSavedDiskEqualsSelectedGitObject)throw Error('FINAL_DISK_GIT_MISMATCH '+file.path);for(const fn of file.functions){if(seen.has(fn.signature))throw Error('DUPLICATE_FINAL_SIGNATURE '+fn.signature);seen.set(fn.signature,file.path);}}
const rotation=[describe(commits.rotation,dirs.rotation+'forward/025_claim_placement.sqlinc','SUPERSEDED_LINEAGE_ONLY'),describe(commits.rotation,dirs.rotation+'forward/028_claim_lock.sqlinc','SUPERSEDED_LINEAGE_ONLY')];
const vatNativeParent=describe(commits.c86,'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/23072026_2207_private_invoice_generation_vat_policy_batch.sql','RETAIN_NATIVE_PARENT_DROP_AND_ACL_NOT_A_SECOND_DEFINITION');
const out={version:'HEALTH_INVOICE_FINAL_EXACT_MOUNT_SELECTION_V1',qualification:'SOURCE_ONLY_NATIVE_NOT_RUN',
 sourceCommits:commits,baseRecipe:{path:recipePath,sha256:sha(fs.readFileSync(path.join(repo,recipePath))),completeBaseFileCount:53},
 finalFileCount:selected.length,finalSignatureCount:seen.size,installAsOneAtomicComposition:true,
 physicalOwner:{source:'975376fd97855248c76b0ccbe027eed9b50f25ff',finalCandidateMap:'f5668bf45854ca36b47e713c0e8f3dd3ae4b852b',rule:'Use the complete final Physical-selected eight-argument workflow owner; never overwrite it with c86/150ac generic template. Retain exact f017 family-entry kernel. No new Bank ABI.'},
 precedence:['c86 complete 53-file base dependency inventory','330a only complete015 and035','ffc025/028 retained as predecessor lineage only','b45 new024 and complete025/026/027/028/030 are final','f017 complete001 two locking helpers supersedes150ac001','150ac twelve complete native ingress owners; preserve native preimages and financial suffixes','Physical975/f566 owns complete generic WF and current/pending/rotation/native publication composition'],
 oneTimeSchema:{path:dirs.c86+'forward/001_billing_schema.sqlinc',rule:'Six billing stores belong once in canonical Root01 after physical schema. Do not recreate if Root01 already contains exact schema. Retain every table/check/index/trigger/RLS/owner/revoke statement; no inference from function list.'},
 aclRule:'Execute complete retained099 after all selected new helpers; b45 new024 and f017001 include their own exact owner/revoke ACL. Preserve each existing native owner ACL. Root final security and PostgREST reload remain controlling.',
 nonFunctionRule:'Every selected file records complete non-function statements and hashes. 001 schema is one-time; other non-function ACL/trigger effects require their existing migration/repeatable phase, never omit them while copying only functions. 033c is CREATE FUNCTION only: replace the complete native parent CREATE definition and retain that parent DROP FUNCTION and existing ACL, rather than blindly appending CREATE onto an existing function. Root decides canonical generator allocation and idempotent trigger mounting.',
 retainedNativeParentNonFunctionDependencies:[vatNativeParent],
 baseDependencies:baseFiles,selected,rotationPredecessorOnly:rotation,
 mandatoryPhysicalDependencies:recipe.physical_dependencies,
 additionalPendingDependencies:['private.timesheet_contract_native_rotation_binding_v1(jsonb,boolean)','private.timesheet_contract_native_allocation_binding_v1(jsonb,boolean)','private.timesheet_contract_native_pending_for_v1(uuid)','private.timesheet_contract_native_pending_week_v1(uuid)','private.timesheet_contract_native_pending_workflow_v1(jsonb,uuid)','Physical975 nullable current-placement schema, exact audited withdrawal/real allocation publishers, H103/accessors and deferred commit guard'],
 physicalNonFunctionEffects:['Canonical16102026_0159 central physical+Invoice001 schema with exact owner-only FORCE RLS/ACL','Complete00f physical00..04a internal DDL/trigger order and05ACL; retain physical/TF guards','Daily04b/04c/04d before c86 dependent functions','f566 native-pending one-time ALTER/deferred trigger/index schema body','f566 forward/CANDIDATE_EFFECTS.sqlinc exact summary-cache trigger drop/create','f566 CANDIDATE_FINAL_ACL.sqlinc56signatures+notify; exact physical FROZEN_PHYSICAL_INVOICE_PREREQUISITES.json/MOUNT_RECIPE controlling'],
 noDefinitionOfInvoiceEditContext:'This selection does not define public.weekly_source_invoice_edit_context_v1(jsonb); preserve root c29 selected CURRENT presentation invoice-line singleton refusal in both actual reader leaves.',
 rootIntegrationRequired:true,nativeDatabaseApiConcurrency:'NOT_RUN'};
fs.writeFileSync(path.join(here,'FINAL_MOUNT_SELECTION.json'),JSON.stringify(out,null,2)+'\n');
const compact=[...seen].map(([signature,file])=>`${signature}\t${selected.find(r=>r.path===file).sourceCommit}\t${file}`).join('\n')+'\n';
fs.writeFileSync(path.join(here,'FINAL_SIGNATURES.tsv'),compact);
console.log(JSON.stringify({base:baseFiles.length,selected:selected.length,signatures:seen.size,nonFunctionStatements:selected.reduce((n,r)=>n+r.nonFunctionEffects.length,0),sha256:sha(JSON.stringify(out,null,2)+'\n')}));
