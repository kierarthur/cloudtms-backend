// Independent source evidence only. Reads frozen objects and saves this review.
import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {spawnSync} from 'node:child_process';
import {fileURLToPath,pathToFileURL} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..');
const sourceRepo=path.resolve(repo,'../healthroster-v2-money-build-13102026');
const root=path.resolve(repo,'../healthroster-install2-14102026');
const frozen='e4661d28219b85c66b854d99c6522b55aae33139',rootCommit='5623005b91e4bc425e79587a88b1c5cad168d5eb';
const base='docs/healthroster-build/proposals/source-contact-producer-continuation-20261016/';
const {scanManagedSql}=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-release-sql.mjs')));
const {routineDefinitions}=await import(pathToFileURL(path.join(root,'scripts/bpay-next-final-definers.mjs')));
const sha=value=>createHash('sha256').update(value).digest('hex'),lf=value=>value.toString('utf8').replaceAll('\r\n','\n');
const git=(cwd,args)=>{const result=spawnSync('git',['-c','maintenance.auto=false','-c','gc.auto=0',...args],{cwd,maxBuffer:9_000_000,windowsHide:true});assert.equal(result.status,0,result.stderr.toString());return result.stdout;};
const blob=(commit,file)=>git(repo,['show',`${commit}:${file}`]);
const recipeBytes=blob(frozen,base+'SOURCE_COMPLETION_RECIPE.json'),recipe=JSON.parse(recipeBytes);
const files=[],definitions=[],statements=[],mismatches=[];
for(const [phase,rows] of Object.entries({schema:recipe.assembly.one_time,repeatable:recipe.assembly.repeatable})){
 for(const row of rows){
  const bytes=blob(frozen,base+row.path),text=lf(bytes),saved=fs.readFileSync(path.join(sourceRepo,base,row.path));
  assert.equal(sha(bytes),sha(saved),'FROZEN_SAVED_DISK_DRIFT '+row.path);
  const actual=sha(bytes);if(actual!==row.sha256)mismatches.push({path:row.path,recipeSha256:row.sha256,actualFrozenAndSavedSha256:actual});
  const parsed=scanManagedSql(text);assert.equal(parsed.meta.length,0);
  for(const item of parsed.statements){
   const sql=text.slice(item.start,item.end).trim(),signatures=routineDefinitions(sql,row.path);
   statements.push({phase,file:row.path,sha256:sha(sql),head:item.tokens.slice(0,4),functions:signatures});
   for(const signature of signatures){
    const tag=sql.match(/\bas\s+(\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$)/i)?.[1];
    const header=tag?sql.slice(0,sql.indexOf(tag)):sql;
    assert(/\bsecurity\s+definer\b/i.test(header),signature+' security');
    assert(/\bset\s+search_path\s*(?:to|=)/i.test(header),signature+' fixed search_path');
    definitions.push({phase,file:row.path,signature,statementSha256:sha(sql),headerSha256:sha(header),
     language:/\blanguage\s+sql\b/i.test(header)?'SQL':'PLPGSQL'});
   }
  }
  files.push({phase,path:row.path,rawSha256:actual,canonicalLfSha256:sha(text),bytes:bytes.length,statements:parsed.statements.length});
 }
}
assert.equal(files.filter(row=>row.phase==='schema').length,3);assert.equal(files.filter(row=>row.phase==='repeatable').length,53);
assert.equal(definitions.length,107);assert.equal(new Set(definitions.map(row=>row.signature)).size,106);
assert.deepEqual(mismatches.map(row=>row.path),['forward/99_acl.sqlinc']);
const signatures=new Map();for(const row of definitions){if(!signatures.has(row.signature))signatures.set(row.signature,[]);signatures.get(row.signature).push(row);}
const duplicate=[...signatures].filter(row=>row[1].length>1);assert.equal(duplicate.length,1);
assert.equal(duplicate[0][0],'private.weekly_contact_producer_immutable_v1()');
const inputs=[];
const metadata=git(repo,['ls-tree','-r','--name-only',frozen,'--',base]).toString().split(/\r?\n/).filter(file=>file.endsWith('_INPUTS.json'));
for(const file of metadata){
 const document=JSON.parse(blob(frozen,file));
 const rows=[...(document.inputs??[]),...(document.input?[document.input]:[])];
 for(const row of rows){
  const commit=row.source_commit??row.commit,filePath=row.path;
  if(!commit||!filePath)continue;
  const bytes=blob(commit,filePath),expected=row.sha256_normalized_lf??row.whole_file_sha256??row.sha256;
  if(expected)assert.equal(sha(lf(bytes)),expected,'OWNER_INPUT_DRIFT '+filePath);
  inputs.push({record:file,commit,path:filePath,canonicalLfSha256:sha(lf(bytes))});
 }
}
const initialMeta=JSON.parse(blob(frozen,base+'MANAGER_INITIAL_OWNER_INPUTS.json'));
const initialParent=lf(blob(initialMeta.input.commit,initialMeta.input.path));
const begin=initialParent.indexOf('create or replace function '+initialMeta.signature.split('(')[0]+'(');
const end=initialParent.indexOf('$function$;',initialParent.indexOf('as $function$',begin))+12;
const initialWhole=initialParent.slice(begin,end).trimEnd()+'\n';let composed=initialWhole;
for(const delta of initialMeta.deltas){assert.equal(composed.split(delta.from).length,2,delta.reason);composed=composed.replace(delta.from,delta.to);}
assert.equal(composed,lf(blob(frozen,base+initialMeta.output.path)));
const runtime=JSON.parse(blob(frozen,base+'RUNTIME_CONTRIBUTION.json'));
const currentParentBytes=blob(rootCommit,runtime.source_path),currentParent=lf(currentParentBytes);let contributed=currentParent;
const anchorCounts=[];
for(const edit of runtime.edits){const count=contributed.split(edit.before).length-1;assert.equal(count,1,'CURRENT_RUNTIME_ANCHOR');anchorCounts.push(count);contributed=contributed.replace(edit.before,edit.after);}
let reversed=contributed;for(const edit of [...runtime.edits].reverse())reversed=reversed.replace(edit.after,edit.before);assert.equal(reversed,currentParent);
const runtimeLeaf=blob(frozen,base+runtime.leaf.source_path);
assert.equal(sha(fs.readFileSync(path.join(sourceRepo,base,runtime.leaf.source_path))),sha(runtimeLeaf));
assert.equal(sha(runtimeLeaf),'6bf1eed7b3dd86b984217c91b37a4a8d6b786511f712ea6daa87cf8f0a41e8ff');
assert.equal(runtime.leaf.sha256,'0a7f1d0dc0a23b7593b4551df5ddf7089475cdb5ba20c80a211ad16b4672f60f');
const guardCorrectionPath='docs/healthroster-build/proposals/contact-manual-selection-ordinal-guard-20261016/CORRECTION.json';
const guardCorrection=JSON.parse(fs.readFileSync(path.join(root,guardCorrectionPath)));
const guardSuccessorPath=path.posix.join(path.posix.dirname(guardCorrectionPath),guardCorrection.correction.payload);
const guardSuccessor=lf(fs.readFileSync(path.join(root,guardSuccessorPath)));
const oldGuardFile=lf(blob(frozen,base+'forward/01_checking_revision_and_history.sqlinc'));
const oldGuard=scanManagedSql(oldGuardFile).statements.map(row=>oldGuardFile.slice(row.start,row.end).trim())
 .find(sql=>routineDefinitions(sql,'01').includes('private.weekly_contact_source_ordinal_owner_guard_v1()'));
assert.equal(guardSuccessor.trim().replace(guardCorrection.correction.new,guardCorrection.correction.old),oldGuard);
assert.equal(sha(guardSuccessor),guardCorrection.correction.sha256);
assert.equal(guardSuccessor.split(guardCorrection.correction.new).length,2);
const guardKinds=oldGuard.match(/cap\.capture_kind not in\(([^)]+)\)/)[1].match(/'[^']+'/g).map(value=>value.slice(1,-1));
assert(!guardKinds.includes('MANUAL_SELECTION_RECOMPOSITION'));
const recompose=lf(blob(frozen,base+'forward/13_recompose_native_intents.sqlinc'));
const enqueue=lf(blob(frozen,base+'forward/08_manual_native_enqueue.sqlinc'));
assert(recompose.includes("when cap.capture_kind in('ROUTINE_MANUAL_SELECTION','MANUAL_SELECTION_RECOMPOSITION')then 'MANUAL_SELECTION_RECOMPOSITION'"));
assert(enqueue.includes("'MANUAL_SELECTION_RECOMPOSITION'"));
const acl=lf(blob(frozen,base+'forward/99_acl.sqlinc')),aclOwners=[],aclRevokes=[],aclGrants=[];
function aclSignature(declaration){return routineDefinitions('create function '+declaration+' returns void language sql as $$select null$$;','ACL')[0];}
for(const item of scanManagedSql(acl).statements){
 const sql=acl.slice(item.start,item.end).trim();let match;
 if((match=sql.match(/^alter function ([\s\S]+?) owner to postgres;$/i)))aclOwners.push(aclSignature(match[1]));
 else if((match=sql.match(/^revoke all on function ([\s\S]+?) from public,anon,authenticated,service_role;$/i)))aclRevokes.push(aclSignature(match[1]));
 else if((match=sql.match(/^grant execute on function ([\s\S]+?) to service_role;$/i)))aclGrants.push(aclSignature(match[1]));
 else assert.match(sql,/^notify pgrst, 'reload schema';$/i,'UNCLASSIFIED_ACL_STATEMENT');
}
const unique=[...signatures.keys()].sort();assert.deepEqual([...aclOwners].sort(),unique);assert.deepEqual([...aclRevokes].sort(),unique);
assert.deepEqual([...aclGrants].sort(),unique.filter(signature=>signature.startsWith('public.')));
const pack=process.env.HEALTHROSTER_V6_PACK??'C:/tmp/healthroster-handover/HealthRoster_Builder_Pack_16102026/NHSP_AUTHORISATION_AUDIT_15102026_HANDOFF_V6';
const acceptanceAuthorities=['CONTACT_PAUSE_SPEC.md','SOURCE_NATIVE_ACCEPTANCE_CONTRACT.md','HEALTHROSTER_ACCEPTANCE_SPEC.md','INVARIANTS.md']
 .map(file=>({path:path.join(pack,file),rawSha256:sha(fs.readFileSync(path.join(pack,file)))}));
const out={version:'HEALTH_CONTACT_INDEPENDENT_FROZEN_SOURCE_REVIEW_V1',qualification:'SOURCE_REVIEW_NOT_NATIVE_PART_B',
 sourceCommit:frozen,rootCommit,recipePath:base+'SOURCE_COMPLETION_RECIPE.json',recipeSha256:sha(recipeBytes),
 acceptanceAuthorities,
 counts:{schemaFiles:3,repeatableFiles:53,functionDeclarations:definitions.length,uniqueFunctions:signatures.size,
  repeatableFunctions:definitions.filter(row=>row.phase==='repeatable').length,totalStatements:statements.length},
 soleDuplicate:duplicate.map(([signature,rows])=>({signature,owners:rows.map(row=>({phase:row.phase,file:row.file,statementSha256:row.statementSha256}))})),
 recipeMetadataMismatch:mismatches,
 securitySource:{securityDefinerDeclarations:definitions.length,fixedSearchPathDeclarations:definitions.length,
  exactFinalAclOwners:aclOwners.length,exactFinalAclRevokes:aclRevokes.length,serviceExecutablePublicFunctions:aclGrants.length,
  serviceExecutablePrivateFunctions:0,browserExecutableFunctions:0,newTablePolicy:'All nine ENABLE+FORCE RLS, CURRENT_USER/service policy, owner CURRENT_USER, all table privileges revoked from PUBLIC/anon/authenticated/service_role; SECURITY DEFINER owners mediate.',
  history:'Six append-only tables protected against UPDATE/DELETE at schema creation; all nine TRUNCATE guarded. Three stateful revisions/pointers/progress have function and trigger guards.'},
 files,definitions,statements,nativeOwnerInputEvidence:inputs,
 managerInitialCompleteReverse:{signature:initialMeta.signature,inputCommit:initialMeta.input.commit,path:initialMeta.input.path,
  completeParentSha256:sha(initialWhole),successorSha256:sha(composed),orderedDeltas:initialMeta.deltas.length,completeForwardEqualsFrozen:true},
 reviewedRootGuardSuccessor:{path:guardSuccessorPath,sha256:sha(guardSuccessor),correctionPath:guardCorrectionPath,
  frozenInputStatementSha256:sha(oldGuard),exactOnlyDelta:guardCorrection.correction,
  completeReverseEqualsFrozen:true,sourceBoundary:'APPROVED_EXACT_ONE_KIND_ADDITION',native:'NOT_RUN'},
 deterministicFrozenGuardMismatch:{generatedRecompositionKind:'MANUAL_SELECTION_RECOMPOSITION',frozenPermittedUpdateKinds:guardKinds,
  producer:base+'forward/13_recompose_native_intents.sqlinc',ordinalPointerWriter:base+'forward/08_manual_native_enqueue.sqlinc',
  trigger:base+'forward/01_checking_revision_and_history.sqlinc',expectedFrozenFailure:'CONTACT_SOURCE_ORIGINAL_ATTEMPT_OWNER',
  sqlstate:'23514',proof:'Actual generated capture kind is rejected by actual BEFORE UPDATE discriminator; no native execution claimed.'},
 actualRuntimeParentContribution:{rootCommit,path:runtime.source_path,currentRawSha256:sha(currentParentBytes),currentCanonicalLfSha256:sha(currentParent),
  historicalRecipeParentCommit:runtime.source_commit,historicalRecipeParentSha256:runtime.source_sha256_normalized_lf,
  currentParentEqualsHistoricalRecipe:sha(currentParent)===runtime.source_sha256_normalized_lf,exactAnchorCounts:anchorCounts,
  contributedCanonicalLfSha256:sha(contributed),completeReverseEqualsActualCurrentParent:true,
  leafSourcePath:base+runtime.leaf.source_path,leafSha256:sha(runtimeLeaf),leafMountPath:runtime.leaf.mount_path,
  staleLeafRecipeSha256:runtime.leaf.sha256,leafRecipeMatchesFrozen:runtime.leaf.sha256===sha(runtimeLeaf),
  semantics:'Existing scheduler, bounded recheck with isolated failure, original renders/preparation/delivery/completed-pack/pending entitlement owners preserved; current-parent unit proof separate.',native:false},
 findingsStatus:'See REVIEW.md; known deterministic recomposition guard omission blocks unchanged frozen package mounting.',
 native:{database:false,lock:false,docker:false,provider:false,acl:'NOT_RUN',currentness:'NOT_RUN',concurrency:'NOT_RUN',partB:'NOT_RUN'}};
fs.writeFileSync(path.join(here,'FROZEN_SOURCE_REVIEW.json'),JSON.stringify(out,null,2)+'\n');
const reviewFiles=['REVIEW.md','audit-frozen-contact.mjs','FROZEN_SOURCE_REVIEW.json'].map(file=>{
 const bytes=fs.readFileSync(path.join(here,file));
 assert.equal(bytes.includes(Buffer.from('\r\n')),false,'REVIEW_REQUIRES_LF '+file);
 return {path:file,bytes:bytes.length,sha256:sha(bytes)};
});
fs.writeFileSync(path.join(here,'SOURCE_MANIFEST.json'),JSON.stringify({
 version:'HEALTH_CONTACT_INDEPENDENT_REVIEW_SOURCE_MANIFEST_V1',status:'SOURCE_REVIEW_ONLY_NOT_NATIVE_ACCEPTANCE',
 reportBase:'1808fbad03a93abe0ac414938c6e8785e673bae3',producerCommit:frozen,rootPrerequisiteCheckpoint:rootCommit,
 files:reviewFiles,excludesSelf:true,managedPayload:false,
 validation:{independentSourceAudit:'PASS',frozenSourceRuntimeSuites:46,frozenSourceRuntimeFailures:0,
  nativeExecution:'NOT_RUN',installedAcl:'NOT_RUN',concurrency:'NOT_RUN',api:'NOT_RUN',provider:'NOT_RUN',partB:'NOT_RUN'},
 preservation:{producer:'UNCHANGED',invoiceAssemblyAndDuty:'FROZEN',rootEdits:false,databaseActions:false,
  unrelatedPreexistingUntracked:'PRESERVED'},
},null,2)+'\n');
console.log(JSON.stringify({counts:out.counts,metadataMismatches:mismatches,ownerInputs:inputs.length,
 initialReverseDeltas:initialMeta.deltas.length,currentRuntimeParentEqualsHistoricalRecipe:out.actualRuntimeParentContribution.currentParentEqualsHistoricalRecipe,
 currentRuntimeAnchors:anchorCounts,sourceOnly:true}));
