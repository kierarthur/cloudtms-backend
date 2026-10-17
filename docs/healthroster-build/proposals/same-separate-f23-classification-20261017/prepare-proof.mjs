// Independent complete Health owner and catalogue/DDL input proof. Preliminary.
import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';
import {fileURLToPath,pathToFileURL} from 'node:url';import {createHash} from 'node:crypto';import {execFileSync} from 'node:child_process';
import {originalReviews} from './prior-review.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),root='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026',money='D:/Development/GitHub/.codex-worktrees/healthroster-v2-money-build-13102026';
for(const p of [here,root,money,process.cwd()])assert.match(fs.realpathSync.native(p).replaceAll('\\','/'),/^D:\/Development\/GitHub(?:\/|$)/);
const canon=v=>v.toString('utf8').replaceAll('\r\n','\n'),sha=v=>createHash('sha256').update(v).digest('hex');
const read=file=>canon(fs.readFileSync(path.join(root,file))),write=(file,value)=>{const target=path.join(here,file);assert.ok(target.startsWith(here+path.sep));fs.mkdirSync(path.dirname(target),{recursive:true});fs.writeFileSync(target,value);};
const git=(repo,args)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',repo,...args],{windowsHide:true,maxBuffer:6*1024*1024});
const frozenInputs=[],frozen=(commit,file)=>{const bytes=git(money,['show',commit+':'+file]);frozenInputs.push({commit,file,bytes:bytes.length,sha256:sha(bytes)});return canon(bytes);};
const finalMode=process.argv.includes('--final'),censusFile=finalMode?'CENSUS.json':'PRELIMINARY_CENSUS.json';
const census=JSON.parse(fs.readFileSync(path.join(here,censusFile),'utf8')),
 packet='docs/healthroster-build/proposals/same-separate-health-current-successor-20261017',commit=census.functional;
const {scanManagedSql}=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-release-sql.mjs')).href),
 {routineDefinitions}=await import(pathToFileURL(path.join(root,'scripts/bpay-next-final-definers.mjs')).href);
const statements=source=>{const p=scanManagedSql(source);return p.statements.map(s=>p.source.slice(s.start,s.end).trim());};
const complete=(source,signature)=>{const choices=statements(source).filter(s=>routineDefinitions(s,'owner').includes(signature));assert.equal(choices.length,1,signature);return choices[0];};
const reverse=(source,hunks)=>{for(const h of [...hunks].reverse()){const count=h.occurrences??1;assert.ok(Number.isSafeInteger(count)&&count>=1&&count<=2);
 assert.equal(source.split(h.after).length,count+1,'EXACT_BOUNDED_LITERAL_REVERSE_ANCHOR');source=source.replaceAll(h.after,()=>h.before);}return source;};
const edits=JSON.parse(frozen(commit,packet+'/CURRENT_OWNER_EDITS.json')),schemaSource='supabase/release/current-contract.json',schema=JSON.parse(read(schemaSource)),relations={},owners=[];
const schemaAST=JSON.parse(fs.readFileSync(path.join(here,'SCHEMA_AST.json'),'utf8'));
const selectedSource=new Map(census.files.map(f=>[f.file,canon(git(money,['show',f.sourceCommit+':'+f.sourcePath]))]));
for(const e of edits.owners){
 if(!e.signature)continue;
 const file=census.files.find(f=>f.file.includes('/17102026_0231_')&&path.posix.basename(f.file)===path.posix.basename(e.targetCompleteDefinition));if(!file)continue;
 const successor=complete(selectedSource.get(file.file),e.signature);assert.equal(sha(successor),e.outputSha256,e.signature);
 if(!e.current){owners.push({file:file.file,signature:e.signature,state:'NEW_COMPLETE_DEFINITION',successorStatementSha256:sha(successor)});continue;}
 const previous=complete(read(e.current.physical),e.signature),fromGit=complete(frozen(e.current.commit,e.current.path),e.signature);
 assert.equal(previous,fromGit,'ACTUAL_SAVED_COMMITTED_PARENT '+e.signature);
 // Some current complete owners retain genuine mixed CRLF insertion bytes.
 // Prove the raw whole-owner reverse, then the gate's canonical LF image.
 const originalRaw=git(money,['show',commit+':'+packet+'/current/'+path.posix.basename(e.targetCompleteDefinition)]).toString('utf8').trim(),
  actualRaw=fs.readFileSync(path.join(root,e.current.physical),'utf8');
 assert.equal(sha(originalRaw),e.inputSha256,'RAW_CURRENT_OWNER_HASH '+e.signature);
 assert.equal(actualRaw.split(originalRaw).length,2,'ACTUAL_COMPLETE_RAW_OWNER '+e.signature);
 const reversedRaw=reverse(successor+'\n',e.hunks).trimEnd();assert.equal(reversedRaw,originalRaw,'RAW_WHOLE_CURRENT_OWNER_REVERSE '+e.signature);
 assert.equal(canon(reversedRaw),previous,'CANONICAL_CURRENT_OWNER_EQUAL '+e.signature);
 const priorFile='inputs/owners/'+String(owners.filter(r=>r.priorFile).length+1).padStart(2,'0')+'.sqlinc';write(priorFile,reversedRaw+'\n');
 owners.push({file:file.file,signature:e.signature,state:'COMPLETE_CURRENT_PARENT_REVERSE',current:e.current,priorFile,
  priorStatementSha256:sha(previous),priorRawStatementSha256:sha(reversedRaw),successorStatementSha256:sha(successor),hunks:e.hunks});
}
assert.equal(owners.length,61);assert.equal(owners.filter(r=>r.priorFile).length,27);
if(finalMode){
 const counter='docs/healthroster-build/proposals/same-separate-counter-forward-20261017',counterEdits=JSON.parse(frozen(census.finalCommit,counter+'/COUNTER_EDITS.json'));
 for(const e of counterEdits.files){
  if(!e.target.startsWith('supabase/')&&!e.target.startsWith('native/'))continue;
  const file=census.files.find(f=>f.rawSha256===e.outputSha256);assert.ok(file,'EXACT_COUNTER_SELECTED_OUTPUT '+e.target);
  const rawAfter=git(money,['show',file.sourceCommit+':'+file.sourcePath]).toString('utf8'),rawBefore=git(money,['show',census.finalCommit+':'+counter+'/current/'+(e.input.definitionOnly?path.posix.basename(e.target):e.target)]).toString('utf8');
  assert.equal(sha(rawAfter),e.outputSha256);assert.equal(sha(rawBefore),e.input.sha256);assert.equal(reverse(rawAfter,e.hunks),rawBefore,'COUNTER_WHOLE_RAW_REVERSE '+e.target);
  const signatures=routineDefinitions(canon(rawAfter),'counter');assert.equal(signatures.length,1);const signature=signatures[0];
  const previous=complete(canon(rawBefore),signature),successor=complete(canon(rawAfter),signature);
  let currentFile,rootCurrentFile,parentOwnerFile;
  if(e.input.definitionOnly){
   const actual=fs.readFileSync(path.join(root,e.input.path),'utf8'),committed=frozen(e.input.commit,e.input.path);
   assert.equal(complete(canon(actual),signature),previous);assert.equal(complete(committed,signature),previous);
   assert.equal(actual.split(rawBefore.trimEnd()).length,2,'ACTUAL_RAW_REVIEW_OWNER');currentFile=e.input.path;rootCurrentFile=e.input.path;
  }else{
   assert.equal(rawBefore,git(money,['show',e.input.commit+':'+e.input.path]).toString('utf8'),'RAW_VIRTUAL_PARENT_EQUALITY');
   const parentOwner=owners.find(o=>o.signature===signature&&o.file===e.target);assert.ok(parentOwner);
   parentOwnerFile=parentOwner.file;rootCurrentFile=parentOwner.rootCurrentFile??parentOwner.current?.physical??null;
  }
  const priorFile='inputs/owners/'+String(owners.filter(r=>r.priorFile).length+1).padStart(2,'0')+'.sqlinc';write(priorFile,rawBefore);
  owners.push({file:file.file,signature,state:'COMPLETE_COUNTER_PARENT_REVERSE',currentFile,rootCurrentFile,parentOwnerFile,
   input:e.input,priorFile,priorStatementSha256:sha(previous),priorRawStatementSha256:sha(rawBefore.trimEnd()),successorStatementSha256:sha(successor),hunks:e.hunks});
 }
 assert.equal(owners.length,71);assert.equal(new Set(owners.map(o=>o.signature)).size,62);assert.equal(owners.filter(r=>r.priorFile).length,37);
}
for(const file of census.files)for(const point of file.points){if(!point.hazard.startsWith('ROWTYPE_SELECT:'))continue;
 const name=point.hazard.split(':')[2].replaceAll(' ','').toLowerCase();if(relations[name])continue;
 const actual=schema.relations.find(r=>r.schema+'.'+r.name===name);
 if(actual)relations[name]={kind:actual.kind,columns:actual.columns};
 else{const ddlProof=schemaAST.currentDDLRelations.find(r=>r.relation===name);assert.ok(ddlProof,'ACTUAL_CATALOGUE_OR_DDL_RELATION '+name);
  assert.equal(sha(fs.readFileSync(path.join(root,ddlProof.path))),ddlProof.rawSha256);
  const matching=statements(read(ddlProof.path)).filter(s=>s.startsWith('create table '+name+' '));assert.equal(matching.length,1);
  relations[name]={kind:'r',columns:ddlProof.columns,ddlPath:ddlProof.path,ddl:matching[0],rawSha256:ddlProof.rawSha256};}
}
// Actual scalar UUID operands also require their native relation columns.
for(const name of ['public.weekly_source_invoice_presentation_lines','private.bpay_next_source_current_scopes']){
 const actual=schema.relations.find(r=>r.schema+'.'+r.name===name);assert.ok(actual,'ACTUAL_SCALAR_RELATION '+name);
 relations[name]={kind:actual.kind,columns:actual.columns};
}
const original=await originalReviews(root),transfers=[],unproved=[];
for(const file of census.files)for(const point of file.points){if(point.hazard.startsWith('ROWTYPE_SELECT:')){assert.ok(original.tupleProof(point));continue;}
 const signature=JSON.parse(point.key)[0],owner=owners.find(o=>o.file===file.file&&o.signature===signature);
 const rootCurrentFile=owner?.rootCurrentFile??owner?.current?.physical;
 if(!rootCurrentFile){unproved.push({file:file.file,...point});continue;}
 const previous=read(rootCurrentFile),matching=original.gate.specialConstructViolations(previous,rootCurrentFile).filter(p=>p.key===point.key).length,
 classified=original.classifyPrior(previous,rootCurrentFile,original.baseline).get(point.key)??0;
 if(matching>=point.count&&classified>=point.count)transfers.push({file:file.file,signature,key:point.key,count:point.count,currentFile:rootCurrentFile,originalOccurrences:matching,originalAllowance:classified});
 else unproved.push({file:file.file,...point,matchingPriorOccurrences:matching,priorAllowance:classified});
}
const classifier=JSON.parse(frozen(commit,packet+'/CLASSIFIER_EDITS.json'));
assert.equal(reverse(selectedSource.get(classifier.target),classifier.hunks),read(classifier.inputTarget),'ENTIRE_CURRENT_CLASSIFIER_REVERSE');
const value={version:finalMode?'SAME_SEPARATE_FINAL_SOURCE_PROOF_INPUTS_V1':'SAME_SEPARATE_PRELIMINARY_SOURCE_PROOF_INPUTS_V1',
 status:finalMode?'FINAL_A7E452_SOURCE_INPUTS_NOT_NATIVE':'AWAIT_CORRECTED_MONEY_FREEZE',censusSha256:sha(fs.readFileSync(path.join(here,censusFile))),
 schema:{path:schemaSource,sha256:sha(fs.readFileSync(path.join(root,schemaSource))),relations,addedColumns:schemaAST.migration.addedColumns},owners,transfers,unproved,classifier,
 frozenInputs,baselineEvidence:original.baselineEvidence,native:'NOT_RUN',catalogueDependentBodies:'FOUR_NOT_PROVED',bankingBodiesRead:0};
write(finalMode?'PROOF_INPUTS.json':'PRELIMINARY_PROOF_INPUTS.json',JSON.stringify(value,null,2)+'\n');
console.log(JSON.stringify({owners:owners.length,fullReverses:owners.filter(r=>r.priorFile).length,relations:Object.keys(relations).length,transfers:transfers.length,
 unproved:unproved.map(p=>({file:p.file,hazard:p.hazard,count:p.count,signature:JSON.parse(p.key)[0],matchingPrior:p.matchingPriorOccurrences,priorAllowance:p.priorAllowance})),native:'NOT_RUN'}));
