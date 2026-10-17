// Finite Source classifier identity delegation. This grants no SQL/runtime authority.
import fs from 'node:fs';
import path from 'node:path';
import {createHash} from 'node:crypto';
import {repoRoot,sqlDateKey} from './cloudtms-db-release-lib.mjs';
import {scanManagedSql,canonical} from './cloudtms-managed-release-sql.mjs';
import {routineDefinitions} from './bpay-next-final-definers.mjs';

const hash=value=>createHash('sha256').update(value).digest('hex');
const fail=code=>{throw Error('HEALTHROSTER_SOURCE_CLASSIFIER_DELEGATION_'+code);};
const caller='private._candidate_expense_source_family_v1(uuid)';
const physical='supabase/repeatable/16102026_1110_nhsp_v6_source_pending_current/01_complete_pending_current.sqlinc';
const delegates=Object.freeze(['private.timesheet_contract_native_pending_for_v1','private.timesheet_contract_current_placement_v1','private.timesheet_contract_source_original_eligible_v1','private.timesheet_contract_move_accepting_v1']);
export const classificationPath='docs/healthroster-build/v6-owner-delivery-20261016/SOURCE_CLASSIFIER_DELEGATION_20261016/CLASSIFICATION.json';
export const classificationSha256='abe64516babd46100709f45baaa815c7a6c1070f47fd5b9c655e87d09971c14a';
const registryPath='docs/bpay-next/final-definer-closures.json';
const evidenceDirectory=path.posix.dirname(classificationPath)+'/review';
const manifestSha256='252d2c2f3f2d4acfe88bdfecf16f07345c3646e649c1a5856188ecf3295b0c92';
const reviewSha256='5d68b3e1c95547efc2b35b301dcd00c65763b0f836dc03946601bad42f58223a';
const extractionSha256='fe7989d90221195a74c5986a2d96540df665506d58f63b52f8834f66de78b879';
const equal=(a,b)=>JSON.stringify(a)===JSON.stringify(b);
const safe=file=>{
 if(typeof file!=='string'||file.includes('\\')||path.posix.isAbsolute(file)||file.split('/').some(s=>s==='..'||s==='.'||s===''))fail('PATH_INVALID');
 return file;
};
function access(root,overrides={}){
 const read=overrides.readFile??(file=>fs.readFileSync(path.join(root,safe(file))));
 const list=overrides.listFiles??(directory=>fs.readdirSync(path.join(root,directory),{withFileTypes:true}).filter(e=>e.isFile()&&e.name.toLowerCase().endsWith('.sql')).map(e=>directory+'/'+e.name));
 return{read:file=>Buffer.from(read(safe(file))),list};
}
function closure(file,io,stack=[]){
 safe(file);if(!file.startsWith('supabase/')||stack.includes(file))fail('CLOSURE_PATH_INVALID');
 const parsed=scanManagedSql(io.read(file).toString('utf8')),events=[...parsed.statements.map(s=>({...s,kind:'statement'})),...parsed.meta.filter(s=>/^\\ir\s/i.test(s.text)).map(s=>({...s,kind:'include'}))].sort((a,b)=>a.start-b.start),result=[];
 for(const event of events){
  if(event.kind==='statement')result.push({physical:file,sql:parsed.source.slice(event.start,event.end).trim()});
  else{
   const match=event.text.match(/^\\ir\s+(?:'([^']+)'|"([^"]+)"|([^\s;]+))\s*;?\s*$/i);if(!match)fail('CLOSURE_INCLUDE_INVALID');
   const child=path.posix.normalize(path.posix.join(path.posix.dirname(file),match[1]??match[2]??match[3]));
   result.push(...closure(child,io,[...stack,file]));
  }
 }return result;
}
function aclRelevant(sql,names){
 const normal=scanManagedSql(sql.replace(/"([a-z_][a-z0-9_$]*)"/g,'$1')).statements.flatMap(s=>s.tokens).join(' ').toLowerCase().replace(/\s*\.\s*/g,'.');
 if(!/^(?:alter (?:function|procedure|routine|default privileges)|grant\b|revoke\b)/.test(normal))return false;
 return [...names].some(name=>new RegExp('\\b'+name.replaceAll('.','\\.')+'\\s*\\(').test(normal))
  ||/\bon all (?:functions|routines|procedures) in schema\b/.test(normal)
  ||/^alter default privileges\b/.test(normal);
}
// Read-only census of actual executable roots. New unrelated routines remain free
// to evolve; a competing definition or changed relevant ACL for these 13 does not.
export function sourceClassifierEvidenceSnapshot(root,rows,overrides={}){
 const io=access(root,overrides),signatures=new Set(rows.map(r=>r.signature)),names=new Set(rows.map(r=>r.signature.split('(')[0])),definitions=[],acl=[];
 for(const directory of ['supabase/migrations','supabase/repeatable']){
  const roots=io.list(directory).map(safe).sort((a,b)=>sqlDateKey(a).localeCompare(sqlDateKey(b))||a.localeCompare(b));
  if(new Set(roots).size!==roots.length||roots.some(p=>path.posix.dirname(p)!==directory||!p.toLowerCase().endsWith('.sql')))fail('ROOT_CENSUS_INVALID');
  for(const rootPath of roots)for(const item of closure(rootPath,io)){
   for(const signature of routineDefinitions(item.sql,item.physical))if(signatures.has(signature))definitions.push({root:rootPath,physical:item.physical,signature,sha256:hash(item.sql)});
   if(aclRelevant(item.sql,names))acl.push({root:rootPath,physical:item.physical,sha256:hash(item.sql),statement:item.sql});
  }
 }
 return{definitions,acl};
}
function load(root,io){
 const bytes=io.read(classificationPath);if(hash(bytes)!==classificationSha256)fail('CLASSIFICATION_CHANGED');
 const data=JSON.parse(bytes),manifestBytes=io.read(evidenceDirectory+'/SOURCE_MANIFEST.json'),reviewBytes=io.read(evidenceDirectory+'/REVIEW.json'),extractionBytes=io.read(evidenceDirectory+'/EXTRACTION.json');
 if(hash(manifestBytes)!==manifestSha256||hash(reviewBytes)!==reviewSha256||hash(extractionBytes)!==extractionSha256)fail('REVIEW_ARTIFACT_CHANGED');
 const manifest=JSON.parse(manifestBytes),review=JSON.parse(reviewBytes),extraction=JSON.parse(extractionBytes);
 if(data.version!=='SOURCE_CLASSIFIER_FINITE_DELEGATION_CLASSIFICATION_V1'||data.native!=='NOT_RUN'||data.reviewCommit!=='9bb4deecdc3503585148ab0542e92b13ff943f65'
  ||data.sourceCheckpoint!=='fae51b0e0bb33e3e3d62cce47bb6aa96da9b3fbe'||extraction.rows.length!==13||review.checks.passed!==55||review.checks.failed!==0
  ||!equal(extraction.newDirectDelegates,delegates)||extraction.caller!==caller||!equal(data.rows,extraction.rows)||review.native!=='NOT_RUN'||manifest.native!=='NOT_RUN')fail('REVIEW_SCOPE_CHANGED');
 for(const pin of manifest.files){const content=io.read(evidenceDirectory+'/'+pin.path);if(content.length!==pin.bytes||hash(content)!==pin.sha256)fail('REVIEW_EVIDENCE_CHANGED');}
 for(const pin of data.filePins)if(hash(canonical(io.read(pin.path).toString('utf8')))!==pin.sha256)fail('SOURCE_DEPENDENCY_CHANGED');
 const registry=JSON.parse(io.read(registryPath));if(registry.formatVersion!==1||!Array.isArray(registry.chains))fail('REGISTRY_CHANGED');
 for(const row of data.rows){
  const entries=registry.chains.filter(r=>r.signature===row.signature);if(entries.length!==1||!equal(entries[0],row.chain))fail('FINAL_SOURCE_OWNER_CHANGED');
  const parsed=scanManagedSql(io.read(row.physical).toString('utf8')),statements=parsed.statements.filter(s=>routineDefinitions(parsed.source.slice(s.start,s.end),row.physical).includes(row.signature));
  if(statements.length!==1||hash(parsed.source.slice(statements[0].start,statements[0].end).trim())!==row.completeStatementSha256)fail('COMPLETE_DEPENDENCY_CHANGED');
  if(hash(canonical(io.read(evidenceDirectory+'/'+row.bodyPath).toString('utf8')).trim())!==row.completeStatementSha256)fail('COMPLETE_REVIEW_BODY_CHANGED');
 }
 return data;
}
// Preserve the immutable75-event delegation proof. Only the two reasserted
// owner-only Source ACL events may reverse to their exact original provenance.
export function sourceClassifierAclPredecessor(currentAcl,historicalAcl,readFile){
 const originalBytes=Buffer.from(readFile(classificationPath));
 if(hash(originalBytes)!==classificationSha256)fail('ACL_ORIGINAL_REVIEW_CHANGED');
 const original=JSON.parse(originalBytes);
 if(!equal(historicalAcl,original.aclEvents))fail('ACL_HISTORICAL_EVENTS_CHANGED');
 if(equal(currentAcl,historicalAcl))return currentAcl;
 const proofPath='docs/healthroster-build/proposals/source-read-delegation-acl-forward-20261017/ACL_SOURCE_TRANSFER.json';
 const proofBytes=Buffer.from(readFile(proofPath));
 if(hash(proofBytes)!=='63b811a96ad050d92cd200a1de23a9464ec7a95c4f9ae909861cc412c3ba7547')fail('ACL_SOURCE_TRANSFER_CHANGED');
 const proof=JSON.parse(proofBytes);
 if(proof.version!=='HEALTH_SOURCE_READ_FIXED_ACL_EVENT_INVERSE_V1'||proof.native!=='NOT_RUN'
  ||proof.baselineRefreshed!==false||proof.roleChange!==false||proof.historicalAclCount!==75||proof.currentAclCount!==77
  ||proof.addedEvents.length!==2||proof.originalEvents.length!==2
  ||hash(JSON.stringify(historicalAcl))!==proof.historicalAclEventsSha256)fail('ACL_TRANSFER_SCOPE_CHANGED');
 const io={read:file=>Buffer.from(readFile(safe(file)))};
 for(const pin of [...proof.completeCurrentSourcePins,proof.unchangedOriginalSource])
  if(hash(canonical(io.read(pin.path).toString('utf8')))!==pin.sha256)fail('ACL_COMPLETE_SOURCE_CHANGED');
 const source=scanManagedSql(io.read(proof.unchangedOriginalSource.path).toString('utf8'));
 const statements=source.statements.map(s=>source.source.slice(s.start,s.end).trim());
 for(const event of proof.originalEvents){
  if(historicalAcl.filter(e=>equal(e,event)).length!==1||statements.filter(s=>s===event.statement).length!==1)fail('ACL_ORIGINAL_STATEMENT_CHANGED');
 }
 const names=new Set(original.rows.map(row=>row.signature.split('(')[0]));
 const rootPath=proof.addedEvents[0].root;
 const current=closure(rootPath,io).filter(row=>aclRelevant(row.sql,names))
  .map(row=>({root:rootPath,physical:row.physical,sha256:hash(row.sql),statement:row.sql}));
 if(!equal(current,proof.addedEvents)||!equal(current.map(e=>e.statement),proof.originalEvents.map(e=>e.statement)))fail('ACL_CURRENT_STATEMENT_CHANGED');
 const acl=scanManagedSql(io.read(proof.addedEvents[0].physical).toString('utf8'));
 if(acl.statements.length!==proof.currentAclStatements)fail('ACL_COMPLETE_PROGRAM_CHANGED');
 if(!equal(currentAcl,[...historicalAcl,...current]))fail('ACTUAL_ACL_CHANGED');
 return historicalAcl;
}

// The injectable read/list boundary is for source-only drift tests. It cannot
// change the fixed review hashes, caller, path, signatures or returned four names.
export function createReviewedSourceClassifierDelegation(root=repoRoot,overrides={}){
 return function reviewedSourceClassifierDelegates(source,file,signature,part){
  if(signature!==caller)return [];
  if(file!==physical)fail('CALLER_PATH_CHANGED');
  const io=access(root,overrides),data=load(root,io),pin=data.rows[0],parsed=scanManagedSql(source),whole=parsed.statements.filter(s=>routineDefinitions(parsed.source.slice(s.start,s.end),file).includes(caller));
  if(whole.length!==1||hash(parsed.source.slice(whole[0].start,whole[0].end).trim())!==pin.completeStatementSha256||!part||part.signature!==caller||part.unclassified||hash(part.text.trim())!==pin.bodySha256)fail('COMPLETE_CALLER_CHANGED');
  const current=sourceClassifierEvidenceSnapshot(root,data.rows,overrides);
  if(!equal(current.definitions,data.definitionEvents))fail('ACTUAL_DEFINITION_OWNER_CHANGED');
  if(!equal(sourceClassifierAclPredecessor(current.acl,data.aclEvents,file=>io.read(file)),data.aclEvents))fail('ACTUAL_ACL_CHANGED');
  for(const row of data.rows){const final=current.definitions.filter(d=>d.signature===row.signature&&d.root.startsWith('supabase/repeatable/')).at(-1);if(!final||final.root!==row.root||final.physical!==row.physical||final.sha256!==row.completeStatementSha256)fail('ACTUAL_FINAL_OWNER_CHANGED');}
  return [...delegates];
 };
}
export const reviewedSourceClassifierDelegates=createReviewedSourceClassifierDelegation();
