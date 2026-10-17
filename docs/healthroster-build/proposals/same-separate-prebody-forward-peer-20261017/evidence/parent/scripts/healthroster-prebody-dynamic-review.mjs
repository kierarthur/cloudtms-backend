// Four exact catalogue-name DROP constructions, contingent on their actual
// checked compiler/prebody packets. Source construction only; no DB execution.
import fs from 'node:fs';import path from 'node:path';import{fileURLToPath}from'node:url';
import{canonical,digest,exactAtomicBody}from'./cloudtms-managed-release-sql.mjs';
import{compileManagedUnit,unitCheckpointSql,assertWriterSql,migrationLedgerSql}from'./cloudtms-managed-release.mjs';
import{compiledAtomicRehearsalUnit}from'./healthroster-install1-full-rehearsal.mjs';
import{mapExecutableSqlSource}from'./cloudtms-db-release-lib.mjs';
const repoRoot=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
export const reviewPath='docs/healthroster-build/v6-owner-delivery-20261016/F23_PREBODY_DYNAMIC_REVIEW_20261016/CLASSIFICATION.json';
export const reviewSha256='69d2b3aee092e78e36386642a8fbf65df406d5fc9f5d8e6a6d42bccf993895b0';
const targets=new Set(['supabase/migrations/13102026_1430_weekly_source_invoice_money_origins.sql','supabase/migrations/14102026_0400_healthroster_protected_source_capability.sql']);
const fail=code=>{throw Error('HEALTHROSTER_PREBODY_DYNAMIC_'+code);};
const same=(a,b)=>JSON.stringify(a)===JSON.stringify(b);
// The separately reviewed 16Oct RPC reload tail changed one unrelated replay
// pin. Reverse only that exact whole-file successor for the original prebody
// compiler review; the four immutable migration packets remain checked below.
export function prebodyDependencySource(input,value){
 const source=canonical(value);
 const prior='069e6d1d2361c6c8026b193116db565d40503b9a2e338ddb75c4b1dadb15d415';
 if(input.file!=='scripts/cloudtms-managed-execution-policy.mjs'||input.sha256!==prior||digest(source)===prior)return source;
 if(digest(source)!=='ba1ed5d23cdd639e814c7e009bfa07502dd633511e9e0be265d2a8486fa495d1')fail('BOUND_SOURCE_CHANGED');
 const before="['repeatable/28092026_1234_stage2_hosted_final_authority_reassert_v1.sql','4b918f87f5c0e5878b1b338b8f0db1e3aa130375439d15734a2f7f41cd277f53','SOURCE_REASSERT']";
 const after="['repeatable/28092026_1234_stage2_hosted_final_authority_reassert_v1.sql','3b276d4fe9cdd614aa10d5779c299f233ffae59e792b47e155a89a831e8d923c','SOURCE_REASSERT']";
 if(source.split(after).length!==2)fail('BOUND_SOURCE_CHANGED');
 const reversed=source.replace(after,()=>before);
 if(digest(reversed)!==prior)fail('BOUND_SOURCE_CHANGED');return reversed;
}
function load(root){
 const read=file=>fs.readFileSync(path.join(root,file)),bytes=read(reviewPath);
 if(digest(bytes)!==reviewSha256)fail('REVIEW_CHANGED');const review=JSON.parse(bytes);
 if(review.version!=='HEALTHROSTER_FOUR_PREBODY_DYNAMIC_SOURCE_CLASSIFICATION_V1'
  ||review.status!=='SOURCE_CONSTRUCTION_ONLY_NATIVE0400_NOT_RUN'||review.native0400!=='NOT_RUN'
  ||review.sourceConstructionOnly!==true||review.points!==4||review.files.length!==2
  ||review.files.reduce((n,f)=>n+f.points.length,0)!==4||review.databaseCalls!==0)fail('REVIEW_SCOPE_CHANGED');
 for(const input of review.dependencies){const value=read(input.file);
  if(digest(prebodyDependencySource(input,value))!==input.sha256||(input.preservedRawSha256&&digest(value)!==input.preservedRawSha256))fail('BOUND_SOURCE_CHANGED');
 }
 // Detect stale critical exports after an on-disk implementation replacement.
 for(const [file,fn]of[['scripts/cloudtms-managed-release.mjs',compileManagedUnit],['scripts/cloudtms-managed-release.mjs',unitCheckpointSql],['scripts/cloudtms-managed-release.mjs',migrationLedgerSql],['scripts/healthroster-install1-full-rehearsal.mjs',compiledAtomicRehearsalUnit],['scripts/cloudtms-db-release-lib.mjs',mapExecutableSqlSource]])
  if(!canonical(read(file)).includes(canonical(fn.toString())))fail('LOADED_COMPILER_CHANGED');
 if(digest(canonical(assertWriterSql))!==review.writerGuardSha256)fail('LOADED_WRITER_CHANGED');
 const priorBytes=read(review.priorReview.path);if(digest(priorBytes)!==review.priorReview.sha256)fail('PRIOR_REVIEW_CHANGED');
 const prior=JSON.parse(priorBytes);if(prior.version!=='HEALTHROSTER_CURRENT_DYNAMIC_SOURCE_REVIEW_V1'||prior.rows.flatMap(r=>r.points).length!==99)fail('PRIOR_REVIEW_CHANGED');
 const seen=new Set();for(const file of review.files){
  if(!targets.has(file.path)||seen.has(file.path)||file.kind!=='migration'||file.points.length!==(file.path.includes('13102026_1430_')?3:1))fail('REVIEW_SCOPE_CHANGED');seen.add(file.path);
  const keys=new Set();for(const point of file.points){
   if(point.count!==1||keys.has(point.keySha256)||digest(point.key)!==point.keySha256||digest(point.statement)!==point.statementSha256)fail('POINT_CHANGED');keys.add(point.keySha256);
   const parsed=JSON.parse(point.key);if(parsed[0]!==point.signature||parsed[1]!=='DYNAMIC_SQL_UNCLASSIFIED'||parsed[2]!==point.statement)fail('POINT_CHANGED');
   const original=prior.rows.filter(r=>r.file===file.path&&r.signature===point.signature).flatMap(r=>r.points).filter(p=>p.key===point.key);if(original.length!==1||original[0].bodySha256!==point.bodySha256||original[0].statementSha256!==point.statementSha256)fail('PRIOR_POINT_CHANGED');
   if(digest(canonical(read(point.bodyFile)).trim())!==point.bodySha256)fail('PRODUCER_CHANGED');
  }
 }
 return review;
}
function checkedPacket(unit,file,review,root){
 const entry=review.files.find(f=>f.path===file);if(!entry||unit.item?.path!==file||unit.item.sha256!==entry.sha256||unit.kind!=='migration')fail('PACKET_SCOPE_CHANGED');
 if(unit.exception||unit.ownerRlsEnvelope||unit.immutableGuardDependency||unit.contactClaimCompanion)fail('FOREIGN_ENVELOPE');
 const receipt=unit[entry.receiptField],other=entry.receiptField==='moneyPredecessorCompanion'?'capabilityPredecessorCompanion':'moneyPredecessorCompanion';
 if(!receipt||unit[other]||receipt.sourceRoot!==root||receipt.placement!=='BEFORE_IMMUTABLE_BODY_SAME_TRANSACTION'
  ||receipt.version!==entry.version||receipt.companionPath!==entry.program.file||receipt.companionSha256!==entry.program.sha256
  ||!same(receipt.statementSha256,entry.program.statementSha256)||receipt.migrationPath!==file||receipt.migrationSha256!==entry.sha256
  ||receipt.closureSha256!==entry.closureSha256||receipt.sql!==canonical(fs.readFileSync(path.join(root,entry.program.file))))fail('RECEIPT_CHANGED');
 const variant=entry.variants.find(v=>v.sourceSha256===digest(unit.closure?.expanded??''));
 if(!variant||unit.closure.path!==file||unit.closure.closureHash!==entry.closureSha256
  ||unit.body!==exactAtomicBody(unit.closure.expanded)||digest(unit.body)!==variant.bodySha256||receipt.bodySha256!==variant.bodySha256)fail('BODY_CHANGED');
 const packet=unitCheckpointSql(unit,review.context),expected=`begin;\n${assertWriterSql}\n${receipt.sql}\n${unit.body}\n${assertWriterSql}\n${migrationLedgerSql(unit.item,review.context)}\ncommit;`;
 if(packet!==expected||digest(packet)!==variant.releasePacketSha256)fail('PACKET_ORDER_CHANGED');
 const rehearsal=compiledAtomicRehearsalUnit(unit);
 if(rehearsal.sql!==receipt.sql+'\n'+unit.body||rehearsal.sqlHash!==digest(rehearsal.sql)
  ||digest(rehearsal.sql)!==variant.rehearsalSqlSha256||digest(JSON.stringify(rehearsal))!==variant.rehearsalReceiptSha256
  ||!same(rehearsal,variant.rehearsalReceipt))fail('REHEARSAL_RECEIPT_CHANGED');
 return entry;
}
// Exposed for finite refusal tests; no callback, executor or alternate approval.
export function assertReviewedPrebodyPacket(unit,file,{root=repoRoot}={}){
 if(!targets.has(file))fail('PACKET_SCOPE_CHANGED');return checkedPacket(unit,file,load(root),root);
}
export function createReviewedPrebodyDynamicClassifier(root=repoRoot){
 return function reviewedPrebodyDynamicHazards(source,file,baseline,violations,parts){
  if(!targets.has(file))return baseline;const review=load(root),entry=review.files.find(f=>f.path===file);
  if(digest(canonical(source))!==entry.sha256||digest(canonical(fs.readFileSync(path.join(root,file))))!==entry.sha256)fail('IMMUTABLE_SOURCE_CHANGED');
  const raw=violations.filter(v=>v.hazard==='DYNAMIC_SQL_UNCLASSIFIED');if(raw.length!==entry.points.length)fail('POINT_CENSUS_CHANGED');
  const unit=compileManagedUnit({path:file,sha256:entry.sha256},'migration',{root,mapSource:mapExecutableSqlSource});checkedPacket(unit,file,review,root);
  const result=new Map(baseline);for(const point of entry.points){
   const actual=raw.filter(v=>digest(v.key)===point.keySha256);if(actual.length!==1||actual[0].key!==point.key)fail('POINT_CHANGED');
   const body=parts.filter(p=>p.signature===point.signature&&p.text.includes(point.statement));
   if(body.length!==1||body[0].unclassified||digest(body[0].text.trim())!==point.bodySha256)fail('PRODUCER_CHANGED');
   result.set(point.key,Math.max(result.get(point.key)??0,1));
  }return result;
 };
}
export const reviewedPrebodyDynamicHazards=createReviewedPrebodyDynamicClassifier();
