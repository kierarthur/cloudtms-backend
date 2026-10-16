// Exact0400 prebody boundary. Source only; no native execution or generic waiver.
import fs from'node:fs';import path from'node:path';
import{canonical,digest,exactAtomicBody,scanManagedSql}from'./cloudtms-managed-release-sql.mjs';
const deepFreeze=value=>{if(value&&typeof value==='object'){for(const v of Object.values(value))deepFreeze(v);Object.freeze(value);}return value;};
export const CAPABILITY_PREDECESSOR_MIGRATION=deepFreeze(__PIN__);
const fail=code=>{throw Error('HEALTHROSTER_CAPABILITY_PREDECESSOR_'+code);};
function assertClosure(closure,kind){
 const p=CAPABILITY_PREDECESSOR_MIGRATION;
 if(kind!=='migration'||closure.path!==p.path||closure.files?.size!==1||closure.ordered?.length!==1
  ||closure.ordered[0].path!==p.path||closure.files.get(p.path)!==p.contentSha256
  ||digest(closure.ordered[0].source)!==p.contentSha256||closure.closureHash!==p.closureSha256
  ||closure.source!==closure.expanded)fail('IMMUTABLE_SOURCE_CHANGED');
 const source=digest(closure.expanded),expected=source===p.contentSha256?p.rawBodySha256:
  source===p.mappedSourceSha256?p.mappedBodySha256:null;
 if(!expected||digest(exactAtomicBody(closure.expanded))!==expected)fail('MAPPED_BODY_CHANGED');
}
export function capabilityPredecessorCompanionFor(closure,kind,{root,mapSource=value=>value}={}){
 const p=CAPABILITY_PREDECESSOR_MIGRATION;if(closure.path!==p.path)return null;
 assertClosure(closure,kind);const read=file=>canonical(fs.readFileSync(path.join(root,file),'utf8'));
 const program=read(p.companionPath),constructor=read(p.constructorPath);
 if(digest(program)!==p.companionSha256||digest(constructor)!==p.constructorSha256)fail('COMPANION_SOURCE_CHANGED');
 for(const input of p.evidenceFiles)if(digest(read(input.file))!==input.sha256)fail('EVIDENCE_SOURCE_CHANGED');
 const mappedConstructor=mapSource(constructor,p.constructorPath),ctorHash=digest(mappedConstructor);
 const ctorBody=ctorHash===p.constructorSha256?p.constructorRawBodySha256:
  ctorHash===p.constructorMappedSourceSha256?p.constructorMappedBodySha256:null;
 if(!ctorBody||digest(exactAtomicBody(mappedConstructor))!==ctorBody)fail('CONSTRUCTOR_MAPPING_CHANGED');
 const proof=JSON.parse(read(p.evidencePath)),expression=JSON.parse(read(p.expressionPath)),capture=JSON.parse(read(p.capturePath));
 if(proof.version!=='HEALTHROSTER_CAPABILITY0400_PREBODY_SOURCE_DESIGN_V1'
  ||proof.consumer.kind!=='migration'||proof.consumer.path!==p.path||proof.consumer.sourceSha256!==p.contentSha256
  ||proof.program.sha256!==p.companionSha256||proof.constructor.sha256!==p.constructorSha256
  ||proof.successor.expressionReview.sha256!==p.evidenceFiles.find(x=>x.file===p.expressionPath)?.preservedRawSha256
  ||proof.successor.sourceDesignApproved!==true||proof.successor.nativeCompanionApproved!==false
  ||proof.successor.nativeCompanion!=='NOT_RUN'||expression.sourceDesignExpressionApproved!==true
  ||expression.nativeCompanionApproved!==false||expression.nativeCompanion!=='NOT_RUN'
  ||capture.expression_approved!==false||capture.native_companion_approved!==false
  ||capture.all_check_count!==10||capture.selected_count!==2||capture.retained_check_count!==8)fail('EVIDENCE_SCOPE_CHANGED');
 const parsed=scanManagedSql(program),statementSha256=parsed.statements.map(s=>digest(program.slice(s.start,s.end)));
 if(parsed.meta.length||parsed.statements.length!==2||parsed.statements[0].tokens[0]!=='LOCK'
  ||parsed.statements[1].tokens[0]!=='DO'||JSON.stringify(statementSha256)!==JSON.stringify(p.statementSha256))fail('COMPANION_PACKET_CHANGED');
 const sql=mapSource(program,p.companionPath);if(sql!==program)fail('COMPANION_MAPPING_CHANGED');
 return{version:p.version,sourceRoot:root,migrationPath:p.path,migrationSha256:p.contentSha256,
  closureSha256:p.closureSha256,bodySha256:digest(exactAtomicBody(closure.expanded)),
  companionPath:p.companionPath,companionSha256:p.companionSha256,statementSha256,
  placement:'BEFORE_IMMUTABLE_BODY_SAME_TRANSACTION',sql,
  sourceFiles:[{file:p.companionPath,sha256:p.companionSha256},{file:p.constructorPath,sha256:p.constructorSha256}],
  evidenceFiles:p.evidenceFiles.map(row=>({...row}))};
}
export function capabilityPredecessorMigrationSql(unit,mapSource=value=>value){
 const p=CAPABILITY_PREDECESSOR_MIGRATION;
 if(unit.kind!=='migration'||unit.item?.path!==p.path||unit.item.sha256!==p.contentSha256
  ||unit.exception||unit.ownerRlsEnvelope||unit.immutableGuardDependency||unit.contactClaimCompanion||unit.moneyPredecessorCompanion
  ||!unit.capabilityPredecessorCompanion||unit.body!==exactAtomicBody(unit.closure.expanded))fail('ATOMIC_SCOPE_INVALID');
 assertClosure(unit.closure,unit.kind);
 const expected=capabilityPredecessorCompanionFor(unit.closure,unit.kind,{root:unit.capabilityPredecessorCompanion.sourceRoot,mapSource});
 if(JSON.stringify(unit.capabilityPredecessorCompanion)!==JSON.stringify(expected))fail('ATOMIC_COMPANION_CHANGED');
 return expected.sql;
}
