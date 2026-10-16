// Exact corrected Contact/Office scalar/tuple/closed-DDL source points only.
import fs from'node:fs';import path from'node:path';import{fileURLToPath}from'node:url';import{createHash}from'node:crypto';
import{scanManagedSql}from'./cloudtms-managed-release-sql.mjs';import{routineDefinitions}from'./bpay-next-final-definers.mjs';
const repoRoot=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..'),hash=x=>createHash('sha256').update(x).digest('hex'),canonical=x=>x.toString().replaceAll('\r\n','\n'),fail=code=>{throw Error('HEALTHROSTER_CONTACT_FINITE_'+code);};
export const reviewPath='docs/healthroster-build/v6-owner-delivery-20261016/F23_CONTACT_OFFICE_FINITE_REVIEW_20261016/CLASSIFICATION.json';
export const reviewSha256='REPLACE_REVIEW_SHA';
const targets=new Set(REPLACE_TARGETS);
function load(root){
 const read=file=>fs.readFileSync(path.join(root,file)),bytes=read(reviewPath);if(hash(bytes)!==reviewSha256)fail('CLASSIFICATION_CHANGED');const data=JSON.parse(bytes);
 if(data.version!=='HEALTHROSTER_CONTACT_OFFICE_FINITE_CLASSIFICATION_V1'||data.native!=='NOT_RUN'||data.sourceAssemblyCommit!=='bf7a321429969f2c43345ce05ad2679ce3dbe83c'||data.lockGroupingSuccessorCommit!=='bdb86a015c343b685f94682e5ae644f63fab7468'||data.filePins.length!==65||data.newOccurrences!==187||data.newUniqueKeys!==184)fail('CLASSIFICATION_SCOPE_CHANGED');
 for(const p of[...data.filePins,...data.dependencies])if(hash(canonical(read(p.path)))!==p.sha256)fail('SOURCE_DEPENDENCY_CHANGED');
 for(const p of data.evidence)if(hash(read(p.path))!==p.sha256)fail('REVIEW_EVIDENCE_CHANGED');
 for(const p of data.producers)if(hash(canonical(read(p.path)))!==p.bodySha256)fail('PRESERVED_PRODUCER_CHANGED');
 const map=JSON.parse(read(data.mapPath)),proof=JSON.parse(read(data.proofPath)),raw=JSON.parse(read(data.rawPath));
 if(map.version!=='CONTACT_OFFICE_FINITE_EXTENSION_INPUT_V1'||proof.version!=='CONTACT_OFFICE_FINITE_F23_SOURCE_REVIEW_V1'||raw.version!=='CONTACT_OFFICE_ACTUAL_CORRECTED_F23_CENSUS_V1'||map.native!=='NOT_RUN'||proof.native!=='NOT_RUN'||raw.native!=='NOT_RUN'
  ||map.points.length!==184||map.points.reduce((n,p)=>n+p.newDiagnosticCount,0)!==187||map.completeProducerPins.length!==80||proof.rows.length!==305||raw.rows.length!==305||proof.rows.filter(r=>r.baselineCovered).length!==118
  ||map.genericFenceEdits!==false||map.sqlEdits!==0||map.bankingSchemaOrAbiAssumptions.length||map.newUuidAllowance!==0||map.newJsonAllowance!==0
  ||map.counts.newDynamic!==8||map.counts.newAggregates!==15||map.counts.newTuples!==164||map.counts.functionDeclarations!==114||map.counts.uniqueSignatures!==112
  ||proof.closedCommandStart.retainedRefusal!=='WEEKLY_SOURCE_PER_TARGET_DISPATCH_REQUIRED'||proof.closedCommandStart.effects!=='NO_DML_NO_PERMIT_NO_PROVIDER_ATTEMPT')fail('REVIEW_SCOPE_CHANGED');
 if(JSON.stringify(data.filePins)!==JSON.stringify(raw.files.map(p=>({path:p.file,sha256:p.sha256}))))fail('FILE_CENSUS_CHANGED');
 const seen=new Set(),verdicts=new Set(['FINITE_SAME_RELATION_WHOLE_TUPLE_PROVED','FINITE_EXACT_SCALAR_AGGREGATE_PROVED','FINITE_CLOSED_CONTACT_DDL_IDENTIFIERS_PROVED','FINITE_CONTACT_EXACT_UNIQUE_PREDECESSOR_PROVED']);
 for(const p of map.points){
  const identity=p.file+'\0'+p.key;if(seen.has(identity)||!targets.has(p.file)||p.newDiagnosticCount+p.exactFrozenBaselineConsumedCount!==p.wholeSourceOccurrenceCount||p.reviewIds.length!==p.newDiagnosticCount||hash(p.key)!==p.keySha256||!verdicts.has(p.reviewVerdict))fail('POINT_CHANGED');seen.add(identity);
  const[signature,hazard,statement]=JSON.parse(p.key);if(signature!==p.signature||hazard!==p.hazard||hash(statement)!==p.statementSha256)fail('POINT_CHANGED');
  for(const id of p.reviewIds){const row=proof.rows.find(r=>r.id===id);if(!row||row.file!==p.file||row.key!==p.key||row.bodySha256!==p.bodySha256||row.statementSha256!==p.statementSha256||row.baselineCovered!==false||row.proof.verdict!==p.reviewVerdict)fail('POINT_PROOF_CHANGED');}
 }
 return{data,map,raw};
}
export function createReviewedContactFiniteClassifier(root=repoRoot){
 return function reviewedContactFiniteHazards(source,file,baseline,violations,parts){
  if(!targets.has(file))return baseline;const{data,map,raw}=load(root),pin=data.filePins.find(p=>p.path===file);
  if(!pin||hash(canonical(source))!==pin.sha256)fail('CURRENT_SOURCE_CHANGED');
  const expected=raw.rows.filter(r=>r.file===file);if(violations.length!==expected.length)fail('RAW_CENSUS_CHANGED');
  const keys=new Map();for(const r of expected)keys.set(r.key,(keys.get(r.key)??0)+1);
  for(const[key,count]of keys)if(violations.filter(v=>v.key===key&&v.hazard===JSON.parse(key)[1]).length!==count)fail('RAW_POINT_CHANGED');
  const producerPins=map.completeProducerPins.filter(p=>p.file===file),statements=producerPins.some(p=>p.completeStatementSha256)?scanManagedSql(source).statements:[];
  for(const p of producerPins){
   const whole=parts.filter(x=>x.signature===p.signature&&hash(x.text.trim())===p.bodySha256);if(whole.length!==1||whole[0].unclassified)fail('COMPLETE_PRODUCER_CHANGED');
   if(p.completeStatementSha256&&statements.filter(s=>{const text=source.slice(s.start,s.end).trim();return hash(text)===p.completeStatementSha256&&routineDefinitions(text,file).includes(p.signature);}).length!==1)fail('COMPLETE_STATEMENT_CHANGED');
  }
  const result=new Map(baseline);for(const p of map.points.filter(p=>p.file===file)){
   const actual=violations.filter(v=>v.key===p.key&&v.hazard===p.hazard);if(actual.length!==p.wholeSourceOccurrenceCount)fail('POINT_MULTIPLICITY_CHANGED');
   const statement=JSON.parse(p.key)[2],body=parts.filter(x=>x.signature===p.signature&&hash(x.text.trim())===p.bodySha256&&x.text.includes(statement));if(body.length!==1||body[0].unclassified)fail('POINT_PRODUCER_CHANGED');
   const inherited=baseline.get(p.key)??0;if(inherited>p.exactFrozenBaselineConsumedCount)fail('BASELINE_SCOPE_CHANGED');
   // One leaf40 key has one genuine old occurrence plus one new occurrence.
   // Empty baseline supplies only the reviewed increment, never the old credit.
   result.set(p.key,inherited+p.newDiagnosticCount);
  }return result;
 };
}
export const reviewedContactFiniteHazards=createReviewedContactFiniteClassifier();
