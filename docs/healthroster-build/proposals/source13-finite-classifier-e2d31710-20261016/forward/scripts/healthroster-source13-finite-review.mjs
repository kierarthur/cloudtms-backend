// Exact Source13 scalar/tuple and closed DDL classifications only.
import fs from'node:fs';import path from'node:path';import{fileURLToPath}from'node:url';import{createHash}from'node:crypto';
import{scanManagedSql}from'./cloudtms-managed-release-sql.mjs';import{routineDefinitions}from'./bpay-next-final-definers.mjs';
const repoRoot=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..'),hash=x=>createHash('sha256').update(x).digest('hex'),canonical=x=>x.toString().replaceAll('\r\n','\n'),fail=code=>{throw Error('HEALTHROSTER_SOURCE13_'+code);};
export const reviewPath='docs/healthroster-build/v6-owner-delivery-20261016/F23_SOURCE13_FINITE_REVIEW_20261016/CLASSIFICATION.json';
export const reviewSha256='b633d7541cec1af44cd133819682d69e8ef98180f2493d31151745d614c5bfec';
const targets=new Set(["supabase/migrations/16102026_1100_nhsp_v6_source_health_metadata.sql","supabase/migrations/16102026_1101_nhsp_v6_source_retained_native_preparations.sql","supabase/migrations/16102026_1102_nhsp_v6_source_acceptance_applications_and_adoptions.sql","supabase/migrations/16102026_1103_nhsp_v6_source_canonical_publication_bridge.sql","supabase/migrations/16102026_1104_nhsp_v6_source_owner_only_rls.sql","supabase/migrations/16102026_1105_nhsp_v6_source_incident_resolution.sql","supabase/repeatable/16102026_1100_nhsp_v6_source_pay_authority/01_complete_source_authorities.sqlinc","supabase/repeatable/16102026_1100_nhsp_v6_source_pay_authority/02_exact_health_trigger_effects.sqlinc","supabase/repeatable/16102026_1100_nhsp_v6_source_pay_authority_v1.sql","supabase/repeatable/16102026_1110_nhsp_v6_source_pending_current/01_complete_pending_current.sqlinc","supabase/repeatable/16102026_1110_nhsp_v6_source_pending_current_v1.sql","supabase/repeatable/16102026_1120_nhsp_v6_source_invoice_duty/01_complete_invoice_duty.sqlinc","supabase/repeatable/16102026_1120_nhsp_v6_source_invoice_duty_v1.sql"]);
function load(root){
 const read=file=>fs.readFileSync(path.join(root,file)),bytes=read(reviewPath);if(hash(bytes)!==reviewSha256)fail('CLASSIFICATION_CHANGED');const data=JSON.parse(bytes);
 if(data.version!=='HEALTHROSTER_SOURCE13_FINITE_CLASSIFICATION_V1'||data.native!=='NOT_RUN'||data.sourceReviewCommit!=='e2d31710e925fbcfa4f5ce16760cdf42ea918e92'||data.filePins.length!==13||data.newOccurrences!==26||data.newUniqueKeys!==23)fail('CLASSIFICATION_SCOPE_CHANGED');
 for(const pin of[...data.filePins,...data.dependencies])if(hash(canonical(read(pin.path)))!==pin.sha256)fail('SOURCE_DEPENDENCY_CHANGED');
 for(const pin of data.evidence)if(hash(read(pin.path))!==pin.sha256)fail('REVIEW_EVIDENCE_CHANGED');
 const map=JSON.parse(read(data.mapPath)),review=JSON.parse(read(data.proofPath)),raw=JSON.parse(read(data.rawPath));
 if(map.version!=='F23_SOURCE13_FINITE_EXTENSION_INPUT_V1'||map.qualification!=='SOURCE_REVIEWED_FINITE_INPUT_NOT_ALREADY_GATE_OR_NATIVE_ADMITTED'
  ||map.counts.rawDiagnostics!==38||map.counts.frozenBaselineCoveredOccurrences!==12||map.counts.newFiniteOccurrences!==26||map.counts.newFiniteUniqueKeys!==23||map.counts.completeBodies!==11
  ||map.points.length!==23||map.completeProducerPins.length!==11||map.points.reduce((n,p)=>n+p.newDiagnosticCount,0)!==26
  ||map.native!=='NOT_RUN'||review.version!=='F23_MANAGED_SOURCE13_FINITE_REVIEW_V1'||review.native!=='NOT_RUN'||raw.native!=='NOT_RUN'||review.rows.length!==38||raw.rows.length!==38
  ||review.rows.filter(r=>r.counterpart.baselineCovered).length!==12||review.pendingReleaseSingletonLimit.status!=='NOT_PROVED_BY_DECLARED_UNIQUE_KEY'
  ||review.genericFenceEdits!==false||review.requiredSqlCorrections.length||map.requiredSqlCorrections.length||map.bankingSchemaOrAbiAssumptions.length||map.unchangedGenericFences!==true||map.unchangedOriginalSourceTypeMaps!==true)fail('REVIEW_SCOPE_CHANGED');
 if(JSON.stringify(data.filePins)!==JSON.stringify(map.filePins.map(p=>({path:p.path,sha256:p.sha256}))))fail('FILE_CENSUS_CHANGED');
 const seen=new Set(),allowed=new Set(['FINITE_CLOSED_HEALTH_DDL_FORMAT_PROVED','FINITE_INERT_CHECK_COMPARISON_PROVED','FINITE_SOURCE_SCALAR_AGGREGATE_TYPES_PROVED','FINITE_SOURCE_DECLARED_TUPLE_TYPE_PROVED']);
 for(const p of map.points){
  const identity=p.file+'\0'+p.key;if(seen.has(identity)||!targets.has(p.file)||p.exactFrozenBaselineConsumedCount!==0||p.newDiagnosticCount!==p.wholeSourceOccurrenceCount||p.reviewIds.length!==p.newDiagnosticCount||hash(p.key)!==p.keySha256||!allowed.has(p.reviewVerdict))fail('POINT_CHANGED');seen.add(identity);
  const [signature,hazard,statement]=JSON.parse(p.key);if(signature!==p.signature||hazard!==p.hazard||hash(statement)!==p.statementSha256)fail('POINT_CHANGED');
  for(const id of p.reviewIds){const proof=review.rows.find(r=>r.id===id);if(!proof||proof.file!==p.file||proof.key!==p.key||proof.bodySha256!==p.bodySha256||proof.statementSha256!==p.statementSha256||proof.proof.verdict!==p.reviewVerdict||proof.counterpart.baselineCovered!==false)fail('POINT_PROOF_CHANGED');}
 }
 for(const pin of data.producers)if(hash(canonical(read(pin.path)))!==pin.bodySha256)fail('COMPLETE_PRODUCER_CHANGED');
 return{data,map,review,raw};
}
export function createReviewedSource13Classifier(root=repoRoot){
 return function reviewedSource13Hazards(source,file,baseline,violations,parts){
  if(!targets.has(file))return baseline;const{data,map,raw}=load(root),pin=data.filePins.find(p=>p.path===file);
  if(!pin||hash(canonical(source))!==pin.sha256)fail('CURRENT_SOURCE_CHANGED');
  const expected=raw.rows.filter(r=>r.file===file);if(violations.length!==expected.length)fail('RAW_CENSUS_CHANGED');
  const keys=new Map();for(const row of expected)keys.set(row.key,(keys.get(row.key)??0)+1);
  for(const[key,count]of keys)if(violations.filter(v=>v.key===key).length!==count)fail('RAW_POINT_CHANGED');
  const producerPins=map.completeProducerPins.filter(p=>p.file===file),statements=producerPins.some(p=>p.completeStatementSha256)?scanManagedSql(source).statements:[];
  for(const producer of producerPins){
   const whole=parts.filter(p=>p.signature===producer.signature&&hash(p.text.trim())===producer.bodySha256);if(whole.length!==1||whole[0].unclassified)fail('COMPLETE_PRODUCER_CHANGED');
   if(producer.completeStatementSha256&&statements.filter(s=>{const text=source.slice(s.start,s.end).trim();return hash(text)===producer.completeStatementSha256&&routineDefinitions(text,file).includes(producer.signature);}).length!==1)fail('COMPLETE_STATEMENT_CHANGED');
  }
  const result=new Map(baseline);for(const p of map.points.filter(p=>p.file===file)){
   const actual=violations.filter(v=>v.key===p.key&&v.hazard===p.hazard);if(actual.length!==p.wholeSourceOccurrenceCount)fail('POINT_MULTIPLICITY_CHANGED');
   const statement=JSON.parse(p.key)[2],body=parts.filter(x=>x.signature===p.signature&&hash(x.text.trim())===p.bodySha256&&x.text.includes(statement));if(body.length!==1||body[0].unclassified)fail('POINT_PRODUCER_CHANGED');
   result.set(p.key,Math.max(result.get(p.key)??0,p.wholeSourceOccurrenceCount));
  }return result;
 };
}
export const reviewedSource13Hazards=createReviewedSource13Classifier();
