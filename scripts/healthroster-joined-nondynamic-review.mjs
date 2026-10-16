// Exact tuple/scalar/cardinality source evidence for two reviewed joined leaves.
import fs from 'node:fs';import path from 'node:path';import{fileURLToPath}from'node:url';import{createHash}from'node:crypto';import{scanManagedSql}from'./cloudtms-managed-release-sql.mjs';import{routineDefinitions}from'./bpay-next-final-definers.mjs';
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
export const reviewFolder='docs/healthroster-build/v6-owner-delivery-20261016/F23_JOINED_NON_DYNAMIC_REVIEW_20261016';
export const reviewSha256='660d83da88bf88054106e4f407cc43bfe5b455cc82727dc5c705448641f6ab0e';
const hash=x=>createHash('sha256').update(x).digest('hex'),canon=x=>x.toString().replaceAll('\r\n','\n');
const knownFiles=new Set(['supabase/repeatable/16102026_0955_nhsp_v6_invoice_native_authority/01_complete_invoice_authorities.sqlinc','supabase/repeatable/16102026_1003_nhsp_v6_candidate_native_authority/01_final_functions.sqlinc']);
const fail=()=>{throw Error('HEALTHROSTER_JOINED_NON_DYNAMIC_REVIEW_CHANGED');};
export function createReviewedJoinedNonDynamicClassifier(repoRoot=root){
 const read=file=>fs.readFileSync(path.join(repoRoot,file));
 return function reviewedJoinedNonDynamicHazards(source,file,baseline,violations,parts){
  if(!knownFiles.has(file))return baseline;
  const bytes=read(reviewFolder+'/EXTENSION_MAP.json');if(hash(bytes)!==reviewSha256)fail();const map=JSON.parse(bytes);
  if(map.version!=='F23_JOINED_NON_DYNAMIC_FINITE_EXTENSION_MAP_V1'||map.qualification!=='SOURCE_REVIEWED_EXTENSION_INPUT_NOT_ALREADY_ADMITTED'||map.native!=='NOT_RUN'||map.sourceCommit!=='c1ebb691feb6a11324060e0c28210e2bd735624a'||map.points.length!==37||map.filePins.length!==2||map.completeFunctionPins.length!==16)fail();
  for(const artifact of map.artifacts)if(hash(read(reviewFolder+'/'+artifact.path))!==artifact.sha256)fail();
  const review=JSON.parse(read(reviewFolder+'/REVIEW.json')),provenance=JSON.parse(read(reviewFolder+'/PROVENANCE.json')),correction=JSON.parse(read(reviewFolder+'/CORRECTION_RECEIPT.json'));
  if(review.counts.tupleStatements!==26||review.counts.aggregateStatements!==7||review.counts.uuidStatements!==4||review.counts.minMaxExpressions!==64||review.counts.uuidCasts!==7||review.counts.eligibleFiniteExtensionPoints!==37||review.native!=='NOT_RUN'||provenance.native!=='NOT_RUN')fail();
  // These are exact current schema and additive DDL inputs, not type-name guesses.
  for(const input of [review.schema,review.schema.additiveMigration,{path:review.uuidNativeSchemaEvidence.aliases.path,sha256:review.uuidNativeSchemaEvidence.aliases.sourceSha256}])if(hash(read(input.path))!==input.sha256)fail();
  const originals=new Map();for(const pin of map.filePins){if(!knownFiles.has(pin.path))fail();let actual=canon(read(pin.path));if(hash(actual)!==pin.afterRequiredCorrectionSha256)fail();
   if(pin.correctionRequired){if(pin.path!==correction.target||pin.originalSha256!==correction.target_before_sha256||pin.afterRequiredCorrectionSha256!==correction.target_after_exact_replacement_sha256||correction.exactReplacement.count!==1||actual.split(correction.exactReplacement.after).length!==2)fail();actual=actual.replace(correction.exactReplacement.after,correction.exactReplacement.before);}
   if(hash(actual)!==pin.originalSha256)fail();originals.set(pin.path,actual);
  }
  // Full declaration/body provenance is preserved even for the single corrected
  // grouping owner. Only the enumerated other statements gain a finite allowance.
  const declarations=new Map();for(const [name,text]of originals){for(const row of scanManagedSql(text).statements){const complete=text.slice(row.start,row.end);for(const signature of routineDefinitions(complete,name)){const key=name+'#'+signature;if(declarations.has(key))fail();declarations.set(key,hash(complete));}}}
  for(const pin of map.completeFunctionPins)if(declarations.get(pin.file+'#'+pin.signature)!==pin.routineSha256)fail();
  for(const pin of provenance.nativeBindingDependencies)if(declarations.get(pin.file+'#'+pin.signature)!==pin.completeFunctionSha256)fail();
  const entry=map.filePins.find(p=>p.path===file);if(!entry||hash(canon(source))!==entry.afterRequiredCorrectionSha256)fail();
  const proofRows=[...review.tupleRows,...review.aggregateRows,...review.uuidRows],result=new Map(baseline),seen=new Set();
  for(const point of map.points.filter(p=>p.file===file)){
   if(!/^(?:ROWTYPE_SELECT:|AGGREGATE_TYPE_UNCLASSIFIED$|UUID_CARDINALITY_UNCLASSIFIED$)/.test(point.hazard)||seen.has(point.keySha256)||point.wholeSourceOccurrenceCount!==1||point.exactFrozenBaselineConsumedCount!==0||point.newDiagnosticCount!==1||hash(point.key)!==point.keySha256)fail();seen.add(point.keySha256);
   const proof=proofRows.filter(r=>r.file===file&&r.keySha256===point.keySha256);if(proof.length!==1||proof[0].proof.verdict!==point.reviewVerdict||proof[0].routineSha256!==point.completeFunctionSha256||proof[0].bodySha256!==point.bodySha256)fail();
   const actual=violations.filter(v=>v.hazard===point.hazard&&hash(v.key)===point.keySha256);if(actual.length!==1||actual[0].key!==point.key)fail();const [signature,hazard,statement]=JSON.parse(actual[0].key);if(signature!==point.signature||hazard!==point.hazard||hash(statement)!==point.statementSha256)fail();
   const body=parts.filter(p=>p.signature===signature&&p.text.includes(statement));if(body.length!==1||body[0].unclassified||hash(body[0].text.trim())!==point.bodySha256)fail();
   result.set(actual[0].key,Math.max(result.get(actual[0].key)??0,1));
  }return result;
 };
}
export const reviewedJoinedNonDynamicHazards=createReviewedJoinedNonDynamicClassifier();
