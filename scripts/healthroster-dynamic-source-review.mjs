// Exact source construction classification; never runtime/financial authority.
import fs from 'node:fs';import path from 'node:path';import{fileURLToPath}from'node:url';import{createHash}from'node:crypto';
const repoRoot=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
export const reviewPath='docs/healthroster-build/v6-owner-delivery-20261016/F23_DYNAMIC_SOURCE_REVIEW_20261016/CLASSIFICATION.json';
export const reviewSha256='6f66d839660fff83046838763cf111d79ac41ce6f0834d32f62f16c4634777da';
const hash=x=>createHash('sha256').update(x).digest('hex'),fail=()=>{throw Error('HEALTHROSTER_DYNAMIC_SOURCE_REVIEW_CHANGED');};
const canonical=x=>x.toString().replaceAll('\r\n','\n');

const groupingPath='docs/healthroster-build/v6-owner-delivery-20261016/F23_DYNAMIC_SOURCE_REVIEW_20261016/INVOICE_GROUPING_SUCCESSOR.json';
const groupingSha256='3f8aae697e6bea64dd41157379101bd8a84ad43c97e6ea9aaae19dd8fd1738d5';
function priorReviewedSource(source,file){
 const text=canonical(source);if(file!=='supabase/repeatable/16102026_0955_nhsp_v6_invoice_native_authority/01_complete_invoice_authorities.sqlinc')return text;
 if(hash(text)!=='e03acb39db8cc2441cde46125ad9b38e8594b3790c772b247cea76a052cb2044'||text.split("refs:=(snap->'rate_source_refs_json')||").length!==2)fail();
 const prior=text.replace("refs:=(snap->'rate_source_refs_json')||","refs:=snap->'rate_source_refs_json'||");
 if(hash(prior)!=='a8c57548c911fb11140be3b3d2256aba876995fb196396850b6fe38e2f0797f4')fail();return prior;
}

// A supplied root only locates the same immutable review and source pins. It
// cannot supply an alternate approval hash, reviewed body or acceptance rule.
export function createReviewedHealthDynamicClassifier(root=repoRoot){
 const read=(file,raw=false)=>{const b=fs.readFileSync(path.join(root,file));return raw?b:canonical(b);};
 function load(){
  if(hash(read(groupingPath,true))!==groupingSha256)fail();
  const b=read(reviewPath,true);if(hash(b)!==reviewSha256)fail();const data=JSON.parse(b);
  if(data.version!=='HEALTHROSTER_EXACT_DYNAMIC_CONSTRUCTION_CLASSIFICATION_V1'
   ||data.status!=='SOURCE_EXECUTION_CONSTRUCTION_ONLY_NATIVE_NOT_RUN'||data.native!=='NOT_RUN'
   ||data.sourceCommit!=='c1ebb691feb6a11324060e0c28210e2bd735624a'
   ||data.counts.diagnostics!==99||data.counts.proposedFinitePoints!==95
   ||data.counts.withheld1430!==3||data.counts.withheld0400!==1
   ||data.files.reduce((n,f)=>n+f.points.length,0)!==95)fail();
  for(const input of [data.review,data.priorReview,data.ast])if(hash(read(input.path,true))!==input.sha256)fail();
  const review=JSON.parse(read(data.review.path,true)),prior=JSON.parse(read(data.priorReview.path,true)),ast=JSON.parse(read(data.ast.path,true));
  if(review.version!=='HEALTHROSTER_CURRENT_DYNAMIC_SOURCE_REVIEW_V1'||review.native!=='NOT_RUN'
   ||review.status!==data.status||review.sourceCommit!==data.sourceCommit||review.rows.length!==38
   ||review.rows.flatMap(r=>r.points).length!==99||review.producers.length!==44
   ||review.inheritedFrozenPoints.length!==3||review.dailyLiteralSignatures.length!==52
   ||prior.version!=='HEALTH_F23_DYNAMIC_EXECUTION_READ_REVIEW_V1'||prior.counts.real_execute_points!==95
   ||ast.native!=='NOT_RUN'||ast.databaseCalls!==0||ast.counts.completeProducers!==44
   ||ast.counts.actualDynamicExecuteStatements!==100||ast.counts.dynamicForStatements!==0
   ||ast.counts.all35FilesCompleteDynamicProducers!==45||ast.counts.all35FilesActualDynamicExecuteStatements!==102)fail();
  // Recheck source inputs on each invocation: a reused process must not retain
  // approval after an on-disk dependency, owner, callee or registry edit.
  for(const input of [...data.dependencies,...data.files])if(hash(priorReviewedSource(read(input.path),input.path))!==input.sha256)fail();
  if(JSON.stringify(data.dependencies)!==JSON.stringify(review.dependencies))fail();
  const files=new Map();for(const f of data.files){
   if(files.has(f.path)||f.path.includes('13102026_1430_')||f.path.includes('14102026_0400_'))fail();
   const seen=new Set();for(const point of f.points){
    if(point.count!==1||seen.has(point.keySha256))fail();seen.add(point.keySha256);
    const rows=review.rows.filter(r=>r.file===f.path&&r.signature===point.signature);
    const evidence=rows.flatMap(r=>r.points).filter(p=>p.keySha256===point.keySha256);
    if(evidence.length!==1||evidence[0].disposition!=='FINITE_EXECUTION_CONSTRUCTION_ONLY'
     ||evidence[0].bodySha256!==point.bodySha256||evidence[0].statementSha256!==point.statementSha256
     ||rows[0].review.kind!==point.reviewKind||hash(evidence[0].key)!==point.keySha256
     ||hash(evidence[0].statement)!==point.statementSha256)fail();
   }files.set(f.path,f);
  }
  return files;
 }
 return function reviewedHealthDynamicHazards(source,file,baseline,violations,parts){
  const entry=load().get(file);if(!entry)return baseline;
  if(hash(priorReviewedSource(source,file))!==entry.sha256)fail();
  const raw=violations.filter(v=>v.hazard==='DYNAMIC_SQL_UNCLASSIFIED');
  if(raw.length!==entry.rawDynamicKeys.length)fail();
  for(const expected of entry.rawDynamicKeys)if(expected.count!==1||raw.filter(v=>hash(v.key)===expected.keySha256).length!==1)fail();
  const result=new Map(baseline);
  for(const point of entry.points){
   const actual=raw.filter(v=>hash(v.key)===point.keySha256);if(actual.length!==1)fail();
   const [signature,hazard,statement]=JSON.parse(actual[0].key);
   if(signature!==point.signature||hazard!=='DYNAMIC_SQL_UNCLASSIFIED'||hash(statement)!==point.statementSha256)fail();
   const body=parts.filter(p=>p.signature===signature&&p.text.includes(statement));
   if(body.length!==1||body[0].unclassified||hash(body[0].text.trim())!==point.bodySha256)fail();
   result.set(actual[0].key,Math.max(result.get(actual[0].key)??0,point.count));
  }return result;
 };
}
export const reviewedHealthDynamicHazards=createReviewedHealthDynamicClassifier();
