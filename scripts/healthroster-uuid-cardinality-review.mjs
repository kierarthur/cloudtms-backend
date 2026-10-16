// Finite reviewed UUID-cardinality statements. Not a generic MIN(UUID) allowance.
import fs from 'node:fs';import path from 'node:path';import {fileURLToPath} from 'node:url';import {createHash} from 'node:crypto';
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
export const reviewPath='docs/healthroster-build/v6-owner-delivery-20261016/UUID_CARDINALITY_CLASSIFICATION.json';
export const reviewSha256='4d7bd39d0d2669cb07a332e57f371b7d3b7e1ba4cbe9c189d8432b247f766aec';
const hash=value=>createHash('sha256').update(value).digest('hex');
const fail=()=>{throw Error('HEALTHROSTER_UUID_CARDINALITY_REVIEW_CHANGED');};let cached;
function readReview(){
 if(cached)return cached;const bytes=fs.readFileSync(path.join(root,reviewPath));if(hash(bytes)!==reviewSha256)fail();
 const data=JSON.parse(bytes);
 if(data.version!=='HEALTHROSTER_EXACT_UUID_CARDINALITY_CLASSIFICATION_V1'||data.status!=='SOURCE_CARDINALITY_ONLY_NATIVE_NOT_RUN'
  ||data.native!=='NOT_RUN'||data.files.length!==19||data.files.reduce((n,row)=>n+row.points.length,0)!==21)fail();
 for(const input of [data.review,{path:data.review.correction,sha256:data.review.correctionSha256},...data.schemaInputs])
  if(hash(fs.readFileSync(path.join(root,input.path??input.file)))!==input.sha256)fail();
 cached=new Map(data.files.map(row=>[row.path,row]));if(cached.size!==19)fail();return cached;
}
export function reviewedHealthUuidHazards(source,file,baseline,violations,parts){
 const entry=readReview().get(file);if(!entry)return baseline;
 if(hash(source.replaceAll('\r\n','\n'))!==entry.sha256)fail();
 const body=parts.filter(part=>part.signature===entry.signature);if(body.length!==1||body[0].unclassified||hash(body[0].text.trim())!==entry.bodySha256)fail();
 for(const guard of entry.guards)if(!body[0].text.includes(guard.text)||guard.sha256&&hash(guard.text)!==guard.sha256)fail();
 const points=violations.filter(row=>row.hazard==='UUID_CARDINALITY_UNCLASSIFIED'&&JSON.parse(row.key)[0]===entry.signature);
 const counts=new Map();for(const row of points){const keyHash=hash(row.key),prior=counts.get(keyHash);
  if(prior&&prior.key!==row.key)fail();counts.set(keyHash,{key:row.key,count:(prior?.count??0)+1});}
 if(counts.size!==entry.points.length)fail();const result=new Map(baseline),seen=new Set();
 for(const point of entry.points){
  const actual=counts.get(point.keySha256);if(!actual||seen.has(point.keySha256))fail();seen.add(point.keySha256);
  const [signature,hazard,statement]=JSON.parse(actual.key);
  if(signature!==entry.signature||hazard!=='UUID_CARDINALITY_UNCLASSIFIED'||hash(statement)!==point.statementSha256||actual.count!==point.count)fail();
  result.set(actual.key,Math.max(result.get(actual.key)??0,point.count));
 }
 return result;
}
