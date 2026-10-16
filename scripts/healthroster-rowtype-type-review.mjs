// Finite source-only tuple type review. No SQL, authority, predicate or effect waiver.
import fs from 'node:fs';
import path from 'node:path';
import {createHash} from 'node:crypto';
import {fileURLToPath} from 'node:url';
import {scanManagedSql} from './cloudtms-managed-release-sql.mjs';
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
export const reviewPath='docs/healthroster-build/v6-owner-delivery-20261016/ROWTYPE_TUPLE_TYPE_REVIEW.json';
export const reviewSha256='5b5a3dba3a65e2be56844eeec6a5ad5fbc582a7b423b7fcc01151e3a51d68fcc';
const hash=text=>createHash('sha256').update(text).digest('hex');
const fail=()=>{throw Error('HEALTHROSTER_ROWTYPE_TYPE_REVIEW_CHANGED');};
// This proves the complete declared table tuple, never a joined/partial tuple.
// Unclassified generic SELECT-star remains refused by the ordinary gate.
export function provesSingleTableTuple(row){
 let parsed;try{parsed=JSON.parse(row.key);}catch{return false;}
 if(!Array.isArray(parsed)||parsed.length!==3||typeof parsed[0]!=='string'||typeof parsed[2]!=='string'||parsed[1]!==row.hazard)return false;
 const match=row.hazard.match(/^ROWTYPE_SELECT:([A-Z_][A-Z0-9_]*):((?:PUBLIC|PRIVATE|PG_CATALOG) \. [A-Z_][A-Z0-9_]*)$/);
 if(!match)return false;
 const statements=scanManagedSql(parsed[2]).statements;if(statements.length!==1)return false;
 const t=statements[0].tokens,into=t.indexOf('INTO'),select=t.lastIndexOf('SELECT',into);
 const target=into+(t[into+1]==='STRICT'?2:1),relation=match[2].split(' ');
 if(into<0||select<0||t.slice(select+1,into).join(' ')!=='*'||t[target]!==match[1]
  ||t[target+1]!=='FROM'||t.slice(target+2,target+2+relation.length).join(' ')!==relation.join(' '))return false;
 let at=target+2+relation.length;
 const tails=new Set(['WHERE','ORDER','GROUP','HAVING','LIMIT','OFFSET','FETCH','FOR']);
 if(t[at]==='AS'){
  if(!/^[A-Z_][A-Z0-9_]*$/.test(t[at+1]??'')||tails.has(t[at+1]))return false;at+=2;
 }else if(t[at]&&!tails.has(t[at])){
  if(!/^[A-Z_][A-Z0-9_]*$/.test(t[at])||['JOIN','UNION','INTERSECT','EXCEPT','LOOP','THEN','END'].includes(t[at]))return false;at++;
 }
 if(at<t.length&&!tails.has(t[at]))return false;
 let depth=0;
 for(let i=at;i<t.length;i++){
  if(t[i]==='('||t[i]==='[')depth++;
  else if(t[i]===')'||t[i]===']')depth--;
  if(depth<0||depth===0&&['JOIN','UNION','INTERSECT','EXCEPT','LOOP'].includes(t[i]))return false;
 }
 return depth===0;
}
let checked;
function readReview(){
 if(checked)return checked;
 const bytes=fs.readFileSync(path.join(root,reviewPath));if(hash(bytes)!==reviewSha256)fail();
 const review=JSON.parse(bytes);
 if(review.version!=='HEALTHROSTER_EXACT_ROWTYPE_TUPLE_TYPE_REVIEW_V1'||review.qualification!=='SOURCE_TUPLE_TYPE_CLASSIFICATION_ONLY'
  ||review.native!=='NOT_RUN'||review.postgres17.points!==1204||review.postgres17.passed!==1204||review.postgres17.failures!==0
  ||!Array.isArray(review.files)||review.files.length!==151)fail();
 checked=new Map(review.files.map(file=>[file.path,file]));if(checked.size!==review.files.length)fail();return checked;
}
export function reviewedHealthRowtypeHazards(source,file,baseline,violations){
 const approved=readReview().get(file);if(!approved)return baseline;
 if(hash(source.replaceAll('\r\n','\n'))!==approved.sha256)fail();
 const actual=new Map();
 for(const row of violations.filter(row=>row.hazard.startsWith('ROWTYPE_SELECT:'))){
  if(!provesSingleTableTuple(row))fail();
  const keyHash=hash(row.key),prior=actual.get(keyHash);
  if(prior&&prior.key!==row.key)fail();actual.set(keyHash,{key:row.key,row,count:(prior?.count??0)+1});
 }
 if(actual.size!==approved.rows.length)fail();
 const result=new Map(baseline),seen=new Set();
 for(const row of approved.rows){
  const observed=actual.get(row.keySha256);if(!observed||seen.has(row.keySha256))fail();seen.add(row.keySha256);
  const [signature,hazard,statement]=JSON.parse(observed.key);
  if(row.signature!==signature||row.hazard!==hazard||row.statementSha256!==hash(statement)||row.count!==observed.count
   ||row.relation!==hazard.split(':').slice(2).join(':').replace(/\s+/g,'').toLowerCase())fail();
  result.set(observed.key,Math.max(result.get(observed.key)??0,row.count));
 }
 return result;
}
