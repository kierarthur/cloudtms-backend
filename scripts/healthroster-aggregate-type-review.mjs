// Finite scalar-type evidence for reviewed complete producers, never an alias rule.
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {createHash} from 'node:crypto';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
export const reviewPath='docs/healthroster-build/v6-owner-delivery-20261016/AGGREGATE_TYPE_CLASSIFICATION.json';
export const reviewSha256='26dc83bb7ad2ccf6881b8c75de806e24cae018f7e4086e827c81c7522b188127';
const hash=value=>createHash('sha256').update(value).digest('hex');
const fail=()=>{throw Error('HEALTHROSTER_AGGREGATE_TYPE_REVIEW_CHANGED');};
const scalarTypes=new Set(['text','numeric','integer','bigint','date','timestamp with time zone']);
const canonicalObject=value=>JSON.stringify(Object.fromEntries(Object.entries(value).sort(([a],[b])=>a<b?-1:a>b?1:0)));
let cached;

function readReview(){
 if(cached)return cached;
 const bytes=fs.readFileSync(path.join(root,reviewPath));
 if(hash(bytes)!==reviewSha256)fail();
 const data=JSON.parse(bytes);
 if(data.version!=='HEALTHROSTER_EXACT_AGGREGATE_TYPE_CLASSIFICATION_V1'
  ||data.status!=='SOURCE_SCALAR_TYPE_ONLY_NATIVE_NOT_RUN'||data.native!=='NOT_RUN'
  ||data.files.length!==51||data.files.reduce((n,f)=>n+f.points.length,0)!==74)fail();
 for(const input of [data.sourceReview,data.routeReview,...data.schemaInputs])
  if(hash(fs.readFileSync(path.join(root,input.path)))!==input.sha256)fail();
 const review=JSON.parse(fs.readFileSync(path.join(root,data.sourceReview.path)));
 if(review.qualification!=='SOURCE_REVIEW_ONLY_NOT_GATE_ADMISSION'||review.native!=='NOT_RUN'
  ||review.rows.length!==74||review.aggregateOccurrenceCount!==356||review.distinctExpressionCount!==146)fail();
 const contract=JSON.parse(fs.readFileSync(path.join(root,review.schema.path)));
 const relations=new Map(contract.relations.map(row=>[`${row.schema}.${row.name}`,row]));
 for(const [ref,evidence] of Object.entries(review.schema.columns)){
  if(hash(canonicalObject(evidence.column))!==evidence.columnObjectSha256)fail();
  const [schema,table,column]=ref.split('.');
  const actual=relations.get(`${schema}.${table}`)?.columns.find(row=>row.name===column);
  if(evidence.column.proof==='native migration DDL'){
   const ddl=review.schema.additiveNativeDdl[ref];if(!ddl)fail();
   const source=fs.readFileSync(path.join(root,ddl.path),'utf8').replaceAll('\r\n','\n');
   if(hash(source)!==ddl.sha256||!source.includes(ddl.declaration))fail();
   const expected=evidence.column.type==='timestamptz'?'timestamp with time zone':evidence.column.type;
   if(actual&&actual.type!==expected)fail();
  }else if(!actual||canonicalObject(actual)!==canonicalObject(evidence.column))fail();
 }
 const expressions=new Set();let occurrences=0;
 for(const row of review.rows){
  if(row.hazard!=='AGGREGATE_TYPE_UNCLASSIFIED'||hash(row.statement)!==row.statementSha256
   ||hash(JSON.stringify([row.signature,row.hazard,row.statement]))!==row.keySha256)fail();
  for(const expression of row.aggregates){
   if(!scalarTypes.has(expression.inputType)||!expression.bindingProof
    ||hash(expression.input)!==expression.inputSha256)fail();
   const exact=row.statement.slice(expression.offset,expression.end);
   if(exact.slice(exact.indexOf('(')+1,-1)!==expression.input)fail();
   for(const ref of expression.nativeColumnRefs)if(!review.schema.columns[ref])fail();
   expressions.add(expression.expressionIndex);occurrences++;
  }
 }
 if(expressions.size!==146||occurrences!==356)fail();
 const rows=new Map(review.rows.map(row=>[row.id,row]));
 for(const file of data.files)for(const point of file.points){
  const row=rows.get(point.reviewId);
  if(!row||row.file!==file.path||row.sourceSha256!==file.sha256||row.signature!==point.signature
   ||row.keySha256!==point.keySha256||row.statementSha256!==point.statementSha256||point.count!==1)fail();
 }
 cached=new Map(data.files.map(file=>[file.path,file]));if(cached.size!==51)fail();
 return cached;
}

export function reviewedHealthAggregateHazards(source,file,baseline,violations,parts){
 const entry=readReview().get(file);if(!entry)return baseline;
 if(hash(source.replaceAll('\r\n','\n'))!==entry.sha256)fail();
 const result=new Map(baseline),seen=new Set();
 for(const point of entry.points){
  if(seen.has(point.keySha256))fail();seen.add(point.keySha256);
  const actual=violations.filter(row=>row.hazard==='AGGREGATE_TYPE_UNCLASSIFIED'&&hash(row.key)===point.keySha256);
  if(actual.length!==point.count)fail();
  const [signature,hazard,statement]=JSON.parse(actual[0].key);
  if(signature!==point.signature||hazard!=='AGGREGATE_TYPE_UNCLASSIFIED'||hash(statement)!==point.statementSha256)fail();
  const body=parts.filter(part=>part.signature===signature&&part.text.includes(statement));
  if(body.length!==1||body[0].unclassified||hash(body[0].text.trim())!==point.bodySha256)fail();
  result.set(actual[0].key,Math.max(result.get(actual[0].key)??0,point.count));
 }
 return result;
}
