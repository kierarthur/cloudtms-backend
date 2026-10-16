import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {createHash} from 'node:crypto';
import {reviewedHealthAggregateHazards,reviewPath,reviewSha256} from '../scripts/healthroster-aggregate-type-review.mjs';
import {specialConstructViolations,executableParts,assertSpecialConstructs} from '../scripts/bpay-next-source-gates.mjs';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const bytes=fs.readFileSync(path.join(root,reviewPath));
const approval=JSON.parse(bytes);
const hash=value=>createHash('sha256').update(value).digest('hex');
const evidence=JSON.parse(fs.readFileSync(path.join(root,approval.sourceReview.path)));
const file=approval.files[0].path;
const source=fs.readFileSync(path.join(root,file),'utf8').replaceAll('\r\n','\n');
const violations=specialConstructViolations(source,file);
const parts=executableParts(source,file);
const classify=(s=source,f=file,v=violations,p=parts,b=new Map())=>reviewedHealthAggregateHazards(s,f,b,v,p);
const refused=work=>assert.throws(work,/HEALTHROSTER_AGGREGATE_TYPE_REVIEW_CHANGED/);

test('all 74 exact points in 51 complete source files consume their own pinned proof',()=>{
 assert.equal(hash(bytes),reviewSha256);
 let consumed=0;
 for(const entry of approval.files){
  const sql=fs.readFileSync(path.join(root,entry.path),'utf8').replaceAll('\r\n','\n');
  const result=classify(sql,entry.path,specialConstructViolations(sql,entry.path),executableParts(sql,entry.path));
  assert.equal(result.size,entry.points.length,entry.path);
  consumed+=result.size;
 }
 assert.equal(consumed,74);
});

test('generic unproved aggregate and UUID selection continue to fail',()=>{
 for(const expression of ['min(t.id)','min(t.payload)','min(t.id::text)::uuid']){
  const sql=`create function public.unreviewed() returns text language sql as $$select ${expression} from public.unreviewed_rows t$$;`;
  const foreign='supabase/repeatable/unreviewed.sql';
  const baseline=classify(sql,foreign,specialConstructViolations(sql,foreign),executableParts(sql,foreign));
  assert.equal(baseline.size,0);
  assert.throws(()=>assertSpecialConstructs(sql,foreign,baseline),/UNCLASSIFIED/);
 }
});

test('a foreign path cannot borrow a reviewed alias or statement',()=>{
 const baseline=new Map([['unrelated',2]]);
 assert.equal(classify(source,'supabase/repeatable/foreign.sql',violations,parts,baseline),baseline);
 assert.equal(baseline.size,1);
});

test('a predicate, comment, grouping or cast edit invalidates the complete source pin',()=>{
 for(const suffix of ['\n-- changed\n','\nselect 1;','\nselect min(id) from public.timesheets;'])
  refused(()=>classify(source+suffix));
 refused(()=>classify(source.replace(/\bmin\s*\(/i,'max(')));
});

test('proof does not migrate to a different routine signature',()=>{
 const changed=violations.map(v=>{
  if(v.hazard!=='AGGREGATE_TYPE_UNCLASSIFIED')return v;
  const key=JSON.parse(v.key);key[0]='public.foreign_owner()';
  return {...v,key:JSON.stringify(key)};
 });
 refused(()=>classify(source,file,changed));
});

test('missing and repeated exact occurrences are refused',()=>{
 const point=violations.find(v=>v.hazard==='AGGREGATE_TYPE_UNCLASSIFIED');
 refused(()=>classify(source,file,violations.filter(v=>v!==point)));
 refused(()=>classify(source,file,[...violations,point]));
});

test('a body replacement cannot retain an earlier expression proof',()=>{
 refused(()=>classify(source,file,violations,parts.map(p=>({...p,text:p.text+'\n-- replaced'}))));
 refused(()=>classify(source,file,violations,parts.map(p=>({...p,unclassified:true}))));
 refused(()=>classify(source,file,violations,[]));
});

test('other hazard kinds and previous baseline entries are preserved',()=>{
 const baseline=new Map([['original baseline',3]]);
 const other={hazard:'DYNAMIC_SQL_UNCLASSIFIED',key:JSON.stringify(['public.other()','DYNAMIC_SQL_UNCLASSIFIED','execute user_sql;'])};
 const result=classify(source,file,[...violations,other],parts,baseline);
 assert.equal(result.get('original baseline'),3);
 assert.equal(result.has(other.key),false);
 assert.equal(baseline.size,1);
 assert.equal(result.size,approval.files[0].points.length+1);
});

test('the saved type evidence includes every occurrence and keeps lossy route identity explicit',()=>{
 assert.equal(evidence.rows.flatMap(r=>r.aggregates).length,356);
 assert.equal(new Set(evidence.rows.flatMap(r=>r.aggregates.map(e=>e.expressionIndex))).size,146);
 assert.equal(evidence.findings[0].expressionIndex,119);
 assert.match(approval.routeReview.qualification,/not a manager recipient or complete policy-route seal/);
 assert.equal(approval.native,'NOT_RUN');
 for(const a of evidence.rows.flatMap(r=>r.aggregates)){
  assert.notEqual(a.inputType,'uuid');assert.notEqual(a.inputType,'jsonb');assert.notEqual(a.inputType,'bytea');
  if(a.aggregate==='SUM'&&a.inputType==='bigint')assert.equal(a.resultType,'numeric');
 }
});
