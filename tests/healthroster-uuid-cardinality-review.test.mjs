import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';import path from 'node:path';import {fileURLToPath} from 'node:url';
import {reviewPath,reviewedHealthUuidHazards} from '../scripts/healthroster-uuid-cardinality-review.mjs';
import {assertSpecialConstructs,specialConstructViolations,executableParts} from '../scripts/bpay-next-source-gates.mjs';
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..'),review=JSON.parse(fs.readFileSync(path.join(root,reviewPath),'utf8'));
for(const entry of review.files)test('exact whole guard/source/type review: '+entry.path,()=>{
 const source=fs.readFileSync(path.join(root,entry.path),'utf8'),rows=specialConstructViolations(source,entry.path),parts=executableParts(source,entry.path);
 const baseline=new Map([['retained-other-key',2]]),approved=reviewedHealthUuidHazards(source,entry.path,baseline,rows,parts);
 assert.equal(approved.get('retained-other-key'),2);assert.equal(baseline.size,1);
 for(const row of rows.filter(row=>row.hazard==='UUID_CARDINALITY_UNCLASSIFIED'&&JSON.parse(row.key)[0]===entry.signature))assert.ok(approved.get(row.key));
 for(const row of rows.filter(row=>row.hazard!=='UUID_CARDINALITY_UNCLASSIFIED'))assert.equal(approved.has(row.key),false);
});
const entry=review.files[0],source=fs.readFileSync(path.join(root,entry.path),'utf8'),rows=specialConstructViolations(source,entry.path),parts=executableParts(source,entry.path);
const uuidRow=rows.find(row=>row.hazard==='UUID_CARDINALITY_UNCLASSIFIED'&&JSON.parse(row.key)[0]===entry.signature);
test('ordinary MIN(TEXT)::UUID remains unclassified without a finite owner proof',()=>{
 assert.throws(()=>assertSpecialConstructs('do $$declare v uuid;begin select min(x.id::text)::uuid into v from public.example x;end$$;','unreviewed.sql'),/UUID_CARDINALITY/);
});
test('changed singleton guard or whole-source predicate fails closed',()=>{
 const changed=source.replace('count(*)=1','count(*)<=1');assert.notEqual(changed,source);
 assert.throws(()=>reviewedHealthUuidHazards(changed,entry.path,new Map(),rows,parts),/CARDINALITY_REVIEW_CHANGED/);
});
test('same source cannot borrow a different owner path',()=>{
 const baseline=new Map();assert.equal(reviewedHealthUuidHazards(source,'supabase/repeatable/foreign.sql',baseline,rows,parts),baseline);
});
test('changed body or missing/duplicate exact UUID point fails closed',()=>{
 assert.throws(()=>reviewedHealthUuidHazards(source,entry.path,new Map(),rows,parts.map(p=>p.signature===entry.signature?{...p,text:p.text+'\nperform 1;'}:p)),/CARDINALITY_REVIEW_CHANGED/);
 assert.throws(()=>reviewedHealthUuidHazards(source,entry.path,new Map(),rows.filter(r=>r!==uuidRow),parts),/CARDINALITY_REVIEW_CHANGED/);
 assert.throws(()=>reviewedHealthUuidHazards(source,entry.path,new Map(),[...rows,uuidRow],parts),/CARDINALITY_REVIEW_CHANGED/);
});
test('an extra foreign UUID statement receives no classification',()=>{
 const hazard='UUID_CARDINALITY_UNCLASSIFIED',foreign={hazard,key:JSON.stringify(['private.foreign()',hazard,'select min(x.id::text)::uuid from public.other x;'])};
 const approved=reviewedHealthUuidHazards(source,entry.path,new Map(),[...rows,foreign],parts);assert.equal(approved.has(foreign.key),false);
});
