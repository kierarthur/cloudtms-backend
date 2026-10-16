import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {provesSingleTableTuple,reviewedHealthRowtypeHazards,reviewPath} from '../scripts/healthroster-rowtype-type-review.mjs';
import {assertSpecialConstructs,specialConstructViolations} from '../scripts/bpay-next-source-gates.mjs';
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const tuple=(sql,hazard='ROWTYPE_SELECT:R:PUBLIC . EXAMPLE')=>({hazard,key:JSON.stringify(['private.example()',hazard,sql])});
for(const sql of ['select * into r from public.example;','select * into strict r from public.example e where e.id=p_id for update;',
 'select * into r from public.example as e where e.state=case when p then 1 else 2 end;',
 'select * into r from public.example where exists(select 1 from public.other o join public.third z on z.id=o.id);'])
 test('complete one-table tuple: '+sql,()=>assert.equal(provesSingleTableTuple(tuple(sql)),true));
for(const sql of ['select id into r from public.example;','select * into r from public.other;',
 'select * into r from private.example;','select * into r from public.example e join public.other o on true;',
 'select * into r from public.example e,public.other o;','select * into r from public.example union select * from public.other;',
 'select * into r from (select * from public.example) x;','select * into r,x from public.example;',
 'select * into r from public.example e(id);','select * into r from public.example; select 1;',
 'select * into r from public.example e where true loop;', 'select * into r from public.example as where;'])
 test('refuse changed projection/source: '+sql,()=>assert.equal(provesSingleTableTuple(tuple(sql)),false));
test('generic unclassified SELECT-star still refuses without the finite review',()=>{
 assert.throws(()=>assertSpecialConstructs('do $$declare r public.example%rowtype;begin select * into r from public.example;end$$;','source.sql'),/ROWTYPE_SELECT/);
});
const review=JSON.parse(fs.readFileSync(path.join(root,reviewPath),'utf8')),entry=review.files[0];
const source=fs.readFileSync(path.join(root,entry.path),'utf8'),violations=specialConstructViolations(source,entry.path);
const rowViolations=violations.filter(row=>row.hazard.startsWith('ROWTYPE_SELECT:'));
test('exact reviewed source admits only consumed tuple keys and retains baseline',()=>{
 const baseline=new Map([['unrelated-preserved-key',2]]),approved=reviewedHealthRowtypeHazards(source,entry.path,baseline,violations);
 assert.equal(approved.get('unrelated-preserved-key'),2);assert.equal(baseline.size,1);
 for(const row of rowViolations)assert.ok(approved.get(row.key)>0);
 for(const row of violations.filter(row=>!row.hazard.startsWith('ROWTYPE_SELECT:')))assert.equal(approved.has(row.key),false);
});
test('changed predicate or even comment requires fresh whole-file review',()=>{
 assert.throws(()=>reviewedHealthRowtypeHazards(source+'\n-- source changed\n',entry.path,new Map(),violations),/TYPE_REVIEW_CHANGED/);
});
test('same statement cannot borrow another path approval',()=>{
 const baseline=new Map();assert.equal(reviewedHealthRowtypeHazards(source,'supabase/repeatable/unreviewed.sql',baseline,violations),baseline);
});
test('duplicate or removed tuple occurrence fails exact consumed census',()=>{
 assert.throws(()=>reviewedHealthRowtypeHazards(source,entry.path,new Map(),[...violations,rowViolations[0]]),/TYPE_REVIEW_CHANGED/);
 assert.throws(()=>reviewedHealthRowtypeHazards(source,entry.path,new Map(),violations.filter(row=>row!==rowViolations[0])),/TYPE_REVIEW_CHANGED/);
});
test('changed signature/declaration cannot borrow an identical tuple',()=>{
 const row=rowViolations[0],[sig,hazard,sql]=JSON.parse(row.key);
 const altered={hazard,key:JSON.stringify([sig+'_foreign',hazard,sql])};
 assert.throws(()=>reviewedHealthRowtypeHazards(source,entry.path,new Map(),violations.map(item=>item===row?altered:item)),/TYPE_REVIEW_CHANGED/);
});
