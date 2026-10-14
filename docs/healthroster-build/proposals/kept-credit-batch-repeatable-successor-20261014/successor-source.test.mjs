import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import test from 'node:test';import {createHash} from 'node:crypto';import {fileURLToPath} from 'node:url';
import {functionEvents,splitSqlStatements,sqlTokens} from '../a0-current-root-install2-forward/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
const dir=path.dirname(fileURLToPath(import.meta.url)),read=f=>fs.readFileSync(path.join(dir,f),'utf8'),sha=s=>createHash('sha256').update(s).digest('hex');
const proof=JSON.parse(read('COMPLETE_SUCCESSOR_AND_REVERSES.json')),old=read('before/10_complete_adjustments_apply.sqlinc'),writer=read('forward/10_complete_adjustments_apply.sqlinc');
test('Each complete successor preserves signatures and uniquely reverses every byte to exact frozen Keep32',()=>{
 for(const x of proof.changes){let s=read(x.forward_file);assert.equal(sha(s),x.after_sha256);for(const h of x.hunks.toReversed()){assert.equal(s.split(h.after).length,2);s=s.replace(h.after,h.before);}assert.equal(s,read(x.before_file));assert.equal(sha(s),x.before_sha256);}
});
// Counterexample schedule model uses the actual writer's facts-call placement.
// This exercises source ordering only, never PostgreSQL/financial qualification.
function sourceSchedule(source,batch,foreign=[]){const facts=source.indexOf('private.weekly_source_kept_as_paid_facts_v1('),add=source.indexOf('    for item in select value'),remove=source.indexOf('      delete from public.invoice_lines');assert.ok(facts>0&&add>0&&remove>0);
 const perItem=facts>add,credits=new Map([['event-a',new Set(batch.initial??[])]]),initial=[...credits.get('event-a')].sort().join('|');const readFacts=()=>[...credits.get('event-a')].sort().join('|');
 for(const id of foreign)credits.get('event-a').add(id);const validate=()=>assert.equal(readFacts(),initial,'source CAS changed');
 if(!perItem)validate();for(const id of batch.remove??[])credits.get('event-a').delete(id);
 for(const item of batch.add??[]){if(perItem)validate();credits.get('event-a').add(item.id);}return [...credits.get('event-a')].sort();}
test('KC1 source schedule: two distinct negative ADJs for same event no longer invalidate their own batch',()=>{
 const batch={initial:[],add:[{id:'credit-one',client_token:'one',amount:'-2.00'},{id:'credit-two',client_token:'two',amount:'-3.00'}]};
 assert.throws(()=>sourceSchedule(old,batch),/source CAS changed/);assert.deepEqual(sourceSchedule(writer,batch),['credit-one','credit-two']);
});
test('KC1 source schedule: remove an existing genuine credit plus its replacement validates before deletion',()=>{
 const batch={initial:['old-credit'],remove:['old-credit'],add:[{id:'replacement-credit',client_token:'replacement',amount:'-2.00'}]};
 assert.throws(()=>sourceSchedule(old,batch),/source CAS changed/);assert.deepEqual(sourceSchedule(writer,batch),['replacement-credit']);
});
test('Foreign pre-write credit census drift still refuses in both actual source schedules',()=>{
 const batch={initial:['original'],add:[{id:'new-credit'}]};for(const s of [old,writer])assert.throws(()=>sourceSchedule(s,batch,['foreign-credit']),/source CAS changed/);
});
test('Current facts/root/cycle/paid basis and full genuine removal metadata are checked before any own DML',()=>{
 const firstDml=Math.min(writer.indexOf('      delete from public.invoice_lines'),writer.indexOf('        insert into public.invoice_lines'));
 assert.ok(writer.indexOf('private.weekly_source_kept_as_paid_facts_v1(')<firstDml);assert.equal((writer.match(/private.weekly_source_kept_as_paid_facts_v1\(/g)||[]).length,1);
 for(const text of ['v_frame_ids is distinct from v_target_ids','v_removal_lines is distinct from','root_timesheet_ids','source_cycle_id','native_paid_basis_sha256'])assert.ok(writer.indexOf(text)<firstDml,text);
 const perAdd=writer.slice(writer.indexOf('    for item in select value'));assert.ok(!perAdd.includes('weekly_source_kept_as_paid_facts_v1('));
 for(const owner of ['invoice_recompute_totals','weekly_source_invoice_allocation_assert_v1','_audit_insert','_ctms_assert_invoice_mutable_draft_v1'])assert.ok(writer.includes(owner));
});
test('Source scope covers union of actual added and removed pointer events before first original invoice lock',()=>{
 const admit=read('forward/08_credited_targets_admit.sqlinc'),pub=read('forward/09_complete_invoice_apply_edits.sqlinc');
 assert.match(admit,/select unnest\(v_add_ids\) id union select/);assert.match(admit,/private.weekly_source_invoice_adjustment_line_valid_v1/);
 assert.ok(admit.indexOf('for v_candidate in')<admit.indexOf('perform 1 from public.weekly_source_groups'));
 assert.match(pub,/and \(exists\(select 1 from jsonb_array_elements/);assert.ok(pub.indexOf('weekly_source_invoice_credited_targets_admit_v1')<pub.indexOf('perform public._ctms_assert_invoice_mutable_draft_v1'));
 assert.match(admit,/v_id=any\(v_add_ids\) and/);assert.ok(!admit.includes('delete from public.invoice_lines'));
});
test('KC2 exact trigger-source replay replaces same three native guards and preserves unrelated audit triggers',()=>{
 const source=read('forward/12_keep_ack_native_guard.sqlinc'),before=read('before/12_keep_ack_native_guard.sqlinc');
 const entries=splitSqlStatements(source).statements.filter(x=>/^create or replace trigger\b/i.test(x.text.trim()));assert.equal(entries.length,3);
 const state=new Map([['unrelated_audit_guard','UNCHANGED']]);function replay(sql){for(const x of splitSqlStatements(sql).statements){const m=x.text.trim().match(/^create( or replace)? trigger (\w+)([\s\S]*)$/i);if(!m)continue;if(!m[1]&&state.has(m[2]))throw new Error('duplicate_object42710');state.set(m[2],m[3]);}}
 replay(source);const once=JSON.stringify([...state]);replay(source);assert.equal(JSON.stringify([...state]),once);assert.equal(state.get('unrelated_audit_guard'),'UNCHANGED');
 assert.throws(()=>replay(before),/duplicate_object42710/);const oldFunction=functionEvents(before).events[0],newFunction=functionEvents(source).events[0];assert.equal(newFunction.definition,oldFunction.definition);
});
test('All complete SQL bodies retain balanced literal parentheses and delimiter lexing, with no compiled/native claim',()=>{
 for(const f of fs.readdirSync(path.join(dir,'forward'))){const s=read('forward/'+f);assert.equal(splitSqlStatements(s).meta.length,0);for(const e of functionEvents(s).events){let depth=0;for(const t of sqlTokens(e.body)){if(t.kind!=='punctuation')continue;if(t.value==='(')depth++;if(t.value===')')depth--;assert.ok(depth>=0,f);}assert.equal(depth,0,f);}}
 assert.equal(proof.native_proof,false);assert.equal(proof.sql_compiled,false);assert.equal(proof.current_root_canonical_sources_modified,false);
});
