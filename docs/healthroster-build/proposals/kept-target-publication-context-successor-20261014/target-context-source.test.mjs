import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import test from 'node:test';import {createHash} from 'node:crypto';import {fileURLToPath} from 'node:url';
import {functionEvents,sqlTokens} from '../a0-current-root-install2-forward/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
const dir=path.dirname(fileURLToPath(import.meta.url)),read=f=>fs.readFileSync(path.join(dir,f),'utf8'),hash=b=>createHash('sha256').update(b).digest('hex');
const context=read('forward/01_target_publication_context.sqlinc'),facts=read('forward/00_target_publication_facts.sqlinc');
const body=(s,id)=>{const e=functionEvents(s).events.filter(e=>e.kind==='CREATE'&&e.id===id);assert.equal(e.length,1,id);return e[0].body;};
const ctx=body(context,'private.weekly_source_kept_as_paid_context_for_publication_v1(uuid,uuid,uuid)'),fac=body(facts,'private.weekly_source_kept_as_paid_facts_for_publication_v1(uuid,text,uuid)');
const ctxOld=read('before/source_keep32_context.sqlinc'),factsOld=read('before/source_keep32_facts.sqlinc');

for(const [kind,after,before] of [['CONTEXT',context,ctxOld],['FACTS',facts,factsOld]])test(kind+' entire source reverse is exact and every hunk unique',()=>{
 const proof=JSON.parse(read('TARGET_'+kind+'_COMPLETE_REVERSE.json'));assert.equal(proof.forward_sha256,hash(after));assert.equal(proof.input_sha256,hash(before));let reversed=after;
 for(const h of [...proof.hunks].reverse()){assert.equal(reversed.split(h.after).length,2,h.name);reversed=reversed.replace(h.after,h.before);}assert.equal(reversed,before);assert.equal(proof.full_file_reverse_verified,true);
});
test('one original financial context body, with only literal target-facts binding changed',()=>{
 const old=body(ctxOld,'private.weekly_source_kept_as_paid_context_v1(uuid,uuid)');const marker='  v_source_present:=exists(';
 assert.equal(ctx.slice(ctx.indexOf(marker)).replace('private.weekly_source_kept_as_paid_facts_for_publication_v1(v_event.id,v_reason,v_publication)','private.weekly_source_kept_as_paid_facts_v1(v_event.id,v_reason)'),old.slice(old.indexOf(marker)));
 assert.equal((context.match(/private\.weekly_source_settlement_allocation_v1\(v_root\)/g)||[]).length,1);
 assert.equal((context.match(/private\.weekly_source_office_next_paid_evidence_page_v1\(/g)||[]).length,1);
});
test('primary facts tuple and complete native pay-head/credit body remain byte-identical',()=>{
 const old=body(factsOld,'private.weekly_source_kept_as_paid_facts_v1(uuid,text)'),marker='  select * into strict v_upload from public.weekly_source_uploads where id=v_publication.upload_id;';
 assert.equal(fac.slice(fac.indexOf(marker)),old.slice(old.indexOf(marker)));assert.match(fac,/'WEEKLY_SOURCE_KEPT_AS_PAID_FACTS_V1'/);
});
test('original wrapper retains exact event/lineage admission before latest-current selection',()=>{
 const old=body(ctxOld,'private.weekly_source_kept_as_paid_context_v1(uuid,uuid)'),wrapper=body(context,'private.weekly_source_kept_as_paid_context_v1(uuid,uuid)');
 const start='  select * into strict v_event from public.weekly_work_events where id=p_work_event_id;',end='  select publication.id';
 const prefix=old.slice(old.indexOf(start),old.indexOf(end));assert.ok(wrapper.includes(prefix));assert.ok(wrapper.indexOf('WEEKLY_SOURCE_KEEP_AS_PAID_LINEAGE_UNBOUND')<wrapper.indexOf('WEEKLY_SOURCE_KEEP_AS_PAID_EXPORT_UNAVAILABLE'));
 assert.match(wrapper,/publication\.state='CURRENT' and upload\.state='CURRENT' and upload\.purpose='ORDINARY'/);assert.doesNotMatch(wrapper,/CORRECTION_READY/);
});
test('context and facts bind the same supplied target and literal current/session authority',()=>{
 for(const s of [ctx,fac])for(const literal of ['publication.id=p_projection_publication_id','publication.source_cycle_id=cycle.id','cycle.current_projection_publication_id=publication.id','publication.correction_session_id=correction.id','correction.replacement_correction_upload_id=upload.id','correction.replacement_projection_publication_id=publication.id','prior.manifest_hash=correction.expected_final_manifest_hash','cycle.current_final_revision_id=prior.id',"'WEEKLY_SOURCE_CORRECT_FINAL_REVIEW_RESULT_V1'"]){assert.ok(s.includes(literal),literal);}
 assert.match(ctx,/correction\.actor_user_id=p_actor_user_id/);assert.doesNotMatch(fac,/p_actor_user_id/);
 assert.ok(ctx.includes('private.weekly_source_kept_as_paid_facts_for_publication_v1(v_event.id,v_reason,v_publication)'));
});
test('actual native correction schema provides every referenced session field',()=>{
 const schema=read('before/current_correction_session.table.sqlinc');const fields=new Set([...schema.matchAll(/^  ([a-z_][a-z_0-9]*) (?:uuid|bytea|jsonb|bigint|text|timestamptz)/gm)].map(x=>x[1]));
 for(const name of new Set([...ctx.matchAll(/correction\.([a-z_][a-z_0-9]*)/g)].map(x=>x[1])))assert.ok(fields.has(name),name);
 assert.match(schema,/'REVIEWED','PREPARING','PREPARED','COMMITTING'/);
});
test('source selection counterexample: old owner reselects CURRENT instead of requested correction',()=>{
 // This deliberately models only the SELECT target/state boundary visible in
 // the actual source. It does not evaluate SQL, actor authority or finance.
 const pick=(s,rows,target)=>rows.filter(r=>s.includes('publication.id=p_projection_publication_id')?r.id===target:r.state==='CURRENT').sort((a,b)=>b.order-a.order)[0]?.id;
 const rows=[{id:'old-current',state:'CURRENT',order:2},{id:'real-replacement',state:'CORRECTION_READY',order:3}];
 assert.equal(pick(ctxOld,rows,'real-replacement'),'old-current');assert.equal(pick(ctx,rows,'real-replacement'),'real-replacement');assert.equal(pick(fac,rows,'real-replacement'),'real-replacement');
 rows.push({id:'other-current',state:'CURRENT',order:4});assert.equal(pick(ctx,rows,'real-replacement'),'real-replacement');assert.equal(pick(fac,rows,'missing-target'),undefined);
});
test('raw presence, exact current money, unknown paid/withheld and acknowledgement order retained',()=>{
 for(const literal of ['row.external_source_key=v_event.profile_external_key',"v_paid_state:='UNKNOWN'",'order by (audit.after_json->>\'acknowledgement_sequence\')::bigint desc',"'paid_quantity_known',false,'withheld_amount_known',false"]){assert.ok(ctx.includes(literal),literal);}
 assert.ok(ctx.indexOf("order by (audit.after_json->>'acknowledgement_sequence')::bigint desc")<ctx.indexOf('v_decision.facts_fingerprint=v_facts'));
 assert.doesNotMatch(ctx,/paid:=true;\s*v_paid_state:='UNKNOWN'/);
});
test('exact four signatures and immediate owner-private ACL, no public entry or money write',()=>{
 const expected=['private.weekly_source_kept_as_paid_context_for_publication_v1(uuid,uuid,uuid)','private.weekly_source_kept_as_paid_context_v1(uuid,uuid)','private.weekly_source_kept_as_paid_facts_for_publication_v1(uuid,text,uuid)','private.weekly_source_kept_as_paid_facts_v1(uuid,text)'];
 assert.deepEqual([...functionEvents(context+facts).events.filter(e=>e.kind==='CREATE').map(e=>e.id)].sort(),expected.sort());
 for(const id of expected){assert.ok((context+facts).includes('alter function '+id+' owner to postgres;'));assert.ok((context+facts).includes('revoke all on function '+id+' from public,anon,authenticated,service_role;'));}
 for(const s of [ctx,fac])assert.doesNotMatch(s,/\b(?:insert into|update public\.|delete from|truncate|pg_advisory_xact_lock)\b/i);
});
test('literal body parentheses balance without claiming SQL compilation',()=>{
 for(const s of [context,facts])for(const e of functionEvents(s).events.filter(e=>e.kind==='CREATE')){
  let n=0,opens=0;for(const t of sqlTokens(e.body)){if(t.kind!=='punctuation')continue;if(t.value==='('){n++;opens++;}if(t.value===')')n--;assert.ok(n>=0,e.id);}assert.ok(opens>0,e.id+' actually scanned punctuation');assert.equal(n,0,e.id);
 }
});
