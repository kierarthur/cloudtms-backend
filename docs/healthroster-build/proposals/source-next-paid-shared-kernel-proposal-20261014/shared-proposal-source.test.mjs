import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';import {fileURLToPath} from 'node:url';import test from 'node:test';
import {functionEvents,splitSqlStatements} from '../a0-current-root-install2-forward/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
const dir=path.dirname(fileURLToPath(import.meta.url)),read=f=>fs.readFileSync(path.join(dir,f),'utf8'),sha=b=>createHash('sha256').update(b).digest('hex');
const proof=JSON.parse(read('COMPLETE_SHARED_EXTRACTION_AND_REVERSES.json'));
const definition=f=>functionEvents(read(f)).events.find(x=>x.kind==='CREATE').definition;
const before=definition(proof.wrapper.before_file),wrapper=definition(proof.wrapper.forward_file),kernel=definition(proof.kernel.file);
test('Actual current public prefix through both admin checks and all pre-financial errors stays byte-identical',()=>{
 const h=proof.wrapper.hunks[0];assert.equal(wrapper.split(h.after).length,2);assert.equal(wrapper.replace(h.after,h.before),before);
 const prefix=before.slice(0,before.indexOf(h.before));assert.ok(wrapper.startsWith(prefix));
 assert.equal((wrapper.match(/weekly_source_office_authority_v1\(/g)||[]).length,2);
 assert.ok(wrapper.indexOf('return private.bpay_next_source_paid_evidence_kernel_v1')>wrapper.indexOf(proof.preserved_public_admissions[1].source));
 assert.equal(sha(before),proof.input.definition_sha256);assert.ok(!wrapper.includes('execute v_query'));
});
test('Whole pure native kernel reverses exactly to original with only name and two admission removals',()=>{
 const hs=proof.kernel.extraction_hunks;let reverse=kernel.replace(hs[0].after,hs[0].before);
 reverse=reverse.replace('\n  -- Current physical permission',hs[1].before+'\n  -- Current physical permission').replace('\n  v_origin:=',hs[2].before+'\n  v_origin:=');
 assert.equal(reverse,before);assert.equal(sha(kernel),proof.kernel.definition_sha256);
 assert.ok(!kernel.includes('weekly_source_office_authority_v1('));assert.match(kernel,/weekly_source_query_require_service_v1/);
});
test('Every original origin/header/three financial branches/assembly/result/cursor byte is single-owner unchanged',()=>{
 const marker='  v_origin:=private.bpay_next_source_origin_state_v1';const suffix=before.slice(before.indexOf(marker));
 assert.equal(kernel.slice(kernel.indexOf(marker)),suffix);assert.equal(sha(suffix),proof.kernel.financial_origin_header_three_queries_assembly_suffix_sha256);
 for(const branch of ["if v_kind='COMPONENTS' then","elsif v_kind='ACTIVE_HOLDS' then","else\n    v_query:=$query$"]){assert.ok(kernel.includes(branch),branch);}
 for(const name of ['BPAY_NEXT_SOURCE_PAID_FINANCIAL_TUPLE_INVALID','BPAY_NEXT_SOURCE_PAID_ROW_TOO_LARGE','BPAY_NEXT_SOURCE_PAID_PAGE_TOO_LARGE','quantity_certificate','activity_coverage','next_cursor'])assert.ok(kernel.includes(name));
 assert.ok(!wrapper.includes('BPAY_NEXT_SOURCE_PAID_FINANCIAL_TUPLE_INVALID'));
});
test('Original public service-only ACL is retained and both new private kernels have no direct service/browser grant',()=>{
 const pub=read(proof.wrapper.forward_file);assert.match(pub,/revoke all.*from public,anon,authenticated;/);assert.match(pub,/grant execute.*to service_role;/);
 for(const f of [proof.kernel.file,proof.source_purpose.file]){const s=read(f);assert.match(s,/revoke all.*from public,anon,authenticated,service_role;/);assert.ok(!/grant\s+execute/i.test(s));}
});
test('Source purpose derives true event/root/native work/current revision/component under native VIEW_PAY_WORK',()=>{
 const s=read(proof.source_purpose.file);for(const name of ['_pay_timesheet_rotation_scope','VIEW_PAY_WORK','lineage.work_event_id=v_event.id','c.candidate_id=v_event.candidate_id','c.client_id=v_event.client_id','v_event.work_date between','approved.source_component_id=v_component','revision.physical_timesheet_version=v_scope.version','component.component_member_identity=v_event.id::text'])assert.ok(s.includes(name),name);
 assert.match(s,/'kind','COMPONENTS','context_id',null/);assert.match(s,/v_page:=private.bpay_next_source_paid_evidence_kernel_v1\(v_request\)/);
 assert.ok(!s.includes('VIEW_SOURCE_PROGRESS'));assert.ok(!s.includes('execute v_query'));assert.ok(!s.includes('SETTLED_ORIGINAL'));
 assert.ok(!s.includes('set_config('));assert.ok(!/\b(insert into|update|delete from)\b/i.test(s));
});
test('Full Source context successor preserves all raw native identity/pages/UNKNOWN and restores exact frozen input',()=>{
 const beforeContext=read(proof.source_context.before_file);let after=read(proof.source_context.forward_file);
 for(const h of proof.source_context.hunks.toReversed()){assert.equal(after.split(h.after).length,2);after=after.replace(h.after,h.before);}assert.equal(after,beforeContext);assert.equal(sha(beforeContext),proof.source_context.input_sha256);
 const s=read(proof.source_context.forward_file);for(const text of ['v_header is distinct from','v_matched_component_count<>1','v_seen ?','NO_QUALIFIED_ORIGINAL','v_unknown','paid_quantity_known','withheld_amount_known'])assert.ok(s.includes(text));
 assert.ok(!s.includes("lower(btrim(role))='admin'"));assert.match(s,/weekly_source_kept_paid_components_page_v1\(p_actor_user_id,v_event.id,v_root,v_after,100\)/);
});
test('All proposed complete owners have stable SD type and literal signature/delimiter lexing only',()=>{
 for(const f of fs.readdirSync(path.join(dir,'forward')).filter(f=>f.endsWith('.sqlinc'))){const s=read('forward/'+f);assert.equal(splitSqlStatements(s).meta.length,0);assert.equal(functionEvents(s).events.filter(x=>x.kind==='CREATE').length,1);assert.match(s,/stable security definer/i);}
 assert.equal(proof.shared_banking_owner_change_proposed,true);assert.equal(proof.shared_banking_owner_change_approved,false);assert.equal(proof.canonical_sources_modified,false);assert.equal(proof.native_proof,false);
});
