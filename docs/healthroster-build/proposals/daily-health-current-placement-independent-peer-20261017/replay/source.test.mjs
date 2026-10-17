import test from 'node:test';import assert from 'node:assert/strict';import fs from 'node:fs';import path from 'node:path';import cp from 'node:child_process';import crypto from 'node:crypto';import {fileURLToPath,pathToFileURL} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),read=f=>fs.readFileSync(path.join(here,f)),text=f=>read(f).toString(),sha=b=>crypto.createHash('sha256').update(b).digest('hex'),root='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026';
const sig='private.daily_rate_correction_health_current_placement_v1(uuid,uuid)',selection=JSON.parse(read('SOURCE_SELECTION.json')),edits=JSON.parse(read('FORWARD_EDITS.json'));
const ast=JSON.parse(read('PLACEMENT_AST_RESULTS.json'));
const contract=await import(pathToFileURL(path.join(here,'reference/root/broker/src/contract-correction-v3.mjs')));
test('all five current complete-parent forwards invert exactly to frozen Git source',()=>{
 assert.equal(edits.edits.length,5);
 for(const r of edits.edits){let s=text(r.file);assert.equal(sha(read(r.file)),r.afterSha256);for(const h of [...r.hunks].reverse()){assert.equal(s.split(h.after).length-1,1);s=s.replace(h.after,()=>h.before);}const before=read('reference/parent/'+r.file);assert.equal(s,before.toString());assert.equal(sha(before),r.beforeSha256);const git=cp.spawnSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',root,'show',r.parentCommit+':'+r.parentFile],{maxBuffer:20e6});assert.equal(git.status,0);assert(before.equals(git.stdout));}
});
test('one new private signature, no public DTO/schema/registry/runtime installation',()=>{
 assert.deepEqual(selection.newSignatures,[sig]);assert.equal(selection.publicDtoChanges,0);assert.equal(selection.schemaChanges,0);assert.equal(selection.registryChanges,0);assert.deepEqual(selection.runtimeTargets,[]);assert.equal(selection.installable,false);assert.equal(selection.bankJoins,'THREE_UNPROVIDED');assert.equal(sha(read(selection.newFiles[0].file)),selection.newFiles[0].sha256);
});
test('complete nine-owner selection retains five unchanged full files and exactly three held Bank slots',()=>{
 const inherited=JSON.parse(read('INHERITED_OWNERS.json'));assert.equal(inherited.inherited.length,5);assert.equal(selection.healthDefinitions,9);let combined='';for(const r of selection.completeHealthOwners){assert.equal(sha(read(r.file)),r.sha256);combined+=text(r.file)+'\n';}assert.equal((combined.match(/create or replace function private\./g)||[]).length,9);for(const tag of inherited.threeUnprovidedSlots)assert.equal(combined.split(tag).length-1,1);for(const r of inherited.inherited)assert.equal(sha(read(r.file)),r.sha256);assert.equal(inherited.runtimeEmission,'REFUSED_UNPROVIDED_GENUINE_BANK_COMPOSITION');
});
test('new routine admission is name-wide before definitions, owner-only ACL and independent exact verifier',()=>{
 const admission=text('admission/ONE_TIME_ADMISSION.sqlinc');assert.match(admission,/pg_catalog\.pg_proc p join pg_catalog\.pg_namespace n on n\.oid=p\.pronamespace/);assert.match(admission,/n\.nspname='private' and p\.proname='daily_rate_correction_health_current_placement_v1'/);assert.match(admission,/DAILY_CURRENT_PLACEMENT_NAME_COLLISION/);assert(admission.includes("'"+sig+"'"));assert.equal((admission.match(/'private\.daily_rate_correction_/g)||[]).length,9);
 const acl=text('health/99_acl.sqlinc');assert(acl.includes('alter function '+sig+' owner to postgres;'));assert(acl.includes('revoke all on function '+sig+' from PUBLIC,anon,authenticated,service_role;'));assert(text('verification/mandatory_security.sql').includes("('"+sig+"','jsonb','v','pg_catalog,public,private,pg_temp')"));assert(text('ADDITIVE_REVERSE.sqlinc').includes('drop function if exists '+sig+';'));
});
test('actual source scalar types are bigint ordinals, integer versions and opaque text booking',()=>{
 const relations=JSON.parse(read('reference/SCHEMA_TYPES.json')).relations;
 for(const [relation,column,type]of [['bpay_next_work','booking_id','text'],['bpay_next_work','current_revision_no','bigint'],['bpay_next_work_revision','revision_no','bigint'],['bpay_next_work_revision','physical_timesheet_version','integer'],['timesheets','version','integer'],['timesheets_financials','rate_source_refs_json','jsonb']])assert.equal(relations.find(r=>r.name===relation).columns.find(c=>c.name===column).type,type);
 assert.match(text('reference/root/supabase/migrations/16102026_0159_nhsp_v6_01_physical_source_review_schema.sql'),/original_booking_id text not null/);
 assert.match(text('health/20_current_placement.sqlinc'),/m\.original_booking_id=current_binding->>'booking_id'/);assert.doesNotMatch(text('health/20_current_placement.sqlinc'),/booking_id'\)::uuid/);
});
test('actual existing native approval owns exact current-operation/review provenance',()=>{
 const native=text('reference/root/supabase/repeatable/16102026_0858_nhsp_v6_daily_native_authority/02_admission.sqlinc');
 assert(native.includes("'approval_purpose','RATE_CORRECTION','correction_operation_id',o.id,'accepted_review_id',r.id"));assert(native.includes("'approved_by_user_id',c.actor_user_id"));assert(native.includes("'revision_no',context->'current_financial_revision'"));
 assert(text('reference/root/supabase/repeatable/17102026_0910_nhsp_v6_daily_role_identity/01_complete_context.sqlinc').includes("w.current_revision_no::text"));
});
test('new current reader preserves active Office role basis, no mutation-context financial shortcut',()=>{
 const s=text('health/20_current_placement.sqlinc');assert(s.includes('private.daily_rate_correction_health_result_basis_v1(p_operation_id,p_actor_user_id)'));assert(s.includes('private.daily_rate_correction_health_result_basis_v1(current_operation,p_actor_user_id)'));assert.doesNotMatch(s,/daily_rate_correction_context_v1|weekly_source_pay_actor_allowed_v1|\b(?:insert|update|delete|merge|execute|nextval|setval)\s/i);assert.doesNotMatch(s,/bpay_next_[a-z_]+\s*\(/); // Native relations are read; no invented Bank callable reader.
});
test('Result composes current Health placement before and after the genuine unprovided Bank join',()=>{
 const s=text('templates/03_result.sql.tmpl'),call='private.daily_rate_correction_health_current_placement_v1(o.id,p_actor_user_id)';assert.equal(s.split(call).length-1,2);const before=s.indexOf(call),slot=s.indexOf('@@BANK_RESULT@@'),after=s.lastIndexOf(call);assert(before<slot&&slot<after);assert.equal(s.split('@@BANK_RESULT@@').length-1,1);assert.match(s,/if placement is distinct from placement_before then\s+raise exception 'REVIEW_STALE' using errcode='40001'/);assert(s.includes('if retained_binding is distinct from retained_before then'));assert(s.includes("'placement',placement,"));assert(!s.includes('DAILY_RATE_CURRENT_PLACEMENT_READ_UNPROVIDED'));assert(s.includes("'placement_revision','0','receipt_id',null"));
});
test('retained accepted association remains separate and old receipt scalar is never a current selector',()=>{
 const helper=text('health/20_current_placement.sqlinc'),result=text('templates/03_result.sql.tmpl');assert.equal((result.match(/accepted_revision_id/g)||[]).length,2);assert.equal((result.match(/v\.financial_snapshot_id=m\.to_financial_id/g)||[]).length,2);assert.doesNotMatch(helper,/placement_receipt_json->>'placement_revision'/);assert.doesNotMatch(result,/billing_revision::text|placement_receipt_json->>'placement_revision'/);assert(helper.includes("'placement_revision',current_binding->'revision_no'"));
});
for(const c of ast.publicDecoderCases)test('existing exact Daily public Result decoder accepts structural AST '+c.case,()=>{
 const result={version:'DAILY_RATE_CORRECTION_V1',purpose:'DAILY_RATE_CORRECTION',operation_id:'00000000-0000-0000-0000-00000000000b',state:'COMPLETE',state_revision:'3',placement:c.placement,financial:{status:'REGISTERED',command_id:null,cut_id:null,summary:{visibility:'REDACTED'}},billing:{status:'NOT_REQUIRED',revision:'0',group_id:null,visible_summary:null},next_actions:[],links:[]};
 // This tests only the existing decoder with a synthetic redacted envelope.
 // It is not a successful Bank response or a native Result.
 assert.equal(contract.validateResult(result,{financialVisible:false,billingVisible:false}),result);assert.equal(JSON.parse(JSON.stringify(result)).placement.placement_revision,c.placement.placement_revision);assert.deepEqual(Object.keys(c.placement),['status','original_timesheet_id','current_timesheet_id','placement_revision','receipt_id']);
});
test('existing Result decoder still refuses zero committed ordinal and unrelated physical identity',()=>{
 const p=ast.publicDecoderCases[0].placement,make=placement=>({version:'DAILY_RATE_CORRECTION_V1',purpose:'DAILY_RATE_CORRECTION',operation_id:'00000000-0000-0000-0000-00000000000b',state:'COMPLETE',state_revision:'3',placement,financial:{status:'NOT_REQUIRED',command_id:null,cut_id:null,summary:null},billing:{status:'NOT_REQUIRED',revision:'0',group_id:null,visible_summary:null},next_actions:[],links:[]});
 for(const placement of [{...p,placement_revision:'0'},{...p,placement_revision:9007199254740992},{...p,placement_revision:'01'},{...p,current_timesheet_id:'00000000-0000-0000-0000-000000000999'}])assert.throws(()=>contract.validateResult(make(placement)));
});
