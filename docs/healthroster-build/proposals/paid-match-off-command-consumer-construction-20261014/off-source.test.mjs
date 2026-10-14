import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import test from 'node:test';
import {createHash} from 'node:crypto';import {fileURLToPath,pathToFileURL} from 'node:url';import {scanManagedSql} from '../strict-qualification-install1-forward/PINNED_MANAGED_SQL_SCANNER.mjs';
import {functionEvents} from '../a0-current-root-install2-forward/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
const dir=path.dirname(fileURLToPath(import.meta.url)),read=f=>fs.readFileSync(path.join(dir,f),'utf8'),sha=b=>createHash('sha256').update(b).digest('hex');
const proxy=await import(pathToFileURL(path.join(dir,'forward/broker/src/weekly-source/off-rule-native-proxy.mjs')));
const id=n=>`10000000-0000-4000-8000-${String(n).padStart(12,'0')}`;
test('all five complete OFF consumers reverse exactly seven unique hunks',()=>{
 const d=JSON.parse(read('COMPLETE_CONSUMER_REVERSES.json'));assert.equal(d.records.length,5);assert.equal(d.records.reduce((n,r)=>n+r.hunks.length,0),7);
 for(const r of d.records){let a=read('forward/'+r.output);assert.equal(sha(a),r.after_sha256);for(const h of [...r.hunks].reverse()){assert.equal(a.split(h.after).length,2);a=a.replace(h.after,h.before);}assert.equal(a,read('before/'+r.output));assert.equal(sha(a),r.before_sha256);}
});
test('Office operation uses existing SOURCE_FIXTURE_SETUP owner/ABI, no AUTOMATIC extension',()=>{
 for(const f of ['01_complete_off_command.sqlinc','02_complete_off_transport.sqlinc']){const s=read('forward/'+f);assert.match(s,/if op in \('weekly_source_office_recheck_begin_v1',[\s\S]*?then(?: purpose:=|\s+purpose:=)'SOURCE_FIXTURE_SETUP'/);}
 const setup=read('forward/03_complete_off_setup_shape.sqlinc'),source=setup.match(/when 'SOURCE_FIXTURE_SETUP' then array\[([\s\S]*?)\] when 'AUTOMATIC_JOURNEY'/);assert.ok(source&&source[1].includes('weekly_source_office_recheck_begin_v1'));
 assert.ok(proxy.sourceOperations.includes('weekly_source_office_recheck_begin_v1'));assert.ok(!proxy.automaticOperations.includes('weekly_source_office_recheck_begin_v1'));
 assert.match(read('forward/01_complete_off_command.sqlinc'),/when 'weekly_source_office_recheck_begin_v1' then result:=public\.weekly_source_office_recheck_begin_v1\(req\)/);
});
test('command scope gate/OPEN/parent census/native audit stays before actual command',()=>{
 const s=functionEvents(read('forward/01_complete_off_command.sqlinc')).events.find(e=>e.kind==='CREATE'&&e.id==='private.weekly_source_rule_qualification_execute_v1(jsonb)').definition;assert.ok(s.indexOf('pg_try_advisory_xact_lock')<s.indexOf('weekly_source_rule_qualification_scope_assert_v1'));
 assert.ok(s.indexOf('parent_scope:=')<s.indexOf("'WEEKLY_SOURCE_OFF_RULE_COMMAND_ADMITTED'"));assert.ok(s.indexOf("'WEEKLY_SOURCE_OFF_RULE_COMMAND_ADMITTED'")<s.indexOf('result:=public.weekly_source_office_recheck_begin_v1'));
 assert.match(s,/not\(auth->'allowed_operations' @> jsonb_build_array\(op\)\)/);
});
test('actual target is read in parent discovery before frame; existing foreign identities and mixed-cycle refusal retained',()=>{
 const s=read('forward/04_complete_off_parent_scope.sqlinc');assert.match(s,/when 'work_event_id','prior_work_event_id','paid_overlap_target_work_event_id' then/);
 assert.match(s,/into strict parent from public\.weekly_work_events w where w\.id=selector/);
 assert.match(s,/parent->>'source_group_id' is distinct from p_auth->>'source_group_id'/);
 for(const key of ['client_ids','candidate_ids','contract_ids'])assert.ok(s.includes("{fixture_manifest,"+key+"}"));
 assert.match(s,/if qualified then raise; end if/);assert.match(s,/observed_cycle_ids/);
});
test('actual dependency proxy forwards exact native request to existing command owner',async()=>{
 const calls=[],actor=id(1),agency=id(2),cycle=id(3),req={actor_user_id:actor,upload_id:id(4),upload_row_id:id(5),candidate_id:id(6),client_id:id(7),contract_id:id(8),pay_effect:'NONE',paid_overlap_target_work_event_id:id(9),expected_paid_overlap_inputs_sha256:'a'.repeat(64)};
 const dependencies={dataRpc:async(name,args)=>{calls.push({name,args});if(calls.length===1)return{contract:'WEEKLY_SOURCE_OFF_RULE_TEST_ADMISSION_V1',qualified_fixture:true,operational_ready:false,purpose:'SOURCE_FIXTURE_SETUP',source_cycle_id:cycle,actor_user_id:actor,allowed_operations:['weekly_source_office_recheck_begin_v1']};return{status:'STAGED'};}};
 const wrapped=await proxy.createOffRuleNativeDependencies({actorUserId:actor,agencyId:agency,environment:'TEST',sourceCycleId:cycle,purpose:'SOURCE_FIXTURE_SETUP',dependencies});
 await wrapped.dataRpc('weekly_source_office_recheck_begin_v1',{p_request:req});assert.equal(calls[1].name,'weekly_source_group_rule_feature_set_atomic_v1');assert.strictEqual(calls[1].args.p_request.native_request,req);
 assert.equal(calls[1].args.p_request.qualification_action,'OFF_RULE_COMMAND');assert.equal(calls[1].args.p_request.native_operation,'weekly_source_office_recheck_begin_v1');
});
test('qualified proxy refuses operation absent from actual OPEN allowed_operations',async()=>{
 const actor=id(1),cycle=id(3),dependencies={dataRpc:async()=>({contract:'WEEKLY_SOURCE_OFF_RULE_TEST_ADMISSION_V1',qualified_fixture:true,operational_ready:false,purpose:'SOURCE_FIXTURE_SETUP',source_cycle_id:cycle,actor_user_id:actor,allowed_operations:['weekly_source_upload_context_v1']})};
 const wrapped=await proxy.createOffRuleNativeDependencies({actorUserId:actor,agencyId:id(2),environment:'TEST',sourceCycleId:cycle,purpose:'SOURCE_FIXTURE_SETUP',dependencies});
 await assert.rejects(wrapped.dataRpc('weekly_source_office_recheck_begin_v1',{p_request:{}}),/OFF_RULE_PROXY_OPERATION_REFUSED/);
});
test('ordinary no-fixture and LIVE paths retain original dependency identity and payload',async()=>{
 const dependencies={dataRpc:async()=>({contract:'WEEKLY_SOURCE_OFF_RULE_TEST_ADMISSION_V1',qualified_fixture:false,operational_ready:false})},input={actorUserId:id(1),agencyId:id(2),sourceCycleId:id(3),purpose:'SOURCE_FIXTURE_SETUP',dependencies};
 assert.strictEqual(await proxy.createOffRuleNativeDependencies({...input,environment:'TEST'}),dependencies);assert.strictEqual(await proxy.createOffRuleNativeDependencies({...input,environment:'LIVE'}),dependencies);
});
test('SQL leaves retain finite source-only statements with no new private grants or money fixture',()=>{
 for(const f of fs.readdirSync(path.join(dir,'forward')).filter(f=>f.endsWith('.sqlinc'))){const s=read('forward/'+f),p=scanManagedSql(s);assert.ok(p.statements.length);assert.equal(p.meta.length,0);assert.doesNotMatch(s,/set paid_at|insert into public\.pay_settlements|create table/i);}
});
