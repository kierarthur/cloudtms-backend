import fs from 'node:fs';
import crypto from 'node:crypto';
import test from 'node:test';
import assert from 'node:assert/strict';
import {originalClients,remainingClients,sourceVectorHash,projectFinancialIdentity} from './cas-vectors.mjs';
import {buildQualification,gates,sha256,pgJsonbText,domainHash,assertManagedQualificationEvidence} from './release-qualification.mjs';
const dir=new URL('./',import.meta.url),read=name=>fs.readFileSync(new URL(name,dir),'utf8');
const a='11111111-1111-4111-8111-111111111111',b='22222222-2222-4222-8222-222222222222',c='33333333-3333-4333-8333-333333333333';
// All qualification objects below are explicitly synthetic unit fixtures.
// They are never exported to an install/activation setup manifest.
const ctx={release_id:'SYNTHETIC_UNIT_ONLY',git_commit:'a'.repeat(40),contract_sha256:'b'.repeat(64),agency_id:a,environment:'TEST',database:'cloudtms_test_clone',source_group_id:b,effective_from_week_ending:'2026-10-18',effective_to_week_ending:null,capabilities:['BULK_SIGNED']};
function nativeInputs(context=ctx){return Object.keys(gates).filter(k=>k!=='AUTOMATIC_COMPONENT_DELTA'||context.capabilities.includes('AUTOMATIC_RECONCILE')).map(gate=>{
 const value={schema_version:'WEEKLY_SOURCE_NATIVE_JOURNEY_RECEIPT_V1',gate,release_id:context.release_id,source_commit:context.git_commit,contract_sha256:context.contract_sha256,
  agency_id:context.agency_id,environment:context.environment,database:context.database,source_group_id:context.source_group_id,effective_from_week_ending:context.effective_from_week_ending,
  effective_to_week_ending:context.effective_to_week_ending,execution:'AGENCY_TEST_NATIVE',native_execution_receipt:{accepted:true,database:context.database,rolledBack:true,connectedFresh:true,tool_receipt_sha256:'c'.repeat(64),native_journey_id:'PROTECTED_CAPABILITY_'+gate},assertions:Object.fromEntries(gates[gate].map(x=>[x,true]))};
 const receipt_bytes=JSON.stringify(value);return {gate,receipt_id:'SYNTHETIC/'+gate,receipt_bytes,expected_receipt_sha256:sha256(receipt_bytes)};
 });}
const mutate=(inputs,fn)=>{const x=structuredClone(inputs),r=JSON.parse(x[0].receipt_bytes);fn(r);x[0].receipt_bytes=JSON.stringify(r);x[0].expected_receipt_sha256=sha256(x[0].receipt_bytes);return x;};
test('original two-client census survives first complete, both complete and no OPEN questions',()=>{
 assert.deepEqual(originalClients([b,a,b]),[a,b]);assert.deepEqual(remainingClients([a,b],[b]),[a,b]);assert.deepEqual(remainingClients([a,b],[]),[a,b]);
 assert.throws(()=>remainingClients([a,b],[c]),/CHANGED/);assert.throws(()=>originalClients([null]),/INVALID/);
});
test('completed original client policy drift changes Source hash while legitimate protection completion does not',()=>{
 const facts={client_ids:[a,b],policies:[{client_id:a,authority_mode:'SOURCE_AUTHORITY'},{client_id:b,authority_mode:'SOURCE_AUTHORITY'}],pay_receipts:[{client_id:a,receipt_hash:'11'}]};
 const initial=sourceVectorHash(facts),changed=structuredClone(facts);changed.policies[0].authority_mode='TIMESHEET_AUTHORITY';assert.notEqual(sourceVectorHash(changed),initial);
 const projection=projectFinancialIdentity({id:c,timesheet_id:a,client_id:b,candidate_id:a,contract_id:c,is_current:true,pay_status:'UNPAID'});
 assert.deepEqual(projection,projectFinancialIdentity({id:c,timesheet_id:a,client_id:b,candidate_id:a,contract_id:c,is_current:false,pay_status:'PAID'}));
 assert.notDeepEqual(projection,projectFinancialIdentity({id:c,timesheet_id:a,client_id:a,candidate_id:a,contract_id:c}));
});
test('every independently named Source fact category appears once in the actual JSON builder',()=>{
 const inv=JSON.parse(read('SOURCE_INVENTORY.json')),sql=read('SOURCE_CAS.sqlinc'),builder=sql.slice(sql.indexOf("select jsonb_build_object('schema_version'"),sql.indexOf(' into facts;'));
 const actual=[];let depth=0;
 for(let i=0;i<builder.length;i++){
  if(builder[i]==="'"){let text='';while(++i<builder.length){if(builder[i]==="'"&&builder[i+1]==="'"){text+="'";i++;}else if(builder[i]==="'")break;else text+=builder[i];}
   if(depth===1&&/^[a-z_]+$/.test(text))actual.push(text);
  }else if(builder[i]==='(')depth++;else if(builder[i]===')')depth--;
 }
 assert.deepEqual([...new Set(actual)].sort(),[...inv.facts_categories].sort());
 for(const item of inv.relations)assert.ok(sql.includes(item.name),item.name);
 const refs=[...new Set([...sql.matchAll(/public\.([a-z_]+)/g)].map(m=>'public.'+m[1]))].sort();assert.deepEqual(refs,inv.relations.map(x=>x.name).sort());
});
test('Source CAS excludes every protection table and mutable lifecycle; no locking/writer/capability decision',()=>{
 const sql=read('SOURCE_CAS.sqlinc').replace(/--[^\n]*/g,'');
 for(const table of ['weekly_exceptional_c1_publication_requests','weekly_exceptional_pay_generations','weekly_protected_hours_events','weekly_exceptional_orchestration_runs','weekly_source_target_families'])assert.ok(!sql.includes(table));
 assert.ok(!/for\s+(update|share)|pg_advisory|\b(insert into|update public|delete from)\b/i.test(sql));
 assert.ok(sql.includes('incident.work_event_id=event.id'));assert.ok(!sql.includes("incident.state='OPEN'"));assert.ok(!sql.includes('financial.is_current'));
});
test('qualification byte hash and exact native assertion census',()=>{
 const q=buildQualification(ctx,nativeInputs());assert.equal(q.gate_receipts.length,9);const {qualification_sha256,...body}=q;
 assert.equal(qualification_sha256,domainHash('WEEKLY_SOURCE_PROTECTED_CAPABILITY_QUALIFICATION_V1',body));
 const inputs=nativeInputs();inputs[0].expected_receipt_sha256='0'.repeat(64);assert.throws(()=>buildQualification(ctx,inputs),/ARTIFACT/);
 assert.throws(()=>buildQualification(ctx,mutate(nativeInputs(),r=>delete r.assertions.sorted_original_clients)),/CENSUS/);
 assert.throws(()=>buildQualification(ctx,mutate(nativeInputs(),r=>r.assertions.sorted_original_clients=false)),/FAILED/);
});
test('mere presence, definitions compile, local PASS, rollback absence or foreign scope never qualifies',()=>{
 for(const change of [r=>r.execution='SOURCE_FUNCTIONS_PRESENT',r=>r.execution='DEFINITIONS_ACL_COMPILE',r=>r.native_execution_receipt.connectedFresh=false,
   r=>r.native_execution_receipt.rolledBack=false,r=>r.native_execution_receipt.native_journey_id='COMPILE_ONLY',r=>r.source_group_id=c,r=>r.source_commit='d'.repeat(40)])
  assert.throws(()=>buildQualification(ctx,mutate(nativeInputs(),change)),/QUALIFICATION/);
 assert.throws(()=>buildQualification({...ctx,environment:'LIVE'},nativeInputs()),/CONTEXT/);
});
test('automatic readiness needs both capabilities and extra actual component proof',()=>{
 const auto={...ctx,capabilities:['BULK_SIGNED','AUTOMATIC_RECONCILE']};assert.equal(buildQualification(auto,nativeInputs(auto)).gate_receipts.length,10);
 assert.throws(()=>buildQualification(auto,nativeInputs()),/CENSUS/);assert.throws(()=>buildQualification({...ctx,capabilities:['AUTOMATIC_RECONCILE']},nativeInputs()),/CONTEXT/);
 const sql=read('CAPABILITY_OWNERS.sqlinc');assert.ok(sql.includes("'CLIENT_SELF_BILL'"));assert.ok(sql.includes("'CLEAR_AUTOMATICALLY'"));assert.ok(sql.includes("order by started_at_utc desc,release_id desc"));
});
test('PG JSONB text uses exact domain delimiter and key ordering for qualification strings',()=>{
 assert.equal(pgJsonbText({aa:'x',b:'y',ccc:null,a:true}),'{'+'"a": true, "b": "y", "aa": "x", "ccc": null}');
 assert.equal(domainHash('x',{a:'b'}),crypto.createHash('sha256').update('x\x1f{"a": "b"}').digest('hex'));
 assert.throws(()=>pgJsonbText({amount:1.2}),/NUMERIC/);
});
test('full activation owner preserves native agency Client transition and narrows metadata-only requalification',()=>{
 const sql=read('generated/05_complete_existing_activation.sqlinc');
 assert.ok(sql.includes("'PROTECTED_SOURCE'"));assert.ok(sql.includes('public.weekly_source_client_settings_save_atomic_v1'));
 assert.ok(sql.includes('WEEKLY_SOURCE_RULE_CLIENT_RESTORE_STALE'));assert.ok(sql.includes('WEEKLY_SOURCE_RULE_FINALISED_WEEK_REFUSED'));
 assert.ok(sql.includes('v_assignment.feature_receipt_json||jsonb_build_object'));
 const requal=sql.slice(sql.indexOf('if v_assignment.feature_enabled=v_enabled then'),sql.indexOf('select v.values_json'));
 assert.ok(!requal.includes('client_settings_save_atomic'));assert.ok(!requal.includes('set feature_enabled='));
});
test('every live round/stage call uses the original trusted census while PREVIEW/ADMIT stay unchanged',()=>{
 const sql=read('generated/08_complete_bulk_command.sqlinc');
 assert.equal(sql.split('selection_v1(v_cycle_id,v_actor,v_context,v_original_clients)').length-1,2);
 assert.equal(sql.split('selection_v1(v_cycle_id,v_actor,null)').length-1,2);
 assert.ok(read('generated/07_complete_stage_proof.sqlinc').includes('v_context,v_original_clients)'));
 assert.ok(read('ORIGINAL_CLIENTS.sqlinc').includes('event_sequence_is_authoritative'));
});
test('generated whole owners contain no psql directives or dollar expansion residues',()=>{
 for(const file of fs.readdirSync(new URL('generated/',dir)).filter(x=>x.endsWith('.sqlinc'))){const sql=read('generated/'+file);assert.ok(!/^\\(endif|if|else|set|ir|quit)/m.test(sql),file);}
 const generator=read('generate-complete-owners.mjs');assert.ok(generator.includes('body.replaceAll(anchor,()=>value)'));
 const value="select '$&', '$`', '$\u0027', '$1';";assert.equal('A before Z'.replaceAll('before',()=>value),'A '+value+' Z');
});
test('managed evidence preserves old absence and pins exact release/hash before qualification publication',()=>{
 const context={releaseId:ctx.release_id,gitCommit:ctx.git_commit,expectedHash:ctx.contract_sha256,environment:'TEST',expectedDatabase:'cloudtms_test_clone'};
 assertManagedQualificationEvidence(context,{});const q=buildQualification(ctx,nativeInputs());assertManagedQualificationEvidence(context,{weekly_source_protected_capability_qualifications:[q]});
 assert.throws(()=>assertManagedQualificationEvidence({...context,releaseId:'foreign'},{weekly_source_protected_capability_qualifications:[q]}),/INVALID/);
 assert.throws(()=>assertManagedQualificationEvidence(context,{weekly_source_protected_capability_qualifications:[q,q]}),/DUPLICATE/);
});
test('four non-self-bill Source owner successors preserve every original native call and mutation target',()=>{
 const manifest=JSON.parse(read('generated/SOURCE_MANIFEST.json'));
 const cases=[['17_prepare_family.sqlinc','weekly_exceptional_pay_prepare_family_v1'],['18_prepare_action.sqlinc','weekly_exceptional_pay_prepare_action_v1'],
  ['19_action_context.sqlinc','weekly_exceptional_pay_action_context_v1'],['20_stage_request.sqlinc','weekly_exceptional_pay_stage_c1_request_v1']];
 const calls=sql=>[...sql.matchAll(/\b(private|public)\.([a-z_0-9]+)\s*\(/g)].map(m=>m[1]+'.'+m[2]).filter(x=>x!=='private.weekly_source_protected_non_self_bill_mode_v1').sort();
 const mutations=sql=>[...sql.matchAll(/\b(?:insert\s+into|update|delete\s+from)\s+((?:public|private)\.[a-z_0-9]+)/gi)].map(m=>m[1]).sort();
 for(const [file,name] of cases){
  const input=manifest.inputs.find(x=>x.file.endsWith('/'+file)),original=fs.readFileSync(input.file,'utf8').replaceAll('\r\n','\n');
  const start=original.indexOf('create or replace function public.'+name+'('),body=original.slice(start,original.indexOf('$function$;',start)+11);
  const successor=read('generated/12_complete_'+file);assert.deepEqual(calls(successor),calls(body),name);assert.deepEqual(mutations(successor),mutations(body),name);
  assert.ok(successor.includes('v_activated_source_mode is null'));assert.ok(successor.includes("coalesce(v_policy->>'c1_source_mode',v_activated_source_mode)"));
 }
});
test('bounded non-self-bill mode is actual policy/group/date/declared-client/activation scope and never rewrites policy',()=>{
 const sql=read('NONSELFBILL_ADMISSION.sqlinc');
 assert.ok(sql.includes("policy->'self_bill_enabled' is distinct from 'false'::jsonb"));assert.ok(sql.includes('weekly_source_is_import_authoritative_v1'));
 assert.ok(sql.includes('weekly_source_protected_capability_receipt_v1'));assert.ok(sql.includes('weekly_source_checking_client_scope_v1'));
 assert.ok(!/\b(update|insert\s+into|delete\s+from)\s+(public|private)\./i.test(sql));
});
