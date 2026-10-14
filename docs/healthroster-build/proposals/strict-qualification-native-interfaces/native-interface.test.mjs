import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {createOffRuleNativeDependencies,sourceOperations,automaticOperations} from './off-rule-native-proxy.mjs';
import {buildOffRuleTestAuthorization,assertManagedOffRuleTestEvidence} from './off-rule-setup-evidence.mjs';
import {domainHash} from '../protected-source-capability/release-qualification.mjs';
import {buildMeasuredNativePartial} from './measured-partial-evidence.mjs';
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {buildActivationWindowBinding,assertActivationWindowBinding} from './activation-window-binding.mjs';
import {createOffRuleTransportRpc} from './off-rule-transport-rpc.mjs';
import {assertPostVerifiedReceipt} from './post-verified-evidence-controller.mjs';
import {loadOperationalJourneys} from './final-operational-evidence.mjs';
const out=path.dirname(fileURLToPath(import.meta.url)),id=n=>`91301300-0000-4000-8000-${String(n).padStart(12,'0')}`,h='a'.repeat(64);
const scope={actorUserId:id(1),agencyId:id(2),environment:'TEST',sourceCycleId:id(3),purpose:'SOURCE_FIXTURE_SETUP'};
// UNIT VECTORS ONLY: no one of these synthetic strings is an acceptance input.
function vectors(){const context={environment:'TEST',expectedDatabase:'cloudtms_test_clone',mode:'UPGRADE',releaseId:'UNIT_VECTOR_NOT_NATIVE_EVIDENCE',gitCommit:'b'.repeat(40),expectedHash:h};
 const controls={schema_version:'WEEKLY_SOURCE_OFF_RULE_TEST_CONTROLS_V1',release_id:context.releaseId,source_commit:context.gitCommit,contract_sha256:h,
  banking_install_receipt_sha256:h,security_receipt_sha256:h,source_effects_review_sha256:h,banking_first_install_complete:true,effective_acl_pass:true,browser_denied:true,source_effects_no_external_calls:true};
 const input={authorization_id:id(4),scope_id:id(5),agency_id:id(2),source_group_id:id(6),source_cycle_id:id(3),setup_actor_user_id:id(7),journey_actor_user_id:id(1),
  purpose:'SOURCE_FIXTURE_SETUP',week_ending:'2026-10-18',expires_at_utc:'2026-10-23T12:00:00Z',fixture_namespace:'WSQ_TEST_',
  fixture_manifest:{group_code:'WSQ_TEST_INTERFACE',client_ids:[id(8)],contract_ids:[id(9)],candidate_ids:[id(10)],
   client_refs:[{client_id:id(8),cli_ref:'CLI-91301301'}],candidate_refs:[{candidate_id:id(10),tms_ref:'CCR-91301301'}]},
  assignment_id:id(11),assignment_version:'1',assignment_sha256:h,rule_set_version_id:id(12),rule_values_sha256:h,original_source_cas:null,
  allowed_operations:[...sourceOperations].sort(),banking_install_receipt_sha256:h,security_receipt_sha256:h,source_effects_review_sha256:h};
 return {context,controls,input};}
test('managed OFF constructor and revalidator have exactly the same mandatory closure',()=>{
 const {context,controls,input}=vectors();const q=buildOffRuleTestAuthorization(context,input,controls);
 const evidence={weekly_source_off_rule_test_controls:controls,weekly_source_protected_test_setup_authorizations:[q]};
 assert.doesNotThrow(()=>assertManagedOffRuleTestEvidence(context,evidence));
 for(const key of ['banking_first_install_complete','effective_acl_pass','browser_denied','source_effects_no_external_calls']){
  const bad=structuredClone(evidence);bad.weekly_source_off_rule_test_controls[key]=false;
  assert.throws(()=>assertManagedOffRuleTestEvidence(context,bad),/CONTROLS_REQUIRED/);}
 for(const key of ['rule_set_version_id','assignment_version','rule_values_sha256','allowed_operations','fixture_manifest','original_source_cas']){
  const bad=structuredClone(evidence);bad.weekly_source_protected_test_setup_authorizations[0][key]=key==='original_source_cas'?{}:null;
  const row=bad.weekly_source_protected_test_setup_authorizations[0];const {authorization_sha256,...body}=row;
  row.authorization_sha256=domainHash(row.schema_version,body);assert.throws(()=>assertManagedOffRuleTestEvidence(context,bad));}
});
test('an Automatic scope needs exactly one original Client and literal Source CAS; no S18b override accepted',()=>{
 const {context,controls,input}=vectors();input.purpose='AUTOMATIC_JOURNEY';input.allowed_operations=[...automaticOperations].sort();
 assert.throws(()=>buildOffRuleTestAuthorization(context,input,controls),/SOURCE_CAS_INVALID/);
 input.original_source_cas={schema_version:'WEEKLY_SOURCE_BULK_SOURCE_CAS_V1',source_cycle_id:input.source_cycle_id,client_ids:input.fixture_manifest.client_ids,source_cas_sha256:h};
 assert.doesNotThrow(()=>buildOffRuleTestAuthorization(context,input,controls));
 input.equal_protected_hours='CLEAR_AUTOMATICALLY';assert.throws(()=>buildOffRuleTestAuthorization(context,input,controls),/INPUT_INVALID/);
});
test('ordinary and LIVE dependencies are returned unchanged; a claimed malformed admission refuses',async()=>{
 let calls=0;const dependencies={dataRpc:async()=>{calls++;return {contract:'WEEKLY_SOURCE_OFF_RULE_TEST_ADMISSION_V1',qualified_fixture:false,operational_ready:false};}};
 assert.equal(await createOffRuleNativeDependencies({...scope,dependencies}),dependencies);assert.equal(calls,1);
 assert.equal(await createOffRuleNativeDependencies({...scope,environment:'LIVE',dependencies}),dependencies);assert.equal(calls,1);
 dependencies.dataRpc=async()=>({contract:'WEEKLY_SOURCE_OFF_RULE_TEST_ADMISSION_V1',qualified_fixture:true,operational_ready:true});
 await assert.rejects(createOffRuleNativeDependencies({...scope,dependencies}),/ADMISSION_INVALID/);
});
test('qualified commands preserve the full actual native request and trusted server actor; no browser selector is passed',async()=>{
 const calls=[];const dependencies={calculator:{unchanged:true},dataRpc:async(name,args)=>{
  calls.push({name,args});if(calls.length===1)return {contract:'WEEKLY_SOURCE_OFF_RULE_TEST_ADMISSION_V1',qualified_fixture:true,operational_ready:false,
   purpose:scope.purpose,source_cycle_id:scope.sourceCycleId,actor_user_id:scope.actorUserId,allowed_operations:[...sourceOperations]};return {native:true};}};
 const wrapped=await createOffRuleNativeDependencies({...scope,dependencies});const req={actor_user_id:scope.actorUserId,source_cycle_id:scope.sourceCycleId,rows:[{literal:'whole native payload'}]};
 assert.equal(wrapped.calculator,dependencies.calculator);assert.deepEqual(await wrapped.dataRpc('weekly_source_upload_stage_rows_atomic_v1',{p_request:req}),{native:true});
 assert.equal(calls[1].args.p_request.native_request,req);assert.equal(calls[1].args.p_request.actor_user_id,scope.actorUserId);
 assert.deepEqual(Object.keys(calls[1].args.p_request).sort(),['actor_user_id','agency_id','environment','qualification_action','source_cycle_id','native_operation','native_request'].sort());
 await assert.rejects(wrapped.dataRpc('weekly_source_finalise_atomic_v1',{p_request:req,qualification_enabled:true}),/NATIVE_PAYLOAD_INVALID/);
});
test('the native two-parameter reader, Source CAS projection, revision and calendar owners keep the boundary closed',()=>{
 const sql=file=>fs.readFileSync(path.join(out,file),'utf8');
 assert.match(sql('02_effective_rule_owner.sqlinc'),/if auth is null then return null/);
 assert.match(sql('03_cas_operational_projection.sqlinc'),/weekly_source_operational_effective_rule_set_v1\(client,c.id\)/);
 assert.doesNotMatch(sql('03_cas_operational_projection.sqlinc'),/weekly_source_rule_qualification_context_v1/);
 assert.match(sql('04_final_revision_rule_guard.sqlinc'),/if tg_op='UPDATE'/);
 assert.match(sql('04_final_revision_rule_guard.sqlinc'),/WEEKLY_SOURCE_FINAL_RULE_VERSION_IMMUTABLE/);
 assert.match(sql('05_coverage_floor_owner.sqlinc'),/WEEKLY_SOURCE_OFF_RULE_TEST_OPENED/);
 assert.match(sql('OFF_RULE_AUTHORITY.sqlinc'),/if a.effective_from_week_ending<today then\s+perform private.weekly_source_historical_fixture_setup_assert_v1/);
 assert.match(sql('OFF_RULE_AUTHORITY.sqlinc'),/c.finalisation_week_ending<>a.effective_from_week_ending/);
 assert.doesNotMatch(sql('OFF_RULE_AUTHORITY.sqlinc'),/set_config\(|p_now|feature_enabled\s*=\s*true/i);
 assert.match(sql('OFF_RULE_COMMAND.sqlinc'),/case op/);assert.doesNotMatch(sql('OFF_RULE_COMMAND.sqlinc'),/execute\s+(?:format|op)/i);
 const proofs=JSON.parse(sql('OWNER_SOURCE_REVERSE_PROOFS.json'));assert.equal(proofs.edits.length,6);
 assert.ok(proofs.edits.every(row=>row.reverse_hunk_matches_entire_input_definition===true));
});
test('the historical exception retains native assignment guards, real audit and disabled future reference; ordinary activation stays future only',()=>{
 const sql=file=>fs.readFileSync(path.join(out,file),'utf8');
 const assign=sql('11_complete_historical_fixture_native_assignment.sqlinc'),scope=sql('HISTORICAL_FIXTURE_ASSIGNMENT.sqlinc'),setup=sql('HISTORICAL_FIXTURE_SETUP_ASSERT.sqlinc');
 assert.match(assign,/if v_from<\(transaction_timestamp\(\) at time zone 'Europe\/London'\)::date then[\s\S]*?weekly_source_historical_fixture_assignment_assert_v1\(p_request\)/);
 for(const code of ['RULE_GROUP_STALE','RULE_REQUEST_REUSED','RULE_FINALISED_WEEK_REFUSED','RULE_ASSIGNMENT_OVERLAP','RULE_ACTIVE_AGENCY_TRANSITION_REQUIRED'])assert.ok(assign.includes(code),code);
 assert.match(scope,/pg_current_xact_id_if_assigned\(\)::text/);assert.match(scope,/weekly_source_historical_fixture_rule_assign_v1\\\(uuid,jsonb/);
 assert.match(scope,/public.weekly_source_group_rule_assign_atomic_v1\(p_request->'native_request'\)/);
 assert.match(scope,/cycle.scope_client_id=any\(clients\)/);assert.match(scope,/u.file_metadata_json->>'client_id'/);
 assert.match(setup,/completed->'native_receipt'/);assert.match(setup,/assignment.assignment_request_sha256/);
 assert.match(setup,/reference.effective_from_week_ending<today/);assert.match(setup,/not exists\(select 1 from public.audit_events admitted/);
 assert.match(setup,/HISTORICAL_FIXTURE_NOT_FRESH/);assert.match(setup,/cycle.finalisation_week_ending=week/);
 const feature=sql('09_complete_controlled_setup_transport.sqlinc');
 assert.match(feature,/if v_from<\(transaction_timestamp\(\) at time zone 'Europe\/London'\)::date then\s+raise exception 'WEEKLY_SOURCE_RULE_FUTURE_WEEK_REQUIRED'/);
 assert.doesNotMatch(feature,/historical_fixture_assignment_assert/);
 const proof=JSON.parse(sql('HISTORICAL_FIXTURE_REVERSE.json'));assert.equal(proof.proofs.length,3);
 for(const p of proof.proofs){assert.equal(p.reverse_hunk_matches_entire_input_definition,true);
  if(p.file==='OFF_RULE_AUTHORITY.sqlinc'){
   const successor=JSON.parse(sql('CRITIC_NULLABLE_ROOT_REVERSE.json')).proofs.find(row=>row.file===p.file);
   assert.equal(successor.input_sha256,p.output_sha256);assert.equal(createHash('sha256').update(sql(p.file)).digest('hex'),successor.output_sha256);
  }else assert.equal(createHash('sha256').update(sql(p.file)).digest('hex'),p.output_sha256);}
});
test('closed OFF setup still rejects spoofed namespaces, actors, assignments and cycles with resealed hashes',()=>{
 const {context,controls,input}=vectors();input.week_ending='2026-10-04';const result=buildOffRuleTestAuthorization(context,input,controls);
 assert.equal(result.week_ending,'2026-10-04');assert.equal(result.original_source_cas,null);
 for(const patch of [{fixture_namespace:'LIVE_'},{journey_actor_user_id:'missing'},{assignment_sha256:'missing'},
  {assignment_version:'01'},{week_ending:'2026-10-05'},{allowed_operations:['send_email']},{extra:true}]){
  assert.throws(()=>buildOffRuleTestAuthorization(context,{...input,...patch},controls));}
 const bad=structuredClone(input);bad.fixture_manifest.client_refs[0].cli_ref='CLI-1';assert.throws(()=>buildOffRuleTestAuthorization(context,bad,controls));
 const sql=fs.readFileSync(path.join(out,'POST_VERIFIED_SETUP_ASSERT.sqlinc'),'utf8');
 assert.match(sql,/historical_fixture_setup_assert_v1\(p_artifact,p_artifact->>'purpose'='SOURCE_FIXTURE_SETUP'\)/);
 assert.match(sql,/final.rule_set_version_id=v.id/);assert.match(sql,/weekly_source_bulk_source_cas_v1\(c.id,clients\)/);
});
test('transport preserves ordinary RPC results and exceptions; positional native projection remains exact',async()=>{
 const calls=[],ordinaryValue=[{native:'original response envelope'}];
 const base=async(name,args,options)=>{calls.push({name,args,options});if(name==='weekly_source_group_rule_feature_set_atomic_v1')
  return {contract:'WEEKLY_SOURCE_OFF_RULE_TEST_ADMISSION_V1',qualified_fixture:false,operational_ready:false};return ordinaryValue;};
 const rpc=createOffRuleTransportRpc({baseRpc:base,getActor:()=>id(1),agencyId:id(2),environment:'TEST'});
 const args={p_actor_user_id:id(1),p_publication_id:id(20),p_rows:[{source:'unchanged'}]},options={timeoutMs:20000};
 assert.equal(await rpc('weekly_source_projection_rows_apply_atomic_v1',args,options),ordinaryValue);
 assert.equal(calls[0].args.p_request.native_request,args);assert.equal(calls[1].args,args);assert.equal(calls[1].options,options);
 const refusal=Object.assign(Error('native refusal'),{status:409,code:'NATIVE_CAS_REFUSED'});
 await assert.rejects(createOffRuleTransportRpc({baseRpc:async()=>{throw refusal;},getActor:()=>id(1),agencyId:id(2),environment:'TEST'})
  ('weekly_source_finalise_atomic_v1',{p_request:{}}),error=>error===refusal);
 const untouched={arbitrary:true};assert.equal(await rpc('ordinary_native_rpc',untouched,options),ordinaryValue);assert.equal(calls.at(-1).args,untouched);
});
test('all interface-only additions remain owner-only in the complete literal union and Report ABI stays separate',()=>{
 const census=JSON.parse(fs.readFileSync(path.join(out,'INTERFACE_LITERAL_SIGNATURE_UNION.json'))),acl=fs.readFileSync(path.join(out,'OWNER_ONLY_ADDITIONS_ACL.sqlinc'),'utf8');
 assert.equal(census.preserved_all_275,true);assert.ok(census.signatures.includes('private.weekly_source_historical_fixture_setup_assert_v1(jsonb,boolean)'));
 assert.ok(census.signatures.includes('private.weekly_source_historical_fixture_rule_assign_v1(uuid,jsonb)'));
 for(const row of census.additions){assert.ok(acl.includes(`revoke all on function ${row.signature} from PUBLIC,anon,authenticated,service_role,authenticator,supabase_admin;`));
  assert.ok(acl.includes(`grant execute on function ${row.signature} to "postgres";`));}
 assert.doesNotMatch(acl,/grant execute[^;]*to service_role/i);assert.equal(census.report_external_dependencies.length,2);
});

test('direct command and terminal setup serialize before OPEN without acquiring native I1 in reverse order',()=>{
 const sql=fs.readFileSync(path.join(out,'OFF_RULE_COMMAND.sqlinc'),'utf8'),command=sql.slice(sql.indexOf('create or replace function private.weekly_source_rule_qualification_execute_v1'));
 const gate=command.indexOf("pg_try_advisory_xact_lock(hashtextextended('weekly-source-off-rule-test:'"),open=command.indexOf('weekly_source_rule_qualification_scope_assert_v1(auth,actor,false)'),parent=command.indexOf('weekly_source_off_rule_native_parent_scope_v1(req,actor,auth)'),frame=command.indexOf('frame:=jsonb_build_object'),dispatch=command.indexOf('  case op'),completed=command.indexOf("'WEEKLY_SOURCE_OFF_RULE_COMMAND_COMPLETED',frame");
 assert.ok(gate>=0&&gate<open&&open<parent&&parent<frame&&frame<dispatch&&dispatch<completed);
 assert.match(sql,/pg_advisory_xact_lock\(hashtextextended\('weekly-source-off-rule-test:'/);
 assert.doesNotMatch(command,/pg_advisory_unlock|for (?:update|share)|pg_advisory_xact_lock\(/i);
 // Counterexample schedule: native command blocked on a native row while CLOSE
 // tries to commit. The scope gate keeps CLOSE behind the whole transaction.
 const state={held:null,closed:false};
 const begin=owner=>{if(state.held)return false;state.held=owner;return true;};
 assert.equal(begin('command'),true);assert.equal(begin('close'),false);assert.equal(state.closed,false);
 state.held=null;assert.equal(begin('close'),true);state.closed=true;state.held=null;
 assert.equal(begin('command'),true);assert.equal(state.closed,true); // OPEN recheck must refuse before frame.
});

test('actual command resolves all native foreign parent selectors including nested payloads before frame',()=>{
 const sql=fs.readFileSync(path.join(out,'OFF_RULE_NATIVE_PARENT_SCOPE.sqlinc'),'utf8');
 for(const key of ['final_revision_id','run_id','task_id','orchestration_run_id','projection_receipt_id','root_timesheet_id','evidence_timesheet_id','publication_request_id','current_comparison_revision_id','expected_source_revision','c1_request','service_snapshot','p_rows'])assert.ok(sql.includes("'"+key+"'"),key);
 assert.match(sql,/r\.source_cycle_id,'final_revision_id',r\.final_revision_id,'run_id',r\.id/);
 assert.match(sql,/parent->>'run_id' is distinct from \(subject->>'run_id'\)::uuid::text/);
 assert.match(sql,/observed<>expected/);assert.match(sql,/WEEKLY_SOURCE_OFF_RULE_NATIVE_PARENT_FOREIGN/);
 assert.match(sql,/r\.source_cycle_id=expected and t\.root_timesheet_id=root\.timesheet_id/);
 assert.doesNotMatch(sql,/for (?:update|share)|execute\s+format|root\.week_ending_date\s*<>|work_date.*not between/i);
 const admits=(expected,parents)=>parents.every(p=>p.cycle===expected&&p.run===p.declaredRun);
 assert.equal(admits('fixture',[{cycle:'foreign',run:'same-actor-ready',declaredRun:'same-actor-ready'}]),false);
 assert.equal(admits('fixture',[{cycle:'fixture',run:'foreign-task-run',declaredRun:'fixture-run'}]),false);
 assert.equal(admits('fixture',[{cycle:'fixture',run:'fixture-run',declaredRun:'fixture-run'}]),true);
});

test('orchestration provenance uses actual run fingerprint and complete prepare audit while ordinary unstaged status survives',()=>{
 const sql=fs.readFileSync(path.join(out,'OFF_RULE_NATIVE_PARENT_SCOPE.sqlinc'),'utf8'),transport=fs.readFileSync(path.join(out,'OFF_RULE_TRANSPORT_STATUS.sqlinc'),'utf8');
 assert.doesNotMatch(transport,/from public\.weekly_exceptional_orchestration_runs/);
 assert.doesNotMatch(sql,/run_row\.source_cycle_id/);
 for(const domain of ['WEEKLY_PROTECTED_PREPARE_ACTION_REQUEST_V1','WEEKLY_SOURCE_OFF_RULE_NATIVE_REQUEST_V1','WEEKLY_SOURCE_OFF_RULE_NATIVE_RESULT_V1','WEEKLY_SOURCE_OFF_RULE_COMMAND_V1'])assert.ok(sql.includes(domain));
 assert.match(sql,/done\.before_json=admitted\.after_json/);assert.match(sql,/creation_orchestration_run_id=run_row\.id/);
 assert.match(sql,/if qualified then raise; end if/);assert.match(transport,/qualified_fixture',false/);
 assert.match(sql,/'observed_cycle_ids',to_jsonb\(cycles\)/);assert.match(transport,/if \(claim->>'qualified_fixture'\)::boolean then/);
 assert.doesNotMatch(transport,/exception when no_data_found/);
 const discovery=(cycles,ambiguous,knownOff)=>{if(new Set(cycles).size>1||(ambiguous&&cycles.some(c=>knownOff.has(c))))throw Error('SCOPE_CHANGED');return cycles.length===0?'ORDINARY':'STATUS';};
 assert.equal(discovery([],false,new Set()),'ORDINARY');assert.equal(discovery([],true,new Set()),'ORDINARY');
 assert.throws(()=>discovery(['fixture'],true,new Set(['fixture'])),/SCOPE_CHANGED/);
 assert.throws(()=>discovery(['fixture','foreign'],true,new Set(['fixture'])),/SCOPE_CHANGED/);
 const census=JSON.parse(fs.readFileSync(path.join(out,'NATIVE_CONSUMER_AND_EXECUTOR_CENSUS.json')));
 assert.ok(census.source_executor_chain.some(row=>row.signature==='public.weekly_exceptional_pay_action_publication_status_v1(jsonb)'));
});

test('captured physical NULL and signed evidence roots outside pay receipts are rechecked without recollecting original items',()=>{
 const sql=fs.readFileSync(path.join(out,'13_complete_nullable_physical_qualification_scope.sqlinc'),'utf8'),authority=fs.readFileSync(path.join(out,'OFF_RULE_AUTHORITY.sqlinc'),'utf8'),bulk=fs.readFileSync(path.join(out,'10_complete_bulk_test_scope.sqlinc'),'utf8');
 assert.equal((sql.match(/'candidate_id',contract\.candidate_id,'client_id',sheet\.client_id\)/g)||[]).length,2);
 assert.match(authority,/lease#>'\{original_census,original_root_identities\}'/);assert.match(authority,/actual is distinct from item/);
 assert.match(bulk,/'client_id',s\.client_id\) into actual/);
 const original={timesheet_id:id(31),booking_id:'SIGNED_OUTSIDE_SOURCE_PAY_RECEIPTS',contract_id:id(9),candidate_id:id(10),client_id:null};
 assert.notDeepEqual({...original,client_id:id(8)},original);assert.notDeepEqual({...original,booking_id:'CHANGED'},original);
 const proof=JSON.parse(fs.readFileSync(path.join(out,'CRITIC_NULLABLE_ROOT_REVERSE.json')));assert.equal(proof.original_source39_and_captured_once_items_preserved,true);
});

test('unchanged actual release caller can omit operational descriptor and shared Report bundle retains closed WITH-context schema',()=>{
 const {context}=vectors();assert.deepEqual(loadOperationalJourneys(path.resolve(out,'../../../..'),{},context),[]);
 const sql=fs.readFileSync(path.join(out,'final-operational-evidence.mjs'),'utf8'),controller=fs.readFileSync(path.join(out,'post-verified-evidence-controller.mjs'),'utf8');
 assert.match(sql,/context\.receiptInputCommit\?\?context\.gitCommit/);assert.doesNotMatch(sql,/!bundle\.qualifications\.length/);
 assert.match(sql,/exact\(row,\['context','native_context','native_gate_inputs','remote_gate_inputs','setup_closure','activation_scope'\]/);
 assert.match(controller,/\['context','native_context','native_gate_inputs','remote_gate_inputs','setup_closure','activation_scope'\]/);
 assert.match(sql,/const installation=row\.context/);assert.match(sql,/installation\.git_commit!==context\.gitCommit/);
});
test('nullable B clones only actual last Report row/window and preserves every ordinary calculator/identity guard',()=>{
 const source=fs.readFileSync(path.join(out,'12_complete_report_nullable_rounding_owner.sqlinc'),'utf8');
 const proof=JSON.parse(fs.readFileSync(path.join(out,'REPORT_ROUNDING_ORIGIN_REVERSE.json')));
 assert.equal(proof.reverse_hunks_match_entire_original_definition,true);assert.equal(proof.no_new_signature_table_grant_or_selector,true);
 assert.equal(createHash('sha256').update(source).digest('hex'),proof.output_definition_sha256);
 assert.match(source,/revision.authority_scope_kind='SELF_BILL_REPORT_SCOPE' and profile='SELF_BILL_REPORT'/);
 assert.match(source,/identity_row.source_line_kind='SELF_BILL_REPORT_LINE'/);
 for(const field of ['self_bill_report_id','report_scope_id','upload_row_id','source_row_ordinal','rule_set_version_id'])
  assert.ok(source.includes("identity_row.source_facts_json->>'"+field+"'"),field);
 assert.match(source,/report_upload.report_scope_id is distinct from report_scope.id/);
 assert.match(source,/report_upload.content_sha256 is distinct from report.content_sha256/);
 assert.match(source,/p_invoice_window_cycle_id<>revision.source_cycle_id/);
 assert.match(source,/later.source_row_ordinal>report_row.source_row_ordinal/);
 assert.match(source,/identity_row.canonical_pay_vector_json is distinct from private.weekly_source_invoice_only_vector_v1\('PAY',0\)/);
 assert.match(source,/else\s+if identity_row.final_revision_id<>revision.id or identity_row.actual_client_id<>p_client_id\s+or identity_row.source_line_kind='INVOICE_VAT_ROUNDING' or identity_row.candidate_id is null\s+or identity_row.contract_id is null or identity_row.work_event_id is null or identity_row.invoice_timesheet_id is null then/);
 assert.doesNotMatch(source,/created_at_utc\s*=|coalesce\(identity_row\.(?:candidate_id|contract_id|work_event_id|invoice_timesheet_id)/i);
});
test('new Report factory guards use the exact real schema rather than an invented Report upload column',()=>{
 const own=path.resolve(out,'../../../..'),schema=execFileSync('git',['show','2151af65:supabase/migrations/13102026_1344_weekly_source_self_bill_report_schema.sql'],{cwd:own}).toString('utf8');
 const declaration=schema.match(/create table public\.weekly_source_self_bill_reports \(([\s\S]*?)\n\);/)[1];
 const columns=new Set([...declaration.matchAll(/^\s{2}([a-z_0-9]+)\s+(?:uuid|text|date|bytea|integer|bigint|timestamptz)\b/gm)].map(row=>row[1]));
 const source=fs.readFileSync(path.join(out,'12_complete_report_nullable_rounding_owner.sqlinc'),'utf8');
 for(const field of new Set([...source.matchAll(/\breport\.([a-z_0-9]+)/g)].map(row=>row[1])))assert.ok(columns.has(field),'ACTUAL_REPORT_COLUMN '+field);
 assert.ok(columns.has('source_cycle_id'));assert.equal(columns.has('upload_id'),false);
});
test('a real-shaped non-NEXT partial does not manufacture NEXT or an operational certificate',()=>{
 const context={release_id:'UNIT_VECTOR_NOT_NATIVE_EVIDENCE',git_commit:'b'.repeat(40),contract_sha256:h,agency_id:id(2),environment:'TEST',
  database:'cloudtms_test_clone',source_group_id:id(6),effective_from_week_ending:'2026-10-18',effective_to_week_ending:null,capabilities:['BULK_SIGNED']};
 const row={schema_version:'WEEKLY_SOURCE_NATIVE_JOURNEY_RECEIPT_V1',gate:'OWN_STAGE_CAS',release_id:context.release_id,source_commit:context.git_commit,
  contract_sha256:h,agency_id:context.agency_id,environment:'TEST',database:'cloudtms_test_clone',source_group_id:context.source_group_id,
  effective_from_week_ending:context.effective_from_week_ending,effective_to_week_ending:null,execution:'AGENCY_TEST_NATIVE',
  native_execution_receipt:{accepted:true,database:'cloudtms_test_clone',rolledBack:true,connectedFresh:true,tool_receipt_sha256:h,native_journey_id:'PROTECTED_CAPABILITY_OWN_STAGE_CAS'},
  assertions:{own_stage_excluded:true,foreign_stage_not_excluded:true,completed_client_retained:true}};
 // Use the literal original assertion census, not guessed assertion names.
 return import('../protected-source-capability/release-qualification.mjs').then(({gates})=>{
  row.assertions=Object.fromEntries(gates.OWN_STAGE_CAS.map(key=>[key,true]));
  const bytes=Buffer.from(JSON.stringify(row)),digest=createHash('sha256').update(bytes).digest('hex');
  const q=buildMeasuredNativePartial(context,[{gate:'OWN_STAGE_CAS',receipt_id:'UNIT_VECTOR_NOT_NATIVE_EVIDENCE',receipt_bytes:bytes,expected_receipt_sha256:digest}]);
  assert.equal(q.operational_ready,false);assert.equal(q.phase,'MEASURED_PARTIAL_NON_OPERATIONAL');assert.ok(q.held_gates.includes('NEXT_PAY'));
  assert.equal(q.gate_receipts.length,1);assert.doesNotMatch(q.schema_version,/OPERATIONAL_QUALIFICATION/);
 });
});
test('same installed evidence record preserves every release identity field and refuses replacing evidence',()=>{
 const sql=fs.readFileSync(path.join(out,'POST_VERIFIED_EVIDENCE_RECORD.sqlinc'),'utf8');
 assert.match(sql,/update private.cloudtms_database_releases as release_row set evidence_json=next_evidence/);
 assert.doesNotMatch(sql,/set status=|set git_commit=|insert into private.cloudtms_(?:migration|repeatable)_ledger|create table/i);
 assert.match(sql,/identity_after is distinct from identity_before/);assert.match(sql,/POST_VERIFIED_REPLACEMENT_REFUSED/);
 assert.match(sql,/POST_VERIFIED_AMBIGUOUS_SCOPE_REFUSED/);assert.match(sql,/lock_row.database=/);
 assert.match(sql,/cloudtms_database_release_admission_v1/);assert.match(sql,/lock_row.classid=20261013::oid/);
 const forward=JSON.parse(fs.readFileSync(path.join(out,'SAME_INSTALLED_CERTIFICATE_REVERSE.json')));
 assert.equal(forward.proofs.length,2);assert.ok(forward.proofs.every(p=>p.reverse_hunk_matches_entire_input_definition));
 const cert=fs.readFileSync(path.join(out,'SAME_INSTALLED_OPERATIONAL_CERTIFICATE.sqlinc'),'utf8');
 assert.doesNotMatch(cert,/executed.release_id=r.release_id or/);assert.match(cert,/executed.status<>'VERIFIED'/);
 assert.match(cert,/ALL_PRE_SWITCH_GATES_QUALIFIED/);assert.match(cert,/PHYSICAL_TESTFLIGHT_MYTMS_PHONE/);
});
test('historical fixture dates remain literal while future activation binds the same immutable rule and original census',()=>{
 const native={source_group_id:id(6),effective_from_week_ending:'2026-10-18',effective_to_week_ending:'2026-10-18',capabilities:['BULK_SIGNED','AUTOMATIC_RECONCILE']};
 const current={...native,effective_from_week_ending:'2026-10-25',effective_to_week_ending:null};
 const closure={scope_id:id(5),scope_sha256:h,original_census_sha256:h,source_cycle_id:id(3)};
 const input={assignment_id:id(11),assignment_version:'1',assignment_sha256:h,source_group_id:id(6),feature_purpose:'RULE_SET',
  rule_set_version_id:id(12),rule_values_sha256:h,effective_from_week_ending:'2026-10-25',effective_to_week_ending:null,
  native_effective_from_week_ending:'2026-10-18',native_effective_to_week_ending:'2026-10-18',fixture_cycle_id:id(3),
  fixture_rule_set_version_id:id(12),fixture_rule_values_sha256:h,scope_id:id(5),scope_sha256:h,original_census_sha256:h};
 const binding=buildActivationWindowBinding(input,current,native,closure);assert.equal(binding.native_effective_from_week_ending,'2026-10-18');
 for(const patch of [{fixture_rule_set_version_id:id(19)},{fixture_rule_values_sha256:'b'.repeat(64)},
  {fixture_cycle_id:id(19)},{original_census_sha256:'b'.repeat(64)},{native_effective_from_week_ending:'2026-10-25'},
  {effective_from_week_ending:'2026-10-11'},{assignment_version:'01'}]){
  const changed={...binding,...patch};const {scope_binding_sha256,...body}=changed;changed.scope_binding_sha256=domainHash(changed.schema_version,body);
  assert.throws(()=>assertActivationWindowBinding(changed,current,native,closure),/WINDOW_BINDING_INVALID/);
 }
});
test('actual nested transport uses server actor, exact admission, unchanged native payload/options and unwraps only its own envelope',async()=>{
 const calls=[],req={actor_user_id:id(1),upload_id:id(20),rows:[{literal:'whole native rows'}]},options={timeoutMs:120000};
 const baseRpc=async(name,args,opts)=>{calls.push({name,args,opts});return [{weekly_source_group_rule_feature_set_atomic_v1:calls.length===1
  ?{contract:'WEEKLY_SOURCE_OFF_RULE_TEST_ADMISSION_V1',qualified_fixture:true,operational_ready:false,purpose:'SOURCE_FIXTURE_SETUP',
   source_cycle_id:id(3),actor_user_id:id(1),allowed_operations:[...sourceOperations],scope_id:id(5),authorization_sha256:h}
  :{native:true,unchanged_result:'whole native result'}}];};
 const rpc=createOffRuleTransportRpc({baseRpc,getActor:()=>id(1),agencyId:id(2),environment:'TEST'});
 assert.deepEqual(await rpc('weekly_source_upload_stage_rows_atomic_v1',{p_request:req},options),{native:true,unchanged_result:'whole native result'});
 assert.equal(calls[1].args.p_request.native_request,req);assert.equal(calls[1].opts,options);
 await assert.rejects(rpc('weekly_source_finalise_atomic_v1',{p_request:{actor_user_id:id(19)}}),/NATIVE_PAYLOAD_INVALID/);
 const ordinary=async()=>({contract:'WEEKLY_SOURCE_OFF_RULE_TEST_ADMISSION_V1',qualified_fixture:false,operational_ready:false,hidden_ready:true});
 await assert.rejects(createOffRuleTransportRpc({baseRpc:ordinary,getActor:()=>id(1),agencyId:id(2),environment:'TEST'})('weekly_source_finalise_atomic_v1',{p_request:{}}),/ADMISSION_INVALID/);
 assert.equal(createOffRuleTransportRpc({baseRpc,getActor:()=>null,agencyId:id(2),environment:'LIVE'}),baseRpc);
});
test('post-VERIFIED receipt has the exact thirteen fields/domains and cannot carry changed operational settings or a relabelled execution',()=>{
 const execution={release_id:'UNIT_VECTOR_NOT_NATIVE_EVIDENCE',source_commit:'b'.repeat(40)};
 const request={expected_execution:execution,receipt_input_commit:'c'.repeat(40),receipt_tree_proof:{source_equivalence_sha256:h},
  artifact_slot:'weekly_source_protected_partial_native_evidence',artifacts:[{unit_only:true}],expected_evidence_sha256:h};
 const body={schema_version:'WEEKLY_SOURCE_POST_VERIFIED_EVIDENCE_RECEIPT_V1',execution,receipt_input_commit:request.receipt_input_commit,
  receipt_tree_sha256:h,artifact_slot:request.artifact_slot,artifacts_sha256:domainHash('WEEKLY_SOURCE_POST_VERIFIED_ARTIFACT_SET_V1',request.artifacts),
  prior_evidence_sha256:h,evidence_sha256:'d'.repeat(64),actor_user_id:id(1),recorded_at_utc:'2026-10-14T12:00:00Z',replayed:false,operational_settings_changed:false};
 const receipt={...body,receipt_sha256:domainHash(body.schema_version,body)};assert.equal(Object.keys(receipt).length,13);
 assert.equal(assertPostVerifiedReceipt(receipt,request,id(1)),receipt);
 for(const patch of [{operational_settings_changed:true},{receipt_input_commit:'b'.repeat(40)},{artifacts_sha256:h},{unknown_key:true}]){
  const changed={...receipt,...patch};const {receipt_sha256,...data}=changed;changed.receipt_sha256=domainHash(changed.schema_version,data);
  assert.throws(()=>assertPostVerifiedReceipt(changed,request,id(1)),/RECEIPT_INVALID/);
 }
 const closure=JSON.parse(fs.readFileSync(path.join(out,'OFF_CLOSURE_REVERSE.json')));assert.equal(closure.reverse_hunk_matches_entire_input_definition,true);
 const transport=JSON.parse(fs.readFileSync(path.join(out,'BROKER_TRANSPORT_REVERSE.json')));assert.equal(transport.reverse_hunk_matches_entire_input_definition,true);
 assert.equal(transport.native_recording_rpc_callers,5);assert.equal(transport.unchanged_calculator_and_c1_raw_dependencies,true);
});
