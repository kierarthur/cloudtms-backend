import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {fileURLToPath} from 'node:url';
const out=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(process.argv[2]??'../healthroster-v2-backend-13102026');
const cap=path.resolve(out,'../protected-source-capability'),q=path.resolve(out,'../protected-source-qualification');
const sha=bytes=>crypto.createHash('sha256').update(bytes).digest('hex'),inputs=[],proofs=[];
const read=file=>{const bytes=fs.readFileSync(file);inputs.push({path:path.relative(process.cwd(),file).replaceAll('\\','/'),sha256:sha(bytes),bytes:bytes.length});return bytes.toString('utf8').replaceAll('\r\n','\n');};
const emit=(file,body)=>fs.writeFileSync(path.join(out,file),body.trimEnd()+'\n');
const replace=(body,anchor,delta)=>{if(body.split(anchor).length!==2)throw Error('QUALIFICATION_FORWARD_ANCHOR_CHANGED');return body.replaceAll(anchor,()=>delta);};
const get=(body,name)=>{const rows=[...body.matchAll(/create or replace function\s+([\w.]+)\s*\([\s\S]*?\bas\s+(\$[\w]*\$)[\s\S]*?\2\s*;/gi)].filter(row=>row[1]===name);if(rows.length!==1)throw Error('QUALIFICATION_FORWARD_OWNER_COUNT '+name);return rows[0][0];};
const proof=(file,original,result,hunks)=>{
 let reverse=result;for(const [anchor,delta] of [...hunks].reverse())reverse=replace(reverse,delta,anchor);
 if(reverse!==original)throw Error('QUALIFICATION_FORWARD_REVERSE_FAILED');
 if(file.endsWith('.sqlinc')&&/\\(?:if|endif)\b/.test(result))throw Error('QUALIFICATION_FORWARD_BODY_CORRUPT');
 emit(file,result);proofs.push({path:file,input_sha256:sha(original),output_sha256:sha(result),
  reverse_hunk_matches_entire_input_definition:true,hunks:hunks.map(([before,after])=>({before,after}))});
};
let scope=read(path.join(q,'QUALIFICATION_SCOPE.sqlinc')),scopeOriginal=scope;
const scopeAnchor="and lock_row.mode='ExclusiveLock' and lock_row.classid=20261013::oid and lock_row.objid=1::oid and lock_row.objsubid=2";
const scopeDelta=scopeAnchor+"\n      and lock_row.database=(select oid from pg_database where datname=current_database())";
if(scope.split(scopeAnchor).length!==3)throw Error('QUALIFICATION_TURN_PREDICATE_COUNT');
scope=scope.replaceAll(scopeAnchor,()=>scopeDelta);
emit('01_complete_qualification_scope.sqlinc',scope);
proofs.push({path:'01_complete_qualification_scope.sqlinc',input_sha256:sha(scopeOriginal),output_sha256:sha(scope),
 reverse_hunk_matches_entire_input_definition:scope.replaceAll(scopeDelta,()=>scopeAnchor)===scopeOriginal,
 delta:'Both advisory lock predicates additionally bind the current database OID. Existing root-owned turn PID binding retained.'});
let prereq=read(path.join(q,'prerequisite-evidence.mjs'));
const first=prereq.indexOf('// Independent literal source inventory boundaries'),last=prereq.indexOf('// Honest prerequisite assertions.');
if(first<0||last<=first)throw Error('QUALIFICATION_SOURCE_INVENTORY_ANCHOR');
prereq=prereq.slice(0,first)+"// Complete literal execution tree inventory (mode/type/workflow/release/verifiers).\nexport { proveCommittedSources } from './source-equivalence.mjs';\n"+prereq.slice(last);
prereq=replace(prereq,"import path from 'node:path';","import path from 'node:path';\nimport {proveCommittedSources} from './source-equivalence.mjs';");
prereq=prereq.replaceAll("['schema_version','execution_source_commit','installation_source_commit','files','source_equivalence_sha256']",
 ()=>"['schema_version','execution_source_commit','installation_source_commit','inventory_schema','files','receipt_bindings','source_equivalence_sha256']");
prereq=prereq.replaceAll('WEEKLY_SOURCE_COMMITTED_NATIVE_SOURCE_EQUIVALENCE_V1',()=> 'WEEKLY_SOURCE_COMMITTED_EXECUTION_TREE_EQUIVALENCE_V2');
emit('prerequisite-evidence.mjs',prereq);
let prereqReader=read(path.join(q,'QUALIFICATION_R2_READER.sqlinc'));
const start=prereqReader.indexOf('  source_value:=q->\'source_equivalence\';'),stop=prereqReader.indexOf('  needed:=array[');
if(start<0||stop<=start)throw Error('QUALIFICATION_PREREQUISITE_READER_ANCHOR');
prereqReader=prereqReader.slice(0,start)+`  perform private.weekly_source_protected_execution_tree_assert_v2(q->'source_equivalence',executed.git_commit,r.git_commit);
  if jsonb_typeof(q->'capabilities') is distinct from 'array'
    or q->'capabilities' not in ('["BULK_SIGNED"]'::jsonb,'["AUTOMATIC_RECONCILE","BULK_SIGNED"]'::jsonb)
    or jsonb_typeof(q->'gate_receipts') is distinct from 'array' then
    raise exception 'WEEKLY_SOURCE_PROTECTED_CAPABILITY_RELEASE_UNQUALIFIED' using errcode='55000'; end if;
`+prereqReader.slice(stop);
emit('02_complete_prerequisite_reader.sqlinc',prereqReader);
let allCap=read(path.join(cap,'CAPABILITY_OWNERS.sqlinc'));
const reader=get(allCap,'private.weekly_source_protected_capability_release_evidence_v1');
const anchor="  if count_rows<>1 or not private.weekly_exceptional_json_keys_exact_v1(q,array[";
const delta=`  if count_rows<>1 then raise exception 'WEEKLY_SOURCE_PROTECTED_OPERATIONAL_GATES_REQUIRED' using errcode='55000'; end if;
  return private.weekly_source_protected_operational_certificate_assert_v1(q,p_group_id,p_from,p_to,p_expected_sha256);
  -- The original V1-only validator remains unreachable evidence below. It
  -- cannot admit native-only or pending certificates into an operational path.
  if count_rows<>1 or not private.weekly_exceptional_json_keys_exact_v1(q,array[`;
proof('03_complete_operational_release_reader.sqlinc',reader,replace(reader,anchor,delta),[[anchor,delta]]);
const receipt=get(allCap,'private.weekly_source_protected_capability_receipt_v1');
const rAnchor="  if q is distinct from receipt->'protected_capability_qualification' or q->>'release_id' is distinct from a.feature_release_id then";
const rDelta=`  if q->>'schema_version' is distinct from 'WEEKLY_SOURCE_PROTECTED_CAPABILITY_OPERATIONAL_QUALIFICATION_V1'
    or q->>'phase' is distinct from 'ALL_PRE_SWITCH_GATES_QUALIFIED'
    or q is distinct from receipt->'protected_capability_qualification' or q->>'release_id' is distinct from a.feature_release_id then`;
proof('04_complete_capability_receipt.sqlinc',receipt,replace(receipt,rAnchor,rDelta),[[rAnchor,rDelta]]);
const bulkReady=get(allCap,'private.weekly_source_bulk_producer_ready_v1');
const bAnchor="  return q->'capabilities' @> '[\"BULK_SIGNED\"]'::jsonb;";
const bDelta=`  return coalesce(q->>'schema_version'='WEEKLY_SOURCE_PROTECTED_CAPABILITY_OPERATIONAL_QUALIFICATION_V1'
    and q->>'phase'='ALL_PRE_SWITCH_GATES_QUALIFIED' and q->'capabilities' @> '["BULK_SIGNED"]'::jsonb,false);`;
proof('05_complete_bulk_readiness.sqlinc',bulkReady,replace(bulkReady,bAnchor,bDelta),[[bAnchor,bDelta]]);
const autoReady=get(allCap,'private.weekly_source_automatic_reconcile_producer_ready_v1');
const aAnchor="  if not(q->'capabilities' @> '[\"AUTOMATIC_RECONCILE\"]'::jsonb) then return false; end if;";
const aDelta=`  if q->>'schema_version' is distinct from 'WEEKLY_SOURCE_PROTECTED_CAPABILITY_OPERATIONAL_QUALIFICATION_V1'
    or q->>'phase' is distinct from 'ALL_PRE_SWITCH_GATES_QUALIFIED'
    or not(q->'capabilities' @> '["AUTOMATIC_RECONCILE"]'::jsonb) then return false; end if;`;
proof('06_complete_automatic_readiness.sqlinc',autoReady,replace(autoReady,aAnchor,aDelta),[[aAnchor,aDelta]]);
let activation=read(path.join(cap,'generated/05_complete_existing_activation.sqlinc')),activationOriginal=activation;
const activationHunks=[
 ['begin\n  perform private._weekly_source_settings_assert_request_v1',`begin
  if p_request ? 'qualification_action' then
    return private.weekly_source_protected_test_setup_atomic_v1(p_request);
  end if;
  perform private._weekly_source_settings_assert_request_v1`],
 ["    if v_assignment.feature_request_sha256<>v_hash then raise exception 'WEEKLY_SOURCE_RULE_REQUEST_REUSED' using errcode='22023'; end if;",`    if v_assignment.feature_request_sha256<>v_hash then raise exception 'WEEKLY_SOURCE_RULE_REQUEST_REUSED' using errcode='22023'; end if;
    if v_assignment.feature_enabled and v_assignment.feature_receipt_json ? 'protected_capability_qualification' then
      perform private.weekly_source_protected_operational_certificate_assert_v1(
        v_assignment.feature_receipt_json->'protected_capability_qualification',v_group_id,v_from,v_assignment.effective_to_week_ending,
        v_assignment.feature_receipt_json#>>'{protected_capability_qualification,qualification_sha256}');
    end if;`],
 ["      v_assignment.effective_to_week_ending,p_request->>'protected_capability_qualification_sha256');",`      v_assignment.effective_to_week_ending,p_request->>'protected_capability_qualification_sha256');
    perform private.weekly_source_protected_operational_certificate_assert_v1(v_qualification,v_group_id,v_from,
      v_assignment.effective_to_week_ending,p_request->>'protected_capability_qualification_sha256');`]
];
for(const [before,after]of activationHunks)activation=replace(activation,before,after);
proof('07_complete_existing_feature_setup.sqlinc',activationOriginal,activation,activationHunks);
const casOriginal=read(path.join(q,'03_complete_cas_admission.sqlinc'));
const overload=`create or replace function private.weekly_source_bulk_source_cas_assert_v1(p_cycle_id uuid,p_client_ids uuid[],p_actor_user_id uuid)
returns jsonb language plpgsql stable security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare clients uuid[]; result jsonb;
begin
  begin return private.weekly_source_bulk_source_cas_assert_v1(p_cycle_id,p_client_ids);
  exception when sqlstate '55000' then
    if sqlerrm<>'WEEKLY_SOURCE_BULK_PRODUCER_UNAVAILABLE' then raise; end if;
  end;
  if p_client_ids is null or array_position(p_client_ids,null::uuid) is not null then
    raise exception 'WEEKLY_SOURCE_BULK_SOURCE_CAS_INVALID' using errcode='22023'; end if;
  select coalesce(array_agg(distinct item order by item),'{}'::uuid[]) into clients from unnest(p_client_ids) item;
  perform private.weekly_source_protected_test_scope_assert_v1(p_cycle_id,p_actor_user_id,clients,null);
  result:=private.weekly_source_bulk_source_cas_v1(p_cycle_id,clients);
  if result->>'schema_version' is distinct from 'WEEKLY_SOURCE_BULK_SOURCE_CAS_V1'
    or result->>'source_cycle_id' is distinct from p_cycle_id::text or result->'client_ids' is distinct from to_jsonb(clients)
    or coalesce(result->>'source_cas_sha256','') !~ '^[0-9a-f]{64}$' then
    raise exception 'WEEKLY_SOURCE_BULK_SOURCE_CAS_INVALID' using errcode='55000'; end if;
  return result;
end;
$function$;`;
emit('08_complete_cas_admission.sqlinc',casOriginal+'\n'+overload);
let mode=read(path.join(q,'04_complete_non_self_bill_admission.sqlinc'));
const modeOriginal=mode;
mode=replace(mode,'p_client_id uuid,p_contract_id uuid,p_work_date date,p_cycle_id uuid',
 'p_client_id uuid,p_contract_id uuid,p_work_date date,p_cycle_id uuid,p_actor_user_id uuid,p_work_event_id uuid');
const mAnchor="    exception when sqlstate '42501' or sqlstate '55000' then return null;\n    end;";
const mDelta=`    exception when sqlstate '42501' or sqlstate '55000' then
      if p_work_event_id is null then return null; end if;
      begin qualification:=private.weekly_source_protected_test_scope_assert_v1(c.id,p_actor_user_id,null,p_work_event_id);
      exception when sqlstate '42501' or sqlstate '55000' then return null; end;
      if not exists(select 1 from jsonb_array_elements(qualification#>'{original_census,original_items}') item
        where item->>'client_id'=p_client_id::text and item->>'contract_id'=p_contract_id::text
          and item->>'work_date'=p_work_date::text and item->>'work_event_id'=p_work_event_id::text) then return null; end if;
    end;`;
mode=replace(mode,mAnchor,mDelta);
proof('09_complete_actor_non_self_bill_admission.sqlinc',modeOriginal,mode,
 [['p_client_id uuid,p_contract_id uuid,p_work_date date,p_cycle_id uuid','p_client_id uuid,p_contract_id uuid,p_work_date date,p_cycle_id uuid,p_actor_user_id uuid,p_work_event_id uuid'],[mAnchor,mDelta]]);
const selOriginal=read(path.join(cap,'generated/06_complete_selection.sqlinc'));
const sBefore='private.weekly_source_bulk_source_cas_assert_v1(v_cycle.id,v_clients)',sAfter='private.weekly_source_bulk_source_cas_assert_v1(v_cycle.id,v_clients,p_actor_user_id)';
proof('10_complete_actor_bulk_selection.sqlinc',selOriginal,replace(selOriginal,sBefore,sAfter),[[sBefore,sAfter]]);
const ownOriginal=get(read(path.join(root,'supabase/repeatable/13102026_1438_weekly_source_protection_queries/25_bulk_stage_proof.sqlinc')),
 'private.weekly_source_bulk_own_stage_v1');
const oBefore='private.weekly_source_bulk_source_cas_assert_v1(p_cycle_id,v_clients)',oAfter='private.weekly_source_bulk_source_cas_assert_v1(p_cycle_id,v_clients,p_actor_id)';
proof('11_complete_actor_own_stage.sqlinc',ownOriginal,replace(ownOriginal,oBefore,oAfter),[[oBefore,oAfter]]);
for(const [name,call,actor,event]of [
 ['17_prepare_family','v_client_id,v_contract_id,v_work_date,v_cycle.id','v_actor_user_id','v_work_event_id'],
 ['18_prepare_action','v_contract.client_id,v_contract.id,v_event.work_date,v_cycle.id','v_actor','v_event.id'],
 ['19_action_context','v_contract.client_id,v_contract.id,v_event.work_date,v_cycle.id','v_actor','v_event.id'],
 ['20_stage_request','v_client_id,v_family.contract_id,v_work_date,v_cycle.id','v_actor_user_id','v_work_event_id']]){
 const body=read(path.join(cap,'generated/12_complete_'+name+'.sqlinc')),
  before='private.weekly_source_protected_non_self_bill_mode_v1('+call+')',after='private.weekly_source_protected_non_self_bill_mode_v1('+call+','+actor+','+event+')';
 proof('12_complete_actor_'+name+'.sqlinc',body,replace(body,before,after),[[before,after]]);
}
const newSignatures=['private.weekly_source_qualification_closed_object_v1(jsonb,text[])',
 'private.weekly_source_protected_operational_certificate_assert_v1(jsonb,uuid,date,date,text)',
 'private.weekly_source_protected_execution_tree_assert_v2(jsonb,text,text)',
 'private.weekly_source_protected_test_authorization_v1(uuid)',
 'private.weekly_source_protected_test_scope_assert_v1(uuid,uuid,uuid[],uuid)',
 'private.weekly_source_protected_test_setup_atomic_v1(jsonb)',
 'private.weekly_source_bulk_source_cas_assert_v1(uuid,uuid[],uuid)',
 'private.weekly_source_protected_non_self_bill_mode_v1(uuid,uuid,date,uuid,uuid,uuid)'];
emit('13_owner_only_acl.sqlinc',newSignatures.map(s=>`alter function ${s} owner to postgres;\nrevoke all on function ${s} from public,anon,authenticated,service_role;`).join('\n'));
let managed=read(path.join(q,'09_complete_managed_release.mjs')),managedOriginal=managed;
const managedHunks=[
 ["import { buildPrerequisiteQualification, assertManagedPrerequisiteEvidence, proveCommittedSources, installedQualificationCatalogueEvidence } from './weekly-source-protected-prerequisite-evidence.mjs';",
  "import { buildPrerequisiteQualification, assertManagedPrerequisiteEvidence, proveCommittedSources, installedQualificationCatalogueEvidence } from './weekly-source-protected-prerequisite-evidence.mjs';\nimport { assertManagedOperationalEvidence } from './weekly-source-protected-operational-evidence.mjs';\nimport { buildTestSetupAuthorization } from './weekly-source-protected-test-setup-evidence.mjs';"],
 ['preapply, verify, protectedCapabilityJourneys = [], log', 'preapply, verify, protectedCapabilityJourneys = [], protectedTestSetupAuthorizations = [], log'],
 ["        if (journey.phase==='ROLLBACK_PREREQUISITE') {",`        if (journey.phase==='FULL_OPERATIONAL') {
          if (!journey.qualification || journey.qualification.release_id!==context.releaseId
            || journey.qualification.git_commit!==context.gitCommit || journey.qualification.contract_sha256!==context.expectedHash)
            throw Error('OPERATIONAL_MANAGED_SCOPE_INVALID');
          return journey.qualification;
        }
        if (journey.phase==='ROLLBACK_PREREQUISITE') {`],
 ["    assertManagedPrerequisiteEvidence(context,verified.evidence??{},root);",`    const qualifications=verified.evidence?.weekly_source_protected_capability_qualifications??[];
    assertManagedPrerequisiteEvidence(context,{weekly_source_protected_capability_qualifications:qualifications.filter(
      q=>q.schema_version==='WEEKLY_SOURCE_PROTECTED_CAPABILITY_PREREQUISITE_QUALIFICATION_V2')},root);
    assertManagedOperationalEvidence(context,{weekly_source_protected_capability_qualifications:qualifications.filter(
      q=>q.schema_version!=='WEEKLY_SOURCE_PROTECTED_CAPABILITY_PREREQUISITE_QUALIFICATION_V2')},root);
    if (!Array.isArray(protectedTestSetupAuthorizations)) throw Error('TEST_SETUP_INPUT_INVALID');
    if (protectedTestSetupAuthorizations.length) {
      if (verified.evidence?.weekly_source_protected_test_setup_authorizations!==undefined) throw Error('TEST_SETUP_EVIDENCE_REPLACEMENT_REFUSED');
      const authorizations=protectedTestSetupAuthorizations.map(input=>buildTestSetupAuthorization({...context,mode},input,qualifications));
      for (const auth of authorizations) {
        const actual=JSON.parse(await execute('select private.weekly_source_protected_qualification_census_v1('
          +literal(auth.source_cycle_id)+'::uuid)::text'));
        if (domainHash('TEST_SETUP_ACTUAL_CENSUS',actual)!==domainHash('TEST_SETUP_ACTUAL_CENSUS',auth.original_census))
          throw Error('TEST_SETUP_NATIVE_CENSUS_CHANGED');
      }
      verified.evidence={...(verified.evidence??{}),weekly_source_protected_test_setup_authorizations:authorizations};
    }
    for (const q of qualifications.filter(q=>q.schema_version==='WEEKLY_SOURCE_PROTECTED_CAPABILITY_OPERATIONAL_QUALIFICATION_V1')) {
      const native=q.native_execution_qualification,closure=q.test_setup_closure;
      const recorded=JSON.parse(await execute('select jsonb_build_object('+literal('release_id')+',r.release_id,'+
        literal('source_commit')+',r.git_commit,'+literal('contract_sha256')+',r.installed_contract_sha256,'+
        literal('catalogue_sha256')+',r.evidence_json#>>'+literal('{weekly_source_protected_qualification_catalogue,jsonb_sha256}')+
        ')::text from private.cloudtms_database_releases r where r.release_id='+literal(native.release_id)+
        ' and r.status='+literal('VERIFIED')+' and r.completed_at_utc is not null and r.installed_contract_sha256=r.repository_contract_sha256'));
      if (!recorded || recorded.source_commit!==native.git_commit || recorded.contract_sha256!==context.expectedHash
        || recorded.catalogue_sha256!==verified.evidence.weekly_source_protected_qualification_catalogue.jsonb_sha256)
        throw Error('OPERATIONAL_RECORDED_EXECUTION_CHANGED');
      const actual=JSON.parse(await execute('select jsonb_build_object('+literal('id')+',a.id,'+literal('after')+',a.after_json,'+
        literal('sha256')+',encode(private.weekly_source_sha256_jsonb_v1('+literal('WEEKLY_SOURCE_TEST_JOURNEY_CLOSED_V1')+
        ',a.after_json),'+literal('hex')+'))::text from public.audit_events a where a.id='+literal(closure.closure_audit_id)+
        '::uuid and a.object_type='+literal('weekly_source_protected_test_journey')+' and a.object_id_text='+literal(closure.scope_id)+
        ' and a.action='+literal('WEEKLY_SOURCE_TEST_JOURNEY_CLOSED')+' and a.event_sequence_is_authoritative'));
      const {closure_audit_id,closure_audit_sha256,...after}=closure;
      if (!actual || actual.id!==closure_audit_id || actual.sha256!==closure_audit_sha256
        || domainHash('ACTUAL_TEST_SETUP_CLOSURE',actual.after)!==domainHash('ACTUAL_TEST_SETUP_CLOSURE',after))
        throw Error('OPERATIONAL_ACTUAL_SETUP_CLOSURE_REQUIRED');
    }`]
];
for(const [before,after]of managedHunks)managed=replace(managed,before,after);
// Replay cannot replace an already recorded setup authorization either.
const replayAnchor="    await execute(`begin;${assertWriterSql}${identitySql(context)}\n      update private.cloudtms_database_releases r set status='VERIFIED'";
const replayDelta=`    if (protectedTestSetupAuthorizations.length) {
      const old=JSON.parse(await execute('select coalesce(evidence_json->'+literal('weekly_source_protected_test_setup_authorizations')+','+
        literal('null')+'::jsonb)::text from private.cloudtms_database_releases where release_id='+literal(context.releaseId)));
      if (old!==null && domainHash('REPLAY_TEST_SETUP_AUTHORIZATIONS',old)!==domainHash('REPLAY_TEST_SETUP_AUTHORIZATIONS',
        verified.evidence.weekly_source_protected_test_setup_authorizations)) throw Error('TEST_SETUP_EVIDENCE_REPLACEMENT_REFUSED');
    }
`+replayAnchor;
managed=replace(managed,replayAnchor,replayDelta);managedHunks.push([replayAnchor,replayDelta]);
proof('14_complete_managed_release.mjs',managedOriginal,managed,managedHunks);
let caller=read(path.join(root,'scripts/cloudtms-db-release.mjs')),callerOriginal=caller;
const callerHunks=[
 ['#!/usr/bin/env node\n',"#!/usr/bin/env node\nimport {loadCommittedPrerequisiteJourneys} from './weekly-source-protected-prerequisite-evidence.mjs';\nimport {loadOperationalJourneys} from './weekly-source-protected-operational-evidence.mjs';\nimport {loadTestSetupInputs} from './weekly-source-protected-test-setup-evidence.mjs';\n"],
 ['      preapply: runBankingPayCatalogPreapply,',`      preapply: runBankingPayCatalogPreapply,
      protectedCapabilityJourneys:[...loadCommittedPrerequisiteJourneys(repoRoot,release,{...releaseVerifierContext,mode,expectedHash}),
        ...loadOperationalJourneys(repoRoot,release,{...releaseVerifierContext,mode,expectedHash})],
      protectedTestSetupAuthorizations:loadTestSetupInputs(repoRoot,release,{...releaseVerifierContext,mode,expectedHash}),`]
];
for(const [before,after]of callerHunks)caller=replace(caller,before,after);
proof('15_complete_current_release_caller.mjs',callerOriginal,caller,callerHunks);
for(const [file,target]of [['source-equivalence.mjs','weekly-source-protected-execution-tree.mjs'],
 ['operational-evidence.mjs','weekly-source-protected-operational-evidence.mjs'],['setup-evidence.mjs','weekly-source-protected-test-setup-evidence.mjs'],
 ['prerequisite-evidence.mjs','weekly-source-protected-prerequisite-evidence.mjs']]){
 const body=read(path.join(out,file)).replaceAll("'../protected-source-capability/release-qualification.mjs'",()=>"'./weekly-source-protected-release-qualification.mjs'")
  .replaceAll("'./source-equivalence.mjs'",()=>"'./weekly-source-protected-execution-tree.mjs'");
 emit(target,body);
}
emit('weekly-source-protected-release-qualification.mjs',read(path.join(cap,'release-qualification.mjs')));
const commandOriginal=read(path.join(cap,'generated/08_complete_bulk_command.sqlinc'));
const commandBefore="    return jsonb_build_object('ok',true,'mode','PREVIEW')||private.weekly_source_protect_all_signed_selection_v1(v_cycle_id,v_actor,null);";
const commandAfter=`    v_selection:=private.weekly_source_protect_all_signed_selection_v1(v_cycle_id,v_actor,null);
    if not private.weekly_source_bulk_producer_ready_v1(v_cycle_id) then
      v_context:=private.weekly_source_protected_test_scope_assert_v1(v_cycle_id,v_actor,null,null);
      return jsonb_build_object('ok',true,'mode','PREVIEW','qualification',jsonb_build_object(
        'contract','WEEKLY_SOURCE_TEST_JOURNEY_PRESENTATION_V1','scope_id',v_context->>'scope_id',
        'scope_sha256',v_context->>'scope_sha256','expires_at_utc',v_context->>'expires_at_utc',
        'source_cycle_id',v_cycle_id,'actor_user_id',v_actor,'operational_ready',false,
        'allowed_action','PROTECT_ALL_SIGNED'))||v_selection;
    end if;
    return jsonb_build_object('ok',true,'mode','PREVIEW')||v_selection;`;
proof('16_complete_bulk_command_presentation.sqlinc',commandOriginal,replace(commandOriginal,commandBefore,commandAfter),[[commandBefore,commandAfter]]);
let routes=read(path.join(root,'broker/src/weekly-source/routes.js')),routesOriginal=routes;
const routeHunks=[
 ["  ...PROTECTED_ACTIONS,", "  ...PROTECTED_ACTIONS, 'PROTECT_ALL_SIGNED',"],
 ["  if (PROTECTED_ACTIONS.has(action)) {", `  if (action === 'PROTECT_ALL_SIGNED') {
    if (typeof dependencies.orchestrateProtectedBulk !== 'function') {
      fail('WEEKLY_SOURCE_BULK_PRODUCER_UNAVAILABLE', 'Approved hours cannot be changed right now.', 503);
    }
    // The closed native selection either proves operational readiness or the
    // exact TEST fixture lease. No browser capability/strategy is forwarded.
    return jsonResponse(200, await dependencies.orchestrateProtectedBulk({
      request: payload, actor: user, env, ctx,
    }));
  }
  if (PROTECTED_ACTIONS.has(action)) {`]
];
for(const [before,after]of routeHunks)routes=replace(routes,before,after);
proof('17_complete_current_routes.js',routesOriginal,routes,routeHunks);
const index=read(path.join(root,'broker/src/index.js'));
const callbackAnchor='        orchestrateProtectedAction: ({ action, request }) => orchestrateWeeklyProtectedAction({';
if(index.split(callbackAnchor).length!==2)throw Error('QUALIFICATION_INDEX_CALLBACK_CHANGED');
const callback=`        orchestrateProtectedBulk: ({ request, actor }) => orchestrateWeeklyProtectedBulk({
          request,
          actorUserId: actor.id,
          dependencies: {
            dataRpc: (functionName, args, options) => sbRpcRecordingGuardRefusal(env, functionName, args, options),
            calculateWeeklySnapshot: (calculationInput) => calculateWeeklyProtectedSnapshot(env, calculationInput),
            c1RawRpc: createWeeklySourceC1RawRpc({ baseUrl: env.SUPABASE_URL, headers: sbHeaders(env), fetchImpl: fetch })
          }
        }),
`;
const importAnchor="import { orchestrateWeeklyProtectedAction } from './weekly-source/protected-action-orchestrator.js';";
let actualImport=[...index.matchAll(/^import[^\n]*orchestrateWeeklyProtectedAction[^\n]*$/gm)].map(row=>row[0]);
if(actualImport.length!==1)throw Error('QUALIFICATION_INDEX_IMPORT_CHANGED');
const indexHunks=[[actualImport[0],actualImport[0]+"\nimport { orchestrateWeeklyProtectedBulk } from './weekly-source/protected-bulk-action.mjs';"],
 [callbackAnchor,callback+callbackAnchor]];
let proposedIndex=index;for(const [before,after]of indexHunks)proposedIndex=replace(proposedIndex,before,after);
let reverseIndex=proposedIndex;for(const [before,after]of [...indexHunks].reverse())reverseIndex=replace(reverseIndex,after,before);
if(reverseIndex!==index)throw Error('QUALIFICATION_INDEX_REVERSE_FAILED');
emit('INDEX_CALL_SITE_DELTAS.json',JSON.stringify({schema_version:'PROTECTED_SOURCE_BULK_TRUSTED_WORKER_CALL_SITE_V1',
 native_path:'broker/src/index.js',input_sha256:sha(index),proposed_complete_owner_sha256:sha(proposedIndex),
 reverse_hunk_matches_entire_input_definition:true,hunks:indexHunks.map(([before,after])=>({before,after})),
 qualification_admission_owner:'private.weekly_source_protected_test_scope_assert_v1(uuid,uuid,uuid[],uuid)',
 operational_ready_never_overridden:true,root_must_reconcile_concurrent_index_deltas:true},null,2));
emit('OWNER_DELTAS.json',JSON.stringify({schema_version:'PROTECTED_SOURCE_OPERATIONAL_GATE_FORWARD_OWNER_DELTA_V1',inputs,proofs,
 new_private_signatures:newSignatures,new_public_rpcs:0,new_service_grants:0,new_tables:0,new_columns:0,
 operational_certificate_required:true,prerequisite_certificate_operational:false,
 qualification_scope_cross_session_capability:'BULK_SIGNED_ONLY',automatic_positive_held:true,
 source_only:true,native_acceptance:false},null,2));
console.log(JSON.stringify({full_owner_deltas:proofs.length,new_private_signatures:newSignatures.length,source_only:true}));
