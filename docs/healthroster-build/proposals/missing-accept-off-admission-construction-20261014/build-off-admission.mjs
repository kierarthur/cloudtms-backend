import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(dir,'../../../..'),proposals=path.dirname(dir),sha=b=>createHash('sha256').update(b).digest('hex'),commit='33927fff2f7b90b90241d746d9e35aa21c57bd07',own='f1aa8d578b34d2016e916f29c43e5b0c617ed1e8',pins=[],proofs=[];assert.ok(!fs.existsSync(path.join(dir,'FROZEN_ARTIFACT_MANIFEST.json')));
function input(file,c=commit){const b=execFileSync('git',['show',c+':'+file],{cwd:root,maxBuffer:4e6});pins.push({commit:c,file,bytes:b.length,sha256:sha(b)});return b;}
function compose(file,b,hunks){const eol=b.includes(Buffer.from('\r\n'))?'\r\n':'\n';let s=b.toString();const actual=hunks.map(h=>({...h,before:h.before.replaceAll('\n',eol),after:h.after.replaceAll('\n',eol)}));for(const h of actual){assert.equal(s.split(h.before).length,2,h.name);s=s.replace(h.before,h.after);}const after=Buffer.from(s);for(const h of [...actual].reverse()){assert.equal(s.split(h.after).length,2,h.name);s=s.replace(h.after,h.before);}assert.deepEqual(Buffer.from(s),b);for(const side of ['before','forward'])fs.mkdirSync(path.dirname(path.join(dir,side,file)),{recursive:true});fs.writeFileSync(path.join(dir,'before',file),b);fs.writeFileSync(path.join(dir,'forward',file),after);proofs.push({file,before_bytes:b.length,before_sha256:sha(b),after_bytes:after.length,after_sha256:sha(after),unique_hunks:actual.length,hunks:actual,whole_reverse_verified:true});}
const native='supabase/repeatable/14102026_0430_healthroster_money_native_interfaces/',add="'weekly_source_missing_accept_context_v1','weekly_source_missing_accept_prepare_atomic_v1','weekly_source_later_change_decide_atomic_v1'";
compose('01_complete_off_command.sqlinc',input(native+'06_OFF_RULE_COMMAND.sqlinc'),[
 {name:'finite Missing journey operations',before:"'weekly_exceptional_pay_stage_c1_request_v1') then purpose:='AUTOMATIC_JOURNEY';",after:"'weekly_exceptional_pay_stage_c1_request_v1',"+add+") then purpose:='AUTOMATIC_JOURNEY';"},
 {name:'only reserved Missing later approval is an OFF command',before:'  auth:=private.weekly_source_rule_qualification_authorization_v1(cycle_id,actor,purpose);',after:"  if op='weekly_source_later_change_decide_atomic_v1' and req->>'decision' is distinct from 'APPROVE_UPDATED_HOURS' then\n    raise exception 'WEEKLY_SOURCE_OFF_RULE_NATIVE_OPERATION_REFUSED' using errcode='42501';end if;\n  auth:=private.weekly_source_rule_qualification_authorization_v1(cycle_id,actor,purpose);"},
 {name:'literal native dispatcher retains original owner ABI',before:"    when 'weekly_exceptional_pay_stage_c1_request_v1' then result:=public.weekly_exceptional_pay_stage_c1_request_v1(req);",after:"    when 'weekly_exceptional_pay_stage_c1_request_v1' then result:=public.weekly_exceptional_pay_stage_c1_request_v1(req);\n    when 'weekly_source_missing_accept_context_v1' then result:=public.weekly_source_missing_accept_context_v1(req);\n    when 'weekly_source_missing_accept_prepare_atomic_v1' then result:=public.weekly_source_missing_accept_prepare_atomic_v1(req);\n    when 'weekly_source_later_change_decide_atomic_v1' then result:=public.weekly_source_later_change_decide_atomic_v1(req);"}
]);
compose('02_complete_off_transport.sqlinc',input(native+'08_OFF_RULE_TRANSPORT_STATUS.sqlinc'),[{name:'same literal automatic purpose inventory',before:"'weekly_exceptional_pay_action_context_v1','weekly_exceptional_pay_action_publication_status_v1','weekly_exceptional_pay_stage_c1_request_v1') then purpose:='AUTOMATIC_JOURNEY';",after:"'weekly_exceptional_pay_action_context_v1','weekly_exceptional_pay_action_publication_status_v1','weekly_exceptional_pay_stage_c1_request_v1',"+add+") then purpose:='AUTOMATIC_JOURNEY';"}]);
compose('03_complete_off_setup_shape.sqlinc',input(native+'21_SETUP_STATIC_SHAPE_ASSERT.sqlinc'),[{name:'setup permits exact finite new operations only',before:"'weekly_exceptional_pay_action_context_v1','weekly_exceptional_pay_action_publication_status_v1','weekly_exceptional_pay_stage_c1_request_v1'] else null end;",after:"'weekly_exceptional_pay_action_context_v1','weekly_exceptional_pay_action_publication_status_v1','weekly_exceptional_pay_stage_c1_request_v1',"+add+"] else null end;"}]);
const auditScope=`select 1 from public.audit_events missing
            where missing.action='WEEKLY_SOURCE_MISSING_ACCEPT_PROPOSED' and missing.object_type='weekly_source_entitlement_decision_bundles'
              and missing.object_id_text=subject->>'decision_bundle_id'
              and missing.after_json->>'bundle_revision'=subject->>'bundle_revision'
              and missing.after_json#>>'{inputs,target_final_revision_id}'=subject->>'final_revision_id'
              and missing.after_json#>>'{inputs,root_timesheet_id}'=root.timesheet_id::text
              and missing.after_json#>>'{inputs,client_id}'=contract.client_id::text
              and missing.after_json#>>'{inputs,candidate_id}'=contract.candidate_id::text
              and missing.after_json#>>'{inputs,contract_id}'=contract.id::text
              and missing.after_json->>'proof_sha256'=encode(private.weekly_source_sha256_jsonb_v1(
                'WEEKLY_SOURCE_MISSING_ACCEPT_PROPOSAL_PROOF_V1',missing.after_json-'proof_sha256'),'hex')`;
compose('04_complete_off_parent_scope.sqlinc',input(native+'05_OFF_RULE_NATIVE_PARENT_SCOPE.sqlinc'),[
 {name:'actual native decision bundle row and proof locals',before:'  proof jsonb; proof_result jsonb; prepare_request jsonb; command_frame jsonb; n integer; key text;',after:'  proof jsonb; proof_result jsonb; prepare_request jsonb; command_frame jsonb; n integer; key text;\n  decision_bundle public.weekly_source_entitlement_decision_bundles%rowtype;missing_audit public.audit_events%rowtype;'},
 {name:'discover finite native Missing bundle selector',before:"'root_timesheet_id','evidence_timesheet_id','upload_row_id','client_id','candidate_id','contract_id'] loop",after:"'root_timesheet_id','evidence_timesheet_id','upload_row_id','client_id','candidate_id','contract_id','decision_bundle_id'] loop"},
 {name:'reserved proof binds the native bundle to real final cycle and full parent identity',before:"        when 'client_id' then parent:=jsonb_build_object('client_id',selector);",after:`        when 'decision_bundle_id' then
          select * into strict decision_bundle from public.weekly_source_entitlement_decision_bundles
            where decision_bundle_id=selector and bundle_revision=(subject->>'bundle_revision')::bigint;
          parent:=jsonb_build_object('decision_bundle_id',selector,'bundle_revision',decision_bundle.bundle_revision,
            'root_timesheet_id',decision_bundle.source_root_timesheet_id,'contract_id',decision_bundle.source_contract_id,
            'candidate_id',decision_bundle.candidate_id);
          select count(*) into n from public.audit_events audit
            where audit.action='WEEKLY_SOURCE_MISSING_ACCEPT_PROPOSED' and audit.object_type='weekly_source_entitlement_decision_bundles'
              and audit.object_id_text=selector::text and audit.after_json->>'bundle_revision'=decision_bundle.bundle_revision::text;
          if n>1 or (qualified and n<>1) then
            raise exception 'WEEKLY_SOURCE_OFF_RULE_NATIVE_PARENT_LINK_CHANGED' using errcode='42501';end if;
          if n=1 then
            select * into strict missing_audit from public.audit_events audit
              where audit.action='WEEKLY_SOURCE_MISSING_ACCEPT_PROPOSED' and audit.object_type='weekly_source_entitlement_decision_bundles'
                and audit.object_id_text=selector::text and audit.after_json->>'bundle_revision'=decision_bundle.bundle_revision::text;
            proof:=missing_audit.after_json;
            if proof->>'proof_sha256' is distinct from encode(private.weekly_source_sha256_jsonb_v1(
                'WEEKLY_SOURCE_MISSING_ACCEPT_PROPOSAL_PROOF_V1',proof-'proof_sha256'),'hex')
              or proof->>'actor_user_id' is distinct from missing_audit.actor_user_id::text
              or decision_bundle.decided_by_user_id is distinct from missing_audit.actor_user_id
              or proof#>>'{inputs,root_timesheet_id}' is distinct from decision_bundle.source_root_timesheet_id::text
              or proof#>>'{inputs,contract_id}' is distinct from decision_bundle.source_contract_id::text
              or proof#>>'{inputs,candidate_id}' is distinct from decision_bundle.candidate_id::text
              or proof#>>'{inputs,target_final_revision_id}' is distinct from subject->>'final_revision_id' then
              raise exception 'WEEKLY_SOURCE_OFF_RULE_NATIVE_PARENT_LINK_CHANGED' using errcode='42501';end if;
            parent:=parent||jsonb_build_object('source_cycle_id',proof#>'{inputs,source_cycle_id}',
              'final_revision_id',proof#>'{inputs,target_final_revision_id}',
              'work_event_id',proof#>'{inputs,work_event_id}','client_id',proof#>'{inputs,client_id}');
          end if;
        when 'client_id' then parent:=jsonb_build_object('client_id',selector);`},
 {name:'genuine Missing root may be retained rather than present in current Final movements',before:"where m.final_revision_id=(subject->>'final_revision_id')::uuid and m.invoice_timesheet_id=root.timesheet_id and m.actual_client_id=contract.client_id) then",after:"where m.final_revision_id=(subject->>'final_revision_id')::uuid and m.invoice_timesheet_id=root.timesheet_id and m.actual_client_id=contract.client_id)\n            and not exists("+auditScope+") then"}
]);
compose('broker/src/weekly-source/off-rule-native-proxy.mjs',input('broker/src/weekly-source/off-rule-native-proxy.mjs'),[{name:'current literal runtime operation inventory',before:"'weekly_exceptional_pay_action_context_v1','weekly_exceptional_pay_action_publication_status_v1','weekly_exceptional_pay_stage_c1_request_v1']);",after:"'weekly_exceptional_pay_action_context_v1','weekly_exceptional_pay_action_publication_status_v1','weekly_exceptional_pay_stage_c1_request_v1',"+add+"]);"}]);
const saved='docs/healthroster-build/proposals/missing-accept-current-native-construction-20261014/forward/';
compose('05_complete_missing_accept_inputs.sqlinc',input(saved+'00_missing_accept_inventory_inputs.sqlinc',own),[{name:'actual scoped fresh HealthRoster admission before deriving current proposal inputs',before:"  select revision.id into v_target_final_revision",after:"  perform private.weekly_source_missing_accept_admission_v1(v_work.client_id,v_cycle.id,p_actor_user_id);\n  select revision.id into v_target_final_revision"}]);
compose('06_complete_missing_accept_prepare.sqlinc',input(saved+'01_missing_accept_prepare.sqlinc',own),[{name:'existing native fresh query contention gate after exact historical replay before I1/business locks',before:'  v_initial:=private.weekly_source_missing_accept_inventory_inputs_v1',after:'  perform private.weekly_source_pay_query_admit_v2();\n  v_initial:=private.weekly_source_missing_accept_inventory_inputs_v1'}]);
for(const file of ['broker/src/weekly-source/off-rule-transport-rpc.mjs','scripts/weekly-source-off-rule-setup-evidence.mjs','supabase/repeatable/14102026_0410_healthroster_protected_source_prerequisites/34_q45_actor_event_closed_dto_cross_session_04_complete_capability_receipt.sqlinc','supabase/repeatable/05102026_0044_bpay_next_source_pay_query_admission_v2.sql','supabase/migrations/15092026_1534_weekly_source_plan6_schema.sql'])input(file);
fs.writeFileSync(path.join(dir,'CURRENT_INPUT_PINS.json'),JSON.stringify({source_only:true,current_input_commit:commit,own_input_commit:own,inputs:pins,canonical_mounted:false,native_proof:false},null,2)+'\n');fs.writeFileSync(path.join(dir,'COMPLETE_CURRENT_REVERSES.json'),JSON.stringify({source_only:true,complete_owners:proofs.length,unique_hunks:proofs.reduce((n,p)=>n+p.unique_hunks,0),proofs,shared_banking_owner_changes:0,canonical_mounted:false,native_proof:false},null,2)+'\n');console.log(JSON.stringify({complete_reverses:proofs.length,unique_hunks:proofs.reduce((n,p)=>n+p.unique_hunks,0)}));
