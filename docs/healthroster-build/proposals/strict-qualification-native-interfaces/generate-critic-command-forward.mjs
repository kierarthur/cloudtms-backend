import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const out=path.dirname(fileURLToPath(import.meta.url)),own=path.resolve(out,'../../../..'),prefix='docs/healthroster-build/proposals/strict-qualification-native-interfaces/',base='fef4717f93c0925167d4b74332560781af421b4d',sha=x=>createHash('sha256').update(x).digest('hex');
const proofs=[];
function rewrite(file,hunks){let source=execFileSync('git',['show',base+':'+prefix+file],{cwd:own,encoding:'utf8',maxBuffer:2_000_000});const original=source;
 for(const h of hunks){assert.equal(source.split(h.old).length-1,1,h.name);source=source.replace(h.old,h.new);}
 let reversed=source;for(const h of [...hunks].reverse()){assert.equal(reversed.split(h.new).length-1,1,h.name);reversed=reversed.replace(h.new,h.old);}assert.equal(reversed,original);
 fs.writeFileSync(path.join(out,file),source);proofs.push({file,base_commit:base,input_sha256:sha(original),output_sha256:sha(source),reverse_restores_entire_source:true,hunks:hunks.map(h=>({name:h.name,old:h.old,new:h.new}))});}
const command=execFileSync('git',['show',base+':'+prefix+'OFF_RULE_COMMAND.sqlinc'],{cwd:own,encoding:'utf8'});
const start=command.indexOf('  perform private.weekly_source_rule_qualification_scope_assert_v1(auth,actor,false);'),end=command.indexOf('  if exists(select 1 from public.audit_events audit',start);
rewrite('OFF_RULE_COMMAND.sqlinc',[
 {name:'command locals',old:'command_id uuid:=gen_random_uuid(); stack text; discovered uuid; item jsonb;',new:'command_id uuid:=gen_random_uuid(); stack text; parent_scope jsonb; completion jsonb;'},
 {name:'nonwaiting scope gate before OPEN and all native parents before frame',old:command.slice(start,end),new:`  -- Nonwaiting same-scope gate precedes OPEN revalidation. It remains held
  -- through native dispatch/completion until transaction end. No group/row
  -- lock is acquired ahead of native I1; a CLOSE holder returns busy here.
  if not pg_try_advisory_xact_lock(hashtextextended('weekly-source-off-rule-test:'||(auth->>'scope_id'),0)) then
    raise exception 'WEEKLY_SOURCE_OFF_RULE_SCOPE_BUSY' using errcode='55P03'; end if;
  perform private.weekly_source_rule_qualification_scope_assert_v1(auth,actor,false);
  parent_scope:=private.weekly_source_off_rule_native_parent_scope_v1(req,actor,auth);
`},
 {name:'measured parent/protected prepare trace',old:"'WEEKLY_SOURCE_OFF_RULE_NATIVE_REQUEST_V1',req),'hex'),'operational_ready',false);",new:"'WEEKLY_SOURCE_OFF_RULE_NATIVE_REQUEST_V1',req),'hex'),'native_parent_scope',parent_scope,'operational_ready',false);\n  if op='weekly_exceptional_pay_prepare_action_v1' then frame:=frame||jsonb_build_object('native_prepare_request',req); end if;"},
 {name:'complete genuine prepare result for later actual run discovery',old:`  perform public._audit_insert('weekly_source_off_rule_command',command_id::text,'WEEKLY_SOURCE_OFF_RULE_COMMAND_COMPLETED',frame,
    jsonb_build_object('command_id',command_id,'command_sha256',frame->>'command_sha256','native_result_sha256',
      encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_OFF_RULE_NATIVE_RESULT_V1',result),'hex'),'operational_ready',false),
    'Controlled OFF fixture native command complete',actor);`,new:`  completion:=jsonb_build_object('command_id',command_id,'command_sha256',frame->>'command_sha256','native_result_sha256',
    encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_OFF_RULE_NATIVE_RESULT_V1',result),'hex'),'operational_ready',false);
  if op='weekly_exceptional_pay_prepare_action_v1' then
    if result->>'source_cycle_id' is distinct from cycle_id::text or result->>'family_id' is distinct from req->>'family_id'
      or result->>'work_event_id' is distinct from req->>'work_event_id'
      or not exists(select 1 from public.weekly_exceptional_orchestration_runs r where r.id=(result->>'orchestration_run_id')::uuid
        and r.family_id=(req->>'family_id')::uuid and r.requested_by_user_id=actor
        and r.request_fingerprint=private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_PREPARE_ACTION_REQUEST_V1',req)) then
      raise exception 'WEEKLY_SOURCE_OFF_RULE_PREPARE_RESULT_CHANGED' using errcode='42501'; end if;
    completion:=completion||jsonb_build_object('native_prepare_result',result);
  end if;
  perform public._audit_insert('weekly_source_off_rule_command',command_id::text,'WEEKLY_SOURCE_OFF_RULE_COMMAND_COMPLETED',frame,
    completion,'Controlled OFF fixture native command complete',actor);`}
]);
const transport=execFileSync('git',['show',base+':'+prefix+'OFF_RULE_TRANSPORT_STATUS.sqlinc'],{cwd:own,encoding:'utf8'}),tstart=transport.indexOf("  if nullif(req->>'source_cycle_id'"),tend=transport.indexOf('  result:=private.weekly_source_rule_qualification_status_v1',tstart);
rewrite('OFF_RULE_TRANSPORT_STATUS.sqlinc',[
 {name:'shared discovery locals',old:"declare req jsonb; cycles uuid[]:='{}'::uuid[]; discovered uuid; cycle_id uuid; actor uuid; op text; purpose text; result jsonb;",new:'declare req jsonb; parent_scope jsonb; cycle_id uuid; actor uuid; op text; purpose text; result jsonb; claim jsonb;'},
 {name:'same real parents without invented orchestration cycle',old:transport.slice(tstart,tend),new:`  parent_scope:=private.weekly_source_off_rule_native_parent_scope_v1(req,actor,null);
  if parent_scope->>'source_cycle_id' is null or (parent_scope->>'ambiguous')::boolean then
    if (select count(distinct value) from jsonb_array_elements_text(parent_scope->'observed_cycle_ids') value)>1 then
      raise exception 'WEEKLY_SOURCE_OFF_RULE_TRANSPORT_SCOPE_CHANGED' using errcode='40001'; end if;
    for cycle_id in select distinct value::uuid from jsonb_array_elements_text(parent_scope->'observed_cycle_ids') value loop
      claim:=private.weekly_source_rule_qualification_status_v1(jsonb_build_object('actor_user_id',actor,'agency_id',p_request->>'agency_id',
        'environment','TEST','qualification_action','OFF_RULE_STATUS','source_cycle_id',cycle_id,'purpose',purpose));
      if (claim->>'qualified_fixture')::boolean then
        raise exception 'WEEKLY_SOURCE_OFF_RULE_TRANSPORT_SCOPE_CHANGED' using errcode='40001'; end if;
    end loop;
    -- Advisory discovery never admits OFF. In particular an ordinary
    -- unstaged run has no OFF cycle and retains its actual native response.
    return jsonb_build_object('contract','WEEKLY_SOURCE_OFF_RULE_TEST_ADMISSION_V1','qualified_fixture',false,'operational_ready',false);
  end if;
  cycle_id:=(parent_scope->>'source_cycle_id')::uuid;
`},
 {name:'claimed OFF status errors stay fatal',old:`exception when no_data_found or too_many_rows then
  -- A missing parent is left to the exact normal native owner/refusal. It
  -- cannot authorize an OFF frame or become guessed cycle authority.
  return jsonb_build_object('contract','WEEKLY_SOURCE_OFF_RULE_TEST_ADMISSION_V1','qualified_fixture',false,'operational_ready',false);
end;
$function$;`,new:'end;\n$function$;'}
]);
fs.writeFileSync(path.join(out,'CRITIC_COMMAND_TRANSPORT_REVERSE.json'),JSON.stringify({source_only:true,proofs,native_dispatch_abi_unchanged:true},null,2)+'\n');
console.log(JSON.stringify({source_only:true,complete_reverse_proofs:proofs.length}));
