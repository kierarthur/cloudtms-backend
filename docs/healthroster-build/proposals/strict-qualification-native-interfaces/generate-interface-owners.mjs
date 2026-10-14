import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {fileURLToPath,pathToFileURL} from 'node:url';
const out=path.dirname(fileURLToPath(import.meta.url)),own=path.resolve(out,'../../../..');
const root=path.resolve(own,'../healthroster-v2-backend-13102026'),sha=b=>createHash('sha256').update(b).digest('hex');
const {scanManagedSql}=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-release-sql.mjs')));
const prior=path.resolve(out,'../protected-source-qualification-operational-gate');
const inputs=fs.readFileSync(path.join(out,'CURRENT_SOURCE_OWNER_INPUTS.sqlinc'),'utf8');
const edits=[];
function definition(source,name,parameterTest=()=>true){const defs=scanManagedSql(source).statements.map(r=>source.slice(r.start,r.end)).filter(text=>{
 const match=text.match(/^\s*create\s+(?:or\s+replace\s+)?function\s+((?:public|private)\.[\w]+)\s*\(([\s\S]*?)\)\s*returns/i);return match?.[1]?.toLowerCase()===name&&parameterTest(match[2]);
 });assert.equal(defs.length,1,'EXACT_ONE_DEFINITION '+name);return defs[0];}
function replaceOnce(source,anchor,replacement){assert.equal(source.split(anchor).length-1,1,'EXACT_ONE_ANCHOR '+anchor);return source.replace(anchor,()=>replacement);}
function save(name,before,after,hunks){let reverse=after;for(const h of [...hunks].reverse())reverse=replaceOnce(reverse,h.after,h.before);
 assert.equal(reverse,before,'ENTIRE_SOURCE_REVERSE '+name);fs.writeFileSync(path.join(out,name),after.trimEnd()+'\n');
 edits.push({file:name,input_definition_sha256:sha(Buffer.from(before)),output_definition_sha256:sha(Buffer.from(after)),
  reverse_hunk_matches_entire_input_definition:true,serialization:'Only whitespace outside the terminal SQL statement is normalized to one newline; body/statement reverses to entire input definition.',hunks:hunks.map(h=>({before_sha256:sha(Buffer.from(h.before)),after_sha256:sha(Buffer.from(h.after))}))});}
const effective=definition(fs.readFileSync(path.join(root,'supabase/repeatable/13102026_1134_weekly_source_rule_sets_v1.sql'),'utf8').replaceAll('\r\n','\n'),'private.weekly_source_effective_rule_set_v1');
const operational=replaceOnce(effective,'private.weekly_source_effective_rule_set_v1(','private.weekly_source_operational_effective_rule_set_v1(');
fs.writeFileSync(path.join(out,'01_operational_rule_projection.sqlinc'),operational+'\n');
fs.writeFileSync(path.join(out,'02_effective_rule_owner.sqlinc'),`create or replace function private.weekly_source_effective_rule_set_v1(p_client_id uuid,p_cycle_id uuid)
returns jsonb language plpgsql stable security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare normal jsonb; auth jsonb; result jsonb;
begin
  normal:=private.weekly_source_operational_effective_rule_set_v1(p_client_id,p_cycle_id);
  if normal is not null then return normal; end if;
  auth:=private.weekly_source_rule_qualification_context_v1(p_client_id,p_cycle_id);
  if auth is null then return null; end if;
  select jsonb_build_object('contract','WEEKLY_SOURCE_EFFECTIVE_RULE_SET_V1','source_group_id',g.id,
    'assignment_id',a.id,'assignment_version',a.version,'rule_set_id',rules.id,'rule_set_version_id',v.id,
    'version_number',v.version_number,'values_sha256',v.values_sha256,'values',v.values_json) into strict result
  from public.weekly_source_cycles c join public.weekly_source_groups g on g.id=c.source_group_id and g.active and g.source_family='ROSTER'
  join public.weekly_source_group_clients member on member.source_group_id=g.id and member.client_id=p_client_id
    and c.finalisation_week_ending between member.valid_from and coalesce(member.valid_to,'infinity'::date)
  join public.weekly_source_group_rule_assignments a on a.source_group_id=g.id and a.id=(auth->>'assignment_id')::uuid and not a.feature_enabled
    and c.finalisation_week_ending between a.effective_from_week_ending and coalesce(a.effective_to_week_ending,'infinity'::date)
  join public.weekly_source_rule_set_versions v on v.id=a.rule_set_version_id and v.id=(auth->>'rule_set_version_id')::uuid
  join public.weekly_source_rule_sets rules on rules.id=v.rule_set_id and rules.agency_id=g.agency_id and rules.environment=g.environment
  where c.id=p_cycle_id;
  return result;
end;
$function$;\n`);
const cas=fs.readFileSync(path.join(out,'../native-financial-fixtures/CAPABILITY_SOURCE_CAS_SCHEMA_FORWARD.sqlinc'),'utf8').replaceAll('\r\n','\n');
const casAnchor='private.weekly_source_effective_rule_set_v1(client,c.id)';
save('03_cas_operational_projection.sqlinc',cas,replaceOnce(cas,casAnchor,'private.weekly_source_operational_effective_rule_set_v1(client,c.id)'),
 [{before:casAnchor,after:'private.weekly_source_operational_effective_rule_set_v1(client,c.id)'}]);
const guard=definition(fs.readFileSync(path.join(root,'supabase/repeatable/13102026_1134_weekly_source_rule_sets_v1.sql'),'utf8').replaceAll('\r\n','\n'),'private.weekly_source_final_rule_version_guard_v1');
const guardBefore="  if new.rule_set_version_id is not null and new.rule_set_version_id is distinct from v_version then";
const guardAfter=`  if v_version is null then
    -- One exact per-command actual F1 reference; the normal disabled path is
    -- unchanged. UPDATE immutability above and the locked native FIN remain.
    v_version:=nullif(private.weekly_source_rule_qualification_context_v1(null,new.source_cycle_id)->>'rule_set_version_id','')::uuid;
  end if;
${guardBefore}`;
save('04_final_revision_rule_guard.sqlinc',guard,replaceOnce(guard,guardBefore,guardAfter),[{before:guardBefore,after:guardAfter}]);
const floor=definition(inputs,'private.weekly_source_rule_coverage_floor_v1');
const floorBefore='  -- F1\'s only feature writer refuses false->false.';
const floorAfter=`  if private.weekly_source_operational_effective_rule_set_v1(p_client_id,p_cycle_id) is null then
    -- Qualification exposure is its genuine existing audit OPEN, never an
    -- invented normal activation receipt or a checking-time clock substitute.
    select (audit.after_json->>'coverage_floor_local_date')::date into strict v_floor
    from public.audit_events audit
    where audit.object_type='weekly_source_off_rule_test_journey' and audit.action='WEEKLY_SOURCE_OFF_RULE_TEST_OPENED'
      and audit.event_sequence_is_authoritative and audit.object_id_text=private.weekly_source_rule_qualification_context_v1(p_client_id,p_cycle_id)->>'scope_id';
    if v_floor<>v_cycle.finalisation_week_ending-6 then
      raise exception 'WEEKLY_SOURCE_RULE_COVERAGE_HISTORY_UNPROVABLE' using errcode='55000'; end if;
    return v_floor;
  end if;
${floorBefore}`;
save('05_coverage_floor_owner.sqlinc',floor,replaceOnce(floor,floorBefore,floorAfter),[{before:floorBefore,after:floorAfter}]);
const setup=fs.readFileSync(path.join(prior,'07_complete_existing_feature_setup.sqlinc'),'utf8').replaceAll('\r\n','\n');
const setupBefore="  if p_request ? 'qualification_action' then";
const setupAfter=`  if p_request->>'qualification_action' in ('OFF_RULE_OPEN','OFF_RULE_CLOSE','OFF_RULE_REVOKE') then
    return private.weekly_source_rule_qualification_setup_atomic_v1(p_request);
  elsif p_request->>'qualification_action'='OFF_RULE_COMMAND' then
    return private.weekly_source_rule_qualification_execute_v1(p_request);
  elsif p_request->>'qualification_action'='OFF_RULE_STATUS' then
    return private.weekly_source_rule_qualification_status_v1(p_request);
  end if;
${setupBefore}`;
const setupHunks=[{before:setupBefore,after:setupAfter},
 {before:"'client_settings_versions','protected_capability_qualification_sha256']",after:"'client_settings_versions','protected_capability_qualification_sha256','activation_probe_scope_id']"},
 {before:"if v_purpose='PROTECTED_SOURCE' and v_enabled and not(p_request ? 'protected_capability_qualification_sha256') then",
  after:"if v_purpose in ('RULE_SET','PROTECTED_SOURCE') and v_enabled and not(p_request ? 'protected_capability_qualification_sha256') and not(p_request ? 'activation_probe_scope_id') then"},
 {before:"  if v_purpose='SIGNED_CHECK_ONLY' and p_request ? 'protected_capability_qualification_sha256' then",
  after:`  if p_request ? 'activation_probe_scope_id' then
    if v_purpose is distinct from 'PROTECTED_SOURCE' or v_enabled is distinct from true or v_version_id is not null
      or p_request ? 'protected_capability_qualification_sha256' then
      raise exception 'WEEKLY_SOURCE_RULE_FEATURE_REQUEST_INVALID' using errcode='22023'; end if;
    perform private.weekly_source_protected_activation_probe_assert_v1((p_request->>'activation_probe_scope_id')::uuid,
      v_actor,v_group_id,v_from,nullif(p_request->>'effective_to_week_ending','')::date,v_assignment_id);
  end if;
  if v_purpose='SIGNED_CHECK_ONLY' and p_request ? 'protected_capability_qualification_sha256' then`},
 {before:`      perform private.weekly_source_protected_operational_certificate_assert_v1(
        v_assignment.feature_receipt_json->'protected_capability_qualification',v_group_id,v_from,v_assignment.effective_to_week_ending,
        v_assignment.feature_receipt_json#>>'{protected_capability_qualification,qualification_sha256}');`,
  after:`      if p_request ? 'activation_probe_scope_id' then
        perform private.weekly_source_protected_activation_probe_assert_v1((p_request->>'activation_probe_scope_id')::uuid,
          v_actor,v_group_id,v_from,v_assignment.effective_to_week_ending,v_assignment_id);
      else
        perform private.weekly_source_protected_operational_certificate_assert_v1(
          v_assignment.feature_receipt_json->'protected_capability_qualification',v_group_id,v_from,v_assignment.effective_to_week_ending,
          v_assignment.feature_receipt_json#>>'{protected_capability_qualification,qualification_sha256}');
      end if;`},
 {before:"  if v_enabled and p_request ? 'protected_capability_qualification_sha256' then",
  after:`  if p_request ? 'activation_probe_scope_id' then
    v_qualification:=private.weekly_source_protected_activation_probe_assert_v1((p_request->>'activation_probe_scope_id')::uuid,
      v_actor,v_group_id,v_from,v_assignment.effective_to_week_ending,v_assignment_id);
  elsif v_enabled and p_request ? 'protected_capability_qualification_sha256' then`}];
let setupValue=setup;for(const h of setupHunks)setupValue=replaceOnce(setupValue,h.before,h.after);
// The request has no effective_to field. The early owner assertion uses the
// server-owned saved assignment's actual end date, exactly as the locked check.
setupValue=replaceOnce(setupValue,"v_actor,v_group_id,v_from,nullif(p_request->>'effective_to_week_ending','')::date,v_assignment_id)",
 "v_actor,v_group_id,v_from,(select effective_to_week_ending from public.weekly_source_group_rule_assignments where id=v_assignment_id),v_assignment_id)");
setupHunks[3].after=setupHunks[3].after.replace("nullif(p_request->>'effective_to_week_ending','')::date",
 "(select effective_to_week_ending from public.weekly_source_group_rule_assignments where id=v_assignment_id)");
save('06_existing_controlled_setup.sqlinc',setup,setupValue,setupHunks);
const cap=definition(inputs,'private.weekly_source_automatic_reconcile_capability_assert_v1');
const capDeclBefore='declare v_ready boolean;',capDeclAfter='declare v_ready boolean; v_auth jsonb;';
const capBefore="    raise exception 'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_DELTA_UNAVAILABLE' using errcode='55000'; end if;\nend;";
const capAfter=`    v_auth:=private.weekly_source_rule_qualification_context_v1(null,p_cycle_id);
    if v_auth is null or v_auth->>'purpose' is distinct from 'AUTOMATIC_JOURNEY' then
      raise exception 'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_DELTA_UNAVAILABLE' using errcode='55000'; end if;
  end if;
end;`;
save('07_automatic_capability_admission.sqlinc',cap,replaceOnce(replaceOnce(cap,capDeclBefore,capDeclAfter),capBefore,capAfter),
 [{before:capDeclBefore,after:capDeclAfter},{before:capBefore,after:capAfter}]);
const casAdmission=definition(fs.readFileSync(path.join(prior,'08_complete_cas_admission.sqlinc'),'utf8').replaceAll('\r\n','\n'),
 'private.weekly_source_bulk_source_cas_assert_v1',params=>!params.includes('p_actor_user_id'));
const casDeclBefore='declare v_ready boolean; v_cas jsonb; v_clients uuid[];',casDeclAfter=casDeclBefore+' v_auth jsonb;';
const casBefore="      raise exception 'WEEKLY_SOURCE_BULK_PRODUCER_UNAVAILABLE' using errcode='55000';\n    end;";
const casAfter=`      v_auth:=private.weekly_source_rule_qualification_context_v1(null,p_cycle_id);
      if v_auth is null or v_auth->>'purpose' is distinct from 'AUTOMATIC_JOURNEY'
        or v_auth#>'{fixture_manifest,client_ids}' is distinct from to_jsonb(v_clients) then
        raise exception 'WEEKLY_SOURCE_BULK_PRODUCER_UNAVAILABLE' using errcode='55000'; end if;
    end;`;
save('08_cas_admission_owner.sqlinc',casAdmission,replaceOnce(replaceOnce(casAdmission,casDeclBefore,casDeclAfter),casBefore,casAfter),
 [{before:casDeclBefore,after:casDeclAfter},{before:casBefore,after:casAfter}]);
fs.writeFileSync(path.join(out,'OWNER_SOURCE_REVERSE_PROOFS.json'),JSON.stringify({source_only:true,root_pin:'ab26a5ff22200be037d215b1100c969d6c0bae73',
 original_operational_reader:{input_sha256:sha(Buffer.from(effective)),renamed_copy_sha256:sha(Buffer.from(operational)),query_bytes_unchanged:true},edits,
 no_original_policy_hash_or_pay_nochange_change:true,no_installed_execution:true},null,2)+'\n');
console.log(JSON.stringify({source_only:true,complete_owner_artifacts:8,entire_source_reverse_proofs:edits.length}));
