import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {fileURLToPath} from 'node:url';
const out=path.dirname(fileURLToPath(import.meta.url)),sha=x=>createHash('sha256').update(x).digest('hex');
const file=path.join(out,'OFF_RULE_COMMAND.sqlinc');
let source=fs.readFileSync(file,'utf8');
// The generator is idempotent without reconstructing a changed predecessor.
if(source.includes('OFF_RULE_ORIGINAL_CENSUS_CLOSED'))process.exit(0);
const before=source,hunks=[];
function edit(anchor,replacement){assert.equal(source.split(anchor).length-1,1);source=source.replace(anchor,()=>replacement);hunks.push({before:anchor,after:replacement});}
edit('  assignment public.weekly_source_group_rule_assignments%rowtype;',
 '  assignment public.weekly_source_group_rule_assignments%rowtype; census jsonb; clients uuid[]; audit_id uuid;');
edit("    lease:=lease||jsonb_build_object('scope_sha256'",`    if auth->>'purpose'='AUTOMATIC_JOURNEY' then
      -- Capture ONCE, before any protected round. Every remaining Client and
      -- original physical root stays in this immutable audited census.
      census:=private.weekly_source_protected_qualification_census_v1((auth->>'source_cycle_id')::uuid);
      if census->'original_client_ids' is distinct from auth#>'{fixture_manifest,client_ids}'
        or census->'source_cas' is distinct from auth->'original_source_cas'
        or (select coalesce(jsonb_agg(distinct row_value->'contract_id' order by row_value->'contract_id'),'[]'::jsonb)
          from jsonb_array_elements(census->'original_items') row_value) is distinct from auth#>'{fixture_manifest,contract_ids}'
        or (select coalesce(jsonb_agg(distinct row_value->'candidate_id' order by row_value->'candidate_id'),'[]'::jsonb)
          from jsonb_array_elements(census->'original_items') row_value) is distinct from auth#>'{fixture_manifest,candidate_ids}' then
        raise exception 'WEEKLY_SOURCE_OFF_RULE_ORIGINAL_CENSUS_CHANGED' using errcode='40001'; end if;
      lease:=lease||jsonb_build_object('original_census',census);
    end if;
    lease:=lease||jsonb_build_object('scope_sha256'`);
const start=source.indexOf("    result:=jsonb_build_object('schema_version','WEEKLY_SOURCE_OFF_RULE_TEST_CLOSURE_V1'");
const end=source.indexOf("  else result:=",start);assert.ok(start>=0&&end>start);
edit(source.slice(start,end),`    if auth->>'purpose'='AUTOMATIC_JOURNEY' then
      census:=lease->'original_census';
      if census is null or census->>'census_sha256' is distinct from encode(private.weekly_source_sha256_jsonb_v1(
          'WEEKLY_SOURCE_QUALIFICATION_CENSUS_V1',census-'census_sha256'),'hex') then
        raise exception 'WEEKLY_SOURCE_OFF_RULE_ORIGINAL_CENSUS_UNAVAILABLE' using errcode='55000'; end if;
      select array_agg(value::uuid order by value::uuid) into clients from jsonb_array_elements_text(census->'original_client_ids') value;
      if private.weekly_source_bulk_source_cas_v1((auth->>'source_cycle_id')::uuid,clients) is distinct from census->'source_cas' then
        raise exception 'WEEKLY_SOURCE_OFF_RULE_ORIGINAL_CENSUS_CHANGED' using errcode='40001'; end if;
      -- Same closed DTO as the original controlled journey. The closure audit
      -- preserves the OPEN census; it never recollects remaining incidents.
      result:=jsonb_build_object('scope_id',auth->>'scope_id','scope_sha256',lease->>'scope_sha256',
        'setup_authorization_sha256',auth->>'authorization_sha256','original_census_sha256',census->>'census_sha256',
        'actor_user_id',journey,'source_cycle_id',auth->>'source_cycle_id','week_ending',auth->>'week_ending',
        'execution_release_id',auth->>'release_id','execution_source_commit',auth->>'source_commit',
        'execution_contract_sha256',auth->>'contract_sha256','state','CLOSED','closed_at_utc',clock_timestamp(),
        'all_assignments_disabled',true,'all_operational_producers_false',true);
    else
      -- The first genuine Source producer may have no protected incidents.
      -- Its resulting frozen Source CAS is a setup result, not a protected
      -- capability census/certificate or manufactured protection evidence.
      select array_agg(value::uuid order by value::uuid) into clients from jsonb_array_elements_text(auth#>'{fixture_manifest,client_ids}') value;
      result:=jsonb_build_object('schema_version','WEEKLY_SOURCE_OFF_RULE_TEST_SOURCE_CLOSURE_V1','scope_id',auth->>'scope_id',
        'scope_sha256',lease->>'scope_sha256','authorization_sha256',auth->>'authorization_sha256',
        'source_cycle_id',auth->>'source_cycle_id','week_ending',auth->>'week_ending','rule_set_version_id',auth->>'rule_set_version_id',
        'rule_values_sha256',auth->>'rule_values_sha256','result_source_cas',private.weekly_source_bulk_source_cas_v1((auth->>'source_cycle_id')::uuid,clients),
        'actor_user_id',journey,'release_id',auth->>'release_id','source_commit',auth->>'source_commit','contract_sha256',auth->>'contract_sha256',
        'state','CLOSED','closed_at_utc',clock_timestamp(),'all_assignments_disabled',true,'all_operational_producers_false',true);
    end if;
`);
edit("    lease,result,p_request->>'reason',actor);\n  return result;\nend;\n$function$;",`    lease,result,p_request->>'reason',actor);
  if action='OFF_RULE_CLOSE' and auth->>'purpose'='AUTOMATIC_JOURNEY' then
    select audit.id into strict audit_id from public.audit_events audit
      where audit.object_type='weekly_source_off_rule_test_journey' and audit.object_id_text=auth->>'scope_id'
        and audit.action='WEEKLY_SOURCE_OFF_RULE_TEST_CLOSED' and audit.event_sequence_is_authoritative and audit.after_json=result;
    -- OFF_RULE_ORIGINAL_CENSUS_CLOSED: returned identifiers are measured from
    -- the genuine audit row, never supplied by caller or inserted as evidence.
    return result||jsonb_build_object('closure_audit_id',audit_id,'closure_audit_sha256',
      encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_TEST_JOURNEY_CLOSED_V1',result),'hex'));
  end if;
  return result;
end;
$function$;`);
let reversed=source;for(const h of [...hunks].reverse()){assert.equal(reversed.split(h.after).length-1,1);reversed=reversed.replace(h.after,()=>h.before);}assert.equal(reversed,before);
fs.writeFileSync(file,source.trimEnd()+'\n');
fs.writeFileSync(path.join(out,'OFF_CLOSURE_REVERSE.json'),JSON.stringify({source_only:true,input_sha256:sha(before),output_sha256:sha(source),
 reverse_hunk_matches_entire_input_definition:true,original_census_captured_once_before_any_round:true,no_runtime_acceptance:true,
 hunks:hunks.map(h=>({before_sha256:sha(h.before),after_sha256:sha(h.after)}))},null,2)+'\n');
