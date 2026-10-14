import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';import {fileURLToPath} from 'node:url';
const out=path.dirname(fileURLToPath(import.meta.url)),file=path.join(out,'ACTIVATION_PROBE_SCOPE.sqlinc'),sha=x=>createHash('sha256').update(x).digest('hex');
let source=fs.readFileSync(file,'utf8');if(source.includes('OFF_CLOSED_ORIGINAL_CENSUS_BRIDGE'))process.exit(0);
const before=source,hunks=[];function edit(a,b){assert.equal(source.split(a).length-1,1);source=source.replace(a,()=>b);hunks.push({before:a,after:b});}
edit('actor uuid; n integer; clients uuid[]; old_auth jsonb;','actor uuid; n integer; clients uuid[]; old_auth jsonb; original_census jsonb; original_lease jsonb;');
const a="  if old_auth#>>'{original_census,census_sha256}' is distinct from auth#>>'{assignment_scope,original_census_sha256}' then";
const b=`  -- OFF_CLOSED_ORIGINAL_CENSUS_BRIDGE. The new independent-session path
  -- retains the original census in its real OPEN audit. It is not derived
  -- from whatever OPEN incidents remain after native protected rounds.
  if old_auth->>'schema_version'='WEEKLY_SOURCE_OFF_RULE_TEST_AUTHORIZATION_V1' then
    if old_auth->>'purpose' is distinct from 'AUTOMATIC_JOURNEY' then
      raise exception 'WEEKLY_SOURCE_ACTIVATION_PROBE_ORIGINAL_CENSUS_CHANGED' using errcode='40001'; end if;
    select audit.after_json into strict original_lease from public.audit_events audit
      where audit.object_type='weekly_source_off_rule_test_journey' and audit.object_id_text=old_auth->>'scope_id'
        and audit.action='WEEKLY_SOURCE_OFF_RULE_TEST_OPENED' and audit.event_sequence_is_authoritative
        and audit.after_json->>'authorization_sha256'=old_auth->>'authorization_sha256';
    if original_lease->>'scope_sha256' is distinct from auth#>>'{test_setup_closure,scope_sha256}'
      or original_lease->>'scope_sha256' is distinct from encode(private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_SOURCE_OFF_RULE_TEST_SCOPE_V1',original_lease-'scope_sha256'),'hex')
      or not exists(select 1 from public.audit_events closed where closed.id=(auth#>>'{test_setup_closure,closure_audit_id}')::uuid
        and closed.object_type='weekly_source_off_rule_test_journey' and closed.object_id_text=old_auth->>'scope_id'
        and closed.action='WEEKLY_SOURCE_OFF_RULE_TEST_CLOSED' and closed.event_sequence_is_authoritative
        and closed.after_json=auth->'test_setup_closure'-'closure_audit_id'-'closure_audit_sha256') then
      raise exception 'WEEKLY_SOURCE_ACTIVATION_PROBE_ORIGINAL_CENSUS_CHANGED' using errcode='40001'; end if;
    original_census:=original_lease->'original_census';
    if original_census->'source_cas' is distinct from old_auth->'original_source_cas' then
      raise exception 'WEEKLY_SOURCE_ACTIVATION_PROBE_ORIGINAL_CENSUS_CHANGED' using errcode='40001'; end if;
  else original_census:=old_auth->'original_census'; end if;
  if original_census->>'census_sha256' is distinct from encode(private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_QUALIFICATION_CENSUS_V1',original_census-'census_sha256'),'hex')
    or original_census->>'source_cycle_id' is distinct from old_auth->>'source_cycle_id'
    or original_census->>'census_sha256' is distinct from auth#>>'{assignment_scope,original_census_sha256}' then`;
edit(a,b);
edit("jsonb_array_elements_text(old_auth#>'{original_census,original_client_ids}')","jsonb_array_elements_text(original_census->'original_client_ids')");
edit("is distinct from old_auth#>'{original_census,source_cas}'","is distinct from original_census->'source_cas'");
edit("'original_census',old_auth->'original_census'","'original_census',original_census");
let reverse=source;for(const h of [...hunks].reverse()){assert.equal(reverse.split(h.after).length-1,1);reverse=reverse.replace(h.after,()=>h.before);}assert.equal(reverse,before);
fs.writeFileSync(file,source.trimEnd()+'\n');fs.writeFileSync(path.join(out,'OFF_PROBE_BRIDGE_REVERSE.json'),JSON.stringify({source_only:true,
 input_sha256:sha(before),output_sha256:sha(source),reverse_hunk_matches_entire_input_definition:true,no_runtime_acceptance:true,
 hunks:hunks.map(h=>({before_sha256:sha(h.before),after_sha256:sha(h.after)}))},null,2)+'\n');
