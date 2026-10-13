import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';import {fileURLToPath} from 'node:url';
const out=path.dirname(fileURLToPath(import.meta.url)),sha=x=>createHash('sha256').update(x).digest('hex');
const source=fs.readFileSync(path.join(out,'SAME_INSTALLED_OPERATIONAL_CERTIFICATE.sqlinc'),'utf8');
const end=source.indexOf('\ncreate or replace function private.weekly_source_protected_execution_tree_assert_v2');assert.ok(end>0);
let value=source.slice(0,end);const original=value,changes=[];
function change(before,after){assert.equal(value.split(before).length-1,1,before);value=value.replace(before,()=>after);changes.push({before,after});}
change('private.weekly_source_protected_operational_certificate_assert_v1(','private.weekly_source_protected_activation_probe_authorization_assert_v1(');
change("'native_execution_qualification','remote_gate_receipts','test_setup_closure','source_equivalence','qualification_sha256']",
 "'native_execution_qualification','remote_gate_receipts','test_setup_closure','source_equivalence','assignment_scope','operational_ready','probe_authorization_sha256']");
change("q->>'schema_version' is distinct from 'WEEKLY_SOURCE_PROTECTED_CAPABILITY_OPERATIONAL_QUALIFICATION_V1'",
 "q->>'schema_version' is distinct from 'WEEKLY_SOURCE_ACTIVATION_PROBE_AUTHORIZATION_V1'");
change("q->>'phase' is distinct from 'ALL_PRE_SWITCH_GATES_QUALIFIED'","q->>'phase' is distinct from 'ALL_OTHER_GATES_QUALIFIED_SECURITY_ACTIVATION_PROBE_ONLY'\n    or q->'operational_ready' is distinct from 'false'::jsonb");
change("q->>'qualification_sha256' is distinct from p_expected_sha256","q->>'probe_authorization_sha256' is distinct from p_expected_sha256");
change("q->>'qualification_sha256' is distinct from encode(private.weekly_source_sha256_jsonb_v1(\n      'WEEKLY_SOURCE_PROTECTED_CAPABILITY_OPERATIONAL_QUALIFICATION_V1',q-'qualification_sha256'),'hex')",
 "q->>'probe_authorization_sha256' is distinct from encode(private.weekly_source_sha256_jsonb_v1(\n      'WEEKLY_SOURCE_ACTIVATION_PROBE_AUTHORIZATION_V1',q-'probe_authorization_sha256'),'hex')");
change("'source_group_id','effective_from_week_ending','effective_to_week_ending','capabilities','gate_receipts','qualification_sha256']",
 "'source_group_id','effective_from_week_ending','effective_to_week_ending','capabilities','gate_receipts','negative_security_receipt','other_gates_sha256']");
change("native->>'schema_version' is distinct from 'WEEKLY_SOURCE_PROTECTED_CAPABILITY_QUALIFICATION_V1'",
 "native->>'schema_version' is distinct from 'WEEKLY_SOURCE_ACTIVATION_PROBE_NATIVE_OTHER_GATES_V1'");
change("native->>'effective_from_week_ending' is distinct from p_from::text","native->>'effective_from_week_ending' is distinct from q#>>'{assignment_scope,native_effective_from_week_ending}'");
change("native->>'effective_to_week_ending' is distinct from p_to::text","native->>'effective_to_week_ending' is distinct from q#>>'{assignment_scope,native_effective_to_week_ending}'");
change("native->>'qualification_sha256' is distinct from encode(private.weekly_source_sha256_jsonb_v1(\n      'WEEKLY_SOURCE_PROTECTED_CAPABILITY_QUALIFICATION_V1',native-'qualification_sha256'),'hex')",
 "native->>'other_gates_sha256' is distinct from encode(private.weekly_source_sha256_jsonb_v1(\n      'WEEKLY_SOURCE_ACTIVATION_PROBE_NATIVE_OTHER_GATES_V1',native-'other_gates_sha256'),'hex')");
change("needed:=array['SOURCE_CAS_NATIVE','NATIVE_PROTECTION','SECURITY','LEGACY_PAY','NEXT_PAY','CLIENT_API','DOCUMENT','OWN_STAGE_CAS','SOURCE_ROUTING'];",
 "needed:=array['SOURCE_CAS_NATIVE','NATIVE_PROTECTION','LEGACY_PAY','NEXT_PAY','CLIENT_API','DOCUMENT','OWN_STAGE_CAS','SOURCE_ROUTING'];");
change("(closure->>'week_ending')::date<p_from or (p_to is not null and (closure->>'week_ending')::date>p_to)",
 "(closure->>'week_ending')::date<(native->>'effective_from_week_ending')::date\n    or (native->>'effective_to_week_ending' is not null and (closure->>'week_ending')::date>(native->>'effective_to_week_ending')::date)");
const anchor="  needed:=array['SOURCE_CAS_NATIVE','NATIVE_PROTECTION','LEGACY_PAY','NEXT_PAY','CLIENT_API','DOCUMENT','OWN_STAGE_CAS','SOURCE_ROUTING'];";
const inserted=`  row_value:=native->'negative_security_receipt';
  if not private.weekly_source_qualification_closed_object_v1(row_value,array['gate','status','execution','database','receipt_id','receipt_sha256',
    'source_commit','contract_sha256','source_group_id','agency_id','environment'])
    or row_value->>'gate' is distinct from 'SECURITY_NEGATIVES' or row_value->>'status' is distinct from 'NEGATIVE_PASS'
    or row_value->>'execution' is distinct from 'AGENCY_TEST_NATIVE' or row_value->>'database' is distinct from current_database()
    or row_value->>'source_commit' is distinct from executed.git_commit or row_value->>'contract_sha256' is distinct from executed.installed_contract_sha256
    or row_value->>'source_group_id' is distinct from g.id::text or row_value->>'agency_id' is distinct from g.agency_id::text
    or row_value->>'environment' is distinct from 'TEST' or coalesce(row_value->>'receipt_id','') !~ '^[A-Za-z0-9_./:@-]{1,256}$'
    or coalesce(row_value->>'receipt_sha256','') !~ '^[0-9a-f]{64}$' then
    raise exception 'WEEKLY_SOURCE_ACTIVATION_PROBE_OTHER_GATES_REQUIRED' using errcode='55000'; end if;
${anchor}`;
change(anchor,inserted);
const endAnchor="  perform private.weekly_source_protected_execution_tree_assert_v2(q->'source_equivalence',executed.git_commit,r.git_commit);";
change(endAnchor,`  if not private.weekly_source_qualification_closed_object_v1(q->'assignment_scope',array['scope_id','scope_sha256','original_census_sha256',
    'setup_actor_user_id','assignment_id','expected_assignment_version','expected_assignment_sha256','source_group_id','effective_from_week_ending',
    'effective_to_week_ending','rule_set_version_id','native_effective_from_week_ending','native_effective_to_week_ending'])
    or q#>'{assignment_scope,rule_set_version_id}' is distinct from 'null'::jsonb
    or q#>>'{assignment_scope,source_group_id}' is distinct from g.id::text
    or q#>>'{assignment_scope,effective_from_week_ending}' is distinct from p_from::text
    or q#>>'{assignment_scope,effective_to_week_ending}' is distinct from p_to::text
    or q#>>'{assignment_scope,original_census_sha256}' is distinct from closure->>'original_census_sha256'
    or p_from<(native->>'effective_from_week_ending')::date then
    raise exception 'WEEKLY_SOURCE_ACTIVATION_PROBE_ASSIGNMENT_INVALID' using errcode='55000'; end if;
${endAnchor}`);
let reverse=value;for(const h of [...changes].reverse()){assert.equal(reverse.split(h.after).length-1,1);reverse=reverse.replace(h.after,()=>h.before);}assert.equal(reverse,original);
fs.writeFileSync(path.join(out,'ACTIVATION_PROBE_AUTHORIZATION_ASSERT.sqlinc'),value.trimEnd()+'\n');
fs.writeFileSync(path.join(out,'ACTIVATION_PROBE_ASSERT_REVERSE.json'),JSON.stringify({source_only:true,input_sha256:sha(original),output_sha256:sha(value),
 reverse_hunk_matches_entire_input_definition:true,full_remote_provenance_checks_retained:true,actual_probe:false,operational_ready:false,
 changes:changes.map(h=>({before_sha256:sha(h.before),after_sha256:sha(h.after)}))},null,2)+'\n');
console.log(JSON.stringify({source_only:true,full_remote_sql_validator:true,reverse_hunk_matches_entire_input_definition:true,actual_probe:false}));
