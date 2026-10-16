import { readFileSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { createHash } from 'node:crypto';
const base = path.dirname(fileURLToPath(import.meta.url));
const input = 'native-entry/weekly_exceptional_pay_stage_c1_request_v1.sqlinc';
const original = readFileSync(path.join(base, input), 'utf8');
let sql = original;const edits = [];
const sha = value => createHash('sha256').update(value).digest('hex');
function change(before, after) {
  if (sql.split(before).length !== 2) throw new Error(`Local native stage seam changed: ${before.slice(0, 100)}`);
  sql = sql.replace(before, after);edits.push({ before, after });
}
change('public.weekly_exceptional_pay_stage_c1_request_v1(\n  p_request jsonb\n)',
  'private.weekly_source_protected_local_stage_engine_v1(\n  p_request jsonb,p_source_decision_preparation_id uuid,p_validate_only boolean\n)');
change('  v_source_gate jsonb;', '  v_source_gate jsonb;v_source_application_id uuid;v_placement jsonb;v_checked_placement jsonb;');
const begin = sql.indexOf('\nbegin\n');
if (begin < 0) throw new Error('Missing actual stage body');
const before = sql.slice(begin, begin + '\nbegin\n'.length);
change(before, `
begin
  if p_validate_only is null or (p_validate_only and p_source_decision_preparation_id is null) then
    raise exception 'SOURCE_NATIVE_VALIDATION_SCOPE_REQUIRED' using errcode='22023';end if;
`);
change("  perform private.weekly_source_pay_decision_native_normal_gate_v1(v_family.id,v_run.id,v_actor_user_id,'STAGE_LOCAL');",
  `  if p_source_decision_preparation_id is null then
    perform private.weekly_source_pay_decision_native_normal_gate_v1(v_family.id,v_run.id,v_actor_user_id,'STAGE_LOCAL');
  else
    v_source_application_id:=private.weekly_source_pay_decision_native_request_scope_v1(
      p_source_decision_preparation_id,p_request,'STAGE_LOCAL',not p_validate_only);
  end if;`);
change('  -- SP-12: existing admission only; no producer/version evidence.', `  -- Source ownership is checked before BOTH native staged-request replay
  -- branches. Their original ordinary behavior remains byte-for-byte below.
  if p_source_decision_preparation_id is not null then
    v_source_application_id:=private.weekly_source_pay_decision_native_request_scope_v1(
      p_source_decision_preparation_id,p_request,'STAGE_LOCAL',not p_validate_only);
  elsif exists(select 1 from private.weekly_source_pay_decision_unprotect_preparations p where p.orchestration_run_id=v_run_id)
    or exists(select 1 from private.weekly_source_pay_decision_native_adoptions a where a.orchestration_run_id=v_run_id) then
    perform private.weekly_source_pay_decision_native_normal_gate_v1(v_family_id,v_run_id,v_actor_user_id,'STAGE_LOCAL');
  end if;
  -- SP-12: existing admission only; no producer/version evidence.`);
change('  v_lock_family:=private.weekly_source_protected_family_lock_v1(v_family_id);', `  v_placement:=private.weekly_source_protected_current_placement_v1(v_family_id,v_work_event_id,false);
  if v_placement->'moved'='true'::jsonb then
    perform private.timesheet_contract_move_family_lock_v1((v_placement->>'original_root_timesheet_id')::uuid);
    select jsonb_build_object('root_timesheet_id',f.root_timesheet_id,'candidate_id',f.candidate_id,
      'contract_id',f.contract_id,'week_ending_date',f.week_ending_date) into strict v_lock_family
      from public.weekly_exceptional_pay_target_families f where f.id=v_family_id;
  else v_lock_family:=private.weekly_source_protected_family_lock_v1(v_family_id);end if;
  v_checked_placement:=private.weekly_source_protected_current_placement_v1(v_family_id,v_work_event_id,false);
  if v_checked_placement is distinct from v_placement then
    raise exception 'WEEKLY_SOURCE_PROTECTED_LOCK_SCOPE_CHANGED' using errcode='40001';end if;`);
change('  where timesheet.timesheet_id=v_family.root_timesheet_id\n  for share;',
  "  where timesheet.timesheet_id=(v_placement->>'current_timesheet_id')::uuid\n  for share;");
change('  if v_root.contract_id is distinct from v_family.contract_id',
  "  if v_root.contract_id::text is distinct from v_placement->>'current_contract_id'");
for (const prefix of ["(v_start->>'contract_id')::uuid", "(v_source_entry->>'contract_id')::uuid", "(v_source_document->>'contract_id')::uuid"]) {
  change(`${prefix} is distinct from v_family.contract_id`, `${prefix} is distinct from v_root.contract_id`);
}
change('      and evidence.is_current and evidence.revoked_at is null\n      and evidence.archived_at_utc is null;',
  "      and (evidence.is_current or v_placement->'moved'='true'::jsonb) and evidence.revoked_at is null\n      and (evidence.archived_at_utc is null or v_placement->'moved'='true'::jsonb);");
change('    v_signed_evidence:=private.weekly_exceptional_candidate_signed_evidence_v1(\n      v_evidence_timesheet_id\n    );',
  '    v_signed_evidence:=private.weekly_source_protected_signed_evidence_v1(v_evidence_timesheet_id,v_family.id,v_event.id);');
change("    elsif v_source_kind='ROOT_FINANCIAL'\n       and (v_source_document->>'financial_row_id')::uuid is distinct from v_fin.id then",
  `    elsif v_source_kind='ROOT_FINANCIAL' and (
       (v_source_document->>'financial_row_id')::uuid is distinct from v_fin.id
       or (v_placement->'moved'='true'::jsonb and (
        v_source_document->>'financial_revision_digest' is distinct from encode(private.weekly_source_sha256_jsonb_v1(
          'WEEKLY_PROTECTED_ROOT_FINANCIAL_V1',to_jsonb(v_fin)),'hex')
        or v_source_entry->>'source_document_sha256' is distinct from encode(private.weekly_source_sha256_jsonb_v1(
          'WEEKLY_PROTECTED_ROOT_FINANCIAL_V1',to_jsonb(v_fin)),'hex')
        or v_source_document->>'root_version' is distinct from v_root.version::text
        or v_source_document->>'financial_timesheet_version' is distinct from v_fin.timesheet_version::text))) then`);
change("  if v_component_identity_context->>'root_timesheet_id' is distinct from v_root.timesheet_id::text",
  "  v_component_identity_context:=private.weekly_source_protected_entitlement_projection_v1(v_component_identity_context);\n  if v_component_identity_context->>'root_timesheet_id' is distinct from v_root.timesheet_id::text");
change("  v_next_vector:=v_next_vector||jsonb_build_object(\n    'protected_component_identity_basis',v_component_identity_basis);",
  "  v_next_vector:=v_next_vector||jsonb_build_object(\n    'protected_component_identity_basis',v_component_identity_basis);\n  if v_placement->'moved'='true'::jsonb then\n    v_next_vector:=v_next_vector||jsonb_build_object('accepted_current_placement',v_placement);end if;");
change('    v_family.candidate_id,v_family.contract_id,v_root.timesheet_id,',
  '    v_family.candidate_id,v_root.contract_id,v_root.timesheet_id,');
change('      v_family.candidate_id,v_family.contract_id,\n      private.weekly_exceptional_hex_sha256_v1',
  '      v_family.candidate_id,v_root.contract_id,\n      private.weekly_exceptional_hex_sha256_v1');
change('  insert into public.weekly_exceptional_payment_approvals(\n', `  if p_validate_only then
    return jsonb_build_object('version','SOURCE_NATIVE_VALIDATED_REQUEST_V1','route','SOURCE_LOCAL',
      'family_id',v_family.id,'orchestration_run_id',v_run.id,
      'native_request_sha256',encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_C1_STAGE_REQUEST_BODY_V1',p_request),'hex'),
      'target_snapshot',v_target_snapshot,'component_identity_basis',v_component_identity_basis);
  end if;
  insert into public.weekly_exceptional_payment_approvals(
`);
let reversed = sql;
for (const edit of edits.toReversed()) reversed = reversed.replace(edit.after, edit.before);
if (reversed !== original) throw new Error('Local stage complete reverse failed');
sql += `
revoke all on function private.weekly_source_protected_local_stage_engine_v1(jsonb,uuid,boolean)
 from public,anon,authenticated,service_role;

create or replace function private.weekly_source_pay_decision_native_local_payload_validate_v1(p_request jsonb,p_preparation_id uuid)
returns jsonb language sql volatile security definer set search_path=pg_catalog,private,public as $function$
 select private.weekly_source_protected_local_stage_engine_v1(p_request,p_preparation_id,true);
$function$;
revoke all on function private.weekly_source_pay_decision_native_local_payload_validate_v1(jsonb,uuid)
 from public,anon,authenticated,service_role;

create or replace function private.weekly_exceptional_pay_stage_c1_request_impl_v1(p_request jsonb,p_source_decision_preparation_id uuid)
returns jsonb language plpgsql volatile security definer set search_path=pg_catalog,private,public as $function$
declare result jsonb;prep private.weekly_source_pay_decision_unprotect_preparations%rowtype;
begin
 result:=private.weekly_source_protected_local_stage_engine_v1(p_request,p_source_decision_preparation_id,false);
 if p_source_decision_preparation_id is null then return result;end if;
 select p.* into strict prep from private.weekly_source_pay_decision_unprotect_preparations p where p.id=p_source_decision_preparation_id;
 if result->'ok' is distinct from 'true'::jsonb
  or result->>'publication_request_id' is distinct from prep.payload_json#>>'{complete_request,publication_request_id}'
  or result->>'request_sha256' is distinct from prep.payload_json#>>'{complete_request,expected_request_sha256}' then
  raise exception 'SOURCE_NATIVE_LOCAL_STAGE_RESULT_INVALID' using errcode='23514';end if;
 -- Both real native owners share this one outer ACCEPT transaction. Source
 -- callers receive only the final exact composed result, never a staged label.
 return private.weekly_exceptional_pay_complete_local_impl_v1(prep.payload_json->'complete_request',prep.id);
end;
$function$;
revoke all on function private.weekly_exceptional_pay_stage_c1_request_impl_v1(jsonb,uuid)
 from public,anon,authenticated,service_role;

create or replace function public.weekly_exceptional_pay_stage_c1_request_v1(p_request jsonb)
returns jsonb language sql volatile security definer set search_path=pg_catalog,private,public as $function$
 select private.weekly_exceptional_pay_stage_c1_request_impl_v1(p_request,null);
$function$;
revoke all on function public.weekly_exceptional_pay_stage_c1_request_v1(jsonb) from public,anon,authenticated;
grant execute on function public.weekly_exceptional_pay_stage_c1_request_v1(jsonb) to service_role;
notify pgrst,'reload schema';
`;
writeFileSync(path.join(base, 'forward/20_native_local_stage_implementation.sqlinc'), sql);
writeFileSync(path.join(base, 'NATIVE_LOCAL_STAGE_RECEIPT.json'), JSON.stringify({ input, input_sha256: sha(original),
  output: 'forward/20_native_local_stage_implementation.sqlinc', output_sha256: sha(sql), complete_reverse_equals_input: true,
  ordinary_body_changes: 0, source_effect: 'Real complete native stage then commissioned private complete_local in the same outer transaction.',
  validation_mode: 'Private owner-only; complete actual native preflight returns before first approval INSERT.',
  qualification: 'SOURCE_ONLY_NATIVE_REQUIRED_NOT_RUN', edits }, null, 2) + '\n');
process.stdout.write('Generated real Local stage/private implementation/NULL delegate; complete reverse passes.\n');
