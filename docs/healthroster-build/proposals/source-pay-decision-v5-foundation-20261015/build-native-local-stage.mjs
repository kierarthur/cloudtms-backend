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
change('  v_source_gate jsonb;', '  v_source_gate jsonb;v_source_application_id uuid;');
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
