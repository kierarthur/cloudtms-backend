import { readFileSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { createHash } from 'node:crypto';
const base = path.dirname(fileURLToPath(import.meta.url));
const input = 'native-entry/weekly_exceptional_pay_publish_next_v1.sqlinc';
const original = readFileSync(path.join(base, input), 'utf8');
let sql = original;const edits = [];
const sha = value => createHash('sha256').update(value).digest('hex');
function change(before, after) {
  if (sql.split(before).length !== 2) throw new Error(`NEXT native seam changed: ${before.slice(0, 100)}`);
  sql = sql.replace(before, after);edits.push({ before, after });
}
change('public.weekly_exceptional_pay_publish_next_v1(p_request jsonb)',
  'private.weekly_source_protected_next_engine_v1(p_request jsonb,p_source_decision_preparation_id uuid,p_validate_only boolean)');
change('  v_source_gate jsonb;', '  v_source_gate jsonb;v_source_application_id uuid;v_source_result jsonb;v_source_control jsonb;');
change('begin\n  if coalesce', `begin
  if p_validate_only is null or (p_validate_only and p_source_decision_preparation_id is null) then
    raise exception 'SOURCE_NATIVE_VALIDATION_SCOPE_REQUIRED' using errcode='22023';end if;
  if coalesce`);
change("     or not exists(select 1 from public.tms_users u where u.id=v_actor and u.is_active\n       and (u.payment_authoriser or u.payment_golden_key)) then",
  '     or not private.weekly_source_pay_actor_allowed_v1(v_actor) then');
change("  v_source_gate:=private.weekly_source_pay_decision_native_normal_gate_v1(v_family.id,v_run.id,v_actor,'PUBLISH_NEXT');\n  if v_source_gate is not null then return v_source_gate; end if;",
  `  if p_source_decision_preparation_id is null then
    v_source_gate:=private.weekly_source_pay_decision_native_normal_gate_v1(v_family.id,v_run.id,v_actor,'PUBLISH_NEXT');
    if v_source_gate is not null then return v_source_gate; end if;
  else
    v_source_application_id:=private.weekly_source_pay_decision_native_request_scope_v1(
      p_source_decision_preparation_id,p_request,'NEXT',not p_validate_only);
  end if;`);
change('  insert into public.weekly_exceptional_payment_approvals(pay_target_family_id,work_event_id,evidence_timesheet_id,',
  `  if p_validate_only then
    return jsonb_build_object('version','SOURCE_NATIVE_VALIDATED_REQUEST_V1','route','NEXT',
      'family_id',v_family.id,'orchestration_run_id',v_run.id,'native_request_sha256',encode(v_request_hash,'hex'),
      'target_snapshot',p_request->'target_snapshot');
  end if;
  insert into public.weekly_exceptional_payment_approvals(pay_target_family_id,work_event_id,evidence_timesheet_id,`);
change('  select p.* into strict v_publication from private.bpay_next_publish_source_staged_pair_v1(\n    array[v_stage.work_id],array[v_stage.revision_id],array[v_approval.id]) p;',
  `  if p_source_decision_preparation_id is null then
    select p.* into strict v_publication from private.bpay_next_publish_source_staged_pair_v1(
      array[v_stage.work_id],array[v_stage.revision_id],array[v_approval.id]) p;
  end if;`);
const receiptStart = '  insert into private.bpay_next_protected_source_receipt(orchestration_run_id,family_id,actor_user_id,approval_id,generation_id,';
const receiptEnd = '    v_publication.agency_sequence,v_family.bound_version+1,v_run.request_fingerprint,v_request_hash,v_source_hash);';
const a = sql.indexOf(receiptStart);const b = sql.indexOf(receiptEnd, a) + receiptEnd.length;
if (a < 0 || b < a) throw new Error('NEXT immutable receipt seam changed');
const receipt = sql.slice(a, b);
change(receipt, `  if p_source_decision_preparation_id is null then\n${receipt}\n  else
    v_source_control:=jsonb_build_object('family_id',v_family.id,'orchestration_run_id',v_run.id,
      'approval_id',v_approval.id,'generation_id',v_generation.id,'work_id',v_stage.work_id,'revision_id',v_stage.revision_id,
      'new_family_bound_version',(v_family.bound_version+1)::text,
      'approved_pay_ex_vat',to_char(v_total,'FM9999999999999990.00'),'source_manifest_sha256',encode(v_source_hash,'hex'));
    select jsonb_build_object('version','SOURCE_NATIVE_COMPOSED_RESULT_V1','application_id',v_source_application_id,
      'decision_id',p.decision_id,'request_id',p.accept_or_resume_request_id,'review_id',p.review_id,
      'member_key',p.member_key,'preparation_id',p.id,'route','NEXT','origin','APPLIED_IN_THIS_TRANSACTION',
      'control_status','WITHDRAWN','native_receipt',v_source_control,'failure_code',null)
      into strict v_source_result from private.weekly_source_pay_decision_unprotect_preparations p where p.id=p_source_decision_preparation_id;
    insert into public.weekly_exceptional_payment_events(family_id,approval_id,event_kind,lifecycle_view,bounded_payload_json,idempotency_key)
      values(v_family.id,v_approval.id,'PROTECTED_STATE_OBSERVED','LIVE',v_source_result,v_key||':payment-event');
    select coalesce(max(s.sequence),0)+1 into v_step from public.weekly_exceptional_orchestration_steps s where s.orchestration_run_id=v_run.id;
    insert into public.weekly_exceptional_orchestration_steps(orchestration_run_id,sequence,step_kind,idempotency_key,
      allowlisted_owner_name,allowlisted_owner_signature,bounded_request_hash,before_state_fingerprint,bounded_owner_response_json,
      owner_response_hash,after_state_fingerprint,outcome,completed_at_utc)
      values(v_run.id,v_step,'PUBLISH_NEXT',v_key,'private.weekly_exceptional_pay_publish_next_impl_v1','jsonb,uuid->jsonb',
        v_request_hash,v_run.before_state_fingerprint,v_source_result,
        private.weekly_source_sha256_jsonb_v1('SOURCE_NATIVE_COMPOSED_RESULT_V1',v_source_result),v_request_hash,'COMPLETE',transaction_timestamp());
    return private.weekly_source_pay_decision_record_native_adoption_v1(
      p_source_decision_preparation_id,v_source_application_id,v_source_control);
  end if;`);
// Whole-body reversal restores every original normal financial statement. The
// Source arm alone omits standalone command/old receipt and records real IDs.
let reversed = sql;
for (const edit of edits.toReversed()) reversed = reversed.replace(edit.after, edit.before);
if (reversed !== original) throw new Error('NEXT engine whole-owner reverse failed');
sql += `
revoke all on function private.weekly_source_protected_next_engine_v1(jsonb,uuid,boolean)
 from public,anon,authenticated,service_role;

create or replace function private.weekly_source_pay_decision_native_next_payload_validate_v1(p_request jsonb,p_preparation_id uuid)
returns jsonb language sql volatile security definer set search_path=pg_catalog,private,public as $function$
 select private.weekly_source_protected_next_engine_v1(p_request,p_preparation_id,true);
$function$;
revoke all on function private.weekly_source_pay_decision_native_next_payload_validate_v1(jsonb,uuid)
 from public,anon,authenticated,service_role;

create or replace function private.weekly_exceptional_pay_publish_next_impl_v1(p_request jsonb,p_source_decision_preparation_id uuid)
returns jsonb language sql volatile security definer set search_path=pg_catalog,private,public as $function$
 select private.weekly_source_protected_next_engine_v1(p_request,p_source_decision_preparation_id,false);
$function$;
revoke all on function private.weekly_exceptional_pay_publish_next_impl_v1(jsonb,uuid)
 from public,anon,authenticated,service_role;

create or replace function public.weekly_exceptional_pay_publish_next_v1(p_request jsonb)
returns jsonb language sql volatile security definer set search_path=pg_catalog,private,public as $function$
 select private.weekly_exceptional_pay_publish_next_impl_v1(p_request,null);
$function$;
revoke all on function public.weekly_exceptional_pay_publish_next_v1(jsonb) from public,anon,authenticated;
grant execute on function public.weekly_exceptional_pay_publish_next_v1(jsonb) to service_role;
notify pgrst,'reload schema';
`;
writeFileSync(path.join(base, 'forward/16_native_next_implementation.sqlinc'), sql);
writeFileSync(path.join(base, 'NATIVE_NEXT_IMPLEMENTATION_RECEIPT.json'), JSON.stringify({ input, input_sha256: sha(original),
  output: 'forward/16_native_next_implementation.sqlinc', output_sha256: sha(sql), complete_reverse_equals_input: true,
  ordinary_body_changes: 'Shared active actor predicate; otherwise every ordinary native statement retained under NULL preparation.',
  source_control: 'Actual native writes and Banking protected stage; suppress standalone publication + old receipt only. Genuine application/adoption required.',
  moved_next_dependency: 'Existing Banking stage_protected current-B producer qualification remains required; no Banking body changed.',
  validation_mode: 'Private owner-only; same complete native preflight; returns before first financial write.',
  qualification: 'SOURCE_ONLY_NATIVE_REQUIRED_NOT_RUN', edits }, null, 2) + '\n');
process.stdout.write('Generated actual NEXT engine/private implementation/NULL wrapper; complete reverse passes.\n');
