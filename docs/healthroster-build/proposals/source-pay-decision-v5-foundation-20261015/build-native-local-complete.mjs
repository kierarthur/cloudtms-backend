import { readFileSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { createHash } from 'node:crypto';
const base = path.dirname(fileURLToPath(import.meta.url));
const input = 'native-entry/weekly_exceptional_pay_complete_local_v1.sqlinc';
const original = readFileSync(path.join(base, input), 'utf8');
let sql = original;const edits = [];
const sha = value => createHash('sha256').update(value).digest('hex');
function change(before, after) {
  if (sql.split(before).length !== 2) throw new Error(`Local completion seam changed: ${before.slice(0, 100)}`);
  sql = sql.replace(before, after);edits.push({ before, after });
}
change('public.weekly_exceptional_pay_complete_local_v1(p_request jsonb)',
  'private.weekly_exceptional_pay_complete_local_impl_v1(p_request jsonb,p_source_decision_preparation_id uuid)');
change('  v_source_gate jsonb;', `  v_source_gate jsonb;v_source_application_id uuid;v_source_control jsonb;v_source_entitlement jsonb;
  v_placement jsonb;v_checked_placement jsonb;
  v_source_head public.weekly_source_entitlement_heads%rowtype;
  v_source_receipt private.weekly_source_entitlement_publication_receipts%rowtype;
  v_source_pending public.weekly_source_pending_entitlement_bundles%rowtype;`);
change(`  v_source_gate:=private.weekly_source_pay_decision_native_normal_gate_v1(
    v_request.family_id,v_request.orchestration_run_id,v_actor,'COMPLETE_LOCAL');
  if v_source_gate is not null then return v_source_gate; end if;`,
`  if p_source_decision_preparation_id is null then
    v_source_gate:=private.weekly_source_pay_decision_native_normal_gate_v1(
      v_request.family_id,v_request.orchestration_run_id,v_actor,'COMPLETE_LOCAL');
    if v_source_gate is not null then return v_source_gate; end if;
  else
    v_source_application_id:=private.weekly_source_pay_decision_native_request_scope_v1(
      p_source_decision_preparation_id,p_request,'COMPLETE_LOCAL',true);
  end if;`);
change(`  v_locks:=private.weekly_source_lock_and_resolve_families_v1(v_request.candidate_id,
    array[v_request.root_timesheet_id],'WORKBENCH_CANDIDATE_ENTITLEMENT_PUBLICATION',
    v_request.id,'WEEKLY_SOURCE_ENTITLEMENT_PUBLICATION');`,
`  v_placement:=private.weekly_source_protected_current_placement_v1(v_request.family_id,
    (select a.work_event_id from public.weekly_exceptional_payment_approvals a
     where a.creation_orchestration_run_id=v_request.orchestration_run_id and a.pay_target_family_id=v_request.family_id),false);
  if v_placement->'moved'='true'::jsonb then
    perform private.timesheet_contract_move_family_lock_v1((v_placement->>'original_root_timesheet_id')::uuid);
    v_locks:=jsonb_build_object('ok',true);
  else
    v_locks:=private.weekly_source_lock_and_resolve_families_v1(v_request.candidate_id,
      array[v_request.root_timesheet_id],'WORKBENCH_CANDIDATE_ENTITLEMENT_PUBLICATION',
      v_request.id,'WEEKLY_SOURCE_ENTITLEMENT_PUBLICATION');
  end if;
  v_checked_placement:=private.weekly_source_protected_current_placement_v1(v_request.family_id,
    (select a.work_event_id from public.weekly_exceptional_payment_approvals a
     where a.creation_orchestration_run_id=v_request.orchestration_run_id and a.pay_target_family_id=v_request.family_id),false);
  if v_checked_placement is distinct from v_placement then
    raise exception 'WEEKLY_SOURCE_PROTECTED_LOCK_SCOPE_CHANGED' using errcode='40001';end if;`);
change('  select * into strict v_root from public.timesheets where timesheet_id=v_family.root_timesheet_id for update;',
  "  select * into strict v_root from public.timesheets where timesheet_id=(v_placement->>'current_timesheet_id')::uuid for update;");
change('     or v_root.contract_id is distinct from v_family.contract_id',
  "     or v_root.contract_id::text is distinct from v_placement->>'current_contract_id'\n     or v_request.root_timesheet_id is distinct from v_root.timesheet_id or v_request.contract_id is distinct from v_root.contract_id");
change("          is distinct from v_family.contract_id::text", "          is distinct from v_root.contract_id::text");
const anchor = '  -- Finish the actual completion evidence before any query resolution or';
change(anchor, `  if p_source_decision_preparation_id is not null then
    -- Actual native result/step are complete. Source's selected contact/query
    -- retirement follows canonical Source publication in the outer owner.
    select r.* into strict v_receipt from private.weekly_source_local_protected_decision_receipts r
      where r.publication_request_id=v_request.id;
    if v_result->>'authority'='COMMON_ENTITLEMENT_HEAD' then
      if v_pending then
        select p.* into strict v_source_pending from public.weekly_source_pending_entitlement_bundles p
          where p.id::text=v_result#>>'{publication,pending_bundle_verified}';
        v_source_entitlement:=jsonb_build_object('kind','PENDING_BUNDLE','pending_bundle_id',v_source_pending.id,
          'decision_bundle_id',v_source_pending.decision_bundle_id,'bundle_revision',v_source_pending.bundle_revision::text);
      else
        select r.* into strict v_source_receipt from private.weekly_source_entitlement_publication_receipts r
          where r.decision_bundle_id=v_receipt.common_decision_bundle_id and r.bundle_revision=v_receipt.common_bundle_revision
            and r.member_root_ids=array[v_receipt.root_timesheet_id] and r.candidate_id=v_family.candidate_id;
        select h.* into strict v_source_head from public.weekly_source_entitlement_heads h
          where h.id=v_source_receipt.head_ids[1] and h.root_timesheet_id=v_receipt.root_timesheet_id;
        v_source_entitlement:=jsonb_build_object('kind','COMMITTED_HEAD','decision_bundle_id',v_source_receipt.decision_bundle_id,
          'bundle_revision',v_source_receipt.bundle_revision::text,'source_head_id',v_source_head.id,'publication_receipt_id',v_source_receipt.id);
      end if;
    end if;
    v_source_control:=jsonb_build_object('family_id',v_family.id,'orchestration_run_id',v_run.id,'approval_id',v_approval.id,
      'generation_id',v_generation.id,'publication_request_id',v_request.id,'new_family_bound_version',v_result->>'family_bound_version',
      'local_state',v_receipt.state,'authority',v_result->>'authority','requires_first_authorisation',v_result->'requires_first_authorisation',
      'timesheet_financials_id',v_result->'timesheet_financials_id','entitlement',v_source_entitlement,
      'local_receipt_sha256',encode(private.weekly_source_sha256_jsonb_v1('SOURCE_NATIVE_LOCAL_RECEIPT_ROW_V1',to_jsonb(v_receipt)),'hex'),
      'native_result_sha256',encode(v_result_hash,'hex'));
    return private.weekly_source_pay_decision_record_native_adoption_v1(
      p_source_decision_preparation_id,v_source_application_id,v_source_control);
  end if;
` + anchor);
let reversed = sql;
for (const edit of edits.toReversed()) reversed = reversed.replace(edit.after, edit.before);
if (reversed !== original) throw new Error('Local completion complete reverse failed');
sql += `
revoke all on function private.weekly_exceptional_pay_complete_local_impl_v1(jsonb,uuid) from public,anon,authenticated,service_role;
create or replace function public.weekly_exceptional_pay_complete_local_v1(p_request jsonb)
returns jsonb language sql volatile security definer set search_path=pg_catalog,private,public as $function$
 select private.weekly_exceptional_pay_complete_local_impl_v1(p_request,null);
$function$;
revoke all on function public.weekly_exceptional_pay_complete_local_v1(jsonb) from public,anon,authenticated;
grant execute on function public.weekly_exceptional_pay_complete_local_v1(jsonb) to service_role;
notify pgrst,'reload schema';
`;
writeFileSync(path.join(base, 'forward/21_native_local_complete_implementation.sqlinc'), sql);
writeFileSync(path.join(base, 'NATIVE_LOCAL_COMPLETE_RECEIPT.json'), JSON.stringify({ input, input_sha256: sha(original),
  output: 'forward/21_native_local_complete_implementation.sqlinc', output_sha256: sha(sql), complete_reverse_equals_input: true,
  ordinary_body_changes: 0, source_effect: 'Real native TSFIN/common head/pending result and original completion step; exact control/adoption after actual completion, before selected contact/query retirement.',
  qualification: 'SOURCE_ONLY_NATIVE_REQUIRED_NOT_RUN', edits }, null, 2) + '\n');
process.stdout.write('Generated actual Local completion/private implementation/NULL delegate; complete reverse passes.\n');
