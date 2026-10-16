import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const base = path.dirname(fileURLToPath(import.meta.url));
const commit = 'a934ee1d460b444939234e812babccb90dc3ddff';
const input = 'supabase/repeatable/14102026_0410_healthroster_protected_source_prerequisites/44_q45_actor_event_closed_dto_cross_session_12_complete_actor_19_action_context.sqlinc';
const source = execFileSync('git', ['show', `${commit}:${input}`], { encoding: 'utf8', maxBuffer: 8 * 1024 * 1024 });
const sha = text => createHash('sha256').update(text).digest('hex');
const edits = [];
let text = source;
function change(before, after) {
  if (text.split(before).length !== 2) throw new Error(`Native context anchor changed: ${before.slice(0, 80)}`);
  text = text.replace(before, after); edits.push({ before, after });
}
change('public.weekly_exceptional_pay_action_context_v1(\n  p_request jsonb\n)',
  'private.weekly_source_protected_action_facts_v1(\n  p_request jsonb,p_request_kind text\n)');
change("    'schema_version','actor_user_id','family_id','orchestration_run_id',", "    'schema_version','actor_user_id','family_id',");
change('  v_run_id uuid;\n', '');
change('  v_run public.weekly_exceptional_orchestration_runs%rowtype;\n',
  '  v_placement jsonb;v_checked_placement jsonb;v_current_entitlement jsonb;\n');
change("p_request->>'schema_version'<>'WEEKLY_PROTECTED_ACTION_CONTEXT_V1'", "p_request->>'schema_version'<>'WEEKLY_PROTECTED_ACTION_FACTS_V1'");
change("    v_run_id:=(p_request->>'orchestration_run_id')::uuid;\n", '');
change('  v_lock_family:=private.weekly_source_protected_family_lock_v1(v_family_id);',
  `  v_placement:=private.weekly_source_protected_current_placement_v1(v_family_id,v_event_id,false);
  if v_placement->'moved'='true'::jsonb then
    perform private.timesheet_contract_move_family_lock_v1((v_placement->>'original_root_timesheet_id')::uuid);
    select jsonb_build_object('root_timesheet_id',f.root_timesheet_id,'candidate_id',f.candidate_id,
      'contract_id',f.contract_id,'week_ending_date',f.week_ending_date) into strict v_lock_family
      from public.weekly_exceptional_pay_target_families f where f.id=v_family_id;
  else
    v_lock_family:=private.weekly_source_protected_family_lock_v1(v_family_id);
  end if;
  v_checked_placement:=private.weekly_source_protected_current_placement_v1(v_family_id,v_event_id,false);
  if v_checked_placement is distinct from v_placement then
    raise exception 'WEEKLY_SOURCE_PROTECTED_LOCK_SCOPE_CHANGED' using errcode='40001';end if;`);
change('  select run.* into strict v_run\n  from public.weekly_exceptional_orchestration_runs run\n  where run.id=v_run_id and run.family_id=v_family.id for share;\n', '');
change("  if v_run.requested_by_user_id is distinct from v_actor\n     or v_run.state not in ('RUNNING','COMPLETE')\n     or v_run.request_kind not in", '  if p_request_kind not in');
change('     or not v_root.is_current or v_root.is_adjustment\n     or v_root.revoked_at is not null or v_root.archived_at_utc is not null',
  "     or (not v_root.is_current and v_placement->'moved'<>'true'::jsonb) or v_root.is_adjustment\n     or v_root.revoked_at is not null or (v_root.archived_at_utc is not null and v_placement->'moved'<>'true'::jsonb)");
change('    v_signed:=private.weekly_exceptional_candidate_signed_evidence_v1(v_evidence_id);',
  '    v_signed:=private.weekly_source_protected_signed_evidence_v1(v_evidence_id,v_family.id,v_event.id);');
change("  return pg_catalog.jsonb_build_object(\n    'ok',true,'contract','WEEKLY_PROTECTED_ACTION_CONTEXT_V1',", `  select jsonb_build_object('timesheet',to_jsonb(t),'contract_record',to_jsonb(c),
    'current_financial',(select to_jsonb(f) from public.timesheets_financials f
      where f.id=(v_placement->>'current_financial_id')::uuid),
    'current_approval_work_id',v_placement->'current_approval_work_id') into strict v_current_entitlement
    from public.timesheets t join public.contracts c on c.id=t.contract_id
    where t.timesheet_id=(v_placement->>'current_timesheet_id')::uuid;
  return pg_catalog.jsonb_build_object(`);
change("    'orchestration_run_id',v_run.id,'family_bound_version',v_family.bound_version,",
  "    'family_bound_version',v_family.bound_version::text,");
change("      'target_pay_method',v_contract.pay_method_snapshot,\n      'target_umbrella_id',nullif(pg_catalog.to_jsonb(v_contract)->>'umbrella_id','')::uuid,",
  "      'target_pay_method',v_current_entitlement#>>'{contract_record,pay_method_snapshot}',\n      'target_umbrella_id',nullif(v_current_entitlement#>>'{contract_record,umbrella_id}','')::uuid,");
change("    'zero_rate_source_refs',v_rate_refs\n  )||case when v_final_source ? 'next_source_segments'\n    then pg_catalog.jsonb_build_object('next_source_segments',v_final_source->'next_source_segments',\n      'family_bound_version',v_family.bound_version::text) else '{}'::jsonb end;",
  "    'zero_rate_source_refs',v_rate_refs,'next_source_segments',v_final_source->'next_source_segments',\n    'protected_current_placement',v_placement,'current_entitlement',v_current_entitlement\n  );");
// Every remaining occurrence is a decision-state use, not a run lookup/row.
const occurrences = text.split('v_run.request_kind').length - 1;
text = text.replaceAll('v_run.request_kind', 'p_request_kind');
edits.push({ before: 'v_run.request_kind', after: 'p_request_kind', occurrences });
if (/weekly_exceptional_orchestration_runs|\bv_run\b|orchestration_run_id/.test(text)) {
  throw new Error('Run identity leaked into run-free fact projection');
}
text += `
revoke all on function private.weekly_source_protected_action_facts_v1(jsonb,text)
 from public,anon,authenticated,service_role;

-- Existing public native DTO and genuine run authority are preserved. Only its
-- read-only fact projection is shared with Source PREVIEW.
create or replace function public.weekly_exceptional_pay_action_context_v1(p_request jsonb)
returns jsonb language plpgsql volatile security definer
set search_path=pg_catalog,private,public as $function$
declare run public.weekly_exceptional_orchestration_runs%rowtype;actor uuid;facts jsonb;
 keys constant text[]:=array['schema_version','actor_user_id','family_id','orchestration_run_id',
  'source_cycle_id','work_event_id','protected_schedule','evidence_timesheet_id'];
begin
 if coalesce(current_setting('request.jwt.claim.role',true),
  nullif(current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
  raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';end if;
 if not private.weekly_exceptional_json_keys_exact_v1(p_request,keys) or not(p_request ?& keys)
  or p_request->>'schema_version' is distinct from 'WEEKLY_PROTECTED_ACTION_CONTEXT_V1' then
  raise exception 'WEEKLY_PROTECTED_ACTION_CONTEXT_INVALID' using errcode='22023';end if;
 actor:=(p_request->>'actor_user_id')::uuid;
 if actor is null or not private.weekly_source_pay_actor_allowed_v1(actor) then
  raise exception 'WEEKLY_PROTECTED_ACTION_CONTEXT_ACTOR_REQUIRED' using errcode='42501';end if;
 select r.* into strict run from public.weekly_exceptional_orchestration_runs r
  where r.id=(p_request->>'orchestration_run_id')::uuid and r.family_id=(p_request->>'family_id')::uuid;
 if run.requested_by_user_id is distinct from actor or run.state not in('RUNNING','COMPLETE')
  or run.request_kind not in('APPROVE','AMEND','WITHDRAW','WAIT','RECONCILE','RECORD_NOT_WORKED') then
  raise exception 'WEEKLY_PROTECTED_ACTION_CONTEXT_SCOPE_INVALID' using errcode='55000';end if;
 facts:=private.weekly_source_protected_action_facts_v1((p_request-'orchestration_run_id')
  ||jsonb_build_object('schema_version','WEEKLY_PROTECTED_ACTION_FACTS_V1'),run.request_kind);
 -- Recheck the genuine run after native locks. No fabricated preview run.
 perform 1 from public.weekly_exceptional_orchestration_runs r where r.id=run.id
  and r.family_id=run.family_id and r.requested_by_user_id=actor and r.request_kind=run.request_kind
  and r.request_fingerprint=run.request_fingerprint and r.state in('RUNNING','COMPLETE') for share;
 if not found then raise exception 'WEEKLY_PROTECTED_ACTION_CONTEXT_SCOPE_INVALID' using errcode='55000';end if;
 -- Preserve the normal Local context's pre-existing scalar/optional-field
 -- representation; the private run-free fact projection is canonical instead.
 if facts->'next_source_segments'='null'::jsonb and not exists(
   select 1 from private.weekly_source_pay_decision_unprotect_preparations p where p.orchestration_run_id=run.id) then
  facts:=(facts-'next_source_segments')||jsonb_build_object('family_bound_version',(facts->>'family_bound_version')::bigint);
 end if;
 return facts||jsonb_build_object('ok',true,'contract','WEEKLY_PROTECTED_ACTION_CONTEXT_V1','orchestration_run_id',run.id);
exception when no_data_found or too_many_rows or invalid_text_representation then
 raise exception 'WEEKLY_PROTECTED_ACTION_CONTEXT_SCOPE_INVALID' using errcode='55000';
end;
$function$;
revoke all on function public.weekly_exceptional_pay_action_context_v1(jsonb) from public,anon,authenticated;
grant execute on function public.weekly_exceptional_pay_action_context_v1(jsonb) to service_role;
notify pgrst,'reload schema';
`;
writeFileSync(path.join(base, 'forward/10_shared_native_context.sqlinc'), text);
writeFileSync(path.join(base, 'NATIVE_CONTEXT_RECEIPT.json'), JSON.stringify({
  input_commit: commit, input, input_sha256: sha(source), output: 'forward/10_shared_native_context.sqlinc',
  output_sha256: sha(text), edits, qualification: 'SOURCE_ONLY_NATIVE_REQUIRED_NOT_RUN',
  run_free_projection_has_no_run_table_or_run_id: true,
}, null, 2) + '\n');
process.stdout.write(`Factored actual native context: ${edits.length} explicit edits, ${occurrences} decision-state uses; no run in shared projection.\n`);
