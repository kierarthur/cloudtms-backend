import { execFileSync } from 'node:child_process';
import { writeFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
const base = path.dirname(fileURLToPath(import.meta.url));
const commit = 'a934ee1d460b444939234e812babccb90dc3ddff';
const input = 'supabase/repeatable/14102026_0410_healthroster_protected_source_prerequisites/43_q45_actor_event_closed_dto_cross_session_12_complete_actor_18_prepare_action.sqlinc';
const source = execFileSync('git', ['show', `${commit}:${input}`], { encoding: 'utf8', maxBuffer: 4 * 1024 * 1024 });
const sha = value => createHash('sha256').update(value).digest('hex');
const start = source.indexOf('create or replace function public.weekly_exceptional_pay_prepare_action_v1(');
const end = source.indexOf('$function$;', source.indexOf('as $function$', start)) + '$function$;'.length;
if (start < 0 || end < start) throw new Error('Actual complete native prepare_action owner missing');
const original = source.slice(start, end);let sql = original;const edits = [];
function change(before, after) {
  if (sql.split(before).length !== 2) throw new Error(`Current native preparation seam changed: ${before.slice(0, 90)}`);
  sql = sql.replace(before, after);edits.push({ before, after });
}
change('  v_saved_route jsonb;', '  v_saved_route jsonb;v_placement jsonb;v_checked_placement jsonb;');
change('  v_lock_family:=private.weekly_source_protected_family_lock_v1(v_family_id);', `  v_placement:=private.weekly_source_protected_current_placement_v1(v_family_id,v_event_id,false);
  if v_placement->'moved'='true'::jsonb then
    perform private.timesheet_contract_move_family_lock_v1((v_placement->>'original_root_timesheet_id')::uuid);
    select jsonb_build_object('root_timesheet_id',f.root_timesheet_id,'candidate_id',f.candidate_id,
      'contract_id',f.contract_id,'week_ending_date',f.week_ending_date) into strict v_lock_family
      from public.weekly_exceptional_pay_target_families f where f.id=v_family_id;
  else v_lock_family:=private.weekly_source_protected_family_lock_v1(v_family_id);end if;
  v_checked_placement:=private.weekly_source_protected_current_placement_v1(v_family_id,v_event_id,false);
  if v_checked_placement is distinct from v_placement then
    raise exception 'WEEKLY_SOURCE_PROTECTED_LOCK_SCOPE_CHANGED' using errcode='40001';end if;`);
change(`      'protected_schedule',v_schedule,'reason',v_reason
    )
  );`, `      'protected_schedule',v_schedule,'reason',v_reason
    )||case when v_placement->'moved'='true'::jsonb
      then jsonb_build_object('accepted_current_placement',v_placement) else '{}'::jsonb end
  );`);
let reversed = sql;for (const edit of edits.toReversed()) reversed = reversed.replace(edit.after, edit.before);
if (reversed !== original) throw new Error('Native current preparation whole-owner reverse failed');
sql += `
revoke all on function public.weekly_exceptional_pay_prepare_action_v1(jsonb) from public,anon,authenticated;
grant execute on function public.weekly_exceptional_pay_prepare_action_v1(jsonb) to service_role;

-- Existing native action prepare before-state domain, with the actual moved
-- certificate sealed only for the new moved branch. No existing native receipt
-- or unmoved hash semantics changes. First APPROVE has its own prepare owner;
-- its different before-state hash is not reinterpreted by this helper.
create or replace function private.weekly_source_protected_action_placement_assert_v1(p_request jsonb,p_placement jsonb)
returns void language plpgsql volatile security definer set search_path=pg_catalog,private,public as $function$
declare run public.weekly_exceptional_orchestration_runs%rowtype;
 family public.weekly_exceptional_pay_target_families%rowtype;
 cycle public.weekly_source_cycles%rowtype;before_hash bytea;
begin
 if p_placement->'moved' is distinct from 'true'::jsonb then return;end if;
 select r.* into strict run from public.weekly_exceptional_orchestration_runs r where r.id=(p_request->>'orchestration_run_id')::uuid;
 if run.request_kind='APPROVE' then return;end if;
 select f.* into strict family from public.weekly_exceptional_pay_target_families f where f.id=run.family_id;
 select c.* into strict cycle from public.weekly_source_cycles c where c.id=(p_request->>'source_cycle_id')::uuid;
 if run.request_kind not in('AMEND','WITHDRAW','WAIT','RECONCILE','RECORD_NOT_WORKED')
  or run.state<>'RUNNING' or run.requested_by_user_id::text is distinct from p_request->>'actor_user_id'
  or family.id::text is distinct from p_request->>'family_id'
  or family.bound_version::text is distinct from p_request->>'expected_family_bound_version'
  or p_placement is distinct from private.weekly_source_protected_current_placement_v1(
    family.id,(p_request->>'work_event_id')::uuid,false) then
  raise exception 'WEEKLY_SOURCE_PROTECTED_LOCK_SCOPE_CHANGED' using errcode='40001';end if;
 before_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_ACTION_PREPARE_BEFORE_V1',jsonb_build_object(
   'family_id',family.id,'family_bound_version',family.bound_version,
   'current_generation_id',family.current_generation_id,'current_generation_number',family.current_generation_number,
   'current_target_vector_hash',encode(family.current_complete_target_vector_hash,'hex'),
   'source_cycle_id',cycle.id,'source_group_id',cycle.source_group_id,
   'work_event_id',(p_request->>'work_event_id')::uuid,'action',run.request_kind,'actor_user_id',run.requested_by_user_id,
   'protected_schedule',p_request->'protected_schedule','reason',btrim(p_request->>'reason'),
   'accepted_current_placement',p_placement));
 if before_hash is distinct from run.before_state_fingerprint then
  raise exception 'WEEKLY_SOURCE_PROTECTED_LOCK_SCOPE_CHANGED' using errcode='40001';end if;
exception when no_data_found or too_many_rows or invalid_text_representation then
 raise exception 'WEEKLY_SOURCE_PROTECTED_LOCK_SCOPE_CHANGED' using errcode='40001';
end;
$function$;
revoke all on function private.weekly_source_protected_action_placement_assert_v1(jsonb,jsonb)
 from public,anon,authenticated,service_role;
notify pgrst,'reload schema';
`;
const output = 'forward/43_current_native_action_preparation.sqlinc';
writeFileSync(path.join(base, output), sql);
writeFileSync(path.join(base, 'NATIVE_CURRENT_PREPARATION_RECEIPT.json'), JSON.stringify({ input_commit: commit, input,
  input_sha256: sha(source), selected_native_body_sha256: sha(original), output, output_sha256: sha(sql), edits,
  complete_reverse_equals_input: true, unmoved_before_hash_changed: false,
  original_domain: 'WEEKLY_PROTECTED_ACTION_PREPARE_BEFORE_V1', moved_only_addition: 'accepted_current_placement: actual full PROTECTED_CURRENT_PLACEMENT_V1',
  first_approve_boundary: 'Separate native prepare owner/hash unchanged; helper does not claim first-APPROVE coverage.',
  qualification: 'SOURCE_ONLY_NATIVE_REQUIRED_NOT_RUN' }, null, 2) + '\n');
process.stdout.write('Generated complete current native action preparation and placement assertion; exact reverse passes.\n');
