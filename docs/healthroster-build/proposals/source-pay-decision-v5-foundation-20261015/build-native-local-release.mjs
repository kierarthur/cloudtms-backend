import { execFileSync } from 'node:child_process';
import { writeFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
const base = path.dirname(fileURLToPath(import.meta.url));
const commit = 'a934ee1d460b444939234e812babccb90dc3ddff';
const sha = value => createHash('sha256').update(value).digest('hex');
const inputRelease = 'supabase/repeatable/04102026_1257_weekly_source_local_protected_decision_v1.sql';
const inputStatus = 'supabase/repeatable/05102026_0624_weekly_source_local_saved_status_v1.sql';
const read = input => execFileSync('git', ['show', `${commit}:${input}`], { encoding: 'utf8', maxBuffer: 2 * 1024 * 1024 });
const releaseFile = read(inputRelease);const statusFile = read(inputStatus);
const start = releaseFile.indexOf('create or replace function private.weekly_source_local_protected_pending_released_v1()');
const release = releaseFile.slice(start, releaseFile.indexOf('\nalter function ', start));
const anchor = '  perform private.weekly_source_protected_contact_retire_v1(v_family.id,v_work_event_id);\n  return new;';
const addition = `  -- Same native callback, original immutable Source application/adoption.
  -- Derive the exact original-step/current-row proof after the real native
  -- release checks and atomic result/run/request updates have all completed.
  perform private.weekly_source_pay_decision_local_release_proof_v1(p.id)
    from private.weekly_source_pay_decision_unprotect_preparations p
    join public.weekly_exceptional_c1_publication_requests r on r.orchestration_run_id=p.orchestration_run_id
    where r.id=v_local.publication_request_id;
`;
if (release.split(anchor).length !== 2) throw new Error('Current pending release hook changed');
const changedRelease = release.replace(anchor, anchor.replace('  return new;', addition + '  return new;'));
const statusStart = statusFile.indexOf('create or replace function ');
const status = statusFile.slice(statusStart, statusFile.indexOf('\nalter function ', statusStart));
const changedStatus = status.replace('private.weekly_source_local_saved_status_v1(', 'private.weekly_source_local_saved_status_native_v1(');
if (changedStatus === status || changedRelease.replace(addition, '') !== release
    || changedStatus.replace('private.weekly_source_local_saved_status_native_v1(', 'private.weekly_source_local_saved_status_v1(') !== status) {
  throw new Error('Full native release/status reverse failed');
}
const wrapper = `
revoke all on function private.weekly_source_local_protected_pending_released_v1() from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_local_saved_status_native_v1(uuid,uuid,uuid) from public,anon,authenticated,service_role;
create or replace function private.weekly_source_local_saved_status_v1(p_family_id uuid,p_orchestration_run_id uuid,p_actor_user_id uuid)
returns jsonb language plpgsql stable security definer set search_path=pg_catalog,private,public as $function$
declare result jsonb;prep private.weekly_source_pay_decision_unprotect_preparations%rowtype;
begin
 -- Keep every existing genuine native origin/snapshot/component/step/release
 -- check, and its current Office actor/financial/source permission boundary.
 result:=private.weekly_source_local_saved_status_native_v1(p_family_id,p_orchestration_run_id,p_actor_user_id);
 select p.* into prep from private.weekly_source_pay_decision_unprotect_preparations p where p.orchestration_run_id=p_orchestration_run_id;
 if found and result is not null then
  if prep.family_id is distinct from p_family_id or prep.actor_user_id is distinct from p_actor_user_id or prep.route<>'SOURCE_LOCAL' then
   raise exception 'SOURCE_NATIVE_LOCAL_RECEIPT_PROOF_INVALID' using errcode='23514';end if;
  perform private.weekly_source_pay_decision_local_release_proof_v1(prep.id);
 end if;
 return result;
end;
$function$;
revoke all on function private.weekly_source_local_saved_status_v1(uuid,uuid,uuid) from public,anon,authenticated,service_role;
`;
const output = 'forward/25_actual_local_release_and_saved_status.sqlinc';
const sql = changedRelease + '\n' + changedStatus + wrapper;
writeFileSync(path.join(base, output), sql);
writeFileSync(path.join(base, 'NATIVE_LOCAL_RELEASE_RECEIPT.json'), JSON.stringify({ input_commit: commit,
  inputs: [{ path: inputRelease, sha256: sha(releaseFile), selected_native_body_sha256: sha(release) },
    { path: inputStatus, sha256: sha(statusFile), selected_native_body_sha256: sha(status) }],
  output, output_sha256: sha(sql), complete_reverse_equals_input: true, ordinary_body_changes: 0,
  source_effect: 'Read-only exact original pending step / actual RELEASED row / full reconstructed original Local row proof; no automatic Source acceptance or Banking mapping.',
  qualification: 'SOURCE_ONLY_NATIVE_REQUIRED_NOT_RUN' }, null, 2) + '\n');
process.stdout.write('Generated actual Local release/saved-status composition; complete native body reverse passes.\n');
