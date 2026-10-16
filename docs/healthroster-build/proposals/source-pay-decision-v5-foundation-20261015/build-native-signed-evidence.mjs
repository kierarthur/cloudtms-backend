import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
const base = path.dirname(fileURLToPath(import.meta.url));
const commit = 'a934ee1d460b444939234e812babccb90dc3ddff';
const input = 'supabase/repeatable/15092026_1534_weekly_source_protected_pay_publisher_v1.sql';
const source = execFileSync('git', ['show', `${commit}:${input}`], { encoding: 'utf8', maxBuffer: 8 * 1024 * 1024 });
const start = source.indexOf('create or replace function private.weekly_exceptional_candidate_signed_evidence_v1(');
const end = source.indexOf('$function$;', source.indexOf('as $function$', start)) + '$function$;'.length;
const original = source.slice(start, end) + '\n';
const sha = text => createHash('sha256').update(text).digest('hex');
let output = original;
const edits = [];
function change(before, after) {
  if (output.split(before).length !== 2) throw new Error('Signed evidence current source changed');
  output = output.replace(before, after); edits.push({ before, after });
}
change('private.weekly_exceptional_candidate_signed_evidence_v1(\n  p_timesheet_id uuid\n)',
  'private.weekly_source_protected_signed_evidence_v1(\n  p_timesheet_id uuid,p_family_id uuid,p_work_event_id uuid\n)');
change('  v_kind text;\nbegin\n', `  v_kind text;
  v_certificate jsonb;v_certified_evidence boolean:=false;
  v_alias private.timesheet_contract_family_aliases%rowtype;
  v_placement private.timesheet_contract_current_placements%rowtype;
  v_member private.timesheet_contract_move_members%rowtype;
  v_operation private.timesheet_contract_move_operations%rowtype;
begin
  select t.* into strict v_timesheet from public.timesheets t where t.timesheet_id=p_timesheet_id;
  if v_timesheet.archived_at_utc is null then
    return private.weekly_exceptional_candidate_signed_evidence_v1(p_timesheet_id);
  end if;
  v_certificate:=private.weekly_source_protected_current_placement_v1(p_family_id,p_work_event_id,false);
  select a.* into strict v_alias from private.timesheet_contract_family_aliases a where a.timesheet_id=p_timesheet_id;
  select p.* into strict v_placement from private.timesheet_contract_current_placements p
    where p.family_root_id=v_alias.family_root_id and p.member_key=v_alias.member_key;
  select m.* into strict v_member from private.timesheet_contract_move_members m
    where m.operation_id=v_placement.operation_id and m.member_key=v_alias.member_key;
  select o.* into strict v_operation from private.timesheet_contract_move_operations o where o.id=v_alias.accepted_operation_id;
  if v_certificate->'moved' is distinct from 'true'::jsonb
    or v_alias.family_root_id::text is distinct from v_certificate->>'family_root_id'
    or v_alias.original_timesheet_id is distinct from p_timesheet_id
    or v_operation.state not in('COMPLETE','SUPERSEDED') or v_operation.purpose<>'CONTRACT_CORRECTION'
    or v_timesheet.archived_reason_code is distinct from 'CONTRACT_MOVE'
    or v_timesheet.archived_by_user_id is distinct from v_operation.created_by_user_id
    or v_member.immutable_evidence_hash is distinct from private.timesheet_contract_move_row_hash_v1(
      private.timesheet_contract_move_evidence_v1(p_timesheet_id))
    or not exists(select 1 from public.weekly_exceptional_payment_approvals a
      join public.weekly_exceptional_pay_target_families f on f.id=a.pay_target_family_id
      where a.pay_target_family_id=p_family_id and a.work_event_id=p_work_event_id and a.evidence_timesheet_id=p_timesheet_id
       and a.candidate_id=f.candidate_id and a.contract_id=f.contract_id and a.client_id=v_operation.actual_client_id)
    or not exists(select 1 from private.timesheet_contract_move_members m where m.operation_id=v_operation.id
      and m.member_key=v_alias.member_key and m.from_timesheet_id=p_timesheet_id)
    or not exists(select 1 from private.timesheet_contract_move_capability_consumptions c
      where c.operation_id=v_operation.id and c.member_key=v_alias.member_key and c.phase='ACCEPT_PLACEMENT'
       and c.guard_name='public.timesheet_archive_row_guard_v1' and c.relation_oid='public.timesheets'::regclass::oid
       and c.row_operation='UPDATE' and c.authority_receipt_id=(v_operation.placement_receipt_json->>'receipt_id')::uuid
       and c.row_sha256=private.timesheet_contract_move_row_hash_v1(to_jsonb(v_timesheet))) then
    raise exception 'WEEKLY_PROTECTED_EVIDENCE_INVALID' using errcode='55000';end if;
  v_certified_evidence:=true;
`);
change('    and timesheet.is_current\n    and timesheet.revoked_at is null\n    and timesheet.archived_at_utc is null;',
  '    and (timesheet.is_current or v_certified_evidence)\n    and timesheet.revoked_at is null\n    and (timesheet.archived_at_utc is null or v_certified_evidence);');
// Complete reversal includes the exact signature/evidence algorithm. Original
// normal owner remains untouched and is used directly on every unarchived row.
let reversed = output;
for (const edit of edits.toReversed()) reversed = reversed.replace(edit.after, edit.before);
if (reversed !== original) throw new Error('Signed evidence whole-owner reverse failed');
output += '\nrevoke all on function private.weekly_source_protected_signed_evidence_v1(uuid,uuid,uuid)\n from public,anon,authenticated,service_role;\n';
writeFileSync(path.join(base, 'forward/09_certified_native_signed_evidence.sqlinc'), output);
writeFileSync(path.join(base, 'NATIVE_SIGNED_EVIDENCE_RECEIPT.json'), JSON.stringify({ input_commit: commit, input,
  original_owner_sha256: sha(original), output: 'forward/09_certified_native_signed_evidence.sqlinc',
  output_sha256: sha(output), complete_reverse_equals_original: true, edits,
  qualification: 'SOURCE_ONLY_NATIVE_REQUIRED_NOT_RUN' }, null, 2) + '\n');
process.stdout.write('Generated certified archived evidence branch; complete native algorithm reverse passes.\n');
