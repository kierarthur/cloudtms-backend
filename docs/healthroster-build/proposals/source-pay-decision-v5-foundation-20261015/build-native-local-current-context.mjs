import { execFileSync } from 'node:child_process';
import { writeFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
const base = path.dirname(fileURLToPath(import.meta.url));
const commit = 'a934ee1d460b444939234e812babccb90dc3ddff';
const sha = value => createHash('sha256').update(value).digest('hex');
const files = [
  ['supabase/repeatable/04102026_1257_weekly_source_local_protected_decision_v1.sql','private.weekly_source_local_preauthorisation_write_allowed_v1','uuid,jsonb'],
  ['supabase/repeatable/05102026_0212_weekly_source_local_publication_context_v2.sql','private.weekly_source_local_publication_context_v2','uuid,uuid,uuid'],
];
const bodies = [];const receipts = [];
for (const [input, name, signature] of files) {
  const source = execFileSync('git', ['show', `${commit}:${input}`], { encoding: 'utf8', maxBuffer: 4 * 1024 * 1024 });
  const start = source.indexOf(`create or replace function ${name}(`);
  const end = source.indexOf('$function$;', source.indexOf('as $function$', start));
  if (start < 0 || end < start) throw new Error(`Missing complete native Local context ${name}`);
  const original = source.slice(start, end + '$function$;'.length);
  let sql = original;const edits = [];
  function change(before, after) {
    if (sql.split(before).length !== 2) throw new Error(`Native Local current context seam changed: ${before.slice(0, 100)}`);
    sql = sql.replace(before, after);edits.push({ before, after });
  }
  if (name.endsWith('preauthorisation_write_allowed_v1')) {
    change('    and family.root_timesheet_id=root.timesheet_id', `    and (family.root_timesheet_id=root.timesheet_id or (
      private.timesheet_contract_current_placement_v1(family.root_timesheet_id)->>'current_timesheet_id'=root.timesheet_id::text
      and private.timesheet_contract_current_placement_v1(family.root_timesheet_id)->>'current_contract_id'=root.contract_id::text
      and exists(select 1 from public.contracts c where c.id=root.contract_id and c.candidate_id=family.candidate_id)
      and root.week_ending_date=family.week_ending_date))`);
    // Keep the existing E7 boolean-only execute grant. This does not grant a
    // write capability or expose any retained row/snapshot to service callers.
    bodies.push(sql + `\nrevoke all on function ${name}(${signature}) from public,anon,authenticated,service_role;\ngrant execute on function ${name}(${signature}) to service_role;\n`);
  } else {
    change('  v_stage_hash bytea;', '  v_stage_hash bytea;v_placement jsonb;');
    change(`  if not found or v_family.root_timesheet_id is distinct from p_root_timesheet_id
     or v_family.root_family_booking_id is distinct from v_scope->>'family_booking_id'
     or v_family.candidate_id is distinct from v_request.candidate_id
     or v_family.contract_id is distinct from v_request.contract_id`,
`  if found and v_family.root_timesheet_id is distinct from p_root_timesheet_id then
    v_placement:=private.weekly_source_protected_current_placement_v1(v_family.id,
      (select a.work_event_id from public.weekly_exceptional_payment_approvals a
       where a.creation_orchestration_run_id=v_run.id and a.pay_target_family_id=v_family.id),false);
    if v_placement->'moved' is distinct from 'true'::jsonb
      or v_placement->>'current_timesheet_id' is distinct from p_root_timesheet_id::text
      or v_placement->>'current_contract_id' is distinct from v_request.contract_id::text then return null;end if;
  end if;
  if v_family.id is null or (v_placement is null and (
       v_family.root_timesheet_id is distinct from p_root_timesheet_id
       or v_family.root_family_booking_id is distinct from v_scope->>'family_booking_id'
       or v_family.contract_id is distinct from v_request.contract_id))
     or v_family.candidate_id is distinct from v_request.candidate_id`);
    change('or s.contract_id is distinct from v_family.contract_id)) then return null; end if;',
      'or s.contract_id is distinct from v_request.contract_id)) then return null; end if;');
    bodies.push(sql + `\nrevoke all on function ${name}(${signature}) from public,anon,authenticated,service_role;\n`);
  }
  let reversed = sql;for (const edit of edits.toReversed()) reversed = reversed.replace(edit.after, edit.before);
  if (reversed !== original) throw new Error(`Full native Local context reverse failed: ${name}`);
  receipts.push({ input, name, input_sha256: sha(source), selected_native_body_sha256: sha(original), edits });
}
const output = 'forward/29_current_local_native_context.sqlinc';const sql = bodies.join('\n');
writeFileSync(path.join(base, output), sql);
writeFileSync(path.join(base, 'NATIVE_LOCAL_CURRENT_CONTEXT_RECEIPT.json'), JSON.stringify({ input_commit: commit, inputs: receipts,
  output, output_sha256: sha(sql), complete_reverse_equals_input: true, ordinary_body_changes: 0,
  qualification: 'SOURCE_ONLY_NATIVE_REQUIRED_NOT_RUN' }, null, 2) + '\n');
process.stdout.write('Generated genuine current-B Local preauthorisation/publication contexts; complete ordinary reverses pass.\n');
