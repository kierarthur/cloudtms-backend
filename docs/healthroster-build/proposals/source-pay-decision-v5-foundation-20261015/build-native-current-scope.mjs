import { execFileSync } from 'node:child_process';
import { writeFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
const base = path.dirname(fileURLToPath(import.meta.url));
const commit = 'a934ee1d460b444939234e812babccb90dc3ddff';
const sha = value => createHash('sha256').update(value).digest('hex');
const files = [
  ['supabase/repeatable/15092026_1534_weekly_source_ordinary_pay_projection_v1.sql','private.weekly_source_target_family_for_root_v1'],
  ['supabase/repeatable/05102026_0057_weekly_source_pay_query_scope_v1.sql','private.weekly_source_pay_query_scope_v1'],
];
const bodies = [];const receipts = [];
for (const [input, name] of files) {
  const source = execFileSync('git', ['show', `${commit}:${input}`], { encoding: 'utf8', maxBuffer: 4 * 1024 * 1024 });
  const start = source.indexOf(`create or replace function ${name}(`);
  const end = source.indexOf('$function$;', source.indexOf('as $function$', start));
  if (start < 0 || end < start) throw new Error(`Missing complete native scope ${name}`);
  const original = source.slice(start, end + '$function$;'.length);
  let sql = original;const edits = [];
  function change(before, after) {
    if (sql.split(before).length !== 2) throw new Error(`Current scope seam changed: ${before.slice(0, 100)}`);
    sql = sql.replace(before, after);edits.push({ before, after });
  }
  change('  v_count integer;', '  v_count integer;v_placement jsonb;');
  if (name.endsWith('target_family_for_root_v1')) {
    change(`  if v_count=0 then
    return null;
  end if;`, `  if v_count=0 then
    -- Direct certified physical alias only. Keep the genuine original target
    -- family A; never create/reparent one or derive it by booking ancestry.
    select count(*) into v_count from public.weekly_exceptional_pay_target_families f
      join private.timesheet_contract_family_aliases original_alias on original_alias.timesheet_id=f.root_timesheet_id
      join private.timesheet_contract_family_aliases current_alias on current_alias.family_root_id=original_alias.family_root_id
        and current_alias.member_key=original_alias.member_key and current_alias.timesheet_id=p_root_timesheet_id
      where f.ownership_state='TARGET_MANAGED';
    if v_count=0 then return null;end if;
    if v_count<>1 then raise exception 'WEEKLY_SOURCE_TARGET_FAMILY_AMBIGUOUS' using errcode='55000';end if;
    select f.* into strict v_family from public.weekly_exceptional_pay_target_families f
      join private.timesheet_contract_family_aliases original_alias on original_alias.timesheet_id=f.root_timesheet_id
      join private.timesheet_contract_family_aliases current_alias on current_alias.family_root_id=original_alias.family_root_id
        and current_alias.member_key=original_alias.member_key and current_alias.timesheet_id=p_root_timesheet_id
      where f.ownership_state='TARGET_MANAGED';
    v_placement:=private.timesheet_contract_current_placement_v1(v_family.root_timesheet_id);
    if v_placement->>'current_timesheet_id' is distinct from p_root_timesheet_id::text
      or v_placement->'legacy_origin' is distinct from 'false'::jsonb
      or not exists(select 1 from public.timesheets t join public.contracts c on c.id=t.contract_id
       where t.timesheet_id=p_root_timesheet_id and t.is_current and t.archived_at_utc is null and t.revoked_at is null
        and c.candidate_id=v_family.candidate_id and t.week_ending_date=v_family.week_ending_date
        and c.id::text=v_placement->>'current_contract_id') then
      raise exception 'WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE' using errcode='55000';end if;
    return v_family;
  end if;`);
  } else {
    change('  v_source:=private._candidate_expense_source_family_v1(p_root_timesheet_id);', `  v_source:=private._candidate_expense_source_family_v1(p_root_timesheet_id);
  if v_source is distinct from true and exists(select 1 from private.timesheet_contract_family_aliases a where a.timesheet_id=p_root_timesheet_id) then
    v_family:=private.weekly_source_target_family_for_root_v1(p_root_timesheet_id);
    if v_family.id is not null and v_family.root_timesheet_id is distinct from p_root_timesheet_id then
      v_placement:=private.timesheet_contract_current_placement_v1(v_family.root_timesheet_id);
      if v_placement->>'current_timesheet_id' is distinct from p_root_timesheet_id::text
       or v_placement->>'current_contract_id' is distinct from v_root.contract_id::text then
       raise exception 'WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE' using errcode='55000';end if;
      -- Source classification stays with the real immutable original owner;
      -- this certified current financial B scope does not change that owner.
      v_source:=private._candidate_expense_source_family_v1(v_family.root_timesheet_id);
    end if;
  end if;`);
    change(`  -- Ordinary Source publication does not create an exceptional target family.`, `  if v_count=0 then
    v_family:=private.weekly_source_target_family_for_root_v1(p_root_timesheet_id);
    if v_family.id is not null then
      v_placement:=private.timesheet_contract_current_placement_v1(v_family.root_timesheet_id);
      if v_placement->>'current_timesheet_id' is distinct from p_root_timesheet_id::text
        or v_placement->>'current_contract_id' is distinct from v_contract.id::text
        or v_placement->'legacy_origin' is distinct from 'false'::jsonb
        or v_family.candidate_id is distinct from v_contract.candidate_id
        or v_family.week_ending_date is distinct from v_root.week_ending_date or v_family.bound_version<1 then
        raise exception 'WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE' using errcode='55000';end if;
    end if;
  end if;
  -- Ordinary Source publication does not create an exceptional target family.`);
  }
  let reversed = sql;for (const edit of edits.toReversed()) reversed = reversed.replace(edit.after, edit.before);
  if (reversed !== original) throw new Error(`Full native current scope reverse failed: ${name}`);
  bodies.push(sql + `\nrevoke all on function ${name}(uuid) from public,anon,authenticated,service_role;\n`);
  receipts.push({ input, name, input_sha256: sha(source), selected_native_body_sha256: sha(original), edits });
}
const output = 'forward/27_certified_current_native_scope.sqlinc';const sql = bodies.join('\n');
writeFileSync(path.join(base, output), sql);
writeFileSync(path.join(base, 'NATIVE_CURRENT_SCOPE_RECEIPT.json'), JSON.stringify({ input_commit: commit, inputs: receipts,
  output, output_sha256: sha(sql), complete_reverse_equals_input: true, ordinary_body_changes: 0,
  qualification: 'SOURCE_ONLY_NATIVE_REQUIRED_NOT_RUN' }, null, 2) + '\n');
process.stdout.write('Generated exact direct certified current native family/scope consumers; full ordinary reverses pass.\n');
