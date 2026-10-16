import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { mkdirSync, writeFileSync } from 'node:fs';
import path from 'node:path';

const inputCommit = 'a934ee1d460b444939234e812babccb90dc3ddff';
const base = path.dirname(fileURLToPath(import.meta.url));
const sha = (text) => createHash('sha256').update(text).digest('hex');
function read(name) { return execFileSync('git', ['show', `${inputCommit}:${name}`], { encoding: 'utf8', maxBuffer: 32 * 1024 * 1024 }); }
function owner(text, name) {
  const start = text.indexOf(`create or replace function public.${name}(`);
  if (start < 0) throw new Error(`Missing exact current owner ${name}`);
  const end = text.indexOf('$function$;', text.indexOf('as $function$', start));
  if (end < 0) throw new Error(`Missing owner terminator ${name}`);
  return text.slice(start, end + '$function$;'.length) + '\n';
}
function replaceOnce(text, before, after) {
  if (text.split(before).length !== 2) throw new Error('Native parent hunk changed/ambiguous');
  return text.replace(before, after);
}
const inputs = [
  ['weekly_exceptional_pay_next_publication_status_v1', 'supabase/repeatable/04102026_0345_banking_pay_next_protected_source_v1.sql',
    "  select r.* into v_receipt from private.bpay_next_protected_source_receipt r\n    where r.orchestration_run_id=v_run.id;",
    "  v_source_gate:=private.weekly_source_pay_decision_native_normal_gate_v1(v_family.id,v_run.id,v_actor,'STATUS');\n  if v_source_gate is not null then return v_source_gate; end if;\n"],
  ['weekly_exceptional_pay_publish_next_v1', 'supabase/repeatable/11102026_2337_banking_pay_next_source_query_workbench_refresh_v1.sql',
    '  select r.* into v_receipt from private.bpay_next_protected_source_receipt r where r.orchestration_run_id=v_run.id;',
    "  v_source_gate:=private.weekly_source_pay_decision_native_normal_gate_v1(v_family.id,v_run.id,v_actor,'PUBLISH_NEXT');\n  if v_source_gate is not null then return v_source_gate; end if;\n"],
  ['weekly_exceptional_pay_stage_c1_request_v1', 'supabase/repeatable/14102026_0420_healthroster_current_native_composition/weekly_exceptional_pay_stage_c1_request_v1.complete.sqlinc',
    '  if v_run.requested_by_user_id is distinct from v_actor_user_id\n',
    "  perform private.weekly_source_pay_decision_native_normal_gate_v1(v_family.id,v_run.id,v_actor_user_id,'STAGE_LOCAL');\n"],
  ['weekly_exceptional_pay_complete_local_v1', 'supabase/repeatable/14102026_0420_healthroster_current_native_composition/weekly_exceptional_pay_complete_local_v1.complete.sqlinc',
    '  -- SELECT INTO above changes FOUND; test the retained receipt identity itself.\n',
    "  select * into strict v_request from public.weekly_exceptional_c1_publication_requests where id=v_request_id;\n  v_source_gate:=private.weekly_source_pay_decision_native_normal_gate_v1(\n    v_request.family_id,v_request.orchestration_run_id,v_actor,'COMPLETE_LOCAL');\n  if v_source_gate is not null then return v_source_gate; end if;\n"],
];
const receipt = { input_commit: inputCommit, qualification: 'SOURCE_ONLY_NATIVE_REQUIRED_NOT_RUN',
  dependency: 'private.bpay_next_source_decision_native_status_v1(uuid,uuid,uuid)', owners: [] };
mkdirSync(path.join(base, 'native-entry'), { recursive: true });
for (const [name, input, anchor, addition] of inputs) {
  const original = owner(read(input), name);
  let output = replaceOnce(original, 'declare\n', 'declare\n  v_source_gate jsonb;\n');
  // NEXT publisher contains a second matching replay query in its lock recovery
  // block; only the first, actor-checked read is the intended entry boundary.
  if (name === 'weekly_exceptional_pay_publish_next_v1') {
    const first = output.indexOf(anchor);
    if (first < 0) throw new Error('NEXT actor/run entry changed');
    output = output.slice(0, first) + addition + output.slice(first);
  } else output = replaceOnce(output, anchor, addition + anchor);
  const filename = `native-entry/${name}.sqlinc`;
  writeFileSync(path.join(base, filename), output);
  const reverse = output.replace('declare\n  v_source_gate jsonb;\n', 'declare\n').replace(addition, '');
  if (reverse !== original) throw new Error(`Whole-owner reverse failed ${name}`);
  receipt.owners.push({ name: `public.${name}(jsonb)`, input, original_sha256: sha(original), output: filename,
    output_sha256: sha(output), complete_reverse_equals_input: true, ordinary_body_changes: 0 });
}
writeFileSync(path.join(base, 'native-entry/owner_acl.sqlinc'), inputs.map(([name]) =>
  `revoke all on function public.${name}(jsonb) from public,anon,authenticated;\ngrant execute on function public.${name}(jsonb) to service_role;\n`).join(''));
writeFileSync(path.join(base, 'NATIVE_ENTRY_RECEIPT.json'), JSON.stringify(receipt, null, 2) + '\n');
process.stdout.write(`Generated ${receipt.owners.length} exact native entry owners; all complete reverses match.\n`);
