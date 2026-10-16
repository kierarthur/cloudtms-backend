import { execFileSync } from 'node:child_process';
import { writeFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
const base = path.dirname(fileURLToPath(import.meta.url));
const commit = 'a934ee1d460b444939234e812babccb90dc3ddff';
const input = 'supabase/repeatable/includes/05102026_0659_bpay_next_protected_owner_route_v1.sqlinc';
const original = execFileSync('git', ['show', `${commit}:${input}`], { encoding: 'utf8', maxBuffer: 4 * 1024 * 1024 });
const sha = value => createHash('sha256').update(value).digest('hex');
const declaration = '  v_receipt private.bpay_next_protected_source_receipt%rowtype;';
const anchor = "  if v_family.target_domain_version='C1_V1' then";
if (original.split(declaration).length !== 2 || original.split(anchor).length !== 2) throw new Error('Current route owner changed');
const addition = `  if v_family.target_domain_version='NEXT_V1' and v_receipt.orchestration_run_id is null then
    v_source_route:=private.weekly_source_pay_decision_family_native_route_v1(v_family.id,v_family.current_generation_id);
    if v_source_route is not null then return v_source_route;end if;
  end if;
`;
const output = original.replace(declaration, declaration + '\n  v_source_route text;').replace(anchor, addition + anchor);
if (output.replace(declaration + '\n  v_source_route text;', declaration).replace(addition, '') !== original) {
  throw new Error('Whole route owner reverse failed');
}
writeFileSync(path.join(base, 'forward/18_current_native_owner_route.sqlinc'), output);
writeFileSync(path.join(base, 'NATIVE_OWNER_ROUTE_RECEIPT.json'), JSON.stringify({ input_commit: commit, input,
  input_sha256: sha(original), output: 'forward/18_current_native_owner_route.sqlinc', output_sha256: sha(output),
  complete_reverse_equals_input: true, ordinary_body_changes: 0, qualification: 'SOURCE_ONLY_NATIVE_REQUIRED_NOT_RUN' }, null, 2) + '\n');
process.stdout.write('Added exact Source-composed route branch; complete ordinary owner reverse passes.\n');
