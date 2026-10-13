import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFileSync, writeFileSync } from 'node:fs';

// Complete successor of the independently reviewed sole current engine.
// Byte-exact unaffected sections preserve current NEXT, locking and receipts.
const sourcePath = 'supabase/repeatable/15092026_1534_weekly_source_finalisation_v1.sql';
const outputPath = 'supabase/repeatable/13102026_1210_weekly_source_finalisation_unchanged_values_v1.sql';
const source = readFileSync(sourcePath, 'utf8').replaceAll('\r\n', '\n');
assert.equal(createHash('sha256').update(source).digest('hex'), '19018ea7be5e06a510fdd748409bcd84a18d0e31724daa19afb0a4471349a95b', 'Review a changed predecessor before regenerating');
const name = 'create or replace function private.weekly_source_finalise_engine_v1(';
const start = source.indexOf(name), end = source.indexOf('$function$;', start) + '$function$;'.length;
assert.ok(start >= 0 && end > start);
let engine = source.slice(start, end);
function replaceOnce(before, after) {
  assert.equal(engine.split(before).length, 2, `Expected exactly one reviewed anchor: ${before.slice(0, 100)}`);
  engine = engine.replace(before, () => after);
}
replaceOnce('  v_effective_policy jsonb;\n', `  v_effective_policy jsonb;
  v_authority_subset jsonb;
  v_prior_authority_subset jsonb;
  v_current_authority_subset jsonb;
  v_comparison_current_authority jsonb;
`);
const sourceAuthorityGuard = `      if (v_effective_policy->>'authority_mode') is distinct from 'SOURCE_AUTHORITY'
         or coalesce((v_effective_policy->>'self_bill_enabled')::boolean,false) is not true
         or (v_effective_policy->>'source_group_id')::uuid is distinct from v_group.id then
        raise exception 'WEEKLY_SOURCE_FINALISATION_POLICY_STALE' using errcode='40001';
      end if;
`;
replaceOnce("      v_snapshot_hash:=private.weekly_source_sha256_jsonb_v1(\n", `      -- Freeze only the four authority fields for this exact new snapshot.
      -- The full economic/provenance hashes remain unchanged below.
      v_effective_policy:=private._weekly_source_effective_policy_v1(
        v_row.client_id,v_row.contract_id,v_row.work_date
      );
${sourceAuthorityGuard}      v_authority_subset:=private.weekly_source_authority_subset_v1(v_effective_policy);
      v_snapshot_hash:=private.weekly_source_sha256_jsonb_v1(
`);
replaceOnce('        policy_fingerprint,snapshot_line_hash\n', '        policy_fingerprint,snapshot_line_hash,authority_subset_json,authority_subset_sha256,invoice_economics_sha256\n');
replaceOnce('        v_economic.effective_policy_fingerprint,v_snapshot_hash\n', `        v_economic.effective_policy_fingerprint,v_snapshot_hash,v_authority_subset,
        private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_AUTHORITY_SUBSET_V1',v_authority_subset),
        null -- SOURCE_AMOUNT wire waits for the exact F5 released-money producer.
`);
const oldWholeGuard = `         or private.weekly_source_finalisation_hex32_v1(
              v_effective_policy->>'policy_sha256','WEEKLY_SOURCE_POLICY_FINGERPRINT_INVALID'
            ) is distinct from coalesce(
              v_current_snapshot.policy_fingerprint,v_prior_snapshot.policy_fingerprint
            ) then`;
replaceOnce(oldWholeGuard, `         or (coalesce(v_current_authority_subset,v_prior_authority_subset) is not null
           and v_authority_subset is distinct from coalesce(v_current_authority_subset,v_prior_authority_subset))
         or (coalesce(v_current_authority_subset,v_prior_authority_subset) is null
           and private.weekly_source_finalisation_hex32_v1(
              v_effective_policy->>'policy_sha256','WEEKLY_SOURCE_POLICY_FINGERPRINT_INVALID'
            ) is distinct from coalesce(
              v_current_snapshot.policy_fingerprint,v_prior_snapshot.policy_fingerprint
            )) then`);
const policyLookup = `      v_effective_policy:=private._weekly_source_effective_policy_v1(
        coalesce(v_current_snapshot.client_id,v_prior_snapshot.client_id),
        coalesce(v_current_snapshot.contract_id,v_prior_snapshot.contract_id),
        coalesce(v_current_snapshot.work_date,v_prior_snapshot.work_date)
      );
`;
replaceOnce(policyLookup, `${policyLookup}${sourceAuthorityGuard}      v_authority_subset:=private.weekly_source_authority_subset_v1(v_effective_policy);
      v_prior_authority_subset:=private.weekly_source_snapshot_authority_subset_v1(v_transition.prior_snapshot_id);
      v_current_authority_subset:=private.weekly_source_snapshot_authority_subset_v1(v_transition.current_snapshot_id);
`);
const stateStart = engine.indexOf('      v_prior_state:=case when v_transition.prior_snapshot_id is null');
const stateEnd = engine.indexOf('      v_outcome:=case', stateStart);
assert.ok(stateStart > 0 && stateEnd > stateStart);
const oldStateBuilders = engine.slice(stateStart, stateEnd);
replaceOnce(oldStateBuilders, `      -- Unknown pre-upgrade authority keeps the whole-hash fence. Equality
      -- proves compatibility without inventing historical four-field JSON.
      v_comparison_current_authority:=v_current_authority_subset;
      if v_transition.prior_snapshot_id is not null and v_prior_authority_subset is null
         and v_transition.current_snapshot_id is not null then
        if private.weekly_source_finalisation_hex32_v1(
             v_effective_policy->>'policy_sha256','WEEKLY_SOURCE_POLICY_FINGERPRINT_INVALID'
           ) is distinct from v_prior_snapshot.policy_fingerprint then
          raise exception 'WEEKLY_SOURCE_FINALISATION_POLICY_STALE' using errcode='40001';
        end if;
        v_comparison_current_authority:=null;
      end if;
      v_prior_state:=private.weekly_source_snapshot_value_fingerprint_v1(
        v_transition.prior_snapshot_id,v_prior_authority_subset
      );
      v_current_state:=private.weekly_source_snapshot_value_fingerprint_v1(
        v_transition.current_snapshot_id,v_comparison_current_authority
      );
`);
const output = `-- WP-F2 complete finalise-engine successor; reviewed predecessor ${sourcePath}.
-- Preserve all native NEXT hooks, locks, current CAS, NHSP amounts and receipts.
-- Source money correction/unwind remains gated by its own F5 producer.
\\set ON_ERROR_STOP on
begin;
\\ir support/13102026_1210_weekly_source_final_value_helpers_v1.sql

${engine}

alter function private.weekly_source_finalise_engine_v1(jsonb,uuid,boolean) owner to postgres;
revoke all on function private.weekly_source_finalise_engine_v1(jsonb,uuid,boolean)
  from public,anon,authenticated,service_role;
notify pgrst, 'reload schema';
commit;
`;
if (process.argv.includes('--check')) assert.equal(readFileSync(outputPath, 'utf8').replaceAll('\r\n', '\n'), output, 'F2 complete successor is stale');
else writeFileSync(outputPath, output);
console.log(JSON.stringify({ outputPath, completeEngine: true, sha256: createHash('sha256').update(output).digest('hex') }));
