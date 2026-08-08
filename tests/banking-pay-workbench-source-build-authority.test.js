import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const repoRoot = new URL('..', import.meta.url);
const read = (path) => readFileSync(new URL(path, repoRoot), 'utf8').replace(/\r\n/g, '\n');

const canonical = read('supabase/repeatable/07082026_1013_pay_workbench_candidate_source_build_chunk.sql');
const lateAuthority = read('supabase/repeatable/08082026_0322_pay_workbench_candidate_source_build_chunk_authority.sql');
const claimStart = read('supabase/repeatable/07082026_1012_pay_workbench_source_build_attempt_claim_start_v1.sql');

test('late authority reasserts the profile-2 source-build owner after historical omnibus files', () => {
  assert.match(lateAuthority, /\\ir 07082026_1013_pay_workbench_candidate_source_build_chunk\.sql/);
  assert.match(canonical, /effect_plan_sealed/i);
  assert.match(canonical, /PAY_WORKBENCH_EFFECT_PLAN_CAPTURE_ROLLBACK/i);
  assert.match(canonical, /fact_family='EXPECTED_FINANCE_EFFECT'/i);
  assert.match(canonical, /reconcile_capture_timing_json/i);
  assert.match(canonical, /PAY_WORKBENCH_CANONICAL_PUBLICATION_DIGEST_MISMATCH/i);
});

test('capture is durably sealed before execute and does not weaken Policy X', () => {
  const captureBranch = canonical.indexOf("effect_plan_sealed')::boolean,false) IS NOT TRUE");
  const executeCall = canonical.lastIndexOf('v_sync_result:=public.pay_sync_overpayments_from_preview');
  assert.ok(captureBranch >= 0, 'capture branch must exist');
  assert.ok(executeCall > captureBranch, 'execute must follow the sealed capture branch');
  assert.match(canonical, /PAY_WORKBENCH_EXPECTED_EFFECT_PLAN_MISMATCH/i);
  assert.match(canonical, /captured_candidate_generation/i);
  assert.match(canonical, /captured_source_change_seq/i);
  assert.doesNotMatch(canonical, /statement_timeout\s*=/i);
});

test('candidate material work does not exclusively lock the shared session row', () => {
  const legacyBody = canonical.match(
    /CREATE OR REPLACE FUNCTION private\.pay_workbench_candidate_source_build_chunk_legacy_v1[\s\S]*?\$function\$;/i,
  )?.[0] || '';

  assert.match(
    legacyBody,
    /SELECT \* INTO v_session FROM public\.banking_pay_workbench_sessions\s+WHERE id=p_session_id;/i,
  );
  assert.doesNotMatch(
    legacyBody,
    /FROM public\.banking_pay_workbench_sessions\s+WHERE id=p_session_id\s+FOR (?:UPDATE|SHARE)/i,
  );
  assert.match(legacyBody, /terminal completion\/publication owns/i);
});

test('RPC 1 validates the shared session without serialising candidate lanes', () => {
  assert.match(
    claimStart,
    /SELECT \* INTO v_session\s+FROM public\.banking_pay_workbench_sessions AS session_row\s+WHERE session_row\.id=v_job\.session_id;/i,
  );
  assert.doesNotMatch(
    claimStart,
    /FROM public\.banking_pay_workbench_sessions AS session_row\s+WHERE session_row\.id=v_job\.session_id\s+FOR (?:UPDATE|SHARE)/i,
  );
  assert.match(claimStart, /RPC 2\/final publication revalidate/i);
});
