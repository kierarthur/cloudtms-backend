import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const repoRoot = new URL('..', import.meta.url);
const read = (path) => readFileSync(new URL(path, repoRoot), 'utf8').replace(/\r\n/g, '\n');

const canonical = read('supabase/repeatable/07082026_1013_pay_workbench_candidate_source_build_chunk.sql');
const lateAuthority = read('supabase/repeatable/08082026_0322_pay_workbench_candidate_source_build_chunk_authority.sql');
const claimStart = read('supabase/repeatable/07082026_1012_pay_workbench_source_build_attempt_claim_start_v1.sql');
const clone = read('supabase/repeatable/04082026_1302_pay_workbench_session_clone_eligible_rows_v1.sql');
const enqueue = read('supabase/repeatable/07082026_1017_pay_workbench_enqueue_candidate_refresh.sql');
const cursorPreserve = read('supabase/repeatable/05082026_1348_pay_workbench_fact_cursor_preserve_v2.sql');
const cursorTransition = read('supabase/repeatable/05082026_1539_pay_workbench_fact_cursor_transition_v3.sql');

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

test('reservation paging seals the normal active-item domain once and preserves cursor authority', () => {
  assert.match(canonical, /active_item_candidates AS MATERIALIZED/);
  assert.match(canonical,
    /public\._pay_batch_item_economic_components\(\s*NULL::uuid,item_ids\.pay_batch_item_ids\)/);
  assert.equal((canonical.match(/public\._pay_batch_item_economic_components\(/g) || []).length, 1);
  assert.match(canonical, /NOT EXISTS \(\s*SELECT 1\s*FROM public\.pay_advance_reservations advance[\s\S]*advance\.pay_batch_item_id=item\.id[\s\S]*advance\.id=item\.reservation_id/);
  assert.match(canonical, /'~ITEM:'\|\|item\.pay_batch_item_id::text/);
  assert.match(canonical, /ORDER BY \('~ITEM:'\|\|item\.pay_batch_item_id::text\) COLLATE "C"/);
  assert.match(canonical, /reservation\.id::text COLLATE "C"\s*>v_last_source_key COLLATE "C"/);
  assert.match(canonical, /ORDER BY reservation\.id::text COLLATE "C"/);
  assert.match(canonical, /ORDER BY page\.source_key COLLATE "C"/);
  assert.match(canonical, /max\(page\.source_key COLLATE "C"\)/);
  assert.match(canonical, /RESERVATION_COMPONENT_SOURCE_KEY_C_V1/);
  assert.match(canonical, /PAY_WORKBENCH_RESERVATION_ORDER_CONTRACT_OBSOLETE/);
  assert.match(cursorPreserve, /reservation_source_key_order_contract/);
  assert.match(cursorPreserve, /PAY_WORKBENCH_RESERVATION_ORDER_CONTRACT_OBSOLETE/);
  assert.match(cursorTransition, /RESERVATION_COMPONENT_SOURCE_KEY_C_V1/);
  assert.match(canonical, /'RESERVATION_DUPLICATE_LOGICAL_OWNER'/);
  assert.match(canonical, /'RESERVATION_ECONOMIC_KEY_MISSING'/);
  assert.match(canonical, /'RESERVATION_ECONOMIC_KEY_CONFLICT'/);
});

test('clone repair reproduces the enqueue authority fingerprint and compares all four authority fields', () => {
  for (const marker of [
    'WORKBENCH_SOURCE_OWNER_V3',
    'WORKBENCH_SOURCE_OWNER_V2',
    'READY_TO_PAY_SEMANTIC_V2',
    'source_publication_baseline_required',
    'required_physical_publication_contract_version',
    'authority_fingerprint_version',
    'authority_fingerprint',
  ]) {
    assert.ok(enqueue.includes(marker), `enqueue is missing fingerprint marker: ${marker}`);
    assert.ok(clone.includes(marker), `clone is missing fingerprint marker: ${marker}`);
  }
  assert.match(clone, /extensions\.digest\(pg_catalog\.convert_to\(v_authority_fingerprint_text,'UTF8'\),'sha256'\)/);
  assert.equal((clone.match(/payload_json->>'authority_fingerprint_version'/g) || []).length, 4);
  assert.equal((clone.match(/payload_json->>'authority_fingerprint'/g) || []).length, 4);
  assert.equal((clone.match(/payload_json->>'source_publication_baseline_required'/g) || []).length, 4);
  assert.equal((clone.match(/payload_json->>'required_physical_publication_contract_version'/g) || []).length, 4);
});

test('exhausted delivered attempts compose the existing repair owner and prove public convergence', () => {
  const recoveryStart = claimStart.indexOf('DELIVERED_ATTEMPT_EXHAUSTED');
  const normalClaimStart = claimStart.indexOf("v_source_build_run_id := (v_job.payload_json->>'source_build_run_id')::uuid");
  assert.ok(recoveryStart >= 0 && normalClaimStart > recoveryStart);
  const recovery = claimStart.slice(recoveryStart, normalClaimStart);
  assert.match(recovery, /pay_workbench_repair_orphaned_pending_source_build/);
  assert.match(recovery, /PAY_WORKBENCH_EXHAUSTED_ATTEMPT_CONVERGENCE_UNPROVEN/);
  assert.match(recovery, /REBOUND_ACTIVE_SUCCESSOR/);
  assert.match(recovery, /RECONCILED_SUCCESSFUL_BUILD/);
  assert.match(recovery, /FAILED_CLOSED_MAX_ATTEMPTS/);
  assert.match(recovery, /recovery_required_count/);
  assert.match(recovery, /pending_scope_without_active_job/);
  assert.match(recovery, /v_terminal_candidate_state_present:=FOUND/);
  assert.doesNotMatch(recovery, /TERMINAL_CANDIDATE_STATE_MISSING/);
});
