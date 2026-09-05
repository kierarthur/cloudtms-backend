const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawnSync } = require('node:child_process');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const historicalPath = 'supabase/repeatable/04082026_1147_pay_payment_correction_selection_prepare_chunk_v1.sql';
const replacementPath = 'supabase/repeatable/05092026_0405_banking_pay_one_candidate_cancellation_scope_integrity_v1.sql';
const generatorPath = 'scripts/generate-banking-pay-one-candidate-cancellation-scope-integrity-v1.mjs';
const runnerPath = 'scripts/verify-banking-pay-draft-v1-v8-cancellation-parity.mjs';
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), 'utf8').replace(/\r\n/g, '\n');
const sha256 = (text) => crypto.createHash('sha256').update(text).digest('hex');

const historical = read(historicalPath);
const replacement = read(replacementPath);
const runner = read(runnerPath);

const completeUniverseScan = [
  '          AND (v_cursor_candidate_id IS NULL OR candidate_row.id > v_cursor_candidate_id)',
  '        -- Both sides of the unchanged-unselected integrity proof must page the',
  '        -- same complete frozen batch universe.  EXPLICIT still selects only'
].join('\n');
const selectedOnlyReversion = [
  "                v_mode <> 'EXPLICIT'",
  '                OR candidate_row.id::text IN (',
  '                  SELECT explicit_token.value',
  '                  FROM pg_catalog.jsonb_array_elements_text(',
  "                    COALESCE(v_selection->'canonical_explicit_candidate_tokens', '[]'::jsonb)"
].join('\n');
const unselectedAudit = [
  "        IF v_mode = 'EXPLICIT' AND NOT v_candidate_explicitly_selected THEN",
  '          -- This is the exact audit shape recomputed by FINALISE.  It proves'
].join('\n');
const completeUniverseContinuation = [
  '    SELECT EXISTS (',
  '      SELECT 1',
  '      FROM public.pay_batch_candidates AS remaining_candidate',
  '      WHERE remaining_candidate.pay_batch_id = v_batch.id',
  '        AND (',
  '          v_scan_last_candidate_id IS NULL',
  '          OR remaining_candidate.id > v_scan_last_candidate_id'
].join('\n');
const runnerFinancialRequirements = [
  'function readCandidateCancellationFinancials(target, candidateToken) {',
  'selectedCandidateFinancialsAfter.voided_item_count - selectedCandidateFinancialsBefore.voided_item_count',
  "for (const amountKind of ['ex_vat', 'vat', 'inc_vat']) {",
  'assert.equal(selectedCandidateFinancialsAfter.reservation_count, selectedCandidateFinancialsBefore.reservation_count);',
  'selectedCandidateFinancialsAfter.released_reservation_count - selectedCandidateFinancialsBefore.released_reservation_count',
  'assert.equal(after.voided_ex_vat_pence, before.active_ex_vat_pence);',
  'assert.equal(after.voided_vat_pence, before.active_vat_pence);',
  'assert.equal(after.voided_inc_vat_pence, before.active_inc_vat_pence);',
  'assert.equal(after.reservation_count, before.reservation_count);',
  'after.released_reservation_count - before.released_reservation_count'
];

function integrityViolations(source) {
  const requirements = [
    [completeUniverseScan, 'complete frozen Candidate universe scan'],
    [selectedOnlyReversion, 'selected-only cancellation reversion input'],
    [unselectedAudit, 'unselected Candidate exact finalizer audit shape'],
    [completeUniverseContinuation, 'complete-universe pagination continuation']
  ];
  return requirements.filter(([needle]) => !source.includes(needle)).map(([, label]) => label);
}

test('historical selection owner remains byte-identical and generated replacement is reproducible', () => {
  assert.equal(sha256(historical), '8fc121202bfb96f537c2d0a307a61410cd177d764b4f14016ed9efbfc2796ca5');
  assert.equal(sha256(replacement), '47d79ef8f50a6a313e2ac20d0ff9b6e4d6ec5ee6c99a7d0be376724564b10634');

  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'cloudtms-h2-cancel-scope-'));
  const generatedPath = path.join(tempRoot, 'generated.sql');
  try {
    const generated = spawnSync(process.execPath, [path.join(root, generatorPath)], {
      cwd: root,
      encoding: 'utf8',
      env: { ...process.env, H2_ONE_CANDIDATE_SCOPE_OUTPUT: generatedPath },
      windowsHide: true
    });
    assert.equal(generated.status, 0, generated.stderr || generated.stdout);
    assert.equal(fs.readFileSync(generatedPath, 'utf8').replace(/\r\n/g, '\n'), replacement);
  } finally {
    fs.rmSync(tempRoot, { recursive: true, force: true });
  }
});

test('replacement is one exact function with unchanged service-only metadata and budgets', () => {
  assert.equal((replacement.match(/CREATE OR REPLACE FUNCTION public\.pay_payment_correction_selection_prepare_chunk_v1\s*\(/g) || []).length, 1);
  assert.equal((replacement.match(/CREATE OR REPLACE FUNCTION /g) || []).length, 1);
  assert.match(replacement, /p_correction_request_id uuid,[\s\S]*p_operation_id uuid,[\s\S]*p_cursor_json jsonb DEFAULT NULL::jsonb,[\s\S]*p_limit integer DEFAULT 50,[\s\S]*p_worker_id text DEFAULT NULL::text,[\s\S]*p_actor_user_id uuid DEFAULT NULL::uuid/);
  assert.match(replacement, /LANGUAGE plpgsql\s+VOLATILE\s+PARALLEL UNSAFE\s+SECURITY DEFINER/);
  assert.match(replacement, /SET search_path TO pg_catalog, private, extensions, pg_temp/);
  assert.match(replacement, /SET statement_timeout TO '6000ms'/);
  assert.match(replacement, /SET lock_timeout TO '1000ms'/);
  assert.match(replacement, /ALTER FUNCTION public\.pay_payment_correction_selection_prepare_chunk_v1\(uuid,uuid,jsonb,integer,text,uuid\) OWNER TO postgres/);
  assert.match(replacement, /REVOKE ALL ON FUNCTION public\.pay_payment_correction_selection_prepare_chunk_v1\(uuid,uuid,jsonb,integer,text,uuid\) FROM PUBLIC/);
  assert.match(replacement, /REVOKE ALL ON FUNCTION public\.pay_payment_correction_selection_prepare_chunk_v1\(uuid,uuid,jsonb,integer,text,uuid\) FROM anon/);
  assert.match(replacement, /REVOKE ALL ON FUNCTION public\.pay_payment_correction_selection_prepare_chunk_v1\(uuid,uuid,jsonb,integer,text,uuid\) FROM authenticated/);
  assert.match(replacement, /GRANT EXECUTE ON FUNCTION public\.pay_payment_correction_selection_prepare_chunk_v1\(uuid,uuid,jsonb,integer,text,uuid\) TO service_role/);
});

test('EXPLICIT cancellation fingerprints untouched Candidates but mutates only requested membership', () => {
  assert.deepEqual(integrityViolations(replacement), []);
  assert.match(replacement, /v_candidate_explicitly_selected boolean := false/);
  assert.match(replacement, /IF v_mode = 'EXPLICIT' AND NOT v_candidate_explicitly_selected THEN[\s\S]*v_unselected_chain_hash := private\.pay_payment_correction_sha256_v1\([\s\S]*CONTINUE;/);
  assert.match(replacement, /IF v_item_count < 1 AND v_mode = 'EXPLICIT' THEN[\s\S]*PAYMENT_CORRECTION_SELECTION_EMPTY/);
  assert.doesNotMatch(completeUniverseScan, /canonical_explicit_candidate_tokens/);
});

test('parity runner proves one-Candidate isolation, open-Draft state and zero provider effects', () => {
  assert.match(runner, /cancellationScope === 'ONE_CANDIDATE'/);
  assert.match(runner, /assert\.equal\(after\.batch_status, 'DRAFT'\)/);
  assert.match(runner, /assert\.equal\(after\.active_item_count, before\.active_item_count - selectedCandidate\.active_item_count\)/);
  assert.match(runner, /assert\.equal\(after\.voided_item_count, selectedCandidate\.active_item_count\)/);
  assert.match(runner, /assert\.deepEqual\(unselectedCandidateAuthorityAfter, unselectedCandidateAuthorityBefore\)/);
  assert.match(runner, /assert\.equal\(after\.provider_attempt_count, before\.provider_attempt_count\)/);
  assert.match(runner, /assert\.equal\(after\.provider_event_count, before\.provider_event_count\)/);
  for (const requirement of runnerFinancialRequirements) {
    assert.ok(runner.includes(requirement), `missing exact cancellation financial proof: ${requirement}`);
  }
});

test('every cancellation financial reconciliation boundary has a killed mutation', () => {
  for (const requirement of runnerFinancialRequirements) {
    const mutated = runner.replace(requirement, `REMOVED_${crypto.randomUUID()}`);
    assert.notEqual(mutated, runner, `mutation target missing: ${requirement}`);
    assert.ok(
      runnerFinancialRequirements.some((candidate) => !mutated.includes(candidate)),
      `financial mutation survived: ${requirement}`
    );
  }
});

test('every scope-integrity boundary has a mutation killed by the source validator', () => {
  for (const operator of [completeUniverseScan, selectedOnlyReversion, unselectedAudit, completeUniverseContinuation]) {
    const mutated = replacement.replace(operator, `REMOVED_${crypto.randomUUID()}`);
    assert.notEqual(mutated, replacement, `mutation target missing: ${operator}`);
    assert.ok(integrityViolations(mutated).length > 0, `mutation survived: ${operator}`);
  }
});
