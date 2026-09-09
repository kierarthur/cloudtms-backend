const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const repoRoot = path.resolve(__dirname, '..');
const historicalPath = path.join(
  repoRoot,
  'supabase',
  'repeatable',
  '04092026_2350_banking_pay_cancellation_completion_v1.sql'
);
const ownerPath = path.join(
  repoRoot,
  'supabase',
  'repeatable',
  '09092026_0020_banking_pay_no_money_workbench_return_v1.sql'
);
const preparePath = path.join(
  repoRoot,
  'supabase',
  'repeatable',
  '08092026_0510_banking_pay_payment_correction_prepare_idempotency_v1.sql'
);
const workerPath = path.join(repoRoot, 'broker', 'src', 'index.js');
const verifierPath = path.join(
  repoRoot,
  'supabase',
  'verification',
  '09092026_0022_banking_pay_no_money_workbench_return_verification.sql'
);

const oldGate =
  "ELSIF v_requested_action IN ('DRAFT_CANCEL','PRE_BANK_CANCEL','CANCEL_PAYMENT','NO_MONEY_UNWIND') THEN";
const newGate =
  "ELSIF v_requested_action IN ('DRAFT_CANCEL','PRE_BANK_CANCEL','CANCEL_PAYMENT','NO_MONEY_RELEASE','NO_MONEY_UNWIND') THEN";

function sha256(value) {
  return crypto.createHash('sha256').update(value).digest('hex');
}

function readLf(filePath) {
  return fs.readFileSync(filePath, 'utf8').replaceAll('\r\n', '\n');
}

function candidateBody(source) {
  const start = source.indexOf(
    'CREATE OR REPLACE FUNCTION public.pay_payment_correction_process_chunk('
  );
  assert.ok(start >= 0);
  return source.slice(start);
}

test('historical cancellation owner remains the exact immutable source', () => {
  const bytes = fs.readFileSync(historicalPath);
  assert.equal(
    sha256(bytes),
    '60e5fc26fbd147991c16ada0aaf2a9691143ccf46aa6e3227dc78d823a7a7ce1'
  );
  const source = bytes.toString('utf8').replaceAll('\r\n', '\n');
  assert.equal(source.split(oldGate).length - 1, 1);
  assert.equal(source.includes(newGate), false);
});

test('replacement changes only the canonical no-money action admission', () => {
  const historical = candidateBody(readLf(historicalPath));
  const candidate = candidateBody(readLf(ownerPath));
  assert.equal((candidate.match(/CREATE OR REPLACE FUNCTION /g) || []).length, 1);
  assert.equal(candidate.split(newGate).length - 1, 1);
  assert.equal(candidate.includes(oldGate), false);
  assert.equal(candidate.replace(newGate, oldGate), historical);
  assert.match(candidate, /ALTER FUNCTION public\.pay_payment_correction_process_chunk\(uuid,integer,text,uuid\) OWNER TO postgres;/);
  assert.match(candidate, /SET statement_timeout TO '6000ms'/);
  assert.match(candidate, /SET lock_timeout TO '1000ms'/);
  assert.match(candidate, /GRANT EXECUTE ON FUNCTION public\.pay_payment_correction_process_chunk\(uuid,integer,text,uuid\) TO service_role;/);
});

test('public no-money action and internal work kind remain deliberately distinct', () => {
  const prepare = readLf(preparePath);
  const worker = readLf(workerPath);
  assert.match(
    worker,
    /matchPath\(p, '\/api\/banking\/pay\/batch\/:id\/payment-status\/release-failed'\)[\s\S]{0,180}handleBankingPayCorrectionPlanV1\(env, req, user, m\.id, 'NO_MONEY_RELEASE'\)/
  );
  assert.match(
    prepare,
    /v_action NOT IN \('DRAFT_CANCEL', 'PRE_BANK_CANCEL', 'CANCEL_PAYMENT', 'NO_MONEY_RELEASE', 'NO_MONEY_UNWIND'\)/
  );
  assert.match(
    prepare,
    /v_correction_kind := CASE WHEN v_action IN \('NO_MONEY_RELEASE', 'NO_MONEY_UNWIND'\)[\s\S]{0,120}THEN 'NO_MONEY_UNWIND' ELSE 'PRE_BANK_CANCEL' END;/
  );
});

test('boundary mutations cannot erase or broaden the one approved alias gate', () => {
  const candidate = candidateBody(readLf(ownerPath));
  const mutations = [
    candidate.replace("'NO_MONEY_RELEASE','NO_MONEY_UNWIND'", "'NO_MONEY_UNWIND'"),
    candidate.replace("'NO_MONEY_RELEASE','NO_MONEY_UNWIND'", "'NO_MONEY_RELEASE'"),
    candidate.replace("'NO_MONEY_RELEASE','NO_MONEY_UNWIND'", "'NO_MONEY_RELEASE','NO_MONEY_UNWIND','SETTLED_REVERSAL'"),
    candidate.replace(newGate, newGate.replace(' THEN', ' AND true THEN'))
  ];
  const historical = candidateBody(readLf(historicalPath));
  for (const mutation of mutations) {
    assert.notEqual(mutation, candidate);
    const exactGateCount = mutation.split(newGate).length - 1;
    const inverseIsHistorical = mutation.replace(newGate, oldGate) === historical;
    assert.equal(exactGateCount === 1 && inverseIsHistorical, false);
  }
});

test('release verifier freezes metadata, ACL and the one exact handoff gate', () => {
  const verifier = readLf(verifierPath);
  assert.match(verifier, /BANKING_PAY_NO_MONEY_WORKBENCH_RETURN_GATE_INVALID/);
  assert.match(verifier, /v_gate_count IS DISTINCT FROM 1/);
  assert.match(verifier, /statement_timeout=6000ms/);
  assert.match(verifier, /lock_timeout=1000ms/);
  assert.match(verifier, /pg_catalog\.pg_get_userbyid\(procedure_row\.proowner\) = current_user/);
  assert.doesNotMatch(verifier, /pg_catalog\.pg_get_userbyid\(procedure_row\.proowner\) = 'postgres'/);
  assert.match(verifier, /pg_catalog\.has_function_privilege\('anon'/);
  assert.match(verifier, /pg_catalog\.has_function_privilege\('authenticated'/);
  assert.match(verifier, /NOT pg_catalog\.has_function_privilege\('service_role'/);
  assert.doesNotMatch(verifier, /GRANT|ALTER FUNCTION|CREATE OR REPLACE FUNCTION/);
});
