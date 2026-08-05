import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const repoRoot = new URL('..', import.meta.url);
const read = relativePath => readFileSync(new URL(relativePath, repoRoot), 'utf8');

const bootstrap = read('codex_outputs/banking-pay-bounded-scope-v12/bootstrap-test.sql');
const schema = read('supabase/migrations/04082026_1134_banking_pay_bounded_scope_v12.sql');

test('bootstrap registry insertion uses one stable statement clock for lifecycle timestamps', () => {
  const registryInsertStart = bootstrap.indexOf(
    'INSERT INTO private.banking_pay_workbench_candidate_scope_registry(',
  );
  const jobInsertStart = bootstrap.indexOf(
    'INSERT INTO public.banking_pay_workbench_jobs(',
    registryInsertStart,
  );
  assert.ok(registryInsertStart >= 0 && jobInsertStart > registryInsertStart);

  const registryInsert = bootstrap.slice(registryInsertStart, jobInsertStart);
  assert.match(
    registryInsert,
    /statement_timestamp\(\),statement_timestamp\(\),statement_timestamp\(\)/i,
  );
  assert.doesNotMatch(
    registryInsert,
    /clock_timestamp\(\),clock_timestamp\(\),clock_timestamp\(\)/i,
  );
});

test('bootstrap lifecycle timestamps satisfy the installed terminal ordering constraint', () => {
  assert.match(
    schema,
    /last_dirtied_at_utc\s*>=\s*created_at_utc/i,
  );
  assert.match(
    schema,
    /updated_at_utc\s*>=\s*created_at_utc/i,
  );
  assert.match(bootstrap, /BEGIN;[\s\S]*COMMIT;/i);
});

test('bootstrap remains bounded queue activation rather than direct financial execution', () => {
  assert.match(bootstrap, /WORKBENCH_CANDIDATE_SOURCE_BUILD/i);
  assert.match(bootstrap, /legacy_bootstrap',true/i);
  assert.doesNotMatch(
    bootstrap,
    /public\.(?:pay_sync_overpayments|pay_execute_bank|pay_settle_rail|pay_bank_transfers_claim_provider_submit|pay_remittance)[a-z0-9_]*\s*\(/i,
  );
});
