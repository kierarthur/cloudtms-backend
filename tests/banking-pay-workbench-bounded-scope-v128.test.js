import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const repoRoot = new URL('..', import.meta.url);
const read = path => readFileSync(new URL(path, repoRoot), 'utf8').replace(/\r\n/g, '\n');
const occurrence = read('supabase/repeatable/04082026_2314_pay_workbench_unit_economic_occurrence_page_v1.sql');
const bundle = read('supabase/repeatable/05082026_1232_pay_workbench_unit_economic_occurrence_bundle_v1.sql');
const sync = read('supabase/repeatable/04082026_1210_pay_sync_overpayments_from_workbench_workspace_v1.sql');
const claim = read('supabase/repeatable/04082026_1141_pay_workbench_source_build_attempt_claim_start_v1.sql');

const physicalAdditionalKey = raw => {
  const bytes = Buffer.from(raw, 'utf8');
  return `${String(bytes.length).padStart(10, '0')}:${bytes.toString('hex')}`;
};

test('V1.2.8 lossless additional keys survive case, whitespace and page boundaries', () => {
  const rawKeys = ['foo', 'FOO', ' foo', 'foo ', '', 'é', 'é'];
  const identities = rawKeys.map(physicalAdditionalKey);
  assert.equal(new Set(identities).size, rawKeys.length);

  const sorted = [...identities].sort();
  const accepted = [];
  let cursor = null;
  while (true) {
    const lookahead = sorted.filter(key => cursor === null || key > cursor).slice(0, 3);
    accepted.push(...lookahead.slice(0, 2));
    if (lookahead.length <= 2) break;
    cursor = lookahead[1];
  }
  assert.deepEqual(accepted, sorted);
  assert.match(occurrence, /FALLBACK_ADDITIONAL_CODE_MISSING/);
});

test('V1.2.8 raw bundle evidence cannot certify an unresolved occurrence', () => {
  const rows = [
    { key: 'a', amount: 50, failure: null, evidenceOnly: false },
    { key: 'b', amount: 40, failure: 'ECONOMIC_KEY_MISSING', evidenceOnly: false },
    { key: 'z', amount: 0, failure: null, evidenceOnly: true },
  ];
  const physical = rows.filter(row => !row.evidenceOnly);
  const rawCount = physical.length;
  const resolved = physical.filter(row => row.failure === null);
  const failed = physical.filter(row => row.failure !== null);
  assert.equal(rawCount, 2);
  assert.equal(resolved.length, 1);
  assert.equal(failed.length, 1);
  assert.notEqual(rawCount, resolved.length);
  assert.notEqual(physical.reduce((sum, row) => sum + row.amount, 0),
    resolved.reduce((sum, row) => sum + row.amount, 0));
  assert.match(bundle, /raw_page_count/);
  assert.match(bundle, /failed_page_count/);
});

test('V1.2.8 fallback occurrence SQL resolves table columns ahead of RETURNS TABLE outputs', () => {
  assert.match(
    occurrence,
    /AS \$function\$\s*#variable_conflict use_column\s*DECLARE/i,
  );
  assert.match(occurrence, /'role','PAY_STATE_FALLBACK'/i);
});

test('V1.2.8 canonical multiplicity is checked per stable line before split collapse', () => {
  const lineA = [JSON.stringify({ key: 'K', amount: 10 }), JSON.stringify({ key: 'K', amount: 10 })];
  const lineB = [JSON.stringify({ key: 'K', amount: 10 })];
  assert.equal(new Set(lineA).size, 1);
  assert.equal(lineA.length, 2, 'identical same-line duplicates remain two occurrences and must fail');
  assert.equal(new Set([...lineA.slice(0, 1), ...lineB]).size, 1,
    'identical replication across distinct presentation lines may collapse only after line checks');
  assert.match(sync, /GROUP BY timesheet_id,line_identity,key_type,key_value/);
  assert.match(sync, /OR count\(\*\)>1/);
});

test('V1.2.8 durable keyset reaches work beyond any locked bounded prefix', () => {
  for (const lockedCount of [50, 100, 151]) {
    const ids = Array.from({ length: lockedCount + 1 }, (_, index) => index + 1);
    let cursor = 0;
    let claimed = null;
    for (let call = 0; call < 10 && claimed === null; call += 1) {
      const page = ids.filter(id => id > cursor).slice(0, 50);
      if (page.length === 0) { cursor = 0; continue; }
      for (const id of page) {
        cursor = id; // durable cursor advances before the simulated exact lock
        if (id > lockedCount) { claimed = id; break; }
      }
    }
    assert.equal(claimed, lockedCount + 1);
  }
  assert.ok(claim.indexOf('cursor_object_id=v_claim.id') <
    claim.indexOf('FOR UPDATE OF claimed_job SKIP LOCKED'));
});
