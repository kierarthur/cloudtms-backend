// WP-16d. The 29-area rule set and its measurement, checked without a database.

import test from 'node:test';
import assert from 'node:assert/strict';
import {
  WEEKLY_SOURCE_NEW_OWNER_PATTERN,
  WEEKLY_SOURCE_PROTECTED_AREA_RULES,
} from './differential-protected-area-rules.mjs';
import { excludedOwnerSqlFor, measurementSqlFor, sqlLiteral } from './differential-protected-area-measure.mjs';
import { assertDistinctBuilds } from './differential-protected-area-runner.mjs';
import { WEEKLY_SOURCE_DIFFERENTIAL_ADAPTER_CONTRACT } from '../harness/differential-protection-contract.mjs';

test('the rule set covers every controlling protected ID exactly once', () => {
  const ids = WEEKLY_SOURCE_PROTECTED_AREA_RULES.map((rule) => rule.protectionId);
  assert.equal(ids.length, WEEKLY_SOURCE_DIFFERENTIAL_ADAPTER_CONTRACT.protectedAreaCount);
  assert.equal(new Set(ids).size, ids.length);
  for (const added of WEEKLY_SOURCE_DIFFERENTIAL_ADAPTER_CONTRACT.newInPlan62) {
    const rule = WEEKLY_SOURCE_PROTECTED_AREA_RULES.find((item) => item.protectionId === added.protectionId);
    assert.ok(rule, `${added.protectionId} has no rule`);
    assert.equal(rule.requiredDifferential, added.requiredDifferential);
    assert.ok(!rule.unmeasurable, `${added.protectionId} may not be declared unmeasurable`);
  }
});

test('every measurable rule produces a statement, and every unmeasurable one gives a reason', () => {
  for (const rule of WEEKLY_SOURCE_PROTECTED_AREA_RULES) {
    if (rule.unmeasurable) {
      assert.ok(rule.unmeasurable.length > 60, `${rule.protectionId} must say why and what is needed`);
      continue;
    }
    const sql = measurementSqlFor(rule, WEEKLY_SOURCE_NEW_OWNER_PATTERN);
    assert.match(sql, /order by row_text;$/);
    assert.ok(sql.includes('select row_text from ('), rule.protectionId);
  }
});

test('the new-source-owner exclusion is applied only where the rule asks for it', () => {
  const excluded = new Set();
  for (const rule of WEEKLY_SOURCE_PROTECTED_AREA_RULES) {
    if (rule.unmeasurable) continue;
    const sql = measurementSqlFor(rule, WEEKLY_SOURCE_NEW_OWNER_PATTERN);
    const applied = sql.includes(sqlLiteral(WEEKLY_SOURCE_NEW_OWNER_PATTERN));
    if (rule.excludeNewSourceOwners && !rule.browserReachable && !rule.configurationSurface) {
      assert.ok(applied, `${rule.protectionId} asked for the exclusion and did not get it`);
      assert.ok(excludedOwnerSqlFor(rule, WEEKLY_SOURCE_NEW_OWNER_PATTERN),
        `${rule.protectionId} must be able to report how many owners it excluded`);
      excluded.add(rule.protectionId);
    } else {
      assert.ok(!applied, `${rule.protectionId} must not exclude anything`);
    }
  }
  // The four areas whose forbidden change is precisely a new source owner arriving.
  for (const id of ['PROT-SEC-001', 'PROT-ADV-001', 'PROT-BANKALERT-001', 'PROT-INFRA-001']) {
    assert.ok(!excluded.has(id), `${id} must measure new source owners, not exclude them`);
  }
});

test('a pair of identical builds is refused, because it would prove nothing', () => {
  const identity = { migrations: 241, repeatables: 645, routines: 1901 };
  assert.throws(() => assertDistinctBuilds(identity, { ...identity }),
    (error) => error.code === 'WEEKLY_SOURCE_DIFFERENTIAL_SAME_STATE');
  assert.equal(assertDistinctBuilds({ migrations: 235, repeatables: 611, routines: 1540 }, identity), true);
});
