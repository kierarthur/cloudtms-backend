import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { routineDefinitions } from '../scripts/bpay-next-final-definers.mjs';

const source = type => `create or replace function private.range_probe(p_window ${type})
returns boolean language sql as $$ select true $$;`;

test('native daterange identities remain exact for named, qualified and array parameters', () => {
  for (const type of ['daterange', 'pg_catalog.daterange']) {
    assert.deepEqual(routineDefinitions(source(type), 'range-probe.sql'),
      ['private.range_probe(daterange)']);
    assert.deepEqual(routineDefinitions(source(type+'[]'), 'range-probe.sql'),
      ['private.range_probe(daterange[])']);
  }
  assert.deepEqual(routineDefinitions(source('daterange').replace('p_window ', ''), 'range-probe.sql'),
    ['private.range_probe(daterange)']);
});

test('the complete current Source coverage owner is classified without changing its SQL', () => {
  const sql = fs.readFileSync(new URL('../supabase/repeatable/15102026_0205_healthroster_source_arbiter_kernel/02_complete_coverage_and_dispatch.sqlinc', import.meta.url), 'utf8');
  const signatures = routineDefinitions(sql, 'current-source-coverage.sqlinc');
  assert(signatures.some(s => s.includes('daterange')));
  assert(signatures.every(s => s.startsWith('private.')));
});

test('unknown and unsafe catalog type identities still fail closed', () => {
  assert.throws(() => routineDefinitions(source('unknown_range'), 'range-probe.sql'),
    /BPAY_NEXT_SIGNATURE_UNQUALIFIED_TYPE_UNCLASSIFIED/);
  assert.throws(() => routineDefinitions(source('pg_catalog.unknown_range'), 'range-probe.sql'),
    /BPAY_NEXT_SIGNATURE_QUALIFIED_TYPE_UNCLASSIFIED/);
  assert.throws(() => routineDefinitions(source('pg_catalog.char'), 'range-probe.sql'),
    /BPAY_NEXT_SIGNATURE_QUALIFIED_TYPE_UNCLASSIFIED/);
});
