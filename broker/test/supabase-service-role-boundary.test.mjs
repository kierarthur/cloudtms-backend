import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const brokerSource = readFileSync(new URL('../src/index.js', import.meta.url), 'utf8');

function functionSource(name) {
  const marker = `async function ${name}`;
  const start = brokerSource.indexOf(marker);
  assert.notEqual(start, -1, `${name} must remain present in the broker source`);

  const nextFunction = brokerSource.indexOf('\nasync function ', start + marker.length);
  return brokerSource.slice(start, nextFunction === -1 ? brokerSource.length : nextFunction);
}

test('legacy weekly correction lookup fails closed without the service-role credential', () => {
  const source = functionSource('applyWeeklyHoursCorrections');

  assert.match(source, /const key = env\.SUPABASE_SERVICE_ROLE_KEY;/);
  assert.doesNotMatch(source, /SUPABASE_SERVICE_KEY/);
  assert.doesNotMatch(source, /SUPABASE_ANON_KEY/);
  assert.match(
    source,
    /missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY for contract self_bill lookup/
  );
});
