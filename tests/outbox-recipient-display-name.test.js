import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const sql = readFileSync(
  new URL('../supabase/repeatable/04032026_mailshots.sql', import.meta.url),
  'utf8',
);

function functionBody(name) {
  const start = sql.toLowerCase().indexOf(`create or replace function public.${name}(`);
  assert.notEqual(start, -1, `${name} definition is missing`);

  const end = sql.indexOf('\n$$;', start);
  assert.notEqual(end, -1, `${name} definition is incomplete`);
  return sql.slice(start, end);
}

const candidateNameFallback = /when lower\(coalesce\(u\.recipient_kind, ''\)\) = 'candidate' then coalesce\(\s*nullif\(btrim\(c_rec\.display_name\), ''\),\s*nullif\(btrim\(concat_ws\(' ', c_rec\.first_name, c_rec\.last_name\)\), ''\),\s*nullif\(btrim\(c_rec\.email\), ''\),\s*nullif\(btrim\(c_rec\.phone\), ''\),\s*nullif\(btrim\(u\.to_address\), ''\)\s*\)/i;

for (const name of ['outbox_unified_list', 'outbox_unified_get']) {
  test(`${name} skips a blank candidate display name before selecting first and last name`, () => {
    const body = functionBody(name);

    assert.match(body, candidateNameFallback);
    assert.doesNotMatch(
      body,
      /coalesce\(c_rec\.display_name, concat_ws\(' ', c_rec\.first_name, c_rec\.last_name\)/i,
    );
  });
}
