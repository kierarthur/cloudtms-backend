const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const sql = fs.readFileSync(path.join(__dirname, '../supabase/repeatable/22092026_1226_client_initial_settings_baseline_v1.sql'), 'utf8');
const start = sql.indexOf('CREATE OR REPLACE FUNCTION public.client_create_with_settings_v1(');
const create = sql.slice(start, sql.indexOf('$function$;', start) + 11);
test('initial client settings use server-owned 1900 baseline, not request or current date', () => {
  assert.ok(start >= 0);
  assert.match(create, /v_settings_input\.margin_includes,\s*--[^\n]*\n\s*DATE '1900-01-01',\s*v_now,/);
  assert.doesNotMatch(create, /COALESCE\(\s*v_settings_input\.effective_from/);
});
test('canonical initial date is replay-safe without changing legacy comparison', () => {
  assert.match(create, /WHERE NOT \(requested_key\.key_name = 'effective_from'\s+AND v_existing_settings\.effective_from = DATE '1900-01-01'\)/);
  assert.match(create, /CLIENT_CREATE_IDEMPOTENCY_CONFLICT/);
  assert.match(create, /pg_try_advisory_xact_lock/);
});
