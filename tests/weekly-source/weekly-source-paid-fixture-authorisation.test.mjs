import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const sql = readFileSync(new URL('../../supabase/verification/15092026_1534_weekly_source_ordinary_pay_projection_v1.sql', import.meta.url), 'utf8');

test('paid projection fixture establishes genuine first authorisation before simulating payment', () => {
  const authorise = sql.indexOf('create temp table paid_root_first_authorisation as');
  const paid = sql.indexOf('set paid_at_utc=pg_catalog.statement_timestamp()');
  assert.ok(authorise > 0 && paid > authorise);
  const preparation = sql.slice(authorise, paid);
  assert.match(preparation, /public\.weekly_source_first_authorise_v1\(/);
  assert.match(preparation, /receipt\.root_timesheet_id,receipt\.root_timesheet_id,null/);
  assert.match(preparation, /authorisation\.withdrawn_at_utc is null/);
  assert.match(preparation, /financial\.authorised_at_utc is not null/);
  assert.match(preparation, /approval_basis,coverage_complete/);
  assert.doesNotMatch(preparation, /set\s+authorised_at|insert into public\.weekly_source_root_authorisations/i);
});

test('paid fixture preserves refusal and unchanged-row regression assertions', () => {
  assert.match(sql, /create temp table paid_root_refusal as/);
  assert.match(sql, /projection-a6-paid-refusal/);
  assert.match(sql, /paid_root_before/);
  assert.match(sql, /private\.weekly_source_ordinary_projection_root_hash_v1/);
});
