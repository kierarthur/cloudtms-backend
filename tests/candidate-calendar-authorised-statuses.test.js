import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';

const root = process.cwd();
const sql = fs.readFileSync(
  path.join(root, 'supabase', 'repeatable', '25082026_1040_candidate_calendar_authorised_statuses.sql'),
  'utf8'
);

test('candidate calendar keeps the public RPC result contract and uses the approved state vocabulary', () => {
  assert.match(sql, /create or replace function public\.calendar_candidate_day_feed\(/i);
  assert.match(sql, /returns table\s*\([\s\S]*?invoice_nos text\s*\)/i);

  for (const state of [
    'PLANNED',
    'NEEDS_ATTENTION',
    'AWAITING_AUTHORISATION',
    'AUTHORISED',
    'INVOICED',
    'PAID',
    'ON_HOLD',
    'EMPTY'
  ]) {
    assert.match(sql, new RegExp(`'${state}'`));
  }

  assert.doesNotMatch(sql, /then 'READY'/i);
  assert.doesNotMatch(sql, /then 'PROCESSED_NOT_READY'/i);
  assert.doesNotMatch(sql, /then 'AUTHORISED_FOR_INVOICING'/i);
});

test('candidate calendar reads only current active timesheet and financial versions', () => {
  assert.match(sql, /timesheet\.is_current = true/i);
  assert.match(sql, /timesheet\.archived_at_utc is null/i);
  assert.match(sql, /timesheet\.revoked_at is null/i);
  assert.match(sql, /financial\.is_current = true/i);
});

test('candidate calendar derives lifecycle from canonical processing, invoice and pay display truth', () => {
  assert.match(sql, /timesheet_summary_pay_state_cache/i);
  assert.match(sql, /summary_state_applies/i);
  assert.match(sql, /timesheet_pay_state/i);
  assert.match(sql, /READY_FOR_INVOICE/i);
  assert.match(sql, /PENDING_AUTH/i);
  assert.match(sql, /public\.invoices/i);
  assert.match(sql, /pay_status_code in \('PAID', 'OVERPAID'\)/i);
});

test('candidate calendar remains read-only and preserves Policy X boundaries', () => {
  assert.match(sql, /Policy X boundary/i);
  assert.doesNotMatch(sql, /\b(insert|update|delete|merge|truncate)\s+(?:into\s+)?public\./i);
});
