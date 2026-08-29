import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const sql = readFileSync(
  new URL('../supabase/repeatable/27082026_2350_candidate_timesheet_card_base_expense_fallback_v1.sql', import.meta.url),
  'utf8',
);
const verification = readFileSync(
  new URL('../supabase/verification/27082026_2351_candidate_timesheet_card_base_expense_fallback_verification.sql', import.meta.url),
  'utf8',
);

test('timesheet cards prefer a separate expense carrier and otherwise use the combined timesheet totals', () => {
  for (const field of [
    'expenses_pay_ex_vat',
    'mileage_pay_ex_vat',
    'travel_pay_ex_vat',
    'accommodation_pay_ex_vat',
    'other_pay_ex_vat',
  ]) {
    assert.match(
      sql,
      new RegExp(`coalesce\\(totals\\.${field},base\\.${field},0\\) as overlay_${field}`, 'i'),
    );
    assert.doesNotMatch(
      sql,
      new RegExp(`coalesce\\(totals\\.${field},0\\) as overlay_${field}`, 'i'),
    );
  }
});

test('the replacement preserves draft linkage and the service-only browser boundary', () => {
  assert.match(sql, /classified\.state in \('CREATED','WORKER_DRAFT'\)/i);
  assert.match(sql, /workflows\.display_timesheet_id=coalesce\(base\.timesheet_id,base\.id\)/i);
  assert.match(sql, /security definer/i);
  assert.match(sql, /revoke all on function public\.candidate_app_timesheet_page_v1[\s\S]*from public, anon, authenticated/i);
  assert.match(sql, /grant execute on function public\.candidate_app_timesheet_page_v1[\s\S]*to service_role/i);
  assert.match(verification, /CANDIDATE_TIMESHEET_CARD_BASE_EXPENSE_FALLBACK_MISSING/);
  assert.match(verification, /CANDIDATE_TIMESHEET_PAGE_BROWSER_EXECUTE_EXPOSED/);
});
