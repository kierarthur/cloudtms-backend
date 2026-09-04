import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

const repeatable = await readFile(
  new URL('../supabase/repeatable/04092026_1603_candidate_expense_email_admission_v1.sql', import.meta.url),
  'utf8'
);

test('separated expense claims fail before draft entry when their delivery email is missing', () => {
  assert.match(repeatable, /v_expense_admission_ready:=not v_separate[\s\S]*expense_invoice_email_ready/);
  assert.match(repeatable, /v_reasons:=v_reasons\|\|'"EXPENSE_INVOICE_EMAIL_REQUIRED"'::jsonb/);
  assert.match(repeatable, /'candidate_expenses_allowed',v_expense_route_allowed and v_expense_admission_ready/);
  assert.match(repeatable, /'can_edit_expenses',v_expense_route_allowed and v_expense_admission_ready/);
  assert.match(repeatable, /'can_attach_mileage_evidence',v_expense_route_allowed and v_expense_admission_ready/);
});

test('expense placement preserves the exact configuration failure instead of returning a generic view-only reason', () => {
  const exactGuard = repeatable.indexOf("? 'EXPENSE_INVOICE_EMAIL_REQUIRED'");
  const genericGuard = repeatable.indexOf("'reason_code','CANDIDATE_RECORD_VIEW_ONLY'");
  assert.ok(exactGuard >= 0);
  assert.ok(genericGuard > exactGuard);
  assert.match(repeatable, /'placement','BLOCKED','reason_code','EXPENSE_INVOICE_EMAIL_REQUIRED'/);
});

test('the complete capability and placement authorities retain their security boundaries', () => {
  assert.match(repeatable, /security definer[\s\S]*set search_path = pg_catalog, public, private, extensions, pg_temp/);
  assert.match(repeatable, /revoke all on function private\._candidate_record_capabilities_v1\(uuid,uuid,jsonb\) from public,anon,authenticated,service_role/);
  assert.match(repeatable, /revoke all on function public\.expense_placement_resolve_v1\(uuid,text,uuid,uuid,jsonb,timestamptz\) from public,anon,authenticated/);
  assert.match(repeatable, /grant execute on function public\.expense_placement_resolve_v1\(uuid,text,uuid,uuid,jsonb,timestamptz\) to service_role/);
});
