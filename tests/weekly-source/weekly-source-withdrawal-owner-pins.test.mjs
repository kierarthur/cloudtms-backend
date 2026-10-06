import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';

const fixture = readFileSync(new URL('../../supabase/verification/17092026_0610_weekly_source_withdrawal_supersession_v1.sql', import.meta.url), 'utf8');

test('withdrawal verifier retains the unchanged ordinary Unauthorise owner pin', () => {
  assert.match(fixture, /p\.proname='timesheet_unauthorise_atomic'\)[\s\S]*?='e12dcdb94291d1e840bf8961e50be2cd'/);
});

test('withdrawal verifier requires the exact jointly reviewed current CALL-only successors', () => {
  assert.match(fixture, /p\.proname='timesheet_authorise_generic_atomic'\)\s*='643b1bf291f93b2ab92e5b9b76c8ef69'/);
  assert.match(fixture, /p\.proname='pay_workbench_scope_invalidate_v1'\)\s*='31dff4424dcbf63f476451f59d9025f4'/);
  assert.doesNotMatch(fixture, /cd5f05df8e4be03dec4b3f1bc56adaa6|0d26de465bc221f6a41043fb27c8d797/);
});

test('pin reconciliation cannot redefine owners or remove service/security assertions', () => {
  assert.doesNotMatch(fixture, /create\s+(?:or\s+replace\s+)?function\s+(?:public\.timesheet_(?:un)?authorise(?:_generic)?_atomic|private\.pay_workbench_scope_invalidate_v1)\s*\(/i);
  assert.match(fixture, /exactly one Workbench selector definition/);
  assert.match(fixture, /not pg_catalog\.has_function_privilege\('anon'/);
  assert.match(fixture, /not pg_catalog\.has_function_privilege\('authenticated'/);
  assert.match(fixture, /pg_catalog\.has_function_privilege\('service_role'/);
  assert.match(fixture, /ordinary owner is untouched/);
});
