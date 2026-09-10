import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const source = readFileSync(
  new URL(
    '../supabase/repeatable/10092026_0545_candidate_terminal_expense_owner_sync_v1.sql',
    import.meta.url
  ),
  'utf8'
);

test('terminal expense sync keeps the exact revoked weekly owner as history', () => {
  assert.match(
    source,
    /v_workflow\.state in \(''CANCELLED'',''EXPIRED'',''SUPERSEDED'',''REJECTED'',''REFUSED''\)/
  );
  assert.match(
    source,
    /where financial\.timesheet_id=v_workflow\.target_timesheet_id\\n      order by financial\.is_current desc/
  );
  assert.match(
    source,
    /where timesheet\.timesheet_id=v_workflow\.target_timesheet_id;\\n      if not found/
  );
});

test('live workflow sync still requires one current unarchived weekly owner', () => {
  assert.match(
    source,
    /where financial\.timesheet_id=v_workflow\.target_timesheet_id and financial\.is_current/
  );
  assert.match(
    source,
    /where timesheet\.timesheet_id=v_workflow\.target_timesheet_id and timesheet\.is_current/
  );
  assert.match(source, /v_timesheet\.archived_at_utc is not null/);
  assert.match(source, /v_timesheet\.sheet_scope<>''WEEKLY''::public\.timesheet_scope_enum/);
});

test('the closure is drift guarded and preserves the private ACL', () => {
  assert.match(source, /CANDIDATE_TERMINAL_EXPENSE_OWNER_SYNC_DRIFT/);
  assert.match(
    source,
    /revoke all on function private\._candidate_expense_components_sync_v1\(uuid,timestamptz\)[\s\S]*?from public,anon,authenticated,service_role;/
  );
  assert.doesNotMatch(source, /grant execute on function private\._candidate_expense_components_sync_v1/);
});
