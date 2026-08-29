import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const sql = readFileSync(new URL(
  '../supabase/repeatable/29082026_1847_timesheet_daily_abandoned_receipt_delete_v1.sql',
  import.meta.url
), 'utf8');
const broker = readFileSync(new URL('../broker/src/index.js', import.meta.url), 'utf8');

test('Daily abandoned receipt authority is an exact service-only two-RPC boundary', () => {
  assert.equal((sql.match(/create or replace function public\.timesheet_daily_abandoned_receipt_delete_/gi) || []).length, 2);
  for (const name of [
    'timesheet_daily_abandoned_receipt_delete_preview_v1',
    'timesheet_daily_abandoned_receipt_delete_apply_v1'
  ]) {
    assert.match(sql, new RegExp(`security definer[\\s\\S]*?alter function public\\.${name}`, 'i'));
    assert.match(sql, new RegExp(`revoke all on function public\\.${name}[^;]+ from public`, 'i'));
    assert.match(sql, new RegExp(`revoke all on function public\\.${name}[^;]+ from anon`, 'i'));
    assert.match(sql, new RegExp(`revoke all on function public\\.${name}[^;]+ from authenticated`, 'i'));
    assert.match(sql, new RegExp(`grant execute on function public\\.${name}[^;]+ to service_role`, 'i'));
  }
  assert.match(sql, /notify pgrst\s*,\s*'reload schema'/i);
});

test('preview delegates existing financial deletion classification and fails closed on approval or retained authority', () => {
  assert.match(sql, /v_standard\s*:=\s*public\.timesheet_standard_delete_preview_v1/i);
  assert.match(sql, /DAILY_RECEIPT_ALREADY_AUTHORISED/);
  assert.match(sql, /DAILY_RECEIPT_MANAGER_OR_FINAL_AUTHORITY_EXISTS/);
  assert.match(sql, /DAILY_RECEIPT_MANAGER_APPROVAL_EXISTS/);
  assert.match(sql, /DAILY_RECEIPT_FINAL_DOCUMENT_EXISTS/);
  assert.match(sql, /DAILY_RECEIPT_OFFICE_EVIDENCE_EXISTS/);
  assert.match(sql, /DAILY_RECEIPT_REPLACEMENT_HISTORY_EXISTS/);
  assert.match(sql, /upper\(coalesce\(v_standard->>'decision','BLOCKED'\)\)='PERMANENT_DELETE'/i);
});

test('apply preserves normal finance owners and proves the delegated standard delete', () => {
  assert.match(sql, /public\.timesheet_standard_delete_apply_v1\s*\(/i);
  assert.match(sql, /DAILY_RECEIPT_STANDARD_DELETE_NOT_PROVEN/);
  assert.match(sql, /database_commit_confirmed/);
  assert.doesNotMatch(sql, /update\s+public\.timesheets_financials\b/i);
  assert.doesNotMatch(sql, /delete\s+from\s+public\.timesheets_financials\b/i);
  assert.doesNotMatch(sql, /session_replication_role|disable\s+trigger|enable\s+trigger/i);
  assert.doesNotMatch(sql, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
});

test('delete cancels only unsent manager mail and retains durable operation audit', () => {
  assert.match(sql, /update public\.mail_outbox set status='FAILED'::public\.mail_status_enum/i);
  assert.match(sql, /status='QUEUED'::public\.mail_status_enum and sent_at is null/i);
  assert.match(sql, /CANCELLED_DAILY_ABANDONED_RECEIPT_DELETE/);
  assert.match(sql, /CANDIDATE_DAILY_ABANDONED_RECEIPT_DELETE_APPLIED/);
  assert.match(sql, /UNAPPROVED_FINANCIALLY_CLEAN_DAILY_RECEIPT/);
  assert.match(sql, /delete from public\.candidate_approval_requests where workflow_id=v_workflow_id/i);
  assert.match(sql, /delete from public\.candidate_submission_workflows where id=v_workflow_id/i);
});

test('Office broker previews the Daily adapter first and falls back for every other Timesheet', () => {
  assert.match(broker, /TIMESHEET_DELETE_PREVIEW_DAILY_RECEIPT/);
  assert.match(broker, /TIMESHEET_DELETE_PREVIEW_STANDARD/);
  assert.match(broker, /if \(!preview \|\| preview\.applicable !== true\)/);
  assert.match(broker, /TIMESHEET_DAILY_ABANDONED_RECEIPT_DELETE_APPLY/);
  assert.match(broker, /\['STANDARD_DELETE', 'DAILY_ABANDONED_RECEIPT_DELETE'\]\.includes\(previewKind\)/);
});

