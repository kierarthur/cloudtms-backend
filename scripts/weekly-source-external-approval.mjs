import fs from 'node:fs';
import path from 'node:path';

export const WEEKLY_SOURCE_HANDOVER2_APPROVAL_PATH =
  'supabase/release/weekly-source-handover2-approval.json';

export const WEEKLY_SOURCE_HANDOVER2_FILES = Object.freeze([
  'supabase/repeatable/04082026_1146_pay_workbench_timesheet_input_fingerprint_v1.sql',
  'supabase/repeatable/04082026_2314_pay_workbench_unit_economic_occurrence_page_v1.sql',
]);

export function requireWeeklySourceHandover2Approval({ repoRoot, release, inventory }) {
  if (!String(release?.releaseId ?? '').includes('weekly-source-plan62')) return null;
  const approvalPath = path.join(repoRoot, WEEKLY_SOURCE_HANDOVER2_APPROVAL_PATH);
  if (!fs.existsSync(approvalPath)) {
    throw new Error('WEEKLY_SOURCE_HANDOVER2_APPROVAL_MISSING');
  }
  const approval = JSON.parse(fs.readFileSync(approvalPath, 'utf8'));
  if (
    approval.formatVersion !== 1
    || approval.releaseId !== release.releaseId
    || approval.owner !== 'HANDOVER_2'
    || approval.status !== 'APPROVED_FOR_INTEGRATED_INSTALL'
  ) throw new Error('WEEKLY_SOURCE_HANDOVER2_APPROVAL_INVALID');

  const expected = new Map(WEEKLY_SOURCE_HANDOVER2_FILES.map((file) => [file, null]));
  for (const item of inventory.repeatables ?? []) {
    if (expected.has(item.path)) expected.set(item.path, item.sha256);
  }
  const supplied = new Map((approval.files ?? []).map((item) => [item.path, item.sha256]));
  if (
    supplied.size !== expected.size
    || [...expected].some(([file, sha256]) => !sha256 || supplied.get(file) !== sha256)
  ) throw new Error('WEEKLY_SOURCE_HANDOVER2_APPROVAL_HASH_MISMATCH');
  return Object.freeze({ path: WEEKLY_SOURCE_HANDOVER2_APPROVAL_PATH, approval });
}
