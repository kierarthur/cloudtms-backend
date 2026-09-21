import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import {
  WEEKLY_SOURCE_HANDOVER2_APPROVAL_PATH,
  WEEKLY_SOURCE_HANDOVER2_FILES,
  requireWeeklySourceHandover2Approval,
} from '../../scripts/weekly-source-external-approval.mjs';

const release = { releaseId: '20260920-weekly-source-plan62' };
const inventory = {
  repeatables: WEEKLY_SOURCE_HANDOVER2_FILES.map((file, index) => ({
    path: file,
    sha256: String(index + 1).repeat(64),
  })),
};

function rootWith(value) {
  const root = mkdtempSync(path.join(os.tmpdir(), 'ws-h2-approval-'));
  const file = path.join(root, WEEKLY_SOURCE_HANDOVER2_APPROVAL_PATH);
  mkdirSync(path.dirname(file), { recursive: true });
  if (value) writeFileSync(file, JSON.stringify(value));
  return root;
}

test('integrated Weekly Source release fails closed without HANDOVER 2 approval', () => {
  assert.throws(
    () => requireWeeklySourceHandover2Approval({ repoRoot: rootWith(null), release, inventory }),
    /WEEKLY_SOURCE_HANDOVER2_APPROVAL_MISSING/,
  );
});

test('approval must name the exact HANDOVER 2 files and current hashes', () => {
  const approval = {
    formatVersion: 1,
    releaseId: release.releaseId,
    owner: 'HANDOVER_2',
    status: 'APPROVED_FOR_INTEGRATED_INSTALL',
    files: inventory.repeatables.map((item) => ({ ...item })),
  };
  const result = requireWeeklySourceHandover2Approval({
    repoRoot: rootWith(approval), release, inventory,
  });
  assert.equal(result.path, WEEKLY_SOURCE_HANDOVER2_APPROVAL_PATH);
  approval.files[0].sha256 = 'f'.repeat(64);
  assert.throws(
    () => requireWeeklySourceHandover2Approval({ repoRoot: rootWith(approval), release, inventory }),
    /WEEKLY_SOURCE_HANDOVER2_APPROVAL_HASH_MISMATCH/,
  );
});

test('unrelated releases are unaffected', () => {
  assert.equal(requireWeeklySourceHandover2Approval({
    repoRoot: rootWith(null), release: { releaseId: 'ordinary-release' }, inventory,
  }), null);
});

test('the real release runner checks approval before any release SQL can run', () => {
  const source = readFileSync(new URL('../../scripts/cloudtms-db-release.mjs', import.meta.url), 'utf8');
  const inventory = source.indexOf('const current = inventory();');
  const approval = source.indexOf('requireWeeklySourceHandover2Approval({ repoRoot, release, inventory: current });');
  const firstReleaseSql = source.indexOf("if (mode === 'LEGACY_UPGRADE')", approval);
  assert.ok(inventory >= 0, 'the real runner must build the exact current inventory');
  assert.ok(approval > inventory, 'the approval must bind to that inventory');
  assert.ok(firstReleaseSql > approval, 'the approval must run before every release-mode SQL branch');
});
