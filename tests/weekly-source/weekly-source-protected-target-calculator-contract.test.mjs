import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const repositoryRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..');
const source = fs.readFileSync(path.join(repositoryRoot, 'broker/src/index.js'), 'utf8');

test('the established Weekly calculator exposes only a non-writing protected target preview seam', () => {
  assert.match(source, /ignore_locked_segments_for_preview\s*=\s*false/);
  assert.match(
    source,
    /ignore_locked_segments_for_preview\s*===\s*true\s*&&\s*write_now\s*===\s*true[\s\S]*PROTECTED_TARGET_PREVIEW_MUST_NOT_WRITE_TSFINS/,
  );
  assert.equal(
    (source.match(/!isCorrection\s*&&\s*ignore_locked_segments_for_preview\s*!==\s*true/g) ?? []).length,
    2,
  );
});

test('the preview seam does not alter Banking Pay, Draft, invoice or provider routes', () => {
  const seam = source.match(
    /async function buildWeeklyScheduleSegmentsSnapshot[\s\S]*?async function rebuildFromExistingSegmentsEvidence/,
  )?.[0] ?? '';
  assert.ok(seam);
  assert.equal(/pay_batch_prepare|create-draft|provider_submit|invoice_generate|bank_transfer/i.test(seam), false);
});
