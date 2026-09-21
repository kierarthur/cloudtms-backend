import assert from 'node:assert/strict';
import test from 'node:test';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  WEEKLY_SOURCE_PROTECTED_NOT_CAPTURED,
  WEEKLY_SOURCE_PROTECTED_OWNERS,
  preImplementationFiles,
  runWeeklySourceHarnessPhase,
} from './differential-phase-adapter.mjs';
import { WEEKLY_SOURCE_DIFFERENTIAL_ADAPTER_CONTRACT } from '../harness/differential-protection-contract.mjs';

test('the adapter accounts for every one of the 29 protected areas', () => {
  const captured = Object.keys(WEEKLY_SOURCE_PROTECTED_OWNERS);
  const declared = Object.keys(WEEKLY_SOURCE_PROTECTED_NOT_CAPTURED);
  assert.equal(captured.length + declared.length,
    WEEKLY_SOURCE_DIFFERENTIAL_ADAPTER_CONTRACT.protectedAreaCount);
  // No area may be in both lists: an area is either measured or declared
  // unmeasurable, never quietly both.
  for (const id of captured) assert(!declared.includes(id), `${id} is in both lists`);
  for (const id of declared) {
    assert(WEEKLY_SOURCE_PROTECTED_NOT_CAPTURED[id].trim().length > 10,
      `${id} must say WHY it cannot be captured`);
  }
});

test('the two areas Plan 6.2 added are measured, not waved through', () => {
  for (const entry of WEEKLY_SOURCE_DIFFERENTIAL_ADAPTER_CONTRACT.newInPlan62) {
    const area = WEEKLY_SOURCE_PROTECTED_OWNERS[entry.protectionId];
    assert(area, `${entry.protectionId} must have measurable owners`);
    assert(area.routines.length > 0, `${entry.protectionId} must name at least one owner`);
    assert.equal(area.requiredDifferential, entry.requiredDifferential);
  }
});

test('every measured area names real owners and no owner is named twice in one area', () => {
  for (const [protectionId, area] of Object.entries(WEEKLY_SOURCE_PROTECTED_OWNERS)) {
    assert(Array.isArray(area.routines), `${protectionId} routines must be an array`);
    assert(area.routines.length > 0, `${protectionId} must name at least one routine`);
    assert.equal(new Set(area.routines).size, area.routines.length,
      `${protectionId} names the same routine twice`);
    for (const routine of area.routines) {
      assert(/^(public|private)\.[a-z0-9_]+$/.test(routine),
        `${protectionId} names ${routine}, which is not a schema-qualified routine`);
    }
    for (const relation of area.relations ?? []) {
      assert(/^(public|private)\.[a-z0-9_]+$/.test(relation),
        `${protectionId} names ${relation}, which is not a schema-qualified relation`);
    }
  }
});

test('the BEFORE side is built from origin/test blobs, never from a checkout', () => {
  const repoRoot = path.resolve(fileURLToPath(new URL('../../..', import.meta.url)));
  const files = preImplementationFiles(repoRoot);
  // The repository may be clean in some environments; what must always hold is
  // that every entry is a read-only `git show` reference to the base ref.
  for (const entry of files) {
    assert(entry.ref.startsWith('origin/test:'), `${entry.file} is not read from the base ref`);
    assert(entry.file.startsWith('supabase/repeatable/'), `${entry.file} is outside the repeatables`);
  }
});

test('the adapter accepts only its own phase', async () => {
  await assert.rejects(
    () => runWeeklySourceHarnessPhase({ phase: 'db:new', repoRoot: '.', connectionUrl: 'postgresql://postgres@127.0.0.1:55433/postgres' }),
    (error) => error.code === 'WEEKLY_SOURCE_DIFFERENTIAL_PHASE_INVALID',
  );
});

test('the adapter refuses a differential whose controlling pack is absent', async () => {
  await assert.rejects(
    () => runWeeklySourceHarnessPhase({
      phase: 'differential',
      repoRoot: '.',
      connectionUrl: 'postgresql://postgres@127.0.0.1:55433/postgres',
    }),
    (error) => error.code === 'WEEKLY_SOURCE_DIFFERENTIAL_PACK_REQUIRED',
  );
});

test('the adapter refuses a target that is not the local disposable cluster', async () => {
  await assert.rejects(
    () => runWeeklySourceHarnessPhase({
      phase: 'differential',
      repoRoot: '.',
      connectionUrl: 'postgresql://postgres@db.example.test:5432/postgres',
    }),
    (error) => error.code === 'WEEKLY_SOURCE_DIFFERENTIAL_TARGET_REFUSED',
  );
});

test('the adapter refuses to run with no target at all', async () => {
  await assert.rejects(
    () => runWeeklySourceHarnessPhase({
      phase: 'differential',
      repoRoot: '.',
    }),
    (error) => error.code === 'WEEKLY_SOURCE_DIFFERENTIAL_TARGET_REQUIRED',
  );
});
