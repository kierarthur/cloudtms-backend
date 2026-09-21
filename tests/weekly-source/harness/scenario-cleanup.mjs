import { access, lstat, mkdir, mkdtemp, readFile, realpath, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { canonicalJsonPretty, cloneJson, deepFreeze } from './canonical-json.mjs';

const MARKER_NAME = '.cloudtms-weekly-source-harness.json';
const DIRECTORY_PREFIX = 'cloudtms-weekly-source-';

async function canonicalRoot(root) {
  const resolved = path.resolve(root);
  await mkdir(resolved, { recursive: true });
  return realpath(resolved);
}

function assertDirectChild(target, root) {
  const relative = path.relative(root, target);
  if (!relative || relative.startsWith('..') || path.isAbsolute(relative) || relative.includes(path.sep)) {
    throw new Error('Cleanup target is not an exact task-owned child of the approved temporary root');
  }
  if (!path.basename(target).startsWith(DIRECTORY_PREFIX)) {
    throw new Error('Cleanup target does not use the Weekly Source task prefix');
  }
}

export async function createScenarioWorkspace(scenarioId, { root = os.tmpdir() } = {}) {
  if (!/^WS-[A-Z0-9][A-Z0-9_-]{2,79}$/.test(scenarioId)) throw new TypeError('A valid Weekly Source scenario ID is required');
  const approvedRoot = await canonicalRoot(root);
  const safeId = scenarioId.toLowerCase().replace(/[^a-z0-9]+/g, '-').slice(0, 48);
  const workspace = await mkdtemp(path.join(approvedRoot, `${DIRECTORY_PREFIX}${safeId}-`));
  assertDirectChild(workspace, approvedRoot);
  const marker = { schemaVersion: 'WEEKLY_SOURCE_TEMP_WORKSPACE_V1', scenarioId };
  await writeFile(path.join(workspace, MARKER_NAME), canonicalJsonPretty(marker), { encoding: 'utf8', flag: 'wx' });
  return deepFreeze({ scenarioId, path: workspace, approvedRoot });
}

export async function cleanupScenarioWorkspace(workspace) {
  if (!workspace?.scenarioId || !workspace?.path || !workspace?.approvedRoot) throw new TypeError('A created scenario workspace descriptor is required');
  const approvedRoot = await realpath(workspace.approvedRoot);
  const target = path.resolve(workspace.path);
  assertDirectChild(target, approvedRoot);
  const stat = await lstat(target);
  if (!stat.isDirectory() || stat.isSymbolicLink()) throw new Error('Cleanup target must be a regular directory');
  const markerPath = path.join(target, MARKER_NAME);
  const markerStat = await lstat(markerPath);
  if (!markerStat.isFile() || markerStat.isSymbolicLink()) throw new Error('Cleanup marker must be a regular file');
  const marker = JSON.parse(await readFile(markerPath, 'utf8'));
  if (marker.schemaVersion !== 'WEEKLY_SOURCE_TEMP_WORKSPACE_V1' || marker.scenarioId !== workspace.scenarioId) {
    throw new Error('Cleanup marker does not match the requested scenario');
  }
  await rm(target, { recursive: true, force: false });
  const absent = await access(target).then(() => false, () => true);
  if (!absent) throw new Error('Scenario workspace still exists after cleanup');
  return deepFreeze({ scenarioId: workspace.scenarioId, removed: true, freshProbe: 'ABSENT' });
}

export class ScenarioCleanupRegistry {
  #scenarioId;
  #steps = [];

  constructor(scenarioId) {
    if (!/^WS-[A-Z0-9][A-Z0-9_-]{2,79}$/.test(scenarioId)) throw new TypeError('A valid Weekly Source scenario ID is required');
    this.#scenarioId = scenarioId;
  }

  register(label, cleanup, freshProbe) {
    if (!/^[A-Z][A-Z0-9_-]{1,79}$/.test(label)) throw new TypeError('Cleanup label must be a stable upper-case identifier');
    if (typeof cleanup !== 'function' || typeof freshProbe !== 'function') throw new TypeError('Cleanup and freshProbe callbacks are required');
    if (this.#steps.some((step) => step.label === label)) throw new Error(`Duplicate cleanup label ${label}`);
    this.#steps.push({ label, cleanup, freshProbe });
  }

  async run() {
    const results = [];
    const failures = [];
    for (const step of [...this.#steps].reverse()) {
      let cleanupError = null;
      try {
        await step.cleanup();
      } catch (error) {
        cleanupError = error;
      }
      let proof = null;
      try {
        proof = await step.freshProbe();
      } catch (error) {
        failures.push({ label: step.label, phase: 'FRESH_PROBE', message: error.message });
      }
      if (cleanupError) failures.push({ label: step.label, phase: 'CLEANUP', message: cleanupError.message });
      if (!proof || proof.cleaned !== true) {
        failures.push({ label: step.label, phase: 'RESIDUE', message: 'Fresh probe did not certify cleanup' });
      } else {
        results.push({ label: step.label, cleaned: true, proof: cloneJson(proof) });
      }
    }
    if (failures.length) {
      const error = new Error(`Weekly Source cleanup failed for ${failures.map((item) => item.label).join(', ')}`);
      error.name = 'ScenarioCleanupError';
      error.code = 'WEEKLY_SOURCE_CLEANUP_INCOMPLETE';
      error.failures = deepFreeze(failures);
      error.completedResults = deepFreeze(results);
      throw error;
    }
    return deepFreeze({ scenarioId: this.#scenarioId, complete: true, results });
  }
}
