import { canonicalDigest, cloneJson, deepFreeze } from './canonical-json.mjs';

export const WEEKLY_SOURCE_PHASE_ORDER = Object.freeze([
  'builders',
  'unit',
  'db:new',
  'db:upgrade',
  'service',
  'browser',
  'differential',
  'model',
]);

function fail(code, message, details = {}) {
  const error = new Error(message);
  error.name = 'WeeklySourcePhaseControllerError';
  error.code = code;
  error.details = deepFreeze(cloneJson(details));
  throw error;
}

function normalizeResult(phase, result) {
  if (!result || typeof result !== 'object' || Array.isArray(result)) {
    fail('WEEKLY_SOURCE_PHASE_RESULT_INVALID', `Phase ${phase} returned no evidence object.`);
  }
  if (result.executed !== true || result.pass !== true) {
    fail('WEEKLY_SOURCE_PHASE_NOT_PROVED', `Phase ${phase} did not return executed PASS evidence.`, {
      executed: result.executed ?? null,
      pass: result.pass ?? null,
      code: result.code ?? null,
    });
  }
  if (!Array.isArray(result.evidence) || result.evidence.length === 0) {
    fail('WEEKLY_SOURCE_PHASE_EVIDENCE_MISSING', `Phase ${phase} returned no evidence records.`);
  }
  if (phase.startsWith('db:') && result.cleanup?.complete !== true) {
    fail('WEEKLY_SOURCE_PHASE_CLEANUP_MISSING', `Phase ${phase} has no complete task-owned database cleanup proof.`);
  }
  const body = {
    schemaVersion: 'WEEKLY_SOURCE_HARNESS_PHASE_RESULT_V1',
    phase,
    executed: true,
    pass: true,
    evidence: cloneJson(result.evidence),
    cleanup: cloneJson(result.cleanup ?? null),
    sourceDigest: result.sourceDigest ?? null,
  };
  return deepFreeze({ ...body, evidenceDigest: canonicalDigest(body) });
}

export function createPhaseController({ executors = {} } = {}) {
  if (!executors || typeof executors !== 'object' || Array.isArray(executors)) {
    throw new TypeError('executors must be an object');
  }
  const completed = new Map();
  return Object.freeze({
    async runPhase(phase, context = {}) {
      if (!WEEKLY_SOURCE_PHASE_ORDER.includes(phase)) {
        fail('WEEKLY_SOURCE_PHASE_UNKNOWN', `Unknown Weekly Source phase ${phase}.`);
      }
      const executor = executors[phase];
      if (typeof executor !== 'function') {
        fail('WEEKLY_SOURCE_PHASE_EXECUTOR_MISSING', `Phase ${phase} has no executable owner.`);
      }
      if (completed.has(phase)) {
        fail('WEEKLY_SOURCE_PHASE_DUPLICATE', `Phase ${phase} has already executed in this run.`);
      }
      const result = normalizeResult(phase, await executor(Object.freeze({
        ...context,
        phase,
        priorPhaseResults: deepFreeze([...completed.values()]),
      })));
      completed.set(phase, result);
      return result;
    },
    async runAll(context = {}) {
      const missing = WEEKLY_SOURCE_PHASE_ORDER.filter((phase) => typeof executors[phase] !== 'function');
      if (missing.length) {
        fail('WEEKLY_SOURCE_ALL_PHASES_MISSING', 'The complete harness cannot run because one or more phase owners are missing.', { missing });
      }
      const results = [];
      for (const phase of WEEKLY_SOURCE_PHASE_ORDER) {
        results.push(await this.runPhase(phase, context));
      }
      const body = {
        schemaVersion: 'WEEKLY_SOURCE_HARNESS_ALL_RESULT_V1',
        phaseOrder: [...WEEKLY_SOURCE_PHASE_ORDER],
        phaseEvidenceDigests: results.map((item) => item.evidenceDigest),
        pass: true,
      };
      return deepFreeze({ ...body, evidenceDigest: canonicalDigest(body), results });
    },
    completed() {
      return deepFreeze([...completed.values()]);
    },
  });
}
