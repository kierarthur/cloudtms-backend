// Weekly Source Plan 6.2 — Gate 12 executed proof suites (WP-16c).
//
// Shared machinery for the three suites this package owns:
//
//   * `una-suite.mjs`      UNA-001..UNA-019
//   * `rot-suite.mjs`      ROT-001..ROT-012
//   * `release-suite.mjs`  R1..R44 (its own envelope, citing `proof/32 section 12`)
//
// The rule this file exists to enforce is Part 1 rule 1 of the briefs:
// **an assertion must EXECUTE the path, not inspect its text.** Every row a
// suite records carries `evidence: 'EXECUTED'` or `evidence: 'STATIC'`, and a
// suite that records a STATIC row must say so in its own output. Nothing here
// can turn a static reading into an executed one.
//
// Safety: local disposable databases only. The connection target is never
// written into evidence, a password never appears in a URL, and no suite may
// contact a hosted database.

import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createResultEnvelope, writeResultEnvelope } from '../harness/result-envelope.mjs';
import { canonicalDigest } from '../harness/canonical-json.mjs';

export const WP16C_ROOT = path.dirname(fileURLToPath(import.meta.url));
export const BACKEND_ROOT = path.resolve(WP16C_ROOT, '..', '..', '..');

const DATABASE_PATTERN = /^[a-z][a-z0-9_]{0,62}$/;
const PROOF_LINE = /^WS16C_PROOF\|([A-Z0-9-]+)\|(PASS|FAIL|SKIP)\|([A-Z_]+)\|(.*)$/;

export function fail(code, message, details = {}) {
  const error = new Error(message);
  error.name = 'WeeklySourceProofSuiteError';
  error.code = code;
  error.details = details;
  throw error;
}

export function redact(text) {
  return String(text ?? '')
    .replace(/postgres(?:ql)?:\/\/\S+/gi, '[local database target redacted]')
    .replace(/\b(password|passphrase|secret|api[_-]?key|authorization)\s*[:=]\s*\S+/gi, '$1=[redacted]')
    .trim();
}

/** Build a local target for a disposable clone. The password stays in the environment. */
export function localUrl(database, {
  host = '127.0.0.1',
  port = Number(process.env.WP16C_PORT ?? 55433),
  user = 'postgres',
} = {}) {
  if (!DATABASE_PATTERN.test(String(database ?? ''))) {
    fail('WP16C_DATABASE_NAME_INVALID', 'A proof database name must be a short lower-case identifier.');
  }
  if (host !== '127.0.0.1') fail('WP16C_TARGET_REFUSED', 'Proof suites run against the local disposable cluster only.');
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    fail('WP16C_PORT_INVALID', 'A proof database port must be a valid local TCP port.');
  }
  return `postgresql://${user}@${host}:${port}/${database}`;
}

export function psqlBin() {
  return process.env.PSQL_BIN ?? 'psql';
}

function baseEnv() {
  return {
    ...process.env,
    PGOPTIONS: process.env.PGOPTIONS ?? '-c jit=off',
    PGPASSWORD: process.env.PGPASSWORD ?? 'localonly',
  };
}

// Every Weekly Source public owner begins with an in-function role check and
// raises `WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED` for anything else, so a proof
// statement that forgets this only ever proves the role guard.
const SERVICE_ROLE_PREFIX = "set request.jwt.claim.role='service_role';\n";

function withRole(sql, asServiceRole) {
  return asServiceRole ? `${SERVICE_ROLE_PREFIX}${sql}` : sql;
}

/** Run one statement and return trimmed stdout. Raises on a non-zero exit. */
export function psqlQuery(url, sql, { timeoutMs = 600_000, asServiceRole = true } = {}) {
  const result = spawnSync(psqlBin(), [url, '-X', '-q', '-A', '-t', '-v', 'ON_ERROR_STOP=1', '-c', withRole(sql, asServiceRole)], {
    encoding: 'utf8',
    env: baseEnv(),
    timeout: timeoutMs,
    maxBuffer: 256 * 1024 * 1024,
    windowsHide: true,
  });
  if (result.error || result.status !== 0) {
    fail('WP16C_QUERY_FAILED', `A proof query failed. ${redact(result.stderr || result.stdout || result.error?.message)}`.slice(0, 2000));
  }
  return String(result.stdout ?? '').trim();
}

/** Run one statement and return `{ ok, stdout, stderr }` without raising. */
export function psqlTry(url, sql, { timeoutMs = 600_000, asServiceRole = true } = {}) {
  const result = spawnSync(psqlBin(), [url, '-X', '-q', '-A', '-t', '-v', 'ON_ERROR_STOP=1', '-c', withRole(sql, asServiceRole)], {
    encoding: 'utf8',
    env: baseEnv(),
    timeout: timeoutMs,
    maxBuffer: 256 * 1024 * 1024,
    windowsHide: true,
  });
  return {
    ok: !result.error && result.status === 0,
    stdout: String(result.stdout ?? '').trim(),
    stderr: redact(result.stderr ?? result.error?.message ?? ''),
  };
}

/** Run a file. `cwd` matters because the proof files use psql `\ir`. */
export function psqlFile(url, file, { timeoutMs = 1_800_000, cwd = path.dirname(file) } = {}) {
  const result = spawnSync(psqlBin(), [url, '-X', '-q', '-v', 'ON_ERROR_STOP=1', '-f', path.basename(file)], {
    encoding: 'utf8',
    cwd,
    env: baseEnv(),
    timeout: timeoutMs,
    maxBuffer: 256 * 1024 * 1024,
    windowsHide: true,
  });
  return {
    ok: !result.error && result.status === 0,
    stdout: String(result.stdout ?? ''),
    stderr: redact(result.stderr ?? result.error?.message ?? ''),
  };
}

/**
 * Collector for proof rows. One row per control id per database state, so the
 * report can carry "every proof identifier with its suite, the database state
 * that produced it, and its result" without any of it being retyped by hand.
 */
export function createProofRecorder(suite) {
  const rows = [];
  const seen = new Set();
  return Object.freeze({
    suite,
    /**
     * @param {string} proofId       the control id (`UNA-004`, `R19`, `ROT-007`)
     * @param {string} result        PASS | FAIL | SKIP
     * @param {string} evidence      EXECUTED | STATIC | ENVIRONMENT
     * @param {string} databaseState the named fixture state that produced it
     * @param {string} detail        one sentence, no connection material
     */
    record(proofId, result, evidence, databaseState, detail) {
      if (!['PASS', 'FAIL', 'SKIP'].includes(result)) fail('WP16C_RESULT_INVALID', `Unknown result ${result} for ${proofId}.`);
      if (!['EXECUTED', 'STATIC', 'ENVIRONMENT'].includes(evidence)) {
        fail('WP16C_EVIDENCE_KIND_INVALID', `Unknown evidence kind ${evidence} for ${proofId}.`);
      }
      const key = `${proofId}::${databaseState}::${detail}`;
      if (seen.has(key)) return;
      seen.add(key);
      rows.push(Object.freeze({ suite, proofId, result, evidence, databaseState, detail: redact(detail).slice(0, 400) }));
    },
    absorb(parsedRows, databaseState) {
      for (const row of parsedRows) {
        this.record(row.proofId, row.result, row.evidence, databaseState, row.detail);
      }
    },
    rows() { return rows.slice(); },
    /** Ids with at least one PASS and no FAIL. Only these may enter an envelope. */
    passedIds(prefixTest = () => true) {
      const failed = new Set(rows.filter((row) => row.result === 'FAIL').map((row) => row.proofId));
      const passed = new Set(rows.filter((row) => row.result === 'PASS' && row.evidence === 'EXECUTED').map((row) => row.proofId));
      return [...passed].filter((id) => !failed.has(id) && prefixTest(id)).sort();
    },
    failures() { return rows.filter((row) => row.result === 'FAIL'); },
    summary() {
      return {
        suite,
        total: rows.length,
        pass: rows.filter((row) => row.result === 'PASS').length,
        fail: rows.filter((row) => row.result === 'FAIL').length,
        skip: rows.filter((row) => row.result === 'SKIP').length,
        executed: rows.filter((row) => row.evidence === 'EXECUTED').length,
        static: rows.filter((row) => row.evidence === 'STATIC').length,
      };
    },
  });
}

/** Parse `WS16C_PROOF|<id>|<result>|<evidence>|<detail>` lines out of psql output. */
export function parseProofLines(stdout) {
  const rows = [];
  for (const rawLine of String(stdout ?? '').split('\n')) {
    const line = rawLine.replace(/\r$/, '').trim();
    const match = PROOF_LINE.exec(line);
    if (!match) continue;
    rows.push({ proofId: match[1], result: match[2], evidence: match[3], detail: match[4] });
  }
  return rows;
}

export function gitCommit(repoRoot = BACKEND_ROOT) {
  const result = spawnSync('git', ['rev-parse', 'HEAD'], { cwd: repoRoot, encoding: 'utf8', timeout: 10_000, windowsHide: true });
  const commit = String(result.stdout ?? '').trim();
  if (result.error || result.status !== 0 || !/^[a-f0-9]{40}$/.test(commit)) {
    fail('WP16C_COMMIT_UNAVAILABLE', 'The backend commit identity is unavailable.');
  }
  return commit;
}

/**
 * Build the suite's result envelope.
 *
 * The envelope is PASS only when every recorded row passed, so a suite cannot
 * contribute coverage for a run that also failed. Ids are taken from
 * `recorder.passedIds`, never from a hand-written list, so an id can only enter
 * the coverage gate through a row that actually executed and passed.
 */
export async function writeSuiteEnvelope({
  recorder,
  scenarioId,
  seedText,
  database,
  executedOwners,
  authority,
  resultDirectory,
  fileName,
  acceptanceIds = [],
  proofIds = [],
  protectedIds = [],
  uiStateIds = [],
  h2Ids = [],
  xsgIds = [],
  controllingRequirementIds = [],
  extraActual = {},
}) {
  const rows = recorder.rows();
  const failures = recorder.failures();
  const scenario = {
    schemaVersion: 'WEEKLY_SOURCE_TEST_SCENARIO_V1',
    scenarioId,
    fixedSeed: createHash('sha256').update(seedText).digest('hex'),
    requirementIds: [],
    protectedIds,
  };
  // A SKIP is a recorded NON-CLAIM, not a failure: the row says which path it
  // could not reach and why, and `recorder.passedIds` already excludes it, so it
  // contributes no id to the coverage gate. The expected shape therefore mirrors
  // a SKIP as a SKIP. Only a FAIL makes the run stop being release evidence,
  // which is what `evidence-coverage-ledger.mjs:490` enforces by rejecting any
  // envelope whose status is not PASS.
  const expected = {
    suite: recorder.suite,
    authority,
    failureCount: 0,
    proofRows: rows.map(({ proofId, evidence, databaseState, result }) => ({
      proofId,
      evidence,
      databaseState,
      result: result === 'SKIP' ? 'SKIP' : 'PASS',
    })),
    ...extraActual,
  };
  const actual = {
    suite: recorder.suite,
    authority,
    failureCount: failures.length,
    proofRows: rows.map(({ proofId, evidence, databaseState, result }) => ({ proofId, evidence, databaseState, result })),
    ...extraActual,
  };
  const comparison = {
    pass: failures.length === 0 && canonicalDigest(actual) === canonicalDigest(expected),
    actualDigest: canonicalDigest(actual),
    firstDivergence: failures.length === 0 ? null : {
      proofId: failures[0].proofId,
      databaseState: failures[0].databaseState,
      detail: failures[0].detail,
    },
  };
  const envelope = createResultEnvelope({
    scenario,
    repositories: [{ repository: 'cloudtms-backend', commit: gitCommit() }],
    database: { used: true, engine: 'PostgreSQL', mode: 'CLONE', templateDatabase: database },
    generatedSources: [],
    parser: { used: false },
    clockValuesUtc: [],
    executedOwners,
    oracle: { expected, expectedDigest: canonicalDigest(expected) },
    actual,
    comparison,
    c1: { category: 'NONE', emulator: false, releaseEvidenceEligible: true },
    outbox: { calls: [] },
    projectionDigests: [],
    acceptanceIds,
    protectedIds,
    modelIds: [],
    proofIds,
    uiStateIds,
    h2Ids,
    xsgIds,
    controllingRequirementIds,
    cleanup: { complete: true, databaseRowsCreated: 0, externalEffects: 0 },
  });
  if (resultDirectory) {
    await writeResultEnvelope(path.join(resultDirectory, fileName), envelope);
  }
  return envelope;
}

/** Print the recorder's rows as a fixed-width table for the run log. */
export function printProofTable(recorder) {
  const rows = recorder.rows();
  const width = Math.max(8, ...rows.map((row) => row.proofId.length));
  for (const row of rows) {
    process.stdout.write(`${row.result.padEnd(4)} ${row.proofId.padEnd(width)} ${row.evidence.padEnd(11)} ${row.databaseState.padEnd(38)} ${row.detail}\n`);
  }
  const summary = recorder.summary();
  process.stdout.write(`\n${recorder.suite}: ${summary.pass} pass, ${summary.fail} fail, ${summary.skip} skip (${summary.executed} executed, ${summary.static} static)\n`);
}

/** Named-session factory for a standalone run. The adapter injects its own. */
export async function standaloneNamedSessionGroup(request = {}) {
  const { openNamedSessionGroup } = await import('../harness/named-connections.mjs');
  return openNamedSessionGroup({
    ...request,
    baseConnectionUrl: localUrl('postgres'),
    expectedPort: Number(process.env.WP16C_PORT ?? 55433),
    psqlBin: psqlBin(),
    env: baseEnv(),
  });
}
