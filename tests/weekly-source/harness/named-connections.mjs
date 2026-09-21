// Named concurrent PostgreSQL sessions for the Weekly Source harness.
//
// `P:\23A_EXECUTABLE_TEST_HARNESS_SPECIFICATION.md` section 13: "Tests involving advisory
// locks/concurrency run in a dedicated serial group with two or more named connections."
// The database adapter runs one `psql -f` per verification file per clone, which cannot
// express a race. `R37`, `ROT-002`, `ROT-003`, `ROT-012` and `UNA-014` all need two sessions
// holding transactions at the same time, so the harness owns that capability here.
//
// Safety contract:
//   * every session is derived from a connection target the PostgreSQL controller already
//     proved, so host, port and user cannot drift;
//   * a password is never placed in a URL. psql reads `PGPASSWORD` from the environment;
//   * psql meta-commands are refused in caller SQL, so `\!`, `\copy` and `\i` cannot escape;
//   * evidence carries session names, the database and counters only, never a target URL.

import { spawn } from 'node:child_process';
import { deepFreeze } from './canonical-json.mjs';

const SENTINEL_PREFIX = 'WS_HARNESS_SESSION_SENTINEL_';
const DATABASE_PATTERN = /^[a-z][a-z0-9_]{0,62}$/;
const SESSION_NAME_PATTERN = /^[a-z][a-z0-9_-]{1,31}$/;
const MAX_SESSIONS = 8;
const DEFAULT_STATEMENT_WAIT_MS = 120_000;

function fail(code, message, details = {}) {
  const error = new Error(message);
  error.name = 'WeeklySourceNamedConnectionError';
  error.code = code;
  error.details = deepFreeze(details);
  throw error;
}

function redact(text) {
  return String(text ?? '')
    .replace(/postgres(?:ql)?:\/\/\S+/gi, '[local database target redacted]')
    .replace(/\b(password|passphrase|secret|api[_-]?key|authorization)\s*[:=]\s*\S+/gi, '$1=[redacted]')
    .trim();
}

/**
 * Accept only a local, task-owned disposable database reached with no password in the URL.
 * `expectedPort` is the port the PostgreSQL controller already discovered and asserted.
 */
export function assertNamedSessionTarget(connectionUrl, { expectedPort, database } = {}) {
  let url;
  try {
    url = new URL(connectionUrl);
  } catch {
    fail('NAMED_SESSION_TARGET_INVALID', 'The named-session connection target is invalid.');
  }
  const actualDatabase = decodeURIComponent(url.pathname.replace(/^\//, ''));
  if (
    url.protocol !== 'postgresql:'
    || url.hostname !== '127.0.0.1'
    || !url.port
    || (expectedPort !== undefined && Number(url.port) !== Number(expectedPort))
    || decodeURIComponent(url.username) !== 'postgres'
    || url.password
    || url.search
    || !DATABASE_PATTERN.test(actualDatabase)
    || (database !== undefined && actualDatabase !== database)
  ) {
    fail('NAMED_SESSION_TARGET_REFUSED', 'Named sessions accept only the exact local task-owned database with no password in the target.');
  }
  return true;
}

/** Point an already-proved local target at one of its clones without rebuilding the URL. */
export function deriveNamedSessionUrl(baseConnectionUrl, database, { expectedPort } = {}) {
  if (!DATABASE_PATTERN.test(String(database ?? ''))) {
    fail('NAMED_SESSION_DATABASE_INVALID', 'A named-session database name must be a short lower-case identifier.');
  }
  let url;
  try {
    url = new URL(baseConnectionUrl);
  } catch {
    fail('NAMED_SESSION_TARGET_INVALID', 'The named-session base connection target is invalid.');
  }
  url.pathname = `/${encodeURIComponent(database)}`;
  const derived = url.toString();
  assertNamedSessionTarget(derived, { expectedPort, database });
  return derived;
}

export function assertNoMetaCommand(sql) {
  const text = String(sql ?? '');
  if (!text.trim()) fail('NAMED_SESSION_SQL_EMPTY', 'A named-session statement cannot be empty.');
  for (const line of text.split('\n')) {
    if (/^\s*\\/.test(line)) {
      fail('NAMED_SESSION_META_COMMAND_REFUSED', 'Named sessions refuse psql meta-commands; pass SQL only.');
    }
  }
  return true;
}

function createSession({ name, connectionUrl, psqlBin, env, database, applicationName }) {
  const child = spawn(psqlBin, [
    connectionUrl,
    '-X',
    '-q',
    '-A',
    '-t',
    '-v', 'ON_ERROR_STOP=0',
    '--no-psqlrc',
  ], {
    env: { ...env, PGAPPNAME: applicationName },
    windowsHide: true,
    stdio: ['pipe', 'pipe', 'pipe'],
  });

  let sequence = 0;
  let stdoutBuffer = '';
  let stderrBuffer = '';
  let closed = false;
  let exitInfo = null;
  const queue = [];
  let active = null;

  function settleActive() {
    if (!active || !active.stdoutSeen || !active.stderrSeen) return;
    const current = active;
    active = null;
    current.resolve(deepFreeze({
      session: name,
      rows: Object.freeze(current.rows.slice()),
      error: current.error ? deepFreeze(current.error) : null,
    }));
    pump();
  }

  function consumeStdout() {
    let index = stdoutBuffer.indexOf('\n');
    while (index >= 0) {
      const line = stdoutBuffer.slice(0, index).replace(/\r$/, '');
      stdoutBuffer = stdoutBuffer.slice(index + 1);
      if (active && line === active.sentinel) {
        active.stdoutSeen = true;
        settleActive();
      } else if (active) {
        active.rows.push(line);
      }
      index = stdoutBuffer.indexOf('\n');
    }
  }

  function consumeStderr() {
    let index = stderrBuffer.indexOf('\n');
    while (index >= 0) {
      const line = stderrBuffer.slice(0, index).replace(/\r$/, '');
      stderrBuffer = stderrBuffer.slice(index + 1);
      if (active && line === active.sentinel) {
        active.stderrSeen = true;
        settleActive();
      } else if (active && line.trim()) {
        const codeMatch = /^(ERROR|FATAL|PANIC|WARNING|NOTICE):\s*(.*)$/.exec(line);
        if (codeMatch && (codeMatch[1] === 'ERROR' || codeMatch[1] === 'FATAL' || codeMatch[1] === 'PANIC')) {
          active.error = active.error ?? { severity: codeMatch[1], message: redact(codeMatch[2]), sqlstate: null };
        } else if (active.error && /^DETAIL:|^HINT:|^CONTEXT:/.test(line)) {
          active.error.detail = redact([active.error.detail, line].filter(Boolean).join(' '));
        } else if (!codeMatch && active.error && !active.error.sqlstate) {
          const state = /\b([0-9A-Z]{5})\b/.exec(line);
          if (state) active.error.sqlstate = state[1];
        }
      }
      index = stderrBuffer.indexOf('\n');
    }
  }

  child.stdout.setEncoding('utf8');
  child.stderr.setEncoding('utf8');
  child.stdout.on('data', (chunk) => { stdoutBuffer += chunk; consumeStdout(); });
  child.stderr.on('data', (chunk) => { stderrBuffer += chunk; consumeStderr(); });
  child.on('exit', (code, signal) => {
    closed = true;
    exitInfo = { code, signal };
    const pending = active ? [active, ...queue] : [...queue];
    active = null;
    queue.length = 0;
    for (const item of pending) {
      item.reject(Object.assign(new Error(`Named session ${name} closed before its statement completed.`), {
        code: 'NAMED_SESSION_CLOSED',
      }));
    }
  });

  function pump() {
    if (active || queue.length === 0 || closed) return;
    active = queue.shift();
    child.stdin.write(`${active.sql}\n\\echo ${active.sentinel}\n\\warn ${active.sentinel}\n`);
  }

  function enqueue(sql) {
    assertNoMetaCommand(sql);
    if (closed) fail('NAMED_SESSION_CLOSED', `Named session ${name} is closed.`);
    sequence += 1;
    const sentinel = `${SENTINEL_PREFIX}${name.toUpperCase().replace(/-/g, '_')}_${sequence}`;
    const record = {
      sql: String(sql).trim(),
      sentinel,
      rows: [],
      error: null,
      stdoutSeen: false,
      stderrSeen: false,
      resolve: null,
      reject: null,
      startedAtMs: Date.now(),
    };
    const promise = new Promise((resolve, reject) => {
      record.resolve = resolve;
      record.reject = reject;
    });
    record.promise = promise;
    queue.push(record);
    pump();
    return record;
  }

  return Object.freeze({
    name,
    database,
    applicationName,
    /** Queue one SQL statement. Not awaiting it is how a session is left blocked on a lock. */
    run(sql) {
      return enqueue(sql).promise;
    },
    /** Queue a statement and refuse a wait longer than `timeoutMs`. */
    async runWithin(sql, timeoutMs = DEFAULT_STATEMENT_WAIT_MS) {
      const record = enqueue(sql);
      let timer = null;
      try {
        return await Promise.race([
          record.promise,
          new Promise((resolve, reject) => {
            timer = setTimeout(() => reject(Object.assign(
              new Error(`Named session ${name} did not complete its statement within ${timeoutMs} ms.`),
              { code: 'NAMED_SESSION_STATEMENT_TIMEOUT' },
            )), timeoutMs);
          }),
        ]);
      } finally {
        if (timer) clearTimeout(timer);
      }
    },
    begin(isolation) {
      return this.run(isolation ? `begin isolation level ${isolation};` : 'begin;');
    },
    commit() { return this.run('commit;'); },
    rollback() { return this.run('rollback;'); },
    pendingStatementCount() { return (active ? 1 : 0) + queue.length; },
    isBusy() { return Boolean(active); },
    isClosed() { return closed; },
    exitInfo() { return exitInfo ? deepFreeze({ ...exitInfo }) : null; },
    async close(timeoutMs = 10_000) {
      if (closed) return;
      child.stdin.write('\\q\n');
      child.stdin.end();
      await new Promise((resolve) => {
        const timer = setTimeout(() => { child.kill('SIGKILL'); resolve(); }, timeoutMs);
        child.on('exit', () => { clearTimeout(timer); resolve(); });
      });
    },
  });
}

/**
 * Open a serial group of named sessions against one local database.
 *
 * @param {object} request
 * @param {string} request.baseConnectionUrl a target the PostgreSQL controller already proved
 * @param {number} request.expectedPort      the port that target was proved on
 * @param {string} request.database          the exact database or clone to open
 * @param {string[]} request.names           two or more short session names
 * @param {string} [request.psqlBin]
 * @param {string} [request.groupId]         appears in `pg_stat_activity.application_name`
 */
export async function openNamedSessionGroup({
  baseConnectionUrl,
  expectedPort,
  database,
  names,
  psqlBin = process.env.PSQL_BIN ?? 'psql',
  groupId = 'ws-serial',
  env = process.env,
  statementTimeoutMs = null,
  lockTimeoutMs = null,
}) {
  if (!Array.isArray(names) || names.length < 2) {
    fail('NAMED_SESSION_GROUP_TOO_SMALL', 'A serial concurrency group needs two or more named connections.');
  }
  if (names.length > MAX_SESSIONS) {
    fail('NAMED_SESSION_GROUP_TOO_LARGE', `A serial concurrency group is bounded to ${MAX_SESSIONS} named connections.`);
  }
  if (new Set(names).size !== names.length || names.some((name) => !SESSION_NAME_PATTERN.test(name))) {
    fail('NAMED_SESSION_NAME_INVALID', 'Session names must be unique, short and lower-case.');
  }
  if (!SESSION_NAME_PATTERN.test(groupId)) fail('NAMED_SESSION_GROUP_ID_INVALID', 'A serial group id must be short and lower-case.');
  const connectionUrl = deriveNamedSessionUrl(baseConnectionUrl, database, { expectedPort });

  const sessions = new Map();
  try {
    for (const name of names) {
      const session = createSession({
        name,
        connectionUrl,
        psqlBin,
        env,
        database,
        applicationName: `${groupId}:${name}`,
      });
      sessions.set(name, session);
      const opened = await session.runWithin('select current_database();', 30_000);
      if (opened.error) {
        fail('NAMED_SESSION_OPEN_FAILED', `Named session ${name} could not open the local database.`, {
          sqlstate: opened.error.sqlstate ?? null,
        });
      }
      if (opened.rows.filter(Boolean).at(-1) !== database) {
        fail('NAMED_SESSION_DATABASE_REFUSED', `Named session ${name} opened the wrong database.`);
      }
      if (statementTimeoutMs !== null) await session.runWithin(`set statement_timeout = ${Number(statementTimeoutMs)};`, 30_000);
      if (lockTimeoutMs !== null) await session.runWithin(`set lock_timeout = ${Number(lockTimeoutMs)};`, 30_000);
      // JIT is off for every Weekly Source proof database (environment report section 6.1).
      await session.runWithin('set jit = off;', 30_000);
    }
  } catch (error) {
    for (const session of sessions.values()) await session.close().catch(() => {});
    throw error;
  }

  return Object.freeze({
    database,
    groupId,
    names: Object.freeze([...names]),
    session(name) {
      const found = sessions.get(name);
      if (!found) fail('NAMED_SESSION_UNKNOWN', `Named session ${name} is not part of this serial group.`);
      return found;
    },
    /**
     * Prove from a third session that `name` is genuinely waiting on a lock rather than
     * merely slow. Returns the observed wait event.
     */
    async waitUntilBlocked(observerName, blockedName, { timeoutMs = 30_000, pollMs = 100 } = {}) {
      const observer = this.session(observerName);
      const blocked = this.session(blockedName);
      const deadline = Date.now() + timeoutMs;
      while (Date.now() < deadline) {
        const probe = await observer.runWithin(
          `select coalesce(max(wait_event_type), '') from pg_stat_activity where application_name = '${blocked.applicationName}' and pid <> pg_backend_pid() and state <> 'idle';`,
          10_000,
        );
        const waitEvent = probe.rows.filter(Boolean).at(-1) ?? '';
        if (waitEvent === 'Lock') return deepFreeze({ blocked: blockedName, waitEventType: waitEvent });
        if (!blocked.isBusy()) {
          fail('NAMED_SESSION_NOT_BLOCKED', `Named session ${blockedName} completed instead of blocking on a lock.`);
        }
        await new Promise((resolve) => setTimeout(resolve, pollMs));
      }
      fail('NAMED_SESSION_BLOCK_TIMEOUT', `Named session ${blockedName} did not enter a lock wait within ${timeoutMs} ms.`);
    },
    evidence() {
      return deepFreeze({
        contract: 'WEEKLY_SOURCE_NAMED_CONNECTION_GROUP_V1',
        groupId,
        database,
        sessionCount: sessions.size,
        sessionNames: [...sessions.keys()],
        applicationNames: [...sessions.values()].map((session) => session.applicationName),
        openSessions: [...sessions.values()].filter((session) => !session.isClosed()).length,
      });
    },
    async closeAll() {
      const closed = [];
      for (const [name, session] of sessions) {
        await session.close().catch(() => {});
        closed.push(name);
      }
      return deepFreeze({ complete: true, closed });
    },
  });
}

export const WEEKLY_SOURCE_NAMED_CONNECTION_CONTRACT = deepFreeze({
  version: 'WEEKLY_SOURCE_NAMED_CONNECTION_V1',
  authority: '23A_EXECUTABLE_TEST_HARNESS_SPECIFICATION.md section 13',
  minimumSessions: 2,
  maximumSessions: MAX_SESSIONS,
  localHostOnly: true,
  passwordInUrl: false,
  metaCommandsRefused: true,
  servesProofIds: Object.freeze(['R12', 'R15', 'R37', 'R42', 'ROT-002', 'ROT-003', 'ROT-012', 'UNA-014']),
});
