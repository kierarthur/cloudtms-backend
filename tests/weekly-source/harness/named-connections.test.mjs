import assert from 'node:assert/strict';
import test from 'node:test';
import {
  assertNamedSessionTarget,
  assertNoMetaCommand,
  deriveNamedSessionUrl,
  openNamedSessionGroup,
  WEEKLY_SOURCE_NAMED_CONNECTION_CONTRACT,
} from './named-connections.mjs';

const BASE = 'postgresql://postgres@127.0.0.1:55433/banking_modal_v2_test';

test('TH-004 named sessions accept only a local task-owned target with no password in the URL', () => {
  assert.equal(assertNamedSessionTarget(BASE, { expectedPort: 55433, database: 'banking_modal_v2_test' }), true);
  assert.equal(assertNamedSessionTarget('postgresql://postgres@127.0.0.1:55433/ws_new_01_01', { expectedPort: 55433 }), true);
  for (const target of [
    'postgresql://postgres@example.invalid:55433/banking_modal_v2_test',
    'postgresql://postgres@127.0.0.1:55433/cloudtms_test_clone2/x',
    'postgresql://postgres:localonly@127.0.0.1:55433/banking_modal_v2_test',
    'postgresql://someone@127.0.0.1:55433/banking_modal_v2_test',
    'postgres://postgres@127.0.0.1:55433/banking_modal_v2_test',
    'postgresql://postgres@127.0.0.1:55433/banking_modal_v2_test?sslmode=disable',
  ]) {
    assert.throws(
      () => assertNamedSessionTarget(target, { expectedPort: 55433 }),
      (error) => error.code === 'NAMED_SESSION_TARGET_REFUSED' || error.code === 'NAMED_SESSION_TARGET_INVALID',
      target,
    );
  }
  // A different port than the one the controller proved is refused.
  assert.throws(
    () => assertNamedSessionTarget(BASE, { expectedPort: 55499 }),
    (error) => error.code === 'NAMED_SESSION_TARGET_REFUSED',
  );
});

test('TH-004 a clone target is derived from the proved base, never rebuilt by hand', () => {
  assert.equal(
    deriveNamedSessionUrl(BASE, 'ws62_wp16b_a', { expectedPort: 55433 }),
    'postgresql://postgres@127.0.0.1:55433/ws62_wp16b_a',
  );
  for (const bad of ['Postgres', 'ws62-wp16b', '', 'a'.repeat(64), 'ws62/wp']) {
    assert.throws(
      () => deriveNamedSessionUrl(BASE, bad, { expectedPort: 55433 }),
      (error) => error.code === 'NAMED_SESSION_DATABASE_INVALID',
      String(bad),
    );
  }
});

test('TH-017 named sessions refuse psql meta-commands so a statement cannot escape the session', () => {
  assert.equal(assertNoMetaCommand('select 1;'), true);
  for (const bad of ['\\! dir', '  \\i /tmp/x.sql', 'select 1;\n\\copy t from stdin', '\\q', '   ']) {
    assert.throws(
      () => assertNoMetaCommand(bad),
      (error) => error.code === 'NAMED_SESSION_META_COMMAND_REFUSED' || error.code === 'NAMED_SESSION_SQL_EMPTY',
      JSON.stringify(bad),
    );
  }
});

test('TH-004 a serial group needs two or more uniquely named, bounded connections', async () => {
  for (const [names, code] of [
    [['only'], 'NAMED_SESSION_GROUP_TOO_SMALL'],
    [Array.from({ length: 9 }, (_, index) => `s${index}`), 'NAMED_SESSION_GROUP_TOO_LARGE'],
    [['same', 'same'], 'NAMED_SESSION_NAME_INVALID'],
    [['Winner', 'loser'], 'NAMED_SESSION_NAME_INVALID'],
  ]) {
    await assert.rejects(
      () => openNamedSessionGroup({ baseConnectionUrl: BASE, expectedPort: 55433, database: 'banking_modal_v2_test', names }),
      (error) => error.code === code,
      code,
    );
  }
  assert.equal(WEEKLY_SOURCE_NAMED_CONNECTION_CONTRACT.minimumSessions, 2);
  assert.equal(WEEKLY_SOURCE_NAMED_CONNECTION_CONTRACT.passwordInUrl, false);
  for (const id of ['R37', 'ROT-002', 'ROT-003', 'ROT-012', 'UNA-014']) {
    assert(WEEKLY_SOURCE_NAMED_CONNECTION_CONTRACT.servesProofIds.includes(id), id);
  }
});

// Live proof. Opt in with a local, task-owned PostgreSQL 17.11 clone:
//   CLOUDTMS_WEEKLY_SOURCE_NAMED_SESSION_URL=postgresql://postgres@127.0.0.1:55433/<clone>
// psql reads PGPASSWORD from the environment; the URL never carries it.
test('TH-004 two named sessions really contend for the same row and the loser observes the winner', async (t) => {
  const url = process.env.CLOUDTMS_WEEKLY_SOURCE_NAMED_SESSION_URL;
  if (!url) return t.skip('CLOUDTMS_WEEKLY_SOURCE_NAMED_SESSION_URL is not set');
  const parsed = new URL(url);
  const database = decodeURIComponent(parsed.pathname.slice(1));
  const group = await openNamedSessionGroup({
    baseConnectionUrl: url,
    expectedPort: Number(parsed.port),
    database,
    names: ['winner', 'loser', 'observer'],
    groupId: 'ws-race',
    psqlBin: process.env.PSQL_BIN ?? 'psql',
    lockTimeoutMs: 0,
  });
  try {
    const winner = group.session('winner');
    const loser = group.session('loser');
    const observer = group.session('observer');

    await winner.runWithin('create temporary table ws_race_probe(id int primary key, note text);', 30_000);
    // A real shared row: a temporary table is per-session, so use an unlogged table both see.
    const setup = await observer.runWithin(
      'create unlogged table if not exists ws_named_session_probe(id int primary key, note text); truncate ws_named_session_probe; insert into ws_named_session_probe values (1, \'initial\');',
      30_000,
    );
    assert.equal(setup.error, null, 'probe table setup must succeed');

    await winner.runWithin('begin;', 10_000);
    const locked = await winner.runWithin('select note from ws_named_session_probe where id = 1 for update;', 30_000);
    assert.equal(locked.error, null);
    assert.equal(locked.rows.filter(Boolean).at(-1), 'initial');

    // The loser's statement is deliberately not awaited: it must block on the row lock.
    const contended = loser.run('select note from ws_named_session_probe where id = 1 for update;');
    const blocked = await group.waitUntilBlocked('observer', 'loser', { timeoutMs: 30_000 });
    assert.equal(blocked.waitEventType, 'Lock', 'the loser must be waiting on a Lock, not merely slow');
    assert.equal(loser.isBusy(), true);

    await winner.runWithin('update ws_named_session_probe set note = \'winner-committed\' where id = 1;', 30_000);
    await winner.runWithin('commit;', 30_000);

    const observed = await contended;
    assert.equal(observed.error, null);
    assert.equal(observed.rows.filter(Boolean).at(-1), 'winner-committed', 'the loser must observe the winner committed state');
    await loser.runWithin('rollback;', 10_000);

    const evidence = group.evidence();
    assert.equal(evidence.sessionCount, 3);
    assert.equal(evidence.database, database);
    assert(!JSON.stringify(evidence).includes('postgresql://'), 'named-session evidence must not carry a connection target');

    const cleaned = await observer.runWithin('drop table if exists ws_named_session_probe;', 30_000);
    assert.equal(cleaned.error, null);
  } finally {
    const closed = await group.closeAll();
    assert.equal(closed.complete, true);
  }
});

// R37, ROT-002, ROT-003 and ROT-012 all assert "no deadlock in either order". That claim is
// only testable if the harness can produce a deadlock and read PostgreSQL's own verdict.
test('TH-004 a real deadlock is surfaced as SQLSTATE 40P01 instead of hanging the group', async (t) => {
  const url = process.env.CLOUDTMS_WEEKLY_SOURCE_NAMED_SESSION_URL;
  if (!url) return t.skip('CLOUDTMS_WEEKLY_SOURCE_NAMED_SESSION_URL is not set');
  const parsed = new URL(url);
  const database = decodeURIComponent(parsed.pathname.slice(1));
  const group = await openNamedSessionGroup({
    baseConnectionUrl: url,
    expectedPort: Number(parsed.port),
    database,
    names: ['first', 'second', 'watcher'],
    groupId: 'ws-deadlock',
    psqlBin: process.env.PSQL_BIN ?? 'psql',
  });
  try {
    const first = group.session('first');
    const second = group.session('second');
    const watcher = group.session('watcher');
    const setup = await watcher.runWithin(
      'create unlogged table if not exists ws_named_session_deadlock(id int primary key, note text); truncate ws_named_session_deadlock; insert into ws_named_session_deadlock values (1, \'a\'), (2, \'b\');',
      30_000,
    );
    assert.equal(setup.error, null);

    await first.runWithin('begin;', 10_000);
    await second.runWithin('begin;', 10_000);
    assert.equal((await first.runWithin('update ws_named_session_deadlock set note = \'a1\' where id = 1;', 30_000)).error, null);
    assert.equal((await second.runWithin('update ws_named_session_deadlock set note = \'b2\' where id = 2;', 30_000)).error, null);

    const firstWaits = first.run('update ws_named_session_deadlock set note = \'a2\' where id = 2;');
    await group.waitUntilBlocked('watcher', 'first', { timeoutMs: 30_000 });
    const secondWaits = second.run('update ws_named_session_deadlock set note = \'b1\' where id = 1;');

    const [firstResult, secondResult] = await Promise.all([firstWaits, secondWaits]);
    const errors = [firstResult.error, secondResult.error].filter(Boolean);
    assert.equal(errors.length, 1, 'exactly one session must lose the deadlock');
    assert.equal(errors[0].severity, 'ERROR');
    assert.match(String(errors[0].message), /deadlock detected/i);

    await first.runWithin('rollback;', 10_000);
    await second.runWithin('rollback;', 10_000);
    assert.equal((await watcher.runWithin('drop table if exists ws_named_session_deadlock;', 30_000)).error, null);
  } finally {
    const closed = await group.closeAll();
    assert.equal(closed.complete, true);
  }
});
