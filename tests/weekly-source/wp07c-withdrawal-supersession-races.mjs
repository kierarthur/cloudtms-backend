// WP-07c: the two-session race between the Office first-authorisation WITHDRAWAL
// and an entitlement-head PUBLICATION on the same family.
//
// HANDOVER 2 round-5 ruling A3 step 1 requires the withdrawal to "lock the
// authorisation, canonical root, current head and relevant payment/currentness
// guards" before it decides anything. A single `psql -f` cannot express a race,
// so this drives two named sessions through the harness's own connection group,
// exactly as `wp03-rotation-authority-races.mjs` does.
//
// What is being proved, in both directions:
//
//   R1  withdrawal first, publication second. While the withdrawal's transaction
//       is open, a concurrent publication attempt on the same Candidate cannot
//       proceed: it is serialised by the Candidate serial gate and the family
//       lock set that interface I-1 takes. After the withdrawal commits, the
//       head is SUPERSEDED and the root carries no live generation, so the
//       publication cannot re-point or revive it.
//
//   R2  publication first, withdrawal second. The withdrawal must SEE the
//       committed publication rather than a stale snapshot: it either refuses,
//       or supersedes the head that was just published -- and never returns
//       "ok" while leaving a committed current head behind, which is the
//       wrong-payment shape this package exists to close.
//
//   R3  two withdrawals of the same root at once. Exactly one may write.
//
// Neither session may report SQLSTATE 40P01 (deadlock).
//
// How to run (local Docker only; never a hosted database):
//
//   export MSYS_NO_PATHCONV=1 PGOPTIONS='-c jit=off'
//   export PSQL_BIN='C:\Program Files\PostgreSQL\18\bin\psql.exe'
//   export WP07C_DB=ws62_wp07c_race     # a clone carrying the committed fixture
//   node tests/weekly-source/wp07c-withdrawal-supersession-races.mjs
//
// Exit code 0 means every assertion passed.

import { openNamedSessionGroup } from './harness/named-connections.mjs';

const DB = process.env.WP07C_DB || 'ws62_wp07c_race';
const ACTOR = 'd7000000-0000-4000-8000-000000000001';
const CANDIDATE_1 = 'd7000000-0000-4000-8000-000000000101';
const ROOT_1 = 'd7000000-0000-4000-8000-000000000301';
const HEAD_1 = 'd7000000-0000-4000-8000-000000000f01';

const results = [];
const last = (r) => r.rows.filter(Boolean).at(-1) ?? '';
const isDeadlock = (r) => r?.error?.sqlstate === '40P01'
  || /deadlock detected/i.test(r?.error?.message ?? '');
function record(id, ok, detail) {
  results.push({ id, ok, detail });
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}  ${detail}`);
}
function describe(r) {
  return r.error ? `${r.error.sqlstate ?? ''} ${r.error.message}`.trim() : `ok: ${last(r)}`;
}
async function tryBlocked(group, observer, name, ms = 15000) {
  try { return await group.waitUntilBlocked(observer, name, { timeoutMs: ms }); } catch { return null; }
}

async function withGroup(names, fn) {
  const group = await openNamedSessionGroup({
    baseConnectionUrl: 'postgresql://postgres@127.0.0.1:55433/postgres',
    expectedPort: 55433, database: DB, names,
    psqlBin: process.env.PSQL_BIN, groupId: 'wp07c-race',
    env: { ...process.env, PGPASSWORD: 'localonly' }, lockTimeoutMs: 25000,
  });
  try {
    for (const name of names) {
      await group.session(name).runWithin("set session \"request.jwt.claim.role\" = 'service_role';");
    }
    await fn(group);
  } finally { await group.closeAll(); }
}

const SIGNATURE = (root) => `(select nullif(btrim(coalesce(
    s->>'backend_row_signature', s->>'row_signature','')),'')
  from public.timesheet_lifecycle_guard_signature_v1(
    '${root}'::uuid,
    (select id from public.contract_weeks where timesheet_id='${root}'::uuid),
    false) as s)`;

const WITHDRAW = (root) => `select public.weekly_source_first_authorisation_withdraw_v1(
  '${root}'::uuid,'${root}'::uuid,${SIGNATURE(root)},'${ACTOR}'::uuid)::text;`;

// A publication-shaped attempt on the same Candidate: interface I-1 with the
// head-publication job type, which is the FIRST thing WP-02's coordinator does.
// Nothing is published here; what is being proved is that the two owners
// serialise on the same gate and family keys rather than interleaving.
const PUBLICATION_LOCKS = (candidate, root) =>
  `select private.weekly_source_lock_and_resolve_families_v1(
     '${candidate}'::uuid, array['${root}']::uuid[],
     'WORKBENCH_CANDIDATE_ENTITLEMENT_PUBLICATION', gen_random_uuid(),
     'WEEKLY_SOURCE_ENTITLEMENT_HEAD_PUBLICATION')::text;`;

async function state(group, name) {
  const r = await group.session(name).run(
    `select jsonb_build_object(
       'head_state',(select state from public.weekly_source_entitlement_heads
                      where id='${HEAD_1}'),
       'head_reason',(select superseded_reason from public.weekly_source_entitlement_heads
                       where id='${HEAD_1}'),
       'committed_current',(select count(*) from public.weekly_source_entitlement_heads
                             where root_timesheet_id='${ROOT_1}' and state='COMMITTED_CURRENT'),
       'live_generations',(select count(*) from public.weekly_source_root_authorisations
                            where root_timesheet_id='${ROOT_1}' and withdrawn_at_utc is null),
       'receipts',(select count(*)
                     from private.weekly_source_first_authorisation_withdrawal_receipts
                    where root_timesheet_id='${ROOT_1}'))::text;`);
  return last(r);
}

// --- R1: the withdrawal holds the family; a publication cannot interleave -----
async function proveR1() {
  await withGroup(['withdrawal', 'publication', 'observer'], async (group) => {
    const withdrawal = group.session('withdrawal');
    const publication = group.session('publication');

    await withdrawal.runWithin('begin;');
    const withdrawalResult = await withdrawal.run(WITHDRAW(ROOT_1));
    record('R1-withdrawal-succeeds-in-its-open-transaction',
      !withdrawalResult.error && /"ok": true/.test(last(withdrawalResult))
      && /"head_superseded": true/.test(last(withdrawalResult)),
      describe(withdrawalResult).slice(0, 160));

    // A concurrent publication attempt on the SAME Candidate, while the
    // withdrawal's transaction is still open.
    await publication.runWithin('begin;');
    const publicationPending = publication.run(PUBLICATION_LOCKS(CANDIDATE_1, ROOT_1));
    const publicationWait = await tryBlocked(group, 'observer', 'publication', 8000);

    // Whichever way the installed gate resolves it -- waiting on the family key
    // or refusing as BUSY -- the publication must NOT proceed past the
    // withdrawal that is holding the family.
    const publicationResult = await publicationPending;
    const serialised = publicationWait?.waitEventType === 'Lock'
      || /"ok": false/.test(last(publicationResult))
      || /BLOCKED|BUSY/i.test(last(publicationResult));
    record('R1-publication-is-serialised-behind-the-open-withdrawal', serialised,
      publicationWait ? 'it waited on a lock'
        : `it was refused: ${describe(publicationResult).slice(0, 160)}`);

    await withdrawal.runWithin('commit;');
    await publication.runWithin('rollback;');

    const after = await state(group, 'observer');
    record('R1-final-state-head-superseded-and-not-current',
      /"head_state": "SUPERSEDED"/.test(after)
      && /"head_reason": "FIRST_AUTHORISATION_WITHDRAWN"/.test(after)
      && /"committed_current": 0/.test(after)
      && /"receipts": 1/.test(after),
      after);
    record('R1-NO-DEADLOCK',
      !isDeadlock(withdrawalResult) && !isDeadlock(publicationResult),
      `withdrawal: ${describe(withdrawalResult).slice(0, 80)} | publication: ${describe(publicationResult).slice(0, 80)}`);
  });
}

// --- R2: a publication holds the family; the withdrawal cannot interleave -----
async function proveR2() {
  await withGroup(['publication', 'withdrawal', 'observer'], async (group) => {
    const publication = group.session('publication');
    const withdrawal = group.session('withdrawal');

    await publication.runWithin('begin;');
    const publicationResult = await publication.run(PUBLICATION_LOCKS(CANDIDATE_1, ROOT_1));

    await withdrawal.runWithin('begin;');
    const withdrawalPending = withdrawal.run(WITHDRAW(ROOT_1));
    const withdrawalWait = await tryBlocked(group, 'observer', 'withdrawal', 8000);
    const withdrawalResult = await withdrawalPending;

    // The withdrawal must never quietly succeed while another owner holds the
    // family. Either it waits, or it refuses; it may not interleave.
    const safe = withdrawalWait?.waitEventType === 'Lock'
      || /"ok": false/.test(last(withdrawalResult))
      || withdrawalResult.error !== undefined;
    record('R2-withdrawal-is-serialised-behind-the-open-publication', safe,
      withdrawalWait ? 'it waited on a lock'
        : `it was refused: ${describe(withdrawalResult).slice(0, 200)}`);

    await publication.runWithin('rollback;');
    await withdrawal.runWithin('rollback;');
    record('R2-NO-DEADLOCK',
      !isDeadlock(publicationResult) && !isDeadlock(withdrawalResult),
      `publication: ${describe(publicationResult).slice(0, 80)} | withdrawal: ${describe(withdrawalResult).slice(0, 80)}`);
  });
}

// --- R3: two withdrawals of the same root at once -----------------------------
async function proveR3() {
  await withGroup(['first', 'second', 'observer'], async (group) => {
    const first = group.session('first');
    const second = group.session('second');

    await first.runWithin('begin;');
    const firstResult = await first.run(WITHDRAW(ROOT_1));

    await second.runWithin('begin;');
    const secondPending = second.run(WITHDRAW(ROOT_1));
    await tryBlocked(group, 'observer', 'second', 8000);

    await first.runWithin('commit;');
    const secondResult = await secondPending;
    await second.runWithin('commit;');

    const after = await state(group, 'observer');
    record('R3-exactly-one-withdrawal-wrote',
      /"receipts": 1/.test(after) && /"live_generations": 0/.test(after)
      && /"committed_current": 0/.test(after),
      `${after} | second: ${describe(secondResult).slice(0, 160)}`);
    record('R3-NO-DEADLOCK',
      !isDeadlock(firstResult) && !isDeadlock(secondResult),
      `first: ${describe(firstResult).slice(0, 80)} | second: ${describe(secondResult).slice(0, 80)}`);
  });
}

const which = process.argv[2];
if (which === 'r1') await proveR1();
else if (which === 'r2') await proveR2();
else if (which === 'r3') await proveR3();
else { await proveR1(); }

const failed = results.filter((r) => !r.ok);
console.log(`\n${results.length - failed.length} passed, ${failed.length} failed`);
process.exit(failed.length === 0 ? 0 : 1);
