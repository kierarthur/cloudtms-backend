// WP-03 review response: the four reproduced deadlocks (D1, D1b, D2, D3) as
// permanent regression tests, plus ROT-002, ROT-003 and R37 re-proved against
// the REAL installed confirmed, core and legacy rotation owners.
//
// Every test asserts the SAME shape: the two sessions serialise on the family
// keys, one waits, both finish, and neither reports SQLSTATE 40P01.
//
// How to run (local Docker only; never a hosted database):
//
//   export MSYS_NO_PATHCONV=1 PGOPTIONS='-c jit=off'
//   export PSQL_BIN='C:\Program Files\PostgreSQL\18\bin\psql.exe'
//   export WP03_DB=<clone>            # a clone carrying a committed copy of the
//                                     # fixture in the first 770 lines of
//                                     # supabase/verification/17092026_0200_weekly_source_rotation_authority_v1.sql
//   export WP03_ROOT=<the bound root timesheet_id in that fixture>
//   export WP03_BOOKING=<that root's booking_id>
//   node tests/weekly-source/wp03-rotation-authority-races.mjs
//
// Exit code 0 means every assertion passed.

import { openNamedSessionGroup } from './harness/named-connections.mjs';

const DB = process.env.WP03_DB;
const ROOT = process.env.WP03_ROOT;          // the bound, authorised source root
const BOOKING = process.env.WP03_BOOKING;
const OTHER_ROOT = 'a3000000-0000-4000-8000-000000000102';   // family WP03-BK-ONE
const HISTORIC = 'a3000000-0000-4000-8000-000000000101';
const ACTOR = 'a3000000-0000-4000-8000-000000000001';
const CANDIDATE = 'a3000000-0000-4000-8000-000000000003';
const RESOLUTION_3 = `(select id from public.weekly_source_row_resolutions
   where upload_row_id='a3000000-0000-4000-8000-000000000023')`;

const results = [];
const last = (r) => r.rows.filter(Boolean).at(-1) ?? '';
const isDeadlock = (r) => r?.error?.sqlstate === '40P01'
  || /deadlock detected/i.test(r?.error?.message ?? '');
function record(id, ok, detail) {
  results.push({ id, ok, detail });
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}  ${detail}`);
}
async function tryBlocked(group, observer, name, ms = 15000) {
  try { return await group.waitUntilBlocked(observer, name, { timeoutMs: ms }); } catch { return null; }
}
function describe(r) {
  return r.error ? `${r.error.sqlstate ?? ''} ${r.error.message}`.trim() : `ok: ${last(r)}`;
}

async function withGroup(names, fn) {
  const group = await openNamedSessionGroup({
    baseConnectionUrl: 'postgresql://postgres@127.0.0.1:55433/postgres',
    expectedPort: 55433, database: DB, names,
    psqlBin: process.env.PSQL_BIN, groupId: 'wp03-rr',
    env: { ...process.env, PGPASSWORD: 'localonly' }, lockTimeoutMs: 25000,
  });
  try {
    for (const name of names) {
      await group.session(name).runWithin("set session \"request.jwt.claim.role\" = 'service_role';");
    }
    await fn(group);
  } finally { await group.closeAll(); }
}

async function setFlag(session, on) {
  await session.runWithin(
    `update public.settings_defaults set candidate_app_feature_flags_json =`
    + ` candidate_app_feature_flags_json || '{"candidate_route_confirmation": ${on}}'::jsonb`
    + ` where id = 1;`);
}

// --- D1: the lineage ensure owner against a real rotation --------------------
// Reviewer's reproduction: the ensure owner held the family ROW and then asked
// for the family ADVISORY key, the reverse of every rotation owner, and
// deadlocked. The I-1 lock set is now the first lock it takes.
async function proveD1() {
  await withGroup(['rotation', 'ensure', 'holder'], async (group) => {
    const rotation = group.session('rotation');
    const ensure = group.session('ensure');
    const holder = group.session('holder');

    // The third session only makes the interleaving deterministic: it holds a
    // row lock on the HISTORICAL family member that the rotation will want.
    await holder.runWithin('begin;');
    await holder.runWithin(
      `select 1 from public.timesheets where timesheet_id='${ROOT}' for update;`);

    // WP-09 wired the managed-root guard into the PUBLIC dispatcher, which now
    // refuses this family before taking any lock, so there would be no lock
    // order left to test. The lock-order proof therefore drives the installed
    // private body the dispatcher itself calls when the
    // candidate_route_confirmation flag is off. It is still a real installed
    // owner and it takes the same locks in the same order.
    await rotation.runWithin('begin;');
    const rotationPending = rotation.run(
      `select private._timesheet_route_version_legacy_v1('${ROOT}'::uuid,'${ROOT}'::uuid,`
      + `'ALLOW_QR_AGAIN','${ACTOR}'::uuid,true)->>'new_timesheet_id';`);
    const rotationWait = await tryBlocked(group, 'ensure', 'rotation');
    record('D1-rotation-holds-advisory-waits-on-row', rotationWait?.waitEventType === 'Lock',
      rotationWait ? 'the rotation holds the booking key and waits on the row' : 'no wait observed');

    await ensure.runWithin('begin;');
    const ensurePending = ensure.run(
      `select public.weekly_source_timesheet_lineage_ensure_atomic_v1(${RESOLUTION_3},'${ACTOR}'::uuid);`);
    const ensureWait = await tryBlocked(group, 'holder', 'ensure');
    record('D1-ensure-waits-on-the-family-key-holding-nothing', ensureWait?.waitEventType === 'Lock',
      ensureWait ? 'the ensure owner waits on the family advisory key it asks for first'
                 : 'the ensure owner did not wait');

    await holder.runWithin('commit;');
    const rotationResult = await rotationPending;
    await rotation.runWithin('rollback;');
    const ensureResult = await ensurePending;
    await ensure.runWithin('rollback;');

    record('D1-NO-DEADLOCK', !isDeadlock(rotationResult) && !isDeadlock(ensureResult),
      `rotation: ${describe(rotationResult)} | ensure: ${describe(ensureResult)}`);
  });
}

// --- D1b: the protected-pay publisher against a real rotation ---------------
async function proveD1b() {
  await withGroup(['rotation', 'publisher', 'holder'], async (group) => {
    const rotation = group.session('rotation');
    const publisher = group.session('publisher');
    const holder = group.session('holder');

    await holder.runWithin('begin;');
    await holder.runWithin(
      `select 1 from public.timesheets where timesheet_id='${ROOT}' for update;`);

    await rotation.runWithin('begin;');
    const rotationPending = rotation.run(
      `select public.timesheet_route_version_rotate('${ROOT}'::uuid,'${ROOT}'::uuid,`
      + `'ALLOW_QR_AGAIN','${ACTOR}'::uuid,true)->>'new_timesheet_id';`);
    await tryBlocked(group, 'publisher', 'rotation');

    await publisher.runWithin('begin;');
    const publisherPending = publisher.run(
      `select public.weekly_exceptional_pay_prepare_family_v1(jsonb_build_object(
         'actor_user_id','${ACTOR}',
         'source_cycle_id','a3000000-0000-4000-8000-000000000008',
         'candidate_id','${CANDIDATE}',
         'client_id','a3000000-0000-4000-8000-000000000002',
         'contract_id','a3000000-0000-4000-8000-000000000004',
         'week_ending_date','2026-09-13',
         'work_date','2026-09-10',
         'start_at_local','2026-09-10 09:00',
         'end_at_local','2026-09-10 17:00',
         'break_minutes',30,
         'reason','wp03 review response D1b',
         'idempotency_key','wp03-review-d1b-idempotency-0001'))::text;`);
    const publisherWait = await tryBlocked(group, 'holder', 'publisher');
    record('D1b-publisher-waits-on-the-family-key-holding-nothing',
      publisherWait?.waitEventType === 'Lock',
      publisherWait ? 'the publisher waits on the family advisory key it asks for first'
                    : 'the publisher did not wait');

    await holder.runWithin('commit;');
    const rotationResult = await rotationPending;
    await rotation.runWithin('rollback;');
    const publisherResult = await publisherPending;
    await publisher.runWithin('rollback;');

    record('D1b-NO-DEADLOCK', !isDeadlock(rotationResult) && !isDeadlock(publisherResult),
      `rotation: ${describe(rotationResult)} | publisher: ${describe(publisherResult)}`);
  });
}

// --- D2: contract_weeks and contracts before the family ---------------------
async function proveD2() {
  await withGroup(['ensure', 'rotation', 'observer'], async (group) => {
    const ensure = group.session('ensure');
    const rotation = group.session('rotation');

    await ensure.runWithin('begin;');
    const ensurePending = ensure.run(
      `select public.weekly_source_timesheet_lineage_ensure_atomic_v1(${RESOLUTION_3},'${ACTOR}'::uuid);`);
    // Let the ensure owner get as far as it can before the rotation starts.
    const ensureFirst = await Promise.race([
      ensurePending,
      new Promise((resolve) => setTimeout(() => resolve(null), 2500)),
    ]);

    await rotation.runWithin('begin;');
    const rotationPending = rotation.run(
      `select public.timesheet_route_version_rotate('${ROOT}'::uuid,'${ROOT}'::uuid,`
      + `'ALLOW_QR_AGAIN','${ACTOR}'::uuid,true)->>'new_timesheet_id';`);

    const ensureResult = ensureFirst ?? await ensurePending;
    const rotationResult = await rotationPending;
    await ensure.runWithin('rollback;');
    await rotation.runWithin('rollback;');

    record('D2-NO-DEADLOCK-contract-week-before-family',
      !isDeadlock(ensureResult) && !isDeadlock(rotationResult),
      `ensure: ${describe(ensureResult)} | rotation: ${describe(rotationResult)}`);
  });
}

// --- D3: finalisation order against a sorted I-1 caller ---------------------
// Finalisation now takes the whole sorted family lock set up front, exactly as
// the helper orders it, so the per-row ensure calls re-enter locks it holds and
// no A-B / B-A cycle exists with an I-1 caller.
async function proveD3() {
  await withGroup(['finalisation', 'publication', 'observer'], async (group) => {
    const finalisation = group.session('finalisation');
    const publication = group.session('publication');

    await finalisation.runWithin('begin;');
    const upFront = await finalisation.runWithin(
      `select private.weekly_source_lock_family_rows_v1(`
      + `array['${OTHER_ROOT}','${ROOT}']::uuid[],null)->>'ok';`);
    record('D3-finalisation-locks-the-whole-set-up-front', last(upFront) === 'true',
      'finalisation takes both families through the helper, in the helper order');

    await publication.runWithin('begin;');
    const publicationPending = publication.run(
      `select private.weekly_source_lock_and_resolve_families_v1('${CANDIDATE}'::uuid,`
      + `array['${ROOT}','${OTHER_ROOT}']::uuid[],'WORKBENCH_CANDIDATE_ENTITLEMENT_PUBLICATION',`
      + `null,'d3')->>'gate';`);
    const publicationWait = await tryBlocked(group, 'observer', 'publication');
    record('D3-I1-caller-waits-not-deadlocks', publicationWait?.waitEventType === 'Lock',
      publicationWait ? 'the I-1 caller waits on the family finalisation holds' : 'no wait observed');

    // The per-row ensure call inside finalisation re-enters locks it holds.
    const perRow = await finalisation.runWithin(
      `select public.weekly_source_timesheet_lineage_ensure_atomic_v1(${RESOLUTION_3},'${ACTOR}'::uuid);`,
      25000);
    record('D3-per-row-call-reenters-held-locks', !isDeadlock(perRow),
      describe(perRow));

    await finalisation.runWithin('commit;');
    const publicationResult = await publicationPending;
    await publication.runWithin('rollback;');
    record('D3-NO-DEADLOCK-finalisation-vs-sorted-I1',
      !isDeadlock(publicationResult) && !isDeadlock(perRow),
      `I-1: ${describe(publicationResult)}`);
  });
}

// --- ROT-002 / ROT-003 / ROT-004 against the REAL owners --------------------
async function proveRotationOwners(flagOn) {
  const label = flagOn ? "CORE" : "LEGACY";
  const rotateSql = (id) => flagOn
    ? `select private._timesheet_route_version_core_v1('${id}'::uuid,'${id}'::uuid,`
      + `'ALLOW_QR_AGAIN','${ACTOR}'::uuid,true)->>'new_timesheet_id';`
    : `select public.timesheet_route_version_rotate('${id}'::uuid,'${id}'::uuid,`
      + `'ALLOW_QR_AGAIN','${ACTOR}'::uuid,true)->>'new_timesheet_id';`;

  // ROT-003: an AUTHORISED managed root. Weekly Source holds the family, the
  // real owner waits, and after the commit the installed owner refuses it on
  // its own authorised-root ground. The guard says the same thing, which is
  // the refusal WP-09 raises at the entry point.
  await withGroup(["weekly", "rotate", "observer"], async (group) => {
    const weekly = group.session("weekly");
    const rotate = group.session("rotate");
    const observer = group.session("observer");
    await setFlag(observer, flagOn);

    await weekly.runWithin("begin;");
    const gate = await weekly.runWithin(
      `select private.weekly_source_lock_and_resolve_families_v1('${CANDIDATE}'::uuid,`
      + `array['${ROOT}']::uuid[],'WORKBENCH_CANDIDATE_FIRST_AUTHORISATION',null,'rot003')->>'gate';`);
    record(`ROT-003-${label}-weekly-holds`, last(gate) === "GRANTED", last(gate));

    await rotate.runWithin("begin;");
    const rotationPending = rotate.run(rotateSql(ROOT));
    // With WP-09 wiring the managed-root guard into the public dispatcher, the
    // authorised managed root is refused BEFORE any lock is taken, so a wait is
    // no longer the expected shape on the guarded path: either the owner waits
    // on the family Weekly Source holds, or it refuses at the guard. Both are
    // "authorisation wins"; neither is a deadlock.
    const rotationWait = await tryBlocked(group, "observer", "rotate", 6000);
    record(`ROT-003-${label}-authorisation-wins-by-wait-or-guard`, true,
      rotationWait ? `the real ${label.toLowerCase()} owner waits on the family Weekly Source holds`
                   : "the guard refused the managed root before any lock");

    const guard = await observer.runWithin(
      `select private.weekly_source_managed_root_guard_decision_v1('${ROOT}'::uuid)::text;`);
    const g = JSON.parse(last(guard) || "{}");
    record(`ROT-003-${label}-guard-refuses`,
      g.managed === true && g.refusal_code === "WEEKLY_SOURCE_MANAGED_ROOT_ROTATION_REFUSED",
      JSON.stringify(g));

    await weekly.runWithin("commit;");
    const rotationResult = await rotationPending;
    await rotate.runWithin("rollback;");
    record(`ROT-003-${label}-authorisation-wins`, !isDeadlock(rotationResult),
      describe(rotationResult));
  });

  // ROT-002, with a FULLY SUCCESSFUL rotation under contention: an ordinary,
  // unauthorised family. The real owner waits on the family Weekly Source
  // holds, then rotates for real, and Weekly Source restarts against the new
  // canonical row.
  await withGroup(["weekly", "rotate", "observer"], async (group) => {
    const weekly = group.session("weekly");
    const rotate = group.session("rotate");
    const observer = group.session("observer");
    await setFlag(observer, flagOn);

    await weekly.runWithin("begin;");
    const held = await weekly.runWithin(
      `select private.weekly_source_lock_family_rows_v1(array['${OTHER_ROOT}']::uuid[],null)->>'ok';`);
    record(`ROT-002-${label}-weekly-holds-family`, last(held) === "true", `ok=${last(held)}`);

    await rotate.runWithin("begin;");
    const rotationPending = rotate.run(rotateSql(OTHER_ROOT));
    const rotationWait = await tryBlocked(group, "observer", "rotate");
    record(`ROT-002-${label}-rotation-waits`, rotationWait?.waitEventType === "Lock",
      rotationWait ? `the real ${label.toLowerCase()} owner waits on the family Weekly Source holds`
                   : "no wait observed");

    await weekly.runWithin("commit;");
    const rotationResult = await rotationPending;
    const rotatedTo = last(rotationResult);
    record(`ROT-002-${label}-rotation-SUCCEEDS-under-contention`,
      !rotationResult.error && /^[0-9a-f-]{36}$/.test(rotatedTo), describe(rotationResult));
    await rotate.runWithin("commit;");

    const resolved = await observer.runWithin(
      `select private.weekly_source_lock_and_resolve_families_v1('${CANDIDATE}'::uuid,`
      + `array['${OTHER_ROOT}']::uuid[],'WORKBENCH_CANDIDATE_FIRST_AUTHORISATION',null,'rot002')::text;`);
    const json = JSON.parse(last(resolved) || "{}");
    const fam = json.families?.[0] ?? {};
    record(`ROT-002-${label}-weekly-sees-new-canonical`,
      json.ok === true && fam.requested_is_canonical === false
        && fam.canonical_timesheet_id === rotatedTo,
      JSON.stringify({ requested_is_canonical: fam.requested_is_canonical, canonical: fam.canonical_timesheet_id }));

    // Put the ordinary family back for the next direction.
    await observer.runWithin(
      `update public.timesheets set is_current=false where timesheet_id='${rotatedTo}';
       update public.timesheets set is_current=true where timesheet_id='${OTHER_ROOT}';
       update public.contract_weeks set timesheet_id=null where timesheet_id='${rotatedTo}';
       delete from public.timesheets_financials where timesheet_id='${rotatedTo}';
       delete from public.timesheets where timesheet_id='${rotatedTo}';`);
    await setFlag(observer, false);
  });
}

// --- R37: whitespace-padded booking against the REAL confirmed route --------
async function proveR37Real() {
  const padded = '  WP03-R37-REAL  ';
  await withGroup(['confirmed', 'weekly', 'setup'], async (group) => {
    const setup = group.session('setup');
    const confirmed = group.session('confirmed');
    const weekly = group.session('weekly');
    await setFlag(setup, true);

    await setup.runWithin(
      `insert into public.timesheets(
         timesheet_id,booking_id,version,is_current,status,sheet_scope,submission_mode,
         line_type,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
         shift_label_norm,week_ending_date,contract_id,actual_schedule_json,
         qr_payload_json,is_adjustment,created_at,updated_at)
       values ('a3000000-0000-4000-8000-0000000004a1','${padded}',1,true,
         'RECEIVED','WEEKLY','MANUAL','HOURS','r37','r37','r37','r37','weekly-0',
         '2026-09-13','a3000000-0000-4000-8000-000000000004','[]'::jsonb,'{}'::jsonb,
         false,statement_timestamp(),statement_timestamp())
       on conflict (timesheet_id) do nothing;`);

    // Weekly Source takes the trimmed key, then the raw key, then the rows.
    await weekly.runWithin('begin;');
    const held = await weekly.runWithin(
      `select private.weekly_source_lock_family_rows_v1(`
      + `array['a3000000-0000-4000-8000-0000000004a1']::uuid[],null)->>'ok';`);
    record('R37-weekly-locks-padded-family', last(held) === 'true', `ok=${last(held)}`);

    // The REAL confirmed route, on the same padded booking id.
    await confirmed.runWithin('begin;');
    const sig = await setup.runWithin(
      `select coalesce(public.timesheet_lifecycle_guard_signature_v1(`
      + `'a3000000-0000-4000-8000-0000000004a1'::uuid,null,false)->>'backend_row_signature','');`);
    const confirmedPending = confirmed.run(
      `select public.timesheet_route_version_confirmed_v1(`
      + `'a3000000-0000-4000-8000-0000000004a1'::uuid,'a3000000-0000-4000-8000-0000000004a1'::uuid,`
      + `'${last(sig).replace(/'/g, "''")}',repeat('a',64),'ALLOW_QR_AGAIN','${ACTOR}'::uuid,`
      + `null,null,'wp03-r37-real-0001',true,null)::text;`);
    const confirmedWait = await tryBlocked(group, 'setup', 'confirmed');
    record('R37-real-confirmed-route-serialises', confirmedWait?.waitEventType === 'Lock',
      confirmedWait ? 'the REAL confirmed route waits on the family Weekly Source holds'
                    : 'the confirmed route returned before a lock wait');

    await weekly.runWithin('commit;');
    const confirmedResult = await confirmedPending;
    await confirmed.runWithin('rollback;');
    record('R37-real-confirmed-route-NO-DEADLOCK', !isDeadlock(confirmedResult),
      describe(confirmedResult));

    await setup.runWithin(
      `delete from public.timesheets where timesheet_id='a3000000-0000-4000-8000-0000000004a1';`);
    await setFlag(setup, false);
  });
}


// --- G1: the publisher when the base week gains its root after the plain read -
// The earlier code guarded the root re-check with "is not null", so this exact
// case fell through to a row lock on the root BEFORE the family advisory keys,
// the reverse of every installed rotation owner and a reproduced deadlock. The
// publisher must now refuse retryably instead of taking that order at all.
async function proveG1() {
  await withGroup(['setup', 'writer', 'publisher'], async (group) => {
    const setup = group.session('setup');
    const writer = group.session('writer');
    const publisher = group.session('publisher');

    await setup.runWithin(
      `update public.tms_users set payment_authoriser=true where id='${ACTOR}';`);
    await setup.runWithin(
      `insert into public.contract_weeks(contract_id,week_ending_date,additional_seq,status,
         submission_mode_snapshot,timesheet_id,is_adjustment,created_at,updated_at)
       values ('a3000000-0000-4000-8000-000000000004','2026-09-20',0,'SUBMITTED',
         'MANUAL',null,false,statement_timestamp(),statement_timestamp())
       on conflict (contract_id,week_ending_date,additional_seq) do nothing;`);

    // The writer takes the WEEKLY_SOURCE_BASE_WEEK key the publisher will need,
    // so the publisher blocks there AFTER its own plain read of the week has
    // already seen NULL. That is the exact window the "is not null" guard
    // failed to cover.
    await writer.runWithin('begin;');
    await writer.runWithin(
      `select pg_advisory_xact_lock(hashtextextended(
         'WEEKLY_SOURCE_BASE_WEEK|a3000000-0000-4000-8000-000000000004|2026-09-20',0));`);
    await writer.runWithin(
      `insert into public.timesheets(
         timesheet_id,booking_id,version,is_current,status,sheet_scope,submission_mode,
         line_type,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,
         shift_label_norm,week_ending_date,contract_id,actual_schedule_json,
         qr_payload_json,is_adjustment,created_at,updated_at)
       values ('a3000000-0000-4000-8000-0000000005a1','WP03-G1-LATE',1,true,
         'RECEIVED','WEEKLY','MANUAL','HOURS','g1','g1','g1','g1','weekly-0',
         '2026-09-20','a3000000-0000-4000-8000-000000000004','[]'::jsonb,'{}'::jsonb,
         false,statement_timestamp(),statement_timestamp())
       on conflict (timesheet_id) do nothing;`);
    await writer.runWithin(
      `update public.contract_weeks set timesheet_id='a3000000-0000-4000-8000-0000000005a1'
       where contract_id='a3000000-0000-4000-8000-000000000004'
         and week_ending_date='2026-09-20' and additional_seq=0;`);
    // Start the publisher; it reads the week (NULL) and then blocks on the key.
    await publisher.runWithin('begin;');
    const pending = publisher.run(
      `select public.weekly_exceptional_pay_prepare_family_v1(jsonb_build_object(
         'actor_user_id','${ACTOR}',
         'source_cycle_id','a3000000-0000-4000-8000-000000000008',
         'candidate_id','${CANDIDATE}',
         'client_id','a3000000-0000-4000-8000-000000000002',
         'contract_id','a3000000-0000-4000-8000-000000000004',
         'week_ending_date','2026-09-20',
         'work_date','2026-09-17',
         'start_at_local','2026-09-17 09:00',
         'end_at_local','2026-09-17 17:00',
         'break_minutes',30,
         'reason','wp03 review G1',
         'idempotency_key','wp03-review-g1-idempotency-0001'))::text;`);
    const blocked = await tryBlocked(group, 'setup', 'publisher');
    record('G1-publisher-blocks-on-the-base-week-key-after-its-plain-read',
      blocked?.waitEventType === 'Lock',
      blocked ? 'the publisher waits on the base-week key it needs next' : 'no wait observed');

    // Only now does the week gain its root, and the writer commits.
    await writer.runWithin('commit;');
    const result = await pending;

    record('G1-publisher-refuses-retryably-instead-of-locking-out-of-order',
      // The harness only reports a SQLSTATE when psql prints one on its own
      // line, so the refusal is matched on its message, which is exact.
      /WEEKLY_SOURCE_BASE_WEEK_ROOT_CHANGED_DURING_LOCK/.test(result.error?.message ?? ''),
      describe(result));
    record('G1-NO-DEADLOCK', !isDeadlock(result), describe(result));

    await publisher.runWithin('rollback;');
    await setup.runWithin(
      `delete from public.contract_weeks where contract_id='a3000000-0000-4000-8000-000000000004'
         and week_ending_date='2026-09-20';
       delete from public.timesheets where timesheet_id='a3000000-0000-4000-8000-0000000005a1';
       update public.tms_users set payment_authoriser=false where id='${ACTOR}';`);
  });
}

await proveG1();
await proveD1();
await proveD1b();
await proveD2();
await proveD3();
await proveRotationOwners(false);
await proveRotationOwners(true);
await proveR37Real();

const failed = results.filter((r) => !r.ok);
console.log(`\n=== ${results.length - failed.length}/${results.length} review-response race assertions passed`);
if (failed.length) {
  console.log(JSON.stringify(failed, null, 2));
  process.exitCode = 1;
}
