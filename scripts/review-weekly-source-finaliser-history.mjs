import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import path from 'node:path';

// Read-only Git/object census. Only the already-curated release metadata is
// accepted; this script neither queries a database nor discovers credentials.
const input = process.argv[2];
assert.ok(input, 'Pass the curated F2_COMPLETE_SOURCE_ERA_RELEASES.json');
const releases = JSON.parse(readFileSync(input, 'utf8')).rows;
assert.equal(releases.length, 109);
const sourcePath = 'supabase/repeatable/15092026_1534_weekly_source_finalisation_v1.sql';
const name = 'create or replace function private.weekly_source_finalise_engine_v1(';
const sha = text => createHash('sha256').update(text).digest('hex');
const git = args => execFileSync('git', args, { encoding: 'utf8', maxBuffer: 32 * 1024 * 1024, windowsHide: true }).replaceAll('\r\n', '\n');
const versions = new Map();
const rows = [];
for (const release of releases) {
  const source = git(['show', `${release.git_commit}:${sourcePath}`]);
  const start = source.indexOf(name), end = source.indexOf('$function$;', start);
  assert.ok(start >= 0 && end > start, `Complete engine missing: ${release.git_commit}`);
  const engine = source.slice(start, end);
  const engineSha256 = sha(engine);
  if (!versions.has(engineSha256)) {
    const snapshotInsert = engine.indexOf('insert into public.weekly_source_final_snapshot_lines(');
    const transitions = engine.indexOf('for v_transition in');
    const guard = engine.indexOf("if (v_effective_policy->>'authority_mode') is distinct from 'SOURCE_AUTHORITY'", transitions);
    const refusal = engine.indexOf("raise exception 'WEEKLY_SOURCE_FINALISATION_POLICY_STALE'", guard);
    const movement = engine.indexOf('insert into public.weekly_source_state_transitions(', guard);
    const prepared = engine.indexOf("'ok',true,'status','PREPARED'", movement);
    assert.ok(snapshotInsert > 0 && snapshotInsert < transitions && transitions < guard && guard < refusal && refusal < movement && movement < prepared);
    const policyGuard = engine.slice(guard, refusal);
    assert.ok(policyGuard.includes("coalesce((v_effective_policy->>'self_bill_enabled')::boolean,false) is not true"));
    assert.ok(policyGuard.includes("(v_effective_policy->>'source_group_id')::uuid is distinct from v_group.id"));
    assert.ok(engine.slice(movement, prepared).includes('end loop;'));
    assert.match(engine.slice(engine.indexOf('\nexception\n')), /raise exception 'WEEKLY_SOURCE_FINALISE_VALUE_INVALID'/);
    assert.match(engine, /case when p_prepare_only then 'PREPARED' else 'CURRENT' end/);
    const returns = [...engine.matchAll(/^\s*return\b/gm)].map(match => match.index);
    assert.equal(returns.length, 3);
    assert.ok(returns[0] < snapshotInsert && returns[1] > movement && returns[2] > returns[1]);
    versions.set(engineSha256, { sourceCommit: release.git_commit, sourceSha256: sha(source), engineSha256,
      snapshotInsertBeforeGuard: true, everyTransitionGuardedBeforeMovement: true,
      earlyReplayCreatesNoSnapshot: true, prepareReturnAfterTransitionLoop: true,
      guardedFailureRaisesAndRollsBackFunction: true });
  }
  const references = git(['grep', '-n', '-i', '-e', 'weekly_source_finalise_engine_v1', release.git_commit, '--', ':(glob)supabase/**/*.sql']);
  const sourceReferences = references.split('\n').filter(line => /:supabase\/(migrations|repeatable)\//.test(line));
  const paths = [...new Set(sourceReferences.map(line => line.split(':')[1]))].sort();
  assert.deepEqual(paths, ['supabase/repeatable/15092026_1534_weekly_source_correct_final_source_v1.sql', sourcePath].sort());
  const definitions = sourceReferences.filter(line => /create (or replace )?function private\.weekly_source_finalise_engine_v1\(/i.test(line));
  assert.equal(definitions.length, 1);
  const writes = git(['grep', '-n', '-i', '-e', 'insert into public.weekly_source_final_snapshot_lines', release.git_commit, '--', ':(glob)supabase/**/*.sql'])
    .split('\n').filter(line => /:supabase\/(migrations|repeatable)\//.test(line));
  assert.equal(writes.length, 1);
  assert.ok(writes[0].includes(`:${sourcePath}:`));
  rows.push({ release_id: release.release_id, git_commit: release.git_commit, status: release.status,
    engineSha256, engineDeclarationCount: definitions.length, snapshotWriterCount: writes.length,
    sourceReferencePaths: paths });
}
assert.equal(versions.size, 5);
assert.deepEqual([...versions.keys()], [
  '23af80196d5b33bf0970535acedfe2ee3049a85ffea86ff014e00e2176abf324',
  'db4c5a1c4c3394760bbc0110d1ade15b734b168c1006b09e2b33b41a704a2a7f',
  '02bbe0da283e4ab18bc4416fe5d629a9eb8a2a98b199fe78da916347abd2c60c',
  '7cf00f3e160c7c171bd895d7dd46ec4ba26a4955f2d1fafe544e1009ab086196',
  'b8f625660b1fd8894bdd496cdd4c390b04f2a5f4f9936129413737acc877a9b3'
], 'A different recorded engine needs fresh complete control-flow review');
const report = { scope: '109 recorded source-era release attempts, including FAILED attempts; Git source/control-flow review plus separately attributed integrator binding',
  releaseCount: rows.length, versions: [...versions.values()], rows,
  approvedControlledHistoryGate: {
    approval: 'Integrator froze the exact gate on 13 October 2026 after independent source/control-flow census and fresh TEST receipts.',
    installedEngineBodyMd5: '7821cf16ae3c98aaa212325228e147a2',
    originalFinClosureSha256: '240e2460baf9bff3164cc4ed389b6acb5774f8f88ab0deefc70d6f085fae00f1',
    schemaContentSha256: '1fc7833c7054cb56da725d15280ff757f3b78e1046b80f12c337020695374c0d',
    f2MigrationPath: 'supabase/migrations/13102026_1210_weekly_source_final_value_snapshot_authority.sql',
    f2MigrationContentSha256: '0d0564eea33cff211efe8452d3c7d4cf0817aeb21854a967a4f7ad402966a7d6',
    f2Cutover: 'The exact F2 migration ledger applied_at_utc; only earlier canonical snapshot/revision origins derive.',
    parentReceipts: ['HEALTHROSTER_BUILD_13102026/F2_INSTALLED_FINALISER_BINDING.json', 'HEALTHROSTER_BUILD_13102026/F2_SNAPSHOT_IMMUTABILITY_RECEIPT.json'],
    snapshotWrites: 'anon/authenticated/service_role denied INSERT/UPDATE/DELETE/TRUNCATE; immutable UPDATE/DELETE/TRUNCATE triggers enabled in parent receipt.',
    proofClass: 'Controlled recorded-source construction; does not assert theoretical absence of an unrecorded administrator write.'
  },
  limitations: ['This lane made no database call or installation. A changed original FIN closure requires fresh source/binding review before changing the allowlist. Installed HC-2 and joined business acceptance remain integrator gates.'] };
const output = path.resolve('docs/healthroster-build/F2_HISTORICAL_SOURCE_REVIEW.json');
mkdirSync(path.dirname(output), { recursive: true });
writeFileSync(output, `${JSON.stringify(report, null, 2)}\n`);
console.log(JSON.stringify({ releaseCount: rows.length, versions: report.versions, output }));
