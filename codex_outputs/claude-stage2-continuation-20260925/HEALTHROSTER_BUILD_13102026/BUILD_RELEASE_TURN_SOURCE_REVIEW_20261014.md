# Shared HealthRoster rehearsal and managed-release turn — source review

Base: pushed `171778d7214c2dad98ce392981fecd2f47db20b6`. Isolated branch: `codex/healthroster-build-release-turn-20261014`; local repository: `.codex-worktrees/healthroster-build-release-turn-20261014`. Whole-file identities are frozen in the adjacent `BUILD_RELEASE_TURN_FROZEN_MANIFEST_20261014.json`.

This is source-only glue for Install1 and Install2. It does not claim a native turn, rehearsal, release, contract seal, installed helper, deployment or acceptance. Banking's successful protected run is insufficient while its completed install/explicit shared-turn release remains unrecorded. No database, lab, probe, Docker, deployment, normal `test`/Office `main` promotion, credential read or Banking message was performed in this lane.

Read SEND5, SEND5B, SEND7, current release documents, the existing turn/CLI/renderer/coordinator and final `INSTALL1_SOURCE_READINESS_REVIEW.md`, whose saved whole-file SHA-256 was verified as `8be87ec1dacdd55a059139459ef02ecbfc6451cd807600e88f4e1967d94f5718`. The owner now permits at most two installations and requires one exact full clean rollback rehearsal immediately before each. Root owns that candidate composition and its native gates.

## Implemented source behavior

- The separate session takes only `(20261013,1)`, retains the original identity/module/release-start gate, then proves its actual `pg_backend_pid()`, physical database name/OID, TEST identity, LEGACY:1 and the exact granted `pg_locks` row including `objsubid=1`. Repeated probes require the same PID/OID. No broad application-name or supplied-PID exemption exists: the supplied activity predicate excludes only that proved same-database PID and the activity query's own `pg_backend_pid()`.
- An idle retained session probes every five seconds. Connection close/error and failed ownership latch an AbortSignal, write `LOST_INCOMPLETE`, and refuse later work/publication. Only the small probe has a finite ten-second liveness bound; full SQL has no automatic wall-clock stop. The exact local child created by `runOwnedChild` receives loss cancellation. No remote session/workflow/process lookup, `pg_cancel_backend`, `pg_terminate_backend`, process-tree kill or GitHub cancel is used.
- `work(turn)` owns before/full SQL/ROLLBACK/fresh restoration proof and managed release in one callback. `assertHeld(phase)` guards boundaries, `signal` is live, and `runOwnedChild({file,args,input,options})` supports streamed psql stdin. Root must supply the reviewed exact SQL, diagnostics and native restoration assertions; this lane does not replace that executor.
- `runManagedRelease` runs the canonical `npm run release:test`, using the absolute npm CLI through Node. A random process-local capability and local named pipe carry the live turn identity, phase requests and loss to the child. The capability/pipe never enter receipts; a saved identity JSON cannot substitute. The optional `beforePhase(phase,identity)` hook lets root re-read Banking completion, exact shared source and protected status immediately before `protected-dispatch`.
- The managed coordinator now runs Git and gh asynchronously. It checks the live turn around commands/waits and before every push, preserves exact clean `test`/repository/build-proof gates, rechecks proof freshness/source and refuses another active protected workflow immediately before dispatch. The exact protected AUTO route and database → normal backend → real private → synthetic private → broker → Office order remain.
- A dispatched run is pinned by exact ID/source. Loss retains `STOPPED`, the run ID where obtained (or dispatch time/source when uncertain), `runtimeOfficePublicationHeld`, and an explicit reconciliation record: remote stop is unproved, duplicate dispatch is disallowed. A later command refuses to overwrite an interrupted receipt or dispatch again until the operator reconciles that exact own run/installed state. No remote cancellation is attempted.
- Explicit false unlock cannot produce a clean result. Original work/source errors remain primary when cleanup also fails; the RELEASED record retains the actual unlock/close/proved booleans. A loss never returns a proved callback result.

## Pure checks

`node --test tests/healthroster-rehearsal-turn.test.mjs tests/healthroster-turn-control.test.mjs tests/request-test-release-turn.test.mjs tests/healthroster-full-rehearsal-protocol.test.mjs tests/automatic-test-release-policy.test.mjs`: 50/50 passed, zero failed/skipped at this review. Includes original false-unlock, busy/five-minute cooldown, identity/refusal and full renderer tests; exact PID/OID, loss at every callback phase, periodic actual lock loss, live local-pipe capability/loss, only-own-child cancellation, no healthy-child deadline, responsive long gh read, source/foreign-run guard, exact run retention/no redispatch, database failure and publication order. Fake connections/children and a local pipe only; no PostgreSQL/server calls.

`git diff --check` passed. Native connectivity, actual hosted lock ownership, liveness, rollback restoration and compatible protected publication remain unproved until root runs the authorised recipe after Banking releases TEST.

## Native integration recipe (not executed here)

After the completed Banking install and explicit shared-turn release are recorded, root freezes the exact pushed candidate/current shared `test`, canonical plan, verifier-effect review/exclusions, outer isolation, marker and sealed fingerprints. The existing renderer safety gates remain mandatory. Root calls the helper from the reviewed candidate:

```js
await withSharedRehearsalTurn({
  task: 'HealthRoster Install1 exact rehearsal and managed release',
  progressPath,
  async beforePhase(phase, identity) {
    if (phase === 'protected-dispatch') await rootRecheckBankingSourceAndReleaseGates(identity);
  },
  async work(turn) {
    await turn.assertHeld('before-fingerprint');
    const before = await rootFreshBeforeProof(turn.identity, turn.activityExclusionSql);
    const trial = await turn.runOwnedChild({
      file: approvedPsqlPath, args: ['-X', '-q', '-v', 'VERBOSITY=sqlstate'],
      input: exactReviewedBeginAllErrorsMarkerSqlRollback,
      options: {env: approvedProcessLocalTestEnvironment, stdio: ['pipe','pipe','pipe']}
    });
    await turn.assertHeld('after-rollback');
    // Require explicit ROLLBACK, zero diagnostic errors and fresh unchanged
    // ledger/catalogue/business fingerprints, marker absent and LEGACY:1.
    await rootRequireCleanRehearsalAndFreshRestoration(before, trial, turn);
    // Release any distinct rehearsal admission gate before remote APPLY;
    // retain SEND7 throughout. Promote only the exact proved candidate using
    // root's reviewed non-forced source workflow and fresh remote evidence.
    await rootPromoteExactProvedCandidate(turn);
    const release = await turn.runManagedRelease({
      npmCli: absoluteNpmCliPath, connectionsProof: freshAgentCreatedProofPath,
      officePath: reviewedCleanOfficeWorktree, cwd: reviewedBackendWorktree,
      env: approvedProcessEnvironment
    });
    if (release.code !== 0) throw Error('MANAGED_RELEASE_REQUIRES_RECONCILIATION');
    await turn.assertHeld('native-readback');
    await rootRequireExactInstalledRuntimeAndOfficeReadback(turn);
    return {status:'NATIVE_REHEARSAL_AND_MANAGED_RELEASE_PROVED'};
  }
});
```

Names prefixed `root` in this recipe denote root's required executor/gates, not functions provided by this module. The usable APIs are `withSharedRehearsalTurn`, its context, the async canonical coordinator and its live pipe. Full SQL is supplied by root, uses statement_timeout=0/jit off/collector off, one outer transaction and no harness deadline. If the turn is lost, preserve the incomplete native receipt and reconcile only the recorded own protected run/current installed state; never cancel somebody else or infer remote termination. Repeat this lifecycle for Install2 after its separately clean exact rehearsal, within the owner maximum of two installs and three fix rounds per candidate. Features remain off for Install1.
