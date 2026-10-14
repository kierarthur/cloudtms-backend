# Install 1 receipt/observer fix — independent source critic, 14 October 2026

**Scoped source PASS after forward correction of the turn-log/cooldown and PAUSE observer issues.** Initial findings and pins below are retained. This approves the receipt/observer source correction only, with no database connection, rehearsal, release, promotion or installation proof. Exact committed backup and new clean rehearsal remain root-owned gates.

Reviewed dirty forward source in `C:\Users\KierArthur\OneDrive - Arthur Rai\Documents\GitHub\.codex-worktrees\healthroster-install1-14102026`, based on `206dc3dbb50ca971af8faa26a5f4ed384570368b`. Exact initial saved file SHA-256 values:

| File | SHA-256 |
| --- | --- |
| `scripts/healthroster-install1-native-coordinator.mjs` | `1a9055406c60780177c876187f6daf2bf7c59e3142e524b4cb38305593160f75` |
| `scripts/healthroster-turn-control.mjs` | `f718c9b0bce53460e68fd5175b58d99177935ae052a1a31b46909d14ae1c4beb` |
| `scripts/healthroster-receipt-journal.mjs` | `3e9bb8a622c96f51a95906867de95988322ba6c16b36e0be1e77487196fa6f67` |
| `tests/healthroster-turn-control.test.mjs` | `794d6c0d0f7dfbc0f83c06c44f32171b7fcd446b6ad00f08a197815309fe01af` |
| `tests/healthroster-receipt-journal.test.mjs` | `f0ca3e6c167f7f6b700cff8448233f21552035b121a54b5ee88d3f7a893b8296` |

Read the workspace/lane instructions, complete changed scripts/tests, actual turn controller and full-rehearsal fresh-proof/child adapters. Owner SEND7, two-install batching, exact-commit source backup and every native verification gate remain controlling.

## P1-1 — failed turn-log archive loses the five-minute retry authority

The new coordinator sends turn records to a per-attempt `C:/tmp/.../SHARED_TURN.md`, at `healthroster-install1-native-coordinator.mjs:156`. It archives that log to OneDrive `progressPath` only in final cleanup, with an explicit `TURN_LOG_ARCHIVE_PENDING` catch.

The actual `withSharedRehearsalTurn` still obtains its last BUSY timestamp exclusively from `progressPath` (`healthroster-rehearsal-turn.mjs:78–95`). If the append archive fails—the filesystem condition this correction must survive—the BUSY_NO_TRIAL record remains only in the previous local attempt directory. A new invocation has no last-busy timestamp and can open another turn connection before SEND7's required five-minute retry interval.

Keep a stable, authoritative, small local last-busy record/journal consulted **before** protected checks/connections, independent of the delayed OneDrive archive. Alternatively fail closed while a busy-log archive is pending. A per-attempt log not consulted by later attempts does not preserve the cooldown. Preserve the unchanged shared key `(20261013,1)` and nonwaiting acquisition.

## P1-2 — synchronous log failure can escape the asynchronous loss listener

The coordinator injects another throwing `fs.appendFileSync` as `deps.log`. `withSharedRehearsalTurn` calls that callback directly from `lose()` (`healthroster-rehearsal-turn.mjs:101`), reached by the turn connection's AbortSignal `connectionLost` listener at line 102. There is no containment for that callback exception. AbortSignal listener exceptions can escape asynchronously rather than following the enclosing async work's rejection/cleanup path. Thus a failed live turn-log write can still crash the parent process despite the corrected stdout observer.

Contain log failures in the turn owner, keep only closed diagnostics, mark the attempt incomplete and preserve its primary failure. An owned rollback child must be stopped through its exact loss signal; a dispatched durable release must finish for reconciliation. Also prevent the final RELEASED log from masking an existing source/child/ownership failure. Do not cancel or inspect another session/process.

Both findings were sent promptly to root. They were corrected in saved forward source and independently re-reviewed as recorded below.

## Forward re-review disposition

Exact corrected saved hashes: coordinator `a7ea96ec88582aa1318eda69f69efe94a05819fc67dc1b665a91f04a1a405373`; turn-control `f718c9b0bce53460e68fd5175b58d99177935ae052a1a31b46909d14ae1c4beb`; rehearsal-turn `b7e16cdb12e8b5c6d11894e53296e4d20aeeaceb8dd968ed32936884f3ab3610`; receipt-journal `644035d62b09e97e5c8a219e93be4728a394b8a4d287d32a2f7c3c37a5edeb29`. Concurrent per-table diagnostic/full-rehearsal and archive-preflight changes are outside this receipt-fix source verdict.

Root added immutable records in stable local directory `C:/tmp/healthroster-install1-turn-journal-20261014`, read before a subsequent connection through the actual `readLastBusy` dependency. Existing progress records are also read. The local BUSY record is written before optional live/OneDrive logs. Malformed/unreadable authority fails before connection, rather than waiving cooldown. **P1-1 closed in source.**

The actual turn owner wraps its logging callback with a closed `TURN_JOURNAL_WRITE_FAILED` error and aborts owned work. The loss listener contains logging failure; final RELEASED logging retains the original primary failure and still requires explicit unlock/connection closure. **P1-2 closed in source.**

Additional PAUSE callback finding: the original stdin listener saved a receipt before reaching owned turn close, so a journal-write failure could escape the event listener and prevent the explicit pause from reaching close. Root now uses the contained restoration-tolerant save and guarantees exact `turnConnection.close()` in `finally`, with its async failure held in metadata. **PAUSE finding closed in source.**

`recordReceipt` and `freshAfter` now tolerate journal persistence failure long enough to run the required fresh restoration proof. Any failure sets `receiptPersistenceFailed`; actual publication refuses that flag. Error collection/restoration is never replaced by a receipt-write success.

Independently reran inspected mock-only journal/turn tests **26/26 pass**, plus exact observer/durable-child/broken-stdin tests **4/4 pass**. No local-pipe, native child, secret reader or database test ran. Updated coordinator/turn syntax checks pass. Root's reported wider 39-test result is separate creator evidence.

Root reports interrupted predecessor rehearsal restoration had equal ledger/catalogue/relation census, absent marker and LEGACY:1, but different business aggregate fingerprints. This critic made no fresh database call and does not confirm those historical observations. The changed aggregate remains **HOLD** for installation; per-table diagnostics may locate it but must not waive or replace the aggregate comparison.

## Correction that is sound in the reviewed source

The receipt journal writes unique `wx` generations in a fresh local directory and returns each path only after the write succeeds. Progress markers are small separate generations; they no longer rewrite the full reader-visible receipt from every stdout event. The final OneDrive archive happens after `withSharedRehearsalTurn` returns/cleans up, with archive failures held as pending. Failed journal writes expose only a closed diagnostic.

`runOwnedTurnChild` contains stdout/stderr observer exceptions. For rollback work it signals only the exact spawned child and escalates on that same child after five seconds; it waits for close and rejects instead of producing a success receipt. With `cancelOnLoss:false`, a durable child is retained to completion, then the observer error holds reconciliation. A healthy child still has no harness deadline and is checked under the live turn before and after execution. Existing broken-stdin handling remains fail closed.

The correction does not replace fresh TEST identity, managed-release vacancy, exact pushed source/ancestry/contract/source-binding, Banking completion, original before/after ledger/catalogue/business fingerprints, explicit rollback marker, relation census, LEGACY:1, fresh connection or expected contract gates. Observer failure is an incomplete rehearsal; it cannot permit publication. If a critical full receipt write itself fails before the fresh-after callback, the coordinator remains held and root must collect the required fresh restoration proof separately; a pending local artifact is never restoration evidence.

## Effect-free verification and limits

Ran only inspected pure tests with mock writes/children: receipt-journal tests **2/2 pass**; targeted throwing stdout/stderr observer, retained durable child and broken-stdin tests **4/4 pass**. JavaScript syntax checks for all three changed modules pass. The selected tests do not start a real child or database connection; unselected local-pipe/other tests were not run. No CLI entry point, unknown harness, secret reader, native callback or source generator was executed.

Only this separately requested critic artifact was written. All source/dirty files and user changes were preserved. No database/lab/connector/secret/browser/Docker/network/Git mutation, TEST/LIVE release/deploy/promotion, payment/provider/settlement/bank/email/drain or phone action occurred. Money/OFF review remains independently held pending its own frozen forward packet.
