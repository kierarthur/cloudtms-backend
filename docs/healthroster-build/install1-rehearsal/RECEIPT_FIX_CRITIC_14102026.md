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

## Additional per-table diagnostics at exact commit 37ddadc0

**Narrow diagnostic source PASS**, at committed source `37ddadc0a50721834c9e8a6c06becfc983f0dc32`. The clean local branch and existing remote-tracking ref both resolve to that exact commit; no network/ref mutation was performed by this critic. Read the committed full-rehearsal/coordinator blobs and exact diff, committed interrupted report/JSON, and the actual fresh-restoration guards.

The full-rehearsal diff is one additive `businessRelations` JSON property. It returns only actual relation name, row count and the already-computed per-table row fingerprint from the **same** `business` CTE used by the retained aggregate counts/rows hashes. Raw rows remain inside PostgreSQL. It does not alter the relation census, table selection, hashing algorithm, catalogue/ledger queries, exact marker handling, activity exemption or any restoration assertion.

The coordinator's `freshAfter` compares those actual before/after per-table metadata entries and saves `businessChanges` containing only relation, before/after count and before/after hash. This summary does not determine PASS, select an exclusion, attribute a cause, erase a changed table or replace the aggregate proof. A removed/added relation remains subject to the existing complete relation-census assertion. Missing legacy per-table diagnostics cannot explain an old aggregate mismatch; the helper does not pretend otherwise.

Committed `REHEARSAL_14102026_0101.md` explicitly calls candidate `206dc3dbb50ca971af8faa26a5f4ed384570368b` incomplete, records unit 112 of 145 before interruption, and refuses to infer complete diagnostics, explicit rollback or final contract proof. `INTERRUPTED_RESTORATION_14102026.json` remains `RESTORATION_HELD` with `REHEARSAL_RESTORATION_MISMATCH`. Its business counts/rows aggregates differ; the committed report says their origin is unknown. The archived partial `INTERRUPTED_FULL_REHEARSAL_14102026.json` remains a partial running receipt with `publication:null`, not a retrospective clean result. **That attempt cannot qualify an install, and its old drift remains unproved.**

A genuinely new complete rehearsal can qualify **its own exact pushed commit against its own freshly proved current TEST before state** independently, if every existing source/target/contract/Banking/turn/activity gate, complete SQL/error/explicit rollback/final contract requirement and fresh-after relation/ledger/catalogue/business aggregate equality passes unchanged. This would prove the new attempt's bounded restoration and release readiness. It would not close, explain or retroactively certify the interrupted attempt, and must not be presented as root-cause attribution or historical no-write proof. Preserve the old held artifacts and the three-round/two-install limits; any new mismatch still holds publication.

Seven inspected effect-free native-gate/restoration/snapshot/protocol tests pass, including deliberate aggregate mismatch and missing rollback refusal. No native SQL, database connection, secret read or real child ran. No new source defect was found in this additive diagnostic change; actual PostgreSQL execution of the new field remains root-owned and unproved by this critic.

## Psql line-diagnostic adapter forward after held attempt 2

**Narrow diagnostic adapter source PASS.** Reviewed the saved/staged forward over exact `37ddadc0a50721834c9e8a6c06becfc983f0dc32`, without a database or native child. The changed adapter is `scripts/healthroster-install1-full-rehearsal.mjs`, imported by the native coordinator. Saved SHA-256 is `9066f35729cdea6b63198bf5a758ce7c6e3f4e983607da113577479c1ecebf88`; its changed test file hashes to `1f3120bfc7352ff0ab1867bc5be4da854970da51be35d89cea10ceb493a60840`.

`executePsqlWithinOwnedTurn()` now passes the exact arguments `['-X','-q','-A','-t','-f','-']` while retaining stdin SQL, process-scoped libpq environment, the owned rollback child/loss behavior, existing statement/collector settings and no harness deadline. It does not add a connection route, shell transport, SQL transformation or error-control override. The existing sealed line map and closed literal-source diagnostic codes remain authoritative.

Any ERROR/FATAL that cannot be mapped sets `unexpectedError=true`, produces only a five-character SQLSTATE or NULL plus `DATABASE_DIAGNOSTIC_LOCATION_UNPROVED`, and keeps `errorCollectionComplete=false`. Raw message/detail/context/query/business/credential bytes are not returned or persisted. The SQLSTATE fallback does not imply source location or a complete error count. Even complete unit markers, exit code zero and explicit rollback cannot override incomplete diagnostics; the full runner still requires complete collection and matching installed contract before publication, and still demands unchanged fresh restoration fingerprints.

Independently ran the inspected **10 effect-free protocol tests** and **5 targeted adapter/restoration/held-publication tests**, all PASS with no failure/skip/cancellation. They exercise the actual required argument array, redaction, missing marker/unmapped error refusal, explicit rollback failure and aggregate mismatch. All session/child transports are mocks; no native executable or database connection ran. Root's wider 27-test report remains separate creator evidence.

Read saved `REHEARSAL_14102026_0127.md`, `FULL_ATTEMPT2_HELD_14102026.json` and `NATIVE_LINE_DIAGNOSTIC_PROBE_14102026.json`. The full attempt remains **HELD_NOT_COMPLETE**, `publication:null`, `errorCollectionComplete=false`, `UNMAPPED_DATABASE_ERROR`, and `REHEARSAL_RESTORATION_MISMATCH`. An empty mapped-errors array is not zero real errors. All 145 markers, matching contract and explicit rollback do not close the diagnostic gap. Its two measured business-table changes remain held, with no cause attribution or permitted exclusion inferred by this critic. Earlier interrupted evidence remains held separately.

The root-supplied short read-only probe artifact has SHA-256 `8172b18e6b3b6d51d887b847f3d63deb71e3d7874f44ce2941ee8c2185557c5d`: it records mapped line 7 / SELECT_DIVISION_BY_ZERO / SQLSTATE 22012 and explicit rollback under its own separate turn. It is explicitly **READ_ONLY_DIAGNOSTIC_PASS_NOT_REHEARSAL** with no installed contract and `REHEARSAL_CONTRACT_ENVELOPE_INCOMPLETE`; the report records unlock/close. This critic checked the retained source/metadata only and did not repeat or independently authenticate that native probe. It cannot qualify either full attempt or an install. Full held attempt 2 JSON SHA-256 is `17ab5628481aac56b785d3d3110b1ce6f73446f30c88184b45da1d447befe09c`.

The next full attempt still requires exact reviewed/pushed source, fresh target and quiet-state proof, the separate owned shared turn, every unchanged original assertion, complete closed diagnostics, explicit rollback, final canonical contract and equal fresh aggregate restoration. Preserve the three-round and two-install limits. No schedule/data/runtime change, waiver, root-cause conclusion or native signoff is supplied by this source verdict.

### Additional lowercase psql client diagnostic boundary

**Source PASS retained after the additional saved/staged correction.** `closedRehearsalDiagnostic()` now captures lowercase psql `error`/`fatal` line prefixes with sealed source location where available, fixed `PSQL_CLIENT_ERROR` and NULL SQLSTATE. It retains no arbitrary client diagnostic text. The adapter's fallback also latches unmapped lowercase errors; an unmapped client error cannot disappear into a complete/clean result. A mapped client error remains in the nonempty errors array, so complete markers/exit zero/explicit rollback cannot qualify the full runner. No native assertion or restoration rule changes.

Latest saved hashes superseding the earlier staged diagnostic hashes: full-rehearsal adapter `0da03974515c9a7f8f6c8bb2b7d9ac0110e4d0e4cc109ac4f950d3a8c3e21db8`; protocol `8acacff2acd8fdc5149af90fb1b479d265d522e4a1ec0b9da29a863090421df6`; adapter tests `81eeff5972df525f8cc7deaebe0d15504470e9de8b6f4102c20248af392cf042`. Independently reran **10 protocol plus 6 targeted adapter/restoration/client tests, all PASS**, zero failure/skip/cancellation. Root's reported 28-test suite is separate creator evidence. All reviewer transports remain mock-only.

Read the now-saved `NATIVE_CLIENT_DIAGNOSTIC_PROBE_14102026.json`, SHA-256 `60f12f889d09e4323cc8a09347afcf69e3eeae9907fc1c5acf18905faed1512e`. It records the root's separate own-turn read-only probe at `2026-10-14T02:13:41.559Z`: line 7/SQLSTATE 22012 and line 8/fixed PSQL_CLIENT_ERROR, explicit rollback, and complete diagnostic collection. The artifact still says READ_ONLY_DIAGNOSTIC_PASS_NOT_REHEARSAL, has no installed contract and retains REHEARSAL_CONTRACT_ENVELOPE_INCOMPLETE. Complete collection of two deliberate errors is not a clean SQL pass. Root reports explicit unlock/close; this critic did not repeat or independently authenticate the native probe. Both full attempts and their aggregate drift remain held without attribution or waiver.
