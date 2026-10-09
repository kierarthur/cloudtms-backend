# F-09 normal source-gate scope

Read-only investigation, 9 October 2026. Re-read the owner start sheet section 3, F-09, B11 and F09-D01–D17. Inspected the exact 16 proposed RESET test sources and their imports, transports and saved-file reads. No test, Worker, SQL, database, Docker, mock caller or hosted operation was executed; no product/test source was changed.

## Authority and decision

The owner start sheet section 3 permits the normal source gates, changed-JS `node --check`, local changed-SQL loading, `db:check`, sealing and coupling. It excludes data fixtures, payment/scenario rehearsals, rollback labs, timing/performance tests except the phase-3 exception, new harnesses and repeated method rounds. F-09 expressly permits unit tests run by the normal repository source gate; B11 expressly admits `tests/*.test.js` unit regressions while excluding the `tests/run-bpay-next-*` proof runners and joined/native vectors.

The existing package gate is `npm run candidate:bindings:check && node --test tests/*.test.js`. All 16 proposed tests are in-process unit/source checks admitted by that gate. Each uses only Node test/assert/crypto/source-file readers and the selected Worker modules. No proposed test imports a fixture or runner, starts a server/process/database, loads SQL, reads environment secrets, calls an external transport or drains a real queue. The supplied `fetchImpl` functions return controlled Responses or count/refuse calls; callbacks named send/receive/RPC are in-memory assertion dependencies. `Request`, `Response`, streams, AbortController and small test timers exercise caller bounds/timeout handling, not a deployed Worker or performance programme. The unsigned service-JWT strings are explicitly synthetic shapes.

That classification is based on the saved implementation, not the word "runtime" in a filename. The runtime-RPC and expiry-RPC tests validate fixed metadata transports and refuse financial/actor/clock data; their loopback/invalid example URLs are intercepted by mocks. Conversely, an actual Wrangler/local queue/database/provider scenario would remain excluded even if named `.test.js`.

## Exact 16 checks

Every row below is **ALLOWED NORMAL SOURCE UNIT**, subject to preserving its in-process dependencies.

| Test under `tests/` | Checked source behaviour | Additional saved-source dependency / sequencing |
| --- | --- | --- |
| `bpay-next-background.test.js` | Enrollment/expiry/CLAIM disposition, awaited durable publication, four bounded recovery lanes | Selected background/runtime/expiry/financial-queue module set only |
| `bpay-next-command-rpc.test.js` | Fixed intake/bind endpoint, authenticated actor, exact body/signal, no retry | One `index.js` command-route assertion belongs with F-10 wiring |
| `bpay-next-command.test.js` | Closed actions, exact money/IDs/receipts, auth, bounded bytes, unknown outcomes | Reads `04102026_0270` intake-service SQL |
| `bpay-next-financial-queue.test.js` | One bounded owner step, exact nonce/epoch/cursor retry, durable publication before ack, finite phases | Reads its own Worker source only; queue methods are mocks |
| `bpay-next-frozen-advice.test.js` | Escaped streamed immutable advice, bounded constituent pages/chunks, exact values and detail | Reads `04102026_0255` advice-page SQL; the 300 rendered in-memory rows are stream unit operands, not the owner's 300-Timesheet timing exception |
| `bpay-next-frozen-read.test.js` | Typed frozen pages, exact tuple cursors/money/timestamps, auth, stream bounds | Module only |
| `bpay-next-frozen-rpc.test.js` | Fixed read endpoint, actor/signal, no arbitrary function or retry | One `index.js` frozen-detail-route assertion belongs with F-10 wiring |
| `bpay-next-job-rpc.test.js` | Fixed CLAIM/STEP wrapper, typed kind, exact original body and signal | Module only |
| `bpay-next-job-step.test.js` | Owner-authoritative claims/checkpoints/replay, exact recovery, lossless values and explicit Candidate issues | Reads `04102026_0280` job-service SQL and its own module |
| `bpay-next-legacy-email.test.js` | Exact metadata classification and JS/SQL predicate parity; no mail delivery | Reads `04102026_0504`, `04102026_0558`, retained `23072026_2207`, catalogue `07092026_2300` SQL |
| `bpay-next-legacy-fence.test.js` | Fresh finite owner/epoch status and fail-closed callback fence, bounded fixed transport | Reads `04102026_0550` fence SQL; permitted callback is an assertion, not a provider/mail action |
| `bpay-next-payment-history-caller.test.js` | Frozen 52-section registry, masked bounded exact cursor/amounts, actor/error/privacy boundary | Reads `06102026_1206` history SQL and `index.js`; history-route assertion belongs with F-10 wiring |
| `bpay-next-preparation-expiry-rpc.test.js` | Fixed due/receive endpoints, six-digit UTC tuple, closed inputs/receipts, bounded raw stream and uncertainty | Reads its own module; no actual expiry receive |
| `bpay-next-preparation-rpc.test.js` | Fixed intake/page transports, typed actions/kinds, actor/signal, no retry | Module only |
| `bpay-next-preparation.test.js` | Revision-bound work/component/case selection, factual current pages/status, exact confirmation and bounds | Reads `04102026_1241`, `04102026_0570`, `04102026_0575` SQL |
| `bpay-next-runtime-rpc.test.js` | Fixed enrollment/wake/expiry metadata RPCs, exact bounded pages, no caller financial/lease authority, unknown outcomes | Reads its own module; no actual enrollment/publication |

## Safe inclusion and acceptance boundary

The 17 module port is F-09; `index.js` wiring remains F-10–F-14. Three of the 16 complete test files contain route assertions that cannot pass before F-10: command-RPC, frozen-RPC and payment-history-caller. Preserve those assertions. Port/run those complete files with their wiring owner; do not weaken their assertions or wire `index.js` prematurely just to make an F-09 gate pass. The other 13 have no later HTTP-wiring dependency. All listed SQL files currently exist in BUILD-BE; no fixture or obsolete migration needs importing to make these source reads resolve.

Several source assertions read historical repeatable files that now have final successor closures, including the F-05 pending-successor file. Passing those assertions does not establish final database definitions. F-04 final-definer/source gates, changed-SQL compile, seals and coupling remain responsible for that authority. Any source-path evolution must follow the relevant card and actual final contract; the owner's instruction remains to fix code when a gate fails, not dilute an expectation.

No new harness, proof programme, test data set or extra testing round is recommended. Root may include applicable existing unit regressions in the normal gate and record their actual result. This note supplies scope classification only, with no PASS claim for tests or app/runtime behaviour.

Sources: owner `BUILDER_START_SHEET_BPAY_NEXT_R3.md` sections 3–5; plan `01_BUILDER_RULES_AND_FOUNDATION.md` B11/F-09; BUILD-BE `package.json`; RESET exact 16 saved test files; `F-09-worker-port-inventory.md` exact selection.
