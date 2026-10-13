# F-09 Worker module source review

**PASS for the F-09 source port (17 deliverables).** Independent review, 9 October 2026. Re-read F-09 and F09-D01–D17, the owner start sheet section 3 and B11. Read all 17 saved BUILD-BE modules in full, checked raw bytes against RESET and the root provenance receipt, and inspected the closed contracts, import graph, recovery/lease containment, streams and queue dispositions. No product/test/SQL edits, JavaScript or mocked caller execution, database calls, fixtures, runtime, Docker or hosted actions were performed. This is source review, not installation/deployment/app acceptance.

## Exact port and upstream safety

All 17 selected paths exist. Every current raw SHA256 equals both the saved RESET file and `F-09-module-provenance.json`; the full hashes are in that receipt and `design/F-09-worker-port-inventory.md`. No byte divergence was found. The 15 absent modules were copied and the two older dead modules, command/preparation, were replaced by the reviewed RESET bodies. A fresh read-only Git comparison of build base `a6f7423eb71e6e7cd173e705c2290e5408a1d077` to cached `origin/test` returned no difference for those two paths; their path history ends at `420513c6`. This check confirms the cached upstream source boundary, not a new remote fetch.

| Deliverable | `broker/src/` module | Independent result |
| --- | --- | --- |
| F-09-D01 | `bpay-next-background.js` | PASS |
| F-09-D02 | `bpay-next-command.js` | PASS |
| F-09-D03 | `bpay-next-command-rpc.js` | PASS |
| F-09-D04 | `bpay-next-financial-queue.js` | PASS |
| F-09-D05 | `bpay-next-frozen-advice.js` | PASS, unwired wording follow-up below |
| F-09-D06 | `bpay-next-frozen-advice-rpc.js` | PASS |
| F-09-D07 | `bpay-next-frozen-read.js` | PASS |
| F-09-D08 | `bpay-next-frozen-rpc.js` | PASS |
| F-09-D09 | `bpay-next-history.js` | PASS |
| F-09-D10 | `bpay-next-job-rpc.js` | PASS |
| F-09-D11 | `bpay-next-job-step.js` | PASS |
| F-09-D12 | `bpay-next-legacy-email.js` | PASS |
| F-09-D13 | `bpay-next-legacy-fence.js` | PASS |
| F-09-D14 | `bpay-next-preparation.js` | PASS |
| F-09-D15 | `bpay-next-preparation-expiry-rpc.js` | PASS |
| F-09-D16 | `bpay-next-preparation-rpc.js` | PASS |
| F-09-D17 | `bpay-next-runtime-rpc.js` | PASS |

The six static relative imports all resolve inside this selected set: background → runtime-RPC/expiry-RPC/financial-queue; financial-queue → job-step; frozen-advice → frozen-read; preparation-RPC → preparation. Background line 11 retains the required static expiry-RPC import. No F-09 `index.js` or Wrangler wiring/config difference is present. Root reports syntax/import checks PASS in the provenance receipt; the reviewer did not execute them.

## Preserved source contracts

- `job-step.js:54` contains the exact `SOURCE_PAY_QUERY_OPEN` and `SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE` strings. They remain explicit PREPARE issue values, with REVIEW requiring a Work identity for these Work issues; the closed result validator does not erase them into a generic dependency failure. No rename, changed casing or substitute success was introduced.
- Command/preparation/frozen/history/advice callers obtain the injected active Office admin identity before protected dispatch, send the trusted actor through a fixed public RPC, retain closed request/response key sets and preserve decimal/bigint/timestamp strings. Their RPC transports return raw Responses; byte caps apply before parsing. No generic SQL/private function selection, money reconstruction, live Timesheet fallback or browser-supplied actor was added.
- The 13 job kinds remain consistent across job-RPC, job-step, queue and runtime-RPC. CLAIM/STEP schemas preserve exact job/kind/nonce/epoch/phase/cursor/page limits. Unknown CLAIM recovery does not adopt an active nonce; unknown STEP recovery preserves the original input. Definite terminal claims and owner-returned completed steps have explicit dispositions. PREPARE/position and case allocation continuations retain the finite owner phases and exact opaque or lossless cursors.
- Financial queue work performs one adapter call and publishes at most one continuation. It awaits durable publication before `message.ack()`, retries the original input on uncertainty, requires durable failure recording before a permanent-failure ack, and never infers lease expiry from a local clock. Message methods are invoked with their receiver intact. Missing ack/retry methods throw rather than allowing implicit successful batch disposition.
- Background sends at most 100 small envelopes per publication. A recovery tick visits enrollment, two independent bounded job lanes and expiry once each. The required EXPIRY receiver accepts only a run identity; SQL owns SYSTEM/clock/command identity. It acknowledges the Office-won race only for the exact definite NOT_ELIGIBLE code; other unknown/damaged states retry. No queue drain, provider action or inline money calculation was added.
- Runtime/expiry raw factories admit only fixed public endpoints, service-header shapes and HTTPS or loopback HTTP. Their parsed adapters bound response bytes/deadlines/rows, reject duplicate JSON keys, preserve microsecond UTC tuple ordering and fresh owner results, cancel late responses and report unknown metadata/receive outcomes. A decoded JWT role is explicitly a local shape check, never authorization.
- Frozen advice retains escaped labels, immutable projection/revision/line scope, nested bounded pages, at most one page per level, backpressure with 16 KiB chunks and a visible incomplete/failure marker until the final conserved member/detail counts. History keeps its frozen 52-section registry, ancestry-qualified cursor envelope and string/null scalar encoding, with no activation switch or financial writer.
- Legacy status is freshly obtained with a finite owner and positive lossless epoch. NEXT/DISABLED/unavailable status fails closed before the injected legacy callback. Email classification holds financial or malformed metadata without sending mail or changing delivery state. Known factual routes and the six exact NEXT route shapes are separate from the broad old financial-route fence; wildcard `/next/*` is not an exemption.

## Current Workers API review

Applied the local Workers best practices skill and retrieved current primary [Workers best practices](https://developers.cloudflare.com/workers/best-practices/workers-best-practices/) and [Queues JavaScript APIs](https://developers.cloudflare.com/queues/configuration/javascript-apis/). Compared the queue operations with task-cached `@cloudflare/workers-types` **5.20261009.1**, `.codex-local/workers-types/package/index.d.ts:2398` and `:2453`: `sendBatch` takes body/contentType envelopes and returns a Promise; Message ack/retry are void methods and retry admits delaySeconds. The port's awaited publication and bound message methods match those signatures. Streams are bounded or incremental, no request-scoped mutable module cache was found, and best-effort cancellation promises are explicitly handled with `void ...catch`; no money/publication promise is floated. Config/entry-handler changes belong to later wiring cards and were not invented by this review.

## Follow-up before visible wiring

`frozen-advice.js:268` retains the literal label `Net additions (including loan/credit cash)`, and `:296` renders raw `case_kind`, potentially `LOAN`. B11/OR-42 requires **Payment Advance** in new NEXT labels/exports while OR-45 preserves stored kinds and harmless unused labels. The exact F-09 modules are currently unwired, so this is not a failure of the required byte-preserved port. When F-10 exposes advice, change the visible label and display mapping; retain the `LOAN` stored kind, typed schemas and monetary contracts. Do not claim visible wording acceptance until this is addressed.

The exact 16 proposed existing tests are classified in `design/F-09-source-gate-scope.md`. They are allowed normal in-process unit/source gates, not business fixtures or runtime exercises. Complete command-RPC, frozen-RPC and payment-history-caller tests assert F-10 routes, so preserve and include those tests with the wiring; the other 13 have no later HTTP dependency. No test outcome is inferred from this source review.

Installed, deployed bundle and app checks remain **NOT_RUN**. F-10–F-14 must still connect the actual entry paths with fresh ownership, route fences, shared hooks, queue/cron awaits and quiet-window containment; F-24's future deployment remains the bundle acceptance boundary.
