# WB-35 retained upstream source review

**PASS: current named upstream routes and TSFIN cron remain outside the legacy Banking fence. Preserve the explicit SP-02 Recover refusal under NEXT.** Read the full WB-35 card (`02_WORKBENCH.md:1263–1281`), ITEM and D01–D16 rows, actual current handler/dispatch/fence/cron definitions, 0550/0555/0715 source and registry, policy snapshot source, SP-02 card and `design/WB-35-upstream-route-census.md`. No product source, SQL, test, gate, queue, drain, cron, settings, provider, app, network or hosted action was executed or changed.

## Actual retained paths

The current `isBpayNextLegacyFinancialRoute` winner is `broker/src/bpay-next-legacy-fence.js:162–181`. Its normalized exact matches concern Banking pay/finance, named Timesheet financial actions, remittance mutations, reauthentication and Candidate pay-method-change. None of the following actual methods/paths matches those branches. The index calls this predicate before its ordinary router (`198589`), authenticates fenced Office requests before service owner lookup and returns exact retired/disabled/unavailable status (`198594–198606`). No new exemption is necessary for these upstream paths.

| Clause | Independently inspected current source | Result |
|---|---|---|
| LSV-126 GET/POST `/api/rates/resolve-preview` | Actual dispatch `index.js:199962–199963`; one handler at `161373`. Admin authentication; GET query/POST body; actual Candidate override plus unified dated default and charge; actual pay-channel selection and missing/notFound response (`161394–161437`). | Retained unchanged; no Banking mutation admission added. |
| LSV-127 POST `/api/timesheets/finance-preview` | Dispatch `200117`; handler `173276`. Bounded actual IDs/current TF; real current physical context and `loadPolicy` with the actual Timesheet/Contract/workflow (`173302–173332`). | Retained factual preview and frozen settings authority; preview is not new approval/publication. |
| LSV-128 POST `/api/tsfin/recompute` | Dispatch `200124`; admin handler `172865–172879`; original maximum 200 requested IDs and one `enqueue_ts_financials_priority` RPC with MANUAL. | Retained enqueue, without owner selection/payment or extra projection. |
| LSV-129 POST `/api/tsfin/queue/drain` | Dispatch `200123`; admin handler `172853–172859`, existing clamp 1–500, actual `runTsfinWorkerOnce`. Existing worker leases through specific/batch owners (`166820–166829`) and calls actual snapshot completion (`167724`). | Retained factual worker. No retired Banking-owner check at this route. |
| LSV-130 GET `/api/tsfin/financials` | Dispatch `200125`; admin handler `172886`; original current TF query (`172893–172906`) and optional actual requested Timesheet-ID filter. | Existing read retained. It is not a new bounded NEXT financial protocol; this review does not strengthen its pre-existing optional scope. |
| LSV-131 finance windows | GET/POST base routes `199785–199786`; PATCH/DELETE actual `/:id` forms `199788–199795`. Admin handler `3414`; real `settings_finance_list` at `3443`, actual settings table POST/PATCH/DELETE (`3506–3510`, `3582–3588`, `3607–3612`). | Every actual method/path stays outside the legacy fence; existing validation and settings authority remain. |
| LSV-168 TSFIN cron | Independent `broaderScheduledTask` at `201576`, existing settings availability gate and actual TSFIN loop `201647–201654`. Banking's separate task checks its own cron/owner and calls NEXT recovery (`201482–201499`). | F-12 retires old Banking work without suppressing genuine TSFIN scheduled processing under NEXT. |

The census maps actual current imports/adapters/background continuation and service0560/job0280 context rather than treating historical modules as live dispatch. Shared publisher1009/approval1530/staged1430 remains the approved-publication path. The source-only route check does not close later snooze, stored projection/refresh or complete Worker→queue→minute app acceptance; those named later obligations remain with their actual editors.

## DB fence and settings evidence

Registry confirms the final writer fence is actual `04102026_0550_banking_pay_next_legacy_writer_fence_v1.sql`, callback gate is 0555 and exact retired-finance entry is 0715. The 0550 source enumerates exactly 33 legacy financial relations and requires two ALWAYS-enabled row/truncate triggers per relation, exactly 66 in total. Its body/ACLs declare VOLATILE SECURITY DEFINER, `pg_catalog,private`, revocation from ordinary/browser/service roles and actual singleton FOR SHARE admission through `bpay_next_assert_legacy_writer_v1`. It does not list TF/TSFIN outbox, settings windows, rates/defaults/overrides or factual Source scope-change tables. Source runtime catalogue metadata was not independently queried by this reviewer; reported local catalogue observations must remain distinguished from these source declarations.

The real policy trigger (`1324:76–109`) orders against current policy control, creates a new immutable policy/window snapshot from actual settings and advances only current policy control. The revision's captured policy UUID stays bound; changing windows does not rewrite approved revision money or old frozen settings. No settings exemption or new policy writer is needed for WB-35.

## Stale Recover prose resolved by the binding rule

WB-35's older `1266/1270/1279` successful NEXT Recover wording conflicts with the explicit SP-02 rule (`05_SOURCE_AND_PROTECTED_PAY.md:131–147`): one writer, no revived G01/V8 engine, retain factual Source, but `RECOVER_FINALISED_PAY` receives the typed refusal. SP-02 explicitly names LSV-132 and OR-45/B4.4. The narrower exact distinction is required: **the shared command address and genuine Source facts/permissions remain; the retired financial action is refused before it opens old work.** Do not remove admission or add a NEXT exemption restoring old finance.

Actual command route `weekly-source/routes.js:624–638` invokes the existing dependency with the authenticated actor. The recovery owner normalizes the original request, checks actor equality (`finalisation-pay-orchestrator.mjs:1061–1072`) then calls `bpay_next_source_legacy_finance_admit_v1` (`1074–1078`). Its exact DB refusal becomes HTTP410 and the required plain message (`1080–1089`) **before** factual `weekly_source_finalisation_pay_open_atomic_v1` at `1092`. Actual service-only 0715 returns true for LEGACY, PT410 retired for NEXT and PT503 disabled through the same validated shared singleton lock. Shared 2336 factual/open/task definitions remain available for genuine callers. This is correct SP-02 reuse, not a WB-35 product gap.

## Completion-note review

Fresh `WB-35-COMPLETION.md`, SHA-256 `e1694e0f3da9fc99a701d8d9af67af2b32b404e232fc853917b056d4519a8a01`: **PASS**. BD053 explicitly resolves the old Recover wording with OR-45/SP-02, retains actual permissions/address and factual owners, and keeps the exact NEXT refusal. Its named method/handler/policy/publication/cron descriptions agree with source. It does not claim preview is publication or actual joined/hosted/timing acceptance. Local catalogue/check receipts are root-reported evidence, not a business execution by this reviewer. No new SQL, Worker, screen, broad exemption, money owner or F25 producer protocol is justified.

## Fresh source hashes

| Saved source | Raw SHA-256 |
|---|---|
| Current index | `eb507f1fa44ae63b75f3c86a86dc5217753ad6fea5fd9321dcce55d70fad2c14` |
| Legacy HTTP predicate | `afdbdda4ac440ada0a6267523a4a60398ae47feec85fcf8772f0fb2a2c6dfef8` |
| Source routes | `3641498765ed87d9d2c450e07cfc3dca05a9601725976d2e82e315f40dde4b11` |
| Finalisation/recovery orchestrator | `549e63d14c160eb1ad932089dc4febced99854f843bec7d36edb81f649dcfd0b` |
| Writer fence0550 | `de1314ab23759fe8b584e6757bdca49addb18267d060aab544d98fc015bb0145` |
| Retired entry0715 | `358abee947f0b03aeba9cbbbcadc17f6a7fe54bc56260951592b8a2279470044` |
| Upstream source census | `0932ee66736c3f30c7d3cb026b26af85697c78cac13ca3951db2fbe2634fa135` |

No actionable current product mismatch found. Actual in-app rate/recompute/drain/cron/publication, stored projection, queue and hosted/timing observations remain **NOT_RUN**.
