# PF-13 caller and scope review

**PASS — D02 source/caller review and D03 authored future checklist review.** No concrete remaining caller/scope gap found in the fresh saved source below. Actual app/installed/browser/full Worker/queue/minute/environment joined receipt remains **NOT_RUN**. D01 whole Worker router/server review belongs to its independent owner; this file checks the finite emitted/consumed contract and caller seams, not a duplicate D01 runtime verdict. No product edit, test/app/browser/scenario/SQL/runtime/hosted/data operation was performed by this reviewer.

## Binding scope

Full PF-13 card `06_CONTRACT_MOVES_DAILY_PAYMENT_SUMMARY_PERFORMANCE.md:1938–1958`, exact current PF-13-D01/D02/D03 checklist rows, and `design/PF-13-SCOPE.md` BD030 were read. Dependencies F-12/F-15/F-16 remain separate from their future runtime acceptance. D02 is the current reconciled Office capability/environment/error display; D03 is the finite future joined checklist. The scope retains exact existing business handlers, Invoice Discounting and other Banking tabs, F-25 Rule-B captured choices, journals and provider-off authority. It adds no SQL marker, provider/CoP/funding readiness or earlier future-card implementation.

## Closed diagnostic contract and readiness

Current `js/bpay-next-office.js:173–182` adds a finite environment value (`TEST` by default for this TEST frontend) and closed capability receipt. Exactly 11 required fields and their semantics are checked: `ok`, `scope`, `worker_environment`, `requested_environment`, `environment_ready`, `test_mode`, `env_mismatch`, `cop_available`, `provider_status`, `funding_status`, `csv_available`. Unknown/extra/missing fields, foreign request identity, contradictory readiness/test/mismatch/CSV values, invented CoP true/provider-ready/funding-ready are rejected. Worker environment may be unavailable/null diagnostically; it cannot authorize readiness. This matches the actual literal `bpayNextCapabilities` emitted fields in `broker/src/bpay-next-environment.js:18–21`.

`transport.capabilities()` uses exactly authenticated `GET /api/banking/capabilities?scope=NEXT`, bounded JSON, existing actor validation and timeout; no body or business command. The selected contract is a configuration diagnostic and does not invoke the old default provider adapter or certify physical DB TEST parity. This review does not reinterpret the untouched default capabilities/Invoice Discounting contract.

Office initial render has no capabilities, so current business controls are locked. Startup fetches capabilities first and refreshes a retained run only when the exact diagnostic environment is ready. Re-check clears readiness before awaiting the diagnostic read. Top display states TEST MODE when the trusted worker diagnostic says TEST, explicit environment when ready, and an alert blocker for unavailable/mismatched capability/environment; name check is unavailable, provider disabled and funding unverified. It never displays a guessed balance or bank/CoP connection. Close stays direct/enabled. Re-check remains an ordinary diagnostic control, subject only to current busy read/operation state.

Fresh `locked` at 539 contains `!capabilities?.environment_ready` plus existing busy/pending/owner gates. **Exact Retry at 550 also has the environment-ready gate**, so an uncertain request cannot be sent while the diagnostic is blocked. Factual progress polling follows the same current locked gate. Re-check does not enqueue/resume/drain a command or replace accepted state. Errors are plain, and an explicit `BPAY_NEXT_ENVIRONMENT_MISMATCH` invalidates cached capabilities and requests Re-check.

## HTTP environment and identity preservation

- Existing common Office business transport at 224 adds only `X-CloudTMS-Banking-Environment: environment(options)`. Content type, accept, redirect/error policy, auth behavior, body bytes, routes and args remain current.
- History's existing bounded POST adds the same header and validates the explicit environment enum/default at client creation. Manifest/section/cursor body and actor/disposal/frame/Back semantics are untouched.
- Original frozen advice GET adds the same header; immutable instruction ID, template/stream markers, bounded UTF-8/actor checks, incomplete-on-failure behavior and PF-11 London print presentation remain unchanged. Bank CSV stays original issued bytes.
- Read-only comparison shows the entire current Office `createController` block through the display helpers is byte-equivalent to HEAD. The PF-13 diff contains no mutation body/journal/adoption/retry/financial calculation change. UI capability state remains separate from the exact accepted/uncertain command journal.
- Transport's existing closed error decoder receives known non-acceptance for an initial pre-RPC environment refusal. Existing `dispatch(previousOutcomeUnknown)` at 427–446 deliberately clears pending only for an initial known refusal. An exact retry with an earlier lost reply uses `dispatch(true)` and retains original identity/bytes through any later auth/environment refusal, until a validated receipt; it does not falsely convert a previous unknown outcome into non-acceptance.
- The supported TEST caller is fixed by this frontend's default; editable payroll_testing and provider SANDBOX/PROD are not environment authority. No production route or provider selection is introduced here. The environment helper's error envelopes are finite and match current callers; advice displays INCOMPLETE on a refused/non-template response. Whole router-before-RPC ordering is the D01 server review's responsibility.

## Existing other Banking tabs and coherent assets

`js/main.js` has **no PF-13 edit** (Git diff to HEAD empty; current raw SHA listed below). Its existing `renderBankingBanners` and default capabilities contracts remain independent. Existing 46122 keeps its precise healthy/bootstrap Pay chip condition; 46127 excludes shared banners on Pay and retains them for other Banking tabs. The current NEXT sheet is entered directly before the old Banking surface and shows only its own scoped diagnostic/error presentation, without resurrecting retired Funding Required/global-rail widgets on NEXT Pay. Payment Batches, Payment Advances/Snoozes and Invoice Discounting keep their existing presentation and ownership.

`index.html` advances only current NEXT CSS/helper/owner/Office/history asset markers to coherent `20261010-next-environment-r1`, keeping the required UI/date/owner dependency order. No main/global theme change was made. PF-12 scoped focus/caret/safe-area behavior is retained.

## Ordinary source additions and D03

The additions stay in existing ordinary Office/history unit files. They exercise the real capabilities call's exact URL, GET/bodylessness/header, accepted false diagnostic state, and contradictory/extra capability rejection. Finite source assertions check actual locking/header/banner/other-tab wiring and coherent markers. Existing journal/late-actor/advice/history field/cursor contracts remain. No new fixture/scenario/proof runner or provider probe is introduced. Builder's ordinary unit execution belongs to its recorded run; this reviewer did not execute them.

`docs/bpay-next/PF-13-JOINED-ACCEPTANCE.md` is present and **REVIEWED for D03**. It explicitly covers scoped capabilities versus untouched default/Invoice contract; truthful TEST/environment/CoP/provider/funding diagnosis; other-tabs shared banner/chip preservation; authentication-before-guard then refusal-before-business-RPC; all current history/frozen/preparation/advice/command interfaces; HTTP/CORS header and unchanged bodies/cursors/retry; known initial refusal versus retained earlier uncertain identity; diagnostics versus F-22/physical DB proof; actual actor/owner/frame/error/template/served assets. Every actual app/installed/browser/full Worker/queue/minute clause is clearly NOT_RUN. Authoring/reviewing this checklist does not certify real application behavior or installation.

## Fresh source/document SHA-256

| Exact path | SHA-256 |
|---|---|
| `C:\Users\KierArthur\OneDrive - Arthur Rai\Documents\GitHub\.codex-worktrees\bpay-next-build-frontend-20261009\js\bpay-next-office.js` | `31752685c20b61df2d9e4cecdf218ed7f26ba730c565d0de6437b2a5a90df356` |
| `C:\Users\KierArthur\OneDrive - Arthur Rai\Documents\GitHub\.codex-worktrees\bpay-next-build-frontend-20261009\js\bpay-next-payment-history.js` | `e6efd41005e6c80808570ea1d92c25f3e16adcf1eb8afb5cf1835444cf7323c5` |
| `C:\Users\KierArthur\OneDrive - Arthur Rai\Documents\GitHub\.codex-worktrees\bpay-next-build-frontend-20261009\index.html` | `c8dec7a0bc91f3cc8194ec8f4ad74525eb6f0cd2dbf8631ef1a1691a5e4b5b14` |
| `C:\Users\KierArthur\OneDrive - Arthur Rai\Documents\GitHub\.codex-worktrees\bpay-next-build-frontend-20261009\js\main.js` | `337ad43e351b88f8661169365c391fbca9e9d98cb2b877828df49aa7e2c2963c` |
| `C:\Users\KierArthur\OneDrive - Arthur Rai\Documents\GitHub\.codex-worktrees\bpay-next-build-frontend-20261009\tests\unit\bpay-next-office.test.cjs` | `2cef4b47bc9e53972c6075171163a8fe68ef567ce1ef8968e48c0a614a02f63e` |
| `C:\Users\KierArthur\OneDrive - Arthur Rai\Documents\GitHub\.codex-worktrees\bpay-next-build-frontend-20261009\tests\unit\bpay-next-payment-history.test.cjs` | `eaabc0a33acd75568698c218438a64bb61529d1fc211e506bb4d2e5fcd3a5af3` |
| `C:\Users\KierArthur\OneDrive - Arthur Rai\Documents\GitHub\.codex-worktrees\bpay-next-build-backend-20261009\broker\src\bpay-next-environment.js` | `82d0339fd2a3881d94cc73a7237fd27c5e5755a60ca9133c1a0a460dabfa3424` |
| `C:\Users\KierArthur\OneDrive - Arthur Rai\Documents\GitHub\.codex-worktrees\bpay-next-build-backend-20261009\docs\bpay-next\design\PF-13-SCOPE.md` | `df7d07d6ff8087bbc5e780246f1fb0d47ba61b13c77a8e8b1416f6d8041929a3` |
| `C:\Users\KierArthur\OneDrive - Arthur Rai\Documents\GitHub\.codex-worktrees\bpay-next-build-backend-20261009\docs\bpay-next\PF-13-JOINED-ACCEPTANCE.md` | `8d7d12fcfa6f02721cc110530d22128340ff47ffed8dd2f454512f9d7251d93e` |
