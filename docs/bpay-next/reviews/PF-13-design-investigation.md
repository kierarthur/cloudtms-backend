# PF-13 design investigation — environment and capabilities banners

Date: 10 October 2026. Source-only investigation; no tests, SQL, business actions, provider calls, hosted reads or secret reads were performed. Root remains the sole product writer. This note covers the three PF-13 checklist objects and does not claim runtime acceptance.

## Authority and exact scope

Read the complete PF-13 card in `06_CONTRACT_MOVES_DAILY_PAYMENT_SUMMARY_PERFORMANCE.md` (1938–1960), the three current checklist rows, owner start sheet §3, F-12/F-15 source/design receipts and the F-16 visibility dependency. D01 is the capabilities read and NEXT environment boundary in `broker/src/index.js`; D02 is the existing Office capability/environment/error presentation; D03 is the future joined check. The card requires environment enforcement for reads, prepare, export, execute, retry and corrections, truthful capabilities while the provider is unavailable, TEST presentation, and preservation of Invoice Discounting's independent behavior. WB-42 and EX-47 consume this authority later; they are not built ahead here.

BD019 rule B remains binding: expected revision, captured selection Candidate settings, query snapshot and existing `pin_v2`. No parked F-25 producer/resolver, PIN, enroller, SQL owner or new command protocol belongs in this item.

## Existing source to retain

* `GET /api/banking/capabilities` already escapes the F-12 legacy financial classifier: `/api/banking/capabilities` is outside its `/pay` and `/finance` families. Do not broaden that classifier or add a prefix exemption. The existing route remains Office-admin authenticated.
* F-15 `GET /api/banking/pay/next/owner-status` is a diagnostic bootstrap read with its existing 401/403 authorization, two-field result, fixed service RPC and bounded error handling. It must remain available when the business environment is unavailable.
* The exact NEXT route block normalizes duplicate/trailing slashes, authenticates with `requireUser`, then explicitly requires admin before constructing a business RPC. Preserve that order, exact advice UUID matching, accepted wakes, closed parsers and retained refusal/journal behavior.
* Default `handleBankingCapabilities` reads legacy settings and loops rail adapters. REVOLUT's `capabilities` can call `revolutAuth_getAccessToken`; this is not a harmless NEXT diagnostic. Preserve the default route for Invoice Discounting, and branch the explicit NEXT diagnostic before settings/provider probes.
* `wrangler.toml` TEST vars already establish `CANDIDATE_APP_ENVIRONMENT = "TEST"`. This is trusted deployment configuration. `rail_env_default` (`SANDBOX`/`PROD`), provider API hostname inference and editable `payroll_testing` are different facts and cannot authorize the application environment.
* `main.js::renderBankingTab` already suppresses shared rail/funding/global banners on Pay and preserves them on the other Banking surfaces, including Invoice Discounting. Keep those two existing scoped conditions. Do not copy the legacy `renderBankingBanners` global settings actions or its “bank may validate” CoP copy into NEXT.

## D01 — smallest implementation

Use the already selected finite interface: `GET /api/banking/capabilities?scope=NEXT` and the existing HTTP request header `X-CloudTMS-Banking-Environment`. Add the header to the existing CORS allow list. Neither changes a closed command/preparation/history body, request hash, nonce or journal bytes.

The NEXT branch is configuration-only and has a closed factual result: scope `NEXT`; deployment/requested application environment (`TEST`, `LIVE` or null); an environment-ready/mismatch boolean; TEST mode; CoP false; provider `DISABLED`; funding verification `UNAVAILABLE`; CSV availability only when the supported environment boundary is ready. Those last three facts do not certify provider, balance or funding readiness. Diagnostic failure must use bounded fixed copy/codes and no dependency details. Default capabilities keep their existing behavior.

The actual build has TEST authority. Missing/unsupported trusted runtime configuration, missing/unsupported caller header, caller/runtime mismatch or an unsupported effective backend destination fails closed. A header is caller intent, not authority; it cannot select or override the Worker/database environment. The frontend expectation must come from this TEST build's trusted application configuration, never from echoing the returned Worker environment. Do not invent a LIVE gateway, approve LIVE operation, or treat `SANDBOX`/`PROD` as application environment values. A future LIVE destination needs its own established source/release authority; this item need not implement it.

For the six existing business interfaces (history-page, frozen-detail, preparation-page, advice UUID, preparation and command), apply one pure environment check **after the existing 401/403 Office authorization and before the first business RPC factory/invocation**. They already bypass the legacy owner lookup. Owner-status and capabilities are diagnostic exceptions. All financial command kinds, including export/execute/retry/corrections, share the command interface, so the check covers them without per-action rewiring. BD030's final scope refinement preserves unknown paths' existing F-12 exact fence and unsupported-route semantics. Each later card that actually adds a route must wire this same helper after its authentication and before its business RPC; no speculative prefix authentication, broader dispatch or automatic future inheritance is required here.

Return the established native error shape for each interface. Preparation, preparation-page and command retain the four-field envelope including `outcome_unknown: false` for a known pre-RPC refusal; history/frozen reads retain their three-field bounded refusal schema, and advice remains an unsuccessful non-HTML response. A new definite refusal cannot erase an earlier unknown attempt in the same retained journal. Do not infer that a prior request was unaccepted from the current environment error.

### Findings in the fresh draft during this investigation

Root began the bounded implementation while this design was being saved. The fresh `index.js` includes the explicit NEXT capabilities branch before the legacy probe and the CORS header; these match the design. Two corrections were reported immediately:

1. The global environment call at approximately 198520 currently precedes the exact NEXT Office authorization. A missing-header anonymous request therefore receives 409 before the preserved 401/403. Move the business guard behind the exact auth block, still before its first RPC; do not change auth semantics to satisfy the environment check.
2. The fresh helper contains a guessed `cloudtms-live-miget-gateway.kier-88a.workers.dev` destination. No source/release authority for that hostname was established in this TEST investigation. Refuse unsupported environments/destinations instead of certifying that guessed LIVE route.

These are draft findings, not a completed-product verdict. Root owns their correction and the subsequent source review.

## D02 — existing Office integration

The NEXT Office transport, history client and original-advice fetch must carry the same fixed build environment header on every business request. Leave owner-status diagnostic transport separate. The capabilities read uses `scope=NEXT`, the same intended environment header and a small bounded closed decoder; it must not call the old global Banking bootstrap or adapter probe.

Retain current actor/frame/close safety and pending journal handling. A single capabilities read on open and an explicit Re-check are sufficient; no financial polling or new protocol is required. Track finite pending/ready/unavailable state. Unknown or mismatched environment blocks business controls and business progress reads, while Close and diagnostic Re-check remain usable. Do not trigger command retries, review, enrollment or drains from this diagnostic.

Render factual TEST/application-environment and wrong-environment/unavailable notices at the top of the NEXT sheet using the existing safe text/alert presentation. Show CoP/provider/funding verification as unavailable while the provider is disabled. CSV artifact availability is separate and cannot imply provider or funding verification. Preserve the current local error/unknown-outcome notice and its recovery language. Do not add legacy Funding Required/Open Global Settings actions or restore the legacy rail chip on Pay.

The named other Banking surfaces retain their existing shared funding/global/error/rail presentation; Invoice Discounting retains its default capabilities contract. OR45's retired legacy financial surfaces are not resurrected to satisfy this banner item. Later NEXT screen owners consume the same capability/environment facts where their cards require them; PF-13 does not build their workflows.

## D03 and completion criteria

D03 is a saved future joined acceptance checklist, marked NOT_RUN. It should cover: NEXT capabilities remain reachable under NEXT and unavailable/mismatched environment; no settings/adapter/token/provider probe in the NEXT branch; TEST label and unavailable CoP/funding/provider are factual; correct expected header on Office/history/advice paths; each business path refuses before its business RPC; 401/403 and diagnostic bootstrap remain intact; current closed errors and prior-unknown journal bytes survive; Invoice Discounting/default capabilities and existing scoped banners remain unchanged. Runtime/browser/provider/fixture/timing checks are not authorized in this phase; ordinary source checks belong to root's permitted workflow.

D01/D02 are complete only when the actual guard and every current client business path are wired, the bounded NEXT diagnostic and truthful banners are used, and the finite source review closes the two draft findings above. D03 can be document-complete while the joined execution remains NOT_RUN. No database marker, provider-readiness routine, F-22 configuration programme or parked F-25 work is required.

## Source receipt

Saved source inspected on 10 October 2026 (SHA-256):

| File | SHA-256 |
|---|---|
| BE `broker/src/index.js` (fresh draft) | `796b9847e258d12ba5a9c8b80845ce050b573aacf2a005f7ed7097e76e0f60bd` |
| BE `broker/src/bpay-next-environment.js` (fresh draft) | `89c925bd58eada8c447c585722ffc381b7f7b7c59df43aaada6920f528aee04b` |
| BE `broker/src/bpay-next-legacy-fence.js` | `afdbdda4ac440ada0a6267523a4a60398ae47feec85fcf8772f0fb2a2c6dfef8` |
| BE `wrangler.toml` | `abe27f2dff0d9c0c07a4af699d2125b8c6bd0c60948e5d2f7a5d18632455903d` |
| FE `js/bpay-next-office.js` | `abb793e194df1ed714677796bd9351f7df8aaadd367317b0ae448ee9fbbd114a` |
| FE `js/bpay-next-payment-history.js` | `ca46c0d595ba94757feb869242b2209df2e6047eb778f2787f97d5ef4d8b87db` |
| FE `js/main.js` | `337ad43e351b88f8661169365c391fbca9e9d98cb2b877828df49aa7e2c2963c` |

No product, SQL, test or runtime files were edited by this reviewer.

## Final design closure — 10 October 2026

Verdict: **PASS for the bounded PF-13 design and saved D03 future checklist**. Freshly read `design/PF-13-SCOPE.md`/BD030, `PF-13-JOINED-ACCEPTANCE.md`, the complete finite environment helper and the actual authenticated routing/capabilities/client seams. Both draft findings above are closed in the saved source: the environment guard now follows `requireUser` and explicit admin 403 in the exact NEXT route block, before its RPC adapters; supported destinations are only the evidenced TEST canonical gateway origin/root path or TEST HTTP loopback. LIVE may be diagnosed as a label but cannot become environment-ready. No guessed LIVE destination remains.

The NEXT capability branch still returns before legacy settings/adapters; default capabilities and the existing Pay/other-tab banner rules remain separate. Office transport, history reads and original advice carry the explicit environment header without changing request bodies or journal bytes. The final design correctly protects all six current business interfaces, preserves owner-status/capabilities diagnostics and leaves unknown/future routes to the existing fence and each later route's own finite integration. The saved D03 checklist covers factual labels, pre-RPC refusal, authorization, unavailable diagnostics, exact retry preservation and independent Invoice Discounting behavior, and marks every actual joined/runtime/browser/queue clause NOT_RUN. Other reviewers own complete implementation review; this closure does not replace it or claim installed/app acceptance.

Fresh closure SHA-256:

| File | SHA-256 |
|---|---|
| BE `docs/bpay-next/design/PF-13-SCOPE.md` | `df7d07d6ff8087bbc5e780246f1fb0d47ba61b13c77a8e8b1416f6d8041929a3` |
| BE `docs/bpay-next/PF-13-JOINED-ACCEPTANCE.md` | `8d7d12fcfa6f02721cc110530d22128340ff47ffed8dd2f454512f9d7251d93e` |
| BE `broker/src/index.js` | `9610a75e0cd0d83f023a050104f7fad3de3995a5592b1b26ea9b4e3bfadd9fa3` |
| BE `broker/src/bpay-next-environment.js` | `82d0339fd2a3881d94cc73a7237fd27c5e5755a60ca9133c1a0a460dabfa3424` |
| FE `js/bpay-next-office.js` | `31752685c20b61df2d9e4cecdf218ed7f26ba730c565d0de6437b2a5a90df356` |
| FE `js/bpay-next-payment-history.js` | `e6efd41005e6c80808570ea1d92c25f3e16adcf1eb8afb5cf1835444cf7323c5` |
| FE `js/main.js` | `337ad43e351b88f8661169365c391fbca9e9d98cb2b877828df49aa7e2c2963c` |

No tests, SQL, runtime, provider, network or business action was executed for this closure. Only this review document was edited.
