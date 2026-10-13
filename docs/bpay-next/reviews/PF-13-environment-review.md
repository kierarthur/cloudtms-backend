# PF-13 independent environment/capabilities review

Reviewed saved BUILD-BE source on 10 October 2026 against the full PF-13 card, three canonical checklist rows and BD030 `design/PF-13-SCOPE.md`. Scope here is D01 and its actual shared environment/error/CORS contracts. No product edits, executions, tests, database, secrets, browser or hosted calls. D03 joined acceptance remains **NOT_RUN**.

## PF13-E01 — earlier automatic future inheritance wording

**CLOSED as documentation precision; no current product defect.** `broker/src/index.js:198706` uses the exact present method/path list and calls the environment helper inside that authenticated seam. The earlier scope said future routes automatically inherited the guard, which that seam does not implement. On full-path reread, the existing F12 classifier at `broker/src/bpay-next-legacy-fence.js:168–177` exempts only the exact current NEXT methods/routes and positively fences all other `/api/banking/pay/...` paths before the NEXT dispatcher. Unknown NEXT paths therefore retain retirement/unsupported behavior; there is no current bypass or missing business handler. BD030 now accurately requires later cards adding a route to wire the same pure helper at their authenticated seam. The earlier proposal to broaden prefix authentication/dispatch is withdrawn: preserving the exact F12 fence and present route scope is correct.

## PF13-E02 — configuration matcher accepts a different effective origin/base path

**CLOSED on fresh saved source.** The initial helper checked only the hosted hostname and HTTPS protocol, permitting an alternate port or `/other` base path to be declared ready even though adapters use that changed effective origin/path. The final helper requires `base.pathname==='/'` and exact hosted TEST `base.origin`, which correctly includes the protocol/default port boundary. Explicit HTTP loopback hostnames retain local ports. Existing credentials/query/fragment/foreign gateway/LIVE negatives remain. The source unit adds alternate hosted port and non-root path negatives. This was a configuration false-positive finding, not an observation of a hosted or physical database mismatch.

## Passed source boundaries

- `requireUser` and the existing admin check precede the environment gate for every presently named NEXT handler. Gate refusals occur before constructing preparation, financial command, history, frozen detail and advice RPC adapters. No guard changes the authority, replay or financial handler bodies.
- Exact GET owner-status remains authenticated/admin-only but diagnostic and environment-exempt. Scoped `GET /api/banking/capabilities?scope=NEXT` is reached only after generic banking authentication; its early response occurs before settings reads, adapter registry, token/provider capabilities or funding probes.
- The default capabilities branch is preserved in the inspected diff. Existing Invoice Discounting and other legacy capabilities consumers retain their separate contract.
- The closed capabilities response distinguishes Worker/caller TEST configuration from readiness: CoP false, provider DISABLED and funding UNAVAILABLE. LIVE/missing/unsupported deployment or foreign/missing caller environment cannot enable it. It does not claim installed database parity or use editable `payroll_testing`/provider SANDBOX configuration as authority.
- Environment refusal uses HTTP 409, `cache-control:no-store`, fixed `BPAY_NEXT_ENVIRONMENT_MISMATCH` and `retryable:false`. Command, preparation and GET preparation-page return the existing four-key error shape including `outcome_unknown:false`; history/frozen/advice return the three-key read shape. Current normalized repeated/trailing slash routes and helper normalization agree. The only helper diagnostic exemption is exact GET owner-status.
- CORS adds only the named environment request header to the existing allowlist. Existing permitted origins, credentials, methods, OPTIONS preflight and response wrapping remain intact. No header value or database URL is returned in errors.
- The existing source unit retains current fence cases and adds finite TEST/missing/LIVE/foreign-caller/gateway negatives, loopback positive, diagnostic exemption and closed shapes. It verifies current router ordering and the early capabilities branch. The reviewer did not execute it; root reports its 15 cases passed.
- The new D03 joined checklist maps every actual current business interface and diagnostics exception, existing 401/403, same caller header, closed errors, CORS, retained unknown journal, Re-check/no financial work, actor guards and truthful banner facts. It distinguishes provider SANDBOX/editable payroll settings/configuration evidence from physical database parity and F22 authority. All actual app/browser/installed/full Worker/queue/minute acceptance remains NOT_RUN.

## Final saved source identity

| File | SHA-256 |
| --- | --- |
| `broker/src/index.js` | `9610a75e0cd0d83f023a050104f7fad3de3995a5592b1b26ea9b4e3bfadd9fa3` |
| `broker/src/bpay-next-environment.js` | `82d0339fd2a3881d94cc73a7237fd27c5e5755a60ca9133c1a0a460dabfa3424` |
| `tests/bpay-next-legacy-fence.test.js` | `319d4f57eb5273a856fc8e39adb561e06366ed2246bdf5bc3b29f10595bcbe8c` |
| `docs/bpay-next/design/PF-13-SCOPE.md` | `df7d07d6ff8087bbc5e780246f1fb0d47ba61b13c77a8e8b1416f6d8041929a3` |
| `docs/bpay-next/PF-13-JOINED-ACCEPTANCE.md` | `8d7d12fcfa6f02721cc110530d22128340ff47ffed8dd2f454512f9d7251d93e` |

**Verdict: PASS for D01's bounded current source and D03's saved acceptance mapping.** No open concrete finding remains. D02 consumer source is separately reviewed by its assigned reviewer. No installed or app-tested claim; actual joined acceptance remains NOT_RUN.
