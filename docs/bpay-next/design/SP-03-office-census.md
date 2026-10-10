# SP-03 Office caller census

Read-only source investigation, 10 October 2026. Full `05_SOURCE_AND_PROTECTED_PAY.md:159–187` and the ITEM plus four SP-03 checklist rows reread. Scope is BD043 and the complete required card behavior. Existing success-tail/routing reuse does not prove complete DB gates or refusal transport. No tests, SQL, runtime, business actions or hosted calls were performed. Joined/application acceptance remains **NOT_RUN**.

## Existing authority and routing

| Object | Current source finding | Required action |
| --- | --- | --- |
| D01 first-authorisation `(uuid,uuid,text,uuid)` | Current 09102026_2018 successor retains the genuine successful outer 0557 NEXT stage/publication tail. The `active_owner='NEXT'` gate precedes staging/publication and the initial binder. Separate DB review found incomplete exact WAIT accepted-receipt qualification and the required payment-authoriser authority call. | Success tail is reusable; DB gate closure is deferred to the DB reviewer and root's narrow named-owner correction. No money-body rewrite is proposed. |
| D02 initial TSFIN bind | Existing 07102026_0018 binder, integrated in the current outer success tail and reviewed in SP-08, supplies the real initial approval binding. An initial TSFIN without HEAD remains valid. | Reuse; no invented HEAD, Final, producer or Rule-B changes. |
| D03 routing | `authorise-routing.mjs::weeklySourceAuthoriseRouting` calls the narrowly typed `weekly_source_office_authorise_scope_v1`. Actual payment/publication admission stays in `weekly_source_first_authorise_v1`. The probe has no paid-evidence dependency. Current real handler defaults lose known Source refusals. | Reuse dispatch, but root must preserve the finite known refusals in the actual single/Contract Week adapters. No new route or owner decision. |
| D04 Office messages | Before the root's current SP-03 edits, `main.js` contained neither named code. Root has now authored a finite shared presentation helper and the caller seams below. | Complete/refine those exact paths only. |

Actual current Worker locations are `index.js:168412` (Timesheet routing), `170097` (Contract Week routing), `94027` (gold bulk item reaches `handleTimesheetAuthoriseGeneric`) and `95907/95914` (existing bulk row dispatch reaches Contract Week/Timesheet handlers). The managed branches call the same four-argument Source owner. Historical card line numbers 95533/97420 refer to older source positioning.

**Correction after the DB/Worker census:** Passing `parseRpcFailure` into `mutationErrorResponse` does not establish preservation. The single adapter's unrecognised default at 168175–168181 emits `UNKNOWN_LIFECYCLE_OUTCOME`; the Contract Week adapter's default at 169952 emits a generic server error. Both can hide real Candidate-busy/Additional/query refusals. The DB/Worker census (`SP-03-authorisation-census.md:42–48`) owns the finite transport repair and its full source verification. This Office census does not mark D01/D03 complete. The same census records that current core's latest WAIT predicate lacks the exact matching accepted receipt and that the actual first-authorise path omits the required `APPROVE_PROTECTED_PAY` authority invocation. Root is correcting those gate seams; DB verdict is deferred to that independent review. The unchanged initial TSFIN binder and actual dispatch are the reusable pieces.

## Exact Office seams

Line numbers below are from the freshly saved root-authored `main.js` snapshot; no product edits were made by this reviewer.

| Caller | Current seam and original behavior | Minimal presentation support |
| --- | --- | --- |
| Single Timesheet | `authoriseTimesheet` at 314594; POST and original error loop around 315077–315134. Only existing expense/pair confirmations explicitly repeat the request. Original lifecycle/moved reconciliation precedes final throw. Footer catches at 334652 and displays `e.message` at 334663. | Map the known refusal before the original reconciliation/final throw (current 315137), preserve status/code/body/payload and the existing confirmation/reconciliation branches. Never put Candidate busy into the loop's `continue` branch. |
| Contract Week manual | `contractWeekAuthorise` at 315634; existing expected-ID/signature POST, evidence warning and affected-row refresh. Mutation-in-flight guard is removed by `finally`. | A narrow catch (current 315732) maps/rethrows the same two errors; retain the original POST and `finally`, and let existing callers display the mapped message. |
| Bulk transport/result normalization | `authoriseSelectedTimesheets` at 196787, `normalizeResult` at 196857. Actual operation polling precedes the final result mapping. `results` and server `failed_items` are separate paths at 196962–196963. | Qualify once only if a known refusal exists; map both arrays without rewriting success flags, operation IDs, row keys, signatures, counts or durable polling. A supplied `failed_items` array otherwise bypasses `normalizeResult`. Preserve raw structured refusal data. |
| Bulk completion summary | Current 261168–261180 builds the visible result. Original server `failure_count` still truthfully records unsuccessful authorisations. | Partition only visible known NEXT Candidate-busy rows into “Waiting — try again in a moment”; show the exact busy sentence per row. Leave other errors and internal counts untouched. Mixed successful/waiting/refused rows remain truthful. Busy must never appear beneath the visible “Failures” label or use the “Failed” fallback. |
| Whole bulk HTTP refusal | Existing wrapper catches around its POST; workbench catch at 261232–261242 displays the thrown message. | Map/rethrow known whole-request errors in the wrapper before this generic catch. Do not retry automatically or turn a refusal into success. |

There is **no `normalizeRawFailures` symbol** in current `main.js`. The nearby helper is `collectFailedKeys` (around 260949), used for row selection/reconciliation. The actual refusal display is the summary path above. Unauthorise's separate summary is outside SP-03.

`apiPostJson` at 3794 already preserves non-2xx `.status`, `.body`, `.json`, `.payload`, `.backendPayload` and raw response text (4094 onward). A shared global HTTP rewrite is unnecessary. Recognition should use only the exact two actual refusal identities; arbitrary message substring matching would broaden this card.

## Positive F-15 status controls wording only

Reuse `bankingNextOwnerClient` at 5061. It is the existing admin-only, current-actor client; owner module `peek()` returns null when closed or its actor is no longer current. Missing/unavailable F-15 status must neither grant nor block genuine payment-authoriser capability or change routing. Apply these two labels only when the current positive `peek().active_owner === 'NEXT'`.

The root's current helper at 5076–5110 qualifies only after detecting a known refusal, then changes the presentation. If that qualification awaits, retain the initiating actor across the wait: recheck it before rewriting/returning a late result. Existing current-actor `peek` protects the owner read, but a later helper call must not silently adopt another signed-in actor's NEXT status for the earlier actor's refusal. Use the caller's existing current frame/request guards where present; no new owner route, timer, retry or authority check is needed.

Candidate busy copy is exactly: **“A payment step for this Candidate is running. Try again in a moment.”** It is retryable information; the user initiates any later retry. Keep the original unsuccessful result and immutable request identities.

## Additional item identity is genuinely absent on refusal

`04102026_2120_weekly_source_approval_basis_v2.sql:74–123` iterates the actual additional-unit entries. Missing/malformed required unit facts, numeric/scale failures and computed-value mismatches raise bare SQLSTATE 23514 message `BPAY_NEXT_SOURCE_FIRST_ADDITIONAL_UNMAPPED`. Those raises provide **no offending item code in DETAIL**. Therefore the existing transported refusal cannot identify a particular code.

Plain copy can truthfully name the category “An Additional item” and explain missing/mismatched approved units. Include a specific Additional code only if the actual structured response supplies it; never choose an array member, infer it from amount/ordinal, or claim the first item caused the refusal. No DB/Worker detail extension is required here. The actual creating component identity remains the single stable call at 2120:107–108; 2307 and 1550 retain the I1 component ID rather than reconstructing it.

## Source receipts and acceptance limit

Initial pre-edit Office `main.js` SHA256: `a686f08d916ac57230eb695cd2e56ccf2ca3bcc624c59d82489c6a813fec895c` (neither code present). Fresh root-authored caller snapshot SHA256: `d3e0e10b402943b80a91c1173e974cdb0ffff0420743f743ae7e4b0e3c7c5136`. These are investigation receipts, not a completed-product verdict.

Unchanged routing module SHA256: `fec2954202a0a326738937540ed1ef129d8839991d1354f21873ef3e7d445dcd`. Current SP-03 scope SHA256: `ac0e9252903dd1b1e958345a7a07ad41126da5d6407103284310e11396eb3aba`.

Future joined checks must observe the genuine 7.5-hour initial WORK/revision/book/position, explicit busy retry, manual-review block versus missing-signature allowance, real authoriser permission refusal, unmapped Additional refusal, and two-row bulk Source routing while the paid read is unavailable. These observations have not been run; source reuse and Office presentation do not establish their business outcome.

Final Office follow-up: `reviews/SP-03-office-review.md` records D04 source PASS at `main.js` SHA256 `4e65bb35b89748c74c60eda943cb6564c23c2b7b9182bc774722bc543f104fa0`. All three actual request entries now capture the initiating actor before asynchronous work and carry it through before/after F15 checks, both result arrays and the summary. The actor finding above is closed. DB gate and Worker code-retention verdicts remain separate; application acceptance is NOT_RUN.
