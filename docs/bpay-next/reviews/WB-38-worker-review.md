# WB-38 Worker source review

11 October 2026. Independent read-only review of the saved Worker sources and the 0312/0316 public contract boundary. Root is the sole product writer. No tests, syntax commands, SQL execution, database calls, browser journeys, fixtures, hosted calls or commits were performed by this reviewer. Frontend UI is outside this review and remains with its separate owner.

## Current verdict

**PASS for the reviewed Worker source/public contract boundary.** The route admission, receipt completeness, version and context identity findings below have been corrected in saved source and independently reread. No essential source defect remains in this boundary. This verdict does not extend to the shared window/UI or installed runtime evidence.

## Findings and closure

1. **P1, corrected: W1 context route was unreachable.** The SNOOZE_CHECK handler called the reader RPC factory while its guard admitted only ordinary row-reader KINDS. `broker/src/bpay-next-workbench.js:638` now admits exactly SNOOZE_CHECK in addition to the existing list. The ordinary KINDS list remains unchanged, which also preserves `json()` row telemetry: a context response has no rows and must not enter that row metadata branch.

2. **P2, corrected: MANY receipts could omit requested results.** `broker/src/bpay-next-snooze.js:155` now requires a nonempty bounded result page, exact retry membership through strict increasing uniqueness, requested membership and equal cardinality, and terminal normal-page successful plus failed counters equal to the frozen total. Retry pages deliberately do not require global total conservation: native 0316 marks a retry result page complete while the original full walk can still have unvisited members. The saved checks match that distinction.

3. **P2, corrected: a single mutation receipt did not bind its version increment.** `broker/src/bpay-next-snooze.js:117` now compares the returned version with `BigInt(args.expected_version) + 1n`. Native single SET/CLEAR advances one version. All version operands remain canonical decimal strings; no JavaScript Number conversion is involved.

4. **P2, corrected: exact context identity binding.** `broker/src/bpay-next-snooze.js:81` now binds returned snooze_kind to the requested canonical `args.snooze_kind`. For physical PRE_OPEN, it requires both resolved native work_id and expected_revision_id to be UUIDs while comparing the exact physical UUID/version supplied in the request. No identity is invented or inferred in the browser. The saved correction was independently reread.

## Source checks

- The new read uses the existing authenticated GET Workbench route, exact kind/snooze query members, duplicate-key JSON rejection, the fixed `bpay_next_workbench_page_v1` RPC, bounded request/response bodies and a four-second abort deadline. Native 0312 returns a genuine pure context read through the existing private function. It does not issue an acknowledgement or create a snooze during PRE_OPEN/CHECK.
- The native physical Timesheet opening resolves only one current sealed Work/revision for the supplied Candidate and exact physical Timesheet UUID/version. The bounded two-row lookup refuses zero or multiple matches. It supplies saved active date/note and London business date; the mutation path retains PRE_SAVE/date/basis/version checks. No guessed Case or Work UUID is introduced.
- New mutation actions use the existing W2 POST dispatcher. The outer body is closed to action/readback/args; the single and MANY inner schemas are separately closed and byte bounded. UUIDs alone are canonicalised; stable keys preserve case. Dates use valid civil-date checks, note length is by code point, and bigint operands remain strings with native signed-bigint bounds.
- The mutation path has one eight-second deadline including request-body reading. Response consumption is bounded and abortable. Once dispatch is attempted, timeout, network failure, malformed response and unknown upstream failure retain outcome_unknown. Only a recognised native SQL refusal clears attempted, since that failed statement did not commit. There is no automatic mutation retry.
- Single readback retrieves the existing actor/action/exact-body receipt qualified by the current native owner epoch. Missing readback returns received=false and result=null; it does not execute the mutation. MANY readback performs the same exact parent/page qualification before its mutation branch. Returned request/Candidate/page identities are compared with submitted operands.
- `RETRY_WORK_JOB` retains its original exact argument/receipt branch and default factory action. Snooze admission does not broaden its argument schema. Existing index routing only wakes a READY Work retry receipt; snooze receipts do not accidentally enter that branch.
- 0312 retains the prior 0207 Workbench reader apart from the named context branch and its exception handling. Its public command preserves the original retry path and delegates MANY to native 0316. The public boundary remains service-role only, active-admin checked, with explicit owner/search_path and no browser-role grants.
- MANY receipt amounts are server factual strings validated for finite decimal shape. No money calculation, financial producer, row kernel, hold, reservation, claim-authorisation write or additional endpoint is introduced in the Worker.
- The potential unbounded unit-label concern was traced to current producer source rather than expanded into schema scope. In `10102026_1236_banking_pay_next_segment_authority_final_v1.sql:991`, ordinary expense inserts omit unit_label; mileage uses literal `mile` at 1004; Source expense inserts at 1699 and 2630 also omit unit_label. The current expense/mileage paths therefore return short fallback/native labels through 0316. The generic core column's lack of a length check alone is not evidence for an unavoidable new schema change. Worker response validation remains byte bounded and fail closed.

## Sources and limits

Reviewed `broker/src/bpay-next-snooze.js` (full current module), `broker/src/bpay-next-workbench.js` (current diff and affected read/command factories and handlers), `broker/src/index.js:198882` (existing route/wake integration), `supabase/repeatable/11102026_0312_banking_pay_next_snooze_context_controls_v1.sql`, `supabase/repeatable/11102026_0312_banking_pay_next_snooze_workbench_controls_v1.sql`, and the relevant receipt/readback portions of `supabase/repeatable/11102026_0316_banking_pay_next_snooze_many_v1.sql`. The native MANY implementation has its own independent database review owner.

Authority read: `docs/bpay-next/design/WB-38-build-scope.md` (BD084), the full approved WB-38 card at original `02_WORKBENCH.md:1359`, all eight checklist rows at `02_WORKBENCH.md:2202`, R2-F05 and the WB-38 R2 amendment. Policy X and the physical deliverable D06 remain the scope boundary. Official [Cloudflare Workers best practices](https://developers.cloudflare.com/workers/best-practices/workers-best-practices/) was read for the applied workers-best-practices skill; the findings above are grounded in local saved source.

This is source-contract evidence only. It does not establish installed SQL, live transport behaviour, joined in-app flows, performance or rendered screen acceptance. Those remain AUTHORED/NOT_RUN or with the owner's separately authorised evidence.
