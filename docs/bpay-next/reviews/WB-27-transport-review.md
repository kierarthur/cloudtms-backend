# WB-27 Worker / Office estimate source review

11 October 2026. Independent source review only; root is the sole product writer.

Final saved-source verdict: **PASS. The P2 UI completion edge is closed; no essential open transport/UI defect remains in this reviewed scope.** No allocation, authority or native-money defect was found in the Worker/FE additions. Native Source/book/version completeness has its own SQL review. The original finding and its final closure are retained below.

## Scope and sources

Read the full approved WB-27 card, every checklist row, R2 F01/F03, and `docs/bpay-next/design/WB-27-build-scope.md` including final BD081. Reviewed the actual backend `broker/src/bpay-next-workbench.js`, frontend `js/bpay-next-workbench.js`, its existing Office wiring, and the response/continuation branch in `supabase/repeatable/11102026_0207_banking_pay_next_candidate_estimate_reader_v1.sql:189–216/563–625`. Read the shared private plan and corrected preparation successor separately; their source approval is in `WB-27-plan-prepare-review.md`.

## Actionable finding

**P2 — a late estimate callback can leave the returning Candidate controls disabled and its state stuck Updating.** FE `showEstimate` tests `frame.active()` after each await (line 1407), but starting an estimate does not block existing Open/Pay lines child actions (1474–1475). If a child opens before the estimate reply, the request exits through the inactive-frame fence. Its `finally` renders the still-alive inactive Candidate (1426), which disables its tick/Show estimate controls. Candidate `onMount` at 1505 only appends the title identity; closing the child does not render those controls again. The same path leaves `estimateState='UPDATING'` after `estimateBusy=false`. Narrow closure: render when the Candidate remounts and settle a same-ticket abandoned Updating request to an honest nonbusy state. No fetch or intent write is required. Root was notified before this review was saved.

## Findings supporting the remaining source

- One named read kind stays on the existing authenticated GET and fixed service RPC. Closed raw descriptor/context/version input contains no browser monetary E/H; ordinary kinds reject non-null estimate input. Admin/actor/environment, unique JSON/query keys, streaming byte limits, abort/timeout and service body checks remain (Worker 5–12/417/464–480/522–538/638–663).
- ESTIMATE/OUTDATED/PENDING are explicit and closed. Non-estimate states must be complete and contain no money, rows, estimate scope or continuation. Estimate responses compare exact descriptor, financial operand, complete native version array, prior context, tuple ordering and page receipt. Zero-Case completion has an explicit verification cursor (Worker 470–480; FE 471–481).
- Both schemas now measure the real endpoint and full encoded continuation. SQL refuses an oversized full request conservatively before offering continuation and keeps its 120000-byte response budget. No truncation, larger envelope, digest, secret, token or preview storage was introduced.
- FE freezes the original resolved local descriptor with its original namespace/memory, while narrowing the read to one Candidate. It captures the local selection revision, memory reference, context and estimate ticket; checks them after every await; and rejects duplicate component identities across its bounded Map (1401–1417). Tick handlers do not request estimates.
- No page is adopted early. The walk compares complete page context and all totals, then sends a fresh same-reader `verify_only` call with the terminal native cursor and full original versions. Only after exact final agreement are result and per-Case rows adopted (1418–1423). Closing invalidates the ticket and drops rows (1505). SQL rederives the whole bounded scope before terminal validation; an empty suffix does not bypass freshness.
- The earlier Updating problem is closed: starting a new request clears old result/rows and refreshes current detail first (1406). Selection changes retain a visibly Outdated prior estimate without a network request. Native ROW/detail amounts use returned decimal text and original cap labels; rendering formats them without calculating headroom or allocating money. Detail now references its native `member`, and cap text preserves the required weekly-limit content (1296–1300/1365/1451–1473).

Generic bounded version tuples remain comparison operands, and generic decimal/code validation relies on the authoritative SQL plan for native penny/reason semantics. These are not identified as essential failures or authority extensions. No additional proof framework is recommended.

## Evidence limits

No test, syntax execution, DB connection/SQL execution, browser/screenshot, fixture, hosted change, product edit or commit was performed. The P2 path is a direct source/control-flow finding. Actual captures, DRP P05 page-size equality and joined Draft/estimate acceptance remain pending. The final source closure below does not claim runtime evidence.

## Final source closure — PASS

Independently re-read the actual saved FE corrections on 11 October 2026. Candidate `onMount` now appends identity and renders when returning (1505). Same-ticket estimate `finally` changes an abandoned UPDATING state to OUTDATED before clearing busy and rendering (1426). These close the inactive-child callback/remount path without another read or selection write. Starting a new estimate still clears old result/rows and refreshes detail (1406). Detail uses its actual native `member.case_component_id` and appends the estimate cap explanation beside the amount, preserving the existing weekly-limit/minimum-earnings/take-home content (1296–1300). Existing actor/window/ticket/revision/reference/context fences, final fresh-read adoption and no-per-tick-request behaviour remain. **The recorded P2 and old-Updating-money issues are closed.**

Also reviewed the actual diff and surrounding checks in `scripts/bpay-next-source-gates.mjs`. The gate now adds the exact `private.bpay_next_case_allocation_plan_v1(jsonb)` operational signature (43), its finite named read callees (120), and the actual plan/current-read delegates in the existing public reader and prepare call sets (131/226). The only history qualification expansion gives that exact plan signature the same original literal FUNDED-event identity/time point clause moved from prepare (670–671). Remaining helper delegation and history relation scans still fail closed (647–699); no broad suppression, unchanged-file waiver, disabled hazard or dynamic delegate was introduced. The other visible diff changes preserve existing native type classification and are line-ending normalisation. This is source review of the gate, not a claim that this reviewer ran it.

**Final result: PASS for the named plan/read-callee gate expansion and the Worker/FE estimate transport/UI source scope. No essential open finding remains here.** Native money/input coverage, compilation, actual screenshots and later app acceptance retain their separate evidence boundaries.
