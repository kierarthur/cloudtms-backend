# WB-01 authorise missing-used-rate interface investigation

10 October 2026. Read-only source investigation of BUILD-BE and BUILD-FE. Read the full WB-01 card `02_WORKBENCH.md:84–119` and exact D10 checklist row. No product edits, tests, SQL, data, runtime, fixtures, remote or secret operations. Application acceptance remains NOT_RUN. Next master item is WB-03; this note does not build WB-02.

## Required finite behavior

WB-01 preserves the existing genuine authorisation refusal `BPAY_NEXT_ORDINARY_USED_RATE_MISSING`; it adds only missing bucket/Contract detail, a typed409 transport and the existing Timesheet/Contract-rates presentation. No Banking missing-rate decision kind, zero-valued publication, new route, financial owner or automatic retry is required.

Current final publisher body in `supabase/repeatable/09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql:302–321` iterates genuine used DAY/NIGHT/SAT/SUN/BH rates; the actual refusal is SQLSTATE23514 with the fixed message and currently no DETAIL. The named DB owner successor should add genuine native bucket and Contract id at that existing raise. The current generic SQL wrapper0931 ends with bare RAISE for this error, so its original DETAIL survives; no wrapper/money-body rewrite is needed to transport it.

Recommended minimum trusted packet: exact code `BPAY_NEXT_ORDINARY_USED_RATE_MISSING`, finite bucket `DAY|NIGHT|SAT|SUN|BH` and genuine native Contract UUID. A fixed bucket-label map is Day/Night/Saturday/Sunday/Bank Holiday. Do not derive the Contract from current modal state or turn arbitrary23514/text/timeout into this refusal. If the DB DETAIL includes a code, keep it exact; otherwise the Worker must inspect the exact parsed original DB message while extracting its typed DETAIL.

HTTP409 includes `{code,bucket,contract_id}` as required; existing standard error_code/message envelope fields may accompany these when needed by actual callers. The fixed copy is:

> Pay rate missing for <bucket>; set the rate on the Contract, then authorise again.

The refusal is definite business rollback, not BUSY, stale, timeout, a reconciliation opportunity or an invitation to repost automatically. Unknown/lost response paths retain their original reconciliation and pending behavior.

## Current Worker seams and gaps

All positions refer to current saved `broker/src/index.js`.

| Actual seam | Finding / smallest required support |
| --- | --- |
| `handleTimesheetAuthoriseGeneric`, entry167752; `parseRpcFailure`167790–167811 | Parser already retains original payload.message and JSON DETAILS. `rpcErr.code` commonly contains23514. The later catch passes `rpcErr.code || rpcErr.message`, so a new mapper cannot depend only on its rawCode argument. Normalize this one exact DB refusal from parsed message + typed detail before generic handling; do not make23514 itself a classifier. |
| Single Timesheet `mutationErrorResponse`168052; RPC catch168548–168679 | Existing fixed Source refusals are handled here; missing-used-rate currently falls to UNKNOWN_LIFECYCLE_OUTCOME409. Add the exact finite typed branch before that fallback. Place known refusal recognition before unknown-outcome reconciliation when examining a caught RPC error; retain the existing unknown predicate and actual reconciliation for everything else. The returned-payload failure at168706 should use the same finite mapping if the original DB owner supplies a failure object. |
| Contract-week manual authorise `mutationErrorResponse`169867; direct RPC catch170203–170213 | Same missing branch; current fallback is generic server error. Original parser exposes genuine message/detail here too. Apply the exact typed packet before generic fallback; preserve required admin auth, original routing/Source first-authorise branch and native expected Timesheet/row-signature checks. |
| `callGoldTimesheetLifecycleActionForBulkItem`93948; result construction94061–94087 | Ordinary selected-row authorisation calls the actual single handler. Its return object currently copies status/error_code/message but **drops bucket/contract_id**. Copy only this validated refusal's typed fields. Do not infer them from the selection or copy arbitrary raw RPC details. |
| `summarizeTimesheetLifecycleBulkOperation`94260–94302 | Durable operation item.result_json is reread but the public summary again constructs a reduced row and **drops bucket/contract_id**. Preserve the exact validated fields here too, so poll/readback retains the same refusal/link. Existing result_json persistence needs no new table/command/protocol. |
| `handleTimesheetLifecycleBulkActionRequest`94431; single row94501–94530 | The single-selected Gold result is already placed directly in results/failed_items; the finite Gold copies above suffice. Preserve current response totals/status and success/failure semantics. |
| Same bulk owner, correction-pair branch94478–94496 | This actual branch directly calls `timesheet_authorise_bulk_atomic`; its non55P03 SQL exception is bare rethrown by physical `14082026_1310:2511–2519`. The current Worker call has no local typed catch, so this genuine refusal can reach the outer generic error path. Catch/map only the exact missing-rate packet here; leave the pair transaction/permission/confirmation/order unchanged. It is one atomic refusal, not manufactured per-member partial success. |

Current transient retry helper `callTimesheetLifecycleRpcWithTransientRetry`95116 distinguishes retryable contention from business errors and rethrows a nonretryable original error. Do not add this rate code to transient/retry sets. A lost response after an earlier attempt remains unknown until its existing reconciliation proves an outcome. Preserve unknown handling and nonce/body/row-signature identity; a typed refusal for an actual subsequent attempted RPC can be recognized without misreporting an earlier unknown attempt as rolled back.

No new global Banking owner check or F-15 preflight is required to label a validated code generated by this genuine NEXT authorisation owner. This is different from SP-03's two context-dependent Source labels; keep those F-15/actor checks intact.

## Current Office seams and gaps

All positions refer to BUILD-FE `js/main.js`.

| Actual caller/presentation | Finding / smallest required support |
| --- | --- |
| Existing helpers5078–5113 | `weeklySourceMapAuthoriseError`/result presentation recognize only Source busy and Additional-unmapped. Keep those rules unchanged. Add a separate finite missing-used-rate parser/presentation or a clearly separate branch, preserving the original Error.json/status/body and initiating actor. Validate code, finite bucket and native UUID before constructing copy/link. |
| `authoriseTimesheet`314646; no-result error315190–315218 | Actual error is passed to `tsHandleLifecycleReconcileModal` before final throw. Handle this definite typed409 before that call, so the missing-rate message/link remains visible and it cannot be relabeled as an unknown update. |
| `tsHandleLifecycleReconcileModal`276654–276711 | Its shouldHandle includes **any refresh_required=true**, even when the code is not in its controlled stale/unknown set. Therefore copying SP-03's refresh_required=true response shape into this new refusal and relying on normal fallthrough is unsafe. A finite known-refusal early bypass/presentation is required; changing general reconciliation policy is unnecessary. |
| Footer Timesheet Authorise catch334710–334727 | Current final UI only alerts Error.message after moved409 handling. The bucket sentence alone is insufficient: this actual surface needs the Contract rates action/link. Reuse a shared known-refusal presenter, returning handled:true/definite failure without success toast or automatic authorise. |
| `contractWeekAuthorise`315687; catch315786 | Uses the real manual-authorise endpoint and maps Source errors only. Preserve its in-flight key/finally, expected Timesheet/signature and ordinary success refresh; present the same finite missing-rate refusal/link in its actual catch/upstream UI. |
| `authoriseSelectedTimesheets`196837; normalizeResult196910; arrays197013–197014 | Normalizer spreads native source rows and failed_items are cloned, so they can retain validated bucket/contract_id once the Worker copies them. Add finite fixed copy to both results and failed_items, including durable operation poll output. Do not treat missing rate as Source BUSY/Waiting or modify success/failure counts. |
| `handleBulkAuthoriseSelected`260853; summary261220–261249 | Current summary is plain text and default completion modal only says OK. It can show the bucket message, but has no navigable Contract link. Retain a finite list of validated missing-rate refusals keyed by their native row/Contract; expose actual Contract rates actions in the existing summary/error surface. Multiple refused Contracts need their own typed actions, not one inferred active-row link. |
| `renderBulkAuthoriseShell`256260, summary area256662–256666 | Existing summary_message is HTML-escaped plain text. Add explicit safe DOM/button/anchor markup beside that summary for validated UUIDs, with scoped click wiring; do not turn raw summary text/DB reason into HTML. Preserve actual state/frame lifetime and user navigation. |
| `runBulkTimesheetSelectionChunks`18720 onward | Uses the same authoriseSelectedTimesheets executor and clones per-row result arrays. Preserve typed refusal metadata through its aggregate/failed_items; its actual multirow progress/results path must also offer the relevant Contract rates actions. Do not map one structural unknown error to every row or automatically retry the chunk. |

The current Source helpers already capture the initiating requestActor at entry. Reuse that actor for this new UI seam, with exact originating frame/state guards before displaying late refusal and before/after the optional Contract read. Never open a Contract for a different logged-in actor or an unrelated frame after async completion.

## Actual Contract rates navigation — existing APIs only

`getContract(contract_id)` at115101–115118 performs the existing authenticated GET `/api/contracts/:id`, returning the full Contract wrapper. `openContract(row,openOptions)` at298296 consumes that genuine wrapper; it already defines Rates tab key `rates` at298643 and calls `renderContractRatesTab` at298663. Current frame exposes `setTab('rates')`. Existing openOptions has no initialTab/rates option; do not pretend one is supported or invent a URL/deep-link route.

The smallest deliberate link/action is:

1. On the user's Contract-rates click only, retain the validated refusal Contract UUID, initiating actor and original parent/frame. Respect existing parent/discard-change gating.
2. `await getContract(contract_id)`; require same actor/frame and actual returned Contract identity before opening. If unavailable, keep the typed refusal and show a read error; do not substitute `{id}` or another Contract.
3. `openContract(freshWrapper)` with normal existing modal gating. Capture/check the resulting actual contracts frame and exact Contract identity, then `frame.setTab('rates')`.

The existing `openUiConfirmModal`143001 supports plain escaped message/extra_html and `on_open`143316; a small dedicated refusal presentation can reuse it with an explicit “Open Contract rates” action or scoped actual link. Bulk summary links belong in the existing summary/progress surface. All actions read/open a screen only. No rate mutation, authorise retry, provider check or new endpoint is dispatched by the link. User fixes the Contract and deliberately authorises again through the original control.

## Saved source checkpoint

| Source | SHA-256 |
| --- | --- |
| BUILD-BE `broker/src/index.js` | `e0a9e2cf4a6df036c3b4123fba6d193e600a30a9abdd0ac25026a58979092e68` |
| Foundation2018 ordinary publisher | `7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b` |
| Outer0931 generic wrapper | `7fdddc57b5f1ca6aff196567a10dd09b393185b17efdfe9b58bb766aea7e847b` |
| Current bulk-authorise physical1310 | `f19f780febcc8c1b74a492a1f9ac2bb80c10f5a936ed866b667da267b86db28b` |
| BUILD-FE `js/main.js` | `b920c598768eb6c68cffc198b25cc31d80775d534759fafd96f3168c50f2d057` |

Read current bytes again before edits/review. This note is caller/interface design only; the publisher's rate/policy/notification deliverables and eventual J1a/J1c/J2b application checks require their own evidence. No app acceptance, transaction contention or missing-rate fixture was run.
