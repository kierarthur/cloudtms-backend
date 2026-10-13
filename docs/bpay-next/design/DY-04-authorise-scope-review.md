# DY-04 — independent Authorise support scope assessment

11 October 2026. **Scope assessment: the minimal qualified read-only missing-used-rate refusal is necessary and authorised supporting work in the existing Authorise path.** This is approval of the narrow design boundary, not a PASS for unwritten implementation, installed behavior or joined acceptance. Only this review note was written. No product edits, database/hosted calls, tests, fixtures or commits.

Read `design/DY-04-build-design.md`, full DY04 card `PLAN/06_CONTRACT_MOVES_DAILY_PAYMENT_SUMMARY_PERFORMANCE.md:954–980`, all current DY04 checklist D01–D03, DY02 current investigation/source review, current generic Authorise0931, Daily canonical missing-rate owner, actual publisher refusal and existing Worker refusal parser. PLAN: `C:\Users\KierArthur\OneDrive - Arthur Rai\Documents\GitHub\cloudtms-backend\codex_outputs\claude-stage2-continuation-20260925\BPAY_NEXT_BUILD_PLAN_20261009_codex_r3`.

## Why this small support belongs here

DY02 expressly requires the actual used-bucket refusal, including genuine absent dated Candidate override/Client default; its helper-only correction deliberately leaves the proved Authorise/transport gap to DY04. DY04 provides canonical Daily Process → existing Office Authorise → ordinary publication and checklist **DY-04-D02 specifically names broker/src/index.js's existing authorise/materialise call path**. The card's Worker/Screen:none prohibits inventing another route/screen/protocol; it does not turn the explicitly named current call-path diagnostic into an unrelated feature. The missing-rate support is indispensable to the two connected clauses, not justification for a general authoriser rewrite.

Current `broker/src/daily-canonical-authority.js:17–25,161,207–210` marks actual used pay **or charge** NULL as RATE_MISSING (or retained UNPROCESSED with has_rate_issue). Current final `public.timesheet_authorise_generic_atomic(uuid,uuid,uuid,timestamptz,text)` in `10102026_0931_weekly_source_pay_query_outer_entry_admission_v1.sql:1753` locks the current Timesheet and TF; its status gate2145–2147 rejects those states before TF authorisation2200–2207. Therefore publisher1236:804–806 cannot deliver the requested bucket refusal on this genuine path. The approved-evidence helper also rightly rejects rate-issue provenance; calling it before Authorise is not a valid diagnostic alternative.

## Exact permitted seam and mandatory constraints

Add only necessary local declarations and one qualified read-only block **immediately before the original2145 processing-status gate**, after original current-row locks, expected identity/signature, archive/invoice/already-authorised and existing validation checks. Preserve the original gate text and all later mutations/response logic.

| Boundary | Required narrow behavior |
| --- | --- |
| Original owner and admission | NEXT only, using existing module authority. Keep early `private.weekly_source_pay_query_admit_v2()` and every Source/query/busy propagation call in the generic function byte-identical. No new `weekly_source_*` definition, producer repair or query protocol. |
| Factual Daily identity | Positively qualify current nonadjustment DAILY/HOURS, actual booking, current nonstale SELF_REPORTED TF, same physical Timesheet/version, factual Candidate/Client/role and valid stored channel. NULL Contract remains NULL; a real Contract remains factual. Do not broaden to Weekly, Source/imports, expenses or malformed/stale/unassigned rows. |
| Original state/refusal priority | The proved gap is the originally non-authorisable missing-rate state. Prefer RATE_MISSING, plus retained UNPROCESSED only with genuine has_rate_issue and missing used cell. Do not relabel unrelated blocked/CLIENT_UNRESOLVED/UNASSIGNED/stale refusal as rate missing. Do not change those states into PENDING_AUTH/READY_FOR_HR. |
| Actual missing bucket | Inspect the already locked TF's five actual quantities and selected pay/charge cells; first native used bucket in fixed DAY/NIGHT/SAT/SUN/BH order with quantity>0 and pay OR charge NULL. Preserve genuine zero and unused NULL. No resolver, calculator, reprocessing, money formula or post-Draft live lookup. |
| Definite existing error | Raise existing `BPAY_NEXT_ORDINARY_USED_RATE_MISSING` with bounded factual Daily subject/bucket/context. No authoritative success, authorisation stamp/status/amount update, new request kind or RPC. If no qualifying bucket exists, fall through unchanged original status refusal. |
| Existing Worker mapping | Extend only `bpayNextOrdinaryMissingRateRefusal` with a finite positively qualified Daily branch, before ordinary Contract guidance; retain exact error recognition, contracted UUID branch and existing handlers. Contracted Daily uses dated Daily authority too; do not tell it to edit Contract Standard rates. No preflight query or producer call. |
| Review/source protection | Copy only the exact latest generic Authorise definition, not the other definitions sharing0931's physical file. Signature, original search path, owner/grants and reload stay their current authority. Finite source-gate allowance must pin exact declaration/block spans and reconstruct the unchanged predecessor body; never waive the whole root/function or Source patterns. |

The generic function is a shared lifecycle entry, and its containing0931 file also owns query functions. **A narrow change to this one named Authorise definition does not authorise editing those other definitions or their admission semantics.** The parent/source reviewer must prove every non-Daily block and existing producer/query dependency unchanged against the current0931 function. The scope guard/F25 direction is preserved by that exact boundary; file-name coincidence is not permission to promote its whole capsule.

Suggested plain diagnostic purpose: name the actual Daily used bucket and guide staff to the dated Candidate override/Client default, then Process and Authorise again. No invented Contract, source authority or silent zero. Existing error details remain machine-typed and finite; raw code/UUID must not leak into Office copy.

## Alternatives and acceptance limit

A Worker-only preflight is **not an equivalent safe alternative**: it would read outside the existing SQL current-row/signature locks, risk a different physical TF/error priority, and add a read/route or payload authority without need. Relaxing RATE_MISSING to reach the publisher is forbidden because it would weaken authorisation eligibility. Calling the authorised-only evidence helper or a new resolver/calculator here is also unnecessary and cannot close the missing bucket honestly.

If the insertion cannot be isolated and predecessor-pinned as above, retain the original generic refusal and leave the named-bucket clause OPEN_DEPENDENCY_DY04; do not mark DY02/DY04 complete or broaden into Source/pay-query producers. A later revised, equally narrow in-lock design would be required. No such blocker is evidenced for this exact insertion.

Actual generic refusal/Worker single-and-bulk Authorise, both channels, pay-only/charge-only missing cells, unused NULL/valid zero, stale signature/current version, Source/query contention, contracted ordinary behavior and complete contractless publication remain future joined **AUTHORED_NOT_RUN**. This assessment does not execute them or claim whole DY04 SOURCE/runtime PASS.

## Inspected boundary hashes

| Current saved source/document | SHA256 |
| --- | --- |
| generic Authorise0931 physical root | `7fdddc57b5f1ca6aff196567a10dd09b393185b17efdfe9b58bb766aea7e847b` |
| canonical Daily calculator/missing-state source | `64cd2109da90f9ee277c2829a734683e7e06649ebebe14b1e2d296ac1c04d00b` |
| DY-04-build-design.md at this assessment | `a211c34ccf0e85de6b596e830af84ad7e1bbee7be2721f3466bc61922925ee69` |

These identify the design/parent inspected here; a final successor's actual changed bytes require their separate independent source review. No product/schema/producer/query definition was written or invoked by this reviewer.
