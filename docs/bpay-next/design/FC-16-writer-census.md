# FC-16 writer census and protection boundary

11 October 2026; sequence 100. Independent static source investigation only. Product, checklist, database, business rows, tests, runtime and deployment were not changed or invoked. This is a writer/lock-order census, not FC-16 acceptance.

## Controlling scope

The complete [FC-16 card](<C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/cloudtms-backend/codex_outputs/claude-stage2-continuation-20260925/BPAY_NEXT_BUILD_PLAN_20261009_codex_r3/03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:1372>) requires protection of accepted PREPARING Case inputs **before** holds, plus ACTIVE READY/DRAFT slices. Future unreserved authority may change; captured choices, rule/period openings and confirmed Draft money may not. Whole-Case success must not hide an outstanding protected slice. Eligible removal uses DR-05 WORKER_RELEASE; confirmed payment requires future EX-27 cancellation, without an FC-16 dependency on or call to EX-27.

All nine checklist rows were considered: [IMPLEMENTATION_CHECKLIST.md:2945](../IMPLEMENTATION_CHECKLIST.md#L2945). D01/D02 retain 0310 enforcement; D03 retains the current allocation owner; D04/D08/D09 own the new change-guard repeatable, bounded references and assertion; D05 reuses WORKER_RELEASE; D06/D07 supply output/schema context. The master order is FC-16/100, FC-03/101, FC-09/107, FC-10/108, FC-11/132, FC-12/133, FC-13/134. Those future commands must wire the assertion when their owners are built; this census does not implement them early. R3:452–460 expressly adds accepted-before-hold protection. R2:270 keeps accepted channel decisions frozen; FC-16 must not resolve a new channel or reprice original instructions.

## Actual current mutation owners

I scanned registry-selected final private NEXT function statements for direct INSERT/UPDATE/DELETE of finance_case, case_rule, case_component, case_period, case_hold and case_capacity_use. Exactly eight functions write these tables; no DELETE appeared in that current census. Historical competing definitions, parked references and future-card proposals are not current owners.

| Current owner / physical source and line | Actual mutation | Required FC-16 treatment |
| --- | --- | --- |
| apply_case_create_v1 — 0330:186,223–258 | New Case/rule/components/initial periods and APPROVED event | Fresh creation, not an existing protected-Case amendment. Retain original ordered admission, immutable request and creation authority. |
| apply_write_off_v1 — 1647:268,304–338 | Existing component written-off amount, Case principal_written_off/status/revision; WRITTEN_OFF event | **Current logical amendment requiring the assertion.** Recheck before the event and both counter writes; accepted PREPARING membership must be considered even with zero active holds. |
| prepare_case_page_v1 — 0904:77,333–397,473–499 | Opens qualified period, exact funding-only period qualification, automatic resolution; allocates original instruction/result and ACTIVE hold/use plus counters | Preserve current accepted-cut derivation, live locked balances and 0204/0290 one-plan/kernel. Its own reservation must not be rejected by a generic change refusal. |
| apply_case_paye_net_page_v1 — 0559:2988,3160–3257 | Replaces its exact prior DRAFT/NET recovery reservation with a new result/hold/use; matching component/period/Case counters | Native frozen projection owner. Keep own-hold exclusion and exact release/replacement authority; no logical Case edit or new rate/channel resolution. |
| post_case_transfer_member_v1 — 0410:352,414–522 | Realises exact hold/use; funding/paid-credit/recovery counters and event; first-funding successor rule and exact zero-period qualification | Native settlement/funding owner. Continue captured instruction/result amounts and original event. Funding successor at 449–454 and zero-period qualification at 464–483 are not an arbitrary edit of consumed history. |
| cancel_case_worker_page_v1 — 0559:3882,3935–3945,4018–4063 | Exact confirmed-worker cancellation release, with original run/worker/binding fences and component/period/Case decrements | Existing lifecycle cancellation, not permission to change a confirmed Draft through a new Case command. Preserve native scope; FC-16 calls no future EX-27 object. |
| release_prepare_case_hold_v1 — 0814:727,756–794 | Shared original expiry/discard/worker-release exact hold/use release and matching decrements | Preserve existing native release scope and exact frozen tuples. Do not deny release merely because this same hold is protected. |
| reconcile_work_collection_v1 — 0814:885,1134–1189 | Creates one automatic Work debt; corrects current principal/component resolution or closes it under native position/source/settlement/release ownership | Existing lifecycle/current-authority correction, not a user edit command. At 1162–1179 it retains realised R, written-off W and protected H and changes future resolution only. Do not block original lifecycle writers or rewrite their money formula. |

Exact final physical paths are in the provenance table below. The pure case_allocation_plan_v1 (0204) and case_allocation_row_v1 (0290) do not write these tables and need no new protection/refusal branch. Case selection, immutable instruction/result/event guards are supporting capture ownership, not new financial amendment owners.

## Concrete missing logical protection

Current accept_write_off_v1 (1647:24–89) captures the component/Case basis under Candidate → Case → component locks, retains exact replay first, and creates an ordered request. Current apply_write_off_v1 checks fresh revisions and active_recovery_hold_amount, then writes the event and counters. At 320–323 it refuses amounts above remaining minus active holds. **This does not cover an accepted selected PREPARING Case component with no hold.** FC-16 must wire its native assertion before that mutation, rather than declaring 0310 sufficient.

Application must retain the existing request's no-repricing semantics: stale basis remains REVIEW; exact command replay remains original. A protection check at admission is useful disclosure, but does not replace the fresh application check after earlier ordered work. No current pause/edit/restructure command owner was found in the selected final writers. The later FC-09/10/11/12/13 owners must call this same protection boundary; do not add their command intake or UI now.

0310 case_hold_guard_v1:211–247 protects typed ACTIVE insertion and immutable ACTIVE → RELEASED/REALISED transitions. case_capacity_use_guard_v1:249–281 binds use amount/status and realisation event to its hold. case_rule_insert_guard_v1:104–118 qualifies funding evidence. These do **not** aggregate protected capacity or identify accepted-before-hold membership. Preserve their original tuple enforcement and supplement the actual logical writer; do not replace them with a broad bypass flag or blanket trigger rejecting legitimate capture/post/release.

## Lock order and safe release seam

* Write-off application: module SHARE → Candidate control UPDATE → job/request UPDATE → native position UPDATE → Case UPDATE → component UPDATE (1647:278–305). Its read-only protection summary/assertion must not acquire a protected run header after Candidate/Case. Reading existing frozen protected references under the original serialization is distinct from invoking release.
* Prepare Case: module SHARE NOWAIT → run header UPDATE NOWAIT → Candidate control UPDATE NOWAIT → job → worker → DRAFT allocation state; then worker-week/channel and Case → component → period (0904:122–131,257,333–338,473–476).
* NET: run header → Candidate → job → worker → NET state/worker-week/channel; exact own hold and Case/component/period under native Candidate serialization (0559:3020–3026,3065,3142,3160,3184–3187).
* Settlement helper itself takes worker → Case → component → period → hold → capacity-use (0410:404,414–420), within its existing qualified lifecycle caller/lease. Shared release helper takes Case → component → period → hold → use (0814:756–760), after its original parent release scope. Do not invent a new run lock inside either helper.
* WORKER_RELEASE admission already locks the **run header before Candidate** (0814:3605–3609). It sets run PREPARING and increments review_revision **before any hold mutation** (3699–3704), then the original finite release/project phases finish. Its later pages require that exact unconfirmed PREPARING header (3746 onward).

Therefore the narrow integration is an honest protection result followed by the existing separately ordered whole-Candidate WORKER_RELEASE with its exact native worker/preparation identity. Do not call that action recursively from a Case assertion while holding Candidate/Case locks, auto-release only one Case hold, or remove a READY hold while its run remains confirmable. After actual release completes, a new Case intent rechecks current authority. Confirmed DRAFT protection is refusal and the named future cancellation next step; no FC-16 financial mutation.

The current original Create also locks Candidate/Case-component authority before accepting membership (1358:184–225); accepted member/header storage is immutable. In particular, its component SHARE conflicts with logical component UPDATE: the final assertion must read accepted membership freshly **after** that lock is acquired. A STABLE read that retains the outer statement snapshot from before waiting on a concurrent Create could miss the newly accepted protection; a fresh native reproof is necessary at the write seam. Do not acquire the Create/run header after the component lock. The implementation must use that actual accepted membership predicate, not an unused accepted-input table or browser amount. Root/reader investigation owns its exact bounded membership/page design. Current economic Case revision remains distinct from mutable execution revision; retained retry must not look up a fresh logical Case rule.

## Minimal implementation conclusion

Add the card-named private bounded reference reader/assertion and wire **current write-off application** plus any currently reachable logical authority owner demonstrated by source, preserving admission/replay and original mutation vocabulary. Keep the seven other actual writers under their existing native lifecycle/capture fences; expose any necessary aggregate-safe assertion as an exact native owner qualification, not a new money owner or arbitrary GUC exemption. Future command wiring is assigned to FC-03/FC-09–13. No new producer, table, command kind, allocation formula, general framework, EX-27 call or screen is justified by this census.

The bounded output must distinguish accepted protection with no allocated hold from exact allocated money. A not-yet-allocated protected component is not evidence of £0 capacity. Reader scope and canonical amount treatment need the reader investigator's contract; do not calculate an estimate in the writer guard.

## Saved-source provenance

SHA-256 of exact saved bytes used for this census:

| File under backend worktree | SHA-256 |
| --- | --- |
| docs/bpay-next/final-definer-closures.json | 60ac256660798807f2ad55fd3db990b1bbade96f5cafdb495c1dca0d4bf12d26 |
| supabase/repeatable/04102026_0310_banking_pay_next_case_guards_v1.sql | 7dd5085d3307a0194b3745487cc19a952ee3b8cb11d03f096d275d23632500fe |
| supabase/repeatable/04102026_0330_banking_pay_next_case_creation_selection_v1.sql | 9dc328ef5fb4e172622f72a97ce3cb84184740c5fe2575eb67f8bf1dac5fe903 |
| supabase/repeatable/04102026_0410_banking_pay_next_case_transfer_post_v1.sql | f56898dc7e992fc271d2a90c0c7fdb85d77dde4bac5e8b852e008c0da90793d8 |
| supabase/repeatable/04102026_1647_banking_pay_next_work_collection_write_off_v1.sql | 77019761e500193ddd5d8c4147aa0db5bf389c2d1deb02d576407548c0ed4a87 |
| supabase/repeatable/11102026_0559_banking_pay_next_worker_reprepare_v1.sql | 514fb6a069dfe860d06c76d98302554bf3ed93e8e027cdbac460bf8cea7da9a6 |
| supabase/repeatable/11102026_0814_banking_pay_next_preparation_discard_v1.sql | 29f3099ebfb8a7e11d8c23a8996c5351eb9330316d8bdb6fd6d3a5136d3f379c |
| supabase/repeatable/11102026_0904_banking_pay_next_early_candidate_financial_fence_v1.sql | fa3c73a83075f67246e62adea7ba1467c019bada601d3db880ec51b48cdd1a9e |
| supabase/repeatable/11102026_1358_banking_pay_next_guarded_create_v1.sql | 6d13cd0a482be586e49aaeb2fb67bca82613aa092657cbcc208e6f012acbc0a6 |

Static census and the narrow integration recommendation are complete. Implementation, compile/source checks and future joined scenarios remain with the author; none was run or approved by this note.
