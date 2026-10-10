# F-25 Candidate submission-completion tail

10 October 2026. Bounded saved-source investigation only; no SQL, runtime, tests, fixtures, hosted operations or product edits. Root alone implements. This refines the Materialise/comparison closure boundary and does not mark F-25 complete.

The real `public.weekly_source_timesheet_submission_complete_atomic_v1(jsonb)` is at `supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql:388–640`. Its only saved executable application SQL caller is Materialise at `15092026_2203_weekly_source_candidate_app_contract_v1.sql:2725`; verification SQL also calls it, and the ordinary service-only ACL lists it. No saved JS/Worker call or alternative repeatable definition appeared in the bounded census.

**The call does UPDATE one of the nineteen relations (`weekly_discrepancy_incidents`), but its actual updates are canonical tuple no-ops.** It does not produce additional material PAY_QUERY events under the current finite inventory. The entire call still has real business locks and writes and must finish before Materialise's clock/immutable certificate. Finishing the parent at the comparison return would be premature.

## Exact canonical/no-op disposition

The current incident tuple is candidate/client/work/source cycle/group/state/current comparison (0017:848–853). Identity checks precede the exact tuple no-op return0017:854–857; producer/top-owner staging occurs only afterward. Parent canonical relation allowlist2357:95–102 contains no submission request/membership, Candidate outreach generation, manager route/generation/membership/message or audit relation.

| Original tail statement | Actual write | Canonical disposition |
|---|---|---|
|1534:513–518|Submission membership WAITING to SUBMITTED_MATCHED/SUBMITTED_WITH_ISSUES; submitted Timesheet ID/version/hash and completion timestamp.|Outside the nineteen relations; preserve exact original authority.|
|1534:521–530|Existing OPEN incident `candidate_action_state='NOT_REQUIRED'`.|Incident relation is allowlisted, but none of its canonical fields changes; exact tuple no-op.|
|1534:542–556|Existing qualified OPEN incident `manager_action_state='DUE'`, preserving already accepted manager ownership and covered-hours filters.|Same tuple no-op; no incident state/current-comparison change.|
|1534:557–565|MANAGER route activation INSERT ON CONFLICT DO NOTHING.|Outside inventory.|
|1534:578–588|Submission PARTLY_SUBMITTED/COMPLETE; completed outreach generation; retire DUE/RENDERED message intents.|Outside inventory.|
|1534:593–624|Manager generation/intent work, due-event consumption and actual intent ID result set.|Outside inventory; helper qualification below.|
|1534:626–634|Original completion audit.|Outside inventory; no invented query event.|

Current `F-25-core-query-writer-census.md:47–48/95–96` independently classifies both1534 incident statements as irrelevant/no-op. No new material comparison INSERT, OPEN→RESOLVED incident transition, pointer replacement, protected qualification or financial Source relation is performed by this owner.

The ISSUES branch calls `private.weekly_source_query_cohort_ensure_v1` at531. Its actual4018–4096 body reads original policy, ensures Candidate cohort and manager recipient route, and returns the original routing flags. These writes are outside the inventory. The later manager-generation helper3589–3902 locks the actual route/current generation, retains actual membership/accepted-batch semantics and updates manager cohorts/routes/batches/receipts/items/due events/message dispatch/renders. Its only incident UPDATE3793–3800 changes manager_action_state/waiting_source_state/reconciliation_state under `p_trigger_kind='NEW_INCIDENT'`; this caller passes **OFFICE_DIRECT** at593–595, so that branch is not reached. Even that specific other branch would leave the current incident canonical tuple unchanged. Manager-intent3904–3958 validates actual route/generation and creates/reuses an exact original logical message intent; it writes no canonical relation. These are durable database intents; this call does not execute network email/provider effects.

## Real locks, inherited owner and true tail

Original service/shape/header parsing stays419–433. The first business lock is submission request FOR UPDATE434–435. The exact original COMPLETE replay439 returns before current-publication validation and generation/member writes. Preserve this original Candidate/request/state replay; it is not a full immutable historic reply receipt.

Fresh authority then checks current publication443, locks the actual active submission outreach generation444–450 and, in native membership UUID order, locks each WAITING membership464–468. It proves the actual signed current HOURS/WEEKLY Timesheet scope/version/contract/week/signature469–476, canonical signed hash480–485, legal outcome486–489 and exact whole-week OPEN-issue population490–511. The selection/completed count equality573–574, remaining-WAITING request branch576–589 and all manager/outreach work590–625 are necessary parts of the original successful operation. Manager generation obtains route/current generation business locks3620–3626; no such work may run after the agency clock.

For actual SUBMIT_TIMESHEET Materialise, the existing caller already locks the exact submission2710–2716 and WAITING membership2717–2720, executes the comparison2721–2724, then calls this full completion body2725–2736. The admitted parent remains CANDIDATE_MATERIALISE, directly or nested under the actual APP owner; the compare child has left back to that parent with input OPEN/BUSINESS_OPEN. This no-op canonical tail needs **no second producer, nested QUERY_SYNC frame, private completion kind, metadata command or fresh Source/module admission under these business locks**. Keep the existing public completion declaration/body and service/validation/replay semantics.

Materialise must retain its producer until after this call, full result construction2738–2746 and final original workflow response UPDATE2747–2748. Insert/assert actual Materialise completion only there, then leave to the real APP parent or finish the direct MAT root after its own adjunct. SELF_SUBMIT/CHECK_HOURS do not take this SUBMIT_TIMESHEET compare/completion branch under the original2709 condition. Do not add it to SELF merely because a generic stack enum permits that owner.

The comparison certificate's range ends at the original comparison1905/result tail. Materialise's encompassing range ends at its actual later workflow response tail. Under these actual writes the later submission-completion call adds zero canonical ordinals; equality of the two range endpoints does **not** prove the business tail ran. The sole private actual-owner certificate capture after2748 provides that completion authority. If a materially unexpected event appears in that later range, the parent whole-stage census must refuse/classify it explicitly rather than hiding it with a no-op assumption.

## Immutable authority and honest replay limit

This public completion function has no immutable whole-request receipt, request semantic hash or immutable per-member submission-completion event. It updates mutable submission/membership/generation state and returns the original result635–638 (including actual manager intent IDs). Original audit evidence does not provide a returned immutable whole-request business receipt. Do not synthesize one from today's COMPLETE request, current membership/Timesheet or a count of audits.

Actual comparison evidence has its own genuine immutable comparison/recheck/resolution originals and subordinate protocol certificate. That certificate alone does not prove the later submission-completion callback. The fresh Materialise certificate/result SHA must bind the actual unchanged `v_result`, which includes both original `comparison` and original `completion` operands at2744, and may be authored only at the true tail after2748. Existing APP FINAL_SUBMIT immutable receipt remains the true APP business authority. The new private protocol adjunct proves the actual parent lifetime without replacing the original public response/replay contract or pretending to reconstruct old callback replies from mutable tables.

## Saved source hashes

- Current1534 delivery: `dcfa67862e75cff20e06d0694910ea603d329f3f0dc20fd351fca07cf598332d`.
- Actual2203 Materialise caller: `2f28f6b60ef929617e2a2e5075e0b63875e21d0eea096d9ab63bcc501e3840d6`.
- Current core writer census: `487897c5341efa1e700cf013ec8e968249b85724dc48ddc50ce37d3af726f9e4`.
- Incident staging0017: `08c95f0067da9f7d9d9ef3277584102011f75d73d6761bd9d6a8a0fb2ed2e270` (prior bounded review).
