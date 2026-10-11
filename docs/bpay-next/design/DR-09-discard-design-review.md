# DR-09 whole-run discard: independent design review

11 October 2026. Read-only design/source review. Only this note is written; no product edits, SQL/database execution, tests, fixtures, hosted/browser actions, commits or checklist changes. This is supporting investigation for the already-approved card, not a new design-first gate. Application acceptance remains **NOT_RUN**.

## Scope and recommendation

Implement the card-named ordered **PREPARATION_DISCARD** through its own private root request, finite Candidate bindings and own native job lane. Reuse the corrected existing release mutations and bounded projection pattern. Do not fan out artificial DR-05 worker requests, borrow PREPARATION_EXPIRY's request shape, weaken its deadline, or introduce a general cancellation framework.

Authority is the full ordinary approved card `03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:249–272`, all local checklist rows `IMPLEMENTATION_CHECKLIST.md:2529–2551` (owning D03/D04/D06/D07; D01/D02/D05/D08 context), RQ-115/RQ-118 in plan `evidence/REQUIREMENTS.md:168/171`, and RQ-312's empty/resumable/group protection clause in `evidence/CRITIC_REQUIREMENTS.md:27`. Master `08_BUILD_ORDER_AND_CODEX_INSTRUCTION.md:169/470` places this at sequence85, after DR-05/DR-06/WB-08/F-16/DR-08 dependencies. Confirmed Draft cancellation remains EX-27; no provider/CSV execution expansion follows from this card. The freeze boundary remains the original accepted Create cut (`09_OWNER_ANSWERS_BINDING.md:12–20`); discard changes reservations and status, not captured inputs or prices.

The current `docs/bpay-next/final-definer-closures.json` selects0559 for DR-05 and the public financial service,0746 for corrected expiry release/projection/guard/progress/Confirm/Case-release, and0708 for public preparation intake/page. The historical D01/D02 TEST ledger does not choose a successor parent.

## Why a new typed binding is smaller than WR request reuse

The existing `bpay_next_worker_reprepare_request` requires a non-null worker, original/input/previous PREPARE jobs, positive old/new execution revisions, and target=expected+1 (`supabase/migrations/11102026_0559_banking_pay_next_worker_reprepare.sql:27–85`). Its action guard requires PREPARE command/job ownership (`supabase/repeatable/11102026_0559_banking_pay_next_worker_reprepare_v1.sql:215–244`). Manual WORKER_RELEASE requires original full SEALED enrollment and an existing DONE job, then READY/REVIEW worker (`0559:96–119`). It cannot express an original member not yet enrolled, or a PREPARING worker holding only a captured prefix.

Relaxing those invariants to reuse that table would spread whole-run cancellation across retry identity/admission/guards. A discard binding can instead capture the existing execution and economic preparation revisions and use its own job's lease. Keep `run_worker.preparation_revision`, `captured_preparation_revision` and `active_prepare_job_id` unchanged. A whole-run discard alone does **not** need a worker-execution-guard successor. Per-Candidate Remove continues using the already-typed DR-05 WORKER_RELEASE path; its narrower admission must be represented honestly in the UI.

## Minimal durable support

Root request: immutable discard command/run/original PREPARE command, authenticated discard actor, accepted module epoch, expected root revision, accepted original selection state/count/page/Candidate counts, original removed count, optional bounded reason, acceptance time; maintained native enrollment/completion and REMOVED/ALREADY_REMOVED/BLOCKED counts, terminal result/time. Do not overwrite the original run actor. The card's `expected_preparation_revision` should explicitly fence the existing **run.review_revision**, including0 before any worker finishes; there is no run.preparation_revision column (`03102026_1324_banking_pay_next_core.sql:418–434`). Supply that hidden scalar via the existing PROGRESS read, without adding a second revision clock.

Candidate binding: immutable original `selection_candidate.member_no`/Candidate, own discard job, optional existing worker and original/current PREPARE jobs, captured economic/execution/Case selection identities and Case state, original status/issue, exact captured Work/ACTIVE Case-hold counts; release seeks/checkpoint, separate original-selection projection seeks, at most100 pending native ORIGIN Work IDs, terminal per-member outcome/reason/time. No-worker and already-completed worker RELEASE are explicit zero-money cases, not fabricated worker rows. Foreign keys, finite constraints, private ACLs and seek/unique indexes on the new bindings are necessary supporting schema for the expressly approved operation.

Original membership indexes already exist: `run_selection(run_id,candidate_id,selection_no)` and original `selection_candidate` native member order (`1324:450–471`); sealed Case items have native selection-number uniqueness (`supabase/migrations/04102026_0300_banking_pay_next_case_instruction_core.sql:189–208`). Do not add a duplicate whole-Candidate inventory/index merely for projection.

## Acceptance, ordering and partial enrollment

Take the same original run header UPDATE lock used by Confirm and expiry, then validate original run is unconfirmed PREPARING/REVIEW and expected root revision matches. Validate actor under the existing authority gate. Same-command replay must compare immutable run/actor/revision/reason before late eligibility; a new command cannot adopt another request or current live lease. Increment run.review_revision once on acceptance. Existing due expiry and Confirm retain their real72h checks and first-commit exclusion.

The existing expiry admission is the source model for retiring exact partial old jobs and retaining prefix identity (`0559:6346–6453,6457–6564`), not an RPC to invoke. Its current pending WORKER_RELEASE deferral (`0559:6392–6393`) is correct to reuse: let an accepted release finish its money/projection before accepting whole discard; do not reset its cursor or count it twice. Pending REPREPARE and original financial jobs need discard-qualified retirement under the same header/Candidate/job locking and immutable job evidence. Claim only a fresh native lease for the new discard job.

Enrollment must walk original accepted Candidate membership, one native member per existing ordered enroller call. The original accepted-but-not-enrolled PREPARE path and partially ENROLLING path require separately proved discard predicates: `0559:6394–6435` and `0559:2595–2665`. Do not emit EXPIRED metadata/receipt for a manual discard. In particular, **zero enrolled members is not the same as an empty requested selection**: accepted positive Create can still be OPEN/RECEIVED with no jobs. Its original command must retire safely so the ordered enrollment clock can advance. Genuine empty selected scope creates no new cancellation job or money operation.

The original enroller's RECEIVED path deliberately does not take a run lock; preserve that documented ordering. ENROLLING takes run SHARE while already owning the enrollment command/clock. Use an exact fence/retirement branch and NOWAIT/backoff where the corresponding native path requires it; do not introduce a blocking command→run inversion against discard's run→command acceptance. Parent header CANCELLING prevents stale old financial completion. Returning a partial result to REVIEW must not resurrect old jobs or publish an old revision.

## Exact existing successors required

| Existing signature / seam | Current parent; narrow delta |
| --- | --- |
| `private.bpay_next_preparation_expiry_guard_v1()` |0746:238–350. Its pay-run branch currently requires an expiry request for every CANCELLING transition (:251–257). Add a separate exact discard-request alternative, retaining immutable deadline checks and every expiry branch. A new trigger alone cannot bypass this existing guard. |
| `private.bpay_next_prepare_release_scope_v1(uuid,uuid,text)` |0559:253–293. Add only a separately bound PREPARATION_DISCARD WORK/CASE branch proving own lease/epoch/Candidate/run/header/revisions/counts/checkpoint and zero realised/NET/transfer protection. Preserve PREPARE and due-expiry branches. |
| `private.bpay_next_release_prepare_case_hold_v1(uuid,uuid,timestamptz)` |0746:502–576. Add discard's exact binding selector and typed collection reconciliation. Retain instruction-id read, native Case/component/period/hold/capacity mutations and amounts. |
| `private.bpay_next_exclude_preparation_week_v1(uuid,uuid)` |0559:549–611. Add discard's exact terminal money stage/lease/member revision branch; retain existing native unpaid ARRANGED contribution exclusion and no-new-contribution behavior. |
| `private.bpay_next_reconcile_work_collection_v1(uuid,uuid,text,uuid)` |0559:5815–5825 admission and5892 cancellation-kind classification exemption. Admit discard only through its own exact released collection proof; then include its kind in the same cancellation exemption so later approval changes do not falsely unbind release. No formula rewrite. |
| `private.bpay_next_work_collection_guard_v1()` |0559:6091–6132. Add only discard kind plus its exact typed scope arm, null financial effect and RELEASE phase. Preserve all other source/effect guards. |
| `private.bpay_next_enroll_one_v1()` |0559:2500–2681. Exact discard enrollment plus qualified original/retry retirement and native clock advancement; never label it expiry. |
| `public.bpay_next_financial_job_v1(text,uuid,text,jsonb)` |0559:4595–5140. Closed kind/phase/CLAIM/paged STEP/typed result/yield additions for discard's own lane. Preserve fresh-CLAIM-derived one-STEP transport and exact own-lease yield. |
| `public.bpay_next_preparation_intake_v1(uuid,text,jsonb)` |0708. Add only card-named action and closed scalar input/receipt/error maps; same authenticated service authority. |
| `private.bpay_next_preparation_progress_v1(uuid)` / public PAGE |Private current0746:352–418; public current0708. Maintain actor/terminal/count facts distinct from expiry and expose root revision. Public reader successor only if its closed envelope needs the new fields. |
| `private.bpay_next_confirm_simple_run_v1(uuid,bigint)` |0746:420+. Existing unconfirmed review gate already refuses CANCELLING; add only exact discard refusal/partial terminal fence if necessary for the closed card outcome. Do not alter confirmed replay or due fence. |

New discard admission/enrollment/fence/claim/page/released-collection scope/receipt helpers and binding guards belong to the named D03 owner. They are finite operation-specific helpers. Generic `receive_command` already accepts a bounded kind string (1404); base job/command kind columns are text, so do not rewrite them just to add the approved tag. Original WORK release helper0559:295–324 calls the shared scope: its money mutation body can stay unchanged. Current Case hold/capacity transition guards0310 accept native ACTIVE→RELEASED and need no kind exemption. No Source/public Timesheet trigger, row calculator, producer or retry economic-identity rewrite is needed.

## Bounded release, refresh and mixed results

Use current0746's exact WORK/CASE release and projection pattern in the new lane: WORK<=100, mixed Case/Work page<=50 to bound direct identities<=100; prelock affected Work rows before Case/component/period/hold mutation. Bump Candidate financial revision for actual release before same-transaction WB-08 refresh. Walk original Work/selected sealed Case manifests, including uncaptured suffixes and genuine Case collection Work origins. Keep economic revision0/no-worker Case resolution exact; no guessed UUID or new live selection.

Reuse the corrected pending-ORIGIN lifecycle: active native work waits/checkpoints only; completed old pending clears in its **own checkpoint-only step** before any next-page refresh can reuse that Work identity (`0746:78–101`). Projection/wait pages do not repeatedly bump financial revision. Terminal requires exhausted original manifests and exact pending native generations complete; money-page completion alone is insufficient.

Root's proposed REMOVED/ALREADY_REMOVED/BLOCKED outcomes are safe if the original protected group is left untouched and all counts are native. Already-completed DR-05 release has no second money release or parent cancellation increment. Protected NET/transfers/realisation/wrong state gives exact BLOCKED evidence, preserving original amounts and status. Do not convert an arbitrary structural/replay/lease error into BLOCKED and thereby claim known protection.

A PARTIAL→REVIEW parent transition is feasible only through the separate discard guard, with blocked>0, exact intended membership exhausted, removed+already_removed+blocked=expected, all own discard jobs terminal/current checkpoint, and accepted root revision+1. Full discarded→CANCELLED requires every intended group genuinely removed/already removed and projected. Preserve the original surviving ready/review/preparing facts; do not report all workers ready/completed merely because the cancellation command is terminal. A blocked PREPARING worker whose old job was retired can retain incomplete holds with no normal DR-05 Try again/Remove admission (those require REVIEW/READY); represent it as unresolved and refuse false Confirm/completion. Do not silently restart financial capture or invent automatic release for that case.

## Transport, UI and selection preservation

Necessary closed maps are `broker/src/bpay-next-preparation.js` (action/receipt/errors/accepted ENROLL wake at554), `bpay-next-financial-queue.js:26–27` (one new kind and paged lane) and `bpay-next-job-step.js:29–95,251,284` (claim/phase/closed page result). Use existing {id,kind} queue, native lease/checkpoint continuation, unknown request bytes/readback journal and bounded bigint validation. Discard terminal copy must not be EXPIRED. No browser Candidate inventory, digest or new queue protocol is required.

Existing progress/review windows own the two controls and confirmation naming the run/count. Closing remains closing. Authenticated actor, agency/epoch/run/revision must qualify pending readback; an uncertain mutation retains its exact bytes/command ID. Completion returns to Banking Pay, refreshing the maintained rows without changing WB-21 baseline or current local intent.

Verified current selection memory resolver remains2306:46–81 and selects the acknowledged HEADER by exact agency/epoch/actor/date/scope/run; it **does not filter pay_run status**. CANCELLED therefore retains eligibility. Its immutable guard2306:1810+ need not change because discard does not write the memory rows. FE `js/bpay-next-workbench.js:1032` acknowledges the accepted run into `memory:ref(runId)` and clears only local overrides; `js/bpay-next-office.js:750–761` calls it after sealed readback. Keep that baseline and any later intent untouched. No restore table/list/framework is needed.

## Saved-source conclusion

The separately typed own-lane design is the narrow complete direction. Essential implementation closures are partial/zero enrollment retirement, exact existing header guard, third typed release/collection/week admission, full original-manifest projection with pending-origin completion, and honest mixed-outcome/selection preservation. This note grants no item build/install/app status.

Inspected SHA256:0559 `514fb6a069dfe860d06c76d98302554bf3ed93e8e027cdbac460bf8cea7da9a6`;0746 `f1e6505c4f0ce89be8da8090dbc30467ef0ec50518536eb797f11dab37d152e6`;0708 `2494774c07a27861f05e32015e9de26e81a91942a04cc8d211dac149744bbbf8`.

