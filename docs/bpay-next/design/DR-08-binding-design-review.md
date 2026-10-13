# DR-08 binding design review

11 October2026. Read-only design review of the proposed existing expiry-binding extension. No product edits, execution, database requests, fixtures or tests. Authority: complete DR-08 card `03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:224–247`, local checklist:2465–2492 and `DR-08-expiry-investigation.md`. This is a design verdict, not item/runtime completion.

**Recommendation: proceed with the proposed three fields and two internal stages, subject to the exact conditions below.** Existing binding-owned projection seek cursors and bounded pending Work identities close the card's terminal summary gap without a new command, table, queue kind, proof framework or WB-08 rewrite. Keep the external job phase RELEASE and response CANCELLING until the normal native EXPIRED receipt is complete.

## Required schema shape

The current binding is0501:50–105. Add nullable positive-bigint `projection_work_cursor` and `projection_case_cursor`; NULL is the initial lower seek boundary. Never reset or reuse either financial `work_cursor`/`case_hold_cursor`. Add `pending_work_ids uuid[] NOT NULL DEFAULT '{}'::uuid[]`, cardinality<=100, no NULL elements, one-dimensional ordinary lower bound for nonempty arrays. Canonical distinct IDs in the writer/guard avoid duplicate operands consuming the bound. A fixed16KiB byte check is consistent with the existing WB-08 input envelope, though100 UUIDs are already intrinsically small.

Extend only the binding stage CK and status/stage CK to include PROJECT_WORK and PROJECT_CASE under RELEASING, between CASE and FINAL. Preserve READY/NEW/checkpoint0 and EXPIRED/COMPLETE with exact released financial counts and finite started/finished timestamps. Require initial projection cursors NULL and pending IDs empty on READY insertion. Require pending IDs empty on EXPIRED; keep finished_at NULL throughout release/projection/wait. Existing immutable captured economic revision, expected execution revision, original jobs, Case state, original counts and accepted timestamps remain immutable. Existing indexes suffice: run_selection `(run_id,candidate_id,selection_no)` and Case item's full unique `(run_id,candidate_id,selection_revision,selection_no)`; no additional index is needed.

## Exact update-guard changes

Current0559:6659–6666 excludes only the original mutable fields from its immutable row-tuple comparison. Add exactly these three fields to both exclusions. Preserve old.status EXPIRED refusal, checkpoint exactly old+1, nondecreasing financial counts/cursors and immutable started_at.

Legal stage transitions should be:

| Old stage | Allowed new stage |
| --- | --- |
| NEW | WORK or CASE, retaining the original empty Work-page transition |
| WORK | WORK or CASE |
| CASE | CASE or PROJECT_WORK |
| PROJECT_WORK | PROJECT_WORK or PROJECT_CASE |
| PROJECT_CASE | PROJECT_CASE or FINAL |
| FINAL | COMPLETE |

A wait is a same-stage update in any RELEASING stage; FINAL may need FINAL only if the implementation permits pending IDs to reach FINAL. Prefer making entry to FINAL require pending empty, so the original strict FINAL→COMPLETE rule remains sufficient. Do not retain old CASE→FINAL as a shortcut.

Once non-NULL, each projection cursor must remain non-NULL and >=old. It may advance only while the old stage owns that walk, with NEW values tied to the exact original manifest's native selection_no and Candidate/run (plus exact sealed Case selection revision). Equal cursor is valid for pending waits and empty-page transitions. PROJECT_WORK must not mutate the Case seek cursor; PROJECT_CASE must not mutate Work seek or either financial release cursor/count. Release counts/cursors remain unchanged throughout both projection stages. Entering PROJECT_WORK requires original Work and Case release counts complete and no ACTIVE own Case hold; entering FINAL/COMPLETE requires no remaining original Work membership or original selected sealed Case membership beyond the stored seeks and no pending origin work. Check predicates must reject NULL/unknown Boolean proof rather than allowing a PL/pgSQL IF NULL to pass.

The page implementation, not an invented second row kernel, supplies its <=100 page proof and advances the cursor only after successful native refresh. Its existing accepted scope, module/job/lease/checkpoint validation and terminal replay remain ahead of new work. Keep financial parent counters and EXPIRED finished timestamps after all projection checks, once per binding.

## Pending-origin wait and capture

Native ORIGIN scheduling1429:91–143 uses the unique `ORIGIN:<Work UUID>` task key, exact Candidate/Work/module facts, and records the Candidate financial_view_revision. Its STEP398–402 restarts the task when that financial revision changes. Therefore the proposed wait branch must not increment financial_view_revision, call refresh_many, redo release, move any cursor/stage or replace/clear still-active pending IDs. Update only the binding checkpoint and existing job checkpoint/lease, then return the ordinary RELEASE page receipt with zero visits/releases. This lets the existing independent Workbench task lane finish.

Read the exact existing ORIGIN task rows under the already-held Candidate lock; Workbench task STEP373–387 also takes Candidate before Work/Case/task. Match native task_type/work_id/candidate_id/module epoch, not only a guessed textual key or a Candidate-wide pending flag. ERROR, READY and LEASED all mean unfinished. A pending task's DONE state is native evidence: ORIGIN_WORK512–514 performs refresh_work_core before finish_task253–259 updates DONE and maintained metadata. Work IDs suffice; do not add task generation/receipt machinery because the existing unique task row is rescheduled in place and any non-DONE current generation remains a wait.

If pending_work_ids stores every exact affected Work returned from a refresh page, an absent ORIGIN task is valid synchronous completion; if it stores only tasks actually queued, unexpected absence should fail closed. Choose and document one meaning. Capture IDs after refresh_many, from only that bounded native page. They must belong to this Candidate and either the original selected Work manifest or the exact original Case hold/instruction→work_collection relation released on that page. Case-related Work can be outside run_selection, so a selection-only guard would incorrectly reject legitimate collection release. Clear completed pending IDs only after the exact check; then the next page may replace them with its newly queued native Work set in the same transaction. Never lose the prior pending set to make a later page fit100.

## Release pages and projection pages

The CASE<=50 proposal correctly bounds the direct arrays to at most50 distinct related Works +50 distinct Cases. Preselect this native hold page, take Candidate→Work→Case locks in sorted native identity order, then component/period/hold locks, and call the original exact release helper. For related Case prelocks mirror refresh_many1438:368–381 exactly: its <=101 related expansion uses the explicit Cases fallback when combined size>100. Do not build an unbounded all-related Case array or acquire a new Work after locking Case. Already-held Work identity makes original collection reconciliation safe.

Increment Candidate financial revision for the genuine release mutation before the same-transaction native refresh so any queued task captures the new value. Do not keep the old unconditional0559:4546 bump on wait pages. Projection-only continuation should not invent a financial mutation; use the existing native refresh owner and its maintained Workbench revisions. Failure rolls the release, refresh, cursors and pending capture back together.

PROJECT_WORK must visit original run_selection, including uncaptured selected suffix. PROJECT_CASE uses the original sealed Case membership and the original revision rule; only selected rows need refresh because run cancellation changes their accepted classification. A0/no-worker binding must still find the exact accepted sealed Case header where present, matching the established DR-040533:82–95 fallback. Do not use a new live Case/Work selection or conflate economic preparation revision with retry execution revision. Each page remains<=100; native projection may truthfully produce WAITING/BLOCKED for other current causes rather than forcing Ready.

The already_worker_released branch currently0559:4500–4508 traverses WORK→CASE→FINAL without money. It must instead traverse CASE→PROJECT_WORK→PROJECT_CASE→FINAL too, preserving the exact prior RELEASE-complete check and zero release counts. No-worker/zero-hold bindings also traverse the finite original selection walks. The whole-run zero-selected tombstone accepted0559:6443–6449 remains an immediate exact zero-member EXPIRED receipt; it creates no worker/projection job and requires no synthetic business rows.

## Verdict and successor boundary

The design is sufficient if these conditions are enforced. Necessary successors are the current0559 expiry binding guard and expiry page, plus a small migration updating the existing binding columns/CKs. Enrollment can rely on NULL/empty defaults while its INSERT guard explicitly requires them; change its body only if actual projection Case scope must be captured there. Existing claim0510:161–187, fence0510:6–18, public due/receive0520 and shared release kernels need no change solely for internal binding stages. Existing release_scope0559:253–292 continues to admit only WORK/CASE money stages; do not broaden it to projection stages. Existing apply_expired_week0559:4584–4588 still requires EXPIRED/COMPLETE under the original LEASED RELEASE job and exact checkpoint; retain that ordering before job DONE. Preserve DR-05's current economic/execution identity and completed-release branch rather than copying old0510 bodies.

Source-only conditional PASS. Runtime, contract/export closure, owner screens and full DR-08 acceptance remain pending and were not executed here.
