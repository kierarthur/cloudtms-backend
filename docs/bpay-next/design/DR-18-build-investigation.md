# DR-18 current build investigation

12 October 2026, approved sequence124. Source-only investigation of full card03:473–496, current ITEM and D01–D05 checklist rows, R3:164–168/476, master08:208/682, current final-definer registry and saved native code. No product/checklist edits, SQL/database/business calls, tests, fixtures, hosted actions, rehearsals or captures. Future joined, timing and in-app acceptance remain **NOT_RUN**.

## Exact task and retained chain

Confirmation changes the locked Run's state under the exact accepted review revision. It must not copy/reprice money, hash inputs, recheck later financial freshness, split Source from ordinary, or expire a confirmed Draft. The output is one address per nonempty PAYE/UMBRELLA channel, PAYE first, with an Open action for each. The remaining WB08 change is **atomically enqueued exact finite affected Work/Case membership**, consumed in bounded transactions after Confirm; no all-page refresh loop in the confirmation transaction (card477–489; R3:166).

| Exact current object | Final physical owner |
|---|---|
| `private.bpay_next_confirm_simple_run_v1(uuid,bigint)` | `11102026_0847_banking_pay_next_discard_row_projection_v1.sql`:1085–1166 |
| `private.bpay_next_review_simple_run_v1(uuid)` | `11102026_2259_banking_pay_next_prepare_retry_v1.sql` |
| `public.bpay_next_preparation_intake_v1(uuid,text,jsonb)` | `11102026_1358_banking_pay_next_paye_guard_intake_v1.sql`:156–159 |
| `private.bpay_next_preparation_authority_v1(uuid)` | `04102026_0575_banking_pay_next_preparation_service_v1.sql` |
| `private.bpay_next_channel_drafts_page_v1(uuid)` | `06102026_0236_banking_pay_next_channel_draft_readers_v1.sql`:6–34 |
| `private.bpay_next_channel_worker_page_v1(uuid,text,uuid,integer)`, `private.bpay_next_channel_status_page_v1(uuid,text,uuid,integer,integer)` | same0236 |
| `public.bpay_next_preparation_page_v1(uuid,text,jsonb)` | `12102026_0442_banking_pay_next_run_review_totals_v1.sql` |
| `private.bpay_next_workbench_schedule_task_v1(text,uuid,uuid,uuid,boolean,boolean,boolean)`, `...queue_origin_v1(uuid)`, `...claim_task_v1(uuid,integer)`, `...step_task_v1(uuid,uuid,bigint,bigint,bigint,text,text,integer)`, `...finish_task_v1(uuid)` | `10102026_1429_banking_pay_next_workbench_current_continuation_v1.sql`:66/268/282/308/245 |
| `public.bpay_next_workbench_runtime_service_v1(text,uuid,jsonb)` | same1429:555 |
| `private.bpay_next_workbench_refresh_work_v1(uuid)`, `...refresh_many_v1(uuid[],uuid[])` | `10102026_1438_banking_pay_next_workbench_origin_projection_v1.sql`:318/354 |
| `private.bpay_next_workbench_refresh_work_core_v1(uuid)` | `11102026_2223_banking_pay_next_shift_query_exclusion_v1.sql` |
| `private.bpay_next_workbench_refresh_case_v1(uuid)` | `12102026_0207_banking_pay_next_origin_wait_recovery_v1.sql` |

D01/D05 refer to one complete confirm successor, not two independent implementations. D02/D03 own the native confirmation/success flow. D04 is context/buildTask:false. Dependencies F08/DR16/DR05/WB08/F16 have reusable current owners; this investigation establishes no new blocked dependency. DR07 banners and DR27's maintained channel Draft list are later items, not permission to build those screens/stores here.

## What the current confirmation already gets right

0847 locks module then Run (1100–1107), returns permanent exact DRAFT replay before deadline checks (1108–1112), refuses bound discard/expiry (1114–1118), requires REVIEW/SEALED/exact review revision and complete original Work choices (1119–1125), reads the real clock **after** the header lock (1127–1133), verifies sealed native original PREPARE membership/jobs and unfinished DR05 actions (1135–1145), and admits only READY or exactly completed typed RELEASE workers (1146–1153). It updates only status/confirmed time and returns the existing channel helper (1159–1164). These complete original guards, including PF08/DR05 and expiry/discard behavior, must survive the successor.

There is no current confirmation refresh scheduling in that function. Add only the required typed enqueue/state/counter support; do not introduce live approval/channel/Case-rule repricing or recreate frozen lines/holds. Permission remains the exact existing service intake authority, not a new route.

**Concrete channel membership gap:** current0236:22–28 includes every retained worker. Card489 requires at least one worker that was **not removed before Confirm**. The existing helper needs a narrow complete successor excluding exact completed pre-confirm DR05 RELEASE membership, rather than a blanket current status filter. Preserve the channel address when an originally confirmed worker is CANCELLED later by channel/execution cancellation. Retain zero-money original members; do not substitute positive money for membership. An all-pre-confirm-released Run must not be reported as one/two created Drafts merely because historical workers remain. Resolve its empty-result admission explicitly with original review/confirmation semantics; do not fabricate an address or silently reprice/restart it. The exact immutable predicate and source proof are recorded below.

The two channel probes already use only captured `target_pay_channel` and order PAYE then UMBRELLA; Source and ordinary are unified. Index `bpay_next_run_worker_channel_page_idx(run_id,target_pay_channel,candidate_id,id)` is installed-source09102026_2012:5–6. No origin split or extra channel table is necessary for this card.

## Existing finite projection seams and the real support gap

WB08's existing private task table is `bpay_next_workbench_task` (migration1429:37–85). It has task UUID/key, generation/module/owner epochs, lease, step, <=64-byte cursor, wake/available times, status and exact checkpoint; indexed READY/ERROR wake and expired LEASED discovery already exist. Allowed types are **SEED/METADATA/ORIGIN/UMBRELLA**. ORIGIN has one required Work UUID and no Case/run binding. METADATA updates labels/summary contributions, not the actual Work/Case classification. Do not claim either already implements a Case-only confirmation refresh.

The existing task STEP discovers finite native pages, takes Candidate → Work → Case locks before task/metadata/global revision, then validates exact lease/generation/step/phase/cursor and native current tuple (1429:308–411). Finish updates the normal pending/dirty/current barrier (245–265). Queue-origin schedules exact Work and marks processing (268–279). `refresh_many` requires <=100 combined IDs/16KiB, preholds exact Candidate/Work/related Case roots, and uses bounded ORIGIN fallback when related membership is larger (1438:354–408). Use these owners and lock order; do not copy financial logic.

Immutable native membership is already available:
- `run_work(run_worker_id,work_id,captured_revision_id)`, unique worker/Work and page index(worker,work,id), core1324:505–519/1031–1034.
- `run_selection(run_id,selection_no,work_id,candidate_id)`, unique Run/Work and Run/selection number, core450–459. This includes original accepted scope and is not by itself proof every member remained in the confirmed offer.
- `case_selection_item(run_id,candidate_id,selection_revision,selection_no,case_component_id,case_id,is_selected)`, PK exact component and unique native selection number; selected-page index on exact Run/Candidate/revision, schema0300:189–208.
- `run_case_instruction` has exact worker/economic preparation revision/component/Case and FK back to the selected item, schema0300:270–281; its native order index is (worker,preparation_revision,age_key,case_id,component_ordinal,component_id),295–296.
These permit finite exact traversal including Case-only Candidates without Candidate-directory/history scans or new money copies.

**Minimal complete implementation choice still to settle:** add one finite confirmation-specific scope/phase to the existing WB08 task machinery, bound to Run + accepted review revision, with exact Work/Case members snapshotted atomically under the Run lock. A private supporting migration is justified only for this missing typed binding/membership/index, not a generic queue/proof framework or new public table. One set-based native member insertion is the card's atomic membership capture; it is not an all-page refresh loop. Pages then reuse original CLAIM/STEP/DUE/finish and bounded refresh helpers.

If avoiding copied members by traversing existing immutable rows, first prove original Confirm-eligible worker membership through an immutable native association. **Do not recompute it from later `run_worker.status`**: run_work/Case rows survive typed removal and worker status can later change after confirmation. A current-status predicate on later pages can lose or gain the original affected scope. Snapshot actual non-cancelled confirmed membership once, or provide a precise existing immutable predicate that distinguishes pre-confirm DR05 RELEASE from later channel cancellation. The same task must cover Case-only membership, large one-Candidate scope and any actual collection-origin Work needed by Case refresh, with Work preholds before Case and native ORIGIN completion/retry preserved.

No new financial job/command or queue payload is required. Existing background20/61–67/150 dispatches `{id,kind:'WORKBENCH'}` and fresh CLAIM → one STEP. Supporting finite type/phase schema additions must match `broker/src/bpay-next-runtime-rpc.js`:53–54/85–90; retain its exact request/result/cursor/byte limits. Public runtime signature can remain unchanged; any same-signature successor is only for the new exact branch. No new public membership object is necessary.

## Native Office/transport completion

F08 intake1358:156–159 forwards phase/review/replay/`drafts` exactly. BE preparation139/193/666 and FE Office144/171/620 already close CONFIRM to original Run/review revision and validate one/two channel addresses; no Worker business transport rewrite is needed. CHANNEL_DRAFTS validation is BE766 and FE1013; current native channel workers/status preserve existing identity guards.

FE native review Office2192 currently sends CONFIRM directly, says “Draft confirmed.” and stays in review. Thin Sheet2610 and2606–2607 expose existing confirm/channel controls, but do not prove the required native two-window flow. Complete:
1. Native Confirm dialog showing exact server whole/channel Candidate counts and frozen totals; capture the offered review revision once and send that same revision after approval, not a later polled value.
2. Native “Payment Draft created” success, one row per returned channel, each Open; same original receipt recovery must reach this success after an unknown result/reload, without another confirmation/repricing.
3. Reuse current `CHANNEL_DRAFTS` and channel-qualified workers/status opener; Office2578–2580 already reads/binds that scope. Future Payment Batches/banners consume the same addresses. Do not fabricate a new batch directory or early DR07/DR27.
4. Preserve existing controller unknown pending bytes and `retry()`; a definite stale/expiry/discard refusal retains honest native feedback and explicit Refresh/recovery rather than automatic altered CONFIRM.

DR16 already supplies server totals via `REVIEW_TOTALS` (0442:35–46), including Candidate/READY counts per channel and totals revisions. Do not count visible Candidate pages or sum their money in the browser.

**Binding owner correction:** despite older card491/D02 pay-date wording, ownerA3a/pack `SCREEN_DESIGN_POLICY.md`:72/472 (SDP12a) says no pay date before Execute. SCR48/48b is native Confirm with channels/counts/frozen amounts; SCR49/49b is created/open handoff (B4 audit181–182). SDP60:248 owns the small success window and primary Go to Payment Batches. No confirm actor label is supplied by this reader; pack B2/B5/B6 audit52 removes that unsupported picture field. Actual A4/joined acceptance is NOT_RUN here.

## Saved source provenance

SHA256: confirm0847 `86343D45963A9335DE8F3AF0A3465CBBCDD5C4C2FD3EEEF69C628060C8DF1C47`; channel0236 `A90B3B2FE01E079E7177BEDDE583D5E0B66DD037B2344272A6A8FB288D008719`; intake1358 `5777B06B24A9AA1DEDAD608926A6481665C02043652A842F3AF4AB6A48E4750D`; task1429 `DC539089FAE0CD1A8BAF4765A89CEA9FF1D1E8039200ECDA5D80371DDA2335DD`; refresh1438 `B8D445C4A4CFC02978E8AC77A159350B0E1978AAFF9A1172DA8E7E157A6414F5`; Office `0CC97ECA7ACED3CD95CAC4370122955814A48F1B6493E9A287F84020FB459FFD`. These identify inspected source, not installation or whole-item PASS.


## Lean task design assessment (12 October, root scope follow-up)

Root's proposed implementation narrows the unresolved choice above to **one CONFIRM task on the existing WB08 table**, Run/review-revision binding, and one private exact task-member table with WORK/CASE native FKs. A set-based DISTINCT snapshot of native run_work + current economic Case instructions for workers READY under the locked Confirm provides stable membership; later worker status is irrelevant. Two finite CONFIRM_WORK/CONFIRM_CASE phases consume it and call the existing projection owners. This is genuinely required card489 support, not another financial job/command or public route.

Source assessment: **design sound with these exact closure requirements**.

- The normal global pending barrier is sufficient: current public Workbench owner0235:156–161 refuses a completed/current page while pending_task_count is nonzero. Keep the confirmation task's candidate/work/umbrella fields NULL, increment pending_task_count exactly once, and bump Workbench revision atomically with Run state + members. Do not manufacture Candidate metadata freshness or change captured financial revision at Confirm.
- Existing finish1429:245–265 already handles a task with NULL Candidate: it skips Candidate metadata/dirty-counter updates and decrements the global task count once. Do not debit dirty_candidate_count or clear unrelated continuation_pending flags. Projection pages' normal contribution/summary changes remain transaction-local and the global barrier stays until the last phase is exhausted.
- Exact DRAFT replay must enqueue no second task/member set and increment no counter. Bind unique task_key=CONFIRM:RunUUID, task type, Run FK and original review revision. Membership uses unique/PK(task_id,member_kind,native_id), required UUID and closed WORK/CASE enum; conditional native FKs must match the same native_id with explicitly true/null-safe CHECKs. Guard insert against the exact native original Run/worker/economic instruction membership, and preserve immutable members against UPDATE/DELETE/TRUNCATE with owner-only forced-RLS/table ACL discipline. Do not copy money.
- STEP plain-discovers at most100 members by kind + native UUID keyset, preholds Candidate → Work → Case roots in order, then revalidates exact native lease/module/generation/step/phase/cursor and the captured task's Run/review binding before projection/advance. Exhaustion and two-phase transition must be based on the native frozen member page, not current worker state or an unbounded membership count. Existing helper lock order and summary application must remain unchanged.
- **Required current-owner edge:** runtime DUE1429:655–657 passes any old-module task_type to schedule_task, whose77–78 whitelist excludes CONFIRM. Give the new task one exact epoch/recovery branch retaining its captured members and same task identity; do not regenerate current financial scope or leave the task permanently counted pending. Preserve the existing native OWNER/CLAIM/STEP replay and wake path.
- Runtime Worker finite type/phase whitelists53–54/85–90 need the same CONFIRM phases. Queue bodies remain exactly{id,kind}; no new payload fields, dispatcher route or financial command. Closed helper/callee/source-gate additions must name this finite branch only.

This amendment supersedes the earlier choice discussion; it does not authorize result-time metadata, a new freeze/proof framework, source/ordinary splitting or early DR07/DR27 work. Product, installed/runtime and app verdicts remain NOT_RUN.

## Channel-address refinement and original economic membership proof

12 October root scope follow-up: **the proposed immutable RELEASE exclusion is source-sound**. The channel reader should use NOT EXISTS an exact native worker_reprepare_request with action RELEASE, status COMPLETE, matching Run/worker, target_preparation_revision = worker.preparation_revision, captured_preparation_revision = worker.captured_preparation_revision, and released_work_count = expected_work_count / released_case_hold_count = expected_case_hold_count (the same release completion accepted by Confirm0847:1148–1151). Retain the original two indexed channel existence probes and PAYE-first order. Do not filter on later current worker.status or current positive money.

Evidence:
- Current accept_worker_action2259:2703–2705 locks the Run. After same-original receipt replay2713–2717, first-use2739–2753 refuses a non-NULL confirmed_at_utc or status outside PREPARING/REVIEW. Thus a new DR05 RELEASE cannot be accepted after Confirm; replay only returns the same already retained receipt.
- Current worker_action_guard0814:3877–3896 refuses DELETE, immutable tuple changes and **every UPDATE once old.status is COMPLETE**. Action, Run/worker/Candidate, target/captured revisions, expected counts and completed released counts therefore remain a stable pre-confirm exclusion. Schema0559:26–79 requires exact worker/Run/Candidate FK, unique worker/target revision, COMPLETE/terminal-time equivalence and count bounds.
- The later current ordinary cancellation0260:362 and Case cancellation2129:1429/1537 mutate status CANCELLING/CANCELLED, and use native cancel_request/case_cancel_binding. They do not change the worker's execution/economic preparation revisions or create a DR05 RELEASE. Therefore an original confirmed worker later cancelled remains a channel member/address. Empty noncancelled-at-current-time money does not erase a historical channel Draft.
- Do **not** call the more elaborate worker_release_complete0559:6322–6342 for this channel history predicate. That helper additionally tests current module, worker status, jobs/holds/transfers/net effects and cutoff; those live eligibility predicates are appropriate to expiry/discard, not immutable channel-address membership.

CONFIRM_WORK's bounded page can call the existing current refresh_work_core directly after whole-page Candidate/Work preholds and exact task revalidation. This avoids refresh_many scheduling late related Case locks after the task lock. Snapshot selected Case IDs plus actual collection-linked Case IDs from the selected captured Work into the immutable CASE member set once, then use CONFIRM_CASE preholds and current refresh_case; existing workbench_origin_case_page_idx(work_id,case_id) (migration1429:90–91) supports that native collection join. This is projection membership, not new accepted economic Case selection.

Case-only snapshot uses the exact original instruction run_id/run_worker_id and **instruction.preparation_revision = worker.captured_preparation_revision**, with selection_revision = worker.case_selection_revision. The active execution worker.preparation_revision may advance under DR05/PF08 while original economic instructions remain unchanged; using it would omit genuine Case-only/current captured Case rows. Instruction/schema0300:270–281 stores the exact component/worker economic identities and original selected-item FK. Include native selected IDs once; DISTINCT removes duplicate Case membership without summing money. Later current Case rules/allocations/status do not regenerate this set.

No new channel store, financial owner, public object, command or late freshness check is necessary for this refinement. The earlier bare “non-cancelled worker” wording is now explicitly interpreted at locked Confirm, preserving later historical Draft identity. Product/source implementation review and runtime/app acceptance remain NOT_RUN.


