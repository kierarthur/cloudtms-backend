# DR-05 E5 retained-input retry design check

11 October 2026. Independent read-only source/design investigation requested by root. No product/checklist edits, SQL execution, database/hosted calls, tests, fixtures, browser or Git mutations. Read the full DR05 card (`03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:155–177`), every checklist row (`IMPLEMENTATION_CHECKLIST.md:2492–2517`), and `DR-05-worker-release-design-investigation.md`. Current human owner E2/E5, relayed by root, supersedes the card's newer-figures retry example: Try again creates a new affected preparation revision but repeats the same sealed click inputs and fetches no new selection/approval/rule/query cut. E5 does not itself require releasing still-ACTIVE holds. E2 separately requires system release for its six named failures.

## Narrow conclusion

**Do not infer a full revision-tag rewrite from E5.** A true monotone retry can retain original ordinary Work/position/line rows and ACTIVE holds. It must continue from exact retained progress, preserve counts/gross/managed completion and spend no captured prefix again. Those ordinary identities need no extra preparation-revision column merely because execution gets a new fenced revision.

**A worker revision bump plus a new PREPARE job is nevertheless insufficient as saved.** The new job needs the original sealed PREPARE command/run input identity, target execution revision, retained economic revision/state where applicable, and recoverable original financial progress. Case downstream consumers and already-released holds are separate unavoidable identity cases. The following conditions must be settled before claiming a complete retry owner; a request-only scaffold is not a proved resume path.

## Ordinary ACTIVE-hold resume

Current final ordinary owners are0533. Capture inserts one `run_position` per native component and increments captured counts (`0533:656–657,905–919`); hold walks those rows after its native cursor (`1078–1088`). It copies current summary counters rather than resetting them (`1073–1077`). Original core uniqueness is run_work(worker,work), run_position(run_work,component), run_line(worker,line_no)/(worker,approved_line)/(run_work,component), and hold(run_line) (`1324:507–514,527–549,561–607`).

The hold owner compares live realised/H/source basis to the **prehold** run_position snapshot at `0533:1105–1130`, marks a managed capture once at1154–1160, then inserts line/hold and credits live H/gross/counters at1170–1218. Re-entering an already-held component fails the live-H comparison; bypassing it would duplicate a native reservation or hit unique keys. A safe ACTIVE resume skips exactly the completed prefix and keeps its existing line/hold IDs and amounts. It does not recalculate those lines, reset counts, unmark managed completion, release/reacquire them, or reactivate an old released row.

Pre-write PIN/RESERVE failures can be resumed by a bounded native prefix walk that proves/reuses already existing exact run_work/run_position rows and resumes the first missing member. If that approach is selected, skip paths must not repeat count or segment-parent effects. It remains finite original membership, not Candidate-wide discovery. A saved checkpoint is simpler where available.

**Financial checkpoint is currently lost at terminal handoff.** `begin_prepare_projection0533:27–28` changes the original failed phase to WORKBENCH_WORK and clears integer and position cursor pairs. Later projectionDONE cannot tell the failed financial phase. The typed retry provenance must retain it before handoff, or the builder must define and source-review a bounded native prefix reconstruction for each admitted failure. Reading the original now-DONE job phase alone is insufficient; do not revive that historic job.

**Special spent segment failure:** `capture_segment1236:2934–2939` inserts the segment/parent facts, advances failed cursor/processed_count, and returns `SEGMENT_CREDIT_ATTRIBUTION_UNAVAILABLE` with parent attribution still incomplete. Normal resume after that cursor skips the failed completion proof; resetting before it duplicates parent rows/counters. An exact bounded revalidation/completion of that retained failed parent is required, with no second insert/count, or Try again must honestly retain the failure. This code must not be treated as an ordinary pre-write RESERVE refusal.

## Case retained economic revision

Case tables already distinguish immutable preparation revisions and typed result/state/instruction FKs (`0300:211–217,224–275,306–340,365–395`). Old instruction/result rows are immutable (`0310:286–289`), and Case holds/capacity uses cannot be rebound or reactivated (`0310:236–243,265–269`). Relabelling them as a new revision is prohibited.

Final prepare0542 resolves state by current job_id at57–58, compares state preparation revision to worker at204–206, stamps new instructions from worker at308, and selects instructions by worker revision at354/372. A new job/revision therefore misses old state or rejects it. Its REVIEW handler also sets prepare_stage REVIEW and weekly binding UNBOUND at494, discarding the prior stage label even while retaining capture/allocation cursors and headroom. A resumed typed request must identify the exact original state and retained data revision, keep its economic cursors/headroom/captured counts, and retain/resolve the failed stage without resetting spent allocation. Do not copy old allocations into a new state as though their existing holds belonged to new results.

The revision link continues after preparation; changing only0542 admission is incomplete:

| Current final owner | Direct saved dependency |
|---|---|
| Case Net0390 | state/payout admission62/80/86; active worker/request equality253; instruction queries279/356/482 |
| Case transfer0410 | draft state lookup49; active worker/build equality245; typed instruction/build member guards |
| Case cancel0460 | draft lookup22/100; worker/binding equality187; held instruction revision check |
| Expiry1832/0510 | original state lookup1832:1817; worker binding0510:374; held instruction equality426 |
| Workbench Case1946 | instruction join479 requires instruction.preparation_revision=worker.preparation_revision |

A narrow native **execution revision versus retained economic revision** association can avoid tagging every ordinary frozen table. Persist that association in the card-named typed request, or one explicit existing-worker economic-revision field if needed for these consumers. The active execution revision still fences every mutable child. The exact Case consumers above must deliberately resolve the retained economic revision instead of silently comparing it to the new execution revision. This is necessary native identity plumbing, not permission to relax all revisions or add a generic retry framework. Preserve native immutable instruction/state FKs and original Case rule/control cuts; live balances remain under original money locks.

## Completed release is excluded from retry

The chosen owner scope in `DR-05-worker-reprepare.md:7` excludes a completed released/excluded Candidate from Try again in that Draft. Its work belongs in the next Draft. ACTIVE-prefix resume therefore needs no new hold reservation identity, no dropped `hold UNIQUE(run_line_id)` constraint (`1324:605`), and no ordinary table-wide revision tags. The earlier hypothetical released retry is not selected and is not a supporting requirement. This conclusion does not rely on an ordinary-hold immutability trigger: the reviewed ordinary source proves its unique line identity and status/finished-time constraint, while Case terminal hold/capacity immutability is explicit at0310:236–243/265–269.

## Required decision and acceptance boundary

Use the retained ACTIVE-prefix branch where its exact failed checkpoint/data revision is proved. Completed E2/manual release is excluded from retry. Do not silently substitute newer figures or call the original whole-run expiry/cancel admission unchanged. The request, active revision fence, original command-input mapping, native counter deltas, parent review exclusion and bounded Work/Case refresh remain mandatory from the companion investigation.

The original investigation established the required supporting amendments, not a complete unchanged-owner reuse path. The chosen architecture and final design verdict below supersede that initial conditional result. Runtime and application acceptance: **NOT_RUN**.

## Chosen architecture review — PASS before implementation

Fresh read of `DR-05-worker-reprepare.md:1–35`, SHA256 `3cd1e58a74f52a267d3021b021f5c70471edbc7c1c6231d04a973f83fa2c81c6`. **PASS for design implementation:** it settles the original-input mapping, ACTIVE prefix checkpoint, fixed economic Case revision, separate execution fence, spent-segment completion, per-worker native release and expiry identity hazards identified above. Product/source acceptance is pending; this verdict does not claim the new owners exist or joined races have run.

Retain `active_prepare_job_id` together with the current execution revision for mutation admission. Resolve Case state by exact worker plus captured economic revision, retaining its original command/job and immutable FKs. The failed financial stage/cursors and Case prior stage **and weekly binding state** must be saved before diagnostic Workbench/REVIEW replacement. Capture must persist the exact already-written prefix counters/cursor before every early REVIEW handoff; resume skips that prefix rather than inserting or counting it again. PIN, ordinary hold and completed segment parents retain their original facts. Remove only the Case diagnostic increment on retry; preserve genuine Work financial-resolution counts.

The minimum active-execution admission census is concrete:

| Current final source / owner | Required narrow amendment |
|---|---|
| 0533 `bpay_next_begin_prepare_projection_v1`, `bpay_next_prepare_projection_page_v1`, PREPARE CLAIM/PIN/capture/hold/no-Case/public dispatcher | Resolve the exact original map or typed retry request; check active job/execution revision and retained checkpoints. Original run_command is not overwritten. No-Case READY is not used merely because the new job misses the old Case state. |
| 0542 `bpay_next_prepare_case_worker_page_v1` | Exact retained state lookup; economic revision for instructions/results; active execution fence; preserve allocation cursor/headroom, counts and old stage/binding. |
| 0044 `bpay_next_run_work_query_pin_guard_v2` at46–71 | Permit only the exact active retry job while keeping the original immutable sealed query-source pin and its original input provenance. |
| 1236 `bpay_next_segment_prepare_owner_v1` at3840–3851 | Add exact active retry admission. Existing segment fact/progress/capture guards then inherit the same owner fence; do not relax immutable segment facts or monotone counters. |
| 1236 `bpay_next_capture_segment_page_v1` at2865–2869/2934–2963 | Its original-command job join also needs the bound active job. Recheck only retained incomplete parent conservation/native binder, without another segment insert/count or cursor rewind. |
| 1236 `bpay_next_work_collection_capture_guard_v1` at3821–3830 | Remaining original positive captures/managed completion must admit the exact active job. Its segment branch at3760–3797 already validates totals and the native collection origin; retain those checks. |

The minimum Case economic-consumer census is also specific; change immutable key association, not money formulas or every occurrence of preparation_revision:

| Current final function | Economic-revision dependency |
|---|---|
| `bpay_next_accept_case_net_request_v1` / `bpay_next_apply_case_paye_net_page_v1` (0390:62,80,86,105,253) | Exact retained draft/payout lookup and Net request stamp/current worker association; request-keyed instruction queries stay internally consistent. |
| `bpay_next_begin_case_transfer_v1` / `bpay_next_build_case_transfer_page_v1` (0410:49,84–90,245) | Retained draft lookup, build/destination stamp and worker/build association. |
| `bpay_next_build_destination_group_page_v1` (1827:406) | Worker/group association; native group/instruction keys remain economic. |
| `bpay_next_case_cancel_block_v1`, `bpay_next_accept_case_cancel_v1`, `bpay_next_cancel_case_worker_page_v1` (0460:22,100,108,187) | Retained state lookup, cancel binding stamp and worker/binding association. |
| `bpay_next_apply_cancelled_week_v1` (0430:196 onward) | Case binding/worker association; simple cancellation branch stays unchanged. |
| `bpay_next_enroll_preparation_expiry_one_v1`, `bpay_next_preparation_expiry_guard_v1` (1832:1805–1829/1839 onward) | Retain original input job and economic state FK; retire exact active retry and add separate expected execution revision. Completed exclusions are recognized once. |
| `bpay_next_expire_preparation_worker_page_v1`, `bpay_next_apply_expired_week_v1`, `bpay_next_expired_work_collection_scope_v1` (0510:374,426/209/264–287) | Preserve economic instruction/state identity; fence worker mutation by separate expected execution revision. Original state/job provenance stays intact. |
| `bpay_next_workbench_refresh_case_v1` (1946:479) | Join retained instruction economic revision to the worker's captured economic revision so its ACTIVE holds remain visible. |
| `bpay_next_case_instruction_insert_guard_v1` (0310:159–185) | Economic instruction key plus exact current active job/execution admission; do not permit a stale old child merely because its economic revision remains equal. |

Existing instruction/result/state FKs, Case result/hold/capacity money guards, instruction destination capture and Net accumulation already associate their native economic objects consistently and do not require broad revision rewrites. Transfer member/build/cancel binding guards need no formula change merely because the existing keyed rows retain an older economic revision. Original reservation0639 already follows the native group/Net-state economic revision (279–285); history0653 does not filter immutable facts by the worker's new execution revision. Pure plan0204 and row kernel0290 remain unchanged. Source versions above are the exact original bodies, not permission to reinstall older files wholesale.

Expiry's existing preparation_revision participates in its Case-state FK (`0501:89–90`); using execution2 there with retained economic state1 would fail. The design's separate expected execution revision is essential. At acceptance, original72h whole-run admission remains strict. Deferring expiry enrollment behind an already accepted indexed RELEASE cleanup avoids abandoning a partially released prefix; the original Confirm time fence still blocks new reservations. Completed release cannot restart. Parent readiness/cancellation and weekly contribution deltas must be applied once under the original locks, with bounded projection before DONE. These are implementation review requirements; no further reservation identity, producer, calculator or generic proof framework is necessary for the chosen branch.
