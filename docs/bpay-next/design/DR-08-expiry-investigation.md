# DR-08 expiry investigation — sequence 83

11 October 2026. Independent saved-source investigation only. No product changes, SQL execution, database requests, fixtures, tests or browser actions. This does not award DR-08 completion; DR-06 is still closing and the DR-05 ITEM remains open for its later DR-15 identity dependency.

## Authority and current parents

The complete card is the ordinary approved plan `03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:224–247`. All local DR-08 rows are at `docs/bpay-next/IMPLEMENTATION_CHECKLIST.md:2465–2492`: owning D01/D02/D03/D04/D06/D07/D09, context D05/D08, with D07 display and D09 refresh still missing. Dependencies are F-07, F-11, DR-06, WB-08 and F-16. Master `08_BUILD_ORDER_AND_CODEX_INSTRUCTION.md:59` orders expiry DR-08 → DR-05 → FC-15 → CM-13a → CM-15; sequence83 is recorded at167/468. The saved registry and already-installed DR-05 successors take precedence when selecting a physical parent; do not reinstall the older0510 bodies over0559.

`docs/bpay-next/final-definer-closures.json` selects:

| Native signature | Exact current physical parent |
| --- | --- |
| private.bpay_next_accept_preparation_expiry_v1(uuid,uuid,uuid,timestamptz,timestamptz) | 11102026_0559_banking_pay_next_worker_reprepare_v1.sql:6346 |
| private.bpay_next_enroll_preparation_expiry_one_v1(uuid) | same0559:6457 |
| private.bpay_next_expire_preparation_worker_page_v1(uuid,uuid,bigint,bigint,integer) | same0559:4429 |
| private.bpay_next_apply_expired_week_v1(uuid) | same0559:4575 |
| private.bpay_next_preparation_expiry_guard_v1() | same0559:6569 |
| private.bpay_next_expired_work_collection_scope_v1(uuid,uuid,uuid) | same0559:6674 |
| private.bpay_next_expired_prepare_receipt_v1(uuid) | same0559:6731 |
| private.bpay_next_claim_preparation_expiry_job_v1(uuid,integer) | 04102026_0510_banking_pay_next_preparation_expiry_v1.sql:161 |
| private.bpay_next_preparation_expiry_fenced_v1(uuid,uuid) | same0510:6 |
| private.bpay_next_reconcile_expired_work_collection_v1(uuid,uuid) | same0510:303 |
| public.bpay_next_preparation_expiry_due_v1(timestamptz,uuid,integer); public.bpay_next_preparation_expiry_receive_v1(uuid) | 04102026_0520_banking_pay_next_preparation_expiry_service_v1.sql |
| private.bpay_next_preparation_progress_v1(uuid); public.bpay_next_preparation_page_v1(uuid,text,jsonb) | installed11102026_0708_banking_pay_next_preparation_progress_v1.sql:5/83 |
| private.bpay_next_confirm_simple_run_v1(uuid,bigint) | same0559:5379 |

The inspected SHA256 values are0559 `514fb6a069dfe860d06c76d98302554bf3ed93e8e027cdbac460bf8cea7da9a6`,0708 `2494774c07a27861f05e32015e9de26e81a91942a04cc8d211dac149744bbbf8`,0510 `f4eb08c3c963c1850396642fd98947cf27722d96e20b6bb77db394635c3341aa`,0520 `81ffc53b673cfb5c0d5fa352c4508f14ef347fd73582c8a09022f210f92c7b9d`.

## Preserve the installed eligibility and identity fences

The original deadline is finite and exactly created_at_utc +72 hours (`04102026_0501_banking_pay_next_preparation_expiry.sql:6–9`), with a matching native due index at23–24. Acceptance0559:6362–6389 takes the original run UPDATE lock, returns exact existing command replay before fresh eligibility, then obtains the actual clock after the lock. Only unconfirmed PREPARING/REVIEW is newly eligible. The public receive wrapper supplies NULL for the private observed-clock operand; no browser or transport clock is admitted. Confirm0559:5401–5423 uses the same run lock, preserves confirmed DRAFT replay first, and checks the real deadline after waiting. Preserve this first-commit race and SQLSTATE55000 refusal; never add expiry to confirmed/submitted/unknown-execution obligations.

The original fence0510:6–18 binds original PREPARE command, immutable deadline, original selection counts, captured review revision and terminal enrollment/completion equality. Retain it on every stale child. The current0559 expiry bindings distinguish captured economic preparation revision from expected execution revision; keep the immutable Case state/instructions and exact retired original/retry jobs. Preserve its completed-worker-release recognition, unfinished RELEASE deferral, legal zero-money WORK→CASE→FINAL path, own ACTIVE hold/capacity checks, counted checkpoint replay, and parent completion equality. Expiry does not reprice, fetch a new choice set, or realise money.

Completion note for T-05/CM-15: current ordinary states are run/request CANCELLING, expiry worker RELEASING with stage WORK or CASE, then FINAL/COMPLETE and worker/request EXPIRED with run CANCELLED. These are the actual typed states; comparison expiry must align with them when its separately owned chain is installed.

## F-07 composed source comparison

Imported `SOURCE_COMPARISON_JOINED_EXPIRY_V1_20261007/COMPOSITION.json` explicitly says E_ONLY_UNADOPTED and nativeProof:false. READINESS.md limits its four focused checks to source composition and leaves native release/lease/deadline/replay proof pending. `imported-20261007/MANIFEST.md:26,142–144,434–443` classifies it T05_COMPOSED_EXPIRY_REFERENCE, requiring later Contract-move rebuilding, not an ordinary CLEAR installation.

Its pinned0510 candidate SHA is `47ae450ae5a5cdff55290a47e67e6f88b4fd87a4da1acb788d6ccaf897588a66`. The relevant extra spans are comparison Work carry at408 and comparison Case collection reconciliation at315–316. Neither is an ordinary DR-08 adoption: they reference comparison fields/helpers, and the composition leaves the ordinary expiry guard unchanged. Preserve the reference and record current ordinary states; do not import its raw replacement or revive absent comparison schema. F-07's design note:46/82 makes the same distinction.

## Remaining SQL closure: all affected projection identities

Current0559 release helpers295–324 and372–446 update only exact financial positions/holds/capacity and collection reconciliation. Expiry WORK/CASE pages4510–4532 do not call WB-08 refresh. Terminal status/counters4547–4566 can therefore finish with stale Workbench groups and summaries. This is the concrete D09 gap.

Refreshing only released hold IDs is insufficient. The accepted Work reader1252:165–175 excludes an entire run as soon as it becomes CANCELLING; current Case refresh0559:4311–4330 has the same run-status condition. Original accepted Work/Case selections that were never captured before expiry also change classification. Walk this binding's original `bpay_next_run_selection` and exact sealed `bpay_next_case_selection_item` identities in finite pages, including the uncaptured suffix. Reuse the native selection_no seek pattern from DR-04 projection0533:74–95, with no Candidate-wide inventory. For Case revision0/no worker, retain its exact original sealed-header resolution; a nonempty positive captured revision must match the original SEALED header. Do not guess a Case UUID or read a new live selection.

The smallest complete support is bounded projection continuation inside the existing expiry job/binding after exact release, before the binding and parent advertise EXPIRED. Use native typed Work/Case seek operands on the existing binding if current cursors cannot represent both traversals; extend only its stage/transition guard as required. Do not recycle a completed DONE job or reset financial release cursors. The existing job phase can remain RELEASE and the public receipt remains CANCELLING until this work completes, so no new queue kind, command, table or financial calculation is needed. Keep each page <=100 and advance its own checkpoint atomically after its projection call.

Release-page refresh must still occur in the same transaction as the page's affected hold changes. Acquire any affected Work identities before Case/component/period/hold locks; calling refresh_many after taking Case locks can otherwise acquire Work in the opposite order. Current outer expiry order is module SHARE → original run UPDATE → Candidate UPDATE → job UPDATE → expiry binding/worker; preserve it. Existing refresh_many1438:354–407 bounds its original inputs to100/16KiB and locks Candidate → Work → Case. It can expand related collection Cases and queue an existing ORIGIN task when the total exceeds100. Its successful return alone does not prove all related summaries complete. Keep the terminal claim honest if that fallback is reached; use the existing bounded origin completion mechanism rather than silently counting a queued projection as finished.

No new paging index is justified by the source census: run_selection has `(run_id,candidate_id,selection_no)` at core1324:460–461; Case items have full unique `(run_id,candidate_id,selection_revision,selection_no)` at0300:200, plus selected partial index208; run_line has `(run_worker_id,line_no,id)` at1324:1029–1030; ACTIVE Case holds have `(run_worker_id,id)` at0450:55. Due and expiry binding indexes already exist0501:23/107. Supporting cursor/stage columns, if needed, are a small migration on the existing binding, not new business rows.

## Remaining transport and Office closure

The existing0520 due/receive admission already selects only native-clock due, unconfirmed PREPARING/REVIEW and uses the original tuple cursor <=100. Worker background19/77–87/125–129 already publishes and receives exact `{id,kind:'EXPIRY'}` notifications, then the native ENROLL wake. The expiry adapter179–182 validates CANCELLING/EXPIRED with exact counts;210–213 treats unfinished worker release as retryable. Preserve these lanes and the fresh CLAIM/one STEP lease discipline. There is no evidence requiring another expiry Worker command or scheduler lane.

Current0708 PROGRESS:36–57 already reports cleanup pending from captured expiry counts and suppresses stale financial-ready buckets, but exposes no actual expiry finished timestamp. Add one nullable native completion fact from the exact existing expiry request, admitted only for terminal EXPIRED with completed=enrolled=expected and original run CANCELLED/unconfirmed. Keep the STABLE snapshot/no writer-lock read. The byte/schema validators and named public constructor must change together. Do not use the deadline or a browser timestamp as “Expired at”. Preserve truthful zero-selection expiry; current0559:6421–6449 can complete an accepted OPEN tombstone with no business rows, while0708:17 refuses all non-SEALED runs. If this original tombstone is reached through restored intake readback, use its exact terminal typed expiry receipt to admit/display only that cancelled zero-member result; do not broaden ordinary unaccepted PROGRESS admission.

Office `js/bpay-next-office.js:1104–1113` currently calls both CANCELLING/CANCELLED terminal for preparation presentation, displays “Expiry time” and generic payments-returning/back copy. DR-08 must present partial cleanup as CANCELLING and show “Expired at … — this preparation's holds were released” only after exact native terminal completion. Confirmed DRAFT/EXECUTING/COMPLETE currently omit the expiry line; preserve that. `openWorkerReview:948–1033` has no “Expires DD/MM/YYYY hh:mm:ss” header yet; decorate it from the same existing PROGRESS read, with actor/run/owner/request-generation fences and continuous native scrolling. No added Candidate identity belongs here.

The server Confirm already emits `BPAY_NEXT_CONFIRM_PREPARATION_EXPIRED_OR_UNBOUND` at0559:5423, but Worker `bpay-next-preparation.js:518` collapses it to generic NOT_ELIGIBLE and review act960 displays a generic failure. Add only this exact closed refusal mapping/copy, including the already-CANCELLING expiry race, so Confirm explains “preparation expired” without weakening its server fence or retrying financial work automatically.

## Narrow delivery boundary

Required successors are the current0559 expiry page and only supporting binding guard/enrollment fields actually needed for its bounded projection continuation; current0708 progress/private scalar/public named page constructor; exact Worker/FE progress schema and Confirm refusal mapping; and the existing review/progress surfaces. Claim/due/receive/fence/financial release kernels can remain unchanged where their original inputs and phases still cover the new internal stage. Update exact final-definer/source-gate/contract closure entries after authoring, without broad exemptions. Any later changed helper must cite its current registry parent, not its older0510 preimage.

All runtime card vectors remain NOT_RUN here: actual72h partial cleanup, stale child refusal, Confirm race, released Work Ready, confirmed Draft after a week, retry/unknown replay and exact terminal summaries. No rehearsal or new business rows were used for this investigation.
