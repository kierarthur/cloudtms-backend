# DR-05 exact Worker adapter integration

11 October 2026. Independent read-only review of current saved Worker source and **unfinished** `11102026_0559_banking_pay_next_worker_reprepare_v1.sql`. No product/checklist edits, execution, database calls, fixtures, screenshots or test programme. This is a required-delta note, not implementation approval. Actual adapter/runtime and application acceptance: **NOT_RUN**.

## Immediate answer

Existing `workerStatus` maps **already allow CANCELLING and CANCELLED** (`bpay-next-job-step.js:135`; `bpay-next-preparation.js:220`). Keep them. Current rejection is in PREPARE phase/checkpoint/done and state-specific result guards, plus continuation payload construction. Use only the original preparation endpoint and existing PREPARE command/job kinds; no new release job kind, RPC family, queue envelope or public screen reader.

The unfinished SQL already chooses **WORKER_RELEASE_WORK → WORKER_RELEASE_CASE → WORKER_RELEASE_FINAL → WORKBENCH_WORK → WORKBENCH_CASE → CANCELLED**, with a nullable positive bigint checkpoint + bounded limit for the three release phases. CANCELLED becomes terminal only after both native Workbench refresh walks finish. The browser/Worker must not treat the release-final hold cleanup, acceptance receipt or intermediate CANCELLED worker status as job completion.

## Exact preparation transport delta

`broker/src/bpay-next-preparation.js`:

1. Add exactly WORKER_REPREPARE and WORKER_RELEASE to BPAY_NEXT_PREPARATION_INPUTS. Both have exactly `{command_id:'uuid',run_worker_id:'uuid',expected_preparation_revision:'positive'}`. This is the current SQL intake key list at0559:5152–5153; do not add run_id, actor/system flags, job_id, approval IDs or caller phase/cursor.
2. Add the matching **exact** mutation OUTPUTS shape, from `bpay_next_worker_action_receipt_v1` at0559:34–39: command_id UUID, run_id UUID, run_worker_id UUID, action closed REPREPARE|RELEASE, expected_preparation_revision positive string, preparation_revision positive string, review_revision positive string, agency_sequence positive string, phase closed ACCEPTED|RUNNING|COMPLETE, replay boolean. Correlate internal result action to the outer submitted action.
3. Extend mutation result identity comparison at preparation.js:287–290 to include **run_worker_id** and **expected_preparation_revision**. Require target preparation_revision = expected_preparation_revision +1 using BigInt, never Number. Native SQL rejects max-bigint expected revision. run_id is returned, not a submitted argument; the Office caller must qualify it against its accepted run context.
4. Extend the existing accepted ENROLL expression at:523 from only SEAL_ENQUEUE to these two additional actions. Any valid replay acceptance may nudge the same native enrollment recovery. Reads and unrelated mutation pages stay non-waking.
5. Keep existing safeError truth. Exact state/revision/checkpoint refusals are definite NOT_ELIGIBLE/CONFLICT/REQUEST_INVALID as their existing SQLSTATE maps dictate; timeout/invalid dependency reply after dispatch remains outcome_unknown. Do not falsely report a success or add an automatic second mutation.

`bpay-next-preparation-rpc.js` imports the maps and already sends p_action/p_args to **bpay_next_preparation_intake_v1**; no new endpoint/map is necessary. `index.js:198855–198878` already consumes the handler’s ENROLL header and sends exactly `{id:'1',kind:'ENROLL'}`; only its now-SEAL-only comment requires updating. The same original enrollment/service owns these accepted PREPARE requests.

## Exact PREPARE job adapter / queue delta

The adapter is `broker/src/bpay-next-job-step.js`, with continuation in `bpay-next-financial-queue.js`. There is no separate saved prepare-adapter file. `bpay-next-job-rpc.js` forwards existing PREPARE to bpay_next_financial_job_v1 unchanged.

| Current source location | Minimal required change |
| --- | --- |
| job-step:36,49,62,129–131 | Add the three release phase names to FinancialJobPrepareCompleteStep, PREPARE_PHASES (and thereby CLAIM_PHASES/prepareClaimPhase). Add CANCELLED to **preparePhase terminal outputs only**; never to claimable/input phases. |
| job-step:162–170,193–195 | Each release phase must require the same exact `lease_nonce,owner_epoch,expected_phase,expected_cursor,limit` key set and positive?-string / integer1–100 validation as FINANCE_ALLOCATE and the Workbench phases. Do not weaken unknown-key rejection or admit the Work/component pair in a release phase. |
| job-step:289–296 prepareCheckpoint | Treat each release phase as integer checkpoint: work_cursor and component_cursor must both be null; cursor nullable positive string. CANCELLED terminal requires all three cursors null. CLAIM replay must still reject a mixed tuple. |
| job-step:266–280 PREPARE result guards | Introduce exact release-phase classification. Nonterminal release phases require CANCELLING, done false, null issue fields. Workbench projection permits CANCELLED **with done false**, in addition to original READY/REVIEW. Terminal phase CANCELLED requires worker_status CANCELLED, done true, null cursors/issues. Preserve original REVIEW/READY rules, count≤limit, zero replay-count rule, and nonterminal nonce/epoch/deadline truth. |
| financial-queue:96–110 | Add the three release phases to the same-phase integer checkpoint progress test. Require strictly increasing checkpoint on an unchanged phase; a zero-row native phase transition is allowed. Compare bigint strings with BigInt. |
| financial-queue:130–141 prepareContinuation | For each release phase construct expected_cursor=result.cursor and bounded limit, exactly as the existing Workbench phase branch. Keep no extra keys. CANCELLED done is stopped by existing terminal handling and never creates a continuation. |

Existing OUTPUTS.PREPARE retains the exact original tuple: job_id/job_kind/owner_epoch/lease_until_utc/done/replay/phase/cursor/work_cursor/component_cursor/run_worker_id/worker_status/issue_code/issue_work_id/rows_visited. The0559 public wrapper at5044–5055 supplies that tuple for release and terminal cancellation. No output field expansion is currently necessary. Keep the separate EXPIRED null-worker schema; do not use that exception to make ordinary release worker IDs optional.

Current SQL release page at0559:472–529 increments its own checkpoint on each actual release/final page and changes phase when ready. FINAL reports one row (within minimum limit1), transitions to WORKBENCH_WORK with cursor null, worker CANCELLED and done false. Projection at2464–2479 alone returns terminal CANCELLED/done true after bounded Work + Case refresh. A guard that requires PREPARING for every nonprojection phase or READY/REVIEW for every projection will reject these legitimate receipts unless narrowed as above.

## Original system issue handling versus E2

E2’s exact six-code set already exists in PREPARE_ISSUES. Do not add a wildcard issue/code status or a new automatic-release kind.

Current0559 `begin_prepare_projection_v1:2374–2383` captures the original failed checkpoint, admits the same private RELEASE request for one of the six E2 failures, and returns nonterminal WORKER_RELEASE_WORK. `accept_worker_action_v1:132–151` stores original_issue_code / original_issue_work_id in the typed request and clears the current worker issue fields on the new revision. The public PREPARE wrapper reads the new worker issue fields; consequently this source’s release and CANCELLED projection receipts have **null issue fields**. Original failure history remains in the typed request. This does not conflict with strict ordinary issue/work-ID validation once release/projection classification is added.

Do not broaden the ordinary “reviewing iff issue present” rule to allow arbitrary diagnostics in any PREPARE phase. Retain it for original REVIEW and REVIEW projection. Current E2 release is a separate execution state, not an unresolved REVIEW. If the builder later chooses to return retained issues in release receipts, that becomes an explicit changed SQL contract requiring its exact issue/work-ID validation; this note approves no such broadening.

Source contracts must remain exact even while0559 is incomplete. Re-read its saved receipt and phase bodies immediately before editing product. This note authorises no runtime, business mutation or installation; those remain separate root-owned steps.
