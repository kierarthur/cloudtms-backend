# DR-05 expiry integration — independent source review

Source-only review of the saved, unfinished `11102026_0559_banking_pay_next_worker_reprepare_v1.sql`, its migration and the existing expiry Worker/service chain on 11 October 2026. Product writes belong to the root agent. No SQL execution, fixtures, tests, hosted calls or app acceptance were performed; runtime and item acceptance remain **NOT_RUN**.

## Essential finding at the first saved checkpoint

**Completed RELEASE is rejected before its zero-money branch.** Repeatable0559 `:4497–4499` requires every existing worker to be CANCELLING and to have captured_line_count equal to the expiry expected_work_count. Enrollment `:6526–6528,6558–6559` deliberately keeps a proved completed RELEASE worker CANCELLED and stores expected_work_count0. Its first page therefore throws `BPAY_NEXT_EXPIRY_FENCE_BROKEN` before `:4500–4509` can validate the exact completed receipt and traverse the native zero-money stages. Minimal correction: qualify only the generic worker predicate with `not v_binding.already_worker_released`; preserve the outer RELEASING requirement and the exact completed-receipt/no-money check at `:4501–4503`. This was sent to the root immediately. No product or item PASS is claimed.

## Pending RELEASE mapping and native rediscovery

The current saved Worker already contains the necessary finite correction: `broker/src/bpay-next-preparation-expiry-rpc.js:210` maps only SQLSTATE55000 plus exact `BPAY_NEXT_EXPIRY_WORKER_RELEASE_PENDING` to the existing `TRANSACTION_RETRY,true`. Its existing known-error result therefore remains outcome_unknownfalse and uses `RETRY_SAME_RUN`; other55000 refusals retain their prior classification (`:211–214,297–300`). A new public result/code/route or scheduler branch is unnecessary.

`public.bpay_next_preparation_expiry_receive_v1` in repeatable0520 `:78–87` preserves its SQL-derived same-run command identity, current stored deadline and null SYSTEM actor/real-clock operands. Accept0559 holds the original runUPDATE, replays an existing receipt first (`:6359–6373`), checks the genuine stored deadline/due state and then refuses pending same-run actionRELEASE/status<>COMPLETE (`:6382–6393`) before any command retirement, expiry receipt or rootCANCELLING write. Migration0559 `:81–82` supplies the exact partial native pending-run/action index.

`bpay-next-background.js:77–87` ACKs only known NOT_ELIGIBLE; every other refusal retries the same `{id:run_id,kind:'EXPIRY'}` message with its existing delay. Its minute EXPIRY lane `:125–129` publishes the bounded native due page. Repeatable0520 `:36–54` keeps an unconfirmed PREPARING/REVIEW root eligible after a pending refusal. Repeatable0560 `:111–134` advances its bounded sweep independently of financial effects and resets after a completed cycle, explicitly revisiting lost/failed headers. No cursor hold, new message envelope, active lease adoption or ACK mapping for the pending refusal is needed.

## Retained expiry fences and closures observed

- Original original-input PREPARE retirement is retained at0559 `:6510–6520`: exact original command/Candidate/PREPARE/module, DONE/EXPIRED, cleared lease and `BPAY_NEXT_PREPARATION_EXPIRED`. The distinct active execution is retired only after exact native request/original input/current execution/economic/selection/module/Candidate validation (`:6533–6545`). Neither original retained cursor nor captured Case instruction is reset.
- Enrollment records economic captured_preparation_revision separately from expected_execution_revision and finds the original DRAFT state by economic revision (`:6529–6531,6551–6559`). The initial expiry guard checks both revisions plus original immutable worker diagnostics and ordinary counts, admitting zero counts/CANCELLED only through the exact completed-release helper (`:6628–6648`). Existing UPDATE guard leaves the added fields immutable and retains one-checkpoint, monotone cursors/counts and legal native stages (`:6658–6667`).
- The completed helper `:6322–6342` point-joins the current target request, active jobDONE/CANCELLED, original input jobDONE, original run command and current module. It requires RELEASE/COMPLETE/COMPLETE before the expiry accepted cut, exact economic/selection and released counters, no ACTIVE Work/Case holds and no realised/net/Transfer state. It does not infer completion from workerCANCELLED alone.
- The zero-money branch uses legal WORK→CASE→FINAL→COMPLETE stages (`:4504–4509`), with no release mutation. Terminal processing skips week exclusion and cancelled_candidate_count increment for an already-released member (`:4548,4555–4559`), still increments expiry completed_candidate_count once (`:4550–4554`), and sets rootCANCELLED/expiry commandCOMPLETE only when all native expiry members are finished (`:4563–4566`). This prevents an already-saturated cancelled count from prematurely closing a multi-member expiry.
- The exact active REPREPARE request is completed at expiry member closure (`:4560–4562`), while the existing action receipt reports EXPIRED through the original expiry fence (`:31–40`). Later receipt handling retains the original command/deadline and verifies a non-original job through its matching immutable native retry request (`:6733–6762`). Further focused comparison of the Case state job association remains in progress at this checkpoint.

The original72h deadline, confirmation exclusion, original0550 LEGACY writer fence, and original expiry lease/order/provenance rules are requirements retained by this review. A source observation is not an installation or operational proof.

## Correction and final focused source checkpoint

Re-read of the saved repeatable confirms `:4497` now applies the ordinary CANCELLING/count predicate only when `not v_binding.already_worker_released`. The outer RELEASING requirement and the exact completed-release validation at `:4501–4503` remain. The demonstrated blocker above is **corrected**; its first-checkpoint finding is preserved as review history.

The Case state job comparison is resolved without another change. Original0510 `:269` required state.job_id=original input job. Current0559 `:2044–2050` can first create the same captured economic DRAFT state during a later exact REPREPARE execution, before which no Case state existed; its job_id is therefore legitimately that later execution. Current expired managed-collection scope `:6693–6695` keeps exact expiry draft_state_id, worker, Candidate, economic revision, selection, DRAFT and projection0 and resolves the retained state's job through the native original-run-command/request helper (`:5–14`). Original instruction/result/hold/capacity provenance and amounts, active execution, due72h, lease/order and no-realised/net/Transfer conditions remain (`:6685–6727`). The manual release scope uses the same retained-state association (`:332–354`). Keeping the literal original-job comparison would reject this valid first-state-on-retry path. No new economic state, money formula or hold generation is introduced by this association.

The expired receipt's original-input branch remains intact; its non-original job is accepted only through matching request job/command/worker/run/Candidate/module/input-original-command/current execution/economic/selection fields (`:6754–6760`). Current saved registry names0559 for accept/enroll/guard/expired receipt/expired collection scope. Original expiry claim, expiry fence and reconciliation wrapper remain0510; public due/receive remain0520; recovery sweep remains0560; legacy writer assertion and trigger remain0550. The audited migration replaces only the recognized original worker-shape CHECK and names the replacement `bpay_next_expiry_worker_shape_v2_ck` (`:92–113`), retaining ordinary and no-worker shapes alongside the explicit zero-money completed-release shape.

No further essential source defect was identified in the reviewed pending-refusal, completed-release bypass, exact retry retirement/receipt and parent-count seams. This is **not an item completion, runtime, installation or app acceptance verdict**; all such acceptance remains NOT_RUN, and the root owns required compilation/source gates.

Saved source SHA256 at this correction checkpoint:

| Source | SHA256 |
| --- | --- |
| repeatable0559 | `2273C25235E12A395B1E15271A8AF0BF7DC108389196CDB2C2F483B41D5833D4` |
| migration0559 | `1619C1D4EF5B06D0086E9757CE4B43D7C4D316ABAF757B2B1D814F8171557C20` |
| bpay-next-preparation-expiry-rpc.js | `8297108442FDD3036B1B78D5E313754E61B9C240D897589FFC0EF2C6337465E5` |
| bpay-next-background.js | `0524712D1BF52209EEA3A7F8846491BAC4FC452B2EA593978C85162476049514` |
