# DR-05 Worker transport source review

11 October 2026. Independent source-only review of the saved four Worker changes, their unchanged original route/RPC owners and the current unfinished0559 SQL receipt/phase contract. No product/checklist edits, SQL load or execution, database/hosted calls, tests, fixtures or screenshots. **Source verdict: PASS for the narrow Worker delta; no concrete defect found.** This is not a DR05 item, installed contract, runtime or application PASS. All native-adapter execution and application acceptance: **NOT_RUN**.

## Reviewed behaviour

| Check | Source result and evidence |
| --- | --- |
| Only two Office mutations | PASS. preparation.js adds only WORKER_REPREPARE / WORKER_RELEASE, both exact command_id + run_worker_id + expected_preparation_revision. commandCall retains unknown-key rejection, positive bigint-string validation and UUID normalization. Active admin/actor injection and original intake endpoint are unchanged. No browser SYSTEM/job/nonce/cursor input. |
| Exact action receipt | PASS. Both schemas require the0559 tuple: command/run/worker IDs, internal action, expected and target preparation revisions, review revision, agency sequence, phase and replay. Worker ID/expected revision joins existing identity checks; internal action must match submitted action; target must equal expected +1 via BigInt. Response extra/missing keys fail closed. No inferred server money or count. |
| Closed action phase including expiry | PASS. ACCEPTED, RUNNING, COMPLETE, EXPIRED are explicit, and only those values are accepted. Current0559 worker_action_receipt deliberately emits EXPIRED from original preparation-expiry fence; it preserves original accepted target/review/command identity. Receipt validation therefore admits that truthful replay without mistaking it for another action or revision. |
| ENROLL after accepted action | PASS. Only valid success for SEAL_ENQUEUE or either new action requests the existing ENROLL header. Existing index publisher consumes that header and sends exactly {id:'1',kind:'ENROLL'}. RPC transport derives its names from the same closed input maps. No new route/command/job kind, queue shape or drain. |
| Release CLAIM/input/checkpoint | PASS. Three explicit WORKER_RELEASE_WORK/CASE/FINAL phases enter PREPARE claim/input maps. STEP uses exact integer checkpoint keys with positive?-string expected_cursor and limit1–100; Work/component pair is forbidden. prepareCheckpoint requires both pair cursors null. CANCELLED is an output terminal only, never a claimable/input phase. |
| Release result states and completion | PASS. Intermediate release phase requires CANCELLING and done false. Workbench projection now permits CANCELLED with done false. Terminal CANCELLED requires matching worker status, done true and all-null checkpoint. Original READY/REVIEW, issue/work-ID association, count bounds, replay-zero and null STEP lease checks remain. The separate EXPIRED null-worker exception remains separate. Existing workerStatus enums already included both cancellation statuses; no broad new enum was required. |
| Queue continuation and unknown recovery | PASS. Release phases construct the same integer expected_cursor + bounded limit payload and join the strict same-phase BigInt advance test. Phase changes can return null cursor; a same-phase null/nonadvancing checkpoint is rejected. Handler still fresh CLAIMs, executes at most one STEP, ACKs done, otherwise publishes the unchanged wake identity before ACK. Unknown claim/send/STEP preserves original recovery and lease-expiry rules; no active-lease adoption or automatic mutation drain. |
| Expiry waiting for worker cleanup | PASS. Only SQLSTATE55000 + exact BPAY_NEXT_EXPIRY_WORKER_RELEASE_PENDING gains retryable TRANSACTION_RETRY. Other55000 refusals retain their old classification. sqlFailure establishes a known rollback refusal, so RECEIVE returns retryable RETRY_SAME_RUN with outcome_unknown false. Network/invalid reply uncertainty remains separate. |
| E2 original failure truth | PASS scoped to read source contract.0559 copies original_issue_code/work ID and retained financial checkpoint into the typed request, then clears current Worker issue fields on release revision. Public wrapper therefore sends null issues during CANCELLING/CANCELLED; current job-step keeps ordinary “reviewing iff issue present” and issue/work-ID validation intact. It does not pretend release is unresolved REVIEW or lose the retained historical cause. No wildcard issue allowance or new automatic-release job. |

The unfinished SQL’s release FINAL changes worker to CANCELLED, debits preparing and refreshes through WORKBENCH_WORK/CASE before terminal CANCELLED. The reviewed adapter admits this exact progression and does not stop just because the intermediate worker status is CANCELLED. SQL lock order, hold conservation, counter correctness, retry economic identity and compile/load validity remain separate database-source reviews; they are not proved by this transport receipt.

Existing index comment still names accepted SEAL_ENQUEUE alone, although its generic header consumer correctly handles the two added actions. This is a stale comment, not a runtime defect or a reason to add another send path. Existing parked accepted-input/progress readers are outside this contract and were not treated as an installed DR05 read path.

## Saved-byte SHA256

| Reviewed source | SHA256 |
| --- | --- |
| broker/src/bpay-next-preparation.js | `e0fcb152503dfda1bceff278918a34e2f575f5eaf71b264e34b76cce90cff602` |
| broker/src/bpay-next-job-step.js | `06bab120f75ae85bb95650026a1fd6d941a2f418c07a62546f86b5e37ce957ab` |
| broker/src/bpay-next-financial-queue.js | `a667a86c6b9a7707cea2184edf150d05fe6c4eb68f10ddf0b506de753c07e8b7` |
| broker/src/bpay-next-preparation-expiry-rpc.js | `8297108442fdd3036b1b78d5e313754e61b9c240d897589ffc0ef2c6337465e5` |

Original route/authority owners read: broker/src/bpay-next-preparation-rpc.js, bpay-next-job-rpc.js, index.js exact preparation publisher, and the0559 draft. Source review inspected native adapter guards directly; no adapter was invoked. Real pixels, joined app, runtime, SQL installation and item acceptance all remain **NOT_RUN**.
