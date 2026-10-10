# PF-06 Worker measurement source review

10 October 2026. **PASS for the saved PF-06-D02 Worker hooks and PF-06-D03 future joined acceptance checklist. No actionable source defect found.** Independent source-only review: no code, tests, SQL, runtime, fixture, hosted or network execution; no product edits. PF-06-D01 has its separate saved lifecycle review. This verdict does not claim deployment or application/timing acceptance.

Read PF-06's full card (`06_CONTRACT_MOVES_DAILY_PAYMENT_SUMMARY_PERFORMANCE.md:1782–1803`), all three current checklist rows, `design/PF-06-SCOPE.md`/BD025, the existing scope/count census and lifecycle review, the complete financial job adapter, background and financial queue modules, the bounded index dependency change, both changed ordinary unit sections and the existing timeout/late-response units, and `PF-06-JOINED-ACCEPTANCE.md`.

## Observations and preserved behavior

- The real dependency chain is `index.js::bpayNextBackgroundDependencies` → `handleBpayNextBackgroundMessage` → `handleBpayNextFinancialQueueMessage` → `stepBpayNextFinancialJob`. The index supplies the observer; the two intermediate modules only forward it. Runtime enrollment/wake/expiry metadata calls retain their existing APIs and do not acquire a fabricated financial step observation.
- Each valid attempted financial CLAIM/STEP reaches the single optional observer in the adapter's `finally`. Invalid input or a missing transport never sets `attempted`. The existing one-call transport, 8-second deadline, bounded reader, validation, typed success and sanitized failure return remain intact. There is no second RPC, automatic repeat, re-claim or next-page call.
- The structured object contains exactly `job_id`, `kind`, `step`, `rows`, `bytes`, `ms` and is frozen before delivery. Identity/kind come from the validated input; step is CLAIM, STEP, or the finite validated **attempted input phase**, rather than the returned next phase. The index selects between two constant log labels. It logs no raw error, SQL message, body, Candidate/Work identity, amount, cursor, lease nonce/epoch, request headers or credentials.
- Rows are assigned only after the entire receipt passes the unchanged closed schema and identity/state validation. The value is the existing decimal-string per-call `rows_visited`, including genuine zero, or NULL when the owner's receipt has no applicable count. Cumulative member/applied/attempt counts are not substituted; zero is not used to infer replay. Malformed/unknown outcomes retain NULL rows.
- Bytes reuse the reader's actual chunk `byteLength` counter. A successful full read therefore measures encoded response bytes through UTF-8 decode, JSON parse and result validation; no JSON reserialization or Content-Length estimate is used. Refusal before reading leaves NULL. A partial/error read may report actual bytes already consumed, while rows remain NULL. Wall milliseconds start at the actual transport attempt and end at the bounded outcome, including response read/validation; this is not isolated database execution time.
- Exact `55P03` plus `WEEKLY_SOURCE_PAY_QUERY_ADMISSION_BUSY` is recognized from the original parsed non-OK response before `dependencyError` translates it, or from a thrown raw dependency error. The separate boolean selects the finite busy label without adding a seventh object field. Other lock/transaction retries keep the ordinary label and the existing error/retry/recovery contract.
- Timeout resolves the existing unknown-outcome recovery and emits its single final observation. A late transport/read continuation does not re-enter the observer or modify the already frozen values. Existing abort/cancel logic and CLAIM's refusal to adopt a late nonce remain unchanged. Async continuation errors are already owned by the existing Promise race.
- Synchronous observer exceptions are caught; a returned rejecting promise is given a rejection handler and never awaited. Logging does not participate in SQL success or queue disposition. Financial continuation send remains awaited before ACK, retained lease/cursor data remain unchanged, and send failure retains the existing retry behavior. No public closed request/response schema, SQL owner, queue envelope or provider action changed.

## Source units and future acceptance

The two new ordinary adapter units inspect exact six-field output, one transport/one observation, streamed encoded bytes, a valid visited count versus unknown single-step count, input PINNING versus returned RESERVE phase, exact busy message versus another `55P03`, and observer failure on success/refusal. The strengthened existing background CLAIM unit traces the actual dependency chain and retains `CLAIM → send → durable → ack` despite observer failure, with no nonce in the observed object. Existing timeout, cancellation, late CLAIM, malformed/oversized response, replay and closed contract assertions remain in place. These assertions were read; none were executed by this reviewer.

The D03 document preserves every relevant later clause: Draft clock/log correlation, small/large FIFO enrollment delay, cancellation/CSV return/PAYE completion and pending/refusal/replay, real Pay Query contention, six-field containment, and separate DB/Worker/queue/browser/memory timing. It correctly reuses a later joined receipt, preserves historical clock/count uncertainty, and leaves all full Worker/queue/minute/application checks **NOT_RUN**. It adds no fixture or harness and does not authorize hosted actions. Rule B and the original financial owners remain unchanged.

## Exact saved source hashes

| File | SHA-256 |
| --- | --- |
| `broker/src/index.js` | `164abc3f3836abbc95af562493fd25291014f0b6ec335e562b5af006e77f3f22` |
| `broker/src/bpay-next-background.js` | `d1d73c0559dda63c9b25b2799e1c2e0bb1055bf569bce82a25bb86ac81886855` |
| `broker/src/bpay-next-financial-queue.js` | `59bb0fc90f37d3e211f9bcdbf9a17ee3f1ecbfbd6e6d3b94197cf2369ba38e20` |
| `broker/src/bpay-next-job-step.js` | `53e2f466929e6640de7be4823aebf4979b81510c3de38931d198897e2de2c810` |
| `tests/bpay-next-job-step.test.js` | `69466aa0d2886adaa8b1c48b248b8cd90acf675df7987edd25477e8e077f5087` |
| `tests/bpay-next-background.test.js` | `18af9c90bbf95e9dfd05e20bad2861da8909cfd182c06167dcbe1a22abbc05bf` |
| `docs/bpay-next/PF-06-JOINED-ACCEPTANCE.md` | `c57390f2c80963002d6061f32b895dcbbd57e1ef65c8d1880939488670a42815` |

The index verdict covers the three-line observer injection and its existing background dependency seam, not unrelated code elsewhere in the shared file. Hashes bind the saved source read; installation and later timing proofs remain separate.
