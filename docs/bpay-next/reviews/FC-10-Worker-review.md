# FC-10 independent Worker and Office contract review

11 October 2026. **SOURCE PASS after the final qualification amendment below.** The original two findings are retained as superseded review history. This is a read-only source review of the two native status actions and their existing transports/controller. No product/checklist edit, database/business call, test, execution, fixture, hosted action, screenshot or commit was performed. SQL is reviewed separately; the native confirmation and A4 are not approved by this receipt.

Authority: full approved card `PLAN/03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:1217–1236`, all ITEM/D01–D10 checklist rows, R3-A265/A266/A267, R2 Candidate-wide fence and current `FC-10-builder-design.md` (BD120). PLAN is the ordinary backend's approved R3 pack; BE/FE are the active build worktrees. BD120 now records factual FUTURE_ONLY admission followed by strict ordered WHOLE_CASE application; it accepts a pending intent without claiming a status change.

## Required corrections

1. **P2 — a completed STEP must reject PENDING.** `BE/broker/src/bpay-next-job-step.js:234–235` requires `done=true` for CASE_PAUSE/CASE_RESUME but delegates to `casePauseReceipt`, which permits a PENDING receipt with all result fields NULL. That combination currently passes the closed decoder and can be ACKed by the existing queue. These are single-step terminal owners: require CASE_PAUSED, CASE_RESUMED or BLOCKED for their STEP response. PENDING remains legitimate only for the original CASE_COMPONENTS readback.

2. **P2 — a protection refusal must retain the required native disclosure.** `BE/broker/src/bpay-next-case-contract.js:90` and the FE mirror only require an issue and NULL applied fields for BLOCKED. Thus PROTECTED_SLICE or PROTECTION_PENDING with NULL summary/reference passes, allowing `Office.finishCasePause` to acknowledge the original command without the card's amount/action facts. For those two issues require a nonzero native protected count and first protection. PROTECTION_PENDING retains NULL protected amount/capacity; the known protected-slice result retains a non-NULL protected amount. The zero-count summary should also require a numerically zero protected amount, rather than any non-NULL money string. These are consistency checks on existing server operands, not browser money derivation or a new contract.

Other refusal reasons may legitimately have NULL summary/reference (unsupported Case or invalid native balance); do not impose protection facts on every BLOCKED code. A changed-revision refusal may describe a later current revision, so do not bind its summary cursor to the original expected revision.

## Reviewed retained behavior

- Exactly CASE_PAUSE and CASE_RESUME are added to the existing command intake, financial RPC, job claim/step RPC, runtime wake registry and financial queue identity map. The queue keeps the original two-field id/kind delivery, fresh native CLAIM and one STEP. It neither borrows an active lease nor supplies financial inputs.
- Inputs are closed command ID, Case ID, positive native expected revision and required-nullable reason. The reason is bounded to2048 UTF8 bytes/no NUL, and only these actions receive the16384 serialized command ceiling. Bigint max is excluded before the required increment. Existing2048/Create/Edit limits, authority checks, timeout and unknown-response handling remain.
- Admission is distinct from completion: action, Case, expected revision and original command are crosschecked. Terminal success requires the action-specific status and exact expected+1 revision; no browser recomputation of balances/allocation occurs.
- CASE_COMPONENTS retains its existing kind/cursor and adds the typed state and optional exact original command readback. Candidate/Case/command identities are checked at both Worker and Office boundaries. Unrequested receipt fields must be NULL; requested readback must exist. No extra history/list/route is introduced.
- Version7 journal adoption accepts each exact prior version shape and adds only NULL case_pause, retaining the original pending bytes. Accepted pause records retain original action/IDs/revision/sequence and canonical request_bytes, qualify Case/Candidate on reload, and block a fresh mutation until the original terminal receipt. finishCasePause binds every original operand and refuses PENDING; the terminal qualification above must be fixed before it is sufficient.
- Exact unknown retry continues dispatching the retained original bytes. A later refusal does not clear an earlier unknown request. Reader-only accepted Refresh may observe later Case state while the immutable original result remains the acknowledgment authority.

The generic Banking Pay controller is separate from the Case-qualified confirmation journal; native caller integration/parent fences and final screenshots remain separate review work. No allocation/prepare/kernel/producer rewrite is warranted by these transport changes.

## Reviewed source snapshot

| File | SHA-256 |
| --- | --- |
| BE broker/src/bpay-next-case-contract.js | 05b0adee617d27443a674dd6df657ac8ec4b94e5f8d17dfd9820f9362c668184 |
| BE broker/src/bpay-next-command.js | 5e9126530f6e4d3e28f49a3bf2c40096c40b83e6f0d43195ec312ebeed2df85f |
| BE broker/src/bpay-next-command-rpc.js | daacf7eb1b47b28a66cbc81d44557b6824b0a0b52453e8c0218de1fe96594e85 |
| BE broker/src/bpay-next-job-step.js | 36f481e3865a277b681633542e9a55f85431d1d2ed56abaf9a68c513388e138e |
| BE broker/src/bpay-next-job-rpc.js | 65454073073b13c740db5557a9a84a3cd6f64ef71f8493b2ef8a1fbfae7f5b26 |
| BE broker/src/bpay-next-preparation.js | ccb6ef70bf87f5a6bef6ccf0482a5f30c34db75b4b20b2a00b4c4da7c73f9db2 |
| BE broker/src/bpay-next-runtime-rpc.js | 55e682eb8421314ab603c13d5913d365cfe90dcb0399a83167d01dd7a7aa232c |
| BE broker/src/bpay-next-financial-queue.js | 19f08cbdd3ba40b9439b99cc274d7fa48b98daed82bbd31207ca58c5c638cd68 |
| FE js/bpay-next-office.js | ebf5237c32438a7ff3b1e94944d0c07221bc7391b027621ecd473a8fc8e433eb |
| FE js/bpay-next-case-preview.js | 9280b67bc7fb56beff5b5c49d0f53f2109322e5a4d2fdaafd43e50f5fc8e3a71 |
| BD120 design | 8fa9aa9123f2481102ae94c228aa78429cb2bb89477cf00bcc4ff9f308c72b60 |
| SQL2041 read solely for native receipt shape | 80675b1f77af852977da427d0ce0702bfb2a2da6fafcea2a5e05b731810de44b |

Runtime/joined acceptance remains AUTHORED_NOT_RUN; this review does not mark FC10 built.

## Final qualification amendment

Fresh saved bytes independently reread on11 October2026. Both findings are closed: the two single-step STEP decoders explicitly refuse PENDING, while the original reader may still return that legitimate nonterminal receipt. Both shared contract copies require positive protected membership and the complete typed first native reference for PROTECTED_SLICE/PROTECTION_PENDING. Pending amount/capacity remain NULL; a known protected slice requires a non-NULL amount. Zero-count summaries require a zero monetary text, with existing closed numeric validation retaining its bound. Successful action/status/revision and original journal identity checks remain intact. No new field, protocol or financial formula was added.

**Final SOURCE PASS** for the reviewed Worker/RPC/queue, shared contracts and Office V7 journal changes. No further essential defect identified in this scope. Native module/caller review, SQL review and A4 remain separate; runtime/joined acceptance remains AUTHORED_NOT_RUN.

| Final changed source | SHA-256 |
| --- | --- |
| BE broker/src/bpay-next-case-contract.js | 41f21248a3044b88840537419d2571d7e23c1b04f15f3dfadb3a21925b2505df |
| BE broker/src/bpay-next-job-step.js | d1a1349423ff00c7ed2713ea053e7d334c2015eeb250e8f2a33aa8aa42b07608 |
| FE js/bpay-next-case-preview.js | dac5dbf44b4dcbbd9825412239d312750409cf63d0a9658965093f6df6837226 |
| FE js/bpay-next-office.js, unchanged | ebf5237c32438a7ff3b1e94944d0c07221bc7391b027621ecd473a8fc8e433eb |

The remaining six Worker snapshot hashes above were freshly checked and remain unchanged.
