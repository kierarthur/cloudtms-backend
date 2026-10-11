# DR-05 shared worker-release design review

11 October 2026. Independent source/design review of saved `design/DR-05-worker-reprepare.md:1–35`. No product edit, SQL/DB execution, tests, fixtures, browser, hosted action or Git operation. Runtime/application acceptance **NOT_RUN**.

**Verdict: architecture sound; one essential design amendment remains before release implementation is complete.** Two shared exact mutation helpers with separately typed expiry and worker-release wrappers are appropriately narrow. Manual requests use existing PREPARE command/job kinds; E2 remains inside its current original PREPARE job, avoiding predecessor deadlock. The composite request, retained economic versus active execution revisions, native provenance, exact amount locks, once-only counts and bounded original selection projection are consistent with the card and current E2/E5. No second release calculator or generalized job protocol is needed.

## Required amendment: expiry during an unfinished release

Root note27 handles completed exclusions and active retry retirement, but must explicitly handle **an incomplete worker release**, including its financial release already complete while Workbench projection is still pending.

Current source proves the need: expiry enroller1832 `:1812–1815` admits only PREPARING/REVIEW/READY, rejecting a CANCELLING release worker;0510 `:393–408` walks all frozen positive lines and requires their holds ACTIVE, so a fresh expiry walk over a partly released prefix fails; `:474–498` performs worker cancellation and increments cancelled_candidate_count. A projected-but-not-DONE release can already have CANCELLED status/count ownership while lacking a completed release receipt. A blanket terminal-status exemption or restarting the walk is unsafe.

The narrow required branch is:
- Under original due72h expiry admission and header/control/job fence, resolve the exact card request and active release execution, retaining its economic state/revision, expected line/Case totals, WORK/CASE native cursors, released-prefix counts and whether the completed-exclusion count delta has already occurred.
- Retire only that admitted active execution (and required original input job) under the existing expiry authority. Preserve its native diagnostic/checkpoint/history and prevent the old child from writing again; do not adopt its live lease.
- Continue only the remaining release suffix through the same shared mutations. If its financial release has already closed, require its exact native receipt/noACTIVE closure and perform no second money walk. Do not pretend the incomplete request was already a completed exclusion.
- Carry cancelled/excluded count ownership once. The genuine expiry request must complete this member once while avoiding duplicate money, original gross/counter changes or a second cancelled-count increment.
- Use the existing bounded projection identities or explicitly complete the transferred projection before the final expiry receipt. Keep old request history exact and record its expiry handoff rather than allowing two terminal owners.

This is a necessary DR05 support in its shared0510 successor and1832 enrollment/guard; it does not weaken the captured deadline, PREPARATION_EXPIRY command, whole-run CANCELLING or native tuple fence.

## Conditions to preserve in the implementation

Root note19's manual last-execution-DONE admission avoids introducing a second retirement path for an unfinished ordinary prepare. System E2 must persist its six-code diagnostic and original failed financial checkpoint before terminal/projection handoff and must not allocate a later command behind itself.

The worker-release scope alternative belongs only at actual1236 reconciliation/guard and1519 identity guard for the exact PREPARE release phase/request. Existing expiry scope remains strict. Shared mutation helpers must resolve/validate native ownership, not accept caller-asserted money or an arbitrary record.

Counter transitions must be tied to the retained request/job checkpoint. Keep original selected/enrolled counts, distinguish completed exact exclusions from arbitrary CANCELLED workers, and block confirmation until all release/retry/projection requests finish. Confirmed-DRAFT replay and the original expiry deadline remain intact.

Root's no-current-Draft retry after completed release is an explicit design disposition; no released hold is reactivated. This avoids the broader reservation-generation migration identified in the retained-retry check. ACTIVE retry still needs the exact spent-cursor/data-revision association in all named Case consumers; the design note already records that necessary support.

This review covers architecture and source implications only. A PASS for implementation, source gates, compilation or application journeys is not claimed.

## Superseding assessment — deferred expiry acceptance

Root subsequently proposed the narrower alternative in the current task: `accept_preparation_expiry` probes indexed native pending RELEASE requests under the same run header lock and refuses `BPAY_NEXT_EXPIRY_WORKER_RELEASE_PENDING` **before** original command retirement, expiry command receipt or rootCANCELLING. The already accepted release completes its bounded release plus projection, including after the deadline; no hold creation is permitted by that continuation. Later genuine expiry admits the exact completed exclusion branch.

**Design verdict: PASS for this narrower alternative.** It resolves the incomplete-release race above without copying a financial cursor or retiring a partly spent release. The prior cursor-handoff recommendation is **superseded when this deferral is implemented**. The design must retain these precise conditions:

1. Existing exact expiry receipt replay remains first and permanent (`1832:1655–1664`). The pending probe runs under its existing rootUPDATE lock (`:1653`) and before any first-receipt writes (`:1710,1726–1735`). Manual/system release admission takes that same header, so neither can start after successful expiry admission.
2. Pending includes every accepted same-run RELEASE request lacking its exact completed release+projection receipt, including financially closed/projection-pending and unresolved failed cleanup. Use the card table's native indexed run/status/action predicates, not an unbounded history sum or arbitrary CANCELLED status. It must not block on completed historical release requests.
3. Already accepted release children and native projection continue after72h under their original request/current revision/nonce/epoch fence. They release/refresh only; no retry, capture, replacement hold or new pricing is admitted through this exception. Confirmation retains its independent after-lock real-clock deadline refusal (`2018:1483–1489`).
4. Current due discovery is a bounded keyset read (`0520:36–54`); dispatch advances/recycles independently of financial effects and explicitly revisits old headers (`0560:115–130`). Background `:77–85` retries an expiry refusal except the specific already-finished NOT_ELIGIBLE case. Thus a deferred root remains eligible and does not abort the recovery lanes or require a new scheduler/action/protocol.
5. Give the new55000 pending reason an exact retryable same-run mapping in the existing expiry RPC error classifier (`broker/src/bpay-next-preparation-expiry-rpc.js:210–213`); currently it becomes generic STATE_INVALID even though background still retries. Do not map it to NOT_ELIGIBLE/ACK.
6. After completed release, original due72h expiry still verifies its exact completed request/economic and current execution revision/noACTIVE closure, completes its expiry member once and avoids another money release or cancelled-count increment. Its existing deadline and whole-run command fences remain.

This is a design PASS, not product/source/SQL/runtime/application acceptance. No execution occurred.

