# DR-37 early D14/D18 fence prerequisite — revised scope

10 October 2026. Read-only design revision under the owner's latest independent-assessment correction 7, recorded in BUILD_LOG.md:398. Full DR-37 card (03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:716–738), exact D14/D18 rows, addendum §2, current master chains and native schema/owners were reread. No product source or test tools were changed; no SQL, runtime, tests, fixtures or hosted calls were executed.

**This revision supersedes the entire previous early-fence proposal in this file.** In particular, the old recommendation to build a real replacement table/FK and D17 high-water/pending columns early is withdrawn. Phase 3 builds only **D14 candidate_week_commit_fence and D18 assert_candidate_financial_fence**. D12 net_replacement, D13 staging, D15 refusal, D16 cancellation handoff, D17 columns and D19 money guards remain with their named later card deliveries. F-26 remains unbuilt at sequence 280.

## Ordering and current authority

Schedule the separate D14/D18 prerequisite after the required core/Case identity schema and before the first DR-08 writer successor needs the assertion. Do not move the complete DR-37 card (sequence 130) earlier or add a cyclic whole-card dependency. Later full DR-37 still depends on DR-18/DR-21/F-16 and completes the entire replacement/refusal protocol; the early contract does not prove that journey built.

The existing successor owner adds its one fence assertion at its scheduled turn. Early D14/D18 do not redefine 0360,0390,0430,0460,0510,1647,0200,0490 or FC mutation owners. Preserve expiry chain DR-08 → DR-05 → FC-15 → CM-13a → CM-15, case NET DR-37 → DR-23 → DR-22 → EX-13 → CM-13a, and the recorded Case-prepare chain. No new producer/enroller/PIN/clock protocol is introduced.

The Worker STATUS chain is explicitly **04102026_0580 → PS-14 (10102026_0639) → later DR-37**. Current registered private.bpay_next_worker_status_page_v1(uuid,uuid,integer,integer) finalPhysical is 0639. Later DR-37 must start from that full current body and retain all PS-14 reservation header/leg fields and native lifecycle checks. Master row 76's old “one editor/no chain” wording is superseded by correction 7; returning to raw 0580 would erase delivered PS-14 behavior. No STATUS successor is part of this early slice.

Rule B remains binding: accepted Create inputs are selected expected revisions/captured Candidate channel, VAT/floor, query snapshot and existing pin_v2. Do not revive 2153/resolver/producer-version designs. F-26 is not an implicit dependency or a claimed automatic old-epoch takeover implementation.

## D14 can bind a real operation without D12

Use the already installed immutable PAYE NET request and job as the operation identity. Existing source proves these keys:

| Existing native object | Source/key |
| --- | --- |
| run_worker | core1324:470–500; PK id and UNIQUE(id,candidate_id), native run_id |
| paye_net_request | core1324:828–840; PK command_id, UNIQUE(worker,request_no), same-Candidate worker/member FKs; immutable original request |
| command/member | core1324:795–821; exact ordered agency_sequence/module_epoch and (command_id,candidate_id) membership |
| job | core1324:967–991; actual command/Candidate/sequence/module, job_kind/status/phase, lease_nonce/deadline/owner_epoch |
| composite job owner | case0300:303; UNIQUE(id,command_id,candidate_id,module_epoch) already exists |
| Candidate serialization | worker_control at core1324:779; active_owner_epoch and Candidate PK |
| exact week | worker_week at case0300:598–608; PK(candidate_id,pay_week_start), finite Monday basis |
| actual NET target | case_allocation_state at case0300:304 onward; exact command/job/module/worker/Candidate; existing PAYE NET state binding widened by 0380 |

Recommended minimal D14 columns are candidate_id, pay_week_start, command_id (the real immutable NET-request command, not a new opaque operation UUID), run_worker_id, original_job_id, module_epoch, allocation_state_id (the real NET target state), closed fence stage, and server-created/updated timestamps. Use a primary/unique (candidate_id,pay_week_start) current fence identity and a partial UNIQUE(candidate_id) over active stages, so an operation in one week excludes conflicting shared-capacity writes in every other week of that Candidate.

Use existing FK keys only: same-Candidate worker FK; real request command_id FK; same-Candidate command-member FK; (original_job_id,command_id,candidate_id,module_epoch) native job-owner FK; existing Candidate/week FK; existing allocation-state PK FK. D18 must recheck the immutable request's exact worker/Candidate and the NET state's exact command/job/module/worker/Candidate/week; a single-column state/request FK alone does not prove those cross-links. No new replacement schema or fabricated replacement_id is needed to make this assertion real.

Do not put a nullable future replacement ID in the early table as authority. Later D12 can add its exact replacement FK/link to these already real command/job identities, once D12 exists. Likewise no net_request_high_water or net_replacement_pending_id is read or written by early D18.

Closed active stages are STAGING (only the final dependency-admission fence), COMMITTING, CANCEL_REQUESTED and CANCELLING. COMPLETE/REFUSED/CANCELLED are inactive terminal states. There is no PRECHECK fence by default and no lease-expiry auto-clear. The exact acquisition, stage transitions, terminal retirement/reuse and immutable cancellation-handoff authorisation remain the later genuine 0390/D12/D16/D19 owners; early D14 creates no active row and adds no acquisition/cancel RPC. Its row identity cannot be replaced while active. Terminal-to-new-operation reuse must receive the full later guarded owner implementation, not an early convenience reset.

Keep private owner-only RLS/FORCE RLS, audited logical-owner policy/ACL and native FK/index discipline. No browser or service-role direct table writes, no public/service grant for D18, and no financial snapshots/counters/calculator or parallel ledger. Private objects use the repository release owner adapter.

## D18 exact assertion contract

private.bpay_next_assert_candidate_financial_fence_v1(candidate_id uuid,job_id uuid,lease_nonce uuid,owner_epoch bigint) RETURNS void; private SECURITY INVOKER, fixed pg_catalog/private search path, audited owner-only execution. It neither receives a replacement request nor acquires/adopts/clears a fence.

1. Reject a null Candidate. Under the existing Candidate-control order, obtain the same native serialization protection used by the actual caller and look up the Candidate's active fence by its partial unique key **without filtering to the caller's week**. No active fence returns normally; the caller still performs all its original service/module/permission/lease/order/business checks. It is not a grant of eligibility. No count scan or cap is needed.
2. For an active fence, verify its real immutable NET request, same-Candidate worker/member/job, current NEXT module epoch and exact NET allocation-state scope. The allocation_state must match the stored command/job/module/Candidate/worker and exact pay_week_start and be a NET pass; do not choose a latest state. Identity corruption fails closed as integrity failure, not “no fence”. Existing 0380 request/state links remain untouched.
3. An exemption is allowed only for **the exact original NET job already stored in the fence**, with actual PAYE_NET_ENTRY kind and its current valid FINANCE_ALLOCATE lease: matching p_job_id/p_lease_nonce/p_owner_epoch, unexpired native deadline, job/module epoch equal to current NEXT, and worker_control.active_owner_epoch equal to the actual job epoch. Confirm native command_sequence matches the immutable command; use the original request and target state. No persisted stale nonce, browser claim, current pointer, unrelated job or GUC proves ownership.
4. Same-job ordinary lease renewal/takeover can pass only through the already authoritative native job's fresh lease and unchanged operation/module identity. This does not implement F-26 cross-module-epoch resume. An old module epoch remains refused until its later explicit owner is built; expiry is never permission to delete or adopt the fence.
5. Any other financial writer, another week's writer or an unproved cancellation delegate receives prompt retryable SQLSTATE 55P03 with the single bounded Candidate-financial-fence busy reason. Preserve existing progress/cursor/holds/capacity; do not set REVIEW or call it a late business refusal. A lease-free writer can pass its absent job proof only when no active fence exists; it has no active-fence exemption.
6. A later SIMPLE_CANCEL/RELEASE delegate is **not** admitted in this early contract. Full D16/D19 must add exact immutable original-NET-cleanup/handoff plus 0430 certificate checks before allowing its real later RELEASE lease. A cancellation intent, NET nonce reused as RELEASE, or later command does not constitute that proof. The original NET job can retain its own CANCELLING cleanup scope; no cancel orchestration is authored here.

The control/fence check must remain under the writer's real short ordered locking: module FOR SHARE → run/root → Candidate control → actual job/worker/fence/state → sorted money/dependency rows. Use existing bounded/NOWAIT admission for contention; a helper inserted after an unbounded earlier lock cannot establish prompt deferral by itself. The caller retains its completed replay and exact cursor/lease checks before mutation admission where appropriate. No duplicate Candidate clock or process-local exemption is added.

## Later writer insertion map, no early body rewiring

| Owning card | Actual family consuming D18 at its scheduled successor |
| --- | --- |
| DR-08 | 0510 preparation expiry, then preserve through its master chain |
| DR-11 | 0360 Case preparation/floor/week allocation |
| FC-10 / FC-12 / FC-13 | the named Case mutation/restructure owners |
| FC-11 | 1647 write-off |
| EX-14 | 0200 settled financial posting |
| EX-34 | 0490 internal zero-cash posting |
| EX-27 | 0430/0460 week and Case cancellation |
| Full DR-37 | 0390 NET owner and 0300/0310 result/hold/capacity guards; D12/D13/D15/D16/D17/D19 and exact certified cancellation admission |

Each actual next successor inserts one assertion after its native replay/order/lease/cursor admission and before changing staged dependency, floor, capacity, hold, week or result facts. Do not copy these bodies into early DR-37 or infer money-owner closure from an empty fence table. Actual contention, bounded pages, cancellation, interrupted resume, held money and late refusal behavior remain full-card future acceptance.

## Read receipts

core1324 SHA256 f38878c7b09e3fbac2d21acbf96c4e88bdac1d4c32abac7ee98d2672df7ba52c; case0300 04b44425d288e714037012c3b4eb4d27679137905e1c380682812cc8273ad40e; case0380 9b750149eb86e7ddd6cff218783cbff2674c64abf3a117f20e66579d5f3d291e. Current NET parent0559 physical SHA256 514fb6a069dfe860d06c76d98302554bf3ed93e8e027cdbac460bf8cea7da9a6; current PS14 STATUS0639 15c88f4c45051087976ed9187a741da494c69a052611c2a41892417f9665a22d.

This is a revised design only. It makes no migration installation, early writer-hook completion, replacement/cancellation implementation or application-acceptance claim.

11October2026 implementation boundary: early D14/D18 only built under BD096/DR11; current Caseprepare0904 and owedDR08expiry0904→0910 assertion installed solely in localPG17. FullNET/status/replacement/acquisition/cancel/handoff/other-writer delivery stays with later owning cards. Independent design/source reviews preserved.
