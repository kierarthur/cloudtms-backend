# DR-37 early D14/D18 independent design review

11 October 2026. Source-only review of the revised early prerequisite. No product, checklist, SQL, database, test, fixture, browser, hosted or Git action was performed. This review does not close the full DR-37 card.

## Verdict and authority

**PASS for the narrow D14/D18 architecture, subject to the exact implementation conditions below.** The native immutable NET request, Candidate member, worker, job and NET allocation-state identities are sufficient. No early D12 replacement table, D17 worker columns, acquisition protocol, new command, cancellation delegate or money-owner rewrite is required.

Authority read: full approved `03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:716–738`; all DR-37 checklist rows at `docs/bpay-next/IMPLEMENTATION_CHECKLIST.md:3723–3754`; start sheet `00_READ_ME_FIRST_SUMMARY.md`; master ordering/chain table `08_BUILD_ORDER_AND_CODEX_INSTRUCTION.md:13–20,43,59,61,214`; `addendum/R2_EXACT_CONNECTIONS.md:27–46`; `check/R3_FENCE_DEPENDENCY_EXCEPTION.json`; explicit owner correction 7 recorded in `docs/bpay-next/BUILD_LOG.md:398,402`; revised `docs/bpay-next/design/DR-37-early-fence.md:5–15`. The plan files are under the ordinary backend `codex_outputs/claude-stage2-continuation-20260925/BPAY_NEXT_BUILD_PLAN_20261009_codex_r3` directory. The exception permits a separately scheduled prerequisite, not a claim that full sequence130 is built. D12/D13/D15/D16/D17/D19 and F-26 remain later owners.

## Minimal table and assertion

The proposed non-null Candidate/week, immutable request command, worker, original job, module epoch and target allocation-state identity can use existing keys. Core1324 provides same-Candidate worker and request/member keys (`supabase/migrations/03102026_1324_banking_pay_next_core.sql:470–500,795–840,967–991`); Case0300 provides the composite job-owner unique key and allocation-state identities (`supabase/migrations/04102026_0300_banking_pay_next_case_instruction_core.sql:303–350`). Its existing Candidate/week key is at598–608. Case0380 binds the immutable request to its original DRAFT state and widens the actual NET state (`supabase/migrations/04102026_0380_banking_pay_next_case_net_binding.sql:8–42`).

Keep unique `(candidate_id,pay_week_start)` plus partial unique active Candidate ownership over exactly STAGING/COMMITTING/CANCEL_REQUESTED/CANCELLING. The assertion must seek the active Candidate key without a caller-week filter: Case/component capacity is shared across weeks. Terminal COMPLETE/REFUSED/CANCELLED rows are inactive; lease expiry never retires ownership. Keep the table and assertion owner-only, private, with the repository's owner RLS/FORCE RLS/ACL discipline. Do not grant direct browser/service mutation or introduce an early row acquisition/reset route. Future exact acquisition, transitions, cancellation certificates and terminal reuse remain the genuine later owners.

The private SECURITY INVOKER assertion returns void, uses the closed `(uuid,uuid,uuid,bigint)` signature, and leaves the caller's original authority/business checks intact. Under native Candidate serialization it returns when no active fence exists. With an active fence it first verifies structural cross-links, then admits only the real stored NET job under its fresh native FINANCE_ALLOCATE lease. Everyone else, including another week, absent/partial job proof, an expired lease and an unproved cancellation delegate, gets retryable `55P03`; no REVIEW, page spend, hold change, new money fact or auto-clear occurs.

Use NULL-safe comparisons/missing-row rejection for all required identity operands. FK references to request/state PKs alone do not establish that those records share the stored worker, Candidate, original job, command, module and week. Reject corruption as integrity failure. A structurally valid old-module fence remains a busy exclusion; it cannot gain an exemption or be deleted through lease expiry. A terminal fence does not need a current lease and should not be revalidated as active.

## Exact native NET ownership proof

There are **two different clocks**; the implementation must keep them separate:

- `fence.module_epoch = job.module_epoch = command.module_epoch = target_state.module_epoch = module_control.owner_epoch`, with module owner NEXT.
- `p_owner_epoch = job.owner_epoch = worker_control.active_owner_epoch`, with the actual matching non-null lease nonce, LEASED status, FINANCE_ALLOCATE phase and unexpired `lease_until_utc`.

The native claimant increments the Candidate lease clock, not the module clock (`supabase/repeatable/04102026_0110_banking_pay_next_simple_paye_projection_v1.sql:25–62`). Do not equate job owner_epoch to job/module epoch. The design note's “actual job epoch” wording at48 needs this explicit distinction.

Require the exact immutable command's PAYE_NET_ENTRY kind, agency_sequence equal to job.command_sequence, same-Candidate membership and request.command_id equal to the stored command. Match request.run_worker_id/candidate_id, target state's id/run_worker_id/run_id/candidate_id/command_id/job_id/module_epoch, NET pass and `projection_no = request.request_no`. Match its selection/preparation identities to the original request/DRAFT binding and exact stored week; the economic preparation identity is `worker.captured_preparation_revision`, not the mutable execution revision. Current definitions prove these operands at `supabase/repeatable/11102026_0559_banking_pay_next_worker_reprepare_v1.sql:2915–2981,3016–3050,3071–3080`.

Do not substitute the latest allocation state or accepted projection. Current NET continuation is job FINANCE_ALLOCATE while its exact target is `status='BUILDING'` and `net_stage` WEEK_CAPTURE or ALLOCATE; current terminal pairs are COMPLETE/COMPLETE with job DONE/PROJECTED, or OUTDATED/REVIEW with job DONE/DONE (`0559:3095–3100,3317–3335`). The future fence's cancellation stage is a separate owner fact; this early slice does not add new NET-state stages or delegate authority.

**Do not demand `target_state.owner_epoch = p_owner_epoch` before takeover.** The current NET page updates that carried state epoch only after admission (`0559:3098–3100`). Fresh authoritative same-job lease takeover must pass unchanged immutable identity even if that carried epoch is older. Similarly, state financial_view_revision is captured evidence, not a lease operand to compare blindly with the current Candidate revision. No state money or captured financial operands become assertion inputs.

## Precise two writer insertion points

| Current registered writer | Safe insertion | Reason |
| --- | --- | --- |
| `private.bpay_next_expire_preparation_worker_page_v1(uuid,uuid,bigint,bigint,integer)` in `supabase/repeatable/11102026_0746_banking_pay_next_expiry_projection_v1.sql` | Immediately after checkpoint replay/ahead admission at48–52, before53–55 and the READY→RELEASING block at56 | Leaves terminal39–42 and older-checkpoint readback untouched. Blocks worker CANCELLING mutation70, Work/Case release116/142, ORIGIN requeue91, financial revision/week changes and projection continuation under an active foreign fence. |
| `private.bpay_next_prepare_case_page_v1(uuid,uuid,bigint,bigint,integer)` in `supabase/repeatable/11102026_0559_banking_pay_next_worker_reprepare_v1.sql` | Immediately after exact cursor replay/ahead checks at1944–1953, before the no-Case shortcut1954–1961 | Leaves completed1915–1924 and lost-page1945–1949 receipts readable. Blocks the no-Case helper's week publication as well as floor/week/state/period/result/hold/allocation writes in the ordinary Case path. |

Both calls pass their exact native Candidate, job, lease nonce and owner epoch. Both current bodies already hold module→run→Candidate→job/binding/worker/state before the point. Preserve that ordering and all existing fences, counters, row/byte limits, Rule B and the 0290 kernel. Do not insert inside a per-row loop or only in an allocation substage: first-page binding and resumed weekly/capture stages also alter protected facts.

Prompt contention remains a real implementation condition. Current writer parent/Candidate locks are plain blocking locks (`0746:26–29`; `0559:1905–1911`), so a helper placed later cannot retrospectively guarantee prompt rejection while an earlier lock is occupied. Use the existing owner's bounded/NOWAIT admission where needed; do not claim the early empty-table check proves full contention acceptance. Any foreign active-fence failure must roll back the whole caller transaction, not be translated to a business refusal. No need to duplicate unrelated writer bodies now.

The current registered Case/NET parent is0559, not raw0390; the early note's line72 “Current NET0390” hash is historical source evidence. The registry records current expiry0746 and Case/NET0559. Later whole DR-37 consumes those exact current bodies and the PS14 STATUS0639 successor. The two assertions do not yet protect every direct privileged writer or establish the later cancellation journey; full D19 and named writer owners still have their own obligations.

## Read receipts

Fresh SHA256: early design `dd4affa5be7ad532ab0a03bd90dd126f2d818ab9722b8b37852720ee39161162`; expiry0746 `f1e6505c4f0ce89be8da8090dbc30467ef0ec50518536eb797f11dab37d152e6`; current0559 `514fb6a069dfe860d06c76d98302554bf3ed93e8e027cdbac460bf8cea7da9a6`; final-definer registry `7fe1613b93af79005608826ccf565401a37bd2cccb1b5e7f1c57b876399ff444`.

This is a design/source verdict only. Actual product source, installation/security checks and later app contention/replay/cancellation acceptance remain unreviewed or NOT_RUN here.
