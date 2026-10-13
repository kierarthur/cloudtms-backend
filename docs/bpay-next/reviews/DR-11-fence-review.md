# DR-11 / early DR-37 D14-D18 fence source review

11 October 2026. Independent source-only review. **PASS on the final bytes recorded below; no essential source defect remains in this narrow early slice.** No product edits, SQL execution, database calls, tests, fixtures, browser, hosted or Git actions were performed. Installation, source gates and app/runtime acceptance are separate. Full DR-37 remains deferred.

## Reviewed scope and current parents

Read the approved DR-37 card716–738, all DR-37 checklist rows3723–3754, R2 exact-connections §2, dependency exception, start sheet/master chains, owner correction7 and the revised early note. The independent architecture review is `docs/bpay-next/design/DR-37-early-fence-design-review.md`.

Reviewed product files:

- `supabase/migrations/11102026_0904_banking_pay_next_early_candidate_financial_fence.sql` — new owner-only private D14 table/index/policy.
- `supabase/repeatable/11102026_0904_banking_pay_next_early_candidate_financial_fence_v1.sql` — new private D18 assertion and exactly two writer successors.

The copied Case owner is the current `11102026_0559_banking_pay_next_worker_reprepare_v1.sql` definition of `private.bpay_next_prepare_case_page_v1(uuid,uuid,bigint,bigint,integer)`. Expiry uses the current `11102026_0746_banking_pay_next_expiry_projection_v1.sql` definition of `private.bpay_next_expire_preparation_worker_page_v1(uuid,uuid,bigint,bigint,integer)`. Registry advancement was still pending at this read; this is not a registry/installation receipt.

## Closed findings

The first assertion draft omitted the NET/request economic revisions and original DRAFT-state/week comparison. Fresh source now loads the immutable request's exact `case_draft_state_id` and rejects missing/mismatched DRAFT, target NET, worker, run, Candidate, captured preparation revision, selection revision and week with NULL-safe comparisons (`0904 repeatable:35–60`). It also compares exact request_no to target projection_no. The existing0380 FK/shape remains the original request/DRAFT authority.

The first Case copy made only Candidate admission NOWAIT. Fresh source now uses NOWAIT for every native module/run/Candidate/job/worker/state lock before the assertion (`0904:122–131`), as does expiry's module/run/Candidate/job/binding/worker admission (`611–618`). This closes the identified pre-hook blocking gap without changing the original order. Dependency/money row locks after successful admission retain their parents.

## Final source findings

The migration has a non-null finite Monday week, exact immutable NET command, same-Candidate worker/member/composite job-owner FKs and native Candidate/week and allocation-state references. Its `(candidate_id,pay_week_start)` primary key and partial unique Candidate index over exactly STAGING/COMMITTING/CANCEL_REQUESTED/CANCELLING enforce Candidate-wide active exclusion. COMPLETE/REFUSED/CANCELLED are inactive. The current writer's week is never used to filter the active-fence lookup (`0904:26–28`).

The assertion separates module identity from lease ownership correctly: fence/job/command/state module identities match one another; the exemption requires the fence module to equal current NEXT. Independently, caller owner epoch must match job.owner_epoch and worker_control.active_owner_epoch, with the exact non-null nonce, LEASED status, FINANCE_ALLOCATE phase and an unexpired native deadline (`63–68`). It does not equate the two clocks or compare the carried NET-state owner epoch with a fresh lease; original NET0559 updates that carried epoch only after admission at3098–3100. Same-job fresh native takeover can therefore pass, while old-module ownership cannot pass or be cleared implicitly.

Identity verification uses the exact stored request, original job, command, worker and target state, not latest/current pointers (`30–60`). It verifies PAYE_NET_ENTRY kind, ordered command_sequence, membership, immutable economic identities, NET pass/projection and exact DRAFT/target week. Required mismatches/missing native rows fail closed as integrity errors. Absent fence returns without conferring business eligibility. Any valid active fence without the exact original NET lease gets `55P03`; assertion lock contention is normalized to its bounded busy reason (`69–71`). There is no cancellation delegate, lease-expiry deletion, acquisition/reset function or money calculation.

The two hook placements preserve original receipts: Case assertion174 follows completed and older-cursor replay/ahead checks and precedes the no-Case shortcut, floor/week/state/period/result/hold/allocation writes. Expiry assertion641 follows terminal and checkpoint replay/admission and precedes worker CANCELLING, ORIGIN requeue, release/projection/financial revision/week writes. A failure rolls back the whole caller transaction; no handler turns it into REVIEW or advances a page. Both calls use the actual native Candidate/job/nonce/owner operands.

Static complete-definition comparison confirmed **both copies equal their exact current parent definitions after removing only the one assertion line and new NOWAIT keywords**. All monetary formulas, the one0290 row kernel, accepted Rule B sources, business classifications, row limits, carried counters/cursors, no-Case semantics and expiry72h/confirmation fencing are preserved. This was text comparison, not test execution.

The table enables and forces RLS, grants its policy only to the installing current owner, and revokes PUBLIC/anon/authenticated/service_role table access. The new assertion is private SECURITY INVOKER with fixed `pg_catalog,private` search path, logical postgres ownership and revoked browser/service execution (`migration:24–30`; repeatable:6,74–75). Existing repository `scripts/cloudtms-db-release-lib.mjs:112–124` maps audited unquoted logical postgres ownership to CURRENT_USER; this migration/assertion needs no new quoted/dynamic owner exception. The existing two functions retain their ACLs through CREATE OR REPLACE. No public wrapper/command/tablewrite path is added.

No early D12/D17, NET staging/acquisition/cancellation protocol, full result/hold/capacity guard rewrite or F-26 takeover is present. The two selected writer hooks and empty-table foundation do not prove all later named writers or full DR-37 cancellation/contended app acceptance. Those remain with their own owners.

## Exact read receipt

- 0904 migration SHA256 `fbcd4a69aac8e14389c371313e25e2e8839e152603f7e30522a0a129740f145b`.
- 0904 repeatable SHA256 `fa3c73a83075f67246e62adea7ba1467c019bada601d3db880ec51b48cdd1a9e`.
- Current0559 parent SHA256 `514fb6a069dfe860d06c76d98302554bf3ed93e8e027cdbac460bf8cea7da9a6`.
- Current0746 parent SHA256 `f1e6505c4f0ce89be8da8090dbc30467ef0ec50518536eb797f11dab37d152e6`.

Source PASS covers these bytes only. SQL compilation/security/registry/contract verification and joined/runtime/app acceptance were not run by this reviewer.
