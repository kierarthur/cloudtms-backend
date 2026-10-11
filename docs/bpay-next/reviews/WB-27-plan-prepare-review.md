# WB-27 shared Case plan / preparation source review

Date: 11 October 2026. Independent source review only. Root remains the sole product writer.

Verdict: **APPROVE the DERIVE + ALLOCATE extraction at the reviewed saved source. No essential financial defect found.** This approves this private plan/preparation seam, not the pending estimate reader, native-version scope coverage, database compilation or runtime acceptance.

## Reviewed authority and source

Authority: the full WB-27 card in the approved ordinary backend pack `codex_outputs/claude-stage2-continuation-20260925/BPAY_NEXT_BUILD_PLAN_20261009_codex_r3/02_WORKBENCH.md:1014`; every WB-27 row in `IMPLEMENTATION_CHECKLIST.md:2135`; R2 F01/F03 and WB-27 rulings; the master dependency chain; `docs/bpay-next/design/WB-27-build-scope.md`, including BD081; and `docs/bpay-next/design/WB-27-design-review-notes.md`.

Product reviewed:

- Current writable predecessor: `supabase/repeatable/11102026_0102_banking_pay_next_case_choice_boundary_v1.sql`.
- Shared private plan: `supabase/repeatable/11102026_0204_banking_pay_next_case_allocation_plan_v1.sql:5`.
- Writable successor: `supabase/repeatable/11102026_0205_banking_pay_next_prepare_shared_case_plan_v1.sql:4`.
- Native period schema: `supabase/migrations/04102026_0300_banking_pay_next_case_instruction_core.sql:135`, with the collection column added by `supabase/migrations/04102026_1637_bpay_next_work_collection_recovery.sql:69`.
- Original collection qualification: `supabase/repeatable/10102026_1236_banking_pay_next_segment_authority_final_v1.sql:4461`; its STABLE segment basis dependencies at lines 4076/4102.
- Original allocation kernel: `supabase/repeatable/04102026_0290_banking_pay_next_case_allocation_row_v1.sql:9`.
- Original valuer: `supabase/repeatable/06102026_1157_banking_pay_shared_selected_quote_v1.sql:932`.

The comparison used saved source and a read-only diff against 0102. Historical 0549 was not substituted for the current predecessor.

## Concrete findings

1. **Rule B stays an immutable control cut; money stays live.** DERIVE lines 56–87 retain the accepted run's exact Case LEAF, typed identity, captured status/due and immutable rule identity/revision. Missing or null required captured revisions fail closed. The accepted path does not replace an old selected rule with the current Case pointer. The writer locks the native Case/component first, locks the existing period before DERIVE, and supplies the original accepted run, captured Candidate floor/channel and weekly binding state (0205 lines 257–269). Nominal and capacity calculations still use native current balances under those locks.

2. **Prospective period JSON matches the real schema and operation.** DERIVE lines 224–238 construct every field of the current period row, including zero defaults and nullable collection identity. The current period has no extra creation/update timestamp to defeat the writer's complete JSON comparison. Opening due is calculated before status/due suppresses this selection's nominal, preserving the original legitimate weekly opening (212–217). The plan writes nothing. The owner applies INSERT or FUNDING_ZERO, reads the actual row and refuses any mismatch (0205 lines 276–298).

3. **Funding remains the narrow original successor.** DERIVE lines 242–276 preserve identical schedule/policy/typed facts, an exact FUNDED event and original funded timestamp. An unfunded recovery period can advance only when all five opening/realised/active counters are zero. A consumed funded period or original payout period retains its recorded opening rule and counters. The writer additionally fences the zero-period update by the expected prior revision and the same five zero counters. It does not rewrite old frozen instructions/results.

4. **No balance is subtracted twice or repriced.** DERIVE lines 179–210 and 277–286 preserve the original ordinary recovery, automatic collection and payout operands. Automatic collection qualification remains the original exact paid Work/position/policy proof, moved as plain reads into the STABLE snapshot (99–144); a Source comparison is still refused before qualification (92–96). The original 1157 valuer and missing/ambiguous policy handling remain (287–300). Current Work WAIT suppression applies only to current estimate derivation, with no accepted-Draft eligibility rewrite (301–305).

5. **Captured revisions belong to the actual writes.** The owner applies the component resolution, receives its incremented revision, then augments the returned instruction with the actual Case/component/period revisions and the original run/selection/preparation identities (0205 lines 299–321). Planned revisions are not substituted for post-write native revisions.

6. **One allocation owner preserves the original kernel and order.** ALLOCATE lines 358–395 bounds the instruction array, orders the original native tuple and uses unchanged 0290 for recovery. Original PAYOUT behaviour and signed H remain. The owner supplies exact frozen instruction rows and persisted server-owned E/H, then keeps the original result/hold/capacity use, Case/component/period counters, destination capture, cursor, stage, revision and finalisation writes (0205 lines 367–513). No new calculator, table, economic command, hold type or preview write was introduced.

## Nonblocking source packaging note

At this review, 0205 also repeats the unchanged no-Case completion/publisher definitions at lines 515/612 and retains the LEAF owner/revoke statements at 724/725 without a LEAF definition. The diff shows no change to those function bodies; their existing owner/ACLs are preserved. This is incidental extraction scope, not a financial defect. Root was notified so the successor can contain only its intended preparation definition and matching ownership/revokes.

## Evidence boundary

Packaging closure, 11 October 2026: re-read the saved 0204/0205 after root's correction. 0205 now contains only the current preparation definition (line 4), its matching owner/revoke (514–515) and transaction wrapper. The unchanged no-Case/publisher definitions and stray LEAF ownership statements are removed. 0204 remains the single STABLE private plan; its 0290 recovery call is at line 384 and matching owner/revoke at 399–400. The original locks and the reviewed plan/write integration remain. **The nonblocking packaging note is closed; source approval stands.** No product edit or execution was performed by this reviewer.

No SQL execution, database connection, tests, syntax run, browser operation, fixture, hosted change, commit or product edit was performed by this reviewer. The new files are UNLOADED source. Database compilation and release-source catalogue checks remain the builder's responsibility. The estimate reader still requires its own review of complete current-input membership, Source/book qualification, native-version continuation, honest fixed budgets and final fresh recheck. Joined Draft/estimate equality and DRP P05 remain later runtime evidence; this review does not claim them.
