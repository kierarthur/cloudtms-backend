# WB-08 draft schema and four-function summary review

10 October 2026. Independent read-only review of the UNINSTALLED1252 migration and the current 145-line/four-function repeatable prefix against the approved design, card02:320–386, D01–D13, R2 §1/§5/W12–W14 and actual core/Case/WB-36 parent keys. No product edit, SQL, tests/gates, business/runtime or hosted action. Refresh work/Case, metadata/name owners, final apply seams and final owner/ACL/NOTIFY/COMMIT tail are still being authored; their absence is explicitly outside this prefix verdict.

| Reviewed source | Raw SHA-256 |
|---|---|
| `10102026_1252_banking_pay_next_workbench_summary.sql` migration | `65debd2c8e6f6cb83763b86a7c24c371ac33ea830e2ac4164de03fd4eb31ddb2` |
| `10102026_1252_banking_pay_next_workbench_summary_v1.sql` prefix | `b5682902258dce8cabfed1e8c9d0280d7544acc840d0180f0e965b368d108e7d` |
| Approved design | `81fb741983bb6addb970b97b3a077cd9758789aa008c8e809c49d9e8d3c75182` |

**Finite schema/aggregation corrections required before sealing this subset.** Native references resolve to real parents. Candidate/agency count transition mechanics and the security plan are sound, but correct independent UUID FKs are not yet full common-scope/conservation proof.

## WS08-01 — bind the item to its actual group owner

Migration128–146 binds `(group_key,candidate_id)` and separately binds `(work_id,candidate_id)` or `(case_id,candidate_id)`. It does not require the item's work/Case to be the work/Case named by that group. A WORK:A group and WORK:B item for the same Candidate pass those independent FKs; GROUP_EVIDENCE may likewise name another same-Candidate owner. Add the finite `group_key='WORK:'||work_id` / `'CASE:'||case_id` ownership CHECK or an exact mandatory common-scope qualifier. Bind CASE component_key/rule to its actual component where those asserted fields are consumed; independently valid same-Case rule/component UUIDs do not establish their exact tuple. This is narrow native-parent qualification, not another financial authority.

## WS08-02 — enforce the claimed one-group natural-scope bound

`group_contribution` repeatable33–81 expands the **item** Client and target channel and groups those values. The schema does not bind those fields to a single group scope. With items in one group using two actual Clients and both PAYE/UMBRELLA, the natural expansion is three Client scopes × three channel scopes × five sections =45 rows, exceeding the advertised20 without any array/rowset rejection in this function. The later delta's cardinality check does not make the producer's at-most20 claim true.

Qualify a group's real single Client/target scope and compute expansion from that authoritative group scope, or enforce matching item/group scope with a NULL-safe native guard. Keep genuinely Client-less Case rows only in Client-ALL; do not assign a fabricated Client or silently truncate scope rows. Actual Candidate/group native ownership must also be proved under WS08-01. A bounded output/byte assertion can fail closed rather than advertise a bound that depends on unconstrained fields. No new fanout or parser is needed.

## WS08-03 — close known-money NULL shapes

Migration149–153 requires nine core amounts when amount_known=true, but omits fields used by the prefix's sums:

- `selected_amount_known=true` only implies is_selected; it does not require selected_source_ex_vat. `group_contribution`75 turns its NULL into selected_amount0 while unknown_selected_amount_count remains0.
- A known WAITING NET_ADD row may have nominal_source_ex_vat NULL; line66 returns waiting_net_add_amount0 with no unknown_amount_count.
- A known DEDUCTION row may have deduction_balance_source_ex_vat NULL; lines67–68 return gross/net deduction balance0 with no unknown_amount_count.

Require each used dimension's exact non-NULL known shape (and its native direction/stage semantics) or count that dimension as unknown. An unknown row must not contribute unqualified money to known totals merely because a nullable amount was populated: either enforce the finite unknown tuple as NULL or condition sums on genuine known dimensions. Future readers must still expose unknown-count totals as unavailable; that does not justify claiming a missing component as known zero internally.

## WS08-04 — qualify disjoint portions and monetary/selectability semantics

The current table allows a known AVAILABLE/ZERO/RESERVED/EVIDENCE row with unrelated A/C/H/residual/gross/included/excluded/recovery amounts; it has no portion shape or native monetary/zero check. `selectable` only implies READY/known/AVAILABLE-or-ZERO, so a known nonzero fake ZERO or wholly covered paid remainder can be marked selectable. CASE direction/payroll stage/component key also are not bound to their native component facts. The delta algebra faithfully subtracts/adds whatever it receives, which is not proof of genuine contribution conservation.

Add the finite required schema checks/mandatory qualifier for known amounts and kind/portion shape, with actual parent/sibling conservation at the refresh boundary. Preserve signed native recovery; do not impose a blanket nonnegative approved amount. AVAILABLE uses actual positive free remainder, RESERVED carries actual H once, base A/C once across portions, actual recovery is informational when a real Case owns the deduction, and ZERO must be genuine certified current zero with no realised/reserved gross. Fully paid/held zero cannot supply Ready context/history filler. Unknown/EVIDENCE cannot manufacture a known zero. Since base A/C and H can live on different disjoint rows, a naive per-row `residual=A-C-H` CHECK is insufficient; the exact native member's complete portion set must reconcile without duplicating A/C or reserved amounts. This is a required row/invariant support for the named projection, not a demand to finish all pending refresh/producer code in this review.

## Sound prefix mechanics and remaining tail boundary

The migration's explicit Client scope (`real UUID text` or ALL) is non-null and constrained; natural summary PKs keep Client/channel/section ALL distinct without fake Client UUIDs. FORCE RLS, static mapped logical owners, `CURRENT_USER` owner policies and revoked PUBLIC/anon/authenticated/service_role table/type privileges are correct. The private SECURITY DEFINER functions fix search_path to pg_catalog/private and require NEXT. Final function owner/revoke and atomic repeatable tail remain required when the file is complete; no prefix install is claimed.

The aggregate distinguishes group item_count from raw portion contribution_count and DISTINCT native member count, excluding GROUP_EVIDENCE from financial child counts. ALL-section DISTINCT member counts do not add AVAILABLE/RESERVED as two segments. Ready/waiting GROSS_ADD and NET_ADD and gross/net deduction balances remain separate. Unknown and selected-unknown counts are carried explicitly; the missing-dimension defects above are finite closures of that intended design.

`apply_delta` checks both input arrays≤20 and combined encoded native payload≤32KiB, expands before negative/after positive typed rows, aggregates each natural key once and visits a deterministic Candidate/client/channel/section order. It reads old Candidate item_count, applies the group delta, and changes agency candidate_count only on zero↔positive Candidate membership; multiple groups/components do not each add another Candidate. Replayed identical before/after is zero delta. Nonnegative count CHECKs and zero-Candidate/zero-group conservation refusals fail rather than clamp or leave stale money; empty conserved rows are deleted.

The existing singleton lock serializes these summaries. Future refresh owners must acquire Candidate/business/metadata locks before that singleton consistently, supply genuine complete before/after group aggregates, and allocate a positive projection revision before summary insertion (the migration begins revision0 while summary projection_revision>0). These are explicit integration checks for the still-unfinished tail, not newly reported prefix bugs. Adoption begins UNSEEDED and no code here enables CURRENT; only real WB-44 seed/fanout can do so. No installed/business/app/queue/timing behavior is inferred from this review.

## Complete saved-source follow-up — WS08-01–04 closed

10 October 2026. Re-read the complete 1252 repeatable (743 lines at this checkpoint), immutable 1252 migration, additive 1316 migration, full 1309 Source seed successor, current registry entries and the source-gate diff. This remains a source-only verdict: no SQL, tests/gates, business calls, runtime, fixtures or hosted reads were executed by this reviewer.

| Reviewed source | Raw SHA-256 |
|---|---|
| Immutable 1252 migration | `aa9a42864893856c9ae02479edb1172cdf39c764a6ca2e9e77f2fd11253affab` |
| `10102026_1316_banking_pay_next_workbench_native_conservation.sql` | `367f4e133ff30453e6eeb9ce86cf96ff503f45ebb4da94b3ad96a91bf4098339` |
| Complete 1252 repeatable | `2fe6c4c816613aaa554d012289124c47219fd228a2af229c2ceabdb8f0cc305b` |
| `10102026_1309_banking_pay_next_workbench_source_seed_refresh_v1.sql` | `e1330272dfaa2978e537c49e074669bb9273410620f016bdcd1d4f05bf567241` |
| `scripts/bpay-next-source-gates.mjs` | `33954f0dc341c9bd7b894ec15278ce6923a61d69e8e517b45a6d2f5fb234fe3f` |

**PASS for this completed schema/summary/refresh/Source-seed source subset. No remaining concrete compile or source-safety defect found in the reviewed bytes. This does not close WB-44 producer/seed/fanout, future classifier branches, readers or application acceptance.**

WS08-01 is closed by the NULL-safe member-kind owner shapes plus the exact WORK:/CASE: group-key CHECK. The Case refresh obtains component key, direction/stage, amounts and its exact rule from the actual Case component and scoped rule, rather than accepting independent caller-supplied tuples. WS08-02 is closed by deriving Candidate/Client/channel from the authoritative group. One group can expand to at most two Client scopes, two channel scopes and five sections; Client-less Cases have only Client-ALL. WS08-03 is closed by the actual used-dimension non-NULL checks and the unknown-money NULL tuple. A selected amount is marked known only when the retained run-line/segment-parent/Case-instruction amount exists.

WS08-04 is closed by finite AVAILABLE/RESERVED/RECOVERY/ZERO/EVIDENCE shapes and mandatory refresh-boundary conservation. Work portions reconcile summed A/C/H, signed residual and positive-included/excluded minus recovery/held-overhang against the exact native member before applying any summary delta. Case portions reconcile A, realised, H, write-off, residual and the one outstanding deduction balance. Base A/C and debt balance are retained once; additional RESERVED portions carry H without duplicating the base or accepted amount. Fully settled members do not supply Ready history fillers. Genuine certified current zero members remain ZERO; damaged parent or segment attribution yields NULL-money EVIDENCE.

Two fresh implementation blockers were reported during this reread and then closed before this checkpoint:

- **WS08-05, missing write-off column:** the refresh bodies assigned/read `v_item.written_off_source_ex_vat` without a matching item field. The additive 1316 migration now adds the exact penny-domain column and requires nonnegative/non-NULL values on known rows, zero on ZERO, and NULL on unknown rows. Work writes actual zero; Case writes the actual native write-off. The immutable 1252 migration was preserved.
- **WS08-06, empty certified approval:** the initial zero-line branch changed an EVIDENCE row to amount_known=true, contradicting the EVIDENCE CHECK. It now retains a nonselectable, NULL-money GROUP_EVIDENCE row and records the genuine current certificate on the group. The aggregate excludes only complete certified GROUP_EVIDENCE from its unknown count. It adds no fake financial component or selectable wage; genuine empty zero and missing approval/evidence remain distinct.

Accepted WORK membership is bounded to actual SEALED run/choice/selection-Candidate plus the unique run_command PREPARE receipt. Native segments require their exact INCLUDED selection; indivisible ALL/SUBSET membership requires the accepted expected-revision inventory and no accepted-cut snooze. Worker/run terminal statuses release that membership while genuine ACTIVE holds remain separate. Case membership uses the exact sealed effective selection revision and retained instruction where available. Neither command completion nor a later module epoch silently releases the accepted offer.

The complete repeatable fixes each function's owner/search_path and revokes PUBLIC/anon/authenticated/service_role execution, attaches only the named metadata trigger, notifies schema reload and commits atomically. Metadata/name/bank changes advance only metadata and the dirty barrier; the NEXT-only path does not claim that the pending WB-44 continuation has run. Work/Case refreshes take Candidate control, native owner and metadata before the singleton revision; bounded refresh_many preholds the complete finite Candidate/owner set, including exact linked collection Cases, before refreshing any group. Known data never comes from a Candidate history or NET/headroom allocation scan.

Full CREATE text comparison confirms the 1309 Source seed definition equals the original installed 1246 definition after removing exactly one added `perform private.bpay_next_workbench_refresh_work_v1(p_work_id)` following the successful position update. The original NEXT shared lock, source proof, seed authority, conflict checks and pure READY replay return are unchanged. Its exact final registry entry selects 1309; the eleven projection roots select 1252. The source gate adds those exact current helper roots and their finite known callees plus the Source-seed refresh edge. It leaves the four historical point-read clauses, unknown-delegation rejection, dynamic-SQL denial and other existing source checks unchanged; there is no historical-file waiver.

Application/read availability remains explicitly unproved. Adoption is still UNSEEDED until WB-44's real complete census and concurrent-writer closure; later WB-02/WB-07/WB-40/WB-41/WB-29 classifier predicates and WB-37 promotion remain their named successors. This source subset does not provide a new Source producer, F25 resolver/version protocol, financial counter writer, browser permission, runtime route or Office implementation.
