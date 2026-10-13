# WB-41 independent SQL source review

12 October 2026. **SOURCE PASS** for the three current SQL successors and the finite source-gate delta. No essential defect remains in this reviewed slice. This is not whole-item, runtime, performance, screen or joined acceptance. The reviewer ran no SQL, database/business call, test, fixture or hosted action. Installation and compilation were not independently observed.

## Reviewed authority and bytes

Read the full WB-41 card (`02_WORKBENCH.md:1465–1492`), all eleven current checklist rows, R2:304–306, current investigation, BD131 build note and independent design review. D09 is the implementation row; the other retained context owners remain part of the behavior contract.

| Current source | SHA-256 |
|---|---|
| `supabase/repeatable/12102026_0207_banking_pay_next_origin_wait_recovery_v1.sql` | `15518A516D7E4E1264E44B949EE432B694418DAAFD9DF9FDAF6FED22ABCA9F27` |
| `scripts/bpay-next-source-gates.mjs` | `45339DB93CBB17091D4A21DA21E24EC7BFDDE23E72CC42B82E8C347E8534E5FF` |
| `design/WB-41-build.md` | `DA412E827AC4EADDA7FC782179B27086E1C8B5C2FFAFB70ABAD66FBF59C77B7E` |

The final-definer registry names this 0207 file for exactly `private.bpay_next_case_allocation_plan_v1(jsonb)`, `private.bpay_next_workbench_refresh_case_v1(uuid)` and `public.bpay_next_workbench_page_v1(jsonb)`. Static reconstruction used read-only file reads and the assembler's uniquely anchored text substitutions, stopping before all writes. All three actual complete bodies equal the expected complete current parent bodies with only the inspected WB-41 substitutions, after normalizing CRLF to LF: plan 33,729 bytes, classifier 16,118 bytes, reader 90,577 bytes. Parent file hashes:

- 1939 Case edit: `7B723A61096AC50853DBC609ACBBA6437B403F11AA796A6F77E39A78B8308F66`.
- 0559 worker reprepare: `514FB6A069DFE860D06C76D98302554BF3ED93E8E027CDBAC460BF8CEA7DA9A6`.
- 0041 snooze management: `58D05480DA8E138D89233044E985B34782035E86D0CC16E271D237AFD0A8A599`.

## Shared plan and accepted-cut behavior

0207:327–331 preserves the original condition `v_run_id is null and v_automatic` before reading the existing native WAIT helper. Only the current unaccepted offer receives zero nominal/capacity and the typed explanation. Accepted Create therefore keeps its original Rule B input; no later live withdrawal reprices that captured membership. The existing component/Case balances, period arithmetic, original/current-rule provenance, valuation, funding, age and floor operands are preserved by the complete-body comparison.

0207:399–416 retains exact age/Case/component ordering and the original `bpay_next_case_allocation_row_v1` invocation. The new block at418–431 only annotates its zero result: it requires a native collection, the exact automatic OVERPAYMENT/RECOVERY/DEDUCTION/GROSS_DEDUCT/GROSS_RECOVERY tuple, all four nominal/capacity values zero, taken zero and unchanged E/H. It does not calculate another amount or reserve money. The original writable prepare consumer and its locks remain unchanged.

The reused native predicate is explicit in `10102026_1332_banking_pay_next_withdrawal_projection_v1.sql:11–13`:

> `return v_state='WITHDRAWN' and coalesce((select sum(p.realised_source_ex_vat)`

Its continuation is `from private.bpay_next_position p where p.work_id=p_work_id),0)>0;`. This is the exact Work's signed realised pay, not Query presence, another Work's debt or positive-only history.

## Classifier and disjoint portions

0207:565 stores `v_base` before the new branch. Lines568–580 change only positive AVAILABLE, unselected, lower-priority eligible automatic Overpayment recovery. Native origin joins require the same Case, component and Candidate, paid qualified READY collection and the exact native Work WAIT predicate. Higher-priority review/data and accepted selection branches keep precedence.

Lines582–591 still construct RESERVED from the original `v_base`; outstanding and hold conservation are untouched. A £60 paused unreserved recovery remains £60 outstanding with an ACTION reason, not an inferred £0 balance or a selectable Case. The original bounded work-origin refresh owner remains responsible for linked Case refresh.

## Informational estimate and first-use closure

0207:887–905 collects WAIT components separately from original selection leaves. Membership is Candidate/scope/channel qualified, AVAILABLE, known, unselected, unaccepted and from complete current groups/items. It is inspected with LIMIT1001 and combined with existing selected components under the original finite 1000-membership budget before use. It does not add a LEAF, selected choice or Create member.

All informational components enter the existing native version set before continuation validation: Case, component, rule, period, collection, original position and Work (981–994), together with the retained source/approval/control qualification and original financial/context bounds. No new continuation protocol or money cursor is introduced.

The independent design finding WB41-DR01 is closed by1269–1287: before ALLOCATE, every informational component must still have a freshly positive exact native WAIT origin and return the exact automatic recovery tuple, typed WAIT marker, collection identity and four zero monetary operands. Approved-but-unapplied reauthorisation can leave a stale maintained ACTION row; that row now returns the existing PENDING via the named check violation, rather than entering a positive informational allocation. Existing selected membership remains unchanged.

Original deterministic replay/paging and verify-only checks are preserved (1295–1326); request/scope and response bounds remain at1329–1339. Known incomplete qualification returns PENDING with null aggregate money (1342–1354), not a fabricated zero estimate.

## Legacy allocation condition read

The card's legacy-read requirement was followed. In the retained current legacy baseline `03102026_0700_stage2_finance_baseline_after_h1h2_retry.sql:337–338`, the native Overpayment nominal rule is:

> `then greatest(coalesce(vfcr.outstanding_amount,0) - coalesce(vfcr.active_reserved_amount,0), 0)`

The same owner admits positive Overpayment rows at430–449, keeps native recovery order at443–446 and calls the original protected allocator at505–510. Its inherited `finance_case_baseline_scope` in `26092026_0203_banking_pay_stage2_workbench_draft_v1.sql:40375–40398` requires ACTIVE, positive outstanding and applicable due week. The latest native register definition `19072026_0120_fix_finance_case_resolution_register.sql` carries linked Timesheet identity but contains no Timesheet authorisation/withdrawal WAIT predicate; it separately labels ordinary Draft authorisation at377–399.

These inspected legacy allocation/register owners do **not** establish an existing origin-unauthorised pause. Do not describe the new branch as literal legacy WAIT parity. Its authority is the explicit OR31/R2 behavior and the already-retained exact native 1332 WAIT predicate. No legacy allocator, query producer or Source qualification owner is changed.

## Scope, security and limits

The source-gate diff adds only `private.bpay_next_work_wait_state_v1` to the two exact existing Case-refresh callee maps (ordinary and DR05). It adds no general SQL/dynamic/history exception. Plan remains STABLE invoker; the existing classifier/reader definer modes, search paths and reader timeout remain. 0207:1468–1474 revokes all three functions from PUBLIC/browser/service roles, granting only the public reader back to service_role. No private browser exposure, table, trigger, command, money owner or early producer is added.

Source paid-withdrawal/comparison refusal paths remain inherited. Their joined journey remains BLOCKED_ON_CM02/SP04 and NOT_RUN. Screens, real withdrawal/reauthorisation, estimate/preparation equality, timing, compilation and all runtime acceptance are NOT_RUN by this reviewer.

