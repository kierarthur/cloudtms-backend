# DY-06 retained financial predicate — independent source review

11 October 2026. **PASS for the authored retention predicate and its private access boundary; no essential finding remains.** The1107 product is not installed at this review. This is static saved-source evidence only: no database invocation, compile, test, fixture, application journey, query-plan or runtime acceptance was performed by this reviewer. Public lock placement and complete parent preservation have their separate review; this receipt does not substitute for them.

The full DY06 card/D01–D05, BD104 scope and [twelve-root census](../design/DY-06-retention-census.md) govern. Reviewed `private.bpay_next_daily_pay_retention_v1(text,jsonb)` at1107:7–79 against the original retention producer `26052026_2100HRS_NEW_FUNCTIONS.sql:183474–183812`, original removal/invoice owner at153516–153768 and accepted Rule B membership at0426:515–587. No approval-only Archive marker is used as financial evidence.

| Native retention root | Result and exact retained meaning |
| --- | --- |
| timesheets_financials | PASS — historical physical-family rows; paid timestamp/user, nonblank payment reference, remittance timestamp or positive send count; no current-only filter. |
| pay_batch_items | PASS — any physical-family membership, including retained Draft/cancelled history. |
| pay_batches | PASS — native UUID-array force-include overlap; no batch-status restriction or whole-array unnest. |
| pay_batch_timesheet_snapshots | PASS — any physical-family snapshot. |
| timesheet_pay_state | PASS — all five native non-NULL retained facts and the exact six paid/processing status literals. Native non-NULL signature/snapshot semantics remain even for UNPAID; no invented nonempty-object rule. |
| timesheet_pay_state_history | PASS — any physical-family history row. |
| ts_pay_adjustments | PASS — any physical-family adjustment; no paid-only filter. |
| pay_finance_case_components | PASS — any linked physical identity; closed Cases remain covered. |
| pay_advances | PASS — any linked physical identity; no active/kind/status narrowing. |
| pay_payment_correction_items | PASS — any physical-family correction membership. |
| pay_manual_adjustment_carry_forwards | PASS — any physical-family carry-forward, including terminal history. |
| timesheet_payment_overrides | PASS — any physical-family override, including cleared/consumed facts. |

**Invoice/blocker coverage PASS.** Context supplies the same already-read native history for the same immutable booking family at1107:432–444. Direct fallback calls that original owner at40–41. Every native blocker except `TIMESHEET_STILL_AUTHORISED` remains financial protection, including archived targets and invoice locks. The original owner covers current financial lock timestamp/credit-unlock semantics, both supported invoice-breakdown shapes/lock keys, physical invoice lines, NULL-physical booking fallback and INVOICED Contract Weeks. Neither native invoice logic nor the global sticky Archive/removal owner is rewritten.

**Rule B capture PASS.** A Work-point `work_choice` existence read includes OPEN/SUBSET and PINNING capture without waiting for SEALED, READY or a Run line. The original choice has `(run_id,work_id)` FK to run_selection and `(work_id,expected_revision_id)` FK to its immutable revision (`04102026_1241_bpay_next_component_selection.sql:19–37`). Current0426 inserts those original objects only after its skip/refusal branch:578–582. Skip-only run_choice_member records are not counted. Any run_work or financial_effect also retains protection. Run lines reference run_work through the existing composite FK at core1324:586–587, so their capture is covered without a redundant line-history scan. No current-head substitution or parked accepted-input authority is introduced.

**Bounds and access PASS.** The exact booking-point family is read with LIMIT33 and refuses cardinality>32 rather than truncating. The direct Contract Week fallback likewise now refuses>32 before invoking history; context keeps its existing native history scope. All financial probes are existence reads by this family or one native Work, with native force-include GIN overlap. The supporting migration adds only the three census gaps: Work-choice `(work_id,run_id)`, advances linked physical identity and carry-forwards physical identity. The latter two exclude NULL only and retain all statuses. Root's fresh catalogue absence/type evidence is separate; this reviewer did not obtain a fresh database plan.

The helper is STABLE, SECURITY INVOKER, fixed-search-path and postgres-owned. EXECUTE is revoked from PUBLIC/anon/authenticated/service_role at1107:1242–1243; no public helper grant or new RPC exists. Its only new call sites are owner-run route context and existing native public route owners. The optional history argument is internally supplied from that same booking read, never a caller-supplied public parameter. Genuine DAILY/NEXT guards at442–444 and the direct callers keep LEGACY/non-Daily marker interpretation unchanged. The predicate performs no writes, money calculation, history bootstrap, marker clearing, new table/column/protocol or public trigger. Final source reread includes removal of both unnecessary UUID ORDER BY clauses before LIMIT33; membership is unordered and a33rd member always refuses, so no sorting or silently truncated facts are required.

## Reviewed raw source hashes

| File | SHA-256 |
| --- | --- |
| supabase/repeatable/11102026_1107_banking_pay_next_daily_route_retention_v1.sql | f7964500c4c691390eb75a1150a634a7e73bd2918747ef57cb7ce81de139411f |
| supabase/migrations/11102026_1107_banking_pay_next_daily_retention_point_indexes_v1.sql | 4cc1fcb40abe6545465625e1ff0f34559de76fba5b151a617c0de0557e6e5324 |
| supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql | 8b3cb3e112ae227a80bf2e661272264c3d6145d0e2ff6ad8d88e8eee2db1553f |
| supabase/migrations/04102026_1241_bpay_next_component_selection.sql | 21716604ef859fea23dd1f15d0e5f642528b2f9457e9e96c7acf32615164fe3d |
| supabase/migrations/03102026_1324_banking_pay_next_core.sql | f38878c7b09e3fbac2d21acbf96c4e88bdac1d4c32abac7ee98d2672df7ba52c |
| supabase/repeatable/11102026_0426_banking_pay_next_pay_hold_accepted_choice_v1.sql | aec4c904cee22e748ea1ab3006e8cfdbf50bcdb9dd567b1fb1b2bc9da7391b92 |
| docs/bpay-next/design/DY-06-retention-census.md | 441a009af1bf754bd0d7be2f54afb76a2e97be97ed51d180d9293e05f9ca3ab7 |

Existing DY06 joined journeys and installation/runtime acceptance remain NOT_RUN here. No additional financial test programme or speculative storage is required by this source finding.
