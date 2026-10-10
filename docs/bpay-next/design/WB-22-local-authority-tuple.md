# WB-22 finite local reconciliation authority

Read-only recommendation, 10 October2026. These tuples qualify **unsent local Keep/untick state**, not persisted New/Changed equality, a financial snapshot or monetary authority. Persisted memory remains root's exact per-native LEAF/set comparison, with no timestamps/Candidate FV/digest. No product edits, SQL/runtime/tests/data/fixtures/network.

## Exact native fields

Actual position table is `private.bpay_next_position`, not a separate work_position object. Core1324:328–349 gives PK `(work_id,component_key)`, `applied_revision_id`, `source_basis_channel`, `approved_source_ex_vat`, `realised_source_ex_vat`, `held_source_ex_vat`, realised/held target `ex_vat/vat/inc_vat`, and `updated_at_utc`. Source extension2010:24–38 adds genuine `source_book_state`, `source_book_revision`, `native_realised_paye`, `native_realised_umbrella`, `native_held_paye`, `native_held_umbrella`; these are real existing authority, not a new version owner. Work1324:111–129 supplies Candidate/contract/native current/applied revision and approval_state. Approved-line identity/current component mapping is original immutable revision membership.

Snooze1100 schema7–22/51–60 stores real scope-specific `control_sequence` on row and immutable events. Original SET/CLEAR1100:234–235/297–315 increments it from the original accepted command; guarded current row retains it after clear. **Include inactive/cleared relevant rows**, otherwise clearing the last active Snooze can erase the comparison signal. Restrict component to genuine applicable whole-Work/expense-all/same-component scopes; a different component's SEGMENT Snooze should not pollute the child tuple. Work-wide parent covers all its own scopes. Case uses exact Case-component. Do not read an unrelated Candidate-global command clock.

Candidate target terms are fixed direct values: normalized current `pay_method`, `umbrella_id`, actual `umbrellas.vat_chargeable`, `min_take_home_wtd`. No names/reference/bank warning/metadata timestamp. Query/pay-hold qualification remains exact current scoped maintained facts/current original predicate; original Create1832:420–424 obtains actual current `timesheets_financials.pay_on_hold` through current physical booking row, not an unpopulated Workbench placeholder. Reader tuples can retain existing qualified query_state/query_qualified/pay_hold availability; they cannot turn unavailable evidence into CLEAR/false.

## Component and Case recommendation

| Local identity | Fixed authority tuple |
|---|---|
| Work component | kind, Candidate+Work+component_key, current/applied revision and approval_state, actual approved_line ID, position presence/applied revision/source_basis_channel, all A/C/H and target counters, Source book state/revision/native channel counters when applicable, same-component/whole applicable Snooze max control_sequence, exact current target/floor/VAT terms, scoped query/pay-hold eligibility/known fields. Preserve null/presence when no native row. Do not reuse Candidate FV. |
| Case component | kind, Case/Candidate/component UUID, component_revision, rule ID/revision, actual instruction/direction/stage/source channel, resolution_state/target channel/policy/window/resolution_ref, A/funded/recovered/paid-credit/writeoff/active-payout/active-recovery counters, actual current Case status/rule terms/next-due authority and exact applicable Snooze max, Candidate target/floor/VAT. Current0300:74–98 gives these counters and resolution fields; component_revision alone is insufficient for hold/settlement/recovery changes. |
| Candidate parent | Existing worker_control.financial_view_revision is a legitimate coarse **Candidate-parent local** guard, separated from child persisted basis/default equality. It should not qualify Work/ROW/Case child overrides. |

All tuple keys must be closed/typed; UUIDs and nullable integers/string-decimal amounts mirror actual values. No monetary sums in Office, full component JSON, segment list, audit fields in accepted basis or new hash. Immutable revision ID pins the approved segment inventory; detailed native availability/set equality stays server-owned. The tuple is local reconciliation data, never the accepted Rule-B expected revision/settings/query/PIN input replacement.

## Whole Work parent: proposed existing timestamp marker

Root proposed Work current/applied revision + own position-row count/direct A/C/H/target sums/max existing position.updated_at_utc + own source_book_revision aggregate + own applicable Snooze max and Candidate target settings. A static census of **current registry physical definitions** found nine direct position statements in eight functions:

| Final physical source and line | Current writer | Existing timestamp |
|---|---|---|
| `10102026_1346_banking_pay_next_position_projection_boundary_v1.sql:316` | apply_position_page INSERT/ONCONFLICT | default on INSERT; explicit transaction_timestamp on conflict |
| same384 | apply_position_page obsolete component zeroing | transaction_timestamp |
| `10102026_1438_banking_pay_next_hold_workbench_projection_v1.sql:246` | hold_fresh_position_page | transaction_timestamp |
| `09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql:2472` | post_simple_outcome_page | transaction_timestamp |
| `10102026_0316_banking_pay_next_csv_umbrella_row_projection_v1.sql:479` | apply_work_collection_recovery | transaction_timestamp |
| `04102026_0260_banking_pay_next_simple_cancel_v1.sql:403` | cancel_simple_worker_page | v_now |
| `04102026_0460_banking_pay_next_case_cancel_v1.sql:276` | cancel_case_worker_page | v_now |
| `04102026_0510_banking_pay_next_preparation_expiry_v1.sql:401` | expire_preparation_worker_page | v_now |
| `10102026_1309_banking_pay_next_workbench_source_seed_refresh_v1.sql:33` | source_disposition_seed | **no timestamp assignment**; changes source_book_state and source_book_revision from UNBOUND/0 to READY/1 |

Thus every enumerated direct native amount mutation updates the existing position timestamp; Source seed is the concrete exception and must be covered by actual source-book state/revision/counters in the local tuple. The current native Source book guard additionally owns book fields on relevant original counter changes; do not add a writer to manufacture a local timestamp. These are original financial row writers; Candidate name/bank metadata continuation does not mutate these positions. Original Work projection refresh is read/projection-only, so this local marker avoids unrelated Candidate FV.

Use the maximum timestamp only as **existing native row-mutation evidence**, with original native balances/revision/count/book terms as direct comparison. It is not a monotonic sequence or a proof encoding every component vector: transaction_timestamp can precede commit, two rows can be updated in one transaction, and equal aggregate A/C/H can conceal redistribution between components. Do not promote a matching aggregate into exact persisted equality or claim exact per-component history from it. The new native baseline set comparison supplies precise New/Changed defaults and original Create still validates every actual component/revision under its original locks.

There is no existing universal independent Work-level scalar version in these schemas. Command_member is Candidate-only (core816–821), and job is Candidate/command-based (967–990); pretending either is a Work-specific complete mutation revision would invent authority. A new digest/clock/producer is prohibited here. For local Work parent, the proposed fixed native marker is the smallest conservative existing-source tuple; if a dirty local copy lacks its originally observed tuple, or current exact server component/default facts expose a mismatch that the parent marker does not distinguish, retain the intent visibly unresolved and require the existing explicit row/global choice. Do not attach today's tuple to the old untick. This follows BD077's explicit reconciliation rather than fabricating full original inventory history.

Source metadata-only changes must neither advance persisted New/Changed nor silently erase local exclusions. Work native timestamp is **local-only**; no position timestamp field belongs in per-native financial baseline. The same distinction applies to Candidate FV. Server replies stay fixed shape, null-aware and bounded; root alone implements and concrete successor review remains required.
