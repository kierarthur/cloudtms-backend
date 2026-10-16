# Genuine native rollback assertions — NOT_RUN

`native-owner-assertions.sqlinc` is fixture source only, outside the 53 install
fragments. It creates a temporary assertion ledger and seven pg_temp functions.
It must never be mounted into a permanent migration/repeatable or normal runtime.

Root may use these only inside its authorised server-owned rollback rehearsal,
with the current complete real owners and actual fixture RPC-created business
origins. No helper constructs a Move, Source movement, final/issued document,
monetary cut, claim, accepted valuation or success receipt. A missing genuine
producer remains a real dependency; do not seed a substitute to run these cases.

The transient witness functions must be invoked at these precise existing owner
seams. Root can append the assertion calls to temporary copies of those exact
owner definitions within its rollback transaction. Preserve the real ownership,
original validation, line projection, lease/fence checks and cleanup. These
temporary assertions do not create authority through a caller name or GUC.

| Actual native seam | Assertion |
| --- | --- |
| End of `invoice_source_admission_line_enter_v1`, after native claim check | `pg_temp.healthroster_v6_source_admission_enter_assert_v1(p_invoice_id)` |
| Start of `invoice_source_admission_line_finish_v1`, after native line INSERT and before stamp removal | `pg_temp.healthroster_v6_source_admission_finish_assert_v1(p_invoice_id,p_invoice_line_id)` |
| End of `invoice_source_allocation_enter_v1`, after native claim check | `pg_temp.healthroster_v6_source_allocation_enter_assert_v1(p_destination)` |
| Existing native generation COMMIT, after real claim entrance, only when saved `source_original_line_plan` exists | `pg_temp.healthroster_v6_source_generation_assert_v1(actual_chunk_id)` |
| Genuine Daily preview with validated current selector/builder result | `pg_temp.healthroster_v6_daily_preview_assert_v1(actual_scope,actual_daily,actual_actor)` |
| Genuine Daily accepted revision, after actual publisher/receipt and before selected invoice edits | `pg_temp.healthroster_v6_daily_accepted_assert_v1(actual_operation_id,old_finance_captured_before_native_revision)` |
| Normal owner return and a separately forced-rollback case | `pg_temp.healthroster_v6_no_source_stamps_assert_v1(actual_invoice_ids,actual_chunk_ids)` |

Header/claim mutations are hostile test inputs inside nested exception
subtransactions. Successful refusal restores the actual original witness; each
function verifies restoration. The original valid predicate must pass first,
so a missing producer cannot masquerade as a successful hostile rejection.
Source pence changes come from the actual native planned line, not alternative
valuation. The duplicate check runs against the real native inserted line.

Use actual native admitted replay and between-Draft allocation APIs for replay,
then verify unchanged real line IDs/counts, full original presentation bytes and
stamp absence. Separate fresh-backend rejection, concurrency, forced rollback,
native ACL, all stored monetary totals and real document bytes remain required
by `../NATIVE_QUALIFICATION_CASES.md`. A source parser pass is not that evidence.
