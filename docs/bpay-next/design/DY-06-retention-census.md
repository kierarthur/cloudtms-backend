# DY-06 finite legacy financial-retention census

11 October 2026. Read-only source investigation for DY-06 sequence93. Full card06:1008–1040, checklistD01–D05 and design/DY-06-current-investigation.md govern. No product, DB, fixture, test, capture or hosted operation was performed. Root must independently prove the current catalogue types/indexes before relying on these saved-source declarations.

## Minimum classification owner

Keep the global removal/Archive classifier and sticky marker unchanged. In the existing private._timesheet_route_change_context_v1(uuid,text), only under active_owner NEXT, replace marker-only FINANCIAL_HISTORY interpretation with the complete finite native financial facts below, plus the named existing NEXT accepted/captured/effect facts. Retain every removal blocker except TIMESHEET_STILL_AUTHORISED, current paid/invoiced checks, and the original import → financial → authorised → workflow order.

No existing point helper found distinguishes approval-only marker origin. public.timesheet_removal_financial_history_v1 is the existing bounded family/auth/invoice owner; its archive_required is solely marker existence (monolith153700–153710), so it cannot itself classify approval-only versus pay retention. The existing backfill/completeness routines enumerate all durable financial roots; they are not hot-path helpers and must not be called from this route. Reuse their finite predicate meaning with EXISTS on the already-resolved native physical family (the removal helper enforces maximum32 UUIDs). No ledger amounts, history sums, full batch unnest, Candidate-wide history, new reason column or protocol is needed.

## Complete existing legacy roots

Source producer: public.timesheet_financial_retention_capture_trigger_v1 in supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql:183474–183812. Its backfill at183830 repeats the same twelve roots. Source index locations below use supabase/baseline/22082026_1501_cloudtms_test_structural_finishing.sql unless a different migration is named. They prove saved DDL only, not installed/current index validity.

| Exact root / producer line | Financial fact to retain for a family UUID | Saved point/index support |
| --- | --- | --- |
| public.timesheets_financials,183487 | Any matching physical Timesheet financial row with paid_at_utc, paid_by_user_id, nonblank payment_reference, remittance_last_sent_at_utc, or remittance_send_count >0. Do not restrict this retention check to current rows: former financial versions remain facts. | idx_tsfin_ts(timesheet_id,timesheet_version DESC),finishing9479. Current-only uq_tsfin_current/idx_tsfin_current_timesheet do not cover historical retention. |
| public.pay_batch_items,183551 | Any timesheet_id member, including unpaid Draft and later voided/cancelled historical items. Do not add an active-status/item-type filter. | Full idx_pay_batch_items_timesheet(timesheet_id),finishing9143. Partial finance-item index is insufficient for all item types/history. |
| public.pay_batches,183571 | force_include_timesheet_ids contains/overlaps any family UUID, regardless of batch status. | idx_pay_batches_force_include_ts_gin,finishing9161; original migration03032026_policy_x_changes.sql:48. Use UUID[] containment/overlap supported by that GIN; do not unnest all batches. |
| public.pay_batch_timesheet_snapshots,183598 | Any snapshot timesheet_id. | ix_pay_batch_timesheet_snapshots_timesheet(timesheet_id),finishing9590; migration22022026_banking_tables.sql:307. Batch-leading unique key alone is insufficient. |
| public.timesheet_pay_state,183614 | last_settled_pay_batch_id, last_settled_at_utc, last_settled_signature, last_settled_snapshot_json or summary_pay_paid_at_utc non-NULL; OR upper(coalesce(summary_pay_status_code,'UNPAID')) in PAID, PARTIALLY_PAID, PART_PAID, OVERPAID, UNDERPAID, PROCESSING. Preserve this exact native producer predicate. | timesheet_pay_state_pkey(timesheet_id),finishing4392. |
| public.timesheet_pay_state_history,183676 | Any matching timesheet_id history row, without a status filter. This is a key-bounded EXISTS, not a history scan or aggregate. | idx_timesheet_pay_state_history_ts(timesheet_id,settled_at_utc DESC),finishing9296. |
| public.ts_pay_adjustments,183692 | Any matching timesheet_id adjustment, including paid rows. | ts_pay_adjustments_timesheet_idx(timesheet_id),finishing9839. Candidate/unpaid partial index is insufficient. |
| public.pay_finance_case_components,183708 | Any linked_timesheet_id, including closed components. | Full idx_pay_finance_case_components_linked_timesheet_id,finishing9200; migration15032026_updated_tables_for_new_pay_umbrella_switches_pay.sql:264. Open-only index is insufficient. |
| public.pay_advances,183728 | Any linked_timesheet_id advance, across all kinds/statuses, cleared or written-off history. | No full linked-Timesheet point index found in saved SQL. Existing candidate/open and OVERPAYMENT/UNDERPAYMENT status-partial unique indexes do not cover this predicate. Fresh catalogue must confirm; if absent, only a linked_timesheet_id index (optionally WHERE linked_timesheet_id IS NOT NULL) is required. |
| public.pay_payment_correction_items,183748 | Any matching timesheet_id correction, including terminal history. | pay_payment_correction_items_timesheet_key_idx(timesheet_id,economic_key_type,economic_key_value),finishing9707; migration30042026_1341_newcolumns_reverse_payments.sql:492. |
| public.pay_manual_adjustment_carry_forwards,183764 | Any matching timesheet_id carry-forward, including CANCELLED/SUPERSEDED/consumed rows. | No Timesheet-leading index found in saved SQL; its PK is id (finishing4323). Fresh catalogue must confirm; if absent, only timesheet_id (optionally WHERE timesheet_id IS NOT NULL) is required. |
| public.timesheet_payment_overrides,183780 | Any timesheet_id override, including cleared/consumed history. | Full bpay_timesheet_payment_overrides_work_fk_v1(timesheet_id,bpay_work_revision_v1),migration26092026_0200_banking_pay_stage2_work_reference_schema.sql:232. Active-only partial indexes are insufficient. |

Physical/link identities are UUIDs in the structural baseline. force_include_timesheet_ids is UUID[]. Exact table declarations in structural_baseline1500 are: TF4364; batch items3291; batches3362; snapshots3349; pay state4176; pay-state history4189; adjustments4530; finance components3430; advances3059; corrections3578; carry-forwards3523; overrides4199. timesheet_pay_state has NOT NULL last_settled_snapshot_json/jsonb and last_settled_signature/text in saved baseline4176–4187. Therefore its native IS NOT NULL predicate admits any row carrying those fields even if summary status reads UNPAID: do not invent a new “nonempty signature/JSON” policy or reduce the check to PAID/paid_at. Root's fresh type/default census should record the actual schema.

No legacy batch/advance/correction status filter is authorised for these existence roots: marker capture/backfill does not apply one. Its UPDATE branch marks old as well as new identities when a relation moves; sticky marker rows are never deleted or repurposed. Do not infer marker origin from first_retained_at_utc or treat an approval mark as an authorised zero. This census supplies the card's existing-fact classification; it does not claim a reason is recoverable from the reasonless marker itself.

## Existing invoice and NEXT facts

Reuse the existing removal helper's invoice predicates at153630–153697: native current financial invoice locks/locked timestamp, locked segment identities, invoice_lines by exact physical UUID or its original booking fallback, and INVOICED Contract Week. Keep its blockers independently from archive_required. Invoice lines already have idx_invoice_lines_ts(timesheet_id),finishing8840; a new invoice protocol/table is unnecessary for this correction. Preserve the existing booking fallback owner rather than widening it.

For NEXT, keep the actual immutable Work resolved from the native booking identity. Core1324:128 supplies booking uniqueness; idx_timesheets_booking_id supports the physical family (finishing9326). Accepted eligible Work choice plus its original run_selection/revision is already a retention fact while preparation is PINNING, before run_line exists: do not require SEALED/DRAFT/READY and do not count skipped run_choice_member rows. Existing work_choice keys are run-leading; its Work-leading lookup needs only private.bpay_next_work_choice(work_id,run_id) if fresh catalogue confirms the saved-source gap. Captured run_work exists by bpay_next_run_work_work_idx(work_id,id),core1324:1033–1034; financial_effect has the Work-leading index at1011–1012. Native invoice facts remain the existing retained invoice owner; no separate NEXT invoice-lock table was found in this census, and none should be fabricated.

Read-only classification does not itself serialize destructive rotation against accepted Create. Reuse the current investigation's exact Work-lock/reproof requirement in the actual native mutation; do not turn this census into a new latch/framework. The correct existing0939 OLD UUID/version withdrawal remains unchanged.

## Source authority and provenance

The saved repeatable producer/backfill/removal definitions are in26052026_2100HRS_NEW_FUNCTIONS.sql; their baseline routine counterparts are1503_routines16/18. The route context is in08082026_2035_timesheet_route_version_rotate.sql. No matching entry for these private/native context/retention signatures was found in the current final-definer registry; do not label an unlisted ancestor as an independently proved installed final definer. Root's exact catalogue/definition proof remains required. The current public route dispatcher is the retained0922 successor described by DY-06-current-investigation.md.

| Read saved source | SHA256 |
| --- | --- |
| monolith26052026_2100HRS_NEW_FUNCTIONS.sql | 8b3cb3e112ae227a80bf2e661272264c3d6145d0e2ff6ad8d88e8eee2db1553f |
| route2035 | d61a1cd25137aad4387345dc6fe6c120c5448e28bb8f30f37ddfb091b376664a |
| structural baseline1500 | aec4d8985fed34c08cfd61521db4ad27b273a85e90251ccd8007eee0489b7730 |
| structural finishing1501 | 367d70659e4ab05a7f18a86fd29b3d74d018d3f916a1e1e38f23e63243bfb304 |

The minimum remaining source support is the exact classifier successor and only proved-missing point indexes, plus the already-identified native mutation serialization. No new money owner, history reader, reason column, public API, data backfill or test programme is proposed. Installed/query-plan/application acceptance remains NOT_RUN by this reviewer.
