# Exact F23 UUID cardinality source review

Census 3b9f5ab8cf5ed3bc93f3075f1f0b92483be3ad9c; diagnostic b41e99f03a2040b1efe071051e3716cd0539ea3ee63fc2d8e713c082090dbdb9. All19 census files matched saved current I2 exactly at read commit 742a3b4c6190537c9e87086bd0f20765344e8798. Read only; no product/gate/Bank edits or DB execution.

19 diagnostics expand to21 scanner statement keys and23 direct MIN(TEXT)::UUID expressions.17 diagnostics have exact source cardinality proof; two invoice-edit diagnostics contain an unguarded invoice-line representative and remain refused.

The two unsafe public weekly_source_invoice_edit_context_v1(jsonb) statements emit min(binding.invoice_line_id::text)::uuid per presentation. Native UUID NOT NULL and current-movement uniqueness do not prove one invoice line per presentation. Admission currently authors that relationship and move preserves it, but the read does not assert it; adjustment rule/VAT authority and inverse per-invoice-line allocation checks do not supply the missing guard. Recommend a finite selected-invoice CURRENT group count(distinct invoice_line_id)<>1 refusal before JSON. Preserve legitimate multi-movement NET presentations with one invoice line. No runtime data corruption is asserted.

| # | File | Verdict | Exact F23 statement SHA256 |
|---|---|---|---|
| 1 | supabase/repeatable/13102026_1143_healthroster_scoped_credit/13102026_1143_snapshot.sqlinc | SOURCE_CARDINALITY_PROVED | dac0fc36c09498a8cd4fa9481f6903668ad5cad92b3d826e9639824fe38e187d |
| 2 | supabase/repeatable/13102026_1434_weekly_source_invoice_money_admission_v1.sql | SOURCE_CARDINALITY_PROVED | 4361ae3ffa098ec9508923b756eef87a423c84c59ccdef365c22bab5f506d4d7 |
| 3 | supabase/repeatable/13102026_1435_weekly_source_invoice_only_pay_isolation_v1.sql | SOURCE_CARDINALITY_PROVED | 3c1c23d6593b402f47c9243f965212ad72a1b2b07fa72d3a5001f9d064006394<br>b450a304eb0dc2cc51d11be83e60c4198acec5cbd203797d099ae251624965e0 |
| 4 | supabase/repeatable/13102026_1438_weekly_source_protection_queries/02_wait_witness.sqlinc | SOURCE_CARDINALITY_PROVED | 25fe7948b359818c738576969acb1b0b878e099ea4fc5079245c9e9ab0172866 |
| 5 | supabase/repeatable/13102026_1438_weekly_source_protection_queries/25_bulk_stage_proof.sqlinc | SOURCE_CARDINALITY_PROVED | 40cfc3e00317f59cb11fc7c90ee95f2b68618ce00fa0c4a7b0b1d85f3a9e19cd |
| 6 | supabase/repeatable/13102026_1700_weekly_source_invoice_window_receipt/05_complete_admission.sqlinc | SOURCE_CARDINALITY_PROVED | c242ed2389d7dfd29a64dc3fc2be471fc2c2214200dcebefc0f13d0f0bf053dd |
| 7 | supabase/repeatable/13102026_1700_weekly_source_invoice_window_receipt/08_complete_batch_detail.sqlinc | REAL_SOURCE_CARDINALITY_GAP_DO_NOT_CLASSIFY | 1302a11db9edad395f4c1a2e2f69d6365faabffc445164a6fedeb2df56a2ca69 |
| 8 | supabase/repeatable/13102026_1700_weekly_source_invoice_window_receipt/09_complete_pay_consumers.sqlinc | SOURCE_CARDINALITY_PROVED | 3c1c23d6593b402f47c9243f965212ad72a1b2b07fa72d3a5001f9d064006394<br>b450a304eb0dc2cc51d11be83e60c4198acec5cbd203797d099ae251624965e0 |
| 9 | supabase/repeatable/13102026_1700_weekly_source_invoice_window_receipt/11_complete_native_snapshot_assembler.sqlinc | SOURCE_CARDINALITY_PROVED | 2b02f3745c8cbe9d278d33f8c4b3a584e958fc6f959731e370c773d7eb5c2173 |
| 10 | supabase/repeatable/13102026_1705_weekly_source_invoice_adjustments/05_invoice_edit_context.sqlinc | REAL_SOURCE_CARDINALITY_GAP_DO_NOT_CLASSIFY | f9e61dca47294be32ba1b57de7dbe7ff025220894f506e952f946d0c4397969e |
| 11 | supabase/repeatable/13102026_2110_healthroster_report_native_fin_worker/04_complete_fin_overload.sqlinc | SOURCE_CARDINALITY_PROVED | d935b005b631243b0ec6c76836f8c7a285c66f407cb365fc2e32835f5343ab7d |
| 12 | supabase/repeatable/14102026_0410_healthroster_protected_source_prerequisites/22_q45_actor_event_closed_dto_cross_session_CAPABILITY_SOURCE_CAS_SCHEMA_FORWARD.sqlinc | SOURCE_CARDINALITY_PROVED | 5f853cfc46b6700b525c19e4048c2cf6101c6fc370a15b46789ee87d3dcb6590 |
| 13 | supabase/repeatable/14102026_0410_healthroster_protected_source_prerequisites/41_q45_actor_event_closed_dto_cross_session_11_complete_actor_own_stage.sqlinc | SOURCE_CARDINALITY_PROVED | 40cfc3e00317f59cb11fc7c90ee95f2b68618ce00fa0c4a7b0b1d85f3a9e19cd |
| 14 | supabase/repeatable/14102026_0410_healthroster_protected_source_prerequisites/50_final_original_source_cas_schema_lineage_CAPABILITY_SOURCE_CAS_SCHEMA_FORWARD.sqlinc | SOURCE_CARDINALITY_PROVED | 5f853cfc46b6700b525c19e4048c2cf6101c6fc370a15b46789ee87d3dcb6590 |
| 15 | supabase/repeatable/14102026_0430_healthroster_money_native_interfaces/11_03_cas_operational_projection.sqlinc | SOURCE_CARDINALITY_PROVED | 7a03eeb5942af82b6b2d3e4b9b8ed307878ec16b4d5263f2517c03e983cbf83f |
| 16 | supabase/repeatable/14102026_0541_healthroster_report_current_native_composition/06_complete_fin4_lock_observation.sqlinc | SOURCE_CARDINALITY_PROVED | d935b005b631243b0ec6c76836f8c7a285c66f407cb365fc2e32835f5343ab7d |
| 17 | supabase/repeatable/14102026_0541_healthroster_report_current_native_composition/15_complete_native_report_admission.sqlinc | SOURCE_CARDINALITY_PROVED | 4df31231e0267063a169e6735ff2f5fe29b60248f35c435604a4855814857f2b |
| 18 | supabase/repeatable/14102026_0541_healthroster_report_current_native_composition/14_complete_native_snapshot_assembler.sqlinc | SOURCE_CARDINALITY_PROVED | f6922dbb3d6e6f707af9eb327f22959fb5a2c05e357b89668ec5f8e812e7b74f |
| 19 | supabase/repeatable/14102026_0821_healthroster_fin4_rebill_record_fields/01_complete_fin4.sqlinc | SOURCE_CARDINALITY_PROVED | d935b005b631243b0ec6c76836f8c7a285c66f407cb365fc2e32835f5343ab7d |

REVIEW.json contains each exact signature, file/body/statement pins, native type/constraint sources, complete count/refusal/zero/multiple behavior, first use, and finite recommendation. It also separately reviews the FILTER-wrapped UUID MAX in the source context; current scanner does not emit that adjacent expression. No generic exception is recommended.
