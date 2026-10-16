# Exact F23 dynamic SQL source review

Census 3b9f5ab8cf5ed3bc93f3075f1f0b92483be3ad9c.95 exact statements in35 file/signature groups; all saved current source files matched census bytes at ef5468b5288e2d4ffef61edb20b703583fe2b66c. No source/gate/Bank/DB changes.

All95 are real EXECUTE, not inert-only metadata. Two broad1430catalog CHECK-drop loops lack exact predecessor-set/refusal proof. The guarded single-column price and counted capability selectors have explicit expression-equality caveats. No generic format(), dollar-body, optional-helper or pg_temp exception is recommended.

Constant credit SQL was parsed offline (one9511-byte SQL statement,8DML CTEs on6relations). This is grammar/source inventory only. Existing financial/native/ACL/race/provider/rollback qualification remainsNOT_RUN.

| Group | File/signature | Points | Classification |
|---|---|---|---|
| 1 | supabase/migrations/13102026_1133_weekly_source_rule_sets_schema.sql<br>outer:supabase/migrations/13102026_1133_weekly_source_rule_sets_schema.sql | 4 | CLOSED_NEW_TABLE_RLS_ACL_LOOP |
| 2 | supabase/migrations/13102026_1344_weekly_source_self_bill_report_schema.sql<br>outer:supabase/migrations/13102026_1344_weekly_source_self_bill_report_schema.sql | 4 | CLOSED_NEW_TABLE_RLS_ACL_LOOP |
| 3 | supabase/migrations/13102026_1430_weekly_source_invoice_money_origins.sql<br>outer:supabase/migrations/13102026_1430_weekly_source_invoice_money_origins.sql | 3 | CATALOG_CONSTRAINT_DROPS |
| 4 | supabase/migrations/13102026_1680_weekly_source_invoice_window_receipt_schema.sql<br>outer:supabase/migrations/13102026_1680_weekly_source_invoice_window_receipt_schema.sql | 1 | CLOSED_SINGLE_NATIVE_UNIQUE_DROP |
| 5 | supabase/migrations/14102026_0400_healthroster_protected_source_capability.sql<br>outer:supabase/migrations/14102026_0400_healthroster_protected_source_capability.sql | 1 | COUNTED_SUBSTRING_NATIVE_CHECK_REPLACEMENT |
| 6 | supabase/migrations/15102026_0802_weekly_contact_pause_permits.sql<br>outer:supabase/migrations/15102026_0802_weekly_contact_pause_permits.sql | 4 | CLOSED_NEW_TABLE_RLS_ACL_LOOP |
| 7 | supabase/migrations/16102026_0142_weekly_contact_reuse_native_attempt_uniqueness.sql<br>outer:supabase/migrations/16102026_0142_weekly_contact_reuse_native_attempt_uniqueness.sql | 1 | CONSTANT_PROVED_REDUNDANT_INDEX_DROP |
| 8 | supabase/migrations/16102026_0159_nhsp_v6_01_physical_source_review_schema.sql<br>outer:supabase/migrations/16102026_0159_nhsp_v6_01_physical_source_review_schema.sql | 13 | CLOSED_PRIVATE_NEW_TABLE_RLS_ACL_LOOPS |
| 9 | supabase/migrations/16102026_0339_weekly_contact_query_lineage_and_claim_fence.sql<br>outer:supabase/migrations/16102026_0339_weekly_contact_query_lineage_and_claim_fence.sql | 1 | CLOSED_SINGLE_NATIVE_UNIQUE_DROP |
| 10 | supabase/repeatable/13102026_1111_healthroster_credit_runtime/13102026_1111_generation.sqlinc<br>private._invoice_generation_advance_core_v8(jsonb,timestamp with time zone) | 1 | CONSTANT_NATIVE_CREDIT_CTE_EXECUTION |
| 11 | supabase/repeatable/13102026_1127_healthroster_correction_requests/13102026_1127_context.sqlinc<br>private.timesheet_correction_actor_allowed_v1(uuid) | 1 | LITERAL_PRIVATE_BOOLEAN_CALL |
| 12 | supabase/repeatable/13102026_1127_healthroster_correction_requests/13102026_1127_context.sqlinc<br>private.timesheet_correction_actor_assert_v1(uuid) | 1 | LITERAL_PRIVATE_BOOLEAN_CALL |
| 13 | supabase/repeatable/13102026_1127_healthroster_correction_requests/13102026_1127_context.sqlinc<br>private.timesheet_correction_origin_v1(uuid) | 2 | LITERAL_OPTIONAL_ORIGIN_READ_CALLS |
| 14 | supabase/repeatable/13102026_1127_healthroster_correction_requests/13102026_1127_cancel.sqlinc<br>private.timesheet_correction_cancel_capability_v1(uuid,uuid) | 1 | GUARDED_NATIVE_TEMP_CAPABILITY_READ |
| 15 | supabase/repeatable/13102026_1143_healthroster_scoped_credit/13102026_1143_generation.sqlinc<br>private._invoice_generation_advance_core_v8(jsonb,timestamp with time zone) | 1 | CONSTANT_NATIVE_CREDIT_CTE_EXECUTION |
| 16 | supabase/repeatable/13102026_1205_healthroster_issued_signed_query_v1.sql<br>private.weekly_source_issued_signed_query_evidence_v1(uuid,uuid) | 1 | LITERAL_OPTIONAL_SIGNED_SCOPE_READ |
| 17 | supabase/repeatable/13102026_1222_healthroster_configured_upload_profile_v1.sql<br>private.weekly_source_configured_upload_profile_v1(uuid,uuid,text,text) | 1 | LITERAL_CURRENT_EFFECTIVE_RULE_READ |
| 18 | supabase/repeatable/13102026_1438_weekly_source_protection_queries/23_bulk_signed_selection.sqlinc<br>private.weekly_source_protect_all_signed_selection_v1(uuid,uuid,jsonb) | 1 | LITERAL_OWN_STAGE_SELF_BILL_FACT_READ |
| 19 | supabase/repeatable/13102026_1438_weekly_source_protection_queries/25_bulk_stage_proof.sqlinc<br>private.weekly_source_bulk_source_cas_assert_v1(uuid,uuid[]) | 2 | LITERAL_NATIVE_CAPABILITY_AND_CAS_READS |
| 20 | supabase/repeatable/13102026_1438_weekly_source_protection_queries/27_automatic_reconcile.sqlinc<br>private.weekly_source_automatic_reconcile_capability_assert_v1(uuid) | 1 | LITERAL_NATIVE_AUTOMATIC_READINESS_READ |
| 21 | supabase/repeatable/13102026_1438_weekly_source_protection_queries/27_automatic_reconcile.sqlinc<br>private.weekly_source_automatic_reconcile_certificate_v1(uuid,uuid,uuid) | 1 | LITERAL_NATIVE_ZERO_DELTA_CERTIFICATE_READ |
| 22 | supabase/repeatable/healthroster_wp_u3_source_proposals/wp_u3_client_week_context.sqlinc<br>private.weekly_source_healthroster_checklist_leaf_v1(jsonb) | 1 | LITERAL_QUALIFIED_WAIT_FACT_READ |
| 23 | supabase/repeatable/13102026_2205_weekly_source_automatic_native_delta/03_approved_detail_evidence.sqlinc<br>private.weekly_source_approved_detail_evidence_v1(uuid,jsonb) | 1 | LITERAL_CHOSEN_DETAIL_RELATION_READ |
| 24 | supabase/repeatable/13102026_2205_weekly_source_automatic_native_delta/13_complete_final_acl_union.sqlinc<br>outer:supabase/repeatable/13102026_2205_weekly_source_automatic_native_delta/13_complete_final_acl_union.sqlinc | 17 | PINNED_LITERAL_REGISTRY_ACL_CLOSURE |
| 25 | supabase/repeatable/14102026_0410_healthroster_protected_source_prerequisites/06_cap_original_functions_06_complete_selection.sqlinc<br>private.weekly_source_protect_all_signed_selection_v1(uuid,uuid,jsonb,uuid[]) | 1 | LITERAL_OWN_STAGE_SELF_BILL_FACT_READ |
| 26 | supabase/repeatable/14102026_0410_healthroster_protected_source_prerequisites/16_q25_owner_xid_qualification_03_complete_cas_admission.sqlinc<br>private.weekly_source_bulk_source_cas_assert_v1(uuid,uuid[]) | 2 | LITERAL_NATIVE_CAPABILITY_AND_CAS_READS |
| 27 | supabase/repeatable/14102026_0410_healthroster_protected_source_prerequisites/18_q25_owner_xid_qualification_05_complete_automatic_admission.sqlinc<br>private.weekly_source_automatic_reconcile_capability_assert_v1(uuid) | 1 | LITERAL_NATIVE_AUTOMATIC_READINESS_READ |
| 28 | supabase/repeatable/14102026_0410_healthroster_protected_source_prerequisites/27_q45_actor_event_closed_dto_cross_session_05_complete_automatic_admission.sqlinc<br>private.weekly_source_automatic_reconcile_capability_assert_v1(uuid) | 1 | LITERAL_NATIVE_AUTOMATIC_READINESS_READ |
| 29 | supabase/repeatable/14102026_0410_healthroster_protected_source_prerequisites/38_q45_actor_event_closed_dto_cross_session_08_complete_cas_admission.sqlinc<br>private.weekly_source_bulk_source_cas_assert_v1(uuid,uuid[]) | 2 | LITERAL_NATIVE_CAPABILITY_AND_CAS_READS |
| 30 | supabase/repeatable/14102026_0410_healthroster_protected_source_prerequisites/40_q45_actor_event_closed_dto_cross_session_10_complete_actor_bulk_selection.sqlinc<br>private.weekly_source_protect_all_signed_selection_v1(uuid,uuid,jsonb,uuid[]) | 1 | LITERAL_OWN_STAGE_SELF_BILL_FACT_READ |
| 31 | supabase/repeatable/14102026_0430_healthroster_money_native_interfaces/14_07_automatic_capability_admission.sqlinc<br>private.weekly_source_automatic_reconcile_capability_assert_v1(uuid) | 1 | LITERAL_NATIVE_AUTOMATIC_READINESS_READ |
| 32 | supabase/repeatable/14102026_0430_healthroster_money_native_interfaces/15_08_cas_admission_owner.sqlinc<br>private.weekly_source_bulk_source_cas_assert_v1(uuid,uuid[]) | 2 | LITERAL_NATIVE_CAPABILITY_AND_CAS_READS |
| 33 | supabase/repeatable/14102026_0709_healthroster_current_literal_classification/01_complete_current_literal_classification.sqlinc<br>outer:supabase/repeatable/14102026_0709_healthroster_current_literal_classification/01_complete_current_literal_classification.sqlinc | 17 | PINNED_LITERAL_REGISTRY_ACL_CLOSURE |
| 34 | supabase/verification/13102026_1206_weekly_source_rule_sets_verification.sql<br>pg_temp.f1_refuses(text,text) | 1 | TEMP_REFUSAL_FIXTURE_EXECUTOR |
| 35 | supabase/verification/13102026_1440_weekly_source_invoice_money_staging_hold_verification.sql<br>pg_temp.f5_refuses(text,text) | 1 | TEMP_REFUSAL_FIXTURE_EXECUTOR |

REVIEW.json records all95 exact statement/body/file hashes,35 complete producer assessments, closed identifiers/USING values, exact emitted SQL/templates, write targets/callees, whole ACL registry sets and constructors, fixed rollback-fixture callers, baseline predecessor CHECKs and finite recommendations. Native/external remainsNOT_RUN.
