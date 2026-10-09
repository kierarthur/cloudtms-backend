# F-02 port inventory

Fixed backend source a6f7423eb71e6e7cd173e705c2290e5408a1d077; fixed Office source aada67455afe5a3b6828a713c7062832c0617782. Every dirty RESET/OFFICE path appears once. Hashes R/B/U are complete Git blob identities in PORT_INVENTORY.json; H is a matching historical upstream commit. No product port is implied by classification. Reference sources remain read-only.

## RESET — 636 paths

C1=177; C2=15; C3=1; C4=5; C5=22; C6=148; C7=3; C8=264; C9=1

| Path | Class | R / B / U | H | Owner | Action |
|---|---|---|---|---|---|
| broker/src/candidate-app-backend.js | C2 | 7df98b8d / dda990e1 / 6dd94b63 | cf83b626 | F-03/F-08 keep upstream only | Stale reference bytes match historical upstream; keep fixed upstream. |
| broker/src/index.js | C9 | f4c9867c / 3672aa3d / e63ff85f | no | F-10/F-11/F-12/F-14 | Hunk-level NEXT port against current upstream; preserve all unrelated changes. |
| broker/src/weekly-source/authorise-routing.mjs | C1 | ae602ede / 131146fb / ae602ede | no | none (already upstream) | Already published; keep fixed upstream. |
| broker/src/weekly-source/protected-action-orchestrator.mjs | C4 | a91468cb / 94e3b64f / 3817e125 | no | F-14 | Both changed; three-way merge reference/base/upstream, retaining upstream fixes. |
| docs/weekly-source/WEEKLY_SOURCE_QUERY_FINALISATION_DECISIONS_20261002.md | C4 | 60b8d80c / 4d97ffe2 / c4bfe00c | no | F-08 documentation triage | Reference-only documentation delta; do not overwrite current decisions/owner rulings. |
| scripts/cloudtms-db-release-lib.mjs | C1 | a4f84994 / 7a446142 / a4f84994 | no | none (already upstream) | Already published; keep fixed upstream. |
| scripts/cloudtms-db-release.mjs | C1 | ad77b0c0 / b5d5628a / ad77b0c0 | no | none (already upstream) | Already published; keep fixed upstream. |
| scripts/generate-h1h2-successor-reassert.mjs | C3 | 2d0e57b1 / 8ffd0be1 / 8ffd0be1 | no | F-08/F-23 source gate | Port only reviewed local delta. |
| supabase/release/current-contract.json | C7 | 1cd09019 / 9b9141cc / 6d5260a3 | no | F-23 | Preserve current upstream metadata; deliberately extend manifest/append immutable lock; regenerate contract. |
| supabase/release/current-release.json | C7 | ad804383 / 5bd9edb9 / f0cf5574 | no | F-23 | Preserve current upstream metadata; deliberately extend manifest/append immutable lock; regenerate contract. |
| supabase/release/migration-lock.json | C7 | aba713c3 / b8b3e78a / 75869b7b | no | F-23 | Preserve current upstream metadata; deliberately extend manifest/append immutable lock; regenerate contract. |
| supabase/repeatable/01102026_1201_weekly_source_combined_workspace.sql | C2 | e8724258 / 0750f7e4 / ea70fabc | 420513c6 | F-03/F-08 keep upstream only | Stale reference bytes match historical upstream; keep fixed upstream. |
| supabase/repeatable/02102026_1601_weekly_source_manual_review_v1.sql | C1 | cd09b0f5 / 850b1241 / cd09b0f5 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/03102026_0500_h1h2_successor_authorities_after_retry.sql | C1 | 373d7776 / fb1845f9 / 373d7776 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/07082026_2224_candidate_app_weekly_office_replacements_v1.sql | C1 | c529c7d8 / 3aa6fdd8 / c529c7d8 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/08082026_2035_timesheet_route_version_rotate.sql | C1 | 4bbf8f94 / f3b9b666 / 4bbf8f94 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/14082026_1310_timesheet_processing_status_and_authorise_authority_v1.sql | C1 | 23d92dd0 / 6ce45e1f / 23d92dd0 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/15012026_summarytotals.sql | C1 | cd25d4a6 / 9bf339af / cd25d4a6 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/15092026_1534_weekly_source_acl_contract_v1.sql | C1 | b820fa7d / 62191f8a / b820fa7d | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/15092026_1534_weekly_source_charge_acceptance_v1.sql | C1 | da853e6d / 5d30add0 / da853e6d | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/15092026_1534_weekly_source_correct_final_source_v1.sql | C1 | 07b4efcd / 0d103552 / 07b4efcd | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/15092026_1534_weekly_source_finalisation_v1.sql | C1 | 6e91d9aa / eba4d19a / 6e91d9aa | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/15092026_1534_weekly_source_invoice_batch_ordinary_isolation_v1.sql | C1 | 2be7d087 / 667e6fd7 / 2be7d087 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/15092026_1534_weekly_source_ordinary_pay_projection_v1.sql | C1 | 7a646961 / b1ab8b70 / 7a646961 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/15092026_1534_weekly_source_projection_build_v1.sql | C2 | 2bf21c54 / 57858498 / 830efb9c | 420513c6 | F-03/F-08 keep upstream only | Stale reference bytes match historical upstream; keep fixed upstream. |
| supabase/repeatable/15092026_1534_weekly_source_protected_action_orchestration_v1.sql | C2 | 851ed760 / a5432f3f / a1fc10eb | 420513c6 | F-03/F-08 keep upstream only | Stale reference bytes match historical upstream; keep fixed upstream. |
| supabase/repeatable/15092026_1534_weekly_source_protected_pay_c1_publication_v1.sql | C1 | dc3cc462 / 2d88cce3 / dc3cc462 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/15092026_1534_weekly_source_protected_pay_publisher_v1.sql | C1 | 1eddcabe / afd172fa / 1eddcabe | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql | C1 | 004411a5 / 81737f20 / 004411a5 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/15092026_1534_weekly_source_read_projections_v1.sql | C1 | a818f16b / 5b8007ec / a818f16b | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/15092026_1534_weekly_source_timesheet_lineage_v1.sql | C1 | 574a02ea / 449cfeda / 574a02ea | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/15092026_1534_weekly_source_upload_publication_v1.sql | C1 | fa0ceeb1 / b89f25d0 / fa0ceeb1 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/17092026_0300_weekly_source_entitlement_publication_v1.sql | C1 | d6b233ae / f654dabd / d6b233ae | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/17092026_0600_weekly_source_first_authorisation_v1.sql | C1 | eeefd025 / 2cc71311 / eeefd025 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/17092026_0700_weekly_source_pending_entitlement_release_v1.sql | C1 | 2ba0a7bb / 3a840a2d / 2ba0a7bb | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/17092026_1100_weekly_source_candidate_view_producer_v1.sql | C1 | 4e190c36 / fbcfd1b9 / 4e190c36 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/17092026_1200_weekly_source_audit_and_export_v1.sql | C1 | f329b95b / 2e18ed05 / f329b95b | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/19012026_extras.sql | C1 | 1e8810b0 / 4d8e2ae0 / 1e8810b0 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/21072026_1235_37_tsfin_write_current_snapshot_single_bounded.sql | C1 | d65416e1 / 8d814700 / d65416e1 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/26092026_0202_banking_pay_stage2_source_authorisation_v1.sql | C1 | cded25d4 / 07ba8363 / cded25d4 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/verification/15092026_1534_weekly_source_acl_contract_v1.sql | C1 | b5d65fab / dd793bf0 / b5d65fab | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/verification/15092026_1534_weekly_source_finalisation_v1.sql | C2 | cdee38d9 / ddd7b658 / 84274dd4 | fc7628f9 | F-03/F-08 keep upstream only | Stale reference bytes match historical upstream; keep fixed upstream. |
| supabase/verification/15092026_1534_weekly_source_invoice_admission_v1.sql | C2 | d1b154a4 / 6798c105 / 7f07e76a | 420513c6 | F-03/F-08 keep upstream only | Stale reference bytes match historical upstream; keep fixed upstream. |
| supabase/verification/15092026_1534_weekly_source_invoice_batch_integration_v1.sql | C2 | c7747809 / 1447338f / bf35cb9f | 420513c6 | F-03/F-08 keep upstream only | Stale reference bytes match historical upstream; keep fixed upstream. |
| supabase/verification/15092026_1534_weekly_source_ordinary_pay_projection_v1.sql | C2 | a6b6c49a / 4c832294 / f8e28ad8 | 420513c6 | F-03/F-08 keep upstream only | Stale reference bytes match historical upstream; keep fixed upstream. |
| supabase/verification/15092026_1534_weekly_source_protected_pay_publisher_v1.sql | C1 | b5e60401 / 5456dc66 / b5e60401 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/verification/15092026_1534_weekly_source_query_delivery_v1.sql | C1 | 0b77d8a1 / 84728436 / 0b77d8a1 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/verification/15092026_1534_weekly_source_read_projections_v1.sql | C2 | 1e3c6dd7 / 47972116 / 193887e6 | 420513c6 | F-03/F-08 keep upstream only | Stale reference bytes match historical upstream; keep fixed upstream. |
| supabase/verification/17092026_0300_weekly_source_entitlement_publication_v1.sql | C1 | 1a34d6fc / 1fb8d64c / 1a34d6fc | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/verification/17092026_0610_weekly_source_withdrawal_supersession_v1.sql | C1 | 4efeafe6 / ad8fff4c / 4efeafe6 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/verification/17092026_0900_weekly_source_installed_writer_census_v1.sql | C4 | 6b62e1f3 / ff08c816 / a8d27fd7 | no | F-03/F-06/F-23 | Merge only thirteenth exact dynamic-reader acknowledgement after canonical local compile; preserve current upstream census. |
| supabase/verification/17092026_1100_weekly_source_candidate_view_producer_v1.sql | C2 | 0df45fdb / bc40f0f6 / bef80f23 | 420513c6 | F-03/F-08 keep upstream only | Stale reference bytes match historical upstream; keep fixed upstream. |
| supabase/verification/17092026_1200_weekly_source_audit_and_export_v1.sql | C2 | 920dec65 / c286edef / 5ab07d2c | 420513c6 | F-03/F-08 keep upstream only | Stale reference bytes match historical upstream; keep fixed upstream. |
| supabase/verification/22082026_1302_general_browser_isolation_verification.sql | C1 | 438404b0 / 0ee492e9 / 438404b0 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/verification/27082026_1947_candidate_named_security_verification_v3.sql | C1 | b423e00b / 5c23a212 / b423e00b | no | none (already upstream) | Already published; keep fixed upstream. |
| tests/database-release-system.test.mjs | C2 | 39df8cca / 62dd6d7e / c4ed99cb | 420513c6 | F-03/F-08 keep upstream only | Stale reference bytes match historical upstream; keep fixed upstream. |
| tests/weekly-source/miget-finalizer-census-pin.test.mjs | C1 | 0174916f / 04711466 / 0174916f | no | none (already upstream) | Already published; keep fixed upstream. |
| tests/weekly-source/protected-action-orchestrator.test.mjs | C4 | 0faf5c3d / ab1597db / 5217e5d5 | no | F-08/F-23 existing source gate only | Both changed; three-way merge reference/base/upstream, retaining upstream fixes. |
| tests/weekly-source/weekly-source-acl-contract.test.mjs | C4 | 2c5a4e41 / 27bf4eb2 / d7fdcc8b | no | F-08/F-23 existing source gate only | Both changed; three-way merge reference/base/upstream, retaining upstream fixes. |
| tests/weekly-source/weekly-source-invoice-batch-integration-sql-contract.test.mjs | C1 | 324ff1fa / 8afd86bb / 324ff1fa | no | none (already upstream) | Already published; keep fixed upstream. |
| tests/weekly-source/weekly-source-rate-warning-acceptance-sql-contract.test.mjs | C1 | 9cb19bf7 / 0c73b96c / 9cb19bf7 | no | none (already upstream) | Already published; keep fixed upstream. |
| broker/src/bpay-next-background.js | C6 | 7dfc37cf / absent / absent | no | F-09 (referenced feature successor later) | New local; select product source by exact card; do not wholesale port. |
| broker/src/bpay-next-command-rpc.js | C6 | 7106ed2e / absent / absent | no | F-09 (referenced feature successor later) | New local; select product source by exact card; do not wholesale port. |
| broker/src/bpay-next-command.js | C5 | 97766bcc / absent / 6700c733 | no | F-09 | Both added; manual local delta onto fixed upstream. |
| broker/src/bpay-next-financial-queue.js | C6 | a4f51741 / absent / absent | no | F-09 (referenced feature successor later) | New local; select product source by exact card; do not wholesale port. |
| broker/src/bpay-next-frozen-advice-rpc.js | C6 | c5d31e74 / absent / absent | no | F-09 (referenced feature successor later) | New local; select product source by exact card; do not wholesale port. |
| broker/src/bpay-next-frozen-advice.js | C6 | c4b6a2ca / absent / absent | no | F-09 (referenced feature successor later) | New local; select product source by exact card; do not wholesale port. |
| broker/src/bpay-next-frozen-read.js | C6 | d9b06f6d / absent / absent | no | F-09 (referenced feature successor later) | New local; select product source by exact card; do not wholesale port. |
| broker/src/bpay-next-frozen-rpc.js | C6 | 26debcbc / absent / absent | no | F-09 (referenced feature successor later) | New local; select product source by exact card; do not wholesale port. |
| broker/src/bpay-next-history.js | C6 | fe9f5ab4 / absent / absent | no | F-09 (referenced feature successor later) | New local; select product source by exact card; do not wholesale port. |
| broker/src/bpay-next-job-rpc.js | C6 | 2d31f6d5 / absent / absent | no | F-09 (referenced feature successor later) | New local; select product source by exact card; do not wholesale port. |
| broker/src/bpay-next-job-step.js | C6 | 030ba698 / absent / absent | no | F-09 (referenced feature successor later) | New local; select product source by exact card; do not wholesale port. |
| broker/src/bpay-next-legacy-email.js | C6 | afe4e3da / absent / absent | no | F-09 (referenced feature successor later) | New local; select product source by exact card; do not wholesale port. |
| broker/src/bpay-next-legacy-fence.js | C6 | 5e4412ba / absent / absent | no | F-09 (referenced feature successor later) | New local; select product source by exact card; do not wholesale port. |
| broker/src/bpay-next-preparation-expiry-rpc.js | C6 | cfa18a56 / absent / absent | no | F-09 (referenced feature successor later) | New local; select product source by exact card; do not wholesale port. |
| broker/src/bpay-next-preparation-rpc.js | C6 | f24e3ecd / absent / absent | no | F-09 (referenced feature successor later) | New local; select product source by exact card; do not wholesale port. |
| broker/src/bpay-next-preparation.js | C5 | cc57cdf3 / absent / e35a1dd9 | no | F-09 | Both added; manual local delta onto fixed upstream. |
| broker/src/bpay-next-runtime-rpc.js | C6 | 52fa7589 / absent / absent | no | F-09 (referenced feature successor later) | New local; select product source by exact card; do not wholesale port. |
| broker/src/weekly-source/next-paid-evidence.mjs | C1 | f8cd97d9 / absent / f8cd97d9 | no | none (already upstream) | Already published; keep fixed upstream. |
| broker/src/weekly-source/protected-component-identity.mjs | C1 | fcd34784 / absent / fcd34784 | no | none (already upstream) | Already published; keep fixed upstream. |
| broker/src/weekly-source/protected-next-preview.mjs | C6 | af510853 / absent / absent | no | F-14 | New local; select product source by exact card; do not wholesale port. |
| docs/SOURCE_ATTENTION_CONTACT_FINITE_HANDOFF_20261005.md | C6 | 6d76b633 / absent / absent | no | F-08 reference triage | Finite handoff evidence; retain as reference unless exact card requires durable documentation. |
| docs/SOURCE_AUTHORISE_SCOPE_FINITE_HANDOFF_20261005.md | C6 | 74e75a07 / absent / absent | no | F-08 reference triage | Finite handoff evidence; retain as reference unless exact card requires durable documentation. |
| scripts/cloudtms-managed-execution-policy.mjs | C2 | 81c14816 / absent / e63448dc | 420513c6 | F-03/F-08 keep upstream only | Stale reference bytes match historical upstream; keep fixed upstream. |
| scripts/cloudtms-managed-psql.mjs | C1 | a8c98322 / absent / a8c98322 | no | none (already upstream) | Already published; keep fixed upstream. |
| scripts/cloudtms-managed-release-sql.mjs | C1 | aac853ed / absent / aac853ed | no | none (already upstream) | Already published; keep fixed upstream. |
| scripts/cloudtms-managed-release.mjs | C1 | 7afdb030 / absent / 7afdb030 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/migrations/03102026_1324_banking_pay_next_core.sql | C1 | 7f171a92 / absent / 7f171a92 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/migrations/04102026_0300_banking_pay_next_case_instruction_core.sql | C1 | 57da5914 / absent / 57da5914 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/migrations/04102026_0320_banking_pay_next_case_create_request.sql | C1 | d92b1831 / absent / d92b1831 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/migrations/04102026_0340_banking_pay_next_protected_source_receipt.sql | C1 | e5daf9a9 / absent / e5daf9a9 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/migrations/04102026_0350_banking_pay_next_reissue_cancel_request.sql | C1 | c80a504c / absent / c80a504c | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/migrations/04102026_0370_banking_pay_next_case_prepare_binding.sql | C1 | 58fb15d6 / absent / 58fb15d6 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/migrations/04102026_0380_banking_pay_next_case_net_binding.sql | C1 | 9b94f3f2 / absent / 9b94f3f2 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/migrations/04102026_0400_banking_pay_next_case_transfer_binding.sql | C1 | 841615b1 / absent / 841615b1 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/migrations/04102026_0440_banking_pay_next_non_earnings_return_week_binding.sql | C1 | efc66b21 / absent / efc66b21 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/migrations/04102026_0450_banking_pay_next_case_cancel_binding.sql | C1 | 1d83eb5a / absent / 1d83eb5a | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/migrations/04102026_0470_banking_pay_next_source_current_selector.sql | C1 | d012653e / absent / d012653e | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/migrations/04102026_0480_banking_pay_next_internal_zero_execution.sql | C1 | eadc3df8 / absent / eadc3df8 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/migrations/04102026_0500_banking_pay_next_source_current_hooks.sql | C1 | b2818cd7 / absent / b2818cd7 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/migrations/04102026_0501_banking_pay_next_preparation_expiry.sql | C1 | aab7c556 / absent / aab7c556 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/migrations/04102026_0502_banking_pay_next_queue_recovery.sql | C1 | b09c6c4d / absent / b09c6c4d | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/migrations/04102026_0503_banking_pay_next_expiry_sweep_cursor.sql | C1 | f7bb63ef / absent / f7bb63ef | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/migrations/04102026_0504_banking_pay_next_factual_email_claim_index.sql | C1 | 53f6d885 / absent / 53f6d885 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/migrations/04102026_1134_bpay_next_ordinary_day_shift_facts.sql | C1 | ba2b6275 / absent / ba2b6275 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/migrations/04102026_1241_bpay_next_component_selection.sql | C1 | be14def2 / absent / be14def2 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/migrations/04102026_1242_banking_pay_next_current_case_reader_index.sql | C1 | f4608670 / absent / f4608670 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/migrations/04102026_1257_weekly_source_local_protected_decision_receipts_v1.sql | C1 | 6a3fe683 / absent / 6a3fe683 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/migrations/04102026_1519_banking_pay_next_work_collection.sql | C1 | 3b9178ae / absent / 3b9178ae | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/migrations/04102026_1637_bpay_next_work_collection_recovery.sql | C1 | 00d9b2a3 / absent / 00d9b2a3 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/migrations/04102026_1647_banking_pay_next_work_collection_write_off.sql | C1 | fca25cee / absent / fca25cee | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/migrations/04102026_1827_banking_pay_next_candidate_destination_group.sql | C1 | 0e39e18d / absent / 0e39e18d | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/migrations/04102026_2011_banking_pay_next_split_week_basis.sql | C1 | 1e954003 / absent / 1e954003 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/migrations/04102026_2025_banking_pay_next_destination_lifecycle.sql | C1 | a2f2122d / absent / a2f2122d | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/migrations/04102026_2049_banking_pay_next_current_stored_credit_index.sql | C1 | 9d83940b / absent / 9d83940b | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/migrations/04102026_2106_banking_pay_next_source_inventory_digest.sql | C1 | bf2c7aa6 / absent / bf2c7aa6 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/migrations/04102026_2133_banking_pay_next_destination_cancel_checkpoint.sql | C1 | 416dbfe3 / absent / 416dbfe3 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/migrations/04102026_2137_weekly_source_head_origin_evidence_v2.sql | C1 | 3b66c530 / absent / 3b66c530 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/migrations/04102026_2155_weekly_source_accepted_publication_actions_v1.sql | C1 | 76fa1e54 / absent / 76fa1e54 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/migrations/05102026_0044_bpay_next_run_work_query_pin_v2.sql | C1 | 3f2a1521 / absent / 3f2a1521 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/migrations/05102026_0155_weekly_source_local_publication_origin_v2.sql | C1 | ef4c5e2d / absent / ef4c5e2d | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/migrations/05102026_0355_weekly_source_local_flat_origin_cutover_v1.sql | C1 | a6ffb75d / absent / a6ffb75d | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/migrations/05102026_0428_weekly_source_manual_review_commands.sql | C1 | 9bdd49cd / absent / 9bdd49cd | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/migrations/05102026_0432_weekly_source_later_change_receipt_index.sql | C1 | e0a6a61b / absent / e0a6a61b | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/migrations/06102026_0236_banking_pay_next_channel_page_index_v1.sql | C6 | ef34f2c7 / absent / absent | no | F-06 | New local; select product source by exact card; do not wholesale port. |
| supabase/migrations/06102026_1206_banking_pay_payment_history_scope_indexes_v1.sql | C6 | d07431fe / absent / absent | no | F-06 | New local; select product source by exact card; do not wholesale port. |
| supabase/migrations/06102026_1245_banking_pay_next_source_book_binding.sql | C6 | a0d67e24 / absent / absent | no | F-06 | New local; select product source by exact card; do not wholesale port. |
| supabase/migrations/07102026_0018_banking_pay_next_initial_tsfin_umbrella_work_shape_v1.sql | C6 | 956ac0bf / absent / absent | no | F-06 | New local; select product source by exact card; do not wholesale port. |
| supabase/repeatable/03102026_1342_banking_pay_next_guards_v1.sql | C1 | 77c9883a / absent / 77c9883a | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/03102026_1404_banking_pay_next_command_intake_v1.sql | C1 | a8d76ce3 / absent / a8d76ce3 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/03102026_1430_banking_pay_next_position_writer_v1.sql | C1 | 03639f5c / absent / 03639f5c | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/03102026_1450_banking_pay_next_producer_route_v1.sql | C1 | 7ba6bca1 / absent / 7ba6bca1 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/03102026_1500_banking_pay_next_target_valuation_v1.sql | C5 | 9b4c5571 / absent / 4cb4097c | no | F-05 | Both added; manual local delta onto fixed upstream. |
| supabase/repeatable/03102026_1510_banking_pay_next_rate_rows_v1.sql | C1 | f0bdeb11 / absent / f0bdeb11 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/03102026_1530_banking_pay_next_ordinary_approval_v1.sql | C5 | 7abd0dc3 / absent / b2ade617 | no | F-05 | Both added; manual local delta onto fixed upstream. |
| supabase/repeatable/03102026_1540_banking_pay_next_source_ordered_publish_v1.sql | C1 | 94a10baa / absent / 94a10baa | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/03102026_1540_candidate_approved_hours_visible.sql | C1 | a0aee745 / absent / a0aee745 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/03102026_1548_banking_pay_next_source_chosen_detail_v1.sql | C1 | c2e4bb43 / absent / c2e4bb43 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/03102026_1550_banking_pay_next_source_stage_v1.sql | C1 | ddd18a41 / absent / ddd18a41 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/03102026_1560_banking_pay_next_source_first_owner_hook_v1.sql | C1 | 13b23518 / absent / 13b23518 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/03102026_1570_banking_pay_next_source_receipt_hook_v1.sql | C1 | 4fd6402a / absent / 4fd6402a | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/03102026_1580_banking_pay_next_source_withdrawal_guard_v1.sql | C1 | 0ce641fd / absent / 0ce641fd | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/03102026_1590_banking_pay_next_selection_manifest_v1.sql | C1 | c73b9529 / absent / c73b9529 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/03102026_1600_banking_pay_next_prepare_pin_v1.sql | C5 | 6b1f8432 / absent / d79cc1c1 | no | F-05 | Both added; manual local delta onto fixed upstream. |
| supabase/repeatable/03102026_1610_banking_pay_next_position_capture_v1.sql | C5 | df8e979f / absent / 388b0b1b | no | F-05 | Both added; manual local delta onto fixed upstream. |
| supabase/repeatable/03102026_1620_banking_pay_next_fresh_positive_hold_v1.sql | C5 | 56e408f5 / absent / c594fe64 | no | F-05 | Both added; manual local delta onto fixed upstream. |
| supabase/repeatable/03102026_1630_banking_pay_next_no_case_completion_v1.sql | C1 | 08054347 / absent / 08054347 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/03102026_2359_banking_pay_next_simple_review_confirm_v1.sql | C5 | 0b87b376 / absent / 857546f0 | no | F-05 | Both added; manual local delta onto fixed upstream. |
| supabase/repeatable/04102026_0025_banking_pay_next_frozen_detail_pages_v1.sql | C1 | 8d5d1298 / absent / 8d5d1298 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/04102026_0100_banking_pay_next_paye_net_intake_v1.sql | C1 | 327ada05 / absent / 327ada05 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/04102026_0110_banking_pay_next_simple_paye_projection_v1.sql | C1 | 3ee0e4c5 / absent / 3ee0e4c5 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/04102026_0120_banking_pay_next_simple_transfer_begin_v1.sql | C5 | b6f15e8e / absent / b0012508 | no | F-05 | Both added; manual local delta onto fixed upstream. |
| supabase/repeatable/04102026_0130_banking_pay_next_simple_transfer_members_v1.sql | C5 | 5cbde820 / absent / 410e2e24 | no | F-05 | Both added; manual local delta onto fixed upstream. |
| supabase/repeatable/04102026_0140_banking_pay_next_simple_csv_destination_v1.sql | C5 | 60ad65b4 / absent / 752a908a | no | F-05 | Both added; manual local delta onto fixed upstream. |
| supabase/repeatable/04102026_0145_banking_pay_next_transfer_guards_v1.sql | C1 | f4d07cfd / absent / f4d07cfd | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/04102026_0150_banking_pay_next_simple_csv_issue_v1.sql | C5 | c15aec88 / absent / 81329331 | no | F-05 | Both added; manual local delta onto fixed upstream. |
| supabase/repeatable/04102026_0200_banking_pay_next_simple_csv_settlement_v1.sql | C5 | 0ad1c427 / absent / 4a1be1aa | no | F-05 | Both added; manual local delta onto fixed upstream. |
| supabase/repeatable/04102026_0230_banking_pay_next_simple_csv_return_v1.sql | C1 | 3a085c13 / absent / 3a085c13 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/04102026_0240_banking_pay_next_simple_cash_reissue_v1.sql | C5 | 39badee5 / absent / 39220ab8 | no | F-05 | Both added; manual local delta onto fixed upstream. |
| supabase/repeatable/04102026_0250_banking_pay_next_frozen_detail_service_v1.sql | C1 | c7f92213 / absent / c7f92213 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/04102026_0255_banking_pay_next_original_csv_advice_page_v1.sql | C5 | 4fbff637 / absent / e53c17ca | no | F-05 | Both added; manual local delta onto fixed upstream. |
| supabase/repeatable/04102026_0260_banking_pay_next_simple_cancel_v1.sql | C5 | b71207b8 / absent / afdef19a | no | F-05 | Both added; manual local delta onto fixed upstream. |
| supabase/repeatable/04102026_0270_banking_pay_next_financial_intake_service_v1.sql | C5 | 279ffc12 / absent / 5c3a4dc2 | no | F-05 | Both added; manual local delta onto fixed upstream. |
| supabase/repeatable/04102026_0280_banking_pay_next_financial_job_service_v1.sql | C1 | 9434bf74 / absent / 9434bf74 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/04102026_0290_banking_pay_next_case_allocation_row_v1.sql | C1 | 9701eaa5 / absent / 9701eaa5 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/04102026_0310_banking_pay_next_case_guards_v1.sql | C1 | c25f55be / absent / c25f55be | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/04102026_0330_banking_pay_next_case_creation_selection_v1.sql | C1 | 987bb5ab / absent / 987bb5ab | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/04102026_0345_banking_pay_next_protected_source_v1.sql | C1 | 08f8a6d5 / absent / 08f8a6d5 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/04102026_0355_banking_pay_next_reissue_cancel_v1.sql | C1 | ebc4e9c9 / absent / ebc4e9c9 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/04102026_0360_banking_pay_next_case_preparation_v1.sql | C1 | bfd89c58 / absent / bfd89c58 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/04102026_0390_banking_pay_next_case_paye_net_v1.sql | C1 | 4414b495 / absent / 4414b495 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/04102026_0410_banking_pay_next_case_transfer_post_v1.sql | C1 | f8d35047 / absent / f8d35047 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/04102026_0420_banking_pay_next_no_case_week_fact_v1.sql | C1 | 8eca740b / absent / 8eca740b | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/04102026_0430_banking_pay_next_week_lifecycle_v1.sql | C5 | 5f8ac08a / absent / 737a53c3 | no | F-05 | Both added; manual local delta onto fixed upstream. |
| supabase/repeatable/04102026_0460_banking_pay_next_case_cancel_v1.sql | C1 | 6848494b / absent / 6848494b | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/04102026_0475_banking_pay_next_source_current_selector_v1.sql | C1 | 3073d7ab / absent / 3073d7ab | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/04102026_0490_banking_pay_next_internal_zero_settlement_v1.sql | C1 | b2c07411 / absent / b2c07411 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/04102026_0505_banking_pay_next_source_current_hooks_v1.sql | C1 | eba9d0ce / absent / eba9d0ce | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/04102026_0510_banking_pay_next_preparation_expiry_v1.sql | C1 | 34f55809 / absent / 34f55809 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/04102026_0520_banking_pay_next_preparation_expiry_service_v1.sql | C1 | f1453937 / absent / f1453937 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/04102026_0550_banking_pay_next_legacy_writer_fence_v1.sql | C1 | 3acf0608 / absent / 3acf0608 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/04102026_0555_banking_pay_next_factual_legacy_projection_boundary_v1.sql | C1 | a5415e1f / absent / a5415e1f | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/04102026_0556_banking_pay_next_source_factual_legacy_boundary_v1.sql | C1 | a6ef06a6 / absent / a6ef06a6 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/04102026_0557_banking_pay_next_source_owner_completion_v1.sql | C5 | f40be7e8 / absent / 8366ba06 | no | F-05 | Both added; manual local delta onto fixed upstream. |
| supabase/repeatable/04102026_0558_banking_pay_next_legacy_email_claim_boundary_v1.sql | C1 | b824ad18 / absent / b824ad18 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/04102026_0560_banking_pay_next_queue_recovery_service_v1.sql | C1 | ef6b9e98 / absent / ef6b9e98 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/04102026_0570_banking_pay_next_component_selection_v1.sql | C1 | 3ed7b257 / absent / 3ed7b257 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/04102026_0575_banking_pay_next_preparation_service_v1.sql | C5 | eff9a48d / absent / 75d4eb40 | no | F-05 | Both added; manual local delta onto fixed upstream. |
| supabase/repeatable/04102026_0580_banking_pay_next_office_status_v1.sql | C1 | 80f2acfc / absent / 80f2acfc | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/04102026_0585_banking_pay_next_current_case_pages_v1.sql | C1 | a0a9977f / absent / a0a9977f | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/04102026_1134_bpay_next_frozen_shift_page_v2.sql | C1 | af591e27 / absent / af591e27 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/04102026_1257_weekly_source_local_protected_decision_v1.sql | C1 | 1e7eea06 / absent / 1e7eea06 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/04102026_1519_banking_pay_next_work_collection_v1.sql | C5 | f0b1566b / absent / a94d7560 | no | F-05 | Both added; manual local delta onto fixed upstream. |
| supabase/repeatable/04102026_1647_banking_pay_next_work_collection_write_off_v1.sql | C1 | d321cf31 / absent / d321cf31 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/04102026_1827_banking_pay_next_candidate_destination_group_v1.sql | C1 | 446c8adc / absent / 446c8adc | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/04102026_2025_banking_pay_next_destination_execution_v1.sql | C1 | 6958670c / absent / 6958670c | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/04102026_2049_banking_pay_next_current_stored_credit_page_v1.sql | C1 | ad56a5f6 / absent / ad56a5f6 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/04102026_2102_banking_pay_next_destination_cancel_v1.sql | C1 | 809f0e8e / absent / 809f0e8e | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/04102026_2106_banking_pay_next_source_origin_state_v1.sql | C1 | f62dc16e / absent / f62dc16e | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/04102026_2120_weekly_source_approval_basis_v2.sql | C1 | 70b368b0 / absent / 70b368b0 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/04102026_2206_weekly_source_accepted_publication_evidence_v1.sql | C1 | f447b50c / absent / f447b50c | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/04102026_2253_weekly_source_candidate_initial_approval_v2.sql | C1 | f553c931 / absent / f553c931 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/04102026_2307_weekly_source_retain_approved_additional_v2.sql | C1 | e61c8dee / absent / e61c8dee | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/04102026_2317_weekly_source_candidate_head_hours_v2.sql | C1 | a3b8450f / absent / a3b8450f | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/04102026_2338_weekly_source_candidate_saved_local_hours_v2.sql | C1 | 6283bdfe / absent / 6283bdfe | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/05102026_0001_banking_pay_next_return_cash_status_v1.sql | C1 | f7be6252 / absent / f7be6252 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/05102026_0044_bpay_next_source_pay_query_admission_v2.sql | C1 | f14c796c / absent / f14c796c | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/05102026_0057_weekly_source_pay_query_scope_v1.sql | C1 | 9efa05d2 / absent / 9efa05d2 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/05102026_0100_weekly_source_candidate_approved_clock_row_v2.sql | C1 | 504bd0cc / absent / 504bd0cc | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/05102026_0114_weekly_source_pay_query_evidence_v1.sql | C1 | ce7b63c7 / absent / ce7b63c7 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/05102026_0155_weekly_source_local_publication_origin_guard_v2.sql | C1 | a3a28d42 / absent / a3a28d42 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/05102026_0201_weekly_source_local_origin_canonical_v2.sql | C1 | a3727e0f / absent / a3727e0f | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/05102026_0212_weekly_source_local_publication_context_v2.sql | C1 | 77f6ba64 / absent / 77f6ba64 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/05102026_0224_weekly_source_local_entitlement_proposal_v2.sql | C1 | ed8d1c11 / absent / ed8d1c11 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/05102026_0245_weekly_source_pay_query_c1_receipt_v1.sql | C1 | 313e6677 / absent / 313e6677 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/05102026_0258_weekly_source_pay_query_next_receipt_v1.sql | C1 | 4051602d / absent / 4051602d | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/05102026_0302_banking_pay_next_local_source_chosen_detail_v1.sql | C1 | 5d0b2a18 / absent / 5d0b2a18 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/05102026_0308_weekly_source_pay_query_gate_v1.sql | C1 | a156d788 / absent / a156d788 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/05102026_0428_weekly_source_manual_review_commands_v2.sql | C1 | 9f82257a / absent / 9f82257a | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/05102026_0444_weekly_source_protected_saved_prepare_route_v1.sql | C1 | 66126142 / absent / 66126142 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/05102026_0512_weekly_source_operational_category_v2.sql | C1 | 8f9c5cb5 / absent / 8f9c5cb5 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/05102026_0559_weekly_source_office_category_batch_v1.sql | C1 | c93a7947 / absent / c93a7947 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/05102026_0624_weekly_source_local_saved_status_v1.sql | C1 | 4891f534 / absent / 4891f534 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/05102026_0645_weekly_source_selected_source_witness_v1.sql | C1 | 9453c32b / absent / 9453c32b | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/05102026_0650_weekly_source_accepted_removal_query_evidence_v1.sql | C1 | 409e6a79 / absent / 409e6a79 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/05102026_0659_bpay_next_protected_owner_route_v1.sql | C1 | d3b984b9 / absent / d3b984b9 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/05102026_0710_weekly_source_invoice_duty_v1.sql | C1 | d1a4edfc / absent / d1a4edfc | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/05102026_0740_weekly_source_approval_duty_v1.sql | C1 | 49e6fcf8 / absent / 49e6fcf8 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/05102026_0834_weekly_source_protected_contact_retirement_v1.sql | C1 | 6cf8e04d / absent / 6cf8e04d | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/05102026_1112_bpay_next_source_paid_evidence_page_v1.sql | C1 | 7de3a315 / absent / 7de3a315 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/05102026_1210_weekly_source_office_authorise_scope_v1.sql | C1 | bc49825f / absent / bc49825f | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/05102026_1242_weekly_source_office_next_paid_context_v1.sql | C1 | b85f28a8 / absent / b85f28a8 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/05102026_1417_weekly_source_coverage_support/qualified_coverage.inc | C1 | 47f16565 / absent / 47f16565 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/repeatable/06102026_0236_banking_pay_next_channel_draft_readers_v1.sql | C6 | 0fb41b8a / absent / absent | no | F-06 | New local; select product source by exact card; do not wholesale port. |
| supabase/repeatable/06102026_1157_banking_pay_shared_selected_quote_v1.sql | C6 | 417e143c / absent / absent | no | F-04/F-06 | New local; select product source by exact card; do not wholesale port. |
| supabase/repeatable/06102026_1206_banking_pay_payment_history_page_v1.sql | C6 | e858fced / absent / absent | no | F-06 | New local; select product source by exact card; do not wholesale port. |
| supabase/repeatable/06102026_1246_banking_pay_next_source_book_binding_v1.sql | C6 | e3721c0b / absent / absent | no | F-06 | New local; select product source by exact card; do not wholesale port. |
| supabase/repeatable/06102026_1301_banking_pay_next_source_receipt_binding_v1.sql | C6 | 0b39115f / absent / absent | no | F-06 | New local; select product source by exact card; do not wholesale port. |
| supabase/repeatable/07102026_0018_banking_pay_next_initial_tsfin_daily_evidence_v1.sql | C6 | 3ef4d91d / absent / absent | no | F-06 | New local; select product source by exact card; do not wholesale port. |
| supabase/repeatable/08102026_0549_banking_pay_next_overpayment_week_applicability_v1.sql | C6 | bb76cb4e / absent / absent | no | F-07/CM-02a ordinary/comparison split | New local; select product source by exact card; do not wholesale port. |
| supabase/repeatable/08102026_0718_banking_pay_next_frozen_run_read_safety_v1.sql | C6 | 07b1c0db / absent / absent | no | F-06 | New local; select product source by exact card; do not wholesale port. |
| supabase/repeatable/includes/05102026_0659_bpay_next_protected_owner_route_v1.sqlinc | C1 | 4523c1a8 / absent / 4523c1a8 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/verification/04102026_1257_weekly_source_local_protected_decision_v1.sql | C1 | 091915e5 / absent / 091915e5 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/verification/04102026_2206_weekly_source_accepted_publication_evidence_v1.sql | C1 | 1fc170d4 / absent / 1fc170d4 | no | none (already upstream) | Already published; keep fixed upstream. |
| supabase/verification/05102026_0552_weekly_source_invoice_isolation_fixture_v1.sql | C2 | 4f97cb56 / absent / 2d95419f | 420513c6 | F-03/F-08 keep upstream only | Stale reference bytes match historical upstream; keep fixed upstream. |
| tests/BPAY_NEXT_CASE_KERNEL_NEXT_SLICE.md | C6 | f9dd2da6 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/BPAY_NEXT_PROTECTED_SOURCE_NEXT_SLICE.md | C6 | 0b03001f / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/BPAY_NEXT_QUERY_PIN_V2_INTERFACE.md | C6 | 8e00205d / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/banking-pay-shared-selected-quote.test.cjs | C6 | 2497a1c4 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-authorisation-point-owner.test.mjs | C6 | 51cc93f3 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-background.test.js | C6 | 23fca059 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-command-rpc.test.js | C6 | 0f901876 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-command.test.js | C6 | 22b41ce5 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-current-case-page.test.js | C6 | 1bb904c6 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-current-stored-credit-window.test.js | C6 | 2e3c548c / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-current-stored-credit.test.js | C6 | a8f3a05e / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-destination-cancel.test.js | C6 | b483ac4a / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-destination-execution.test.js | C6 | d75292d5 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-destination-group.test.js | C6 | ef9949ee / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-destination-return-advice.test.js | C6 | d6901e9c / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-factual-legacy-projection-boundary.test.cjs | C6 | fabdc0b3 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-financial-queue.test.js | C6 | 3c42d368 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-frozen-advice.test.js | C6 | 29a46abc / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-frozen-entry-method.test.js | C6 | 6eb1960f / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-frozen-read.test.js | C6 | 01821b5f / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-frozen-rpc.test.js | C6 | 76e51c12 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-general-browser-census-successor.test.cjs | C6 | 0471ed4d / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-job-rpc.test.js | C6 | 1b9552fc / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-job-step.test.js | C6 | 82b99ede / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-legacy-email.test.js | C6 | 31616805 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-legacy-fence.test.js | C6 | d3f3411b / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-legacy-mail-entry.test.js | C6 | 798f242c / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-local-completion-order.test.cjs | C6 | 44429e70 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-local-contact-completion-fixture.test.cjs | C6 | 3fa5434f / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-local-draft-cancel-fixture.test.cjs | C6 | 36590175 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-local-draft-cancel-v2-fixture.test.cjs | C6 | d5669785 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-local-owner-admission-fixture-v1.test.cjs | C6 | de83186d / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-local-proposal-recorder.test.cjs | C6 | f9986731 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-local-publication-canonical.test.cjs | C6 | ad4f3306 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-local-saved-prepare-integration.test.cjs | C6 | f6919e02 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-local-source-chosen-detail.test.cjs | C6 | 182ff368 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-local-staged-context.test.cjs | C6 | 6d3e5180 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-p12-p13-critical-schema-census.mjs | C6 | cef750e8 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-p12-p13-financial-native.test.cjs | C6 | 8d3c4899 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-p12-p13-financial-vector.test.cjs | C6 | 7b2fe759 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-p12-p13-install-only.test.cjs | C6 | cb488339 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-p12-p13-prerequisite-census.mjs | C6 | b0dcdafe / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-p12-p13-prerequisite-census.test.cjs | C6 | 71ec2162 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-p13-frozen-advice-presentation.test.js | C6 | 3e50d378 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-pay-query-pin.test.js | C6 | ad62ebd8 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-payment-history-caller.test.js | C6 | 33f91bc6 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-payment-history.test.js | C6 | 7affa95a / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-preparation-expiry-boundary.test.js | C6 | 4a308735 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-preparation-expiry-rpc.test.js | C6 | a7b700f1 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-preparation-rpc.test.js | C6 | 08f19b60 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-preparation.test.js | C6 | 01785fdc / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-protected-local-exclusive-entry.test.cjs | C6 | 9621e9ff / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-q09-two-run-fixture.test.cjs | C6 | c38e42aa / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-q09-two-run-v2-fixture.test.cjs | C6 | c9cc6af3 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-q09-two-run-v3-fixture.test.cjs | C6 | 4e86a822 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-query-pin-v2.test.js | C6 | edbfd0d4 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-release8-resume-owner-local.test.mjs | C6 | 3dae0b1a / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-release8-source-charge-contract-local.test.mjs | C6 | a3491fcb / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-retired-import-correction-route.test.js | C6 | 114d0e72 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-return-cash.test.js | C6 | 8ff3aeef / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-review-replay.test.js | C6 | 721dd947 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-route1450-point-controls.test.cjs | C6 | eaa97add / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-runtime-rpc.test.js | C6 | fec2852d / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-shared-target-valuation.test.js | C6 | 6c19b3d0 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-source-accepted-app-integration.test.cjs | C6 | 0ccbd931 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-source-accepted-app-owner-integration.test.cjs | C6 | 10dc6bc2 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-source-app-context-query-admission.test.cjs | C6 | ff604b39 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-source-approved-basis-app-joined-fixture.test.cjs | C6 | 10054738 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-source-approved-basis-insert-atomic-failure-fixture.test.cjs | C6 | 1bddd4b5 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-source-approved-basis-integration.test.cjs | C6 | c7a2ab75 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-source-book-binding.test.cjs | C6 | fe2f5bae / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-source-chosen-utc.test.js | C6 | f620045b / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-source-current-selector-model.test.js | C6 | 8c240652 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-source-factual-legacy-boundary.test.cjs | C6 | 951d1a05 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-source-final-query-admission.test.cjs | C6 | 1b4a2ec0 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-source-fractional-snapshot-hash.test.cjs | C6 | f30f9153 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-source-group-a-join.test.cjs | C6 | f8c46f93 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-source-group-b-integration.test.cjs | C6 | 021279ea / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-source-head-origin-capture.test.js | C6 | 6edff42d / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-source-joined-general-catalogue-proof-v1.mjs | C6 | 9edb0d3a / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-source-later-change-receipt-index.test.cjs | C6 | 1b0975f5 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-source-local-owner-integration.test.cjs | C6 | 520456e3 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-source-manual-final-hooks.test.cjs | C6 | 44ed21dc / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-source-manual-query-correct-v2.test.cjs | C6 | d77bbf8b / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-source-office-detail-census.mjs | C6 | d498a499 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-source-office-detail-census.test.cjs | C6 | f5c0fad5 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-source-office-detail-native-v2.test.cjs | C6 | a796afdd / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-source-office-detail-native.test.cjs | C6 | 565a3e1a / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-source-office-detail-prefix.test.cjs | C6 | 7b0a5dee / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-source-origin-state.test.js | C6 | 8b4b470c / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-source-owner-completion.test.cjs | C6 | d062d00b / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-source-paid-evidence-normal-native-v2.test.cjs | C6 | 1f23b5e9 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-source-paid-evidence-normal-native-v3.test.cjs | C6 | 00a9cc98 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-source-paid-evidence-normal-native.test.cjs | C6 | 6e941f8b / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-source-paid-evidence.test.cjs | C6 | 63f999a7 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-source-pair-before-caller.test.cjs | C6 | deeca5b9 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-source-pair-before-detail.test.cjs | C6 | d7304c15 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-source-publication-query-admission.test.cjs | C6 | aac831f0 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-source-query-writer-admission.test.cjs | C6 | 0964ace4 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-source-receipt-binding.test.cjs | C6 | 41f8bfc1 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-source-removal-finite-integration.test.cjs | C6 | 75051f56 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-source-selected-recovery.test.js | C6 | cc9058c0 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-source-work-collection.test.js | C6 | beaba2c1 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-work-collection-expiry.test.js | C6 | 92a7a590 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-work-collection-recovery.test.js | C6 | 98d19d37 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-work-collection.test.js | C6 | b3dee7d3 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-worker-entry.test.js | C6 | fbc24582 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/bpay-next-write-off.test.js | C6 | dd247b5e / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/candidate-approved-hours-display.test.js | C1 | 55f0b485 / absent / 55f0b485 | no | none (already upstream) | Already published; keep fixed upstream. |
| tests/diagnose-bpay-next-source-manual-query-correct-compile.mjs | C6 | 0b7595e1 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/dynamic-logical-postgres-owner.test.mjs | C6 | d0b48bc0 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/fixtures/04102026_2205_l22_cancel_committed_check_fence.sql | C8 | d45e764f / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/06102026_1118_bpay_next_shared_target_valuation_differential.sql | C8 | c429b74e / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/06102026_1157_banking_pay_shared_selected_quote_differential.sql | C8 | cf5cb2ec / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/06102026_1206_bpay_next_payment_history_reader_verification.sql | C8 | 31ce1c4d / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/06102026_1246_banking_pay_next_source_book_binding_native.sql | C8 | 197fdb6d / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-authorisation-point-flags-real.sql | C8 | c9723789 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-background-api-setup-real.sql | C8 | a624fcab / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-bulk-contention-local.sql | C8 | 11105e32 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-cancel-local-schema-upgrade.sql | C8 | 307b6d79 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-case-allocation-kernel.sql | C8 | cc13bf8f / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-case-api-setup-real.sql | C8 | e9630781 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-case-cancel-real.sql | C8 | 0f78bd6a / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-case-creation-selection-real.sql | C8 | c6ce1888 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-case-guards.sql | C8 | 81319ec8 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-case-instruction-core.sql | C8 | cefad47e / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-case-paye-net-real.sql | C8 | 00a0688c / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-case-preparation-real.sql | C8 | 8440bb01 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-case-transfer-post-real.sql | C8 | 42581c0d / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-command-concurrent-a.sql | C8 | b4bd0f67 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-command-concurrent-b.sql | C8 | b3130e83 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-command-race-call.sql | C8 | dcfc3421 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-command-race-holder.sql | C8 | ba68a35b / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-command-race-setup-real.sql | C8 | e1842765 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-command-receipt.sql | C8 | 892e1395 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-command-service-real.sql | C8 | bd05acec / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-component-capture-real.sql | C8 | 3c727777 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-component-selection-real.sql | C8 | 8a8aa596 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-contract-rate-rows.sql | C8 | 62b3ec65 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-core-identity.sql | C8 | 81878c26 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-csv-config-matrix-real.sql | C8 | 557c2b38 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-current-stored-credit-real.sql | C8 | c5677e92 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-current-stored-credit-window-real.sql | C8 | 9ede667d / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-current-stored-credit-window-v2-real.sql | C8 | 977f74b1 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-destination-api-setup-real.sql | C8 | 3b31381b / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-destination-cancel-real.sql | C8 | 19712ec8 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-destination-execution-real.sql | C8 | c6c48eaf / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-destination-group-real.sql | C8 | 68ac9527 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-destination-return-advice-real.sql | C8 | c4d57420 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-empty-reference-stubs.sql | C8 | eb7433ef / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-expiry-sweep-recovery-real.sql | C8 | 55434898 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-factual-email-claim.sql | C8 | 7ae44dc8 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-factual-legacy-projection-boundary.sql | C8 | 4fe2840d / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-financial-job-boundary-real.sql | C8 | 6e89a668 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-first300-real.mjs | C8 | e9753690 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-four-owner-supplied-source-stage-20261005.sql | C8 | 332f768b / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-frozen-byte-page-real.sql | C8 | f1800284 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-frozen-detail-service-real.sql | C8 | 68b717bb / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-frozen-rate-detail.sql | C8 | 078c0051 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-independent-candidate.sql | C8 | 3159db58 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-internal-zero-api-setup-real.sql | C8 | 36034dc1 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-internal-zero-real.sql | C8 | 9dfc306d / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-job-identity.sql | C8 | 8caaab67 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-legacy-writer-fence-real.sql | C8 | 3d20dd1b / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-local-contact-completion-pins.json | C8 | 0b2693e8 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-local-contact-completion-real.sql | C8 | e1a4a472 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-local-draft-cancel-pins.json | C8 | bdbf9ec3 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-local-draft-cancel-real.sql | C8 | 3f09e371 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-local-draft-cancel-v2-pins.json | C8 | 551cd1f8 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-local-owner-admission-pins-v1.json | C8 | cd8f4141 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-magnit-import-expense-real.mjs | C8 | ba190faa / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-no-case-week-real.sql | C8 | 09b17d62 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-office-keyset-plan-real.sql | C8 | 98d9048a / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-office-net-status-assertions.sql | C8 | 3e7f06d5 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-office-p01-api-setup-real.sql | C8 | 5848c743 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-office-status-real.sql | C8 | c982a36d / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-ordinary-approval-trigger.sql | C8 | 0cde2805 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-ordinary-current-generic-caller.sql | C8 | 399be232 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-ordinary-day-continuity-real.sql | C8 | 32617e16 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-ordinary-paid-archive-real-owner.sql | C8 | 0cf38e78 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-ordinary-real-owner.sql | C8 | fd126673 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-ordinary-structural-shape.sql | C8 | 7c8e93ee / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-ordinary-version-archive-real-owner.sql | C8 | c717b094 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-p12-p13-financial-vector-compose.mjs | C8 | 2dca5f77 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-p12-p13-financial-vector-real.sql | C8 | 585d8d77 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-p12-p13-install-only-pins.json | C8 | 00796824 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-policy-version.sql | C8 | e4ae8647 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-position-order.sql | C8 | 476d681c / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-preparation-active-hold-residual-shape.sql | C8 | a437f1dd / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-preparation-api-setup-real.sql | C8 | 2e33b842 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-preparation-enrollment-first-commit.sql | C8 | 64590073 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-preparation-enrollment-real.sql | C8 | f39fee27 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-preparation-enrollment-recovered.sql | C8 | 512ad18c / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-preparation-expiry-api-setup.sql | C8 | fb6a36f9 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-preparation-expiry-real.sql | C8 | 3b62c78a / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-preparation-expiry-service-real.sql | C8 | 61c13c7f / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-preparation-fresh-holds-real.sql | C8 | 50d3a780 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-preparation-hold-first-commit.sql | C8 | c42e74bc / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-preparation-hold-recovered.sql | C8 | 8f889589 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-preparation-joined-api-setup-real.sql | C8 | 42432d3c / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-preparation-negative-realised-review.sql | C8 | 775bb4d1 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-preparation-no-case-ready-real.sql | C8 | 4df0646a / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-preparation-pending-review-real.sql | C8 | d206cacf / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-preparation-pin-first-commit.sql | C8 | a1bcf607 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-preparation-pin-real.sql | C8 | 147aac5a / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-preparation-pin-recovered.sql | C8 | 16c44db7 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-preparation-position-capture-edge.sql | C8 | 2908b8cf / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-preparation-position-capture-real.sql | C8 | f9bf7252 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-preparation-second-run-real-held-delta.sql | C8 | ba9a0094 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-preparation-signed-hold-refusal.sql | C8 | 1ab4d23a / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-preparation-source-basis-review.sql | C8 | fde4c933 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-producer-route.sql | C8 | 26047fe8 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-protected-source-import-real.sql | C8 | 2ab0430c / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-protected-source-real.sql | C8 | 3394f0da / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-protected-source-requirements.md | C8 | cd5bf3a4 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-q09-two-run-real.sql | C8 | c0423ffe / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-q09-two-run-v2-prerequisites.mjs | C8 | 76bdb0cc / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-q09-two-run-v3-prerequisites.mjs | C8 | 92d1bdd8 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-query-admission-contender.sql | C8 | edae1c61 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-query-admission-holder.sql | C8 | 99ffe5e3 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-query-admission-release-acl.sql | C8 | 1671ccaf / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-queue-recovery-service-real.sql | C8 | 95480316 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-reissue-cancel-real.sql | C8 | 40b7d132 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-return-cash-status-real.sql | C8 | 7e8cbba7 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-review-replay-real.sql | C8 | 16f395c5 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-revision-comparison-page-bounds.sql | C8 | b288171d / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-route1450-point-controls-real.sql | C8 | efa0604f / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-selection-manifest-real.sql | C8 | 0a2b4c2a / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-simple-cancel-real.sql | C8 | 040317b1 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-simple-cancel-recovered.sql | C8 | 4a41e7f7 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-simple-cancel-requirements.md | C8 | 72b3551b / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-simple-cash-reissue-real.sql | C8 | 45c5d53e / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-simple-csv-issue-real.sql | C8 | 42eb940a / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-simple-csv-return-real.sql | C8 | 0a4e8302 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-simple-csv-settlement-real.sql | C8 | fb528cee / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-simple-csv-settlement-recovered.sql | C8 | 4870a69d / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-simple-frozen-draft-real.sql | C8 | 0f617a99 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-simple-paye-net-intake-real.sql | C8 | 9e7c23d6 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-simple-paye-projection-real.sql | C8 | 7331e0ba / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-simple-transfer-members-real.sql | C8 | cff16d77 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-source-approved-basis-app-joined-real.sql | C8 | f4032a67 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-source-approved-basis-insert-atomic-failure-real.sql | C8 | 6bd96efe / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-source-certified-zero-preparation-real.sql | C8 | 33916859 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-source-chosen-detail.sql | C8 | 8925204f / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-source-chosen-utc-retention-unit.sql | C8 | 796577e7 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-source-current-hooks-real.sql | C8 | dda6c46a / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-source-current-selector-real.sql | C8 | 3dc67acb / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-source-factual-legacy-boundary.sql | C8 | 90da880e / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-source-first-real-owner.sql | C8 | 313865c6 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-source-fresh-holds-real.sql | C8 | 33b39345 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-source-head-real-owner.sql | C8 | 5c36025a / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-source-manual-query-correct-real.sql | C8 | 1b37670f / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-source-manual-query-correct-v2-real.sql | C8 | 49d15969 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-source-manual-query-normal-real.sql | C8 | c5154415 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-source-office-decision-real-owner.sql | C8 | e4fc37e2 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-source-office-detail-census-pins.json | C8 | 974c0458 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-source-office-detail-native-pins.json | C8 | 239810d7 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-source-office-detail-prefix.mjs | C8 | 9ba63355 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-source-ordered-pair.sql | C8 | f7f60bdf / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-source-origin-state-real.sql | C8 | d7d233cf / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-source-paid-archive-real-owner.sql | C8 | 24d9627a / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-source-paid-evidence-compose.mjs | C8 | dfa49043 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-source-paid-evidence-extension-real.sql | C8 | d434d3dd / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-source-paid-evidence-native-role-v2.sql | C8 | 72101ef7 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-source-paid-evidence-native-role.sql | C8 | 40a991e0 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-source-paid-journey-real.sql | C8 | 167a82d8 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-source-position-capture-real.sql | C8 | 0d9357ce / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-source-preparation-pin-real.sql | C8 | cd69ae0b / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-source-selected-recovery-real.sql | C8 | 693ef939 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-source-stage-stub.sql | C8 | f5a5805c / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-source-target-separation.sql | C8 | eb565ab2 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-source-withdraw-real-owner.sql | C8 | 1c0a2964 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-source-work-collection-real.sql | C8 | 046abd4c / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-target-valuation.sql | C8 | 9760fcb2 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-week-lifecycle-real.sql | C8 | 8f2e876f / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-work-collection-expiry-real.sql | C8 | 23d5763b / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-work-collection-real.sql | C8 | f895870b / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-work-collection-recovery-real.sql | C8 | fb652373 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-work-collection-write-off-real.sql | C8 | dc6fe937 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/helpers/bpay-next-source-pair-caller-delta.cjs | C6 | 8032123b / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/managed-database-release.test.mjs | C2 | b900a037 / absent / bed80bd4 | 420513c6 | F-03/F-08 keep upstream only | Stale reference bytes match historical upstream; keep fixed upstream. |
| tests/run-bpay-next-authorisation-point-flags-local.ps1 | C8 | 42ea4bb6 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-background-api-local.mjs | C8 | 44d3f923 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-background-api-local.ps1 | C8 | 28fb984a / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-cancel-local.ps1 | C8 | eb4154f5 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-case-api-local.mjs | C8 | d8f85911 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-case-cancel-local.ps1 | C8 | 07b5b907 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-case-cancel-race-local.mjs | C8 | 68c8c366 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-case-cancel-race-local.ps1 | C8 | 022a6891 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-case-creation-selection-local.ps1 | C8 | dda5eeba / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-case-net-local.ps1 | C8 | 01d23872 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-case-post-local.ps1 | C8 | fd21d183 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-case-preparation-local.ps1 | C8 | b3833f21 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-command-api-local.mjs | C8 | 2634b47f / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-command-api-local.ps1 | C8 | 9a22eb4e / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-command-local.ps1 | C8 | 492f0f00 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-command-race-setup-local.ps1 | C8 | a9e8e07f / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-component-capture-local.ps1 | C8 | f29ab86e / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-component-selection-local.ps1 | C8 | 45a31b59 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-csv-config-local.ps1 | C8 | 47741e10 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-csv-outcome-local.ps1 | C8 | 3bd2b6dc / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-current-new-owner-local-v2.mjs | C8 | 53faaa43 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-current-new-owner-local-v3.mjs | C8 | 4b24adce / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-current-new-owner-local-v4.mjs | C8 | 16afbf42 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-current-new-owner-local.mjs | C8 | dec16fb5 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-current-stored-credit-window-local.ps1 | C8 | 0cdd2432 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-current-stored-credit-window-v2-local.ps1 | C8 | 84d9acad / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-destination-api-local.mjs | C8 | f44f8c5d / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-destination-api-local.ps1 | C8 | 298a3c65 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-destination-cancel-committed-check-local.ps1 | C8 | 2160570c / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-destination-cancel-local.ps1 | C8 | b8694739 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-destination-execution-local.ps1 | C8 | ca1be664 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-destination-group-local.ps1 | C8 | 0d3880e1 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-destination-return-advice-local.ps1 | C8 | 3d93851a / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-expiry-sweep-local.ps1 | C8 | 581249db / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-first300-authorisation-probe-local.mjs | C8 | 01c5d22c / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-first300-local.ps1 | C8 | f0d3cd2c / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-frozen-advice-api-local.mjs | C8 | f3b01b10 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-frozen-api-local.mjs | C8 | 62f3b2e1 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-frozen-read-local.ps1 | C8 | 0bc2d53b / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-internal-zero-api-local.mjs | C8 | a59c8644 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-internal-zero-local.ps1 | C8 | e559ab89 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-issue-cancel-concurrent-local.mjs | C8 | 31343ab8 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-job-claim-concurrent-local.mjs | C8 | a6d8c7ba / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-local-contact-completion-native.mjs | C8 | 27a9d9cb / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-local-draft-cancel-native-v2.mjs | C8 | 4c9bd45d / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-local-draft-cancel-native.mjs | C8 | dad400f1 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-local-owner-admission-native-v1.mjs | C8 | ca6e6a74 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-no-case-week-local.ps1 | C8 | a1982a61 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-normal-source-query-admission-native.mjs | C8 | 21aa53b7 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-office-keyset-plan-local.ps1 | C8 | c9c9e5a1 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-office-net-status-local.ps1 | C8 | 9f4f291d / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-office-p01-api-local.mjs | C8 | cbdb228c / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-office-p01-api-local.ps1 | C8 | 8b684049 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-office-readers-advice-local.ps1 | C8 | d5e2a8d6 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-office-status-api-local.mjs | C8 | f62bab47 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-ordinary-day-local.ps1 | C8 | 8bfb5cdb / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-p12-p13-financial-native.mjs | C8 | b1e4b592 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-p12-p13-install-only.mjs | C8 | 411873ce / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-preparation-api-local.mjs | C8 | e0cb1be0 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-preparation-expiry-local.ps1 | C8 | 034a877c / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-preparation-expiry-recovery-local.mjs | C8 | d78dbdf1 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-preparation-expiry-recovery-local.ps1 | C8 | e52d03da / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-preparation-joined-api-local.mjs | C8 | 748ca822 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-preparation-joined-api-local.ps1 | C8 | cb02a31a / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-protected-source-import-local.ps1 | C8 | b216be5e / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-q09-prerequisites-local.mjs | C8 | 2b574096 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-q09-two-run-local.mjs | C8 | 54f5bfa6 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-q09-two-run-v2-local.mjs | C8 | e42bb85b / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-q09-two-run-v3-local.mjs | C8 | 1587b82b / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-q09-v3-prerequisites-local.mjs | C8 | e5865017 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-queue-recovery-local.ps1 | C8 | 993dc566 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-release8-resume-owner-local.mjs | C8 | 4ad72d17 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-release8-routine-catalogue-readonly.mjs | C8 | df13c4b4 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-release8-source-charge-contract-local.mjs | C8 | bd560bdf / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-return-cash-status-local.ps1 | C8 | 78b92112 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-review-replay-local.ps1 | C8 | 349f62d9 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-route1450-point-controls-local.ps1 | C8 | cd3167d4 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-source-current-hooks-local.ps1 | C8 | edf6e8ca / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-source-current-selector-local.ps1 | C8 | 255f8c16 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-source-manual-query-correct-native.mjs | C8 | 46df1300 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-source-manual-query-correct-v2-native.mjs | C8 | 58a5071f / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-source-manual-query-normal-native.mjs | C8 | 670a607d / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-source-office-detail-native-v2.mjs | C8 | 46c74ab4 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-source-office-detail-native.mjs | C8 | 8d29f8ca / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-source-origin-state-local.ps1 | C8 | 6bc0ad12 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-source-owner-portability-local.ps1 | C8 | 84d9abf1 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-source-paid-evidence-normal-native-v2.mjs | C8 | 6cb09800 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-source-paid-evidence-normal-native-v3.mjs | C8 | 3644702c / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-source-paid-evidence-normal-native.mjs | C8 | 477936fe / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-source-paid-local.ps1 | C8 | b668062b / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-source-selected-recovery-local.ps1 | C8 | 562f3306 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-source-work-collection-local.ps1 | C8 | f8d46363 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-week-lifecycle-local.ps1 | C8 | 171b8213 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-work-collection-local.ps1 | C8 | 0f4dd843 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-work-collection-recovery-local.ps1 | C8 | f4678841 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/run-bpay-next-work-collection-write-off-local.ps1 | C8 | 7d0ca36f / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/weekly-source/bpay-next-factual-email-census-classification.test.mjs | C6 | 75fcde60 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/weekly-source/bpay-next-installed-writer-census-successor.test.mjs | C6 | b9e245f9 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/weekly-source/next-paid-evidence.test.mjs | C1 | ec0620ee / absent / ec0620ee | no | none (already upstream) | Already published; keep fixed upstream. |
| tests/weekly-source/protected-local-saved-route.test.mjs | C6 | a637e937 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/weekly-source/protected-next-preview.test.mjs | C6 | e29b9332 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/weekly-source/weekly-source-authorise-scope.test.mjs | C1 | 1cb28efc / absent / 1cb28efc | no | none (already upstream) | Already published; keep fixed upstream. |
| tests/weekly-source/weekly-source-candidate-named-inventory-seal.test.mjs | C6 | 169d1fdb / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/weekly-source/weekly-source-coverage-install-order.test.mjs | C1 | 48d646f2 / absent / 48d646f2 | no | none (already upstream) | Already published; keep fixed upstream. |
| tests/weekly-source/weekly-source-local-next-admission-order.test.mjs | C6 | 04c15755 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/weekly-source/weekly-source-manual-final-report-banking-integration.test.mjs | C6 | 738ccf99 / absent / absent | no | F-08 source-gate triage | Existing unit/source test only if current normal source gate needs it; no new harness/fixture programme. |
| tests/weekly-source/weekly-source-paid-fixture-authorisation.test.mjs | C1 | 44ab8db9 / absent / 44ab8db9 | no | none (already upstream) | Already published; keep fixed upstream. |
| tests/weekly-source/weekly-source-protected-contact-publication.test.mjs | C5 | 44435dbe / absent / e89d1e80 | no | F-08/F-23 existing source gate only | Both added; manual local delta onto fixed upstream. |
| tests/weekly-source/weekly-source-query-delivery-command-fixture.test.mjs | C1 | 3205e294 / absent / 3205e294 | no | none (already upstream) | Already published; keep fixed upstream. |
| tests/weekly-source/weekly-source-withdrawal-owner-pins.test.mjs | C1 | 99517bba / absent / 99517bba | no | none (already upstream) | Already published; keep fixed upstream. |

## OFFICE — 17 paths

C3=1; C4=2; C6=3; C8=11

| Path | Class | R / B / U | H | Owner | Action |
|---|---|---|---|---|---|
| BANKING_PAY_BIBLE.md | C3 | 730028bc / 21e303b4 / 21e303b4 | no | F-16 | Port only reviewed local delta. |
| index.html | C4 | e893d0d4 / 5ca3d0d3 / 92021e59 | no | F-16 | Both changed; three-way merge reference/base/upstream, retaining upstream fixes. |
| js/main.js | C4 | f7481e5f / d56e0324 / 57fbb0b8 | no | F-16 | Both changed; three-way merge reference/base/upstream, retaining upstream fixes. |
| css/bpay-next-office.css | C6 | ce7ed0f0 / absent / absent | no | F-16 | New local; select product source by exact card; do not wholesale port. |
| js/bpay-next-office.js | C6 | e464183e / absent / absent | no | F-16 | New local; select product source by exact card; do not wholesale port. |
| js/bpay-next-payment-history.js | C6 | b4a57793 / absent / absent | no | F-16 | New local; select product source by exact card; do not wholesale port. |
| tests/.local-next-office-artifacts/.last-run.json | C8 | cbcc1fba / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/bpay-next-office.playwright.config.cjs | C8 | 75e14859 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/e2e/bpay-next-office-local.spec.cjs | C8 | 1ea8deb5 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-office-browser.html | C8 | 1ab7773b / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-office-browser.js | C8 | 887314f8 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-payment-history-browser.html | C8 | 66ee0708 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/fixtures/bpay-next-payment-history-browser.js | C8 | 6e902e74 / absent / absent | no | reference | Leave evidence/runner/fixture in reference worktree. |
| tests/unit/bpay-next-office-frozen-detail.test.cjs | C8 | 304186aa / absent / absent | no | F-16 conditional source-gate admission | Leave in OFFICE unless current frontend source gate expressly runs this existing unit test; no new local test programme. |
| tests/unit/bpay-next-office.test.cjs | C8 | 70974b80 / absent / absent | no | F-16 conditional source-gate admission | Leave in OFFICE unless current frontend source gate expressly runs this existing unit test; no new local test programme. |
| tests/unit/bpay-next-payment-history.browser.test.cjs | C8 | 8a6590e6 / absent / absent | no | F-16 conditional source-gate admission | Leave in OFFICE unless current frontend source gate expressly runs this existing unit test; no new local test programme. |
| tests/unit/bpay-next-payment-history.test.cjs | C8 | 220c509d / absent / absent | no | F-16 conditional source-gate admission | Leave in OFFICE unless current frontend source gate expressly runs this existing unit test; no new local test programme. |

## Reconciliation exceptions

- Tracked SQL: 33 C1, 10 C2, one C4 (17092026_0900_weekly_source_installed_writer_census_v1.sql). Retain current upstream census; after F-06 compiles the channel reader, add only its exact signature/hash acknowledgement and update Twelve to Thirteen. All other RESET census changes are upstream already.
- Candidate backend is C2; authorise-routing and next-paid-evidence are C1. Keep upstream. Protected orchestrator is C4: F-14 retains current Source-owned local completion/replay and adds NEXT only; no old external C1 publication fallback.
- Nineteen C5 NEXT SQL files remain F-05-owned. Two C5 Worker modules are F-09-owned; a C5 source test is only eligible through the normal source gate. Release metadata is C7, never wholesale copied.
- The inventory has 653 unique paths, not 653 product changes. Fixtures, proof runners, artifacts and unrelated documentation remain in their reference worktrees. Current MyTMS origin/main/active source is 6dbba3d67e770d1f67cfbcb16452b5f1e4a7848c; its existing dirty app/eas files remain owner work.
