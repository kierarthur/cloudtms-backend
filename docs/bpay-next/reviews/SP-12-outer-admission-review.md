# SP-12 outer admission source review

Verdict: **PASS — source review of the seven 0931 successors and their registry entries.** No actionable source defect found in this bounded batch. This does not claim business execution, installation, whole SP-12 completion, or application acceptance. No SQL, tests, fixtures, runtime, network or business calls were executed by this reviewer.

Reviewed against all seven exact signature/base pairs in `.codex-local/SP-12-outer-targets.json`, the SP-12 card/checklist scope, and the existing admission/auth/preview sources. Rule B remains unchanged; this batch does not introduce a producer protocol or change a money owner.

## Full predecessor preservation

For each definition, compared the complete `CREATE OR REPLACE FUNCTION` text with its designated current predecessor. After LF normalization and removing only the eight identified insertion blocks, every definition is exactly equal, including parameters/defaults, return type, declarations, language/security/search path/configuration, all original statements and exception handling. There is exactly one original and one successor definition per target.

| Exact public owner | Current predecessor file | Preserved definition characters | Admission seam in 0931 |
| --- | --- | ---: | --- |
| `candidate_submission_finalize_atomic_v1` | `24092026_2247_candidate_provisional_expense_carrier_lifecycle.sql` | 54,957 | Lines 106–119, before workflow `FOR UPDATE`; existing environment/feature/key checks remain first; plain session auth uses `p_lock=false`; original locked auth and workflow/Candidate checks remain. |
| `candidate_submission_finalize_single_flight_v1` | `04092026_1420_candidate_finalisation_single_flight_v1.sql` | 2,057 | Lines 1039–1076, before global/workflow advisory keys; finite nested declaration block reuses the atomic child's environment, service context, feature, key and plain session checks. Original capacity/pending returns and exact child call remain. |
| `candidate_weekly_paper_target_prepare_v1` | `30082026_0714_candidate_weekly_paper_target_ready_state_v1.sql` | 12,268 | Lines 1166–1197, before original locking auth and paper/workflow locks; duplicates original service-context validation or plain session auth, with selected-Candidate qualification. Original `p_lock=true` revalidation remains. |
| `candidate_office_expense_paper_prepare_v1` | `06092026_2355_candidate_advanced_expense_completion_v1.sql` | 3,376 | Lines 1491–1495, after original environment/input validation and before operation/update/workflow `FOR UPDATE`; existing actor/action/environment/workflow/state tuple checks, context open/close and bare exception rethrow remain. |
| `contract_week_manual_upsert_bulk_process_atomic` | `21072026_1235_34_contract_week_manual_upsert_bulk_process_atomic.sql` | 9,115 | Lines 1578–1582, valid nonnull week only, before the continuing correction-transition prebranch and outside the original six-attempt retry loop. Original null-week/correction branches and child retry behavior remain. |
| `timesheet_authorise_generic_atomic` | `03102026_0500_h1h2_successor_authorities_after_retry.sql` | 29,288 | Lines 1816–1830, duplicate original required-ID checks then admission before the entry diagnostic and first Timesheet row lock. Original correction refusal, later validation, `revoked_at` restoration and all business statements remain. |
| `timesheet_weekly_chain_delete_apply` | `17092026_0900_weekly_source_pinned_owner_rotation_guard_v1.sql` | 8,855 | Lines 2417–2421, after original preview/actor validation and before managed-root guard/base owner and later contract locks. Original expected identity arrays, protected refusal, delete base invocation and result handling remain. |

The eighth insertion is the generic owner's lines 2355–2358: after `GET STACKED DIAGNOSTICS`, exact `55P03` plus `WEEKLY_SOURCE_PAY_QUERY_ADMISSION_BUSY` is bare rethrown **before** exception diagnostics and the historical generic `LOCK_TIMEOUT` translation. Other errors retain their original handling.

## Boundary, locks and security

Each admission call is conditional on actual module `active_owner='NEXT'`. Bulk admission additionally requires a nonnull week, so it cannot introduce an early lock on the original null-week path. Admission precedes the actual continuing correction branch because that branch can itself lock before reaching the manual-upsert child. Admission refusal is outside the retry loop, preventing that Source refusal from entering historical contention retries.

The reused `_candidate_session_context_v1(...,false)` branch reads/authenticates session/account/Candidate scope without its session `FOR UPDATE`; its original true branch remains downstream. The service-context predicate is the existing stable helper. The chain-delete preview has no business write/lock statements and validates a nonnull active actor before reading the Timesheet; its financial-history call is retained. The preview's original declaration does **not** explicitly mark it `STABLE`; the review relies on its actual read-only body, not a claimed volatility annotation. No preview/auth helper is rewritten here.

The unchanged admission helper takes the single existing transaction advisory key with `pg_try_advisory_xact_lock`, producing only the exact Source busy error on contention. Nested calls reuse that transaction key. Original downstream locking and generation/permission/state rechecks remain; no early scope probe replaces them. The expense paper owner's existing service-only entry and qualified stored operation/update/workflow checks remain its authority; no caller-supplied actor ID is newly treated as a permission grant.

All seven functions retain their original exact signatures and `SECURITY DEFINER` configurations. The batch adds only the existing logical `postgres` owner declarations; it has no new `GRANT`, `REVOKE`, schema/table/trigger or public interface. Existing ACLs survive `CREATE OR REPLACE`; the predecessor Candidate/chain-delete sources retain their service-only grants and browser-role revocations. The file has one managed transaction envelope and one final schema-cache notification.

## Final chain and source identity

All seven exact signatures select 0931 as both `finalRoot` and `finalPhysical`. Their predecessor/allowed-physical lists retain the designated original sources and include the new physical definition; the chain-delete predecessor list retains its historical/base guard order. No parked F-25 definition or pin/enroller/producer owner is selected by this batch.

SHA-256 of the reviewed saved bytes:

| Source | SHA-256 |
| --- | --- |
| `10102026_0931_weekly_source_pay_query_outer_entry_admission_v1.sql` | `7fdddc57b5f1ca6aff196567a10dd09b393185b17efdfe9b58bb766aea7e847b` |
| `SP-12-outer-targets.json` | `f431b4d4295185c96e8790a91880248fcb02302bdf1a0cdde82fbd87a207c9c2` |
| `final-definer-closures.json` | `003016d201a1ba3688ed2b4b311e8ab05b012c19a40603791c0109671f7a3368` |
| `24092026_2247_candidate_provisional_expense_carrier_lifecycle.sql` | `0c055f4091f36cac0eabb920b19d819fd205df5dbeb55ddebc107c404cdd2c21` |
| `04092026_1420_candidate_finalisation_single_flight_v1.sql` | `98d88b42ea50d66a21fd146c9277b2748b88da35ecf9e1f95c36c47e86f1a8c0` |
| `30082026_0714_candidate_weekly_paper_target_ready_state_v1.sql` | `49fc1fe0b7c2740ce86a046ce525c7d123b6f350309cb4b2afcfcce156bac04d` |
| `06092026_2355_candidate_advanced_expense_completion_v1.sql` | `c840d1462ff0c31bbe8b9cb56841bd6d8c0e732aeacf81e65d9d7879e610707b` |
| `21072026_1235_34_contract_week_manual_upsert_bulk_process_atomic.sql` | `bf83ca830340900ad0f22e482b18fa061df3e1f0ee59334f7f01540c8f2dba3d` |
| `03102026_0500_h1h2_successor_authorities_after_retry.sql` | `2246174a1df3ffb20da053b548b2224fe8b67afcc6f0e4fe3fabf3b35f876b67` |
| `17092026_0900_weekly_source_pinned_owner_rotation_guard_v1.sql` | `eb5f823f94ff458df054826f0a976781103ee42a5b79aeb115bea879b68f1789` |
