# Availability disagreement: 13 October 2026

## Observed cause and limits

Kier's saved Google window covered 12–25 October. The database had ten canonical choices but no row for 19, 21, 22 or 24 October; MyTMS correctly renders absence as PENDING. Google's saved choices for those dates were LD, LD, N/A and N/A. The last explicit legacy submission completed at 12 October 23:10:42.862 UTC (13 October 00:10:42 UK), advancing canonical version 58 to 59 and accepting all nine changed dates. The legacy browser sends changed dates only, so unchanged saved Google choices were not included.

All 59 canonical command receipts were inspected in version order, with no version gaps. None contains those four dates. The observed generation clear on 10 October affected 10 October only. No source deletion of availability rows was found. This is evidence of choices never synchronised into canonical storage, not evidence that this rollover deleted canonical choices. The exact original creation time of these Google cells is not established; unlogged historical Google events must not be represented as proved. The earlier Google day-alignment fault is a separate incident.

## Reviewed correction

The reconciliation RPC locks the candidate's authority scope and fills an absent nonblank choice only under stable database-primary authority, in its exact active generation, without a booking or block. Existing canonical rows, including explicit PENDING, remain authoritative and are never overwritten by observation. Missing blank choices remain logically PENDING. Each inserted choice receives a canonical version and a durable RECONCILIATION receipt with reason MISSING_AVAILABILITY_BACKFILLED; exact replay does not duplicate writes. Browser grants, SECURITY DEFINER and empty search_path remain unchanged.

After an accepted enrolled-worker legacy save, the Google bridge now reconciles the actual saved window, rather than replaying all dates as new worker intent. Unsettled repair reports PENDING with intent_saved=true and count-only diagnostics. Non-enrolled workers retain the legacy-only path. The TEST-only Kier operator performs the same signed bounded reconciliation without changing Sheet choices.

## Verification and installation status

- Backend JavaScript regressions: 1,487 passed.
- MyTMS/Google regressions: 296 passed, including receipt-only retry after a confirmed save whose window reconciliation is still pending. Source pushed to kierarthur/mytms-app, codex/candidate-approved-hours-20261003, cb0c4a873a367e6af2fc84e297df628f04fbd488.
- Managed database/release tests: 66 passed.
- Arthur's hosted TEST rollback lab: proposed RPC and disposable positive/negative fixture passed; physical database cloudtms_test_clone, outer transaction rolled back, fresh connection proved fixtures absent and original definition restored.
- Local PostgreSQL 17 catalogue proof: all portable verifiers passed after granting the disposable local non-superuser owner the parameter permission required by an existing trigger-suppression fixture. No hosted permission was changed. Regenerated contract 0a961ae1f146184fff7d3a191f8bf598735da67dbba1ff666c0394961bcbeda5 changes only the intended reconciliation routine's definition hash. Second clean NEW replay passed every required verifier and exact contract comparison, ending VERIFIED. Protected TEST installation remains pending at preparation; this report does not assert hosted installation yet.
- Google saved baseline independently copied from Arthur's Availability editor: LF SHA-256 c4d91d46a87d523649c78a7b240059203d9382659d4cad390c03c630b7f983b4. Reviewed three-function patch preserves 57 unrelated functions byte-for-byte, including installed retry handling. Prepared patch LF SHA-256 28c8d066a83142db4903c6626d53170e0cb2683f3bf2b8d7b270a6f504ada01c. Not yet saved or published at preparation.

No Master rollover timing/trigger changes, credential changes, LIVE work, app-store upload or new native build are required. Natural rollover acceptance remains the separate scheduled date-keyed before/after check; a mock or immediate repair is not proof of tomorrow's real trigger.
