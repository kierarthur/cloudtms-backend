# WB-01 ordinary Office authorisation — current source chain

**Closed source-only provenance record for the existing NX-022 → effective ordinary authorisation → NX-003/NX-002 → NX-001 chain. No new guard/body is needed for this record.** Read on 10 October 2026 from BUILD-BE current disk and `final-definer-closures.json`. This records source authority, not runtime/installation proof, full WB-01 projection completion or an app journey. No product, gate, test or SQL execution/change was performed.

The explicit requirement is normal R3 `02_WORKBENCH.md:93,97,100,113`: record the ordinary route and authorise-authority reader, preserve only effective Office authorisation as the publication boundary. The historical `evidence/NEXT_INVENTORY.md:55` identifies NX-022 with the complete 1310 authority file. Its parenthetical route/module/worker reads occur in that file's bulk mutation owner, not in the pure Workbench row-source reader. Do not describe the reader as invoking a payment publisher or granting mutation authority.

## Physical winners and exact seams

All paths below are relative to BUILD-BE `C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/bpay-next-build-backend-20261009`.

| Stage / exact current signature | Current physical path and exact lines | Meaning |
| --- | --- | --- |
| NX-022 retained reader `public.bulk_timesheet_workbench_row_source_v1(jsonb)` | `supabase/repeatable/14082026_1310_timesheet_processing_status_and_authorise_authority_v1.sql:6–11,1105–1109,1124–1154,1155–1197,1281–1343,1485–1489` | STABLE/security-definer row source exposes real TF processing status, summary stage, ordinary route/display context. PENDING_AUTH is retained at 1150 and tools/display derive Processed from PENDING_AUTH plus actual unauthorised physical identity (1294–1304,1329–1339). It is a reader, not permission to bypass the mutation owner. |
| Effective Office generic mutation `public.timesheet_authorise_generic_atomic(uuid,uuid,uuid,timestamp with time zone,text)` | `supabase/repeatable/10102026_0931_weekly_source_pay_query_outer_entry_admission_v1.sql:1753–1758,1816–1829,1845–1863,1879–1897,1925–1935,2073,2089–2098,2101–2126,2145–2155` | Original payload and current-identity checks remain. It locks actual Timesheet/current TF, rejects archive/invoice/already-authorised and mismatched expected identity/signature, then retains AUTHORISE_NOT_ALLOWED unless the actual previous TF status is PENDING_AUTH or READY_FOR_HR. SP-12 contributes only the existing NEXT early admission. This does not remove the retained status refusal. |
| Actual authorisation stamps | Same 0931 file `2173–2185,2200–2207` | Original lifecycle context is set; physical `authorised_at_server` is stamped before the exact current TF's `authorised_at_utc`, with original processing-status/actor updates. The TF UPDATE is the real trigger seam, not a proposal/import reader. |
| NX-003 trigger `private.bpay_next_ordinary_authorisation_trigger_v1()` and actual attachment | `supabase/repeatable/03102026_1530_banking_pay_next_ordinary_approval_v1.sql:608–641,686–690` | AFTER INSERT or UPDATE OF `authorised_at_utc` on `public.timesheets_financials`; INSERT nonnull or UPDATE NULL→nonnull is actual new authorisation. Nontransition and non-NEXT return. It calls the actual route, and only ORDINARY_HOURS / EXPENSE_CARRIER invokes the publisher with the exact `new.id`. |
| NX-002 `private.bpay_next_approval_route_v1(uuid)` | `supabase/repeatable/03102026_1450_banking_pay_next_producer_route_v1.sql:33–48,50–92` | Exact current/unarchived/unrevoked physical identity and real Contract; expenses/mileage are carrier. Proven genuine import-correction children return FACTUAL_IMPORT_CORRECTION; self-bill or actual Source family returns SOURCE_HOURS. Remaining HOURS returns ORDINARY_HOURS at 92. Import labels alone cannot take the ordinary path. |
| Current ordinary publisher `private.bpay_next_publish_ordinary_from_financial_v1(uuid)` | `supabase/repeatable/10102026_1009_banking_pay_next_ordinary_approval_rate_freeze_v1.sql:77–98,261–304,683–688` | Rechecks NEXT, exact TF/Timesheet/Contract, permitted ordinary/carrier route, current/nonstale TF, both authorisation stamps, version and Candidate/Client/channel. Creates actual immutable revision/rate/line evidence, reconciles exact stored totals and invokes staged publication once with the actual work/revision/command. |
| NX-001 `private.bpay_next_publish_staged_revision_v1(uuid,uuid,uuid)` | `supabase/repeatable/03102026_1430_banking_pay_next_position_writer_v1.sql:29–61,67–99` | Original exact replay check, physical booking/version/current/archive/revoke validation, retained Timesheet marker, predecessor, seal and current pointer. Final-tail POSITION_APPLY receipt/member/publication/SEALED command are one transaction. No early fake readiness or partial publication is introduced. |

The actual Worker ordinary choice is `broker/src/index.js:168244–168248` (generic versus explicit duplicate-expense-reviewed owner). Its linked Contract fallback calls the generic owner with exact Timesheet, expected Timesheet, actor, time and optional expected signature at `170245–170252`; this records existing caller wiring, not a new route decision. The reviewed-owner wrapper remains a genuine delegate; there is no need to replace it for this provenance record.

## Registry/include provenance

At this checkpoint the registry gives the reader finalRoot `supabase/repeatable/05092026_1200_banking_pay_draft_v8_final_authority_closure.sql`, finalPhysical the exact 1310 file above. The verified include edges are 1200:13 → `30082026_2358_banking_pay_dirty_apply_family_authority_repair_v1.sql:18` → `08082026_0902_reassert_authorities_after_legacy_monolith.sql:57` → 1310. Later functions within 1310 do not redefine this reader.

The registry independently gives the actual generic mutation finalRoot/finalPhysical 0931, trigger 1530, route 1450, ordinary publisher 1009, and staged publisher 1430. The old 2018 publisher and 1310 generic mutation are predecessors, not the current physical winner. The ordinary source publication from the retained trigger therefore resolves the 1009 function at execution; the trigger body need not be recopied.

| Current source | Raw SHA-256 |
| --- | --- |
| 1310 reader/authority file | `f19f780febcc8c1b74a492a1f9ac2bb80c10f5a936ed866b667da267b86db28b` |
| 0931 current generic authorise file | `7fdddc57b5f1ca6aff196567a10dd09b393185b17efdfe9b58bb766aea7e847b` |
| 1530 retained trigger file | `df943acaa6ddc4fa5bea877621762e96c93360d4030383f29b5bffca2d3aa301` |
| 1450 actual route file | `a8f9dc118ad75b62b3863820e38bd29cbc4c05aa47facf12c11e45b9013bfc74` |
| 1009 current publisher file | `34227084b44b53abfb441b2b2826d9ce437fdb20e636e7d3d1d3f54bf324c7d7` |
| 1430 retained staged publisher file | `395ead5cf46a1399ef4859f056d89508b0862f9b0fda8dde92454107f17602fa` |
| Current final-definer registry | `2dc51e7f79fb45b97905af6bcdc39bef57d172c57e53c111af2ce2ab7b0fd8e4` |

An unauthorised save/proposal/import has no new authorised TF transition into this ordinary trigger. Even an attempted direct call to the ordinary publisher still requires the genuine current TF and physical authorisation stamps and ordinary/carrier route. This is the finite source boundary; no runtime claim is made. The remaining Workbench refresh/projection and later named Daily/tax/WAIT chain are still governed by BD050 and their separate cards.
