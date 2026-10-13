# WB-44 stale-snooze design review

12 October 2026. Independent read-only design/source check, Sol6.1 Extra High. **DESIGN PASS after the deferred READY-state correction below.** This is permission to implement the narrow design, not a source/build/runtime completion claim. No product/checklist edits, database or business SQL, tests, fixtures, browser/hosted actions or commits were performed. The reviewer amended only the requested design note and this review.

Authority checked: complete approved `02_WORKBENCH.md:1287–1330` and `1557–1580`, current WB36/WB44 checklist rows, and owner11October A2 recorded on WB44 ITEM/D12. WB36:1312 explicitly requires real worker-financial replacement to cancel stale active snoozes with append-only audit; charge-only changes preserve them and frozen offers remain unchanged. WB44 supplies the existing bounded producer/barrier owner. WB39 and broader producer work remain separate.

## Resolved essential finding

The original design's deferred intermediate receipt required LEASED/SNOOZE. Current `public.bpay_next_financial_job_v1(text,uuid,text,jsonb)` first validates the POSITION owner's result and twin cursor (`11102026_2259_banking_pay_next_prepare_retry_v1.sql:889–930`), then yields its exact successful nonterminal lease to READY and clears nonce/lease in the same transaction (`936–945`). A deferred trigger on that successful page therefore observes READY, so the former LEASED-only rule would roll back a valid cancellation page.

Root accepted and the reviewer amended design §5: immediate UPDATE/event admission remains exact SEALED/APPLYING/SNOOZE plus the native LEASED job and valid lease at mutation. Deferred intermediate receipt permits the exact READY/SNOOZE job with NULL nonce/lease after that existing yield, or a direct native LEASED/SNOOZE job with its lease valid at the event's server mutation time. Both require the original command/publication/member/Work binding, owner_epoch equal to the Candidate's current native epoch, equal publication/job UUID checkpoint at or beyond the event's snooze UUID, predecessor applied pointer and exact event/version/snapshots. Final COMPLETE/APPLIED/DONE receipt requires no active lease. This does not introduce a lease proof store, replacement nonce or new receipt protocol.

No other concrete essential design failure remains in the reviewed scope.

## Required implementation boundary

The proposed placement is correct: retain current1346 NEW/REMOVED money loops and their original counts; add bounded SNOOZE after REMOVED exhaustion; move the original final APPLIED/applied-pointer/command/job completion into SNOOZE exhaustion. Intermediate pages retain the predecessor Work applied pointer and Processing barrier. The current1346 owner has no cancellation operation, so this is a genuine remaining A2 gap, not a duplicate producer implementation.

Each page must preserve the existing1–100 bound, native UUID seek and identical publication/job checkpoint. Advance over every visited control, not only changed controls. SNOOZE does not increase applied_line_count or reconcile money. Original DONE and stale-cursor readback remain before mutation (`1346:275–293`). Mere unchanged SNOOZE visits must not inherit the existing `v_seen>0` financial-view bump; actual cancellation already uses the snooze trigger's original metadata/ORIGIN barrier, and genuine terminal completion retains the original final invalidation/refresh (`1346:421–434`).

Use only the publication's sealed revision and original command sequence. Exact WHOLE/EXPENSES_ALL bases and stable-key SEGMENT/EXPENSE_LINE points already exist (`10102026_1236_banking_pay_next_segment_authority_final_v1.sql:10–42,85–96`; equivalent scope mapping `11102026_0312_banking_pay_next_snooze_context_controls_v1.sql:104–127`). A missing entire basis or inconsistent membership refuses; genuine absent stable membership cancels that scope. Equal worker basis and controls at/after the original publication sequence survive. No current/latest revision substitution, segment remapping, financial calculation or frozen-row update is authorised.

System cancellation must retain the original scope/basis/human-origin tuple, advance version by exactly1, set active=false and the original publication sequence/request, and append the existing unique CANCELLED event with actorNULL/reasonWORKER_PAY_CHANGED and exact before/after snapshots. Keeping cleared_reason NULL avoids an unnecessary CHECK change. The guards0255 ordinary human/DUE paths remain intact; the new system branches precede those fallbacks and perform indexed exact-row/event/previous-version checks. Defer checkpoint checks until the native page checkpoint exists; do not demand the new cursor at event insertion.

Necessary support is confined to the publication phase CHECK, active `(work_id,id) WHERE active` seek index, current apply owner, two snooze guards (and an optional private point validator), POSITION-only phase/cursor admission in the current financial service/Retry helper, and the three existing Worker/FE POSITION schemas. `claim_position_job_v1`, pointer guard0928, publisher/basis/seal owners, queue{id,kind}, PREPARE/PF08 and ordinary snooze commands need no new owner/protocol. SNOOZE cursors are NULL or canonical UUID; existing NEW/REMOVED component cursors keep their original typing.

Existing lock order remains module → Candidate → exact job → publication → Work → bounded snooze UUID rows. Reuse the existing snooze trigger and final bounded projection refresh; do not add Case locks, agency/history materialisation, a command-clock allocation or producer rewiring to cancellation.

## Reviewed raw source identities

| Artifact | SHA256 |
| --- | --- |
| Amended `design/WB-44-stale-snooze-design.md` | `1aaa00088c36944c7ef1d0d1342a5c243ed0ee2f26da1ae43ea301a1f250186d` |
| `design/WB-44-stale-snooze-investigation.md` | `4c43f5a3dbea1155f328a94c8a6f7bfd126e9294c451f169d8aaadfb7ce5764c` |
| Apply/publisher1346 | `8934065d152ab69bbe2ec8ee298bd03b66f4c20a82fbbcdced78567db37bb69f` |
| Snooze guards0255 | `b0d1efa5398f6340e3c3a399b7334bbabd46f33f9128731945a81587872d8c16` |
| Financial service/Retry2259 | `1a57552c42d63d0dc885a4e7294472c03319537da9e913a96cbbadc10c2ab879` |
| Basis/seal1236 | `f74ea457bf77f0d85ce7f3f21089bdecf9d9d9121d4005850de430d50aa7379d` |
| Work pointer guard0928 | `2a7a8cb0d64c8e56404b1889b8e5f5bbb6bae7ac49700342fa3e81b315a8a7f3` |
| Current final-definer registry | `76f9a407c3268aaa3ba1ec8339c8e898dff584b473e27535cc9b6fe2175acc07` |

Compilation/catalogue, exact constraints/ACL/source gates, charge-only preservation, genuine pay-change/removal cancellation, more-than100 controls, later-control/earlier-Create ordering, lost-reply recovery and full queue/app/timing acceptance remain **NOT_RUN** by this reviewer. WB44 ITEM/D12 remains open until root implements and receipts the required successor; this verdict does not close WB39.
