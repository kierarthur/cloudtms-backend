# SP-02 current entry and factual-boundary census

10 October2026. Read-only independent investigation of full card05:128–158, all six exact deliverable rows, current scope, F04 selected definitions,0550/0555/0556 and actual Source callers. No product changes, SQL, tests, runtime, fixtures, hosted/data/network operations. This is source design evidence, not an installed/in-flight observation or whole-SP02 acceptance.

**Main finding:** do not retire2336 OPEN/task preparation or1534 stage_c1_request merely because their names contain pay/C1. They are used by current factual Source/TSFIN and Source-local protected decisions under NEXT. The explicit RECOVER_FINALISED_PAY and external legacy C1 financial admission need a separate genuine DB-owned typed boundary before the first mutating/network call. Existing0550 emits55000, not PT410.

## Current true definitions / dispositions

| Real entry or owner | Actual current source and evidence | NEXT disposition |
| --- | --- | --- |
| public.weekly_source_finalise_atomic_v1(jsonb) → private.weekly_source_finalise_core_v1 → finalise_engine_v1 |1534 finalisation:2782–2812; immutable Source final revisions/snapshots, billing movements/manifests and expense invoice facts. No call to finalisation-pay OPEN/apply and no legacy Banking/Workbench mutation in this owner. | Retain factual finalisation, replay, chronology and invoice authority. Never add a generic legacy-finance gate here. |
| public.weekly_source_finalisation_pay_open_atomic_v1(jsonb) |2336:296–464; service/actor/Office authority, manifest/run/tasks417/431 and native context. Worker automatic finalisation calls it after the genuine finalise RPC at988. | Retain shared Source durable task preparation. Gating it globally would leave successful Source finalisation unable to prepare its real TSFIN/proposal tasks. |
| public.weekly_source_finalisation_pay_task_start_atomic_v1 / task_finish_atomic_v1 / task_unknown_atomic_v1(jsonb) |2336:465/567/678; task/version/replay and actual ordinary-projection receipt ownership. Finish652–670 marks Source state transitions and completes the exact task; unknown preserves ambiguous result. | Retain genuine Source checkpoint/receipt reconciliation. No invented terminal success and no retry based on absence. |
| public.weekly_source_finalisation_pay_task_recover_atomic_v1(jsonb) |2336:752–888; native run/task/version checks; retained ordinary projection receipt828–855; otherwise marks unknown or, only explicit confirm, re-arms the same Source task873–877. | This function is not intrinsically a legacy money writer. The user-facing RECOVER_FINALISED_PAY policy must nevertheless obtain the card's typed NEXT refusal at its true action entry before OPEN/re-arm. Preserve honest old receipts/unknowns and keep factual automatic paths separate. |
| public.weekly_source_ordinary_pay_projection_apply_atomic_v1(jsonb) |1534 ordinary projection:2334; never-authorised branch2969–3004 uses real Timesheet schedule/TSFIN/expense owners, deliberately does not Authorise; authorised branch3013–3034 records Source entitlement proposal without rewriting the root. Receipt/transition3040 onward are Source evidence. Current correction owner also calls it at1534 correct-final-source:2160. | Retain genuine Source preparation/proposal semantics. Its pay name is not authority to fence TSFIN.0555 suppresses the actual retired callbacks below. |
| public.weekly_source_target_managed_root_prepare_atomic_v1(jsonb) |1534 ordinary projection:4262; actual root TSFIN preparation or Source proposal and immutable audit result; Worker protected non-NEXT-route context preparation uses it. | Retain Source-local protected preparation; direct NEXT route intentionally avoids this old root preparation. Do not replace with a fake bank/Source receipt. |
| public.weekly_exceptional_pay_stage_c1_request_v1(jsonb) |1534 protected C1:904–1907. Current Worker protected action:724 still calls it for SOURCE_LOCAL then completeLocalDecision. It writes actual protected family/approval/generation/events/staged source records;1711–1712 captures the genuine NEXT current decision. | **Must remain available for Source-local decisions under NEXT.** It stages the local receipt identity, not permission to execute external C1 finance. |
| public.weekly_exceptional_pay_read_c1_request_v1 / record_c1_unknown_v1 / record_c1_checkpoint_v1 / record_c1_recovery_v1(jsonb) |Same1534 protected C1:719/38/252/529; retained staged payload/checkpoint/unknown receipt owners, not the external financial API bodies. | Preserve native read/reconciliation and exact persisted unknown identity. Do not blanket classify by C1 spelling. Fresh external delivery is a separate admission boundary. |
| public.weekly_exceptional_pay_complete_c1_publication_v1(jsonb) |Same1534:1909–2160. Existing PUBLISHED replay1986–2009; otherwise exact SUBMITTED/PENDING_C1, real terminal PUBLISH checkpoint2010–2056, operation/request/hash agreement, then retained family/generation state. | Old external-C1 completion, not the new NEXT publication route. Never use it to manufacture NEXT success or revive external delivery. A genuine already-admitted terminal checkpoint/replay is a real reconciliation obligation; preserve its exact evidence rather than broadly refusing/dropping it. Active/unknown cases remain F18/F19-owned, not automatically approved here. |
| public.weekly_exceptional_pay_next_publication_status_v1 / publish_next_v1 |Actual0345; current protected Worker:531–580 selects NEXT/C1/SOURCE_LOCAL from DB, direct NEXT publishes through the genuine NEXT owner. | Preserve direct NEXT approved publication and exact saved replay. No Worker-created owner choice. |
| public.weekly_exceptional_pay_complete_local_v1(jsonb) |Actual1257; exact staged/request/actor/run/family/generation proof; local source owner and original receipt, request RETIRED423, run COMPLETE425, real local receipt COMPLETE/PENDING_FREEZE427.0510-series local context validates exact stage ownership;0659 route requires genuine native domain evidence. | Preserve. RETIRED is accepted only with the same native local receipt proof; active/unknown C1 never ignored or synthesized into local completion. |

Single-definition Source entries above need not appear as a duplicate/F04 chain row; their saved physical definitions and sealed catalogue identities were read. For registered shared roots, actual F04 finalPhysical is0555 for all eight callbacks,0556 for Source publication/withdrawal factual boundary,2018 for first Authorise, and the0659 sqlinc for protected owner-route selection. Nothing here uses the parked F25 producer protocol.

## Existing physical and callback containment

0550 fences33 actual legacy Banking relations on row INSERT/UPDATE/DELETE and statement TRUNCATE. Its private assert takes module singleton FOR SHARE and validates owner/epoch; any non-LEGACY writer raises `55000/BPAY_NEXT_LEGACY_WRITER_RETIRED`. The shared banking_pay_scope_change_transactions token is intentionally excluded only together with the0556 actual zero-effect Source contract; do not add a token-only exemption.

0555 already adds a pre-work owner callback boundary to these eight current selected definitions. Their actual gate lines are77,869,1712,1938,3100,3404,3513,3601 respectively:

1. public.pay_timesheet_summary_pay_state_refresh(uuid[],uuid)
2. public.pay_timesheet_summary_pay_state_refresh_trigger()
3. public.pay_timesheet_summary_advance_state_refresh(uuid,uuid)
4. public.pay_workbench_mark_candidate_dirty()
5. public.pay_workbench_dirty_event_enqueue(text,text,text,uuid,uuid[],uuid[],jsonb,text,integer,timestamptz)
6. private.weekly_source_entitlement_head_ledger_mark_v1()
7. public.pay_workbench_scope_counter_stage_trg_v1()
8. private.pay_workbench_scope_invalidate_v1(uuid[],uuid[],text,uuid,jsonb)

Under NEXT those callback owners return their factual skipped result/NULL before legacy projection/queue/scope work. Keep generic Timesheet Authorise and genuine Source head/first-authorise/withdraw facts; do not replace them with PT410.0556 proves the genuine unallocated native token, current observation frame and zero JOB/SCOPE/REGISTRY effects for publication/withdrawal.2018's genuine NEXT first-authorise receipt stays its selected owner. Existing inventory seed/tracker names alone are not proof that they should be called under NEXT; current retired Workbench fan-out is contained by the eight callbacks/F12 and not a reason to revive G01/V8.

## Concrete RECOVER entry hazard and minimum placement

Current `recoverWeeklySourceFinalisationPayProjection`1039 calls mutating OPEN before TASK_RECOVER1057. An early refusal solely inside TASK_RECOVER is too late to claim no new run/task writes. Conversely an unconditional NEXT refusal in shared OPEN breaks automatic factual finalisation.

The explicit action therefore needs a DB-owned retirement check before its first OPEN call, after genuine authenticated actor/request validation. That check must derive the singleton owner in DB and return/raise the one typed `PT410/BPAY_NEXT_LEGACY_WRITER_RETIRED` result under NEXT; the Worker preserves it and does not transform it into a successful Source run, generic preparation failure, unknown financial outcome or permission to retry. Reuse established LEGACY authority/caller flow where retained by the exact card. Do not reprice or resolve real historical receipt ambiguities merely to satisfy this action fence. The chosen supporting entry/probe implementation is the builder's design decision; this census does not authorize a blanket shared-owner gate.

## External C1 raw RPC boundary: exact source limit

Current C1 adapter53–70 names18 RPCs: weekly_source_start_c1, stage_c1, continue_c1, certify_c1, publish_c1, pending_c1, abort_c1, retire_c1, repair_c1, status_c1, scope_start_c1, scope_continue_c1, begin_r01_c1, scope_status_c1, scope_abort_c1, scope_release_c1, workbench_execute_c1, cleanup_c1.

Those external stream API bodies are absent from canonical BUILD repeatables/migrations and the current sealed catalogue contains zero matching `weekly_source_*_c1(...)` identities. The Source checkpoint owner records their names as **C1.** external witnesses; that is not a public agency function definition. This is local source evidence only, not a hosted claim. Do not invent replacement financial SQL bodies from adapter names or weaken real installed ownership. Pure status versus fresh financial mutation/abort/repair cannot be proved from spelling alone.

The current F14 protected Worker never calls external C1 to save SOURCE_LOCAL or direct NEXT. D05's supporting raw-RPC boundary must obtain a real DB-owned typed refusal before any fresh external C1/Workbench submission under NEXT, preserve the lossless/no-retry request/result contract, and not reject genuine Source dataRpc stage/local/publish-next owners. Genuine already-admitted status/terminal receipt reconciliation needs exact durable owner evidence and remains separately reconciled before switch; neither an absent body nor a retained old name manufactures a safe financial route.

## Banking absence reader

Current17092026_0100 is private informational status. Its arrival detector currently probes the old installed money-movement classifier's two marker branches; boundary notice318 currently says Banking is disconnected and references the obsolete Workbench activation gate. Under positive NEXT it needs the card's NEXT information and real NEXT arrival meaning, without probing/preserving old Banking parity or mutating LEGACY declaration facts. This reader/notice is not financial admission and must not serve as a payment-safety proof. The current canonical body is source evidence, not a newly fetched installed definition.

## Key raw SHA256

| Source | SHA256 |
| --- | --- |
|2336 finalisation-pay orchestration | `2d0f9225dc4607932b2d731b453f07ce2665a5208ddc6ee764ddb8aa02488709` |
|1534 ordinary projection | `665ddd27108a867f92e1f04dd346130736df27031a577f108b7f9c1bf6807900` |
|1534 factual finalisation | `19018ea7be5e06a510fdd748409bcd84a18d0e31724daa19afb0a4471349a95b` |
|1534 protected C1 staging/receipts | `c74ea692273600ce753ee92c09efca5e812d8a61c4a536058c2a405c9be3f790` |
|0550 fence | `de1314ab23759fe8b584e6757bdca49addb18267d060aab544d98fc015bb0145` |
|0555 callback boundary | `eba6b3596434aa6eecf133594579fb7833a7e3acdbe7d6fbd3676fbfb913db86` |
|0556 factual boundary | `64ef7e6d04e1b652af722cb9dd050310fcd8e1cd249f59b9359428a310b70933` |
|Finalisation-pay Worker | `f151a46ca7d206d572a120d2ad65d1ddc2773dbfe6a269aeaff8950d89c90361` |
|Protected action Worker | `5352ef8bcc66e87b090d5a1d4f40ba872cf7a542d38f60aaf690ec1eea80f3c3` |
|C1 raw RPC | `c3a150ea58a22834c6d94bf962e83b2c9331e694eb9e805ee04bae1827673214` |
|C1 adapter | `0af84a1c0e190de3813160cfb05a870488d511446818a89a3518bb65da5abc59` |
|17092026_0100 absence reader | `191cfbf4427c4dc4a35d50fcd679e6e09c06189b3f8561473f4bdc2f0c75c6a8` |

No known staged C1 request/run was queried or touched. Remaining concrete build choices are the explicit RECOVER pre-OPEN DB boundary, raw external-C1 pre-network boundary and truthful NEXT informational reader; preserve all factual owners above.
