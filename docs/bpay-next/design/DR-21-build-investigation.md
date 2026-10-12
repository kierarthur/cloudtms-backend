# DR-21 build investigation — sequence127

12 October2026. Read-only saved-source/design investigation; **DR21 is not implemented or approved by this note.** Root alone authors product. No database/business RPC, hosted request, fixture/test programme, render tool, pack change, product edit or commit was performed.

Read the complete DR21 card in `C:\Users\KierArthur\OneDrive - Arthur Rai\Documents\GitHub\cloudtms-backend\codex_outputs\claude-stage2-continuation-20260925\BPAY_NEXT_BUILD_PLAN_20261009_codex_r3\03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:547`, all nine exact current checklist rows, R3.md#dr-21/R3.json and the current final-definer registry. The active exact completion is R3's bounded all-worker/frozen-label/last-complete-projection contract; the older installed baseline and a thin local transport are not the completed worksheet. Shared safety remains OR44/PolicyX accepted-input ownership; no current Candidate/rate/Case reread may reprice the Draft.

## Required deliverables and current gap

| Row | Exact ownership / remaining change |
| --- | --- |
| D01/D04/D07 | Context only: existing public page, typed preparation transport and reviewed card. No duplicate implementation task or historical rewrite. |
| D02 | New `banking_pay_next_paye_worksheet_v1` reader: every PAYE worker in exact run/channel, bounded keyset/filter; frozen labels/gross/stage deductions; accepted versus pending net, actual bank amount/source and payout-only truth. Currently missing. |
| D03 | Closed `PAYE_WORKSHEET` request/response/cursor/error contract and exact field mappings, not an arbitrary JSON passthrough. |
| D05 | Add exact kind/argument/receipt/error map to existing BE `broker/src/bpay-next-preparation.js`; preserve admin/actor/NEXT qualification and original route. Currently CHANNEL_WORKERS is not this DTO. |
| D06 | Native reusable worksheet viewing component in F16 Office host, consumed by the DR22 window and later DR29 PAYE Worksheet tab. Existing single-worker net box does not satisfy all-worker view/filter/detail. Later host/edit joins must stay explicitly open until built. |
| D08 | Exact new kind branch in current `public.bpay_next_preparation_page_v1(uuid,text,jsonb)`, retaining every existing branch and current shared edits. |

Direct dependencies: DR18 and F08. Original chain is current F04 registry authority, not the old0575 ledger file. DR37 later adds detailed net-refused code/replacement behaviour; DR22 owns edit/save/re-save/clear, DR24 import, DR25 print/export, DR26 whole-channel completeness/totals. DR28(seq155)/DR29(seq156) own the real Payment Batches host/tab; FC08(seq216) owns Accept payout projection. No early money command, host, import or export implementation is authorised by DR21.

## Exact current saved parents and reusable objects

| Object / seam | Current source / limits |
| --- | --- |
| `public.bpay_next_preparation_page_v1(uuid,text,jsonb)` | F04 finalPhysical/finalRoot `supabase/repeatable/12102026_0718_banking_pay_next_active_preparations_v1.sql`. Kind/allowed-key dispatch481ff, CHANNEL_WORKERS516ff, LINES662ff. Add worksheet branch here in successor order. |
| `private.bpay_next_channel_worker_page_v1(uuid,text,uuid,integer)` | `06102026_0236_banking_pay_next_channel_draft_readers_v1.sql:37`. Exact confirmed channel membership, run/channel equality before Candidate-ID keyset and bounded limit. DTO has worker/Candidate IDs, state and gross only; no frozen name/ref, deductions or net/pending/source. Do not extend its meaning silently or call it a worksheet. |
| `private.bpay_next_channel_status_page_v1(uuid,text,uuid,integer,integer)` | Same0236:94; qualifies exact channel then original worker status reader. |
| `private.bpay_next_worker_status_page_v1(uuid,uuid,integer,integer)` | Current finalPhysical `12102026_0442_banking_pay_next_run_review_totals_v1.sql:200`. Accepted projection join246ff uses worker projection pointer + non-retired row; pending is separate revision comparison, not newest-request money. Reuse this identity/stage rule. |
| `private.bpay_next_run_channel_contribution_v1(private.bpay_next_run_worker)` | `12102026_0442_banking_pay_next_run_channel_totals_trigger_v1.sql:5`. Completed DRAFT state, actual NET projection and indexed current request; existing maintained channel totals include missing/pending counts. Do not add a second live totals framework or sum loaded pages. |
| Case result/stage qualification | `12102026_0442_banking_pay_next_run_review_totals_v1.sql:77–108` shows exact instruction + captured preparation/selection + complete DRAFT or complete current NET result choice. Reuse those bindings, not nominal due as actual recovery. The REVIEW_CASES callable itself is a review-screen contract, not automatically a post-confirm worksheet call. |
| `private.bpay_next_net_projection` | `03102026_1324_banking_pay_next_core.sql:631`; unique(worker,projection_no), request_command_id, input_kind, entered_paye_net, accepted_recoveries/cash and retirement.0300 adds accepted_net_additions and CASE_PAYOUT. No generic projection-status column exists to copy. |
| `private.bpay_next_paye_net_request` | Core1324:828, unique(worker,request_no), immutable requested amount;0380 binds case preparation/selection/expected projection and PAYE_MANUAL/CASE_PAYOUT shapes. Request_no can have gaps in future replacement; never assume accepted+1 or count rows to recover a number. |
| Native frozen labels | Core1324 work_revision:158ff has captured candidate display/reference, client display, Timesheet reference, week ending; run_work joins its captured_revision_id. No separate captured surname field was established in this census: do not parse last words or consult today's Candidate surname as a frozen financial truth. Design must make the exact native stable label-order key explicit. |
| Frozen detail / actual used units | Existing LINES returns captured line/revision/component/date/channel and literal frozen subtotal. `12102026_0620_banking_pay_next_frozen_used_line_detail_v1.sql` supplies original Client origin and guarded approved unit/quantity/rate, via existing `public.bpay_next_frozen_detail_page_v1`. Use actual captured fields; never divide amount by rate or invent payable units from approved quantities. |
| FE typed transport/controller | `js/bpay-next-office.js`: CHANNEL_WORKERS rows194/request369; original confirmed channel opener2077; existing thin workers2768 and single-worker PAYE net2815. New worksheet schema/component is missing. |
| Native legacy view locations (context) | Current `js/main.js`: openBankingPayBatchPayeEntryModal90874, final renderBankingPayBatchPayeEntryModal92619, renderBankingPayBatchChildModalPayeWorksheetTab93537. They own old pay_batch_id/client-side legacy render semantics, not a NEXT run/channel DTO. The native window adapter/paging layout can be reused; NEXT must qualify before legacy reads and never fall through on a failed NEXT read. |

Historical installed evidence: checklist/R3 ledger records0575 SHA46791cabb… release06Oct08:22. It proves that old physical file at that checkpoint only. Current saved final parents above are authoritative for successor authoring; this read-only task did not query a current installed catalogue or claim latest TEST installation/runtime parity.

## Critical field distinctions — required before build

1. **Do not subtract gross recovery twice.** Existing0390 checks `captured_work_gross + allocated_gross_additions - allocated_gross_deductions = worker.gross_inc_vat`. Worker gross is already the payroll gross after DRAFT gross deductions. Supplied SCR57 shows earnings before recovery and a separate “earnings − recovery = gross to payroll” working. Reader must expose the native completed-DRAFT earnings/additions/deductions and payroll gross distinctly. Labelling worker gross as pre-deduction earnings and subtracting again is wrong.
2. Payment Advance repayment and non-taxable Manual Debt are NET-stage recoveries; Overpayment recovery and taxable Manual Debt are gross-stage. Split columns/captions by captured instruction `payroll_stage`/kind and actual allocation result, not current Case balance/nominal instruction. Use last complete accepted NET result for accepted recovery/cash, with DRAFT reserved recovery separately where needed. No reallocation or browser money formula.
3. **Accepted net is the last complete, accepted, non-retired projection.** Pending/refused input is a separate current immutable request, not worker's newest typed amount. Read accepted projection by exact worker revision; include current revision as expected_projection_revision. Derive the finite presentation statuses NONE/REQUESTED/APPLYING/ACCEPTED/REFUSED from actual existing command/job/NET-state facts. Do not pretend such a status column already exists.
4. Unknown net/cash/source is null/“—”; a saved literal0.00 is ENTERED_ZERO and prefill0.00. Empty editable boxes never prefill gross. A retained unsaved local value belongs to DR22's edit lifecycle; DR21 read-only view cannot manufacture it.
5. **Payout-only is not every zero-gross worker.** Native0390:84ff requires zero captured lines/gross plus an exact SEALED positive payout basis tied to worker/preparation and completed DRAFT net addition. Reuse genuine payout basis/classification. Include these rows in a mixed worksheet, exclude from applicable-net denominator, and refuse the whole worksheet when no applicable PAYE payroll worker exists. Pure Umbrella or payout-only Draft gets exact NOT_APPLICABLE truth, no fabricated payroll.
6. Source comes from accepted projection `input_kind` and, when delivered, actual import receipt. PAYE_MANUAL→Manual; PAYE_IMPORT→Sage import; CASE_PAYOUT has no payroll input source. Current0380 request checks do not admit PAYE_IMPORT and import_id/import_row_no are DR22/DR24 future additions: read real historical projection kind if present, do not add input writers/import metadata now or infer source from today's Candidate.
7. Detail is never per-Timesheet NET. Show exact frozen Client/week/type/unit/quantity/rate/subtotal plus frozen Case workings. If a native field is historically unknown, retain that unknown and record the supplied-picture difference. Approved full quantities are not proof of quantity of a partial payable difference.

## Bounded/current scope and safety design

- Exact request address `run_id` + literal PAYE, optional candidate_id filter, limit1–100 and opaque typed after cursor. All eligible workers must remain reachable. No first-page-only Candidate selector, all-run JSON, browser-loaded-page filter or client financial sorting.
- Cursor binds run/channel/filter/ordering and the reader's actual generation/revisions; key includes native frozen label/order and Candidate ID + run_worker ID tie-break, with explicit null ordering. Preserve original rows/scroll anchor while fetching Loading more…; context changes require a visible Refresh reason, not silent mixing.
- Existing run_worker index(core1023) supports run/Candidate ID order; it does not prove an indexed frozen-name or any monetary sort. Owner A3e/POLICY13 require design to state exact supported column sorts and native index/currentness before implementing this list. Do not promise sortable Gross/Bank amount with a page-local sort or unbounded computed all-run sort. Any supporting change must stay the exact worksheet reader/index ownership.
- Whole-scope denominator/entered/missing values must come from server authority and exclude genuine payout-only; existing channel totals are reusable evidence, but DR26 owns its completed display/execution gate. No “x of y” from retained page length.
- Service/admin/NEXT/actor and exact Draft-address admission remain the current public reader/Worker guards. No new command/job kind, financial admission fence, source producer, allocation owner, rule, table-sized payload or legacy fallback.
- Need2s reader usability and150-worker reachability after implementation, but this task ran neither timing nor business acceptance. Root's future local compile/source checks and later authorised in-app journey stay distinct.

## Read-only supplied design mapping / later surfaces

Exact inventory/index: **SCR57 PAYE Worksheet** B5 → T08 pattern only, owning DR21/DR22/DR25; read spec `batches/B5/specs/SCR-57_paye_worksheet_view.json` and individually viewed dark/phone PNGs of the same basename. SCR58 saved/window and SCR58b unsaved notice belong DR22/DR23; SCR59 net-change confirmation DR23; SCR60 replacement/refusal DR37; SCR63 totals DR26.

Required current read-only picture pattern: native wide title/header Close, Candidate filter over full scope, one table with Candidate caret only where genuine useful captured lines exist; Gross / Payment Advance repayment / Overpayment recovery / accepted PAYE net / literal Bank amount / Source / state. Expanded lines show Client/week ending/type/units/rate/subtotal and distinct gross-to-payroll / taken-from-net workings. Phone cards, labelled Sort by,44px controls, dark identical under device-light; modern-scoped native tokens, no!important.

Picture composite controls and human PB reference/date/count cannot be invented. Import Sage CSV belongs DR24; Edit/Save DR22; Print/Export DR25; Accept payout FC08; totals strip DR26; four-tab Payment Batch host DR29. A minimal native callable read-only worksheet can reuse F16 confirmed-channel context now; it does not prove later host integration or permit an empty button that claims unbuilt editing. D06/full ITEM must remain explicitly incomplete if required DR22/DR29 joins are not built; do not water down the required view by calling the old single net box equivalent.

Owner11Oct comparison applies after actual product captures: every visible deviation/control must be fixed or justified by card/native available data. Design viewing here is not A4 approval. Before payroll figures are coded, record earnings-versus-after-deduction labels and the actual frozen name/order authority.

## Future acceptance states to preserve (not executed)

Actual local screen review should cover missing vs entered zero, accepted Manual/import if native historical fact available, accepted old net plus pending new net, refused replacement, gross-stage versus NET-stage deductions including Manual Debt, one useful Candidate line expansion, mixed payout-only row and pure payout-only refusal, Candidate filter whose match is not on first loaded page, continuous loading/retained/final page, failed read/Refresh, changed context and phone final row. Use native schema-faithful minimal responses only; no invented PB reference/pay date/finance field.150-worker,2s, real save/import/clear, queue and actual batch tab journey remain later authorised acceptance, not proof from presentation samples.

## Saved-source hashes

| BE file | SHA-256 |
| --- | --- |
| 12102026_0718_banking_pay_next_active_preparations_v1.sql | d040516eb3ad9f488025493d6d91eaadc99cee780c22e09f7fddb7ecc3eff501 |
| 12102026_0442_banking_pay_next_run_review_totals_v1.sql | 8cbbd0d0bde4fc93c507ef1002a94b050dd3a85d64107ddf592b1e2db6fd978e |
| 12102026_0442_banking_pay_next_run_channel_totals_trigger_v1.sql | ecec108929c8c2492a8d104a7f648d420c70e754fe45761044fb8e3fb9c68a11 |
| 06102026_0236_banking_pay_next_channel_draft_readers_v1.sql | a90b3b2fe01e079e7177bedde583d5e0b66dd037b2344272a6a8fb288d008719 |
| 12102026_0620_banking_pay_next_frozen_used_line_detail_v1.sql | 96a26c83328dbb7253e8526c789dd11544d04d1b8a9dcf1b421e946fcd7dbc65 |
| broker/src/bpay-next-preparation.js | 80fdcf78fdb3c7e4110d976a7b1c7c7935e4d3ac7029b14176e5c84286b4c527 |

Hashes are this investigation's saved source checkpoint, not a fresh installed database seal. Root must re-read the card, registry and current parents at the actual DR21 build boundary because shared owners are still advancing.
