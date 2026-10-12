# DR-17 screen investigation and supplied-design comparison

12 October 2026; sequence123. Independent read-only investigation, not a build or A4 verdict. Read the complete approved card `03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:449–471`, all D01–D08/ITEM (`IMPLEMENTATION_CHECKLIST.md:3617–3639`), current registry, R2 cutoff ownership, and the supplied SCR47/SCR54 designs. Root remains the sole product author. No product/spec/pack edits, tests, SQL/database/business calls, renderer, capture, or hosted action were performed.

## Narrow complete boundary

Reuse the existing frozen route and native `openFrozenWorkerDetail` child. The current child already has Lines, Shifts, Breaks, Used rates and Available rates, bounded continuous scrolling, exact failed-read retry, captured Work subject, retained selected Shift, and native Close/parent restoration. The remaining owning screen work is the card's callable five controls from the actual review Candidate context, the precise frozen Source/protected explanation, and its later DR-31 call seam. Do not rebuild the renderer, calculator, capture pipeline, payment tree, or History reader.

The requested `js/bpay-next-window-host.js` is absent. The actual reusable host is `js/bpay-next-ui.js:32–75`, `CloudTMSBpayNextUi.createStandardWindowAdapter`; real main callers create that adapter (for example `js/main.js:7233`). It preserves the original modal stack, dark standard frame, header Close/Escape, mounted content, focus, and guarded dismissal.

## Existing callable surface and exact seeds

`js/bpay-next-office.js:1700–1785` implements and exports `openFrozenWorkerDetail(options, workerId)` (export2747). It requires native `detailRunId`, `detailCandidateId`, worker UUID, current actor and NEXT owner. The current review READY row offers Draft detail, double-click/Enter, and the phone Open action (`2156–2172`); its current Candidate caret opens Case facts, not the five frozen controls. Therefore the existing READY detail button alone does not establish D06's named review-expand controls.

The smallest addition is a finite initial-view seed on this same child, defaulting to Lines for every existing caller, plus the five named review controls. Keep each seed qualified by the actual run/Candidate/worker tuple; a particular line/Shift/Work must come from the existing native returned row, never a constructed UUID. Non-Line views remain explicitly for the selected frozen line/Work. A Candidate seed may discover its first native line through LINES, but must not present that line's shifts or rate schedule as an aggregate across every Work. Keep a visible original Work/Timesheet subject and the existing way to return to Lines/select another line. Preserve current READY and run-state admission; no new permission or live-source fallback.

The existing late-read check binds `origin.revision_id` to the selected line's `captured_revision_id`, keeps the original origin tuple equal, and rechecks current actor/owner/namespace before attaching results. Preserve these checks, bounded three-page retention/32KiB history, selected Shift identity, independent STATUS-total error recovery, and exact failed-read arguments when adding an initial tab or direct caller. The new controls must not create an all-Candidate details array or eagerly fetch all tabs.

## Closed native reads and one essential label gap

| Read | Native input/cursor and present facts | Required treatment |
| --- | --- | --- |
| LINES | worker UUID; `after_line_no`; native line/revision/Timesheet identity, component kind and captured monetary difference | Reuse; no quantity or rate reconstructed by dividing amount. |
| SHIFTS | selected run-line UUID; `after_detail_no`; original date, clocks, overnight, approved quantities and bucket quantities | Reuse; approved quantities are not paid hours. |
| BREAKS | selected run-line + exact returned Shift UUID; `after_break_no`; genuine clocks or recorded minutes | Reuse; length-only clocks stay a dash. |
| RATE_DETAILS | selected run-line; `after_bucket`; approved bucket hours and actual source pay rate | Reuse fixed Day/Night/Sat/Sun/BH order. No actual used charge rate is provided. |
| RATE_SCHEDULE | exact run-Work; `(after_rate_family, after_rate_code)`; all stored PAYE/Umbrella/charge cells, unit/family/code | Reuse; every genuine available family remains visible, including all-NULL cells. |

Sources: Office closed schemas `195–196,340–351`, transport `863–876`, and reader calls `1728–1746`; public frozen owner `11102026_1005_banking_pay_next_frozen_client_origin_v1.sql`; natural-key private readers `04102026_0025_banking_pay_next_frozen_detail_pages_v1.sql`. Requests are POST-only/admin/NEXT, limit1–100. Private line/schedule guards require parent REVIEW/DRAFT and worker READY. Historical state-independent detail remains with its own owner; do not widen this guard to serve History/paid/cancelled.

**Essential existing-boundary addition:** current `FROZEN_ORIGIN` contains captured Client/Daily/revision labels but no typed `source_kind`. DR17/SP22 explicitly requires the frozen Source/protected explanation. The smallest support is to expose the already immutable `v_revision.source_kind` through existing `private.bpay_next_payment_client_origin_v1(uuid)` and the current frozen wrapper/closed Worker+Office origin schemas. The native revision enum is ORDINARY/SOURCE/PROTECTED/EXPENSE/ADJUSTMENT; no producer change or inferred classification is necessary. This is whole-revision classification, not proof that every Shift in a mixed common HEAD is protected. Preserve the selected line's existing explicit `PROTECTED_WORK` classification; never stamp all shifts Protected pay from a whole-revision Source kind or today's Source flags. If a supplied per-line badge cannot be positively bound by the actual returned native fields, omit that badge rather than invent it.

## Supplied designs inspected and actual-data limits

Individually viewed all five B4 desktop PNGs, the SCR47 phone PNG, and B5 SCR54; read all five matching B4 specs and the SCR54 spec. `DESIGN_INDEX.md:62` maps SCR47 jointly to DR17/WB20/SP22/DY05/CM19; line69 maps SCR54 to DR31/DR17/XD06. Pack root is the ordinary backend `codex_outputs/claude-stage2-continuation-20260925/SCREEN_DESIGN_POLICY_20261010`.

- SCR47 Lines supplies Timesheet/Client, line bucket, approved hours, rate, amount and a gross total. Current LINES supplies component/date/amount, not every pictured quantity/rate/label or full-scope count. Keep the existing truthful fields and native STATUS totals; do not invent missing detail or add a summary by summing loaded lines. Add the approved-quantity caveat where these quantities are displayed.
- SCR47b supplies Client/Contract name, role/band, clocks, break, approved hours and Rate. Captured Client/role/band/No Contract (Daily), clocks and approved quantities already exist. Contract display text and an aggregated break value are not provided by current origin/Shift DTOs; the genuine recorded breaks remain in their own view. A2 forbids creating these solely from the picture or guessing a bucket for mixed hours.
- SCR47c's all-Shift display becomes the existing selected-Shift break view with an exact native Shift chooser; document that scope. Duration-only records show unknown clocks, not fabricated start/end.
- SCR47d includes a used charge column. RATE_DETAILS returns only actual source pay, so omit unavailable used charge; do not use available schedule charge as a substitute. Fixed business bucket order remains.
- SCR47e already matches the three separate available-rate columns and NULL dash behavior. Additional/mileage families required by the card remain native reader facts where present, even though the supplied picture shows Standard only. No cross-channel conversion or second pricing owner.

These are reader/card limitations under owner A2, not approval to manufacture picture-only data. All visible differences and control purposes require the later actual A4 comparison. Use the existing dark native frame, 980px detail sizing, phone wrapping/44px targets, labelled scroll region, right/tabular numbers, plain one-sentence errors, and no footer duplicate Close.

## Sorting and later DR-31 seam

SDP34 (`SCREEN_DESIGN_POLICY.md:172`) explicitly exempts dialogs, expanded child rows and fixed business bucket order. SCR47 is indexed as dialogs; this narrow existing child can retain natural line/detail keys and bucket order. Do not add fake sort arrows or locally sort loaded arbitrary rows. If the implementation instead introduces a whole-Candidate/whole-Draft sortable list, owner A3e requires the actual displayed data columns' server sort/keyset/WB10-index support at that touch. The supplied Lines/Shift/Break sort arrows alone do not authorise this broader reader work.

SCR54's beneficiaries/Candidate→Week→Client→Timesheet tree, aggregate counts, Expand all, row retry and its server sorting belong to DR31. Record a call seam now: its future exact native run/Candidate/worker and selected captured line/Work can call this same exported child with the finite initial-view seed. Preserve original Draft-address qualification and do not build DR31's tree/table/commands early. D06's DR31 integration acceptance remains a named dependency until that actual host exists; the full card is not complete merely because a callback is callable.

## Current owner chain and saved-byte anchors

Current final registry: frozen public reader and Client-origin helper `11102026_1005`; immutable-effect function `03102026_1342`; four UPDATE/DELETE and TRUNCATE attachments on run_work/run_line `08102026_0718:42–60`; capture and fresh-positive hold `12102026_0005`; ordinary publisher `11102026_1001`. Retain these complete current parents. Card D03–D05 are shared-owner evidence, not authority to reinstall old1610/1620 or reopen parked F25. Any canonical capture-family omission is recorded for the designated WB29–WB35 owner, as card463 directs; it is not reimplemented here.

| Saved source | SHA-256 |
| --- | --- |
| FE Office | `c2a15bc9cf598dc61e007ac89ceb97a5f3aa87fe5ced38ef58c761e9199e0246` |
| FE native adapter | `f535e7e75579428ca7b97b57d406ccaeadaebbf0fe15e5935574f435b772248d` |
| Current frozen public/origin owner1005 | `ccb57080d3119372be6408c1ced497c95783eb06521d2727d4be3fce4436ea53` |
| Original private frozen readers0025 | `d7433fd91345a02ea75a596577c66514465c007d622dd0d225ca7edf96708ce9` |
| Current capture/hold0005 | `df49b9fc467ae9806effb19ec3b39820059fa46980a2f761f62c3f2ef8a96111` |
| Design index | `cba0ef6e87c8937d558b20ef9c9ee010fcc9f9a8b479b626351206d07076cffe` |
| Screen policy | `8bc212b174469236f2b46dbf70eadc3664a946636458321cd3c77b7146323de5` |

Actual unchanged-after-edit frozen acceptance, immutable-trigger refusal, mixed ordinary/Source/protected/Daily and both channels, History independence, all-family capture, large-list behavior and actual A4 remain NOT_RUN here.
