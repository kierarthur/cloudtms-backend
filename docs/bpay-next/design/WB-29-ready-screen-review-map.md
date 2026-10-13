# WB-29 chosen Ready screen review map

Read-only preparation, 13 October 2026. Root authors product, fixed responses and captures. No product, fixture, harness or design-pack edit, execution, SQL/RPC, browser, hosted or business action was performed here. Future actual-screen A4 and joined/runtime acceptance are NOT_RUN; whole WB-29 remains open.

## Supplied comparison references

Pack root: `C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/cloudtms-backend/codex_outputs/claude-stage2-continuation-20260925/SCREEN_DESIGN_POLICY_20261010`.

| Actual family | Read-only supplied references | Required comparison |
| --- | --- | --- |
| Main Banking Pay Ready | png/T01_ready_list_dark.png, png/T01_ready_list_devicelight.png; specs/T01_ready_list.json; phone companion png/T06_phone_ready_phone_dark.png | Native Ready list, authorised amount basis, Candidate/reference, original selection/open pattern, quiet Export and primary Create Draft. |
| Candidate chosen Ready groups | png/SCR-09c_candidate_rate_set_ready_dark.png; specs/SCR-09c_candidate_rate_set_ready.json; T02 desktop pair | Literal chosen ex-VAT group amount and native target channel; informational rate-set badge beside payment name; Candidate identity, selection, sort/order and existing footer. |
| Expanded chosen component/shifts | png/T02_candidate_window_dark.png, png/T02_candidate_window_devicelight.png; specs/T02_candidate_window.json | Useful native day/shift facts, genuine single-shift shortcut only when complete, no repeated parent row, literal chosen component/leaf amount cells, existing inline controls. |
| Existing Pay lines, if actually opened/touched | png/T02e_pay_lines_dark.png; specs/T02e_pay_lines.json | Original/suggested native rate facts and fixed bucket order; no extra total or invented unit/rate/charge. This existing child is not needed solely to prove Ready amount presenter changes. |

The four main desktop PNGs above were individually viewed, alongside their specs/index/inventory. SCR-09c explicitly documents replacing the authorised PAYE amount with chosen Umbrella ex VAT and clearing the badge after Draft admission. Its supplied badge tone is informational blue. T02/T02e do **not** require an additional rate-set badge on expanded component headings; the DTO-presenter note's component-badge suggestion is not a supplied-picture/card requirement. Do not add one on that basis.

The current native reader still owns Source Candidate/header totals. Explicit “as authorised” labels on the main/header/Candidate summary and main amount heading distinguish those totals from chosen Work group amounts; a new chosen aggregate or equality between those totals is not available. Record this necessary basis clarification as a design difference, together with the tiny sample's actual counts/values. Existing owner 11 October caret, inline action, phone Open, no redundant parent, secondary Export and footer primary rules remain binding. Picture-only pay dates, extra channels/amounts, rate×hours computations or missing-reader facts must not be introduced.

## Smallest actual local setup

Reuse only the native index/static-file routing and actual Office/standard-window adapter setup from `.codex-local/WB27-owner-row-screenshots.cjs` or `WB26-screenshots.cjs`. Both restrict the loaded origin to local static FE files, substitute capabilities/workbench/preparation reads, remove only the local login overlay and call actual CloudTMSBpayNextOffice.open with the native adapter. Root must retain blocked nonlocal networking and reject unsupported API paths/mutations.

Their old Case-oriented responses and exact button name “Expand” are obsolete for this slice. Use the Work sample/SHIFTS/BREAKS construction in `.codex-local/WB25-screenshots.cjs` and `screen-designs/SCR-10/WB25-fixed-read-responses.json`, supplemented by current closed DTO fields. One Candidate, one current Work, one chosen taxable component with two actual segment/shift identities and one unchanged component is sufficient. Choose fixed literal server values that visibly distinguish authorised Source money from chosen money; do not calculate them in Office or claim the sample proves quotation correctness. No Case data, estimate, pending command, funding, bank details or new API is required for these screen comparisons.

Use actual scope/currentness on every read. Main load requests HEADER for READY/ACTION/BLOCKED and, for ALL channel scope, READY per PAYE/UMBRELLA. Candidate load also requests its own section headers and CANDIDATES count/selection basis. Return empty nonapplicable sections with their correct closed zero/null values rather than omitting keys. Native WORK_JOB_PROGRESS can be complete with no jobs. Preserve current capability owner qualification and actual actor/agency identity; use matching context revisions/date on every reply.

## Response requirements

| Read | Required actual response facts |
| --- | --- |
| All workbench kinds | Exact current envelope: ok/kind/workbench_revision/complete/context/rows/next_cursor/page_receipt/affected_candidate_count; context agency, actor, owner epoch, official date, workbench/adoption revisions, kind, exact scope and selection_memory. page_receipt has the same context, actual start/end keys and decimal-string row_count. Complete tiny pages have next_cursor null. HEADER additionally supplies the native job_progress envelope. |
| HEADER/CANDIDATES | Existing complete closed DTO and count/selection-memory fields, actual Candidate name/reference and sort identity; their amount is authorised Source money. No new chosen fields belong here. Keep PARENT financial-view selection basis and original counts coherent with the small sample. |
| CANDIDATE_GROUPS | Existing complete WORK group DTO, actual Work/current revision/physical Timesheet identity, complete true/dirty false/CURRENT, READY/selectable and no accepted tuple; literal chosen native group_amount. New rate_set_pay_channel is explicit PAYE or UMBRELLA only when qualified, equal to target_pay_channel; otherwise explicit null. Existing WORK selection basis remains the Source basis, including native position/count/book/target operands. |
| GROUP_LINES | Existing complete member DTO and memory basis, exact Work/revision/approved-line/segment/member keys, section/portion/progress, Source money and component-known amounts. The new chosen_component_ready_amount, chosen_leaf_amount and rate_set_pay_channel are all nonnull only for qualified unselected taxable READY selectable PAYMENT AVAILABLE/ZERO rows with known component/leaf amounts; otherwise all explicit null. Same component's nonnull chosen component/channel facts agree. Source selection basis/amounts remain unchanged; chosen zero must remain zero through nullish fallback. |
| SHIFTS | Existing preparation-page envelope with complete true/next_cursor null. Retain shift_detail_id/detail_no/work_date, actual local/time fields, overnight, submitted/approved minutes/hours, detail_label, native bucket hours, segment_pay_ex_vat/pay_excluded, approved_segment_id/segment_key and job_title/band_label. Match approved-line and exact native segment/detail identity. Root's local sample selector key approved_line_id is stripped before returning the closed SHIFTS row. |
| BREAKS | Same preparation envelope; actual break_detail_id/break_no/start/finish local or timestamp fields/break_minutes. Root's local selector shift_detail_id is stripped before returning a BREAKS row. A complete empty breaks read is valid and shows the native dash. |

The actual expanded renderer performs a fresh GROUP_LINES exact-item point read after each SHIFTS/BREAKS read. The harness must honor candidate_id/group_key/section/item_id, return that same revision/member and identical selection memory, and preserve exact GROUP_LINES UUID ordering/page_receipt. Segmented SHIFTS requests use after_detail_no = shift_detail_no − 1 and limit 1; filter/slice accordingly. A wrong “all shifts” response would correctly fail this native guard. Full component paging must be complete from its beginning before the shortcut can combine a one-shift day. Do not remove any guard to make a screenshot work.

For an optional existing Pay lines capture, supply current COMPONENTS and RATES from the native read schema using the same approved-line identities and original facts. This is not a request for new chosen Rate Details fields or a new reader.

## Capture and acceptance map

Proposed current files, all under BE `docs/bpay-next/screen-designs`:

| Scope | Proposed prefix | Capture state |
| --- | --- | --- |
| SCR-04 | WB29-ready-main-{dark,device-light,phone-dark}.png | Main Ready strip and Candidate authorised amount heading/value. |
| SCR-09 | WB29-ready-chosen-{dark,device-light,phone-dark}.png | Candidate group chosen amount, target channel and informational rate-set badge, plus unchanged summary/footer. |
| SCR-10 | WB29-ready-expanded-{dark,device-light,phone-dark}.png | Actual caret expansion with chosen component and attributed shift values, unchanged control/component and useful native shift facts. |

These nine base PNGs are the minimum full-mode matrix. Add phone scrolled captures only when the chosen leaf money or native controls are below the viewport. If the existing Filters count label is changed in this slice, capture its small native child in the three modes too. Additional channel/zero/unavailable examples are necessary only for a distinct changed visible branch that the base sample does not show; do not broaden this into a financial test programme. A read-only null-chosen control already exercises original Source fallback without a false rate-set badge. Do not dispatch selection, financial or export commands merely to produce these images.

Use desktop 1600×1000 dark and device-light plus 390×844 phone-dark, actual saved FE index/modules/CSS and existing native frame. After actual row load, double-click/Enter the Candidate; use `[data-next-focus-key="candidate-expand:WORK:<actual work id>"]` for the payment caret, not an exact “Expand” label. Wait for native detail/loading status and factual reads to settle. Capture any needed phone scroll through the existing actual list, not forced layout or invented controls. Persist the tiny fixed responses, actual local GET request log, source identity and final PNG hashes after root's last replacement capture.

Future A4 must inspect every final PNG beside the supplied reference, list every visible difference and every control's payroll purpose, and score all 42 policy entries. Apply item 16 manually against tools/policy.json wording; owner exceptions make item 25 and item 26 readerAdditions N/A. Items 23/24 and user-eyes checks require the actual final pixels, including device-light matching dark appearance, phone 44px targets/no overlap, amount grouping/alignment, original footer and no redundant details. This note does not preapprove them.

## Preparation provenance

WB27-owner-row-screenshots.cjs SHA-256 ADFEBB4E2335D5A0E09D33A06C609BC6669223E9BED6FAFBD656C9744448CC4F; WB26-screenshots.cjs 10C363402672B9CB4D299920609470C8605B7609743E68AC7F7E498D6BEE5FE2; WB25-screenshots.cjs FA2C5EB0EF9BB9EEA44E47A2CDF05FDA7D1E1D450CE11B1EEA00BB76ACF6AB10. Current WB-29-ready-DTO-presenter-map.md SHA-256 74938B58DE216C81937E17F91D609BD1419628E52922BAC1F7685B3A23AFAFB0. FE presenter edits are in progress; final screen receipts must bind the later frozen source and actual capture bytes rather than this preparation checkpoint.
