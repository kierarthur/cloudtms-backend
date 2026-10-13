# SCR-57 / DR-21 independent A4 review

12 October 2026. **Final A4 SOURCE/PIXEL PASS for the DR-21 read-only worksheet slice: 38 PASS, 4 scoped N/A, 0 FAIL.** All fourteen final native PNGs were opened directly and compared with the supplied B5 SCR-57 desktop and phone PNGs. The supplied full SCREEN_DESIGN_POLICY.md (all42 items), tools/policy.json wording rules, B5 spec/inventory/index, full DR-21 card and BD141 were read. Earlier findings below are historical and closed; they do not describe the final files. No supplied pack was copied/edited; no product edit, browser action, render, test, business RPC or database use by this reviewer.

The approval boundary is this native worksheet component. Actual DR-22 editing and DR-29 batch host remain later; D06/whole ITEM cannot claim those joins. BD141/A2 omits composite Edit/Import/Export/Print/Accept payout, batch tabs/reference/creation/totals and Updated time because the current reader/action/host does not supply them. Item25 and only the readerAdditions part of26 are scoped N/A by the instruction; actual named-field mapping in26 passes. Item16 is wording inspection against tools/policy.json as expressly directed by root, not an assertion that check-wording.cjs was executed. Joined/application, real150-Candidate scale, database first-use and timing acceptance remain **NOT_RUN**.

## Historical findings and their final closure

| Finding | Evidence | Required correction |
|---|---|---|
| P57-01 | All native captures repeat Close as a filled footer button; Office:2239 | Keep Close in the header only (SDP24/items5/10). |
| P57-02 | Chloe status `Net missing`; source:2176 | Use exact `PAYE net not entered` (SDP19a/63/66/items16/20). |
| P57-03 | Entered/Entered £0.00, pending, missing badges all custom neutral outlines | Reuse native candidate-office-badge tones: success for accepted/zero, warning for missing, info for applying, neutral payout; explicit refused danger. Items9/11. |
| P57-04 | Main desktop headings title case; child Used rate/Subtotal left-aligned | Universal uppercase11px headings; right-align monetary child values with tabular figures (item8). |
| P57-05 | Expanded workings `£1155.10` and `£1095.10` | Use required `£1,155.10`/`£1,095.10` (item21); no substring formatting that changes authority. |
| P57-06 | Main row keyboard handler:2226 supports Enter only | Add native Arrow/Home/End row navigation/focus. Item22. |
| P57-07 | Next-page busy notice generic `Loading…` below host; source:2234 | Existing bounded scroll needs `Loading more…` with role status; preserve retained rows. Item12. |
| P57-08 | Stale capture retains ordinary-strength figures; error uses two sentences | Grey retained figures and use one short error sentence with Refresh instruction; no zero/empty fallback. Items17/19. |
| P57-09 | Initial review challenged Case instruction count >1 | Closed by the owner's 11 October component-caret judgment: one plain Payment Advance/payout repeats the main row and has no extra caret; more than one Case or actual additional Manual Debt/other-repayment workings can expand. FC08 payout action is not built early. |
| P57-10 | Bethany expanded capture uses Amelia's £960.00 line although Bethany earnings £1,155.10 | Correct the minimal local reply to the exact expanded worker; this is a capture-data mismatch, not a requested financial owner change. Item39. |
| P57-11 | Phone repeats sort arrow in `CANDIDATE ↑` data label; Open visually below44px | CSS-only active sort arrow; actual phone action target >=44px. Items13/23. |
| P57-12 | Expanded phone PNG shows only Bethany's header at viewport bottom | Capture the actual visible child after scrolling it into view to verify labels/layout and button sizing. Do not claim phone expanded details from an offscreen child. Item23. |

P57-01–08 and10–12 are closed in the final source and actual pixels: header-only Close; exact missing-net wording and native five-tone badges; uppercase/right-aligned table fields; comma formatting in main amounts, workings and child money; row Arrow/Home/End/Enter paths; Loading more state painted before the read; grey retained failed-read rows and one-sentence error; exact Bethany frozen sample; CSS-only desktop sort arrow and phone Sort by; visible scrolled phone child and44px phone actions. Two final follow-ups are also closed: **Approved rate** truthfully names approved_unit_rate (not a newly calculated used rate), and the child money marker/header Close44px corrections were re-captured in all fourteen final files. No financial value or authority changed for these fixes.

## Visible differences from the supplied design

The following is the final difference ledger, after directly viewing the supplied and actual PNGs; each difference is justified within BD141/A2 or the native presentation rules.

| Final visible difference | Reason/boundary |
|---|---|
| Standalone **PAYE Worksheet** title replaces composite PAYE Draft · PB-0412-P, creation date/amount/Candidate subtitle and four batch tabs. | This is the callable native read-only component at the exact confirmed PAYE address. No unsupported batch reference/count/date or DR29 host is invented. |
| NET-entered/gross header facts, Updated time, footer148-Candidate/sort/count line and five summary-total facts are absent. | The owned worksheet response supplies rows, not those full-scope figures or a read time. A2 forbids picture-filling data and browser totals. Whole-list count/summary work is not silently claimed here. |
| Import Sage CSV, More, Edit PAYE nets, Print/Export and Accept payout are absent; header Close is the sole dismissal and the native empty lower frame band remains. | Their real readers/actions/hosts belong DR22/24/25/26/29/FC08. No inert control or second Close is supplied. The native frame is retained. |
| Explicit Candidate label, Name or TMS reference input, Search and Refresh replace the design's unlabelled search/Import/More strip. | Exact frozen name/reference server search and explicit read recovery are the present useful actions; Enter also submits the filter. |
| Native centered window is shorter for the initial/empty/error states and grows for detail; desktop wrapping/column widths, text weight, focus outlines and native heading/palette inheritance differ from the composite illustration. | Existing native modal/universal-table classes and tokens are reused. Eight real fields remain readable; uppercase11px headings, tabular right-aligned money, visible focus and native badge tones conform. No new palette or !important. |
| First chunk has3 fixed rows and the completed capture6, rather than the design's7 illustrative rows/148 whole-scope count; names/references, Client, Work date, units, rates and amounts differ. | Small actual-UI fixed replies demonstrate the owned shape, not business fixtures or full-scale acceptance. Bethany's exact reply is Southgate Hospital /11/10/2026 /Work /Hour /46.20 approved units /£25.00 approved rate /£1,155.10 retained subtotal. |
| Actual captured zero deductions/gross use £0.00 where the illustration sometimes uses dashes; missing net/cash/source remain —. | Typed real zero and unknown are distinct. Ivan's payout-only row does not invent the illustration's £300 bank amount. |
| Native small triangle caret replaces the boxed illustration caret; a pure one-component Payment Advance/payout has no repetitive caret. | Owner11 October judgment: genuine lines, multiple Cases, or additional Manual Debt/other repayment workings can expand. Future FC08 payout action is not added. |
| Updating… plus a separate £420.00 pending line replaces the illustration's PAYE net entry in progress; accepted £400.00/bank£400.00 remain current accepted facts. | Native approved plain state vocabulary conveys the current immutable input's status without treating pending as accepted. Entered, Entered £0.00, PAYE net not entered, Payout only and Failed retain distinct native tones. |
| Workings precede child lines rather than following them; the accepted-net deduction is a labelled amount instead of an uppercase footer sentence. | Plain card-required frozen gross-to-payroll and NET workings remain visible and qualify their stage; no per-Timesheet PAYE net. |
| Child has Unit, **Approved units**, **Approved rate**, actual native Type and one captured line rather than the illustration's Day/Night/Mileage rows and generic Units/Rate. | These names map to actual approved_line fields. Subtotal is frozen_ex_vat, never calculated from quantity×rate. No Day/Night/used-charge inference. |
| Conditional taxable/non-taxable Manual Debt, other repayment and addition workings and Open Timesheet detail exist beyond the illustration. | These are genuine DR21 deduction/line facts and the existing guarded five-view detail action. No new command or reader to fill a picture. |
| Phone has labelled vertical cards, explicit Search/Refresh, a Sort by select with ascending/descending words, an extra Open control,44px actions, and scrollable child detail; no composite edit footer. | Same native whole-server sorts and row expansion remain accessible without horizontal scrolling. Hidden desktop heads are display:none, not offscreen interactive targets. Phone geometry follows the real native frame. |
| Loading skeleton, empty message, first-read failure and grey retained stale rows are additional state captures; the supplied decorative SCR57 annotation is absent. | Required state handling is real native UI, with one-sentence recovery notices before the toolbar. Design annotations are not product copy. |

No final unexplained visible difference remains. This ledger approves presentation of the scoped reader; it neither adds the omitted future capability nor changes its later ownership.

## Every current control's purpose

| Current control | Purpose |
|---|---|
| Header Close / Escape | Safely dismiss this read-only native child and return focus to its parent; no unsaved financial change. |
| Candidate input / Search / Enter | Apply frozen scoped name/reference filtering to the whole server list. The input itself is not an all-Candidate directory. |
| Refresh | Explicitly reread the same worksheet address/filter/order after a failed or changed read. |
| Eight desktop heading buttons | Sort the full reader scope, toggle direction, and expose the single active CSS arrow/aria-sort. |
| Phone Sort by select | Choose the same sixteen key/direction combinations; Candidate ascending is the visible default. |
| Row click / Arrow Up/Down / Home/End | Focus/choose the native row without mutating payment. |
| Caret / Enter / double-click / phone Open | Show or hide actual frozen lines and useful Case workings for that exact worker. Phone Open is the accessible labelled equivalent of its caret. |
| Main scroll region / upward retained navigation | Read native subsequent/previous retained cursor chunks continuously; no page buttons, all-page array or browser sort. |
| Inner Payment lines scroll region | Read bounded line continuation in its native business order. |
| Open Timesheet detail | Open the existing exact captured five-view detail, only with its original READY/DRAFT guard. Unsupported later/refused child joins are not fabricated. |

No current control authorises, saves, imports, executes, accepts payout or recalculates payment. The renderer retains at most3 worksheet pages and30 child lines; original context changes refuse a mixed read instead of merging snapshots (Office:2179–2198,2233–2235).

Every visible datum maps to the actual own contract: candidate_display_name/reference; earnings_gross, advance_repayment, overpayment_recovery, accepted_net, pending_net, cash_amount, input_source, projection_status, net_state, payout_only; gross_stage_deductions/payroll_gross, gross_manual_debt/net_manual_debt/other_repayment/net_recovery/net_additions and deduction_basis; LINES client_display_name/week_ending_date/component_kind/unit_label/approved_quantity/approved_unit_rate/frozen_ex_vat. Text/format mappings do not calculate financial values. Internal UUIDs/context/cursors never appear on screen.

## Final 42-item policy checklist

38 PASS,4 scoped N/A,0 FAIL. Items1/2/26 pass for the exact read-only component described above, not the still-open DR22/29 integration. Item16 follows the explicit reviewer instruction to inspect wording against tools/policy.json; no checker execution is claimed. Item41 is source/pixel robustness evidence, not a timed/hundreds-of-Candidates acceptance claim.

| # | Result | Evidence/reason |
|---|---|---|
| 1 | PASS | Owned read/filter/sort/expand/net states/workings reachable; P57-09 owner judgment applied. Later D06 editing/host remain open. |
| 2 | PASS | View/filter/expand retained; future editing/import/export host not claimed (BD141). |
| 3 | PASS | PAYE net not entered, Entered £0.00, Payment Advance repayment and plain pending/stale wording present. |
| 4 | PASS | Read-only captured facts; accepted and pending money remain separate. |
| 5 | PASS | Existing native title/frame; only header Close, no invented subtitle or duplicate dismissal. |
| 6 | PASS | Standalone owned window; no extra tab tiers. |
| 7 | PASS | Simple toolbar/notice/table; no nested bordered-card stack. |
| 8 | PASS | Native universal sticky uppercase heads; desktop main/child money right-aligned/tabular; phone labelled cards. |
| 9 | PASS | Native candidate-office-badge success/zero, warning/missing, info/updating, neutral/payout or pending, danger/refused; text retained. |
| 10 | PASS | Read-only component has no save/execute next step; header Close only, useful quiet Search/Refresh/row actions, no redundant primary footer. |
| 11 | PASS | Existing modal/table/badge classes and tokens reused; scoped layout only, no new palette or !important. |
| 12 | PASS | Continuous native cursor scroll; Loading more… role status painted beforeawait; no page/Load more controls. |
| 13 | PASS | All8 desktop columns server-sort, one CSS arrow/aria-sort; phone Sort by states Candidate ascending and all directions. |
| 14 | N/A | Read-only worksheet has no selection ticks. |
| 15 | PASS | No aggregate count/total is invented; unsupported composite totals omitted A2/BD141. |
| 16 | PASS | Exact policy.json never-show/chatter/20-word/two-sentence/UK-format rules inspected; owner one-sentence errors. No unobserved checker run claimed. |
| 17 | PASS | First failure and changed-list notices are one plain sentence with Refresh instruction, inline before the toolbar. |
| 18 | N/A | No mutation or confirmation in this read-only slice. |
| 19 | PASS | Actual skeleton/empty/first-failed/stale PNGs; failed read keeps rows grey, never invents zero, first failure says no worksheet loaded. |
| 20 | PASS | Payment Advance consistently named; missing-net approved phrase, no technical IDs/codes/loan/NEXT/frozen labels. |
| 21 | PASS | Main/workings/child £1,155.10 and £1,095.10; dates11/10/2026; unknown— distinct from real£0.00. |
| 22 | PASS | Native Tab/Enter/Escape and Arrow/Home/End source paths; focus visible in PNGs, labelled regions/carets, aria-sort and row selection. |
| 23 | PASS | All4 final phone captures viewed: cards/Sort by/no horizontal scroll, visible child,44px Close/Search/Refresh/Open/detail/carets; pixel metadata confirms. |
| 24 | PASS | All3 actual desktop/device-light pairs viewed and byte-identical; dark native appearance retained. |
| 25 | N/A | Actual native screenshots/fixed responses, not new generated design tooling; owner-scoped instruction. |
| 26 | PASS | Every actual value maps to the named own reader fields listed above; conditional Manual Debt workings card-required. Only readerAdditions subpart N/A. |
| 27 | PASS | No pre-execution pay date. |
| 28 | PASS | Plain approved payroll labels, exact amounts, Approved units/rate and qualified gross-to-payroll workings. |
| 29 | PASS | Search/Refresh near filter; row carets/phone Open next to affected Candidate. |
| 30 | PASS | Read-only check has direct filter/row detail actions and safe header Close; no false next save/execute step. |
| 31 | PASS | Each Candidate shows earnings/net/bank amount/source/status; no money inferred. |
| 32 | PASS | Distinct missing/updating/pending/refused states and inline Refresh refusal explain the present issue; later net edit actions are not inertly offered. |
| 33 | PASS | Read/filter/expand results remain in the worksheet. No financial action. |
| 34 | PASS | All controls have a purpose above; composite/future facts and duplicate Close omitted. |
| 35 | PASS | PAYE net, Payment Advance, Manual/Sage import and Timesheet detail use the established terms. |
| 36 | PASS | No money-changing actions; future editing not presented inertly. |
| 37 | PASS | Header Close/native Escape; no unsaved data. |
| 38 | PASS | Direct filter/sort/expand without rebuilding or an unrelated viewer. |
| 39 | PASS | Actual Bethany£1,155.10−£60.00=£1,095.10; accepted£884.20−£150.00=£734.20; exact child retained£1,155.10. Pending£420 not accepted£400; no fabricated totals. |
| 40 | N/A | Already confirmed Draft; Create Draft selection confidence is outside this window. |
| 41 | PASS | Source bounds3 pages/30 child lines and exact-context refusal; all14 actual state/mode captures plus keyboard source path inspected. Real150-Candidate load/performance NOT_RUN. |
| 42 | PASS | PAYE Worksheet title, exact captured Candidate rows, visible missing/pending states and notice-first stale/failure copy orient returning users. |

Final verdict: **A4 SOURCE/PIXEL PASS for this read-only component.** No unresolved source/visual finding remains in the reviewed bytes. DR22/DR29 integration, joined/application/business acceptance and timed scale checks remain **NOT_RUN/open** as applicable; this receipt does not mark the whole item complete.

## Final raw SHA256 evidence

Source read from the current frontend worktree; local fixed responses/requests/phone metadata are capture evidence, not business execution. All14 PNGs below were viewed after the last fixes. Normal, expanded and last desktop/device-light pairs are independently viewed and byte-identical. The supplied B5 reference PNGs were also reopened directly for the final comparison.

| File | SHA256 |
|---|---|
| FE js/bpay-next-office.js (final committed bytes) | 8B519C661005B7B996381F18AAB230C30A4B2C4709065374E1514C1741941072 |
| FE css/bpay-next-office.css | 9C02014E021338890CD059C8D19278B7BB0501A274C57B7DB1C41B78FDDFF910 |
| design/DR-21.md (BD141) | AA9B7EE64242E46284CDD5F860C0BC0D6C71939B83881D5FC79067CFF32F59A3 |
| DR21-fixed-responses.json | 3AB360DE36806EDA6A086FDFE5E9B95AACE3D659A49182FA16B6FFB169E1A275 |
| DR21-local-requests.json | 00F16102CADC9CD3D8A6287935008484CFE47C7F148C0AB9AEC0F18933E394B3 |
| DR21-phone-control-pixels.json | E2D177A3064C0DD8EF7176E4A6A30E65E81D0388506B58830552F167D32AA8DD |
| DR21-worksheet-dark.png | B0162D2CA4234155B6B8C9DAB0B834049F87B18C94516FB05369456F3F524B9F |
| DR21-worksheet-device-light.png | B0162D2CA4234155B6B8C9DAB0B834049F87B18C94516FB05369456F3F524B9F |
| DR21-worksheet-phone-dark.png | 82F1807830A33A98FC2050237CD84A1484642C57C203869D0F14E949B4068B39 |
| DR21-worksheet-expanded-dark.png | 1ECB1B68D16159EA876FC6F5A3053CA63A2272A52B524179D4570AB493881BAA |
| DR21-worksheet-expanded-device-light.png | 1ECB1B68D16159EA876FC6F5A3053CA63A2272A52B524179D4570AB493881BAA |
| DR21-worksheet-expanded-phone-dark.png | BA65F7B48762E361456123F22724C8F5BC411CEF6BC0F71EE6B4533840E747AB |
| DR21-worksheet-expanded-detail-phone-dark.png | 57E290F7AB409C6888A7BC63E212760CD3BD9828FEDE759F86FB51FAD1537049 |
| DR21-worksheet-last-dark.png | 3F71D7C1E1A86F03E21B56FA4F6777E6104B40EDE14B150E02A7C506BE413B9A |
| DR21-worksheet-last-device-light.png | 3F71D7C1E1A86F03E21B56FA4F6777E6104B40EDE14B150E02A7C506BE413B9A |
| DR21-worksheet-last-phone-dark.png | F72922A8992E1C8D64972A60947202D6D2D7DB36174CAFEEA701159A7F2BCECF |
| DR21-worksheet-stale-dark.png | AB1EEBFB5079299A948084054A7A81A2B91FDAEB267432E19F7D4871902AAD7A |
| DR21-worksheet-loading-dark.png | 098201862A7BEFA2E35FDDEFF6EEE25386A49C5993926ABAE1E9B71D18FBBC75 |
| DR21-worksheet-empty-dark.png | EE034C73A3B870297B05641BB134F0E23830187C9649B4D34A2014E1A6033BE5 |
| DR21-worksheet-first-failed-dark.png | AF7D1534FF3CCA56537E9EDFF182ACF5D82814262487C84678423701203BF026 |

Final frontend commit independently read: **ccb8d6e17093436c6dad4189ee10e93ec072568a**. The only post-capture Office byte change removes two spaces from otherwise empty line12. A read-only in-memory reconstruction that restores exactly those two spaces reproduces the reviewed/captured Office SHA256 **EB1D9FBB4027424814970C80EF23C992AE0C3FD3DBA24B60EE9F1DB89ECA3616** exactly. No semantic or rendered difference; final SOURCE/PIXEL PASS remains valid. CSS/hash and fourteen final images are unchanged.
