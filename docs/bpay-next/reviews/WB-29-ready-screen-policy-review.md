# WB-29 chosen Ready display — independent A4 review

Verdict: **A4 PASS / APPROVED — implemented chosen Ready display slice; 37 PASS, 0 FAIL, 5 scoped N/A across all 42 policy items.** All 15 final actual local PNGs were individually inspected. P29-01–07 and D29-01 are closed. The final receipt below supersedes the preserved chronological checkpoint. Whole WB29, business actions, joined-app, database, command and scale acceptance remain open / NOT_RUN.

## Findings saved immediately

- **P29-01 — normal-state capture dependency failure.** `SCR-09/WB29-main-dark.png` and the main phone image show “Could not check payment Drafts; refresh and try again.” above the Ready facts. This is a genuine native failed read, not the intended normal chosen-Ready state. Supply the exact existing bounded Draft-list/context reply in the local capture; retain the product guard/error behavior. Re-capture the normal main views. Source error anchors: Office 2714/2723.
- **P29-02 — phone Payment value splits into the label column.** `WB29-candidate-phone-dark.png` places `TS 6111` under the left PAYMENT label while its rate badge is on the right. The renderer appends payment name, reference and badge as separate direct children of the same td (Workbench around 1530). Keep these factual children in one value wrapper so the phone label/value adaptation preserves their association. No extra badge or field is required.
- **P29-03 — Candidate table/expanded nested framing differs without a card need.** The Candidate outer table has square, unframed edges; expansion has a blue bordered wrapper, padded day panel and separately padded/bordered shift tables. T02 uses one native table host and flat day/shift rows. SDP-17/18 require no card inside card and one table-host border. Reuse the existing universal host/frame and flatten the extra shift frames, retaining all reader facts and controls. This is presentation only, not a new data/action requirement.
- **D29-01 — phone lower-detail evidence is incomplete.** `WB29-expanded-bottom-phone-dark.png` stops at Morning Break; neither its £47.20/Snooze nor Afternoon and the £15.00 expense are visible. Add the smallest native scrolled capture(s) showing those amount/action and last-row facts. This is an evidence gap; no assertion is made that the controls are unreachable.
- **P29-04 — Filters phone touch targets.** `WB29-filters-phone-dark.png` has header Close and footer Clear filters/Apply about 36px high, below SDP-81/84's 44px. Use a Filters-qualified modern-modal phone rule for these existing controls; preserve desktop sizing.
- **P29-05 — clipped desktop More after Payment-width repair.** Fresh Candidate/expanded frames still cut the More right border/chevron at the host edge; explicit Actions180px does not reserve enough total inline width. Keep the two existing inline actions fully within the host.
- **P29-06 — desktop day/expense money-column alignment (CLOSED).** The original Amount+Status colspan placed £94.40/£15.00 at the far right while £47.20 leaves were under Amount. Root separated Amount and Status cells; fresh desktop frame now aligns all three and places Snooze under Status.
- **P29-07 — phone colspan-row native labels.** The corresponding fresh phone day/expense rows show ROLE beside £94.40/£15.00, with an empty BAND row. Native automatic table labels index the three td nodes despite colspan6; retain explicit Amount/Status phone labels for these rows. An expense Status/Snooze lower capture is still needed.

Fresh corrected-state closure so far: P29-01 no erroneous normal Draft banner, P29-02 reference and badge together in the phone Payment value, P29-03 flat desktop children in one rounded native host, and P29-04 Filters controls 44px. Morning £47.20/Snooze is visibly covered in the new shift-bottom phone capture. Final verdict remains pending P29-05/P29-07 and the remaining expense lower evidence; checkpoint FAIL/PENDING rows below are historical until the final replacement table.

Closed by fresh inspection: the desktop payment caret is now 24×24 by explicit native scoped CSS (phone remains 44×44), and the group rate-set badge uses the native info tone. T02/T02e do not require an additional expanded component badge.

## Scope and comparison baseline

Actual sample is one Candidate, one Timesheet, two chosen shifts and an unchanged reimbursement. Authorised Source £95.00 is correctly distinguished from the chosen group £109.40; the chosen component is £94.40, its two leaves £47.20 each, and the unchanged expense £15.00. These are literal reader values, not browser valuation. Main/facts/Filters use “as authorised” for the Source basis.

Supplied comparisons: `png/T01_ready_list_dark.png`, `png/SCR-09c_candidate_rate_set_ready_dark.png`, `png/T02_candidate_window_dark.png` in the read-only design pack. Actual/design pairs were opened in the same tool call. The current reader has no display-group count: HEADER item_count is leaf membership. Picture-only group counters are omitted under owner A2; no group count is inferred. Optional memory selection is not relabelled as an authoritative selected-payment count.

All 13 current images (main, Candidate, expanded and Filters in dark/device-light/phone, plus expanded scrolled phone) were individually viewed. The four device-light views retain the dark palette; main Updated changes with the actual capture time. SDP-83 explicitly permits phone tabs to wrap onto two lines; the four Candidate sections therefore conform. No nowrap or sideways tab strip is needed. Desktop Payment/date wrapping can be reduced with the supplied SCR-09c's wider Payment-column proportion, without adding data.

## Full 42-item policy checkpoint

PASS entries below are scoped visual/source checks of these Ready display and Filters changes. N/A states that this slice does not own or capture the named behavior. No entry attests to a business action or hundreds-row app test. Final hashes and corrected-state verdict will replace this checkpoint after re-capture.

| Item | Result | Evidence / boundary |
|---|---|---|
| 1 | PASS | Existing scope actions: open Candidate/Timesheet, expand, ticks, Filters, Refresh, Export and estimate remain reachable. |
| 2 | PASS | Original native callbacks and shared controls retained; no legacy capability removed by chosen-display map. |
| 3 | PASS | Candidate/Timesheet/Umbrella rate set and authorised qualifier match owner/card vocabulary. |
| 4 | PASS | Literal Source summaries separate from chosen component/leaf values; no tick valuation, pay date or rebuild. |
| 5 | PASS | Native wide/small frames, factual Candidate subtitle and header Close. |
| 6 | PASS | One main tab tier; one Candidate segmented control; two-line phone wrap allowed by SDP-83. |
| 7 | FAIL | P29-03 nested expanded panels differ from native flat T02 rows. |
| 8 | FAIL | P29-03 missing outer native table host/frame and repeated inner framing. Money itself is right aligned. |
| 9 | PASS | Ready success; chosen-rate info; existing failed-read danger, with text. |
| 10 | PASS | Create Draft/Show estimate/Apply sole footer primaries; Export/Clear filters secondary; no duplicate Close. |
| 11 | PASS | Current changed caret selectors are modern scoped, reuse native tokens; no new !important. |
| 12 | PASS | Original bounded automatic scrolling/source continuation; no page buttons. Large app run NOT_RUN. |
| 13 | PASS | Native whole-scope sorts, one active direction and Sort by; expanded child rows exempt SDP-34. |
| 14 | PASS | Native Candidate/Work/component choice store and mixed counts preserved; no loaded-row inference. |
| 15 | PASS | Main/Filters 1 Candidate and £95 authorised Source from whole-scope DTO; unavailable group totals omitted, not guessed. |
| 16 | PASS | Manual tools/policy.json neverShow/chatter/formats/vocabulary review; no codes, UUIDs, chatter, ISO dates or GBP displayed. |
| 17 | PASS | Observed genuine Draft read error is one plain sentence, alert and Refresh; P29-01 concerns normal-state evidence. |
| 18 | N/A | No confirmation or destructive action belongs to this chosen-display capture slice. |
| 19 | PENDING | Native load/failed paths retain truth; normal main capture still fails its Draft dependency (P29-01). |
| 20 | PASS | No technical Case identifiers, “loan” or implementation language. |
| 21 | PASS | UK dates/times, £ values with pence, unknown values retain dash; native formatted-money markers. |
| 22 | PASS | Native labels, focus, row/keyboard guards, mixed ticks and aria-sort retained; no keyboard business journey claimed. |
| 23 | FAIL | P29-02 phone value association, P29-04 Filters targets and D29-01 lower-detail evidence. No sideways page overflow observed. |
| 24 | PASS | Every current device-light view remains dark; actual Updated clock differs naturally. |
| 25 | N/A | Owner supplies specs/PNGs; no generated spec/html or pack tool run. |
| 26 | N/A | Owner excludes spec readerAdditions; actual added fields are finite own-reader facts, separately source reviewed. |
| 27 | PASS | No pre-execution pay date; week ending/day dates are actual work context. |
| 28 | FAIL | P29-02 Timesheet reference visually sits in phone label column. Other language is plain. |
| 29 | PASS | Natural header tools, payment-row actions, More menu and bottom next action. |
| 30 | PASS | Estimate/Create/Apply named plainly; genuine failed read gives Refresh. |
| 31 | PENDING | Actual person and chosen group facts visible; D29-01 still lacks lower phone amount/action proof. |
| 32 | N/A | Waiting/Action/Blocked rows are not this current chosen Ready sample; labels preserve their owning readers. |
| 33 | N/A | No business mutation/outcome/return journey is executed or owned by this slice. |
| 34 | PASS | Each added displayed figure/badge is necessary for chosen-rate truth; no extra expanded badge/control. |
| 35 | PASS | Candidate, Timesheet, pay channel, authorised Source qualifier consistent. |
| 36 | PASS | No new destructive path; original exact choice/frozen-create and guarded actions retained. |
| 37 | PASS | Header Close/native parent restoration; no edits or financial action executed. |
| 38 | PASS | Caret only for useful detail; Open + More inline; no redundant Expand wording or full duplicate one-line child. |
| 39 | PENDING | Desktop literal £47.20 + £47.20 = £94.40; + £15.00 = £109.40. Phone lower fact proof D29-01 outstanding. |
| 40 | PENDING | Source-versus-chosen basis clear; intended normal main confidence blocked by capture error P29-01. |
| 41 | PENDING | Native bounded/retained read, keyboard and dark/phone source supports scope; phone findings still open, hundreds/joined NOT_RUN. |
| 42 | PASS | Person, current Ready section, sort direction and authorised/chosen labels visible; no invented progress or timing. |

## Visible differences and control purposes (initial set)

- **Main/T01:** one sample Candidate rather than 230; current literal £95.00 rather than the illustration; no fabricated bank warning, zero-only row or failed item. “Ready as authorised” and “Amount ex VAT (as authorised)” qualify the existing Source summary instead of misrepresenting chosen £109.40. No Timesheets View column is invented from absent main-list facts; Candidate opens its existing breakdown. Native Select all/Clear all remain WB-12 controls; All except 0 is the owner's current selection wording. Updated and tools use original native layout; taller whitespace scales with a one-row sample. Unexpected failed Draft banner P29-01 is not accepted as the intended normal difference.
- **Candidate/SCR-09c:** actual Amelia/reference/Client/week/channel rather than pictured Bethany/hospital/multiple groups. Existing Source £95.00 header is explicitly authorised; chosen £109.40 and info badge satisfy SDP-49a. Omit picture-only group counts because HEADER counts leaf membership, and omit an unproved selected-payment total; actual All payments included is the existing store's known native truth. TS 6111 is the own-reader reference. Existing Open Timesheet + More obey owner inline-action pattern. 24px desktop and 44px phone caret replaces the prior oversized button. Square/nested framing and phone reference placement are failures, not approved variations. No extra expanded rate badge is needed.
- **Expanded/T02:** two native shifts in one day and one £15 expense instead of the pictured mixed days/snoozes/Cases. Literal native roles, band, start/finish and breaks shown. Expenses and Additional covers both native supported kinds; no fabricated mileage quantity, deduction, estimated cash or rates are added. Day subtotal £94.40 and individual £47.20 leaves use their respective reader facts. Plain Ready children omit duplicate Ready badges. Framing P29-03 must be fixed; D29-01 lower phone evidence must be supplied.
- **Filters/SCR-02:** native unfiltered PAYE+Umbrella, All Candidates/Clients and one Candidate £95.00 differ only by real sample/scope. “ready, as authorised” is the required amount basis. No pay date. Candidate/Client controls open original pickers; radios choose channel; Clear filters resets scope; Apply applies staged scope; Close backs out. Reset selection/Clear all decisions are outside this WB29 copy/amount support; no new future resolution action is introduced. Phone targets P29-04 must be fixed. Parent background is not fabricated for the local isolated fixed-read frame.
- **Every other visible control:** header Refresh re-reads; row Open Timesheet opens the qualified native item; caret expands its useful shifts/expenses; existing group/day/expense ticks choose membership without recalculating; shift/expense Snooze goes to the existing shared window; More holds existing snooze/Pay lines; Export creates the existing CSV view; Show estimate requests its existing explicit estimate; Create Draft is the existing guarded next step. No new control is proposed just to imitate the picture.

Existing per-screen review history remains untouched. A final scoped A4 receipt requires closure of the findings and current-byte hashes; this checkpoint cannot close WB29 or its later rates screens/whole joined behavior.

## Final current-byte A4 receipt — 13 October 2026

**A4 PASS / APPROVED: 37 PASS / 0 FAIL / 5 N/A. No remaining essential visual finding in this implemented slice.** Reviewed all 15 current PNGs after the final Actions220px, explicit phone Amount/Status labels and expense-control scroll capture. This section replaces the checkpoint verdict, not the historical record of fixes. No product, pack, capture, sample, database or runtime change was made by this reviewer.

- P29-01: the exact native ACTIVE_PREPARATIONS dependency is supplied; normal main views no longer show the failed-Draft banner. The product failed-read guard was preserved.
- P29-02: Timesheet name, TS 6111 and the native info badge share one phone value wrapper.
- P29-03: one rounded 13px native Candidate host, flat desktop day/shift/expense rows and retained bounded native scrollers replace nested padded panels.
- P29-04: Filters phone Close, Clear filters and Apply are 44px targets.
- P29-05: Open Timesheet and More, including chevron and right border, fit inside the desktop host at Actions220px.
- P29-06: day £94.40, both £47.20 leaves and expense £15.00 align under Amount; Snooze is under Status.
- P29-07: phone day/expense money is labelled Amount, the expense control Status, and genuinely empty Status is omitted. Nonwork status now also retains the existing memberStatus factual Waiting/Reserved wording; source evidence only for those non-Ready states.
- D29-01: the final shift and expense lower phone captures visibly include their literal money and 44px Snooze controls. No reachability defect remains in those inspected states.

### Final all-42 checklist

PASS means the actual captured Ready-display slice plus its relevant saved source conforms. It does not claim an executed financial/keyboard/large-data journey. N/A entries exclude behavior this change does not own or exercise.

| Item | Result | Current evidence / boundary |
|---|---|---|
| 1 | PASS | Existing Candidate/Timesheet opening, useful expansion, ticks, Filters, Refresh, Export and explicit estimate seams remain. |
| 2 | PASS | Native controls/callbacks retained; no product capability removed by chosen-display mapping. |
| 3 | PASS | Banking Pay, Candidate, Timesheet, pay channel and rate-set vocabulary match the owning card. |
| 4 | PASS | Source summaries stay authorised; chosen component/leaf values are literal DTO facts. No tick valuation or pay-date fiction. |
| 5 | PASS | Native wide and small frames, Candidate subtitle and safe header Close. |
| 6 | PASS | One main tier and Candidate segmented tier; SDP-83 permits the phone two-line wrap. |
| 7 | PASS | P29-03 closed: flat native factual desktop rows, no extra card inside card. |
| 8 | PASS | One 13px table host; quiet sticky headings, right-aligned tabular money and no clipped inline actions. |
| 9 | PASS | Ready success, rate-set info and native failed-read danger retain distinct text/tone. |
| 10 | PASS | One footer primary per state: Create Draft, Show estimate or Apply. Export/Clear filters secondary. |
| 11 | PASS | Changed CSS is modern-modal scoped, native-token based and adds no !important. |
| 12 | PASS | Original bounded automatic read/scroll retained; no Previous/Next controls. Large runtime NOT_RUN. |
| 13 | PASS | Native whole-scope sort keys and one active direction; phone Sort by. Factual child table exempt under SDP-34. |
| 14 | PASS | Existing native choice store, mixed state and original membership semantics preserved; no loaded-row inference. |
| 15 | PASS | Main/Filters count and £95.00 come from whole-scope Source DTO; unavailable display-group/selected-payment totals omitted under A2. |
| 16 | PASS | Manual tools/policy.json neverShow, chatter, format and vocabulary review: no exposed internal code/UUID/technical prose or disallowed money/date/time. |
| 17 | PASS | Preserved native failed-Draft path uses one plain sentence, alert and Refresh; corrected normal state has no error. |
| 18 | N/A | No confirmation/destructive action is owned or exercised by this display slice. |
| 19 | PASS | Corrected normal read truth; original bounded loading/retained/error handling preserved. |
| 20 | PASS | No technical Case names, loan terminology or implementation explanation. |
| 21 | PASS | UK dates, 24-hour times and £ with pence; unknown stays dash. Existing money marker prevents native reformatting. |
| 22 | PASS | Labels, mixed ticks, aria-sort, useful caret aria-expanded and native qualified focus/row guards retained; no performed keyboard action claim. |
| 23 | PASS | Actual 390px captures: value association, labels, targets, useful scroll coverage and reachable footer; no sideways page overflow observed. |
| 24 | PASS | All device-light views retain dark palette. Candidate/expanded/Filters pairs byte-identical; main Updated differs naturally. |
| 25 | N/A | Owner supplies specs/PNGs; no spec/render tool or pack change. |
| 26 | N/A | Owner excludes readerAdditions generation; finite factual DTO additions were separately source reviewed. |
| 27 | PASS | No pay date before execution; visible work dates/week ending are actual context. |
| 28 | PASS | Timesheet reference stays with its name/badge; plain authorised/chosen labels and correct phone money labels. |
| 29 | PASS | Header tools, inline Open/More, row Snooze and bottom next step are next to what they affect. |
| 30 | PASS | Named Create Draft/Show estimate/Apply primary; original failed read points to Refresh. |
| 31 | PASS | Real Candidate/reference/channel, Source qualifier and chosen group/leaf/expense amounts visible; lower phone facts now covered. |
| 32 | N/A | Waiting/Action/Blocked presentation is not the current chosen Ready capture sample; original source reason paths retained. |
| 33 | N/A | No business mutation/outcome/return journey is owned or executed by this slice. |
| 34 | PASS | Each new displayed fact/badge explains chosen-rate truth; no unnecessary expanded badge/control. |
| 35 | PASS | Same Candidate, Timesheet, pay channel and authorised/chosen vocabulary throughout. |
| 36 | PASS | No new destructive path or financial operation on a display click; original guarded choice/Create owners preserved. |
| 37 | PASS | Native Close/parent restoration and untick remain; no irreversible action executed. |
| 38 | PASS | Small useful caret; Open Timesheet + More inline; phone Open; no Expand text or redundant complete one-line parent copy. |
| 39 | PASS | Literal £47.20 + £47.20 = £94.40; + £15.00 = £109.40. £95.00 is explicitly a separate authorised Source basis. |
| 40 | PASS | Before Create, Source-versus-chosen basis is explicit; original Show estimate remains the next valuation action. No bank-cash promise is invented. |
| 41 | PASS | Inspected phone/current read layout and relevant bounded loading/error/continuity source conform; hundreds-row/joined/runtime acceptance NOT_RUN. |
| 42 | PASS | Banking/Candidate context, Ready tier, sort direction, reference and authorised/chosen labels remain clear. |

### Final visible-difference and control inventory

The initial inventory above is retained as chronology. Its failures are resolved; the following are the remaining visible differences from the supplied PNGs and their card/native-reader justification.

- **Main versus T01/T06:** one actual sample Candidate and two payment members rather than the illustrative cohort; zero Action/Blocked badges and no fictional bank-warning/failed/zero-only rows. Literal £95.00 is labelled Ready as authorised / Amount ex VAT (as authorised), distinguishing it from chosen £109.40. Native Updated occupies its existing row; Select all/Clear all remain the owning WB-12 controls; All except 0 follows the owner wording. The one-row cohort has native whitespace, rather than invented rows to fill the picture. No Timesheets View column or main rate-set badge is fabricated from absent main DTO fields. Candidate detail is the existing way to inspect the actual Work. Main phone Open exists in the bounded native row (source verified), below the currently visible card portion; these screenshots do not claim an executed main-phone Open journey.
- **Candidate versus SCR-09c:** real Amelia Hughes/TMS 10421, TS 6111, Riverside Care Home and w/e 04/10/2026 replace sample identities. One Work rather than mixed Cases. Authorised £95.00 header and chosen £109.40 group keep their own bases; native info badge below the reference stays associated on phone. No group count is inferred from leaf item_count, and no unproved selected-payment total is shown. Known All payments included comes from the native store. Small desktop caret/44px phone caret, inline Open Timesheet + More and phone Open meet the owner's action pattern. Active-arrow colour follows the existing native header; its actual direction remains explicit. Candidate tabs wrap onto two phone lines as SDP-83 allows.
- **Expanded versus T02:** the actual day has two shifts and one unchanged expense; its literal native role, band, times and breaks replace illustration data. £94.40 is the day amount, £47.20 each shift and £15.00 the expense. Expenses and Additional names the supported native kinds. No absent mileage quantity, deduction, estimated bank cash, extra rate badge or repeated Ready status is fabricated. One flat desktop host retains bounded native nested read regions; phone uses the existing labelled factual-row cards and supplemental scroll views. No new data/control is required to imitate mixed illustration rows.
- **Filters versus SCR-02:** current unfiltered PAYE+Umbrella, All Candidates/Clients and factual one Candidate £95.00 replace the picture's PAYE/Client selection. The Source amount qualifier is required. Native picker buttons, 44px radio-row spacing and original fieldset treatment preserve the actual shared picker/window adaptation; no fictional parent background is manufactured. Reset selection/Clear all decisions are outside this WB29 amount/copy support and are not newly added. Whole Filters capability outside this scoped reuse is not approved by this receipt.
- **Controls:** Close backs out; Refresh re-reads; Filters/pickers/radios/Clear filters/Apply edit staged filter scope. Main/global/Candidate/day/expense ticks use the existing selection owner without browser money calculation. Caret expands useful factual shifts/expenses; Open Timesheet/phone Open preserve the native qualified item callback. More retains existing Snooze/Pay lines, and individual Snooze uses the existing shared window. Export remains secondary and read-only; Show estimate is explicit; guarded Create Draft remains the existing next step. No extra control is added only to match an illustration.

Source review of the final nonwork Status repair confirms it calls the existing memberStatus for each retained member before adding existing expense/mileage Snooze; empty Ready status remains empty. This is not a screenshot approval of unrendered Waiting rows. Source qualification and backend/Worker business semantics are outside this A4 receipt.

### Provenance and evidence limits

Backend source checkpoint: `d23544f600187821ca24195a09a78648bdcc9c4e`. Frontend HEAD at hash capture: `f0d71fa73b038a95f81086ba84a3e55fb98c6f4e`; the final scoped Workbench/CSS/index changes are saved working bytes, identified below. The original Office and modal modernisation hashes are dependencies, not a claim to re-review all their unrelated code. All images are root-produced actual native Office renders with bounded local fixed read responses; no database rows, hosted reads or financial actions are acceptance evidence. Business dispatch, joined original Timesheet/Draft/CSV/Snooze journeys, hundreds-row performance and whole WB29 completion remain NOT_RUN/open.

| Saved frontend source | SHA-256 |
|---|---|
| js/bpay-next-workbench.js | 26E457E3335CF79E18386463AF7DC31A61C44FDC0D6613B32CEC140EE98AF265 |
| css/bpay-next-office.css | A046A6EA08037328C966588D912775A3CC83404341B228ED76021A05F524FE14 |
| index.html | 7DFB708A132093FA03C85F08A1D7D4867191A10202F79F832FD9DE8E55076502 |
| js/bpay-next-office.js (dependency) | 1DBE0073A2E226D11FD7778F43A347B5264DA57BFE9EEAE0525B966BA0401B29 |
| js/modal-modernisation.js (dependency) | 5EEA89B05FBEA6320656BFDDB7DCAB6574AFC742563F6E2B4E7885F732E610E5 |

Fixed read response `screen-designs/SCR-09/WB29-fixed-read-responses.json`: SHA-256 `F59247A91E5A0728D1DAC44D68562FF2996E83BBCF1E776BE74B6D0B2B39839A`.

All 15 current PNGs below were individually viewed; actual/design pairs were opened together for main, phone main, Candidate, expanded and Filters families. No design PNG was copied or changed.

| SCR-09 actual PNG | SHA-256 |
|---|---|
| WB29-candidate-dark.png | 8745EDF01D57A3F9BADE45163040D11538C20AD1FADE10840C26A89CDA85FCE5 |
| WB29-candidate-device-light.png | 8745EDF01D57A3F9BADE45163040D11538C20AD1FADE10840C26A89CDA85FCE5 |
| WB29-candidate-phone-dark.png | 67AE456434CB7C4D98A7EDD60C8FA3E19ABD952644BD7C279D69AF073899D367 |
| WB29-expanded-bottom-phone-dark.png | 52FC172E69315C33DE11CCBECA39A3AB577B5CCF75C9F5B8096644E39465CA45 |
| WB29-expanded-dark.png | BE05CFB8258EA02D66B598609500D9723157F1DF414EA880F5D738F05F8CB9BE |
| WB29-expanded-device-light.png | BE05CFB8258EA02D66B598609500D9723157F1DF414EA880F5D738F05F8CB9BE |
| WB29-expanded-expense-bottom-phone-dark.png | 8D0B80886D90A705BB19332531D51995F6FEB8DC9C5FD29826A312BC24105A4C |
| WB29-expanded-phone-dark.png | BE7A356CFC2B48894C88D348A7951A352A076AA5C30294647DC9DDF8E223F385 |
| WB29-expanded-shift-bottom-phone-dark.png | 91952CBA9D1B15E4DBD19641774D70D8672DF3A164C85C3FE6C109755E6B2269 |
| WB29-filters-dark.png | 533566577F2CDC200E4D6A1BF2AFE1021E6E4AB9914360EDF524404C83AA8936 |
| WB29-filters-device-light.png | 533566577F2CDC200E4D6A1BF2AFE1021E6E4AB9914360EDF524404C83AA8936 |
| WB29-filters-phone-dark.png | AC1A1681580259B8A622A26C5CA6005308CD0E541F9ECF93B13D14ACEBE59BCA |
| WB29-main-dark.png | D73F1BF57ECAEE650B20375BE019BADACA0797F8D1943A317A1172FBEF6723AF |
| WB29-main-device-light.png | AF1FA86241908F2D76EA5284633554F4C33536CC00A5CD4BEC515E44F018EE3C |
| WB29-main-phone-dark.png | E8D55B245984D93F3A9A259CB9EBB9261C019F5B4902288F0B73C64530AD68F5 |
