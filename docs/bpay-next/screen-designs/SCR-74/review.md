# SCR-74 EX-08 independent A4 review

## Checkpoint — 11 October 2026

**A4 pending; visible findings block approval.** Review is read-only on actual local native Office captures and the supplied B6 SCR-74 design. No product, design-pack, runtime, database or business action is performed by this reviewer.

- **P74-01:** protected Draft detail says “Captured work”, a forbidden word in `tools/policy.json` and an internal description. Use a factual plain label supported by the existing row.
- **P74-02:** `EX08-accepted-dark.png` still shows “The earlier change needs its result confirmed.” despite the accepted scenario. Show the actual known result; do not leave accepted change looking unknown.
- **P74-03:** that accepted screen has a primary footer Close as well as the header Close. SDP-24 requires Close only in the header.

These findings were reported to the product owner immediately.

- **P74-04:** native phone header Close and footer controls initially measured about 36px visually; the custom wrapper's 44px rule misses the native footer's moved buttons.
- **P74-05:** failed read says “Choose a company.” below an already selected company. The correct next step is Refresh.
- **P74-06:** error result always used `role=status`; failures require `alert`, and initial/continuation loading requires `status`.
- **P74-07:** the retained request's company chooser is visibly repeated despite `hidden=true`; its scoped flex display overrides the hidden attribute.
- **P74-08:** wrapped company-change phone subtitle and Close are clipped by the fixed header/body boundary.
- **P74-09:** phone money values lose their right alignment in the native labelled cards.
- **P74-10:** the new sentinel CSS selector lacks the required native modal prefix.
- **P74-11:** impact table headings lack sticky positioning against the continuous body scroll.
- **P74-12:** outer card radius13 and inner table radius12 differ from policy card16/table13.
- **P74-13:** the formatter puts the pound sign before a negative sign; the required form is “−£150.00”.

All nine state groups were individually viewed in all three modes beside the supplied B6 design (27 images). Product repairs and recapture are in progress, so final source/pixel hashes and the completed 42-row verdict will follow the final bytes. No complete approval is claimed at this checkpoint.

## Final A4 receipt — 11 October 2026

**PASS for the current EX-08 impact dialog: 37 PASS, 0 FAIL, 5 scoped N/A across all 42 policy rows.** All 27 actual local native screenshots were individually inspected beside the supplied B6 SCR-74 PNG. Every image affected by the final sticky-corner repair was reopened; unchanged image hashes preserve the completed phone and recovery-state review. P74-01–P74-13, including the sticky-heading corner follow-up, are closed. No essential visible defect remains in this scope.

This supersedes the checkpoint verdict without deleting its history. The accepted-labelled early capture showed an unconfirmed state; the final accepted capture uses the strict accepted receipt and shows “Pay channel changed.” Initial capture/state setup and product presentation findings are not evidence that a real command failed. Capture session59973 was reported complete before this final byte review. The reviewer read images, saved source and capture metadata only; no product, pack, rendering tool, fixture, database, hosted request or business dispatch was performed.

Approval covers the native Candidate entry integration by source inspection and the actual local impact-dialog states below. Joined application, real change/replay, queue projection, keyboard interaction with an installed app, hundreds of rows and later WB29/WB45/CM-11 decision handling remain **AUTHORED / NOT_RUN**. Sticky CSS and continuous cursor handling were inspected in source; the small desktop sample fits the viewport and does not prove a long-list sticky interaction.

### Closed findings

| Finding | Final evidence |
| --- | --- |
| P74-01 | Technical protected-Work subline omitted; factual original Draft channel retained. |
| P74-02 / 03 | Known accepted state has its actual acknowledgement and header Close only. |
| P74-04 | Capture metadata measures every visible phone button/select at 44px high; all nine phone states have reachable controls. |
| P74-05 | Failed read shows the inline cause and Refresh; no misleading “Choose a company.” skeleton below the selected company. |
| P74-06 | Read/command failures use alert; accepted result and initial/continuation loading use status. Source-only accessibility evidence accompanies the real pixels. |
| P74-07 / 08 | Retained chooser is absent; phone subtitle wraps fully and header Close is not clipped. |
| P74-09 / 10 | Phone money is right aligned and tabular; supporting Client copy occupies its value column. Sentinel selector has the native modal prefix. |
| P74-11 / 12 | Sticky table headings, card16/table13 radii and first/last heading13px top corners; no overflow clipping defeats sticky positioning. |
| P74-13 | Exact decimal-string formatter groups thousands, preserves pence, displays null as “—” and places the Unicode minus before £. Negative-money rendering is source evidence, not an additional captured sample. |

### Supplied-design comparison and visible differences

The read-only inventory assigns SCR-74 to EX-08/B6. The supplied example and spec were inspected alongside actual images; neither was copied or edited. The root's [difference note](EX08-supplied-design-differences.md) records the same scope. Differences below were checked independently, including each control's payroll purpose.

1. The actual native Office frame retains its focus outline and real Candidate background; the supplied annotation strip is design commentary. The native medium980 frame, standard title/subtitle and header Close match the intended structure. Native text metrics, roomier row spacing, table heading colour and column proportions differ slightly from the staged image while keeping the same business order, legibility and existing Office pieces. Those differences come from the required native universal-table adapter, not another window system.
2. Stays payable contains the exact server count2, converted £672.00 and unchanged Mileage £25.00. The illustration's reimbursement explanation is omitted because the closed row supplies classification, label and amount, not that extra tax/bucket explanation. No missing amount is invented.
3. Needs a decision shows the actual £296.45 source figure with “PAYE, as authorised”, as required by the owner. “Pay channel decision needed” replaces the illustration's Night-specific rate label because this reader supplies no rate bucket. “Moves to Action required” is not asserted before the later classifier/resolver. No early WB29 decision control is introduced.
4. Not changed shows the whole-list server count2 instead of restricting its heading to “Already in a Draft or paid”; the category also admits unchanged unavailable future work. Protected Work shows its original “PAYE Draft”, “Stays PAYE” and missing amount “—”. The illustrated short Batch reference, two-Timesheet aggregate and £734.20 are not reader facts and are omitted. The original Payment Advance repayment shows the supplied £150.00, “In PAYE Draft” and “Stays as agreed”. No technical identity or extra detail viewer is exposed.
5. PAYE target and same-channel company change are card-required variants absent from the illustration. PAYE has no Umbrella picker; company change retains the selected company and “Umbrella to Umbrella” header. The retained original request uses its known Candidate/reference/target/company rather than a fresh chooser.
6. Empty, loading, failed-read, retained uncertainty and accepted states are necessary lifecycle states. They use plain inline copy with an appropriate next step; no toast, fake zero, pay date or fabricated completion is added. Accepted acknowledges the change, without claiming its later projection has finished.
7. Phone uses the native labelled-card table adaptation, continuous body scroll and reachable footer. Its wrapped subtitle, 44px targets, right-aligned figures and final protected Case were inspected. There is no Sort by control because SDP-34 exempts this impact dialog. Device-light preserves the same dark Office appearance.
8. Tables are the permitted inner structure within the three supplied impact sections. There are no extra nested panels, tabs, ticks, carets, More menus, bank inputs, rate inputs, row actions or extra viewers. The illustration's next step remains Apply change; uncertain recovery substitutes Try again and accepted completion retains only header Close.

| State (all three modes inspected) | State-specific comparison / necessary next step |
| --- | --- |
| preview | Same three impact sections as B6; exact2/1/2 counts and five native facts. Company choice and Apply change affect only the future target. |
| empty | Three genuine zero category counts and “No payments.”; Apply change still changes the Candidate's future channel/company, without making up money. |
| loading | “Loading payments…” explains disabled choice/Apply. No Candidate name or amounts are invented before the first reader response. |
| failed | Selected company remains; one inline changed-list sentence, secondary Refresh and disabled Apply. No stale cards or false £0.00. |
| pending | Original known Candidate/reference/method/company survives; chooser absent, earlier result is explicitly unconfirmed and Try again recovers the retained request. |
| accepted | “Pay channel changed.” and header Close; no duplicate footer Close, stale uncertainty or projection-completed claim. |
| paye | Umbrella-to-PAYE header and no company picker; original protected facts stay separate. |
| company-change | Umbrella-to-Umbrella header and selected future company; complete phone wrapping. |
| scrolled | Desktop's five-row sample fits unchanged; phone's actual body scroll reaches the final Payment Advance and fixed footer, without pages or load buttons. |

| Visible control | Why payroll needs it / expected effect |
| --- | --- |
| Header Close / native Escape path | Back out of preview safely; retain uncertainty after dispatch. After acceptance, return the accepted target to the original Candidate form. |
| Umbrella company select | Choose the future enabled company; a fresh target change invalidates and rereads its exact preview. Disabled while reading and absent for PAYE or retained intent. |
| Apply change | Sole primary; submit the exact server preview context and future channel/company, with no browser revaluation of protected money. |
| Refresh | Secondary after failed preview; reread that preview, not automatically resend a command. |
| Try again | Sole primary for an earlier uncertain request; reuse its original endpoint, operation and exact stored request bytes. |
| Continuous body scroll | Reach every classified fact and the final protected Case; continuation uses the same context/cursor. |
| Accepted header Close | Finish this window after the actual accepted receipt; no redundant footer action. |

### Complete policy checklist

PASS means applicable current-dialog source/pixel evidence, not unperformed joined acceptance. The five N/A rows are specific exclusions. Row16 is a **manual** comparison against the actual neverShow/chatter/format/vocabulary lists; the pack wording program was not run. Row26's actual reader mapping passes; only its proposed readerAdditions alternative is N/A under owner A2.

| # | Result | Evidence / scope |
| --- | --- | --- |
| 1 | PASS | EX-08 future channel/company preview, classified impact and Apply are present; later resolver cards are not claimed here. |
| 2 | PASS | Native Candidate pay-method entry LUI-185 is retained and selects the guarded NEXT route in saved source. Joined opening is NOT_RUN. |
| 3 | PASS | Required future-only consequence, impact labels and owner-required “PAYE, as authorised”; vocabulary corrections are recorded above. |
| 4 | PASS | Server prepared quotes/classifications; original protected money stays unchanged, no per-tick valuation or payment execution. |
| 5 | PASS | Native medium980 frame, title/subject and header Close only, including accepted and phone states. |
| 6 | PASS | No added tab row or second navigation level. |
| 7 | PASS | Picker then three impact sections then consequence/action; supplied section/table structure, no extra nested panel. |
| 8 | PASS | Native universal tables; sticky opaque headings with rounded top corners; money right/tabular on desktop and phone. Long-list sticky interaction NOT_RUN. |
| 9 | PASS | Plain text accompanies blue converted-state and amber decision badges; no colour-only state. |
| 10 | PASS | Apply change or Try again is the sole primary; failed read has only secondary Refresh, Close stays in header. |
| 11 | PASS | Existing native frame/table/footer adaptation; new EX-08 selectors have native modal prefix, token palette and no !important or new framework. |
| 12 | PASS | Continuous body and bounded same-context cursor continuation; status Loading more… sentinel, no page controls. |
| 13 | N/A | SDP-34 exempts this finite impact/confirmation dialog from sortable headings and phone Sort by. |
| 14 | N/A | No selection operation or tick column is owned by this impact dialog. |
| 15 | PASS | category_counts and total_member_count come from the reader; not sums of displayed pages. |
| 16 | PASS | Manual policy.json list/format check: no forbidden technical wording, chatter, UUID/code, overlong message or UK-format breach in final displayed copy. |
| 17 | PASS | Inline one-sentence changed-list cause with Refresh; retained uncertainty has its exact Try again path. |
| 18 | N/A | No additional confirmation or financial/destructive operation is specified; the supplied impact preview itself precedes Apply. |
| 19 | PASS | Empty/loading/failed/unknown/accepted are distinct; failed/unknown amounts never become false £0.00. |
| 20 | PASS | “Payment Advance repayment”; human Candidate reference only, no technical IDs/codes. |
| 21 | PASS | DD/MM/YYYY work labels, grouped two-pence money and missing dash; Unicode negative prefix verified in formatter source. No time is fabricated. |
| 22 | PASS | Native buttons/select and visible focus; saved frame keyboard path, status loading/result and alert failure roles. Actual installed keyboard interaction NOT_RUN; sort/tick roles are inapplicable. |
| 23 | PASS | Nine actual390px phone renders: labelled cards, no sideways body overflow, 44px visible controls, reachable main action and final row. |
| 24 | PASS | All nine device-light images remain visually the same dark Office. Eight pairs are byte-identical; preview differs at19 company-select border pixels only, with no theme/content/geometry change. |
| 25 | N/A | Owner supplied the immutable design pack and forbade copying/generating specs or running pack render tools; actual native PNG evidence is preserved here. |
| 26 | PASS | Header candidate_name/reference and context methods; category_counts; rows.label/detail/source_channel/source_amount/classification/original_section; quote.target_ex_vat; accepted receipt. No invented values; readerAdditions alternative N/A. |
| 27 | PASS | No official pay date or pre-execution payment date appears. |
| 28 | PASS | Yes: plain payroll terms, human identity, amounts and future-change consequence. |
| 29 | PASS | Yes: company choice above its impact; next step and recovery beside the current state in the footer/body. |
| 30 | PASS | Yes: Apply change, Refresh, Try again or header Close is the natural next step for the displayed state. |
| 31 | PASS | Yes for this preview: Candidate and exact available amounts, protected missing dash and future decision classification. It does not claim to be the payment/late resolver screen. |
| 32 | PASS | Yes within impact scope: decision-needed reason is stated; protected original state is plain. Later WB29 resolution actions/unknown bucket data are explicitly outside this approval. |
| 33 | PASS | Yes: accepted result is visible here; uncertainty remains visibly unconfirmed. Captures use retained local states, not real command execution. |
| 34 | PASS | Yes: necessary preview facts and controls only; raw protected detail, retained picker and duplicate footer Close removed. |
| 35 | PASS | Yes: pay channel, Draft, Payment Advance and Try again vocabulary is consistent. |
| 36 | PASS | Yes by source/preview evidence: exact context, actor/owner fence and retained request; protected money is not rewritten. No payment is executed by Apply. |
| 37 | PASS | Yes: native Close throughout; dismissal preserves dispatched uncertainty and only returns a known accepted result. |
| 38 | PASS | Yes: native entry → impact/choice → Apply; no additional viewer or detour. |
| 39 | PASS | Yes:2+1+2=5 native members, genuine empty0/0/0; exact visible figures, no invented aggregate cash total. |
| 40 | N/A | Create Draft is not an action or owning scope of EX-08. |
| 41 | PASS | Yes for captured empty/loading/slow/failed/phone states and bounded paging/keyboard source. Hundreds-row and installed-app interaction remain NOT_RUN. |
| 42 | PASS | Yes: title/subject, original retained target and explicit uncertainty/accepted message distinguish where the user is and what waits. |

### Evidence and SHA-256

Source inspected at the final saved bytes (no reviewer execution):

| File / source | SHA-256 |
| --- | --- |
| FE js/bpay-next-pay-method-change.js | 4bf56a3f87aa85d5c9ac762923dbb20639549fce49a202d4afd9a38ee9fbba99 |
| FE js/main.js | 35073a4ed6ca21d8dbc7bdf40d6970f175656f258567282a57eebbd106bfc243 |
| FE css/bpay-next-office.css | 63144a3f4f13b617dca0d057126def00bc402573c5cbebb1941c99c8041b804c |
| FE index.html | 4b919e4cc1e55eb77f56a1860239ce5cbd24c5f49507b9be8d9999b452b99e7c |
| Pack SCREEN_DESIGN_POLICY.md | 8bc212b174469236f2b46dbf70eadc3664a946636458321cd3c77b7146323de5 |
| Pack tools/policy.json | 45036d841d4568aa2d4d7adf4f063d6d17b70288e289ee0b285068797a87369d |
| Pack B6 SCR-74 spec | 02ee328f343ff0a050a38b270d60b4c0fcedb8c2ff2558a13cb270135a113c88 |
| Pack B6 SCR-74 PNG | 46a5d9824b01ce42ab6e2c888dd1ad4e05b1f6db9b59fdeb28ba5abbb3462174 |

The local capture metadata contains27 entries and zero browser errors. Every phone body measures364px wide with scrollWidth364; every visible phone control is44px high. Dark/device-light comparisons are visual and byte-based as noted in row24. The reviewer's additional image comparison decoded existing PNGs read-only; it generated no images or test data.

Final per-image and supporting-file manifest:

| File | SHA-256 |
| --- | --- |
| EX08-accepted-dark.png | e67ddbdd12d03cc99730015bc53652bcc005aee6e0743d55bf91f5abc00c979d |
| EX08-accepted-device-light.png | e67ddbdd12d03cc99730015bc53652bcc005aee6e0743d55bf91f5abc00c979d |
| EX08-accepted-phone-dark.png | ae8c3af3289067fb874fa2a857785d724ca4322fe85bfd820813a15abc763a46 |
| EX08-company-change-dark.png | cc2abcb9370c00c77389e83cc013dd719e5af42e3a99cd24a58cdd6bb54a1bde |
| EX08-company-change-device-light.png | cc2abcb9370c00c77389e83cc013dd719e5af42e3a99cd24a58cdd6bb54a1bde |
| EX08-company-change-phone-dark.png | 4e42273a9e1aa6ee7f617b64d7dded041947e420668e6f6753bde4e02ac9262e |
| EX08-empty-dark.png | d8ebc4412dff227ee85c4cfa731e7c78b393fe745f98a81f27acea8e850ba324 |
| EX08-empty-device-light.png | d8ebc4412dff227ee85c4cfa731e7c78b393fe745f98a81f27acea8e850ba324 |
| EX08-empty-phone-dark.png | 8dddbedf182c9a1be2bfaed45797a616cc5fb03286a1641915fa62c24c19537d |
| EX08-failed-dark.png | 25883a543d11322d875b6886104ea457b1c88a57b5768e4a3010a989502b67af |
| EX08-failed-device-light.png | 25883a543d11322d875b6886104ea457b1c88a57b5768e4a3010a989502b67af |
| EX08-failed-phone-dark.png | db2919ca82fd53571adeadfcffa8de16e3174ee32d54acf336aba9e86b12d3b4 |
| EX08-fixed-responses.json | 35fa2a175eb76161553d1edf69f09ef34413ea1c7d572a3d4acf70089572641c |
| EX08-loading-dark.png | 62d04a0d73342fcef1022846f73502a78f0c06fdb11974053ce8b49d7256f85b |
| EX08-loading-device-light.png | 62d04a0d73342fcef1022846f73502a78f0c06fdb11974053ce8b49d7256f85b |
| EX08-loading-phone-dark.png | 8a2ca0f7949c348d25e2f1dfaae2238e98f22ce25afd781fd7fef2e42d72c335 |
| EX08-local-captures.json | 5bf917aefbd274763fcdf475d1913b5c9b4a0c76bda10f631e7d9c7be4b0a640 |
| EX08-paye-dark.png | 8de439a3f9f0448039c29d18b2f105fc5ec688179abcf73f499b6790ad51b0f8 |
| EX08-paye-device-light.png | 8de439a3f9f0448039c29d18b2f105fc5ec688179abcf73f499b6790ad51b0f8 |
| EX08-paye-phone-dark.png | 697249a8abae4d6cd89b408387f41d5cdfbeb071bd59f4f3ea99ba8c7e193e14 |
| EX08-pending-dark.png | 0a3de331c3477e5c61136ab1bdd1079ff828141c4f76dc5d46646228bfdb25ef |
| EX08-pending-device-light.png | 0a3de331c3477e5c61136ab1bdd1079ff828141c4f76dc5d46646228bfdb25ef |
| EX08-pending-phone-dark.png | 5a51357adc7a6d68fb968cf4fa00479dece3c5580f087366acb7975e9a05e530 |
| EX08-preview-dark.png | a858fb7a7b287cdeb5824151c57ec3cf642c2e2ad3166d616222bfe6e53e0b6b |
| EX08-preview-device-light.png | 942572a2a13e4a255def100d581fd3f7ab1944eca5204a862c82a83d6324638f |
| EX08-preview-phone-dark.png | e2c04ddd78d57c506fe46fea900e0bded4ecba0380a821e3cafe268907331222 |
| EX08-scrolled-dark.png | a858fb7a7b287cdeb5824151c57ec3cf642c2e2ad3166d616222bfe6e53e0b6b |
| EX08-scrolled-device-light.png | a858fb7a7b287cdeb5824151c57ec3cf642c2e2ad3166d616222bfe6e53e0b6b |
| EX08-scrolled-phone-dark.png | c3cf4f9af873a4924f945ccdf5a32ed25137b971d1f838465779d229b418ab1f |
| EX08-supplied-design-differences.md | 0ce38cd9be410aabdde5abff51c29da8751daaa0b12357a8f2ae050210b192fe |
