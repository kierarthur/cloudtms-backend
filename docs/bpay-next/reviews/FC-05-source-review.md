# FC-05 D03/D04 — independent source review

11 October 2026. **SOURCE PASS for FC-05-D03 document contract and FC-05-D04 native creation source/shared-contract reuse; no essential source defect remains.** This is a source receipt against the complete FC-05 card, exact D03/D04 working rows, BD117 and original SCR-118 requirements. The current D03 JSON contract is independently checked below. This is not A4, installed-app, queue, due-once, funding, payout or next-Draft acceptance; A4 remains a separate receipt. All joined journeys remain NOT_RUN.

Read-only product review: read complete new `js/bpay-next-manual-credit.js`, current shared `js/bpay-next-case-preview.js`, exact native main/menu diff, CSS/index diff, relevant original Office controller/footer and native adapter, current final-definer/1536/1738/Worker reuse and BD117/design check. No product/checklist edits, SQL/business/hosted action, fixture, test, browser or screenshot operation was performed. Only this Markdown receipt was written.

## Findings and required source behavior

| Requirement | Concrete reviewed evidence | Finding |
|---|---|---|
| Explicit tax; no default | Manual Credit:27–34 creates two native radios without checked/defaultChecked and maps no choice to null; existing `caseInput` rejects null tax. Form problem:35–45 states the tax-choice reason. Shared Preview/Apply requires valid input then an identical validated Preview. | PASS; no omission/default becomes an accepted credit. |
| No schedule; bounded Comment/Note | Manual Credit:33–45 fixes CREDIT/MANUAL_CREDIT and every date/repayment/affordability/reference operand to null. Positive decimal/pence input and 1024-byte native numeric shape are preserved. Comment maps to trimmed nonblank reason, Note to nullable actual note, each <=2048 UTF-8 bytes and without NUL; original closed validator also enforces these shapes. | PASS; no local money calculation or hidden schedule. |
| Honest Credit Preview | Shared preview:74–83 adds only `creditPresentation===true && shownInput.case_kind==='CREDIT'`. It uses the validated total and original chosen tax for “£x credit”, Total and Tax; excludes threshold/floor and repayment dates. Original Advance branch is retained. PAYE-only hints in Manual Credit:47–50 are hidden for Umbrella/unknown channel and authoritative Preview context supplies the actual channel. | PASS; no bank-cash/net/headroom/future-payment estimate. |
| Original family/journal/unknown identity | Manual Credit:56–67 reuses `.case-create.<Candidate>`, qualifies CREDIT/MANUAL_CREDIT before hydration, and blocks other-family pending creation without modifying the original journal. Office:995 onward validates canonical saved action/body and exact original pending bytes; original retry dispatch never creates a replacement command. Shared footer:1934–1950 qualifies original Candidate/action. | PASS; original actor key/request/token/UUID and staff-triggered exact retry retained. |
| Native actor/owner/parent and picker lifecycle | Main:14508–14530 captures actual actor, full native stack prefix and parent context; checks fresh NEXT/admin and exact owner epoch before the new window, before all legacy state/session/bank reads. Manual Credit:5–23 and59–73 recheck actor/owner/frame/generation; picker selection is retained while the original frame remains alive and remounted on native onMount. Native adapter owns Close, frame active/alive and dismissal. | PASS; no adoption of another frame, late-result repaint or qualification-failure legacy fallback. Existing edit identities are excluded from Create. |
| Accepted target/readback and local completion | Shared footer calls original CASE_PREVIEW, CASE_CREATE and CASE_COMPONENTS for exact Candidate/Case. Shared refresh:104–111 refuses mismatched Case rows; Manual Credit:65–67 refuses mismatched Candidate rows/current owner before callback. Accepted form defaults are hidden; generic real Candidate/result remains. Empty/read-failed states preserve acceptance and offer exact Refresh. Existing Office finish occurs only after validated callback. Main:95247–95248 suppresses old whole-workbench refresh for the returned bpay_next result. | PASS; receipt is not represented as funded/paid, and no fabricated future Case host. |
| Scoped native menu/assets/style | Main:7120,7142–7170 adds only own NEXT Manual Credit destination, current actor/scope owner admission and native create call. Index loads the one new file and updates affected cache labels. CSS adds one `.bpay-next-credit` tax-hint font rule; original native Advance/small-frame/footer/hidden behavior is reused. | PASS within D04; actual pixels are A4's separate verdict. |

The current original `1536` typed terms/apply already owns CREDIT/MANUAL_CREDIT, explicit TAXABLE/NON_TAXABLE and repayment-field refusal, with GROSS_ADD versus NET_ADD at apply:336–343. Current `1738` public reader retains the original typed Preview and targeted Case-components branches. The relevant SQL and Worker files have no working-copy diff for FC-05. Their receipt/input/preview schemas, permissions, byte ceilings, queue, locks and money owners are not changed by this form. No new endpoint, reader field, action, proof, bank-detail block, producer, funding/settlement behavior or generic architecture is introduced. Future FC-02/FC-07/FC-08/FC-09/CM-13a remain outside this source batch.

The prior investigation's Advance cross-family hydration concern is historical: current FC-04 guard already refuses other families, and current FC-05 mirrors it. Accepted-awaiting-read state intentionally remains generic until actual Case facts arrive; the original journal does not retain all entered terms or certify the current window family. Existing current-page transport/actor/NEXT/token qualification remains; no cross-page endpoint metadata/assurance is added or claimed.

## Exact reviewed saved bytes

FE base HEAD at review: `df95a98209f11e75b57ae4f6e2b52dce45ca9299`; FC-05 product remains the reviewed working copy. SHA256:

| File | SHA256 |
|---|---|
| FE `js/bpay-next-manual-credit.js` | `A71D4BAB44527E4B57F14F6D44E19DA94247E8629CA0FCC9380865D890050552` |
| FE `js/bpay-next-case-preview.js` | `16914287A7F09824A4A9D9E8F99533209918970AE7A311660F9C17828A792CB2` |
| FE `js/main.js` | `92DA2244129E65FEC443411019EC06D8729EC0F1287752166AB2B037F92AC32B` |
| FE `css/bpay-next-office.css` | `DC976DD81D95E7FAC3555A1A9C2470782C4AB84C61C16E22AA4B6CB619D6FEB4` |
| FE `index.html` | `7DE983892C198B241D39BDF270E4C8A5E1BAA2F1582DA60622845A4032828E9D` |
| FE unchanged `js/bpay-next-office.js` | `CBDB844F11509A334CAB8E87FC0CDC247B6E73EBE3F741AC8CB5BE789E700745` |
| FE reused `js/bpay-next-payment-advance.js` | `CCCC07B702B6DE26AF6E9A13D2423C910DBFBDE5E9FB74DC0EACB19F93A5D46B` |
| BE creator `11102026_1536_banking_pay_next_case_create_intake_v1.sql` | `853DDCE45455DEFED9F715A2593267794F3B224E469A9EACCDE2AECA6E44A470` |
| BE reader `11102026_1738_banking_pay_next_case_list_v1.sql` | `AB912386DAF210E25A2D5B6F37038B00AAC8081CA0BA27E059DEFFDB17F47231` |
| BE `broker/src/bpay-next-command.js` | `91BDFD79D1DA7CF62214EE743284AAFD471F527308FA995CD0B86C5CE4EBDB94` |
| BE `broker/src/bpay-next-preparation.js` | `40C02030AFE7FBF15366D2EA9CFBFD19745A9A8E5C399B85B6BC04156025C646` |
| BD117 `FC-05-implementation-boundary.md` | `00C3ED55E9F4EFD772EF1FF6445250B5201DABD871B9B8A784607C6DA83ADD68` |
| D03 `FC-05-case-create-contract.json` | `8E75704F0A96C0DBBD334398D9D4D2384CFA59D73A690A109C878CD72B426A7C` |

No source test/compile or runtime result is inferred from reading these bytes. A4 must independently compare actual native states to original SCR-118/SCR-116, account for every visible difference/control and complete the full42 policy review. The owning £40 non-taxable due-once next-Draft NET_ADD, actual idempotence/queue/funding/payout and joined application paths remain AUTHORED_NOT_RUN / NOT_RUN.

### Current-source amendment — foreign-family refusal

Re-read the complete current Manual Credit file after the root's A4 P11801 correction. The only source change since historical `291C137B…` is inside the existing foreign-family refusal branch: the retained actual Candidate label is displayed outside the hidden form, and its empty owned footer is hidden through the existing `bpay-next-window-actions` pattern. Original family qualification, pending bytes, controller, input, actions, native frame and accepted callback remain unchanged. Current Manual Credit SHA256 is `A71D4BAB44527E4B57F14F6D44E19DA94247E8629CA0FCC9380865D890050552`; other listed source hashes are unchanged. **SOURCE PASS remains current.** Root reports final-source syntax passed; the reviewer did not execute it, and no runtime/A4/ joined acceptance is inferred from that report.

### Historical D03 document checkpoint — initial saved contract

Read the complete `design/FC-05-case-create-contract.json`, initial SHA256 `B92B0EDAF95DA0CC6DBACE0DD9DF5E8623C291D9090A4C1571134E6836057C65`, against the actual original schemas, creator/reader registry and source reviewed above. Owners1536/1738, NONE server/Worker change, original CASE_PREVIEW/CASE_CREATE/CASE_COMPONENTS methods/kinds, fourteen-field input reference, explicit tax, seven null UI operands, Comment/Note bounds, original receipts/family journal/readback, later-owner exclusions and AUTHORED_NOT_RUN boundaries match the source.

At this historical checkpoint, two document-only wording corrections were sent to root before D03 PASS: principal's “maximum two pence” needed “at most two decimal places (whole pence)” rather than imply a £0.02 amount ceiling; the future FC-09 label needed pre-activity edit/read-only restrictions rather than post-commit editing. The actual source already permits positive £40 and does not add future editing. D03 was pending those precise wording corrections at that checkpoint; existing native product SOURCE PASS was unchanged. The similarly ambiguous phrase in approved BD117 was reported without silently changing its approved bytes/hash. Both are closed by the current amendment below; no new implementation requirement results.

### Final D03 and phone-control source amendment

Re-read the complete corrected JSON contract, SHA256 `8E75704F0A96C0DBBD334398D9D4D2384CFA59D73A690A109C878CD72B426A7C`. Principal now states at most two decimal places (whole pence) with the original 1024-byte limit, and the FC-09 exclusion now states pre-activity edit/read-only restrictions. All original owners, exact existing kinds/actions, input/null/tax/Comment/Note facts, original family journal/receipt/readback and future AUTHORED_NOT_RUN assertions match the actual source. **FC-05-D03 document SOURCE PASS.** BD117 has the same corrected precision wording, current SHA256 `00C3ED55E9F4EFD772EF1FF6445250B5201DABD871B9B8A784607C6DA83ADD68`.

Reviewed the only later CSS delta for A4 P11802: an <=480px rule scoped to the native read window containing `.bpay-next-advance` gives existing header Close/footer buttons min-height/min-width44px. That existing form class is used by Manual Credit and Payment Advance, including unselected/foreign-family states whose footer lacks the shared preview class. It changes dimensions only, without hiding/enabling controls, changing ownership/lifecycle or affecting unrelated windows. Current CSS SHA256 is `DC976DD81D95E7FAC3555A1A9C2470782C4AB84C61C16E22AA4B6CB619D6FEB4`; other listed FE/BE source hashes are unchanged. **D03/D04 SOURCE PASS remains current.** Actual rendered touch targets, A4 and all joined application/money/queue acceptance remain separate and NOT_RUN by this reviewer.
