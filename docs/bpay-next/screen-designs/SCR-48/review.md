# DR-18 SCR-48 independent A4 final review

12 October 2026. **A4 PASS / APPROVED for the implemented DR-18 presentation slice: 33 PASS, 0 FAIL, 9 N/A (all42 items).** Independently viewed all24 current actual PNGs:11 SCR48 and13 SCR49. No unexplained visible difference or remaining screen finding in this slice.

**D02/ITEM remain BLOCKED_ON_DR28_HANDOFF.** This approval does not close actual NEXT Payment Batches main/menu integration, joined Open/Close callbacks, real Confirm/expiry race, background queue, or Workbench In Draft acceptance. Those are AUTHORED / NOT_RUN. Local injected callbacks and fixed-response screenshots prove presentation only. Reviewer performed no product/pack edits, render/capture generation, tests, database/RPC/hosted/business action or commit.

Owning evidence: full DR18 card03:473–496 and checklist D01–D05; BD138/build-scope/screen-investigation; current native Office source; read-only design index/inventory, SCREEN_DESIGN_POLICY and tools/policy.json. SDP12a/ownerA3a removes the old card pay-date field; ownerA3g requires one plain inline error and next step. Items16/25/26 follow the owner's manual-wording/spec/readerAdditions rulings. Existing parent review content is context, not reapproval of its whole card.

Final findings: P18-P01 CLOSED by native13px bordered table host and desktop right-aligned/tabular Candidates/Amount. P18-P02 CLOSED by preserved role=status Opening Draft… / Opening Payment Batches… while controls are disabled, followed by role=alert on failure. P18-P03 CLOSED by “This Draft has expired; create a new Draft from Banking Pay.” and truthful sample start09/10/2026 07:20:14, deadline12/10/2026 07:20:14 before Updated07:35:00. Final refused and pending/failed-open desktop/phone bytes were reopened after correction.

Supplied-vs-actual visible differences and control purpose (references viewed alongside actual PNGs):

- B4/png/SCR-48_confirm_draft_dark.png and SCR-48b_confirm_draft_refused_dark.png are the current references. Native small600px confirmation retains its existing body heading/outlined accent-token question icon, ordinary native footer/control geometry and live parent backdrop; the illustrative filled blue icon/header divider/background rows are not a new dialog framework or fabricated app data. This is the original card-owned native confirmation adapter, with the same clear question/consequence/safe-first controls.
- Sample225/148/77 Candidates and £38,912.40/£19,573.54 become actual whole2/channel1+1 and £169.10 gross/£282.00 incVAT. PAYE-only/Umbrella-only questions show only their nonempty native channel with singular grammar. Line wrapping and window height follow those real facts, not the sample length.
- The owner requires no pay date and no new capabilities just to fill the picture. Native REVIEW_TOTALS fields candidate_count, pay_channel, gross_ex_vat/gross_inc_vat supply all displayed new figures; unknown bank cash is not inferred from gross.
- SCR48b's separate error dialog/Go to Banking Pay/illustrative expired timestamp become one original-Review inline alert with the required new Draft/Banking Pay remedy and existing safe Close/Refresh. OwnerA3g requires inline one-sentence errors; the actual refusal supplies no completed-expiry timestamp or new navigation callback. The retained started/deadline/update facts are genuine fixed reader fields. No second error window/unsupported Go capability is added.
- Phone stacks/wraps the same question and retains native44px buttons; no subtitle repeats the parent actor/count. Device-light keeps the identical dark palette. Parent review figures/rows in refused captures are context; this receipt approves the new alert/deadline truth, not all older parent screen work.

Every new control is needed: Cancel abandons the question safely; Confirm Draft accepts the already reviewed revision through the original journal; header Close exits the original result/refused parent; existing Refresh obtains current status after refusal. No extra control is introduced for an unsupported picture fact.

Full42 policy checklist (each PASS is limited to the implemented slice):

| # | Policy check | Result | Evidence / scope |
|---|---|---|---|
| 1 | Card capabilities | PASS | Scoped original Review Confirm question and mixed/single-channel result are callable; actual D02/DR28 join remains blocked above. |
| 2 | Legacy capabilities | PASS | Original native confirmation/read-window/controller retained; source preservation, not claimed LUI099/LSV158 business journey. |
| 3 | Required wording | PASS | Confirm Draft, consequence and actual channel/count/gross labels; definite refusal gives Banking Pay/new Draft next step. |
| 4 | NEXT behaviour | PASS | Shows accepted review figures and nonempty channels only; no repricing, pay date or fabricated payment outcome. |
| 5 | Frame/title/Close | PASS | Native small confirmation question; Cancel safe exit. Inline refusal stays in its original titled Review frame/header Close. |
| 6 | Tabs | N/A | New confirmation/refusal slice adds no tabs. |
| 7 | Body order/cards | PASS | Question → consequence → factual summary → two-button footer; refusal alert precedes retained parent facts. |
| 8 | Universal table | N/A | Question is a compact factual paragraph, not a list; unchanged parent table is outside this slice. |
| 9 | Badge tones | N/A | No new status badge in confirmation/refusal; original parent Ready badges are contextual. |
| 10 | Primary/footer | PASS | Cancel first secondary, Confirm Draft last sole primary; refusal cannot continue Confirm and uses existing safe Close/Refresh. |
| 11 | Native pieces/CSS | PASS | Native adapters/tokens and modern-modal-scoped additions; no new !important or generic theme override. |
| 12 | Continuous loading | N/A | Fixed one/two-channel confirmation has no paged list. |
| 13 | Sorting | N/A | SDP34 fixed confirmation summary/dialog exemption; no artificial sortable field. |
| 14 | Ticks | N/A | Post-Create confirmation has no selection controls. |
| 15 | Whole-scope counts | PASS | Whole/channel candidate_count and money are REVIEW_TOTALS facts, not page count or browser arithmetic. |
| 16 | Manual wording | PASS | Manually checked tools/policy.json neverShow/chatter/rewrites/formatRules; no software codes, chatter or overlong error sentence. |
| 17 | Errors | PASS | Actual expiry refusal is one plain inline sentence with a new Draft/Banking Pay next step; no invented expiry timestamp. |
| 18 | Confirmations | PASS | Meaningful question/consequence, safe Cancel first, verb Confirm Draft second; no one-click confirmation. |
| 19 | Empty/loading/failure | PASS | Source disables unavailable confirmation and retains real facts on refusal; no false £0.00 from failed reads. |
| 20 | Vocabulary/IDs | PASS | Draft/PAYE/Umbrella/Candidates are business words; no UUID, protocol or error code visible. |
| 21 | UK formats/missing | PASS | Actual £169.10 gross / £282.00 inc VAT; DD/MM/YYYY/24-hour parent facts; absent facts are not invented. |
| 22 | Keyboard/ARIA | PASS | Native real buttons/focus path; question accessible heading and decorative aria-hidden icon; refusal role=alert. |
| 23 | Phone | PASS | All mixed/single-channel phone questions and refused phone viewed; wrapped copy, no sideways overflow, reachable44px controls. |
| 24 | Device light | PASS | All three normal dark/device-light pairs independently viewed and SHA256 identical; refusal uses same native dark palette. |
| 25 | Spec regeneration | N/A | Owner excludes spec/pack-tool regeneration; evidence is actual local native screenshots. |
| 26 | Reader additions | N/A | Owner excludes spec readerAdditions; exact displayed field mapping is recorded below, no new value invented. |
| 27 | Pay date | PASS | No pay date appears; shown dates are factual started/figures/expiry/read times in retained context. |
| 28 | Understandable | PASS | Question/count, one/two named Drafts, consequence and gross/incVAT amounts are plain. |
| 29 | Action placement | PASS | Cancel/Confirm at question footer; refusal guidance appears before review facts. |
| 30 | Next step | PASS | Confirm is the obvious requested action; refusal explicitly requires a new Draft from Banking Pay. |
| 31 | Who/how much/why | PASS | This confirmation aggregates the already reviewed Candidates by channel and states amounts; identity detail remains in original Review. |
| 32 | Blocking reasons | PASS | Actual expiry reason and remedy are visible; retained actions are disabled rather than suggesting confirmation succeeded. |
| 33 | Action feedback | PASS | New refusal visible in the same original Review window; successful presentation is separately approved SCR49. |
| 34 | Needed content | PASS | Only question/consequence/channel facts; no future execute controls, technical justification or invented status/reference. |
| 35 | Consistent names | PASS | Confirm Draft/PAYE Draft/Umbrella Draft/Banking Pay remain consistent with supplied designs and card. |
| 36 | Mistake prevention | PASS | Safe-first question plus current actor/run/review-revision journal guards preserved in source; no dispatch by reviewer. |
| 37 | Safe exit | PASS | Cancel leaves question without confirmation; original header Close remains reachable after refusal. |
| 38 | Fewest clicks | PASS | Existing Review button opens one native question; refusal inline rather than an extra error window. |
| 39 | Figures/counts | PASS | 2 Candidates equals1 PAYE+1 Umbrella; single-channel questions show1 and only its native amount. |
| 40 | Before Create Draft | N/A | This is confirmation after the initial Create/Review boundary. |
| 41 | Edge/busy/keyboard | PASS | Mixed/PAYE/Umbrella and definite refusal actual desktop/phone coverage; hundreds of rows/real races/keyboard dispatch NOT_RUN. |
| 42 | Return/context | PASS | Native question returns to the original Review; refusal keeps current titled frame/context. Joined parent route acceptance NOT_RUN. |

Current source SHA256 (saved disk):

| File | SHA256 |
|---|---|
| FE js/bpay-next-office.js | e179e0b9f443d7e97eed95cf88e7e5b50a3465d8f61a7ed5fef3b6ab974781af |
| FE css/bpay-next-office.css | 2aa4fa2f24665e8ff8f64b1eafc76c386d4ad80f9523d2a4c0a951bad8d1f194 |
| FE index.html | 3e34174753fe71d17c5ce4b4cc5bc23bdb0d58a9ae5ca9c90d9cc3b66a2898fc |
| FE js/modal-modernisation.js | 5eea89b05fbea6320656bfddb7dcab6574afc742563f6e2b4e7885f732e610e5 |

Reviewed source is root's b5678df/1cc6b07 chain plus the saved P03 copy correction; receipt binds exact bytes rather than claiming an unpublished commit. Source lifecycle/controller review remains in ../../reviews/DR-18-office-review.md; no reviewer runtime acceptance.

Read-only references: SCREEN_DESIGN_POLICY.md SHA256 8bc212b174469236f2b46dbf70eadc3664a946636458321cd3c77b7146323de5; tools/policy.json 45036d841d4568aa2d4d7adf4f063d6d17b70288e289ee0b285068797a87369d; DESIGN_INDEX.md cba0ef6e87c8937d558b20ef9c9ee010fcc9f9a8b479b626351206d07076cffe.

Actual independently viewed image/evidence manifest (SHA256):

| File | SHA256 |
|---|---|
| DR18-confirm-mixed-dark.png | c1a76733f65904f92abf4ef7cdc81bcc3742a63dd6c7ab3b2856752578d56de6 |
| DR18-confirm-mixed-device-light.png | c1a76733f65904f92abf4ef7cdc81bcc3742a63dd6c7ab3b2856752578d56de6 |
| DR18-confirm-mixed-phone-dark.png | 44c858a7c100d1308c2342438ad38c6b333ee927b08048ba1496d0d81ef58540 |
| DR18-confirm-paye-dark.png | c734c299b60ffb1400b2718768d73d2620c664c64f48011132da031d73b70d84 |
| DR18-confirm-paye-device-light.png | c734c299b60ffb1400b2718768d73d2620c664c64f48011132da031d73b70d84 |
| DR18-confirm-paye-phone-dark.png | 6a70351a1a2240e79134bcb689c297bdf5879ef46a40c89bc61b590589e8e9b6 |
| DR18-confirm-refused-dark.png | c57a78b90825dc4ddfcdf071856066cb0b3797c5658f4f4c0aae6443a781aa28 |
| DR18-confirm-refused-phone-dark.png | 51cfad9e4f7470521e90a0c04c73da92de4c33e6128d92e00adaffa50b672e71 |
| DR18-confirm-umbrella-dark.png | 607edfa9629b9d7762935b8f1d3bb4e1b3529e96c813f2ebc3b94ea1e1d3f795 |
| DR18-confirm-umbrella-device-light.png | 607edfa9629b9d7762935b8f1d3bb4e1b3529e96c813f2ebc3b94ea1e1d3f795 |
| DR18-confirm-umbrella-phone-dark.png | 33fe41c893064056759bccb4a44c10e47d2efefede6a7dafcbf1f082a6f20d1f |
| DR18-fixed-responses.json | e6fc0b582127a2b02ec43ffda87e32b69bf09c0576064d39bb209a79c8dd67ad |
