# DR-18 SCR-49 independent A4 final review

12 October 2026. **A4 PASS / APPROVED for the implemented DR-18 presentation slice: 33 PASS, 0 FAIL, 9 N/A (all42 items).** Independently viewed all24 current actual PNGs:11 SCR48 and13 SCR49. No unexplained visible difference or remaining screen finding in this slice.

**D02/ITEM remain BLOCKED_ON_DR28_HANDOFF.** This approval does not close actual NEXT Payment Batches main/menu integration, joined Open/Close callbacks, real Confirm/expiry race, background queue, or Workbench In Draft acceptance. Those are AUTHORED / NOT_RUN. Local injected callbacks and fixed-response screenshots prove presentation only. Reviewer performed no product/pack edits, render/capture generation, tests, database/RPC/hosted/business action or commit.

Owning evidence: full DR18 card03:473–496 and checklist D01–D05; BD138/build-scope/screen-investigation; current native Office source; read-only design index/inventory, SCREEN_DESIGN_POLICY and tools/policy.json. SDP12a/ownerA3a removes the old card pay-date field; ownerA3g requires one plain inline error and next step. Items16/25/26 follow the owner's manual-wording/spec/readerAdditions rulings. Existing parent review content is context, not reapproval of its whole card.

Final findings: P18-P01 CLOSED by native13px bordered table host and desktop right-aligned/tabular Candidates/Amount. P18-P02 CLOSED by preserved role=status Opening Draft… / Opening Payment Batches… while controls are disabled, followed by role=alert on failure. P18-P03 CLOSED by “This Draft has expired; create a new Draft from Banking Pay.” and truthful sample start09/10/2026 07:20:14, deadline12/10/2026 07:20:14 before Updated07:35:00. Final refused and pending/failed-open desktop/phone bytes were reopened after correction.

Supplied-vs-actual visible differences and control purpose (references viewed alongside actual PNGs):

- B4/png/SCR-49_draft_created_dark.png / SCR-49b_draft_created_one_channel_dark.png define the small native result, PAYE-first order, framed table, outlined row Open and primary Go to Payment Batches. All are now present, including the restored13px frame/right-aligned numbers. Actual native font/spacing/header-gradient/backdrop follow the retained Office adapter rather than fabricated background rows.
- Sample counts148/77 or79 and amounts£38,912.40/£19,573.54/£19,980.12 become actual channel count1, £169.10 Gross PAYE and £282.00 Inc VAT Umbrella. One-channel results legitimately omit the other channel; desktop column widths and height adapt to the actual complete receipt.
- PB-0412-P/U/PB-0413-U, Awaiting PAYE net and Awaiting approval sublines are omitted: the immutable returned address exposes only run_id/pay_channel, not a human reference or approval state. OwnerA2/BD138 forbids fabricating them or adding a new reader for the picture. Candidate_count/gross_ex_vat/gross_inc_vat are current native REVIEW_TOTALS channel facts; gross is never falsely labelled PAYE bank/net amount.
- On phone the fixed table becomes native labelled cards; actual amount and Gross/Inc VAT caption remain together in one value block. Header Close/row Open/footer Go are44px and no sideways scroll occurs. Device-light pairs are identical to dark.
- New pending/failed Open states add the card/policy-required original inline notice before facts: neutral Opening Draft… status while navigation buttons disable, then red one-sentence alert and restored controls after one fixed failed read. These are SDP79/17 necessities, not a second error screen. Missing navigation integration is an honest future DR28 dependency, not evidence that an actual Payment Batch opened.

Every control is needed: Open PAYE/Umbrella Draft inspects the exact returned channel via the existing guarded native sheet; Go to Payment Batches is the card's direct next-step callback seam (actual host deferredDR28); header Close safely dismisses the result. Focusable row Enter/double-click is the existing equivalent Open path, not another visible action. There is no tick, sorter, speculative approval/execute action or duplicate footer Close.

Full42 policy checklist (each PASS is limited to the implemented slice):

| # | Policy check | Result | Evidence / scope |
|---|---|---|---|
| 1 | Card capabilities | PASS | Scoped original created result offers each returned nonempty channel and Payment Batches callback; D02 real DR28 integration remains blocked. |
| 2 | Legacy capabilities | PASS | Original native read-window/sheet callback seam retained; source-only preservation, no claimed installed LUI099/LSV158 journey. |
| 3 | Required wording | PASS | Payment Draft created; Open PAYE Draft / Open Umbrella Draft; Go to Payment Batches; Gross/Inc VAT remain explicit. |
| 4 | NEXT behaviour | PASS | PAYE first, one row per actual returned channel; no Source/ordinary split, pay date, assumed approval state or PAYE cash. |
| 5 | Frame/title/Close | PASS | Standard small native titled result with header Close; no duplicate footer Close. |
| 6 | Tabs | N/A | Fixed created result has no tabs. |
| 7 | Body order/cards | PASS | Status/error above factual channel table/cards, then sole next-step footer; one native bordered host without decorative nested boxes. |
| 8 | Universal table | PASS | Native13px bordered/rounded host; desktop Candidates/Amount right-aligned/tabular; two-row fixed table needs no scrolling. |
| 9 | Badge tones | N/A | No invented approval/status badge belongs to this two-field address receipt. |
| 10 | Primary/footer | PASS | Row Open buttons outlined secondary; Go to Payment Batches sole footer primary, header Close secondary. |
| 11 | Native pieces/CSS | PASS | Existing universal table/window/buttons and modern-modal-scoped additions; no new !important. |
| 12 | Continuous loading | N/A | Complete fixed receipt contains at most two channels, not a paged list. |
| 13 | Sorting | N/A | SDP34 dialog exemption plus mandatory PAYE-first order; no meaningless arrow or new sort. |
| 14 | Ticks | N/A | Read-only successful result is not a selection list. |
| 15 | Whole-scope counts | PASS | Native REVIEW_TOTALS channel candidate_count / gross_ex_vat or gross_inc_vat, never local row sums. |
| 16 | Manual wording | PASS | Manual policy.json neverShow/chatter/rewrites/formatRules review; Opening Draft… and one-sentence failed-open remedy are plain. |
| 17 | Errors | PASS | Actual failed Open shows inline red role=alert “Could not open this window; try again.” before facts; controls restore. |
| 18 | Confirmations | N/A | Open/Go/Close are navigation; original confirming question is separately approved SCR48. |
| 19 | Empty/loading/failure | PASS | Hidden empty notice leaves no stripe; opening status explains disabled controls, failure preserves actual confirmed figures. |
| 20 | Vocabulary/IDs | PASS | No native UUID, internal address, assumed human PB reference or technical error visible. |
| 21 | UK formats/missing | PASS | Literal £169.10 Gross / £282.00 Inc VAT; unsupported reference/status omitted, unknown money uses existing em dash. |
| 22 | Keyboard/ARIA | PASS | Native buttons plus focusable rows/Enter/double-click source path; selected row aria-selected; pending status and failed alert roles. |
| 23 | Phone | PASS | All three normal phone results plus Opening/failed Open viewed; labelled cards, intact amount/caption,44px reachable actions, no sideways overflow. |
| 24 | Device light | PASS | All three normal dark/device-light pairs individually viewed and SHA256 identical; added states use same native palette. |
| 25 | Spec regeneration | N/A | Owner excludes generated spec/pack rendering; actual native local captures are evidence. |
| 26 | Reader additions | N/A | Owner excludes readerAdditions step; exact current fields/unavailable picture facts mapped below. |
| 27 | Pay date | PASS | No execution pay date or speculative future payment date. |
| 28 | Understandable | PASS | Each actual channel, Candidate count, gross/incVAT amount and next step is readable. |
| 29 | Action placement | PASS | Specific Open beside its channel, Go to Payment Batches in footer, safe Close in header. |
| 30 | Next step | PASS | Go to Payment Batches is sole primary; channel Open permits immediate inspection. |
| 31 | Who/how much/why | PASS | Channel/count/amount answer the created-Draft aggregate task; unsupported PB reference/approval facts stay absent. |
| 32 | Blocking reasons | PASS | Actual opening status explains disabled navigation; failure states what happened and how to retry. |
| 33 | Action feedback | PASS | Opening status visible in current result; failed Open produces inline alert and restores controls. No joined-success claim. |
| 34 | Needed content | PASS | Only returned channels, corresponding actual totals, navigation and safe exit; no extra future capability. |
| 35 | Consistent names | PASS | Payment Draft/PAYE Draft/Umbrella Draft/Payment Batches are the same named concepts across question/result. |
| 36 | Mistake prevention | PASS | Read-only navigation; exact run/channel addresses plus original actor/current frame checks preserved; confirmed receipt cannot be reconfirmed here. |
| 37 | Safe exit | PASS | Native header Close remains reachable during opening and failure; no artificial footer Close. |
| 38 | Fewest clicks | PASS | Direct per-channel Open and direct Batches next step; no detour or duplicate one-line expanded parent. |
| 39 | Figures/counts | PASS | OnePAYE+oneUmbrella =2 Candidates; each single-channel result1; result amount equals its confirmation/native totals. |
| 40 | Before Create Draft | N/A | Already confirmed presentation, not initial selection. |
| 41 | Edge/busy/keyboard | PASS | Mixed/single-channel, opening and failed Open actual desktop/phone viewed; real queue, large-list and business keyboard journeys NOT_RUN. |
| 42 | Return/context | PASS | Created result remains titled with confirmed facts through failure; original guarded native return path is source evidence only. |

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
| DR18-created-mixed-dark.png | 3c22c0c77183ebe96d38cd5a593229484b5f4d8d75c636f1022d4967a4d42533 |
| DR18-created-mixed-device-light.png | 3c22c0c77183ebe96d38cd5a593229484b5f4d8d75c636f1022d4967a4d42533 |
| DR18-created-mixed-phone-dark.png | 887f69df2ec16e29fecce9f3e6d1632041898736a9bad8e48f8c17e8f425e3a9 |
| DR18-created-open-failed-dark.png | 80a9e7db1a2e0bb90d97d3786c1c3ebc81a7274674beda27bb6eb39af28f5c2a |
| DR18-created-open-failed-phone-dark.png | 0ee70a5a19c2fc66e77457954f14468738e6e1d687307348736c1db817735a73 |
| DR18-created-opening-dark.png | a52f4259971ff1bd1423edab0cff36a348dd1b268a68caf22ad2543bc3fdc934 |
| DR18-created-opening-phone-dark.png | 64f7eb44ec65040e7d82e3d921c596768a9b9b646f659a714fc3ace5ed42bed2 |
| DR18-created-paye-dark.png | 6cd41904e81151e7f3d72977377fe9163554b338d1193f47e32eeb4280cd30b9 |
| DR18-created-paye-device-light.png | 6cd41904e81151e7f3d72977377fe9163554b338d1193f47e32eeb4280cd30b9 |
| DR18-created-paye-phone-dark.png | b69b6d12b99bc27679401d76d9c1c7f1d49f86bfa8913a362b38720a7132d414 |
| DR18-created-umbrella-dark.png | 7555d440f762a7e00e6cd0cd4210ca9c1665295bb1d5c11bb5ede9a4d9d05d70 |
| DR18-created-umbrella-device-light.png | 7555d440f762a7e00e6cd0cd4210ca9c1665295bb1d5c11bb5ede9a4d9d05d70 |
| DR18-created-umbrella-phone-dark.png | be205a16f4572ac99f0f7af7d63fcb402d56de98d5f9db8bf90b4f898a10dcd3 |
| DR18-fixed-responses.json | e6fc0b582127a2b02ec43ffda87e32b69bf09c0576064d39bb209a79c8dd67ad |
| DR18-mixed-local-replies.json | cd95f07286961a815eaa78a40779ef8f35dfbdf9d01701d8aaaf7c1205b0964b |
| DR18-paye-local-replies.json | fc3a9c429dfd2baf74728f67eafcf0f91e4b7e28b395a2e72437054f264ab43a |
| DR18-umbrella-local-replies.json | 24044c81efb96cb0a68b366d572f9d32810fb4ce89b8ff3f80abad98e31241f7 |
