# FC-06 source review — sequence106

**SOURCE PASS** at the reviewed bytes below; no essential source defect found. Read-only review of root-authored product on11 October2026. This reviewer authored BD118, so this is an independent review of the implementation, not a claim of independent review of that design document. No product edits, tests, SQL/business execution, fixtures, rendering, screenshots or hosted actions were performed.

## Exact authority and scope

Reread the full approved FC-06 card at ordinary backend `BPAY_NEXT_BUILD_PLAN_20261009_codex_r3/03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:1130–1147`, all six local checklist rows at `docs/bpay-next/IMPLEMENTATION_CHECKLIST.md:3131–3154`, BD118 and the current investigation. Owning D03 contract and D04 native screen use FC-03/WB-08/F-16. Supplied SCR-119 B9 PNG/spec/index were individually inspected during that investigation; this source review makes no new actual-pixel claim.

The new native Manual Debt form and four existing private SQL owner successors close the mandatory-reference and reference-reader omissions. No new table/index, money formula, command/RPC, financial producer, queue or generic framework is added. Current unchanged terms/context/apply and original14-argument wrapper remain their owners. The actual closed input has **14 entered members**; CASE_CREATE adds original command_id/preview_token, not a second outstanding amount.

## Source findings and reasoning

| Area | Source conclusion |
| --- | --- |
| Explicit tax and required fields | `js/bpay-next-manual-debt.js:34–50,51–69` leaves both tax radios unselected, maps Comment to reason, requires original reference and preserves optional note. Fresh Preview cannot proceed with null tax/blank reference. Amount/weekly/affordability syntax is bounded, positive principal/weekly enforced, weeks bounded to bigint and reason/reference/note bounded to2048 UTF-8 bytes without NUL. |
| Schedule and affordability | The form validates genuine DD/MM/YYYY and Monday and selects only the entered weeks or weekly amount. It keeps blank threshold/floor null and real zero distinct. It uses shared server Preview; browser operations are date parsing/default-next-Monday and input validation, not money/schedule derivation. Server terms still own ceil-to-penny schedule/final partial instalment. Nullable Candidate default floor is retained. |
| Genuine channel | Native Candidate seed/picker channel is factual; unknown is not guessed. Preview's exact current context qualifies channel. PAYE-only tax hint sublines are hidden outside PAYE (`paintForm`). Existing missing-channel refusal is retained. No Umbrella allocation is invented. |
| Fresh versus replay | New SQL1910 Preview at9–12 refuses missing Manual Debt reference after unchanged terms validation. The17-argument accept returns the original exact prior receipt at80–98 before the fresh requirement99–101. Shared BE/FE input/token schemas retain nullable reference compatibility. Original14-argument signature/body remain untouched. |
| Old uncertain null reference | `restorePending` restores visible fields but does not rewrite the journal. A retained null reference displays blank; the shared footer detects pending independently of fresh validInput and invokes `controller.retry` at shared preview123. It hides fresh Preview and sends original saved bytes, UUID and token. It does not reinterpret the old null as a new empty-string request or fetch a replacement Preview. Existing controller's unknown/refusal retention rules remain unchanged. |
| Other pending Case | The new form blocks another retained CASE_CREATE family instead of submitting a Manual Debt over it. Office mount still validates original pending action/Candidate and original Case/run/worker scope. Accepted target recovery stays in the existing controller and targeted reader. |
| Actor, parent and owner | Main's qualified opener at15579+ captures actor, original stack prefix and parent context; after fresh owner begin it requires same actor/parent/current modal before opening. Native form `current`, active-frame and generation fences protect picker/Preview/Apply/late callbacks. Office requireActor also checks current NEXT owner readiness; transport checks it after awaited replies. Return callback rechecks current context after awaiting onCaseCreated. No current-bank intent or legacy/edit owner is changed. |
| Acknowledgement and return | Shared footer reads exact CASE_COMPONENTS after the validated receipt, refuses wrong Case/Candidate, retains Refresh while the Case is not yet visible, and calls finishCaseCreation only after the qualified onApplied callback. The new form returns the actual Case/Candidate/page, then closes only its active native frame. |
| Saved reference readers |1910 component reader197–200 and Case-list row334–337 perform exact Case/Candidate/MANUAL_DEBT point lookup into immutable original request. Unique case_id already exists. Only original_liability_reference is added; no extra note/provenance payload, list count/sort input or financial figure changes. Old/other origins return null. |
| Closed output | BE preparation CASE_COMPONENTS/CASE_LIST and FE Office CASE_COMPONENTS require the new member with nullable value. Non-null string must be nonblank, NUL-free and within2048 UTF-8 bytes, and the row must be MANUAL_DEBT/MANUAL_DEBT. Missing member, wrong family, malformed identity/cursor and oversize reply continue to fail closed. |
| Native reachability | Main's actual NEXT Banking menu adds only New Manual Debt and qualified original creation entry. LEGACY and edit branch remain. Index loads the module after shared Office/Case code and bumps changed main/Office/CSS URLs. New CSS reuses the scoped native Advance layout and phone44px selectors. No future Case-page screen is built. |

Static complete-definition comparison confirmed each of the four SQL1910 bodies, after removing only its named mandatory-reference or saved-reference insertion, equals its exact current1536/2026/1738 parent. This is a source comparison, not SQL execution. The four signatures in parent provenance and final registry agree with the successor; each retains postgres ownership and explicit PUBLIC/anon/authenticated/service_role revocation.

The form keeps one inline error, entered fields and live-copy marker; shared Preview/Apply handles busy, changed, unknown and accepted states. Native view remount preserves already-owned nodes/buttons via unchanged adapter. Actual geometry, copy differences, phone behaviour and all policy rows still require the separate A4 receipt.

## Reviewed bytes

SHA-256:

| Source | Hash |
| --- | --- |
| `supabase/repeatable/11102026_1910_banking_pay_next_manual_debt_reference_v1.sql` | `644995fb19e4a58d0854de8191980dc7e543015358d81d229785165377e2845c` |
| `broker/src/bpay-next-preparation.js` | `d98553f186257186e0680bac2617b644871e222eb59705d66df1d9029f82ace8` |
| unchanged BE case contract | `91d2bc7499d1b786ef743e5ae1a9997446d90d4fe1fe84647e84d0892b3fc135` |
| `js/bpay-next-manual-debt.js` | `228df8476030356857f3fc3cfd9d6b8272cb9aa9500448a1b3874efab92b651d` |
| `js/main.js` | `74d30eb024f6ae5b0bde73d590a3af5ef2b93a37fb1d09bab1eb3560f889a07c` |
| `js/bpay-next-office.js` | `1cc98547542c99652c6b0e6c5139e64c9ac8a4dbb2e24c782ec0eda29b9b5b77` |
| unchanged shared Case preview | `16914287a7f09824a4a9d9e8f99533209918970ae7a311660f9c17828a792cb2` |
| unchanged native window adapter | `718c2ac4bd4827cbdd07218fa846db5ccfaf9f5457fea0ff7ef7b4fcc8911462` |
| `css/bpay-next-office.css` | `b58a7eee24f26721fc9967ec46f0d2a30d256b7f327a2d4be82f4582f4de33cb` |
| `index.html` | `0215f79b7f5829c3146104863cbe5b910ba531289919a59987d9720c0f61359d` |
| BD118 at review | `9b83b27a84b16de5bfedf2474e8515c7674889e19f9b8f779311bbf681323e4a` |
| parent provenance | `8833606b3dda389ce64b61bf1356d862b95f420578d4b98e500fbf1feba7dfeb` |
| current final-definer registry | `674bef14fc610cc56d448f1588cfd456245b9f2379f047c7bad8410fb20f0b89` |

SOURCE PASS does not mark A4/installed/application boxes or whole ITEM complete. £120 taxable/4weeks/INV-2291 → native £30 weekly gross-stage PAYE Draft, entered-NET recovery, later Umbrella recovery and visible saved Case reference remain joined acceptance **NOT_RUN** in this review.

