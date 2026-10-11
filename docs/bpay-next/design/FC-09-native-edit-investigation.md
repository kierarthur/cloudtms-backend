# FC-09 native edit investigation

11 October 2026; sequence 107. Independent read-only source/design investigation at root's direction. Product, checklist, database and runtime remain unchanged. This is an implementation boundary, not A4 or joined acceptance.

## Required narrow boundary

The FC-09 card (03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md, lines 1196–1215) and all eleven current checklist rows require pre-activity editing of Payment Advance, Manual Credit and Manual Debt through CASE_EDIT. Owning D02/D03/D05/D06/D08/D10 remain required. Original CASE_CREATE input/receipt is immutable; the accepted amendment and its exact rule/component/period provenance must be independently retained. FC-16 accepted PREPARING binding can block editing before an ACTIVE hold exists. Zero displayed funded/recovered amounts alone do not prove eligibility.

Reuse the three existing native creators, the standard modal adapter, the shared Case Preview/footer and the original actor-scoped controller. Extend existing CASE_COMPONENTS and CASE_PREVIEW with finite edit metadata/variant and add the card's CASE_EDIT action/receipt. No new Case page, generic editor, bank writer, financial calculator, allocation owner, preflight store, command family or modal framework is needed. The separate root boundary/kernel investigation owns the exact amendment and opening-period implementation.

Applicable BE/FE AGENTS and the full FE BANKING_PAY_BIBLE.md were read. Policy X, Rule B, current disk authority, immutable financial receipts, exact authority adoption and source-only evidence apply. Read-only scope does not authorise the Bible's implementation tests or release actions.

## Actual seams and minimum changes

| Current source seam | Required edit boundary |
|---|---|
| The three creator modules reject any seed finance_case_id/case_id/id. | Add an explicit closed edit mode; seed IDs locate an authoritative read only. Removing the rejection and trusting seed terms would be incomplete. Preserve CREATE unchanged. |
| main.js native openers qualify NEXT only for a seed with no existing Case ID. An existing Case therefore reaches the legacy reader/writer path. | Admit supported existing-Case NEXT edit before legacy reads, using the current admin/actor/module owner and immutable current-window API. LEGACY callers keep their original route. |
| Each creator uses .case-create.<Candidate> and restores only its exact family input from pending bytes. | Give editing its own actor/Candidate/Case-qualified journal, separate from creation. Restore only the validated original edit request; do not mint new intent while the prior request is uncertain. |
| Office CASE_PREVIEW request and the shared Case module accept only the closed CREATE input/context schema. | Add an explicit closed edit variant binding Case ID, expected Case revision and exact current terms/provenance to the same input calculator. Mirror the actual root contract in Worker and Office; do not permit arbitrary optional keys or weaken the CREATE schema. |
| mountCasePreviewFooter (Office lines 1936–1951) hardcodes CASE_CREATE, accepted Case ID, create copy and finishCaseCreation. | Select the explicit edit action, validators, labels, accepted metadata and acknowledgement operation. Reuse the injected preview/submit/retry/readCase callbacks and native footer. |
| Controller v5 keeps immutable pending action/bytes but its Case adoption records only case_id. | Persist the closed CASE_EDIT receipt/readback identity needed to distinguish the existing target from an accepted edit. Case ID alone is inadequate. Adopt older journals only through an explicit exact schema migration; do not discard or reinterpret uncertain requests. |
| Shared refreshCase currently acknowledges any non-empty same-Case component page. | For editing, require the exact returned Candidate/Case and accepted new Case revision/rule/amendment evidence. Old rows from an existing Case must retain the accepted-awaiting-read state, not report Case changed. Clear the journal only after exact authoritative adoption/callback succeeds. |
| Office already has one bounded CASE_COMPONENTS inspection surface. | Reuse a small qualified Edit/inspection entry and targeted Candidate/Case refresh, rather than constructing FC-02's full Cases page early. Mutation availability comes from the native reader/guard, not visual status or amounts. |

The existing native action names are banking:finance:editPaymentAdvance, editManualCredit and editManualDebt (main.js lines 102719, 102736 and 102792). Current availability relies on legacy Sheet data; it is not proof of NEXT eligibility. The existing original modal's actor, bound-context generation, current owner-client epoch, frame alive/onMount and late-response guards must also cover edit reads, Preview, Save, Retry and accepted readback. Child picker callbacks are irrelevant for a locked edit Candidate; no Candidate picker/change action is needed in edit mode.

The retired no-caller bank-edit boundary is finite: banking:finance:editBankDetails at main.js lines 102753–102789 and its sole helper canEditBankDetailsForFinanceCase at line 100495. Whole-source search found that helper's declaration and only that branch call. Delete only this branch and its sole helper under the card's RQ-090 requirement; preserve genuine Candidate-bank navigation and all ordinary banking controls.

## Authoritative prefill and protection

Current private.bpay_next_current_case_component_page_v1 in repeatable 1910 (lines 155–220) checks the exact Candidate/Case and OPEN/PAUSED lifecycle, returns ordered components with bounded limit <=100 and a 120,000-byte response bound, and exposes current principal, rule/schedule, tax/stage/channel and native monetary facts. It returns rows/complete, not full edit context. It does not currently provide exact Case revision, current Reason/Note, chosen week-count-versus-weekly-amount input mode, complete immutable amended metadata or complete protection/status presentation.

The smallest necessary addition is finite edit context in that same reader, bound to the exact Case/Candidate/component/current rule and original creation or explicitly accepted amendment. Reuse existing native Case row and FC-16 protection helpers for factual name/reference/status/creator/protection rather than introducing another list/read authority. Root must specify the exact keys before mirror validators are authored.

Prefill requirements:

- Candidate, family/subtype, Case/component identities, source channel/currency and original creation/funding age are locked factual values. No seed-label or live settings fallback replaces captured origin.
- Amount and supported tax treatment come from the exact current approved terms. No independently editable Outstanding amount and no browser financial arithmetic.
- Schedule preserves the approved existing start Monday and the original/amended chosen week_count or weekly_due input, never the create form's next-Monday/default values or a reverse calculation from derived preview fields.
- Null threshold/floor remains null; genuine zero remains zero. A Candidate-default floor is labelled as such and Preview supplies its actual current policy value.
- Current Comment/Reason/Note and Manual Debt reference come from the exact origin/amendment metadata. If reference is editable, return current amended metadata while retaining the original immutable liability reference; if root treats it as root identity, make it factual read-only and record that choice.
- Eligibility, no-activity and Batch blocked are native facts, not inferred from Not paid out yet or from no ACTIVE hold. Protect accepted pending binding even with no Worker/hold and show unknown protected amount as unknown, never £0.
- Existing FC-16 protection output supplies native binding/run/channel/status/actor/protected amount and preparer display name. It presently supplies no human NEXT Draft reference. Show an actual reference only if the original owner supplies it; do not substitute UUIDs, fabricated PB references or an official pay date.

If prefill read fails, retain inspection identity and one plain read failure with Refresh; Save/Preview stay unavailable. A missing/malformed/mixed component or provenance tuple is a truthful refusal, not an editable default form. A fully locked activity/committed/protected Case still permits factual inspection; it does not permit partial metadata saving.

## Preview, journal and exact completion

Preview remains read-only, uses the original terms calculator and includes the full entered edit tuple plus exact Case revision/current provenance. Any changed input invalidates the Preview token. Admission and ordered apply re-prove native activity/protection; UI preflight cannot authorise a save by itself.

Before Save, the existing controller stores the exact CASE_EDIT action/request bytes and original command UUID. Unknown outcome, Close/reopen and later auth/currentness refusal retain that request. Try again replays identical bytes, not a fresh token, expected revision, command or input. A conflicting create/edit request cannot be hidden or replaced. Busy/pending/accepted states lock all editable fields and hide redundant Preview.

An accepted edit needs a closed typed receipt carrying the exact original target and new revision/provenance required by the root contract. The existing Case row's mere presence is not completion. While its new projection is pending, show accepted-awaiting-read with Refresh and real Candidate/Case facts, retaining only original journal-backed terms when available. If the journal has no terms for a recovered accepted receipt, hide editable fields instead of fabricating them.

Readback requires the exact accepted new revision and current rule/amendment/component lineage, with complete supported membership. Old revision, wrong Candidate/Case, partial mixed rows or failed read remain pending. Only that exact readback may acknowledge the edit, target-refresh the named existing Case/Candidate surface and expose the resulting current state. Preserve original created age and old receipt/history; no whole Workbench rebuild or original creation acknowledgement masquerades as edit success.

## Supplied design and required differences

Read-only B9 references inspected as PNG and full overlay specs:

- batches/B9/png/SCR-122_edit_case_dark.png and specs/SCR-122_edit_case.json: native Edit Payment Advance, locked Candidate/channel/status/payout/repayment facts, amount/schedule/affordability/optional Note, Preview changes and disabled Save changes until Preview.
- batches/B9/png/SCR-122b_edit_credit_committed_dark.png and specs/SCR-122b_edit_credit_committed.json: locked protected Manual Credit amount/tax/comment and actual Draft reference.
- DESIGN_INDEX, B9 critic and policy notes identify these same supplied overlays. The critic's Note-only Save example is not a card mutation authority.

SCR-122b's editable Note and Save changes on a committed Credit must be omitted and logged under owner A2. The card forbids that financial activity state from editing and provides no separate Note-only command. Read-only Note may be shown if actually provided. Keep the blocked explanation and locked facts. READY protection points to the existing DR-05 release owner; confirmed DRAFT protection points to its existing cancellation owner only when available. Do not claim that cancellation has occurred, add an inline release/cancel mutation or fabricate a date/reference.

Real fields/statuses replace Bethany/TMS10502/£300/£35/PB0412P and example dates. Existing native frame/Close/shared footer and phone 44px controls remain required. No A4 pass is claimed before real screenshots; every actual visible deviation must later be fixed or tied to the card/owner requirement.

## Required local screenshot coverage after root implementation

Dark, device-light and 390px phone: eligible Advance edit and Preview changes; Credit explicit tax/comment edit; Debt chosen schedule/reference/affordability edit; null and genuine-zero affordability; inline invalid amount/non-Monday errors; Preview invalidated by edited field; busy saving; unknown exact Retry after reopen; accepted awaiting new readback; an existing old component page that must not acknowledge the edit; final exact accepted revision; failed prefill/readback and recovered Refresh; stale revision/refused protection; accepted PREPARING protection with no hold/unknown amount; ACTIVE hold and committed Credit read-only; wrong-family/foreign retained intent locked truth. Phone top/lower controls and native Close must be inspected in real captures.

These are a finite A4 evidence plan, not business fixtures or a new test programme. All policy42 items, manual wording lists, owner 11 October every-difference/control-purpose comparison and explicit source-vs-pixel evidence remain due at that later review. Joined Office/Worker/SQL edit/prepare/release/activity acceptance is AUTHORED_NOT_RUN.

## Current source identities

SHA-256 of saved disk bytes read for this investigation; no product edits by this reviewer:

| Source | SHA-256 |
|---|---|
| BE AGENTS.md | ae1357fb0372932d1ae05cc15c7b02662c71d5e96f00cecb185fccc3f2be29e9 |
| FE AGENTS.md | 12d2db00e94fae90c4a324f7aafaa532349f24f7d2938b6fd3076cbf510a24fd |
| FE BANKING_PAY_BIBLE.md | df2b72b102576c039887feb89751235b0747b26a267917982f7fd45733ba486d |
| FE js/bpay-next-office.js | 1cc98547542c99652c6b0e6c5139e64c9ac8a4dbb2e24c782ec0eda29b9b5b77 |
| FE js/bpay-next-case-preview.js | 16914287a7f09824a4a9d9e8f99533209918970ae7a311660f9c17828a792cb2 |
| FE js/bpay-next-payment-advance.js | cccc07b702b6de26af6e9a13d2423c910dbfbde5e9fb74dc0eacb19f93a5d46b |
| FE js/bpay-next-manual-credit.js | a71d4bab44527e4b57f14f6d44e19da94247e8629ca0fcc9380865d890050552 |
| FE js/bpay-next-manual-debt.js | 228df8476030356857f3fc3cfd9d6b8272cb9aa9500448a1b3874efab92b651d |
| FE js/main.js | 74d30eb024f6ae5b0bde73d590a3af5ef2b93a37fb1d09bab1eb3560f889a07c |
| BE repeatable 1910 manual_debt_reference_v1 | 644995fb19e4a58d0854de8191980dc7e543015358d81d229785165377e2845c |
| BE repeatable 1509 case_change_guard_v1 | 087c3c5d3ae9cfaa94b66ea58b338358d0236f2b4953be5ee6adf7e3dfc60fd4 |

No database query, function invocation, fixtures, test run, renderer, hosted request, product/pack/checklist edit or commit was performed.
