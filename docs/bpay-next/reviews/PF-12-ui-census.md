# PF-12 NEXT UI source census

Status: read-only inventory complete; existing baseline plus finite remaining UI work identified. No product edits, app/browser/business/scenario/test/runtime/SQL/hosted operation. No installed-screen or joined acceptance claim.

## Exact scope and deliverables

R3 `06_CONTRACT_MOVES_DAILY_PAYMENT_SUMMARY_PERFORMANCE.md:1919–1936` was read in full. PF-12 sequence 25 depends on F-16 (`08_BUILD_ORDER_AND_CODEX_INSTRUCTION.md:109,410`). RQ-256 requires mouse and keyboard access, phone width with reachable Close, and dark-mode legibility. Reuse NX-179 Escape/focus and current `css/bpay-next-office.css`. Screen substance: complete narrow-width and `prefers-color-scheme`/theme-variable CSS, explicit Close/Back focus, keyboard reachability across every later CM/DY/XD control. No DB/Worker or financial owner, no new proof framework and no adjacent future-card build.

Current BUILD-BE `docs/bpay-next/IMPLEMENTATION_CHECKLIST.md:609–628` / equivalent CSV has exactly three rows:

| Row | Required current object | Current recorded status | Scope |
|---|---|---|---|
| PF-12-D01 | `js/bpay-next-office.js :: responsive/theme/keyboard controls completion` | PARTLY_DONE | Current F-16 Office plus history integration. Historical RESET path maps to active BUILD-FE. |
| PF-12-D02 | `css/bpay-next-office.css :: narrow-width/theme styles` | PARTLY_DONE | Responsive/theme CSS; history's shared shell needs scoped treatment too. |
| PF-12-D03 | PF-12 future joined in-app TEST checklist | MISSING / NOT_RUN | 375 px sheet/history Close, Tab every control, mouse access, Escape/Close focus return, light/dark legibility. Build/review/install/app boxes stay separate. |

`review/R6.json` PF-12 confirms baseline Escape/focus is evidence, not completion of requested feature. Card requires future controls to inherit the completed UI behavior; do not implement CM/DY/XD financial features ahead of their cards. Leave their current financial and request identities untouched (including F-25 Rule B).

## Actual product paths and reusable baseline

Active FE root: `C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/bpay-next-build-frontend-20261009`.

- `js/bpay-next-office.js:480–607` owns current sheet; all frozen-detail controls live there. No separate product frozen-detail JS exists.
- `js/bpay-next-payment-history.js:308–374` renders current 52-section read-only history inside existing `showModal`, kind `banking-pay-history`, frameEntity `payment-history`, showSave/showApply false. Keep this shared modal and exact read contract.
- `main.js:7076–7083` opens Office using existing owner/auth/Candidate callbacks; `333465–333470` opens history from current eligible Candidate or Timesheet modal. No provider, money, owner-status or native record workflow change is needed.
- `index.html:15` includes current `css/bpay-next-office.css`. Shared modal exists at 2868–2891 with header Close 2877, tabs/body below it, actions outside body. Modernisation stylesheet is loaded at 2819; existing shared modal styles can outrank earlier generic CSS. Use explicit NEXT-only selector specificity/variables, never a global palette change.
- `js/modal-modernisation.js:750–752` derives exact `ctms-kind-${kindSlug}` class; current history kind maps to `ctms-kind-banking-pay-history`. Its generic modern shell at `css/modal-modernisation.css:46–61` bounds width and `100dvh`, has min-width 0 and overflow hidden. Header 73–88, body 120–128, tabs 130–143 keep shell/body roles. These are reuse, not authority to edit unrelated record/modal families.
- Bible mobile rule at `BANKING_PAY_BIBLE.md:613`: top child heading/Close stays reachable; actual complete table owns internal vertical/horizontal scroll; use dynamic viewport and safe-area insets. The older 31-Aug mobile change record is history, not evidence of current NEXT installation.

## Office CSS census and finite work

Current `css/bpay-next-office.css` is 10 compact rules/lines, all fixed colors:

| Lines | Existing baseline | Finite needed closure |
|---|---|---|
| 1 | Fixed sheet; high z-index; desktop max-width 1250; `100dvh`; safe-area-aware inset; flex column | Keep bounded sheet. Introduce NEXT-scoped palette variables; honor light/dark preference or exact existing theme signal, retaining legible dark defaults. |
| 2 | Header outside body scroll; Close built as ordinary button | Keep Close independent of table overflow and never disable it for pending/read work. Allow title shrink/wrap, Close flex-shrink 0 and touch size. |
| 4 | Body overflow:auto, min-height 0, flex 1, overscroll contain | Keep actual full table horizontally scrollable; set appropriate min-width/box-sizing to avoid horizontal overflow moving the header. No omitted fields or mobile abbreviated money rows. |
| 5–7 | Native buttons/inputs, disabled styling; visible focus on button/input; labels wrap inputs | Broaden shared visible-focus treatment only to actual NEXT controls (anchors, summary, select/textarea if later added); keep native keyboard interaction. |
| 8 | Full tables min-width 850; no fake responsive data omission | Keep full columns inside body/table scroll. Keyboard focus may scroll the actual control into view; Close remains fixed. |
| 9 | Alert colored for dark background | Theme alert/background/text/border together; no hidden refusal or changed request state. |
| 10 | max-width 600: inset6px, viewport max-height100dvh−12px, tighter padding, header button min-height42 | Phone override currently discards line1's env safe-area inset. Preserve safe-area-aware top/right/bottom/left at narrow width and calculate actual remaining viewport height; retain always-visible header. |

Dark mode is already a fixed dark rendering baseline; source alone does not prove visual contrast. The card still explicitly calls for theme variables/preference support and light/dark legibility. Existing `index.html:18–35` root palette and modal-modernisation variables are dark-only; avoid introducing a global application theme or modifying unrelated screens. Small NEXT-scoped variables with `prefers-color-scheme` override are sufficient; history can override its modern modal variables under the exact history kind class and its own body styles.

## Keyboard and focus census

### Office sheet

- 487: real dialog section, aria-modal, label, tabIndex −1.
- 488/493: captures prior focus and calls its `.focus()` on Close; removes sheet/listeners/timer, retains controller journal.
- 490–491: every current action is a native `<button type=button>` and inputs are wrapped by labels; checkbox rows have explicit aria-labels (540,549). Mouse click already invokes the same action as native keyboard activation. Do not add a separate Enter handler that duplicates financial mutations.
- 494: existing Escape closes; Tab cycles `button:not(:disabled),input:not(:disabled),a[href]`. It does not filter hidden/tabIndex−1/inert controls or include later select/textarea/summary; focus escaping/outside-node recovery is limited. Extend the finite visible enabled control set for this sheet only, avoiding negative-tabindex/inert/hidden controls. Keep Escape idempotent.
- 525: Close is always built enabled; it is direct and bypasses the readBusy mutation guard. Preserve.
- 595: `host.replaceChildren(header,body)` rebuilds every header/button/input after action/loading/owner/poll render; current active control is removed with no focus preservation. This is the concrete keyboard continuity gap. Capture a stable UI action/row key before rebuild, restore its enabled visible equivalent after rebuild; when target disappears/disabled, focus the persistent/rebuilt Close or sheet. Use existing exact row IDs, not new financial identity or focus by displayed text. Keep values and accepted/pending controller state unchanged.
- 605: opening/coalescing focuses sheet. After Close guard `.isConnected` on opener and use a sensible existing page/nav fallback if removed; avoid claiming a detached `.focus()` proves restoration.

Current controls to cover through the same primitive, not individual new workflows: Choose Candidate / Candidate bank-details delegation; pay-date, resume, net inputs; selected Work and Case checkboxes; page next controls; Case component select/seal; Prepare/review/confirm; channel Draft open; worker/transfer/return reads; PAYE NET / CSV actions; frozen shifts/breaks/rates/comparison detail; original advice; exact uncertain-request Retry; Close. Input/mutation/financial semantics remain current owners'. No CM/DY/XD controls currently absent are to be fabricated.

### History modal

- 318/320/326/333/335: actual native buttons for Refresh, All sections, Back, linked child sections and Next page; `<details><summary>` for scope/groups/row evidence. The latter are real keyboard controls and should be included in local focus handling.
- 340–345 and 347–352: each loading/repaint invokes existing frame.setTab; rendered body action elements are replaced. No stable action/child-index focus restoration is present. Restore a finite action identity after repaint; Back should return to the originating group's/row's reachable control, with safe fallback to the history heading/Close. Preserve navigation requests and exact keys, loading guards, disposal and actor validation.
- 368–370: showModal reuse keeps parent stack and shared Close. Do not build a second dialog framework.
- Shared `main.js:337690–337717` already wires Escape via current header Close. **Tab trapping is conditional at 337697 on `isRecordOwnedFrame(top,parent)`.** `isRecordFrame` at 325461–325464 names candidates/clients/contracts only. Candidate history inherits a record parent and obtains this baseline; Timesheet history does not. Therefore history needs an explicit NEXT-local visible-control trap for both entry paths, rather than assuming the shared conditional is universal.
- Shared initial focus at 325630–325636 is likewise record-owned only; record opener capture at 332517–332519 is only initial parent record. Current history module itself captures no opener and sets no initial history focus. Add module-local focus into the actual history shell/Close and restore to originating Payment history button (or connected original element) after parent frame returns.
- `onDismiss` 315 currently disposes client/click listener but does not restore focus. Shared `cleanupClosingFrame` calls `_onDismiss` at 336921 before parent chrome/tab rebuild, so synchronous focus restoration to a removed child/opener is insufficient. Use the existing post-return rendering seam or a guarded deferred focus attempt after the actual parent returns; never steal focus from a subsequently opened unrelated frame. Preserve full existing Close/parent-return lifecycle.
- A NEXT-specific trap should act only while the actual current frame kind is banking-pay-history, preventing duplicate/outdated parent handlers from trapping focus outside the child. Keep current exact public close/dispose and actor late-response guards.

## Small recommended implementation / ordinary checks

1. Complete existing Office CSS with finite palette variables, light/dark preference, safe-area-aware phone viewport and reliable fixed Close; extend only exact history kind/body shell selectors using existing modern modal variables. No global CSS or application theme rewrite.
2. Complete Office's current focus trap and focus continuity around its existing rebuild; keep every action native and preserve journals/mutations/requests.
3. Add history-local actual-frame keyboard/focus continuity and after-parent-return restoration. Reuse existing showModal/Close/Escape/stack; no unrelated main.js framework refactor.
4. Reuse ordinary source units `tests/unit/bpay-next-office.test.cjs`, `bpay-next-payment-history.test.cjs` and existing modal source-contract units if applicable. Current Office unit source checks Candidate navigation/Close journal/session safety, and history tests validate actors/disposal/contracts; no inspected assertion proves keyboard/phone/theme behavior. A finite source check plus pure local keyboard/focus helpers can cover disabled/hidden/summary/last-first, rerender fallback and exact scope without business fixtures or new proof frameworks.
5. Record D03 future joined 375px mouse/keyboard/Close/Escape/Back focus and light/dark legibility as NOT_RUN until owner-authorised real joined acceptance. Pure source checks cannot certify actual phone layout or installed parity. Later CM/DY/XD controls inherit this baseline when their owners add them; each owner checks its actual new controls, not an invented future inventory here.

## Saved source SHA-256 checkpoint

Hashes identify saved source read for this inventory; root may concurrently author PF-12 changes later. They are provenance, not a requested hash-based testing programme.

| Exact path | SHA-256 |
|---|---|
| `C:\Users\KierArthur\OneDrive - Arthur Rai\Documents\GitHub\.codex-worktrees\bpay-next-build-frontend-20261009\css\bpay-next-office.css` | `5fa3e52ab734f0fee7071d364f57c44e7130478648fe797d98bf3b6804b50b1f` |
| `C:\Users\KierArthur\OneDrive - Arthur Rai\Documents\GitHub\.codex-worktrees\bpay-next-build-frontend-20261009\js\bpay-next-office.js` | `d6f0a878ed356c5e23fe3121813928af130966611f967ba52c1829c8ab33e0f9` |
| `C:\Users\KierArthur\OneDrive - Arthur Rai\Documents\GitHub\.codex-worktrees\bpay-next-build-frontend-20261009\js\bpay-next-payment-history.js` | `b81e0968410a2c6cc5500da50283d911c4ec603bc5b85f2b86de95b3d14cd258` |
| `C:\Users\KierArthur\OneDrive - Arthur Rai\Documents\GitHub\.codex-worktrees\bpay-next-build-frontend-20261009\js\main.js` | `337ad43e351b88f8661169365c391fbca9e9d98cb2b877828df49aa7e2c2963c` |
| `C:\Users\KierArthur\OneDrive - Arthur Rai\Documents\GitHub\.codex-worktrees\bpay-next-build-frontend-20261009\index.html` | `4b854d032c9f37d4402d13f7b4e31b23634e436aceb37a14f3ad90d90ff28096` |
| `C:\Users\KierArthur\OneDrive - Arthur Rai\Documents\GitHub\.codex-worktrees\bpay-next-build-frontend-20261009\js\modal-modernisation.js` | `7c3ac05d122089765d9ba6bcadd1138ccf7f7deed32e6a22d1fccfe5d3e3be4e` |
| `C:\Users\KierArthur\OneDrive - Arthur Rai\Documents\GitHub\.codex-worktrees\bpay-next-build-frontend-20261009\css\modal-modernisation.css` | `266e62f9d4ebf9b3e1d4bfb99b0ac63af0eda67f34afacc97230e0c7579ddd57` |
| `C:\Users\KierArthur\OneDrive - Arthur Rai\Documents\GitHub\.codex-worktrees\bpay-next-build-frontend-20261009\tests\unit\bpay-next-office.test.cjs` | `e5b406c49ee9a5f20cbcc10d82dd24e24b12297d5fc3ed9056ad8ae018b068b2` |
| `C:\Users\KierArthur\OneDrive - Arthur Rai\Documents\GitHub\.codex-worktrees\bpay-next-build-frontend-20261009\tests\unit\bpay-next-payment-history.test.cjs` | `a5f8cfd186accc30b079a85f487e3738f8afaabd3d674d1fbe842024714c875a` |
