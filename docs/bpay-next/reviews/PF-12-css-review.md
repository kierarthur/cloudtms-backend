# PF-12 independent CSS and scope review

Reviewed saved BUILD-FE source on 10 October 2026 against the full PF-12 card, canonical three-row checklist and `design/PF-12-SCOPE.md`. Read-only source review: no browser, app, tests, business requests or database execution. D03 joined phone/mouse/keyboard/light/dark checks remain **NOT_RUN**.

## Finding PF12-CSS-01 — active History tab escapes the light theme

**CLOSED on fresh source reread.** `css/modal-modernisation.css:162` and `:168` retain `#modal.ctms-modern-modal #modalTabs button:hover:not(.disabled)` / `button.active` rules with two ID selectors. They force near-white/white text on translucent pale blue backgrounds. The initial new general `#modal.ctms-modern-modal.ctms-kind-banking-pay-history :is(button,.btn)` had one ID selector, so it could not override those states. The final CSS adds exact NEXT-history `#modalTabs button`, `button.active` and `button:hover:not(.disabled)` theme overrides. Each applicable override outranks the corresponding modern rule and uses the button background, text and border variables in both themes.

The root has received the finding. The explicit `--next-scheme` dark/light assignment and use by the modern shell are already corrected and pass source review.

## Passed boundaries

- CSS changes are confined to the NEXT Office sheet and exact banking-pay-history shell. Explicit NEXT theme variables override modern shell panel/canvas/text variables and general header/body/card styles. No global theme or financial formatting owner is changed.
- Office uses a fixed safe-area-aware inset, dynamic viewport height, non-shrinking header/44px Close, and a separate flex body with `min-height:0; overflow:auto`. The 850px minimum table width retains every existing column and supplies horizontal scrolling inside that body. At the 600px breakpoint the Office gap/padding shrink; history width/height account for viewport safe areas. These are source properties, not an observed 375px app receipt.
- Focus-visible styles cover native controls, links, summary and tab-indexed controls with an explicit contrasting outline and `!important`, so generic modal outline suppression does not erase keyboard indication. The bounded shared helper filters disconnected, disabled, negative-tabindex, hidden/inert/aria-hidden and non-rendered controls, wraps Tab/Shift-Tab, and retains stable action/row identity across re-rendering.
- Office Escape/Close and history's existing `showModal` framework remain in use. History's Tab listener is current-frame scoped; dismissal removes that listener. The final dismissal observes actual parent chrome availability with a `MutationObserver`, restores the visible original opener only while the original parent frame is current, and disconnects on restoration, a different frame or user return intent. It does not replace parent frame methods or use a guessed timeout. Child/back navigation retains the original request and a focus token. Requests, journals, raw Rule-B inputs, paging and monetary fields remain unchanged in the inspected diff.
- HTML loads the shared UI helper before both actual consumers, with coherent `20261010-next-ui-r1` cache markers for the NEXT UI assets. The stylesheet loads before modernisation CSS; selector specificity, rather than source order alone, is required for the remaining tab fix.

The final phone history selector includes `[role=dialog]`. Its one ID and three class/attribute components outrank the later `#modal.ctms-modern-modal[class*="ctms-family-"]` selector's one ID and two class/attribute components in `modal-modernisation.css:3979`, so that later family-width rule cannot replace the safe-area-aware phone width/max-width.

## Final saved source identity

| BUILD-FE file | SHA-256 |
| --- | --- |
| `css/bpay-next-office.css` | `16f273ad1bdc141f3d44813eb2653f8ea1be8fc771abb799d5f9d4a04f4b861e` |
| `index.html` | `6f426a38b5a3594065cbef906e5b2d0342c040a08fd2a4a976e9b9eb0afb0ec6` |
| `js/bpay-next-ui.js` | `73f3a0aafd0d2f4df99c6163c659d43a41de8e191ace7eaca049e4e16249590e` |
| `js/bpay-next-office.js` | `abb793e194df1ed714677796bd9351f7df8aaadd367317b0ae448ee9fbbd114a` |
| `js/bpay-next-payment-history.js` | `ca46c0d595ba94757feb869242b2209df2e6047eb778f2787f97d5ef4d8b87db` |
| `tests/unit/bpay-next-office.test.cjs` | `402d52476057b9e762a085014548291a09af458c98f487ef0bb55ff63174331e` |

**Verdict: PASS for the bounded D01/D02 source and scope review.** No open concrete finding remains in this reviewed subset. Root reports 65 ordinary checks passed; this reviewer did not execute them. No installed or app-tested claim. D03 remains NOT_RUN.
