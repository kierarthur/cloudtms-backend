# WB-33 initial supplied-design / native UI differences

**INITIAL FINDINGS — no screenshot or A4 approval.**

13 October 2026. Read-only source/design review by `/root/screen_review`; root alone authors product and captures. Read the full WB33 card02:1219–1239, BD174 boundary, B2 SCR28/28b specs, policy v1.2 and wording JSON; individually viewed both supplied PNGs. This compares intended source output with the supplied design, not an actual rendered screen. No SQL, business action, test, fixture or pack edit was performed.

The supplied dialogs are compact: danger question, one consequence line, exact values/scope, optional linked consent, two buttons. SCR28 uses Keep rate / Cancel rate; SCR28b uses Keep decision / Cancel decision. SDP21 requires destructive visual placement on the left, overriding the pictured right placement; safe DOM/default focus stays first under SDP55.

## Required corrections before A4

| ID | Actual source gap | Smallest correction / evidence needed |
|---|---|---|
| P33-01 | `keep`/`cancel` use Keep rate / Cancel rate for every non-GROSS family, including `options.channel`, although the title asks about the pay channel. | Use Keep decision / Cancel decision for the channel question; retain Keep rate / Cancel rate for rate and the genuine amount family labels. These controls must name the decision being removed. |
| P33-02 | The list shows `amount_ex_vat` beside Day/Night with only PAYE or Umbrella, ex VAT underneath. The supplied rate example shows unit rates20.85/24.97; this native field is a component amount, not a per-unit rate. | Explicitly label native totals Amount / Amount ex VAT (Gross total for actual amount-led family). Do not display a stored manual operand as a payable total or divide totals into picture rates. Exact current native quote totals satisfy the card’s amount confirmation; record this required difference. |
| P33-03 | Consequence appears after the full list; the optional checkbox is before the values and there is no live scope hint beside it. Generic excluded copy asserts all three causes regardless of actual `excluded[].reason`. | Put a neutral one-line consequence immediately below the question, exact values next, optional linked consent and exact native scope/exclusion hint after them. Use IN_DRAFT / CHANGED / UNAVAILABLE facts and native complete counts only. A Draft already created stays unchanged. Do not promise all cleared work becomes Needs a rate; native automatic Ready may be correct. |
| P33-04 | Existing CSS has no `bpay-next-clear-*` selectors. Shared destructive-left/footer/44px rules target other confirmation classes. Safe button is first in DOM, but the new footer has no scoped layout ensuring the destructive action is visually left. | Reuse native confirmation frame/tokens with narrow `#modal.ctms-modern-modal` selectors: compact question/icon, flat values, full-width footer, warn action left, safe action right, 44px phone buttons/checkbox. Keep safe DOM/default focus first; no new !important or nested cards. Actual pixels must verify inheritance and left placement. |
| P33-05 | The same `confirmation:true` frame remains through pending/accepted restoration. Accepted state changes Keep to footer Close, while result and Processing lines have no live roles/positive native treatment. | Keep the two-button question only during consent. For non-consent progress/results, use the existing native ordinary result presentation/header Close, without a duplicate footer Close or lingering destructive question. Result role=status/live-copy; positive Choice removed plus separate Processing. Only an actual check-progress/recovery control remains when needed. |
| P33-06 | Status is in the footer after facts and every busy operation says Loading…, including cancellation/recovery/restoration. Pending copy is an ordinary paragraph. | Show one live status/error before retained facts. Distinguish Loading…, Cancelling…, Checking… and Updating… as appropriate; role=status while busy, alert for a failure. Explain disabled controls on screen; do not duplicate an error in footer and body. |
| P33-07 | Failed explicit reload retains `reply` and its controller. Cancel is disabled only by busy/current/clear_class, not `error`, so a retained failed-refresh view can offer cancellation again. An unavailable root merely disables Cancel and offers no Refresh because Refresh requires `error`. | Gate consent/dispatch on successful current read; after failed/stale reload keep facts read-only and use explicit Refresh as the next step. For native NOT_CLEARABLE amount, show its plain unavailable reason and Refresh, never amount-zero or enabled cancellation. Retain the original request on unknown outcomes. |
| P33-08 | `recover()` sets retry.textContent=Try again after an absent receipt, but its finally render sets it back to Check result whenever pending; onclick remains retryOriginal. | Derive the visible label and handler together from existing controller.canRetry(). Check result reads first; only proved-absent original journal may offer explicit Try again. No automatic resend or new request. This is a control-expectation source finding, not performed recovery evidence. |

## Required and permitted supplied-design differences

- Native Candidate display/reference exists in `clear_review.header`; show it with real Client/week facts from each owner. The compact supplied overlay does not show all that subject text, but it is factual protection against cancelling the wrong choice.
- Linked consent starts **off**, as BD174 requires explicit opt-in; supplied SCR28 starts checked. Use only genuine compatible native linked owners; do not invent the picture’s two linked / one excluded example.
- The pictured SCR28b mixed STANDARD+Additional scope is split by the card-required WORK_BUCKET/GROSS family refusal. This dialog cannot imply one command clears both or removes an automatically converted value.
- Native current component amounts replace picture-only unit-rate amounts unless an exact native rate fact is already available. No amount/rate arithmetic, live Contract inference, unsupported timestamp or new reader field.
- Loading, unknown-result, unavailable-root and restoration-failure presentations are necessary native states absent from the two supplied static pictures. They must be plain inline states, with original retained bytes and truthful Processing.
- Existing native scroll body and phone wrapping may adapt the desktop600px picture. Two flat value columns and a single bounded scroll surface suffice; no sorting/ticks or new tree controls (SDP34 dialog exemption).
- Wider Candidate totals/selection/adoption remain WB34; WB33 must restore its own actionable editor or genuine Ready state through exact accepted published revision, without repricing a frozen offer or claiming a whole-app join.

## Every visible control and its payroll purpose

Keep rate / Keep decision / Keep amount backs out safely. Cancel rate / Cancel decision / Cancel amount removes only the disclosed exact saved owner(s). Optional linked checkbox explicitly extends that disclosed closure. Refresh obtains new current read proof after unavailable/stale facts. Check result reads the retained request; Try again, only after proved absence, resends identical bytes. Check progress continues exact published-revision readback and never repeats CLEAR. Header Close/Escape is for ordinary progress/results; the two consent buttons close a confirmation. No footer Close, unrelated More, finance CLEAR, new auth step or extra primary is needed.

## Minimal actual capture acceptance list

Capture rate whole-root, exact row amount/GROSS, channel family, linked off/on with native excluded reason/count, unavailable root, initial read failure, stale/failed refresh with retained facts, unknown result, proved-absent retry, accepted Processing/restoration and restored native editor/Ready. Use actual native dark/device-light/390px phone for changed family; include a phone lower frame when values/control scroll. Reuse the smallest existing fixed local replies/journal, not business commands. Full42-policy/A4 approval follows only after each real required PNG is viewed beside these references;25/readerAdditions26 are owner workflow N/A,16 manually uses policy JSON,23/24 use actual captures.

## Inspected source/reference SHA-256

| File | SHA-256 |
|---|---|
| FE js/bpay-next-clear-choice.js | 7e2f5b059570167c449a70983b94bcd8c9d0b998f8b9c626d525bb242660f888 |
| FE css/bpay-next-office.css | bdca5176c8b24d00fcf26341a80ea52e2abcf330f248f144439662546eb81aa1 |
| BD174 WB-33-build-boundary.md | 5e61fe60a29893425282820287cb37ed5335a0adf4b0a0257e332583b26fa152 |
| B2/png/SCR-28_cancel_resolved_rate_dark.png | 22b5c0d42c56fbafdedb96a5f56f56faf533ca1c90b0b389eee30aeacfdc845e |
| B2/png/SCR-28b_cancel_resolved_pay_channel_dark.png | 0c601dacc285c719069c74ea8706d14ccfffea661e34c2aaef632454439f10bc |
| B2/specs/SCR-28_cancel_resolved_rate.json | 4c9b1418fdef560de27fb2be8aa327425f6583eb3d2905cf2ff4e5c6d1b4251d4 |
| B2/specs/SCR-28b_cancel_resolved_pay_channel.json | 17b9ac69940df2e069f017cd050756ac68012b60f3837ad2bde275b0df75ad89 |
| SCREEN_DESIGN_POLICY.md | 8bc212b174469236f2b46dbf70eadc3664a946636458321cd3c77b7146323de5 |
| tools/policy.json | 45036d841d4568aa2d4d7adf4f063d6d17b70288e289ee0b285068797a87369d |

All findings are initial source/design evidence. No screenshot verdict, runtime recovery result, joined acceptance or whole-item PASS is asserted.
