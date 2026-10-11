# DY-10 D03 independent Office source review

**FINAL CURRENT SOURCE PASS — reviewed saved DY10 bytes committed by root as FE `9ff07fd9d4d793c632ce5f1cfd48207b2d099e50`. All essential source findings closed; final native adapters and exact hashes are below. A4 separately closes54 actual PNGs in SCR-156/review.md. No joined/runtime business acceptance.**

## Historical initial source checkpoint

11 October 2026. **SOURCE CHANGES REQUIRED at the hashes below.** Read-only review of the saved Daily state mapper, native Timesheet Overview integration, targeted recompute and full-details refresh path against the full DY10 sequence97 card/D01–D05, BD109 and the independent screen/design review. D02 Worker closure is separate. No product, database, test, fixture, hosted call, render, capture, checklist edit or commit was performed. No A4 or joined acceptance is claimed.

## Essential findings

1. **P2 — unknown Retry can regain Retry after a failed Refresh.** `js/bpay-next-office.js:59–65` retains `unknown` only in the current mounted closure. `js/main.js:329672` delegates to the original full-details refresh; its failed-read branches372185–372215 call `safeRerenderTimesheetModal` before returning false. That rebuild disconnects the old host, so `refreshed===false && current()` cannot throw and the new mount starts with `unknown=false` from retained FAILED/UNPREPARED details. An unconfirmed recompute followed by a failed explicit Refresh can therefore expose Retry without a successful status read. Preserve the same actor/modal/physical-ID unknown/request state across failed rerenders, or ensure the fresh host stays Refresh-only until actual current evidence is read. Do not automatically repost.

2. **P2 — mismatched identity hides the required unavailable recovery.** Main329665–329670 requires `tsNow()` to equal the returned Timesheet ID before mounting any notice. If the active modal targets A but returned Daily Timesheet B disagrees with current metadata A, `dailyProcessingState` correctly returns UNAVAILABLE, yet the strict mutation guard prevents the host from rendering its reason, dash and Refresh. The new Overview row is then just its label/hidden empty host. Separate the safe presentation/read guard from Retry qualification. Refresh may use the original validated modal target; mismatched returned identity must never become the mutation target.

3. **P2 — pay channel is labelled pay method.** Office51 says “The pay method needs to be set before authorisation.” The state is native PAY_CHANNEL_MISSING / has_pay_channel_issue, and policy16 explicitly forbids this pay-channel wording. Use “pay channel”; preserve the original native channel field and control.

## Source behavior supported by this review

The three-field Daily processing fact is passed through the original `fetchTimesheetDetails` object (main359317). The mapper positively qualifies DAILY, current UUID/version and matching fact identity/status; missing/malformed processing evidence becomes UNAVAILABLE rather than zero. Exact current TF identity/version/currentness and actual authorisation stamps are required before the authorised/zero-hours labels. Native revocation/archive is distinct; QR cancellation is not used. Only literal total_hours zero selects zero hours, so positive hours with zero money cannot become that label. Outbox FAILED/PENDING, missing TF, native rate/channel issues, not-authorised and damaged evidence have separate branches. No new financial value is calculated.

Retry submits one current physical Timesheet ID through existing admin `POST /api/tsfin/recompute` → original bounded `enqueue_ts_financials_priority(..., MANUAL)` (BE index172945–172960). It does not invoke drain, authorise, fabricate Work or mutate money. Acknowledged enqueued=1 is labelled only “Processing requested.” The integration checks the native modal token, same actor, retained details/version, connected host and current NEXT owner; late callbacks are rejected. Busy disables the local action. Full-details refresh is explicitly requested, preserving the original native read/rotation path.

The false no-TF “No real hours have been entered” fallback is suppressed for positively recognised Daily state, and the new presentation occupies the existing Overview rather than a fabricated Banking row. The controls exemption is confined to the Daily host. Outline Retry/Refresh has44px minimum dimensions. All final status tones, retained facts, native control purposes, placement differences and phone geometry remain subject to real A4 pixels. Genuine booking cancellation has no positively supplied native authority here; its absence is an explicit evidence limit, not a fabricated passed branch.

The renderer currently suppresses the old fallback and inserts the row without checking NEXT/admin eligibility, while mounting is NEXT/admin-only. At minimum hide the whole unmounted row and preserve appropriate original-owner fallback, so non-qualified native views do not lose their only state or show an empty “Daily Timesheet” label. This is also the visible consequence of finding2; do not solve it by accepting mismatched Retry identity.

## Reviewed saved bytes

FE HEAD at review: `b28dd501b5acc896f658eb79226cdb4e6bf576c7`; this receipt reviews the saved uncommitted DY10 delta, not that HEAD alone. Raw SHA-256:

| File | SHA-256 |
| --- | --- |
| FE js/bpay-next-office.js | `9ddcf7942ab3a0b531e229b30baa64ce14ec676c91aa4caada86b84f105af74f` |
| FE js/main.js | `cbb8d6bc808d672d93b75fafec210b9d4488fc6b739ddcbcef89c72dc4445e9a` |
| FE css/bpay-next-office.css | `9fa16a4e7dbc009b7b3f7ca81fbf363a738ff4c29f674801641c7a4bb40fd311` |
| FE index.html | `be400790b47690e2c7ccf26a1365a29d8e7636a570cb4ce65b3729acaad56e34` |
| BE broker/src/index.js | `1ae4b5df3c72a5659fc4454a49e7b617b541fc71d0c312ed7a78b272f5f5f2ff` |
| BD109 design/DY-10-design.md | `2a8d6fd0a650232b6abd58df317fee29f6625ad62dd07a3361bb7e63a50424d2` |

Scope remains D03 source only. Correct findings1–3 and re-read final bytes before source closure. A4 and D05 financial, queue, accepted-cutoff, failure recovery and full native-app journeys remain NOT_RUN by this reviewer.

## Fresh source re-review — original findings closed, one exact-read guard pending

The saved fixes were read in full alongside the native wiring diff. Findings1–3 above are **closed**: the same native actor/token/modal-target/version request state survives rerender; unknown is set before send/read and the active painter owns busy/unknown controls. Refresh uses the original modal Timesheet target, while Retry separately requires returned physical identity and a retryable mapper state. Unavailable evidence now mounts under the safe presentation guard. Pay-channel copy is corrected. The renderer/fallback suppression is positive NEXT/admin only, and the already existing record-owner qualification callback narrowly repaints Daily Overview rather than adding an owner request.

**Remaining P2 exact-read guard:** main359367–359380 tests `dailyProcessingState(detail)` only for truthiness before clearing retained unknown. The mapper's UNAVAILABLE result is also an object, so a malformed/cross-identity DTO with an otherwise allowed status can clear unknown despite failing the exact bounded fact validation. Require the mapper result key to differ from UNAVAILABLE, or repeat its exact DTO identity/key/type checks. No new state or reader is needed. This does not qualify invalid returned identity for Retry, but it fails the explicit successful-exact-read condition for clearing an unconfirmed request.

Fresh raw hashes: Office `8ecb7ffec4872a19a0e08b858fcaa7709f82baec6acd4cbf124fd2845b231555`; main `9d1a31374cc2ab1baa4e886fb3133b1ff65926ee3786f8acd19f7c6a8126f3dc`; CSS `5fcfdcb9ed1d5ea7df8052539280e3e85e926154f17bc45599c9dc844f89e1d5`; index unchanged `be400790b47690e2c7ccf26a1365a29d8e7636a570cb4ce65b3729acaad56e34`. Source verdict remains CHANGES REQUIRED only for that narrow clearing predicate; A4 remains pending final captures.

## Final D03 behavior-source closure

Fresh main359374–359375 now requires both a non-null mapper result and its key different from UNAVAILABLE before clearing retained unknown; all exact native actor/token/target/version/status conditions remain. The exact-read finding is **closed**. **D03 JavaScript behavior SOURCE PASS at these saved bytes:** Office `8ecb7ffec4872a19a0e08b858fcaa7709f82baec6acd4cbf124fd2845b231555`; main `dab9f0392dee3e383ff74b8cd38bf832d149be87814a08d48899cbad0da0f6b7`; index `be400790b47690e2c7ccf26a1365a29d8e7636a570cb4ce65b3729acaad56e34`. No essential behavior finding remains.

**New CSS policy finding, still pending at this historical checkpoint:** CSS `38eff3a736bedb3d0d9cbcc52ad6872b73de717f62f4dc03fe1a6756a98ea97a` adds three per-button `!important` overrides for `[data-ts-action="add-additional-manual"]`. SDP20/full checklist11 prohibits new `!important`, and the native Buttons style table prohibits per-button overrides. Use the existing scoped native class/moderniser handling and ordinary modern-modal selectors. This is a source-policy finding, not an observed pixel verdict; final screenshots are still being recaptured. D03 JS closure does not waive CSS/A4. Genuine cancellation authority and all D05 joined journeys remain explicitly unproved.

## Current final source receipt — 11 October 2026

**D03 SOURCE PASS at the current saved hashes below; no essential source finding remains.** The earlier CSS finding is closed: no added `!important`, new selectors remain scoped to `#modal.ctms-modern-modal`, and only the native Timesheet Additional action in a qualified Daily notice is exempted from the existing primary verb matcher. The original seven-tab renderer's retained failed-read exception now requires the exact actor/token/physical/version key and NEXT admin owner; it keeps the existing suppressed lifecycle/edit actions. Successful exact current reads clear obsolete “Could not” result copy. Failed/damaged/unavailable lines and failed result spans use alert; positive/pending results use status. The finite native badges distinguish Failed, Updating and Authorised without inventing money or approval.

The existing universal enhancer now suppresses a Daily Timesheet footer only when the native hint is blank and every native button/input/select/link is absent or hidden. It retains the previous inline display in a named attribute and restores it if an actual control appears or the native window ceases to qualify. This is confined to the existing enhancer, with no observer/framework or global modal behavior change. The previous CSS-only attempt did not beat native inline display; the empty-footer **pixel** finding stays open until the replacement captures are individually re-viewed. A4 is not inferred from this source receipt.

| Current saved file | Raw SHA-256 |
| --- | --- |
| FE js/bpay-next-office.js | `4c94f4b141da939b2fbe7e7512a5394bb955fd15a9ae85225a53570161e59211` |
| FE js/main.js | `fdd305b676d649a327f4865cbc26cf70f80ea6ea00a0d9b5b8e9aec9adcf1193` |
| FE js/modal-modernisation.js | `07cafecc9a9cc79fd475303d2d43bb0aba7f78f722f57ee70fcf5fc29cb03755` |
| FE css/bpay-next-office.css | `3dcb84f0a005919e9dbcf26e114ea4fbae2c3765080b29604dab5ed24b5b4146` |
| FE index.html | `115d705eb719a66a287b817bccb816d0ce20776ea55fb1d26648beb367e863d9` |

This supersedes the historical pending source findings above. No database/runtime, installed-app, queue, payment, accepted-cutoff/race, keyboard journey or D05 business acceptance was performed by this reviewer. Genuine booking-cancellation authority remains unprovided; native revocation/archive and QR cancellation are not substituted for it.


## Frozen capture-source closure

The final ancestor-aware footer adapter was re-read: it tests each control and its ancestors up to, but excluding, the footer. Hidden native Contract-detail containers therefore cannot keep an empty footer open, and the footer's own temporary display cannot prevent restoration when an actual control appears. Original inline display is saved/restored exactly. No extra observer, framework, !important or non-Daily lifecycle behavior is introduced. **Final D03 SOURCE PASS; all source findings closed.** The final desktop pixels independently confirm the blank strip is gone.

- FE js/bpay-next-office.js: `4c94f4b141da939b2fbe7e7512a5394bb955fd15a9ae85225a53570161e59211`
- FE js/main.js: `fdd305b676d649a327f4865cbc26cf70f80ea6ea00a0d9b5b8e9aec9adcf1193`
- FE js/modal-modernisation.js: `c2262959e41516ebfc8069f88b997d71e4d5763e36e0edc90bc7ff57ce7970ed`
- FE css/bpay-next-office.css: `3dcb84f0a005919e9dbcf26e114ea4fbae2c3765080b29604dab5ed24b5b4146`
- FE index.html: `115d705eb719a66a287b817bccb816d0ce20776ea55fb1d26648beb367e863d9`

Final captured source is the saved DY10 delta over FE HEAD `b28dd501b5acc896f658eb79226cdb4e6bf576c7`, not that commit alone. Root froze final54 captures from loop42475. Pixel coverage and the independent phone receipt are recorded in SCR-156/review.md. Joined D05 financial/queue/race and installed-app journeys remain NOT_RUN.
