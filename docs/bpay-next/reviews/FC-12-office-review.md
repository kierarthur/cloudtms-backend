# FC-12 independent native Office source review

12 October 2026. **Native Office SOURCE PASS — O12-01/O12-02 closed by the final override below.** Independent read-only review of the native Restructure component and its existing Case More/main NEXT callers against BD147, complete FC-12 card/checklist and SCR125 scope. Root alone writes product. This receipt does not approve pixels or joined business behavior. No product/checklist edits, tests, database/RPC/business SQL, fixtures, rendering, network or hosted actions were performed.

## Concrete findings

**O12-01 — ancillary storage can erase terminal knowledge or prevent settlement.** `js/bpay-next-case-restructure.js:49` validates the exact terminal receipt, then calls `save()` before assigning `record`/`result`. If sessionStorage starts failing after the original request has already been persisted and sent, repeated successful terminal reads enter the unknown-result catch forever; the window never presents the genuine CASE_RESTRUCTURED outcome. At line60, terminal cleanup calls `storage.removeItem(key)` before `resolve(result)`, so a cleanup failure leaves the already-closed child promise and its awaiting caller unsettled. Preserve the validated terminal record/result independently of optional journal metadata writes, isolate cleanup from settlement, and retain original persisted bytes on failure. Persistence before first dispatch and before unknown retry remains mandatory; this correction must not create a new command or reinterpret an unknown prior request.

**O12-02 — stale refusal has two sentences.** `error()` at line32 emits “This case has changed. Refresh and try again.” for the three named protection/total changes. Owner A3g requires one-sentence refusal copy; use the same words joined by a semicolon. No error-code, dispatch or financial behavior change is required.

## Source checked

The component scopes its original serialized request to environment/agency/owner epoch/actor/Candidate/Case and revalidates the closed CASE_RESTRUCTURE body on restore. It persists exact request bytes before first dispatch, persists uncertainty before each dispatch, retains prior unknown identity, and removes an initial definitely refused request only when it was never previously uncertain. Apply requires the current preview token and exact entered-input equality. Inputs/radio changes invalidate the token; server Preview supplies all amount/schedule/threshold/floor facts. Client money/date work only formats values or normalizes validated input; it does not calculate repayment amounts.

The finite CASE_COMPONENTS point read requires a complete page with exact Case/Candidate rows and one legitimate recovery component. Pending/terminal readback qualifies every annotated receipt against original command, Case/Candidate, expected revision and retained sequence; all rows must agree. Original accepted receipts can survive newer current Case terms. BLOCKED refresh clears that terminal request explicitly before seeking a new preview; unknown retry keeps original bytes. Visibility/focus polling only reads a retained accepted request and is cancelled on dismiss. Parent refresh failure is already caught separately from the authoritative result.

Both native entry points use the existing standard window adapter and current actor/NEXT owner epoch. The main opener retains the original stack prefix, exact parent context identity and post-await owner/context checks before opening; the CaseList opener reuses its existing childOptions/current lifetime. CaseList only adds Restructure to the legitimate LOAN/ADVANCE/non-taxable MANUAL_DEBT Case route; unrelated channel restructure stays assigned to its later owner. The existing shared adapter mounts asynchronously and preserves the native stack/lifecycle. No new endpoint, authentication flow, Case-money event, channel conversion, unrelated producer or generic framework is introduced.

The scoped CSS/index additions load the native component and preserve hidden controls, money alignment and phone control bounds. Visual differences, required control purposes and actual SCR125 states remain for the separate A4 review. Transport SOURCE PASS is recorded separately in `FC-12-transport-review.md`.

## Saved source checkpoint

| Source | SHA-256 |
|---|---|
| FE `js/bpay-next-case-restructure.js` | `4e3db33485badfd06b9ddd7e6cedf69c99bb6563add4d00e643f094f82170056` |
| FE `js/bpay-next-office.js` | `456a48e11e6efd161fdeb3adf4cb356356dd42b1d3e95949a7da270577d11c91` |
| FE `js/bpay-next-case-list.js` | `55d450b1782c2b3c9511388b0ea0f923fc850db997d8a9f105bb30e6e2d76d2f` |
| FE `js/main.js` | `934e06de52b4e2f536a983036490c7e24f9e625e6ec0a0dfe208ef619570efe0` |
| FE `css/bpay-next-office.css` | `f1b6ad2fe0bb6490be943a14cdb37192600adb88c65ad26d82d6b89bfc5fd90f` |
| FE `index.html` | `fcc181806258dcd8cf4c65812243a7b5581c0fee85acd65e30ed06a3db51f799` |

Root is still editing. Final saved-byte closure must verify the two finite corrections; no item-built, app-tested or A4 claim follows from this checkpoint.

## Final native source override — SOURCE PASS

Fresh saved source closes O12-01 at line49: optional accepted metadata persistence is caught locally after the exact point receipt is validated; terminal `record`/`result` and parent refresh follow independently. Original request bytes remain saved for recovery. Mandatory pre-dispatch saves at lines54–55 still prevent a financial call if persistence fails, and prior unknown attempts still retain identical command/body/token identity. Line60 now isolates terminal cleanup with catch/finally and always resolves the closed child result. O12-02 is closed at line32 with the exact one-sentence semicolon copy. No new command, storage schema, money owner or refusal protocol was introduced.

Final FE `js/bpay-next-case-restructure.js` SHA-256 is `79ef7d3810b584e55414cf4ad2d1488249b89c444838b26776febc3b56120ef5`. Fresh hashes for Office, CaseList, main, CSS and index remain those in the checkpoint table. No further essential native source defect was identified in this assigned scope. A4 and all joined business acceptance remain separately NOT_RUN; root's compilation/catalogue/contract results are not executions performed or proved by this read-only review.

## Final A4-driven source amendment — SOURCE PASS retained

Fresh current component/CSS/index/CaseList reads confirm the finite presentation repairs: successful terminal state hides the extra footer and retains the native header Close/settlement path; BLOCKED retains its explicit Refresh; errors carry alert role and precede facts; absent before/after terms show an em dash rather than implying a Candidate default; supplied actual NULL floor still displays Candidate default. Existing exact-input markers and the added comparison money marker preserve native values through the formatter. Scoped currency alignment, bordered13px comparison text/sticky heading, success tone and empty-footer hiding affect only this native window. They do not change request validation, preview invalidation, saved bytes, original UUID/token, actor/parent/owner fences or point receipt/polling semantics. O12-01/O12-02 remain closed.

Final current SHA-256 values supersede earlier component/CSS hashes:

| Source | SHA-256 |
|---|---|
| FE `js/bpay-next-case-restructure.js` | `920956e944a70ed931a896916c1098b488ca1a2439fc42baafa66219649ce0d2` |
| FE `css/bpay-next-office.css` | `c6110e9ccd27805319baba0454aae759971046cc307b6be181e047449bd072d6` |
| FE `index.html` | `fcc181806258dcd8cf4c65812243a7b5581c0fee85acd65e30ed06a3db51f799` |
| FE `js/bpay-next-case-list.js` | `55d450b1782c2b3c9511388b0ea0f923fc850db997d8a9f105bb30e6e2d76d2f` |
| FE `js/bpay-next-office.js` | `456a48e11e6efd161fdeb3adf4cb356356dd42b1d3e95949a7da270577d11c91` |
| FE `js/main.js` | `934e06de52b4e2f536a983036490c7e24f9e625e6ec0a0dfe208ef619570efe0` |

No further essential source defect was identified. This source-only amendment is independent of the separate full42 A4 pixel receipt and does not claim joined business acceptance.

## Commit-boundary hint/token amendment — SOURCE PASS retained

Fresh current reads verify the only later JavaScript change: “Preview first.” appears after the existing loading/journal/terminal branches, only when no busy/error/token exists. It neither enables Apply nor changes input, preview, journal, mutation, readback or lifecycle logic. Its scoped12px native-subtext styling and the restructure-only replacements with existing native panel/text/line/success tokens introduce no broader selector or behavior change. The previous source findings remain closed.

Final current FE component SHA-256: `dd84bd9fbffe4dae93e0c77a6832741e1e154ae2f8ce48a9262f83484069e559`; CSS SHA-256: `e05d4b75fad945d57ab4a51e17818e65612f6ca621d22eba7dc913528cdc4724`. These supersede the component/CSS hashes above. Other current source hashes remain as recorded. SOURCE PASS is retained; full42 pixels are owned by the separate A4 receipt, and no joined business execution is claimed here.
