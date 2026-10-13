# WB-29 final Ready Office source review

13 October 2026. Read-only saved-source comparison of frontend Workbench, Office stylesheet and index against frontend HEAD, with the earlier Ready DTO/presenter SOURCE PASS retained. Root alone changes product. No product, SQL/RPC, database, business, fixture, test, runtime, browser or screen action was performed by this reviewer. This is not A4 pixel or joined application acceptance.

## Initial current finding FEO-01

The task names a final `expenseSnoozeStatus` factual display correction, but current saved Workbench bytes at SHA `52174ad8240c378711291e327c16acf75776b9473e533efbbea55c352cd17878` contain no helper/reference with that name. The touched non-Work detail branch1310 appends only existing `snoozeControls(statusCell, member, 'EXPENSE_LINE')` before continuing; it does not append `memberStatus`/Snoozed-until wording. Its new Status cell therefore contains actions without the described factual snooze date. Reported to root immediately. The expected small status-only save remains to be reread before final SOURCE closure.

## Reviewed retained behavior and layout delta

All current chosen DTO assignments and closed eligibility/null/channel guards match the prior reviewed block. The existing amount function still gives manual-unavailable/reference NULL, accepted known captured Source, unknown and RESERVED values precedence; only eligible unselected READY uses the server `chosen_leaf_amount ?? included_source_ex_vat`. Zero is preserved. Component heading finds a genuinely nonnull chosen server amount and retains separate native waiting/in-Draft figures. The group uses native maintained group_amount, with original Case estimate/Outstanding/Due precedence and the native rate-set badge. No browser quote, conversion, new money basis, selection arithmetic or post-read resort appears.

The final payment-value wrapper moves the same factual name, native caret, weekly limit, reference and badge into one value container. Exact focus keys, event propagation, captured row identity, existing active gates and callbacks remain. Native table rows and cursor/continuous-scroll anchors are retained. Component day/expense rows now use six description columns plus one Amount and one Status cell, keeping the original eight-column structure while moving existing snooze controls out of the monetary cell. Original Source selection basis, command journal, snooze seed/expected versions and financially guarded requests are unchanged by the diff.

The CSS diff is confined to the existing modernized read modal and actual Workbench classes: one bordered13px Candidate frame, flat transparent detail/day/nested-table surfaces, zero padding around native nested shift scrollers, desktop Payment/Actions widths,24px desktop caret and44px phone caret, and44px Close/footer targets only for the native filter content. It retains existing Candidate and nested-shift maximum heights, overflow/overscroll, focus, scrolling listeners and paging bounds. No global selector, `!important`, overlay, extra action or new screen host is introduced. Actual pixel geometry/accessibility remains the independent A4 review.

Index changes only bump the existing Office CSS and Workbench asset cache versions to `20261013-wb29-ready-r1`; script order/routes/modules remain original. Authorised Source summary copy is already part of the preceding DTO/presenter PASS, with all corresponding native HEADER/CANDIDATE amounts unchanged.

## Initial raw SHA-256

| Frontend source | SHA-256 |
| --- | --- |
| js/bpay-next-workbench.js | `52174ad8240c378711291e327c16acf75776b9473e533efbbea55c352cd17878` |
| css/bpay-next-office.css | `5b32cfe34afaa6bf9a986028b7cb0bbe48522372f5ca5566bbe67fe97f204d29` |
| index.html | `7dfb708a132093fa03c85f08a1d7d4867191a10202f79f832fd9de8e55076502` |

No other concrete source defect found in the reviewed final delta. Initial final verdict remains OPEN only for FEO-01/the named status-only save.

## Final saved-source closure

FEO-01 is closed in the freshly reread non-Work branch at line1310. The implementation reuses existing `memberStatus(member)` for each actual retained member before the existing Expense/Mileage snooze controls. It does not require a new helper: READY remains blank, a genuine dated SNOOZED member shows its native date, RESERVED shows In Draft, and other states use the original issue wording. The amount, chosen/frozen/Source precedence, selection basis, native action guards and request/journal semantics are unchanged by this correction.

The final full diff against frontend HEAD was reread. Latest scoped CSS uses220px for desktop Actions, preserves the one native scroll frame and flat detail tables, explicitly exposes phone Amount/Status labels, hides only a genuinely empty Status cell, and retains44px native filter controls/phone caret and24px desktop caret. These changes add no control, money calculation, request operand, cursor, financial authority or scroller lifecycle. The index retains only the two previously reviewed asset-version bumps.

Final verdict: **SOURCE PASS; zero open findings** for the requested three-file final Ready frontend slice. The prior closed DTO/presenter SOURCE PASS is retained. This source receipt does not approve pixels, runtime behavior, joined acceptance or completion of the whole WB-29 card; those have their separate owning reviews.

| Final frontend source | Raw SHA-256 |
| --- | --- |
| js/bpay-next-workbench.js | `26e457e3335cf79e18386463af7dc31a61c44fdc0d6613b32cec140ee98af265` |
| css/bpay-next-office.css | `a046a6ea08037328c966588d912775a3cc83404341b228ed76021a05f524fe14` |
| index.html | `7dfb708a132093fa03c85f08a1d7d4867191a10202f79f832fd9de8e55076502` |
