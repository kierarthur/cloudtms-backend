# DR-13 transport and Office source review

Reviewed 12 October 2026, independently, against the complete approved DR-13 card (03:357–376), D01–D06/ITEM, and BD135 `DR-13-build-scope.md`. Root is the sole product author. **SOURCE PASS for the reviewed transport/presentation additions; no essential source defect found.** This is not runtime, A4, joined financial acceptance, or whole-item completion.

## Contract and native authority

The Worker and Office shared review-contract blocks are exactly identical after line-ending normalization (8,306 characters). Their closed `REVIEW_CASE` shape adds precisely `principal_outstanding`, `active_reserved_before`, `recoverable_this_run`, `remaining_unreserved`, and `unreserved_limit_note`. Missing keys and extra keys remain rejected. The four amounts retain nullable, nonnegative, at-most-two-decimal string typing; the note admits only the one native literal. The explicit finite type dispatcher adds `reviewLimitNote` without intercepting the older REVIEW phase/reason types. Sources: Worker `broker/src/bpay-next-preparation.js:18,30,66–69,286`; Office `js/bpay-next-office.js:209,221,257–260,413`.

The native reader derives source balances from the original instruction/selection observations and the exact accepted DRAFT or NET allocation result. It does not substitute current Case balances. `recoverable_this_run` is allocated target money for a selected deduction, and equals the existing `this_draft` field. Principal, prior reserved balance, outstanding, and remaining unreserved are source money. The remaining figure uses both captured component and Case constraints; the shared-balance note explains a tighter Case constraint without misrepresenting it as component-only capacity. Historical missing reservation observations remain NULL, so the reader cannot manufacture a known remaining amount. Sources: `supabase/repeatable/12102026_0552_banking_pay_next_recovery_explanation_v1.sql:80–89,138–139` and its original instruction/result joins.

Both validators require the recoverable/selected/direction tuple to match, require principal/reserved/timestamp when remaining is known, and reject an unqualified shared-limit note or a remaining amount exceeding the same-source unreserved upper bound. Their BigInt penny comparison is a consistency check only: it does not price, convert, allocate, or replace a returned amount. Existing context revisions, frozen-offer qualification, result identity, foreign-capacity cause pairing, page/cursor limits, deadline/error handling, actor/NEXT gates, and the original mutation journals remain in place.

## Office presentation and preserved behavior

The existing five columns and ten native sort choices remain. This Draft uses native `recoverable_this_run` for deductions; the existing payout amount remains the payment value. The owned first cell separately labels Original balance, Already held, and Unreserved after this Draft. Unknown money displays a dash. The saved source channel and Figures from timestamp make the original observation and monetary basis visible; the existing Due/Outstanding source qualifier remains. Notes use the native plain explanation rather than raw technical codes. No browser rate, VAT, floor, headroom, or allocation calculation was added. Sources: Office `js/bpay-next-office.js:2047–2055`.

The existing native selection/focus, bounded continuous-scroll reads, current actor/owner/namespace checks, and error/recovery lifecycle are retained. No new command, route, kind, list, money owner, or legacy renderer branch was added. The actual stylesheet is `css/bpay-next-office.css` (`css/style.css` does not exist in this worktree). Its new rules remain under `#modal.ctms-modern-modal`; the final phone override keeps all five existing cells displayed as stacked facts. Source: `css/bpay-next-office.css:1044–1052`. Visual fit and control policy remain the separate A4 review.

## Explicit acceptance limits

BD135 correctly retains the D03 conflict: the current WB-41 pre-click ORIGIN_UNAUTHORISED_WAIT item is informational/unselectable and absent from accepted LEAF/Create membership. This batch preserves the dormant typed explanation and does not invent an accepted WAIT row or replace Rule B captured money with a later live origin state. D03, the WAIT part of D05, and ITEM therefore remain open. This review does not claim that flow passes.

The owner E2 SYSTEM release direction continues to control CASE_NO_PAYABLE_AMOUNT instead of the older card's manual Remove wording. This batch changes neither release mechanics nor command authority. Zero/capped recovery figures continue to come from the existing native allocation result, leaving the balance outstanding; no permanent-block or zero-entitlement rewrite is introduced.

No product edits, SQL/database calls, fixtures, tests, browser actions, screenshots, hosted calls, or commits were performed by this reviewer.

## Reviewed saved-byte SHA-256

| File | SHA-256 |
| --- | --- |
| BE `broker/src/bpay-next-preparation.js` | `acb469410855505e1d8c4113274d71db7a2df8b62a0b3af2e50b3eb40e2be32e` |
| FE `js/bpay-next-office.js` | `c2a15bc9cf598dc61e007ac89ceb97a5f3aa87fe5ced38ef58c761e9199e0246` |
| FE `css/bpay-next-office.css` | `3c6e8faeea37f90d4b830733d27deedf9aba816a762edfb217c65d742dfd2c64` |
| Native recovery explanation `0552` | `36d4639005dba6cd7b43ef406e14623f22dd6d692717f38e509d2efe40e6da45` |
| BD135 build scope | `d572dd18145dc111b7bebb869b505c304e07cfe2795917402958c77207d32c96` |
