# EX-01 protected action consumption

11 October 2026, BD110. This registry distinguishes the actual current acceptance seams from later cards. Registration alone never implements a money action.

The current chain is `0270 -> WB-19 2150 -> EX-01 11102026_1306`. The six native branches below have closed Worker token validation, fixed proof recording, a locked server scope and the exact five-argument consume call before native acceptance in the same transaction. Original native money writers are retained.

| Protected action | Current scope and consumption | Office caller / later owner |
| --- | --- | --- |
| CANCEL_REQUEST | 1302 resolver; 1306 consume; original cancellation request owner | Existing Cancel payment uses the native helper |
| CSV_ISSUE | 1302 resolver; 1306 consume before first issue; exact already-issued instruction/issuer readback exempt | Existing first single-transfer CSV issue uses helper; original instruction retrieval retained |
| CASH_REISSUE | 1302 resolver; 1306 consume; immutable whole returned-cash native request | EX-32 supplies its screen |
| REISSUE_CANCEL | 1302 resolver; 1306 consume; original transfer/member/request scope | EX-33 supplies its screen |
| CSV_RETURN | 1302 resolver; 1306 consume; exact native transfer/receipt/amount/time/original command | EX-31 supplies its screen |
| WRITE_OFF | 1302 resolver; 1306 consume; exact component/revision/scope/raw amount/reason | FC-11 supplies its screen |
| PAYE_SAME_WEEK_OVERRIDE | Registered, acceptance not installed by EX-01 | DR-10, next item |
| EXECUTE_REQUEST | Registered, acceptance not installed by EX-01 | EX-02 |
| EXECUTION_APPROVE | Registered, acceptance not installed by EX-01 | EX-03 |
| CSV_FILE_ISSUE | Registered, first issue only; no redownload proof | EX-10 |
| CSV_DISPOSITION | Registered, acceptance not installed by EX-01 | EX-13 |
| CANCEL_SELECTION | Registered, acceptance not installed by EX-01 | EX-27; EX-29 scheduled password-only exception |
| RELEASE_REFUSED | Registered, acceptance not installed by EX-01 | EX-23 |
| RETURN_SELECTION | Registered, acceptance not installed by EX-01 | EX-31 |
| OUTCOME_REVIEW_RESOLVE | Registered, acceptance not installed by EX-01 | EX-25 |

All future rows must add their actual native joins, typed route, owner dispatcher and caller in that card's chain order. No string-only success, stub command or speculative money writer is installed here. Existing outcome-only settlement/refusal/unknown/internal actions retain their actual authoriser gates without a new proof requirement.

Actual authentication is the existing START/VERIFY pair with the exact BPAY_NEXT_ACTION purpose. START computes native scope independently and compares the displayed preview hash for concurrency only; VERIFY signs stored challenge action/scope. Worker verifies the actual actor/action/signature and records only verified metadata/token SHA. SQL receives proof UUID only. First-use business locks precede proof-row lock. Exact consumed native replay uses retained receipt qualification and the original scope; unused expiry or a new identity refuses.

The Office journal retains the original typed bytes/UUID/token before dispatch and offers identical recovery after an unknown financial response. Proof-record uncertainty alone is not financial uncertainty. Historical tokenless journals stay readable; new protected submission is refused without authentication, with exact already-issued CSV readback the narrow exception. No historical rows are assumed or changed.

Evidence: EX-01-design.md, EX-01-scope-replay-investigation.md, three independent source reviews and the SCR-66 A4 receipt. All joined authentication, payment, replay and permission journeys remain AUTHORED / NOT_RUN.
