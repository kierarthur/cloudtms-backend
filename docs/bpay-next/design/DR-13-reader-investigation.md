# DR-13 reader investigation — read-ahead only

11 October 2026. DR-13 is approved sequence **122**, not88 (`08_BUILD_ORDER_AND_CODEX_INSTRUCTION.md:206`, subject to the current sequence file). Root corrected the next item to DY-02/88; this note stops short of implementation. No product, checklist, SQL/database/business action, tests, fixtures or hosted calls. Application acceptance **NOT_RUN**.

## Exact contract and boundaries

The complete card is `03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:357–376`; all its checklist rows are `docs/bpay-next/IMPLEMENTATION_CHECKLIST.md:3515–3535`. D02 REVIEW_CASES, D03 ORIGIN_UNAUTHORISED_WAIT, D04 CASE_NO_PAYABLE_AMOUNT and D05 the Draft review display are remaining child artefacts. D01 is shared immutable allocation-row evidence; D06 is the reviewed output-only specification receipt. Dependencies are DR11, DR16, WB41 and F16. The card requires no design deliverable of its own and no new allocator/money owner.

Rows must explain the **frozen allocation result**, separately showing principal outstanding, active reserved, recoverable this run and remaining unreserved balance (card361). Required reader fields are `recoverable_this_run`, `outstanding_after`, `reason_code` (370). A result's shortfall is not interchangeable with total principal outstanding or unreserved residual. Live current Case balances must not replace the accepted explanation. WAIT is typed paused recovery with balance intact, not zero entitlement (363).

The display is capped `GBP x.xx recoverable this run, GBP y.yy outstanding`; PAY_HEADROOM/TAKE_HOME_FLOOR zero uses insufficient pay, EARNINGS_THRESHOLD uses below the minimum earnings threshold, CASE_CAPACITY uses the already-held weekly instalment explanation, and WAIT uses `GBP 0.00 this run – recovery paused, waiting for re-authorisation` (371). `CASE_NO_PAYABLE_AMOUNT` is a whole-Worker REVIEW code, **not a per-row reason** (365). Its Candidate treatment/Remove and closed plain-language map belong to DR15/DR05, consumed here.

## Current source facts

The registry's current existing preparation reader is `public.bpay_next_preparation_page_v1(uuid,text,jsonb)` in `11102026_0814_banking_pay_next_preparation_discard_v1.sql:2983`. Its closed kind/input switch starts2992; CURRENT_CASES and CASE_COMPONENTS are3007–3009. The Worker closed page map is `broker/src/bpay-next-preparation.js:30` onward and has no REVIEW_CASES entry. The existing current Case component output107 exposes live approved/funded/recovered/active counters, current rule fields and outstanding, while `recoverable_this_run` is constrained to null. These existing current-case rows cannot satisfy the frozen Draft review contract by relabeling fields.

The native stored result already has allocated target amount, shortfall and separate typed cap/affordability/capacity reasons (`04102026_0300_banking_pay_next_case_instruction_core.sql:361–403`). Its cap reason enum is only TAKE_HOME_FLOOR, EARNINGS_THRESHOLD, PAY_HEADROOM and CASE_CAPACITY (380–382); no ORIGIN_UNAUTHORISED_WAIT result exists in this inspected enum. Original component counters are native live money facts (89–90); period opening and active capacity have distinct semantics (133–152). Native instruction nominal/capacity fields are not automatically a snapshot of the four balances required by the revised contract (222–286).

## Narrow future seam; unresolved source trace

At DR13's own sequence boundary, re-read DR16's exact REVIEW_CASES owner and current WB41 frozen WAIT support, plus all later shared-chain definitions. Extend only that named reader and its existing closed Worker/UI contracts to expose genuine immutable result/basis operands. If the four required historical balances are not retained by then, report the precise named capture/basis gap before adding support; do not reconstruct them from mutable current component counters or invent a new table/protocol. No current money formula, row kernel, producer, public trigger or background job is justified by this read-ahead.

The ordinary0340/page and complete DR16/WB41 source qualification remain **untraced in this intentionally stopped read-ahead**. No completeness or build verdict is asserted. DY02/88 investigation resumes next.

Sequence122 update12October: this dated read-ahead is retained provenance only. Current0552 implementation and BD135 supersede its old owner/gap/manual-release recommendations; see DR-13-build-scope.md/read-contracts.md/completion-partial.md. D03/WAIT-specificD05/ITEM explicitly blocked; no genuine accepted WAIT is claimed.
