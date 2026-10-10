# PS-14 Worker / Office caller review

**PASS for saved D05/D06 caller source — no actionable caller defect found.** Independent read-only review,10 October2026, against the full PS-14 card, exact checklist and BD038 reservation design. No SQL, tests, runtime, fixtures, network or product edits performed. This verdict does not certify the financial qualification predicates inside the new SQL helper or complete the whole card; the SQL owner review and D04 future joined note remain separate.

## Closed contract and authority

BE `broker/src/bpay-next-preparation.js` and FE `js/bpay-next-office.js` add the same exact five STATUS header fields: `reservation_mode`, nullable `reservation_projection_id`, nullable `remaining_reservation_cash`, boolean `reservation_internal_zero`, and `reservation_holds_state`. Each transfer row adds exact `reservation_state`, nullable `remaining_reservation_cash`, and `reservation_holds_state`. Existing exact-object validation rejects extra keys; enums are finite. STATUS/CHANNEL_STATUS share the same actual row/header schema, preserving run/worker/channel identity and original transfer cursor validation. Current accepted projection fields remain separate from the original reservation projection identity.

Both callers retain exact decimal strings. Reservation amounts use the actual source's canonical nonnegative two-decimal cash transport; NULL is not converted to zero. Header amounts exist only for ORIGINAL_OBLIGATION/CANCELLING/CANCELLED; materialised ORIGINAL_LEGS requires an actual reservation projection and a deliberately NULL parent amount. Genuine internal-zero parent obligation requires ORIGINAL_OBLIGATION and `0.00`. Cancellation cash must be exactly `0.00`.

The row checks preserve the mutually exclusive sources: INCLUDED_IN_ORIGINAL_OBLIGATION has NULL amount and requires the original parent mode; RESERVED/ISSUED_AWAITING_RECEIPT use the exact native transfer cash in ORIGINAL_LEGS and require BANK execution. Unknown/pending/conflicting execution stays NULL. Reissue is explicitly excluded using the actual original_transfer_id discriminator. Executed/returned/cancelled/internal closed rows carry exact0; internal states require actual INTERNAL_ZERO and native cash0. These are checks of server facts, not a browser remaining-money calculator. `PAGED_ORIGINAL_LEGS` is the private helper's header-only state; STATUS exposes its header mode rather than emitting that state as a transfer row, so the transfer validator correctly does not treat it as a payable row.

The inspected0639 STATUS projection matches the new caller field names/types. Its header is a point worker scope; rows remain the original bounded ordered transfer page. No new Client/Timesheet NET share, member aggregation, command, financial input, bank data or broad receipt tree enters the wire. Existing accepted projection pending semantics are retained rather than redefined by PS-14.

## Presentation and lifetime

Office `reservationDescription` uses explicit mode/state labels and directly appends the unchanged server amount only when non-NULL. An original parent obligation appears once while legs are building; a fully materialised parent says reservations are shown per original transfer, with no parent cash repeated. Partial child rows say they are included in that original obligation. Original transfer reservations occupy a separate column from frozen cash/payment status. Reissue/returned cash, recorded settlement awaiting posting, internal execution, cancellation and uncertain evidence retain distinct labels. A real zero is displayed, while NULL yields no false numeric value.

The screen explicitly says each page is a live read and amounts are not summed across pages. There is no reduce/SUM/subtraction or allocation added to the renderer. Existing accepted bank cash is labelled accepted data, not added to remaining reservations. Frozen work/member detail remains separately identifiable through existing readers; no Summary/export/layout programme is introduced.

`loadStatus` clears the prior STATUS and sibling details before reading, retains the exact run/worker/Candidate scope, validates returned Candidate, and uses the existing typed `after_transfer_no` key. STATUS/CHANNEL_STATUS substitution still qualifies the actual channel. Each Next page replaces the displayed page; no all-page cash cache is accumulated. Render epoch checks suppress stale UI adoption. Existing transport checks authenticated actor/owner before fetch, after response and after decoding; actor-scoped journal and exact uncertain command bytes remain unchanged. The new reservation fields are response-only and are not written into a monetary cache or persisted journal. Existing mutation/retry/confirm/CSV/bank-safety and Draft gates are unchanged.

## Scope and remaining verification

D05/D06 implement only the exact caller decoding/display demanded by RQ-294/RQ-296. SQL0639 may be refined by its independent financial/source review; its final field/state output must remain compatible with this closed contract. Whole-card completion still requires that owner review, the normal existing affected checks and the explicit D04 joined checklist. Actual partial own/external/Case-only materialisation, execution/cancellation and full Worker/queue/minute acceptance remain NOT_RUN.

## Fresh source follow-up: lifecycle consistency

Re-read both saved mirrored `validReservation` bodies and the added existing BE/FE source cases. **PASS remains current.** The finite native-status table now rejects a payable RESERVED state on SETTLED, restricts issued/uncertain/posted/returned/internal states to their real transfer lifecycle statuses, requires ACTIVE holds for RESERVED/ISSUED_AWAITING_RECEIPT, binds uncertain execution to ORIGINAL_LEGS, and binds cancellation row state to the same header cancellation mode. Original amount/null/reissue/internal-zero rules are retained without financial arithmetic. These checks strengthen the caller boundary and do not reinterpret a cash amount.

The existing BE PS14 case exercises parent ORIGINAL_OBLIGATION with a BUILDING child whose counted amount is NULL, accepted executed/posting evidence with0, and negatives for missing parent cash, parent cash repeated in paged mode, duplicated child cash, SETTLED-as-RESERVED and numeric cash. The existing FE case uses the actual transport to accept the parent/included-child shape and reject missing parent cash, counted child duplication and contradictory settled reservation; source checks retain the explicit paged notice and reject reservation arithmetic. Existing cache/dependency assertions now select the actual versioned Office asset while preserving the shared UI-before-consumer requirement. Existing actor/cursor/channel checks remain. No tests or runtime were executed by reviewer.

## Raw reviewed hashes

| Source | SHA-256 |
| --- | --- |
|BE `broker/src/bpay-next-preparation.js` |`3917aa142e8eff4b34fd16ec979ee4adc45ff3c6cc44dbd0e5b1ea835ea23244` |
|FE `js/bpay-next-office.js` |`c36b465b844348fe2c37f0d8d577119ff0f7c0719894a159a4e4f299c58b04ae` |
|BE existing preparation tests |`d78383f90ab7e83c7abd15e608f5f63179caefc7a525fb1b2ec3065427c85bdf` |
|FE existing Office tests |`4a55d22bee52daed82162f81ea80ce4584b98315c9ae783d4c8bf68c38bfe896` |
|BD038 scope |`bc11b07a3373d0f7360adeb47d5e5b214d4ccb091da9fd544d3720c6d55bc174` |
|0639 SQL output inspected for field agreement |`03bc60d6b4f197c585ae7b6c7ef2000f95b6b589df9a7ac8d01887e5c43f5445` |

Root follow-up: the only subsequent Office source edit removes one blank-line trailing space (no executable change); final raw Office SHA2569a946927ad0329c5da6e8da550543ec0cce6d34ac6dfd6b9e0e188a21af6bab4. BD038 scope now appends concrete implementation/unchanged-body-copy details, hashd2a3e40c1b905d36a152b151f73c19541c468e322d8979db6accdaf001157b7a, independently rechecked by SQL reviewer. Caller semantic source PASS retained.
