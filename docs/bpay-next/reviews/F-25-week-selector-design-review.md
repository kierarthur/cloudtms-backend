# F-25 finite weekly selector design review

9 October 2026. Independent saved-source review only. No SQL, runtime, tests, fixtures, hosted reads or product edits were executed. This review concerns design fragments, not installed functions or full F-25 completion.

Reviewed `docs/bpay-next/design/F-25-week-origin-selector.md`, current raw SHA-256 `73d0680de86d22455dc13787c946932c5c147e948a149a401612b7576fcb3380`, including its final prefix and receipt-chain appendices, against the existing economic and eligibility owners. This differs from the earlier supplied `e1938…` hash because the saved design has later additions.

## WD01 — preserve genuine zero OWN settlement with positive split projection

**P1, open design correction.** The INTERNAL_SETTLEMENT predicate at design lines 339–343 requires `projection_cash=0`. Existing `04102026_0490_banking_pay_next_internal_zero_settlement_v1.sql` lines 183–191 explicitly permits positive total projection cash when the actual destination execution leg is OWN. A sealed case NET projection can have `own_amount=0` and positive external credits. Its zero OWN transfer completes through INTERNAL_ZERO; the external leg remains separate, and total projection cash remains positive. Rejecting this receipt makes a genuine completed prior week unavailable to fresh affected-case acceptance.

Use the real owner's distinction: require exact internal receipt/transfer/worker/Candidate/projection identity, INTERNAL_ZERO and zero transfer amount; for an unsplit transfer require zero total projection cash, while for an OWN leg require the exact completed destination state, group/anchor/projection binding and zero original OWN amount. The existing later original-scope guard must remain. Preserve the case NET eligibility state and its genuine zero own operand; positive external credit is not Candidate earnings.

## Checks and required implementation obligations

The design correctly enumerates actual original workers before testing contributions/completion; refuses incomplete or missing origins; selects latest accepted NET including pending requests; separates PREPARE/NET economic basis from later outcome/cancel/expiry eligibility; preserves historical retired projections through receipt ancestry; and leaves returned ordinary earnings UNBOUND. CASE_PAYOUT is explicitly excluded zero. The completed-original expiry path checks the exact old DONE/READY snapshot rather than treating EXPIRED as a completion certificate. Partial preparation or missing contribution remains an explicit refusal.

The design itself correctly labels these still-required parent checks; they must be present in the product review and are not proved by the fragments alone:

- Complete bounded original-worker, request-prefix and receipt identity census. Latest/count-only selection cannot authenticate predecessor retirement; every successor must bind the genuine prior projection and owner completion, with reconstructible immutable output digests.
- Exact DRAFT instruction/results, NET_DEDUCT results and reused DRAFT outputs, with retained original identities, rather than only matching counts.
- Exact serial reissue transfer and return-cash lineage with the immediately preceding receipt; complete ONEOFF receipts remain outside the earnings transition chain.
- Pre-clock business/FK locks and comparison of reconstructed basis/eligibility/transfer tuple with the observed genuine contribution, followed by retained-child consumption only.
- The retained proof must meet 2225's 16,384-byte JSON limit before agency-clock allocation. The independent limits of 1,000 instructions or 100 receipts do not imply that full identity/output arrays fit that limit. Use a reconstructible closed scope/digest representation or an explicit pre-clock bounded-scope refusal; never discover a size violation only at child INSERT after allocating the clock.

Full F-25 remains incomplete. WD01 must be corrected before the affected INTERNAL_ZERO branch can receive source PASS; the incoming weekly producer requires a separate fresh review.
