# Banking Pay Draft V8 — final TEST release handover

Status: **local seal passed; Miget TEST release pending**.

This candidate combines the completed Workbench certification producer and the bounded Create Draft consumer. It is a reliability and orchestration correction. It does not change payment policy, eligibility, amounts, gross/net treatment, PAYE treatment, Umbrella VAT treatment, payment method, routing, approvals, provider execution, settlement or remittance.

## What the candidate changes

- A selected Workbench set is certified once and consumed from bounded server-side rows. The Worker does not carry repeated giant selection arrays.
- Create Draft advances in short, replay-safe pieces and preserves the existing fixed statement and lock budgets.
- The configured selection ceiling remains 50,000. The largest materialised test is 5,000, as directed; no 50,000-row workload was run. An exact scalar count of 50,001 fails before work begins.
- PAYE and Umbrella are created as the same authoritative channel partitions and durable Draft artifacts expected by the existing downstream payment route.
- A mid-route failure is compensated: partial Draft batches/items are left terminal and inactive, reservations are released, truthful audit evidence remains, and the exact retry resumes or returns the prior terminal result.
- The failed-payment no-money release now accepts its two existing terminal movement classifications and returns the same 66-field result without exceeding PostgreSQL's constructor-argument limit.

## Policy and downstream equivalence

The controlling policy artifact is `codex_outputs/banking-pay-create-draft-policy-v1/BANKING_PAY_CREATE_DRAFT_EXECUTE_POLICY_CONTRACT_V1.json`.

The Create Draft → PAYE net entry/Umbrella preparation → Execute Payment → status/cancellation/reversion relationship is recorded in `codex_outputs/banking-pay-create-draft-policy-v1/BANKING_PAY_CREATE_DRAFT_TO_EXECUTE_PAYMENT_LIFECYCLE_MAP_V1.md`.

The executable catalogue contains 88 finite Banking Pay classes. It covers ordinary and paired Timesheets, PAYE and Umbrella, gross/net additions and deductions, advances, loans, credits/debits, expenses and mileage, prior/partial/settled/superseded states, recoveries and headroom, saved rate and payment-method resolution, scheduled/failed payments, whole-Candidate and whole-Draft cancellation, Workbench return and Execute Payment preparation.

No V8-specific downstream payment branch is introduced. Existing downstream owners continue to consume the same batch, Candidate, item, allocation, reservation, snapshot, breakdown, finance-effect, expected-effect and frozen-lineage contracts.

## Final local proof

- 1,246/1,246 normal tests passed.
- 1,008/1,008 active CommonJS tests passed; 101 database/environment-gated cases remain explicitly labelled, not silently accepted.
- 498/498 active module tests passed; one structural environment gate remains explicitly labelled.
- Database integrity: 221 migrations and 569 repeatables.
- Contract coupling passed.
- PostgreSQL 17 and 18 both passed clean NEW and UPGRADE rehearsals at commit `0415cdfa86c8dc5bea6bf7a4ee4244fd5a8a6f31`.
- All four rehearsals installed contract SHA-256 `2cb1f059ff9f661105eb3e15b2fc985a45a46f17e7a061c23366ae3f8ee8b56b` exactly.
- The TEST Worker packaged successfully with Wrangler 4.125.0 in dry-run mode.
- The largest measured production-owned database call in the bounded 5,000-row proof was 3.987 seconds; the largest reservation-finalisation call was 1.000 second. No production timeout was increased or bypassed.
- Replay, response loss, same-selection concurrency, mixed PAYE/Umbrella failure compensation and zero provider/payment/settlement/remittance effects all passed.

## Release order

1. Commit this evidence pack and prove the candidate tree is clean.
2. Fetch the shared TEST branch and stop if a new commit has appeared.
3. Push the one coherent candidate to `test`.
4. Let the repository-connected TEST Worker build the exact pushed commit, while the database contract capability gate keeps unavailable V8 paths fail-closed.
5. Dispatch the protected managed Miget TEST `UPGRADE` workflow for that exact commit.
6. Prove the Miget release is `VERIFIED`, repository/installed contract hashes agree, required function definitions match and browser roles remain isolated.
7. Prove the TEST Worker active version/commit and `/healthz`/`/readyz` health.
8. Run read-only browser acceptance. Do not create a real Draft and do not execute a payment without separate authority.

## Local-only caveat

The candidate is not yet installed merely because the local proof is green. Final completion requires the exact protected Miget TEST release, Worker deployment and read-only installed-state audit described above.
