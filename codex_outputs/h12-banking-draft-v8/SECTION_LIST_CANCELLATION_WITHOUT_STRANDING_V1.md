# SECTION LIST — Cancellation without stranding

This list is frozen. A failed check is resolved within its existing item; it does
not create a hidden extra stage.

## 1. Freeze policy — DONE

- A confirmed-unpaid whole-Candidate payment or whole Draft must be cancellable.
- A queued, sent, wrong or stale email must not block the financial cancellation.
- A cancellation email is sent only when the authoritative outbox proves the
  corresponding original paid/being-paid email reached `SENT`. A queued or absent
  original email does not qualify, and email state never vetoes cancellation.
- A complete source-less adjustment is carried forward unchanged by the existing
  owner.
- An incomplete source-less adjustment is not recreated, deleted or recalculated.
  Its frozen evidence remains retained and Banking receives an investigation
  alert.
- Paid, possibly paid, provider-pending, stale, raced, partial-scope and settlement
  safeguards remain unchanged.

## 2. Prove current failure — DONE

- Current owners independently turn an incomplete source link into a cancellation
  veto.
- Current apply owners also couple financial cancellation to payment-email cleanup.
- Immutable pre-change evidence records both failures before correction.

## 3. Freeze the smallest safe correction — DONE

- Keep the source-restoration detector truthful.
- Change cancellation admission only for a complete, confirmed-unpaid Candidate or
  Draft.
- Preserve safe carry-forward unchanged.
- Preserve incomplete frozen facts without guessing a future financial source.
- Derive one bounded existing-style Banking investigation alert per affected item.
- Remove payment-email cleanup from the financial transaction; later notification
  remains separate.

## 4. Write the local correction — DONE

- Add exact replacement database owners; keep historical owners byte-identical.
- Activate the new email-independent rule only after a final locked check proves
  there are zero active correction requests; do not add migration or compatibility
  workload for old requests that do not exist.
- Add narrow indexes and bounded alert projection.
- Add Worker allowlist support and item-specific alert acknowledgement.
- Add frontend label, Current Payment Status navigation and item-specific clearing.
- Add a separate idempotent cancellation-notice decision after financial success;
  it may queue a notice only for the exact matching original notice proved `SENT`.
- Add focused source, mutation and rollback verifiers.
- Correct the read-only integrity checker so it verifies each frozen request with
  that request's own exact versioned fingerprint rather than reporting a false
  mismatch.
- Restore the existing manual carry-forward identity omitted by the current
  Workbench producer, so the successful cancellation can rebuild without changing
  its amount, sign, tax/VAT treatment or any eligibility rule.
- Treat completed `REVIEW_REQUIRED` operation history as terminal audit evidence,
  not active cutover work; the final locked zero-active check remains mandatory.

## 5. Prove both cancellation routes — DONE

- Pre-bank whole-Candidate and whole-Draft cancellation.
- Confirmed-no-money whole-Candidate and whole-Draft unwind.
- PAYE and Umbrella; ordinary, safe carry-forward and every executable incomplete
  source shape.
- Exact retry, lost reply, simultaneous request, stale/raced/paid/provider-unknown,
  wrong-link, partial-scope and communication-independence checks.

## 6. Prove the complete return journey — DONE

- Exact items, allocations, reservations, Candidate/batch totals and frozen evidence.
- Safe carry-forward reappears once in the Workbench.
- Incomplete adjustment remains preserved once, is not guessed back into pay, and
  has one actionable Banking investigation alert.
- Alert acknowledgement affects display only.
- A cancellation email is queued exactly once only when the matching original
  paid/being-paid email is authoritatively `SENT`; `QUEUED`, failed, cancelled,
  unrelated and absent originals produce no cancellation email.
- Local proof uses rollback and performs no external provider, payment, settlement,
  remittance or email delivery; it verifies only the exact durable email decision.

## 7. Seal the coherent release proof — IN PROGRESS

- PostgreSQL 17 and 18 NEW, UPGRADE, first use and idempotent reapply.
- Fixed statement/lock budgets and bounded 1/101/1,001/5,000 work.
- Wider Banking Pay/Create Draft/Execute Payment/cancellation policy-parity suites.
- Security, metadata, exact contract/manifest/release closure and rollback.
- Latest shared-source reconciliation, independent review, one coherent candidate,
  protected Miget TEST installation, installed hashes and authorised TEST browser
  acceptance.
- Fresh post-install audit with zero unexplained divergence before final PASS.

## Current position

Items 1–6 complete. Item 7 is active.

Item 4 completion ledger:

- DONE — cancellation admission remains independent of email state.
- DONE — bounded investigation-alert projection and exact complete count/hash;
  PostgreSQL 17 and 18 proved 1/101/1,001/5,000 rows, with a 1.467-second
  slowest call under unchanged budgets.
- DONE — Banking alert allowlist/frontend handling and manual carry-forward
  economic identity correction are locally written and source-tested.
- DONE — the immutable pre-correction matrix is frozen on PostgreSQL 17 and 18:
  all four PAYE/Umbrella Candidate/Draft routes reproduce the exact current veto,
  with zero financial, provider, settlement or remittance effects and no active
  historical-request runtime cases.
- DONE — exact matching-original-`SENT` cancellation-notice owner and its
  retry/lost-reply race hooks.
- DONE — final combined source freeze and exact two-engine runtime matrix.
- DONE — one fresh complete cancellation audit covered all 40 logical cases in
  16 packed physical journeys across PostgreSQL 17 and 18. The machine ledger
  found zero missing, duplicate or extra cases. The slowest cancellation-route
  call was 1.183 seconds and the slowest individual Workbench call was 2.101
  seconds under the unchanged 15-second outer budget.
- DONE — PAYE and Umbrella were exercised independently for one-Candidate and
  whole-Draft PRE_BANK and NO_MONEY routes; neither channel is credited by
  analogy to the other.
- DONE — focused source and mutation checks passed 120 with zero failures or
  TODOs. Two environment-gated concurrency wrappers remain visible for the
  release runner even though their underlying database journeys are covered.
- ACTIVE — full repository, release-engine, contract, security and latest-source
  reconciliation required by item 7.
