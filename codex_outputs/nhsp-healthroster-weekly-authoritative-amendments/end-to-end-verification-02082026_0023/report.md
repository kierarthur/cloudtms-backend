# NHSP/HealthRoster Weekly authoritative amendments — implementation and end-to-end verification

## Outcome

The targeted NHSP Weekly and HealthRoster Weekly import-authoritative amendment implementation is installed in TEST and the supported routes have been exercised successfully.

Proved in real TEST data:

- HealthRoster Weekly: original issued position, first reversal/replacement, repeated invoiced correction generation, and amendment of the latest uninvoiced generation in place.
- NHSP Weekly: original issued position, first reversal/replacement, amendment of that uninvoiced generation in place, issue of the pair, and creation of the next correction generation against the immediately preceding positive member.
- HealthRoster Weekly validation-only: both +1 hour and -1 hour imports produced validation/query outcomes and made no change to the timesheet, TSFIN, invoice, invoice lines, or correction children.
- HealthRoster Daily validation-only: both +1 hour and -1 hour imports produced validation/query outcomes and made no change to the timesheet, TSFIN, invoice, invoice lines, or correction children.

Proved in a disposable PostgreSQL fixture for both NHSP and HealthRoster:

- cross-import discovery of a physically deleted invoiced member;
- archived/live correction-member supersession and later reconstruction;
- multi-source aggregate credit allocation by exact frozen source segment;
- zero-hours with non-zero financial balance blocking;
- transitive archived-member supersession;
- invalid and incomplete operation-authority controls;
- malformed credit provenance controls;
- repeated `+10`, `-10/+11`, `-11/+12`, `-12/+10` correction mathematics;
- terminal effective-positive schedule and policy authority;
- real Import Review apply-envelope evidence production.

The fixture schema now embeds the current canonical helper definition, so the supplied schema and fixture files run together without a manual function replacement.

## Business rules demonstrated

1. Weekly import-authoritative processing is confined to NHSP Weekly and HealthRoster Weekly contracts configured as import-authoritative.
2. HealthRoster Weekly validation-only and HealthRoster Daily remain validation/reference-only; they do not create, amend, reverse, authorise, unauthorise, invoice, or pay timesheets.
3. An authorised but uninvoiced ordinary source is amended through the normal source route where lifecycle protection permits it.
4. A complete uninvoiced correction generation is amended in place; replay or a later authoritative import does not create a duplicate generation merely because hours changed again.
5. Once the latest positive corrected-hours member has been invoiced, the next correction generation reverses that immediately preceding positive member, not the original timesheet by assumption.
6. The cumulative frozen Weekly-hours ledger, not a surviving row guess, determines the effective invoiced position.
7. Archived rows are audit-only. An archived/live sibling can be repaired using a fresh correction identity; an archived-only generation cannot become the active mutable generation.
8. Physically missing historical members are reconstructed only from validated durable operation/audit/frozen evidence.
9. Non-hours invoice components do not affect changed-hours route selection or role invoice state.
10. Credits use exact source-segment hours and economics for aggregate invoice lines. Ambiguous or malformed provenance fails closed.
11. A zero-hours but non-zero-money historical balance is non-standard and is blocked.
12. A fully credited zero balance may use `AMEND_SOURCE` only when a genuinely safe, current, unpaid, unlocked, non-invoice-lined ordinary source exists. Paid or invoice-lined current-source states remain blocked; genuinely paid-and-uninvoiced sources retain the existing rollover route.
13. Frozen invoice and credit artifacts are not rewritten by Import Review.

## Real TEST evidence

### HealthRoster Weekly import-authoritative

Root timesheet: `268a1afc-c800-4435-80ec-8630539499df`.

Observed sequence:

```text
12 original issued
-12 / +13 generation 1 issued
-13 / +11 generation 2 issued
-11 / +12 generation 3 created
-11 / +10 generation 3 amended in place using the same member IDs
generation 3 issued
```

Final effective position:

```text
10 hours
£200 pay
£400 charge
£200 margin
```

Generation 3 used the generation-2 positive member as its parent. It did not repeatedly reverse the original timesheet.

### NHSP Weekly import-authoritative

Root timesheet: `fb83a726-f4b2-40df-bc3f-d41a84746650`.

Observed sequence:

```text
+7.5 original issued
-7.5 / +8.5 generation 1 created
same generation 1 amended in place to -7.5 / +6.5
generation 1 issued
-6.5 / +8.5 generation 2 created
```

Generation 2 used the generation-1 positive member as its parent. It did not use the original timesheet as the new reversal basis.

### Validation-only isolation

HealthRoster Weekly and HealthRoster Daily were each tested with one hour added and one hour removed. Before/after digests for the source timesheet, current TSFIN, invoice header and invoice-line set were identical, and the correction-child count remained zero in every run.

The Weekly validation-only test used an invoice-locked source deliberately. It remained validation-only and non-mutating.

## Code changes

The implementation changes are limited to these responsibilities:

- `_wkimp_bucket_hours_from_policy`: correct PL/pgSQL set-returning behaviour.
- `correction_financials_policy_resolve_v1`: use consistent frozen line evidence when the current Invoice V8 header no longer carries a VAT rate.
- HealthRoster/NHSP Weekly phase 3: reconstruct the financial-position-only carrier from the validated reconciliation unit when required.
- HealthRoster/NHSP Weekly transactional callers: exclude the protected historical root from follow-up TSFIN mutation targets.
- Import Review effective-balance/catalog/lifecycle definitions: terminal schedule and policy authority, complete B/M/A bucket comparison, operation self-exclusion while transitioning, finished-generation classification, and immutable-B re-attestation.
- Direct invoice generation/issue batch dispatch: preserve direct non-batch root operations while continuing to gate manifest members. This was needed for the authorised real TEST issue operations used by the correction-chain proof.

No frontend, Worker JavaScript, Banking Pay, payment, provider, settlement, remittance, Policy X, HealthRoster Daily, or validation-only production function was changed.

## Automated verification

Focused Node test run:

```text
180 tests
180 passed
0 failed
```

Suites:

- `import-authoritative-effective-balance-helper.test.cjs`
- `import-correction-policy-paid-date.test.cjs`
- `invoice-async-direct-root-dispatch.test.cjs`
- `invoice-async-v8-sql-contract.test.js`
- `invoice_async_backend.test.js`

Self-contained database fixture:

```text
18 source-specific fixture assertions
18 passed
0 failed
transaction rolled back
```

Every fixture category ran independently for NHSP and HealthRoster.

Wrangler Tail captured the real validation-only Weekly requests while the stream was open. It observed 1,692 events with no application error marker and no stderr output.

## Deployed-source parity

The following normalized TEST definitions match the current saved canonical source:

| Definition | Normalized MD5 |
|---|---|
| `_import_review_action_catalog_core_v1` | `1cbb12dc19ed487848925e9d2c3a4454` |
| `_import_review_apply_envelope_core_v1` | `2566d14561a7eb6d1c56a8e85834d630` |
| `_import_review_effective_invoice_balance_core_v1` | `eedb2415b30f88a50d696ea0181bcd15` |
| `hr_weekly_apply_transactional` | `d3861523ebb42cb6a763fee014075a4c` |
| `hr_weekly_phase3_apply_adjustment_truth(uuid,text[],uuid)` | `f596a488606ad42129a1b4d452c41fe0` |
| `import_review_correction_generation_transition_v1` | `c55ac38360f6c3952f128f87f3959e27` |
| `nhsp_weekly_apply_transactional` | `fb54838a7e8b2459fd8706b8e6407828` |
| `nhsp_weekly_phase3_apply_adjustment_truth(uuid,text[],uuid)` | `6a793b1cadce5fd3e2e950cd8fe63a71` |
| `timesheet_paid_uninvoiced_rollover_v1` | `10864b3da22bcdb7d4de17358be2fe89` |
| `_wkimp_bucket_hours_from_policy` | `d36ec92bafa1f4612c912c3c343716d7` |
| `correction_financials_policy_resolve_v1` | `f9ae87152edf78a9be902d380c173060` |
| `_invoice_generation_advance_batch` | `14a20172ceb6a6bd502d898fe27c9b33` |
| `_invoice_issue_advance_batch` | `3946cb6832c30d7c986980d0b2449767` |

TEST Supabase was healthy when parity was checked. The SQL changes were already installed in normal TEST. No Worker JavaScript deployment was required.

## Known safe boundary

One pre-existing legacy NHSP data shape contains an uninvoiced correction pair whose net mutable position is non-zero while the effective frozen invoice balance is zero. The current code does not create this shape. Review blocks it atomically with `IMPORT_REVIEW_EFFECTIVE_POSITION_NOT_STANDARD_REPRESENTABLE`; it does not guess, mutate, duplicate, invoice, or pay it. This is recorded as a safe historical-data boundary, not evidence that a current supported route failed.

## Safety and scope

- Environment: TEST only.
- Production accessed or deployed: no.
- Payments executed: no.
- Banking Pay changed: no.
- Policy decision changed: no.
- Invoice and credit artifacts changed by Import Review: no.
- Worker JavaScript deployment required: no.
- Secrets printed or committed: no.
- Raw Wrangler logs committed: no.
- Unrelated dirty invoice work included: no.
