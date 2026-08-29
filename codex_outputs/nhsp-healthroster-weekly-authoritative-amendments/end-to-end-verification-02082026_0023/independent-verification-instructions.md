# Independent verification instructions

## Required verdict

Independently verify the implementation against `implementation-plan-and-function-map.md`, `report.md`, the supplied patch, canonical saved files, current GitHub `test`, and current TEST Supabase definitions.

Do not accept narrative claims without checking source and executable evidence. Equally, do not broaden the review into unrelated systems merely because shared database or invoice infrastructure exists.

## Mandatory scope discipline

This is exclusively the NHSP Weekly and HealthRoster Weekly import-authoritative amendments implementation and the narrowly required direct invoice-dispatch safeguard used to prove the correction chain.

It is not authority to inspect or propose changes to:

- Banking Pay;
- payment calculations, provider processing, CSV execution, settlement, remittances, or Policy X;
- unrelated invoice work currently dirty in the repository;
- invoice totals, VAT policy, credit-note creation, or invoice documents;
- general TSFIN calculation or Worker design;
- frontend or Bulk Authorise;
- HealthRoster Daily mutation;
- HealthRoster Weekly validation-only mutation.

## Function-by-function review required

Review these exact functions and state for each whether it is fully compliant or still requires a demonstrated targeted amendment:

1. `_wkimp_bucket_hours_from_policy`
2. `correction_financials_policy_resolve_v1`
3. `_import_review_effective_invoice_balance_core_v1`
4. `_import_review_action_catalog_core_v1`
5. `_import_review_apply_envelope_core_v1`
6. `import_review_correction_generation_transition_v1`
7. `timesheet_paid_uninvoiced_rollover_v1`
8. `hr_weekly_phase3_apply_adjustment_truth(uuid,text[],uuid)`
9. `nhsp_weekly_phase3_apply_adjustment_truth(uuid,text[],uuid)`
10. `hr_weekly_apply_transactional`
11. `nhsp_weekly_apply_transactional`
12. `private._invoice_generation_advance_batch(jsonb,timestamptz)`
13. `private._invoice_issue_advance_batch(jsonb,timestamptz)`

Confirm separately that HealthRoster Weekly validation-only and HealthRoster Daily remain non-mutating and that no Banking Pay file/function appears in the implementation diff.

## Evidence checks

- Re-run the supplied self-contained runtime schema and fixture SQL in a disposable PostgreSQL database.
- Re-run the focused Node suites.
- Compare every installed TEST target function with its canonical committed definition using a line-ending/whitespace-normalized method.
- Confirm there is one canonical definition per exact signature and that repeatable source is not duplicated by a second competing definition.
- Check that real TEST evidence supports both NHSP and HealthRoster authoritative reversal/replacement, repeated correction, and uninvoiced in-place amendment.
- Check validation-only before/after digests.
- Check that the patch and commit contain no unrelated dirty invoice files.

## Required response format

Return:

1. Overall verdict: `FULLY IMPLEMENTED` or `FURTHER TARGETED WORK REQUIRED`.
2. Scope verdict, explicitly confirming no Banking Pay involvement.
3. Function-by-function verdict for all 13 named functions.
4. Test and deployment-evidence verdict.
5. If and only if a demonstrable defect remains, a highly detailed function-by-function implementation plan listing only the functions that actually require work.

Do not use this handover as an opportunity for a rewrite, a new pathway, architectural tidying, or speculative hardening. A proposed change must identify a concrete failing rule, the exact current code responsible, a reproducible fixture, the smallest correction, and the regression assertions. Functions that do not require changes must be explicitly marked `NO CHANGE` and must not appear in the implementation delta.
