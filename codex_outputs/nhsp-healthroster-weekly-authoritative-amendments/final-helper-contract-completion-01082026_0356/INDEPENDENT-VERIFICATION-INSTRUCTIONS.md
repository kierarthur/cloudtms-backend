# Independent verification instruction

Verify backend commit `cffab09d284a690c5130b6821bef53a50f1dbcdc` and its installed TEST helper against `report.md`.

This is a tightly scoped Import Review correction. The sole production function changed is:

```text
public._import_review_effective_invoice_balance_core_v1(uuid,jsonb,integer,integer,integer,integer)
```

Do not broaden the review into Banking Pay, invoice writers, credit writers, TSFIN, frontend, Workers, phase-3 functions, Weekly callers, HealthRoster Daily or HealthRoster validation-only.

Re-run the supplied source tests and PostgreSQL rollback fixtures. Re-query live TEST parity. Review every implemented contract described in `report.md`.

If anything remains defective, return a function-by-function implementation plan containing only functions for which you have direct evidence of a remaining defect. For each function give the exact source defect, failing executable scenario, narrow code amendment and required regression test. Do not recommend a broad rewrite.

