# Banking Pay Draft V8 — TEST rollback plan

Rollback must remain forward-safe and must not delete financial, audit, certificate or operation evidence.

## Before release

- Record the exact active TEST Worker version and the exact latest `VERIFIED` Miget release.
- Confirm the pushed commit is the reviewed candidate and that the database workflow is source-gated to it.
- Do not start a real Draft, payment, provider submission, settlement or remittance as part of release verification.

## If the Worker deployment fails before the database upgrade

- Keep the currently active TEST Worker version.
- Do not run the database upgrade until the Worker build failure is understood.
- No database rollback is required because Miget has not changed.

## If the database upgrade fails

- The protected engine records the failed release and stops.
- Do not delete rows, migrations, relations, certificates, operations or audit evidence.
- Keep V8 unavailable through the existing database-contract capability gate.
- Repair the exact failed authority in a new reviewed successor commit and rerun the complete release checks. Do not hot-patch Miget.

## If the database is verified but the Worker is unhealthy

- Return the TEST Worker to the exact pre-release Worker version/source commit.
- Leave the additive V8 database objects and the two failed-payment compatibility corrections installed but inactive. They are backward-compatible and removing them would reintroduce known failures.
- Verify the prior Worker is healthy and its V1 route remains compatible with the additive schema.

## If a post-release read-only audit finds drift

- Disable use of the new route by returning to the prior Worker source; do not rewrite or remove financial rows.
- Preserve every operation/certificate/audit record indefinitely.
- Produce one complete successor release containing the narrow corrected authority, generated contract, verifiers and evidence. Apply it through the protected Miget TEST workflow.

## Data and policy invariants during rollback

- Never delete or rewrite executed payment, provider, settlement, remittance, reservation, cancellation or frozen-lineage evidence.
- Never revive a cancelled/failed batch or release/reserve money merely to make a test pass.
- Never restore the old 66-pair PostgreSQL JSON constructor or remove the two legitimate terminal no-money movement values.
- Never widen a timeout or lock budget.
- Never alter PAYE/Umbrella, gross/net, VAT, payment-method, routing, approval, headroom or cancellation policy.

The pre-candidate shared source is commit `a8c88e6393629a0ca25e0072c7310ea37906f0d5`. That identity is a source reference only; any rollback deployment must first prove the exact active pre-release Worker version and database compatibility rather than blindly resetting Git history.
