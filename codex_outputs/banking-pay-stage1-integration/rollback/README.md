# Banking Pay Stage 1 rollback pack

This pack was frozen from `test-cloudtms` immediately before the integrated
Stage 1 installation on 4 August 2026.

Rollback order:

1. Disable new Stage 1 claiming at the application boundary and recover any
   in-flight attempt to a terminal state.
2. Restore cancellation replacement functions from
   `installed_before/01_*.sql` through `installed_before/15_*.sql`.
3. Run `cancellation_schema_rollback.sql`.
4. Restore the four compatibility entry-point definitions and ACLs from
   `installed_before/16_*.sql` through `installed_before/19_*.sql`.
5. Run the bounded-scope `rollback.sql` in
   `../../banking-pay-bounded-scope-v12/rollback.sql`.
6. Re-run the installed-catalogue, ACL, trigger, Policy X and public-contract
   verification before resuming the old Worker path.

The 19 function files contain exact pre-install `pg_get_functiondef` output,
owner restoration and execute-grant restoration. The bounded-scope rollback is
the separately frozen exact installed baseline created by that implementation.

This rollback pack is not auto-run and must never be applied to production.
