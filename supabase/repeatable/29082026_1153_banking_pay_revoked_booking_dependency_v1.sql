-- Banking Pay source-build correction: withdrawn/revoked Timesheet versions are
-- retained evidence, not current economic members of their booking family.
-- Reinstall the complete existing dependency-closure authority with that one
-- current-version fence. Non-revoked members without candidate financial
-- ownership continue to fail closed.

\set ON_ERROR_STOP on

begin;

\ir 04082026_1151_pay_workbench_timesheet_dependency_closure_v2.sql

commit;
