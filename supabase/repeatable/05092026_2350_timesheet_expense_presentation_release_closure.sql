-- Applying the historical Timesheet summary repeatable on an existing database
-- must not leave its older copies of unrelated Audit and Contract authorities
-- installed. Reassert their current repository-owned definitions after the
-- Timesheet expense-presentation change.

\set ON_ERROR_STOP on

\ir 19072026_1722_audit_events_list.sql
\ir 27082026_1946_contract_override_inheritance_v2.sql
