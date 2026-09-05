-- Final current-authority closure for the active legacy extras repeatable.
--
-- The legacy bundle still owns current Timesheet-summary behaviour, so a
-- legitimate edit makes that whole repeatable pending during UPGRADE. Two
-- definitions inside it have newer owners. Always replay those exact owners
-- immediately after the bundle so an UPGRADE and clean NEW installation end
-- with identical routine definitions, metadata and ACLs.
--
-- This file owns no business rule and contains no replacement function body.
-- Its recursive release hash binds all three sources and makes an interrupted
-- replay safely repeatable by the release engine.

\set ON_ERROR_STOP on

\ir 19012026_extras.sql
\ir 19072026_1722_audit_events_list.sql
\ir 27082026_1946_contract_override_inheritance_v2.sql
