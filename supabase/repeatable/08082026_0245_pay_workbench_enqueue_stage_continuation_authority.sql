-- Canonical late authority for the Banking Pay stage-continuation owner.
--
-- The historical omnibus repeatable also contains an obsolete copy of this
-- function. Re-include the focused, current definition after that omnibus so a
-- changed deployment installs the linkage/fencing contract deliberately. The
-- catalogue verifier owns the installed identity and will fail a later deploy
-- if another repeatable overwrites it.
-- Reasserted after the final removal of the monolith's obsolete prepare drop.
\ir 04082026_1219_pay_workbench_enqueue_stage_continuation.sql
