-- Reassert the exact certified Banking Pay release-authority closure after the
-- interrupted historical compatibility replay changed six current routine
-- bodies.  The included closure is already contract-verified and restores
-- only its eight named authorities plus their service-only ACLs.
--
-- Keep this file deliberately additive and later-ordered.  Do not move the
-- correction back into an historical repeatable: doing so would make the
-- release engine replay unrelated legacy authority again.

\set ON_ERROR_STOP on

\ir 29082026_0326_banking_pay_release_authority_repair_v1.sql
