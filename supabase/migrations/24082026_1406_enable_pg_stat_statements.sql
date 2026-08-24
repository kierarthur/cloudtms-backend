-- One-time CloudTMS operational extension migration: enable_pg_stat_statements
-- Miget preloads the extension library, but restored databases do not
-- necessarily retain the database-local CREATE EXTENSION registration.
-- This is provider-neutral, idempotent and does not alter application data,
-- business logic, RLS, roles, payment behaviour, or CloudTMS Policy X.

\set ON_ERROR_STOP on

begin;

create extension if not exists pg_stat_statements with schema extensions;

commit;
