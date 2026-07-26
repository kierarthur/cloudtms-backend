-- Disable plpgsql_check's passive per-function runtime instrumentation.
--
-- TEST encountered intermittent XX000 failures from plpgsql_check 2.7's
-- internal pldbgapi2 call stack while otherwise valid nested PL/pgSQL was
-- executing. This database default affects only new sessions; recycle the
-- database connection pool after installation.
--
-- This statement is intentionally idempotent and safe to rerun. It does not
-- uninstall the extension, alter application data, or change SQL semantics.
alter database postgres
  set plpgsql_check.mode to 'disabled';
