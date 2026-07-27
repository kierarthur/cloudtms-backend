-- Disable plpgsql_check's passive runtime instrumentation.
--
-- TEST encountered intermittent XX000 failures from plpgsql_check 2.7's
-- internal pldbgapi2 call stack while otherwise valid nested PL/pgSQL was
-- executing. Disabling mode alone is insufficient because the preloaded 2.7
-- library retains its constant-tracing and cursor-leak trackers. These
-- database defaults affect only new sessions; recycle the database connection
-- pool after installation.
--
-- This statement is intentionally idempotent and safe to rerun. It does not
-- uninstall the extension, alter application data, or change SQL semantics.
alter database postgres
  set plpgsql_check.mode to 'disabled';

alter database postgres
  set plpgsql_check.profiler to 'off';

alter database postgres
  set plpgsql_check.tracer to 'off';

alter database postgres
  set plpgsql_check.constants_tracing to 'off';

alter database postgres
  set plpgsql_check.cursors_leaks to 'off';

alter database postgres
  set plpgsql_check.strict_cursors_leaks to 'off';

alter database postgres
  set plpgsql_check.fatal_errors to 'off';
