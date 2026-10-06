-- Rollback-contained verification support only. Never install as a repeatable.
-- Existing rows are not an empty-database precondition and may not be mutated.
-- Use the established key/counter capture, not a replacement Banking owner.
\ir 22092026_1850_source_fixture_capture.sql

do $watch_source_workbench_fixture$
declare v_relation regclass;
begin
  foreach v_relation in array array[
    'public.banking_pay_workbench_candidate_delta_projection_runs'::regclass,
    'public.banking_pay_workbench_candidate_line_work'::regclass,
    'public.banking_pay_workbench_candidate_source_lines'::regclass,
    'public.banking_pay_workbench_case_resolution_carry_registrations'::regclass,
    'public.banking_pay_workbench_jobs'::regclass,
    'public.banking_pay_workbench_preview_rows'::regclass,
    'public.banking_pay_workbench_selection_carry_registrations'::regclass,
    'public.banking_pay_workbench_session_candidate_state'::regclass,
    'public.banking_pay_workbench_session_case_resolutions'::regclass,
    'public.banking_pay_workbench_session_overrides'::regclass,
    'public.banking_pay_workbench_session_scope'::regclass,
    'public.banking_pay_workbench_sessions'::regclass
  ] loop
    perform pg_temp.ws_verify_watch(v_relation);
  end loop;
end $watch_source_workbench_fixture$;

-- Hash each complete row first; aggregate only fixed-size digests, not customer
-- payloads. Include count and multiplicity. No row contents leave this session.
create or replace function pg_temp.ws_verify_workbench_fingerprint()
returns jsonb language plpgsql set search_path='' as $f$
declare v_relation text; v_result jsonb := '{}'::jsonb; v_fingerprint jsonb;
begin
  foreach v_relation in array array[
    'public.banking_pay_workbench_candidate_delta_projection_runs',
    'public.banking_pay_workbench_candidate_line_work',
    'public.banking_pay_workbench_candidate_source_lines',
    'public.banking_pay_workbench_case_resolution_carry_registrations',
    'public.banking_pay_workbench_preview_rows',
    'public.banking_pay_workbench_selection_carry_registrations',
    'public.banking_pay_workbench_session_candidate_state',
    'public.banking_pay_workbench_session_case_resolutions',
    'public.banking_pay_workbench_session_overrides',
    'public.banking_pay_workbench_session_scope',
    'public.banking_pay_workbench_sessions'
  ] loop
    execute pg_catalog.format($q$
      select pg_catalog.jsonb_build_object('count',pg_catalog.count(*),'sha256',
        pg_catalog.encode(extensions.digest(coalesce(
          pg_catalog.string_agg(row_digest,'' order by row_digest),''),'sha256'),'hex'))
      from (select pg_catalog.encode(extensions.digest(
        pg_catalog.to_jsonb(x)::text,'sha256'),'hex') as row_digest from %s x) rows
    $q$,v_relation) into v_fingerprint;
    v_result := v_result || pg_catalog.jsonb_build_object(v_relation,v_fingerprint);
  end loop;
  return v_result;
end $f$;

create or replace function pg_temp.ws_verify_other_jobs_fingerprint(p_owned uuid[])
returns jsonb language sql set search_path='' as $f$
  select pg_catalog.jsonb_build_object('count',pg_catalog.count(*),'sha256',
    pg_catalog.encode(extensions.digest(coalesce(
      pg_catalog.string_agg(row_digest,'' order by row_digest),''),'sha256'),'hex'))
  from (select pg_catalog.encode(extensions.digest(
    pg_catalog.to_jsonb(j)::text,'sha256'),'hex') as row_digest
    from public.banking_pay_workbench_jobs j
    where not(j.id=any(p_owned))) rows;
$f$;
