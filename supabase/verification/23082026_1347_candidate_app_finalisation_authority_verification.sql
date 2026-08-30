-- Read-only verification for the pre-app adaptive-break, returned-paper QR
-- proof, Candidate identity mail defaults, and service-only authority seams.

do $candidate_app_finalisation_authority_verification$
declare
  v_public_count integer;
  v_browser_executable integer;
  v_service_missing integer;
  v_shape_mismatch integer;
  v_private_helper_executable integer;
  v_initial_template_mismatch integer;
begin
  with expected(signature,expected_config,expected_volatility) as (
    values
      ('candidate_app_timesheet_detail_v2(uuid,text,uuid,uuid,uuid,timestamp with time zone)',
        array['search_path=pg_catalog, public, private, extensions, pg_temp']::text[],'s'::"char"),
      ('candidate_break_entry_context_get_v1(uuid,text,uuid,timestamp with time zone)',
        array['search_path=pg_catalog, public, private, extensions, pg_temp']::text[],'s'::"char"),
      ('candidate_paper_return_proof_validate_v1(uuid,text,uuid,integer,text,text,text,text,timestamp with time zone)',
        array['search_path=pg_catalog, public, private, extensions, pg_temp']::text[],'v'::"char")
  ), target as (
    select
      e.signature,
      p.oid,
      p.prosecdef,
      p.provolatile,
      p.proconfig,
      owner_role.rolname as owner_name,
      pg_catalog.has_function_privilege('service_role',p.oid,'EXECUTE') as service_execute,
      pg_catalog.has_function_privilege('anon',p.oid,'EXECUTE') as anon_execute,
      pg_catalog.has_function_privilege('authenticated',p.oid,'EXECUTE') as authenticated_execute,
      e.expected_config,
      e.expected_volatility
    from expected e
    left join pg_catalog.pg_proc p
      on p.oid=pg_catalog.to_regprocedure('public.'||e.signature)
    left join pg_catalog.pg_roles owner_role on owner_role.oid=p.proowner
  )
  select
    count(*) filter(where oid is not null),
    count(*) filter(where oid is null or not service_execute),
    count(*) filter(where oid is not null and (anon_execute or authenticated_execute)),
    count(*) filter(where oid is null or not prosecdef or provolatile<>expected_volatility
      or owner_name<>current_user or proconfig is distinct from expected_config)
  into v_public_count,v_service_missing,v_browser_executable,v_shape_mismatch
  from target;

  select count(*) into v_private_helper_executable
  from pg_catalog.pg_proc p
  where p.oid in (
      pg_catalog.to_regprocedure('private._timesheet_break_entry_precedence_v1(uuid,uuid,date)'),
      pg_catalog.to_regprocedure('private._candidate_break_entry_context_core_v1(uuid,uuid,date,jsonb)')
    )
    and (
      pg_catalog.has_function_privilege('service_role',p.oid,'EXECUTE')
      or pg_catalog.has_function_privilege('anon',p.oid,'EXECUTE')
      or pg_catalog.has_function_privilege('authenticated',p.oid,'EXECUTE')
    );

  select count(*) into v_initial_template_mismatch
  from public.settings_defaults settings
  cross join lateral (values
    (settings.candidate_manager_email_templates_json#>>'{TIMESHEET,INITIAL,body_text}'),
    (settings.candidate_manager_email_templates_json#>>'{EXPENSE_CLAIM,INITIAL,body_text}')
  ) initial_template(body_text)
  where initial_template.body_text is distinct from
    'The below candidate has submitted a timesheet or expenses for approval. You can approve or refuse the complete submission using the secure link below.';

  if v_public_count<>3 or v_service_missing<>0 or v_browser_executable<>0
     or v_shape_mismatch<>0 or v_private_helper_executable<>0
     or v_initial_template_mismatch<>0 then
    raise exception
      'CANDIDATE_APP_FINALISATION_AUTHORITY_VERIFICATION_FAILED:public=% service_missing=% browser=% shape=% private_helper=% template=%',
      v_public_count,v_service_missing,v_browser_executable,v_shape_mismatch,
      v_private_helper_executable,v_initial_template_mismatch;
  end if;
end
$candidate_app_finalisation_authority_verification$;

select 'PASS'::text as candidate_app_finalisation_authority_verification;
