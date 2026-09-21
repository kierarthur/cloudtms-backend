\set ON_ERROR_STOP on

do $verify_weekly_delivery_targets$
declare
  v_relation text;
  v_function regprocedure;
begin
  foreach v_relation in array array[
    'weekly_message_dispatch_targets',
    'weekly_message_target_attempts',
    'weekly_candidate_message_notifications',
    'weekly_message_delivery_failures'
  ] loop
    if pg_catalog.to_regclass('public.'||v_relation) is null then
      raise exception 'missing relation public.%',v_relation;
    end if;
    if not exists(
      select 1 from pg_catalog.pg_class
      where oid=pg_catalog.to_regclass('public.'||v_relation)
        and relrowsecurity and relforcerowsecurity
    ) then
      raise exception 'RLS is not forced for public.%',v_relation;
    end if;
    if pg_catalog.has_table_privilege('anon','public.'||v_relation,'select')
       or pg_catalog.has_table_privilege('authenticated','public.'||v_relation,'select') then
      raise exception 'browser role can read public.%',v_relation;
    end if;
  end loop;

  foreach v_function in array array[
    'public.weekly_source_message_render_due_list_v1(jsonb)'::pg_catalog.regprocedure,
    'public.weekly_source_message_dispatch_claim_v1(jsonb)'::pg_catalog.regprocedure,
    'public.weekly_source_message_targets_register_atomic_v1(jsonb)'::pg_catalog.regprocedure,
    'public.weekly_source_message_dispatch_target_claim_v1(jsonb)'::pg_catalog.regprocedure,
    'public.weekly_source_message_dispatch_target_start_atomic_v1(jsonb)'::pg_catalog.regprocedure,
    'public.weekly_source_message_dispatch_target_result_atomic_v1(jsonb)'::pg_catalog.regprocedure
  ] loop
    if pg_catalog.has_function_privilege('anon',v_function,'execute')
       or pg_catalog.has_function_privilege('authenticated',v_function,'execute') then
      raise exception 'browser execute privilege remains on %',v_function;
    end if;
    if not pg_catalog.has_function_privilege('service_role',v_function,'execute') then
      raise exception 'service execute privilege is missing on %',v_function;
    end if;
  end loop;

  if pg_catalog.pg_get_functiondef(
       'public.weekly_source_message_dispatch_submission_start_atomic_v1(jsonb)'::pg_catalog.regprocedure
     ) not ilike '%WEEKLY_SOURCE_PER_TARGET_DISPATCH_REQUIRED%'
     or pg_catalog.pg_get_functiondef(
       'public.weekly_source_message_dispatch_result_atomic_v1(jsonb)'::pg_catalog.regprocedure
     ) not ilike '%WEEKLY_SOURCE_PER_TARGET_DISPATCH_REQUIRED%' then
    raise exception 'unsafe command-level dispatch remains callable';
  end if;
  if not exists(
    select 1 from pg_catalog.pg_trigger
    where tgrelid='public.weekly_message_intents'::pg_catalog.regclass
      and tgname='weekly_source_candidate_notification_intent' and not tgisinternal
  ) then
    raise exception 'candidate in-app notification trigger is missing';
  end if;
  if pg_catalog.pg_get_functiondef(
       'private.weekly_source_candidate_notification_intent_v1()'::pg_catalog.regprocedure
     ) not ilike '%WEEKLY_SOURCE_CANDIDATE_APP_UNAVAILABLE%'
     or pg_catalog.pg_get_functiondef(
       'public.weekly_source_message_dispatch_target_start_atomic_v1(jsonb)'::pg_catalog.regprocedure
     ) not ilike '%SUBMISSION_STARTED%'
     or pg_catalog.pg_get_functiondef(
       'public.weekly_source_message_dispatch_target_result_atomic_v1(jsonb)'::pg_catalog.regprocedure
     ) not ilike '%PROVIDER_AMBIGUOUS%' then
    raise exception 'candidate availability/submission/ambiguity contract drifted';
  end if;
end;
$verify_weekly_delivery_targets$;
