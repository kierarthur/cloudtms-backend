-- LEGACY_UPGRADE dependency preload for the exact current diagnostic helper.
-- Historical migrations can remove this helper before early Banking owners
-- rerun and fire current triggers that call it. Install the exact reviewed
-- definition before the Banking bootstrap and again before repeatable
-- convergence. The function is inert unless the existing temp_log setting is
-- enabled; this file calls it zero times and changes no application row.

CREATE OR REPLACE FUNCTION public._temp_diag_log(p_action text, p_object_type text, p_object_id_text text DEFAULT NULL::text, p_payload_json jsonb DEFAULT '{}'::jsonb)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
DECLARE
  v_enabled boolean := false;
  v_payload jsonb := '{}'::jsonb;
BEGIN
  BEGIN
    SELECT COALESCE(sd.temp_log, false)
      INTO v_enabled
    FROM public.settings_defaults AS sd
    ORDER BY sd.id
    LIMIT 1;
  EXCEPTION
    WHEN undefined_table OR undefined_column THEN
      RETURN;
    WHEN OTHERS THEN
      RETURN;
  END;

  IF COALESCE(v_enabled, false) IS NOT TRUE THEN
    RETURN;
  END IF;

  v_payload := COALESCE(p_payload_json, '{}'::jsonb);

  IF LENGTH(v_payload::text) > 12000 THEN
    v_payload := jsonb_build_object(
      'truncated', true,
      'original_length', LENGTH(COALESCE(p_payload_json, '{}'::jsonb)::text)
    );
  END IF;

  BEGIN
    INSERT INTO public.audit_events (
      actor_user_id,
      actor_display,
      actor_role_at_time,
      object_type,
      object_id_text,
      action,
      before_json,
      after_json,
      reason,
      ip,
      user_agent,
      correlation_id
    )
    VALUES (
      NULL::uuid,
      NULL::text,
      NULL::text,
      COALESCE(NULLIF(BTRIM(p_object_type), ''), 'TEMP_DIAG'),
      NULLIF(BTRIM(COALESCE(p_object_id_text, '')), ''),
      COALESCE(NULLIF(BTRIM(p_action), ''), 'TEMP_DIAG_STAGE'),
      NULL::jsonb,
      v_payload,
      'TEMP_DIAG',
      NULL::text,
      NULL::text,
      NULL::text
    );
  EXCEPTION
    WHEN OTHERS THEN
      RETURN;
  END;
END;
$function$;

revoke all privileges on function public._temp_diag_log(text,text,text,jsonb)
  from public, anon, authenticated, service_role, authenticator, supabase_admin;
grant execute on function public._temp_diag_log(text,text,text,jsonb)
  to current_user, service_role;

do $verify_temp_diag_log_preload$
declare
  v_signature regprocedure := to_regprocedure('public._temp_diag_log(text,text,text,jsonb)');
begin
  if v_signature is null
     or not has_function_privilege('service_role',v_signature,'EXECUTE')
     or has_function_privilege('anon',v_signature,'EXECUTE')
     or has_function_privilege('authenticated',v_signature,'EXECUTE') then
    raise exception 'LEGACY_TEMP_DIAG_LOG_PRELOAD_VERIFICATION_FAILED';
  end if;
end
$verify_temp_diag_log_preload$;
