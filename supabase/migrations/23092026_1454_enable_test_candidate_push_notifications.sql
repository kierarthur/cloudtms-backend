-- TEST activation of the already-installed Candidate push transport.
-- This changes only the existing global Candidate feature flag. It does not
-- register a device, send a notification, or change a payment route.
do $activation$
declare
  v_environment text;
  v_flags jsonb;
begin
  select candidate_app_environment, candidate_app_feature_flags_json
    into v_environment, v_flags
  from public.settings_defaults
  where id = 1
  for update;

  if not found or v_environment is distinct from 'TEST'
     or pg_catalog.jsonb_typeof(v_flags) is distinct from 'object' then
    raise exception using errcode = '55000',
      message = 'TEST_CANDIDATE_PUSH_ACTIVATION_CONTEXT_INVALID';
  end if;

  update public.settings_defaults
  set candidate_app_feature_flags_json = pg_catalog.jsonb_set(
    v_flags, '{push_enabled}', 'true'::jsonb, true
  )
  where id = 1;
end;
$activation$;
