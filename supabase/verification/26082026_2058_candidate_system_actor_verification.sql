-- Verify Candidate canonical-finalisation audit authority without exposing it
-- to browser roles or requiring an interactive Office user.
\set ON_ERROR_STOP on

do $verification$
declare
  v_actor public.tms_users%rowtype;
begin
  select u.*
  into v_actor
  from public.settings_defaults sd
  join public.tms_users u
    on u.id = sd.candidate_app_system_actor_user_id
  where sd.id = 1;

  if not found then
    raise exception 'CANDIDATE_SYSTEM_ACTOR_NOT_CONFIGURED';
  end if;

  if lower(btrim(v_actor.email)) = 'candidate-app-system@cloudtms.invalid' then
    if v_actor.email <> 'candidate-app-system@cloudtms.invalid'
       or v_actor.role <> 'user'
       or v_actor.is_active
       or v_actor.password_hash <> '!cloudtms-system-actor-no-login-v1!'
       or v_actor.payment_authoriser
       or v_actor.payment_golden_key
       or coalesce(v_actor.display_name, '') <> 'MyTMS Candidate system' then
      raise exception 'CANDIDATE_SYSTEM_ACTOR_IDENTITY_UNSAFE';
    end if;
  end if;
end;
$verification$;
