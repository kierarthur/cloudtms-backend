-- Install the dedicated, non-login CloudTMS user used only for Candidate
-- canonical finalisation audit fields. Existing configured authority wins:
-- this migration never replaces a non-null system actor.

\set ON_ERROR_STOP on

begin;

do $migration$
declare
  v_actor_id uuid;
  v_candidate_count integer;
  v_candidate public.tms_users%rowtype;
  v_system_email constant text := 'candidate-app-system@cloudtms.invalid';
  v_password_sentinel constant text := '!cloudtms-system-actor-no-login-v1!';
begin
  select sd.candidate_app_system_actor_user_id
  into v_actor_id
  from public.settings_defaults sd
  where sd.id = 1
  for update;

  if not found then
    raise exception 'CANDIDATE_SYSTEM_ACTOR_SETTINGS_MISSING';
  end if;

  if v_actor_id is not null then
    if not exists (
      select 1
      from public.tms_users u
      where u.id = v_actor_id
    ) then
      raise exception 'CANDIDATE_SYSTEM_ACTOR_REFERENCE_INVALID';
    end if;
    return;
  end if;

  select count(*)::integer
  into v_candidate_count
  from public.tms_users u
  where lower(btrim(u.email)) = v_system_email;

  if v_candidate_count > 1 then
    raise exception 'CANDIDATE_SYSTEM_ACTOR_IDENTITY_AMBIGUOUS';
  end if;

  if v_candidate_count = 1 then
    select u.*
    into strict v_candidate
    from public.tms_users u
    where lower(btrim(u.email)) = v_system_email;

    if v_candidate.email <> v_system_email
       or v_candidate.role <> 'user'
       or v_candidate.is_active
       or v_candidate.password_hash <> v_password_sentinel
       or v_candidate.payment_authoriser
       or v_candidate.payment_golden_key
       or coalesce(v_candidate.display_name, '') <> 'MyTMS Candidate system' then
      raise exception 'CANDIDATE_SYSTEM_ACTOR_IDENTITY_UNSAFE';
    end if;

    v_actor_id := v_candidate.id;
  else
    insert into public.tms_users(
      email,
      role,
      is_active,
      password_hash,
      display_name,
      payment_authoriser,
      payment_golden_key
    ) values (
      v_system_email,
      'user',
      false,
      v_password_sentinel,
      'MyTMS Candidate system',
      false,
      false
    )
    returning id into v_actor_id;
  end if;

  update public.settings_defaults sd
  set candidate_app_system_actor_user_id = v_actor_id
  where sd.id = 1
    and sd.candidate_app_system_actor_user_id is null;

  if not found then
    raise exception 'CANDIDATE_SYSTEM_ACTOR_CONFIGURATION_RACE';
  end if;
end;
$migration$;

commit;
