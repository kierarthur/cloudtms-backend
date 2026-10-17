create or replace function public._inv_write_audit(
  p_actor_user_id uuid,
  p_action text,
  p_after_json jsonb,
  p_entity text,
  p_subject_id text,
  p_before_json jsonb default null,
  p_reason text default null,
  p_ip text default null,
  p_user_agent text default null,
  p_correlation_id text default null
)
returns void
language plpgsql
security definer
set search_path = public
as $$
declare
  v_actor_display text := null;
  v_actor_role text := 'system';
begin
  if p_actor_user_id is not null then
    select
      nullif(btrim(coalesce(u.display_name,'')), ''),
      nullif(btrim(coalesce(u.role,'')), '')
    into v_actor_display, v_actor_role
    from public.tms_users u
    where u.id = p_actor_user_id
    limit 1;

    if v_actor_display is null then
      select nullif(btrim(coalesce(u.email,'')), '')
      into v_actor_display
      from public.tms_users u
      where u.id = p_actor_user_id
      limit 1;
    end if;
  end if;

  if v_actor_display is null then
    v_actor_display := 'CloudTMS server';
  end if;

  if v_actor_role is null then
    v_actor_role := 'system';
  end if;

  insert into public.audit_events(
    object_type,
    object_id_text,
    action,
    before_json,
    after_json,
    reason,
    actor_user_id,
    actor_display,
    actor_role_at_time,
    ip,
    user_agent,
    correlation_id
  )
  values (
    coalesce(nullif(btrim(p_entity),''), 'generic'),
    nullif(btrim(p_subject_id), ''),
    coalesce(nullif(btrim(p_action),''), 'EVENT'),
    p_before_json,
    p_after_json,
    p_reason,
    p_actor_user_id,
    v_actor_display,
    v_actor_role,
    nullif(btrim(p_ip), ''),
    nullif(btrim(p_user_agent), ''),
    nullif(btrim(p_correlation_id), '')
  );
end;
$$;