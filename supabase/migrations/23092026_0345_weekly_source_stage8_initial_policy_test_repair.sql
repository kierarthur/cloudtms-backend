-- One-time repair for the dedicated Stage 8 TEST client created while the
-- first-policy baseline fix was not yet installed. On every other database
-- this migration is an intentional no-op.

\set ON_ERROR_STOP on

begin;

do $repair$
declare
  v_client_id constant uuid := '7ead2058-a6b1-417c-aa71-d05c8b56cbd2';
  v_group_id constant uuid := '81829cdc-f7e2-4f72-b65c-895502f5f207';
  v_membership public.weekly_source_group_clients%rowtype;
  v_policy public.weekly_source_client_policies%rowtype;
  v_membership_count integer;
  v_policy_count integer;
begin
  if not exists(
    select 1
    from public.clients client
    where client.id=v_client_id
      and client.cli_ref='CLI-04170'
      and client.name='CloudTMS Stage 8 NHSP Test Trust'
  ) then
    return;
  end if;

  if not exists(
    select 1
    from public.weekly_source_groups source_group
    where source_group.id=v_group_id
      and source_group.environment='TEST'
      and source_group.source_family='NHSP'
      and source_group.display_name='Stage 8 NHSP Test'
  ) then
    raise exception 'WEEKLY_SOURCE_STAGE8_BASELINE_REPAIR_GROUP_MISMATCH'
      using errcode='55000';
  end if;

  select count(*) into v_membership_count
  from public.weekly_source_group_clients membership
  where membership.client_id=v_client_id;

  select count(*) into v_policy_count
  from public.weekly_source_client_policies policy
  where policy.client_id=v_client_id;

  if v_membership_count<>1 or v_policy_count<>1 then
    raise exception 'WEEKLY_SOURCE_STAGE8_BASELINE_REPAIR_HISTORY_UNSAFE'
      using errcode='55000';
  end if;

  select membership.* into strict v_membership
  from public.weekly_source_group_clients membership
  where membership.client_id=v_client_id
  for update;

  select policy.* into strict v_policy
  from public.weekly_source_client_policies policy
  where policy.client_id=v_client_id
  for update;

  if v_membership.source_group_id<>v_group_id
     or v_policy.source_group_id<>v_group_id
     or v_membership.valid_to is not null
     or v_policy.effective_to is not null
     or v_membership.valid_from<>date '2026-09-14'
     or v_policy.effective_from<>date '2026-09-14'
     or v_membership.created_at_utc<>v_policy.created_at_utc then
    raise exception 'WEEKLY_SOURCE_STAGE8_BASELINE_REPAIR_SOURCE_MISMATCH'
      using errcode='55000';
  end if;

  update public.weekly_source_group_clients
  set valid_from=date '1900-01-01'
  where id=v_membership.id;

  update public.weekly_source_client_policies
  set effective_from=date '1900-01-01'
  where id=v_policy.id;
end
$repair$;

commit;
