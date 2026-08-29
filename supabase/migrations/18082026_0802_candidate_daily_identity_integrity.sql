begin;

do $preflight$
declare
  v_duplicate_active_cid1_groups integer:=0;
  v_duplicate_source_hmac_groups integer:=0;
begin
  select count(*)
  into v_duplicate_active_cid1_groups
  from (
    select upper(btrim(c.key_norm))
    from public.candidates c
    where c.active is true
      and c.key_norm is not null
      and upper(btrim(c.key_norm)) ~ '^CID1-[0-9A-HJKMNP-TV-Z]{5,160}$'
    group by upper(btrim(c.key_norm))
    having count(*)>1
  ) duplicates;

  if v_duplicate_active_cid1_groups<>0 then
    raise exception using
      errcode='23505',
      message='CANDIDATE_DAILY_CID1_NORMALIZED_DUPLICATE_PRECHECK_FAILED';
  end if;

  select count(*)
  into v_duplicate_source_hmac_groups
  from (
    select l.environment,l.source_system,l.hmac_key_version,l.identifier_hmac
    from private.candidate_daily_source_links l
    group by l.environment,l.source_system,l.hmac_key_version,l.identifier_hmac
    having count(*)>1
  ) duplicates;

  if v_duplicate_source_hmac_groups<>0 then
    raise exception using
      errcode='23505',
      message='CANDIDATE_DAILY_SOURCE_HMAC_HISTORY_DUPLICATE_PRECHECK_FAILED';
  end if;
end;
$preflight$;

create unique index if not exists candidates_active_normalized_cid1_uq
  on public.candidates((upper(btrim(key_norm))))
  where active is true
    and key_norm is not null
    and upper(btrim(key_norm)) ~ '^CID1-[0-9A-HJKMNP-TV-Z]{5,160}$';

create unique index if not exists candidate_daily_source_links_history_hmac_uq
  on private.candidate_daily_source_links(
    environment,source_system,hmac_key_version,identifier_hmac
  );

create or replace function private._candidate_daily_source_link_identity_history_guard_v1()
returns trigger
language plpgsql
security definer
set search_path=''
as $function$
declare
  v_existing_count integer:=0;
begin
  if new.environment not in ('TEST','LIVE')
     or new.source_system<>'GOOGLE_CREDENTIALLY_PUBLIC_ID'
     or new.hmac_key_version<=0
     or new.identifier_hmac !~ '^[a-f0-9]{64}$' then
    raise exception using errcode='22023',message='VALIDATION_FAILED';
  end if;

  perform pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtextextended(
      new.environment||':SOURCE:'||new.hmac_key_version::text||':'||new.identifier_hmac,
      0
    )
  );

  if tg_op='UPDATE' and (
    new.environment is distinct from old.environment
    or new.source_system is distinct from old.source_system
    or new.hmac_key_version is distinct from old.hmac_key_version
    or new.identifier_hmac is distinct from old.identifier_hmac
    or new.candidate_id is distinct from old.candidate_id
  ) then
    raise exception using errcode='23505',message='IDENTITY_LINK_CONFLICT';
  end if;

  select count(*)
  into v_existing_count
  from private.candidate_daily_source_links l
  where l.environment=new.environment
    and l.source_system=new.source_system
    and l.hmac_key_version=new.hmac_key_version
    and l.identifier_hmac=new.identifier_hmac
    and (tg_op='INSERT' or l.link_id<>new.link_id);

  if v_existing_count<>0 then
    raise exception using errcode='23505',message='IDENTITY_LINK_CONFLICT';
  end if;

  return new;
end;
$function$;

revoke all on function private._candidate_daily_source_link_identity_history_guard_v1()
  from public;

do $acl$
begin
  if exists(select 1 from pg_roles where rolname='anon') then
    revoke all on function private._candidate_daily_source_link_identity_history_guard_v1()
      from anon;
  end if;
  if exists(select 1 from pg_roles where rolname='authenticated') then
    revoke all on function private._candidate_daily_source_link_identity_history_guard_v1()
      from authenticated;
  end if;
  if exists(select 1 from pg_roles where rolname='service_role') then
    revoke all on function private._candidate_daily_source_link_identity_history_guard_v1()
      from service_role;
  end if;
end;
$acl$;

drop trigger if exists candidate_daily_source_link_identity_history_guard_v1
  on private.candidate_daily_source_links;

create trigger candidate_daily_source_link_identity_history_guard_v1
before insert or update of environment,source_system,hmac_key_version,identifier_hmac,candidate_id
on private.candidate_daily_source_links
for each row
execute function private._candidate_daily_source_link_identity_history_guard_v1();

commit;
