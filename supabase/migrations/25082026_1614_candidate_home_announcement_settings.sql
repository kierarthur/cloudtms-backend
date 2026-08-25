begin;

alter table public.settings_defaults
  add column if not exists candidate_home_announcement_text text not null default '',
  add column if not exists candidate_home_announcement_version bigint not null default 1,
  add column if not exists candidate_home_announcement_sha256 bytea,
  add column if not exists candidate_home_announcement_updated_at_utc timestamptz
    not null default pg_catalog.transaction_timestamp(),
  add column if not exists candidate_home_announcement_updated_by_hmac bytea;

update public.settings_defaults
set candidate_home_announcement_sha256 = extensions.digest(
  pg_catalog.convert_to(candidate_home_announcement_text,'UTF8'),'sha256'
)
where candidate_home_announcement_sha256 is null;

alter table public.settings_defaults
  alter column candidate_home_announcement_sha256 set not null;

do $migration$
begin
  if not exists (
    select 1 from pg_catalog.pg_constraint
    where conname='settings_defaults_candidate_home_announcement_version_chk'
      and conrelid='public.settings_defaults'::pg_catalog.regclass
  ) then
    alter table public.settings_defaults
      add constraint settings_defaults_candidate_home_announcement_version_chk
      check (candidate_home_announcement_version>=1);
  end if;
  if not exists (
    select 1 from pg_catalog.pg_constraint
    where conname='settings_defaults_candidate_home_announcement_text_chk'
      and conrelid='public.settings_defaults'::pg_catalog.regclass
  ) then
    alter table public.settings_defaults
      add constraint settings_defaults_candidate_home_announcement_text_chk
      check (pg_catalog.char_length(candidate_home_announcement_text)<=600);
  end if;
  if not exists (
    select 1 from pg_catalog.pg_constraint
    where conname='settings_defaults_candidate_home_announcement_sha256_chk'
      and conrelid='public.settings_defaults'::pg_catalog.regclass
  ) then
    alter table public.settings_defaults
      add constraint settings_defaults_candidate_home_announcement_sha256_chk
      check (pg_catalog.octet_length(candidate_home_announcement_sha256)=32);
  end if;
  if not exists (
    select 1 from pg_catalog.pg_constraint
    where conname='settings_defaults_candidate_home_announcement_actor_chk'
      and conrelid='public.settings_defaults'::pg_catalog.regclass
  ) then
    alter table public.settings_defaults
      add constraint settings_defaults_candidate_home_announcement_actor_chk
      check (
        candidate_home_announcement_updated_by_hmac is null
        or pg_catalog.octet_length(candidate_home_announcement_updated_by_hmac)=32
      );
  end if;
end;
$migration$;

create table if not exists public.candidate_home_announcement_versions (
  version bigint primary key check (version>=1),
  announcement_text text not null check (pg_catalog.char_length(announcement_text)<=600),
  semantic_sha256 bytea not null check (pg_catalog.octet_length(semantic_sha256)=32),
  actor_identity_hmac bytea check (
    actor_identity_hmac is null or pg_catalog.octet_length(actor_identity_hmac)=32
  ),
  idempotency_key text not null unique check (
    pg_catalog.char_length(pg_catalog.btrim(idempotency_key)) between 1 and 200
  ),
  reason_code text not null check (reason_code in ('INSTALL_DEFAULT','OFFICE_SAVE','OFFICE_RESET')),
  recorded_at_utc timestamptz not null default pg_catalog.transaction_timestamp()
);

insert into public.candidate_home_announcement_versions(
  version,announcement_text,semantic_sha256,actor_identity_hmac,
  idempotency_key,reason_code,recorded_at_utc
)
select s.candidate_home_announcement_version,s.candidate_home_announcement_text,
  s.candidate_home_announcement_sha256,null,
  'candidate-home-announcement-install-default-v1','INSTALL_DEFAULT',
  s.candidate_home_announcement_updated_at_utc
from public.settings_defaults s
where s.id=1
on conflict do nothing;

alter table public.candidate_home_announcement_versions enable row level security;
alter table public.candidate_home_announcement_versions force row level security;

do $policy$
begin
  execute pg_catalog.format(
    'create policy cloudtms_miget_service_owner_all on public.candidate_home_announcement_versions for all to %I, service_role using (true) with check (true)',
    current_user
  );
end;
$policy$;

revoke all on table public.candidate_home_announcement_versions
  from public,anon,authenticated,service_role;

commit;
