begin;

create table if not exists public.candidate_daily_information_settings (
  id smallint primary key check (id=1),
  hospital_addresses jsonb not null default '[]'::jsonb
    check (pg_catalog.jsonb_typeof(hospital_addresses)='array'),
  accommodation_contacts jsonb not null default '[]'::jsonb
    check (pg_catalog.jsonb_typeof(accommodation_contacts)='array'),
  version bigint not null default 1 check (version>=1),
  semantic_sha256 bytea not null check (pg_catalog.octet_length(semantic_sha256)=32),
  updated_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  updated_by_hmac bytea check (
    updated_by_hmac is null or pg_catalog.octet_length(updated_by_hmac)=32
  )
);

insert into public.candidate_daily_information_settings(
  id,hospital_addresses,accommodation_contacts,version,semantic_sha256,
  updated_at_utc,updated_by_hmac
) values (
  1,'[]'::jsonb,'[]'::jsonb,1,
  extensions.digest(
    pg_catalog.convert_to(
      pg_catalog.jsonb_build_object(
        'accommodation_contacts','[]'::jsonb,
        'hospital_addresses','[]'::jsonb
      )::text,
      'UTF8'
    ),
    'sha256'
  ),
  pg_catalog.transaction_timestamp(),null
)
on conflict (id) do nothing;

create table if not exists public.candidate_daily_information_versions (
  version bigint primary key check (version>=1),
  payload jsonb not null check (pg_catalog.jsonb_typeof(payload)='object'),
  semantic_sha256 bytea not null check (pg_catalog.octet_length(semantic_sha256)=32),
  actor_identity_hmac bytea check (
    actor_identity_hmac is null or pg_catalog.octet_length(actor_identity_hmac)=32
  ),
  idempotency_key text not null unique check (
    pg_catalog.char_length(pg_catalog.btrim(idempotency_key)) between 16 and 200
  ),
  reason_code text not null check (reason_code in ('INSTALL_DEFAULT','OFFICE_SAVE')),
  recorded_at_utc timestamptz not null default pg_catalog.transaction_timestamp()
);

insert into public.candidate_daily_information_versions(
  version,payload,semantic_sha256,actor_identity_hmac,idempotency_key,
  reason_code,recorded_at_utc
)
select s.version,
  pg_catalog.jsonb_build_object(
    'accommodation_contacts',s.accommodation_contacts,
    'hospital_addresses',s.hospital_addresses
  ),
  s.semantic_sha256,null,'candidate-daily-information-install-default-v1',
  'INSTALL_DEFAULT',s.updated_at_utc
from public.candidate_daily_information_settings s
where s.id=1
on conflict do nothing;

alter table public.candidate_daily_information_settings enable row level security;
alter table public.candidate_daily_information_settings force row level security;
alter table public.candidate_daily_information_versions enable row level security;
alter table public.candidate_daily_information_versions force row level security;

do $policy$
begin
  execute pg_catalog.format(
    'create policy cloudtms_miget_service_owner_all on public.candidate_daily_information_settings for all to %I, service_role using (true) with check (true)',
    current_user
  );
  execute pg_catalog.format(
    'create policy cloudtms_miget_service_owner_all on public.candidate_daily_information_versions for all to %I, service_role using (true) with check (true)',
    current_user
  );
end;
$policy$;

revoke all on table public.candidate_daily_information_settings
  from public,anon,authenticated,service_role;
revoke all on table public.candidate_daily_information_versions
  from public,anon,authenticated,service_role;

commit;
