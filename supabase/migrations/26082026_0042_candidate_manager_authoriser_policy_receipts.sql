begin;

create table if not exists public.candidate_manager_authoriser_policy_receipts (
  receipt_id uuid primary key default extensions.gen_random_uuid(),
  entity_kind text not null check (entity_kind in ('CLIENT','CONTRACT')),
  entity_id uuid not null,
  idempotency_key text not null check (char_length(idempotency_key) between 16 and 200),
  semantic_sha256 bytea not null check (octet_length(semantic_sha256)=32),
  response_json jsonb not null check (jsonb_typeof(response_json)='object'),
  actor_user_id uuid not null,
  recorded_at_utc timestamptz not null default transaction_timestamp(),
  unique (entity_kind,entity_id,idempotency_key)
);

create index if not exists candidate_manager_authoriser_policy_receipts_entity_idx
  on public.candidate_manager_authoriser_policy_receipts(entity_kind,entity_id,recorded_at_utc desc);

alter table public.candidate_manager_authoriser_policy_receipts enable row level security;
alter table public.candidate_manager_authoriser_policy_receipts force row level security;

alter table public.candidate_manager_authoriser_policy_receipts owner to postgres;

do $policy$
begin
  execute pg_catalog.format(
    'create policy cloudtms_miget_service_owner_all on public.candidate_manager_authoriser_policy_receipts for all to %I, service_role using (true) with check (true)',
    current_user
  );
end;
$policy$;

revoke all on table public.candidate_manager_authoriser_policy_receipts from public,anon,authenticated,service_role;

commit;
