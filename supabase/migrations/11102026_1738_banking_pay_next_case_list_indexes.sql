-- FC-01 maintained read keys only. Original rule-created time remains authority.
-- No money counters, legacy carry-over, public trigger, agency switch or seed data.
\set ON_ERROR_STOP on
begin;
create table private.bpay_next_case_list_key (
 case_id uuid primary key references private.bpay_next_finance_case(id) on delete restrict,
 candidate_id uuid not null references public.candidates(id) on delete restrict,
 created_at_utc timestamptz check(created_at_utc is null or pg_catalog.isfinite(created_at_utc)),
 candidate_sort_name text collate "C" not null check(pg_catalog.octet_length(candidate_sort_name)<=8192),
 candidate_display_name text not null check(pg_catalog.octet_length(candidate_display_name)<=8192),
 candidate_reference text check(pg_catalog.octet_length(candidate_reference)<=8192),
 case_subtype text check(pg_catalog.octet_length(case_subtype)<=64),
 case_type_label text collate "C" not null check(pg_catalog.octet_length(case_type_label)<=64),
 client_id uuid references public.clients(id) on delete restrict,
 client_display_name text check(pg_catalog.octet_length(client_display_name)<=8192),
 client_association text not null check(client_association in ('FROZEN','UNASSOCIATED','UNKNOWN')),
 foreign key(case_id,candidate_id) references private.bpay_next_finance_case(id,candidate_id) on delete restrict,
 check((client_association='FROZEN')=(client_id is not null))
);
create index bpay_next_case_list_created_desc_idx on private.bpay_next_case_list_key
 ((created_at_utc is null),created_at_utc desc nulls last,case_id);
create index bpay_next_case_list_created_asc_idx on private.bpay_next_case_list_key
 ((created_at_utc is null),created_at_utc asc,case_id);
create index bpay_next_case_list_candidate_asc_idx on private.bpay_next_case_list_key
 ((left(candidate_sort_name,512)) collate "C" asc,(created_at_utc is null),created_at_utc desc nulls last,case_id);
create index bpay_next_case_list_candidate_desc_idx on private.bpay_next_case_list_key
 ((left(candidate_sort_name,512)) collate "C" desc,(created_at_utc is null),created_at_utc desc nulls last,case_id);
create index bpay_next_case_list_type_asc_idx on private.bpay_next_case_list_key
 (case_type_label asc,(created_at_utc is null),created_at_utc desc nulls last,case_id);
create index bpay_next_case_list_type_desc_idx on private.bpay_next_case_list_key
 (case_type_label desc,(created_at_utc is null),created_at_utc desc nulls last,case_id);
create index bpay_next_case_list_candidate_scope_idx on private.bpay_next_case_list_key(candidate_id,case_id);
create index bpay_next_case_list_client_scope_idx on private.bpay_next_case_list_key(client_id,case_id) where client_id is not null;
create index bpay_next_case_list_subtype_scope_idx on private.bpay_next_case_list_key(case_subtype,case_id);
alter table private.bpay_next_case_list_key owner to postgres;
alter table private.bpay_next_case_list_key enable row level security;
alter table private.bpay_next_case_list_key force row level security;
revoke all on private.bpay_next_case_list_key from public,anon,authenticated,service_role;
create policy cloudtms_miget_service_owner_all on private.bpay_next_case_list_key to postgres using(true) with check(true);
commit;
