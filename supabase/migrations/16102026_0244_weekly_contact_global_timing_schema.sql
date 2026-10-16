-- V6 CONTACT_PAUSE_SPEC: additive global timing policy and immutable save receipts.
-- Existing singleton/clocks/automatic actor retained; no dispatch or external effect.
\set ON_ERROR_STOP on
begin;
create or replace function private.weekly_source_manager_interval_valid_v1(p_value integer,p_unit text,p_zero boolean)
returns boolean language sql immutable security definer set search_path='pg_catalog','pg_temp'
as $function$
 select coalesce(p_value>=case when p_zero then 0 else 1 end and
  ((p_unit='HOURS' and p_value<=8784) or (p_unit in('CALENDAR_DAYS','CLIENT_WORKING_DAYS') and p_value<=366)),false)
$function$;

create or replace function private.weekly_source_manager_followups_valid_v1(p_vector jsonb)
returns boolean language plpgsql immutable security definer set search_path='pg_catalog','private','pg_temp'
as $function$
declare row jsonb;n integer:=0;
begin
 if jsonb_typeof(p_vector) is distinct from 'array' or jsonb_array_length(p_vector)>10 then return false;end if;
 for row in select value from jsonb_array_elements(p_vector) loop
  n:=n+1;
  if jsonb_typeof(row) is distinct from 'object' or not(row ?& array['sequence','interval_value','interval_unit','mode'])
   or row-array['sequence','interval_value','interval_unit','mode']<>'{}'::jsonb
   or jsonb_typeof(row->'sequence') is distinct from 'number' or row->>'sequence' !~ '^[0-9]+$'
   or jsonb_typeof(row->'interval_value') is distinct from 'number' or row->>'interval_value' !~ '^[0-9]+$'
   or length(row->>'interval_value')>4 or row->>'sequence'<>n::text
   or not coalesce(row->>'mode' in('MANUAL','AUTOMATIC'),false)
   or jsonb_typeof(row->'interval_unit') is distinct from 'string' then return false;end if;
  if not private.weekly_source_manager_interval_valid_v1((row->>'interval_value')::integer,row->>'interval_unit',false) then return false;end if;
 end loop;
 return true;
end;
$function$;

alter function private.weekly_source_manager_interval_valid_v1(integer,text,boolean) owner to postgres;
revoke all on function private.weekly_source_manager_interval_valid_v1(integer,text,boolean) from public,anon,authenticated,service_role;
alter function private.weekly_source_manager_followups_valid_v1(jsonb) owner to postgres;
revoke all on function private.weekly_source_manager_followups_valid_v1(jsonb) from public,anon,authenticated,service_role;

-- Install validation helpers first. Existing six clocks/automatic actor remain unchanged.
alter table public.weekly_source_global_settings
 add column manager_first_delay_value integer not null default 0,
 add column manager_first_delay_unit text not null default 'HOURS',
 add column manager_followups jsonb not null default '[{"sequence":1,"interval_value":2,"interval_unit":"CLIENT_WORKING_DAYS","mode":"MANUAL"}]'::jsonb,
 add column manager_recent_completed_weeks integer not null default 1,
 add column manager_max_work_age_days integer not null default 14,
 add constraint weekly_source_manager_first_delay_v1 check(private.weekly_source_manager_interval_valid_v1(manager_first_delay_value,manager_first_delay_unit,true)),
 add constraint weekly_source_manager_followups_v1 check(private.weekly_source_manager_followups_valid_v1(manager_followups)),
 add constraint weekly_source_manager_recent_weeks_v1 check(manager_recent_completed_weeks between 1 and 52),
 add constraint weekly_source_manager_max_age_v1 check(manager_max_work_age_days between 1 and 366);

create table public.weekly_source_global_settings_receipts (
 request_id uuid primary key,
 agency_id uuid not null,
 environment text not null check(environment in('TEST','LIVE')),
 actor_user_id uuid not null references public.tms_users(id),
 request_digest bytea not null check(octet_length(request_digest)=32),
 expected_settings_version bigint not null check(expected_settings_version>=1),
 accepted_settings_version bigint not null check(accepted_settings_version>=expected_settings_version),
 response_json jsonb not null check(jsonb_typeof(response_json)='object'),
 created_at_utc timestamptz not null,
 completed_at_utc timestamptz not null
);
alter table public.weekly_source_global_settings_receipts owner to postgres;
alter table public.weekly_source_global_settings_receipts enable row level security;
alter table public.weekly_source_global_settings_receipts force row level security;
create policy cloudtms_miget_service_owner_all on public.weekly_source_global_settings_receipts
 to current_user,service_role using(true) with check(true);
revoke all on table public.weekly_source_global_settings_receipts from public,anon,authenticated,service_role;

create or replace function private.weekly_source_global_settings_receipt_guard_v1()
returns trigger language plpgsql security definer set search_path='pg_catalog','pg_temp'
as $function$
begin raise exception 'WEEKLY_SOURCE_SETTINGS_RECEIPT_IMMUTABLE' using errcode='42501';end;
$function$;
create trigger weekly_source_global_settings_receipt_immutable_v1 before update or delete on public.weekly_source_global_settings_receipts
 for each row execute function private.weekly_source_global_settings_receipt_guard_v1();

alter function private.weekly_source_global_settings_receipt_guard_v1() owner to postgres;
revoke all on function private.weekly_source_global_settings_receipt_guard_v1() from public,anon,authenticated,service_role;

commit;
