-- SOURCE PROPOSAL ONLY. No execution or native seal is recorded.
-- Concurrency tokens contain no price, actor, status, business clock or readiness.
do $native_domains$
declare r record; v_count integer:=0;
begin
  if current_setting('block_size')::integer<>8192 then
    raise exception 'CORRECTION_A0_TOKEN_INDEX_PROFILE_UNPROVED' using errcode='55000';
  end if;
  if not exists(select 1 from pg_catalog.pg_collation c where c.oid='pg_catalog.default'::regcollation
    and c.collisdeterministic) then
    raise exception 'CORRECTION_A0_JSONB_ARBITER_EQUALITY_UNPROVED' using errcode='55000';
  end if;
  -- Deterministic text equality has a byte tie-breaker. JSONB string equality
  -- can therefore be the exact arbiter, while native btrim remains the owner.
  -- Unknown/nondeterministic collation refuses this NEW enforcement schema;
  -- it does not change an existing ordinary database's collation or strings.
  for r in select n.nspname,c.relname,a.attname,a.atttypid,a.attcollation,co.collisdeterministic
    from pg_catalog.pg_attribute a join pg_catalog.pg_class c on c.oid=a.attrelid
    join pg_catalog.pg_namespace n on n.oid=c.relnamespace
    join pg_catalog.pg_collation co on co.oid=a.attcollation
    where n.nspname='public' and not a.attisdropped and a.attnum>0
      and (c.relname,a.attname) in (values ('timesheets','booking_id'),('invoice_lines','booking_id'),
        ('timesheets','occupant_key_norm'),('candidates','key_norm'))
  loop
    v_count:=v_count+1;
    if r.atttypid<>'text'::regtype or r.collisdeterministic is distinct from true then
      raise exception 'CORRECTION_A0_NATIVE_TEXT_EQUALITY_UNPROVED' using errcode='55000';
    end if;
  end loop;
  if v_count<>4 or not exists(select 1 from pg_catalog.pg_attribute a
    where a.attrelid='public.candidates'::regclass and a.attname='nhsp_hr_name_aliases'
      and not a.attisdropped and a.atttypid='jsonb'::regtype) then
    raise exception 'CORRECTION_A0_NATIVE_TOKEN_SCHEMA_UNPROVED' using errcode='55000';
  end if;
  if to_regclass('private.timesheet_correction_a0_transaction_fences') is not null then
    raise exception 'CORRECTION_A0_TOKEN_SCHEMA_ALREADY_EXISTS' using errcode='55000';
  end if;
end;
$native_domains$;

create table private.timesheet_correction_a0_transaction_fences (
  token_kind text collate "C" not null,
  uuid_identity uuid,
  json_identity jsonb,
  touch_bit boolean not null default false,
  constraint timesheet_correction_a0_fences_shape check (
    (token_kind in ('FAMILY_UUID','PHYSICAL_UUID') and uuid_identity is not null and json_identity is null)
    or (token_kind in ('BOOKING','KEY_NORM','ALIAS_JSON_STRING') and uuid_identity is null and json_identity is not null
      and jsonb_typeof(json_identity)='string' and octet_length(json_identity::text)<=1536)
  ),
  constraint timesheet_correction_a0_fences_identity_key
    unique nulls not distinct (token_kind,uuid_identity,json_identity)
);
alter table private.timesheet_correction_a0_transaction_fences owner to postgres;
alter table private.timesheet_correction_a0_transaction_fences enable row level security;
create policy cloudtms_miget_service_owner_all on private.timesheet_correction_a0_transaction_fences
  for all to CURRENT_USER,service_role using (true) with check (true);
revoke all on table private.timesheet_correction_a0_transaction_fences from public,anon,authenticated,service_role;
-- No ordinary permanent seeding, retention timer, business-history update,
-- trigger, sequence, FK, read-projection grant or token retirement is installed.
