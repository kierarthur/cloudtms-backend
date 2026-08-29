\set ON_ERROR_STOP on

begin;

-- Provider-neutral compatibility for PostgreSQL services that do not ship the
-- Supabase auth schema. CloudTMS authenticates application users from
-- public.tms_users; this one-column table exists only as the historical
-- foreign-key target retained by the approved database contract.
create schema if not exists auth;

create table if not exists auth.users (
  id uuid primary key
);

create or replace function auth.uid()
returns uuid
language sql
stable
as $function$
  select nullif(
    coalesce(
      current_setting('request.jwt.claim.sub', true),
      (nullif(current_setting('request.jwt.claims', true), '')::jsonb ->> 'sub')
    ),
    ''
  )::uuid
$function$;

create or replace function auth.role()
returns text
language sql
stable
as $function$
  select nullif(
    coalesce(
      current_setting('request.jwt.claim.role', true),
      (nullif(current_setting('request.jwt.claims', true), '')::jsonb ->> 'role')
    ),
    ''
  )
$function$;

create or replace function auth.jwt()
returns jsonb
language sql
stable
as $function$
  select coalesce(
    nullif(current_setting('request.jwt.claims', true), '')::jsonb,
    '{}'::jsonb
  )
$function$;

grant usage on schema auth to anon, authenticated, service_role;
grant execute on function auth.uid() to anon, authenticated, service_role;
grant execute on function auth.role() to anon, authenticated, service_role;
grant execute on function auth.jwt() to anon, authenticated, service_role;

commit;
