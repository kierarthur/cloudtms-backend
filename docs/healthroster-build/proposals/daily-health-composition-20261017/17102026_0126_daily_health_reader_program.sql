-- REQUIRED_NOT_RUN. Root supplies its genuine accepted Daily operation and a
-- reviewed actor matrix via psql variables. No user/role/fixture mutation here.
\if :{?daily_health_reader_fixture}
\else
\echo 'daily_health_reader_fixture required'
\quit 3
\endif
create temporary table daily_health_reader_input(args jsonb not null) on commit drop;
insert into daily_health_reader_input values(:'daily_health_reader_fixture'::jsonb);
-- Fixture: {operation_id,actors:[{actor_user_id,expected_active,expected_admin}]}
-- Root must include active Admin, pay-only, ordinary and inactive actors; at
-- least one active noncreator who formerly had Admin must now be nonAdmin.
do $reader$
declare fixture jsonb; actor jsonb; user_row public.tms_users%rowtype; basis jsonb; rejected boolean; operation uuid;
begin
 select i.args into strict fixture from daily_health_reader_input i;operation:=(fixture->>'operation_id')::uuid;
 if jsonb_array_length(fixture->'actors')<4 then raise exception 'GENUINE_CURRENT_ROLE_MATRIX_REQUIRED';end if;
 if (select count(distinct a->>'actor_user_id') from jsonb_array_elements(fixture->'actors') a)<>jsonb_array_length(fixture->'actors')
  or not exists(select 1 from jsonb_array_elements(fixture->'actors') a join public.tms_users u on u.id=(a->>'actor_user_id')::uuid
    where u.is_active and lower(btrim(u.role))='admin')
  or not exists(select 1 from jsonb_array_elements(fixture->'actors') a join public.tms_users u on u.id=(a->>'actor_user_id')::uuid
    where u.is_active and lower(btrim(u.role))<>'admin' and (u.payment_authoriser or u.payment_golden_key))
  or not exists(select 1 from jsonb_array_elements(fixture->'actors') a join public.tms_users u on u.id=(a->>'actor_user_id')::uuid
    where u.is_active and lower(btrim(u.role))<>'admin' and not u.payment_authoriser and not u.payment_golden_key)
  or not exists(select 1 from jsonb_array_elements(fixture->'actors') a join public.tms_users u on u.id=(a->>'actor_user_id')::uuid
    where not u.is_active) then raise exception 'ADMIN_PAY_ORDINARY_INACTIVE_MATRIX_REQUIRED';end if;
 for actor in select value from jsonb_array_elements(fixture->'actors') loop
  select u.* into strict user_row from public.tms_users u where u.id=(actor->>'actor_user_id')::uuid;
  if user_row.is_active is distinct from (actor->>'expected_active')::boolean
   or (lower(btrim(user_row.role))='admin') is distinct from (actor->>'expected_admin')::boolean then
   raise exception 'READER_MATRIX_CURRENT_NATIVE_ROLE_MISMATCH';end if;
  rejected:=false;basis:=null;
  begin basis:=private.daily_rate_correction_health_result_basis_v1(operation,user_row.id);
  exception when insufficient_privilege then rejected:=true;end;
  if rejected=user_row.is_active then raise exception 'CURRENT_ACTIVE_READER_ADMISSION_MISMATCH';end if;
  if user_row.is_active then
   if basis->>'operation_id' is distinct from operation::text or basis?'placement_receipt'
    or basis->'financial_command_id' is distinct from 'null'::jsonb
    or basis->'current_financial_cut_id' is distinct from 'null'::jsonb then
    raise exception 'RAW_FINANCIAL_RECEIPT_OR_ID_LEAK';end if;
   if lower(btrim(user_row.role))<>'admin' and
    (basis#>'{billing,group_id}' is distinct from 'null'::jsonb
      or (basis#>>'{billing,status}'<>'NOT_REQUIRED' and basis#>'{billing,visible_summary}' is distinct from '{"visibility":"REDACTED"}'::jsonb)) then
    raise exception 'CURRENT_NONADMIN_NATIVE_BILLING_REDACTION_MISMATCH';end if;
  end if;
 end loop;
end;
$reader$;
select 'DAILY_HEALTH_ACTIVE_READER_CURRENT_ROLE_REDACTION_ASSERTED' as result;
