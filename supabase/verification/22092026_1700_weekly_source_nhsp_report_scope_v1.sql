-- Rollback-only PostgreSQL 17 proof for NHSP report-scope resolution.
\set ON_ERROR_STOP on

begin;
set local request.jwt.claim.role='service_role';

create function pg_temp.assert_true(p_condition boolean,p_message text)
returns void language plpgsql as $function$
begin
  if p_condition is distinct from true then
    raise exception 'ASSERTION_FAILED: %',p_message;
  end if;
end;
$function$;

select pg_temp.assert_true(
  pg_catalog.to_regprocedure('public.weekly_source_nhsp_report_scope_resolve_atomic_v1(jsonb)') is not null,
  'NHSP report-scope resolver is missing'
);
select pg_temp.assert_true(
  pg_catalog.has_function_privilege('service_role',
    'public.weekly_source_nhsp_report_scope_resolve_atomic_v1(jsonb)','EXECUTE')
  and not pg_catalog.has_function_privilege('authenticated',
    'public.weekly_source_nhsp_report_scope_resolve_atomic_v1(jsonb)','EXECUTE'),
  'NHSP report-scope resolver ACL is incorrect'
);

insert into public.tms_users(id,email,role,is_active,password_hash,payment_authoriser,payment_golden_key)
values ('92200000-0000-4000-8000-000000000001','nhsp-scope@example.invalid','admin',true,'not-a-login',false,false);
insert into public.clients(id,cli_ref,name)
values ('92200000-0000-4000-8000-000000000002','CLI-92200','Exact NHSP Trust');
insert into public.weekly_source_groups(
  id,environment,agency_id,code,display_name,source_family,timezone,
  cutoff_weekday,cutoff_local_time,nhsp_report_heading_name
) values (
  '92200000-0000-4000-8000-000000000003','TEST',
  '92200000-0000-4000-8000-000000000004','NHSP_SCOPE_VERIFY','NHSP Scope Verify',
  'NHSP','Europe/London',3,'15:00','Exact Agency Heading'
);
insert into public.weekly_source_group_clients(
  source_group_id,client_id,valid_from,created_by_user_id
) values (
  '92200000-0000-4000-8000-000000000003','92200000-0000-4000-8000-000000000002',
  '2026-01-01','92200000-0000-4000-8000-000000000001'
);
insert into public.weekly_source_cycles(
  id,source_group_id,finalisation_week_ending,cutoff_at_utc
) values (
  '92200000-0000-4000-8000-000000000005','92200000-0000-4000-8000-000000000003',
  '2026-09-20','2026-09-23 14:00:00+00'
);

do $verification$
declare
  v_first jsonb;
  v_second jsonb;
begin
  v_first:=public.weekly_source_nhsp_report_scope_resolve_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','92200000-0000-4000-8000-000000000001',
    'source_group_id','92200000-0000-4000-8000-000000000003',
    'source_cycle_id','92200000-0000-4000-8000-000000000005',
    'trust_name',' exact nhsp trust '
  ));
  v_second:=public.weekly_source_nhsp_report_scope_resolve_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','92200000-0000-4000-8000-000000000001',
    'source_group_id','92200000-0000-4000-8000-000000000003',
    'source_cycle_id','92200000-0000-4000-8000-000000000005',
    'trust_name','Exact NHSP Trust'
  ));
  perform pg_temp.assert_true(
    v_first->>'report_scope_id'=v_second->>'report_scope_id'
      and v_first->>'client_id'='92200000-0000-4000-8000-000000000002'
      and (select pg_catalog.count(*)=1 from public.weekly_source_report_scopes scope
        where scope.source_cycle_id='92200000-0000-4000-8000-000000000005'),
    'NHSP report scope was not resolved idempotently'
  );
end;
$verification$;

rollback;
