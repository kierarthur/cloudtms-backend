-- WP-PP harmless actor/read first use only. Fixtures always roll back.
-- No protected publication, Draft, payment, provider or communications owner is called.
\set ON_ERROR_STOP on
begin;
select set_config('request.jwt.claim.role','service_role',true);
select set_config('request.jwt.claims','{"role":"service_role"}',true);

do $vacancy$
begin
  if exists(select 1 from public.tms_users where id::text like 'f7100000-%'
      or lower(email) in ('wp-pp-admin@example.invalid','wp-pp-authoriser@example.invalid',
        'wp-pp-golden@example.invalid','wp-pp-inactive-admin@example.invalid',
        'wp-pp-unflagged@example.invalid','wp-pp-inactive-authoriser@example.invalid'))
    or exists(select 1 from public.weekly_source_groups where id::text like 'f7100000-%'
      or code in ('WP_PP_ROSTER','WP_PP_NHSP'))
    or exists(select 1 from public.clients where id::text like 'f7100000-%')
    or exists(select 1 from public.weekly_source_group_clients where id::text like 'f7100000-%')
    or exists(select 1 from public.weekly_source_cycles where id::text like 'f7100000-%') then
    raise exception 'WP_PP_FIXTURE_ID_COLLISION';
  end if;
end;
$vacancy$;

insert into public.tms_users(id,email,role,is_active,password_hash,payment_authoriser,payment_golden_key)
values
 ('f7100000-0000-4000-8000-000000000001','wp-pp-admin@example.invalid','admin',true,'not-a-login',false,false),
 ('f7100000-0000-4000-8000-000000000002','wp-pp-authoriser@example.invalid','user',true,'not-a-login',true,false),
 ('f7100000-0000-4000-8000-000000000003','wp-pp-golden@example.invalid','user',true,'not-a-login',true,true),
 ('f7100000-0000-4000-8000-000000000004','wp-pp-inactive-admin@example.invalid','admin',false,'not-a-login',true,true),
 ('f7100000-0000-4000-8000-000000000005','wp-pp-unflagged@example.invalid','user',true,'not-a-login',false,false),
 ('f7100000-0000-4000-8000-000000000006','wp-pp-inactive-authoriser@example.invalid','user',false,'not-a-login',true,true);

insert into public.clients(id,name)
values ('f7100000-0000-4000-8000-000000000010','WP-PP Roster permission fixture'),
       ('f7100000-0000-4000-8000-000000000016','WP-PP NHSP permission fixture');
insert into public.weekly_source_groups(id,environment,agency_id,code,display_name,source_family,
  cutoff_weekday,cutoff_local_time,nhsp_report_heading_name)
values
 ('f7100000-0000-4000-8000-000000000011','TEST','f7100000-0000-4000-8000-000000000019',
  'WP_PP_ROSTER','WP-PP Roster','ROSTER',3,'15:00',null),
 ('f7100000-0000-4000-8000-000000000012','TEST','f7100000-0000-4000-8000-000000000019',
  'WP_PP_NHSP','WP-PP NHSP','NHSP',3,'15:00','Trust');
insert into public.weekly_source_group_clients(id,source_group_id,client_id,valid_from,valid_to,created_by_user_id)
values
 ('f7100000-0000-4000-8000-000000000013','f7100000-0000-4000-8000-000000000011',
  'f7100000-0000-4000-8000-000000000010','2026-09-01','2026-09-30','f7100000-0000-4000-8000-000000000001'),
 ('f7100000-0000-4000-8000-000000000014','f7100000-0000-4000-8000-000000000012',
  'f7100000-0000-4000-8000-000000000016','2026-09-01','2026-09-30','f7100000-0000-4000-8000-000000000001');
insert into public.weekly_source_cycles(id,source_group_id,finalisation_week_ending,cutoff_at_utc,state,version)
values ('f7100000-0000-4000-8000-000000000015','f7100000-0000-4000-8000-000000000011',
  '2026-09-13','2026-09-16 14:00:00+00','OPEN',1);

do $actor_matrix$
declare
  v_actor uuid; v_group uuid; v_client uuid; v_operation text; v_receipt jsonb;
begin
  foreach v_actor in array array[
    'f7100000-0000-4000-8000-000000000001'::uuid,
    'f7100000-0000-4000-8000-000000000002'::uuid,
    'f7100000-0000-4000-8000-000000000003'::uuid]
  loop
    if private.weekly_source_pay_actor_allowed_v1(v_actor) is distinct from true then
      raise exception 'WP_PP_ALLOWED_ACTOR_REFUSED: %',v_actor;
    end if;
    foreach v_group in array array[
      'f7100000-0000-4000-8000-000000000011'::uuid,
      'f7100000-0000-4000-8000-000000000012'::uuid]
    loop
      v_client:=case when v_group='f7100000-0000-4000-8000-000000000011'::uuid
        then 'f7100000-0000-4000-8000-000000000010'::uuid
        else 'f7100000-0000-4000-8000-000000000016'::uuid end;
      foreach v_operation in array array['APPROVE_PROTECTED_PAY','AMEND_PROTECTED_PAY',
        'WITHDRAW_PROTECTED_PAY','RECORD_NOT_WORKED','RECONCILE_PROTECTED_PAY',
        'ACCEPT_SYSTEM_HOURS','OPEN_PAY_QUERY','VIEW_PAY_WORK']
      loop
        v_receipt:=private.weekly_source_office_authority_v1(v_actor,v_operation,v_group,v_client,'2026-09-13');
        if (v_receipt->>'allowed')::boolean is distinct from true
          or v_receipt->>'operation' is distinct from v_operation then
          raise exception 'WP_PP_OPERATION_MATRIX_FAILED';
        end if;
      end loop;
    end loop;
  end loop;
  foreach v_actor in array array[
    'f7100000-0000-4000-8000-000000000004'::uuid,
    'f7100000-0000-4000-8000-000000000005'::uuid,
    'f7100000-0000-4000-8000-000000000006'::uuid,
    'f7100000-0000-4000-8000-000000000099'::uuid,null::uuid]
  loop
    if private.weekly_source_pay_actor_allowed_v1(v_actor) is distinct from false then
      raise exception 'WP_PP_DENIED_ACTOR_ACCEPTED';
    end if;
  end loop;
  -- The missing/inactive error continues to precede actor/payment and scope errors.
  begin
    perform private.weekly_source_office_authority_v1('f7100000-0000-4000-8000-000000000004',
      'APPROVE_PROTECTED_PAY','f7100000-0000-4000-8000-000000000099');
    raise exception 'WP_PP_INACTIVE_NOT_REFUSED';
  exception when insufficient_privilege then
    if sqlerrm is distinct from 'WEEKLY_SOURCE_OFFICE_ACTOR_INACTIVE' then raise; end if;
  end;
  begin
    perform private.weekly_source_office_authority_v1('f7100000-0000-4000-8000-000000000005','APPROVE_PROTECTED_PAY');
    raise exception 'WP_PP_UNFLAGGED_NOT_REFUSED';
  exception when insufficient_privilege then
    if sqlerrm is distinct from 'WEEKLY_SOURCE_PAYMENT_AUTHORISER_REQUIRED' then raise; end if;
  end;
  foreach v_operation in array array['UPLOAD_SOURCE','RECHECK_SOURCE','FINALISE_WEEK','CORRECT_FINAL_SOURCE',
    'ASK_CANDIDATES','SEND_MANAGER','RESEND_MANAGER','VIEW_SOURCE_PROGRESS','ADMIT_SOURCE_INVOICE','MOVE_SOURCE_INVOICE']
  loop
    begin
      perform private.weekly_source_office_authority_v1('f7100000-0000-4000-8000-000000000002',v_operation);
      raise exception 'WP_PP_ADMIN_OPERATION_WIDENED: %',v_operation;
    exception when insufficient_privilege then
      if sqlerrm is distinct from 'WEEKLY_SOURCE_OFFICE_ADMIN_REQUIRED' then raise; end if;
    end;
    perform private.weekly_source_office_authority_v1('f7100000-0000-4000-8000-000000000001',v_operation);
  end loop;
  begin
    perform private.weekly_source_office_authority_v1('f7100000-0000-4000-8000-000000000002','VIEW_PAY_WORK',
      'f7100000-0000-4000-8000-000000000011','f7100000-0000-4000-8000-000000000010','2026-10-01');
    raise exception 'WP_PP_EXPIRED_MEMBERSHIP_ACCEPTED';
  exception when invalid_parameter_value then
    if sqlerrm is distinct from 'WEEKLY_SOURCE_CLIENT_NOT_IN_GROUP' then raise; end if;
  end;
end;
$actor_matrix$;

do $read_first_use$
declare v_result jsonb; v_request jsonb;
begin
  v_request:=jsonb_build_object('actor_user_id','f7100000-0000-4000-8000-000000000002',
    'source_group_id','f7100000-0000-4000-8000-000000000011',
    'source_cycle_id','f7100000-0000-4000-8000-000000000015','tab','queries');
  v_result:=public.weekly_source_office_workspace_v1(v_request);
  if v_result#>>'{permissions,view}' is distinct from 'PAY'
    or v_result ?| array['imports','finalise','history','recheck_payload','lifecycle_policy']
    or v_result#>'{permissions,allowed_tabs}' is distinct from '["queries"]'::jsonb
    or v_result#>'{permissions,checklist}' is distinct from
      '{"available":false,"read_only":true,"pay_only":true,"action_links":false}'::jsonb then
    raise exception 'WP_PP_REDUCED_VIEW_FAILED';
  end if;
  begin
    perform public.weekly_source_office_workspace_v1(v_request||jsonb_build_object('tab','finalise'));
    raise exception 'WP_PP_DIRECT_FINALISE_READ_ACCEPTED';
  exception when insufficient_privilege then
    if sqlerrm is distinct from 'WEEKLY_SOURCE_OFFICE_ADMIN_REQUIRED' then raise; end if;
  end;
  v_result:=public.weekly_source_combined_review_workspace_v1(v_request-'source_cycle_id');
  if v_result#>>'{permissions,view}' is distinct from 'PAY'
    or v_result ?| array['attention','summary'] then
    raise exception 'WP_PP_COMBINED_PAY_VIEW_FAILED';
  end if;
end;
$read_first_use$;

do $acl$
declare v_signature text; v_oid oid; v_definition text;
begin
  foreach v_signature in array array[
    'private.weekly_source_pay_actor_allowed_v1(uuid)',
    'private.weekly_source_office_authority_v1(uuid,text,uuid,uuid,date)',
    'public.weekly_exceptional_pay_record_c1_unknown_v1(jsonb)',
    'public.weekly_exceptional_pay_record_c1_checkpoint_v1(jsonb)',
    'public.weekly_exceptional_pay_record_c1_recovery_v1(jsonb)',
    'public.weekly_exceptional_pay_read_c1_request_v1(jsonb)',
    'public.weekly_exceptional_pay_complete_c1_publication_v1(jsonb)',
    'public.weekly_exceptional_pay_action_publication_status_v1(jsonb)',
    'public.weekly_exceptional_pay_complete_local_v1(jsonb)',
    'public.weekly_source_office_workspace_v1(jsonb)',
    'public.weekly_source_manual_review_open_v1(jsonb)',
    'public.weekly_source_office_authorise_scope_v1(jsonb)',
    'public.weekly_source_workspace_scopes_v1(jsonb)',
    'public.weekly_source_combined_review_workspace_v1(jsonb)',
    'public.weekly_source_office_notifications_list_v1(jsonb)',
    'public.weekly_source_office_timesheet_presentation_v1(jsonb)']
  loop
    v_oid:=to_regprocedure(v_signature);
    if v_oid is null or has_function_privilege('anon',v_oid,'EXECUTE')
      or has_function_privilege('authenticated',v_oid,'EXECUTE')
      or not has_function_privilege('service_role',v_oid,'EXECUTE')
      or exists(select 1 from pg_proc p, lateral aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) acl
        where p.oid=v_oid and acl.grantee=0 and acl.privilege_type='EXECUTE')
      or not exists(select 1 from pg_proc p where p.oid=v_oid and p.prosecdef and p.proconfig is not null) then
      raise exception 'WP_PP_SERVICE_ONLY_ACL_FAILED: %',v_signature;
    end if;
    v_definition:=pg_get_functiondef(v_oid);
    if v_definition~*'pg_catalog\.(coalesce|nullif|least|greatest)\s*\(' then
      raise exception 'WP_PP_ILLEGAL_CONDITIONAL_QUALIFICATION: %',v_signature;
    end if;
  end loop;
end;
$acl$;
rollback;
