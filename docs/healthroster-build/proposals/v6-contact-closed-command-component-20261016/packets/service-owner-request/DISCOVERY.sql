select 'ACTOR_CANDIDATE_NOT_ROLE_PROOF'case_kind,category,id actor_user_id,
 md5(jsonb_build_array(id,is_active,lower(btrim(coalesce(role,''))))::text)authority_fields_md5
 from(select * from(select 'ACTIVE_ADMIN'category,id,is_active,role from public.tms_users where is_active is true and lower(btrim(coalesce(role,'')))='admin'order by id limit 1)a
 union all select * from(select 'ACTIVE_NONADMIN'category,id,is_active,role from public.tms_users where is_active is true and lower(btrim(coalesce(role,'')))<>'admin'order by id limit 1)b
 union all select * from(select 'INACTIVE'category,id,is_active,role from public.tms_users where is_active is false order by id limit 1)c)s order by category;

select 'ORIGINAL_START_RECOVERY_CANDIDATE_ONLY'case_kind,a.id attempt_id,t.id target_id,c.agency_id,
 a.completed_at_utc is null unknown_attempt,md5(jsonb_build_array(a.id,a.dispatch_target_id,a.attempt_number,a.completed_at_utc)::text)attempt_state_md5
 from public.weekly_message_target_attempts a join public.weekly_message_dispatch_targets t on t.id=a.dispatch_target_id
 join public.weekly_message_dispatch_commands c on c.id=t.dispatch_command_id where c.environment='TEST'
 and a.lease_token is not null and nullif(a.lease_owner,'')is not null order by a.id limit 5;

select 'COMPLETED_ORIGINAL_RESULT_CANDIDATE_ONLY'case_kind,a.id attempt_id,t.id target_id,c.agency_id,
 md5(jsonb_build_array(a.id,a.dispatch_target_id,a.attempt_number,a.completed_at_utc)::text)attempt_state_md5
 from public.weekly_message_target_attempts a join public.weekly_message_dispatch_targets t on t.id=a.dispatch_target_id
 join public.weekly_message_dispatch_commands c on c.id=t.dispatch_command_id where c.environment='TEST'
 and a.completed_at_utc is not null and a.result_hash is not null and a.outcome in('ACCEPTED','DEFINITELY_REJECTED','TRANSIENT_FAILURE','AMBIGUOUS')
 and a.result_hash=extensions.digest(convert_to(jsonb_build_object('provider_attempt_id',a.id,'outcome',upper(btrim(a.outcome)),
 'provider_message_id',nullif(btrim(a.provider_message_id),''),'bounded_provider_receipt',coalesce(a.bounded_provider_receipt_json,'{}'::jsonb),
 'bounded_error',coalesce(a.bounded_error_json,'{}'::jsonb))::text,'UTF8'),'sha256')order by a.id limit 5;
