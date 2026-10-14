-- Separate corrected first-use construction for the proposed all-owner
-- alias census. Requires the independently reviewed OWNER_CONFLICT/55000 ABI;
-- the earlier draft remains unchanged and must not be executed.
-- Unmounted native helper first-use construction. Run only after the reviewed
-- alias owner is composed, with the approved rollback/release verifier runner.
-- Synthetic direct fixture rows test this read helper, not alias birth through
-- the full publication/paid pipeline. No paid/economic/history acceptance claim.
\set ON_ERROR_STOP on
begin;
set local request.jwt.claim.role='service_role';

create function pg_temp.hr_alias_assert(ok boolean,code text)
returns void language plpgsql as $f$
begin if ok is distinct from true then raise exception '%',code using errcode='55000';end if;end;
$f$;

-- Vacant identifiers are asserted before the first business fixture write.
-- This exact finite manifest is also consumed by the fresh restoration proof.
create temporary table hr_alias_fixture_manifest(id uuid primary key) on commit drop;
insert into hr_alias_fixture_manifest
select ('f7120000-0000-4000-8000-'||lpad(i::text,12,'0'))::uuid from generate_series(1,90) i;
select pg_temp.hr_alias_assert(not exists(
  select 1 from hr_alias_fixture_manifest f where exists(select 1 from public.tms_users x where x.id=f.id)
    or exists(select 1 from public.clients x where x.id=f.id)
    or exists(select 1 from public.candidates x where x.id=f.id)
    or exists(select 1 from public.contracts x where x.id=f.id)
    or exists(select 1 from public.weekly_source_groups x where x.id=f.id)
    or exists(select 1 from public.weekly_source_cycles x where x.id=f.id)
    or exists(select 1 from public.weekly_source_uploads x where x.id=f.id)
    or exists(select 1 from public.weekly_source_upload_rows x where x.id=f.id)
    or exists(select 1 from public.weekly_source_row_resolutions x where x.id=f.id)
    or exists(select 1 from public.weekly_work_events x where x.id=f.id)
    or exists(select 1 from public.weekly_work_event_source_links x where x.id=f.id)
),'HR_ALIAS_FIXTURE_ID_COLLISION');
select pg_temp.hr_alias_assert(
  not exists(select 1 from public.tms_users where email='hr-alias-verifier@arthur-rai.co.uk')
  and not exists(select 1 from public.clients where cli_ref in('CLI-991714','CLI-991715'))
  and not exists(select 1 from public.candidates where tms_ref in('CCR-991714','CCR-991715'))
  and not exists(select 1 from public.weekly_source_groups where code in('HR_ALIAS_NATIVE_1714','HR_ALIAS_OTHER_1714')),
  'HR_ALIAS_FIXTURE_NATURAL_KEY_COLLISION');
select pg_temp.hr_alias_assert(to_regprocedure('private.weekly_source_known_source_key_v1(uuid,uuid,uuid)') is not null,
  'HR_ALIAS_REVIEWED_OWNER_NOT_COMPOSED');
select pg_temp.hr_alias_assert(not has_function_privilege('service_role','private.weekly_source_known_source_key_v1(uuid,uuid,uuid)','EXECUTE')
  and not has_function_privilege('anon','private.weekly_source_known_source_key_v1(uuid,uuid,uuid)','EXECUTE')
  and not has_function_privilege('authenticated','private.weekly_source_known_source_key_v1(uuid,uuid,uuid)','EXECUTE'),
  'HR_ALIAS_PRIVATE_OWNER_ACL');

insert into public.tms_users(id,email,role,is_active,password_hash,payment_authoriser,payment_golden_key)
values('f7120000-0000-4000-8000-000000000001','hr-alias-verifier@arthur-rai.co.uk','admin',true,'not-a-login',false,false);
insert into public.clients(id,cli_ref,name)
values('f7120000-0000-4000-8000-000000000002','CLI-991714','Synthetic alias verifier');
insert into public.candidates(id,tms_ref,display_name)
values('f7120000-0000-4000-8000-000000000003','CCR-991714','Synthetic alias candidate');
insert into public.contracts(id,client_id,candidate_id,start_date,end_date,pay_method_snapshot,rates_json)
values('f7120000-0000-4000-8000-000000000004','f7120000-0000-4000-8000-000000000002',
 'f7120000-0000-4000-8000-000000000003','2026-01-01','2026-12-31','PAYE',
 '{"paye_day":10,"paye_night":10,"paye_sat":10,"paye_sun":10,"paye_bh":10,"charge_day":20,"charge_night":20,"charge_sat":20,"charge_sun":20,"charge_bh":20}');
insert into public.weekly_source_groups(id,environment,agency_id,code,display_name,source_family,timezone,cutoff_weekday,cutoff_local_time)
values('f7120000-0000-4000-8000-000000000005','TEST','f7120000-0000-4000-8000-000000000006','HR_ALIAS_NATIVE_1714','Synthetic alias group','ROSTER','Europe/London',3,'15:00'),
 ('f7120000-0000-4000-8000-000000000007','TEST','f7120000-0000-4000-8000-000000000006','HR_ALIAS_OTHER_1714','Synthetic other group','ROSTER','Europe/London',3,'15:00');
insert into public.weekly_source_group_clients(source_group_id,client_id,valid_from,created_by_user_id)
select id,'f7120000-0000-4000-8000-000000000002','2026-01-01','f7120000-0000-4000-8000-000000000001'
from public.weekly_source_groups where id in('f7120000-0000-4000-8000-000000000005','f7120000-0000-4000-8000-000000000007');
insert into public.weekly_source_cycles(id,source_group_id,finalisation_week_ending,cutoff_at_utc)
values('f7120000-0000-4000-8000-000000000008','f7120000-0000-4000-8000-000000000005','2026-09-20','2026-09-16 14:00:00+00'),
 ('f7120000-0000-4000-8000-000000000009','f7120000-0000-4000-8000-000000000007','2026-09-20','2026-09-16 14:00:00+00');

insert into public.clients(id,cli_ref,name) values('f7120000-0000-4000-8000-000000000040','CLI-991715','Synthetic other alias client');
insert into public.candidates(id,tms_ref,display_name) values('f7120000-0000-4000-8000-000000000041','CCR-991715','Synthetic other alias candidate');
insert into public.contracts(id,client_id,candidate_id,start_date,end_date,pay_method_snapshot,rates_json)
values('f7120000-0000-4000-8000-000000000042','f7120000-0000-4000-8000-000000000040',
 'f7120000-0000-4000-8000-000000000041','2026-01-01','2026-12-31','PAYE',
 '{"paye_day":10,"paye_night":10,"paye_sat":10,"paye_sun":10,"paye_bh":10,"charge_day":20,"charge_night":20,"charge_sat":20,"charge_sun":20,"charge_bh":20}');
insert into public.weekly_source_group_clients(source_group_id,client_id,valid_from,created_by_user_id)
values('f7120000-0000-4000-8000-000000000005','f7120000-0000-4000-8000-000000000040','2026-01-01','f7120000-0000-4000-8000-000000000001');

create function pg_temp.hr_alias_seed_upload(p_id uuid,p_cycle uuid,p_profile text)
returns void language plpgsql as $f$
declare profile uuid;
begin
 select id into strict profile from public.weekly_source_format_profiles where profile_code=p_profile and active;
 insert into public.weekly_source_uploads(id,source_cycle_id,original_filename,content_sha256,byte_count,source_format_profile_id,
   parser_version,normaliser_version,header_coordinate_map_hash,declared_scope_fingerprint,coverage_proof_kind,
   physical_row_count,uploaded_by_user_id)
 values(p_id,p_cycle,'synthetic-alias-verifier.csv',decode(repeat('71',32),'hex'),1,profile,
   'NATIVE_HELPER_FIXTURE','NATIVE_HELPER_FIXTURE',decode(repeat('72',32),'hex'),decode(repeat('73',32),'hex'),
   'OFFICE_COMPLETE_EXPORT_ATTESTATION',0,'f7120000-0000-4000-8000-000000000001');
end;
$f$;
select pg_temp.hr_alias_seed_upload('f7120000-0000-4000-8000-000000000010','f7120000-0000-4000-8000-000000000008','HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1');
select pg_temp.hr_alias_seed_upload('f7120000-0000-4000-8000-000000000011','f7120000-0000-4000-8000-000000000008','HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1');
select pg_temp.hr_alias_seed_upload('f7120000-0000-4000-8000-000000000012','f7120000-0000-4000-8000-000000000009','HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1');
select pg_temp.hr_alias_seed_upload('f7120000-0000-4000-8000-000000000013','f7120000-0000-4000-8000-000000000008','HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1');
select pg_temp.hr_alias_seed_upload('f7120000-0000-4000-8000-000000000030','f7120000-0000-4000-8000-000000000008','HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1');

create function pg_temp.hr_alias_seed_row(p_row uuid,p_upload uuid,p_ordinal integer,p_key text,p_event uuid,p_resolution uuid,p_link uuid)
returns void language plpgsql as $f$
begin
 insert into public.weekly_source_upload_rows(id,upload_id,source_row_ordinal,external_source_key,source_candidate_identity,
   source_client_identity,work_date,start_at_local,end_at_local,break_minutes,actual_net_minutes,row_finalisation_state,finalised_by,normalised_row_hash)
 values(p_row,p_upload,p_ordinal,p_key,'Synthetic alias candidate','Synthetic alias verifier','2026-09-15',
   '2026-09-15 09:00','2026-09-15 17:00',30,450,'SOURCE_WORKED','Synthetic fixture',decode(repeat('74',32),'hex'));
 if p_event is null then return;end if;
 insert into public.weekly_source_row_resolutions(id,upload_row_id,generation,candidate_id,client_id,contract_id,work_event_id,
   paid_minutes,mapping_state,contract_selection_method,work_event_match_kind,work_event_match_fingerprint,
   qualification_profile_fingerprint,qualifying_contract_count,qualifying_contract_set_hash,source_row_fingerprint)
 values(p_resolution,p_row,1,'f7120000-0000-4000-8000-000000000003','f7120000-0000-4000-8000-000000000002',
   'f7120000-0000-4000-8000-000000000004',p_event,450,'RESOLVED','DURABLE_LINEAGE','EXACT_DURABLE_LINEAGE',
   decode(repeat('75',32),'hex'),decode(repeat('76',32),'hex'),1,decode(repeat('77',32),'hex'),decode(repeat('74',32),'hex'));
 insert into public.weekly_work_event_source_links(id,work_event_id,upload_row_id,row_resolution_id,link_kind,link_hash)
 values(p_link,p_event,p_row,p_resolution,'POSITIVE_SOURCE',private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_WORK_EVENT_LINK_V1',
   jsonb_build_object('work_event_id',p_event,'upload_row_id',p_row,'resolution_id',p_resolution,'link_kind','POSITIVE_SOURCE')));
end;
$f$;

insert into public.weekly_work_events(id,candidate_id,client_id,work_date,identity_kind,profile_external_key,durable_identity_hash,first_source_group_id,source_format_profile_id)
select 'f7120000-0000-4000-8000-000000000020','f7120000-0000-4000-8000-000000000003','f7120000-0000-4000-8000-000000000002',
 '2026-09-15','PROFILE_EXTERNAL_KEY','A',decode(repeat('78',32),'hex'),'f7120000-0000-4000-8000-000000000005',id
from public.weekly_source_format_profiles where profile_code='HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1' and active;
select pg_temp.hr_alias_seed_row('f7120000-0000-4000-8000-000000000021','f7120000-0000-4000-8000-000000000010',1,'B','f7120000-0000-4000-8000-000000000020','f7120000-0000-4000-8000-000000000022','f7120000-0000-4000-8000-000000000023');
select pg_temp.hr_alias_seed_row('f7120000-0000-4000-8000-000000000024','f7120000-0000-4000-8000-000000000011',1,'B',null,null,null);
select pg_temp.hr_alias_seed_row('f7120000-0000-4000-8000-000000000025','f7120000-0000-4000-8000-000000000011',2,'C',null,null,null);
select pg_temp.hr_alias_seed_row('f7120000-0000-4000-8000-000000000026','f7120000-0000-4000-8000-000000000012',1,'B',null,null,null);
select pg_temp.hr_alias_seed_row('f7120000-0000-4000-8000-000000000027','f7120000-0000-4000-8000-000000000013',1,'B',null,null,null);

do $native_helper_cases$
declare own_candidate uuid:='f7120000-0000-4000-8000-000000000003';own_client uuid:='f7120000-0000-4000-8000-000000000002';
begin
 perform pg_temp.hr_alias_assert(private.weekly_source_known_source_key_v1('f7120000-0000-4000-8000-000000000024',own_candidate,own_client)='f7120000-0000-4000-8000-000000000020','HR_ALIAS_REPEATED_B_IDENTITY');
 perform pg_temp.hr_alias_assert(private.weekly_source_known_source_key_v1('f7120000-0000-4000-8000-000000000025',own_candidate,own_client) is null,'HR_ALIAS_NEW_C_NOT_KNOWN');
 perform pg_temp.hr_alias_assert(private.weekly_source_known_source_key_v1('f7120000-0000-4000-8000-000000000026',own_candidate,own_client) is null,'HR_ALIAS_WRONG_GROUP');
 perform pg_temp.hr_alias_assert(private.weekly_source_known_source_key_v1('f7120000-0000-4000-8000-000000000027',own_candidate,own_client) is null,'HR_ALIAS_WRONG_PROFILE');
 begin
  perform private.weekly_source_known_source_key_v1('f7120000-0000-4000-8000-000000000024','f7120000-0000-4000-8000-000000000041',own_client);
  raise exception 'HR_ALIAS_FOREIGN_CANDIDATE_NOT_REFUSED' using errcode='P0001';
 exception when sqlstate '55000' then
  if SQLERRM is distinct from 'WEEKLY_SOURCE_SOURCE_KEY_ALIAS_OWNER_CONFLICT' then raise;end if;
 end;
 begin
  perform private.weekly_source_known_source_key_v1('f7120000-0000-4000-8000-000000000024',own_candidate,'f7120000-0000-4000-8000-000000000040');
  raise exception 'HR_ALIAS_FOREIGN_CLIENT_NOT_REFUSED' using errcode='P0001';
 exception when sqlstate '55000' then
  if SQLERRM is distinct from 'WEEKLY_SOURCE_SOURCE_KEY_ALIAS_OWNER_CONFLICT' then raise;end if;
 end;
 perform pg_temp.hr_alias_assert(private.weekly_source_known_source_key_v1('f7120000-0000-4000-8000-000000000024',own_candidate,'f7120000-0000-4000-8000-000000000090') is null,'HR_ALIAS_WRONG_CLIENT');
 perform pg_temp.hr_alias_assert(private.weekly_source_known_source_key_v1(null,own_candidate,own_client) is null,'HR_ALIAS_NULL_IDENTITY');
 perform pg_temp.hr_alias_assert((select profile_external_key='A' from public.weekly_work_events where id='f7120000-0000-4000-8000-000000000020'),'HR_ALIAS_ORIGINAL_KEY_PRESERVED');
end;
$native_helper_cases$;

-- A second genuine typed fixture B link is owned by a different actual
-- Candidate/client. Both owners must survive the all-owner census; no local
-- subset is allowed to hide the foreign owner and return a UUID.
insert into public.weekly_work_events(id,candidate_id,client_id,work_date,identity_kind,profile_external_key,durable_identity_hash,first_source_group_id,source_format_profile_id)
select 'f7120000-0000-4000-8000-000000000043','f7120000-0000-4000-8000-000000000041','f7120000-0000-4000-8000-000000000040',
 '2026-09-15','PROFILE_EXTERNAL_KEY','D',decode(repeat('7a',32),'hex'),'f7120000-0000-4000-8000-000000000005',id
from public.weekly_source_format_profiles where profile_code='HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1' and active;
insert into public.weekly_source_upload_rows(id,upload_id,source_row_ordinal,external_source_key,source_candidate_identity,
 source_client_identity,work_date,start_at_local,end_at_local,break_minutes,actual_net_minutes,row_finalisation_state,finalised_by,normalised_row_hash)
values('f7120000-0000-4000-8000-000000000044','f7120000-0000-4000-8000-000000000030',1,'B','Synthetic other alias candidate',
 'Synthetic other alias client','2026-09-15','2026-09-15 09:00','2026-09-15 17:00',30,450,'SOURCE_WORKED','Synthetic fixture',decode(repeat('7b',32),'hex'));
insert into public.weekly_source_row_resolutions(id,upload_row_id,generation,candidate_id,client_id,contract_id,work_event_id,
 paid_minutes,mapping_state,contract_selection_method,work_event_match_kind,work_event_match_fingerprint,
 qualification_profile_fingerprint,qualifying_contract_count,qualifying_contract_set_hash,source_row_fingerprint)
values('f7120000-0000-4000-8000-000000000045','f7120000-0000-4000-8000-000000000044',1,
 'f7120000-0000-4000-8000-000000000041','f7120000-0000-4000-8000-000000000040','f7120000-0000-4000-8000-000000000042',
 'f7120000-0000-4000-8000-000000000043',450,'RESOLVED','DURABLE_LINEAGE','EXACT_DURABLE_LINEAGE',decode(repeat('75',32),'hex'),
 decode(repeat('76',32),'hex'),1,decode(repeat('77',32),'hex'),decode(repeat('7b',32),'hex'));
insert into public.weekly_work_event_source_links(id,work_event_id,upload_row_id,row_resolution_id,link_kind,link_hash)
values('f7120000-0000-4000-8000-000000000046','f7120000-0000-4000-8000-000000000043','f7120000-0000-4000-8000-000000000044',
 'f7120000-0000-4000-8000-000000000045','POSITIVE_SOURCE',private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_WORK_EVENT_LINK_V1',
 jsonb_build_object('work_event_id','f7120000-0000-4000-8000-000000000043'::uuid,'upload_row_id','f7120000-0000-4000-8000-000000000044'::uuid,
 'resolution_id','f7120000-0000-4000-8000-000000000045'::uuid,'link_kind','POSITIVE_SOURCE')));
do $native_mixed_alias$
begin
 begin
  perform private.weekly_source_known_source_key_v1('f7120000-0000-4000-8000-000000000024',
   'f7120000-0000-4000-8000-000000000003','f7120000-0000-4000-8000-000000000002');
  raise exception 'HR_ALIAS_MIXED_OWNER_NOT_REFUSED' using errcode='P0001';
 exception when sqlstate '55000' then
  if SQLERRM is distinct from 'WEEKLY_SOURCE_SOURCE_KEY_ALIAS_AMBIGUOUS' then raise;end if;
 end;
end;
$native_mixed_alias$;

-- Genuine current canonical-key precedence is tested with a separately admitted
-- native event row; no existing immutable event or old source link is rewritten.
insert into public.weekly_work_events(id,candidate_id,client_id,work_date,identity_kind,profile_external_key,durable_identity_hash,first_source_group_id,source_format_profile_id)
select 'f7120000-0000-4000-8000-000000000028','f7120000-0000-4000-8000-000000000003','f7120000-0000-4000-8000-000000000002',
 '2026-09-15','PROFILE_EXTERNAL_KEY','B',decode(repeat('79',32),'hex'),'f7120000-0000-4000-8000-000000000005',id
from public.weekly_source_format_profiles where profile_code='HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1' and active;
select pg_temp.hr_alias_assert(private.weekly_source_known_source_key_v1('f7120000-0000-4000-8000-000000000024',
 'f7120000-0000-4000-8000-000000000003','f7120000-0000-4000-8000-000000000002') is null,'HR_ALIAS_CANONICAL_KEY_PRECEDENCE');
select pg_temp.hr_alias_assert((select count(*)=2 from public.weekly_work_events where id in('f7120000-0000-4000-8000-000000000020','f7120000-0000-4000-8000-000000000028')),
 'HR_ALIAS_FIXTURE_EVENT_CENSUS');
rollback;
