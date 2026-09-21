\set ON_ERROR_STOP on

begin;
set local request.jwt.claim.role='service_role';

insert into public.settings_defaults(
  id,candidate_manager_email_templates_sha256,candidate_home_announcement_sha256
) values (
  1,decode(repeat('01',32),'hex'),decode(repeat('02',32),'hex')
) on conflict (id) do update set
  candidate_manager_email_templates_sha256=excluded.candidate_manager_email_templates_sha256,
  candidate_home_announcement_sha256=excluded.candidate_home_announcement_sha256;

insert into public.tms_users(id,email,role,is_active,password_hash)
values ('70000000-0000-4000-8000-000000000001','plan6-projection@example.test','admin',true,'not-a-login');
insert into public.clients(id,name)
values ('70000000-0000-4000-8000-000000000002','Plan 6 Projection Client');
insert into public.client_settings(client_id,vat_rate_pct,effective_from)
values ('70000000-0000-4000-8000-000000000002',20,'2026-01-01');
insert into public.candidates(id,display_name)
values ('70000000-0000-4000-8000-000000000003','Plan 6 Candidate');
insert into public.contracts(
  id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
  weekly_timesheet_source,self_bill,no_timesheet_required,requires_hr,autoprocess_hr
) values (
  '70000000-0000-4000-8000-000000000004',
  '70000000-0000-4000-8000-000000000003',
  '70000000-0000-4000-8000-000000000002',
  '2026-01-01','2026-12-31','PAYE','{}'::jsonb,'HEALTHROSTER',true,true,true,true
);
insert into public.weekly_source_groups(
  id,environment,agency_id,code,display_name,source_family,cutoff_weekday,cutoff_local_time
) values (
  '70000000-0000-4000-8000-000000000005','TEST',
  '70000000-0000-4000-8000-000000000006','PLAN6_PROJECTION','Plan 6 Projection','ROSTER',3,'15:00'
);
insert into public.weekly_source_group_clients(
  id,source_group_id,client_id,valid_from,created_by_user_id
) values (
  '70000000-0000-4000-8000-000000000007',
  '70000000-0000-4000-8000-000000000005',
  '70000000-0000-4000-8000-000000000002','2026-01-01',
  '70000000-0000-4000-8000-000000000001'
);
insert into public.weekly_source_client_policies(
  id,source_group_id,client_id,effective_from,authority_mode,document_mode,
  self_bill_enabled,weekly_rate_classification_method,manager_queries_enabled,
  manager_query_recipient,created_by_user_id
) values (
  '70000000-0000-4000-8000-000000000012',
  '70000000-0000-4000-8000-000000000005',
  '70000000-0000-4000-8000-000000000002','2026-01-01',
  'SOURCE_AUTHORITY','CHECK_ONLY',true,'SPLIT_RATE_WINDOWS',true,
  'manager@example.test','70000000-0000-4000-8000-000000000001'
);
insert into public.weekly_source_cycles(
  id,source_group_id,finalisation_week_ending,cutoff_at_utc,state,version,projection_state
) values (
  '70000000-0000-4000-8000-000000000008',
  '70000000-0000-4000-8000-000000000005','2026-09-13','2026-09-09T14:00:00Z',
  'OPEN',1,'REBUILDING'
);
insert into public.weekly_source_uploads(
  id,source_cycle_id,original_filename,content_sha256,byte_count,source_format_profile_id,
  parser_version,normaliser_version,header_coordinate_map_json,header_coordinate_map_hash,
  declared_scope_fingerprint,suggested_coverage_start_local_date,suggested_coverage_end_local_date,
  confirmed_coverage_start_local_date,confirmed_coverage_end_local_date,coverage_timezone,
  coverage_confirmation_version,coverage_confirmed_by_user_id,coverage_confirmed_at_utc,
  coverage_shrink_acknowledged,coverage_state,coverage_proof_kind,physical_row_count,
  accepted_count,row_manifest_hash,state,uploaded_by_user_id
) values (
  '70000000-0000-4000-8000-000000000009',
  '70000000-0000-4000-8000-000000000008','projection.csv',decode(repeat('01',32),'hex'),100,
  '35555555-5555-4555-8555-555555555555','PARSER_V1','NORMALISER_V1','{}',decode(repeat('02',32),'hex'),
  decode(repeat('03',32),'hex'),'2026-09-07','2026-09-07','2026-09-07','2026-09-07','Europe/London',
  'COMPLETE_EXPORT_V1','70000000-0000-4000-8000-000000000001',clock_timestamp(),false,'COMPLETE',
  'OFFICE_COMPLETE_EXPORT_ATTESTATION',3,3,decode(repeat('04',32),'hex'),'CURRENT',
  '70000000-0000-4000-8000-000000000001'
);
update public.weekly_source_cycles
set current_complete_upload_id='70000000-0000-4000-8000-000000000009'
where id='70000000-0000-4000-8000-000000000008';
insert into public.weekly_source_upload_rows(
  id,upload_id,source_row_ordinal,external_source_key,source_candidate_identity,
  source_client_identity,work_date,start_at_local,end_at_local,break_minutes,
  actual_net_minutes,row_finalisation_state,source_expense_pence,
  source_expense_parse_state,normalised_row_hash
) values
(
  '70000000-0000-4000-8000-000000000010',
  '70000000-0000-4000-8000-000000000009',1,'line-1','Plan 6 Candidate',
  'Plan 6 Projection Client','2026-09-07','2026-09-07 09:00','2026-09-07 17:00',30,450,
  'NOT_APPLICABLE',0,'OMITTED_ZERO',decode(repeat('05',32),'hex')
),(
  '70000000-0000-4000-8000-000000000013',
  '70000000-0000-4000-8000-000000000009',2,'line-2','Plan 6 Candidate',
  'Plan 6 Projection Client','2026-09-08','2026-09-08 09:00','2026-09-08 17:00',30,450,
  'NOT_APPLICABLE',0,'OMITTED_ZERO',decode(repeat('0a',32),'hex')
),(
  '70000000-0000-4000-8000-000000000014',
  '70000000-0000-4000-8000-000000000009',3,'line-3','Plan 6 Candidate',
  'Plan 6 Projection Client','2026-09-09',null,null,0,0,
  'SOURCE_ABSENT_ZERO',0,'OMITTED_ZERO',decode(repeat('0b',32),'hex')
);
insert into public.weekly_source_projection_publications(
  id,source_cycle_id,authority_scope_kind,upload_id,authority_scope_version,
  comparison_manifest_hash,issue_set_hash,state
) values (
  '70000000-0000-4000-8000-000000000011',
  '70000000-0000-4000-8000-000000000008','CYCLE',
  '70000000-0000-4000-8000-000000000009',1,
  decode(repeat('06',32),'hex'),decode(repeat('07',32),'hex'),'BUILDING'
);

select public.weekly_source_projection_rows_apply_atomic_v1(
  '70000000-0000-4000-8000-000000000001',
  '70000000-0000-4000-8000-000000000011',
  jsonb_build_array(
    jsonb_build_object(
      'upload_row_id','70000000-0000-4000-8000-000000000010',
      'mapping_state','RESOLVED',
      'candidate_id','70000000-0000-4000-8000-000000000003',
      'client_id','70000000-0000-4000-8000-000000000002',
      'contract_id','70000000-0000-4000-8000-000000000004',
      'contract_selection_method','AUTO_UNIQUE',
      'qualifying_contract_ids',jsonb_build_array('70000000-0000-4000-8000-000000000004'),
      'identity_kind','PROFILE_EXTERNAL_KEY','profile_external_key','line-1',
      'link_kind','POSITIVE_SOURCE',
      'economic_snapshot',jsonb_build_object(
        'schema_version','WEEKLY_SOURCE_CANONICAL_ECONOMIC_SNAPSHOT_V1',
        'calculator_version','WEEKLY_SHIFT_FINANCIAL_SEGMENT_V1',
        'source_mode','HEALTHROSTER_WEEKLY','rate_method','SPLIT_RATE_WINDOWS',
        'sign',1,'paid_minutes',450,'break_minutes',30,
        'bucket_minutes',jsonb_build_object('day',450,'night',0,'sat',0,'sun',0,'bh',0),
        'hours',jsonb_build_object('day',7.5,'night',0,'sat',0,'sun',0,'bh',0),
        'pay_rates',jsonb_build_object('day',10,'night',10,'sat',10,'sun',10,'bh',10),
        'charge_rates',jsonb_build_object('day',20,'night',20,'sat',20,'sun',20,'bh',20),
        'total_pay_pence','7500','calculated_charge_pence','15000'
      )
    ),
    jsonb_build_object(
      'upload_row_id','70000000-0000-4000-8000-000000000013',
      'mapping_state','RESOLVED',
      'candidate_id','70000000-0000-4000-8000-000000000003',
      'client_id','70000000-0000-4000-8000-000000000002',
      'contract_id','70000000-0000-4000-8000-000000000004',
      'contract_selection_method','AUTO_UNIQUE',
      'qualifying_contract_ids',jsonb_build_array('70000000-0000-4000-8000-000000000004'),
      'identity_kind','PROFILE_EXTERNAL_KEY','profile_external_key','line-2',
      'link_kind','POSITIVE_SOURCE',
      'economic_snapshot',jsonb_build_object(
        'schema_version','WEEKLY_SOURCE_CANONICAL_ECONOMIC_SNAPSHOT_V1',
        'calculator_version','WEEKLY_SHIFT_FINANCIAL_SEGMENT_V1',
        'source_mode','HEALTHROSTER_WEEKLY','rate_method','SPLIT_RATE_WINDOWS',
        'sign',1,'paid_minutes',450,'break_minutes',30,
        'bucket_minutes',jsonb_build_object('day',450,'night',0,'sat',0,'sun',0,'bh',0),
        'hours',jsonb_build_object('day',7.5,'night',0,'sat',0,'sun',0,'bh',0),
        'pay_rates',jsonb_build_object('day',10,'night',10,'sat',10,'sun',10,'bh',10),
        'charge_rates',jsonb_build_object('day',20,'night',20,'sat',20,'sun',20,'bh',20),
        'total_pay_pence','7500','calculated_charge_pence','15000'
      )
    ),
    jsonb_build_object(
      'upload_row_id','70000000-0000-4000-8000-000000000014',
      'mapping_state','RESOLVED',
      'candidate_id','70000000-0000-4000-8000-000000000003',
      'client_id','70000000-0000-4000-8000-000000000002',
      'contract_id','70000000-0000-4000-8000-000000000004',
      'contract_selection_method','AUTO_UNIQUE',
      'qualifying_contract_ids',jsonb_build_array('70000000-0000-4000-8000-000000000004'),
      'identity_kind','PROFILE_EXTERNAL_KEY','profile_external_key','line-3',
      'link_kind','ZERO_SOURCE'
    )
  )
);

do $verify$
begin
  if (select count(*) from public.weekly_source_row_resolutions
      where upload_row_id in (
        '70000000-0000-4000-8000-000000000010',
        '70000000-0000-4000-8000-000000000013',
        '70000000-0000-4000-8000-000000000014'
      ))<>3 then
    raise exception 'PROJECTION_RESOLUTION_NOT_CREATED';
  end if;
  if (select count(*) from public.weekly_work_events
      where candidate_id='70000000-0000-4000-8000-000000000003')<>3 then
    raise exception 'PROJECTION_WORK_EVENT_NOT_CREATED';
  end if;
  if (select count(*) from public.weekly_work_event_source_links)<>3 then
    raise exception 'PROJECTION_SOURCE_LINK_NOT_CREATED';
  end if;
  if (select count(*) from public.weekly_source_row_economic_snapshots)<>2
     or exists(
       select 1
       from public.weekly_source_row_economic_snapshots snapshot
       where snapshot.upload_row_id='70000000-0000-4000-8000-000000000014'
     ) then
    raise exception 'PROJECTION_ECONOMIC_SNAPSHOT_CARDINALITY_INVALID';
  end if;
  if exists(select 1 from public.timesheets)
     or exists(select 1 from public.weekly_source_billing_movements)
     or exists(select 1 from public.weekly_exceptional_pay_target_families) then
    raise exception 'PROJECTION_CROSSED_FINANCIAL_BOUNDARY';
  end if;
end;
$verify$;

-- Publish the exact projection, then prove the source Timesheet lineage owner
-- creates one ordinary base Weekly Timesheet and reuses it for every worked
-- row in the same Contract week.  A zero-source row receives durable source
-- identity only; it creates no Timesheet lineage, economics, billing or
-- payment movement.
update public.weekly_source_projection_publications
set state='CURRENT',published_at_utc=clock_timestamp()
where id='70000000-0000-4000-8000-000000000011';
update public.weekly_source_cycles
set projection_state='CURRENT',
    current_projection_publication_id='70000000-0000-4000-8000-000000000011'
where id='70000000-0000-4000-8000-000000000008';

select public.weekly_source_timesheet_lineage_ensure_atomic_v1(
  (select id from public.weekly_source_row_resolutions
   where upload_row_id='70000000-0000-4000-8000-000000000010'),
  '70000000-0000-4000-8000-000000000001'
);

do $zero_lineage_rejected$
begin
  begin
    perform public.weekly_source_timesheet_lineage_ensure_atomic_v1(
      (select id from public.weekly_source_row_resolutions
       where upload_row_id='70000000-0000-4000-8000-000000000014'),
      '70000000-0000-4000-8000-000000000001'
    );
    raise exception 'ZERO_SOURCE_TIMESHEET_LINEAGE_WAS_ACCEPTED';
  exception
    when sqlstate '55000' then
      if sqlerrm is distinct from 'WEEKLY_SOURCE_ZERO_HOUR_EXPENSE_LINEAGE_INVALID' then
        raise;
      end if;
  end;
end;
$zero_lineage_rejected$;
select public.weekly_source_timesheet_lineage_ensure_atomic_v1(
  (select id from public.weekly_source_row_resolutions
   where upload_row_id='70000000-0000-4000-8000-000000000010'),
  '70000000-0000-4000-8000-000000000001'
);

-- Model a nurse-signed check Timesheet arriving after the source container was
-- born.  Binding the next row in the same week must preserve it byte-for-byte;
-- source rows never replace candidate-submitted evidence.
update public.timesheets
set status='STORED'::public.timesheet_status_enum,
    actual_schedule_json='[{"date":"2026-09-07","start":"09:00","end":"17:00","break_minutes":30}]'::jsonb,
    r2_nurse_key='test-only/nurse-signature.png',
    img_sha256_nurse=repeat('a',64),
    updated_at=clock_timestamp()
where timesheet_id=(
  select timesheet_id from public.weekly_source_row_timesheet_lineages
  where row_resolution_id=(
    select id from public.weekly_source_row_resolutions
    where upload_row_id='70000000-0000-4000-8000-000000000010'
  )
);
update public.contract_weeks
set status='INVOICED'::public.contract_week_status_enum,
    updated_at=clock_timestamp()
where timesheet_id=(
  select timesheet_id from public.weekly_source_row_timesheet_lineages
  where row_resolution_id=(
    select id from public.weekly_source_row_resolutions
    where upload_row_id='70000000-0000-4000-8000-000000000010'
  )
);

select public.weekly_source_timesheet_lineage_ensure_atomic_v1(
  (select id from public.weekly_source_row_resolutions
   where upload_row_id='70000000-0000-4000-8000-000000000013'),
  '70000000-0000-4000-8000-000000000001'
);

do $lineage_verify$
begin
  if (select count(*) from public.weekly_source_row_timesheet_lineages)<>2 then
    raise exception 'PROJECTION_TIMESHEET_LINEAGE_CARDINALITY_INVALID';
  end if;
  if (select count(*) from public.contract_weeks)<>1
     or (select count(*) from public.timesheets)<>1 then
    raise exception 'PROJECTION_BASE_TIMESHEET_NOT_REUSED';
  end if;
  if exists(
    select 1 from public.contract_weeks
    where status<>'INVOICED'::public.contract_week_status_enum
  ) then
    raise exception 'PROJECTION_LINEAGE_REOPENED_INVOICED_WEEK';
  end if;
  if exists(
    select 1 from public.timesheets
    where actual_schedule_json is distinct from
          '[{"date":"2026-09-07","start":"09:00","end":"17:00","break_minutes":30}]'::jsonb
       or status<>'STORED'::public.timesheet_status_enum
       or authorised_at_server is not null
       or r2_nurse_key is distinct from 'test-only/nurse-signature.png'
       or img_sha256_nurse is distinct from repeat('a',64)
       or r2_auth_key is not null
  ) then
    raise exception 'PROJECTION_LINEAGE_MUTATED_EVIDENCE_OR_AUTHORITY';
  end if;
  if (select count(*) from public.audit_events
      where action='WEEKLY_SOURCE_BASE_TIMESHEET_CREATED')<>1 then
    raise exception 'PROJECTION_LINEAGE_AUDIT_CARDINALITY_INVALID';
  end if;
  if (select count(*) from public.audit_events
      where action='WEEKLY_SOURCE_TIMESHEET_LINEAGE_CREATED')<>2 then
    raise exception 'PROJECTION_SOURCE_LINEAGE_AUDIT_CARDINALITY_INVALID';
  end if;
  if exists(select 1 from public.weekly_source_billing_movements)
     or exists(select 1 from public.timesheets_financials)
     or exists(select 1 from public.pay_batch_items) then
    raise exception 'PROJECTION_LINEAGE_CROSSED_FINANCIAL_BOUNDARY';
  end if;
end;
$lineage_verify$;

select jsonb_build_object(
  'ok',true,'verification','weekly_source_projection_build_v1',
  'resolution_count',(select count(*) from public.weekly_source_row_resolutions),
  'work_event_count',(select count(*) from public.weekly_work_events),
  'timesheet_lineage_count',(select count(*) from public.weekly_source_row_timesheet_lineages),
  'ordinary_timesheet_count',(select count(*) from public.timesheets),
  'financial_writes',(
    select count(*) from public.weekly_source_billing_movements
  )
);

-- ---------------------------------------------------------------------------
-- Plan 6.2 additions (WP-04).  A second, independent scope proves:
--   G6-4  the Contract selection method is proved from server-derived facts
--         (24 §8: "The system must never offer a chooser when only one
--         Contract is eligible"; 24 §9 unique prior lineage);
--   G8-1/G8-2 (XSG-009) a signed-Timesheet-authority row is recorded as
--         production Mode A authority on
--         public.weekly_timesheet_authority_resolutions instead of being
--         merely labelled TIMESHEET_EVIDENCE (24 §14; 25 §9), with
--         "Reference required before pay" read from the installed contract
--         settings authority and the auto-authorisation decision taken from
--         the established public.import_auto_authorise_policy_resolve_v2.
-- ---------------------------------------------------------------------------
insert into public.clients(id,name)
values ('71000000-0000-4000-8000-000000000002','Plan 6.2 Mode A Client');
insert into public.client_settings(client_id,vat_rate_pct,effective_from)
values ('71000000-0000-4000-8000-000000000002',20,'2026-01-01');
insert into public.candidates(id,display_name)
values ('71000000-0000-4000-8000-000000000003','Plan 6.2 Mode A Candidate');
insert into public.contracts(
  id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
  weekly_timesheet_source,self_bill,no_timesheet_required,requires_hr,autoprocess_hr
) values (
  '71000000-0000-4000-8000-000000000004',
  '71000000-0000-4000-8000-000000000003',
  '71000000-0000-4000-8000-000000000002',
  '2026-01-01','2026-12-31','PAYE','{}'::jsonb,'HEALTHROSTER',false,false,true,true
),(
  '71000000-0000-4000-8000-000000000005',
  '71000000-0000-4000-8000-000000000003',
  '71000000-0000-4000-8000-000000000002',
  '2026-01-01','2026-12-31','PAYE','{}'::jsonb,'HEALTHROSTER',false,false,true,true
);
insert into public.weekly_source_groups(
  id,environment,agency_id,code,display_name,source_family,cutoff_weekday,cutoff_local_time
) values (
  '71000000-0000-4000-8000-000000000006','TEST',
  '71000000-0000-4000-8000-000000000007','PLAN62_MODE_A','Plan 6.2 Mode A','ROSTER',3,'15:00'
);
insert into public.weekly_source_group_clients(
  id,source_group_id,client_id,valid_from,created_by_user_id
) values (
  '71000000-0000-4000-8000-000000000008',
  '71000000-0000-4000-8000-000000000006',
  '71000000-0000-4000-8000-000000000002','2026-01-01',
  '70000000-0000-4000-8000-000000000001'
);
insert into public.weekly_source_client_policies(
  id,source_group_id,client_id,effective_from,authority_mode,document_mode,
  self_bill_enabled,weekly_rate_classification_method,manager_queries_enabled,
  manager_query_recipient,created_by_user_id
) values (
  '71000000-0000-4000-8000-000000000009',
  '71000000-0000-4000-8000-000000000006',
  '71000000-0000-4000-8000-000000000002','2026-01-01',
  'TIMESHEET_AUTHORITY','INVOICE_EVIDENCE_REQUIRED',false,'SPLIT_RATE_WINDOWS',true,
  'manager@example.test','70000000-0000-4000-8000-000000000001'
);
insert into public.weekly_source_cycles(
  id,source_group_id,finalisation_week_ending,cutoff_at_utc,state,version,projection_state
) values (
  '71000000-0000-4000-8000-00000000000a',
  '71000000-0000-4000-8000-000000000006','2026-09-13','2026-09-09T14:00:00Z',
  'OPEN',1,'REBUILDING'
);
insert into public.weekly_source_uploads(
  id,source_cycle_id,original_filename,content_sha256,byte_count,source_format_profile_id,
  parser_version,normaliser_version,header_coordinate_map_json,header_coordinate_map_hash,
  declared_scope_fingerprint,suggested_coverage_start_local_date,suggested_coverage_end_local_date,
  confirmed_coverage_start_local_date,confirmed_coverage_end_local_date,coverage_timezone,
  coverage_confirmation_version,coverage_confirmed_by_user_id,coverage_confirmed_at_utc,
  coverage_shrink_acknowledged,coverage_state,coverage_proof_kind,physical_row_count,
  accepted_count,row_manifest_hash,state,uploaded_by_user_id
) values (
  '71000000-0000-4000-8000-00000000000b',
  '71000000-0000-4000-8000-00000000000a','mode-a.csv',decode(repeat('11',32),'hex'),100,
  '35555555-5555-4555-8555-555555555555','PARSER_V1','NORMALISER_V1','{}',decode(repeat('12',32),'hex'),
  decode(repeat('13',32),'hex'),'2026-09-07','2026-09-07','2026-09-07','2026-09-07','Europe/London',
  'COMPLETE_EXPORT_V1','70000000-0000-4000-8000-000000000001',clock_timestamp(),false,'COMPLETE',
  'OFFICE_COMPLETE_EXPORT_ATTESTATION',1,1,decode(repeat('14',32),'hex'),'CURRENT',
  '70000000-0000-4000-8000-000000000001'
);
update public.weekly_source_cycles
set current_complete_upload_id='71000000-0000-4000-8000-00000000000b'
where id='71000000-0000-4000-8000-00000000000a';
insert into public.weekly_source_upload_rows(
  id,upload_id,source_row_ordinal,external_source_key,source_candidate_identity,
  source_client_identity,work_date,start_at_local,end_at_local,break_minutes,
  actual_net_minutes,row_finalisation_state,source_expense_pence,
  source_expense_parse_state,normalised_row_hash
) values (
  '71000000-0000-4000-8000-00000000000c',
  '71000000-0000-4000-8000-00000000000b',1,'mode-a-line-1','Plan 6.2 Mode A Candidate',
  'Plan 6.2 Mode A Client','2026-09-07','2026-09-07 09:00','2026-09-07 17:00',30,450,
  'NOT_APPLICABLE',0,'OMITTED_ZERO',decode(repeat('15',32),'hex')
);
insert into public.weekly_source_projection_publications(
  id,source_cycle_id,authority_scope_kind,upload_id,authority_scope_version,
  comparison_manifest_hash,issue_set_hash,state
) values (
  '71000000-0000-4000-8000-00000000000d',
  '71000000-0000-4000-8000-00000000000a','CYCLE',
  '71000000-0000-4000-8000-00000000000b',1,
  decode(repeat('16',32),'hex'),decode(repeat('17',32),'hex'),'BUILDING'
);

create function pg_temp.mode_a_apply(
  p_selection_method text,
  p_contract_id uuid,
  p_qualifying jsonb,
  p_prior_work_event_id uuid default null
) returns jsonb language sql as $mode_a_apply$
  select public.weekly_source_projection_rows_apply_atomic_v1(
    '70000000-0000-4000-8000-000000000001',
    '71000000-0000-4000-8000-00000000000d',
    pg_catalog.jsonb_build_array(pg_catalog.jsonb_strip_nulls(
      pg_catalog.jsonb_build_object(
        'upload_row_id','71000000-0000-4000-8000-00000000000c',
        'mapping_state','RESOLVED',
        'candidate_id','71000000-0000-4000-8000-000000000003',
        'client_id','71000000-0000-4000-8000-000000000002',
        'contract_id',p_contract_id,
        'prior_work_event_id',p_prior_work_event_id,
        'contract_selection_method',p_selection_method,
        'qualifying_contract_ids',p_qualifying,
        'identity_kind','PROFILE_EXTERNAL_KEY',
        'profile_external_key','mode-a-line-1',
        'link_kind','TIMESHEET_EVIDENCE'
      )
    ))
  );
$mode_a_apply$;

do $selection_method_proof$
declare
  v_both constant jsonb:=pg_catalog.jsonb_build_array(
    '71000000-0000-4000-8000-000000000004','71000000-0000-4000-8000-000000000005'
  );
  v_one constant jsonb:=pg_catalog.jsonb_build_array('71000000-0000-4000-8000-000000000004');
begin
  -- G6-4: AUTO_UNIQUE is only provable when exactly one Contract qualified.
  begin
    perform pg_temp.mode_a_apply('AUTO_UNIQUE','71000000-0000-4000-8000-000000000004',v_both);
    raise exception 'AUTO_UNIQUE_WITH_TWO_QUALIFYING_CONTRACTS_WAS_ACCEPTED';
  exception when sqlstate '22023' then
    if sqlerrm is distinct from 'WEEKLY_SOURCE_AUTO_UNIQUE_NOT_UNIQUE' then raise; end if;
  end;
  -- G6-4: a chooser is never warranted when only one Contract is eligible.
  begin
    perform pg_temp.mode_a_apply('OFFICE_SELECTED','71000000-0000-4000-8000-000000000004',v_one);
    raise exception 'OFFICE_SELECTED_WITH_ONE_QUALIFYING_CONTRACT_WAS_ACCEPTED';
  exception when sqlstate '22023' then
    if sqlerrm is distinct from 'WEEKLY_SOURCE_OFFICE_CHOICE_NOT_WARRANTED' then raise; end if;
  end;
  -- G6-4: durable lineage must be provable from a prior resolved mapping.
  begin
    perform pg_temp.mode_a_apply('DURABLE_LINEAGE','71000000-0000-4000-8000-000000000004',v_both);
    raise exception 'DURABLE_LINEAGE_WITHOUT_PRIOR_RESOLUTION_WAS_ACCEPTED';
  exception when sqlstate '22023' then
    if sqlerrm is distinct from 'WEEKLY_SOURCE_DURABLE_LINEAGE_UNPROVEN' then raise; end if;
  end;
  if exists(
    select 1 from public.weekly_source_row_resolutions
    where upload_row_id='71000000-0000-4000-8000-00000000000c'
  ) then
    raise exception 'REFUSED_SELECTION_METHOD_LEFT_A_RESOLUTION';
  end if;
  if exists(select 1 from public.weekly_timesheet_authority_resolutions) then
    raise exception 'REFUSED_SELECTION_METHOD_LEFT_A_MODE_A_RESOLUTION';
  end if;
  -- The Office decision between two qualifying Contracts is accepted.
  perform pg_temp.mode_a_apply('OFFICE_SELECTED','71000000-0000-4000-8000-000000000005',v_both);
end;
$selection_method_proof$;

do $mode_a_proof$
declare
  v_resolution public.weekly_timesheet_authority_resolutions%rowtype;
begin
  -- G8-1/G8-2: production code, not a verification fixture, now writes the
  -- Mode A authority resolution.
  if (select count(*) from public.weekly_timesheet_authority_resolutions)<>1 then
    raise exception 'MODE_A_AUTHORITY_RESOLUTION_NOT_WRITTEN_EXACTLY_ONCE';
  end if;
  select * into strict v_resolution from public.weekly_timesheet_authority_resolutions;
  if v_resolution.source_cycle_id is distinct from '71000000-0000-4000-8000-00000000000a'
     or v_resolution.client_id is distinct from '71000000-0000-4000-8000-000000000002'
     or v_resolution.contract_id is distinct from '71000000-0000-4000-8000-000000000005'
     or v_resolution.work_date is distinct from '2026-09-07'::date
     or v_resolution.authority_mode is distinct from 'TIMESHEET_AUTHORITY'
     or v_resolution.document_mode is distinct from 'INVOICE_EVIDENCE_REQUIRED' then
    raise exception 'MODE_A_AUTHORITY_RESOLUTION_SCOPE_INVALID';
  end if;
  -- 25 §9 and 24 §14: "Reference required before pay" defaults to false.
  if v_resolution.require_reference_to_pay is not false then
    raise exception 'MODE_A_REFERENCE_REQUIRED_BEFORE_PAY_DID_NOT_DEFAULT_FALSE';
  end if;
  -- Mode A never creates source finalisation, self-bill or protected-pay work.
  if exists(select 1 from public.weekly_source_row_economic_snapshots
            where upload_row_id='71000000-0000-4000-8000-00000000000c')
     or exists(select 1 from public.weekly_source_charge_checks
               where upload_row_id='71000000-0000-4000-8000-00000000000c')
     or exists(select 1 from public.weekly_source_contract_qualification_observations
               where upload_row_id='71000000-0000-4000-8000-00000000000c') then
    raise exception 'MODE_A_ROW_CROSSED_INTO_SOURCE_AUTHORITY_ECONOMICS';
  end if;
  if exists(
    select 1 from public.weekly_source_row_resolutions
    where upload_row_id='71000000-0000-4000-8000-00000000000c'
      and contract_selection_method is distinct from 'OFFICE_SELECTED'
  ) then
    raise exception 'MODE_A_SELECTION_METHOD_NOT_RECORDED';
  end if;
end;
$mode_a_proof$;

-- ---------------------------------------------------------------------------
-- Plan 6.2 G6-2: the database re-derives the NHSP_TWO_COMPONENT_PENCE_V1
-- verdict from its own facts and never accepts the caller's word for it.
-- Authority: 25 §6; 24 §13; 14 §4.3 items 1 and 5; NHSP-BR-013; PRC-006,
-- PRC-007, PRC-020, NHSBR-019, NHSBR-020.
-- ---------------------------------------------------------------------------
insert into public.clients(id,name)
values ('72000000-0000-4000-8000-000000000002','Plan 6.2 Penny Trust');
insert into public.client_settings(client_id,vat_rate_pct,effective_from)
values ('72000000-0000-4000-8000-000000000002',20,'2026-01-01');
insert into public.candidates(id,display_name)
values ('72000000-0000-4000-8000-000000000003','Plan 6.2 Penny Candidate');
insert into public.contracts(
  id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
  weekly_timesheet_source,self_bill,no_timesheet_required,requires_hr,autoprocess_hr,is_nhsp
) values (
  '72000000-0000-4000-8000-000000000004',
  '72000000-0000-4000-8000-000000000003',
  '72000000-0000-4000-8000-000000000002',
  '2026-01-01','2026-12-31','PAYE','{}'::jsonb,'NHSP',true,true,true,true,true
);
insert into public.weekly_source_groups(
  id,environment,agency_id,code,display_name,source_family,cutoff_weekday,cutoff_local_time,
  nhsp_report_heading_name
) values (
  '72000000-0000-4000-8000-000000000006','TEST',
  '72000000-0000-4000-8000-000000000007','PLAN62_PENNY','Plan 6.2 Penny','NHSP',3,'15:00',
  'Plan 6.2 Penny Backing Report'
);
insert into public.weekly_source_group_clients(
  id,source_group_id,client_id,valid_from,created_by_user_id
) values (
  '72000000-0000-4000-8000-000000000008',
  '72000000-0000-4000-8000-000000000006',
  '72000000-0000-4000-8000-000000000002','2026-01-01',
  '70000000-0000-4000-8000-000000000001'
);
insert into public.weekly_source_client_policies(
  id,source_group_id,client_id,effective_from,authority_mode,document_mode,
  self_bill_enabled,weekly_rate_classification_method,manager_queries_enabled,
  manager_query_recipient,created_by_user_id
) values (
  '72000000-0000-4000-8000-000000000009',
  '72000000-0000-4000-8000-000000000006',
  '72000000-0000-4000-8000-000000000002','2026-01-01',
  'SOURCE_AUTHORITY','CHECK_ONLY',true,'SPLIT_RATE_WINDOWS',true,
  'manager@example.test','70000000-0000-4000-8000-000000000001'
);
insert into public.weekly_source_cycles(
  id,source_group_id,finalisation_week_ending,cutoff_at_utc,state,version,projection_state
) values (
  '72000000-0000-4000-8000-00000000000a',
  '72000000-0000-4000-8000-000000000006','2026-09-13','2026-09-09T14:00:00Z',
  'OPEN',1,'REBUILDING'
),(
  '72000000-0000-4000-8000-000000000010',
  '72000000-0000-4000-8000-000000000006','2026-09-20','2026-09-16T14:00:00Z',
  'OPEN',1,'REBUILDING'
),(
  '72000000-0000-4000-8000-000000000014',
  '72000000-0000-4000-8000-000000000006','2026-09-27','2026-09-23T14:00:00Z',
  'OPEN',1,'REBUILDING'
);
insert into public.weekly_source_uploads(
  id,source_cycle_id,original_filename,content_sha256,byte_count,source_format_profile_id,
  parser_version,normaliser_version,header_coordinate_map_json,header_coordinate_map_hash,
  declared_scope_fingerprint,suggested_coverage_start_local_date,suggested_coverage_end_local_date,
  confirmed_coverage_start_local_date,confirmed_coverage_end_local_date,coverage_timezone,
  coverage_confirmation_version,coverage_confirmed_by_user_id,coverage_confirmed_at_utc,
  coverage_shrink_acknowledged,coverage_state,coverage_proof_kind,physical_row_count,
  accepted_count,row_manifest_hash,state,uploaded_by_user_id
) values (
  '72000000-0000-4000-8000-00000000000b',
  '72000000-0000-4000-8000-00000000000a','penny-exact.csv',decode(repeat('21',32),'hex'),100,
  '32222222-2222-4222-8222-222222222222','PARSER_V1','NORMALISER_V1','{}',decode(repeat('22',32),'hex'),
  decode(repeat('23',32),'hex'),'2026-09-07','2026-09-07','2026-09-07','2026-09-07','Europe/London',
  'COMPLETE_EXPORT_V1','70000000-0000-4000-8000-000000000001',clock_timestamp(),false,'COMPLETE',
  'OFFICE_COMPLETE_EXPORT_ATTESTATION',1,1,decode(repeat('24',32),'hex'),'CURRENT',
  '70000000-0000-4000-8000-000000000001'
),(
  '72000000-0000-4000-8000-000000000011',
  '72000000-0000-4000-8000-000000000010','penny-below.csv',decode(repeat('25',32),'hex'),100,
  '32222222-2222-4222-8222-222222222222','PARSER_V1','NORMALISER_V1','{}',decode(repeat('26',32),'hex'),
  decode(repeat('27',32),'hex'),'2026-09-14','2026-09-14','2026-09-14','2026-09-14','Europe/London',
  'COMPLETE_EXPORT_V1','70000000-0000-4000-8000-000000000001',clock_timestamp(),false,'COMPLETE',
  'OFFICE_COMPLETE_EXPORT_ATTESTATION',1,1,decode(repeat('28',32),'hex'),'CURRENT',
  '70000000-0000-4000-8000-000000000001'
),(
  '72000000-0000-4000-8000-000000000015',
  '72000000-0000-4000-8000-000000000014','zero-source-charge.csv',decode(repeat('31',32),'hex'),100,
  '32222222-2222-4222-8222-222222222222','PARSER_V1','NORMALISER_V1','{}',decode(repeat('32',32),'hex'),
  decode(repeat('33',32),'hex'),'2026-09-21','2026-09-21','2026-09-21','2026-09-21','Europe/London',
  'COMPLETE_EXPORT_V1','70000000-0000-4000-8000-000000000001',clock_timestamp(),false,'COMPLETE',
  'OFFICE_COMPLETE_EXPORT_ATTESTATION',1,1,decode(repeat('34',32),'hex'),'CURRENT',
  '70000000-0000-4000-8000-000000000001'
);
update public.weekly_source_cycles
set current_complete_upload_id='72000000-0000-4000-8000-00000000000b'
where id='72000000-0000-4000-8000-00000000000a';
update public.weekly_source_cycles
set current_complete_upload_id='72000000-0000-4000-8000-000000000011'
where id='72000000-0000-4000-8000-000000000010';
update public.weekly_source_cycles
set current_complete_upload_id='72000000-0000-4000-8000-000000000015'
where id='72000000-0000-4000-8000-000000000014';
insert into public.weekly_source_upload_rows(
  id,upload_id,source_row_ordinal,external_source_key,source_candidate_identity,
  source_client_identity,work_date,start_at_local,end_at_local,break_minutes,
  actual_net_minutes,row_finalisation_state,role_band_source,
  source_commission_pence,source_total_cost_pence,source_shift_charge_pence,
  source_money_parse_state,source_qualification_profile_version,
  source_expense_parse_state,normalised_row_hash
) values (
  '72000000-0000-4000-8000-00000000000c',
  '72000000-0000-4000-8000-00000000000b',1,'NHSP-PENNY-1',
  'Plan 6.2 Penny Candidate','Plan 6.2 Penny Trust','2026-09-07',
  '2026-09-07 09:00','2026-09-07 17:00',30,450,'SOURCE_WORKED','BAND 5',
  500,14500,15000,'VALID','NHSP_TWO_COMPONENT_PENCE_V1','NOT_APPLICABLE',
  decode(repeat('29',32),'hex')
),(
  '72000000-0000-4000-8000-000000000012',
  '72000000-0000-4000-8000-000000000011',1,'NHSP-PENNY-2',
  'Plan 6.2 Penny Candidate','Plan 6.2 Penny Trust','2026-09-14',
  '2026-09-14 09:00','2026-09-14 17:00',30,450,'SOURCE_WORKED','BAND 5',
  500,14499,14999,'VALID','NHSP_TWO_COMPONENT_PENCE_V1','NOT_APPLICABLE',
  decode(repeat('2a',32),'hex')
),(
  '72000000-0000-4000-8000-000000000016',
  '72000000-0000-4000-8000-000000000015',1,'NHSP-ZERO-1',
  'Plan 6.2 Penny Candidate','Plan 6.2 Penny Trust','2026-09-21',
  '2026-09-21 09:00','2026-09-21 17:00',30,450,'SOURCE_WORKED','BAND 5',
  0,0,0,'VALID','NHSP_TWO_COMPONENT_PENCE_V1','NOT_APPLICABLE',
  decode(repeat('35',32),'hex')
);
insert into public.weekly_source_projection_publications(
  id,source_cycle_id,authority_scope_kind,upload_id,authority_scope_version,
  comparison_manifest_hash,issue_set_hash,state
) values (
  '72000000-0000-4000-8000-00000000000d',
  '72000000-0000-4000-8000-00000000000a','CYCLE',
  '72000000-0000-4000-8000-00000000000b',1,
  decode(repeat('2b',32),'hex'),decode(repeat('2c',32),'hex'),'BUILDING'
),(
  '72000000-0000-4000-8000-000000000013',
  '72000000-0000-4000-8000-000000000010','CYCLE',
  '72000000-0000-4000-8000-000000000011',1,
  decode(repeat('2d',32),'hex'),decode(repeat('2e',32),'hex'),'BUILDING'
),(
  '72000000-0000-4000-8000-000000000017',
  '72000000-0000-4000-8000-000000000014','CYCLE',
  '72000000-0000-4000-8000-000000000015',1,
  decode(repeat('36',32),'hex'),decode(repeat('37',32),'hex'),'BUILDING'
);

create function pg_temp.penny_apply(
  p_publication_id uuid,
  p_upload_row_id uuid,
  p_external_key text,
  p_commission bigint,
  p_total_cost bigint,
  p_source_charge bigint,
  p_row_sign_kind text,
  p_comparison_result text,
  p_qualification_result text default null
) returns jsonb language sql as $penny_apply$
  select public.weekly_source_projection_rows_apply_atomic_v1(
    '70000000-0000-4000-8000-000000000001',
    p_publication_id,
    pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object(
        'upload_row_id',p_upload_row_id,
        'mapping_state','RESOLVED',
        'candidate_id','72000000-0000-4000-8000-000000000003',
        'client_id','72000000-0000-4000-8000-000000000002',
        'contract_id','72000000-0000-4000-8000-000000000004',
        'contract_selection_method','AUTO_UNIQUE',
        'qualifying_contract_ids',
          pg_catalog.jsonb_build_array('72000000-0000-4000-8000-000000000004'),
        'identity_kind','SCHEDULE_TUPLE',
        'link_kind','POSITIVE_SOURCE',
        'economic_snapshot',pg_catalog.jsonb_build_object(
          'schema_version','WEEKLY_SOURCE_CANONICAL_ECONOMIC_SNAPSHOT_V1',
          'calculator_version','WEEKLY_SHIFT_FINANCIAL_SEGMENT_V1','source_mode','NHSP_WEEKLY',
          'rate_method','SPLIT_RATE_WINDOWS','sign',1,'paid_minutes',450,'break_minutes',30,
          'bucket_minutes',pg_catalog.jsonb_build_object('day',450,'night',0,'sat',0,'sun',0,'bh',0),
          'hours',pg_catalog.jsonb_build_object('day',7.5,'night',0,'sat',0,'sun',0,'bh',0),
          'pay_rates',pg_catalog.jsonb_build_object('day',10,'night',10,'sat',10,'sun',10,'bh',10),
          'charge_rates',pg_catalog.jsonb_build_object('day',20,'night',20,'sat',20,'sun',20,'bh',20),
          'total_pay_pence','7500','calculated_charge_pence','15000'
        ),
        'qualification_observations',case when p_qualification_result is null
          then '[]'::jsonb
          else pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
            'contract_id','72000000-0000-4000-8000-000000000004',
            'contract_revision_fingerprint',repeat('ab',32),
            'source_shift_charge_pence',p_source_charge::text,
            'canonical_calculated_pence','15000',
            'comparison_result',p_qualification_result,
            'reason_codes',pg_catalog.jsonb_build_array(p_qualification_result)
          )) end,
        'charge_check',pg_catalog.jsonb_build_object(
          'row_sign_kind',p_row_sign_kind,
          'source_commission_pence',p_commission::text,
          'source_total_cost_pence',p_total_cost::text,
          'source_shift_charge_pence',p_source_charge::text,
          'calculated_segment_charge_pence','15000',
          'comparison_result',p_comparison_result,
          'comparison_reason_code',p_comparison_result,
          'phase_severity',case
            when p_comparison_result in ('MISMATCH','ZERO_SOURCE_CHARGE')
              then 'PROVISIONAL_WARNING'
            else 'NONE'
          end
        )
      )
    )
  );
$penny_apply$;

-- WP-58 / WP-50 finding F7. The NHSP identity guard, driven.
--
-- Pack 24 section 1 supersedes "the NHSP Reference Number is the durable
-- identity of the real shift", and 24 section 9 says the Reference Number is
-- "not sole durable work identity". An NHSP shift is the same shift as another
-- by the schedule tuple, never by the reference; reference-keyed identity turns
-- one corrected shift into three work events. Before WP-58 the only thing
-- sending SCHEDULE_TUPLE for NHSP was one ternary in
-- broker/src/weekly-source/upload-publication-owner.mjs, and the database
-- accepted either kind from anyone. It no longer does.
--
-- This block exists so the guard is DRIVEN (standing rule 17): delete the
-- guard from weekly_source_projection_rows_apply_atomic_v1 and this fails.
-- The upload here carries NHSP_FINAL_BACKING_V1, and the refusal is a
-- subtransaction rollback, so it leaves no resolution, work event or charge
-- check behind -- which the next block re-checks for these same rows.
create function pg_temp.nhsp_identity_apply(p_identity_kind text)
returns jsonb language sql as $nhsp_identity_apply$
  select public.weekly_source_projection_rows_apply_atomic_v1(
    '70000000-0000-4000-8000-000000000001',
    '72000000-0000-4000-8000-00000000000d',
    pg_catalog.jsonb_build_array(pg_catalog.jsonb_strip_nulls(
      pg_catalog.jsonb_build_object(
        'upload_row_id','72000000-0000-4000-8000-00000000000c',
        'mapping_state','RESOLVED',
        'candidate_id','72000000-0000-4000-8000-000000000003',
        'client_id','72000000-0000-4000-8000-000000000002',
        'contract_id','72000000-0000-4000-8000-000000000004',
        'contract_selection_method','AUTO_UNIQUE',
        'qualifying_contract_ids',
          pg_catalog.jsonb_build_array('72000000-0000-4000-8000-000000000004'),
        'identity_kind',p_identity_kind,
        'profile_external_key',
          case when p_identity_kind='PROFILE_EXTERNAL_KEY' then 'NHSP-PENNY-1' end,
        'link_kind','POSITIVE_SOURCE',
        'economic_snapshot',pg_catalog.jsonb_build_object(
          'schema_version','WEEKLY_SOURCE_CANONICAL_ECONOMIC_SNAPSHOT_V1',
          'calculator_version','WEEKLY_SHIFT_FINANCIAL_SEGMENT_V1','source_mode','NHSP_WEEKLY',
          'rate_method','SPLIT_RATE_WINDOWS','sign',1,'paid_minutes',450,'break_minutes',30,
          'bucket_minutes',pg_catalog.jsonb_build_object('day',450,'night',0,'sat',0,'sun',0,'bh',0),
          'hours',pg_catalog.jsonb_build_object('day',7.5,'night',0,'sat',0,'sun',0,'bh',0),
          'pay_rates',pg_catalog.jsonb_build_object('day',10,'night',10,'sat',10,'sun',10,'bh',10),
          'charge_rates',pg_catalog.jsonb_build_object('day',20,'night',20,'sat',20,'sun',20,'bh',20),
          'total_pay_pence','7500','calculated_charge_pence','15000'
        ),
        'charge_check',pg_catalog.jsonb_build_object(
          'row_sign_kind','POSITIVE','source_commission_pence','500',
          'source_total_cost_pence','14500','source_shift_charge_pence','15000',
          'calculated_segment_charge_pence','15000','comparison_result','EXACT',
          'comparison_reason_code','EXACT','phase_severity','NONE'
        )
      )
    ))
  );
$nhsp_identity_apply$;

do $nhsp_identity_guard_proof$
declare
  v_events_before bigint;
begin
  select pg_catalog.count(*) into v_events_before from public.weekly_work_events;
  begin
    perform pg_temp.nhsp_identity_apply('PROFILE_EXTERNAL_KEY');
    raise exception 'NHSP_REFERENCE_KEYED_IDENTITY_WAS_ACCEPTED';
  exception when sqlstate '22023' then
    if sqlerrm is distinct from 'WEEKLY_SOURCE_SCHEDULE_TUPLE_EVENT_KEY_REQUIRED' then raise; end if;
  end;
  if (select pg_catalog.count(*) from public.weekly_work_events)<>v_events_before then
    raise exception 'A_REFUSED_NHSP_IDENTITY_CREATED_A_WORK_EVENT';
  end if;
  if exists(
    select 1 from public.weekly_work_events
    where source_format_profile_id='32222222-2222-4222-8222-222222222222'
      and identity_kind<>'SCHEDULE_TUPLE'
  ) then
    raise exception 'AN_NHSP_WORK_EVENT_CARRIES_A_NON_SCHEDULE_TUPLE_IDENTITY';
  end if;
  -- The identity-kind whitelist beside it, also previously undriven.
  begin
    perform pg_temp.nhsp_identity_apply('REFERENCE_NUMBER');
    raise exception 'AN_UNKNOWN_WORK_EVENT_IDENTITY_KIND_WAS_ACCEPTED';
  exception when sqlstate '22023' then
    if sqlerrm is distinct from 'WEEKLY_SOURCE_WORK_EVENT_IDENTITY_INVALID' then raise; end if;
  end;
  -- WP-58 handoff: the mirror guard this one was modelled on,
  -- WEEKLY_SOURCE_PROFILE_EXTERNAL_EVENT_KEY_REQUIRED, is still undriven. It
  -- cannot be driven from the Mode A fixture above, because that publication
  -- already carries generation 1 and the owner then returns
  -- {"idempotent":true} without revalidating a single row. Driving it needs a
  -- second, un-applied ROSTER publication, which is new fixture surface rather
  -- than a correction, so WP-58 recorded it instead of inventing it here.
end;
$nhsp_identity_guard_proof$;

do $penny_rederivation_proof$
declare
  v_state text;
  v_message text;
  v_constraint text;
begin
  -- A caller may not upgrade an EXACT row to the one-penny profile.
  begin
    perform pg_temp.penny_apply(
      '72000000-0000-4000-8000-00000000000d','72000000-0000-4000-8000-00000000000c',
      'NHSP-PENNY-1',500,14500,15000,'POSITIVE','SOURCE_ROUNDING_EQUIVALENT');
    raise exception 'CALLER_UPGRADED_EXACT_TO_ROUNDING_EQUIVALENT';
  exception when sqlstate '22023' then
    if sqlerrm is distinct from 'WEEKLY_SOURCE_CHARGE_CHECK_RESULT_NOT_REDERIVED' then raise; end if;
  end;
  -- A caller may not claim EXACT when the server re-derives a mismatch.
  begin
    perform pg_temp.penny_apply(
      '72000000-0000-4000-8000-000000000013','72000000-0000-4000-8000-000000000012',
      'NHSP-PENNY-2',500,14499,14999,'POSITIVE','EXACT');
    raise exception 'CALLER_CLAIMED_EXACT_ON_A_ONE_PENNY_ROW';
  exception when sqlstate '22023' then
    if sqlerrm is distinct from 'WEEKLY_SOURCE_CHARGE_CHECK_RESULT_NOT_REDERIVED' then raise; end if;
  end;
  -- The row sign kind is re-derived too.
  begin
    perform pg_temp.penny_apply(
      '72000000-0000-4000-8000-00000000000d','72000000-0000-4000-8000-00000000000c',
      'NHSP-PENNY-1',500,14500,15000,'FULL_NEGATIVE','EXACT');
    raise exception 'CALLER_CLAIMED_THE_WRONG_ROW_SIGN_KIND';
  exception when sqlstate '22023' then
    if sqlerrm is distinct from 'WEEKLY_SOURCE_CHARGE_CHECK_SIGN_NOT_REDERIVED' then raise; end if;
  end;
  -- A per-Contract qualification verdict is re-derived on the same rule.
  begin
    perform pg_temp.penny_apply(
      '72000000-0000-4000-8000-00000000000d','72000000-0000-4000-8000-00000000000c',
      'NHSP-PENNY-1',500,14500,15000,'POSITIVE','EXACT','MISMATCH');
    raise exception 'CALLER_CLAIMED_A_FALSE_QUALIFICATION_VERDICT';
  exception when sqlstate '22023' then
    if sqlerrm is distinct from 'WEEKLY_SOURCE_QUALIFICATION_RESULT_NOT_REDERIVED' then raise; end if;
  end;
  if exists(
    select 1 from public.weekly_source_charge_checks
    where upload_row_id in (
      '72000000-0000-4000-8000-00000000000c','72000000-0000-4000-8000-000000000012'
    )
  ) then
    raise exception 'A_REFUSED_CHARGE_CHECK_WAS_PERSISTED';
  end if;

  -- The truthful EXACT row is admitted and stored with the server's own values.
  perform pg_temp.penny_apply(
    '72000000-0000-4000-8000-00000000000d','72000000-0000-4000-8000-00000000000c',
    'NHSP-PENNY-1',500,14500,15000,'POSITIVE','EXACT','EXACT');
  if not exists(
    select 1 from public.weekly_source_charge_checks
    where upload_row_id='72000000-0000-4000-8000-00000000000c'
      and comparison_result='EXACT' and row_sign_kind='POSITIVE'
      and source_charge_difference_pence=0
      and source_shift_charge_pence=15000 and calculated_segment_charge_pence=15000
  ) then
    raise exception 'EXACT_CHARGE_CHECK_NOT_STORED_FROM_SERVER_FACTS';
  end if;
  if not exists(
    select 1 from public.weekly_source_contract_qualification_observations
    where upload_row_id='72000000-0000-4000-8000-00000000000c'
      and comparison_result='EXACT' and qualification_passed
      and source_charge_difference_pence=0
  ) then
    raise exception 'EXACT_QUALIFICATION_OBSERVATION_NOT_STORED_FROM_SERVER_FACTS';
  end if;

  -- A valid £0 source amount is not a reversal.  It keeps sign +1 for the
  -- worked-time/pay calculation and is classified as the Office-acceptible
  -- rate-card warning rather than being rejected as an invalid economic row.
  perform pg_temp.penny_apply(
    '72000000-0000-4000-8000-000000000017','72000000-0000-4000-8000-000000000016',
    'NHSP-ZERO-1',0,0,0,'POSITIVE','ZERO_SOURCE_CHARGE','ZERO_SOURCE_CHARGE');
  if not exists(
    select 1
    from public.weekly_source_charge_checks charge
    join public.weekly_source_row_economic_snapshots economic
      on economic.row_resolution_id=charge.row_resolution_id
    where charge.upload_row_id='72000000-0000-4000-8000-000000000016'
      and charge.comparison_result='ZERO_SOURCE_CHARGE'
      and charge.row_sign_kind='POSITIVE'
      and charge.source_shift_charge_pence=0
      and charge.calculated_segment_charge_pence=15000
      and economic.row_sign=1
      and economic.total_pay_pence=7500
  ) then
    raise exception 'ZERO_SOURCE_CHARGE_DID_NOT_RETAIN_POSITIVE_WORKED_ECONOMICS';
  end if;

  -- Plan 6.2 newly admits a positive source one penny BELOW the calculation.
  -- The owner's re-derivation accepts it; the only remaining refusal is the
  -- directional CHECK still carried by the unapplied schema migration
  -- (supabase/migrations/15092026_1534_weekly_source_plan6_schema.sql,
  -- constraint weekly_source_charge_checks_check4), which is owned by the
  -- schema work package.  This proof passes before and after that change and
  -- names the blocker when it is still present.
  begin
    perform pg_temp.penny_apply(
      '72000000-0000-4000-8000-000000000013','72000000-0000-4000-8000-000000000012',
      'NHSP-PENNY-2',500,14499,14999,'POSITIVE','SOURCE_ROUNDING_EQUIVALENT','SOURCE_ROUNDING_EQUIVALENT');
    if not exists(
      select 1 from public.weekly_source_charge_checks
      where upload_row_id='72000000-0000-4000-8000-000000000012'
        and comparison_result='SOURCE_ROUNDING_EQUIVALENT'
        and source_charge_difference_pence=-1
    ) then
      raise exception 'SYMMETRIC_ONE_PENNY_ROW_NOT_STORED';
    end if;
  exception
    when sqlstate '22023' then
      raise exception 'SYMMETRIC_ONE_PENNY_ROW_REFUSED_BY_THE_OWNER: %',sqlerrm;
    when sqlstate '23514' then
      get stacked diagnostics v_constraint=constraint_name,v_message=message_text;
      if v_constraint is distinct from 'weekly_source_charge_checks_check4' then
        raise exception 'SYMMETRIC_ONE_PENNY_ROW_REFUSED_BY_AN_UNEXPECTED_CONSTRAINT: % / %',
          v_constraint,v_message;
      end if;
      raise notice 'PLAN_6_2_PENDING: the owner admits the symmetric one-penny row; % still refuses it',
        v_constraint;
  end;
end;
$penny_rederivation_proof$;

select jsonb_build_object(
  'ok',true,'verification','weekly_source_projection_build_v1_plan62',
  'mode_a_authority_resolutions',(
    select count(*) from public.weekly_timesheet_authority_resolutions
  ),
  'nhsp_charge_checks',(
    select count(*) from public.weekly_source_charge_checks
  ),
  'nhsp_qualification_observations',(
    select count(*) from public.weekly_source_contract_qualification_observations
  )
);

rollback;
