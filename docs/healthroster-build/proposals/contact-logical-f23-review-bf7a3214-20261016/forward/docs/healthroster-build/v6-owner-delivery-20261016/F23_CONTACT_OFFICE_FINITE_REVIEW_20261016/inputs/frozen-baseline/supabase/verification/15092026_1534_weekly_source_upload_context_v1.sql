-- Rollback-only PG17 and ACL proof for weekly_source_upload_context_v1.
-- Prerequisites: Plan 6 schema, private classifiers, settings authority,
-- upload/publication, projection build, then upload context.

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
  pg_catalog.to_regprocedure('public.weekly_source_upload_context_v1(jsonb)') is not null,
  'upload context function is missing'
);
select pg_temp.assert_true(
  pg_catalog.has_function_privilege('service_role','public.weekly_source_upload_context_v1(jsonb)','EXECUTE'),
  'service_role must execute upload context'
);
select pg_temp.assert_true(
  not pg_catalog.has_function_privilege('anon','public.weekly_source_upload_context_v1(jsonb)','EXECUTE')
  and not pg_catalog.has_function_privilege('authenticated','public.weekly_source_upload_context_v1(jsonb)','EXECUTE'),
  'browser roles must not execute upload context'
);
select pg_temp.assert_true(
  (select p.prosecdef and p.provolatile='s' and p.proconfig @> array['search_path=public, private, extensions, pg_catalog, pg_temp']
   from pg_catalog.pg_proc p
   where p.oid='public.weekly_source_upload_context_v1(jsonb)'::pg_catalog.regprocedure),
  'upload context must be stable, security definer and search-path pinned'
);

insert into public.tms_users(
  id,email,role,is_active,password_hash,payment_authoriser,payment_golden_key
) values (
  '91000000-0000-4000-8000-000000000001',
  'weekly-upload-context-verification@example.invalid','admin',true,'not-a-login',false,false
);
insert into public.clients(id,cli_ref,name) values
  ('91000000-0000-4000-8000-000000000020','CLI-99994','Roster Context Client'),
  ('91000000-0000-4000-8000-000000000040','CLI-99995','NHSP Context Trust');
insert into public.settings_defaults(
  id,candidate_manager_email_templates_sha256,candidate_home_announcement_sha256
) values (
  1,decode(repeat('01',32),'hex'),decode(repeat('02',32),'hex')
) on conflict (id) do nothing;
insert into public.client_settings(client_id,vat_rate_pct,effective_from) values
  ('91000000-0000-4000-8000-000000000020',20,'2026-01-01');
insert into public.candidates(id,tms_ref,display_name,nhsp_hr_name_aliases) values (
  '91000000-0000-4000-8000-000000000060','CCR-99999','Exact Saved Candidate',
  pg_catalog.jsonb_build_array('Exact Saved Candidate')
);
insert into public.contracts(
  id,client_id,candidate_id,start_date,end_date,pay_method_snapshot,rates_json
) values
  ('91000000-0000-4000-8000-000000000061','91000000-0000-4000-8000-000000000020',
   '91000000-0000-4000-8000-000000000060','2026-01-01','2026-12-31','PAYE',
   '{"paye_day":10,"paye_night":10,"paye_sat":10,"paye_sun":10,"paye_bh":10,"charge_day":20,"charge_night":20,"charge_sat":20,"charge_sun":20,"charge_bh":20}'::jsonb),
  ('91000000-0000-4000-8000-000000000062','91000000-0000-4000-8000-000000000020',
   '91000000-0000-4000-8000-000000000060','2026-01-01','2026-12-31','PAYE',
   '{"paye_day":11,"paye_night":11,"paye_sat":11,"paye_sun":11,"paye_bh":11,"charge_day":21,"charge_night":21,"charge_sat":21,"charge_sun":21,"charge_bh":21}'::jsonb);
insert into public.weekly_source_groups(
  id,environment,agency_id,code,display_name,source_family,timezone,
  cutoff_weekday,cutoff_local_time,nhsp_report_heading_name
) values
  ('91000000-0000-4000-8000-000000000010','TEST',
   '91000000-0000-4000-8000-000000000002','ROSTER_CONTEXT_VERIFY','Roster Context Verify',
   'ROSTER','Europe/London',3,'15:00',null),
  ('91000000-0000-4000-8000-000000000030','TEST',
   '91000000-0000-4000-8000-000000000002','NHSP_CONTEXT_VERIFY','NHSP Context Verify',
   'NHSP','Europe/London',3,'15:00','NHSP Context Heading');
insert into public.weekly_source_group_clients(
  source_group_id,client_id,valid_from,created_by_user_id
) values
  ('91000000-0000-4000-8000-000000000010','91000000-0000-4000-8000-000000000020','2026-01-01','91000000-0000-4000-8000-000000000001'),
  ('91000000-0000-4000-8000-000000000030','91000000-0000-4000-8000-000000000040','2026-01-01','91000000-0000-4000-8000-000000000001');
insert into public.weekly_source_client_policies(
  source_group_id,client_id,effective_from,authority_mode,document_mode,
  self_bill_enabled,created_by_user_id
) values (
  '91000000-0000-4000-8000-000000000010','91000000-0000-4000-8000-000000000020',
  '2026-01-01','TIMESHEET_AUTHORITY','INVOICE_EVIDENCE_REQUIRED',false,
  '91000000-0000-4000-8000-000000000001'
);
insert into public.weekly_source_cycles(
  id,source_group_id,finalisation_week_ending,cutoff_at_utc
) values
  ('91000000-0000-4000-8000-000000000011','91000000-0000-4000-8000-000000000010','2026-09-20','2026-09-16 14:00:00+00'),
  ('91000000-0000-4000-8000-000000000031','91000000-0000-4000-8000-000000000030','2026-09-20','2026-09-16 14:00:00+00');
insert into public.weekly_source_report_scopes(
  id,source_cycle_id,environment,agency_id,source_group_id,client_id,cutoff_at_utc
) values (
  '91000000-0000-4000-8000-000000000050','91000000-0000-4000-8000-000000000031','TEST',
  '91000000-0000-4000-8000-000000000002','91000000-0000-4000-8000-000000000030',
  '91000000-0000-4000-8000-000000000040','2026-09-16 14:00:00+00'
);

do $verification$
declare
  v_roster jsonb;
  v_nhsp jsonb;
begin
  v_roster:=public.weekly_source_upload_context_v1(pg_catalog.jsonb_build_object(
    'operation','DISCOVER_SCOPE',
    'actor_user_id','91000000-0000-4000-8000-000000000001',
    'source_group_id','91000000-0000-4000-8000-000000000010',
    'source_cycle_id','91000000-0000-4000-8000-000000000011'
  ));
  perform pg_temp.assert_true(
    v_roster->>'environment'='TEST'
    and v_roster->>'agency_id'='91000000-0000-4000-8000-000000000002'
    and v_roster->>'client_id'='91000000-0000-4000-8000-000000000020'
    and coalesce((v_roster->>'client_selection_required')::boolean,true)=false,
    'Roster scope was not derived from the one saved group membership'
  );

  v_nhsp:=public.weekly_source_upload_context_v1(pg_catalog.jsonb_build_object(
    'operation','DISCOVER_SCOPE',
    'actor_user_id','91000000-0000-4000-8000-000000000001',
    'source_group_id','91000000-0000-4000-8000-000000000030',
    'source_cycle_id','91000000-0000-4000-8000-000000000031',
    'report_scope_id','91000000-0000-4000-8000-000000000050'
  ));
  perform pg_temp.assert_true(
    v_nhsp->>'authority_scope_kind'='NHSP_REPORT_SCOPE'
    and v_nhsp->>'client_id'='91000000-0000-4000-8000-000000000040'
    and v_nhsp->>'nhsp_report_heading_name'='NHSP Context Heading',
    'NHSP Trust scope was not derived from the saved report scope'
  );

  begin
    perform public.weekly_source_upload_context_v1(pg_catalog.jsonb_build_object(
      'operation','DISCOVER_SCOPE',
      'actor_user_id','91000000-0000-4000-8000-000000000001',
      'source_group_id','91000000-0000-4000-8000-000000000010',
      'source_cycle_id','91000000-0000-4000-8000-000000000011',
      'rates_json',pg_catalog.jsonb_build_object('charge_day',999)
    ));
    raise exception 'ASSERTION_FAILED: browser rate facts were accepted';
  exception when sqlstate '22023' then
    null;
  end;
end;
$verification$;

do $build_projection_runtime$
declare
  v_result jsonb;
  v_upload_id uuid;
  v_context jsonb;
  v_publication_id uuid;
  v_recheck jsonb;
  v_recheck_request jsonb;
  v_rows jsonb;
begin
  v_result:=public.weekly_source_upload_stage_begin_atomic_v1(
    pg_catalog.jsonb_build_object(
      'actor_user_id','91000000-0000-4000-8000-000000000001',
      'environment','TEST','agency_id','91000000-0000-4000-8000-000000000002',
      'source_group_id','91000000-0000-4000-8000-000000000010',
      'source_cycle_id','91000000-0000-4000-8000-000000000011',
      'client_id','91000000-0000-4000-8000-000000000020',
      'original_filename','context-build-verification.xlsx',
      'content_sha256',pg_catalog.repeat('9',64),'byte_count',2048,
      'profile_code','HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1','profile_version',1,
      'parser_version','WEEKLY_SOURCE_STRICT_V1',
      'normaliser_version','HEALTHROSTER_NORMALISER_V1',
      'workbook_part_and_sheet_fingerprint',pg_catalog.repeat('8',64),
      'header_coordinate_map_json',pg_catalog.jsonb_build_object(
        'Actual Start','A','Actual End','B','Actual Break','C','Actual Hours','D',
        'Timesheet Finalised By','E'
      ),
      'purpose','ORDINARY',
      'suggested_coverage_start_local_date','2026-09-14',
      'suggested_coverage_end_local_date','2026-09-14',
      'confirmed_coverage_start_local_date','2026-09-14',
      'confirmed_coverage_end_local_date','2026-09-14',
      'coverage_timezone','Europe/London',
      'coverage_confirmation_version','HEALTHROSTER_COMPLETE_EXPORT_ATTESTATION_V1',
      'coverage_state','COMPLETE',
      'coverage_proof_kind','HEALTHROSTER_COMPLETE_EXPORT_ATTESTATION',
      'physical_row_count',2,'header_count',1,'trailer_count',0,
      'continuation_count',0,'accepted_count',1,
      'blocking_economic_duplicate_count',0,'malformed_count',0,'blocked_count',0,
      'file_metadata_json',pg_catalog.jsonb_build_object(
        'client_id','91000000-0000-4000-8000-000000000020',
        'saved_finalisation_profile_map',pg_catalog.jsonb_build_object(
          'profile','HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1','version',1
        )
      ),
      'parser_summary_json',pg_catalog.jsonb_build_object('fatal_errors',0)
    )
  );
  perform pg_temp.assert_true(v_result->>'status'='STAGING','BUILD proof upload did not stage');
  v_upload_id:=(v_result->>'logical_upload_id')::uuid;

  v_result:=public.weekly_source_upload_stage_rows_atomic_v1(
    pg_catalog.jsonb_build_object(
      'actor_user_id','91000000-0000-4000-8000-000000000001','upload_id',v_upload_id,
      'physical_rows',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object(
          'source_row_ordinal',1,'classification','HEADER',
          'bounded_raw_cells_json',pg_catalog.jsonb_build_object('A','Actual Start')
        ),
        pg_catalog.jsonb_build_object(
          'source_row_ordinal',2,'classification','ACCEPTED_SHIFT',
          'bounded_raw_cells_json',pg_catalog.jsonb_build_object('A','','D','0')
        )
      ),
      'normalised_rows',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object(
          'source_row_ordinal',2,'external_source_key','CONTEXT-HR-1',
          'source_candidate_identity','Exact Saved Candidate','source_client_identity','Roster Context Client',
          'work_date','2026-09-14','start_at_local',null,'end_at_local',null,
          'break_minutes',null,'actual_net_minutes',0,
          'row_finalisation_state','SOURCE_UNFINALISED','finalised_by',null,
          'source_money_parse_state','NOT_APPLICABLE',
          'source_expense_parse_state','NOT_APPLICABLE',
          'bounded_raw_columns_json',pg_catalog.jsonb_build_object(
            'worker_name','Exact Saved Candidate','request_id','CONTEXT-HR-1'
          )
        )
      ),
      'money_evidence','[]'::jsonb,'expense_evidence','[]'::jsonb
    )
  );
  perform pg_temp.assert_true(v_result->>'status'='STAGING','BUILD proof row did not stage');
  v_result:=public.weekly_source_upload_seal_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','91000000-0000-4000-8000-000000000001','upload_id',v_upload_id
  ));
  perform pg_temp.assert_true(v_result->>'status'='CURRENT','BUILD proof upload did not seal');

  v_context:=public.weekly_source_upload_context_v1(pg_catalog.jsonb_build_object(
    'operation','BUILD_PROJECTION',
    'actor_user_id','91000000-0000-4000-8000-000000000001',
    'upload_id',v_upload_id
  ));
  perform pg_temp.assert_true(
    v_context->>'source_group_id'='91000000-0000-4000-8000-000000000010'
    and v_context->>'client_id'='91000000-0000-4000-8000-000000000020'
    and v_context->>'profile_id'='HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1'
    and (v_context->>'row_manifest_hash')~'^[0-9a-f]{64}$'
    and pg_catalog.jsonb_array_length(v_context->'rows')=1
    and v_context#>>'{rows,0,candidate_match_count}'='1'
    and v_context#>>'{rows,0,candidate_id}'='91000000-0000-4000-8000-000000000060'
    and pg_catalog.jsonb_array_length(v_context#>'{rows,0,contracts}')=2,
    'BUILD projection did not return the sealed exact source census'
  );
  v_result:=public.weekly_source_projection_begin_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','91000000-0000-4000-8000-000000000001','upload_id',v_upload_id,
    'expected_authority_scope_version',(v_context->>'authority_scope_version')::bigint));
  v_publication_id:=(v_result->>'publication_id')::uuid;
  select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('upload_row_id',id,
    'mapping_state','SOURCE_ROW_BLOCKED','blocker_code','VERIFICATION_MAPPING_NOT_IN_SCOPE',
    'qualifying_contract_ids','[]'::jsonb)) into v_rows
    from public.weekly_source_upload_rows where upload_id=v_upload_id;
  perform public.weekly_source_projection_rows_apply_atomic_v1(
    '91000000-0000-4000-8000-000000000001',v_publication_id,v_rows);
  perform public.weekly_source_projection_publish_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','91000000-0000-4000-8000-000000000001','publication_id',v_publication_id));
  v_recheck_request:=pg_catalog.jsonb_build_object(
    'actor_user_id','91000000-0000-4000-8000-000000000001','request_id',pg_catalog.gen_random_uuid(),
    'upload_id',v_upload_id,'projection_publication_id',v_publication_id,
    'expected_authority_scope_version',(v_context->>'authority_scope_version')::bigint,
    'expected_row_manifest_hash',v_context->>'row_manifest_hash',
    'upload_row_id',v_context#>>'{rows,0,upload_row_id}',
    'candidate_id','91000000-0000-4000-8000-000000000060',
    'client_id','91000000-0000-4000-8000-000000000020');
  update public.candidates set active=false where id='91000000-0000-4000-8000-000000000060';
  begin
    perform public.weekly_source_office_recheck_begin_v1(v_recheck_request);
    raise exception 'VERIFY_FAILED: inactive candidate link accepted';
  exception when sqlstate '22023' then
    if sqlerrm<>'WEEKLY_SOURCE_CANDIDATE_INACTIVE_OR_MISSING' then raise; end if;
  end;
  update public.candidates set active=true where id='91000000-0000-4000-8000-000000000060';
  v_recheck:=public.weekly_source_office_recheck_begin_v1(v_recheck_request);
  perform pg_temp.assert_true(v_recheck->>'status'='BUILDING'
    and v_recheck->>'publication_id'<>v_publication_id::text,'Recheck did not create a fresh comparison');
  v_result:=public.weekly_source_office_recheck_begin_v1(v_recheck_request);
  perform pg_temp.assert_true(v_result->>'publication_id'=v_recheck->>'publication_id'
    and (v_result->>'idempotent')::boolean,'Exact recheck retry did not retain its comparison');
  begin
    perform public.weekly_source_office_recheck_begin_v1(v_recheck_request||pg_catalog.jsonb_build_object('request_id',pg_catalog.gen_random_uuid()));
    raise exception 'VERIFY_FAILED: stale recheck accepted';
  exception when sqlstate '40001' then
    if sqlerrm<>'WEEKLY_SOURCE_PREVIEW_STALE' then raise; end if;
  end;
  v_result:=public.weekly_source_upload_context_v1(pg_catalog.jsonb_build_object(
    'operation','BUILD_PROJECTION','actor_user_id','91000000-0000-4000-8000-000000000001','upload_id',v_upload_id));
  perform pg_temp.assert_true(v_result#>>'{rows,0,candidate_id}'='91000000-0000-4000-8000-000000000060'
    and v_result->>'row_manifest_hash'=v_context->>'row_manifest_hash','Office choice changed file evidence or lost its candidate');
  perform public.weekly_source_projection_rows_apply_atomic_v1(
    '91000000-0000-4000-8000-000000000001',(v_recheck->>'publication_id')::uuid,v_rows);
  v_result:=public.weekly_source_projection_publish_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','91000000-0000-4000-8000-000000000001','publication_id',v_recheck->>'publication_id'));
  perform pg_temp.assert_true(v_result->>'status'='CURRENT','Rechecked comparison did not publish');
end;
$build_projection_runtime$;

-- Released NHSP files are row-wise: only NHSP-enabled clients may cross groups.
-- Reuse only the rollback-contained fixtures above; no historical TEST rows.
do $released_client_runtime$
declare
  v_base_upload uuid;
  v_base_row uuid;
  v_upload uuid:=pg_catalog.gen_random_uuid();
  v_row uuid:=pg_catalog.gen_random_uuid();
  v_second_row uuid:=pg_catalog.gen_random_uuid();
  v_pinned_upload uuid:=pg_catalog.gen_random_uuid();
  v_context jsonb;
  v_pub jsonb;
  v_rows jsonb;
  v_request jsonb;
  v_result jsonb;
begin
  insert into public.clients(id,cli_ref,name) values
    ('91000000-0000-4000-8000-000000000070','CLI-99992','Outside Group NHSP Trust'),
    ('91000000-0000-4000-8000-000000000071','CLI-99991','Missing Settings Trust');
  insert into public.client_settings(client_id,vat_rate_pct,effective_from,is_nhsp) values
    ('91000000-0000-4000-8000-000000000040',20,'2026-01-01',true),
    ('91000000-0000-4000-8000-000000000070',20,'2026-01-01',true);
  select id into strict v_base_upload from public.weekly_source_uploads
    where original_filename='context-build-verification.xlsx'
      and uploaded_by_user_id='91000000-0000-4000-8000-000000000001';
  select id into strict v_base_row from public.weekly_source_upload_rows where upload_id=v_base_upload;
  insert into public.weekly_source_uploads
    select (pg_catalog.jsonb_populate_record(null::public.weekly_source_uploads,
      pg_catalog.to_jsonb(upload)||pg_catalog.jsonb_build_object(
        'id',v_upload,'source_cycle_id','91000000-0000-4000-8000-000000000031',
        'source_format_profile_id','31111111-1111-4111-8111-111111111111',
        'declared_scope_fingerprint',private.weekly_source_scope_fingerprint_v1(
          'TEST','91000000-0000-4000-8000-000000000002',
          '91000000-0000-4000-8000-000000000030','91000000-0000-4000-8000-000000000031',null,null),
        'file_metadata_json',upload.file_metadata_json-'client_id',
        'original_filename','released-multi-client-verification.xlsx',
        'accepted_count',2,'physical_row_count',3
      ))).* from public.weekly_source_uploads upload where id=v_base_upload;
  insert into public.weekly_source_upload_rows
    select (pg_catalog.jsonb_populate_record(null::public.weekly_source_upload_rows,
      pg_catalog.to_jsonb(source_row)||pg_catalog.jsonb_build_object(
        'id',v_row,'upload_id',v_upload,'source_client_identity','NHSP Context Trust',
        'bounded_raw_columns_json',source_row.bounded_raw_columns_json||
          '{"trust":"NHSP Context Trust"}'::jsonb
      ))).* from public.weekly_source_upload_rows source_row where id=v_base_row;
  insert into public.weekly_source_upload_rows
    select (pg_catalog.jsonb_populate_record(null::public.weekly_source_upload_rows,
      pg_catalog.to_jsonb(source_row)||pg_catalog.jsonb_build_object(
        'id',v_second_row,'upload_id',v_upload,'source_row_ordinal',3,'external_source_key','RELEASED-SECOND-CLIENT',
        'source_client_identity','Outside Group NHSP Trust',
        'normalised_row_hash',decode(repeat('ab',32),'hex'),
        'bounded_raw_columns_json',source_row.bounded_raw_columns_json||
          '{"trust":"Outside Group NHSP Trust"}'::jsonb
      ))).*
    from public.weekly_source_upload_rows source_row where id=v_base_row;
  update public.weekly_source_cycles
    set current_complete_upload_id=v_upload,current_projection_publication_id=null,
      projection_state='REBUILDING',version=1
    where id='91000000-0000-4000-8000-000000000031';

  perform pg_temp.assert_true(
    private.weekly_source_upload_client_eligible_v1(v_upload,'91000000-0000-4000-8000-000000000070','2026-09-14'),
    'released file rejected a real client outside its group');
  perform pg_temp.assert_true(
    not private.weekly_source_upload_client_eligible_v1(v_upload,'91000000-0000-4000-8000-000000000099','2026-09-14'),
    'released file accepted a missing client');
  perform pg_temp.assert_true(
    not private.weekly_source_upload_client_eligible_v1(v_upload,'91000000-0000-4000-8000-000000000020','2026-09-14')
    and not private.weekly_source_upload_client_eligible_v1(v_upload,'91000000-0000-4000-8000-000000000071','2026-09-14'),
    'released file accepted a non-NHSP client or missing settings');
  -- A saved released profile is not authority to accept a non-NHSP source.
  update public.weekly_source_uploads set source_cycle_id='91000000-0000-4000-8000-000000000011'
    where id=v_upload;
  perform pg_temp.assert_true(not private.weekly_source_upload_client_eligible_v1(
    v_upload,'91000000-0000-4000-8000-000000000070','2026-09-14'),
    'released profile accepted a non-NHSP source');
  update public.weekly_source_uploads set source_cycle_id='91000000-0000-4000-8000-000000000031'
    where id=v_upload;
  v_context:=public.weekly_source_upload_context_v1(jsonb_build_object(
    'operation','BUILD_PROJECTION','actor_user_id','91000000-0000-4000-8000-000000000001',
    'upload_id',v_upload));
  perform pg_temp.assert_true(
    v_context#>>'{rows,0,client_id}'='91000000-0000-4000-8000-000000000040'
    and v_context#>>'{rows,1,client_id}'='91000000-0000-4000-8000-000000000070',
    'released multi-client rows were not independently matched');

  -- Old true, applicable false, future true: the work-date setting wins.
  insert into public.client_settings(client_id,vat_rate_pct,effective_from,is_nhsp) values
    ('91000000-0000-4000-8000-000000000070',20,'2026-09-01',false),
    ('91000000-0000-4000-8000-000000000070',20,'2026-10-01',true);
  v_result:=public.weekly_source_upload_context_v1(jsonb_build_object(
    'operation','BUILD_PROJECTION','actor_user_id','91000000-0000-4000-8000-000000000001','upload_id',v_upload));
  perform pg_temp.assert_true(v_result#>>'{rows,1,client_id}' is null,
    'released automatic match ignored applicable non-NHSP settings');
  insert into public.client_settings(client_id,vat_rate_pct,effective_from,is_nhsp) values
    ('91000000-0000-4000-8000-000000000070',20,'2026-09-10',true);
  -- Two eligible exact names must not silently choose one client.
  insert into public.clients(id,cli_ref,name) values
    ('91000000-0000-4000-8000-000000000072','CLI-99990','NHSP Context Trust');
  insert into public.client_settings(client_id,vat_rate_pct,effective_from,is_nhsp) values
    ('91000000-0000-4000-8000-000000000072',20,'2026-01-01',true);
  v_result:=public.weekly_source_upload_context_v1(jsonb_build_object(
    'operation','BUILD_PROJECTION','actor_user_id','91000000-0000-4000-8000-000000000001','upload_id',v_upload));
  perform pg_temp.assert_true(v_result#>>'{rows,0,client_id}' is null,
    'released automatic match silently selected an ambiguous NHSP client');
  update public.client_settings set is_nhsp=false
    where client_id='91000000-0000-4000-8000-000000000072';
  v_result:=public.weekly_source_upload_context_v1(jsonb_build_object(
    'operation','BUILD_PROJECTION','actor_user_id','91000000-0000-4000-8000-000000000001','upload_id',v_upload));
  perform pg_temp.assert_true(v_result#>>'{rows,0,client_id}'='91000000-0000-4000-8000-000000000040',
    'non-NHSP namesake blocked a unique eligible NHSP match');

  -- A saved upload-level hint must not turn a multi-client checking file into
  -- a one-client report. Construct new evidence instead of editing source rows.
  insert into public.weekly_source_uploads
    select (pg_catalog.jsonb_populate_record(null::public.weekly_source_uploads,
      pg_catalog.to_jsonb(upload)||pg_catalog.jsonb_build_object(
        'id',v_pinned_upload,'original_filename','released-client-hint-verification.xlsx',
        'file_metadata_json',upload.file_metadata_json||jsonb_build_object(
          'client_id','91000000-0000-4000-8000-000000000040'),
        'declared_scope_fingerprint',private.weekly_source_scope_fingerprint_v1(
          'TEST','91000000-0000-4000-8000-000000000002',
          '91000000-0000-4000-8000-000000000030','91000000-0000-4000-8000-000000000031',
          null,'91000000-0000-4000-8000-000000000040')
      ))).* from public.weekly_source_uploads upload where id=v_upload;
  insert into public.weekly_source_upload_rows
    select (pg_catalog.jsonb_populate_record(null::public.weekly_source_upload_rows,
      pg_catalog.to_jsonb(source_row)||jsonb_build_object(
        'id',gen_random_uuid(),'upload_id',v_pinned_upload
      ))).* from public.weekly_source_upload_rows source_row where upload_id=v_upload;
  v_result:=public.weekly_source_upload_context_v1(jsonb_build_object(
    'operation','BUILD_PROJECTION','actor_user_id','91000000-0000-4000-8000-000000000001',
    'upload_id',v_pinned_upload));
  perform pg_temp.assert_true(
    v_result#>>'{rows,0,client_id}'='91000000-0000-4000-8000-000000000040'
    and v_result#>>'{rows,1,client_id}'='91000000-0000-4000-8000-000000000070',
    'released upload client hint overrode row clients');

  v_pub:=public.weekly_source_projection_begin_atomic_v1(jsonb_build_object(
    'actor_user_id','91000000-0000-4000-8000-000000000001','upload_id',v_upload,
    'expected_authority_scope_version',1));
  select jsonb_agg(jsonb_build_object('upload_row_id',id,
    'mapping_state','SOURCE_ROW_BLOCKED','blocker_code','VERIFICATION_MAPPING_NOT_IN_SCOPE',
    'qualifying_contract_ids','[]'::jsonb)) into v_rows
    from public.weekly_source_upload_rows where upload_id=v_upload;
  perform public.weekly_source_projection_rows_apply_atomic_v1(
    '91000000-0000-4000-8000-000000000001',(v_pub->>'publication_id')::uuid,v_rows);
  perform public.weekly_source_projection_publish_atomic_v1(jsonb_build_object(
    'actor_user_id','91000000-0000-4000-8000-000000000001','publication_id',v_pub->>'publication_id'));
  v_request:=jsonb_build_object(
    'actor_user_id','91000000-0000-4000-8000-000000000001','request_id',gen_random_uuid(),
    'upload_id',v_upload,'projection_publication_id',v_pub->>'publication_id',
    'expected_authority_scope_version',1,
    'expected_row_manifest_hash',v_context->>'row_manifest_hash',
    'upload_row_id',v_second_row,'client_id','91000000-0000-4000-8000-000000000070');
  begin
    perform public.weekly_source_office_recheck_begin_v1(v_request||jsonb_build_object(
      'client_id','91000000-0000-4000-8000-000000000020'));
    raise exception 'VERIFY_FAILED: released manual link accepted a non-NHSP client';
  exception when sqlstate '22023' then
    if sqlerrm<>'WEEKLY_SOURCE_CLIENT_NOT_ELIGIBLE' then raise; end if;
  end;
  perform pg_temp.assert_true(not exists(select 1 from private.weekly_source_office_row_choices
    where upload_row_id=v_second_row),'rejected non-NHSP link saved a choice');

  -- Even another group member must not replace a backing report's saved Trust.
  insert into public.clients(id,cli_ref,name) values(
    '91000000-0000-4000-8000-000000000099','CLI-99993','Another Backing Trust');
  insert into public.weekly_source_group_clients(source_group_id,client_id,valid_from,created_by_user_id)
    values('91000000-0000-4000-8000-000000000030','91000000-0000-4000-8000-000000000099',
      '2026-01-01','91000000-0000-4000-8000-000000000001');
  update public.weekly_source_uploads
    set source_format_profile_id='32222222-2222-4222-8222-222222222222',
      report_scope_id='91000000-0000-4000-8000-000000000050' where id=v_upload;
  update public.weekly_source_report_scopes
    set current_complete_upload_id=v_upload,current_projection_publication_id=(v_pub->>'publication_id')::uuid,
      version=1 where id='91000000-0000-4000-8000-000000000050';
  perform pg_temp.assert_true(
    private.weekly_source_upload_client_eligible_v1(v_upload,'91000000-0000-4000-8000-000000000040','2026-09-14')
    and not private.weekly_source_upload_client_eligible_v1(v_upload,'91000000-0000-4000-8000-000000000099','2026-09-14'),
    'backing report lost its exact client boundary');
  begin
    perform public.weekly_source_office_recheck_begin_v1(v_request||jsonb_build_object(
      'client_id','91000000-0000-4000-8000-000000000099'));
    raise exception 'VERIFY_FAILED: backing report accepted a different client';
  exception when sqlstate '22023' then
    if sqlerrm<>'WEEKLY_SOURCE_CLIENT_NOT_ELIGIBLE' then raise; end if;
  end;
  perform pg_temp.assert_true(not exists(select 1 from private.weekly_source_office_row_choices
    where upload_row_id=v_second_row),'rejected backing link saved a choice');
  -- A released profile attached to a report scope is malformed, not a bypass.
  update public.weekly_source_uploads set source_format_profile_id='31111111-1111-4111-8111-111111111111'
    where id=v_upload;
  perform pg_temp.assert_true(not private.weekly_source_upload_client_eligible_v1(
    v_upload,'91000000-0000-4000-8000-000000000070','2026-09-14'),
    'malformed released report scope bypassed client checks');
  update public.weekly_source_uploads set report_scope_id=null where id=v_upload;
  -- The eligible NHSP client has no membership of this selected source group.
  v_result:=public.weekly_source_office_recheck_begin_v1(v_request);
  perform pg_temp.assert_true(v_result->>'status'='BUILDING','released manual client link did not start recheck');
  perform pg_temp.assert_true((public.weekly_source_office_recheck_begin_v1(v_request)->>'idempotent')::boolean,
    'released manual client retry was not idempotent');
  v_context:=public.weekly_source_upload_context_v1(jsonb_build_object(
    'operation','BUILD_PROJECTION','actor_user_id','91000000-0000-4000-8000-000000000001','upload_id',v_upload));
  perform pg_temp.assert_true(v_context#>>'{rows,1,client_id}'='91000000-0000-4000-8000-000000000070',
    'released saved manual client choice was lost');
  begin
    perform public.weekly_source_office_recheck_begin_v1(v_request||jsonb_build_object('request_id',gen_random_uuid()));
    raise exception 'VERIFY_FAILED: released stale recheck accepted';
  exception when sqlstate '40001' then
    if sqlerrm<>'WEEKLY_SOURCE_PREVIEW_STALE' then raise; end if;
  end;
end;
$released_client_runtime$;

select pg_temp.assert_true(
  not pg_catalog.has_function_privilege('anon','private.weekly_source_upload_client_eligible_v1(uuid,uuid,date)','EXECUTE')
  and not pg_catalog.has_function_privilege('authenticated','private.weekly_source_upload_client_eligible_v1(uuid,uuid,date)','EXECUTE')
  and not pg_catalog.has_function_privilege('service_role','private.weekly_source_upload_client_eligible_v1(uuid,uuid,date)','EXECUTE'),
  'row client helper must be private to definer owners');

rollback;
