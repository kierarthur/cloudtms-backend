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
end;
$build_projection_runtime$;

rollback;
