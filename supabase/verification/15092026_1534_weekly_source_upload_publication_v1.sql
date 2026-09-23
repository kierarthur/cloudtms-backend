-- Rollback-only proof for weekly_source_upload_publication_v1.
-- Prerequisites: Plan 6 schema, private classifiers, settings/profile registry,
-- upload/publication repeatable, projection-build repeatable and final ACL
-- repeatable, in that order.

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

create function pg_temp.roster_begin(
  p_content_hash text,
  p_filename text,
  p_physical_count integer default 3,
  p_purpose text default 'ORDINARY',
  p_correction_session_id uuid default null,
  p_coverage_start date default '2026-09-14',
  p_coverage_end date default '2026-09-14',
  p_expected_correction_session_version bigint default null
) returns jsonb language plpgsql as $function$
declare
  v_result jsonb;
begin
  v_result:=public.weekly_source_upload_stage_begin_atomic_v1(
    pg_catalog.jsonb_build_object(
      'actor_user_id','90000000-0000-4000-8000-000000000001',
      'environment','TEST',
      'agency_id','90000000-0000-4000-8000-000000000002',
      'source_group_id','90000000-0000-4000-8000-000000000010',
      'source_cycle_id','90000000-0000-4000-8000-000000000011',
      'client_id','90000000-0000-4000-8000-000000000020',
      'original_filename',p_filename,
      'content_sha256',p_content_hash,
      'byte_count',1024,
      'profile_code','ROSTER_WEEKLY_SUMMARY_ACTUAL_V1',
      'profile_version',1,
      'parser_version','WEEKLY_SOURCE_STRICT_V1',
      'normaliser_version','ROSTER_WEEKLY_SUMMARY_NORMALISER_V1',
      'header_coordinate_map_json',pg_catalog.jsonb_build_object(
        'Booking Start','A','Booking End','B','Total Hours','C','Expenses','D'
      ),
      'purpose',p_purpose,
      'correction_session_id',p_correction_session_id,
      'expected_correction_session_version',p_expected_correction_session_version,
      'suggested_coverage_start_local_date',p_coverage_start,
      'suggested_coverage_end_local_date',p_coverage_end,
      'confirmed_coverage_start_local_date',p_coverage_start,
      'confirmed_coverage_end_local_date',p_coverage_end,
      'coverage_timezone','Europe/London',
      'coverage_confirmation_version','OFFICE_COMPLETE_EXPORT_ATTESTATION_V1',
      'coverage_state','COMPLETE',
      'coverage_proof_kind','OFFICE_COMPLETE_EXPORT_ATTESTATION',
      'physical_row_count',p_physical_count,
      'header_count',1,
      'trailer_count',0,
      'continuation_count',0,
      'accepted_count',2,
      'blocking_economic_duplicate_count',0,
      'malformed_count',0,
      'blocked_count',0,
      'file_metadata_json',pg_catalog.jsonb_build_object(
        'client_id','90000000-0000-4000-8000-000000000020'
      ),
      'parser_summary_json',pg_catalog.jsonb_build_object('fatal_errors',0)
    )
  );
  return v_result;
end;
$function$;

create function pg_temp.roster_stage_rows(p_upload_id uuid,p_suffix text)
returns void language plpgsql as $function$
declare
  v_result jsonb;
begin
  v_result:=public.weekly_source_upload_stage_rows_atomic_v1(
    pg_catalog.jsonb_build_object(
      'actor_user_id','90000000-0000-4000-8000-000000000001',
      'upload_id',p_upload_id,
      'physical_rows',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object(
          'source_row_ordinal',1,'classification','HEADER',
          'bounded_raw_cells_json',pg_catalog.jsonb_build_object('A','Booking Start')
        ),
        pg_catalog.jsonb_build_object(
          'source_row_ordinal',2,'classification','ACCEPTED_SHIFT',
          'bounded_raw_cells_json',pg_catalog.jsonb_build_object('A','2026-09-14 09:00:00','row',p_suffix)
        ),
        pg_catalog.jsonb_build_object(
          'source_row_ordinal',3,'classification','ACCEPTED_SHIFT',
          'bounded_raw_cells_json',pg_catalog.jsonb_build_object(
            'A','2026-09-14 18:00:00','C','0','row',p_suffix||'-ZERO'
          )
        )
      ),
      'normalised_rows',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object(
          'source_row_ordinal',2,
          'external_source_key','LINE-'||p_suffix,
          'source_candidate_identity','Candidate One',
          'source_client_identity','Roster Client',
          'work_date','2026-09-14',
          'start_at_local','2026-09-14T09:00:00',
          'end_at_local','2026-09-14T17:00:00',
          'break_minutes',30,
          'actual_net_minutes',450,
          'row_finalisation_state','SOURCE_WORKED',
          'role_band_source','RGN',
          'source_money_parse_state','NOT_APPLICABLE',
          'source_expense_pence',0,
          'source_expense_parse_state','OMITTED_ZERO',
          'bounded_raw_columns_json',pg_catalog.jsonb_build_object('Line ID','LINE-'||p_suffix)
        ),
        pg_catalog.jsonb_build_object(
          'source_row_ordinal',3,
          'external_source_key','LINE-'||p_suffix||'-ZERO',
          'source_candidate_identity','Candidate One',
          'source_client_identity','Roster Client',
          'work_date','2026-09-14',
          'start_at_local','2026-09-14T18:00:00',
          'end_at_local','2026-09-14T20:00:00',
          'break_minutes',0,
          'actual_net_minutes',0,
          'row_finalisation_state','SOURCE_ABSENT_ZERO',
          'role_band_source','RGN',
          'source_money_parse_state','NOT_APPLICABLE',
          'source_expense_pence',0,
          'source_expense_parse_state','OMITTED_ZERO',
          'bounded_raw_columns_json',pg_catalog.jsonb_build_object(
            'Line ID','LINE-'||p_suffix||'-ZERO','Total Hours','0'
          )
        )
      ),
      'money_evidence','[]'::jsonb,
      'expense_evidence',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object(
          'source_row_ordinal',2,
          'source_column_index',64,
          'cell_coordinate','BM2',
          'source_kind','CSV_DECODED_TEXT',
          'original_token','',
          'decoded_token','',
          'cell_type_marker','CSV_FIELD',
          'formula_present',false,
          'lexical_profile_version','SOURCE_FIXED_EXPENSE_PENCE_V1',
          'parse_state','OMITTED_ZERO',
          'parsed_pence',0
        ),
        pg_catalog.jsonb_build_object(
          'source_row_ordinal',3,
          'source_column_index',64,
          'cell_coordinate','BM3',
          'source_kind','CSV_DECODED_TEXT',
          'original_token','',
          'decoded_token','',
          'cell_type_marker','CSV_FIELD',
          'formula_present',false,
          'lexical_profile_version','SOURCE_FIXED_EXPENSE_PENCE_V1',
          'parse_state','OMITTED_ZERO',
          'parsed_pence',0
        )
      )
    )
  );
  perform pg_temp.assert_true(v_result->>'status'='STAGING','Roster evidence did not stage');
end;
$function$;

create function pg_temp.complete_projection(p_publication_id uuid)
returns void language plpgsql as $function$
declare
  v_rows jsonb;
  v_result jsonb;
begin
  select coalesce(
    pg_catalog.jsonb_agg(
      pg_catalog.jsonb_build_object(
        'upload_row_id',source_row.id,
        'mapping_state','SOURCE_ROW_BLOCKED',
        'blocker_code','VERIFICATION_MAPPING_NOT_IN_SCOPE',
        'qualifying_contract_ids','[]'::jsonb
      ) order by source_row.source_row_ordinal
    ),
    '[]'::jsonb
  ) into v_rows
  from public.weekly_source_projection_publications publication
  join public.weekly_source_upload_rows source_row
    on source_row.upload_id=publication.upload_id
  where publication.id=p_publication_id;

  v_result:=public.weekly_source_projection_rows_apply_atomic_v1(
    '90000000-0000-4000-8000-000000000001',p_publication_id,v_rows
  );
  perform pg_temp.assert_true(
    coalesce((v_result->>'ok')::boolean,false)
      and (v_result->>'applied_row_count')::integer=pg_catalog.jsonb_array_length(v_rows),
    'Projection owner did not persist the complete verification row census'
  );
end;
$function$;

create function pg_temp.healthroster_begin(
  p_content_hash text,
  p_filename text
) returns jsonb language plpgsql as $function$
begin
  return public.weekly_source_upload_stage_begin_atomic_v1(
    pg_catalog.jsonb_build_object(
      'actor_user_id','90000000-0000-4000-8000-000000000001',
      'environment','TEST',
      'agency_id','90000000-0000-4000-8000-000000000002',
      'source_group_id','90000000-0000-4000-8000-000000000070',
      'source_cycle_id','90000000-0000-4000-8000-000000000071',
      'client_id','90000000-0000-4000-8000-000000000021',
      'original_filename',p_filename,
      'content_sha256',p_content_hash,
      'byte_count',2048,
      'profile_code','HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1',
      'profile_version',1,
      'parser_version','WEEKLY_SOURCE_STRICT_V1',
      'normaliser_version','HEALTHROSTER_NORMALISER_V1',
      'workbook_part_and_sheet_fingerprint',repeat('7',64),
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
        'client_id','90000000-0000-4000-8000-000000000021',
        'saved_finalisation_profile_map',pg_catalog.jsonb_build_object(
          'profile','HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1','version',1
        )
      ),
      'parser_summary_json',pg_catalog.jsonb_build_object('fatal_errors',0)
    )
  );
end;
$function$;

create function pg_temp.healthroster_stage_row(
  p_upload_id uuid,
  p_state text,
  p_finalised_by text default null
) returns void language plpgsql as $function$
declare
  v_result jsonb;
begin
  v_result:=public.weekly_source_upload_stage_rows_atomic_v1(
    pg_catalog.jsonb_build_object(
      'actor_user_id','90000000-0000-4000-8000-000000000001',
      'upload_id',p_upload_id,
      'physical_rows',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object(
          'source_row_ordinal',1,'classification','HEADER',
          'bounded_raw_cells_json',pg_catalog.jsonb_build_object('A','Actual Start')
        ),
        pg_catalog.jsonb_build_object(
          'source_row_ordinal',2,'classification','ACCEPTED_SHIFT',
          'bounded_raw_cells_json',pg_catalog.jsonb_build_object(
            'A','','B','','C','','D','0','E',coalesce(p_finalised_by,'')
          )
        )
      ),
      'normalised_rows',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object(
          'source_row_ordinal',2,'external_source_key','HR-'||p_upload_id::text,
          'source_candidate_identity','Candidate HealthRoster',
          'source_client_identity','HealthRoster Client','work_date','2026-09-14',
          'start_at_local',null,'end_at_local',null,'break_minutes',null,
          'actual_net_minutes',0,'row_finalisation_state',p_state,
          'finalised_by',p_finalised_by,'source_money_parse_state','NOT_APPLICABLE',
          'source_expense_parse_state','NOT_APPLICABLE',
          'bounded_raw_columns_json',pg_catalog.jsonb_build_object('Actual Hours','0')
        )
      ),
      'money_evidence','[]'::jsonb,'expense_evidence','[]'::jsonb
    )
  );
  perform pg_temp.assert_true(v_result->>'status'='STAGING','HealthRoster evidence did not stage');
end;
$function$;

create function pg_temp.nhsp_begin(
  p_scope_id uuid,
  p_client_id uuid,
  p_report_number text,
  p_content_hash text,
  p_parser_version text default 'WEEKLY_SOURCE_STRICT_V1',
  p_source_kind text default 'XLSX',
  p_force_fingerprint boolean default false
) returns jsonb language plpgsql as $function$
begin
  return public.weekly_source_upload_stage_begin_atomic_v1(
    pg_catalog.jsonb_build_object(
      'actor_user_id','90000000-0000-4000-8000-000000000001',
      'environment','TEST',
      'agency_id','90000000-0000-4000-8000-000000000002',
      'source_group_id','90000000-0000-4000-8000-000000000030',
      'source_cycle_id','90000000-0000-4000-8000-000000000031',
      'report_scope_id',p_scope_id,
      'client_id',p_client_id,
      'original_filename','nhsp-'||p_report_number||
        case when p_source_kind='HTML' then '.xls' else '.xlsx' end,
      'content_sha256',p_content_hash,
      'byte_count',2048,
      'profile_code','NHSP_FINAL_BACKING_V1',
      'profile_version',1,
      'parser_version',p_parser_version,
      'normaliser_version','NHSP_BACKING_NORMALISER_V1',
      'workbook_part_and_sheet_fingerprint',
        case when p_source_kind='HTML' and not p_force_fingerprint then null
             else repeat('1',64) end,
      'header_coordinate_map_json',pg_catalog.jsonb_build_object(
        'Actual Start','L','Actual End','M','Actual Break','N','Actual Total','O',
        'Commission','P','Total Cost','Q','FMC','R'
      ),
      'money_lexical_authority_version','XLSX_BINARY64_SAME_VALUE_PENCE_V1',
      'purpose','ORDINARY',
      'coverage_proof_kind','NHSP_TRUST_REPORT_SCOPE',
      'physical_row_count',3,
      'header_count',1,
      'trailer_count',1,
      'continuation_count',0,
      'accepted_count',1,
      'blocking_economic_duplicate_count',0,
      'malformed_count',0,
      'blocked_count',0,
      'file_metadata_json',pg_catalog.jsonb_build_object(
        'nhsp_report_number',p_report_number,
        'nhsp_report_heading_name','Example Trust'
      ),
      'parser_summary_json',pg_catalog.jsonb_build_object(
        'fatal_errors',0,'source_kind',p_source_kind
      )
    )
  );
end;
$function$;

create function pg_temp.nhsp_stage_rows(
  p_upload_id uuid,
  p_suffix text,
  p_zero_charge boolean default false,
  p_money_source_kind text default 'XLSX_NUMERIC_TOKEN'
)
returns void language plpgsql as $function$
declare
  v_result jsonb;
begin
  v_result:=public.weekly_source_upload_stage_rows_atomic_v1(
    pg_catalog.jsonb_build_object(
      'actor_user_id','90000000-0000-4000-8000-000000000001',
      'upload_id',p_upload_id,
      'physical_rows',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object(
          'source_row_ordinal',1,'classification','HEADER',
          'bounded_raw_cells_json',pg_catalog.jsonb_build_object('A','Agency')
        ),
        pg_catalog.jsonb_build_object(
          'source_row_ordinal',2,'classification','ACCEPTED_SHIFT',
          'bounded_raw_cells_json',pg_catalog.jsonb_build_object('A','Candidate NHSP','row',p_suffix)
        ),
        pg_catalog.jsonb_build_object(
          'source_row_ordinal',3,'classification','TRAILER',
          'bounded_raw_cells_json',pg_catalog.jsonb_build_object(
            'Q',case when p_zero_charge then '0.00' else '15.00' end
          )
        )
      ),
      'normalised_rows',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object(
          'source_row_ordinal',2,
          'external_source_key','NHSP-'||p_suffix,
          'source_candidate_identity','Candidate NHSP',
          'source_client_identity','Example Trust',
          'work_date','2026-09-14',
          'start_at_local','2026-09-14T09:00:00',
          'end_at_local','2026-09-14T17:00:00',
          'break_minutes',30,
          'actual_net_minutes',450,
          'row_finalisation_state','SOURCE_WORKED',
          'role_band_source','BAND 5',
          'source_commission_pence',case when p_zero_charge then 0 else 500 end,
          'source_total_cost_pence',case when p_zero_charge then 0 else 1500 end,
          'source_shift_charge_pence',case when p_zero_charge then 0 else 2000 end,
          'source_money_parse_state','VALID',
          'source_qualification_profile_version','NHSP_TWO_COMPONENT_PENCE_V1',
          'source_expense_parse_state','NOT_APPLICABLE',
          'bounded_raw_columns_json',pg_catalog.jsonb_build_object('Actual Start','09:00')
        )
      ),
      'money_evidence',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object(
          'source_row_ordinal',2,'money_field_kind','COMMISSION','source_column_index',15,
          'cell_coordinate','P2','source_kind',p_money_source_kind,
          'original_token',case when p_zero_charge then '0' else '5' end,
          'decoded_token',case when p_zero_charge then '0.00' else '5.00' end,
          'cell_type_marker','n','formula_present',false,'parse_state','VALID',
          'parsed_pence',case when p_zero_charge then 0 else 500 end
        ),
        pg_catalog.jsonb_build_object(
          'source_row_ordinal',2,'money_field_kind','TOTAL_COST','source_column_index',16,
          'cell_coordinate','Q2','source_kind',p_money_source_kind,
          'original_token',case when p_zero_charge then '0' else '15' end,
          'decoded_token',case when p_zero_charge then '0.00' else '15.00' end,
          'cell_type_marker','n','formula_present',false,'parse_state','VALID',
          'parsed_pence',case when p_zero_charge then 0 else 1500 end
        ),
        pg_catalog.jsonb_build_object(
          'source_row_ordinal',2,'money_field_kind','FMC','source_column_index',17,
          'cell_coordinate','R2','source_kind',p_money_source_kind,
          'original_token','0','decoded_token','0.00','cell_type_marker','n',
          'formula_present',false,'parse_state','VALID','parsed_pence',0
        )
      ),
      'expense_evidence','[]'::jsonb
    )
  );
  perform pg_temp.assert_true(v_result->>'status'='STAGING','NHSP evidence did not stage');
end;
$function$;

insert into public.tms_users(
  id,email,role,is_active,password_hash,payment_authoriser,payment_golden_key
) values (
  '90000000-0000-4000-8000-000000000001',
  'weekly-upload-publication-verification@example.invalid','admin',true,'not-a-login',true,false
);
insert into public.clients(id,cli_ref,name) values
  ('90000000-0000-4000-8000-000000000020','CLI-99990','Roster Client'),
  ('90000000-0000-4000-8000-000000000021','CLI-99993','HealthRoster Client'),
  ('90000000-0000-4000-8000-000000000040','CLI-99991','NHSP Trust A'),
  ('90000000-0000-4000-8000-000000000041','CLI-99992','NHSP Trust B');
insert into public.weekly_source_groups(
  id,environment,agency_id,code,display_name,source_family,timezone,
  cutoff_weekday,cutoff_local_time,nhsp_report_heading_name
) values
  ('90000000-0000-4000-8000-000000000010','TEST',
   '90000000-0000-4000-8000-000000000002','ROSTER_VERIFY','Roster Verify',
   'ROSTER','Europe/London',3,'15:00',null),
  ('90000000-0000-4000-8000-000000000070','TEST',
   '90000000-0000-4000-8000-000000000002','HEALTHROSTER_VERIFY','HealthRoster Verify',
   'ROSTER','Europe/London',3,'15:00',null),
  ('90000000-0000-4000-8000-000000000030','TEST',
   '90000000-0000-4000-8000-000000000002','NHSP_VERIFY','NHSP Verify',
   'NHSP','Europe/London',3,'15:00','Example Trust');
insert into public.weekly_source_group_clients(
  source_group_id,client_id,valid_from,created_by_user_id
) values
  ('90000000-0000-4000-8000-000000000010','90000000-0000-4000-8000-000000000020','2026-01-01','90000000-0000-4000-8000-000000000001'),
  ('90000000-0000-4000-8000-000000000070','90000000-0000-4000-8000-000000000021','2026-01-01','90000000-0000-4000-8000-000000000001'),
  ('90000000-0000-4000-8000-000000000030','90000000-0000-4000-8000-000000000040','2026-01-01','90000000-0000-4000-8000-000000000001'),
  ('90000000-0000-4000-8000-000000000030','90000000-0000-4000-8000-000000000041','2026-01-01','90000000-0000-4000-8000-000000000001');
insert into public.weekly_source_cycles(
  id,source_group_id,finalisation_week_ending,cutoff_at_utc
) values
  ('90000000-0000-4000-8000-000000000011','90000000-0000-4000-8000-000000000010','2026-09-20','2026-09-16 14:00:00+00'),
  ('90000000-0000-4000-8000-000000000071','90000000-0000-4000-8000-000000000070','2026-09-20','2026-09-16 14:00:00+00'),
  ('90000000-0000-4000-8000-000000000031','90000000-0000-4000-8000-000000000030','2026-09-20','2026-09-16 14:00:00+00');
insert into public.weekly_source_report_scopes(
  id,source_cycle_id,environment,agency_id,source_group_id,client_id,cutoff_at_utc
) values
  ('90000000-0000-4000-8000-000000000050','90000000-0000-4000-8000-000000000031','TEST',
   '90000000-0000-4000-8000-000000000002','90000000-0000-4000-8000-000000000030',
   '90000000-0000-4000-8000-000000000040','2026-09-16 14:00:00+00'),
  ('90000000-0000-4000-8000-000000000051','90000000-0000-4000-8000-000000000031','TEST',
   '90000000-0000-4000-8000-000000000002','90000000-0000-4000-8000-000000000030',
   '90000000-0000-4000-8000-000000000041','2026-09-16 14:00:00+00');

do $verification$
declare
  v_a uuid;
  v_b uuid;
  v_bad uuid;
  v_coverage_bad uuid;
  v_d uuid;
  v_hr_unfinalised uuid;
  v_hr_bad_zero uuid;
  v_nhsp_a uuid;
  v_nhsp_b uuid;
  v_prefinal uuid;
  v_pub_a uuid;
  v_pub_b uuid;
  v_pub_d uuid;
  v_pub_nhsp uuid;
  v_pub_prefinal uuid;
  v_correction_upload uuid;
  v_correction_retry uuid;
  v_correction_rejected uuid;
  v_result jsonb;
  v_count integer;
  v_before_timesheets bigint;
  v_before_invoices bigint;
  v_before_pay_batches bigint;
  v_before_incidents bigint;
begin
  select count(*) into v_before_timesheets from public.timesheets;
  select count(*) into v_before_invoices from public.invoices;
  select count(*) into v_before_pay_batches from public.pay_batches;
  select count(*) into v_before_incidents from public.weekly_discrepancy_incidents;

  v_result:=pg_temp.roster_begin(repeat('a',64),'roster-a.csv');
  perform pg_temp.assert_true(v_result->>'status'='STAGING','A was not staged');
  v_a:=(v_result->>'logical_upload_id')::uuid;
  perform pg_temp.roster_stage_rows(v_a,'A');
  v_result:=public.weekly_source_upload_seal_atomic_v1(
    pg_catalog.jsonb_build_object('actor_user_id','90000000-0000-4000-8000-000000000001','upload_id',v_a)
  );
  perform pg_temp.assert_true(
    v_result->>'status'='CURRENT' and (v_result->>'authority_scope_version')::bigint=1,
    'A was not made current at version 1'
  );
  perform pg_temp.assert_true(
    (select count(*)=1
     from public.weekly_source_upload_rows source_row
     where source_row.upload_id=v_a
       and source_row.row_finalisation_state='SOURCE_ABSENT_ZERO'
       and source_row.actual_net_minutes=0),
    'The generic roster SOURCE_ZERO row was not retained as SOURCE_ABSENT_ZERO evidence'
  );
  v_result:=public.weekly_source_projection_begin_atomic_v1(
    pg_catalog.jsonb_build_object('actor_user_id','90000000-0000-4000-8000-000000000001',
      'upload_id',v_a,'expected_authority_scope_version',1)
  );
  v_pub_a:=(v_result->>'publication_id')::uuid;
  begin
    perform public.weekly_source_projection_publish_atomic_v1(
      pg_catalog.jsonb_build_object(
        'actor_user_id','90000000-0000-4000-8000-000000000001',
        'publication_id',v_pub_a
      )
    );
    raise exception 'INCOMPLETE_PROJECTION_UNEXPECTEDLY_PUBLISHED';
  exception when sqlstate '55000' then
    perform pg_temp.assert_true(
      sqlerrm='WEEKLY_SOURCE_PROJECTION_RESOLUTION_CENSUS_INCOMPLETE',
      'An incomplete projection failed for the wrong reason'
    );
  end;
  perform pg_temp.complete_projection(v_pub_a);
  v_result:=public.weekly_source_projection_publish_atomic_v1(
    pg_catalog.jsonb_build_object('actor_user_id','90000000-0000-4000-8000-000000000001',
      'publication_id',v_pub_a)
  );
  perform pg_temp.assert_true(v_result->>'status'='CURRENT','A publication was not current');
  perform private.weekly_source_current_publication_guard_v1(
    '90000000-0000-4000-8000-000000000011','CYCLE',null,v_a,v_pub_a,1
  );

  v_result:=pg_temp.roster_begin(repeat('b',64),'roster-b.csv');
  v_b:=(v_result->>'logical_upload_id')::uuid;
  perform pg_temp.roster_stage_rows(v_b,'B');
  v_result:=public.weekly_source_upload_seal_atomic_v1(
    pg_catalog.jsonb_build_object('actor_user_id','90000000-0000-4000-8000-000000000001','upload_id',v_b)
  );
  perform pg_temp.assert_true(
    v_result->>'status'='CURRENT' and (v_result->>'superseded_upload_id')::uuid=v_a
      and (v_result->>'authority_scope_version')::bigint=2,
    'B did not supersede A at version 2'
  );
  perform pg_temp.assert_true(
    (select state='SUPERSEDED' from public.weekly_source_uploads where id=v_a),
    'A did not remain immutable superseded evidence'
  );
  perform pg_temp.assert_true(
    (select state='STALE' from public.weekly_source_projection_publications where id=v_pub_a),
    'A publication was not made stale'
  );

  v_result:=pg_temp.roster_begin(repeat('a',64),'roster-a.csv');
  perform pg_temp.assert_true(
    v_result->>'status'='DUPLICATE'
      and (v_result->>'logical_upload_id')::uuid=v_a
      and (select current_complete_upload_id=v_b from public.weekly_source_cycles
           where id='90000000-0000-4000-8000-000000000011'),
    'Replaying superseded A resurrected or duplicated it'
  );

  v_result:=pg_temp.roster_begin(repeat('c',64),'roster-count-mismatch.csv',4);
  v_bad:=(v_result->>'logical_upload_id')::uuid;
  perform pg_temp.roster_stage_rows(v_bad,'BAD');
  v_result:=public.weekly_source_upload_seal_atomic_v1(
    pg_catalog.jsonb_build_object('actor_user_id','90000000-0000-4000-8000-000000000001','upload_id',v_bad)
  );
  perform pg_temp.assert_true(
    v_result->>'status'='REJECTED' and v_result->>'reason_code'='WEEKLY_SOURCE_ROW_COUNTS_MISMATCH'
      and (select state='REJECTED' from public.weekly_source_uploads where id=v_bad)
      and (select current_complete_upload_id=v_b and version=2 from public.weekly_source_cycles
           where id='90000000-0000-4000-8000-000000000011'),
    'Count mismatch did not fail closed while preserving B'
  );

  v_result:=public.weekly_source_projection_begin_atomic_v1(
    pg_catalog.jsonb_build_object('actor_user_id','90000000-0000-4000-8000-000000000001',
      'upload_id',v_b,'expected_authority_scope_version',2)
  );
  v_pub_b:=(v_result->>'publication_id')::uuid;
  v_result:=pg_temp.roster_begin(repeat('d',64),'roster-d.csv');
  v_d:=(v_result->>'logical_upload_id')::uuid;
  perform pg_temp.roster_stage_rows(v_d,'D');
  v_result:=public.weekly_source_upload_seal_atomic_v1(
    pg_catalog.jsonb_build_object('actor_user_id','90000000-0000-4000-8000-000000000001','upload_id',v_d)
  );
  perform pg_temp.assert_true(v_result->>'status'='CURRENT','D did not become current');
  v_result:=public.weekly_source_projection_publish_atomic_v1(
    pg_catalog.jsonb_build_object('actor_user_id','90000000-0000-4000-8000-000000000001',
      'publication_id',v_pub_b)
  );
  perform pg_temp.assert_true(v_result->>'status'='STALE','Late B publication did not fail CAS');
  v_result:=public.weekly_source_projection_begin_atomic_v1(
    pg_catalog.jsonb_build_object('actor_user_id','90000000-0000-4000-8000-000000000001',
      'upload_id',v_d,'expected_authority_scope_version',3)
  );
  v_pub_d:=(v_result->>'publication_id')::uuid;
  perform pg_temp.complete_projection(v_pub_d);
  v_result:=public.weekly_source_projection_publish_atomic_v1(
    pg_catalog.jsonb_build_object('actor_user_id','90000000-0000-4000-8000-000000000001',
      'publication_id',v_pub_d)
  );
  perform pg_temp.assert_true(v_result->>'status'='CURRENT','D publication did not become current');
  perform private.weekly_source_current_publication_guard_v1(
    '90000000-0000-4000-8000-000000000011','CYCLE',null,v_d,v_pub_d,3
  );

  v_result:=pg_temp.roster_begin(
    repeat('8',64),'roster-wrong-coverage.csv',3,'ORDINARY',null,
    '2026-09-13','2026-09-14'
  );
  v_coverage_bad:=(v_result->>'logical_upload_id')::uuid;
  perform pg_temp.roster_stage_rows(v_coverage_bad,'WRONG-COVERAGE');
  v_result:=public.weekly_source_upload_seal_atomic_v1(
    pg_catalog.jsonb_build_object(
      'actor_user_id','90000000-0000-4000-8000-000000000001','upload_id',v_coverage_bad
    )
  );
  perform pg_temp.assert_true(
    v_result->>'status'='REJECTED'
      and v_result->>'reason_code'='WEEKLY_SOURCE_COVERAGE_EVIDENCE_MISMATCH'
      and (select current_complete_upload_id=v_d and version=3
           from public.weekly_source_cycles where id='90000000-0000-4000-8000-000000000011'),
    'A claimed coverage range outside the imported first/last work date did not fail closed'
  );

  begin
    perform private.weekly_source_current_publication_guard_v1(
      '90000000-0000-4000-8000-000000000011','CYCLE',null,v_a,v_pub_a,1
    );
    raise exception 'OLD_UPLOAD_GUARD_UNEXPECTEDLY_PASSED';
  exception when sqlstate '55000' then
    perform pg_temp.assert_true(sqlerrm='SOURCE_CHECK_IN_PROGRESS','Old guard failed for wrong reason');
  end;

  -- A HealthRoster row with a finalisation column remains unfinalised when
  -- that column is not affirmative.  The currently released strict parser
  -- does not prove affirmative-finalised zero as source absence, so a caller
  -- cannot relabel that row SOURCE_ABSENT_ZERO at the database boundary.
  v_result:=pg_temp.healthroster_begin(repeat('1',64),'healthroster-unfinalised.xlsx');
  v_hr_unfinalised:=(v_result->>'logical_upload_id')::uuid;
  perform pg_temp.healthroster_stage_row(v_hr_unfinalised,'SOURCE_UNFINALISED',null);
  v_result:=public.weekly_source_upload_seal_atomic_v1(
    pg_catalog.jsonb_build_object(
      'actor_user_id','90000000-0000-4000-8000-000000000001','upload_id',v_hr_unfinalised
    )
  );
  perform pg_temp.assert_true(
    v_result->>'status'='CURRENT'
      and (select row_finalisation_state='SOURCE_UNFINALISED'
           from public.weekly_source_upload_rows where upload_id=v_hr_unfinalised),
    'HealthRoster non-affirmative finalisation was not retained as SOURCE_UNFINALISED'
  );

  v_result:=pg_temp.healthroster_begin(repeat('0',64),'healthroster-bad-finalised-zero.xlsx');
  v_hr_bad_zero:=(v_result->>'logical_upload_id')::uuid;
  perform pg_temp.healthroster_stage_row(v_hr_bad_zero,'SOURCE_ABSENT_ZERO','Roster Manager');
  v_result:=public.weekly_source_upload_seal_atomic_v1(
    pg_catalog.jsonb_build_object(
      'actor_user_id','90000000-0000-4000-8000-000000000001','upload_id',v_hr_bad_zero
    )
  );
  perform pg_temp.assert_true(
    v_result->>'status'='REJECTED'
      and v_result->>'reason_code'='WEEKLY_SOURCE_ROW_FINALISATION_STATE_INVALID'
      and (select current_complete_upload_id=v_hr_unfinalised and version=1
           from public.weekly_source_cycles where id='90000000-0000-4000-8000-000000000071'),
    'Unproved affirmative-finalised HealthRoster zero did not fail closed'
  );

  -- A correction upload is append-only evidence and never moves the current
  -- source pointer until the separate correction owner applies it.  Exact
  -- retries remain idempotent after READY; abort/rejection release the session
  -- so Office can stage a corrected replacement without deleting evidence.
  insert into public.weekly_source_final_revisions(
    id,source_cycle_id,authority_scope_kind,report_scope_id,revision_number,
    upload_id,coverage_start_local_date,coverage_end_local_date,coverage_timezone,
    reason,finalised_by_user_id,manifest_hash,policy_fingerprint,state
  ) values
    (
      '90000000-0000-4000-8000-000000000061',
      '90000000-0000-4000-8000-000000000011','CYCLE',null,91,v_d,
      '2026-09-07','2026-09-14','Europe/London','INITIAL_FINALISATION',
      '90000000-0000-4000-8000-000000000001',
      pg_catalog.decode(repeat('1',64),'hex'),pg_catalog.decode(repeat('2',64),'hex'),
      'SUPERSEDED'
    ),
    (
      '90000000-0000-4000-8000-000000000063',
      '90000000-0000-4000-8000-000000000011','CYCLE',null,92,v_d,
      '2026-09-07','2026-09-14','Europe/London','INITIAL_FINALISATION',
      '90000000-0000-4000-8000-000000000001',
      pg_catalog.decode(repeat('3',64),'hex'),pg_catalog.decode(repeat('4',64),'hex'),
      'SUPERSEDED'
    ),
    (
      '90000000-0000-4000-8000-000000000065',
      '90000000-0000-4000-8000-000000000011','CYCLE',null,93,v_d,
      '2026-09-07','2026-09-14','Europe/London','INITIAL_FINALISATION',
      '90000000-0000-4000-8000-000000000001',
      pg_catalog.decode(repeat('5',64),'hex'),pg_catalog.decode(repeat('6',64),'hex'),
      'SUPERSEDED'
    );
  insert into public.weekly_final_source_correction_sessions(
    id,source_cycle_id,authority_scope_kind,expected_current_final_revision_id,
    expected_final_manifest_hash,actor_user_id,reason,idempotency_key,
    request_hash,guard_fingerprint
  ) values (
    '90000000-0000-4000-8000-000000000060',
    '90000000-0000-4000-8000-000000000011','CYCLE',
    '90000000-0000-4000-8000-000000000061',
    pg_catalog.decode(repeat('1',64),'hex'),
    '90000000-0000-4000-8000-000000000001','Correct saved final source',
    'verify-correction-ready',pg_catalog.decode(repeat('a',64),'hex'),
    pg_catalog.decode(repeat('6',64),'hex')
  );
  begin
    perform pg_temp.roster_begin(
      repeat('7',64),'roster-correction-stale-version.csv',3,
      'FINAL_SOURCE_CORRECTION','90000000-0000-4000-8000-000000000060',
      '2026-09-14','2026-09-14',999
    );
    raise exception 'STALE_CORRECTION_VERSION_UNEXPECTEDLY_STAGED';
  exception when sqlstate '22023' then
    perform pg_temp.assert_true(
      sqlerrm='WEEKLY_SOURCE_CORRECTION_SESSION_INVALID',
      'A stale correction version failed for the wrong reason'
    );
  end;
  v_result:=pg_temp.roster_begin(
    repeat('6',64),'roster-correction-ready.csv',3,
    'FINAL_SOURCE_CORRECTION','90000000-0000-4000-8000-000000000060',
    '2026-09-14','2026-09-14',1
  );
  v_correction_upload:=(v_result->>'logical_upload_id')::uuid;
  perform pg_temp.roster_stage_rows(v_correction_upload,'CORRECTION-READY');
  v_result:=public.weekly_source_upload_seal_atomic_v1(
    pg_catalog.jsonb_build_object(
      'actor_user_id','90000000-0000-4000-8000-000000000001',
      'upload_id',v_correction_upload
    )
  );
  perform pg_temp.assert_true(
    v_result->>'status'='CORRECTION_READY'
      and (select current_complete_upload_id=v_d and version=3
           from public.weekly_source_cycles
           where id='90000000-0000-4000-8000-000000000011'),
    'Correction-ready evidence moved the current complete-source pointer'
  );
  v_result:=pg_temp.roster_begin(
    repeat('6',64),'roster-correction-ready-replay.csv',3,
    'FINAL_SOURCE_CORRECTION','90000000-0000-4000-8000-000000000060'
  );
  perform pg_temp.assert_true(
    v_result->>'status'='DUPLICATE'
      and (v_result->>'logical_upload_id')::uuid=v_correction_upload,
    'A correction upload exact replay was not idempotent after READY'
  );
  v_result:=pg_temp.roster_begin(
    repeat('5',64),'roster-correction-conflict.csv',3,
    'FINAL_SOURCE_CORRECTION','90000000-0000-4000-8000-000000000060'
  );
  perform pg_temp.assert_true(
    v_result->>'status'='CONFLICT'
      and v_result->>'reason_code'='CORRECTION_SESSION_REPLACEMENT_CONFLICT',
    'Changed correction bytes did not conflict with the session replacement'
  );
  update public.weekly_final_source_correction_sessions
  set state='CANCELLED',completed_at_utc=pg_catalog.transaction_timestamp()
  where id='90000000-0000-4000-8000-000000000060';

  insert into public.weekly_final_source_correction_sessions(
    id,source_cycle_id,authority_scope_kind,expected_current_final_revision_id,
    expected_final_manifest_hash,actor_user_id,reason,idempotency_key,
    request_hash,guard_fingerprint
  ) values (
    '90000000-0000-4000-8000-000000000062',
    '90000000-0000-4000-8000-000000000011','CYCLE',
    '90000000-0000-4000-8000-000000000063',
    pg_catalog.decode(repeat('3',64),'hex'),
    '90000000-0000-4000-8000-000000000001','Replace aborted correction upload',
    'verify-correction-abort',pg_catalog.decode(repeat('b',64),'hex'),
    pg_catalog.decode(repeat('7',64),'hex')
  );
  v_result:=pg_temp.roster_begin(
    repeat('4',64),'roster-correction-abort.csv',3,
    'FINAL_SOURCE_CORRECTION','90000000-0000-4000-8000-000000000062'
  );
  v_correction_rejected:=(v_result->>'logical_upload_id')::uuid;
  perform pg_temp.roster_stage_rows(v_correction_rejected,'CORRECTION-ABORT');
  v_result:=public.weekly_source_upload_abort_atomic_v1(
    pg_catalog.jsonb_build_object(
      'actor_user_id','90000000-0000-4000-8000-000000000001',
      'upload_id',v_correction_rejected
    )
  );
  perform pg_temp.assert_true(
    v_result->>'status'='REJECTED'
      and (select state='DRAFT' and replacement_correction_upload_id is null
           from public.weekly_final_source_correction_sessions
           where id='90000000-0000-4000-8000-000000000062'),
    'Aborting a correction upload did not release its session safely'
  );
  v_result:=pg_temp.roster_begin(
    repeat('3',64),'roster-correction-retry.csv',3,
    'FINAL_SOURCE_CORRECTION','90000000-0000-4000-8000-000000000062'
  );
  v_correction_retry:=(v_result->>'logical_upload_id')::uuid;
  perform pg_temp.roster_stage_rows(v_correction_retry,'CORRECTION-RETRY');
  v_result:=public.weekly_source_upload_seal_atomic_v1(
    pg_catalog.jsonb_build_object(
      'actor_user_id','90000000-0000-4000-8000-000000000001',
      'upload_id',v_correction_retry
    )
  );
  perform pg_temp.assert_true(
    v_result->>'status'='CORRECTION_READY',
    'A correction session could not stage a replacement after abort'
  );
  update public.weekly_final_source_correction_sessions
  set state='CANCELLED',completed_at_utc=pg_catalog.transaction_timestamp()
  where id='90000000-0000-4000-8000-000000000062';

  insert into public.weekly_final_source_correction_sessions(
    id,source_cycle_id,authority_scope_kind,expected_current_final_revision_id,
    expected_final_manifest_hash,actor_user_id,reason,idempotency_key,
    request_hash,guard_fingerprint
  ) values (
    '90000000-0000-4000-8000-000000000064',
    '90000000-0000-4000-8000-000000000011','CYCLE',
    '90000000-0000-4000-8000-000000000065',
    pg_catalog.decode(repeat('5',64),'hex'),
    '90000000-0000-4000-8000-000000000001','Replace rejected correction upload',
    'verify-correction-reject',pg_catalog.decode(repeat('c',64),'hex'),
    pg_catalog.decode(repeat('8',64),'hex')
  );
  v_result:=pg_temp.roster_begin(
    repeat('2',64),'roster-correction-count-mismatch.csv',4,
    'FINAL_SOURCE_CORRECTION','90000000-0000-4000-8000-000000000064'
  );
  v_correction_rejected:=(v_result->>'logical_upload_id')::uuid;
  perform pg_temp.roster_stage_rows(v_correction_rejected,'CORRECTION-REJECT');
  v_result:=public.weekly_source_upload_seal_atomic_v1(
    pg_catalog.jsonb_build_object(
      'actor_user_id','90000000-0000-4000-8000-000000000001',
      'upload_id',v_correction_rejected
    )
  );
  perform pg_temp.assert_true(
    v_result->>'status'='REJECTED'
      and (select state='DRAFT' and replacement_correction_upload_id is null
           from public.weekly_final_source_correction_sessions
           where id='90000000-0000-4000-8000-000000000064'),
    'A rejected correction upload did not release its session safely'
  );

  -- NHSP pre-final is useful checking evidence but can never become final authority.
  v_result:=public.weekly_source_upload_stage_begin_atomic_v1(
    pg_catalog.jsonb_build_object(
      'actor_user_id','90000000-0000-4000-8000-000000000001','environment','TEST',
      'agency_id','90000000-0000-4000-8000-000000000002',
      'source_group_id','90000000-0000-4000-8000-000000000030',
      'source_cycle_id','90000000-0000-4000-8000-000000000031',
      'original_filename','nhsp-prefinal.xlsx','content_sha256',repeat('9',64),'byte_count',4096,
      'profile_code','NHSP_PREFINAL_RELEASED_V1','profile_version',1,
      'parser_version','WEEKLY_SOURCE_STRICT_V1','normaliser_version','NHSP_PREFINAL_NORMALISER_V1',
      'workbook_part_and_sheet_fingerprint',repeat('8',64),
      'header_coordinate_map_json',pg_catalog.jsonb_build_object('Actual Start','A'),
      'money_lexical_authority_version','XLSX_BINARY64_SAME_VALUE_PENCE_V1',
      'purpose','ORDINARY','suggested_coverage_start_local_date','2026-09-14',
      'suggested_coverage_end_local_date','2026-09-14',
      'confirmed_coverage_start_local_date','2026-09-14',
      'confirmed_coverage_end_local_date','2026-09-14','coverage_timezone','Europe/London',
      'coverage_confirmation_version','FORMAT_MANIFEST_V1','coverage_state','COMPLETE',
      'coverage_proof_kind','FORMAT_MANIFEST','physical_row_count',2,'header_count',1,
      'trailer_count',0,'continuation_count',0,'accepted_count',1,
      'blocking_economic_duplicate_count',0,'malformed_count',0,'blocked_count',0,
      'file_metadata_json','{}'::jsonb,'parser_summary_json','{}'::jsonb
    )
  );
  v_prefinal:=(v_result->>'logical_upload_id')::uuid;
  v_result:=public.weekly_source_upload_stage_rows_atomic_v1(
    pg_catalog.jsonb_build_object(
      'actor_user_id','90000000-0000-4000-8000-000000000001','upload_id',v_prefinal,
      'physical_rows',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object('source_row_ordinal',1,'classification','HEADER','bounded_raw_cells_json',pg_catalog.jsonb_build_object('A','Header')),
        pg_catalog.jsonb_build_object('source_row_ordinal',2,'classification','ACCEPTED_SHIFT','bounded_raw_cells_json',pg_catalog.jsonb_build_object('A','09:00'))
      ),
      'normalised_rows',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object(
          'source_row_ordinal',2,'external_source_key','PREFINAL-1',
          'source_candidate_identity','Candidate NHSP','source_client_identity','Example Trust',
          'work_date','2026-09-14','start_at_local','2026-09-14T09:00:00',
          'end_at_local','2026-09-14T17:00:00','break_minutes',30,'actual_net_minutes',450,
          'row_finalisation_state','SOURCE_WORKED','source_money_parse_state','FORMULA',
          'source_expense_parse_state','NOT_APPLICABLE','bounded_raw_columns_json','{}'::jsonb
        )
      ),
      'money_evidence',pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object(
          'source_row_ordinal',2,'money_field_kind','COMMISSION','source_column_index',15,
          'cell_coordinate','P2','source_kind','XLSX_STRING_TOKEN',
          'original_token','=A1','decoded_token','5.00','cell_type_marker','f',
          'formula_present',true,'parse_state','FORMULA','parsed_pence',null
        ),
        pg_catalog.jsonb_build_object(
          'source_row_ordinal',2,'money_field_kind','TOTAL_COST','source_column_index',16,
          'cell_coordinate','Q2','source_kind','XLSX_NUMERIC_TOKEN',
          'original_token','15','decoded_token','15.00','cell_type_marker','n',
          'formula_present',false,'parse_state','VALID','parsed_pence',1500
        )
      ),
      'expense_evidence','[]'::jsonb
    )
  );
  v_result:=public.weekly_source_upload_seal_atomic_v1(
    pg_catalog.jsonb_build_object('actor_user_id','90000000-0000-4000-8000-000000000001','upload_id',v_prefinal)
  );
  perform pg_temp.assert_true(
    v_result->>'status'='CURRENT'
      and (select count(*)=2 from public.weekly_source_money_cell_evidence
           where upload_id=v_prefinal),
    'NHSP pre-final did not retain non-blocking Commission/Total Cost checking evidence'
  );
  v_result:=public.weekly_source_projection_begin_atomic_v1(
    pg_catalog.jsonb_build_object('actor_user_id','90000000-0000-4000-8000-000000000001',
      'upload_id',v_prefinal,'expected_authority_scope_version',1)
  );
  v_pub_prefinal:=(v_result->>'publication_id')::uuid;
  perform pg_temp.complete_projection(v_pub_prefinal);
  perform public.weekly_source_projection_publish_atomic_v1(
    pg_catalog.jsonb_build_object('actor_user_id','90000000-0000-4000-8000-000000000001',
      'publication_id',v_pub_prefinal)
  );
  begin
    perform private.weekly_source_current_publication_guard_v1(
      '90000000-0000-4000-8000-000000000031','CYCLE',null,v_prefinal,v_pub_prefinal,1
    );
    raise exception 'PREFINAL_GUARD_UNEXPECTEDLY_PASSED';
  exception when sqlstate '55000' then
    perform pg_temp.assert_true(
      sqlerrm='WEEKLY_SOURCE_PREFINAL_NOT_FINAL_AUTHORITY',
      'NHSP pre-final guard failed for wrong reason'
    );
  end;

  -- Each NHSP Trust has its own pointer/version and stable report identity.
  v_result:=pg_temp.nhsp_begin(
    '90000000-0000-4000-8000-000000000050','90000000-0000-4000-8000-000000000040',
    'BR-100',repeat('e',64)
  );
  v_nhsp_a:=(v_result->>'logical_upload_id')::uuid;
  perform pg_temp.nhsp_stage_rows(v_nhsp_a,'TRUST-A');
  v_result:=public.weekly_source_upload_seal_atomic_v1(
    pg_catalog.jsonb_build_object('actor_user_id','90000000-0000-4000-8000-000000000001','upload_id',v_nhsp_a)
  );
  perform pg_temp.assert_true(v_result->>'status'='CURRENT','Trust A report did not become current');

  v_result:=pg_temp.nhsp_begin(
    '90000000-0000-4000-8000-000000000051','90000000-0000-4000-8000-000000000041',
    'BR-200',repeat('f',64)
  );
  v_nhsp_b:=(v_result->>'logical_upload_id')::uuid;
  perform pg_temp.nhsp_stage_rows(v_nhsp_b,'TRUST-B',true);
  v_result:=public.weekly_source_upload_seal_atomic_v1(
    pg_catalog.jsonb_build_object('actor_user_id','90000000-0000-4000-8000-000000000001','upload_id',v_nhsp_b)
  );
  perform pg_temp.assert_true(
    v_result->>'status'='CURRENT'
      and (select current_complete_upload_id=v_nhsp_a and version=1
           from public.weekly_source_report_scopes where id='90000000-0000-4000-8000-000000000050')
      and (select current_complete_upload_id=v_nhsp_b and version=1
           from public.weekly_source_report_scopes where id='90000000-0000-4000-8000-000000000051'),
    'NHSP Trust scopes interfered with each other'
  );
  perform pg_temp.assert_true(
    (select source_money_parse_state='VALID'
         and source_commission_pence=0
         and source_total_cost_pence=0
         and source_shift_charge_pence=0
     from public.weekly_source_upload_rows
     where upload_id=v_nhsp_b),
    'a structurally valid £0 NHSP worked row did not survive publication as current evidence'
  );
  v_result:=pg_temp.nhsp_begin(
    '90000000-0000-4000-8000-000000000050','90000000-0000-4000-8000-000000000040',
    ' br-100 ',repeat('e',64),'WEEKLY_SOURCE_STRICT_V2'
  );
  perform pg_temp.assert_true(
    v_result->>'status'='DUPLICATE' and (v_result->>'logical_upload_id')::uuid=v_nhsp_a,
    'Identical NHSP business report did not replay idempotently across parser versions'
  );
  v_result:=pg_temp.nhsp_begin(
    '90000000-0000-4000-8000-000000000050','90000000-0000-4000-8000-000000000040',
    'BR-100',repeat('7',64)
  );
  perform pg_temp.assert_true(
    v_result->>'status'='CONFLICT'
      and v_result->>'reason_code'='NHSP_REPORT_IDENTITY_BYTES_CONFLICT'
      and (select current_complete_upload_id=v_nhsp_a and version=1
           from public.weekly_source_report_scopes where id='90000000-0000-4000-8000-000000000050'),
    'Changed bytes under the same NHSP report identity did not conflict safely'
  );
  v_result:=public.weekly_source_projection_begin_atomic_v1(
    pg_catalog.jsonb_build_object('actor_user_id','90000000-0000-4000-8000-000000000001',
      'upload_id',v_nhsp_a,'expected_authority_scope_version',1)
  );
  v_pub_nhsp:=(v_result->>'publication_id')::uuid;
  perform pg_temp.complete_projection(v_pub_nhsp);
  perform public.weekly_source_projection_publish_atomic_v1(
    pg_catalog.jsonb_build_object('actor_user_id','90000000-0000-4000-8000-000000000001',
      'publication_id',v_pub_nhsp)
  );
  perform private.weekly_source_current_publication_guard_v1(
    '90000000-0000-4000-8000-000000000031','NHSP_REPORT_SCOPE',
    '90000000-0000-4000-8000-000000000050',v_nhsp_a,v_pub_nhsp,1
  );

  select count(*) into v_count from public.weekly_source_upload_attempts
  where actor_user_id='90000000-0000-4000-8000-000000000001';
  perform pg_temp.assert_true(v_count=19,'Every completed/duplicate/conflict/rejected attempt was not logged exactly once');
  perform pg_temp.assert_true(
    (select count(*)=1 from public.weekly_source_upload_attempts
     where logical_upload_id=v_a and result='DUPLICATE'),
    'Duplicate attempt did not point to its original logical upload'
  );

  -- The real NHSP backing report can be a self-contained HTML table carrying
  -- an .xls filename. It has exact source-cell evidence but no OOXML part.
  v_result:=pg_temp.nhsp_begin(
    '90000000-0000-4000-8000-000000000050','90000000-0000-4000-8000-000000000040',
    'BR-HTML',repeat('9',64),'WEEKLY_SOURCE_STRICT_V1','HTML'
  );
  perform pg_temp.assert_true(
    v_result->>'status'='STAGING'
      and (select workbook_part_and_sheet_fingerprint is null
                 and parser_summary_json->>'source_kind'='HTML'
           from public.weekly_source_uploads
           where id=(v_result->>'logical_upload_id')::uuid),
    'Self-contained NHSP HTML backing report did not stage without an OOXML fingerprint'
  );
  perform pg_temp.nhsp_stage_rows(
    (v_result->>'logical_upload_id')::uuid,'TRUST-HTML',false,'HTML_DECODED_TEXT'
  );
  v_result:=public.weekly_source_upload_seal_atomic_v1(
    pg_catalog.jsonb_build_object(
      'actor_user_id','90000000-0000-4000-8000-000000000001',
      'upload_id',(v_result->>'logical_upload_id')::uuid
    )
  );
  perform pg_temp.assert_true(
    v_result->>'status'='CURRENT',
    'Self-contained NHSP HTML backing report did not seal decoded money evidence'
  );
  v_result:=pg_temp.nhsp_begin(
    '90000000-0000-4000-8000-000000000050','90000000-0000-4000-8000-000000000040',
    'BR-HTML-WRONG-MONEY-KIND',repeat('6',64),'WEEKLY_SOURCE_STRICT_V1','HTML'
  );
  perform pg_temp.nhsp_stage_rows((v_result->>'logical_upload_id')::uuid,'TRUST-HTML-WRONG');
  v_result:=public.weekly_source_upload_seal_atomic_v1(
    pg_catalog.jsonb_build_object(
      'actor_user_id','90000000-0000-4000-8000-000000000001',
      'upload_id',(v_result->>'logical_upload_id')::uuid
    )
  );
  perform pg_temp.assert_true(
    v_result->>'status'='REJECTED'
      and v_result->>'reason_code'='WEEKLY_SOURCE_NHSP_MONEY_EVIDENCE_INVALID',
    'HTML with XLSX money evidence was not rejected at seal'
  );
  begin
    perform pg_temp.nhsp_begin(
      '90000000-0000-4000-8000-000000000050','90000000-0000-4000-8000-000000000040',
      'BR-HTML-FORGED-PART',repeat('8',64),'WEEKLY_SOURCE_STRICT_V1','HTML',true
    );
    raise exception 'HTML_WITH_OOXML_FINGERPRINT_UNEXPECTEDLY_PASSED';
  exception when sqlstate '22023' then
    perform pg_temp.assert_true(
      sqlerrm='WEEKLY_SOURCE_WORKBOOK_FINGERPRINT_REQUIRED',
      'HTML with forged OOXML evidence failed for the wrong reason'
    );
  end;
  begin
    perform pg_temp.nhsp_begin(
      '90000000-0000-4000-8000-000000000050','90000000-0000-4000-8000-000000000040',
      'BR-UNKNOWN-CONTAINER',repeat('7',64),'WEEKLY_SOURCE_STRICT_V1','CSV'
    );
    raise exception 'UNKNOWN_WORKBOOK_CONTAINER_UNEXPECTEDLY_PASSED';
  exception when sqlstate '22023' then
    perform pg_temp.assert_true(
      sqlerrm='WEEKLY_SOURCE_WORKBOOK_FINGERPRINT_REQUIRED',
      'Unknown workbook container failed for the wrong reason'
    );
  end;

  begin
    update public.weekly_source_physical_rows set classification='HEADER'
    where upload_id=v_d and source_row_ordinal=2;
    raise exception 'IMMUTABLE_EVIDENCE_UPDATE_UNEXPECTEDLY_PASSED';
  exception when sqlstate '55000' then
    perform pg_temp.assert_true(sqlerrm='WEEKLY_SOURCE_IMMUTABLE_RECORD','Evidence guard failed for wrong reason');
  end;
  begin
    delete from public.weekly_source_upload_attempts where logical_upload_id=v_a;
    raise exception 'IMMUTABLE_ATTEMPT_DELETE_UNEXPECTEDLY_PASSED';
  exception when sqlstate '55000' then
    perform pg_temp.assert_true(sqlerrm='WEEKLY_SOURCE_IMMUTABLE_RECORD','Attempt guard failed for wrong reason');
  end;

  perform pg_temp.assert_true(
    not pg_catalog.has_table_privilege('service_role','public.weekly_source_uploads','select')
      and not pg_catalog.has_table_privilege('service_role','public.weekly_source_uploads','insert')
      and not pg_catalog.has_table_privilege('authenticated','public.weekly_source_uploads','select')
      and pg_catalog.has_function_privilege(
        'service_role','public.weekly_source_upload_stage_begin_atomic_v1(jsonb)','execute'
      )
      and not pg_catalog.has_function_privilege(
        'authenticated','public.weekly_source_upload_stage_begin_atomic_v1(jsonb)','execute'
      ),
    'Upload evidence escaped the reviewed service-only RPC ACL'
  );
  perform pg_temp.assert_true(
    (select p.prosecdef and r.rolname='postgres'
     from pg_catalog.pg_proc p
     join pg_catalog.pg_namespace n on n.oid=p.pronamespace
     join pg_catalog.pg_roles r on r.oid=p.proowner
     where n.nspname='public' and p.proname='weekly_source_upload_stage_begin_atomic_v1'),
    'Upload RPC is not SECURITY DEFINER owned by postgres'
  );

  perform pg_temp.assert_true((select count(*) from public.timesheets)=v_before_timesheets,
    'Upload/publication changed Timesheets');
  perform pg_temp.assert_true((select count(*) from public.invoices)=v_before_invoices,
    'Upload/publication changed invoices');
  perform pg_temp.assert_true((select count(*) from public.pay_batches)=v_before_pay_batches,
    'Upload/publication changed Banking Pay batches');
  perform pg_temp.assert_true((select count(*) from public.weekly_discrepancy_incidents)=v_before_incidents,
    'Upload/publication itself sent/reopened an incident');
end;
$verification$;

select 'weekly_source_upload_publication_v1 rollback proof passed' as verification_result;

rollback;
