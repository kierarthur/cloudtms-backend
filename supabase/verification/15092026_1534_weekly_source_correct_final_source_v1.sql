-- Rollback-only proof for the four-stage same-cycle Correct final source owner.
-- OPEN freezes the old authority and blocker/root census. REVIEW returns the
-- exact Office preview without creating live Timesheets or lineage. PREPARE
-- builds an inactive immutable replacement only after Office confirms the
-- sealed review. APPLY atomically swaps authority and reprojects the complete
-- union of prior and replacement ordinary Weekly roots.

\set ON_ERROR_STOP on

begin;
set local request.jwt.claim.role='service_role';
\set weekly_source_verification_outer_transaction true
\set weekly_source_ordinary_verification_outer_transaction true
\ir 15092026_1534_weekly_source_ordinary_pay_projection_v1.sql

create function pg_temp.correct_final_stage_replacement(
  p_correction_session_id uuid,
  p_upload_id uuid,
  p_publication_id uuid,
  p_upload_row_id uuid,
  p_start_at timestamp without time zone,
  p_end_at timestamp without time zone,
  p_break_minutes integer,
  p_net_minutes integer,
  p_total_pay_pence bigint,
  p_total_charge_pence bigint
) returns jsonb language plpgsql as $function$
declare
  v_session public.weekly_final_source_correction_sessions%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_prior_upload public.weekly_source_uploads%rowtype;
  v_row_hash bytea:=private.weekly_source_sha256_jsonb_v1(
    'CORRECT_FINAL_TEST_ROW',pg_catalog.to_jsonb(p_upload_row_id)
  );
  v_manifest_hash bytea:=private.weekly_source_sha256_jsonb_v1(
    'CORRECT_FINAL_TEST_ROW_MANIFEST',pg_catalog.to_jsonb(p_upload_id)
  );
  v_comparison_hash bytea;
  v_issue_hash bytea;
begin
  select * into strict v_session
  from public.weekly_final_source_correction_sessions
  where id=p_correction_session_id for update;
  select * into strict v_cycle
  from public.weekly_source_cycles where id=v_session.source_cycle_id for update;
  select * into strict v_prior_upload
  from public.weekly_source_uploads
  where id=(select revision.upload_id
            from public.weekly_source_final_revisions revision
            where revision.id=v_session.expected_current_final_revision_id);
  if v_session.state<>'DRAFT' then
    raise exception 'CORRECT_FINAL_TEST_SESSION_NOT_DRAFT';
  end if;

  insert into public.weekly_source_uploads(
    id,source_cycle_id,original_filename,content_sha256,byte_count,
    source_format_profile_id,parser_version,normaliser_version,
    header_coordinate_map_json,header_coordinate_map_hash,purpose,
    correction_session_id,declared_scope_fingerprint,
    suggested_coverage_start_local_date,suggested_coverage_end_local_date,
    confirmed_coverage_start_local_date,confirmed_coverage_end_local_date,
    coverage_timezone,coverage_confirmation_version,
    coverage_confirmed_by_user_id,coverage_confirmed_at_utc,
    coverage_shrink_acknowledged,coverage_state,coverage_proof_kind,
    physical_row_count,accepted_count,row_manifest_hash,state,
    uploaded_by_user_id,file_metadata_json
  ) values (
    p_upload_id,v_cycle.id,'correct-final-replacement.csv',
    private.weekly_source_sha256_jsonb_v1(
      'CORRECT_FINAL_TEST_CONTENT',pg_catalog.to_jsonb(p_upload_id)
    ),100,v_prior_upload.source_format_profile_id,
    'CORRECT_FINAL_TEST_PARSER_V1','CORRECT_FINAL_TEST_NORMALISER_V1',
    '{}'::jsonb,private.weekly_source_sha256_jsonb_v1(
      'CORRECT_FINAL_TEST_HEADERS',pg_catalog.to_jsonb(p_upload_id)
    ),'FINAL_SOURCE_CORRECTION',v_session.id,
    private.weekly_source_sha256_jsonb_v1(
      'CORRECT_FINAL_TEST_SCOPE',pg_catalog.to_jsonb(v_cycle.id)
    ),'2026-12-31','2026-12-31','2026-12-31','2026-12-31',
    'Europe/London','OFFICE_COMPLETE_EXPORT_ATTESTATION_V1',
    v_session.actor_user_id,pg_catalog.clock_timestamp(),false,'COMPLETE',
    'OFFICE_COMPLETE_EXPORT_ATTESTATION',1,1,v_manifest_hash,
    'CORRECTION_READY',v_session.actor_user_id,
    pg_catalog.jsonb_build_object(
      'client_id','a0000000-0000-4000-8000-000000000002'
    )
  );
  insert into public.weekly_source_upload_rows(
    id,upload_id,source_row_ordinal,external_source_key,
    source_candidate_identity,source_client_identity,work_date,start_at_local,
    end_at_local,break_minutes,actual_net_minutes,row_finalisation_state,
    source_money_parse_state,source_expense_pence,source_expense_parse_state,
    normalised_row_hash
  ) values (
    p_upload_row_id,p_upload_id,1,'CORRECT-FINAL-ROW','Finaliser Candidate',
    'Finaliser Roster Client','2026-12-31',p_start_at,p_end_at,
    p_break_minutes,p_net_minutes,'NOT_APPLICABLE','NOT_APPLICABLE',0,
    'OMITTED_ZERO',v_row_hash
  );
  update public.weekly_final_source_correction_sessions
  set state='READY',replacement_correction_upload_id=p_upload_id,
      version=version+1,updated_at_utc=pg_catalog.transaction_timestamp()
  where id=v_session.id and state='DRAFT';
  if not found then raise exception 'CORRECT_FINAL_TEST_UPLOAD_CAS_LOST'; end if;

  insert into public.weekly_source_projection_publications(
    id,source_cycle_id,authority_scope_kind,upload_id,correction_session_id,
    authority_scope_version,comparison_manifest_hash,issue_set_hash,state
  ) values (
    p_publication_id,v_cycle.id,'CYCLE',p_upload_id,v_session.id,v_cycle.version,
    private.weekly_source_sha256_jsonb_v1(
      'CORRECT_FINAL_TEST_COMPARISON_PLACEHOLDER',pg_catalog.to_jsonb(p_publication_id)
    ),private.weekly_source_sha256_jsonb_v1(
      'CORRECT_FINAL_TEST_ISSUES_PLACEHOLDER',pg_catalog.to_jsonb(p_publication_id)
    ),'BUILDING'
  );
  perform public.weekly_source_projection_rows_apply_atomic_v1(
    v_session.actor_user_id,p_publication_id,
    pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'upload_row_id',p_upload_row_id,'mapping_state','RESOLVED',
      'candidate_id','a0000000-0000-4000-8000-000000000003',
      'client_id','a0000000-0000-4000-8000-000000000002',
      'contract_id','a0000000-0000-4000-8000-000000000004',
      'contract_selection_method','AUTO_UNIQUE',
      'qualifying_contract_ids',pg_catalog.jsonb_build_array(
        'a0000000-0000-4000-8000-000000000004'
      ),'identity_kind','PROFILE_EXTERNAL_KEY',
      'profile_external_key','CORRECT-FINAL-ROW','link_kind','POSITIVE_SOURCE',
      'economic_snapshot',pg_catalog.jsonb_build_object(
        'schema_version','WEEKLY_SOURCE_CANONICAL_ECONOMIC_SNAPSHOT_V1',
        'calculator_version','WEEKLY_SHIFT_FINANCIAL_SEGMENT_V1',
        'source_mode','HEALTHROSTER_WEEKLY','rate_method','SPLIT_RATE_WINDOWS',
        'sign',1,'paid_minutes',p_net_minutes,'break_minutes',p_break_minutes,
        'bucket_minutes',pg_catalog.jsonb_build_object(
          'day',p_net_minutes,'night',0,'sat',0,'sun',0,'bh',0
        ),'hours',pg_catalog.jsonb_build_object(
          'day',p_net_minutes::numeric/60,'night',0,'sat',0,'sun',0,'bh',0
        ),'pay_rates',pg_catalog.jsonb_build_object(
          'day',10,'night',10,'sat',10,'sun',10,'bh',10
        ),'charge_rates',pg_catalog.jsonb_build_object(
          'day',20,'night',20,'sat',20,'sun',20,'bh',20
        ),'total_pay_pence',p_total_pay_pence::text,
        'calculated_charge_pence',p_total_charge_pence::text
      )
    ))
  );
  v_comparison_hash:=private.weekly_source_projection_comparison_manifest_hash_v1(
    p_publication_id
  );
  v_issue_hash:=private.weekly_source_projection_issue_set_hash_v1(
    p_publication_id
  );
  update public.weekly_source_projection_publications
  set comparison_manifest_hash=v_comparison_hash,issue_set_hash=v_issue_hash,
      state='CORRECTION_READY',published_at_utc=pg_catalog.transaction_timestamp()
  where id=p_publication_id and state='BUILDING';
  update public.weekly_final_source_correction_sessions
  set replacement_projection_publication_id=p_publication_id,
      version=version+1,updated_at_utc=pg_catalog.transaction_timestamp()
  where id=v_session.id and state='READY'
    and replacement_correction_upload_id=p_upload_id;
  if not found then raise exception 'CORRECT_FINAL_TEST_PUBLICATION_CAS_LOST'; end if;
  return pg_catalog.jsonb_build_object(
    'replacement_upload_id',p_upload_id,
    'replacement_projection_publication_id',p_publication_id,
    'expected_authority_scope_version',v_cycle.version,
    'expected_row_manifest_hash',pg_catalog.encode(v_manifest_hash,'hex'),
    'expected_comparison_manifest_hash',pg_catalog.encode(v_comparison_hash,'hex'),
    'expected_issue_set_hash',pg_catalog.encode(v_issue_hash,'hex'),
    'version',(select version from public.weekly_final_source_correction_sessions
               where id=v_session.id)
  );
end;
$function$;

create function pg_temp.correct_final_seed_nhsp_cycle(
  p_cycle_id uuid,
  p_scope_id uuid,
  p_upload_id uuid,
  p_publication_id uuid,
  p_upload_row_id uuid,
  p_backing_report_number text
) returns void language plpgsql as $function$
declare
  v_manifest_hash bytea:=private.weekly_source_sha256_jsonb_v1(
    'CORRECT_FINAL_NHSP_ORIGINAL_MANIFEST',pg_catalog.to_jsonb(p_upload_id)
  );
  v_comparison_hash bytea;
  v_issue_hash bytea;
begin
  insert into public.weekly_source_cycles(
    id,source_group_id,finalisation_week_ending,cutoff_at_utc,state,version,projection_state
  ) values (
    p_cycle_id,'b0000000-0000-4000-8000-000000000005','2027-01-03',
    pg_catalog.transaction_timestamp()-interval '1 day','OPEN',1,'REBUILDING'
  );
  insert into public.weekly_source_report_scopes(
    id,source_cycle_id,environment,agency_id,source_group_id,client_id,
    cutoff_at_utc,version,state,projection_state
  ) values (
    p_scope_id,p_cycle_id,'TEST','a0000000-0000-4000-8000-000000000006',
    'b0000000-0000-4000-8000-000000000005',
    'b0000000-0000-4000-8000-000000000002',
    pg_catalog.transaction_timestamp()-interval '1 day',1,'OPEN','REBUILDING'
  );
  insert into public.weekly_source_uploads(
    id,source_cycle_id,report_scope_id,original_filename,content_sha256,byte_count,
    source_format_profile_id,parser_version,normaliser_version,
    workbook_part_and_sheet_fingerprint,header_coordinate_map_json,
    header_coordinate_map_hash,money_lexical_authority_version,
    declared_scope_fingerprint,coverage_proof_kind,physical_row_count,accepted_count,
    row_manifest_hash,state,uploaded_by_user_id,file_metadata_json
  ) values (
    p_upload_id,p_cycle_id,p_scope_id,p_backing_report_number||'.xlsx',
    private.weekly_source_sha256_jsonb_v1(
      'CORRECT_FINAL_NHSP_ORIGINAL_CONTENT',pg_catalog.to_jsonb(p_upload_id)
    ),200,'32222222-2222-4222-8222-222222222222',
    'CORRECT_FINAL_NHSP_TEST_PARSER_V1','NHSP_BACKING_NORMALISER_V1',
    private.weekly_source_sha256_jsonb_v1(
      'CORRECT_FINAL_NHSP_ORIGINAL_WORKBOOK',pg_catalog.to_jsonb(p_upload_id)
    ),'{}'::jsonb,
    private.weekly_source_sha256_jsonb_v1(
      'CORRECT_FINAL_NHSP_ORIGINAL_HEADERS',pg_catalog.to_jsonb(p_upload_id)
    ),'XLSX_BINARY64_SAME_VALUE_PENCE_V1',
    private.weekly_source_sha256_jsonb_v1(
      'CORRECT_FINAL_NHSP_ORIGINAL_SCOPE',pg_catalog.to_jsonb(p_scope_id)
    ),'NHSP_TRUST_REPORT_SCOPE',1,1,v_manifest_hash,'CURRENT',
    'a0000000-0000-4000-8000-000000000001',
    pg_catalog.jsonb_build_object(
      'nhsp_report_number',p_backing_report_number,
      'nhsp_report_heading_name','Finaliser NHSP Trust'
    )
  );
  update public.weekly_source_report_scopes
  set current_complete_upload_id=p_upload_id
  where id=p_scope_id;
  insert into public.weekly_source_upload_rows(
    id,upload_id,source_row_ordinal,external_source_key,source_candidate_identity,
    source_client_identity,work_date,start_at_local,end_at_local,break_minutes,
    actual_net_minutes,row_finalisation_state,role_band_source,
    source_commission_pence,source_total_cost_pence,source_shift_charge_pence,
    source_money_parse_state,source_qualification_profile_version,
    source_expense_parse_state,normalised_row_hash
  ) values (
    p_upload_row_id,p_upload_id,1,'NHSP-CORRECT-FINAL-ORIGINAL',
    'Finaliser Candidate','Finaliser NHSP Trust','2026-12-30',
    '2026-12-30 09:00','2026-12-30 17:00',30,450,'SOURCE_WORKED','BAND 5',
    500,14500,15000,'VALID','NHSP_TWO_COMPONENT_PENCE_V1','NOT_APPLICABLE',
    private.weekly_source_sha256_jsonb_v1(
      'CORRECT_FINAL_NHSP_ORIGINAL_ROW',pg_catalog.to_jsonb(p_upload_row_id)
    )
  );
  insert into public.weekly_source_projection_publications(
    id,source_cycle_id,authority_scope_kind,report_scope_id,upload_id,
    authority_scope_version,comparison_manifest_hash,issue_set_hash,state
  ) values (
    p_publication_id,p_cycle_id,'NHSP_REPORT_SCOPE',p_scope_id,p_upload_id,1,
    private.weekly_source_sha256_jsonb_v1(
      'CORRECT_FINAL_NHSP_ORIGINAL_COMPARISON_PLACEHOLDER',pg_catalog.to_jsonb(p_publication_id)
    ),private.weekly_source_sha256_jsonb_v1(
      'CORRECT_FINAL_NHSP_ORIGINAL_ISSUES_PLACEHOLDER',pg_catalog.to_jsonb(p_publication_id)
    ),'BUILDING'
  );
  perform public.weekly_source_projection_rows_apply_atomic_v1(
    'a0000000-0000-4000-8000-000000000001',p_publication_id,
    pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'upload_row_id',p_upload_row_id,'mapping_state','RESOLVED',
      'candidate_id','a0000000-0000-4000-8000-000000000003',
      'client_id','b0000000-0000-4000-8000-000000000002',
      'contract_id','b0000000-0000-4000-8000-000000000004',
      'contract_selection_method','AUTO_UNIQUE',
      'qualifying_contract_ids',pg_catalog.jsonb_build_array(
        'b0000000-0000-4000-8000-000000000004'
      -- WP-58. NHSP work identity is the schedule tuple, never the Reference
      -- Number (pack 24 sections 1 and 9). This is what the broker sends for an
      -- NHSP profile, and since WP-58 it is the only kind
      -- weekly_source_projection_rows_apply_atomic_v1 will accept for one.
      ),'identity_kind','SCHEDULE_TUPLE','link_kind','POSITIVE_SOURCE',
      'economic_snapshot',pg_catalog.jsonb_build_object(
        'schema_version','WEEKLY_SOURCE_CANONICAL_ECONOMIC_SNAPSHOT_V1',
        'calculator_version','WEEKLY_SHIFT_FINANCIAL_SEGMENT_V1',
        'source_mode','NHSP_WEEKLY','rate_method','SPLIT_RATE_WINDOWS',
        'sign',1,'paid_minutes',450,'break_minutes',30,
        'bucket_minutes',pg_catalog.jsonb_build_object(
          'day',450,'night',0,'sat',0,'sun',0,'bh',0
        ),'hours',pg_catalog.jsonb_build_object(
          'day',7.5,'night',0,'sat',0,'sun',0,'bh',0
        ),'pay_rates',pg_catalog.jsonb_build_object(
          'day',10,'night',10,'sat',10,'sun',10,'bh',10
        ),'charge_rates',pg_catalog.jsonb_build_object(
          'day',20,'night',20,'sat',20,'sun',20,'bh',20
        ),'total_pay_pence','7500','calculated_charge_pence','15000'
      ),'charge_check',pg_catalog.jsonb_build_object(
        'row_sign_kind','POSITIVE','source_commission_pence','500',
        'source_total_cost_pence','14500','source_shift_charge_pence','15000',
        'calculated_segment_charge_pence','15000','comparison_result','EXACT',
        'comparison_reason_code','EXACT','phase_severity','NONE'
      )
    ))
  );
  v_comparison_hash:=private.weekly_source_projection_comparison_manifest_hash_v1(
    p_publication_id
  );
  v_issue_hash:=private.weekly_source_projection_issue_set_hash_v1(
    p_publication_id
  );
  update public.weekly_source_projection_publications
  set comparison_manifest_hash=v_comparison_hash,issue_set_hash=v_issue_hash,
      state='CURRENT',published_at_utc=pg_catalog.transaction_timestamp()
  where id=p_publication_id and state='BUILDING';
  update public.weekly_source_report_scopes
  set projection_state='CURRENT',current_projection_publication_id=p_publication_id
  where id=p_scope_id;
end;
$function$;

create function pg_temp.correct_final_stage_nhsp_replacement(
  p_correction_session_id uuid,
  p_upload_id uuid,
  p_publication_id uuid,
  p_upload_row_id uuid,
  p_backing_report_number text,
  p_source_total_cost_pence bigint,
  p_source_commission_pence bigint,
  p_source_shift_charge_pence bigint
) returns jsonb language plpgsql as $function$
declare
  v_session public.weekly_final_source_correction_sessions%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_prior_upload public.weekly_source_uploads%rowtype;
  v_row_hash bytea:=private.weekly_source_sha256_jsonb_v1(
    'CORRECT_FINAL_NHSP_TEST_ROW',pg_catalog.to_jsonb(p_upload_row_id)
  );
  v_manifest_hash bytea:=private.weekly_source_sha256_jsonb_v1(
    'CORRECT_FINAL_NHSP_TEST_ROW_MANIFEST',pg_catalog.to_jsonb(p_upload_id)
  );
  v_comparison_hash bytea;
  v_issue_hash bytea;
begin
  select * into strict v_session
  from public.weekly_final_source_correction_sessions
  where id=p_correction_session_id for update;
  select * into strict v_cycle
  from public.weekly_source_cycles where id=v_session.source_cycle_id for update;
  select * into strict v_prior_upload
  from public.weekly_source_uploads
  where id=(select revision.upload_id
            from public.weekly_source_final_revisions revision
            where revision.id=v_session.expected_current_final_revision_id);
  if v_session.state<>'DRAFT'
     or v_session.authority_scope_kind<>'NHSP_REPORT_SCOPE'
     or v_session.report_scope_id is null then
    raise exception 'CORRECT_FINAL_NHSP_TEST_SESSION_NOT_DRAFT';
  end if;

  insert into public.weekly_source_uploads(
    id,source_cycle_id,report_scope_id,original_filename,content_sha256,byte_count,
    source_format_profile_id,parser_version,normaliser_version,
    workbook_part_and_sheet_fingerprint,header_coordinate_map_json,
    header_coordinate_map_hash,money_lexical_authority_version,
    declared_scope_fingerprint,coverage_proof_kind,physical_row_count,accepted_count,
    row_manifest_hash,state,purpose,correction_session_id,
    uploaded_by_user_id,file_metadata_json
  ) values (
    p_upload_id,v_cycle.id,v_session.report_scope_id,
    p_backing_report_number||'.xlsx',
    private.weekly_source_sha256_jsonb_v1(
      'CORRECT_FINAL_NHSP_TEST_CONTENT',pg_catalog.to_jsonb(p_upload_id)
    ),200,v_prior_upload.source_format_profile_id,
    'CORRECT_FINAL_NHSP_TEST_PARSER_V1','NHSP_BACKING_NORMALISER_V1',
    private.weekly_source_sha256_jsonb_v1(
      'CORRECT_FINAL_NHSP_TEST_WORKBOOK',pg_catalog.to_jsonb(p_upload_id)
    ),'{}'::jsonb,
    private.weekly_source_sha256_jsonb_v1(
      'CORRECT_FINAL_NHSP_TEST_HEADERS',pg_catalog.to_jsonb(p_upload_id)
    ),'XLSX_BINARY64_SAME_VALUE_PENCE_V1',
    private.weekly_source_sha256_jsonb_v1(
      'CORRECT_FINAL_NHSP_TEST_SCOPE',pg_catalog.to_jsonb(v_session.report_scope_id)
    ),'NHSP_TRUST_REPORT_SCOPE',1,1,v_manifest_hash,'CORRECTION_READY',
    'FINAL_SOURCE_CORRECTION',v_session.id,v_session.actor_user_id,
    pg_catalog.jsonb_build_object(
      'nhsp_report_number',p_backing_report_number,
      'nhsp_report_heading_name','Finaliser NHSP Trust'
    )
  );
  insert into public.weekly_source_upload_rows(
    id,upload_id,source_row_ordinal,external_source_key,source_candidate_identity,
    source_client_identity,work_date,start_at_local,end_at_local,break_minutes,
    actual_net_minutes,row_finalisation_state,role_band_source,
    source_commission_pence,source_total_cost_pence,source_shift_charge_pence,
    source_money_parse_state,source_qualification_profile_version,
    source_expense_parse_state,normalised_row_hash
  ) values (
    p_upload_row_id,p_upload_id,1,'NHSP-CORRECT-FINAL-ORIGINAL',
    'Finaliser Candidate','Finaliser NHSP Trust','2026-12-30',
    '2026-12-30 09:00','2026-12-30 17:00',30,450,'SOURCE_WORKED','BAND 5',
    p_source_commission_pence,p_source_total_cost_pence,p_source_shift_charge_pence,
    'VALID','NHSP_TWO_COMPONENT_PENCE_V1','NOT_APPLICABLE',v_row_hash
  );
  update public.weekly_final_source_correction_sessions
  set state='READY',replacement_correction_upload_id=p_upload_id,
      version=version+1,updated_at_utc=pg_catalog.transaction_timestamp()
  where id=v_session.id and state='DRAFT';
  if not found then raise exception 'CORRECT_FINAL_NHSP_TEST_UPLOAD_CAS_LOST'; end if;

  insert into public.weekly_source_projection_publications(
    id,source_cycle_id,authority_scope_kind,report_scope_id,upload_id,
    correction_session_id,authority_scope_version,
    comparison_manifest_hash,issue_set_hash,state
  ) values (
    p_publication_id,v_cycle.id,'NHSP_REPORT_SCOPE',v_session.report_scope_id,
    p_upload_id,v_session.id,
    (select version from public.weekly_source_report_scopes
     where id=v_session.report_scope_id),
    private.weekly_source_sha256_jsonb_v1(
      'CORRECT_FINAL_NHSP_TEST_COMPARISON_PLACEHOLDER',pg_catalog.to_jsonb(p_publication_id)
    ),private.weekly_source_sha256_jsonb_v1(
      'CORRECT_FINAL_NHSP_TEST_ISSUES_PLACEHOLDER',pg_catalog.to_jsonb(p_publication_id)
    ),'BUILDING'
  );
  perform public.weekly_source_projection_rows_apply_atomic_v1(
    v_session.actor_user_id,p_publication_id,
    pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
      'upload_row_id',p_upload_row_id,'mapping_state','RESOLVED',
      'candidate_id','a0000000-0000-4000-8000-000000000003',
      'client_id','b0000000-0000-4000-8000-000000000002',
      'contract_id','b0000000-0000-4000-8000-000000000004',
      'contract_selection_method','AUTO_UNIQUE',
      'qualifying_contract_ids',pg_catalog.jsonb_build_array(
        'b0000000-0000-4000-8000-000000000004'
      -- WP-58. NHSP work identity is the schedule tuple, never the Reference
      -- Number (pack 24 sections 1 and 9). This is what the broker sends for an
      -- NHSP profile, and since WP-58 it is the only kind
      -- weekly_source_projection_rows_apply_atomic_v1 will accept for one.
      ),'identity_kind','SCHEDULE_TUPLE','link_kind','POSITIVE_SOURCE',
      'economic_snapshot',pg_catalog.jsonb_build_object(
        'schema_version','WEEKLY_SOURCE_CANONICAL_ECONOMIC_SNAPSHOT_V1',
        'calculator_version','WEEKLY_SHIFT_FINANCIAL_SEGMENT_V1',
        'source_mode','NHSP_WEEKLY','rate_method','SPLIT_RATE_WINDOWS',
        'sign',1,'paid_minutes',450,'break_minutes',30,
        'bucket_minutes',pg_catalog.jsonb_build_object(
          'day',450,'night',0,'sat',0,'sun',0,'bh',0
        ),'hours',pg_catalog.jsonb_build_object(
          'day',7.5,'night',0,'sat',0,'sun',0,'bh',0
        ),'pay_rates',pg_catalog.jsonb_build_object(
          'day',10,'night',10,'sat',10,'sun',10,'bh',10
        ),'charge_rates',pg_catalog.jsonb_build_object(
          'day',20,'night',20,'sat',20,'sun',20,'bh',20
        ),'total_pay_pence','7500','calculated_charge_pence','15000'
      ),
      'charge_check',pg_catalog.jsonb_build_object(
        'row_sign_kind','POSITIVE',
        'source_commission_pence',p_source_commission_pence::text,
        'source_total_cost_pence',p_source_total_cost_pence::text,
        'source_shift_charge_pence',p_source_shift_charge_pence::text,
        'calculated_segment_charge_pence','15000',
        'comparison_result',case when p_source_shift_charge_pence=15000
          then 'EXACT' else 'SOURCE_ROUNDING_EQUIVALENT' end,
        'comparison_reason_code',case when p_source_shift_charge_pence=15000
          then 'EXACT' else 'ONE_PENNY_SOURCE_ROUNDING' end,
        'phase_severity','NONE'
      )
    ))
  );
  v_comparison_hash:=private.weekly_source_projection_comparison_manifest_hash_v1(
    p_publication_id
  );
  v_issue_hash:=private.weekly_source_projection_issue_set_hash_v1(
    p_publication_id
  );
  update public.weekly_source_projection_publications
  set comparison_manifest_hash=v_comparison_hash,issue_set_hash=v_issue_hash,
      state='CORRECTION_READY',published_at_utc=pg_catalog.transaction_timestamp()
  where id=p_publication_id and state='BUILDING';
  update public.weekly_final_source_correction_sessions
  set replacement_projection_publication_id=p_publication_id,
      version=version+1,updated_at_utc=pg_catalog.transaction_timestamp()
  where id=v_session.id and state='READY'
    and replacement_correction_upload_id=p_upload_id;
  if not found then raise exception 'CORRECT_FINAL_NHSP_TEST_PUBLICATION_CAS_LOST'; end if;
  return pg_catalog.jsonb_build_object(
    'replacement_upload_id',p_upload_id,
    'replacement_projection_publication_id',p_publication_id,
    'expected_authority_scope_version',
      (select version from public.weekly_source_report_scopes
       where id=v_session.report_scope_id),
    'expected_row_manifest_hash',pg_catalog.encode(v_manifest_hash,'hex'),
    'expected_comparison_manifest_hash',pg_catalog.encode(v_comparison_hash,'hex'),
    'expected_issue_set_hash',pg_catalog.encode(v_issue_hash,'hex'),
    'version',(select version from public.weekly_final_source_correction_sessions
               where id=v_session.id)
  );
end;
$function$;

create function pg_temp.correct_final_prepared_service_snapshot(
  p_prepared_final_revision_id uuid,
  p_root_timesheet_id uuid
) returns jsonb language plpgsql as $function$
declare
  v_cycle_id uuid;
  v_prior_revision_id uuid;
  v_request jsonb;
  v_expected_expenses jsonb;
  v_service_snapshot jsonb;
  v_tsfin jsonb;
  v_old_expense_pay numeric:=0;
  v_old_expense_charge numeric:=0;
begin
  select revision.source_cycle_id,revision.predecessor_revision_id
    into strict v_cycle_id,v_prior_revision_id
  from public.weekly_source_final_revisions revision
  where revision.id=p_prepared_final_revision_id and revision.state='PREPARED';
  update public.weekly_source_final_revisions set state='SUPERSEDED'
  where id=v_prior_revision_id and state='CURRENT';
  update public.weekly_source_final_revisions set state='CURRENT'
  where id=p_prepared_final_revision_id and state='PREPARED';
  v_request:=pg_temp.ordinary_projection_request(
    v_cycle_id,'correct-final-snapshot-builder',p_root_timesheet_id
  );
  v_expected_expenses:=private.weekly_source_ordinary_projection_current_expenses_v1(
    p_root_timesheet_id,p_prepared_final_revision_id
  );
  update public.weekly_source_final_revisions set state='PREPARED'
  where id=p_prepared_final_revision_id and state='CURRENT';
  update public.weekly_source_final_revisions set state='CURRENT'
  where id=v_prior_revision_id and state='SUPERSEDED';
  v_service_snapshot:=v_request->'service_snapshot';
  v_tsfin:=v_service_snapshot->'tsfin_snapshot_json';
  if pg_catalog.jsonb_array_length(v_expected_expenses)=0
     and v_tsfin#>>'{expenses_evidence_manifest,schema_version}'=
       'WEEKLY_SOURCE_FIXED_EXPENSE_TSFIN_LINEAGE_V1' then
    v_old_expense_pay:=coalesce((v_tsfin->>'expenses_pay_ex_vat')::numeric,0);
    v_old_expense_charge:=coalesce((v_tsfin->>'expenses_charge_ex_vat')::numeric,0);
    v_tsfin:=v_tsfin||pg_catalog.jsonb_build_object(
      'expenses_pay_ex_vat',0,'expenses_charge_ex_vat',0,
      'expenses_description',null,'expenses_evidence_r2_key',null,
      'expenses_evidence_manifest','null'::jsonb,
      'total_pay_ex_vat',pg_catalog.round(
        (v_tsfin->>'total_pay_ex_vat')::numeric-v_old_expense_pay,2
      ),
      'total_charge_ex_vat',pg_catalog.round(
        (v_tsfin->>'total_charge_ex_vat')::numeric-v_old_expense_charge,2
      ),
      'invoice_breakdown_json',pg_catalog.jsonb_set(
        pg_catalog.jsonb_set(
          v_tsfin->'invoice_breakdown_json','{totals,total_pay_ex_vat}',
          pg_catalog.to_jsonb(pg_catalog.round(
            (v_tsfin#>>'{invoice_breakdown_json,totals,total_pay_ex_vat}')::numeric
              -v_old_expense_pay,2
          )),false
        ),'{totals,total_charge_ex_vat}',
        pg_catalog.to_jsonb(pg_catalog.round(
          (v_tsfin#>>'{invoice_breakdown_json,totals,total_charge_ex_vat}')::numeric
            -v_old_expense_charge,2
        )),false
      )
    );
    v_service_snapshot:=pg_catalog.jsonb_set(
      v_service_snapshot,'{tsfin_snapshot_json}',v_tsfin,false
    );
  end if;
  return v_service_snapshot;
end;
$function$;

-- Build one canonical projection entry for the four-root union proof below.
-- The dates are deliberately in separate contract weeks so one contract and
-- candidate still produce four distinct root Timesheets.
create function pg_temp.correct_final_union_projection_entry(
  p_upload_row_id uuid,
  p_external_key text,
  p_net_minutes integer,
  p_break_minutes integer,
  p_total_pay_pence bigint,
  p_total_charge_pence bigint
) returns jsonb language sql immutable as $function$
  select pg_catalog.jsonb_build_object(
    'upload_row_id',p_upload_row_id,'mapping_state','RESOLVED',
    'candidate_id','a0000000-0000-4000-8000-000000000003',
    'client_id','a0000000-0000-4000-8000-000000000002',
    'contract_id','a0000000-0000-4000-8000-000000000004',
    'contract_selection_method','AUTO_UNIQUE',
    'qualifying_contract_ids',pg_catalog.jsonb_build_array(
      'a0000000-0000-4000-8000-000000000004'
    ),
    'identity_kind','PROFILE_EXTERNAL_KEY','profile_external_key',p_external_key,
    'link_kind','POSITIVE_SOURCE',
    'economic_snapshot',pg_catalog.jsonb_build_object(
      'schema_version','WEEKLY_SOURCE_CANONICAL_ECONOMIC_SNAPSHOT_V1',
      'calculator_version','WEEKLY_SHIFT_FINANCIAL_SEGMENT_V1',
      'source_mode','HEALTHROSTER_WEEKLY','rate_method','SPLIT_RATE_WINDOWS',
      'sign',1,'paid_minutes',p_net_minutes,'break_minutes',p_break_minutes,
      'bucket_minutes',pg_catalog.jsonb_build_object(
        'day',p_net_minutes,'night',0,'sat',0,'sun',0,'bh',0
      ),
      'hours',pg_catalog.jsonb_build_object(
        'day',p_net_minutes::numeric/60,'night',0,'sat',0,'sun',0,'bh',0
      ),
      'pay_rates',pg_catalog.jsonb_build_object(
        'day',10,'night',10,'sat',10,'sun',10,'bh',10
      ),
      'charge_rates',pg_catalog.jsonb_build_object(
        'day',20,'night',20,'sat',20,'sun',20,'bh',20
      ),
      'total_pay_pence',p_total_pay_pence::text,
      'calculated_charge_pence',p_total_charge_pence::text
    )
  );
$function$;

create function pg_temp.correct_final_seed_union_prior(
  p_cycle_id uuid,
  p_upload_id uuid,
  p_publication_id uuid
) returns void language plpgsql as $function$
declare
  v_manifest_hash bytea:=private.weekly_source_sha256_jsonb_v1(
    'CORRECT_FINAL_UNION_PRIOR_MANIFEST',pg_catalog.to_jsonb(p_upload_id)
  );
  v_comparison_hash bytea;
  v_issue_hash bytea;
begin
  -- Reuse the ordinary-cycle owner to establish the exact scope/profile, but
  -- ask it for an empty upload before installing this proof's three rows.
  perform pg_temp.roster_cycle(
    p_cycle_id,p_upload_id,p_publication_id,
    'cf600000-0000-4000-8000-000000000099',
    '2027-04-04','35555555-5555-4555-8555-555555555555',
    'UNUSED-EMPTY','NOT_APPLICABLE','2027-03-01 09:00','2027-03-01 17:00',
    30,450,7500,15000,0,false,'2027-03-01','2027-03-01','2027-03-22'
  );
  update public.weekly_source_uploads
  set physical_row_count=3,accepted_count=3,row_manifest_hash=v_manifest_hash,
      suggested_coverage_start_local_date='2027-03-01',
      suggested_coverage_end_local_date='2027-03-22',
      confirmed_coverage_start_local_date='2027-03-01',
      confirmed_coverage_end_local_date='2027-03-22'
  where id=p_upload_id;
  insert into public.weekly_source_upload_rows(
    id,upload_id,source_row_ordinal,external_source_key,source_candidate_identity,
    source_client_identity,work_date,start_at_local,end_at_local,break_minutes,
    actual_net_minutes,row_finalisation_state,source_money_parse_state,
    source_expense_pence,source_expense_parse_state,normalised_row_hash
  ) values
    ('cf600000-0000-4000-8000-000000000004',p_upload_id,1,'UNION-CHANGED',
     'Finaliser Candidate','Finaliser Roster Client','2027-03-01',
     '2027-03-01 09:00','2027-03-01 17:00',30,450,'NOT_APPLICABLE',
     'NOT_APPLICABLE',0,'OMITTED_ZERO',private.weekly_source_sha256_jsonb_v1(
       'CORRECT_FINAL_UNION_PRIOR_ROW','"cf600000-0000-4000-8000-000000000004"'::jsonb)),
    ('cf600000-0000-4000-8000-000000000005',p_upload_id,2,'UNION-REMOVED',
     'Finaliser Candidate','Finaliser Roster Client','2027-03-08',
     '2027-03-08 09:00','2027-03-08 17:00',30,450,'NOT_APPLICABLE',
     'NOT_APPLICABLE',0,'OMITTED_ZERO',private.weekly_source_sha256_jsonb_v1(
       'CORRECT_FINAL_UNION_PRIOR_ROW','"cf600000-0000-4000-8000-000000000005"'::jsonb)),
    ('cf600000-0000-4000-8000-000000000006',p_upload_id,3,'UNION-UNCHANGED',
     'Finaliser Candidate','Finaliser Roster Client','2027-03-15',
     '2027-03-15 09:00','2027-03-15 17:00',30,450,'NOT_APPLICABLE',
     'NOT_APPLICABLE',0,'OMITTED_ZERO',private.weekly_source_sha256_jsonb_v1(
       'CORRECT_FINAL_UNION_PRIOR_ROW','"cf600000-0000-4000-8000-000000000006"'::jsonb));
  update public.weekly_source_projection_publications set state='BUILDING'
  where id=p_publication_id;
  update public.weekly_source_cycles set projection_state='REBUILDING'
  where id=p_cycle_id;
  perform public.weekly_source_projection_rows_apply_atomic_v1(
    'a0000000-0000-4000-8000-000000000001',p_publication_id,
    pg_catalog.jsonb_build_array(
      pg_temp.correct_final_union_projection_entry(
        'cf600000-0000-4000-8000-000000000004','UNION-CHANGED',450,30,7500,15000),
      pg_temp.correct_final_union_projection_entry(
        'cf600000-0000-4000-8000-000000000005','UNION-REMOVED',450,30,7500,15000),
      pg_temp.correct_final_union_projection_entry(
        'cf600000-0000-4000-8000-000000000006','UNION-UNCHANGED',450,30,7500,15000)
    )
  );
  v_comparison_hash:=private.weekly_source_projection_comparison_manifest_hash_v1(
    p_publication_id
  );
  v_issue_hash:=private.weekly_source_projection_issue_set_hash_v1(p_publication_id);
  update public.weekly_source_projection_publications
  set comparison_manifest_hash=v_comparison_hash,issue_set_hash=v_issue_hash,
      state='CURRENT',published_at_utc=pg_catalog.transaction_timestamp()
  where id=p_publication_id and state='BUILDING';
  update public.weekly_source_cycles
  set projection_state='CURRENT',current_projection_publication_id=p_publication_id
  where id=p_cycle_id;
end;
$function$;

create function pg_temp.correct_final_stage_union_replacement(
  p_correction_session_id uuid,
  p_upload_id uuid,
  p_publication_id uuid
) returns jsonb language plpgsql as $function$
declare
  v_session public.weekly_final_source_correction_sessions%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_prior_upload public.weekly_source_uploads%rowtype;
  v_manifest_hash bytea:=private.weekly_source_sha256_jsonb_v1(
    'CORRECT_FINAL_UNION_REPLACEMENT_MANIFEST',pg_catalog.to_jsonb(p_upload_id)
  );
  v_comparison_hash bytea;
  v_issue_hash bytea;
begin
  select * into strict v_session from public.weekly_final_source_correction_sessions
  where id=p_correction_session_id for update;
  select * into strict v_cycle from public.weekly_source_cycles
  where id=v_session.source_cycle_id for update;
  select upload_row.* into strict v_prior_upload
  from public.weekly_source_final_revisions revision
  join public.weekly_source_uploads upload_row on upload_row.id=revision.upload_id
  where revision.id=v_session.expected_current_final_revision_id;
  if v_session.state<>'DRAFT' then
    raise exception 'CORRECT_FINAL_UNION_SESSION_NOT_DRAFT';
  end if;
  insert into public.weekly_source_uploads(
    id,source_cycle_id,original_filename,content_sha256,byte_count,
    source_format_profile_id,parser_version,normaliser_version,
    header_coordinate_map_json,header_coordinate_map_hash,purpose,
    correction_session_id,declared_scope_fingerprint,
    suggested_coverage_start_local_date,suggested_coverage_end_local_date,
    confirmed_coverage_start_local_date,confirmed_coverage_end_local_date,
    coverage_timezone,coverage_confirmation_version,coverage_confirmed_by_user_id,
    coverage_confirmed_at_utc,coverage_shrink_acknowledged,coverage_state,
    coverage_proof_kind,physical_row_count,accepted_count,row_manifest_hash,state,
    uploaded_by_user_id,file_metadata_json
  ) values (
    p_upload_id,v_cycle.id,'correct-final-union-replacement.csv',
    private.weekly_source_sha256_jsonb_v1(
      'CORRECT_FINAL_UNION_REPLACEMENT_CONTENT',pg_catalog.to_jsonb(p_upload_id)),
    300,v_prior_upload.source_format_profile_id,'CORRECT_FINAL_UNION_PARSER_V1',
    'CORRECT_FINAL_UNION_NORMALISER_V1','{}'::jsonb,
    private.weekly_source_sha256_jsonb_v1(
      'CORRECT_FINAL_UNION_REPLACEMENT_HEADERS',pg_catalog.to_jsonb(p_upload_id)),
    'FINAL_SOURCE_CORRECTION',v_session.id,
    private.weekly_source_sha256_jsonb_v1(
      'CORRECT_FINAL_UNION_REPLACEMENT_SCOPE',pg_catalog.to_jsonb(v_cycle.id)),
    '2027-03-01','2027-03-22','2027-03-01','2027-03-22','Europe/London',
    'OFFICE_COMPLETE_EXPORT_ATTESTATION_V1',v_session.actor_user_id,
    pg_catalog.transaction_timestamp(),false,'COMPLETE',
    'OFFICE_COMPLETE_EXPORT_ATTESTATION',3,3,v_manifest_hash,'CORRECTION_READY',
    v_session.actor_user_id,pg_catalog.jsonb_build_object(
      'client_id','a0000000-0000-4000-8000-000000000002')
  );
  insert into public.weekly_source_upload_rows(
    id,upload_id,source_row_ordinal,external_source_key,source_candidate_identity,
    source_client_identity,work_date,start_at_local,end_at_local,break_minutes,
    actual_net_minutes,row_finalisation_state,source_money_parse_state,
    source_expense_pence,source_expense_parse_state,normalised_row_hash
  ) values
    ('cf700000-0000-4000-8000-000000000004',p_upload_id,1,'UNION-CHANGED',
     'Finaliser Candidate','Finaliser Roster Client','2027-03-01',
     '2027-03-01 09:00','2027-03-01 18:00',30,510,'NOT_APPLICABLE',
     'NOT_APPLICABLE',0,'OMITTED_ZERO',private.weekly_source_sha256_jsonb_v1(
       'CORRECT_FINAL_UNION_REPLACEMENT_ROW','"cf700000-0000-4000-8000-000000000004"'::jsonb)),
    ('cf700000-0000-4000-8000-000000000005',p_upload_id,2,'UNION-UNCHANGED',
     'Finaliser Candidate','Finaliser Roster Client','2027-03-15',
     '2027-03-15 09:00','2027-03-15 17:00',30,450,'NOT_APPLICABLE',
     'NOT_APPLICABLE',0,'OMITTED_ZERO',private.weekly_source_sha256_jsonb_v1(
       'CORRECT_FINAL_UNION_REPLACEMENT_ROW','"cf700000-0000-4000-8000-000000000005"'::jsonb)),
    ('cf700000-0000-4000-8000-000000000006',p_upload_id,3,'UNION-ADDED',
     'Finaliser Candidate','Finaliser Roster Client','2027-03-22',
     '2027-03-22 09:00','2027-03-22 17:00',30,450,'NOT_APPLICABLE',
     'NOT_APPLICABLE',0,'OMITTED_ZERO',private.weekly_source_sha256_jsonb_v1(
       'CORRECT_FINAL_UNION_REPLACEMENT_ROW','"cf700000-0000-4000-8000-000000000006"'::jsonb));
  update public.weekly_final_source_correction_sessions
  set state='READY',replacement_correction_upload_id=p_upload_id,
      version=version+1,updated_at_utc=pg_catalog.transaction_timestamp()
  where id=v_session.id and state='DRAFT';
  if not found then raise exception 'CORRECT_FINAL_UNION_UPLOAD_CAS_LOST'; end if;
  insert into public.weekly_source_projection_publications(
    id,source_cycle_id,authority_scope_kind,upload_id,correction_session_id,
    authority_scope_version,comparison_manifest_hash,issue_set_hash,state
  ) values (
    p_publication_id,v_cycle.id,'CYCLE',p_upload_id,v_session.id,v_cycle.version,
    private.weekly_source_sha256_jsonb_v1(
      'CORRECT_FINAL_UNION_COMPARISON_PLACEHOLDER',pg_catalog.to_jsonb(p_publication_id)),
    private.weekly_source_sha256_jsonb_v1(
      'CORRECT_FINAL_UNION_ISSUES_PLACEHOLDER',pg_catalog.to_jsonb(p_publication_id)),
    'BUILDING'
  );
  perform public.weekly_source_projection_rows_apply_atomic_v1(
    v_session.actor_user_id,p_publication_id,
    pg_catalog.jsonb_build_array(
      pg_temp.correct_final_union_projection_entry(
        'cf700000-0000-4000-8000-000000000004','UNION-CHANGED',510,30,8500,17000),
      pg_temp.correct_final_union_projection_entry(
        'cf700000-0000-4000-8000-000000000005','UNION-UNCHANGED',450,30,7500,15000),
      pg_temp.correct_final_union_projection_entry(
        'cf700000-0000-4000-8000-000000000006','UNION-ADDED',450,30,7500,15000)
    )
  );
  v_comparison_hash:=private.weekly_source_projection_comparison_manifest_hash_v1(
    p_publication_id
  );
  v_issue_hash:=private.weekly_source_projection_issue_set_hash_v1(p_publication_id);
  update public.weekly_source_projection_publications
  set comparison_manifest_hash=v_comparison_hash,issue_set_hash=v_issue_hash,
      state='CORRECTION_READY',published_at_utc=pg_catalog.transaction_timestamp()
  where id=p_publication_id and state='BUILDING';
  update public.weekly_final_source_correction_sessions
  set replacement_projection_publication_id=p_publication_id,
      version=version+1,updated_at_utc=pg_catalog.transaction_timestamp()
  where id=v_session.id and state='READY'
    and replacement_correction_upload_id=p_upload_id;
  if not found then raise exception 'CORRECT_FINAL_UNION_PUBLICATION_CAS_LOST'; end if;
  return pg_catalog.jsonb_build_object(
    'replacement_upload_id',p_upload_id,
    'replacement_projection_publication_id',p_publication_id,
    'expected_authority_scope_version',v_cycle.version,
    'expected_row_manifest_hash',pg_catalog.encode(v_manifest_hash,'hex'),
    'expected_comparison_manifest_hash',pg_catalog.encode(v_comparison_hash,'hex'),
    'expected_issue_set_hash',pg_catalog.encode(v_issue_hash,'hex'),
    'version',(select version from public.weekly_final_source_correction_sessions
               where id=v_session.id)
  );
end;
$function$;

create temp table correct_final_state (
  source_cycle_id uuid not null,
  prior_final_revision_id uuid not null,
  prior_upload_id uuid not null,
  prior_publication_id uuid not null,
  root_timesheet_id uuid not null,
  correction_session_id uuid not null,
  replacement_upload_id uuid not null,
  replacement_publication_id uuid not null,
  review_result jsonb,
  prepare_result jsonb,
  prepared_service_snapshot jsonb
);

create function pg_temp.correct_final_apply_request(
  p_idempotency_key text,
  p_root_service_snapshots jsonb default null
) returns jsonb language sql stable as $function$
  select pg_catalog.jsonb_build_object(
    'schema_version','WEEKLY_SOURCE_CORRECT_FINAL_APPLY_V1',
    'actor_user_id','a0000000-0000-4000-8000-000000000001',
    'correction_session_id',state.correction_session_id,
    'replacement_upload_id',state.replacement_upload_id,
    'replacement_projection_publication_id',state.replacement_publication_id,
    'expected_session_version',(state.prepare_result->>'version')::bigint,
    'expected_authority_scope_version',1,
    'expected_row_manifest_hash',pg_catalog.encode(upload_row.row_manifest_hash,'hex'),
    'expected_comparison_manifest_hash',pg_catalog.encode(publication.comparison_manifest_hash,'hex'),
    'expected_issue_set_hash',pg_catalog.encode(publication.issue_set_hash,'hex'),
    'expected_preview_hash',state.prepare_result->'office_preview'->>'preview_hash',
    'reason','Replace the mistaken same-cycle final source.',
    'confirmation_text',state.prepare_result->'office_preview'->>'confirmation_text',
    'idempotency_key',p_idempotency_key,
    'root_service_snapshots',coalesce(p_root_service_snapshots,
      pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'root_timesheet_id',state.root_timesheet_id,
        'prepared_context_hash',state.prepare_result->'root_contexts'->0->>'prepared_context_hash',
        'service_snapshot',state.prepared_service_snapshot
      )))
  )
  from pg_temp.correct_final_state state
  join public.weekly_source_uploads upload_row
    on upload_row.id=state.replacement_upload_id
  join public.weekly_source_projection_publications publication
    on publication.id=state.replacement_publication_id;
$function$;

create function pg_temp.correct_final_open_request(
  p_idempotency_key text,
  p_reason text default 'Replace the mistaken same-cycle final source.'
) returns jsonb language sql stable as $function$
  select pg_catalog.jsonb_build_object(
    'schema_version','WEEKLY_SOURCE_CORRECT_FINAL_OPEN_V1',
    'actor_user_id','a0000000-0000-4000-8000-000000000001',
    'source_cycle_id',state.source_cycle_id,'authority_scope_kind','CYCLE',
    'report_scope_id',null,
    'expected_current_final_revision_id',state.prior_final_revision_id,
    'expected_final_manifest_hash',pg_catalog.encode(revision.manifest_hash,'hex'),
    'idempotency_key',p_idempotency_key,'reason',p_reason
  )
  from pg_temp.correct_final_state state
  join public.weekly_source_final_revisions revision
    on revision.id=state.prior_final_revision_id;
$function$;

create function pg_temp.correct_final_review_request(
  p_idempotency_key text,
  p_expected_scope_version bigint default null
) returns jsonb language sql stable as $function$
  select pg_catalog.jsonb_build_object(
    'schema_version','WEEKLY_SOURCE_CORRECT_FINAL_REVIEW_V1',
    'actor_user_id','a0000000-0000-4000-8000-000000000001',
    'correction_session_id',state.correction_session_id,
    'replacement_upload_id',state.replacement_upload_id,
    'replacement_projection_publication_id',state.replacement_publication_id,
    'expected_session_version',case when state.review_result is null
      then session.version else (state.review_result->>'version')::bigint-1 end,
    'expected_authority_scope_version',coalesce(
      p_expected_scope_version,publication.authority_scope_version
    ),
    'expected_row_manifest_hash',pg_catalog.encode(upload_row.row_manifest_hash,'hex'),
    'expected_comparison_manifest_hash',pg_catalog.encode(publication.comparison_manifest_hash,'hex'),
    'expected_issue_set_hash',pg_catalog.encode(publication.issue_set_hash,'hex'),
    'idempotency_key',p_idempotency_key
  )
  from pg_temp.correct_final_state state
  join public.weekly_final_source_correction_sessions session
    on session.id=state.correction_session_id
  join public.weekly_source_uploads upload_row
    on upload_row.id=state.replacement_upload_id
  join public.weekly_source_projection_publications publication
    on publication.id=state.replacement_publication_id;
$function$;

create function pg_temp.correct_final_prepare_request(
  p_idempotency_key text,
  p_expected_scope_version bigint default null
) returns jsonb language sql stable as $function$
  select pg_catalog.jsonb_build_object(
    'schema_version','WEEKLY_SOURCE_CORRECT_FINAL_PREPARE_V1',
    'actor_user_id','a0000000-0000-4000-8000-000000000001',
    'correction_session_id',state.correction_session_id,
    'replacement_upload_id',state.replacement_upload_id,
    'replacement_projection_publication_id',state.replacement_publication_id,
    'expected_session_version',(state.review_result->>'version')::bigint,
    'expected_authority_scope_version',coalesce(
      p_expected_scope_version,publication.authority_scope_version
    ),
    'expected_row_manifest_hash',pg_catalog.encode(upload_row.row_manifest_hash,'hex'),
    'expected_comparison_manifest_hash',pg_catalog.encode(publication.comparison_manifest_hash,'hex'),
    'expected_issue_set_hash',pg_catalog.encode(publication.issue_set_hash,'hex'),
    'expected_preview_hash',state.review_result->'office_preview'->>'preview_hash',
    'reason','Replace the mistaken same-cycle final source.',
    'confirmation_text',state.review_result->'office_preview'->>'confirmation_text',
    'idempotency_key',p_idempotency_key
  )
  from pg_temp.correct_final_state state
  join public.weekly_final_source_correction_sessions session
    on session.id=state.correction_session_id
  join public.weekly_source_uploads upload_row
    on upload_row.id=state.replacement_upload_id
  join public.weekly_source_projection_publications publication
    on publication.id=state.replacement_publication_id;
$function$;

-- The inherited finalisation verifier deliberately leaves a later Contract
-- override with source-fixed expenses disabled. Re-enable them for this
-- correction fixture so the replacement proves a real source-expense-to-zero
-- transition rather than a synthetic snapshot edit.
update public.weekly_source_contract_policies
set effective_to='2026-12-30'
where id='f0000000-0000-4000-8000-000000000001'
  and effective_from='2026-04-01' and effective_to is null;
insert into public.weekly_source_contract_policies(
  id,contract_id,effective_from,source_fixed_expenses_enabled_override,
  created_by_user_id
) values (
  'cf000000-0000-4000-8000-000000000010',
  'a0000000-0000-4000-8000-000000000004','2026-12-31',true,
  'a0000000-0000-4000-8000-000000000001'
);

select pg_temp.roster_cycle(
  'cf100000-0000-4000-8000-000000000001',
  'cf100000-0000-4000-8000-000000000002',
  'cf100000-0000-4000-8000-000000000003',
  'cf100000-0000-4000-8000-000000000004',
  '2027-01-10','35555555-5555-4555-8555-555555555555',
  'CORRECT-FINAL-ROW','NOT_APPLICABLE','2026-12-31 09:00',
  '2026-12-31 17:00',30,450,7500,15000,1234,true,
  '2026-12-31','2026-12-31','2026-12-31'
);
select pg_temp.finalise_cycle(
  'cf100000-0000-4000-8000-000000000001',
  'cf100000-0000-4000-8000-000000000002',
  'cf100000-0000-4000-8000-000000000003'
);
select public.weekly_source_ordinary_pay_projection_apply_atomic_v1(
  pg_temp.ordinary_projection_request(
    'cf100000-0000-4000-8000-000000000001','correct-final-original'
  )
);
select pg_temp.assert_true(
  exists(
    select 1
    from public.weekly_source_cycles cycle
    join public.weekly_source_ordinary_pay_projection_receipts receipt
      on receipt.final_revision_id=cycle.current_final_revision_id
    join public.timesheets_financials financial
      on financial.timesheet_id=receipt.root_timesheet_id and financial.is_current
    where cycle.id='cf100000-0000-4000-8000-000000000001'
      and receipt.idempotency_key='correct-final-original'
      and financial.expenses_pay_ex_vat=12.34
      and financial.expenses_charge_ex_vat=12.34
      and financial.expenses_evidence_manifest->>'schema_version'=
        'WEEKLY_SOURCE_FIXED_EXPENSE_TSFIN_LINEAGE_V1'
  ),
  'the original final authority must publish its source-fixed expense before correction'
);

insert into pg_temp.correct_final_state(
  source_cycle_id,prior_final_revision_id,prior_upload_id,prior_publication_id,
  root_timesheet_id,correction_session_id,replacement_upload_id,
  replacement_publication_id,review_result,prepare_result,prepared_service_snapshot
)
select cycle.id as source_cycle_id,cycle.current_final_revision_id as prior_final_revision_id,
       cycle.current_complete_upload_id as prior_upload_id,
       cycle.current_projection_publication_id as prior_publication_id,
       receipt.root_timesheet_id,'00000000-0000-0000-0000-000000000000'::uuid
         as correction_session_id,
       'cf200000-0000-4000-8000-000000000002'::uuid as replacement_upload_id,
       'cf200000-0000-4000-8000-000000000003'::uuid as replacement_publication_id,
       null::jsonb as review_result,null::jsonb as prepare_result,
       null::jsonb as prepared_service_snapshot
from public.weekly_source_cycles cycle
join public.weekly_source_ordinary_pay_projection_receipts receipt
  on receipt.final_revision_id=cycle.current_final_revision_id
where cycle.id='cf100000-0000-4000-8000-000000000001'
  and receipt.idempotency_key='correct-final-original';

with opened as (
  select public.weekly_source_correct_final_open_atomic_v1(
    pg_temp.correct_final_open_request('correct-final-open-happy')
  ) result
)
update pg_temp.correct_final_state state
set correction_session_id=(opened.result->>'correction_session_id')::uuid
from opened;

select pg_temp.correct_final_stage_replacement(
  state.correction_session_id,state.replacement_upload_id,
  state.replacement_publication_id,'cf200000-0000-4000-8000-000000000004',
  '2026-12-31 09:00','2026-12-31 18:00',30,510,8500,17000
)
from pg_temp.correct_final_state state;

do $source_family_mismatch$
declare v_upload_id uuid;
begin
  select replacement_upload_id into strict v_upload_id
  from pg_temp.correct_final_state;
  begin
    update public.weekly_source_uploads
    set source_format_profile_id='32222222-2222-4222-8222-222222222222'
    where id=v_upload_id;
    perform public.weekly_source_correct_final_review_atomic_v1(
      pg_temp.correct_final_review_request('correct-final-wrong-family')
    );
    raise exception 'ASSERTION_FAILED: replacement source family mismatch was accepted';
  exception when sqlstate '55000' then
    if sqlerrm<>'WEEKLY_SOURCE_CORRECTION_SOURCE_FAMILY_MISMATCH' then raise; end if;
  end;
end;
$source_family_mismatch$;

do $side_effect_free_review$
declare
  v_timesheets_before bigint;
  v_contract_weeks_before bigint;
  v_lineage_before bigint;
  v_tsfin_before bigint;
  v_review jsonb;
begin
  select pg_catalog.count(*) into v_timesheets_before from public.timesheets;
  select pg_catalog.count(*) into v_contract_weeks_before from public.contract_weeks;
  select pg_catalog.count(*) into v_lineage_before
  from public.weekly_source_row_timesheet_lineages;
  select pg_catalog.count(*) into v_tsfin_before from public.timesheets_financials;

  v_review:=public.weekly_source_correct_final_review_atomic_v1(
    pg_temp.correct_final_review_request('correct-final-review-happy')
  );
  if v_review->>'status'<>'READY_FOR_CONFIRMATION'
     or (v_review->>'version')::bigint<=0 then
    raise exception 'ASSERTION_FAILED: correction REVIEW was not sealed';
  end if;
  if (select pg_catalog.count(*) from public.timesheets)<>v_timesheets_before
     or (select pg_catalog.count(*) from public.contract_weeks)<>v_contract_weeks_before
     or (select pg_catalog.count(*) from public.weekly_source_row_timesheet_lineages)<>v_lineage_before
     or (select pg_catalog.count(*) from public.timesheets_financials)<>v_tsfin_before then
    raise exception 'ASSERTION_FAILED: correction REVIEW changed live Timesheet or lineage state';
  end if;
  update pg_temp.correct_final_state set review_result=v_review;
end;
$side_effect_free_review$;

select pg_temp.assert_true(
  (public.weekly_source_correct_final_review_atomic_v1(
    pg_temp.correct_final_review_request('correct-final-review-happy')
  )->>'idempotent_replay')::boolean,
  'identical REVIEW retry must replay the sealed side-effect-free review'
);

with prepared as (
  select public.weekly_source_correct_final_prepare_atomic_v1(
    pg_temp.correct_final_prepare_request('correct-final-prepare-happy')
  ) result
)
update pg_temp.correct_final_state state
set prepare_result=prepared.result
from prepared;

select pg_temp.assert_true(
  (select revision.state='CURRENT' and upload_row.state='CURRENT'
          and publication.state='CURRENT'
          and cycle.current_final_revision_id=state.prior_final_revision_id
          and session.state='PREPARED'
          and prepared_revision.state='PREPARED'
          and replacement_upload.state='CORRECTION_READY'
          and replacement_publication.state='CORRECTION_READY'
   from pg_temp.correct_final_state state
   join public.weekly_source_cycles cycle on cycle.id=state.source_cycle_id
   join public.weekly_source_final_revisions revision
     on revision.id=state.prior_final_revision_id
   join public.weekly_source_uploads upload_row on upload_row.id=state.prior_upload_id
   join public.weekly_source_projection_publications publication
     on publication.id=state.prior_publication_id
   join public.weekly_final_source_correction_sessions session
     on session.id=state.correction_session_id
   join public.weekly_source_final_revisions prepared_revision
     on prepared_revision.id=(state.prepare_result->>'final_revision_id')::uuid
   join public.weekly_source_uploads replacement_upload
     on replacement_upload.id=state.replacement_upload_id
   join public.weekly_source_projection_publications replacement_publication
     on replacement_publication.id=state.replacement_publication_id),
  'PREPARE must preserve old CURRENT authority and keep replacement inactive'
);

update pg_temp.correct_final_state state
set prepared_service_snapshot=pg_temp.correct_final_prepared_service_snapshot(
  (state.prepare_result->>'final_revision_id')::uuid,state.root_timesheet_id
);

select pg_temp.assert_true(
  (public.weekly_source_correct_final_prepare_atomic_v1(
    pg_temp.correct_final_prepare_request('correct-final-prepare-happy')
  )->>'idempotent_replay')::boolean,
  'identical PREPARE retry must replay the sealed inactive replacement'
);

do $prepare_idempotency_collision$
begin
  begin
    perform public.weekly_source_correct_final_prepare_atomic_v1(
      pg_temp.correct_final_prepare_request('correct-final-prepare-happy',2)
    );
    raise exception 'ASSERTION_FAILED: changed PREPARE reused an idempotency key';
  exception when sqlstate '22023' then
    if sqlerrm<>'WEEKLY_SOURCE_CORRECTION_PREPARE_IDEMPOTENCY_COLLISION' then raise; end if;
  end;
end;
$prepare_idempotency_collision$;

do $correction_descendant_blocker$
declare v_prior_revision_id uuid; v_actor constant uuid:=
  'a0000000-0000-4000-8000-000000000001';
begin
  select prior_final_revision_id into strict v_prior_revision_id
  from pg_temp.correct_final_state;
  begin
    perform private.weekly_source_correct_final_preconditions_v1(
      v_prior_revision_id,v_actor,null::uuid,false
    );
    raise exception 'ASSERTION_FAILED: existing correction descendant was accepted';
  exception when sqlstate '55000' then
    if sqlerrm<>'WEEKLY_SOURCE_CORRECTION_DESCENDANT_EXISTS' then raise; end if;
  end;
end;
$correction_descendant_blocker$;

do $active_pay_draft_blocker$
declare
  v_root uuid;
  v_candidate uuid;
  v_batch constant uuid:='cf300000-0000-4000-8000-000000000001';
  v_batch_candidate constant uuid:='cf300000-0000-4000-8000-000000000002';
begin
  select state.root_timesheet_id,contract.candidate_id into strict v_root,v_candidate
  from pg_temp.correct_final_state state
  join public.timesheets timesheet_row on timesheet_row.timesheet_id=state.root_timesheet_id
  join public.contracts contract on contract.id=timesheet_row.contract_id;
  begin
    insert into public.pay_batches(
      id,pay_date,status,banking_system_snapshot,external_paye_system_snapshot
    ) values (v_batch,'2027-01-08','DRAFT','MONZO_CSV','CSV');
    insert into public.pay_batch_candidates(id,pay_batch_id,candidate_id)
    values (v_batch_candidate,v_batch,v_candidate);
    insert into public.pay_batch_items(
      id,pay_batch_candidate_id,item_type,timesheet_id,pay_channel,
      amount_ex_vat,amount_vat,amount_inc_vat
    ) values (
      'cf300000-0000-4000-8000-000000000003',v_batch_candidate,
      'TIMESHEET',v_root,'PAYE',75,0,75
    );
    perform public.weekly_source_correct_final_apply_atomic_v1(
      pg_temp.correct_final_apply_request('correct-final-active-pay-draft')
    );
    raise exception 'ASSERTION_FAILED: active Banking Draft was accepted';
  exception when sqlstate '55000' then
    if sqlerrm<>'WEEKLY_SOURCE_CORRECTION_ACTIVE_PAY_DRAFT' then raise; end if;
  end;
end;
$active_pay_draft_blocker$;

do $paid_root_blocker$
declare v_root uuid;
begin
  select root_timesheet_id into strict v_root from pg_temp.correct_final_state;
  begin
    update public.timesheets_financials
    set paid_at_utc=pg_catalog.statement_timestamp()
    where timesheet_id=v_root and is_current;
    perform public.weekly_source_correct_final_apply_atomic_v1(
      pg_temp.correct_final_apply_request('correct-final-paid-root')
    );
    raise exception 'ASSERTION_FAILED: paid root was accepted';
  exception when sqlstate '55000' then
    if sqlerrm<>'WEEKLY_SOURCE_CORRECTION_ALREADY_PAID' then raise; end if;
  end;
end;
$paid_root_blocker$;

do $active_invoice_operation_blocker$
declare v_root uuid;
begin
  select root_timesheet_id into strict v_root from pg_temp.correct_final_state;
  begin
    insert into public.invoice_operations(
      id,operation_type,entity_type,entity_id,actor_user_id,idempotency_key,
      status,phase,input_json,config_json,progress_json
    ) values (
      'cf300000-0000-4000-8000-000000000004','BUILD_DOCUMENT','TIMESHEET',v_root,
      'a0000000-0000-4000-8000-000000000001','correct-final-active-invoice-operation',
      'QUEUED','SUBMITTED','{}'::jsonb,
      pg_catalog.jsonb_build_object('processor_policy',private._invoice_processor_limits()),
      '{}'::jsonb
    );
    perform public.weekly_source_correct_final_apply_atomic_v1(
      pg_temp.correct_final_apply_request('correct-final-active-invoice-op')
    );
    raise exception 'ASSERTION_FAILED: active invoice operation was accepted';
  exception when sqlstate '55000' then
    if sqlerrm<>'WEEKLY_SOURCE_CORRECTION_ACTIVE_INVOICE_OPERATION' then raise; end if;
  end;
end;
$active_invoice_operation_blocker$;

do $later_final_cycle_blocker$
begin
  begin
    perform pg_temp.roster_cycle(
      'cf300000-0000-4000-8000-000000000010',
      'cf300000-0000-4000-8000-000000000011',
      'cf300000-0000-4000-8000-000000000012',
      'cf300000-0000-4000-8000-000000000013',
      '2027-01-17','35555555-5555-4555-8555-555555555555',
      'CORRECT-FINAL-LATER-ROW','NOT_APPLICABLE','2026-12-30 09:00',
      '2026-12-30 17:00',30,450,7500,15000,0,true,
      '2026-12-30','2026-12-30','2026-12-30'
    );
    perform pg_temp.finalise_cycle(
      'cf300000-0000-4000-8000-000000000010',
      'cf300000-0000-4000-8000-000000000011',
      'cf300000-0000-4000-8000-000000000012'
    );
    perform public.weekly_source_correct_final_apply_atomic_v1(
      pg_temp.correct_final_apply_request('correct-final-later-cycle')
    );
    raise exception 'ASSERTION_FAILED: correction before a later final cycle was accepted';
  exception when sqlstate '55000' then
    if sqlerrm<>'WEEKLY_SOURCE_CORRECTION_LATER_FINAL_CYCLE_EXISTS' then raise; end if;
  end;
end;
$later_final_cycle_blocker$;

do $bad_service_snapshot_atomic_rollback$
declare
  v_bad jsonb;
  v_failed boolean:=false;
begin
  select pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
    'root_timesheet_id',state.root_timesheet_id,
    'prepared_context_hash',state.prepare_result->'root_contexts'->0->>'prepared_context_hash',
    'service_snapshot','{}'::jsonb
  )) into strict v_bad from pg_temp.correct_final_state state;
  begin
    perform public.weekly_source_correct_final_apply_atomic_v1(
      pg_temp.correct_final_apply_request('correct-final-bad-service-snapshot',v_bad)
    );
  exception when others then
    v_failed:=true;
  end;
  if not v_failed then
    raise exception 'ASSERTION_FAILED: invalid service snapshot was accepted';
  end if;
  if not exists(
    select 1
    from pg_temp.correct_final_state state
    join public.weekly_final_source_correction_sessions session
      on session.id=state.correction_session_id and session.state='PREPARED'
    join public.weekly_source_final_revisions prior_revision
      on prior_revision.id=state.prior_final_revision_id and prior_revision.state='CURRENT'
    join public.weekly_source_final_revisions prepared_revision
      on prepared_revision.id=(state.prepare_result->>'final_revision_id')::uuid
     and prepared_revision.state='PREPARED'
  ) then
    raise exception 'ASSERTION_FAILED: failed APPLY did not restore old authority atomically';
  end if;
end;
$bad_service_snapshot_atomic_rollback$;

do $missing_snapshot$
begin
  begin
    perform public.weekly_source_correct_final_apply_atomic_v1(
      pg_temp.correct_final_apply_request('correct-final-missing-set','[]'::jsonb)
    );
    raise exception 'ASSERTION_FAILED: missing root snapshot set was accepted';
  exception when sqlstate '40001' then
    if sqlerrm<>'WEEKLY_SOURCE_CORRECTION_ROOT_SNAPSHOT_SET_MISMATCH' then raise; end if;
  end;
end;
$missing_snapshot$;

do $stale_context_hash$
declare v_bad jsonb;
begin
  select pg_catalog.jsonb_build_array(
    pg_catalog.jsonb_build_object(
      'root_timesheet_id',state.root_timesheet_id,
      'prepared_context_hash',repeat('0',64),
      'service_snapshot',state.prepared_service_snapshot
    )) into v_bad
  from pg_temp.correct_final_state state;
  begin
    perform public.weekly_source_correct_final_apply_atomic_v1(
      pg_temp.correct_final_apply_request('correct-final-stale-context',v_bad)
    );
    raise exception 'ASSERTION_FAILED: stale prepared context hash was accepted';
  exception when sqlstate '40001' then
    if sqlerrm<>'WEEKLY_SOURCE_CORRECTION_ROOT_SNAPSHOT_SET_MISMATCH' then raise; end if;
  end;
end;
$stale_context_hash$;

do $changed_root$
declare v_root uuid;
begin
  select root_timesheet_id into v_root from pg_temp.correct_final_state;
  begin
    update public.timesheets set version=version+1 where timesheet_id=v_root;
    perform public.weekly_source_correct_final_apply_atomic_v1(
      pg_temp.correct_final_apply_request('correct-final-root-changed')
    );
    raise exception 'ASSERTION_FAILED: changed root after PREPARE was accepted';
  exception when sqlstate '40001' then
    if sqlerrm<>'WEEKLY_SOURCE_CORRECTION_PREPARED_CONTEXT_STALE' then raise; end if;
  end;
end;
$changed_root$;

do $invoice_blocker$
declare v_root uuid; v_client uuid; v_invoice uuid:=pg_catalog.gen_random_uuid();
begin
  select state.root_timesheet_id,contract.client_id into v_root,v_client
  from pg_temp.correct_final_state state
  join public.timesheets timesheet_row on timesheet_row.timesheet_id=state.root_timesheet_id
  join public.contracts contract on contract.id=timesheet_row.contract_id;
  perform pg_catalog.set_config(
    'cloudtms.weekly_source_invoice_owner','ADMIT_SOURCE_INVOICE',true
  );
  begin
    insert into public.invoices(id,client_id) values (v_invoice,v_client);
    insert into public.invoice_lines(invoice_id,timesheet_id,description)
    values (v_invoice,v_root,'Correct final blocker proof');
    perform public.weekly_source_correct_final_apply_atomic_v1(
      pg_temp.correct_final_apply_request('correct-final-invoice-blocked')
    );
    raise exception 'ASSERTION_FAILED: invoiced root was accepted';
  exception when sqlstate '55000' then
    if sqlerrm<>'WEEKLY_SOURCE_CORRECTION_INVOICE_LINE_EXISTS' then raise; end if;
  end;
  perform pg_catalog.set_config(
    'cloudtms.weekly_source_invoice_owner','',true
  );
end;
$invoice_blocker$;

select public.weekly_source_correct_final_apply_atomic_v1(
  pg_temp.correct_final_apply_request('correct-final-apply-happy')
) as correct_final_apply_result;

select pg_temp.assert_true(
  (select session.state='APPLIED'
          and prior_revision.state='SUPERSEDED'
          and prepared_revision.state='CURRENT'
          and prior_upload.state='SUPERSEDED'
          and prior_publication.state='STALE'
          and replacement_upload.state='CURRENT'
          and replacement_publication.state='CURRENT'
          and cycle.current_final_revision_id=prepared_revision.id
          and cycle.current_complete_upload_id=replacement_upload.id
          and cycle.current_projection_publication_id=replacement_publication.id
   from pg_temp.correct_final_state state
   join public.weekly_final_source_correction_sessions session
     on session.id=state.correction_session_id
   join public.weekly_source_cycles cycle on cycle.id=state.source_cycle_id
   join public.weekly_source_final_revisions prior_revision
     on prior_revision.id=state.prior_final_revision_id
   join public.weekly_source_final_revisions prepared_revision
     on prepared_revision.id=session.applied_final_revision_id
   join public.weekly_source_uploads prior_upload on prior_upload.id=state.prior_upload_id
   join public.weekly_source_projection_publications prior_publication
     on prior_publication.id=state.prior_publication_id
   join public.weekly_source_uploads replacement_upload
     on replacement_upload.id=state.replacement_upload_id
   join public.weekly_source_projection_publications replacement_publication
     on replacement_publication.id=state.replacement_publication_id),
  'APPLY must atomically swap all current authority pointers'
);
select pg_temp.assert_true(
  (select pg_catalog.count(*)>0 and pg_catalog.bool_and(
            movement.placement_state='VOIDED_BY_CORRECT_FINAL'
          )
   from pg_temp.correct_final_state state
   join public.weekly_source_billing_movements movement
     on movement.final_revision_id=state.prior_final_revision_id),
  'APPLY must retain and void every prior unplaced source movement'
);
select pg_temp.assert_true(
  (select pg_catalog.count(*)>0 and pg_catalog.bool_and(
            movement.placement_state='UNPLACED'
          )
   from pg_temp.correct_final_state state
   join public.weekly_final_source_correction_sessions session
     on session.id=state.correction_session_id
   join public.weekly_source_billing_movements movement
     on movement.final_revision_id=session.applied_final_revision_id),
  'replacement source movements must remain invoice-eligible and unplaced'
);
select pg_temp.assert_true(
  (select financial.total_hours=8.50
          and financial.total_pay_ex_vat=85.00
          and financial.total_charge_ex_vat=170.00
          and financial.expenses_pay_ex_vat=0
          and financial.expenses_charge_ex_vat=0
          and financial.expenses_description is null
          and financial.expenses_evidence_r2_key is null
          and financial.expenses_evidence_manifest is not distinct from 'null'::jsonb
   from pg_temp.correct_final_state state
   join public.timesheets_financials financial
     on financial.timesheet_id=state.root_timesheet_id and financial.is_current),
  'APPLY must rebuild changed source rows and remove the superseded source-fixed expense'
);
select pg_temp.assert_true(
  (select pg_catalog.count(*)=1
   from pg_temp.correct_final_state state
   join public.weekly_final_source_correction_root_impacts impact
     on impact.correction_session_id=state.correction_session_id
    and impact.root_timesheet_id=state.root_timesheet_id),
  'APPLY must record one immutable impact for every affected published root'
);
select pg_temp.assert_true(
  (select pg_catalog.count(*)=4
   from pg_temp.correct_final_state state
   join public.audit_events audit
     on audit.object_id_text=state.correction_session_id::text
   where audit.action in (
     'WEEKLY_SOURCE_CORRECT_FINAL_OPENED',
     'WEEKLY_SOURCE_CORRECT_FINAL_REVIEWED',
     'WEEKLY_SOURCE_CORRECT_FINAL_PREPARED',
     'WEEKLY_SOURCE_CORRECT_FINAL_APPLIED'
   )),
  'OPEN, REVIEW, PREPARE and APPLY must each append an immutable audit event'
);
select pg_temp.assert_true(
  (public.weekly_source_correct_final_apply_atomic_v1(
    pg_temp.correct_final_apply_request('correct-final-apply-happy')
  )->>'idempotent_replay')::boolean,
  'identical APPLY retry must replay the sealed result'
);
select pg_temp.assert_true(
  (public.weekly_source_correct_final_prepare_atomic_v1(
    pg_temp.correct_final_prepare_request('correct-final-prepare-happy')
  )->>'idempotent_replay')::boolean,
  'identical PREPARE retry after APPLY must replay the sealed prepare result'
);
select pg_temp.assert_true(
  (public.weekly_source_correct_final_open_atomic_v1(
    pg_temp.correct_final_open_request('correct-final-open-happy')
  )->>'idempotent_replay')::boolean,
  'identical OPEN retry after APPLY must return the same correction session'
);
do $open_idempotency_collision$
begin
  begin
    perform public.weekly_source_correct_final_open_atomic_v1(
      pg_temp.correct_final_open_request(
        'correct-final-open-happy','A different correction reason must not reuse the key.'
      )
    );
    raise exception 'ASSERTION_FAILED: changed OPEN reused an idempotency key';
  exception when sqlstate '22023' then
    if sqlerrm<>'WEEKLY_SOURCE_CORRECTION_OPEN_IDEMPOTENCY_COLLISION' then raise; end if;
  end;
end;
$open_idempotency_collision$;
do $apply_idempotency_collision$
begin
  begin
    perform public.weekly_source_correct_final_apply_atomic_v1(
      pg_temp.correct_final_apply_request('correct-final-apply-happy','[]'::jsonb)
    );
    raise exception 'ASSERTION_FAILED: changed APPLY reused an idempotency key';
  exception when sqlstate '22023' then
    if sqlerrm<>'WEEKLY_SOURCE_CORRECTION_APPLY_IDEMPOTENCY_COLLISION' then raise; end if;
  end;
end;
$apply_idempotency_collision$;
select pg_temp.assert_true(
  (select pg_catalog.count(*)=1
   from pg_temp.correct_final_state state
   join public.weekly_final_source_correction_root_impacts impact
     on impact.correction_session_id=state.correction_session_id),
  'APPLY replay must not duplicate correction impacts'
);

-- A complete replacement is not required to contain the same candidates or
-- shifts.  Prove the real PREPARE/APPLY owners cover the full set union:
-- changed, removed, unchanged and newly added roots.  The whole script is
-- transaction-scoped and rolls back at the end.  Extend this test Contract
-- into the isolated union window so projection still proves exact eligibility
-- rather than colliding with the earlier 31 December happy-path root.
update public.contracts
set end_date='2027-04-30'
where id='a0000000-0000-4000-8000-000000000004'
  and end_date='2026-12-31';
select pg_temp.assert_true(
  (select end_date='2027-04-30'
   from public.contracts
   where id='a0000000-0000-4000-8000-000000000004'),
  'the isolated four-root proof requires its rollback-only Contract window'
);
select pg_temp.correct_final_seed_union_prior(
  'cf600000-0000-4000-8000-000000000001',
  'cf600000-0000-4000-8000-000000000002',
  'cf600000-0000-4000-8000-000000000003'
);
select pg_temp.finalise_cycle(
  'cf600000-0000-4000-8000-000000000001',
  'cf600000-0000-4000-8000-000000000002',
  'cf600000-0000-4000-8000-000000000003'
);
do $publish_union_prior_roots$
declare v_root uuid;
begin
  for v_root in
    select distinct lineage.timesheet_id
    from public.weekly_source_row_timesheet_lineages lineage
    join public.weekly_source_row_resolutions resolution
      on resolution.id=lineage.row_resolution_id
    join public.weekly_source_upload_rows source_row
      on source_row.id=resolution.upload_row_id
    where source_row.upload_id='cf600000-0000-4000-8000-000000000002'
    order by lineage.timesheet_id
  loop
    perform public.weekly_source_ordinary_pay_projection_apply_atomic_v1(
      pg_temp.ordinary_projection_request(
        'cf600000-0000-4000-8000-000000000001',
        'correct-final-union-prior:'||v_root::text,v_root
      )
    );
  end loop;
end;
$publish_union_prior_roots$;

create temp table correct_final_union_state as
select cycle.id source_cycle_id,cycle.current_final_revision_id prior_final_revision_id,
       cycle.current_complete_upload_id prior_upload_id,
       cycle.current_projection_publication_id prior_publication_id,
       '00000000-0000-0000-0000-000000000000'::uuid correction_session_id,
       'cf700000-0000-4000-8000-000000000002'::uuid replacement_upload_id,
       'cf700000-0000-4000-8000-000000000003'::uuid replacement_publication_id,
       null::jsonb review_result,null::jsonb prepare_result,
       null::jsonb root_service_snapshots
from public.weekly_source_cycles cycle
where cycle.id='cf600000-0000-4000-8000-000000000001';

with opened as (
  select public.weekly_source_correct_final_open_atomic_v1(
    pg_catalog.jsonb_build_object(
      'schema_version','WEEKLY_SOURCE_CORRECT_FINAL_OPEN_V1',
      'actor_user_id','a0000000-0000-4000-8000-000000000001',
      'source_cycle_id',state.source_cycle_id,'authority_scope_kind','CYCLE',
      'report_scope_id',null,
      'expected_current_final_revision_id',state.prior_final_revision_id,
      'expected_final_manifest_hash',pg_catalog.encode(revision.manifest_hash,'hex'),
      'idempotency_key','correct-final-union-open',
      'reason','Replace the mistaken complete final source.'
    )
  ) result
  from pg_temp.correct_final_union_state state
  join public.weekly_source_final_revisions revision
    on revision.id=state.prior_final_revision_id
)
update pg_temp.correct_final_union_state state
set correction_session_id=(opened.result->>'correction_session_id')::uuid
from opened;

select pg_temp.correct_final_stage_union_replacement(
  state.correction_session_id,state.replacement_upload_id,
  state.replacement_publication_id
)
from pg_temp.correct_final_union_state state;

do $review_union_side_effect_free$
declare
  v_timesheets_before bigint;
  v_contract_weeks_before bigint;
  v_lineage_before bigint;
  v_tsfin_before bigint;
  v_review jsonb;
begin
  select pg_catalog.count(*) into v_timesheets_before from public.timesheets;
  select pg_catalog.count(*) into v_contract_weeks_before from public.contract_weeks;
  select pg_catalog.count(*) into v_lineage_before
  from public.weekly_source_row_timesheet_lineages;
  select pg_catalog.count(*) into v_tsfin_before from public.timesheets_financials;
  select public.weekly_source_correct_final_review_atomic_v1(
    pg_catalog.jsonb_build_object(
      'schema_version','WEEKLY_SOURCE_CORRECT_FINAL_REVIEW_V1',
      'actor_user_id','a0000000-0000-4000-8000-000000000001',
      'correction_session_id',state.correction_session_id,
      'replacement_upload_id',state.replacement_upload_id,
      'replacement_projection_publication_id',state.replacement_publication_id,
      'expected_session_version',session.version,
      'expected_authority_scope_version',cycle.version,
      'expected_row_manifest_hash',pg_catalog.encode(upload_row.row_manifest_hash,'hex'),
      'expected_comparison_manifest_hash',pg_catalog.encode(publication.comparison_manifest_hash,'hex'),
      'expected_issue_set_hash',pg_catalog.encode(publication.issue_set_hash,'hex'),
      'idempotency_key','correct-final-union-review'
    )
  ) into strict v_review
  from pg_temp.correct_final_union_state state
  join public.weekly_final_source_correction_sessions session
    on session.id=state.correction_session_id
  join public.weekly_source_cycles cycle on cycle.id=state.source_cycle_id
  join public.weekly_source_uploads upload_row on upload_row.id=state.replacement_upload_id
  join public.weekly_source_projection_publications publication
    on publication.id=state.replacement_publication_id;
  if v_review->>'status'<>'READY_FOR_CONFIRMATION'
     or pg_catalog.jsonb_array_length(v_review->'office_preview'->'changes')<>3
     or not (v_review->'office_preview'->'changes' @>
       '[{"result":"Added"},{"result":"Removed"},{"result":"Changed"}]'::jsonb) then
    raise exception 'ASSERTION_FAILED: union REVIEW did not present added, removed and changed rows';
  end if;
  if (select pg_catalog.count(*) from public.timesheets)<>v_timesheets_before
     or (select pg_catalog.count(*) from public.contract_weeks)<>v_contract_weeks_before
     or (select pg_catalog.count(*) from public.weekly_source_row_timesheet_lineages)<>v_lineage_before
     or (select pg_catalog.count(*) from public.timesheets_financials)<>v_tsfin_before then
    raise exception 'ASSERTION_FAILED: union REVIEW created live Timesheet or lineage state';
  end if;
  update pg_temp.correct_final_union_state set review_result=v_review;
end;
$review_union_side_effect_free$;

with prepared as (
  select public.weekly_source_correct_final_prepare_atomic_v1(
    pg_catalog.jsonb_build_object(
      'schema_version','WEEKLY_SOURCE_CORRECT_FINAL_PREPARE_V1',
      'actor_user_id','a0000000-0000-4000-8000-000000000001',
      'correction_session_id',state.correction_session_id,
      'replacement_upload_id',state.replacement_upload_id,
      'replacement_projection_publication_id',state.replacement_publication_id,
      'expected_session_version',(state.review_result->>'version')::bigint,
      'expected_authority_scope_version',cycle.version,
      'expected_row_manifest_hash',pg_catalog.encode(upload_row.row_manifest_hash,'hex'),
      'expected_comparison_manifest_hash',pg_catalog.encode(publication.comparison_manifest_hash,'hex'),
      'expected_issue_set_hash',pg_catalog.encode(publication.issue_set_hash,'hex'),
      'expected_preview_hash',state.review_result->'office_preview'->>'preview_hash',
      'reason','Replace the mistaken complete final source.',
      'confirmation_text',state.review_result->'office_preview'->>'confirmation_text',
      'idempotency_key','correct-final-union-prepare'
    )
  ) result
  from pg_temp.correct_final_union_state state
  join public.weekly_source_cycles cycle on cycle.id=state.source_cycle_id
  join public.weekly_source_uploads upload_row on upload_row.id=state.replacement_upload_id
  join public.weekly_source_projection_publications publication
    on publication.id=state.replacement_publication_id
)
update pg_temp.correct_final_union_state state
set prepare_result=prepared.result
from prepared;

select pg_temp.assert_true(
  (select pg_catalog.jsonb_array_length(state.prepare_result->'root_contexts')=4
          and (select pg_catalog.count(*) from pg_catalog.jsonb_array_elements(
                 state.prepare_result->'root_contexts') root(value)
               where root.value->>'prior_projection_receipt_id' is null)=1
          and (select pg_catalog.count(*) from pg_catalog.jsonb_array_elements(
                 state.prepare_result->'root_contexts') root(value)
               where root.value->>'prior_projection_receipt_id' is not null
                 and pg_catalog.jsonb_array_length(root.value->'expected_segments')=0)=1
          and (select pg_catalog.count(*) from pg_catalog.jsonb_array_elements(
                 state.prepare_result->'root_contexts') root(value)
               where root.value->>'prior_projection_receipt_id' is not null
                 and pg_catalog.jsonb_array_length(root.value->'expected_segments')>0)=2
   from pg_temp.correct_final_union_state state),
  'PREPARE must return one added, one removed and two retained root contexts'
);

update pg_temp.correct_final_union_state state
set root_service_snapshots=(
  select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
    'root_timesheet_id',root.value->>'root_timesheet_id',
    'prepared_context_hash',root.value->>'prepared_context_hash',
    'service_snapshot',pg_temp.correct_final_prepared_service_snapshot(
      (state.prepare_result->>'final_revision_id')::uuid,
      (root.value->>'root_timesheet_id')::uuid
    )
  ) order by root.value->>'root_timesheet_id')
  from pg_catalog.jsonb_array_elements(state.prepare_result->'root_contexts') root(value)
);

select public.weekly_source_correct_final_apply_atomic_v1(
  pg_catalog.jsonb_build_object(
    'schema_version','WEEKLY_SOURCE_CORRECT_FINAL_APPLY_V1',
    'actor_user_id','a0000000-0000-4000-8000-000000000001',
    'correction_session_id',state.correction_session_id,
    'replacement_upload_id',state.replacement_upload_id,
    'replacement_projection_publication_id',state.replacement_publication_id,
    'expected_session_version',(state.prepare_result->>'version')::bigint,
    'expected_authority_scope_version',cycle.version,
    'expected_row_manifest_hash',pg_catalog.encode(upload_row.row_manifest_hash,'hex'),
    'expected_comparison_manifest_hash',pg_catalog.encode(publication.comparison_manifest_hash,'hex'),
    'expected_issue_set_hash',pg_catalog.encode(publication.issue_set_hash,'hex'),
    'expected_preview_hash',state.prepare_result->'office_preview'->>'preview_hash',
    'reason','Replace the mistaken complete final source.',
    'confirmation_text',state.prepare_result->'office_preview'->>'confirmation_text',
    'idempotency_key','correct-final-union-apply',
    'root_service_snapshots',state.root_service_snapshots
  )
)
from pg_temp.correct_final_union_state state
join public.weekly_source_cycles cycle on cycle.id=state.source_cycle_id
join public.weekly_source_uploads upload_row on upload_row.id=state.replacement_upload_id
join public.weekly_source_projection_publications publication
  on publication.id=state.replacement_publication_id;

select pg_temp.assert_true(
  (select pg_catalog.count(*)=4
          and pg_catalog.count(*) filter(
            where impact.impact_kind='PUBLISHED_REPLACEMENT_ONLY_ROOT')=1
          and pg_catalog.count(*) filter(
            where impact.impact_kind='REPROJECTED_WITHOUT_CURRENT_REVISION_MOVEMENTS')=1
          and pg_catalog.count(*) filter(
            where impact.impact_kind='REPROJECTED_WITH_CURRENT_REVISION_MOVEMENTS')=2
   from pg_temp.correct_final_union_state state
   join public.weekly_final_source_correction_root_impacts impact
     on impact.correction_session_id=state.correction_session_id),
  'APPLY must rebuild replacement-only, prior-only, changed and unchanged roots exactly once'
);
select pg_temp.assert_true(
  (select pg_catalog.count(*)=4
          and pg_catalog.count(*) filter(
            where timesheet_row.week_ending_date='2027-03-07'
              and financial.total_hours=8.50)=1
          and pg_catalog.count(*) filter(
            where timesheet_row.week_ending_date='2027-03-14'
              and financial.total_hours=0)=1
          and pg_catalog.count(*) filter(
            where timesheet_row.week_ending_date='2027-03-21'
              and financial.total_hours=7.50)=1
          and pg_catalog.count(*) filter(
            where timesheet_row.week_ending_date='2027-03-28'
              and financial.total_hours=7.50)=1
   from pg_temp.correct_final_union_state state
   join public.weekly_final_source_correction_root_impacts impact
     on impact.correction_session_id=state.correction_session_id
   join public.timesheets timesheet_row on timesheet_row.timesheet_id=impact.root_timesheet_id
   join public.timesheets_financials financial
     on financial.timesheet_id=impact.root_timesheet_id and financial.is_current),
  'the four rebuilt roots must contain the corrected, removed, unchanged and added hours'
);

-- NHSP uses an exact per-Trust report scope.  Correct final source must keep
-- the old report current through PREPARE, then atomically replace it with the
-- new physical signed rows and their exact source-pence invoice presentation.
select pg_temp.correct_final_seed_nhsp_cycle(
  'cf400000-0000-4000-8000-000000000001',
  'cf400000-0000-4000-8000-000000000002',
  'cf400000-0000-4000-8000-000000000003',
  'cf400000-0000-4000-8000-000000000004',
  'cf400000-0000-4000-8000-000000000005','BR-CF-001'
);
select pg_temp.finalise_nhsp(
  'cf400000-0000-4000-8000-000000000001',
  'cf400000-0000-4000-8000-000000000002',
  'cf400000-0000-4000-8000-000000000003',
  'cf400000-0000-4000-8000-000000000004'
);
select public.weekly_source_ordinary_pay_projection_apply_atomic_v1(
  pg_temp.ordinary_projection_request(
    'cf400000-0000-4000-8000-000000000001','correct-final-nhsp-original'
  )
);

create temp table correct_final_nhsp_state as
select scope.source_cycle_id,scope.id as report_scope_id,
       scope.current_final_revision_id as prior_final_revision_id,
       scope.current_complete_upload_id as prior_upload_id,
       scope.current_projection_publication_id as prior_publication_id,
       receipt.root_timesheet_id,
       '00000000-0000-0000-0000-000000000000'::uuid as correction_session_id,
       'cf500000-0000-4000-8000-000000000003'::uuid as replacement_upload_id,
       'cf500000-0000-4000-8000-000000000004'::uuid as replacement_publication_id,
       null::jsonb as review_result,null::jsonb as prepare_result,
       null::jsonb as prepared_service_snapshot
from public.weekly_source_report_scopes scope
join public.weekly_source_ordinary_pay_projection_receipts receipt
  on receipt.final_revision_id=scope.current_final_revision_id
where scope.id='cf400000-0000-4000-8000-000000000002'
  and receipt.idempotency_key='correct-final-nhsp-original';

create function pg_temp.correct_final_nhsp_apply_request(
  p_idempotency_key text
) returns jsonb language sql stable as $function$
  select pg_catalog.jsonb_build_object(
    'schema_version','WEEKLY_SOURCE_CORRECT_FINAL_APPLY_V1',
    'actor_user_id','a0000000-0000-4000-8000-000000000001',
    'correction_session_id',state.correction_session_id,
    'replacement_upload_id',state.replacement_upload_id,
    'replacement_projection_publication_id',state.replacement_publication_id,
    'expected_session_version',(state.prepare_result->>'version')::bigint,
    'expected_authority_scope_version',scope.version,
    'expected_row_manifest_hash',pg_catalog.encode(upload_row.row_manifest_hash,'hex'),
    'expected_comparison_manifest_hash',pg_catalog.encode(publication.comparison_manifest_hash,'hex'),
    'expected_issue_set_hash',pg_catalog.encode(publication.issue_set_hash,'hex'),
    'expected_preview_hash',state.prepare_result->'office_preview'->>'preview_hash',
    'reason','Replace the mistaken NHSP Trust backing report.',
    'confirmation_text',state.prepare_result->'office_preview'->>'confirmation_text',
    'idempotency_key',p_idempotency_key,
    'root_service_snapshots',pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object(
        'root_timesheet_id',state.root_timesheet_id,
        'prepared_context_hash',state.prepare_result->'root_contexts'->0->>'prepared_context_hash',
        'service_snapshot',state.prepared_service_snapshot
      )
    )
  )
  from pg_temp.correct_final_nhsp_state state
  join public.weekly_source_report_scopes scope on scope.id=state.report_scope_id
  join public.weekly_source_uploads upload_row
    on upload_row.id=state.replacement_upload_id
  join public.weekly_source_projection_publications publication
    on publication.id=state.replacement_publication_id;
$function$;

with opened as (
  select public.weekly_source_correct_final_open_atomic_v1(
    pg_catalog.jsonb_build_object(
      'schema_version','WEEKLY_SOURCE_CORRECT_FINAL_OPEN_V1',
      'actor_user_id','a0000000-0000-4000-8000-000000000001',
      'source_cycle_id',state.source_cycle_id,
      'authority_scope_kind','NHSP_REPORT_SCOPE',
      'report_scope_id',state.report_scope_id,
      'expected_current_final_revision_id',state.prior_final_revision_id,
      'expected_final_manifest_hash',pg_catalog.encode(revision.manifest_hash,'hex'),
      'idempotency_key','correct-final-nhsp-open',
      'reason','Replace the mistaken NHSP Trust backing report.'
    )
  ) result
  from pg_temp.correct_final_nhsp_state state
  join public.weekly_source_final_revisions revision
    on revision.id=state.prior_final_revision_id
)
update pg_temp.correct_final_nhsp_state state
set correction_session_id=(opened.result->>'correction_session_id')::uuid
from opened;

select pg_temp.correct_final_stage_nhsp_replacement(
  state.correction_session_id,state.replacement_upload_id,
  state.replacement_publication_id,
  'cf500000-0000-4000-8000-000000000005','BR-CF-002',14501,500,15001
)
from pg_temp.correct_final_nhsp_state state;

with reviewed as (
  select public.weekly_source_correct_final_review_atomic_v1(
    pg_catalog.jsonb_build_object(
      'schema_version','WEEKLY_SOURCE_CORRECT_FINAL_REVIEW_V1',
      'actor_user_id','a0000000-0000-4000-8000-000000000001',
      'correction_session_id',state.correction_session_id,
      'replacement_upload_id',state.replacement_upload_id,
      'replacement_projection_publication_id',state.replacement_publication_id,
      'expected_session_version',session.version,
      'expected_authority_scope_version',scope.version,
      'expected_row_manifest_hash',pg_catalog.encode(upload_row.row_manifest_hash,'hex'),
      'expected_comparison_manifest_hash',pg_catalog.encode(publication.comparison_manifest_hash,'hex'),
      'expected_issue_set_hash',pg_catalog.encode(publication.issue_set_hash,'hex'),
      'idempotency_key','correct-final-nhsp-review'
    )
  ) result
  from pg_temp.correct_final_nhsp_state state
  join public.weekly_final_source_correction_sessions session
    on session.id=state.correction_session_id
  join public.weekly_source_report_scopes scope on scope.id=state.report_scope_id
  join public.weekly_source_uploads upload_row
    on upload_row.id=state.replacement_upload_id
  join public.weekly_source_projection_publications publication
    on publication.id=state.replacement_publication_id
)
update pg_temp.correct_final_nhsp_state state
set review_result=reviewed.result
from reviewed;

with prepared as (
  select public.weekly_source_correct_final_prepare_atomic_v1(
    pg_catalog.jsonb_build_object(
      'schema_version','WEEKLY_SOURCE_CORRECT_FINAL_PREPARE_V1',
      'actor_user_id','a0000000-0000-4000-8000-000000000001',
      'correction_session_id',state.correction_session_id,
      'replacement_upload_id',state.replacement_upload_id,
      'replacement_projection_publication_id',state.replacement_publication_id,
      'expected_session_version',(state.review_result->>'version')::bigint,
      'expected_authority_scope_version',scope.version,
      'expected_row_manifest_hash',pg_catalog.encode(upload_row.row_manifest_hash,'hex'),
      'expected_comparison_manifest_hash',pg_catalog.encode(publication.comparison_manifest_hash,'hex'),
      'expected_issue_set_hash',pg_catalog.encode(publication.issue_set_hash,'hex'),
      'expected_preview_hash',state.review_result->'office_preview'->>'preview_hash',
      'reason','Replace the mistaken NHSP Trust backing report.',
      'confirmation_text',state.review_result->'office_preview'->>'confirmation_text',
      'idempotency_key','correct-final-nhsp-prepare'
    )
  ) result
  from pg_temp.correct_final_nhsp_state state
  join public.weekly_final_source_correction_sessions session
    on session.id=state.correction_session_id
  join public.weekly_source_report_scopes scope on scope.id=state.report_scope_id
  join public.weekly_source_uploads upload_row
    on upload_row.id=state.replacement_upload_id
  join public.weekly_source_projection_publications publication
    on publication.id=state.replacement_publication_id
)
update pg_temp.correct_final_nhsp_state state
set prepare_result=prepared.result
from prepared;

select pg_temp.assert_true(
  (select scope.current_final_revision_id=state.prior_final_revision_id
          and prior_revision.state='CURRENT'
          and prepared_revision.state='PREPARED'
          and prior_upload.state='CURRENT'
          and replacement_upload.state='CORRECTION_READY'
          and prior_publication.state='CURRENT'
          and replacement_publication.state='CORRECTION_READY'
   from pg_temp.correct_final_nhsp_state state
   join public.weekly_source_report_scopes scope on scope.id=state.report_scope_id
   join public.weekly_source_final_revisions prior_revision
     on prior_revision.id=state.prior_final_revision_id
   join public.weekly_source_final_revisions prepared_revision
     on prepared_revision.id=(state.prepare_result->>'final_revision_id')::uuid
   join public.weekly_source_uploads prior_upload on prior_upload.id=state.prior_upload_id
   join public.weekly_source_uploads replacement_upload
     on replacement_upload.id=state.replacement_upload_id
   join public.weekly_source_projection_publications prior_publication
     on prior_publication.id=state.prior_publication_id
   join public.weekly_source_projection_publications replacement_publication
     on replacement_publication.id=state.replacement_publication_id),
  'NHSP PREPARE must preserve the old per-Trust report as current'
);

update pg_temp.correct_final_nhsp_state state
set prepared_service_snapshot=pg_temp.correct_final_prepared_service_snapshot(
  (state.prepare_result->>'final_revision_id')::uuid,state.root_timesheet_id
);

select public.weekly_source_correct_final_apply_atomic_v1(
  pg_temp.correct_final_nhsp_apply_request('correct-final-nhsp-apply')
) as correct_final_nhsp_apply_result;

select pg_temp.assert_true(
  (select session.state='APPLIED'
          and scope.current_final_revision_id=session.applied_final_revision_id
          and scope.current_complete_upload_id=state.replacement_upload_id
          and scope.current_projection_publication_id=state.replacement_publication_id
          and old_revision.state='SUPERSEDED' and new_revision.state='CURRENT'
   from pg_temp.correct_final_nhsp_state state
   join public.weekly_final_source_correction_sessions session
     on session.id=state.correction_session_id
   join public.weekly_source_report_scopes scope on scope.id=state.report_scope_id
   join public.weekly_source_final_revisions old_revision
     on old_revision.id=state.prior_final_revision_id
   join public.weekly_source_final_revisions new_revision
     on new_revision.id=session.applied_final_revision_id),
  'NHSP APPLY must atomically replace only the exact per-Trust report scope'
);
select pg_temp.assert_true(
  (select pg_catalog.count(*)=1
          and pg_catalog.min(movement.source_profile_kind)='NHSP_TRUST_BACKING_REPORT'
          and pg_catalog.min(movement.source_validation_charge_pence)=15001
          and pg_catalog.min(movement.invoice_presentation_charge_pence)=15001
          and pg_catalog.min(movement.calculated_comparison_charge_pence)=15000
          and pg_catalog.min(movement.price_check_result)='SOURCE_ROUNDING_EQUIVALENT'
   from pg_temp.correct_final_nhsp_state state
   join public.weekly_final_source_correction_sessions session
     on session.id=state.correction_session_id
   join public.weekly_source_billing_movements movement
     on movement.final_revision_id=session.applied_final_revision_id),
  'NHSP replacement must invoice exact signed source pence after the independent CloudTMS price gate'
);
select pg_temp.assert_true(
  (select report.backing_report_number='BR-CF-002'
          and report.physical_line_count=1
          and report.source_total_cost_pence=14501
          and report.source_commission_pence=500
          and report.source_invoice_total_pence=15001
   from pg_temp.correct_final_nhsp_state state
   join public.weekly_final_source_correction_sessions session
     on session.id=state.correction_session_id
   join public.weekly_source_nhsp_backing_reports report
     on report.final_revision_id=session.applied_final_revision_id),
  'NHSP corrected authority must seal its replacement backing-report number and totals'
);
select pg_temp.assert_true(
  (select pg_catalog.count(*)=1 and pg_catalog.bool_and(
            movement.placement_state='VOIDED_BY_CORRECT_FINAL'
          )
   from pg_temp.correct_final_nhsp_state state
   join public.weekly_source_billing_movements movement
     on movement.final_revision_id=state.prior_final_revision_id),
  'NHSP prior physical movements must remain immutable and become non-admissible after correction'
);
select pg_temp.assert_true(
  not exists(
    select 1 from pg_temp.correct_final_nhsp_state state
    join public.weekly_final_source_correction_sessions session
      on session.id=state.correction_session_id
    join public.weekly_source_state_transitions transition_row
      on transition_row.final_revision_id=session.applied_final_revision_id
  ),
  'NHSP correction must remain exact physical movement authority with no omission inference'
);

-- Static boundaries: Correct final source owns source authority and invokes
-- only the existing ordinary projection lifecycle; it never writes Banking
-- Pay, Workbench, pay batches or invoice lines.
select pg_temp.assert_true(
  pg_catalog.strpos(pg_catalog.lower(pg_catalog.pg_get_functiondef(
    'public.weekly_source_correct_final_apply_atomic_v1(jsonb)'::pg_catalog.regprocedure
  )),'insert into public.pay_batch')=0
  and pg_catalog.strpos(pg_catalog.lower(pg_catalog.pg_get_functiondef(
    'public.weekly_source_correct_final_apply_atomic_v1(jsonb)'::pg_catalog.regprocedure
  )),'update public.pay_batch')=0
  and pg_catalog.strpos(pg_catalog.lower(pg_catalog.pg_get_functiondef(
    'public.weekly_source_correct_final_apply_atomic_v1(jsonb)'::pg_catalog.regprocedure
  )),'insert into public.invoice_lines')=0,
  'Correct final source must not write Banking Pay, Workbench or invoice lines'
);

select pg_catalog.jsonb_build_object(
  'ok',true,'verification','weekly_source_correct_final_source_v1',
  'lifecycle','OPEN_REVIEW_PREPARE_APPLY',
  'prior_authority_preserved_until_apply',true,
  'root_snapshot_census_exact',true,
  'banking_workbench_writes',0
);

rollback;
\unset weekly_source_verification_outer_transaction
\unset weekly_source_ordinary_verification_outer_transaction
