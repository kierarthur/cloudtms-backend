-- Rollback-only proof for weekly_source_finalisation_v1.
-- Baseline prerequisite: 03092026_1640_contract_settings_authority_snapshot.sql
-- plus 03092026_1641_contract_settings_effective_authority_v1.sql.
-- Plan 6 prerequisites: schema, private classifiers, settings/profile registry,
-- upload/publication, projection-build, Timesheet-lineage and finalisation,
-- in that order.

\set ON_ERROR_STOP on

\if :{?weekly_source_verification_correction_presentation}
\else
\set weekly_source_verification_correction_presentation 'FULL_REVERSAL_REPLACEMENT'
\endif
\if :{?weekly_source_verification_expense_vat_enabled}
\else
\set weekly_source_verification_expense_vat_enabled false
\endif
\if :{?weekly_source_verification_outer_transaction}
\else
begin;
\endif
set local request.jwt.claim.role='service_role';

create function pg_temp.assert_true(p_condition boolean,p_message text)
returns void language plpgsql as $function$
begin
  if p_condition is distinct from true then
    raise exception 'ASSERTION_FAILED: %',p_message;
  end if;
end;
$function$;

create function pg_temp.remove_empty_successor(
  p_source_group_id uuid,
  p_finalisation_week_ending date
) returns void language plpgsql as $function$
begin
  if exists(
    select 1 from public.weekly_source_cycles cycle
    where cycle.source_group_id=p_source_group_id
      and cycle.finalisation_week_ending=p_finalisation_week_ending
      and (cycle.state<>'OPEN' or cycle.version<>0 or cycle.projection_state<>'NONE'
        or cycle.current_complete_upload_id is not null
        or exists(select 1 from public.weekly_source_uploads upload where upload.source_cycle_id=cycle.id))
  ) then
    raise exception 'ASSERTION_FAILED: the successor cycle was not an empty server-created OPEN cycle';
  end if;
  delete from public.weekly_source_cycles cycle
  where cycle.source_group_id=p_source_group_id
    and cycle.finalisation_week_ending=p_finalisation_week_ending
    and cycle.state='OPEN' and cycle.version=0 and cycle.projection_state='NONE'
    and cycle.current_complete_upload_id is null
    and not exists(select 1 from public.weekly_source_uploads upload where upload.source_cycle_id=cycle.id);
end;
$function$;

create function pg_temp.roster_cycle(
  p_cycle_id uuid,
  p_upload_id uuid,
  p_publication_id uuid,
  p_upload_row_id uuid,
  p_finalisation_week_ending date,
  p_profile_id uuid,
  p_external_key text,
  p_row_state text,
  p_start_at timestamp without time zone,
  p_end_at timestamp without time zone,
  p_break_minutes integer,
  p_net_minutes integer,
  p_total_pay_pence bigint,
  p_total_charge_pence bigint,
  p_source_expense_pence bigint,
  p_include_row boolean default true,
  p_work_date date default '2026-09-07',
  p_coverage_start date default '2026-09-07',
  p_coverage_end date default '2026-09-08',
  p_source_group_id uuid default 'a0000000-0000-4000-8000-000000000005'
) returns void language plpgsql as $function$
declare
  v_row_hash bytea:=private.weekly_source_sha256_jsonb_v1(
    'FINALISER_TEST_ROW_MANIFEST',pg_catalog.to_jsonb(p_upload_id)
  );
  v_comparison_hash bytea:=private.weekly_source_sha256_jsonb_v1(
    'FINALISER_TEST_COMPARISON',pg_catalog.to_jsonb(p_publication_id)
  );
  v_issue_hash bytea:=private.weekly_source_sha256_jsonb_v1(
    'FINALISER_TEST_ISSUES',pg_catalog.to_jsonb(p_publication_id)
  );
  v_link_kind text;
  v_rows jsonb:='[]'::jsonb;
begin
  if exists(
    select 1 from public.weekly_source_cycles cycle
    where cycle.source_group_id=p_source_group_id
      and cycle.finalisation_week_ending=p_finalisation_week_ending
      and cycle.id<>p_cycle_id
      and (cycle.state<>'OPEN' or cycle.version<>0 or cycle.projection_state<>'NONE'
        or cycle.current_complete_upload_id is not null
        or exists(select 1 from public.weekly_source_uploads upload where upload.source_cycle_id=cycle.id))
  ) then
    raise exception 'ASSERTION_FAILED: the successor cycle was not an empty server-created OPEN cycle';
  end if;
  delete from public.weekly_source_cycles cycle
  where cycle.source_group_id=p_source_group_id
    and cycle.finalisation_week_ending=p_finalisation_week_ending
    and cycle.id<>p_cycle_id
    and cycle.state='OPEN' and cycle.version=0 and cycle.projection_state='NONE'
    and cycle.current_complete_upload_id is null
    and not exists(select 1 from public.weekly_source_uploads upload where upload.source_cycle_id=cycle.id);
  insert into public.weekly_source_cycles(
    id,source_group_id,finalisation_week_ending,cutoff_at_utc,state,version,projection_state
  ) values (
    p_cycle_id,p_source_group_id,p_finalisation_week_ending,
    '2026-09-01T14:00:00Z','OPEN',1,'REBUILDING'
  );
  insert into public.weekly_source_uploads(
    id,source_cycle_id,original_filename,content_sha256,byte_count,source_format_profile_id,
    parser_version,normaliser_version,header_coordinate_map_json,header_coordinate_map_hash,
    declared_scope_fingerprint,suggested_coverage_start_local_date,suggested_coverage_end_local_date,
    confirmed_coverage_start_local_date,confirmed_coverage_end_local_date,coverage_timezone,
    coverage_confirmation_version,coverage_confirmed_by_user_id,coverage_confirmed_at_utc,
    coverage_shrink_acknowledged,coverage_state,coverage_proof_kind,physical_row_count,
    accepted_count,row_manifest_hash,state,uploaded_by_user_id,file_metadata_json
  ) values (
    p_upload_id,p_cycle_id,'finaliser-roster-'||p_cycle_id::text||'.csv',
    private.weekly_source_sha256_jsonb_v1('FINALISER_TEST_CONTENT',pg_catalog.to_jsonb(p_cycle_id)),
    100,p_profile_id,'FINALISER_TEST_PARSER_V1','FINALISER_TEST_NORMALISER_V1','{}'::jsonb,
    private.weekly_source_sha256_jsonb_v1('FINALISER_TEST_HEADERS',pg_catalog.to_jsonb(p_cycle_id)),
    private.weekly_source_sha256_jsonb_v1('FINALISER_TEST_SCOPE',pg_catalog.to_jsonb(p_cycle_id)),
    p_coverage_start,p_coverage_end,p_coverage_start,p_coverage_end,'Europe/London',
    'OFFICE_COMPLETE_EXPORT_ATTESTATION_V1','a0000000-0000-4000-8000-000000000001',
    pg_catalog.clock_timestamp(),false,'COMPLETE','OFFICE_COMPLETE_EXPORT_ATTESTATION',
    case when p_include_row then 1 else 0 end,case when p_include_row then 1 else 0 end,
    v_row_hash,'CURRENT','a0000000-0000-4000-8000-000000000001',
    pg_catalog.jsonb_build_object('client_id','a0000000-0000-4000-8000-000000000002')
  );
  update public.weekly_source_cycles
  set current_complete_upload_id=p_upload_id
  where id=p_cycle_id;

  if p_include_row then
    insert into public.weekly_source_upload_rows(
      id,upload_id,source_row_ordinal,external_source_key,source_candidate_identity,
      source_client_identity,work_date,start_at_local,end_at_local,break_minutes,
      actual_net_minutes,row_finalisation_state,source_money_parse_state,
      source_expense_pence,source_expense_parse_state,normalised_row_hash
    ) values (
      p_upload_row_id,p_upload_id,1,p_external_key,'Finaliser Candidate','Finaliser Roster Client',
      p_work_date,p_start_at,p_end_at,p_break_minutes,p_net_minutes,p_row_state,
      'NOT_APPLICABLE',case when p_profile_id='35555555-5555-4555-8555-555555555555'
        then p_source_expense_pence else null end,
      case when p_profile_id<>'35555555-5555-4555-8555-555555555555' then 'NOT_APPLICABLE'
        when p_source_expense_pence=0 then 'OMITTED_ZERO' else 'VALID' end,
      private.weekly_source_sha256_jsonb_v1('FINALISER_TEST_ROW',pg_catalog.to_jsonb(p_upload_row_id))
    );
  end if;

  insert into public.weekly_source_projection_publications(
    id,source_cycle_id,authority_scope_kind,upload_id,authority_scope_version,
    comparison_manifest_hash,issue_set_hash,state
  ) values (
    p_publication_id,p_cycle_id,'CYCLE',p_upload_id,1,
    v_comparison_hash,v_issue_hash,'BUILDING'
  );

  if p_include_row then
    v_link_kind:=case
      when p_row_state='SOURCE_ABSENT_ZERO' then 'ZERO_SOURCE'
      when p_row_state='SOURCE_UNFINALISED' then 'PROVISIONAL_SOURCE'
      else 'POSITIVE_SOURCE' end;
    v_rows:=pg_catalog.jsonb_build_array(pg_catalog.jsonb_strip_nulls(
      pg_catalog.jsonb_build_object(
        'upload_row_id',p_upload_row_id,'mapping_state','RESOLVED',
        'candidate_id','a0000000-0000-4000-8000-000000000003',
        'client_id','a0000000-0000-4000-8000-000000000002',
        'contract_id','a0000000-0000-4000-8000-000000000004',
        'contract_selection_method','AUTO_UNIQUE',
        'qualifying_contract_ids',pg_catalog.jsonb_build_array(
          'a0000000-0000-4000-8000-000000000004'
        ),
        'identity_kind','PROFILE_EXTERNAL_KEY','profile_external_key',p_external_key,
        'link_kind',v_link_kind,
        'economic_snapshot',case when v_link_kind='POSITIVE_SOURCE' then
          pg_catalog.jsonb_build_object(
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
          ) else null end
      )
    ));
  end if;
  perform public.weekly_source_projection_rows_apply_atomic_v1(
    'a0000000-0000-4000-8000-000000000001',p_publication_id,v_rows
  );
  update public.weekly_source_projection_publications
  set state='CURRENT',published_at_utc=pg_catalog.clock_timestamp()
  where id=p_publication_id;
  update public.weekly_source_cycles
  set projection_state='CURRENT',current_projection_publication_id=p_publication_id
  where id=p_cycle_id;
end;
$function$;

create function pg_temp.finalise_cycle(
  p_cycle_id uuid,p_upload_id uuid,p_publication_id uuid
) returns jsonb language plpgsql as $function$
declare
  v_result jsonb;
begin
  select public.weekly_source_finalise_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','a0000000-0000-4000-8000-000000000001',
    'source_cycle_id',p_cycle_id,'authority_scope_kind','CYCLE',
    'upload_id',p_upload_id,'projection_publication_id',p_publication_id,
    'expected_authority_scope_version',1,
    'expected_row_manifest_hash',pg_catalog.encode(upload_row.row_manifest_hash,'hex'),
    'expected_comparison_manifest_hash',pg_catalog.encode(publication.comparison_manifest_hash,'hex'),
    'expected_issue_set_hash',pg_catalog.encode(publication.issue_set_hash,'hex')
  )) into v_result
  from public.weekly_source_uploads upload_row
  join public.weekly_source_projection_publications publication
    on publication.id=p_publication_id
  where upload_row.id=p_upload_id;
  return v_result;
end;
$function$;

create function pg_temp.finalise_nhsp(
  p_cycle_id uuid,p_scope_id uuid,p_upload_id uuid,p_publication_id uuid
) returns jsonb language plpgsql as $function$
declare
  v_result jsonb;
begin
  select public.weekly_source_finalise_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','a0000000-0000-4000-8000-000000000001',
    'source_cycle_id',p_cycle_id,'authority_scope_kind','NHSP_REPORT_SCOPE',
    'report_scope_id',p_scope_id,'upload_id',p_upload_id,
    'projection_publication_id',p_publication_id,
    'expected_authority_scope_version',1,
    'expected_row_manifest_hash',pg_catalog.encode(upload_row.row_manifest_hash,'hex'),
    'expected_comparison_manifest_hash',pg_catalog.encode(publication.comparison_manifest_hash,'hex'),
    'expected_issue_set_hash',pg_catalog.encode(publication.issue_set_hash,'hex')
  )) into v_result
  from public.weekly_source_uploads upload_row
  join public.weekly_source_projection_publications publication
    on publication.id=p_publication_id
  where upload_row.id=p_upload_id;
  return v_result;
end;
$function$;

insert into public.settings_defaults(
  id,candidate_manager_email_templates_sha256,candidate_home_announcement_sha256
) values (
  1,decode(repeat('01',32),'hex'),decode(repeat('02',32),'hex')
) on conflict (id) do update set
  candidate_manager_email_templates_sha256=excluded.candidate_manager_email_templates_sha256,
  candidate_home_announcement_sha256=excluded.candidate_home_announcement_sha256;
insert into public.tms_users(id,email,role,is_active,password_hash)
values ('a0000000-0000-4000-8000-000000000001','finaliser@example.test','admin',true,'not-a-login');
insert into public.clients(id,name)
values ('a0000000-0000-4000-8000-000000000002','Finaliser Roster Client');
insert into public.client_settings(client_id,vat_rate_pct,effective_from)
values ('a0000000-0000-4000-8000-000000000002',20,'2026-01-01');
insert into public.candidates(id,display_name)
values ('a0000000-0000-4000-8000-000000000003','Finaliser Candidate');
insert into public.contracts(
  id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
  weekly_timesheet_source,self_bill,no_timesheet_required,requires_hr,autoprocess_hr
) values (
  'a0000000-0000-4000-8000-000000000004',
  'a0000000-0000-4000-8000-000000000003',
  'a0000000-0000-4000-8000-000000000002',
  '2026-01-01','2026-12-31','PAYE','{}'::jsonb,'HEALTHROSTER',true,true,true,true
);
insert into public.weekly_source_groups(
  id,environment,agency_id,code,display_name,source_family,cutoff_weekday,cutoff_local_time
) values (
  'a0000000-0000-4000-8000-000000000005','TEST',
  'a0000000-0000-4000-8000-000000000006','FINALISER_ROSTER','Finaliser Roster','ROSTER',3,'15:00'
);
insert into public.weekly_source_group_clients(
  id,source_group_id,client_id,valid_from,created_by_user_id
) values (
  'a0000000-0000-4000-8000-000000000007',
  'a0000000-0000-4000-8000-000000000005',
  'a0000000-0000-4000-8000-000000000002','2026-01-01',
  'a0000000-0000-4000-8000-000000000001'
);
insert into public.weekly_source_client_policies(
  id,source_group_id,client_id,effective_from,authority_mode,document_mode,
  self_bill_enabled,self_bill_correction_presentation,
  source_fixed_expenses_enabled,source_expense_vat_enabled,
  weekly_rate_classification_method,manager_queries_enabled,manager_query_recipient,
  created_by_user_id
) values (
  'a0000000-0000-4000-8000-000000000012',
  'a0000000-0000-4000-8000-000000000005',
  'a0000000-0000-4000-8000-000000000002','2026-01-01',
  'SOURCE_AUTHORITY','CHECK_ONLY',true,:'weekly_source_verification_correction_presentation',
  true,:weekly_source_verification_expense_vat_enabled,'SPLIT_RATE_WINDOWS',true,
  'manager@example.test','a0000000-0000-4000-8000-000000000001'
);

-- ADD.
select pg_temp.roster_cycle(
  'a1000000-0000-4000-8000-000000000001','a1000000-0000-4000-8000-000000000002',
  'a1000000-0000-4000-8000-000000000003','a1000000-0000-4000-8000-000000000004',
  '2026-09-13','35555555-5555-4555-8555-555555555555','LINE-1','NOT_APPLICABLE',
  '2026-09-07 09:00','2026-09-07 17:00',30,450,7500,15000,100,true
);
select pg_temp.finalise_cycle(
  'a1000000-0000-4000-8000-000000000001','a1000000-0000-4000-8000-000000000002',
  'a1000000-0000-4000-8000-000000000003'
);
select pg_temp.assert_true(
  (pg_temp.finalise_cycle(
    'a1000000-0000-4000-8000-000000000001','a1000000-0000-4000-8000-000000000002',
    'a1000000-0000-4000-8000-000000000003'
  )->>'idempotent')::boolean,
  'exact finalisation retry must be idempotent'
);
select pg_temp.assert_true(
  pg_catalog.current_setting('lock_timeout')='5s',
  'top-level finaliser must own the transaction-local five-second lock timeout'
);
select pg_temp.assert_true(
  (select pg_catalog.count(*)=1 from public.weekly_source_state_transitions
   where finalisation_cycle_id='a1000000-0000-4000-8000-000000000001' and outcome='ADD'),
  'generic first appearance must be ADD'
);
select pg_temp.assert_true(
  (select pg_catalog.count(*)=1 from public.weekly_source_billing_movements
   where finalisation_cycle_id='a1000000-0000-4000-8000-000000000001'
     and movement_role='POSITIVE'
     and canonical_pay_vector_json->>'source_mode'='HEALTHROSTER_WEEKLY'),
  'generic/Magnit-style source must publish exactly one HEALTHROSTER_WEEKLY movement'
);
select pg_temp.assert_true(
  (select pg_catalog.count(*)=1
   from public.weekly_source_billing_movements movement
   where movement.finalisation_cycle_id='a1000000-0000-4000-8000-000000000001'
     and movement.movement_role='EXPENSE_POSITIVE'
     and movement.source_line_kind='SOURCE_FIXED_EXPENSE'
     and movement.total_pay_ex_vat=1.00
     and movement.invoice_presentation_charge_pence=100
     and movement.vat_rate_pct=case when :weekly_source_verification_expense_vat_enabled then 20 else 0 end
     and movement.vat_amount=case when :weekly_source_verification_expense_vat_enabled then 0.20 else 0 end
     and movement.total_inc_vat=case when :weekly_source_verification_expense_vat_enabled then 1.20 else 1.00 end),
  'source-fixed expense must materialise once with equal pay and charge and configured VAT'
);
select pg_temp.assert_true(
  (select pg_catalog.count(distinct movement.invoice_timesheet_id)=1
   from public.weekly_source_billing_movements movement
   where movement.finalisation_cycle_id='a1000000-0000-4000-8000-000000000001'
     and movement.movement_role in ('POSITIVE','EXPENSE_POSITIVE'))
  and (select pg_catalog.count(*)=2
       from public.weekly_source_billing_movements movement
       where movement.finalisation_cycle_id='a1000000-0000-4000-8000-000000000001'
         and movement.movement_role in ('POSITIVE','EXPENSE_POSITIVE')),
  'worked source hours and their source-fixed expense must share one ordinary Weekly HOURS root'
);

-- Identical later coverage is NO_CHANGE even though the physical upload and
-- projection match audit are new.
select pg_temp.roster_cycle(
  'a2000000-0000-4000-8000-000000000001','a2000000-0000-4000-8000-000000000002',
  'a2000000-0000-4000-8000-000000000003','a2000000-0000-4000-8000-000000000004',
  '2026-09-20','35555555-5555-4555-8555-555555555555','LINE-1','NOT_APPLICABLE',
  '2026-09-07 09:00','2026-09-07 17:00',30,450,7500,15000,100,true
);
do $stale_preview$
begin
  begin
    perform public.weekly_source_finalise_atomic_v1(pg_catalog.jsonb_build_object(
      'actor_user_id','a0000000-0000-4000-8000-000000000001',
      'source_cycle_id','a2000000-0000-4000-8000-000000000001',
      'authority_scope_kind','CYCLE',
      'upload_id','a2000000-0000-4000-8000-000000000002',
      'projection_publication_id','a2000000-0000-4000-8000-000000000003',
      'expected_authority_scope_version',1,
      'expected_row_manifest_hash',repeat('ff',32),
      'expected_comparison_manifest_hash',pg_catalog.encode(
        (select comparison_manifest_hash from public.weekly_source_projection_publications
         where id='a2000000-0000-4000-8000-000000000003'),'hex'
      ),
      'expected_issue_set_hash',pg_catalog.encode(
        (select issue_set_hash from public.weekly_source_projection_publications
         where id='a2000000-0000-4000-8000-000000000003'),'hex'
      )
    ));
    raise exception 'STALE_PREVIEW_WAS_ACCEPTED';
  exception when serialization_failure then
    if sqlerrm<>'WEEKLY_SOURCE_FINALISE_PREVIEW_CHANGED' then raise; end if;
  end;
end;
$stale_preview$;
select pg_temp.assert_true(
  not exists(select 1 from public.weekly_source_final_revisions
             where source_cycle_id='a2000000-0000-4000-8000-000000000001'),
  'stale preview rejection must write no final revision'
);
select pg_temp.finalise_cycle(
  'a2000000-0000-4000-8000-000000000001','a2000000-0000-4000-8000-000000000002',
  'a2000000-0000-4000-8000-000000000003'
);
select pg_temp.assert_true(
  (select pg_catalog.count(*)=1 from public.weekly_source_state_transitions
   where finalisation_cycle_id='a2000000-0000-4000-8000-000000000001' and outcome='NO_CHANGE'),
  'identical later source must be NO_CHANGE'
);
select pg_temp.assert_true(
  not exists(select 1 from public.weekly_source_billing_movements
             where finalisation_cycle_id='a2000000-0000-4000-8000-000000000001'),
  'NO_CHANGE must emit no billing movement'
);

-- AMEND produces a full negative of the preceding positive plus a complete
-- replacement, with one correction-unit identity.
select pg_temp.roster_cycle(
  'a3000000-0000-4000-8000-000000000001','a3000000-0000-4000-8000-000000000002',
  'a3000000-0000-4000-8000-000000000003','a3000000-0000-4000-8000-000000000004',
  '2026-09-27','35555555-5555-4555-8555-555555555555','LINE-1','NOT_APPLICABLE',
  '2026-09-07 09:00','2026-09-07 18:00',30,510,8500,17000,125,true
);
select pg_temp.finalise_cycle(
  'a3000000-0000-4000-8000-000000000001','a3000000-0000-4000-8000-000000000002',
  'a3000000-0000-4000-8000-000000000003'
);
select pg_temp.assert_true(
  (select pg_catalog.count(*)=1 from public.weekly_source_state_transitions
   where finalisation_cycle_id='a3000000-0000-4000-8000-000000000001' and outcome='AMEND'),
  'changed current state must be AMEND'
);
select pg_temp.assert_true(
  (select pg_catalog.count(*)=2 from public.weekly_source_billing_movements
   where finalisation_cycle_id='a3000000-0000-4000-8000-000000000001'
     and movement_role in ('REVERSAL','REPLACEMENT')),
  'AMEND must emit paired full reversal and replacement'
);
select pg_temp.assert_true(
  (select pg_catalog.count(distinct correction_unit_id)=1
   from public.weekly_source_billing_movements
   where finalisation_cycle_id='a3000000-0000-4000-8000-000000000001'
     and source_line_kind<>'SOURCE_FIXED_EXPENSE')
  and (select pg_catalog.count(*)=2 from public.weekly_source_billing_movements
       where finalisation_cycle_id='a3000000-0000-4000-8000-000000000001'
         and correction_unit_id is not null
         and source_line_kind<>'SOURCE_FIXED_EXPENSE'),
  'AMEND pair must share one non-null correction unit'
);
select pg_temp.assert_true(
  (select pg_catalog.count(*)=2
   from public.weekly_source_billing_movements movement
   where movement.finalisation_cycle_id='a3000000-0000-4000-8000-000000000001'
     and movement.movement_role in ('EXPENSE_REVERSAL','EXPENSE_REPLACEMENT')
     and movement.source_line_kind='SOURCE_FIXED_EXPENSE')
  and (select pg_catalog.count(distinct movement.correction_unit_id)=1
       from public.weekly_source_billing_movements movement
       where movement.finalisation_cycle_id='a3000000-0000-4000-8000-000000000001'
         and movement.source_line_kind='SOURCE_FIXED_EXPENSE')
  and (select pg_catalog.sum(movement.invoice_presentation_charge_pence)=25
       from public.weekly_source_billing_movements movement
       where movement.finalisation_cycle_id='a3000000-0000-4000-8000-000000000001'
         and movement.source_line_kind='SOURCE_FIXED_EXPENSE'),
  'changed source-fixed expense must emit an exact paired reversal and replacement'
);

-- An explicit zero-source observation is absent current state and reverses
-- the latest replacement.  It never creates a positive snapshot/movement.
select pg_temp.roster_cycle(
  'a4000000-0000-4000-8000-000000000001','a4000000-0000-4000-8000-000000000002',
  'a4000000-0000-4000-8000-000000000003','a4000000-0000-4000-8000-000000000004',
  '2026-10-04','35555555-5555-4555-8555-555555555555','LINE-1','SOURCE_ABSENT_ZERO',
  null,null,0,0,0,0,0,true
);
select pg_temp.finalise_cycle(
  'a4000000-0000-4000-8000-000000000001','a4000000-0000-4000-8000-000000000002',
  'a4000000-0000-4000-8000-000000000003'
);
select pg_temp.assert_true(
  (select pg_catalog.count(*)=1 from public.weekly_source_state_transitions
   where finalisation_cycle_id='a4000000-0000-4000-8000-000000000001' and outcome='CANCEL'),
  'SOURCE_ABSENT_ZERO must cancel the prior present state'
);
select pg_temp.assert_true(
  (select pg_catalog.count(*)=1 from public.weekly_source_billing_movements
   where finalisation_cycle_id='a4000000-0000-4000-8000-000000000001'
     and movement_role='REVERSAL'),
  'SOURCE_ABSENT_ZERO must emit only the full reversal'
);
select pg_temp.assert_true(
  (select pg_catalog.count(*)=1
   from public.weekly_source_billing_movements movement
   where movement.finalisation_cycle_id='a4000000-0000-4000-8000-000000000001'
     and movement.movement_role='EXPENSE_REVERSAL'
     and movement.invoice_presentation_charge_pence=-125
     and movement.total_pay_ex_vat=-1.25),
  'source disappearance must change source-fixed expense to zero with one full reversal'
);
select pg_temp.assert_true(
  not exists(select 1 from public.weekly_source_final_snapshot_lines snapshot_line
             join public.weekly_source_final_revisions revision
               on revision.id=snapshot_line.final_revision_id
             where revision.source_cycle_id='a4000000-0000-4000-8000-000000000001'),
  'SOURCE_ABSENT_ZERO must not create a current snapshot line'
);
select pg_temp.assert_true(
  not exists(
    select 1
    from public.weekly_source_row_timesheet_lineages lineage
    join public.weekly_source_row_resolutions resolution
      on resolution.id=lineage.row_resolution_id
    where resolution.upload_row_id='a4000000-0000-4000-8000-000000000004'
  ),
  'zero-valued SOURCE_ABSENT_ZERO must not create Timesheet lineage'
);

-- A following completely empty confirmed publication remains empty and does
-- not repeat the cancellation.  Client scope comes from immutable upload
-- metadata because there are no row resolutions from which to derive it.
select pg_temp.roster_cycle(
  'a5000000-0000-4000-8000-000000000001','a5000000-0000-4000-8000-000000000002',
  'a5000000-0000-4000-8000-000000000003',null,
  '2026-10-11','35555555-5555-4555-8555-555555555555',null,null,
  null,null,0,0,0,0,0,false
);
select pg_temp.finalise_cycle(
  'a5000000-0000-4000-8000-000000000001','a5000000-0000-4000-8000-000000000002',
  'a5000000-0000-4000-8000-000000000003'
);
select pg_temp.assert_true(
  not exists(select 1 from public.weekly_source_state_transitions
             where finalisation_cycle_id='a5000000-0000-4000-8000-000000000001')
  and not exists(select 1 from public.weekly_source_billing_movements
                 where finalisation_cycle_id='a5000000-0000-4000-8000-000000000001'),
  'a repeated empty complete source must not repeat a cancellation'
);

-- HealthRoster uses only finalised Actual rows as present source.  A retained
-- but unfinalised row is absent evidence and therefore cancels the previously
-- finalised Actual row inside the confirmed coverage.
select pg_temp.roster_cycle(
  'a6000000-0000-4000-8000-000000000001','a6000000-0000-4000-8000-000000000002',
  'a6000000-0000-4000-8000-000000000003','a6000000-0000-4000-8000-000000000004',
  '2026-10-18','33333333-3333-4333-8333-333333333333','HR-LINE-1','SOURCE_WORKED',
  '2026-09-07 20:00','2026-09-08 08:00',30,690,11500,23000,0,true
);
select pg_temp.finalise_cycle(
  'a6000000-0000-4000-8000-000000000001','a6000000-0000-4000-8000-000000000002',
  'a6000000-0000-4000-8000-000000000003'
);
select pg_temp.roster_cycle(
  'a7000000-0000-4000-8000-000000000001','a7000000-0000-4000-8000-000000000002',
  'a7000000-0000-4000-8000-000000000003','a7000000-0000-4000-8000-000000000004',
  '2026-10-25','33333333-3333-4333-8333-333333333333','HR-LINE-1','SOURCE_UNFINALISED',
  null,null,0,0,0,0,0,true
);
select pg_temp.finalise_cycle(
  'a7000000-0000-4000-8000-000000000001','a7000000-0000-4000-8000-000000000002',
  'a7000000-0000-4000-8000-000000000003'
);
select pg_temp.assert_true(
  (select pg_catalog.count(*)=1 from public.weekly_source_state_transitions transition_row
   where transition_row.finalisation_cycle_id='a7000000-0000-4000-8000-000000000001'
     and transition_row.source_profile_kind='HEALTHROSTER_ACTUAL_ROWS'
     and transition_row.outcome='CANCEL'),
  'SOURCE_UNFINALISED must cancel a preceding finalised HealthRoster Actual row'
);
select pg_temp.assert_true(
  (select pg_catalog.count(*)=1 from public.weekly_source_billing_movements movement
   where movement.finalisation_cycle_id='a7000000-0000-4000-8000-000000000001'
     and movement.movement_role='REVERSAL'),
  'HealthRoster unfinalised evidence must emit one full reversal only'
);
select pg_temp.assert_true(
  not exists(
    select 1
    from public.weekly_expense_authority_generations expense
    join public.weekly_source_final_revisions revision
      on revision.id=expense.final_revision_id
    where revision.source_cycle_id in (
      'a6000000-0000-4000-8000-000000000001',
      'a7000000-0000-4000-8000-000000000001'
    )
  ),
  'HealthRoster rows with no source-expense field must not create expense authority'
);

-- The a6 then a7 path above proves ordinary chronological processing.  Once
-- that later authority exists, an older cycle in the same logical
-- HealthRoster history must fail before it can create Timesheet lineage.
select pg_temp.roster_cycle(
  'a8000000-0000-4000-8000-000000000001','a8000000-0000-4000-8000-000000000002',
  'a8000000-0000-4000-8000-000000000003','a8000000-0000-4000-8000-000000000004',
  '2026-10-12','33333333-3333-4333-8333-333333333333','HR-OLDER-LINE','SOURCE_WORKED',
  '2026-09-07 09:00','2026-09-07 17:00',30,450,7500,15000,0,true
);
do $out_of_order$
begin
  begin
    perform pg_temp.finalise_cycle(
      'a8000000-0000-4000-8000-000000000001',
      'a8000000-0000-4000-8000-000000000002',
      'a8000000-0000-4000-8000-000000000003'
    );
    raise exception 'OUT_OF_ORDER_FINALISATION_WAS_ACCEPTED';
  exception when sqlstate '55000' then
    if sqlerrm<>'WEEKLY_SOURCE_FINALISATION_OUT_OF_ORDER' then
      raise;
    end if;
  end;
end;
$out_of_order$;
select pg_temp.assert_true(
  not exists(select 1 from public.weekly_source_final_revisions revision
             where revision.source_cycle_id='a8000000-0000-4000-8000-000000000001')
  and not exists(
    select 1
    from public.weekly_source_row_timesheet_lineages lineage
    join public.weekly_source_row_resolutions resolution
      on resolution.id=lineage.row_resolution_id
    where resolution.upload_row_id='a8000000-0000-4000-8000-000000000004'
  )
  and (select cycle.state='OPEN' and cycle.current_final_revision_id is null
       from public.weekly_source_cycles cycle
       where cycle.id='a8000000-0000-4000-8000-000000000001'),
  'out-of-order HealthRoster finalisation must fail without finalisation residue'
);

-- A moving complete-coverage window must not erase an older still-current
-- source state.  January is present, February is a disjoint complete export,
-- then a later January window explicitly reports the January row as absent.
-- The last step must CANCEL January rather than treating the immediately prior
-- February revision as the whole history.
select pg_temp.roster_cycle(
  'd1000000-0000-4000-8000-000000000001','d1000000-0000-4000-8000-000000000002',
  'd1000000-0000-4000-8000-000000000003','d1000000-0000-4000-8000-000000000004',
  '2026-11-01','35555555-5555-4555-8555-555555555555','WINDOW-JAN','NOT_APPLICABLE',
  '2026-01-05 09:00','2026-01-05 17:00',30,450,7500,15000,0,true,
  '2026-01-05','2026-01-05','2026-01-05'
);
select pg_temp.finalise_cycle(
  'd1000000-0000-4000-8000-000000000001','d1000000-0000-4000-8000-000000000002',
  'd1000000-0000-4000-8000-000000000003'
);
select pg_temp.roster_cycle(
  'd2000000-0000-4000-8000-000000000001','d2000000-0000-4000-8000-000000000002',
  'd2000000-0000-4000-8000-000000000003','d2000000-0000-4000-8000-000000000004',
  '2026-11-08','35555555-5555-4555-8555-555555555555','WINDOW-FEB','NOT_APPLICABLE',
  '2026-02-02 09:00','2026-02-02 17:00',30,450,7500,15000,0,true,
  '2026-02-02','2026-02-02','2026-02-02'
);
select pg_temp.finalise_cycle(
  'd2000000-0000-4000-8000-000000000001','d2000000-0000-4000-8000-000000000002',
  'd2000000-0000-4000-8000-000000000003'
);
select pg_temp.roster_cycle(
  'd3000000-0000-4000-8000-000000000001','d3000000-0000-4000-8000-000000000002',
  'd3000000-0000-4000-8000-000000000003','d3000000-0000-4000-8000-000000000004',
  '2026-11-15','35555555-5555-4555-8555-555555555555','WINDOW-JAN','SOURCE_ABSENT_ZERO',
  null,null,0,0,0,0,0,true,'2026-01-05','2026-01-05','2026-01-05'
);
select pg_temp.finalise_cycle(
  'd3000000-0000-4000-8000-000000000001','d3000000-0000-4000-8000-000000000002',
  'd3000000-0000-4000-8000-000000000003'
);
select pg_temp.assert_true(
  (select pg_catalog.count(*)=1
   from public.weekly_source_state_transitions transition_row
   where transition_row.finalisation_cycle_id='d3000000-0000-4000-8000-000000000001'
     and transition_row.outcome='CANCEL')
  and (select pg_catalog.count(*)=1
       from public.weekly_source_billing_movements movement
       where movement.finalisation_cycle_id='d3000000-0000-4000-8000-000000000001'
         and movement.movement_role='REVERSAL'),
  'latest work-event state across disjoint coverage must drive the later cancellation'
);

-- A configured source-fixed expense remains authoritative even when the row
-- explicitly says zero worked hours.  It owns the same ordinary Weekly HOURS
-- Timesheet lineage, but it must not invent a worked shift snapshot,
-- transition or movement.
select pg_temp.roster_cycle(
  'e1000000-0000-4000-8000-000000000001','e1000000-0000-4000-8000-000000000002',
  'e1000000-0000-4000-8000-000000000003','e1000000-0000-4000-8000-000000000004',
  '2026-11-22','35555555-5555-4555-8555-555555555555','EXPENSE-ONLY',
  'SOURCE_ABSENT_ZERO','2026-03-02 09:00','2026-03-02 17:00',30,0,0,0,125,true,
  '2026-03-02','2026-03-02','2026-03-02'
);
select pg_temp.finalise_cycle(
  'e1000000-0000-4000-8000-000000000001','e1000000-0000-4000-8000-000000000002',
  'e1000000-0000-4000-8000-000000000003'
);
select pg_temp.assert_true(
  not exists(
    select 1
    from public.weekly_source_row_economic_snapshots economic
    join public.weekly_source_row_resolutions resolution
      on resolution.id=economic.row_resolution_id
    where resolution.upload_row_id='e1000000-0000-4000-8000-000000000004'
  )
  and not exists(
    select 1 from public.weekly_source_final_snapshot_lines snapshot
    where snapshot.upload_row_id='e1000000-0000-4000-8000-000000000004'
  )
  and not exists(
    select 1 from public.weekly_source_state_transitions transition_row
    where transition_row.finalisation_cycle_id='e1000000-0000-4000-8000-000000000001'
  )
  and not exists(
    select 1 from public.weekly_source_billing_movements movement
    where movement.finalisation_cycle_id='e1000000-0000-4000-8000-000000000001'
      and movement.source_line_kind<>'SOURCE_FIXED_EXPENSE'
  ),
  'expense-only zero-hours authority must create no worked source position'
);
select pg_temp.assert_true(
  exists(
    select 1
    from public.weekly_source_row_expense_policy_snapshots expense_policy
    join public.weekly_source_row_resolutions resolution
      on resolution.id=expense_policy.row_resolution_id
    where resolution.upload_row_id='e1000000-0000-4000-8000-000000000004'
      and expense_policy.source_expense_pence=125
      and expense_policy.source_expense_parse_state='VALID'
      and expense_policy.source_expense_vat_enabled=:weekly_source_verification_expense_vat_enabled
  )
  and exists(
    select 1
    from public.weekly_expense_authority_generations authority
    join public.weekly_source_billing_movements movement
      on movement.expense_authority_generation_id=authority.id
    join public.weekly_source_row_timesheet_lineages lineage
      on lineage.timesheet_id=movement.invoice_timesheet_id
    join public.timesheets root on root.timesheet_id=lineage.timesheet_id
    where authority.final_revision_id=(
        select revision.id from public.weekly_source_final_revisions revision
        where revision.source_cycle_id='e1000000-0000-4000-8000-000000000001'
      )
      and authority.state='CURRENT' and authority.source_observation_kind='ROW_PRESENT'
      and authority.source_expense_pence=125
      and authority.candidate_reimbursement_ex_vat=1.25
      and authority.client_charge_ex_vat=1.25
      and movement.movement_role='EXPENSE_POSITIVE'
      and movement.total_pay_ex_vat=1.25
      and movement.source_validation_charge_pence=125
      and movement.invoice_presentation_charge_pence=125
      and movement.vat_rate_pct=case
        when :weekly_source_verification_expense_vat_enabled then 20 else 0 end
      and movement.vat_amount=case
        when :weekly_source_verification_expense_vat_enabled then .25 else 0 end
      and movement.total_inc_vat=case
        when :weekly_source_verification_expense_vat_enabled then 1.50 else 1.25 end
      and root.sheet_scope='WEEKLY'::public.timesheet_scope_enum
      and root.line_type='HOURS'::public.timesheet_line_type_enum
      and not root.is_adjustment
  ),
  'expense-only authority must use exact source cents on the ordinary Weekly HOURS root'
);
select pg_temp.assert_true(
  (pg_temp.finalise_cycle(
    'e1000000-0000-4000-8000-000000000001','e1000000-0000-4000-8000-000000000002',
    'e1000000-0000-4000-8000-000000000003'
  )->>'idempotent')::boolean
  and (select pg_catalog.count(*)=1
       from public.weekly_source_billing_movements movement
       where movement.finalisation_cycle_id='e1000000-0000-4000-8000-000000000001'),
  'expense-only finalisation retry must not duplicate authority or movement'
);

-- An explicit source zero clears the prior source-fixed expense exactly once.
select pg_temp.roster_cycle(
  'e2000000-0000-4000-8000-000000000001','e2000000-0000-4000-8000-000000000002',
  'e2000000-0000-4000-8000-000000000003','e2000000-0000-4000-8000-000000000004',
  '2026-11-29','35555555-5555-4555-8555-555555555555','EXPENSE-ONLY',
  'SOURCE_ABSENT_ZERO','2026-03-02 09:00','2026-03-02 17:00',30,0,0,0,0,true,
  '2026-03-02','2026-03-02','2026-03-02'
);
select pg_temp.finalise_cycle(
  'e2000000-0000-4000-8000-000000000001','e2000000-0000-4000-8000-000000000002',
  'e2000000-0000-4000-8000-000000000003'
);
select pg_temp.assert_true(
  exists(
    select 1
    from public.weekly_expense_authority_generations authority
    where authority.final_revision_id=(
        select revision.id from public.weekly_source_final_revisions revision
        where revision.source_cycle_id='e2000000-0000-4000-8000-000000000001'
      )
      and authority.state='CURRENT' and authority.source_observation_kind='ROW_PRESENT'
      and authority.source_expense_pence=0
      and authority.row_expense_policy_snapshot_id is not null
  )
  and exists(
    select 1 from public.weekly_source_billing_movements movement
    where movement.finalisation_cycle_id='e2000000-0000-4000-8000-000000000001'
      and movement.movement_role='EXPENSE_REVERSAL'
      and movement.source_validation_charge_pence=-125
      and movement.invoice_presentation_charge_pence=-125
  )
  and not exists(
    select 1 from public.weekly_source_billing_movements movement
    where movement.finalisation_cycle_id='e2000000-0000-4000-8000-000000000001'
      and movement.source_line_kind<>'SOURCE_FIXED_EXPENSE'
  ),
  'explicit source expense zero must reverse the prior expense without worked time'
);

-- A later source amount can reappear without worked hours, then complete
-- coverage omission clears it through the same immutable authority chain.
select pg_temp.roster_cycle(
  'e3000000-0000-4000-8000-000000000001','e3000000-0000-4000-8000-000000000002',
  'e3000000-0000-4000-8000-000000000003','e3000000-0000-4000-8000-000000000004',
  '2026-12-06','35555555-5555-4555-8555-555555555555','EXPENSE-ONLY',
  'SOURCE_ABSENT_ZERO','2026-03-02 09:00','2026-03-02 17:00',30,0,0,0,200,true,
  '2026-03-02','2026-03-02','2026-03-02'
);
select pg_temp.finalise_cycle(
  'e3000000-0000-4000-8000-000000000001','e3000000-0000-4000-8000-000000000002',
  'e3000000-0000-4000-8000-000000000003'
);
select pg_temp.roster_cycle(
  'e4000000-0000-4000-8000-000000000001','e4000000-0000-4000-8000-000000000002',
  'e4000000-0000-4000-8000-000000000003',null,
  '2026-12-13','35555555-5555-4555-8555-555555555555','EXPENSE-ONLY',
  'SOURCE_ABSENT_ZERO',null,null,0,0,0,0,0,false,
  '2026-03-02','2026-03-02','2026-03-02'
);
select pg_temp.finalise_cycle(
  'e4000000-0000-4000-8000-000000000001','e4000000-0000-4000-8000-000000000002',
  'e4000000-0000-4000-8000-000000000003'
);
select pg_temp.assert_true(
  exists(
    select 1 from public.weekly_source_billing_movements movement
    where movement.finalisation_cycle_id='e3000000-0000-4000-8000-000000000001'
      and movement.movement_role='EXPENSE_POSITIVE'
      and movement.source_validation_charge_pence=200
  )
  and exists(
    select 1 from public.weekly_expense_authority_generations authority
    where authority.final_revision_id=(
        select revision.id from public.weekly_source_final_revisions revision
        where revision.source_cycle_id='e4000000-0000-4000-8000-000000000001'
      )
      and authority.state='CURRENT'
      and authority.source_observation_kind='OMITTED_IN_COMPLETE_COVERAGE'
      and authority.source_expense_pence=0
      and authority.row_expense_policy_snapshot_id is null
  )
  and exists(
    select 1 from public.weekly_source_billing_movements movement
    where movement.finalisation_cycle_id='e4000000-0000-4000-8000-000000000001'
      and movement.movement_role='EXPENSE_REVERSAL'
      and movement.source_validation_charge_pence=-200
      and movement.invoice_presentation_charge_pence=-200
  ),
  'complete-coverage omission must clear the latest expense-only authority'
);
select pg_temp.assert_true(
  not exists(
    select 1 from public.timesheets root
    join public.weekly_source_row_timesheet_lineages lineage
      on lineage.timesheet_id=root.timesheet_id
    where root.line_type<>'HOURS'::public.timesheet_line_type_enum
  ),
  'source-fixed expenses must never create or reuse an ordinary expense Timesheet'
);

-- With the setting disabled, the same generic profile cannot create source
-- expense provenance or movements; ordinary evidence-led expenses remain on
-- their pre-existing route.
insert into public.weekly_source_contract_policies(
  id,contract_id,effective_from,source_fixed_expenses_enabled_override,
  created_by_user_id
) values (
  'f0000000-0000-4000-8000-000000000001',
  'a0000000-0000-4000-8000-000000000004','2026-04-01',false,
  'a0000000-0000-4000-8000-000000000001'
);
select pg_temp.roster_cycle(
  'f1000000-0000-4000-8000-000000000001','f1000000-0000-4000-8000-000000000002',
  'f1000000-0000-4000-8000-000000000003','f1000000-0000-4000-8000-000000000004',
  '2026-12-20','35555555-5555-4555-8555-555555555555','EXPENSE-DISABLED',
  'SOURCE_ABSENT_ZERO','2026-04-06 09:00','2026-04-06 17:00',30,0,0,0,500,true,
  '2026-04-06','2026-04-06','2026-04-06'
);
select pg_temp.finalise_cycle(
  'f1000000-0000-4000-8000-000000000001','f1000000-0000-4000-8000-000000000002',
  'f1000000-0000-4000-8000-000000000003'
);
select pg_temp.assert_true(
  not exists(
    select 1 from public.weekly_source_row_expense_policy_snapshots expense_policy
    join public.weekly_source_row_resolutions resolution
      on resolution.id=expense_policy.row_resolution_id
    where resolution.upload_row_id='f1000000-0000-4000-8000-000000000004'
  )
  and not exists(
    select 1 from public.weekly_expense_authority_generations authority
    where authority.final_revision_id=(
      select revision.id from public.weekly_source_final_revisions revision
      where revision.source_cycle_id='f1000000-0000-4000-8000-000000000001'
    )
  )
  and not exists(
    select 1 from public.weekly_source_billing_movements movement
    where movement.finalisation_cycle_id='f1000000-0000-4000-8000-000000000001'
  ),
  'disabled source-fixed expenses must not contaminate any expense or invoice route'
);

-- The zero-hours lineage exception is deliberately narrow and fail-closed.
-- Build one current but not-finalised positive-expense row, then prove that
-- missing, duplicate, stale, tampered or worked-economic evidence is refused.
select pg_temp.roster_cycle(
  '91000000-0000-4000-8000-000000000001','91000000-0000-4000-8000-000000000002',
  '91000000-0000-4000-8000-000000000003','91000000-0000-4000-8000-000000000004',
  '2026-12-27','35555555-5555-4555-8555-555555555555','EXPENSE-LINEAGE-NEGATIVE',
  'SOURCE_ABSENT_ZERO','2026-03-09 09:00','2026-03-09 17:00',30,0,0,0,150,true,
  '2026-03-09','2026-03-09','2026-03-09'
);
do $expense_lineage_missing_snapshot$
declare v_resolution_id uuid;
begin
  select resolution.id into strict v_resolution_id
  from public.weekly_source_row_resolutions resolution
  where resolution.upload_row_id='91000000-0000-4000-8000-000000000004';
  begin
    -- Fixture-only corruption: the installed append-only guard is disabled
    -- only for this row mutation, then restored before the owner is exercised.
    alter table public.weekly_source_row_expense_policy_snapshots
      disable trigger weekly_source_immutable_record_guard;
    delete from public.weekly_source_row_expense_policy_snapshots
    where row_resolution_id=v_resolution_id;
    alter table public.weekly_source_row_expense_policy_snapshots
      enable trigger weekly_source_immutable_record_guard;
    perform public.weekly_source_timesheet_lineage_ensure_atomic_v1(
      v_resolution_id,'a0000000-0000-4000-8000-000000000001'
    );
    raise exception 'ASSERTION_FAILED: absent expense snapshot was accepted';
  exception when sqlstate '55000' then
    if sqlerrm<>'WEEKLY_SOURCE_EXPENSE_POLICY_SNAPSHOT_CARDINALITY' then raise; end if;
  end;
end;
$expense_lineage_missing_snapshot$;

do $expense_lineage_duplicate_snapshot$
declare v_resolution_id uuid;
begin
  select resolution.id into strict v_resolution_id
  from public.weekly_source_row_resolutions resolution
  where resolution.upload_row_id='91000000-0000-4000-8000-000000000004';
  begin
    insert into public.weekly_source_row_expense_policy_snapshots(
      id,row_resolution_id,upload_row_id,generation,work_event_id,candidate_id,
      client_id,contract_id,source_expense_pence,source_expense_parse_state,
      source_expense_vat_enabled,invoice_vat_chargeable,invoice_vat_rate_pct,
      correction_presentation,effective_policy_fingerprint,
      invoice_vat_policy_fingerprint,snapshot_hash,created_at_utc
    )
    select pg_catalog.gen_random_uuid(),snapshot.row_resolution_id,
           snapshot.upload_row_id,snapshot.generation,snapshot.work_event_id,
           snapshot.candidate_id,snapshot.client_id,snapshot.contract_id,
           snapshot.source_expense_pence,snapshot.source_expense_parse_state,
           snapshot.source_expense_vat_enabled,snapshot.invoice_vat_chargeable,
           snapshot.invoice_vat_rate_pct,snapshot.correction_presentation,
           snapshot.effective_policy_fingerprint,snapshot.invoice_vat_policy_fingerprint,
           private.weekly_source_sha256_jsonb_v1(
             'FINALISER_TEST_DUPLICATE_EXPENSE_POLICY',pg_catalog.to_jsonb(snapshot.id)
           ),pg_catalog.statement_timestamp()
    from public.weekly_source_row_expense_policy_snapshots snapshot
    where snapshot.row_resolution_id=v_resolution_id;
    raise exception 'ASSERTION_FAILED: duplicate expense snapshot was accepted';
  exception when unique_violation then
    null;
  end;
end;
$expense_lineage_duplicate_snapshot$;

do $expense_lineage_stale_policy$
declare v_resolution_id uuid;
begin
  select resolution.id into strict v_resolution_id
  from public.weekly_source_row_resolutions resolution
  where resolution.upload_row_id='91000000-0000-4000-8000-000000000004';
  begin
    update public.weekly_source_client_policies
    set source_expense_vat_enabled=not source_expense_vat_enabled
    where id='a0000000-0000-4000-8000-000000000012';
    perform public.weekly_source_timesheet_lineage_ensure_atomic_v1(
      v_resolution_id,'a0000000-0000-4000-8000-000000000001'
    );
    raise exception 'ASSERTION_FAILED: stale expense policy was accepted';
  exception when sqlstate '55000' then
    if sqlerrm<>'WEEKLY_SOURCE_TIMESHEET_LINEAGE_POLICY_INVALID' then raise; end if;
  end;
end;
$expense_lineage_stale_policy$;

do $expense_lineage_tampered_snapshot$
declare v_resolution_id uuid;
begin
  select resolution.id into strict v_resolution_id
  from public.weekly_source_row_resolutions resolution
  where resolution.upload_row_id='91000000-0000-4000-8000-000000000004';
  begin
    alter table public.weekly_source_row_expense_policy_snapshots
      disable trigger weekly_source_immutable_record_guard;
    update public.weekly_source_row_expense_policy_snapshots
    set invoice_vat_rate_pct=invoice_vat_rate_pct+1
    where row_resolution_id=v_resolution_id;
    alter table public.weekly_source_row_expense_policy_snapshots
      enable trigger weekly_source_immutable_record_guard;
    perform public.weekly_source_timesheet_lineage_ensure_atomic_v1(
      v_resolution_id,'a0000000-0000-4000-8000-000000000001'
    );
    raise exception 'ASSERTION_FAILED: tampered expense snapshot was accepted';
  exception when sqlstate '55000' then
    if sqlerrm<>'WEEKLY_SOURCE_EXPENSE_POLICY_SNAPSHOT_INVALID' then raise; end if;
  end;
end;
$expense_lineage_tampered_snapshot$;

do $expense_lineage_worked_snapshot$
declare v_resolution_id uuid;
begin
  select resolution.id into strict v_resolution_id
  from public.weekly_source_row_resolutions resolution
  where resolution.upload_row_id='91000000-0000-4000-8000-000000000004';
  begin
    insert into public.weekly_source_row_economic_snapshots(
      row_resolution_id,upload_row_id,generation,work_event_id,
      candidate_id,client_id,contract_id,calculator_version,source_mode,rate_method,row_sign,
      paid_minutes,break_minutes,minutes_day,minutes_night,minutes_sat,minutes_sun,minutes_bh,
      hours_day,hours_night,hours_sat,hours_sun,hours_bh,
      pay_day,pay_night,pay_sat,pay_sun,pay_bh,
      charge_day,charge_night,charge_sat,charge_sun,charge_bh,
      total_pay_pence,calculated_charge_pence,
      invoice_vat_chargeable,invoice_vat_rate_pct,source_expense_vat_enabled,
      canonical_result_json,contract_and_rate_fingerprint,effective_policy_fingerprint,
      invoice_vat_policy_fingerprint,calculation_fingerprint
    )
    select v_resolution_id,'91000000-0000-4000-8000-000000000004',1,
           target_resolution.work_event_id,target_resolution.candidate_id,
           target_resolution.client_id,target_resolution.contract_id,
           template.calculator_version,template.source_mode,template.rate_method,
           template.row_sign,template.paid_minutes,template.break_minutes,
           template.minutes_day,template.minutes_night,template.minutes_sat,
           template.minutes_sun,template.minutes_bh,template.hours_day,
           template.hours_night,template.hours_sat,template.hours_sun,
           template.hours_bh,template.pay_day,template.pay_night,template.pay_sat,
           template.pay_sun,template.pay_bh,template.charge_day,
           template.charge_night,template.charge_sat,template.charge_sun,
           template.charge_bh,template.total_pay_pence,
           template.calculated_charge_pence,template.invoice_vat_chargeable,
           template.invoice_vat_rate_pct,template.source_expense_vat_enabled,
           template.canonical_result_json,template.contract_and_rate_fingerprint,
           target_resolution.effective_policy_fingerprint,
           template.invoice_vat_policy_fingerprint,template.calculation_fingerprint
    from public.weekly_source_row_economic_snapshots template
    join public.weekly_source_row_resolutions target_resolution
      on target_resolution.id=v_resolution_id
    limit 1;
    perform public.weekly_source_timesheet_lineage_ensure_atomic_v1(
      v_resolution_id,'a0000000-0000-4000-8000-000000000001'
    );
    raise exception 'ASSERTION_FAILED: zero-hours row with worked snapshot was accepted';
  exception when sqlstate '55000' then
    if sqlerrm<>'WEEKLY_SOURCE_ZERO_HOUR_EXPENSE_LINEAGE_INVALID' then raise; end if;
  end;
end;
$expense_lineage_worked_snapshot$;

select pg_temp.roster_cycle(
  '92000000-0000-4000-8000-000000000001','92000000-0000-4000-8000-000000000002',
  '92000000-0000-4000-8000-000000000003','92000000-0000-4000-8000-000000000004',
  '2027-01-03','35555555-5555-4555-8555-555555555555','EXPENSE-LINEAGE-ZERO',
  'SOURCE_ABSENT_ZERO','2026-03-16 09:00','2026-03-16 17:00',30,0,0,0,0,true,
  '2026-03-16','2026-03-16','2026-03-16'
);
do $expense_lineage_zero_value$
declare v_resolution_id uuid;
begin
  select resolution.id into strict v_resolution_id
  from public.weekly_source_row_resolutions resolution
  where resolution.upload_row_id='92000000-0000-4000-8000-000000000004';
  begin
    perform public.weekly_source_timesheet_lineage_ensure_atomic_v1(
      v_resolution_id,'a0000000-0000-4000-8000-000000000001'
    );
    raise exception 'ASSERTION_FAILED: zero-valued source expense created lineage';
  exception when sqlstate '55000' then
    if sqlerrm<>'WEEKLY_SOURCE_ZERO_HOUR_EXPENSE_LINEAGE_INVALID' then raise; end if;
  end;
end;
$expense_lineage_zero_value$;

-- NHSP is an exact physical signed-movement report, per Trust.  A positive
-- and its full negative can coexist in one backing report, bind the same
-- ordinary Contract/week Timesheet, and still remain two distinct physical
-- row-resolution lineages and work events.
insert into public.clients(id,name)
values ('b0000000-0000-4000-8000-000000000002','Finaliser NHSP Trust');
insert into public.client_settings(
  client_id,vat_rate_pct,effective_from,is_nhsp,autoprocess_hr,
  requires_hr,no_timesheet_required
) values (
  'b0000000-0000-4000-8000-000000000002',20,'2026-01-01',true,false,false,false
);
insert into public.contracts(
  id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
  weekly_timesheet_source,self_bill,no_timesheet_required,requires_hr,autoprocess_hr,is_nhsp
) values (
  'b0000000-0000-4000-8000-000000000004',
  'a0000000-0000-4000-8000-000000000003',
  'b0000000-0000-4000-8000-000000000002',
  '2026-01-01','2026-12-31','PAYE','{}'::jsonb,null,true,false,false,false,false
);
insert into public.weekly_source_groups(
  id,environment,agency_id,code,display_name,source_family,cutoff_weekday,
  cutoff_local_time,nhsp_report_heading_name
) values (
  'b0000000-0000-4000-8000-000000000005','TEST',
  'a0000000-0000-4000-8000-000000000006','FINALISER_NHSP',
  'Finaliser NHSP','NHSP',3,'15:00','Finaliser NHSP Trust'
);
insert into public.weekly_source_group_clients(
  id,source_group_id,client_id,valid_from,created_by_user_id
) values (
  'b0000000-0000-4000-8000-000000000007',
  'b0000000-0000-4000-8000-000000000005',
  'b0000000-0000-4000-8000-000000000002','2026-01-01',
  'a0000000-0000-4000-8000-000000000001'
);
insert into public.weekly_source_client_policies(
  id,source_group_id,client_id,effective_from,authority_mode,document_mode,
  self_bill_enabled,weekly_rate_classification_method,manager_queries_enabled,
  manager_query_recipient,created_by_user_id
) values (
  'b0000000-0000-4000-8000-000000000012',
  'b0000000-0000-4000-8000-000000000005',
  'b0000000-0000-4000-8000-000000000002','2026-01-01',
  'SOURCE_AUTHORITY','CHECK_ONLY',true,'SPLIT_RATE_WINDOWS',true,
  'manager@example.test','a0000000-0000-4000-8000-000000000001'
);
insert into public.weekly_source_cycles(
  id,source_group_id,finalisation_week_ending,cutoff_at_utc,state,version,projection_state
) values (
  'b1000000-0000-4000-8000-000000000001',
  'b0000000-0000-4000-8000-000000000005','2026-09-13',
  '2026-09-01T14:00:00Z','OPEN',1,'REBUILDING'
);
insert into public.weekly_source_report_scopes(
  id,source_cycle_id,environment,agency_id,source_group_id,client_id,
  cutoff_at_utc,version,state,projection_state
) values (
  'b1000000-0000-4000-8000-000000000002',
  'b1000000-0000-4000-8000-000000000001','TEST',
  'a0000000-0000-4000-8000-000000000006',
  'b0000000-0000-4000-8000-000000000005',
  'b0000000-0000-4000-8000-000000000002',
  '2026-09-01T14:00:00Z',1,'OPEN','REBUILDING'
);
insert into public.weekly_source_uploads(
  id,source_cycle_id,report_scope_id,original_filename,content_sha256,byte_count,
  source_format_profile_id,parser_version,normaliser_version,
  workbook_part_and_sheet_fingerprint,header_coordinate_map_json,
  header_coordinate_map_hash,money_lexical_authority_version,
  declared_scope_fingerprint,coverage_proof_kind,physical_row_count,accepted_count,
  row_manifest_hash,state,uploaded_by_user_id,file_metadata_json
) values (
  'b1000000-0000-4000-8000-000000000003',
  'b1000000-0000-4000-8000-000000000001',
  'b1000000-0000-4000-8000-000000000002','BR-001.xlsx',
  private.weekly_source_sha256_jsonb_v1('FINALISER_NHSP_CONTENT','{"report":"BR-001"}'::jsonb),
  200,'32222222-2222-4222-8222-222222222222','FINALISER_TEST_PARSER_V1',
  'NHSP_BACKING_NORMALISER_V1',
  private.weekly_source_sha256_jsonb_v1('FINALISER_NHSP_WORKBOOK','{"report":"BR-001"}'::jsonb),
  '{}'::jsonb,
  private.weekly_source_sha256_jsonb_v1('FINALISER_NHSP_HEADERS','{"report":"BR-001"}'::jsonb),
  'XLSX_BINARY64_SAME_VALUE_PENCE_V1',
  private.weekly_source_sha256_jsonb_v1('FINALISER_NHSP_SCOPE','{"report":"BR-001"}'::jsonb),
  'NHSP_TRUST_REPORT_SCOPE',2,2,
  private.weekly_source_sha256_jsonb_v1('FINALISER_NHSP_ROWS','{"report":"BR-001"}'::jsonb),
  'CURRENT','a0000000-0000-4000-8000-000000000001',
  pg_catalog.jsonb_build_object(
    'nhsp_report_number','BR-001','nhsp_report_heading_name','Finaliser NHSP Trust'
  )
);
update public.weekly_source_report_scopes
set current_complete_upload_id='b1000000-0000-4000-8000-000000000003'
where id='b1000000-0000-4000-8000-000000000002';
insert into public.weekly_source_upload_rows(
  id,upload_id,source_row_ordinal,external_source_key,source_candidate_identity,
  source_client_identity,work_date,start_at_local,end_at_local,break_minutes,
  actual_net_minutes,row_finalisation_state,role_band_source,
  source_commission_pence,source_total_cost_pence,source_shift_charge_pence,
  source_money_parse_state,source_qualification_profile_version,
  source_expense_parse_state,normalised_row_hash
) values
(
  'b1000000-0000-4000-8000-000000000004',
  'b1000000-0000-4000-8000-000000000003',1,'NHSP-SHIFT-1',
  'Finaliser Candidate','Finaliser NHSP Trust','2026-09-07',
  '2026-09-07 09:00','2026-09-07 17:00',30,450,'SOURCE_WORKED','BAND 5',
  500,14500,15000,'VALID','NHSP_TWO_COMPONENT_PENCE_V1','NOT_APPLICABLE',
  private.weekly_source_sha256_jsonb_v1('FINALISER_NHSP_ROW','{"row":1}'::jsonb)
),
(
  'b1000000-0000-4000-8000-000000000005',
  'b1000000-0000-4000-8000-000000000003',2,'NHSP-SHIFT-1-REV',
  'Finaliser Candidate','Finaliser NHSP Trust','2026-09-07',
  '2026-09-07 09:00','2026-09-07 17:00',30,450,'SOURCE_WORKED','BAND 5',
  -500,-14501,-15001,'VALID','NHSP_TWO_COMPONENT_PENCE_V1','NOT_APPLICABLE',
  private.weekly_source_sha256_jsonb_v1('FINALISER_NHSP_ROW','{"row":2}'::jsonb)
);
insert into public.weekly_source_projection_publications(
  id,source_cycle_id,authority_scope_kind,report_scope_id,upload_id,
  authority_scope_version,comparison_manifest_hash,issue_set_hash,state
) values (
  'b1000000-0000-4000-8000-000000000006',
  'b1000000-0000-4000-8000-000000000001','NHSP_REPORT_SCOPE',
  'b1000000-0000-4000-8000-000000000002',
  'b1000000-0000-4000-8000-000000000003',1,
  private.weekly_source_sha256_jsonb_v1('FINALISER_NHSP_COMPARISON','{"report":"BR-001"}'::jsonb),
  private.weekly_source_sha256_jsonb_v1('FINALISER_NHSP_ISSUES','{"report":"BR-001"}'::jsonb),
  'BUILDING'
);
select public.weekly_source_projection_rows_apply_atomic_v1(
  'a0000000-0000-4000-8000-000000000001',
  'b1000000-0000-4000-8000-000000000006',
  pg_catalog.jsonb_build_array(
    pg_catalog.jsonb_build_object(
      'upload_row_id','b1000000-0000-4000-8000-000000000004',
      'mapping_state','RESOLVED','candidate_id','a0000000-0000-4000-8000-000000000003',
      'client_id','b0000000-0000-4000-8000-000000000002',
      'contract_id','b0000000-0000-4000-8000-000000000004',
      'contract_selection_method','AUTO_UNIQUE',
      'qualifying_contract_ids',pg_catalog.jsonb_build_array('b0000000-0000-4000-8000-000000000004'),
      -- 24 s9 and 25 s106: the NHSP Reference Number is evidence, never sole
      -- durable work identity.  The broker sends SCHEDULE_TUPLE for every NHSP
      -- row (upload-publication-owner.mjs) and the database now requires it
      -- for the NHSP profiles, so this fixture states the shape the pack
      -- allows.  It used to send PROFILE_EXTERNAL_KEY and then ASSERT the
      -- forbidden result - three work events for one corrected shift - which
      -- made the registered gate defend the behaviour it exists to prevent.
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
      'charge_check',pg_catalog.jsonb_build_object(
        'row_sign_kind','POSITIVE','source_commission_pence','500',
        'source_total_cost_pence','14500','source_shift_charge_pence','15000',
        'calculated_segment_charge_pence','15000','comparison_result','EXACT',
        'comparison_reason_code','EXACT','phase_severity','NONE'
      )
    ),
    pg_catalog.jsonb_build_object(
      'upload_row_id','b1000000-0000-4000-8000-000000000005',
      'mapping_state','RESOLVED','candidate_id','a0000000-0000-4000-8000-000000000003',
      'client_id','b0000000-0000-4000-8000-000000000002',
      'contract_id','b0000000-0000-4000-8000-000000000004',
      'contract_selection_method','AUTO_UNIQUE',
      'qualifying_contract_ids',pg_catalog.jsonb_build_array('b0000000-0000-4000-8000-000000000004'),
      'identity_kind','SCHEDULE_TUPLE',
      'link_kind','FULL_NEGATIVE_SOURCE',
      'economic_snapshot',pg_catalog.jsonb_build_object(
        'schema_version','WEEKLY_SOURCE_CANONICAL_ECONOMIC_SNAPSHOT_V1',
        'calculator_version','WEEKLY_SHIFT_FINANCIAL_SEGMENT_V1','source_mode','NHSP_WEEKLY',
        'rate_method','SPLIT_RATE_WINDOWS','sign',-1,'paid_minutes',450,'break_minutes',30,
        'bucket_minutes',pg_catalog.jsonb_build_object('day',450,'night',0,'sat',0,'sun',0,'bh',0),
        'hours',pg_catalog.jsonb_build_object('day',-7.5,'night',0,'sat',0,'sun',0,'bh',0),
        'pay_rates',pg_catalog.jsonb_build_object('day',10,'night',10,'sat',10,'sun',10,'bh',10),
        'charge_rates',pg_catalog.jsonb_build_object('day',20,'night',20,'sat',20,'sun',20,'bh',20),
        'total_pay_pence','-7500','calculated_charge_pence','-15000'
      ),
      'charge_check',pg_catalog.jsonb_build_object(
        'row_sign_kind','FULL_NEGATIVE','source_commission_pence','-500',
        'source_total_cost_pence','-14501','source_shift_charge_pence','-15001',
        'calculated_segment_charge_pence','-15000',
        'comparison_result','SOURCE_ROUNDING_EQUIVALENT',
        'comparison_reason_code','ONE_PENNY_SOURCE_ROUNDING','phase_severity','NONE'
      )
    )
  )
);
update public.weekly_source_projection_publications
set state='CURRENT',published_at_utc=pg_catalog.clock_timestamp()
where id='b1000000-0000-4000-8000-000000000006';
update public.weekly_source_report_scopes
set projection_state='CURRENT',
    current_projection_publication_id='b1000000-0000-4000-8000-000000000006'
where id='b1000000-0000-4000-8000-000000000002';
select public.weekly_source_finalise_atomic_v1(pg_catalog.jsonb_build_object(
  'actor_user_id','a0000000-0000-4000-8000-000000000001',
  'source_cycle_id','b1000000-0000-4000-8000-000000000001',
  'authority_scope_kind','NHSP_REPORT_SCOPE',
  'report_scope_id','b1000000-0000-4000-8000-000000000002',
  'upload_id','b1000000-0000-4000-8000-000000000003',
  'projection_publication_id','b1000000-0000-4000-8000-000000000006',
  'expected_authority_scope_version',1,
  'expected_row_manifest_hash',pg_catalog.encode((select row_manifest_hash
    from public.weekly_source_uploads where id='b1000000-0000-4000-8000-000000000003'),'hex'),
  'expected_comparison_manifest_hash',pg_catalog.encode((select comparison_manifest_hash
    from public.weekly_source_projection_publications
    where id='b1000000-0000-4000-8000-000000000006'),'hex'),
  'expected_issue_set_hash',pg_catalog.encode((select issue_set_hash
    from public.weekly_source_projection_publications
    where id='b1000000-0000-4000-8000-000000000006'),'hex')
));
select pg_temp.assert_true(
  (select pg_catalog.count(*)=2 from public.weekly_source_billing_movements
   where finalisation_cycle_id='b1000000-0000-4000-8000-000000000001'
     and source_profile_kind='NHSP_TRUST_BACKING_REPORT'),
  'each physical NHSP row must create exactly one movement'
);
-- 24 s9: durable work identity is the exact Candidate, actual Client, worked
-- date and a compatible schedule.  A physical positive and the physical full
-- negative that reverses the same shift are therefore ONE work event, and
-- 24 s9's closing sentence keeps them two separate invoice movements on it.
-- Asserting two work events here was asserting the forbidden shape: it is what
-- let the reference-keyed identity through and hid the pay defect WP-52 fixes,
-- because a reversal that lands on its own work event never collides with the
-- positive it reverses.
select pg_temp.assert_true(
  (select pg_catalog.count(distinct work_event_id)=1
   from public.weekly_source_billing_movements
   where finalisation_cycle_id='b1000000-0000-4000-8000-000000000001')
  and (select pg_catalog.count(distinct invoice_timesheet_id)=1
       from public.weekly_source_billing_movements
       where finalisation_cycle_id='b1000000-0000-4000-8000-000000000001')
  and (select pg_catalog.count(*)=2
       from public.weekly_source_billing_movements
       where finalisation_cycle_id='b1000000-0000-4000-8000-000000000001'),
  'an NHSP positive and the full negative reversing the same shift are one work event and two movements'
);
select pg_temp.assert_true(
  not exists(
    select 1
    from public.weekly_source_billing_movements movement
    join public.weekly_work_events work_event
      on work_event.id=movement.work_event_id
    where movement.finalisation_cycle_id='b1000000-0000-4000-8000-000000000001'
      and work_event.identity_kind is distinct from 'SCHEDULE_TUPLE'
  ),
  'NHSP work events must never be keyed on the Reference Number'
);
select pg_temp.assert_true(
  (select pg_catalog.count(*)=2
   from public.weekly_source_row_timesheet_lineages lineage
   join public.weekly_source_row_resolutions resolution
     on resolution.id=lineage.row_resolution_id
   join public.weekly_source_upload_rows source_row
     on source_row.id=resolution.upload_row_id
   where source_row.upload_id='b1000000-0000-4000-8000-000000000003')
  and (select pg_catalog.count(distinct lineage.timesheet_id)=1
       from public.weekly_source_row_timesheet_lineages lineage
       join public.weekly_source_row_resolutions resolution
         on resolution.id=lineage.row_resolution_id
       join public.weekly_source_upload_rows source_row
         on source_row.id=resolution.upload_row_id
       where source_row.upload_id='b1000000-0000-4000-8000-000000000003'),
  'every physical NHSP row must own immutable lineage while same Contract/week reuses one Timesheet'
);
-- The rule this states is that an NHSP physical full negative is supplied by
-- the Trust as an independent row (14 s4.2.4): it must never be routed through
-- the generic AMEND/CANCEL path, which looks a prior movement up and raises
-- WEEKLY_SOURCE_PRIOR_MOVEMENT_MISSING when there is none.  The previous form
-- asserted that no positive shared the negative's work event, which is not
-- that rule at all - it only ever passed because the reference-keyed identity
-- put them on different work events (24 s9 forbids that), and it would have
-- gone on passing if the two had been wrongly split for any other reason.
select pg_temp.assert_true(
  exists(
    select 1
    from public.weekly_source_billing_movements negative_movement
    where negative_movement.nhsp_upload_row_id='b1000000-0000-4000-8000-000000000005'
      and negative_movement.movement_role='REVERSAL'
      and negative_movement.source_line_kind='NHSP_PHYSICAL_FULL_NEGATIVE'
      and negative_movement.prior_movement_id is null
      and negative_movement.correction_unit_id is null
      and negative_movement.transition_id is null
  ),
  'a first-appearance physical NHSP full-negative must not require a prior positive movement'
);
select pg_temp.assert_true(
  (select pg_catalog.array_agg(invoice_presentation_charge_pence order by invoice_presentation_charge_pence)
   from public.weekly_source_billing_movements
   where finalisation_cycle_id='b1000000-0000-4000-8000-000000000001')
     =array[-15001::bigint,15000::bigint],
  'NHSP invoice presentation must preserve exact signed source pence after the gate'
);
select pg_temp.assert_true(
  (select pg_catalog.array_agg(calculated_comparison_charge_pence order by calculated_comparison_charge_pence)
   from public.weekly_source_billing_movements
   where finalisation_cycle_id='b1000000-0000-4000-8000-000000000001')
     =array[-15000::bigint,15000::bigint],
  'NHSP CloudTMS calculation must remain the independent comparator'
);
select pg_temp.assert_true(
  exists(select 1 from public.weekly_source_nhsp_backing_reports report
         where report.final_revision_id=(select current_final_revision_id
           from public.weekly_source_report_scopes
           where id='b1000000-0000-4000-8000-000000000002')
           and report.backing_report_number='BR-001'
           and report.physical_line_count=2
           and report.source_invoice_total_pence=-1),
  'NHSP backing-report number and signed totals must be sealed'
);
select pg_temp.assert_true(
  exists(select 1 from public.weekly_source_client_manifests manifest
         where manifest.source_cycle_id='b1000000-0000-4000-8000-000000000001'
           and manifest.client_id='b0000000-0000-4000-8000-000000000002'
           and manifest.backing_report_number='BR-001' and manifest.movement_count=2),
  'NHSP must build one per-Trust manifest containing every physical movement'
);
select pg_temp.assert_true(
  not exists(select 1 from public.weekly_source_state_transitions
             where finalisation_cycle_id='b1000000-0000-4000-8000-000000000001'),
  'NHSP exact reports must not infer generic ADD/AMEND/CANCEL transitions'
);
select pg_temp.assert_true(
  pg_temp.finalise_nhsp(
    'b1000000-0000-4000-8000-000000000001',
    'b1000000-0000-4000-8000-000000000002',
    'b1000000-0000-4000-8000-000000000003',
    'b1000000-0000-4000-8000-000000000006'
  )->>'idempotent'='true'
  and (select pg_catalog.count(*)=2
       from public.weekly_source_billing_movements movement
       where movement.finalisation_cycle_id='b1000000-0000-4000-8000-000000000001'),
  'exact NHSP finalisation retry must return the sealed revision without duplicating movements'
);

-- A later empty Trust backing report is authoritative only for its own
-- physical contents.  It must not infer reversals from rows present in BR-001.
select pg_temp.remove_empty_successor(
  'b0000000-0000-4000-8000-000000000005','2026-09-20'
);
insert into public.weekly_source_cycles(
  id,source_group_id,finalisation_week_ending,cutoff_at_utc,state,version,projection_state
) values (
  'b2000000-0000-4000-8000-000000000001',
  'b0000000-0000-4000-8000-000000000005','2026-09-20',
  '2026-09-02T14:00:00Z','OPEN',1,'REBUILDING'
);
insert into public.weekly_source_report_scopes(
  id,source_cycle_id,environment,agency_id,source_group_id,client_id,
  cutoff_at_utc,version,state,projection_state
) values (
  'b2000000-0000-4000-8000-000000000002',
  'b2000000-0000-4000-8000-000000000001','TEST',
  'a0000000-0000-4000-8000-000000000006',
  'b0000000-0000-4000-8000-000000000005',
  'b0000000-0000-4000-8000-000000000002',
  '2026-09-02T14:00:00Z',1,'OPEN','REBUILDING'
);
insert into public.weekly_source_uploads(
  id,source_cycle_id,report_scope_id,original_filename,content_sha256,byte_count,
  source_format_profile_id,parser_version,normaliser_version,
  workbook_part_and_sheet_fingerprint,header_coordinate_map_json,
  header_coordinate_map_hash,money_lexical_authority_version,
  declared_scope_fingerprint,coverage_proof_kind,physical_row_count,accepted_count,
  row_manifest_hash,state,uploaded_by_user_id,file_metadata_json
) values (
  'b2000000-0000-4000-8000-000000000003',
  'b2000000-0000-4000-8000-000000000001',
  'b2000000-0000-4000-8000-000000000002','BR-002.xlsx',
  private.weekly_source_sha256_jsonb_v1('FINALISER_NHSP_CONTENT','{"report":"BR-002"}'::jsonb),
  100,'32222222-2222-4222-8222-222222222222','FINALISER_TEST_PARSER_V1',
  'NHSP_BACKING_NORMALISER_V1',
  private.weekly_source_sha256_jsonb_v1('FINALISER_NHSP_WORKBOOK','{"report":"BR-002"}'::jsonb),
  '{}'::jsonb,
  private.weekly_source_sha256_jsonb_v1('FINALISER_NHSP_HEADERS','{"report":"BR-002"}'::jsonb),
  'XLSX_BINARY64_SAME_VALUE_PENCE_V1',
  private.weekly_source_sha256_jsonb_v1('FINALISER_NHSP_SCOPE','{"report":"BR-002"}'::jsonb),
  'NHSP_TRUST_REPORT_SCOPE',0,0,
  private.weekly_source_sha256_jsonb_v1('FINALISER_NHSP_ROWS','{"report":"BR-002"}'::jsonb),
  'CURRENT','a0000000-0000-4000-8000-000000000001',
  pg_catalog.jsonb_build_object(
    'nhsp_report_number','BR-002','nhsp_report_heading_name','Finaliser NHSP Trust'
  )
);
update public.weekly_source_report_scopes
set current_complete_upload_id='b2000000-0000-4000-8000-000000000003'
where id='b2000000-0000-4000-8000-000000000002';
insert into public.weekly_source_projection_publications(
  id,source_cycle_id,authority_scope_kind,report_scope_id,upload_id,
  authority_scope_version,comparison_manifest_hash,issue_set_hash,state,published_at_utc
) values (
  'b2000000-0000-4000-8000-000000000006',
  'b2000000-0000-4000-8000-000000000001','NHSP_REPORT_SCOPE',
  'b2000000-0000-4000-8000-000000000002',
  'b2000000-0000-4000-8000-000000000003',1,
  private.weekly_source_sha256_jsonb_v1('FINALISER_NHSP_COMPARISON','{"report":"BR-002"}'::jsonb),
  private.weekly_source_sha256_jsonb_v1('FINALISER_NHSP_ISSUES','{"report":"BR-002"}'::jsonb),
  'CURRENT',pg_catalog.clock_timestamp()
);
update public.weekly_source_report_scopes
set projection_state='CURRENT',
    current_projection_publication_id='b2000000-0000-4000-8000-000000000006'
where id='b2000000-0000-4000-8000-000000000002';
select pg_temp.finalise_nhsp(
  'b2000000-0000-4000-8000-000000000001',
  'b2000000-0000-4000-8000-000000000002',
  'b2000000-0000-4000-8000-000000000003',
  'b2000000-0000-4000-8000-000000000006'
);
select pg_temp.assert_true(
  not exists(select 1 from public.weekly_source_billing_movements movement
             where movement.finalisation_cycle_id='b2000000-0000-4000-8000-000000000001')
  and not exists(select 1 from public.weekly_source_state_transitions transition_row
                 where transition_row.finalisation_cycle_id='b2000000-0000-4000-8000-000000000001')
  and exists(
    select 1
    from public.weekly_source_nhsp_backing_reports report
    where report.report_scope_id='b2000000-0000-4000-8000-000000000002'
      and report.backing_report_number='BR-002'
      and report.physical_line_count=0
      and report.source_invoice_total_pence=0
  ),
  'NHSP omission must never infer a cancellation or synthetic movement'
);

-- A two-pence discrepancy is not source rounding.  Projection may publish it
-- for Office review, but finalisation must reject it atomically and leave the
-- report scope open with no lineage, revision, manifest or movement residue.
select pg_temp.remove_empty_successor(
  'b0000000-0000-4000-8000-000000000005','2026-09-27'
);
insert into public.weekly_source_cycles(
  id,source_group_id,finalisation_week_ending,cutoff_at_utc,state,version,projection_state
) values (
  'b3000000-0000-4000-8000-000000000001',
  'b0000000-0000-4000-8000-000000000005','2026-09-27',
  '2026-09-03T14:00:00Z','OPEN',1,'REBUILDING'
);
insert into public.weekly_source_report_scopes(
  id,source_cycle_id,environment,agency_id,source_group_id,client_id,
  cutoff_at_utc,version,state,projection_state
) values (
  'b3000000-0000-4000-8000-000000000002',
  'b3000000-0000-4000-8000-000000000001','TEST',
  'a0000000-0000-4000-8000-000000000006',
  'b0000000-0000-4000-8000-000000000005',
  'b0000000-0000-4000-8000-000000000002',
  '2026-09-03T14:00:00Z',1,'OPEN','REBUILDING'
);
insert into public.weekly_source_uploads(
  id,source_cycle_id,report_scope_id,original_filename,content_sha256,byte_count,
  source_format_profile_id,parser_version,normaliser_version,
  workbook_part_and_sheet_fingerprint,header_coordinate_map_json,
  header_coordinate_map_hash,money_lexical_authority_version,
  declared_scope_fingerprint,coverage_proof_kind,physical_row_count,accepted_count,
  row_manifest_hash,state,uploaded_by_user_id,file_metadata_json
) values (
  'b3000000-0000-4000-8000-000000000003',
  'b3000000-0000-4000-8000-000000000001',
  'b3000000-0000-4000-8000-000000000002','BR-003.xlsx',
  private.weekly_source_sha256_jsonb_v1('FINALISER_NHSP_CONTENT','{"report":"BR-003"}'::jsonb),
  100,'32222222-2222-4222-8222-222222222222','FINALISER_TEST_PARSER_V1',
  'NHSP_BACKING_NORMALISER_V1',
  private.weekly_source_sha256_jsonb_v1('FINALISER_NHSP_WORKBOOK','{"report":"BR-003"}'::jsonb),
  '{}'::jsonb,
  private.weekly_source_sha256_jsonb_v1('FINALISER_NHSP_HEADERS','{"report":"BR-003"}'::jsonb),
  'XLSX_BINARY64_SAME_VALUE_PENCE_V1',
  private.weekly_source_sha256_jsonb_v1('FINALISER_NHSP_SCOPE','{"report":"BR-003"}'::jsonb),
  'NHSP_TRUST_REPORT_SCOPE',1,1,
  private.weekly_source_sha256_jsonb_v1('FINALISER_NHSP_ROWS','{"report":"BR-003"}'::jsonb),
  'CURRENT','a0000000-0000-4000-8000-000000000001',
  pg_catalog.jsonb_build_object(
    'nhsp_report_number','BR-003','nhsp_report_heading_name','Finaliser NHSP Trust'
  )
);
update public.weekly_source_report_scopes
set current_complete_upload_id='b3000000-0000-4000-8000-000000000003'
where id='b3000000-0000-4000-8000-000000000002';
insert into public.weekly_source_upload_rows(
  id,upload_id,source_row_ordinal,external_source_key,source_candidate_identity,
  source_client_identity,work_date,start_at_local,end_at_local,break_minutes,
  actual_net_minutes,row_finalisation_state,role_band_source,
  source_commission_pence,source_total_cost_pence,source_shift_charge_pence,
  source_money_parse_state,source_qualification_profile_version,
  source_expense_parse_state,normalised_row_hash
) values (
  'b3000000-0000-4000-8000-000000000004',
  'b3000000-0000-4000-8000-000000000003',1,'NHSP-MISMATCH-1',
  'Finaliser Candidate','Finaliser NHSP Trust','2026-09-07',
  '2026-09-07 09:00','2026-09-07 17:00',30,450,'SOURCE_WORKED','BAND 5',
  500,14502,15002,'VALID','NHSP_TWO_COMPONENT_PENCE_V1','NOT_APPLICABLE',
  private.weekly_source_sha256_jsonb_v1('FINALISER_NHSP_ROW','{"row":"mismatch"}'::jsonb)
);
insert into public.weekly_source_projection_publications(
  id,source_cycle_id,authority_scope_kind,report_scope_id,upload_id,
  authority_scope_version,comparison_manifest_hash,issue_set_hash,state
) values (
  'b3000000-0000-4000-8000-000000000006',
  'b3000000-0000-4000-8000-000000000001','NHSP_REPORT_SCOPE',
  'b3000000-0000-4000-8000-000000000002',
  'b3000000-0000-4000-8000-000000000003',1,
  private.weekly_source_sha256_jsonb_v1('FINALISER_NHSP_COMPARISON','{"report":"BR-003"}'::jsonb),
  private.weekly_source_sha256_jsonb_v1('FINALISER_NHSP_ISSUES','{"report":"BR-003"}'::jsonb),
  'BUILDING'
);
select public.weekly_source_projection_rows_apply_atomic_v1(
  'a0000000-0000-4000-8000-000000000001',
  'b3000000-0000-4000-8000-000000000006',
  pg_catalog.jsonb_build_array(
    pg_catalog.jsonb_build_object(
      'upload_row_id','b3000000-0000-4000-8000-000000000004',
      'mapping_state','RESOLVED','candidate_id','a0000000-0000-4000-8000-000000000003',
      'client_id','b0000000-0000-4000-8000-000000000002',
      'contract_id','b0000000-0000-4000-8000-000000000004',
      'contract_selection_method','AUTO_UNIQUE',
      'qualifying_contract_ids',pg_catalog.jsonb_build_array('b0000000-0000-4000-8000-000000000004'),
      -- WP-58. NHSP work identity is the schedule tuple, never the Reference
      -- Number (pack 24 sections 1 and 9). This is what the broker sends for an
      -- NHSP profile, and since WP-58 it is the only kind
      -- weekly_source_projection_rows_apply_atomic_v1 will accept for one.
      -- WP-52 corrected BR-001 above; this BR-003 row is the site it missed.
      -- BR-003 reports the same Candidate, Client, date and worked interval as
      -- BR-001, so under the correct rule this row resolves to BR-001's work
      -- event instead of inventing a second one. That is incidental to what
      -- this case proves: an NHSP row whose source charge differs from the
      -- calculated charge by more than a penny must block finalisation with
      -- WEEKLY_SOURCE_NHSP_PRICE_GATE_FAILED and leave no residue, which is
      -- exactly what the assertions below still demand.
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
      'charge_check',pg_catalog.jsonb_build_object(
        'row_sign_kind','POSITIVE','source_commission_pence','500',
        'source_total_cost_pence','14502','source_shift_charge_pence','15002',
        'calculated_segment_charge_pence','15000','comparison_result','MISMATCH',
        'comparison_reason_code','SOURCE_DIFFERS_BY_MORE_THAN_ONE_PENNY',
        'phase_severity','FINALISATION_BLOCKER','blocker_code','NHSP_SOURCE_CHARGE_MISMATCH'
      )
    )
  )
);
update public.weekly_source_projection_publications
set state='CURRENT',published_at_utc=pg_catalog.clock_timestamp()
where id='b3000000-0000-4000-8000-000000000006';
update public.weekly_source_report_scopes
set projection_state='CURRENT',
    current_projection_publication_id='b3000000-0000-4000-8000-000000000006'
where id='b3000000-0000-4000-8000-000000000002';
do $block$
begin
  begin
    perform pg_temp.finalise_nhsp(
      'b3000000-0000-4000-8000-000000000001',
      'b3000000-0000-4000-8000-000000000002',
      'b3000000-0000-4000-8000-000000000003',
      'b3000000-0000-4000-8000-000000000006'
    );
    raise exception 'NHSP_PRICE_GATE_DID_NOT_BLOCK';
  exception when sqlstate '55000' then
    if sqlerrm<>'WEEKLY_SOURCE_NHSP_PRICE_GATE_FAILED' then
      raise;
    end if;
  end;
end;
$block$;
select pg_temp.assert_true(
  not exists(select 1 from public.weekly_source_final_revisions revision
             where revision.source_cycle_id='b3000000-0000-4000-8000-000000000001')
  and not exists(select 1 from public.weekly_source_billing_movements movement
                 where movement.finalisation_cycle_id='b3000000-0000-4000-8000-000000000001')
  and not exists(
    select 1
    from public.weekly_source_row_timesheet_lineages lineage
    join public.weekly_source_row_resolutions resolution
      on resolution.id=lineage.row_resolution_id
    where resolution.upload_row_id='b3000000-0000-4000-8000-000000000004'
  )
  and not exists(select 1 from public.weekly_source_client_cycle_completions completion
                 where completion.source_cycle_id='b3000000-0000-4000-8000-000000000001')
  and (select scope.current_final_revision_id is null and scope.state='OPEN'
       from public.weekly_source_report_scopes scope
       where scope.id='b3000000-0000-4000-8000-000000000002'),
  'NHSP finalisation price blocker must roll back every finalisation-side effect'
);

-- A structurally valid £0 final NHSP row is a warning, not a malformed shift.
-- Drive the real projection, opaque Office acceptance and finalisation owners.
insert into public.weekly_source_cycles(
  id,source_group_id,finalisation_week_ending,cutoff_at_utc,state,version,projection_state
) values (
  'b4000000-0000-4000-8000-000000000001',
  'b0000000-0000-4000-8000-000000000005','2026-10-11',
  '2026-09-16T14:00:00Z','OPEN',1,'REBUILDING'
);
insert into public.weekly_source_report_scopes(
  id,source_cycle_id,environment,agency_id,source_group_id,client_id,
  cutoff_at_utc,version,state,projection_state
) values (
  'b4000000-0000-4000-8000-000000000002',
  'b4000000-0000-4000-8000-000000000001','TEST',
  'a0000000-0000-4000-8000-000000000006',
  'b0000000-0000-4000-8000-000000000005',
  'b0000000-0000-4000-8000-000000000002',
  '2026-09-16T14:00:00Z',1,'OPEN','REBUILDING'
);
insert into public.weekly_source_uploads(
  id,source_cycle_id,report_scope_id,original_filename,content_sha256,byte_count,
  source_format_profile_id,parser_version,normaliser_version,
  workbook_part_and_sheet_fingerprint,header_coordinate_map_json,
  header_coordinate_map_hash,money_lexical_authority_version,
  declared_scope_fingerprint,coverage_proof_kind,physical_row_count,accepted_count,
  row_manifest_hash,state,uploaded_by_user_id,file_metadata_json
) values (
  'b4000000-0000-4000-8000-000000000003',
  'b4000000-0000-4000-8000-000000000001',
  'b4000000-0000-4000-8000-000000000002','BR-004.xlsx',
  private.weekly_source_sha256_jsonb_v1('FINALISER_NHSP_CONTENT','{"report":"BR-004"}'::jsonb),
  100,'32222222-2222-4222-8222-222222222222','FINALISER_TEST_PARSER_V1',
  'NHSP_BACKING_NORMALISER_V1',
  private.weekly_source_sha256_jsonb_v1('FINALISER_NHSP_WORKBOOK','{"report":"BR-004"}'::jsonb),
  '{}'::jsonb,
  private.weekly_source_sha256_jsonb_v1('FINALISER_NHSP_HEADERS','{"report":"BR-004"}'::jsonb),
  'XLSX_BINARY64_SAME_VALUE_PENCE_V1',
  private.weekly_source_sha256_jsonb_v1('FINALISER_NHSP_SCOPE','{"report":"BR-004"}'::jsonb),
  'NHSP_TRUST_REPORT_SCOPE',1,1,
  private.weekly_source_sha256_jsonb_v1('FINALISER_NHSP_ROWS','{"report":"BR-004"}'::jsonb),
  'CURRENT','a0000000-0000-4000-8000-000000000001',
  pg_catalog.jsonb_build_object(
    'nhsp_report_number','BR-004','nhsp_report_heading_name','Finaliser NHSP Trust'
  )
);
update public.weekly_source_report_scopes
set current_complete_upload_id='b4000000-0000-4000-8000-000000000003'
where id='b4000000-0000-4000-8000-000000000002';
insert into public.weekly_source_upload_rows(
  id,upload_id,source_row_ordinal,external_source_key,source_candidate_identity,
  source_client_identity,work_date,start_at_local,end_at_local,break_minutes,
  actual_net_minutes,row_finalisation_state,role_band_source,
  source_commission_pence,source_total_cost_pence,source_shift_charge_pence,
  source_money_parse_state,source_qualification_profile_version,
  source_expense_parse_state,normalised_row_hash
) values (
  'b4000000-0000-4000-8000-000000000004',
  'b4000000-0000-4000-8000-000000000003',1,'NHSP-ZERO-CHARGE-1',
  'Finaliser Candidate','Finaliser NHSP Trust','2026-09-14',
  '2026-09-14 09:00','2026-09-14 17:00',30,450,'SOURCE_WORKED','BAND 5',
  0,0,0,'VALID','NHSP_TWO_COMPONENT_PENCE_V1','NOT_APPLICABLE',
  private.weekly_source_sha256_jsonb_v1('FINALISER_NHSP_ROW','{"row":"zero-charge"}'::jsonb)
);
insert into public.weekly_source_projection_publications(
  id,source_cycle_id,authority_scope_kind,report_scope_id,upload_id,
  authority_scope_version,projection_generation,
  comparison_manifest_hash,issue_set_hash,state
) values (
  'b4000000-0000-4000-8000-000000000006',
  'b4000000-0000-4000-8000-000000000001','NHSP_REPORT_SCOPE',
  'b4000000-0000-4000-8000-000000000002',
  'b4000000-0000-4000-8000-000000000003',1,1,
  private.weekly_source_sha256_jsonb_v1('FINALISER_NHSP_COMPARISON','{"report":"BR-004"}'::jsonb),
  private.weekly_source_sha256_jsonb_v1('FINALISER_NHSP_ISSUES','{"report":"BR-004"}'::jsonb),
  'BUILDING'
);
select public.weekly_source_projection_rows_apply_atomic_v1(
  'a0000000-0000-4000-8000-000000000001',
  'b4000000-0000-4000-8000-000000000006',
  pg_catalog.jsonb_build_array(
    pg_catalog.jsonb_build_object(
      'upload_row_id','b4000000-0000-4000-8000-000000000004',
      'mapping_state','RESOLVED','candidate_id','a0000000-0000-4000-8000-000000000003',
      'client_id','b0000000-0000-4000-8000-000000000002',
      'contract_id','b0000000-0000-4000-8000-000000000004',
      'contract_selection_method','AUTO_UNIQUE',
      'qualifying_contract_ids',pg_catalog.jsonb_build_array('b0000000-0000-4000-8000-000000000004'),
      'identity_kind','SCHEDULE_TUPLE','link_kind','POSITIVE_SOURCE',
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
      'qualification_observations',pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'contract_id','b0000000-0000-4000-8000-000000000004',
        'contract_revision_fingerprint',repeat('ab',32),
        'source_shift_charge_pence','0','canonical_calculated_pence','15000',
        'comparison_result','ZERO_SOURCE_CHARGE',
        'reason_codes',pg_catalog.jsonb_build_array('ZERO_SOURCE_CHARGE')
      )),
      'charge_check',pg_catalog.jsonb_build_object(
        'row_sign_kind','POSITIVE','source_commission_pence','0',
        'source_total_cost_pence','0','source_shift_charge_pence','0',
        'calculated_segment_charge_pence','15000','comparison_result','ZERO_SOURCE_CHARGE',
        'comparison_reason_code','ZERO_SOURCE_CHARGE','phase_severity','PROVISIONAL_WARNING'
      )
    )
  )
);
update public.weekly_source_projection_publications
set state='CURRENT',published_at_utc=pg_catalog.clock_timestamp()
where id='b4000000-0000-4000-8000-000000000006';
update public.weekly_source_report_scopes
set projection_state='CURRENT',
    current_projection_publication_id='b4000000-0000-4000-8000-000000000006'
where id='b4000000-0000-4000-8000-000000000002';
do $accept_zero_charge$
declare
  v_generation bigint;
  v_workspace_version text;
  v_proof text;
  v_result jsonb;
begin
  select projection_generation into strict v_generation
  from public.weekly_source_projection_publications
  where id='b4000000-0000-4000-8000-000000000006';
  v_workspace_version:=private.weekly_source_office_workspace_version_v1(
    'b4000000-0000-4000-8000-000000000001',
    'b4000000-0000-4000-8000-000000000006'
  );
  v_proof:=pg_catalog.encode(private.weekly_source_sha256_jsonb_v1(
    'NHSP_RATE_WARNING_SELECTION_V1',
    pg_catalog.jsonb_build_object(
      'source_cycle_id','b4000000-0000-4000-8000-000000000001'::uuid,
      'projection_publication_id','b4000000-0000-4000-8000-000000000006'::uuid,
      'projection_generation',v_generation,
      'workspace_version',v_workspace_version,
      'eligible_warning_keys',pg_catalog.jsonb_build_array('all-zero-source-charge')
    )
  ),'hex');
  v_result:=public.weekly_source_charge_accept_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','a0000000-0000-4000-8000-000000000001',
    'source_cycle_id','b4000000-0000-4000-8000-000000000001',
    'projection_publication_id','b4000000-0000-4000-8000-000000000006',
    'warning_keys',pg_catalog.jsonb_build_array('all-zero-source-charge'),
    'selection_proof',v_proof
  ));
  if v_result->>'status'<>'ACCEPTED' or (v_result->>'accepted_count')::integer<>1 then
    raise exception 'ASSERTION_FAILED: zero source charge was not accepted through the Office owner %',v_result;
  end if;
end;
$accept_zero_charge$;
select pg_temp.finalise_nhsp(
  'b4000000-0000-4000-8000-000000000001',
  'b4000000-0000-4000-8000-000000000002',
  'b4000000-0000-4000-8000-000000000003',
  'b4000000-0000-4000-8000-000000000006'
);
select pg_temp.assert_true(
  exists(
    select 1
    from public.weekly_source_billing_movements movement
    join public.weekly_source_charge_acceptances acceptance
      on acceptance.id=movement.charge_acceptance_id
    where movement.finalisation_cycle_id='b4000000-0000-4000-8000-000000000001'
      and movement.movement_role='POSITIVE'
      and movement.source_line_kind='NHSP_PHYSICAL_POSITIVE'
      and movement.source_validation_charge_pence=0
      and movement.invoice_presentation_charge_pence=0
      and movement.calculated_comparison_charge_pence=15000
      and movement.price_check_result='ACCEPTED_ZERO'
      and movement.total_pay_ex_vat=75.00
      and acceptance.acceptance_kind='ACCEPTED_ZERO'
  ),
  'an accepted £0 source row must retain £0 invoice authority and positive independently calculated pay'
);
select pg_temp.assert_true(
  exists(
    select 1 from public.weekly_source_client_manifests manifest
    where manifest.source_cycle_id='b4000000-0000-4000-8000-000000000001'
      and manifest.backing_report_number='BR-004'
      and manifest.movement_count=1
  ),
  'the accepted £0 physical source movement must remain present in the client manifest'
);

-- The finalisation owner must be isolated from protected pay, query delivery,
-- invoice admission and every Banking Pay / Draft owner.
select pg_temp.assert_true(
  pg_catalog.strpos(pg_catalog.pg_get_functiondef(
    'private.weekly_source_finalise_engine_v1(jsonb,uuid,boolean)'::pg_catalog.regprocedure
  ),'weekly_protected_pay')=0,
  'finaliser must not read protected-pay state'
);
select pg_temp.assert_true(
  pg_catalog.strpos(pg_catalog.pg_get_functiondef(
    'private.weekly_source_finalise_engine_v1(jsonb,uuid,boolean)'::pg_catalog.regprocedure
  ),'weekly_query')=0,
  'finaliser must not read query-delivery state'
);
select pg_temp.assert_true(
  pg_catalog.strpos(pg_catalog.pg_get_functiondef(
    'private.weekly_source_finalise_engine_v1(jsonb,uuid,boolean)'::pg_catalog.regprocedure
  ),'pay_batch')=0
  and pg_catalog.strpos(pg_catalog.pg_get_functiondef(
    'private.weekly_source_finalise_engine_v1(jsonb,uuid,boolean)'::pg_catalog.regprocedure
  ),'banking_pay')=0,
  'finaliser must not cross the Workbench/Banking boundary'
);
select pg_temp.assert_true(
  pg_catalog.strpos(pg_catalog.pg_get_functiondef(
    'private.weekly_source_finalise_engine_v1(jsonb,uuid,boolean)'::pg_catalog.regprocedure
  ),'weekly_source_finalise_order:')>0
  and pg_catalog.strpos(pg_catalog.pg_get_functiondef(
    'private.weekly_source_finalise_engine_v1(jsonb,uuid,boolean)'::pg_catalog.regprocedure
  ),'v_source_profile_kind')>0,
  'finaliser must serialize the complete logical group/Client/authority-kind history'
);
select pg_temp.assert_true(
  pg_catalog.strpos(pg_catalog.upper(pg_catalog.pg_get_functiondef(
    'private.weekly_source_finalise_engine_v1(jsonb,uuid,boolean)'::pg_catalog.regprocedure
  )),'MAGNIT')=0,
  'generic roster must remain HEALTHROSTER_WEEKLY at the C1 boundary'
);

select pg_catalog.jsonb_build_object(
  'ok',true,'verification','weekly_source_finalisation_v1',
  'generic_transitions',(
    select pg_catalog.jsonb_object_agg(outcome,n)
    from (select outcome,pg_catalog.count(*) n
          from public.weekly_source_state_transitions group by outcome) counts
  ),
  'generic_movements',(select pg_catalog.count(*) from public.weekly_source_billing_movements),
  'roster_c1_source_mode','HEALTHROSTER_WEEKLY'
);

\if :{?weekly_source_verification_outer_transaction}
\else
rollback;
\endif
