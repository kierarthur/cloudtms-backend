-- Dedicated rollback-only invoice-discovery seed. Genuine a1 Final and
-- first mutable TSFIN preparation only: no fabricated paid/approved HEAD.
-- Finite helper/setup bodies copied from the reviewed existing verifiers.
-- This does not execute or certify their unrelated later lifecycle cases.
\set ON_ERROR_STOP on

-- Existing TEST history is valid, not an empty-database prerequisite. Capture
-- fixture inserts and reject any UPDATE/DELETE/TRUNCATE of unrelated history.
\ir support/06102026_1117_source_workbench_fixture_isolation.sql
do $invoice_fixture_watch$
declare v_relation regclass;
begin
  if exists(select 1 from public.candidates where id='a0000000-0000-4000-8000-000000000003')
     or exists(select 1 from public.contracts where id='a0000000-0000-4000-8000-000000000004')
     or exists(select 1 from public.weekly_source_cycles where id='a1000000-0000-4000-8000-000000000001')
     or exists(select 1 from public.timesheets where timesheet_id in (
       'fedcba98-0000-4000-8000-0000000027a1','fedcba98-0000-4000-8000-0000000027a2',
       'fedcba98-0000-4000-8000-0000000027a3','fedcba98-0000-4000-8000-0000000027a4')) then
    raise exception 'INVOICE_FIXTURE_NAMESPACE_COLLISION';
  end if;
  foreach v_relation in array array[
    'public.timesheets'::regclass,
    'public.timesheets_financials'::regclass,
    'public.weekly_source_root_authorisations'::regclass,
    'public.weekly_exceptional_pay_target_families'::regclass,
    'public.invoices'::regclass,
    'public.invoice_lines'::regclass
  ] loop
    perform pg_temp.ws_verify_watch(v_relation);
  end loop;
end $invoice_fixture_watch$;

create function pg_temp.invoice_fixture_nontarget_fingerprint()
returns jsonb language plpgsql set search_path='' as $f$
declare v_relation record; v_fingerprint jsonb; v_result jsonb:='{}'::jsonb;
begin
  for v_relation in
    select watched.rel,watched.key_expression,
      pg_catalog.format('%I.%I',namespace.nspname,relation.relname) as qualified_name
    from pg_temp.ws_verify_relations watched
    join pg_catalog.pg_class relation on relation.oid=watched.rel
    join pg_catalog.pg_namespace namespace on namespace.oid=relation.relnamespace
    order by namespace.nspname,relation.relname
  loop
    execute pg_catalog.format($q$
      select pg_catalog.jsonb_build_object('count',pg_catalog.count(*),'sha256',
        pg_catalog.encode(extensions.digest(coalesce(
          pg_catalog.string_agg(row_digest,'' order by row_digest),''),'sha256'),'hex'))
      from (select pg_catalog.encode(extensions.digest(
        pg_catalog.to_jsonb(x)::text,'sha256'),'hex') as row_digest
        from %s x where not exists (
          select 1 from pg_temp.ws_verify_keys own
          where own.rel=%s and own.key=pg_catalog.jsonb_build_array(%s))) rows
    $q$,v_relation.qualified_name,v_relation.rel::oid,
      pg_catalog.replace(v_relation.key_expression,'($1)','x')) into v_fingerprint;
    v_result:=v_result||pg_catalog.jsonb_build_object(v_relation.qualified_name,v_fingerprint);
  end loop;
  return v_result;
end $f$;
create temporary table invoice_fixture_nontarget_before on commit drop as
  select pg_temp.invoice_fixture_nontarget_fingerprint() as fingerprint;

\if :{?weekly_source_verification_correction_presentation}
\else
\set weekly_source_verification_correction_presentation 'FULL_REVERSAL_REPLACEMENT'
\endif
\if :{?weekly_source_verification_expense_vat_enabled}
\else
\set weekly_source_verification_expense_vat_enabled false
\endif
create function pg_temp.assert_true(p_condition boolean,p_message text)
returns void language plpgsql as $function$
begin
  if p_condition is distinct from true then
    raise exception 'ASSERTION_FAILED: %',p_message;
  end if;
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
  p_source_group_id uuid default 'a0000000-0000-4000-8000-000000000005',
  p_prepared boolean default true
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
    pg_catalog.jsonb_build_object('client_id','a0000000-0000-4000-8000-000000000002',
      'import_use',case when p_prepared then 'PREPARE_FINALISATION' else 'CHECKING' end)
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
  p_cycle_id uuid,p_upload_id uuid,p_publication_id uuid,p_exclude_unfinalised boolean default false
) returns jsonb language plpgsql as $function$
declare
  v_result jsonb;
begin
  select public.weekly_source_finalise_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','a0000000-0000-4000-8000-000000000001',
    'source_cycle_id',p_cycle_id,'authority_scope_kind','CYCLE',
    'upload_id',p_upload_id,'projection_publication_id',p_publication_id,
    'expected_authority_scope_version',1,
    'exclude_unfinalised_acknowledged',p_exclude_unfinalised,
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
  '2026-09-07 09:00','2026-09-07 17:00',30,450,7500,15000,100,true,p_prepared=>false
);
do $checking_then_prepare$
begin
  begin
    perform pg_temp.finalise_cycle(
      'a1000000-0000-4000-8000-000000000001','a1000000-0000-4000-8000-000000000002',
      'a1000000-0000-4000-8000-000000000003');
    raise exception 'VERIFY_FAILED: checking file was finalised without preparation';
  exception when sqlstate '55000' then
    if sqlerrm<>'WEEKLY_SOURCE_CHECKING_FILE_NOT_FINALISABLE' then raise; end if;
  end;
  perform public.weekly_source_import_prepare_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','a0000000-0000-4000-8000-000000000001',
    'upload_id','a1000000-0000-4000-8000-000000000002',
    'projection_publication_id','a1000000-0000-4000-8000-000000000003',
    'expected_authority_scope_version',1,
    'expected_row_manifest_hash',(select pg_catalog.encode(row_manifest_hash,'hex')
      from public.weekly_source_uploads where id='a1000000-0000-4000-8000-000000000002')
  ));
end;
$checking_then_prepare$;
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
create function pg_temp.ordinary_projection_request(
  p_source_cycle_id uuid,
  p_idempotency_key text,
  p_root_timesheet_id uuid default null
) returns jsonb language plpgsql as $function$
declare
  v_actor constant uuid:='a0000000-0000-4000-8000-000000000001';
  v_revision public.weekly_source_final_revisions%rowtype;
  v_profile public.weekly_source_format_profiles%rowtype;
  v_manifest public.weekly_source_client_manifests%rowtype;
  v_timesheet public.timesheets%rowtype;
  v_contract public.contracts%rowtype;
  v_current public.timesheets_financials%rowtype;
  v_root uuid;
  v_client uuid;
  v_source_mode text;
  v_source_units jsonb;
  v_segments jsonb;
  v_expenses jsonb;
  v_actual jsonb;
  v_rate_refs jsonb;
  v_policy jsonb;
  v_invoice_breakdown jsonb;
  v_tsfin jsonb;
  v_first jsonb;
  v_source_unit_hash bytea;
  v_source_expense_hash bytea;
  v_active_segment_hash bytea;
  v_core_pay numeric:=0;
  v_core_charge numeric:=0;
  v_hours_day numeric:=0;
  v_hours_night numeric:=0;
  v_hours_sat numeric:=0;
  v_hours_sun numeric:=0;
  v_hours_bh numeric:=0;
  v_additional_pay numeric:=0;
  v_additional_charge numeric:=0;
  v_additional_margin numeric:=0;
  v_expense_pay numeric:=0;
  v_expense_charge numeric:=0;
  v_mileage_pay numeric:=0;
  v_mileage_charge numeric:=0;
  v_total_pay numeric:=0;
  v_total_charge numeric:=0;
  v_margin numeric:=0;
  v_wage_pay numeric:=0;
  v_reimbursement_pay numeric:=0;
  v_erni_pct numeric:=0;
  v_erni_multiplier numeric:=1;
  v_expense_description text;
  v_expense_evidence_r2_key text;
  v_expense_evidence_manifest jsonb;
begin
  select revision.* into strict v_revision
  from public.weekly_source_final_revisions revision
  where revision.source_cycle_id=p_source_cycle_id and revision.state='CURRENT';
  select profile.* into strict v_profile
  from public.weekly_source_uploads upload_row
  join public.weekly_source_format_profiles profile
    on profile.id=upload_row.source_format_profile_id
  where upload_row.id=v_revision.upload_id;
  if p_root_timesheet_id is null then
    select movement.invoice_timesheet_id,movement.actual_client_id
      into strict v_root,v_client
    from public.weekly_source_billing_movements movement
    where movement.final_revision_id=v_revision.id
    order by case when movement.source_line_kind='SOURCE_FIXED_EXPENSE' then 1 else 0 end,
             movement.id
    limit 1;
  else
    -- Correct Final must also submit a current service snapshot for a root
    -- removed by the replacement revision.  Such a prior-only root has no
    -- replacement movement, so its immutable Timesheet/Contract owns client
    -- identity for this verifier request.
    v_root:=p_root_timesheet_id;
    select contract.client_id into strict v_client
    from public.timesheets timesheet
    join public.contracts contract on contract.id=timesheet.contract_id
    where timesheet.timesheet_id=v_root;
  end if;
  select manifest.* into strict v_manifest
  from public.weekly_source_client_manifests manifest
  where manifest.final_revision_id=v_revision.id and manifest.client_id=v_client;
  select timesheet.* into strict v_timesheet
  from public.timesheets timesheet where timesheet.timesheet_id=v_root;
  select contract.* into strict v_contract
  from public.contracts contract where contract.id=v_timesheet.contract_id;
  select financial.* into v_current
  from public.timesheets_financials financial
  where financial.timesheet_id=v_root and financial.is_current
  order by financial.computed_at_utc desc nulls last,
           financial.updated_at desc nulls last,financial.id desc
  limit 1;

  v_source_mode:=case when v_profile.final_authority_kind='NHSP_TRUST_BACKING_REPORT'
    then 'NHSP_WEEKLY' else 'HEALTHROSTER_WEEKLY' end;
  v_source_units:=private.weekly_source_ordinary_projection_source_units_v1(
    v_revision.id,v_root
  );
  v_segments:=private.weekly_source_ordinary_projection_current_segments_v1(
    v_root,v_revision.id
  );
  v_expenses:=private.weekly_source_ordinary_projection_current_expenses_v1(
    v_root,v_revision.id
  );
  v_actual:=private.weekly_source_ordinary_projection_actual_schedule_v1(v_segments);
  v_source_unit_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_ORDINARY_UNIT_MANIFEST_V1',v_source_units
  );
  v_source_expense_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_ORDINARY_SOURCE_EXPENSE_MANIFEST_V1',v_expenses
  );
  v_active_segment_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_ORDINARY_ACTIVE_SEGMENTS_V1',v_segments
  );
  v_rate_refs:=pg_catalog.jsonb_build_object(
    'schema_version','WEEKLY_SOURCE_FINAL_AUTHORITY_RATE_SOURCE_V1',
    'source_mode',v_source_mode,'root_timesheet_id',v_root,
    'final_revision_id',v_revision.id,
    'final_manifest_hash',pg_catalog.encode(v_revision.manifest_hash,'hex'),
    'final_policy_fingerprint',pg_catalog.encode(v_revision.policy_fingerprint,'hex'),
    'client_manifest_hash',pg_catalog.encode(v_manifest.manifest_hash,'hex'),
    'source_unit_manifest_hash',pg_catalog.encode(v_source_unit_hash,'hex'),
    'source_expense_manifest_hash',pg_catalog.encode(v_source_expense_hash,'hex'),
    'active_segment_manifest_hash',pg_catalog.encode(v_active_segment_hash,'hex')
  );
  v_policy:=(private._timesheet_settings_authority_frozen_v1(v_root)->'values')
    -'resolved_at_utc';

  select
    coalesce(pg_catalog.sum((segment.value->>'pay_amount')::numeric),0),
    coalesce(pg_catalog.sum((segment.value->>'charge_amount')::numeric),0),
    coalesce(pg_catalog.sum((segment.value->>'hours_day')::numeric),0),
    coalesce(pg_catalog.sum((segment.value->>'hours_night')::numeric),0),
    coalesce(pg_catalog.sum((segment.value->>'hours_sat')::numeric),0),
    coalesce(pg_catalog.sum((segment.value->>'hours_sun')::numeric),0),
    coalesce(pg_catalog.sum((segment.value->>'hours_bh')::numeric),0)
  into v_core_pay,v_core_charge,v_hours_day,v_hours_night,
       v_hours_sat,v_hours_sun,v_hours_bh
  from pg_catalog.jsonb_array_elements(v_segments) segment(value);
  v_additional_pay:=coalesce(v_current.additional_pay_ex_vat,0);
  v_additional_charge:=coalesce(v_current.additional_charge_ex_vat,0);
  v_additional_margin:=coalesce(v_current.additional_margin_ex_vat,0);
  if pg_catalog.jsonb_array_length(v_expenses)>0 then
    select coalesce(pg_catalog.sum((expense.value->>'source_expense_pence')::numeric),0)/100
      into v_expense_pay
    from pg_catalog.jsonb_array_elements(v_expenses) expense(value);
    v_expense_charge:=v_expense_pay;
    v_expense_description:='Source-approved expenses';
    v_expense_evidence_r2_key:=null;
    v_expense_evidence_manifest:=pg_catalog.jsonb_build_object(
      'schema_version','WEEKLY_SOURCE_FIXED_EXPENSE_TSFIN_LINEAGE_V1',
      'authorities',v_expenses,
      'manifest_hash',pg_catalog.encode(v_source_expense_hash,'hex')
    );
  elsif coalesce(v_current.expenses_evidence_manifest->>'schema_version','')=
        'WEEKLY_SOURCE_FIXED_EXPENSE_TSFIN_LINEAGE_V1' then
    -- The newest zero authority is an immutable tombstone, not payable
    -- Timesheet evidence.  Build the service snapshot without the superseded
    -- source-fixed expense while preserving unrelated expense families below.
    v_expense_pay:=0;
    v_expense_charge:=0;
    v_expense_description:=null;
    v_expense_evidence_r2_key:=null;
    v_expense_evidence_manifest:='null'::jsonb;
  else
    v_expense_pay:=coalesce(v_current.expenses_pay_ex_vat,0);
    v_expense_charge:=coalesce(v_current.expenses_charge_ex_vat,0);
    v_expense_description:=v_current.expenses_description;
    v_expense_evidence_r2_key:=v_current.expenses_evidence_r2_key;
    v_expense_evidence_manifest:=coalesce(
      pg_catalog.to_jsonb(v_current.expenses_evidence_manifest),'null'::jsonb
    );
  end if;
  v_mileage_pay:=coalesce(v_current.mileage_pay_ex_vat,0);
  v_mileage_charge:=coalesce(v_current.mileage_charge_ex_vat,0);
  v_total_pay:=pg_catalog.round(
    v_core_pay+v_additional_pay+v_expense_pay+v_mileage_pay,2
  );
  v_total_charge:=pg_catalog.round(
    v_core_charge+v_additional_charge+v_expense_charge+v_mileage_charge,2
  );
  v_wage_pay:=pg_catalog.round(v_core_pay+v_additional_pay,2);
  v_reimbursement_pay:=pg_catalog.round(v_expense_pay+v_mileage_pay,2);
  v_erni_pct:=coalesce((v_policy->>'erni_pct')::numeric,0);
  if v_erni_pct>0 then
    v_erni_multiplier:=1+case when v_erni_pct>1 then v_erni_pct/100 else v_erni_pct end;
  end if;
  v_margin:=pg_catalog.round(v_total_charge-(
    case when pg_catalog.upper(coalesce(v_contract.pay_method_snapshot,''))='PAYE'
              and pg_catalog.upper(coalesce(v_policy->>'apply_erni_to','PAYE_ONLY'))
                    in ('ALL','PAYE_ONLY')
      then pg_catalog.round(v_wage_pay*v_erni_multiplier,2)
      else v_wage_pay end
    +v_reimbursement_pay
  ),2);
  v_first:=v_segments->0;
  v_invoice_breakdown:=pg_catalog.jsonb_build_object(
    'mode','SEGMENTS','segments',v_segments,
    'additional',pg_catalog.jsonb_build_object(
      'units',coalesce(v_current.additional_units_json,'{}'::jsonb),
      'pay_ex_vat',v_additional_pay,'charge_ex_vat',v_additional_charge,
      'margin_ex_vat',v_additional_margin
    ),
    'totals',pg_catalog.jsonb_build_object(
      'total_pay_ex_vat',v_total_pay,'total_charge_ex_vat',v_total_charge,
      'margin_ex_vat',v_margin
    )
  );

  v_tsfin:=pg_catalog.jsonb_build_object(
    'timesheet_id',v_root,'timesheet_version',v_timesheet.version,
    'basis',case when v_source_mode='NHSP_WEEKLY' then 'NHSP'
                 else 'HEALTHROSTER_SELF_BILL' end,
    'candidate_assignment','ASSIGNED','processing_status','PENDING_AUTH',
    'candidate_id',v_contract.candidate_id,'client_id',v_contract.client_id,
    'role',v_contract.role,'band',v_contract.band,
    'pay_method',v_contract.pay_method_snapshot,
    'policy_snapshot_json',v_policy,'rate_source_refs_json',v_rate_refs,
    'invoice_breakdown_json',v_invoice_breakdown,
    'hours_day',pg_catalog.round(v_hours_day,2),
    'hours_night',pg_catalog.round(v_hours_night,2),
    'hours_sat',pg_catalog.round(v_hours_sat,2),
    'hours_sun',pg_catalog.round(v_hours_sun,2),
    'hours_bh',pg_catalog.round(v_hours_bh,2),
    'total_hours',pg_catalog.round(
      v_hours_day+v_hours_night+v_hours_sat+v_hours_sun+v_hours_bh,2
    )
  )||pg_catalog.jsonb_build_object(
    'pay_day',nullif(v_first#>>'{weekly_source,pay_vector,rates,day}','')::numeric,
    'pay_night',nullif(v_first#>>'{weekly_source,pay_vector,rates,night}','')::numeric,
    'pay_sat',nullif(v_first#>>'{weekly_source,pay_vector,rates,sat}','')::numeric,
    'pay_sun',nullif(v_first#>>'{weekly_source,pay_vector,rates,sun}','')::numeric,
    'pay_bh',nullif(v_first#>>'{weekly_source,pay_vector,rates,bh}','')::numeric,
    'charge_day',nullif(v_first#>>'{weekly_source,charge_vector,rates,day}','')::numeric,
    'charge_night',nullif(v_first#>>'{weekly_source,charge_vector,rates,night}','')::numeric,
    'charge_sat',nullif(v_first#>>'{weekly_source,charge_vector,rates,sat}','')::numeric,
    'charge_sun',nullif(v_first#>>'{weekly_source,charge_vector,rates,sun}','')::numeric,
    'charge_bh',nullif(v_first#>>'{weekly_source,charge_vector,rates,bh}','')::numeric,
    'total_pay_ex_vat',v_total_pay,'total_charge_ex_vat',v_total_charge,
    'margin_ex_vat',v_margin,
    'additional_units_json',coalesce(v_current.additional_units_json,'{}'::jsonb),
    'additional_pay_ex_vat',v_additional_pay,
    'additional_charge_ex_vat',v_additional_charge,
    'additional_margin_ex_vat',v_additional_margin
  )||pg_catalog.jsonb_build_object(
    'expenses_pay_ex_vat',v_expense_pay,
    'expenses_charge_ex_vat',v_expense_charge,
    'expenses_description',v_expense_description,
    'expenses_evidence_r2_key',v_expense_evidence_r2_key,
    'expenses_evidence_manifest',v_expense_evidence_manifest,
    'mileage_units',coalesce(v_current.mileage_units,0),
    'mileage_pay_ex_vat',v_mileage_pay,
    'mileage_charge_ex_vat',v_mileage_charge,
    'mileage_pay_rate',v_current.mileage_pay_rate,
    'mileage_charge_rate',v_current.mileage_charge_rate,
    'mileage_evidence_r2_key',v_current.mileage_evidence_r2_key,
    'mileage_evidence_manifest',coalesce(
      pg_catalog.to_jsonb(v_current.mileage_evidence_manifest),'null'::jsonb
    )
  );

  return pg_catalog.jsonb_build_object(
    'actor_user_id',v_actor,
    'final_revision_id',v_revision.id,
    'idempotency_key',p_idempotency_key,
    'root_timesheet_id',v_root,
    'schema_version','WEEKLY_SOURCE_ORDINARY_PAY_PROJECTION_REQUEST_V1',
    'service_snapshot',pg_catalog.jsonb_build_object(
      'schema_version','WEEKLY_SOURCE_ORDINARY_TSFIN_SERVICE_SNAPSHOT_V1',
      'calculator_owner','buildWeeklyScheduleSegmentsSnapshot',
      'source_actual_schedule_json',v_actual,
      'tsfin_snapshot_json',v_tsfin
    )
  );
end;
$function$;
select public.weekly_source_ordinary_pay_projection_apply_atomic_v1(
  pg_temp.ordinary_projection_request(
    'a1000000-0000-4000-8000-000000000001','projection-a1'
  )
) as ordinary_add_result;

select pg_temp.assert_true(
  (select financial.total_hours=7.50
          and financial.total_pay_ex_vat=76.00
          and financial.total_charge_ex_vat=151.00
          and financial.expenses_pay_ex_vat=1.00
          and financial.expenses_charge_ex_vat=1.00
          and financial.expenses_evidence_r2_key is null
          and financial.mileage_units=0
          and financial.mileage_pay_ex_vat=0
          and financial.mileage_charge_ex_vat=0
          and financial.mileage_evidence_r2_key is null
          and financial.processing_status='PENDING_AUTH'::public.ts_fin_processing_status_enum
   from public.timesheets_financials financial
   join public.weekly_source_ordinary_pay_projection_receipts receipt
     on receipt.published_timesheet_financial_id=financial.id
   where receipt.idempotency_key='projection-a1'
     and receipt.final_revision_id=(select id from public.weekly_source_final_revisions
       where source_cycle_id='a1000000-0000-4000-8000-000000000001' and state='CURRENT')),
  'ADD must publish source hours plus source-fixed expense through current TSFIN'
);
select pg_temp.assert_true(
  (select pg_catalog.count(*)=1
   from public.weekly_source_expense_pay_materialisations materialisation
   join public.weekly_source_ordinary_pay_projection_receipts receipt
     on receipt.published_timesheet_financial_id=
          materialisation.candidate_timesheet_financial_id
   where receipt.idempotency_key='projection-a1'
     and receipt.final_revision_id=(select id from public.weekly_source_final_revisions
       where source_cycle_id='a1000000-0000-4000-8000-000000000001' and state='CURRENT')),
  'ADD must bind the positive source expense authority to ordinary TSFIN once'
);

select pg_temp.assert_true(
  (public.weekly_source_ordinary_pay_projection_apply_atomic_v1(
    pg_temp.ordinary_projection_request(
      'a1000000-0000-4000-8000-000000000001','projection-a1'
    )
  )->>'idempotent_replay')::boolean,
  'identical projection replay must return the immutable receipt'
);
select pg_temp.assert_true(
  (select count(*)=1 from public.weekly_source_final_revisions
    where source_cycle_id='a1000000-0000-4000-8000-000000000001' and state='CURRENT'),
  'invoice seed has exactly one genuine current Final');
select pg_temp.assert_true(
  not exists(select 1 from public.weekly_source_root_authorisations authorisation
    join public.timesheets root on root.timesheet_id=authorisation.root_timesheet_id
    where root.contract_id='a0000000-0000-4000-8000-000000000004')
  and not exists(select 1 from public.timesheets
    where contract_id='a0000000-0000-4000-8000-000000000004' and authorised_at_server is not null)
  and not exists(select 1 from public.weekly_exceptional_pay_target_families
    where agency_id='a0000000-0000-4000-8000-000000000006'
      and candidate_id='a0000000-0000-4000-8000-000000000003'
      and contract_id='a0000000-0000-4000-8000-000000000004')
  and (select count(*)=1 from public.timesheets_financials financial
    join public.timesheets root on root.timesheet_id=financial.timesheet_id
    where root.contract_id='a0000000-0000-4000-8000-000000000004' and financial.is_current),
  'invoice seed is first preparation, not Authorise, paid state or TARGET ownership');
