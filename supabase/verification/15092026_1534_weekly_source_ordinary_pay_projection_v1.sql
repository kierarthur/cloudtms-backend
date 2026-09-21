-- Rollback-only proof for weekly_source_ordinary_pay_projection_v1.
-- Reuses the full finalisation verifier inside this outer transaction so the
-- projection is exercised against real frozen final revisions, movements,
-- economic snapshots and Timesheet lineage rather than synthetic shortcuts.

\set ON_ERROR_STOP on

\if :{?weekly_source_ordinary_verification_outer_transaction}
\else
begin;
\endif
set local request.jwt.claim.role='service_role';
\set weekly_source_verification_outer_transaction true
\ir 15092026_1534_weekly_source_finalisation_v1.sql

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

create function pg_temp.target_zero_request(
  p_family_prepare_result jsonb,
  p_idempotency_key text
) returns jsonb language plpgsql as $function$
declare
  v_actor uuid;
  v_family public.weekly_exceptional_pay_target_families%rowtype;
  v_timesheet public.timesheets%rowtype;
  v_contract public.contracts%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_effective_policy jsonb;
  v_settings_policy jsonb;
  v_source_mode text;
  v_source_profile_domain text;
  v_rate_refs jsonb;
  v_invoice_breakdown jsonb;
  v_tsfin jsonb;
begin
  select run.requested_by_user_id into strict v_actor
  from public.weekly_exceptional_orchestration_runs run
  where run.id=(p_family_prepare_result->>'orchestration_run_id')::uuid;
  select family.* into strict v_family
  from public.weekly_exceptional_pay_target_families family
  where family.id=(p_family_prepare_result->>'family_id')::uuid;
  select timesheet.* into strict v_timesheet
  from public.timesheets timesheet
  where timesheet.timesheet_id=v_family.root_timesheet_id;
  select contract.* into strict v_contract
  from public.contracts contract where contract.id=v_family.contract_id;
  select cycle.* into strict v_cycle
  from public.weekly_source_cycles cycle
  where cycle.id=(p_family_prepare_result->>'source_cycle_id')::uuid;
  select source_group.* into strict v_group
  from public.weekly_source_groups source_group
  where source_group.id=v_cycle.source_group_id;
  v_effective_policy:=private._weekly_source_effective_policy_v1(
    v_contract.client_id,v_contract.id,v_family.week_ending_date
  );
  v_settings_policy:=(
    private._timesheet_settings_authority_frozen_v1(v_timesheet.timesheet_id)->'values'
  )-'resolved_at_utc';
  v_source_mode:=v_effective_policy->>'c1_source_mode';
  v_source_profile_domain:=case when v_source_mode='NHSP_WEEKLY'
    then 'NHSP_TRUST_BACKING_REPORT' else 'ROSTER_FINAL_AUTHORITY' end;
  v_rate_refs:=pg_catalog.jsonb_build_object(
    'schema_version','WEEKLY_SOURCE_TARGET_MANAGED_ZERO_RATE_SOURCE_V1',
    'source_cycle_id',v_cycle.id,'source_group_id',v_group.id,
    'source_family',v_group.source_family,'source_mode',v_source_mode,
    'source_profile_domain',v_source_profile_domain,
    'target_family_id',v_family.id,'root_timesheet_id',v_timesheet.timesheet_id,
    'effective_policy_sha256',v_effective_policy->>'policy_sha256'
  );
  v_invoice_breakdown:=pg_catalog.jsonb_build_object(
    'mode','SEGMENTS','segments','[]'::jsonb,
    'additional',pg_catalog.jsonb_build_object(
      'units','{}'::jsonb,'pay_ex_vat',0,'charge_ex_vat',0,'margin_ex_vat',0
    ),
    'totals',pg_catalog.jsonb_build_object(
      'total_pay_ex_vat',0,'total_charge_ex_vat',0,'margin_ex_vat',0
    )
  );
  v_tsfin:=pg_catalog.jsonb_build_object(
    'timesheet_id',v_timesheet.timesheet_id,
    'timesheet_version',v_timesheet.version,
    'basis',case when v_source_mode='NHSP_WEEKLY' then 'NHSP'
                 else 'HEALTHROSTER_SELF_BILL' end,
    'candidate_assignment','ASSIGNED','processing_status','PENDING_AUTH',
    'candidate_id',v_contract.candidate_id,'client_id',v_contract.client_id,
    'role',v_contract.role,'band',v_contract.band,
    'pay_method',v_contract.pay_method_snapshot,
    'policy_snapshot_json',v_settings_policy,
    'rate_source_refs_json',v_rate_refs,
    'invoice_breakdown_json',v_invoice_breakdown,
    'hours_day',0,'hours_night',0,'hours_sat',0,'hours_sun',0,'hours_bh',0,
    'total_hours',0,
    'pay_day',null,'pay_night',null,'pay_sat',null,'pay_sun',null,'pay_bh',null,
    'charge_day',null,'charge_night',null,'charge_sat',null,'charge_sun',null,
    'charge_bh',null,'total_pay_ex_vat',0,'total_charge_ex_vat',0,
    'margin_ex_vat',0,'additional_units_json','{}'::jsonb,
    'additional_pay_ex_vat',0,'additional_charge_ex_vat',0,
    'additional_margin_ex_vat',0,'expenses_pay_ex_vat',0,
    'expenses_charge_ex_vat',0,'expenses_description',null,
    'expenses_evidence_r2_key',null,'expenses_evidence_manifest','null'::jsonb,
    'mileage_units',0,'mileage_pay_ex_vat',0,'mileage_charge_ex_vat',0,
    'mileage_pay_rate',null,'mileage_charge_rate',null,
    'mileage_evidence_r2_key',null,'mileage_evidence_manifest','null'::jsonb
  );
  return pg_catalog.jsonb_build_object(
    'actor_user_id',v_actor,'idempotency_key',p_idempotency_key,
    'root_timesheet_id',v_timesheet.timesheet_id,
    'schema_version','WEEKLY_SOURCE_TARGET_MANAGED_ROOT_PREPARE_REQUEST_V1',
    'service_snapshot',pg_catalog.jsonb_build_object(
      'schema_version','WEEKLY_SOURCE_ORDINARY_TSFIN_SERVICE_SNAPSHOT_V1',
      'calculator_owner','buildWeeklyScheduleSegmentsSnapshot',
      'source_actual_schedule_json','[]'::jsonb,'tsfin_snapshot_json',v_tsfin
    ),
    'source_cycle_id',v_cycle.id,'target_family_id',v_family.id
  );
end;
$function$;

-- Build a standalone first-ever NHSP full-negative on its own Contract/week
-- root.  This is deliberately separate from the mixed positive/negative B1
-- report so the NO_OP_FIRST_NEGATIVE receipt path is proved directly.
insert into public.weekly_source_cycles(
  id,source_group_id,finalisation_week_ending,cutoff_at_utc,state,version,
  projection_state
) values (
  'c1000000-0000-4000-8000-000000000001',
  'b0000000-0000-4000-8000-000000000005','2026-10-04',
  '2026-09-04T14:00:00Z','OPEN',1,'REBUILDING'
);
insert into public.weekly_source_report_scopes(
  id,source_cycle_id,environment,agency_id,source_group_id,client_id,
  cutoff_at_utc,version,state,projection_state
) values (
  'c1000000-0000-4000-8000-000000000002',
  'c1000000-0000-4000-8000-000000000001','TEST',
  'a0000000-0000-4000-8000-000000000006',
  'b0000000-0000-4000-8000-000000000005',
  'b0000000-0000-4000-8000-000000000002',
  '2026-09-04T14:00:00Z',1,'OPEN','REBUILDING'
);
insert into public.weekly_source_uploads(
  id,source_cycle_id,report_scope_id,original_filename,content_sha256,byte_count,
  source_format_profile_id,parser_version,normaliser_version,
  workbook_part_and_sheet_fingerprint,header_coordinate_map_json,
  header_coordinate_map_hash,money_lexical_authority_version,
  declared_scope_fingerprint,coverage_proof_kind,physical_row_count,
  accepted_count,row_manifest_hash,state,uploaded_by_user_id,file_metadata_json
) values (
  'c1000000-0000-4000-8000-000000000003',
  'c1000000-0000-4000-8000-000000000001',
  'c1000000-0000-4000-8000-000000000002','BR-NEG-ONLY.xlsx',
  private.weekly_source_sha256_jsonb_v1(
    'ORDINARY_NHSP_NEG_CONTENT','{"report":"BR-NEG-ONLY"}'::jsonb
  ),100,'32222222-2222-4222-8222-222222222222',
  'ORDINARY_TEST_PARSER_V1','NHSP_BACKING_NORMALISER_V1',
  private.weekly_source_sha256_jsonb_v1(
    'ORDINARY_NHSP_NEG_WORKBOOK','{"report":"BR-NEG-ONLY"}'::jsonb
  ),'{}'::jsonb,
  private.weekly_source_sha256_jsonb_v1(
    'ORDINARY_NHSP_NEG_HEADERS','{"report":"BR-NEG-ONLY"}'::jsonb
  ),'XLSX_BINARY64_SAME_VALUE_PENCE_V1',
  private.weekly_source_sha256_jsonb_v1(
    'ORDINARY_NHSP_NEG_SCOPE','{"report":"BR-NEG-ONLY"}'::jsonb
  ),'NHSP_TRUST_REPORT_SCOPE',1,1,
  private.weekly_source_sha256_jsonb_v1(
    'ORDINARY_NHSP_NEG_ROWS','{"report":"BR-NEG-ONLY"}'::jsonb
  ),'CURRENT','a0000000-0000-4000-8000-000000000001',
  pg_catalog.jsonb_build_object(
    'nhsp_report_number','BR-NEG-ONLY',
    'nhsp_report_heading_name','Finaliser NHSP Trust'
  )
);
update public.weekly_source_report_scopes
set current_complete_upload_id='c1000000-0000-4000-8000-000000000003'
where id='c1000000-0000-4000-8000-000000000002';
insert into public.weekly_source_upload_rows(
  id,upload_id,source_row_ordinal,external_source_key,
  source_candidate_identity,source_client_identity,work_date,start_at_local,
  end_at_local,break_minutes,actual_net_minutes,row_finalisation_state,
  role_band_source,source_commission_pence,source_total_cost_pence,
  source_shift_charge_pence,source_money_parse_state,
  source_qualification_profile_version,source_expense_parse_state,
  normalised_row_hash
) values (
  'c1000000-0000-4000-8000-000000000004',
  'c1000000-0000-4000-8000-000000000003',1,
  'NHSP-FIRST-NEGATIVE-ONLY','Finaliser Candidate','Finaliser NHSP Trust',
  '2026-09-28','2026-09-28 09:00','2026-09-28 17:00',30,450,
  'SOURCE_WORKED','BAND 5',-500,-14500,-15000,'VALID',
  'NHSP_TWO_COMPONENT_PENCE_V1','NOT_APPLICABLE',
  private.weekly_source_sha256_jsonb_v1(
    'ORDINARY_NHSP_NEG_ROW','{"row":1}'::jsonb
  )
);
insert into public.weekly_source_projection_publications(
  id,source_cycle_id,authority_scope_kind,report_scope_id,upload_id,
  authority_scope_version,comparison_manifest_hash,issue_set_hash,state
) values (
  'c1000000-0000-4000-8000-000000000006',
  'c1000000-0000-4000-8000-000000000001','NHSP_REPORT_SCOPE',
  'c1000000-0000-4000-8000-000000000002',
  'c1000000-0000-4000-8000-000000000003',1,
  private.weekly_source_sha256_jsonb_v1(
    'ORDINARY_NHSP_NEG_COMPARISON','{"report":"BR-NEG-ONLY"}'::jsonb
  ),
  private.weekly_source_sha256_jsonb_v1(
    'ORDINARY_NHSP_NEG_ISSUES','{"report":"BR-NEG-ONLY"}'::jsonb
  ),'BUILDING'
);
select public.weekly_source_projection_rows_apply_atomic_v1(
  'a0000000-0000-4000-8000-000000000001',
  'c1000000-0000-4000-8000-000000000006',
  pg_catalog.jsonb_build_array(
    pg_catalog.jsonb_build_object(
      'upload_row_id','c1000000-0000-4000-8000-000000000004',
      'mapping_state','RESOLVED',
      'candidate_id','a0000000-0000-4000-8000-000000000003',
      'client_id','b0000000-0000-4000-8000-000000000002',
      'contract_id','b0000000-0000-4000-8000-000000000004',
      'contract_selection_method','AUTO_UNIQUE',
      'qualifying_contract_ids',
        pg_catalog.jsonb_build_array('b0000000-0000-4000-8000-000000000004'),
      -- WP-58. NHSP work identity is the schedule tuple, never the Reference
      -- Number (pack 24 sections 1 and 9). This is what the broker sends for an
      -- NHSP profile, and since WP-58 it is the only kind
      -- weekly_source_projection_rows_apply_atomic_v1 will accept for one.
      'identity_kind','SCHEDULE_TUPLE',
      'link_kind','FULL_NEGATIVE_SOURCE',
      'economic_snapshot',pg_catalog.jsonb_build_object(
        'schema_version','WEEKLY_SOURCE_CANONICAL_ECONOMIC_SNAPSHOT_V1',
        'calculator_version','WEEKLY_SHIFT_FINANCIAL_SEGMENT_V1',
        'source_mode','NHSP_WEEKLY','rate_method','SPLIT_RATE_WINDOWS','sign',-1,
        'paid_minutes',450,'break_minutes',30,
        'bucket_minutes',pg_catalog.jsonb_build_object(
          'day',450,'night',0,'sat',0,'sun',0,'bh',0
        ),
        'hours',pg_catalog.jsonb_build_object(
          'day',-7.5,'night',0,'sat',0,'sun',0,'bh',0
        ),
        'pay_rates',pg_catalog.jsonb_build_object(
          'day',10,'night',10,'sat',10,'sun',10,'bh',10
        ),
        'charge_rates',pg_catalog.jsonb_build_object(
          'day',20,'night',20,'sat',20,'sun',20,'bh',20
        ),
        'total_pay_pence','-7500','calculated_charge_pence','-15000'
      ),
      'charge_check',pg_catalog.jsonb_build_object(
        'row_sign_kind','FULL_NEGATIVE','source_commission_pence','-500',
        'source_total_cost_pence','-14500',
        'source_shift_charge_pence','-15000',
        'calculated_segment_charge_pence','-15000',
        'comparison_result','EXACT','comparison_reason_code','EXACT',
        'phase_severity','NONE'
      )
    )
  )
);
update public.weekly_source_projection_publications
set state='CURRENT',published_at_utc=pg_catalog.clock_timestamp()
where id='c1000000-0000-4000-8000-000000000006';
update public.weekly_source_report_scopes
set projection_state='CURRENT',
    current_projection_publication_id='c1000000-0000-4000-8000-000000000006'
where id='c1000000-0000-4000-8000-000000000002';
select public.weekly_source_finalise_atomic_v1(pg_catalog.jsonb_build_object(
  'actor_user_id','a0000000-0000-4000-8000-000000000001',
  'source_cycle_id','c1000000-0000-4000-8000-000000000001',
  'authority_scope_kind','NHSP_REPORT_SCOPE',
  'report_scope_id','c1000000-0000-4000-8000-000000000002',
  'upload_id','c1000000-0000-4000-8000-000000000003',
  'projection_publication_id','c1000000-0000-4000-8000-000000000006',
  'expected_authority_scope_version',1,
  'expected_row_manifest_hash',pg_catalog.encode((select row_manifest_hash
    from public.weekly_source_uploads
    where id='c1000000-0000-4000-8000-000000000003'),'hex'),
  'expected_comparison_manifest_hash',pg_catalog.encode((select comparison_manifest_hash
    from public.weekly_source_projection_publications
    where id='c1000000-0000-4000-8000-000000000006'),'hex'),
  'expected_issue_set_hash',pg_catalog.encode((select issue_set_hash
    from public.weekly_source_projection_publications
    where id='c1000000-0000-4000-8000-000000000006'),'hex')
));

create temp table ordinary_projection_boundary_before as
select
  (select pg_catalog.count(*) from public.weekly_source_billing_movements)
    as billing_movement_count,
  (select private.weekly_source_sha256_jsonb_v1(
     'WEEKLY_SOURCE_ORDINARY_PROJECTION_MOVEMENT_BOUNDARY_V1',
     coalesce(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(movement) order by movement.id),'[]'::jsonb)
   ) from public.weekly_source_billing_movements movement) as billing_movement_hash,
  (select pg_catalog.count(*) from public.weekly_source_expense_materialisations)
    as invoice_expense_facet_count,
  (select pg_catalog.count(*) from public.invoice_lines) as invoice_line_count,
  (select pg_catalog.count(*) from public.pay_batches) as pay_batch_count,
  (select pg_catalog.count(*) from public.pay_batch_items) as pay_batch_item_count,
  (select pg_catalog.count(*)
   from public.timesheets_financials financial
   where not exists(
     select 1 from public.weekly_source_row_timesheet_lineages lineage
     where lineage.timesheet_id=financial.timesheet_id
   ) and not exists(
     select 1 from public.weekly_exceptional_pay_target_families family
     where family.root_timesheet_id=financial.timesheet_id
   )) as ordinary_non_source_financial_count,
  (select private.weekly_source_sha256_jsonb_v1(
     'WEEKLY_SOURCE_ORDINARY_NON_SOURCE_TSFIN_BOUNDARY_V1',
     coalesce(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(financial) order by financial.id),'[]'::jsonb)
   )
   from public.timesheets_financials financial
   where not exists(
     select 1 from public.weekly_source_row_timesheet_lineages lineage
     where lineage.timesheet_id=financial.timesheet_id
   ) and not exists(
     select 1 from public.weekly_exceptional_pay_target_families family
     where family.root_timesheet_id=financial.timesheet_id
   )) as ordinary_non_source_financial_hash;

-- A later final revision must not be projected before its prior source event.
do $predecessor$
begin
  begin
    perform public.weekly_source_ordinary_pay_projection_apply_atomic_v1(
      pg_temp.ordinary_projection_request(
        'a3000000-0000-4000-8000-000000000001','projection-a3-before-a1'
      )
    );
    raise exception 'PROJECTION_PREDECESSOR_WAS_ACCEPTED';
  exception when object_not_in_prerequisite_state then
    if sqlerrm<>'WEEKLY_SOURCE_PROJECTION_PREDECESSOR_REQUIRED' then raise; end if;
  end;
end;
$predecessor$;

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
   where receipt.idempotency_key='projection-a1'),
  'ADD must publish source hours plus source-fixed expense through current TSFIN'
);
select pg_temp.assert_true(
  (select pg_catalog.count(*)=1
   from public.weekly_source_expense_pay_materialisations materialisation
   join public.weekly_source_ordinary_pay_projection_receipts receipt
     on receipt.published_timesheet_financial_id=
          materialisation.candidate_timesheet_financial_id
   where receipt.idempotency_key='projection-a1'),
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

select public.weekly_source_ordinary_pay_projection_apply_atomic_v1(
  pg_temp.ordinary_projection_request(
    'a3000000-0000-4000-8000-000000000001','projection-a3'
  )
) as ordinary_amend_result;

select pg_temp.assert_true(
  (select financial.total_hours=8.50
          and financial.total_pay_ex_vat=86.25
          and financial.total_charge_ex_vat=171.25
          and financial.expenses_pay_ex_vat=1.25
          and financial.processing_status='PENDING_AUTH'::public.ts_fin_processing_status_enum
   from public.timesheets_financials financial
   join public.weekly_source_ordinary_pay_projection_receipts receipt
     on receipt.published_timesheet_financial_id=financial.id
   where receipt.idempotency_key='projection-a3'),
  'AMEND must replace ordinary source hours and source-fixed expense exactly'
);
select pg_temp.assert_true(
  (select pg_catalog.count(*)=2
   from public.weekly_source_expense_pay_materialisations),
  'AMEND must retain immutable prior pay provenance and bind the new authority'
);

select public.weekly_source_ordinary_pay_projection_apply_atomic_v1(
  pg_temp.ordinary_projection_request(
    'a4000000-0000-4000-8000-000000000001','projection-a4'
  )
) as ordinary_cancel_result;

select pg_temp.assert_true(
  (select financial.total_hours=0
          and financial.total_pay_ex_vat=0
          and financial.total_charge_ex_vat=0
          and financial.expenses_pay_ex_vat=0
          and financial.expenses_charge_ex_vat=0
          and financial.processing_status='PENDING_AUTH'::public.ts_fin_processing_status_enum
   from public.timesheets_financials financial
   join public.weekly_source_ordinary_pay_projection_receipts receipt
     on receipt.published_timesheet_financial_id=financial.id
   where receipt.idempotency_key='projection-a4'),
  'CANCEL must publish an authorised zero-current ordinary source position'
);
select pg_temp.assert_true(
  not exists(
    select 1
    from public.weekly_source_expense_pay_materialisations materialisation
    join public.weekly_expense_authority_generations authority
      on authority.id=materialisation.expense_authority_generation_id
    where authority.state='CURRENT'
  ),
  'CANCEL must leave no pay materialisation for the current zero authority'
);

-- BR-001 carries a +7.50 h physical positive and a physical full negative for
-- the SAME Candidate, Client, date and worked interval.  Under 24 s9 those are
-- ONE work event, so the negative is the reversal of that very shift and the
-- current source position is empty: 0.00 h, and the Candidate is paid nothing
-- for a shift the Trust took back in the same report.
--
-- This assertion used to expect 7.50 h / GBP 75.00 and one unit flagged
-- `first_plan6_negative`.  That was not a statement about pay at all - it was
-- an artefact of the fixture sending `identity_kind='PROFILE_EXTERNAL_KEY'`,
-- which split one corrected shift across two work events (the shape 24 s9
-- forbids and WP-50 finding F7 recorded).  With the fixture corrected to the
-- SCHEDULE_TUPLE identity the broker actually sends, the negative meets the
-- positive it reverses and the old numbers are no longer true of this data.
-- WP-52 adopted the new truth here and restored the coverage the old numbers
-- were standing in for - a genuine positive surviving a historical reversal -
-- as an explicit mixed-root case in the NHSP row-order regression at the end
-- of this file, where the two shifts are actually different shifts.
select public.weekly_source_ordinary_pay_projection_apply_atomic_v1(
  pg_temp.ordinary_projection_request(
    'b1000000-0000-4000-8000-000000000001','projection-nhsp-b1'
  )
) as nhsp_first_negative_projection_result;
select pg_temp.assert_true(
  (select pg_catalog.count(distinct work_event_id)=1
          and pg_catalog.count(*)=2
   from public.weekly_source_billing_movements
   where finalisation_cycle_id='b1000000-0000-4000-8000-000000000001'),
  'BR-001 must be one work event carrying both physical movements'
);
select pg_temp.assert_true(
  (select receipt.outcome='PREPARED_FOR_AUTHORISATION'
          and receipt.source_unit_count=2
          and (select pg_catalog.count(*)=0
               from pg_catalog.jsonb_array_elements(
                 receipt.source_unit_outcomes_json
               ) unit(value)
               where coalesce((unit.value->>'first_plan6_negative')::boolean,false))
          and financial.total_hours=0.00
          and financial.total_pay_ex_vat=0.00
          and financial.total_charge_ex_vat=0.00
          and pg_catalog.jsonb_array_length(
                financial.invoice_breakdown_json->'segments'
              )=0
   from public.weekly_source_ordinary_pay_projection_receipts receipt
   join public.timesheets_financials financial
     on financial.id=receipt.published_timesheet_financial_id
   where receipt.idempotency_key='projection-nhsp-b1'),
  'a full negative for the same shift in the same report must leave no current source position'
);

select public.weekly_source_ordinary_pay_projection_apply_atomic_v1(
  pg_temp.ordinary_projection_request(
    'c1000000-0000-4000-8000-000000000001','projection-nhsp-negative-only-c1'
  )
) as nhsp_negative_only_projection_result;
select pg_temp.assert_true(
  (select receipt.outcome='NO_OP_FIRST_NEGATIVE'
          and receipt.source_unit_count=1
          and receipt.published_timesheet_financial_id is null
          and receipt.root_before_hash=receipt.root_after_hash
          and pg_catalog.jsonb_array_length(receipt.source_unit_outcomes_json)=1
          and coalesce((receipt.source_unit_outcomes_json->0->>
                        'first_plan6_negative')::boolean,false)
          and not exists(
            select 1 from public.timesheets_financials financial
            where financial.timesheet_id=receipt.root_timesheet_id
          )
   from public.weekly_source_ordinary_pay_projection_receipts receipt
   where receipt.idempotency_key='projection-nhsp-negative-only-c1'),
  'a standalone first-ever NHSP negative must record a zero-write no-op receipt'
);
select pg_temp.assert_true(
  (public.weekly_source_ordinary_pay_projection_apply_atomic_v1(
    pg_temp.ordinary_projection_request(
      'c1000000-0000-4000-8000-000000000001',
      'projection-nhsp-negative-only-c1'
    )
  )->>'idempotent_replay')::boolean,
  'the standalone first-negative no-op must replay its immutable receipt'
);

-- The finalisation verifier has already frozen a four-cycle expense-only
-- lineage on one ordinary Weekly HOURS root: 125p, explicit zero, 200p and a
-- complete-coverage omission.  Project those authorities in source order.
select public.weekly_source_ordinary_pay_projection_apply_atomic_v1(
  pg_temp.ordinary_projection_request(
    'e1000000-0000-4000-8000-000000000001','projection-expense-only-e1'
  )
) as expense_only_projection_result;

select pg_temp.assert_true(
  (select receipt.source_unit_count=0
          and pg_catalog.jsonb_array_length(receipt.source_unit_outcomes_json)=0
          and pg_catalog.jsonb_array_length(receipt.source_expense_authorities_json)=1
          and financial.total_hours=0
          and financial.expenses_pay_ex_vat=1.25
          and financial.expenses_charge_ex_vat=1.25
          and financial.expenses_evidence_r2_key is null
          and financial.mileage_units=0
          and financial.mileage_pay_ex_vat=0
          and financial.mileage_charge_ex_vat=0
          and financial.mileage_evidence_r2_key is null
          and financial.total_pay_ex_vat=1.25
          and financial.total_charge_ex_vat=1.25
          and financial.processing_status=
            'PENDING_AUTH'::public.ts_fin_processing_status_enum
          and financial.invoice_breakdown_json->'segments'='[]'::jsonb
   from public.weekly_source_ordinary_pay_projection_receipts receipt
   join public.timesheets_financials financial
     on financial.id=receipt.published_timesheet_financial_id
   where receipt.idempotency_key='projection-expense-only-e1'),
  'expense-only source authority must publish zero hours plus the exact expense'
);
select pg_temp.assert_true(
  (select pg_catalog.count(*)=1
   from public.weekly_source_expense_pay_materialisations pay_facet
   join public.weekly_source_ordinary_pay_projection_receipts receipt
     on receipt.published_timesheet_financial_id=
          pay_facet.candidate_timesheet_financial_id
    and receipt.root_timesheet_id=pay_facet.root_timesheet_id
   where receipt.idempotency_key='projection-expense-only-e1')
  and not exists(
    select 1
    from public.weekly_source_state_transitions transition_row
    join public.weekly_source_ordinary_pay_projection_receipts receipt
      on receipt.final_revision_id=transition_row.final_revision_id
    where receipt.idempotency_key='projection-expense-only-e1'
  ),
  'expense-only pay provenance must use the same root without a worked transition'
);
select pg_temp.assert_true(
  (select (movement.vat_rate_pct=case
             when :weekly_source_verification_expense_vat_enabled then 20 else 0 end)
          and (movement.vat_amount=case
             when :weekly_source_verification_expense_vat_enabled then 0.25 else 0 end)
          and (movement.total_inc_vat=case
             when :weekly_source_verification_expense_vat_enabled then 1.50 else 1.25 end)
   from public.weekly_source_billing_movements movement
   join public.weekly_source_ordinary_pay_projection_receipts receipt
     on receipt.final_revision_id=movement.final_revision_id
    and receipt.root_timesheet_id=movement.invoice_timesheet_id
   where receipt.idempotency_key='projection-expense-only-e1'
     and movement.movement_role='EXPENSE_POSITIVE'),
  'expense-only invoice VAT must follow the sealed source-expense policy'
);
select pg_temp.assert_true(
  (public.weekly_source_ordinary_pay_projection_apply_atomic_v1(
    pg_temp.ordinary_projection_request(
      'e1000000-0000-4000-8000-000000000001','projection-expense-only-e1'
    )
  )->>'idempotent_replay')::boolean
  and (select pg_catalog.count(*)=1
       from public.weekly_source_expense_pay_materialisations pay_facet
       join public.weekly_source_ordinary_pay_projection_receipts receipt
         on receipt.published_timesheet_financial_id=
              pay_facet.candidate_timesheet_financial_id
       where receipt.idempotency_key='projection-expense-only-e1'),
  'expense-only replay must not duplicate the immutable pay facet'
);

-- An explicit source zero clears the expense on the same ordinary root.
select public.weekly_source_ordinary_pay_projection_apply_atomic_v1(
  pg_temp.ordinary_projection_request(
    'e2000000-0000-4000-8000-000000000001','projection-expense-only-e2-zero'
  )
) as expense_only_zero_projection_result;
select pg_temp.assert_true(
  (select receipt.root_timesheet_id=prior_receipt.root_timesheet_id
          and receipt.source_unit_count=0
          and financial.total_hours=0
          and financial.expenses_pay_ex_vat=0
          and financial.expenses_charge_ex_vat=0
          and financial.total_pay_ex_vat=0
          and financial.total_charge_ex_vat=0
          and financial.processing_status=
            'PENDING_AUTH'::public.ts_fin_processing_status_enum
   from public.weekly_source_ordinary_pay_projection_receipts receipt
   join public.weekly_source_ordinary_pay_projection_receipts prior_receipt
     on prior_receipt.idempotency_key='projection-expense-only-e1'
   join public.timesheets_financials financial
     on financial.id=receipt.published_timesheet_financial_id
   where receipt.idempotency_key='projection-expense-only-e2-zero')
  and (select pg_catalog.count(*)=1
       from public.weekly_source_expense_pay_materialisations pay_facet
       join public.weekly_source_ordinary_pay_projection_receipts receipt
         on receipt.root_timesheet_id=pay_facet.root_timesheet_id
       where receipt.idempotency_key='projection-expense-only-e2-zero'),
  'later omission must zero the same root without a second expense pay facet'
);

-- A later source expense can reappear without worked time, and subsequent
-- complete-coverage omission clears it while preserving both positive pay
-- provenance rows and every historical TSFIN version.
select public.weekly_source_ordinary_pay_projection_apply_atomic_v1(
  pg_temp.ordinary_projection_request(
    'e3000000-0000-4000-8000-000000000001','projection-expense-only-e3'
  )
) as expense_only_reappearance_projection_result;
select pg_temp.assert_true(
  (select receipt.root_timesheet_id=first_receipt.root_timesheet_id
          and receipt.source_unit_count=0
          and financial.total_hours=0
          and financial.expenses_pay_ex_vat=2.00
          and financial.expenses_charge_ex_vat=2.00
          and financial.total_pay_ex_vat=2.00
          and financial.total_charge_ex_vat=2.00
          and financial.invoice_breakdown_json->'segments'='[]'::jsonb
   from public.weekly_source_ordinary_pay_projection_receipts receipt
   join public.weekly_source_ordinary_pay_projection_receipts first_receipt
     on first_receipt.idempotency_key='projection-expense-only-e1'
   join public.timesheets_financials financial
     on financial.id=receipt.published_timesheet_financial_id
   where receipt.idempotency_key='projection-expense-only-e3')
  and (select pg_catalog.count(*)=2
       from public.weekly_source_expense_pay_materialisations pay_facet
       join public.weekly_source_ordinary_pay_projection_receipts receipt
         on receipt.root_timesheet_id=pay_facet.root_timesheet_id
       where receipt.idempotency_key='projection-expense-only-e3'),
  'expense-only reappearance must publish the exact amount on the same root'
);

select public.weekly_source_ordinary_pay_projection_apply_atomic_v1(
  pg_temp.ordinary_projection_request(
    'e4000000-0000-4000-8000-000000000001','projection-expense-only-e4-omission'
  )
) as expense_only_omission_projection_result;
select pg_temp.assert_true(
  (select receipt.root_timesheet_id=first_receipt.root_timesheet_id
          and receipt.source_unit_count=0
          and financial.total_hours=0
          and financial.expenses_pay_ex_vat=0
          and financial.expenses_charge_ex_vat=0
          and financial.total_pay_ex_vat=0
          and financial.total_charge_ex_vat=0
          and financial.invoice_breakdown_json->'segments'='[]'::jsonb
   from public.weekly_source_ordinary_pay_projection_receipts receipt
   join public.weekly_source_ordinary_pay_projection_receipts first_receipt
     on first_receipt.idempotency_key='projection-expense-only-e1'
   join public.timesheets_financials financial
     on financial.id=receipt.published_timesheet_financial_id
   where receipt.idempotency_key='projection-expense-only-e4-omission')
  and (select pg_catalog.count(*)=2
       from public.weekly_source_expense_pay_materialisations pay_facet
       join public.weekly_source_ordinary_pay_projection_receipts receipt
         on receipt.root_timesheet_id=pay_facet.root_timesheet_id
       where receipt.idempotency_key='projection-expense-only-e4-omission'),
  'complete-coverage omission must zero the same root without a new pay facet'
);

-- A service-built snapshot is data, not authority.  Any changed total must be
-- rejected before a receipt or Timesheet mutation is written.
do $tampered_service_snapshot$
declare
  v_request jsonb;
  v_root uuid;
  v_before_hash bytea;
  v_before_financial_count bigint;
begin
  v_request:=pg_temp.ordinary_projection_request(
    'd2000000-0000-4000-8000-000000000001','projection-d2-tampered'
  );
  v_root:=(v_request->>'root_timesheet_id')::uuid;
  v_before_hash:=private.weekly_source_ordinary_projection_root_hash_v1(v_root);
  select pg_catalog.count(*) into v_before_financial_count
  from public.timesheets_financials where timesheet_id=v_root;
  begin
    perform public.weekly_source_ordinary_pay_projection_apply_atomic_v1(
      pg_catalog.jsonb_set(
        v_request,'{service_snapshot,tsfin_snapshot_json,total_pay_ex_vat}',
        pg_catalog.to_jsonb(999.99::numeric),false
      )
    );
    raise exception 'TAMPERED_SERVICE_SNAPSHOT_WAS_ACCEPTED';
  exception when invalid_parameter_value then
    if sqlerrm<>'WEEKLY_SOURCE_TSFIN_TOTALS_MISMATCH' then raise; end if;
  end;
  if private.weekly_source_ordinary_projection_root_hash_v1(v_root)
       is distinct from v_before_hash
     or (select pg_catalog.count(*) from public.timesheets_financials
         where timesheet_id=v_root)<>v_before_financial_count
     or exists(
       select 1 from public.weekly_source_ordinary_pay_projection_receipts
       where idempotency_key='projection-d2-tampered'
     ) then
    raise exception 'TAMPERED_SERVICE_SNAPSHOT_LEFT_RESIDUE';
  end if;
end;
$tampered_service_snapshot$;

-- Once Office protects an otherwise ordinary source root, the source owner
-- records the frozen source receipt and advances only its projection state. It
-- must not publish or replace candidate pay on that TARGET_MANAGED root.
update public.tms_users
set payment_authoriser=true
where id='a0000000-0000-4000-8000-000000000001';

create temp table d2_target_family_result as
select public.weekly_exceptional_pay_prepare_family_v1(
  pg_catalog.jsonb_build_object(
    'actor_user_id','a0000000-0000-4000-8000-000000000001',
    'source_cycle_id','d2000000-0000-4000-8000-000000000001',
    'candidate_id',movement.candidate_id,'client_id',movement.actual_client_id,
    'contract_id',movement.contract_id,'week_ending_date',root.week_ending_date,
    'work_event_id',movement.work_event_id,'work_date',event_row.work_date,
    'start_at_local',source_row.start_at_local,'end_at_local',source_row.end_at_local,
    'break_minutes',source_row.break_minutes,
    'reason','Verifier target-managed suppression.',
    'idempotency_key','projection-d2-family-0001'
  )
) as result
from public.weekly_source_billing_movements movement
join public.timesheets root on root.timesheet_id=movement.invoice_timesheet_id
join public.weekly_work_events event_row on event_row.id=movement.work_event_id
join public.weekly_source_row_resolutions resolution
  on resolution.id=(movement.source_facts_json->>'row_resolution_id')::uuid
join public.weekly_source_upload_rows source_row on source_row.id=resolution.upload_row_id
where movement.finalisation_cycle_id='d2000000-0000-4000-8000-000000000001'
  and movement.movement_role='POSITIVE'
limit 1;
create temp table d2_target_before as
select
  (result->>'root_timesheet_id')::uuid as root_timesheet_id,
  private.weekly_source_ordinary_projection_root_hash_v1(
    (result->>'root_timesheet_id')::uuid
  ) as root_hash,
  (select pg_catalog.count(*) from public.timesheets_financials financial
   where financial.timesheet_id=(result->>'root_timesheet_id')::uuid)
    as financial_count,
  (select pg_catalog.count(*) from public.weekly_source_expense_pay_materialisations)
    as pay_facet_count
from d2_target_family_result;
create temp table d2_target_projection_result as
select public.weekly_source_ordinary_pay_projection_apply_atomic_v1(
  pg_temp.ordinary_projection_request(
    'd2000000-0000-4000-8000-000000000001','projection-d2-target-managed'
  )
) as result;
select pg_temp.assert_true(
  (select result->>'outcome'='TARGET_MANAGED_SUPPRESSED'
          and (result->>'ok')::boolean
          and result->>'timesheet_financials_id' is null
   from d2_target_projection_result)
  and (select private.weekly_source_ordinary_projection_root_hash_v1(root_timesheet_id)
                =root_hash
              and (select pg_catalog.count(*) from public.timesheets_financials financial
                   where financial.timesheet_id=d2_target_before.root_timesheet_id)
                    =financial_count
              and (select pg_catalog.count(*)
                   from public.weekly_source_expense_pay_materialisations)=pay_facet_count
       from d2_target_before)
  and exists(
    select 1 from public.weekly_source_state_transitions transition_row
    join public.weekly_source_final_revisions revision
      on revision.id=transition_row.final_revision_id
    where revision.source_cycle_id='d2000000-0000-4000-8000-000000000001'
      and transition_row.ordinary_source_entitlement_projection_state='PUBLISHED'
  ),
  'TARGET_MANAGED source must suppress ordinary pay without changing the root'
);
select pg_temp.assert_true(
  (public.weekly_source_ordinary_pay_projection_apply_atomic_v1(
    pg_temp.ordinary_projection_request(
      'd2000000-0000-4000-8000-000000000001','projection-d2-target-managed'
    )
  )->>'idempotent_replay')::boolean,
  'TARGET_MANAGED suppression must replay the immutable receipt exactly'
);
do $target_managed_collision$
declare v_request jsonb;
begin
  v_request:=pg_temp.ordinary_projection_request(
    'd2000000-0000-4000-8000-000000000001','projection-d2-target-managed'
  );
  begin
    perform public.weekly_source_ordinary_pay_projection_apply_atomic_v1(
      pg_catalog.jsonb_set(
        v_request,'{service_snapshot,tsfin_snapshot_json,total_pay_ex_vat}',
        pg_catalog.to_jsonb(1::numeric),false
      )
    );
    raise exception 'PROJECTION_IDEMPOTENCY_COLLISION_WAS_ACCEPTED';
  exception when invalid_parameter_value then
    if sqlerrm<>'WEEKLY_SOURCE_PROJECTION_IDEMPOTENCY_COLLISION' then raise; end if;
  end;
end;
$target_managed_collision$;

-- Paid roots remain on the already-supported correction/rollover route.  The
-- new owner writes only an immutable refusal receipt and never changes the
-- paid Timesheet, TSFIN or frozen source movement.
update public.timesheets_financials financial
set paid_at_utc=pg_catalog.statement_timestamp()
from public.weekly_source_ordinary_pay_projection_receipts receipt
where receipt.idempotency_key='projection-a4'
  and receipt.published_timesheet_financial_id=financial.id;
create temp table paid_root_before as
select
  receipt.root_timesheet_id,
  private.weekly_source_ordinary_projection_root_hash_v1(receipt.root_timesheet_id)
    as root_hash,
  (select pg_catalog.count(*) from public.timesheets_financials financial
   where financial.timesheet_id=receipt.root_timesheet_id) as financial_count,
  (select pg_catalog.count(*) from public.weekly_source_billing_movements) as movement_count
from public.weekly_source_ordinary_pay_projection_receipts receipt
where receipt.idempotency_key='projection-a4';
create temp table paid_root_refusal as
select public.weekly_source_ordinary_pay_projection_apply_atomic_v1(
  pg_temp.ordinary_projection_request(
    'a6000000-0000-4000-8000-000000000001','projection-a6-paid-refusal'
  )
) as result;
-- =========================================================================
-- Gate 2's central negative test (XSG-002, 25 section 1 "Removed").
--
-- This is the exact case the superseded owner handled by un-approving the
-- Timesheet, overwriting the hours the worker submitted, rewriting the current
-- financial snapshot and re-approving it -- and, when it could not, by writing
-- a REFUSED_LOCKED dead end that nothing consumed.
--
-- After Gate 2 a paid root is neither mutated nor refused.  It gets a complete
-- PROPOSED entitlement and the Office decides.  The proof is that the receipt
-- says PROPOSED and that the root hash, the financial-row count and the
-- movement count are all byte-for-byte what they were before the call.
-- =========================================================================
select pg_temp.assert_true(
  (select (result->>'ok')::boolean
          and result->>'outcome'='PROPOSED'
          and result->>'error_code' is null
          and result->>'required_path'='PAID_UNINVOICED_ROLLOVER'
          and result->>'timesheet_financials_id' is null
          and pg_catalog.jsonb_typeof(result->'proposal')='object'
          and (result#>>'{proposal,ok}')::boolean
          and (result#>>'{proposal,state}')='PROPOSED'
   from paid_root_refusal)
  and (select private.weekly_source_ordinary_projection_root_hash_v1(root_timesheet_id)
                =root_hash
              and (select pg_catalog.count(*) from public.timesheets_financials financial
                   where financial.timesheet_id=paid_root_before.root_timesheet_id)
                    =financial_count
              and (select pg_catalog.count(*) from public.weekly_source_billing_movements)
                    =movement_count
       from paid_root_before),
  'paid source root must receive a complete PROPOSED entitlement with no economic mutation'
);
-- The proposal is a real, complete, decidable entitlement, not a placeholder:
-- exactly one PROPOSED decision bundle revision exists for it, it carries the
-- pre-allocated head id the request declared, and no head, receipt, pointer,
-- invalidation or dirty job was created.
select pg_temp.assert_true(
  (select pg_catalog.count(*)=1
   from public.weekly_source_entitlement_decision_bundles bundle_row
   join paid_root_refusal
     on bundle_row.decision_bundle_id=(paid_root_refusal.result#>>'{proposal,decision_bundle_id}')::uuid
   where bundle_row.state='PROPOSED'
     and bundle_row.bundle_kind='SINGLE_ROOT'
     and pg_catalog.cardinality(bundle_row.proposed_head_ids)=1)
  and (select pg_catalog.count(*)=0 from public.weekly_source_entitlement_heads)
  and (select pg_catalog.count(*)=0
       from private.weekly_source_entitlement_publication_receipts),
  'a proposal creates one PROPOSED bundle revision and no head, and publishes nothing'
);
select pg_temp.assert_true(
  (public.weekly_source_ordinary_pay_projection_apply_atomic_v1(
    pg_temp.ordinary_projection_request(
      'a6000000-0000-4000-8000-000000000001','projection-a6-paid-refusal'
    )
  )->>'idempotent_replay')::boolean
  and (select pg_catalog.count(*)=1
       from public.weekly_source_ordinary_pay_projection_receipts receipt
       where receipt.idempotency_key='projection-a6-paid-refusal'),
  'paid-root refusal must replay without duplicate receipts or mutations'
);

-- Source-absent protected roots are prepared by the protected-family owner;
-- this function only adds the legitimate authorised zero current TSFIN needed
-- before C1 publication.  Prove both configurable Roster and NHSP source modes.
create temp table roster_zero_family as
select public.weekly_exceptional_pay_prepare_family_v1(
  pg_catalog.jsonb_build_object(
    'actor_user_id','a0000000-0000-4000-8000-000000000001',
    'source_cycle_id','a1000000-0000-4000-8000-000000000001',
    'candidate_id','a0000000-0000-4000-8000-000000000003',
    'client_id','a0000000-0000-4000-8000-000000000002',
    'contract_id','a0000000-0000-4000-8000-000000000004',
    'week_ending_date','2026-05-10','work_date','2026-05-04',
    'start_at_local','2026-05-04 09:00:00','end_at_local','2026-05-04 17:00:00',
    'break_minutes',30,'reason','Verifier Roster source-absent protection.',
    'idempotency_key','projection-roster-zero-family-0001'
  )
) as result;
create temp table roster_zero_request as
select pg_temp.target_zero_request(
  result,'projection-roster-zero-root-0001'
) as request from roster_zero_family;
-- Gate 2 / G2-2: capture the PUBLIC root before the preparer runs, so the
-- assertion below can prove the Candidate's submitted schedule evidence was not
-- zeroed (25 section 1 "Removed"; 24 section 2).  The superseded owner wrote
-- actual_schedule_json='[]' here.
create temp table roster_zero_root_before as
select root.timesheet_id,
       root.actual_schedule_json,
       root.version,
       private.weekly_source_ordinary_projection_root_hash_v1(root.timesheet_id) as root_hash
from roster_zero_family family
join public.timesheets root
  on root.timesheet_id=(family.result->>'root_timesheet_id')::uuid;
create temp table roster_zero_result as
select public.weekly_source_target_managed_root_prepare_atomic_v1(request) as result
from roster_zero_request;
select pg_temp.assert_true(
  (select (result->>'ok')::boolean and result->>'outcome'='PREPARED'
          and result->>'source_mode'='HEALTHROSTER_WEEKLY'
          and result->>'source_profile_domain'='ROSTER_FINAL_AUTHORITY'
   from roster_zero_result)
  and (select financial.total_hours=0 and financial.total_pay_ex_vat=0
              and financial.total_charge_ex_vat=0 and financial.is_current
              -- Gate 2 / G2-2: the preparer no longer authorises, and it no
              -- longer zeroes the PUBLIC schedule, which is Candidate evidence
              -- (24 section 2).  Certified zero is an explicit head instead.
              and financial.authorised_at_utc is null
              and root.actual_schedule_json
                  is not distinct from before_root.actual_schedule_json
              and root.version=before_root.version
       from roster_zero_result prepared
       join public.timesheets_financials financial
         on financial.id=(prepared.result->>'timesheet_financials_id')::uuid
       join public.timesheets root on root.timesheet_id=financial.timesheet_id
       join roster_zero_root_before before_root
         on before_root.timesheet_id=root.timesheet_id)
  and (select pg_catalog.count(*)=0
       from public.weekly_source_billing_movements movement
       join roster_zero_result prepared
         on movement.invoice_timesheet_id=(prepared.result->>'root_timesheet_id')::uuid)
  and (select pg_catalog.count(*)=0
       from public.invoice_lines invoice_line
       join roster_zero_result prepared
         on invoice_line.timesheet_id=(prepared.result->>'root_timesheet_id')::uuid),
  'Roster protected root must receive one UNAUTHORISED zero TSFIN, keep its public schedule and take no invoice movement'
);
select pg_temp.assert_true(
  (select (public.weekly_source_target_managed_root_prepare_atomic_v1(request)
              ->>'idempotent_replay')::boolean from roster_zero_request),
  'Roster protected zero-root preparation must replay exactly'
);

create temp table nhsp_zero_family as
select public.weekly_exceptional_pay_prepare_family_v1(
  pg_catalog.jsonb_build_object(
    'actor_user_id','a0000000-0000-4000-8000-000000000001',
    'source_cycle_id','b1000000-0000-4000-8000-000000000001',
    'candidate_id','a0000000-0000-4000-8000-000000000003',
    'client_id','b0000000-0000-4000-8000-000000000002',
    'contract_id','b0000000-0000-4000-8000-000000000004',
    'week_ending_date','2026-05-17','work_date','2026-05-11',
    'start_at_local','2026-05-11 09:00:00','end_at_local','2026-05-11 17:00:00',
    'break_minutes',30,'reason','Verifier NHSP source-absent protection.',
    'idempotency_key','projection-nhsp-zero-family-0001'
  )
) as result;
create temp table nhsp_zero_request as
select pg_temp.target_zero_request(
  result,'projection-nhsp-zero-root-0001'
) as request from nhsp_zero_family;
create temp table nhsp_zero_result as
select public.weekly_source_target_managed_root_prepare_atomic_v1(request) as result
from nhsp_zero_request;
select pg_temp.assert_true(
  (select (result->>'ok')::boolean and result->>'outcome'='PREPARED'
          and result->>'source_mode'='NHSP_WEEKLY'
          and result->>'source_profile_domain'='NHSP_TRUST_BACKING_REPORT'
   from nhsp_zero_result)
  and (select financial.total_hours=0 and financial.total_pay_ex_vat=0
              and financial.total_charge_ex_vat=0 and financial.is_current
              -- Gate 2 / G2-0: prepared, never authorised here.
              and financial.authorised_at_utc is null
       from nhsp_zero_result prepared
       join public.timesheets_financials financial
         on financial.id=(prepared.result->>'timesheet_financials_id')::uuid)
  and (select pg_catalog.count(*)=0
       from public.weekly_source_billing_movements movement
       join nhsp_zero_result prepared
         on movement.invoice_timesheet_id=(prepared.result->>'root_timesheet_id')::uuid),
  'NHSP protected root must resolve NHSP mode and publish only zero ordinary truth'
);

-- Public service owners are not browser RPCs and private helpers are never API
-- surfaces, even though each owner is SECURITY DEFINER.
select pg_temp.assert_true(
  pg_catalog.has_function_privilege(
    'service_role','public.weekly_source_ordinary_pay_projection_apply_atomic_v1(jsonb)',
    'EXECUTE'
  )
  and pg_catalog.has_function_privilege(
    'service_role','public.weekly_source_target_managed_root_prepare_atomic_v1(jsonb)',
    'EXECUTE'
  )
  and not pg_catalog.has_function_privilege(
    'anon','public.weekly_source_ordinary_pay_projection_apply_atomic_v1(jsonb)','EXECUTE'
  )
  and not pg_catalog.has_function_privilege(
    'authenticated','public.weekly_source_ordinary_pay_projection_apply_atomic_v1(jsonb)',
    'EXECUTE'
  )
  and not pg_catalog.has_function_privilege(
    'anon','public.weekly_source_target_managed_root_prepare_atomic_v1(jsonb)','EXECUTE'
  )
  and not pg_catalog.has_function_privilege(
    'authenticated','public.weekly_source_target_managed_root_prepare_atomic_v1(jsonb)',
    'EXECUTE'
  )
  and not pg_catalog.has_function_privilege(
    'service_role','private.weekly_source_ordinary_projection_snapshot_assert_v1(uuid,text,jsonb,jsonb,jsonb,jsonb,jsonb)',
    'EXECUTE'
  ),
  'ordinary projection RPC grants must be service-only with private helpers closed'
);

-- Projection changes only ordinary Timesheet/TSFIN truth, immutable projection
-- receipts, pay-facet provenance and its one lifecycle state.  It never writes
-- source invoice movements, invoice-owned expense facets, invoices or pay runs.
select pg_temp.assert_true(
  (select billing_movement_count=(
            select pg_catalog.count(*) from public.weekly_source_billing_movements
          )
          and billing_movement_hash=(
            select private.weekly_source_sha256_jsonb_v1(
              'WEEKLY_SOURCE_ORDINARY_PROJECTION_MOVEMENT_BOUNDARY_V1',
              coalesce(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(movement) order by movement.id),'[]'::jsonb)
            ) from public.weekly_source_billing_movements movement
          )
          and invoice_expense_facet_count=(
            select pg_catalog.count(*) from public.weekly_source_expense_materialisations
          )
          and invoice_line_count=(select pg_catalog.count(*) from public.invoice_lines)
          and pay_batch_count=(select pg_catalog.count(*) from public.pay_batches)
          and pay_batch_item_count=(select pg_catalog.count(*) from public.pay_batch_items)
   from ordinary_projection_boundary_before),
  'ordinary projection must leave invoice and Banking Pay-owned tables untouched'
);

-- Source-supplied expenses never enter the candidate/Office receipt or
-- mileage path.  Equally, every financial row outside Weekly Source and the
-- protected-pay family remains byte-for-byte unchanged, preserving the
-- existing receipt/mileage workflow for ordinary expenses.
select pg_temp.assert_true(
  (select ordinary_non_source_financial_count=(
            select pg_catalog.count(*)
            from public.timesheets_financials financial
            where not exists(
              select 1 from public.weekly_source_row_timesheet_lineages lineage
              where lineage.timesheet_id=financial.timesheet_id
            ) and not exists(
              select 1 from public.weekly_exceptional_pay_target_families family
              where family.root_timesheet_id=financial.timesheet_id
            )
          )
          and ordinary_non_source_financial_hash=(
            select private.weekly_source_sha256_jsonb_v1(
              'WEEKLY_SOURCE_ORDINARY_NON_SOURCE_TSFIN_BOUNDARY_V1',
              coalesce(
                pg_catalog.jsonb_agg(pg_catalog.to_jsonb(financial) order by financial.id),
                '[]'::jsonb
              )
            )
            from public.timesheets_financials financial
            where not exists(
              select 1 from public.weekly_source_row_timesheet_lineages lineage
              where lineage.timesheet_id=financial.timesheet_id
            ) and not exists(
              select 1 from public.weekly_exceptional_pay_target_families family
              where family.root_timesheet_id=financial.timesheet_id
            )
          )
   from ordinary_projection_boundary_before),
  'ordinary receipt and mileage Timesheet financials must remain byte-for-byte unchanged'
);

select pg_temp.assert_true(
  not exists(
    select 1 from public.weekly_source_state_transitions transition_row
    where transition_row.final_revision_id in (
      select receipt.final_revision_id
      from public.weekly_source_ordinary_pay_projection_receipts receipt
      where receipt.idempotency_key in ('projection-a1','projection-a3','projection-a4')
    ) and transition_row.ordinary_source_entitlement_projection_state<>'PUBLISHED'
  ),
  'successful ordinary projection must publish only its lifecycle state column'
);

-- =========================================================================
-- WP-02b: the WP-06c independent review's findings F2, F3, F4 and F5 against
-- the generalised proposal recorder, the cross-Contract builder's identity
-- guard and interface I-7's singleton read.  Everything here runs inside the
-- outer rollback-only transaction; the one deliberate local patch (dropping the
-- partial unique indexes for F5) is taken and released inside a savepoint and
-- is labelled at the assertion it serves.
-- =========================================================================
do $wp02b_review_findings$
declare
  v_root uuid;
  v_revision uuid;
  v_agency uuid;
  v_candidate uuid;
  v_actor constant uuid:='a0000000-0000-4000-8000-000000000001';
  v_timesheet public.timesheets%rowtype;
  v_contract public.contracts%rowtype;
  v_request jsonb;
  v_result jsonb;
  v_digest bytea;
  v_head_count integer;
  v_effective jsonb;
  v_two_member jsonb;
  v_code text;
begin
  -- ---- F3: the derived-identifier helper on an absent input --------------
  -- It used to `coalesce(p_key,'')`, so EVERY component of a request whose
  -- bundle identity was null derived the SAME movement identity.  A derived
  -- money identity with no input is not an identity.
  begin
    perform private.weekly_source_entitlement_derived_uuid_v1(
      'WEEKLY_SOURCE_ENTITLEMENT_MOVEMENT_V1',null);
    raise exception 'WP02B_F3_NULL_KEY_WAS_ACCEPTED';
  exception when invalid_parameter_value then
    get stacked diagnostics v_code=message_text;
    perform pg_temp.assert_true(
      v_code='WEEKLY_SOURCE_ENTITLEMENT_DERIVED_IDENTITY_KEY_INVALID',
      'F3: a null derivation key must raise by name, got '||v_code);
  end;
  begin
    perform private.weekly_source_entitlement_derived_uuid_v1(null,'k');
    raise exception 'WP02B_F3_NULL_DOMAIN_WAS_ACCEPTED';
  exception when invalid_parameter_value then
    get stacked diagnostics v_code=message_text;
    perform pg_temp.assert_true(
      v_code='WEEKLY_SOURCE_ENTITLEMENT_DERIVED_IDENTITY_KEY_INVALID',
      'F3: a null derivation domain must raise by name, got '||v_code);
  end;
  perform pg_temp.assert_true(
    private.weekly_source_entitlement_derived_uuid_v1('D','k')
      =private.weekly_source_entitlement_derived_uuid_v1('D','k')
    and private.weekly_source_entitlement_derived_uuid_v1('D','k')
       <>private.weekly_source_entitlement_derived_uuid_v1('D','k2'),
    'F3: the derivation must stay reproducible and injective on distinct keys');

  -- ---- a real single-root request over the paid root ---------------------
  select receipt.root_timesheet_id,receipt.final_revision_id
    into strict v_root,v_revision
    from public.weekly_source_ordinary_pay_projection_receipts receipt
   where receipt.idempotency_key='projection-a6-paid-refusal';
  select timesheet.* into strict v_timesheet
    from public.timesheets timesheet where timesheet.timesheet_id=v_root;
  select contract.* into strict v_contract
    from public.contracts contract where contract.id=v_timesheet.contract_id;
  select source_group.agency_id into strict v_agency
    from public.weekly_source_final_revisions revision_row
    join public.weekly_source_cycles cycle_row
      on cycle_row.id=revision_row.source_cycle_id
    join public.weekly_source_groups source_group
      on source_group.id=cycle_row.source_group_id
   where revision_row.id=v_revision;
  -- `contracts.candidate_id` is nullable, so the Candidate is taken from the
  -- root's own current financial snapshot when the Contract does not carry one.
  v_candidate:=v_contract.candidate_id;
  if v_candidate is null then
    select financial.candidate_id into strict v_candidate
      from public.timesheets_financials financial
     where financial.timesheet_id=v_root and financial.is_current;
  end if;

  v_request:=private.weekly_source_entitlement_proposal_request_v1(
    v_root,v_revision,'LOCKED_FINAL_SOURCE',
    'b2000000-0000-4000-8000-000000000b01',1::bigint,
    'b2000000-0000-4000-8000-000000000e01',
    'b2000000-0000-4000-8000-000000000d01','[]'::jsonb);

  -- ---- F4: p_agency_id was written unchecked ----------------------------
  -- A null gave a raw 23502 not-null violation, and a WRONG non-null agency was
  -- not detectable by anything installed.  The coordinator copies this value
  -- into every head, so it is the agency a published entitlement is filed under.
  begin
    perform private.weekly_source_entitlement_proposal_record_v1(
      v_request,null,v_timesheet.contract_id,v_timesheet.week_ending_date,v_actor);
    raise exception 'WP02B_F4_NULL_AGENCY_WAS_ACCEPTED';
  exception when invalid_parameter_value then
    get stacked diagnostics v_code=message_text;
    perform pg_temp.assert_true(
      v_code='WEEKLY_SOURCE_PROPOSAL_AGENCY_INVALID',
      'F4: a null agency must be refused by name, got '||v_code);
  end;
  begin
    perform private.weekly_source_entitlement_proposal_record_v1(
      v_request,'b2000000-0000-4000-8000-0000000000bb',
      v_timesheet.contract_id,v_timesheet.week_ending_date,v_actor);
    raise exception 'WP02B_F4_WRONG_AGENCY_WAS_ACCEPTED';
  exception when invalid_parameter_value then
    get stacked diagnostics v_code=message_text;
    perform pg_temp.assert_true(
      v_code='WEEKLY_SOURCE_PROPOSAL_AGENCY_DISAGREES_WITH_SOURCE_REVISION',
      'F4: an agency that disagrees with the source revision''s own chain must be '
        ||'refused by name, got '||v_code);
  end;
  perform pg_temp.assert_true(
    (select pg_catalog.count(*)=0
       from public.weekly_source_entitlement_decision_bundles bundle_row
      where bundle_row.decision_bundle_id='b2000000-0000-4000-8000-000000000b01'),
    'F4: an agency refusal writes no bundle row');

  -- The correct agency still records, and re-recording the identical proposal
  -- is still idempotent.
  v_result:=private.weekly_source_entitlement_proposal_record_v1(
    v_request,v_agency,v_timesheet.contract_id,v_timesheet.week_ending_date,v_actor);
  perform pg_temp.assert_true(
    coalesce((v_result->>'ok')::boolean,false)
    and coalesce((v_result->>'created')::boolean,false),
    'F4: the agency on the source revision''s own chain must record, got '||v_result::text);
  v_result:=private.weekly_source_entitlement_proposal_record_v1(
    v_request,v_agency,v_timesheet.contract_id,v_timesheet.week_ending_date,v_actor);
  perform pg_temp.assert_true(
    coalesce((v_result->>'ok')::boolean,false)
    and coalesce((v_result->>'created')::boolean,true) is false,
    'F2: an identical re-record is still idempotent, got '||v_result::text);

  -- ---- F2: an idempotent re-record used to DROP a whole-root review ------
  -- Idempotence is keyed on the request digest, and the review is control scope,
  -- so it is not in the digest.  A single-root bundle can never carry a review
  -- at all, so the request-side disagreement is caught earlier and by a
  -- different name; the branch under test here is the ROW side.  The accepted
  -- decision below is a SEEDED row (labelled): it carries the same request
  -- digest and a persisted review, and the request that follows carries none.
  v_digest:=private.weekly_source_publication_request_digest_v1(
    private.weekly_source_publication_request_canonical_v1(
      pg_catalog.jsonb_set(v_request,'{decision_bundle_id}',
        '"b2000000-0000-4000-8000-000000000b02"'::jsonb),'IMMEDIATE',null));
  insert into public.weekly_source_entitlement_decision_bundles(
    decision_bundle_id,bundle_revision,agency_id,candidate_id,week_ending_date,bundle_kind,
    source_root_family_booking_id,source_root_timesheet_id,source_contract_id,
    target_root_family_booking_id,target_root_timesheet_id,target_contract_id,
    decision_id,decided_by_user_id,publication_mode,request_digest,source_revision_digest,
    contract_choice_digest,before_inventory_digest,proposed_head_ids,
    whole_root_review_required,whole_root_reviewed_by_user_id,whole_root_reviewed_at_utc,state)
  select 'b2000000-0000-4000-8000-000000000b02',1,v_agency,v_candidate,
         v_timesheet.week_ending_date,'CROSS_CONTRACT_A_B',
         v_timesheet.booking_id,v_root,v_timesheet.contract_id,
         v_timesheet.booking_id||'-TARGET',v_root,other_contract.id,
         'b2000000-0000-4000-8000-000000000d02',v_actor,'IMMEDIATE',v_digest,
         pg_catalog.decode(pg_catalog.repeat('b1',32),'hex'),
         pg_catalog.decode(pg_catalog.repeat('b2',32),'hex'),
         pg_catalog.decode(pg_catalog.repeat('b3',32),'hex'),
         array['b2000000-0000-4000-8000-000000000e02'::uuid],
         true,v_actor,'2026-09-18 09:00:00+00','PROPOSED'
    from public.contracts other_contract
   where other_contract.id<>v_timesheet.contract_id
     and other_contract.candidate_id=v_candidate
   order by other_contract.id
   limit 1;
  if found then
    begin
      perform private.weekly_source_entitlement_proposal_record_v1(
        pg_catalog.jsonb_set(v_request,'{decision_bundle_id}',
          '"b2000000-0000-4000-8000-000000000b02"'::jsonb),
        v_agency,v_timesheet.contract_id,v_timesheet.week_ending_date,v_actor);
      raise exception 'WP02B_F2_REVIEW_WAS_SILENTLY_DROPPED';
    exception when invalid_parameter_value then
      get stacked diagnostics v_code=message_text;
      perform pg_temp.assert_true(
        v_code='WEEKLY_SOURCE_PROPOSAL_REVIEW_DISAGREES_WITH_ACCEPTED_DECISION',
        'F2: a re-record whose review facts differ from the accepted decision must be '
          ||'refused by name, got '||v_code);
    end;
    perform pg_temp.assert_true(
      (select bundle_row.whole_root_review_required
         from public.weekly_source_entitlement_decision_bundles bundle_row
        where bundle_row.decision_bundle_id='b2000000-0000-4000-8000-000000000b02'),
      'F2: the persisted review survives the refusal untouched');
  else
    raise notice 'F2 row-side case skipped: the fixture has no second Contract for the Candidate';
  end if;

  -- ---- round-5 ruling, Part E: no PARTIAL Contract-to-Contract move ------
  -- "Not in scope for this release.  The supported operation is the
  --  whole-entitlement move."  WHOLE means the source root retains nothing.
  -- The builder refuses to compose one and the coordinator refuses to publish
  -- one; the recorder is the third point, and the one that stops a hand-built
  -- partial request being RECORDED as an accepted decision that can never be
  -- published.  The two-member request below is synthesised from the real
  -- single-root request by duplicating its aligned arrays: the recorder refuses
  -- on the source member's own certified-zero fact, before any read of the
  -- roots and before its insert, so no second real root is needed.
  v_two_member:=v_request
    ||pg_catalog.jsonb_build_object(
      'member_root_ids',pg_catalog.jsonb_build_array(
        v_root,'b2000000-0000-4000-8000-000000000c02'),
      'member_family_booking_ids',pg_catalog.jsonb_build_array(
        v_timesheet.booking_id,v_timesheet.booking_id||'-TARGET'),
      'member_root_versions',pg_catalog.jsonb_build_array(v_timesheet.version,1),
      'head_ids',pg_catalog.jsonb_build_array(
        'b2000000-0000-4000-8000-000000000e11','b2000000-0000-4000-8000-000000000e12'));
  v_two_member:=pg_catalog.jsonb_set(v_two_member,'{control,bundle_kind}',
    '"CROSS_CONTRACT_A_B"'::jsonb);
  v_two_member:=pg_catalog.jsonb_set(v_two_member,
    '{financial_request,contract_choices}',
    (v_request#>'{financial_request,contract_choices}')
      ||pg_catalog.jsonb_build_array(
          (v_request#>'{financial_request,contract_choices,0}')
          ||'{"root_ordinal":2}'::jsonb));
  v_two_member:=pg_catalog.jsonb_set(v_two_member,'{control,before_positions}',
    (v_request#>'{control,before_positions}')
      ||pg_catalog.jsonb_build_array(
          (v_request#>'{control,before_positions,0}')||'{"root_ordinal":2}'::jsonb));
  -- member 1 keeps a component: a PARTIAL move.
  v_two_member:=pg_catalog.jsonb_set(v_two_member,
    '{financial_request,member_entitlements}',
    pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object('root_ordinal',1,
        'authority_kind','LOCKED_FINAL_SOURCE','certified_zero',false,'component_count',1,
        -- one component left behind on the source: that is what makes it partial.
        -- The complete I-3 section 4.1 allowlist, every key present.
        'components',pg_catalog.jsonb_build_array(
          pg_catalog.jsonb_build_object(
            'component_ordinal',1,
            'component_id','b2000000-0000-4000-8000-000000000c11',
            'component_kind','WORKED_TIME','economic_key_type','SEGMENT',
            'economic_key_value','wp02b-partial-seg','component_member_identity','wp02b-partial-mem',
            'segment_id',null,'segment_key',null,'segment_stable_key',null,
            'work_date',null,'reference_number',null,
            'hours_day','1.000000','hours_night',null,'hours_sat',null,'hours_sun',null,
            'hours_bh',null,'additional_code_raw',null,'unit_count',null,
            'unit_pay_rate',null,'unit_charge_rate',null,'expense_code',null,
            'pay_ex_vat','10.00','charge_ex_vat',null,
            'exclude_from_pay',false,'origin','WEEKLY_SOURCE',
            'movement_id',null,'movement_group_id',null))),
      pg_catalog.jsonb_build_object('root_ordinal',2,
        'authority_kind','LOCKED_FINAL_SOURCE','certified_zero',true,'component_count',0,
        'components',pg_catalog.jsonb_build_array())));
  begin
    perform private.weekly_source_entitlement_proposal_record_v1(
      v_two_member,v_agency,v_timesheet.contract_id,v_timesheet.week_ending_date,v_actor);
    raise exception 'WP02B_PARTIAL_MOVE_WAS_RECORDED';
  exception when invalid_parameter_value then
    get stacked diagnostics v_code=message_text;
    perform pg_temp.assert_true(
      v_code='WEEKLY_SOURCE_PROPOSAL_PARTIAL_MOVE_UNSUPPORTED',
      'Part E: the recorder must refuse a partial move by name, got '||v_code);
  end;
  -- The same request with the source member certified zero is a WHOLE move and
  -- gets past the scope gate: whatever it is refused for afterwards, it is not
  -- refused for being partial.
  v_two_member:=pg_catalog.jsonb_set(v_two_member,
    '{financial_request,member_entitlements,0}',
    pg_catalog.jsonb_build_object('root_ordinal',1,
      'authority_kind','LOCKED_FINAL_SOURCE','certified_zero',true,'component_count',0,
      'components',pg_catalog.jsonb_build_array()));
  v_code:=null;
  begin
    perform private.weekly_source_entitlement_proposal_record_v1(
      v_two_member,v_agency,v_timesheet.contract_id,v_timesheet.week_ending_date,v_actor);
  exception when others then
    get stacked diagnostics v_code=message_text;
  end;
  perform pg_temp.assert_true(
    coalesce(v_code,'')<>'WEEKLY_SOURCE_PROPOSAL_PARTIAL_MOVE_UNSUPPORTED',
    'Part E: a WHOLE move must not be refused for being partial, got '||coalesce(v_code,'<recorded>'));

  -- ---- F5: interface I-7's singleton read --------------------------------
  -- DELIBERATE LOCAL PATCH, inside a savepoint: the two partial unique indexes
  -- are dropped to stand for the later change that removes them.  Part 1 review
  -- rule 5 is that safety must not be expressed through "the unique index makes
  -- this impossible"; with the index gone the read must still fail closed rather
  -- than return an arbitrary row, and this answer decides a pay outcome.
  begin
    drop index public.weekly_source_entitlement_heads_committed_current_uq;
    drop index public.weekly_source_entitlement_heads_committed_root_uq;
    insert into public.weekly_source_entitlement_heads(
      id,authority_kind,agency_id,candidate_id,contract_id,week_ending_date,
      root_timesheet_id,root_family_booking_id,root_timesheet_version,head_revision,
      prior_head_id,state,certified_zero,component_count,entitlement_digest,inventory_digest,
      source_generation_digest,publication_receipt_digest,decision_bundle_id,bundle_revision,
      decision_id,decided_by_user_id,scope_change_tx_token,committed_at_utc)
    values
     ('b2000000-0000-4000-8000-000000000f01','LOCKED_FINAL_SOURCE',v_agency,
      v_candidate,v_timesheet.contract_id,v_timesheet.week_ending_date,
      v_root,v_timesheet.booking_id,v_timesheet.version,1,null,
      'COMMITTED_CURRENT',true,0,
      pg_catalog.decode(pg_catalog.repeat('c1',32),'hex'),
      pg_catalog.decode(pg_catalog.repeat('c2',32),'hex'),
      pg_catalog.decode(pg_catalog.repeat('c3',32),'hex'),
      pg_catalog.decode(pg_catalog.repeat('c4',32),'hex'),
      'b2000000-0000-4000-8000-000000000b01',1,
      'b2000000-0000-4000-8000-000000000d01',v_actor,
      pg_catalog.gen_random_uuid(),pg_catalog.clock_timestamp()),
     ('b2000000-0000-4000-8000-000000000f02','LOCKED_FINAL_SOURCE',v_agency,
      v_candidate,v_timesheet.contract_id,v_timesheet.week_ending_date,
      v_root,v_timesheet.booking_id,v_timesheet.version,2,
      'b2000000-0000-4000-8000-000000000f01',
      'COMMITTED_CURRENT',true,0,
      pg_catalog.decode(pg_catalog.repeat('d1',32),'hex'),
      pg_catalog.decode(pg_catalog.repeat('d2',32),'hex'),
      pg_catalog.decode(pg_catalog.repeat('d3',32),'hex'),
      pg_catalog.decode(pg_catalog.repeat('d4',32),'hex'),
      'b2000000-0000-4000-8000-000000000b01',1,
      'b2000000-0000-4000-8000-000000000d01',v_actor,
      pg_catalog.gen_random_uuid(),pg_catalog.clock_timestamp());
    select pg_catalog.count(*)::integer into v_head_count
      from public.weekly_source_entitlement_heads head_row
     where pg_catalog.btrim(head_row.root_family_booking_id)
           =pg_catalog.btrim(v_timesheet.booking_id)
       and head_row.state='COMMITTED_CURRENT';
    perform pg_temp.assert_true(v_head_count=2,
      'F5 setup: two committed current heads must exist for the family once the index is gone');
    v_effective:=private.weekly_source_effective_inventory_v1(v_root);
    perform pg_temp.assert_true(
      coalesce((v_effective->>'ok')::boolean,true) is false
      and v_effective->>'code'='WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE'
      and v_effective->'detail'->>'reason'='MULTIPLE_COMMITTED_CURRENT_HEADS_FOR_THE_FAMILY',
      'F5: two committed current heads must fail closed, not return an arbitrary one, got '
        ||v_effective::text);
    raise exception 'WP02B_F5_RELEASE_THE_LOCAL_PATCH';
  exception when raise_exception then
    get stacked diagnostics v_code=message_text;
    if v_code<>'WP02B_F5_RELEASE_THE_LOCAL_PATCH' then raise; end if;
  end;
  -- The sub-block's raise rolled the index drop and the seeded heads back.
  perform pg_temp.assert_true(
    (select pg_catalog.count(*)=2 from pg_catalog.pg_indexes
      where schemaname='public'
        and indexname in ('weekly_source_entitlement_heads_committed_current_uq',
                          'weekly_source_entitlement_heads_committed_root_uq')),
    'F5: the local patch must be released again');
end
$wp02b_review_findings$;

-- ===========================================================================
-- WP-52: NHSP row order must have no financial meaning (14 s4.2.5).
--
-- WP-50 finding F1, executed: two Candidates with identical facts - an 8 h
-- shift, then a full reversal of it plus a re-issued 9 h line - ended on
-- 9.00 h / GBP 90.00 when the Trust listed the reversal first and
-- 0.00 h / GBP 0.00 when it listed the reversal second, while the Client was
-- invoiced GBP 180.00 either way.  Across reports it was worse: +8 h, a +9 h
-- re-issue, then a later reversal of the original left the Candidate on
-- GBP 0.00 against a GBP 180.00 invoice.
--
-- These cases drive the REAL owners - weekly_source_projection_rows_apply_
-- atomic_v1 with the broker's SCHEDULE_TUPLE identity, weekly_source_finalise_
-- atomic_v1 and weekly_source_ordinary_pay_projection_apply_atomic_v1 - and
-- assert that every permutation produces the SAME money, that the sentinel
-- which should differ does differ, and that a historical reversal of another
-- shift still leaves a genuine positive paid.
-- ===========================================================================

-- A source group, Trust and policy of its own.  These cases add several
-- finalised cycles, and a later final cycle inside a shared scope would change
-- what OTHER verifiers that include this file are allowed to do - the
-- correct-final owner refuses an open with WEEKLY_SOURCE_CORRECTION_LATER_
-- FINAL_CYCLE_EXISTS.  Isolating the fixture keeps this regression from
-- deciding anything outside itself.
insert into public.clients(id,name)
values ('52000000-0000-4000-8000-000000000002','WP52 NHSP Trust');
insert into public.client_settings(
  client_id,vat_rate_pct,effective_from,is_nhsp,autoprocess_hr,
  requires_hr,no_timesheet_required
) values (
  '52000000-0000-4000-8000-000000000002',20,'2026-01-01',true,false,false,false
);
insert into public.weekly_source_groups(
  id,environment,agency_id,code,display_name,source_family,cutoff_weekday,
  cutoff_local_time,nhsp_report_heading_name
) values (
  '52000000-0000-4000-8000-000000000005','TEST',
  'a0000000-0000-4000-8000-000000000006','WP52_NHSP',
  'WP52 NHSP','NHSP',3,'15:00','WP52 NHSP Trust'
);
insert into public.weekly_source_group_clients(
  id,source_group_id,client_id,valid_from,created_by_user_id
) values (
  '52000000-0000-4000-8000-000000000007',
  '52000000-0000-4000-8000-000000000005',
  '52000000-0000-4000-8000-000000000002','2026-01-01',
  'a0000000-0000-4000-8000-000000000001'
);
insert into public.weekly_source_client_policies(
  id,source_group_id,client_id,effective_from,authority_mode,document_mode,
  self_bill_enabled,weekly_rate_classification_method,manager_queries_enabled,
  manager_query_recipient,created_by_user_id
) values (
  '52000000-0000-4000-8000-000000000012',
  '52000000-0000-4000-8000-000000000005',
  '52000000-0000-4000-8000-000000000002','2026-01-01',
  'SOURCE_AUTHORITY','CHECK_ONLY',true,'SPLIT_RATE_WINDOWS',true,
  'manager@example.test','a0000000-0000-4000-8000-000000000001'
);

create function pg_temp.wp52_econ(p_net integer,p_break integer,p_sign integer)
returns jsonb language sql immutable as $function$
  select pg_catalog.jsonb_build_object(
    'schema_version','WEEKLY_SOURCE_CANONICAL_ECONOMIC_SNAPSHOT_V1',
    'calculator_version','WEEKLY_SHIFT_FINANCIAL_SEGMENT_V1','source_mode','NHSP_WEEKLY',
    'rate_method','SPLIT_RATE_WINDOWS','sign',p_sign,
    'paid_minutes',p_net,'break_minutes',p_break,
    'bucket_minutes',pg_catalog.jsonb_build_object('day',p_net,'night',0,'sat',0,'sun',0,'bh',0),
    'hours',pg_catalog.jsonb_build_object(
      'day',pg_catalog.round((p_net::numeric/60)*p_sign,2),'night',0,'sat',0,'sun',0,'bh',0),
    'pay_rates',pg_catalog.jsonb_build_object('day',10,'night',10,'sat',10,'sun',10,'bh',10),
    'charge_rates',pg_catalog.jsonb_build_object('day',20,'night',20,'sat',20,'sun',20,'bh',20),
    'total_pay_pence',((pg_catalog.round(pg_catalog.round(
        pg_catalog.abs(pg_catalog.round((p_net::numeric/60)*p_sign,2))*10,2)*100,0)::bigint)*p_sign)::text,
    'calculated_charge_pence',((pg_catalog.round(pg_catalog.round(
        pg_catalog.abs(pg_catalog.round((p_net::numeric/60)*p_sign,2))*20,2)*100,0)::bigint)*p_sign)::text
  );
$function$;

create function pg_temp.wp52_candidate(p_code text) returns jsonb language plpgsql as $function$
declare v_cand uuid:=pg_catalog.gen_random_uuid(); v_con uuid:=pg_catalog.gen_random_uuid();
begin
  insert into public.candidates(id,display_name) values (v_cand,'WP52 '||p_code);
  insert into public.contracts(
    id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
    weekly_timesheet_source,self_bill,no_timesheet_required,requires_hr,autoprocess_hr,is_nhsp
  ) values (
    v_con,v_cand,'52000000-0000-4000-8000-000000000002','2026-01-01','2026-12-31','PAYE','{}'::jsonb,
    null,true,false,false,false,false
  );
  return pg_catalog.jsonb_build_object('candidate_id',v_cand,'contract_id',v_con,'code',p_code);
end; $function$;

create function pg_temp.wp52_scope(p_code text,p_week_ending date,p_cutoff timestamptz,p_rows integer)
returns jsonb language plpgsql as $function$
declare v_cycle uuid:=pg_catalog.gen_random_uuid(); v_scope uuid:=pg_catalog.gen_random_uuid();
        v_upload uuid:=pg_catalog.gen_random_uuid(); v_pub uuid:=pg_catalog.gen_random_uuid();
        v_actor constant uuid:='a0000000-0000-4000-8000-000000000001';
begin
  insert into public.weekly_source_cycles(
    id,source_group_id,finalisation_week_ending,cutoff_at_utc,state,version,projection_state
  ) values (v_cycle,'52000000-0000-4000-8000-000000000005',p_week_ending,p_cutoff,'OPEN',1,'REBUILDING');
  insert into public.weekly_source_report_scopes(
    id,source_cycle_id,environment,agency_id,source_group_id,client_id,cutoff_at_utc,
    version,state,projection_state
  ) values (v_scope,v_cycle,'TEST','a0000000-0000-4000-8000-000000000006',
    '52000000-0000-4000-8000-000000000005','52000000-0000-4000-8000-000000000002',p_cutoff,
    1,'OPEN','REBUILDING');
  insert into public.weekly_source_uploads(
    id,source_cycle_id,report_scope_id,original_filename,content_sha256,byte_count,
    source_format_profile_id,parser_version,normaliser_version,
    workbook_part_and_sheet_fingerprint,header_coordinate_map_json,header_coordinate_map_hash,
    money_lexical_authority_version,declared_scope_fingerprint,coverage_proof_kind,
    physical_row_count,accepted_count,row_manifest_hash,state,uploaded_by_user_id,file_metadata_json
  ) values (
    v_upload,v_cycle,v_scope,'wp52-'||p_code||'.xlsx',
    private.weekly_source_sha256_jsonb_v1('WP52_CONTENT',pg_catalog.to_jsonb(p_code)),200,
    '32222222-2222-4222-8222-222222222222','WP52_PARSER_V1','NHSP_BACKING_NORMALISER_V1',
    private.weekly_source_sha256_jsonb_v1('WP52_WORKBOOK',pg_catalog.to_jsonb(p_code)),'{}'::jsonb,
    private.weekly_source_sha256_jsonb_v1('WP52_HEADERS',pg_catalog.to_jsonb(p_code)),
    'XLSX_BINARY64_SAME_VALUE_PENCE_V1',
    private.weekly_source_sha256_jsonb_v1('WP52_SCOPE',pg_catalog.to_jsonb(p_code)),
    'NHSP_TRUST_REPORT_SCOPE',p_rows,p_rows,
    private.weekly_source_sha256_jsonb_v1('WP52_ROWS',pg_catalog.to_jsonb(p_code)),
    'CURRENT',v_actor,
    pg_catalog.jsonb_build_object(
      'nhsp_report_number','BR-52-'||p_code,'nhsp_report_heading_name','WP52 NHSP Trust')
  );
  update public.weekly_source_report_scopes
  set current_complete_upload_id=v_upload where id=v_scope;
  insert into public.weekly_source_projection_publications(
    id,source_cycle_id,authority_scope_kind,report_scope_id,upload_id,authority_scope_version,
    comparison_manifest_hash,issue_set_hash,state
  ) values (v_pub,v_cycle,'NHSP_REPORT_SCOPE',v_scope,v_upload,1,
    private.weekly_source_sha256_jsonb_v1('WP52_CMP',pg_catalog.to_jsonb(p_code)),
    private.weekly_source_sha256_jsonb_v1('WP52_ISS',pg_catalog.to_jsonb(p_code)),'BUILDING');
  return pg_catalog.jsonb_build_object(
    'cycle_id',v_cycle,'scope_id',v_scope,'upload_id',v_upload,'pub_id',v_pub,'code',p_code);
end; $function$;

create function pg_temp.wp52_row(p_scope jsonb,p_ordinal integer,p_reference text,p_candidate jsonb,
  p_date date,p_start time,p_end time,p_charge bigint) returns uuid language plpgsql as $function$
declare v_row uuid:=pg_catalog.gen_random_uuid();
        v_net integer:=(pg_catalog.date_part('epoch',p_end-p_start)/60)::integer;
        v_commission bigint:=case when p_charge<0 then -500 else 500 end;
begin
  insert into public.weekly_source_upload_rows(
    id,upload_id,source_row_ordinal,external_source_key,source_candidate_identity,
    source_client_identity,work_date,start_at_local,end_at_local,break_minutes,
    actual_net_minutes,row_finalisation_state,role_band_source,source_commission_pence,
    source_total_cost_pence,source_shift_charge_pence,source_money_parse_state,
    source_qualification_profile_version,source_expense_parse_state,normalised_row_hash
  ) values (
    v_row,(p_scope->>'upload_id')::uuid,p_ordinal,p_reference,
    'WP52 '||(p_candidate->>'code'),'WP52 NHSP Trust',
    p_date,(p_date::text||' '||p_start::text)::timestamp,
    (p_date::text||' '||p_end::text)::timestamp,0,v_net,'SOURCE_WORKED','BAND 5',
    v_commission,p_charge-v_commission,p_charge,'VALID',
    'NHSP_TWO_COMPONENT_PENCE_V1','NOT_APPLICABLE',
    private.weekly_source_sha256_jsonb_v1('WP52_ROW',pg_catalog.jsonb_build_object(
      's',p_scope->>'code','o',p_ordinal,'r',p_reference))
  );
  return v_row;
end; $function$;

create function pg_temp.wp52_entry(p_candidate jsonb,p_row uuid,p_charge bigint,p_net integer)
returns jsonb language sql immutable as $function$
  select pg_catalog.jsonb_build_object(
    'upload_row_id',p_row,'mapping_state','RESOLVED',
    'candidate_id',(p_candidate->>'candidate_id')::uuid,
    'client_id','52000000-0000-4000-8000-000000000002',
    'contract_id',(p_candidate->>'contract_id')::uuid,
    'contract_selection_method','AUTO_UNIQUE',
    'qualifying_contract_ids',pg_catalog.jsonb_build_array((p_candidate->>'contract_id')::uuid),
    -- 24 s9: the Reference Number is never the durable work identity.  This is
    -- the identity the broker sends for every NHSP row.
    'identity_kind','SCHEDULE_TUPLE',
    'link_kind',case when p_charge<0 then 'FULL_NEGATIVE_SOURCE' else 'POSITIVE_SOURCE' end,
    'economic_snapshot',pg_temp.wp52_econ(p_net,0,case when p_charge<0 then -1 else 1 end),
    'charge_check',pg_catalog.jsonb_build_object(
      'row_sign_kind',case when p_charge<0 then 'FULL_NEGATIVE' else 'POSITIVE' end,
      'source_commission_pence',(case when p_charge<0 then -500 else 500 end)::text,
      'source_total_cost_pence',(p_charge-case when p_charge<0 then -500 else 500 end)::text,
      'source_shift_charge_pence',p_charge::text,
      'calculated_segment_charge_pence',
        (pg_temp.wp52_econ(p_net,0,case when p_charge<0 then -1 else 1 end)
          ->>'calculated_charge_pence'),
      'comparison_result','EXACT','comparison_reason_code','EXACT','phase_severity','NONE')
  );
$function$;

-- One report: stage its rows in the exact physical order given, publish and
-- finalise, then run the real ordinary pay projection on the root.
create function pg_temp.wp52_report(p_code text,p_candidate jsonb,p_week_ending date,
  p_cutoff timestamptz,p_rows jsonb,p_root uuid) returns jsonb language plpgsql as $function$
declare v_scope jsonb; v_entries jsonb:='[]'::jsonb; v_row jsonb; v_row_id uuid;
        v_net integer; v_index integer; v_result jsonb; v_root uuid:=p_root;
begin
  v_scope:=pg_temp.wp52_scope(p_code,p_week_ending,p_cutoff,
    pg_catalog.jsonb_array_length(p_rows));
  for v_index in 0..pg_catalog.jsonb_array_length(p_rows)-1 loop
    v_row:=p_rows->v_index;
    v_net:=(pg_catalog.date_part('epoch',
      (v_row->>'end')::time-(v_row->>'start')::time)/60)::integer;
    v_row_id:=pg_temp.wp52_row(v_scope,v_index+1,p_code||'-R'||v_index,p_candidate,
      (v_row->>'date')::date,(v_row->>'start')::time,(v_row->>'end')::time,
      (v_row->>'charge')::bigint);
    v_entries:=v_entries||pg_catalog.jsonb_build_array(
      pg_temp.wp52_entry(p_candidate,v_row_id,(v_row->>'charge')::bigint,v_net));
  end loop;
  perform public.weekly_source_projection_rows_apply_atomic_v1(
    'a0000000-0000-4000-8000-000000000001',(v_scope->>'pub_id')::uuid,v_entries);
  update public.weekly_source_projection_publications
  set state='CURRENT',published_at_utc=pg_catalog.clock_timestamp()
  where id=(v_scope->>'pub_id')::uuid;
  update public.weekly_source_report_scopes
  set projection_state='CURRENT',current_projection_publication_id=(v_scope->>'pub_id')::uuid
  where id=(v_scope->>'scope_id')::uuid;
  perform public.weekly_source_finalise_atomic_v1(pg_catalog.jsonb_build_object(
    'actor_user_id','a0000000-0000-4000-8000-000000000001',
    'source_cycle_id',(v_scope->>'cycle_id')::uuid,'authority_scope_kind','NHSP_REPORT_SCOPE',
    'report_scope_id',(v_scope->>'scope_id')::uuid,'upload_id',(v_scope->>'upload_id')::uuid,
    'projection_publication_id',(v_scope->>'pub_id')::uuid,'expected_authority_scope_version',1,
    'expected_row_manifest_hash',(select pg_catalog.encode(u.row_manifest_hash,'hex')
      from public.weekly_source_uploads u where u.id=(v_scope->>'upload_id')::uuid),
    'expected_comparison_manifest_hash',(select pg_catalog.encode(p.comparison_manifest_hash,'hex')
      from public.weekly_source_projection_publications p where p.id=(v_scope->>'pub_id')::uuid),
    'expected_issue_set_hash',(select pg_catalog.encode(p.issue_set_hash,'hex')
      from public.weekly_source_projection_publications p where p.id=(v_scope->>'pub_id')::uuid)
  ));
  if v_root is null then
    select movement.invoice_timesheet_id into v_root
    from public.weekly_source_billing_movements movement
    join public.weekly_source_final_revisions revision on revision.id=movement.final_revision_id
    where revision.source_cycle_id=(v_scope->>'cycle_id')::uuid
      and movement.candidate_id=(p_candidate->>'candidate_id')::uuid
    limit 1;
  end if;
  v_result:=public.weekly_source_ordinary_pay_projection_apply_atomic_v1(
    pg_temp.ordinary_projection_request(
      (v_scope->>'cycle_id')::uuid,'wp52-'||p_code,v_root));
  return pg_catalog.jsonb_build_object(
    'scope',v_scope,'root_timesheet_id',v_root,'outcome',v_result->>'outcome');
end; $function$;

-- The paid position of a root, as one comparable value.  `uq_tsfin_current`
-- makes `is_current` single-valued per Timesheet, so this needs no ordering
-- and no `limit`.
create function pg_temp.wp52_paid(p_root uuid) returns text language sql stable as $function$
  select coalesce((
    select pg_catalog.format('%s|%s|%s',
      financial.total_hours,financial.total_pay_ex_vat,financial.total_charge_ex_vat)
    from public.timesheets_financials financial
    where financial.timesheet_id=p_root and financial.is_current
  ),'<none>');
$function$;

-- What the Client is invoiced for this Candidate: the signed presentation sum.
create function pg_temp.wp52_invoiced(p_candidate jsonb) returns numeric language sql stable as $function$
  select coalesce(pg_catalog.sum(movement.invoice_presentation_charge_pence),0)::numeric/100
  from public.weekly_source_billing_movements movement
  where movement.candidate_id=(p_candidate->>'candidate_id')::uuid;
$function$;

do $wp52_nhsp_row_order$
declare
  v_pos8 constant jsonb:='{"date":"2026-09-07","start":"09:00","end":"17:00","charge":16000}';
  v_pos9 constant jsonb:='{"date":"2026-09-07","start":"09:00","end":"18:00","charge":18000}';
  v_neg8 constant jsonb:='{"date":"2026-09-07","start":"09:00","end":"17:00","charge":-16000}';
  c1 jsonb; c2 jsonb; c3 jsonb; c4 jsonb; c5 jsonb; c6 jsonb;
  r1 jsonb; r2 jsonb; r3 jsonb; r4 jsonb; r5 jsonb; r6 jsonb;
  v_root1 uuid; v_root2 uuid; v_root3 uuid; v_root4 uuid; v_root5 uuid; v_root6 uuid;
  v_paid1 text; v_paid2 text; v_paid3 text; v_paid4 text; v_paid5 text; v_paid6 text;
begin
  -- P1 / P2.  One prior report establishes 8 h.  The correction arrives as one
  -- report carrying the reversal and the 9 h re-issue, listed in OPPOSITE
  -- physical order for the two Candidates.  14 s4.2.5.
  c1:=pg_temp.wp52_candidate('perm-reversal-first');
  c2:=pg_temp.wp52_candidate('perm-reversal-second');
  r1:=pg_temp.wp52_report('p1a',c1,'2029-01-06','2026-08-02T10:01:00Z',
    pg_catalog.jsonb_build_array(v_pos8),null);
  v_root1:=(r1->>'root_timesheet_id')::uuid;
  perform pg_temp.assert_true(pg_temp.wp52_paid(v_root1)='8.00|80.00|160.00',
    'row order P1 setup: the first report must pay 8.00 h / 80.00, got '
      ||pg_temp.wp52_paid(v_root1));
  r2:=pg_temp.wp52_report('p2a',c2,'2029-01-13','2026-08-02T10:02:00Z',
    pg_catalog.jsonb_build_array(v_pos8),null);
  v_root2:=(r2->>'root_timesheet_id')::uuid;
  perform pg_temp.assert_true(pg_temp.wp52_paid(v_root2)='8.00|80.00|160.00',
    'row order P2 setup: the first report must pay 8.00 h / 80.00, got '
      ||pg_temp.wp52_paid(v_root2));

  r1:=pg_temp.wp52_report('p1b',c1,'2029-01-20','2026-08-02T10:03:00Z',
    pg_catalog.jsonb_build_array(v_neg8,v_pos9),v_root1);
  r2:=pg_temp.wp52_report('p2b',c2,'2029-01-27','2026-08-02T10:04:00Z',
    pg_catalog.jsonb_build_array(v_pos9,v_neg8),v_root2);
  v_paid1:=pg_temp.wp52_paid(v_root1);
  v_paid2:=pg_temp.wp52_paid(v_root2);

  perform pg_temp.assert_true(
    (select pg_catalog.count(*)=1 from public.weekly_work_events
      where candidate_id=(c1->>'candidate_id')::uuid)
    and (select pg_catalog.count(*)=1 from public.weekly_work_events
      where candidate_id=(c2->>'candidate_id')::uuid),
    'a reversal and its re-issue must resolve to one work event in either row order');
  perform pg_temp.assert_true(v_paid1='9.00|90.00|180.00',
    'reversal listed FIRST must pay 9.00 h / 90.00, got '||v_paid1);
  perform pg_temp.assert_true(v_paid2='9.00|90.00|180.00',
    'reversal listed SECOND must pay 9.00 h / 90.00, got '||v_paid2);
  perform pg_temp.assert_true(v_paid1=v_paid2,
    'the paid position must not depend on the physical row order: '||v_paid1||' vs '||v_paid2);
  perform pg_temp.assert_true(
    pg_temp.wp52_invoiced(c1)=180.00 and pg_temp.wp52_invoiced(c2)=180.00,
    'both row orders must invoice the Client GBP 180.00');

  -- P3.  The same correction split across reports, with the reversal of the
  -- ORIGINAL line arriving last.  14 s4.2.5: "a negative and a later positive
  -- may appear in different reports and cycles".
  c3:=pg_temp.wp52_candidate('perm-split-reversal-last');
  r3:=pg_temp.wp52_report('p3a',c3,'2029-02-03','2026-08-02T10:05:00Z',
    pg_catalog.jsonb_build_array(v_pos8),null);
  v_root3:=(r3->>'root_timesheet_id')::uuid;
  r3:=pg_temp.wp52_report('p3b',c3,'2029-02-10','2026-08-02T10:06:00Z',
    pg_catalog.jsonb_build_array(v_pos9),v_root3);
  perform pg_temp.assert_true(pg_temp.wp52_paid(v_root3)='9.00|90.00|180.00',
    'the re-issued 9 h line must become the current position, got '
      ||pg_temp.wp52_paid(v_root3));
  r3:=pg_temp.wp52_report('p3c',c3,'2029-02-17','2026-08-02T10:07:00Z',
    pg_catalog.jsonb_build_array(v_neg8),v_root3);
  v_paid3:=pg_temp.wp52_paid(v_root3);
  perform pg_temp.assert_true(v_paid3='9.00|90.00|180.00',
    'a later reversal of the ORIGINAL 8 h line must not wipe the re-issued 9 h pay, got '
      ||v_paid3);
  perform pg_temp.assert_true(v_paid3=v_paid1,
    'splitting the same correction across reports must not change the paid position: '
      ||v_paid3||' vs '||v_paid1);
  perform pg_temp.assert_true(pg_temp.wp52_invoiced(c3)=180.00,
    'the split correction must invoice the Client GBP 180.00');

  -- SENTINEL that must DIFFER: the shift is reversed and never re-issued.
  c4:=pg_temp.wp52_candidate('sentinel-no-reissue');
  r4:=pg_temp.wp52_report('p4a',c4,'2029-02-24','2026-08-02T10:08:00Z',
    pg_catalog.jsonb_build_array(v_pos8),null);
  v_root4:=(r4->>'root_timesheet_id')::uuid;
  r4:=pg_temp.wp52_report('p4b',c4,'2029-03-03','2026-08-02T10:09:00Z',
    pg_catalog.jsonb_build_array(v_neg8),v_root4);
  v_paid4:=pg_temp.wp52_paid(v_root4);
  perform pg_temp.assert_true(v_paid4='0.00|0.00|0.00',
    'a reversal with no re-issue must leave no current source position, got '||v_paid4);
  perform pg_temp.assert_true(v_paid4<>v_paid1,
    'the sentinel must differ from the re-issued cases, or this proves nothing');
  perform pg_temp.assert_true(pg_temp.wp52_invoiced(c4)=0.00,
    'a reversal with no re-issue must leave the Client invoiced GBP 0.00');

  -- MIXED ROOT.  One report carries a genuine new Wednesday shift and a
  -- historical full reversal of a Monday shift CloudTMS never saw.  14 s4.3.8:
  -- the negative invoices, and Candidate pay for the Wednesday is untouched.
  -- Driven in BOTH physical orders for the same reason as above.
  c5:=pg_temp.wp52_candidate('mixed-root');
  r5:=pg_temp.wp52_report('p5a',c5,'2029-03-10','2026-08-02T10:10:00Z',
    pg_catalog.jsonb_build_array(
      '{"date":"2026-09-14","start":"09:00","end":"17:00","charge":-16000}'::jsonb,
      '{"date":"2026-09-16","start":"09:00","end":"17:00","charge":16000}'::jsonb),null);
  v_root5:=(r5->>'root_timesheet_id')::uuid;
  v_paid5:=pg_temp.wp52_paid(v_root5);
  perform pg_temp.assert_true(v_paid5='8.00|80.00|160.00',
    'a historical reversal of another shift must not suppress a genuine positive, got '
      ||v_paid5);
  perform pg_temp.assert_true(
    (select pg_catalog.count(*)=1
     from pg_catalog.jsonb_array_elements(
       private.weekly_source_ordinary_projection_source_units_v1(
         (select revision.id from public.weekly_source_final_revisions revision
          where revision.source_cycle_id=(r5->'scope'->>'cycle_id')::uuid
            and revision.state='CURRENT'),v_root5)) unit(value)
     where coalesce((unit.value->>'first_plan6_negative')::boolean,false)),
    'exactly the historical reversal must be flagged as a first Plan 6 negative');
  perform pg_temp.assert_true(pg_temp.wp52_invoiced(c5)=0.00,
    'the mixed root must invoice GBP 160.00 less GBP 160.00');

  c6:=pg_temp.wp52_candidate('mixed-root-permuted');
  r6:=pg_temp.wp52_report('p6a',c6,'2029-03-17','2026-08-02T10:11:00Z',
    pg_catalog.jsonb_build_array(
      '{"date":"2026-09-16","start":"09:00","end":"17:00","charge":16000}'::jsonb,
      '{"date":"2026-09-14","start":"09:00","end":"17:00","charge":-16000}'::jsonb),null);
  v_root6:=(r6->>'root_timesheet_id')::uuid;
  v_paid6:=pg_temp.wp52_paid(v_root6);
  perform pg_temp.assert_true(v_paid6=v_paid5,
    'the mixed root must pay the same in either physical order: '||v_paid6||' vs '||v_paid5);

  -- The rule must hold on the installed text, not on a reading of it: no part
  -- of the live-position owner may consult the physical row ordinal.
  perform pg_temp.assert_true(
    pg_catalog.pg_get_functiondef(
      'private.weekly_source_ordinary_projection_active_movements_v1(uuid,uuid)'::regprocedure
    ) not like '%source_row_ordinal%',
    'the current-source-position owner must never read source_row_ordinal');
end
$wp52_nhsp_row_order$;

\if :{?weekly_source_ordinary_verification_outer_transaction}
\else
rollback;
\endif
