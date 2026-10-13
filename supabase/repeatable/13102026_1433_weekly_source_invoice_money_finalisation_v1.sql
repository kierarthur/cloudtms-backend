-- HealthRoster v2 F5/F6 complete controlling successors; adjacent source owners only.
\set ON_ERROR_STOP on
begin;

create or replace function private.weekly_source_finalisation_insert_current_movement_v1(
  p_transition_id uuid,
  p_nhsp_upload_row_id uuid,
  p_final_revision_id uuid,
  p_finalisation_cycle_id uuid,
  p_source_profile_kind text,
  p_movement_role text,
  p_source_line_kind text,
  p_row_resolution_id uuid,
  p_economic_snapshot_id uuid,
  p_invoice_timesheet_id uuid,
  p_source_validation_charge_pence bigint,
  p_invoice_presentation_charge_pence bigint,
  p_price_check_result text,
  p_price_check_fingerprint bytea,
  p_charge_acceptance_id uuid,
  p_source_facts_json jsonb,
  p_correction_unit_id uuid default null,
  p_prior_movement_id uuid default null
) returns uuid
language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_resolution public.weekly_source_row_resolutions%rowtype;
  v_source_row public.weekly_source_upload_rows%rowtype;
  v_economic public.weekly_source_row_economic_snapshots%rowtype;
  v_upload public.weekly_source_uploads%rowtype;
  v_invoice_rules jsonb;
  v_invoice_amounts jsonb;
  v_source_amount boolean:=false;
  v_source_facts jsonb:=p_source_facts_json;
  v_pay_vector jsonb;
  v_charge_vector jsonb;
  v_mapping_fingerprint bytea;
  v_vat_rate numeric(5,2);
  v_vat numeric(12,2);
  v_total numeric(12,2);
  v_hash bytea;
  v_id uuid;
begin
  if (p_transition_id is null)::integer+(p_nhsp_upload_row_id is null)::integer<>1
     or p_final_revision_id is null or p_finalisation_cycle_id is null
     or p_source_profile_kind not in (
       'GENERIC_COMPLETE_SNAPSHOT','NHSP_TRUST_BACKING_REPORT','HEALTHROSTER_ACTUAL_ROWS'
     )
     or p_movement_role not in ('POSITIVE','REPLACEMENT','REVERSAL')
     or p_source_line_kind not in (
       'SOURCE_ORDINARY','SOURCE_REPLACEMENT','NHSP_PHYSICAL_POSITIVE','NHSP_PHYSICAL_FULL_NEGATIVE'
     )
     or pg_catalog.jsonb_typeof(p_source_facts_json)<>'object' then
    raise exception 'WEEKLY_SOURCE_MOVEMENT_INPUT_INVALID' using errcode='22023';
  end if;
  select * into strict v_resolution
  from public.weekly_source_row_resolutions where id=p_row_resolution_id;
  select * into strict v_source_row
  from public.weekly_source_upload_rows where id=v_resolution.upload_row_id;
  select * into strict v_upload
  from public.weekly_source_uploads where id=v_source_row.upload_id;
  select * into strict v_economic
  from public.weekly_source_row_economic_snapshots where id=p_economic_snapshot_id;
  v_invoice_rules:=private.weekly_source_frozen_rule_values_v1(p_final_revision_id);
  v_source_amount:=v_invoice_rules is not null and v_invoice_rules->>'invoice_sender'='CLIENT_SELF_BILL' and v_invoice_rules->>'invoice_amount_basis'='SOURCE_AMOUNT' and v_invoice_rules->>'source_amount_basis'='EXPORT_ACTUAL_COST_INC_VAT_NET_AGENCY_WORKER_PAY';
  if v_economic.row_resolution_id is distinct from v_resolution.id
     or v_resolution.mapping_state<>'RESOLVED'
     or p_invoice_timesheet_id is null
     or not exists(
       select 1
       from public.weekly_source_final_revisions revision
       where revision.id=p_final_revision_id
         and revision.source_cycle_id=p_finalisation_cycle_id
         and (
           revision.state='CURRENT'
           or (
             revision.state='PREPARED'
             and revision.reason='CORRECT_FINAL_SOURCE'
             and exists(
               select 1
               from public.weekly_final_source_correction_sessions correction
               join public.weekly_source_uploads replacement_upload
                 on replacement_upload.id=correction.replacement_correction_upload_id
                and replacement_upload.id=revision.upload_id
                and replacement_upload.source_cycle_id=revision.source_cycle_id
                and replacement_upload.state='CORRECTION_READY'
               join public.weekly_source_projection_publications replacement_publication
                 on replacement_publication.id=correction.replacement_projection_publication_id
                and replacement_publication.upload_id=replacement_upload.id
                and replacement_publication.source_cycle_id=revision.source_cycle_id
                and replacement_publication.state='CORRECTION_READY'
               where correction.source_cycle_id=revision.source_cycle_id
                 and correction.authority_scope_kind=revision.authority_scope_kind
                 and correction.report_scope_id is not distinct from revision.report_scope_id
                 and correction.state='PREPARING'
                 and correction.expected_current_final_revision_id=revision.predecessor_revision_id
             )
           )
         )
     )
     or private.weekly_source_finalisation_economic_assert_v1(
          p_row_resolution_id,p_finalisation_cycle_id,
          case when p_source_profile_kind='NHSP_TRUST_BACKING_REPORT'
            then 'NHSP_WEEKLY' else 'HEALTHROSTER_WEEKLY' end
        ) is distinct from p_economic_snapshot_id
     or private.weekly_source_finalisation_lineage_assert_v1(
          p_row_resolution_id,p_finalisation_cycle_id
        ) is distinct from p_invoice_timesheet_id
     or (p_source_profile_kind='NHSP_TRUST_BACKING_REPORT') is distinct from
        (p_nhsp_upload_row_id is not null)
     or (p_source_profile_kind='NHSP_TRUST_BACKING_REPORT' and (
       p_nhsp_upload_row_id is distinct from v_source_row.id
       or p_source_validation_charge_pence is null
       or p_source_validation_charge_pence is distinct from v_source_row.source_shift_charge_pence
       or p_source_validation_charge_pence is distinct from p_invoice_presentation_charge_pence
       or p_price_check_result not in ('EXACT','SOURCE_ROUNDING_EQUIVALENT','ACCEPTED_DISPARITY','ACCEPTED_ZERO')
       or p_price_check_fingerprint is distinct from v_economic.calculation_fingerprint
       or (p_price_check_result in ('EXACT','SOURCE_ROUNDING_EQUIVALENT')) is distinct from
          (p_charge_acceptance_id is null)
       or (p_price_check_result in ('ACCEPTED_DISPARITY','ACCEPTED_ZERO') and not exists(
         select 1
         from public.weekly_source_charge_acceptances acceptance
         join public.weekly_source_charge_checks charge_check
           on charge_check.id=acceptance.charge_check_id
         where acceptance.id=p_charge_acceptance_id
           and acceptance.upload_row_id=v_source_row.id
           and acceptance.row_resolution_id=v_resolution.id
           and acceptance.source_upload_id=v_upload.id
           and acceptance.contract_id=v_resolution.contract_id
           and acceptance.acceptance_kind=p_price_check_result
           and acceptance.source_upload_hash=v_upload.content_sha256
           and acceptance.source_row_fingerprint=v_resolution.source_row_fingerprint
           and acceptance.contract_and_rate_fingerprint=v_economic.contract_and_rate_fingerprint
           and acceptance.effective_policy_fingerprint=v_economic.effective_policy_fingerprint
           and acceptance.charge_calculation_fingerprint=v_economic.calculation_fingerprint
           and acceptance.acceptance_policy_fingerprint=
             private.weekly_source_charge_acceptance_policy_fingerprint_v1()
           and charge_check.upload_row_id=v_source_row.id
           and charge_check.row_resolution_id=v_resolution.id
           and charge_check.charge_calculation_fingerprint=v_economic.calculation_fingerprint
           and charge_check.comparison_result=case p_price_check_result
             when 'ACCEPTED_ZERO' then 'ZERO_SOURCE_CHARGE' else 'MISMATCH' end
       ))
       or p_correction_unit_id is not null
       or p_prior_movement_id is not null
       or not (
         (v_economic.row_sign=1 and p_movement_role='POSITIVE'
           and p_source_line_kind='NHSP_PHYSICAL_POSITIVE'
           and v_source_row.source_shift_charge_pence>=0)
         or (v_economic.row_sign=-1 and p_movement_role='REVERSAL'
           and p_source_line_kind='NHSP_PHYSICAL_FULL_NEGATIVE'
           and v_source_row.source_shift_charge_pence<0)
       )
     ))
     or (p_source_profile_kind<>'NHSP_TRUST_BACKING_REPORT' and (
       (not v_source_amount and (p_source_validation_charge_pence is not null
         or p_invoice_presentation_charge_pence is distinct from v_economic.calculated_charge_pence
         or p_price_check_result<>'NOT_APPLICABLE' or p_price_check_fingerprint is not null or p_charge_acceptance_id is not null))
       or (v_source_amount and (p_source_validation_charge_pence is distinct from v_source_row.source_shift_charge_pence
         or p_invoice_presentation_charge_pence is distinct from v_source_row.source_shift_charge_pence
         or p_price_check_result not in ('EXACT','SOURCE_ROUNDING_EQUIVALENT','SOURCE_AMOUNT_DIFFERS','ACCEPTED_DISPARITY')
         or p_price_check_fingerprint is distinct from v_economic.calculation_fingerprint
         or ((p_price_check_result='ACCEPTED_DISPARITY') is distinct from (p_charge_acceptance_id is not null))))
       or v_economic.row_sign<>1
       or not (
         (p_movement_role='POSITIVE' and p_source_line_kind='SOURCE_ORDINARY'
           and p_correction_unit_id is null and p_prior_movement_id is null)
         or (p_movement_role='REPLACEMENT' and p_source_line_kind='SOURCE_REPLACEMENT'
           and p_correction_unit_id is not null and p_prior_movement_id is not null)
       )
     )) then
    raise exception 'WEEKLY_SOURCE_MOVEMENT_AUTHORITY_INVALID' using errcode='55000';
  end if;
  if p_source_profile_kind='NHSP_TRUST_BACKING_REPORT' then
    if not exists(
      select 1
      from public.weekly_source_final_revisions revision
      where revision.id=p_final_revision_id
        and revision.authority_scope_kind='NHSP_REPORT_SCOPE'
    ) then
      raise exception 'WEEKLY_SOURCE_MOVEMENT_SCOPE_INVALID' using errcode='55000';
    end if;
  elsif not exists(
    select 1
    from public.weekly_source_state_transitions transition_row
    where transition_row.id=p_transition_id
      and transition_row.final_revision_id=p_final_revision_id
      and transition_row.finalisation_cycle_id=p_finalisation_cycle_id
      and transition_row.work_event_id=v_resolution.work_event_id
      and transition_row.outcome=case when p_movement_role='POSITIVE' then 'ADD' else 'AMEND' end
  ) then
    raise exception 'WEEKLY_SOURCE_MOVEMENT_TRANSITION_INVALID' using errcode='55000';
  end if;

  if p_source_profile_kind<>'NHSP_TRUST_BACKING_REPORT' and v_invoice_rules is not null and v_invoice_rules->>'invoice_sender'='CLIENT_SELF_BILL' then
    v_invoice_amounts:=private.weekly_source_invoice_line_amounts_v1(v_resolution.id,p_final_revision_id);
    if (v_invoice_amounts->>'net_pence')::bigint is distinct from p_invoice_presentation_charge_pence
      or v_invoice_amounts->>'price_check_result' is distinct from p_price_check_result
      or nullif(v_invoice_amounts->>'charge_acceptance_id','')::uuid is distinct from p_charge_acceptance_id then raise exception 'WEEKLY_SOURCE_INVOICE_MONEY_NOT_REDERIVED' using errcode='55000'; end if;
    v_source_facts:=v_source_facts||jsonb_build_object('rule_set_version_id',v_invoice_amounts->'rule_set_version_id',
      'rule_set_values_sha256',(select version.values_sha256 from public.weekly_source_rule_set_versions version where version.id=(v_invoice_amounts->>'rule_set_version_id')::uuid),
      'invoice_hold',v_invoice_amounts->'invoice_hold','hold_release_id',v_invoice_amounts->'hold_release_id');
  end if;
  v_pay_vector:=private.weekly_source_finalisation_vector_v1(v_economic.id,'PAY',1::smallint);
  v_charge_vector:=private.weekly_source_finalisation_vector_v1(v_economic.id,'CHARGE',1::smallint);
  v_mapping_fingerprint:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_MAPPING_RATE_POLICY_V1',
    pg_catalog.jsonb_build_object(
      'row_resolution_id',v_resolution.id,
      'work_event_id',v_resolution.work_event_id,
      'candidate_id',v_resolution.candidate_id,
      'client_id',v_resolution.client_id,
      'contract_id',v_resolution.contract_id,
      'contract_and_rate_fingerprint',pg_catalog.encode(v_economic.contract_and_rate_fingerprint,'hex'),
      'effective_policy_fingerprint',pg_catalog.encode(v_economic.effective_policy_fingerprint,'hex'),
      'invoice_vat_policy_fingerprint',pg_catalog.encode(v_economic.invoice_vat_policy_fingerprint,'hex'),
      'calculation_fingerprint',pg_catalog.encode(v_economic.calculation_fingerprint,'hex')
    )
  );
  v_vat_rate:=case when v_economic.invoice_vat_chargeable
    then v_economic.invoice_vat_rate_pct else 0 end;
  v_vat:=pg_catalog.round((p_invoice_presentation_charge_pence::numeric/100)*(v_vat_rate/100),2);
  v_total:=(p_invoice_presentation_charge_pence::numeric/100)+v_vat;
  if v_invoice_amounts is not null then v_vat:=(v_invoice_amounts->>'vat_pence')::numeric/100; v_total:=(v_invoice_amounts->>'gross_pence')::numeric/100; end if;
  v_hash:=private.weekly_source_billing_movement_hash_v1(
    pg_catalog.jsonb_build_object(
      'transition_id',p_transition_id,'nhsp_upload_row_id',p_nhsp_upload_row_id,
      'final_revision_id',p_final_revision_id,'finalisation_cycle_id',p_finalisation_cycle_id,
      'actual_client_id',v_resolution.client_id,'candidate_id',v_resolution.candidate_id,
      'contract_id',v_resolution.contract_id,'work_event_id',v_resolution.work_event_id,
      'movement_role',p_movement_role,'correction_unit_id',p_correction_unit_id,
      'prior_movement_id',p_prior_movement_id,'source_profile_kind',p_source_profile_kind,
      'source_line_kind',p_source_line_kind,'source_facts',v_source_facts,
      'pay_vector',v_pay_vector,'charge_vector',v_charge_vector,
      'total_pay_pence',v_economic.total_pay_pence,
      'calculated_comparison_charge_pence',v_economic.calculated_charge_pence,
      'source_validation_charge_pence',p_source_validation_charge_pence,
      'invoice_presentation_charge_pence',p_invoice_presentation_charge_pence,
      'vat_rate_pct',v_vat_rate,'vat_amount',v_vat,'total_inc_vat',v_total,
      'price_check_result',p_price_check_result,
      'price_check_fingerprint',case when p_price_check_fingerprint is null then null
        else pg_catalog.encode(p_price_check_fingerprint,'hex') end,
      'charge_acceptance_id',p_charge_acceptance_id,
      'mapping_rate_policy_fingerprint',pg_catalog.encode(v_mapping_fingerprint,'hex'),
      'invoice_timesheet_id',p_invoice_timesheet_id
    )
  );
  insert into public.weekly_source_billing_movements(
    transition_id,nhsp_upload_row_id,final_revision_id,finalisation_cycle_id,
    actual_client_id,candidate_id,contract_id,work_event_id,movement_role,
    correction_unit_id,prior_movement_id,source_profile_kind,source_line_kind,
    source_facts_json,canonical_pay_vector_json,canonical_charge_vector_json,
    total_pay_ex_vat,calculated_comparison_charge_pence,
    source_validation_charge_pence,invoice_presentation_charge_pence,
    vat_rate_pct,vat_amount,total_inc_vat,price_check_result,price_check_fingerprint,
    charge_acceptance_id,mapping_rate_policy_fingerprint,invoice_timesheet_id,original_cycle_key,
    movement_economic_hash,placement_state
  ) values (
    p_transition_id,p_nhsp_upload_row_id,p_final_revision_id,p_finalisation_cycle_id,
    v_resolution.client_id,v_resolution.candidate_id,v_resolution.contract_id,
    v_resolution.work_event_id,p_movement_role,p_correction_unit_id,p_prior_movement_id,
    p_source_profile_kind,p_source_line_kind,v_source_facts,v_pay_vector,v_charge_vector,
    v_economic.total_pay_pence::numeric/100,v_economic.calculated_charge_pence,
    p_source_validation_charge_pence,p_invoice_presentation_charge_pence,
    v_vat_rate,v_vat,v_total,p_price_check_result,p_price_check_fingerprint,
    p_charge_acceptance_id,v_mapping_fingerprint,p_invoice_timesheet_id,p_finalisation_cycle_id::text,
    v_hash,'UNPLACED'
  ) returning id into v_id;
  return v_id;
end;
$function$;

create or replace function private.weekly_source_finalisation_insert_reversal_v1(
  p_transition_id uuid,
  p_final_revision_id uuid,
  p_finalisation_cycle_id uuid,
  p_prior_movement_id uuid,
  p_correction_unit_id uuid,
  p_source_profile_kind text,
  p_source_facts_json jsonb
) returns uuid
language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_prior public.weekly_source_billing_movements%rowtype;
  v_prior_invoice jsonb;
  v_prior_vat numeric;
  v_prior_total numeric;
  v_pay_vector jsonb;
  v_charge_vector jsonb;
  v_hash bytea;
  v_id uuid;
begin
  select * into v_prior
  from public.weekly_source_billing_movements where id=p_prior_movement_id;
  if not found or v_prior.source_profile_kind='NHSP_TRUST_BACKING_REPORT'
     or v_prior.movement_role not in ('POSITIVE','REPLACEMENT')
     or p_transition_id is null or p_final_revision_id is null
     or p_finalisation_cycle_id is null or p_correction_unit_id is null
     or p_source_profile_kind not in ('GENERIC_COMPLETE_SNAPSHOT','HEALTHROSTER_ACTUAL_ROWS')
     or p_source_profile_kind is distinct from v_prior.source_profile_kind
     or pg_catalog.jsonb_typeof(p_source_facts_json)<>'object' then
    raise exception 'WEEKLY_SOURCE_REVERSAL_INPUT_INVALID' using errcode='55000';
  end if;
  v_prior_invoice:=private.weekly_source_invoice_movement_money_v1(v_prior.id);
  v_prior_vat:=(v_prior_invoice->>'vat_pence')::numeric/100; v_prior_total:=(v_prior_invoice->>'gross_pence')::numeric/100;
  v_pay_vector:=private.weekly_source_finalisation_negate_vector_v1(v_prior.canonical_pay_vector_json);
  v_charge_vector:=private.weekly_source_finalisation_negate_vector_v1(v_prior.canonical_charge_vector_json);
  v_hash:=private.weekly_source_billing_movement_hash_v1(
    pg_catalog.jsonb_build_object(
      'transition_id',p_transition_id,'final_revision_id',p_final_revision_id,
      'finalisation_cycle_id',p_finalisation_cycle_id,'prior_movement_id',v_prior.id,
      'correction_unit_id',p_correction_unit_id,'source_profile_kind',p_source_profile_kind,
      'source_line_kind','GENERATED_HISTORICAL_REVERSAL','source_facts',p_source_facts_json,
      'pay_vector',v_pay_vector,'charge_vector',v_charge_vector,
      'total_pay_ex_vat',-v_prior.total_pay_ex_vat,
      'calculated_comparison_charge_pence',-v_prior.calculated_comparison_charge_pence,
      'source_validation_charge_pence',case when v_prior.source_validation_charge_pence is null
        then null else -v_prior.source_validation_charge_pence end,
      'invoice_presentation_charge_pence',-v_prior.invoice_presentation_charge_pence,
      'vat_rate_pct',v_prior.vat_rate_pct,'vat_amount',-v_prior_vat,
      'total_inc_vat',-v_prior_total,
      'invoice_timesheet_id',v_prior.invoice_timesheet_id
    )
  );
  insert into public.weekly_source_billing_movements(
    transition_id,nhsp_upload_row_id,final_revision_id,finalisation_cycle_id,
    actual_client_id,candidate_id,contract_id,work_event_id,movement_role,
    correction_unit_id,prior_movement_id,source_profile_kind,source_line_kind,
    source_facts_json,canonical_pay_vector_json,canonical_charge_vector_json,
    total_pay_ex_vat,calculated_comparison_charge_pence,
    source_validation_charge_pence,invoice_presentation_charge_pence,
    vat_rate_pct,vat_amount,total_inc_vat,price_check_result,price_check_fingerprint,
    mapping_rate_policy_fingerprint,invoice_timesheet_id,original_cycle_key,
    movement_economic_hash,placement_state
  ) values (
    p_transition_id,null,p_final_revision_id,p_finalisation_cycle_id,
    v_prior.actual_client_id,v_prior.candidate_id,v_prior.contract_id,v_prior.work_event_id,
    'REVERSAL',p_correction_unit_id,v_prior.id,p_source_profile_kind,
    'GENERATED_HISTORICAL_REVERSAL',p_source_facts_json,v_pay_vector,v_charge_vector,
    -v_prior.total_pay_ex_vat,-v_prior.calculated_comparison_charge_pence,
    case when v_prior.source_validation_charge_pence is null then null
      else -v_prior.source_validation_charge_pence end,
    -v_prior.invoice_presentation_charge_pence,v_prior.vat_rate_pct,
    -v_prior_vat,-v_prior_total,v_prior.price_check_result,
    v_prior.price_check_fingerprint,v_prior.mapping_rate_policy_fingerprint,
    v_prior.invoice_timesheet_id,v_prior.original_cycle_key,v_hash,'UNPLACED'
  ) returning id into v_id;
  return v_id;
end;
$function$;

create or replace function private.weekly_source_finalisation_materialise_expense_invoice_v1(
  p_final_revision_id uuid,
  p_finalisation_cycle_id uuid,
  p_source_profile_kind text,
  p_expense_authority_generation_id uuid,
  p_prior_expense_authority_generation_id uuid,
  p_expense_policy_snapshot_id uuid,
  p_invoice_timesheet_id uuid,
  p_correction_presentation text
) returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_current public.weekly_expense_authority_generations%rowtype;
  v_prior public.weekly_expense_authority_generations%rowtype;
  v_expense_policy public.weekly_source_row_expense_policy_snapshots%rowtype;
  v_contract public.contracts%rowtype;
  v_event public.weekly_work_events%rowtype;
  v_prior_movement public.weekly_source_billing_movements%rowtype;
  v_revision public.weekly_source_final_revisions%rowtype;
  v_old_pence bigint:=0;
  v_new_pence bigint;
  v_correction_unit_id uuid;
  v_mapping_fingerprint bytea;
  v_reversal_hash bytea;
  v_replacement_hash bytea;
  v_reversal_id uuid;
  v_replacement_id uuid;
  v_reversal_vector jsonb;
  v_replacement_pay_vector jsonb;
  v_replacement_charge_vector jsonb;
  v_vat_rate numeric(5,2);
  v_vat numeric(12,2);
  v_total numeric(12,2);
  v_count integer:=0;
  v_prepared boolean:=false;
begin
  if p_final_revision_id is null or p_finalisation_cycle_id is null
     or p_expense_authority_generation_id is null
     or p_source_profile_kind not in ('GENERIC_COMPLETE_SNAPSHOT','HEALTHROSTER_ACTUAL_ROWS')
     or p_correction_presentation not in ('FULL_REVERSAL_REPLACEMENT','NET_DIFFERENCE_PRESENTATION') then
    raise exception 'WEEKLY_SOURCE_EXPENSE_MOVEMENT_INPUT_INVALID' using errcode='22023';
  end if;

  select * into strict v_revision
  from public.weekly_source_final_revisions
  where id=p_final_revision_id and source_cycle_id=p_finalisation_cycle_id
    and state in ('PREPARED','CURRENT') and authority_scope_kind='CYCLE';
  v_prepared:=v_revision.state='PREPARED';
  select * into strict v_current
  from public.weekly_expense_authority_generations
  where id=p_expense_authority_generation_id and final_revision_id=p_final_revision_id
    and state=case when v_prepared then 'PREPARED' else 'CURRENT' end;
  v_new_pence:=(v_current.client_charge_ex_vat*100)::bigint;
  if v_current.source_expense_pence is distinct from v_new_pence
     or v_current.candidate_reimbursement_ex_vat is distinct from v_current.client_charge_ex_vat
     or v_current.prior_expense_authority_generation_id
          is distinct from p_prior_expense_authority_generation_id
     or v_current.correction_presentation is distinct from p_correction_presentation then
    raise exception 'WEEKLY_SOURCE_EXPENSE_AUTHORITY_INVALID' using errcode='55000';
  end if;

  if p_prior_expense_authority_generation_id is not null then
    select * into strict v_prior
    from public.weekly_expense_authority_generations
    where id=p_prior_expense_authority_generation_id
      and work_event_id=v_current.work_event_id
      and (
        (not v_prepared and generation=v_current.generation-1)
        or (v_prepared and generation<v_current.generation)
      )
      and (
        (not v_prepared and state='SUPERSEDED')
        or (v_prepared and state in ('CURRENT','SUPERSEDED'))
      );
    if v_prepared and exists(
      select 1
      from public.weekly_expense_authority_generations skipped
      where skipped.work_event_id=v_current.work_event_id
        and skipped.generation>v_prior.generation
        and skipped.generation<v_current.generation
        and skipped.final_revision_id is distinct from v_revision.predecessor_revision_id
    ) then
      raise exception 'WEEKLY_SOURCE_PREPARED_EXPENSE_HISTORY_INVALID'
        using errcode='55000';
    end if;
    v_old_pence:=(v_prior.client_charge_ex_vat*100)::bigint;
    if v_prior.source_expense_pence is distinct from v_old_pence
       or v_prior.candidate_reimbursement_ex_vat is distinct from v_prior.client_charge_ex_vat then
      raise exception 'WEEKLY_SOURCE_PRIOR_EXPENSE_AUTHORITY_INVALID' using errcode='55000';
    end if;
  elsif v_current.generation<>1 then
    if not v_prepared or exists(
      select 1
      from public.weekly_expense_authority_generations skipped
      where skipped.work_event_id=v_current.work_event_id
        and skipped.generation<v_current.generation
        and skipped.final_revision_id is distinct from v_revision.predecessor_revision_id
    ) then
      raise exception 'WEEKLY_SOURCE_PRIOR_EXPENSE_AUTHORITY_MISSING' using errcode='55000';
    end if;
  end if;

  if v_new_pence=v_old_pence and (
       p_prior_expense_authority_generation_id is null
       or (v_current.source_expense_vat_enabled is not distinct from v_prior.source_expense_vat_enabled
           and v_current.contract_id is not distinct from v_prior.contract_id)
     ) then
    return pg_catalog.jsonb_build_object(
      'movement_count',0,'primary_movement_id',null,'zero_noop',true
    );
  end if;
  v_correction_unit_id:=case when v_old_pence>0 then pg_catalog.gen_random_uuid() end;

  if v_old_pence>0 then
    select * into v_prior_movement
    from public.weekly_source_billing_movements movement
    where movement.expense_authority_generation_id=p_prior_expense_authority_generation_id
      and movement.source_line_kind='SOURCE_FIXED_EXPENSE'
      and movement.movement_role in ('EXPENSE_POSITIVE','EXPENSE_REPLACEMENT')
      and movement.invoice_presentation_charge_pence=v_old_pence
    order by movement.created_at_utc desc,movement.id desc
    limit 1;
    if not found then
      raise exception 'WEEKLY_SOURCE_PRIOR_EXPENSE_MOVEMENT_MISSING' using errcode='55000';
    end if;
    v_reversal_vector:=pg_catalog.jsonb_build_object(
      'schema_version','WEEKLY_SOURCE_FIXED_EXPENSE_VECTOR_V1',
      'kind','PAY_AND_CHARGE','source_expense_pence',-v_old_pence,
      'source_expense_vat_enabled',v_prior.source_expense_vat_enabled
    );
    v_mapping_fingerprint:=private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_FIXED_EXPENSE_MAPPING_V1',
      pg_catalog.jsonb_build_object(
        'current_expense_authority_generation_id',v_current.id,
        'prior_expense_authority_generation_id',v_prior.id,
        'prior_movement_id',v_prior_movement.id,
        'role','EXPENSE_REVERSAL'
      )
    );
    v_reversal_hash:=private.weekly_source_billing_movement_hash_v1(
      pg_catalog.jsonb_build_object(
        'expense_authority_generation_id',v_current.id,
        'final_revision_id',p_final_revision_id,
        'finalisation_cycle_id',p_finalisation_cycle_id,
        'actual_client_id',v_prior_movement.actual_client_id,
        'candidate_id',v_prior_movement.candidate_id,
        'contract_id',v_prior_movement.contract_id,
        'work_event_id',v_prior_movement.work_event_id,
        'movement_role','EXPENSE_REVERSAL','correction_unit_id',v_correction_unit_id,
        'prior_movement_id',v_prior_movement.id,
        'source_profile_kind',p_source_profile_kind,
        'source_line_kind','SOURCE_FIXED_EXPENSE',
        'source_facts',pg_catalog.jsonb_build_object(
          'schema_version','WEEKLY_SOURCE_FIXED_EXPENSE_FACTS_V1',
          'correction_presentation',p_correction_presentation,
          'old_expense_pence',v_old_pence,'new_expense_pence',v_new_pence,
          'authority_generation_id',v_current.id,
          'prior_authority_generation_id',v_prior.id,
          'row_expense_policy_snapshot_id',v_current.row_expense_policy_snapshot_id,
          'source_observation_kind',v_current.source_observation_kind
        ),
        'vector',v_reversal_vector,'invoice_presentation_charge_pence',-v_old_pence,
        'vat_rate_pct',v_prior_movement.vat_rate_pct,
        'vat_amount',-v_prior_movement.vat_amount,
        'total_inc_vat',-v_prior_movement.total_inc_vat,
        'mapping_rate_policy_fingerprint',pg_catalog.encode(v_mapping_fingerprint,'hex'),
        'invoice_timesheet_id',v_prior_movement.invoice_timesheet_id
      )
    );
    insert into public.weekly_source_billing_movements(
      transition_id,nhsp_upload_row_id,expense_authority_generation_id,
      final_revision_id,finalisation_cycle_id,actual_client_id,candidate_id,
      contract_id,work_event_id,movement_role,correction_unit_id,prior_movement_id,
      source_profile_kind,source_line_kind,source_facts_json,
      canonical_pay_vector_json,canonical_charge_vector_json,total_pay_ex_vat,
      calculated_comparison_charge_pence,source_validation_charge_pence,
      invoice_presentation_charge_pence,vat_rate_pct,vat_amount,total_inc_vat,
      price_check_result,price_check_fingerprint,mapping_rate_policy_fingerprint,
      invoice_timesheet_id,original_cycle_key,movement_economic_hash,placement_state
    ) values (
      null,null,v_current.id,p_final_revision_id,p_finalisation_cycle_id,
      v_prior_movement.actual_client_id,v_prior_movement.candidate_id,
      v_prior_movement.contract_id,v_prior_movement.work_event_id,
      'EXPENSE_REVERSAL',v_correction_unit_id,v_prior_movement.id,
      p_source_profile_kind,'SOURCE_FIXED_EXPENSE',
      pg_catalog.jsonb_build_object(
        'schema_version','WEEKLY_SOURCE_FIXED_EXPENSE_FACTS_V1',
        'correction_presentation',p_correction_presentation,
        'old_expense_pence',v_old_pence,'new_expense_pence',v_new_pence,
        'authority_generation_id',v_current.id,
        'prior_authority_generation_id',v_prior.id,
        'row_expense_policy_snapshot_id',v_current.row_expense_policy_snapshot_id,
        'source_observation_kind',v_current.source_observation_kind
      ),v_reversal_vector,v_reversal_vector,-v_old_pence::numeric/100,
      -v_old_pence,-v_old_pence,-v_old_pence,v_prior_movement.vat_rate_pct,
      -v_prior_movement.vat_amount,-v_prior_movement.total_inc_vat,
      'NOT_APPLICABLE',null,v_mapping_fingerprint,
      v_prior_movement.invoice_timesheet_id,p_finalisation_cycle_id::text,
      v_reversal_hash,'UNPLACED'
    ) returning id into v_reversal_id;
    v_count:=v_count+1;
  end if;

  if v_new_pence>0 then
    if p_expense_policy_snapshot_id is null or p_invoice_timesheet_id is null
       or v_current.source_observation_kind<>'ROW_PRESENT'
       or v_current.row_expense_policy_snapshot_id is distinct from p_expense_policy_snapshot_id then
      raise exception 'WEEKLY_SOURCE_EXPENSE_POLICY_SNAPSHOT_MISSING' using errcode='55000';
    end if;
    select * into strict v_expense_policy
    from public.weekly_source_row_expense_policy_snapshots
    where id=p_expense_policy_snapshot_id;
    select * into strict v_contract
    from public.contracts where id=v_current.contract_id;
    select * into strict v_event
    from public.weekly_work_events where id=v_current.work_event_id;
    if v_expense_policy.work_event_id is distinct from v_current.work_event_id
       or v_expense_policy.contract_id is distinct from v_current.contract_id
       or v_expense_policy.source_expense_pence is distinct from v_current.source_expense_pence
       or v_contract.id is distinct from v_expense_policy.contract_id
       or v_contract.client_id is distinct from v_expense_policy.client_id
       or v_contract.candidate_id is distinct from v_expense_policy.candidate_id
       or v_event.client_id is distinct from v_expense_policy.client_id
       or v_event.candidate_id is distinct from v_expense_policy.candidate_id
       or v_expense_policy.source_expense_vat_enabled is distinct from v_current.source_expense_vat_enabled then
      raise exception 'WEEKLY_SOURCE_EXPENSE_POLICY_SNAPSHOT_INVALID' using errcode='55000';
    end if;
    v_vat_rate:=case when v_current.source_expense_vat_enabled
      then v_expense_policy.invoice_vat_rate_pct else 0 end;
    v_vat:=pg_catalog.round((v_new_pence::numeric/100)*(v_vat_rate/100),2);
    v_total:=v_new_pence::numeric/100+v_vat;
    v_replacement_pay_vector:=pg_catalog.jsonb_build_object(
      'schema_version','WEEKLY_SOURCE_FIXED_EXPENSE_VECTOR_V1','kind','PAY',
      'source_expense_pence',v_new_pence,
      'source_expense_vat_enabled',v_current.source_expense_vat_enabled
    );
    v_replacement_charge_vector:=v_replacement_pay_vector||pg_catalog.jsonb_build_object('kind','CHARGE');
    v_mapping_fingerprint:=private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_FIXED_EXPENSE_MAPPING_V1',
      pg_catalog.jsonb_build_object(
        'current_expense_authority_generation_id',v_current.id,
        'prior_expense_authority_generation_id',p_prior_expense_authority_generation_id,
        'expense_policy_snapshot_id',v_expense_policy.id,
        'invoice_vat_policy_fingerprint',pg_catalog.encode(v_expense_policy.invoice_vat_policy_fingerprint,'hex'),
        'role',case when v_old_pence=0 then 'EXPENSE_POSITIVE' else 'EXPENSE_REPLACEMENT' end
      )
    );
    v_replacement_hash:=private.weekly_source_billing_movement_hash_v1(
      pg_catalog.jsonb_build_object(
        'expense_authority_generation_id',v_current.id,
        'final_revision_id',p_final_revision_id,
        'finalisation_cycle_id',p_finalisation_cycle_id,
        'actual_client_id',v_expense_policy.client_id,'candidate_id',v_expense_policy.candidate_id,
        'contract_id',v_expense_policy.contract_id,'work_event_id',v_expense_policy.work_event_id,
        'movement_role',case when v_old_pence=0 then 'EXPENSE_POSITIVE' else 'EXPENSE_REPLACEMENT' end,
        'correction_unit_id',v_correction_unit_id,'prior_movement_id',v_prior_movement.id,
        'source_profile_kind',p_source_profile_kind,'source_line_kind','SOURCE_FIXED_EXPENSE',
        'source_facts',pg_catalog.jsonb_build_object(
          'schema_version','WEEKLY_SOURCE_FIXED_EXPENSE_FACTS_V1',
          'correction_presentation',p_correction_presentation,
          'old_expense_pence',v_old_pence,'new_expense_pence',v_new_pence,
          'authority_generation_id',v_current.id,
          'prior_authority_generation_id',p_prior_expense_authority_generation_id,
          'row_expense_policy_snapshot_id',v_current.row_expense_policy_snapshot_id,
          'source_observation_kind',v_current.source_observation_kind
        ),
        'pay_vector',v_replacement_pay_vector,'charge_vector',v_replacement_charge_vector,
        'invoice_presentation_charge_pence',v_new_pence,'vat_rate_pct',v_vat_rate,
        'vat_amount',v_vat,'total_inc_vat',v_total,
        'mapping_rate_policy_fingerprint',pg_catalog.encode(v_mapping_fingerprint,'hex'),
        'invoice_timesheet_id',p_invoice_timesheet_id
      )
    );
    insert into public.weekly_source_billing_movements(
      transition_id,nhsp_upload_row_id,expense_authority_generation_id,
      final_revision_id,finalisation_cycle_id,actual_client_id,candidate_id,
      contract_id,work_event_id,movement_role,correction_unit_id,prior_movement_id,
      source_profile_kind,source_line_kind,source_facts_json,
      canonical_pay_vector_json,canonical_charge_vector_json,total_pay_ex_vat,
      calculated_comparison_charge_pence,source_validation_charge_pence,
      invoice_presentation_charge_pence,vat_rate_pct,vat_amount,total_inc_vat,
      price_check_result,price_check_fingerprint,mapping_rate_policy_fingerprint,
      invoice_timesheet_id,original_cycle_key,movement_economic_hash,placement_state
    ) values (
      null,null,v_current.id,p_final_revision_id,p_finalisation_cycle_id,
      v_expense_policy.client_id,v_expense_policy.candidate_id,v_expense_policy.contract_id,
      v_expense_policy.work_event_id,
      case when v_old_pence=0 then 'EXPENSE_POSITIVE' else 'EXPENSE_REPLACEMENT' end,
      v_correction_unit_id,case when v_old_pence>0 then v_prior_movement.id end,
      p_source_profile_kind,'SOURCE_FIXED_EXPENSE',
      pg_catalog.jsonb_build_object(
        'schema_version','WEEKLY_SOURCE_FIXED_EXPENSE_FACTS_V1',
        'correction_presentation',p_correction_presentation,
        'old_expense_pence',v_old_pence,'new_expense_pence',v_new_pence,
        'authority_generation_id',v_current.id,
        'prior_authority_generation_id',p_prior_expense_authority_generation_id,
        'row_expense_policy_snapshot_id',v_current.row_expense_policy_snapshot_id,
        'source_observation_kind',v_current.source_observation_kind
      ),v_replacement_pay_vector,v_replacement_charge_vector,v_new_pence::numeric/100,
      v_new_pence,v_new_pence,v_new_pence,v_vat_rate,v_vat,v_total,
      'NOT_APPLICABLE',null,v_mapping_fingerprint,p_invoice_timesheet_id,
      p_finalisation_cycle_id::text,v_replacement_hash,'UNPLACED'
    ) returning id into v_replacement_id;
    v_count:=v_count+1;
  end if;

  return pg_catalog.jsonb_build_object(
    'movement_count',v_count,
    'reversal_movement_id',v_reversal_id,
    'replacement_movement_id',v_replacement_id,
    'primary_movement_id',coalesce(v_replacement_id,v_reversal_id),
    'zero_noop',false
  );
end;
$function$;

create or replace function private.weekly_source_ordinary_projection_expense_movement_assert_v1(
  p_billing_movement_id uuid,
  p_final_revision_id uuid,
  p_finalisation_cycle_id uuid,
  p_client_manifest_id uuid,
  p_root_timesheet_id uuid
) returns boolean
language plpgsql stable security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_movement public.weekly_source_billing_movements%rowtype;
  v_authority public.weekly_expense_authority_generations%rowtype;
  v_prior_authority public.weekly_expense_authority_generations%rowtype;
  v_policy public.weekly_source_row_expense_policy_snapshots%rowtype;
  v_resolution public.weekly_source_row_resolutions%rowtype;
  v_source_row public.weekly_source_upload_rows%rowtype;
  v_prior_movement public.weekly_source_billing_movements%rowtype;
  v_event public.weekly_work_events%rowtype;
  v_contract public.contracts%rowtype;
  v_old_pence bigint:=0;
  v_new_pence bigint;
  v_expected_facts jsonb;
  v_expected_pay_vector jsonb;
  v_expected_charge_vector jsonb;
  v_expected_mapping_fingerprint bytea;
  v_expected_movement_hash bytea;
  v_expected_authority_hash bytea;
  v_expected_policy_snapshot_hash bytea;
  v_expected_vat_rate numeric:=0;
  v_expected_vat numeric:=0;
  v_expected_total_inc_vat numeric:=0;
  v_expected_role text;
  -- WP-30 (WP-27 sweep finding N6), standing rule 3.  The lineage assert below
  -- keyed the source-row binding on the physical root id, so a legitimate
  -- rotated root raised WEEKLY_SOURCE_EXPENSE_POLICY_LINEAGE_INVALID instead of
  -- projecting.  It fails CLOSED, so it is a correctness defect rather than a
  -- money risk, and it moves to the family with the rest of the class.  One
  -- resolution, through the one installed adapter.
  v_family uuid[];
begin
  if p_billing_movement_id is null or p_final_revision_id is null
     or p_finalisation_cycle_id is null or p_client_manifest_id is null
     or p_root_timesheet_id is null then
    raise exception 'WEEKLY_SOURCE_EXPENSE_MOVEMENT_ASSERT_INPUT_INVALID'
      using errcode='22023';
  end if;
  v_family:=private.weekly_source_invoice_family_timesheet_ids_v1(p_root_timesheet_id);
  -- Standing rule 3's fail-closed branch: an explicit cardinality test, never a
  -- `limit`.  This assert already refuses on every other unprovable fact.
  if v_family is null or pg_catalog.cardinality(v_family)=0 then
    raise exception 'WEEKLY_SOURCE_EXPENSE_POLICY_LINEAGE_FAMILY_UNRESOLVED'
      using errcode='55000';
  end if;
  select * into strict v_movement
  from public.weekly_source_billing_movements movement
  where movement.id=p_billing_movement_id;
  if v_movement.final_revision_id is distinct from p_final_revision_id
     or v_movement.finalisation_cycle_id is distinct from p_finalisation_cycle_id
     or v_movement.invoice_timesheet_id is distinct from p_root_timesheet_id
     or v_movement.transition_id is not null
     or v_movement.nhsp_upload_row_id is not null
     or v_movement.expense_authority_generation_id is null
     or v_movement.source_profile_kind<>'GENERIC_COMPLETE_SNAPSHOT'
     or v_movement.source_line_kind<>'SOURCE_FIXED_EXPENSE'
     or v_movement.movement_role not in (
       'EXPENSE_POSITIVE','EXPENSE_REVERSAL','EXPENSE_REPLACEMENT'
     )
     or v_movement.price_check_result<>'NOT_APPLICABLE'
     or v_movement.price_check_fingerprint is not null
     or v_movement.original_cycle_key is distinct from p_finalisation_cycle_id::text
     or not exists(
       select 1 from public.weekly_source_manifest_movements manifest_movement
       where manifest_movement.client_manifest_id=p_client_manifest_id
         and manifest_movement.billing_movement_id=v_movement.id
         and manifest_movement.movement_hash=v_movement.movement_economic_hash
     ) then
    raise exception 'WEEKLY_SOURCE_EXPENSE_MOVEMENT_INTEGRITY_FAILED'
      using errcode='55000';
  end if;

  select * into strict v_authority
  from public.weekly_expense_authority_generations authority
  where authority.id=v_movement.expense_authority_generation_id;
  select * into strict v_event
  from public.weekly_work_events event_row where event_row.id=v_authority.work_event_id;
  select * into strict v_contract
  from public.contracts contract_row where contract_row.id=v_authority.contract_id;
  v_new_pence:=v_authority.source_expense_pence;
  if v_authority.final_revision_id is distinct from p_final_revision_id
     or not (
       (v_authority.state='CURRENT' and not exists(
         select 1 from public.weekly_expense_authority_generations later_authority
         where later_authority.prior_expense_authority_generation_id=v_authority.id
       ))
       or
       (v_authority.state='SUPERSEDED' and exists(
         select 1 from public.weekly_expense_authority_generations later_authority
         where later_authority.prior_expense_authority_generation_id=v_authority.id
           and later_authority.work_event_id=v_authority.work_event_id
           and later_authority.generation=v_authority.generation+1
       ))
     )
     or v_authority.work_event_id is distinct from v_movement.work_event_id
     or v_authority.candidate_reimbursement_ex_vat
          is distinct from v_new_pence::numeric/100
     or v_authority.client_charge_ex_vat
          is distinct from v_new_pence::numeric/100
     or v_contract.candidate_id is distinct from v_event.candidate_id
     or v_contract.client_id is distinct from v_event.client_id then
    raise exception 'WEEKLY_SOURCE_EXPENSE_AUTHORITY_INTEGRITY_FAILED'
      using errcode='55000';
  end if;
  if v_authority.prior_expense_authority_generation_id is not null then
    select * into strict v_prior_authority
    from public.weekly_expense_authority_generations authority
    where authority.id=v_authority.prior_expense_authority_generation_id
      and authority.work_event_id=v_authority.work_event_id
      and authority.generation=v_authority.generation-1
      and authority.state='SUPERSEDED';
    v_old_pence:=v_prior_authority.source_expense_pence;
    if v_prior_authority.candidate_reimbursement_ex_vat
          is distinct from v_old_pence::numeric/100
       or v_prior_authority.client_charge_ex_vat
          is distinct from v_old_pence::numeric/100 then
      raise exception 'WEEKLY_SOURCE_PRIOR_EXPENSE_AUTHORITY_INTEGRITY_FAILED'
        using errcode='55000';
    end if;
  elsif v_authority.generation<>1 then
    raise exception 'WEEKLY_SOURCE_PRIOR_EXPENSE_AUTHORITY_MISSING'
      using errcode='55000';
  end if;
  v_expected_authority_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_EXPENSE_AUTHORITY_GENERATION_V1',
    pg_catalog.jsonb_build_object(
      'final_revision_id',v_authority.final_revision_id,
      'work_event_id',v_authority.work_event_id,
      'contract_id',v_authority.contract_id,
      'row_expense_policy_snapshot_id',v_authority.row_expense_policy_snapshot_id,
      'prior_expense_authority_generation_id',
        v_authority.prior_expense_authority_generation_id,
      'generation',v_authority.generation,
      'source_observation_kind',v_authority.source_observation_kind,
      'correction_presentation',v_authority.correction_presentation,
      'source_expense_pence',v_authority.source_expense_pence,
      'source_expense_vat_enabled',v_authority.source_expense_vat_enabled
    )
  );
  if v_authority.authority_hash is distinct from v_expected_authority_hash then
    raise exception 'WEEKLY_SOURCE_EXPENSE_AUTHORITY_HASH_MISMATCH'
      using errcode='55000';
  end if;

  if v_authority.source_observation_kind='ROW_PRESENT' then
    select * into strict v_policy
    from public.weekly_source_row_expense_policy_snapshots policy
    where policy.id=v_authority.row_expense_policy_snapshot_id;
    select resolution.* into strict v_resolution
    from public.weekly_source_row_resolutions resolution
    where resolution.id=v_policy.row_resolution_id;
    select source_row.* into strict v_source_row
    from public.weekly_source_upload_rows source_row
    join public.weekly_source_uploads upload_row
      on upload_row.id=source_row.upload_id
     and upload_row.source_cycle_id=p_finalisation_cycle_id
    where source_row.id=v_resolution.upload_row_id;
    v_expected_policy_snapshot_hash:=private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_ROW_EXPENSE_POLICY_SNAPSHOT_V1',
      pg_catalog.jsonb_build_object(
        'row_resolution_id',v_resolution.id,
        'upload_row_id',v_source_row.id,
        'generation',v_resolution.generation,
        'work_event_id',v_resolution.work_event_id,
        'candidate_id',v_resolution.candidate_id,
        'client_id',v_resolution.client_id,
        'contract_id',v_resolution.contract_id,
        'row_finalisation_state',v_source_row.row_finalisation_state,
        'source_expense_pence',v_source_row.source_expense_pence,
        'source_expense_parse_state',v_source_row.source_expense_parse_state,
        'source_expense_vat_enabled',v_policy.source_expense_vat_enabled,
        'invoice_vat_chargeable',v_policy.invoice_vat_chargeable,
        'invoice_vat_rate_pct',v_policy.invoice_vat_rate_pct,
        'correction_presentation',v_policy.correction_presentation,
        'effective_policy_fingerprint',
          pg_catalog.encode(v_policy.effective_policy_fingerprint,'hex'),
        'invoice_vat_policy_fingerprint',
          pg_catalog.encode(v_policy.invoice_vat_policy_fingerprint,'hex')
      )
    );
    -- The final revision freezes the policy used at finalisation.  Recompute
    -- that immutable snapshot; do not re-resolve today's settings, because a
    -- later-dated policy row must not retroactively invalidate a frozen source
    -- cycle before it reaches the ordinary Timesheet projection.
    if v_resolution.mapping_state<>'RESOLVED'
       or v_policy.upload_row_id is distinct from v_source_row.id
       or v_policy.generation is distinct from v_resolution.generation
       or v_policy.work_event_id is distinct from v_authority.work_event_id
       or v_policy.contract_id is distinct from v_authority.contract_id
       or v_policy.candidate_id is distinct from v_event.candidate_id
       or v_policy.client_id is distinct from v_event.client_id
       or v_resolution.work_event_id is distinct from v_policy.work_event_id
       or v_resolution.candidate_id is distinct from v_policy.candidate_id
       or v_resolution.client_id is distinct from v_policy.client_id
       or v_resolution.contract_id is distinct from v_policy.contract_id
       or v_resolution.effective_policy_fingerprint
            is distinct from v_policy.effective_policy_fingerprint
       or v_source_row.row_finalisation_state not in (
            'NOT_APPLICABLE','SOURCE_WORKED','SOURCE_ABSENT_ZERO'
          )
       or v_policy.source_expense_pence is distinct from v_new_pence
       or v_policy.source_expense_pence is distinct from v_source_row.source_expense_pence
       or v_policy.source_expense_parse_state
            is distinct from v_source_row.source_expense_parse_state
       or v_policy.source_expense_vat_enabled
            is distinct from v_authority.source_expense_vat_enabled
       or v_policy.correction_presentation
            is distinct from v_authority.correction_presentation
       or (
         v_source_row.row_finalisation_state='SOURCE_ABSENT_ZERO'
         and exists(
           select 1
           from public.weekly_source_row_economic_snapshots economic
           where economic.row_resolution_id=v_resolution.id
         )
       )
       or (
         v_policy.source_expense_pence>0
         and (
           v_source_row.start_at_local is null
           or v_source_row.end_at_local is null
           or v_source_row.end_at_local<=v_source_row.start_at_local
           or v_source_row.break_minutes is null
         )
       )
       or v_policy.snapshot_hash is distinct from v_expected_policy_snapshot_hash
       or (
         v_new_pence>0
         and not exists(
           select 1
           from public.weekly_source_row_timesheet_lineages lineage
           where lineage.row_resolution_id=v_policy.row_resolution_id
             and lineage.work_event_id=v_policy.work_event_id
             and lineage.contract_id=v_policy.contract_id
             and lineage.timesheet_id=any(v_family)
         )
       ) then
      raise exception 'WEEKLY_SOURCE_EXPENSE_POLICY_LINEAGE_INVALID'
        using errcode='55000';
    end if;
  elsif v_authority.source_observation_kind='OMITTED_IN_COMPLETE_COVERAGE' then
    if v_authority.row_expense_policy_snapshot_id is not null
       or v_authority.prior_expense_authority_generation_id is null
       or v_new_pence<>0 then
      raise exception 'WEEKLY_SOURCE_EXPENSE_OMISSION_INVALID'
        using errcode='55000';
    end if;
  else
    raise exception 'WEEKLY_SOURCE_EXPENSE_OBSERVATION_KIND_INVALID'
      using errcode='55000';
  end if;

  if v_old_pence>0 then
    select * into strict v_prior_movement
    from public.weekly_source_billing_movements movement
    where movement.expense_authority_generation_id=v_prior_authority.id
      and movement.source_line_kind='SOURCE_FIXED_EXPENSE'
      and movement.movement_role in ('EXPENSE_POSITIVE','EXPENSE_REPLACEMENT')
      and movement.invoice_presentation_charge_pence=v_old_pence
    order by movement.created_at_utc desc,movement.id desc
    limit 1;
  end if;
  v_expected_facts:=pg_catalog.jsonb_build_object(
    'schema_version','WEEKLY_SOURCE_FIXED_EXPENSE_FACTS_V1',
    'correction_presentation',v_authority.correction_presentation,
    'old_expense_pence',v_old_pence,'new_expense_pence',v_new_pence,
    'authority_generation_id',v_authority.id,
    'prior_authority_generation_id',v_authority.prior_expense_authority_generation_id,
    'row_expense_policy_snapshot_id',v_authority.row_expense_policy_snapshot_id,
    'source_observation_kind',v_authority.source_observation_kind
  );
  if v_movement.source_facts_json is distinct from v_expected_facts then
    raise exception 'WEEKLY_SOURCE_EXPENSE_MOVEMENT_FACTS_MISMATCH'
      using errcode='55000';
  end if;

  if v_movement.movement_role='EXPENSE_REVERSAL' then
    if v_old_pence<=0 or v_movement.prior_movement_id is distinct from v_prior_movement.id
       or v_movement.actual_client_id is distinct from v_prior_movement.actual_client_id
       or v_movement.candidate_id is distinct from v_prior_movement.candidate_id
       or v_movement.contract_id is distinct from v_prior_movement.contract_id
       or v_movement.work_event_id is distinct from v_prior_movement.work_event_id then
      raise exception 'WEEKLY_SOURCE_EXPENSE_REVERSAL_LINEAGE_INVALID'
        using errcode='55000';
    end if;
    v_expected_pay_vector:=pg_catalog.jsonb_build_object(
      'schema_version','WEEKLY_SOURCE_FIXED_EXPENSE_VECTOR_V1',
      'kind','PAY_AND_CHARGE','source_expense_pence',-v_old_pence,
      'source_expense_vat_enabled',v_prior_authority.source_expense_vat_enabled
    );
    v_expected_charge_vector:=v_expected_pay_vector;
    v_expected_vat_rate:=v_prior_movement.vat_rate_pct;
    v_expected_vat:=-v_prior_movement.vat_amount;
    v_expected_total_inc_vat:=-v_prior_movement.total_inc_vat;
    v_expected_mapping_fingerprint:=private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_FIXED_EXPENSE_MAPPING_V1',
      pg_catalog.jsonb_build_object(
        'current_expense_authority_generation_id',v_authority.id,
        'prior_expense_authority_generation_id',v_prior_authority.id,
        'prior_movement_id',v_prior_movement.id,'role','EXPENSE_REVERSAL'
      )
    );
    v_expected_movement_hash:=private.weekly_source_billing_movement_hash_v1(
      pg_catalog.jsonb_build_object(
        'expense_authority_generation_id',v_authority.id,
        'final_revision_id',p_final_revision_id,
        'finalisation_cycle_id',p_finalisation_cycle_id,
        'actual_client_id',v_movement.actual_client_id,
        'candidate_id',v_movement.candidate_id,
        'contract_id',v_movement.contract_id,
        'work_event_id',v_movement.work_event_id,
        'movement_role','EXPENSE_REVERSAL',
        'correction_unit_id',v_movement.correction_unit_id,
        'prior_movement_id',v_movement.prior_movement_id,
        'source_profile_kind',v_movement.source_profile_kind,
        'source_line_kind','SOURCE_FIXED_EXPENSE',
        'source_facts',v_movement.source_facts_json,
        'vector',v_movement.canonical_pay_vector_json,
        'invoice_presentation_charge_pence',
          v_movement.invoice_presentation_charge_pence,
        'vat_rate_pct',v_movement.vat_rate_pct,
        'vat_amount',v_movement.vat_amount,
        'total_inc_vat',v_movement.total_inc_vat,
        'mapping_rate_policy_fingerprint',
          pg_catalog.encode(v_movement.mapping_rate_policy_fingerprint,'hex'),
        'invoice_timesheet_id',v_movement.invoice_timesheet_id
      )
    );
    if v_movement.total_pay_ex_vat is distinct from -v_old_pence::numeric/100
       or v_movement.calculated_comparison_charge_pence is distinct from -v_old_pence
       or v_movement.source_validation_charge_pence is distinct from -v_old_pence
       or v_movement.invoice_presentation_charge_pence is distinct from -v_old_pence then
      raise exception 'WEEKLY_SOURCE_EXPENSE_REVERSAL_ECONOMICS_INVALID'
        using errcode='55000';
    end if;
  else
    v_expected_role:=case when v_old_pence=0
      then 'EXPENSE_POSITIVE' else 'EXPENSE_REPLACEMENT' end;
    if v_new_pence<=0 or v_movement.movement_role<>v_expected_role
       or v_movement.actual_client_id is distinct from v_policy.client_id
       or v_movement.candidate_id is distinct from v_policy.candidate_id
       or v_movement.contract_id is distinct from v_policy.contract_id
       or v_movement.work_event_id is distinct from v_policy.work_event_id
       or v_movement.prior_movement_id is distinct from
            (case when v_old_pence>0 then v_prior_movement.id end) then
      raise exception 'WEEKLY_SOURCE_EXPENSE_POSITIVE_LINEAGE_INVALID'
        using errcode='55000';
    end if;
    v_expected_pay_vector:=pg_catalog.jsonb_build_object(
      'schema_version','WEEKLY_SOURCE_FIXED_EXPENSE_VECTOR_V1','kind','PAY',
      'source_expense_pence',v_new_pence,
      'source_expense_vat_enabled',v_authority.source_expense_vat_enabled
    );
    v_expected_charge_vector:=v_expected_pay_vector
      ||pg_catalog.jsonb_build_object('kind','CHARGE');
    v_expected_vat_rate:=case when v_authority.source_expense_vat_enabled
      then v_policy.invoice_vat_rate_pct else 0 end;
    v_expected_vat:=pg_catalog.round(
      (v_new_pence::numeric/100)*(v_expected_vat_rate/100),2
    );
    v_expected_total_inc_vat:=v_new_pence::numeric/100+v_expected_vat;
    v_expected_mapping_fingerprint:=private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_FIXED_EXPENSE_MAPPING_V1',
      pg_catalog.jsonb_build_object(
        'current_expense_authority_generation_id',v_authority.id,
        'prior_expense_authority_generation_id',
          v_authority.prior_expense_authority_generation_id,
        'expense_policy_snapshot_id',v_policy.id,
        'invoice_vat_policy_fingerprint',
          pg_catalog.encode(v_policy.invoice_vat_policy_fingerprint,'hex'),
        'role',v_expected_role
      )
    );
    v_expected_movement_hash:=private.weekly_source_billing_movement_hash_v1(
      pg_catalog.jsonb_build_object(
        'expense_authority_generation_id',v_authority.id,
        'final_revision_id',p_final_revision_id,
        'finalisation_cycle_id',p_finalisation_cycle_id,
        'actual_client_id',v_movement.actual_client_id,
        'candidate_id',v_movement.candidate_id,
        'contract_id',v_movement.contract_id,
        'work_event_id',v_movement.work_event_id,
        'movement_role',v_movement.movement_role,
        'correction_unit_id',v_movement.correction_unit_id,
        'prior_movement_id',v_movement.prior_movement_id,
        'source_profile_kind',v_movement.source_profile_kind,
        'source_line_kind','SOURCE_FIXED_EXPENSE',
        'source_facts',v_movement.source_facts_json,
        'pay_vector',v_movement.canonical_pay_vector_json,
        'charge_vector',v_movement.canonical_charge_vector_json,
        'invoice_presentation_charge_pence',
          v_movement.invoice_presentation_charge_pence,
        'vat_rate_pct',v_movement.vat_rate_pct,
        'vat_amount',v_movement.vat_amount,
        'total_inc_vat',v_movement.total_inc_vat,
        'mapping_rate_policy_fingerprint',
          pg_catalog.encode(v_movement.mapping_rate_policy_fingerprint,'hex'),
        'invoice_timesheet_id',v_movement.invoice_timesheet_id
      )
    );
    if v_movement.total_pay_ex_vat is distinct from v_new_pence::numeric/100
       or v_movement.calculated_comparison_charge_pence is distinct from v_new_pence
       or v_movement.source_validation_charge_pence is distinct from v_new_pence
       or v_movement.invoice_presentation_charge_pence is distinct from v_new_pence then
      raise exception 'WEEKLY_SOURCE_EXPENSE_POSITIVE_ECONOMICS_INVALID'
        using errcode='55000';
    end if;
  end if;

  if v_movement.canonical_pay_vector_json is distinct from v_expected_pay_vector
     or v_movement.canonical_charge_vector_json is distinct from v_expected_charge_vector
     or v_movement.vat_rate_pct is distinct from v_expected_vat_rate
     or v_movement.vat_amount is distinct from v_expected_vat
     or v_movement.total_inc_vat is distinct from v_expected_total_inc_vat
     or v_movement.mapping_rate_policy_fingerprint
          is distinct from v_expected_mapping_fingerprint
     or v_movement.movement_economic_hash is distinct from v_expected_movement_hash then
    raise exception 'WEEKLY_SOURCE_EXPENSE_MOVEMENT_ECONOMIC_HASH_MISMATCH'
      using errcode='55000',detail=pg_catalog.jsonb_build_object(
        'movement_id',v_movement.id,
        'pay_vector_match',v_movement.canonical_pay_vector_json=v_expected_pay_vector,
        'charge_vector_match',v_movement.canonical_charge_vector_json=v_expected_charge_vector,
        'vat_rate_match',v_movement.vat_rate_pct=v_expected_vat_rate,
        'vat_match',v_movement.vat_amount=v_expected_vat,
        'total_inc_vat_match',v_movement.total_inc_vat=v_expected_total_inc_vat,
        'mapping_fingerprint_match',v_movement.mapping_rate_policy_fingerprint=
          v_expected_mapping_fingerprint,
        'movement_hash_match',v_movement.movement_economic_hash=v_expected_movement_hash,
        'actual_movement_hash',pg_catalog.encode(v_movement.movement_economic_hash,'hex'),
        'expected_movement_hash',pg_catalog.encode(v_expected_movement_hash,'hex')
      )::text;
  end if;
  return true;
exception
  when no_data_found or too_many_rows then
    raise exception 'WEEKLY_SOURCE_EXPENSE_MOVEMENT_SCOPE_INVALID'
      using errcode='55000';
end;
$function$;

create or replace function private.weekly_source_finalise_engine_v1(
  p_request jsonb,
  p_correction_session_id uuid,
  p_prepare_only boolean
) returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_allowed_keys constant text[]:=array[
    'actor_user_id','source_cycle_id','authority_scope_kind','report_scope_id',
    'upload_id','projection_publication_id','expected_authority_scope_version',
    'expected_row_manifest_hash','expected_comparison_manifest_hash',
    'expected_issue_set_hash','exclude_unfinalised_acknowledged'
  ]::text[];
  v_unknown_key text;
  v_actor uuid;
  v_cycle_id uuid;
  v_scope_kind text;
  v_report_scope_id uuid;
  v_upload_id uuid;
  v_publication_id uuid;
  v_scope_version bigint;
  v_expected_row_hash bytea;
  v_expected_comparison_hash bytea;
  v_expected_issue_hash bytea;
  v_cycle public.weekly_source_cycles%rowtype;
  v_scope public.weekly_source_report_scopes%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_upload public.weekly_source_uploads%rowtype;
  v_publication public.weekly_source_projection_publications%rowtype;
  v_profile public.weekly_source_format_profiles%rowtype;
  v_existing_revision public.weekly_source_final_revisions%rowtype;
  v_correction public.weekly_final_source_correction_sessions%rowtype;
  v_guard jsonb;
  v_client_id uuid;
  v_resolved_client_id uuid;
  v_resolved_client_count integer;
  v_source_profile_kind text;
  v_source_mode text;
  v_generation integer;
  v_coverage_start date;
  v_coverage_end date;
  v_prior_revision_id uuid;
  v_revision_number integer;
  v_revision_id uuid;
  v_revision_manifest_hash bytea;
  v_policy_fingerprint bytea;
  v_finalised_at timestamptz:=pg_catalog.transaction_timestamp();
  v_row record;
  v_finalisation_root_ids uuid[];
  v_finalisation_root_recheck uuid[];
  v_finalisation_lock_result jsonb;
  v_lock_phase integer;
  v_lock_discovery jsonb;
  v_lock_observed jsonb;
  v_finalisation_lock_context boolean:=false;
  v_next_source_hooks boolean:=false;
  v_finalisation_lock_operation uuid;
  v_prior_finalisation_lock_operation text;
  v_prior_finalisation_root_set text;
  v_economic_id uuid;
  v_timesheet_id uuid;
  v_economic public.weekly_source_row_economic_snapshots%rowtype;
  v_snapshot_id uuid;
  v_snapshot_hash bytea;
  v_mapping_fingerprint bytea;
  v_pay_vector jsonb;
  v_charge_vector jsonb;
  v_transition record;
  v_prior_snapshot public.weekly_source_final_snapshot_lines%rowtype;
  v_current_snapshot public.weekly_source_final_snapshot_lines%rowtype;
  v_prior_state bytea;
  v_current_state bytea;
  v_outcome text;
  v_transition_id uuid;
  v_transition_fingerprint bytea;
  v_prior_movement public.weekly_source_billing_movements%rowtype;
  v_correction_unit_id uuid;
  v_charge_check public.weekly_source_charge_checks%rowtype;
  v_charge_acceptance public.weekly_source_charge_acceptances%rowtype;
  v_price_admission_result text;
  v_movement_id uuid;
  v_backing_report_number text;
  v_source_total_cost bigint;
  v_source_commission bigint;
  v_source_invoice_total bigint;
  v_line_count integer;
  v_manifest record;
  v_client_manifest_id uuid;
  v_client_manifest_hash bytea;
  v_completion_generation integer;
  v_completion_hash bytea;
  v_expense_observation record;
  v_target_expense_pence bigint;
  v_target_expense_vat boolean;
  v_target_expense_contract_id uuid;
  v_expense_observation_kind text;
  v_current_expense public.weekly_expense_authority_generations%rowtype;
  v_expense_policy public.weekly_source_row_expense_policy_snapshots%rowtype;
  v_expense_generation integer;
  v_expense_hash bytea;
  v_expense_authority_id uuid;
  v_prior_expense_authority_id uuid;
  v_expense_timesheet_id uuid;
  v_expense_policy_id uuid;
  v_expense_movement_result jsonb;
  v_rule_set jsonb;
  v_rule_values jsonb;
  v_rule_version uuid;
  v_invoice_amounts jsonb;
  v_invoice_correction jsonb;
  v_effective_policy jsonb;
  v_authority_subset jsonb;
  v_prior_authority_subset jsonb;
  v_current_authority_subset jsonb;
  v_comparison_current_authority jsonb;
  v_correction_presentation text;
  v_movement_count integer:=0;
  v_transition_count integer:=0;
  v_snapshot_count integer:=0;
  v_unchanged_count integer:=0;
  v_expense_generation_count integer:=0;
begin
  perform pg_catalog.set_config('lock_timeout','5s',true);
  if coalesce(current_setting('request.jwt.claim.role',true),
       nullif(current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;
  if p_prepare_only is null
     or (p_prepare_only and p_correction_session_id is null)
     or (not p_prepare_only and p_correction_session_id is not null) then
    raise exception 'WEEKLY_SOURCE_FINALISE_EXECUTION_MODE_INVALID' using errcode='22023';
  end if;
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object' then
    raise exception 'WEEKLY_SOURCE_FINALISE_REQUEST_INVALID' using errcode='22023';
  end if;
  select key into v_unknown_key
  from pg_catalog.jsonb_object_keys(p_request) key
  where not (key=any(v_allowed_keys))
  order by key limit 1;
  if v_unknown_key is not null then
    raise exception 'WEEKLY_SOURCE_FINALISE_REQUEST_UNKNOWN_FIELD'
      using errcode='22023',detail=v_unknown_key;
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_cycle_id:=(p_request->>'source_cycle_id')::uuid;
    v_scope_kind:=pg_catalog.upper(pg_catalog.btrim(coalesce(p_request->>'authority_scope_kind','')));
    v_report_scope_id:=nullif(p_request->>'report_scope_id','')::uuid;
    v_upload_id:=(p_request->>'upload_id')::uuid;
    v_publication_id:=(p_request->>'projection_publication_id')::uuid;
    v_scope_version:=(p_request->>'expected_authority_scope_version')::bigint;
  exception when invalid_text_representation or numeric_value_out_of_range then
    raise exception 'WEEKLY_SOURCE_FINALISE_REQUEST_INVALID' using errcode='22023';
  end;
  if v_actor is null or v_cycle_id is null or v_upload_id is null
     or v_publication_id is null or v_scope_version is null or v_scope_version<1
     or v_scope_kind not in ('CYCLE','NHSP_REPORT_SCOPE')
     or (v_scope_kind='NHSP_REPORT_SCOPE') is distinct from (v_report_scope_id is not null) then
    raise exception 'WEEKLY_SOURCE_FINALISE_SCOPE_INVALID' using errcode='22023';
  end if;
  v_expected_row_hash:=private.weekly_source_finalisation_hex32_v1(
    p_request->>'expected_row_manifest_hash','WEEKLY_SOURCE_ROW_MANIFEST_HASH_REQUIRED'
  );
  v_expected_comparison_hash:=private.weekly_source_finalisation_hex32_v1(
    p_request->>'expected_comparison_manifest_hash','WEEKLY_SOURCE_COMPARISON_HASH_REQUIRED'
  );
  v_expected_issue_hash:=private.weekly_source_finalisation_hex32_v1(
    p_request->>'expected_issue_set_hash','WEEKLY_SOURCE_ISSUE_HASH_REQUIRED'
  );

  -- Fresh Final admission precedes the first cycle/business lock. Recursive
  -- lineage/current-publication owners inherit this transaction's admission.
  perform private.weekly_source_pay_query_admit_v2();

  -- G1 plain scope discovery -> native complete I-1 -> group -> cycle.
  -- Repeat the same guards under locks; changed source/rules/roots retry before any mutation.
  for v_lock_phase in 1..2 loop
  if v_lock_phase=2 then
    -- Use the discovered exact group, never a fresh group chosen after I-1.
    select * into v_group from public.weekly_source_groups
    where id=(v_lock_discovery#>>'{group,id}')::uuid for update;
    if not found or to_jsonb(v_group) is distinct from v_lock_discovery->'group' then
      raise exception 'WEEKLY_SOURCE_FINALISATION_SCOPE_CHANGED_DURING_LOCK' using errcode='40001'; end if;
    select * into v_cycle from public.weekly_source_cycles where id=v_cycle_id for update;
    if not found or to_jsonb(v_cycle) is distinct from v_lock_discovery->'cycle' then
      raise exception 'WEEKLY_SOURCE_FINALISATION_SCOPE_CHANGED_DURING_LOCK' using errcode='40001'; end if;
  else
    select * into v_cycle from public.weekly_source_cycles where id=v_cycle_id;
  end if;
  if not found then
    raise exception 'WEEKLY_SOURCE_CYCLE_NOT_FOUND' using errcode='22023';
  end if;
  select * into strict v_group
  from public.weekly_source_groups where id=v_cycle.source_group_id;
  if v_scope_kind='NHSP_REPORT_SCOPE' then
  if v_lock_phase=2 then
    select * into v_scope from public.weekly_source_report_scopes where id=v_report_scope_id for update;
    if not found or to_jsonb(v_scope) is distinct from v_lock_discovery->'scope' then
      raise exception 'WEEKLY_SOURCE_FINALISATION_SCOPE_CHANGED_DURING_LOCK' using errcode='40001'; end if;
  else
    select * into v_scope from public.weekly_source_report_scopes where id=v_report_scope_id;
  end if;
    if not found or v_scope.source_cycle_id is distinct from v_cycle.id
       or v_scope.source_group_id is distinct from v_group.id
       or v_scope.environment is distinct from v_group.environment
       or v_scope.agency_id is distinct from v_group.agency_id
       or v_scope.cutoff_at_utc is distinct from v_cycle.cutoff_at_utc then
      raise exception 'WEEKLY_SOURCE_REPORT_SCOPE_MISMATCH' using errcode='22023';
    end if;
  end if;
  if v_lock_phase=2 then
    select * into v_upload from public.weekly_source_uploads where id=v_upload_id for share;
    if not found or to_jsonb(v_upload) is distinct from v_lock_discovery->'upload' then
      raise exception 'WEEKLY_SOURCE_FINALISATION_SCOPE_CHANGED_DURING_LOCK' using errcode='40001'; end if;
  else
    select * into v_upload from public.weekly_source_uploads where id=v_upload_id;
  end if;
  if not found then
    raise exception 'WEEKLY_SOURCE_UPLOAD_NOT_FOUND' using errcode='22023';
  end if;
  if v_lock_phase=2 then
    select * into v_publication from public.weekly_source_projection_publications where id=v_publication_id for share;
    if not found or to_jsonb(v_publication) is distinct from v_lock_discovery->'publication' then
      raise exception 'WEEKLY_SOURCE_FINALISATION_SCOPE_CHANGED_DURING_LOCK' using errcode='40001'; end if;
  else
    select * into strict v_publication from public.weekly_source_projection_publications where id=v_publication_id;
  end if;
  select * into strict v_profile
  from public.weekly_source_format_profiles where id=v_upload.source_format_profile_id;
  if v_lock_phase=2 and to_jsonb(v_profile) is distinct from v_lock_discovery->'profile' then
    raise exception 'WEEKLY_SOURCE_FINALISATION_SCOPE_CHANGED_DURING_LOCK' using errcode='40001'; end if;

  if p_prepare_only then
  if v_lock_phase=2 then
    select * into v_correction from public.weekly_final_source_correction_sessions where id=p_correction_session_id for share;
    if not found or to_jsonb(v_correction) is distinct from v_lock_discovery->'correction' then
      raise exception 'WEEKLY_SOURCE_FINALISATION_SCOPE_CHANGED_DURING_LOCK' using errcode='40001'; end if;
  else
    select * into v_correction from public.weekly_final_source_correction_sessions where id=p_correction_session_id;
  end if;
    if not found or v_correction.state<>'PREPARING'
       or v_correction.source_cycle_id is distinct from v_cycle.id
       or v_correction.authority_scope_kind is distinct from v_scope_kind
       or v_correction.report_scope_id is distinct from v_report_scope_id
       or v_correction.replacement_correction_upload_id is distinct from v_upload.id
       or v_correction.replacement_projection_publication_id is distinct from v_publication.id
       or v_upload.purpose<>'FINAL_SOURCE_CORRECTION'
       or v_upload.correction_session_id is distinct from v_correction.id
       or v_upload.state<>'CORRECTION_READY'
       or v_publication.state<>'CORRECTION_READY'
       or v_publication.correction_session_id is distinct from v_correction.id
       or v_publication.upload_id is distinct from v_upload.id
       or v_publication.authority_scope_version is distinct from v_scope_version then
      raise exception 'WEEKLY_SOURCE_CORRECTION_SESSION_STALE' using errcode='40001';
    end if;
    v_guard:=pg_catalog.jsonb_build_object(
      'row_manifest_hash',pg_catalog.encode(v_upload.row_manifest_hash,'hex'),
      'comparison_manifest_hash',pg_catalog.encode(v_publication.comparison_manifest_hash,'hex'),
      'issue_set_hash',pg_catalog.encode(v_publication.issue_set_hash,'hex')
    );
  else
    v_guard:=private.weekly_source_current_publication_guard_v1(
      v_cycle.id,v_scope_kind,v_report_scope_id,v_upload.id,v_publication.id,v_scope_version
    );
  end if;
  if private.weekly_source_finalisation_hex32_v1(
       v_guard->>'row_manifest_hash','WEEKLY_SOURCE_GUARD_ROW_HASH_INVALID'
     ) is distinct from v_expected_row_hash
     or private.weekly_source_finalisation_hex32_v1(
       v_guard->>'comparison_manifest_hash','WEEKLY_SOURCE_GUARD_COMPARISON_HASH_INVALID'
     ) is distinct from v_expected_comparison_hash
     or private.weekly_source_finalisation_hex32_v1(
       v_guard->>'issue_set_hash','WEEKLY_SOURCE_GUARD_ISSUE_HASH_INVALID'
     ) is distinct from v_expected_issue_hash then
    raise exception 'WEEKLY_SOURCE_FINALISE_PREVIEW_CHANGED' using errcode='40001';
  end if;

  v_client_id:=case when v_scope_kind='NHSP_REPORT_SCOPE' then v_scope.client_id
    else nullif(v_upload.file_metadata_json->>'client_id','')::uuid end;
  perform private.weekly_source_office_authority_v1(
    v_actor,case when p_prepare_only then 'CORRECT_FINAL_SOURCE' else 'FINALISE_WEEK' end,
    v_group.id,v_client_id,v_cycle.finalisation_week_ending
  );
  if p_correction_session_id is null then
    if v_upload.purpose<>'ORDINARY' or v_upload.correction_session_id is not null then
      raise exception 'WEEKLY_SOURCE_CORRECTION_REQUIRES_DEDICATED_OWNER' using errcode='55000';
    end if;
  else
    if v_correction.id is null or v_correction.state<>'PREPARING'
       or v_correction.source_cycle_id is distinct from v_cycle.id
       or v_correction.authority_scope_kind is distinct from v_scope_kind
       or v_correction.report_scope_id is distinct from v_report_scope_id
       or v_correction.replacement_correction_upload_id is distinct from v_upload.id
       or v_correction.replacement_projection_publication_id is distinct from v_publication.id
       or v_upload.purpose<>'FINAL_SOURCE_CORRECTION'
       or v_upload.correction_session_id is distinct from v_correction.id then
      raise exception 'WEEKLY_SOURCE_CORRECTION_SESSION_STALE' using errcode='40001';
    end if;
  end if;
  perform private.weekly_source_final_upload_assert_v1(v_upload.id);
  if not private.weekly_source_import_is_prepared_v1(v_upload.id) then
    raise exception 'WEEKLY_SOURCE_CHECKING_FILE_NOT_FINALISABLE' using errcode='55000';
  end if;
  if v_profile.profile_code in ('HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1','HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1')
    and exists(select 1 from public.weekly_source_upload_rows source_row
      where source_row.upload_id=v_upload.id and source_row.row_finalisation_state='SOURCE_UNFINALISED')
    and p_request->'exclude_unfinalised_acknowledged' is distinct from 'true'::jsonb then
    raise exception 'WEEKLY_SOURCE_UNFINALISED_EXCLUSION_ACKNOWLEDGEMENT_REQUIRED' using errcode='55000';
  end if;
  if v_finalised_at<coalesce(v_scope.cutoff_at_utc,v_cycle.cutoff_at_utc)
    and v_profile.profile_code not in ('HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1','HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1') then
    raise exception 'WEEKLY_SOURCE_CUTOFF_NOT_REACHED' using errcode='55000';
  end if;

  if v_upload.state<>(case when p_prepare_only then 'CORRECTION_READY' else 'CURRENT' end)
     or v_upload.row_manifest_hash is null
     or v_upload.blocked_count<>0 or v_upload.blocking_economic_duplicate_count<>0
     or v_upload.malformed_count<>0
     or v_upload.physical_row_count<>
       v_upload.header_count+v_upload.trailer_count+v_upload.continuation_count+
       v_upload.accepted_count+v_upload.blocking_economic_duplicate_count+v_upload.malformed_count
     or (select pg_catalog.count(*) from public.weekly_source_upload_rows
         where upload_id=v_upload.id)<>v_upload.accepted_count then
    raise exception 'WEEKLY_SOURCE_UPLOAD_NOT_FINALISABLE' using errcode='55000';
  end if;
  -- WP-54, pack 03 section 7: "Overlapping candidate work events are checked
  -- BEFORE FINALISATION.  The system does not assume two overlapping records
  -- are valid because their references differ."  The same guard runs at the
  -- seal, where a refused report never becomes CURRENT.  It runs again here
  -- because this is the owner the pack names and because the rows this engine
  -- is about to turn into movements are the rows that must satisfy it -- pack
  -- 14 section 8.1, the database decides source authority, never the browser.
  -- Deliberately NOT accompanied by a cutoff re-check: NHSP-BR-006 governs the
  -- cutoff occurrence Office confirms when the report is accepted, and the
  -- seal is where that confirmation is made authoritative.
  perform private.weekly_source_overlap_admission_assert_v1(v_upload.id);
  if v_scope_version>2147483647
     or coalesce(v_publication.projection_generation,0)>2147483647 then
    raise exception 'WEEKLY_SOURCE_GENERATION_OVERFLOW' using errcode='22003';
  end if;
  v_generation:=coalesce(
    v_publication.projection_generation,
    v_scope_version::integer
  );
  if (select pg_catalog.count(*)
      from public.weekly_source_row_resolutions resolution
      join public.weekly_source_upload_rows source_row on source_row.id=resolution.upload_row_id
      where source_row.upload_id=v_upload.id and resolution.generation=v_generation)
       <>v_upload.accepted_count
     or exists(
       select 1
       from public.weekly_source_upload_rows source_row
       left join public.weekly_source_row_resolutions resolution
         on resolution.upload_row_id=source_row.id and resolution.generation=v_generation
       where source_row.upload_id=v_upload.id
         and (resolution.id is null or resolution.mapping_state<>'RESOLVED')
     )
     or (v_profile.final_authority_kind<>'NHSP_TRUST_BACKING_REPORT' and exists(
       select 1
       from public.weekly_source_row_resolutions resolution
       join public.weekly_source_upload_rows source_row on source_row.id=resolution.upload_row_id
       where source_row.upload_id=v_upload.id and resolution.generation=v_generation
       group by resolution.work_event_id having pg_catalog.count(*)<>1
     )) then
    raise exception 'WEEKLY_SOURCE_RESOLUTION_CENSUS_NOT_FINALISABLE' using errcode='55000';
  end if;
  if v_scope_kind='CYCLE' then
    select pg_catalog.count(distinct resolution.client_id)::integer,
           pg_catalog.min(resolution.client_id::text)::uuid
    into v_resolved_client_count,v_resolved_client_id
    from public.weekly_source_row_resolutions resolution
    join public.weekly_source_upload_rows source_row on source_row.id=resolution.upload_row_id
    where source_row.upload_id=v_upload.id and resolution.generation=v_generation;
    if v_resolved_client_count>1 then
      raise exception 'WEEKLY_SOURCE_SINGLE_CLIENT_RESOLUTION_REQUIRED' using errcode='55000';
    end if;
    if v_resolved_client_count=1 and v_client_id is not null
       and v_client_id is distinct from v_resolved_client_id then
      raise exception 'WEEKLY_SOURCE_UPLOAD_CLIENT_MISMATCH' using errcode='55000';
    end if;
    v_client_id:=coalesce(v_resolved_client_id,v_client_id);
    if v_cycle.scope_client_id is not null and v_cycle.scope_client_id is distinct from v_client_id then
      raise exception 'WEEKLY_SOURCE_FINAL_CLIENT_SCOPE_REQUIRED' using errcode='55000';
    end if;
    if v_client_id is null or not exists(
      select 1
      from public.weekly_source_group_clients group_client
      where group_client.source_group_id=v_group.id
        and group_client.client_id=v_client_id
        and v_cycle.finalisation_week_ending between group_client.valid_from
          and coalesce(group_client.valid_to,'infinity'::date)
    ) then
      raise exception 'WEEKLY_SOURCE_FINAL_CLIENT_SCOPE_REQUIRED' using errcode='55000';
    end if;
    perform private.weekly_source_office_authority_v1(
      v_actor,case when p_prepare_only then 'CORRECT_FINAL_SOURCE' else 'FINALISE_WEEK' end,
      v_group.id,v_client_id,v_cycle.finalisation_week_ending
    );
  elsif not exists(
    select 1
    from public.weekly_source_group_clients group_client
    where group_client.source_group_id=v_group.id
      and group_client.client_id=v_client_id
      and v_cycle.finalisation_week_ending between group_client.valid_from
        and coalesce(group_client.valid_to,'infinity'::date)
  ) then
    raise exception 'WEEKLY_SOURCE_FINAL_CLIENT_SCOPE_REQUIRED' using errcode='55000';
  end if;

  v_rule_set:=private.weekly_source_effective_rule_set_v1(v_client_id,v_cycle.id);
  v_rule_values:=v_rule_set->'values'; v_rule_version:=nullif(v_rule_set->>'rule_set_version_id','')::uuid;
  v_source_profile_kind:=v_profile.final_authority_kind;
  if v_profile.profile_code='NHSP_FINAL_BACKING_V1' then
    if v_scope_kind<>'NHSP_REPORT_SCOPE' or v_group.source_family<>'NHSP'
       or v_source_profile_kind<>'NHSP_TRUST_BACKING_REPORT'
       or v_profile.omission_meaning<>'NO_INFERENCE'
       or not v_profile.report_number_required or not v_profile.fmc_must_equal_zero
       or v_client_id is null
       or exists(select 1 from public.weekly_source_upload_rows source_row
         where source_row.upload_id=v_upload.id
           and (source_row.row_finalisation_state not in ('NOT_APPLICABLE','SOURCE_WORKED')
              or source_row.source_money_parse_state<>'VALID'
              or source_row.source_shift_charge_pence is null
              or source_row.source_expense_parse_state<>'NOT_APPLICABLE'
             or source_row.source_expense_pence is not null)) then
      raise exception 'WEEKLY_SOURCE_NHSP_FINAL_NOT_FINALISABLE' using errcode='55000';
    end if;
    v_source_mode:='NHSP_WEEKLY';
    v_coverage_start:=null;
    v_coverage_end:=null;
    v_backing_report_number:=private.weekly_source_canonical_report_number_v1(
      v_upload.file_metadata_json->>'nhsp_report_number'
    );
    if v_backing_report_number is null then
      raise exception 'WEEKLY_SOURCE_NHSP_REPORT_NUMBER_REQUIRED' using errcode='55000';
    end if;
  elsif v_profile.profile_code in (
    'HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1','HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1'
  ) then
    if v_scope_kind<>'CYCLE' or v_group.source_family<>'ROSTER'
       or v_source_profile_kind<>'HEALTHROSTER_ACTUAL_ROWS'
       or v_profile.omission_meaning<>'CANCEL_INSIDE_CONFIRMED_COVERAGE'
       or v_client_id is null
       or exists(select 1 from public.weekly_source_upload_rows source_row
         where source_row.upload_id=v_upload.id
           and (source_row.row_finalisation_state not in ('SOURCE_WORKED','SOURCE_UNFINALISED')
             or source_row.source_expense_parse_state<>'NOT_APPLICABLE'
             or source_row.source_expense_pence is not null)) then
      raise exception 'WEEKLY_SOURCE_HEALTHROSTER_NOT_FINALISABLE' using errcode='55000';
    end if;
    v_source_mode:='HEALTHROSTER_WEEKLY';
    v_coverage_start:=v_upload.confirmed_coverage_start_local_date;
    v_coverage_end:=v_upload.confirmed_coverage_end_local_date;
  elsif v_profile.profile_code='ROSTER_WEEKLY_SUMMARY_ACTUAL_V1' then
    if v_scope_kind<>'CYCLE' or v_group.source_family<>'ROSTER'
       or v_source_profile_kind<>'GENERIC_COMPLETE_SNAPSHOT'
       or v_profile.omission_meaning<>'CANCEL_INSIDE_CONFIRMED_COVERAGE'
       or v_client_id is null
       or exists(select 1 from public.weekly_source_upload_rows source_row
         where source_row.upload_id=v_upload.id
           and (source_row.row_finalisation_state not in ('NOT_APPLICABLE','SOURCE_WORKED','SOURCE_ABSENT_ZERO')
             or source_row.source_expense_parse_state not in ('NOT_APPLICABLE','VALID','OMITTED_ZERO')
             or (source_row.source_expense_parse_state='NOT_APPLICABLE')
                  is distinct from (source_row.source_expense_pence is null)
             or (source_row.source_expense_parse_state='OMITTED_ZERO'
                 and source_row.source_expense_pence<>0))) then
      raise exception 'WEEKLY_SOURCE_ROSTER_COMPLETE_NOT_FINALISABLE' using errcode='55000';
    end if;
    v_source_mode:='HEALTHROSTER_WEEKLY';
    v_coverage_start:=v_upload.confirmed_coverage_start_local_date;
    v_coverage_end:=v_upload.confirmed_coverage_end_local_date;
  else
    raise exception 'WEEKLY_SOURCE_PROFILE_NOT_FINAL_AUTHORITY' using errcode='55000';
  end if;
  if v_scope_kind='CYCLE' and (
    v_upload.coverage_state<>'COMPLETE' or v_upload.coverage_timezone<>'Europe/London'
    or v_coverage_start is null or v_coverage_end is null or v_coverage_start>v_coverage_end
  ) then
    raise exception 'WEEKLY_SOURCE_CONFIRMED_COVERAGE_REQUIRED' using errcode='55000';
  end if;
  if exists(
    select 1
    from public.weekly_source_row_resolutions resolution
    join public.weekly_source_upload_rows source_row on source_row.id=resolution.upload_row_id
    where source_row.upload_id=v_upload.id and resolution.generation=v_generation
      and (resolution.client_id is distinct from v_client_id
        or source_row.work_date<coalesce(v_coverage_start,source_row.work_date)
        or source_row.work_date>coalesce(v_coverage_end,source_row.work_date))
  ) then
    raise exception 'WEEKLY_SOURCE_RESOLUTION_OUTSIDE_FINAL_SCOPE' using errcode='55000';
  end if;

  v_lock_observed:=jsonb_build_object(
    'group',to_jsonb(v_group),'cycle',to_jsonb(v_cycle),'scope',to_jsonb(v_scope),
    'upload',to_jsonb(v_upload),'publication',to_jsonb(v_publication),
    'profile',to_jsonb(v_profile),'correction',to_jsonb(v_correction),
    'client_id',v_client_id,'generation',v_generation,'coverage_start',v_coverage_start,
    'coverage_end',v_coverage_end,'source_profile_kind',v_source_profile_kind,
    'rule_set',v_rule_set,'resolutions',coalesce((select jsonb_agg(to_jsonb(r) order by r.id)
      from public.weekly_source_row_resolutions r join public.weekly_source_upload_rows s on s.id=r.upload_row_id
      where s.upload_id=v_upload.id and r.generation=v_generation),'[]'::jsonb));
  if v_lock_phase=1 then
    v_lock_discovery:=v_lock_observed;
  -- Review F2.  Finalisation calls the lineage ensure owner once per source row
  -- inside ONE transaction, so without this it accumulates family lock sets in
  -- source_row_ordinal order rather than the (btrim(booking_id), booking_id)
  -- order proof/32 section 6 step 2 fixes.  That is an A-B / B-A cycle against
  -- any sorted I-1 caller -- a publication, a release tick or a first
  -- authorisation -- and the I-1 caller was the victim (reproduction D3).
  --
  -- The complete set of roots this cycle will touch is therefore locked ONCE,
  -- up front, through the same helper, which sorts them.  Every later per-row
  -- call then re-enters locks this transaction already holds.
  --
  -- The Candidate serial gate is deliberately NOT taken here.  proof/32
  -- section 6 step 1 makes the gate per Candidate and pins it to four job
  -- types: first authorisation, first-authorisation withdrawal, entitlement
  -- publication and pending release.  Finalisation is none of them -- it
  -- authorises nothing and publishes no entitlement -- and one finalisation
  -- spans many Candidates, so a single gate call cannot cover it and a gate per
  -- Candidate would invent a lock ladder the pack does not describe.
  -- proof/34 section 5 asks this path only for the family lock set and the
  -- resolver, which is exactly what it takes.
  v_prior_finalisation_lock_operation:=coalesce(pg_catalog.current_setting('cloudtms.weekly_source_finalisation_lock_operation',true),'');
  v_prior_finalisation_root_set:=coalesce(pg_catalog.current_setting('cloudtms.weekly_source_finalisation_root_set',true),'');
  v_finalisation_lock_context:=true;
  select active_owner='NEXT' into v_next_source_hooks
  from private.bpay_next_module_control where id=1 for share;
  if coalesce(v_next_source_hooks,false) then

  -- Include prior-only CURRENT HR cancellation and positive-expense omission
  -- roots BEFORE taking any booking key. Current partial indexes provide the
  -- coverage inventory; only this upload/exact corrected revision contributes
  -- immutable emitted roots. No historical chooser is added here.
  v_finalisation_lock_operation:=private.bpay_next_source_current_lock_inventory_v1(
    v_upload.id,v_generation,case when p_prepare_only then v_correction.expected_current_final_revision_id else null end,
    v_group.id,v_client_id,v_coverage_start,v_coverage_end
  );
  perform pg_catalog.set_config('cloudtms.weekly_source_finalisation_lock_operation',v_finalisation_lock_operation::text,true);
  perform pg_catalog.set_config('cloudtms.weekly_source_finalisation_root_set','',true);
  else
  -- Retained default LEGACY / DISABLED Source path: no selector inventory,
  -- no assumption that pre-existing Source facts have been bootstrapped.
  select pg_catalog.array_agg(distinct contract_week.timesheet_id order by contract_week.timesheet_id)
  into v_finalisation_root_ids
  from public.weekly_source_upload_rows source_row
  join public.weekly_source_row_resolutions resolution
    on resolution.upload_row_id=source_row.id and resolution.generation=v_generation
  join public.contract_weeks contract_week
    on contract_week.contract_id=resolution.contract_id
   and contract_week.additional_seq=0
   and contract_week.week_ending_date=source_row.work_date+
       ((coalesce((
          select contract.week_ending_weekday_snapshot
          from public.contracts contract where contract.id=resolution.contract_id
        ),0)-extract(dow from source_row.work_date)::integer+7)%7)
  where source_row.upload_id=v_upload.id
    and contract_week.timesheet_id is not null;

  if v_finalisation_root_ids is not null
     and pg_catalog.array_length(v_finalisation_root_ids,1) is not null then
    v_finalisation_lock_result:=private.weekly_source_lock_family_rows_v1(
      v_finalisation_root_ids,null
    );
    if coalesce((v_finalisation_lock_result->>'ok')::boolean,false) is not true then
      raise exception '%',v_finalisation_lock_result->>'code'
        using errcode='55000',detail=v_finalisation_lock_result::text;
    end if;
  end if;

  -- Review G2.  The set above was computed from an UNLOCKED read filtered on a
  -- non-null Contract Week Timesheet id, so a root another owner commits between
  -- that read and the loop would be locked later, in source-row order, out of
  -- the sorted order proof/32 section 6 step 2 fixes.  Recompute it under the
  -- family locks now held; if it grew, refuse retryably (40001) rather than take
  -- a second lock set out of order.  A retry recomputes the larger set up front.
  select pg_catalog.array_agg(distinct contract_week.timesheet_id order by contract_week.timesheet_id)
  into v_finalisation_root_recheck
  from public.weekly_source_upload_rows source_row
  join public.weekly_source_row_resolutions resolution
    on resolution.upload_row_id=source_row.id and resolution.generation=v_generation
  join public.contract_weeks contract_week
    on contract_week.contract_id=resolution.contract_id
   and contract_week.additional_seq=0
   and contract_week.week_ending_date=source_row.work_date+
       ((coalesce((
          select contract.week_ending_weekday_snapshot
          from public.contracts contract where contract.id=resolution.contract_id
        ),0)-extract(dow from source_row.work_date)::integer+7)%7)
  where source_row.upload_id=v_upload.id
    and contract_week.timesheet_id is not null;

  if coalesce(v_finalisation_root_recheck,array[]::uuid[])
     <>coalesce(v_finalisation_root_ids,array[]::uuid[]) then
    raise exception 'WEEKLY_SOURCE_FINALISATION_ROOT_SET_CHANGED_DURING_LOCK'
      using errcode='40001',
        detail=pg_catalog.jsonb_build_object(
          'locked_root_count',
            coalesce(pg_catalog.array_length(v_finalisation_root_ids,1),0),
          'observed_root_count',
            coalesce(pg_catalog.array_length(v_finalisation_root_recheck,1),0)
        )::text;
  end if;

  -- Publish the locked set for the per-row calls.  The lineage ensure owner
  -- refuses 40001 when it discovers a root this set does not contain, so a root
  -- that appears after the recheck can never be locked out of order either.
  -- This is a Weekly-Source-private setting, never a Workbench-owned one.
  perform pg_catalog.set_config(
    'cloudtms.weekly_source_finalisation_root_set',
    coalesce(pg_catalog.array_to_string(v_finalisation_root_ids,','),'EMPTY_LOCKED_SET'),
    true
  );
  perform pg_catalog.set_config('cloudtms.weekly_source_finalisation_lock_operation','',true);
  end if;


  else
    if v_lock_observed is distinct from v_lock_discovery then
      raise exception 'WEEKLY_SOURCE_FINALISATION_SCOPE_CHANGED_DURING_LOCK' using errcode='40001'; end if;
    if v_finalisation_lock_operation is not null then
      perform private.bpay_next_source_current_lock_recheck_v1(v_finalisation_lock_operation);
    else
  -- Review G2.  The set above was computed from an UNLOCKED read filtered on a
  -- non-null Contract Week Timesheet id, so a root another owner commits between
  -- that read and the loop would be locked later, in source-row order, out of
  -- the sorted order proof/32 section 6 step 2 fixes.  Recompute it under the
  -- family locks now held; if it grew, refuse retryably (40001) rather than take
  -- a second lock set out of order.  A retry recomputes the larger set up front.
  select pg_catalog.array_agg(distinct contract_week.timesheet_id order by contract_week.timesheet_id)
  into v_finalisation_root_recheck
  from public.weekly_source_upload_rows source_row
  join public.weekly_source_row_resolutions resolution
    on resolution.upload_row_id=source_row.id and resolution.generation=v_generation
  join public.contract_weeks contract_week
    on contract_week.contract_id=resolution.contract_id
   and contract_week.additional_seq=0
   and contract_week.week_ending_date=source_row.work_date+
       ((coalesce((
          select contract.week_ending_weekday_snapshot
          from public.contracts contract where contract.id=resolution.contract_id
        ),0)-extract(dow from source_row.work_date)::integer+7)%7)
  where source_row.upload_id=v_upload.id
    and contract_week.timesheet_id is not null;

  if coalesce(v_finalisation_root_recheck,array[]::uuid[])
     <>coalesce(v_finalisation_root_ids,array[]::uuid[]) then
    raise exception 'WEEKLY_SOURCE_FINALISATION_ROOT_SET_CHANGED_DURING_LOCK'
      using errcode='40001',
        detail=pg_catalog.jsonb_build_object(
          'locked_root_count',
            coalesce(pg_catalog.array_length(v_finalisation_root_ids,1),0),
          'observed_root_count',
            coalesce(pg_catalog.array_length(v_finalisation_root_recheck,1),0)
        )::text;
  end if;


    end if;
  end if;
  end loop;

  -- One transaction at a time may decide the final order for a logical
  -- group/Client/source-authority history.  Both supported HealthRoster
  -- layouts share the same final_authority_kind and therefore the same lock.
  -- The lock is acquired only after the exact current publication and Client
  -- have been resolved; no other owner acquires this finaliser-only domain.
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
    'weekly_source_finalise_order:'||v_group.id::text||':'||v_client_id::text||':'||v_source_profile_kind,
    0
  ));

  if p_prepare_only then
    if coalesce(v_scope.current_final_revision_id,v_cycle.current_final_revision_id)
         is distinct from v_correction.expected_current_final_revision_id then
      raise exception 'WEEKLY_SOURCE_CORRECTION_FINAL_REVISION_STALE' using errcode='40001';
    end if;
  elsif coalesce(v_scope.current_final_revision_id,v_cycle.current_final_revision_id) is not null then
    select * into strict v_existing_revision
    from public.weekly_source_final_revisions
    where id=coalesce(v_scope.current_final_revision_id,v_cycle.current_final_revision_id);
    if v_existing_revision.upload_id=v_upload.id
       and v_existing_revision.authority_scope_kind=v_scope_kind
       and v_existing_revision.report_scope_id is not distinct from v_report_scope_id
       and v_existing_revision.state='CURRENT' then
      -- Native operation is transaction-local, including exact immutable replay.
      if v_finalisation_lock_operation is not null then
        perform private.bpay_next_source_current_lock_release_v1(v_finalisation_lock_operation);
      end if;
      if v_finalisation_lock_context then
    perform pg_catalog.set_config('cloudtms.weekly_source_finalisation_lock_operation',v_prior_finalisation_lock_operation,true);
    perform pg_catalog.set_config('cloudtms.weekly_source_finalisation_root_set',v_prior_finalisation_root_set,true);
      end if;
      return pg_catalog.jsonb_build_object(
        'ok',true,'status','FINALISED','idempotent',true,
        'final_revision_id',v_existing_revision.id,'source_cycle_id',v_cycle.id,
        'authority_scope_kind',v_scope_kind,'report_scope_id',v_report_scope_id,
        'upload_id',v_upload.id
      );
    end if;
    raise exception 'WEEKLY_SOURCE_SCOPE_ALREADY_FINALISED' using errcode='55000';
  end if;

  if v_scope_kind='CYCLE' and exists(
    select 1
    from public.weekly_source_final_revisions later_revision
    join public.weekly_source_cycles later_cycle
      on later_cycle.id=later_revision.source_cycle_id
    join public.weekly_source_client_manifests later_manifest
      on later_manifest.final_revision_id=later_revision.id
     and later_manifest.client_id=v_client_id
    join public.weekly_source_uploads later_upload
      on later_upload.id=later_revision.upload_id
    join public.weekly_source_format_profiles later_profile
      on later_profile.id=later_upload.source_format_profile_id
    where later_revision.authority_scope_kind='CYCLE'
      and later_revision.state='CURRENT'
      and later_cycle.source_group_id=v_group.id
      and later_cycle.finalisation_week_ending>v_cycle.finalisation_week_ending
      and later_profile.final_authority_kind=v_source_profile_kind
  ) then
    raise exception 'WEEKLY_SOURCE_FINALISATION_OUT_OF_ORDER' using errcode='55000';
  end if;

  -- The source Timesheet owner is the only component permitted to create or
  -- reuse ordinary Timesheet lineage.  Finalisation calls it, then validates
  -- the resulting immutable binding; it never accepts a caller-owned id.
  for v_row in
    select resolution.id as resolution_id,source_row.row_finalisation_state
    from public.weekly_source_upload_rows source_row
    join public.weekly_source_row_resolutions resolution
      on resolution.upload_row_id=source_row.id and resolution.generation=v_generation
    where source_row.upload_id=v_upload.id
      and (v_rule_values is null or v_rule_values->>'invoice_sender'<>'CLIENT_SELF_BILL' or v_rule_values->>'self_bill_source'<>'SAME_FILE'
          or coalesce((private.weekly_source_shift_on_self_bill_v1(resolution.work_event_id,v_cycle.id)->>'s25_pay_selected')::boolean,false))
      and (v_source_profile_kind='NHSP_TRUST_BACKING_REPORT'
        or source_row.row_finalisation_state='SOURCE_WORKED'
        or (v_source_profile_kind='GENERIC_COMPLETE_SNAPSHOT'
          and source_row.row_finalisation_state='NOT_APPLICABLE'))
    order by source_row.source_row_ordinal
  loop
    perform public.weekly_source_timesheet_lineage_ensure_atomic_v1(v_row.resolution_id,v_actor);
    v_economic_id:=private.weekly_source_finalisation_economic_assert_v1(
      v_row.resolution_id,v_cycle.id,v_source_mode
    );
    v_timesheet_id:=private.weekly_source_finalisation_lineage_assert_v1(
      v_row.resolution_id,v_cycle.id
    );
  end loop;

  if v_scope_kind='CYCLE' then
    select revision.id into v_prior_revision_id
    from public.weekly_source_final_revisions revision
    join public.weekly_source_cycles prior_cycle on prior_cycle.id=revision.source_cycle_id
    join public.weekly_source_client_manifests prior_manifest
      on prior_manifest.final_revision_id=revision.id and prior_manifest.client_id=v_client_id
    join public.weekly_source_uploads prior_upload on prior_upload.id=revision.upload_id
    join public.weekly_source_format_profiles prior_profile
      on prior_profile.id=prior_upload.source_format_profile_id
    where revision.authority_scope_kind='CYCLE' and revision.state='CURRENT'
      and prior_cycle.source_group_id=v_group.id and prior_cycle.id<>v_cycle.id
      and prior_cycle.finalisation_week_ending<v_cycle.finalisation_week_ending
      and prior_profile.final_authority_kind=v_source_profile_kind
    order by prior_cycle.finalisation_week_ending desc,revision.finalised_at_utc desc,revision.id desc
    limit 1;
  end if;

  select coalesce(pg_catalog.max(revision.revision_number),0)+1 into v_revision_number
  from public.weekly_source_final_revisions revision
  where revision.source_cycle_id=v_cycle.id
    and revision.authority_scope_kind=v_scope_kind
    and revision.report_scope_id is not distinct from v_report_scope_id;
  select private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_FINAL_POLICY_SET_V1',
    pg_catalog.jsonb_build_object(
      'profile_code',v_profile.profile_code,'profile_version',v_profile.version,
      'source_profile_kind',v_source_profile_kind,'source_mode',v_source_mode,
      'economics',coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
        'row_resolution_id',resolution.id,
        'contract_and_rate_fingerprint',case when economic.id is null then null
          else pg_catalog.encode(economic.contract_and_rate_fingerprint,'hex') end,
        'effective_policy_fingerprint',case when economic.id is null then null
          else pg_catalog.encode(economic.effective_policy_fingerprint,'hex') end,
        'invoice_vat_policy_fingerprint',case when economic.id is null then null
          else pg_catalog.encode(economic.invoice_vat_policy_fingerprint,'hex') end,
        'calculation_fingerprint',case when economic.id is null then null
          else pg_catalog.encode(economic.calculation_fingerprint,'hex') end,
        'expense_policy_snapshot_id',expense_policy.id,
        'expense_policy_snapshot_hash',case when expense_policy.id is null then null
          else pg_catalog.encode(expense_policy.snapshot_hash,'hex') end
      ) order by source_row.source_row_ordinal),'[]'::jsonb)
    )
  ) into v_policy_fingerprint
  from public.weekly_source_upload_rows source_row
  join public.weekly_source_row_resolutions resolution
    on resolution.upload_row_id=source_row.id and resolution.generation=v_generation
  left join public.weekly_source_row_economic_snapshots economic
    on economic.row_resolution_id=resolution.id
  left join public.weekly_source_row_expense_policy_snapshots expense_policy
    on expense_policy.row_resolution_id=resolution.id
  where source_row.upload_id=v_upload.id;
  v_revision_manifest_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_FINAL_REVISION_MANIFEST_V1',
    pg_catalog.jsonb_build_object(
      'source_cycle_id',v_cycle.id,'authority_scope_kind',v_scope_kind,
      'report_scope_id',v_report_scope_id,'revision_number',v_revision_number,
      'upload_id',v_upload.id,'authority_scope_version',v_scope_version,
      'row_manifest_hash',pg_catalog.encode(v_expected_row_hash,'hex'),
      'comparison_manifest_hash',pg_catalog.encode(v_expected_comparison_hash,'hex'),
      'issue_set_hash',pg_catalog.encode(v_expected_issue_hash,'hex'),
      'profile_code',v_profile.profile_code,'profile_version',v_profile.version,
      'coverage_start_local_date',v_coverage_start,'coverage_end_local_date',v_coverage_end,
      'reason',case when p_correction_session_id is null then 'INITIAL_FINALISATION'
        else 'CORRECT_FINAL_SOURCE' end,
      'predecessor_revision_id',case when p_correction_session_id is null then null
        else v_correction.expected_current_final_revision_id end,
      'prior_state_cutoff_revision_id',v_prior_revision_id,
      'policy_fingerprint',pg_catalog.encode(v_policy_fingerprint,'hex')
    )||case when v_rule_version is null then '{}'::jsonb else jsonb_build_object(
      'rule_set_version_id',v_rule_version,'rule_set_values_sha256',v_rule_set->>'values_sha256') end
  );
  insert into public.weekly_source_final_revisions(
    source_cycle_id,authority_scope_kind,report_scope_id,revision_number,upload_id,
    predecessor_revision_id,coverage_start_local_date,coverage_end_local_date,
    coverage_timezone,prior_state_cutoff_revision_id,reason,finalised_by_user_id,
    finalised_at_utc,manifest_hash,policy_fingerprint,state,rule_set_version_id
  ) values (
    v_cycle.id,v_scope_kind,v_report_scope_id,v_revision_number,v_upload.id,
    case when p_correction_session_id is null then null
      else v_correction.expected_current_final_revision_id end,
    v_coverage_start,v_coverage_end,
    case when v_scope_kind='CYCLE' then 'Europe/London' else null end,
    v_prior_revision_id,
    case when p_correction_session_id is null then 'INITIAL_FINALISATION'
      else 'CORRECT_FINAL_SOURCE' end,
    v_actor,v_finalised_at,
    v_revision_manifest_hash,v_policy_fingerprint,
    case when p_prepare_only then 'PREPARED' else 'CURRENT' end,v_rule_version
  ) returning id into v_revision_id;

  if v_source_profile_kind in ('GENERIC_COMPLETE_SNAPSHOT','HEALTHROSTER_ACTUAL_ROWS') then
    for v_row in
      select source_row.*,resolution.id as resolution_id,
             resolution.work_event_id,resolution.candidate_id,resolution.client_id,
             resolution.contract_id,resolution.work_event_match_kind,
             resolution.work_event_match_fingerprint
      from public.weekly_source_upload_rows source_row
      join public.weekly_source_row_resolutions resolution
        on resolution.upload_row_id=source_row.id and resolution.generation=v_generation
      where source_row.upload_id=v_upload.id
        and (v_rule_values is null or v_rule_values->>'invoice_sender'<>'CLIENT_SELF_BILL' or v_rule_values->>'self_bill_source'<>'SAME_FILE'
          or coalesce((private.weekly_source_shift_on_self_bill_v1(resolution.work_event_id,v_cycle.id)->>'s25_pay_selected')::boolean,false))
        and (source_row.row_finalisation_state='SOURCE_WORKED'
          or (v_source_profile_kind='GENERIC_COMPLETE_SNAPSHOT'
            and source_row.row_finalisation_state='NOT_APPLICABLE'))
      order by source_row.source_row_ordinal
    loop
      v_economic_id:=private.weekly_source_finalisation_economic_assert_v1(
        v_row.resolution_id,v_cycle.id,'HEALTHROSTER_WEEKLY'
      );
      v_timesheet_id:=private.weekly_source_finalisation_lineage_assert_v1(
        v_row.resolution_id,v_cycle.id
      );
      select * into strict v_economic
      from public.weekly_source_row_economic_snapshots where id=v_economic_id;
      v_pay_vector:=private.weekly_source_finalisation_vector_v1(v_economic.id,'PAY',1::smallint);
      v_charge_vector:=private.weekly_source_finalisation_vector_v1(v_economic.id,'CHARGE',1::smallint);
      v_mapping_fingerprint:=private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_SOURCE_FINAL_MAPPING_V1',
        pg_catalog.jsonb_build_object(
          'work_event_id',v_row.work_event_id,'candidate_id',v_row.candidate_id,
          'client_id',v_row.client_id,'contract_id',v_row.contract_id
        )
      );
      -- Freeze only the four authority fields for this exact new snapshot.
      -- The full economic/provenance hashes remain unchanged below.
      v_effective_policy:=private._weekly_source_effective_policy_v1(
        v_row.client_id,v_row.contract_id,v_row.work_date
      );
      if (v_effective_policy->>'authority_mode') is distinct from 'SOURCE_AUTHORITY'
         or coalesce((v_effective_policy->>'self_bill_enabled')::boolean,false) is not true
         or (v_effective_policy->>'source_group_id')::uuid is distinct from v_group.id then
        raise exception 'WEEKLY_SOURCE_FINALISATION_POLICY_STALE' using errcode='40001';
      end if;
      v_authority_subset:=private.weekly_source_authority_subset_v1(v_effective_policy);
      v_snapshot_hash:=private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_SOURCE_FINAL_SNAPSHOT_LINE_V1',
        pg_catalog.jsonb_build_object(
          'final_revision_id',v_revision_id,'upload_row_id',v_row.id,
          'row_resolution_id',v_row.resolution_id,'work_event_id',v_row.work_event_id,
          'candidate_id',v_row.candidate_id,'client_id',v_row.client_id,
          'contract_id',v_row.contract_id,'work_date',v_row.work_date,
          'start_at_local',v_row.start_at_local,'end_at_local',v_row.end_at_local,
          'break_minutes',v_row.break_minutes,'actual_net_minutes',v_row.actual_net_minutes,
          'pay_vector',v_pay_vector,'charge_vector',v_charge_vector,
          'source_expense_pence',v_row.source_expense_pence,
          'source_fingerprint',pg_catalog.encode(v_row.normalised_row_hash,'hex'),
          'mapping_fingerprint',pg_catalog.encode(v_mapping_fingerprint,'hex'),
          'rate_fingerprint',pg_catalog.encode(v_economic.calculation_fingerprint,'hex'),
          'policy_fingerprint',pg_catalog.encode(v_economic.effective_policy_fingerprint,'hex')
        )
      );
      insert into public.weekly_source_final_snapshot_lines(
        final_revision_id,upload_row_id,row_resolution_id,charge_check_id,
        source_profile_kind,work_event_id,external_event_identity,candidate_id,
        client_id,contract_id,work_date,start_at_local,end_at_local,break_minutes,
        actual_net_minutes,source_classifications_json,pay_vector_json,charge_vector_json,
        source_expense_pence,source_fingerprint,mapping_fingerprint,rate_fingerprint,
        policy_fingerprint,snapshot_line_hash,authority_subset_json,authority_subset_sha256,invoice_economics_sha256
      ) values (
        v_revision_id,v_row.id,v_row.resolution_id,null,v_source_profile_kind,
        v_row.work_event_id,v_row.external_source_key,v_row.candidate_id,v_row.client_id,
        v_row.contract_id,v_row.work_date,v_row.start_at_local,v_row.end_at_local,
        v_row.break_minutes,v_row.actual_net_minutes,v_economic.canonical_result_json,
        v_pay_vector,v_charge_vector,v_row.source_expense_pence,v_row.normalised_row_hash,
        v_mapping_fingerprint,v_economic.calculation_fingerprint,
        v_economic.effective_policy_fingerprint,v_snapshot_hash,v_authority_subset,
        private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_AUTHORITY_SUBSET_V1',v_authority_subset),
        case when v_rule_values is not null and v_rule_values->>'invoice_sender'='CLIENT_SELF_BILL' and v_rule_values->>'invoice_amount_basis'='SOURCE_AMOUNT' and v_rule_values->>'source_amount_basis'='EXPORT_ACTUAL_COST_INC_VAT_NET_AGENCY_WORKER_PAY'
          then private.weekly_source_invoice_economics_fingerprint_v1(jsonb_build_object('source_shift_charge_pence',v_row.source_shift_charge_pence,
            'source_gross_pence',v_row.source_gross_pence,'source_vat_pence',v_row.source_vat_pence),v_rule_values->>'line_vat_method',
            case when v_economic.invoice_vat_chargeable then v_economic.invoice_vat_rate_pct else 0 end) else null end
      ) returning id into v_snapshot_id;
      v_snapshot_count:=v_snapshot_count+1;
    end loop;

    for v_transition in
      with prior_states as (
        select distinct on (prior_transition.work_event_id)
               prior_transition.work_event_id,
               prior_transition.new_snapshot_line_id
        from public.weekly_source_state_transitions prior_transition
        join public.weekly_source_final_revisions prior_revision
          on prior_revision.id=prior_transition.final_revision_id
         and prior_revision.state='CURRENT'
        join public.weekly_source_cycles prior_cycle
          on prior_cycle.id=prior_transition.finalisation_cycle_id
        join public.weekly_source_client_manifests prior_manifest
          on prior_manifest.final_revision_id=prior_revision.id
         and prior_manifest.client_id=v_client_id
        where prior_transition.source_profile_kind=v_source_profile_kind
          and prior_cycle.source_group_id=v_group.id
          and prior_cycle.finalisation_week_ending<v_cycle.finalisation_week_ending
        order by prior_transition.work_event_id,
                 prior_cycle.finalisation_week_ending desc,
                 prior_revision.finalised_at_utc desc,
                 prior_revision.revision_number desc,
                 prior_transition.created_at_utc desc,
                 prior_transition.id desc
      ), prior_lines as (
        select prior_line.id,prior_state.work_event_id
        from prior_states prior_state
        join public.weekly_source_final_snapshot_lines prior_line
          on prior_line.id=prior_state.new_snapshot_line_id
        where prior_line.client_id=v_client_id
          and not (v_rule_values is not null and v_rule_values->>'invoice_sender'='CLIENT_SELF_BILL' and v_rule_values->>'self_bill_source'='SAME_FILE'
            and coalesce((private.weekly_source_shift_on_self_bill_v1(prior_line.work_event_id,v_cycle.id)->>'bound_to_finalised_export')::boolean,false)
            and not coalesce((private.weekly_source_shift_on_self_bill_v1(prior_line.work_event_id,v_cycle.id)->>'s25_pay_selected')::boolean,false))
          and (
            prior_line.work_date between v_coverage_start and v_coverage_end
            or exists(
              select 1
              from public.weekly_source_row_resolutions current_resolution
              join public.weekly_source_upload_rows current_row
                on current_row.id=current_resolution.upload_row_id
              where current_row.upload_id=v_upload.id
                and current_resolution.generation=v_generation
                and current_resolution.work_event_id=prior_line.work_event_id
            )
          )
      ), current_lines as (
        select current_line.id,current_line.work_event_id
        from public.weekly_source_final_snapshot_lines current_line
        where current_line.final_revision_id=v_revision_id
      )
      select coalesce(prior_lines.work_event_id,current_lines.work_event_id) as work_event_id,
             prior_lines.id as prior_snapshot_id,current_lines.id as current_snapshot_id
      from prior_lines full join current_lines using(work_event_id)
      order by coalesce(prior_lines.work_event_id,current_lines.work_event_id)
    loop
      select * into v_prior_snapshot
      from public.weekly_source_final_snapshot_lines
      where id=v_transition.prior_snapshot_id;
      select * into v_current_snapshot
      from public.weekly_source_final_snapshot_lines
      where id=v_transition.current_snapshot_id;
      v_effective_policy:=private._weekly_source_effective_policy_v1(
        coalesce(v_current_snapshot.client_id,v_prior_snapshot.client_id),
        coalesce(v_current_snapshot.contract_id,v_prior_snapshot.contract_id),
        coalesce(v_current_snapshot.work_date,v_prior_snapshot.work_date)
      );
      if (v_effective_policy->>'authority_mode') is distinct from 'SOURCE_AUTHORITY'
         or coalesce((v_effective_policy->>'self_bill_enabled')::boolean,false) is not true
         or (v_effective_policy->>'source_group_id')::uuid is distinct from v_group.id then
        raise exception 'WEEKLY_SOURCE_FINALISATION_POLICY_STALE' using errcode='40001';
      end if;
      v_authority_subset:=private.weekly_source_authority_subset_v1(v_effective_policy);
      v_prior_authority_subset:=private.weekly_source_snapshot_authority_subset_v1(v_transition.prior_snapshot_id);
      v_current_authority_subset:=private.weekly_source_snapshot_authority_subset_v1(v_transition.current_snapshot_id);
      if (v_effective_policy->>'authority_mode') is distinct from 'SOURCE_AUTHORITY'
         or coalesce((v_effective_policy->>'self_bill_enabled')::boolean,false) is not true
         or (v_effective_policy->>'source_group_id')::uuid is distinct from v_group.id
         or (coalesce(v_current_authority_subset,v_prior_authority_subset) is not null
           and v_authority_subset is distinct from coalesce(v_current_authority_subset,v_prior_authority_subset))
         or (coalesce(v_current_authority_subset,v_prior_authority_subset) is null
           and private.weekly_source_finalisation_hex32_v1(
              v_effective_policy->>'policy_sha256','WEEKLY_SOURCE_POLICY_FINGERPRINT_INVALID'
            ) is distinct from coalesce(
              v_current_snapshot.policy_fingerprint,v_prior_snapshot.policy_fingerprint
            )) then
        raise exception 'WEEKLY_SOURCE_FINALISATION_POLICY_STALE' using errcode='40001';
      end if;
      v_correction_presentation:=pg_catalog.upper(pg_catalog.btrim(coalesce(
        v_effective_policy->>'self_bill_correction_presentation',''
      )));
      if v_correction_presentation not in (
        'FULL_REVERSAL_REPLACEMENT','NET_DIFFERENCE_PRESENTATION'
      ) then
        raise exception 'WEEKLY_SOURCE_SELF_BILL_CORRECTION_POLICY_REQUIRED'
          using errcode='55000';
      end if;
      -- Unknown pre-upgrade authority keeps the whole-hash fence. Equality
      -- proves compatibility without inventing historical four-field JSON.
      v_comparison_current_authority:=v_current_authority_subset;
      if v_transition.prior_snapshot_id is not null and v_prior_authority_subset is null
         and v_transition.current_snapshot_id is not null then
        if private.weekly_source_finalisation_hex32_v1(
             v_effective_policy->>'policy_sha256','WEEKLY_SOURCE_POLICY_FINGERPRINT_INVALID'
           ) is distinct from v_prior_snapshot.policy_fingerprint then
          raise exception 'WEEKLY_SOURCE_FINALISATION_POLICY_STALE' using errcode='40001';
        end if;
        v_comparison_current_authority:=null;
      end if;
      v_prior_state:=private.weekly_source_snapshot_value_fingerprint_v1(
        v_transition.prior_snapshot_id,v_prior_authority_subset
      );
      v_current_state:=private.weekly_source_snapshot_value_fingerprint_v1(
        v_transition.current_snapshot_id,v_comparison_current_authority
      );
      v_outcome:=case
        when v_transition.prior_snapshot_id is null then 'ADD'
        when v_transition.current_snapshot_id is null then 'CANCEL'
        when v_prior_state=v_current_state then 'NO_CHANGE'
        else 'AMEND' end;
      v_transition_fingerprint:=private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_SOURCE_STATE_TRANSITION_V1',
        pg_catalog.jsonb_build_object(
          'final_revision_id',v_revision_id,'work_event_id',v_transition.work_event_id,
          'previous_snapshot_line_id',v_transition.prior_snapshot_id,
          'new_snapshot_line_id',v_transition.current_snapshot_id,
          'outcome',v_outcome,
          'prior_state_fingerprint',case when v_prior_state is null then null
            else pg_catalog.encode(v_prior_state,'hex') end,
          'new_state_fingerprint',case when v_current_state is null then null
            else pg_catalog.encode(v_current_state,'hex') end
        )
      );
      insert into public.weekly_source_state_transitions(
        final_revision_id,finalisation_cycle_id,source_profile_kind,work_event_id,
        previous_present,new_present,previous_snapshot_line_id,new_snapshot_line_id,
        outcome,identity_match_kind,match_confidence,prior_state_fingerprint,
        new_state_fingerprint,transition_fingerprint,
        ordinary_source_entitlement_projection_state
      ) values (
        v_revision_id,v_cycle.id,v_source_profile_kind,v_transition.work_event_id,
        v_transition.prior_snapshot_id is not null,v_transition.current_snapshot_id is not null,
        v_transition.prior_snapshot_id,v_transition.current_snapshot_id,v_outcome,
        (select event.identity_kind from public.weekly_work_events event
          where event.id=v_transition.work_event_id),
        case when (select event.identity_kind from public.weekly_work_events event
          where event.id=v_transition.work_event_id)='PROFILE_EXTERNAL_KEY'
          then 'EXACT_PROFILE_KEY' else 'EXACT_DURABLE_LINEAGE' end,
        v_prior_state,v_current_state,v_transition_fingerprint,
        case when v_outcome='NO_CHANGE' then 'NOT_APPLICABLE' else 'PENDING' end
      ) returning id into v_transition_id;
      v_transition_count:=v_transition_count+1;

      if v_outcome='NO_CHANGE' then
        v_unchanged_count:=v_unchanged_count+1;
        if v_transition.new_snapshot_line_id is not null and v_transition.previous_snapshot_line_id is not null and exists(select 1 from public.weekly_source_final_snapshot_lines current_line join public.weekly_source_final_snapshot_lines prior_line on prior_line.id=v_transition.previous_snapshot_line_id where current_line.id=v_transition.new_snapshot_line_id and current_line.external_event_identity is distinct from prior_line.external_event_identity) then
          if private.weekly_source_rebill_movement_insert_v1(v_transition_id,v_revision_id) is not null then v_movement_count:=v_movement_count+1; end if;
        else
          v_invoice_correction:=private.weekly_source_invoice_correction_insert_v1(v_transition_id,v_transition.work_event_id,v_revision_id);
          v_movement_count:=v_movement_count+coalesce((v_invoice_correction->>'movement_count')::integer,0);
        end if;
      else
        if v_outcome in ('AMEND','CANCEL') then
          select movement.* into v_prior_movement
          from public.weekly_source_billing_movements movement
          join public.weekly_source_final_revisions movement_revision
            on movement_revision.id=movement.final_revision_id
          join public.weekly_source_cycles movement_cycle
            on movement_cycle.id=movement.finalisation_cycle_id
          where movement.work_event_id=v_transition.work_event_id
            and movement.source_profile_kind=v_source_profile_kind
            and movement.movement_role in ('POSITIVE','REPLACEMENT')
            and movement.source_line_kind not in ('SELF_BILL_REPORT_LINE','INVOICE_VAT_ROUNDING','SOURCE_REBILL_LINE','SOURCE_INVOICE_CORRECTION')
            and movement.final_revision_id<>v_revision_id
            and movement_cycle.finalisation_week_ending<v_cycle.finalisation_week_ending
          order by movement_cycle.finalisation_week_ending desc,
                   movement_revision.finalised_at_utc desc,movement.created_at_utc desc,movement.id desc
          limit 1;
          if not found then
            raise exception 'WEEKLY_SOURCE_PRIOR_MOVEMENT_MISSING' using errcode='55000';
          end if;
          v_correction_unit_id:=pg_catalog.gen_random_uuid();
          v_movement_id:=private.weekly_source_finalisation_insert_reversal_v1(
            v_transition_id,v_revision_id,v_cycle.id,v_prior_movement.id,
            v_correction_unit_id,v_source_profile_kind,
            pg_catalog.jsonb_build_object(
              'schema_version','WEEKLY_SOURCE_GENERATED_REVERSAL_FACTS_V1',
              'outcome',v_outcome,'prior_snapshot_line_id',v_transition.prior_snapshot_id,
              'prior_movement_id',v_prior_movement.id,
              'correction_presentation',v_correction_presentation
            )
          );
          v_movement_count:=v_movement_count+1;
          if private.weekly_source_invoice_correction_unwind_v1(v_transition_id,v_transition.work_event_id,v_revision_id) is not null then v_movement_count:=v_movement_count+1; end if;
        else
          v_correction_unit_id:=null;
          v_prior_movement.id:=null;
        end if;
        if v_outcome in ('ADD','AMEND') then
          select resolution.id as resolution_id into strict v_row
          from public.weekly_source_row_resolutions resolution
          where resolution.id=v_current_snapshot.row_resolution_id;
          v_economic_id:=private.weekly_source_finalisation_economic_assert_v1(
            v_row.resolution_id,v_cycle.id,'HEALTHROSTER_WEEKLY'
          );
          v_timesheet_id:=private.weekly_source_finalisation_lineage_assert_v1(
            v_row.resolution_id,v_cycle.id
          );
          select * into strict v_economic
          from public.weekly_source_row_economic_snapshots where id=v_economic_id;
          v_invoice_amounts:=private.weekly_source_invoice_line_amounts_v1(v_row.resolution_id,v_revision_id);
          v_movement_id:=private.weekly_source_finalisation_insert_current_movement_v1(
            v_transition_id,null,v_revision_id,v_cycle.id,v_source_profile_kind,
            case when v_outcome='ADD' then 'POSITIVE' else 'REPLACEMENT' end,
            case when v_outcome='ADD' then 'SOURCE_ORDINARY' else 'SOURCE_REPLACEMENT' end,
            v_row.resolution_id,v_economic.id,v_timesheet_id,case when (v_invoice_amounts->>'source_amount')::boolean then (v_invoice_amounts->>'net_pence')::bigint else null end,
            (v_invoice_amounts->>'net_pence')::bigint,v_invoice_amounts->>'price_check_result',case when (v_invoice_amounts->>'source_amount')::boolean then v_economic.calculation_fingerprint else null end,
            nullif(v_invoice_amounts->>'charge_acceptance_id','')::uuid,
            pg_catalog.jsonb_build_object(
              'schema_version','WEEKLY_SOURCE_CURRENT_FACTS_V1','outcome',v_outcome,
              'snapshot_line_id',v_current_snapshot.id,
              'upload_row_id',v_current_snapshot.upload_row_id,
              'row_resolution_id',v_current_snapshot.row_resolution_id,
              'correction_presentation',v_correction_presentation
            ),v_correction_unit_id,
            case when v_outcome='AMEND' then v_prior_movement.id else null end
          );
          v_movement_count:=v_movement_count+1;
        end if;
      end if;

    end loop;

    if v_profile.profile_code='ROSTER_WEEKLY_SUMMARY_ACTUAL_V1' then
      -- The profile can serve Clients with or without source-fixed expenses.
      -- A snapshot is mandatory exactly where the sealed effective policy
      -- enables that route, and forbidden otherwise.  This prevents both a
      -- missing configured expense and accidental contamination of ordinary
      -- evidence-led expenses.
      for v_row in
        select source_row.work_date,resolution.id as resolution_id,
               resolution.client_id,resolution.contract_id,
               resolution.effective_policy_fingerprint,
               expense_policy.id as expense_policy_snapshot_id
        from public.weekly_source_upload_rows source_row
        join public.weekly_source_row_resolutions resolution
          on resolution.upload_row_id=source_row.id and resolution.generation=v_generation
        left join public.weekly_source_row_expense_policy_snapshots expense_policy
          on expense_policy.row_resolution_id=resolution.id
        where source_row.upload_id=v_upload.id
        order by source_row.source_row_ordinal
      loop
        v_effective_policy:=private._weekly_source_effective_policy_v1(
          v_row.client_id,v_row.contract_id,v_row.work_date
        );
        if (v_effective_policy->>'authority_mode') is distinct from 'SOURCE_AUTHORITY'
           or coalesce((v_effective_policy->>'self_bill_enabled')::boolean,false) is not true
           or (v_effective_policy->>'source_group_id')::uuid is distinct from v_group.id
           or private.weekly_source_finalisation_hex32_v1(
                v_effective_policy->>'policy_sha256','WEEKLY_SOURCE_POLICY_FINGERPRINT_INVALID'
              ) is distinct from v_row.effective_policy_fingerprint then
          raise exception 'WEEKLY_SOURCE_FINALISATION_POLICY_STALE' using errcode='40001';
        end if;
        if coalesce((v_effective_policy->>'source_fixed_expenses_enabled')::boolean,false) then
          if v_row.expense_policy_snapshot_id is null
             or private.weekly_source_finalisation_expense_policy_assert_v1(
                  v_row.resolution_id,v_cycle.id
                ) is distinct from v_row.expense_policy_snapshot_id then
            raise exception 'WEEKLY_SOURCE_EXPENSE_POLICY_SNAPSHOT_CARDINALITY'
              using errcode='55000';
          end if;
        elsif v_row.expense_policy_snapshot_id is not null then
          raise exception 'WEEKLY_SOURCE_EXPENSE_POLICY_NOT_ENABLED' using errcode='55000';
        end if;
      end loop;

      for v_expense_observation in
        with current_observation as (
          select expense_policy.id as expense_policy_snapshot_id,
                 expense_policy.row_resolution_id,expense_policy.work_event_id,
                 expense_policy.contract_id,expense_policy.source_expense_pence,
                 expense_policy.source_expense_vat_enabled,
                 expense_policy.correction_presentation
          from public.weekly_source_row_expense_policy_snapshots expense_policy
          join public.weekly_source_row_resolutions resolution
            on resolution.id=expense_policy.row_resolution_id
          join public.weekly_source_upload_rows source_row
            on source_row.id=resolution.upload_row_id
          where source_row.upload_id=v_upload.id and resolution.generation=v_generation
        ), prior_authority as (
          select
                 case when p_prepare_only
                           and authority.final_revision_id=v_correction.expected_current_final_revision_id
                        then baseline.id else authority.id end as prior_authority_id,
                 authority.work_event_id,
                 case when p_prepare_only
                           and authority.final_revision_id=v_correction.expected_current_final_revision_id
                        then baseline.contract_id else authority.contract_id end as contract_id,
                 case when p_prepare_only
                           and authority.final_revision_id=v_correction.expected_current_final_revision_id
                        then baseline.source_expense_pence else authority.source_expense_pence end
                   as source_expense_pence,
                 case when p_prepare_only
                           and authority.final_revision_id=v_correction.expected_current_final_revision_id
                        then baseline.source_expense_vat_enabled
                        else authority.source_expense_vat_enabled end
                   as source_expense_vat_enabled,
                 case when p_prepare_only
                           and authority.final_revision_id=v_correction.expected_current_final_revision_id
                        then baseline.correction_presentation
                        else authority.correction_presentation end
                   as correction_presentation,
                 event.work_date
          from public.weekly_expense_authority_generations authority
          join public.weekly_work_events event on event.id=authority.work_event_id
          left join public.weekly_expense_authority_generations baseline
            on p_prepare_only
           and authority.final_revision_id=v_correction.expected_current_final_revision_id
           and baseline.id=authority.prior_expense_authority_generation_id
          where authority.state='CURRENT'
            and event.first_source_group_id=v_group.id
            and event.client_id=v_client_id
            and not (
              p_prepare_only
              and authority.final_revision_id=v_correction.expected_current_final_revision_id
              and baseline.id is null
            )
            and (
              event.work_date between v_coverage_start and v_coverage_end
              or exists(
                select 1 from current_observation current_match
                where current_match.work_event_id=authority.work_event_id
              )
            )
        )
        select coalesce(current_observation.work_event_id,prior_authority.work_event_id) as work_event_id,
               current_observation.expense_policy_snapshot_id,
               current_observation.row_resolution_id,
               current_observation.contract_id as current_contract_id,
               current_observation.source_expense_pence as current_expense_pence,
               current_observation.source_expense_vat_enabled as current_expense_vat_enabled,
               current_observation.correction_presentation as current_correction_presentation,
               prior_authority.prior_authority_id,
               prior_authority.contract_id as prior_contract_id,
               prior_authority.source_expense_pence as prior_expense_pence,
               prior_authority.source_expense_vat_enabled as prior_expense_vat_enabled,
               prior_authority.correction_presentation as prior_correction_presentation
        from current_observation
        full join prior_authority using(work_event_id)
        order by coalesce(current_observation.work_event_id,prior_authority.work_event_id)
      loop
        v_expense_policy_id:=v_expense_observation.expense_policy_snapshot_id;
        v_prior_expense_authority_id:=v_expense_observation.prior_authority_id;
        v_expense_authority_id:=null;
        v_expense_timesheet_id:=null;
        v_current_expense.id:=null;
        if v_expense_policy_id is not null then
          if private.weekly_source_finalisation_expense_policy_assert_v1(
               v_expense_observation.row_resolution_id,v_cycle.id
             ) is distinct from v_expense_policy_id then
            raise exception 'WEEKLY_SOURCE_EXPENSE_POLICY_SNAPSHOT_INVALID' using errcode='55000';
          end if;
          select * into strict v_expense_policy
          from public.weekly_source_row_expense_policy_snapshots
          where id=v_expense_policy_id;
          v_target_expense_pence:=v_expense_policy.source_expense_pence;
          v_target_expense_vat:=v_expense_policy.source_expense_vat_enabled;
          v_target_expense_contract_id:=v_expense_policy.contract_id;
          v_expense_observation_kind:='ROW_PRESENT';
          v_correction_presentation:=v_expense_policy.correction_presentation;
          if v_target_expense_pence>0 then
            perform public.weekly_source_timesheet_lineage_ensure_atomic_v1(
              v_expense_policy.row_resolution_id,v_actor
            );
            v_expense_timesheet_id:=private.weekly_source_finalisation_lineage_assert_v1(
              v_expense_policy.row_resolution_id,v_cycle.id
            );
          end if;
        else
          v_target_expense_pence:=0;
          v_target_expense_vat:=v_expense_observation.prior_expense_vat_enabled;
          v_target_expense_contract_id:=v_expense_observation.prior_contract_id;
          v_expense_observation_kind:='OMITTED_IN_COMPLETE_COVERAGE';
          v_correction_presentation:=v_expense_observation.prior_correction_presentation;
        end if;
        if v_prior_expense_authority_id is not null then
          select * into strict v_current_expense
          from public.weekly_expense_authority_generations expense
          where expense.id=v_prior_expense_authority_id
            and expense.state in ('CURRENT','SUPERSEDED')
            and expense.work_event_id=v_expense_observation.work_event_id
          for update;
        end if;
        if v_prior_expense_authority_id is null
           or v_current_expense.source_expense_pence is distinct from v_target_expense_pence
           or v_current_expense.source_expense_vat_enabled is distinct from v_target_expense_vat
           or v_current_expense.contract_id is distinct from v_target_expense_contract_id
           or v_current_expense.source_observation_kind is distinct from v_expense_observation_kind then
          if v_prior_expense_authority_id is not null then
            if not p_prepare_only then
              update public.weekly_expense_authority_generations
              set state='SUPERSEDED' where id=v_prior_expense_authority_id;
            end if;
          end if;
          select coalesce(pg_catalog.max(expense.generation),0)+1 into v_expense_generation
          from public.weekly_expense_authority_generations expense
          where expense.work_event_id=v_expense_observation.work_event_id;
          v_expense_hash:=private.weekly_source_sha256_jsonb_v1(
            'WEEKLY_SOURCE_EXPENSE_AUTHORITY_GENERATION_V1',
            pg_catalog.jsonb_build_object(
              'final_revision_id',v_revision_id,
              'work_event_id',v_expense_observation.work_event_id,
              'contract_id',v_target_expense_contract_id,
              'row_expense_policy_snapshot_id',v_expense_policy_id,
              'prior_expense_authority_generation_id',v_prior_expense_authority_id,
              'generation',v_expense_generation,
              'source_observation_kind',v_expense_observation_kind,
              'correction_presentation',v_correction_presentation,
              'source_expense_pence',v_target_expense_pence,
              'source_expense_vat_enabled',v_target_expense_vat
            )
          );
          insert into public.weekly_expense_authority_generations(
            final_revision_id,work_event_id,contract_id,row_expense_policy_snapshot_id,
            prior_expense_authority_generation_id,generation,source_observation_kind,
            correction_presentation,source_expense_pence,source_expense_vat_enabled,
            candidate_reimbursement_ex_vat,client_charge_ex_vat,authority_hash,state
          ) values (
            v_revision_id,v_expense_observation.work_event_id,v_target_expense_contract_id,
            v_expense_policy_id,v_prior_expense_authority_id,v_expense_generation,
            v_expense_observation_kind,v_correction_presentation,v_target_expense_pence,
            v_target_expense_vat,v_target_expense_pence::numeric/100,
            v_target_expense_pence::numeric/100,v_expense_hash,
            case when p_prepare_only then 'PREPARED' else 'CURRENT' end
          ) returning id into v_expense_authority_id;
          v_expense_movement_result:=
            private.weekly_source_finalisation_materialise_expense_invoice_v1(
              v_revision_id,v_cycle.id,v_source_profile_kind,
              v_expense_authority_id,v_prior_expense_authority_id,
              v_expense_policy_id,v_expense_timesheet_id,
              v_correction_presentation
            );
          v_movement_count:=v_movement_count+
            coalesce((v_expense_movement_result->>'movement_count')::integer,0);
          v_expense_generation_count:=v_expense_generation_count+1;
        end if;
        v_current_expense.id:=null;
      end loop;
    end if;
  else
    v_source_total_cost:=0;
    v_source_commission:=0;
    v_source_invoice_total:=0;
    v_line_count:=0;
    for v_row in
      select source_row.*,resolution.id as resolution_id,
             resolution.work_event_id,resolution.candidate_id,resolution.client_id,
             resolution.contract_id
      from public.weekly_source_upload_rows source_row
      join public.weekly_source_row_resolutions resolution
        on resolution.upload_row_id=source_row.id and resolution.generation=v_generation
      where source_row.upload_id=v_upload.id
      order by source_row.source_row_ordinal
    loop
      v_economic_id:=private.weekly_source_finalisation_economic_assert_v1(
        v_row.resolution_id,v_cycle.id,'NHSP_WEEKLY'
      );
      v_timesheet_id:=private.weekly_source_finalisation_lineage_assert_v1(
        v_row.resolution_id,v_cycle.id
      );
      select * into strict v_economic
      from public.weekly_source_row_economic_snapshots where id=v_economic_id;
      select * into v_charge_check
      from public.weekly_source_charge_checks charge_check
      where charge_check.upload_row_id=v_row.id
        and charge_check.row_resolution_id=v_row.resolution_id
        and charge_check.generation=v_generation;
      v_charge_acceptance.id:=null;
      if found and v_charge_check.comparison_result in ('MISMATCH','ZERO_SOURCE_CHARGE') then
        select * into v_charge_acceptance
        from public.weekly_source_charge_acceptances acceptance
        where acceptance.charge_check_id=v_charge_check.id
          and acceptance.upload_row_id=v_row.id
          and acceptance.row_resolution_id=v_row.resolution_id
          and acceptance.source_upload_id=v_upload.id
          and acceptance.contract_id=v_row.contract_id
          and acceptance.acceptance_kind=case v_charge_check.comparison_result
            when 'ZERO_SOURCE_CHARGE' then 'ACCEPTED_ZERO' else 'ACCEPTED_DISPARITY' end
          and acceptance.source_upload_hash=v_upload.content_sha256
          and acceptance.source_row_fingerprint=(select resolution.source_row_fingerprint
            from public.weekly_source_row_resolutions resolution where resolution.id=v_row.resolution_id)
          and acceptance.contract_and_rate_fingerprint=v_economic.contract_and_rate_fingerprint
          and acceptance.effective_policy_fingerprint=v_economic.effective_policy_fingerprint
          and acceptance.charge_calculation_fingerprint=v_economic.calculation_fingerprint
          and acceptance.acceptance_policy_fingerprint=
            private.weekly_source_charge_acceptance_policy_fingerprint_v1();
      end if;
      v_price_admission_result:=case v_charge_check.comparison_result
        when 'ZERO_SOURCE_CHARGE' then 'ACCEPTED_ZERO'
        when 'MISMATCH' then 'ACCEPTED_DISPARITY'
        else v_charge_check.comparison_result end;
      if not found or (select pg_catalog.count(*)
          from public.weekly_source_charge_checks charge_check
          where charge_check.upload_row_id=v_row.id
            and charge_check.row_resolution_id=v_row.resolution_id
            and charge_check.generation=v_generation)<>1
         or v_charge_check.source_commission_pence is distinct from v_row.source_commission_pence
         or v_charge_check.source_total_cost_pence is distinct from v_row.source_total_cost_pence
         or v_charge_check.source_shift_charge_pence is distinct from v_row.source_shift_charge_pence
         or v_charge_check.calculated_segment_charge_pence is distinct from v_economic.calculated_charge_pence
         or v_charge_check.source_charge_difference_pence is distinct from
              (v_row.source_shift_charge_pence-v_economic.calculated_charge_pence)
         or v_charge_check.charge_calculation_fingerprint is distinct from
              v_economic.calculation_fingerprint
         or v_charge_check.comparison_profile_version<>'NHSP_TWO_COMPONENT_PENCE_V1'
         or v_charge_check.comparison_result not in (
              'EXACT','SOURCE_ROUNDING_EQUIVALENT','MISMATCH','ZERO_SOURCE_CHARGE'
            )
         or not (
           (v_charge_check.comparison_result='EXACT'
             and v_charge_check.source_charge_difference_pence=0)
           or (v_charge_check.comparison_result='SOURCE_ROUNDING_EQUIVALENT'
             and v_charge_check.row_sign_kind in ('POSITIVE','FULL_NEGATIVE')
             and (v_charge_check.source_charge_difference_pence=1
               or v_charge_check.source_charge_difference_pence=-1)
             and ((v_charge_check.source_shift_charge_pence>0
                   and v_charge_check.calculated_segment_charge_pence>0)
               or (v_charge_check.source_shift_charge_pence<0
                   and v_charge_check.calculated_segment_charge_pence<0)))
           or (v_charge_check.comparison_result='MISMATCH'
             and v_charge_acceptance.id is not null
             and v_charge_acceptance.acceptance_kind='ACCEPTED_DISPARITY')
           or (v_charge_check.comparison_result='ZERO_SOURCE_CHARGE'
             and v_charge_check.source_commission_pence=0
             and v_charge_check.source_total_cost_pence=0
             and v_charge_check.source_shift_charge_pence=0
             and v_charge_check.calculated_segment_charge_pence>0
             and v_charge_acceptance.id is not null
             and v_charge_acceptance.acceptance_kind='ACCEPTED_ZERO')
         )
         or v_charge_check.row_sign_kind is distinct from (case
              when v_economic.row_sign=1 then 'POSITIVE' else 'FULL_NEGATIVE' end)
         or v_charge_check.phase_severity='FINALISATION_BLOCKER'
         or v_charge_check.blocker_code is not null then
        raise exception 'WEEKLY_SOURCE_NHSP_PRICE_GATE_FAILED'
          using errcode='55000',detail=v_row.source_row_ordinal::text;
      end if;
      v_movement_id:=private.weekly_source_finalisation_insert_current_movement_v1(
        null,v_row.id,v_revision_id,v_cycle.id,'NHSP_TRUST_BACKING_REPORT',
        case when v_economic.row_sign=1 then 'POSITIVE' else 'REVERSAL' end,
        case when v_economic.row_sign=1
          then 'NHSP_PHYSICAL_POSITIVE' else 'NHSP_PHYSICAL_FULL_NEGATIVE' end,
        v_row.resolution_id,v_economic.id,v_timesheet_id,
        v_row.source_shift_charge_pence,v_row.source_shift_charge_pence,
        v_price_admission_result,v_charge_check.charge_calculation_fingerprint,
        v_charge_acceptance.id,
        pg_catalog.jsonb_build_object(
          'schema_version','WEEKLY_SOURCE_NHSP_PHYSICAL_FACTS_V1',
          'backing_report_number',v_backing_report_number,
          'report_scope_id',v_report_scope_id,'source_row_ordinal',v_row.source_row_ordinal,
          'upload_row_id',v_row.id,'row_resolution_id',v_row.resolution_id,
          'work_date',v_row.work_date,'start_at_local',v_row.start_at_local,
          'end_at_local',v_row.end_at_local,'break_minutes',v_row.break_minutes,
          'actual_net_minutes',v_row.actual_net_minutes,
          'source_commission_pence',v_row.source_commission_pence,
          'source_total_cost_pence',v_row.source_total_cost_pence,
          'source_shift_charge_pence',v_row.source_shift_charge_pence,
          'calculated_comparison_charge_pence',v_economic.calculated_charge_pence,
          'comparison_result',v_charge_check.comparison_result,
          'price_admission_result',v_price_admission_result,
          'charge_acceptance_id',v_charge_acceptance.id,
          'physical_sign',case when v_economic.row_sign=1
            then 'POSITIVE' else 'FULL_NEGATIVE' end
        ),null,null
      );
      v_source_total_cost:=v_source_total_cost+v_row.source_total_cost_pence;
      v_source_commission:=v_source_commission+v_row.source_commission_pence;
      v_source_invoice_total:=v_source_invoice_total+v_row.source_shift_charge_pence;
      v_line_count:=v_line_count+1;
      v_movement_count:=v_movement_count+1;
    end loop;
    if v_line_count<>v_upload.accepted_count
       or v_source_invoice_total<>v_source_total_cost+v_source_commission then
      raise exception 'WEEKLY_SOURCE_NHSP_REPORT_TOTAL_INVALID' using errcode='55000';
    end if;
    insert into public.weekly_source_nhsp_backing_reports(
      report_scope_id,final_revision_id,upload_id,client_id,backing_report_number,
      cutoff_at_utc,physical_line_count,source_total_cost_pence,
      source_commission_pence,source_invoice_total_pence,row_manifest_hash
    ) values (
      v_report_scope_id,v_revision_id,v_upload.id,v_client_id,v_backing_report_number,
      v_scope.cutoff_at_utc,v_line_count,v_source_total_cost,v_source_commission,
      v_source_invoice_total,v_upload.row_manifest_hash
    );
  end if;

  for v_manifest in
    select client_id
    from (
      select movement.actual_client_id as client_id
      from public.weekly_source_billing_movements movement
      where movement.final_revision_id=v_revision_id
      union
      select v_client_id
    ) clients
    where client_id is not null
    order by client_id
  loop
    select private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_CLIENT_MANIFEST_V1',
      pg_catalog.jsonb_build_object(
        'final_revision_id',v_revision_id,'source_group_id',v_group.id,
        'source_cycle_id',v_cycle.id,'client_id',v_manifest.client_id,
        'finalisation_week_ending',v_cycle.finalisation_week_ending,
        'backing_report_number',v_backing_report_number,
        'movements',coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
          'movement_id',movement.id,
          'movement_economic_hash',pg_catalog.encode(movement.movement_economic_hash,'hex')
        ) order by pg_catalog.encode(movement.movement_economic_hash,'hex')),
        '[]'::jsonb),
        'expense_authorities',coalesce((
          select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
            'expense_authority_generation_id',expense.id,
            'authority_hash',pg_catalog.encode(expense.authority_hash,'hex')
          ) order by pg_catalog.encode(expense.authority_hash,'hex'))
          from public.weekly_expense_authority_generations expense
          join public.weekly_work_events event on event.id=expense.work_event_id
          where expense.final_revision_id=v_revision_id and event.client_id=v_manifest.client_id
        ),'[]'::jsonb)
      )||case when v_rule_version is null then '{}'::jsonb else jsonb_build_object('rule_set_version_id',v_rule_version,'rule_set_values_sha256',v_rule_set->>'values_sha256') end
    ) into v_client_manifest_hash
    from public.weekly_source_billing_movements movement
    where movement.final_revision_id=v_revision_id
      and movement.actual_client_id=v_manifest.client_id;
    insert into public.weekly_source_client_manifests(
      final_revision_id,source_group_id,source_cycle_id,client_id,
      finalisation_week_ending,backing_report_number,manifest_hash,movement_count,
      unchanged_snapshot_count,expense_line_count,invoice_state
    ) values (
      v_revision_id,v_group.id,v_cycle.id,v_manifest.client_id,
      v_cycle.finalisation_week_ending,v_backing_report_number,v_client_manifest_hash,
      (select pg_catalog.count(*)::integer
       from public.weekly_source_billing_movements movement
       where movement.final_revision_id=v_revision_id
         and movement.actual_client_id=v_manifest.client_id),
      (select pg_catalog.count(*)::integer
       from public.weekly_source_state_transitions transition_row
       join public.weekly_source_final_snapshot_lines snapshot_line
         on snapshot_line.id=transition_row.new_snapshot_line_id
       where transition_row.final_revision_id=v_revision_id
         and transition_row.outcome='NO_CHANGE'
         and snapshot_line.client_id=v_manifest.client_id),
      (select pg_catalog.count(*)::integer
       from public.weekly_expense_authority_generations expense
       join public.weekly_work_events event on event.id=expense.work_event_id
       where expense.final_revision_id=v_revision_id
         and event.client_id=v_manifest.client_id),
      'READY'
    ) returning id into v_client_manifest_id;
    insert into public.weekly_source_manifest_movements(
      client_manifest_id,billing_movement_id,manifest_ordinal,movement_hash
    )
    select v_client_manifest_id,movement.id,
           pg_catalog.row_number() over(
             order by pg_catalog.encode(movement.movement_economic_hash,'hex')
           )::integer,
           movement.movement_economic_hash
    from public.weekly_source_billing_movements movement
    where movement.final_revision_id=v_revision_id
      and movement.actual_client_id=v_manifest.client_id;
  end loop;

  if v_finalisation_lock_operation is not null then
    -- Includes the later positive-expense lineage calls. Advisory/row locks
    -- remain transaction-held; remove only this operation's working copies.
    perform private.bpay_next_source_current_lock_release_v1(v_finalisation_lock_operation);
  end if;
  if v_finalisation_lock_context then
    perform pg_catalog.set_config('cloudtms.weekly_source_finalisation_lock_operation',v_prior_finalisation_lock_operation,true);
    perform pg_catalog.set_config('cloudtms.weekly_source_finalisation_root_set',v_prior_finalisation_root_set,true);
  end if;

  if p_prepare_only then
    return pg_catalog.jsonb_build_object(
      'ok',true,'status','PREPARED','idempotent',false,
      'final_revision_id',v_revision_id,'source_cycle_id',v_cycle.id,
      'authority_scope_kind',v_scope_kind,'report_scope_id',v_report_scope_id,
      'upload_id',v_upload.id,'profile_code',v_profile.profile_code,
      'client_id',v_client_id,'snapshot_count',v_snapshot_count,
      'transition_count',v_transition_count,'movement_count',v_movement_count,
      'unchanged_count',v_unchanged_count,
      'expense_generation_count',v_expense_generation_count
    );
  end if;

  update public.weekly_source_client_cycle_completions completion
  set state='SUPERSEDED',superseded_at_utc=v_finalised_at
  where completion.source_cycle_id=v_cycle.id
    and completion.client_id=v_client_id and completion.state='CURRENT';
  select coalesce(pg_catalog.max(completion.completion_generation),0)+1
  into v_completion_generation
  from public.weekly_source_client_cycle_completions completion
  where completion.source_cycle_id=v_cycle.id and completion.client_id=v_client_id;
  v_completion_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_CLIENT_CYCLE_COMPLETION_V1',
    pg_catalog.jsonb_build_object(
      'source_cycle_id',v_cycle.id,'source_group_id',v_group.id,
      'client_id',v_client_id,'completion_generation',v_completion_generation,
      'completion_kind','FINAL_SOURCE','final_revision_id',v_revision_id,
      'actor_user_id',v_actor
    )
  );
  insert into public.weekly_source_client_cycle_completions(
    source_cycle_id,source_group_id,client_id,completion_generation,
    completion_kind,final_revision_id,attested_by_user_id,attested_at_utc,
    attestation_text,completion_hash,state
  ) values (
    v_cycle.id,v_group.id,v_client_id,v_completion_generation,
    'FINAL_SOURCE',v_revision_id,v_actor,v_finalised_at,null,v_completion_hash,'CURRENT'
  );

  if v_scope_kind='CYCLE' then
    update public.weekly_source_cycles
    set current_final_revision_id=v_revision_id,state='FINALISED',
        finalised_at_utc=v_finalised_at,finalised_by_user_id=v_actor
    where id=v_cycle.id and current_complete_upload_id=v_upload.id
      and current_projection_publication_id=v_publication.id
      and version=v_scope_version and current_final_revision_id is null;
    if not found then
      raise exception 'WEEKLY_SOURCE_FINALISE_CAS_LOST' using errcode='40001';
    end if;
  else
    update public.weekly_source_report_scopes
    set current_final_revision_id=v_revision_id,state='FINALISED',
        updated_at_utc=v_finalised_at
    where id=v_scope.id and current_complete_upload_id=v_upload.id
      and current_projection_publication_id=v_publication.id
      and version=v_scope_version and current_final_revision_id is null;
    if not found then
      raise exception 'WEEKLY_SOURCE_FINALISE_CAS_LOST' using errcode='40001';
    end if;
    if not exists(
      select 1
      from public.weekly_source_group_clients membership
      where membership.source_group_id=v_group.id
        and v_cycle.finalisation_week_ending between membership.valid_from
          and coalesce(membership.valid_to,'infinity'::date)
        and not exists(
          select 1
          from public.weekly_source_client_cycle_completions completion
          where completion.source_cycle_id=v_cycle.id
            and completion.client_id=membership.client_id
            and completion.state='CURRENT'
        )
    ) then
      update public.weekly_source_cycles
      set state='FINALISED',finalised_at_utc=v_finalised_at,
          finalised_by_user_id=v_actor
      where id=v_cycle.id;
    else
      update public.weekly_source_cycles
      set state='FINALISABLE'
      where id=v_cycle.id and state in ('OPEN','FINALISABLE');
    end if;
  end if;

  -- Only this genuine new CURRENT transition may resolve contained manual
  -- queries. PREPARED and retained Final replay returned earlier. Query-only
  -- resolution shares this transaction; it never changes frozen pay or money.
  perform private.weekly_source_manual_reviews_finalised_v1(v_revision_id,v_actor);

  -- PREPARED returned above and remains invisible. Capture only AFTER the
  -- real cycle/report CURRENT CAS; any failure rolls back Source and selector
  -- together. The same immutable economic/lineage facts remain authoritative.
  if coalesce(v_next_source_hooks,false) then
    perform private.bpay_next_source_current_publish_revision_v1(v_revision_id);
  end if;

  if p_correction_session_id is null and exists(
    select 1
    from public.weekly_source_cycles cycle
    where cycle.id=v_cycle.id and cycle.state='FINALISED'
  ) then
    perform private._weekly_source_settings_ensure_open_cycle_v1(
      v_group.id,v_finalised_at
    );
  end if;

  return pg_catalog.jsonb_build_object(
    'ok',true,'status',case when p_correction_session_id is null
      then 'FINALISED' else 'CORRECTED' end,'idempotent',false,
    'final_revision_id',v_revision_id,'source_cycle_id',v_cycle.id,
    'authority_scope_kind',v_scope_kind,'report_scope_id',v_report_scope_id,
    'upload_id',v_upload.id,'profile_code',v_profile.profile_code,
    'client_id',v_client_id,'snapshot_count',v_snapshot_count,
    'transition_count',v_transition_count,'movement_count',v_movement_count,
    'unchanged_count',v_unchanged_count,
    'expense_generation_count',v_expense_generation_count
  );
exception
  when invalid_text_representation or numeric_value_out_of_range or datetime_field_overflow then
    -- The function subtransaction already rolled back Source/cache/control
    -- writes on entry to this handler. Restore only the caller's exact GUCs.
    if v_finalisation_lock_context then
      perform pg_catalog.set_config('cloudtms.weekly_source_finalisation_lock_operation',v_prior_finalisation_lock_operation,true);
      perform pg_catalog.set_config('cloudtms.weekly_source_finalisation_root_set',v_prior_finalisation_root_set,true);
    end if;
    raise exception 'WEEKLY_SOURCE_FINALISE_VALUE_INVALID' using errcode='22023';
  when others then
    if v_finalisation_lock_context then
      perform pg_catalog.set_config('cloudtms.weekly_source_finalisation_lock_operation',v_prior_finalisation_lock_operation,true);
      perform pg_catalog.set_config('cloudtms.weekly_source_finalisation_root_set',v_prior_finalisation_root_set,true);
    end if;
    raise;
end;
$function$;

create or replace function public.weekly_source_timesheet_lineage_ensure_atomic_v1(
  p_row_resolution_id uuid,
  p_actor_user_id uuid
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','extensions','pg_catalog','pg_temp'
as $function$
declare
  v_resolution public.weekly_source_row_resolutions%rowtype;
  v_source_row public.weekly_source_upload_rows%rowtype;
  v_upload public.weekly_source_uploads%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_scope public.weekly_source_report_scopes%rowtype;
  v_publication public.weekly_source_projection_publications%rowtype;
  v_correction public.weekly_final_source_correction_sessions%rowtype;
  v_profile public.weekly_source_format_profiles%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_event public.weekly_work_events%rowtype;
  v_contract public.contracts%rowtype;
  v_candidate public.candidates%rowtype;
  v_client public.clients%rowtype;
  v_expense_policy public.weekly_source_row_expense_policy_snapshots%rowtype;
  v_policy jsonb;
  v_contract_week public.contract_weeks%rowtype;
  v_timesheet public.timesheets%rowtype;
  v_lineage public.weekly_source_row_timesheet_lineages%rowtype;
  v_guard jsonb;
  v_contract_week_settings jsonb;
  v_timesheet_settings jsonb;
  v_week_ending_date date;
  v_weekday integer;
  v_booking_id text;
  v_booking_material text;
  v_occupant_norm text;
  v_hospital_norm text;
  v_ward_norm text;
  v_role_norm text;
  v_lineage_fingerprint bytea;
  v_expected_expense_policy_hash bytea;
  v_created_timesheet boolean:=false;
  v_existing_lineage boolean:=false;
  v_is_correction boolean:=false;
  v_lock_result jsonb;
  v_lineage_lock_phase integer;
  v_lineage_discovery jsonb;
  v_lineage_observed jsonb;
  v_family jsonb;
  v_known_root_timesheet_id uuid;
  v_root_authorisation jsonb;
  v_bound_timesheet_id uuid;
  v_finalisation_root_set text;
  v_finalisation_lock_operation_text text;
  v_finalisation_lock_operation private.bpay_next_source_current_lock_operations%rowtype;
begin
  if coalesce(
       pg_catalog.current_setting('request.jwt.claim.role',true),
       nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role',
       ''
     )<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;
  if p_row_resolution_id is null or p_actor_user_id is null then
    raise exception 'WEEKLY_SOURCE_TIMESHEET_LINEAGE_INPUT_REQUIRED' using errcode='22023';
  end if;

  select * into v_resolution
  from public.weekly_source_row_resolutions
  where id=p_row_resolution_id;
  if not found or v_resolution.mapping_state<>'RESOLVED' then
    raise exception 'WEEKLY_SOURCE_RESOLUTION_NOT_CURRENT' using errcode='55000';
  end if;

  select * into strict v_source_row
  from public.weekly_source_upload_rows
  where id=v_resolution.upload_row_id;
  select * into strict v_upload
  from public.weekly_source_uploads
  where id=v_source_row.upload_id;

  perform private.weekly_source_pay_query_admit_v2();
  -- G1 standalone and inherited FIN paths share the same physical I-1 owner.
  -- Phase1 is plain key/scope admission plus its unchanged family call. Phase2
  -- locks discovered group -> cycle and revalidates every selector before DML.
  for v_lineage_lock_phase in 1..2 loop
    if v_lineage_lock_phase=2 then
      select * into v_group from public.weekly_source_groups
      where id=(v_lineage_discovery#>>'{group,id}')::uuid for share;
      if not found or to_jsonb(v_group) is distinct from v_lineage_discovery->'group' then
        raise exception 'WEEKLY_SOURCE_LINEAGE_SCOPE_CHANGED_DURING_LOCK' using errcode='40001'; end if;
    end if;
  if v_lineage_lock_phase=2 then
  select * into v_cycle
  from public.weekly_source_cycles
  where id=v_upload.source_cycle_id
  for update;
    if not found or to_jsonb(v_cycle) is distinct from v_lineage_discovery->'cycle' then
      raise exception 'WEEKLY_SOURCE_LINEAGE_SCOPE_CHANGED_DURING_LOCK' using errcode='40001'; end if;
  else
  select * into v_cycle
  from public.weekly_source_cycles
  where id=v_upload.source_cycle_id;
  end if;
  if not found then
    raise exception 'WEEKLY_SOURCE_CYCLE_NOT_FOUND' using errcode='55000';
  end if;

  if v_upload.report_scope_id is not null then
    if v_lineage_lock_phase=2 then
  select * into v_scope
    from public.weekly_source_report_scopes
    where id=v_upload.report_scope_id
    for update;
    if not found or to_jsonb(v_scope) is distinct from v_lineage_discovery->'scope' then
      raise exception 'WEEKLY_SOURCE_LINEAGE_SCOPE_CHANGED_DURING_LOCK' using errcode='40001'; end if;
  else
  select * into v_scope
    from public.weekly_source_report_scopes
    where id=v_upload.report_scope_id;
  end if;
    if not found or v_scope.source_cycle_id is distinct from v_cycle.id then
      raise exception 'WEEKLY_SOURCE_REPORT_SCOPE_NOT_CURRENT' using errcode='55000';
    end if;
  end if;

  if v_lineage_lock_phase=2 then
  select * into v_upload
  from public.weekly_source_uploads
  where id=v_source_row.upload_id
    and source_cycle_id=v_cycle.id
  for share;
    if not found or to_jsonb(v_upload) is distinct from v_lineage_discovery->'upload' then
      raise exception 'WEEKLY_SOURCE_LINEAGE_SCOPE_CHANGED_DURING_LOCK' using errcode='40001'; end if;
  else
  select * into v_upload
  from public.weekly_source_uploads
  where id=v_source_row.upload_id
    and source_cycle_id=v_cycle.id;
  end if;
  if not found then
    raise exception 'WEEKLY_SOURCE_UPLOAD_NOT_CURRENT' using errcode='55000';
  end if;

  if v_lineage_lock_phase=2 then
  select * into v_publication
  from public.weekly_source_projection_publications publication
  where publication.source_cycle_id=v_cycle.id
    and publication.upload_id=v_upload.id
    and publication.authority_scope_version=v_resolution.generation
    and (
      (
        publication.state='CURRENT'
        and v_upload.purpose='ORDINARY'
        and v_upload.correction_session_id is null
        and (
          (v_upload.report_scope_id is null
           and publication.authority_scope_kind='CYCLE'
           and publication.report_scope_id is null
           and v_cycle.current_projection_publication_id=publication.id
           and v_cycle.current_complete_upload_id=v_upload.id)
          or
          (v_upload.report_scope_id is not null
           and publication.authority_scope_kind='NHSP_REPORT_SCOPE'
           and publication.report_scope_id=v_upload.report_scope_id
           and v_scope.current_projection_publication_id=publication.id
           and v_scope.current_complete_upload_id=v_upload.id)
        )
      )
      or (
        publication.state='CORRECTION_READY'
        and v_upload.state='CORRECTION_READY'
        and v_upload.purpose='FINAL_SOURCE_CORRECTION'
        and v_upload.correction_session_id is not null
        and publication.correction_session_id=v_upload.correction_session_id
        and (
          (v_upload.report_scope_id is null
           and publication.authority_scope_kind='CYCLE'
           and publication.report_scope_id is null)
          or
          (v_upload.report_scope_id is not null
           and publication.authority_scope_kind='NHSP_REPORT_SCOPE'
           and publication.report_scope_id=v_upload.report_scope_id)
        )
      )
    )
  for share;
    if not found or to_jsonb(v_publication) is distinct from v_lineage_discovery->'publication' then
      raise exception 'WEEKLY_SOURCE_LINEAGE_SCOPE_CHANGED_DURING_LOCK' using errcode='40001'; end if;
  else
  select * into v_publication
  from public.weekly_source_projection_publications publication
  where publication.source_cycle_id=v_cycle.id
    and publication.upload_id=v_upload.id
    and publication.authority_scope_version=v_resolution.generation
    and (
      (
        publication.state='CURRENT'
        and v_upload.purpose='ORDINARY'
        and v_upload.correction_session_id is null
        and (
          (v_upload.report_scope_id is null
           and publication.authority_scope_kind='CYCLE'
           and publication.report_scope_id is null
           and v_cycle.current_projection_publication_id=publication.id
           and v_cycle.current_complete_upload_id=v_upload.id)
          or
          (v_upload.report_scope_id is not null
           and publication.authority_scope_kind='NHSP_REPORT_SCOPE'
           and publication.report_scope_id=v_upload.report_scope_id
           and v_scope.current_projection_publication_id=publication.id
           and v_scope.current_complete_upload_id=v_upload.id)
        )
      )
      or (
        publication.state='CORRECTION_READY'
        and v_upload.state='CORRECTION_READY'
        and v_upload.purpose='FINAL_SOURCE_CORRECTION'
        and v_upload.correction_session_id is not null
        and publication.correction_session_id=v_upload.correction_session_id
        and (
          (v_upload.report_scope_id is null
           and publication.authority_scope_kind='CYCLE'
           and publication.report_scope_id is null)
          or
          (v_upload.report_scope_id is not null
           and publication.authority_scope_kind='NHSP_REPORT_SCOPE'
           and publication.report_scope_id=v_upload.report_scope_id)
        )
      )
    );
  end if;
  if not found then
    raise exception 'WEEKLY_SOURCE_PROJECTION_NOT_CURRENT' using errcode='55000';
  end if;

  v_is_correction:=v_publication.state='CORRECTION_READY';
  if v_is_correction then
    if v_lineage_lock_phase=2 then
  select * into v_correction
    from public.weekly_final_source_correction_sessions correction
    where correction.id=v_upload.correction_session_id
    for share;
    if not found or to_jsonb(v_correction) is distinct from v_lineage_discovery->'correction' then
      raise exception 'WEEKLY_SOURCE_LINEAGE_SCOPE_CHANGED_DURING_LOCK' using errcode='40001'; end if;
  else
  select * into v_correction
    from public.weekly_final_source_correction_sessions correction
    where correction.id=v_upload.correction_session_id;
  end if;
    if not found or v_correction.state<>'PREPARING'
       or v_correction.source_cycle_id is distinct from v_cycle.id
       or v_correction.authority_scope_kind is distinct from
          (case when v_upload.report_scope_id is null then 'CYCLE'
                else 'NHSP_REPORT_SCOPE' end)
       or v_correction.report_scope_id is distinct from v_upload.report_scope_id
       or v_correction.replacement_correction_upload_id is distinct from v_upload.id
       or v_correction.replacement_projection_publication_id is distinct from v_publication.id
       or coalesce(v_scope.current_final_revision_id,v_cycle.current_final_revision_id)
            is distinct from v_correction.expected_current_final_revision_id then
      raise exception 'WEEKLY_SOURCE_CORRECTION_SESSION_STALE' using errcode='40001';
    end if;
  else
    v_guard:=private.weekly_source_current_publication_guard_v1(
      v_cycle.id,
      case when v_upload.report_scope_id is null then 'CYCLE' else 'NHSP_REPORT_SCOPE' end,
      v_upload.report_scope_id,
      v_upload.id,
      v_publication.id,
      v_publication.authority_scope_version
    );
    if coalesce((v_guard->>'ok')::boolean,false) is not true
       or v_upload.purpose is distinct from 'ORDINARY'
       or v_upload.correction_session_id is not null then
      raise exception 'WEEKLY_SOURCE_PROJECTION_NOT_CURRENT' using errcode='55000';
    end if;
  end if;

  if v_lineage_lock_phase=2 then
  select * into v_resolution
  from public.weekly_source_row_resolutions
  where id=p_row_resolution_id
    and upload_row_id=v_source_row.id
    and generation=v_publication.authority_scope_version
  for update;
    if not found or to_jsonb(v_resolution) is distinct from v_lineage_discovery->'resolution' then
      raise exception 'WEEKLY_SOURCE_LINEAGE_SCOPE_CHANGED_DURING_LOCK' using errcode='40001'; end if;
  else
  select * into v_resolution
  from public.weekly_source_row_resolutions
  where id=p_row_resolution_id
    and upload_row_id=v_source_row.id
    and generation=v_publication.authority_scope_version;
  end if;
  if not found or v_resolution.mapping_state<>'RESOLVED' then
    raise exception 'WEEKLY_SOURCE_RESOLUTION_NOT_CURRENT' using errcode='55000';
  end if;
  select * into strict v_source_row
  from public.weekly_source_upload_rows
  where id=v_resolution.upload_row_id and upload_id=v_upload.id;

  select * into strict v_profile
  from public.weekly_source_format_profiles
  where id=v_upload.source_format_profile_id;
  select * into strict v_group
  from public.weekly_source_groups
  where id=v_cycle.source_group_id;
  select * into strict v_event
  from public.weekly_work_events
  where id=v_resolution.work_event_id;
  select * into v_contract
  from public.contracts
  where id=v_resolution.contract_id;
  if not found then
    raise exception 'WEEKLY_SOURCE_CONTRACT_NOT_FOUND' using errcode='55000';
  end if;

  if v_upload.report_scope_id is not null and not v_is_correction then
    if v_scope.source_cycle_id is distinct from v_cycle.id
       or v_scope.client_id is distinct from v_resolution.client_id
       or v_scope.current_projection_publication_id is distinct from v_publication.id
       or v_scope.current_complete_upload_id is distinct from v_upload.id then
      raise exception 'WEEKLY_SOURCE_REPORT_SCOPE_NOT_CURRENT' using errcode='55000';
    end if;
  end if;

  if not v_profile.active
     or v_profile.profile_code not in (
       'NHSP_FINAL_BACKING_V1',
       'HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1',
       'HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1',
       'ROSTER_WEEKLY_SUMMARY_ACTUAL_V1'
     )
     or v_source_row.row_finalisation_state not in (
       'NOT_APPLICABLE','SOURCE_WORKED','SOURCE_ABSENT_ZERO'
     )
     or (v_source_row.row_finalisation_state='SOURCE_ABSENT_ZERO'
         and v_profile.profile_code<>'ROSTER_WEEKLY_SUMMARY_ACTUAL_V1')
     or v_resolution.work_event_id is distinct from v_event.id
     or v_resolution.candidate_id is distinct from v_event.candidate_id
     or v_resolution.client_id is distinct from v_event.client_id
     or v_source_row.work_date is distinct from v_event.work_date
     or v_resolution.client_id is distinct from v_contract.client_id
     or v_resolution.candidate_id is distinct from v_contract.candidate_id
     or v_source_row.work_date not between v_contract.start_date and v_contract.end_date then
    raise exception 'WEEKLY_SOURCE_TIMESHEET_LINEAGE_SCOPE_INVALID' using errcode='55000';
  end if;

  v_policy:=private._weekly_source_effective_policy_v1(
    v_resolution.client_id,v_resolution.contract_id,v_source_row.work_date
  );
  if v_policy->>'authority_mode'<>'SOURCE_AUTHORITY'
     or coalesce((v_policy->>'self_bill_enabled')::boolean,false) is not true
     or v_policy->>'c1_source_mode' not in ('NHSP_WEEKLY','HEALTHROSTER_WEEKLY')
     or private.weekly_source_projection_hex32_v1(
          v_policy->>'policy_sha256','WEEKLY_SOURCE_POLICY_FINGERPRINT_INVALID'
        ) is distinct from v_resolution.effective_policy_fingerprint
     or (v_group.source_family='NHSP') is distinct from
        (v_policy->>'c1_source_mode'='NHSP_WEEKLY')
     or (v_profile.final_authority_kind='NHSP_TRUST_BACKING_REPORT') is distinct from
        (v_group.source_family='NHSP') then
    raise exception 'WEEKLY_SOURCE_TIMESHEET_LINEAGE_POLICY_INVALID' using errcode='55000';
  end if;

  if v_source_row.row_finalisation_state='SOURCE_ABSENT_ZERO' then
    if coalesce((v_policy->>'source_fixed_expenses_enabled')::boolean,false) is not true
       or v_source_row.source_expense_parse_state<>'VALID'
       or coalesce(v_source_row.source_expense_pence,0)<=0
       or v_source_row.actual_net_minutes<>0
       or v_source_row.start_at_local is null
       or v_source_row.end_at_local is null
       or v_source_row.end_at_local<=v_source_row.start_at_local
       or v_source_row.break_minutes is null
       or exists(
         select 1 from public.weekly_source_row_economic_snapshots economic
         where economic.row_resolution_id=v_resolution.id
       ) then
      raise exception 'WEEKLY_SOURCE_ZERO_HOUR_EXPENSE_LINEAGE_INVALID' using errcode='55000';
    end if;

    select * into v_expense_policy
    from public.weekly_source_row_expense_policy_snapshots expense_policy
    where expense_policy.row_resolution_id=v_resolution.id;
    if not found or (
      select pg_catalog.count(*)
      from public.weekly_source_row_expense_policy_snapshots expense_policy
      where expense_policy.row_resolution_id=v_resolution.id
    )<>1 then
      raise exception 'WEEKLY_SOURCE_EXPENSE_POLICY_SNAPSHOT_CARDINALITY' using errcode='55000';
    end if;
    v_expected_expense_policy_hash:=private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_ROW_EXPENSE_POLICY_SNAPSHOT_V1',
      pg_catalog.jsonb_build_object(
        'row_resolution_id',v_resolution.id,'upload_row_id',v_source_row.id,
        'generation',v_resolution.generation,'work_event_id',v_resolution.work_event_id,
        'candidate_id',v_resolution.candidate_id,'client_id',v_resolution.client_id,
        'contract_id',v_resolution.contract_id,
        'row_finalisation_state',v_source_row.row_finalisation_state,
        'source_expense_pence',v_source_row.source_expense_pence,
        'source_expense_parse_state',v_source_row.source_expense_parse_state,
        'source_expense_vat_enabled',v_expense_policy.source_expense_vat_enabled,
        'invoice_vat_chargeable',v_expense_policy.invoice_vat_chargeable,
        'invoice_vat_rate_pct',v_expense_policy.invoice_vat_rate_pct,
        'correction_presentation',v_expense_policy.correction_presentation,
        'effective_policy_fingerprint',
          pg_catalog.encode(v_expense_policy.effective_policy_fingerprint,'hex'),
        'invoice_vat_policy_fingerprint',
          pg_catalog.encode(v_expense_policy.invoice_vat_policy_fingerprint,'hex')
      )
    );
    if v_expense_policy.upload_row_id is distinct from v_source_row.id
       or v_expense_policy.generation is distinct from v_resolution.generation
       or v_expense_policy.work_event_id is distinct from v_resolution.work_event_id
       or v_expense_policy.candidate_id is distinct from v_resolution.candidate_id
       or v_expense_policy.client_id is distinct from v_resolution.client_id
       or v_expense_policy.contract_id is distinct from v_resolution.contract_id
       or v_expense_policy.source_expense_pence is distinct from v_source_row.source_expense_pence
       or v_expense_policy.source_expense_parse_state is distinct from v_source_row.source_expense_parse_state
       or v_expense_policy.effective_policy_fingerprint is distinct from
          v_resolution.effective_policy_fingerprint
       or v_expense_policy.source_expense_vat_enabled is distinct from
          coalesce((v_policy->>'source_expense_vat_enabled')::boolean,false)
       or v_expense_policy.correction_presentation is distinct from
          pg_catalog.upper(pg_catalog.btrim(coalesce(
            v_policy->>'self_bill_correction_presentation',''
          )))
       or v_expense_policy.snapshot_hash is distinct from v_expected_expense_policy_hash then
      raise exception 'WEEKLY_SOURCE_EXPENSE_POLICY_SNAPSHOT_INVALID' using errcode='55000';
    end if;
  elsif (select pg_catalog.count(*)
         from public.weekly_source_row_economic_snapshots economic
         where economic.row_resolution_id=v_resolution.id)<>1 then
    raise exception 'WEEKLY_SOURCE_ECONOMIC_SNAPSHOT_CARDINALITY' using errcode='55000';
  end if;

  perform private.weekly_source_office_authority_v1(
    p_actor_user_id,'FINALISE_WEEK',v_group.id,v_resolution.client_id,
    v_cycle.finalisation_week_ending
  );

  v_weekday:=v_contract.week_ending_weekday_snapshot;
  if v_weekday not between 0 and 6 then
    raise exception 'WEEKLY_SOURCE_WEEK_ENDING_POLICY_INVALID' using errcode='55000';
  end if;
  v_week_ending_date:=v_source_row.work_date+
    ((v_weekday-extract(dow from v_source_row.work_date)::integer+7)%7);

  -- Note on the Candidate serial gate (proof/32 section 6 step 1): this owner
  -- binds a resolved source row to its ordinary base Weekly Timesheet.  It is
  -- not one of the four pinned Weekly Source job types (first authorisation,
  -- first-authorisation withdrawal, entitlement publication, pending release),
  -- so it does not take the gate; the first-authorisation owner does, through
  -- interface I-1.  It does take the full rotation lock set below.

  -- Review F1 and F4.  Every installed rotation owner takes the family advisory
  -- key and then the family rows, and only then contract_weeks (core
  -- 08082026_2035_...:126-131 then :190-193; legacy 16122025_...:844-849;
  -- confirmed route :2374-2376 then :2384-2385).  This owner previously locked
  -- the Contract Week, the Contract and the root row FIRST and asked for the
  -- family advisory keys afterwards, which is the reverse order and a proven
  -- deadlock (reproductions D1 and D2).  The root is therefore discovered with a
  -- PLAIN read, the I-1 lock set is taken FIRST, and every other lock follows.
  select contract_week.timesheet_id into v_known_root_timesheet_id
  from public.contract_weeks contract_week
  where contract_week.contract_id=v_resolution.contract_id
    and contract_week.week_ending_date=v_week_ending_date
    and contract_week.additional_seq=0;

  if v_lineage_lock_phase=2 and v_known_root_timesheet_id is distinct from
      nullif(v_lineage_discovery->>'root_timesheet_id','')::uuid then
    raise exception 'WEEKLY_SOURCE_BASE_WEEK_ROOT_CHANGED_DURING_LOCK' using errcode='40001'; end if;

  -- Review G2, the finalisation side of the same rule.  When a finalisation has
  -- already taken the complete sorted family lock set for this cycle, it
  -- publishes that set here.  A root this owner discovers that the set does not
  -- contain would be locked out of that sorted order, so refuse retryably
  -- instead.  Outside a finalisation the setting is absent and nothing changes.
  v_finalisation_lock_operation_text:=nullif(pg_catalog.btrim(coalesce(
    pg_catalog.current_setting('cloudtms.weekly_source_finalisation_lock_operation',true),''
  )), '');
  if v_finalisation_lock_operation_text is not null
     and (select active_owner from private.bpay_next_module_control where id=1)='NEXT' then
    -- One owner-private operation UUID replaces the whole-root CSV only for
    -- NEXT. It is not authority by itself: bind transaction and actual Source
    -- upload/publication/generation/exact old correction revision first.
    select o.* into v_finalisation_lock_operation
    from private.bpay_next_source_current_lock_operations o
    where o.id=v_finalisation_lock_operation_text::uuid
      and o.transaction_id=pg_catalog.pg_current_xact_id()
      and o.upload_id=v_upload.id and o.projection_publication_id=v_publication.id
      and o.generation=v_resolution.generation
      and o.old_revision_id is not distinct from case when v_is_correction
        then v_correction.expected_current_final_revision_id else null end
      and o.source_group_id=v_group.id and o.client_id=v_resolution.client_id;
    if not found then
      raise exception 'WEEKLY_SOURCE_FINALISATION_LOCK_OPERATION_UNBOUND' using errcode='40001';
    end if;
    if v_known_root_timesheet_id is not null and not exists(
      select 1 from private.bpay_next_source_current_lock_members m
      join public.timesheets t on t.timesheet_id=m.root_timesheet_id
      where m.operation_id=v_finalisation_lock_operation.id
        and m.root_timesheet_id=v_known_root_timesheet_id and m.raw_booking_id=t.booking_id
    ) then
      raise exception 'WEEKLY_SOURCE_FINALISATION_ROOT_SET_CHANGED_DURING_LOCK' using errcode='40001';
    end if;
  else
  -- Retain the unrelated/default OFF Source guard exactly as before.
  v_finalisation_root_set:=nullif(pg_catalog.btrim(coalesce(
    pg_catalog.current_setting('cloudtms.weekly_source_finalisation_root_set',true),''
  )),'');
  if v_finalisation_root_set is not null
     and v_known_root_timesheet_id is not null
     and (v_finalisation_root_set='EMPTY_LOCKED_SET'
       or not (v_known_root_timesheet_id::text
              =any(pg_catalog.string_to_array(v_finalisation_root_set,',')))) then
    raise exception 'WEEKLY_SOURCE_FINALISATION_ROOT_SET_CHANGED_DURING_LOCK'
      using errcode='40001',
        detail=pg_catalog.jsonb_build_object(
          'code','WEEKLY_SOURCE_FINALISATION_ROOT_SET_CHANGED_DURING_LOCK',
          'observed_root_timesheet_id',v_known_root_timesheet_id
        )::text;
  end if;
  end if;

  if v_known_root_timesheet_id is not null and v_lineage_lock_phase=1 then
    v_lock_result:=private.weekly_source_lock_family_rows_v1(
      array[v_known_root_timesheet_id]::uuid[],v_resolution.candidate_id
    );
    if coalesce((v_lock_result->>'ok')::boolean,false) is not true then
      raise exception '%',v_lock_result->>'code'
        using errcode='55000',detail=v_lock_result::text;
    end if;
    v_family:=v_lock_result->'families'->0;
  end if;

  v_lineage_observed:=jsonb_build_object('cycle',to_jsonb(v_cycle),'group',to_jsonb(v_group),
    'scope',to_jsonb(v_scope),'upload',to_jsonb(v_upload),'publication',to_jsonb(v_publication),
    'correction',to_jsonb(v_correction),'resolution',to_jsonb(v_resolution),'source_row',to_jsonb(v_source_row),
    'profile',to_jsonb(v_profile),'event',to_jsonb(v_event),'contract',to_jsonb(v_contract),
    'policy',v_policy,'expense_policy',to_jsonb(v_expense_policy),
    'week_ending_date',v_week_ending_date,'root_timesheet_id',v_known_root_timesheet_id);
  if v_lineage_lock_phase=1 then v_lineage_discovery:=v_lineage_observed;
  elsif v_lineage_observed is distinct from v_lineage_discovery then
    raise exception 'WEEKLY_SOURCE_LINEAGE_SCOPE_CHANGED_DURING_LOCK' using errcode='40001'; end if;
  end loop;

  -- A missing base Contract Week has no row to lock, so use the same scoped
  -- advisory lock before selecting/inserting it.  This is not a business key
  -- or an externally visible lock, and it is now taken after the family keys.
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
    'WEEKLY_SOURCE_BASE_WEEK|'||v_resolution.contract_id::text||'|'||v_week_ending_date::text,
    0
  ));

  insert into public.contract_weeks(
    contract_id,week_ending_date,additional_seq,status,
    submission_mode_snapshot,timesheet_id,is_adjustment,created_at,updated_at
  ) values (
    v_resolution.contract_id,v_week_ending_date,0,
    'SUBMITTED'::public.contract_week_status_enum,
    'MANUAL'::public.submission_mode_enum,null,false,
    pg_catalog.statement_timestamp(),pg_catalog.statement_timestamp()
  ) on conflict (contract_id,week_ending_date,additional_seq) do nothing;

  select * into strict v_contract_week
  from public.contract_weeks
  where contract_id=v_resolution.contract_id
    and week_ending_date=v_week_ending_date
    and additional_seq=0
  for update;

  -- Existing manual week owners lock the base week before the Contract.  Keep
  -- that order, then revalidate every mutable Contract fact and recompute the
  -- week end while both rows are protected.
  select * into v_contract
  from public.contracts
  where id=v_resolution.contract_id
  for update;
  if not found
     or v_resolution.client_id is distinct from v_contract.client_id
     or v_resolution.candidate_id is distinct from v_contract.candidate_id
     or v_source_row.work_date not between v_contract.start_date and v_contract.end_date
     or v_contract.week_ending_weekday_snapshot not between 0 and 6
     or v_week_ending_date is distinct from v_source_row.work_date+
        ((v_contract.week_ending_weekday_snapshot-
          extract(dow from v_source_row.work_date)::integer+7)%7) then
    raise exception 'WEEKLY_SOURCE_CONTRACT_CHANGED_DURING_LINEAGE' using errcode='55000';
  end if;

  if v_contract_week.contract_id is distinct from v_resolution.contract_id
     or v_contract_week.week_ending_date is distinct from v_week_ending_date
     or v_contract_week.additional_seq<>0
     or v_contract_week.is_adjustment
     or v_contract_week.status='CANCELLED'::public.contract_week_status_enum then
    raise exception 'WEEKLY_SOURCE_BASE_CONTRACT_WEEK_INVALID' using errcode='55000';
  end if;

  -- Under the locks, the Contract Week must still name the root whose family
  -- this transaction locked.  If it now names a different root, or gained one
  -- after the plain read found none, the family locks would be the wrong ones:
  -- refuse retryably rather than take a second family lock set out of order.
  if v_contract_week.timesheet_id is distinct from v_known_root_timesheet_id then
    raise exception 'WEEKLY_SOURCE_BASE_WEEK_ROOT_CHANGED_DURING_LOCK'
      using errcode='40001',
        detail=pg_catalog.jsonb_build_object(
          'expected_root_timesheet_id',v_known_root_timesheet_id,
          'observed_root_timesheet_id',v_contract_week.timesheet_id
        )::text;
  end if;

  if v_contract_week.timesheet_id is not null then
    -- The family rows are already locked by the I-1 lock set above, so this is
    -- a re-entrant read of a row this transaction holds, not a new lock order.
    select * into v_timesheet
    from public.timesheets
    where timesheet_id=v_contract_week.timesheet_id
    for update;
    -- proof/34 rule 6 and section 5 step 3: a stored contract-week Timesheet id
    -- that is no longer the current version is a ROTATION, not an invalid
    -- Timesheet.  It is classified below, after the rotation lock set has been
    -- taken, so it carries the exact
    -- WEEKLY_SOURCE_TIMESHEET_ROTATED_BEFORE_AUTHORISATION refusal (ROT-001).
    if not found
       or v_timesheet.contract_id is distinct from v_resolution.contract_id
       or v_timesheet.week_ending_date is distinct from v_week_ending_date
       or v_timesheet.sheet_scope<>'WEEKLY'::public.timesheet_scope_enum
       or v_timesheet.line_type<>'HOURS'::public.timesheet_line_type_enum
       or v_timesheet.is_adjustment
       or v_timesheet.revoked_at is not null
       or v_timesheet.archived_at_utc is not null then
      raise exception 'WEEKLY_SOURCE_EXISTING_TIMESHEET_INVALID' using errcode='55000';
    end if;
  else
    if v_contract_week.status in (
      'AUTHORISED'::public.contract_week_status_enum,
      'INVOICED'::public.contract_week_status_enum,
      'CANCELLED'::public.contract_week_status_enum
    ) then
      raise exception 'WEEKLY_SOURCE_FINAL_WEEK_TIMESHEET_MISSING' using errcode='55000';
    end if;

    select * into strict v_candidate
    from public.candidates where id=v_resolution.candidate_id;
    select * into strict v_client
    from public.clients where id=v_resolution.client_id;

    v_occupant_norm:=pg_catalog.lower(coalesce(
      nullif(pg_catalog.btrim(v_candidate.tms_ref),''),
      nullif(pg_catalog.btrim(v_candidate.display_name),''),
      v_candidate.id::text
    ));
    v_hospital_norm:=pg_catalog.lower(coalesce(
      nullif(pg_catalog.btrim(v_contract.display_site),''),
      nullif(pg_catalog.btrim(v_client.name),''),
      v_client.id::text
    ));
    v_ward_norm:=pg_catalog.lower(coalesce(
      nullif(pg_catalog.btrim(v_contract.ward_hint),''),'contract'
    ));
    v_role_norm:=pg_catalog.lower(coalesce(
      nullif(pg_catalog.btrim(v_contract.role),''),'weekly'
    ));
    v_booking_material:='weekly-source|'||v_resolution.contract_id::text||'|'||
      v_week_ending_date::text;
    v_booking_id:='bk_'||pg_catalog.substr(pg_catalog.encode(
      extensions.digest(pg_catalog.convert_to(v_booking_material,'UTF8'),'sha256'),'hex'
    ),1,24);

    if exists(
      select 1
      from public.timesheets existing
      where existing.booking_id=v_booking_id
        and existing.is_current
    ) then
      raise exception 'WEEKLY_SOURCE_BOOKING_ID_COLLISION' using errcode='55000';
    end if;

    insert into public.timesheets(
      booking_id,version,is_current,status,sheet_scope,submission_mode,line_type,
      occupant_key_norm,hospital_norm,ward_norm,job_title_norm,shift_label_norm,
      week_ending_date,contract_id,actual_schedule_json,qr_payload_json,
      is_adjustment,created_at,updated_at
    ) values (
      v_booking_id,1,true,'RECEIVED'::public.timesheet_status_enum,
      'WEEKLY'::public.timesheet_scope_enum,'MANUAL'::public.submission_mode_enum,
      'HOURS'::public.timesheet_line_type_enum,v_occupant_norm,v_hospital_norm,
      v_ward_norm,v_role_norm,'weekly-0',v_week_ending_date,
      v_resolution.contract_id,'[]'::jsonb,'{}'::jsonb,false,
      pg_catalog.statement_timestamp(),pg_catalog.statement_timestamp()
    ) returning * into v_timesheet;
    v_created_timesheet:=true;

    if v_finalisation_lock_operation.id is not null then
      -- Only this genuine INSERT RETURNING may add a new private root; never
      -- a lookup/fallback or a root that appeared externally. No pre-existing
      -- booking key is invented: retain the Source base-week creation serial
      -- policy. Subsequent WORK/expense rows can re-enter this exact own root.
      insert into private.bpay_next_source_current_lock_members(
        operation_id,root_timesheet_id,raw_booking_id,canonical_booking_id
      ) values(v_finalisation_lock_operation.id,v_timesheet.timesheet_id,
        v_timesheet.booking_id,pg_catalog.btrim(v_timesheet.booking_id));
    end if;

    update public.contract_weeks
    set timesheet_id=v_timesheet.timesheet_id,
        status=case when status='AUTHORISED'::public.contract_week_status_enum
          then status else 'SUBMITTED'::public.contract_week_status_enum end,
        updated_at=pg_catalog.statement_timestamp()
    where id=v_contract_week.id
    returning * into v_contract_week;

    insert into public.audit_events(
      ts_utc,actor_user_id,actor_display,actor_role_at_time,
      object_type,object_id_text,action,before_json,after_json,reason
    )
    select
      pg_catalog.statement_timestamp(),p_actor_user_id,actor.display_name,actor.role,
      'timesheets',v_timesheet.timesheet_id::text,
      'WEEKLY_SOURCE_BASE_TIMESHEET_CREATED',null,
      pg_catalog.jsonb_build_object(
        'source_cycle_id',v_cycle.id,
        'row_resolution_id',v_resolution.id,
        'work_event_id',v_resolution.work_event_id,
        'contract_id',v_resolution.contract_id,
        'contract_week_id',v_contract_week.id,
        'candidate_id',v_resolution.candidate_id,
        'client_id',v_resolution.client_id,
        'week_ending_date',v_week_ending_date
      ),'FINAL_SOURCE_LINEAGE'
    from public.tms_users actor
    where actor.id=p_actor_user_id;

    if not found then
      raise exception 'WEEKLY_SOURCE_ACTOR_NOT_FOUND' using errcode='55000';
    end if;
  end if;

  -- proof/34 section 5 step 2: the deadlock-free rotation lock set and a
  -- re-resolution of the canonical current version through the installed
  -- Workbench authority.  A stored or newly minted physical id is only a lookup
  -- key; it is never trusted on its own (rule 6).
  --
  -- For an existing root the lock set was already taken, before every other
  -- lock, so this call is re-entrant and re-resolves under the locks.  For a
  -- root this transaction has just minted, no other session can know the
  -- booking id yet, so taking the family keys here cannot join a cycle.
  v_lock_result:=private.weekly_source_lock_family_rows_v1(
    array[v_timesheet.timesheet_id]::uuid[],v_resolution.candidate_id
  );
  if coalesce((v_lock_result->>'ok')::boolean,false) is not true then
    raise exception '%',v_lock_result->>'code'
      using errcode='55000',detail=v_lock_result::text;
  end if;
  v_family:=v_lock_result->'families'->0;

  -- Review F5 / proof/34 rule 7: an approved decision is never silently
  -- transferred from one physical Timesheet id to another.  If the family
  -- already carries a live root authorisation on a member that is NOT the
  -- canonical row, the authorised root has rotated: refuse rather than bind a
  -- new source row to the new physical id and report success.
  v_root_authorisation:=private.weekly_source_root_authorisation_state_v1(
    (v_family->>'canonical_timesheet_id')::uuid,
    (select pg_catalog.array_agg(member.value::uuid)
     from pg_catalog.jsonb_array_elements_text(
       v_family->'member_timesheet_ids'
     ) member(value))
  );
  -- Review G8: the guard treats a missing authorisation relation as "cannot
  -- tell" and fails closed; this owner must point the same way, not bind.
  if coalesce((v_root_authorisation->>'relation_present')::boolean,false) is not true then
    raise exception 'WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE'
      using errcode='55000',
        detail=pg_catalog.jsonb_build_object(
          'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE',
          'reason',coalesce(v_root_authorisation->>'reason',
                            'ROOT_AUTHORISATION_RELATION_MISSING')
        )::text;
  end if;
  if coalesce((v_root_authorisation->>'live_on_other_member_count')::integer,0)>0 then
    raise exception 'WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE'
      using errcode='55000',
        detail=pg_catalog.jsonb_build_object(
          'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE',
          'reason','AUTHORISED_ROOT_NOT_CANONICAL',
          'canonical_timesheet_id',v_family->>'canonical_timesheet_id',
          'authorised_timesheet_id',
            v_root_authorisation->>'live_on_other_member_timesheet_id'
        )::text;
  end if;

  -- proof/34 section 5 step 3: a changed current id before first authorisation
  -- makes the request stale.  Write nothing and never authorise the older id.
  if coalesce((v_family->>'requested_is_canonical')::boolean,false) is not true
     or coalesce((v_family->>'family_is_current')::boolean,false) is not true then
    raise exception 'WEEKLY_SOURCE_TIMESHEET_ROTATED_BEFORE_AUTHORISATION'
      using errcode='55000',detail=v_family::text;
  end if;

  -- The family rows are locked now, so re-read the root under the lock before
  -- any fact derived from it is frozen into the lineage record.
  select * into strict v_timesheet
  from public.timesheets
  where timesheet_id=v_timesheet.timesheet_id;
  if v_timesheet.booking_id is distinct from (v_family->>'family_booking_id')
     or v_timesheet.version is distinct from (v_family->>'canonical_version')::integer
     or not v_timesheet.is_current then
    raise exception 'WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE'
      using errcode='55000',detail=v_family::text;
  end if;

  -- Decision D8: this owner writes a BINDING record, never an authorisation.
  -- The row signature at authorisation, the authorisation generation and the
  -- entitlement head pointer all belong to
  -- public.weekly_source_root_authorisations, written only by the
  -- first-authorisation owner (interface I-6) after the ordinary Authorise
  -- succeeds, and marked withdrawn only by the withdrawal owner.  This owner
  -- therefore computes and stores no signature at all.

  v_contract_week_settings:=
    private.weekly_source_contract_week_settings_authority_assert_v1(v_contract_week.id);
  v_timesheet_settings:=
    private._timesheet_settings_authority_frozen_v1(v_timesheet.timesheet_id);
  if v_timesheet.settings_authority_resolved_at is null
     or (v_timesheet_settings->>'resolved_at_utc')::timestamptz is distinct from
        v_timesheet.settings_authority_resolved_at
     or v_timesheet.settings_authority_fingerprint is distinct from
        v_contract_week.settings_authority_fingerprint
     or v_timesheet_settings->>'authority_fingerprint' is distinct from
        v_contract_week_settings->>'authority_fingerprint' then
    raise exception 'WEEKLY_SOURCE_SETTINGS_AUTHORITY_NOT_FROZEN' using errcode='55000';
  end if;

  v_lineage_fingerprint:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_ROW_TIMESHEET_LINEAGE_V1',
    pg_catalog.jsonb_build_object(
      'row_resolution_id',v_resolution.id,
      'source_cycle_id',v_cycle.id,
      'work_event_id',v_resolution.work_event_id,
      'candidate_id',v_resolution.candidate_id,
      'client_id',v_resolution.client_id,
      'contract_id',v_resolution.contract_id,
      'contract_week_id',v_contract_week.id,
      'timesheet_id',v_timesheet.timesheet_id,
      'week_ending_date',v_week_ending_date
    )
  );

  -- Decision D8: one binding row per resolution.  A replay is a no-op; this
  -- owner never writes an authorisation generation, and re-authorisation after
  -- a withdrawal appends a row to public.weekly_source_root_authorisations,
  -- written only by the first-authorisation owner (interface I-6).
  select exists(
    select 1 from public.weekly_source_row_timesheet_lineages lineage
    where lineage.row_resolution_id=v_resolution.id
  ) into v_existing_lineage;

  -- Decision D8: the binding record.  It carries the binding-time family
  -- identity and physical version, which stale detection needs, and no
  -- authorisation fact of any kind.
  insert into public.weekly_source_row_timesheet_lineages(
    row_resolution_id,source_cycle_id,work_event_id,candidate_id,client_id,
    contract_id,contract_week_id,timesheet_id,family_booking_id,timesheet_version,
    week_ending_date,lineage_fingerprint
  ) values (
    v_resolution.id,v_cycle.id,v_resolution.work_event_id,v_resolution.candidate_id,
    v_resolution.client_id,v_resolution.contract_id,v_contract_week.id,
    v_timesheet.timesheet_id,v_timesheet.booking_id,v_timesheet.version,
    v_week_ending_date,v_lineage_fingerprint
  ) on conflict (row_resolution_id) do nothing;

  -- Review G3 / proof/34 section 5 step 3 and section 6.  A binding that already
  -- exists on a DIFFERENT physical id means the base Timesheet rotated since it
  -- was bound.  Before first authorisation that is permitted and rotation WINS
  -- (rule 2): the request is STALE, so refuse with
  -- WEEKLY_SOURCE_TIMESHEET_ROTATED_BEFORE_AUTHORISATION and say that the
  -- resolution must be rebuilt at a new generation.  Only when the family
  -- carries a live authorisation record is it the section 6 integrity failure,
  -- which the assert below raises.  Without this the immutable binding would
  -- make that source row impossible to finalise ever again.
  select lineage.timesheet_id into v_bound_timesheet_id
  from public.weekly_source_row_timesheet_lineages lineage
  where lineage.row_resolution_id=v_resolution.id;
  if v_bound_timesheet_id is distinct from v_timesheet.timesheet_id then
    if coalesce((v_root_authorisation->>'live_on_canonical')::boolean,false) is not true
       and coalesce((v_root_authorisation->>'live_on_other_member_count')::integer,0)=0 then
      raise exception 'WEEKLY_SOURCE_TIMESHEET_ROTATED_BEFORE_AUTHORISATION'
        using errcode='55000',
          detail=pg_catalog.jsonb_build_object(
            'code','WEEKLY_SOURCE_TIMESHEET_ROTATED_BEFORE_AUTHORISATION',
            'reason','BOUND_ROOT_ROTATED_BEFORE_AUTHORISATION',
            'bound_timesheet_id',v_bound_timesheet_id,
            'canonical_timesheet_id',v_timesheet.timesheet_id,
            'rebuild','REBUILD_RESOLUTION_AT_A_NEW_GENERATION'
          )::text;
    end if;
    raise exception 'WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE'
      using errcode='55000',
        detail=pg_catalog.jsonb_build_object(
          'code','WEEKLY_SOURCE_ROOT_INTEGRITY_FAILURE',
          'reason','AUTHORISED_ROOT_ROTATED_AFTER_AUTHORISATION',
          'bound_timesheet_id',v_bound_timesheet_id,
          'canonical_timesheet_id',v_timesheet.timesheet_id
        )::text;
  end if;

  v_lineage:=private.weekly_source_timesheet_lineage_assert_v1(v_resolution.id);
  if v_lineage.lineage_fingerprint is distinct from v_lineage_fingerprint then
    raise exception 'WEEKLY_SOURCE_TIMESHEET_LINEAGE_CONFLICT' using errcode='55000';
  end if;

  if not v_existing_lineage then
    insert into public.audit_events(
      ts_utc,actor_user_id,actor_display,actor_role_at_time,
      object_type,object_id_text,action,before_json,after_json,reason
    )
    select
      pg_catalog.statement_timestamp(),p_actor_user_id,actor.display_name,actor.role,
      'weekly_source_row_timesheet_lineages',v_lineage.id::text,
      'WEEKLY_SOURCE_TIMESHEET_LINEAGE_CREATED',null,
      pg_catalog.jsonb_build_object(
        'source_cycle_id',v_lineage.source_cycle_id,
        'row_resolution_id',v_lineage.row_resolution_id,
        'work_event_id',v_lineage.work_event_id,
        'contract_id',v_lineage.contract_id,
        'contract_week_id',v_lineage.contract_week_id,
        'timesheet_id',v_lineage.timesheet_id,
        'candidate_id',v_lineage.candidate_id,
        'client_id',v_lineage.client_id,
        'week_ending_date',v_lineage.week_ending_date,
        'created_timesheet',v_created_timesheet
      ),'FINAL_SOURCE_LINEAGE'
    from public.tms_users actor
    where actor.id=p_actor_user_id;

    if not found then
      raise exception 'WEEKLY_SOURCE_ACTOR_NOT_FOUND' using errcode='55000';
    end if;
  end if;

  return pg_catalog.jsonb_build_object(
    'row_resolution_id',v_resolution.id,
    'source_cycle_id',v_cycle.id,
    'contract_week_id',v_lineage.contract_week_id,
    'timesheet_id',v_lineage.timesheet_id,
    'week_ending_date',v_lineage.week_ending_date,
    'created_timesheet',v_created_timesheet,
    'idempotent_replay',v_existing_lineage
  );
exception
  when no_data_found or too_many_rows then
    raise exception 'WEEKLY_SOURCE_TIMESHEET_LINEAGE_SCOPE_INVALID' using errcode='55000';
end;
$function$;

alter function private.weekly_source_finalisation_insert_current_movement_v1(uuid,uuid,uuid,uuid,text,text,text,uuid,uuid,uuid,bigint,bigint,text,bytea,uuid,jsonb,uuid,uuid) owner to postgres;
revoke all on function private.weekly_source_finalisation_insert_current_movement_v1(uuid,uuid,uuid,uuid,text,text,text,uuid,uuid,uuid,bigint,bigint,text,bytea,uuid,jsonb,uuid,uuid) from public,anon,authenticated,service_role;
alter function private.weekly_source_finalisation_insert_reversal_v1(uuid,uuid,uuid,uuid,uuid,text,jsonb) owner to postgres;
revoke all on function private.weekly_source_finalisation_insert_reversal_v1(uuid,uuid,uuid,uuid,uuid,text,jsonb) from public,anon,authenticated,service_role;
alter function private.weekly_source_finalisation_materialise_expense_invoice_v1(uuid,uuid,text,uuid,uuid,uuid,uuid,text) owner to postgres;
revoke all on function private.weekly_source_finalisation_materialise_expense_invoice_v1(uuid,uuid,text,uuid,uuid,uuid,uuid,text) from public,anon,authenticated,service_role;
alter function private.weekly_source_ordinary_projection_expense_movement_assert_v1(uuid,uuid,uuid,uuid,uuid) owner to postgres;
revoke all on function private.weekly_source_ordinary_projection_expense_movement_assert_v1(uuid,uuid,uuid,uuid,uuid) from public,anon,authenticated,service_role;
alter function private.weekly_source_finalise_engine_v1(jsonb,uuid,boolean) owner to postgres;
revoke all on function private.weekly_source_finalise_engine_v1(jsonb,uuid,boolean) from public,anon,authenticated,service_role;
alter function public.weekly_source_timesheet_lineage_ensure_atomic_v1(uuid,uuid) owner to postgres;
revoke all on function public.weekly_source_timesheet_lineage_ensure_atomic_v1(uuid,uuid) from public,anon,authenticated,service_role;
grant execute on function public.weekly_source_timesheet_lineage_ensure_atomic_v1(uuid,uuid) to service_role;
notify pgrst,'reload schema';
commit;
