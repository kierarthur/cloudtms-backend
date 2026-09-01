-- Repeatable CloudTMS function authority: candidate_authorised_hours_expense_anchor_v1
-- Keeps authorised hours immutable while permitting a separately allocated Candidate expense carrier.

\set ON_ERROR_STOP on

begin;

create or replace function private._candidate_record_capabilities_v1(
  p_timesheet_id uuid default null,
  p_contract_week_id uuid default null,
  p_proposed_claim jsonb default '{}'::jsonb
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare
  v_week public.contract_weeks%rowtype;
  v_contract public.contracts%rowtype;
  v_timesheet public.timesheets%rowtype;
  v_fin public.timesheets_financials%rowtype;
  v_policy jsonb;
  v_hours numeric:=0;
  v_additional numeric:=0;
  v_expenses numeric:=0;
  v_mileage numeric:=0;
  v_travel numeric:=0;
  v_accommodation numeric:=0;
  v_other numeric:=0;
  v_import boolean:=false;
  v_protected boolean:=false;
  v_candidate_mutation_locked boolean:=false;
  v_separate boolean:=false;
  v_has_timesheet boolean:=false;
  v_has_claim_evidence boolean:=false;
  v_has_embedded_submission_evidence boolean:=false;
  v_has_worked_schedule boolean:=false;
  v_has_active_submission_workflow boolean:=false;
  v_role text;
  v_route jsonb;
  v_route_family text;
  v_hours_route_allowed boolean:=false;
  v_expense_route_allowed boolean:=false;
  v_paper_route_allowed boolean:=false;
  v_no_work_route_allowed boolean:=false;
  v_reasons jsonb:='[]'::jsonb;
  v_result jsonb;
begin
  if p_timesheet_id is null and p_contract_week_id is null then
    raise exception 'CANDIDATE_RECORD_IDENTITY_REQUIRED' using errcode='22023';
  end if;

  if p_timesheet_id is not null then
    select * into v_timesheet from public.timesheets where timesheet_id=p_timesheet_id;
    if not found then raise exception 'CANDIDATE_TIMESHEET_NOT_FOUND' using errcode='P0002'; end if;
  end if;

  if p_contract_week_id is not null then
    select * into v_week from public.contract_weeks where id=p_contract_week_id;
  else
    select cw.* into v_week from public.contract_weeks cw
    where cw.timesheet_id=p_timesheet_id order by cw.updated_at desc,cw.id desc limit 1;
  end if;
  if v_week.id is null then
    -- DAILY is timesheet-owned and intentionally has no contract_weeks row.
    if v_timesheet.timesheet_id is null
       or v_timesheet.sheet_scope<>'DAILY'::public.timesheet_scope_enum
       or v_timesheet.contract_id is null then
      raise exception 'CANDIDATE_CONTRACT_WEEK_NOT_FOUND' using errcode='P0002';
    end if;
    select * into v_contract from public.contracts where id=v_timesheet.contract_id;
  else
    select * into v_contract from public.contracts where id=v_week.contract_id;
  end if;
  if not found then raise exception 'CANDIDATE_CONTRACT_NOT_FOUND' using errcode='P0002'; end if;

  if v_timesheet.timesheet_id is null and v_week.timesheet_id is not null then
    select * into v_timesheet from public.timesheets where timesheet_id=v_week.timesheet_id;
  end if;

  if v_timesheet.timesheet_id is not null then
    select * into v_fin from public.timesheets_financials
    where timesheet_id=v_timesheet.timesheet_id and is_current=true
    order by computed_at_utc desc nulls last,updated_at desc,id desc limit 1;
  end if;

  v_policy:=private._candidate_policy_resolve_v1(
    v_contract.client_id,v_contract.id,coalesce(v_week.week_ending_date,v_timesheet.week_ending_date)
  );
  v_route:=private._candidate_route_family_v1(v_timesheet.timesheet_id,v_week.id);
  v_route_family:=v_route->>'route_family';
  v_hours_route_allowed:=coalesce((v_route->>'candidate_hours_submission_allowed')::boolean,false);
  v_expense_route_allowed:=coalesce((v_route->>'candidate_expenses_allowed')::boolean,false);
  v_paper_route_allowed:=coalesce((v_route->>'candidate_paper_submission_allowed')::boolean,false);
  v_no_work_route_allowed:=coalesce((v_route->>'candidate_no_work_allowed')::boolean,false);
  v_separate:=coalesce((v_policy->>'expenses_require_separate_timesheet')::boolean,false);
  v_hours:=coalesce(v_fin.total_hours,0);
  v_additional:=private._candidate_json_numeric_sum(coalesce(v_fin.additional_units_json,'{}'::jsonb));
  if v_additional=0 then
    v_additional:=private._candidate_json_numeric_sum(coalesce(v_timesheet.additional_units_week,'{}'::jsonb))
      +private._candidate_json_numeric_sum(coalesce(v_timesheet.additional_units_per_day,'{}'::jsonb));
  end if;
  v_mileage:=abs(coalesce(v_fin.mileage_units,0))+abs(coalesce(v_fin.mileage_pay_ex_vat,0))+abs(coalesce(v_fin.mileage_charge_ex_vat,0));
  v_travel:=abs(coalesce(v_fin.travel_pay_ex_vat,0))+abs(coalesce(v_fin.travel_charge_ex_vat,0));
  v_accommodation:=abs(coalesce(v_fin.accommodation_pay_ex_vat,0))+abs(coalesce(v_fin.accommodation_charge_ex_vat,0));
  v_other:=abs(coalesce(v_fin.expenses_pay_ex_vat,0))+abs(coalesce(v_fin.expenses_charge_ex_vat,0))
    +abs(coalesce(v_fin.other_pay_ex_vat,0))+abs(coalesce(v_fin.other_charge_ex_vat,0));
  v_expenses:=v_mileage+v_travel+v_accommodation+v_other;

  if jsonb_typeof(p_proposed_claim)='object' then
    v_expenses:=greatest(v_expenses,
      abs(coalesce(nullif(p_proposed_claim->>'expenses_pay_ex_vat','')::numeric,0))
      +abs(coalesce(nullif(p_proposed_claim->>'expenses_charge_ex_vat','')::numeric,0))
      +abs(coalesce(nullif(p_proposed_claim->>'mileage_pay_ex_vat','')::numeric,0))
      +abs(coalesce(nullif(p_proposed_claim->>'mileage_charge_ex_vat','')::numeric,0))
    );
    v_mileage:=greatest(v_mileage,
      abs(coalesce(nullif(p_proposed_claim->>'mileage_units','')::numeric,0))
      +abs(coalesce(nullif(p_proposed_claim->>'mileage_pay_ex_vat','')::numeric,0))
      +abs(coalesce(nullif(p_proposed_claim->>'mileage_charge_ex_vat','')::numeric,0)));
    v_travel:=greatest(v_travel,
      abs(coalesce(nullif(p_proposed_claim->>'travel_pay_ex_vat','')::numeric,0))
      +abs(coalesce(nullif(p_proposed_claim->>'travel_charge_ex_vat','')::numeric,0)));
    v_accommodation:=greatest(v_accommodation,
      abs(coalesce(nullif(p_proposed_claim->>'accommodation_pay_ex_vat','')::numeric,0))
      +abs(coalesce(nullif(p_proposed_claim->>'accommodation_charge_ex_vat','')::numeric,0)));
    v_other:=greatest(v_other,
      abs(coalesce(nullif(p_proposed_claim->>'expenses_pay_ex_vat','')::numeric,0))
      +abs(coalesce(nullif(p_proposed_claim->>'expenses_charge_ex_vat','')::numeric,0))
      +abs(coalesce(nullif(p_proposed_claim->>'other_pay_ex_vat','')::numeric,0))
      +abs(coalesce(nullif(p_proposed_claim->>'other_charge_ex_vat','')::numeric,0)));
    v_expenses:=greatest(v_expenses,v_mileage+v_travel+v_accommodation+v_other);
  end if;

  v_import:=coalesce((v_route->>'import_authoritative')::boolean,false);
  v_protected:=v_timesheet.archived_at_utc is not null
    or (v_timesheet.timesheet_id is not null and (not v_timesheet.is_current))
    or v_fin.paid_at_utc is not null
    or v_fin.locked_by_invoice_id is not null
    or coalesce(v_week.status in (
      'INVOICED'::public.contract_week_status_enum,'CANCELLED'::public.contract_week_status_enum
    ),false);
  v_candidate_mutation_locked:=v_fin.authorised_at_utc is not null;
  if v_candidate_mutation_locked then
    v_reasons:=v_reasons||'"CANDIDATE_MUTATION_LOCKED_AUTHORISED"'::jsonb;
  end if;

  select exists(
    select 1 from public.timesheet_evidence e
    where e.timesheet_id=v_timesheet.timesheet_id
      and upper(btrim(e.kind))='TIMESHEET'
      and e.processing_state<>'SUPERSEDED'
  ) into v_has_timesheet;

  select exists(
    select 1 from public.timesheet_evidence e
    where e.timesheet_id=v_timesheet.timesheet_id
      and e.processing_state<>'SUPERSEDED'
  ) into v_has_claim_evidence;

  v_has_embedded_submission_evidence:=
    nullif(btrim(coalesce(v_timesheet.r2_nurse_key,'')),'') is not null
    or nullif(btrim(coalesce(v_timesheet.r2_auth_key,'')),'') is not null
    or nullif(btrim(coalesce(v_timesheet.qr_r2_key,'')),'') is not null
    or v_timesheet.qr_signed_hash is not null
    or v_timesheet.qr_signed_at_utc is not null
    or v_timesheet.authorised_at_server is not null;
  v_has_worked_schedule:=
    coalesce(v_timesheet.worked_minutes,0)<>0
    or v_timesheet.worked_start_iso is not null
    or v_timesheet.worked_end_iso is not null
    or coalesce(v_timesheet.actual_schedule_json,'{}'::jsonb) not in ('{}'::jsonb,'[]'::jsonb,'null'::jsonb);

  select exists(
    select 1
    from public.candidate_submission_workflows workflow
    where workflow.candidate_id=v_contract.candidate_id
      and workflow.contract_id=v_contract.id
      and workflow.week_ending_date=coalesce(v_week.week_ending_date,v_timesheet.week_ending_date)
      and workflow.state not in ('CANCELLED','EXPIRED','SUPERSEDED')
      and (
        workflow.contract_week_id is not distinct from v_week.id
        or workflow.target_timesheet_id=v_timesheet.timesheet_id
        or workflow.anchor_timesheet_id=v_timesheet.timesheet_id
      )
  ) into v_has_active_submission_workflow;

  if v_protected then v_role:='PROTECTED'; v_reasons:=v_reasons||'"LIFECYCLE_PROTECTED"'::jsonb;
  elsif v_import and v_expenses<>0 then v_role:='CONFLICT'; v_reasons:=v_reasons||'"IMPORT_SOURCE_HAS_EXPENSES"'::jsonb;
  elsif v_import then v_role:='IMPORT_HOURS'; v_reasons:=v_reasons||'"IMPORT_AUTHORITATIVE_HOURS"'::jsonb;
  elsif v_separate and (v_hours<>0 or v_additional<>0) and v_expenses<>0 then v_role:='CONFLICT'; v_reasons:=v_reasons||'"SEPARATION_MIXED_ECONOMICS"'::jsonb;
  elsif v_expenses<>0 and v_hours=0 and v_additional=0 then v_role:='EXPENSE_ONLY';
  elsif (v_hours<>0 or v_additional<>0) and v_expenses=0 then v_role:='HOURS_ONLY';
  elsif not v_separate and v_timesheet.timesheet_id is not null then v_role:='COMBINED_ALLOWED';
  elsif v_week.additional_seq>0 and v_timesheet.timesheet_id is null then v_role:='FLEXIBLE';
  elsif v_week.additional_seq>0 and v_hours=0 and v_additional=0 and v_expenses=0 then v_role:='FLEXIBLE';
  else v_role:='HOURS_ONLY';
  end if;

  v_result:=jsonb_build_object(
    'record_role',v_role,
    'reason_codes',v_reasons,
    'timesheet_id',v_timesheet.timesheet_id,
    'contract_week_id',v_week.id,
    'contract_id',v_contract.id,
    'candidate_id',v_contract.candidate_id,
    'client_id',v_contract.client_id,
    'week_ending_date',v_week.week_ending_date,
    'additional_seq',v_week.additional_seq,
    'hours_value',v_hours,
    'additional_units_value',v_additional,
    'expense_value',v_expenses,
    'effective_separation',v_separate,
    'import_authoritative',v_import,
    'route_family',v_route_family,
    'effective_submission_mode',v_route->'effective_submission_mode',
    'protected',v_protected,
    'candidate_mutation_locked',v_candidate_mutation_locked,
    'has_active_timesheet_evidence',v_has_timesheet,
    'has_active_claim_evidence',v_has_claim_evidence,
    'has_embedded_submission_evidence',v_has_embedded_submission_evidence,
    'has_worked_schedule',v_has_worked_schedule,
    'has_active_submission_workflow',v_has_active_submission_workflow,
    'candidate_hours_submission_allowed',v_hours_route_allowed and not v_protected and not v_candidate_mutation_locked,
    'candidate_expenses_allowed',v_expense_route_allowed and not v_protected,
    'candidate_paper_submission_allowed',v_paper_route_allowed and not v_protected and not v_candidate_mutation_locked,
    'candidate_no_work_allowed',v_no_work_route_allowed and not v_protected and not v_candidate_mutation_locked
      and coalesce(v_week.additional_seq,0)=0 and not coalesce(v_week.is_adjustment,false)
      and v_hours=0 and v_additional=0 and v_expenses=0
      and not v_has_claim_evidence and not v_has_embedded_submission_evidence
      and not v_has_worked_schedule and not v_has_active_submission_workflow,
    'can_edit_hours',v_hours_route_allowed and v_role in ('HOURS_ONLY','COMBINED_ALLOWED','FLEXIBLE') and not v_protected and not v_candidate_mutation_locked and not v_import,
    -- Imported hours remain immutable, but the Candidate may start the
    -- mandatory separate expense route against that worked-week anchor.
    -- Authorised hours remain immutable, but can still anchor the separately
    -- allocated Candidate expense carrier. The placement resolver forbids
    -- SAME_RECORD when candidate_mutation_locked is true.
    'can_edit_expenses',v_expense_route_allowed and not v_protected and (
      v_role in ('EXPENSE_ONLY','COMBINED_ALLOWED','FLEXIBLE','IMPORT_HOURS')
      or (v_role='HOURS_ONLY' and (v_separate or v_candidate_mutation_locked))
    ),
    'can_attach_timesheet',v_hours_route_allowed and v_role in ('HOURS_ONLY','COMBINED_ALLOWED') and not v_protected and not v_candidate_mutation_locked and not v_has_timesheet,
    'can_attach_expense_evidence',v_expense_route_allowed and v_role in ('EXPENSE_ONLY','COMBINED_ALLOWED','FLEXIBLE') and not v_protected and not v_candidate_mutation_locked,
    'can_attach_mileage_evidence',v_expense_route_allowed and v_role in ('EXPENSE_ONLY','COMBINED_ALLOWED','FLEXIBLE') and not v_protected and not v_candidate_mutation_locked and v_mileage<>0,
    'can_attach_travel_evidence',v_expense_route_allowed and v_role in ('EXPENSE_ONLY','COMBINED_ALLOWED','FLEXIBLE') and not v_protected and not v_candidate_mutation_locked and v_travel<>0,
    'can_attach_accommodation_evidence',v_expense_route_allowed and v_role in ('EXPENSE_ONLY','COMBINED_ALLOWED','FLEXIBLE') and not v_protected and not v_candidate_mutation_locked and v_accommodation<>0,
    'can_attach_other_evidence',v_expense_route_allowed and v_role in ('EXPENSE_ONLY','COMBINED_ALLOWED','FLEXIBLE') and not v_protected and not v_candidate_mutation_locked and v_other<>0,
    'can_process',v_role not in ('PROTECTED','CONFLICT') and not v_protected and not v_candidate_mutation_locked,
    'can_reject_candidate_submission',v_timesheet.timesheet_id is not null and not v_protected and v_fin.authorised_at_utc is null,
    'reject_scope',case when v_role='EXPENSE_ONLY' then 'COMPLETE_EXPENSE_CLAIM' else 'COMPLETE_TIMESHEET_RECORD' end,
    'requires_carrier',v_role='IMPORT_HOURS'
      or (v_role='HOURS_ONLY' and (v_separate or v_candidate_mutation_locked)),
    'expense_invoice_email_ready',coalesce((v_policy->>'expense_invoice_email_ready')::boolean,false),
    'policy',v_policy
  );

  return v_result||jsonb_build_object(
    'capability_hash',encode(extensions.digest(convert_to(v_result::text,'UTF8'),'sha256'),'hex')
  );
exception
  when invalid_text_representation then
    raise exception 'CANDIDATE_PROPOSED_CLAIM_INVALID' using errcode='22023';
end;
$function$;

alter function private._candidate_record_capabilities_v1(uuid,uuid,jsonb) owner to postgres;
revoke all on function private._candidate_record_capabilities_v1(uuid,uuid,jsonb) from public,anon,authenticated,service_role;
grant execute on function private._candidate_record_capabilities_v1(uuid,uuid,jsonb) to postgres;

comment on function private._candidate_record_capabilities_v1(uuid,uuid,jsonb) is
  'Single server capability authority for role, editors, ATTACH choices, preview border, processing, rejection and expense placement; authorised hours may anchor only a separately allocated expense carrier.';

notify pgrst, 'reload schema';

commit;
