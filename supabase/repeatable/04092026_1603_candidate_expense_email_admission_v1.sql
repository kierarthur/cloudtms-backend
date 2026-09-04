-- Candidate expense email admission authority.
-- A separated/import-authoritative expense claim cannot start until the
-- effective dedicated Expense Invoice Email is configured. The finalisation
-- guard remains authoritative; this repeatable moves the same failure to
-- Candidate capability and placement admission so no worker can complete a
-- claim that is guaranteed to fail after manager approval.

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
  v_expense_admission_ready boolean:=false;
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
  v_daily_candidate_id uuid;
  v_daily_environment text;
  v_daily_identity_count integer;
begin
  if p_timesheet_id is null and p_contract_week_id is null then
    raise exception 'CANDIDATE_RECORD_IDENTITY_REQUIRED' using errcode='22023';
  end if;

  if p_timesheet_id is not null then
    select * into v_timesheet from public.timesheets where timesheet_id=p_timesheet_id;
    if not found then raise exception 'CANDIDATE_TIMESHEET_NOT_FOUND' using errcode='P0002'; end if;
    if v_timesheet.sheet_scope='DAILY' and v_timesheet.contract_id is null
       and p_contract_week_id is null then
      -- Candidate-created Daily receipts are owned by the immutable booking
      -- workflow, not by a fictitious weekly Contract. Preserve all other paths.
      select count(*)::integer,min(identity_row.candidate_id::text)::uuid,
        min(identity_row.environment)
      into v_daily_identity_count,v_daily_candidate_id,v_daily_environment
      from (
        select distinct w.candidate_id,w.environment
        from public.candidate_submission_workflows w
        join public.timesheets origin on origin.timesheet_id=w.anchor_timesheet_id
        where w.workflow_kind='DAILY' and origin.booking_id=v_timesheet.booking_id
          and origin.idempotency_key like 'candidate-daily-first:%'
          and w.creation_identity_json#>>'{request,daily_source,booking_id}'=v_timesheet.booking_id
      ) identity_row;
      if v_daily_identity_count=1 then
        return private._candidate_daily_read_projection_v1(
          v_daily_environment,v_daily_candidate_id,p_timesheet_id,now())->'capabilities';
      end if;
    end if;
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
  v_expense_admission_ready:=not v_separate
    or coalesce((v_policy->>'expense_invoice_email_ready')::boolean,false);
  if not v_expense_admission_ready then
    v_reasons:=v_reasons||'"EXPENSE_INVOICE_EMAIL_REQUIRED"'::jsonb;
  end if;
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
  v_candidate_mutation_locked:=v_fin.authorised_at_utc is not null
    or coalesce(v_fin.processing_status in (
      'PENDING_AUTH'::public.ts_fin_processing_status_enum,
      'READY_FOR_HR'::public.ts_fin_processing_status_enum,
      'READY_FOR_INVOICE'::public.ts_fin_processing_status_enum
    ),false);
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
  -- An additional expense carrier remains expense-only even when its Client
  -- inherits an import-authoritative route. Only an imported hours record that
  -- has also acquired expenses is a mixed-source conflict.
  elsif v_timesheet.line_type in ('EXPENSES','MILEAGE')
     and v_expenses<>0 and v_hours=0 and v_additional=0 then v_role:='EXPENSE_ONLY';
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
    'candidate_expenses_allowed',v_expense_route_allowed and v_expense_admission_ready and (
      not v_protected or v_hours<>0 or v_additional<>0 or v_import
    ),
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
    'can_edit_expenses',v_expense_route_allowed and v_expense_admission_ready and (
      (
        not v_protected and (
          v_role in ('EXPENSE_ONLY','COMBINED_ALLOWED','FLEXIBLE','IMPORT_HOURS')
          or (v_role='HOURS_ONLY' and (v_separate or v_candidate_mutation_locked))
        )
      )
      or (
        v_protected and (v_hours<>0 or v_additional<>0 or v_import)
      )
    ),
    'can_attach_timesheet',v_hours_route_allowed and v_role in ('HOURS_ONLY','COMBINED_ALLOWED') and not v_protected and not v_candidate_mutation_locked and not v_has_timesheet,
    'can_attach_expense_evidence',v_expense_route_allowed and v_expense_admission_ready and v_role in ('EXPENSE_ONLY','COMBINED_ALLOWED','FLEXIBLE') and not v_protected and not v_candidate_mutation_locked,
    'can_attach_mileage_evidence',v_expense_route_allowed and v_expense_admission_ready and v_role in ('EXPENSE_ONLY','COMBINED_ALLOWED','FLEXIBLE') and not v_protected and not v_candidate_mutation_locked and v_mileage<>0,
    'can_attach_travel_evidence',v_expense_route_allowed and v_expense_admission_ready and v_role in ('EXPENSE_ONLY','COMBINED_ALLOWED','FLEXIBLE') and not v_protected and not v_candidate_mutation_locked and v_travel<>0,
    'can_attach_accommodation_evidence',v_expense_route_allowed and v_expense_admission_ready and v_role in ('EXPENSE_ONLY','COMBINED_ALLOWED','FLEXIBLE') and not v_protected and not v_candidate_mutation_locked and v_accommodation<>0,
    'can_attach_other_evidence',v_expense_route_allowed and v_expense_admission_ready and v_role in ('EXPENSE_ONLY','COMBINED_ALLOWED','FLEXIBLE') and not v_protected and not v_candidate_mutation_locked and v_other<>0,
    'can_process',v_role not in ('PROTECTED','CONFLICT') and not v_protected and not v_candidate_mutation_locked,
    'can_reject_candidate_submission',v_timesheet.timesheet_id is not null and not v_protected and v_fin.authorised_at_utc is null,
    'reject_scope',case when v_role='EXPENSE_ONLY' then 'COMPLETE_EXPENSE_CLAIM' else 'COMPLETE_TIMESHEET_RECORD' end,
    'requires_carrier',v_role='IMPORT_HOURS'
      or (v_role='HOURS_ONLY' and (v_separate or v_candidate_mutation_locked))
      or (v_protected and (v_hours<>0 or v_additional<>0 or v_import)),
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

create or replace function public.expense_placement_resolve_v1(
  p_candidate_id uuid,
  p_environment text,
  p_anchor_timesheet_id uuid,
  p_contract_week_id uuid default null,
  p_proposed_claim jsonb default '{}'::jsonb,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_environment text;
  v_anchor_week public.contract_weeks%rowtype;
  v_contract public.contracts%rowtype;
  v_anchor_capabilities jsonb;
  v_positive_work boolean:=false;
  v_candidate_count integer:=0;
  v_candidate_week_id uuid;
  v_candidate_timesheet_id uuid;
  v_candidate_role text;
  v_result text;
  v_reason text;
begin
  v_environment:=private._candidate_assert_environment(p_environment);
  perform private._candidate_require_feature_v1(v_environment,'candidate_expense_atomic_placement');
  if p_candidate_id is null or (p_anchor_timesheet_id is null and p_contract_week_id is null) then
    raise exception 'EXPENSE_PLACEMENT_IDENTITY_REQUIRED' using errcode='22023';
  end if;
  if p_contract_week_id is not null then
    select * into v_anchor_week from public.contract_weeks where id=p_contract_week_id;
  else
    select * into v_anchor_week from public.contract_weeks
    where timesheet_id=p_anchor_timesheet_id order by updated_at desc,id desc limit 1;
  end if;
  if not found then raise exception 'EXPENSE_PLACEMENT_ANCHOR_NOT_FOUND' using errcode='P0002'; end if;
  select * into v_contract from public.contracts where id=v_anchor_week.contract_id and candidate_id=p_candidate_id;
  if not found then raise exception 'EXPENSE_PLACEMENT_CANDIDATE_MISMATCH' using errcode='28000'; end if;
  v_anchor_capabilities:=private._candidate_record_capabilities_v1(
    coalesce(p_anchor_timesheet_id,v_anchor_week.timesheet_id),v_anchor_week.id,coalesce(p_proposed_claim,'{}'::jsonb)
  );
  if coalesce(v_anchor_capabilities->'reason_codes','[]'::jsonb) ? 'EXPENSE_INVOICE_EMAIL_REQUIRED' then
    return jsonb_build_object(
      'ok',true,'placement','BLOCKED','reason_code','EXPENSE_INVOICE_EMAIL_REQUIRED',
      'anchor_timesheet_id',p_anchor_timesheet_id,'anchor_contract_week_id',v_anchor_week.id,
      'capabilities',v_anchor_capabilities
    );
  end if;
  if not coalesce((v_anchor_capabilities->>'candidate_expenses_allowed')::boolean,false) then
    return jsonb_build_object(
      'ok',true,'placement','BLOCKED','reason_code','CANDIDATE_RECORD_VIEW_ONLY',
      'anchor_timesheet_id',p_anchor_timesheet_id,'anchor_contract_week_id',v_anchor_week.id,
      'capabilities',v_anchor_capabilities
    );
  end if;

  select exists(
    select 1
    from public.contract_weeks cw
    join public.timesheets t on t.timesheet_id=cw.timesheet_id and t.is_current=true and t.archived_at_utc is null
    join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current=true
    where cw.contract_id=v_contract.id and cw.week_ending_date=v_anchor_week.week_ending_date
      and (
        coalesce(tf.total_hours,0)>0
        or private._candidate_json_numeric_sum(coalesce(tf.additional_units_json,'{}'::jsonb))>0
        or private._candidate_json_numeric_sum(coalesce(t.additional_units_week,'{}'::jsonb))
          +private._candidate_json_numeric_sum(coalesce(t.additional_units_per_day,'{}'::jsonb))>0
      )
  ) into v_positive_work;
  if not v_positive_work then
    return jsonb_build_object(
      'ok',true,'placement','BLOCKED','reason_code','NO_POSITIVE_WORKED_TIME',
      'anchor_timesheet_id',p_anchor_timesheet_id,'anchor_contract_week_id',v_anchor_week.id,
      'capabilities',v_anchor_capabilities
    );
  end if;

  if coalesce((v_anchor_capabilities->>'effective_separation')::boolean,false)=false
     and v_anchor_capabilities->>'record_role' in ('COMBINED_ALLOWED','EXPENSE_ONLY','FLEXIBLE')
     and coalesce((v_anchor_capabilities->>'protected')::boolean,false)=false
     and coalesce((v_anchor_capabilities->>'candidate_mutation_locked')::boolean,false)=false
     and coalesce((v_anchor_capabilities->>'can_edit_expenses')::boolean,false) then
    return jsonb_build_object(
      'ok',true,'placement','SAME_RECORD','reason_code','COMBINED_ALLOWED',
      'anchor_timesheet_id',coalesce(p_anchor_timesheet_id,v_anchor_week.timesheet_id),
      'target_timesheet_id',coalesce(p_anchor_timesheet_id,v_anchor_week.timesheet_id),
      'target_contract_week_id',v_anchor_week.id,'capabilities',v_anchor_capabilities
    );
  end if;

  with candidate_carriers as (
    select cw.id as contract_week_id,cw.timesheet_id,
      private._candidate_record_capabilities_v1(cw.timesheet_id,cw.id,coalesce(p_proposed_claim,'{}'::jsonb)) as capabilities
    from public.contract_weeks cw
    left join public.timesheets t on t.timesheet_id=cw.timesheet_id
    where cw.contract_id=v_contract.id and cw.week_ending_date=v_anchor_week.week_ending_date
      and cw.additional_seq>0
      and (t.timesheet_id is null or (t.is_current=true and t.archived_at_utc is null))
      and not exists(
        select 1 from public.candidate_submission_workflows w
        where w.contract_week_id=cw.id and w.candidate_id<>p_candidate_id
          and w.state in ('CREATED','WORKER_SUBMITTED','AWAITING_MANAGER_APPROVAL','MANAGER_APPROVED','AWAITING_PAPER_RETURN','RECEIVED')
      )
  ), safe as (
    select * from candidate_carriers
    where capabilities->>'record_role' in ('EXPENSE_ONLY','FLEXIBLE')
      and coalesce((capabilities->>'protected')::boolean,false)=false
      and coalesce((capabilities->>'candidate_mutation_locked')::boolean,false)=false
      and coalesce((capabilities->>'import_authoritative')::boolean,false)=false
  )
  select count(*)::integer into v_candidate_count from safe;

  if v_candidate_count=1 then
    with candidate_carriers as (
      select cw.id as contract_week_id,cw.timesheet_id,
        private._candidate_record_capabilities_v1(cw.timesheet_id,cw.id,coalesce(p_proposed_claim,'{}'::jsonb)) as capabilities
      from public.contract_weeks cw
      left join public.timesheets t on t.timesheet_id=cw.timesheet_id
      where cw.contract_id=v_contract.id and cw.week_ending_date=v_anchor_week.week_ending_date
        and cw.additional_seq>0
        and (t.timesheet_id is null or (t.is_current=true and t.archived_at_utc is null))
        and not exists(
          select 1 from public.candidate_submission_workflows w
          where w.contract_week_id=cw.id and w.candidate_id<>p_candidate_id
            and w.state in ('CREATED','WORKER_SUBMITTED','AWAITING_MANAGER_APPROVAL','MANAGER_APPROVED','AWAITING_PAPER_RETURN','RECEIVED')
        )
    )
    select contract_week_id,timesheet_id,capabilities->>'record_role'
    into v_candidate_week_id,v_candidate_timesheet_id,v_candidate_role
    from candidate_carriers
    where capabilities->>'record_role' in ('EXPENSE_ONLY','FLEXIBLE')
      and coalesce((capabilities->>'protected')::boolean,false)=false
      and coalesce((capabilities->>'candidate_mutation_locked')::boolean,false)=false
      and coalesce((capabilities->>'import_authoritative')::boolean,false)=false
    order by contract_week_id
    limit 1;
  end if;

  if v_candidate_count>1 then v_result:='BLOCKED';v_reason:='EXPENSE_CARRIER_AMBIGUOUS';
  elsif v_candidate_count=1 then v_result:='REUSE_CARRIER';v_reason:='SAFE_EXISTING_CARRIER';
  else v_result:='CREATE_CARRIER';v_reason:='NO_SAFE_CARRIER'; end if;

  return jsonb_build_object(
    'ok',true,'placement',v_result,'reason_code',v_reason,
    'anchor_timesheet_id',coalesce(p_anchor_timesheet_id,v_anchor_week.timesheet_id),
    'anchor_contract_week_id',v_anchor_week.id,
    'target_timesheet_id',v_candidate_timesheet_id,
    'target_contract_week_id',v_candidate_week_id,
    'target_record_role',v_candidate_role,
    'capabilities',v_anchor_capabilities
  );
end;
$function$;

revoke all on function public.expense_placement_resolve_v1(uuid,text,uuid,uuid,jsonb,timestamptz) from public,anon,authenticated;
grant execute on function public.expense_placement_resolve_v1(uuid,text,uuid,uuid,jsonb,timestamptz) to service_role;

commit;
