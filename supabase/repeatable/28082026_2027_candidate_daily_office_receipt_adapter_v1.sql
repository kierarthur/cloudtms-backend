-- Candidate Daily Office adapter; weekly and financial owners are unchanged.
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


create or replace function public.candidate_submission_reject_atomic_v1(
  p_actor_user_id uuid,
  p_environment text,
  p_timesheet_id uuid,
  p_expected_timesheet_id uuid,
  p_expected_row_signature text,
  p_reason text,
  p_idempotency_key text,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_environment text;
  v_timesheet public.timesheets%rowtype;
  v_fin public.timesheets_financials%rowtype;
  v_week public.contract_weeks%rowtype;
  v_signature jsonb;
  v_capabilities jsonb;
  v_qr_result record;
  v_new_timesheet_id uuid;
  v_qr_backed boolean:=false;
  v_reject_scope text;
  v_workflow record;
  v_rejected_workflow_ids uuid[]:='{}'::uuid[];
  v_paper_workflow_ids uuid[]:='{}'::uuid[];
  v_paper_workflow_generations integer[]:='{}'::integer[];
  v_paper_retirement_result jsonb;
  v_rejection_family_contract_id uuid;
  v_rejection_family_week_ending_date date;
  v_rejection_family_key text;
  v_rejection_request_hash text;
  v_rejection_receipt_before jsonb;
  v_response jsonb;
begin
  v_environment:=private._candidate_assert_environment(p_environment);
  if not private._candidate_office_service_context_valid_v1(
    v_environment,p_actor_user_id,'REJECT_CONFIRM'
  ) then
    perform private._candidate_require_feature_v1(v_environment,'candidate_app_writes');
  end if;
  if p_actor_user_id is null or p_timesheet_id is null or p_expected_timesheet_id is null
     or nullif(btrim(coalesce(p_expected_row_signature,'')),'') is null
     or nullif(btrim(coalesce(p_reason,'')),'') is null
     or nullif(btrim(coalesce(p_idempotency_key,'')),'') is null then
    raise exception 'CANDIDATE_REJECT_PAYLOAD_INVALID' using errcode='22023';
  end if;
  perform pg_advisory_xact_lock(hashtextextended(
    'CANDIDATE_REJECTION_IDEMPOTENCY:'||p_actor_user_id::text||':'||p_idempotency_key,0
  ));
  -- Serialise every Candidate rejection for the same contract/week before
  -- locking either the hours row or a separate expense carrier. This prevents
  -- the inverse target/workflow/source lock order that can deadlock H1/E1.
  select target.contract_id,target.week_ending_date
  into v_rejection_family_contract_id,v_rejection_family_week_ending_date
  from public.timesheets target
  where target.timesheet_id=p_timesheet_id;
  if not found then
    raise exception 'CANDIDATE_REJECT_TARGET_NOT_FOUND' using errcode='P0002';
  end if;
  v_rejection_family_key:='CANDIDATE_PAPER_FAMILY:'||v_environment||':'
    ||coalesce(v_rejection_family_contract_id::text,'-')||':'
    ||coalesce(v_rejection_family_week_ending_date::text,'-');
  perform pg_advisory_xact_lock(hashtextextended(v_rejection_family_key,0));

  v_rejection_request_hash:=encode(extensions.digest(convert_to(jsonb_build_object(
    'contract_version','CANDIDATE_REJECTION_REQUEST_V2',
    'environment',v_environment,
    'actor_user_id',p_actor_user_id,
    'timesheet_id',p_timesheet_id,
    'expected_timesheet_id',p_expected_timesheet_id,
    'expected_row_signature',btrim(p_expected_row_signature),
    'reason',btrim(p_reason)
  )::text,'UTF8'),'sha256'),'hex');
  select ae.before_json,ae.after_json into v_rejection_receipt_before,v_response
  from public.audit_events ae
  where ae.object_type='candidate_submission_rejection_receipt'
    and ae.actor_user_id=p_actor_user_id
    and ae.correlation_id=p_idempotency_key
  order by ae.ts_utc desc,ae.id desc
  limit 1;
  if found then
    if v_rejection_receipt_before->>'request_sha256' is distinct from v_rejection_request_hash then
      raise exception 'CANDIDATE_IDEMPOTENCY_CONFLICT'
        using errcode='40001',detail=jsonb_build_object(
          'code','CANDIDATE_IDEMPOTENCY_CONFLICT','idempotency_key',p_idempotency_key
        )::text;
    end if;
    return coalesce(v_response,'{}'::jsonb)||jsonb_build_object('idempotent_replay',true);
  end if;

  select * into v_timesheet from public.timesheets where timesheet_id=p_timesheet_id for update;
  if not found then raise exception 'CANDIDATE_REJECT_TARGET_NOT_FOUND' using errcode='P0002'; end if;
  if not v_timesheet.is_current or v_timesheet.timesheet_id<>p_expected_timesheet_id then
    raise exception 'TIMESHEET_MOVED' using errcode='40001';
  end if;
  select * into v_week from public.contract_weeks where timesheet_id=v_timesheet.timesheet_id for update;
  if not found and v_timesheet.sheet_scope<>'DAILY'::public.timesheet_scope_enum then
    raise exception 'CANDIDATE_CONTRACT_WEEK_NOT_FOUND' using errcode='P0002';
  end if;
  select * into v_fin from public.timesheets_financials
  where timesheet_id=v_timesheet.timesheet_id and is_current=true for update;
  if v_fin.authorised_at_utc is not null or v_timesheet.authorised_at_server is not null then
    raise exception 'CANDIDATE_REJECT_REQUIRES_UNAUTHORISE' using errcode='55000';
  end if;
  if v_fin.paid_at_utc is not null or v_fin.locked_by_invoice_id is not null then
    raise exception 'CANDIDATE_REJECT_PROTECTED_HISTORY' using errcode='55000';
  end if;
  v_signature:=public.timesheet_lifecycle_guard_signature_v1(v_timesheet.timesheet_id,v_week.id,false);
  if coalesce(v_signature->>'row_signature',v_signature->>'backend_row_signature','')<>p_expected_row_signature then
    raise exception 'ROW_SIGNATURE_MISMATCH' using errcode='40001';
  end if;
  v_capabilities:=private._candidate_record_capabilities_v1(v_timesheet.timesheet_id,v_week.id,'{}'::jsonb);
  if coalesce((v_capabilities->>'can_reject_candidate_submission')::boolean,false)=false then
    raise exception 'CANDIDATE_REJECT_NOT_ALLOWED' using errcode='55000',detail=v_capabilities::text;
  end if;
  v_reject_scope:=v_capabilities->>'reject_scope';
  v_qr_backed:=v_timesheet.qr_status is not null or v_timesheet.qr_token is not null or v_timesheet.qr_r2_key is not null
    or exists(select 1 from public.candidate_submission_workflows w where w.target_timesheet_id=v_timesheet.timesheet_id and w.route='PAPER');

  for v_workflow in
    select w.id,w.generation,w.route,w.state
    from public.candidate_submission_workflows w
    where w.environment=v_environment
      and (
        (
          w.state not in ('FINALISED','REFUSED','REJECTED','CANCELLED','EXPIRED','SUPERSEDED')
          and (
            w.target_timesheet_id=v_timesheet.timesheet_id
            or w.anchor_timesheet_id=v_timesheet.timesheet_id
          )
        )
        or (
          w.state='FINALISED'
          and w.target_timesheet_id=v_timesheet.timesheet_id
        )
      )
    order by w.id
    for update
  loop
    v_rejected_workflow_ids:=array_append(v_rejected_workflow_ids,v_workflow.id);
    if v_workflow.route='PAPER'
       and v_workflow.state in ('AWAITING_PAPER_RETURN','RECEIVED','FINALISED') then
      v_paper_workflow_ids:=array_append(v_paper_workflow_ids,v_workflow.id);
      v_paper_workflow_generations:=array_append(
        v_paper_workflow_generations,v_workflow.generation
      );
    end if;
  end loop;

  if cardinality(v_paper_workflow_ids)>0 then
    v_paper_retirement_result:=private._candidate_paper_delivery_retire_set_v1(
      v_paper_workflow_ids,v_paper_workflow_generations,
      'OFFICE_REJECTED',p_now_utc
    );
    if not coalesce((v_paper_retirement_result->>'retired')::boolean,false)
       or not coalesce(
         (v_paper_retirement_result->>'qr_invalidation_proven')::boolean,false
       ) then
      raise exception 'CANDIDATE_PAPER_QR_INVALIDATION_NOT_PROVEN'
        using errcode='40001',detail=jsonb_build_object(
          'code','CANDIDATE_PAPER_QR_INVALIDATION_NOT_PROVEN',
          'workflow_ids',to_jsonb(v_paper_workflow_ids),
          'retirement_receipt',v_paper_retirement_result
        )::text;
    end if;
  end if;

  if v_timesheet.sheet_scope='DAILY'::public.timesheet_scope_enum then
    if v_fin.id is null and v_timesheet.contract_id is null then
      v_new_timesheet_id:=nullif(
        private._candidate_daily_receipt_reset_v1(
          v_environment,(v_capabilities->>'candidate_id')::uuid,
          v_timesheet.timesheet_id,v_timesheet.timesheet_id,btrim(p_reason),
          p_actor_user_id,'OFFICE_REJECTED',p_now_utc
        )->>'current_timesheet_id',''
      )::uuid;
    else
      -- Existing financial/history reset owner is deliberately unchanged.
      v_new_timesheet_id:=nullif(
        private._candidate_daily_submission_reset_v1(
          v_timesheet.timesheet_id,v_timesheet.timesheet_id,btrim(p_reason),
          p_actor_user_id,'OFFICE_REJECTED',p_now_utc
        )->>'current_timesheet_id',''
      )::uuid;
    end if;
  elsif v_qr_backed then
    select * into v_qr_result from public.timesheet_qr_refuse_and_reset(
      v_timesheet.timesheet_id,v_timesheet.timesheet_id,btrim(p_reason),p_actor_user_id
    );
    v_new_timesheet_id:=v_qr_result.timesheet_id;
  else
    v_new_timesheet_id:=private._candidate_timesheet_reject_rotate_v1(
      v_timesheet.timesheet_id,v_timesheet.timesheet_id,btrim(p_reason),p_actor_user_id,p_now_utc);
  end if;
  if v_new_timesheet_id is null then raise exception 'CANDIDATE_REJECT_ROTATION_FAILED' using errcode='55000'; end if;

  update public.timesheets set
    authorised_at_server=null,worked_start_iso=null,worked_end_iso=null,break_start_iso=null,break_end_iso=null,
    break_minutes=null,actual_schedule_json=null,additional_units_week='{}'::jsonb,additional_units_per_day='{}'::jsonb,
    manual_pdf_r2_key=null,reference_number=null,day_references_json=null,
    qr_token=null,qr_status=case when v_qr_backed then 'PENDING'::public.timesheet_qr_status_enum else null end,
    qr_payload_json='{}'::jsonb,qr_generated_at=null,qr_scanned_at=null,qr_scan_info_json=null,qr_r2_key=null,
    qr_last_sent_hash=null,qr_last_sent_at_utc=null,qr_signed_hash=null,qr_signed_at_utc=null,
    updated_at=p_now_utc
  where timesheet_id=v_new_timesheet_id;

  update public.timesheets_financials set
    processing_status=case when v_timesheet.sheet_scope='DAILY'::public.timesheet_scope_enum
      then 'UNASSIGNED'::public.ts_fin_processing_status_enum
      else 'UNPROCESSED'::public.ts_fin_processing_status_enum end,
    worked_start_iso=null,worked_end_iso=null,break_start_iso=null,break_end_iso=null,
    break_minutes=null,hours_day=0,hours_night=0,hours_sat=0,hours_sun=0,hours_bh=0,total_hours=0,
    additional_units_json='{}'::jsonb,additional_pay_ex_vat=0,additional_charge_ex_vat=0,additional_margin_ex_vat=0,
    expenses_pay_ex_vat=0,expenses_charge_ex_vat=0,expenses_description=null,expenses_evidence_r2_key=null,
    expenses_evidence_manifest=null,mileage_units=0,mileage_pay_ex_vat=0,mileage_charge_ex_vat=0,
    mileage_evidence_r2_key=null,mileage_evidence_manifest=null,travel_pay_ex_vat=0,travel_charge_ex_vat=0,
    accommodation_pay_ex_vat=0,accommodation_charge_ex_vat=0,other_pay_ex_vat=0,other_charge_ex_vat=0,
    actual_schedule_json=null,actual_minutes_by_day_json=null,total_pay_ex_vat=0,total_charge_ex_vat=0,margin_ex_vat=0,
    authorised_at_utc=null,authorised_by_user_id=null,updated_at=p_now_utc
  where timesheet_id=v_new_timesheet_id and is_current=true;

  if v_week.id is not null then
    update public.contract_weeks set status='OPEN',day_entries_json='[]'::jsonb,
      totals_json='{}'::jsonb,updated_at=p_now_utc where id=v_week.id;
  end if;
  update public.timesheet_evidence set processing_state='SUPERSEDED'
  where timesheet_id=v_timesheet.timesheet_id and processing_state<>'SUPERSEDED';
  for v_workflow in
    select w.id,w.account_id,w.candidate_id,w.generation,w.state as captured_state,
      case when w.state='FINALISED' then greatest(w.generation-1,1) else w.generation end as artifact_generation
    from public.candidate_submission_workflows w
    where w.id=any(v_rejected_workflow_ids)
    order by w.id
    for update
  loop
    update public.candidate_approval_requests set
      state='SUPERSEDED',superseded_at_utc=p_now_utc,updated_at_utc=p_now_utc
    where workflow_id=v_workflow.id
      and workflow_generation=v_workflow.artifact_generation
      and state in ('PENDING','APPROVED');
    update public.candidate_submission_components set
      state='REJECTED',superseded_at_utc=p_now_utc
    where workflow_id=v_workflow.id
      and workflow_generation=v_workflow.artifact_generation
      and state not in ('REJECTED','SUPERSEDED','ABANDONED');
    update public.candidate_submission_workflows set
      state='REJECTED',generation=v_workflow.generation+1,
      rejection_reason=btrim(p_reason),rejection_scope=v_reject_scope,updated_at_utc=p_now_utc
    where id=v_workflow.id and generation=v_workflow.generation
      and state=v_workflow.captured_state
      and state not in ('REFUSED','REJECTED','CANCELLED','EXPIRED','SUPERSEDED');
    if not found then
      raise exception 'CANDIDATE_REJECT_WORKFLOW_CONFLICT' using errcode='40001';
    end if;
    perform private._candidate_notification_insert_v1(v_workflow.account_id,v_workflow.candidate_id,v_workflow.id,v_new_timesheet_id,
      'OFFICE_REJECTED','office_rejection','candidate-office-rejected-v1',
      jsonb_build_object('reason',btrim(p_reason),'resubmission_scope',v_reject_scope),
      jsonb_build_object('type','timesheet','timesheet_id',v_new_timesheet_id),
      'CANDIDATE_OFFICE_REJECTED_V1:'||v_workflow.id::text||':'||(v_workflow.generation+1)::text,p_now_utc);
  end loop;
  v_response:=jsonb_build_object(
    'ok',true,'old_timesheet_id',v_timesheet.timesheet_id,'timesheet_id',v_new_timesheet_id,
    'scope',case when v_timesheet.sheet_scope='DAILY'::public.timesheet_scope_enum then 'DAILY' else 'WEEKLY' end,
    'contract_week_id',v_week.id,
    'contract_week_status',case when v_week.id is null then null else 'OPEN' end,
    'processing_status',case when v_week.id is null then 'UNASSIGNED' else 'UNPROCESSED' end,
    'rejection_scope',v_reject_scope,'qr_reissue_required',v_qr_backed,
    'paper_retirement_receipt',v_paper_retirement_result,
    'idempotency_key',p_idempotency_key
  );
  perform private._candidate_audit_v1('timesheet',v_new_timesheet_id::text,'CANDIDATE_SUBMISSION_REJECTED',
    jsonb_build_object('old_timesheet_id',v_timesheet.timesheet_id),v_response,btrim(p_reason),
    p_actor_user_id,p_idempotency_key,p_now_utc);
  insert into public.audit_events(
    actor_user_id,object_type,object_id_text,action,before_json,after_json,
    reason,correlation_id,ts_utc
  ) values (
    p_actor_user_id,'candidate_submission_rejection_receipt',p_timesheet_id::text,
    'CANDIDATE_SUBMISSION_REJECTION_RECEIPT',jsonb_build_object(
      'request_sha256',v_rejection_request_hash,'contract_version','CANDIDATE_REJECTION_REQUEST_V2'
    ),v_response,btrim(p_reason),p_idempotency_key,p_now_utc
  );
  return v_response;
end;
$function$;


alter function public.candidate_submission_reject_atomic_v1(uuid,text,uuid,uuid,text,text,text,timestamptz) owner to postgres;
revoke all on function public.candidate_submission_reject_atomic_v1(uuid,text,uuid,uuid,text,text,text,timestamptz) from public,anon,authenticated;
grant execute on function public.candidate_submission_reject_atomic_v1(uuid,text,uuid,uuid,text,text,text,timestamptz) to service_role;
notify pgrst, 'reload schema';
commit;
