-- Candidate expense separation and expense-invoice delivery authority.
-- Import-authoritative work always separates expenses. Otherwise an explicit
-- Contract override wins the Client setting. Protected hours may anchor a new,
-- separate expense carrier, but protected records themselves remain immutable.
-- Expense invoices use the configured Expense Invoice Email and do not inherit
-- self-bill suppression from their related hours invoice.

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
    'candidate_expenses_allowed',v_expense_route_allowed and (
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
    'can_edit_expenses',v_expense_route_allowed and (
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
    'can_attach_expense_evidence',v_expense_route_allowed and v_role in ('EXPENSE_ONLY','COMBINED_ALLOWED','FLEXIBLE') and not v_protected and not v_candidate_mutation_locked,
    'can_attach_mileage_evidence',v_expense_route_allowed and v_role in ('EXPENSE_ONLY','COMBINED_ALLOWED','FLEXIBLE') and not v_protected and not v_candidate_mutation_locked and v_mileage<>0,
    'can_attach_travel_evidence',v_expense_route_allowed and v_role in ('EXPENSE_ONLY','COMBINED_ALLOWED','FLEXIBLE') and not v_protected and not v_candidate_mutation_locked and v_travel<>0,
    'can_attach_accommodation_evidence',v_expense_route_allowed and v_role in ('EXPENSE_ONLY','COMBINED_ALLOWED','FLEXIBLE') and not v_protected and not v_candidate_mutation_locked and v_accommodation<>0,
    'can_attach_other_evidence',v_expense_route_allowed and v_role in ('EXPENSE_ONLY','COMBINED_ALLOWED','FLEXIBLE') and not v_protected and not v_candidate_mutation_locked and v_other<>0,
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

create or replace function private._invoice_delivery_routes_batch(
  p_requests jsonb,
  p_evaluation_date date
) returns table(
  request_key text,
  invoice_id uuid,
  canonical_to jsonb,
  canonical_cc jsonb,
  canonical_bcc jsonb,
  invalid_to_count integer,
  invalid_cc_count integer,
  invalid_bcc_count integer,
  recipient_set_hash text,
  route_policy_hash text,
  route_source text,
  client_settings_id uuid,
  contract_settings_ids jsonb,
  effective_date date,
  client_id uuid,
  invoice_group_identity text,
  self_bill boolean,
  do_not_send boolean,
  delivery_suppressed boolean,
  suppression_reason text,
  warning_codes jsonb,
  warning_details jsonb,
  blocker_codes jsonb,
  blocker_details jsonb,
  grouping_identity text
)
language sql
stable
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
with raw as materialized (
  select x.ordinality::integer request_no,x.value request_json,
    nullif(btrim(coalesce(x.value->>'request_key','')),'') request_key,
    case when coalesce(x.value->>'invoice_id','')~*
      '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      then(x.value->>'invoice_id')::uuid end invoice_id,
    case when jsonb_typeof(x.value->'recipient_set')='array'
      then x.value->'recipient_set' else '[]'::jsonb end requested_to,
    case when jsonb_typeof(x.value->'cc')='array'
      then x.value->'cc' else '[]'::jsonb end requested_cc,
    case when jsonb_typeof(x.value->'bcc')='array'
      then x.value->'bcc' else '[]'::jsonb end requested_bcc,
    upper(coalesce(nullif(btrim(x.value->>'delivery_policy'),''),
      'ATTACH')) delivery_policy,
    coalesce(nullif(btrim(x.value->>'template_version'),''),
      'invoice-delivery-v1') template_version
  from jsonb_array_elements(case when jsonb_typeof(p_requests)='array'
    then p_requests else '[]'::jsonb end)
    with ordinality x(value,ordinality)
  where jsonb_typeof(x.value)='object'
),
request_counts as materialized (
  select r.request_key,count(*)::integer request_key_count
  from raw r
  where r.request_key is not null
  group by r.request_key
),
facts as materialized (
  select r.*,coalesce(rc.request_key_count,0) request_key_count,
    i.client_id,
    coalesce(nullif(i.header_snapshot_json#>>'{meta,invoice_week_start}',''),
      nullif(i.header_snapshot_json->>'invoice_week_start',''),'NO_WEEK')
      invoice_week_identity,
    coalesce(i.do_not_send,false) invoice_do_not_send,
    lower(coalesce(i.header_snapshot_json#>>'{meta,self_bill}',
      i.header_snapshot_json->>'self_bill','false'))
      in('true','t','1','yes') invoice_self_bill,
    case when private._candidate_feature_enabled_current_v1('candidate_expense_invoice_routing_v1')
      then upper(coalesce(nullif(btrim(i.header_snapshot_json->>'invoice_stream'),''),
        case when lower(coalesce(i.header_snapshot_json#>>'{meta,self_bill}',
          i.header_snapshot_json->>'self_bill','false')) in('true','t','1','yes')
          then 'SELF_BILL' else 'NORMAL' end))
      else case when lower(coalesce(i.header_snapshot_json#>>'{meta,self_bill}',
          i.header_snapshot_json->>'self_bill','false')) in('true','t','1','yes')
          then 'SELF_BILL' else 'NORMAL' end
    end invoice_stream,
    nullif(btrim(coalesce(i.header_snapshot_json->>
      'client_primary_invoice_email',cl.primary_invoice_email,'')),'')
      primary_email,
    cs.id client_settings_id,cs.effective_from client_settings_effective_from,
    cs.send_manual_invoices_to_different_email client_alt_enabled,
    nullif(btrim(cs.manual_invoices_alt_email_address),'') client_alt_email,
    lower(nullif(btrim(cs.candidate_expense_invoice_email),''))
      client_expense_invoice_email,
    cs.self_bill_no_invoices_sent
  from raw r
  left join request_counts rc on rc.request_key=r.request_key
  left join public.invoices i on i.id=r.invoice_id
  left join public.clients cl on cl.id=i.client_id
  left join lateral (
    select s.id,s.effective_from,s.send_manual_invoices_to_different_email,
      s.manual_invoices_alt_email_address,s.candidate_expense_invoice_email,
      s.self_bill_no_invoices_sent
    from public.client_settings s
    where p_evaluation_date is not null
      and s.client_id=i.client_id
      and(s.effective_from is null or s.effective_from<=p_evaluation_date)
    order by s.effective_from desc nulls last,s.updated_at desc nulls last,
      s.created_at desc nulls last,s.id desc
    limit 1
  ) cs on true
),
line_routes as materialized (
  select f.request_no,f.request_key,f.invoice_id,l.timesheet_id,
    f.client_expense_invoice_email,
    l.timesheet_id is not null and ts.timesheet_id is null
      missing_current_timesheet,
    (
      (coalesce(ts.is_adjustment,false) or coalesce(cw.is_adjustment,false))
      and(
        upper(coalesce(ts.submission_mode::text,'')) in('MANUAL','QR')
        or nullif(btrim(coalesce(ts.qr_status::text,'')),'') is not null
        or nullif(btrim(coalesce(ts.qr_token::text,'')),'') is not null)
      and not(
        coalesce(ts.is_adjustment,false)
        and(
          left(upper(coalesce(ts.adjustment_origin::text,'')),7)='IMPORT_'
          or ts.correction_id is not null
          or nullif(btrim(coalesce(ts.correction_kind::text,'')),'') is not null))
    ) manual_adjustment,
    coalesce(ts.contract_id,cw.contract_id) contract_id
  from facts f
  join public.invoice_lines l on l.invoice_id=f.invoice_id
  left join public.timesheets ts
    on ts.timesheet_id=l.timesheet_id and ts.is_current
  left join lateral (
    select coalesce(bool_or(coalesce(w.is_adjustment,false)),false)
        is_adjustment,
      (array_agg(w.contract_id order by w.updated_at desc nulls last,
        w.created_at desc nulls last,w.id desc)
        filter(where w.contract_id is not null))[1] contract_id
    from public.contract_weeks w
    where w.timesheet_id=l.timesheet_id
  ) cw on true
  where l.timesheet_id is not null
),
contract_routes as materialized (
  select lr.*,ct.id is null contract_missing,
    coalesce(ct.overrideclientsettings,false)
      and coalesce(ct.send_manual_invoices_to_different_email,false)
      override_enabled,
    nullif(btrim(coalesce(ct.manual_invoices_alt_email_address,'')),'')
      alt_email,
    lower(nullif(btrim(coalesce(ct.manual_invoices_alt_email_address,'')),'') )
      contract_alt_policy_email,
    lower(coalesce(
      nullif(btrim(ct.candidate_expense_invoice_email_override),''),
      lr.client_expense_invoice_email)) effective_expense_invoice_email
  from line_routes lr
  left join public.contracts ct on ct.id=lr.contract_id
),
route_rollup as materialized (
  select f.request_no,f.request_key,f.invoice_id,
    coalesce(bool_or(cr.missing_current_timesheet),false)
      missing_current_timesheet,
    coalesce(bool_or(cr.manual_adjustment),false) has_manual_adjustment,
    coalesce(bool_or(cr.manual_adjustment and cr.contract_id is null),false)
      missing_contract,
    coalesce(bool_or(cr.manual_adjustment and cr.contract_missing),false)
      contract_data_missing,
    coalesce(bool_or(cr.manual_adjustment and cr.override_enabled),false)
      has_contract_override,
    coalesce(bool_or(cr.manual_adjustment and cr.override_enabled
      and cr.alt_email is null),false) contract_alt_missing,
    count(distinct lower(cr.alt_email))
      filter(where cr.manual_adjustment and cr.override_enabled
        and cr.alt_email is not null) contract_alt_count,
    min(lower(cr.alt_email))
      filter(where cr.manual_adjustment and cr.override_enabled
        and cr.alt_email is not null) contract_alt_email,
    count(distinct cr.effective_expense_invoice_email)
      filter(where cr.effective_expense_invoice_email is not null)
      expense_email_count,
    min(cr.effective_expense_invoice_email)
      filter(where cr.effective_expense_invoice_email is not null)
      expense_invoice_email,
    coalesce(jsonb_agg(distinct jsonb_build_object(
      'contract_id',cr.contract_id,
      'override_client_settings',cr.override_enabled,
      'manual_invoice_alternate_email',cr.contract_alt_policy_email,
      'contract_missing',cr.contract_missing)
      order by jsonb_build_object(
        'contract_id',cr.contract_id,
        'override_client_settings',cr.override_enabled,
        'manual_invoice_alternate_email',cr.contract_alt_policy_email,
        'contract_missing',cr.contract_missing))
      filter(where cr.manual_adjustment and cr.contract_id is not null),
      '[]'::jsonb) contract_setting_identities
  from facts f
  left join contract_routes cr
    on cr.request_no=f.request_no and cr.invoice_id=f.invoice_id
  group by f.request_no,f.request_key,f.invoice_id
),
chosen as materialized (
  select f.*,rr.missing_current_timesheet,rr.has_manual_adjustment,
    rr.missing_contract,rr.contract_data_missing,rr.has_contract_override,
    rr.contract_alt_missing,rr.contract_alt_count,rr.contract_alt_email,
    rr.expense_email_count,rr.expense_invoice_email,
    rr.contract_setting_identities,
    case
      when private._candidate_feature_enabled_current_v1('candidate_expense_invoice_routing_v1')
       and f.invoice_stream='EXPENSE' then 'EXPENSE_INVOICE_EMAIL'
      when jsonb_array_length(f.requested_to)>0 then 'REQUESTED'
      when rr.contract_alt_count=1 then 'CONTRACT_MANUAL_ALTERNATE'
      when rr.has_manual_adjustment and not rr.has_contract_override
        and coalesce(f.client_alt_enabled,false)
        then 'CLIENT_MANUAL_ALTERNATE'
      else 'CLIENT_PRIMARY'
    end route_source,
    case
      when private._candidate_feature_enabled_current_v1('candidate_expense_invoice_routing_v1')
       and f.invoice_stream='EXPENSE'
        then jsonb_build_array(rr.expense_invoice_email)
      when jsonb_array_length(f.requested_to)>0 then f.requested_to
      when rr.contract_alt_count=1 then jsonb_build_array(rr.contract_alt_email)
      when rr.has_manual_adjustment and not rr.has_contract_override
        and coalesce(f.client_alt_enabled,false)
        then jsonb_build_array(f.client_alt_email)
      else jsonb_build_array(f.primary_email)
    end selected_to,
    f.invoice_do_not_send
      or(f.invoice_stream<>'EXPENSE' and f.invoice_self_bill
        and coalesce(f.self_bill_no_invoices_sent,true))
      delivery_suppressed,
    case
      when f.invoice_do_not_send then 'DO_NOT_SEND'
      when f.invoice_stream<>'EXPENSE' and f.invoice_self_bill
        and coalesce(f.self_bill_no_invoices_sent,true)
        then 'SELF_BILL_SUPPRESSED'
    end suppression_reason
  from facts f
  left join route_rollup rr on rr.request_no=f.request_no
),
canonical as materialized (
  select c.*,
    coalesce((select jsonb_agg(e order by e) from(
      select distinct lower(btrim(v.value)) e
      from jsonb_array_elements_text(c.selected_to) v(value)
      where nullif(btrim(v.value),'') is not null
        and btrim(v.value)~*'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$'
    ) q),'[]'::jsonb) to_json,
    coalesce((select jsonb_agg(e order by e) from(
      select distinct lower(btrim(v.value)) e
      from jsonb_array_elements_text(c.requested_cc) v(value)
      where nullif(btrim(v.value),'') is not null
        and btrim(v.value)~*'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$'
    ) q),'[]'::jsonb) cc_json,
    coalesce((select jsonb_agg(e order by e) from(
      select distinct lower(btrim(v.value)) e
      from jsonb_array_elements_text(c.requested_bcc) v(value)
      where nullif(btrim(v.value),'') is not null
        and btrim(v.value)~*'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$'
    ) q),'[]'::jsonb) bcc_json,
    (select count(*)::integer
      from jsonb_array_elements_text(c.selected_to) v(value)
      where nullif(btrim(v.value),'') is not null
        and btrim(v.value)!~*'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$')
      invalid_to,
    (select count(*)::integer
      from jsonb_array_elements_text(c.requested_cc) v(value)
      where nullif(btrim(v.value),'') is not null
        and btrim(v.value)!~*'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$')
      invalid_cc,
    (select count(*)::integer
      from jsonb_array_elements_text(c.requested_bcc) v(value)
      where nullif(btrim(v.value),'') is not null
        and btrim(v.value)!~*'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$')
      invalid_bcc
  from chosen c
),
classified as materialized (
  select c.*,
    array_remove(array[
      case when c.missing_current_timesheet then
        'EMAIL_ROUTING_CHECK_FAILED' end,
      case when c.has_manual_adjustment and c.missing_contract then
        'CONTRACT_ROUTING_CHECK_FAILED' end,
      case when c.has_manual_adjustment and c.contract_data_missing then
        'CONTRACT_ROUTING_CHECK_FAILED' end,
      case when c.has_manual_adjustment and c.contract_alt_missing then
        'CONTRACT_MANUAL_EMAIL_MISSING' end,
      case when c.has_manual_adjustment and c.contract_alt_count>1 then
        'CONTRACT_MANUAL_EMAIL_CONFLICT' end,
      case when c.has_manual_adjustment and not c.has_contract_override
        and coalesce(c.client_alt_enabled,false) and c.client_alt_email is null
        then 'CLIENT_MANUAL_EMAIL_MISSING' end
    ],null)::text[] warnings,
    array_remove(array[
      case when c.request_key is null then 'REQUEST_KEY_REQUIRED' end,
      case when c.request_key_count>1 then 'REQUEST_KEY_DUPLICATE' end,
      case when c.invoice_id is null then 'INVOICE_ID_INVALID' end,
      case when c.client_id is null and c.invoice_id is not null
        then 'INVOICE_NOT_FOUND' end,
      case when p_evaluation_date is null then 'EVALUATION_DATE_REQUIRED' end,
      case when c.delivery_policy not in('ATTACH','SPLIT','SECURE_LINK')
        then 'DELIVERY_POLICY_INVALID' end,
      case when private._candidate_feature_enabled_current_v1('candidate_expense_invoice_routing_v1')
       and c.invoice_stream='EXPENSE'
        and (coalesce(c.expense_email_count,0)<>1
          or jsonb_array_length(c.to_json)=0)
        then 'EXPENSE_INVOICE_EMAIL_REQUIRED' end,
      case when c.invoice_stream<>'EXPENSE'
        and not c.delivery_suppressed and jsonb_array_length(c.to_json)=0
        then 'MISSING_RECIPIENT' end,
      case when c.invalid_to>0 then 'INVALID_TO_RECIPIENT' end,
      case when c.invalid_cc>0 then 'INVALID_CC_RECIPIENT' end,
      case when c.invalid_bcc>0 then 'INVALID_BCC_RECIPIENT' end
    ],null)::text[] blockers
  from canonical c
),
hashed as materialized (
  select c.*,
    encode(digest(jsonb_build_object(
      'to',case when c.delivery_suppressed then '[]'::jsonb else c.to_json end,
      'cc',case when c.delivery_suppressed then '[]'::jsonb else c.cc_json end,
      'bcc',case when c.delivery_suppressed then '[]'::jsonb else c.bcc_json end
    )::text,'sha256'),'hex') calculated_recipient_set_hash
  from classified c
),
policy_hashed as materialized (
  select h.*,
    encode(digest((jsonb_build_object(
      'policy_version',case
        when private._candidate_feature_enabled_current_v1('candidate_expense_invoice_routing_v1')
          then 'INVOICE_DELIVERY_ROUTE_V6'
        else 'INVOICE_DELIVERY_ROUTE_V5'
      end,
      'client_id',h.client_id,
      'invoice_week_identity',h.invoice_week_identity,
      'recipient_set_hash',h.calculated_recipient_set_hash,
      'to',case when h.delivery_suppressed then '[]'::jsonb else h.to_json end,
      'cc',case when h.delivery_suppressed then '[]'::jsonb else h.cc_json end,
      'bcc',case when h.delivery_suppressed then '[]'::jsonb else h.bcc_json end,
      'route_source',h.route_source,
      'client_settings_id',h.client_settings_id,
      'client_settings_effective_from',h.client_settings_effective_from,
      'contract_settings',h.contract_setting_identities,
      'self_bill',h.invoice_self_bill,
      'do_not_send',h.invoice_do_not_send,
      'delivery_suppressed',h.delivery_suppressed,
      'suppression_reason',h.suppression_reason,
      'warnings',to_jsonb(h.warnings),'blockers',to_jsonb(h.blockers),
      'template_version',h.template_version,
      'delivery_policy',h.delivery_policy
    ) || case
      when private._candidate_feature_enabled_current_v1('candidate_expense_invoice_routing_v1')
        then jsonb_build_object('invoice_stream',h.invoice_stream)
      else '{}'::jsonb
    end)::text,'sha256'),'hex') calculated_route_policy_hash
  from hashed h
)
select p.request_key,p.invoice_id,
  case when p.delivery_suppressed then '[]'::jsonb else p.to_json end,
  case when p.delivery_suppressed then '[]'::jsonb else p.cc_json end,
  case when p.delivery_suppressed then '[]'::jsonb else p.bcc_json end,
  p.invalid_to,p.invalid_cc,p.invalid_bcc,p.calculated_recipient_set_hash,
  p.calculated_route_policy_hash,p.route_source,p.client_settings_id,
  p.contract_setting_identities,p_evaluation_date,p.client_id,
  p.invoice_week_identity,p.invoice_self_bill,p.invoice_do_not_send,
  p.delivery_suppressed,p.suppression_reason,to_jsonb(p.warnings),
  jsonb_build_object(
    'missing_current_timesheet',p.missing_current_timesheet,
    'manual_adjustment',p.has_manual_adjustment,
    'contract_override_count',p.contract_alt_count)
    || case
      when private._candidate_feature_enabled_current_v1('candidate_expense_invoice_routing_v1')
        then jsonb_build_object(
          'invoice_stream',p.invoice_stream,
          'expense_email_count',p.expense_email_count)
      else '{}'::jsonb
    end,
  to_jsonb(p.blockers),
  jsonb_build_object(
    'invalid_to_count',p.invalid_to,'invalid_cc_count',p.invalid_cc,
    'invalid_bcc_count',p.invalid_bcc),
  p.calculated_route_policy_hash
from policy_hashed p
order by p.request_key nulls first,p.invoice_id nulls first,p.request_no;
$function$;

create or replace function private._invoice_issue_validate_batch(
  p_requests jsonb,
  p_evaluation_date date
) returns table(
  request_key text,
  invoice_id uuid,
  hard_blocker_codes jsonb,
  document_dependency_codes jsonb,
  delivery_blocker_codes jsonb,
  warning_codes jsonb,
  can_issue_only boolean,
  can_issue_and_deliver boolean,
  route_policy_result jsonb,
  detail_json jsonb
)
language sql
stable
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
with raw as materialized (
  select x.ordinality::integer request_no,
      nullif(btrim(coalesce(x.value->>'request_key','')),'') request_key,
      case when coalesce(x.value->>'invoice_id','')~
        '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
        then (x.value->>'invoice_id')::uuid end invoice_id,
      case when coalesce(x.value->>'operation_id','')~
        '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
        then (x.value->>'operation_id')::uuid end operation_id,
      case when jsonb_typeof(x.value->'expected_revision')='number'
        and coalesce(x.value->>'expected_revision','')~'^[0-9]+$'
        then (x.value->>'expected_revision')::bigint end expected_revision,
      case when jsonb_typeof(x.value->'allow_early')='boolean'
        then(x.value->>'allow_early')::boolean else false end allow_early,
      case when jsonb_typeof(x.value->'deliver')='boolean'
        then(x.value->>'deliver')::boolean else false end deliver,
      case when jsonb_typeof(x.value->'recipient_set')='array'
        then x.value->'recipient_set' else '[]'::jsonb end recipient_set,
      case when jsonb_typeof(x.value->'cc')='array'
        then x.value->'cc' else '[]'::jsonb end cc,
      case when jsonb_typeof(x.value->'bcc')='array'
        then x.value->'bcc' else '[]'::jsonb end bcc,
      jsonb_typeof(x.value->'expected_revision')='number'
        and coalesce(x.value->>'expected_revision','')~'^[0-9]+$'
        expected_revision_well_formed,
      (not (x.value ? 'allow_early')
        or jsonb_typeof(x.value->'allow_early')='boolean')
        allow_early_well_formed,
      (not (x.value ? 'deliver')
        or jsonb_typeof(x.value->'deliver')='boolean')
        deliver_well_formed
    from jsonb_array_elements(
      case when jsonb_typeof(p_requests)='array' then p_requests else '[]'::jsonb end)
      with ordinality x(value,ordinality)
    where jsonb_typeof(x.value)='object'
),
invoice_scope as materialized (
  select distinct r.invoice_id
  from raw r
  where r.invoice_id is not null
),
request_counts as materialized (
  select r.request_key,count(*)::integer request_key_count
  from raw r where r.request_key is not null group by r.request_key
),
invoice_ids as materialized (
  select coalesce(array_agg(r.invoice_id order by r.invoice_id),array[]::uuid[]) ids
  from invoice_scope r
),
references_batch as materialized (
  select r.*
  from invoice_ids i
  cross join lateral private._invoice_reference_rows_batch(i.ids) r
),
correction_scopes as materialized (
  select coalesce(jsonb_agg(jsonb_build_object(
    'request_key',r.request_key,
    'scope_key',r.request_key,
    'invoice_id',r.invoice_id,
    'validation_purpose','ISSUE',
    'expected_client_id',i.client_id,
    'expected_invoice_stream',case
      when private._candidate_feature_enabled_current_v1('candidate_expense_invoice_routing_v1')
        then upper(coalesce(
          nullif(i.header_snapshot_json#>>'{meta,invoice_stream}',''),
          nullif(i.header_snapshot_json->>'invoice_stream',''),
          case when lower(coalesce(i.header_snapshot_json#>>'{meta,self_bill}','false'))
            in('true','t','1','yes') then 'SELF_BILL' else 'NORMAL' end))
      when lower(coalesce(i.header_snapshot_json#>>'{meta,self_bill}','false'))
        in('true','t','1','yes') then 'SELF_BILL'
      else 'NORMAL'
    end,
    'planned_members',coalesce((select jsonb_agg(jsonb_build_object(
      'timesheet_id',l.timesheet_id,
      'vat_rate_pct',l.vat_rate_pct)
      order by l.timesheet_id,l.id)
      from public.invoice_lines l
      where l.invoice_id=r.invoice_id and l.timesheet_id is not null),
      '[]'::jsonb)) order by r.request_no),'[]'::jsonb) scopes
  from raw r
  left join public.invoices i on i.id=r.invoice_id
  where r.invoice_id is not null
),
corrections as materialized (
  select c.*
  from correction_scopes s
  cross join lateral private._invoice_correction_validate_batch(
    s.scopes,p_evaluation_date) c
),
routes as materialized (
  select d.*
  from private._invoice_delivery_routes_batch(
    (select coalesce(jsonb_agg(jsonb_build_object(
      'request_key',r.request_key,'invoice_id',r.invoice_id,
      'recipient_set',r.recipient_set,
      'cc',r.cc,'bcc',r.bcc)),'[]'::jsonb) from raw r),
    p_evaluation_date) d
),
line_totals as materialized (
  select r.invoice_id,count(l.id) line_count,
    round(coalesce(sum(l.total_charge_ex_vat),0),2) net,
    round(coalesce(sum(l.vat_amount),0),2) vat,
    round(coalesce(sum(l.total_inc_vat),0),2) gross,
    count(*) filter(where upper(coalesce(l.meta_json->>'line_type',''))
      like '%HIGHER_RATE%'
      and not exists(
        select 1 from public.invoice_hr_source_rows hs
        where hs.invoice_id=r.invoice_id
          and hs.source_system in('HEALTHROSTER','NHSP')))
      missing_higher_rate
  from invoice_scope r
  left join public.invoice_lines l on l.invoice_id=r.invoice_id
  group by r.invoice_id
),
source_requirements as materialized (
  select l.invoice_id,l.timesheet_id,
    bool_or(
      upper(coalesce(l.meta_json->>'line_type','')) like 'HOURS%'
      or upper(coalesce(l.meta_json->>'line_type',''))
        like 'ADDITIONAL_RATE%') reference_required,
    bool_or(
      upper(coalesce(l.meta_json->>'line_type','')) in(
        'EXPENSE_MILEAGE','MILEAGE')
      or coalesce(l.source_key,'') like '%:MILEAGE') mileage_required,
    bool_or(upper(coalesce(l.meta_json->>'line_type',''))
      like '%TRAVEL%') travel_required,
    bool_or(upper(coalesce(l.meta_json->>'line_type',''))
      like '%ACCOMMODATION%') accommodation_required,
    bool_or(
      upper(coalesce(l.meta_json->>'line_type','')) in(
        'EXPENSES_TOTAL','EXPENSE_TOTAL','OTHER_EXPENSE','EXPENSE_OTHER')
      or(
        upper(coalesce(l.meta_json->>'line_type','')) like 'EXPENSE_%'
        and upper(coalesce(l.meta_json->>'line_type','')) not in(
          'EXPENSE_MILEAGE','EXPENSE_TRAVEL','EXPENSE_ACCOMMODATION')))
      general_expense_required
  from public.invoice_lines l
  join invoice_scope s on s.invoice_id=l.invoice_id
  where l.timesheet_id is not null
  group by l.invoice_id,l.timesheet_id
),
timesheet_state as materialized (
  select s.invoice_id,s.timesheet_id,s.reference_required,
    s.mileage_required,s.travel_required,s.accommodation_required,
    s.general_expense_required,
    t.timesheet_id is not null source_exists,
    upper(coalesce(t.submission_mode::text,'')) submission_mode,
    coalesce(pc.effective_ts_attach_to_invoice,true)
      and not coalesce(summary.client_no_timesheet_required,false)
      and not coalesce(summary.client_is_nhsp,false) document_required,
    coalesce(pc.issue_missing_reference,false) issue_missing_reference,
    manual_source.evidence_id manual_source_evidence_id,
    coalesce(t.manual_document_asset_id,manual_source.document_asset_id)
      manual_document_asset_id,
    manual_asset.status manual_asset_status,
    manual_asset.operation_id manual_asset_operation_id,
    manual_asset_op.status manual_asset_operation_status,
    v.id timesheet_document_version_id,
    v.status timesheet_document_status,
    v.operation_id timesheet_document_operation_id,
    vop.status timesheet_document_operation_status,
    t.document_revision timesheet_document_revision,
    upper(coalesce(t.submission_mode::text,''))='QR'
      and(nullif(t.qr_signed_hash,'') is null
        or t.qr_signed_at_utc is null) qr_unsigned
  from source_requirements s
  left join public.timesheets t
    on t.timesheet_id=s.timesheet_id and t.is_current
  left join public.v_ts_invoice_precheck pc on pc.timesheet_id=s.timesheet_id
  left join public.v_timesheets_summary_base summary
    on summary.timesheet_id=s.timesheet_id
  left join lateral (
    select ev.id evidence_id,ev.document_asset_id
    from public.timesheet_evidence ev
    left join public.invoice_document_assets candidate_asset
      on candidate_asset.id=ev.document_asset_id
    where ev.timesheet_id=t.timesheet_id
      and upper(coalesce(ev.kind,''))='TIMESHEET'
      and coalesce(ev.processing_state,'')<>'SUPERSEDED'
    order by(ev.document_asset_id=t.manual_document_asset_id) desc,
      (candidate_asset.status='READY') desc,
      ev.created_at desc nulls last,ev.id desc
    limit 1
  ) manual_source on true
  left join public.invoice_document_assets manual_asset
    on manual_asset.id=coalesce(t.manual_document_asset_id,
      manual_source.document_asset_id)
  left join public.invoice_operations manual_asset_op
    on manual_asset_op.id=manual_asset.operation_id
  left join lateral (
    select dv.*
    from public.invoice_document_versions dv
    where dv.entity_type='TIMESHEET' and dv.entity_id=t.timesheet_id
      and dv.purpose='TIMESHEET'
      and dv.source_revision=t.document_revision::text
      and dv.template_version='timesheet-professional-v1'
      and dv.status in(
        'PLANNING','WAITING_FOR_INPUTS','RENDERING','ASSEMBLING',
        'VERIFYING','READY','FAILED','SUPERSEDED','CANCELLED')
    order by
      (dv.status='READY') desc,
      (dv.status in('PLANNING','WAITING_FOR_INPUTS','RENDERING',
        'ASSEMBLING','VERIFYING')) desc,
      dv.created_at_utc desc,dv.id desc
    limit 1
  ) v on true
  left join public.invoice_operations vop on vop.id=v.operation_id
),
timesheet_readiness as materialized (
  select t.*,
    case
      when not t.document_required then 'NOT_REQUIRED'
      when not t.source_exists then 'SOURCE_MISSING'
      when t.qr_unsigned then 'QR_UNSIGNED'
      when t.submission_mode in('MANUAL','QR')
        and t.manual_document_asset_id is null then 'MANUAL_SOURCE_MISSING'
      when t.submission_mode in('MANUAL','QR')
        and t.manual_document_asset_id is not null
        and t.manual_asset_status is null then 'ASSET_NOT_REGISTERED'
      when t.submission_mode in('MANUAL','QR') and(
        t.manual_asset_status in(
          'UNSUPPORTED','CORRUPT','MISSING','FAILED','SUPERSEDED')
        or t.manual_asset_operation_status='DEAD_LETTER')
        then 'ASSET_PERMANENT_FAILURE'
      when t.submission_mode in('MANUAL','QR')
        and t.manual_asset_status in('DISCOVERED','INSPECTING','NORMALISING')
        and coalesce(t.manual_asset_operation_status,'') not in(
          'QUEUED','RUNNING','WAITING','RETRY_WAIT')
        then 'ASSET_WORKFLOW_MISSING'
      when t.submission_mode in('MANUAL','QR')
        and t.manual_asset_status in('DISCOVERED','INSPECTING','NORMALISING')
        then 'ASSET_IN_PROGRESS'
      when t.timesheet_document_status='READY' then 'DOCUMENT_READY'
      when t.timesheet_document_status in('FAILED','SUPERSEDED','CANCELLED')
        and coalesce(t.timesheet_document_operation_status,'') not in(
          'QUEUED','RUNNING','WAITING','RETRY_WAIT')
        then 'DOCUMENT_PERMANENT_FAILURE'
      when t.timesheet_document_status in(
          'PLANNING','WAITING_FOR_INPUTS','RENDERING','ASSEMBLING','VERIFYING')
        or t.timesheet_document_operation_status in(
          'QUEUED','RUNNING','WAITING','RETRY_WAIT')
        then 'DOCUMENT_IN_PROGRESS'
      else 'DOCUMENT_CREATABLE'
    end readiness_classification
  from timesheet_state t
),
timesheet_readiness_json as materialized (
  select t.invoice_id,jsonb_agg(jsonb_build_object(
    'timesheet_id',t.timesheet_id,
    'required',t.document_required,
    'submission_mode',t.submission_mode,
    'readiness_classification',t.readiness_classification,
    'manual_source_evidence_id',t.manual_source_evidence_id,
    'manual_document_asset_id',t.manual_document_asset_id,
    'manual_asset_status',t.manual_asset_status,
    'manual_asset_operation_id',t.manual_asset_operation_id,
    'manual_asset_operation_status',t.manual_asset_operation_status,
    'timesheet_document_revision',t.timesheet_document_revision,
    'timesheet_document_version_id',t.timesheet_document_version_id,
    'timesheet_document_status',t.timesheet_document_status,
    'timesheet_document_operation_id',t.timesheet_document_operation_id,
    'timesheet_document_operation_status',t.timesheet_document_operation_status,
    'qr_unsigned',t.qr_unsigned)
    order by t.timesheet_id) readiness_rows
  from timesheet_readiness t
  group by t.invoice_id
),timesheet_checks as materialized (
  select i.invoice_id,
    count(*) filter(where t.document_required and not t.source_exists)
      missing_timesheet,
    count(*) filter(where t.document_required and t.source_exists
      and t.submission_mode in('MANUAL','QR')
      and t.manual_document_asset_id is null) missing_manual_source,
    count(*) filter(where t.document_required and t.source_exists
      and t.submission_mode in('MANUAL','QR')
      and t.manual_document_asset_id is not null
      and t.manual_asset_status is null) missing_asset_registration,
    count(*) filter(where t.document_required and t.qr_unsigned)
      unsigned_qr_source,
    count(*) filter(where t.reference_required
      and t.issue_missing_reference) missing_reference,
    count(*) filter(where t.document_required and t.source_exists
      and t.submission_mode in('MANUAL','QR')
      and(
        t.manual_asset_status in(
          'UNSUPPORTED','CORRUPT','MISSING','FAILED','SUPERSEDED')
        or t.manual_asset_operation_status='DEAD_LETTER'))
      permanently_failed_required_asset,
    count(*) filter(where t.document_required and t.source_exists
      and t.submission_mode in('MANUAL','QR')
      and t.manual_asset_status in('DISCOVERED','INSPECTING','NORMALISING')
      and coalesce(t.manual_asset_operation_status,'') not in(
        'QUEUED','RUNNING','WAITING','RETRY_WAIT'))
      missing_asset_workflow,
    count(*) filter(where t.document_required and t.source_exists
      and t.submission_mode in('MANUAL','QR')
      and t.manual_asset_status in('DISCOVERED','INSPECTING','NORMALISING')
      and t.manual_asset_operation_status in(
        'QUEUED','RUNNING','WAITING','RETRY_WAIT'))
      queueable_required_asset,
    count(*) filter(where t.document_required and t.source_exists
      and t.timesheet_document_status in(
        'FAILED','SUPERSEDED','CANCELLED')
      and coalesce(t.timesheet_document_operation_status,'') not in(
        'QUEUED','RUNNING','WAITING','RETRY_WAIT'))
      permanently_failed_timesheet_document,
    count(*) filter(where t.document_required and t.source_exists
      and coalesce(t.timesheet_document_status,'')<>'READY'
      and(
        t.timesheet_document_status in(
          'PLANNING','WAITING_FOR_INPUTS','RENDERING',
          'ASSEMBLING','VERIFYING')
        or t.timesheet_document_operation_status in(
          'QUEUED','RUNNING','WAITING','RETRY_WAIT')
        or(
          t.timesheet_document_version_id is null
          and t.submission_mode not in('MANUAL','QR'))
        or(
          t.timesheet_document_version_id is null
          and t.submission_mode in('MANUAL','QR')
          and t.manual_asset_status='READY')
      )) queueable_timesheet_document
  from invoice_scope i
  left join timesheet_readiness t on t.invoice_id=i.invoice_id
  group by i.invoice_id
),
import_source_requirements as materialized (
  select distinct s.invoice_id,s.timesheet_id,
    case
      when coalesce(summary.client_is_nhsp,false)
        or upper(coalesce(summary.route_type,'')) like '%NHSP%'
        or upper(coalesce(financial.basis::text,'')) like 'NHSP%'
        then 'NHSP'
      when upper(coalesce(summary.route_type,'')) like '%HEALTHROSTER%'
        or upper(coalesce(financial.basis::text,'')) in(
          'HEALTHROSTER','HEALTHROSTER_ADJUSTMENT','HR_WEEKLY','HR_DAILY')
        then 'HEALTHROSTER'
    end source_system,
    financial.nhsp_import_id import_id
  from source_requirements s
  left join public.v_timesheets_summary_base summary
    on summary.timesheet_id=s.timesheet_id
  left join public.timesheets_financials financial
    on financial.timesheet_id=s.timesheet_id and financial.is_current
  join public.invoices invoice_policy on invoice_policy.id=s.invoice_id
  where (
      coalesce(summary.client_is_nhsp,false)
      or upper(coalesce(summary.route_type,'')) like '%NHSP%'
      or upper(coalesce(financial.basis::text,'')) like 'NHSP%'
    )
    or (
      (
        upper(coalesce(summary.route_type,'')) like '%HEALTHROSTER%'
        or upper(coalesce(financial.basis::text,'')) in(
          'HEALTHROSTER','HEALTHROSTER_ADJUSTMENT','HR_WEEKLY','HR_DAILY')
      )
      and coalesce(
        (invoice_policy.header_snapshot_json#>>'{attach_policy,hr_attach_to_invoice}')::boolean,
        true
      )
    )
),
import_source_checks as materialized (
  select i.invoice_id,
    count(*) filter(where r.source_system is not null and not exists(
      select 1
      from public.invoice_hr_source_rows source
      where source.invoice_id=r.invoice_id
        and upper(coalesce(source.source_system,''))=r.source_system
        and(r.import_id is null or source.import_id=r.import_id)
        and jsonb_typeof(source.rows_json)='array'
        and jsonb_array_length(source.rows_json)>0
    )) missing_import_source
  from invoice_scope i
  left join import_source_requirements r on r.invoice_id=i.invoice_id
  group by i.invoice_id
),
evidence_requirements as materialized (
  select s.invoice_id,s.timesheet_id,'MILEAGE'::text requirement
  from source_requirements s where s.mileage_required
  union all
  select s.invoice_id,s.timesheet_id,'TRAVEL'
  from source_requirements s where s.travel_required
  union all
  select s.invoice_id,s.timesheet_id,'ACCOMMODATION'
  from source_requirements s where s.accommodation_required
  union all
  select s.invoice_id,s.timesheet_id,'GENERAL_EXPENSE'
  from source_requirements s where s.general_expense_required
),
evidence_state as materialized (
  select r.*,e.id evidence_id,e.document_asset_id,
    a.status asset_status,op.status asset_operation_status
  from evidence_requirements r
  left join lateral (
    select ev.*
    from public.timesheet_evidence ev
    left join public.invoice_document_assets candidate_asset
      on candidate_asset.id=ev.document_asset_id
    where ev.timesheet_id=r.timesheet_id
      and(
        upper(coalesce(ev.kind,''))=r.requirement
        or r.requirement='GENERAL_EXPENSE'
          and upper(coalesce(ev.kind,'')) in('OTHER','EXPENSE','EXPENSES'))
    order by
      (candidate_asset.status='READY') desc,
      (candidate_asset.status in(
        'DISCOVERED','INSPECTING','NORMALISING')) desc,
      ev.created_at desc nulls last,ev.id desc
    limit 1
  ) e on true
  left join public.invoice_document_assets a on a.id=e.document_asset_id
  left join public.invoice_operations op on op.id=a.operation_id
),
evidence_checks as materialized (
  select i.invoice_id,
    count(*) filter(where e.requirement='MILEAGE'
      and e.evidence_id is null) missing_mileage,
    count(*) filter(where e.requirement<>'MILEAGE'
      and e.evidence_id is null) missing_expense,
    count(*) filter(where e.evidence_id is not null
      and(e.document_asset_id is null or e.asset_status is null))
      missing_asset_registration,
    count(*) filter(where e.asset_status in(
      'UNSUPPORTED','CORRUPT','MISSING','FAILED','SUPERSEDED')
      or e.asset_operation_status='DEAD_LETTER')
      permanently_failed_required_asset,
    count(*) filter(where e.asset_status in(
      'DISCOVERED','INSPECTING','NORMALISING')
      and e.asset_operation_status in(
        'QUEUED','RUNNING','WAITING','RETRY_WAIT'))
      queueable_required_asset,
    count(*) filter(where e.asset_status in(
      'DISCOVERED','INSPECTING','NORMALISING')
      and coalesce(e.asset_operation_status,'') not in(
        'QUEUED','RUNNING','WAITING','RETRY_WAIT'))
      missing_asset_workflow
  from invoice_scope i
  left join evidence_state e on e.invoice_id=i.invoice_id
  group by i.invoice_id
),
reference_checks as materialized (
  select r.invoice_id,
    count(*) filter(where
      coalesce(precheck.reference_number_required_to_issue_invoice,false)
      and nullif(btrim(ref.current_reference),'') is null)
      missing_required_reference
  from invoice_scope r
  left join references_batch ref on ref.invoice_id=r.invoice_id
  left join public.v_ts_invoice_precheck precheck
    on precheck.timesheet_id=ref.timesheet_id
  group by r.invoice_id
),
correction_checks as materialized (
  select r.invoice_id,
    count(*) filter(where not coalesce(c.valid,true)) invalid_correction,
    (array_agg(c.blocker_code order by c.request_key) filter(where not coalesce(c.valid,true)))[1]
      correction_blocker
  from invoice_scope r
  left join corrections c on c.invoice_id=r.invoice_id
  group by r.invoice_id
),
facts as materialized (
  select r.request_no,r.request_key,r.operation_id,r.expected_revision,
    r.allow_early,r.deliver,r.expected_revision_well_formed,
    r.allow_early_well_formed,r.deliver_well_formed,
    r.invoice_id,
    r.recipient_set,r.cc,r.bcc,
    coalesce(rkc.request_key_count,0) request_key_count,
    i.status invoice_status,i.document_revision,i.on_hold_reason,
    i.subtotal_ex_vat,i.vat_amount,i.total_inc_vat,i.header_snapshot_json,
    lt.line_count,lt.net,lt.vat,lt.gross,
    ec.missing_mileage,ec.missing_expense,lt.missing_higher_rate,
    isc.missing_import_source,
    tc.missing_timesheet,tc.missing_manual_source,tc.unsigned_qr_source,
    tc.missing_reference,
    coalesce(tc.missing_asset_registration,0)
      +coalesce(ec.missing_asset_registration,0) missing_asset_registration,
    coalesce(tc.missing_asset_workflow,0)
      +coalesce(ec.missing_asset_workflow,0) missing_asset_workflow,
    coalesce(tc.permanently_failed_required_asset,0)
      +coalesce(ec.permanently_failed_required_asset,0)
      permanently_failed_required_asset,
    coalesce(tc.queueable_required_asset,0)
      +coalesce(ec.queueable_required_asset,0) queueable_required_asset,
    tc.permanently_failed_timesheet_document,
    tc.queueable_timesheet_document,rc.missing_required_reference,
    cc.invalid_correction,cc.correction_blocker,
    tr.readiness_rows timesheet_readiness,
    dr.canonical_to,dr.canonical_cc,dr.canonical_bcc,
    dr.recipient_set_hash,dr.grouping_identity,
    dr.route_policy_hash,
    dr.route_source,dr.warning_codes route_warnings,
    dr.blocker_codes route_blockers,dr.do_not_send,
    dr.delivery_suppressed,
    exists(
      select 1 from public.invoice_operation_chunks oc
      join public.invoice_operations other_operation
        on other_operation.id=oc.operation_id
      where oc.chunk_type='ISSUE_INVOICE' and oc.entity_type='INVOICE'
        and oc.entity_id=r.invoice_id
        and(r.operation_id is null or oc.operation_id<>r.operation_id)
        and oc.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
        and other_operation.status in(
          'QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED'))
      conflicting_issue
  from raw r
  left join request_counts rkc on rkc.request_key=r.request_key
  left join public.invoices i on i.id=r.invoice_id
  left join line_totals lt on lt.invoice_id=r.invoice_id
  left join timesheet_checks tc on tc.invoice_id=r.invoice_id
  left join import_source_checks isc on isc.invoice_id=r.invoice_id
  left join evidence_checks ec on ec.invoice_id=r.invoice_id
  left join reference_checks rc on rc.invoice_id=r.invoice_id
  left join correction_checks cc on cc.invoice_id=r.invoice_id
  left join timesheet_readiness_json tr on tr.invoice_id=r.invoice_id
  left join routes dr
    on dr.request_key=r.request_key and dr.invoice_id=r.invoice_id
),
classified as materialized (
  select f.*,
    array_remove(array[
      case when f.request_key is null then 'REQUEST_KEY_REQUIRED' end,
      case when f.request_key_count>1 then 'REQUEST_KEY_DUPLICATE' end,
      case when p_evaluation_date is null then 'EVALUATION_DATE_REQUIRED' end,
      case when f.invoice_status is null then 'INVOICE_NOT_FOUND' end,
      case when not f.expected_revision_well_formed
        then 'EXPECTED_REVISION_INVALID' end,
      case when not f.allow_early_well_formed
        then 'ALLOW_EARLY_INVALID' end,
      case when not f.deliver_well_formed
        then 'DELIVERY_INTENT_INVALID' end,
      case when f.invoice_status='ON_HOLD' or f.on_hold_reason is not null
        then 'INVOICE_ON_HOLD' end,
      case when f.invoice_status is not null and f.invoice_status<>'DRAFT'
        then 'INVOICE_NOT_DRAFT' end,
      case when f.expected_revision is not null
        and f.expected_revision<>f.document_revision
        then 'SOURCE_REVISION_CHANGED' end,
      case when coalesce(f.line_count,0)=0
        or round(coalesce(f.subtotal_ex_vat,0),2)<>coalesce(f.net,0)
        or round(coalesce(f.vat_amount,0),2)<>coalesce(f.vat,0)
        or round(coalesce(f.total_inc_vat,0),2)<>coalesce(f.gross,0)
        then 'INVALID_TOTALS' end,
      case when coalesce(f.missing_required_reference,0)>0
        or coalesce(f.missing_reference,0)>0 then 'MISSING_REFERENCE' end,
      case when coalesce(f.missing_timesheet,0)>0 then 'MISSING_TIMESHEET' end,
      case when coalesce(f.missing_manual_source,0)>0
        then 'MANUAL_TIMESHEET_SOURCE_MISSING' end,
      case when coalesce(f.unsigned_qr_source,0)>0
        then 'QR_TIMESHEET_UNSIGNED' end,
      case when coalesce(f.missing_asset_registration,0)>0
        then 'ASSET_NOT_REGISTERED' end,
      case when coalesce(f.missing_asset_workflow,0)>0
        then 'ASSET_WORKFLOW_MISSING' end,
      case when coalesce(f.permanently_failed_required_asset,0)>0
        then 'REQUIRED_ASSET_FAILED' end,
      case when coalesce(f.permanently_failed_timesheet_document,0)>0
        then 'TIMESHEET_DOCUMENT_FAILED' end,
      case when coalesce(f.missing_mileage,0)>0
        then 'MISSING_MILEAGE_EVIDENCE' end,
      case when coalesce(f.missing_expense,0)>0
        then 'MISSING_EXPENSE_EVIDENCE' end,
      case when coalesce(f.missing_higher_rate,0)>0
        then 'MISSING_HIGHER_RATE_SUPPORT' end,
      case when coalesce(f.missing_import_source,0)>0
        then 'MISSING_IMPORT_SOURCE_EVIDENCE' end,
      case when coalesce(f.invalid_correction,0)>0
        then coalesce(f.correction_blocker,'CORRECTION_LINES_NOT_UNIT_SAFE') end,
      case when not f.allow_early
        and coalesce(
          case when coalesce(f.header_snapshot_json#>>'{meta,invoice_week_start}','')
            ~'^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
            then (f.header_snapshot_json#>>'{meta,invoice_week_start}')::date+6
          end,p_evaluation_date-1)
          >=p_evaluation_date
        then 'EARLY_ISSUE_NOT_ALLOWED' end,
      case when f.conflicting_issue then 'CONFLICTING_ISSUE_OPERATION' end
    ],null)::text[] blockers
  from facts f
)
select c.request_key,c.invoice_id,
  to_jsonb(c.blockers),
  (case when coalesce(c.queueable_timesheet_document,0)>0
      then jsonb_build_array('TIMESHEET_DOCUMENT_NOT_READY')
    else '[]'::jsonb end)
  ||(case when coalesce(c.queueable_required_asset,0)>0
      then jsonb_build_array('REQUIRED_ASSET_NOT_READY')
    else '[]'::jsonb end),
  case when c.deliver
    then coalesce(c.route_blockers,'[]'::jsonb) else '[]'::jsonb end,
  coalesce(c.route_warnings,'[]'::jsonb),
  cardinality(c.blockers)=0,
  cardinality(c.blockers)=0
    and(not c.deliver or jsonb_array_length(
      coalesce(c.route_blockers,'[]'::jsonb))=0),
  jsonb_build_object(
    'request_key',c.request_key,
    'invoice_id',c.invoice_id,
    'route_policy_hash',c.route_policy_hash,
    'recipient_set_hash',c.recipient_set_hash,
    'grouping_identity',c.grouping_identity,
    'route_source',c.route_source,
    'do_not_send',c.do_not_send,
    'delivery_suppressed',c.delivery_suppressed,
    'canonical_to',c.canonical_to,
    'canonical_cc',c.canonical_cc,
    'canonical_bcc',c.canonical_bcc,
    'warnings',coalesce(c.route_warnings,'[]'::jsonb),
    'blockers',coalesce(c.route_blockers,'[]'::jsonb)),
  jsonb_build_object(
    'request_key',c.request_key,
    'evaluation_date',p_evaluation_date,
    'expected_revision',c.expected_revision,
    'current_revision',c.document_revision,
    'net_expected',c.subtotal_ex_vat,'net_lines',c.net,
    'vat_expected',c.vat_amount,'vat_lines',c.vat,
    'gross_expected',c.total_inc_vat,'gross_lines',c.gross,
    'hard_blockers',to_jsonb(c.blockers),
    'timesheet_readiness',coalesce(c.timesheet_readiness,'[]'::jsonb),
    'document_dependency_codes',
      (case when coalesce(c.queueable_timesheet_document,0)>0
          then jsonb_build_array('TIMESHEET_DOCUMENT_NOT_READY')
        else '[]'::jsonb end)
      ||(case when coalesce(c.queueable_required_asset,0)>0
          then jsonb_build_array('REQUIRED_ASSET_NOT_READY')
        else '[]'::jsonb end),
    'queueable_asset_count',coalesce(c.queueable_required_asset,0),
    'missing_import_source_count',coalesce(c.missing_import_source,0),
    'missing_asset_workflow_count',coalesce(c.missing_asset_workflow,0),
    'queueable_timesheet_document_count',
      coalesce(c.queueable_timesheet_document,0),
    'delivery_blockers',case when c.deliver
      then coalesce(c.route_blockers,'[]'::jsonb) else '[]'::jsonb end,
    'can_issue_only',cardinality(c.blockers)=0,
    'can_issue_and_deliver',cardinality(c.blockers)=0
      and(not c.deliver or jsonb_array_length(
        coalesce(c.route_blockers,'[]'::jsonb))=0),
    'recipient_set_hash',c.recipient_set_hash,
    'route_policy_hash',c.route_policy_hash,
    'grouping_identity',c.grouping_identity,
    'recipient_to',c.canonical_to,
    'recipient_cc',c.canonical_cc,
    'recipient_bcc',c.canonical_bcc,
    'route_source',c.route_source,
    'do_not_send',c.do_not_send,
    'delivery_suppressed',c.delivery_suppressed)
from classified c
order by c.request_no;
$function$;

create or replace function private._invoice_generation_advance_core_v8(
  p_claims jsonb,
  p_now_utc timestamptz
) returns jsonb
language plpgsql
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_now timestamptz := coalesce(p_now_utc,now());
  v_result jsonb := '[]'::jsonb;
  v_part jsonb;
begin
  -- VALIDATE_SOURCES: no invoice/header/line writes are permitted in this block.
  with claim_ids as materialized (
    select (x->>'chunk_id')::uuid chunk_id
    from jsonb_array_elements(p_claims) x where x->>'phase'='VALIDATE_SOURCES'
  ),
  member_values as materialized (
    select c.id chunk_id,c.operation_id,c.payload_json,
      m.value member,m.ordinality
    from claim_ids q
    join public.invoice_operation_chunks c on c.id=q.chunk_id
    cross join lateral jsonb_array_elements(
      case when jsonb_typeof(c.payload_json->'canonical_source_members')='array'
          and jsonb_array_length(c.payload_json->'canonical_source_members')>0
        then c.payload_json->'canonical_source_members'
        else coalesce((
          select jsonb_agg(jsonb_build_object(
            'source_type','TIMESHEET','source_id',s.value,
            'related_timesheet_id',s.value) order by s.ordinality)
          from jsonb_array_elements_text(coalesce(
            c.payload_json->'canonical_source_ids',
            c.payload_json->'source_ids','[]'::jsonb))
            with ordinality s(value,ordinality)
        ),'[]'::jsonb) end) with ordinality m(value,ordinality)
    where c.payload_json->>'command_type'<>'GENERATE_CREDIT_NOTE'
  ),
  members as materialized (
    select m.chunk_id,m.operation_id,m.payload_json,m.member,
      upper(coalesce(m.member->>'source_type','TIMESHEET')) source_type,
      case when coalesce(m.member->>'source_id','')~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        then(m.member->>'source_id')::uuid end source_id,
      case when coalesce(m.member->>'related_timesheet_id',
          m.member->>'source_id','')~*
        '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        then coalesce(m.member->>'related_timesheet_id',
          m.member->>'source_id')::uuid end timesheet_id,
      nullif(btrim(m.member->>'segment_id'),'') segment_id,
      coalesce(nullif(btrim(m.member->>'source_member_key'),''),
        encode(digest(concat_ws('|',
          upper(coalesce(m.member->>'source_type','TIMESHEET')),
          coalesce(m.member->>'source_id',''),
          coalesce(m.member->>'related_timesheet_id',''),
          coalesce(m.member->>'segment_id','WHOLE'),
          coalesce(m.member->>'target_invoice_week','')),'sha256'),'hex'))
        source_member_key,
      nullif(btrim(coalesce(m.member->>'row_revision',
        m.member->>'source_revision','')),'') expected_revision,
      m.ordinality
    from member_values m
  ),
  reference_eval as materialized (
    select distinct on (r.source_member_key) r.*
    from private._invoice_source_reference_validate_batch(coalesce((
      select jsonb_agg(jsonb_build_object(
        'source_member_key',m.source_member_key,
        'source_type',m.source_type,'source_id',m.source_id,
        'related_timesheet_id',m.timesheet_id,
        'segment_id',m.segment_id,
        'target_invoice_week',m.member->>'target_invoice_week',
        'invoice_stream',m.payload_json->>'invoice_stream',
        'consolidation_mode',m.payload_json->>'consolidation_mode',
        'source_revision',m.expected_revision)
        order by m.chunk_id,m.ordinality)
      from members m
    ),'[]'::jsonb)) r
    order by r.source_member_key
  ),
  vat_eval as materialized (
    select distinct on (v.source_member_key) v.*
    from private._invoice_generation_vat_policy_batch(coalesce((
      select jsonb_agg(jsonb_build_object(
        'source_member_key',m.source_member_key,
        'source_type',m.source_type,'source_id',m.source_id,
        'timesheet_id',m.timesheet_id,'segment_id',m.segment_id,
        'effective_date',coalesce(
          case when m.payload_json->>'effective_settings_date'
              ~'^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
              and pg_input_is_valid(
                m.payload_json->>'effective_settings_date','date')
            then m.payload_json->>'effective_settings_date' end,
          (select ts_vat.week_ending_date::text
             from public.timesheets ts_vat
            where ts_vat.timesheet_id=m.timesheet_id
              and ts_vat.is_current
            limit 1)))
        order by m.chunk_id,m.ordinality)
      from members m
    ),'[]'::jsonb)) v
    order by v.source_member_key
  ),
  correction_scopes as materialized (
    select coalesce(jsonb_agg(jsonb_build_object(
      'request_key','generation-validate:'||scope.chunk_id::text,
      'scope_key',scope.chunk_id::text,
      'validation_purpose','GENERATION_VALIDATE',
      'expected_client_id',scope.payload_json->>'client_id',
      'target_invoice_week',scope.payload_json->>'target_invoice_week',
      'expected_invoice_stream',scope.payload_json->>'invoice_stream',
      'planned_members',scope.planned_members)
      order by scope.chunk_id),'[]'::jsonb) scopes
    from(
      select m.chunk_id,min(m.payload_json::text)::jsonb payload_json,
        jsonb_agg(jsonb_build_object(
          'timesheet_id',m.timesheet_id,
          'source_type',m.source_type,
          'source_id',m.source_id,
          'source_member_key',m.source_member_key,
          'segment_id',m.segment_id,
          'target_invoice_week',m.payload_json->>'target_invoice_week',
          'vat_rate_pct',v.vat_rate)
          order by m.ordinality) planned_members
      from members m
      left join vat_eval v on v.source_member_key=m.source_member_key
      group by m.chunk_id
    ) scope
  ),
  correction_eval as materialized (
    select r.*
    from correction_scopes s
    cross join lateral private._invoice_correction_validate_batch(
      s.scopes,(v_now at time zone 'Europe/London')::date) r
  ),  source_eval as materialized (
    select m.chunk_id,m.operation_id,m.timesheet_id,m.segment_id,
      m.source_member_key,m.ordinality,
      tf.id tsfin_id,
      encode(digest(concat_ws('|',tf.id::text,tf.timesheet_version::text,
        tf.updated_at::text,ts.version::text,ts.updated_at::text,
        coalesce(tf.invoice_breakdown_json::text,'')),'sha256'),'hex') row_revision,
      array_remove(array[
        case when ts.timesheet_id is null then 'TIMESHEET_NOT_FOUND' end,
        case when ts.timesheet_id is not null and (not ts.is_current or ts.revoked_at is not null) then 'TIMESHEET_NOT_CURRENT' end,
        case when tf.id is null then 'CURRENT_FINANCIALS_MISSING' end,
        case when tf.id is not null and tf.is_stale then 'FINANCIALS_STALE' end,
        case when tf.id is not null and tf.processing_status::text<>'READY_FOR_INVOICE' then 'NOT_READY_FOR_INVOICE' end,
        case when tf.id is not null and tf.has_rate_issue then 'RATE_MISSING' end,
        case when tf.id is not null and tf.has_pay_channel_issue then 'PAY_CHANNEL_MISSING' end,
        case when tf.id is not null and tf.client_id is null then 'CLIENT_UNRESOLVED' end,
        case when tf.id is not null and tf.locked_by_invoice_id is not null then 'SOURCE_ALREADY_LOCKED' end,
        case when ts.authorised_at_server is null
          then 'TIMESHEET_NOT_AUTHORISED' end,
        case when upper(coalesce(ts.submission_mode::text,''))='QR'
          and(nullif(ts.qr_signed_hash,'') is null
            or ts.qr_signed_at_utc is null)
          then 'QR_TIMESHEET_UNSIGNED' end,
        case when pc.require_reference_to_invoice
          and coalesce(ref.reference_ready,false) is not true
          then coalesce(ref.blocker_code,'MISSING_REFERENCE') end,
        case when coalesce(tf.mileage_pay_ex_vat,0)<>0
            or coalesce(tf.mileage_charge_ex_vat,0)<>0
          then case when not exists(
            select 1 from public.timesheet_evidence e
            where e.timesheet_id=m.timesheet_id
              and upper(coalesce(e.kind,''))='MILEAGE'
              and nullif(e.storage_key,'') is not null)
            then 'MISSING_MILEAGE_EVIDENCE' end end,
        case when(coalesce(tf.mileage_pay_ex_vat,0)<>0
            or coalesce(tf.mileage_charge_ex_vat,0)<>0)
          and exists(
            select 1 from public.timesheet_evidence e
            where e.timesheet_id=m.timesheet_id
              and upper(coalesce(e.kind,''))='MILEAGE'
              and nullif(e.storage_key,'') is not null
              and e.document_asset_id is null)
          then 'MILEAGE_ASSET_NOT_REGISTERED' end,
        case when coalesce(tf.expenses_pay_ex_vat,0)<>0
            or coalesce(tf.expenses_charge_ex_vat,0)<>0
            or coalesce(tf.travel_pay_ex_vat,0)<>0
            or coalesce(tf.travel_charge_ex_vat,0)<>0
            or coalesce(tf.accommodation_pay_ex_vat,0)<>0
            or coalesce(tf.accommodation_charge_ex_vat,0)<>0
          then case when not exists(
            select 1 from public.timesheet_evidence e
            where e.timesheet_id=m.timesheet_id
              and upper(coalesce(e.kind,'')) in(
                'TRAVEL','ACCOMMODATION','OTHER','EXPENSE','EXPENSES')
              and nullif(e.storage_key,'') is not null)
            then 'MISSING_EXPENSE_EVIDENCE' end end,
        case when(
            coalesce(tf.expenses_pay_ex_vat,0)<>0
            or coalesce(tf.expenses_charge_ex_vat,0)<>0
            or coalesce(tf.travel_pay_ex_vat,0)<>0
            or coalesce(tf.travel_charge_ex_vat,0)<>0
            or coalesce(tf.accommodation_pay_ex_vat,0)<>0
            or coalesce(tf.accommodation_charge_ex_vat,0)<>0)
          and exists(
            select 1 from public.timesheet_evidence e
            where e.timesheet_id=m.timesheet_id
              and upper(coalesce(e.kind,'')) in(
                'TRAVEL','ACCOMMODATION','OTHER','EXPENSE','EXPENSES')
              and nullif(e.storage_key,'') is not null
              and e.document_asset_id is null)
          then 'EXPENSE_ASSET_NOT_REGISTERED' end,
        case when(coalesce(tf.travel_pay_ex_vat,0)<>0
            or coalesce(tf.travel_charge_ex_vat,0)<>0)
          and not exists(
            select 1 from public.timesheet_evidence e
            where e.timesheet_id=m.timesheet_id
              and upper(coalesce(e.kind,''))='TRAVEL'
              and nullif(e.storage_key,'') is not null)
          then 'MISSING_TRAVEL_EVIDENCE' end,
        case when(coalesce(tf.accommodation_pay_ex_vat,0)<>0
            or coalesce(tf.accommodation_charge_ex_vat,0)<>0)
          and not exists(
            select 1 from public.timesheet_evidence e
            where e.timesheet_id=m.timesheet_id
              and upper(coalesce(e.kind,''))='ACCOMMODATION'
              and nullif(e.storage_key,'') is not null)
          then 'MISSING_ACCOMMODATION_EVIDENCE' end,
        case when(coalesce(tf.other_pay_ex_vat,0)<>0
            or coalesce(tf.other_charge_ex_vat,0)<>0)
          and not exists(
            select 1 from public.timesheet_evidence e
            where e.timesheet_id=m.timesheet_id
              and upper(coalesce(e.kind,'')) in(
                'OTHER','EXPENSE','EXPENSES')
              and nullif(e.storage_key,'') is not null)
          then 'MISSING_OTHER_EXPENSE_EVIDENCE' end,
        case when exists(
            select 1 from public.timesheet_evidence e
            join public.invoice_document_assets a
              on a.id=e.document_asset_id
            where e.timesheet_id=m.timesheet_id
              and a.status in(
                'UNSUPPORTED','CORRUPT','MISSING','FAILED','SUPERSEDED'))
          then 'REQUIRED_ASSET_PERMANENT_FAILURE' end,
        case when coalesce(vat.valid,false) is not true
          then coalesce(vat.blocker_code,'VAT_POLICY_UNRESOLVED') end,
        case when corr.valid is false
          then coalesce(corr.blocker_code,'CORRECTION_VALIDATION_FAILED') end,
        case when coalesce(vs.hr_validation_required_for_invoice,false)
          and upper(coalesce(tf.hr_crosscheck_status,'')) not in(
            'OK','PASS','PASSED','MATCHED','NOT_REQUIRED')
          then 'HEALTHROSTER_VALIDATION_REQUIRED' end,
        case when coalesce(vs.client_is_nhsp,false)
          and coalesce(vs.nhsp_shift_count,0)>0
          and not exists(
            select 1
            from public.nhsp_shifts ns_ready
            where ns_ready.timesheet_id=m.timesheet_id
              and ns_ready.invoice_status='PENDING'
              and ns_ready.invoice_id is null
              and ns_ready.cancelled_at_utc is null)
          then 'NHSP_SOURCE_NOT_READY' end,
        case when exists(select 1 from public.invoice_lines l join public.invoices i on i.id=l.invoice_id
                         where l.timesheet_id=m.timesheet_id and i.status in ('DRAFT','ISSUED','ON_HOLD'))
             and coalesce(tf.invoice_breakdown_json->>'mode','')<>'SEGMENTS' then 'SOURCE_ALREADY_INVOICED' end
      ],null)::text[] blockers
    from members m
    left join public.timesheets ts on ts.timesheet_id=m.timesheet_id
    left join public.timesheets_financials tf on tf.timesheet_id=m.timesheet_id and tf.is_current
    left join public.v_ts_invoice_precheck pc on pc.timesheet_id=m.timesheet_id
    left join public.v_timesheets_summary_base vs
      on vs.timesheet_id=m.timesheet_id
    left join reference_eval ref on ref.source_member_key=m.source_member_key
    left join vat_eval vat on vat.source_member_key=m.source_member_key
    left join correction_eval corr
      on corr.scope_key=m.chunk_id::text
  ),
  resolver_inputs as materialized (
    select c.id chunk_id,c.payload_json,
      row_number() over(order by c.id)::integer command_no
    from claim_ids q
    join public.invoice_operation_chunks c on c.id=q.chunk_id
    where c.payload_json->>'command_type'<>'GENERATE_CREDIT_NOTE'
  ),
  canonical_now as materialized (
    select ri.chunk_id,r.group_key,r.source_revision_hash,
      r.blocker_code,r.blocker_detail
    from private._invoice_generation_resolve_command_groups(
      (select coalesce(jsonb_agg(ri.payload_json order by ri.command_no),
        '[]'::jsonb) from resolver_inputs ri),null,v_now) r
    join resolver_inputs ri on ri.command_no=r.command_no
    where r.group_key=case
      when left(coalesce(ri.payload_json->>'selection_key',''),9)='generate:'
        then substr(ri.payload_json->>'selection_key',10)
      else ri.payload_json->>'group_key' end
  ),
  per_chunk as materialized (
    select c.id chunk_id,
      n.source_revision_hash current_revision,
      coalesce(jsonb_agg(jsonb_build_object('source_id',se.timesheet_id,
        'segment_id',se.segment_id,
        'codes',to_jsonb(se.blockers)) order by se.ordinality)
        filter(where cardinality(se.blockers)>0),'[]'::jsonb)
        ||case when n.blocker_code is null then '[]'::jsonb
          else jsonb_build_array(coalesce(n.blocker_detail,
            jsonb_build_object('code',n.blocker_code))) end blockers,
      count(se.timesheet_id)::integer source_count
    from claim_ids q join public.invoice_operation_chunks c on c.id=q.chunk_id
    left join source_eval se on se.chunk_id=c.id
    left join canonical_now n on n.chunk_id=c.id
    group by c.id,n.source_revision_hash,n.blocker_code,n.blocker_detail
  ),
  updated as (
    update public.invoice_operation_chunks c
    set phase=case
          when p.source_count=0 then 'BLOCKED'
          when jsonb_array_length(p.blockers)>0 then 'BLOCKED'
          when nullif(c.payload_json->>'source_revision','') is not null
           and c.payload_json->>'source_revision'<>p.current_revision then 'SUPERSEDED'
          else 'PLAN' end,
        status=case
          when p.source_count=0 or jsonb_array_length(p.blockers)>0 then 'BLOCKED'
          when nullif(c.payload_json->>'source_revision','') is not null
           and c.payload_json->>'source_revision'<>p.current_revision then 'SUPERSEDED'
          else 'QUEUED' end,
        payload_json=c.payload_json||jsonb_build_object('source_revision',p.current_revision,'source_count',p.source_count),
        progress_json=jsonb_build_object('status_message',
          case when p.source_count=0 then 'No sources resolved'
               when jsonb_array_length(p.blockers)>0 then 'Source validation blocked'
               when nullif(c.payload_json->>'source_revision','') is not null
                and c.payload_json->>'source_revision'<>p.current_revision then 'Source changed'
               else 'Sources validated' end,'source_count',p.source_count),
        error_json=case
          when p.source_count=0 then jsonb_build_object('code','NO_SOURCES','sources','[]'::jsonb)
          when jsonb_array_length(p.blockers)>0 then jsonb_build_object('code','SOURCE_VALIDATION_BLOCKED','sources',p.blockers)
          when nullif(c.payload_json->>'source_revision','') is not null
           and c.payload_json->>'source_revision'<>p.current_revision then jsonb_build_object('code','SOURCE_CHANGED')
          else null end,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null,
        updated_at_utc=v_now
    from per_chunk p where c.id=p.chunk_id
    returning c.id,c.operation_id,c.status,c.phase,c.error_json
  )
  select coalesce(jsonb_agg(jsonb_build_object('chunk_id',id,'status',status,'phase',phase,'error',error_json)),'[]'::jsonb)
  into v_part from updated;
  v_result:=v_result||coalesce(v_part,'[]'::jsonb);

  -- Credit-note validation is deliberately separate from normal timesheet sources.
  with claim_ids as (
    select (x->>'chunk_id')::uuid chunk_id
    from jsonb_array_elements(p_claims) x where x->>'phase'='VALIDATE_SOURCES'
  ),
  credit_eval as (
    select c.id chunk_id,i.id invoice_id,
      encode(digest(concat_ws('|',i.id::text,i.updated_at::text,i.status::text,
        i.subtotal_ex_vat::text,i.vat_amount::text,i.total_inc_vat::text),'sha256'),'hex') revision,
      case when i.id is null then 'INVOICE_NOT_FOUND'
           when i.type::text<>'INVOICE' then 'CREDIT_SOURCE_NOT_INVOICE'
           when i.status::text not in ('ISSUED','PAID') then 'CREDIT_SOURCE_NOT_ISSUED'
           when exists(select 1 from public.invoices cn where cn.original_invoice_id=i.id and cn.type='CREDIT_NOTE'
                       and cn.status in('DRAFT','ISSUED','PAID')) then 'CREDIT_ALREADY_EXISTS'
           else null end blocker
    from claim_ids q join public.invoice_operation_chunks c on c.id=q.chunk_id
    left join public.invoices i on i.id=c.entity_id
    where c.payload_json->>'command_type'='GENERATE_CREDIT_NOTE'
  ),
  updated as (
    update public.invoice_operation_chunks c
    set phase=case when e.blocker is null then 'PLAN' else 'BLOCKED' end,
        status=case when e.blocker is null then 'QUEUED' else 'BLOCKED' end,
        payload_json=c.payload_json||jsonb_build_object('source_revision',e.revision,'source_invoice_id',e.invoice_id),
        progress_json=jsonb_build_object('status_message',case when e.blocker is null then 'Credit source validated' else 'Credit source blocked' end),
        error_json=case when e.blocker is null then null else jsonb_build_object('code',e.blocker,'invoice_id',c.entity_id) end,
        lease_owner=null,lease_token=null,lease_expires_at_utc=null,
        updated_at_utc=v_now
    from credit_eval e where c.id=e.chunk_id
    returning c.id,c.status,c.phase,c.error_json
  )
  select v_result||coalesce(jsonb_agg(jsonb_build_object('chunk_id',id,'status',status,'phase',phase,'error',error_json)),'[]'::jsonb)
  into v_result from updated;

  -- PLAN: compact financial/settings summary only; no complete line arrays.
  with claim_ids as materialized (
    select (x->>'chunk_id')::uuid chunk_id
    from jsonb_array_elements(p_claims) x where x->>'phase'='PLAN'
  ),
  members as materialized (
    select c.id chunk_id,c.payload_json,
      case when pg_input_is_valid(s.value,'uuid')
        then s.value::uuid end timesheet_id
    from claim_ids q join public.invoice_operation_chunks c on c.id=q.chunk_id
    cross join lateral jsonb_array_elements_text(coalesce(
      c.payload_json->'canonical_source_ids',c.payload_json->'source_ids','[]'::jsonb)) s(value)
    where c.payload_json->>'command_type'<>'GENERATE_CREDIT_NOTE'
      and pg_input_is_valid(s.value,'uuid')
  ),
  values_by_chunk as materialized (
    select m.chunk_id,count(*)::integer source_count,
      round(sum(coalesce(tf.total_pay_ex_vat,0)),2) expected_pay_ex_vat,
      round(sum(coalesce(tf.total_charge_ex_vat,0)),2) expected_charge_ex_vat,
      (array_agg(tf.client_id order by tf.client_id))[1] client_id,
      jsonb_build_object(
        'client_id',(array_agg(tf.client_id order by tf.client_id))[1],'consolidation_mode',max(c.payload_json->>'consolidation_mode'),
        'stream',max(c.payload_json->>'invoice_stream'),
        'invoice_week_start',max(c.payload_json->>'target_invoice_week'),
        'client',jsonb_build_object('name',max(cl.name),'vat_chargeable',bool_and(cl.vat_chargeable),
          'payment_terms_days',max(cl.payment_terms_days),'primary_invoice_email',max(cl.primary_invoice_email)),
        'attach_policy',jsonb_build_object(
          'hr_attach_to_invoice',coalesce(bool_or(cs.hr_attach_to_invoice),bool_or(sd.hr_attach_to_invoice),true),
          'ts_attach_to_invoice',coalesce(bool_or(cs.ts_attach_to_invoice),bool_or(sd.ts_attach_to_invoice),true),
          'requires_hr',coalesce(bool_or(cs.requires_hr),false))
      ) settings_snapshot
    from members m join public.invoice_operation_chunks c on c.id=m.chunk_id
    join public.timesheets_financials tf on tf.timesheet_id=m.timesheet_id and tf.is_current
    join public.clients cl on cl.id=tf.client_id
    left join lateral (
      select s.* from public.client_settings s where s.client_id=tf.client_id
       and (s.effective_from is null or s.effective_from<=coalesce(
      case when c.payload_json->>'effective_settings_date' ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
          and pg_input_is_valid(c.payload_json->>'effective_settings_date','date')
           then (c.payload_json->>'effective_settings_date')::date end,
         (v_now at time zone 'Europe/London')::date))
      order by s.effective_from desc nulls last,s.updated_at desc nulls last,
        s.created_at desc nulls last,s.id desc
      limit 1
    ) cs on true
    cross join public.settings_defaults sd
    where sd.id=1
    group by m.chunk_id
  ),
  plans as materialized (
    select c.id chunk_id,coalesce(
      case when pg_input_is_valid(
        nullif(c.payload_json->>'planned_invoice_id',''),'uuid')
        then(c.payload_json->>'planned_invoice_id')::uuid end,
      case when c.payload_json->>'consolidation_mode' in ('BY_WEEK','ANY_WEEK') then (
        select i.id from public.invoices i
        where i.client_id=v.client_id and i.status='DRAFT' and i.type='INVOICE'
          and coalesce(i.header_snapshot_json#>>'{meta,consolidation_mode}','NONE')=c.payload_json->>'consolidation_mode'
          and coalesce(i.header_snapshot_json#>>'{meta,self_bill}','false')=
              case when c.payload_json->>'invoice_stream'='SELF_BILL' then 'true' else 'false' end
          and (not private._candidate_feature_enabled_current_v1('candidate_expense_invoice_routing_v1')
            or upper(coalesce(
              nullif(i.header_snapshot_json#>>'{meta,invoice_stream}',''),
              nullif(i.header_snapshot_json->>'invoice_stream',''),
              case when lower(coalesce(i.header_snapshot_json#>>'{meta,self_bill}','false'))
                in('true','t','1','yes') then 'SELF_BILL' else 'NORMAL' end
            ))=upper(coalesce(c.payload_json->>'invoice_stream','NORMAL')))
          and (c.payload_json->>'consolidation_mode'='ANY_WEEK'
               or i.header_snapshot_json#>>'{meta,invoice_week_start}'=c.payload_json->>'target_invoice_week')
        order by i.created_at desc limit 1
      ) end,gen_random_uuid()) planned_invoice_id,
      v.source_count,v.expected_charge_ex_vat,v.expected_pay_ex_vat,v.client_id,
      v.settings_snapshot,encode(digest(v.settings_snapshot::text,'sha256'),'hex') settings_hash
    from claim_ids q join public.invoice_operation_chunks c on c.id=q.chunk_id
    join values_by_chunk v on v.chunk_id=c.id
  ),
  credit_plans as materialized (
    select c.id chunk_id,gen_random_uuid() planned_invoice_id,1 source_count,
      -i.subtotal_ex_vat expected_charge_ex_vat,-coalesce(sum(l.total_pay_ex_vat),0) expected_pay_ex_vat,
      i.client_id,jsonb_build_object('source_invoice_id',i.id,'source_invoice_no',i.invoice_no,
        'client_id',i.client_id,'credit_note',true) settings_snapshot,
      encode(digest(concat_ws('|',i.id::text,i.updated_at::text),'sha256'),'hex') settings_hash
    from claim_ids q join public.invoice_operation_chunks c on c.id=q.chunk_id
    join public.invoices i on i.id=c.entity_id
    left join public.invoice_lines l on l.invoice_id=i.id
    where c.payload_json->>'command_type'='GENERATE_CREDIT_NOTE'
    group by c.id,i.id
  ),
  all_plans as materialized (select * from plans union all select * from credit_plans),
  updated as (
    update public.invoice_operation_chunks c
    set phase='COMMIT',status='QUEUED',
      payload_json=c.payload_json||jsonb_build_object('plan',jsonb_build_object(
        'planned_invoice_id',p.planned_invoice_id,'intended_invoice_count',1,
        'source_count',p.source_count,'expected_pay_ex_vat',p.expected_pay_ex_vat,
        'expected_charge_ex_vat',p.expected_charge_ex_vat,'settings_snapshot',p.settings_snapshot,
        'settings_hash',p.settings_hash,'source_revision',c.payload_json->>'source_revision')),
      progress_json=jsonb_build_object('status_message','Generation plan ready','source_count',p.source_count),
      error_json=null,lease_owner=null,lease_token=null,
      lease_expires_at_utc=null,updated_at_utc=v_now
    from all_plans p where c.id=p.chunk_id
    returning c.id,c.status,c.phase,c.payload_json->'plan' plan
  )
  select coalesce(jsonb_agg(jsonb_build_object('chunk_id',id,'status',status,'phase',phase,'plan',plan)),'[]'::jsonb)
    into v_part from updated;
  v_result:=v_result||coalesce(v_part,'[]'::jsonb);

  -- COMMIT: revalidate immediately, then headers, lines, locks, documents and audit set-wise.
  with recursive claim_ids as materialized (
    select (x->>'chunk_id')::uuid chunk_id
    from jsonb_array_elements(p_claims) x where x->>'phase'='COMMIT'
  ),
  normal_chunks as materialized (
    select c.*,case when pg_input_is_valid(
        c.payload_json#>>'{plan,planned_invoice_id}','uuid')
      then(c.payload_json#>>'{plan,planned_invoice_id}')::uuid end planned_invoice_id
    from claim_ids q join public.invoice_operation_chunks c on c.id=q.chunk_id
    where c.payload_json->>'command_type'<>'GENERATE_CREDIT_NOTE'
  ),
  members as materialized (
    select c.id chunk_id,c.operation_id,c.planned_invoice_id,c.payload_json,
      upper(coalesce(s.value->>'source_type','TIMESHEET')) source_type,
      case when pg_input_is_valid(s.value->>'source_id','uuid')
        then(s.value->>'source_id')::uuid end source_id,
      case when pg_input_is_valid(coalesce(
          s.value->>'related_timesheet_id',s.value->>'source_id'),'uuid')
        then coalesce(s.value->>'related_timesheet_id',
          s.value->>'source_id')::uuid end timesheet_id,
      nullif(btrim(s.value->>'segment_id'),'') segment_id,
      coalesce(nullif(btrim(s.value->>'source_member_key'),''),
        encode(digest(concat_ws('|',
          upper(coalesce(s.value->>'source_type','TIMESHEET')),
          coalesce(s.value->>'source_id',''),
          coalesce(s.value->>'related_timesheet_id',''),
          coalesce(s.value->>'segment_id','WHOLE'),
          coalesce(s.value->>'target_invoice_week','')),'sha256'),'hex'))
        source_member_key
    from normal_chunks c
    cross join lateral jsonb_array_elements(case
      when jsonb_typeof(c.payload_json->'canonical_source_members')='array'
        and jsonb_array_length(c.payload_json->'canonical_source_members')>0
      then c.payload_json->'canonical_source_members'
      else coalesce((
        select jsonb_agg(jsonb_build_object(
          'source_type','TIMESHEET','source_id',ids.value,
          'related_timesheet_id',ids.value) order by ids.ordinality)
        from jsonb_array_elements_text(coalesce(
          c.payload_json->'canonical_source_ids',
          c.payload_json->'source_ids','[]'::jsonb))
          with ordinality ids(value,ordinality)
      ),'[]'::jsonb) end) s(value)
    where pg_input_is_valid(coalesce(
      s.value->>'related_timesheet_id',s.value->>'source_id'),'uuid')
  ),
  selected_segments as materialized (
    select c.id chunk_id,
      (m.value->>'source_id')::uuid timesheet_id,
      array_agg(distinct m.value->>'segment_id'
        order by m.value->>'segment_id')
        filter(where nullif(m.value->>'segment_id','') is not null) segment_ids
    from normal_chunks c
    cross join lateral jsonb_array_elements(
      case when jsonb_typeof(c.payload_json->'canonical_source_members')='array'
        then c.payload_json->'canonical_source_members' else '[]'::jsonb end)
      m(value)
    where coalesce(m.value->>'source_id','')~
      '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
    group by c.id,(m.value->>'source_id')::uuid
  ),
  commit_reference_eval as materialized (
    select distinct on(r.source_member_key) r.*
    from private._invoice_source_reference_validate_batch(coalesce((
      select jsonb_agg(jsonb_build_object(
        'source_member_key',m.source_member_key,
        'source_type',m.source_type,
        'source_id',m.source_id,
        'related_timesheet_id',m.timesheet_id,
        'segment_id',m.segment_id,
        'target_invoice_week',m.payload_json->>'target_invoice_week',
        'invoice_stream',m.payload_json->>'invoice_stream',
        'consolidation_mode',m.payload_json->>'consolidation_mode')
        order by m.chunk_id,m.source_member_key)
      from members m),'[]'::jsonb)) r
    order by r.source_member_key
  ),
  commit_vat_eval as materialized (
    select distinct on(v.source_member_key) v.*
    from private._invoice_generation_vat_policy_batch(coalesce((
      select jsonb_agg(jsonb_build_object(
        'source_member_key',m.source_member_key,
        'source_type',m.source_type,
        'source_id',m.source_id,
        'timesheet_id',m.timesheet_id,
        'segment_id',m.segment_id,
        'effective_date',coalesce(
          case when m.payload_json->>'effective_settings_date'
              ~'^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
              and pg_input_is_valid(
                m.payload_json->>'effective_settings_date','date')
            then m.payload_json->>'effective_settings_date' end,
          (select ts_vat.week_ending_date::text
             from public.timesheets ts_vat
            where ts_vat.timesheet_id=m.timesheet_id
              and ts_vat.is_current
            limit 1)))
        order by m.chunk_id,m.source_member_key)
      from members m),'[]'::jsonb)) v
    order by v.source_member_key
  ),
  commit_correction_scopes as materialized (
    select coalesce(jsonb_agg(jsonb_build_object(
      'request_key','generation-commit:'||scope.planned_invoice_id::text,
      'scope_key',scope.planned_invoice_id::text,
      'invoice_id',case when exists(select 1 from public.invoices existing
        where existing.id=scope.planned_invoice_id)
        then scope.planned_invoice_id end,
      'validation_purpose','GENERATION_COMMIT',
      'expected_client_id',scope.payload_json->>'client_id',
      'target_invoice_week',scope.payload_json->>'target_invoice_week',
      'expected_invoice_stream',scope.payload_json->>'invoice_stream',
      'planned_members',scope.planned_members)
      order by scope.planned_invoice_id),'[]'::jsonb) scopes
    from(
      select m.planned_invoice_id,min(m.payload_json::text)::jsonb payload_json,
        jsonb_agg(jsonb_build_object(
          'timesheet_id',m.timesheet_id,
          'source_type',m.source_type,
          'source_id',m.source_id,
          'source_member_key',m.source_member_key,
          'segment_id',m.segment_id,
          'target_invoice_week',m.payload_json->>'target_invoice_week',
          'vat_rate_pct',v.vat_rate)
          order by m.source_member_key) planned_members
      from members m
      left join commit_vat_eval v
        on v.source_member_key=m.source_member_key
      group by m.planned_invoice_id
    ) scope
  ),
  commit_correction_eval as materialized (
    select r.*
    from commit_correction_scopes s
    cross join lateral private._invoice_correction_validate_batch(
      s.scopes,(v_now at time zone 'Europe/London')::date) r
  ),  source_still_valid as materialized (
    select m.chunk_id,
      bool_and(tf.processing_status='READY_FOR_INVOICE' and not tf.is_stale
        and (tf.locked_by_invoice_id is null
          or coalesce(tf.invoice_breakdown_json->>'mode','')='SEGMENTS')
        and ts.authorised_at_server is not null
        and not(upper(coalesce(ts.submission_mode::text,''))='QR'
          and(nullif(ts.qr_signed_hash,'') is null
            or ts.qr_signed_at_utc is null))
        and(not coalesce(pc.require_reference_to_invoice,false)
          or coalesce(ref.reference_ready,false))
        and coalesce(vat.valid,false)
        and coalesce(corr.valid,true)
        and not(coalesce(vs.hr_validation_required_for_invoice,false)
          and upper(coalesce(tf.hr_crosscheck_status,'')) not in(
            'OK','PASS','PASSED','MATCHED','NOT_REQUIRED'))
        and not(coalesce(vs.client_is_nhsp,false)
          and coalesce(vs.nhsp_shift_count,0)>0
          and not exists(
            select 1
            from public.nhsp_shifts ns_ready
            where ns_ready.timesheet_id=m.timesheet_id
              and ns_ready.invoice_status='PENDING'
              and ns_ready.invoice_id is null
              and ns_ready.cancelled_at_utc is null))
        and not((coalesce(tf.mileage_pay_ex_vat,0)<>0
              or coalesce(tf.mileage_charge_ex_vat,0)<>0)
          and not exists(
            select 1 from public.timesheet_evidence e
            join public.invoice_document_assets a
              on a.id=e.document_asset_id
             and a.status not in('UNSUPPORTED','CORRUPT','MISSING',
               'FAILED','SUPERSEDED')
            where e.timesheet_id=m.timesheet_id
              and upper(coalesce(e.kind,''))='MILEAGE'
              and nullif(e.storage_key,'') is not null))
        and not((coalesce(tf.travel_pay_ex_vat,0)<>0
              or coalesce(tf.travel_charge_ex_vat,0)<>0)
          and not exists(
            select 1 from public.timesheet_evidence e
            join public.invoice_document_assets a
              on a.id=e.document_asset_id
             and a.status not in('UNSUPPORTED','CORRUPT','MISSING',
               'FAILED','SUPERSEDED')
            where e.timesheet_id=m.timesheet_id
              and upper(coalesce(e.kind,''))='TRAVEL'
              and nullif(e.storage_key,'') is not null))
        and not((coalesce(tf.accommodation_pay_ex_vat,0)<>0
              or coalesce(tf.accommodation_charge_ex_vat,0)<>0)
          and not exists(
            select 1 from public.timesheet_evidence e
            join public.invoice_document_assets a
              on a.id=e.document_asset_id
             and a.status not in('UNSUPPORTED','CORRUPT','MISSING',
               'FAILED','SUPERSEDED')
            where e.timesheet_id=m.timesheet_id
              and upper(coalesce(e.kind,''))='ACCOMMODATION'
              and nullif(e.storage_key,'') is not null))
        and not((coalesce(tf.expenses_pay_ex_vat,0)<>0
              or coalesce(tf.expenses_charge_ex_vat,0)<>0)
          and not exists(
            select 1 from public.timesheet_evidence e
            join public.invoice_document_assets a
              on a.id=e.document_asset_id
             and a.status not in('UNSUPPORTED','CORRUPT','MISSING',
               'FAILED','SUPERSEDED')
            where e.timesheet_id=m.timesheet_id
              and upper(coalesce(e.kind,'')) in(
                'OTHER','EXPENSE','EXPENSES')
              and nullif(e.storage_key,'') is not null))
        and not((coalesce(tf.other_pay_ex_vat,0)<>0
              or coalesce(tf.other_charge_ex_vat,0)<>0)
          and not exists(
            select 1 from public.timesheet_evidence e
            join public.invoice_document_assets a
              on a.id=e.document_asset_id
             and a.status not in('UNSUPPORTED','CORRUPT','MISSING',
               'FAILED','SUPERSEDED')
            where e.timesheet_id=m.timesheet_id
              and upper(coalesce(e.kind,'')) in(
                'OTHER','EXPENSE','EXPENSES')
              and nullif(e.storage_key,'') is not null))
        and not exists(
          select 1
          from public.timesheet_evidence e
          left join public.invoice_document_assets a
            on a.id=e.document_asset_id
          where e.timesheet_id=m.timesheet_id
            and(
              (upper(coalesce(e.kind,''))='MILEAGE'
                and(coalesce(tf.mileage_pay_ex_vat,0)<>0
                  or coalesce(tf.mileage_charge_ex_vat,0)<>0))
              or(upper(coalesce(e.kind,'')) in(
                  'TRAVEL','ACCOMMODATION','OTHER','EXPENSE','EXPENSES')
                and(coalesce(tf.expenses_pay_ex_vat,0)<>0
                  or coalesce(tf.expenses_charge_ex_vat,0)<>0
                  or coalesce(tf.travel_pay_ex_vat,0)<>0
                  or coalesce(tf.travel_charge_ex_vat,0)<>0
                  or coalesce(tf.accommodation_pay_ex_vat,0)<>0
                  or coalesce(tf.accommodation_charge_ex_vat,0)<>0)))
            and(e.document_asset_id is null
              or a.status in('UNSUPPORTED','CORRUPT','MISSING',
                'FAILED','SUPERSEDED')))
        and not exists(
          select 1
          from public.invoice_lines l
          join public.invoices existing on existing.id=l.invoice_id
          where l.timesheet_id=m.timesheet_id
            and existing.id<>m.planned_invoice_id
            and existing.status in('DRAFT','ISSUED','ON_HOLD')
            and coalesce(tf.invoice_breakdown_json->>'mode','')<>'SEGMENTS')
      ) still_valid
    from members m join public.timesheets_financials tf on tf.timesheet_id=m.timesheet_id and tf.is_current
    join public.timesheets ts on ts.timesheet_id=m.timesheet_id and ts.is_current and ts.revoked_at is null
    left join public.v_ts_invoice_precheck pc on pc.timesheet_id=m.timesheet_id
    left join public.v_timesheets_summary_base vs
      on vs.timesheet_id=m.timesheet_id
    left join commit_reference_eval ref
      on ref.source_member_key=m.source_member_key
    left join commit_vat_eval vat
      on vat.source_member_key=m.source_member_key
    left join commit_correction_eval corr
      on corr.scope_key=m.planned_invoice_id::text
    group by m.chunk_id
  ),
  commit_resolver_inputs as materialized (
    select c.id chunk_id,c.payload_json,
      row_number() over(order by c.id)::integer command_no
    from normal_chunks c
  ),
  commit_resolver_results as materialized (
    select ri.chunk_id,r.source_revision_hash,r.blocker_code
    from private._invoice_generation_resolve_command_groups(
      (select coalesce(jsonb_agg(ri.payload_json order by ri.command_no),
        '[]'::jsonb) from commit_resolver_inputs ri),null,v_now) r
    join commit_resolver_inputs ri on ri.command_no=r.command_no
    where r.group_key=case
      when left(coalesce(ri.payload_json->>'selection_key',''),9)='generate:'
        then substr(ri.payload_json->>'selection_key',10)
      else ri.payload_json->>'group_key' end
  ),
  revision_check as materialized (
    select c.id chunk_id,r.source_revision_hash current_revision,
      s.still_valid and r.blocker_code is null still_valid
    from normal_chunks c
    join source_still_valid s on s.chunk_id=c.id
    join commit_resolver_results r on r.chunk_id=c.id
  ),
  rejected as (
    update public.invoice_operation_chunks c
    set status='SUPERSEDED',phase='SUPERSEDED',
      error_json=jsonb_build_object('code','SOURCE_CHANGED_BEFORE_COMMIT'),
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,
      updated_at_utc=v_now
    from revision_check r
    where c.id=r.chunk_id and (not r.still_valid or r.current_revision<>c.payload_json->>'source_revision')
    returning c.id
  ),
  valid_chunks as materialized (
    select c.* from normal_chunks c join revision_check r on r.chunk_id=c.id
    where r.still_valid and r.current_revision=c.payload_json->>'source_revision'
      and not exists(select 1 from rejected x where x.id=c.id)
  ),
  header_source as materialized (
    select vc.id chunk_id,vc.planned_invoice_id,(array_agg(tf.client_id order by tf.client_id))[1] client_id,
      max(cl.name) client_name,max(cl.invoice_address) client_invoice_address,
      max(cl.primary_invoice_email) primary_invoice_email,bool_and(cl.vat_chargeable) vat_chargeable,
      max(cl.payment_terms_days) payment_terms_days,
      max(sd.agency_name) agency_name,max(sd.agency_logo) agency_logo,
      max(sd.registered_address) registered_address,max(sd.company_reg_number) company_reg_number,
      max(sd.bank_name) bank_name,max(sd.bank_sort_code) bank_sort_code,
      max(sd.bank_account_number) bank_account_number,max(sd.vat_registration_number) vat_registration_number,
      count(*)::integer source_count
    from valid_chunks vc join members m on m.chunk_id=vc.id
    join public.timesheets_financials tf on tf.timesheet_id=m.timesheet_id and tf.is_current
    join public.clients cl on cl.id=tf.client_id cross join public.settings_defaults sd
    where sd.id=1 group by vc.id,vc.planned_invoice_id
  ),
  source_rows_base as materialized (
    select m.chunk_id,m.planned_invoice_id,m.payload_json,
      m.source_member_key,m.source_type,m.source_id,m.segment_id,tf.*,ts.booking_id,
      ts.week_ending_date,ts.reference_number,ts.sheet_scope,ts.submission_mode,ts.day_references_json,
      ts.actual_schedule_json,coalesce(ts.contract_id,cw.contract_id) contract_id,
      coalesce(cd.display_name,'Candidate '||left(coalesce(tf.candidate_id::text,m.timesheet_id::text),8)) candidate_display,
      coalesce(ct.daily_calc_of_invoices,false) daily_calc_of_invoices,
      coalesce(ct.bucket_labels_json,
        jsonb_build_object('day','Day','night','Night','sat','Sat','sun','Sun','bh','BH'))
        bucket_labels_json,
      ct.role contract_role,ct.display_site contract_display_site,
      ct.ward_hint contract_ward_hint,
      null::numeric ordinary_vat_rate,
      case when ts.sheet_scope::text='WEEKLY'
          and ts.submission_mode::text='MANUAL'
          and jsonb_typeof(ts.actual_schedule_json)='array'
        then coalesce((
          select jsonb_agg(jsonb_build_object(
            'date',e.value->>'date','start',e.value->>'start',
            'end',e.value->>'end','start_utc',e.value->>'start_utc',
            'end_utc',e.value->>'end_utc','ref_num',e.value->>'ref_num')
            order by e.ordinality)
          from jsonb_array_elements(ts.actual_schedule_json)
            with ordinality e(value,ordinality)
          where nullif(btrim(e.value->>'start'),'') is not null
            and nullif(btrim(e.value->>'end'),'') is not null
        ),'[]'::jsonb)
        else '[]'::jsonb end schedule_refs
    from members m join valid_chunks vc on vc.id=m.chunk_id
    join public.timesheets_financials tf on tf.timesheet_id=m.timesheet_id and tf.is_current
    join public.timesheets ts on ts.timesheet_id=m.timesheet_id and ts.is_current
    left join lateral (
      select w.contract_id
      from public.contract_weeks w
      where w.timesheet_id=m.timesheet_id
      order by w.updated_at desc nulls last,w.created_at desc nulls last,w.id desc
      limit 1
    ) cw on true
    left join public.contracts ct on ct.id=coalesce(ts.contract_id,cw.contract_id)
    left join public.candidates cd on cd.id=tf.candidate_id
  ),
  vat_policy as materialized (
    select distinct on (v.source_member_key) v.*
    from private._invoice_generation_vat_policy_batch(coalesce((
      select jsonb_agg(jsonb_build_object(
        'source_member_key',s.source_member_key,
        'source_type',s.source_type,'source_id',s.source_id,
        'timesheet_id',s.timesheet_id,'segment_id',s.segment_id,
        'ordinary_rate',s.ordinary_vat_rate,
        'effective_date',coalesce(
          case when s.payload_json->>'effective_settings_date'
              ~'^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
              and pg_input_is_valid(
                s.payload_json->>'effective_settings_date','date')
            then(s.payload_json->>'effective_settings_date')::date end,
          s.week_ending_date::date))
        order by s.chunk_id,s.timesheet_id)
      from source_rows_base s
    ),'[]'::jsonb)) v
    order by v.source_member_key
  ),
  source_rows as materialized (
    select s.*,v.vat_rate
    from source_rows_base s
    join vat_policy v on v.source_member_key=s.source_member_key and v.valid
  ),
  source_timesheet_ancestry(
    planned_invoice_id,
    root_timesheet_id,
    source_timesheet_id,
    ancestry_path,
    ancestry_depth
  ) as materialized (
    select distinct s.planned_invoice_id,s.timesheet_id,s.timesheet_id,
      array[s.timesheet_id]::uuid[],0
    from source_rows s
    union all
    select a.planned_invoice_id,a.root_timesheet_id,t.parent_timesheet_id,
      a.ancestry_path||t.parent_timesheet_id,a.ancestry_depth+1
    from source_timesheet_ancestry a
    join public.timesheets t
      on t.timesheet_id=a.source_timesheet_id
     and t.is_current
    where t.parent_timesheet_id is not null
      and not(t.parent_timesheet_id=any(a.ancestry_path))
      and a.ancestry_depth<32
  ),
  adjustment_segment_refs as materialized (
    select distinct s.planned_invoice_id,s.timesheet_id root_timesheet_id,
      coalesce(
        (s.payload_json#>>'{plan,settings_snapshot,attach_policy,hr_attach_to_invoice}')::boolean,
        true
      ) healthroster_attach_allowed,
      nullif(btrim(seg.value->>'ref_num'),'') ref_num,
      case when pg_input_is_valid(
          seg.value->>'start_utc','timestamp with time zone')
        then(seg.value->>'start_utc')::timestamptz end start_utc,
      case when pg_input_is_valid(
          seg.value->>'end_utc','timestamp with time zone')
        then(seg.value->>'end_utc')::timestamptz end end_utc,
      lower(coalesce(seg.value->>'is_reversal','false')) in(
        'true','t','1','yes') is_reversal
    from source_rows s
    cross join lateral jsonb_array_elements(
      case when jsonb_typeof(s.invoice_breakdown_json->'segments')='array'
        then s.invoice_breakdown_json->'segments' else '[]'::jsonb end)
      seg(value)
    where s.basis::text in('NHSP_ADJUSTMENT','HEALTHROSTER_ADJUSTMENT')
  ),
  segment_lock_targets_pre as materialized (
    select coalesce(jsonb_agg(jsonb_build_object(
      'invoice_id',s.planned_invoice_id,
      'timesheet_id',s.timesheet_id,
      'segment_ids',to_jsonb(p.segment_ids),
      'expected_financial_revision',encode(digest(jsonb_build_object(
        'financial_id',s.id,
        'timesheet_version',s.timesheet_version,
        'updated_at',s.updated_at,
        'basis',s.basis,
        'invoice_breakdown_json',s.invoice_breakdown_json
      )::text,'sha256'),'hex'))
      order by s.planned_invoice_id,s.timesheet_id),'[]'::jsonb) targets
    from source_rows s
    join selected_segments p
      on p.chunk_id=s.chunk_id and p.timesheet_id=s.timesheet_id
    where cardinality(coalesce(p.segment_ids,array[]::text[]))>0
      and exists(
        select 1
        from public.invoices existing_header
        where existing_header.id=s.planned_invoice_id)
  ),
  segment_lock_authority_pre as materialized (
    select r.*
    from segment_lock_targets_pre t
    cross join lateral private._invoice_segment_lock_batch(t.targets,v_now) r
  ),
  segment_lock_failures_pre as materialized (
    select * from segment_lock_authority_pre where not success
  ),
  segment_entries as materialized (
    select s.*,seg.ordinality,
      coalesce(nullif(seg.value->>'segment_id',''),
        left(encode(digest(seg.value::text,'sha256'),'hex'),24)) segment_id,
      case when left(coalesce(seg.value->>'date',''),10)
        ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
        then left(seg.value->>'date',10) end work_date,
      case when coalesce(seg.value->>'hours_day','')~
        '^[+-]?[0-9]+([.][0-9]+)?$' then(seg.value->>'hours_day')::numeric
        else 0 end segment_hours_day,
      case when coalesce(seg.value->>'hours_night','')~
        '^[+-]?[0-9]+([.][0-9]+)?$' then(seg.value->>'hours_night')::numeric
        else 0 end segment_hours_night,
      case when coalesce(seg.value->>'hours_sat','')~
        '^[+-]?[0-9]+([.][0-9]+)?$' then(seg.value->>'hours_sat')::numeric
        else 0 end segment_hours_sat,
      case when coalesce(seg.value->>'hours_sun','')~
        '^[+-]?[0-9]+([.][0-9]+)?$' then(seg.value->>'hours_sun')::numeric
        else 0 end segment_hours_sun,
      case when coalesce(seg.value->>'hours_bh','')~
        '^[+-]?[0-9]+([.][0-9]+)?$' then(seg.value->>'hours_bh')::numeric
        else 0 end segment_hours_bh,
      case
        when coalesce(seg.value->>'pay_amount','')~
          '^[+-]?[0-9]+([.][0-9]+)?$' then(seg.value->>'pay_amount')::numeric
        when coalesce(seg.value->>'pay_ex_vat','')~
          '^[+-]?[0-9]+([.][0-9]+)?$' then(seg.value->>'pay_ex_vat')::numeric
        else 0 end segment_pay_ex,
      case
        when coalesce(seg.value->>'charge_amount','')~
          '^[+-]?[0-9]+([.][0-9]+)?$' then(seg.value->>'charge_amount')::numeric
        when coalesce(seg.value->>'charge_ex_vat','')~
          '^[+-]?[0-9]+([.][0-9]+)?$' then(seg.value->>'charge_ex_vat')::numeric
        else 0 end segment_charge_ex
    from source_rows s
    cross join lateral jsonb_array_elements(
      case when jsonb_typeof(s.invoice_breakdown_json->'segments')='array'
        then s.invoice_breakdown_json->'segments' else '[]'::jsonb end)
      with ordinality seg(value,ordinality)
    where upper(coalesce(s.invoice_breakdown_json->>'mode',''))='SEGMENTS'
      and nullif(seg.value->>'invoice_locked_invoice_id','') is null
      and exists(
        select 1 from selected_segments picked
        where picked.chunk_id=s.chunk_id
          and picked.timesheet_id=s.timesheet_id
          and coalesce(nullif(seg.value->>'segment_id',''),
            left(encode(digest(seg.value::text,'sha256'),'hex'),24))
            =any(coalesce(picked.segment_ids,array[]::text[])))
  ),
  segment_daily_lines as materialized (
    select s.chunk_id,s.planned_invoice_id,s.timesheet_id,s.booking_id,
      s.candidate_display,s.week_ending_date,s.work_date,
      sum(s.segment_hours_day) h_day,sum(s.segment_hours_night) h_night,
      sum(s.segment_hours_sat) h_sat,sum(s.segment_hours_sun) h_sun,
      sum(s.segment_hours_bh) h_bh,sum(s.segment_pay_ex) pay_ex,
      sum(s.segment_charge_ex) charge_ex,max(s.vat_rate) vat_rate,
      max(s.charge_day) charge_day,max(s.charge_night) charge_night,
      max(s.charge_sat) charge_sat,max(s.charge_sun) charge_sun,
      max(s.charge_bh) charge_bh,
      max(s.timesheet_version) timesheet_version,
      (array_agg(s.id order by s.id))[1] tsfin_id,
      max(s.contract_role) contract_role,
      max(s.contract_display_site) contract_display_site,
      max(s.contract_ward_hint) contract_ward_hint,
      (array_agg(s.bucket_labels_json order by s.ordinality))[1]
        bucket_labels_json
    from segment_entries s
    where s.daily_calc_of_invoices and s.work_date is not null
    group by s.chunk_id,s.planned_invoice_id,s.timesheet_id,s.booking_id,
      s.candidate_display,s.week_ending_date,s.work_date
  ),
  segment_weekly_lines as materialized (
    select s.chunk_id,s.planned_invoice_id,s.timesheet_id,s.booking_id,
      s.candidate_display,s.week_ending_date,
      sum(s.segment_hours_day) h_day,sum(s.segment_hours_night) h_night,
      sum(s.segment_hours_sat) h_sat,sum(s.segment_hours_sun) h_sun,
      sum(s.segment_hours_bh) h_bh,sum(s.segment_pay_ex) pay_ex,
      sum(s.segment_charge_ex) charge_ex,max(s.vat_rate) vat_rate,
      max(s.charge_day) charge_day,max(s.charge_night) charge_night,
      max(s.charge_sat) charge_sat,max(s.charge_sun) charge_sun,
      max(s.charge_bh) charge_bh,
      max(s.timesheet_version) timesheet_version,
      (array_agg(s.id order by s.id))[1] tsfin_id,
      max(s.contract_role) contract_role,
      max(s.contract_display_site) contract_display_site,
      max(s.contract_ward_hint) contract_ward_hint,
      (array_agg(s.bucket_labels_json order by s.ordinality))[1]
        bucket_labels_json
    from segment_entries s
    where not s.daily_calc_of_invoices
       or not exists(select 1 from segment_entries d
         where d.chunk_id=s.chunk_id and d.timesheet_id=s.timesheet_id
           and d.daily_calc_of_invoices and d.work_date is not null)
    group by s.chunk_id,s.planned_invoice_id,s.timesheet_id,s.booking_id,
      s.candidate_display,s.week_ending_date
  ),
  nonsegment_lines as materialized (
    select s.chunk_id,s.planned_invoice_id,s.timesheet_id,s.booking_id,s.candidate_display,s.week_ending_date,
      coalesce(s.hours_day,0) h_day,coalesce(s.hours_night,0) h_night,coalesce(s.hours_sat,0) h_sat,
      coalesce(s.hours_sun,0) h_sun,coalesce(s.hours_bh,0) h_bh,
      round(coalesce(s.total_pay_ex_vat,0)-coalesce(s.additional_pay_ex_vat,0)
        -coalesce(s.expenses_pay_ex_vat,0)-coalesce(s.mileage_pay_ex_vat,0),2) pay_ex,
      round(coalesce(s.total_charge_ex_vat,0)-coalesce(s.additional_charge_ex_vat,0)
        -coalesce(s.expenses_charge_ex_vat,0)-coalesce(s.mileage_charge_ex_vat,0),2) charge_ex,
      s.vat_rate,s.charge_day,s.charge_night,s.charge_sat,s.charge_sun,
      s.charge_bh,s.timesheet_version,s.id tsfin_id,s.contract_role,
      s.contract_display_site,s.contract_ward_hint,s.bucket_labels_json
    from source_rows s
    where upper(coalesce(s.invoice_breakdown_json->>'mode',''))<>'SEGMENTS'
       or jsonb_array_length(case when jsonb_typeof(s.invoice_breakdown_json->'segments')='array'
                                  then s.invoice_breakdown_json->'segments' else '[]'::jsonb end)=0
  ),
  additional_daily_lines as materialized (
    select s.chunk_id,s.planned_invoice_id,s.timesheet_id,s.booking_id,
      s.candidate_display,s.week_ending_date,upper(a.key) code,
      left(d.key,10) work_date,
      case when pg_input_is_valid(d.value,'numeric')
        then d.value::numeric else 0 end units,
      case when pg_input_is_valid(a.value->>'pay_rate','numeric')
        then(a.value->>'pay_rate')::numeric else 0 end pay_rate,
      case when pg_input_is_valid(a.value->>'charge_rate','numeric')
        then(a.value->>'charge_rate')::numeric else 0 end charge_rate,
      round((case when pg_input_is_valid(d.value,'numeric')
          then d.value::numeric else 0 end)*
        (case when pg_input_is_valid(a.value->>'pay_rate','numeric')
          then(a.value->>'pay_rate')::numeric else 0 end),2) pay_ex,
      round((case when pg_input_is_valid(d.value,'numeric')
          then d.value::numeric else 0 end)*
        (case when pg_input_is_valid(a.value->>'charge_rate','numeric')
          then(a.value->>'charge_rate')::numeric else 0 end),2) charge_ex,
      coalesce(nullif(a.value->>'bucket_name',''),a.key) bucket_name,
      coalesce(nullif(a.value->>'unit_name',''),'units') unit_name,
      a.value->'frequency' frequency,s.vat_rate,
      s.charge_day,s.charge_night,s.charge_sat,s.charge_sun,s.charge_bh,
      s.timesheet_version,s.id tsfin_id,s.contract_role,
      s.contract_display_site,s.contract_ward_hint,s.bucket_labels_json
    from source_rows s
    cross join lateral jsonb_each(
      case when jsonb_typeof(s.additional_units_json)='object'
        then s.additional_units_json else '{}'::jsonb end) a
    cross join lateral jsonb_each_text(
      case when jsonb_typeof(a.value->'days')='object'
        then a.value->'days' else '{}'::jsonb end) d
    where s.daily_calc_of_invoices
      and left(d.key,10) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
      and exists(select 1 from segment_entries se
        where se.chunk_id=s.chunk_id and se.timesheet_id=s.timesheet_id
          and se.work_date is not null)
  ),
  additional_weekly_lines as materialized (
    select s.chunk_id,s.planned_invoice_id,s.timesheet_id,s.booking_id,s.candidate_display,s.week_ending_date,
      upper(a.key) code,
      case when pg_input_is_valid(a.value->>'unit_count','numeric')
        then(a.value->>'unit_count')::numeric else 0 end units,
      case when pg_input_is_valid(a.value->>'pay_rate','numeric')
        then(a.value->>'pay_rate')::numeric else 0 end pay_rate,
      case when pg_input_is_valid(a.value->>'charge_rate','numeric')
        then(a.value->>'charge_rate')::numeric else 0 end charge_rate,
      case when pg_input_is_valid(a.value->>'pay_ex_vat','numeric')
        then(a.value->>'pay_ex_vat')::numeric else 0 end pay_ex,
      case when pg_input_is_valid(a.value->>'charge_ex_vat','numeric')
        then(a.value->>'charge_ex_vat')::numeric else 0 end charge_ex,
      coalesce(nullif(a.value->>'bucket_name',''),a.key) bucket_name,
      coalesce(nullif(a.value->>'unit_name',''),'units') unit_name,
      a.value->'frequency' frequency,s.vat_rate,
      s.charge_day,s.charge_night,s.charge_sat,s.charge_sun,s.charge_bh,
      s.timesheet_version,s.id tsfin_id,s.contract_role,
      s.contract_display_site,s.contract_ward_hint,s.bucket_labels_json
    from source_rows s cross join lateral jsonb_each(
      case when jsonb_typeof(s.additional_units_json)='object'
        then s.additional_units_json else '{}'::jsonb end) a
    where jsonb_typeof(a.value)='object'
      and ((case when pg_input_is_valid(a.value->>'pay_ex_vat','numeric')
              then(a.value->>'pay_ex_vat')::numeric else 0 end)<>0
        or (case when pg_input_is_valid(a.value->>'charge_ex_vat','numeric')
              then(a.value->>'charge_ex_vat')::numeric else 0 end)<>0)
      and not(s.daily_calc_of_invoices
        and jsonb_typeof(a.value->'days')='object'
        and exists(select 1 from jsonb_each_text(
          case when jsonb_typeof(a.value->'days')='object'
            then a.value->'days' else '{}'::jsonb end) d
          where left(d.key,10) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'))
  ),
  expense_lines as materialized (
    select s.chunk_id,s.planned_invoice_id,s.timesheet_id,s.booking_id,s.candidate_display,s.week_ending_date,
      e.code,e.pay_ex,e.charge_ex,s.vat_rate,s.expenses_description,
      s.expenses_evidence_r2_key,s.expenses_evidence_manifest,
      s.mileage_units,s.mileage_pay_rate,s.mileage_charge_rate,
      s.mileage_evidence_r2_key,s.mileage_evidence_manifest,
      s.charge_day,s.charge_night,s.charge_sat,s.charge_sun,s.charge_bh,
      s.timesheet_version,s.id tsfin_id,s.contract_role,
      s.contract_display_site,s.contract_ward_hint,s.bucket_labels_json
    from source_rows s
    cross join lateral (
      values
       ('TRAVEL',coalesce(s.travel_pay_ex_vat,0),coalesce(s.travel_charge_ex_vat,0)),
       ('ACCOMMODATION',coalesce(s.accommodation_pay_ex_vat,0),coalesce(s.accommodation_charge_ex_vat,0)),
       ('OTHER',coalesce(s.other_pay_ex_vat,0),coalesce(s.other_charge_ex_vat,0)),
       ('EXPENSES_FALLBACK',
          case when coalesce(s.travel_pay_ex_vat,0)+coalesce(s.accommodation_pay_ex_vat,0)+coalesce(s.other_pay_ex_vat,0)=0 then coalesce(s.expenses_pay_ex_vat,0) else 0 end,
          case when coalesce(s.travel_charge_ex_vat,0)+coalesce(s.accommodation_charge_ex_vat,0)+coalesce(s.other_charge_ex_vat,0)=0 then coalesce(s.expenses_charge_ex_vat,0) else 0 end),
       ('MILEAGE',coalesce(s.mileage_pay_ex_vat,0),coalesce(s.mileage_charge_ex_vat,0))
    ) e(code,pay_ex,charge_ex)
    where e.pay_ex<>0 or e.charge_ex<>0
  ),
  line_union as materialized (
    select chunk_id,planned_invoice_id,timesheet_id,booking_id,
      candidate_display||' - '||work_date description,h_day,h_night,h_sat,h_sun,h_bh,
      null::numeric pay_day,null::numeric pay_night,null::numeric pay_sat,
      null::numeric pay_sun,null::numeric pay_bh,
      charge_day,charge_night,charge_sat,charge_sun,charge_bh,
      pay_ex,charge_ex,vat_rate,'HOURS_DAILY' line_type,
      'TS:'||timesheet_id||':HOURS:'||work_date source_key,
      jsonb_build_object('date',work_date,'timesheet_version',timesheet_version,
        'tsfin_id',tsfin_id,'role',contract_role,'hospital',contract_display_site,
        'ward',contract_ward_hint,'bucket_labels',bucket_labels_json) detail
    from segment_daily_lines
    union all
    select chunk_id,planned_invoice_id,timesheet_id,booking_id,
      candidate_display||' - W/E '||week_ending_date, h_day,h_night,h_sat,h_sun,h_bh,
      null::numeric,null::numeric,null::numeric,null::numeric,null::numeric,
      charge_day,charge_night,charge_sat,charge_sun,charge_bh,
      pay_ex,charge_ex,vat_rate,'HOURS_WEEKLY','TS:'||timesheet_id||':HOURS:WEEK',
      jsonb_build_object('timesheet_version',timesheet_version,'tsfin_id',tsfin_id,
        'role',contract_role,'hospital',contract_display_site,
        'ward',contract_ward_hint,'bucket_labels',bucket_labels_json)
    from segment_weekly_lines
    union all
    select chunk_id,planned_invoice_id,timesheet_id,booking_id,
      candidate_display||' - W/E '||week_ending_date, h_day,h_night,h_sat,h_sun,h_bh,
      null::numeric,null::numeric,null::numeric,null::numeric,null::numeric,
      charge_day,charge_night,charge_sat,charge_sun,charge_bh,
      pay_ex,charge_ex,vat_rate,'HOURS_WEEKLY','TS:'||timesheet_id||':HOURS:WEEK',
      jsonb_build_object('timesheet_version',timesheet_version,'tsfin_id',tsfin_id,
        'role',contract_role,'hospital',contract_display_site,
        'ward',contract_ward_hint,'bucket_labels',bucket_labels_json)
    from nonsegment_lines
    where pay_ex<>0 or charge_ex<>0 or h_day+h_night+h_sat+h_sun+h_bh<>0
    union all
    select chunk_id,planned_invoice_id,timesheet_id,booking_id,
      candidate_display||' - '||bucket_name||' - '||work_date||' - '||
        units||' '||unit_name,0,0,0,0,0,
      null::numeric,null::numeric,null::numeric,null::numeric,null::numeric,
      charge_day,charge_night,charge_sat,charge_sun,charge_bh,
      pay_ex,charge_ex,vat_rate,'ADDITIONAL_RATE_DAILY',
      'TS:'||timesheet_id||':ADD:'||code||':'||work_date,
      jsonb_build_object('date',work_date,'timesheet_version',timesheet_version,
        'tsfin_id',tsfin_id,'role',contract_role,'hospital',contract_display_site,
        'ward',contract_ward_hint,'bucket_labels',bucket_labels_json,
        'bucket',jsonb_build_object('code',code,'bucket_name',bucket_name,
          'unit_name',unit_name,'frequency',frequency),
        'units',jsonb_build_object('unit_count',units,'pay_rate',pay_rate,
          'charge_rate',charge_rate))
    from additional_daily_lines
    union all
    select chunk_id,planned_invoice_id,timesheet_id,booking_id,
      candidate_display||' - '||bucket_name||' - '||units||' '||unit_name,0,0,0,0,0,
      null::numeric,null::numeric,null::numeric,null::numeric,null::numeric,
      charge_day,charge_night,charge_sat,charge_sun,charge_bh,
      pay_ex,charge_ex,vat_rate,'ADDITIONAL_RATE',
      'TS:'||timesheet_id||':ADD:'||code||':WEEK',
      jsonb_build_object('timesheet_version',timesheet_version,'tsfin_id',tsfin_id,
        'role',contract_role,'hospital',contract_display_site,
        'ward',contract_ward_hint,'bucket_labels',bucket_labels_json,
        'bucket',jsonb_build_object('code',code,'bucket_name',bucket_name,
          'unit_name',unit_name,'frequency',frequency),
        'units',jsonb_build_object('unit_count',units,'pay_rate',pay_rate,
          'charge_rate',charge_rate))
    from additional_weekly_lines
    union all
    select chunk_id,planned_invoice_id,timesheet_id,booking_id,
      case when code='MILEAGE' then 'Mileage - '||coalesce(mileage_units,0)||
          ' miles (W/E '||week_ending_date||')'
        else initcap(replace(code,'_',' '))||' (W/E '||week_ending_date||')' end,
      0,0,0,0,0,
      null::numeric,null::numeric,null::numeric,null::numeric,null::numeric,
      charge_day,charge_night,charge_sat,charge_sun,charge_bh,
      pay_ex,charge_ex,vat_rate,
      case when code='MILEAGE' then 'MILEAGE'
        when code='EXPENSES_FALLBACK' then 'EXPENSES_TOTAL'
        else 'EXPENSE_'||code end,
      case when code='MILEAGE' then 'TS:'||timesheet_id||':MILEAGE'
        when code='EXPENSES_FALLBACK' then 'TS:'||timesheet_id||':EXP:TOTAL'
        else 'TS:'||timesheet_id||':EXP:'||code end,
      jsonb_build_object('timesheet_version',timesheet_version,'tsfin_id',tsfin_id,
        'role',contract_role,'hospital',contract_display_site,
        'ward',contract_ward_hint,'bucket_labels',bucket_labels_json,
        'expense',case when code='MILEAGE' then jsonb_build_object(
          'category','MILEAGE','mileage_units',mileage_units,
          'pay_rate',mileage_pay_rate,'charge_rate',mileage_charge_rate,
          'evidence_r2_key',mileage_evidence_r2_key,
          'evidence_manifest',mileage_evidence_manifest)
        else jsonb_build_object('category',code,'note',expenses_description,
          'evidence_r2_key',expenses_evidence_r2_key,
          'evidence_manifest',expenses_evidence_manifest) end)
    from expense_lines
  ),
  correction_scopes_pre as materialized (
    select coalesce(jsonb_agg(jsonb_build_object(
      'request_key','generation-prewrite:'||scope.planned_invoice_id::text,
      'scope_key',scope.planned_invoice_id::text,
      'invoice_id',case when exists(select 1 from public.invoices existing
        where existing.id=scope.planned_invoice_id)
        then scope.planned_invoice_id end,
      'validation_purpose','GENERATION_PREWRITE',
      'expected_client_id',scope.payload_json->>'client_id',
      'target_invoice_week',scope.payload_json->>'target_invoice_week',
      'expected_invoice_stream',scope.payload_json->>'invoice_stream',
      'planned_members',scope.planned_members)
      order by scope.planned_invoice_id),'[]'::jsonb) scopes
    from(
      select l.planned_invoice_id,min(vc.payload_json::text)::jsonb payload_json,
        jsonb_agg(distinct jsonb_build_object(
          'timesheet_id',l.timesheet_id,
          'vat_rate_pct',l.vat_rate))
          filter(where l.timesheet_id is not null) planned_members
      from line_union l
      join valid_chunks vc on vc.id=l.chunk_id
      group by l.planned_invoice_id
    ) scope
  ),
  correction_validation_pre as materialized (
    select r.*
    from correction_scopes_pre s
    cross join lateral private._invoice_correction_validate_batch(
      s.scopes,(v_now at time zone 'Europe/London')::date) r
  ),
  correction_failures_pre as materialized (
    select scope_key,blocker_code,detail_json detail
    from correction_validation_pre
    where not valid
  ),  write_eligible_chunks as materialized (
    select vc.*
    from valid_chunks vc
    where not exists(
      select 1 from correction_failures_pre f
      where f.scope_key=vc.planned_invoice_id::text)
  ),
  line_totals as materialized (
    select l.planned_invoice_id,
      round(sum(round(l.charge_ex,2)),2) subtotal_ex_vat,
      round(sum(round(l.charge_ex*l.vat_rate/100,2)),2) vat_amount,
      round(sum(round(l.charge_ex+(l.charge_ex*l.vat_rate/100),2)),2) total_inc_vat
    from line_union l
    join write_eligible_chunks w on w.id=l.chunk_id
    group by l.planned_invoice_id
  ),
  existing_target_headers as materialized (
    select vc.id chunk_id,i.id invoice_id
    from write_eligible_chunks vc
    join header_source h on h.chunk_id=vc.id
    join public.invoices i on i.id=vc.planned_invoice_id
      and i.client_id=h.client_id and i.type='INVOICE' and i.status='DRAFT'
    where not exists(
      select 1 from segment_lock_failures_pre f
      where f.invoice_id=vc.planned_invoice_id)
  ),
  inserted_headers as (
    insert into public.invoices(id,client_id,status,status_date_utc,subtotal_ex_vat,vat_amount,total_inc_vat,
      header_snapshot_json,document_revision,document_state,created_at,updated_at)
    select h.planned_invoice_id,h.client_id,'DRAFT',v_now,
      t.subtotal_ex_vat,t.vat_amount,t.total_inc_vat,
      jsonb_build_object('client_id',h.client_id,'client_name',h.client_name)
      ||case when private._candidate_feature_enabled_current_v1('candidate_expense_invoice_routing_v1')
        then jsonb_build_object('invoice_stream',upper(coalesce(vc.payload_json->>'invoice_stream','NORMAL')))
        else '{}'::jsonb end
      ||jsonb_build_object(
        'client_invoice_address',h.client_invoice_address,'client_primary_invoice_email',h.primary_invoice_email,
        'agency_name',h.agency_name,'agency_logo',h.agency_logo,'registered_address',h.registered_address,
        'company_reg_number',h.company_reg_number,'company_registration_number',h.company_reg_number,
        'vat_chargeable',h.vat_chargeable,'payment_terms_days',h.payment_terms_days,
        'issued_at_utc',null,'due_at_utc',null,
        'totals',jsonb_build_object(
          'subtotal_ex_vat',t.subtotal_ex_vat,
          'vat_amount',t.vat_amount,'total_inc_vat',t.total_inc_vat),
        'bank',jsonb_build_object('name',h.bank_name,'sort_code',h.bank_sort_code,'account_number',h.bank_account_number),
        'vat_registration_number',h.vat_registration_number,
        'meta',jsonb_build_object('source','INVOICE_OPERATION_QUEUE',
          'invoice_week_start',vc.payload_json->>'target_invoice_week',
          'consolidation_mode',vc.payload_json->>'consolidation_mode',
          'self_bill',vc.payload_json->>'invoice_stream'='SELF_BILL','timesheet_count',h.source_count)
          ||case when private._candidate_feature_enabled_current_v1('candidate_expense_invoice_routing_v1')
            then jsonb_build_object('invoice_stream',upper(coalesce(vc.payload_json->>'invoice_stream','NORMAL')))
            else '{}'::jsonb end,
        'attach_policy',vc.payload_json#>'{plan,settings_snapshot,attach_policy}'),
      1,'STALE',v_now,v_now
    from header_source h join write_eligible_chunks vc on vc.id=h.chunk_id
    join line_totals t on t.planned_invoice_id=h.planned_invoice_id
    where not exists(
      select 1 from segment_lock_failures_pre f
      where f.invoice_id=h.planned_invoice_id)
    on conflict(id) do nothing returning id
  ),
  target_headers as materialized (
    select e.chunk_id,e.invoice_id
    from existing_target_headers e
    where not exists(
      select 1
      from inserted_headers h
      where h.id=e.invoice_id)
  ),
  deferred_new_headers as (
    update public.invoice_operation_chunks c
    set status='QUEUED',
      phase='COMMIT',
      progress_json=coalesce(c.progress_json,'{}'::jsonb)
        ||jsonb_build_object('status_message',
          'Invoice header created; applying authoritative source ownership'),
      error_json=null,
      lease_owner=null,
      lease_token=null,
      lease_expires_at_utc=null,
      updated_at_utc=v_now
    from inserted_headers h
    join write_eligible_chunks vc on vc.planned_invoice_id=h.id
    where c.id=vc.id
    returning c.id
  ),
  inserted_lines as (
    insert into public.invoice_lines(
      invoice_id,timesheet_id,booking_id,description,hours_day,hours_night,hours_sat,hours_sun,hours_bh,
      pay_day,pay_night,pay_sat,pay_sun,pay_bh,
      charge_day,charge_night,charge_sat,charge_sun,charge_bh,
      total_pay_ex_vat,total_charge_ex_vat,margin_ex_vat,vat_rate_pct,vat_amount,total_inc_vat,
      paper_ts_r2_key,meta_json,source_key
    )
    select l.planned_invoice_id,l.timesheet_id,l.booking_id,l.description,
      round(l.h_day,2),round(l.h_night,2),round(l.h_sat,2),round(l.h_sun,2),round(l.h_bh,2),
      l.pay_day,l.pay_night,l.pay_sat,l.pay_sun,l.pay_bh,
      l.charge_day,l.charge_night,l.charge_sat,l.charge_sun,l.charge_bh,
      round(l.pay_ex,2),round(l.charge_ex,2),round(l.charge_ex-l.pay_ex,2),l.vat_rate,
      round(l.charge_ex*l.vat_rate/100,2),round(l.charge_ex+(l.charge_ex*l.vat_rate/100),2),
      null,jsonb_build_object('line_type',l.line_type,'timesheet_id',l.timesheet_id,
        'candidate_display',l.description,'week_ending_date',
        (select s.week_ending_date from source_rows s where s.timesheet_id=l.timesheet_id limit 1),
        'schedule_references',
        (select s.schedule_refs from source_rows s where s.timesheet_id=l.timesheet_id limit 1),
        'totals',jsonb_build_object('line_pay_ex_vat',round(l.pay_ex,2),
          'line_charge_ex_vat',round(l.charge_ex,2),'margin_ex_vat',round(l.charge_ex-l.pay_ex,2),
          'vat_rate_pct',l.vat_rate,'vat_amount',round(l.charge_ex*l.vat_rate/100,2),
          'total_inc_vat',round(l.charge_ex+(l.charge_ex*l.vat_rate/100),2)))||l.detail,l.source_key
    from line_union l join target_headers h on h.invoice_id=l.planned_invoice_id
    on conflict(invoice_id,source_key) do nothing
    returning invoice_id,timesheet_id,total_charge_ex_vat,vat_amount,total_inc_vat
  ),
  all_target_lines as materialized (
    select l.invoice_id,l.total_charge_ex_vat,l.vat_amount,l.total_inc_vat
    from public.invoice_lines l
    where l.invoice_id in(select invoice_id from target_headers)
    union all
    select l.invoice_id,l.total_charge_ex_vat,l.vat_amount,l.total_inc_vat
    from inserted_lines l
  ),
  all_line_totals as materialized (
    select l.invoice_id,count(*)::integer line_count,
      round(sum(l.total_charge_ex_vat),2) subtotal_ex_vat,
      round(sum(l.vat_amount),2) vat_amount,
      round(sum(l.total_inc_vat),2) total_inc_vat
    from all_target_lines l
    group by l.invoice_id
  ),
  target_header_streams as materialized (
    select h.invoice_id,
      max(upper(coalesce(c.payload_json->>'invoice_stream','NORMAL'))) invoice_stream
    from target_headers h
    join public.invoice_operation_chunks c on c.id=h.chunk_id
    group by h.invoice_id
  ),
  updated_header_totals as (
    update public.invoices i
    set subtotal_ex_vat=t.subtotal_ex_vat,vat_amount=t.vat_amount,
      total_inc_vat=t.total_inc_vat,document_state='STALE',
      header_snapshot_json=coalesce(i.header_snapshot_json,'{}'::jsonb)
        ||jsonb_build_object('totals',jsonb_build_object(
          'subtotal_ex_vat',t.subtotal_ex_vat,'vat_amount',t.vat_amount,
          'total_inc_vat',t.total_inc_vat))
        ||case when private._candidate_feature_enabled_current_v1('candidate_expense_invoice_routing_v1')
          then jsonb_build_object(
            'invoice_stream',s.invoice_stream,
            'meta',coalesce(i.header_snapshot_json->'meta','{}'::jsonb)
              ||jsonb_build_object('invoice_stream',s.invoice_stream))
          else '{}'::jsonb end,
      updated_at=v_now
    from all_line_totals t
    join target_header_streams s on s.invoice_id=t.invoice_id
    where i.id=t.invoice_id
    returning i.id
  ),
  segment_lock_targets as materialized (
    select targets from segment_lock_targets_pre
  ),
  segment_lock_authority as materialized (
    select * from segment_lock_authority_pre
  ),
  segment_lock_failures as materialized (
    select * from segment_lock_authority where not success
  ),
  whole_lock as (
    update public.timesheets_financials tf
    set locked_by_invoice_id=vc.planned_invoice_id,locked_at_utc=v_now,updated_at=v_now
    from members m join write_eligible_chunks vc on vc.id=m.chunk_id
    join target_headers h
      on h.chunk_id=vc.id and h.invoice_id=vc.planned_invoice_id
    cross join (select count(*) applied_count from segment_lock_authority) segment_application
    where tf.timesheet_id=m.timesheet_id and tf.is_current
      and (coalesce(tf.invoice_breakdown_json->>'mode','')<>'SEGMENTS'
        or not exists(select 1 from jsonb_array_elements(coalesce(tf.invoice_breakdown_json->'segments','[]'::jsonb)) e
                      where nullif(e->>'invoice_locked_invoice_id','') is null))
    returning tf.timesheet_id
  ),
  week_lock as (
    update public.contract_weeks cw set status='INVOICED',updated_at=v_now
    where cw.timesheet_id in(
      select m.timesheet_id
      from members m
      join target_headers h on h.chunk_id=m.chunk_id)
      and exists(select 1 from public.timesheets_financials tf where tf.timesheet_id=cw.timesheet_id
        and tf.is_current and (tf.locked_by_invoice_id is not null or
          not exists(select 1 from jsonb_array_elements(coalesce(tf.invoice_breakdown_json->'segments','[]'::jsonb)) e
                     where nullif(e->>'invoice_locked_invoice_id','') is null)))
    returning cw.id
  ),
  nhsp_shift_inclusion as (
    update public.nhsp_shifts ns
    set invoice_status='INCLUDED',
      invoice_id=h.invoice_id,
      updated_at=v_now
    from source_rows s
    join target_headers h
      on h.chunk_id=s.chunk_id and h.invoice_id=s.planned_invoice_id
    where ns.timesheet_id=s.timesheet_id
      and ns.invoice_status='PENDING'
      and ns.invoice_id is null
      and ns.cancelled_at_utc is null
      and exists(
        select 1
        from jsonb_array_elements(
          case when jsonb_typeof(s.invoice_breakdown_json->'segments')='array'
            then s.invoice_breakdown_json->'segments'
            else '[]'::jsonb end) segment(value)
        where coalesce(
            nullif(segment.value->>'nhsp_shift_id',''),
            nullif(segment.value->>'shift_id',''))=ns.id::text)
    returning ns.id
  ),
  hr_sources as (
    insert into public.invoice_hr_source_rows(invoice_id,source_system,import_id,header_columns,rows_json,header_rows)
    select distinct h.invoice_id,
      case when tf.basis::text in('NHSP','NHSP_ADJUSTMENT')
        then 'NHSP' else 'HEALTHROSTER' end,
      tf.nhsp_import_id,'[]'::jsonb,
      case when jsonb_typeof(tf.external_source_rows_json)='array' then tf.external_source_rows_json else '[]'::jsonb end,
      '[]'::jsonb
    from members m
    join target_headers h on h.chunk_id=m.chunk_id
    join public.timesheets_financials tf on tf.timesheet_id=m.timesheet_id and tf.is_current
    where tf.nhsp_import_id is not null
    on conflict(invoice_id,source_system,import_id) do update set rows_json=excluded.rows_json
    returning invoice_id
  ),
  source_segments as materialized (
    select distinct h.invoice_id planned_invoice_id,
      coalesce(
        case when coalesce(x.value->>'nhsp_shift_id','')~*
            '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          then(x.value->>'nhsp_shift_id')::uuid end,
        case when coalesce(x.value->>'shift_id','')~*
            '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          then(x.value->>'shift_id')::uuid end,
        case when substr(coalesce(x.value->>'segment_id',''),6)~*
            '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          then substr(x.value->>'segment_id',6)::uuid end) shift_id,
      case
        when s.basis::text in('NHSP_ADJUSTMENT','HEALTHROSTER_ADJUSTMENT')
         and (
           lower(coalesce(x.value->>'is_reversal','false')) in(
             'true','t','1','yes')
           or coalesce(s.total_hours,0)<0
         ) then 'REVERSAL'
        when s.basis::text in('NHSP_ADJUSTMENT','HEALTHROSTER_ADJUSTMENT')
          then 'CORRECTED_HOURS'
        else 'SOURCE'
      end evidence_role_key
    from source_rows s
    join target_headers h
      on h.chunk_id=s.chunk_id and h.invoice_id=s.planned_invoice_id
    cross join lateral jsonb_array_elements(
      case when jsonb_typeof(s.invoice_breakdown_json->'segments')='array'
        then s.invoice_breakdown_json->'segments' else '[]'::jsonb end) x(value)
    where coalesce(
        nullif(x.value->>'nhsp_shift_id',''),
        nullif(x.value->>'shift_id',''),
        case when left(x.value->>'segment_id',5)='nhsp:'
          then substr(x.value->>'segment_id',6) end)~*
      '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      and (
        upper(coalesce(x.value->>'source_system',''))='NHSP'
        or (upper(coalesce(x.value->>'source_system',''))='HEALTHROSTER'
          and coalesce((s.payload_json#>>'{plan,settings_snapshot,attach_policy,hr_attach_to_invoice}')::boolean,true)))
    union
    select distinct r.planned_invoice_id,n.id shift_id,
      case when r.is_reversal then 'REVERSAL'
        else 'CORRECTED_HOURS' end evidence_role_key
    from adjustment_segment_refs r
    join source_timesheet_ancestry a
      on a.planned_invoice_id=r.planned_invoice_id
     and a.root_timesheet_id=r.root_timesheet_id
     and a.ancestry_depth>0
    join public.nhsp_shifts n
      on n.timesheet_id=a.source_timesheet_id
     and n.latest_import_id is not null
     and n.external_row_key is not null
     and (
       upper(coalesce(n.source_system::text,''))='NHSP'
       or (
         upper(coalesce(n.source_system::text,''))='HEALTHROSTER'
         and r.healthroster_attach_allowed
       )
     )
     and (
       (r.ref_num is not null
         and btrim(coalesce(n.ref_num,''))=r.ref_num)
       or (
         r.ref_num is null
         and r.start_utc is not null
         and r.end_utc is not null
         and n.start_utc=r.start_utc
         and n.end_utc=r.end_utc
       )
     )
  ),
  source_import_rows as materialized (
    select distinct s.planned_invoice_id,
      upper(coalesce(n.source_system::text,'UNKNOWN')) source_system,
      n.latest_import_id import_id,n.external_row_key,s.evidence_role_key
    from source_segments s
    join public.nhsp_shifts n on n.id=s.shift_id
    where n.latest_import_id is not null
      and n.external_row_key is not null
  ),
  source_imports as materialized (
    select s.planned_invoice_id,s.source_system,s.import_id,
      jsonb_agg(jsonb_build_object(
        'external_row_key',s.external_row_key,
        'evidence_role_key',s.evidence_role_key)
        order by s.external_row_key,s.evidence_role_key) row_occurrences
    from source_import_rows s
    group by s.planned_invoice_id,s.source_system,s.import_id
  ),
  authoritative_hr_sources as (
    insert into public.invoice_hr_source_rows(
      invoice_id,source_system,import_id,header_columns,rows_json)
    select g.planned_invoice_id,g.source_system,g.import_id,
      case when jsonb_typeof(i.parse_summary_json->'header_columns')='array'
        then i.parse_summary_json->'header_columns' else '[]'::jsonb end,
      coalesce((select jsonb_agg(
          r.payload_json
            ||jsonb_build_object(
              'evidence_role',case occurrence.value->>'evidence_role_key'
                when 'REVERSAL' then case when g.source_system='NHSP'
                  then 'NHSP Reversal' else 'HealthRoster Reversal' end
                when 'CORRECTED_HOURS' then case when g.source_system='NHSP'
                  then 'NHSP Corrected Hours' else 'HealthRoster Corrected Hours' end
                else case when g.source_system='NHSP'
                  then 'NHSP Shift' else 'HealthRoster Shift' end
              end)
            ||case when occurrence.value->>'evidence_role_key'='REVERSAL'
              then jsonb_build_object('reversal_state','REVERSED')
              else '{}'::jsonb end
          order by r.id,occurrence.value->>'evidence_role_key')
        from jsonb_array_elements(g.row_occurrences) occurrence(value)
        join public.hr_rows r
          on r.import_id=g.import_id
         and r.external_row_key=occurrence.value->>'external_row_key'),
        '[]'::jsonb)
    from source_imports g join public.hr_imports i on i.id=g.import_id
    on conflict(invoice_id,source_system,import_id) do update
      set header_columns=excluded.header_columns,rows_json=excluded.rows_json
    returning invoice_id
  ),
  audits as (
    insert into public.audit_events(ts_utc,actor_user_id,actor_display,actor_role_at_time,
      object_type,object_id_text,action,after_json,reason)
    select v_now,o.actor_user_id,u.display_name,u.role,'invoices',vc.planned_invoice_id::text,
      case when exists(select 1 from inserted_headers h
        where h.id=vc.planned_invoice_id)
        then 'INVOICE_CREATED' else 'INVOICE_DRAFT_APPENDED' end,
      jsonb_build_object('invoice_id',vc.planned_invoice_id,
        'source_ids',coalesce(vc.payload_json->'canonical_source_ids',
          vc.payload_json->'source_ids','[]'::jsonb),
        'source_revision',vc.payload_json->>'source_revision',
        'operation_id',vc.operation_id),'INVOICE_OPERATION_QUEUE'
    from write_eligible_chunks vc
    join target_headers h
      on h.chunk_id=vc.id and h.invoice_id=vc.planned_invoice_id
    join public.invoice_operations o on o.id=vc.operation_id
    left join public.tms_users u on u.id=o.actor_user_id
    returning id
  ),
  correction_failures as materialized (
    select scope_key::uuid invoice_id,blocker_code,detail
    from correction_failures_pre
    where scope_key~*
      '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  )
  update public.invoice_operation_chunks c
    set status=case when exists(select 1 from all_line_totals l
                               where l.invoice_id=vc.planned_invoice_id)
          and not exists(select 1 from correction_failures cf
            where cf.invoice_id=vc.planned_invoice_id)
          and not exists(select 1 from segment_lock_failures sf
            where sf.invoice_id=vc.planned_invoice_id)
        then 'COMPLETE' else 'BLOCKED' end,
      phase=case when exists(select 1 from all_line_totals l
                            where l.invoice_id=vc.planned_invoice_id)
          and not exists(select 1 from correction_failures cf
            where cf.invoice_id=vc.planned_invoice_id)
          and not exists(select 1 from segment_lock_failures sf
            where sf.invoice_id=vc.planned_invoice_id)
        then 'COMPLETE' else 'BLOCKED' end,
      result_json=jsonb_build_object('invoice_ids',jsonb_build_array(vc.planned_invoice_id),
        'source_revision',vc.payload_json->>'source_revision'),
      error_json=case
        when not exists(select 1 from all_line_totals l
          where l.invoice_id=vc.planned_invoice_id)
          then jsonb_build_object('code','NO_INVOICE_LINES_CREATED')
        when exists(select 1 from segment_lock_failures sf
          where sf.invoice_id=vc.planned_invoice_id)
          then jsonb_build_object('code','SEGMENT_LOCK_FAILED','detail',(
            select jsonb_agg(to_jsonb(sf))
            from segment_lock_failures sf
            where sf.invoice_id=vc.planned_invoice_id))
        when exists(select 1 from correction_failures cf
          where cf.invoice_id=vc.planned_invoice_id)
          then jsonb_build_object('code',(
            select cf.blocker_code from correction_failures cf
            where cf.invoice_id=vc.planned_invoice_id))
        else null end,
      completed_at_utc=case
        when exists(select 1 from all_line_totals l
          where l.invoice_id=vc.planned_invoice_id)
          and not exists(select 1 from correction_failures cf
            where cf.invoice_id=vc.planned_invoice_id)
          and not exists(select 1 from segment_lock_failures sf
            where sf.invoice_id=vc.planned_invoice_id)
        then v_now else null end,
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,
      updated_at_utc=v_now
    from valid_chunks vc
    join target_headers h
      on h.chunk_id=vc.id and h.invoice_id=vc.planned_invoice_id
    cross join (select count(*) applied_count from segment_lock_authority) segment_application
    cross join (select count(*) cached_count from authoritative_hr_sources) source_cache_application
    where c.id=vc.id
  ;
  select coalesce(jsonb_agg(jsonb_build_object(
      'chunk_id',c.id,'status',c.status,'phase',c.phase,
      'result',c.result_json,'error',c.error_json)),'[]'::jsonb)
    into v_part
  from jsonb_array_elements(p_claims) x
  join public.invoice_operation_chunks c
    on c.id=case when coalesce(x->>'chunk_id','')~*
      '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      then(x->>'chunk_id')::uuid end
  where x->>'phase'='COMMIT'
    and coalesce(c.payload_json->>'command_type','')<>'GENERATE_CREDIT_NOTE';
  v_result:=v_result||coalesce(v_part,'[]'::jsonb);

  -- Credit-note COMMIT: exact negative line clone and source unlock; document creation stays asynchronous.
  execute $q$
  with claim_ids as materialized (
    select (x->>'chunk_id')::uuid chunk_id
    from jsonb_array_elements($1) x where x->>'phase'='COMMIT'
  ),
  credit_revisions as materialized (
    select c.*,i.id source_invoice_id,(c.payload_json#>>'{plan,planned_invoice_id}')::uuid credit_note_id
      ,encode(digest(concat_ws('|',i.id::text,i.updated_at::text,i.status::text,
        i.subtotal_ex_vat::text,i.vat_amount::text,i.total_inc_vat::text),
        'sha256'),'hex') current_revision
    from claim_ids q join public.invoice_operation_chunks c on c.id=q.chunk_id
    join public.invoices i on i.id=c.entity_id
    where c.payload_json->>'command_type'='GENERATE_CREDIT_NOTE'
      and i.type='INVOICE' and i.status in ('ISSUED','PAID')
  ),
  credit_correction_scopes as materialized (
    select coalesce(jsonb_agg(jsonb_build_object(
      'request_key','credit-source:'||c.source_invoice_id::text,
      'scope_key',c.source_invoice_id::text,
      'invoice_id',c.source_invoice_id,
      'validation_purpose','CREDIT_SOURCE')
      order by c.source_invoice_id),'[]'::jsonb) scopes
    from(select distinct source_invoice_id from credit_revisions) c
  ),
  credit_correction_failures as materialized (
    select r.invoice_id source_invoice_id,r.blocker_code,r.detail_json
    from credit_correction_scopes s
    cross join lateral private._invoice_correction_validate_batch(
      s.scopes,($2 at time zone 'Europe/London')::date) r
    where not r.valid
  ),
  credit_rejected as (
    update public.invoice_operation_chunks q
    set status='SUPERSEDED',phase='SUPERSEDED',
      error_json=jsonb_build_object('code',case
        when c.current_revision<>c.payload_json->>'source_revision'
          then 'CREDIT_SOURCE_CHANGED_BEFORE_COMMIT'
        else coalesce((select f.blocker_code
          from credit_correction_failures f
          where f.source_invoice_id=c.source_invoice_id
          order by f.blocker_code limit 1),'CREDIT_CORRECTION_VALIDATION_FAILED')
        end),
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,
      updated_at_utc=$2
    from credit_revisions c
    where q.id=c.id and(
      c.current_revision<>c.payload_json->>'source_revision'
      or exists(select 1 from credit_correction_failures f
        where f.source_invoice_id=c.source_invoice_id))
    returning q.id
  ),
  credits as materialized (
    select c.* from credit_revisions c
    where c.current_revision=c.payload_json->>'source_revision'
      and not exists(select 1 from credit_rejected r where r.id=c.id)
  ),
  inserted_credit as (
    insert into public.invoices(id,type,client_id,status,status_date_utc,subtotal_ex_vat,vat_amount,total_inc_vat,
      original_invoice_id,notes,header_snapshot_json,document_revision,document_state,created_at,updated_at)
    select c.credit_note_id,'CREDIT_NOTE',i.client_id,'DRAFT',$2,
      -i.subtotal_ex_vat,-i.vat_amount,-i.total_inc_vat,i.id,
      c.payload_json->>'credit_reason',
      i.header_snapshot_json||jsonb_build_object(
        'credit_source_invoice_id',i.id,
        'credit_reason',c.payload_json->>'credit_reason'),1,'STALE',$2,$2
    from credits c join public.invoices i on i.id=c.source_invoice_id
    on conflict(id) do nothing returning id
  ),
  cloned_lines as (
    insert into public.invoice_lines(invoice_id,timesheet_id,booking_id,description,
      hours_day,hours_night,hours_sat,hours_sun,hours_bh,pay_day,pay_night,pay_sat,pay_sun,pay_bh,
      charge_day,charge_night,charge_sat,charge_sun,charge_bh,total_pay_ex_vat,total_charge_ex_vat,
      margin_ex_vat,vat_rate_pct,vat_amount,total_inc_vat,paper_ts_r2_key,meta_json,source_key)
    select c.credit_note_id,l.timesheet_id,l.booking_id,
      'CREDIT NOTE – '||coalesce(l.description,''),
      l.hours_day,l.hours_night,l.hours_sat,l.hours_sun,l.hours_bh,
      l.pay_day,l.pay_night,l.pay_sat,l.pay_sun,l.pay_bh,
      l.charge_day,l.charge_night,l.charge_sat,l.charge_sun,l.charge_bh,
      -l.total_pay_ex_vat,-l.total_charge_ex_vat,-l.margin_ex_vat,l.vat_rate_pct,
      -l.vat_amount,-l.total_inc_vat,l.paper_ts_r2_key,
      coalesce(l.meta_json,'{}'::jsonb)||jsonb_build_object(
        'credit_note',true,'original_invoice_id',c.source_invoice_id,
        'original_invoice_line_id',l.id,'credit_of_line_id',l.id,'line_type',
        'CREDIT_'||coalesce(l.meta_json->>'line_type','LINE')),
      'CN:'||c.credit_note_id||':LINE:'||l.id
    from credits c join public.invoice_lines l on l.invoice_id=c.source_invoice_id
    on conflict(invoice_id,source_key) do nothing returning invoice_id
  ),
  source_mark as (
    update public.invoices i set credit_note_created_at_utc=$2,updated_at=$2
    where i.id in(select source_invoice_id from credits) returning i.id
  ),
  unlocks as (
    update public.timesheets_financials tf
    set locked_by_invoice_id=null,locked_at_utc=null,
      unlocked_by_credit_note_id=c.credit_note_id,is_stale=true,
      stale_reason='UNLOCKED_BY_CREDIT',updated_at=$2
    from credits c
    where tf.locked_by_invoice_id=c.source_invoice_id and tf.is_current returning tf.timesheet_id
  ),
  segment_unlocks as (
    update public.timesheets_financials tf
    set invoice_breakdown_json=jsonb_set(tf.invoice_breakdown_json,'{segments}',
      (select jsonb_agg(case
        when e.value->>'invoice_locked_invoice_id'=c.source_invoice_id::text
          then (e.value-'invoice_locked_invoice_id'-'invoice_locked_at_utc')
        else e.value end order by e.ordinality)
       from jsonb_array_elements(tf.invoice_breakdown_json->'segments')
         with ordinality e(value,ordinality)),true),
      unlocked_by_credit_note_id=c.credit_note_id,is_stale=true,
      stale_reason='UNLOCKED_BY_CREDIT',updated_at=$2
    from credits c
    where tf.is_current and jsonb_typeof(tf.invoice_breakdown_json->'segments')='array'
      and exists(select 1 from jsonb_array_elements(tf.invoice_breakdown_json->'segments') e
        where e->>'invoice_locked_invoice_id'=c.source_invoice_id::text)
    returning tf.timesheet_id
  ),
  recompute_outbox as (
    insert into public.ts_financials_outbox(
      timesheet_id,reason,attempt_count,next_attempt_at,last_error,created_at)
    select distinct u.timesheet_id,'VERSION_ROTATED'::public.ts_fin_reason_enum,
      0,$2,null,$2
    from (
      select timesheet_id from unlocks
      union all select timesheet_id from segment_unlocks
    ) u
    on conflict on constraint uq_tsfin_outbox do nothing
    returning timesheet_id
  ),
  credit_audits as (
    insert into public.audit_events(ts_utc,actor_user_id,actor_display,
      actor_role_at_time,object_type,object_id_text,action,after_json,reason)
    select $2,o.actor_user_id,u.display_name,u.role,'invoices',
      c.credit_note_id::text,'CREDIT_NOTE_CREATED',
      jsonb_build_object('credit_note_id',c.credit_note_id,
        'original_invoice_id',c.source_invoice_id,
        'credit_reason',c.payload_json->>'credit_reason',
        'command_token',c.payload_json->>'command_token',
        'subtotal_ex_vat',-i.subtotal_ex_vat,'vat_amount',-i.vat_amount,
        'total_inc_vat',-i.total_inc_vat,
        'financial_recompute_enqueued',true),
      'INVOICE_OPERATION_QUEUE'
    from credits c join public.invoices i on i.id=c.source_invoice_id
    join public.invoice_operations o on o.id=c.operation_id
    left join public.tms_users u on u.id=o.actor_user_id
    returning id
  ),
  completed as (
    update public.invoice_operation_chunks q
    set status='QUEUED',
      phase='QUEUE_DOCUMENT',
      error_json=null,
      completed_at_utc=null,lease_owner=null,lease_token=null,
      lease_expires_at_utc=null,updated_at_utc=$2,
      result_json=jsonb_build_object('invoice_ids',jsonb_build_array(c.credit_note_id),
        'credit_note_id',c.credit_note_id,'source_invoice_id',c.source_invoice_id)
    from credits c
    cross join (select count(*) outbox_count from recompute_outbox) outbox_application
    cross join (select count(*) audit_count from credit_audits) audit_application
    where q.id=c.id
    returning q.id,q.status,q.phase,q.result_json,q.error_json
  )
  select coalesce(jsonb_agg(jsonb_build_object(
    'chunk_id',id,'status',status,'phase',phase,
    'result',result_json,'error',error_json)),'[]'::jsonb)
  from completed
  $q$
  into v_part using p_claims,v_now;
  v_result:=v_result||coalesce(v_part,'[]'::jsonb);

  -- Document work is queued in a separate SQL statement so source-invalidation
  -- statement triggers cannot supersede the freshly-created document operation.
  with claim_ids as materialized (
    select (x->>'chunk_id')::uuid chunk_id
    from jsonb_array_elements(p_claims) x where x->>'phase'='QUEUE_DOCUMENT'
  ),
  queued_sources as materialized (
    select c.id chunk_id,c.operation_id parent_operation_id,
      (c.payload_json#>>'{plan,planned_invoice_id}')::uuid planned_invoice_id,
      c.payload_json->>'source_revision' source_revision,
      c.payload_json->>'command_type' command_type,
      c.payload_json->>'command_token' command_token,
      c.payload_json->>'credit_reason' credit_reason,
      i.document_revision::text document_revision
    from claim_ids q join public.invoice_operation_chunks c on c.id=q.chunk_id
    join public.invoices i
      on i.id=(c.payload_json#>>'{plan,planned_invoice_id}')::uuid
  ),
  existing_doc_versions as materialized (
    select distinct on(q.chunk_id) q.chunk_id,v.id document_version_id,
      v.operation_id,v.status
    from queued_sources q
    join public.invoice_document_versions v
      on v.entity_type='INVOICE'
     and v.entity_id=q.planned_invoice_id
     and v.purpose='DRAFT_PREVIEW'
     and v.source_revision=q.document_revision
     and v.template_version='invoice-professional-v2'
     and v.status in(
       'PLANNING','WAITING_FOR_INPUTS','RENDERING','ASSEMBLING',
       'VERIFYING','READY')
    where q.command_type<>'GENERATE_CREDIT_NOTE'
    order by q.chunk_id,case when v.status='READY' then 0 else 1 end,
      v.created_at_utc desc,v.id desc
  ),
  doc_ops as materialized (
    insert into public.invoice_operations(parent_operation_id,operation_type,entity_type,entity_id,actor_user_id,
      idempotency_key,status,phase,priority,source_revision,template_version,input_json,config_json,progress_json,
      total_units,chunk_count,control_version,change_seq,created_at_utc,updated_at_utc)
    select q.parent_operation_id,'BUILD_DOCUMENT','INVOICE',q.planned_invoice_id,o.actor_user_id,
      encode(digest('DRAFT_PREVIEW|'||q.planned_invoice_id||'|'||q.document_revision||
        '|invoice-professional-v2','sha256'),'hex'),
      'QUEUED','BUILD_MANIFEST',550,q.document_revision,'invoice-professional-v2',
      jsonb_build_object('invoice_id',q.planned_invoice_id,'purpose','DRAFT_PREVIEW'),
      jsonb_build_object('processor_policy',o.config_json->'processor_policy'),
      '{}',1,1,1,
      nextval('public.invoice_operation_change_seq'),v_now,v_now
    from queued_sources q join public.invoice_operations o on o.id=q.parent_operation_id
    where q.command_type<>'GENERATE_CREDIT_NOTE'
      and not exists(
        select 1 from existing_doc_versions e where e.chunk_id=q.chunk_id)
    on conflict(idempotency_key) where status in ('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
    do update set priority=greatest(invoice_operations.priority,excluded.priority),updated_at_utc=v_now
    returning *
  ),
  selected_doc_ops as materialized (
    select q.chunk_id,q.planned_invoice_id,q.document_revision source_revision,
      d.id operation_id,d.control_version,null::uuid existing_version_id,
      null::text existing_status
    from queued_sources q join doc_ops d on d.entity_id=q.planned_invoice_id
    union all
    select q.chunk_id,q.planned_invoice_id,q.document_revision,
      e.operation_id,o.control_version,e.document_version_id,e.status
    from queued_sources q
    join existing_doc_versions e on e.chunk_id=q.chunk_id
    join public.invoice_operations o on o.id=e.operation_id
  ),
  doc_versions as materialized (
    insert into public.invoice_document_versions(entity_type,entity_id,purpose,operation_id,source_revision,
      template_version,status,snapshot_json,snapshot_hash,manifest_json,manifest_hash,created_at_utc)
    select 'INVOICE',d.planned_invoice_id,'DRAFT_PREVIEW',d.operation_id,d.source_revision,
      'invoice-professional-v2','PLANNING','{}',encode(digest('{}','sha256'),'hex'),
      '[]',encode(digest('[]','sha256'),'hex'),v_now
    from selected_doc_ops d
    where d.existing_version_id is null
    on conflict(entity_type,entity_id,purpose,source_revision,template_version)
      where purpose in('DRAFT_PREVIEW','TIMESHEET')
        and status in ('PLANNING','WAITING_FOR_INPUTS','RENDERING','ASSEMBLING','VERIFYING','READY')
    do nothing
    returning id,entity_id,operation_id
  ),
  all_doc_versions as materialized (
    select d.chunk_id,d.planned_invoice_id,d.operation_id,d.control_version,
      d.source_revision,
      coalesce(d.existing_version_id,v.id) document_version_id,
      coalesce(d.existing_status,'PLANNING') document_status
    from selected_doc_ops d
    left join doc_versions v
      on v.entity_id=d.planned_invoice_id and v.operation_id=d.operation_id
    where d.existing_version_id is not null or v.id is not null
  ),
  doc_chunks as (
    insert into public.invoice_operation_chunks(operation_id,chunk_type,phase,work_key,sequence_no,entity_type,entity_id,
      document_version_id,status,priority,run_after_utc,payload_json,operation_control_version,created_at_utc,updated_at_utc)
    select d.operation_id,'DOCUMENT_PLAN','BUILD_MANIFEST',
      encode(digest(concat_ws('|','DOCUMENT_PLAN',d.document_version_id::text,
        d.source_revision,'invoice-professional-v2','1'),'sha256'),'hex'),
      0,'INVOICE',d.planned_invoice_id,d.document_version_id,
      'QUEUED',550,v_now,jsonb_build_object('purpose','DRAFT_PREVIEW'),d.control_version,v_now,v_now
    from all_doc_versions d
    where d.document_status<>'READY'
    on conflict(operation_id,chunk_type,level_no,sequence_no,work_key) do nothing returning operation_id
  ),
  invoice_ptrs as (
    update public.invoices i set preview_document_version_id=d.document_version_id,
      active_document_operation_id=case when d.document_status='READY'
        then null else d.operation_id end,
      document_state=case when d.document_status='READY'
        then 'READY' else 'QUEUED' end,updated_at=v_now
    from all_doc_versions d where i.id=d.planned_invoice_id returning i.id
  ),
  credit_issue_ops as materialized (
    insert into public.invoice_operations(
      parent_operation_id,operation_type,entity_type,entity_id,actor_user_id,
      idempotency_key,status,phase,priority,source_revision,template_version,
      input_json,config_json,progress_json,total_units,chunk_count,
      control_version,change_seq,created_at_utc,updated_at_utc)
    select q.parent_operation_id,'ISSUE_INVOICES','INVOICE_BATCH',null,
      o.actor_user_id,
      encode(digest('CREDIT_ISSUE|'||q.planned_invoice_id||'|'||
        q.document_revision||'|'||coalesce(q.command_token,''),'sha256'),'hex'),
      'QUEUED','VALIDATE',850,q.document_revision,'invoice-professional-v2',
      jsonb_build_object(
        'invoice_ids',jsonb_build_array(q.planned_invoice_id),
        'credit_note',true,'credit_reason',q.credit_reason,
        'command_token',q.command_token,'deliver',false),
      jsonb_build_object('processor_policy',o.config_json->'processor_policy'),
      '{}',1,1,1,nextval('public.invoice_operation_change_seq'),v_now,v_now
    from queued_sources q
    join public.invoice_operations o on o.id=q.parent_operation_id
    where q.command_type='GENERATE_CREDIT_NOTE'
    on conflict(idempotency_key)
      where status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
    do update set priority=greatest(invoice_operations.priority,excluded.priority),
      updated_at_utc=v_now
    returning *
  ),
  selected_credit_issue as materialized (
    select q.chunk_id,q.planned_invoice_id,q.document_revision,
      q.command_token,q.credit_reason,o.id operation_id,o.control_version
    from queued_sources q
    join credit_issue_ops o on o.parent_operation_id=q.parent_operation_id
      and o.operation_type='ISSUE_INVOICES'
    where q.command_type='GENERATE_CREDIT_NOTE'
  ),
  credit_issue_chunks as (
    insert into public.invoice_operation_chunks(
      operation_id,chunk_type,phase,work_key,sequence_no,entity_type,entity_id,status,
      priority,run_after_utc,payload_json,operation_control_version,
      created_at_utc,updated_at_utc)
    select s.operation_id,'ISSUE_INVOICE','VALIDATE',
      encode(digest(concat_ws('|','ISSUE_INVOICE',s.planned_invoice_id::text,
        s.document_revision,s.command_token),'sha256'),'hex'),
      0,'INVOICE',
      s.planned_invoice_id,'QUEUED',850,v_now,
      jsonb_build_object(
        'invoice_id',s.planned_invoice_id,
        'source_revision',s.document_revision,
        'allow_early',true,'deliver',false,
        'command_token',s.command_token,
        'credit_note',true,'credit_reason',s.credit_reason),
      s.control_version,v_now,v_now
    from selected_credit_issue s
    on conflict(operation_id,chunk_type,level_no,sequence_no,work_key) do nothing
    returning operation_id
  ),
  credit_issue_ptrs as (
    update public.invoices i
    set active_issue_operation_id=s.operation_id,issue_state='VALIDATING',
      updated_at=v_now
    from selected_credit_issue s
    where i.id=s.planned_invoice_id
    returning i.id
  ),
  child_results as materialized (
    select d.chunk_id,d.operation_id document_operation_id,
      d.document_version_id,null::uuid issue_operation_id
    from all_doc_versions d
    union all
    select s.chunk_id,null::uuid,null::uuid,s.operation_id
    from selected_credit_issue s
  )
  update public.invoice_operation_chunks c
    set status='COMPLETE',phase='COMPLETE',completed_at_utc=v_now,updated_at_utc=v_now,
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,
      result_json=coalesce(c.result_json,'{}'::jsonb)||jsonb_build_object(
        'document_operation_id',d.document_operation_id,
        'document_version_id',d.document_version_id,
        'issue_operation_id',d.issue_operation_id)
    from child_results d where c.id=d.chunk_id
  ;
  select coalesce(jsonb_agg(jsonb_build_object(
      'chunk_id',c.id,'status',c.status,'phase',c.phase,
      'result',c.result_json,'error',c.error_json)),'[]'::jsonb)
    into v_part
  from jsonb_array_elements(p_claims) x
  join public.invoice_operation_chunks c
    on c.id=case when coalesce(x->>'chunk_id','')~*
      '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      then(x->>'chunk_id')::uuid end
  where x->>'phase'='QUEUE_DOCUMENT';
  v_result:=v_result||coalesce(v_part,'[]'::jsonb);

  return coalesce(v_result,'[]'::jsonb);
end;
$function$;

revoke all on function public.timesheet_qr_refuse_and_reset(uuid,uuid,text,uuid) from public,anon,authenticated;
grant execute on function public.timesheet_qr_refuse_and_reset(uuid,uuid,text,uuid) to authenticated,service_role;
revoke all on function public.client_update_with_settings_v1(uuid,bigint,timestamptz,jsonb,jsonb,uuid,text) from public,anon,authenticated;
grant execute on function public.client_update_with_settings_v1(uuid,bigint,timestamptz,jsonb,jsonb,uuid,text) to service_role;
revoke all on function public.client_create_with_settings_v1(uuid,jsonb,uuid,jsonb,timestamptz) from public,anon,authenticated;
grant execute on function public.client_create_with_settings_v1(uuid,jsonb,uuid,jsonb,timestamptz) to service_role;
revoke all on function private._invoice_generation_resolve_command_groups(jsonb,uuid,timestamptz) from public,anon,authenticated;
grant execute on function private._invoice_generation_resolve_command_groups(jsonb,uuid,timestamptz) to service_role;
revoke all on function private._invoice_delivery_routes_batch(jsonb,date) from public,anon,authenticated;
grant execute on function private._invoice_delivery_routes_batch(jsonb,date) to service_role;

notify pgrst, 'reload schema';

commit;
