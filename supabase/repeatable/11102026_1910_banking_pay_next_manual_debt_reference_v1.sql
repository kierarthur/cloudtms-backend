-- FC06 / BD118: complete current private owners only; fresh reference and saved factual output.
-- No changes to original money/apply, terms/context,14-argument compatibility or public owners.

create or replace function private.bpay_next_case_create_preview_v1(p_actor_user_id uuid,p_input jsonb)
returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare v_terms jsonb;v_context jsonb;v_warnings jsonb:='[]'::jsonb;
begin
 v_terms:=private.bpay_next_case_create_terms_v1(p_input);
 -- FC06: fresh Manual Debt needs its original liability; historical replay is separate.
 if p_input->>'case_kind'='MANUAL_DEBT' and nullif(btrim(p_input->>'original_liability_reference'),'') is null then
  raise exception using errcode='22023',message='BPAY_NEXT_CASE_CREATE_METADATA_INVALID';end if;
 v_context:=private.bpay_next_case_create_context_v1(p_actor_user_id,(p_input->>'candidate_id')::uuid);
 if exists(select 1 from private.bpay_next_case_component c join private.bpay_next_finance_case f on f.id=c.case_id
   join private.bpay_next_case_rule r on r.id=f.current_rule_id and r.id=c.rule_id
   where c.candidate_id=(p_input->>'candidate_id')::uuid and f.status='OPEN' and c.direction='DEDUCTION'
    and r.weekly_due_source_ex_vat>0 and (case when c.case_kind in ('LOAN','ADVANCE') then c.funded_source_ex_vat else c.approved_source_ex_vat end)
       >c.recovered_source_ex_vat+c.written_off_source_ex_vat limit 1) then
  v_warnings:=jsonb_build_array('EXISTING_REPAYMENTS');end if;
 return v_terms||jsonb_build_object('candidate_id',p_input->'candidate_id','effective_take_home_floor',
   coalesce(p_input->>'take_home_floor_override',v_context->>'candidate_default_floor'),
   'warnings',v_warnings,'preview_token',jsonb_build_object('input',p_input,'context',v_context));
end
$function$;

create or replace function private.bpay_next_accept_case_create_v1(
  p_command_id uuid,p_candidate_id uuid,p_actor_user_id uuid,
  p_case_kind text,p_case_subtype text,p_tax_treatment text,p_principal numeric,
  p_due_date date,p_start_monday date,p_weekly_due numeric,p_week_count bigint,
  p_minimum_earnings_threshold numeric,p_take_home_floor_override numeric,p_reason text,
  p_note text,p_original_liability_reference text,p_preview_token jsonb
) returns jsonb
language plpgsql security invoker
set search_path=pg_catalog,private,public
as $function$
declare
  v_input jsonb;v_terms jsonb;v_current_token jsonb;
  v_epoch bigint;
  v_prior private.bpay_next_case_create_request%rowtype;
  v_sequence bigint;
  v_source_channel text;
  v_candidate_active boolean;
  v_policy uuid;
  v_weekly numeric;
  v_weeks bigint;
  v_required_weeks numeric;
  v_at timestamptz;
  v_week date;
  v_recovery_id uuid;
begin
  if p_command_id is null then raise exception using errcode='22023',message='BPAY_NEXT_CASE_CREATE_INPUT_INVALID';end if;
  -- Preserve the installed numeric domain: harmless scales are valid, while
  -- subpenny and nonfinite operands are refused before canonical text shaping.
  if p_principal::text in ('NaN','Infinity','-Infinity') or p_principal<>pg_catalog.trunc(p_principal,2)
    or (p_weekly_due is not null and (p_weekly_due::text in ('NaN','Infinity','-Infinity') or p_weekly_due<>pg_catalog.trunc(p_weekly_due,2)))
    or (p_minimum_earnings_threshold is not null and (p_minimum_earnings_threshold::text in ('NaN','Infinity','-Infinity') or p_minimum_earnings_threshold<>pg_catalog.trunc(p_minimum_earnings_threshold,2)))
    or (p_take_home_floor_override is not null and (p_take_home_floor_override::text in ('NaN','Infinity','-Infinity') or p_take_home_floor_override<>pg_catalog.trunc(p_take_home_floor_override,2))) then
    raise exception using errcode='22023',message='BPAY_NEXT_CASE_CREATE_VALUE_INVALID';end if;
  v_input:=jsonb_build_object('candidate_id',p_candidate_id::text,'case_kind',p_case_kind,'case_subtype',p_case_subtype,'tax_treatment',p_tax_treatment,'principal',pg_catalog.trunc(p_principal,2)::text,'due_date',p_due_date::text,'start_monday',p_start_monday::text,'weekly_due',pg_catalog.trunc(p_weekly_due,2)::text,'week_count',p_week_count::text,'minimum_earnings_threshold',pg_catalog.trunc(p_minimum_earnings_threshold,2)::text,'take_home_floor_override',pg_catalog.trunc(p_take_home_floor_override,2)::text,'reason',p_reason,'note',p_note,'original_liability_reference',p_original_liability_reference);
  v_terms:=private.bpay_next_case_create_terms_v1(v_input);
  v_weekly:=(v_terms->>'weekly_due')::numeric;v_weeks:=(v_terms->>'week_count')::bigint;
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  if not exists(select 1 from public.tms_users u where u.id=p_actor_user_id
    and u.is_active is true and u.role::text='admin' for share) then
    raise exception using errcode='42501',message='BPAY_NEXT_CASE_CREATE_ACTOR_INVALID';
  end if;
  -- Protect the source Candidate while the exact command guard serialises
  -- duplicate receipts. Replay uses its receipt even after channel changes.
  perform 1 from public.candidates c where c.id=p_candidate_id for share;
  if not found then
    raise exception using errcode='22023',message='BPAY_NEXT_CASE_CREATE_CANDIDATE_MISSING';
  end if;
  insert into private.bpay_next_worker_control(candidate_id) values(p_candidate_id) on conflict(candidate_id) do nothing;
  perform 1 from private.bpay_next_worker_control where candidate_id=p_candidate_id for update;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('bpay-next-case-create:'||p_command_id::text,0));
  select r.* into v_prior from private.bpay_next_case_create_request r where r.command_id=p_command_id;
  if found then
    if v_prior.candidate_id<>p_candidate_id or v_prior.actor_user_id<>p_actor_user_id
       or v_prior.case_kind<>p_case_kind or v_prior.case_subtype<>p_case_subtype
       or v_prior.tax_treatment<>p_tax_treatment or v_prior.principal_source_ex_vat<>p_principal
       or v_prior.due_date is distinct from p_due_date or v_prior.input_start_monday is distinct from p_start_monday
       or v_prior.input_weekly_due is distinct from p_weekly_due or v_prior.input_week_count is distinct from p_week_count
       or v_prior.minimum_earnings_threshold is distinct from p_minimum_earnings_threshold
       or v_prior.take_home_floor_override is distinct from p_take_home_floor_override
       or v_prior.approval_reason<>p_reason or v_prior.note is distinct from p_note
       or v_prior.original_liability_reference is distinct from p_original_liability_reference
       or v_prior.preview_token is distinct from p_preview_token then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_CREATE_REPLAY_CONFLICT';
    end if;
    select c.agency_sequence into strict v_sequence from private.bpay_next_command c
      where c.id=p_command_id and c.command_kind='CASE_CREATE' and c.module_epoch=v_epoch;
    return pg_catalog.jsonb_build_object('case_id',v_prior.case_id,'command_id',p_command_id,
      'sequence',v_sequence::text,'phase','ACCEPTED_PENDING_CASE','replay',true);
  end if;
  -- FC06: only fresh admission requires the reference. Exact prior receipts returned above.
  if p_case_kind='MANUAL_DEBT' and nullif(btrim(p_original_liability_reference),'') is null then
    raise exception using errcode='22023',message='BPAY_NEXT_CASE_CREATE_METADATA_INVALID';end if;
  select upper(c.pay_method::text),c.active into strict v_source_channel,v_candidate_active
    from public.candidates c where c.id=p_candidate_id;
  if v_candidate_active is not true then
    raise exception using errcode='55000',message='BPAY_NEXT_CASE_CREATE_CANDIDATE_NOT_ACTIVE';
  end if;
  if v_source_channel is null or v_source_channel not in ('PAYE','UMBRELLA') then
    raise exception using errcode='55000',message='BPAY_NEXT_CASE_CREATE_SOURCE_CHANNEL_UNBOUND';
  end if;
  v_policy:=private.bpay_next_current_valuation_policy_id_v1();
  if p_preview_token is not null then
    -- Compare entered token input with typed operands; numeric text may have
    -- harmless scale differences. The public branch also compares raw inputs.
    v_current_token:=jsonb_build_object('input',p_preview_token->'input',
      'context',private.bpay_next_case_create_context_v1(p_actor_user_id,p_candidate_id));
    if jsonb_typeof(p_preview_token) is distinct from 'object' or octet_length(p_preview_token::text)>24576
      or (select count(*) from jsonb_object_keys(p_preview_token))<>2
      or p_preview_token is distinct from v_current_token then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_PREVIEW_CHANGED';end if;
    perform private.bpay_next_case_create_terms_v1(p_preview_token->'input');
    if (p_preview_token->'input'-array['candidate_id','principal','weekly_due','minimum_earnings_threshold','take_home_floor_override'])
       is distinct from (v_input-array['candidate_id','principal','weekly_due','minimum_earnings_threshold','take_home_floor_override'])
      or (p_preview_token->'input'->>'candidate_id')::uuid is distinct from p_candidate_id
      or (p_preview_token->'input'->>'principal')::numeric is distinct from p_principal
      or (p_preview_token->'input'->>'weekly_due')::numeric is distinct from p_weekly_due
      or (p_preview_token->'input'->>'minimum_earnings_threshold')::numeric is distinct from p_minimum_earnings_threshold
      or (p_preview_token->'input'->>'take_home_floor_override')::numeric is distinct from p_take_home_floor_override then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_PREVIEW_CHANGED';end if;
  end if;
  v_at:=pg_catalog.transaction_timestamp();
  v_week:=(v_at at time zone 'Europe/London')::date;
  v_week:=v_week-(extract(isodow from v_week)::integer-1);
  if p_case_kind in ('LOAN','ADVANCE') then v_recovery_id:=pg_catalog.gen_random_uuid();end if;
  -- The existing short agency clock is LAST in this receipt lock chain.
  v_sequence:=private.bpay_next_receive_command_v1(p_command_id,'CASE_CREATE');
  insert into private.bpay_next_command_member(command_id,candidate_id,member_no)
    values(p_command_id,p_candidate_id,1);
  insert into private.bpay_next_case_create_request
    (command_id,candidate_id,actor_user_id,case_id,rule_id,primary_component_id,recovery_component_id,
     case_kind,case_subtype,tax_treatment,principal_source_ex_vat,source_pay_channel,currency,valuation_policy_id,
     due_date,input_start_monday,input_weekly_due,input_week_count,weekly_due_source_ex_vat,schedule_week_count,
     minimum_earnings_threshold,take_home_floor_override,approval_reason,accepted_at_utc,opening_pay_week_start,note,original_liability_reference,preview_token)
    values(p_command_id,p_candidate_id,p_actor_user_id,p_command_id,p_command_id,p_command_id,v_recovery_id,
     p_case_kind,p_case_subtype,p_tax_treatment,p_principal,v_source_channel,'GBP',v_policy,
     p_due_date,p_start_monday,p_weekly_due,p_week_count,v_weekly,v_weeks,
     p_minimum_earnings_threshold,p_take_home_floor_override,p_reason,v_at,v_week,p_note,p_original_liability_reference,p_preview_token);
  update private.bpay_next_command set expected_member_count=1,status='SEALED',sealed_at_utc=v_at
    where id=p_command_id and status='RECEIVED';
  if not found then raise exception using errcode='23514',message='BPAY_NEXT_CASE_CREATE_COMMAND_INVALID';end if;
  return pg_catalog.jsonb_build_object('case_id',p_command_id,'command_id',p_command_id,
    'sequence',v_sequence::text,'phase','ACCEPTED_PENDING_CASE','replay',false);
end
$function$;

create or replace function private.bpay_next_current_case_component_page_v1(
  p_candidate_id uuid,p_case_id uuid,p_after_component_ordinal bigint,p_limit integer
) returns jsonb language plpgsql stable security invoker
set search_path=pg_catalog,private
as $function$
declare
  v_row record;v_rows jsonb:='[]'::jsonb;v_count integer:=0;
  v_bytes integer:=512;v_more boolean:=false;v_pay_week date;v_date_context jsonb;
begin
  if p_candidate_id is null or p_case_id is null or p_limit is null or p_limit not between 1 and 100
     or (p_after_component_ordinal is not null and p_after_component_ordinal<0) then
    raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
  end if;
  if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT') then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  if not exists(select 1 from private.bpay_next_finance_case c where c.id=p_case_id
      and c.candidate_id=p_candidate_id and c.status in ('OPEN','PAUSED')) then
    raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_SCOPE_INVALID';
  end if;
  v_date_context:=public.pay_banking_official_date_context_v1(null::timestamptz);
  if v_date_context->>'ok'<>'true' or v_date_context->>'current_official_pay_date' is null then
    raise exception using errcode='55000',message='BPAY_NEXT_PREPARATION_DATE_UNAVAILABLE';
  end if;
  v_pay_week:=pg_catalog.date_trunc('week',(v_date_context->>'current_official_pay_date')::date)::date;
  for v_row in
    select pg_catalog.to_jsonb(page) as row_value from (
      select c.id as case_component_id,c.case_id,c.candidate_id,c.component_ordinal::text,
        c.component_revision::text,c.case_kind,c.case_subtype,c.tax_treatment,
        c.instruction_kind,c.direction,c.payroll_stage,c.source_pay_channel,c.resolution_state,
        c.approved_source_ex_vat::text,c.funded_source_ex_vat::text,c.recovered_source_ex_vat::text,
        c.written_off_source_ex_vat::text,c.active_payout_source_ex_vat::text,c.active_recovery_source_ex_vat::text,
        c.rule_id,fc.current_rule_id,r.rule_revision::text,
        fc.principal_approved::text as case_principal_approved,
        r.weekly_due_source_ex_vat::text,r.minimum_earnings_threshold::text,r.take_home_floor_override::text,
        r.schedule_start_monday,r.next_due_monday,r.schedule_week_count::text,
        (case when c.direction='DEDUCTION' then
          case when c.case_kind in ('LOAN','ADVANCE') then c.funded_source_ex_vat else c.approved_source_ex_vat end
          -c.recovered_source_ex_vat-c.written_off_source_ex_vat
          else c.approved_source_ex_vat-c.funded_source_ex_vat-c.paid_credit_source_ex_vat-c.written_off_source_ex_vat end)::text as outstanding_source_ex_vat,
        (p.opening_due_source_ex_vat-p.realised_recovery_source_ex_vat-p.active_unrealised_recovery_source_ex_vat)::text as amount_due,
        null::text as recoverable_this_run,
        (select cr.original_liability_reference from private.bpay_next_case_create_request cr
         where cr.case_id=c.case_id and cr.candidate_id=c.candidate_id
          and cr.case_kind='MANUAL_DEBT' and cr.case_subtype='MANUAL_DEBT'
          and c.case_kind='MANUAL_DEBT' and c.case_subtype='MANUAL_DEBT') as original_liability_reference
      from private.bpay_next_case_component c
      join private.bpay_next_finance_case fc on fc.id=c.case_id and fc.candidate_id=c.candidate_id
      left join private.bpay_next_case_rule r on r.id=fc.current_rule_id and r.case_id=c.case_id
        and r.candidate_id=c.candidate_id and r.rule_revision=fc.current_rule_revision
      left join private.bpay_next_case_period p on p.case_component_id=c.id and p.case_id=c.case_id
        and p.candidate_id=c.candidate_id and p.pay_week_start=v_pay_week and p.rule_id=c.rule_id
      where c.case_id=p_case_id and c.candidate_id=p_candidate_id
        and c.component_ordinal>coalesce(p_after_component_ordinal,0)
      order by c.component_ordinal limit p_limit
    ) page
  loop
    if v_bytes+pg_catalog.octet_length(v_row.row_value::text)+1>120000 then
      if v_count=0 then raise exception using errcode='54000',message='BPAY_NEXT_PREPARATION_ROW_TOO_LARGE';end if;
      v_more:=true;exit;
    end if;
    v_bytes:=v_bytes+pg_catalog.octet_length(v_row.row_value::text)+1;
    v_rows:=v_rows||pg_catalog.jsonb_build_array(v_row.row_value);v_count:=v_count+1;
  end loop;
  return pg_catalog.jsonb_build_object('rows',v_rows,'complete',not(v_more or v_count=p_limit));
end
$function$;

create or replace function private.bpay_next_case_list_row_v1(p_case_id uuid,p_context jsonb)
returns jsonb language plpgsql volatile security invoker set search_path=pg_catalog,private,public
as $function$
declare
 v_case private.bpay_next_finance_case%rowtype;v_key private.bpay_next_case_list_key%rowtype;
 v_rule private.bpay_next_case_rule%rowtype;v_component private.bpay_next_case_component%rowtype;
 v_component_rule private.bpay_next_case_rule%rowtype;v_period private.bpay_next_case_period%rowtype;
 v_metadata private.bpay_next_workbench_candidate_metadata%rowtype;v_binding record;
 v_quote record;v_origin_policy uuid;v_basis bytea;v_snooze record;v_result jsonb;v_component_ids uuid[];
 v_today date:=(p_context->>'local_date')::date;v_week date:=(p_context->>'pay_week_start')::date;
 v_remaining numeric;v_due numeric:=0;v_payout numeric:=0;v_outstanding numeric;v_hold numeric:=0;
 v_components integer:=0;v_unknown_due boolean:=false;v_open_tax boolean:=false;v_open_non_tax boolean:=false;
 v_unresolved boolean:=false;v_stale boolean:=false;v_stale_unknown boolean:=false;v_gap text;
 v_active_snoozes integer:=0;v_stale_snoozes integer:=0;v_indefinite integer:=0;v_until date;
 v_pending boolean:=false;v_hold_text text;v_run uuid;v_channel text;
begin
 select c.* into strict v_case from private.bpay_next_finance_case c where c.id=p_case_id;
 select k.* into strict v_key from private.bpay_next_case_list_key k where k.case_id=p_case_id;
 select r.* into v_rule from private.bpay_next_case_rule r where r.id=v_case.current_rule_id and r.case_id=v_case.id;
 select m.* into v_metadata from private.bpay_next_workbench_candidate_metadata m where m.candidate_id=v_case.candidate_id;
 if v_key.created_at_utc is null then v_gap:='CASE_CREATED_UNAVAILABLE';
 elsif v_key.case_subtype is null or v_key.case_subtype not in ('LOAN','PAYMENT_ADVANCE','OVERPAYMENT','UNDERPAYMENT','MANUAL_CREDIT','MANUAL_DEBT') then v_gap:='CASE_TYPE_UNMAPPED';
 elsif v_rule.id is null then v_gap:='CASE_RULE_UNAVAILABLE';end if;
 if v_key.client_association='UNKNOWN' then v_gap:=coalesce(v_gap,'CASE_CLIENT_UNAVAILABLE');end if;
 v_outstanding:=case when v_case.case_kind in ('LOAN','ADVANCE') then v_case.principal_funded
  else v_case.principal_approved end-v_case.principal_recovered-v_case.principal_written_off-v_case.principal_paid_credit;
 select original_valuation_policy_id into v_origin_policy from private.bpay_next_work_collection where case_id=p_case_id;
 if not found then select valuation_policy_id into v_origin_policy from private.bpay_next_case_create_request where case_id=p_case_id;end if;
 -- Current native producers create one component, or the two instructions of
 -- one Payment Advance. Inspect three keys to detect unsupported membership;
 -- never calculate or publish a prefix total.
 select array_agg(child.id order by child.component_ordinal) into v_component_ids from
  (select id,component_ordinal from private.bpay_next_case_component where case_id=p_case_id order by component_ordinal limit 3) child;
 if cardinality(v_component_ids)>2 then
  v_gap:=coalesce(v_gap,'CASE_COMPONENT_MEMBERSHIP_UNAVAILABLE');
  v_unknown_due:=true;v_payout:=null;v_hold:=v_case.active_hold_amount;
  v_open_tax:=null;v_open_non_tax:=null;v_unresolved:=null;v_stale_unknown:=true;
  v_active_snoozes:=null;v_stale_snoozes:=null;v_indefinite:=null;
 end if;
 for v_component in select component.* from private.bpay_next_case_component component
  where component.case_id=p_case_id and cardinality(v_component_ids)<=2 and component.id=any(v_component_ids)
  order by component.component_ordinal
 loop
  v_components:=v_components+1;
  v_remaining:=case when v_component.direction='DEDUCTION' then
    case when v_component.case_kind in ('LOAN','ADVANCE') then v_component.funded_source_ex_vat else v_component.approved_source_ex_vat end
      -v_component.recovered_source_ex_vat-v_component.written_off_source_ex_vat
   else v_component.approved_source_ex_vat-v_component.funded_source_ex_vat-v_component.paid_credit_source_ex_vat-v_component.written_off_source_ex_vat end;
  v_hold:=v_hold+v_component.active_payout_source_ex_vat+v_component.active_recovery_source_ex_vat;
  if v_component.direction='PAYMENT' then v_payout:=v_payout+greatest(v_remaining-v_component.active_payout_source_ex_vat,0);
  else
   select period.* into v_period from private.bpay_next_case_period period where period.case_component_id=v_component.id
    and period.case_id=p_case_id and period.candidate_id=v_case.candidate_id and period.pay_week_start=v_week and period.rule_id=v_component.rule_id;
   if not found and v_remaining>0 then v_unknown_due:=true;
   elsif found then v_due:=v_due+greatest(v_period.opening_due_source_ex_vat-v_period.realised_recovery_source_ex_vat-v_period.active_unrealised_recovery_source_ex_vat,0);end if;
  end if;
  if v_remaining>0 and v_case.status in ('OPEN','PAUSED') then
   if v_component.tax_treatment='TAXABLE' then
    v_open_tax:=true;
    if v_component.resolution_state='REVIEW' then v_unresolved:=true;
    elsif v_component.resolution_state='RESOLVED' then
     if v_origin_policy is null or v_metadata.target_pay_channel is null
      or (v_metadata.target_pay_channel='UMBRELLA' and (v_metadata.target_umbrella_enabled is distinct from true or v_metadata.target_umbrella_vat_chargeable is null)) then
      v_stale_unknown:=true;v_gap:=coalesce(v_gap,'CASE_RESOLUTION_UNAVAILABLE');
     else
      begin
       select * into strict v_quote from private.bpay_next_value_source_for_target_v1(v_component.approved_source_ex_vat,
        v_component.source_pay_channel,v_metadata.target_pay_channel,v_metadata.target_umbrella_vat_chargeable,
        v_origin_policy,(p_context->>'official_pay_date')::date);
       v_stale:=v_stale or (v_component.target_pay_channel,v_component.valuation_policy_id,v_component.valuation_window_id,
         v_component.target_ex_vat,v_component.target_vat,v_component.target_inc_vat)
        is distinct from (v_metadata.target_pay_channel,v_origin_policy,v_quote.policy_window_id,
         v_quote.target_ex_vat,v_quote.target_vat,v_quote.target_inc_vat);
      exception when check_violation or no_data_found or too_many_rows then
       v_stale_unknown:=true;v_gap:=coalesce(v_gap,'CASE_RESOLUTION_UNAVAILABLE');
      end;
     end if;
    else v_stale_unknown:=true;v_gap:=coalesce(v_gap,'CASE_RESOLUTION_UNAVAILABLE');end if;
   elsif v_component.tax_treatment='NON_TAXABLE' then v_open_non_tax:=true;
   elsif v_component.tax_treatment<>'NOT_APPLICABLE' then v_gap:=coalesce(v_gap,'CASE_TAX_UNMAPPED');end if;
  end if;
  select r.* into v_component_rule from private.bpay_next_case_rule r where r.id=v_component.rule_id and r.case_id=p_case_id;
  if v_component_rule.id is not null then
   -- Exact existing WB36 Case basis recipe; not a new resolution proof.
   v_basis:=pg_catalog.sha256(pg_catalog.convert_to(jsonb_build_array(v_case.candidate_id,v_component.case_id,v_component.id,
    v_component.case_kind,v_component.case_subtype,v_component.tax_treatment,v_component.instruction_kind,v_component.direction,
    v_component.payroll_stage,v_component.source_pay_channel,v_component.approved_source_ex_vat,v_component.funded_source_ex_vat,
    v_component_rule.minimum_earnings_threshold,v_component_rule.take_home_floor_override,v_component_rule.weekly_due_source_ex_vat,
    v_component_rule.schedule_start_monday,v_component_rule.next_due_monday,v_component_rule.schedule_week_count)::text,'UTF8'));
   for v_snooze in select * from private.bpay_next_snooze where case_component_id=v_component.id and case_id=p_case_id
    and candidate_id=v_case.candidate_id and scope_identity=v_component.id::text
    and scope_kind='CASE' and active and (until_date is null or until_date>=v_today)
   loop
    if v_snooze.worker_pay_basis=v_basis then
     v_active_snoozes:=v_active_snoozes+1;
     if v_snooze.until_date is null then v_indefinite:=v_indefinite+1;else v_until:=least(v_until,v_snooze.until_date);end if;
    else v_stale_snoozes:=v_stale_snoozes+1;end if;
   end loop;
  end if;
 end loop;
 if v_components=0 then v_gap:=coalesce(v_gap,'CASE_COMPONENTS_UNAVAILABLE');v_unknown_due:=true;end if;
 select * into v_binding from private.bpay_next_case_protected_bindings_v1(p_case_id,null,1);
 if found then
  v_pending:=v_binding.binding_kind='PENDING';v_run:=v_binding.run_id;v_channel:=v_binding.channel;
  v_hold_text:=case when v_pending then 'Being prepared' when v_channel='PAYE' then 'In PAYE Draft'
   when v_channel='UMBRELLA' then 'In Umbrella Draft' else 'In a Draft' end;
 elsif v_hold>0 then v_hold_text:='Reserved for payment';end if;
 v_result:=jsonb_build_object('case_id',v_case.id,'candidate_id',v_case.candidate_id,
  'candidate_name',v_key.candidate_display_name,'candidate_reference',v_key.candidate_reference,
  'client_id',v_key.client_id,'client_name',v_key.client_display_name,'client_association',v_key.client_association,
  'case_kind',v_case.case_kind,'case_subtype',v_key.case_subtype,'case_type_label',v_key.case_type_label,
  'original_liability_reference',(select cr.original_liability_reference from private.bpay_next_case_create_request cr
   where cr.case_id=v_case.id and cr.candidate_id=v_case.candidate_id
    and cr.case_kind='MANUAL_DEBT' and cr.case_subtype='MANUAL_DEBT'
    and v_case.case_kind='MANUAL_DEBT' and v_key.case_subtype='MANUAL_DEBT'),
  'tax_treatment',v_case.tax_treatment,'tax_label',case when v_key.case_subtype is null or v_key.case_type_label='Unmapped case' then 'Tax treatment unavailable' when v_open_tax and v_open_non_tax then 'Mixed tax treatment'
   when v_case.tax_treatment='TAXABLE' then 'Taxable' when v_case.tax_treatment='NON_TAXABLE' then 'Non-taxable'
   when v_case.tax_treatment='NOT_APPLICABLE' then 'Not applicable' else 'Tax treatment unavailable' end,
  'status',v_case.status,'created_at_utc',v_key.created_at_utc,'case_revision',v_case.case_revision::text,
  'principal_approved',v_case.principal_approved::text,'principal_funded',v_case.principal_funded::text,
  'principal_recovered',v_case.principal_recovered::text,'principal_written_off',v_case.principal_written_off::text,
  'outstanding',v_outstanding::text,'pending_payout',v_payout::text,
  'amount_due',case when v_unknown_due then null else v_due::text end,'recoverable_this_run',null,
  'active_hold_amount',v_hold::text,'active_hold_explanation',v_hold_text,'protected_run_id',v_run,
  'protected_channel',v_channel,'input_pending',v_pending,
  'due_date',v_case.due_date,'weekly_due',v_rule.weekly_due_source_ex_vat::text,
  'schedule_start_monday',v_rule.schedule_start_monday,'next_due_monday',v_rule.next_due_monday,
  'schedule_week_count',v_rule.schedule_week_count::text,
  'minimum_earnings_threshold',v_rule.minimum_earnings_threshold::text,'take_home_floor',v_rule.take_home_floor_override::text,
  'has_active_snooze',v_active_snoozes>0,'active_snooze_count',v_active_snoozes::text,
  'indefinite_snooze_count',v_indefinite::text,'snooze_until_date',v_until,'stale_snooze_count',v_stale_snoozes::text,
  'is_mixed_case',v_open_tax and v_open_non_tax,'unresolved_taxable',v_unresolved,
  'resolution_stale',case when v_stale then true when v_stale_unknown then null else false end,'gap_code',v_gap);
 if pg_catalog.octet_length(v_result::text)>90000 then raise exception using errcode='54000',message='BPAY_NEXT_CASE_LIST_ROW_TOO_LARGE';end if;
 return v_result;
end
$function$;

alter function private.bpay_next_case_create_preview_v1(uuid,jsonb) owner to postgres;
revoke all on function private.bpay_next_case_create_preview_v1(uuid,jsonb) from public,anon,authenticated,service_role;
alter function private.bpay_next_accept_case_create_v1(uuid,uuid,uuid,text,text,text,numeric,date,date,numeric,bigint,numeric,numeric,text,text,text,jsonb) owner to postgres;
revoke all on function private.bpay_next_accept_case_create_v1(uuid,uuid,uuid,text,text,text,numeric,date,date,numeric,bigint,numeric,numeric,text,text,text,jsonb) from public,anon,authenticated,service_role;
alter function private.bpay_next_current_case_component_page_v1(uuid,uuid,bigint,integer) owner to postgres;
revoke all on function private.bpay_next_current_case_component_page_v1(uuid,uuid,bigint,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_case_list_row_v1(uuid,jsonb) owner to postgres;
revoke all on function private.bpay_next_case_list_row_v1(uuid,jsonb) from public,anon,authenticated,service_role;
