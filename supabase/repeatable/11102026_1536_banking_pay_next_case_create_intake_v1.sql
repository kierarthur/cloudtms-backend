-- FC-03 successor: existing creation owners, typed ordered intake, write-free Preview.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_case_create_terms_v1(p_input jsonb)
returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare
 v_keys text[]:=array['candidate_id','case_kind','case_subtype','tax_treatment','principal','due_date','start_monday','weekly_due','week_count','minimum_earnings_threshold','take_home_floor_override','reason','note','original_liability_reference'];v_key text;
 p_candidate_id uuid;
 p_case_kind text;p_case_subtype text;p_tax_treatment text;p_principal numeric;
 p_due_date date;p_start_monday date;p_weekly_due numeric;p_week_count bigint;
 p_minimum_earnings_threshold numeric;p_take_home_floor_override numeric;p_reason text;
 v_weekly numeric;v_weeks bigint;v_required_weeks numeric;v_last date;
begin
 if p_input is null or jsonb_typeof(p_input) is distinct from 'object' or octet_length(p_input::text)>12288
  or (select count(*) from jsonb_object_keys(p_input))<>cardinality(v_keys)
  or exists(select 1 from jsonb_object_keys(p_input) k where not(k=any(v_keys))) then
  raise exception using errcode='22023',message='BPAY_NEXT_CASE_CREATE_INPUT_INVALID';end if;
 foreach v_key in array v_keys loop
  if jsonb_typeof(p_input->v_key) is distinct from 'string'
    and not(v_key=any(array['due_date','start_monday','weekly_due','week_count','minimum_earnings_threshold','take_home_floor_override','note','original_liability_reference']) and p_input->v_key='null'::jsonb) then
   raise exception using errcode='22023',message='BPAY_NEXT_CASE_CREATE_INPUT_INVALID';end if;
  if v_key='candidate_id' and (p_input->>v_key)!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
   raise exception using errcode='22023',message='BPAY_NEXT_CASE_CREATE_INPUT_INVALID';end if;
  if v_key=any(array['principal','weekly_due','minimum_earnings_threshold','take_home_floor_override']) and p_input->>v_key is not null
    and (octet_length(p_input->>v_key)>1024 or (p_input->>v_key)!~'^(0|[1-9][0-9]*)([.][0-9]{1,2})?$') then
   raise exception using errcode='22023',message='BPAY_NEXT_CASE_CREATE_VALUE_INVALID';end if;
  if v_key=any(array['due_date','start_monday']) and p_input->>v_key is not null
    and ((p_input->>v_key)!~'^[0-9]{4}-[0-9]{2}-[0-9]{2}$' or left(p_input->>v_key,4)='0000') then
   raise exception using errcode='22023',message='BPAY_NEXT_CASE_CREATE_VALUE_INVALID';end if;
 end loop;
 if p_input->>'week_count' is not null and ((p_input->>'week_count')!~'^[1-9][0-9]{0,18}$'
   or (p_input->>'week_count')::numeric>9223372036854775807) then
  raise exception using errcode='22023',message='BPAY_NEXT_CASE_CREATE_VALUE_INVALID';end if;
 if p_input->>'note' is not null and octet_length(p_input->>'note')>2048
   or p_input->>'original_liability_reference' is not null and (p_input->>'case_kind'<>'MANUAL_DEBT'
    or octet_length(p_input->>'original_liability_reference') not between 1 and 2048 or btrim(p_input->>'original_liability_reference')='') then
  raise exception using errcode='22023',message='BPAY_NEXT_CASE_CREATE_INPUT_INVALID';end if;
 p_candidate_id:=(p_input->>'candidate_id')::uuid;p_case_kind:=p_input->>'case_kind';p_case_subtype:=p_input->>'case_subtype';
 p_tax_treatment:=p_input->>'tax_treatment';p_principal:=(p_input->>'principal')::numeric;p_due_date:=(p_input->>'due_date')::date;
 p_start_monday:=(p_input->>'start_monday')::date;p_weekly_due:=(p_input->>'weekly_due')::numeric;p_week_count:=(p_input->>'week_count')::bigint;
 p_minimum_earnings_threshold:=(p_input->>'minimum_earnings_threshold')::numeric;p_take_home_floor_override:=(p_input->>'take_home_floor_override')::numeric;p_reason:=p_input->>'reason';
  if p_candidate_id is null
     or p_case_kind is null or p_case_subtype is null or p_tax_treatment is null
     or p_principal is null or p_reason is null
     or pg_catalog.octet_length(p_reason) not between 1 and 2048
     or pg_catalog.btrim(p_reason)='' then
    raise exception using errcode='22023',message='BPAY_NEXT_CASE_CREATE_INPUT_INVALID';
  end if;
  if not ((p_case_kind='LOAN' and p_case_subtype='LOAN' and p_tax_treatment='NOT_APPLICABLE')
    or (p_case_kind='ADVANCE' and p_case_subtype='PAYMENT_ADVANCE' and p_tax_treatment='NOT_APPLICABLE')
    or (p_case_kind='MANUAL_DEBT' and p_case_subtype='MANUAL_DEBT' and p_tax_treatment in ('TAXABLE','NON_TAXABLE'))
    or (p_case_kind='CREDIT' and p_case_subtype='MANUAL_CREDIT' and p_tax_treatment in ('TAXABLE','NON_TAXABLE'))) then
    raise exception using errcode='22023',message='BPAY_NEXT_CASE_CREATE_FAMILY_INVALID';
  end if;
  -- Domain operands are validated without numeric typmod rounding. Do it before
  -- arithmetic so NaN/Infinity/subpenny/negative input cannot affect a schedule.
  if p_principal::text in ('NaN','Infinity','-Infinity') or p_principal<=0
     or p_principal<>pg_catalog.trunc(p_principal,2)
     or (p_weekly_due is not null and (p_weekly_due::text in ('NaN','Infinity','-Infinity')
       or p_weekly_due<=0 or p_weekly_due<>pg_catalog.trunc(p_weekly_due,2)))
     or (p_minimum_earnings_threshold is not null and (p_minimum_earnings_threshold::text in ('NaN','Infinity','-Infinity')
       or p_minimum_earnings_threshold<0 or p_minimum_earnings_threshold<>pg_catalog.trunc(p_minimum_earnings_threshold,2)))
     or (p_take_home_floor_override is not null and (p_take_home_floor_override::text in ('NaN','Infinity','-Infinity')
       or p_take_home_floor_override<0 or p_take_home_floor_override<>pg_catalog.trunc(p_take_home_floor_override,2)))
     or (p_due_date is not null and not pg_catalog.isfinite(p_due_date))
     or (p_start_monday is not null and (not pg_catalog.isfinite(p_start_monday) or extract(isodow from p_start_monday)<>1))
     or (p_week_count is not null and p_week_count<1) then
    raise exception using errcode='22023',message='BPAY_NEXT_CASE_CREATE_VALUE_INVALID';
  end if;
  if p_case_kind='CREDIT' then
    if p_start_monday is not null or p_weekly_due is not null or p_week_count is not null
       or p_minimum_earnings_threshold is not null or p_take_home_floor_override is not null then
      raise exception using errcode='22023',message='BPAY_NEXT_CASE_CREDIT_REPAYMENT_RULE_FORBIDDEN';
    end if;
  else
    if p_start_monday is null or (p_weekly_due is null and p_week_count is null) then
      raise exception using errcode='22023',message='BPAY_NEXT_CASE_REPAYMENT_INPUT_REQUIRED';
    end if;
    v_weekly:=coalesce(p_weekly_due,pg_catalog.ceil((p_principal/p_week_count)*100)/100);
    v_required_weeks:=greatest(pg_catalog.ceil(p_principal/v_weekly),1);
    if v_required_weeks>9223372036854775807 or (p_week_count is not null and p_week_count<v_required_weeks) then
      raise exception using errcode='22023',message='BPAY_NEXT_CASE_SCHEDULE_DOES_NOT_COVER_PRINCIPAL';
    end if;
    v_weeks:=coalesce(p_week_count,v_required_weeks::bigint);
  end if;

 if v_weeks is not null then
  if p_start_monday<date '0001-01-01' or p_start_monday>date '9999-12-31'
   or v_weeks-1>(date '9999-12-31'-p_start_monday)/7 then
   raise exception using errcode='22023',message='BPAY_NEXT_CASE_SCHEDULE_DATE_RANGE_INVALID';end if;
  v_last:=p_start_monday+((v_weeks-1)*7)::integer;
 end if;
 return jsonb_build_object('weekly_due',pg_catalog.trunc(v_weekly,2)::text,'week_count',v_weeks::text,'first_due_monday',p_start_monday,
  'last_due_monday',v_last,'total',p_principal::text,'minimum_earnings_threshold',pg_catalog.trunc(p_minimum_earnings_threshold,2)::text,
  'take_home_floor_override',pg_catalog.trunc(p_take_home_floor_override,2)::text);
end
$function$;

create or replace function private.bpay_next_case_create_context_v1(p_actor_user_id uuid,p_candidate_id uuid)
returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private,public
as $function$
declare v_context jsonb;
begin
 if p_actor_user_id is null or not exists(select 1 from public.tms_users where id=p_actor_user_id and is_active is true and role::text='admin') then
  raise exception using errcode='42501',message='BPAY_NEXT_CASE_CREATE_ACTOR_INVALID';end if;
 if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT') then
  raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
 select jsonb_build_object('actor_user_id',p_actor_user_id,'owner_epoch',m.owner_epoch::text,
   'financial_view_revision',coalesce(w.financial_view_revision,0)::text,'pay_channel',upper(c.pay_method::text),
   'candidate_default_floor',pg_catalog.trunc(c.min_take_home_wtd,2)::text,'valuation_policy_id',p.current_policy_id)
 into v_context from public.candidates c cross join private.bpay_next_module_control m
 cross join private.bpay_next_valuation_policy_control p left join private.bpay_next_worker_control w on w.candidate_id=c.id
 where c.id=p_candidate_id and c.active is true and upper(c.pay_method::text) in ('PAYE','UMBRELLA')
  and (c.min_take_home_wtd is null or (c.min_take_home_wtd::text not in ('NaN','Infinity','-Infinity')
    and c.min_take_home_wtd>=0 and c.min_take_home_wtd=pg_catalog.trunc(c.min_take_home_wtd,2)))
  and m.id=1 and m.active_owner='NEXT' and p.id=1;
 if v_context is null then raise exception using errcode='55000',message='BPAY_NEXT_CASE_CREATE_CONTEXT_UNAVAILABLE';end if;
 if v_context->>'valuation_policy_id' is null or v_context->>'candidate_default_floor' is not null
   and ((v_context->>'candidate_default_floor')::numeric<0 or (v_context->>'candidate_default_floor')::numeric<>trunc((v_context->>'candidate_default_floor')::numeric,2)
     or (v_context->>'candidate_default_floor') in ('NaN','Infinity','-Infinity')) then
  raise exception using errcode='55000',message='BPAY_NEXT_CASE_CREATE_CONTEXT_UNAVAILABLE';end if;
 return v_context;
end
$function$;

create or replace function private.bpay_next_case_create_preview_v1(p_actor_user_id uuid,p_input jsonb)
returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare v_terms jsonb;v_context jsonb;v_warnings jsonb:='[]'::jsonb;
begin
 v_terms:=private.bpay_next_case_create_terms_v1(p_input);
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

create or replace function private.bpay_next_accept_case_create_v1(
  p_command_id uuid,p_candidate_id uuid,p_actor_user_id uuid,
  p_case_kind text,p_case_subtype text,p_tax_treatment text,p_principal numeric,
  p_due_date date,p_start_monday date,p_weekly_due numeric,p_week_count bigint,
  p_minimum_earnings_threshold numeric,p_take_home_floor_override numeric,p_reason text
) returns jsonb
language plpgsql security invoker
set search_path=pg_catalog,private,public
as $function$
begin
 return private.bpay_next_accept_case_create_v1(p_command_id,p_candidate_id,p_actor_user_id,p_case_kind,p_case_subtype,
  p_tax_treatment,p_principal,p_due_date,p_start_monday,p_weekly_due,p_week_count,p_minimum_earnings_threshold,
  p_take_home_floor_override,p_reason,null,null,null);
end
$function$;

create or replace function private.bpay_next_apply_case_create_v1(p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint)
returns jsonb
language plpgsql security invoker
set search_path=pg_catalog,private
as $function$
declare
  v_epoch bigint;v_candidate uuid;v_control private.bpay_next_worker_control%rowtype;
  v_job private.bpay_next_job%rowtype;v_request private.bpay_next_case_create_request%rowtype;
  v_kind text;v_direction text;v_stage text;v_period date;v_opening_due numeric;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null then
    raise exception using errcode='22023',message='BPAY_NEXT_CASE_CREATE_APPLY_INPUT_INVALID';
  end if;
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select j.candidate_id into strict v_candidate from private.bpay_next_job j where j.id=p_job_id;
  select w.* into strict v_control from private.bpay_next_worker_control w where w.candidate_id=v_candidate for update;
  select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id for update;
  select r.* into strict v_request from private.bpay_next_case_create_request r
    where r.command_id=v_job.command_id and r.candidate_id=v_candidate;
  if v_job.job_kind<>'CASE_CREATE' or v_job.module_epoch<>v_epoch then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_CREATE_JOB_SCOPE_INVALID';
  end if;
  if v_job.status='DONE' and v_job.phase='DONE' then
    if not exists(select 1 from private.bpay_next_case_event e where e.id=v_request.command_id
      and e.case_id=v_request.case_id and e.event_kind='APPROVED'
      and e.approved_delta=v_request.principal_source_ex_vat) then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_CREATE_TERMINAL_EVIDENCE_MISSING';
    end if;
    return pg_catalog.jsonb_build_object('phase','CASE_CREATED','case_id',v_request.case_id,'replay',true);
  end if;
  if v_job.status<>'LEASED' or v_job.phase<>'NEW' or v_job.lease_nonce<>p_lease_nonce
     or v_job.owner_epoch<>p_owner_epoch or v_control.active_owner_epoch<>p_owner_epoch
     or v_job.lease_until_utc<=pg_catalog.clock_timestamp() then
    raise exception using errcode='55000',message='BPAY_NEXT_CASE_CREATE_LEASE_STALE';
  end if;
  insert into private.bpay_next_finance_case(id,candidate_id,case_kind,tax_treatment,status,principal_approved,due_date,order_key,case_revision)
    values(v_request.case_id,v_candidate,v_request.case_kind,v_request.tax_treatment,'OPEN',v_request.principal_source_ex_vat,
      v_request.due_date,pg_catalog.floor(extract(epoch from v_request.accepted_at_utc)*1000000)::bigint,1);
  insert into private.bpay_next_case_rule
    (id,case_id,candidate_id,rule_revision,case_kind,case_subtype,tax_treatment,case_created_at_utc,
     minimum_earnings_threshold,take_home_floor_override,weekly_due_source_ex_vat,schedule_start_monday,next_due_monday,schedule_week_count)
    values(v_request.rule_id,v_request.case_id,v_candidate,1,v_request.case_kind,v_request.case_subtype,v_request.tax_treatment,
      v_request.accepted_at_utc,v_request.minimum_earnings_threshold,v_request.take_home_floor_override,v_request.weekly_due_source_ex_vat,
      v_request.input_start_monday,v_request.input_start_monday,v_request.schedule_week_count);
  update private.bpay_next_finance_case set current_rule_id=v_request.rule_id,current_rule_revision=1 where id=v_request.case_id;
  if v_request.case_kind in ('LOAN','ADVANCE') then v_kind:='PAYOUT';v_direction:='PAYMENT';v_stage:='NET_ADD';
  elsif v_request.case_kind='CREDIT' then v_kind:='CREDIT';v_direction:='PAYMENT';v_stage:=case when v_request.tax_treatment='TAXABLE' then 'GROSS_ADD' else 'NET_ADD' end;
  else v_kind:='RECOVERY';v_direction:='DEDUCTION';v_stage:=case when v_request.tax_treatment='TAXABLE' then 'GROSS_DEDUCT' else 'NET_DEDUCT' end;end if;
  insert into private.bpay_next_case_component
    (id,case_id,candidate_id,component_key,component_ordinal,component_revision,rule_id,case_kind,case_subtype,tax_treatment,
     instruction_kind,direction,payroll_stage,source_pay_channel,currency,approved_source_ex_vat,resolution_state)
    values(v_request.primary_component_id,v_request.case_id,v_candidate,v_kind,1,1,v_request.rule_id,v_request.case_kind,
      v_request.case_subtype,v_request.tax_treatment,v_kind,v_direction,v_stage,v_request.source_pay_channel,'GBP',v_request.principal_source_ex_vat,'REVIEW');
  v_period:=case when v_request.case_kind='MANUAL_DEBT' then v_request.input_start_monday else v_request.opening_pay_week_start end;
  v_opening_due:=case when v_request.case_kind='MANUAL_DEBT' then least(v_request.weekly_due_source_ex_vat,v_request.principal_source_ex_vat) else 0 end;
  insert into private.bpay_next_case_period
    (case_component_id,case_id,candidate_id,pay_week_start,rule_id,period_revision,opening_outstanding_source_ex_vat,opening_due_source_ex_vat)
    values(v_request.primary_component_id,v_request.case_id,v_candidate,v_period,v_request.rule_id,1,v_request.principal_source_ex_vat,v_opening_due);
  if v_request.recovery_component_id is not null then
    -- Two instructions describe ONE principal. Neither funded balance is
    -- effective until the original payout settlement owner records FUNDED.
    insert into private.bpay_next_case_component
      (id,case_id,candidate_id,component_key,component_ordinal,component_revision,rule_id,case_kind,case_subtype,tax_treatment,
       instruction_kind,direction,payroll_stage,source_pay_channel,currency,approved_source_ex_vat,resolution_state)
      values(v_request.recovery_component_id,v_request.case_id,v_candidate,'RECOVERY',2,1,v_request.rule_id,v_request.case_kind,
        v_request.case_subtype,v_request.tax_treatment,'RECOVERY','DEDUCTION','NET_DEDUCT',v_request.source_pay_channel,'GBP',v_request.principal_source_ex_vat,'REVIEW');
    insert into private.bpay_next_case_period
      (case_component_id,case_id,candidate_id,pay_week_start,rule_id,period_revision,opening_outstanding_source_ex_vat,opening_due_source_ex_vat)
      values(v_request.recovery_component_id,v_request.case_id,v_candidate,v_request.input_start_monday,v_request.rule_id,1,0,0);
  end if;
  insert into private.bpay_next_case_event(id,case_id,operation_id,operation_item_id,event_kind,approved_delta,occurred_at_utc)
    values(v_request.command_id,v_request.case_id,v_request.command_id,v_request.primary_component_id,'APPROVED',v_request.principal_source_ex_vat,v_request.accepted_at_utc);
  update private.bpay_next_worker_control set financial_view_revision=financial_view_revision+1,
    updated_at_utc=pg_catalog.transaction_timestamp() where candidate_id=v_candidate;
  perform private.bpay_next_workbench_refresh_case_v1(v_request.case_id);
  update private.bpay_next_job set status='DONE',phase='DONE',lease_nonce=null,lease_until_utc=null where id=p_job_id;
  update private.bpay_next_command set status='COMPLETE' where id=v_job.command_id;
  return pg_catalog.jsonb_build_object('phase','CASE_CREATED','case_id',v_request.case_id,'replay',false);
end
$function$;

create or replace function public.bpay_next_financial_intake_v1(
  p_actor_user_id uuid,p_action text,p_args jsonb
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_keys text[];
  v_key text;
  v_command_id uuid;
  v_worker_id uuid;
  v_transfer_id uuid;
  v_instruction_id uuid;
  v_return_cash_id uuid;
  v_run_id uuid;
  v_amount numeric;
  v_occurred_at timestamptz;
  v_result jsonb;
  v_output jsonb;
  v_sequence text;
  v_projection_revision bigint;
  v_case_selection_revision bigint;
  v_case_component_id uuid;
  v_component_revision bigint;
  v_proof_id uuid;
  v_reauth_scope jsonb;
  v_protected boolean;
begin
  if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),
      nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
     or p_actor_user_id is null
     or not exists(select 1 from public.tms_users u
                   where u.id=p_actor_user_id and u.is_active is true
                     and u.role::text='admin' for share) then
    raise exception using errcode='42501',message='BPAY_NEXT_COMMAND_FORBIDDEN';
  end if;
  if not exists(select 1 from private.bpay_next_module_control m
                where m.id=1 and m.active_owner='NEXT' for share) then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  if p_action='CASE_CREATE' then
    if p_args is null or jsonb_typeof(p_args) is distinct from 'object' or octet_length(p_args::text)>32768
      or (select count(*) from jsonb_object_keys(p_args))<>16
      or not(p_args ? 'command_id') or not(p_args ? 'preview_token')
      or jsonb_typeof(p_args->'command_id') is distinct from 'string'
      or (p_args->>'command_id')!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
      or p_args->'preview_token' is null or p_args->'preview_token'='null'::jsonb
      or (p_args-'command_id'-'preview_token') is distinct from (p_args->'preview_token'->'input') then
      raise exception using errcode='22023',message='BPAY_NEXT_CASE_CREATE_INPUT_INVALID';end if;
    perform private.bpay_next_case_create_terms_v1(p_args-'command_id'-'preview_token');
    v_result:=private.bpay_next_accept_case_create_v1((p_args->>'command_id')::uuid,(p_args->>'candidate_id')::uuid,p_actor_user_id,
      p_args->>'case_kind',p_args->>'case_subtype',p_args->>'tax_treatment',(p_args->>'principal')::numeric,
      (p_args->>'due_date')::date,(p_args->>'start_monday')::date,(p_args->>'weekly_due')::numeric,(p_args->>'week_count')::bigint,
      (p_args->>'minimum_earnings_threshold')::numeric,(p_args->>'take_home_floor_override')::numeric,
      p_args->>'reason',p_args->>'note',p_args->>'original_liability_reference',p_args->'preview_token');
    v_output:=pg_catalog.jsonb_build_object('case_id',v_result->'case_id','command_id',v_result->'command_id',
      'sequence',v_result->'sequence','phase',v_result->'phase','replay',v_result->'replay');
    return v_output;
  end if;
  if p_action='RETRY_WORK_JOB' then
    return jsonb_build_object('action',p_action,'result',
      private.bpay_next_retry_work_job_v1(p_actor_user_id,p_args,false));
  end if;
  if p_action is null or p_action not in
      ('PAYE_NET','CASE_PAYOUT','TRANSFER_BEGIN','CANCEL_REQUEST','CASH_REISSUE','REISSUE_CANCEL',
       'CSV_ISSUE','CSV_SETTLEMENT','CSV_RETURN','INTERNAL_SETTLE','PREPARATION_EXPIRE','WRITE_OFF')
     or p_args is null or pg_catalog.jsonb_typeof(p_args) is distinct from 'object'
     or pg_catalog.octet_length(p_args::text)>2048 then
    raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
  end if;
  v_keys:=case p_action
    when 'WRITE_OFF' then array['command_id','case_component_id','expected_component_revision','scope','reason']
      || case when p_args->>'scope'='AMOUNT' then array['amount'] else array[]::text[] end
    when 'PAYE_NET' then array['command_id','run_worker_id','entered_paye_net']
      || case when p_args ? 'expected_projection_revision' then array['expected_projection_revision'] else array[]::text[] end
    when 'CASE_PAYOUT' then array['command_id','run_worker_id','expected_projection_revision']
    when 'TRANSFER_BEGIN' then array['command_id','run_worker_id']
    when 'CANCEL_REQUEST' then array['command_id','run_worker_id']
      || case when p_args ? 'run_id' or p_args ? 'pay_channel'
        then array['run_id','pay_channel'] else array[]::text[] end
    when 'CASH_REISSUE' then array['command_id','return_cash_id']
    when 'REISSUE_CANCEL' then array['command_id','transfer_id']
    when 'CSV_ISSUE' then array['instruction_id','transfer_id']
    when 'INTERNAL_SETTLE' then array['command_id','transfer_id']
    when 'PREPARATION_EXPIRE' then array['command_id','run_id','expected_deadline']
    else array['command_id','transfer_id','receipt_id','amount','occurred_at'] end;
  v_protected:=p_action in ('WRITE_OFF','CANCEL_REQUEST','CASH_REISSUE','REISSUE_CANCEL','CSV_ISSUE','CSV_RETURN');
  if v_protected then
    if p_args ? 'reauth_proof_id' then v_keys:=v_keys||array['reauth_proof_id'];
    elsif p_action<>'CSV_ISSUE' then
      raise exception using errcode='42501',message='BPAY_NEXT_REAUTH_REQUIRED';end if;
  end if;
  if (select count(*) from pg_catalog.jsonb_object_keys(p_args))<>pg_catalog.array_length(v_keys,1)
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_args) k
               where not(k=any(v_keys))) then
    raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
  end if;
  foreach v_key in array v_keys loop
    if pg_catalog.jsonb_typeof(p_args->v_key) is distinct from 'string' then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
    if v_key in ('command_id','run_worker_id','transfer_id','instruction_id','return_cash_id','run_id','case_component_id','reauth_proof_id')
       and (p_args->>v_key)!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
    if v_key='pay_channel' and (p_args->>v_key) not in ('PAYE','UMBRELLA') then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
  end loop;
  v_command_id:=(p_args->>'command_id')::uuid;
  v_worker_id:=(p_args->>'run_worker_id')::uuid;
  v_transfer_id:=(p_args->>'transfer_id')::uuid;
  v_instruction_id:=(p_args->>'instruction_id')::uuid;
  v_return_cash_id:=(p_args->>'return_cash_id')::uuid;
  if p_action='WRITE_OFF' then
    v_case_component_id:=(p_args->>'case_component_id')::uuid;
    if (p_args->>'expected_component_revision')!~'^[1-9][0-9]{0,18}$'
       or (p_args->>'expected_component_revision')::numeric>9223372036854775807 then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
    v_component_revision:=(p_args->>'expected_component_revision')::bigint;
  end if;
  if p_action='PREPARATION_EXPIRE' then
    v_run_id:=(p_args->>'run_id')::uuid;
    if (p_args->>'expected_deadline')!~
        '^[0-9]{4}-[0-9]{2}-[0-9]{2}T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]([.][0-9]{1,6})?(Z|[+-]([01][0-9]|2[0-3]):[0-5][0-9])$' then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
    v_occurred_at:=(p_args->>'expected_deadline')::timestamptz;
    if not pg_catalog.isfinite(v_occurred_at) then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
  end if;
  if p_args ? 'expected_projection_revision' then
    if (p_args->>'expected_projection_revision')!~'^(0|[1-9][0-9]{0,18})$'
       or (p_args->>'expected_projection_revision')::numeric>=9223372036854775807 then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
    v_projection_revision:=(p_args->>'expected_projection_revision')::bigint;
  end if;
  if p_action in ('PAYE_NET','CSV_SETTLEMENT','CSV_RETURN') then
    v_key:=case when p_action='PAYE_NET' then 'entered_paye_net' else 'amount' end;
    -- Current money columns are numeric(18,2). Explicit finite, ordinary
    -- decimal syntax prevents exponent/NaN, narrowing and JSON-number coercion.
    if (p_args->>v_key)!~'^(0|[1-9][0-9]{0,15})([.][0-9]{1,2})?$' then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
    v_amount:=(p_args->>v_key)::numeric;
    if p_action<>'PAYE_NET' and v_amount<=0 then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
  end if;
  if p_action in ('CSV_SETTLEMENT','CSV_RETURN') then
    if pg_catalog.octet_length(p_args->>'receipt_id') not between 1 and 256
       or (p_args->>'occurred_at')!~
         '^[0-9]{4}-[0-9]{2}-[0-9]{2}T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]([.][0-9]{1,6})?(Z|[+-]([01][0-9]|2[0-3]):[0-5][0-9])$' then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
    v_occurred_at:=(p_args->>'occurred_at')::timestamptz;
    if not pg_catalog.isfinite(v_occurred_at) then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
  end if;

  if v_protected then
    -- Shared finite resolver takes the native business locks before recomputing
    -- scope. Same original receipt qualifies consumed replay, never live repricing.
    v_proof_id:=(p_args->>'reauth_proof_id')::uuid;
    v_reauth_scope:=private.bpay_next_reauth_scope_v1(p_actor_user_id,p_action,p_args-'reauth_proof_id',true);
    if not (p_action='CSV_ISSUE' and (v_reauth_scope->>'accepted')::boolean and v_proof_id is null) then
      perform private.bpay_next_reauth_consume_v1(v_proof_id,p_actor_user_id,p_action,
        v_reauth_scope->>'scope_sha256',coalesce(v_command_id,v_instruction_id));
    end if;
  end if;

  -- Fixed branches and exact typed IDs only. No dynamic identifier, query,
  -- caller actor, arbitrary RPC, live finance/history reconstruction or drain.
  case p_action
    when 'WRITE_OFF' then
      v_result:=private.bpay_next_accept_write_off_v1(v_command_id,v_case_component_id,p_actor_user_id,
        v_component_revision,p_args->>'scope',p_args->>'amount',p_args->>'reason');
      v_output:=pg_catalog.jsonb_build_object('command_id',v_result->>'command_id',
        'case_id',v_result->>'case_id','case_component_id',v_result->>'case_component_id',
        'sequence',v_result->>'sequence','phase',v_result->>'phase','requested_scope',v_result->>'requested_scope',
        'applied_amount',v_result->>'applied_amount','remaining_amount',v_result->>'remaining_amount',
        'protected_amount',v_result->>'protected_amount','event_id',v_result->>'event_id',
        'issue_code',v_result->>'issue_code','replay',v_result->'replay');
    when 'PREPARATION_EXPIRE' then
      -- NULL clock selects the real server clock AFTER acquiring the header.
      -- Office cannot supply SYSTEM authority, now, or a renewal duration.
      v_result:=private.bpay_next_accept_preparation_expiry_v1(
        v_command_id,v_run_id,p_actor_user_id,v_occurred_at,null);
      v_output:=pg_catalog.jsonb_build_object(
        'command_id',v_result->>'command_id','run_id',v_result->>'run_id',
        'sequence',v_result->>'sequence','phase',v_result->>'phase',
        'expected_candidates',v_result->>'expected_candidates',
        'completed_candidates',v_result->>'completed_candidates',
        'replay',v_result->'replay');
    when 'PAYE_NET' then
      -- Route by the worker's FROZEN selection, never by live Candidate cases.
      -- Explicit prior projection protects resaves from overwriting a newer one.
      select w.case_selection_revision into strict v_case_selection_revision
        from private.bpay_next_run_worker w where w.id=v_worker_id;
      if v_case_selection_revision>0 then
        if v_projection_revision is null then
          raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
        end if;
        v_result:=private.bpay_next_accept_case_paye_net_v1(
          v_command_id,v_worker_id,p_actor_user_id,v_amount,v_projection_revision);
      else
        if v_projection_revision is not null then
          raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
        end if;
        v_result:=private.bpay_next_accept_simple_paye_net_v1(v_command_id,v_worker_id,v_amount);
      end if;
      v_output:=pg_catalog.jsonb_build_object(
        'command_id',v_command_id,'run_worker_id',v_worker_id,
        'sequence',v_result->>'sequence','request_no',v_result->>'request_no',
        'phase',v_result->>'phase','replay',v_result->'replay');
    when 'CASE_PAYOUT' then
      v_result:=private.bpay_next_accept_case_payout_projection_v1(
        v_command_id,v_worker_id,p_actor_user_id,v_projection_revision);
      v_output:=pg_catalog.jsonb_build_object(
        'command_id',v_command_id,'run_worker_id',v_worker_id,
        'sequence',v_result->>'sequence','request_no',v_result->>'request_no',
        'phase',v_result->>'phase','replay',v_result->'replay');
    when 'TRANSFER_BEGIN' then
      v_result:=private.bpay_next_begin_simple_transfer_v1(v_command_id,v_worker_id);
      v_output:=pg_catalog.jsonb_build_object(
        'command_id',v_command_id,'run_worker_id',v_worker_id,
        'sequence',v_result->>'sequence','transfer_id',v_result->>'transfer_id',
        'phase',v_result->>'phase','replay',v_result->'replay');
    when 'CANCEL_REQUEST' then
      -- Optional address is strictly paired and independently bound before accepting intent.
      if p_args ? 'run_id' and not exists(select 1 from private.bpay_next_run_worker w
        where w.id=v_worker_id and w.run_id=(p_args->>'run_id')::uuid
          and w.target_pay_channel=p_args->>'pay_channel') then
        raise exception using errcode='23514',message='BPAY_NEXT_CANCEL_CHANNEL_SCOPE_INVALID';
      end if;
      v_result:=private.bpay_next_accept_simple_cancel_v1(v_command_id,v_worker_id);
      v_output:=pg_catalog.jsonb_build_object(
        'command_id',v_command_id,'run_worker_id',v_worker_id,
        'sequence',v_result->>'sequence','phase',v_result->>'phase',
        'cursor',v_result->>'cursor','released_line_count',v_result->>'released_line_count',
        'blocked_code',v_result->>'blocked_code','replay',v_result->'replay');
    when 'CASH_REISSUE' then
      v_result:=private.bpay_next_receive_simple_reissue_v1(v_command_id,v_return_cash_id,p_actor_user_id);
      select c.agency_sequence::text into strict v_sequence
        from private.bpay_next_command c where c.id=v_command_id and c.command_kind='CASH_REISSUE';
      v_output:=pg_catalog.jsonb_build_object(
        'command_id',v_command_id,'return_cash_id',v_return_cash_id,
        'sequence',v_sequence,'transfer_id',v_result->>'transfer_id',
        'phase',case when v_result->>'transfer_id' is null
          then 'ACCEPTED_PENDING_BUILD' else 'TRANSFER_CREATED' end,
        'replay',v_result->'replay');
    when 'REISSUE_CANCEL' then
      v_result:=private.bpay_next_accept_reissue_cancel_v1(v_command_id,v_transfer_id,p_actor_user_id);
      select r.blocked_code into v_key from private.bpay_next_reissue_cancel_request r where r.command_id=v_command_id;
      v_output:=pg_catalog.jsonb_build_object('command_id',v_command_id,'transfer_id',v_transfer_id,
        'sequence',v_result->>'sequence','phase',v_result->>'phase','blocked_code',v_key,'replay',v_result->'replay');
    when 'CSV_ISSUE' then
      -- A new CSV issue first binds its current approved destination using the
      -- existing owner. Already-issued replay never rebinds or changes bytes.
      -- Both calls use the same transaction and parent run mutex; any issue
      -- permission/state failure rolls the destination preparation back too.
      -- Acquire that mutex BEFORE checking instruction existence: concurrent
      -- same-ID retry must see the first committed issue and skip rebinding.
      select w.run_id into strict v_run_id
        from private.bpay_next_transfer t
        join private.bpay_next_run_worker w on w.id=t.run_worker_id
        where t.id=v_transfer_id;
      perform 1 from private.bpay_next_pay_run where id=v_run_id for update;
      if not exists(select 1 from private.bpay_next_csv_instruction i where i.id=v_instruction_id) then
        perform private.bpay_next_bind_simple_csv_destination_v1(v_transfer_id);
      end if;
      v_result:=private.bpay_next_issue_simple_csv_v1(v_instruction_id,v_transfer_id,p_actor_user_id);
      v_output:=pg_catalog.jsonb_build_object(
        'instruction_id',v_result->>'instruction_id','transfer_id',v_result->>'transfer_id',
        'file_name',v_result->>'file_name','csv_sha256',v_result->>'csv_sha256',
        'csv_text',v_result->>'csv_text','phase','ISSUED_CSV',
        'payment_recorded',v_result->'payment_recorded','replay',v_result->'replay');
    when 'CSV_SETTLEMENT' then
      v_result:=private.bpay_next_receive_simple_csv_settlement_v1(
        v_command_id,v_transfer_id,p_args->>'receipt_id',v_amount,v_occurred_at,p_actor_user_id);
      select c.agency_sequence::text into strict v_sequence
        from private.bpay_next_command c
        where c.id=(v_result->>'command_id')::uuid and c.command_kind='CSV_SETTLEMENT';
      v_output:=pg_catalog.jsonb_build_object(
        'command_id',v_result->>'command_id','transfer_id',v_transfer_id,
        'outcome_id',v_result->>'outcome_id','sequence',v_sequence,
        'posting_complete',v_result->'posting_complete','replay',v_result->'replay',
        'phase',case when (v_result->>'posting_complete')::boolean
          then 'POSTED' else 'ACCEPTED_PENDING_POSTING' end);
    when 'INTERNAL_SETTLE' then
      -- The server creates an internal receipt for an exact zero-cash frozen
      -- envelope. There is no caller amount, bank receipt or CSV instruction.
      v_result:=private.bpay_next_receive_internal_settlement_v1(
        v_command_id,v_transfer_id,p_actor_user_id);
      v_output:=pg_catalog.jsonb_build_object(
        'command_id',v_result->>'command_id','transfer_id',v_result->>'transfer_id',
        'internal_receipt_id',v_result->>'internal_receipt_id','sequence',v_result->>'sequence',
        'posting_complete',v_result->'posting_complete','phase',v_result->>'phase','replay',v_result->'replay');
    when 'CSV_RETURN' then
      v_result:=private.bpay_next_receive_simple_csv_return_v1(
        v_command_id,v_transfer_id,p_args->>'receipt_id',v_amount,v_occurred_at,p_actor_user_id);
      select c.agency_sequence::text into strict v_sequence
        from private.bpay_next_command c
        where c.id=(v_result->>'command_id')::uuid and c.command_kind='CSV_RETURN';
      v_output:=pg_catalog.jsonb_build_object(
        'command_id',v_result->>'command_id','transfer_id',v_transfer_id,
        'outcome_id',v_result->>'outcome_id','sequence',v_sequence,
        'posting_complete',v_result->'posting_complete','replay',v_result->'replay',
        'phase',case when (v_result->>'posting_complete')::boolean
          then 'POSTED' else 'ACCEPTED_PENDING_POSTING' end);
  end case;
  if pg_catalog.jsonb_typeof(v_result->'replay') is distinct from 'boolean'
     or pg_catalog.octet_length(v_output::text)>120000 then
    -- Exception aborts the whole request transaction; no partial issue or
    -- accepted financial request is committed without its bounded receipt.
    raise exception using errcode='54000',message='BPAY_NEXT_COMMAND_RESPONSE_INVALID';
  end if;
  return v_output;
end
$function$;

create or replace function public.bpay_next_preparation_page_v1(p_actor_user_id uuid,p_kind text,p_args jsonb)
returns jsonb language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_keys text[];v_work uuid;v_revision uuid;v_line uuid;v_shift uuid;v_limit integer;
  v_progress jsonb;v_row jsonb;v_page jsonb[];v_rows jsonb:='[]'::jsonb;v_n integer:=0;v_complete boolean:=true;
begin
  if p_kind='CASE_PREVIEW' then
    -- FC-03 genuinely read-only: no authority helper, receipt, business lock or wake.
    if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
      raise exception using errcode='42501',message='BPAY_NEXT_PREPARATION_FORBIDDEN';end if;
    if p_args is null or pg_catalog.jsonb_typeof(p_args) is distinct from 'object' or (p_args-'input') is distinct from '{}'::jsonb
      or not(p_args ? 'input') then raise exception using errcode='22023',message='BPAY_NEXT_CASE_CREATE_INPUT_INVALID';end if;
    return pg_catalog.jsonb_build_object('rows',pg_catalog.jsonb_build_array(private.bpay_next_case_create_preview_v1(p_actor_user_id,p_args->'input')),'complete',true);
  end if;
  if p_kind='GUARD' then
   -- Read-only guard takes no business locks or mutating authority helper.
   if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
    or p_actor_user_id is null or not exists(select 1 from public.tms_users where id=p_actor_user_id and is_active is true and role::text='admin') then
    raise exception using errcode='42501',message='BPAY_NEXT_PREPARATION_FORBIDDEN';end if;
   if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT') then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
   v_keys:=array['run_id']||case when p_args ? 'descriptor' then array['descriptor'] else array[]::text[] end;
   perform private.bpay_next_preparation_args_v1(p_args,v_keys);
   return pg_catalog.jsonb_build_object('rows',pg_catalog.jsonb_build_array(private.bpay_next_paye_guard_page_v1(p_actor_user_id,(p_args->>'run_id')::uuid,p_args->'descriptor')),'complete',true);
  end if;
  perform private.bpay_next_preparation_authority_v1(p_actor_user_id);
  v_keys:=case p_kind
    when 'PROGRESS' then array['run_id']
    when 'COMPONENTS' then array['work_id','expected_revision_id','after_line_no','limit']
    when 'WORK_CHOICES' then case when p_args ? 'accepted_inputs' then array['run_id','accepted_inputs','after_member_no','limit']||case when p_args ? 'member_kind' then array['member_kind'] else array[]::text[] end else array['run_id','after_work_id','limit'] end
    when 'SELECTED_COMPONENTS' then array['run_id','work_id','after_selection_no','limit']
    when 'CHANNEL_DRAFTS' then array['run_id']
    when 'CHANNEL_WORKERS' then array['run_id','pay_channel','after_candidate_id','limit']
    when 'CHANNEL_STATUS' then array['run_id','pay_channel','run_worker_id','after_transfer_no','limit']
    when 'WORKERS' then array['run_id','after_candidate_id','limit']
    when 'REVISION_COMPARISON' then array['run_worker_id','after_work_id','limit']
    when 'LINES' then array['run_worker_id','after_line_no','limit']
    when 'SHIFTS' then array['work_id','expected_revision_id','approved_line_id','after_detail_no','limit']
    when 'BREAKS' then array['work_id','expected_revision_id','approved_line_id','shift_detail_id','after_break_no','limit']
    when 'RATES' then array['work_id','expected_revision_id','approved_line_id','after_bucket','limit']
    when 'CURRENT_WORK' then array['candidate_id','after_week','after_work_id','limit']
    when 'CURRENT_CASES' then array['candidate_id','after_case_id','limit']
    when 'CURRENT_STORED_CREDITS' then array['candidate_id','after_legacy_case_id','limit']
    when 'CASE_COMPONENTS' then array['candidate_id','case_id','after_component_ordinal','limit']
    when 'STATUS' then array['run_id','run_worker_id','after_transfer_no','limit']
    when 'RETURN_CASH' then array['run_id','run_worker_id','after_transfer_no','limit'] else null end;
  if v_keys is null then raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
  perform private.bpay_next_preparation_args_v1(case when p_kind='WORK_CHOICES' and p_args ? 'accepted_inputs' then p_args-'member_kind' else p_args end,array_remove(v_keys,'member_kind'));
  if p_kind='WORK_CHOICES' and p_args ? 'accepted_inputs' then
    if p_args->'accepted_inputs' is distinct from 'true'::jsonb or (p_args ? 'member_kind' and (pg_catalog.jsonb_typeof(p_args->'member_kind') is distinct from 'string' or p_args->>'member_kind' not in ('WORK','CASE_COMPONENT'))) then raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
    perform private.bpay_next_selection_page_authority_v1((p_args->>'run_id')::uuid,p_actor_user_id);
    return private.bpay_next_choice_inputs_page_v1((p_args->>'run_id')::uuid,(p_args->>'after_member_no')::bigint,(p_args->>'limit')::integer,coalesce(p_args->>'member_kind','WORK'));
  end if;
  if p_kind='PROGRESS' then
    v_progress:=private.bpay_next_preparation_progress_v1((p_args->>'run_id')::uuid);
    return pg_catalog.jsonb_build_object('rows',pg_catalog.jsonb_build_array(v_progress),'complete',true);
  end if;
  v_work:=(p_args->>'work_id')::uuid;v_revision:=(p_args->>'expected_revision_id')::uuid;
  v_line:=(p_args->>'approved_line_id')::uuid;v_shift:=(p_args->>'shift_detail_id')::uuid;v_limit:=(p_args->>'limit')::integer;
  if p_kind='CHANNEL_DRAFTS' then
    return private.bpay_next_channel_drafts_page_v1((p_args->>'run_id')::uuid);
  elsif p_kind='CHANNEL_WORKERS' then
    return private.bpay_next_channel_worker_page_v1((p_args->>'run_id')::uuid,
      p_args->>'pay_channel',(p_args->>'after_candidate_id')::uuid,v_limit);
  elsif p_kind='CHANNEL_STATUS' then
    if p_args->'after_transfer_no'<>'null'::jsonb and ((p_args->>'after_transfer_no')::numeric<1
      or (p_args->>'after_transfer_no')::numeric>2147483647) then
      raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
    end if;
    return private.bpay_next_channel_status_page_v1((p_args->>'run_id')::uuid,p_args->>'pay_channel',
      (p_args->>'run_worker_id')::uuid,(p_args->>'after_transfer_no')::integer,v_limit);
  end if;
  -- Only the two exact factual0580 owners; no competing reader or financial
  -- history/calculation path. CURRENT_WORK does not certify payability.
  if p_kind='CURRENT_WORK' then
    return private.bpay_next_current_work_page_v1((p_args->>'candidate_id')::uuid,
      (p_args->>'after_week')::date,(p_args->>'after_work_id')::uuid,v_limit);
  elsif p_kind='CURRENT_CASES' then
    return private.bpay_next_current_case_page_v1((p_args->>'candidate_id')::uuid,
      (p_args->>'after_case_id')::uuid,v_limit);
  elsif p_kind='CURRENT_STORED_CREDITS' then
    -- Factual, exact-Candidate possible origins only; binding revalidates the
    -- retained creator/component/account facts. This is not pay eligibility.
    -- Closed raw reply includes scan_cursor from the bounded raw window;
    -- filtering can produce an empty nonterminal page without restarting it.
    return private.bpay_next_current_stored_credit_page_v1((p_args->>'candidate_id')::uuid,
      (p_args->>'after_legacy_case_id')::uuid,v_limit);
  elsif p_kind='CASE_COMPONENTS' then
    return private.bpay_next_current_case_component_page_v1((p_args->>'candidate_id')::uuid,
      (p_args->>'case_id')::uuid,(p_args->>'after_component_ordinal')::bigint,v_limit);
  elsif p_kind in ('STATUS','RETURN_CASH') then
    if p_args->'after_transfer_no'<>'null'::jsonb and ((p_args->>'after_transfer_no')::numeric<1
      or (p_args->>'after_transfer_no')::numeric>2147483647) then
      raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
    end if;
    if p_kind='RETURN_CASH' then
      return private.bpay_next_return_cash_page_v1((p_args->>'run_id')::uuid,
        (p_args->>'run_worker_id')::uuid,(p_args->>'after_transfer_no')::integer,v_limit);
    end if;
    return private.bpay_next_worker_status_page_v1((p_args->>'run_id')::uuid,
      (p_args->>'run_worker_id')::uuid,(p_args->>'after_transfer_no')::integer,v_limit);
  end if;
  if p_kind in ('COMPONENTS','SHIFTS','BREAKS','RATES') then
    if not exists(select 1 from private.bpay_next_work w
      join private.bpay_next_work_revision r on r.work_id=w.id and r.id=v_revision
      where w.id=v_work and w.approval_state='APPROVED' and w.current_revision_id=v_revision
        and w.applied_revision_id=v_revision and r.sealed_at_utc is not null and r.approved_at_utc is not null) then
      raise exception using errcode='55000',message='BPAY_NEXT_CHOICE_REVISION_NOT_CURRENT';
    end if;
    if p_kind<>'COMPONENTS' and not exists(select 1 from private.bpay_next_approved_line l where l.id=v_line and l.revision_id=v_revision) then
      raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_LINE_NOT_IN_REVISION';
    end if;
    if p_kind='BREAKS' and not exists(select 1 from private.bpay_next_shift_detail s where s.id=v_shift and s.approved_line_id=v_line) then
      raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_LINE_NOT_IN_REVISION';
    end if;
  elsif p_kind in ('WORK_CHOICES','SELECTED_COMPONENTS') then
    perform 1 from private.bpay_next_pay_run r where r.id=(p_args->>'run_id')::uuid;
    if not found then raise exception using errcode='P0002',message='BPAY_NEXT_PREPARATION_NOT_FOUND';end if;
    if p_kind='SELECTED_COMPONENTS' then
      perform 1 from private.bpay_next_work_choice c where c.run_id=(p_args->>'run_id')::uuid and c.work_id=v_work;
      if not found then raise exception using errcode='P0002',message='BPAY_NEXT_PREPARATION_NOT_FOUND';end if;
    end if;
  end if;
  -- Each branch is a <=100 indexed relational page. A byte boundary ends
  -- before an entire row, never truncates facts; callers resume its last key.
  -- CASE evaluates only its chosen subquery, so unrelated review owners are
  -- never called with NULL scope. Each subquery orders/limits BEFORE packing.
  select case p_kind
    when 'COMPONENTS' then array(
      select pg_catalog.jsonb_build_object(
        'approved_line_id',l.id,'line_no',l.line_no::text,'component_key',l.component_key,'component_kind',l.component_kind,
        'work_date',l.work_date,'source_pay_channel',r.source_pay_channel,'approved_source_ex_vat',l.source_pay_ex_vat::text,
        'unit_label',l.unit_label,'approved_quantity',l.approved_quantity::text,
        'approved_unit_rate',l.approved_unit_rate::text,
        'job_title',r.job_title,'band_label',r.band_label,
        'original_work_ref',parent.reference_number,'original_timesheet_id',parent.timesheet_id,
        'is_adjustment',ts.is_adjustment,'correction_kind',ts.correction_kind,
        'associated_rate_schedule_id',rs.id,'associated_rate_family',rs.rate_family,'associated_rate_code',rs.rate_code,
        'available_paye_rate',rs.paye_rate::text,'available_umbrella_rate',rs.umbrella_rate::text,
        'available_charge_rate',rs.charge_rate::text,'approved_charge_rate',rs.approved_charge_rate::text,
        'standard_rate_schedule',case when l.component_kind in ('WORK','PROTECTED_WORK') then
          (select coalesce(jsonb_agg(snapshot.value order by snapshot.bucket_order),'[]'::jsonb) from (
            select jsonb_build_object('rate_schedule_id',sr.id,'bucket',sr.rate_code,
              'available_paye_rate',sr.paye_rate::text,'available_umbrella_rate',sr.umbrella_rate::text,
              'available_charge_rate',sr.charge_rate::text,'approved_charge_rate',sr.approved_charge_rate::text) value,
              case sr.rate_code when 'DAY' then 1 when 'NIGHT' then 2 when 'SAT' then 3 when 'SUN' then 4 when 'BH' then 5 end bucket_order
            from private.bpay_next_rate_schedule sr where sr.revision_id=v_revision and sr.rate_family='STANDARD'
              and sr.rate_code in ('DAY','NIGHT','SAT','SUN','BH') order by bucket_order limit 5
          ) snapshot) else '[]'::jsonb end,
        'realised_source_ex_vat',p.realised_source_ex_vat::text,'held_source_ex_vat',p.held_source_ex_vat::text,
        'residual_source_ex_vat',(l.source_pay_ex_vat-p.realised_source_ex_vat-p.held_source_ex_vat)::text,
        'position_ready',coalesce(p.applied_revision_id=v_revision and p.approved_source_ex_vat=l.source_pay_ex_vat,false)) as value
        from private.bpay_next_approved_line l join private.bpay_next_work_revision r on r.id=l.revision_id
        join private.bpay_next_work w on w.id=v_work and w.id=r.work_id
        left join public.timesheets ts on ts.timesheet_id=r.physical_timesheet_id and ts.contract_id=w.contract_id
        left join public.timesheets parent on parent.timesheet_id=ts.parent_timesheet_id
          and exists(select 1 from public.contracts pc where pc.id=parent.contract_id and pc.candidate_id=w.candidate_id)
        left join private.bpay_next_rate_schedule rs on rs.revision_id=v_revision
          and ((l.component_kind='ADDITIONAL' and l.component_key like 'ADDITIONAL:%'
            and rs.rate_family='ADDITIONAL' and rs.rate_code=substring(l.component_key from 12))
            or (l.component_kind='MILEAGE' and l.component_key='MILEAGE:MILE' and rs.rate_family='MILEAGE' and rs.rate_code='MILE'))
        left join private.bpay_next_position p on p.work_id=v_work and p.component_key=l.component_key
        where l.revision_id=v_revision and l.line_no>coalesce((p_args->>'after_line_no')::bigint,0)
        order by l.line_no,l.id limit v_limit)
    when 'WORK_CHOICES' then array(
      select pg_catalog.jsonb_build_object('work_id',c.work_id,'expected_revision_id',c.expected_revision_id,
        'selection_mode',c.selection_mode,'selection_state',c.selection_state,'page_count',c.page_count::text,'component_count',c.component_count::text)
        from private.bpay_next_work_choice c where c.run_id=(p_args->>'run_id')::uuid
          and ((p_args->>'after_work_id') is null or c.work_id>(p_args->>'after_work_id')::uuid)
        order by c.work_id limit v_limit)
    when 'SELECTED_COMPONENTS' then array(
      select pg_catalog.jsonb_build_object('selection_no',c.selection_no::text,
        'approved_line_id',c.approved_line_id,'expected_revision_id',c.expected_revision_id,'component_key',c.component_key)
        from private.bpay_next_selected_component c where c.run_id=(p_args->>'run_id')::uuid and c.work_id=v_work
          and c.selection_no>coalesce((p_args->>'after_selection_no')::bigint,0)
        order by c.selection_no limit v_limit)
    when 'WORKERS' then array(
      select pg_catalog.jsonb_build_object('run_worker_id',w.run_worker_id,'candidate_id',w.candidate_id,
        'worker_status',w.worker_status,'frozen_ex_vat',w.frozen_ex_vat::text,'frozen_vat',w.frozen_vat::text,
        'frozen_inc_vat',w.frozen_inc_vat::text,'review_issue_code',w.review_issue_code,
        'discard_outcome',x.outcome,'discard_blocked_code',x.blocked_code)
        from private.bpay_next_simple_review_worker_page_v1((p_args->>'run_id')::uuid,(p_args->>'after_candidate_id')::uuid,v_limit) w
        left join private.bpay_next_preparation_discard_worker x on x.run_id=(p_args->>'run_id')::uuid and x.candidate_id=w.candidate_id
        order by w.candidate_id limit v_limit)
    when 'REVISION_COMPARISON' then array(
      select pg_catalog.jsonb_build_object('run_work_id',w.run_work_id,'work_id',w.work_id,'original_timesheet_id',w.original_timesheet_id,
        'captured_revision_id',w.captured_revision_id,'current_revision_id',w.current_revision_id,'newer_approval_available',w.newer_approval_available)
        from private.bpay_next_simple_revision_comparison_page_v1((p_args->>'run_worker_id')::uuid,(p_args->>'after_work_id')::uuid,v_limit) w
        order by w.work_id limit v_limit)
    when 'LINES' then array(
      select pg_catalog.jsonb_build_object('run_line_id',l.run_line_id,'line_no',l.line_no::text,
        'original_timesheet_id',l.original_timesheet_id,'captured_revision_id',l.captured_revision_id,'component_key',l.component_key,
        'component_kind',l.component_kind,'work_date',l.work_date,'source_pay_channel',l.source_pay_channel,'target_pay_channel',l.target_pay_channel,
        'source_consumed_ex_vat',l.source_consumed_ex_vat::text,'frozen_ex_vat',l.frozen_ex_vat::text,'frozen_vat',l.frozen_vat::text,'frozen_inc_vat',l.frozen_inc_vat::text)
        from private.bpay_next_simple_review_line_page_v1((p_args->>'run_worker_id')::uuid,(p_args->>'after_line_no')::bigint,v_limit) l
        order by l.line_no limit v_limit)
    when 'SHIFTS' then array(
      select pg_catalog.jsonb_build_object('shift_detail_id',s.id,'detail_no',s.detail_no::text,'work_date',s.work_date,
        'shift_start_at',s.shift_start_at,'shift_end_at',s.shift_end_at,'shift_start_local',s.shift_start_local,'shift_end_local',s.shift_end_local,
        'shift_overnight',s.shift_overnight,'submitted_minutes',s.submitted_minutes::text,'approved_minutes',s.approved_minutes::text,
        'approved_hours',s.approved_hours::text,'detail_label',s.detail_label,'segment_pay_ex_vat',s.segment_pay_ex_vat::text,'pay_excluded',s.pay_excluded,
        'hours_day',s.hours_day::text,'hours_night',s.hours_night::text,'hours_sat',s.hours_sat::text,'hours_sun',s.hours_sun::text,'hours_bh',s.hours_bh::text,
        'segment_key',s.segment_key,'approved_segment_id',sg.id,'job_title',wr.job_title,'band_label',wr.band_label)
        from private.bpay_next_shift_detail s
        join private.bpay_next_approved_line al on al.id=s.approved_line_id and al.revision_id=v_revision
        join private.bpay_next_work_revision wr on wr.id=al.revision_id and wr.work_id=v_work
        left join private.bpay_next_approved_segment sg on sg.shift_detail_id=s.id
          and sg.revision_id=v_revision and sg.approved_line_id=v_line and sg.segment_key=s.segment_key
        where s.approved_line_id=v_line and s.detail_no>coalesce((p_args->>'after_detail_no')::bigint,0)
        order by s.detail_no limit v_limit)
    when 'BREAKS' then array(
      select pg_catalog.jsonb_build_object('break_detail_id',b.id,'break_no',b.break_no::text,'break_start_at',b.break_start_at,
        'break_end_at',b.break_end_at,'break_start_local',b.break_start_local,'break_end_local',b.break_end_local,'break_minutes',b.break_minutes::text)
        from private.bpay_next_break_detail b where b.shift_detail_id=v_shift and b.break_no>coalesce((p_args->>'after_break_no')::bigint,0)
        order by b.break_no limit v_limit)
    when 'RATES' then array(
      select pg_catalog.jsonb_build_object('rate_detail_id',r.id,'bucket',r.bucket,'approved_hours',r.approved_hours::text,'source_pay_rate',r.source_pay_rate::text,
        'rate_schedule_id',rs.id,'available_paye_rate',rs.paye_rate::text,'available_umbrella_rate',rs.umbrella_rate::text,
        'available_charge_rate',rs.charge_rate::text,'approved_charge_rate',rs.approved_charge_rate::text)
        from private.bpay_next_rate_detail r
        left join private.bpay_next_rate_schedule rs on rs.revision_id=v_revision
          and rs.rate_family='STANDARD' and rs.rate_code=r.bucket
        where r.approved_line_id=v_line
          and ((p_args->>'after_bucket') is null or r.bucket>p_args->>'after_bucket')
        order by r.bucket limit v_limit)
  end into v_page;
  foreach v_row in array v_page
  loop
    if pg_catalog.octet_length((v_rows||pg_catalog.jsonb_build_array(v_row))::text)>120000 then
      if v_n=0 then raise exception using errcode='54000',message='BPAY_NEXT_PREPARATION_RESPONSE_TOO_LARGE';end if;
      v_complete:=false;exit;
    end if;
    v_rows:=v_rows||pg_catalog.jsonb_build_array(v_row);v_n:=v_n+1;
  end loop;
  if v_n=v_limit then v_complete:=false;end if;
  return pg_catalog.jsonb_build_object('rows',v_rows,'complete',v_complete);
end
$function$;

alter function private.bpay_next_case_create_terms_v1(jsonb) owner to postgres;
revoke all on function private.bpay_next_case_create_terms_v1(jsonb) from public,anon,authenticated,service_role;

alter function private.bpay_next_case_create_context_v1(uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_case_create_context_v1(uuid,uuid) from public,anon,authenticated,service_role;

alter function private.bpay_next_case_create_preview_v1(uuid,jsonb) owner to postgres;
revoke all on function private.bpay_next_case_create_preview_v1(uuid,jsonb) from public,anon,authenticated,service_role;

alter function private.bpay_next_accept_case_create_v1(uuid,uuid,uuid,text,text,text,numeric,date,date,numeric,bigint,numeric,numeric,text,text,text,jsonb) owner to postgres;
revoke all on function private.bpay_next_accept_case_create_v1(uuid,uuid,uuid,text,text,text,numeric,date,date,numeric,bigint,numeric,numeric,text,text,text,jsonb) from public,anon,authenticated,service_role;

alter function private.bpay_next_accept_case_create_v1(uuid,uuid,uuid,text,text,text,numeric,date,date,numeric,bigint,numeric,numeric,text) owner to postgres;
revoke all on function private.bpay_next_accept_case_create_v1(uuid,uuid,uuid,text,text,text,numeric,date,date,numeric,bigint,numeric,numeric,text) from public,anon,authenticated,service_role;

alter function private.bpay_next_apply_case_create_v1(uuid,uuid,bigint) owner to postgres;
revoke all on function private.bpay_next_apply_case_create_v1(uuid,uuid,bigint) from public,anon,authenticated,service_role;

alter function public.bpay_next_financial_intake_v1(uuid,text,jsonb) owner to postgres;
revoke all on function public.bpay_next_financial_intake_v1(uuid,text,jsonb) from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_financial_intake_v1(uuid,text,jsonb) to service_role;

alter function public.bpay_next_preparation_page_v1(uuid,text,jsonb) owner to postgres;
revoke all on function public.bpay_next_preparation_page_v1(uuid,text,jsonb) from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_preparation_page_v1(uuid,text,jsonb) to service_role;


notify pgrst,'reload schema';
commit;
