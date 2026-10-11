-- Repeatable CloudTMS function/view authority: banking_pay_next_reauth_proof_v1
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

-- EX-01 / BD110. Finite native point joins only. p_lock is owner-only and
-- follows each unchanged acceptor's business locks BEFORE the proof row.
create or replace function private.bpay_next_reauth_scope_v1(
  p_actor uuid,p_action text,p_args jsonb,p_lock boolean default false
) returns jsonb language plpgsql security invoker
set search_path=pg_catalog,private,public
as $function$
declare
  v_keys text[];v_key text;v_intent jsonb:=p_args;v_id uuid;v_epoch bigint;
  v_w private.bpay_next_run_worker%rowtype;v_t private.bpay_next_transfer%rowtype;
  v_cash private.bpay_next_return_cash%rowtype;v_case private.bpay_next_finance_case%rowtype;
  v_component private.bpay_next_case_component%rowtype;v_collection private.bpay_next_work_collection%rowtype;
  v_projection private.bpay_next_net_projection%rowtype;v_leg private.bpay_next_destination_group_leg%rowtype;
  v_origin private.bpay_next_stored_credit_origin%rowtype;v_candidate public.candidates%rowtype;
  v_umbrella public.umbrellas%rowtype;v_prior record;v_facts jsonb;v_amount numeric;
  v_candidate_id uuid;v_scope text;v_bank_hash text;v_bank_version timestamptz;
  v_replay boolean:=false;v_kind text;v_label text;v_basis text:='CASH';v_name text;
begin
  if p_actor is null or p_action is null or p_action not in
    ('WRITE_OFF','CANCEL_REQUEST','CASH_REISSUE','REISSUE_CANCEL','CSV_ISSUE','CSV_RETURN')
    or p_args is null or jsonb_typeof(p_args)<>'object' or octet_length(p_args::text)>2048
    or p_lock is null then
    raise exception using errcode='22023',message='BPAY_NEXT_REAUTH_REQUEST_INVALID';end if;
  v_keys:=case p_action
    when 'WRITE_OFF' then array['command_id','case_component_id','expected_component_revision','scope','reason']
      ||case when p_args->>'scope'='AMOUNT' then array['amount'] else array[]::text[] end
    when 'CANCEL_REQUEST' then array['command_id','run_worker_id']
      ||case when p_args ? 'run_id' or p_args ? 'pay_channel' then array['run_id','pay_channel'] else array[]::text[] end
    when 'CASH_REISSUE' then array['command_id','return_cash_id']
    when 'REISSUE_CANCEL' then array['command_id','transfer_id']
    when 'CSV_ISSUE' then array['instruction_id','transfer_id']
    else array['command_id','transfer_id','receipt_id','amount','occurred_at'] end;
  if (select count(*) from jsonb_object_keys(p_args))<>array_length(v_keys,1)
    or exists(select 1 from jsonb_object_keys(p_args) k where not(k=any(v_keys))) then
    raise exception using errcode='22023',message='BPAY_NEXT_REAUTH_REQUEST_INVALID';end if;
  foreach v_key in array v_keys loop
    if jsonb_typeof(p_args->v_key) is distinct from 'string' then
      raise exception using errcode='22023',message='BPAY_NEXT_REAUTH_REQUEST_INVALID';end if;
    if v_key in ('command_id','instruction_id','run_worker_id','transfer_id','return_cash_id','run_id','case_component_id') then
      if (p_args->>v_key)!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
        raise exception using errcode='22023',message='BPAY_NEXT_REAUTH_REQUEST_INVALID';end if;
      v_intent:=jsonb_set(v_intent,array[v_key],to_jsonb((p_args->>v_key)::uuid::text));
    end if;
  end loop;
  v_id:=coalesce(p_args->>'command_id',p_args->>'instruction_id')::uuid;
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT';
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  if not exists(select 1 from public.tms_users u where u.id=p_actor and u.is_active is true and u.role::text='admin'
    and (p_action in ('WRITE_OFF','CANCEL_REQUEST') or u.payment_authoriser is true or u.payment_golden_key is true)) then
    raise exception using errcode='42501',message='BPAY_NEXT_REAUTH_REQUIRED';end if;
  v_kind:=case p_action when 'WRITE_OFF' then 'CASE_WRITE_OFF' when 'CANCEL_REQUEST' then 'SIMPLE_CANCEL' else p_action end;
  v_label:=case p_action when 'WRITE_OFF' then 'Write off debt' when 'CANCEL_REQUEST' then 'Cancel payment'
    when 'CASH_REISSUE' then 'Pay returned payment' when 'REISSUE_CANCEL' then 'Cancel returned payment'
    when 'CSV_ISSUE' then 'Create Bank CSV' else 'Record bank return' end;

  if p_action='WRITE_OFF' then
    if (p_args->>'expected_component_revision')!~'^[1-9][0-9]{0,18}$'
      or (p_args->>'expected_component_revision')::numeric>9223372036854775807
      or p_args->>'scope' not in ('ALL','AMOUNT') or octet_length(p_args->>'reason') not between 1 and 1024
      or btrim(p_args->>'reason')='' or (p_args->>'scope'='AMOUNT' and
        ((p_args->>'amount')!~'^(0|[1-9][0-9]*)([.][0-9]{1,2})?$'
          or octet_length(p_args->>'amount')>1024 or (p_args->>'amount')::numeric<=0)) then
      raise exception using errcode='22023',message='BPAY_NEXT_REAUTH_REQUEST_INVALID';end if;
    v_intent:=jsonb_set(v_intent,'{expected_component_revision}',to_jsonb((p_args->>'expected_component_revision')::bigint::text));
    if p_lock then perform pg_advisory_xact_lock(hashtextextended(v_id::text,0));end if;
    select r.* into strict v_component from private.bpay_next_case_component r where id=(p_args->>'case_component_id')::uuid;
    v_candidate_id:=v_component.candidate_id;
    if p_lock then
      perform 1 from private.bpay_next_worker_control where candidate_id=v_candidate_id for update;
      perform 1 from private.bpay_next_finance_case where id=v_component.case_id for update;
      perform 1 from private.bpay_next_case_component where id=v_component.id for update;
      select r.* into strict v_component from private.bpay_next_case_component r where id=v_component.id;
    end if;
    select r.* into strict v_case from private.bpay_next_finance_case r where id=v_component.case_id and candidate_id=v_candidate_id;
    select r.* into v_prior from private.bpay_next_write_off_request r where command_id=v_id;
    if found then
      if (v_prior.case_component_id,v_prior.actor_user_id,v_prior.expected_component_revision,
        v_prior.requested_scope,v_prior.input_amount,v_prior.reason) is distinct from
        (v_component.id,p_actor,(p_args->>'expected_component_revision')::bigint,p_args->>'scope',p_args->>'amount',p_args->>'reason') then
        raise exception using errcode='42501',message='BPAY_NEXT_REAUTH_REQUIRED';end if;
      v_replay:=true;
      v_amount:=case when v_prior.requested_scope='ALL' then v_prior.basis_principal-v_prior.basis_recovered-v_prior.basis_written_off
        else v_prior.requested_amount end;
    else
      v_collection:=private.bpay_next_work_collection_origin_v1(v_case.id,v_component.id,v_candidate_id);
      if v_collection.id is null or v_case.status not in ('OPEN','PAUSED')
        or v_component.component_revision<>(p_args->>'expected_component_revision')::bigint then
        raise exception using errcode='55000',message='BPAY_NEXT_REAUTH_STALE';end if;
      v_amount:=case when p_args->>'scope'='ALL' then v_case.principal_approved-v_case.principal_recovered-v_case.principal_written_off
        else (p_args->>'amount')::numeric end;
      v_facts:=jsonb_build_array(v_candidate_id,v_case.id,v_component.id,v_collection.id,v_case.case_revision,
        v_component.component_revision,v_case.principal_approved,v_case.principal_recovered,
        v_case.principal_written_off,v_case.active_recovery_hold_amount);
    end if;
  else
    if p_action='CANCEL_REQUEST' then
      select r.* into strict v_w from private.bpay_next_run_worker r where id=(p_args->>'run_worker_id')::uuid;
      if p_args ? 'run_id' and (v_w.run_id is distinct from (p_args->>'run_id')::uuid
        or v_w.target_pay_channel is distinct from p_args->>'pay_channel') then
        raise exception using errcode='42501',message='BPAY_NEXT_REAUTH_REQUIRED';end if;
      if p_lock and v_w.case_selection_revision>0 then perform pg_advisory_xact_lock(hashtextextended(v_id::text,0));end if;
    elsif p_action='CASH_REISSUE' then
      select r.* into strict v_cash from private.bpay_next_return_cash r where id=(p_args->>'return_cash_id')::uuid;
      select r.* into strict v_t from private.bpay_next_transfer r where id=v_cash.original_transfer_id;
      select r.* into strict v_w from private.bpay_next_run_worker r where id=v_t.run_worker_id;
    else
      select r.* into strict v_t from private.bpay_next_transfer r where id=(p_args->>'transfer_id')::uuid;
      select r.* into strict v_w from private.bpay_next_run_worker r where id=v_t.run_worker_id;
    end if;
    v_candidate_id:=v_w.candidate_id;
    if p_lock then
      perform 1 from private.bpay_next_pay_run where id=v_w.run_id for update;
      if p_action in ('CANCEL_REQUEST','REISSUE_CANCEL','CSV_RETURN') then
        -- The original simple cancel creates this control if absent, under the same run lock.
        if p_action='CANCEL_REQUEST' and v_w.case_selection_revision=0 then
          insert into private.bpay_next_worker_control(candidate_id) values(v_candidate_id) on conflict(candidate_id) do nothing;
        end if;
        perform 1 from private.bpay_next_worker_control where candidate_id=v_candidate_id for update;
      end if;
      perform 1 from private.bpay_next_run_worker where id=v_w.id for update;
      select r.* into strict v_w from private.bpay_next_run_worker r where id=v_w.id;
      if p_action in ('CSV_ISSUE','REISSUE_CANCEL','CSV_RETURN') then
        perform 1 from private.bpay_next_transfer where id=v_t.id for update;
        select r.* into strict v_t from private.bpay_next_transfer r where id=v_t.id;
      end if;
      perform 1 from public.tms_users where id=p_actor for share;
      if p_action='CASH_REISSUE' then
        select r.* into strict v_cash from private.bpay_next_return_cash r where id=v_cash.id for update;
      end if;
    end if;
    if p_action='CANCEL_REQUEST' then
      select r.* into v_projection from private.bpay_next_net_projection r where run_worker_id=v_w.id
        and projection_no=v_w.net_projection_revision and retired_at_utc is null;
      v_amount:=coalesce(v_projection.cash_amount,v_w.gross_inc_vat);
      v_basis:=case when v_projection.id is null then 'DRAFT_GROSS' else 'CASH' end;
      select r.* into v_prior from private.bpay_next_cancel_request r where command_id=v_id;
      if found then
        if (v_prior.run_worker_id,v_prior.candidate_id) is distinct from (v_w.id,v_candidate_id)
          or (v_w.case_selection_revision>0 and not exists(select 1 from private.bpay_next_case_cancel_binding
            where command_id=v_id and run_worker_id=v_w.id)) then
          raise exception using errcode='42501',message='BPAY_NEXT_REAUTH_REQUIRED';end if;
        v_replay:=true;
        -- Receipt recovery does not claim a live projection as the original
        -- authorised cash. This immutable Draft gross is explicitly labelled.
        v_amount:=v_w.gross_inc_vat;v_basis:='DRAFT_GROSS';
      end if;
      v_facts:=jsonb_build_array(v_w.id,v_w.run_id,v_candidate_id,v_w.target_pay_channel,v_w.target_umbrella_id,
        v_w.captured_preparation_revision,v_w.case_selection_revision,v_w.captured_line_count,
        v_w.gross_ex_vat,v_w.gross_vat,v_w.gross_inc_vat,v_w.net_request_revision,
        v_w.net_projection_revision,v_projection.id,v_projection.cash_amount,v_projection.accepted_recoveries);
    elsif p_action='CASH_REISSUE' then
      select r.* into v_prior from private.bpay_next_reissue_request r where command_id=v_id;
      if found then
        if (v_prior.return_cash_id,v_prior.run_worker_id,v_prior.candidate_id,v_prior.actor_user_id)
          is distinct from (v_cash.id,v_w.id,v_candidate_id,p_actor) then
          raise exception using errcode='42501',message='BPAY_NEXT_REAUTH_REQUIRED';end if;
        v_replay:=true;
      else
        if v_cash.amount_held<>0 or v_cash.amount_reissued_paid<>0 or v_t.status<>'RETURNED'
          or not exists(select 1 from private.bpay_next_return_request where transfer_id=v_t.id and posting_complete)
          or exists(select 1 from private.bpay_next_reissue_request where return_cash_id=v_cash.id and transfer_id is null) then
          raise exception using errcode='55000',message='BPAY_NEXT_REAUTH_STALE';end if;
      end if;
      v_amount:=case when v_replay then v_cash.amount_owed
        else v_cash.amount_owed-v_cash.amount_held-v_cash.amount_reissued_paid end;
      v_facts:=jsonb_build_array(v_cash.id,v_t.id,v_w.id,v_w.run_id,v_candidate_id,
        v_t.beneficiary_kind,v_t.beneficiary_id,v_t.currency,v_cash.amount_owed,v_cash.amount_held,v_cash.amount_reissued_paid);
    elsif p_action='REISSUE_CANCEL' then
      v_amount:=v_t.cash_amount;
      select r.* into v_prior from private.bpay_next_reissue_cancel_request r where command_id=v_id;
      if found then
        if (v_prior.transfer_id,v_prior.return_cash_id,v_prior.run_worker_id,v_prior.candidate_id,v_prior.actor_user_id,v_prior.amount)
          is distinct from (v_t.id,v_t.return_cash_id,v_w.id,v_candidate_id,p_actor,v_t.cash_amount) then
          raise exception using errcode='42501',message='BPAY_NEXT_REAUTH_REQUIRED';end if;
        v_replay:=true;
      else
        if private.bpay_next_reissue_cancel_block_v1(v_t.id) is not null then
          raise exception using errcode='55000',message='BPAY_NEXT_REAUTH_STALE';end if;
      end if;
      v_facts:=jsonb_build_array(v_t.id,v_w.id,v_w.run_id,v_candidate_id,v_t.return_cash_id,v_t.original_transfer_id,
        v_t.cash_amount,v_t.currency,v_t.beneficiary_kind,v_t.beneficiary_id,v_t.member_count,v_t.member_cash_sum);
    elsif p_action='CSV_RETURN' then
      if (p_args->>'amount')!~'^(0|[1-9][0-9]{0,15})([.][0-9]{1,2})?$'
        or (p_args->>'amount')::numeric<=0 or octet_length(p_args->>'receipt_id') not between 1 and 256
        or (p_args->>'occurred_at')!~'^[0-9]{4}-[0-9]{2}-[0-9]{2}T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]([.][0-9]{1,6})?(Z|[+-]([01][0-9]|2[0-3]):[0-5][0-9])$'
        or not isfinite((p_args->>'occurred_at')::timestamptz) then
        raise exception using errcode='22023',message='BPAY_NEXT_REAUTH_REQUEST_INVALID';end if;
      v_intent:=jsonb_set(v_intent,'{amount}',to_jsonb(round((p_args->>'amount')::numeric,2)::text));
      v_intent:=jsonb_set(v_intent,'{occurred_at}',to_jsonb(to_char((p_args->>'occurred_at')::timestamptz at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"')));
      v_amount:=v_t.cash_amount;
      select r.command_id,r.actor_user_id,r.run_worker_id,r.candidate_id,o.* into v_prior
        from private.bpay_next_transfer_outcome o join private.bpay_next_return_request r on r.outcome_id=o.id
        where o.receipt_id=p_args->>'receipt_id';
      if found then
        if (v_prior.command_id,v_prior.actor_user_id,v_prior.transfer_id,v_prior.run_worker_id,v_prior.candidate_id,
          v_prior.outcome_kind,v_prior.whole_transfer_amount,v_prior.occurred_at_utc) is distinct from
          (v_id,p_actor,v_t.id,v_w.id,v_candidate_id,'RETURNED'::text,(p_args->>'amount')::numeric,(p_args->>'occurred_at')::timestamptz) then
          raise exception using errcode='42501',message='BPAY_NEXT_REAUTH_REQUIRED';end if;
        v_replay:=true;
      else
        if v_t.status<>'SETTLED' or v_t.destination_rail<>'CSV' or v_t.cash_amount<>(p_args->>'amount')::numeric
          or not exists(select 1 from private.bpay_next_outcome_request r join private.bpay_next_transfer_outcome o on o.id=r.outcome_id
            where r.transfer_id=v_t.id and o.outcome_kind='SETTLED' and o.whole_transfer_amount=v_t.cash_amount
              and o.occurred_at_utc<=(p_args->>'occurred_at')::timestamptz) then
          raise exception using errcode='55000',message='BPAY_NEXT_REAUTH_STALE';end if;
      end if;
      v_facts:=jsonb_build_array(v_t.id,v_w.id,v_w.run_id,v_candidate_id,v_t.cash_amount,v_t.currency,
        v_t.beneficiary_kind,v_t.beneficiary_id,v_t.original_transfer_id,v_t.return_cash_id);
    else
      v_amount:=v_t.cash_amount;
      select r.* into v_prior from private.bpay_next_csv_instruction r where id=v_id;
      if found then
        if (v_prior.transfer_id,v_prior.run_worker_id,v_prior.actor_user_id,v_prior.cash_amount)
          is distinct from (v_t.id,v_w.id,p_actor,v_t.cash_amount) then
          raise exception using errcode='42501',message='BPAY_NEXT_REAUTH_REQUIRED';end if;
        v_replay:=true;
      else
        if exists(select 1 from private.bpay_next_csv_instruction where transfer_id=v_t.id) then
          raise exception using errcode='42501',message='BPAY_NEXT_REAUTH_REQUIRED';end if;
        if p_lock then select r.* into strict v_candidate from public.candidates r where id=v_candidate_id for share;
        else select r.* into strict v_candidate from public.candidates r where id=v_candidate_id;end if;
        v_bank_hash:=public._bank_hash(v_candidate.sort_code,v_candidate.account_number,v_candidate.account_holder);
        select r.* into v_leg from private.bpay_next_destination_group_leg r where transfer_id=coalesce(v_t.original_transfer_id,v_t.id);
        if v_w.target_pay_channel='UMBRELLA' then
          if p_lock then select r.* into strict v_umbrella from public.umbrellas r where id=v_w.target_umbrella_id for share;
          else select r.* into strict v_umbrella from public.umbrellas r where id=v_w.target_umbrella_id;end if;
          v_bank_hash:=public._bank_hash(v_umbrella.sort_code,v_umbrella.account_number,v_umbrella.name);
          if v_umbrella.enabled is not true or v_bank_hash is distinct from v_umbrella.bank_details_hash then
            raise exception using errcode='55000',message='BPAY_NEXT_REAUTH_STALE';end if;
        elsif v_leg.leg_kind='ONEOFF' then
          select o.* into strict v_origin from private.bpay_next_net_destination_amount d
            join private.bpay_next_stored_credit_origin o on o.command_id=d.first_origin_command_id where d.destination_id=v_leg.destination_id;
          if p_lock then perform 1 from public.pay_finance_case_oneoff_payout_bank_details
            where finance_case_id=v_origin.legacy_case_id and candidate_id=v_origin.candidate_id for share;end if;
          if not exists(select 1 from public.pay_finance_case_oneoff_payout_bank_details b
            where b.finance_case_id=v_origin.legacy_case_id and b.candidate_id=v_origin.candidate_id
              and (b.updated_at_utc,b.bank_details_hash,b.beneficiary_name,replace(b.sort_code,'-',''),b.account_number)
                is not distinct from (v_origin.bank_version_at_utc,v_origin.bank_details_hash,v_origin.beneficiary_name,v_origin.sort_code,v_origin.account_number)) then
            raise exception using errcode='55000',message='BPAY_NEXT_REAUTH_STALE';end if;
          v_bank_hash:=v_origin.bank_details_hash;v_bank_version:=v_origin.bank_version_at_utc;
        elsif v_bank_hash is distinct from v_candidate.bank_details_hash then
          raise exception using errcode='55000',message='BPAY_NEXT_REAUTH_STALE';end if;
        if v_t.status<>'MEMBERS_READY' or v_t.cash_amount<=0 or v_bank_hash is null then
          raise exception using errcode='55000',message='BPAY_NEXT_REAUTH_STALE';end if;
      end if;
      v_facts:=jsonb_build_array(v_t.id,v_w.id,v_w.run_id,v_candidate_id,v_t.projection_id,v_t.return_cash_id,
        v_t.cash_amount,v_t.currency,v_t.beneficiary_kind,v_t.beneficiary_id,v_bank_hash,v_bank_version,
        v_t.member_count,v_t.member_cash_sum);
    end if;
  end if;

  if v_replay then
    if p_action<>'CSV_ISSUE' and not exists(select 1 from private.bpay_next_command
      where id=v_id and command_kind=v_kind and module_epoch=v_epoch) then
      raise exception using errcode='42501',message='BPAY_NEXT_REAUTH_REQUIRED';end if;
    select scope_sha256 into v_scope from private.bpay_next_reauth_proof
      where consumed_command_id=v_id and actor_user_id=p_actor and action_kind=p_action;
    -- A same-ID native CSV re-download is expressly exempt; no new issue.
    if v_scope is null and p_action<>'CSV_ISSUE' then
      raise exception using errcode='42501',message='BPAY_NEXT_REAUTH_REQUIRED';end if;
  else
    if p_action<>'CSV_ISSUE' and exists(select 1 from private.bpay_next_command where id=v_id) then
      raise exception using errcode='42501',message='BPAY_NEXT_REAUTH_REQUIRED';end if;
    v_scope:=encode(extensions.digest(jsonb_build_array(p_actor,p_action,v_epoch,v_intent,v_facts)::text,'sha256'),'hex');
  end if;
  select coalesce(nullif(btrim(c.display_name),''),nullif(btrim(concat_ws(' ',c.first_name,c.last_name)),''),'Candidate')
    into strict v_name from public.candidates c where c.id=v_candidate_id;
  return jsonb_build_object('action_kind',p_action,'action_label',v_label,'candidate_count',1,
    'candidate_name',v_name,'total_amount',round(v_amount,2)::text,'amount_basis',v_basis,
    'scope_sha256',v_scope,'accepted',v_replay);
end
$function$;

create or replace function public.bpay_next_reauth_preview_v1(p_actor_user_id uuid,p_action text,p_args jsonb)
returns jsonb language plpgsql security definer set search_path=pg_catalog,private,public
as $function$
begin
  if coalesce(current_setting('request.jwt.claim.role',true),
    nullif(current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception using errcode='42501',message='BPAY_NEXT_REAUTH_REQUIRED';end if;
  return private.bpay_next_reauth_scope_v1(p_actor_user_id,p_action,p_args,false);
end
$function$;

create or replace function public.bpay_next_reauth_record_v1(p_request jsonb)
returns jsonb language plpgsql security definer set search_path=pg_catalog,private,public
as $function$
declare v_p private.bpay_next_reauth_proof%rowtype;v_verified timestamptz;v_expires timestamptz;v_key text;
begin
  if coalesce(current_setting('request.jwt.claim.role',true),
    nullif(current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
    or p_request is null or jsonb_typeof(p_request)<>'object' or octet_length(p_request::text)>1024
    or (select count(*) from jsonb_object_keys(p_request))<>7
    or exists(select 1 from jsonb_object_keys(p_request) k where k not in
      ('actor_user_id','action_kind','scope_sha256','token_sha256','second_factor_used','verified_at_utc','expires_at_utc')) then
    raise exception using errcode='42501',message='BPAY_NEXT_REAUTH_REQUIRED';end if;
  foreach v_key in array array['actor_user_id','action_kind','scope_sha256','token_sha256','verified_at_utc','expires_at_utc'] loop
    if jsonb_typeof(p_request->v_key) is distinct from 'string' then
      raise exception using errcode='42501',message='BPAY_NEXT_REAUTH_REQUIRED';end if;
  end loop;
  if (p_request->>'actor_user_id')!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
    or (p_request->>'scope_sha256')!~'^[0-9a-f]{64}$' or (p_request->>'token_sha256')!~'^[0-9a-f]{64}$'
    or jsonb_typeof(p_request->'second_factor_used') is distinct from 'boolean'
    or (p_request->>'verified_at_utc')!~'^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.]+Z$'
    or (p_request->>'expires_at_utc')!~'^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.]+Z$' then
    raise exception using errcode='42501',message='BPAY_NEXT_REAUTH_REQUIRED';end if;
  v_verified:=(p_request->>'verified_at_utc')::timestamptz;v_expires:=(p_request->>'expires_at_utc')::timestamptz;
  if not isfinite(v_verified) or not isfinite(v_expires) or v_expires<=v_verified
    or v_expires>v_verified+interval '300 seconds' or v_verified>clock_timestamp()+interval '5 seconds'
    or not exists(select 1 from public.tms_users where id=(p_request->>'actor_user_id')::uuid and is_active is true and role::text='admin')
    or not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT') then
    raise exception using errcode='42501',message='BPAY_NEXT_REAUTH_REQUIRED';end if;
  -- Serialise only this token's audit record; no business lock is acquired.
  perform pg_advisory_xact_lock(hashtextextended('bpay-next-reauth:'||(p_request->>'token_sha256'),0));
  select r.* into v_p from private.bpay_next_reauth_proof r where token_sha256=p_request->>'token_sha256';
  if found then
    if (v_p.actor_user_id,v_p.action_kind,v_p.scope_sha256,v_p.second_factor_used,v_p.verified_at_utc,v_p.expires_at_utc)
      is distinct from ((p_request->>'actor_user_id')::uuid,p_request->>'action_kind',p_request->>'scope_sha256',
        (p_request->>'second_factor_used')::boolean,v_verified,v_expires) then
      raise exception using errcode='42501',message='BPAY_NEXT_REAUTH_REQUIRED';end if;
    if v_p.consumed_command_id is null and v_expires<=clock_timestamp() then
      raise exception using errcode='55000',message='BPAY_NEXT_REAUTH_STALE';end if;
  else
    if v_expires<=clock_timestamp() then raise exception using errcode='55000',message='BPAY_NEXT_REAUTH_STALE';end if;
    insert into private.bpay_next_reauth_proof(actor_user_id,action_kind,scope_sha256,token_sha256,
      second_factor_used,verified_at_utc,expires_at_utc) values((p_request->>'actor_user_id')::uuid,
      p_request->>'action_kind',p_request->>'scope_sha256',p_request->>'token_sha256',
      (p_request->>'second_factor_used')::boolean,v_verified,v_expires) returning * into v_p;
  end if;
  return jsonb_build_object('reauth_proof_id',v_p.id,'consumed_command_id',v_p.consumed_command_id);
end
$function$;

create or replace function private.bpay_next_reauth_consume_v1(
  p_proof_id uuid,p_actor uuid,p_action text,p_scope_sha256 text,p_command_id uuid
) returns void language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_p private.bpay_next_reauth_proof%rowtype;
begin
  if p_proof_id is null or p_actor is null or p_action is null or p_scope_sha256 is null or p_command_id is null then
    raise exception using errcode='42501',message='BPAY_NEXT_REAUTH_REQUIRED';end if;
  select r.* into v_p from private.bpay_next_reauth_proof r where id=p_proof_id for update;
  if not found or (v_p.actor_user_id,v_p.action_kind) is distinct from (p_actor,p_action)
    or (v_p.consumed_command_id is not null and v_p.consumed_command_id<>p_command_id) then
    raise exception using errcode='42501',message='BPAY_NEXT_REAUTH_REQUIRED';end if;
  if v_p.scope_sha256<>p_scope_sha256 then
    raise exception using errcode='55000',message='BPAY_NEXT_REAUTH_STALE';end if;
  if v_p.consumed_command_id=p_command_id then return;end if;
  if v_p.expires_at_utc<=clock_timestamp() or v_p.verified_at_utc>clock_timestamp()+interval '5 seconds' then
    raise exception using errcode='55000',message='BPAY_NEXT_REAUTH_STALE';end if;
  update private.bpay_next_reauth_proof set consumed_command_id=p_command_id where id=p_proof_id and consumed_command_id is null;
end
$function$;

alter function private.bpay_next_reauth_scope_v1(uuid,text,jsonb,boolean) owner to postgres;
revoke all on function private.bpay_next_reauth_scope_v1(uuid,text,jsonb,boolean) from public,anon,authenticated,service_role;
alter function private.bpay_next_reauth_consume_v1(uuid,uuid,text,text,uuid) owner to postgres;
revoke all on function private.bpay_next_reauth_consume_v1(uuid,uuid,text,text,uuid) from public,anon,authenticated,service_role;
alter function public.bpay_next_reauth_preview_v1(uuid,text,jsonb) owner to postgres;
revoke all on function public.bpay_next_reauth_preview_v1(uuid,text,jsonb) from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_reauth_preview_v1(uuid,text,jsonb) to service_role;
alter function public.bpay_next_reauth_record_v1(jsonb) owner to postgres;
revoke all on function public.bpay_next_reauth_record_v1(jsonb) from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_reauth_record_v1(jsonb) to service_role;
notify pgrst,'reload schema';

commit;
