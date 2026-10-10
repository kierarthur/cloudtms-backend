-- Repeatable CloudTMS function/view authority: banking_pay_next_csv_umbrella_row_projection_v1
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

-- F05 D12/D15/D16/D17 corrections required by F23/B2.13: four newly added
-- row projections use alias.*. Original filters, locks and receipts remain.

create or replace function private.bpay_next_bind_simple_csv_destination_v1(
  p_transfer_id uuid
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_run_id uuid;
  v_run private.bpay_next_pay_run%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_transfer private.bpay_next_transfer%rowtype;
  v_candidate public.candidates%rowtype;
  v_umbrella public.umbrellas%rowtype;
  v_leg private.bpay_next_destination_group_leg%rowtype;
  v_origin private.bpay_next_stored_credit_origin%rowtype;
  v_name text;
  v_sort text;
  v_account text;
  v_hash text;
  v_replay boolean;
begin
  if p_transfer_id is null then
    raise exception using errcode='22023',
      message='BPAY_NEXT_CSV_DESTINATION_INPUT_INVALID';
  end if;
  if not exists(select 1 from private.bpay_next_module_control m
                where m.id=1 and m.active_owner='NEXT' for share) then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select w.run_id into strict v_run_id
    from private.bpay_next_transfer t
    join private.bpay_next_run_worker w on w.id=t.run_worker_id
    where t.id=p_transfer_id;
  select * into strict v_run from private.bpay_next_pay_run
    where id=v_run_id for update;
  select w.* into strict v_worker from private.bpay_next_run_worker w
    join private.bpay_next_transfer t on t.run_worker_id=w.id
    where t.id=p_transfer_id for update of w;
  select * into strict v_transfer from private.bpay_next_transfer
    where id=p_transfer_id for update;
  v_leg:=private.bpay_next_destination_execution_leg_v1(p_transfer_id);
  if v_run.status<>'DRAFT' or v_run.confirmed_at_utc is null
     or exists(select 1 from private.bpay_next_cancel_request r
               where r.run_worker_id=v_worker.id and r.status in ('REQUESTED','CANCELLING'))
     or (v_worker.status<>'READY' and not (v_worker.status='COMPLETE'
       and v_leg.transfer_id is not null and v_transfer.original_transfer_id is not null))
     or v_worker.target_pay_channel not in ('PAYE','UMBRELLA')
     or v_transfer.status<>'MEMBERS_READY'
     or v_transfer.candidate_id<>v_worker.candidate_id
     or not ((v_worker.target_pay_channel='PAYE' and
          v_transfer.beneficiary_kind='CANDIDATE' and v_transfer.beneficiary_id=v_worker.candidate_id)
       or (v_worker.target_pay_channel='UMBRELLA' and v_worker.target_umbrella_id is not null and
          v_transfer.beneficiary_kind='UMBRELLA' and v_transfer.beneficiary_id=v_worker.target_umbrella_id))
     or (v_transfer.projection_id is null and v_transfer.return_cash_id is null)
     or v_transfer.cash_amount<=0
     or v_transfer.member_count<=0
     or v_transfer.member_cash_sum<>v_transfer.cash_amount
     or (not exists(select 1 from private.bpay_next_command c
                   where c.id=v_transfer.build_command_id
                     and c.command_kind in ('TRANSFER_BUILD','CASH_REISSUE')
                     and c.status='COMPLETE')
       and not (v_leg.transfer_id is not null and v_transfer.original_transfer_id is null)) then
    raise exception using errcode='55000',
      message='BPAY_NEXT_CSV_DESTINATION_NOT_ELIGIBLE';
  end if;
  select * into strict v_candidate from public.candidates
    where id=v_worker.candidate_id;
  v_name:=nullif(pg_catalog.btrim(v_candidate.account_holder),'');
  v_sort:=pg_catalog.regexp_replace(
    coalesce(v_candidate.sort_code,''),'[^0-9]','','g');
  v_account:=pg_catalog.regexp_replace(
    coalesce(v_candidate.account_number,''),'[^0-9]','','g');
  v_hash:=public._bank_hash(v_candidate.sort_code,
    v_candidate.account_number,v_candidate.account_holder);
  if v_worker.target_pay_channel='UMBRELLA' then
    if v_leg.transfer_id is not null or v_worker.case_selection_revision<>0
       or v_candidate.active is not true
       or pg_catalog.upper(coalesce(v_candidate.pay_method,''))<>'UMBRELLA'
       or v_candidate.umbrella_id is distinct from v_worker.target_umbrella_id then
      raise exception using errcode='55000',message='BPAY_NEXT_CSV_UMBRELLA_DESTINATION_NOT_READY';
    end if;
    select u.* into strict v_umbrella from public.umbrellas u where u.id=v_worker.target_umbrella_id for share;
    v_name:=nullif(pg_catalog.btrim(v_umbrella.name),'');
    v_sort:=pg_catalog.regexp_replace(coalesce(v_umbrella.sort_code,''),'[^0-9]','','g');
    v_account:=pg_catalog.regexp_replace(coalesce(v_umbrella.account_number,''),'[^0-9]','','g');
    v_hash:=public._bank_hash(v_umbrella.sort_code,v_umbrella.account_number,v_umbrella.name);
    if v_umbrella.enabled is not true or v_hash is distinct from v_umbrella.bank_details_hash then
      raise exception using errcode='55000',message='BPAY_NEXT_CSV_UMBRELLA_DESTINATION_NOT_READY';
    end if;
  end if;
  if v_leg.leg_kind='ONEOFF' then
    select o.* into strict v_origin from private.bpay_next_net_destination_amount d
      join private.bpay_next_stored_credit_origin o on o.command_id=d.first_origin_command_id
      where d.destination_id=v_leg.destination_id;
    v_name:=nullif(pg_catalog.btrim(v_origin.beneficiary_name),'');
    v_sort:=v_origin.sort_code;v_account:=v_origin.account_number;v_hash:=v_origin.bank_details_hash;
  end if;
  if v_candidate.active is not true
     or pg_catalog.upper(coalesce(v_candidate.pay_method,''))<>v_worker.target_pay_channel
     or v_name is null
     or v_sort !~ '^[0-9]{6}$'
     or v_account !~ '^[0-9]{8}$'
     or v_hash is null
     or (v_worker.target_pay_channel='PAYE' and
         v_leg.leg_kind is distinct from 'ONEOFF' and v_hash is distinct from v_candidate.bank_details_hash) then
    raise exception using errcode='55000',
      message='BPAY_NEXT_CSV_BANK_DETAILS_NOT_READY';
  end if;
  v_replay:=coalesce(v_transfer.destination_rail='CSV'
    and v_transfer.beneficiary_name_snapshot=v_name
    and v_transfer.sort_code_snapshot=v_sort
    and v_transfer.account_number_snapshot=v_account
    and v_transfer.bank_details_hash_snapshot=v_hash,false);
  if not v_replay then
    update private.bpay_next_transfer
      set destination_rail='CSV',beneficiary_name_snapshot=v_name,
          sort_code_snapshot=v_sort,account_number_snapshot=v_account,
          bank_details_hash_snapshot=v_hash
      where id=p_transfer_id;
  end if;
  return pg_catalog.jsonb_build_object('transfer_id',p_transfer_id,
    'phase','MEMBERS_READY','destination_rail','CSV',
    'bank_hash',v_hash,'replay',v_replay,
    'instruction_issued',false);
end
$function$;

create or replace function private.bpay_next_issue_simple_csv_v1(
  p_instruction_id uuid,p_transfer_id uuid,p_actor_user_id uuid
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_run_id uuid;
  v_run private.bpay_next_pay_run%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_transfer private.bpay_next_transfer%rowtype;
  v_candidate public.candidates%rowtype;
  v_umbrella public.umbrellas%rowtype;
  v_actor public.tms_users%rowtype;
  v_existing private.bpay_next_csv_instruction%rowtype;
  v_leg private.bpay_next_destination_group_leg%rowtype;
  v_origin private.bpay_next_stored_credit_origin%rowtype;
  v_columns jsonb;
  v_options jsonb;
  v_with_account_type boolean;
  v_name text;
  v_sort text;
  v_account text;
  v_hash text;
  v_reference text;
  v_file_name text;
  v_csv text;
  v_sha text;
begin
  if p_instruction_id is null or p_transfer_id is null
     or p_actor_user_id is null then
    raise exception using errcode='22023',
      message='BPAY_NEXT_CSV_ISSUE_INPUT_INVALID';
  end if;
  if not exists(select 1 from private.bpay_next_module_control m
                where m.id=1 and m.active_owner='NEXT' for share) then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select w.run_id into strict v_run_id
    from private.bpay_next_transfer t
    join private.bpay_next_run_worker w on w.id=t.run_worker_id
    where t.id=p_transfer_id;
  select * into strict v_run from private.bpay_next_pay_run
    where id=v_run_id for update;
  select w.* into strict v_worker from private.bpay_next_run_worker w
    join private.bpay_next_transfer t on t.run_worker_id=w.id
    where t.id=p_transfer_id for update of w;
  select * into strict v_transfer from private.bpay_next_transfer
    where id=p_transfer_id for update;
  select * into strict v_actor from public.tms_users
    where id=p_actor_user_id for share;
  if v_actor.is_active is not true
     or (v_actor.payment_authoriser is not true
         and v_actor.payment_golden_key is not true) then
    raise exception using errcode='42501',
      message='BPAY_NEXT_CSV_ACTOR_NOT_AUTHORISED';
  end if;
  select * into v_existing from private.bpay_next_csv_instruction
    where id=p_instruction_id;
  if found then
    if v_existing.transfer_id<>p_transfer_id then
      raise exception using errcode='23514',
        message='BPAY_NEXT_CSV_ISSUE_REPLAY_CONFLICT';
    end if;
    return pg_catalog.jsonb_build_object(
      'instruction_id',v_existing.id,'transfer_id',p_transfer_id,
      'file_name',v_existing.file_name,'csv_sha256',v_existing.csv_sha256,
      'csv_text',v_existing.csv_text,'replay',true,
      'payment_recorded',exists(select 1 from private.bpay_next_outcome_request r
        where r.transfer_id=p_transfer_id and r.posting_complete));
  end if;
  if exists(select 1 from private.bpay_next_csv_instruction i
            where i.transfer_id=p_transfer_id) then
    raise exception using errcode='23514',
      message='BPAY_NEXT_CSV_TRANSFER_ALREADY_ISSUED';
  end if;
  -- Exact issued receipt readback above precedes current bank safety checks.
  v_leg:=private.bpay_next_destination_execution_leg_v1(p_transfer_id);
  if v_run.status<>'DRAFT' or v_run.confirmed_at_utc is null
     or exists(select 1 from private.bpay_next_cancel_request r
               where r.run_worker_id=v_worker.id and r.status in ('REQUESTED','CANCELLING'))
     or (v_worker.status<>'READY' and not (v_worker.status='COMPLETE'
       and v_leg.transfer_id is not null and v_transfer.original_transfer_id is not null))
     or v_worker.target_pay_channel not in ('PAYE','UMBRELLA')
     or v_transfer.status<>'MEMBERS_READY'
     or v_transfer.candidate_id<>v_worker.candidate_id
     or not ((v_worker.target_pay_channel='PAYE' and
          v_transfer.beneficiary_kind='CANDIDATE' and v_transfer.beneficiary_id=v_worker.candidate_id)
       or (v_worker.target_pay_channel='UMBRELLA' and v_worker.target_umbrella_id is not null and
          v_transfer.beneficiary_kind='UMBRELLA' and v_transfer.beneficiary_id=v_worker.target_umbrella_id))
     or v_transfer.destination_rail<>'CSV'
     or v_transfer.account_approval_ref is not null
     or v_transfer.member_count<=0
     or v_transfer.member_cash_sum<>v_transfer.cash_amount
     or v_transfer.cash_amount<=0 then
    raise exception using errcode='55000',
      message='BPAY_NEXT_CSV_ISSUE_NOT_ELIGIBLE';
  end if;
  -- The Candidate row lock makes the actual issue and a simultaneous bank
  -- change resolve in one order. No historical account or Timesheet scan.
  select * into strict v_candidate from public.candidates
    where id=v_worker.candidate_id for share;
  v_name:=nullif(pg_catalog.btrim(v_candidate.account_holder),'');
  v_sort:=pg_catalog.regexp_replace(
    coalesce(v_candidate.sort_code,''),'[^0-9]','','g');
  v_account:=pg_catalog.regexp_replace(
    coalesce(v_candidate.account_number,''),'[^0-9]','','g');
  v_hash:=public._bank_hash(v_candidate.sort_code,
    v_candidate.account_number,v_candidate.account_holder);
  if v_worker.target_pay_channel='UMBRELLA' then
    if v_leg.transfer_id is not null or v_worker.case_selection_revision<>0
       or v_candidate.active is not true
       or pg_catalog.upper(coalesce(v_candidate.pay_method,''))<>'UMBRELLA'
       or v_candidate.umbrella_id is distinct from v_worker.target_umbrella_id then
      raise exception using errcode='55000',message='BPAY_NEXT_CSV_UMBRELLA_DESTINATION_NOT_READY';
    end if;
    select u.* into strict v_umbrella from public.umbrellas u where u.id=v_worker.target_umbrella_id for share;
    v_name:=nullif(pg_catalog.btrim(v_umbrella.name),'');
    v_sort:=pg_catalog.regexp_replace(coalesce(v_umbrella.sort_code,''),'[^0-9]','','g');
    v_account:=pg_catalog.regexp_replace(coalesce(v_umbrella.account_number,''),'[^0-9]','','g');
    v_hash:=public._bank_hash(v_umbrella.sort_code,v_umbrella.account_number,v_umbrella.name);
    if v_umbrella.enabled is not true or v_hash is distinct from v_umbrella.bank_details_hash then
      raise exception using errcode='55000',message='BPAY_NEXT_CSV_UMBRELLA_DESTINATION_NOT_READY';
    end if;
  end if;
  if v_leg.leg_kind='ONEOFF' then
    select o.* into strict v_origin from private.bpay_next_net_destination_amount d
      join private.bpay_next_stored_credit_origin o on o.command_id=d.first_origin_command_id
      where d.destination_id=v_leg.destination_id;
    v_name:=nullif(pg_catalog.btrim(v_origin.beneficiary_name),'');
    v_sort:=v_origin.sort_code;v_account:=v_origin.account_number;v_hash:=v_origin.bank_details_hash;
  end if;
  if v_candidate.active is not true
     or pg_catalog.upper(coalesce(v_candidate.pay_method,''))<>v_worker.target_pay_channel
     or v_name is null
     or pg_catalog.octet_length(v_name)>70
     or position(E'\r' in v_name)>0
     or position(E'\n' in v_name)>0
     or v_sort !~ '^[0-9]{6}$'
     or v_account !~ '^[0-9]{8}$'
     or v_hash is null
     or (v_worker.target_pay_channel='PAYE' and
         v_leg.leg_kind is distinct from 'ONEOFF' and v_hash is distinct from v_candidate.bank_details_hash)
     or v_name is distinct from v_transfer.beneficiary_name_snapshot
     or v_sort is distinct from v_transfer.sort_code_snapshot
     or v_account is distinct from v_transfer.account_number_snapshot
     or v_hash is distinct from v_transfer.bank_details_hash_snapshot then
    raise exception using errcode='55000',
      message='BPAY_NEXT_CSV_DESTINATION_CHANGED_BEFORE_ISSUE';
  end if;
  select sd.pay_export_csv_columns_json,
         coalesce(sd.pay_export_csv_format_json,'{}'::jsonb)
    into strict v_columns,v_options
    from public.settings_defaults sd where sd.id=1;
  if v_options<>'{}'::jsonb then
    raise exception using errcode='55000',
      message='BPAY_NEXT_CSV_CONFIGURED_FORMAT_NOT_YET_SUPPORTED';
  end if;
  v_with_account_type:=coalesce(v_columns=
    '["payment_reference","payee_name","sort_code","account_number","account_type","amount"]'::jsonb,false);
  if not v_with_account_type and v_columns is distinct from
      '["payment_reference","payee_name","sort_code","account_number","amount"]'::jsonb then
    raise exception using errcode='55000',
      message='BPAY_NEXT_CSV_CONFIGURED_COLUMNS_NOT_YET_SUPPORTED';
  end if;
  v_reference:='BP'||substring(
    pg_catalog.replace(p_transfer_id::text,'-','') from 1 for 18);
  v_file_name:='bpay-next-'||p_instruction_id::text||'.csv';
  v_csv:='Payment reference,Payee name,Sort code,Bank account number,';
  if v_with_account_type then
    v_csv:=v_csv||'Bank account type,';
  end if;
  v_csv:=v_csv||'Amount'||E'\n'||v_reference||',"'||
    pg_catalog.replace(v_name,'"','""')||'",'||v_sort||','||v_account||',';
  if v_with_account_type then
    v_csv:=v_csv||',';
  end if;
  v_csv:=v_csv||pg_catalog.to_char(v_transfer.cash_amount,
    'FM9999999999999990.00')||E'\n';
  v_sha:=pg_catalog.encode(pg_catalog.sha256(
    pg_catalog.convert_to(v_csv,'UTF8')),'hex');
  insert into private.bpay_next_csv_instruction
    (id,transfer_id,run_worker_id,actor_user_id,payment_reference,
     bank_details_hash_snapshot,cash_amount,file_name,csv_text,csv_sha256)
    values(p_instruction_id,p_transfer_id,v_worker.id,p_actor_user_id,
      v_reference,v_hash,v_transfer.cash_amount,v_file_name,v_csv,v_sha);
  update private.bpay_next_transfer
    set account_approval_ref='CSV_INSTRUCTION:'||p_instruction_id::text,
        status='ISSUED_CSV'
    where id=p_transfer_id;
  return pg_catalog.jsonb_build_object(
    'instruction_id',p_instruction_id,'transfer_id',p_transfer_id,
    'file_name',v_file_name,'csv_sha256',v_sha,'csv_text',v_csv,
    'replay',false,'payment_recorded',false);
end
$function$;

alter function private.bpay_next_bind_simple_csv_destination_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_bind_simple_csv_destination_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_issue_simple_csv_v1(uuid,uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_issue_simple_csv_v1(uuid,uuid,uuid) from public,anon,authenticated,service_role;


create or replace function private.bpay_next_receive_simple_reissue_v1(
  p_command_id uuid,p_return_cash_id uuid,p_actor_user_id uuid
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_run uuid;
  v_original private.bpay_next_transfer%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_actor public.tms_users%rowtype;
  v_request private.bpay_next_reissue_request%rowtype;
  v_cash private.bpay_next_return_cash%rowtype;
  v_sequence bigint;
begin
  if p_command_id is null or p_return_cash_id is null or p_actor_user_id is null then
    raise exception using errcode='22023',message='BPAY_NEXT_REISSUE_INPUT_INVALID';
  end if;
  if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share) then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select w.run_id into strict v_run from private.bpay_next_return_cash c
    join private.bpay_next_transfer t on t.id=c.original_transfer_id
    join private.bpay_next_run_worker w on w.id=t.run_worker_id where c.id=p_return_cash_id;
  perform 1 from private.bpay_next_pay_run where id=v_run for update;
  select t.* into strict v_original from private.bpay_next_return_cash c
    join private.bpay_next_transfer t on t.id=c.original_transfer_id where c.id=p_return_cash_id;
  select w.* into strict v_worker from private.bpay_next_run_worker w where w.id=v_original.run_worker_id for update;
  select * into strict v_actor from public.tms_users where id=p_actor_user_id for share;
  if v_actor.is_active is not true or
     (v_actor.payment_authoriser is not true and v_actor.payment_golden_key is not true) then
    raise exception using errcode='42501',message='BPAY_NEXT_REISSUE_ACTOR_NOT_AUTHORISED';
  end if;
  select * into v_request from private.bpay_next_reissue_request where command_id=p_command_id;
  if found then
    if v_request.return_cash_id<>p_return_cash_id then
      raise exception using errcode='23514',message='BPAY_NEXT_REISSUE_REPLAY_CONFLICT';
    end if;
    return pg_catalog.jsonb_build_object('command_id',p_command_id,'transfer_id',v_request.transfer_id,'replay',true);
  end if;
  if v_original.status<>'RETURNED'
     or not ((v_original.beneficiary_kind='CANDIDATE' and v_original.beneficiary_id=v_original.candidate_id)
       or (v_worker.target_pay_channel='UMBRELLA' and v_worker.case_selection_revision=0
         and v_worker.target_umbrella_id is not null and v_original.beneficiary_kind='UMBRELLA'
         and v_original.beneficiary_id=v_worker.target_umbrella_id))
     or not exists(select 1 from private.bpay_next_return_request r
                   where r.transfer_id=v_original.id and r.posting_complete) then
    raise exception using errcode='55000',message='BPAY_NEXT_REISSUE_RETURN_NOT_COMPLETE';
  end if;
  -- Reject duplicate NEW requests before receiving a command. Otherwise an
  -- unfulfillable earlier job could block the actual settlement/return.
  -- Run lock serialises all supported cash writers; the pending index also
  -- prevents two accepted requests before either one builds its transfer.
  select * into strict v_cash from private.bpay_next_return_cash where id=p_return_cash_id for update;
  if v_cash.amount_owed-v_cash.amount_held-v_cash.amount_reissued_paid<=0
     or exists(select 1 from private.bpay_next_reissue_request
               where return_cash_id=p_return_cash_id and transfer_id is null) then
    raise exception using errcode='55000',message='BPAY_NEXT_REISSUE_ALREADY_REQUESTED_OR_RESERVED';
  end if;
  v_sequence:=private.bpay_next_receive_command_v1(p_command_id,'CASH_REISSUE');
  insert into private.bpay_next_command_member(command_id,candidate_id,member_no)
    values(p_command_id,v_original.candidate_id,1);
  insert into private.bpay_next_reissue_request(command_id,return_cash_id,run_worker_id,candidate_id,actor_user_id)
    values(p_command_id,p_return_cash_id,v_original.run_worker_id,v_original.candidate_id,p_actor_user_id);
  update private.bpay_next_command set status='SEALED',expected_member_count=1,
    sealed_at_utc=pg_catalog.transaction_timestamp() where id=p_command_id;
  return pg_catalog.jsonb_build_object('command_id',p_command_id,'sequence',v_sequence,'replay',false);
end
$function$;

alter function private.bpay_next_receive_simple_reissue_v1(uuid,uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_receive_simple_reissue_v1(uuid,uuid,uuid) from public,anon,authenticated,service_role;

create or replace function private.bpay_next_apply_work_collection_recovery_v1(p_job_id uuid,p_case_event_id uuid)
returns void language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_job private.bpay_next_job%rowtype;v_event private.bpay_next_case_event%rowtype;
  v_basis private.bpay_next_work_collection%rowtype;v_instruction private.bpay_next_run_case_instruction%rowtype;
  v_effect private.bpay_next_financial_effect%rowtype;v_effect_id uuid;
  v_source_line private.bpay_next_run_line%rowtype;
begin
  select * into strict v_job from private.bpay_next_job where id=p_job_id;
  select * into strict v_event from private.bpay_next_case_event where id=p_case_event_id;
  select * into strict v_instruction from private.bpay_next_run_case_instruction where id=v_event.instruction_id;
  select * into strict v_basis from private.bpay_next_work_collection where id=v_instruction.work_collection_id;
  if v_job.job_kind not in ('CSV_SETTLEMENT','INTERNAL_SETTLEMENT') or v_job.phase<>'MEMBERS' or v_job.status<>'LEASED'
     or v_job.candidate_id<>v_basis.candidate_id or v_job.lease_until_utc<=pg_catalog.clock_timestamp()
     or (v_event.operation_id,v_event.case_id,v_event.case_component_id,v_event.candidate_id)
       is distinct from (v_job.command_id,v_basis.case_id,v_basis.case_component_id,v_basis.candidate_id)
     or v_event.event_kind<>'RECOVERED' or v_event.source_amount_ex_vat<=0
     or v_instruction.valuation_policy_id<>v_basis.original_valuation_policy_id
     or (v_event.source_amount_ex_vat,v_event.target_amount_ex_vat,v_event.target_amount_vat,v_event.target_amount_inc_vat)
       is distinct from (v_event.recovered_delta,v_event.recovered_delta,0::numeric,v_event.recovered_delta)
     or not exists(select 1 from private.bpay_next_module_control m
       join private.bpay_next_worker_control c on c.candidate_id=v_basis.candidate_id
       where m.id=1 and m.active_owner='NEXT' and m.owner_epoch=v_job.module_epoch and c.active_owner_epoch=v_job.owner_epoch)
     or not exists(select 1 from private.bpay_next_case_hold h
       join private.bpay_next_case_capacity_use u on u.case_hold_id=h.id and u.status='REALISED' and u.realisation_event_id=v_event.id
       join private.bpay_next_outcome_request r on r.command_id=v_job.command_id and r.transfer_id=v_event.original_transfer_id
       join private.bpay_next_transfer_member member on member.transfer_id=r.transfer_id and member.case_hold_id=h.id
         and member.case_instruction_id=v_instruction.id and member.case_allocation_result_id=v_event.allocation_result_id
       where h.id=v_event.operation_item_id and h.status='REALISED' and h.instruction_id=v_instruction.id
         and h.source_reserved_ex_vat=v_event.source_amount_ex_vat and h.run_worker_id=r.run_worker_id) then
    raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_RECOVERY_SCOPE_INVALID';
  end if;
  select l.* into strict v_source_line from private.bpay_next_run_line l where l.id=v_basis.original_run_line_id;
  insert into private.bpay_next_financial_effect(operation_id,operation_item_id,effect_kind,candidate_id,
    work_id,component_key,case_id,original_timesheet_id,original_transfer_id,source_disposition_ex_vat,
    target_amount_ex_vat,target_amount_vat,target_amount_inc_vat,occurred_at_utc,source_run_line_id,
    source_binding_version_id,canonical_source_work_id,canonical_source_component_key,native_source_channel,source_case_event_id)
    select v_job.command_id,v_event.id,'WORK_COLLECTION_RECOVERED',v_basis.candidate_id,
      v_basis.work_id,v_basis.component_key,v_basis.case_id,w.original_timesheet_id,v_event.original_transfer_id,
      -v_event.source_amount_ex_vat,-v_event.target_amount_ex_vat,-v_event.target_amount_vat,-v_event.target_amount_inc_vat,v_event.occurred_at_utc,
      case when v_source_line.source_binding_version_id is null then null else v_source_line.id end,
      v_source_line.source_binding_version_id,v_source_line.canonical_source_work_id,v_source_line.canonical_source_component_key,
      case when v_source_line.source_binding_version_id is null then null else v_source_line.source_pay_channel end,
      case when v_source_line.source_binding_version_id is null then null else v_event.id end
      from private.bpay_next_work w where w.id=v_basis.work_id
    on conflict(operation_id,operation_item_id,effect_kind) do nothing returning id into v_effect_id;
  if v_effect_id is null then
    select * into strict v_effect from private.bpay_next_financial_effect where operation_id=v_job.command_id
      and operation_item_id=v_event.id and effect_kind='WORK_COLLECTION_RECOVERED';
    if (v_effect.candidate_id,v_effect.work_id,v_effect.component_key,v_effect.case_id,v_effect.original_transfer_id,
        v_effect.source_disposition_ex_vat,v_effect.target_amount_ex_vat,v_effect.target_amount_vat,v_effect.target_amount_inc_vat)
      is distinct from (v_basis.candidate_id,v_basis.work_id,v_basis.component_key,v_basis.case_id,v_event.original_transfer_id,
        -v_event.source_amount_ex_vat,-v_event.target_amount_ex_vat,-v_event.target_amount_vat,-v_event.target_amount_inc_vat) then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_RECOVERY_REPLAY_CONFLICT';
    end if;
    if (v_effect.source_run_line_id,v_effect.source_binding_version_id,v_effect.canonical_source_work_id,
        v_effect.canonical_source_component_key,v_effect.native_source_channel,v_effect.source_case_event_id)
      is distinct from (case when v_source_line.source_binding_version_id is null then null else v_source_line.id end,
        v_source_line.source_binding_version_id,v_source_line.canonical_source_work_id,v_source_line.canonical_source_component_key,
        case when v_source_line.source_binding_version_id is null then null else v_source_line.source_pay_channel end,
        case when v_source_line.source_binding_version_id is null then null else v_event.id end) then
      raise exception using errcode='23514',message='BPAY_NEXT_SOURCE_BOOK_EFFECT_REPLAY_CONFLICT';
    end if;
    return;
  end if;
  update private.bpay_next_position set realised_source_ex_vat=realised_source_ex_vat-v_event.source_amount_ex_vat,
    realised_target_ex_vat=realised_target_ex_vat-v_event.target_amount_ex_vat,
    realised_target_vat=realised_target_vat-v_event.target_amount_vat,
    realised_target_inc_vat=realised_target_inc_vat-v_event.target_amount_inc_vat,
    updated_at_utc=pg_catalog.transaction_timestamp()
    where work_id=v_basis.work_id and component_key=v_basis.component_key and source_basis_channel='PAYE'
      and realised_source_ex_vat>=v_event.source_amount_ex_vat
      and (realised_target_ex_vat,realised_target_vat,realised_target_inc_vat)
        is not distinct from (realised_source_ex_vat,0::numeric,realised_source_ex_vat);
  if not found then raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_RECOVERY_POSITION_INVALID';end if;
  perform private.bpay_next_reconcile_work_collection_v1(p_job_id,v_basis.work_id,v_basis.component_key,v_effect_id);
end
$function$;

alter function private.bpay_next_apply_work_collection_recovery_v1(uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_apply_work_collection_recovery_v1(uuid,uuid) from public,anon,authenticated,service_role;

commit;
