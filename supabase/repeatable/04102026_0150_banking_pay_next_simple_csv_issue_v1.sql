-- Owner-only, single-Candidate CSV issue for the first paid-outcome slice.
-- It does not mark money paid. The exact file bytes are stored before they
-- become available; replay returns those bytes, never a repriced/new file.
-- The backend still must authenticate and reauthorise the Office actor.
\set ON_ERROR_STOP on

begin;

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
     or v_worker.target_pay_channel<>'PAYE'
     or v_transfer.status<>'MEMBERS_READY'
     or v_transfer.candidate_id<>v_worker.candidate_id
     or v_transfer.beneficiary_kind<>'CANDIDATE'
     or v_transfer.beneficiary_id<>v_worker.candidate_id
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
  if v_leg.leg_kind='ONEOFF' then
    select o.* into strict v_origin from private.bpay_next_net_destination_amount d
      join private.bpay_next_stored_credit_origin o on o.command_id=d.first_origin_command_id
      where d.destination_id=v_leg.destination_id;
    v_name:=nullif(pg_catalog.btrim(v_origin.beneficiary_name),'');
    v_sort:=v_origin.sort_code;v_account:=v_origin.account_number;v_hash:=v_origin.bank_details_hash;
  end if;
  if v_candidate.active is not true
     or pg_catalog.upper(coalesce(v_candidate.pay_method,''))<>'PAYE'
     or v_name is null
     or pg_catalog.octet_length(v_name)>70
     or position(E'\r' in v_name)>0
     or position(E'\n' in v_name)>0
     or v_sort !~ '^[0-9]{6}$'
     or v_account !~ '^[0-9]{8}$'
     or v_hash is null
     or (v_leg.leg_kind is distinct from 'ONEOFF' and v_hash is distinct from v_candidate.bank_details_hash)
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

alter function private.bpay_next_issue_simple_csv_v1(uuid,uuid,uuid)
  owner to postgres;
revoke all on function private.bpay_next_issue_simple_csv_v1(uuid,uuid,uuid)
  from public,anon,authenticated,service_role;

commit;
