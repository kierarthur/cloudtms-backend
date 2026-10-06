-- Batch-only returned-cash facts. No bank-check/issue eligibility certificate,
-- new payroll, recovery, legacy writer, financial mutation or history rebuild.
\set ON_ERROR_STOP on
begin;
create or replace function private.bpay_next_return_cash_page_v1(
  p_run_id uuid,p_worker_id uuid,p_after_transfer_no integer,p_limit integer
) returns jsonb language plpgsql stable security invoker
set search_path=pg_catalog,private
as $function$
declare
  v_header jsonb;v_candidate uuid;v_row record;
  v_return private.bpay_next_return_request%rowtype;
  v_cash private.bpay_next_return_cash%rowtype;
  v_pending private.bpay_next_reissue_request%rowtype;
  v_rows jsonb:='[]'::jsonb;v_value jsonb;v_query text;
  v_scan integer;v_count integer:=0;v_bytes integer;v_more boolean:=false;
  v_available numeric;v_action text;
begin
  if p_run_id is null or p_worker_id is null or p_limit is null or p_limit not between 1 and 100
     or (p_after_transfer_no is not null and p_after_transfer_no<1) then
    raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
  end if;
  if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT') then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select w.candidate_id,pg_catalog.jsonb_build_object('run_id',r.id,'run_worker_id',w.id,
    'candidate_id',w.candidate_id,'financial_view_revision',c.financial_view_revision::text)
    into v_candidate,v_header
  from private.bpay_next_pay_run r join private.bpay_next_run_worker w on w.run_id=r.id
  join private.bpay_next_worker_control c on c.candidate_id=w.candidate_id
  where r.id=p_run_id and w.id=p_worker_id;
  if not found then raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_SCOPE_INVALID';end if;
  v_bytes:=512+pg_catalog.octet_length(v_header::text);
  -- Existing UNIQUE(worker,transfer_no) seek. Limit the RAW execution window
  -- before filtering originals/returns; an empty output can still advance.
  -- Two fixed query shapes prevent a nullable-OR generic-plan prefix filter.
  v_query:='select t.id,t.transfer_no,t.candidate_id,t.original_transfer_id,t.status,
    t.cash_amount,t.beneficiary_kind,t.beneficiary_id
    from private.bpay_next_transfer t where t.run_worker_id=$1';
  if p_after_transfer_no is not null then v_query:=v_query||' and t.transfer_no>$2';end if;
  v_query:=v_query||' order by t.transfer_no limit $3';
  for v_row in execute v_query using p_worker_id,p_after_transfer_no,p_limit loop
    if v_row.candidate_id<>v_candidate then
      raise exception using errcode='23514',message='BPAY_NEXT_RETURN_CASH_SCOPE_INVALID';
    end if;
    if v_row.original_transfer_id is null then
      select * into v_return from private.bpay_next_return_request where transfer_id=v_row.id;
      select * into v_cash from private.bpay_next_return_cash where original_transfer_id=v_row.id;
      if v_row.status='RETURNED' or v_return.command_id is not null or v_cash.id is not null then
        -- Missing is NOT zero. RETURNED before ordered posting legitimately
        -- has no cash row; a completed original posting must have that row.
        if v_row.status<>'RETURNED' or v_return.command_id is null
          or (v_return.run_worker_id,v_return.candidate_id) is distinct from (p_worker_id,v_candidate)
          or not exists(select 1 from private.bpay_next_transfer_outcome o where o.id=v_return.outcome_id
            and o.transfer_id=v_row.id and o.outcome_kind='RETURNED' and o.whole_transfer_amount=v_row.cash_amount)
          or v_return.posting_complete<>(v_cash.id is not null)
          or (v_cash.id is not null and (v_cash.candidate_id<>v_candidate or v_cash.amount_owed<>v_row.cash_amount)) then
          raise exception using errcode='23514',message='BPAY_NEXT_RETURN_CASH_SCOPE_INVALID';
        end if;
        v_pending:=null;v_available:=null;
        if v_cash.id is not null then
          select * into v_pending from private.bpay_next_reissue_request
            where return_cash_id=v_cash.id and transfer_id is null;
          if v_pending.command_id is not null
            and (v_pending.run_worker_id,v_pending.candidate_id) is distinct from (p_worker_id,v_candidate) then
            raise exception using errcode='23514',message='BPAY_NEXT_RETURN_CASH_SCOPE_INVALID';
          end if;
          v_available:=v_cash.amount_owed-v_cash.amount_held-v_cash.amount_reissued_paid;
        end if;
        v_action:=case
          when not v_return.posting_complete then 'RETURN_POSTING_PENDING'
          when v_row.beneficiary_kind<>'CANDIDATE' or v_row.beneficiary_id<>v_candidate then 'UNSUPPORTED_BENEFICIARY'
          when v_pending.command_id is not null then 'REISSUE_REQUESTED'
          when v_available>0 then 'CASH_AVAILABLE'
          when v_cash.amount_held>0 then 'CASH_RESERVED'
          else 'CASH_REPAID' end;
        v_value:=pg_catalog.jsonb_build_object(
          'original_transfer_id',v_row.id,'original_transfer_no',v_row.transfer_no::text,
          'original_transfer_status',v_row.status,'original_cash_amount',v_row.cash_amount::text,
          'beneficiary_kind',v_row.beneficiary_kind,'beneficiary_id',v_row.beneficiary_id,
          'return_command_id',v_return.command_id,'return_posting_complete',v_return.posting_complete,
          'return_cash_id',v_cash.id,'amount_owed',v_cash.amount_owed::text,'amount_held',v_cash.amount_held::text,
          'amount_reissued_paid',v_cash.amount_reissued_paid::text,'amount_available',v_available::text,
          'pending_reissue_command_id',v_pending.command_id,'action_state',v_action);
        if v_bytes+pg_catalog.octet_length(v_value::text)+1>120000 then
          if v_scan is null then raise exception using errcode='54000',message='BPAY_NEXT_PREPARATION_ROW_TOO_LARGE';end if;
          v_more:=true;exit; -- this raw row remains unconsumed
        end if;
        v_bytes:=v_bytes+pg_catalog.octet_length(v_value::text)+1;
        v_rows:=v_rows||pg_catalog.jsonb_build_array(v_value);
      end if;
    end if;
    v_scan:=v_row.transfer_no;v_count:=v_count+1;
  end loop;
  -- Exact full window deliberately requires an empty terminal read, as STATUS
  -- does. scan_cursor follows examined raw rows, not the last displayed cash.
  return pg_catalog.jsonb_build_object('header',v_header,'rows',v_rows,
    'complete',not(v_more or v_count=p_limit),'scan_cursor',v_scan::text);
end
$function$;
alter function private.bpay_next_return_cash_page_v1(uuid,uuid,integer,integer) owner to postgres;
revoke all on function private.bpay_next_return_cash_page_v1(uuid,uuid,integer,integer)
  from public,anon,authenticated,service_role;
commit;
