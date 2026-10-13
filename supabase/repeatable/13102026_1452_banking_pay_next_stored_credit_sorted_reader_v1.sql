-- XD01 / BD179: same masked current reader, exact server sort keys.
-- Original three-argument2049 remains unchanged; XD07 owns oversized rows.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_current_stored_credit_page_v1(
  p_candidate_id uuid,p_after_legacy_case_id uuid,p_limit integer,p_sort text
) returns jsonb language plpgsql stable security invoker
set search_path=pg_catalog,private,public
set statement_timeout='5s'
as $function$
declare
  v_query text;v_raw record;v_value jsonb;v_rows jsonb:='[]'::jsonb;v_count integer:=0;v_seen integer:=0;
  v_bytes integer:=512;v_more boolean:=false;v_scan_cursor uuid;
  v_anchor_text text;v_anchor_amount numeric;v_anchor_time timestamptz;
  v_desc boolean;v_direction text;v_compare text;
begin
  if p_candidate_id is null or p_limit is null or p_limit not between 1 and 100
    or p_sort is null or p_sort not in ('CREDIT_ASC','CREDIT_DESC','CREATED_ASC','CREATED_DESC','AMOUNT_ASC','AMOUNT_DESC','PAID_TO_ASC','PAID_TO_DESC') then
    raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
  if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT') then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  if not exists(select 1 from public.candidates where id=p_candidate_id and active is true and pay_method='PAYE') then
    return pg_catalog.jsonb_build_object('rows',v_rows,'complete',true,'scan_cursor',null);end if;
  -- Resolve only a current native Candidate/UUID anchor, even when it was
  -- excluded/bound. No caller bank/name/amount becomes a sorting operand.
  v_desc:=pg_catalog.right(p_sort,4)='DESC';
  v_direction:=case when v_desc then ' desc' else ' asc' end;
  v_compare:=case when v_desc then '<' else '>' end;
  if p_after_legacy_case_id is not null then
    if p_sort like 'PAID_TO_%' then
      select b.beneficiary_name into v_anchor_text
      from public.pay_finance_case_oneoff_payout_bank_details b
      where b.finance_case_id=p_after_legacy_case_id and b.candidate_id=p_candidate_id;
    else
      select a.created_at,a.original_amount into v_anchor_time,v_anchor_amount
      from public.pay_advances a where a.id=p_after_legacy_case_id and a.candidate_id=p_candidate_id;
    end if;
    if not found then raise exception using errcode='23514',message='BPAY_NEXT_PREPARATION_CURSOR_CHANGED';end if;
  end if;
  -- <=limit+1 returned raw identities precede unchanged <=limit point checks.
  -- Paid-to prefix ties retain full C-name ordering, not a physical-visit cap.
  if p_sort like 'PAID_TO_%' then
    v_query:=$query$select b.finance_case_id as id
      from public.pay_finance_case_oneoff_payout_bank_details b where b.candidate_id=$1$query$;
    if p_after_legacy_case_id is not null then
      v_query:=v_query||' and (pg_catalog.left(b.beneficiary_name,128) collate "C",b.beneficiary_name collate "C",b.finance_case_id)'||v_compare||
        '(pg_catalog.left($4::text,128) collate "C",$4::text collate "C",$2::uuid)';
    end if;
    v_query:=v_query||' order by pg_catalog.left(b.beneficiary_name,128) collate "C"'||v_direction||
      ',b.beneficiary_name collate "C"'||v_direction||',b.finance_case_id'||v_direction||' limit $3';
  else
    v_query:=$query$select a.id from public.pay_advances a
      where a.candidate_id=$1 and a.case_type='MANUAL_CREDIT_ADJUSTMENT'
        and a.taxability='NON_TAXABLE' and a.routing_kind='ONE_OFF_SPECIFIED_BANK_ACCOUNT'
        and a.oneoff_bank_details_required is true and a.status='ACTIVE' and a.payout_status='PENDING'$query$;
    if p_sort like 'CREATED_%' then
      if p_after_legacy_case_id is not null then v_query:=v_query||' and (a.created_at,a.id)'||v_compare||'($5::timestamptz,$2::uuid)';end if;
      v_query:=v_query||' order by a.created_at'||v_direction||',a.id'||v_direction||' limit $3';
    elsif p_sort like 'AMOUNT_%' then
      if p_after_legacy_case_id is not null then v_query:=v_query||' and (a.original_amount,a.id)'||v_compare||'($6::numeric,$2::uuid)';end if;
      v_query:=v_query||' order by a.original_amount'||v_direction||',a.id'||v_direction||' limit $3';
    else
      if p_after_legacy_case_id is not null then v_query:=v_query||' and a.id'||v_compare||'$2::uuid';end if;
      v_query:=v_query||' order by a.id'||v_direction||' limit $3';
    end if;
  end if;
  for v_raw in execute v_query using p_candidate_id,p_after_legacy_case_id,p_limit+1,v_anchor_text,v_anchor_time,v_anchor_amount loop
    if v_seen=p_limit then v_more:=true;exit;end if;
    -- <=100 point validations, each component uniqueness probe <=2, bank PK
    -- and bound-origin UNIQUE lookup. No eligibility or financial calculation.
    select pg_catalog.to_jsonb(page) into v_value from (
      select a.id as legacy_case_id,a.candidate_id,c.source_amount::text as principal_source_ex_vat,
        c.source_pay_method as original_source_pay_channel,
        a.taxability::text as original_tax_treatment,a.routing_kind::text as original_routing_kind,
        a.created_at as original_created_at_utc,b.updated_at_utc as bank_version_at_utc,
        b.bank_details_hash,b.beneficiary_name,pg_catalog.right(b.account_number,4) as account_last4
      from public.pay_advances a
      cross join lateral (
        select count(*) as component_count,min(probe.id::text)::uuid as component_id
        from (select pc.id from public.pay_finance_case_components pc
          where pc.finance_case_id=a.id order by pc.id limit 2) probe
      ) identity_probe
      join public.pay_finance_case_components c on c.id=identity_probe.component_id and identity_probe.component_count=1
      join public.pay_finance_case_oneoff_payout_bank_details b on b.finance_case_id=a.id and b.candidate_id=a.candidate_id
      where a.id=v_raw.id and a.candidate_id=p_candidate_id and a.case_type='MANUAL_CREDIT_ADJUSTMENT'
        and a.taxability='NON_TAXABLE' and a.routing_kind='ONE_OFF_SPECIFIED_BANK_ACCOUNT'
        and a.oneoff_bank_details_required is true and a.status='ACTIVE' and a.payout_status='PENDING'
        and a.payout_pay_batch_id is null and a.payout_transfer_id is null
        and a.original_amount>0 and a.outstanding_amount=a.original_amount
        and a.cleared_at_utc is null and a.written_off_at_utc is null
        and c.finance_case_id=a.id and c.candidate_id=a.candidate_id and c.source_pay_method='UMBRELLA'
        and c.classification='REIMBURSEMENT_GROSS_FIXED' and c.component_key_type='CASE_TOTAL' and c.component_key_value='TOTAL'
        and c.source_family_key='case:'||a.id::text and c.linked_timesheet_id is null and c.closed_at_utc is null
        and c.source_amount=a.original_amount and c.remaining_source_amount=c.source_amount
        and c.source_amount not in ('NaN'::numeric,'Infinity'::numeric,'-Infinity'::numeric)
        and c.source_basis_json->>'case_type'='MANUAL_CREDIT_ADJUSTMENT'
        and c.source_basis_json->>'taxability'='NON_TAXABLE'
        and c.source_basis_json->>'routing_kind'='ONE_OFF_SPECIFIED_BANK_ACCOUNT'
        and pg_catalog.isfinite(a.created_at) and pg_catalog.isfinite(b.updated_at_utc)
        and pg_catalog.octet_length(b.bank_details_hash) between 1 and 256
        and nullif(pg_catalog.btrim(b.beneficiary_name),'') is not null
        and b.account_number~'^[0-9]{8}$'
        and public._bank_hash(b.sort_code,b.account_number,b.beneficiary_name)=b.bank_details_hash
        and not exists(select 1 from private.bpay_next_stored_credit_origin o where o.legacy_case_id=a.id)
    ) page;
    if found then
      if v_bytes+pg_catalog.octet_length(v_value::text)+1>120000 then
        if v_count=0 then raise exception using errcode='54000',message='BPAY_NEXT_PREPARATION_ROW_TOO_LARGE';end if;
        -- Do not consume this eligible identity or lose its row on continuation.
        v_more:=true;exit;
      end if;
      v_rows:=v_rows||pg_catalog.jsonb_build_array(v_value);
      v_count:=v_count+1;v_bytes:=v_bytes+pg_catalog.octet_length(v_value::text)+1;
    end if;
    v_seen:=v_seen+1;v_scan_cursor:=v_raw.id;
  end loop;
  if v_more and (v_scan_cursor is null or (p_after_legacy_case_id is not null and v_scan_cursor=p_after_legacy_case_id)) then
    raise exception using errcode='23514',message='BPAY_NEXT_PREPARATION_SCAN_CURSOR_INVALID';end if;
  return pg_catalog.jsonb_build_object('rows',v_rows,'complete',not v_more,'scan_cursor',v_scan_cursor);
end
$function$;

alter function private.bpay_next_current_stored_credit_page_v1(uuid,uuid,integer,text) owner to postgres;
revoke all on function private.bpay_next_current_stored_credit_page_v1(uuid,uuid,integer,text) from public,anon,authenticated,service_role;

commit;
