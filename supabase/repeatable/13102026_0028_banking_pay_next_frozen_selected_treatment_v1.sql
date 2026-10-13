-- WB29: qualified frozen selected treatment; original source pages and admission retained.
\set ON_ERROR_STOP on
begin;
create or replace function public.bpay_next_frozen_detail_page_v1(
  p_actor_user_id uuid,p_kind text,p_run_line_id uuid default null,
  p_run_work_id uuid default null,p_shift_detail_id uuid default null,
  p_after_detail_no integer default null,p_after_break_no integer default null,
  p_after_bucket text default null,p_after_rate_family text default null,
  p_after_rate_code text default null,p_limit integer default 50
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_rows jsonb:='[]'::jsonb;
  v_sql text;
  v_page record;
  v_bytes bigint:=512;
  v_count integer:=0;
  v_more boolean:=false;
  v_origin jsonb;
  v_revision_id uuid;
  v_used_line jsonb;
  v_treatment jsonb;
  v_treatment_line private.bpay_next_run_line%rowtype;
  v_treatment_position private.bpay_next_run_position%rowtype;
  v_treatment_worker private.bpay_next_run_worker%rowtype;
  v_treatment_work private.bpay_next_run_work%rowtype;
  v_treatment_run private.bpay_next_pay_run%rowtype;
  v_selected_quote jsonb;
  v_selected_buckets jsonb:='[]'::jsonb;
  v_selected_bucket jsonb;

begin
  if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),
      nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
     or not exists(select 1 from public.tms_users u where u.id=p_actor_user_id
                   and u.is_active is true and u.role::text='admin') then
    raise exception using errcode='42501',message='BPAY_NEXT_FROZEN_FORBIDDEN';
  end if;
  if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT') then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  if p_kind is null or p_kind not in ('SHIFTS','BREAKS','RATE_DETAILS','RATE_SCHEDULE')
     or p_limit is null or p_limit not between 1 and 100 then
    raise exception using errcode='22023',message='BPAY_NEXT_FROZEN_REQUEST_INVALID';
  end if;
  if p_kind='RATE_SCHEDULE' then
    if p_run_work_id is null or p_run_line_id is not null or p_shift_detail_id is not null
       or p_after_detail_no is not null or p_after_break_no is not null or p_after_bucket is not null then
      raise exception using errcode='22023',message='BPAY_NEXT_FROZEN_REQUEST_INVALID';
    end if;
    v_sql:=$query$select to_jsonb(page) row_value
    from (select rate_schedule_id,rate_family,rate_code,unit_label,
          paye_rate::text,umbrella_rate::text,charge_rate::text
          from private.bpay_next_simple_rate_schedule_page_v1($1,$5,$6,$7)) page$query$;
  else
    if p_run_line_id is null or p_run_work_id is not null
       or p_after_rate_family is not null or p_after_rate_code is not null then
      raise exception using errcode='22023',message='BPAY_NEXT_FROZEN_REQUEST_INVALID';
    end if;
    if p_kind='SHIFTS' then
      if p_shift_detail_id is not null or p_after_break_no is not null or p_after_bucket is not null then
        raise exception using errcode='22023',message='BPAY_NEXT_FROZEN_REQUEST_INVALID';
      end if;
      v_sql:=$query$select to_jsonb(page) row_value
      from (select shift_detail_id,detail_no,work_date,shift_start_at,shift_end_at,
            shift_start_local,shift_end_local,shift_overnight,submitted_minutes,
            approved_minutes,approved_hours::text,detail_label,
            segment_pay_ex_vat::text,pay_excluded,
            hours_day::text,hours_night::text,hours_sat::text,hours_sun::text,hours_bh::text
            from private.bpay_next_simple_shift_page_v2($1,$3,$7)) page$query$;
    elsif p_kind='BREAKS' then
      if p_after_detail_no is not null or p_after_bucket is not null then
        raise exception using errcode='22023',message='BPAY_NEXT_FROZEN_REQUEST_INVALID';
      end if;
      v_sql:=$query$select to_jsonb(page) row_value
      from private.bpay_next_simple_break_page_v1($1,$2,$3,$7) page$query$;
    else
      if p_shift_detail_id is not null or p_after_detail_no is not null or p_after_break_no is not null then
        raise exception using errcode='22023',message='BPAY_NEXT_FROZEN_REQUEST_INVALID';
      end if;
      v_sql:=$query$select to_jsonb(page) row_value
      from (select rate_detail_id,bucket,approved_hours::text,source_pay_rate::text
            from private.bpay_next_simple_rate_detail_page_v1($1,$4,$7)) page$query$;
    end if;
  end if;
  -- One captured revision subject, never a current Client/Contract lookup.
  if p_kind='RATE_SCHEDULE' then
    select rw.captured_revision_id into strict v_revision_id
      from private.bpay_next_run_work rw where rw.id=p_run_work_id;
  else
    select g.captured_revision_id into strict v_revision_id
      from private.bpay_next_simple_frozen_line_guard_v1(p_run_line_id) g;
  end if;
  v_origin:=private.bpay_next_payment_client_origin_v1(v_revision_id);
  if p_kind='RATE_DETAILS' then
    -- The guarded immutable line/revision pair owns actual used quantities/rates.
    -- No live rate, amount division or capture/publisher change is permitted.
    select jsonb_build_object('component_kind',l.component_kind,
      'component_key',l.component_key,'unit_label',l.unit_label,
      'approved_quantity',l.approved_quantity::text,
      'approved_unit_rate',l.approved_unit_rate::text)
      into strict v_used_line
      from private.bpay_next_simple_frozen_line_guard_v1(p_run_line_id) g
      join private.bpay_next_approved_line l on l.id=g.approved_line_id
        and l.revision_id=g.captured_revision_id;
  end if;

  -- WB29: exact frozen line, never today's decision/target/Position.
  if p_kind='RATE_DETAILS' then
    select l.* into strict v_treatment_line from private.bpay_next_run_line l where l.id=p_run_line_id;
    if v_treatment_line.accepted_resolution_refs is not null
       and jsonb_array_length(v_treatment_line.accepted_resolution_refs)>0 then
      select p.* into v_treatment_position from private.bpay_next_run_position p
        where p.run_work_id=v_treatment_line.run_work_id and p.component_key=v_treatment_line.component_key;
      select w.* into v_treatment_worker from private.bpay_next_run_worker w where w.id=v_treatment_line.run_worker_id;
      select w.* into v_treatment_work from private.bpay_next_run_work w where w.id=v_treatment_line.run_work_id;
      select r.* into v_treatment_run from private.bpay_next_pay_run r where r.id=v_treatment_worker.run_id;
      if v_treatment_position.id is null or v_treatment_worker.id is null or v_treatment_work.id is null or v_treatment_run.id is null
        or (v_treatment_position.run_worker_id,v_treatment_position.work_id,v_treatment_position.captured_revision_id,v_treatment_position.approved_line_id)
          is distinct from (v_treatment_line.run_worker_id,v_treatment_line.work_id,v_treatment_line.captured_revision_id,v_treatment_line.approved_line_id)
        or (v_treatment_work.run_worker_id,v_treatment_work.work_id,v_treatment_work.captured_revision_id,v_treatment_work.candidate_id)
          is distinct from (v_treatment_worker.id,v_treatment_line.work_id,v_treatment_line.captured_revision_id,v_treatment_worker.candidate_id)
        or v_treatment_line.target_pay_channel is distinct from v_treatment_worker.target_pay_channel
        or v_treatment_line.source_pay_channel is distinct from v_treatment_position.source_pay_channel then
        raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_ACCEPTED_BASIS_CHANGED';end if;
      v_selected_quote:=private.bpay_next_captured_component_resolution_quote_v1(v_treatment_position.id,null);
      if v_selected_quote->'accepted_resolution_refs' is distinct from v_treatment_line.accepted_resolution_refs
        or (v_selected_quote->>'included_source_component_ex_vat')::numeric is distinct from v_treatment_line.source_consumed_ex_vat
        or ((v_selected_quote#>>'{component_quote,ex}')::numeric,(v_selected_quote#>>'{component_quote,vat}')::numeric,(v_selected_quote#>>'{component_quote,inc}')::numeric)
          is distinct from (v_treatment_line.frozen_ex_vat,v_treatment_line.frozen_vat,v_treatment_line.frozen_inc_vat)
        or ((v_selected_quote->>'valuation_policy_id')::uuid,(v_selected_quote->>'valuation_window_id')::uuid)
          is distinct from (v_treatment_line.valuation_policy_id,v_treatment_line.policy_window_id)
        or jsonb_typeof(v_selected_quote->'bucket_details') is distinct from 'array'
        or jsonb_array_length(v_selected_quote->'bucket_details') not between 1 and 5
        or (select count(distinct jsonb_build_array(x.value->>'rate_family',x.value->>'rate_code'))
          from jsonb_array_elements(v_selected_quote->'bucket_details') x)<>jsonb_array_length(v_selected_quote->'bucket_details') then
        raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_ACCEPTED_BASIS_CHANGED';end if;
      for v_selected_bucket in select x.value from jsonb_array_elements(v_selected_quote->'bucket_details') x loop
        if ((v_selected_bucket->>'valuation_policy_id')::uuid,(v_selected_bucket->>'valuation_window_id')::uuid)
          is distinct from (v_treatment_line.valuation_policy_id,v_treatment_line.policy_window_id) then
          raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_ACCEPTED_BASIS_CHANGED';end if;
        v_selected_buckets:=v_selected_buckets||jsonb_build_array(jsonb_build_object(
          'rate_family',v_selected_bucket->>'rate_family','rate_code',v_selected_bucket->>'rate_code',
          'decision_id',v_selected_bucket->>'decision_id','decision_version',v_selected_bucket->>'decision_version',
          'treatment',v_selected_bucket->>'treatment','resolution_mode',v_selected_bucket->>'resolution_mode',
          'approved_units',v_selected_bucket->>'approved_units','included_units',v_selected_bucket->>'included_units',
          'source_rate',v_selected_bucket->>'source_rate','source_ex_vat',v_selected_bucket->>'source_ex_vat',
          'manual_replacement_rate',v_selected_bucket->>'manual_replacement_rate','manual_amount_ex_vat',v_selected_bucket->>'manual_amount_ex_vat',
          'quote',jsonb_build_object('ex',v_selected_bucket#>>'{quote,ex}','vat',v_selected_bucket#>>'{quote,vat}',
            'inc',v_selected_bucket#>>'{quote,inc}','rate',v_selected_bucket#>>'{quote,rate}'),
          'valuation_policy_id',v_selected_bucket->>'valuation_policy_id','valuation_window_id',v_selected_bucket->>'valuation_window_id'));
      end loop;
      v_treatment:=jsonb_build_object('run_line_id',v_treatment_line.id,'run_work_id',v_treatment_work.id,
        'run_worker_id',v_treatment_worker.id,'candidate_id',v_treatment_worker.candidate_id,
        'captured_revision_id',v_treatment_line.captured_revision_id,'approved_line_id',v_treatment_line.approved_line_id,
        'component_key',v_treatment_line.component_key,'source_pay_channel',v_treatment_line.source_pay_channel,
        'target_pay_channel',v_treatment_worker.target_pay_channel,'target_umbrella_id',v_treatment_worker.target_umbrella_id,
        'target_umbrella_vat_chargeable',v_treatment_worker.target_umbrella_vat_chargeable,
        'valuation_policy_id',v_treatment_line.valuation_policy_id,'valuation_window_id',v_treatment_line.policy_window_id,
        'valuation_date',v_treatment_run.pay_date,'source_consumed_ex_vat',v_treatment_line.source_consumed_ex_vat::text,
        'component_quote',jsonb_build_object('ex',v_treatment_line.frozen_ex_vat::text,'vat',v_treatment_line.frozen_vat::text,'inc',v_treatment_line.frozen_inc_vat::text),
        'buckets',v_selected_buckets);
    end if;
    -- Metadata is charged once, before the original source row page.
    v_bytes:=v_bytes+coalesce(octet_length(v_treatment::text),4);
    if v_bytes>120000 then raise exception using errcode='54000',message='BPAY_NEXT_FROZEN_DETAIL_ROW_TOO_LARGE';end if;
  end if;
  v_bytes:=v_bytes+octet_length(v_origin::text)+coalesce(octet_length(v_used_line::text),4);
  if v_bytes>120000 then
    raise exception using errcode='54000',message='BPAY_NEXT_FROZEN_DETAIL_ROW_TOO_LARGE';
  end if;
  -- Fixed SQL choices only: no caller-supplied identifier/query. Each source
  -- returns <=100 rows. Charge encoded bytes BEFORE adding each row to the
  -- response; a byte-short page is explicitly incomplete, not end-of-data.
  for v_page in execute v_sql using coalesce(p_run_line_id,p_run_work_id),p_shift_detail_id,
    coalesce(p_after_detail_no,p_after_break_no),p_after_bucket,p_after_rate_family,p_after_rate_code,p_limit
  loop
    if v_bytes+octet_length(v_page.row_value::text)+1>120000 then
      if v_count=0 then
        raise exception using errcode='54000',message='BPAY_NEXT_FROZEN_DETAIL_ROW_TOO_LARGE';
      end if;
      v_more:=true;
      exit;
    end if;
    v_bytes:=v_bytes+octet_length(v_page.row_value::text)+1;
    v_rows:=v_rows||jsonb_build_array(v_page.row_value);
    v_count:=v_count+1;
  end loop;
  -- Exact-full row pages may need one final empty request; no count scan.
  return jsonb_build_object('rows',v_rows,'complete',not(v_more or v_count=p_limit),'origin',v_origin,'used_line',v_used_line)||case when p_kind='RATE_DETAILS' then jsonb_build_object('selected_treatment',v_treatment) else '{}'::jsonb end;
end
$function$;
create or replace function public.bpay_next_original_csv_advice_page_v1(
  p_actor_user_id uuid,p_instruction_id uuid,p_kind text,
  p_run_work_id uuid default null,p_run_line_id uuid default null,p_shift_detail_id uuid default null,
  p_after_work_id uuid default null,p_after_member_no bigint default null,
  p_after_detail_no integer default null,p_after_break_no integer default null,
  p_after_bucket text default null,p_after_rate_family text default null,p_after_rate_code text default null,
  p_limit integer default 50
) returns jsonb language plpgsql stable security definer
set search_path=pg_catalog,private,public
set timezone='UTC'
as $function$
declare
  v_instruction private.bpay_next_csv_instruction%rowtype;
  v_transfer private.bpay_next_transfer%rowtype;
  v_projection private.bpay_next_net_projection%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_run private.bpay_next_pay_run%rowtype;
  v_leg private.bpay_next_destination_group_leg%rowtype;
  v_group private.bpay_next_destination_group%rowtype;
  v_net private.bpay_next_net_destination_state%rowtype;
  v_destination private.bpay_next_net_destination_amount%rowtype;
  v_origin private.bpay_next_stored_credit_origin%rowtype;
  v_destination_kind text:='OWN';
  v_approved_line uuid;
  v_revision uuid;
  v_sql text;
  v_row record;
  v_rows jsonb:='[]'::jsonb;
  v_bytes integer:=512;
  v_count integer:=0;
  v_more boolean:=false;
  v_treatment jsonb;
  v_treatment_line private.bpay_next_run_line%rowtype;
  v_treatment_position private.bpay_next_run_position%rowtype;
  v_treatment_worker private.bpay_next_run_worker%rowtype;
  v_treatment_work private.bpay_next_run_work%rowtype;
  v_treatment_run private.bpay_next_pay_run%rowtype;
  v_selected_quote jsonb;
  v_selected_buckets jsonb:='[]'::jsonb;
  v_selected_bucket jsonb;

begin
  if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),
    nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
    or not exists(select 1 from public.tms_users u where u.id=p_actor_user_id
      and u.is_active is true and u.role::text='admin') then
    raise exception using errcode='42501',message='BPAY_NEXT_ADVICE_FORBIDDEN';
  end if;
  if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT') then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  if p_instruction_id is null or p_kind is null
    or p_kind not in ('HEADER','WORKS','MEMBERS','SHIFTS','BREAKS','RATE_DETAILS','RATE_SCHEDULE')
    or p_limit is null or p_limit not between 1 and 100
    or (p_after_member_no is not null and p_after_member_no<0)
    or (p_after_detail_no is not null and p_after_detail_no<0)
    or (p_after_break_no is not null and p_after_break_no<0)
    or (p_after_bucket is not null and p_after_bucket not in ('BH','DAY','NIGHT','SAT','SUN'))
    or ((p_after_rate_family is null)<>(p_after_rate_code is null))
    or (p_after_rate_family is not null and (p_after_rate_family not in ('ADDITIONAL','MILEAGE','STANDARD')
      or char_length(p_after_rate_code) not between 1 and 128)) then
    raise exception using errcode='22023',message='BPAY_NEXT_ADVICE_REQUEST_INVALID';
  end if;
  -- Closed shape: no cursor or child ID is silently ignored by a different kind.
  if (p_run_work_id is not null and p_kind<>'RATE_SCHEDULE')
    or (p_run_line_id is not null and p_kind not in ('SHIFTS','BREAKS','RATE_DETAILS'))
    or (p_shift_detail_id is not null and p_kind<>'BREAKS')
    or (p_after_work_id is not null and p_kind<>'WORKS')
    or (p_after_member_no is not null and p_kind<>'MEMBERS')
    or (p_after_detail_no is not null and p_kind<>'SHIFTS')
    or (p_after_break_no is not null and p_kind<>'BREAKS')
    or (p_after_bucket is not null and p_kind<>'RATE_DETAILS')
    or (p_after_rate_family is not null and p_kind<>'RATE_SCHEDULE')
    or (p_kind='RATE_SCHEDULE' and p_run_work_id is null)
    or (p_kind in ('SHIFTS','BREAKS','RATE_DETAILS') and p_run_line_id is null)
    or (p_kind='BREAKS' and p_shift_detail_id is null) then
    raise exception using errcode='22023',message='BPAY_NEXT_ADVICE_REQUEST_INVALID';
  end if;
  select * into v_instruction from private.bpay_next_csv_instruction where id=p_instruction_id;
  if not found then
    raise exception using errcode='P0002',message='BPAY_NEXT_ADVICE_INSTRUCTION_NOT_FOUND';
  end if;
  select * into strict v_transfer from private.bpay_next_transfer where id=v_instruction.transfer_id;
  if v_transfer.original_transfer_id is not null or v_transfer.projection_id is null then
    raise exception using errcode='55000',message='BPAY_NEXT_ADVICE_ORIGINAL_CSV_REQUIRED';
  end if;
  select * into strict v_projection from private.bpay_next_net_projection where id=v_transfer.projection_id;
  select * into strict v_worker from private.bpay_next_run_worker where id=v_instruction.run_worker_id;
  select * into strict v_run from private.bpay_next_pay_run where id=v_worker.run_id;
  if v_transfer.original_transfer_id is not null or v_transfer.execution_kind<>'BANK'
    or not ((v_worker.target_pay_channel='PAYE' and v_transfer.beneficiary_kind='CANDIDATE')
      or (v_worker.target_pay_channel='UMBRELLA' and v_transfer.beneficiary_kind='UMBRELLA'
        and v_worker.target_umbrella_id is not null and v_projection.input_kind='UMBRELLA'
        and v_worker.case_selection_revision=0))
    or v_transfer.run_worker_id<>v_worker.id or v_transfer.candidate_id<>v_worker.candidate_id
    or v_projection.run_worker_id<>v_worker.id
    or v_transfer.beneficiary_id is distinct from
      (case when v_worker.target_pay_channel='UMBRELLA' then v_worker.target_umbrella_id else v_worker.candidate_id end)
    or v_transfer.return_cash_id is not null
    or v_instruction.cash_amount<>v_transfer.cash_amount
    or v_instruction.bank_details_hash_snapshot is distinct from v_transfer.bank_details_hash_snapshot
    or v_transfer.member_count<1 or v_transfer.member_cash_sum<>v_transfer.cash_amount
    or (v_worker.target_pay_channel='PAYE' and v_projection.input_kind not in ('PAYE_MANUAL','PAYE_IMPORT','CASE_PAYOUT'))
    or (v_worker.target_pay_channel='UMBRELLA' and
      (v_projection.entered_paye_net is not null or v_projection.request_command_id is not null
       or v_projection.cash_amount is distinct from v_projection.gross_inc_vat)) then
    raise exception using errcode='55000',message='BPAY_NEXT_ADVICE_ORIGINAL_CSV_REQUIRED';
  end if;
  -- A frozen leg may be smaller than its accepted Candidate projection. This
  -- is a point read of immutable sealed facts, NOT the execution qualifier:
  -- that qualifier checks today's bank version and must not invalidate advice
  -- for an already-issued original instruction after factual account changes.
  select * into v_leg from private.bpay_next_destination_group_leg where transfer_id=v_transfer.id;
  if not found then
    if v_projection.cash_amount<>v_transfer.cash_amount
      or exists(select 1 from private.bpay_next_destination_group where run_worker_id=v_worker.id) then
      raise exception using errcode='55000',message='BPAY_NEXT_ADVICE_FROZEN_DESTINATION_INVALID';end if;
  else
    select * into strict v_group from private.bpay_next_destination_group where anchor_transfer_id=v_leg.anchor_transfer_id;
    select * into strict v_net from private.bpay_next_net_destination_state where state_id=v_group.net_state_id;
    if v_group.stage<>'COMPLETE' or v_group.completed_at_utc is null
      or v_group.run_worker_id<>v_worker.id or v_group.candidate_id<>v_worker.candidate_id
      or v_group.projection_id<>v_projection.id or v_net.run_worker_id<>v_worker.id
      or v_net.projection_id is distinct from v_projection.id or v_net.completed_at_utc is null
      or v_group.processed_work_count<>v_group.expected_work_count or v_group.processed_case_count<>v_group.expected_case_count
      or v_group.created_leg_count<>v_group.expected_leg_count or v_group.sealed_leg_count<>v_group.expected_leg_count
      or v_group.expected_leg_count<>v_net.external_leg_count+1
      or v_group.member_cash_total<>v_projection.cash_amount or v_group.sealed_cash_total<>v_projection.cash_amount
      or v_net.cash_amount is distinct from v_projection.cash_amount
      or v_net.own_amount+v_net.external_amount is distinct from v_projection.cash_amount then
      raise exception using errcode='55000',message='BPAY_NEXT_ADVICE_FROZEN_DESTINATION_INVALID';end if;
    v_destination_kind:=v_leg.leg_kind;
    if v_leg.leg_kind='OWN' then
      if v_transfer.id<>v_group.anchor_transfer_id or v_leg.destination_id is not null
        or v_transfer.cash_amount is distinct from v_net.own_amount then
        raise exception using errcode='55000',message='BPAY_NEXT_ADVICE_FROZEN_DESTINATION_INVALID';end if;
    else
      select * into strict v_destination from private.bpay_next_net_destination_amount where destination_id=v_leg.destination_id;
      select * into strict v_origin from private.bpay_next_stored_credit_origin where command_id=v_destination.first_origin_command_id;
      if v_destination.state_id<>v_net.state_id or v_destination.amount<>v_transfer.cash_amount
        or v_origin.candidate_id<>v_worker.candidate_id or v_destination.bank_details_hash<>v_origin.bank_details_hash
        or v_origin.original_source_pay_channel<>'UMBRELLA' or v_origin.original_tax_treatment<>'NON_TAXABLE'
        or v_origin.original_routing_kind<>'ONE_OFF_SPECIFIED_BANK_ACCOUNT'
        or (v_transfer.bank_details_hash_snapshot,v_transfer.beneficiary_name_snapshot,
            v_transfer.sort_code_snapshot,v_transfer.account_number_snapshot)
          is distinct from (v_origin.bank_details_hash,pg_catalog.btrim(v_origin.beneficiary_name),
            v_origin.sort_code,v_origin.account_number) then
        raise exception using errcode='55000',message='BPAY_NEXT_ADVICE_FROZEN_DESTINATION_INVALID';end if;
    end if;
  end if;
  -- CSV issuance is the immutable read gate. Current worker/transfer/outcome
  -- status and retirement of this NET projection cannot re-price this issue.
  if p_kind='HEADER' then
    v_rows:=jsonb_build_array(jsonb_build_object(
      'instruction_id',v_instruction.id,'transfer_id',v_transfer.id,'projection_id',v_projection.id,
      'projection_no',v_projection.projection_no::text,'input_kind',v_projection.input_kind,
      'run_id',v_run.id,'run_worker_id',v_worker.id,'candidate_id',v_worker.candidate_id,
      'pay_date',v_run.pay_date,'target_pay_channel',v_worker.target_pay_channel,
      'beneficiary_name_snapshot',v_transfer.beneficiary_name_snapshot,
      'payment_reference',v_instruction.payment_reference,'issued_at_utc',v_instruction.issued_at_utc,
      'member_count',v_transfer.member_count::text,
      'gross_ex_vat',v_projection.gross_ex_vat::text,'gross_vat',v_projection.gross_vat::text,
      'gross_inc_vat',v_projection.gross_inc_vat::text,'entered_paye_net',v_projection.entered_paye_net::text,
      'accepted_gross_additions',v_projection.accepted_gross_additions::text,
      'accepted_gross_deductions',v_projection.accepted_gross_deductions::text,
      'accepted_net_additions',v_projection.accepted_net_additions::text,
      'accepted_recoveries',v_projection.accepted_recoveries::text,'cash_amount',v_instruction.cash_amount::text,
      'projection_cash_amount',v_projection.cash_amount::text,'destination_kind',v_destination_kind));
    if octet_length(v_rows::text)>120000 then
      raise exception using errcode='54000',message='BPAY_NEXT_ADVICE_ROW_TOO_LARGE';
    end if;
    return jsonb_build_object('rows',v_rows,'complete',true);
  end if;
  if p_kind in ('SHIFTS','BREAKS','RATE_DETAILS') then
    select l.approved_line_id into v_approved_line
    from private.bpay_next_transfer_member m join private.bpay_next_run_line l on l.id=m.run_line_id
    where m.transfer_id=v_transfer.id and m.subject_kind='WORK' and l.id=p_run_line_id
      and l.run_worker_id=v_worker.id;
    if not found then
      raise exception using errcode='55000',message='BPAY_NEXT_ADVICE_LINE_NOT_IN_INSTRUCTION';
    end if;
  elsif p_kind='RATE_SCHEDULE' then
    select rw.captured_revision_id into v_revision from private.bpay_next_run_work rw
    where rw.id=p_run_work_id and rw.run_worker_id=v_worker.id and exists(
      select 1 from private.bpay_next_run_line l join private.bpay_next_transfer_member m on m.run_line_id=l.id
      where l.run_work_id=rw.id and m.transfer_id=v_transfer.id and m.subject_kind='WORK');
    if not found then
      raise exception using errcode='55000',message='BPAY_NEXT_ADVICE_WORK_NOT_IN_INSTRUCTION';
    end if;
  end if;

  -- WB29: exact frozen line, never today's decision/target/Position.
  if p_kind='RATE_DETAILS' then
    select l.* into strict v_treatment_line from private.bpay_next_run_line l where l.id=p_run_line_id;
    if v_treatment_line.accepted_resolution_refs is not null
       and jsonb_array_length(v_treatment_line.accepted_resolution_refs)>0 then
      select p.* into v_treatment_position from private.bpay_next_run_position p
        where p.run_work_id=v_treatment_line.run_work_id and p.component_key=v_treatment_line.component_key;
      select w.* into v_treatment_worker from private.bpay_next_run_worker w where w.id=v_treatment_line.run_worker_id;
      select w.* into v_treatment_work from private.bpay_next_run_work w where w.id=v_treatment_line.run_work_id;
      select r.* into v_treatment_run from private.bpay_next_pay_run r where r.id=v_treatment_worker.run_id;
      if v_treatment_position.id is null or v_treatment_worker.id is null or v_treatment_work.id is null or v_treatment_run.id is null
        or (v_treatment_position.run_worker_id,v_treatment_position.work_id,v_treatment_position.captured_revision_id,v_treatment_position.approved_line_id)
          is distinct from (v_treatment_line.run_worker_id,v_treatment_line.work_id,v_treatment_line.captured_revision_id,v_treatment_line.approved_line_id)
        or (v_treatment_work.run_worker_id,v_treatment_work.work_id,v_treatment_work.captured_revision_id,v_treatment_work.candidate_id)
          is distinct from (v_treatment_worker.id,v_treatment_line.work_id,v_treatment_line.captured_revision_id,v_treatment_worker.candidate_id)
        or v_treatment_line.target_pay_channel is distinct from v_treatment_worker.target_pay_channel
        or v_treatment_line.source_pay_channel is distinct from v_treatment_position.source_pay_channel then
        raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_ACCEPTED_BASIS_CHANGED';end if;
      v_selected_quote:=private.bpay_next_captured_component_resolution_quote_v1(v_treatment_position.id,null);
      if v_selected_quote->'accepted_resolution_refs' is distinct from v_treatment_line.accepted_resolution_refs
        or (v_selected_quote->>'included_source_component_ex_vat')::numeric is distinct from v_treatment_line.source_consumed_ex_vat
        or ((v_selected_quote#>>'{component_quote,ex}')::numeric,(v_selected_quote#>>'{component_quote,vat}')::numeric,(v_selected_quote#>>'{component_quote,inc}')::numeric)
          is distinct from (v_treatment_line.frozen_ex_vat,v_treatment_line.frozen_vat,v_treatment_line.frozen_inc_vat)
        or ((v_selected_quote->>'valuation_policy_id')::uuid,(v_selected_quote->>'valuation_window_id')::uuid)
          is distinct from (v_treatment_line.valuation_policy_id,v_treatment_line.policy_window_id)
        or jsonb_typeof(v_selected_quote->'bucket_details') is distinct from 'array'
        or jsonb_array_length(v_selected_quote->'bucket_details') not between 1 and 5
        or (select count(distinct jsonb_build_array(x.value->>'rate_family',x.value->>'rate_code'))
          from jsonb_array_elements(v_selected_quote->'bucket_details') x)<>jsonb_array_length(v_selected_quote->'bucket_details') then
        raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_ACCEPTED_BASIS_CHANGED';end if;
      for v_selected_bucket in select x.value from jsonb_array_elements(v_selected_quote->'bucket_details') x loop
        if ((v_selected_bucket->>'valuation_policy_id')::uuid,(v_selected_bucket->>'valuation_window_id')::uuid)
          is distinct from (v_treatment_line.valuation_policy_id,v_treatment_line.policy_window_id) then
          raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_ACCEPTED_BASIS_CHANGED';end if;
        v_selected_buckets:=v_selected_buckets||jsonb_build_array(jsonb_build_object(
          'rate_family',v_selected_bucket->>'rate_family','rate_code',v_selected_bucket->>'rate_code',
          'decision_id',v_selected_bucket->>'decision_id','decision_version',v_selected_bucket->>'decision_version',
          'treatment',v_selected_bucket->>'treatment','resolution_mode',v_selected_bucket->>'resolution_mode',
          'approved_units',v_selected_bucket->>'approved_units','included_units',v_selected_bucket->>'included_units',
          'source_rate',v_selected_bucket->>'source_rate','source_ex_vat',v_selected_bucket->>'source_ex_vat',
          'manual_replacement_rate',v_selected_bucket->>'manual_replacement_rate','manual_amount_ex_vat',v_selected_bucket->>'manual_amount_ex_vat',
          'quote',jsonb_build_object('ex',v_selected_bucket#>>'{quote,ex}','vat',v_selected_bucket#>>'{quote,vat}',
            'inc',v_selected_bucket#>>'{quote,inc}','rate',v_selected_bucket#>>'{quote,rate}'),
          'valuation_policy_id',v_selected_bucket->>'valuation_policy_id','valuation_window_id',v_selected_bucket->>'valuation_window_id'));
      end loop;
      v_treatment:=jsonb_build_object('run_line_id',v_treatment_line.id,'run_work_id',v_treatment_work.id,
        'run_worker_id',v_treatment_worker.id,'candidate_id',v_treatment_worker.candidate_id,
        'captured_revision_id',v_treatment_line.captured_revision_id,'approved_line_id',v_treatment_line.approved_line_id,
        'component_key',v_treatment_line.component_key,'source_pay_channel',v_treatment_line.source_pay_channel,
        'target_pay_channel',v_treatment_worker.target_pay_channel,'target_umbrella_id',v_treatment_worker.target_umbrella_id,
        'target_umbrella_vat_chargeable',v_treatment_worker.target_umbrella_vat_chargeable,
        'valuation_policy_id',v_treatment_line.valuation_policy_id,'valuation_window_id',v_treatment_line.policy_window_id,
        'valuation_date',v_treatment_run.pay_date,'source_consumed_ex_vat',v_treatment_line.source_consumed_ex_vat::text,
        'component_quote',jsonb_build_object('ex',v_treatment_line.frozen_ex_vat::text,'vat',v_treatment_line.frozen_vat::text,'inc',v_treatment_line.frozen_inc_vat::text),
        'buckets',v_selected_buckets);
    end if;
    -- Metadata is charged once, before the original source row page.
    v_bytes:=v_bytes+coalesce(octet_length(v_treatment::text),4);
    if v_bytes>120000 then raise exception using errcode='54000',message='BPAY_NEXT_ADVICE_ROW_TOO_LARGE';end if;
  end if;
  if p_kind='WORKS' then
    v_sql:=$query$select private.bpay_next_advice_work_v1(rw.id) row_value
      from private.bpay_next_run_work rw where rw.run_worker_id=$2
        and ($6::uuid is null or rw.work_id>$6) and exists(
          select 1 from private.bpay_next_run_line l join private.bpay_next_transfer_member m on m.run_line_id=l.id
          where l.run_work_id=rw.id and m.transfer_id=$1 and m.subject_kind='WORK')
      order by rw.work_id limit $12$query$;
  elsif p_kind='MEMBERS' then
    v_sql:=$query$select jsonb_build_object(
      'member_no',m.member_no::text,'subject_kind',m.subject_kind,'signed_cash_contribution',m.signed_cash_contribution::text,
      'work',case when m.subject_kind='WORK' then private.bpay_next_advice_work_v1(l.run_work_id) end,
      'line',case when m.subject_kind='WORK' then jsonb_build_object(
        'run_line_id',l.id,'run_work_id',l.run_work_id,'captured_revision_id',l.captured_revision_id,
        'line_no',l.line_no::text,'component_key',l.component_key,'component_kind',a.component_kind,
        'work_date',a.work_date,'unit_label',a.unit_label,'approved_quantity',a.approved_quantity::text,
        'approved_unit_rate',a.approved_unit_rate::text,'expected_rate_detail_count',a.expected_rate_detail_count,
        'evidence_ref',a.evidence_ref,'approved_source_ex_vat',a.source_pay_ex_vat::text,
        'source_consumed_ex_vat',l.source_consumed_ex_vat::text,'frozen_ex_vat',l.frozen_ex_vat::text,
        'frozen_vat',l.frozen_vat::text,'frozen_inc_vat',l.frozen_inc_vat::text) end,
      'case',case when m.subject_kind='CASE' then jsonb_build_object(
        'case_instruction_id',ci.id,'allocation_result_id',cr.id,'case_id',ci.case_id,
        'case_kind',ci.case_kind,'case_subtype',ci.case_subtype,'tax_treatment',ci.tax_treatment,
        'instruction_kind',ci.instruction_kind,'direction',ci.direction,'payroll_stage',ci.payroll_stage,
        'resolution_ref',ci.resolution_ref,'explanation',ci.explanation,
        'allocated_target_ex_vat',cr.allocated_target_ex_vat::text,'allocated_target_vat',cr.allocated_target_vat::text,
        'allocated_target_inc_vat',cr.allocated_target_inc_vat::text) end) row_value
      from private.bpay_next_transfer_member m
      left join private.bpay_next_run_line l on l.id=m.run_line_id and l.run_worker_id=m.run_worker_id
      left join private.bpay_next_approved_line a on a.id=l.approved_line_id and a.revision_id=l.captured_revision_id
      left join private.bpay_next_run_case_instruction ci on ci.id=m.case_instruction_id and ci.run_worker_id=m.run_worker_id
      left join private.bpay_next_case_allocation_result cr on cr.id=m.case_allocation_result_id and cr.instruction_id=ci.id
      where m.transfer_id=$1 and m.member_no>coalesce($7::bigint,0)
      order by m.member_no limit $12$query$;
  elsif p_kind='SHIFTS' then
    v_sql:=$query$select to_jsonb(page) row_value from (
      select s.id shift_detail_id,s.detail_no,s.work_date,s.shift_start_at,s.shift_end_at,s.shift_start_local,
        s.shift_end_local,s.shift_overnight,s.submitted_minutes,s.approved_minutes,s.approved_hours::text,s.detail_label,
        s.segment_pay_ex_vat::text,s.pay_excluded,s.hours_day::text,s.hours_night::text,s.hours_sat::text,s.hours_sun::text,s.hours_bh::text
      from private.bpay_next_shift_detail s where s.approved_line_id=$4 and s.detail_no>coalesce($8::integer,0)
      order by s.detail_no limit $12) page$query$;
  elsif p_kind='BREAKS' then
    if not exists(select 1 from private.bpay_next_shift_detail where id=p_shift_detail_id and approved_line_id=v_approved_line) then
      raise exception using errcode='55000',message='BPAY_NEXT_ADVICE_BREAK_NOT_IN_LINE';
    end if;
    v_sql:=$query$select to_jsonb(page) row_value from (
      select b.id break_detail_id,b.break_no,b.break_start_at,b.break_end_at,b.break_start_local,b.break_end_local,b.break_minutes
      from private.bpay_next_break_detail b where b.shift_detail_id=$5 and b.break_no>coalesce($8::integer,0)
      order by b.break_no limit $12) page$query$;
  elsif p_kind='RATE_DETAILS' then
    v_sql:=$query$select to_jsonb(page) row_value from (
      select d.id rate_detail_id,d.bucket,d.approved_hours::text,d.source_pay_rate::text
      from private.bpay_next_rate_detail d where d.approved_line_id=$4 and ($9::text is null or d.bucket>$9)
      order by d.bucket limit $12) page$query$;
  else
    v_sql:=$query$select to_jsonb(page) row_value from (
      select s.id rate_schedule_id,s.rate_family,s.rate_code,s.unit_label,s.paye_rate::text,s.umbrella_rate::text,s.charge_rate::text
      from private.bpay_next_rate_schedule s where s.revision_id=$3
        and ($10::text is null or (s.rate_family,s.rate_code)>($10,$11))
      order by s.rate_family,s.rate_code limit $12) page$query$;
  end if;
  for v_row in execute v_sql using v_transfer.id,v_worker.id,v_revision,v_approved_line,p_shift_detail_id,
    p_after_work_id,p_after_member_no,coalesce(p_after_detail_no,p_after_break_no),p_after_bucket,p_after_rate_family,p_after_rate_code,p_limit
  loop
    if v_bytes+octet_length(v_row.row_value::text)+1>120000 then
      if v_count=0 then raise exception using errcode='54000',message='BPAY_NEXT_ADVICE_ROW_TOO_LARGE';end if;
      v_more:=true;exit;
    end if;
    v_bytes:=v_bytes+octet_length(v_row.row_value::text)+1;
    v_rows:=v_rows||jsonb_build_array(v_row.row_value);v_count:=v_count+1;
  end loop;
  return jsonb_build_object('rows',v_rows,'complete',not(v_more or v_count=p_limit))||case when p_kind='RATE_DETAILS' then jsonb_build_object('selected_treatment',v_treatment) else '{}'::jsonb end;
end
$function$;
alter function public.bpay_next_frozen_detail_page_v1(uuid,text,uuid,uuid,uuid,integer,integer,text,text,text,integer) owner to postgres;
revoke all on function public.bpay_next_frozen_detail_page_v1(uuid,text,uuid,uuid,uuid,integer,integer,text,text,text,integer) from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_frozen_detail_page_v1(uuid,text,uuid,uuid,uuid,integer,integer,text,text,text,integer) to service_role;
alter function public.bpay_next_original_csv_advice_page_v1(uuid,uuid,text,uuid,uuid,uuid,uuid,bigint,integer,integer,text,text,text,integer) owner to postgres;
revoke all on function public.bpay_next_original_csv_advice_page_v1(uuid,uuid,text,uuid,uuid,uuid,uuid,bigint,integer,integer,text,text,text,integer) from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_original_csv_advice_page_v1(uuid,uuid,text,uuid,uuid,uuid,uuid,bigint,integer,integer,text,text,text,integer) to service_role;
notify pgrst,'reload schema';
commit;
