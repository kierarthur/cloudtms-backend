-- F-25 WC06 exact retained wire-key successor; original SQL request alias stays.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_stage_accepted_week_v1(
  p_run_id uuid,p_command_id uuid,p_accepted_input_id uuid,p_candidate_id uuid,p_prelocked_run_ids uuid[]
) returns void language plpgsql security definer set search_path=pg_catalog,private
as $function$
declare
  v_parent private.bpay_next_accepted_input%rowtype;
  v_run private.bpay_next_pay_run%rowtype;
  v_child private.bpay_next_accepted_week_input%rowtype;
  v_old record;v_receipt record;
  v_origin jsonb;v_prefix jsonb;v_outputs jsonb;v_receipts jsonb;v_proof jsonb;
  v_cut bigint;v_count bigint;v_seen bigint:=0;v_request_count bigint;v_latest_no bigint;
  v_prep_command uuid;v_prep_seq bigint;v_draft uuid;v_net uuid;
  v_payment text;v_eligibility text;v_operand numeric;
  v_eligibility_command uuid;v_eligibility_sequence bigint;
  v_original_transfer uuid;v_return uuid;v_reissue uuid;v_previous_return uuid;
  v_bank_cash numeric;v_payroll_net numeric;
begin
  if p_run_id is null or p_command_id is null or p_accepted_input_id is null or p_candidate_id is null
    or exists(select 1 from private.bpay_next_input_transaction t
      where t.preparing_transaction_id=pg_catalog.pg_current_xact_id() and t.phase<>'OPEN') then
    raise exception using errcode='55000',message='BPAY_NEXT_WEEK_CAPTURE_OWNER_INVALID';
  end if;
  select parent.* into strict v_parent from private.bpay_next_acceptance_stage s
    cross join lateral pg_catalog.jsonb_populate_record(null::private.bpay_next_accepted_input,s.record_json) parent
    where s.preparing_transaction_id=pg_catalog.pg_current_xact_id() and s.run_id=p_run_id
      and s.prepare_command_id=p_command_id and s.accepted_input_id=p_accepted_input_id
      and s.record_kind='CANDIDATE' and s.entity_id=p_candidate_id;
  select r.* into strict v_run from private.bpay_next_pay_run r where r.id=p_run_id;
  if v_parent.case_scope<>'SEALED_SELECTION' or v_parent.candidate_id<>p_candidate_id
    or v_parent.accepted_sequence<>0
    or v_run.status<>'PREPARING' or v_run.selection_state<>'SEALED'
    or exists(select 1 from private.bpay_next_run_command rc where rc.run_id=p_run_id) then
    raise exception using errcode='55000',message='BPAY_NEXT_WEEK_CAPTURE_OWNER_INVALID';
  end if;
  -- A plain high-water read bounds already accepted inputs; it allocates no
  -- sequence and holds no clock lock. Candidate control and Source admission
  -- are already held by the true acceptance owner throughout this capture.
  select c.last_sequence into strict v_cut from private.bpay_next_command_clock c where c.id=1;
  if v_cut=9223372036854775807 then
    raise exception using errcode='22003',message='BPAY_NEXT_COMMAND_SEQUENCE_EXHAUSTED';
  end if;
  v_cut:=v_cut+1;
  if exists(select 1 from private.bpay_next_run_worker w
    join private.bpay_next_pay_run r on r.id=w.run_id
    where w.candidate_id=p_candidate_id and w.target_pay_channel='PAYE' and r.id<>p_run_id
      and (r.pay_date,r.created_at_utc)=(v_run.pay_date,v_run.created_at_utc)) then
    raise exception using errcode='55000',message='BPAY_NEXT_WEEK_ORIGINAL_ORDER_UNBOUND';
  end if;
  select count(*) into v_count from private.bpay_next_run_worker w
    join private.bpay_next_pay_run r on r.id=w.run_id
    left join private.bpay_next_run_command rc on rc.run_id=r.id
    left join private.bpay_next_command c on c.id=rc.command_id
    where w.candidate_id=p_candidate_id and w.target_pay_channel='PAYE'
      and r.pay_date>=v_parent.pay_week_start and r.pay_date<v_parent.pay_week_start+7
      and (r.pay_date,r.created_at_utc)<(v_run.pay_date,v_run.created_at_utc)
      and (c.id is null or c.agency_sequence<v_cut);
  if v_count>100 then
    raise exception using errcode='54000',message='BPAY_NEXT_WEEK_ORIGINAL_SCOPE_LIMIT';
  end if;
  for v_old in select w.id,r.id as run_id,rc.command_id
    from private.bpay_next_run_worker w join private.bpay_next_pay_run r on r.id=w.run_id
    left join private.bpay_next_run_command rc on rc.run_id=r.id
    left join private.bpay_next_command c on c.id=rc.command_id
    where w.candidate_id=p_candidate_id and w.target_pay_channel='PAYE'
      and r.pay_date>=v_parent.pay_week_start and r.pay_date<v_parent.pay_week_start+7
      and (r.pay_date,r.created_at_utc)<(v_run.pay_date,v_run.created_at_utc)
      and (c.id is null or c.agency_sequence<v_cut)
    order by r.pay_date,r.created_at_utc,w.id
  loop
    v_seen:=v_seen+1;
    -- Run headers were actually prelocked before Candidate control. Never
    -- take an older header lock here: completion owns header then control.
    -- Revalidated new scope must be contained in that exact prelocked set.
    -- Its financial owners serialize on the Candidate control already held.
    if p_prelocked_run_ids is null or not (v_old.run_id=any(p_prelocked_run_ids)) then
      raise exception using errcode='55000',message='BPAY_NEXT_WEEK_PRELOCK_SCOPE_CHANGED';
    end if;
    perform 1 from private.bpay_next_run_worker w where w.id=v_old.id for key share;
    perform 1 from private.bpay_next_worker_week_contribution x where x.original_run_worker_id=v_old.id for key share;
    perform 1 from private.bpay_next_run_case_instruction i where i.run_worker_id=v_old.id order by i.id for key share;
    perform 1 from private.bpay_next_case_allocation_state s where s.run_worker_id=v_old.id order by s.id for key share;
    perform 1 from private.bpay_next_case_allocation_result z where z.run_worker_id=v_old.id order by z.id for key share;
    perform 1 from private.bpay_next_paye_net_request q where q.run_worker_id=v_old.id order by q.request_no for key share;
    perform 1 from private.bpay_next_net_projection p where p.run_worker_id=v_old.id order by p.projection_no for key share;
    perform 1 from private.bpay_next_transfer t where t.run_worker_id=v_old.id order by t.id for key share;
    perform 1 from private.bpay_next_return_cash x where x.candidate_id=p_candidate_id and exists(
      select 1 from private.bpay_next_transfer t where t.id=x.original_transfer_id and t.run_worker_id=v_old.id)
      order by x.id for key share;
    perform 1 from private.bpay_next_command c where c.id=v_old.command_id or exists(
      select 1 from private.bpay_next_paye_net_request q where q.command_id=c.id and q.run_worker_id=v_old.id)
      or exists(select 1 from private.bpay_next_outcome_request q where q.command_id=c.id and q.run_worker_id=v_old.id)
      or exists(select 1 from private.bpay_next_return_request q where q.command_id=c.id and q.run_worker_id=v_old.id)
      or exists(select 1 from private.bpay_next_cancel_request q where q.command_id=c.id and q.run_worker_id=v_old.id)
      or exists(select 1 from private.bpay_next_preparation_expiry_worker q where q.command_id=c.id and q.run_worker_id=v_old.id)
      order by c.agency_sequence for key share;
    v_origin:=private.bpay_next_week_origin_select_v1(p_candidate_id,v_parent.pay_week_start,v_old.id,v_cut);
    if v_origin is null or v_origin->>'issue_code' is not null then
      raise exception using errcode='55000',message='BPAY_NEXT_WEEK_ECONOMIC_ORIGIN_UNBOUND',
        detail=coalesce(v_origin->>'issue_code','MISSING_ORIGIN');
    end if;
    v_prep_command:=(v_origin#>>'{prepare_origin,command_id}')::uuid;
    v_prep_seq:=(v_origin#>>'{prepare_origin,sequence}')::bigint;
    v_draft:=(v_origin#>>'{prepare_origin,draft_state_id}')::uuid;
    v_net:=(v_origin#>>'{net_origin,net_state_id}')::uuid;
    v_latest_no:=coalesce((v_origin#>>'{net_origin,request_no}')::bigint,0);
    select count(*) into v_request_count from private.bpay_next_paye_net_request q
      join private.bpay_next_command c on c.id=q.command_id where q.run_worker_id=v_old.id and c.agency_sequence<v_cut;
    if v_request_count>100 or v_request_count<>v_latest_no or exists(
      select 1 from private.bpay_next_paye_net_request q join private.bpay_next_command c on c.id=q.command_id
      left join private.bpay_next_command_member m on m.command_id=c.id and m.candidate_id=p_candidate_id
      left join private.bpay_next_job j on j.command_id=c.id and j.candidate_id=p_candidate_id and j.job_kind='PAYE_NET_ENTRY'
      left join private.bpay_next_net_projection p on p.request_command_id=c.id
      where q.run_worker_id=v_old.id and c.agency_sequence<v_cut and (
        q.candidate_id is distinct from p_candidate_id or c.command_kind<>'PAYE_NET_ENTRY'
        or c.status not in ('SEALED','COMPLETE') or c.sealed_at_utc is null or c.expected_member_count<>c.enrolled_member_count
        or c.agency_sequence<=v_prep_seq or m.candidate_id is null
        or j.id is null or (j.status,j.phase) is distinct from ('DONE'::text,'PROJECTED'::text)
        or (j.command_sequence,j.module_epoch) is distinct from (c.agency_sequence,c.module_epoch)
        or p.id is null or (p.run_worker_id,p.projection_no,p.input_kind) is distinct from (v_old.id,q.request_no,q.input_kind)
        or q.request_no<>q.expected_projection_revision+1 or q.request_no<1 or q.request_no>v_latest_no
        or (q.expected_projection_revision>0 and not exists(
          select 1 from private.bpay_next_net_projection prior
          join private.bpay_next_command pc on pc.id=prior.request_command_id
          where prior.run_worker_id=v_old.id and prior.projection_no=q.expected_projection_revision
            and pc.agency_sequence<c.agency_sequence and prior.retired_at_utc=p.accepted_at_utc)))) then
      raise exception using errcode='55000',message='BPAY_NEXT_WEEK_NET_PREFIX_UNBOUND';
    end if;
    select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_array(q.command_id,c.agency_sequence,c.module_epoch,
      j.id,q.request_no,q.expected_projection_revision,p.id,p.retired_at_utc) order by q.request_no),'[]'::jsonb)
      into v_prefix from private.bpay_next_paye_net_request q join private.bpay_next_command c on c.id=q.command_id
      join private.bpay_next_job j on j.command_id=c.id and j.candidate_id=p_candidate_id and j.job_kind='PAYE_NET_ENTRY'
      join private.bpay_next_net_projection p on p.request_command_id=c.id
      where q.run_worker_id=v_old.id and c.agency_sequence<v_cut;
    -- Complete exact identities plus a digest of each immutable instruction,
    -- its DRAFT output and (only for NET_DEDUCT) its NET output. Every retained
    -- output can be reconstructed from these append-only identities. No live
    -- case principal, recovery, held or current finance rule is copied.
    select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_array(i.id,d.id,n.id,
      pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.jsonb_build_array(
        pg_catalog.to_jsonb(i),pg_catalog.to_jsonb(d),pg_catalog.to_jsonb(n))::text,'UTF8')),'hex'))
      order by i.id),'[]'::jsonb) into v_outputs
      from private.bpay_next_run_case_instruction i
      join private.bpay_next_case_allocation_result d on d.instruction_id=i.id and d.state_id=v_draft and d.pass_kind='DRAFT'
      left join private.bpay_next_case_allocation_result n on n.instruction_id=i.id and n.state_id=v_net
        and n.pass_kind='NET' and i.payroll_stage='NET_DEDUCT'
      where i.run_worker_id=v_old.id and i.preparation_revision=(select w.preparation_revision from private.bpay_next_run_worker w where w.id=v_old.id)
        and i.selection_revision=(select w.case_selection_revision from private.bpay_next_run_worker w where w.id=v_old.id);
    if pg_catalog.jsonb_array_length(v_outputs)<>coalesce((v_origin#>>'{prepare_origin,instruction_count}')::bigint,0) then
      raise exception using errcode='55000',message='BPAY_NEXT_WEEK_OUTPUT_SCOPE_UNBOUND';
    end if;
    v_payment:=v_origin->>'initial_payment_state';v_eligibility:=v_origin->>'initial_eligibility_state';
    v_operand:=(v_origin->>'initial_operand')::numeric;
    v_eligibility_command:=(v_origin->>'economic_basis_command_id')::uuid;
    v_eligibility_sequence:=(v_origin->>'economic_basis_sequence')::bigint;
    v_original_transfer:=null;v_return:=null;v_reissue:=null;v_previous_return:=null;v_receipts:='[]'::jsonb;
    for v_receipt in select * from private.bpay_next_week_eligibility_receipts_v1(p_candidate_id,v_old.id,v_cut,
      (v_origin->>'economic_basis_sequence')::bigint,(v_origin->>'accepted_projection_id')::uuid,
      v_prep_command,v_draft,v_net)
    loop
      if pg_catalog.jsonb_array_length(v_receipts)>=100 or v_receipt.issue_code is not null then
        raise exception using errcode='55000',message='BPAY_NEXT_WEEK_ELIGIBILITY_ORIGIN_UNBOUND',
          detail=coalesce(v_receipt.issue_code,'RECEIPT_SCOPE_LIMIT');
      end if;
      perform 1 from private.bpay_next_command c where c.id=v_receipt.command_id for key share;
      v_receipts:=v_receipts||pg_catalog.jsonb_build_array(v_receipt.canonical_receipt);
      if not v_receipt.weekly_applicable then continue;end if;
      if v_receipt.canonical_receipt->>'reissue_original' is not null
        and (v_receipt.canonical_receipt->>'reissue_cash')::uuid is distinct from v_previous_return then
        raise exception using errcode='55000',message='BPAY_NEXT_WEEK_REISSUE_LINEAGE_UNBOUND';
      end if;
      if v_receipt.canonical_receipt->>'lane'='RETURN'
        and v_receipt.canonical_receipt->>'reissue_original' is not null
        and (v_receipt.canonical_receipt->>'transfer_id')::uuid is distinct from v_reissue then
        raise exception using errcode='55000',message='BPAY_NEXT_WEEK_REISSUE_RETURN_LINEAGE_UNBOUND';
      end if;
      v_payment:=v_receipt.payment_after;v_eligibility_command:=v_receipt.command_id;
      v_eligibility_sequence:=v_receipt.agency_sequence;
      if v_payment='CANCELLED' then v_eligibility:='EXCLUDED';v_operand:=0;end if;
      if v_receipt.canonical_receipt->>'original_transfer_id' is not null then
        v_original_transfer:=(v_receipt.canonical_receipt->>'original_transfer_id')::uuid;
      end if;
      if v_receipt.canonical_receipt->>'return_cash_id' is not null then
        v_return:=(v_receipt.canonical_receipt->>'return_cash_id')::uuid;
      end if;
      if v_receipt.canonical_receipt->>'lane'='RETURN' then v_previous_return:=v_return;end if;
      if v_receipt.canonical_receipt->>'reissue_original' is not null then
        v_reissue:=(v_receipt.canonical_receipt->>'transfer_id')::uuid;
      end if;
    end loop;
    v_bank_cash:=(v_origin#>>'{net_origin,cash_amount}')::numeric;
    v_payroll_net:=(v_origin#>>'{net_origin,entered_paye_net}')::numeric;
    if (v_origin#>>'{observed,basis_kind}',v_origin#>>'{observed,payment_state}',v_origin#>>'{observed,eligibility_state}',
        (v_origin#>>'{observed,operand}')::numeric,(v_origin#>>'{observed,projection_id}')::uuid,
        (v_origin#>>'{observed,bank_cash}')::numeric,(v_origin#>>'{observed,payroll_net}')::numeric,
        (v_origin#>>'{observed,original_transfer_id}')::uuid,(v_origin#>>'{observed,returned_cash_id}')::uuid,
        (v_origin#>>'{observed,reissue_transfer_id}')::uuid,v_origin#>>'{observed,return_floor_binding_ref}')
      is distinct from (v_origin->>'basis_kind',v_payment,v_eligibility,v_operand,
        (v_origin->>'accepted_projection_id')::uuid,v_bank_cash,v_payroll_net,v_original_transfer,v_return,v_reissue,null::text) then
      raise exception using errcode='55000',message='BPAY_NEXT_WEEK_CONTRIBUTION_ORIGIN_MISMATCH';
    end if;
    v_proof:=pg_catalog.jsonb_build_object('domain','BPAY_NEXT_ACCEPTED_WEEK_ORIGIN_V1',
      'economic_origin',v_origin-'observed','net_prefix',v_prefix,'instruction_results',v_outputs,'eligibility_receipts',v_receipts);
    if pg_catalog.octet_length(v_proof::text)>245760 then
      raise exception using errcode='54000',message='BPAY_NEXT_WEEK_PROOF_SCOPE_LIMIT';
    end if;
    v_child:=null;v_child.accepted_input_id:=p_accepted_input_id;v_child.candidate_id:=p_candidate_id;
    v_child.pay_week_start:=v_parent.pay_week_start;v_child.contribution_id:=(v_origin->>'contribution_id')::uuid;
    v_child.contribution_revision:=(v_origin->>'contribution_revision')::bigint;
    v_child.original_run_worker_id:=v_old.id;v_child.original_pay_date:=(v_origin->>'original_pay_date')::date;
    v_child.original_created_at_utc:=(v_origin->>'original_created_at_utc')::timestamptz;
    v_child.economic_basis_command_id:=(v_origin->>'economic_basis_command_id')::uuid;
    v_child.economic_basis_sequence:=(v_origin->>'economic_basis_sequence')::bigint;
    v_child.economic_origin_proof:=v_proof;
    v_child.economic_basis_sha256:=pg_catalog.sha256(pg_catalog.convert_to(v_proof::text,'UTF8'));
    v_child.eligibility_command_id:=v_eligibility_command;v_child.eligibility_sequence:=v_eligibility_sequence;
    v_child.accepted_projection_id:=(v_origin->>'accepted_projection_id')::uuid;v_child.basis_kind:=v_origin->>'basis_kind';
    v_child.payment_state:=v_payment;v_child.eligibility_state:=v_eligibility;v_child.eligible_arranged_amount:=v_operand;
    v_child.primary_binding_revision:=(v_origin->>'primary_binding_revision')::bigint;
    v_child.original_transfer_id:=v_original_transfer;v_child.returned_cash_id:=v_return;v_child.reissue_transfer_id:=v_reissue;
    insert into private.bpay_next_acceptance_stage(preparing_transaction_id,run_id,prepare_command_id,
      record_kind,entity_id,accepted_input_id,record_json) values(pg_catalog.pg_current_xact_id(),p_run_id,p_command_id,
      'WEEK',v_child.contribution_id,p_accepted_input_id,pg_catalog.to_jsonb(v_child));
  end loop;
  if v_seen<>v_count then
    raise exception using errcode='55000',message='BPAY_NEXT_WEEK_CAPTURE_COUNT_MISMATCH';
  end if;
end
$function$;

alter function private.bpay_next_stage_accepted_week_v1(uuid,uuid,uuid,uuid,uuid[]) owner to postgres;
revoke all on function private.bpay_next_stage_accepted_week_v1(uuid,uuid,uuid,uuid,uuid[]) from public,anon,authenticated,service_role;
notify pgrst,'reload schema';
commit;
