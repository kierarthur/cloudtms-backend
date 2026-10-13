-- E-only private full-family owner. Callers retain their leases/freeze/financial lane.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_source_comparison_scalar_v2(p private.bpay_next_position)
returns jsonb language sql immutable security invoker set search_path=pg_catalog as $function$
  select jsonb_build_object('source_basis_channel',p.source_basis_channel,
    'realised_source_ex_vat',p.realised_source_ex_vat,'held_source_ex_vat',p.held_source_ex_vat,
    'realised_target_ex_vat',p.realised_target_ex_vat,'realised_target_vat',p.realised_target_vat,
    'realised_target_inc_vat',p.realised_target_inc_vat,'held_target_ex_vat',p.held_target_ex_vat,
    'held_target_vat',p.held_target_vat,'held_target_inc_vat',p.held_target_inc_vat)
$function$;

create or replace function private.bpay_next_source_comparison_scalar_delta_v2(
  p_before jsonb,p_kind text,p_native numeric,p_ex numeric,p_vat numeric,p_inc numeric)
returns jsonb language plpgsql immutable security invoker set search_path=pg_catalog as $function$
declare dr numeric;dh numeric;dt integer;ht integer;
begin
  if p_kind not in ('RESERVE','SETTLE','RELEASE','CASE_RECOVERY') then raise exception 'BPAY_NEXT_COMPARISON_OPERATION_INVALID';end if;
  dr:=case p_kind when 'SETTLE' then p_native when 'CASE_RECOVERY' then -p_native else 0 end;
  dh:=case p_kind when 'RESERVE' then p_native when 'SETTLE' then -p_native when 'RELEASE' then -p_native else 0 end;
  dt:=case p_kind when 'SETTLE' then 1 when 'CASE_RECOVERY' then -1 else 0 end;
  ht:=case p_kind when 'RESERVE' then 1 when 'SETTLE' then -1 when 'RELEASE' then -1 else 0 end;
  return jsonb_build_object('source_basis_channel',p_before->>'source_basis_channel',
    'realised_source_ex_vat',(p_before->>'realised_source_ex_vat')::numeric+dr,
    'held_source_ex_vat',(p_before->>'held_source_ex_vat')::numeric+dh,
    'realised_target_ex_vat',(p_before->>'realised_target_ex_vat')::numeric+dt*p_ex,
    'realised_target_vat',(p_before->>'realised_target_vat')::numeric+dt*p_vat,
    'realised_target_inc_vat',(p_before->>'realised_target_inc_vat')::numeric+dt*p_inc,
    'held_target_ex_vat',(p_before->>'held_target_ex_vat')::numeric+ht*p_ex,
    'held_target_vat',(p_before->>'held_target_vat')::numeric+ht*p_vat,
    'held_target_inc_vat',(p_before->>'held_target_inc_vat')::numeric+ht*p_inc);
end $function$;

-- Stable PLAN read: no locks in this function, no history/credit enumeration.
create or replace function private.bpay_next_source_comparison_case_basis_snapshot_v2(p_canonical_work_id uuid,p_canonical_component_key text)
returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private as $function$
declare result jsonb;
begin
  select jsonb_build_object('collection_id',c.id,'case_id',f.id,'case_component_id',x.id,
    'case_source_channel',x.source_pay_channel,'original_source_pay_channel',c.original_source_pay_channel,
    'case_revision',f.case_revision,'component_revision',x.component_revision,'reconcile_revision',c.reconcile_revision,'case_status',f.status,
    'P',x.approved_source_ex_vat,'K',x.recovered_source_ex_vat,'W',x.written_off_source_ex_vat,'J',x.active_recovery_source_ex_vat,
    'rule_id',f.current_rule_id,'rule_revision',f.current_rule_revision)
  into result from private.bpay_next_work_collection c join private.bpay_next_finance_case f on f.id=c.case_id and f.candidate_id=c.candidate_id
    join private.bpay_next_case_component x on x.id=c.case_component_id and x.case_id=f.id and x.candidate_id=c.candidate_id
  where c.work_id=p_canonical_work_id and c.component_key=p_canonical_component_key
    and x.case_kind='OVERPAYMENT' and x.instruction_kind='RECOVERY' and x.direction='DEDUCTION';
  if not found and exists(select 1 from private.bpay_next_work_collection c where c.work_id=p_canonical_work_id
    and c.component_key=p_canonical_component_key and c.case_id is not null) then raise exception 'BPAY_NEXT_COMPARISON_CASE_OWNER_INVALID';end if;
  return result;
end $function$;

-- Fixed current owner only; original payment channel is origin, Case SOURCE is counter basis.
-- Lock order after caller worker -> WORK -> position: collection -> Case -> component.
create or replace function private.bpay_next_source_comparison_case_basis_v2(
  p_canonical_work_id uuid,p_canonical_component_key text,p_lock boolean default false)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private as $function$
declare c private.bpay_next_work_collection%rowtype;f private.bpay_next_finance_case%rowtype;
  x private.bpay_next_case_component%rowtype;
begin
  if p_lock then
    select * into c from private.bpay_next_work_collection where work_id=p_canonical_work_id and component_key=p_canonical_component_key for update;
  else
    select * into c from private.bpay_next_work_collection where work_id=p_canonical_work_id and component_key=p_canonical_component_key;
  end if;
  if not found or c.case_id is null then return null;end if;
  if p_lock then
    select * into strict f from private.bpay_next_finance_case where id=c.case_id for update;
    select * into strict x from private.bpay_next_case_component where id=c.case_component_id and case_id=f.id for update;
  else
    select * into strict f from private.bpay_next_finance_case where id=c.case_id;
    select * into strict x from private.bpay_next_case_component where id=c.case_component_id and case_id=f.id;
  end if;
  if x.case_kind<>'OVERPAYMENT' or x.instruction_kind<>'RECOVERY' or x.direction<>'DEDUCTION'
    or x.candidate_id<>c.candidate_id or f.candidate_id<>c.candidate_id then
    raise exception 'BPAY_NEXT_COMPARISON_CASE_OWNER_INVALID';end if;
  return private.bpay_next_source_comparison_case_basis_snapshot_v2(p_canonical_work_id,p_canonical_component_key);
end $function$;

-- Saved Case monetary remainder, never a new choice or an inverse unit calculation.
-- A missing/exact-rounding mismatch returns NULL; actual native money still posts REVIEW.
create or replace function private.bpay_next_source_comparison_case_remainder_v2(
  p_input private.bpay_next_source_comparison_receipt,p_kind text,p_case_event_id uuid,
  p_comparable_operation numeric,p_paid_after numeric,p_actual_case jsonb)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private,public as $function$
declare ev private.bpay_next_case_event%rowtype;win private.bpay_next_valuation_policy_window%rowtype;
  expected jsonb;r jsonb;s numeric;j numeric;o numeric;manual_remainder numeric;q jsonb;
begin
  if p_input.case_basis is null then return null;end if;
  if p_kind<>'CASE_RECOVERY' then
    if p_actual_case is distinct from p_input.case_basis or greatest(p_paid_after-p_input.approved_source_ex_vat-
      (p_input.case_resolution->>'target_written_off_ex_vat')::numeric,
      (p_input.case_resolution->>'target_active_capacity_ex_vat')::numeric,0::numeric)
      is distinct from (p_input.case_resolution->>'target_outstanding_ex_vat')::numeric then return null;end if;
    return p_input.case_resolution;
  end if;
  select * into strict ev from private.bpay_next_case_event where id=p_case_event_id;
  if ev.event_kind<>'RECOVERED' or ev.case_id<>p_input.case_id or p_comparable_operation is null then return null;end if;
  expected:=p_input.case_basis||jsonb_build_object('K',(p_input.case_basis->>'K')::numeric+ev.source_amount_ex_vat,
    'J',(p_input.case_basis->>'J')::numeric-ev.source_amount_ex_vat,
    'case_revision',(p_input.case_basis->>'case_revision')::bigint+1,
    'component_revision',(p_input.case_basis->>'component_revision')::bigint+1);
  if expected is distinct from p_actual_case then return null;end if;
  r:=p_input.case_resolution;s:=greatest((r->>'source_outstanding_ex_vat')::numeric-ev.source_amount_ex_vat,0::numeric);
  j:=(r->>'target_active_capacity_ex_vat')::numeric-p_comparable_operation;
  o:=greatest(p_paid_after-p_input.approved_source_ex_vat-(r->>'target_written_off_ex_vat')::numeric,j,0::numeric);
  manual_remainder:=greatest((r->>'target_outstanding_ex_vat')::numeric-p_comparable_operation,0::numeric);
  if j<0 or (p_input.qualification_state<>'PENDING' and (p_actual_case->>'P')::numeric is distinct from
    (p_actual_case->>'K')::numeric+(p_actual_case->>'W')::numeric+greatest(s,(p_actual_case->>'J')::numeric)) then return null;end if;
  select * into strict win from private.bpay_next_valuation_policy_window
    where policy_id=p_input.comparison_policy_id and source_window_id=p_input.policy_window_id
      and p_input.effective_pay_date>=date_from and (date_to is null or p_input.effective_pay_date<=date_to);
  if r->>'selected_mode' not in ('SUGGESTED_EQUIVALENT_BASIS','MANUAL_AMOUNT')
    or r->>'case_source_channel' is distinct from p_actual_case->>'case_source_channel' then return null;end if;
  q:=private.pay_finance_selected_quote_v1((r->>'selected_mode')::public.pay_finance_component_resolution_mode_enum,
    s,r->>'case_source_channel',p_input.target_pay_channel,null,null,
    case when r->>'selected_mode'='MANUAL_AMOUNT' then manual_remainder else null end,
    win.erni_pct,win.vat_rate_pct,p_input.umbrella_vat_chargeable);
  if (q->>'ex')::numeric is distinct from o then return null;end if;
  return r||jsonb_build_object('source_outstanding_ex_vat',s,'target_outstanding_ex_vat',o,
    'source_principal_ex_vat',(p_actual_case->>'K')::numeric+(p_actual_case->>'W')::numeric+greatest(s,(p_actual_case->>'J')::numeric),
    'target_active_capacity_ex_vat',j,'selected_quote',q,
    'remainder_proof',jsonb_build_object('input_comparison_id',p_input.id,'case_event_id',ev.id,
      'source_before',(r->>'source_outstanding_ex_vat')::numeric,'target_before',(r->>'target_outstanding_ex_vat')::numeric,
      'actual_source_recovered_ex_vat',ev.source_amount_ex_vat,'comparable_recovered_ex_vat',p_comparable_operation,
      'source_after',s,'target_after',o));
end $function$;

-- Certify the existing 1519 result under the actual job, including None -> one real Case.
create or replace function private.bpay_next_source_comparison_case_reconcile_proof_v2(
  p_input private.bpay_next_source_comparison_receipt,p_job_id uuid,p_collection_id uuid)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private,public as $function$
declare j private.bpay_next_job%rowtype;c private.bpay_next_work_collection%rowtype;
  ev private.bpay_next_case_event%rowtype;actual jsonb;r jsonb;oldcase jsonb;delta numeric;o numeric;s numeric;
  win private.bpay_next_valuation_policy_window%rowtype;q jsonb;
begin
  select * into strict j from private.bpay_next_job where id=p_job_id;
  select * into strict c from private.bpay_next_work_collection where id=p_collection_id;
  actual:=private.bpay_next_source_comparison_case_basis_v2(p_input.canonical_work_id,p_input.canonical_component_key,true);
  if j.status<>'LEASED' or j.lease_nonce is null or j.lease_until_utc<=clock_timestamp() or j.candidate_id<>p_input.candidate_id
    or not ((j.job_kind='POSITION_APPLY' and j.phase in ('NEW','REMOVED'))
      or (j.job_kind in ('CSV_SETTLEMENT','INTERNAL_SETTLEMENT') and j.phase='MEMBERS')
      or (j.job_kind in ('SIMPLE_CANCEL','PREPARATION_EXPIRY') and j.phase='RELEASE'
        and p_input.receipt_kind='CASE_RELEASE' and p_input.operation_job_id=j.id)
      or (j.job_kind='PREPARE' and j.phase='ALLOCATE' and p_input.receipt_kind='CASE_RESERVE' and p_input.operation_job_id=j.id))
    or c.last_job_id<>j.id or c.updated_at_utc is distinct from transaction_timestamp()
    or (c.candidate_id,c.work_id,c.component_key) is distinct from (p_input.candidate_id,p_input.canonical_work_id,p_input.canonical_component_key)
    or actual is null or actual->>'collection_id' is distinct from c.id::text
    or p_input.comparable_paid_ex_vat is null or p_input.comparable_held_ex_vat is null
    or not (p_input.qualification_state in ('READY','PENDING') or p_input.review_issue_code='CASE_RECONCILIATION_REQUIRED')
    or not exists(select 1 from private.bpay_next_module_control m join private.bpay_next_worker_control w
      on w.candidate_id=j.candidate_id where m.id=1 and m.active_owner='NEXT' and m.owner_epoch=j.module_epoch and w.active_owner_epoch=j.owner_epoch)
    or exists(select 1 from private.bpay_next_job prior where prior.candidate_id=j.candidate_id
      and prior.command_sequence<j.command_sequence and prior.status<>'DONE') then raise exception 'BPAY_NEXT_COMPARISON_CASE_RECONCILE_SCOPE_INVALID';end if;
  oldcase:=p_input.case_basis;
  if oldcase is null then
    if (actual->>'case_source_channel') is distinct from p_input.target_pay_channel
      or ((actual->>'K')::numeric,(actual->>'W')::numeric,(actual->>'J')::numeric)
        is distinct from (0::numeric,0::numeric,0::numeric)
      or ((actual->>'case_revision')::bigint,(actual->>'component_revision')::bigint)
        is distinct from (1::bigint,1::bigint) then raise exception 'BPAY_NEXT_COMPARISON_NEW_CASE_COUNTER_BASIS_INVALID';end if;
    o:=greatest(p_input.comparable_paid_ex_vat-p_input.approved_source_ex_vat,0::numeric);s:=o;
    r:=jsonb_build_object('case_source_channel',p_input.target_pay_channel,'source_outstanding_ex_vat',s,
      'source_principal_ex_vat',s,'target_outstanding_ex_vat',o,'target_written_off_ex_vat',0,
      'target_active_capacity_ex_vat',0,'selected_mode','SUGGESTED_EQUIVALENT_BASIS','target_pay_channel',p_input.target_pay_channel,
      'comparison_policy_id',p_input.comparison_policy_id,'policy_window_id',p_input.policy_window_id,
      'effective_pay_date',p_input.effective_pay_date,'umbrella_vat_chargeable',p_input.umbrella_vat_chargeable);
    delta:=(actual->>'P')::numeric;
  else
    if (actual-array['P','case_revision','component_revision','reconcile_revision','case_status'])
      is distinct from (oldcase-array['P','case_revision','component_revision','reconcile_revision','case_status'])
      or (actual->>'reconcile_revision')::bigint<>(oldcase->>'reconcile_revision')::bigint+1 then
      raise exception 'BPAY_NEXT_COMPARISON_CASE_COUNTER_CHANGE_NOT_RECONCILE';end if;
    r:=p_input.case_resolution;
    o:=greatest(p_input.comparable_paid_ex_vat-p_input.approved_source_ex_vat-(r->>'target_written_off_ex_vat')::numeric,
      (r->>'target_active_capacity_ex_vat')::numeric,0::numeric);
    if actual->>'case_source_channel'=p_input.target_pay_channel then
      s:=o;r:=r||jsonb_build_object('selected_mode','SUGGESTED_EQUIVALENT_BASIS','source_outstanding_ex_vat',s,'target_outstanding_ex_vat',o);
    else
      s:=(r->>'source_outstanding_ex_vat')::numeric;
      if (r->>'target_outstanding_ex_vat')::numeric is distinct from o then raise exception 'BPAY_NEXT_COMPARISON_CASE_SELECTED_REMAINDER_REVIEW_REQUIRED';end if;
    end if;
    delta:=(actual->>'P')::numeric-(oldcase->>'P')::numeric;
    if (actual->>'component_revision')::bigint<>(oldcase->>'component_revision')::bigint+(case when delta<>0 then 1 else 0 end)
      or not ((actual->>'case_revision')::bigint=(oldcase->>'case_revision')::bigint+case when delta<>0 then 1 else 0 end
        or (delta=0 and actual->>'case_status'='CLOSED' and oldcase->>'case_status'<>'CLOSED'
          and (actual->>'case_revision')::bigint=(oldcase->>'case_revision')::bigint+1)) then
      raise exception 'BPAY_NEXT_COMPARISON_CASE_RECONCILE_REVISION_INVALID';end if;
  end if;
  if (actual->>'P')::numeric is distinct from (actual->>'K')::numeric+(actual->>'W')::numeric+
    greatest(s,(actual->>'J')::numeric) then raise exception 'BPAY_NEXT_COMPARISON_CASE_PRINCIPAL_NOT_SELECTED_BASIS';end if;
  if delta<>0 then
    select * into strict ev from private.bpay_next_case_event where operation_id=j.command_id and operation_item_id=c.id
      and event_kind=case when oldcase is null then 'APPROVED' else 'CORRECTED' end;
    if ev.case_id<>c.case_id or ev.approved_delta<>delta or ev.funded_delta<>0 or ev.recovered_delta<>0 or ev.written_off_delta<>0
      or (ev.original_effect_id,ev.original_transfer_id) is distinct from (c.original_effect_id,c.original_transfer_id) then
      raise exception 'BPAY_NEXT_COMPARISON_CASE_RECONCILE_EVENT_INVALID';end if;
  end if;
  select * into strict win from private.bpay_next_valuation_policy_window where policy_id=p_input.comparison_policy_id
    and source_window_id=p_input.policy_window_id and p_input.effective_pay_date>=date_from
      and (date_to is null or p_input.effective_pay_date<=date_to);
  q:=private.pay_finance_selected_quote_v1((r->>'selected_mode')::public.pay_finance_component_resolution_mode_enum,s,
    actual->>'case_source_channel',p_input.target_pay_channel,null,null,
    case when r->>'selected_mode'='MANUAL_AMOUNT' then o else null end,win.erni_pct,win.vat_rate_pct,p_input.umbrella_vat_chargeable);
  if (q->>'ex')::numeric is distinct from o then raise exception 'BPAY_NEXT_COMPARISON_CASE_ROUNDING_REMAINDER_REVIEW_REQUIRED';end if;
  r:=r||jsonb_build_object('source_principal_ex_vat',(actual->>'P')::numeric,'selected_quote',q,
    'rounding_proof',jsonb_build_object('source',s,'quoted_target',q->'ex','required_target',o,'exact',true));
  return jsonb_build_object('case_basis',actual,'case_resolution',r,'case_event_id',ev.id);
end $function$;

-- Known Case capacity changes do NOT change WORK H, realised native, or cash.
create or replace function private.bpay_next_source_comparison_case_capacity_proof_v2(
  p_input private.bpay_next_source_comparison_receipt,p_job_id uuid,p_case_hold_id uuid,p_kind text)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private,public as $function$
declare j private.bpay_next_job%rowtype;h private.bpay_next_case_hold%rowtype;i private.bpay_next_run_case_instruction%rowtype;
  m private.bpay_next_source_comparison_case_hold_mapping%rowtype;actual jsonb;expected jsonb;r jsonb;
  amount numeric;compq numeric;js numeric;o numeric;s numeric;delta integer;win private.bpay_next_valuation_policy_window%rowtype;
  quote jsonb;state text;issue text;qualified boolean:=true;
begin
  select * into strict j from private.bpay_next_job where id=p_job_id;
  select * into strict h from private.bpay_next_case_hold where id=p_case_hold_id;
  select * into strict i from private.bpay_next_run_case_instruction where id=h.instruction_id;
  actual:=private.bpay_next_source_comparison_case_basis_v2(p_input.canonical_work_id,p_input.canonical_component_key,true);
  if p_kind not in ('CASE_RESERVE','CASE_RELEASE') or j.status<>'LEASED' or j.lease_nonce is null or j.lease_until_utc<=clock_timestamp()
    or j.candidate_id<>p_input.candidate_id or p_input.case_basis is null or actual is null
    or (i.work_collection_id,i.case_id,i.case_component_id,i.candidate_id)
      is distinct from (p_input.work_collection_id,p_input.case_id,p_input.case_component_id,p_input.candidate_id)
    or i.instruction_kind<>'RECOVERY' or i.case_kind<>'OVERPAYMENT' or h.source_reserved_ex_vat<=0 or h.target_amount_ex_vat<=0
    or not exists(select 1 from private.bpay_next_case_allocation_result a where a.id=h.allocation_result_id and a.instruction_id=i.id
      and (a.allocated_source_ex_vat,a.allocated_target_ex_vat,a.allocated_target_vat,a.allocated_target_inc_vat)
        =(h.source_reserved_ex_vat,h.target_amount_ex_vat,h.target_amount_vat,h.target_amount_inc_vat))
    or not exists(select 1 from private.bpay_next_module_control mc join private.bpay_next_worker_control wc on wc.candidate_id=j.candidate_id
      where mc.id=1 and mc.active_owner='NEXT' and mc.owner_epoch=j.module_epoch and wc.active_owner_epoch=j.owner_epoch)
    or exists(select 1 from private.bpay_next_job prior where prior.candidate_id=j.candidate_id and prior.command_sequence<j.command_sequence and prior.status<>'DONE') then
    raise exception 'BPAY_NEXT_COMPARISON_CASE_CAPACITY_SCOPE_INVALID';end if;
  if p_kind='CASE_RESERVE' then
    -- FINANCE_ALLOCATE is the real PREPARE job phase; ALLOCATE is only its DRAFT state stage.
    if p_input.qualification_state<>'READY' or j.job_kind<>'PREPARE' or j.phase<>'FINANCE_ALLOCATE' or h.status<>'ACTIVE'
      or h.allocation_pass_kind<>'DRAFT' or h.projection_id is not null or h.finished_at_utc is not null
      or i.payroll_stage<>'GROSS_DEDUCT' or i.hold_purpose<>'GROSS_RECOVERY'
      or i.source_comparison_receipt_id is distinct from p_input.id or i.target_pay_channel<>p_input.target_pay_channel
      or (i.source_binding_version_id,i.canonical_source_work_id,i.canonical_source_component_key)
        is distinct from (p_input.source_binding_version_id,p_input.canonical_work_id,p_input.canonical_component_key)
      or not exists(select 1 from private.bpay_next_run_command rc
        join private.bpay_next_pay_run pr on pr.id=rc.run_id
        join private.bpay_next_run_worker rw on rw.run_id=rc.run_id
        join private.bpay_next_case_allocation_result a on a.id=h.allocation_result_id and a.instruction_id=i.id
        join private.bpay_next_case_allocation_state s on s.id=a.state_id
        join private.bpay_next_case_selection cs on cs.run_id=rw.run_id
          and cs.candidate_id=rw.candidate_id and cs.selection_revision=rw.case_selection_revision
        join private.bpay_next_case_capacity_use u on u.case_hold_id=h.id
        join private.bpay_next_position p on p.work_id=p_input.canonical_work_id
          and p.component_key=p_input.canonical_component_key
        where rc.command_id=j.command_id and pr.status='PREPARING' and pr.selection_state='SEALED'
          and rw.id=h.run_worker_id and rw.id=i.run_worker_id and rw.candidate_id=j.candidate_id
          and rw.run_id=i.run_id and rw.status='PREPARING' and rw.review_issue_code is null and rw.financial_resolution_count=0
          and rw.target_pay_channel=i.target_pay_channel and i.allocation_channel=rw.target_pay_channel
          and (rw.target_pay_channel<>'UMBRELLA' or rw.target_umbrella_vat_chargeable is not distinct from p_input.umbrella_vat_chargeable)
          and (i.preparation_revision,i.selection_revision)=(rw.preparation_revision,rw.case_selection_revision)
          and (s.run_worker_id,s.run_id,s.candidate_id,s.preparation_revision,s.selection_revision,s.job_id,s.command_id,s.module_epoch,s.owner_epoch)
            =(rw.id,rw.run_id,rw.candidate_id,rw.preparation_revision,rw.case_selection_revision,j.id,j.command_id,j.module_epoch,j.owner_epoch)
          and s.pass_kind='DRAFT' and s.projection_no=0 and s.projection_id is null
          and s.status='BUILDING' and s.prepare_stage='ALLOCATE' and s.weekly_binding_state='BOUND'
          and cs.status='SEALED' and s.expected_instruction_count=cs.selected_count
          and s.captured_instruction_count=s.expected_instruction_count and rw.case_instruction_count=s.expected_instruction_count
          and (a.run_worker_id,a.candidate_id,a.preparation_revision,a.selection_revision,a.pass_kind,a.allocation_channel,a.hold_purpose)
            =(rw.id,rw.candidate_id,rw.preparation_revision,rw.case_selection_revision,'DRAFT',i.allocation_channel,i.hold_purpose)
          and u.status='ACTIVE' and u.realisation_event_id is null
          and (u.case_component_id,u.case_id,u.candidate_id,u.pay_week_start,u.purpose,u.source_amount_ex_vat)
            =(h.case_component_id,h.case_id,h.candidate_id,h.pay_week_start,h.purpose,h.source_reserved_ex_vat)
          and p.source_book_state='READY' and p.source_book_revision=p_input.source_book_revision
          and p.source_comparison_receipt_id=p_input.id and p.source_comparison_revision=p_input.comparison_revision) then
      raise exception 'BPAY_NEXT_COMPARISON_CASE_RESERVE_NOT_ACCEPTED';end if;
    compq:=h.target_amount_ex_vat;delta:=1;
  else
    if h.status<>'RELEASED' or h.finished_at_utc is null or h.finished_at_utc>clock_timestamp() or j.phase<>'RELEASE'
      or not exists(select 1 from private.bpay_next_case_capacity_use u where u.case_hold_id=h.id and u.status='RELEASED' and u.realisation_event_id is null)
      or not ((j.job_kind='PREPARATION_EXPIRY' and private.bpay_next_source_comparison_expired_case_scope_v2(j.id,i.work_collection_id,h.id))
        or (j.job_kind='SIMPLE_CANCEL' and exists(select 1 from private.bpay_next_case_cancel_binding cb
          join private.bpay_next_cancel_request cr on cr.command_id=cb.command_id and cr.status='CANCELLING'
          join private.bpay_next_run_worker rw on rw.id=cb.run_worker_id and rw.candidate_id=j.candidate_id and rw.status='CANCELLING'
          where cb.command_id=j.command_id and cb.candidate_id=j.candidate_id and cb.run_worker_id=h.run_worker_id
            and cb.status='CANCELLING' and cb.stage='CASE' and h.finished_at_utc>=cr.started_at_utc
            and (cb.case_hold_cursor is null or h.id>cb.case_hold_cursor)))) then raise exception 'BPAY_NEXT_COMPARISON_CASE_RELEASE_NOT_ACTUAL';end if;
    select * into m from private.bpay_next_source_comparison_case_hold_mapping where basis_review_id=p_input.basis_review_id and case_hold_id=h.id;
    if found then
      qualified:=m.instruction_id=i.id and m.allocation_result_id=h.allocation_result_id
        and m.native_target_channel=i.target_pay_channel and m.native_target_ex_vat=h.target_amount_ex_vat;
      compq:=m.comparable_recovery_ex_vat;
    elsif i.source_comparison_receipt_id is not null and i.target_pay_channel=p_input.target_pay_channel then compq:=h.target_amount_ex_vat;
    else qualified:=false;end if;
    delta:=-1;
  end if;
  expected:=p_input.case_basis||jsonb_build_object('J',(p_input.case_basis->>'J')::numeric+delta*h.source_reserved_ex_vat,
    'case_revision',(p_input.case_basis->>'case_revision')::bigint+1,'component_revision',(p_input.case_basis->>'component_revision')::bigint+1);
  if expected is distinct from actual then raise exception 'BPAY_NEXT_COMPARISON_CASE_CAPACITY_NATIVE_TUPLE_INVALID';end if;
  r:=p_input.case_resolution;
  if qualified and p_input.comparable_paid_ex_vat is not null and r->>'target_active_capacity_ex_vat' is not null then
    js:=(r->>'target_active_capacity_ex_vat')::numeric+delta*compq;
    o:=greatest(p_input.comparable_paid_ex_vat-p_input.approved_source_ex_vat-(r->>'target_written_off_ex_vat')::numeric,js,0::numeric);
    s:=(r->>'source_outstanding_ex_vat')::numeric;
    select * into strict win from private.bpay_next_valuation_policy_window where policy_id=p_input.comparison_policy_id
      and source_window_id=p_input.policy_window_id and p_input.effective_pay_date>=date_from and (date_to is null or p_input.effective_pay_date<=date_to);
    quote:=private.pay_finance_selected_quote_v1((r->>'selected_mode')::public.pay_finance_component_resolution_mode_enum,s,
      r->>'case_source_channel',p_input.target_pay_channel,null,null,
      case when r->>'selected_mode'='MANUAL_AMOUNT' then (r->>'target_outstanding_ex_vat')::numeric else null end,
      win.erni_pct,win.vat_rate_pct,p_input.umbrella_vat_chargeable);
    if (quote->>'ex')::numeric is distinct from o and p_kind='CASE_RELEASE' then
      s:=greatest(s-h.source_reserved_ex_vat,0::numeric);
      quote:=private.pay_finance_selected_quote_v1((r->>'selected_mode')::public.pay_finance_component_resolution_mode_enum,s,
        r->>'case_source_channel',p_input.target_pay_channel,null,null,
        case when r->>'selected_mode'='MANUAL_AMOUNT' then greatest((r->>'target_outstanding_ex_vat')::numeric-compq,0::numeric) else null end,
        win.erni_pct,win.vat_rate_pct,p_input.umbrella_vat_chargeable);
    end if;
    qualified:=js>=0 and (quote->>'ex')::numeric is not distinct from o;
    if qualified then r:=r||jsonb_build_object('source_outstanding_ex_vat',s,'target_outstanding_ex_vat',o,
      'target_active_capacity_ex_vat',js,'source_principal_ex_vat',(actual->>'K')::numeric+(actual->>'W')::numeric+greatest(s,(actual->>'J')::numeric),
      'selected_quote',quote,'capacity_remainder_proof',jsonb_build_object('case_hold_id',h.id,'operation_kind',p_kind,
        'actual_source_capacity_delta',delta*h.source_reserved_ex_vat,'comparable_capacity_delta',delta*compq));end if;
  else qualified:=false;end if;
  if not qualified and p_kind='CASE_RESERVE' then raise exception 'BPAY_NEXT_COMPARISON_CASE_CAPACITY_RESERVE_REVIEW_REQUIRED';end if;
  if not qualified then
    state:='REVIEW';issue:='CASE_CAPACITY_COMPARISON_REVIEW_REQUIRED';
    r:=r||jsonb_build_object('qualification_issue',issue,'actual_case_hold_id',h.id);
  elsif p_input.qualification_state='PENDING' then state:='PENDING';issue:='SOURCE_PUBLICATION_PENDING';
  elsif (r->>'source_principal_ex_vat')::numeric is distinct from (actual->>'P')::numeric then state:='REVIEW';issue:='CASE_RECONCILIATION_REQUIRED';
  else state:='READY';end if;
  return jsonb_build_object('case_basis',actual,'case_resolution',r,'qualification_state',state,'review_issue_code',issue,
    'comparable_operation_ex_vat',compq,'instruction_id',i.id,'allocation_result_id',h.allocation_result_id,'native_source_channel',i.target_pay_channel);
end $function$;

-- Fixed point operation proof. No financial history enumeration or caller delta.
create or replace function private.bpay_next_source_comparison_operation_v2(p_job_id uuid,p_kind text,p_operation_id uuid)
returns table(candidate_id uuid,canonical_work_id uuid,canonical_component_key text,local_work_id uuid,local_component_key text,
  native_channel text,native_amount private.bpay_next_penny_amount,target_ex private.bpay_next_penny_amount,
  target_vat private.bpay_next_penny_amount,target_inc private.bpay_next_penny_amount,hold_id uuid,effect_id uuid,
  case_event_id uuid,case_instruction_id uuid,case_result_id uuid,updates_local_scalar boolean)
language plpgsql security invoker set search_path=pg_catalog,private,public as $function$
declare j private.bpay_next_job%rowtype;l private.bpay_next_run_line%rowtype;h private.bpay_next_hold%rowtype;
  e private.bpay_next_financial_effect%rowtype;ev private.bpay_next_case_event%rowtype;
  i private.bpay_next_run_case_instruction%rowtype;a private.bpay_next_case_allocation_result%rowtype;
  b private.bpay_next_source_component_binding%rowtype;
  cr private.bpay_next_source_comparison_receipt%rowtype;cm private.bpay_next_source_comparison_case_hold_mapping%rowtype;
  collection private.bpay_next_work_collection%rowtype;
  captured private.bpay_next_run_position%rowtype;scalar_eligible boolean;
begin
  select * into strict j from private.bpay_next_job where id=p_job_id;
  if p_kind not in ('RESERVE','SETTLE','RELEASE','CASE_RECOVERY') or p_operation_id is null
    or j.status<>'LEASED' or j.lease_nonce is null or j.lease_until_utc<=clock_timestamp()
    or not exists(select 1 from private.bpay_next_module_control m join private.bpay_next_worker_control c
      on c.candidate_id=j.candidate_id where m.id=1 and m.active_owner='NEXT'
        and m.owner_epoch=j.module_epoch and c.active_owner_epoch=j.owner_epoch)
    or exists(select 1 from private.bpay_next_job prior where prior.candidate_id=j.candidate_id
      and prior.command_sequence<j.command_sequence and prior.status<>'DONE') then
    raise exception using errcode='55000',message='BPAY_NEXT_COMPARISON_OPERATION_OWNER_STALE';end if;
  if p_kind='CASE_RECOVERY' then
    select * into strict e from private.bpay_next_financial_effect where id=p_operation_id;
    select * into strict ev from private.bpay_next_case_event where id=e.source_case_event_id;
    select * into strict i from private.bpay_next_run_case_instruction where id=e.source_case_instruction_id;
    select * into strict a from private.bpay_next_case_allocation_result where id=e.source_case_result_id;
    if j.job_kind not in ('CSV_SETTLEMENT','INTERNAL_SETTLEMENT') or j.phase<>'MEMBERS'
      or e.source_run_line_id is not null or e.effect_kind<>'WORK_COLLECTION_RECOVERED'
      or e.operation_id<>j.command_id or e.operation_item_id<>ev.id or e.candidate_id<>j.candidate_id
      or (ev.instruction_id,ev.allocation_result_id,ev.case_id,ev.candidate_id,ev.operation_id)
        is distinct from (i.id,a.id,e.case_id,j.candidate_id,j.command_id)
      or ev.event_kind<>'RECOVERED' or a.instruction_id<>i.id or a.pass_kind not in ('DRAFT','NET')
      or i.instruction_kind<>'RECOVERY' or i.case_kind<>'OVERPAYMENT' or i.direction<>'DEDUCTION'
      or e.native_source_channel is distinct from i.target_pay_channel
      or (ev.source_amount_ex_vat,ev.target_amount_ex_vat,ev.target_amount_vat,ev.target_amount_inc_vat)
        is distinct from (a.allocated_source_ex_vat,a.allocated_target_ex_vat,a.allocated_target_vat,a.allocated_target_inc_vat)
      or (e.source_disposition_ex_vat,e.target_amount_ex_vat,e.target_amount_vat,e.target_amount_inc_vat)
        is distinct from (-ev.target_amount_ex_vat,-ev.target_amount_ex_vat,-ev.target_amount_vat,-ev.target_amount_inc_vat)
      or ev.target_amount_ex_vat<=0
      or not exists(select 1 from private.bpay_next_case_hold ch join private.bpay_next_case_capacity_use u
        on u.case_hold_id=ch.id and u.status='REALISED' and u.realisation_event_id=ev.id
        join private.bpay_next_outcome_request r on r.command_id=j.command_id and r.transfer_id=ev.original_transfer_id
        join private.bpay_next_transfer_member m on m.transfer_id=r.transfer_id and m.case_hold_id=ch.id
          and m.case_instruction_id=i.id and m.case_allocation_result_id=a.id
        where ch.id=ev.operation_item_id and ch.status='REALISED' and ch.instruction_id=i.id
          and ch.source_reserved_ex_vat=ev.source_amount_ex_vat and ch.run_worker_id=r.run_worker_id)
      then raise exception using errcode='23514',message='BPAY_NEXT_COMPARISON_CASE_RECOVERY_NOT_ACTUAL';end if;
    if e.source_comparison_case_hold_id is not null then
      select * into strict cr from private.bpay_next_source_comparison_receipt where id=e.source_comparison_receipt_id;
      select * into strict cm from private.bpay_next_source_comparison_case_hold_mapping
        where basis_review_id=e.source_comparison_basis_review_id and case_hold_id=e.source_comparison_case_hold_id;
      select * into strict collection from private.bpay_next_work_collection where id=i.work_collection_id;
      if i.source_comparison_receipt_id is not null or cr.basis_review_id<>cm.basis_review_id
        or (cm.case_hold_id,cm.instruction_id,cm.allocation_result_id,cm.native_target_channel,cm.native_target_ex_vat)
          is distinct from (ev.operation_item_id,i.id,a.id,i.target_pay_channel,ev.target_amount_ex_vat)
        or (e.source_binding_version_id,e.canonical_source_work_id,e.canonical_source_component_key,e.work_id,e.component_key)
          is distinct from (cr.source_binding_version_id,cr.canonical_work_id,cr.canonical_component_key,collection.work_id,collection.component_key)
        or (collection.case_id,collection.case_component_id,collection.candidate_id,collection.work_id,collection.component_key)
          is distinct from (ev.case_id,ev.case_component_id,j.candidate_id,cr.canonical_work_id,cr.canonical_component_key) then
        raise exception 'BPAY_NEXT_COMPARISON_RETAINED_CASE_HOLD_NOT_CERTIFIED';end if;
      canonical_work_id:=cr.canonical_work_id;canonical_component_key:=cr.canonical_component_key;updates_local_scalar:=false;
    else
      if i.source_comparison_receipt_id is null or (e.source_comparison_receipt_id,e.source_binding_version_id,
          e.canonical_source_work_id,e.canonical_source_component_key,e.work_id,e.component_key)
        is distinct from (i.source_comparison_receipt_id,i.source_binding_version_id,i.canonical_source_work_id,
          i.canonical_source_component_key,i.source_approval_work_id,i.source_approval_component_key) then
        raise exception 'BPAY_NEXT_COMPARISON_CASE_INSTRUCTION_INPUT_INVALID';end if;
      canonical_work_id:=i.canonical_source_work_id;canonical_component_key:=i.canonical_source_component_key;
      updates_local_scalar:=i.source_approval_work_id<>i.canonical_source_work_id;
    end if;
    local_work_id:=e.work_id;local_component_key:=e.component_key;native_channel:=i.target_pay_channel;
    native_amount:=ev.target_amount_ex_vat;target_ex:=ev.target_amount_ex_vat;target_vat:=ev.target_amount_vat;target_inc:=ev.target_amount_inc_vat;
    effect_id:=e.id;case_event_id:=ev.id;case_instruction_id:=i.id;case_result_id:=a.id;
  else
    if p_kind='SETTLE' then
      select * into strict e from private.bpay_next_financial_effect where id=p_operation_id;
      select * into strict h from private.bpay_next_hold where id=e.operation_item_id;
    else select * into strict h from private.bpay_next_hold where id=p_operation_id;end if;
    select * into strict l from private.bpay_next_run_line where id=h.run_line_id;
    if (h.work_id,h.component_key,h.source_reserved_ex_vat,h.target_amount_ex_vat,h.target_amount_vat,h.target_amount_inc_vat)
      is distinct from (l.work_id,l.component_key,l.source_consumed_ex_vat,l.frozen_ex_vat,l.frozen_vat,l.frozen_inc_vat)
      or not exists(select 1 from private.bpay_next_run_worker candidate_worker
        where candidate_worker.id=l.run_worker_id and candidate_worker.candidate_id=j.candidate_id)
      or h.source_reserved_ex_vat<=0 then
      raise exception using errcode='23514',message='BPAY_NEXT_COMPARISON_WORK_HOLD_NOT_EXACT';end if;
    if p_kind='RESERVE' then
      if j.job_kind<>'PREPARE' or j.phase<>'ALLOCATE' or h.status<>'ACTIVE'
        or not exists(select 1 from private.bpay_next_run_command rc join private.bpay_next_run_worker w on w.run_id=rc.run_id
          where rc.command_id=j.command_id and w.id=l.run_worker_id and w.candidate_id=j.candidate_id and w.status='PREPARING') then
        raise exception using errcode='23514',message='BPAY_NEXT_COMPARISON_RESERVE_SCOPE_INVALID';end if;
    elsif p_kind='SETTLE' then
      if j.job_kind not in ('CSV_SETTLEMENT','INTERNAL_SETTLEMENT') or j.phase<>'MEMBERS' or h.status<>'REALISED'
        or e.effect_kind<>'PAYROLL_SETTLED' or e.operation_id<>j.command_id or e.candidate_id<>j.candidate_id
        or (e.work_id,e.component_key,e.source_disposition_ex_vat,e.target_amount_ex_vat,e.target_amount_vat,e.target_amount_inc_vat)
          is distinct from (l.work_id,l.component_key,h.source_reserved_ex_vat,l.frozen_ex_vat,l.frozen_vat,l.frozen_inc_vat)
        or not exists(select 1 from private.bpay_next_outcome_request r join private.bpay_next_transfer_member m
          on m.transfer_id=r.transfer_id and m.run_line_id=l.id and m.subject_kind='WORK'
          where r.command_id=j.command_id and r.transfer_id=e.original_transfer_id and r.run_worker_id=l.run_worker_id
            and m.signed_cash_contribution=l.frozen_inc_vat) then
        raise exception using errcode='23514',message='BPAY_NEXT_COMPARISON_SETTLEMENT_NOT_ACTUAL';end if;
      effect_id:=e.id;
    else
      if h.status<>'RELEASED' or h.finished_at_utc is null or j.phase<>'RELEASE'
        or not ((j.job_kind='SIMPLE_CANCEL' and exists(select 1 from private.bpay_next_cancel_request r
          where r.command_id=j.command_id and r.run_worker_id=l.run_worker_id and r.status='CANCELLING'))
          or (j.job_kind='PREPARATION_EXPIRY' and exists(select 1 from private.bpay_next_preparation_expiry_worker x
            join private.bpay_next_run_worker rw on rw.id=x.run_worker_id and rw.candidate_id=j.candidate_id and rw.status='CANCELLING'
            join private.bpay_next_pay_run pr on pr.id=x.run_id and pr.id=rw.run_id and pr.status='CANCELLING' and pr.confirmed_at_utc is null
            where x.job_id=j.id and x.command_id=j.command_id and x.run_worker_id=l.run_worker_id and x.status='RELEASING' and x.stage='WORK'))) then
        raise exception using errcode='23514',message='BPAY_NEXT_COMPARISON_RELEASE_SCOPE_INVALID';end if;
    end if;
    if l.source_binding_version_id is not null then
      canonical_work_id:=l.canonical_source_work_id;canonical_component_key:=l.canonical_source_component_key;
    else
      -- Retained actual old child: direct canonical identity, no latest detail or historical chain.
      select * into strict b from private.bpay_next_source_component_binding
        where candidate_id=j.candidate_id and canonical_work_id=l.work_id and canonical_component_key=l.component_key;
      canonical_work_id:=b.canonical_work_id;canonical_component_key:=b.canonical_component_key;
    end if;
    local_work_id:=l.work_id;local_component_key:=l.component_key;native_channel:=l.source_pay_channel;
    native_amount:=h.source_reserved_ex_vat;target_ex:=l.frozen_ex_vat;target_vat:=l.frozen_vat;target_inc:=l.frozen_inc_vat;hold_id:=h.id;
    if l.source_comparison_receipt_id is not null then
      select rp.* into strict captured from private.bpay_next_run_position rp
        where rp.run_work_id=l.run_work_id and rp.component_key=l.component_key;
      if (captured.run_worker_id,captured.work_id,captured.captured_revision_id,
          captured.approved_line_id,captured.source_pay_channel,
          captured.source_binding_version_id,captured.canonical_source_work_id,
          captured.canonical_source_component_key,captured.source_book_revision,
          captured.source_comparison_receipt_id,captured.residual_source_ex_vat,
          captured.selected_residual_ex_vat)
        is distinct from (l.run_worker_id,l.work_id,l.captured_revision_id,
          l.approved_line_id,l.source_pay_channel,
          l.source_binding_version_id,l.canonical_source_work_id,
          l.canonical_source_component_key,l.source_book_revision,
          l.source_comparison_receipt_id,l.source_consumed_ex_vat,l.source_consumed_ex_vat)
        or captured.needs_financial_resolution is distinct from false or captured.work_collection_id is not null then
        raise exception using errcode='23514',message='BPAY_NEXT_COMPARISON_LOCAL_SCALAR_CAPTURE_INVALID';
      end if;
      scalar_eligible:=captured.source_basis_channel is not distinct from l.source_pay_channel;
      if l.updates_original_source_scalar is distinct from
        (((local_work_id,local_component_key)=(canonical_work_id,canonical_component_key)) and scalar_eligible) then
        raise exception using errcode='23514',message='BPAY_NEXT_COMPARISON_LOCAL_SCALAR_FLAG_INVALID';
      end if;
      updates_local_scalar:=case when (local_work_id,local_component_key)=(canonical_work_id,canonical_component_key)
        then l.updates_original_source_scalar else scalar_eligible end;
    else
      -- Retained pre-comparison children keep their original scalar posting.
      updates_local_scalar:=true;
    end if;
  end if;
  candidate_id:=j.candidate_id;return next;
end $function$;

-- Frozen comparison line eligibility cannot be changed after allocation.
-- Ordinary and retained pre-comparison row lifecycles remain unchanged.
create or replace function private.bpay_next_source_comparison_run_line_frozen_guard_v2()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private as $function$
begin
  if tg_op='TRUNCATE' then
    raise exception using errcode='23514',message='BPAY_NEXT_COMPARISON_RUN_LINE_IMMUTABLE';
  end if;
  if old.source_comparison_receipt_id is not null
    or (tg_op='UPDATE' and new.source_comparison_receipt_id is not null) then
    raise exception using errcode='23514',message='BPAY_NEXT_COMPARISON_RUN_LINE_IMMUTABLE';
  end if;
  if tg_op='DELETE' then return old;end if;
  return new;
end $function$;

drop trigger if exists bpay_next_source_comparison_run_line_frozen_v2 on private.bpay_next_run_line;
create trigger bpay_next_source_comparison_run_line_frozen_v2 before update or delete
on private.bpay_next_run_line for each row execute function private.bpay_next_source_comparison_run_line_frozen_guard_v2();
drop trigger if exists bpay_next_source_comparison_run_line_no_truncate_v2 on private.bpay_next_run_line;
create trigger bpay_next_source_comparison_run_line_no_truncate_v2 before truncate
on private.bpay_next_run_line for each statement execute function private.bpay_next_source_comparison_run_line_frozen_guard_v2();

create or replace function private.bpay_next_source_comparison_book_certified_v2(p private.bpay_next_position)
returns boolean language sql stable security invoker set search_path=pg_catalog,private as $function$
  select exists(select 1 from private.bpay_next_source_comparison_receipt r where r.id=p.source_comparison_receipt_id
    and (r.canonical_work_id,r.canonical_component_key,r.comparison_revision,r.source_book_revision,
      r.native_realised_paye,r.native_realised_umbrella,r.native_held_paye,r.native_held_umbrella)
      =(p.work_id,p.component_key,p.source_comparison_revision,p.source_book_revision,
        p.native_realised_paye,p.native_realised_umbrella,p.native_held_paye,p.native_held_umbrella)
    and r.snapshot_sha256=sha256(convert_to((to_jsonb(r)-'snapshot_sha256')::text,'UTF8')))
$function$;

-- Finite immutable certificate guard. Direct inserts must prove the same actual operation.
create or replace function private.bpay_next_source_comparison_guard_v2()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private,public as $function$
declare p private.bpay_next_position%rowtype;oldr private.bpay_next_source_comparison_receipt%rowtype;
  w private.weekly_source_destination_reprice_witness_v1%rowtype;v private.bpay_next_source_component_binding_version%rowtype;
  op record;m private.bpay_next_source_comparison_hold_mapping%rowtype;q numeric;plan jsonb;ctx jsonb;item jsonb;counted integer;
  dr numeric;dh numeric;qualified boolean;localp private.bpay_next_position%rowtype;
  basis private.bpay_next_source_comparison_receipt%rowtype;promoting boolean:=false;request jsonb;validation jsonb;
  cm private.bpay_next_source_comparison_case_hold_mapping%rowtype;
  actual_case jsonb;credit_qualified boolean;case_qualified boolean;proof jsonb;expected_state text;
begin
  if tg_op<>'INSERT' then raise exception using errcode='55000',message='BPAY_NEXT_SOURCE_COMPARISON_IMMUTABLE';end if;
  select * into strict p from private.bpay_next_position where work_id=new.canonical_work_id and component_key=new.canonical_component_key;
  select * into strict v from private.bpay_next_source_component_binding_version where id=new.source_binding_version_id;
  if p.source_book_state<>'READY' or new.accepted_at_utc is distinct from transaction_timestamp()
    or new.snapshot_sha256 is distinct from sha256(convert_to((to_jsonb(new)-'snapshot_sha256')::text,'UTF8'))
    or (new.source_book_revision_before,new.comparison_revision_before,new.previous_comparison_id,
      new.native_realised_paye_before,new.native_realised_umbrella_before,new.native_held_paye_before,new.native_held_umbrella_before)
      is distinct from (p.source_book_revision,p.source_comparison_revision,p.source_comparison_receipt_id,
        p.native_realised_paye,p.native_realised_umbrella,p.native_held_paye,p.native_held_umbrella)
    or (new.candidate_id,new.approval_work_id,new.approval_revision_id,new.approval_component_key,new.approved_line_id,
      new.approval_component_state,new.binding_digest,new.approval_component_sha256,new.approval_chosen_detail_sha256)
      is distinct from (v.candidate_id,v.approval_work_id,v.approval_revision_id,v.approval_component_key,v.approved_line_id,
        v.component_state,v.binding_digest,v.component_sha256,v.chosen_detail_sha256) then
    raise exception using errcode='23514',message='BPAY_NEXT_COMPARISON_SNAPSHOT_CURRENTNESS_INVALID';end if;
  if p.source_comparison_receipt_id is not null then
    select * into strict oldr from private.bpay_next_source_comparison_receipt where id=p.source_comparison_receipt_id;
  end if;
  if (new.comparable_paid_ex_vat_before,new.comparable_held_ex_vat_before)
    is distinct from (oldr.comparable_paid_ex_vat,oldr.comparable_held_ex_vat) then
    raise exception using errcode='23514',message='BPAY_NEXT_COMPARISON_BEFORE_QUOTE_INVALID';end if;
  if new.receipt_kind in ('REVIEW','PENDING_REVIEW') then
    if not exists(select 1 from private.bpay_next_source_component_binding b where b.current_binding_version_id=v.id
      and b.canonical_work_id=v.canonical_work_id and b.canonical_component_key=v.canonical_component_key) then
      raise exception 'BPAY_NEXT_COMPARISON_REVIEW_BINDING_NOT_CURRENT';end if;
    select * into strict w from private.weekly_source_destination_reprice_witness_v1 where id=new.review_certificate_id;
    select count(*),jsonb_agg(c) into counted,item from jsonb_array_elements(w.context_json#>'{financial_plan,components}') c
      where c->>'source_component_id'=v.source_component_id::text;
    if counted<>1 then raise exception 'BPAY_NEXT_COMPARISON_REVIEW_PLAN_MISSING';end if;
    ctx:=item->0->'context';plan:=item->0->'plan';
    actual_case:=private.bpay_next_source_comparison_case_basis_v2(new.canonical_work_id,new.canonical_component_key,false);
    request:=w.accepted_request_json#-'{financial_request,contract_choices,1,destination_reprice}';
    request:=jsonb_set(request,'{control,whole_root_office_review}',w.context_json#>'{base_request,control,whole_root_office_review}');
    validation:=private.bpay_next_source_comparison_validate_plan_snapshot_v2(request,w.context_json->'financial_context',w.context_json->'financial_plan');
    if new.receipt_kind='REVIEW' and oldr.qualification_state='PENDING' then
      select * into strict basis from private.bpay_next_source_comparison_receipt where id=oldr.basis_review_id;
      promoting:=basis.receipt_kind='PENDING_REVIEW' and basis.review_certificate_id=w.id
        and oldr.review_certificate_id=w.id and new.basis_review_id=basis.id
        and (oldr.target_pay_channel,oldr.comparison_policy_id,oldr.policy_window_id,oldr.effective_pay_date,
          oldr.umbrella_vat_chargeable,oldr.selected_mode,oldr.quote_json,oldr.basis_sha256)
          is not distinct from (new.target_pay_channel,new.comparison_policy_id,new.policy_window_id,new.effective_pay_date,
            new.umbrella_vat_chargeable,new.selected_mode,new.quote_json,new.basis_sha256);
      if not promoting then raise exception 'BPAY_NEXT_COMPARISON_PENDING_PROMOTION_BASIS_CONFLICT';end if;
    end if;
    if new.case_basis_before is distinct from oldr.case_basis or new.case_basis is distinct from actual_case
      or (not promoting and new.case_basis is distinct from nullif(ctx->'case_basis','null'::jsonb))
      or (promoting and oldr.case_basis is distinct from actual_case)
      or new.case_resolution is distinct from (case when promoting then oldr.case_resolution else nullif(plan->'case_resolution','null'::jsonb) end)
      or (new.work_collection_id::text,new.case_id::text,new.case_component_id::text)
        is distinct from (actual_case->>'collection_id',actual_case->>'case_id',actual_case->>'case_component_id') then
      raise exception 'BPAY_NEXT_COMPARISON_CASE_CURRENT_REVIEW_REQUIRED';end if;
    if (new.receipt_kind='REVIEW' and (w.decision_bundle_id<>v.decision_bundle_id or w.bundle_revision<>v.bundle_revision))
      or new.review_sha256 is distinct from w.review_sha256 or new.actor_user_id<>w.actor_user_id
      or w.accepted_request_sha256 is distinct from private.weekly_source_publication_request_digest_v1(
        private.weekly_source_publication_request_canonical_v1(w.accepted_request_json,'IMMEDIATE',null))
      or w.context_json->>'financial_plan_digest' is distinct from validation->>'financial_plan_digest'
      or validation->>'ok' is distinct from 'true'
      or ctx is null or plan is null
      or (not promoting and (ctx->>'canonical_work_id',ctx->>'canonical_component_key',ctx->>'source_book_revision',
          ctx->>'source_comparison_receipt_id',ctx->>'source_comparison_revision')
        is distinct from (p.work_id::text,p.component_key,p.source_book_revision::text,p.source_comparison_receipt_id::text,p.source_comparison_revision::text))
      or (not promoting and ((ctx->>'native_realised_paye')::numeric,(ctx->>'native_realised_umbrella')::numeric,
          (ctx->>'native_held_paye')::numeric,(ctx->>'native_held_umbrella')::numeric)
        is distinct from (p.native_realised_paye,p.native_realised_umbrella,p.native_held_paye,p.native_held_umbrella))
      or (new.target_pay_channel,new.comparison_policy_id::text,new.policy_window_id::text,new.effective_pay_date::text,
          new.umbrella_vat_chargeable::text,new.selected_mode,new.quote_json,new.basis_sha256,
          new.comparable_paid_ex_vat,new.comparable_held_ex_vat,new.approved_source_ex_vat)
        is distinct from (plan->>'target_pay_channel',plan->>'comparison_policy_id',plan->>'policy_window_id',plan->>'effective_pay_date',
          plan->>'umbrella_vat_chargeable',plan->>'selected_mode',plan->'quote_json',decode(plan->>'basis_sha256','hex'),
          case when promoting then oldr.comparable_paid_ex_vat else (plan->>'comparable_paid_ex_vat')::numeric end,
          case when promoting then oldr.comparable_held_ex_vat else (plan->>'comparable_held_ex_vat')::numeric end,
          (plan->>'approved_source_ex_vat')::numeric)
      or (new.receipt_kind='REVIEW' and new.target_pay_channel<>v.approval_source_channel)
      or new.observed_approved_source_ex_vat is distinct from (case when v.component_state='REMOVED' then 0::numeric
        else (select a.source_pay_ex_vat from private.bpay_next_approved_line a where a.id=v.approved_line_id) end) then
      raise exception using errcode='23514',message='BPAY_NEXT_COMPARISON_REVIEW_NOT_SERVER_CERTIFIED';end if;
    if new.receipt_kind='REVIEW' and new.approved_source_ex_vat<>new.observed_approved_source_ex_vat then
      raise exception 'BPAY_NEXT_COMPARISON_REVIEW_ACTUAL_APPROVAL_MISMATCH';end if;
    if new.receipt_kind='PENDING_REVIEW' and (ctx->>'source_binding_version_id' is distinct from v.id::text
      or new.qualification_state<>'PENDING' or new.basis_review_id<>new.id) then
      raise exception 'BPAY_NEXT_COMPARISON_PENDING_CURRENT_INPUT_REQUIRED';end if;
    if not promoting and new.basis_review_id<>new.id then raise exception 'BPAY_NEXT_COMPARISON_REVIEW_BASIS_INVALID';end if;
    -- Root PLAN owns genuine original paid/held quantity proof; explicit refusal, not a money/rate guess.
    if new.selected_mode='MANUAL_REPLACEMENT_RATE' and plan->>'quantity_partition_qualified' is distinct from 'true' then
      raise exception using errcode='55000',message='BPAY_NEXT_COMPARISON_PAID_RATE_QUANTITY_UNBOUND';end if;
    return new;
  end if;
  if new.receipt_kind='CASE_RECONCILE' then
    proof:=private.bpay_next_source_comparison_case_reconcile_proof_v2(oldr,new.operation_job_id,new.work_collection_id);
    select * into strict basis from private.bpay_next_source_comparison_receipt where id=oldr.basis_review_id;
    expected_state:=case when basis.receipt_kind='PENDING_REVIEW' and basis.source_binding_version_id=oldr.source_binding_version_id then 'PENDING' else 'READY' end;
    if new.operation_identity<>new.work_collection_id or new.operation_case_event_id is distinct from (proof->>'case_event_id')::uuid
      or new.case_basis_before is distinct from oldr.case_basis or new.case_basis is distinct from proof->'case_basis'
      or new.case_resolution is distinct from proof->'case_resolution' or new.qualification_state<>expected_state
      or new.review_issue_code is distinct from (case when expected_state='PENDING' then 'SOURCE_PUBLICATION_PENDING' else null end)
      or (new.work_collection_id::text,new.case_id::text,new.case_component_id::text)
        is distinct from (proof#>>'{case_basis,collection_id}',proof#>>'{case_basis,case_id}',proof#>>'{case_basis,case_component_id}')
      or (to_jsonb(new)-array['id','receipt_kind','qualification_state','review_issue_code','previous_comparison_id','comparison_revision_before',
        'comparison_revision','source_book_revision_before','native_realised_paye_before','native_realised_umbrella_before','native_held_paye_before',
        'native_held_umbrella_before','comparable_paid_ex_vat_before','comparable_held_ex_vat_before','case_basis_before','case_basis','case_resolution',
        'work_collection_id','case_id','case_component_id','operation_job_id','operation_identity','operation_case_event_id','native_delta_realised',
        'native_delta_held','native_source_channel','comparable_operation_ex_vat','local_work_id','local_component_key','local_scalar_before','local_scalar_after',
        'operation_effect_id','operation_hold_id','operation_case_instruction_id','operation_case_result_id','operation_case_hold_id','accepted_at_utc','snapshot_sha256'])
      is distinct from (to_jsonb(oldr)-array['id','receipt_kind','qualification_state','review_issue_code','previous_comparison_id','comparison_revision_before',
        'comparison_revision','source_book_revision_before','native_realised_paye_before','native_realised_umbrella_before','native_held_paye_before',
        'native_held_umbrella_before','comparable_paid_ex_vat_before','comparable_held_ex_vat_before','case_basis_before','case_basis','case_resolution',
        'work_collection_id','case_id','case_component_id','operation_job_id','operation_identity','operation_case_event_id','native_delta_realised',
        'native_delta_held','native_source_channel','comparable_operation_ex_vat','local_work_id','local_component_key','local_scalar_before','local_scalar_after',
        'operation_effect_id','operation_hold_id','operation_case_instruction_id','operation_case_result_id','operation_case_hold_id','accepted_at_utc','snapshot_sha256']) then
      raise exception 'BPAY_NEXT_COMPARISON_CASE_RECONCILE_CERTIFICATE_INVALID';end if;
    return new;
  end if;
  if new.receipt_kind in ('CASE_RESERVE','CASE_RELEASE') then
    proof:=private.bpay_next_source_comparison_case_capacity_proof_v2(oldr,new.operation_job_id,new.operation_case_hold_id,new.receipt_kind);
    if new.operation_identity is distinct from new.operation_case_hold_id
      or (new.operation_case_instruction_id,new.operation_case_result_id,new.native_source_channel,new.comparable_operation_ex_vat)
        is distinct from ((proof->>'instruction_id')::uuid,(proof->>'allocation_result_id')::uuid,
          proof->>'native_source_channel',(proof->>'comparable_operation_ex_vat')::numeric)
      or (new.qualification_state,new.review_issue_code,new.case_basis_before,new.case_basis,new.case_resolution)
        is distinct from (proof->>'qualification_state',proof->>'review_issue_code',oldr.case_basis,proof->'case_basis',proof->'case_resolution')
      or (new.work_collection_id,new.case_id,new.case_component_id) is distinct from (oldr.work_collection_id,oldr.case_id,oldr.case_component_id)
      or (new.source_binding_version_id,new.basis_review_id,new.review_certificate_id,new.review_sha256,new.basis_sha256,new.target_pay_channel,
          new.comparison_policy_id,new.policy_window_id,new.effective_pay_date,new.umbrella_vat_chargeable,new.selected_mode,new.quote_json,
          new.approved_source_ex_vat,new.observed_approved_source_ex_vat,new.comparable_paid_ex_vat,new.comparable_held_ex_vat)
        is distinct from (oldr.source_binding_version_id,oldr.basis_review_id,oldr.review_certificate_id,oldr.review_sha256,oldr.basis_sha256,oldr.target_pay_channel,
          oldr.comparison_policy_id,oldr.policy_window_id,oldr.effective_pay_date,oldr.umbrella_vat_chargeable,oldr.selected_mode,oldr.quote_json,
          oldr.approved_source_ex_vat,oldr.observed_approved_source_ex_vat,oldr.comparable_paid_ex_vat,oldr.comparable_held_ex_vat) then
      raise exception 'BPAY_NEXT_COMPARISON_CASE_CAPACITY_CERTIFICATE_INVALID';end if;
    return new;
  end if;
  select * into strict op from private.bpay_next_source_comparison_operation_v2(new.operation_job_id,new.receipt_kind,new.operation_identity);
  if (new.candidate_id,new.canonical_work_id,new.canonical_component_key,new.local_work_id,new.local_component_key,
      new.native_source_channel,new.operation_hold_id,new.operation_effect_id,new.operation_case_event_id,
      new.operation_case_instruction_id,new.operation_case_result_id)
    is distinct from (op.candidate_id,op.canonical_work_id,op.canonical_component_key,op.local_work_id,op.local_component_key,
      op.native_channel,op.hold_id,op.effect_id,op.case_event_id,op.case_instruction_id,op.case_result_id)
    or (new.source_binding_version_id,new.basis_review_id,new.review_certificate_id,new.review_sha256,new.basis_sha256,
      new.target_pay_channel,new.comparison_policy_id,new.policy_window_id,new.effective_pay_date,new.umbrella_vat_chargeable,
      new.selected_mode,new.quote_json,new.approved_source_ex_vat,new.observed_approved_source_ex_vat)
      is distinct from (oldr.source_binding_version_id,oldr.basis_review_id,oldr.review_certificate_id,oldr.review_sha256,oldr.basis_sha256,
        oldr.target_pay_channel,oldr.comparison_policy_id,oldr.policy_window_id,oldr.effective_pay_date,oldr.umbrella_vat_chargeable,
        oldr.selected_mode,oldr.quote_json,oldr.approved_source_ex_vat,oldr.observed_approved_source_ex_vat) then
    raise exception using errcode='23514',message='BPAY_NEXT_COMPARISON_OPERATION_CERTIFICATE_INVALID';end if;
  dr:=case new.receipt_kind when 'SETTLE' then op.native_amount when 'CASE_RECOVERY' then -op.native_amount else 0 end;
  dh:=case new.receipt_kind when 'RESERVE' then op.native_amount when 'SETTLE' then -op.native_amount when 'RELEASE' then -op.native_amount else 0 end;
  if (new.native_delta_realised,new.native_delta_held,new.native_realised_paye,new.native_realised_umbrella,new.native_held_paye,new.native_held_umbrella)
    is distinct from (dr,dh,p.native_realised_paye+case when op.native_channel='PAYE' then dr else 0 end,
      p.native_realised_umbrella+case when op.native_channel='UMBRELLA' then dr else 0 end,
      p.native_held_paye+case when op.native_channel='PAYE' then dh else 0 end,
      p.native_held_umbrella+case when op.native_channel='UMBRELLA' then dh else 0 end) then
    raise exception using errcode='23514',message='BPAY_NEXT_COMPARISON_NATIVE_DELTA_INVALID';end if;
  select * into strict localp from private.bpay_next_position where work_id=op.local_work_id and component_key=op.local_component_key;
  if new.local_scalar_before is distinct from private.bpay_next_source_comparison_scalar_v2(localp)
    or new.local_scalar_after is distinct from (case when not op.updates_local_scalar then new.local_scalar_before
      else private.bpay_next_source_comparison_scalar_delta_v2(
        new.local_scalar_before,new.receipt_kind,op.native_amount,op.target_ex,op.target_vat,op.target_inc) end) then
    raise exception using errcode='23514',message='BPAY_NEXT_COMPARISON_LOCAL_SCALAR_INVALID';end if;
  if new.receipt_kind='RESERVE' and (oldr.qualification_state<>'READY' or oldr.selected_residual_ex_vat<>op.native_amount
    or op.native_channel<>oldr.target_pay_channel or op.target_ex<>op.native_amount
    or not exists(select 1 from private.bpay_next_hold h join private.bpay_next_run_line l on l.id=h.run_line_id
      where h.id=op.hold_id and l.source_comparison_receipt_id=oldr.id and l.source_book_revision=oldr.source_book_revision
        and l.source_binding_version_id=oldr.source_binding_version_id and l.target_pay_channel=oldr.target_pay_channel)) then
    raise exception 'BPAY_NEXT_COMPARISON_NEW_RESERVE_NOT_EXACT_RESIDUAL';end if;
  if new.receipt_kind='RESERVE' then
    q:=op.native_amount;qualified:=op.native_channel=oldr.target_pay_channel;
  elsif new.receipt_kind='CASE_RECOVERY' then
    select * into cm from private.bpay_next_source_comparison_case_hold_mapping where basis_review_id=oldr.basis_review_id
      and case_hold_id=(select operation_item_id from private.bpay_next_case_event where id=op.case_event_id);
    if found then
      qualified:=cm.native_target_channel=op.native_channel and cm.native_target_ex_vat=op.native_amount
        and cm.instruction_id=op.case_instruction_id and cm.allocation_result_id=op.case_result_id;q:=cm.comparable_recovery_ex_vat;
    else q:=op.native_amount;qualified:=op.native_channel=oldr.target_pay_channel;end if;
  else
    select * into m from private.bpay_next_source_comparison_hold_mapping where basis_review_id=oldr.basis_review_id and hold_id=op.hold_id;
    qualified:=found and m.native_source_channel=op.native_channel and m.native_reserved_ex_vat=op.native_amount;
    q:=m.comparable_reserved_ex_vat;
  end if;
  credit_qualified:=qualified and (oldr.qualification_state in ('READY','PENDING') or oldr.review_issue_code='CASE_RECONCILIATION_REQUIRED');
  case_qualified:=true;
  actual_case:=private.bpay_next_source_comparison_case_basis_v2(new.canonical_work_id,new.canonical_component_key,false);
  if new.case_basis_before is distinct from oldr.case_basis or new.case_basis is distinct from actual_case
    or (new.work_collection_id::text,new.case_id::text,new.case_component_id::text)
      is distinct from (actual_case->>'collection_id',actual_case->>'case_id',actual_case->>'case_component_id') then
    raise exception 'BPAY_NEXT_COMPARISON_CASE_CARRY_TUPLE_INVALID';end if;
  if oldr.case_basis is not null then
    plan:=private.bpay_next_source_comparison_case_remainder_v2(oldr,new.receipt_kind,op.case_event_id,q,
      oldr.comparable_paid_ex_vat+case new.receipt_kind when 'SETTLE' then q when 'CASE_RECOVERY' then -q else 0 end,actual_case);
    case_qualified:=plan is not null;
    if new.case_resolution is distinct from coalesce(plan,oldr.case_resolution) then raise exception 'BPAY_NEXT_COMPARISON_CASE_CARRY_REMAINDER_INVALID';end if;
  elsif actual_case is not null then case_qualified:=false;end if;
  if credit_qualified then
    if new.qualification_state is distinct from (case when case_qualified then oldr.qualification_state else 'REVIEW' end)
      or (not case_qualified and new.review_issue_code is distinct from 'CASE_RECONCILIATION_REQUIRED')
      or new.comparable_operation_ex_vat is distinct from q
      or (new.comparable_paid_ex_vat,new.comparable_held_ex_vat) is distinct from
        (oldr.comparable_paid_ex_vat+case new.receipt_kind when 'SETTLE' then q when 'CASE_RECOVERY' then -q else 0 end,
         oldr.comparable_held_ex_vat+case new.receipt_kind when 'RESERVE' then q when 'SETTLE' then -q when 'RELEASE' then -q else 0 end)
      then raise exception using errcode='23514',message='BPAY_NEXT_COMPARISON_CARRY_AMOUNT_INVALID';end if;
  elsif new.qualification_state<>'REVIEW' or new.comparable_operation_ex_vat is not null
    or new.review_issue_code<>'ACTUAL_OPERATION_COMPARABLE_MAPPING_UNBOUND' then
    raise exception using errcode='23514',message='BPAY_NEXT_COMPARISON_CARRY_REVIEW_REQUIRED';end if;
  return new;
end $function$;
create trigger bpay_next_source_comparison_guard_v2 before insert or update or delete on private.bpay_next_source_comparison_receipt
  for each row execute function private.bpay_next_source_comparison_guard_v2();
create trigger bpay_next_source_comparison_truncate_v2 before truncate on private.bpay_next_source_comparison_receipt
  for each statement execute function private.bpay_next_source_comparison_guard_v2();

create or replace function private.bpay_next_source_comparison_hold_guard_v2()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private,public as $function$
declare r private.bpay_next_source_comparison_receipt%rowtype;h private.bpay_next_hold%rowtype;l private.bpay_next_run_line%rowtype;
  w private.weekly_source_destination_reprice_witness_v1%rowtype;v private.bpay_next_source_component_binding_version%rowtype;plan jsonb;qty numeric;
begin
  if tg_op<>'INSERT' then raise exception 'BPAY_NEXT_SOURCE_COMPARISON_HOLD_IMMUTABLE';end if;
  select * into strict r from private.bpay_next_source_comparison_receipt where id=new.created_by_comparison_id;
  select * into strict h from private.bpay_next_hold where id=new.hold_id;
  select * into strict l from private.bpay_next_run_line where id=h.run_line_id;
  if r.basis_review_id<>new.basis_review_id or h.status<>'ACTIVE'
    or (new.run_line_id,new.native_source_channel,new.native_reserved_ex_vat)
      is distinct from (l.id,l.source_pay_channel,h.source_reserved_ex_vat)
    or new.mapping_sha256 is distinct from sha256(convert_to((to_jsonb(new)-'mapping_sha256')::text,'UTF8')) then
    raise exception 'BPAY_NEXT_COMPARISON_HOLD_MAPPING_INVALID';end if;
  if r.receipt_kind='RESERVE' then
    if r.operation_hold_id<>h.id or r.qualification_state<>'READY' or new.comparable_reserved_ex_vat<>r.comparable_operation_ex_vat then
      raise exception 'BPAY_NEXT_COMPARISON_NEW_HOLD_MAPPING_INVALID';end if;
  elsif r.receipt_kind in ('REVIEW','PENDING_REVIEW') then
    select * into strict w from private.weekly_source_destination_reprice_witness_v1 where id=r.review_certificate_id;
    select * into strict v from private.bpay_next_source_component_binding_version where id=r.source_binding_version_id;
    select c->'plan' into strict plan from jsonb_array_elements(w.context_json#>'{financial_plan,components}') c
      where c->>'source_component_id'=v.source_component_id::text;
    select (x->>'comparable_reserved_ex_vat')::numeric into strict qty from jsonb_array_elements(plan->'hold_mappings') x
      where x->>'hold_id'=h.id::text;
    if qty is distinct from new.comparable_reserved_ex_vat then raise exception 'BPAY_NEXT_COMPARISON_OLD_HOLD_MAPPING_UNBOUND';end if;
  else raise exception 'BPAY_NEXT_COMPARISON_HOLD_MAPPING_OWNER_INVALID';end if;
  return new;
end $function$;
create trigger bpay_next_source_comparison_hold_guard_v2 before insert or update or delete on private.bpay_next_source_comparison_hold_mapping
  for each row execute function private.bpay_next_source_comparison_hold_guard_v2();
create trigger bpay_next_source_comparison_hold_truncate_v2 before truncate on private.bpay_next_source_comparison_hold_mapping
  for each statement execute function private.bpay_next_source_comparison_hold_guard_v2();

create or replace function private.bpay_next_source_comparison_case_hold_guard_v2()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private as $function$
declare r private.bpay_next_source_comparison_receipt%rowtype;h private.bpay_next_case_hold%rowtype;
  i private.bpay_next_run_case_instruction%rowtype;w private.weekly_source_destination_reprice_witness_v1%rowtype;
  v private.bpay_next_source_component_binding_version%rowtype;qty numeric;plan jsonb;
begin
  if tg_op<>'INSERT' then raise exception 'BPAY_NEXT_COMPARISON_CASE_HOLD_IMMUTABLE';end if;
  select * into strict r from private.bpay_next_source_comparison_receipt where id=new.created_by_comparison_id;
  select * into strict h from private.bpay_next_case_hold where id=new.case_hold_id;
  select * into strict i from private.bpay_next_run_case_instruction where id=h.instruction_id;
  select * into strict w from private.weekly_source_destination_reprice_witness_v1 where id=r.review_certificate_id;
  select * into strict v from private.bpay_next_source_component_binding_version where id=r.source_binding_version_id;
  select c->'plan' into strict plan from jsonb_array_elements(w.context_json#>'{financial_plan,components}') c
    where c->>'source_component_id'=v.source_component_id::text;
  select (x->>'comparable_recovery_ex_vat')::numeric into strict qty from jsonb_array_elements(plan->'case_hold_mappings') x
    where x->>'case_hold_id'=h.id::text;
  if r.receipt_kind not in ('REVIEW','PENDING_REVIEW') or r.basis_review_id<>new.basis_review_id or h.status<>'ACTIVE'
    or i.instruction_kind<>'RECOVERY' or i.case_kind<>'OVERPAYMENT'
    or (new.instruction_id,new.allocation_result_id,new.native_target_channel,new.native_target_ex_vat,new.comparable_recovery_ex_vat)
      is distinct from (i.id,h.allocation_result_id,i.target_pay_channel,h.target_amount_ex_vat,qty)
    or not exists(select 1 from private.bpay_next_work_collection c where c.id=i.work_collection_id
      and c.case_id=i.case_id and c.candidate_id=r.candidate_id and c.work_id=r.canonical_work_id and c.component_key=r.canonical_component_key)
    or new.mapping_sha256 is distinct from sha256(convert_to((to_jsonb(new)-'mapping_sha256')::text,'UTF8')) then
    raise exception 'BPAY_NEXT_COMPARISON_CASE_HOLD_NOT_CERTIFIED';end if;
  return new;
end $function$;
create trigger bpay_next_source_comparison_case_hold_guard_v2 before insert or update or delete
  on private.bpay_next_source_comparison_case_hold_mapping for each row execute function private.bpay_next_source_comparison_case_hold_guard_v2();
create trigger bpay_next_source_comparison_case_hold_truncate_v2 before truncate
  on private.bpay_next_source_comparison_case_hold_mapping for each statement execute function private.bpay_next_source_comparison_case_hold_guard_v2();

-- Root precommit/register wrappers recompute the shared quote. This core accepts ONLY that immutable witness's exact component plan.
create or replace function private.bpay_next_source_comparison_register_impl_v2(p_source_binding_version_id uuid,
  p_review_certificate_id uuid,p_financial_context jsonb,p_financial_plan jsonb,p_pending boolean)
returns uuid language plpgsql security invoker set search_path=pg_catalog,private,public as $function$
declare r private.bpay_next_source_comparison_receipt%rowtype;p private.bpay_next_position%rowtype;
  prior private.bpay_next_source_comparison_receipt%rowtype;v private.bpay_next_source_component_binding_version%rowtype;
  w private.weekly_source_destination_reprice_witness_v1%rowtype;item jsonb;m private.bpay_next_source_comparison_hold_mapping%rowtype;
  h private.bpay_next_hold%rowtype;l private.bpay_next_run_line%rowtype;held_p numeric:=0;held_u numeric:=0;held_c numeric:=0;matched jsonb;
  promoting boolean:=false;basis private.bpay_next_source_comparison_receipt%rowtype;
  ch private.bpay_next_case_hold%rowtype;ci private.bpay_next_run_case_instruction%rowtype;cm private.bpay_next_source_comparison_case_hold_mapping%rowtype;
  actual_case jsonb;
begin
  select * into strict v from private.bpay_next_source_component_binding_version where id=p_source_binding_version_id;
  select * into strict w from private.weekly_source_destination_reprice_witness_v1 where id=p_review_certificate_id;
  perform 1 from private.bpay_next_worker_control where candidate_id=v.candidate_id for update;
  perform 1 from private.bpay_next_work where id in (v.canonical_work_id,v.approval_work_id) order by id for update;
  select * into strict p from private.bpay_next_position where work_id=v.canonical_work_id and component_key=v.canonical_component_key for update;
  select c into strict matched from jsonb_array_elements(w.context_json#>'{financial_plan,components}') c where c->>'source_component_id'=v.source_component_id::text;
  if (matched->'context',matched->'plan') is distinct from (p_financial_context,p_financial_plan)
    or not exists(select 1 from private.bpay_next_source_component_binding b where b.canonical_work_id=v.canonical_work_id
      and b.canonical_component_key=v.canonical_component_key and b.current_binding_version_id=v.id) then
    raise exception 'BPAY_NEXT_COMPARISON_REVIEW_CORE_NOT_CERTIFIED';end if;
  select x.id into r.id from private.bpay_next_source_comparison_receipt x
    where x.receipt_kind=case when p_pending then 'PENDING_REVIEW' else 'REVIEW' end
    and x.review_certificate_id=w.id and x.source_binding_version_id=v.id;
  if found then return r.id;end if;
  if p.source_comparison_receipt_id is not null then select * into strict prior from private.bpay_next_source_comparison_receipt where id=p.source_comparison_receipt_id;end if;
  if not p_pending and prior.qualification_state='PENDING' then
    select * into strict basis from private.bpay_next_source_comparison_receipt where id=prior.basis_review_id;
    promoting:=basis.receipt_kind='PENDING_REVIEW' and basis.review_certificate_id=w.id and prior.review_certificate_id=w.id;
    if not promoting then raise exception 'BPAY_NEXT_COMPARISON_PENDING_REVIEW_CERTIFICATE_CONFLICT';end if;
  end if;
  actual_case:=private.bpay_next_source_comparison_case_basis_v2(v.canonical_work_id,v.canonical_component_key,true);
  if (promoting and prior.case_basis is distinct from actual_case)
    or (not promoting and nullif(p_financial_context->'case_basis','null'::jsonb) is distinct from actual_case) then
    raise exception 'BPAY_NEXT_COMPARISON_CASE_CURRENT_REVIEW_REQUIRED';end if;
  r.id:=gen_random_uuid();r.receipt_kind:=case when p_pending then 'PENDING_REVIEW' else 'REVIEW' end;
  r.qualification_state:=case when p_pending then 'PENDING' else 'READY' end;
  r.review_issue_code:=case when p_pending then 'SOURCE_PUBLICATION_PENDING' else null end;r.candidate_id:=v.candidate_id;
  r.canonical_work_id:=v.canonical_work_id;r.canonical_component_key:=v.canonical_component_key;r.source_binding_version_id:=v.id;
  r.approval_work_id:=v.approval_work_id;r.approval_revision_id:=v.approval_revision_id;r.approval_component_key:=v.approval_component_key;r.approval_component_state:=v.component_state;
  r.approved_line_id:=v.approved_line_id;r.binding_digest:=v.binding_digest;r.approval_component_sha256:=v.component_sha256;r.approval_chosen_detail_sha256:=v.chosen_detail_sha256;
  r.review_certificate_id:=w.id;r.review_sha256:=w.review_sha256;
  r.basis_review_id:=case when promoting then prior.basis_review_id else r.id end;r.previous_comparison_id:=p.source_comparison_receipt_id;
  r.comparison_revision_before:=p.source_comparison_revision;r.comparison_revision:=p.source_comparison_revision+1;
  r.source_book_revision_before:=p.source_book_revision;r.source_book_revision:=p.source_book_revision;
  r.native_realised_paye_before:=p.native_realised_paye;r.native_realised_umbrella_before:=p.native_realised_umbrella;
  r.native_held_paye_before:=p.native_held_paye;r.native_held_umbrella_before:=p.native_held_umbrella;
  r.native_realised_paye:=p.native_realised_paye;r.native_realised_umbrella:=p.native_realised_umbrella;r.native_held_paye:=p.native_held_paye;r.native_held_umbrella:=p.native_held_umbrella;
  r.comparable_paid_ex_vat_before:=prior.comparable_paid_ex_vat;r.comparable_held_ex_vat_before:=prior.comparable_held_ex_vat;
  r.comparable_paid_ex_vat:=case when promoting then prior.comparable_paid_ex_vat else (p_financial_plan->>'comparable_paid_ex_vat')::numeric end;
  r.comparable_held_ex_vat:=case when promoting then prior.comparable_held_ex_vat else (p_financial_plan->>'comparable_held_ex_vat')::numeric end;
  r.approved_source_ex_vat:=(p_financial_plan->>'approved_source_ex_vat')::numeric;r.selected_residual_ex_vat:=r.approved_source_ex_vat-r.comparable_paid_ex_vat-r.comparable_held_ex_vat;
  r.observed_approved_source_ex_vat:=case when v.component_state='REMOVED' then 0::numeric else
    (select x.source_pay_ex_vat from private.bpay_next_approved_line x where x.id=v.approved_line_id) end;
  r.target_pay_channel:=p_financial_plan->>'target_pay_channel';r.comparison_policy_id:=(p_financial_plan->>'comparison_policy_id')::uuid;
  r.policy_window_id:=(p_financial_plan->>'policy_window_id')::uuid;r.effective_pay_date:=(p_financial_plan->>'effective_pay_date')::date;
  r.umbrella_vat_chargeable:=(p_financial_plan->>'umbrella_vat_chargeable')::boolean;r.selected_mode:=p_financial_plan->>'selected_mode';
  r.quote_json:=p_financial_plan->'quote_json';r.basis_sha256:=decode(p_financial_plan->>'basis_sha256','hex');r.actor_user_id:=w.actor_user_id;
  r.case_basis_before:=prior.case_basis;r.case_basis:=actual_case;
  r.work_collection_id:=(actual_case->>'collection_id')::uuid;r.case_id:=(actual_case->>'case_id')::uuid;r.case_component_id:=(actual_case->>'case_component_id')::uuid;
  r.case_resolution:=case when promoting then prior.case_resolution else nullif(p_financial_plan->'case_resolution','null'::jsonb) end;
  r.reason:=left(w.reason,256);r.accepted_at_utc:=transaction_timestamp();r.snapshot_sha256:=sha256(convert_to((to_jsonb(r)-'snapshot_sha256')::text,'UTF8'));
  insert into private.bpay_next_source_comparison_receipt select (r).*;
  if not promoting then
  if jsonb_typeof(p_financial_plan->'hold_mappings') is distinct from 'array' then
    raise exception 'BPAY_NEXT_COMPARISON_HOLD_MAPPING_PAGE_INVALID';end if;
  for item in select x from jsonb_array_elements(p_financial_plan->'hold_mappings') x loop
    select * into strict h from private.bpay_next_hold where id=(item->>'hold_id')::uuid for share;
    select * into strict l from private.bpay_next_run_line where id=h.run_line_id;
    if not exists(select 1 from private.bpay_next_run_worker candidate_worker
      where candidate_worker.id=l.run_worker_id and candidate_worker.candidate_id=r.candidate_id)
      or l.component_key<>r.canonical_component_key
      or not ((l.canonical_source_work_id=r.canonical_work_id and l.canonical_source_component_key=r.canonical_component_key)
        or (l.source_binding_version_id is null and l.work_id=r.canonical_work_id)) then
      raise exception 'BPAY_NEXT_COMPARISON_HOLD_MAPPING_COMPONENT_INVALID';end if;
    m.basis_review_id:=r.id;m.hold_id:=h.id;m.run_line_id:=l.id;m.native_source_channel:=l.source_pay_channel;
    m.native_reserved_ex_vat:=h.source_reserved_ex_vat;m.comparable_reserved_ex_vat:=(item->>'comparable_reserved_ex_vat')::numeric;m.created_by_comparison_id:=r.id;
    m.mapping_sha256:=sha256(convert_to((to_jsonb(m)-'mapping_sha256')::text,'UTF8'));
    insert into private.bpay_next_source_comparison_hold_mapping select(m).*;
    if m.native_source_channel='PAYE' then held_p:=held_p+m.native_reserved_ex_vat;else held_u:=held_u+m.native_reserved_ex_vat;end if;
    held_c:=held_c+m.comparable_reserved_ex_vat;
  end loop;
  if (held_p,held_u,held_c) is distinct from (r.native_held_paye,r.native_held_umbrella,r.comparable_held_ex_vat) then
    raise exception 'BPAY_NEXT_COMPARISON_COMPLETE_HOLD_MAPPING_REQUIRED';end if;
  if p_financial_plan ? 'case_hold_mappings' then
    if jsonb_typeof(p_financial_plan->'case_hold_mappings') is distinct from 'array' then raise exception 'BPAY_NEXT_COMPARISON_CASE_HOLD_MAPPING_INVALID';end if;
    for item in select x from jsonb_array_elements(p_financial_plan->'case_hold_mappings') x loop
      select * into strict ch from private.bpay_next_case_hold where id=(item->>'case_hold_id')::uuid for share;
      select * into strict ci from private.bpay_next_run_case_instruction where id=ch.instruction_id;
      cm.basis_review_id:=r.basis_review_id;cm.case_hold_id:=ch.id;cm.instruction_id:=ci.id;cm.allocation_result_id:=ch.allocation_result_id;
      cm.native_target_channel:=ci.target_pay_channel;cm.native_target_ex_vat:=ch.target_amount_ex_vat;
      cm.comparable_recovery_ex_vat:=(item->>'comparable_recovery_ex_vat')::numeric;cm.created_by_comparison_id:=r.id;
      cm.mapping_sha256:=sha256(convert_to((to_jsonb(cm)-'mapping_sha256')::text,'UTF8'));
      insert into private.bpay_next_source_comparison_case_hold_mapping select(cm).*;
    end loop;
  end if;
  end if;
  update private.bpay_next_position set source_comparison_receipt_id=r.id,source_comparison_revision=r.comparison_revision
    where work_id=r.canonical_work_id and component_key=r.canonical_component_key
      and source_book_revision=r.source_book_revision_before and source_comparison_revision=r.comparison_revision_before
      and source_comparison_receipt_id is not distinct from r.previous_comparison_id;
  if not found then raise exception 'BPAY_NEXT_COMPARISON_REVIEW_CAS_STALE';end if;
  return r.id;
end $function$;

create or replace function private.bpay_next_source_comparison_register_core_v2(p_source_binding_version_id uuid,
  p_review_certificate_id uuid,p_financial_context jsonb,p_financial_plan jsonb)
returns uuid language sql security invoker set search_path=pg_catalog,private as $function$
  select private.bpay_next_source_comparison_register_impl_v2(p_source_binding_version_id,p_review_certificate_id,p_financial_context,p_financial_plan,false)
$function$;
create or replace function private.bpay_next_source_comparison_register_pending_core_v2(p_review_certificate_id uuid,
  p_financial_context jsonb,p_financial_plan jsonb)
returns uuid language sql security invoker set search_path=pg_catalog,private as $function$
  select private.bpay_next_source_comparison_register_impl_v2((p_financial_context->>'source_binding_version_id')::uuid,
    p_review_certificate_id,p_financial_context,p_financial_plan,true)
$function$;

-- The ONLY connected native/scalar carry owner. Caller has already made the real hold/effect transition; skip the old scalar UPDATE.
create or replace function private.bpay_next_source_comparison_carry_v2(p_job_id uuid,p_expected_book_revision bigint,
  p_expected_comparison_id uuid,p_operation_kind text,p_operation_id uuid)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private,public as $function$
declare op record;p private.bpay_next_position%rowtype;localp private.bpay_next_position%rowtype;
  r private.bpay_next_source_comparison_receipt%rowtype;oldr private.bpay_next_source_comparison_receipt%rowtype;
  m private.bpay_next_source_comparison_hold_mapping%rowtype;dr numeric;dh numeric;q numeric;qualified boolean;
  cm private.bpay_next_source_comparison_case_hold_mapping%rowtype;
  actual_case jsonb;case_remainder jsonb;credit_qualified boolean;case_qualified boolean:=true;
begin
  -- Exact operation replay precedes currentness: old OUTPUT remains the immutable result after later moves.
  select * into r from private.bpay_next_source_comparison_receipt where receipt_kind=p_operation_kind and operation_identity=p_operation_id;
  if found then
    if (r.operation_job_id,r.source_book_revision_before,r.previous_comparison_id)
      is distinct from (p_job_id,p_expected_book_revision,p_expected_comparison_id) then raise exception 'BPAY_NEXT_COMPARISON_OPERATION_REPLAY_CONFLICT';end if;
    return jsonb_build_object('comparison_id',r.id,'comparison_revision',r.comparison_revision,'book_revision',r.source_book_revision,
      'qualification_state',r.qualification_state,'replay',true);
  end if;
  select * into strict op from private.bpay_next_source_comparison_operation_v2(p_job_id,p_operation_kind,p_operation_id);
  perform 1 from private.bpay_next_worker_control where candidate_id=op.candidate_id for update;
  perform 1 from private.bpay_next_work where id in(op.canonical_work_id,op.local_work_id) order by id for update;
  perform 1 from private.bpay_next_position where (work_id,component_key) in
    ((op.canonical_work_id,op.canonical_component_key),(op.local_work_id,op.local_component_key)) order by work_id,component_key for update;
  select * into strict p from private.bpay_next_position where work_id=op.canonical_work_id and component_key=op.canonical_component_key;
  if p.source_book_revision is distinct from p_expected_book_revision or p.source_comparison_receipt_id is distinct from p_expected_comparison_id
    or not private.bpay_next_source_comparison_book_certified_v2(p) then raise exception 'BPAY_NEXT_COMPARISON_CARRY_STALE';end if;
  select * into strict oldr from private.bpay_next_source_comparison_receipt where id=p.source_comparison_receipt_id;
  select * into strict localp from private.bpay_next_position where work_id=op.local_work_id and component_key=op.local_component_key;
  r:=oldr;r.id:=gen_random_uuid();r.receipt_kind:=p_operation_kind;r.review_issue_code:=null;
  r.previous_comparison_id:=oldr.id;r.comparison_revision_before:=p.source_comparison_revision;r.comparison_revision:=p.source_comparison_revision+1;
  r.source_book_revision_before:=p.source_book_revision;r.source_book_revision:=p.source_book_revision+1;
  r.native_realised_paye_before:=p.native_realised_paye;r.native_realised_umbrella_before:=p.native_realised_umbrella;
  r.native_held_paye_before:=p.native_held_paye;r.native_held_umbrella_before:=p.native_held_umbrella;
  dr:=case p_operation_kind when 'SETTLE' then op.native_amount when 'CASE_RECOVERY' then -op.native_amount else 0 end;
  dh:=case p_operation_kind when 'RESERVE' then op.native_amount when 'SETTLE' then -op.native_amount when 'RELEASE' then -op.native_amount else 0 end;
  r.native_realised_paye:=p.native_realised_paye+case when op.native_channel='PAYE' then dr else 0 end;
  r.native_realised_umbrella:=p.native_realised_umbrella+case when op.native_channel='UMBRELLA' then dr else 0 end;
  r.native_held_paye:=p.native_held_paye+case when op.native_channel='PAYE' then dh else 0 end;
  r.native_held_umbrella:=p.native_held_umbrella+case when op.native_channel='UMBRELLA' then dh else 0 end;
  r.native_source_channel:=op.native_channel;r.native_delta_realised:=dr;r.native_delta_held:=dh;
  r.operation_job_id:=p_job_id;r.operation_identity:=p_operation_id;r.operation_hold_id:=op.hold_id;r.operation_effect_id:=op.effect_id;
  r.operation_case_event_id:=op.case_event_id;r.operation_case_instruction_id:=op.case_instruction_id;r.operation_case_result_id:=op.case_result_id;
  r.operation_case_hold_id:=null;
  r.local_work_id:=op.local_work_id;r.local_component_key:=op.local_component_key;
  r.local_scalar_before:=private.bpay_next_source_comparison_scalar_v2(localp);
  r.local_scalar_after:=case when not op.updates_local_scalar then r.local_scalar_before
    else private.bpay_next_source_comparison_scalar_delta_v2(r.local_scalar_before,p_operation_kind,op.native_amount,op.target_ex,op.target_vat,op.target_inc) end;
  if op.updates_local_scalar and localp.source_basis_channel is distinct from op.native_channel then
    raise exception 'BPAY_NEXT_COMPARISON_LOCAL_NATIVE_BASIS_UNQUALIFIED';end if;
  r.comparable_paid_ex_vat_before:=oldr.comparable_paid_ex_vat;r.comparable_held_ex_vat_before:=oldr.comparable_held_ex_vat;
  if p_operation_kind='RESERVE' then q:=op.native_amount;qualified:=op.native_channel=oldr.target_pay_channel;
  elsif p_operation_kind='CASE_RECOVERY' then
    select * into cm from private.bpay_next_source_comparison_case_hold_mapping where basis_review_id=oldr.basis_review_id
      and case_hold_id=(select operation_item_id from private.bpay_next_case_event where id=op.case_event_id);
    if found then
      qualified:=cm.native_target_channel=op.native_channel and cm.native_target_ex_vat=op.native_amount
        and cm.instruction_id=op.case_instruction_id and cm.allocation_result_id=op.case_result_id;q:=cm.comparable_recovery_ex_vat;
    else q:=op.native_amount;qualified:=op.native_channel=oldr.target_pay_channel;end if;
  else
    select * into m from private.bpay_next_source_comparison_hold_mapping where basis_review_id=oldr.basis_review_id and hold_id=op.hold_id;
    qualified:=found and m.native_source_channel=op.native_channel and m.native_reserved_ex_vat=op.native_amount;q:=m.comparable_reserved_ex_vat;
  end if;
  credit_qualified:=qualified and (oldr.qualification_state in ('READY','PENDING') or oldr.review_issue_code='CASE_RECONCILIATION_REQUIRED');
  actual_case:=private.bpay_next_source_comparison_case_basis_v2(op.canonical_work_id,op.canonical_component_key,true);
  r.case_basis_before:=oldr.case_basis;r.case_basis:=actual_case;
  r.work_collection_id:=(actual_case->>'collection_id')::uuid;r.case_id:=(actual_case->>'case_id')::uuid;r.case_component_id:=(actual_case->>'case_component_id')::uuid;
  if oldr.case_basis is not null then
    case_remainder:=private.bpay_next_source_comparison_case_remainder_v2(oldr,p_operation_kind,op.case_event_id,q,
      oldr.comparable_paid_ex_vat+case p_operation_kind when 'SETTLE' then q when 'CASE_RECOVERY' then -q else 0 end,actual_case);
    case_qualified:=case_remainder is not null;r.case_resolution:=coalesce(case_remainder,oldr.case_resolution);
  elsif actual_case is not null then
    case_qualified:=false;r.case_resolution:=jsonb_build_object('qualification_issue','CASE_OWNER_CHANGED_WITHOUT_ACCEPTED_BASIS');
  end if;
  if p_operation_kind='RESERVE' and oldr.qualification_state<>'READY' then raise exception 'BPAY_NEXT_COMPARISON_PENDING_DENIES_NEW_RESERVE';end if;
  if credit_qualified then
    r.qualification_state:=case when case_qualified then oldr.qualification_state else 'REVIEW' end;
    r.review_issue_code:=case when not case_qualified or oldr.review_issue_code='CASE_RECONCILIATION_REQUIRED' then 'CASE_RECONCILIATION_REQUIRED'
      when oldr.qualification_state='PENDING' then 'SOURCE_PUBLICATION_PENDING' else null end;
    r.comparable_operation_ex_vat:=q;
    r.comparable_paid_ex_vat:=oldr.comparable_paid_ex_vat+case p_operation_kind when 'SETTLE' then q when 'CASE_RECOVERY' then -q else 0 end;
    r.comparable_held_ex_vat:=oldr.comparable_held_ex_vat+case p_operation_kind when 'RESERVE' then q when 'SETTLE' then -q when 'RELEASE' then -q else 0 end;
    r.selected_residual_ex_vat:=r.approved_source_ex_vat-r.comparable_paid_ex_vat-r.comparable_held_ex_vat;
  else
    if p_operation_kind='RESERVE' then raise exception 'BPAY_NEXT_COMPARISON_NEW_RESERVE_REVIEW_REQUIRED';end if;
    r.qualification_state:='REVIEW';r.review_issue_code:='ACTUAL_OPERATION_COMPARABLE_MAPPING_UNBOUND';
    r.comparable_operation_ex_vat:=null;r.comparable_paid_ex_vat:=null;r.comparable_held_ex_vat:=null;r.selected_residual_ex_vat:=null;
  end if;
  r.accepted_at_utc:=transaction_timestamp();r.snapshot_sha256:=sha256(convert_to((to_jsonb(r)-'snapshot_sha256')::text,'UTF8'));
  insert into private.bpay_next_source_comparison_receipt select(r).*;
  if p_operation_kind='RESERVE' then
    m.basis_review_id:=r.basis_review_id;m.hold_id:=op.hold_id;m.run_line_id:=(select run_line_id from private.bpay_next_hold where id=op.hold_id);
    m.native_source_channel:=op.native_channel;m.native_reserved_ex_vat:=op.native_amount;m.comparable_reserved_ex_vat:=q;m.created_by_comparison_id:=r.id;
    m.mapping_sha256:=sha256(convert_to((to_jsonb(m)-'mapping_sha256')::text,'UTF8'));
    insert into private.bpay_next_source_comparison_hold_mapping select(m).*;
  end if;
  if (op.local_work_id,op.local_component_key) is distinct from (op.canonical_work_id,op.canonical_component_key) then
    if localp.source_book_state<>'UNBOUND' then raise exception 'BPAY_NEXT_COMPARISON_LOCAL_CANONICAL_COLLISION';end if;
    update private.bpay_next_position set
      realised_source_ex_vat=(r.local_scalar_after->>'realised_source_ex_vat')::numeric,held_source_ex_vat=(r.local_scalar_after->>'held_source_ex_vat')::numeric,
      realised_target_ex_vat=(r.local_scalar_after->>'realised_target_ex_vat')::numeric,realised_target_vat=(r.local_scalar_after->>'realised_target_vat')::numeric,
      realised_target_inc_vat=(r.local_scalar_after->>'realised_target_inc_vat')::numeric,held_target_ex_vat=(r.local_scalar_after->>'held_target_ex_vat')::numeric,
      held_target_vat=(r.local_scalar_after->>'held_target_vat')::numeric,held_target_inc_vat=(r.local_scalar_after->>'held_target_inc_vat')::numeric,
      updated_at_utc=transaction_timestamp() where work_id=op.local_work_id and component_key=op.local_component_key;
  end if;
  update private.bpay_next_position x set native_realised_paye=r.native_realised_paye,native_realised_umbrella=r.native_realised_umbrella,
    native_held_paye=r.native_held_paye,native_held_umbrella=r.native_held_umbrella,source_book_revision=r.source_book_revision,
    source_comparison_receipt_id=r.id,source_comparison_revision=r.comparison_revision,
    realised_source_ex_vat=case when x.work_id=r.local_work_id then (r.local_scalar_after->>'realised_source_ex_vat')::numeric else x.realised_source_ex_vat end,
    held_source_ex_vat=case when x.work_id=r.local_work_id then (r.local_scalar_after->>'held_source_ex_vat')::numeric else x.held_source_ex_vat end,
    realised_target_ex_vat=case when x.work_id=r.local_work_id then (r.local_scalar_after->>'realised_target_ex_vat')::numeric else x.realised_target_ex_vat end,
    realised_target_vat=case when x.work_id=r.local_work_id then (r.local_scalar_after->>'realised_target_vat')::numeric else x.realised_target_vat end,
    realised_target_inc_vat=case when x.work_id=r.local_work_id then (r.local_scalar_after->>'realised_target_inc_vat')::numeric else x.realised_target_inc_vat end,
    held_target_ex_vat=case when x.work_id=r.local_work_id then (r.local_scalar_after->>'held_target_ex_vat')::numeric else x.held_target_ex_vat end,
    held_target_vat=case when x.work_id=r.local_work_id then (r.local_scalar_after->>'held_target_vat')::numeric else x.held_target_vat end,
    held_target_inc_vat=case when x.work_id=r.local_work_id then (r.local_scalar_after->>'held_target_inc_vat')::numeric else x.held_target_inc_vat end,
    updated_at_utc=transaction_timestamp() where x.work_id=op.canonical_work_id and x.component_key=op.canonical_component_key
      and x.source_book_revision=p_expected_book_revision and x.source_comparison_receipt_id=p_expected_comparison_id;
  if not found then raise exception 'BPAY_NEXT_COMPARISON_CARRY_CAS_STALE';end if;
  return jsonb_build_object('comparison_id',r.id,'comparison_revision',r.comparison_revision,'book_revision',r.source_book_revision,
    'qualification_state',r.qualification_state,'replay',false);
end $function$;

-- Actual 1519 activation/correction. Same snapshot family; ZERO native/book/scalar change.
create or replace function private.bpay_next_source_comparison_case_reconcile_v2(p_job_id uuid,p_expected_book_revision bigint,
  p_expected_comparison_id uuid,p_work_collection_id uuid)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private,public as $function$
declare p private.bpay_next_position%rowtype;oldr private.bpay_next_source_comparison_receipt%rowtype;
  r private.bpay_next_source_comparison_receipt%rowtype;basis private.bpay_next_source_comparison_receipt%rowtype;proof jsonb;
begin
  select * into r from private.bpay_next_source_comparison_receipt where receipt_kind='CASE_RECONCILE'
    and operation_job_id=p_job_id and work_collection_id=p_work_collection_id and previous_comparison_id=p_expected_comparison_id;
  if found then
    if (r.source_book_revision_before,r.previous_comparison_id) is distinct from (p_expected_book_revision,p_expected_comparison_id) then
      raise exception 'BPAY_NEXT_COMPARISON_CASE_RECONCILE_REPLAY_CONFLICT';end if;
    return jsonb_build_object('comparison_id',r.id,'comparison_revision',r.comparison_revision,'book_revision',r.source_book_revision,
      'qualification_state',r.qualification_state,'replay',true);
  end if;
  select * into strict oldr from private.bpay_next_source_comparison_receipt where id=p_expected_comparison_id;
  perform 1 from private.bpay_next_worker_control where candidate_id=oldr.candidate_id for update;
  perform 1 from private.bpay_next_work where id=oldr.canonical_work_id for update;
  select * into strict p from private.bpay_next_position where work_id=oldr.canonical_work_id and component_key=oldr.canonical_component_key for update;
  if p.source_book_revision is distinct from p_expected_book_revision or p.source_comparison_receipt_id is distinct from oldr.id
    or not private.bpay_next_source_comparison_book_certified_v2(p) then raise exception 'BPAY_NEXT_COMPARISON_CASE_RECONCILE_STALE';end if;
  proof:=private.bpay_next_source_comparison_case_reconcile_proof_v2(oldr,p_job_id,p_work_collection_id);
  select * into strict basis from private.bpay_next_source_comparison_receipt where id=oldr.basis_review_id;
  r:=oldr;r.id:=gen_random_uuid();r.receipt_kind:='CASE_RECONCILE';r.previous_comparison_id:=oldr.id;
  r.comparison_revision_before:=p.source_comparison_revision;r.comparison_revision:=p.source_comparison_revision+1;
  r.source_book_revision_before:=p.source_book_revision;r.source_book_revision:=p.source_book_revision;
  r.native_realised_paye_before:=p.native_realised_paye;r.native_realised_umbrella_before:=p.native_realised_umbrella;
  r.native_held_paye_before:=p.native_held_paye;r.native_held_umbrella_before:=p.native_held_umbrella;
  r.comparable_paid_ex_vat_before:=oldr.comparable_paid_ex_vat;r.comparable_held_ex_vat_before:=oldr.comparable_held_ex_vat;
  r.qualification_state:=case when basis.receipt_kind='PENDING_REVIEW' and basis.source_binding_version_id=oldr.source_binding_version_id then 'PENDING' else 'READY' end;
  r.review_issue_code:=case when r.qualification_state='PENDING' then 'SOURCE_PUBLICATION_PENDING' else null end;
  r.operation_job_id:=p_job_id;r.operation_identity:=p_work_collection_id;r.operation_case_event_id:=(proof->>'case_event_id')::uuid;
  r.operation_hold_id:=null;r.operation_effect_id:=null;r.operation_case_instruction_id:=null;r.operation_case_result_id:=null;
  r.operation_case_hold_id:=null;
  r.native_source_channel:=null;r.native_delta_realised:=0;r.native_delta_held:=0;r.comparable_operation_ex_vat:=null;
  r.local_work_id:=null;r.local_component_key:=null;r.local_scalar_before:=null;r.local_scalar_after:=null;
  r.work_collection_id:=p_work_collection_id;r.case_id:=(proof#>>'{case_basis,case_id}')::uuid;r.case_component_id:=(proof#>>'{case_basis,case_component_id}')::uuid;
  r.case_basis_before:=oldr.case_basis;r.case_basis:=proof->'case_basis';r.case_resolution:=proof->'case_resolution';
  r.accepted_at_utc:=transaction_timestamp();r.snapshot_sha256:=sha256(convert_to((to_jsonb(r)-'snapshot_sha256')::text,'UTF8'));
  insert into private.bpay_next_source_comparison_receipt select(r).*;
  update private.bpay_next_position set source_comparison_receipt_id=r.id,source_comparison_revision=r.comparison_revision
    where work_id=p.work_id and component_key=p.component_key and source_book_revision=p_expected_book_revision
      and source_comparison_receipt_id=p_expected_comparison_id;
  if not found then raise exception 'BPAY_NEXT_COMPARISON_CASE_RECONCILE_CAS_STALE';end if;
  return jsonb_build_object('comparison_id',r.id,'comparison_revision',r.comparison_revision,'book_revision',r.source_book_revision,
    'qualification_state',r.qualification_state,'replay',false);
end $function$;

create or replace function private.bpay_next_source_comparison_case_capacity_carry_v2(p_job_id uuid,p_expected_book_revision bigint,
  p_expected_comparison_id uuid,p_case_hold_id uuid,p_kind text)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private,public as $function$
declare p private.bpay_next_position%rowtype;oldr private.bpay_next_source_comparison_receipt%rowtype;
  r private.bpay_next_source_comparison_receipt%rowtype;proof jsonb;
begin
  if p_kind not in ('CASE_RESERVE','CASE_RELEASE') then raise exception 'BPAY_NEXT_COMPARISON_CASE_CAPACITY_KIND_INVALID';end if;
  select * into r from private.bpay_next_source_comparison_receipt where receipt_kind=p_kind and operation_identity=p_case_hold_id;
  if found then
    if (r.operation_job_id,r.previous_comparison_id,r.source_book_revision_before)
      is distinct from (p_job_id,p_expected_comparison_id,p_expected_book_revision) then raise exception 'BPAY_NEXT_COMPARISON_CASE_CAPACITY_REPLAY_CONFLICT';end if;
    return jsonb_build_object('comparison_id',r.id,'comparison_revision',r.comparison_revision,'book_revision',r.source_book_revision,
      'qualification_state',r.qualification_state,'replay',true);
  end if;
  select * into strict oldr from private.bpay_next_source_comparison_receipt where id=p_expected_comparison_id;
  perform 1 from private.bpay_next_worker_control where candidate_id=oldr.candidate_id for update;
  perform 1 from private.bpay_next_work where id=oldr.canonical_work_id for update;
  select * into strict p from private.bpay_next_position where work_id=oldr.canonical_work_id and component_key=oldr.canonical_component_key for update;
  if p.source_book_revision is distinct from p_expected_book_revision or p.source_comparison_receipt_id is distinct from oldr.id
    or not private.bpay_next_source_comparison_book_certified_v2(p) then raise exception 'BPAY_NEXT_COMPARISON_CASE_CAPACITY_STALE';end if;
  proof:=private.bpay_next_source_comparison_case_capacity_proof_v2(oldr,p_job_id,p_case_hold_id,p_kind);
  r:=oldr;r.id:=gen_random_uuid();r.receipt_kind:=p_kind;r.previous_comparison_id:=oldr.id;
  r.comparison_revision_before:=p.source_comparison_revision;r.comparison_revision:=p.source_comparison_revision+1;
  r.source_book_revision_before:=p.source_book_revision;r.native_realised_paye_before:=p.native_realised_paye;r.native_realised_umbrella_before:=p.native_realised_umbrella;
  r.native_held_paye_before:=p.native_held_paye;r.native_held_umbrella_before:=p.native_held_umbrella;
  r.comparable_paid_ex_vat_before:=oldr.comparable_paid_ex_vat;r.comparable_held_ex_vat_before:=oldr.comparable_held_ex_vat;
  r.qualification_state:=proof->>'qualification_state';r.review_issue_code:=proof->>'review_issue_code';
  r.case_basis_before:=oldr.case_basis;r.case_basis:=proof->'case_basis';r.case_resolution:=proof->'case_resolution';
  r.operation_job_id:=p_job_id;r.operation_identity:=p_case_hold_id;r.operation_case_hold_id:=p_case_hold_id;
  r.operation_case_instruction_id:=(proof->>'instruction_id')::uuid;r.operation_case_result_id:=(proof->>'allocation_result_id')::uuid;
  r.native_source_channel:=proof->>'native_source_channel';r.comparable_operation_ex_vat:=(proof->>'comparable_operation_ex_vat')::numeric;
  r.operation_hold_id:=null;r.operation_effect_id:=null;r.operation_case_event_id:=null;r.native_delta_realised:=0;r.native_delta_held:=0;
  r.local_work_id:=null;r.local_component_key:=null;r.local_scalar_before:=null;r.local_scalar_after:=null;
  r.accepted_at_utc:=transaction_timestamp();r.snapshot_sha256:=sha256(convert_to((to_jsonb(r)-'snapshot_sha256')::text,'UTF8'));
  insert into private.bpay_next_source_comparison_receipt select(r).*;
  update private.bpay_next_position set source_comparison_receipt_id=r.id,source_comparison_revision=r.comparison_revision
    where work_id=p.work_id and component_key=p.component_key and source_book_revision=p_expected_book_revision and source_comparison_receipt_id=p_expected_comparison_id;
  if not found then raise exception 'BPAY_NEXT_COMPARISON_CASE_CAPACITY_CAS_STALE';end if;
  return jsonb_build_object('comparison_id',r.id,'comparison_revision',r.comparison_revision,'book_revision',r.source_book_revision,
    'qualification_state',r.qualification_state,'replay',false);
end $function$;

-- Invoked by the narrow 1246 BEFORE-trigger branch, not a GUC waiver.
create or replace function private.bpay_next_source_comparison_assert_book_update_v2(
  p_old private.bpay_next_position,p_new private.bpay_next_position)
returns void language plpgsql security invoker set search_path=pg_catalog,private as $function$
declare r private.bpay_next_source_comparison_receipt%rowtype;expected_scalar jsonb;op record;
begin
  select * into strict r from private.bpay_next_source_comparison_receipt where id=p_new.source_comparison_receipt_id;
  if p_old.source_book_state<>'READY' or p_new.source_book_state<>'READY'
    or r.snapshot_sha256 is distinct from sha256(convert_to((to_jsonb(r)-'snapshot_sha256')::text,'UTF8'))
    or (r.canonical_work_id,r.canonical_component_key,r.previous_comparison_id,r.comparison_revision_before,r.source_book_revision_before,
        r.native_realised_paye_before,r.native_realised_umbrella_before,r.native_held_paye_before,r.native_held_umbrella_before)
      is distinct from (p_old.work_id,p_old.component_key,p_old.source_comparison_receipt_id,p_old.source_comparison_revision,p_old.source_book_revision,
        p_old.native_realised_paye,p_old.native_realised_umbrella,p_old.native_held_paye,p_old.native_held_umbrella)
    or (r.comparison_revision,r.source_book_revision,r.native_realised_paye,r.native_realised_umbrella,r.native_held_paye,r.native_held_umbrella)
      is distinct from (p_new.source_comparison_revision,p_new.source_book_revision,p_new.native_realised_paye,p_new.native_realised_umbrella,
        p_new.native_held_paye,p_new.native_held_umbrella)
    or (to_jsonb(p_new)-array['source_comparison_receipt_id','source_comparison_revision','source_book_revision','native_realised_paye',
      'native_realised_umbrella','native_held_paye','native_held_umbrella','realised_source_ex_vat','held_source_ex_vat',
      'realised_target_ex_vat','realised_target_vat','realised_target_inc_vat','held_target_ex_vat','held_target_vat','held_target_inc_vat','updated_at_utc'])
      is distinct from (to_jsonb(p_old)-array['source_comparison_receipt_id','source_comparison_revision','source_book_revision','native_realised_paye',
      'native_realised_umbrella','native_held_paye','native_held_umbrella','realised_source_ex_vat','held_source_ex_vat',
      'realised_target_ex_vat','realised_target_vat','realised_target_inc_vat','held_target_ex_vat','held_target_vat','held_target_inc_vat','updated_at_utc']) then
    raise exception 'BPAY_NEXT_COMPARISON_BOOK_CERTIFICATE_INVALID';end if;
  expected_scalar:=private.bpay_next_source_comparison_scalar_v2(p_old);
  if r.receipt_kind in ('CASE_RESERVE','CASE_RELEASE') then
    perform private.bpay_next_source_comparison_case_capacity_proof_v2(
      (select x from private.bpay_next_source_comparison_receipt x where x.id=r.previous_comparison_id),r.operation_job_id,r.operation_case_hold_id,r.receipt_kind);
  elsif r.receipt_kind='CASE_RECONCILE' then
    perform private.bpay_next_source_comparison_case_reconcile_proof_v2(
      (select x from private.bpay_next_source_comparison_receipt x where x.id=r.previous_comparison_id),r.operation_job_id,r.work_collection_id);
  elsif r.receipt_kind not in ('REVIEW','PENDING_REVIEW') then
    select * into strict op from private.bpay_next_source_comparison_operation_v2(r.operation_job_id,r.receipt_kind,r.operation_identity);
    if (op.canonical_work_id,op.canonical_component_key) is distinct from (p_old.work_id,p_old.component_key) then
      raise exception 'BPAY_NEXT_COMPARISON_BOOK_OPERATION_SCOPE_INVALID';end if;
    if r.local_work_id=p_old.work_id then
      if r.local_scalar_before is distinct from expected_scalar then raise exception 'BPAY_NEXT_COMPARISON_BOOK_SCALAR_BEFORE_INVALID';end if;
      expected_scalar:=r.local_scalar_after;
    end if;
  end if;
  if private.bpay_next_source_comparison_scalar_v2(p_new) is distinct from expected_scalar then
    raise exception 'BPAY_NEXT_COMPARISON_BOOK_SCALAR_AFTER_INVALID';end if;
end $function$;

create or replace function private.bpay_next_source_comparison_current_v2(p_binding_version_id uuid,p_expected_book_revision bigint,p_expected_comparison_id uuid default null)
returns private.bpay_next_source_comparison_receipt language plpgsql security invoker set search_path=pg_catalog,private as $function$
declare book record;p private.bpay_next_position%rowtype;r private.bpay_next_source_comparison_receipt%rowtype;
begin
  select * into strict book from private.bpay_next_bound_source_snapshot_v1(p_binding_version_id,p_expected_book_revision);
  if book.qualification_state<>'READY' then raise exception 'BPAY_NEXT_COMPARISON_BOUND_SOURCE_NOT_READY';end if;
  select * into strict p from private.bpay_next_position where work_id=book.canonical_work_id and component_key=book.canonical_component_key;
  select * into strict r from private.bpay_next_source_comparison_receipt where id=p.source_comparison_receipt_id;
  if not private.bpay_next_source_comparison_book_certified_v2(p) or r.qualification_state<>'READY'
    or r.source_binding_version_id<>p_binding_version_id or r.approved_source_ex_vat<>book.approved_source_ex_vat
    or r.approval_work_id<>book.approval_work_id or r.approval_revision_id<>book.approval_revision_id
    or (p_expected_comparison_id is not null and r.id<>p_expected_comparison_id) then
    raise exception 'BPAY_NEXT_COMPARISON_CURRENT_REVIEW_REQUIRED';end if;
  if r.case_basis is distinct from private.bpay_next_source_comparison_case_basis_v2(p.work_id,p.component_key,false) then
    raise exception 'BPAY_NEXT_COMPARISON_CASE_CURRENT_REVIEW_REQUIRED';end if;
  return r;
end $function$;

create or replace function private.bpay_next_source_comparison_collection_origin_v2(p_comparison_id uuid,p_candidate_id uuid)
returns private.bpay_next_work_collection language plpgsql stable security invoker set search_path=pg_catalog,private as $function$
declare r private.bpay_next_source_comparison_receipt%rowtype;p private.bpay_next_position%rowtype;c private.bpay_next_work_collection%rowtype;
begin
  select * into strict r from private.bpay_next_source_comparison_receipt where id=p_comparison_id and candidate_id=p_candidate_id;
  select * into strict p from private.bpay_next_position where work_id=r.canonical_work_id and component_key=r.canonical_component_key;
  select * into strict c from private.bpay_next_work_collection where work_id=r.canonical_work_id and component_key=r.canonical_component_key and candidate_id=p_candidate_id;
  if r.qualification_state<>'READY' or p.source_comparison_receipt_id<>r.id or not private.bpay_next_source_comparison_book_certified_v2(p)
    or c.case_id is null or (c.id,c.case_id,c.case_component_id) is distinct from (r.work_collection_id,r.case_id,r.case_component_id)
    or r.case_basis is distinct from private.bpay_next_source_comparison_case_basis_snapshot_v2(p.work_id,p.component_key) then
    raise exception 'BPAY_NEXT_COMPARISON_CASE_CURRENT_REVIEW_REQUIRED';end if;
  if not c.paid_basis_qualified or c.qualification_state<>'READY' or c.original_tax_treatment is distinct from 'TAXABLE'
    or (c.observed_revision_id,c.observed_approved_source_ex_vat,c.observed_realised_source_ex_vat)
      is distinct from (p.applied_revision_id,p.approved_source_ex_vat,p.realised_source_ex_vat)
    or not exists(select 1 from private.bpay_next_financial_effect e
      join private.bpay_next_run_line l on l.id=c.original_run_line_id and l.work_id=c.work_id and l.component_key=c.component_key
      join private.bpay_next_run_worker candidate_worker on candidate_worker.id=l.run_worker_id and candidate_worker.candidate_id=c.candidate_id
      join private.bpay_next_work_revision original on original.id=c.original_revision_id and original.work_id=c.work_id and original.sealed_at_utc is not null
      join private.bpay_next_approved_line a on a.id=c.original_approved_line_id and a.revision_id=original.id and a.component_key=c.component_key
      join private.bpay_next_hold h on h.id=e.operation_item_id and h.run_line_id=l.id and h.status='REALISED'
      join private.bpay_next_transfer t on t.id=c.original_transfer_id
        and (t.status in ('SETTLED','RETURNED') or (t.execution_kind='INTERNAL_ZERO' and t.status='INTERNAL_PROCESSING'))
        and t.original_transfer_id is null and t.return_cash_id is null
      join private.bpay_next_transfer_member m on m.transfer_id=t.id and m.subject_kind='WORK' and m.run_line_id=l.id
      join private.bpay_next_work w on w.id=c.work_id and w.candidate_id=c.candidate_id
      where e.id=c.original_effect_id and e.effect_kind='PAYROLL_SETTLED' and e.candidate_id=c.candidate_id
        and (e.work_id,e.component_key,e.original_transfer_id,e.original_timesheet_id)
          =(c.work_id,c.component_key,t.id,w.original_timesheet_id)
        and (l.captured_revision_id,l.approved_line_id,l.source_pay_channel,l.valuation_policy_id)
          =(original.id,a.id,c.original_source_pay_channel,c.original_valuation_policy_id)
        and c.original_source_pay_channel in ('PAYE','UMBRELLA') and original.source_pay_channel=c.original_source_pay_channel
        and a.component_kind='WORK' and a.tax_treatment='TAXABLE'
        and (e.source_disposition_ex_vat,e.target_amount_ex_vat,e.target_amount_vat,e.target_amount_inc_vat)
          =(h.source_reserved_ex_vat,l.frozen_ex_vat,l.frozen_vat,l.frozen_inc_vat)
        and h.source_reserved_ex_vat=l.source_consumed_ex_vat and m.signed_cash_contribution=l.frozen_inc_vat
        and ((w.work_kind='ORDINARY' and original.source_kind='ORDINARY')
          or (w.work_kind='SOURCE' and a.source_component_id is not null and a.component_key='SOURCE:'||a.source_component_id::text
            and original.source_kind in ('SOURCE','PROTECTED') and original.source_event_id is not null
            and (original.source_head_id is null or original.source_event_id=original.source_head_id)))) then
    raise exception 'BPAY_NEXT_COMPARISON_COLLECTION_ORIGINAL_PAID_PROOF_INVALID';end if;
  return c;
end $function$;

-- COMPONENTS read: same pinned publication/book gates through genuine lock-free sibling.
-- Recognized pending/review/stale comparison never falls back to destination local zero.
create or replace function private.bpay_next_source_comparison_component_display_v2(p_candidate_id uuid,p_work_id uuid,
  p_revision_id uuid,p_approved_line_id uuid)
returns table(realised_source_ex_vat numeric,held_source_ex_vat numeric,residual_source_ex_vat numeric,position_ready boolean)
language plpgsql stable security invoker set search_path=pg_catalog,private as $function$
declare a private.bpay_next_approved_line%rowtype;localp private.bpay_next_position%rowtype;p private.bpay_next_position%rowtype;
  b private.bpay_next_source_component_binding%rowtype;r private.bpay_next_source_comparison_receipt%rowtype;book record;issue text;
begin
  select x.* into strict a from private.bpay_next_approved_line x join private.bpay_next_work_revision rev on rev.id=x.revision_id
    join private.bpay_next_work w on w.id=rev.work_id and w.candidate_id=p_candidate_id
    where x.id=p_approved_line_id and rev.id=p_revision_id and w.id=p_work_id;
  select * into localp from private.bpay_next_position where work_id=p_work_id and component_key=a.component_key;
  if a.source_component_id is not null then
    select * into b from private.bpay_next_source_component_binding where candidate_id=p_candidate_id and source_component_id=a.source_component_id;
    if found then select * into p from private.bpay_next_position where work_id=b.canonical_work_id and component_key=b.canonical_component_key;end if;
  end if;
  if p.source_comparison_receipt_id is null then
    realised_source_ex_vat:=localp.realised_source_ex_vat;held_source_ex_vat:=localp.held_source_ex_vat;
    residual_source_ex_vat:=a.source_pay_ex_vat-localp.realised_source_ex_vat-localp.held_source_ex_vat;
    position_ready:=coalesce(localp.applied_revision_id=p_revision_id and localp.approved_source_ex_vat=a.source_pay_ex_vat,false);
    return next;return;
  end if;
  position_ready:=false;
  select * into strict r from private.bpay_next_source_comparison_receipt where id=p.source_comparison_receipt_id;
  if r.qualification_state<>'READY' or not private.bpay_next_source_comparison_book_certified_v2(p)
    or (r.candidate_id,r.approval_work_id,r.approval_revision_id,r.approved_line_id,r.approval_component_key,r.source_binding_version_id)
      is distinct from (p_candidate_id,p_work_id,p_revision_id,p_approved_line_id,a.component_key,b.current_binding_version_id)
    or r.case_basis is distinct from private.bpay_next_source_comparison_case_basis_snapshot_v2(p.work_id,p.component_key) then
    return next;return;end if;
  select * into strict book from private.bpay_next_bound_source_snapshot_read_v2(r.source_binding_version_id,p.source_book_revision);
  if book.qualification_state<>'READY' or book.approved_source_ex_vat<>r.approved_source_ex_vat then return next;return;end if;
  realised_source_ex_vat:=r.comparable_paid_ex_vat;held_source_ex_vat:=r.comparable_held_ex_vat;
  residual_source_ex_vat:=r.selected_residual_ex_vat;position_ready:=true;return next;
exception when sqlstate '55000' then
  get stacked diagnostics issue=message_text;
  if issue not in ('BPAY_NEXT_BINDING_NEXT_REQUIRED','BPAY_NEXT_SOURCE_BOOK_CONTROL_REQUIRED',
    'BPAY_NEXT_BINDING_RECEIPT_NOT_QUALIFIED','BPAY_NEXT_BINDING_MEMBER_NOT_CURRENT','BPAY_NEXT_INITIAL_AUTH_NOT_CURRENT',
    'BPAY_NEXT_INITIAL_I1_CERTIFICATE_REQUIRED') then raise;end if;
  realised_source_ex_vat:=null;held_source_ex_vat:=null;residual_source_ex_vat:=null;position_ready:=false;return next;
end $function$;

-- All helper entry points are private, deliberately not service-role RPCs.
do $private_helpers$ declare f regprocedure;begin
  for f in select p.oid::regprocedure from pg_proc p join pg_namespace n on n.oid=p.pronamespace
    where n.nspname='private' and p.proname in ('bpay_next_source_comparison_scalar_v2','bpay_next_source_comparison_scalar_delta_v2',
      'bpay_next_source_comparison_operation_v2','bpay_next_source_comparison_book_certified_v2','bpay_next_source_comparison_guard_v2',
      'bpay_next_source_comparison_run_line_frozen_guard_v2',
      'bpay_next_source_comparison_case_basis_v2',
      'bpay_next_source_comparison_case_basis_snapshot_v2','bpay_next_source_comparison_case_reconcile_proof_v2',
      'bpay_next_source_comparison_case_reconcile_v2','bpay_next_source_comparison_collection_origin_v2',
      'bpay_next_source_comparison_case_capacity_proof_v2','bpay_next_source_comparison_case_capacity_carry_v2',
      'bpay_next_source_comparison_component_display_v2',
      'bpay_next_source_comparison_case_remainder_v2',
      'bpay_next_source_comparison_hold_guard_v2','bpay_next_source_comparison_case_hold_guard_v2','bpay_next_source_comparison_register_core_v2','bpay_next_source_comparison_register_pending_core_v2',
      'bpay_next_source_comparison_register_impl_v2','bpay_next_source_comparison_carry_v2',
      'bpay_next_source_comparison_assert_book_update_v2','bpay_next_source_comparison_current_v2') loop
    execute format('alter function %s owner to %I',f,current_user);execute format('revoke all on function %s from public,anon,authenticated,service_role',f);
  end loop;
end $private_helpers$;
commit;
