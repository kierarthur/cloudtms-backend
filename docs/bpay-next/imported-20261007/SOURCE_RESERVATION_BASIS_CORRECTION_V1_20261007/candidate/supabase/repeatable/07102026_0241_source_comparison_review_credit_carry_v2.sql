-- Exact four-function successor; all other frozen bodies unchanged.
\set ON_ERROR_STOP on
begin;

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
  -- Actual known Case release cannot qualify a still-unknown W*/O*.
  if p_input.qualification_state='REVIEW'
    and p_input.review_issue_code='CASE_WRITEOFF_COMPARISON_REVIEW_REQUIRED' then
    if p_kind<>'CASE_RELEASE' then raise exception 'BPAY_NEXT_REVIEW_CREDIT_DENIES_CASE_RESERVE';end if;
    if p_input.case_resolution->>'target_written_off_ex_vat' is not null
      or p_input.case_resolution->>'target_outstanding_ex_vat' is not null then
      raise exception 'BPAY_NEXT_REVIEW_CREDIT_CASE_WRITE_OFF_SHAPE_INVALID';end if;
    js:=case when qualified and r->>'target_active_capacity_ex_vat' is not null
      then (r->>'target_active_capacity_ex_vat')::numeric-compq else null end;
    if js<0 then js:=null;qualified:=false;end if;
    r:=r||jsonb_build_object('source_outstanding_ex_vat',null,'source_principal_ex_vat',null,
      'source_written_off_ex_vat',actual->'W','target_written_off_ex_vat',null,'target_outstanding_ex_vat',null,
      'target_active_capacity_ex_vat',js,'selected_quote',null,'written_off_quote',null,
      'qualification_issue','CASE_WRITEOFF_COMPARISON_REVIEW_REQUIRED',
      'review_credit_capacity_proof',jsonb_build_object('input_comparison_id',p_input.id,'job_id',j.id,
        'case_hold_id',h.id,'actual_source_capacity_delta',-h.source_reserved_ex_vat,
        'comparable_capacity_delta',case when qualified then -compq else null end,
        'before_actual',p_input.case_basis,'after_actual',actual,'case_qualification_state','REVIEW'));
    return jsonb_build_object('case_basis',actual,'case_resolution',r,'qualification_state','REVIEW',
      'review_issue_code','CASE_WRITEOFF_COMPARISON_REVIEW_REQUIRED',
      'comparable_operation_ex_vat',case when qualified then compq else null end,
      'instruction_id',i.id,'allocation_result_id',h.allocation_result_id,'native_source_channel',i.target_pay_channel);
  end if;

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
  if new.receipt_kind='CASE_WRITEOFF' then
    proof:=private.bpay_next_source_comparison_writeoff_proof_v2(oldr,new.operation_job_id,new.operation_case_event_id);
    if new.operation_identity is distinct from new.operation_case_event_id
      or (new.qualification_state,new.review_issue_code,new.case_basis_before,new.case_basis,new.case_resolution,
          new.comparable_operation_ex_vat,new.actor_user_id,new.reason)
        is distinct from (proof->>'qualification_state',proof->>'review_issue_code',oldr.case_basis,proof->'case_basis',proof->'case_resolution',
          (proof->>'comparable_operation_ex_vat')::numeric,(proof->>'actor_user_id')::uuid,proof->>'reason')
      or (new.work_collection_id::text,new.case_id::text,new.case_component_id::text)
        is distinct from (proof#>>'{case_basis,collection_id}',proof#>>'{case_basis,case_id}',proof#>>'{case_basis,case_component_id}')
      or (to_jsonb(new)-array['id','receipt_kind','qualification_state','review_issue_code','previous_comparison_id','comparison_revision_before',
        'comparison_revision','source_book_revision_before','native_realised_paye_before','native_realised_umbrella_before','native_held_paye_before',
        'native_held_umbrella_before','comparable_paid_ex_vat_before','comparable_held_ex_vat_before','case_basis_before','case_basis','case_resolution',
        'work_collection_id','case_id','case_component_id','operation_job_id','operation_identity','operation_case_event_id','native_delta_realised',
        'native_delta_held','native_source_channel','comparable_operation_ex_vat','local_work_id','local_component_key','local_scalar_before','local_scalar_after',
        'operation_effect_id','operation_hold_id','operation_case_instruction_id','operation_case_result_id','operation_case_hold_id',
        'actor_user_id','reason','accepted_at_utc','snapshot_sha256'])
      is distinct from (to_jsonb(oldr)-array['id','receipt_kind','qualification_state','review_issue_code','previous_comparison_id','comparison_revision_before',
        'comparison_revision','source_book_revision_before','native_realised_paye_before','native_realised_umbrella_before','native_held_paye_before',
        'native_held_umbrella_before','comparable_paid_ex_vat_before','comparable_held_ex_vat_before','case_basis_before','case_basis','case_resolution',
        'work_collection_id','case_id','case_component_id','operation_job_id','operation_identity','operation_case_event_id','native_delta_realised',
        'native_delta_held','native_source_channel','comparable_operation_ex_vat','local_work_id','local_component_key','local_scalar_before','local_scalar_after',
        'operation_effect_id','operation_hold_id','operation_case_instruction_id','operation_case_result_id','operation_case_hold_id',
        'actor_user_id','reason','accepted_at_utc','snapshot_sha256']) then
      raise exception 'BPAY_NEXT_COMPARISON_WRITEOFF_CERTIFICATE_INVALID' using errcode='23514';end if;
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
  if op.updates_local_scalar and localp.source_basis_channel is distinct from op.native_channel then
    raise exception 'BPAY_NEXT_COMPARISON_LOCAL_NATIVE_BASIS_UNQUALIFIED';end if;
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
  if oldr.qualification_state='REVIEW'
    and oldr.review_issue_code='CASE_WRITEOFF_COMPARISON_REVIEW_REQUIRED' then
    proof:=private.bpay_next_source_comparison_review_credit_operation_proof_v2(
      oldr,new.operation_job_id,new.receipt_kind,new.operation_identity);
    if proof is not null then
      q:=(proof->>'comparable_operation_ex_vat')::numeric;
      if (new.qualification_state,new.review_issue_code,new.comparable_operation_ex_vat)
          is distinct from ('REVIEW','CASE_WRITEOFF_COMPARISON_REVIEW_REQUIRED',q)
        or (new.case_basis_before,new.case_basis,new.case_resolution)
          is distinct from (oldr.case_basis,proof->'case_basis',proof->'case_resolution')
        or (new.work_collection_id::text,new.case_id::text,new.case_component_id::text)
          is distinct from (proof#>>'{case_basis,collection_id}',proof#>>'{case_basis,case_id}',proof#>>'{case_basis,case_component_id}')
        or (new.comparable_paid_ex_vat,new.comparable_held_ex_vat) is distinct from
          (oldr.comparable_paid_ex_vat+case new.receipt_kind when 'SETTLE' then q when 'CASE_RECOVERY' then -q else 0 end,
           oldr.comparable_held_ex_vat-case when new.receipt_kind in ('SETTLE','RELEASE') then q else 0 end)
        or new.selected_residual_ex_vat is distinct from new.approved_source_ex_vat-new.comparable_paid_ex_vat-new.comparable_held_ex_vat then
        raise exception 'BPAY_NEXT_REVIEW_CREDIT_OPERATION_CERTIFICATE_INVALID';end if;
      return new;
    end if;
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

create or replace function private.bpay_next_source_comparison_carry_v2(p_job_id uuid,p_expected_book_revision bigint,
  p_expected_comparison_id uuid,p_operation_kind text,p_operation_id uuid)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private,public as $function$
declare op record;p private.bpay_next_position%rowtype;localp private.bpay_next_position%rowtype;
  r private.bpay_next_source_comparison_receipt%rowtype;oldr private.bpay_next_source_comparison_receipt%rowtype;
  m private.bpay_next_source_comparison_hold_mapping%rowtype;dr numeric;dh numeric;q numeric;qualified boolean;
  cm private.bpay_next_source_comparison_case_hold_mapping%rowtype;
  actual_case jsonb;case_remainder jsonb;credit_qualified boolean;case_qualified boolean:=true;review_credit_proof jsonb;review_credit boolean:=false;
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
  review_credit_proof:=private.bpay_next_source_comparison_review_credit_operation_proof_v2(oldr,p_job_id,p_operation_kind,p_operation_id);
  review_credit:=review_credit_proof is not null;
  credit_qualified:=review_credit or (qualified and (oldr.qualification_state in ('READY','PENDING') or oldr.review_issue_code='CASE_RECONCILIATION_REQUIRED'));
  actual_case:=private.bpay_next_source_comparison_case_basis_v2(op.canonical_work_id,op.canonical_component_key,true);
  r.case_basis_before:=oldr.case_basis;r.case_basis:=actual_case;
  r.work_collection_id:=(actual_case->>'collection_id')::uuid;r.case_id:=(actual_case->>'case_id')::uuid;r.case_component_id:=(actual_case->>'case_component_id')::uuid;
  if review_credit then
    q:=(review_credit_proof->>'comparable_operation_ex_vat')::numeric;
    if actual_case is distinct from review_credit_proof->'case_basis' then raise exception 'BPAY_NEXT_REVIEW_CREDIT_ACTUAL_CASE_STALE';end if;
    r.case_resolution:=review_credit_proof->'case_resolution';case_qualified:=false;
  elsif oldr.case_basis is not null then
    case_remainder:=private.bpay_next_source_comparison_case_remainder_v2(oldr,p_operation_kind,op.case_event_id,q,
      oldr.comparable_paid_ex_vat+case p_operation_kind when 'SETTLE' then q when 'CASE_RECOVERY' then -q else 0 end,actual_case);
    case_qualified:=case_remainder is not null;r.case_resolution:=coalesce(case_remainder,oldr.case_resolution);
  elsif actual_case is not null then
    case_qualified:=false;r.case_resolution:=jsonb_build_object('qualification_issue','CASE_OWNER_CHANGED_WITHOUT_ACCEPTED_BASIS');
  end if;
  if p_operation_kind='RESERVE' and oldr.qualification_state<>'READY' then raise exception 'BPAY_NEXT_COMPARISON_PENDING_DENIES_NEW_RESERVE';end if;
  if credit_qualified then
    r.qualification_state:=case when case_qualified then oldr.qualification_state else 'REVIEW' end;
    r.review_issue_code:=case when review_credit then 'CASE_WRITEOFF_COMPARISON_REVIEW_REQUIRED'
      when not case_qualified or oldr.review_issue_code='CASE_RECONCILIATION_REQUIRED' then 'CASE_RECONCILIATION_REQUIRED'
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

create or replace function private.bpay_next_reconcile_source_comparison_collection_v2(
 p_job_id uuid,p_canonical_work_id uuid,p_component_key text,p_effect_id uuid default null)
returns void language plpgsql security invoker set search_path=pg_catalog,private,public as $function$
declare j private.bpay_next_job%rowtype;wc private.bpay_next_worker_control%rowtype;
 p private.bpay_next_position%rowtype;r private.bpay_next_source_comparison_receipt%rowtype;
 b private.bpay_next_work_collection%rowtype;v private.bpay_next_source_component_binding_version%rowtype;
 c private.bpay_next_finance_case%rowtype;x private.bpay_next_case_component%rowtype;
 principal numeric;delta numeric;ev text;s numeric;o numeric;actual jsonb;
begin
 select * into strict j from private.bpay_next_job where id=p_job_id;
 select * into strict wc from private.bpay_next_worker_control where candidate_id=j.candidate_id for update;
 perform 1 from private.bpay_next_work where id=p_canonical_work_id and candidate_id=j.candidate_id for update;
 select * into strict p from private.bpay_next_position where work_id=p_canonical_work_id and component_key=p_component_key for update;
 select * into strict r from private.bpay_next_source_comparison_receipt where id=p.source_comparison_receipt_id;
 if j.status<>'LEASED' or j.owner_epoch<>wc.active_owner_epoch or j.lease_until_utc<=clock_timestamp()
   or not exists(select 1 from private.bpay_next_module_control m where m.id=1 and m.active_owner='NEXT' and m.owner_epoch=j.module_epoch)
   or exists(select 1 from private.bpay_next_job prior where prior.candidate_id=j.candidate_id and prior.command_sequence<j.command_sequence and prior.status<>'DONE')
   or r.candidate_id<>j.candidate_id or not private.bpay_next_source_comparison_book_certified_v2(p) then
  raise exception 'BPAY_NEXT_COMPARISON_COLLECTION_OWNER_STALE' using errcode='55000';end if;
 if r.receipt_kind='CASE_RECONCILE' and r.operation_job_id=j.id and
   ((j.job_kind='POSITION_APPLY' and p_effect_id is null and j.phase in ('NEW','REMOVED')) or
    (j.job_kind in ('CSV_SETTLEMENT','INTERNAL_SETTLEMENT') and j.phase='MEMBERS' and p_effect_id is not null
      and exists(select 1 from private.bpay_next_source_comparison_receipt actual_operation
        where actual_operation.id=r.previous_comparison_id and actual_operation.operation_effect_id=p_effect_id
          and actual_operation.operation_job_id=j.id and actual_operation.receipt_kind in ('SETTLE','CASE_RECOVERY')))) then
  return;
 end if;
 -- Do not roll back genuine money or automatically rebase Case P while W* is unqualified.
 if r.qualification_state='REVIEW' and r.review_issue_code='CASE_WRITEOFF_COMPARISON_REVIEW_REQUIRED'
   and r.operation_job_id=j.id and r.comparable_paid_ex_vat is not null and r.comparable_held_ex_vat is not null then
   if (j.job_kind in ('CSV_SETTLEMENT','INTERNAL_SETTLEMENT') and j.phase='MEMBERS'
       and p_effect_id is not null and r.operation_effect_id=p_effect_id and r.receipt_kind in ('SETTLE','CASE_RECOVERY'))
     or (j.job_kind in ('SIMPLE_CANCEL','PREPARATION_EXPIRY') and j.phase='RELEASE'
       and p_effect_id is null and r.receipt_kind='RELEASE' and r.operation_hold_id is not null) then
     actual:=private.bpay_next_source_comparison_review_credit_operation_proof_v2(
       (select input from private.bpay_next_source_comparison_receipt input where input.id=r.previous_comparison_id),
       j.id,r.receipt_kind,r.operation_identity);
     if actual is null or (r.case_basis,r.case_resolution,r.comparable_operation_ex_vat)
       is distinct from (actual->'case_basis',actual->'case_resolution',(actual->>'comparable_operation_ex_vat')::numeric) then
       raise exception 'BPAY_NEXT_REVIEW_CREDIT_COLLECTION_OPERATION_INVALID';end if;
     return;
   end if;
 end if;
 if j.job_kind='POSITION_APPLY' then
  if p_effect_id is not null or j.phase not in ('NEW','REMOVED') or not exists(
    select 1 from private.bpay_next_publication publication where publication.command_id=j.command_id
      and publication.candidate_id=j.candidate_id and publication.work_id=p_canonical_work_id
      and publication.revision_id=p.applied_revision_id and publication.status in ('QUEUED','APPLYING')) then
    raise exception 'BPAY_NEXT_COMPARISON_COLLECTION_PUBLICATION_SCOPE_INVALID';end if;
 elsif j.job_kind in ('CSV_SETTLEMENT','INTERNAL_SETTLEMENT') then
  if p_effect_id is null or j.phase<>'MEMBERS' or r.operation_job_id is distinct from j.id or r.operation_effect_id is distinct from p_effect_id
    or r.receipt_kind not in ('SETTLE','CASE_RECOVERY') then raise exception 'BPAY_NEXT_COMPARISON_COLLECTION_EFFECT_SCOPE_INVALID';end if;
 elsif j.job_kind in ('SIMPLE_CANCEL','PREPARATION_EXPIRY') then
  -- Exact hold carry has already certified the physical release. No hold/history scan.
  if p_effect_id is not null or j.phase<>'RELEASE' or r.receipt_kind<>'CASE_RELEASE'
    or r.operation_job_id is distinct from j.id or r.operation_case_hold_id is null
    or r.review_issue_code is distinct from 'CASE_RECONCILIATION_REQUIRED' then
    raise exception 'BPAY_NEXT_COMPARISON_COLLECTION_CASE_CAPACITY_SCOPE_INVALID';end if;
  perform private.bpay_next_source_comparison_case_capacity_proof_v2(
    (select input from private.bpay_next_source_comparison_receipt input where input.id=r.previous_comparison_id),
    j.id,r.operation_case_hold_id,'CASE_RELEASE');
 else raise exception 'BPAY_NEXT_COMPARISON_COLLECTION_JOB_KIND_INVALID';
 end if;
 if r.comparable_paid_ex_vat is null or r.comparable_held_ex_vat is null then
  -- Truthful actual native posting remains recorded; missing comparison authority
  -- cannot be made READY by a collection calculation.
  return;
 end if;
 if not (r.qualification_state in ('READY','PENDING') or r.review_issue_code='CASE_RECONCILIATION_REQUIRED') then
  raise exception 'BPAY_NEXT_COMPARISON_COLLECTION_CURRENT_REVIEW_REQUIRED' using errcode='55000';end if;
 select * into strict v from private.bpay_next_source_component_binding_version where id=r.source_binding_version_id;
 if not exists(select 1 from private.bpay_next_source_component_binding current where current.current_binding_version_id=v.id
   and current.canonical_work_id=p.work_id and current.canonical_component_key=p.component_key)
   or (v.approval_work_id,v.approval_revision_id,v.approval_component_key,v.approved_line_id,v.candidate_id)
     is distinct from (r.approval_work_id,r.approval_revision_id,r.approval_component_key,r.approved_line_id,r.candidate_id)
   or not ((v.origin_kind='COMMON_HEAD_RECEIPT_V1' and exists(select 1 from public.weekly_source_entitlement_heads h
     join public.weekly_source_entitlement_decision_bundles bundle on bundle.decision_bundle_id=h.decision_bundle_id and bundle.bundle_revision=h.bundle_revision
     join public.weekly_source_root_authorisations auth on auth.current_entitlement_head_id=h.id and auth.root_timesheet_id=h.root_timesheet_id
     where h.id=v.head_id and h.state='COMMITTED_CURRENT' and bundle.state='COMMITTED' and auth.withdrawn_at_utc is null))
     or (v.origin_kind='INITIAL_AUTHORISED_TSFIN_V1' and exists(
       select 1 from private.bpay_next_bound_source_snapshot_v1(v.id,p.source_book_revision) initial
       where initial.qualification_state='READY' and
         (initial.candidate_id,initial.canonical_work_id,initial.canonical_component_key,initial.binding_version_id,
           initial.approval_work_id,initial.approval_revision_id,initial.approved_line_id)
         =(v.candidate_id,v.canonical_work_id,v.canonical_component_key,v.id,v.approval_work_id,v.approval_revision_id,v.approved_line_id)))) then
   raise exception 'BPAY_NEXT_COMPARISON_COLLECTION_SOURCE_AUTHORITY_INVALID';end if;
 select * into b from private.bpay_next_work_collection where work_id=p.work_id and component_key=p.component_key for update;
 if not found or not b.paid_basis_qualified or not private.bpay_next_work_collection_source_original_v2(b) then
   raise exception 'BPAY_NEXT_COMPARISON_COLLECTION_ORIGINAL_ORIGIN_REVIEW_REQUIRED' using errcode='55000';end if;
 actual:=private.bpay_next_source_comparison_case_basis_v2(p.work_id,p.component_key,true);
 if r.case_basis is distinct from actual then raise exception 'BPAY_NEXT_COMPARISON_COLLECTION_CASE_BASIS_CHANGED';end if;
 if b.case_id is null then
   principal:=greatest(r.comparable_paid_ex_vat-r.approved_source_ex_vat,0);
   if principal>0 then
     insert into private.bpay_next_finance_case(id,candidate_id,case_kind,tax_treatment,status,principal_approved,order_key,case_revision)
       values(b.id,j.candidate_id,'OVERPAYMENT','TAXABLE','OPEN',principal,j.command_sequence,1);
     insert into private.bpay_next_case_rule(id,case_id,candidate_id,rule_revision,case_kind,case_subtype,tax_treatment,case_created_at_utc)
       values(b.id,b.id,j.candidate_id,1,'OVERPAYMENT','OVERPAYMENT','TAXABLE',transaction_timestamp());
     update private.bpay_next_finance_case set current_rule_id=b.id,current_rule_revision=1 where id=b.id;
     insert into private.bpay_next_case_component(id,case_id,candidate_id,component_key,component_ordinal,component_revision,rule_id,
       case_kind,case_subtype,tax_treatment,instruction_kind,direction,payroll_stage,source_pay_channel,currency,approved_source_ex_vat,resolution_state)
       values(b.id,b.id,j.candidate_id,'RECOVERY',1,1,b.id,'OVERPAYMENT','OVERPAYMENT','TAXABLE','RECOVERY','DEDUCTION',
         'GROSS_DEDUCT',r.target_pay_channel,'GBP',principal,'REVIEW');
     b.case_id:=b.id;b.case_component_id:=b.id;delta:=principal;ev:='APPROVED';
   end if;
 else
   select * into strict c from private.bpay_next_finance_case where id=b.case_id for update;
   select * into strict x from private.bpay_next_case_component where id=b.case_component_id for update;
   if (c.principal_approved,c.principal_recovered,c.principal_written_off,c.active_recovery_hold_amount)
     is distinct from (x.approved_source_ex_vat,x.recovered_source_ex_vat,x.written_off_source_ex_vat,x.active_recovery_source_ex_vat) then
     raise exception 'BPAY_NEXT_WORK_COLLECTION_BALANCE_MISMATCH';end if;
   o:=greatest(r.comparable_paid_ex_vat-r.approved_source_ex_vat-(r.case_resolution->>'target_written_off_ex_vat')::numeric,
     (r.case_resolution->>'target_active_capacity_ex_vat')::numeric,0);
   if x.source_pay_channel=r.target_pay_channel then s:=o;
   else
     s:=(r.case_resolution->>'source_outstanding_ex_vat')::numeric;
     if s is null or (r.case_resolution->>'target_outstanding_ex_vat')::numeric is distinct from o then
       raise exception 'BPAY_NEXT_COMPARISON_CASE_SELECTED_REMAINDER_REVIEW_REQUIRED' using errcode='55000';end if;
   end if;
   principal:=c.principal_recovered+c.principal_written_off+greatest(s,c.active_recovery_hold_amount);
   delta:=principal-c.principal_approved;
   if delta<>0 then
     update private.bpay_next_finance_case set principal_approved=principal,
       status=case when principal=principal_recovered+principal_written_off then 'CLOSED' when status='PAUSED' then 'PAUSED' else 'OPEN' end,
       case_revision=case_revision+1,updated_at_utc=transaction_timestamp() where id=c.id;
     update private.bpay_next_case_component set approved_source_ex_vat=principal,component_revision=component_revision+1,
       resolution_state='REVIEW',target_pay_channel=null,target_ex_vat=null,target_vat=null,target_inc_vat=null,
       valuation_policy_id=null,valuation_window_id=null,resolution_ref=null,updated_at_utc=transaction_timestamp() where id=x.id;
     ev:='CORRECTED';
   elsif principal=c.principal_recovered+c.principal_written_off and c.status<>'CLOSED' then
     update private.bpay_next_finance_case set status='CLOSED',case_revision=case_revision+1,updated_at_utc=transaction_timestamp() where id=c.id;
   end if;
 end if;
 if ev is not null then
   insert into private.bpay_next_case_event(case_id,operation_id,operation_item_id,event_kind,approved_delta,original_transfer_id,original_effect_id)
     values(b.case_id,j.command_id,b.id,ev,delta,b.original_transfer_id,b.original_effect_id);
 end if;
 update private.bpay_next_work_collection set case_id=b.case_id,case_component_id=b.case_component_id,qualification_state='READY',
   observed_revision_id=p.applied_revision_id,observed_approved_source_ex_vat=p.approved_source_ex_vat,
   observed_realised_source_ex_vat=p.realised_source_ex_vat,reconcile_revision=reconcile_revision+1,last_job_id=j.id,last_effect_id=p_effect_id,
   updated_at_utc=transaction_timestamp() where id=b.id;
 if b.case_id is not null then
   perform private.bpay_next_source_comparison_case_reconcile_v2(j.id,p.source_book_revision,r.id,b.id);
 end if;
end $function$;

alter function private.bpay_next_source_comparison_case_capacity_proof_v2(private.bpay_next_source_comparison_receipt,uuid,uuid,text) owner to postgres;
alter function private.bpay_next_source_comparison_guard_v2() owner to postgres;
alter function private.bpay_next_source_comparison_carry_v2(uuid,bigint,uuid,text,uuid) owner to postgres;
alter function private.bpay_next_reconcile_source_comparison_collection_v2(uuid,uuid,text,uuid) owner to postgres;
revoke all on function private.bpay_next_source_comparison_case_capacity_proof_v2(private.bpay_next_source_comparison_receipt,uuid,uuid,text),
 private.bpay_next_source_comparison_guard_v2(),
 private.bpay_next_source_comparison_carry_v2(uuid,bigint,uuid,text,uuid),
 private.bpay_next_reconcile_source_comparison_collection_v2(uuid,uuid,text,uuid) from public,anon,authenticated,service_role;
commit;
