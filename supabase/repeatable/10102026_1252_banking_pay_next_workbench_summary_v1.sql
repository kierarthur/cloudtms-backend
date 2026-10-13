-- WB-08 only maintained typed projection owners. No money/seed/history owner.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_workbench_require_next_v1()
returns void language plpgsql stable security definer set search_path=pg_catalog,private
as $function$
begin
  if not exists(select 1 from private.bpay_next_module_control m where m.id=1 and m.active_owner='NEXT') then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
end
$function$;

create or replace function private.bpay_next_workbench_ensure_metadata_v1(p_candidate_id uuid)
returns private.bpay_next_workbench_candidate_metadata
language plpgsql security definer set search_path=pg_catalog,private
as $function$
declare v_metadata private.bpay_next_workbench_candidate_metadata%rowtype;
begin
  perform private.bpay_next_workbench_require_next_v1();
  insert into private.bpay_next_workbench_candidate_metadata(candidate_id,candidate_sort_name,tms_ref)
    select c.id,coalesce(nullif(btrim(c.display_name),''),nullif(btrim(concat_ws(' ',c.first_name,c.last_name)),''),c.id::text),c.tms_ref
    from public.candidates c where c.id=p_candidate_id on conflict(candidate_id) do nothing;
  select m.* into strict v_metadata from private.bpay_next_workbench_candidate_metadata m
    where m.candidate_id=p_candidate_id for update;
  return v_metadata;
end
$function$;

-- At most20 aggregate rows for one exact group. Native segment counts exclude
-- portion_kind; a split AVAILABLE/RESERVED member counts once in section ALL.
create or replace function private.bpay_next_workbench_group_contribution_v1(p_group_key text)
returns setof private.bpay_next_workbench_delta
language plpgsql stable security definer set search_path=pg_catalog,private
as $function$
begin
  perform private.bpay_next_workbench_require_next_v1();
  return query
    with scoped as (
      select g.candidate_id,cs.client_id,cs.client_scope,ch.pay_channel,sec.section,
        i.group_key,i.component_key,i.member_kind,i.member_key,i.portion_kind,i.section item_section,
        i.selectable,i.is_selected,i.direction,i.payroll_stage,i.amount_known,i.selected_amount_known,
        (g.complete and g.certified_zero and i.member_kind='GROUP_EVIDENCE') certified_zero_evidence,
        i.reason_code,i.gross_due_source_ex_vat,i.nominal_source_ex_vat,i.approved_source_ex_vat,i.realised_source_ex_vat,
        i.active_reserved_source_ex_vat,i.included_source_ex_vat,i.excluded_source_ex_vat,
        i.recovery_source_ex_vat,i.deduction_balance_source_ex_vat,i.selected_source_ex_vat
      from private.bpay_next_workbench_item i
      join private.bpay_next_workbench_group g on g.group_key=i.group_key
      cross join lateral(select distinct v.client_id,v.client_scope from (values(g.client_id,g.client_id::text),(null::uuid,'ALL'::text)) v(client_id,client_scope)
        where v.client_scope is not null) cs
      cross join lateral(select distinct v.pay_channel from (values(g.target_pay_channel),('ALL'::text)) v(pay_channel)
        where v.pay_channel is not null) ch
      cross join lateral(select distinct v.section from (values(i.section),('ALL'::text)) v(section)) sec
      where i.group_key=p_group_key
    )
    select candidate_id,client_id,client_scope,pay_channel,section,
      count(distinct group_key)::bigint,
      count(*) filter(where member_kind<>'GROUP_EVIDENCE')::bigint,
      count(distinct (component_key,member_kind,member_key)) filter(where member_kind<>'GROUP_EVIDENCE')::bigint,
      count(*) filter(where selectable and direction='PAYMENT')::bigint,
      count(distinct (component_key,member_kind,member_key)) filter(where is_selected)::bigint,
      count(*) filter(where reason_code='SNOOZED')::bigint,
      count(*) filter(where portion_kind='RESERVED')::bigint,
      count(*) filter(where reason_code in ('PAY_QUERY_OPEN','PAY_QUERY_UNAVAILABLE'))::bigint,
      count(*) filter(where item_section='BLOCKED')::bigint,
      count(distinct (component_key,member_kind,member_key)) filter(where selectable and direction='DEDUCTION')::bigint,
      count(*) filter(where not amount_known and not certified_zero_evidence)::bigint,
      count(*) filter(where is_selected and not selected_amount_known)::bigint,
      coalesce(sum(included_source_ex_vat) filter(where item_section='READY' and direction='PAYMENT' and payroll_stage='GROSS_ADD'),0)::numeric,
      coalesce(sum(gross_due_source_ex_vat) filter(where item_section='WAITING' and direction='PAYMENT' and payroll_stage='GROSS_ADD' and portion_kind<>'RESERVED'),0)::numeric,
      coalesce(sum(included_source_ex_vat) filter(where item_section='READY' and direction='PAYMENT' and payroll_stage='NET_ADD'),0)::numeric,
      coalesce(sum(nominal_source_ex_vat) filter(where item_section='WAITING' and direction='PAYMENT' and payroll_stage='NET_ADD' and portion_kind<>'RESERVED'),0)::numeric,
      coalesce(sum(deduction_balance_source_ex_vat) filter(where direction='DEDUCTION' and payroll_stage='GROSS_DEDUCT'),0)::numeric,
      coalesce(sum(deduction_balance_source_ex_vat) filter(where direction='DEDUCTION' and payroll_stage='NET_DEDUCT'),0)::numeric,
      coalesce(sum(approved_source_ex_vat),0)::numeric,
      coalesce(sum(realised_source_ex_vat),0)::numeric,
      coalesce(sum(active_reserved_source_ex_vat),0)::numeric,
      coalesce(sum(excluded_source_ex_vat),0)::numeric,
      coalesce(sum(recovery_source_ex_vat),0)::numeric,
      coalesce(sum(selected_source_ex_vat) filter(where is_selected and selected_amount_known),0)::numeric
    from scoped group by candidate_id,client_id,client_scope,pay_channel,section;
end
$function$;

-- Only refresh_work/refresh_case call this sole natural summary delta owner.
create or replace function private.bpay_next_workbench_apply_delta_v1(
  p_before private.bpay_next_workbench_delta[],p_after private.bpay_next_workbench_delta[])
returns void language plpgsql security definer set search_path=pg_catalog,private
as $function$
declare v record;v_old_count bigint;v_new_count bigint;v_revision bigint;
  v_metadata private.bpay_next_workbench_candidate_metadata%rowtype;
begin
  perform private.bpay_next_workbench_require_next_v1();
  if coalesce(cardinality(p_before),0)>20 or coalesce(cardinality(p_after),0)>20
    or coalesce(pg_column_size(p_before),0)+coalesce(pg_column_size(p_after),0)>32768 then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_DELTA_ENVELOPE_EXCEEDED';
  end if;
  select r.workbench_revision into strict v_revision from private.bpay_next_workbench_revision r where r.id=1 for update;
  for v in
    with signed as (
      select b.candidate_id,b.client_id,b.client_scope,b.pay_channel,b.section,-b.item_count item_count,-b.contribution_count contribution_count,-b.segment_count segment_count,-b.payable_count payable_count,-b.selected_count selected_count,-b.snoozed_count snoozed_count,-b.held_count held_count,-b.queried_count queried_count,-b.blocked_count blocked_count,-b.deduction_item_count deduction_item_count,-b.unknown_amount_count unknown_amount_count,-b.unknown_selected_amount_count unknown_selected_amount_count,-b.amount amount,-b.waiting_amount waiting_amount,-b.ready_net_add_amount ready_net_add_amount,-b.waiting_net_add_amount waiting_net_add_amount,-b.gross_deduction_balance gross_deduction_balance,-b.net_deduction_balance net_deduction_balance,-b.approved_amount approved_amount,-b.paid_credit_amount paid_credit_amount,-b.reserved_amount reserved_amount,-b.excluded_amount excluded_amount,-b.recovery_amount recovery_amount,-b.selected_amount selected_amount
      from unnest(coalesce(p_before,array[]::private.bpay_next_workbench_delta[])) b
      union all
      select a.candidate_id,a.client_id,a.client_scope,a.pay_channel,a.section,a.item_count,a.contribution_count,a.segment_count,a.payable_count,a.selected_count,a.snoozed_count,a.held_count,a.queried_count,a.blocked_count,a.deduction_item_count,a.unknown_amount_count,a.unknown_selected_amount_count,a.amount,a.waiting_amount,a.ready_net_add_amount,a.waiting_net_add_amount,a.gross_deduction_balance,a.net_deduction_balance,a.approved_amount,a.paid_credit_amount,a.reserved_amount,a.excluded_amount,a.recovery_amount,a.selected_amount
      from unnest(coalesce(p_after,array[]::private.bpay_next_workbench_delta[])) a
    ) select candidate_id,client_id,client_scope,pay_channel,section,sum(item_count) item_count,sum(contribution_count) contribution_count,sum(segment_count) segment_count,sum(payable_count) payable_count,sum(selected_count) selected_count,sum(snoozed_count) snoozed_count,sum(held_count) held_count,sum(queried_count) queried_count,sum(blocked_count) blocked_count,sum(deduction_item_count) deduction_item_count,sum(unknown_amount_count) unknown_amount_count,sum(unknown_selected_amount_count) unknown_selected_amount_count,sum(amount) amount,sum(waiting_amount) waiting_amount,sum(ready_net_add_amount) ready_net_add_amount,sum(waiting_net_add_amount) waiting_net_add_amount,sum(gross_deduction_balance) gross_deduction_balance,sum(net_deduction_balance) net_deduction_balance,sum(approved_amount) approved_amount,sum(paid_credit_amount) paid_credit_amount,sum(reserved_amount) reserved_amount,sum(excluded_amount) excluded_amount,sum(recovery_amount) recovery_amount,sum(selected_amount) selected_amount
      from signed group by candidate_id,client_id,client_scope,pay_channel,section
      having sum(item_count)<>0 or sum(contribution_count)<>0 or sum(segment_count)<>0 or sum(payable_count)<>0 or sum(selected_count)<>0 or sum(snoozed_count)<>0 or sum(held_count)<>0 or sum(queried_count)<>0 or sum(blocked_count)<>0 or sum(deduction_item_count)<>0 or sum(unknown_amount_count)<>0 or sum(unknown_selected_amount_count)<>0 or sum(amount)<>0 or sum(waiting_amount)<>0 or sum(ready_net_add_amount)<>0 or sum(waiting_net_add_amount)<>0 or sum(gross_deduction_balance)<>0 or sum(net_deduction_balance)<>0 or sum(approved_amount)<>0 or sum(paid_credit_amount)<>0 or sum(reserved_amount)<>0 or sum(excluded_amount)<>0 or sum(recovery_amount)<>0 or sum(selected_amount)<>0
      order by candidate_id,client_scope,pay_channel,section
  loop
    select m.* into strict v_metadata from private.bpay_next_workbench_candidate_metadata m where m.candidate_id=v.candidate_id;
    insert into private.bpay_next_workbench_candidate_summary(candidate_id,client_id,client_scope,pay_channel,section,
      candidate_sort_name,tms_ref,bank_warning_code,metadata_version,projection_revision)
      values(v.candidate_id,v.client_id,v.client_scope,v.pay_channel,v.section,v_metadata.candidate_sort_name,
        v_metadata.tms_ref,v_metadata.bank_warning_code,v_metadata.metadata_version,v_revision) on conflict do nothing;
    select c.item_count into strict v_old_count from private.bpay_next_workbench_candidate_summary c
      where (c.candidate_id,c.client_scope,c.pay_channel,c.section)=(v.candidate_id,v.client_scope,v.pay_channel,v.section) for update;
    update private.bpay_next_workbench_candidate_summary c set item_count=c.item_count+v.item_count,contribution_count=c.contribution_count+v.contribution_count,segment_count=c.segment_count+v.segment_count,payable_count=c.payable_count+v.payable_count,selected_count=c.selected_count+v.selected_count,snoozed_count=c.snoozed_count+v.snoozed_count,held_count=c.held_count+v.held_count,queried_count=c.queried_count+v.queried_count,blocked_count=c.blocked_count+v.blocked_count,deduction_item_count=c.deduction_item_count+v.deduction_item_count,unknown_amount_count=c.unknown_amount_count+v.unknown_amount_count,unknown_selected_amount_count=c.unknown_selected_amount_count+v.unknown_selected_amount_count,amount=c.amount+v.amount,waiting_amount=c.waiting_amount+v.waiting_amount,ready_net_add_amount=c.ready_net_add_amount+v.ready_net_add_amount,waiting_net_add_amount=c.waiting_net_add_amount+v.waiting_net_add_amount,gross_deduction_balance=c.gross_deduction_balance+v.gross_deduction_balance,net_deduction_balance=c.net_deduction_balance+v.net_deduction_balance,approved_amount=c.approved_amount+v.approved_amount,paid_credit_amount=c.paid_credit_amount+v.paid_credit_amount,reserved_amount=c.reserved_amount+v.reserved_amount,excluded_amount=c.excluded_amount+v.excluded_amount,recovery_amount=c.recovery_amount+v.recovery_amount,selected_amount=c.selected_amount+v.selected_amount,
      projection_revision=v_revision
      where (c.candidate_id,c.client_scope,c.pay_channel,c.section)=(v.candidate_id,v.client_scope,v.pay_channel,v.section)
      returning c.item_count into v_new_count;
    insert into private.bpay_next_workbench_scope_summary(client_id,client_scope,pay_channel,section,projection_revision)
      values(v.client_id,v.client_scope,v.pay_channel,v.section,v_revision) on conflict do nothing;
    update private.bpay_next_workbench_scope_summary c set item_count=c.item_count+v.item_count,contribution_count=c.contribution_count+v.contribution_count,segment_count=c.segment_count+v.segment_count,payable_count=c.payable_count+v.payable_count,selected_count=c.selected_count+v.selected_count,snoozed_count=c.snoozed_count+v.snoozed_count,held_count=c.held_count+v.held_count,queried_count=c.queried_count+v.queried_count,blocked_count=c.blocked_count+v.blocked_count,deduction_item_count=c.deduction_item_count+v.deduction_item_count,unknown_amount_count=c.unknown_amount_count+v.unknown_amount_count,unknown_selected_amount_count=c.unknown_selected_amount_count+v.unknown_selected_amount_count,amount=c.amount+v.amount,waiting_amount=c.waiting_amount+v.waiting_amount,ready_net_add_amount=c.ready_net_add_amount+v.ready_net_add_amount,waiting_net_add_amount=c.waiting_net_add_amount+v.waiting_net_add_amount,gross_deduction_balance=c.gross_deduction_balance+v.gross_deduction_balance,net_deduction_balance=c.net_deduction_balance+v.net_deduction_balance,approved_amount=c.approved_amount+v.approved_amount,paid_credit_amount=c.paid_credit_amount+v.paid_credit_amount,reserved_amount=c.reserved_amount+v.reserved_amount,excluded_amount=c.excluded_amount+v.excluded_amount,recovery_amount=c.recovery_amount+v.recovery_amount,selected_amount=c.selected_amount+v.selected_amount,
      candidate_count=c.candidate_count+(case when v_new_count>0 then 1 else 0 end)-(case when v_old_count>0 then 1 else 0 end),
      projection_revision=v_revision
      where (c.client_scope,c.pay_channel,c.section)=(v.client_scope,v.pay_channel,v.section);
    if v_new_count=0 then
      if exists(select 1 from private.bpay_next_workbench_candidate_summary c
        where (c.candidate_id,c.client_scope,c.pay_channel,c.section)=(v.candidate_id,v.client_scope,v.pay_channel,v.section)
          and (c.contribution_count<>0 or c.segment_count<>0 or c.payable_count<>0 or c.selected_count<>0 or c.snoozed_count<>0 or c.held_count<>0 or c.queried_count<>0 or c.blocked_count<>0 or c.deduction_item_count<>0 or c.unknown_amount_count<>0 or c.unknown_selected_amount_count<>0 or c.amount<>0 or c.waiting_amount<>0 or c.ready_net_add_amount<>0 or c.waiting_net_add_amount<>0 or c.gross_deduction_balance<>0 or c.net_deduction_balance<>0 or c.approved_amount<>0 or c.paid_credit_amount<>0 or c.reserved_amount<>0 or c.excluded_amount<>0 or c.recovery_amount<>0 or c.selected_amount<>0)) then
        raise exception using errcode='23514',message='BPAY_NEXT_WORKBENCH_CANDIDATE_DELTA_NOT_CONSERVED';
      end if;
      delete from private.bpay_next_workbench_candidate_summary c
        where (c.candidate_id,c.client_scope,c.pay_channel,c.section)=(v.candidate_id,v.client_scope,v.pay_channel,v.section);
    end if;
    if exists(select 1 from private.bpay_next_workbench_scope_summary c
      where (c.client_scope,c.pay_channel,c.section)=(v.client_scope,v.pay_channel,v.section) and c.candidate_count=0
        and (c.item_count<>0 or c.contribution_count<>0 or c.segment_count<>0 or c.payable_count<>0 or c.selected_count<>0 or c.snoozed_count<>0 or c.held_count<>0 or c.queried_count<>0 or c.blocked_count<>0 or c.deduction_item_count<>0 or c.unknown_amount_count<>0 or c.unknown_selected_amount_count<>0 or c.amount<>0 or c.waiting_amount<>0 or c.ready_net_add_amount<>0 or c.waiting_net_add_amount<>0 or c.gross_deduction_balance<>0 or c.net_deduction_balance<>0 or c.approved_amount<>0 or c.paid_credit_amount<>0 or c.reserved_amount<>0 or c.excluded_amount<>0 or c.recovery_amount<>0 or c.selected_amount<>0)) then
      raise exception using errcode='23514',message='BPAY_NEXT_WORKBENCH_SCOPE_DELTA_NOT_CONSERVED';
    end if;
    delete from private.bpay_next_workbench_scope_summary c
      where (c.client_scope,c.pay_channel,c.section)=(v.client_scope,v.pay_channel,v.section) and c.candidate_count=0;
  end loop;
end
$function$;
-- WB08 REFRESH OWNERS
-- Exact live accepted member, including enrollment before run_worker exists.
create or replace function private.bpay_next_workbench_accepted_work_v1(
  p_work_id uuid,p_component_key text,p_segment_key text)
returns table(run_id uuid,command_id uuid,module_epoch bigint,selected_source_ex_vat numeric)
language plpgsql stable security definer set search_path=pg_catalog,private
as $function$
declare v_count integer:=0;v record;
begin
  perform private.bpay_next_workbench_require_next_v1();
  for v in
    select r.id run_id,c.id command_id,c.module_epoch,
      case when p_segment_key is not null then
        (select p.included_positive_source_ex_vat from private.bpay_next_run_work_segment_parent p
          where p.run_work_id=rw.id and p.component_key=p_component_key and p.segment_key=p_segment_key)
      else (select l.source_consumed_ex_vat from private.bpay_next_run_line l
          where l.run_work_id=rw.id and l.component_key=p_component_key) end selected_source_ex_vat
    from private.bpay_next_work_choice w
    join private.bpay_next_run_selection rs on rs.run_id=w.run_id and rs.work_id=w.work_id
    join private.bpay_next_pay_run r on r.id=w.run_id and r.selection_state='SEALED'
      and r.status in ('PREPARING','REVIEW','DRAFT','EXECUTING')
    join private.bpay_next_selection_candidate sc on sc.run_id=r.id and sc.candidate_id=rs.candidate_id
    join private.bpay_next_run_command rc on rc.run_id=r.id
    join private.bpay_next_command c on c.id=rc.command_id and c.command_kind='PREPARE'
    left join private.bpay_next_run_worker worker on worker.run_id=r.id and worker.candidate_id=rs.candidate_id
    left join private.bpay_next_run_work rw on rw.run_worker_id=worker.id and rw.work_id=w.work_id
    where w.work_id=p_work_id and w.selection_state='SEALED'
      and (worker.id is null or worker.status in ('PREPARING','REVIEW','READY','DRAFT','CANCELLING','ISSUED'))
      and ((p_segment_key is not null and exists(select 1 from private.bpay_next_selected_segment s
        where s.run_id=r.id and s.work_id=w.work_id and s.component_key=p_component_key
          and s.segment_key=p_segment_key and s.disposition='INCLUDED'))
        or (p_segment_key is null and exists(select 1 from private.bpay_next_approved_line a
          where a.revision_id=w.expected_revision_id and a.component_key=p_component_key
            and not exists(select 1 from private.bpay_next_approved_segment s
              where s.revision_id=a.revision_id and s.component_key=a.component_key))
          and (w.selection_mode='ALL' or exists(select 1 from private.bpay_next_selected_component s
            where s.run_id=r.id and s.work_id=w.work_id and s.component_key=p_component_key))
          and private.bpay_next_snooze_at_cut_v1(w.work_id,w.expected_revision_id,null,p_component_key,
            r.snooze_control_sequence,r.snooze_business_date) is null))
  loop
    v_count:=v_count+1;
    if v_count>1 then raise exception using errcode='23514',message='BPAY_NEXT_WORKBENCH_ACCEPTED_MEMBER_COLLISION';end if;
    run_id:=v.run_id;command_id:=v.command_id;module_epoch:=v.module_epoch;
    selected_source_ex_vat:=v.selected_source_ex_vat;return next;
  end loop;
end
$function$;

-- One exact work inventory. No history/NET allocation or missing credit guess.
create or replace function private.bpay_next_workbench_work_members_v1(p_work_id uuid,p_revision_id uuid)
returns table(component_key text,approved_line_id uuid,approved_segment_id uuid,segment_key text,
  component_kind text,tax_treatment text,work_date date,approved_quantity numeric,unit_label text,
  approved_amount numeric,realised_amount numeric,held_amount numeric,source_pay_channel text,
  parent_complete boolean,segment_attribution_complete boolean)
language plpgsql security definer set search_path=pg_catalog,private
as $function$
begin
  perform private.bpay_next_workbench_require_next_v1();
  return query
  with source_books as materialized (
    select p.component_key,book.qualification_state,book.approval_revision_id,book.approved_line_id,
      book.source_channel,book.approved_source_ex_vat,book.native_realised_paye,book.native_realised_umbrella,
      book.native_held_paye,book.native_held_umbrella
    from private.bpay_next_work w join private.bpay_next_work_revision r on r.id=p_revision_id and r.work_id=w.id
    join private.bpay_next_position p on p.work_id=w.id and p.source_book_state='READY' and p.source_book_revision>0
    left join private.bpay_next_approved_line a on a.revision_id=r.id and a.component_key=p.component_key
    join private.bpay_next_source_component_binding b on b.canonical_work_id=w.id and b.canonical_component_key=p.component_key
    join private.bpay_next_source_component_binding_version v on v.id=b.current_binding_version_id
      and v.approval_work_id=w.id and v.approval_revision_id=r.id and v.approval_component_key=p.component_key
    cross join lateral private.bpay_next_bound_source_snapshot_v1(v.id,p.source_book_revision) book
    where w.id=p_work_id and w.work_kind='SOURCE' and (a.component_kind='WORK' or a.id is null) and (r.source_head_id is not null
      or (r.source_kind='SOURCE' and r.source_event_id is not null and r.financial_snapshot_id is not null))
  ), keys as (
    select a.component_key from private.bpay_next_approved_line a where a.revision_id=p_revision_id
    union select p.component_key from private.bpay_next_position p where p.work_id=p_work_id
  ), parents as (
    select k.component_key,a.id line_id,a.component_kind,a.tax_treatment,a.work_date,a.approved_quantity,a.unit_label,
      p.approved_source_ex_vat a,p.realised_source_ex_vat c,p.held_source_ex_vat h,p.source_basis_channel,
      p.realised_target_ex_vat,p.realised_target_vat,p.realised_target_inc_vat,
      p.held_target_ex_vat,p.held_target_vat,p.held_target_inc_vat,
      (p.work_id is not null and p.applied_revision_id=p_revision_id
        and p.approved_source_ex_vat=coalesce(a.source_pay_ex_vat,0)
        and (a.id is not null or p.approved_source_ex_vat=0)
        and (not (w.work_kind='SOURCE' and (a.component_kind='WORK' or a.id is null) and (r.source_head_id is not null
          or (r.source_kind='SOURCE' and r.source_event_id is not null and r.financial_snapshot_id is not null)))
          or (book.qualification_state='READY' and book.approval_revision_id=p_revision_id
            and book.approved_line_id is not distinct from a.id and book.source_channel=r.source_pay_channel
            and (book.approved_source_ex_vat,book.native_realised_paye,book.native_held_paye,book.native_realised_umbrella,book.native_held_umbrella)
              is not distinct from (p.approved_source_ex_vat,
                case when r.source_pay_channel='PAYE' then p.realised_source_ex_vat else 0::numeric end,
                case when r.source_pay_channel='PAYE' then p.held_source_ex_vat else 0::numeric end,
                case when r.source_pay_channel='UMBRELLA' then p.realised_source_ex_vat else 0::numeric end,
                case when r.source_pay_channel='UMBRELLA' then p.held_source_ex_vat else 0::numeric end)))) complete,
      (exists(select 1 from private.bpay_next_approved_segment s where s.revision_id=p_revision_id and s.component_key=k.component_key)
        or exists(select 1 from private.bpay_next_work_segment_origin_share s where s.work_id=p_work_id and s.component_key=k.component_key)) segmented
    from keys k left join private.bpay_next_approved_line a on a.revision_id=p_revision_id and a.component_key=k.component_key
    left join private.bpay_next_position p on p.work_id=p_work_id and p.component_key=k.component_key
    join private.bpay_next_work w on w.id=p_work_id
    left join private.bpay_next_work_revision r on r.id=p_revision_id and r.work_id=w.id
    left join source_books book on book.component_key=k.component_key
  ), segment_keys as (
    select s.component_key,s.segment_key from private.bpay_next_approved_segment s where s.revision_id=p_revision_id
    union select s.component_key,s.segment_key from private.bpay_next_work_segment_origin_share s where s.work_id=p_work_id
  ), members as (
    select p.*,s.segment_key,seg.id segment_id,seg.work_date segment_date,seg.approved_hours,
      coalesce(seg.approved_source_ex_vat,0) segment_a,b.realised_source_ex_vat segment_c,b.active_held_source_ex_vat segment_h
    from parents p join segment_keys s on s.component_key=p.component_key
    left join private.bpay_next_approved_segment seg on seg.revision_id=p_revision_id
      and seg.component_key=s.component_key and seg.segment_key=s.segment_key
    cross join lateral private.bpay_next_work_segment_gross_balance_v1(p_work_id,s.component_key,s.segment_key) b
  ), totals as (
    select m.component_key,sum(m.segment_a) a,sum(m.segment_c) c,sum(m.segment_h) h from members m group by m.component_key
  ), target_credit as (
    select e.component_key,sum(e.target_ex_vat) ex,sum(e.target_vat) vat,sum(e.target_inc_vat) inc
    from private.bpay_next_work_segment_effect_share e where e.work_id=p_work_id group by e.component_key
  ), target_hold as (
    select s.component_key,sum(s.target_reserved_ex_vat) ex,sum(s.target_reserved_vat) vat,sum(s.target_reserved_inc_vat) inc
    from private.bpay_next_work_segment_origin_share s join private.bpay_next_hold h on h.id=s.hold_id and h.status='ACTIVE'
    where s.work_id=p_work_id group by s.component_key
  )
  select m.component_key,m.line_id,m.segment_id,m.segment_key,m.component_kind,m.tax_treatment,
    m.segment_date,m.approved_hours,'hours'::text,m.segment_a,m.segment_c,m.segment_h,m.source_basis_channel,
    coalesce(m.complete,false),
    ((t.a,t.c,t.h,coalesce(c.ex,0),coalesce(c.vat,0),coalesce(c.inc,0),coalesce(h.ex,0),coalesce(h.vat,0),coalesce(h.inc,0))
      is not distinct from (m.a,m.c,m.h,m.realised_target_ex_vat,m.realised_target_vat,m.realised_target_inc_vat,
        m.held_target_ex_vat,m.held_target_vat,m.held_target_inc_vat))
    from members m join totals t on t.component_key=m.component_key
      left join target_credit c on c.component_key=m.component_key left join target_hold h on h.component_key=m.component_key
  union all
  select p.component_key,p.line_id,null::uuid,null::text,p.component_kind,p.tax_treatment,p.work_date,
    p.approved_quantity,p.unit_label,p.a,p.c,p.h,p.source_basis_channel,coalesce(p.complete,false),true
    from parents p where not p.segmented;
end
$function$;

create or replace function private.bpay_next_workbench_refresh_work_v1(p_work_id uuid)
returns void language plpgsql security definer set search_path=pg_catalog,private
as $function$
declare
  v_work private.bpay_next_work%rowtype;v_revision private.bpay_next_work_revision%rowtype;
  v_metadata private.bpay_next_workbench_candidate_metadata%rowtype;
  v_item private.bpay_next_workbench_item%rowtype;v_base private.bpay_next_workbench_item%rowtype;
  v_before private.bpay_next_workbench_delta[];v_after private.bpay_next_workbench_delta[];
  v_member record;v_accepted record;v_totals record;v_candidate uuid;v_client uuid;v_target text;v_financial bigint;v_projection bigint;
  v_group text:='WORK:'||p_work_id::text;v_today date:=(transaction_timestamp() at time zone 'Europe/London')::date;
  v_sunday date;v_sequence bigint;v_reason text;v_priority smallint;v_section text;v_complete boolean;
  v_positive numeric;v_recovery numeric;v_overhang numeric;v_snooze jsonb;v_count bigint:=0;v_portion text;v_zero boolean;
begin
  perform private.bpay_next_workbench_require_next_v1();
  select w.candidate_id into strict v_candidate from private.bpay_next_work w where w.id=p_work_id;
  select c.financial_view_revision into strict v_financial from private.bpay_next_worker_control c where c.candidate_id=v_candidate for update;
  select w.* into strict v_work from private.bpay_next_work w where w.id=p_work_id for update;
  v_metadata:=private.bpay_next_workbench_ensure_metadata_v1(v_candidate);
  select c.pay_method::text into v_target from public.candidates c where c.id=v_candidate;
  if v_target not in ('PAYE','UMBRELLA') then v_target:=null;end if;
  select c.client_id into v_client from public.contracts c where c.id=v_work.contract_id;
  select r.* into v_revision from private.bpay_next_work_revision r where r.id=v_work.current_revision_id and r.work_id=v_work.id;
  v_complete:=v_work.approval_state='APPROVED' and v_revision.id is not null and v_revision.sealed_at_utc is not null
    and v_work.applied_revision_id=v_work.current_revision_id
    and (select count(*) from private.bpay_next_approved_line a where a.revision_id=v_revision.id)=v_revision.expected_line_count;
  select array_agg(d) into v_before from private.bpay_next_workbench_group_contribution_v1(v_group) d;
  update private.bpay_next_workbench_revision set workbench_revision=workbench_revision+1,updated_at_utc=transaction_timestamp()
    where id=1 returning workbench_revision into v_projection;
  delete from private.bpay_next_workbench_item where group_key=v_group;
  insert into private.bpay_next_workbench_group(group_key,group_kind,work_id,candidate_id,client_id,contract_id,
    physical_timesheet_id,current_revision_id,source_pay_channel,target_pay_channel,week_ending_date,
    complete,projection_revision,financial_view_revision,metadata_version,progress_state)
  values(v_group,'WORK',v_work.id,v_candidate,v_client,v_work.contract_id,v_revision.physical_timesheet_id,v_revision.id,
    v_revision.source_pay_channel,v_target,v_work.week_ending_date,coalesce(v_complete,false),v_projection,v_financial,v_metadata.metadata_version,
    case when v_complete then 'CURRENT' when v_work.approval_state='APPROVED' then 'PROCESSING' else 'UNAVAILABLE' end)
  on conflict(group_key) do update set client_id=excluded.client_id,contract_id=excluded.contract_id,
    physical_timesheet_id=excluded.physical_timesheet_id,current_revision_id=excluded.current_revision_id,
    source_pay_channel=excluded.source_pay_channel,target_pay_channel=excluded.target_pay_channel,week_ending_date=excluded.week_ending_date,
    complete=excluded.complete,projection_revision=excluded.projection_revision,financial_view_revision=excluded.financial_view_revision,
    metadata_version=excluded.metadata_version,progress_state=excluded.progress_state,error_code=null,updated_at_utc=transaction_timestamp();
  v_sunday:=v_today+(7-extract(isodow from v_today)::integer);
  select last_sequence into strict v_sequence from private.bpay_next_command_clock where id=1;
  for v_member in select m.component_key,m.approved_line_id,m.approved_segment_id,m.segment_key,m.component_kind,m.tax_treatment,m.work_date,m.approved_quantity,m.unit_label,m.approved_amount,m.realised_amount,m.held_amount,m.source_pay_channel,m.parent_complete,m.segment_attribution_complete from private.bpay_next_workbench_work_members_v1(v_work.id,v_revision.id) m order by m.component_key,m.segment_key nulls first loop
    v_count:=v_count+1;v_item:=null;v_item.id:=gen_random_uuid();v_item.group_key:=v_group;v_item.candidate_id:=v_candidate;
    v_item.work_id:=v_work.id;v_item.revision_id:=v_revision.id;v_item.approved_line_id:=v_member.approved_line_id;
    v_item.approved_segment_id:=v_member.approved_segment_id;v_item.segment_key:=v_member.segment_key;
    v_item.component_key:=v_member.component_key;v_item.member_kind:=case when v_member.segment_key is null then 'WORK_COMPONENT' else 'WORK_SEGMENT' end;
    v_item.member_key:=coalesce(v_member.segment_key,v_member.component_key);v_item.client_id:=v_client;v_item.contract_id:=v_work.contract_id;
    v_item.physical_timesheet_id:=v_revision.physical_timesheet_id;v_item.work_date:=v_member.work_date;
    v_item.week_ending_date:=v_work.week_ending_date;v_item.candidate_display_name:=v_metadata.candidate_sort_name;
    v_item.candidate_reference:=v_metadata.tms_ref;v_item.client_display_name:=v_revision.client_display_name;
    v_item.timesheet_reference:=v_revision.timesheet_reference;v_item.component_kind:=v_member.component_kind;
    v_item.tax_treatment:=v_member.tax_treatment;v_item.source_pay_channel:=v_member.source_pay_channel;v_item.target_pay_channel:=v_target;
    v_item.direction:='PAYMENT';v_item.payroll_stage:='GROSS_ADD';v_item.approved_quantity:=v_member.approved_quantity;v_item.unit_label:=v_member.unit_label;
    v_item.amount_known:=coalesce(v_complete,false) and v_member.parent_complete and v_member.segment_attribution_complete;
    v_item.is_selected:=false;v_item.selected_amount_known:=false;v_item.query_qualified:=false;v_item.query_state:='NOT_APPLICABLE';
    v_item.financial_view_revision:=v_financial;v_item.projection_revision:=v_projection;v_item.metadata_version:=v_metadata.metadata_version;
    v_item.progress_state:=case when v_complete then 'CURRENT' else 'PROCESSING' end;v_item.updated_at_utc:=transaction_timestamp();
    v_reason:='READY';v_priority:=200;v_section:='READY';
    if v_revision.id is null or v_target is null or v_work.week_ending_date is null
      or extract(isodow from v_work.week_ending_date)<>7 then v_reason:='DATA_UNAVAILABLE';v_priority:=10;v_section:='BLOCKED';
    elsif v_work.approval_state<>'APPROVED' then v_reason:='MISSING_APPROVAL';v_priority:=21;v_section:='BLOCKED';
    elsif not v_complete then v_reason:='APPLY_PENDING';v_priority:=30;v_section:='BLOCKED';
    elsif not v_member.parent_complete then v_reason:='DATA_UNAVAILABLE';v_priority:=10;v_section:='BLOCKED';
    elsif not v_member.segment_attribution_complete then v_reason:='SEGMENT_CREDIT_ATTRIBUTION_UNAVAILABLE';v_priority:=12;v_section:='BLOCKED';end if;
    if v_item.amount_known then
      v_positive:=greatest(v_member.approved_amount-v_member.realised_amount-v_member.held_amount,0);
      v_recovery:=greatest(v_member.realised_amount-v_member.approved_amount,0);
      v_overhang:=greatest(v_member.held_amount-greatest(v_member.approved_amount-v_member.realised_amount,0),0);
      v_item.nominal_source_ex_vat:=v_positive;v_item.approved_source_ex_vat:=v_member.approved_amount;
      v_item.realised_source_ex_vat:=v_member.realised_amount;v_item.active_reserved_source_ex_vat:=0;
      v_item.written_off_source_ex_vat:=0;
      v_item.residual_source_ex_vat:=v_member.approved_amount-v_member.realised_amount-v_member.held_amount;
      v_item.gross_due_source_ex_vat:=v_positive;v_item.included_source_ex_vat:=v_positive;v_item.excluded_source_ex_vat:=0;
      v_item.recovery_source_ex_vat:=v_recovery;v_item.held_overhang_source_ex_vat:=v_overhang;
    else v_positive:=null;v_recovery:=null;v_overhang:=null;end if;
    select a.run_id,a.command_id,a.module_epoch,a.selected_source_ex_vat into v_accepted from private.bpay_next_workbench_accepted_work_v1(v_work.id,v_member.component_key,v_member.segment_key) a;
    if found then
      v_item.is_selected:=true;v_item.accepted_run_id:=v_accepted.run_id;v_item.accepted_command_id:=v_accepted.command_id;
      v_item.accepted_module_epoch:=v_accepted.module_epoch;v_item.selected_source_ex_vat:=v_accepted.selected_source_ex_vat;
      v_item.selected_amount_known:=v_accepted.selected_source_ex_vat is not null;
      if v_priority>50 then v_reason:='IN_PREPARATION';v_priority:=50;v_section:='WAITING';end if;
    end if;
    v_snooze:=private.bpay_next_snooze_at_cut_v1(v_work.id,v_revision.id,v_member.segment_key,v_member.component_key,v_sequence,v_today);
    if v_snooze is not null then
      v_item.snooze_id:=(v_snooze->>'id')::uuid;v_item.snooze_version:=(v_snooze->>'version')::bigint;
      v_item.snooze_scope:=v_snooze->>'scope_kind';v_item.snooze_until_date:=(v_snooze->>'until_date')::date;
      if v_positive>0 then v_item.excluded_source_ex_vat:=v_positive;v_item.included_source_ex_vat:=0;end if;
    elsif exists(select 1 from private.bpay_next_snooze s where s.work_id=v_work.id and s.active
      and (s.until_date is null or s.until_date>=v_today) and ((s.scope_kind='SEGMENT' and s.segment_key=v_member.segment_key)
        or s.scope_kind='WHOLE_TIMESHEET' or (s.scope_kind='EXPENSE_LINE' and s.component_key=v_member.component_key)
        or (s.scope_kind='EXPENSES_ALL' and v_member.component_kind in ('EXPENSE','MILEAGE')))) then
      v_item.stale_snooze_code:=case when v_member.segment_key is not null and v_member.approved_segment_id is null
        then 'SNOOZE_SEGMENT_STALE' else 'SNOOZE_BASIS_STALE' end;
    end if;
    if v_priority>100 and v_work.week_ending_date>v_sunday then
      v_reason:='FUTURE_WEEK';v_priority:=100;v_section:='WAITING';v_item.eligible_from_date:=v_work.week_ending_date-6;
    elsif v_priority>110 and v_snooze is not null and v_positive>0 then v_reason:='SNOOZED';v_priority:=110;v_section:='WAITING';end if;
    v_item.query_qualified:=coalesce(v_work.work_kind='SOURCE' and v_revision.source_kind in ('SOURCE','PROTECTED')
      and v_member.component_kind in ('WORK','PROTECTED_WORK') and v_revision.source_event_id is not null
      and v_revision.physical_timesheet_id is not null and v_item.amount_known and v_item.included_source_ex_vat>0,false);
    if v_item.query_qualified then v_item.query_state:='UNAVAILABLE';end if;
    v_item.section:=v_section;v_item.reason_code:=v_reason;v_item.reason_priority:=v_priority;
    v_item.selectable:=v_section='READY' and v_item.amount_known;v_base:=v_item;
    if not v_item.amount_known or v_positive>0 or (v_member.approved_amount=0 and v_member.realised_amount=0
      and v_member.held_amount=0 and v_revision.certified_zero and v_member.approved_line_id is not null) then
      v_item.portion_kind:=case when not v_item.amount_known then 'EVIDENCE' when v_positive=0 then 'ZERO' else 'AVAILABLE' end;
      insert into private.bpay_next_workbench_item select (v_item).*;
    elsif v_recovery>0 then
      v_item.portion_kind:='RECOVERY';v_item.direction:='INFORMATIONAL';v_item.selectable:=false;
      if v_priority>83 then v_item.reason_code:='RECOVERY_MANAGED';v_item.reason_priority:=83;v_item.section:='ACTION';end if;
      insert into private.bpay_next_workbench_item select (v_item).*;
    end if;
    if v_item.amount_known and v_member.held_amount>0 then
      v_item:=v_base;v_item.id:=gen_random_uuid();v_item.portion_kind:='RESERVED';v_item.selectable:=false;
      v_item.active_reserved_source_ex_vat:=v_member.held_amount;v_item.nominal_source_ex_vat:=v_member.held_amount;
      v_item.gross_due_source_ex_vat:=0;v_item.included_source_ex_vat:=0;v_item.excluded_source_ex_vat:=0;
      v_item.recovery_source_ex_vat:=0;v_item.query_qualified:=false;v_item.query_state:='NOT_APPLICABLE';
      if v_positive>0 or v_recovery>0 then
        v_item.approved_source_ex_vat:=0;v_item.realised_source_ex_vat:=0;v_item.residual_source_ex_vat:=0;v_item.held_overhang_source_ex_vat:=0;
        v_item.is_selected:=false;v_item.selected_amount_known:=false;v_item.selected_source_ex_vat:=null;
      end if;
      if v_priority>111 then v_item.reason_code:='RESERVED';v_item.reason_priority:=111;v_item.section:='WAITING';end if;
      insert into private.bpay_next_workbench_item select (v_item).*;
    end if;
    if v_base.amount_known and (v_positive>0 or v_recovery>0 or v_member.held_amount>0
      or (v_member.approved_amount=0 and v_member.realised_amount=0 and v_revision.certified_zero and v_member.approved_line_id is not null)) then
      select sum(i.approved_source_ex_vat) a,sum(i.realised_source_ex_vat) c,sum(i.active_reserved_source_ex_vat) h,
        sum(i.residual_source_ex_vat) residual,
        sum(i.included_source_ex_vat+i.excluded_source_ex_vat-i.recovery_source_ex_vat-i.held_overhang_source_ex_vat) split_residual
        into v_totals from private.bpay_next_workbench_item i where i.group_key=v_group
          and i.component_key=v_member.component_key and i.member_kind=v_base.member_kind and i.member_key=v_base.member_key;
      if (v_totals.a,v_totals.c,v_totals.h,v_totals.residual,v_totals.split_residual)
        is distinct from (v_member.approved_amount,v_member.realised_amount,v_member.held_amount,
          v_member.approved_amount-v_member.realised_amount-v_member.held_amount,
          v_member.approved_amount-v_member.realised_amount-v_member.held_amount) then
        raise exception using errcode='23514',message='BPAY_NEXT_WORKBENCH_NATIVE_MEMBER_NOT_CONSERVED';end if;
    end if;
  end loop;
  if v_count=0 then
    v_item:=null;v_item.id:=gen_random_uuid();v_item.group_key:=v_group;v_item.candidate_id:=v_candidate;v_item.work_id:=v_work.id;
    v_item.revision_id:=v_revision.id;v_item.member_kind:='GROUP_EVIDENCE';v_item.member_key:=v_work.id::text;v_item.component_key:='';v_item.portion_kind:='EVIDENCE';
    v_item.client_id:=v_client;v_item.contract_id:=v_work.contract_id;v_item.physical_timesheet_id:=v_revision.physical_timesheet_id;
    v_item.week_ending_date:=v_work.week_ending_date;v_item.candidate_display_name:=v_metadata.candidate_sort_name;v_item.candidate_reference:=v_metadata.tms_ref;
    v_item.direction:='INFORMATIONAL';v_item.amount_known:=false;v_item.selected_amount_known:=false;v_item.is_selected:=false;v_item.selectable:=false;
    v_item.section:='BLOCKED';v_item.reason_code:=case when v_work.approval_state<>'APPROVED' then 'MISSING_APPROVAL' else 'DATA_UNAVAILABLE' end;
    v_item.reason_priority:=case when v_work.approval_state<>'APPROVED' then 21 else 10 end;v_item.query_state:='NOT_APPLICABLE';v_item.query_qualified:=false;
    v_item.financial_view_revision:=v_financial;v_item.projection_revision:=v_projection;v_item.metadata_version:=v_metadata.metadata_version;
    v_item.progress_state:='UNAVAILABLE';v_item.updated_at_utc:=transaction_timestamp();v_item.target_pay_channel:=v_target;
    v_zero:=coalesce(v_complete,false) and v_revision.certified_zero and v_revision.approved_source_ex_vat=0
      and v_revision.expected_line_count=0 and v_target is not null and v_work.week_ending_date is not null
      and extract(isodow from v_work.week_ending_date)=7
      and not exists(select 1 from private.bpay_next_position p where p.work_id=v_work.id);
    if v_zero then
      v_item.section:='READY';v_item.reason_code:='CERTIFIED_ZERO';v_item.reason_priority:=200;v_item.progress_state:='CURRENT';
      if v_work.week_ending_date>v_sunday then
        v_item.section:='WAITING';v_item.reason_code:='FUTURE_WEEK';v_item.reason_priority:=100;v_item.eligible_from_date:=v_work.week_ending_date-6;
      end if;
    end if;
    insert into private.bpay_next_workbench_item select (v_item).*;
  end if;
  update private.bpay_next_workbench_group g set ready_count=x.ready,action_count=x.action,blocked_count=x.blocked,waiting_count=x.waiting,
    complete=g.complete and x.unknown=0,certified_zero=coalesce(v_complete,false) and coalesce(v_revision.certified_zero,false)
    from(select count(*) filter(where section='READY') ready,count(*) filter(where section='ACTION') action,
      count(*) filter(where section='BLOCKED') blocked,count(*) filter(where section='WAITING') waiting,
      count(*) filter(where not amount_known and not (member_kind='GROUP_EVIDENCE' and coalesce(v_zero,false))) unknown
      from private.bpay_next_workbench_item where group_key=v_group) x where g.group_key=v_group;
  update private.bpay_next_workbench_group g set stale_snooze_count=x.n,
    stale_snooze_code=case when x.n>0 then 'SNOOZE_SEGMENT_STALE' end
    from(select count(*) n from private.bpay_next_snooze s where s.work_id=v_work.id and s.scope_kind='SEGMENT'
      and s.active and (s.until_date is null or s.until_date>=v_today) and not exists(select 1
        from private.bpay_next_approved_segment a where a.revision_id=v_revision.id and a.segment_key=s.segment_key)) x
    where g.group_key=v_group;
  select array_agg(d) into v_after from private.bpay_next_workbench_group_contribution_v1(v_group) d;
  perform private.bpay_next_workbench_apply_delta_v1(v_before,v_after);
end
$function$;

create or replace function private.bpay_next_workbench_refresh_case_v1(p_case_id uuid)
returns void language plpgsql security definer set search_path=pg_catalog,private
as $function$
declare
  v_case private.bpay_next_finance_case%rowtype;v_component private.bpay_next_case_component%rowtype;
  v_rule private.bpay_next_case_rule%rowtype;v_metadata private.bpay_next_workbench_candidate_metadata%rowtype;
  v_item private.bpay_next_workbench_item%rowtype;v_base private.bpay_next_workbench_item%rowtype;
  v_snooze private.bpay_next_snooze%rowtype;v_accepted record;v_totals record;
  v_before private.bpay_next_workbench_delta[];v_after private.bpay_next_workbench_delta[];
  v_group text:='CASE:'||p_case_id::text;v_candidate uuid;v_client uuid;v_contract uuid;v_target text;
  v_financial bigint;v_projection bigint;v_count integer:=0;v_accepted_count integer;v_capacity numeric;
  v_available numeric;v_reserved numeric;v_realised numeric;v_balance numeric;v_due date;v_basis bytea;
  v_today date:=(transaction_timestamp() at time zone 'Europe/London')::date;
begin
  perform private.bpay_next_workbench_require_next_v1();
  select c.candidate_id into strict v_candidate from private.bpay_next_finance_case c where c.id=p_case_id;
  select c.financial_view_revision into strict v_financial from private.bpay_next_worker_control c where c.candidate_id=v_candidate for update;
  select c.* into strict v_case from private.bpay_next_finance_case c where c.id=p_case_id for update;
  v_metadata:=private.bpay_next_workbench_ensure_metadata_v1(v_candidate);
  select c.pay_method::text into v_target from public.candidates c where c.id=v_candidate;
  if v_target not in ('PAYE','UMBRELLA') then v_target:=null;end if;
  select w.contract_id,co.client_id into v_contract,v_client from private.bpay_next_work_collection wc
    join private.bpay_next_work w on w.id=wc.work_id left join public.contracts co on co.id=w.contract_id where wc.case_id=p_case_id;
  select array_agg(d) into v_before from private.bpay_next_workbench_group_contribution_v1(v_group) d;
  update private.bpay_next_workbench_revision set workbench_revision=workbench_revision+1,updated_at_utc=transaction_timestamp()
    where id=1 returning workbench_revision into v_projection;
  delete from private.bpay_next_workbench_item where group_key=v_group;
  insert into private.bpay_next_workbench_group(group_key,group_kind,case_id,candidate_id,client_id,contract_id,
    target_pay_channel,complete,projection_revision,financial_view_revision,metadata_version,progress_state)
  values(v_group,'CASE',v_case.id,v_candidate,v_client,v_contract,v_target,true,v_projection,v_financial,v_metadata.metadata_version,'CURRENT')
  on conflict(group_key) do update set client_id=excluded.client_id,contract_id=excluded.contract_id,
    target_pay_channel=excluded.target_pay_channel,complete=true,projection_revision=excluded.projection_revision,
    financial_view_revision=excluded.financial_view_revision,metadata_version=excluded.metadata_version,
    progress_state='CURRENT',error_code=null,updated_at_utc=transaction_timestamp();
  for v_component in select c.* from private.bpay_next_case_component c where c.case_id=v_case.id order by c.component_ordinal,c.id loop
    v_count:=v_count+1;
    select r.* into strict v_rule from private.bpay_next_case_rule r where r.id=v_component.rule_id
      and r.case_id=v_case.id and r.candidate_id=v_candidate;
    if v_component.direction='DEDUCTION' then
      v_capacity:=case when v_component.case_kind in ('LOAN','ADVANCE') then v_component.funded_source_ex_vat else v_component.approved_source_ex_vat end;
      v_realised:=v_component.recovered_source_ex_vat;v_reserved:=v_component.active_recovery_source_ex_vat;
      v_balance:=v_capacity-v_component.written_off_source_ex_vat-v_realised;
      v_available:=v_balance-v_reserved;
    else
      v_capacity:=v_component.approved_source_ex_vat;
      v_realised:=v_component.funded_source_ex_vat+v_component.paid_credit_source_ex_vat;
      v_reserved:=v_component.active_payout_source_ex_vat;
      v_balance:=v_capacity-v_component.written_off_source_ex_vat-v_realised;
      v_available:=v_balance-v_reserved;
    end if;
    if v_available<0 or v_balance<0 then raise exception using errcode='23514',message='BPAY_NEXT_WORKBENCH_CASE_CAPACITY_INCONSISTENT';end if;
    if v_available=0 and v_reserved=0 then continue;end if;
    v_item:=null;v_item.id:=gen_random_uuid();v_item.group_key:=v_group;v_item.candidate_id:=v_candidate;
    v_item.case_id:=v_case.id;v_item.case_component_id:=v_component.id;v_item.rule_id:=v_rule.id;
    v_item.member_kind:='CASE_COMPONENT';v_item.member_key:=v_component.id::text;v_item.component_key:=v_component.component_key;
    v_item.client_id:=v_client;v_item.contract_id:=v_contract;v_item.candidate_display_name:=v_metadata.candidate_sort_name;v_item.candidate_reference:=v_metadata.tms_ref;
    v_item.component_kind:=v_component.case_kind;v_item.tax_treatment:=v_component.tax_treatment;
    v_item.source_pay_channel:=v_component.source_pay_channel;v_item.target_pay_channel:=v_target;
    v_item.direction:=v_component.direction;v_item.payroll_stage:=v_component.payroll_stage;
    v_item.amount_known:=true;v_item.nominal_source_ex_vat:=v_available;v_item.approved_source_ex_vat:=v_capacity;
    v_item.realised_source_ex_vat:=v_realised;v_item.active_reserved_source_ex_vat:=0;
    v_item.written_off_source_ex_vat:=v_component.written_off_source_ex_vat;
    v_item.residual_source_ex_vat:=v_available;v_item.gross_due_source_ex_vat:=case when v_component.direction='PAYMENT' then v_available else 0 end;
    v_item.included_source_ex_vat:=case when v_component.direction='PAYMENT' then v_available else 0 end;
    v_item.excluded_source_ex_vat:=0;v_item.recovery_source_ex_vat:=0;v_item.held_overhang_source_ex_vat:=0;
    v_item.deduction_balance_source_ex_vat:=case when v_component.direction='DEDUCTION' then v_balance else 0 end;
    v_item.section:='READY';v_item.reason_code:='READY';v_item.reason_priority:=200;v_item.selectable:=true;
    v_item.is_selected:=false;v_item.selected_amount_known:=false;v_item.query_qualified:=false;v_item.query_state:='NOT_APPLICABLE';
    v_item.financial_view_revision:=v_financial;v_item.projection_revision:=v_projection;v_item.metadata_version:=v_metadata.metadata_version;
    v_item.progress_state:='CURRENT';v_item.updated_at_utc:=transaction_timestamp();
    if v_target is null or v_case.current_rule_id is null then
      v_item.section:='BLOCKED';v_item.reason_code:='DATA_UNAVAILABLE';v_item.reason_priority:=10;
    end if;
    v_accepted_count:=0;
    for v_accepted in
      select r.id run_id,cmd.id command_id,cmd.module_epoch,ci.nominal_source_ex_vat
      from private.bpay_next_case_selection_item si
      join private.bpay_next_case_selection s on (s.run_id,s.candidate_id,s.selection_revision)=(si.run_id,si.candidate_id,si.selection_revision) and s.status='SEALED'
      join private.bpay_next_pay_run r on r.id=si.run_id and r.selection_state='SEALED' and r.status in ('PREPARING','REVIEW','DRAFT','EXECUTING')
      join private.bpay_next_run_command rc on rc.run_id=r.id join private.bpay_next_command cmd on cmd.id=rc.command_id and cmd.command_kind='PREPARE'
      left join private.bpay_next_run_worker rw on rw.run_id=r.id and rw.candidate_id=si.candidate_id
      left join private.bpay_next_run_case_instruction ci on ci.run_worker_id=rw.id and ci.preparation_revision=rw.preparation_revision
        and ci.case_component_id=si.case_component_id
      where si.case_component_id=v_component.id and si.is_selected
        and ((rw.id is not null and rw.status in ('PREPARING','REVIEW','READY','DRAFT','CANCELLING','ISSUED')
          and si.selection_revision=case when rw.case_selection_revision>0 then rw.case_selection_revision
            else (select max(cs.selection_revision::numeric)::bigint from private.bpay_next_case_selection cs
              where cs.run_id=r.id and cs.candidate_id=si.candidate_id and cs.status='SEALED') end)
          or (rw.id is null and si.selection_revision=(select max(cs.selection_revision::numeric)::bigint from private.bpay_next_case_selection cs
            where cs.run_id=r.id and cs.candidate_id=si.candidate_id and cs.status='SEALED')))
    loop
      v_accepted_count:=v_accepted_count+1;
      if v_accepted_count>1 then raise exception using errcode='23514',message='BPAY_NEXT_WORKBENCH_ACCEPTED_CASE_COLLISION';end if;
      v_item.is_selected:=true;v_item.accepted_run_id:=v_accepted.run_id;v_item.accepted_command_id:=v_accepted.command_id;
      v_item.accepted_module_epoch:=v_accepted.module_epoch;v_item.selected_source_ex_vat:=v_accepted.nominal_source_ex_vat;
      v_item.selected_amount_known:=v_accepted.nominal_source_ex_vat is not null;
      if v_item.reason_priority>50 then v_item.section:='WAITING';v_item.reason_code:='IN_PREPARATION';v_item.reason_priority:=50;end if;
    end loop;
    if v_item.reason_priority>82 and v_component.resolution_state='REVIEW' then
      v_item.section:='ACTION';v_item.reason_code:='CASE_REVIEW_REQUIRED';v_item.reason_priority:=82;
    elsif v_item.reason_priority>90 and v_case.status='PAUSED' then
      v_item.section:='WAITING';v_item.reason_code:='CASE_PAUSED';v_item.reason_priority:=90;
    elsif v_case.status in ('CLOSED','WRITTEN_OFF') and v_available>0 then
      v_item.section:='BLOCKED';v_item.reason_code:='DATA_UNAVAILABLE';v_item.reason_priority:=10;
    end if;
    v_due:=case when v_component.direction='DEDUCTION' then coalesce(v_rule.next_due_monday,v_case.due_date) else v_case.due_date end;
    if v_item.reason_priority>100 and v_due>v_today then
      v_item.section:='WAITING';v_item.reason_code:='FUTURE_WEEK';v_item.reason_priority:=100;v_item.eligible_from_date:=v_due;
    end if;
    v_basis:=sha256(convert_to(jsonb_build_array(v_candidate,v_component.case_id,v_component.id,
      v_component.case_kind,v_component.case_subtype,v_component.tax_treatment,v_component.instruction_kind,v_component.direction,
      v_component.payroll_stage,v_component.source_pay_channel,v_component.approved_source_ex_vat,v_component.funded_source_ex_vat,
      v_rule.minimum_earnings_threshold,v_rule.take_home_floor_override,v_rule.weekly_due_source_ex_vat,
      v_rule.schedule_start_monday,v_rule.next_due_monday,v_rule.schedule_week_count)::text,'UTF8'));
    select s.* into v_snooze from private.bpay_next_snooze s where s.case_component_id=v_component.id and s.scope_kind='CASE'
      and s.active and (s.until_date is null or s.until_date>=v_today) and s.worker_pay_basis=v_basis;
    if found then
      v_item.snooze_id:=v_snooze.id;v_item.snooze_version:=v_snooze.version;v_item.snooze_scope:=v_snooze.scope_kind;v_item.snooze_until_date:=v_snooze.until_date;
      if v_item.reason_priority>110 then v_item.section:='WAITING';v_item.reason_code:='SNOOZED';v_item.reason_priority:=110;end if;
      if v_component.direction='PAYMENT' then v_item.excluded_source_ex_vat:=v_item.included_source_ex_vat;v_item.included_source_ex_vat:=0;end if;
    elsif exists(select 1 from private.bpay_next_snooze s where s.case_component_id=v_component.id and s.scope_kind='CASE'
      and s.active and (s.until_date is null or s.until_date>=v_today)) then v_item.stale_snooze_code:='SNOOZE_BASIS_STALE';end if;
    v_item.selectable:=v_item.section='READY';v_base:=v_item;
    if v_available>0 then v_item.portion_kind:='AVAILABLE';insert into private.bpay_next_workbench_item select (v_item).*;end if;
    if v_reserved>0 then
      v_item:=v_base;v_item.id:=gen_random_uuid();v_item.portion_kind:='RESERVED';v_item.selectable:=false;
      v_item.nominal_source_ex_vat:=v_reserved;v_item.active_reserved_source_ex_vat:=v_reserved;
      v_item.gross_due_source_ex_vat:=0;v_item.included_source_ex_vat:=0;v_item.excluded_source_ex_vat:=0;v_item.residual_source_ex_vat:=0;
      if v_available>0 then
        v_item.approved_source_ex_vat:=0;v_item.realised_source_ex_vat:=0;v_item.written_off_source_ex_vat:=0;v_item.deduction_balance_source_ex_vat:=0;
        v_item.is_selected:=false;v_item.selected_amount_known:=false;v_item.selected_source_ex_vat:=null;
      end if;
      if v_item.reason_priority>111 then v_item.section:='WAITING';v_item.reason_code:='RESERVED';v_item.reason_priority:=111;end if;
      insert into private.bpay_next_workbench_item select (v_item).*;
    end if;
    select sum(i.approved_source_ex_vat) a,sum(i.realised_source_ex_vat) c,sum(i.active_reserved_source_ex_vat) h,
      sum(i.written_off_source_ex_vat) w,sum(i.residual_source_ex_vat) residual,
      sum(i.deduction_balance_source_ex_vat) debt into v_totals from private.bpay_next_workbench_item i
      where i.group_key=v_group and i.case_component_id=v_component.id;
    if (v_totals.a,v_totals.c,v_totals.h,v_totals.w,v_totals.residual,v_totals.debt)
      is distinct from (v_capacity,v_realised,v_reserved,v_component.written_off_source_ex_vat,v_available,
        case when v_component.direction='DEDUCTION' then v_balance else 0::numeric end) then
      raise exception using errcode='23514',message='BPAY_NEXT_WORKBENCH_CASE_MEMBER_NOT_CONSERVED';end if;
  end loop;
  if v_count=0 and v_case.status in ('OPEN','PAUSED') then
    v_item:=null;v_item.id:=gen_random_uuid();v_item.group_key:=v_group;v_item.candidate_id:=v_candidate;v_item.case_id:=v_case.id;
    v_item.member_kind:='GROUP_EVIDENCE';v_item.member_key:=v_case.id::text;v_item.component_key:='';v_item.portion_kind:='EVIDENCE';
    v_item.client_id:=v_client;v_item.contract_id:=v_contract;v_item.direction:='INFORMATIONAL';v_item.amount_known:=false;
    v_item.is_selected:=false;v_item.selected_amount_known:=false;v_item.selectable:=false;v_item.section:='BLOCKED';v_item.reason_code:='DATA_UNAVAILABLE';v_item.reason_priority:=10;
    v_item.target_pay_channel:=v_target;v_item.query_state:='NOT_APPLICABLE';v_item.query_qualified:=false;
    v_item.financial_view_revision:=v_financial;v_item.projection_revision:=v_projection;v_item.metadata_version:=v_metadata.metadata_version;
    v_item.progress_state:='UNAVAILABLE';v_item.updated_at_utc:=transaction_timestamp();insert into private.bpay_next_workbench_item select (v_item).*;
  end if;
  update private.bpay_next_workbench_group g set ready_count=x.ready,action_count=x.action,blocked_count=x.blocked,waiting_count=x.waiting,
    complete=x.unknown=0 from(select count(*) filter(where section='READY') ready,count(*) filter(where section='ACTION') action,
      count(*) filter(where section='BLOCKED') blocked,count(*) filter(where section='WAITING') waiting,count(*) filter(where not amount_known) unknown
      from private.bpay_next_workbench_item where group_key=v_group) x where g.group_key=v_group;
  select array_agg(d) into v_after from private.bpay_next_workbench_group_contribution_v1(v_group) d;
  perform private.bpay_next_workbench_apply_delta_v1(v_before,v_after);
end
$function$;

-- Exact bounded fanout prelocks every Candidate/owner/metadata before revision.
create or replace function private.bpay_next_workbench_refresh_many_v1(p_work_ids uuid[],p_case_ids uuid[])
returns void language plpgsql security definer set search_path=pg_catalog,private
as $function$
declare v_id uuid;v_candidate uuid;v_count bigint;
begin
  perform private.bpay_next_workbench_require_next_v1();
  if coalesce(cardinality(p_work_ids),0)+coalesce(cardinality(p_case_ids),0)>100
    or coalesce(pg_column_size(p_work_ids),0)+coalesce(pg_column_size(p_case_ids),0)>16384
    or array_position(p_work_ids,null::uuid) is not null or array_position(p_case_ids,null::uuid) is not null then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REFRESH_ENVELOPE_EXCEEDED';end if;
  if exists(select 1 from unnest(p_work_ids) x where not exists(select 1 from private.bpay_next_work w where w.id=x))
    or exists(select 1 from unnest(p_case_ids) x where not exists(select 1 from private.bpay_next_finance_case c where c.id=x)) then
    raise exception using errcode='23503',message='BPAY_NEXT_WORKBENCH_REFRESH_OWNER_MISSING';end if;
  -- Linked collection Cases are genuine related origins, never unrelated debt.
  select array_agg(x.id order by x.id) into p_case_ids from(select distinct id from unnest(p_case_ids) id
    union select wc.case_id from private.bpay_next_work_collection wc where wc.work_id=any(p_work_ids) and wc.case_id is not null
    order by id limit 101) x;
  select count(distinct id) into v_count from unnest(p_work_ids) id;
  if v_count+coalesce(cardinality(p_case_ids),0)>100 then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REFRESH_LINKED_ENVELOPE_EXCEEDED';end if;
  for v_candidate in select x.candidate_id from (
    select w.candidate_id from private.bpay_next_work w where w.id=any(p_work_ids)
    union select c.candidate_id from private.bpay_next_finance_case c where c.id=any(p_case_ids)) x order by x.candidate_id loop
    perform 1 from private.bpay_next_worker_control c where c.candidate_id=v_candidate for update;
    if not found then raise exception using errcode='23514',message='BPAY_NEXT_WORKBENCH_CANDIDATE_CONTROL_MISSING';end if;
  end loop;
  perform 1 from private.bpay_next_work w where w.id=any(p_work_ids) order by w.id for update;
  perform 1 from private.bpay_next_finance_case c where c.id=any(p_case_ids) order by c.id for update;
  for v_candidate in select x.candidate_id from (
    select w.candidate_id from private.bpay_next_work w where w.id=any(p_work_ids)
    union select c.candidate_id from private.bpay_next_finance_case c where c.id=any(p_case_ids)) x order by x.candidate_id loop
    perform private.bpay_next_workbench_ensure_metadata_v1(v_candidate);
  end loop;
  for v_id in select distinct x from unnest(p_work_ids) x order by x loop perform private.bpay_next_workbench_refresh_work_v1(v_id);end loop;
  for v_id in select distinct x from unnest(p_case_ids) x order by x loop perform private.bpay_next_workbench_refresh_case_v1(v_id);end loop;
end
$function$;

create or replace function private.bpay_next_workbench_set_bank_warning_v1(p_candidate_id uuid,p_warning_code text)
returns void language plpgsql security definer set search_path=pg_catalog,private
as $function$
declare v_metadata private.bpay_next_workbench_candidate_metadata%rowtype;
begin
  perform private.bpay_next_workbench_require_next_v1();
  if p_warning_code is not null and p_warning_code not in ('BANK_DETAILS_MISSING','BANK_DETAILS_CHANGED') then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_BANK_WARNING_INVALID';end if;
  perform 1 from private.bpay_next_worker_control c where c.candidate_id=p_candidate_id for update;
  if not found then raise exception using errcode='23514',message='BPAY_NEXT_WORKBENCH_CANDIDATE_CONTROL_MISSING';end if;
  v_metadata:=private.bpay_next_workbench_ensure_metadata_v1(p_candidate_id);
  if v_metadata.bank_warning_code is not distinct from p_warning_code then return;end if;
  update private.bpay_next_workbench_candidate_metadata set bank_warning_code=p_warning_code,
    metadata_version=metadata_version+1,continuation_pending=true,updated_at_utc=transaction_timestamp() where candidate_id=p_candidate_id;
  update private.bpay_next_workbench_revision set workbench_revision=workbench_revision+1,
    dirty_candidate_count=dirty_candidate_count+(case when v_metadata.continuation_pending then 0 else 1 end),
    updated_at_utc=transaction_timestamp() where id=1;
end
$function$;

create or replace function private.bpay_next_workbench_candidate_name_trigger_v1()
returns trigger language plpgsql security definer set search_path=pg_catalog,private
as $function$
declare v_metadata private.bpay_next_workbench_candidate_metadata%rowtype;v_name text;
begin
  if not exists(select 1 from private.bpay_next_module_control m where m.id=1 and m.active_owner='NEXT') then return new;end if;
  v_name:=coalesce(nullif(btrim(new.display_name),''),nullif(btrim(concat_ws(' ',new.first_name,new.last_name)),''),new.id::text);
  if v_name=coalesce(nullif(btrim(old.display_name),''),nullif(btrim(concat_ws(' ',old.first_name,old.last_name)),''),old.id::text)
    and new.tms_ref is not distinct from old.tms_ref then return new;end if;
  perform 1 from private.bpay_next_worker_control c where c.candidate_id=new.id for update;
  insert into private.bpay_next_workbench_candidate_metadata(candidate_id,candidate_sort_name,tms_ref)
    values(new.id,coalesce(nullif(btrim(old.display_name),''),nullif(btrim(concat_ws(' ',old.first_name,old.last_name)),''),old.id::text),old.tms_ref)
    on conflict do nothing;
  v_metadata:=private.bpay_next_workbench_ensure_metadata_v1(new.id);
  update private.bpay_next_workbench_candidate_metadata set candidate_sort_name=v_name,tms_ref=new.tms_ref,
    metadata_version=metadata_version+1,continuation_pending=true,updated_at_utc=transaction_timestamp() where candidate_id=new.id;
  update private.bpay_next_workbench_revision set workbench_revision=workbench_revision+1,
    dirty_candidate_count=dirty_candidate_count+(case when v_metadata.continuation_pending then 0 else 1 end),
    updated_at_utc=transaction_timestamp() where id=1;
  return new;
end
$function$;

drop trigger if exists bpay_next_workbench_candidate_name_trigger on public.candidates;
create trigger bpay_next_workbench_candidate_name_trigger after update of display_name,first_name,last_name,tms_ref
  on public.candidates for each row execute function private.bpay_next_workbench_candidate_name_trigger_v1();

alter function private.bpay_next_workbench_require_next_v1() owner to postgres;
revoke all on function private.bpay_next_workbench_require_next_v1() from public,anon,authenticated,service_role;

alter function private.bpay_next_workbench_ensure_metadata_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_workbench_ensure_metadata_v1(uuid) from public,anon,authenticated,service_role;

alter function private.bpay_next_workbench_group_contribution_v1(text) owner to postgres;
revoke all on function private.bpay_next_workbench_group_contribution_v1(text) from public,anon,authenticated,service_role;

alter function private.bpay_next_workbench_apply_delta_v1(private.bpay_next_workbench_delta[],private.bpay_next_workbench_delta[]) owner to postgres;
revoke all on function private.bpay_next_workbench_apply_delta_v1(private.bpay_next_workbench_delta[],private.bpay_next_workbench_delta[]) from public,anon,authenticated,service_role;

alter function private.bpay_next_workbench_accepted_work_v1(uuid,text,text) owner to postgres;
revoke all on function private.bpay_next_workbench_accepted_work_v1(uuid,text,text) from public,anon,authenticated,service_role;

alter function private.bpay_next_workbench_work_members_v1(uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_workbench_work_members_v1(uuid,uuid) from public,anon,authenticated,service_role;

alter function private.bpay_next_workbench_refresh_work_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_workbench_refresh_work_v1(uuid) from public,anon,authenticated,service_role;

alter function private.bpay_next_workbench_refresh_case_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_workbench_refresh_case_v1(uuid) from public,anon,authenticated,service_role;

alter function private.bpay_next_workbench_refresh_many_v1(uuid[],uuid[]) owner to postgres;
revoke all on function private.bpay_next_workbench_refresh_many_v1(uuid[],uuid[]) from public,anon,authenticated,service_role;

alter function private.bpay_next_workbench_set_bank_warning_v1(uuid,text) owner to postgres;
revoke all on function private.bpay_next_workbench_set_bank_warning_v1(uuid,text) from public,anon,authenticated,service_role;

alter function private.bpay_next_workbench_candidate_name_trigger_v1() owner to postgres;
revoke all on function private.bpay_next_workbench_candidate_name_trigger_v1() from public,anon,authenticated,service_role;

notify pgrst, 'reload schema';
commit;
