-- DR-37: exact current owner successors and bounded staged PAYE NET replacement.
\set ON_ERROR_STOP on
begin;

-- DR-37 guards
-- DR-37 private lifecycle admission; original ordered NET job remains the owner.
create or replace function private.bpay_next_net_replacement_job_assert_v1(p_replacement_id uuid)
returns void language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_r private.bpay_next_net_replacement%rowtype;
begin
 select dr37_point_1.* into strict v_r from private.bpay_next_net_replacement dr37_point_1 where id=p_replacement_id;
 if not exists(select 1 from private.bpay_next_job j
  join private.bpay_next_command c on c.id=j.command_id
  join private.bpay_next_worker_control w on w.candidate_id=j.candidate_id
  join private.bpay_next_module_control m on m.id=1
  where j.id=v_r.original_job_id and j.command_id=v_r.id and j.candidate_id=v_r.candidate_id
   and j.module_epoch=v_r.module_epoch and m.owner_epoch=v_r.module_epoch and m.active_owner='NEXT'
   and c.command_kind='PAYE_NET_ENTRY' and j.job_kind='PAYE_NET_ENTRY' and j.phase='FINANCE_ALLOCATE'
   and j.command_sequence=c.agency_sequence and j.status='LEASED' and j.lease_nonce is not null
   and j.owner_epoch=w.active_owner_epoch and j.lease_until_utc>pg_catalog.clock_timestamp()) then
  raise exception using errcode='55000',message='BPAY_NEXT_NET_REPLACEMENT_OWNER_STALE';end if;
end $function$;

create or replace function private.bpay_next_net_replacement_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_q private.bpay_next_paye_net_request%rowtype;v_d private.bpay_next_case_allocation_state%rowtype;
 v_w private.bpay_next_run_worker%rowtype;v_s private.bpay_next_case_allocation_state%rowtype;
begin
 if tg_op='DELETE' then raise exception using errcode='23514',message='BPAY_NEXT_NET_REPLACEMENT_DELETE_FORBIDDEN';end if;
 select dr37_point_2.* into strict v_q from private.bpay_next_paye_net_request dr37_point_2 where command_id=new.id;
 select dr37_point_3.* into strict v_d from private.bpay_next_case_allocation_state dr37_point_3 where id=new.draft_state_id;
 select dr37_point_4.* into strict v_w from private.bpay_next_run_worker dr37_point_4 where id=new.run_worker_id;
 if (new.run_worker_id,new.candidate_id,new.request_no,new.prior_projection_no,new.draft_state_id,new.preparation_revision,new.selection_revision)
  is distinct from (v_q.run_worker_id,v_q.candidate_id,v_q.request_no,v_q.expected_projection_revision,v_q.case_draft_state_id,v_q.preparation_revision,v_q.selection_revision)
  or new.run_id<>v_w.run_id or new.candidate_id<>v_w.candidate_id or v_d.pass_kind<>'DRAFT'
  or new.pay_week_start<>v_d.pay_week_start or new.expected_instruction_count<>v_d.expected_instruction_count
  or new.initial_channel_headroom<>coalesce(v_q.entered_paye_net,0)
  or not exists(select 1 from private.bpay_next_command c where c.id=new.id and c.command_kind='PAYE_NET_ENTRY' and c.module_epoch=new.module_epoch)
  or (new.prior_projection_id is not null and not exists(select 1 from private.bpay_next_net_projection p
   join private.bpay_next_case_allocation_state s on s.projection_id=p.id
   where p.id=new.prior_projection_id and p.run_worker_id=new.run_worker_id and p.projection_no=new.prior_projection_no
    and s.id=new.prior_state_id and s.pass_kind='NET' and s.status='COMPLETE' and s.net_stage='COMPLETE'
    and s.run_worker_id=new.run_worker_id and s.preparation_revision=new.preparation_revision and s.selection_revision=new.selection_revision)) then
  raise exception using errcode='23514',message='BPAY_NEXT_NET_REPLACEMENT_SCOPE_INVALID';end if;
 if tg_op='INSERT' then
  if new.stage<>'PRECHECK' or new.staging_step<>'WEEK' or new.original_job_id is not null or new.target_state_id is not null
   or new.prior_projection_no<>v_w.net_projection_revision or new.initial_take_home<>coalesce(v_q.entered_paye_net,0)
   or new.remaining_take_home<>new.initial_take_home or new.remaining_channel_headroom<>new.initial_channel_headroom
   or (new.staged_instruction_count,new.validated_instruction_count,new.committed_instruction_count,new.cleanup_instruction_count,
    new.staged_deduction_count,new.committed_deduction_count,new.staged_old_hold_count,new.proposed_hold_count,
    new.committed_old_hold_count,new.committed_new_hold_count,new.inventory_instruction_count,
    new.expected_releasable_hold_count,new.released_hold_count)
    is distinct from (0::bigint,0::bigint,0::bigint,0::bigint,0::bigint,0::bigint,0::bigint,0::bigint,0::bigint,0::bigint,0::bigint,0::bigint,0::bigint)
   or v_w.net_replacement_pending_id is not null or v_d.status<>'COMPLETE' or v_d.prepare_stage<>'COMPLETE'
   or (new.gross_additions,new.gross_deductions,new.net_additions,new.recoveries,new.staged_old_source_total,new.proposed_source_total,
    new.committed_old_source_total,new.committed_new_source_total,new.expected_releasable_source_total,new.released_source_total)
    is distinct from (0::numeric,0::numeric,0::numeric,0::numeric,0::numeric,0::numeric,0::numeric,0::numeric,0::numeric,0::numeric)
   or new.cursor_component_id is not null or new.validation_cursor<>0 or new.commit_cursor<>0
   or new.cleanup_component_id is not null or new.inventory_component_id is not null or new.cleanup_step<>'INVENTORY'
   or new.metadata_run_cursor is not null or new.validation_run_cursor is not null or new.validation_step<>'INSTRUCTIONS'
   or new.cancel_command_id is not null or new.accepted_projection_id is not null or new.cash_amount is not null
   or new.finished_at_utc is not null then
   raise exception using errcode='23514',message='BPAY_NEXT_NET_REPLACEMENT_INITIAL_INVALID';end if;
 else
  if old.stage in ('COMPLETE','REFUSED','CANCELLED')
   or (new.id,new.run_worker_id,new.run_id,new.candidate_id,new.pay_week_start,new.request_no,new.prior_projection_no,
     new.prior_projection_id,new.draft_state_id,new.prior_state_id,new.module_epoch,new.preparation_revision,new.selection_revision,
     new.expected_instruction_count,new.initial_channel_headroom,new.created_at_utc)
    is distinct from (old.id,old.run_worker_id,old.run_id,old.candidate_id,old.pay_week_start,old.request_no,old.prior_projection_no,
     old.prior_projection_id,old.draft_state_id,old.prior_state_id,old.module_epoch,old.preparation_revision,old.selection_revision,
     old.expected_instruction_count,old.initial_channel_headroom,old.created_at_utc)
   or (old.original_job_id is not null and (new.original_job_id,new.target_state_id) is distinct from (old.original_job_id,old.target_state_id))
   or (old.cancel_command_id is not null and new.cancel_command_id is distinct from old.cancel_command_id)
   or new.staged_instruction_count<old.staged_instruction_count or new.validated_instruction_count<old.validated_instruction_count
   or new.committed_instruction_count<old.committed_instruction_count or new.cleanup_instruction_count<old.cleanup_instruction_count
   or new.inventory_instruction_count<old.inventory_instruction_count
   or new.validation_cursor<old.validation_cursor or new.commit_cursor<old.commit_cursor
   or new.staged_deduction_count<old.staged_deduction_count or new.committed_deduction_count<old.committed_deduction_count
   or new.staged_old_hold_count<old.staged_old_hold_count or new.proposed_hold_count<old.proposed_hold_count
   or new.committed_old_hold_count<old.committed_old_hold_count or new.committed_new_hold_count<old.committed_new_hold_count
   or new.expected_releasable_hold_count<old.expected_releasable_hold_count or new.released_hold_count<old.released_hold_count
   or new.staged_old_source_total<old.staged_old_source_total or new.proposed_source_total<old.proposed_source_total
   or new.committed_old_source_total<old.committed_old_source_total or new.committed_new_source_total<old.committed_new_source_total
   or new.expected_releasable_source_total<old.expected_releasable_source_total or new.released_source_total<old.released_source_total
   or new.gross_additions<old.gross_additions or new.gross_deductions<old.gross_deductions
   or new.net_additions<old.net_additions or new.recoveries<old.recoveries
   or new.initial_take_home<old.initial_take_home or (old.stage<>'PRECHECK' and new.initial_take_home<>old.initial_take_home)
   or new.updated_at_utc<old.updated_at_utc
   or (old.cursor_component_id is not null and (new.cursor_component_id is null or
    (new.cursor_age_key,new.cursor_case_id,new.cursor_component_ordinal,new.cursor_component_id)
     <(old.cursor_age_key,old.cursor_case_id,old.cursor_component_ordinal,old.cursor_component_id)))
   or (old.cleanup_component_id is not null and (new.cleanup_component_id is null or
    (new.cleanup_age_key,new.cleanup_case_id,new.cleanup_component_ordinal,new.cleanup_component_id)
     <(old.cleanup_age_key,old.cleanup_case_id,old.cleanup_component_ordinal,old.cleanup_component_id)))
   or (old.inventory_component_id is not null and (new.inventory_component_id is null or
    (new.inventory_age_key,new.inventory_case_id,new.inventory_component_ordinal,new.inventory_component_id)
     <(old.inventory_age_key,old.inventory_case_id,old.inventory_component_ordinal,old.inventory_component_id)))
   or (old.metadata_run_cursor is not null and (new.metadata_run_cursor is null or
    (new.metadata_pay_date_cursor,new.metadata_created_at_cursor,new.metadata_run_cursor)
     <(old.metadata_pay_date_cursor,old.metadata_created_at_cursor,old.metadata_run_cursor)))
   or (old.validation_run_cursor is not null and (new.validation_run_cursor is null or
    (new.validation_pay_date_cursor,new.validation_created_at_cursor,new.validation_run_cursor)
     <(old.validation_pay_date_cursor,old.validation_created_at_cursor,old.validation_run_cursor)))
   or not(case old.validation_step when 'INSTRUCTIONS' then new.validation_step in ('INSTRUCTIONS','PRIOR_RUNS','DONE')
    when 'PRIOR_RUNS' then new.validation_step in ('PRIOR_RUNS','DONE') when 'DONE' then new.validation_step='DONE' else false end)
   or not(case old.staging_step when 'WEEK' then new.staging_step in ('WEEK','INSTRUCTIONS')
     when 'INSTRUCTIONS' then new.staging_step in ('INSTRUCTIONS','VALIDATE')
     when 'VALIDATE' then new.staging_step in ('VALIDATE','DONE') when 'DONE' then new.staging_step='DONE' else false end)
   or not(case old.cleanup_step when 'INVENTORY' then new.cleanup_step in ('INVENTORY','RELEASE')
     when 'RELEASE' then new.cleanup_step in ('RELEASE','DONE') when 'DONE' then new.cleanup_step='DONE' else false end)
   or not(case old.stage
    when 'PRECHECK' then new.stage in ('PRECHECK','STAGING','REFUSED','CANCEL_REQUESTED','CANCELLING')
    when 'STAGING' then new.stage in ('STAGING','COMMITTING','REFUSED','CANCEL_REQUESTED','CANCELLING')
    when 'COMMITTING' then new.stage in ('COMMITTING','COMPLETE','CANCEL_REQUESTED','CANCELLING')
    when 'CANCEL_REQUESTED' then new.stage in ('CANCEL_REQUESTED','CANCELLING')
    when 'CANCELLING' then new.stage in ('CANCELLING','CANCELLED') else false end) then
   raise exception using errcode='23514',message='BPAY_NEXT_NET_REPLACEMENT_TRANSITION_INVALID';end if;
  -- Only the pass that owns these facts may advance them, including its final page.
  if (old.stage<>'STAGING' or old.staging_step<>'INSTRUCTIONS') and
   (new.staged_instruction_count,new.staged_deduction_count,new.staged_old_hold_count,new.staged_old_source_total,
    new.proposed_hold_count,new.proposed_source_total,new.gross_additions,new.gross_deductions,new.net_additions,new.recoveries,
    new.remaining_channel_headroom,new.cursor_age_key,new.cursor_case_id,new.cursor_component_ordinal,new.cursor_component_id)
   is distinct from (old.staged_instruction_count,old.staged_deduction_count,old.staged_old_hold_count,old.staged_old_source_total,
    old.proposed_hold_count,old.proposed_source_total,old.gross_additions,old.gross_deductions,old.net_additions,old.recoveries,
    old.remaining_channel_headroom,old.cursor_age_key,old.cursor_case_id,old.cursor_component_ordinal,old.cursor_component_id) then
   raise exception using errcode='23514',message='BPAY_NEXT_NET_REPLACEMENT_STAGED_FACTS_FROZEN';end if;
  if (old.stage<>'STAGING' or old.staging_step<>'VALIDATE') and
   (new.validated_instruction_count,new.validation_cursor,new.validation_step,new.validation_pay_date_cursor,new.validation_created_at_cursor,new.validation_run_cursor)
   is distinct from (old.validated_instruction_count,old.validation_cursor,old.validation_step,old.validation_pay_date_cursor,old.validation_created_at_cursor,old.validation_run_cursor) then
   raise exception using errcode='23514',message='BPAY_NEXT_NET_REPLACEMENT_VALIDATION_FACTS_FROZEN';end if;
  if old.stage<>'COMMITTING' and
   (new.committed_instruction_count,new.commit_cursor,new.committed_deduction_count,new.committed_old_hold_count,new.committed_old_source_total,
    new.committed_new_hold_count,new.committed_new_source_total,new.accepted_projection_id,new.cash_amount)
   is distinct from (old.committed_instruction_count,old.commit_cursor,old.committed_deduction_count,old.committed_old_hold_count,old.committed_old_source_total,
    old.committed_new_hold_count,old.committed_new_source_total,old.accepted_projection_id,old.cash_amount) then
   raise exception using errcode='23514',message='BPAY_NEXT_NET_REPLACEMENT_COMMIT_FACTS_FROZEN';end if;
  if (old.stage<>'CANCELLING' or old.cleanup_step<>'INVENTORY') and
   (new.inventory_instruction_count,new.expected_releasable_hold_count,new.expected_releasable_source_total,
    new.inventory_age_key,new.inventory_case_id,new.inventory_component_ordinal,new.inventory_component_id)
   is distinct from (old.inventory_instruction_count,old.expected_releasable_hold_count,old.expected_releasable_source_total,
    old.inventory_age_key,old.inventory_case_id,old.inventory_component_ordinal,old.inventory_component_id) then
   raise exception using errcode='23514',message='BPAY_NEXT_NET_REPLACEMENT_INVENTORY_FACTS_FROZEN';end if;
  if (old.stage<>'CANCELLING' or old.cleanup_step<>'RELEASE') and
   (new.cleanup_instruction_count,new.released_hold_count,new.released_source_total,
    new.cleanup_age_key,new.cleanup_case_id,new.cleanup_component_ordinal,new.cleanup_component_id)
   is distinct from (old.cleanup_instruction_count,old.released_hold_count,old.released_source_total,
    old.cleanup_age_key,old.cleanup_case_id,old.cleanup_component_ordinal,old.cleanup_component_id) then
   raise exception using errcode='23514',message='BPAY_NEXT_NET_REPLACEMENT_CLEANUP_FACTS_FROZEN';end if;
  if (old.stage<>'PRECHECK' and (old.stage<>'STAGING' or old.staging_step<>'INSTRUCTIONS')) and
    new.remaining_take_home is distinct from old.remaining_take_home then
   raise exception using errcode='23514',message='BPAY_NEXT_NET_REPLACEMENT_TAKE_HOME_FROZEN';end if;
  if old.stage='STAGING' and old.staging_step='INSTRUCTIONS' and
   (new.remaining_take_home>old.remaining_take_home or new.remaining_channel_headroom>old.remaining_channel_headroom) then
   raise exception using errcode='23514',message='BPAY_NEXT_NET_REPLACEMENT_CARRY_INCREASE_FORBIDDEN';end if;
  if old.stage<>'PRECHECK' and (new.metadata_pay_date_cursor,new.metadata_created_at_cursor,new.metadata_run_cursor)
    is distinct from (old.metadata_pay_date_cursor,old.metadata_created_at_cursor,old.metadata_run_cursor) then
   raise exception using errcode='23514',message='BPAY_NEXT_NET_REPLACEMENT_PRECHECK_FACTS_FROZEN';end if;
  if old.original_job_id is null then
   if new.original_job_id is null or new.target_state_id is null or not exists(select 1 from private.bpay_next_job j
    join private.bpay_next_worker_control c on c.candidate_id=j.candidate_id
    join private.bpay_next_module_control m on m.id=1 and m.active_owner='NEXT' and m.owner_epoch=new.module_epoch
    join private.bpay_next_command n on n.id=j.command_id and n.command_kind='PAYE_NET_ENTRY' and n.module_epoch=new.module_epoch
    where j.id=new.original_job_id and j.command_id=new.id and j.candidate_id=new.candidate_id and j.module_epoch=new.module_epoch
     and j.job_kind='PAYE_NET_ENTRY' and j.phase='FINANCE_ALLOCATE' and j.status='LEASED'
     and j.command_sequence=n.agency_sequence and j.owner_epoch=c.active_owner_epoch
     and j.lease_nonce is not null and j.lease_until_utc>pg_catalog.clock_timestamp()) then
    raise exception using errcode='55000',message='BPAY_NEXT_NET_REPLACEMENT_OWNER_STALE';end if;
  else perform private.bpay_next_net_replacement_job_assert_v1(old.id);end if;
  if new.cancel_command_id is not null and not exists(select 1 from private.bpay_next_cancel_request q
   join private.bpay_next_command c on c.id=q.command_id join private.bpay_next_command n on n.id=new.id
   where q.command_id=new.cancel_command_id and q.run_worker_id=new.run_worker_id and q.candidate_id=new.candidate_id
    and q.status='REQUESTED' and c.command_kind='SIMPLE_CANCEL' and c.module_epoch=new.module_epoch
    and c.agency_sequence>n.agency_sequence) then
   raise exception using errcode='23514',message='BPAY_NEXT_NET_REPLACEMENT_CANCEL_IDENTITY_INVALID';end if;
  if v_w.net_replacement_pending_id is distinct from new.id then
   raise exception using errcode='23514',message='BPAY_NEXT_NET_REPLACEMENT_PENDING_IDENTITY_INVALID';end if;
 end if;
  if new.stage='COMMITTING' and old.stage<>'COMMITTING' and
   (old.stage<>'STAGING' or old.staging_step<>'VALIDATE' or new.staging_step<>'DONE' or new.validation_step<>'DONE'
    or new.staged_instruction_count<>new.expected_instruction_count or new.validated_instruction_count<>new.expected_instruction_count
    or new.committed_instruction_count<>0 or not exists(select 1 from private.bpay_next_candidate_week_commit_fence f
     where f.replacement_id=new.id and f.candidate_id=new.candidate_id and f.pay_week_start=new.pay_week_start and f.stage='STAGING')) then
   raise exception using errcode='23514',message='BPAY_NEXT_NET_REPLACEMENT_COMMIT_ADMISSION_INVALID';end if;
  if new.stage='COMPLETE' and not exists(select 1 from private.bpay_next_net_projection p
   join private.bpay_next_case_allocation_state s on s.projection_id=p.id
   where p.id=new.accepted_projection_id and p.run_worker_id=new.run_worker_id and p.request_command_id=new.id
    and p.projection_no=new.request_no and p.cash_amount=new.cash_amount and s.id=new.target_state_id
    and s.status='COMPLETE' and s.net_stage='COMPLETE' and s.processed_instruction_count=new.expected_instruction_count)
   then raise exception using errcode='23514',message='BPAY_NEXT_NET_REPLACEMENT_COMPLETION_INVALID';end if;
 if new.target_state_id is not null then
  select dr37_point_5.* into strict v_s from private.bpay_next_case_allocation_state dr37_point_5 where id=new.target_state_id;
  if (v_s.run_worker_id,v_s.run_id,v_s.candidate_id,v_s.command_id,v_s.job_id,v_s.module_epoch,v_s.preparation_revision,v_s.selection_revision,v_s.projection_no,v_s.pay_week_start,v_s.pass_kind)
   is distinct from (new.run_worker_id,new.run_id,new.candidate_id,new.id,new.original_job_id,new.module_epoch,new.preparation_revision,new.selection_revision,new.request_no,new.pay_week_start,'NET'::text) then
   raise exception using errcode='23514',message='BPAY_NEXT_NET_REPLACEMENT_TARGET_INVALID';end if;
 end if;
 return new;
end $function$;

create or replace function private.bpay_next_net_staged_result_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_r private.bpay_next_net_replacement%rowtype;v_i private.bpay_next_run_case_instruction%rowtype;
 v_d private.bpay_next_case_allocation_result%rowtype;v_h private.bpay_next_case_hold%rowtype;
begin
 if tg_op<>'INSERT' then raise exception using errcode='23514',message='BPAY_NEXT_NET_STAGED_RESULT_IMMUTABLE';end if;
 select dr37_point_6.* into strict v_r from private.bpay_next_net_replacement dr37_point_6 where id=new.replacement_id;
 perform private.bpay_next_net_replacement_job_assert_v1(v_r.id);
 select dr37_point_7.* into strict v_i from private.bpay_next_run_case_instruction dr37_point_7 where id=new.instruction_id;
 select dr37_point_8.* into strict v_d from private.bpay_next_case_allocation_result dr37_point_8 where id=new.draft_result_id;
 if v_r.stage<>'STAGING' or v_r.staging_step<>'INSTRUCTIONS' or new.state_id is distinct from v_r.target_state_id
  or (new.run_worker_id,new.candidate_id,new.preparation_revision,new.selection_revision,new.case_id,new.case_component_id,new.pay_week_start,new.payroll_stage,new.hold_purpose,new.allocation_channel)
   is distinct from (v_i.run_worker_id,v_i.candidate_id,v_i.preparation_revision,v_i.selection_revision,v_i.case_id,v_i.case_component_id,v_i.pay_week_start,v_i.payroll_stage,v_i.hold_purpose,v_i.allocation_channel)
  or (new.run_worker_id,new.candidate_id,new.preparation_revision,new.selection_revision,new.pass_kind)
   is distinct from (v_r.run_worker_id,v_r.candidate_id,v_r.preparation_revision,v_r.selection_revision,'NET'::text)
  or v_d.state_id<>v_r.draft_state_id or v_d.instruction_id<>v_i.id
  or new.nominal_target_ex_vat<>v_i.nominal_target_ex_vat
  or new.allocated_source_ex_vat>v_d.allocated_source_ex_vat
  or new.allocated_target_ex_vat>v_d.allocated_target_ex_vat or new.allocated_target_vat<>0
  or new.allocated_source_ex_vat<>new.allocated_target_ex_vat
  or new.result_no<=v_r.staged_instruction_count or new.result_no>v_r.staged_instruction_count+100
  or (new.payroll_stage<>'NET_DEDUCT' and (new.nominal_target_ex_vat,new.usable_capacity_target_ex_vat,
   new.allocated_source_ex_vat,new.allocated_target_ex_vat,new.allocated_target_vat,new.allocated_target_inc_vat,
   new.shortfall_target_ex_vat,new.cap_reason,new.affordability_reason,new.capacity_reason,
   new.channel_headroom_before,new.channel_headroom_after,new.worker_take_home_before,new.worker_take_home_after)
   is distinct from (v_d.nominal_target_ex_vat,v_d.usable_capacity_target_ex_vat,
   v_d.allocated_source_ex_vat,v_d.allocated_target_ex_vat,v_d.allocated_target_vat,v_d.allocated_target_inc_vat,
   v_d.shortfall_target_ex_vat,v_d.cap_reason,v_d.affordability_reason,v_d.capacity_reason,
   v_d.channel_headroom_before,v_d.channel_headroom_after,v_d.worker_take_home_before,v_d.worker_take_home_after))
  or not exists(select 1 from private.bpay_next_finance_case c where c.id=new.case_id and c.candidate_id=new.candidate_id and c.case_revision=new.captured_case_revision)
  or not exists(select 1 from private.bpay_next_case_component c where c.id=new.case_component_id and c.candidate_id=new.candidate_id and c.component_revision=new.captured_component_revision)
  or not exists(select 1 from private.bpay_next_case_period p where p.case_component_id=new.case_component_id and p.pay_week_start=new.pay_week_start and p.period_revision=new.captured_period_revision) then
  raise exception using errcode='23514',message='BPAY_NEXT_NET_STAGED_RESULT_IDENTITY_INVALID';end if;
 if new.prior_hold_id is not null then
  select dr37_point_9.* into strict v_h from private.bpay_next_case_hold dr37_point_9 where id=new.prior_hold_id;
  if v_h.status<>'ACTIVE' or v_h.run_worker_id<>v_r.run_worker_id or v_h.instruction_id<>new.instruction_id
   or v_h.source_reserved_ex_vat<>new.prior_source_amount or v_h.target_amount_ex_vat<>new.prior_source_amount or v_h.target_amount_vat<>0
   or not exists(select 1 from private.bpay_next_case_capacity_use u where u.case_hold_id=v_h.id and u.status='ACTIVE' and u.source_amount_ex_vat=new.prior_source_amount)
   or not exists(select 1 from private.bpay_next_case_allocation_result a where a.id=v_h.allocation_result_id
    and a.state_id in (v_r.draft_state_id,v_r.prior_state_id) and a.instruction_id=new.instruction_id) then
   raise exception using errcode='23514',message='BPAY_NEXT_NET_STAGED_OWN_HOLD_INVALID';end if;
 elsif exists(select 1 from private.bpay_next_case_hold h where h.instruction_id=new.instruction_id and h.status='ACTIVE') then
  raise exception using errcode='23514',message='BPAY_NEXT_NET_STAGED_OWN_HOLD_INVALID';end if;
 return new;
end $function$;

create or replace function private.bpay_next_net_staged_capacity_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_r private.bpay_next_net_replacement%rowtype;v_c private.bpay_next_finance_case%rowtype;
begin
 if tg_op='DELETE' then raise exception using errcode='23514',message='BPAY_NEXT_NET_STAGED_CAPACITY_DELETE_FORBIDDEN';end if;
 select dr37_point_10.* into strict v_r from private.bpay_next_net_replacement dr37_point_10 where id=new.replacement_id;
 select dr37_point_11.* into strict v_c from private.bpay_next_finance_case dr37_point_11 where id=new.case_id;
 perform private.bpay_next_net_replacement_job_assert_v1(v_r.id);
 if v_c.candidate_id<>v_r.candidate_id or v_r.stage not in ('STAGING','COMMITTING') then
  raise exception using errcode='23514',message='BPAY_NEXT_NET_STAGED_CAPACITY_SCOPE_INVALID';end if;
 if tg_op='INSERT' then
  if v_r.stage<>'STAGING' or v_r.staging_step<>'INSTRUCTIONS' or new.captured_case_revision<>v_c.case_revision
   or new.captured_active_recovery_source<>v_c.active_recovery_hold_amount
   or new.captured_outstanding_source<>(case when v_c.case_kind in ('LOAN','ADVANCE') then v_c.principal_funded else v_c.principal_approved end)-v_c.principal_recovered-v_c.principal_written_off
   or new.proposed_release_source<>0 or new.proposed_reserve_source<>0 or new.staged_instruction_count<>0
   or new.committed_instruction_count<>0 or new.committed_release_source<>0 or new.committed_reserve_source<>0 then
   raise exception using errcode='23514',message='BPAY_NEXT_NET_STAGED_CAPACITY_INITIAL_INVALID';end if;
 elsif (new.replacement_id,new.case_id,new.captured_case_revision,new.captured_outstanding_source,new.captured_active_recovery_source)
  is distinct from (old.replacement_id,old.case_id,old.captured_case_revision,old.captured_outstanding_source,old.captured_active_recovery_source)
  or new.staged_instruction_count<old.staged_instruction_count or new.committed_instruction_count<old.committed_instruction_count
  or new.proposed_release_source<old.proposed_release_source or new.proposed_reserve_source<old.proposed_reserve_source
  or new.committed_release_source<old.committed_release_source or new.committed_reserve_source<old.committed_reserve_source
  or (v_r.stage='STAGING' and (new.committed_instruction_count,new.committed_release_source,new.committed_reserve_source)
   is distinct from (old.committed_instruction_count,old.committed_release_source,old.committed_reserve_source))
  or ((v_r.stage='COMMITTING' or v_r.staging_step<>'INSTRUCTIONS') and (new.staged_instruction_count,new.proposed_release_source,new.proposed_reserve_source)
   is distinct from (old.staged_instruction_count,old.proposed_release_source,old.proposed_reserve_source)) then
  raise exception using errcode='23514',message='BPAY_NEXT_NET_STAGED_CAPACITY_TRANSITION_INVALID';end if;
 return new;
end $function$;

create or replace function private.bpay_next_net_refusal_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_r private.bpay_next_net_replacement%rowtype;
begin
 if tg_op<>'INSERT' then raise exception using errcode='23514',message='BPAY_NEXT_NET_REFUSAL_IMMUTABLE';end if;
 select dr37_point_12.* into strict v_r from private.bpay_next_net_replacement dr37_point_12 where id=new.command_id;
 perform private.bpay_next_net_replacement_job_assert_v1(v_r.id);
 if v_r.stage<>'REFUSED' or (new.run_worker_id,new.candidate_id,new.request_no,new.refusal_code)
  is distinct from (v_r.run_worker_id,v_r.candidate_id,v_r.request_no,v_r.refusal_code)
  or v_r.committed_instruction_count<>0
  or not exists(select 1 from private.bpay_next_run_worker w where w.id=new.run_worker_id and w.status='READY'
   and w.net_request_revision=w.net_projection_revision and w.net_replacement_pending_id is null) then
  raise exception using errcode='23514',message='BPAY_NEXT_NET_REFUSAL_SCOPE_INVALID';end if;
 return new;
end $function$;

create or replace function private.bpay_next_net_cancel_hold_member_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_r private.bpay_next_net_replacement%rowtype;v_h private.bpay_next_case_hold%rowtype;
begin
 if tg_op<>'INSERT' then raise exception using errcode='23514',message='BPAY_NEXT_NET_CANCEL_MEMBERSHIP_IMMUTABLE';end if;
 select dr37_point_13.* into strict v_r from private.bpay_next_net_replacement dr37_point_13 where id=new.replacement_id;
 select dr37_point_14.* into strict v_h from private.bpay_next_case_hold dr37_point_14 where id=new.case_hold_id;
 perform private.bpay_next_net_replacement_job_assert_v1(v_r.id);
 if v_r.stage<>'CANCELLING' or v_r.cleanup_step<>'INVENTORY' or v_h.status<>'ACTIVE'
  or v_h.run_worker_id<>v_r.run_worker_id or v_h.candidate_id<>v_r.candidate_id or v_h.instruction_id<>new.instruction_id
  or v_h.source_reserved_ex_vat<>new.source_amount or new.allocation_state_id not in (v_r.draft_state_id,v_r.target_state_id)
    and new.allocation_state_id is distinct from v_r.prior_state_id
  or not exists(select 1 from private.bpay_next_case_allocation_result a where a.id=v_h.allocation_result_id
   and a.state_id=new.allocation_state_id and a.instruction_id=new.instruction_id)
  or not exists(select 1 from private.bpay_next_case_capacity_use u where u.case_hold_id=v_h.id and u.status='ACTIVE' and u.source_amount_ex_vat=new.source_amount) then
  raise exception using errcode='23514',message='BPAY_NEXT_NET_CANCEL_MEMBERSHIP_INVALID';end if;
 return new;
end $function$;

create or replace function private.bpay_next_net_cancel_handoff_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_r private.bpay_next_net_replacement%rowtype;
begin
 if tg_op<>'INSERT' then raise exception using errcode='23514',message='BPAY_NEXT_NET_CANCEL_HANDOFF_IMMUTABLE';end if;
 select dr37_point_15.* into strict v_r from private.bpay_next_net_replacement dr37_point_15 where id=new.replacement_id;
 perform private.bpay_next_net_replacement_job_assert_v1(v_r.id);
 if v_r.stage<>'CANCELLED' or v_r.cleanup_step<>'DONE'
  or (new.cancel_command_id,new.original_job_id,new.run_worker_id,new.candidate_id,new.draft_state_id,new.prior_state_id,new.target_state_id,
   new.prior_projection_id,new.target_projection_id,new.expected_instruction_count,new.cleaned_instruction_count,new.expected_hold_count,
   new.released_hold_count,new.expected_source_total,new.released_source_total,
   new.cleanup_age_key,new.cleanup_case_id,new.cleanup_component_ordinal,new.cleanup_component_id)
  is distinct from (v_r.cancel_command_id,v_r.original_job_id,v_r.run_worker_id,v_r.candidate_id,v_r.draft_state_id,v_r.prior_state_id,v_r.target_state_id,
   v_r.prior_projection_id,v_r.accepted_projection_id,v_r.expected_instruction_count,v_r.cleanup_instruction_count,v_r.expected_releasable_hold_count,
   v_r.released_hold_count,v_r.expected_releasable_source_total,v_r.released_source_total,
   v_r.cleanup_age_key,v_r.cleanup_case_id,v_r.cleanup_component_ordinal,v_r.cleanup_component_id)
  or not exists(select 1 from private.bpay_next_cancel_request q join private.bpay_next_command c on c.id=q.command_id
   where q.command_id=new.cancel_command_id and q.run_worker_id=new.run_worker_id and q.candidate_id=new.candidate_id
    and q.status='REQUESTED' and c.command_kind='SIMPLE_CANCEL') then
  raise exception using errcode='23514',message='BPAY_NEXT_NET_CANCEL_HANDOFF_INVALID';end if;
 return new;
end $function$;

drop trigger if exists bpay_next_net_replacement_guard on private.bpay_next_net_replacement;
create trigger bpay_next_net_replacement_guard before insert or update or delete on private.bpay_next_net_replacement
 for each row execute function private.bpay_next_net_replacement_guard_v1();
drop trigger if exists bpay_next_net_staged_result_guard on private.bpay_next_net_staged_result;
create trigger bpay_next_net_staged_result_guard before insert or update or delete on private.bpay_next_net_staged_result
 for each row execute function private.bpay_next_net_staged_result_guard_v1();
drop trigger if exists bpay_next_net_staged_capacity_guard on private.bpay_next_net_staged_case_capacity;
create trigger bpay_next_net_staged_capacity_guard before insert or update or delete on private.bpay_next_net_staged_case_capacity
 for each row execute function private.bpay_next_net_staged_capacity_guard_v1();
drop trigger if exists bpay_next_net_refusal_guard on private.bpay_next_paye_net_request_refusal;
create trigger bpay_next_net_refusal_guard before insert or update or delete on private.bpay_next_paye_net_request_refusal
 for each row execute function private.bpay_next_net_refusal_guard_v1();
drop trigger if exists bpay_next_net_cancel_hold_member_guard on private.bpay_next_net_cancel_hold_member;
create trigger bpay_next_net_cancel_hold_member_guard before insert or update or delete on private.bpay_next_net_cancel_hold_member
 for each row execute function private.bpay_next_net_cancel_hold_member_guard_v1();
drop trigger if exists bpay_next_net_cancel_handoff_guard on private.bpay_next_net_cancel_handoff;
create trigger bpay_next_net_cancel_handoff_guard before insert or update or delete on private.bpay_next_net_cancel_handoff
 for each row execute function private.bpay_next_net_cancel_handoff_guard_v1();

create or replace function private.bpay_next_net_replacement_truncate_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog
as $function$
begin raise exception using errcode='23514',message='BPAY_NEXT_NET_REPLACEMENT_TRUNCATE_FORBIDDEN';end
$function$;
drop trigger if exists bpay_next_net_replacement_truncate_guard on private.bpay_next_net_replacement;
drop trigger if exists bpay_next_net_replacement_truncate_guard on private.bpay_next_net_replacement;
create trigger bpay_next_net_replacement_truncate_guard before truncate on private.bpay_next_net_replacement
 for each statement execute function private.bpay_next_net_replacement_truncate_guard_v1();
drop trigger if exists bpay_next_net_replacement_truncate_guard on private.bpay_next_net_staged_result;
drop trigger if exists bpay_next_net_replacement_truncate_guard on private.bpay_next_net_staged_result;
create trigger bpay_next_net_replacement_truncate_guard before truncate on private.bpay_next_net_staged_result
 for each statement execute function private.bpay_next_net_replacement_truncate_guard_v1();
drop trigger if exists bpay_next_net_replacement_truncate_guard on private.bpay_next_net_staged_case_capacity;
drop trigger if exists bpay_next_net_replacement_truncate_guard on private.bpay_next_net_staged_case_capacity;
create trigger bpay_next_net_replacement_truncate_guard before truncate on private.bpay_next_net_staged_case_capacity
 for each statement execute function private.bpay_next_net_replacement_truncate_guard_v1();
drop trigger if exists bpay_next_net_replacement_truncate_guard on private.bpay_next_paye_net_request_refusal;
drop trigger if exists bpay_next_net_replacement_truncate_guard on private.bpay_next_paye_net_request_refusal;
create trigger bpay_next_net_replacement_truncate_guard before truncate on private.bpay_next_paye_net_request_refusal
 for each statement execute function private.bpay_next_net_replacement_truncate_guard_v1();
drop trigger if exists bpay_next_net_replacement_truncate_guard on private.bpay_next_net_cancel_handoff;
drop trigger if exists bpay_next_net_replacement_truncate_guard on private.bpay_next_net_cancel_handoff;
create trigger bpay_next_net_replacement_truncate_guard before truncate on private.bpay_next_net_cancel_handoff
 for each statement execute function private.bpay_next_net_replacement_truncate_guard_v1();
drop trigger if exists bpay_next_net_replacement_truncate_guard on private.bpay_next_net_cancel_hold_member;
drop trigger if exists bpay_next_net_replacement_truncate_guard on private.bpay_next_net_cancel_hold_member;
create trigger bpay_next_net_replacement_truncate_guard before truncate on private.bpay_next_net_cancel_hold_member
 for each statement execute function private.bpay_next_net_replacement_truncate_guard_v1();


-- DR-37 intake
create or replace function private.bpay_next_accept_case_net_request_v1(
  p_command_id uuid,p_run_worker_id uuid,p_actor_user_id uuid,p_input_kind text,
  p_entered_paye_net numeric,p_expected_projection_revision bigint
) returns jsonb
language plpgsql security invoker
set search_path=pg_catalog,private,public
as $function$
declare
  v_epoch bigint;v_run_id uuid;v_candidate uuid;v_sequence bigint;v_request_no bigint;v_latest_no bigint;
  v_replacement private.bpay_next_net_replacement%rowtype;v_prior_projection uuid;v_prior_state uuid;
  v_run private.bpay_next_pay_run%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_draft private.bpay_next_case_allocation_state%rowtype;
  v_prior private.bpay_next_paye_net_request%rowtype;
begin
  if p_command_id is null or p_run_worker_id is null or p_actor_user_id is null
     or p_input_kind is null or p_input_kind not in ('PAYE_MANUAL','CASE_PAYOUT')
     or p_expected_projection_revision is null or p_expected_projection_revision<0
     or p_expected_projection_revision=9223372036854775807
     or (p_input_kind='PAYE_MANUAL' and (p_entered_paye_net is null
       or p_entered_paye_net::text in ('NaN','Infinity','-Infinity') or p_entered_paye_net<0
       or p_entered_paye_net>=10000000000000000 or p_entered_paye_net<>pg_catalog.trunc(p_entered_paye_net,2)))
     or (p_input_kind='CASE_PAYOUT' and p_entered_paye_net is not null) then
    raise exception using errcode='22023',message='BPAY_NEXT_CASE_NET_INPUT_INVALID';
  end if;
  select owner_epoch into v_epoch from private.bpay_next_module_control
    where id=1 and active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  if not exists(select 1 from public.tms_users u where u.id=p_actor_user_id
      and u.is_active is true and u.role::text='admin' for share) then
    raise exception using errcode='42501',message='BPAY_NEXT_CASE_NET_ACTOR_INVALID';
  end if;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('bpay-next-case-net:'||p_command_id::text,0));
  select run_id,candidate_id into strict v_run_id,v_candidate from private.bpay_next_run_worker where id=p_run_worker_id;
  select dr37_point_16.* into strict v_run from private.bpay_next_pay_run dr37_point_16 where id=v_run_id for update;
  perform 1 from private.bpay_next_worker_control where candidate_id=v_candidate for update;
  select dr37_point_17.* into strict v_worker from private.bpay_next_run_worker dr37_point_17 where id=p_run_worker_id for update;
  select dr37_point_18.* into v_prior from private.bpay_next_paye_net_request dr37_point_18 where command_id=p_command_id;
  if found then
    if v_prior.run_worker_id<>p_run_worker_id or v_prior.actor_user_id is distinct from p_actor_user_id
       or v_prior.input_kind<>p_input_kind or v_prior.entered_paye_net is distinct from p_entered_paye_net
       or v_prior.expected_projection_revision is distinct from p_expected_projection_revision
       or v_prior.case_draft_state_id is null then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_REPLAY_CONFLICT';
    end if;
    select agency_sequence into strict v_sequence from private.bpay_next_command
      where id=p_command_id and command_kind='PAYE_NET_ENTRY';
    select dr37_point_19.* into v_replacement from private.bpay_next_net_replacement dr37_point_19 where id=p_command_id;
    if not found then
      -- Retain the historical 0559 receipt; never invent/backfill an operation.
      return pg_catalog.jsonb_build_object('sequence',v_sequence::text,'request_no',v_prior.request_no::text,
        'replay',true,'phase','ACCEPTED_PENDING_PROJECTION','run_worker_id',v_worker.id);
    end if;
    return pg_catalog.jsonb_build_object('sequence',v_sequence::text,'request_no',v_prior.request_no::text,
      'replay',true,'phase',case v_replacement.stage when 'COMPLETE' then 'PROJECTED'
        when 'REFUSED' then 'REFUSED' when 'CANCELLED' then 'CANCELLED' else 'ACCEPTED_PENDING_PROJECTION' end,
      'run_worker_id',v_worker.id,'replacement_id',v_replacement.id,'replacement_stage',v_replacement.stage,
      'projection_id',v_replacement.accepted_projection_id,'issue_code',v_replacement.refusal_code);
  end if;
  select s.* into strict v_draft from private.bpay_next_case_allocation_state s
    where s.run_worker_id=v_worker.id and s.preparation_revision=v_worker.captured_preparation_revision
      and s.selection_revision=v_worker.case_selection_revision and s.pass_kind='DRAFT' and s.projection_no=0;
  if v_run.status<>'DRAFT' or v_run.confirmed_at_utc is null or v_worker.status<>'READY'
     or v_worker.target_pay_channel<>'PAYE' or v_worker.gross_vat<>0
     or v_worker.gross_ex_vat<>v_worker.gross_inc_vat or v_worker.gross_inc_vat<0
     or v_draft.status<>'COMPLETE' or v_draft.prepare_stage<>'COMPLETE'
     or v_worker.case_pending_binding_count<>0
     or v_worker.net_projection_revision<>p_expected_projection_revision
     or v_worker.net_request_revision<>v_worker.net_projection_revision
     or v_worker.net_replacement_pending_id is not null
     or v_worker.realised_effect_count<>0
     or v_draft.captured_work_gross+v_draft.allocated_gross_additions-v_draft.allocated_gross_deductions<>v_worker.gross_inc_vat
     or exists(select 1 from private.bpay_next_cancel_request r where r.run_worker_id=v_worker.id
       and r.status in ('REQUESTED','CANCELLING'))
     or exists(select 1 from private.bpay_next_transfer t where t.run_worker_id=v_worker.id) then
    raise exception using errcode='55000',message='BPAY_NEXT_CASE_NET_NOT_ELIGIBLE';
  end if;
  if p_input_kind='PAYE_MANUAL' then
    if p_entered_paye_net>v_worker.gross_inc_vat or exists(select 1 from private.bpay_next_case_payout_basis
      where run_worker_id=v_worker.id and preparation_revision=v_worker.captured_preparation_revision) then
      raise exception using errcode='55000',message='BPAY_NEXT_CASE_NET_PAYROLL_BASIS_INVALID';
    end if;
  else
    if p_expected_projection_revision<>0 or v_worker.captured_line_count<>0 or v_worker.gross_inc_vat<>0
       or not exists(select 1 from private.bpay_next_case_payout_basis b where b.run_worker_id=v_worker.id
         and b.preparation_revision=v_worker.captured_preparation_revision and b.status='SEALED'
         and b.projection_id is null and b.target_total_inc_vat>0
         and b.target_total_inc_vat=v_draft.allocated_net_additions and b.beneficiary_id=v_worker.candidate_id) then
      raise exception using errcode='55000',message='BPAY_NEXT_CASE_PAYOUT_BASIS_INVALID';
    end if;
  end if;
  if p_expected_projection_revision>0 and not exists(select 1 from private.bpay_next_net_projection p
      where p.run_worker_id=v_worker.id and p.projection_no=p_expected_projection_revision
        and p.retired_at_utc is null and p.input_kind='PAYE_MANUAL') then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_PRIOR_PROJECTION_INVALID';
  end if;
  -- High-water is distinct from accepted projection and retains refused gaps.
  select r.request_no into v_latest_no from private.bpay_next_paye_net_request r
    where r.run_worker_id=v_worker.id order by r.request_no desc limit 1;
  v_request_no:=greatest(v_worker.net_request_high_water,coalesce(v_latest_no,0),v_worker.net_projection_revision);
  if v_request_no=9223372036854775807 then raise exception using errcode='22003',message='BPAY_NEXT_NET_REQUEST_NUMBER_EXHAUSTED';end if;
  v_request_no:=v_request_no+1;
  if p_expected_projection_revision>0 then
    select p.id,s.id into strict v_prior_projection,v_prior_state from private.bpay_next_net_projection p
      join private.bpay_next_case_allocation_state s on s.projection_id=p.id and s.status='COMPLETE' and s.net_stage='COMPLETE'
      where p.run_worker_id=v_worker.id and p.projection_no=p_expected_projection_revision and p.retired_at_utc is null
       and s.run_worker_id=v_worker.id and s.preparation_revision=v_worker.captured_preparation_revision
       and s.selection_revision=v_worker.case_selection_revision and s.pass_kind='NET';
  end if;
  v_sequence:=private.bpay_next_receive_command_v1(p_command_id,'PAYE_NET_ENTRY');
  insert into private.bpay_next_command_member(command_id,candidate_id,member_no)
    values(p_command_id,v_worker.candidate_id,1);
  insert into private.bpay_next_paye_net_request(command_id,run_worker_id,candidate_id,request_no,
    entered_paye_net,frozen_gross_inc_vat,input_kind,actor_user_id,case_draft_state_id,
    preparation_revision,selection_revision,expected_projection_revision)
    values(p_command_id,v_worker.id,v_worker.candidate_id,v_request_no,
      p_entered_paye_net,v_worker.gross_inc_vat,p_input_kind,p_actor_user_id,v_draft.id,
      v_worker.captured_preparation_revision,v_worker.case_selection_revision,p_expected_projection_revision);
  insert into private.bpay_next_net_replacement(id,run_worker_id,run_id,candidate_id,pay_week_start,request_no,
    prior_projection_no,prior_projection_id,draft_state_id,prior_state_id,module_epoch,preparation_revision,selection_revision,
    expected_instruction_count,initial_channel_headroom,remaining_channel_headroom,initial_take_home,remaining_take_home)
    values(p_command_id,v_worker.id,v_worker.run_id,v_worker.candidate_id,v_draft.pay_week_start,v_request_no,
      p_expected_projection_revision,v_prior_projection,v_draft.id,v_prior_state,v_epoch,v_worker.captured_preparation_revision,
      v_worker.case_selection_revision,v_draft.expected_instruction_count,coalesce(p_entered_paye_net,0),
      coalesce(p_entered_paye_net,0),coalesce(p_entered_paye_net,0),coalesce(p_entered_paye_net,0));
  update private.bpay_next_run_worker set net_request_revision=v_request_no,net_request_high_water=v_request_no,
    net_replacement_pending_id=p_command_id,
    review_issue_code=case when review_issue_code in ('CASE_INPUT_UNBOUND','CASE_WEEK_BASIS_UNBOUND','CASE_RETURN_FLOOR_UNBOUND') then null else review_issue_code end,
    review_issue_work_id=case when review_issue_code in ('CASE_INPUT_UNBOUND','CASE_WEEK_BASIS_UNBOUND','CASE_RETURN_FLOOR_UNBOUND') then null else review_issue_work_id end
    where id=v_worker.id;
  update private.bpay_next_worker_control set financial_view_revision=financial_view_revision+1,
    updated_at_utc=pg_catalog.transaction_timestamp() where candidate_id=v_candidate;
  update private.bpay_next_command set expected_member_count=1,status='SEALED',sealed_at_utc=pg_catalog.transaction_timestamp()
    where id=p_command_id;
  return pg_catalog.jsonb_build_object('sequence',v_sequence::text,'request_no',v_request_no::text,
    'replay',false,'phase','ACCEPTED_PENDING_PROJECTION','run_worker_id',v_worker.id,
    'replacement_id',p_command_id,'replacement_stage','PRECHECK','projection_id',null,'issue_code',null);
end
$function$;


-- DR-37 prelock
-- One finite page, then the native Case/component/period/hold lock order.
create or replace function private.bpay_next_net_page_prelock_v1(p_replacement_id uuid,p_limit integer,p_step text)
returns void language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_r private.bpay_next_net_replacement%rowtype;v_ids uuid[];
begin
 if p_limit is null or p_limit not between 1 and 100 or p_step is null or p_step not in ('STAGE','VALIDATE','COMMIT','INVENTORY','RELEASE') then
  raise exception using errcode='22023',message='BPAY_NEXT_NET_PRELOCK_INPUT_INVALID';end if;
 select dr37_point_20.* into strict v_r from private.bpay_next_net_replacement dr37_point_20 where id=p_replacement_id;
 perform private.bpay_next_net_replacement_job_assert_v1(v_r.id);
 if p_step in ('VALIDATE','COMMIT') then
  select pg_catalog.array_agg(q.instruction_id order by q.result_no) into v_ids from
   (select a.instruction_id,a.result_no from private.bpay_next_net_staged_result a where a.replacement_id=v_r.id
    and a.result_no>case when p_step='VALIDATE' then v_r.validation_cursor else v_r.commit_cursor end
    order by a.result_no limit p_limit) q;
 else
  select pg_catalog.array_agg(q.id order by q.age_key,q.case_id,q.component_ordinal,q.case_component_id) into v_ids from
   (select i.id,i.age_key,i.case_id,i.component_ordinal,i.case_component_id from private.bpay_next_run_case_instruction i
    where i.run_worker_id=v_r.run_worker_id and i.preparation_revision=v_r.preparation_revision and i.selection_revision=v_r.selection_revision
     and (case p_step when 'STAGE' then v_r.cursor_component_id is null
      or (i.age_key,i.case_id,i.component_ordinal,i.case_component_id)>(v_r.cursor_age_key,v_r.cursor_case_id,v_r.cursor_component_ordinal,v_r.cursor_component_id)
      when 'INVENTORY' then v_r.inventory_component_id is null
      or (i.age_key,i.case_id,i.component_ordinal,i.case_component_id)>(v_r.inventory_age_key,v_r.inventory_case_id,v_r.inventory_component_ordinal,v_r.inventory_component_id)
      else v_r.cleanup_component_id is null
      or (i.age_key,i.case_id,i.component_ordinal,i.case_component_id)>(v_r.cleanup_age_key,v_r.cleanup_case_id,v_r.cleanup_component_ordinal,v_r.cleanup_component_id) end)
    order by i.age_key,i.case_id,i.component_ordinal,i.case_component_id limit p_limit) q;
 end if;
 if v_ids is null then return;end if;
 perform c.id from private.bpay_next_finance_case c where exists(select 1 from private.bpay_next_run_case_instruction i
  where i.id=any(v_ids) and i.case_id=c.id) order by c.id for update nowait;
 perform c.id from private.bpay_next_case_component c where exists(select 1 from private.bpay_next_run_case_instruction i
  where i.id=any(v_ids) and i.case_component_id=c.id) order by c.id for update nowait;
 perform p.case_component_id from private.bpay_next_case_period p where exists(select 1 from private.bpay_next_run_case_instruction i
  where i.id=any(v_ids) and (i.case_component_id,i.pay_week_start)=(p.case_component_id,p.pay_week_start))
  order by p.case_component_id,p.pay_week_start for update nowait;
 perform h.id from private.bpay_next_case_hold h where h.instruction_id=any(v_ids) and h.status='ACTIVE' order by h.id for update nowait;
 perform u.case_hold_id from private.bpay_next_case_capacity_use u where exists(select 1 from private.bpay_next_case_hold h
  where h.id=u.case_hold_id and h.instruction_id=any(v_ids) and h.status='ACTIVE') order by u.case_hold_id for update nowait;
end $function$;


-- DR-37 pages
-- Fixed-size source pages; no product limit on total legitimate instructions.
create or replace function private.bpay_next_net_prior_run_page_v1(p_replacement_id uuid,p_limit integer,p_validation boolean)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_rep private.bpay_next_net_replacement%rowtype;v_run private.bpay_next_pay_run%rowtype;
 v_w private.bpay_next_run_worker%rowtype;v_row record;v_date date;v_created timestamptz;v_id uuid;
 v_seen integer:=0;v_bytes integer:=0;v_size integer;v_more boolean;v_issue text;
begin
 if p_limit is null or p_limit not between 1 and 100 or p_validation is null then
  raise exception using errcode='22023',message='BPAY_NEXT_NET_PRIOR_PAGE_INPUT_INVALID';end if;
 select dr37_point_21.* into strict v_rep from private.bpay_next_net_replacement dr37_point_21 where id=p_replacement_id for update;
 perform private.bpay_next_net_replacement_job_assert_v1(v_rep.id);
 select dr37_point_22.* into strict v_run from private.bpay_next_pay_run dr37_point_22 where id=v_rep.run_id;
 if (not p_validation and v_rep.stage<>'PRECHECK') or (p_validation and (v_rep.stage<>'STAGING' or v_rep.staging_step<>'VALIDATE')) then
  raise exception using errcode='23514',message='BPAY_NEXT_NET_PRIOR_PAGE_STAGE_INVALID';end if;
 if p_validation then
  v_date:=v_rep.validation_pay_date_cursor;v_created:=v_rep.validation_created_at_cursor;v_id:=v_rep.validation_run_cursor;
 else
  v_date:=v_rep.metadata_pay_date_cursor;v_created:=v_rep.metadata_created_at_cursor;v_id:=v_rep.metadata_run_cursor;
 end if;
 for v_row in select r.id,r.pay_date,r.created_at_utc from private.bpay_next_pay_run r
  where r.pay_date>=v_rep.pay_week_start and r.pay_date<v_rep.pay_week_start+7
   and (r.pay_date,r.created_at_utc)<=(v_run.pay_date,v_run.created_at_utc)
   and (v_id is null or (r.pay_date,r.created_at_utc,r.id)>(v_date,v_created,v_id))
  order by r.pay_date,r.created_at_utc,r.id limit p_limit
 loop
  select dr37_point_23.* into v_w from private.bpay_next_run_worker dr37_point_23 where run_id=v_row.id and candidate_id=v_rep.candidate_id;
  v_size:=pg_catalog.octet_length(pg_catalog.jsonb_build_array(v_row.id,v_row.pay_date,v_row.created_at_utc,
   v_w.id,v_w.status)::text);
  if v_size>65536 then raise exception using errcode='54000',message='BPAY_NEXT_NET_PAGE_ROW_BYTES_EXCEEDED';end if;
  if v_bytes+v_size>65536 then exit;end if;
  v_bytes:=v_bytes+v_size;v_seen:=v_seen+1;
  if v_w.id is not null and v_w.status in ('READY','DRAFT','ISSUED','COMPLETE')
   and (v_row.pay_date,v_row.created_at_utc,v_w.id)<(v_run.pay_date,v_run.created_at_utc,v_rep.run_worker_id)
   and not exists(select 1 from private.bpay_next_worker_week_contribution x where x.original_run_worker_id=v_w.id) then
   v_issue:='CASE_WEEK_BASIS_UNBOUND';exit;end if;
  v_date:=v_row.pay_date;v_created:=v_row.created_at_utc;v_id:=v_row.id;
 end loop;
 select exists(select 1 from private.bpay_next_pay_run r where r.pay_date>=v_rep.pay_week_start
  and r.pay_date<v_rep.pay_week_start+7 and (r.pay_date,r.created_at_utc)<=(v_run.pay_date,v_run.created_at_utc)
  and (v_id is null or (r.pay_date,r.created_at_utc,r.id)>(v_date,v_created,v_id))) into v_more;
 if p_validation then
  update private.bpay_next_net_replacement set validation_pay_date_cursor=v_date,validation_created_at_cursor=v_created,
   validation_run_cursor=v_id,validation_step=case when not v_more and v_issue is null then 'DONE' else validation_step end,
   updated_at_utc=pg_catalog.transaction_timestamp() where id=v_rep.id;
 else
  update private.bpay_next_net_replacement set metadata_pay_date_cursor=v_date,metadata_created_at_cursor=v_created,
   metadata_run_cursor=v_id,updated_at_utc=pg_catalog.transaction_timestamp() where id=v_rep.id;
 end if;
 return pg_catalog.jsonb_build_object('rows_visited',v_seen::text,'encoded_bytes',v_bytes::text,'more',v_more,'issue_code',v_issue);
end $function$;

create or replace function private.bpay_next_validate_case_net_page_v1(p_replacement_id uuid,p_limit integer)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_rep private.bpay_next_net_replacement%rowtype;v_s private.bpay_next_case_allocation_state%rowtype;
 v_d private.bpay_next_case_allocation_state%rowtype;v_q private.bpay_next_paye_net_request%rowtype;
 v_a private.bpay_next_net_staged_result%rowtype;v_h private.bpay_next_case_hold%rowtype;
 v_c private.bpay_next_finance_case%rowtype;v_cc private.bpay_next_case_component%rowtype;v_p private.bpay_next_case_period%rowtype;
 v_carry private.bpay_next_net_staged_case_capacity%rowtype;v_week private.bpay_next_worker_week%rowtype;
 v_contribution private.bpay_next_worker_week_contribution%rowtype;
 v_seen integer:=0;v_bytes integer:=0;v_size integer;v_more boolean;v_issue text;v_reply jsonb;
begin
 if p_limit is null or p_limit not between 1 and 100 then raise exception using errcode='22023',message='BPAY_NEXT_NET_VALIDATION_INPUT_INVALID';end if;
 select dr37_point_24.* into strict v_rep from private.bpay_next_net_replacement dr37_point_24 where id=p_replacement_id for update;
 perform private.bpay_next_net_replacement_job_assert_v1(v_rep.id);
 select dr37_point_25.* into strict v_q from private.bpay_next_paye_net_request dr37_point_25 where command_id=v_rep.id;
 select dr37_point_26.* into strict v_s from private.bpay_next_case_allocation_state dr37_point_26 where id=v_rep.target_state_id for update;
 select dr37_point_27.* into strict v_d from private.bpay_next_case_allocation_state dr37_point_27 where id=v_rep.draft_state_id;
 if v_rep.stage<>'STAGING' or v_rep.staging_step<>'VALIDATE' or not exists(select 1 from private.bpay_next_candidate_week_commit_fence f
  where f.candidate_id=v_rep.candidate_id and f.pay_week_start=v_rep.pay_week_start and f.replacement_id=v_rep.id
   and f.command_id=v_rep.id and f.original_job_id=v_rep.original_job_id and f.allocation_state_id=v_rep.target_state_id and f.stage='STAGING') then
  raise exception using errcode='23514',message='BPAY_NEXT_NET_VALIDATION_FENCE_INVALID';end if;
 select dr37_point_28.* into strict v_week from private.bpay_next_worker_week dr37_point_28 where candidate_id=v_rep.candidate_id and pay_week_start=v_rep.pay_week_start for update nowait;
 select dr37_point_29.* into v_contribution from private.bpay_next_worker_week_contribution dr37_point_29 where original_run_worker_id=v_rep.run_worker_id for update nowait;
 if v_s.net_requires_week_binding and v_week.period_revision<>v_s.captured_week_revision then v_issue:='CASE_WEEK_BASIS_UNBOUND';
 elsif v_contribution.id is null or v_contribution.eligibility_state='UNBOUND' or v_contribution.payment_state<>'ARRANGED'
  or v_contribution.original_gross_amount<>v_q.frozen_gross_inc_vat then v_issue:='CASE_WEEK_BASIS_UNBOUND';
 elsif (v_rep.gross_additions,v_rep.gross_deductions,v_rep.net_additions)
  is distinct from (v_d.allocated_gross_additions,v_d.allocated_gross_deductions,v_d.allocated_net_additions)
  or coalesce(v_q.entered_paye_net,0)+v_rep.net_additions-v_rep.recoveries<0
  or coalesce(v_q.entered_paye_net,0)+v_rep.net_additions-v_rep.recoveries>=10000000000000000 then v_issue:='CASE_INPUT_UNBOUND';end if;
 if v_issue is null and v_rep.validation_step='INSTRUCTIONS' then
  perform private.bpay_next_net_page_prelock_v1(v_rep.id,p_limit,'VALIDATE');
  for v_a in select a.* from private.bpay_next_net_staged_result a where a.replacement_id=v_rep.id and a.result_no>v_rep.validation_cursor
   order by a.result_no limit p_limit
  loop
   select dr37_point_30.* into v_h from private.bpay_next_case_hold dr37_point_30 where instruction_id=v_a.instruction_id and status='ACTIVE' for update nowait;
   select dr37_point_31.* into strict v_c from private.bpay_next_finance_case dr37_point_31 where id=v_a.case_id for update nowait;
   select dr37_point_32.* into strict v_cc from private.bpay_next_case_component dr37_point_32 where id=v_a.case_component_id for update nowait;
   select dr37_point_33.* into strict v_p from private.bpay_next_case_period dr37_point_33 where case_component_id=v_a.case_component_id and pay_week_start=v_a.pay_week_start for update nowait;
   v_size:=pg_catalog.octet_length(pg_catalog.jsonb_build_array(pg_catalog.to_jsonb(v_a),pg_catalog.to_jsonb(v_h),
    v_c.case_revision,v_c.active_recovery_hold_amount,v_c.principal_funded,v_c.principal_approved,v_c.principal_recovered,v_c.principal_written_off,
    v_cc.component_revision,v_p.period_revision)::text);
   if v_size>65536 then raise exception using errcode='54000',message='BPAY_NEXT_NET_PAGE_ROW_BYTES_EXCEEDED';end if;
   if v_bytes+v_size>65536 then exit;end if;
   v_seen:=v_seen+1;v_bytes:=v_bytes+v_size;
   if (v_h.id,coalesce(v_h.source_reserved_ex_vat,0)) is distinct from (v_a.prior_hold_id,v_a.prior_source_amount)
    or (v_c.case_revision,v_cc.component_revision,v_p.period_revision)
     is distinct from (v_a.captured_case_revision,v_a.captured_component_revision,v_a.captured_period_revision)
    or v_h.id is not null and not exists(select 1 from private.bpay_next_case_capacity_use u
     where u.case_hold_id=v_h.id and u.status='ACTIVE' and u.source_amount_ex_vat=v_a.prior_source_amount) then v_issue:='CASE_INPUT_UNBOUND';exit;end if;
   if v_a.payroll_stage='NET_DEDUCT' then
    select dr37_point_34.* into strict v_carry from private.bpay_next_net_staged_case_capacity dr37_point_34 where replacement_id=v_rep.id and case_id=v_a.case_id;
    if v_carry.captured_case_revision<>v_c.case_revision or v_carry.captured_active_recovery_source<>v_c.active_recovery_hold_amount
     or v_carry.captured_outstanding_source<>(case when v_c.case_kind in ('LOAN','ADVANCE') then v_c.principal_funded else v_c.principal_approved end)
      -v_c.principal_recovered-v_c.principal_written_off then v_issue:='CASE_INPUT_UNBOUND';exit;end if;
   end if;
   if v_a.result_no<>v_rep.validated_instruction_count+1 then raise exception using errcode='23514',message='BPAY_NEXT_NET_VALIDATION_RESULT_SEQUENCE_INVALID';end if;
   v_rep.validated_instruction_count:=v_rep.validated_instruction_count+1;v_rep.validation_cursor:=v_a.result_no;
  end loop;
  select exists(select 1 from private.bpay_next_net_staged_result a where a.replacement_id=v_rep.id and a.result_no>v_rep.validation_cursor) into v_more;
  if not v_more and v_issue is null and v_rep.validated_instruction_count<>v_rep.expected_instruction_count then
   raise exception using errcode='23514',message='BPAY_NEXT_NET_VALIDATION_COUNT_MISMATCH';end if;
  update private.bpay_next_net_replacement set validated_instruction_count=v_rep.validated_instruction_count,validation_cursor=v_rep.validation_cursor,
   validation_step=case when not v_more and v_issue is null then case when v_s.net_requires_week_binding then 'PRIOR_RUNS' else 'DONE' end else validation_step end,
   updated_at_utc=pg_catalog.transaction_timestamp() where id=v_rep.id;
 elsif v_issue is null and v_rep.validation_step='PRIOR_RUNS' then
  v_reply:=private.bpay_next_net_prior_run_page_v1(v_rep.id,p_limit,true);
  v_seen:=(v_reply->>'rows_visited')::integer;v_bytes:=(v_reply->>'encoded_bytes')::integer;v_issue:=v_reply->>'issue_code';
 end if;
 return pg_catalog.jsonb_build_object('rows_visited',v_seen::text,'encoded_bytes',v_bytes::text,'issue_code',v_issue);
end $function$;

create or replace function private.bpay_next_net_precheck_page_v1(p_replacement_id uuid,p_limit integer)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_rep private.bpay_next_net_replacement%rowtype;v_state private.bpay_next_case_allocation_state%rowtype;
 v_i private.bpay_next_run_case_instruction%rowtype;v_a private.bpay_next_case_allocation_result%rowtype;
 v_seen integer:=0;v_bytes integer:=0;v_size integer;v_more boolean;v_issue text;v_reply jsonb;
begin
 if p_limit is null or p_limit not between 1 and 100 then raise exception using errcode='22023',message='BPAY_NEXT_NET_PRECHECK_INPUT_INVALID';end if;
 select dr37_point_35.* into strict v_rep from private.bpay_next_net_replacement dr37_point_35 where id=p_replacement_id for update;
 perform private.bpay_next_net_replacement_job_assert_v1(v_rep.id);
 select dr37_point_36.* into strict v_state from private.bpay_next_case_allocation_state dr37_point_36 where id=v_rep.target_state_id for update;
 if v_rep.stage<>'PRECHECK' or v_state.net_stage<>'PRECHECK' then raise exception using errcode='23514',message='BPAY_NEXT_NET_PRECHECK_STAGE_INVALID';end if;
 if v_state.captured_instruction_count<v_state.expected_instruction_count then
  for v_i in select i.* from private.bpay_next_run_case_instruction i where i.run_worker_id=v_rep.run_worker_id
   and i.preparation_revision=v_rep.preparation_revision and i.selection_revision=v_rep.selection_revision
   and (v_state.cursor_case_component_id is null or (i.age_key,i.case_id,i.component_ordinal,i.case_component_id)
    >(v_state.cursor_age_key,v_state.cursor_case_id,v_state.cursor_component_ordinal,v_state.cursor_case_component_id))
   order by i.age_key,i.case_id,i.component_ordinal,i.case_component_id limit p_limit
  loop
   select dr37_point_37.* into v_a from private.bpay_next_case_allocation_result dr37_point_37 where state_id=v_rep.draft_state_id and instruction_id=v_i.id;
   v_size:=pg_catalog.octet_length(pg_catalog.jsonb_build_array(pg_catalog.to_jsonb(v_i),pg_catalog.to_jsonb(v_a))::text);
   if v_size>65536 then raise exception using errcode='54000',message='BPAY_NEXT_NET_PAGE_ROW_BYTES_EXCEEDED';end if;
   if v_bytes+v_size>65536 then exit;end if;
   v_seen:=v_seen+1;v_bytes:=v_bytes+v_size;
   if v_a.id is null or v_i.source_pay_channel<>'PAYE' or v_i.target_pay_channel<>'PAYE' or v_i.allocation_channel<>'PAYE'
    or v_i.currency<>'GBP' or v_i.nominal_target_vat<>0 or v_a.allocated_target_vat<>0
    or v_a.allocated_source_ex_vat<>v_a.allocated_target_ex_vat then v_issue:='CASE_INPUT_UNBOUND';exit;end if;
   if v_i.payroll_stage='NET_DEDUCT' and v_i.case_kind in ('LOAN','ADVANCE','MANUAL_DEBT') and v_a.allocated_source_ex_vat>0 then
    v_state.net_requires_week_binding:=true;end if;
   v_state.captured_instruction_count:=v_state.captured_instruction_count+1;
   v_state.cursor_age_key:=v_i.age_key;v_state.cursor_case_id:=v_i.case_id;
   v_state.cursor_component_ordinal:=v_i.component_ordinal;v_state.cursor_case_component_id:=v_i.case_component_id;
  end loop;
  select exists(select 1 from private.bpay_next_run_case_instruction i where i.run_worker_id=v_rep.run_worker_id
   and i.preparation_revision=v_rep.preparation_revision and i.selection_revision=v_rep.selection_revision
   and (v_state.cursor_case_component_id is null or (i.age_key,i.case_id,i.component_ordinal,i.case_component_id)
    >(v_state.cursor_age_key,v_state.cursor_case_id,v_state.cursor_component_ordinal,v_state.cursor_case_component_id))) into v_more;
  if not v_more and v_issue is null and v_state.captured_instruction_count<>v_state.expected_instruction_count then
   raise exception using errcode='23514',message='BPAY_NEXT_NET_PRECHECK_COUNT_MISMATCH';end if;
  update private.bpay_next_case_allocation_state set captured_instruction_count=v_state.captured_instruction_count,
   cursor_age_key=v_state.cursor_age_key,cursor_case_id=v_state.cursor_case_id,cursor_component_ordinal=v_state.cursor_component_ordinal,
   cursor_case_component_id=v_state.cursor_case_component_id,net_requires_week_binding=v_state.net_requires_week_binding where id=v_state.id;
 else
  if v_state.net_requires_week_binding then
   v_reply:=private.bpay_next_net_prior_run_page_v1(v_rep.id,p_limit,false);
   v_seen:=(v_reply->>'rows_visited')::integer;v_bytes:=(v_reply->>'encoded_bytes')::integer;
   v_issue:=v_reply->>'issue_code';v_more:=(v_reply->>'more')::boolean;
  else v_more:=false;end if;
  if not v_more and v_issue is null then
   update private.bpay_next_case_allocation_state set net_stage=case when net_requires_week_binding then 'WEEK_CAPTURE' else 'ALLOCATE' end,
    weekly_binding_state=case when net_requires_week_binding then 'UNBOUND' else 'NOT_AFFECTED' end,
    cursor_age_key=null,cursor_case_id=null,cursor_component_ordinal=null,cursor_case_component_id=null where id=v_state.id;
  end if;
 end if;
 return pg_catalog.jsonb_build_object('rows_visited',v_seen::text,'encoded_bytes',v_bytes::text,'issue_code',v_issue);
end $function$;

create or replace function private.bpay_next_stage_case_net_page_v1(p_replacement_id uuid,p_limit integer)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_rep private.bpay_next_net_replacement%rowtype;v_i private.bpay_next_run_case_instruction%rowtype;
 v_a private.bpay_next_case_allocation_result%rowtype;v_h private.bpay_next_case_hold%rowtype;
 v_c private.bpay_next_finance_case%rowtype;v_cc private.bpay_next_case_component%rowtype;v_p private.bpay_next_case_period%rowtype;
 v_carry private.bpay_next_net_staged_case_capacity%rowtype;v_kernel record;
 v_seen integer:=0;v_bytes integer:=0;v_size integer;v_more boolean;v_issue text;
 v_own numeric;v_capacity numeric;v_outstanding numeric;v_case_outstanding numeric;v_e numeric;v_t numeric;
begin
 if p_limit is null or p_limit not between 1 and 100 then raise exception using errcode='22023',message='BPAY_NEXT_NET_STAGING_INPUT_INVALID';end if;
 select dr37_point_38.* into strict v_rep from private.bpay_next_net_replacement dr37_point_38 where id=p_replacement_id for update;
 perform private.bpay_next_net_replacement_job_assert_v1(v_rep.id);
 if v_rep.stage<>'STAGING' or v_rep.staging_step<>'INSTRUCTIONS' then raise exception using errcode='23514',message='BPAY_NEXT_NET_STAGING_STAGE_INVALID';end if;
 perform private.bpay_next_net_page_prelock_v1(v_rep.id,p_limit,'STAGE');
 v_e:=v_rep.remaining_channel_headroom;v_t:=v_rep.remaining_take_home;
 for v_i in select i.* from private.bpay_next_run_case_instruction i where i.run_worker_id=v_rep.run_worker_id
  and i.preparation_revision=v_rep.preparation_revision and i.selection_revision=v_rep.selection_revision
  and (v_rep.cursor_component_id is null or (i.age_key,i.case_id,i.component_ordinal,i.case_component_id)
   >(v_rep.cursor_age_key,v_rep.cursor_case_id,v_rep.cursor_component_ordinal,v_rep.cursor_component_id))
  order by i.age_key,i.case_id,i.component_ordinal,i.case_component_id limit p_limit
 loop
  select dr37_point_39.* into v_a from private.bpay_next_case_allocation_result dr37_point_39 where state_id=v_rep.draft_state_id and instruction_id=v_i.id;
  select dr37_point_40.* into v_h from private.bpay_next_case_hold dr37_point_40 where instruction_id=v_i.id and status='ACTIVE' for update nowait;
  select dr37_point_41.* into strict v_c from private.bpay_next_finance_case dr37_point_41 where id=v_i.case_id for update nowait;
  select dr37_point_42.* into strict v_cc from private.bpay_next_case_component dr37_point_42 where id=v_i.case_component_id for update nowait;
  select dr37_point_43.* into strict v_p from private.bpay_next_case_period dr37_point_43 where case_component_id=v_i.case_component_id and pay_week_start=v_i.pay_week_start for update nowait;
  v_size:=pg_catalog.octet_length(pg_catalog.jsonb_build_array(pg_catalog.to_jsonb(v_i),pg_catalog.to_jsonb(v_a),pg_catalog.to_jsonb(v_h),
   v_c.case_revision,v_c.principal_funded,v_c.principal_approved,v_c.principal_recovered,v_c.principal_written_off,v_c.active_recovery_hold_amount,
   v_cc.component_revision,v_cc.funded_source_ex_vat,v_cc.approved_source_ex_vat,v_cc.recovered_source_ex_vat,v_cc.written_off_source_ex_vat,
   v_cc.active_recovery_source_ex_vat,v_p.period_revision,v_p.opening_due_source_ex_vat,v_p.realised_recovery_source_ex_vat,v_p.active_unrealised_recovery_source_ex_vat)::text);
  if v_size>65536 then raise exception using errcode='54000',message='BPAY_NEXT_NET_PAGE_ROW_BYTES_EXCEEDED';end if;
  if v_bytes+v_size>65536 then exit;end if;
  v_seen:=v_seen+1;v_bytes:=v_bytes+v_size;v_own:=coalesce(v_h.source_reserved_ex_vat,0);
  if v_a.id is null or v_i.source_pay_channel<>'PAYE' or v_i.target_pay_channel<>'PAYE' or v_i.allocation_channel<>'PAYE'
   or v_i.currency<>'GBP' or v_i.nominal_target_vat<>0 or v_a.allocated_target_vat<>0
   or v_a.allocated_source_ex_vat<>v_a.allocated_target_ex_vat then v_issue:='CASE_INPUT_UNBOUND';exit;end if;
  if v_h.id is not null and (v_h.run_worker_id<>v_rep.run_worker_id or v_h.purpose<>v_i.hold_purpose
   or v_h.source_reserved_ex_vat<>v_h.target_amount_ex_vat or v_h.target_amount_vat<>0
   or not exists(select 1 from private.bpay_next_case_capacity_use u where u.case_hold_id=v_h.id and u.status='ACTIVE' and u.source_amount_ex_vat=v_own)
   or not exists(select 1 from private.bpay_next_case_allocation_result a where a.id=v_h.allocation_result_id
    and a.instruction_id=v_i.id and a.state_id in (v_rep.draft_state_id,v_rep.prior_state_id))) then v_issue:='CASE_INPUT_UNBOUND';exit;end if;
  if v_i.payroll_stage<>'NET_DEDUCT' then
   if v_own<>v_a.allocated_source_ex_vat or v_own>0 and (v_h.allocation_result_id<>v_a.id or v_h.allocation_pass_kind<>'DRAFT') then
    v_issue:='CASE_INPUT_UNBOUND';exit;end if;
   if v_i.payroll_stage='GROSS_ADD' then v_rep.gross_additions:=v_rep.gross_additions+v_a.allocated_target_ex_vat;
   elsif v_i.payroll_stage='GROSS_DEDUCT' then v_rep.gross_deductions:=v_rep.gross_deductions+v_a.allocated_target_ex_vat;
   elsif v_i.payroll_stage='NET_ADD' then v_rep.net_additions:=v_rep.net_additions+v_a.allocated_target_inc_vat;
   else v_issue:='CASE_INPUT_UNBOUND';exit;end if;
  else
   if v_own>v_cc.active_recovery_source_ex_vat or v_own>v_c.active_recovery_hold_amount
    or v_own>v_p.active_unrealised_recovery_source_ex_vat or v_own>v_a.allocated_source_ex_vat then v_issue:='CASE_INPUT_UNBOUND';exit;end if;
   v_outstanding:=(case when v_i.case_kind in ('LOAN','ADVANCE') then v_cc.funded_source_ex_vat else v_cc.approved_source_ex_vat end)
    -v_cc.recovered_source_ex_vat-v_cc.written_off_source_ex_vat;
   v_case_outstanding:=(case when v_i.case_kind in ('LOAN','ADVANCE') then v_c.principal_funded else v_c.principal_approved end)
    -v_c.principal_recovered-v_c.principal_written_off;
   insert into private.bpay_next_net_staged_case_capacity(replacement_id,case_id,captured_case_revision,captured_outstanding_source,captured_active_recovery_source)
    values(v_rep.id,v_i.case_id,v_c.case_revision,v_case_outstanding,v_c.active_recovery_hold_amount) on conflict(replacement_id,case_id) do nothing;
   select dr37_point_44.* into strict v_carry from private.bpay_next_net_staged_case_capacity dr37_point_44 where replacement_id=v_rep.id and case_id=v_i.case_id for update;
   if (v_carry.captured_case_revision,v_carry.captured_outstanding_source,v_carry.captured_active_recovery_source)
    is distinct from (v_c.case_revision,v_case_outstanding,v_c.active_recovery_hold_amount) then v_issue:='CASE_INPUT_UNBOUND';exit;end if;
   v_capacity:=least(v_a.allocated_source_ex_vat,
    greatest(v_p.opening_due_source_ex_vat-v_p.realised_recovery_source_ex_vat-v_p.active_unrealised_recovery_source_ex_vat+v_own,0),
    greatest(v_outstanding-v_cc.active_recovery_source_ex_vat+v_own,0),
    greatest(v_case_outstanding-v_c.active_recovery_hold_amount+v_carry.proposed_release_source-v_carry.proposed_reserve_source+v_own,0));
   select k.* into strict v_kernel from private.bpay_next_case_allocation_row_v1(v_i.case_kind,v_i.nominal_target_ex_vat,v_capacity,
    v_e,v_t,v_i.minimum_earnings_threshold,v_i.take_home_floor_override,v_i.captured_default_floor) k;
   v_a.usable_capacity_target_ex_vat:=v_capacity;v_a.allocated_source_ex_vat:=v_kernel.taken_amount;
   v_a.allocated_target_ex_vat:=v_kernel.taken_amount;v_a.allocated_target_vat:=0;v_a.allocated_target_inc_vat:=v_kernel.taken_amount;
   v_a.shortfall_target_ex_vat:=v_i.nominal_target_ex_vat-v_kernel.taken_amount;v_a.cap_reason:=v_kernel.cap_reason;
   v_a.affordability_reason:=v_kernel.affordability_reason;v_a.capacity_reason:=v_kernel.capacity_reason;
   v_a.channel_headroom_before:=v_e;v_a.channel_headroom_after:=v_kernel.next_channel_headroom;
   v_a.worker_take_home_before:=v_t;v_a.worker_take_home_after:=v_kernel.next_worker_take_home;
   v_rep.staged_deduction_count:=v_rep.staged_deduction_count+1;
   v_rep.staged_old_hold_count:=v_rep.staged_old_hold_count+case when v_h.id is null then 0 else 1 end;
   v_rep.staged_old_source_total:=v_rep.staged_old_source_total+v_own;
   v_rep.proposed_hold_count:=v_rep.proposed_hold_count+case when v_kernel.taken_amount>0 then 1 else 0 end;
   v_rep.proposed_source_total:=v_rep.proposed_source_total+v_kernel.taken_amount;v_rep.recoveries:=v_rep.recoveries+v_kernel.taken_amount;
   v_e:=v_kernel.next_channel_headroom;v_t:=v_kernel.next_worker_take_home;
  end if;
  insert into private.bpay_next_net_staged_result(state_id,instruction_id,run_worker_id,candidate_id,preparation_revision,selection_revision,
   result_no,pass_kind,allocation_channel,hold_purpose,nominal_target_ex_vat,usable_capacity_target_ex_vat,
   allocated_source_ex_vat,allocated_target_ex_vat,allocated_target_vat,allocated_target_inc_vat,shortfall_target_ex_vat,
   cap_reason,affordability_reason,capacity_reason,channel_headroom_before,channel_headroom_after,worker_take_home_before,worker_take_home_after,
   replacement_id,draft_result_id,prior_hold_id,prior_source_amount,case_id,case_component_id,pay_week_start,
   captured_case_revision,captured_component_revision,captured_period_revision,payroll_stage)
   values(v_rep.target_state_id,v_i.id,v_rep.run_worker_id,v_rep.candidate_id,v_rep.preparation_revision,v_rep.selection_revision,
    v_rep.staged_instruction_count+1,'NET',v_i.allocation_channel,v_i.hold_purpose,v_a.nominal_target_ex_vat,v_a.usable_capacity_target_ex_vat,
    v_a.allocated_source_ex_vat,v_a.allocated_target_ex_vat,v_a.allocated_target_vat,v_a.allocated_target_inc_vat,v_a.shortfall_target_ex_vat,
    v_a.cap_reason,v_a.affordability_reason,v_a.capacity_reason,v_a.channel_headroom_before,v_a.channel_headroom_after,v_a.worker_take_home_before,v_a.worker_take_home_after,
    v_rep.id,v_a.id,v_h.id,v_own,v_i.case_id,v_i.case_component_id,v_i.pay_week_start,v_c.case_revision,v_cc.component_revision,v_p.period_revision,v_i.payroll_stage);
  if v_i.payroll_stage='NET_DEDUCT' then
   update private.bpay_next_net_staged_case_capacity set proposed_release_source=proposed_release_source+v_own,
    proposed_reserve_source=proposed_reserve_source+v_kernel.taken_amount,staged_instruction_count=staged_instruction_count+1
    where replacement_id=v_rep.id and case_id=v_i.case_id;end if;
  v_rep.staged_instruction_count:=v_rep.staged_instruction_count+1;v_rep.cursor_age_key:=v_i.age_key;
  v_rep.cursor_case_id:=v_i.case_id;v_rep.cursor_component_ordinal:=v_i.component_ordinal;v_rep.cursor_component_id:=v_i.case_component_id;
 end loop;
 select exists(select 1 from private.bpay_next_run_case_instruction i where i.run_worker_id=v_rep.run_worker_id
  and i.preparation_revision=v_rep.preparation_revision and i.selection_revision=v_rep.selection_revision
  and (v_rep.cursor_component_id is null or (i.age_key,i.case_id,i.component_ordinal,i.case_component_id)
   >(v_rep.cursor_age_key,v_rep.cursor_case_id,v_rep.cursor_component_ordinal,v_rep.cursor_component_id))) into v_more;
 if not v_more and v_issue is null and v_rep.staged_instruction_count<>v_rep.expected_instruction_count then
  raise exception using errcode='23514',message='BPAY_NEXT_NET_STAGING_COUNT_MISMATCH';end if;
 update private.bpay_next_net_replacement set staged_instruction_count=v_rep.staged_instruction_count,
  staged_deduction_count=v_rep.staged_deduction_count,staged_old_hold_count=v_rep.staged_old_hold_count,staged_old_source_total=v_rep.staged_old_source_total,
  proposed_hold_count=v_rep.proposed_hold_count,proposed_source_total=v_rep.proposed_source_total,recoveries=v_rep.recoveries,
  gross_additions=v_rep.gross_additions,gross_deductions=v_rep.gross_deductions,net_additions=v_rep.net_additions,
  remaining_channel_headroom=v_e,remaining_take_home=v_t,cursor_age_key=v_rep.cursor_age_key,cursor_case_id=v_rep.cursor_case_id,
  cursor_component_ordinal=v_rep.cursor_component_ordinal,cursor_component_id=v_rep.cursor_component_id,
  staging_step=case when not v_more and v_issue is null then 'VALIDATE' else staging_step end,
  updated_at_utc=pg_catalog.transaction_timestamp() where id=v_rep.id;
 return pg_catalog.jsonb_build_object('rows_visited',v_seen::text,'encoded_bytes',v_bytes::text,'issue_code',v_issue);
end $function$;


-- DR-37 cancel
create or replace function private.bpay_next_cancel_net_replacement_page_v1(p_replacement_id uuid,p_cancel_command_id uuid,p_limit integer)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare
 v_r private.bpay_next_net_replacement%rowtype;v_q private.bpay_next_cancel_request%rowtype;
 v_j private.bpay_next_job%rowtype;v_w private.bpay_next_run_worker%rowtype;
 v_i private.bpay_next_run_case_instruction%rowtype;v_h private.bpay_next_case_hold%rowtype;
 v_a private.bpay_next_case_allocation_result%rowtype;v_u private.bpay_next_case_capacity_use%rowtype;
 v_m private.bpay_next_net_cancel_hold_member%rowtype;v_seen integer:=0;v_bytes integer:=0;v_size integer;
 v_more boolean;v_amount numeric;v_block text;v_now timestamptz:=pg_catalog.transaction_timestamp();v_reply jsonb;
begin
 if p_limit is null or p_limit not between 1 and 100 or p_cancel_command_id is null then
  raise exception using errcode='22023',message='BPAY_NEXT_NET_CANCEL_PAGE_INPUT_INVALID';end if;
 select dr37_point_45.* into strict v_r from private.bpay_next_net_replacement dr37_point_45 where id=p_replacement_id for update nowait;
 perform private.bpay_next_net_replacement_job_assert_v1(v_r.id);
 select dr37_point_46.* into strict v_j from private.bpay_next_job dr37_point_46 where id=v_r.original_job_id;
 select dr37_point_47.* into strict v_w from private.bpay_next_run_worker dr37_point_47 where id=v_r.run_worker_id for update nowait;
 select dr37_point_48.* into strict v_q from private.bpay_next_cancel_request dr37_point_48 where command_id=p_cancel_command_id for update nowait;
 if v_r.stage not in ('PRECHECK','STAGING','COMMITTING','CANCEL_REQUESTED','CANCELLING')
  or v_r.target_state_id is null or v_q.status<>'REQUESTED' or v_q.run_worker_id<>v_r.run_worker_id
  or v_q.candidate_id<>v_r.candidate_id or v_w.net_replacement_pending_id is distinct from v_r.id
  or v_w.net_request_revision<>v_r.request_no or v_w.net_projection_revision<>v_r.prior_projection_no
  or (v_r.cancel_command_id is not null and v_r.cancel_command_id<>p_cancel_command_id)
  or not exists(select 1 from private.bpay_next_command c where c.id=p_cancel_command_id
   and c.command_kind='SIMPLE_CANCEL' and c.module_epoch=v_r.module_epoch and c.agency_sequence>v_j.command_sequence
   and c.expected_member_count=1 and c.enrolled_member_count=1)
  or not exists(select 1 from private.bpay_next_command_member m where m.command_id=p_cancel_command_id and m.candidate_id=v_r.candidate_id)
  or v_w.realised_effect_count<>0 or v_w.financial_resolution_count<>0 then
  raise exception using errcode='23514',message='BPAY_NEXT_NET_CANCEL_IDENTITY_INVALID';end if;
 v_block:=private.bpay_next_case_cancel_block_v1(v_w.id,true);
 if v_block is not null then raise exception using errcode='23514',message=v_block;end if;
 perform private.bpay_next_assert_candidate_financial_fence_v1(v_r.candidate_id,v_j.id,v_j.lease_nonce,v_j.owner_epoch);
 if v_r.stage<>'CANCELLING' then
  begin
   insert into private.bpay_next_candidate_week_commit_fence as f(candidate_id,pay_week_start,command_id,run_worker_id,
    original_job_id,module_epoch,allocation_state_id,stage,replacement_id)
    values(v_r.candidate_id,v_r.pay_week_start,v_r.id,v_r.run_worker_id,v_j.id,v_r.module_epoch,v_r.target_state_id,'CANCELLING',v_r.id)
    on conflict(candidate_id,pay_week_start) do update set command_id=excluded.command_id,run_worker_id=excluded.run_worker_id,
     original_job_id=excluded.original_job_id,module_epoch=excluded.module_epoch,allocation_state_id=excluded.allocation_state_id,
     stage='CANCELLING',replacement_id=excluded.replacement_id,updated_at_utc=v_now
    where f.stage in ('COMPLETE','REFUSED','CANCELLED') or f.command_id=excluded.command_id;
   if not found then raise exception using errcode='55P03',message='BPAY_NEXT_CANDIDATE_FINANCIAL_FENCE_BUSY';end if;
  exception when unique_violation then raise exception using errcode='55P03',message='BPAY_NEXT_CANDIDATE_FINANCIAL_FENCE_BUSY';end;
  update private.bpay_next_net_replacement set stage='CANCELLING',cancel_command_id=p_cancel_command_id,updated_at_utc=v_now
   where id=v_r.id returning * into v_r;
  update private.bpay_next_case_allocation_state set net_stage='CANCELLING' where id=v_r.target_state_id;
 end if;
 perform private.bpay_next_net_page_prelock_v1(v_r.id,p_limit,case when v_r.cleanup_step='INVENTORY' then 'INVENTORY' else 'RELEASE' end);
 for v_i in select i.* from private.bpay_next_run_case_instruction i
  where i.run_worker_id=v_r.run_worker_id and i.preparation_revision=v_r.preparation_revision and i.selection_revision=v_r.selection_revision
   and (case when v_r.cleanup_step='INVENTORY' then v_r.inventory_component_id is null
    or (i.age_key,i.case_id,i.component_ordinal,i.case_component_id)>(v_r.inventory_age_key,v_r.inventory_case_id,v_r.inventory_component_ordinal,v_r.inventory_component_id)
    else v_r.cleanup_component_id is null
    or (i.age_key,i.case_id,i.component_ordinal,i.case_component_id)>(v_r.cleanup_age_key,v_r.cleanup_case_id,v_r.cleanup_component_ordinal,v_r.cleanup_component_id) end)
  order by i.age_key,i.case_id,i.component_ordinal,i.case_component_id limit p_limit
 loop
  if v_r.cleanup_step='INVENTORY' then
   select dr37_point_49.* into v_h from private.bpay_next_case_hold dr37_point_49 where instruction_id=v_i.id and status='ACTIVE' for update nowait;
  else
   select dr37_point_50.* into v_m from private.bpay_next_net_cancel_hold_member dr37_point_50 where replacement_id=v_r.id and instruction_id=v_i.id;
   select dr37_point_51.* into v_h from private.bpay_next_case_hold dr37_point_51 where id=v_m.case_hold_id for update nowait;
  end if;
  v_a:=null;v_u:=null;
  if v_h.id is not null then
   select dr37_point_52.* into strict v_a from private.bpay_next_case_allocation_result dr37_point_52 where id=v_h.allocation_result_id;
   select dr37_point_53.* into strict v_u from private.bpay_next_case_capacity_use dr37_point_53 where case_hold_id=v_h.id for update nowait;
  end if;
  v_size:=pg_catalog.octet_length(pg_catalog.jsonb_build_array(pg_catalog.to_jsonb(v_i),pg_catalog.to_jsonb(v_h),pg_catalog.to_jsonb(v_m),
   pg_catalog.to_jsonb(v_a),pg_catalog.to_jsonb(v_u))::text);
  if v_size>65536 then raise exception using errcode='54000',message='BPAY_NEXT_NET_PAGE_ROW_BYTES_EXCEEDED';end if;
  if v_bytes+v_size>65536 then exit;end if;v_bytes:=v_bytes+v_size;v_seen:=v_seen+1;
  if v_h.id is not null then
   if v_h.status<>'ACTIVE' or v_h.run_worker_id<>v_r.run_worker_id or v_h.instruction_id<>v_i.id
    or v_a.instruction_id<>v_i.id or (v_a.state_id not in (v_r.draft_state_id,v_r.target_state_id) and v_a.state_id is distinct from v_r.prior_state_id)
    or v_h.allocation_pass_kind<>v_a.pass_kind or v_h.purpose<>v_i.hold_purpose
    or v_u.status<>'ACTIVE' or v_u.realisation_event_id is not null or v_u.source_amount_ex_vat<>v_h.source_reserved_ex_vat
    or (v_h.source_reserved_ex_vat,v_h.target_amount_ex_vat,v_h.target_amount_vat,v_h.target_amount_inc_vat)
     is distinct from (v_a.allocated_source_ex_vat,v_a.allocated_target_ex_vat,v_a.allocated_target_vat,v_a.allocated_target_inc_vat) then
    raise exception using errcode='23514',message='BPAY_NEXT_NET_CANCEL_HOLD_INVALID';end if;
   v_amount:=v_h.source_reserved_ex_vat;
   if v_r.cleanup_step='INVENTORY' then
    insert into private.bpay_next_net_cancel_hold_member(replacement_id,case_hold_id,instruction_id,allocation_state_id,source_amount)
     values(v_r.id,v_h.id,v_i.id,v_a.state_id,v_amount);
    v_r.expected_releasable_hold_count:=v_r.expected_releasable_hold_count+1;
    v_r.expected_releasable_source_total:=v_r.expected_releasable_source_total+v_amount;
   else
    if (v_m.case_hold_id,v_m.allocation_state_id,v_m.source_amount) is distinct from (v_h.id,v_a.state_id,v_amount) then
     raise exception using errcode='23514',message='BPAY_NEXT_NET_CANCEL_MEMBERSHIP_CHANGED';end if;
    if v_h.purpose='PAYOUT' then
     update private.bpay_next_case_component set active_payout_source_ex_vat=active_payout_source_ex_vat-v_amount,
      component_revision=component_revision+1,updated_at_utc=v_now where id=v_h.case_component_id and active_payout_source_ex_vat>=v_amount;
     if not found then raise exception using errcode='23514',message='BPAY_NEXT_NET_CANCEL_COMPONENT_CAPACITY_INVALID';end if;
     update private.bpay_next_finance_case set active_payout_hold_amount=active_payout_hold_amount-v_amount,active_hold_amount=active_hold_amount-v_amount,
      case_revision=case_revision+1,updated_at_utc=v_now where id=v_h.case_id and active_payout_hold_amount>=v_amount and active_hold_amount>=v_amount;
    else
     update private.bpay_next_case_component set active_recovery_source_ex_vat=active_recovery_source_ex_vat-v_amount,
      component_revision=component_revision+1,updated_at_utc=v_now where id=v_h.case_component_id and active_recovery_source_ex_vat>=v_amount;
     if not found then raise exception using errcode='23514',message='BPAY_NEXT_NET_CANCEL_COMPONENT_CAPACITY_INVALID';end if;
     update private.bpay_next_finance_case set active_recovery_hold_amount=active_recovery_hold_amount-v_amount,active_hold_amount=active_hold_amount-v_amount,
      case_revision=case_revision+1,updated_at_utc=v_now where id=v_h.case_id and active_recovery_hold_amount>=v_amount and active_hold_amount>=v_amount;
    end if;
    if not found then raise exception using errcode='23514',message='BPAY_NEXT_NET_CANCEL_CASE_CAPACITY_INVALID';end if;
    if v_h.purpose='PAYOUT' then
     update private.bpay_next_case_period set active_payout_source_ex_vat=active_payout_source_ex_vat-v_amount,period_revision=period_revision+1
      where case_component_id=v_h.case_component_id and pay_week_start=v_h.pay_week_start and active_payout_source_ex_vat>=v_amount;
    else
     update private.bpay_next_case_period set active_unrealised_recovery_source_ex_vat=active_unrealised_recovery_source_ex_vat-v_amount,period_revision=period_revision+1
      where case_component_id=v_h.case_component_id and pay_week_start=v_h.pay_week_start and active_unrealised_recovery_source_ex_vat>=v_amount;
    end if;
    if not found then raise exception using errcode='23514',message='BPAY_NEXT_NET_CANCEL_PERIOD_CAPACITY_INVALID';end if;
    update private.bpay_next_case_hold set status='RELEASED',finished_at_utc=v_now where id=v_h.id;
    perform private.bpay_next_record_case_hold_transition_v1(v_j.id,v_h.id,'RELEASED');
    update private.bpay_next_case_capacity_use set status='RELEASED' where case_hold_id=v_h.id;
    update private.bpay_next_run_worker set active_case_hold_count=active_case_hold_count-1 where id=v_w.id and active_case_hold_count>0;
    if not found then raise exception using errcode='23514',message='BPAY_NEXT_NET_CANCEL_HOLD_COUNT_INVALID';end if;
    v_r.released_hold_count:=v_r.released_hold_count+1;v_r.released_source_total:=v_r.released_source_total+v_amount;
   end if;
  elsif v_r.cleanup_step='RELEASE' and v_m.case_hold_id is not null then
   raise exception using errcode='23514',message='BPAY_NEXT_NET_CANCEL_MEMBER_MISSING';
  end if;
  if v_r.cleanup_step='INVENTORY' then
   v_r.inventory_instruction_count:=v_r.inventory_instruction_count+1;v_r.inventory_age_key:=v_i.age_key;v_r.inventory_case_id:=v_i.case_id;
   v_r.inventory_component_ordinal:=v_i.component_ordinal;v_r.inventory_component_id:=v_i.case_component_id;
  else
   v_r.cleanup_instruction_count:=v_r.cleanup_instruction_count+1;v_r.cleanup_age_key:=v_i.age_key;v_r.cleanup_case_id:=v_i.case_id;
   v_r.cleanup_component_ordinal:=v_i.component_ordinal;v_r.cleanup_component_id:=v_i.case_component_id;
  end if;
 end loop;
 select exists(select 1 from private.bpay_next_run_case_instruction i
  where i.run_worker_id=v_r.run_worker_id and i.preparation_revision=v_r.preparation_revision and i.selection_revision=v_r.selection_revision
   and (case when v_r.cleanup_step='INVENTORY' then v_r.inventory_component_id is null
    or (i.age_key,i.case_id,i.component_ordinal,i.case_component_id)>(v_r.inventory_age_key,v_r.inventory_case_id,v_r.inventory_component_ordinal,v_r.inventory_component_id)
    else v_r.cleanup_component_id is null
    or (i.age_key,i.case_id,i.component_ordinal,i.case_component_id)>(v_r.cleanup_age_key,v_r.cleanup_case_id,v_r.cleanup_component_ordinal,v_r.cleanup_component_id) end)) into v_more;
 if not v_more then
  if v_r.cleanup_step='INVENTORY' then
   if v_r.inventory_instruction_count<>v_r.expected_instruction_count or v_r.expected_releasable_hold_count<>v_w.active_case_hold_count then
    raise exception using errcode='23514',message='BPAY_NEXT_NET_CANCEL_INVENTORY_COUNT_INVALID';end if;
   v_r.cleanup_step:='RELEASE';
  else
   if v_r.cleanup_instruction_count<>v_r.expected_instruction_count or v_r.released_hold_count<>v_r.expected_releasable_hold_count
    or v_r.released_source_total<>v_r.expected_releasable_source_total
    or (select active_case_hold_count from private.bpay_next_run_worker where id=v_r.run_worker_id)<>0 then
    raise exception using errcode='23514',message='BPAY_NEXT_NET_CANCEL_FINAL_COUNT_INVALID';end if;
   v_r.cleanup_step:='DONE';v_r.stage:='CANCELLED';
  end if;
 end if;
 update private.bpay_next_net_replacement set stage=v_r.stage,cleanup_step=v_r.cleanup_step,
  expected_releasable_hold_count=v_r.expected_releasable_hold_count,expected_releasable_source_total=v_r.expected_releasable_source_total,
  inventory_instruction_count=v_r.inventory_instruction_count,inventory_age_key=v_r.inventory_age_key,inventory_case_id=v_r.inventory_case_id,
  inventory_component_ordinal=v_r.inventory_component_ordinal,inventory_component_id=v_r.inventory_component_id,
  cleanup_instruction_count=v_r.cleanup_instruction_count,cleanup_age_key=v_r.cleanup_age_key,cleanup_case_id=v_r.cleanup_case_id,
  cleanup_component_ordinal=v_r.cleanup_component_ordinal,cleanup_component_id=v_r.cleanup_component_id,
  released_hold_count=v_r.released_hold_count,released_source_total=v_r.released_source_total,
  finished_at_utc=case when v_r.stage='CANCELLED' then v_now else null end,updated_at_utc=v_now where id=v_r.id;
 if v_r.stage='CANCELLED' then
  update private.bpay_next_case_allocation_state set status='CANCELLED',net_stage='CANCELLED' where id=v_r.target_state_id;
  insert into private.bpay_next_net_cancel_handoff(replacement_id,cancel_command_id,original_job_id,run_worker_id,candidate_id,
   draft_state_id,prior_state_id,target_state_id,prior_projection_id,target_projection_id,expected_instruction_count,cleaned_instruction_count,
   expected_hold_count,released_hold_count,expected_source_total,released_source_total,
   cleanup_age_key,cleanup_case_id,cleanup_component_ordinal,cleanup_component_id)
   values(v_r.id,v_r.cancel_command_id,v_r.original_job_id,v_r.run_worker_id,v_r.candidate_id,v_r.draft_state_id,v_r.prior_state_id,
    v_r.target_state_id,v_r.prior_projection_id,v_r.accepted_projection_id,v_r.expected_instruction_count,v_r.cleanup_instruction_count,
    v_r.expected_releasable_hold_count,v_r.released_hold_count,v_r.expected_releasable_source_total,v_r.released_source_total,
    v_r.cleanup_age_key,v_r.cleanup_case_id,v_r.cleanup_component_ordinal,v_r.cleanup_component_id);
  -- Retain pending identity until the later ordered SIMPLE_CANCEL publishes cancellation.
  update private.bpay_next_run_worker set net_request_revision=net_projection_revision where id=v_r.run_worker_id;
  update private.bpay_next_job set status='DONE',phase='DONE',cursor_key=null,lease_nonce=null,lease_until_utc=null where id=v_j.id;
  update private.bpay_next_command set status='COMPLETE' where id=v_j.command_id;
 end if;
 v_reply:=pg_catalog.jsonb_build_object('rows_visited',v_seen::text,'encoded_bytes',v_bytes::text,'complete',v_r.stage='CANCELLED');
 if pg_catalog.octet_length(v_reply::text)>65536 then raise exception using errcode='54000',message='BPAY_NEXT_NET_REPLY_BYTES_EXCEEDED';end if;
 return v_reply;
end $function$;


-- DR-37 owner
create or replace function private.bpay_next_apply_case_paye_net_page_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,p_expected_cursor bigint default null,
  p_limit integer default 100
) returns jsonb
language plpgsql security invoker
set search_path=pg_catalog,private,public
as $function$
declare
  v_epoch bigint;v_run_id uuid;v_candidate uuid;v_control_epoch bigint;v_checkpoint bigint;
  v_run private.bpay_next_pay_run%rowtype;v_worker private.bpay_next_run_worker%rowtype;
  v_job private.bpay_next_job%rowtype;v_request private.bpay_next_paye_net_request%rowtype;
  v_draft private.bpay_next_case_allocation_state%rowtype;v_state private.bpay_next_case_allocation_state%rowtype;
  v_week private.bpay_next_worker_week%rowtype;v_prior private.bpay_next_worker_week_contribution%rowtype;
  v_contribution private.bpay_next_worker_week_contribution%rowtype;
  v_instruction private.bpay_next_run_case_instruction%rowtype;v_draft_result private.bpay_next_case_allocation_result%rowtype;
  v_own private.bpay_next_case_hold%rowtype;v_case private.bpay_next_finance_case%rowtype;
  v_component private.bpay_next_case_component%rowtype;v_period private.bpay_next_case_period%rowtype;
  v_channel private.bpay_next_case_allocation_channel%rowtype;
  v_rep private.bpay_next_net_replacement%rowtype;v_staged private.bpay_next_net_staged_result%rowtype;
  v_carry private.bpay_next_net_staged_case_capacity%rowtype;v_reply jsonb;v_cancel uuid;
  v_bytes integer:=0;v_size integer;
  v_seen integer:=0;v_more boolean;v_issue text;v_terminal boolean:=false;v_requires_week boolean;
  v_e numeric;v_h numeric;v_own_amount numeric;v_capacity numeric;v_taken numeric;v_cash numeric;v_own_cash numeric;
  v_outstanding numeric;v_case_outstanding numeric;v_row record;v_result_id uuid;v_hold_id uuid;v_projection_id uuid;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null or p_owner_epoch<1
     or (p_expected_cursor is not null and p_expected_cursor<1) or p_limit is null or p_limit not between 1 and 100 then
    raise exception using errcode='22023',message='BPAY_NEXT_CASE_NET_PAGE_INPUT_INVALID';
  end if;
  select owner_epoch into v_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share nowait;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select w.run_id,w.candidate_id into strict v_run_id,v_candidate from private.bpay_next_job j
    join private.bpay_next_paye_net_request r on r.command_id=j.command_id
    join private.bpay_next_run_worker w on w.id=r.run_worker_id and w.candidate_id=j.candidate_id
    where j.id=p_job_id and r.case_draft_state_id is not null;
  select dr37_point_54.* into strict v_run from private.bpay_next_pay_run dr37_point_54 where id=v_run_id for update nowait;
  select active_owner_epoch into strict v_control_epoch from private.bpay_next_worker_control where candidate_id=v_candidate for update nowait;
  select dr37_point_55.* into strict v_job from private.bpay_next_job dr37_point_55 where id=p_job_id for update nowait;
  select dr37_point_56.* into strict v_request from private.bpay_next_paye_net_request dr37_point_56 where command_id=v_job.command_id;
  select dr37_point_57.* into strict v_worker from private.bpay_next_run_worker dr37_point_57 where id=v_request.run_worker_id for update nowait;
  select dr37_point_58.* into strict v_draft from private.bpay_next_case_allocation_state dr37_point_58 where id=v_request.case_draft_state_id;
  select dr37_point_59.* into v_state from private.bpay_next_case_allocation_state dr37_point_59 where job_id=p_job_id and pass_kind='NET' for update nowait;
  select dr37_point_60.* into v_rep from private.bpay_next_net_replacement dr37_point_60 where id=v_job.command_id for update nowait;
  if v_job.job_kind<>'PAYE_NET_ENTRY' or v_job.module_epoch<>v_epoch or v_job.candidate_id<>v_candidate
     or v_request.candidate_id<>v_candidate or v_draft.run_worker_id<>v_worker.id or v_draft.pass_kind<>'DRAFT'
     or v_draft.preparation_revision<>v_request.preparation_revision or v_draft.selection_revision<>v_request.selection_revision then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_JOB_SCOPE_INVALID';
  end if;
  -- Retained terminal identity survives lease takeover and later deliberate requests.
  if v_job.status='DONE' and v_rep.id is not null and v_rep.stage in ('COMPLETE','REFUSED','CANCELLED') then
    return pg_catalog.jsonb_build_object('phase',case v_rep.stage when 'COMPLETE' then 'PROJECTED' when 'REFUSED' then 'REFUSED' else 'DONE' end,
      'cursor',null,'rows_visited','0','replay',true,'projection_id',v_rep.accepted_projection_id,
      'run_worker_id',v_worker.id,'issue_code',v_rep.refusal_code,'replacement_id',v_rep.id,'replacement_stage',v_rep.stage,
      'request_no',v_rep.request_no::text,'staged_count',v_rep.staged_instruction_count::text,
      'validated_count',v_rep.validated_instruction_count::text,'committed_count',v_rep.committed_instruction_count::text,
      'expected_count',v_rep.expected_instruction_count::text);
  end if;
  if v_job.status='DONE' and v_rep.id is null and v_state.id is not null and v_state.net_stage in ('COMPLETE','REVIEW') then
    return pg_catalog.jsonb_build_object('phase',case when v_state.net_stage='COMPLETE' then 'PROJECTED' else 'REVIEW' end,
      'cursor',null,'rows_visited','0','replay',true,'projection_id',v_state.projection_id,
      'run_worker_id',v_worker.id,'issue_code',v_state.net_issue_code);
  end if;
  if v_job.status<>'LEASED' or v_job.phase<>'FINANCE_ALLOCATE' or v_job.lease_nonce<>p_lease_nonce
     or v_job.owner_epoch<>p_owner_epoch or v_control_epoch<>p_owner_epoch
     or v_job.lease_until_utc<=pg_catalog.clock_timestamp() or v_run.status<>'DRAFT'
     or v_run.confirmed_at_utc is null or v_worker.status<>'READY'
     or v_worker.captured_preparation_revision<>v_request.preparation_revision or v_worker.case_selection_revision<>v_request.selection_revision
     or v_worker.net_request_revision<>v_request.request_no or v_worker.net_projection_revision<>v_request.expected_projection_revision
     or v_rep.id is null or v_worker.net_replacement_pending_id is distinct from v_rep.id
     or v_worker.net_request_high_water<v_request.request_no
     or v_worker.target_pay_channel<>'PAYE' or v_worker.gross_vat<>0 or v_worker.gross_inc_vat<>v_request.frozen_gross_inc_vat
     or v_worker.realised_effect_count<>0 or v_draft.status<>'COMPLETE' or v_draft.prepare_stage<>'COMPLETE'
     or exists(select 1 from private.bpay_next_cancel_request r join private.bpay_next_command c on c.id=r.command_id
       where r.run_worker_id=v_worker.id and (r.status='CANCELLING'
         or (r.status='REQUESTED' and c.agency_sequence<v_job.command_sequence)))
     or exists(select 1 from private.bpay_next_transfer t where t.run_worker_id=v_worker.id) then
    raise exception using errcode='55000',message='BPAY_NEXT_CASE_NET_OWNER_NOT_ELIGIBLE';
  end if;
  v_checkpoint:=v_job.cursor_key::bigint;
  if p_expected_cursor is distinct from v_checkpoint then
    if coalesce(p_expected_cursor,0)<coalesce(v_checkpoint,0) then
      return pg_catalog.jsonb_build_object('phase','FINANCE_ALLOCATE','cursor',v_job.cursor_key,'rows_visited','0',
        'replay',true,'projection_id',null,'run_worker_id',v_worker.id,'issue_code',null,
        'replacement_id',v_rep.id,'replacement_stage',v_rep.stage,'request_no',v_rep.request_no::text,
        'staged_count',v_rep.staged_instruction_count::text,'validated_count',v_rep.validated_instruction_count::text,
        'committed_count',v_rep.committed_instruction_count::text,'expected_count',v_rep.expected_instruction_count::text);
    end if;
    raise exception using errcode='55000',message='BPAY_NEXT_CASE_NET_CURSOR_STALE';
  end if;

  <<net_page>>
  begin
    if v_state.id is null then
      select dr37_point_61.* into strict v_week from private.bpay_next_worker_week dr37_point_61
        where candidate_id=v_candidate and pay_week_start=v_draft.pay_week_start for update nowait;
      -- Derive protected-week dependence in finite PRECHECK instruction pages.
      v_requires_week:=false;
      insert into private.bpay_next_case_allocation_state(run_worker_id,run_id,candidate_id,selection_revision,preparation_revision,
        pass_kind,projection_no,command_id,job_id,module_epoch,owner_epoch,financial_view_revision,status,weekly_binding_state,
        expected_instruction_count,initial_worker_take_home,remaining_worker_take_home,pay_week_start,captured_week_revision,
        captured_work_gross,net_stage,net_requires_week_binding)
        values(v_worker.id,v_run_id,v_candidate,v_request.selection_revision,v_request.preparation_revision,'NET',v_request.request_no,
          v_job.command_id,v_job.id,v_epoch,p_owner_epoch,
          (select financial_view_revision from private.bpay_next_worker_control where candidate_id=v_candidate),'BUILDING',
          case when v_requires_week then 'UNBOUND' else 'NOT_AFFECTED' end,v_draft.expected_instruction_count,
          coalesce(v_request.entered_paye_net,0),coalesce(v_request.entered_paye_net,0),v_draft.pay_week_start,v_week.period_revision,
          v_draft.captured_work_gross,'PRECHECK',v_requires_week)
        returning * into v_state;
      insert into private.bpay_next_case_allocation_channel(state_id,allocation_channel,initial_headroom,remaining_headroom)
        values(v_state.id,'PAYE',coalesce(v_request.entered_paye_net,0),coalesce(v_request.entered_paye_net,0));
      update private.bpay_next_net_replacement set original_job_id=p_job_id,target_state_id=v_state.id,
        updated_at_utc=pg_catalog.transaction_timestamp() where id=v_rep.id returning * into v_rep;
    elsif v_state.status<>'BUILDING' or v_state.net_stage='NOT_CASE_NET' or v_state.projection_no<>v_request.request_no then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_STATE_INVALID';
    end if;
    if v_state.owner_epoch<>p_owner_epoch then
      update private.bpay_next_case_allocation_state set owner_epoch=p_owner_epoch where id=v_state.id;
    end if;
    select dr37_point_62.* into strict v_week from private.bpay_next_worker_week dr37_point_62
      where candidate_id=v_candidate and pay_week_start=v_state.pay_week_start for update nowait;
    select q.command_id into v_cancel from private.bpay_next_cancel_request q
      join private.bpay_next_command c on c.id=q.command_id
      where q.run_worker_id=v_worker.id and q.status='REQUESTED' and c.agency_sequence>v_job.command_sequence
      order by c.agency_sequence limit 1;
    if v_cancel is not null or v_rep.stage in ('CANCEL_REQUESTED','CANCELLING') then
      v_reply:=private.bpay_next_cancel_net_replacement_page_v1(v_rep.id,coalesce(v_rep.cancel_command_id,v_cancel),p_limit);
      v_seen:=(v_reply->>'rows_visited')::integer;v_terminal:=(v_reply->>'complete')::boolean;exit net_page;
    end if;
    if v_state.net_stage='PRECHECK' then
      v_reply:=private.bpay_next_net_precheck_page_v1(v_rep.id,p_limit);
      v_seen:=(v_reply->>'rows_visited')::integer;v_issue:=v_reply->>'issue_code';exit net_page;
    end if;
    if v_state.net_requires_week_binding and v_week.period_revision<>v_state.captured_week_revision then
      v_issue:='CASE_WEEK_BASIS_UNBOUND';exit net_page;
    end if;
    if v_state.net_stage='WEEK_CAPTURE' then
      for v_prior in select x.* from private.bpay_next_worker_week_contribution x
        where x.candidate_id=v_candidate and x.pay_week_start=v_state.pay_week_start
          and (x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id)<(v_run.pay_date,v_run.created_at_utc,v_worker.id)
          and (v_state.prior_worker_cursor is null or (x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id)
            >(v_state.prior_pay_date_cursor,v_state.prior_created_at_cursor,v_state.prior_worker_cursor))
        order by x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id limit p_limit
      loop
        v_size:=pg_catalog.octet_length(pg_catalog.to_jsonb(v_prior)::text);
        if v_size>65536 then raise exception using errcode='54000',message='BPAY_NEXT_NET_PAGE_ROW_BYTES_EXCEEDED';end if;
        if v_bytes+v_size>65536 then exit;end if;
        v_bytes:=v_bytes+v_size;v_seen:=v_seen+1;
        if v_prior.eligibility_state='UNBOUND' then
          v_issue:=case when v_prior.payment_state in ('RETURNED_OWED','REISSUED_PAID') then 'CASE_RETURN_FLOOR_UNBOUND'
            else 'CASE_WEEK_BASIS_UNBOUND' end;exit net_page;
        end if;
        v_state.captured_prior_take_home:=v_state.captured_prior_take_home+v_prior.eligible_arranged_amount;
        v_state.initial_worker_take_home:=v_state.initial_worker_take_home+v_prior.eligible_arranged_amount;
        v_state.remaining_worker_take_home:=v_state.remaining_worker_take_home+v_prior.eligible_arranged_amount;
        v_state.prior_pay_date_cursor:=v_prior.original_pay_date;v_state.prior_created_at_cursor:=v_prior.original_created_at_utc;
        v_state.prior_worker_cursor:=v_prior.original_run_worker_id;
      end loop;
      select exists(select 1 from private.bpay_next_worker_week_contribution x
        where x.candidate_id=v_candidate and x.pay_week_start=v_state.pay_week_start
          and (x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id)<(v_run.pay_date,v_run.created_at_utc,v_worker.id)
          and (v_state.prior_worker_cursor is null or (x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id)
            >(v_state.prior_pay_date_cursor,v_state.prior_created_at_cursor,v_state.prior_worker_cursor))) into v_more;
      update private.bpay_next_case_allocation_state set captured_prior_take_home=v_state.captured_prior_take_home,
        initial_worker_take_home=v_state.initial_worker_take_home,remaining_worker_take_home=v_state.remaining_worker_take_home,
        prior_pay_date_cursor=v_state.prior_pay_date_cursor,prior_created_at_cursor=v_state.prior_created_at_cursor,
        prior_worker_cursor=v_state.prior_worker_cursor,weekly_binding_state=case when v_more then 'UNBOUND' else 'BOUND' end,
        net_stage=case when v_more then 'WEEK_CAPTURE' else 'ALLOCATE' end where id=v_state.id;
      -- Week capture and allocation never share a >limit step.
      exit net_page;
    end if;
    if v_state.weekly_binding_state='UNBOUND' then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_ALLOCATION_UNBOUND';end if;
    if v_rep.stage='PRECHECK' and v_state.net_stage='ALLOCATE' then
      update private.bpay_next_net_replacement set stage='STAGING',staging_step='INSTRUCTIONS',
        initial_take_home=v_state.initial_worker_take_home,remaining_take_home=v_state.remaining_worker_take_home,
        updated_at_utc=pg_catalog.transaction_timestamp() where id=v_rep.id returning * into v_rep;
      update private.bpay_next_case_allocation_state set net_stage='STAGING' where id=v_state.id;
    end if;
    if v_rep.stage='STAGING' and v_rep.staging_step='INSTRUCTIONS' then
      v_reply:=private.bpay_next_stage_case_net_page_v1(v_rep.id,p_limit);
      v_seen:=(v_reply->>'rows_visited')::integer;v_issue:=v_reply->>'issue_code';exit net_page;
    elsif v_rep.stage='STAGING' and v_rep.staging_step='VALIDATE' then
      perform private.bpay_next_assert_candidate_financial_fence_v1(v_candidate,p_job_id,p_lease_nonce,p_owner_epoch);
      begin
        insert into private.bpay_next_candidate_week_commit_fence as f(candidate_id,pay_week_start,command_id,run_worker_id,
          original_job_id,module_epoch,allocation_state_id,stage,replacement_id)
          values(v_candidate,v_state.pay_week_start,v_rep.id,v_worker.id,p_job_id,v_epoch,v_state.id,'STAGING',v_rep.id)
          on conflict(candidate_id,pay_week_start) do update set command_id=excluded.command_id,run_worker_id=excluded.run_worker_id,
            original_job_id=excluded.original_job_id,module_epoch=excluded.module_epoch,allocation_state_id=excluded.allocation_state_id,
            stage='STAGING',replacement_id=excluded.replacement_id,updated_at_utc=pg_catalog.transaction_timestamp()
          where f.stage in ('COMPLETE','REFUSED','CANCELLED') or f.command_id=excluded.command_id;
        if not found then raise exception using errcode='55P03',message='BPAY_NEXT_CANDIDATE_FINANCIAL_FENCE_BUSY';end if;
      exception when unique_violation then
        raise exception using errcode='55P03',message='BPAY_NEXT_CANDIDATE_FINANCIAL_FENCE_BUSY';end;
      v_reply:=private.bpay_next_validate_case_net_page_v1(v_rep.id,p_limit);
      v_seen:=(v_reply->>'rows_visited')::integer;v_issue:=v_reply->>'issue_code';
      select dr37_point_63.* into strict v_rep from private.bpay_next_net_replacement dr37_point_63 where id=v_rep.id;
      if v_issue is null and v_rep.validation_step='DONE' then
        update private.bpay_next_net_replacement set stage='COMMITTING',staging_step='DONE',
          updated_at_utc=pg_catalog.transaction_timestamp() where id=v_rep.id;
        update private.bpay_next_case_allocation_state set net_stage='COMMITTING' where id=v_state.id;
        update private.bpay_next_candidate_week_commit_fence set stage='COMMITTING',updated_at_utc=pg_catalog.transaction_timestamp()
          where replacement_id=v_rep.id and candidate_id=v_candidate and pay_week_start=v_state.pay_week_start;
      end if;
      exit net_page;
    end if;
    if v_rep.stage<>'COMMITTING' or v_state.net_stage<>'COMMITTING' then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_COMMIT_STAGE_INVALID';end if;
    perform private.bpay_next_assert_candidate_financial_fence_v1(v_candidate,p_job_id,p_lease_nonce,p_owner_epoch);
    select dr37_point_64.* into strict v_channel from private.bpay_next_case_allocation_channel dr37_point_64
      where state_id=v_state.id and allocation_channel='PAYE' for update nowait;
    perform private.bpay_next_net_page_prelock_v1(v_rep.id,p_limit,'COMMIT');
    v_e:=v_channel.remaining_headroom;v_h:=v_state.remaining_worker_take_home;
    for v_instruction in select i.* from private.bpay_next_run_case_instruction i
      where i.run_worker_id=v_worker.id and i.preparation_revision=v_request.preparation_revision
        and i.selection_revision=v_request.selection_revision
        and (v_state.cursor_case_component_id is null or (i.age_key,i.case_id,i.component_ordinal,i.case_component_id)
          >(v_state.cursor_age_key,v_state.cursor_case_id,v_state.cursor_component_ordinal,v_state.cursor_case_component_id))
      order by i.age_key,i.case_id,i.component_ordinal,i.case_component_id limit p_limit
    loop
      select dr37_point_65.* into strict v_staged from private.bpay_next_net_staged_result dr37_point_65
        where replacement_id=v_rep.id and instruction_id=v_instruction.id;
      v_size:=pg_catalog.octet_length(pg_catalog.jsonb_build_array(pg_catalog.to_jsonb(v_staged),pg_catalog.to_jsonb(v_instruction))::text);
      if v_size>65536 then raise exception using errcode='54000',message='BPAY_NEXT_NET_PAGE_ROW_BYTES_EXCEEDED';end if;
      if v_bytes+v_size>65536 then exit;end if;
      v_bytes:=v_bytes+v_size;v_seen:=v_seen+1;
      if v_staged.result_no<>v_state.processed_instruction_count+1 then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_COMMIT_SEQUENCE_INVALID';end if;
      select dr37_point_66.* into strict v_draft_result from private.bpay_next_case_allocation_result dr37_point_66
        where state_id=v_draft.id and instruction_id=v_instruction.id;
      if v_instruction.source_pay_channel<>'PAYE' or v_instruction.target_pay_channel<>'PAYE'
         or v_instruction.allocation_channel<>'PAYE' or v_instruction.currency<>'GBP'
         or v_instruction.nominal_target_vat<>0 or v_draft_result.allocated_target_vat<>0
         or v_draft_result.allocated_source_ex_vat<>v_draft_result.allocated_target_ex_vat then
        v_issue:='CASE_INPUT_UNBOUND';exit net_page;
      end if;
      select dr37_point_67.* into v_own from private.bpay_next_case_hold dr37_point_67 where instruction_id=v_instruction.id and status='ACTIVE' for update nowait;
      v_own_amount:=coalesce(v_own.source_reserved_ex_vat,0);
      if (v_own.id,v_own_amount) is distinct from (v_staged.prior_hold_id,v_staged.prior_source_amount) then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_COMMIT_OWN_HOLD_CHANGED';end if;
      if v_own.id is not null and (v_own.run_worker_id<>v_worker.id or v_own.purpose<>v_instruction.hold_purpose
          or v_own.source_reserved_ex_vat<>v_own.target_amount_ex_vat
          or v_own.target_amount_vat<>0 or not exists(select 1 from private.bpay_next_case_capacity_use u
            where u.case_hold_id=v_own.id and u.status='ACTIVE' and u.source_amount_ex_vat=v_own_amount)) then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_OWN_HOLD_INVALID';
      end if;
      if v_instruction.payroll_stage<>'NET_DEDUCT' then
        -- Gross deductions/additions and payout additions remain the exact
        -- Draft result/hold. They neither enter the kernel nor seed E/H.
        if v_own_amount<>v_draft_result.allocated_source_ex_vat
           or (v_own_amount>0 and (v_own.allocation_result_id<>v_draft_result.id or v_own.allocation_pass_kind<>'DRAFT')) then
          raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_FROZEN_GROSS_OR_ADDITION_CHANGED';
        end if;
        if v_instruction.payroll_stage='GROSS_ADD' then
          v_state.allocated_gross_additions:=v_state.allocated_gross_additions+v_draft_result.allocated_target_ex_vat;
        elsif v_instruction.payroll_stage='GROSS_DEDUCT' then
          v_state.allocated_gross_deductions:=v_state.allocated_gross_deductions+v_draft_result.allocated_target_ex_vat;
        elsif v_instruction.payroll_stage='NET_ADD' then
          v_state.allocated_net_additions:=v_state.allocated_net_additions+v_draft_result.allocated_target_inc_vat;
          perform private.bpay_next_accumulate_net_destination_v1(v_state.id,v_instruction.id,v_draft_result.id);
        else raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_STAGE_INVALID';end if;
      else
        select dr37_point_68.* into strict v_case from private.bpay_next_finance_case dr37_point_68 where id=v_instruction.case_id for update nowait;
        select dr37_point_69.* into strict v_component from private.bpay_next_case_component dr37_point_69 where id=v_instruction.case_component_id for update nowait;
        select dr37_point_70.* into strict v_period from private.bpay_next_case_period dr37_point_70
          where case_component_id=v_instruction.case_component_id and pay_week_start=v_instruction.pay_week_start for update nowait;
        if v_own_amount>v_component.active_recovery_source_ex_vat
           or v_own_amount>v_case.active_recovery_hold_amount
           or v_own_amount>v_period.active_unrealised_recovery_source_ex_vat
           or (v_own.id is not null and not (
             (v_request.expected_projection_revision=0 and v_own.allocation_pass_kind='DRAFT'
               and v_own.allocation_result_id=v_draft_result.id)
             or (v_request.expected_projection_revision>0 and v_own.allocation_pass_kind='NET' and exists(
               select 1 from private.bpay_next_case_allocation_result a
                 join private.bpay_next_case_allocation_state s on s.id=a.state_id
               where a.id=v_own.allocation_result_id and s.run_worker_id=v_worker.id
                 and s.pass_kind='NET' and s.projection_no=v_request.expected_projection_revision
                 and s.status='COMPLETE' and s.net_stage='COMPLETE' and s.projection_id is not null)))) then
          raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_SURVIVING_CAPACITY_INVALID';
        end if;
        select dr37_point_71.* into strict v_carry from private.bpay_next_net_staged_case_capacity dr37_point_71
          where replacement_id=v_rep.id and case_id=v_instruction.case_id for update nowait;
        if v_case.case_revision<>v_staged.captured_case_revision+v_carry.committed_instruction_count
          or v_component.component_revision<>v_staged.captured_component_revision or v_period.period_revision<>v_staged.captured_period_revision
          or v_case.active_recovery_hold_amount<>v_carry.captured_active_recovery_source-v_carry.committed_release_source+v_carry.committed_reserve_source then
          raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_COMMIT_DEPENDENCY_CHANGED';end if;
        -- Current disjoint CAPACITY only, never a current rule, valuation,
        -- schedule, nominal, tax, channel or changed-case fallback.
        v_outstanding:=case when v_instruction.case_kind in ('LOAN','ADVANCE') then v_component.funded_source_ex_vat
          else v_component.approved_source_ex_vat end-v_component.recovered_source_ex_vat-v_component.written_off_source_ex_vat;
        v_case_outstanding:=case when v_instruction.case_kind in ('LOAN','ADVANCE') then v_case.principal_funded
          else v_case.principal_approved end-v_case.principal_recovered-v_case.principal_written_off;
        v_capacity:=least(v_draft_result.allocated_source_ex_vat,
          greatest(v_period.opening_due_source_ex_vat-v_period.realised_recovery_source_ex_vat
            -v_period.active_unrealised_recovery_source_ex_vat+v_own_amount,0),
          greatest(v_outstanding-v_component.active_recovery_source_ex_vat+v_own_amount,0),
          greatest(v_case_outstanding-v_case.active_recovery_hold_amount+v_own_amount,0));
        if v_own_amount>v_draft_result.allocated_source_ex_vat then
          raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_DRAFT_CAPACITY_EXCEEDED';
        end if;
        if v_capacity<>v_staged.usable_capacity_target_ex_vat then
          raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_COMMIT_CAPACITY_CHANGED';end if;
        select v_staged.allocated_target_ex_vat taken_amount,v_staged.channel_headroom_after next_channel_headroom,
          v_staged.worker_take_home_after next_worker_take_home,v_staged.cap_reason cap_reason,
          v_staged.affordability_reason affordability_reason,v_staged.capacity_reason capacity_reason into v_row;
        if (v_e,v_h) is distinct from (v_staged.channel_headroom_before,v_staged.worker_take_home_before) then
          raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_COMMIT_CARRY_CHANGED';end if;
        v_taken:=v_row.taken_amount;
        -- 0310 requires release/new exact rows, not an amount rewrite. Old
        -- reservation and all three active counters move in this same page.
        if v_own.id is not null then
          update private.bpay_next_case_hold set status='RELEASED',finished_at_utc=pg_catalog.transaction_timestamp() where id=v_own.id;
    perform private.bpay_next_record_case_hold_transition_v1(p_job_id,v_own.id,'RELEASED');
          update private.bpay_next_case_capacity_use set status='RELEASED' where case_hold_id=v_own.id;
        end if;
        insert into private.bpay_next_case_allocation_result(id,state_id,instruction_id,run_worker_id,candidate_id,
          preparation_revision,selection_revision,result_no,pass_kind,allocation_channel,hold_purpose,nominal_target_ex_vat,
          usable_capacity_target_ex_vat,allocated_source_ex_vat,allocated_target_ex_vat,allocated_target_vat,allocated_target_inc_vat,
          shortfall_target_ex_vat,cap_reason,affordability_reason,capacity_reason,channel_headroom_before,channel_headroom_after,
          worker_take_home_before,worker_take_home_after)
          values(v_staged.id,v_state.id,v_instruction.id,v_worker.id,v_candidate,v_request.preparation_revision,v_request.selection_revision,
            v_state.processed_instruction_count+1,'NET','PAYE','NET_RECOVERY_CAPACITY',v_instruction.nominal_target_ex_vat,
            v_capacity,v_taken,v_taken,0,v_taken,v_instruction.nominal_target_ex_vat-v_taken,v_row.cap_reason,
            v_row.affordability_reason,v_row.capacity_reason,v_e,v_row.next_channel_headroom,v_h,v_row.next_worker_take_home)
          returning id into v_result_id;
        if v_taken>0 then
          insert into private.bpay_next_case_hold(run_worker_id,candidate_id,case_id,amount,status,instruction_id,
            allocation_result_id,allocation_pass_kind,case_component_id,purpose,pay_week_start,source_reserved_ex_vat,
            target_amount_ex_vat,target_amount_vat,target_amount_inc_vat)
            values(v_worker.id,v_candidate,v_instruction.case_id,v_taken,'ACTIVE',v_instruction.id,v_result_id,'NET',
              v_instruction.case_component_id,'NET_RECOVERY_CAPACITY',v_instruction.pay_week_start,v_taken,v_taken,0,v_taken)
            returning id into v_hold_id;
    perform private.bpay_next_record_case_hold_transition_v1(p_job_id,v_hold_id,'ACTIVE');
          insert into private.bpay_next_case_capacity_use(case_hold_id,case_component_id,case_id,candidate_id,pay_week_start,
            purpose,status,source_amount_ex_vat) values(v_hold_id,v_instruction.case_component_id,v_instruction.case_id,v_candidate,
              v_instruction.pay_week_start,'NET_RECOVERY_CAPACITY','ACTIVE',v_taken);
        end if;
        update private.bpay_next_case_component set active_recovery_source_ex_vat=active_recovery_source_ex_vat-v_own_amount+v_taken,
          component_revision=component_revision+1,updated_at_utc=pg_catalog.transaction_timestamp() where id=v_component.id;
        update private.bpay_next_finance_case set active_recovery_hold_amount=active_recovery_hold_amount-v_own_amount+v_taken,
          active_hold_amount=active_hold_amount-v_own_amount+v_taken,case_revision=case_revision+1,
          updated_at_utc=pg_catalog.transaction_timestamp() where id=v_case.id;
        update private.bpay_next_case_period set active_unrealised_recovery_source_ex_vat=active_unrealised_recovery_source_ex_vat-v_own_amount+v_taken,
          period_revision=period_revision+1 where case_component_id=v_component.id and pay_week_start=v_instruction.pay_week_start;
        update private.bpay_next_run_worker set active_case_hold_count=active_case_hold_count
          -case when v_own.id is null then 0 else 1 end+case when v_taken>0 then 1 else 0 end where id=v_worker.id;
        update private.bpay_next_net_staged_case_capacity set committed_instruction_count=committed_instruction_count+1,
          committed_release_source=committed_release_source+v_own_amount,committed_reserve_source=committed_reserve_source+v_taken
          where replacement_id=v_rep.id and case_id=v_instruction.case_id;
        v_rep.committed_deduction_count:=v_rep.committed_deduction_count+1;
        v_rep.committed_old_hold_count:=v_rep.committed_old_hold_count+case when v_own.id is null then 0 else 1 end;
        v_rep.committed_old_source_total:=v_rep.committed_old_source_total+v_own_amount;
        v_rep.committed_new_hold_count:=v_rep.committed_new_hold_count+case when v_taken>0 then 1 else 0 end;
        v_rep.committed_new_source_total:=v_rep.committed_new_source_total+v_taken;
        v_state.recovered_source_total:=v_state.recovered_source_total+v_taken;
        v_state.recovered_target_total:=v_state.recovered_target_total+v_taken;
        v_e:=v_row.next_channel_headroom;v_h:=v_row.next_worker_take_home;
      end if;
      v_state.processed_instruction_count:=v_state.processed_instruction_count+1;
      v_rep.committed_instruction_count:=v_rep.committed_instruction_count+1;v_rep.commit_cursor:=v_staged.result_no;
      v_state.cursor_age_key:=v_instruction.age_key;v_state.cursor_case_id:=v_instruction.case_id;
      v_state.cursor_component_ordinal:=v_instruction.component_ordinal;v_state.cursor_case_component_id:=v_instruction.case_component_id;
    end loop;
    update private.bpay_next_case_allocation_channel set remaining_headroom=v_e where state_id=v_state.id and allocation_channel='PAYE';
    update private.bpay_next_case_allocation_state set processed_instruction_count=v_state.processed_instruction_count,
      remaining_worker_take_home=v_h,recovered_source_total=v_state.recovered_source_total,recovered_target_total=v_state.recovered_target_total,
      cursor_age_key=v_state.cursor_age_key,cursor_case_id=v_state.cursor_case_id,cursor_component_ordinal=v_state.cursor_component_ordinal,
      cursor_case_component_id=v_state.cursor_case_component_id,allocated_gross_additions=v_state.allocated_gross_additions,
      allocated_gross_deductions=v_state.allocated_gross_deductions,allocated_net_additions=v_state.allocated_net_additions where id=v_state.id;
    select exists(select 1 from private.bpay_next_run_case_instruction i where i.run_worker_id=v_worker.id
      and i.preparation_revision=v_request.preparation_revision and i.selection_revision=v_request.selection_revision
      and (v_state.cursor_case_component_id is null or (i.age_key,i.case_id,i.component_ordinal,i.case_component_id)
        >(v_state.cursor_age_key,v_state.cursor_case_id,v_state.cursor_component_ordinal,v_state.cursor_case_component_id))) into v_more;
    update private.bpay_next_net_replacement set committed_instruction_count=v_rep.committed_instruction_count,commit_cursor=v_rep.commit_cursor,
      committed_deduction_count=v_rep.committed_deduction_count,committed_old_hold_count=v_rep.committed_old_hold_count,
      committed_old_source_total=v_rep.committed_old_source_total,committed_new_hold_count=v_rep.committed_new_hold_count,
      committed_new_source_total=v_rep.committed_new_source_total,updated_at_utc=pg_catalog.transaction_timestamp() where id=v_rep.id;
    if not v_more then
      if (v_rep.staged_instruction_count,v_rep.validated_instruction_count,v_rep.committed_instruction_count)
        is distinct from (v_rep.expected_instruction_count,v_rep.expected_instruction_count,v_rep.expected_instruction_count)
        or (v_rep.committed_deduction_count,v_rep.committed_old_hold_count,v_rep.committed_old_source_total,v_rep.committed_new_hold_count,v_rep.committed_new_source_total)
          is distinct from (v_rep.staged_deduction_count,v_rep.staged_old_hold_count,v_rep.staged_old_source_total,v_rep.proposed_hold_count,v_rep.proposed_source_total)
        or v_state.recovered_source_total<>v_rep.recoveries or v_state.recovered_target_total<>v_rep.recoveries then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_COMMIT_TOTALS_MISMATCH';end if;
      if v_state.processed_instruction_count<>v_state.expected_instruction_count
         or (v_state.allocated_gross_additions,v_state.allocated_gross_deductions,v_state.allocated_net_additions)
           is distinct from (v_draft.allocated_gross_additions,v_draft.allocated_gross_deductions,v_draft.allocated_net_additions) then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_FROZEN_TOTALS_MISMATCH';
      end if;
      v_cash:=coalesce(v_request.entered_paye_net,0)+v_state.allocated_net_additions-v_state.recovered_target_total;
      if v_cash<0 or v_cash>=10000000000000000 then
        raise exception using errcode='22023',message='BPAY_NEXT_CASE_NET_CASH_RANGE_INVALID';
      end if;
      select dr37_point_72.* into strict v_contribution from private.bpay_next_worker_week_contribution dr37_point_72 where original_run_worker_id=v_worker.id for update nowait;
      if v_contribution.eligibility_state='UNBOUND' or v_contribution.payment_state<>'ARRANGED'
         or v_contribution.original_gross_amount<>v_request.frozen_gross_inc_vat then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_OWN_WEEK_BASIS_INVALID';
      end if;
      insert into private.bpay_next_net_projection(run_worker_id,request_command_id,projection_no,input_kind,
        gross_ex_vat,gross_vat,gross_inc_vat,entered_paye_net,accepted_recoveries,cash_amount,
        accepted_net_additions,accepted_gross_additions,accepted_gross_deductions)
        values(v_worker.id,v_job.command_id,v_request.request_no,v_request.input_kind,v_worker.gross_ex_vat,0,
          v_worker.gross_inc_vat,v_request.entered_paye_net,v_state.recovered_target_total,v_cash,
          v_state.allocated_net_additions,v_state.allocated_gross_additions,v_state.allocated_gross_deductions)
        returning id into v_projection_id;
      v_own_cash:=private.bpay_next_seal_net_destination_v1(v_state.id,v_projection_id);
      if v_request.expected_projection_revision>0 then
        update private.bpay_next_net_projection set retired_at_utc=pg_catalog.transaction_timestamp()
          where run_worker_id=v_worker.id and projection_no=v_request.expected_projection_revision and retired_at_utc is null;
        if not found then raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_PRIOR_SWITCH_INVALID';end if;
      end if;
      update private.bpay_next_worker_week_contribution set
        basis_kind=case when v_request.input_kind='CASE_PAYOUT' then 'GROSS_FALLBACK' else 'PAYROLL_NET' end,
        original_payroll_net=v_request.entered_paye_net,accepted_projection_id=v_projection_id,accepted_bank_cash=v_cash,
        eligibility_state=case when v_request.input_kind='CASE_PAYOUT' then 'EXCLUDED' else 'ELIGIBLE' end,
        eligible_arranged_amount=case when v_request.input_kind='CASE_PAYOUT' then 0 else v_own_cash end,
        contribution_revision=contribution_revision+1,primary_binding_revision=primary_binding_revision+1
        where id=v_contribution.id;
      update private.bpay_next_worker_week set resolved_arranged_take_home=resolved_arranged_take_home
        -v_contribution.eligible_arranged_amount+case when v_request.input_kind='CASE_PAYOUT' then 0 else v_own_cash end,
        period_revision=period_revision+1 where candidate_id=v_candidate and pay_week_start=v_state.pay_week_start;
      if v_request.input_kind='CASE_PAYOUT' then
        update private.bpay_next_case_payout_basis set projection_id=v_projection_id
          where run_worker_id=v_worker.id and preparation_revision=v_request.preparation_revision and projection_id is null;
        if not found then raise exception using errcode='23514',message='BPAY_NEXT_CASE_PAYOUT_SWITCH_INVALID';end if;
      end if;
      update private.bpay_next_case_allocation_state set status='COMPLETE',net_stage='COMPLETE',projection_id=v_projection_id,
        completed_at_utc=pg_catalog.transaction_timestamp() where id=v_state.id;
      update private.bpay_next_net_replacement set stage='COMPLETE',accepted_projection_id=v_projection_id,cash_amount=v_cash,
        finished_at_utc=pg_catalog.transaction_timestamp(),updated_at_utc=pg_catalog.transaction_timestamp() where id=v_rep.id;
      update private.bpay_next_candidate_week_commit_fence set stage='COMPLETE',updated_at_utc=pg_catalog.transaction_timestamp()
        where replacement_id=v_rep.id and candidate_id=v_candidate and pay_week_start=v_state.pay_week_start;
      update private.bpay_next_run_worker set entered_paye_net=v_request.entered_paye_net,net_replacement_pending_id=null,
        net_projection_revision=v_request.request_no where id=v_worker.id;
      update private.bpay_next_job set status='DONE',phase='PROJECTED',cursor_key=null,lease_nonce=null,lease_until_utc=null where id=p_job_id;
      update private.bpay_next_command set status='COMPLETE' where id=v_job.command_id;
      v_terminal:=true;
    end if;
  end net_page;
  select dr37_point_73.* into strict v_rep from private.bpay_next_net_replacement dr37_point_73 where id=v_rep.id;
  if v_issue is not null then
    if v_rep.stage not in ('PRECHECK','STAGING') or v_rep.committed_instruction_count<>0 then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_LATE_REFUSAL_FORBIDDEN';end if;
    update private.bpay_next_net_replacement set stage='REFUSED',refusal_code=v_issue,
      finished_at_utc=pg_catalog.transaction_timestamp(),updated_at_utc=pg_catalog.transaction_timestamp() where id=v_rep.id;
    update private.bpay_next_case_allocation_state set net_stage='REFUSED',net_issue_code=v_issue,weekly_binding_state='UNBOUND',status='OUTDATED' where id=v_state.id;
    update private.bpay_next_candidate_week_commit_fence set stage='REFUSED',updated_at_utc=pg_catalog.transaction_timestamp()
      where replacement_id=v_rep.id and candidate_id=v_candidate and pay_week_start=v_state.pay_week_start;
    update private.bpay_next_run_worker set net_request_revision=net_projection_revision,net_replacement_pending_id=null,review_issue_code=null,review_issue_work_id=null where id=v_worker.id;
    insert into private.bpay_next_paye_net_request_refusal(command_id,run_worker_id,candidate_id,request_no,refusal_code)
      values(v_rep.id,v_worker.id,v_candidate,v_rep.request_no,v_issue);
    update private.bpay_next_run_worker set review_issue_code=v_issue,review_issue_work_id=null where id=v_worker.id;
    update private.bpay_next_job set status='DONE',phase='DONE',cursor_key=null,lease_nonce=null,lease_until_utc=null where id=p_job_id;
    update private.bpay_next_command set status='COMPLETE' where id=v_job.command_id;
    v_terminal:=true;
  elsif not v_terminal then
    update private.bpay_next_job set cursor_key=(coalesce(v_checkpoint,0)+1)::text where id=p_job_id;
  end if;
  update private.bpay_next_worker_control set financial_view_revision=financial_view_revision+1,
    updated_at_utc=pg_catalog.transaction_timestamp() where candidate_id=v_candidate;
  select dr37_point_74.* into strict v_job from private.bpay_next_job dr37_point_74 where id=p_job_id;
  select dr37_point_75.* into strict v_rep from private.bpay_next_net_replacement dr37_point_75 where id=v_rep.id;
  v_reply:=pg_catalog.jsonb_build_object('phase',case v_rep.stage when 'REFUSED' then 'REFUSED' when 'COMPLETE' then 'PROJECTED'
      when 'CANCELLED' then 'DONE' else 'FINANCE_ALLOCATE' end,'cursor',v_job.cursor_key,
    'rows_visited',v_seen::text,'replay',false,'projection_id',v_rep.accepted_projection_id,'run_worker_id',v_worker.id,'issue_code',v_rep.refusal_code,
    'replacement_id',v_rep.id,'replacement_stage',v_rep.stage,'request_no',v_rep.request_no::text,
    'staged_count',v_rep.staged_instruction_count::text,'validated_count',v_rep.validated_instruction_count::text,
    'committed_count',v_rep.committed_instruction_count::text,'expected_count',v_rep.expected_instruction_count::text);
  if pg_catalog.octet_length(v_reply::text)>65536 then raise exception using errcode='54000',message='BPAY_NEXT_NET_REPLY_BYTES_EXCEEDED';end if;
  return v_reply;
end
$function$;


-- DR-37 fence
create or replace function private.bpay_next_assert_candidate_financial_fence_v1(
 p_candidate_id uuid,p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint)
returns void language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare
 v_module_epoch bigint;
 v_control private.bpay_next_worker_control%rowtype;
 v_fence private.bpay_next_candidate_week_commit_fence%rowtype;
 v_job private.bpay_next_job%rowtype;
 v_command private.bpay_next_command%rowtype;
 v_request private.bpay_next_paye_net_request%rowtype;
 v_worker private.bpay_next_run_worker%rowtype;
 v_state private.bpay_next_case_allocation_state%rowtype;
 v_draft private.bpay_next_case_allocation_state%rowtype;
 v_rep private.bpay_next_net_replacement%rowtype;v_delegate private.bpay_next_job%rowtype;
 v_handoff private.bpay_next_net_cancel_handoff%rowtype;
begin
 if p_candidate_id is null then
  raise exception using errcode='22023',message='BPAY_NEXT_CANDIDATE_FINANCIAL_FENCE_INPUT_INVALID';end if;
 select m.owner_epoch into v_module_epoch from private.bpay_next_module_control m
  where m.id=1 and m.active_owner='NEXT' for share nowait;
 if v_module_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
 select c.* into strict v_control from private.bpay_next_worker_control c
  where c.candidate_id=p_candidate_id for update nowait;
 select f.* into v_fence from private.bpay_next_candidate_week_commit_fence f
  where f.candidate_id=p_candidate_id and f.stage in ('STAGING','COMMITTING','CANCEL_REQUESTED','CANCELLING') for update nowait;
 if not found then return;end if;
 -- Candidate serialization also fences the native owner and exact state reads.
 select j.* into v_job from private.bpay_next_job j where j.id=v_fence.original_job_id;
 select c.* into v_command from private.bpay_next_command c where c.id=v_fence.command_id;
 select r.* into v_request from private.bpay_next_paye_net_request r where r.command_id=v_fence.command_id;
 select w.* into v_worker from private.bpay_next_run_worker w where w.id=v_fence.run_worker_id;
 select s.* into v_state from private.bpay_next_case_allocation_state s where s.id=v_fence.allocation_state_id;
 select d.* into v_draft from private.bpay_next_case_allocation_state d where d.id=v_request.case_draft_state_id;
 if v_job.id is null or v_command.id is null or v_request.command_id is null or v_worker.id is null or v_state.id is null
  or v_job.candidate_id is distinct from p_candidate_id or v_job.command_id is distinct from v_fence.command_id
  or v_job.module_epoch is distinct from v_fence.module_epoch
  or v_command.module_epoch is distinct from v_fence.module_epoch
  or v_job.command_sequence is distinct from v_command.agency_sequence
  or v_command.command_kind is distinct from 'PAYE_NET_ENTRY' or v_job.job_kind is distinct from 'PAYE_NET_ENTRY'
  or v_request.candidate_id is distinct from p_candidate_id or v_request.run_worker_id is distinct from v_fence.run_worker_id
  or v_worker.candidate_id is distinct from p_candidate_id
  or v_state.pass_kind is distinct from 'NET' or v_state.command_id is distinct from v_fence.command_id
  or v_state.job_id is distinct from v_fence.original_job_id or v_state.module_epoch is distinct from v_fence.module_epoch
  or v_state.run_worker_id is distinct from v_fence.run_worker_id or v_state.candidate_id is distinct from p_candidate_id
  or v_state.run_id is distinct from v_worker.run_id or v_state.pay_week_start is distinct from v_fence.pay_week_start
  or v_state.projection_no is distinct from v_request.request_no
  or v_state.preparation_revision is distinct from v_request.preparation_revision
  or v_state.selection_revision is distinct from v_request.selection_revision
  or v_worker.captured_preparation_revision is distinct from v_request.preparation_revision
  or v_worker.case_selection_revision is distinct from v_request.selection_revision
  or v_draft.id is null or v_draft.pass_kind is distinct from 'DRAFT'
  or v_draft.run_worker_id is distinct from v_fence.run_worker_id or v_draft.run_id is distinct from v_worker.run_id
  or v_draft.candidate_id is distinct from p_candidate_id
  or v_draft.preparation_revision is distinct from v_request.preparation_revision
  or v_draft.selection_revision is distinct from v_request.selection_revision
  or v_draft.pay_week_start is distinct from v_fence.pay_week_start
  or not exists(select 1 from private.bpay_next_command_member m where m.command_id=v_fence.command_id and m.candidate_id=p_candidate_id) then
  raise exception using errcode='23514',message='BPAY_NEXT_CANDIDATE_FINANCIAL_FENCE_IDENTITY_INVALID';end if;
 select dr37_point_76.* into v_rep from private.bpay_next_net_replacement dr37_point_76 where id=v_fence.replacement_id;
 if v_rep.id is null or (v_rep.id,v_rep.original_job_id,v_rep.target_state_id,v_rep.candidate_id,v_rep.run_worker_id,v_rep.module_epoch,v_rep.pay_week_start)
  is distinct from (v_fence.command_id,v_fence.original_job_id,v_fence.allocation_state_id,v_fence.candidate_id,v_fence.run_worker_id,v_fence.module_epoch,v_fence.pay_week_start)
  or v_worker.net_replacement_pending_id is distinct from v_rep.id then
  raise exception using errcode='23514',message='BPAY_NEXT_CANDIDATE_FINANCIAL_FENCE_REPLACEMENT_INVALID';end if;
 -- A later genuine cancellation owns a new lease only after original NET DONE.
 if v_rep.stage='CANCELLED' and v_fence.stage='CANCELLING' and p_job_id is distinct from v_fence.original_job_id then
  select dr37_point_77.* into v_handoff from private.bpay_next_net_cancel_handoff dr37_point_77 where replacement_id=v_rep.id;
  select dr37_point_78.* into v_delegate from private.bpay_next_job dr37_point_78 where id=p_job_id;
  if v_handoff.replacement_id=v_rep.id and v_handoff.cancel_command_id=v_rep.cancel_command_id
   and v_handoff.original_job_id=v_job.id and v_handoff.target_state_id=v_state.id
   and (v_handoff.prior_state_id,v_handoff.prior_projection_id,v_handoff.target_projection_id)
    is not distinct from (v_rep.prior_state_id,v_rep.prior_projection_id,v_rep.accepted_projection_id)
   and v_handoff.expected_instruction_count=v_handoff.cleaned_instruction_count
   and v_handoff.expected_hold_count=v_handoff.released_hold_count
   and v_handoff.expected_source_total=v_handoff.released_source_total
   and v_state.status='CANCELLED' and v_state.net_stage='CANCELLED' and v_job.status='DONE' and v_job.phase='DONE'
   and v_fence.module_epoch=v_module_epoch and v_delegate.command_id=v_rep.cancel_command_id
   and v_delegate.job_kind='SIMPLE_CANCEL' and v_delegate.candidate_id=p_candidate_id and v_delegate.module_epoch=v_module_epoch
   and v_delegate.command_sequence>v_job.command_sequence and v_delegate.status='LEASED' and v_delegate.phase='RELEASE'
   and p_lease_nonce is not null and v_delegate.lease_nonce=p_lease_nonce and p_owner_epoch is not null
   and v_delegate.owner_epoch=p_owner_epoch and v_control.active_owner_epoch=p_owner_epoch
   and v_delegate.lease_until_utc>pg_catalog.clock_timestamp()
   and exists(select 1 from private.bpay_next_command c where c.id=v_delegate.command_id and c.command_kind='SIMPLE_CANCEL'
    and c.module_epoch=v_module_epoch and c.agency_sequence=v_delegate.command_sequence)
   and exists(select 1 from private.bpay_next_cancel_request q where q.command_id=v_delegate.command_id
    and q.run_worker_id=v_rep.run_worker_id and q.candidate_id=p_candidate_id and q.status in ('REQUESTED','CANCELLING','CANCELLED'))
   and not exists(select 1 from private.bpay_next_job j where j.candidate_id=p_candidate_id
    and j.command_sequence<v_delegate.command_sequence and j.status<>'DONE') then return;end if;
 end if;
 -- Module identity and lease ownership are different clocks. State's carried
 -- lease epoch is refreshed by the original NET owner after fresh admission.
 if v_fence.module_epoch=v_module_epoch and p_job_id=v_fence.original_job_id
  and p_lease_nonce is not null and p_owner_epoch is not null
  and v_job.status='LEASED' and v_job.phase='FINANCE_ALLOCATE'
  and v_job.lease_nonce=p_lease_nonce and v_job.owner_epoch=p_owner_epoch
  and v_control.active_owner_epoch=p_owner_epoch and v_job.lease_until_utc>pg_catalog.clock_timestamp() then
  return;end if;
 raise exception using errcode='55P03',message='BPAY_NEXT_CANDIDATE_FINANCIAL_FENCE_BUSY';
exception when lock_not_available then
 raise exception using errcode='55P03',message='BPAY_NEXT_CANDIDATE_FINANCIAL_FENCE_BUSY';
end
$function$;


-- DR-37 fence-guard
create or replace function private.bpay_next_net_commit_fence_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_r private.bpay_next_net_replacement%rowtype;v_j private.bpay_next_job%rowtype;
begin
 if tg_op='DELETE' then raise exception using errcode='23514',message='BPAY_NEXT_CANDIDATE_FINANCIAL_FENCE_DELETE_FORBIDDEN';end if;
 select dr37_point_79.* into strict v_r from private.bpay_next_net_replacement dr37_point_79 where id=new.replacement_id;
 if (new.candidate_id,new.pay_week_start,new.command_id,new.run_worker_id,new.original_job_id,new.module_epoch,new.allocation_state_id)
  is distinct from (v_r.candidate_id,v_r.pay_week_start,v_r.id,v_r.run_worker_id,v_r.original_job_id,v_r.module_epoch,v_r.target_state_id)
  or new.stage not in ('STAGING','COMMITTING','CANCELLING','COMPLETE','REFUSED','CANCELLED') then
  raise exception using errcode='23514',message='BPAY_NEXT_CANDIDATE_FINANCIAL_FENCE_SCOPE_INVALID';end if;
 if tg_op='UPDATE' and old.stage in ('STAGING','COMMITTING','CANCEL_REQUESTED','CANCELLING') then
  if (new.candidate_id,new.pay_week_start,new.command_id,new.run_worker_id,new.original_job_id,new.module_epoch,new.allocation_state_id,new.replacement_id)
   is distinct from (old.candidate_id,old.pay_week_start,old.command_id,old.run_worker_id,old.original_job_id,old.module_epoch,old.allocation_state_id,old.replacement_id)
   or not(case old.stage when 'STAGING' then new.stage in ('STAGING','COMMITTING','CANCELLING','REFUSED')
    when 'COMMITTING' then new.stage in ('COMMITTING','CANCELLING','COMPLETE')
    when 'CANCEL_REQUESTED' then new.stage in ('CANCEL_REQUESTED','CANCELLING')
    when 'CANCELLING' then new.stage in ('CANCELLING','CANCELLED') else false end) then
   raise exception using errcode='23514',message='BPAY_NEXT_CANDIDATE_FINANCIAL_FENCE_TRANSITION_INVALID';end if;
 end if;
 if new.stage='CANCELLED' then
  select j.* into strict v_j from private.bpay_next_job j where j.command_id=v_r.cancel_command_id and j.candidate_id=v_r.candidate_id;
  if v_r.stage<>'CANCELLED' or tg_op<>'UPDATE' or old.stage<>'CANCELLING' then
   raise exception using errcode='23514',message='BPAY_NEXT_CANDIDATE_FINANCIAL_FENCE_CANCEL_INVALID';end if;
  perform private.bpay_next_assert_candidate_financial_fence_v1(v_r.candidate_id,v_j.id,v_j.lease_nonce,v_j.owner_epoch);
 else
  perform private.bpay_next_net_replacement_job_assert_v1(v_r.id);
  if (new.stage='STAGING' and v_r.stage<>'STAGING')
   or (new.stage='COMMITTING' and v_r.stage<>'COMMITTING')
   or (new.stage='COMPLETE' and v_r.stage<>'COMPLETE')
   or (new.stage='REFUSED' and v_r.stage<>'REFUSED')
   or (new.stage='CANCELLING' and v_r.stage not in ('PRECHECK','STAGING','COMMITTING','CANCEL_REQUESTED','CANCELLING')) then
   raise exception using errcode='23514',message='BPAY_NEXT_CANDIDATE_FINANCIAL_FENCE_STAGE_INVALID';end if;
 end if;
 return new;
end $function$;
drop trigger if exists bpay_next_net_commit_fence_guard on private.bpay_next_candidate_week_commit_fence;
create trigger bpay_next_net_commit_fence_guard before insert or update or delete on private.bpay_next_candidate_week_commit_fence
 for each row execute function private.bpay_next_net_commit_fence_guard_v1();
drop trigger if exists bpay_next_net_replacement_truncate_guard on private.bpay_next_candidate_week_commit_fence;
create trigger bpay_next_net_replacement_truncate_guard before truncate on private.bpay_next_candidate_week_commit_fence
 for each statement execute function private.bpay_next_net_replacement_truncate_guard_v1();

-- A terminal header and its immutable receipt must commit together.
create or replace function private.bpay_next_net_terminal_receipt_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_r private.bpay_next_net_replacement%rowtype;v_j private.bpay_next_job%rowtype;
 v_w private.bpay_next_run_worker%rowtype;
begin
 select dr37_point_80.* into strict v_r from private.bpay_next_net_replacement dr37_point_80 where id=new.id;
 if v_r.stage not in ('COMPLETE','REFUSED','CANCELLED') then return null;end if;
 select dr37_point_81.* into strict v_j from private.bpay_next_job dr37_point_81 where id=v_r.original_job_id;
 select dr37_point_82.* into strict v_w from private.bpay_next_run_worker dr37_point_82 where id=v_r.run_worker_id;
 if v_j.status<>'DONE' or not exists(select 1 from private.bpay_next_command c where c.id=v_r.id and c.status='COMPLETE')
  or (v_r.stage='COMPLETE' and (v_w.net_replacement_pending_id is not null or v_w.net_projection_revision<>v_r.request_no
    or v_w.net_request_revision<>v_r.request_no or v_j.phase<>'PROJECTED'))
  or (v_r.stage='REFUSED' and (v_w.net_replacement_pending_id is not null or v_w.net_request_revision<>v_w.net_projection_revision
    or v_w.net_projection_revision<>v_r.prior_projection_no or v_j.phase<>'DONE'
    or not exists(select 1 from private.bpay_next_paye_net_request_refusal f where f.command_id=v_r.id
     and f.run_worker_id=v_r.run_worker_id and f.candidate_id=v_r.candidate_id and f.request_no=v_r.request_no and f.refusal_code=v_r.refusal_code)))
  or (v_r.stage='CANCELLED' and (v_w.net_replacement_pending_id is distinct from v_r.id
    or v_w.net_request_revision<>v_w.net_projection_revision or v_w.net_projection_revision<>v_r.prior_projection_no or v_j.phase<>'DONE'
    or not exists(select 1 from private.bpay_next_net_cancel_handoff h where h.replacement_id=v_r.id
     and h.cancel_command_id=v_r.cancel_command_id and h.original_job_id=v_r.original_job_id))) then
  raise exception using errcode='23514',message='BPAY_NEXT_NET_TERMINAL_RECEIPT_MISSING';end if;
 return null;
end $function$;
drop trigger if exists bpay_next_net_terminal_receipt_guard on private.bpay_next_net_replacement;
create constraint trigger bpay_next_net_terminal_receipt_guard after insert or update on private.bpay_next_net_replacement
 deferrable initially deferred for each row execute function private.bpay_next_net_terminal_receipt_guard_v1();


-- DR-37 money-guards
create or replace function private.bpay_next_case_hold_guard_v1()
returns trigger language plpgsql
set search_path=pg_catalog,private
as $function$
declare v_result private.bpay_next_case_allocation_result%rowtype;
 v_f private.bpay_next_candidate_week_commit_fence%rowtype;v_r private.bpay_next_net_replacement%rowtype;
 v_j private.bpay_next_job%rowtype;v_a private.bpay_next_net_staged_result%rowtype;
begin
  if tg_op='DELETE' then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_HOLD_DELETE_FORBIDDEN';
  end if;
  select dr37_point_83.* into v_f from private.bpay_next_candidate_week_commit_fence dr37_point_83
    where candidate_id=new.candidate_id and stage in ('STAGING','COMMITTING','CANCEL_REQUESTED','CANCELLING');
  if v_f.candidate_id is not null then
    select dr37_point_84.* into strict v_r from private.bpay_next_net_replacement dr37_point_84 where id=v_f.replacement_id;
    select dr37_point_85.* into strict v_j from private.bpay_next_job dr37_point_85 where id=v_r.original_job_id;
    perform private.bpay_next_assert_candidate_financial_fence_v1(new.candidate_id,v_j.id,v_j.lease_nonce,v_j.owner_epoch);
    perform private.bpay_next_net_replacement_job_assert_v1(v_r.id);
    select dr37_point_86.* into v_a from private.bpay_next_net_staged_result dr37_point_86 where replacement_id=v_r.id and instruction_id=new.instruction_id;
    if not (
      (v_r.stage='COMMITTING' and v_f.stage='COMMITTING' and v_a.id is not null and
       ((tg_op='INSERT' and new.status='ACTIVE' and new.allocation_result_id=v_a.id
          and new.allocation_pass_kind='NET')
        or (tg_op='UPDATE' and new.status='RELEASED' and new.id=v_a.prior_hold_id)))
      or (v_r.stage='CANCELLING' and v_f.stage='CANCELLING' and v_r.cleanup_step='RELEASE'
       and tg_op='UPDATE' and new.status='RELEASED' and exists(select 1 from private.bpay_next_net_cancel_hold_member m
        where m.replacement_id=v_r.id and m.case_hold_id=new.id and m.instruction_id=new.instruction_id))) is not true then
      raise exception using errcode='55P03',message='BPAY_NEXT_CANDIDATE_FINANCIAL_FENCE_BUSY';end if;
  end if;
  if tg_op='INSERT' then
    if new.status<>'ACTIVE' or new.instruction_id is null or new.allocation_result_id is null then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_HOLD_TYPED_ACTIVE_REQUIRED';
    end if;
    select dr37_point_87.* into strict v_result from private.bpay_next_case_allocation_result dr37_point_87
      where id=new.allocation_result_id for share nowait;
    if v_result.pass_kind not in ('DRAFT','NET')
       or (new.source_reserved_ex_vat,new.target_amount_ex_vat,new.target_amount_vat,new.target_amount_inc_vat)
         is distinct from (v_result.allocated_source_ex_vat,v_result.allocated_target_ex_vat,
           v_result.allocated_target_vat,v_result.allocated_target_inc_vat) then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_HOLD_RESULT_AMOUNT_MISMATCH';
    end if;
    return new;
  end if;
  -- Resizing a NET capacity uses a new exact result/hold after releasing its
  -- own prior hold. Never rewrite an old captured allocation to hide money.
  if old.status<>'ACTIVE' or new.status not in ('RELEASED','REALISED')
     or old.finished_at_utc is not null or new.finished_at_utc is null
     or not pg_catalog.isfinite(new.finished_at_utc)
     or (pg_catalog.to_jsonb(new)-'status'-'projection_id'-'finished_at_utc')
       is distinct from (pg_catalog.to_jsonb(old)-'status'-'projection_id'-'finished_at_utc')
     or (old.projection_id is not null and new.projection_id is distinct from old.projection_id)
     or (new.status='RELEASED' and new.projection_id is distinct from old.projection_id) then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_HOLD_TRANSITION_INVALID';
  end if;
  return new;
end;
$function$;

create or replace function private.bpay_next_case_capacity_use_guard_v1()
returns trigger language plpgsql
set search_path=pg_catalog,private
as $function$
declare v_hold private.bpay_next_case_hold%rowtype;
 v_f private.bpay_next_candidate_week_commit_fence%rowtype;v_r private.bpay_next_net_replacement%rowtype;
 v_j private.bpay_next_job%rowtype;v_a private.bpay_next_net_staged_result%rowtype;
begin
  if tg_op='DELETE' then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_CAPACITY_DELETE_FORBIDDEN';
  end if;
  select dr37_point_88.* into v_f from private.bpay_next_candidate_week_commit_fence dr37_point_88
    where candidate_id=new.candidate_id and stage in ('STAGING','COMMITTING','CANCEL_REQUESTED','CANCELLING');
  if v_f.candidate_id is not null then
    select dr37_point_89.* into strict v_r from private.bpay_next_net_replacement dr37_point_89 where id=v_f.replacement_id;
    select dr37_point_90.* into strict v_j from private.bpay_next_job dr37_point_90 where id=v_r.original_job_id;
    perform private.bpay_next_assert_candidate_financial_fence_v1(new.candidate_id,v_j.id,v_j.lease_nonce,v_j.owner_epoch);
    perform private.bpay_next_net_replacement_job_assert_v1(v_r.id);
    select dr37_point_91.* into strict v_hold from private.bpay_next_case_hold dr37_point_91 where id=new.case_hold_id;
    select dr37_point_92.* into v_a from private.bpay_next_net_staged_result dr37_point_92 where replacement_id=v_r.id and instruction_id=v_hold.instruction_id;
    if not (
      (v_r.stage='COMMITTING' and v_f.stage='COMMITTING' and v_a.id is not null and
       ((tg_op='INSERT' and new.status='ACTIVE' and v_hold.allocation_result_id=v_a.id
          and v_hold.allocation_pass_kind='NET')
        or (tg_op='UPDATE' and new.status='RELEASED' and v_hold.id=v_a.prior_hold_id)))
      or (v_r.stage='CANCELLING' and v_f.stage='CANCELLING' and v_r.cleanup_step='RELEASE'
       and tg_op='UPDATE' and new.status='RELEASED' and exists(select 1 from private.bpay_next_net_cancel_hold_member m
        where m.replacement_id=v_r.id and m.case_hold_id=v_hold.id and m.instruction_id=v_hold.instruction_id))) is not true then
      raise exception using errcode='55P03',message='BPAY_NEXT_CANDIDATE_FINANCIAL_FENCE_BUSY';end if;
  end if;
  select dr37_point_93.* into strict v_hold from private.bpay_next_case_hold dr37_point_93
    where id=new.case_hold_id for share nowait;
  if tg_op='INSERT' then
    if new.status<>'ACTIVE' or v_hold.status<>'ACTIVE'
       or new.source_amount_ex_vat is distinct from v_hold.source_reserved_ex_vat then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_CAPACITY_INITIAL_STATE_INVALID';
    end if;
  elsif old.status<>'ACTIVE' or new.status not in ('RELEASED','REALISED')
     or (pg_catalog.to_jsonb(new)-'status'-'realisation_event_id')
       is distinct from (pg_catalog.to_jsonb(old)-'status'-'realisation_event_id')
     or v_hold.status<>new.status then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_CAPACITY_TRANSITION_INVALID';
  end if;
  if new.status='REALISED' and not exists(select 1 from private.bpay_next_case_event e
      where e.id=new.realisation_event_id and e.instruction_id=v_hold.instruction_id
        and e.allocation_result_id=v_hold.allocation_result_id
        and e.event_kind in ('FUNDED','RECOVERED','PAID')
        and (e.source_amount_ex_vat,e.target_amount_ex_vat,e.target_amount_vat,e.target_amount_inc_vat)=
          (v_hold.source_reserved_ex_vat,v_hold.target_amount_ex_vat,v_hold.target_amount_vat,v_hold.target_amount_inc_vat)) then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_CAPACITY_EVENT_MISMATCH';
  end if;
  return new;
end;
$function$;

create or replace function private.bpay_next_case_result_insert_guard_v1()
returns trigger language plpgsql
set search_path=pg_catalog,private
as $function$
declare v_status text;v_instruction private.bpay_next_run_case_instruction%rowtype;
 v_f private.bpay_next_candidate_week_commit_fence%rowtype;v_r private.bpay_next_net_replacement%rowtype;
 v_j private.bpay_next_job%rowtype;v_a private.bpay_next_net_staged_result%rowtype;
begin
  select dr37_point_94.* into v_f from private.bpay_next_candidate_week_commit_fence dr37_point_94
    where candidate_id=new.candidate_id and stage in ('STAGING','COMMITTING','CANCEL_REQUESTED','CANCELLING');
  if v_f.candidate_id is not null then
    select dr37_point_95.* into strict v_r from private.bpay_next_net_replacement dr37_point_95 where id=v_f.replacement_id;
    select dr37_point_96.* into strict v_j from private.bpay_next_job dr37_point_96 where id=v_r.original_job_id;
    perform private.bpay_next_assert_candidate_financial_fence_v1(new.candidate_id,v_j.id,v_j.lease_nonce,v_j.owner_epoch);
    perform private.bpay_next_net_replacement_job_assert_v1(v_r.id);
    select dr37_point_97.* into v_a from private.bpay_next_net_staged_result dr37_point_97 where replacement_id=v_r.id and instruction_id=new.instruction_id;
    if v_r.stage<>'COMMITTING' or v_f.stage<>'COMMITTING' or new.pass_kind<>'NET' or new.state_id<>v_r.target_state_id
      or v_a.id is null or (new.id,new.state_id,new.instruction_id,new.run_worker_id,new.candidate_id,new.preparation_revision,new.selection_revision,
       new.result_no,new.pass_kind,new.allocation_channel,new.hold_purpose,new.nominal_target_ex_vat,new.usable_capacity_target_ex_vat,
       new.allocated_source_ex_vat,new.allocated_target_ex_vat,new.allocated_target_vat,new.allocated_target_inc_vat,new.shortfall_target_ex_vat,
       new.cap_reason,new.affordability_reason,new.capacity_reason,new.channel_headroom_before,new.channel_headroom_after,
       new.worker_take_home_before,new.worker_take_home_after)
      is distinct from (v_a.id,v_a.state_id,v_a.instruction_id,v_a.run_worker_id,v_a.candidate_id,v_a.preparation_revision,v_a.selection_revision,
       v_a.result_no,v_a.pass_kind,v_a.allocation_channel,v_a.hold_purpose,v_a.nominal_target_ex_vat,v_a.usable_capacity_target_ex_vat,
       v_a.allocated_source_ex_vat,v_a.allocated_target_ex_vat,v_a.allocated_target_vat,v_a.allocated_target_inc_vat,v_a.shortfall_target_ex_vat,
       v_a.cap_reason,v_a.affordability_reason,v_a.capacity_reason,v_a.channel_headroom_before,v_a.channel_headroom_after,
       v_a.worker_take_home_before,v_a.worker_take_home_after) then
      raise exception using errcode='55P03',message='BPAY_NEXT_CANDIDATE_FINANCIAL_FENCE_BUSY';end if;
  end if;
  select s.status into strict v_status from private.bpay_next_case_allocation_state s
    where s.id=new.state_id for share nowait;
  if v_status<>'BUILDING' then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_RESULT_LATE_INSERT_FORBIDDEN';
  end if;
  select dr37_point_98.* into strict v_instruction from private.bpay_next_run_case_instruction dr37_point_98
    where id=new.instruction_id for share nowait;
  if new.nominal_target_ex_vat is distinct from v_instruction.nominal_target_ex_vat
     or new.allocated_source_ex_vat>v_instruction.nominal_source_ex_vat
     or new.allocated_target_ex_vat>v_instruction.nominal_target_ex_vat
     or new.allocated_target_vat>v_instruction.nominal_target_vat
     or new.allocated_target_inc_vat>v_instruction.nominal_target_inc_vat then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_RESULT_FROZEN_AMOUNT_EXCEEDED';
  end if;
  return new;
end;
$function$;



-- DR-37 destination
create or replace function private.bpay_next_accumulate_net_destination_v1(p_state_id uuid,p_instruction_id uuid,p_result_id uuid)
returns void language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_o private.bpay_next_stored_credit_origin%rowtype;v_i private.bpay_next_run_case_instruction%rowtype;
  v_s private.bpay_next_case_allocation_state%rowtype;v_r private.bpay_next_case_allocation_result%rowtype;
  v_d private.bpay_next_net_destination_amount%rowtype;v_new boolean:=false;
begin
  select o.* into v_o from private.bpay_next_instruction_destination d
    join private.bpay_next_stored_credit_origin o on o.command_id=d.origin_command_id where d.instruction_id=p_instruction_id;
  if not found then return;end if;
  select dr37_point_99.* into strict v_s from private.bpay_next_case_allocation_state dr37_point_99 where id=p_state_id;
  select dr37_point_100.* into strict v_i from private.bpay_next_run_case_instruction dr37_point_100 where id=p_instruction_id;
  select dr37_point_101.* into strict v_r from private.bpay_next_case_allocation_result dr37_point_101 where id=p_result_id;
  if v_s.net_stage='COMMITTING' then
    perform private.bpay_next_net_replacement_job_assert_v1(v_s.command_id);
    if not exists(select 1 from private.bpay_next_net_replacement r
      join private.bpay_next_candidate_week_commit_fence f on f.replacement_id=r.id and f.candidate_id=r.candidate_id
      where r.id=v_s.command_id and r.target_state_id=v_s.id and r.stage='COMMITTING' and f.stage='COMMITTING') then
      raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_NET_COMMIT_OWNER_INVALID';end if;
  end if;
  if v_s.pass_kind<>'NET' or v_s.status<>'BUILDING' or v_s.net_stage not in ('ALLOCATE','COMMITTING')
     or v_i.run_worker_id<>v_s.run_worker_id or v_i.candidate_id<>v_o.candidate_id
     or v_i.preparation_revision<>v_s.preparation_revision or v_i.selection_revision<>v_s.selection_revision
     or v_i.case_component_id<>v_o.command_id or v_i.payroll_stage<>'NET_ADD' or v_i.tax_treatment<>'NON_TAXABLE'
     or v_r.instruction_id<>v_i.id or v_r.pass_kind<>'DRAFT'
     or v_r.allocated_target_vat<>0 or v_r.allocated_source_ex_vat<>v_r.allocated_target_inc_vat
     or not exists(select 1 from private.bpay_next_case_allocation_state d where d.id=v_r.state_id
       and d.run_worker_id=v_s.run_worker_id and d.preparation_revision=v_s.preparation_revision
       and d.selection_revision=v_s.selection_revision and d.pass_kind='DRAFT' and d.status='COMPLETE') then
    raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_NET_BASIS_INVALID';end if;
  if v_r.allocated_target_inc_vat=0 then return;end if;
  insert into private.bpay_next_net_destination_state(state_id,run_worker_id) values(v_s.id,v_s.run_worker_id)
    on conflict(state_id) do nothing;
  perform 1 from private.bpay_next_net_destination_state where state_id=v_s.id and projection_id is null for update;
  if not found then raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_NET_ALREADY_SEALED';end if;
  select dr37_point_102.* into v_d from private.bpay_next_net_destination_amount dr37_point_102 where state_id=v_s.id and bank_details_hash=v_o.bank_details_hash for update;
  if not found then
    insert into private.bpay_next_net_destination_amount(state_id,bank_details_hash,first_origin_command_id,credit_count,amount)
      values(v_s.id,v_o.bank_details_hash,v_o.command_id,1,v_r.allocated_target_inc_vat) returning * into v_d;
    v_new:=true;
  else
    update private.bpay_next_net_destination_amount set credit_count=credit_count+1,amount=amount+v_r.allocated_target_inc_vat
      where destination_id=v_d.destination_id;
  end if;
  insert into private.bpay_next_net_destination_credit(state_id,instruction_id,destination_id,allocation_result_id,amount)
    values(v_s.id,v_i.id,v_d.destination_id,v_r.id,v_r.allocated_target_inc_vat);
  update private.bpay_next_net_destination_state set external_credit_count=external_credit_count+1,
    external_leg_count=external_leg_count+case when v_new then 1 else 0 end,external_amount=external_amount+v_r.allocated_target_inc_vat
    where state_id=v_s.id;
end $function$;

create or replace function private.bpay_next_destination_state_guard_v1()
returns trigger language plpgsql set search_path=pg_catalog,private
as $function$
declare v_s private.bpay_next_case_allocation_state%rowtype;
  v_n private.bpay_next_net_destination_state%rowtype;v_o private.bpay_next_stored_credit_origin%rowtype;
begin
  if tg_op='DELETE' then raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_STATE_DELETE_FORBIDDEN';end if;
  select dr37_point_103.* into strict v_s from private.bpay_next_case_allocation_state dr37_point_103 where id=new.state_id;
  if v_s.net_stage='COMMITTING' then
    perform private.bpay_next_net_replacement_job_assert_v1(v_s.command_id);
    if not exists(select 1 from private.bpay_next_net_replacement r
      join private.bpay_next_candidate_week_commit_fence f on f.replacement_id=r.id and f.candidate_id=r.candidate_id
      where r.id=v_s.command_id and r.target_state_id=v_s.id and r.stage='COMMITTING' and f.stage='COMMITTING') then
      raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_NET_COMMIT_OWNER_INVALID';end if;
  end if;
  if v_s.pass_kind<>'NET' or v_s.status<>'BUILDING' or v_s.net_stage not in ('ALLOCATE','COMMITTING')
     or not exists(select 1 from private.bpay_next_job j join private.bpay_next_worker_control c on c.candidate_id=j.candidate_id
       where j.id=v_s.job_id and j.candidate_id=v_s.candidate_id and j.job_kind='PAYE_NET_ENTRY'
       and j.status='LEASED' and j.owner_epoch=c.active_owner_epoch and j.lease_until_utc>pg_catalog.clock_timestamp()) then
    raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_STATE_OWNER_INVALID';end if;
  if tg_table_name='bpay_next_net_destination_state' then
    if new.run_worker_id<>v_s.run_worker_id then raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_STATE_SCOPE_INVALID';end if;
    if tg_op='INSERT' then
      if new.projection_id is not null or new.external_credit_count<>0 or new.external_leg_count<>0 or new.external_amount<>0 then
        raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_STATE_INITIAL_INVALID';end if;
    elsif old.projection_id is not null or new.state_id<>old.state_id or new.run_worker_id<>old.run_worker_id
      or new.external_credit_count<old.external_credit_count or new.external_leg_count<old.external_leg_count
      or new.external_amount<old.external_amount then
      raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_STATE_TRANSITION_INVALID';end if;
  else
    select dr37_point_104.* into strict v_n from private.bpay_next_net_destination_state dr37_point_104 where state_id=new.state_id;
    select dr37_point_105.* into strict v_o from private.bpay_next_stored_credit_origin dr37_point_105 where command_id=new.first_origin_command_id;
    if v_n.projection_id is not null or v_o.candidate_id<>v_s.candidate_id or new.bank_details_hash<>v_o.bank_details_hash then
      raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_AMOUNT_SCOPE_INVALID';end if;
    if tg_op='UPDATE' and ((new.destination_id,new.state_id,new.bank_details_hash,new.first_origin_command_id)
      is distinct from (old.destination_id,old.state_id,old.bank_details_hash,old.first_origin_command_id)
      or new.credit_count<>old.credit_count+1 or new.amount<=old.amount) then
      raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_AMOUNT_TRANSITION_INVALID';end if;
  end if;
  return new;
end $function$;



-- DR-37 audit
create or replace function private.bpay_next_record_case_hold_transition_v1(p_job_id uuid,p_hold_id uuid,p_transition text)
returns void language plpgsql security invoker set search_path=pg_catalog,private,public
as $function$
declare
 v_hold private.bpay_next_case_hold%rowtype;v_job private.bpay_next_job%rowtype;
 v_command uuid;v_kind text;v_actor uuid;v_actor_kind text:='UNKNOWN';v_reason text;v_time timestamptz;
 v_ordinal smallint;v_release record;v_prior private.bpay_next_case_hold_transition%rowtype;
begin
 select h.* into strict v_hold from private.bpay_next_case_hold h where h.id=p_hold_id;
 select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id;
 if v_job.candidate_id is distinct from v_hold.candidate_id or v_hold.status is distinct from p_transition
   or v_hold.instruction_id is null or v_hold.allocation_result_id is null
   or p_transition not in ('ACTIVE','RELEASED','REALISED') then
  raise exception using errcode='23514',message='BPAY_NEXT_CASE_HOLD_AUDIT_SCOPE_INVALID';end if;
 v_command:=v_job.command_id;v_kind:=v_job.job_kind;
 -- A system RELEASE can reuse the original PREPARE job. Its exact running
 -- action must precede PREPARE attribution; an allocation state's old job is not a cause.
 select r.command_id,r.action,r.initiator_kind,r.actor_user_id,r.original_issue_code into v_release
  from private.bpay_next_worker_reprepare_request r
  join private.bpay_next_run_worker w on w.id=r.run_worker_id and w.preparation_revision=private.bpay_next_retry_execution_v1(r.job_id,w.id,r.target_preparation_revision)
  where r.job_id=p_job_id and r.run_worker_id=v_hold.run_worker_id and r.candidate_id=v_hold.candidate_id
   and r.status='RUNNING' and ((p_transition='RELEASED' and r.action='RELEASE') or(p_transition='ACTIVE' and r.action='REPREPARE'));
 if found then
  v_command:=v_release.command_id;v_kind:='WORKER_'||v_release.action;v_actor:=v_release.actor_user_id;
  v_actor_kind:=case v_release.initiator_kind when 'SYSTEM' then 'SYSTEM' when 'OFFICE' then 'HUMAN' else 'UNKNOWN' end;
  v_reason:=v_release.original_issue_code;
 elsif v_job.job_kind='PREPARATION_EXPIRY' then
  select r.actor_user_id,case r.initiator_kind when 'SYSTEM' then 'SYSTEM' else 'HUMAN' end into v_actor,v_actor_kind
   from private.bpay_next_preparation_expiry_worker w join private.bpay_next_preparation_expiry_request r on r.command_id=w.command_id
   where w.job_id=p_job_id and w.run_worker_id=v_hold.run_worker_id and w.candidate_id=v_hold.candidate_id;
 elsif v_job.job_kind='PREPARATION_DISCARD' then
  select r.actor_user_id,'HUMAN',r.reason into v_actor,v_actor_kind,v_reason
   from private.bpay_next_preparation_discard_worker w join private.bpay_next_preparation_discard_request r on r.command_id=w.command_id
   where w.job_id=p_job_id and w.run_worker_id=v_hold.run_worker_id and w.candidate_id=v_hold.candidate_id;
 elsif v_job.job_kind='PAYE_NET_ENTRY' and p_transition='RELEASED' and exists(
  select 1 from private.bpay_next_net_replacement r where r.id=v_job.command_id and r.original_job_id=v_job.id
   and r.stage='CANCELLING' and r.run_worker_id=v_hold.run_worker_id and r.cancel_command_id is not null) then
  select r.cancel_command_id,'SIMPLE_CANCEL',p.actor_user_id,'HUMAN' into v_command,v_kind,v_actor,v_actor_kind
   from private.bpay_next_net_replacement r left join private.bpay_next_reauth_proof p
    on p.consumed_command_id=r.cancel_command_id and p.action_kind='CANCEL_REQUEST'
   where r.id=v_job.command_id and r.run_worker_id=v_hold.run_worker_id;
 elsif v_job.job_kind='PAYE_NET_ENTRY' then
  select r.actor_user_id,'HUMAN' into v_actor,v_actor_kind from private.bpay_next_paye_net_request r
   where r.command_id=v_job.command_id and r.run_worker_id=v_hold.run_worker_id and r.candidate_id=v_hold.candidate_id;
 elsif v_job.job_kind='SIMPLE_CANCEL' then
  select p.actor_user_id,'HUMAN' into v_actor,v_actor_kind from private.bpay_next_reauth_proof p
   where p.consumed_command_id=v_job.command_id and p.action_kind='CANCEL_REQUEST';
 elsif v_job.job_kind in ('CSV_SETTLEMENT','INTERNAL_SETTLEMENT') then
  select coalesce(i.actor_user_id,r.actor_user_id),'HUMAN' into v_actor,v_actor_kind
   from private.bpay_next_outcome_request r left join private.bpay_next_internal_receipt i
    on i.id=r.internal_receipt_id and i.command_id=r.command_id and i.transfer_id=r.transfer_id
     and i.run_worker_id=r.run_worker_id and i.candidate_id=r.candidate_id
   where r.command_id=v_job.command_id and r.run_worker_id=v_hold.run_worker_id and r.candidate_id=v_hold.candidate_id;
 elsif v_job.job_kind='PREPARE' and p_transition='ACTIVE' then
  select r.actor_user_id,'HUMAN' into v_actor,v_actor_kind from private.bpay_next_run_worker w
   join private.bpay_next_run_command c on c.run_id=w.run_id and c.command_id=v_job.command_id
   join private.bpay_next_pay_run r on r.id=c.run_id
   where w.id=v_hold.run_worker_id and w.candidate_id=v_hold.candidate_id;
 end if;
 if v_actor_kind is null or(v_actor_kind='HUMAN' and v_actor is null) then v_actor_kind:='UNKNOWN';v_actor:=null;end if;
 v_time:=case when p_transition='ACTIVE' then v_hold.created_at_utc else v_hold.finished_at_utc end;
 v_ordinal:=case when p_transition='ACTIVE' then 1 else 2 end;
 if v_time is null or not isfinite(v_time) then raise exception using errcode='23514',message='BPAY_NEXT_CASE_HOLD_AUDIT_TIME_INVALID';end if;
 select t.* into v_prior from private.bpay_next_case_hold_transition t where t.hold_id=p_hold_id and t.event_ordinal=v_ordinal;
 if found then
  if (v_prior.transition,v_prior.cause_job_id,v_prior.command_id,v_prior.command_kind,v_prior.actor_kind,v_prior.actor_user_id,v_prior.reason,v_prior.occurred_at_utc)
   is distinct from(p_transition,p_job_id,v_command,v_kind,v_actor_kind,v_actor,v_reason,v_time) then
   raise exception using errcode='23514',message='BPAY_NEXT_CASE_HOLD_AUDIT_REPLAY_MISMATCH';end if;
  return;
 end if;
 insert into private.bpay_next_case_hold_transition(hold_id,event_ordinal,transition,case_id,candidate_id,run_worker_id,
  case_component_id,instruction_id,allocation_result_id,cause_job_id,command_id,command_kind,actor_kind,actor_user_id,reason,
  source_amount_ex_vat,target_amount_ex_vat,target_amount_vat,target_amount_inc_vat,occurred_at_utc)
 values(v_hold.id,v_ordinal,p_transition,v_hold.case_id,v_hold.candidate_id,v_hold.run_worker_id,v_hold.case_component_id,
  v_hold.instruction_id,v_hold.allocation_result_id,p_job_id,v_command,v_kind,v_actor_kind,v_actor,v_reason,
  v_hold.source_reserved_ex_vat,v_hold.target_amount_ex_vat,v_hold.target_amount_vat,v_hold.target_amount_inc_vat,v_time);
end
$function$;


-- DR-37 cancel-joins
create or replace function private.bpay_next_claim_simple_cancel_job_v1(
  p_job_id uuid,p_lease_seconds integer default 120
) returns table(lease_nonce uuid,owner_epoch bigint)
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_epoch bigint;
  v_candidate_id uuid;
  v_job private.bpay_next_job%rowtype;
  v_owner_epoch bigint;
  v_nonce uuid;
begin
  if p_job_id is null or p_lease_seconds is null
     or p_lease_seconds not between 1 and 120 then
    raise exception using errcode='22023',message='BPAY_NEXT_CANCEL_LEASE_INPUT_INVALID';
  end if;
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select candidate_id into strict v_candidate_id from private.bpay_next_job
    where id=p_job_id;
  insert into private.bpay_next_worker_control(candidate_id)
    values(v_candidate_id) on conflict(candidate_id) do nothing;
  perform 1 from private.bpay_next_worker_control
    where candidate_id=v_candidate_id for update;
  select dr37_point_106.* into strict v_job from private.bpay_next_job dr37_point_106
    where id=p_job_id for update;
  if v_job.module_epoch<>v_epoch or v_job.job_kind<>'SIMPLE_CANCEL'
     or v_job.phase not in ('NEW','RELEASE')
     or v_job.status not in ('READY','LEASED')
     or v_job.available_at_utc>pg_catalog.clock_timestamp()
     or (v_job.status='LEASED'
         and v_job.lease_until_utc>pg_catalog.clock_timestamp())
     or not exists(select 1 from private.bpay_next_cancel_request r
                   where r.command_id=v_job.command_id
                     and r.candidate_id=v_candidate_id
                     and r.status in ('REQUESTED','CANCELLING'))
     or exists(select 1 from private.bpay_next_job earlier
               where earlier.candidate_id=v_candidate_id
                 and earlier.command_sequence<v_job.command_sequence
                 and earlier.status<>'DONE') then
    raise exception using errcode='55000',message='BPAY_NEXT_CANCEL_JOB_NOT_CLAIMABLE';
  end if;
  update private.bpay_next_worker_control
    set active_owner_epoch=active_owner_epoch+1,
        updated_at_utc=pg_catalog.transaction_timestamp()
    where candidate_id=v_candidate_id
    returning active_owner_epoch into v_owner_epoch;
  v_nonce:=pg_catalog.gen_random_uuid();
  update private.bpay_next_job
    set status='LEASED',owner_epoch=v_owner_epoch,lease_nonce=v_nonce,
        lease_until_utc=pg_catalog.clock_timestamp()+
          pg_catalog.make_interval(secs=>p_lease_seconds),
        attempt_count=attempt_count+1
    where id=p_job_id;
  if exists(select 1 from private.bpay_next_net_cancel_handoff h where h.cancel_command_id=v_job.command_id
    and h.candidate_id=v_candidate_id) then
    -- This is the later command's own fresh lease, acquired after all predecessors DONE.
    update private.bpay_next_job set phase='RELEASE' where id=p_job_id;
    perform private.bpay_next_assert_candidate_financial_fence_v1(v_candidate_id,p_job_id,v_nonce,v_owner_epoch);
  end if;
  lease_nonce:=v_nonce; owner_epoch:=v_owner_epoch;
  return next;
end
$function$;

create or replace function private.bpay_next_cancel_case_worker_page_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,p_expected_cursor bigint,p_limit integer)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare
  v_epoch bigint;v_run_id uuid;v_candidate uuid;v_checkpoint bigint;v_seen integer:=0;v_holds integer:=0;v_block text;v_more boolean;
  v_run private.bpay_next_pay_run%rowtype;v_control private.bpay_next_worker_control%rowtype;v_job private.bpay_next_job%rowtype;
  v_request private.bpay_next_cancel_request%rowtype;v_worker private.bpay_next_run_worker%rowtype;v_binding private.bpay_next_case_cancel_binding%rowtype;
  v_projection private.bpay_next_net_projection%rowtype;v_net private.bpay_next_case_allocation_state%rowtype;
  v_transfer private.bpay_next_transfer%rowtype;v_line private.bpay_next_run_line%rowtype;v_work_hold private.bpay_next_hold%rowtype;
  v_hold private.bpay_next_case_hold%rowtype;v_use private.bpay_next_case_capacity_use%rowtype;
  v_instruction private.bpay_next_run_case_instruction%rowtype;v_result private.bpay_next_case_allocation_result%rowtype;
  v_case private.bpay_next_finance_case%rowtype;v_component private.bpay_next_case_component%rowtype;
  v_period private.bpay_next_case_period%rowtype;v_amount numeric;v_now timestamptz:=pg_catalog.transaction_timestamp();
  v_group private.bpay_next_destination_group%rowtype;v_group_page jsonb;v_group_started boolean:=false;
  v_handoff private.bpay_next_net_cancel_handoff%rowtype;v_member private.bpay_next_net_cancel_hold_member%rowtype;
  v_audit private.bpay_next_case_hold_transition%rowtype;v_bytes integer:=0;v_size integer;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null or p_owner_epoch<1 or p_limit is null or p_limit not between 1 and 100
     or (p_expected_cursor is not null and p_expected_cursor<1) then raise exception using errcode='22023',message='BPAY_NEXT_CASE_CANCEL_PAGE_INPUT_INVALID';end if;
  select owner_epoch into v_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select w.run_id,j.candidate_id into strict v_run_id,v_candidate from private.bpay_next_job j
    join private.bpay_next_cancel_request r on r.command_id=j.command_id join private.bpay_next_run_worker w on w.id=r.run_worker_id
    where j.id=p_job_id and j.candidate_id=w.candidate_id;
  select dr37_point_107.* into strict v_run from private.bpay_next_pay_run dr37_point_107 where id=v_run_id for update;
  select dr37_point_108.* into strict v_control from private.bpay_next_worker_control dr37_point_108 where candidate_id=v_candidate for update;
  select dr37_point_109.* into strict v_job from private.bpay_next_job dr37_point_109 where id=p_job_id for update;
  select dr37_point_110.* into strict v_request from private.bpay_next_cancel_request dr37_point_110 where command_id=v_job.command_id for update;
  select dr37_point_111.* into strict v_worker from private.bpay_next_run_worker dr37_point_111 where id=v_request.run_worker_id for update;
  select dr37_point_112.* into strict v_binding from private.bpay_next_case_cancel_binding dr37_point_112 where command_id=v_job.command_id for update;
  if v_job.job_kind<>'SIMPLE_CANCEL' or v_job.module_epoch<>v_epoch or v_job.candidate_id<>v_binding.candidate_id
     or v_request.run_worker_id<>v_binding.run_worker_id or v_worker.candidate_id<>v_binding.candidate_id
     or v_worker.captured_preparation_revision<>v_binding.preparation_revision or v_worker.case_selection_revision<>v_binding.selection_revision
     or v_request.expected_line_count<>v_binding.expected_work_count or v_request.released_line_count<>v_binding.released_work_count
     or v_job.applied_line_count<>v_binding.released_work_count
     or coalesce(v_request.cursor_line_no,0)<>v_binding.checkpoint or v_job.cursor_key is distinct from nullif(v_binding.checkpoint,0)::text
     or not exists(select 1 from private.bpay_next_command where id=v_job.command_id and command_kind='SIMPLE_CANCEL'
       and agency_sequence=v_job.command_sequence and module_epoch=v_epoch and expected_member_count=1 and enrolled_member_count=1) then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_SCOPE_INVALID';end if;
  if v_job.status='DONE' and v_request.status in ('CANCELLED','BLOCKED') and v_binding.status=v_request.status then
    return pg_catalog.jsonb_build_object('phase',v_request.status,'run_worker_id',v_worker.id,'cursor',v_job.cursor_key,
      'released_line_count',v_request.released_line_count,'blocked_code',v_request.blocked_code,'rows_visited',0,'holds_released',0,'replay',true);
  end if;
  if v_job.status<>'LEASED' or v_job.phase not in ('NEW','RELEASE') or v_job.lease_nonce<>p_lease_nonce or v_job.owner_epoch<>p_owner_epoch
     or v_control.active_owner_epoch<>p_owner_epoch or v_job.lease_until_utc<=pg_catalog.clock_timestamp()
     or exists(select 1 from private.bpay_next_job where candidate_id=v_candidate and command_sequence<v_job.command_sequence and status<>'DONE') then
    raise exception using errcode='55000',message='BPAY_NEXT_CANCEL_PAGE_LEASE_INVALID';end if;
  if coalesce(p_expected_cursor,0)<>v_binding.checkpoint then
    if coalesce(p_expected_cursor,0)>v_binding.checkpoint then raise exception using errcode='55000',message='BPAY_NEXT_CASE_CANCEL_CURSOR_STALE';end if;
    return pg_catalog.jsonb_build_object('phase',v_request.status,'run_worker_id',v_worker.id,'cursor',v_job.cursor_key,
      'released_line_count',v_request.released_line_count,'rows_visited',0,'holds_released',0,'replay',true);
  end if;
  perform private.bpay_next_assert_candidate_financial_fence_v1(v_candidate,p_job_id,p_lease_nonce,p_owner_epoch);
  select dr37_point_113.* into v_handoff from private.bpay_next_net_cancel_handoff dr37_point_113 where cancel_command_id=v_job.command_id;
  select dr37_point_114.* into v_group from private.bpay_next_destination_group dr37_point_114 where run_worker_id=v_worker.id;
  if v_binding.status='REQUESTED' then
    if (v_job.phase<>'NEW' and not(v_job.phase='RELEASE' and exists(select 1 from private.bpay_next_net_cancel_handoff h
      where h.cancel_command_id=v_job.command_id and h.run_worker_id=v_worker.id and h.candidate_id=v_candidate)))
      or v_request.status<>'REQUESTED' then raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_INTENT_INVALID';end if;
    v_block:=private.bpay_next_case_cancel_block_v1(v_worker.id,false);
    if v_run.status<>'DRAFT' or v_run.confirmed_at_utc is null or v_worker.captured_line_count<>v_binding.expected_work_count then
      v_block:='BPAY_NEXT_CANCEL_DRAFT_CHANGED';end if;
    if v_block is null and v_group.anchor_transfer_id is not null then
      v_group_page:=private.bpay_next_check_destination_cancel_page_v1(p_job_id,p_limit);
      v_block:=v_group_page->>'blocked_code';v_seen:=(v_group_page->>'rows_visited')::integer;
      if v_block is null and (v_group_page->>'complete')::boolean is not true then
        update private.bpay_next_case_cancel_binding set checkpoint=checkpoint+1 where command_id=v_job.command_id
          returning checkpoint into v_checkpoint;
        update private.bpay_next_cancel_request set cursor_line_no=v_checkpoint where command_id=v_job.command_id;
        update private.bpay_next_job set cursor_key=v_checkpoint::text,
          lease_until_utc=pg_catalog.clock_timestamp()+pg_catalog.make_interval(secs=>120) where id=p_job_id;
        return pg_catalog.jsonb_build_object('phase','REQUESTED','run_worker_id',v_worker.id,'cursor',v_checkpoint,
          'released_line_count',0,'rows_visited',v_seen,'holds_released',0,'replay',false);
      end if;
      v_group_started:=true;
    end if;
    if v_block is not null then
      update private.bpay_next_case_cancel_binding set status='BLOCKED',stage='BLOCKED',checkpoint=checkpoint+1,blocked_code=v_block,finished_at_utc=v_now
        where command_id=v_job.command_id returning checkpoint into v_checkpoint;
      update private.bpay_next_cancel_request set status='BLOCKED',blocked_code=v_block,finished_at_utc=v_now,cursor_line_no=v_checkpoint where command_id=v_job.command_id;
      update private.bpay_next_job set status='DONE',phase='BLOCKED',cursor_key=v_checkpoint::text,last_error_code=v_block,lease_nonce=null,lease_until_utc=null where id=p_job_id;
      update private.bpay_next_command set status='COMPLETE' where id=v_job.command_id;
      return pg_catalog.jsonb_build_object('phase','BLOCKED','run_worker_id',v_worker.id,'cursor',v_checkpoint,
        'released_line_count',0,'blocked_code',v_block,'rows_visited',v_seen,'holds_released',0,'replay',false);
    end if;
    v_binding.accepted_projection_revision:=v_worker.net_projection_revision;
    if v_worker.net_projection_revision>0 then
      select dr37_point_115.* into strict v_projection from private.bpay_next_net_projection dr37_point_115 where run_worker_id=v_worker.id
        and projection_no=v_worker.net_projection_revision and retired_at_utc is null;
      select dr37_point_116.* into strict v_net from private.bpay_next_case_allocation_state dr37_point_116 where projection_id=v_projection.id and run_worker_id=v_worker.id
        and pass_kind='NET' and status='COMPLETE' and net_stage='COMPLETE';
      if v_net.preparation_revision<>v_binding.preparation_revision or v_net.selection_revision<>v_binding.selection_revision
         or v_projection.input_kind not in ('PAYE_MANUAL','CASE_PAYOUT') then raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_NET_BINDING_INVALID';end if;
      v_binding.projection_id:=v_projection.id;v_binding.net_state_id:=v_net.id;
    end if;
    if v_group.anchor_transfer_id is not null then
      v_binding.transfer_id:=v_group.anchor_transfer_id;
    else
      select dr37_point_117.* into v_transfer from private.bpay_next_transfer dr37_point_117 where run_worker_id=v_worker.id for update;
      if found then v_binding.transfer_id:=v_transfer.id;update private.bpay_next_transfer set status='CANCELLED' where id=v_transfer.id;end if;
    end if;
    v_binding.expected_case_hold_count:=case when v_handoff.replacement_id is null then v_worker.active_case_hold_count
      else v_handoff.expected_hold_count end;v_binding.started_at_utc:=v_now;
    v_binding.status:='CANCELLING';v_binding.stage:='WORK';
    update private.bpay_next_run_worker set status='CANCELLING',
      review_issue_code=case when review_issue_code in ('CASE_INPUT_UNBOUND','CASE_WEEK_BASIS_UNBOUND','CASE_RETURN_FLOOR_UNBOUND') then null else review_issue_code end,
      review_issue_work_id=case when review_issue_code in ('CASE_INPUT_UNBOUND','CASE_WEEK_BASIS_UNBOUND','CASE_RETURN_FLOOR_UNBOUND') then null else review_issue_work_id end
      where id=v_worker.id;
    update private.bpay_next_cancel_request set status='CANCELLING',started_at_utc=v_now where command_id=v_job.command_id;
    update private.bpay_next_job set phase='RELEASE' where id=p_job_id;
    v_worker.status:='CANCELLING';v_request.status:='CANCELLING';
  end if;
  if v_binding.status<>'CANCELLING' or v_worker.status<>'CANCELLING' or v_request.status<>'CANCELLING'
     or v_run.status<>'DRAFT' or v_worker.realised_effect_count<>0 or v_worker.financial_resolution_count<>0 or v_control.pending_outcome_count<>0
     or v_worker.net_request_revision<>v_binding.accepted_projection_revision or v_worker.net_projection_revision<>v_binding.accepted_projection_revision then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_FENCE_BROKEN';end if;
  if v_group_started then
    -- This STEP examined only the CHECK page. WORK release starts next STEP;
    -- do not combine two independent <=100-row phases into a 200-row call.
    null;
  elsif v_binding.stage='WORK' then
    for v_line in select * from private.bpay_next_run_line where run_worker_id=v_worker.id and line_no>coalesce(v_binding.work_cursor,0)
      order by line_no,id limit p_limit loop
      select dr37_point_118.* into v_work_hold from private.bpay_next_hold dr37_point_118 where run_line_id=v_line.id for update;
      if found then
        if v_work_hold.status<>'ACTIVE' or (v_work_hold.work_id,v_work_hold.component_key,v_work_hold.source_reserved_ex_vat,
          v_work_hold.target_amount_ex_vat,v_work_hold.target_amount_vat,v_work_hold.target_amount_inc_vat)
          is distinct from (v_line.work_id,v_line.component_key,v_line.source_consumed_ex_vat,v_line.frozen_ex_vat,v_line.frozen_vat,v_line.frozen_inc_vat) then
          raise exception using errcode='23514',message='BPAY_NEXT_CANCEL_HOLD_NOT_EXACT_ACTIVE';end if;
        update private.bpay_next_position set held_source_ex_vat=held_source_ex_vat-v_work_hold.source_reserved_ex_vat,
          held_target_ex_vat=held_target_ex_vat-v_work_hold.target_amount_ex_vat,held_target_vat=held_target_vat-v_work_hold.target_amount_vat,
          held_target_inc_vat=held_target_inc_vat-v_work_hold.target_amount_inc_vat,updated_at_utc=v_now
          where work_id=v_work_hold.work_id and component_key=v_work_hold.component_key and held_source_ex_vat>=v_work_hold.source_reserved_ex_vat
            and held_target_ex_vat>=v_work_hold.target_amount_ex_vat and held_target_vat>=v_work_hold.target_amount_vat and held_target_inc_vat>=v_work_hold.target_amount_inc_vat;
        if not found then raise exception using errcode='23514',message='BPAY_NEXT_CANCEL_POSITION_HOLD_MISMATCH';end if;
        update private.bpay_next_hold set status='RELEASED',finished_at_utc=v_now where id=v_work_hold.id;v_holds:=v_holds+1;
      elsif (v_line.source_consumed_ex_vat,v_line.frozen_ex_vat,v_line.frozen_vat,v_line.frozen_inc_vat) is distinct from (0::numeric,0::numeric,0::numeric,0::numeric) then
        raise exception using errcode='23514',message='BPAY_NEXT_CANCEL_POSITIVE_HOLD_MISSING';end if;
      v_binding.work_cursor:=v_line.line_no;v_binding.released_work_count:=v_binding.released_work_count+1;v_seen:=v_seen+1;
    end loop;
    select exists(select 1 from private.bpay_next_run_line where run_worker_id=v_worker.id and line_no>coalesce(v_binding.work_cursor,0)) into v_more;
    if not v_more then
      if v_binding.released_work_count<>v_binding.expected_work_count then raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_WORK_COUNT_INVALID';end if;
      v_binding.stage:='CASE';
    end if;
  elsif v_binding.stage='CASE' and v_handoff.replacement_id is not null then
    -- The original NET job released these exact holds. Only this genuine cancellation
    -- owns the existing linked recovery reconciliation; visit even unlinked members.
    for v_member in select * from private.bpay_next_net_cancel_hold_member
      where replacement_id=v_handoff.replacement_id
        and (v_binding.case_hold_cursor is null or case_hold_id>v_binding.case_hold_cursor)
      order by case_hold_id limit p_limit loop
      select dr37_point_119.* into strict v_hold from private.bpay_next_case_hold dr37_point_119 where id=v_member.case_hold_id;
      select dr37_point_120.* into strict v_instruction from private.bpay_next_run_case_instruction dr37_point_120 where id=v_hold.instruction_id;
      select dr37_point_121.* into strict v_result from private.bpay_next_case_allocation_result dr37_point_121 where id=v_hold.allocation_result_id;
      select dr37_point_122.* into strict v_use from private.bpay_next_case_capacity_use dr37_point_122 where case_hold_id=v_hold.id;
      select dr37_point_123.* into strict v_audit from private.bpay_next_case_hold_transition dr37_point_123 where hold_id=v_hold.id and event_ordinal=2;
      v_size:=pg_catalog.octet_length((pg_catalog.to_jsonb(v_member)||pg_catalog.jsonb_build_object(
        'hold',pg_catalog.to_jsonb(v_hold),'instruction',pg_catalog.to_jsonb(v_instruction),
        'result',pg_catalog.to_jsonb(v_result),'use',pg_catalog.to_jsonb(v_use),'audit',pg_catalog.to_jsonb(v_audit)))::text);
      if v_size>65536 then raise exception using errcode='54000',message='BPAY_NEXT_NET_FIXED_ROW_BYTES_EXCEEDED';end if;
      if v_bytes+v_size>65536 then exit;end if;v_bytes:=v_bytes+v_size;
      if v_handoff.run_worker_id<>v_worker.id or v_handoff.candidate_id<>v_candidate
        or v_handoff.expected_hold_count<>v_handoff.released_hold_count
        or v_binding.expected_case_hold_count<>v_handoff.expected_hold_count
        or v_hold.run_worker_id<>v_worker.id or v_hold.status<>'RELEASED' or v_use.status<>'RELEASED'
        or v_use.realisation_event_id is not null or v_use.source_amount_ex_vat<>v_member.source_amount
        or (v_member.instruction_id,v_member.allocation_state_id,v_member.source_amount)
          is distinct from (v_instruction.id,v_result.state_id,v_hold.source_reserved_ex_vat)
        or v_instruction.run_worker_id<>v_worker.id or v_instruction.preparation_revision<>v_binding.preparation_revision
        or v_instruction.selection_revision<>v_binding.selection_revision or v_result.instruction_id<>v_instruction.id
        or (v_result.state_id in (v_handoff.draft_state_id,v_handoff.prior_state_id,v_handoff.target_state_id)) is not true
        or (v_hold.source_reserved_ex_vat,v_hold.target_amount_ex_vat,v_hold.target_amount_vat,v_hold.target_amount_inc_vat)
          is distinct from (v_result.allocated_source_ex_vat,v_result.allocated_target_ex_vat,v_result.allocated_target_vat,v_result.allocated_target_inc_vat)
        or v_audit.transition<>'RELEASED' or v_audit.cause_job_id<>v_handoff.original_job_id
        or v_audit.command_id<>v_job.command_id or v_audit.command_kind<>'SIMPLE_CANCEL' then
        raise exception using errcode='23514',message='BPAY_NEXT_NET_CANCEL_RECONCILIATION_IDENTITY_INVALID';end if;
      if v_instruction.work_collection_id is not null then
        perform private.bpay_next_reconcile_cancelled_work_collection_v1(p_job_id,v_hold.id);
      end if;
      v_binding.case_hold_cursor:=v_hold.id;v_binding.released_case_hold_count:=v_binding.released_case_hold_count+1;
      v_seen:=v_seen+1;
    end loop;
    select exists(select 1 from private.bpay_next_net_cancel_hold_member where replacement_id=v_handoff.replacement_id
      and (v_binding.case_hold_cursor is null or case_hold_id>v_binding.case_hold_cursor)) into v_more;
    if not v_more then
      if v_binding.released_case_hold_count<>v_binding.expected_case_hold_count or v_worker.active_case_hold_count<>0
        or exists(select 1 from private.bpay_next_case_hold where run_worker_id=v_worker.id and status='ACTIVE') then
        raise exception using errcode='23514',message='BPAY_NEXT_NET_CANCEL_RECONCILIATION_COUNT_INVALID';end if;
      v_binding.stage:='FINAL';
    elsif v_seen=0 then raise exception using errcode='23514',message='BPAY_NEXT_NET_CANCEL_RECONCILIATION_CURSOR_INVALID';end if;
  elsif v_binding.stage='CASE' then
    for v_hold in select * from private.bpay_next_case_hold where run_worker_id=v_worker.id and status='ACTIVE'
      and (v_binding.case_hold_cursor is null or id>v_binding.case_hold_cursor) order by id limit p_limit loop
      select dr37_point_124.* into strict v_instruction from private.bpay_next_run_case_instruction dr37_point_124 where id=v_hold.instruction_id;
      select dr37_point_125.* into strict v_result from private.bpay_next_case_allocation_result dr37_point_125 where id=v_hold.allocation_result_id;
      select dr37_point_126.* into strict v_case from private.bpay_next_finance_case dr37_point_126 where id=v_hold.case_id for update;
      select dr37_point_127.* into strict v_component from private.bpay_next_case_component dr37_point_127 where id=v_hold.case_component_id for update;
      select dr37_point_128.* into strict v_period from private.bpay_next_case_period dr37_point_128 where case_component_id=v_hold.case_component_id and pay_week_start=v_hold.pay_week_start for update;
      perform 1 from private.bpay_next_case_hold where id=v_hold.id for update;
      select dr37_point_129.* into strict v_use from private.bpay_next_case_capacity_use dr37_point_129 where case_hold_id=v_hold.id for update;
      if v_instruction.run_worker_id<>v_worker.id or v_instruction.preparation_revision<>v_binding.preparation_revision
         or v_instruction.selection_revision<>v_binding.selection_revision or v_result.instruction_id<>v_instruction.id
         or v_result.state_id is distinct from (case when v_instruction.payroll_stage='NET_DEDUCT' and v_binding.accepted_projection_revision>0
           then v_binding.net_state_id else v_binding.draft_state_id end)
         or v_hold.allocation_pass_kind<>v_result.pass_kind or v_use.status<>'ACTIVE' or v_use.realisation_event_id is not null
         or v_use.source_amount_ex_vat<>v_hold.source_reserved_ex_vat
         or (v_hold.source_reserved_ex_vat,v_hold.target_amount_ex_vat,v_hold.target_amount_vat,v_hold.target_amount_inc_vat)
           is distinct from (v_result.allocated_source_ex_vat,v_result.allocated_target_ex_vat,v_result.allocated_target_vat,v_result.allocated_target_inc_vat) then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_HOLD_BINDING_INVALID';end if;
      v_amount:=v_hold.source_reserved_ex_vat;
      if v_hold.purpose='PAYOUT' then
        update private.bpay_next_case_component set active_payout_source_ex_vat=active_payout_source_ex_vat-v_amount,
          component_revision=component_revision+1,updated_at_utc=v_now where id=v_component.id and active_payout_source_ex_vat>=v_amount;
        if not found then raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_COMPONENT_CAPACITY_INVALID';end if;
        update private.bpay_next_finance_case set active_payout_hold_amount=active_payout_hold_amount-v_amount,active_hold_amount=active_hold_amount-v_amount,
          case_revision=case_revision+1,updated_at_utc=v_now where id=v_case.id and active_payout_hold_amount>=v_amount and active_hold_amount>=v_amount;
      else
        update private.bpay_next_case_component set active_recovery_source_ex_vat=active_recovery_source_ex_vat-v_amount,
          component_revision=component_revision+1,updated_at_utc=v_now where id=v_component.id and active_recovery_source_ex_vat>=v_amount;
        if not found then raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_COMPONENT_CAPACITY_INVALID';end if;
        update private.bpay_next_finance_case set active_recovery_hold_amount=active_recovery_hold_amount-v_amount,active_hold_amount=active_hold_amount-v_amount,
          case_revision=case_revision+1,updated_at_utc=v_now where id=v_case.id and active_recovery_hold_amount>=v_amount and active_hold_amount>=v_amount;
      end if;
      if not found then raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_CASE_CAPACITY_INVALID';end if;
      if v_hold.purpose='PAYOUT' then
        update private.bpay_next_case_period set active_payout_source_ex_vat=active_payout_source_ex_vat-v_amount,period_revision=period_revision+1
          where case_component_id=v_component.id and pay_week_start=v_hold.pay_week_start and active_payout_source_ex_vat>=v_amount;
      else
        update private.bpay_next_case_period set active_unrealised_recovery_source_ex_vat=active_unrealised_recovery_source_ex_vat-v_amount,period_revision=period_revision+1
          where case_component_id=v_component.id and pay_week_start=v_hold.pay_week_start and active_unrealised_recovery_source_ex_vat>=v_amount;
      end if;
      if not found then raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_PERIOD_CAPACITY_INVALID';end if;
      update private.bpay_next_case_hold set status='RELEASED',finished_at_utc=v_now where id=v_hold.id;
    perform private.bpay_next_record_case_hold_transition_v1(p_job_id,v_hold.id,'RELEASED');
      update private.bpay_next_case_capacity_use set status='RELEASED' where case_hold_id=v_hold.id;
      update private.bpay_next_run_worker set active_case_hold_count=active_case_hold_count-1 where id=v_worker.id and active_case_hold_count>0;
      if not found then raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_WORKER_HOLD_COUNT_INVALID';end if;
      if v_instruction.work_collection_id is not null then
        perform private.bpay_next_reconcile_cancelled_work_collection_v1(p_job_id,v_hold.id);
      end if;
      v_binding.case_hold_cursor:=v_hold.id;v_binding.released_case_hold_count:=v_binding.released_case_hold_count+1;v_seen:=v_seen+1;v_holds:=v_holds+1;
    end loop;
    select exists(select 1 from private.bpay_next_case_hold where run_worker_id=v_worker.id and status='ACTIVE') into v_more;
    if not v_more then
      if v_binding.released_case_hold_count<>v_binding.expected_case_hold_count
         or (select active_case_hold_count from private.bpay_next_run_worker where id=v_worker.id)<>0 then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_CASE_COUNT_INVALID';end if;
      v_binding.stage:='FINAL';
    elsif v_seen=0 then raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_CASE_CURSOR_INVALID';end if;
  elsif v_binding.stage='FINAL' then
    if v_binding.released_work_count<>v_binding.expected_work_count or v_binding.released_case_hold_count<>v_binding.expected_case_hold_count
       or v_worker.active_case_hold_count<>0 or exists(select 1 from private.bpay_next_case_hold where run_worker_id=v_worker.id and status='ACTIVE')
       or v_run.cancelled_candidate_count>=v_run.selected_candidate_count then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_FINAL_COUNT_INVALID';end if;
    if v_group.anchor_transfer_id is not null then
      v_group_page:=private.bpay_next_finish_destination_cancel_page_v1(p_job_id,p_limit);
      v_seen:=(v_group_page->>'rows_visited')::integer;
    end if;
    if v_group.anchor_transfer_id is null or (v_group_page->>'complete')::boolean is true then
      if v_binding.projection_id is not null then update private.bpay_next_net_projection set retired_at_utc=v_now where id=v_binding.projection_id and retired_at_utc is null;end if;
      update private.bpay_next_run_worker set status='CANCELLED' where id=v_worker.id;
      update private.bpay_next_pay_run set cancelled_candidate_count=cancelled_candidate_count+1,
        status=case when cancelled_candidate_count+1=selected_candidate_count then 'CANCELLED' else status end where id=v_run.id;
      v_binding.status:='CANCELLED';v_binding.stage:='COMPLETE';v_binding.finished_at_utc:=v_now;
      if v_group.anchor_transfer_id is null then v_seen:=1;end if;
    end if;
  else raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_STAGE_INVALID';end if;
  update private.bpay_next_case_cancel_binding set status=v_binding.status,stage=v_binding.stage,checkpoint=checkpoint+1,
    released_work_count=v_binding.released_work_count,work_cursor=v_binding.work_cursor,expected_case_hold_count=v_binding.expected_case_hold_count,
    released_case_hold_count=v_binding.released_case_hold_count,case_hold_cursor=v_binding.case_hold_cursor,
    accepted_projection_revision=v_binding.accepted_projection_revision,projection_id=v_binding.projection_id,net_state_id=v_binding.net_state_id,
    transfer_id=v_binding.transfer_id,started_at_utc=v_binding.started_at_utc,finished_at_utc=v_binding.finished_at_utc
    where command_id=v_job.command_id returning checkpoint into v_checkpoint;
  update private.bpay_next_cancel_request set status=v_binding.status,cursor_line_no=v_checkpoint,released_line_count=v_binding.released_work_count,
    finished_at_utc=v_binding.finished_at_utc where command_id=v_job.command_id;
  update private.bpay_next_job set cursor_key=v_checkpoint::text,applied_line_count=v_binding.released_work_count,
    lease_until_utc=pg_catalog.clock_timestamp()+pg_catalog.make_interval(secs=>120) where id=p_job_id;
  update private.bpay_next_worker_control set financial_view_revision=financial_view_revision+1,updated_at_utc=v_now where candidate_id=v_candidate;
  if v_binding.status='CANCELLED' then
    perform private.bpay_next_apply_cancelled_week_v1(p_job_id);
    update private.bpay_next_job set status='DONE',phase='CANCELLED',lease_nonce=null,lease_until_utc=null where id=p_job_id;
    update private.bpay_next_command set status='COMPLETE' where id=v_job.command_id;
  end if;
  return pg_catalog.jsonb_build_object('phase',v_binding.status,'run_worker_id',v_worker.id,'cursor',v_checkpoint,
    'released_line_count',v_binding.released_work_count,'rows_visited',v_seen,'holds_released',v_holds,'replay',false);
end
$function$;

create or replace function private.bpay_next_apply_cancelled_week_v1(p_job_id uuid)
returns void language plpgsql security definer set search_path=pg_catalog,private,public
as $function$
declare
  v_job private.bpay_next_job%rowtype;v_request private.bpay_next_cancel_request%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;v_run private.bpay_next_pay_run%rowtype;
  v_control private.bpay_next_worker_control%rowtype;v_week private.bpay_next_worker_week%rowtype;
  v_prior private.bpay_next_worker_week_contribution%rowtype;v_transfer private.bpay_next_transfer%rowtype;
  v_case_cancel private.bpay_next_case_cancel_binding%rowtype;
  v_group private.bpay_next_destination_group%rowtype;v_group_cancel private.bpay_next_destination_cancel%rowtype;
  v_epoch bigint;v_transfer_count integer:=0;v_old_resolved numeric;v_old_unresolved integer;
  v_handoff private.bpay_next_net_cancel_handoff%rowtype;
begin
  if p_job_id is null then raise exception using errcode='22023',message='BPAY_NEXT_WEEK_CANCEL_INPUT_INVALID';end if;
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id;
  if v_job.job_kind<>'SIMPLE_CANCEL' then
    raise exception using errcode='23514',message='BPAY_NEXT_WEEK_CANCEL_JOB_KIND_INVALID';
  end if;
  select r.* into strict v_request from private.bpay_next_cancel_request r where r.command_id=v_job.command_id;
  select r.* into strict v_run from private.bpay_next_pay_run r
    join private.bpay_next_run_worker w on w.run_id=r.id where w.id=v_request.run_worker_id for update of r;
  select c.* into strict v_control from private.bpay_next_worker_control c where c.candidate_id=v_request.candidate_id for update;
  select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id for update;
  select w.* into strict v_worker from private.bpay_next_run_worker w where w.id=v_request.run_worker_id for update;
  if v_job.job_kind<>'SIMPLE_CANCEL' or v_job.module_epoch<>v_epoch
     or v_job.candidate_id<>v_worker.candidate_id or v_request.candidate_id<>v_worker.candidate_id
     or v_request.status<>'CANCELLED' or v_worker.status<>'CANCELLED'
     or v_request.released_line_count<>v_request.expected_line_count
     or v_job.applied_line_count<>v_request.released_line_count
     or v_worker.realised_effect_count<>0 or v_worker.financial_resolution_count<>0
     then
    raise exception using errcode='23514',message='BPAY_NEXT_WEEK_CANCEL_SCOPE_INVALID';
  end if;
  if v_worker.case_selection_revision>0 then
    select b.* into strict v_case_cancel from private.bpay_next_case_cancel_binding b
      where b.command_id=v_job.command_id;
    if v_case_cancel.run_worker_id<>v_worker.id or v_case_cancel.candidate_id<>v_worker.candidate_id
       or v_case_cancel.preparation_revision<>v_worker.captured_preparation_revision
       or v_case_cancel.selection_revision<>v_worker.case_selection_revision
       or v_case_cancel.status<>'CANCELLED' or v_case_cancel.stage<>'COMPLETE'
       or v_case_cancel.expected_work_count<>v_request.expected_line_count
       or v_case_cancel.released_work_count<>v_request.released_line_count
       or v_case_cancel.expected_case_hold_count is null
       or v_case_cancel.released_case_hold_count<>v_case_cancel.expected_case_hold_count
       or v_worker.active_case_hold_count<>0
       or v_request.cursor_line_no is distinct from v_case_cancel.checkpoint
       or v_job.cursor_key is distinct from v_case_cancel.checkpoint::text
       or exists(select 1 from private.bpay_next_case_hold h
           where h.run_worker_id=v_worker.id and h.status='ACTIVE') then
      raise exception using errcode='23514',message='BPAY_NEXT_WEEK_CASE_CANCEL_BINDING_INVALID';
    end if;
  end if;
  select dr37_point_130.* into v_handoff from private.bpay_next_net_cancel_handoff dr37_point_130 where cancel_command_id=v_job.command_id;
  if v_worker.net_replacement_pending_id is not null then
    if v_handoff.replacement_id is distinct from v_worker.net_replacement_pending_id
      or v_handoff.run_worker_id<>v_worker.id or v_handoff.candidate_id<>v_worker.candidate_id
      or v_handoff.prior_projection_id is distinct from v_case_cancel.projection_id
      or v_handoff.prior_state_id is distinct from v_case_cancel.net_state_id
      or v_handoff.draft_state_id<>v_case_cancel.draft_state_id then
      raise exception using errcode='23514',message='BPAY_NEXT_WEEK_NET_CANCEL_HANDOFF_INVALID';end if;
    perform private.bpay_next_assert_candidate_financial_fence_v1(v_worker.candidate_id,p_job_id,v_job.lease_nonce,v_job.owner_epoch);
  elsif v_handoff.replacement_id is not null then
    if v_job.status<>'DONE' then raise exception using errcode='23514',message='BPAY_NEXT_WEEK_NET_CANCEL_PENDING_MISSING';end if;
  else
    perform private.bpay_next_assert_candidate_financial_fence_v1(v_worker.candidate_id,p_job_id,v_job.lease_nonce,v_job.owner_epoch);
  end if;
  if v_job.status='DONE' then return;end if;
  if v_job.status<>'LEASED' or v_job.phase<>'RELEASE' or v_job.lease_nonce is null
     or v_job.owner_epoch<>v_control.active_owner_epoch or v_job.lease_until_utc<=pg_catalog.clock_timestamp()
     or v_control.pending_outcome_count<>0 then
    raise exception using errcode='55000',message='BPAY_NEXT_WEEK_CANCEL_OWNER_INVALID';
  end if;
  select g.* into v_group from private.bpay_next_destination_group g where g.run_worker_id=v_worker.id;
  if found then
    -- The guarded certificate counts exact <=100-row CHECK and FINAL ranges.
    -- Do not scan every sibling again or use an arbitrary counter as authority.
    select c.* into strict v_group_cancel from private.bpay_next_destination_cancel c where c.command_id=v_job.command_id;
    if v_worker.case_selection_revision<1 or v_group.candidate_id<>v_worker.candidate_id
       or v_group.stage<>'COMPLETE' or v_group.sealed_leg_count<>v_group.expected_leg_count
       or v_group_cancel.anchor_transfer_id<>v_group.anchor_transfer_id
       or v_group_cancel.expected_leg_count<>v_group.expected_leg_count
       or v_group_cancel.checked_leg_count<>v_group_cancel.expected_leg_count
       or v_group_cancel.cancelled_leg_count<>v_group_cancel.expected_leg_count
       or v_group_cancel.check_cursor is null or v_group_cancel.cancel_cursor is null
       or v_group_cancel.check_cursor<>v_group_cancel.cancel_cursor
       or v_case_cancel.transfer_id is distinct from v_group.anchor_transfer_id
       or v_case_cancel.projection_id is distinct from v_group.projection_id
       or v_case_cancel.net_state_id is distinct from v_group.net_state_id then
      raise exception using errcode='23514',message='BPAY_NEXT_WEEK_DESTINATION_CANCEL_CERTIFICATE_INVALID';
    end if;
  else
    -- Ordinary no-split scope retains its exact one-original-transfer guard.
    for v_transfer in select t.* from private.bpay_next_transfer t where t.run_worker_id=v_worker.id
      order by t.transfer_no limit 2 for update
    loop
      v_transfer_count:=v_transfer_count+1;
      if v_transfer_count>1 or v_transfer.status<>'CANCELLED' or v_transfer.original_transfer_id is not null
         or v_transfer.return_cash_id is not null or v_transfer.candidate_id<>v_worker.candidate_id
         or v_transfer.account_approval_ref is not null
         or exists(select 1 from private.bpay_next_csv_instruction i where i.transfer_id=v_transfer.id)
         or exists(select 1 from private.bpay_next_transfer_outcome o where o.transfer_id=v_transfer.id) then
        raise exception using errcode='23514',message='BPAY_NEXT_WEEK_CANCEL_TRANSFER_PROTECTED';
      end if;
    end loop;
  end if;
  select w.* into v_week from private.bpay_next_worker_week w where w.candidate_id=v_worker.candidate_id
    and w.pay_week_start=v_run.pay_date-(extract(isodow from v_run.pay_date)::integer-1) for update;
  select x.* into v_prior from private.bpay_next_worker_week_contribution x where x.original_run_worker_id=v_worker.id for update;
  if v_prior.id is null then
    if v_handoff.replacement_id is not null then raise exception using errcode='23514',message='BPAY_NEXT_WEEK_NET_CANCEL_CONTRIBUTION_MISSING';end if;
    raise notice 'BPAY_NEXT_WEEK_CANCEL_CONTRIBUTION_UNBOUND';return;
  end if;
  if v_week.candidate_id is null or v_prior.candidate_id<>v_worker.candidate_id
     or v_prior.pay_week_start<>v_week.pay_week_start or v_prior.original_pay_date<>v_run.pay_date
     or v_prior.original_created_at_utc<>v_run.created_at_utc or v_prior.original_gross_amount<>v_worker.gross_inc_vat
     or v_prior.original_transfer_id is not null or v_prior.returned_cash_id is not null or v_prior.reissue_transfer_id is not null then
    raise exception using errcode='23514',message='BPAY_NEXT_WEEK_CANCEL_CONTRIBUTION_NOT_EXACT';
  end if;
  if v_prior.payment_state='CANCELLED' and v_prior.eligibility_state='EXCLUDED' and v_prior.eligible_arranged_amount=0 then
  if v_handoff.replacement_id is not null then
    update private.bpay_next_candidate_week_commit_fence set stage='CANCELLED',updated_at_utc=pg_catalog.transaction_timestamp()
      where replacement_id=v_handoff.replacement_id and candidate_id=v_worker.candidate_id and stage='CANCELLING';
    if not found then raise exception using errcode='23514',message='BPAY_NEXT_WEEK_NET_CANCEL_FENCE_MISSING';end if;
    update private.bpay_next_run_worker set net_replacement_pending_id=null where id=v_worker.id
      and net_replacement_pending_id=v_handoff.replacement_id;
    if not found then raise exception using errcode='23514',message='BPAY_NEXT_WEEK_NET_CANCEL_PENDING_MISSING';end if;
  end if;
    return;end if;
  if v_prior.payment_state<>'ARRANGED' then raise exception using errcode='23514',message='BPAY_NEXT_WEEK_CANCEL_NOT_UNPAID';end if;
  v_old_resolved:=coalesce(v_prior.eligible_arranged_amount,0);
  v_old_unresolved:=case when v_prior.eligibility_state='UNBOUND' then 1 else 0 end;
  update private.bpay_next_worker_week_contribution set payment_state='CANCELLED',eligibility_state='EXCLUDED',eligible_arranged_amount=0,
    contribution_revision=contribution_revision+1,primary_binding_revision=primary_binding_revision+1 where id=v_prior.id;
  update private.bpay_next_worker_week set resolved_arranged_take_home=resolved_arranged_take_home-v_old_resolved,
    unresolved_contribution_count=unresolved_contribution_count-v_old_unresolved,period_revision=period_revision+1
    where candidate_id=v_worker.candidate_id and pay_week_start=v_week.pay_week_start
      and resolved_arranged_take_home>=v_old_resolved and unresolved_contribution_count>=v_old_unresolved;
  if not found then raise exception using errcode='23514',message='BPAY_NEXT_WEEK_CANCEL_DELTA_INVALID';end if;
  if v_handoff.replacement_id is not null then
    update private.bpay_next_candidate_week_commit_fence set stage='CANCELLED',updated_at_utc=pg_catalog.transaction_timestamp()
      where replacement_id=v_handoff.replacement_id and candidate_id=v_worker.candidate_id and stage='CANCELLING';
    if not found then raise exception using errcode='23514',message='BPAY_NEXT_WEEK_NET_CANCEL_FENCE_MISSING';end if;
    update private.bpay_next_run_worker set net_replacement_pending_id=null where id=v_worker.id
      and net_replacement_pending_id=v_handoff.replacement_id;
    if not found then raise exception using errcode='23514',message='BPAY_NEXT_WEEK_NET_CANCEL_PENDING_MISSING';end if;
  end if;
end
$function$;


-- DR-37 bridge
create or replace function private.bpay_next_begin_simple_transfer_v1(
  p_command_id uuid,p_run_worker_id uuid
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_run_id uuid;
  v_run private.bpay_next_pay_run%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_projection private.bpay_next_net_projection%rowtype;
  v_transfer private.bpay_next_transfer%rowtype;
  v_sequence bigint;
  v_candidate_id uuid;
  v_case_selection_revision bigint;
begin
  if p_command_id is null or p_run_worker_id is null then
    raise exception using errcode='22023',
      message='BPAY_NEXT_TRANSFER_BEGIN_INPUT_INVALID';
  end if;
  if not exists(select 1 from private.bpay_next_module_control m
                where m.id=1 and m.active_owner='NEXT' for share) then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select run_id,candidate_id,case_selection_revision
    into strict v_run_id,v_candidate_id,v_case_selection_revision from private.bpay_next_run_worker
    where id=p_run_worker_id;
  if v_case_selection_revision>0 then
    return private.bpay_next_begin_case_transfer_v1(p_command_id,p_run_worker_id);
  end if;
  select dr37_point_131.* into strict v_run from private.bpay_next_pay_run dr37_point_131
    where id=v_run_id for update;
  perform 1 from private.bpay_next_worker_control where candidate_id=v_candidate_id for update;
  select dr37_point_132.* into strict v_worker from private.bpay_next_run_worker dr37_point_132
    where id=p_run_worker_id for update;
  select dr37_point_133.* into v_transfer from private.bpay_next_transfer dr37_point_133
    where build_command_id=p_command_id;
  if found then
    if v_transfer.run_worker_id<>p_run_worker_id
       or v_transfer.candidate_id<>v_worker.candidate_id then
      raise exception using errcode='23514',
        message='BPAY_NEXT_TRANSFER_BEGIN_REPLAY_CONFLICT';
    end if;
    select agency_sequence into strict v_sequence
      from private.bpay_next_command
      where id=p_command_id and command_kind='TRANSFER_BUILD';
    return pg_catalog.jsonb_build_object('sequence',v_sequence,
      'transfer_id',v_transfer.id,'phase',v_transfer.status,'replay',true);
  end if;
  if v_run.status<>'DRAFT' or v_run.confirmed_at_utc is null
     or exists(select 1 from private.bpay_next_cancel_request r
               where r.run_worker_id=v_worker.id and r.status in ('REQUESTED','CANCELLING'))
     or v_worker.status<>'READY'
     or v_worker.target_pay_channel not in ('PAYE','UMBRELLA')
     or (v_worker.target_pay_channel='PAYE' and
         (v_worker.net_projection_revision<1 or v_worker.net_request_revision<>v_worker.net_projection_revision or v_worker.net_replacement_pending_id is not null))
     or (v_worker.target_pay_channel='UMBRELLA' and
         (v_worker.net_projection_revision<>0 or v_worker.net_request_revision<>0
          or v_worker.entered_paye_net is not null
          or v_worker.target_umbrella_id is null or v_worker.target_umbrella_vat_chargeable is null
          or v_worker.gross_inc_vat<=0 or v_worker.captured_line_count<1
          or v_worker.financial_resolution_count<>0 or v_worker.expected_managed_position_count<>0
          or exists(select 1 from private.bpay_next_paye_net_request where run_worker_id=v_worker.id)))
     or v_worker.case_selection_revision<>0
     or exists(select 1 from private.bpay_next_transfer t
               where t.run_worker_id=v_worker.id) then
    raise exception using errcode='55000',
      message='BPAY_NEXT_SIMPLE_TRANSFER_NOT_ELIGIBLE';
  end if;
  if v_worker.target_pay_channel='UMBRELLA' then
    -- No payroll NET request/value, CASE adjustment or live financial lookup.
    -- Bounded member pages must certify every included line as same-channel
    -- TAXABLE WORK before the transfer can become issuable.
    insert into private.bpay_next_net_projection
      (run_worker_id,request_command_id,projection_no,input_kind,
       gross_ex_vat,gross_vat,gross_inc_vat,entered_paye_net,accepted_recoveries,cash_amount)
      values(v_worker.id,null,1,'UMBRELLA',v_worker.gross_ex_vat,v_worker.gross_vat,
        v_worker.gross_inc_vat,null,0,v_worker.gross_inc_vat);
    update private.bpay_next_run_worker set net_projection_revision=1 where id=v_worker.id;
  end if;
  select dr37_point_134.* into strict v_projection from private.bpay_next_net_projection dr37_point_134
    where run_worker_id=v_worker.id and projection_no=case when v_worker.target_pay_channel='UMBRELLA' then 1 else v_worker.net_projection_revision end and retired_at_utc is null;
  if v_worker.target_pay_channel='PAYE' and (v_projection.input_kind<>'PAYE_MANUAL'
     or v_projection.gross_ex_vat<>v_worker.gross_ex_vat
     or v_projection.gross_vat<>0
     or v_projection.gross_inc_vat<>v_worker.gross_inc_vat
     or v_projection.entered_paye_net is distinct from
        v_worker.entered_paye_net
     or v_projection.accepted_recoveries<>0
     or v_projection.cash_amount<0
     or v_projection.cash_amount<>v_worker.entered_paye_net
     or v_projection.request_command_id is null) then
    raise exception using errcode='23514',
      message='BPAY_NEXT_SIMPLE_TRANSFER_PROJECTION_INVALID';
  end if;
  if v_worker.target_pay_channel='UMBRELLA' and
     ((v_projection.input_kind,v_projection.gross_ex_vat,v_projection.gross_vat,
       v_projection.gross_inc_vat,v_projection.cash_amount,v_projection.accepted_recoveries,
       v_projection.accepted_gross_additions,v_projection.accepted_gross_deductions,v_projection.accepted_net_additions)
       is distinct from ('UMBRELLA'::text,v_worker.gross_ex_vat,v_worker.gross_vat,
         v_worker.gross_inc_vat,v_worker.gross_inc_vat,0::numeric,0::numeric,0::numeric,0::numeric)
      or v_projection.entered_paye_net is not null or v_projection.request_command_id is not null) then
    raise exception using errcode='23514',message='BPAY_NEXT_SIMPLE_TRANSFER_PROJECTION_INVALID';
  end if;
  v_sequence:=private.bpay_next_receive_command_v1(
    p_command_id,'TRANSFER_BUILD');
  insert into private.bpay_next_command_member
    (command_id,candidate_id,member_no)
    values(p_command_id,v_worker.candidate_id,1);
  insert into private.bpay_next_transfer
    (run_worker_id,candidate_id,projection_id,build_command_id,
     transfer_no,beneficiary_kind,beneficiary_id,cash_amount,status,execution_kind)
    values(v_worker.id,v_worker.candidate_id,v_projection.id,p_command_id,
      1,case when v_worker.target_pay_channel='UMBRELLA' then 'UMBRELLA' else 'CANDIDATE' end,
      case when v_worker.target_pay_channel='UMBRELLA' then v_worker.target_umbrella_id else v_worker.candidate_id end,
      v_projection.cash_amount,'BUILDING',
      case when v_projection.cash_amount=0 then 'INTERNAL_ZERO' else 'BANK' end)
    returning * into v_transfer;
  update private.bpay_next_command
    set expected_member_count=1,status='SEALED',
        sealed_at_utc=pg_catalog.transaction_timestamp()
    where id=p_command_id;
  return pg_catalog.jsonb_build_object('sequence',v_sequence,
    'transfer_id',v_transfer.id,'phase','BUILDING','replay',false);
end
$function$;

create or replace function private.bpay_next_build_simple_transfer_page_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,
  p_expected_cursor bigint,p_limit integer
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_run_id uuid;
  v_candidate_id uuid;
  v_run private.bpay_next_pay_run%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_job private.bpay_next_job%rowtype;
  v_transfer private.bpay_next_transfer%rowtype;
  v_projection private.bpay_next_net_projection%rowtype;
  v_line record;
  v_seen integer:=0;
  v_member_count bigint;
  v_cash_sum numeric(18,2);
  v_cursor bigint;
  v_adjustment numeric(18,2);
  v_epoch bigint;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null
     or p_limit is null or p_limit not between 1 and 100
     or p_expected_cursor is not null and p_expected_cursor<1 then
    raise exception using errcode='22023',
      message='BPAY_NEXT_TRANSFER_PAGE_INPUT_INVALID';
  end if;
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select w.run_id,w.candidate_id into strict v_run_id,v_candidate_id
    from private.bpay_next_job j
    join private.bpay_next_transfer t on t.build_command_id=j.command_id
    join private.bpay_next_run_worker w on w.id=t.run_worker_id
    where j.id=p_job_id and j.candidate_id=w.candidate_id;
  if exists(select 1 from private.bpay_next_destination_group b
     join private.bpay_next_transfer t on t.id=b.anchor_transfer_id
     join private.bpay_next_job j on j.command_id=t.build_command_id where j.id=p_job_id) then
    return private.bpay_next_build_destination_group_page_v1(
      p_job_id,p_lease_nonce,p_owner_epoch,p_expected_cursor,p_limit);
  end if;
  if exists(select 1 from private.bpay_next_case_transfer_build b
     join private.bpay_next_transfer t on t.id=b.transfer_id
     join private.bpay_next_job j on j.command_id=t.build_command_id
     where j.id=p_job_id) then
    return private.bpay_next_build_case_transfer_page_v1(
      p_job_id,p_lease_nonce,p_owner_epoch,p_expected_cursor,p_limit);
  end if;
  select dr37_point_135.* into strict v_run from private.bpay_next_pay_run dr37_point_135
    where id=v_run_id for update;
  perform 1 from private.bpay_next_worker_control
    where candidate_id=v_candidate_id for update;
  select dr37_point_136.* into strict v_job from private.bpay_next_job dr37_point_136
    where id=p_job_id for update;
  select dr37_point_137.* into strict v_transfer from private.bpay_next_transfer dr37_point_137
    where build_command_id=v_job.command_id for update;
  select dr37_point_138.* into strict v_worker from private.bpay_next_run_worker dr37_point_138
    where id=v_transfer.run_worker_id for update;
  select dr37_point_139.* into strict v_projection from private.bpay_next_net_projection dr37_point_139
    where id=v_transfer.projection_id;
  if v_job.job_kind<>'TRANSFER_BUILD'
     or v_job.module_epoch<>v_epoch
     or v_job.candidate_id<>v_worker.candidate_id
     or v_transfer.candidate_id<>v_worker.candidate_id
     or not ((v_worker.target_pay_channel='PAYE' and
          v_transfer.beneficiary_kind='CANDIDATE' and v_transfer.beneficiary_id=v_worker.candidate_id)
       or (v_worker.target_pay_channel='UMBRELLA' and v_worker.target_umbrella_id is not null and
          v_transfer.beneficiary_kind='UMBRELLA' and v_transfer.beneficiary_id=v_worker.target_umbrella_id))
     or v_run.id<>v_worker.run_id
     or v_projection.run_worker_id<>v_worker.id then
    raise exception using errcode='23514',
      message='BPAY_NEXT_TRANSFER_JOB_SCOPE_INVALID';
  end if;
  if v_job.status='DONE' and v_job.phase='MEMBERS_READY'
     and v_transfer.status<>'BUILDING' then
    return pg_catalog.jsonb_build_object('phase','MEMBERS_READY',
      'transfer_status',v_transfer.status,
      'transfer_id',v_transfer.id,'cursor',v_job.cursor_key,
      'member_count',v_transfer.member_count,'replay',true);
  end if;
  if v_job.status<>'LEASED' or v_job.phase<>'WORK'
     or v_job.lease_nonce<>p_lease_nonce
     or v_job.owner_epoch<>p_owner_epoch
     or v_job.lease_until_utc<=pg_catalog.clock_timestamp()
     or v_run.status<>'DRAFT' or v_run.confirmed_at_utc is null
     or v_worker.status<>'READY'
     or v_worker.net_projection_revision<1 or v_worker.net_replacement_pending_id is not null
     or (v_worker.target_pay_channel='PAYE' and v_worker.net_request_revision<>v_worker.net_projection_revision)
     or v_projection.projection_no<>v_worker.net_projection_revision or v_projection.retired_at_utc is not null
     or v_projection.cash_amount<>v_transfer.cash_amount
     or v_transfer.status<>'BUILDING'
     or v_transfer.account_approval_ref is not null
     or v_transfer.original_transfer_id is not null
     or v_transfer.member_count>v_worker.captured_line_count then
    raise exception using errcode='55000',
      message='BPAY_NEXT_TRANSFER_BUILD_NOT_ELIGIBLE';
  end if;
  if v_job.cursor_key is distinct from p_expected_cursor::text then
    return pg_catalog.jsonb_build_object('phase','BUILDING',
      'transfer_id',v_transfer.id,'cursor',v_job.cursor_key,
      'member_count',v_transfer.member_count,'replay',true);
  end if;
  v_member_count:=v_transfer.member_count;
  v_cash_sum:=v_transfer.member_cash_sum;
  v_cursor:=p_expected_cursor;
  for v_line in
    select l.id,l.line_no,l.frozen_inc_vat,l.frozen_ex_vat,l.source_consumed_ex_vat,
      l.source_pay_channel,l.target_pay_channel,l.component_key,l.work_id,
      a.id as qualified_line_id,a.component_kind,a.tax_treatment,
      r.id as qualified_revision_id
    from private.bpay_next_run_line l
    left join private.bpay_next_approved_line a on a.id=l.approved_line_id
      and a.revision_id=l.captured_revision_id and a.component_key=l.component_key
    left join private.bpay_next_work_revision r on r.id=l.captured_revision_id and r.work_id=l.work_id
    where l.run_worker_id=v_worker.id
      and (v_cursor is null or l.line_no>v_cursor)
    order by l.line_no,l.id limit p_limit
  loop
    if v_worker.target_pay_channel='UMBRELLA' and
       (v_projection.input_kind is distinct from 'UMBRELLA'
        or v_worker.case_selection_revision<>0 or v_worker.net_request_revision<>0
        or v_projection.entered_paye_net is not null or v_projection.request_command_id is not null
        or v_projection.cash_amount is distinct from v_worker.gross_inc_vat
        or v_line.qualified_line_id is null or v_line.qualified_revision_id is null
        or v_line.component_kind is distinct from 'WORK' or v_line.tax_treatment is distinct from 'TAXABLE'
        or v_line.source_pay_channel is distinct from 'UMBRELLA'
        or v_line.target_pay_channel is distinct from 'UMBRELLA'
        or v_line.source_consumed_ex_vat<0
        or v_line.frozen_ex_vat is distinct from v_line.source_consumed_ex_vat) then
      raise exception using errcode='55000',message='BPAY_NEXT_UMBRELLA_WORK_EXECUTION_UNSUPPORTED';
    end if;
    v_seen:=v_seen+1;
    v_member_count:=v_member_count+1;
    v_cash_sum:=v_cash_sum+v_line.frozen_inc_vat;
    insert into private.bpay_next_transfer_member
      (transfer_id,run_worker_id,member_no,subject_kind,
       run_line_id,signed_cash_contribution)
      values(v_transfer.id,v_worker.id,v_member_count,'WORK',
             v_line.id,v_line.frozen_inc_vat);
    v_cursor:=v_line.line_no;
  end loop;
  if v_member_count>v_worker.captured_line_count then
    raise exception using errcode='23514',
      message='BPAY_NEXT_TRANSFER_LINE_COUNT_EXCEEDED';
  end if;
  if v_member_count=v_worker.captured_line_count then
    if exists(select 1 from private.bpay_next_run_line l
              where l.run_worker_id=v_worker.id
                and (v_cursor is null or l.line_no>v_cursor))
       or v_cash_sum<>v_worker.gross_inc_vat then
      raise exception using errcode='23514',
        message='BPAY_NEXT_TRANSFER_FROZEN_GROSS_MISMATCH';
    end if;
    if v_worker.target_pay_channel='PAYE' then
      v_adjustment:=v_projection.cash_amount-v_worker.gross_inc_vat;
      insert into private.bpay_next_transfer_member
        (transfer_id,run_worker_id,member_no,subject_kind,
         signed_cash_contribution)
        values(v_transfer.id,v_worker.id,v_member_count+1,
               'NET_ADJUSTMENT',v_adjustment);
      v_member_count:=v_member_count+1;
      v_cash_sum:=v_cash_sum+v_adjustment;
    end if;
    if v_cash_sum<>v_projection.cash_amount then
      raise exception using errcode='23514',
        message='BPAY_NEXT_TRANSFER_CASH_MISMATCH';
    end if;
    if v_worker.target_pay_channel='UMBRELLA' then
      if v_projection.payment_case_target_inc_vat<>0
         or v_projection.deduction_case_target_inc_vat<>0 then
        raise exception using errcode='55000',message='BPAY_NEXT_UMBRELLA_WORK_EXECUTION_UNSUPPORTED';
      end if;
      update private.bpay_next_transfer
        set umbrella_bridge_kind=v_projection.input_kind,
            umbrella_gross_ex_vat=v_projection.gross_ex_vat,
            umbrella_gross_vat=v_projection.gross_vat,
            umbrella_gross_inc_vat=v_projection.gross_inc_vat,
            umbrella_payment_case_target_inc_vat=v_projection.payment_case_target_inc_vat,
            umbrella_deduction_case_target_inc_vat=v_projection.deduction_case_target_inc_vat
        where id=v_transfer.id;
    end if;
    update private.bpay_next_transfer
      set member_count=v_member_count,member_cash_sum=v_cash_sum,
          status='MEMBERS_READY'
      where id=v_transfer.id;
    update private.bpay_next_job
      set status='DONE',phase='MEMBERS_READY',
          cursor_key=v_cursor::text,
          lease_nonce=null,lease_until_utc=null
      where id=p_job_id;
    update private.bpay_next_command set status='COMPLETE'
      where id=v_job.command_id;
    return pg_catalog.jsonb_build_object('phase','MEMBERS_READY',
      'transfer_id',v_transfer.id,'cursor',v_cursor,
      'member_count',v_member_count,'rows_visited',v_seen,'replay',false);
  end if;
  if v_seen=0 then
    raise exception using errcode='23514',
      message='BPAY_NEXT_TRANSFER_FROZEN_LINE_MISSING';
  end if;
  update private.bpay_next_transfer
    set member_count=v_member_count,member_cash_sum=v_cash_sum
    where id=v_transfer.id;
  update private.bpay_next_job set cursor_key=v_cursor::text
    where id=p_job_id;
  return pg_catalog.jsonb_build_object('phase','BUILDING',
    'transfer_id',v_transfer.id,'cursor',v_cursor,
    'member_count',v_member_count,'rows_visited',v_seen,'replay',false);
end
$function$;

create or replace function private.bpay_next_begin_case_transfer_v1(p_command_id uuid,p_run_worker_id uuid)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private,public
as $function$
declare
  v_epoch bigint;v_run_id uuid;v_run private.bpay_next_pay_run%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;v_transfer private.bpay_next_transfer%rowtype;
  v_projection private.bpay_next_net_projection%rowtype;
  v_draft record;v_net private.bpay_next_case_allocation_state%rowtype;
  v_destination private.bpay_next_net_destination_state%rowtype;v_cash numeric;
  v_sequence bigint;
begin
  if p_command_id is null or p_run_worker_id is null then
    raise exception using errcode='22023',message='BPAY_NEXT_CASE_TRANSFER_BEGIN_INPUT_INVALID';end if;
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(p_command_id::text,0));
  select run_id into strict v_run_id from private.bpay_next_run_worker where id=p_run_worker_id;
  select dr37_point_140.* into strict v_run from private.bpay_next_pay_run dr37_point_140 where id=v_run_id for update;
  perform 1 from private.bpay_next_worker_control c join private.bpay_next_run_worker w on w.candidate_id=c.candidate_id
    where w.id=p_run_worker_id for update of c;
  select dr37_point_141.* into strict v_worker from private.bpay_next_run_worker dr37_point_141 where id=p_run_worker_id for update;
  select dr37_point_142.* into v_transfer from private.bpay_next_transfer dr37_point_142 where build_command_id=p_command_id;
  if found then
    if v_transfer.run_worker_id<>v_worker.id or v_transfer.candidate_id<>v_worker.candidate_id
       or (not exists(select 1 from private.bpay_next_case_transfer_build s where s.transfer_id=v_transfer.id)
         and not exists(select 1 from private.bpay_next_destination_group s where s.anchor_transfer_id=v_transfer.id)) then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_TRANSFER_BEGIN_REPLAY_CONFLICT';end if;
    select agency_sequence into strict v_sequence from private.bpay_next_command
      where id=p_command_id and command_kind='TRANSFER_BUILD' and module_epoch=v_epoch;
    return pg_catalog.jsonb_build_object('sequence',v_sequence::text,'transfer_id',v_transfer.id,
      'phase',v_transfer.status,'replay',true);
  end if;
  if v_run.status<>'DRAFT' or v_run.confirmed_at_utc is null or v_worker.status<>'READY'
     or v_worker.target_pay_channel<>'PAYE' or v_worker.case_selection_revision<1
     or v_worker.case_pending_binding_count<>0 or v_worker.realised_effect_count<>0
     or v_worker.net_projection_revision<1 or v_worker.net_replacement_pending_id is not null or v_worker.net_request_revision<>v_worker.net_projection_revision
     or exists(select 1 from private.bpay_next_cancel_request r where r.run_worker_id=v_worker.id
       and r.status in ('REQUESTED','CANCELLING'))
     or exists(select 1 from private.bpay_next_transfer t where t.run_worker_id=v_worker.id) then
    raise exception using errcode='55000',message='BPAY_NEXT_CASE_TRANSFER_NOT_ELIGIBLE';end if;
  select dr37_point_143.* into strict v_projection from private.bpay_next_net_projection dr37_point_143
    where run_worker_id=v_worker.id and projection_no=v_worker.net_projection_revision and retired_at_utc is null;
  select id,processed_instruction_count,expected_instruction_count,preparation_revision,selection_revision into strict v_draft from private.bpay_next_case_allocation_state where run_worker_id=v_worker.id
    and preparation_revision=v_worker.captured_preparation_revision and selection_revision=v_worker.case_selection_revision
    and pass_kind='DRAFT' and projection_no=0 and status='COMPLETE';
  select dr37_point_144.* into strict v_net from private.bpay_next_case_allocation_state dr37_point_144 where projection_id=v_projection.id
    and run_worker_id=v_worker.id and pass_kind='NET' and status='COMPLETE' and net_stage='COMPLETE';
  if v_draft.processed_instruction_count<>v_worker.case_instruction_count
     or v_net.expected_instruction_count<>v_draft.expected_instruction_count
     or v_net.processed_instruction_count<>v_draft.processed_instruction_count
     or v_net.preparation_revision<>v_draft.preparation_revision or v_net.selection_revision<>v_draft.selection_revision
     or v_projection.input_kind not in ('PAYE_MANUAL','CASE_PAYOUT')
     or (v_projection.gross_ex_vat,v_projection.gross_vat,v_projection.gross_inc_vat,v_projection.entered_paye_net)
       is distinct from (v_worker.gross_ex_vat,v_worker.gross_vat,v_worker.gross_inc_vat,v_worker.entered_paye_net)
     or v_projection.cash_amount<0 or v_projection.request_command_id is null
     or not exists(select 1 from private.bpay_next_job j where j.id=v_net.job_id and j.status='DONE' and j.phase='PROJECTED') then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_TRANSFER_PROJECTION_INVALID';end if;
  -- Zero cash is an internal financial envelope, never a bank instruction.
  select dr37_point_145.* into v_destination from private.bpay_next_net_destination_state dr37_point_145 where projection_id=v_projection.id;
  v_cash:=v_projection.cash_amount;
  if found then
    if v_destination.state_id<>v_net.id or v_destination.run_worker_id<>v_worker.id
       or v_destination.completed_at_utc is null or v_destination.cash_amount<>v_projection.cash_amount
       or v_destination.external_leg_count<1 or v_destination.external_amount<=0
       or v_destination.own_amount+v_destination.external_amount<>v_projection.cash_amount then
      raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_TRANSFER_PROJECTION_INVALID';end if;
    v_cash:=v_destination.own_amount;
  end if;
  v_sequence:=private.bpay_next_receive_command_v1(p_command_id,'TRANSFER_BUILD');
  insert into private.bpay_next_command_member(command_id,candidate_id,member_no) values(p_command_id,v_worker.candidate_id,1);
  insert into private.bpay_next_transfer(run_worker_id,candidate_id,projection_id,build_command_id,transfer_no,
    beneficiary_kind,beneficiary_id,cash_amount,status,execution_kind)
    values(v_worker.id,v_worker.candidate_id,v_projection.id,p_command_id,1,'CANDIDATE',v_worker.candidate_id,
      v_cash,'BUILDING',case when v_cash=0 then 'INTERNAL_ZERO' else 'BANK' end)
    returning * into v_transfer;
  if v_destination.state_id is not null then
    insert into private.bpay_next_destination_group(anchor_transfer_id,run_worker_id,candidate_id,projection_id,draft_state_id,
      net_state_id,preparation_revision,selection_revision,stage,expected_work_count,expected_case_count,expected_leg_count)
      values(v_transfer.id,v_worker.id,v_worker.candidate_id,v_projection.id,v_draft.id,v_net.id,
        v_worker.captured_preparation_revision,v_worker.case_selection_revision,'WORK',v_worker.captured_line_count,
        v_draft.expected_instruction_count,v_destination.external_leg_count+1);
    insert into private.bpay_next_destination_group_leg(transfer_id,anchor_transfer_id,leg_kind) values(v_transfer.id,v_transfer.id,'OWN');
  else
    insert into private.bpay_next_case_transfer_build(transfer_id,run_worker_id,candidate_id,projection_id,draft_state_id,
      net_state_id,preparation_revision,selection_revision,stage,expected_work_count,expected_case_count)
      values(v_transfer.id,v_worker.id,v_worker.candidate_id,v_projection.id,v_draft.id,v_net.id,
        v_worker.captured_preparation_revision,v_worker.case_selection_revision,'WORK',v_worker.captured_line_count,v_draft.expected_instruction_count);
  end if;
  update private.bpay_next_command set expected_member_count=1,status='SEALED',sealed_at_utc=pg_catalog.transaction_timestamp()
    where id=p_command_id;
  return pg_catalog.jsonb_build_object('sequence',v_sequence::text,'transfer_id',v_transfer.id,'phase','BUILDING','replay',false);
end
$function$;

create or replace function private.bpay_next_original_reservation_v1(p_worker_id uuid,p_transfer_id uuid)
returns table(reservation_mode text,reservation_projection_id uuid,remaining_reservation_cash text,
  reservation_internal_zero boolean,reservation_state text,reservation_holds_state text)
language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare
  w record;r record;
  g record;t record;
  o record;p record;
  n record;l record;
  ns record;ds record;
  v_cash numeric;v_active boolean:=false;v_cancel boolean:=false;v_posted boolean:=false;
  v_receipt boolean:=false;v_return boolean:=false;v_internal boolean:=false;
begin
  -- Non-Case branches still qualify holds against an explicitly empty Case scope.
  select null::uuid as id,null::text as status,null::text as net_stage,
    null::bigint as projection_no,null::bigint as preparation_revision,null::bigint as selection_revision into ns;
  ds:=ns;
  select id,run_id,candidate_id,status,target_pay_channel,case_selection_revision,net_request_revision,net_projection_revision,net_replacement_pending_id,entered_paye_net,target_umbrella_id,target_umbrella_vat_chargeable,gross_inc_vat,gross_ex_vat,gross_vat,captured_line_count,financial_resolution_count,expected_managed_position_count,preparation_revision into strict w from private.bpay_next_run_worker where id=p_worker_id;
  select id,status,confirmed_at_utc into strict r from private.bpay_next_pay_run where id=w.run_id;
  reservation_mode:='EVIDENCE_CONFLICT';reservation_state:='EVIDENCE_CONFLICT';
  reservation_internal_zero:=false;reservation_holds_state:='NONE';
  select anchor_transfer_id,run_worker_id,candidate_id,projection_id,net_state_id,preparation_revision,selection_revision,expected_leg_count,stage,completed_at_utc,created_leg_count,sealed_leg_count,sealed_cash_total into g from private.bpay_next_destination_group where run_worker_id=w.id;
  if g.anchor_transfer_id is not null then
    select id,candidate_id,original_transfer_id,return_cash_id,projection_id,execution_kind,status,cash_amount into o from private.bpay_next_transfer where id=g.anchor_transfer_id and run_worker_id=w.id;
  else
    -- The original is the native first instruction, never a newest reissue.
    select id,candidate_id,original_transfer_id,return_cash_id,projection_id,execution_kind,status,cash_amount into o from private.bpay_next_transfer where run_worker_id=w.id and transfer_no=1
      and original_transfer_id is null and return_cash_id is null;
  end if;
  if p_transfer_id is not null then
    select id,candidate_id,original_transfer_id,return_cash_id,projection_id,execution_kind,status,cash_amount into strict t from private.bpay_next_transfer where id=p_transfer_id and run_worker_id=w.id;
    if t.candidate_id<>w.candidate_id then return next;return;end if;
    if t.original_transfer_id is not null then
      reservation_mode:='ORIGINAL_LEGS';reservation_state:='REISSUE_EXCLUDED';
      remaining_reservation_cash:='0.00';return next;return;
    end if;
  end if;
  if o.id is not null then
    select id,run_worker_id,projection_no,cash_amount,input_kind,gross_ex_vat,gross_vat,gross_inc_vat,entered_paye_net,accepted_recoveries,request_command_id into p from private.bpay_next_net_projection where id=o.projection_id and run_worker_id=w.id;
    if p.id is null or o.candidate_id<>w.candidate_id or o.original_transfer_id is not null
      or (g.anchor_transfer_id is not null and (g.projection_id<>p.id or g.candidate_id<>w.candidate_id)) then
      return next;return;end if;
  else
    select np.id,np.run_worker_id,np.projection_no,np.cash_amount,np.input_kind,np.gross_ex_vat,np.gross_vat,np.gross_inc_vat,np.entered_paye_net,np.accepted_recoveries,np.request_command_id into p from private.bpay_next_case_cancel_binding b
      join private.bpay_next_cancel_request cr on cr.command_id=b.command_id
      join private.bpay_next_net_projection np on np.id=b.projection_id and np.run_worker_id=b.run_worker_id
      where b.run_worker_id=w.id and b.candidate_id=w.candidate_id and b.status=w.status
        and cr.status=b.status and b.status in ('CANCELLING','CANCELLED');
    if not found then
      select id,run_worker_id,projection_no,cash_amount,input_kind,gross_ex_vat,gross_vat,gross_inc_vat,entered_paye_net,accepted_recoveries,request_command_id into p from private.bpay_next_net_projection where run_worker_id=w.id
        and projection_no=w.net_projection_revision and retired_at_utc is null;
    end if;
  end if;
  reservation_projection_id:=p.id;
  -- CANCEL_REQUEST checking is not the cancellation fence. The native request
  -- and, for Case/group, exact captured binding must already be CANCELLING.
  select exists(select 1 from private.bpay_next_cancel_request cr
    where cr.run_worker_id=w.id and cr.candidate_id=w.candidate_id
      and cr.status in ('CANCELLING','CANCELLED') and w.status=cr.status
      and (w.case_selection_revision=0 or exists(
        select 1 from private.bpay_next_case_cancel_binding b
        where b.command_id=cr.command_id and b.run_worker_id=w.id and b.candidate_id=w.candidate_id
          and b.status=cr.status and b.projection_id is not distinct from p.id
          and b.transfer_id is not distinct from o.id
          and (g.anchor_transfer_id is null or (b.net_state_id=g.net_state_id
            and b.preparation_revision=g.preparation_revision and b.selection_revision=g.selection_revision
            and b.accepted_projection_revision=p.projection_no and exists(
              select 1 from private.bpay_next_destination_cancel dc where dc.command_id=b.command_id
                and dc.anchor_transfer_id=g.anchor_transfer_id and dc.expected_leg_count=g.expected_leg_count
                and dc.checked_leg_count=dc.expected_leg_count)))))
  ) into v_cancel;
  if v_cancel then
    reservation_mode:=w.status;reservation_state:=w.status;remaining_reservation_cash:='0.00';
    reservation_holds_state:=case when w.status='CANCELLING' then 'RELEASING' else 'NONE' end;
    return next;return;
  end if;
  if w.status in ('CANCELLING','CANCELLED') then return next;return;end if;
  if r.confirmed_at_utc is null then
    reservation_mode:='UNCONFIRMED';reservation_state:='SUPPRESSED_UNCONFIRMED';return next;return;
  end if;
  if o.id is null and w.status<>'READY' then
    reservation_mode:='PENDING_PREPARATION';reservation_state:='PENDING';return next;return;
  end if;
  if o.id is null and w.target_pay_channel='PAYE' and
    (p.id is null or w.net_replacement_pending_id is not null or w.net_request_revision<>w.net_projection_revision) then
    reservation_mode:='PENDING_NET';reservation_state:='PENDING';return next;return;
  end if;
  if p.id is null then
    -- Ordinary Umbrella has no projection until BEGIN, and then request0/projection1.
    if o.id is null and w.target_pay_channel='UMBRELLA' and r.status='DRAFT'
      and w.case_selection_revision=0 and w.net_request_revision=0 and w.net_projection_revision=0
      and w.entered_paye_net is null and w.target_umbrella_id is not null
      and w.target_umbrella_vat_chargeable is not null and w.gross_inc_vat>0
      and w.captured_line_count>0 and w.financial_resolution_count=0 and w.expected_managed_position_count=0
      and not exists(select 1 from private.bpay_next_paye_net_request q where q.run_worker_id=w.id) then
      v_cash:=w.gross_inc_vat;
    else return next;return;end if;
  else
    v_cash:=p.cash_amount;
    if (w.target_pay_channel='UMBRELLA' and (p.input_kind<>'UMBRELLA' or w.case_selection_revision<>0))
      or (w.target_pay_channel='PAYE' and p.input_kind not in ('PAYE_MANUAL','PAYE_IMPORT','CASE_PAYOUT')) then
      return next;return;end if;
    if o.id is null and (p.gross_ex_vat,p.gross_vat,p.gross_inc_vat,p.entered_paye_net)
      is distinct from (w.gross_ex_vat,w.gross_vat,w.gross_inc_vat,w.entered_paye_net) then return next;return;end if;
    if w.case_selection_revision>0 then
      select id,status,net_stage,projection_no,preparation_revision,selection_revision into ns from private.bpay_next_case_allocation_state where projection_id=p.id
        and run_worker_id=w.id and pass_kind='NET';
      select id,status,net_stage,projection_no,preparation_revision,selection_revision into ds from private.bpay_next_case_allocation_state where run_worker_id=w.id
        and preparation_revision=coalesce(g.preparation_revision,ns.preparation_revision)
        and selection_revision=coalesce(g.selection_revision,ns.selection_revision)
        and pass_kind='DRAFT' and projection_no=0;
      if ns.id is null or ds.id is null or ns.status not in ('COMPLETE','CANCELLED')
        or ns.net_stage<>'COMPLETE' or ns.projection_no<>p.projection_no
        or ns.preparation_revision<>ds.preparation_revision or ns.selection_revision<>ds.selection_revision
        or ds.status not in ('COMPLETE','CANCELLED') then return next;return;end if;
    end if;
    select state_id,run_worker_id,completed_at_utc,cash_amount,external_leg_count,external_amount,own_amount into n from private.bpay_next_net_destination_state where projection_id=p.id;
    if n.state_id is not null and (n.run_worker_id<>w.id or n.state_id is distinct from ns.id
      or n.completed_at_utc is null or n.cash_amount<>p.cash_amount or n.external_leg_count<1
      or n.external_amount<=0 or n.own_amount+n.external_amount<>p.cash_amount) then return next;return;end if;
  end if;
  if g.anchor_transfer_id is not null then
    if n.state_id is null or g.net_state_id<>n.state_id or g.expected_leg_count<>n.external_leg_count+1 then
      return next;return;end if;
    if g.stage='COMPLETE' and (g.completed_at_utc is null or g.created_leg_count<>g.expected_leg_count
      or g.sealed_leg_count<>g.expected_leg_count or g.sealed_cash_total<>p.cash_amount) then return next;return;end if;
  end if;
  if o.id is null or (g.anchor_transfer_id is not null and g.stage<>'COMPLETE')
    or (g.anchor_transfer_id is null and o.status='BUILDING') then
    reservation_mode:='ORIGINAL_OBLIGATION';reservation_state:='INCLUDED_IN_ORIGINAL_OBLIGATION';
    reservation_internal_zero:=v_cash=0;
    select exists(select 1 from private.bpay_next_run_line rl
      join private.bpay_next_hold h on h.run_line_id=rl.id
      where rl.run_worker_id=w.id and h.status='ACTIVE'
        and (h.work_id,h.component_key,h.source_reserved_ex_vat,h.target_amount_ex_vat,h.target_amount_vat,h.target_amount_inc_vat)
          is not distinct from (rl.work_id,rl.component_key,rl.source_consumed_ex_vat,rl.frozen_ex_vat,rl.frozen_vat,rl.frozen_inc_vat))
      or exists(select 1 from private.bpay_next_case_hold h
        join private.bpay_next_case_allocation_result ar on ar.id=h.allocation_result_id
        where h.run_worker_id=w.id and h.candidate_id=w.candidate_id and h.status='ACTIVE'
          and ar.state_id in (ds.id,ns.id)) into v_active;
    reservation_holds_state:=case when v_active then 'ACTIVE' else 'NONE' end;
    if v_cash>0 and not v_active then
      reservation_mode:='EVIDENCE_CONFLICT';reservation_state:='EVIDENCE_CONFLICT';
      reservation_internal_zero:=false;return next;return;
    end if;
    if p_transfer_id is null then remaining_reservation_cash:=v_cash::numeric(18,2)::text;end if;
    return next;return;
  end if;
  reservation_mode:='ORIGINAL_LEGS';
  if p_transfer_id is null then reservation_state:='PAGED_ORIGINAL_LEGS';return next;return;end if;
  reservation_internal_zero:=t.execution_kind='INTERNAL_ZERO';
  if t.projection_id is distinct from p.id then reservation_mode:='EVIDENCE_CONFLICT';return next;return;end if;
  if g.anchor_transfer_id is not null then
    select transfer_id,leg_kind,destination_id into l from private.bpay_next_destination_group_leg where transfer_id=t.id and anchor_transfer_id=g.anchor_transfer_id;
    if l.transfer_id is null then reservation_mode:='EVIDENCE_CONFLICT';return next;return;end if;
    if l.leg_kind='OWN' and l.destination_id is null and t.id=g.anchor_transfer_id then v_cash:=n.own_amount;
    elsif l.leg_kind='ONEOFF' then
      select amount into v_cash from private.bpay_next_net_destination_amount where destination_id=l.destination_id and state_id=n.state_id;
    else reservation_mode:='EVIDENCE_CONFLICT';return next;return;end if;
  end if;
  if v_cash is null or t.cash_amount<>v_cash or (t.execution_kind='INTERNAL_ZERO')<>(v_cash=0) then
    reservation_mode:='EVIDENCE_CONFLICT';return next;return;end if;
  select exists(select 1 from private.bpay_next_transfer_outcome x
    join private.bpay_next_outcome_request q on q.outcome_id=x.id and q.transfer_id=x.transfer_id
    where x.transfer_id=t.id and x.outcome_kind='SETTLED' and x.whole_transfer_amount=t.cash_amount
      and q.run_worker_id=w.id and q.candidate_id=w.candidate_id),
    exists(select 1 from private.bpay_next_transfer_outcome x
    join private.bpay_next_return_request q on q.outcome_id=x.id and q.transfer_id=x.transfer_id
    where x.transfer_id=t.id and x.outcome_kind='RETURNED' and x.whole_transfer_amount=t.cash_amount
      and q.run_worker_id=w.id and q.candidate_id=w.candidate_id),
    exists(select 1 from private.bpay_next_internal_receipt x
    join private.bpay_next_outcome_request q on q.internal_receipt_id=x.id and q.transfer_id=x.transfer_id
    where x.transfer_id=t.id and x.run_worker_id=w.id and x.candidate_id=w.candidate_id
      and x.projection_id=p.id and x.cash_amount=0 and q.run_worker_id=w.id and q.candidate_id=w.candidate_id),
    exists(select 1 from private.bpay_next_outcome_request q where q.transfer_id=t.id
      and q.run_worker_id=w.id and q.candidate_id=w.candidate_id and q.posting_complete)
    into v_receipt,v_return,v_internal,v_posted;
  if v_return and v_receipt and t.status='RETURNED' then
    reservation_state:='RETURNED';remaining_reservation_cash:='0.00';
  elsif v_receipt and t.status='SETTLED' then
    reservation_state:=case when v_posted then 'POSTED' else 'EXECUTED_PENDING_POSTING' end;
    remaining_reservation_cash:='0.00';reservation_holds_state:=case when v_posted then 'NONE' else 'POSTING' end;
  elsif v_internal and reservation_internal_zero and t.status in ('INTERNAL_PROCESSING','INTERNAL_SETTLED') then
    reservation_state:=case when v_posted then 'CLOSED_INTERNAL' else 'INTERNAL_PENDING_POSTING' end;
    remaining_reservation_cash:='0.00';reservation_holds_state:=case when v_posted then 'NONE' else 'POSTING' end;
  elsif t.status in ('SUBMITTED','UNKNOWN') and not(v_receipt or v_return or v_internal) then
    reservation_state:='EXECUTION_UNCERTAIN';
  elsif t.status in ('MEMBERS_READY','DRAFT','SCHEDULED','REFUSED','ISSUED_CSV') and not(v_receipt or v_return or v_internal) then
    select exists(select 1 from private.bpay_next_transfer_member m
      join private.bpay_next_hold h on h.run_line_id=m.run_line_id
      where m.transfer_id=t.id and m.subject_kind='WORK' and h.status='ACTIVE')
      or exists(select 1 from private.bpay_next_transfer_member m
        join private.bpay_next_case_hold h on h.id=m.case_hold_id
        where m.transfer_id=t.id and m.subject_kind='CASE' and h.status='ACTIVE'
          and h.run_worker_id=w.id and h.candidate_id=w.candidate_id) into v_active;
    reservation_holds_state:=case when v_active then 'ACTIVE' else 'NONE' end;
    if t.cash_amount>0 and not v_active then
      reservation_state:='EVIDENCE_CONFLICT';return next;return;
    end if;
    reservation_state:=case when reservation_internal_zero then 'INTERNAL_RESERVED'
      when t.status='ISSUED_CSV' then 'ISSUED_AWAITING_RECEIPT' else 'RESERVED' end;
    remaining_reservation_cash:=t.cash_amount::text;
  end if;
  return next;
end
$function$;


-- DR-37 header
create or replace function private.bpay_next_run_channel_contribution_v1(p_worker private.bpay_next_run_worker)
returns table(candidate_count bigint,ready_count bigint,gross_ex_vat numeric,gross_vat numeric,
 gross_inc_vat numeric,gross_stage_deductions numeric,net_recovery_reserved numeric,cash_known numeric,
 net_missing_count bigint,applicable_net_missing_count bigint,net_replacement_pending_count bigint,net_refused_code text,frozen_offer_complete boolean)
language sql stable security invoker set search_path=pg_catalog,private as $function$
 with points as (
  select (p_worker).*,d.completed_at_utc draft_completed_at,d.allocated_gross_deductions,d.recovered_target_total,
   p.id projection_id,p.accepted_recoveries,p.cash_amount,req.command_id request_id
  from (select 1) one
  left join private.bpay_next_case_allocation_state d on d.run_worker_id=p_worker.id
   and d.preparation_revision=p_worker.captured_preparation_revision and d.selection_revision=p_worker.case_selection_revision
   and d.pass_kind='DRAFT' and d.projection_no=0
  -- Retirement removes executable eligibility, never immutable accepted money.
  left join private.bpay_next_net_projection p on p.run_worker_id=p_worker.id and p.projection_no=p_worker.net_projection_revision
  left join private.bpay_next_paye_net_request req on req.run_worker_id=p_worker.id and req.request_no=p_worker.net_request_revision
 ), qualified as (
  select x.*,x.status<>'CANCELLED' participating,x.status in ('READY','DRAFT','ISSUED','COMPLETE') ready,
   (x.status in ('READY','DRAFT','ISSUED','COMPLETE','CANCELLING')
    or (x.status='REVIEW' and (x.projection_id is not null or x.draft_completed_at is not null))) monetary,
   x.target_pay_channel='PAYE' and x.status='READY' and (x.net_replacement_pending_id is not null or (x.request_id is not null and x.net_request_revision>x.net_projection_revision)) pending,
   case when x.target_pay_channel='PAYE' and ((x.status='READY' and x.net_replacement_pending_id is null
     and x.net_request_revision=x.net_projection_revision and x.net_request_high_water>x.net_projection_revision)
    or (x.status='REVIEW' and x.request_id is not null and x.net_request_revision>x.net_projection_revision))
    and x.review_issue_code in ('CASE_INPUT_UNBOUND','CASE_WEEK_BASIS_UNBOUND','CASE_RETURN_FLOOR_UNBOUND') then x.review_issue_code end refusal
  from points x
 ) select participating::integer::bigint,ready::integer::bigint,
  case when monetary then q.gross_ex_vat else 0 end,case when monetary then q.gross_vat else 0 end,
  case when monetary then q.gross_inc_vat else 0 end,
  case when monetary and draft_completed_at is not null then coalesce(allocated_gross_deductions,0) else 0 end,
  case when monetary then case when projection_id is not null then accepted_recoveries
   when draft_completed_at is not null then greatest(0,coalesce(recovered_target_total,0)-coalesce(allocated_gross_deductions,0)) else 0 end else 0 end,
  case when monetary then case when projection_id is not null then cash_amount
   when target_pay_channel='UMBRELLA' and case_instruction_count=0 then q.gross_inc_vat else 0 end else 0 end,
  (monetary and status<>'CANCELLING' and target_pay_channel='PAYE' and projection_id is null and (captured_line_count>0 or q.gross_inc_vat>0))::integer::bigint,
  (monetary and status<>'CANCELLING' and target_pay_channel='PAYE' and projection_id is null and (captured_line_count>0 or q.gross_inc_vat>0))::integer::bigint,
  (pending and net_projection_revision>0)::integer::bigint,refusal,
  coalesce(monetary and status<>'CANCELLING',false) from qualified q
$function$;

create or replace function private.bpay_next_channel_draft_summary_apply_v1(
 p_run_id uuid,p_pay_channel text,
 p_old_worker private.bpay_next_run_worker default null,
 p_new_worker private.bpay_next_run_worker default null,
 p_confirm_task_id uuid default null,p_touch boolean default false,
 p_status_group text default null,p_status_note text default null,
 p_execution_route text default null,p_execution_pay_date date default null,
 p_scheduled_at_utc timestamptz default null,p_execution_reference text default null,
 p_has_payment_issue boolean default null
) returns void language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare
 v_old record;v_new record;v_before private.bpay_next_run_channel_totals%rowtype;
 v_run private.bpay_next_pay_run%rowtype;v_code text;v_timesheets bigint;
 v_old_here boolean;v_new_here boolean;v_native_changed boolean;
begin
 if p_run_id is null or p_pay_channel is null or p_pay_channel not in ('PAYE','UMBRELLA')
  or p_touch is null
  or ((p_old_worker).id is not null and (p_old_worker).run_id is distinct from p_run_id)
  or ((p_new_worker).id is not null and (p_new_worker).run_id is distinct from p_run_id)
  or ((p_old_worker).id is not null and (p_new_worker).id is not null
    and ((p_old_worker).id,(p_old_worker).candidate_id) is distinct from ((p_new_worker).id,(p_new_worker).candidate_id))
  or (p_status_group is not null and p_status_group not in ('NOT_PAID','WAITING_APPROVAL','PROCESSING','PARTLY_PAID','BANK_UNAVAILABLE','PAID','CANCELLED'))
  or (p_execution_route is not null and (p_execution_route not in ('BANK_CSV','EXTERNAL','SCHEDULED')
    or p_execution_pay_date is null or not isfinite(p_execution_pay_date)
    or (p_execution_route='SCHEDULED') is distinct from (p_scheduled_at_utc is not null)))
  or (p_execution_route is null and (p_execution_pay_date is not null or p_scheduled_at_utc is not null))
  or (p_scheduled_at_utc is not null and not isfinite(p_scheduled_at_utc)) then
  raise exception using errcode='22023',message='BPAY_NEXT_CHANNEL_DRAFT_SUMMARY_INPUT_INVALID';
 end if;
 select * into strict v_old from private.bpay_next_run_channel_contribution_v1(p_old_worker);
 select * into strict v_new from private.bpay_next_run_channel_contribution_v1(p_new_worker);
 v_old_here:=coalesce((p_old_worker).id is not null and (p_old_worker).target_pay_channel=p_pay_channel,false);
 v_new_here:=coalesce((p_new_worker).id is not null and (p_new_worker).target_pay_channel=p_pay_channel,false);
 v_native_changed:=v_old_here is distinct from v_new_here
  or (v_old_here and v_new_here and (v_old is distinct from v_new
   or ((p_old_worker).status,(p_old_worker).net_request_revision,(p_old_worker).net_projection_revision,(p_old_worker).review_issue_code,(p_old_worker).net_request_high_water,(p_old_worker).net_replacement_pending_id)
    is distinct from ((p_new_worker).status,(p_new_worker).net_request_revision,(p_new_worker).net_projection_revision,(p_new_worker).review_issue_code,(p_new_worker).net_request_high_water,(p_new_worker).net_replacement_pending_id)));
 if not v_native_changed and p_confirm_task_id is null and not p_touch and p_status_group is null
  and p_status_note is null and p_execution_route is null and p_execution_reference is null and p_has_payment_issue is null then return;end if;
 -- No directory or reverse business lock. Original owner holds Run/Candidate.
 select dr27_summary.* into strict v_before from private.bpay_next_run_channel_totals dr27_summary
  where dr27_summary.run_id=p_run_id and dr27_summary.pay_channel=p_pay_channel for update;
 if not v_native_changed and p_confirm_task_id is null and not p_touch
  and (coalesce(p_status_group,v_before.status_group),
   case when p_status_note is not null then nullif(p_status_note,'') else v_before.status_note end,
   coalesce(p_execution_route,v_before.execution_route),coalesce(p_execution_pay_date,v_before.execution_pay_date),
   case when p_execution_route is not null then p_scheduled_at_utc else v_before.scheduled_at_utc end,
   case when p_execution_reference is not null then nullif(p_execution_reference,'') else v_before.execution_reference end,
   coalesce(p_has_payment_issue,v_before.has_payment_issue))
   is not distinct from (v_before.status_group,v_before.status_note,v_before.execution_route,v_before.execution_pay_date,
    v_before.scheduled_at_utc,v_before.execution_reference,v_before.has_payment_issue) then return;end if;
 if v_native_changed then
  update private.bpay_next_run_channel_totals t set
   candidate_count=t.candidate_count+case when v_new_here then coalesce(v_new.candidate_count,0) else 0 end-case when v_old_here then coalesce(v_old.candidate_count,0) else 0 end,
   ready_count=t.ready_count+case when v_new_here then coalesce(v_new.ready_count,0) else 0 end-case when v_old_here then coalesce(v_old.ready_count,0) else 0 end,
   gross_ex_vat=t.gross_ex_vat+case when v_new_here then v_new.gross_ex_vat else 0 end-case when v_old_here then v_old.gross_ex_vat else 0 end,
   gross_vat=t.gross_vat+case when v_new_here then v_new.gross_vat else 0 end-case when v_old_here then v_old.gross_vat else 0 end,
   gross_inc_vat=t.gross_inc_vat+case when v_new_here then v_new.gross_inc_vat else 0 end-case when v_old_here then v_old.gross_inc_vat else 0 end,
   gross_stage_deductions=t.gross_stage_deductions+case when v_new_here then v_new.gross_stage_deductions else 0 end-case when v_old_here then v_old.gross_stage_deductions else 0 end,
   net_recovery_reserved=t.net_recovery_reserved+case when v_new_here then v_new.net_recovery_reserved else 0 end-case when v_old_here then v_old.net_recovery_reserved else 0 end,
   cash_known=t.cash_known+case when v_new_here then v_new.cash_known else 0 end-case when v_old_here then v_old.cash_known else 0 end,
   net_missing_count=t.net_missing_count+case when v_new_here then coalesce(v_new.net_missing_count,0) else 0 end-case when v_old_here then coalesce(v_old.net_missing_count,0) else 0 end,
   applicable_net_missing_count=t.applicable_net_missing_count+case when v_new_here then coalesce(v_new.applicable_net_missing_count,0) else 0 end-case when v_old_here then coalesce(v_old.applicable_net_missing_count,0) else 0 end,
   net_replacement_pending_count=t.net_replacement_pending_count+case when v_new_here then coalesce(v_new.net_replacement_pending_count,0) else 0 end-case when v_old_here then coalesce(v_old.net_replacement_pending_count,0) else 0 end,
   totals_revision=t.totals_revision+1,updated_at_utc=transaction_timestamp()
  where t.run_id=p_run_id and t.pay_channel=p_pay_channel;
  -- First of two current indexed points; never walk retained refusal history.
  select code into v_code from (
   (select w.review_issue_code code from private.bpay_next_run_worker w
    where w.run_id=p_run_id and w.target_pay_channel=p_pay_channel and w.status='READY'
     and w.net_replacement_pending_id is null and w.net_request_revision=w.net_projection_revision
     and w.review_issue_code in ('CASE_INPUT_UNBOUND','CASE_WEEK_BASIS_UNBOUND','CASE_RETURN_FLOOR_UNBOUND')
    order by w.review_issue_code collate "C",w.id limit 1)
   union all
   (select w.review_issue_code code from private.bpay_next_run_worker w
    where w.run_id=p_run_id and w.target_pay_channel=p_pay_channel and w.status='REVIEW'
     and w.net_request_revision>w.net_projection_revision and w.review_issue_code is not null
     and w.review_issue_code in ('CASE_INPUT_UNBOUND','CASE_WEEK_BASIS_UNBOUND','CASE_RETURN_FLOOR_UNBOUND')
    order by w.review_issue_code collate "C",w.id limit 1)
  ) current_codes order by code collate "C" limit 1;
  update private.bpay_next_run_channel_totals set net_refused_code=v_code
   where run_id=p_run_id and pay_channel=p_pay_channel;
 end if;
 if p_confirm_task_id is not null then
  if v_before.list_visible then raise exception using errcode='23514',message='BPAY_NEXT_CHANNEL_DRAFT_ALREADY_CAPTURED';end if;
  select dr27_run.* into strict v_run from private.bpay_next_pay_run dr27_run where dr27_run.id=p_run_id;
  if v_run.confirmed_at_utc is null or v_run.status<>'DRAFT' or v_run.selection_state<>'SEALED'
   or v_before.candidate_count<1 or v_before.ready_count<>v_before.candidate_count
   or v_old_here or v_new_here or not exists(select 1 from private.bpay_next_workbench_task
    where id=p_confirm_task_id and task_type='CONFIRM' and confirm_run_id=p_run_id and confirm_review_revision=v_run.review_revision) then
   raise exception using errcode='23514',message='BPAY_NEXT_CHANNEL_DRAFT_CONFIRM_UNBOUND';end if;
  select count(distinct work.original_timesheet_id) into v_timesheets
   from private.bpay_next_confirm_refresh_member m
   join private.bpay_next_run_work rw on rw.work_id=m.work_id
   join private.bpay_next_run_worker w on w.id=rw.run_worker_id and w.run_id=p_run_id and w.target_pay_channel=p_pay_channel
   join private.bpay_next_work work on work.id=rw.work_id and work.candidate_id=w.candidate_id
   where m.task_id=p_confirm_task_id and m.member_kind='WORK'
    and not exists(select 1 from private.bpay_next_worker_reprepare_request x
      where x.run_worker_id=w.id and x.run_id=p_run_id and x.action='RELEASE' and x.status='COMPLETE'
       and x.target_preparation_revision=w.preparation_revision and x.captured_preparation_revision=w.captured_preparation_revision
       and x.released_work_count=x.expected_work_count and x.released_case_hold_count=x.expected_case_hold_count);
  update private.bpay_next_run_channel_totals set list_visible=true,
   created_at_utc=v_run.created_at_utc,confirmed_at_utc=v_run.confirmed_at_utc,
   original_worker_count=v_before.candidate_count,original_timesheet_count=v_timesheets,
   original_gross_ex_vat=v_before.gross_ex_vat,original_gross_vat=v_before.gross_vat,original_gross_inc_vat=v_before.gross_inc_vat
   where run_id=p_run_id and pay_channel=p_pay_channel;
 end if;
 update private.bpay_next_run_channel_totals t set
  status_group=coalesce(p_status_group,case when t.list_visible and t.candidate_count=0 then 'CANCELLED' else t.status_group end),
  status_note=case when p_status_note is not null then nullif(p_status_note,'') else t.status_note end,
  execution_route=coalesce(p_execution_route,t.execution_route),execution_pay_date=coalesce(p_execution_pay_date,t.execution_pay_date),
  scheduled_at_utc=case when p_execution_route is not null then p_scheduled_at_utc else t.scheduled_at_utc end,
  execution_reference=case when p_execution_reference is not null then nullif(p_execution_reference,'') else t.execution_reference end,
  has_payment_issue=coalesce(p_has_payment_issue,t.has_payment_issue),
  change_revision=t.change_revision+1,updated_at_utc=transaction_timestamp()
 where t.run_id=p_run_id and t.pay_channel=p_pay_channel;
end
$function$;

drop trigger if exists bpay_next_run_channel_totals_v1 on private.bpay_next_run_worker;
create trigger bpay_next_run_channel_totals_v1 after insert or update of status,target_pay_channel,
 gross_ex_vat,gross_vat,gross_inc_vat,entered_paye_net,net_request_revision,net_projection_revision,
 net_request_high_water,net_replacement_pending_id,
 review_issue_code,captured_line_count,case_instruction_count,case_selection_revision,captured_preparation_revision
 on private.bpay_next_run_worker for each row execute function private.bpay_next_run_channel_totals_trigger_v1();


-- DR-37 reader-generation
create or replace function private.bpay_next_net_replacement_reader_revision_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
begin
 if tg_op='UPDATE' and pg_catalog.to_jsonb(new) is not distinct from pg_catalog.to_jsonb(old) then return new;end if;
 if exists(select 1 from private.bpay_next_run_worker w where w.id=new.run_worker_id
  and w.candidate_id=new.candidate_id and (w.net_replacement_pending_id=new.id or w.net_request_high_water=new.request_no)) then
  -- Existing observational reader clock; no money, business lock or recount.
  update private.bpay_next_run_channel_totals set totals_revision=totals_revision+1,updated_at_utc=pg_catalog.transaction_timestamp()
   where run_id=new.run_id and pay_channel='PAYE';
  if not found then raise exception using errcode='23514',message='BPAY_NEXT_REVIEW_TOTALS_NOT_INITIALISED';end if;
  perform private.bpay_next_channel_draft_summary_apply_v1(p_run_id=>new.run_id,p_pay_channel=>'PAYE',p_touch=>true);
 end if;
 return new;
end $function$;
drop trigger if exists bpay_next_net_replacement_reader_revision on private.bpay_next_net_replacement;
create trigger bpay_next_net_replacement_reader_revision after insert or update on private.bpay_next_net_replacement
 for each row execute function private.bpay_next_net_replacement_reader_revision_v1();

create or replace function private.bpay_next_ready_net_refusal_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
begin
 if tg_op='UPDATE' and old.status='READY' and new.status<>'READY'
   and old.review_issue_code in ('CASE_INPUT_UNBOUND','CASE_WEEK_BASIS_UNBOUND','CASE_RETURN_FLOOR_UNBOUND')
   and new.review_issue_code=old.review_issue_code and new.review_issue_work_id is null then
  -- Native cancellation/completion owns the status change. The READY cache ends
  -- with that status; the immutable refusal receipt remains historical.
  new.review_issue_code:=null;new.review_issue_work_id:=null;
 end if;
 if new.status='READY' and new.review_issue_code is not null and not exists(
  select 1 from private.bpay_next_net_replacement r join private.bpay_next_paye_net_request_refusal f on f.command_id=r.id
   and f.run_worker_id=r.run_worker_id and f.candidate_id=r.candidate_id and f.request_no=r.request_no and f.refusal_code=r.refusal_code
  where r.run_worker_id=new.id and r.candidate_id=new.candidate_id
   and r.run_id=new.run_id and r.request_no=new.net_request_high_water and r.stage='REFUSED'
   and r.refusal_code=new.review_issue_code and new.net_replacement_pending_id is null
   and new.net_request_revision=new.net_projection_revision and r.prior_projection_no=new.net_projection_revision) then
  raise exception using errcode='23514',message='BPAY_NEXT_READY_NET_REFUSAL_IDENTITY_INVALID';end if;
 return new;
end $function$;
drop trigger if exists bpay_next_ready_net_refusal_guard on private.bpay_next_run_worker;
create trigger bpay_next_ready_net_refusal_guard before insert or update of status,review_issue_code,net_request_high_water,
 net_replacement_pending_id,net_request_revision,net_projection_revision on private.bpay_next_run_worker
 for each row execute function private.bpay_next_ready_net_refusal_guard_v1();


-- DR-37 status
create or replace function private.bpay_next_worker_status_page_v1(
  p_run_id uuid,p_worker_id uuid,p_after_transfer_no integer,p_limit integer
) returns jsonb language plpgsql stable security invoker
set search_path=pg_catalog,private
as $function$
declare
  v_header jsonb;v_row record;v_rows jsonb:='[]'::jsonb;
  v_bytes integer;v_count integer:=0;v_more boolean:=false;
  v_query text;
begin
  if p_run_id is null or p_worker_id is null or p_limit is null or p_limit not between 1 and 100
     or (p_after_transfer_no is not null and p_after_transfer_no<1) then
    raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
  end if;
  if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT') then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  -- One run, one worker, one accepted-projection unique lookup. During a net
  -- replacement show the accepted projection AND an explicit pending flag;
  -- never portray the new request as already accepted. Original CSV advice
  -- uses the transfer's own projection, not this current progress pointer.
  select pg_catalog.jsonb_build_object(
    'run_id',r.id,'run_status',r.status,'selection_state',r.selection_state,
    'review_revision',r.review_revision::text,'run_worker_id',w.id,
    'candidate_id',w.candidate_id,'worker_status',w.status,
    'target_pay_channel',w.target_pay_channel,'umbrella_id',w.target_umbrella_id,
    'preparation_revision',w.preparation_revision::text,
    'case_selection_revision',w.case_selection_revision::text,
    'net_request_revision',w.net_request_revision::text,
    'net_projection_revision',w.net_projection_revision::text,
    'net_pending',w.net_replacement_pending_id is not null or w.net_request_revision<>w.net_projection_revision,
    'net_replacement_pending_id',w.net_replacement_pending_id,
    'net_request_high_water',w.net_request_high_water::text,'replacement_id',rep.id,'replacement_stage',rep.stage,
    'requested_net',req.entered_paye_net::text,'replacement_request_no',rep.request_no::text,
    'replacement_cursor',job.cursor_key,'replacement_expected_count',rep.expected_instruction_count::text,
    'replacement_staged_count',rep.staged_instruction_count::text,'replacement_validated_count',rep.validated_instruction_count::text,
    'replacement_committed_count',rep.committed_instruction_count::text,'net_refused_code',refusal.refusal_code,
    'review_issue_code',w.review_issue_code,'review_issue_work_id',w.review_issue_work_id,
    'frozen_gross_ex_vat',w.gross_ex_vat::text,'frozen_gross_vat',w.gross_vat::text,
    'frozen_gross_inc_vat',w.gross_inc_vat::text,'gross_stage_deductions',contribution.gross_stage_deductions::text,'net_recovery_reserved',contribution.net_recovery_reserved::text,'frozen_offer_complete',contribution.frozen_offer_complete,
    'projection_id',p.id,'projection_input_kind',p.input_kind,
    'entered_paye_net',p.entered_paye_net::text,
    'accepted_recoveries',p.accepted_recoveries::text,
    'accepted_net_additions',p.accepted_net_additions::text,
    'cash_amount',p.cash_amount::text,
    'reservation_mode',rv.reservation_mode,'reservation_projection_id',rv.reservation_projection_id,
    'remaining_reservation_cash',rv.remaining_reservation_cash::text,'reservation_internal_zero',rv.reservation_internal_zero,
    'reservation_holds_state',rv.reservation_holds_state
  ) into v_header
  from private.bpay_next_pay_run r
  join private.bpay_next_run_worker w on w.run_id=r.id and w.id=p_worker_id
  cross join lateral private.bpay_next_run_channel_contribution_v1(w) contribution
  left join private.bpay_next_net_projection p
    on p.run_worker_id=w.id and p.projection_no=w.net_projection_revision and p.retired_at_utc is null
  left join private.bpay_next_net_replacement rep on rep.run_worker_id=w.id and rep.request_no=w.net_request_high_water
  left join private.bpay_next_paye_net_request req on req.command_id=rep.id
  left join private.bpay_next_job job on job.id=rep.original_job_id
  left join private.bpay_next_paye_net_request_refusal refusal on refusal.command_id=rep.id and rep.stage='REFUSED'
  cross join lateral private.bpay_next_original_reservation_v1(w.id,null) rv
  where r.id=p_run_id;
  if not found then raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_SCOPE_INVALID';end if;
  v_bytes:=512+pg_catalog.octet_length(v_header::text);
  v_query:=$query$select pg_catalog.to_jsonb(page) as row_value from (
      select t.id as transfer_id,t.transfer_no::text,t.projection_id,t.execution_kind,
        t.status,t.cash_amount::text,t.original_transfer_id,
        rv.reservation_state,rv.remaining_reservation_cash,rv.reservation_holds_state
      from private.bpay_next_transfer t
      cross join lateral private.bpay_next_original_reservation_v1(t.run_worker_id,t.id) rv where t.run_worker_id=$1$query$;
  if p_after_transfer_no is not null then
    v_query:=v_query||' and t.transfer_no>$2';
  end if;
  v_query:=v_query||' order by t.transfer_no,t.id limit $3) page';
  for v_row in execute v_query using p_worker_id,p_after_transfer_no,p_limit
  loop
    if v_bytes+pg_catalog.octet_length(v_row.row_value::text)+1>120000 then
      if v_count=0 then raise exception using errcode='54000',message='BPAY_NEXT_PREPARATION_ROW_TOO_LARGE';end if;
      v_more:=true;exit;
    end if;
    v_bytes:=v_bytes+pg_catalog.octet_length(v_row.row_value::text)+1;
    v_rows:=v_rows||pg_catalog.jsonb_build_array(v_row.row_value);v_count:=v_count+1;
  end loop;
  return pg_catalog.jsonb_build_object('header',v_header,'rows',v_rows,'complete',not(v_more or v_count=p_limit));
end
$function$;


-- DR-37 worksheet-view
create or replace view private.bpay_next_paye_worksheet_rows_v1 with(security_invoker=true) as
with frozen as (
 select w.run_id,w.id run_worker_id,w.candidate_id,w.status worker_status,w.captured_line_count,w.case_instruction_count,
  coalesce(s.captured_candidate_display_name,label.candidate_display_name) candidate_display_name,
  coalesce(s.captured_candidate_reference,label.candidate_reference) candidate_reference,
  lower(s.captured_candidate_surname) collate "C" candidate_surname,
  w.gross_inc_vat payroll_gross,
  case when w.case_instruction_count=0 then w.gross_inc_vat when d.id is not null then d.captured_work_gross+d.allocated_gross_additions end earnings_gross,
  case when w.case_instruction_count=0 then 0::numeric when d.id is not null then d.allocated_gross_deductions end gross_stage_deductions,
  case when w.case_instruction_count=0 then 0::numeric when d.id is not null then d.allocated_net_additions end net_additions,
  d.id draft_state_id,n.id accepted_net_state_id,p.id projection_id,p.entered_paye_net accepted_net,p.cash_amount,p.input_kind,
  w.net_projection_revision expected_projection_revision,
  case when rep.stage='REFUSED' or w.net_replacement_pending_id is not null or q.request_no>w.net_projection_revision then q.entered_paye_net end pending_net,
  case when rep.id is not null then
    case when rep.stage='REFUSED' then 'REFUSED'
      when w.net_replacement_pending_id is not null then case when j.attempt_count>0 then 'APPLYING' else 'REQUESTED' end
      when p.id is not null then 'ACCEPTED' else 'NONE' end
   when q.request_no>w.net_projection_revision then
    case when w.status='REVIEW' or pending.net_stage='REVIEW' or j.status='FAILED' then 'REFUSED'
     when j.attempt_count>0 then 'APPLYING' else 'REQUESTED' end
   when p.id is not null then 'ACCEPTED' else 'NONE' end projection_status,
  (w.captured_line_count=0 and w.gross_inc_vat=0 and b.status='SEALED' and b.target_total_inc_vat>0
   and b.target_total_inc_vat=d.allocated_net_additions and b.beneficiary_id=w.candidate_id) is true payout_only,
  w.captured_preparation_revision,w.case_selection_revision,
  rep.id replacement_id,rep.stage replacement_stage,w.net_request_high_water,w.net_replacement_pending_id,
  q.entered_paye_net requested_net,refusal.refusal_code net_refused_code,rep.request_no replacement_request_no,
  j.cursor_key replacement_cursor,rep.expected_instruction_count replacement_expected_count,
  rep.staged_instruction_count replacement_staged_count,rep.validated_instruction_count replacement_validated_count,
  rep.committed_instruction_count replacement_committed_count
 from private.bpay_next_run_worker w
 join private.bpay_next_selection_candidate s on s.run_id=w.run_id and s.candidate_id=w.candidate_id
 left join lateral (
  select r.candidate_display_name,r.candidate_reference from private.bpay_next_run_work rw
  join private.bpay_next_work_revision r on r.id=rw.captured_revision_id and r.work_id=rw.work_id
  where rw.run_worker_id=w.id and rw.candidate_id=w.candidate_id order by rw.work_id,rw.id limit 1
 ) label on s.captured_candidate_display_name is null
 left join private.bpay_next_case_allocation_state d on d.run_worker_id=w.id and d.preparation_revision=w.captured_preparation_revision
  and d.selection_revision=w.case_selection_revision and d.pass_kind='DRAFT' and d.projection_no=0
  and d.status='COMPLETE' and d.completed_at_utc is not null and d.prepare_stage='COMPLETE'
 left join private.bpay_next_net_projection p on p.run_worker_id=w.id and p.projection_no=w.net_projection_revision
  and p.retired_at_utc is null and p.input_kind in ('PAYE_MANUAL','PAYE_IMPORT','CASE_PAYOUT')
 left join private.bpay_next_case_allocation_state n on n.run_worker_id=w.id and n.preparation_revision=w.captured_preparation_revision
  and n.selection_revision=w.case_selection_revision and n.pass_kind='NET' and n.projection_no=w.net_projection_revision
  and n.status='COMPLETE' and n.net_stage='COMPLETE' and n.completed_at_utc is not null and n.projection_id=p.id
 left join private.bpay_next_net_replacement rep on rep.run_worker_id=w.id and rep.request_no=w.net_request_high_water
 left join private.bpay_next_paye_net_request_refusal refusal on refusal.command_id=rep.id and rep.stage='REFUSED'
 left join private.bpay_next_paye_net_request q on q.run_worker_id=w.id
  and q.request_no=case when w.case_selection_revision>0 then greatest(w.net_request_high_water,w.net_request_revision) else w.net_request_revision end
 left join private.bpay_next_job j on j.command_id=q.command_id and j.candidate_id=w.candidate_id and j.job_kind='PAYE_NET_ENTRY'
 left join private.bpay_next_case_allocation_state pending on pending.run_worker_id=w.id and pending.preparation_revision=w.captured_preparation_revision
  and pending.selection_revision=w.case_selection_revision and pending.pass_kind='NET' and pending.projection_no=w.net_request_revision
 left join private.bpay_next_case_payout_basis b on b.run_worker_id=w.id and b.preparation_revision=w.captured_preparation_revision
 where w.target_pay_channel='PAYE'
), figures as (
 select f.*,a.advance_repayment,a.overpayment_recovery,a.gross_manual_debt,a.net_manual_debt,a.other_repayment,
  case when f.case_instruction_count=0 then 0::numeric when f.draft_state_id is not null then a.net_recovery end net_recovery,
  case when f.accepted_net_state_id is not null then 'ACCEPTED' when f.draft_state_id is not null then 'RESERVED' end deduction_basis
 from frozen f
 left join lateral (
  select coalesce(sum(x.allocated_target_ex_vat) filter(where i.payroll_stage='NET_DEDUCT' and i.case_kind='ADVANCE' and i.case_subtype='PAYMENT_ADVANCE'),0) advance_repayment,
   coalesce(sum(x.allocated_target_ex_vat) filter(where i.payroll_stage='GROSS_DEDUCT' and i.case_kind='OVERPAYMENT'),0) overpayment_recovery,
   coalesce(sum(x.allocated_target_ex_vat) filter(where i.payroll_stage='GROSS_DEDUCT' and i.case_kind='MANUAL_DEBT'),0) gross_manual_debt,
   coalesce(sum(x.allocated_target_ex_vat) filter(where i.payroll_stage='NET_DEDUCT' and i.case_kind='MANUAL_DEBT'),0) net_manual_debt,
   coalesce(sum(x.allocated_target_ex_vat) filter(where i.payroll_stage='NET_DEDUCT' and i.case_kind='LOAN'),0) other_repayment,
   coalesce(sum(x.allocated_target_ex_vat) filter(where i.payroll_stage='NET_DEDUCT'),0) net_recovery
  from private.bpay_next_run_case_instruction i
  join private.bpay_next_case_allocation_result x on x.instruction_id=i.id
   and x.state_id=case when i.payroll_stage='NET_DEDUCT' and f.accepted_net_state_id is not null then f.accepted_net_state_id else f.draft_state_id end
   and x.run_worker_id=f.run_worker_id and x.candidate_id=f.candidate_id
   and x.preparation_revision=f.captured_preparation_revision and x.selection_revision=f.case_selection_revision
  where i.run_worker_id=f.run_worker_id and i.preparation_revision=f.captured_preparation_revision and i.selection_revision=f.case_selection_revision
 ) a on f.case_instruction_count=0 or f.draft_state_id is not null
)
select run_id,run_worker_id,candidate_id,candidate_display_name,candidate_reference,candidate_surname,worker_status,
 captured_line_count,case_instruction_count,earnings_gross,payroll_gross,gross_stage_deductions,
 advance_repayment,overpayment_recovery,gross_manual_debt,net_manual_debt,other_repayment,net_recovery,net_additions,deduction_basis,
 payout_only,(captured_line_count>0 or payroll_gross>0) applicable_net,projection_id,
 accepted_net,pending_net,projection_status,expected_projection_revision,cash_amount,
 case when input_kind in ('PAYE_MANUAL','PAYE_IMPORT') then input_kind end input_source,
 case when payout_only then 'PAYOUT_ONLY' when accepted_net=0 then 'ENTERED_ZERO' when accepted_net is not null then 'ENTERED' else 'MISSING' end net_state,
 replacement_id,replacement_stage,net_request_high_water,net_replacement_pending_id,requested_net,net_refused_code,replacement_request_no,replacement_cursor,
 replacement_expected_count,replacement_staged_count,replacement_validated_count,replacement_committed_count
from figures;


-- DR-37 worksheet-page
create or replace function private.bpay_next_paye_worksheet_page_v1(p_actor_user_id uuid,p_run_id uuid,p_candidate_id uuid,p_query text,p_sort text,p_after jsonb,p_limit integer)
returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare
 v_context jsonb;v_rows jsonb:='[]'::jsonb;v_cursor jsonb;v_key jsonb;v_row record;v_sql text;v_col text;
 v_numeric boolean;v_desc boolean;v_count integer:=0;v_bytes integer;v_more boolean:=false;
begin
 if p_actor_user_id is null or p_run_id is null or p_query is null or length(p_query)>120 or p_after is null
  or p_sort is null or p_sort!~'^(CANDIDATE|GROSS|ADVANCE|OVERPAYMENT|NET|CASH|SOURCE|STATUS)_(ASC|DESC)$'
  or p_limit is null or p_limit not between 1 and 100 then
  raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
 perform private.bpay_next_channel_drafts_page_v1(p_run_id);
 if not exists(select 1 from private.bpay_next_run_worker w where w.run_id=p_run_id and w.target_pay_channel='PAYE'
  and (w.captured_line_count>0 or w.gross_inc_vat>0)) then
  raise exception using errcode='55000',message='BPAY_NEXT_PAYE_WORKSHEET_NOT_APPLICABLE';end if;
 select jsonb_build_object('run_id',r.id,'pay_channel','PAYE','actor_user_id',p_actor_user_id,'owner_epoch',m.owner_epoch::text,
  'review_revision',r.review_revision::text,'run_status',r.status,'totals_revision',t.totals_revision::text,
  'net_request_sequence',coalesce((select max(c.agency_sequence::bigint) from private.bpay_next_run_worker w
    join private.bpay_next_paye_net_request q on q.run_worker_id=w.id and q.request_no=case when w.case_selection_revision>0 then greatest(w.net_request_high_water,w.net_request_revision) else w.net_request_revision end
    join private.bpay_next_command c on c.id=q.command_id where w.run_id=r.id and w.target_pay_channel='PAYE'),0)::text,
  'candidate_id',p_candidate_id,'query',p_query,'sort',p_sort)
 into v_context from private.bpay_next_pay_run r join private.bpay_next_module_control m on m.id=1 and m.active_owner='NEXT'
 join private.bpay_next_run_channel_totals t on t.run_id=r.id and t.pay_channel='PAYE' where r.id=p_run_id;
 if not found then raise exception using errcode='55000',message='BPAY_NEXT_PAYE_WORKSHEET_CONTEXT_CHANGED';end if;
 v_col:=case split_part(p_sort,'_',1) when 'CANDIDATE' then 'candidate_surname' when 'GROSS' then 'earnings_gross'
  when 'ADVANCE' then 'advance_repayment' when 'OVERPAYMENT' then 'overpayment_recovery' when 'NET' then 'accepted_net'
  when 'CASH' then 'cash_amount' when 'SOURCE' then 'input_source' else 'projection_status' end;
 v_numeric:=split_part(p_sort,'_',1) in ('GROSS','ADVANCE','OVERPAYMENT','NET','CASH');v_desc:=right(p_sort,4)='DESC';
 if p_after<>'null'::jsonb then
  if jsonb_typeof(p_after) is distinct from 'object' or octet_length(p_after::text)>4096
   or p_after-array['context','candidate_id','run_worker_id']<>'{}'::jsonb or not(p_after?&array['context','candidate_id','run_worker_id'])
   or jsonb_typeof(p_after->'candidate_id') is distinct from 'string' or p_after->>'candidate_id'!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
   or jsonb_typeof(p_after->'run_worker_id') is distinct from 'string' or p_after->>'run_worker_id'!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
   raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
  if p_after->'context' is distinct from v_context then raise exception using errcode='55000',message='BPAY_NEXT_PAYE_WORKSHEET_CONTEXT_CHANGED';end if;
  -- Resolve the native frozen boundary by identity; long labels never enter the GET cursor.
  select jsonb_build_array(case when v_numeric then k.key_num is null else k.key_text is null end,case when v_numeric then k.key_num::numeric(18,2)::text else k.key_text end,w.candidate_id,w.run_worker_id) into v_key from private.bpay_next_paye_worksheet_rows_v1 w cross join lateral(select
 case v_col when 'earnings_gross' then w.earnings_gross when 'advance_repayment' then w.advance_repayment when 'overpayment_recovery' then w.overpayment_recovery when 'accepted_net' then w.accepted_net when 'cash_amount' then w.cash_amount end key_num,
 case v_col when 'candidate_surname' then w.candidate_surname when 'input_source' then w.input_source when 'projection_status' then w.projection_status end key_text) k
   where w.run_id=p_run_id and w.candidate_id=(p_after->>'candidate_id')::uuid and w.run_worker_id=(p_after->>'run_worker_id')::uuid
   and (p_candidate_id is null or w.candidate_id=p_candidate_id)
   and (p_query='' or strpos(lower(coalesce(w.candidate_display_name,'')),lower(p_query))>0 or strpos(lower(coalesce(w.candidate_reference,'')),lower(p_query))>0);
  if v_key is null then raise exception using errcode='55000',message='BPAY_NEXT_PAYE_WORKSHEET_CONTEXT_CHANGED';end if;
 end if;
 -- One typed static scoped query: no caller-controlled identifier or dynamic SQL.
 v_bytes:=1024+octet_length(v_context::text);
 for v_row in select jsonb_build_object('run_worker_id',w.run_worker_id,
 'candidate_id',w.candidate_id,
 'replacement_id',w.replacement_id,'replacement_stage',w.replacement_stage,'net_request_high_water',w.net_request_high_water::text,
 'net_replacement_pending_id',w.net_replacement_pending_id,
 'requested_net',w.requested_net::text,'net_refused_code',w.net_refused_code,'replacement_request_no',w.replacement_request_no::text,
 'replacement_cursor',w.replacement_cursor,'replacement_expected_count',w.replacement_expected_count::text,
 'replacement_staged_count',w.replacement_staged_count::text,'replacement_validated_count',w.replacement_validated_count::text,
 'replacement_committed_count',w.replacement_committed_count::text,
 'candidate_display_name',w.candidate_display_name,
 'candidate_reference',w.candidate_reference,
 'worker_status',w.worker_status,
 'captured_line_count',w.captured_line_count::text,
 'case_instruction_count',w.case_instruction_count::text,
 'earnings_gross',w.earnings_gross::numeric(18,2)::text,
 'payroll_gross',w.payroll_gross::numeric(18,2)::text,
 'gross_stage_deductions',w.gross_stage_deductions::numeric(18,2)::text,
 'advance_repayment',w.advance_repayment::numeric(18,2)::text,
 'overpayment_recovery',w.overpayment_recovery::numeric(18,2)::text,
 'gross_manual_debt',w.gross_manual_debt::numeric(18,2)::text,
 'net_manual_debt',w.net_manual_debt::numeric(18,2)::text,
 'other_repayment',w.other_repayment::numeric(18,2)::text,
 'net_recovery',w.net_recovery::numeric(18,2)::text,
 'net_additions',w.net_additions::numeric(18,2)::text,
 'deduction_basis',w.deduction_basis,
 'payout_only',w.payout_only,
 'applicable_net',w.applicable_net,
 'projection_id',w.projection_id,
 'accepted_net',w.accepted_net::numeric(18,2)::text,
 'pending_net',w.pending_net::numeric(18,2)::text,
 'projection_status',w.projection_status,
 'expected_projection_revision',w.expected_projection_revision::text,
 'cash_amount',w.cash_amount::numeric(18,2)::text,
 'input_source',w.input_source,
 'net_state',w.net_state,'sort_key',jsonb_build_array(case when v_numeric then k.key_num is null else k.key_text is null end,case when v_numeric then k.key_num::numeric(18,2)::text else k.key_text end,w.candidate_id,w.run_worker_id)) row_value
 from private.bpay_next_paye_worksheet_rows_v1 w cross join lateral(select
 case v_col when 'earnings_gross' then w.earnings_gross when 'advance_repayment' then w.advance_repayment when 'overpayment_recovery' then w.overpayment_recovery when 'accepted_net' then w.accepted_net when 'cash_amount' then w.cash_amount end key_num,
 case v_col when 'candidate_surname' then w.candidate_surname when 'input_source' then w.input_source when 'projection_status' then w.projection_status end key_text) k
 where w.run_id=p_run_id and (p_candidate_id is null or w.candidate_id=p_candidate_id)
 and (p_query='' or strpos(lower(coalesce(w.candidate_display_name,'')),lower(p_query))>0 or strpos(lower(coalesce(w.candidate_reference,'')),lower(p_query))>0)
 and (p_after='null'::jsonb or
  (case when v_numeric then k.key_num is null else k.key_text is null end)::integer>(v_key->>0)::boolean::integer
  or ((case when v_numeric then k.key_num is null else k.key_text is null end)=(v_key->>0)::boolean and
   ((v_numeric and ((not v_desc and k.key_num>(v_key->>1)::numeric) or (v_desc and k.key_num<(v_key->>1)::numeric)))
    or (not v_numeric and ((not v_desc and k.key_text collate "C">(v_key->>1) collate "C") or (v_desc and k.key_text collate "C"<(v_key->>1) collate "C")))
    or ((case when v_numeric then k.key_num is not distinct from (v_key->>1)::numeric else k.key_text collate "C" is not distinct from (v_key->>1) collate "C" end)
     and (w.candidate_id,w.run_worker_id)>((v_key->>2)::uuid,(v_key->>3)::uuid)))))
 order by (case when v_numeric then k.key_num is null else k.key_text is null end),
 case when not v_desc then k.key_num end asc,case when v_desc then k.key_num end desc,
 (case when not v_desc then k.key_text end) collate "C" asc,(case when v_desc then k.key_text end) collate "C" desc,w.candidate_id,w.run_worker_id
 limit p_limit loop
  if v_bytes+octet_length(v_row.row_value::text)+1>100000 then
   if v_count=0 then raise exception using errcode='54000',message='BPAY_NEXT_PREPARATION_ROW_TOO_LARGE';end if;
   v_more:=true;exit;end if;
  v_bytes:=v_bytes+octet_length(v_row.row_value::text)+1;v_rows:=v_rows||jsonb_build_array(v_row.row_value);
  v_key:=v_row.row_value->'sort_key';v_count:=v_count+1;
 end loop;
 v_more:=v_more or v_count=p_limit;
 if v_more then v_cursor:=jsonb_build_object('context',v_context,'candidate_id',v_key->>2,'run_worker_id',v_key->>3);
  if octet_length(v_cursor::text)>4096 then raise exception using errcode='54000',message='BPAY_NEXT_PREPARATION_ROW_TOO_LARGE';end if;end if;
 return jsonb_build_object('context',v_context,'rows',v_rows,'complete',not v_more,'next_cursor',v_cursor);
end
$function$;


-- DR-37 dispatch
create or replace function public.bpay_next_financial_job_v1(
  p_action text,p_job_id uuid,p_job_kind text,p_args jsonb
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_job private.bpay_next_job%rowtype;
  v_keys text[];
  v_key text;
  v_paged boolean;
  v_integer_cursor boolean;
  v_phase text;
  v_lease_seconds integer;
  v_nonce uuid;
  v_owner_epoch bigint;
  v_cursor bigint;
  v_text_cursor text;
  v_work_cursor uuid;
  v_component_cursor text;
  v_limit integer;
  v_claim record;
  v_claim_state record;
  v_page record;
  v_worker private.bpay_next_run_worker%rowtype;
  v_result jsonb;
  v_output jsonb;
  v_lease_until text;
  v_claim_failure text;
  v_case_net boolean:=false;
  v_expired_prepare boolean:=false;
  v_discarded_prepare boolean:=false;
  v_defer_pin_module boolean:=false;
begin
  if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),
      nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception using errcode='42501',message='BPAY_NEXT_JOB_FORBIDDEN';
  end if;
  if p_action is null or p_action not in ('CLAIM','STEP','FAILURE') or p_job_id is null
     or p_job_kind is null or p_job_kind not in
       ('PAYE_NET_ENTRY','TRANSFER_BUILD','CSV_SETTLEMENT','CSV_RETURN','CASH_REISSUE','SIMPLE_CANCEL','POSITION_APPLY','PREPARE','REISSUE_CANCEL','INTERNAL_SETTLEMENT','PREPARATION_EXPIRY','PREPARATION_DISCARD','CASE_CREATE','CASE_EDIT','CASE_PAUSE','CASE_RESUME','CASE_WRITE_OFF')
     or p_args is null or pg_catalog.jsonb_typeof(p_args) is distinct from 'object'
     or pg_catalog.octet_length(p_args::text)>2048 then
    raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
  end if;
  if p_action='FAILURE' then
    if p_job_kind<>'PREPARE' then raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';end if;
    return private.bpay_next_prepare_failure_v1(p_job_id,p_args);
  end if;
  v_phase:=p_args->>'expected_phase';
  if p_action='STEP' and (p_job_kind in ('POSITION_APPLY','PREPARE')
      or (p_job_kind='PAYE_NET_ENTRY' and p_args ? 'expected_phase'))
     and (pg_catalog.jsonb_typeof(p_args->'expected_phase') is distinct from 'string'
       or not(v_phase=any(case p_job_kind when 'POSITION_APPLY' then array['NEW','REMOVED','SNOOZE']
         when 'PAYE_NET_ENTRY' then array['FINANCE_ALLOCATE']
         else array['NEW','PINNING','RESERVE','ALLOCATE','FINANCE_ALLOCATE','WORKBENCH_WORK','WORKBENCH_CASE','WORKER_RELEASE_WORK','WORKER_RELEASE_CASE','WORKER_RELEASE_FINAL'] end))) then
    raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
  end if;
  v_paged:=p_job_kind in ('TRANSFER_BUILD','CSV_SETTLEMENT','INTERNAL_SETTLEMENT','SIMPLE_CANCEL','POSITION_APPLY','PREPARATION_EXPIRY','PREPARATION_DISCARD')
    or (p_job_kind='PAYE_NET_ENTRY' and v_phase='FINANCE_ALLOCATE')
    or (p_job_kind='PREPARE' and v_phase in ('RESERVE','ALLOCATE','FINANCE_ALLOCATE','WORKBENCH_WORK','WORKBENCH_CASE','WORKER_RELEASE_WORK','WORKER_RELEASE_CASE','WORKER_RELEASE_FINAL'));
  v_integer_cursor:=p_job_kind in ('TRANSFER_BUILD','CSV_SETTLEMENT','INTERNAL_SETTLEMENT','SIMPLE_CANCEL','PREPARATION_EXPIRY','PREPARATION_DISCARD')
    or (p_job_kind='PAYE_NET_ENTRY' and v_phase='FINANCE_ALLOCATE')
    or (p_job_kind='PREPARE' and v_phase in ('NEW','PINNING','FINANCE_ALLOCATE','WORKBENCH_WORK','WORKBENCH_CASE','WORKER_RELEASE_WORK','WORKER_RELEASE_CASE','WORKER_RELEASE_FINAL'));
  v_keys:=case when p_action='CLAIM' then array['lease_seconds']
    when p_job_kind='POSITION_APPLY' then array['lease_nonce','owner_epoch','expected_phase','expected_cursor','limit']
    when p_job_kind='PAYE_NET_ENTRY' and v_phase='FINANCE_ALLOCATE' then array['lease_nonce','owner_epoch','expected_phase','expected_cursor','limit']
    when p_job_kind='PREPARE' and v_phase in ('NEW','PINNING') then array['lease_nonce','owner_epoch','expected_phase','expected_cursor']
    when p_job_kind='PREPARE' and v_phase in ('RESERVE','ALLOCATE') then array['lease_nonce','owner_epoch','expected_phase','expected_work_cursor','expected_component_cursor','limit']
    when p_job_kind='PREPARE' then array['lease_nonce','owner_epoch','expected_phase','expected_cursor','limit']
    when v_paged then array['lease_nonce','owner_epoch','expected_cursor','limit']
    else array['lease_nonce','owner_epoch'] end;
  if (select count(*) from pg_catalog.jsonb_object_keys(p_args))<>pg_catalog.array_length(v_keys,1)
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_args) k where not(k=any(v_keys))) then
    raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
  end if;
  if p_action='CLAIM' then
    if pg_catalog.jsonb_typeof(p_args->'lease_seconds') is distinct from 'number'
       or (p_args->>'lease_seconds')!~'^[1-9][0-9]{0,2}$' then
      raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
    end if;
    v_lease_seconds:=(p_args->>'lease_seconds')::integer;
    if v_lease_seconds not between 1 and 120 then
      raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
    end if;
  else
    if pg_catalog.jsonb_typeof(p_args->'lease_nonce') is distinct from 'string'
       or (p_args->>'lease_nonce')!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
      raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
    end if;
    foreach v_key in array array['owner_epoch','expected_cursor'] loop
      if v_key='expected_cursor' and (not coalesce(v_integer_cursor,false) or p_args->v_key='null'::jsonb) then
        continue;
      end if;
      if pg_catalog.jsonb_typeof(p_args->v_key) is distinct from 'string'
         or (p_args->>v_key)!~'^[1-9][0-9]{0,18}$' then
        raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
      end if;
      if (p_args->>v_key)::numeric>9223372036854775807 then
        raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
      end if;
    end loop;
    v_nonce:=(p_args->>'lease_nonce')::uuid;
    v_owner_epoch:=(p_args->>'owner_epoch')::bigint;
    if v_integer_cursor then v_cursor:=(p_args->>'expected_cursor')::bigint; end if;
    if p_job_kind='POSITION_APPLY' then
      if p_args->'expected_cursor'<>'null'::jsonb
         and (pg_catalog.jsonb_typeof(p_args->'expected_cursor') is distinct from 'string'
              or pg_catalog.char_length(p_args->>'expected_cursor') not between 1 and 256
              or (v_phase='SNOOZE' and (p_args->>'expected_cursor')!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$')) then
        raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
      end if;
      v_text_cursor:=p_args->>'expected_cursor';
    elsif p_job_kind='PREPARE' and v_phase in ('RESERVE','ALLOCATE') then
      -- A cursor is the complete (UUID work, opaque retained component TEXT)
      -- pair, or both NULL. Component bounds are CHARACTERS, not bytes.
      if (p_args->'expected_work_cursor'='null'::jsonb) is distinct from
          (p_args->'expected_component_cursor'='null'::jsonb)
         or (p_args->'expected_work_cursor'<>'null'::jsonb
             and (pg_catalog.jsonb_typeof(p_args->'expected_work_cursor') is distinct from 'string'
                  or (p_args->>'expected_work_cursor')!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'))
         or (p_args->'expected_component_cursor'<>'null'::jsonb
             and (pg_catalog.jsonb_typeof(p_args->'expected_component_cursor') is distinct from 'string'
                  or pg_catalog.char_length(p_args->>'expected_component_cursor') not between 1 and 256)) then
        raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
      end if;
      v_work_cursor:=(p_args->>'expected_work_cursor')::uuid;
      v_component_cursor:=p_args->>'expected_component_cursor';
    end if;
    if v_paged then
      if pg_catalog.jsonb_typeof(p_args->'limit') is distinct from 'number'
         or (p_args->>'limit')!~'^[1-9][0-9]{0,2}$' then
        raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
      end if;
      v_limit:=(p_args->>'limit')::integer;
      if v_limit not between 1 and 100 then
        raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
      end if;
    end if;
  end if;

  -- Immutable dispatch identity, without an outer job lock. Owners acquire
  -- Candidate -> job for CLAIM, parent run -> Candidate -> job for STEP.
  -- Taking job FOR UPDATE here would invert that established order.
  select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id;
  if v_job.job_kind<>p_job_kind then
    raise exception using errcode='23514',message='BPAY_NEXT_JOB_KIND_MISMATCH';
  end if;
  if p_job_kind='PREPARE' then
    v_output:=private.bpay_next_prepare_retry_admit_v1(p_job_id,p_action,p_args);
    if v_output is not null then return v_output;end if;
  end if;
  -- Query admission is outside even the public module SHARE. Plain dispatch
  -- discovery is not lease/module authority; every owner rechecks under locks.
  -- Definite terminal/advanced/cursor/stale replies must not newly contend on
  -- the query key. Their independent owner locks/validates the module before
  -- any write, and this boundary reasserts it immediately after that call.
  if p_action='STEP' and p_job_kind='PREPARE' and v_phase in ('NEW','PINNING') then
    if v_job.status='LEASED' and v_job.phase in ('NEW','PINNING')
       and v_job.lease_nonce is not distinct from v_nonce
       and v_job.owner_epoch is not distinct from v_owner_epoch
       and v_job.lease_until_utc>pg_catalog.clock_timestamp()
       and v_job.cursor_key::bigint is not distinct from v_cursor then
      perform private.weekly_source_pay_query_admit_v2();
    else
      v_defer_pin_module:=true;
    end if;
  end if;
  if not v_defer_pin_module and not exists(
      select 1 from private.bpay_next_module_control m
      where m.id=1 and m.active_owner='NEXT' for share) then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  if p_job_kind='PAYE_NET_ENTRY' then
    select r.case_draft_state_id is not null into strict v_case_net
      from private.bpay_next_paye_net_request r where r.command_id=v_job.command_id;
    if p_action='STEP' and v_case_net is distinct from (p_args ? 'expected_phase') then
      raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
    end if;
  end if;
  if p_action='CLAIM' then
    if p_job_kind='POSITION_APPLY' and v_job.phase='SNOOZE' and v_job.cursor_key is not null
      and v_job.cursor_key!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
      raise exception using errcode='55000',message='BPAY_NEXT_JOB_PHASE_UNSUPPORTED';end if;
    if (p_job_kind='POSITION_APPLY' and not(v_job.phase=any(case
          when v_job.status='DONE' then array['DONE'] else array['NEW','REMOVED','SNOOZE'] end)))
       or (p_job_kind='PREPARE' and not(v_job.phase=any(case when v_job.status='DONE' then array['READY','DONE','EXPIRED','DISCARDED','CANCELLED']
          else array['NEW','PINNING','RESERVE','ALLOCATE','FINANCE_ALLOCATE','WORKBENCH_WORK','WORKBENCH_CASE','WORKER_RELEASE_WORK','WORKER_RELEASE_CASE','WORKER_RELEASE_FINAL'] end)))
       or (p_job_kind='PREPARATION_EXPIRY' and not(v_job.phase=any(case
          when v_job.status='DONE' then array['EXPIRED'] else array['NEW','RELEASE'] end)))
       or (p_job_kind='PREPARATION_DISCARD' and not(v_job.phase=any(case when v_job.status='DONE' then array['DISCARDED','BLOCKED'] else array['NEW','RELEASE'] end)))
       or (p_job_kind in ('CASE_CREATE','CASE_EDIT','CASE_PAUSE','CASE_RESUME','CASE_WRITE_OFF') and (v_job.phase is distinct from
          case when v_job.status='DONE' then 'DONE' else 'NEW' end or v_job.cursor_key is not null)) then
      raise exception using errcode='55000',message='BPAY_NEXT_JOB_PHASE_UNSUPPORTED';
    end if;
    -- Distinguish terminal and busy claims; never return the stored nonce.
    -- These read-only early checks are not ownership: owners recheck/lock.
    if v_job.status='DONE' then
      raise exception using errcode='55000',message='BPAY_NEXT_JOB_TERMINAL';
    end if;
    if v_job.status='LEASED' and v_job.lease_until_utc>pg_catalog.clock_timestamp() then
      raise exception using errcode='55000',message='BPAY_NEXT_JOB_LEASE_BUSY';
    end if;
    begin
      case p_job_kind
        when 'POSITION_APPLY' then
          select c.* into strict v_claim from private.bpay_next_claim_position_job_v1(p_job_id,v_lease_seconds) c;
        when 'PREPARE' then
          select c.* into strict v_claim from private.bpay_next_claim_prepare_job_v1(p_job_id,v_lease_seconds) c;
        when 'PREPARATION_EXPIRY' then
          select c.* into strict v_claim from private.bpay_next_claim_preparation_expiry_job_v1(p_job_id,v_lease_seconds) c;
        when 'PREPARATION_DISCARD' then
          select c.* into strict v_claim from private.bpay_next_claim_preparation_discard_job_v1(p_job_id,v_lease_seconds) c;
        when 'CASE_CREATE' then
          select c.* into strict v_claim from private.bpay_next_claim_case_create_job_v1(p_job_id,v_lease_seconds) c;
        when 'CASE_EDIT' then
          select c.* into strict v_claim from private.bpay_next_claim_case_edit_job_v1(p_job_id,v_lease_seconds) c;
        when 'CASE_PAUSE','CASE_RESUME' then
          select c.* into strict v_claim from private.bpay_next_claim_case_pause_job_v1(p_job_id,v_lease_seconds) c;
        when 'CASE_WRITE_OFF' then
          select c.* into strict v_claim from private.bpay_next_claim_write_off_job_v1(p_job_id,v_lease_seconds) c;
        when 'PAYE_NET_ENTRY' then
          select c.* into strict v_claim from private.bpay_next_claim_simple_paye_net_job_v1(p_job_id,v_lease_seconds) c;
        when 'TRANSFER_BUILD' then
          select c.* into strict v_claim from private.bpay_next_claim_simple_transfer_job_v1(p_job_id,v_lease_seconds) c;
        when 'SIMPLE_CANCEL' then
          select c.* into strict v_claim from private.bpay_next_claim_simple_cancel_job_v1(p_job_id,v_lease_seconds) c;
        when 'REISSUE_CANCEL' then
          select c.* into strict v_claim from private.bpay_next_claim_reissue_cancel_job_v1(p_job_id,v_lease_seconds) c;
        else
          select c.* into strict v_claim from private.bpay_next_claim_simple_outcome_job_v1(p_job_id,v_lease_seconds) c;
      end case;
    exception when sqlstate '55000' then
      get stacked diagnostics v_claim_failure=message_text;
      if v_claim_failure not in
          ('BPAY_NEXT_PAYE_NET_JOB_NOT_CLAIMABLE','BPAY_NEXT_TRANSFER_JOB_NOT_CLAIMABLE',
           'BPAY_NEXT_OUTCOME_JOB_NOT_CLAIMABLE','BPAY_NEXT_CANCEL_JOB_NOT_CLAIMABLE',
           'BPAY_NEXT_JOB_NOT_CLAIMABLE','BPAY_NEXT_PREPARE_JOB_NOT_CLAIMABLE','BPAY_NEXT_REISSUE_CANCEL_JOB_NOT_CLAIMABLE','BPAY_NEXT_EXPIRY_JOB_NOT_CLAIMABLE','BPAY_NEXT_DISCARD_JOB_NOT_CLAIMABLE','BPAY_NEXT_CASE_CREATE_JOB_NOT_CLAIMABLE','BPAY_NEXT_CASE_EDIT_JOB_NOT_CLAIMABLE','BPAY_NEXT_CASE_PAUSE_JOB_NOT_CLAIMABLE','BPAY_NEXT_WRITE_OFF_JOB_NOT_CLAIMABLE','BPAY_NEXT_EARLIER_WORKER_COMMAND_PENDING') then
        raise;
      end if;
      if v_claim_failure='BPAY_NEXT_EARLIER_WORKER_COMMAND_PENDING'
         and p_job_kind not in ('POSITION_APPLY','PREPARE','REISSUE_CANCEL','PREPARATION_EXPIRY','PREPARATION_DISCARD') then raise; end if;
      -- A loser may have read READY, then waited behind the winning owner.
      -- The failed subtransaction released its owner locks. Classify its
      -- now-visible exact job without acquiring job ahead of Candidate/run,
      -- retrying any owner or adopting the winner's stored lease nonce.
      -- Job and admission predicates share one fresh read snapshot. A
      -- predecessor finishing during separate reads must not manufacture a
      -- permanent eligibility failure for an already accepted financial job.
      select j.*,
        exists(select 1 from private.bpay_next_pay_run r
          where r.id=private.bpay_next_prepare_job_run_v1(j.id) and r.status='PREPARING' and r.selection_state='SEALED'
            and private.bpay_next_prepare_job_active_v1(j.id)
            and exists(select 1 from private.bpay_next_run_worker w where w.run_id=r.id and w.candidate_id=j.candidate_id
              and (w.status='PREPARING' or (j.phase in ('WORKBENCH_WORK','WORKBENCH_CASE') and w.status in ('READY','REVIEW','CANCELLED'))
                or (j.phase in ('WORKER_RELEASE_WORK','WORKER_RELEASE_CASE','WORKER_RELEASE_FINAL') and w.status='CANCELLING')))) as prepare_admissible,
        exists(select 1 from private.bpay_next_cancel_request r
               where r.command_id=j.command_id and r.candidate_id=j.candidate_id
                 and r.status in ('REQUESTED','CANCELLING')) as cancel_request_pending,
        exists(select 1 from private.bpay_next_preparation_expiry_request r
          join private.bpay_next_pay_run run on run.id=r.run_id
          where r.command_id=j.command_id and r.status='CANCELLING'
            and run.status='CANCELLING' and run.confirmed_at_utc is null) as expiry_request_pending,
        exists(select 1 from private.bpay_next_preparation_discard_request q join private.bpay_next_pay_run r on r.id=q.run_id
          where q.command_id=j.command_id and q.status='CANCELLING' and r.status='CANCELLING' and r.confirmed_at_utc is null) as discard_request_pending,
        exists(select 1 from private.bpay_next_case_create_request r
          where r.command_id=j.command_id and r.candidate_id=j.candidate_id) as case_create_request_present,
        exists(select 1 from private.bpay_next_case_edit_request r
          where r.command_id=j.command_id and r.candidate_id=j.candidate_id) as case_edit_request_present,
        exists(select 1 from private.bpay_next_case_pause_request r
          where r.command_id=j.command_id and r.candidate_id=j.candidate_id and r.action=j.job_kind
            and r.module_epoch=j.module_epoch and r.command_sequence=j.command_sequence) as case_pause_request_present,
        exists(select 1 from private.bpay_next_write_off_request r
          where r.command_id=j.command_id and r.candidate_id=j.candidate_id and r.status='REQUESTED') as write_off_request_pending,
        exists(select 1 from private.bpay_next_job earlier
               where earlier.candidate_id=j.candidate_id
                 and earlier.command_sequence<j.command_sequence
                 and earlier.status<>'DONE') as predecessor_pending
      into strict v_claim_state from private.bpay_next_job j where j.id=p_job_id;
      if v_claim_state.job_kind<>p_job_kind or v_claim_state.module_epoch is distinct from
          (select m.owner_epoch from private.bpay_next_module_control m
           where m.id=1 and m.active_owner='NEXT') then
        raise;
      end if;
      if v_claim_state.status='DONE' then
        raise exception using errcode='55000',message='BPAY_NEXT_JOB_TERMINAL';
      end if;
      if not(v_claim_state.phase=any(case p_job_kind
          when 'PAYE_NET_ENTRY' then array['NEW','FINANCE_ALLOCATE']
          when 'TRANSFER_BUILD' then array['WORK']
          when 'SIMPLE_CANCEL' then array['NEW','RELEASE']
          when 'REISSUE_CANCEL' then array['NEW']
          when 'POSITION_APPLY' then array['NEW','REMOVED','SNOOZE']
          when 'PREPARE' then array['NEW','PINNING','RESERVE','ALLOCATE','FINANCE_ALLOCATE','WORKBENCH_WORK','WORKBENCH_CASE','WORKER_RELEASE_WORK','WORKER_RELEASE_CASE','WORKER_RELEASE_FINAL']
          when 'PREPARATION_EXPIRY' then array['NEW','RELEASE']
          when 'PREPARATION_DISCARD' then array['NEW','RELEASE']
          when 'CASE_CREATE' then array['NEW']
          when 'CASE_EDIT' then array['NEW']
          when 'CASE_PAUSE' then array['NEW']
          when 'CASE_RESUME' then array['NEW']
          when 'CASE_WRITE_OFF' then array['NEW']
          else array['MEMBERS'] end))
         or (p_job_kind='PREPARE' and not v_claim_state.prepare_admissible)
         or (p_job_kind='PREPARATION_EXPIRY' and not v_claim_state.expiry_request_pending)
         or (p_job_kind='PREPARATION_DISCARD' and not v_claim_state.discard_request_pending)
         or (p_job_kind='CASE_CREATE' and (not v_claim_state.case_create_request_present
              or v_claim_state.cursor_key is not null))
         or (p_job_kind='CASE_EDIT' and (not v_claim_state.case_edit_request_present
              or v_claim_state.cursor_key is not null))
         or (p_job_kind in ('CASE_PAUSE','CASE_RESUME') and (not v_claim_state.case_pause_request_present
              or v_claim_state.cursor_key is not null))
         or (p_job_kind='CASE_WRITE_OFF' and (not v_claim_state.write_off_request_pending
              or v_claim_state.cursor_key is not null))
         or (p_job_kind='SIMPLE_CANCEL'
             and (v_claim_state.available_at_utc>pg_catalog.clock_timestamp()
                  or not v_claim_state.cancel_request_pending)) then
        raise;
      end if;
      if v_claim_state.status='LEASED' and v_claim_state.lease_until_utc>pg_catalog.clock_timestamp() then
        raise exception using errcode='55000',message='BPAY_NEXT_JOB_LEASE_BUSY';
      end if;
      if v_claim_state.status in ('READY','LEASED') and v_claim_state.predecessor_pending then
        raise exception using errcode='55000',message='BPAY_NEXT_JOB_WAIT_FOR_PREDECESSOR';
      end if;
      if v_claim_state.status in ('READY','LEASED') and not v_claim_state.predecessor_pending
         and (v_claim_state.status='READY'
              or v_claim_state.lease_until_utc<=pg_catalog.clock_timestamp()) then
        -- The predecessor just completed or the short lease just expired.
        -- Only signal a fresh CLAIM; never invoke the owner a second time.
        raise exception using errcode='55000',message='BPAY_NEXT_JOB_CLAIM_RETRY';
      end if;
      -- Other permanent scope/phase/request admission failures retain the
      -- exact original owner rejection, not a general automatic retry.
      raise;
    end;
    -- Only the owner-returned newly acquired token crosses this boundary.
    select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id;
    if v_job.status<>'LEASED' or v_job.lease_nonce is distinct from v_claim.lease_nonce
       or v_job.owner_epoch is distinct from v_claim.owner_epoch then
      raise exception using errcode='23514',message='BPAY_NEXT_JOB_CLAIM_RESULT_INVALID';
    end if;
    v_lease_until:=pg_catalog.to_char(v_job.lease_until_utc at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"');
    v_output:=pg_catalog.jsonb_build_object(
      'job_id',p_job_id,'job_kind',p_job_kind,'phase',v_job.phase,
      'lease_nonce',v_claim.lease_nonce,'owner_epoch',v_claim.owner_epoch::text,
      'lease_until_utc',v_lease_until,'cursor',v_job.cursor_key);
    if p_job_kind='PREPARE' then
      v_output:=v_output||pg_catalog.jsonb_build_object(
        'cursor',case when v_job.phase in ('NEW','PINNING','FINANCE_ALLOCATE','WORKBENCH_WORK','WORKBENCH_CASE','WORKER_RELEASE_WORK','WORKER_RELEASE_CASE','WORKER_RELEASE_FINAL') then v_job.cursor_key else null end,
        'work_cursor',case when v_job.phase in ('RESERVE','ALLOCATE') then v_job.position_work_cursor else null end,
        'component_cursor',case when v_job.phase in ('RESERVE','ALLOCATE') then v_job.position_component_cursor else null end);
    end if;
  else
    -- Do not reject cleared DONE leases here. The owners' scoped DONE replay
    -- branches are authoritative and return the immutable retained result.
    case p_job_kind
      when 'POSITION_APPLY' then
        select p.* into strict v_page from private.bpay_next_apply_position_page_v1(
          p_job_id,v_nonce,v_owner_epoch,v_phase,v_text_cursor,v_limit) p;
        v_result:=pg_catalog.jsonb_build_object('phase',v_page.next_phase,
          'cursor',v_page.next_cursor,'rows_visited',v_page.rows_visited::text,'done',v_page.done);
      when 'PREPARE' then
        -- Terminal authority comes from the retained owner, never a fabricated
        -- READY row or an adopted active cursor/nonce. One owner per STEP.
        select private.bpay_next_preparation_expiry_fenced_v1(
          rc.run_id,rc.command_id) into v_expired_prepare
          from private.bpay_next_run_command rc where rc.run_id=private.bpay_next_prepare_job_run_v1(p_job_id);
        select private.bpay_next_preparation_discard_fenced_v1(rc.run_id,rc.command_id) into v_discarded_prepare
          from private.bpay_next_run_command rc where rc.run_id=private.bpay_next_prepare_job_run_v1(p_job_id);
        v_discarded_prepare:=coalesce(v_discarded_prepare,false) or(v_job.status='DONE' and v_job.phase='DISCARDED'
          and exists(select 1 from private.bpay_next_worker_reprepare_request x where x.previous_job_id=p_job_id
            and x.candidate_id=v_job.candidate_id and x.module_epoch=v_job.module_epoch and x.initiator_kind='OFFICE' and x.action='RELEASE'));
        if coalesce(v_discarded_prepare,false) then
          if v_job.status<>'DONE' then raise exception using errcode='55000',message='BPAY_NEXT_PREPARATION_DISCARDING';end if;
          v_result:=private.bpay_next_discarded_prepare_receipt_v1(p_job_id);
        elsif coalesce(v_expired_prepare,false) then
          if v_job.status<>'DONE' then
            raise exception using errcode='55000',message='BPAY_NEXT_PREPARATION_EXPIRING';
          end if;
          -- May have been retired before its first CLAIM: no worker is invented.
          -- Only the exact expiry owner validates and returns this old receipt.
          v_result:=private.bpay_next_expired_prepare_receipt_v1(p_job_id);
        elsif v_job.status='DONE' and v_job.phase='CANCELLED' then
          if not exists(select 1 from private.bpay_next_worker_reprepare_request x
            join private.bpay_next_run_worker w on w.id=x.run_worker_id
            where x.job_id=p_job_id and x.action='RELEASE' and x.status='COMPLETE'
              and x.target_preparation_revision=w.preparation_revision and w.status='CANCELLED'
              and x.module_epoch=v_job.module_epoch) then
            raise exception using errcode='55000',message='BPAY_NEXT_WORKER_ACTION_REVISION_STALE';end if;
          v_result:=pg_catalog.jsonb_build_object('phase','CANCELLED','cursor',v_job.cursor_key,'rows_visited','0','replay',true);
        elsif v_job.status='DONE' and (exists(select 1 from private.bpay_next_case_allocation_state s
          join private.bpay_next_run_worker w on w.id=s.run_worker_id
          where w.run_id=private.bpay_next_prepare_job_run_v1(p_job_id) and w.candidate_id=v_job.candidate_id
            and s.preparation_revision=w.captured_preparation_revision and s.pass_kind='DRAFT')
          or exists(select 1 from private.bpay_next_run_worker w
            where w.run_id=private.bpay_next_prepare_job_run_v1(p_job_id) and w.candidate_id=v_job.candidate_id and w.status='REVIEW'
              and w.review_issue_code in ('CASE_SELECTION_REQUIRED','CASE_TARGET_UNSUPPORTED','CASE_INPUT_UNBOUND',
                'CASE_WEEK_BASIS_UNBOUND','CASE_RETURN_FLOOR_UNBOUND','CASE_NO_PAYABLE_AMOUNT'))) then
          -- An original NEW/PINNING request has no page limit. Terminal
          -- read-back does no work, but the owner still requires a valid bound.
          v_result:=private.bpay_next_prepare_case_page_v1(p_job_id,v_nonce,v_owner_epoch,v_cursor,coalesce(v_limit,1));
        elsif v_job.status='DONE' and v_job.phase='READY' then
          v_result:=private.bpay_next_complete_no_case_worker_v1(p_job_id,v_nonce,v_owner_epoch);
        elsif v_job.status='DONE' and v_job.phase='DONE' then
          v_result:=private.bpay_next_pin_selected_work_v1(p_job_id,v_nonce,v_owner_epoch,null);
          v_result:=v_result||pg_catalog.jsonb_build_object('rows_visited','0');
        elsif v_phase in ('WORKER_RELEASE_WORK','WORKER_RELEASE_CASE','WORKER_RELEASE_FINAL') then
          v_result:=private.bpay_next_worker_release_page_v1(p_job_id,v_nonce,v_owner_epoch,v_phase,v_cursor,v_limit);
        elsif v_phase in ('WORKBENCH_WORK','WORKBENCH_CASE') then
          v_result:=private.bpay_next_prepare_projection_page_v1(p_job_id,v_nonce,v_owner_epoch,v_phase,v_cursor,v_limit);
        elsif v_phase in ('NEW','PINNING') then
          v_result:=private.bpay_next_pin_selected_work_v1(p_job_id,v_nonce,v_owner_epoch,v_cursor);
          -- This owner examines at most one selected work. It exposes no page
          -- count, but its exact work/replay receipt identifies zero vs one.
          v_result:=v_result||pg_catalog.jsonb_build_object('rows_visited',case
            when v_result->>'rows_visited' is not null then v_result->>'rows_visited'
            when v_result->>'replay'='true' then '0'
            when v_result->>'work_id' is not null then '1' else '0' end);
        elsif v_phase='RESERVE' then
          v_result:=private.bpay_next_capture_position_page_v1(p_job_id,v_nonce,v_owner_epoch,v_work_cursor,v_component_cursor,v_limit);
        elsif v_phase='ALLOCATE' then
          v_result:=private.bpay_next_hold_fresh_position_page_v1(p_job_id,v_nonce,v_owner_epoch,v_work_cursor,v_component_cursor,v_limit);
        elsif v_phase='FINANCE_ALLOCATE' then
          v_result:=private.bpay_next_prepare_case_page_v1(p_job_id,v_nonce,v_owner_epoch,v_cursor,v_limit);
        else
          raise exception using errcode='55000',message='BPAY_NEXT_JOB_PHASE_UNSUPPORTED';
        end if;
      when 'PAYE_NET_ENTRY' then
        if v_case_net then
          v_result:=private.bpay_next_apply_case_paye_net_page_v1(p_job_id,v_nonce,v_owner_epoch,v_cursor,v_limit);
        else
          v_result:=private.bpay_next_apply_simple_paye_net_v1(p_job_id,v_nonce,v_owner_epoch);
        end if;
      when 'TRANSFER_BUILD' then
        v_result:=private.bpay_next_build_simple_transfer_page_v1(p_job_id,v_nonce,v_owner_epoch,v_cursor,v_limit);
      when 'CSV_SETTLEMENT' then
        v_result:=private.bpay_next_post_simple_outcome_page_v1(p_job_id,v_nonce,v_owner_epoch,v_cursor,v_limit);
      when 'INTERNAL_SETTLEMENT' then
        v_result:=private.bpay_next_post_simple_outcome_page_v1(p_job_id,v_nonce,v_owner_epoch,v_cursor,v_limit);
      when 'CSV_RETURN' then
        v_result:=private.bpay_next_post_simple_return_v1(p_job_id,v_nonce,v_owner_epoch);
      when 'CASH_REISSUE' then
        v_result:=private.bpay_next_build_simple_reissue_v1(p_job_id,v_nonce,v_owner_epoch);
      when 'SIMPLE_CANCEL' then
        v_result:=private.bpay_next_cancel_simple_worker_page_v1(p_job_id,v_nonce,v_owner_epoch,v_cursor,v_limit);
      when 'REISSUE_CANCEL' then
        v_result:=private.bpay_next_apply_reissue_cancel_v1(p_job_id,v_nonce,v_owner_epoch);
      when 'PREPARATION_EXPIRY' then
        v_result:=private.bpay_next_expire_preparation_worker_page_v1(
          p_job_id,v_nonce,v_owner_epoch,v_cursor,v_limit);
      when 'PREPARATION_DISCARD' then
        v_result:=private.bpay_next_discard_preparation_worker_page_v1(p_job_id,v_nonce,v_owner_epoch,v_cursor,v_limit);
      when 'CASE_CREATE' then
        -- One exact original approval application. The retained 0330 owner
        -- alone governs lease validation and DONE/DONE immutable replay.
        v_result:=private.bpay_next_apply_case_create_v1(p_job_id,v_nonce,v_owner_epoch);
      when 'CASE_EDIT' then
        v_result:=private.bpay_next_apply_case_edit_v1(p_job_id,v_nonce,v_owner_epoch);
      when 'CASE_PAUSE','CASE_RESUME' then
        v_result:=private.bpay_next_apply_case_pause_v1(p_job_id,v_nonce,v_owner_epoch);
      when 'CASE_WRITE_OFF' then
        v_result:=private.bpay_next_apply_write_off_v1(p_job_id,v_nonce,v_owner_epoch);
    end case;
    if v_defer_pin_module and not exists(
        select 1 from private.bpay_next_module_control m
        where m.id=1 and m.active_owner='NEXT' for share) then
      raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
    end if;
    select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id;
    if v_job.status not in ('LEASED','DONE')
       or (p_job_kind<>'POSITION_APPLY' and pg_catalog.jsonb_typeof(v_result->'replay') is distinct from 'boolean')
       or (p_job_kind='POSITION_APPLY' and ((v_result->>'done')::boolean is distinct from (v_job.status='DONE')
            or v_result->>'phase' is distinct from v_job.phase
            or v_result->>'cursor' is distinct from v_job.cursor_key))
      or (p_job_kind='PREPARE' and v_result->>'phase' is distinct from
            case when v_discarded_prepare then 'DISCARDED' when v_expired_prepare then 'EXPIRED'
              when v_job.status='DONE' and v_job.phase='DONE' then 'REVIEW' else v_job.phase end)
       or (p_job_kind in ('PREPARATION_EXPIRY','PREPARATION_DISCARD') and (v_result->>'phase' is distinct from v_job.phase
            or v_result->>'cursor' is distinct from v_job.cursor_key))
       or (p_job_kind='CASE_CREATE' and (v_job.status<>'DONE' or v_job.phase<>'DONE'
            or v_job.cursor_key is not null or v_result->>'phase' is distinct from 'CASE_CREATED'
            or not exists(select 1 from private.bpay_next_case_create_request r
              where r.command_id=v_job.command_id and r.candidate_id=v_job.candidate_id
                and r.case_id::text=v_result->>'case_id')))
       or (p_job_kind='CASE_EDIT' and (v_job.status<>'DONE' or v_job.phase<>'DONE' or v_job.cursor_key is not null
          or v_result->>'phase' is distinct from case when v_job.last_error_code is null then 'CASE_EDITED' else 'BLOCKED' end
          or v_result->>'issue_code' is distinct from v_job.last_error_code
          or not exists(select 1 from private.bpay_next_case_edit_request q where q.command_id=v_job.command_id
            and q.candidate_id=v_job.candidate_id and q.case_id::text=v_result->>'case_id'
            and q.new_rule_id::text=v_result->>'rule_id' and (q.expected_case_revision+1)::text=v_result->>'case_revision')))
       or (p_job_kind in ('CASE_PAUSE','CASE_RESUME') and (v_job.status<>'DONE' or v_job.phase<>'DONE' or v_job.cursor_key is not null
          or not exists(select 1 from private.bpay_next_case_pause_request q where q.command_id=v_job.command_id
             and q.candidate_id=v_job.candidate_id and q.action=p_job_kind
             and private.bpay_next_case_pause_receipt_v1(q.command_id,q.case_id,q.candidate_id) is not distinct from(v_result-'replay'))))
       or (p_job_kind='CASE_WRITE_OFF' and (v_job.status<>'DONE' or v_job.phase<>'DONE'
            or v_job.cursor_key is not null or not exists(select 1 from private.bpay_next_write_off_request r
              where r.command_id=v_job.command_id and r.candidate_id=v_job.candidate_id
                and r.status in ('WRITTEN_OFF','BLOCKED','REVIEW') and r.status=v_result->>'phase'
                and r.case_id::text=v_result->>'case_id' and r.case_component_id::text=v_result->>'case_component_id'
                and r.applied_amount is not distinct from (v_result->>'applied_amount')::numeric
                and r.remaining_amount is not distinct from (v_result->>'remaining_amount')::numeric
                and r.protected_amount is not distinct from (v_result->>'protected_amount')::numeric
                and r.event_id::text is not distinct from v_result->>'event_id'
                and r.issue_code is not distinct from v_result->>'issue_code')))
       or (v_case_net and (v_result->>'phase' is distinct from
            case when v_job.status='DONE' and v_job.phase='DONE' then coalesce(
              (select case r.stage when 'REFUSED' then 'REFUSED' when 'CANCELLED' then 'DONE' end
                from private.bpay_next_net_replacement r where r.id=v_job.command_id),'REVIEW') else v_job.phase end
          or v_result->>'cursor' is distinct from v_job.cursor_key
          or (v_result->>'run_worker_id')::uuid is distinct from
            (select r.run_worker_id from private.bpay_next_paye_net_request r where r.command_id=v_job.command_id))) then
      raise exception using errcode='23514',message='BPAY_NEXT_JOB_STEP_RESULT_INVALID';
    end if;
    if v_case_net and exists(select 1 from private.bpay_next_net_replacement r where r.id=v_job.command_id)
      and not exists(select 1 from private.bpay_next_net_replacement r where r.id=v_job.command_id
        and r.run_worker_id::text=v_result->>'run_worker_id' and r.id::text=v_result->>'replacement_id'
        and r.stage=v_result->>'replacement_stage' and r.request_no::text=v_result->>'request_no'
        and r.staged_instruction_count::text=v_result->>'staged_count'
        and r.validated_instruction_count::text=v_result->>'validated_count'
        and r.committed_instruction_count::text=v_result->>'committed_count'
        and r.expected_instruction_count::text=v_result->>'expected_count'
        and r.accepted_projection_id::text is not distinct from v_result->>'projection_id'
        and r.refusal_code is not distinct from v_result->>'issue_code') then
      raise exception using errcode='23514',message='BPAY_NEXT_JOB_STEP_RESULT_INVALID';end if;
    if p_job_kind='PREPARE' then
      update private.bpay_next_job set retry_count=0,next_retry_at_utc=null,needs_attention=false,last_retry_owner_epoch=null,
        retry_prior_issue_code=null,retry_prior_issue_work_id=null where id=p_job_id;
    end if;
    -- Yield only this successfully validated page's own lease. The next ID
    -- notification takes a fresh CLAIM from the committed native checkpoint.
    if v_job.status='LEASED' then
      if v_job.lease_nonce is distinct from v_nonce or v_job.owner_epoch is distinct from v_owner_epoch then
        raise exception using errcode='23514',message='BPAY_NEXT_JOB_STEP_RESULT_INVALID';
      end if;
      update private.bpay_next_job set status='READY',lease_nonce=null,lease_until_utc=null
        where id=p_job_id and status='LEASED' and lease_nonce=v_nonce and owner_epoch=v_owner_epoch
        returning * into strict v_job;
    end if;
    v_lease_until:=pg_catalog.to_char(v_job.lease_until_utc at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"');
    v_output:=pg_catalog.jsonb_build_object(
      'job_id',p_job_id,'job_kind',p_job_kind,'owner_epoch',v_job.owner_epoch::text,
      'lease_until_utc',v_lease_until,'done',v_job.status='DONE');
    if p_job_kind<>'POSITION_APPLY' then
      v_output:=v_output||pg_catalog.jsonb_build_object('replay',v_result->'replay');
    end if;
    case p_job_kind
      when 'POSITION_APPLY' then
        v_output:=v_output||pg_catalog.jsonb_build_object('phase',v_result->>'phase',
          'cursor',v_result->>'cursor','rows_visited',v_result->>'rows_visited');
      when 'PREPARE' then
        if coalesce(v_expired_prepare,false) or coalesce(v_discarded_prepare,false) then
          v_output:=v_output||pg_catalog.jsonb_build_object('phase',case when v_discarded_prepare then 'DISCARDED' else 'EXPIRED' end,
            'cursor',v_result->>'cursor','work_cursor',v_result->>'work_cursor',
            'component_cursor',v_result->>'component_cursor',
            'run_worker_id',v_result->>'run_worker_id','worker_status',v_result->>'worker_status',
            'issue_code',v_result->>'issue_code','issue_work_id',v_result->>'issue_work_id',
            'rows_visited',v_result->>'rows_visited');
        else
        select w.* into strict v_worker from private.bpay_next_run_worker w
          where w.run_id=private.bpay_next_prepare_job_run_v1(p_job_id) and w.candidate_id=v_job.candidate_id;
        v_output:=v_output||pg_catalog.jsonb_build_object('phase',v_result->>'phase',
          'cursor',case when v_job.phase in ('NEW','PINNING','FINANCE_ALLOCATE','WORKBENCH_WORK','WORKBENCH_CASE','WORKER_RELEASE_WORK','WORKER_RELEASE_CASE','WORKER_RELEASE_FINAL') then v_job.cursor_key else null end,
          'work_cursor',case when v_job.phase in ('RESERVE','ALLOCATE') then v_job.position_work_cursor else null end,
          'component_cursor',case when v_job.phase in ('RESERVE','ALLOCATE') then v_job.position_component_cursor else null end,
          'run_worker_id',v_worker.id,'worker_status',v_worker.status,
          'issue_code',v_worker.review_issue_code,'issue_work_id',v_worker.review_issue_work_id,
          -- NULL means the exact owner did not report a count; do not fabricate
          -- zero for a REVIEW reached after earlier rows in the same page.
          'rows_visited',v_result->>'rows_visited');
        end if;
      when 'PAYE_NET_ENTRY' then
        v_output:=v_output||pg_catalog.jsonb_build_object(
          'phase',v_result->>'phase','projection_id',v_result->>'projection_id');
        if v_case_net then
          v_output:=v_output||pg_catalog.jsonb_build_object(
            'cursor',v_result->>'cursor','rows_visited',v_result->>'rows_visited',
            'run_worker_id',v_result->>'run_worker_id','issue_code',v_result->>'issue_code',
            'replacement_id',v_result->>'replacement_id','replacement_stage',v_result->>'replacement_stage',
            'request_no',v_result->>'request_no','staged_count',v_result->>'staged_count',
            'validated_count',v_result->>'validated_count','committed_count',v_result->>'committed_count','expected_count',v_result->>'expected_count');
        end if;
      when 'TRANSFER_BUILD' then
        v_output:=v_output||pg_catalog.jsonb_build_object(
          'phase',v_result->>'phase','transfer_id',v_result->>'transfer_id',
          'transfer_status',coalesce(v_result->>'transfer_status',v_result->>'phase'),
          'cursor',v_job.cursor_key,'member_count',v_result->>'member_count',
          'rows_visited',coalesce(v_result->>'rows_visited','0'));
      when 'CSV_SETTLEMENT' then
        v_output:=v_output||pg_catalog.jsonb_build_object(
          'phase',case when v_job.status='DONE' then 'COMPLETE' else 'MEMBERS' end,
          'cursor',v_job.cursor_key,'posted_members',v_result->>'posted_members',
          'rows_visited',coalesce(v_result->>'rows_visited','0'));
      when 'INTERNAL_SETTLEMENT' then
        v_output:=v_output||pg_catalog.jsonb_build_object(
          'phase',case when v_job.status='DONE' then 'COMPLETE' else 'MEMBERS' end,
          'cursor',v_job.cursor_key,'posted_members',v_result->>'posted_members',
          'rows_visited',coalesce(v_result->>'rows_visited','0'));
      when 'CSV_RETURN' then
        v_output:=v_output||pg_catalog.jsonb_build_object(
          'phase','COMPLETE','return_cash_id',v_result->>'return_cash_id');
      when 'CASH_REISSUE' then
        v_output:=v_output||pg_catalog.jsonb_build_object(
          'phase','COMPLETE','transfer_id',v_result->>'transfer_id');
      when 'REISSUE_CANCEL' then
        v_output:=v_output||pg_catalog.jsonb_build_object('phase',v_result->>'phase',
          'transfer_id',v_result->>'transfer_id','blocked_code',v_result->>'blocked_code');
      when 'SIMPLE_CANCEL' then
        v_output:=v_output||pg_catalog.jsonb_build_object(
          'phase',v_result->>'phase','run_worker_id',v_result->>'run_worker_id',
          'cursor',v_job.cursor_key,'released_line_count',v_result->>'released_line_count',
          'rows_visited',coalesce(v_result->>'rows_visited','0'),
          'holds_released',coalesce(v_result->>'holds_released','0'),
          'blocked_code',v_result->>'blocked_code');
      when 'PREPARATION_EXPIRY','PREPARATION_DISCARD' then
        v_output:=v_output||pg_catalog.jsonb_build_object('phase',v_result->>'phase',
          'cursor',v_result->>'cursor','rows_visited',v_result->>'rows_visited',
          'work_released',v_result->>'work_released','case_holds_released',v_result->>'case_holds_released',
          'run_worker_id',v_result->>'run_worker_id');
      when 'CASE_CREATE' then
        v_output:=v_output||pg_catalog.jsonb_build_object('phase',v_result->>'phase',
          'case_id',v_result->>'case_id');
      when 'CASE_EDIT' then
        v_output:=v_output||jsonb_build_object('phase',v_result->>'phase','case_id',v_result->>'case_id',
          'case_revision',v_result->>'case_revision','rule_id',v_result->>'rule_id','issue_code',v_result->>'issue_code');
      when 'CASE_PAUSE','CASE_RESUME' then
        v_output:=v_output||(v_result-'replay');
      when 'CASE_WRITE_OFF' then
        v_output:=v_output||pg_catalog.jsonb_build_object('phase',v_result->>'phase',
          'case_id',v_result->>'case_id','case_component_id',v_result->>'case_component_id',
          'applied_amount',v_result->>'applied_amount','remaining_amount',v_result->>'remaining_amount',
          'protected_amount',v_result->>'protected_amount','event_id',v_result->>'event_id','issue_code',v_result->>'issue_code');
    end case;
  end if;
  if pg_catalog.octet_length(v_output::text)>120000 then
    raise exception using errcode='54000',message='BPAY_NEXT_JOB_RESPONSE_INVALID';
  end if;
  return v_output;
end
$function$;


-- DR-37 command
create or replace function public.bpay_next_financial_intake_v1(
  p_actor_user_id uuid,p_action text,p_args jsonb
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_keys text[];
  v_key text;
  v_command_id uuid;
  v_worker_id uuid;
  v_transfer_id uuid;
  v_instruction_id uuid;
  v_return_cash_id uuid;
  v_run_id uuid;
  v_amount numeric;
  v_occurred_at timestamptz;
  v_result jsonb;
  v_output jsonb;
  v_sequence text;
  v_projection_revision bigint;
  v_case_selection_revision bigint;
  v_case_component_id uuid;
  v_component_revision bigint;
  v_proof_id uuid;
  v_reauth_scope jsonb;
  v_protected boolean;
begin
  if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),
      nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
     or p_actor_user_id is null
     or not exists(select 1 from public.tms_users u
                   where u.id=p_actor_user_id and u.is_active is true
                     and u.role::text='admin' for share) then
    raise exception using errcode='42501',message='BPAY_NEXT_COMMAND_FORBIDDEN';
  end if;
  if not exists(select 1 from private.bpay_next_module_control m
                where m.id=1 and m.active_owner='NEXT' for share) then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  if p_action in ('CASE_PAUSE','CASE_RESUME') then
    return private.bpay_next_accept_case_pause_v1(p_actor_user_id,p_action,p_args);
  end if;
  if p_action='CASE_EDIT' then
    return private.bpay_next_accept_case_edit_v1(p_actor_user_id,p_args);
  end if;
  if p_action='CASE_CREATE' then
    if p_args is null or jsonb_typeof(p_args) is distinct from 'object' or octet_length(p_args::text)>32768
      or (select count(*) from jsonb_object_keys(p_args))<>16
      or not(p_args ? 'command_id') or not(p_args ? 'preview_token')
      or jsonb_typeof(p_args->'command_id') is distinct from 'string'
      or (p_args->>'command_id')!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
      or p_args->'preview_token' is null or p_args->'preview_token'='null'::jsonb
      or (p_args-'command_id'-'preview_token') is distinct from (p_args->'preview_token'->'input') then
      raise exception using errcode='22023',message='BPAY_NEXT_CASE_CREATE_INPUT_INVALID';end if;
    perform private.bpay_next_case_create_terms_v1(p_args-'command_id'-'preview_token');
    v_result:=private.bpay_next_accept_case_create_v1((p_args->>'command_id')::uuid,(p_args->>'candidate_id')::uuid,p_actor_user_id,
      p_args->>'case_kind',p_args->>'case_subtype',p_args->>'tax_treatment',(p_args->>'principal')::numeric,
      (p_args->>'due_date')::date,(p_args->>'start_monday')::date,(p_args->>'weekly_due')::numeric,(p_args->>'week_count')::bigint,
      (p_args->>'minimum_earnings_threshold')::numeric,(p_args->>'take_home_floor_override')::numeric,
      p_args->>'reason',p_args->>'note',p_args->>'original_liability_reference',p_args->'preview_token');
    v_output:=pg_catalog.jsonb_build_object('case_id',v_result->'case_id','command_id',v_result->'command_id',
      'sequence',v_result->'sequence','phase',v_result->'phase','replay',v_result->'replay');
    return v_output;
  end if;
  if p_action='RETRY_WORK_JOB' then
    return jsonb_build_object('action',p_action,'result',
      private.bpay_next_retry_work_job_v1(p_actor_user_id,p_args,false));
  end if;
  if p_action is null or p_action not in
      ('PAYE_NET','CASE_PAYOUT','TRANSFER_BEGIN','CANCEL_REQUEST','CASH_REISSUE','REISSUE_CANCEL',
       'CSV_ISSUE','CSV_SETTLEMENT','CSV_RETURN','INTERNAL_SETTLE','PREPARATION_EXPIRE','WRITE_OFF')
     or p_args is null or pg_catalog.jsonb_typeof(p_args) is distinct from 'object'
     or pg_catalog.octet_length(p_args::text)>2048 then
    raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
  end if;
  v_keys:=case p_action
    when 'WRITE_OFF' then array['command_id','case_component_id','expected_component_revision','scope','reason']
      || case when p_args->>'scope'='AMOUNT' then array['amount'] else array[]::text[] end
    when 'PAYE_NET' then array['command_id','run_worker_id','entered_paye_net']
      || case when p_args ? 'expected_projection_revision' then array['expected_projection_revision'] else array[]::text[] end
    when 'CASE_PAYOUT' then array['command_id','run_worker_id','expected_projection_revision']
    when 'TRANSFER_BEGIN' then array['command_id','run_worker_id']
    when 'CANCEL_REQUEST' then array['command_id','run_worker_id']
      || case when p_args ? 'run_id' or p_args ? 'pay_channel'
        then array['run_id','pay_channel'] else array[]::text[] end
    when 'CASH_REISSUE' then array['command_id','return_cash_id']
    when 'REISSUE_CANCEL' then array['command_id','transfer_id']
    when 'CSV_ISSUE' then array['instruction_id','transfer_id']
    when 'INTERNAL_SETTLE' then array['command_id','transfer_id']
    when 'PREPARATION_EXPIRE' then array['command_id','run_id','expected_deadline']
    else array['command_id','transfer_id','receipt_id','amount','occurred_at'] end;
  v_protected:=p_action in ('WRITE_OFF','CANCEL_REQUEST','CASH_REISSUE','REISSUE_CANCEL','CSV_ISSUE','CSV_RETURN');
  if v_protected then
    if p_args ? 'reauth_proof_id' then v_keys:=v_keys||array['reauth_proof_id'];
    elsif p_action<>'CSV_ISSUE' then
      raise exception using errcode='42501',message='BPAY_NEXT_REAUTH_REQUIRED';end if;
  end if;
  if (select count(*) from pg_catalog.jsonb_object_keys(p_args))<>pg_catalog.array_length(v_keys,1)
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_args) k
               where not(k=any(v_keys))) then
    raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
  end if;
  foreach v_key in array v_keys loop
    if pg_catalog.jsonb_typeof(p_args->v_key) is distinct from 'string' then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
    if v_key in ('command_id','run_worker_id','transfer_id','instruction_id','return_cash_id','run_id','case_component_id','reauth_proof_id')
       and (p_args->>v_key)!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
    if v_key='pay_channel' and (p_args->>v_key) not in ('PAYE','UMBRELLA') then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
  end loop;
  v_command_id:=(p_args->>'command_id')::uuid;
  v_worker_id:=(p_args->>'run_worker_id')::uuid;
  v_transfer_id:=(p_args->>'transfer_id')::uuid;
  v_instruction_id:=(p_args->>'instruction_id')::uuid;
  v_return_cash_id:=(p_args->>'return_cash_id')::uuid;
  if p_action='WRITE_OFF' then
    v_case_component_id:=(p_args->>'case_component_id')::uuid;
    if (p_args->>'expected_component_revision')!~'^[1-9][0-9]{0,18}$'
       or (p_args->>'expected_component_revision')::numeric>9223372036854775807 then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
    v_component_revision:=(p_args->>'expected_component_revision')::bigint;
  end if;
  if p_action='PREPARATION_EXPIRE' then
    v_run_id:=(p_args->>'run_id')::uuid;
    if (p_args->>'expected_deadline')!~
        '^[0-9]{4}-[0-9]{2}-[0-9]{2}T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]([.][0-9]{1,6})?(Z|[+-]([01][0-9]|2[0-3]):[0-5][0-9])$' then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
    v_occurred_at:=(p_args->>'expected_deadline')::timestamptz;
    if not pg_catalog.isfinite(v_occurred_at) then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
  end if;
  if p_args ? 'expected_projection_revision' then
    if (p_args->>'expected_projection_revision')!~'^(0|[1-9][0-9]{0,18})$'
       or (p_args->>'expected_projection_revision')::numeric>=9223372036854775807 then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
    v_projection_revision:=(p_args->>'expected_projection_revision')::bigint;
  end if;
  if p_action in ('PAYE_NET','CSV_SETTLEMENT','CSV_RETURN') then
    v_key:=case when p_action='PAYE_NET' then 'entered_paye_net' else 'amount' end;
    -- Current money columns are numeric(18,2). Explicit finite, ordinary
    -- decimal syntax prevents exponent/NaN, narrowing and JSON-number coercion.
    if (p_args->>v_key)!~'^(0|[1-9][0-9]{0,15})([.][0-9]{1,2})?$' then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
    v_amount:=(p_args->>v_key)::numeric;
    if p_action<>'PAYE_NET' and v_amount<=0 then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
  end if;
  if p_action in ('CSV_SETTLEMENT','CSV_RETURN') then
    if pg_catalog.octet_length(p_args->>'receipt_id') not between 1 and 256
       or (p_args->>'occurred_at')!~
         '^[0-9]{4}-[0-9]{2}-[0-9]{2}T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]([.][0-9]{1,6})?(Z|[+-]([01][0-9]|2[0-3]):[0-5][0-9])$' then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
    v_occurred_at:=(p_args->>'occurred_at')::timestamptz;
    if not pg_catalog.isfinite(v_occurred_at) then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
  end if;

  if v_protected then
    -- Shared finite resolver takes the native business locks before recomputing
    -- scope. Same original receipt qualifies consumed replay, never live repricing.
    v_proof_id:=(p_args->>'reauth_proof_id')::uuid;
    v_reauth_scope:=private.bpay_next_reauth_scope_v1(p_actor_user_id,p_action,p_args-'reauth_proof_id',true);
    if not (p_action='CSV_ISSUE' and (v_reauth_scope->>'accepted')::boolean and v_proof_id is null) then
      perform private.bpay_next_reauth_consume_v1(v_proof_id,p_actor_user_id,p_action,
        v_reauth_scope->>'scope_sha256',coalesce(v_command_id,v_instruction_id));
    end if;
  end if;

  -- Fixed branches and exact typed IDs only. No dynamic identifier, query,
  -- caller actor, arbitrary RPC, live finance/history reconstruction or drain.
  case p_action
    when 'WRITE_OFF' then
      v_result:=private.bpay_next_accept_write_off_v1(v_command_id,v_case_component_id,p_actor_user_id,
        v_component_revision,p_args->>'scope',p_args->>'amount',p_args->>'reason');
      v_output:=pg_catalog.jsonb_build_object('command_id',v_result->>'command_id',
        'case_id',v_result->>'case_id','case_component_id',v_result->>'case_component_id',
        'sequence',v_result->>'sequence','phase',v_result->>'phase','requested_scope',v_result->>'requested_scope',
        'applied_amount',v_result->>'applied_amount','remaining_amount',v_result->>'remaining_amount',
        'protected_amount',v_result->>'protected_amount','event_id',v_result->>'event_id',
        'issue_code',v_result->>'issue_code','replay',v_result->'replay');
    when 'PREPARATION_EXPIRE' then
      -- NULL clock selects the real server clock AFTER acquiring the header.
      -- Office cannot supply SYSTEM authority, now, or a renewal duration.
      v_result:=private.bpay_next_accept_preparation_expiry_v1(
        v_command_id,v_run_id,p_actor_user_id,v_occurred_at,null);
      v_output:=pg_catalog.jsonb_build_object(
        'command_id',v_result->>'command_id','run_id',v_result->>'run_id',
        'sequence',v_result->>'sequence','phase',v_result->>'phase',
        'expected_candidates',v_result->>'expected_candidates',
        'completed_candidates',v_result->>'completed_candidates',
        'replay',v_result->'replay');
    when 'PAYE_NET' then
      -- Route by the worker's FROZEN selection, never by live Candidate cases.
      -- Explicit prior projection protects resaves from overwriting a newer one.
      select w.case_selection_revision into strict v_case_selection_revision
        from private.bpay_next_run_worker w where w.id=v_worker_id;
      if v_case_selection_revision>0 then
        if v_projection_revision is null then
          raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
        end if;
        v_result:=private.bpay_next_accept_case_paye_net_v1(
          v_command_id,v_worker_id,p_actor_user_id,v_amount,v_projection_revision);
      else
        if v_projection_revision is not null then
          raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
        end if;
        v_result:=private.bpay_next_accept_simple_paye_net_v1(v_command_id,v_worker_id,v_amount);
      end if;
      v_output:=pg_catalog.jsonb_build_object(
        'command_id',v_command_id,'run_worker_id',v_worker_id,
        'sequence',v_result->>'sequence','request_no',v_result->>'request_no',
        'phase',v_result->>'phase','replay',v_result->'replay');
      if v_case_selection_revision>0 then
        v_output:=v_output||pg_catalog.jsonb_build_object('replacement_id',v_result->>'replacement_id',
          'replacement_stage',v_result->>'replacement_stage','projection_id',v_result->>'projection_id','issue_code',v_result->>'issue_code');
      end if;
    when 'CASE_PAYOUT' then
      v_result:=private.bpay_next_accept_case_payout_projection_v1(
        v_command_id,v_worker_id,p_actor_user_id,v_projection_revision);
      v_output:=pg_catalog.jsonb_build_object(
        'command_id',v_command_id,'run_worker_id',v_worker_id,
        'sequence',v_result->>'sequence','request_no',v_result->>'request_no',
        'phase',v_result->>'phase','replay',v_result->'replay');
      v_output:=v_output||pg_catalog.jsonb_build_object('replacement_id',v_result->>'replacement_id',
        'replacement_stage',v_result->>'replacement_stage','projection_id',v_result->>'projection_id','issue_code',v_result->>'issue_code');
    when 'TRANSFER_BEGIN' then
      v_result:=private.bpay_next_begin_simple_transfer_v1(v_command_id,v_worker_id);
      v_output:=pg_catalog.jsonb_build_object(
        'command_id',v_command_id,'run_worker_id',v_worker_id,
        'sequence',v_result->>'sequence','transfer_id',v_result->>'transfer_id',
        'phase',v_result->>'phase','replay',v_result->'replay');
    when 'CANCEL_REQUEST' then
      -- Optional address is strictly paired and independently bound before accepting intent.
      if p_args ? 'run_id' and not exists(select 1 from private.bpay_next_run_worker w
        where w.id=v_worker_id and w.run_id=(p_args->>'run_id')::uuid
          and w.target_pay_channel=p_args->>'pay_channel') then
        raise exception using errcode='23514',message='BPAY_NEXT_CANCEL_CHANNEL_SCOPE_INVALID';
      end if;
      v_result:=private.bpay_next_accept_simple_cancel_v1(v_command_id,v_worker_id);
      v_output:=pg_catalog.jsonb_build_object(
        'command_id',v_command_id,'run_worker_id',v_worker_id,
        'sequence',v_result->>'sequence','phase',v_result->>'phase',
        'cursor',v_result->>'cursor','released_line_count',v_result->>'released_line_count',
        'blocked_code',v_result->>'blocked_code','replay',v_result->'replay');
    when 'CASH_REISSUE' then
      v_result:=private.bpay_next_receive_simple_reissue_v1(v_command_id,v_return_cash_id,p_actor_user_id);
      select c.agency_sequence::text into strict v_sequence
        from private.bpay_next_command c where c.id=v_command_id and c.command_kind='CASH_REISSUE';
      v_output:=pg_catalog.jsonb_build_object(
        'command_id',v_command_id,'return_cash_id',v_return_cash_id,
        'sequence',v_sequence,'transfer_id',v_result->>'transfer_id',
        'phase',case when v_result->>'transfer_id' is null
          then 'ACCEPTED_PENDING_BUILD' else 'TRANSFER_CREATED' end,
        'replay',v_result->'replay');
    when 'REISSUE_CANCEL' then
      v_result:=private.bpay_next_accept_reissue_cancel_v1(v_command_id,v_transfer_id,p_actor_user_id);
      select r.blocked_code into v_key from private.bpay_next_reissue_cancel_request r where r.command_id=v_command_id;
      v_output:=pg_catalog.jsonb_build_object('command_id',v_command_id,'transfer_id',v_transfer_id,
        'sequence',v_result->>'sequence','phase',v_result->>'phase','blocked_code',v_key,'replay',v_result->'replay');
    when 'CSV_ISSUE' then
      -- A new CSV issue first binds its current approved destination using the
      -- existing owner. Already-issued replay never rebinds or changes bytes.
      -- Both calls use the same transaction and parent run mutex; any issue
      -- permission/state failure rolls the destination preparation back too.
      -- Acquire that mutex BEFORE checking instruction existence: concurrent
      -- same-ID retry must see the first committed issue and skip rebinding.
      select w.run_id into strict v_run_id
        from private.bpay_next_transfer t
        join private.bpay_next_run_worker w on w.id=t.run_worker_id
        where t.id=v_transfer_id;
      perform 1 from private.bpay_next_pay_run where id=v_run_id for update;
      if not exists(select 1 from private.bpay_next_csv_instruction i where i.id=v_instruction_id) then
        perform private.bpay_next_bind_simple_csv_destination_v1(v_transfer_id);
      end if;
      v_result:=private.bpay_next_issue_simple_csv_v1(v_instruction_id,v_transfer_id,p_actor_user_id);
      v_output:=pg_catalog.jsonb_build_object(
        'instruction_id',v_result->>'instruction_id','transfer_id',v_result->>'transfer_id',
        'file_name',v_result->>'file_name','csv_sha256',v_result->>'csv_sha256',
        'csv_text',v_result->>'csv_text','phase','ISSUED_CSV',
        'payment_recorded',v_result->'payment_recorded','replay',v_result->'replay');
    when 'CSV_SETTLEMENT' then
      v_result:=private.bpay_next_receive_simple_csv_settlement_v1(
        v_command_id,v_transfer_id,p_args->>'receipt_id',v_amount,v_occurred_at,p_actor_user_id);
      select c.agency_sequence::text into strict v_sequence
        from private.bpay_next_command c
        where c.id=(v_result->>'command_id')::uuid and c.command_kind='CSV_SETTLEMENT';
      v_output:=pg_catalog.jsonb_build_object(
        'command_id',v_result->>'command_id','transfer_id',v_transfer_id,
        'outcome_id',v_result->>'outcome_id','sequence',v_sequence,
        'posting_complete',v_result->'posting_complete','replay',v_result->'replay',
        'phase',case when (v_result->>'posting_complete')::boolean
          then 'POSTED' else 'ACCEPTED_PENDING_POSTING' end);
    when 'INTERNAL_SETTLE' then
      -- The server creates an internal receipt for an exact zero-cash frozen
      -- envelope. There is no caller amount, bank receipt or CSV instruction.
      v_result:=private.bpay_next_receive_internal_settlement_v1(
        v_command_id,v_transfer_id,p_actor_user_id);
      v_output:=pg_catalog.jsonb_build_object(
        'command_id',v_result->>'command_id','transfer_id',v_result->>'transfer_id',
        'internal_receipt_id',v_result->>'internal_receipt_id','sequence',v_result->>'sequence',
        'posting_complete',v_result->'posting_complete','phase',v_result->>'phase','replay',v_result->'replay');
    when 'CSV_RETURN' then
      v_result:=private.bpay_next_receive_simple_csv_return_v1(
        v_command_id,v_transfer_id,p_args->>'receipt_id',v_amount,v_occurred_at,p_actor_user_id);
      select c.agency_sequence::text into strict v_sequence
        from private.bpay_next_command c
        where c.id=(v_result->>'command_id')::uuid and c.command_kind='CSV_RETURN';
      v_output:=pg_catalog.jsonb_build_object(
        'command_id',v_result->>'command_id','transfer_id',v_transfer_id,
        'outcome_id',v_result->>'outcome_id','sequence',v_sequence,
        'posting_complete',v_result->'posting_complete','replay',v_result->'replay',
        'phase',case when (v_result->>'posting_complete')::boolean
          then 'POSTED' else 'ACCEPTED_PENDING_POSTING' end);
  end case;
  if pg_catalog.jsonb_typeof(v_result->'replay') is distinct from 'boolean'
     or pg_catalog.octet_length(v_output::text)>120000 then
    -- Exception aborts the whole request transaction; no partial issue or
    -- accepted financial request is committed without its bounded receipt.
    raise exception using errcode='54000',message='BPAY_NEXT_COMMAND_RESPONSE_INVALID';
  end if;
  return v_output;
end
$function$;

alter function private.bpay_next_net_replacement_job_assert_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_net_replacement_job_assert_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_net_replacement_guard_v1() owner to postgres;
revoke all on function private.bpay_next_net_replacement_guard_v1() from public,anon,authenticated,service_role;
alter function private.bpay_next_net_staged_result_guard_v1() owner to postgres;
revoke all on function private.bpay_next_net_staged_result_guard_v1() from public,anon,authenticated,service_role;
alter function private.bpay_next_net_staged_capacity_guard_v1() owner to postgres;
revoke all on function private.bpay_next_net_staged_capacity_guard_v1() from public,anon,authenticated,service_role;
alter function private.bpay_next_net_refusal_guard_v1() owner to postgres;
revoke all on function private.bpay_next_net_refusal_guard_v1() from public,anon,authenticated,service_role;
alter function private.bpay_next_net_cancel_hold_member_guard_v1() owner to postgres;
revoke all on function private.bpay_next_net_cancel_hold_member_guard_v1() from public,anon,authenticated,service_role;
alter function private.bpay_next_net_cancel_handoff_guard_v1() owner to postgres;
revoke all on function private.bpay_next_net_cancel_handoff_guard_v1() from public,anon,authenticated,service_role;
alter function private.bpay_next_net_replacement_truncate_guard_v1() owner to postgres;
revoke all on function private.bpay_next_net_replacement_truncate_guard_v1() from public,anon,authenticated,service_role;
alter function private.bpay_next_accept_case_net_request_v1(uuid,uuid,uuid,text,numeric,bigint) owner to postgres;
revoke all on function private.bpay_next_accept_case_net_request_v1(uuid,uuid,uuid,text,numeric,bigint) from public,anon,authenticated,service_role;
alter function private.bpay_next_net_page_prelock_v1(uuid,integer,text) owner to postgres;
revoke all on function private.bpay_next_net_page_prelock_v1(uuid,integer,text) from public,anon,authenticated,service_role;
alter function private.bpay_next_net_prior_run_page_v1(uuid,integer,boolean) owner to postgres;
revoke all on function private.bpay_next_net_prior_run_page_v1(uuid,integer,boolean) from public,anon,authenticated,service_role;
alter function private.bpay_next_validate_case_net_page_v1(uuid,integer) owner to postgres;
revoke all on function private.bpay_next_validate_case_net_page_v1(uuid,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_net_precheck_page_v1(uuid,integer) owner to postgres;
revoke all on function private.bpay_next_net_precheck_page_v1(uuid,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_stage_case_net_page_v1(uuid,integer) owner to postgres;
revoke all on function private.bpay_next_stage_case_net_page_v1(uuid,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_cancel_net_replacement_page_v1(uuid,uuid,integer) owner to postgres;
revoke all on function private.bpay_next_cancel_net_replacement_page_v1(uuid,uuid,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_apply_case_paye_net_page_v1(uuid,uuid,bigint,bigint,integer) owner to postgres;
revoke all on function private.bpay_next_apply_case_paye_net_page_v1(uuid,uuid,bigint,bigint,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_assert_candidate_financial_fence_v1(uuid,uuid,uuid,bigint) owner to postgres;
revoke all on function private.bpay_next_assert_candidate_financial_fence_v1(uuid,uuid,uuid,bigint) from public,anon,authenticated,service_role;
alter function private.bpay_next_net_commit_fence_guard_v1() owner to postgres;
revoke all on function private.bpay_next_net_commit_fence_guard_v1() from public,anon,authenticated,service_role;
alter function private.bpay_next_net_terminal_receipt_guard_v1() owner to postgres;
revoke all on function private.bpay_next_net_terminal_receipt_guard_v1() from public,anon,authenticated,service_role;
alter function private.bpay_next_case_hold_guard_v1() owner to postgres;
revoke all on function private.bpay_next_case_hold_guard_v1() from public,anon,authenticated,service_role;
alter function private.bpay_next_case_capacity_use_guard_v1() owner to postgres;
revoke all on function private.bpay_next_case_capacity_use_guard_v1() from public,anon,authenticated,service_role;
alter function private.bpay_next_case_result_insert_guard_v1() owner to postgres;
revoke all on function private.bpay_next_case_result_insert_guard_v1() from public,anon,authenticated,service_role;
alter function private.bpay_next_accumulate_net_destination_v1(uuid,uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_accumulate_net_destination_v1(uuid,uuid,uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_destination_state_guard_v1() owner to postgres;
revoke all on function private.bpay_next_destination_state_guard_v1() from public,anon,authenticated,service_role;
alter function private.bpay_next_record_case_hold_transition_v1(uuid,uuid,text) owner to postgres;
revoke all on function private.bpay_next_record_case_hold_transition_v1(uuid,uuid,text) from public,anon,authenticated,service_role;
alter function private.bpay_next_claim_simple_cancel_job_v1(uuid,integer) owner to postgres;
revoke all on function private.bpay_next_claim_simple_cancel_job_v1(uuid,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_cancel_case_worker_page_v1(uuid,uuid,bigint,bigint,integer) owner to postgres;
revoke all on function private.bpay_next_cancel_case_worker_page_v1(uuid,uuid,bigint,bigint,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_apply_cancelled_week_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_apply_cancelled_week_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_begin_simple_transfer_v1(uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_begin_simple_transfer_v1(uuid,uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_build_simple_transfer_page_v1(uuid,uuid,bigint,bigint,integer) owner to postgres;
revoke all on function private.bpay_next_build_simple_transfer_page_v1(uuid,uuid,bigint,bigint,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_begin_case_transfer_v1(uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_begin_case_transfer_v1(uuid,uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_original_reservation_v1(uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_original_reservation_v1(uuid,uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_run_channel_contribution_v1(private.bpay_next_run_worker) owner to postgres;
revoke all on function private.bpay_next_run_channel_contribution_v1(private.bpay_next_run_worker) from public,anon,authenticated,service_role;
alter function private.bpay_next_channel_draft_summary_apply_v1(uuid,text,private.bpay_next_run_worker,private.bpay_next_run_worker,uuid,boolean,text,text,text,date,timestamp with time zone,text,boolean) owner to postgres;
revoke all on function private.bpay_next_channel_draft_summary_apply_v1(uuid,text,private.bpay_next_run_worker,private.bpay_next_run_worker,uuid,boolean,text,text,text,date,timestamp with time zone,text,boolean) from public,anon,authenticated,service_role;
alter function private.bpay_next_net_replacement_reader_revision_v1() owner to postgres;
revoke all on function private.bpay_next_net_replacement_reader_revision_v1() from public,anon,authenticated,service_role;
alter function private.bpay_next_ready_net_refusal_guard_v1() owner to postgres;
revoke all on function private.bpay_next_ready_net_refusal_guard_v1() from public,anon,authenticated,service_role;
alter function private.bpay_next_worker_status_page_v1(uuid,uuid,integer,integer) owner to postgres;
revoke all on function private.bpay_next_worker_status_page_v1(uuid,uuid,integer,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_paye_worksheet_page_v1(uuid,uuid,uuid,text,text,jsonb,integer) owner to postgres;
revoke all on function private.bpay_next_paye_worksheet_page_v1(uuid,uuid,uuid,text,text,jsonb,integer) from public,anon,authenticated,service_role;
alter function public.bpay_next_financial_job_v1(text,uuid,text,jsonb) owner to postgres;
revoke all on function public.bpay_next_financial_job_v1(text,uuid,text,jsonb) from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_financial_job_v1(text,uuid,text,jsonb) to service_role;
alter function public.bpay_next_financial_intake_v1(uuid,text,jsonb) owner to postgres;
revoke all on function public.bpay_next_financial_intake_v1(uuid,text,jsonb) from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_financial_intake_v1(uuid,text,jsonb) to service_role;
alter view private.bpay_next_paye_worksheet_rows_v1 owner to postgres;
revoke all on private.bpay_next_paye_worksheet_rows_v1 from public,anon,authenticated,service_role;
notify pgrst,'reload schema';
commit;
