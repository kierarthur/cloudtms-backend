-- Source-only successor. Not registered, installed, or financially accepted.
-- Exactly two existing function definitions; owner/ACL/search_path posture is retained.
-- Apply only inside the root-owned rolled-back host transaction after independent review.

create or replace function private.bpay_next_prepare_case_page_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,
  p_expected_cursor bigint default null,p_limit integer default 100
) returns jsonb
language plpgsql security invoker
set search_path=pg_catalog,private,public
as $function$
declare
  v_epoch bigint;v_run_id uuid;v_candidate uuid;v_saved_cursor bigint;
  v_run private.bpay_next_pay_run%rowtype;
  v_control private.bpay_next_worker_control%rowtype;
  v_job private.bpay_next_job%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_selection private.bpay_next_case_selection%rowtype;
  v_state private.bpay_next_case_allocation_state%rowtype;
  v_channel private.bpay_next_case_allocation_channel%rowtype;
  v_week private.bpay_next_worker_week%rowtype;
  v_case private.bpay_next_finance_case%rowtype;
  v_component private.bpay_next_case_component%rowtype;
  v_rule private.bpay_next_case_rule%rowtype;
  v_period_rule private.bpay_next_case_rule%rowtype;
  v_origin private.bpay_next_case_create_request%rowtype;
  v_collection private.bpay_next_work_collection%rowtype;
  v_policy_id uuid;
  v_cmp private.bpay_next_source_comparison_receipt%rowtype;
  v_position private.bpay_next_position%rowtype;
  v_win private.bpay_next_valuation_policy_window%rowtype;
  v_snapshot jsonb;v_nominal_q jsonb;v_capacity_q jsonb;v_slice jsonb;
  v_source_taken numeric;v_taken_vat numeric;v_taken_inc numeric;v_gross_vat numeric;
  v_resolution_ref text;v_carry jsonb;
  v_automatic boolean;v_week_not_affected boolean;
  v_period private.bpay_next_case_period%rowtype;
  v_item record;v_prior record;v_value record;v_row record;
  v_instruction private.bpay_next_run_case_instruction%rowtype;
  v_floor numeric;v_outstanding numeric;v_case_outstanding numeric;
  v_nominal numeric;v_opening_due numeric;v_capacity numeric;v_taken numeric;v_e numeric;v_h numeric;
  v_next_e numeric;v_next_h numeric;
  v_gross numeric;v_week_start date;v_age timestamptz;v_age_key bigint;
  v_purpose text;v_explanation text;v_issue text;v_seen bigint:=0;
  v_result_id uuid;v_hold_id uuid;v_instruction_id uuid;v_has_more boolean;v_terminal boolean:=false;
  v_cap_reason text;v_affordability_reason text;v_capacity_reason text;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null or p_owner_epoch<1
     or p_expected_cursor<0 or p_limit is null or p_limit not between 1 and 100 then
    raise exception using errcode='22023',message='BPAY_NEXT_CASE_PREPARE_INPUT_INVALID';
  end if;
  select rc.run_id,j.candidate_id into strict v_run_id,v_candidate
    from private.bpay_next_job j join private.bpay_next_run_command rc on rc.command_id=j.command_id
    where j.id=p_job_id and j.job_kind='PREPARE';
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  -- Same lock order as the real lane; root UPDATE fences confirmation/cancel.
  select r.* into strict v_run from private.bpay_next_pay_run r where r.id=v_run_id for update;
  select c.* into strict v_control from private.bpay_next_worker_control c where c.candidate_id=v_candidate for update;
  select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id for update;
  select w.* into strict v_worker from private.bpay_next_run_worker w
    where w.run_id=v_run_id and w.candidate_id=v_candidate for update;
  select s.* into v_state from private.bpay_next_case_allocation_state s where s.job_id=p_job_id
    and s.run_worker_id=v_worker.id and s.pass_kind='DRAFT' and s.projection_no=0 for update;
  if v_job.module_epoch<>v_epoch then
    raise exception using errcode='55000',message='BPAY_NEXT_CASE_PREPARE_OWNER_STALE';
  end if;
  if v_job.status='DONE' and v_job.phase='READY' and v_state.status='COMPLETE'
     and v_worker.status in ('READY','DRAFT','ISSUED','COMPLETE','CANCELLING','CANCELLED') then
    return pg_catalog.jsonb_build_object('done',true,'phase','READY','cursor',null,'rows_visited','0',
      'replay',true,'run_worker_id',v_worker.id,'worker_status',v_worker.status);
  end if;
  if v_job.status='DONE' and v_job.phase='DONE' and v_worker.status='REVIEW'
     and v_worker.review_issue_code in ('CASE_SELECTION_REQUIRED','CASE_TARGET_UNSUPPORTED','CASE_INPUT_UNBOUND',
       'CASE_WEEK_BASIS_UNBOUND','CASE_RETURN_FLOOR_UNBOUND','CASE_SOURCE_PARTITION_REVIEW_REQUIRED','CASE_NO_PAYABLE_AMOUNT') then
    return pg_catalog.jsonb_build_object('done',true,'phase','REVIEW','cursor',null,'rows_visited','0',
      'replay',true,'run_worker_id',v_worker.id,'worker_status',v_worker.status,'issue_code',v_worker.review_issue_code);
  end if;
  if v_run.status<>'PREPARING' or v_run.selection_state<>'SEALED' or v_job.status<>'LEASED'
     or v_job.phase<>'FINANCE_ALLOCATE' or v_job.lease_nonce<>p_lease_nonce or v_job.owner_epoch<>p_owner_epoch
     or v_job.lease_until_utc<=pg_catalog.clock_timestamp() or v_control.active_owner_epoch<>p_owner_epoch
     or v_worker.status<>'PREPARING' or v_worker.review_issue_code is not null
     or v_worker.financial_resolution_count<>0 then
    raise exception using errcode='55000',message='BPAY_NEXT_CASE_PREPARE_STATE_INVALID';
  end if;
  if v_worker.captured_position_count<>v_worker.captured_line_count+v_worker.captured_managed_position_count
     or v_worker.expected_managed_position_count<>v_worker.captured_managed_position_count
     or exists(select 1 from private.bpay_next_run_position where run_worker_id=v_worker.id
       and work_collection_id is not null and not managed_capture_completed) then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_PREPARE_LINE_COUNT_MISMATCH';
  end if;
  if v_job.cursor_key is not null and v_job.cursor_key !~ '^[0-9]{1,19}$' then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_PREPARE_CURSOR_CORRUPT';
  end if;
  v_saved_cursor:=coalesce(v_job.cursor_key::bigint,0);
  if coalesce(p_expected_cursor,0)<v_saved_cursor then
    -- Lost STEP reply: read back the persisted checkpoint without a second
    -- capture, hold, cursor spend or financial-view bump. Ahead is not replay.
    return pg_catalog.jsonb_build_object('done',false,'phase','FINANCE_ALLOCATE','cursor',v_job.cursor_key,
      'rows_visited','0','replay',true,'run_worker_id',v_worker.id,'worker_status',v_worker.status);
  end if;
  if coalesce(p_expected_cursor,0)>v_saved_cursor then
    raise exception using errcode='55000',message='BPAY_NEXT_CASE_PREPARE_CURSOR_STALE';
  end if;
  -- Only a positively proved no-case context may retain the old owner path.
  if v_state.id is null and not exists(select 1 from private.bpay_next_finance_case c where c.candidate_id=v_candidate)
     and not exists(select 1 from private.bpay_next_case_selection s where s.run_id=v_run_id and s.candidate_id=v_candidate) then
    return private.bpay_next_complete_no_case_worker_v1(p_job_id,p_lease_nonce,p_owner_epoch);
  end if;

  <<prepare_page>>
  begin
    if v_state.id is null then
      select s.* into v_selection from private.bpay_next_case_selection s
        where s.run_id=v_run_id and s.candidate_id=v_candidate order by s.selection_revision desc limit 1;
      if v_selection.run_id is null or v_selection.status<>'SEALED' then
        v_issue:='CASE_SELECTION_REQUIRED';exit prepare_page;
      end if;
      if v_worker.target_pay_channel not in ('PAYE','UMBRELLA')
         or v_worker.gross_inc_vat<>v_worker.gross_ex_vat+v_worker.gross_vat
         or (v_worker.target_pay_channel='PAYE' and v_worker.gross_vat<>0)
         or (v_worker.target_pay_channel='UMBRELLA' and (v_worker.target_umbrella_id is null
           or v_worker.target_umbrella_vat_chargeable is null or v_worker.entered_paye_net is not null
           or v_worker.net_request_revision<>0 or v_worker.net_projection_revision<>0)) then
        v_issue:='CASE_TARGET_UNSUPPORTED';exit prepare_page;
      end if;
      select c.min_take_home_wtd into strict v_floor from public.candidates c where c.id=v_candidate for share;
      if v_floor is null or v_floor::text in ('NaN','Infinity','-Infinity') or v_floor<0
         or v_floor<>pg_catalog.trunc(v_floor,2) then
        v_issue:='CASE_INPUT_UNBOUND';exit prepare_page;
      end if;
      v_week_start:=v_run.pay_date-(extract(isodow from v_run.pay_date)::integer-1);
      -- OVERPAYMENT gross recovery does not consume the protected-debt H/floor
      -- operand (0290). Prove the exact nonempty SEALED selected set, not merely
      -- that no protected row happened to be found. Missing identities/counts
      -- remain week-dependent; instruction capture and allocation re-prove it.
      select count(*)=v_selection.selected_count and count(*)>0
        and coalesce(bool_and((fc.case_kind,cc.case_kind,cc.instruction_kind,cc.payroll_stage)
          is not distinct from ('OVERPAYMENT','OVERPAYMENT','RECOVERY','GROSS_DEDUCT')),false)
        into v_week_not_affected
        from private.bpay_next_case_selection_item si
        left join private.bpay_next_case_component cc
          on cc.id=si.case_component_id and cc.case_id=si.case_id and cc.candidate_id=si.candidate_id
        left join private.bpay_next_finance_case fc on fc.id=si.case_id and fc.candidate_id=si.candidate_id
        where si.run_id=v_selection.run_id and si.candidate_id=v_selection.candidate_id
          and si.selection_revision=v_selection.selection_revision and si.is_selected;
      -- Exact indexed metadata absence remains mandatory for every other
      -- selection. No missing prior NEW fact is made zero or fabricated here.
      if not v_week_not_affected and exists(select 1 from private.bpay_next_run_worker w
        join private.bpay_next_pay_run r on r.id=w.run_id
        where w.candidate_id=v_candidate and w.status in ('READY','DRAFT','ISSUED','COMPLETE')
          and r.pay_date>=v_week_start and r.pay_date<v_week_start+7
          and (r.pay_date,r.created_at_utc,w.id)<(v_run.pay_date,v_run.created_at_utc,v_worker.id)
          and not exists(select 1 from private.bpay_next_worker_week_contribution x where x.original_run_worker_id=w.id)) then
        v_issue:='CASE_WEEK_BASIS_UNBOUND';exit prepare_page;
      end if;
      insert into private.bpay_next_worker_week(candidate_id,pay_week_start,period_revision,
        default_floor,default_floor_revision,resolved_arranged_take_home)
        values(v_candidate,v_week_start,1,v_floor,1,0) on conflict(candidate_id,pay_week_start) do nothing;
      select w.* into strict v_week from private.bpay_next_worker_week w
        where w.candidate_id=v_candidate and w.pay_week_start=v_week_start for update;
      if not v_week_not_affected and v_week.unresolved_contribution_count<>0 then v_issue:='CASE_WEEK_BASIS_UNBOUND';exit prepare_page;end if;
      if v_week.default_floor<>v_floor then
        update private.bpay_next_worker_week set default_floor=v_floor,default_floor_revision=default_floor_revision+1,
          period_revision=period_revision+1 where candidate_id=v_candidate and pay_week_start=v_week_start returning * into v_week;
      end if;
      update private.bpay_next_run_worker set case_selection_revision=v_selection.selection_revision where id=v_worker.id;
      v_worker.case_selection_revision:=v_selection.selection_revision;
      insert into private.bpay_next_case_allocation_state
        (run_worker_id,run_id,candidate_id,selection_revision,preparation_revision,pass_kind,projection_no,
         command_id,job_id,module_epoch,owner_epoch,financial_view_revision,status,weekly_binding_state,
         expected_instruction_count,initial_worker_take_home,remaining_worker_take_home,prepare_stage,
         captured_default_floor,captured_default_floor_revision,pay_week_start,captured_week_revision,captured_work_gross)
        values(v_worker.id,v_run_id,v_candidate,v_selection.selection_revision,v_worker.preparation_revision,'DRAFT',0,
         v_job.command_id,p_job_id,v_epoch,p_owner_epoch,v_control.financial_view_revision,'BUILDING',
         case when v_week_not_affected then 'NOT_AFFECTED' else 'UNBOUND' end,
         v_selection.selected_count,v_worker.gross_ex_vat,v_worker.gross_ex_vat,
         case when v_week_not_affected then 'INSTRUCTION_CAPTURE' else 'WEEK_CAPTURE' end,
         v_floor,v_week.default_floor_revision,v_week_start,v_week.period_revision,v_worker.gross_ex_vat)
        returning * into v_state;
      update private.bpay_next_case_allocation_state set captured_work_vat=v_worker.gross_vat where id=v_state.id;
      v_state.captured_work_vat:=v_worker.gross_vat;
      insert into private.bpay_next_case_allocation_channel(state_id,allocation_channel,initial_headroom,remaining_headroom)
        values(v_state.id,v_worker.target_pay_channel,v_worker.gross_ex_vat,v_worker.gross_ex_vat);
    else
      if v_state.status<>'BUILDING' or v_state.preparation_revision<>v_worker.preparation_revision
         or v_state.selection_revision<>v_worker.case_selection_revision or v_state.prepare_stage='NOT_CASE_PREPARE' then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_PREPARE_BINDING_MISMATCH';
      end if;
      -- Lease takeover does not reset the captured economic cursors/headroom.
      if v_state.owner_epoch<>p_owner_epoch then
        update private.bpay_next_case_allocation_state set owner_epoch=p_owner_epoch where id=v_state.id;
        v_state.owner_epoch:=p_owner_epoch;
      end if;
    end if;

    if v_state.prepare_stage='WEEK_CAPTURE' then
      for v_prior in select x.* from private.bpay_next_worker_week_contribution x
        where x.candidate_id=v_candidate and x.pay_week_start=v_state.pay_week_start
          and (x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id)
            <(v_run.pay_date,v_run.created_at_utc,v_worker.id)
          and (v_state.prior_worker_cursor is null or
            (x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id)
              >(v_state.prior_pay_date_cursor,v_state.prior_created_at_cursor,v_state.prior_worker_cursor))
        order by x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id limit p_limit
      loop
        v_seen:=v_seen+1;
        if v_prior.eligibility_state='UNBOUND' then
          v_issue:=case when v_prior.payment_state in ('RETURNED_OWED','REISSUED_PAID')
            then 'CASE_RETURN_FLOOR_UNBOUND' else 'CASE_WEEK_BASIS_UNBOUND' end;
          exit prepare_page;
        end if;
        v_state.captured_prior_take_home:=v_state.captured_prior_take_home+v_prior.eligible_arranged_amount;
        v_state.initial_worker_take_home:=v_state.initial_worker_take_home+v_prior.eligible_arranged_amount;
        v_state.remaining_worker_take_home:=v_state.remaining_worker_take_home+v_prior.eligible_arranged_amount;
        v_state.prior_pay_date_cursor:=v_prior.original_pay_date;
        v_state.prior_created_at_cursor:=v_prior.original_created_at_utc;
        v_state.prior_worker_cursor:=v_prior.original_run_worker_id;
      end loop;
      select exists(select 1 from private.bpay_next_worker_week_contribution x
        where x.candidate_id=v_candidate and x.pay_week_start=v_state.pay_week_start
          and (x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id)<(v_run.pay_date,v_run.created_at_utc,v_worker.id)
          and (v_state.prior_worker_cursor is null or (x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id)
            >(v_state.prior_pay_date_cursor,v_state.prior_created_at_cursor,v_state.prior_worker_cursor))) into v_has_more;
      update private.bpay_next_case_allocation_state set captured_prior_take_home=v_state.captured_prior_take_home,
        initial_worker_take_home=v_state.initial_worker_take_home,remaining_worker_take_home=v_state.remaining_worker_take_home,
        prior_pay_date_cursor=v_state.prior_pay_date_cursor,prior_created_at_cursor=v_state.prior_created_at_cursor,
        prior_worker_cursor=v_state.prior_worker_cursor,weekly_binding_state=case when v_has_more then 'UNBOUND' else 'BOUND' end,
        prepare_stage=case when v_has_more then 'WEEK_CAPTURE' else 'INSTRUCTION_CAPTURE' end where id=v_state.id;
      exit prepare_page;
    end if;

    if v_state.prepare_stage='INSTRUCTION_CAPTURE' then
      for v_item in select i.* from private.bpay_next_case_selection_item i
        where i.run_id=v_run_id and i.candidate_id=v_candidate and i.selection_revision=v_state.selection_revision
          and i.is_selected and i.selection_no>v_state.instruction_capture_cursor order by i.selection_no limit p_limit
      loop
        v_seen:=v_seen+1;
        v_cmp:=null;v_position:=null;
        select c.* into v_collection from private.bpay_next_work_collection c
          where c.case_id=v_item.case_id and c.case_component_id=v_item.case_component_id and c.candidate_id=v_candidate;
        if v_collection.id is not null then
          select p.* into strict v_position from private.bpay_next_position p
            where p.work_id=v_collection.work_id and p.component_key=v_collection.component_key for update;
          if v_position.source_comparison_receipt_id is not null then
            select r.* into strict v_cmp from private.bpay_next_source_comparison_receipt r where r.id=v_position.source_comparison_receipt_id;
            v_cmp:=private.bpay_next_source_comparison_current_v2(v_cmp.source_binding_version_id,
              v_position.source_book_revision,v_position.source_comparison_receipt_id);
            if v_cmp.target_pay_channel<>v_worker.target_pay_channel
              or (v_cmp.target_pay_channel='UMBRELLA' and v_cmp.umbrella_vat_chargeable is distinct from v_worker.target_umbrella_vat_chargeable)
              or v_cmp.case_resolution is null
              or v_cmp.case_component_id<>v_item.case_component_id then
              v_issue:='CASE_INPUT_UNBOUND';exit prepare_page;end if;
          end if;
        end if;
        select c.* into strict v_case from private.bpay_next_finance_case c where c.id=v_item.case_id and c.candidate_id=v_candidate for update;
        select c.* into strict v_component from private.bpay_next_case_component c
          where c.id=v_item.case_component_id and c.case_id=v_case.id and c.candidate_id=v_candidate for update;
        select r.* into v_rule from private.bpay_next_case_rule r where r.id=v_case.current_rule_id
          and r.case_id=v_case.id and r.candidate_id=v_candidate and r.rule_revision=v_case.current_rule_revision;
        select r.* into v_origin from private.bpay_next_case_create_request r where r.case_id=v_case.id and r.candidate_id=v_candidate;
        v_collection:=private.bpay_next_work_collection_origin_v1(v_case.id,v_component.id,v_candidate);
        v_automatic:=v_collection.id is not null;
        if v_state.weekly_binding_state='NOT_AFFECTED'
          and (v_case.case_kind,v_component.case_kind,v_component.instruction_kind,v_component.payroll_stage)
            is distinct from ('OVERPAYMENT','OVERPAYMENT','RECOVERY','GROSS_DEDUCT') then
          v_issue:='CASE_WEEK_BASIS_UNBOUND';exit prepare_page;
        end if;
        if v_rule.id is null or v_case.case_revision<1 or v_component.rule_id<>v_rule.id then
          v_issue:='CASE_INPUT_UNBOUND';exit prepare_page;
        end if;
        if v_automatic then
          -- Genuine paid WORK origin, not a fabricated CASE_CREATE receipt.
          -- The immutable original paid policy and the exact applied position
          -- are authority; a newer queued WORK approval is not a reprice.
          if v_origin.command_id is not null then v_issue:='CASE_INPUT_UNBOUND';exit prepare_page;end if;
          v_policy_id:=case when v_cmp.id is null then v_collection.original_valuation_policy_id else v_cmp.comparison_policy_id end;
        elsif v_origin.command_id is null
           or v_component.rule_id<>v_rule.id or v_origin.case_kind<>v_case.case_kind
           or v_origin.source_pay_channel<>v_component.source_pay_channel
           or v_origin.principal_source_ex_vat<>v_component.approved_source_ex_vat
           or (v_component.id<>v_origin.primary_component_id
             and (v_origin.recovery_component_id is null or v_component.id<>v_origin.recovery_component_id))
           or (v_component.id=v_origin.primary_component_id and v_component.instruction_kind<>
             case when v_case.case_kind in ('LOAN','ADVANCE') then 'PAYOUT'
               when v_case.case_kind='CREDIT' then 'CREDIT' else 'RECOVERY' end)
           or (v_component.id=v_origin.recovery_component_id and v_component.instruction_kind<>'RECOVERY') then
          v_issue:='CASE_INPUT_UNBOUND';exit prepare_page;
        else
          v_policy_id:=v_origin.valuation_policy_id;
        end if;
        if v_component.source_pay_channel not in ('PAYE','UMBRELLA')
          or ((v_component.source_pay_channel='UMBRELLA' or v_worker.target_pay_channel='UMBRELLA')
            and v_component.tax_treatment<>'TAXABLE') then
          v_issue:='CASE_TARGET_UNSUPPORTED';exit prepare_page;
        end if;
        v_explanation:='Exact approved case; same-channel PAYE current basis captured';
        v_outstanding:=case when v_component.case_kind in ('LOAN','ADVANCE') then v_component.funded_source_ex_vat
          else v_component.approved_source_ex_vat end-v_component.recovered_source_ex_vat-v_component.written_off_source_ex_vat;
        v_case_outstanding:=case when v_case.case_kind in ('LOAN','ADVANCE') then v_case.principal_funded
          else v_case.principal_approved end-v_case.principal_recovered-v_case.principal_written_off-v_case.principal_paid_credit;
        if v_component.instruction_kind='RECOVERY' then
          if v_automatic then
            -- Retained automatic OVERPAYMENT policy has no instalment. Its
            -- remaining principal already excludes realised recovery and W;
            -- capacity below subtracts each disjoint active hold only once.
            v_nominal:=least(v_outstanding,v_case_outstanding);
            v_explanation:='Exact original ordinary PAYE collection; remaining outstanding, no instalment';
          else
          if v_rule.weekly_due_source_ex_vat is null then v_issue:='CASE_INPUT_UNBOUND';exit prepare_page;end if;
          if v_case.case_kind in ('LOAN','ADVANCE') and v_case.principal_funded>0
             and (v_rule.original_funding_event_id is null or v_rule.original_funded_at_utc is null) then
            v_issue:='CASE_INPUT_UNBOUND';exit prepare_page;
          end if;
          v_nominal:=case when v_state.pay_week_start>=v_rule.next_due_monday
            then least(v_rule.weekly_due_source_ex_vat,v_outstanding,v_case_outstanding) else 0 end;
          if v_case.case_kind in ('LOAN','ADVANCE') and v_case.principal_funded=0 then
            v_nominal:=0;v_explanation:='Approved loan/advance is unfunded; repayment not eligible';
          elsif v_state.pay_week_start<v_rule.next_due_monday then
            v_explanation:='Repayment schedule is not yet due in the pay-date week';
          end if;
          end if;
          v_purpose:=case when v_component.payroll_stage='GROSS_DEDUCT' then 'GROSS_RECOVERY' else 'NET_RECOVERY_CAPACITY' end;
        else
          v_outstanding:=v_component.approved_source_ex_vat-v_component.funded_source_ex_vat
            -v_component.paid_credit_source_ex_vat-v_component.written_off_source_ex_vat;
          v_case_outstanding:=v_case.principal_approved-v_case.principal_funded
            -v_case.principal_paid_credit-v_case.principal_written_off;
          v_nominal:=least(v_outstanding,v_case_outstanding);v_purpose:='PAYOUT';
        end if;
        -- Weekly due exists independently of this particular arrangement's
        -- eligibility. An early/paused selection must not make the whole
        -- week's legitimate opening due permanently zero for later pay dates.
        v_opening_due:=case when v_component.instruction_kind='RECOVERY' then v_nominal else 0 end;
        if v_case.status<>'OPEN' or (v_case.due_date is not null and v_case.due_date>v_run.pay_date) then
          v_nominal:=0;v_explanation:='Selected case is not currently open/due; no reservation';
        end if;
        if v_outstanding<0 or v_case_outstanding<0 or v_nominal<0 then
          v_issue:='CASE_INPUT_UNBOUND';exit prepare_page;
        end if;
        -- Opening due is independent of later principal. Never subtract the
        -- same realised recovery twice or use max(overlapping counters).
        insert into private.bpay_next_case_period(case_component_id,case_id,candidate_id,pay_week_start,rule_id,
          period_revision,opening_outstanding_source_ex_vat,opening_due_source_ex_vat,work_collection_id)
          values(v_component.id,v_case.id,v_candidate,v_state.pay_week_start,v_rule.id,1,v_outstanding,
            v_opening_due,case when v_automatic then v_collection.id else null end)
          on conflict(case_component_id,pay_week_start) do nothing;
        select p.* into strict v_period from private.bpay_next_case_period p
          where p.case_component_id=v_component.id and p.pay_week_start=v_state.pay_week_start for update;
        if v_period.work_collection_id is distinct from (case when v_automatic then v_collection.id else null end) then
          v_issue:='CASE_INPUT_UNBOUND';exit prepare_page;
        end if;
        if v_period.rule_id<>v_rule.id then
          -- Genuine original funding advances an immutable loan rule solely
          -- to bind its age/evidence. It is NOT a schedule/policy reprice.
          select * into strict v_period_rule from private.bpay_next_case_rule where id=v_period.rule_id;
          if v_case.case_kind not in ('LOAN','ADVANCE') or v_rule.original_funding_event_id is null
             or (v_period_rule.case_id,v_period_rule.candidate_id,v_period_rule.case_kind,v_period_rule.case_subtype,
               v_period_rule.tax_treatment,v_period_rule.case_created_at_utc,v_period_rule.minimum_earnings_threshold,
               v_period_rule.take_home_floor_override,v_period_rule.weekly_due_source_ex_vat,v_period_rule.schedule_start_monday,
               v_period_rule.next_due_monday,v_period_rule.schedule_week_count)
               is distinct from (v_rule.case_id,v_rule.candidate_id,v_rule.case_kind,v_rule.case_subtype,
                 v_rule.tax_treatment,v_rule.case_created_at_utc,v_rule.minimum_earnings_threshold,v_rule.take_home_floor_override,
                 v_rule.weekly_due_source_ex_vat,v_rule.schedule_start_monday,v_rule.next_due_monday,v_rule.schedule_week_count)
             or not exists(select 1 from private.bpay_next_case_event e where e.id=v_rule.original_funding_event_id
               and e.case_id=v_case.id and e.event_kind='FUNDED' and e.occurred_at_utc=v_rule.original_funded_at_utc)
             or (v_period_rule.original_funding_event_id is not null and
               (v_period_rule.original_funding_event_id,v_period_rule.original_funded_at_utc)
                 is distinct from (v_rule.original_funding_event_id,v_rule.original_funded_at_utc)) then
            v_issue:='CASE_INPUT_UNBOUND';exit prepare_page;
          end if;
          if v_component.instruction_kind='RECOVERY' and v_period_rule.original_funding_event_id is null then
            if (v_period.opening_outstanding_source_ex_vat,v_period.opening_due_source_ex_vat,
                v_period.realised_recovery_source_ex_vat,v_period.active_unrealised_recovery_source_ex_vat,v_period.active_payout_source_ex_vat)
                is distinct from (0::numeric,0::numeric,0::numeric,0::numeric,0::numeric) then
              v_issue:='CASE_INPUT_UNBOUND';exit prepare_page;
            end if;
            -- Only this exact zero/unconsumed pay-week is made ready. Its
            -- frozen old zero instructions/results remain immutable and zero.
            update private.bpay_next_case_period set rule_id=v_rule.id,opening_outstanding_source_ex_vat=v_outstanding,
              opening_due_source_ex_vat=v_opening_due,period_revision=period_revision+1
              where case_component_id=v_component.id and pay_week_start=v_state.pay_week_start returning * into v_period;
          end if;
          -- A consumed funded period (or original payout period) retains its
          -- recorded opening rule and counters; only the new instruction's
          -- captured_rule_id/age advance to the genuine current funded rule.
        end if;
        v_capacity:=case when v_automatic then
          least(v_nominal,greatest(v_outstanding-v_component.active_recovery_source_ex_vat,0),
            greatest(v_case_outstanding-v_case.active_recovery_hold_amount,0))
          when v_component.instruction_kind='RECOVERY' then
          least(v_nominal,greatest(v_period.opening_due_source_ex_vat-v_period.realised_recovery_source_ex_vat
            -v_period.active_unrealised_recovery_source_ex_vat,0),
            greatest(v_outstanding-v_component.active_recovery_source_ex_vat,0),
            greatest(v_case_outstanding-v_case.active_recovery_hold_amount,0))
          else least(v_nominal,greatest(v_outstanding-v_component.active_payout_source_ex_vat,0),
            greatest(v_case_outstanding-v_case.active_payout_hold_amount,0)) end;
        if v_cmp.id is not null then
          select w.* into strict v_win from private.bpay_next_valuation_policy_window w
            where w.policy_id=v_cmp.comparison_policy_id and w.source_window_id=v_cmp.policy_window_id;
          v_snapshot:=jsonb_build_object('selected_mode',v_cmp.case_resolution->>'selected_mode',
            'source_pay_channel',v_component.source_pay_channel,'target_pay_channel',v_worker.target_pay_channel,
            'source_basis_ex_vat',(v_cmp.case_resolution->>'source_outstanding_ex_vat')::numeric,
            'selected_quote',v_cmp.case_resolution->'selected_quote','comparison_policy_id',v_cmp.comparison_policy_id,
            'policy_window_id',v_cmp.policy_window_id,'effective_pay_date',v_cmp.effective_pay_date,
            'erni_pct',v_win.erni_pct,'vat_rate_pct',v_win.vat_rate_pct,'umbrella_vat_chargeable',v_cmp.umbrella_vat_chargeable);
          v_nominal:=least(v_nominal,(v_snapshot->>'source_basis_ex_vat')::numeric);
          v_capacity:=least(v_capacity,v_nominal);
          v_resolution_ref:='SOURCE_COMPARISON:'||v_cmp.id::text;
          -- Capturing an instruction does not mutate the accepted Case basis.
          -- Only the actual hold operation below advances SOURCE J/revisions.
        else
          begin
            select x.* into strict v_value from private.bpay_next_value_source_for_target_v1(
              v_component.approved_source_ex_vat,v_component.source_pay_channel,v_worker.target_pay_channel,
              v_worker.target_umbrella_vat_chargeable,v_policy_id,v_run.pay_date) x;
          exception when check_violation then
            if sqlerrm in ('BPAY_NEXT_TARGET_POLICY_WINDOW_MISSING','BPAY_NEXT_TARGET_POLICY_WINDOW_AMBIGUOUS') then
              v_issue:='CASE_INPUT_UNBOUND';exit prepare_page;else raise;end if;
          end;
          select w.* into strict v_win from private.bpay_next_valuation_policy_window w
            where w.policy_id=v_policy_id and w.source_window_id=v_value.policy_window_id;
          v_snapshot:=jsonb_build_object('selected_mode','SUGGESTED_EQUIVALENT_BASIS',
            'source_pay_channel',v_component.source_pay_channel,'target_pay_channel',v_worker.target_pay_channel,
            'source_basis_ex_vat',v_component.approved_source_ex_vat,
            'selected_quote',private.pay_finance_selected_quote_v1('SUGGESTED_EQUIVALENT_BASIS',
              v_component.approved_source_ex_vat,v_component.source_pay_channel,v_worker.target_pay_channel,
              null,null,null,v_win.erni_pct,v_win.vat_rate_pct,v_worker.target_umbrella_vat_chargeable),
            'comparison_policy_id',v_policy_id,'policy_window_id',v_value.policy_window_id,'effective_pay_date',v_run.pay_date,
            'erni_pct',v_win.erni_pct,'vat_rate_pct',v_win.vat_rate_pct,
            'umbrella_vat_chargeable',v_worker.target_umbrella_vat_chargeable);
          v_resolution_ref:=case when v_automatic then 'WORK_COLLECTION:'||v_collection.id::text||':'||v_worker.target_pay_channel
            else 'CASE_CREATE:'||v_origin.command_id::text||':'||v_worker.target_pay_channel end;
          update private.bpay_next_case_component set resolution_state='RESOLVED',target_pay_channel=v_worker.target_pay_channel,
            target_ex_vat=v_value.target_ex_vat,target_vat=v_value.target_vat,target_inc_vat=v_value.target_inc_vat,
            valuation_policy_id=v_policy_id,valuation_window_id=v_value.policy_window_id,resolution_ref=v_resolution_ref,
            component_revision=component_revision+1,updated_at_utc=pg_catalog.transaction_timestamp()
            where id=v_component.id returning * into v_component;
        end if;
        v_nominal_q:=private.bpay_next_case_quote_from_snapshot_v2(v_snapshot,v_nominal);
        v_capacity_q:=private.bpay_next_case_quote_from_snapshot_v2(v_snapshot,v_capacity);
        v_age:=coalesce(v_rule.original_funded_at_utc,v_rule.case_created_at_utc);
        -- A genuinely bound stored credit keeps its original creator age. The
        -- exact point sidecar is immutable, not a live legacy money read.
        if not v_automatic then
          select coalesce(o.original_created_at_utc,v_age) into v_age
            from (select 1) s left join private.bpay_next_stored_credit_origin o
              on o.command_id=v_component.id and o.candidate_id=v_candidate;
        end if;
        v_age_key:=(extract(epoch from v_age)*1000000)::bigint;
        insert into private.bpay_next_run_case_instruction
          (run_worker_id,run_id,candidate_id,selection_revision,preparation_revision,case_component_id,case_id,
           captured_case_revision,captured_component_revision,captured_period_revision,rule_id,rule_revision,
           case_kind,case_subtype,tax_treatment,instruction_kind,direction,payroll_stage,hold_purpose,
           age_key,debt_age_at_utc,component_ordinal,pay_week_start,source_pay_channel,target_pay_channel,allocation_channel,currency,
           nominal_source_ex_vat,nominal_target_ex_vat,nominal_target_vat,nominal_target_inc_vat,
           capacity_source_ex_vat,capacity_target_ex_vat,capacity_target_vat,capacity_target_inc_vat,
           minimum_earnings_threshold,take_home_floor_override,captured_default_floor,captured_default_floor_revision,
           resolved_take_home_floor,valuation_policy_id,valuation_window_id,resolution_ref,explanation,work_collection_id,
           selected_quote_snapshot,source_comparison_receipt_id,source_binding_version_id,canonical_source_work_id,
           canonical_source_component_key,source_approval_work_id,source_approval_revision_id,source_approval_component_key,
           source_approval_state,source_approved_line_id)
          values(v_worker.id,v_run_id,v_candidate,v_state.selection_revision,v_worker.preparation_revision,v_component.id,v_case.id,
           v_case.case_revision,v_component.component_revision,v_period.period_revision,v_rule.id,v_rule.rule_revision,
           v_component.case_kind,v_component.case_subtype,v_component.tax_treatment,v_component.instruction_kind,
           v_component.direction,v_component.payroll_stage,v_purpose,v_age_key,v_age,v_component.component_ordinal,
           v_state.pay_week_start,v_component.source_pay_channel,v_worker.target_pay_channel,v_worker.target_pay_channel,'GBP',
           v_nominal,(v_nominal_q->>'ex')::numeric,(v_nominal_q->>'vat')::numeric,(v_nominal_q->>'inc')::numeric,
           v_capacity,(v_capacity_q->>'ex')::numeric,(v_capacity_q->>'vat')::numeric,(v_capacity_q->>'inc')::numeric,
           v_rule.minimum_earnings_threshold,v_rule.take_home_floor_override,v_state.captured_default_floor,
           v_state.captured_default_floor_revision,coalesce(v_rule.take_home_floor_override,v_state.captured_default_floor),
           v_policy_id,(v_snapshot->>'policy_window_id')::uuid,v_resolution_ref,v_explanation,
           case when v_automatic then v_collection.id else null end,v_snapshot,
           v_cmp.id,v_cmp.source_binding_version_id,v_cmp.canonical_work_id,v_cmp.canonical_component_key,
           v_cmp.approval_work_id,v_cmp.approval_revision_id,v_cmp.approval_component_key,
           case when v_cmp.id is null then null else (select b.component_state from private.bpay_next_source_component_binding_version b
             where b.id=v_cmp.source_binding_version_id) end,v_cmp.approved_line_id)
          returning id into v_instruction_id;
        perform private.bpay_next_capture_instruction_destination_v1(v_instruction_id);
        v_state.instruction_capture_cursor:=v_item.selection_no;
        v_state.captured_instruction_count:=v_state.captured_instruction_count+1;
        if v_component.payroll_stage='NET_ADD' then v_state.payout_instruction_count:=v_state.payout_instruction_count+1;end if;
        update private.bpay_next_run_worker set case_instruction_count=case_instruction_count+1 where id=v_worker.id;
      end loop;
      select exists(select 1 from private.bpay_next_case_selection_item i where i.run_id=v_run_id
        and i.candidate_id=v_candidate and i.selection_revision=v_state.selection_revision and i.is_selected
        and i.selection_no>v_state.instruction_capture_cursor) into v_has_more;
      if not v_has_more and v_state.captured_instruction_count<>v_state.expected_instruction_count then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_PREPARE_CAPTURE_COUNT_MISMATCH';
      end if;
      update private.bpay_next_case_allocation_state set instruction_capture_cursor=v_state.instruction_capture_cursor,
        captured_instruction_count=v_state.captured_instruction_count,payout_instruction_count=v_state.payout_instruction_count,
        prepare_stage=case when v_has_more then 'INSTRUCTION_CAPTURE' else 'ALLOCATE' end where id=v_state.id;
      exit prepare_page;
    end if;

    if v_state.weekly_binding_state='NOT_AFFECTED' then
      -- Only frozen instructions are authoritative from this point. This
      -- scalar metadata proof is scoped to this exact run worker/revisions;
      -- it never reconstructs money or reads prior payment history.
      select count(*)=v_state.expected_instruction_count and count(*)>0
        and coalesce(bool_and((i.case_kind,i.instruction_kind,i.payroll_stage,i.hold_purpose)
          is not distinct from ('OVERPAYMENT','RECOVERY','GROSS_DEDUCT','GROSS_RECOVERY')
          and si.case_id is not null),false)
        into v_week_not_affected
        from private.bpay_next_run_case_instruction i
        left join private.bpay_next_case_selection_item si
          on si.run_id=i.run_id and si.candidate_id=i.candidate_id and si.selection_revision=i.selection_revision
            and si.case_id=i.case_id and si.case_component_id=i.case_component_id and si.is_selected
        where i.run_worker_id=v_worker.id and i.run_id=v_run_id and i.candidate_id=v_candidate
          and i.preparation_revision=v_worker.preparation_revision and i.selection_revision=v_state.selection_revision;
      if not v_week_not_affected
        or v_state.captured_instruction_count<>v_state.expected_instruction_count
        or v_worker.case_instruction_count<>v_state.expected_instruction_count
        or not exists(select 1 from private.bpay_next_case_selection cs where cs.run_id=v_run_id
          and cs.candidate_id=v_candidate and cs.selection_revision=v_state.selection_revision
          and cs.status='SEALED' and cs.selected_count=v_state.expected_instruction_count) then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_PREPARE_ALLOCATION_NOT_BOUND';
      end if;
    end if;
    if v_state.prepare_stage<>'ALLOCATE' or v_state.weekly_binding_state not in ('BOUND','NOT_AFFECTED') then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_PREPARE_ALLOCATION_NOT_BOUND';
    end if;
    select c.* into strict v_channel from private.bpay_next_case_allocation_channel c
      where c.state_id=v_state.id and c.allocation_channel=v_worker.target_pay_channel for update;
    v_e:=v_channel.remaining_headroom;v_h:=v_state.remaining_worker_take_home;
    for v_instruction in select i.* from private.bpay_next_run_case_instruction i
      where i.run_worker_id=v_worker.id and i.preparation_revision=v_worker.preparation_revision
        and i.selection_revision=v_state.selection_revision
        and (v_state.cursor_case_component_id is null or (i.age_key,i.case_id,i.component_ordinal,i.case_component_id)
          >(v_state.cursor_age_key,v_state.cursor_case_id,v_state.cursor_component_ordinal,v_state.cursor_case_component_id))
      order by i.age_key,i.case_id,i.component_ordinal,i.case_component_id limit p_limit
    loop
      v_seen:=v_seen+1;
      if v_instruction.hold_purpose='PAYOUT' then
        v_taken:=least(v_instruction.nominal_target_ex_vat,v_instruction.capacity_target_ex_vat);
        v_cap_reason:=null;v_affordability_reason:=null;
        v_capacity_reason:=case when v_instruction.capacity_target_ex_vat<v_instruction.nominal_target_ex_vat then 'CASE_CAPACITY' else null end;
        v_next_e:=v_e;v_next_h:=v_h;
      else
        select x.* into strict v_row from private.bpay_next_case_allocation_row_v1(v_instruction.case_kind,
          v_instruction.nominal_target_ex_vat,v_instruction.capacity_target_ex_vat,v_e,v_h,
          v_instruction.minimum_earnings_threshold,v_instruction.take_home_floor_override,v_instruction.captured_default_floor) x;
        v_taken:=v_row.taken_amount;v_cap_reason:=v_row.cap_reason;
        v_affordability_reason:=v_row.affordability_reason;v_capacity_reason:=v_row.capacity_reason;
        v_next_e:=v_row.next_channel_headroom;v_next_h:=v_row.next_worker_take_home;
      end if;
      begin
        v_slice:=private.bpay_next_case_source_slice_v2(v_instruction.selected_quote_snapshot,v_taken,v_instruction.capacity_source_ex_vat);
      exception when sqlstate '55000' then
        if sqlerrm='BPAY_NEXT_CASE_SOURCE_PARTITION_REVIEW_REQUIRED' then
          v_issue:='CASE_SOURCE_PARTITION_REVIEW_REQUIRED';exit prepare_page;else raise;end if;
      end;
      v_source_taken:=(v_slice->>'source')::numeric;v_taken_vat:=(v_slice->>'vat')::numeric;v_taken_inc:=(v_slice->>'inc')::numeric;
      insert into private.bpay_next_case_allocation_result
        (state_id,instruction_id,run_worker_id,candidate_id,preparation_revision,selection_revision,result_no,
         pass_kind,allocation_channel,hold_purpose,nominal_target_ex_vat,usable_capacity_target_ex_vat,
         allocated_source_ex_vat,allocated_target_ex_vat,allocated_target_vat,allocated_target_inc_vat,shortfall_target_ex_vat,
         cap_reason,affordability_reason,capacity_reason,channel_headroom_before,channel_headroom_after,worker_take_home_before,worker_take_home_after)
        values(v_state.id,v_instruction.id,v_worker.id,v_candidate,v_worker.preparation_revision,v_state.selection_revision,
         v_state.processed_instruction_count+1,'DRAFT',v_worker.target_pay_channel,v_instruction.hold_purpose,v_instruction.nominal_target_ex_vat,
         v_instruction.capacity_target_ex_vat,v_source_taken,v_taken,v_taken_vat,v_taken_inc,v_instruction.nominal_target_ex_vat-v_taken,
         v_cap_reason,v_affordability_reason,v_capacity_reason,v_e,v_next_e,v_h,v_next_h)
        returning id into v_result_id;
      if v_taken>0 then
        select c.* into strict v_case from private.bpay_next_finance_case c where c.id=v_instruction.case_id for update;
        select c.* into strict v_component from private.bpay_next_case_component c where c.id=v_instruction.case_component_id for update;
        select p.* into strict v_period from private.bpay_next_case_period p
          where p.case_component_id=v_component.id and p.pay_week_start=v_instruction.pay_week_start for update;
        insert into private.bpay_next_case_hold
          (run_worker_id,candidate_id,case_id,amount,status,instruction_id,allocation_result_id,allocation_pass_kind,
           case_component_id,purpose,pay_week_start,source_reserved_ex_vat,target_amount_ex_vat,target_amount_vat,target_amount_inc_vat)
          values(v_worker.id,v_candidate,v_case.id,v_source_taken,'ACTIVE',v_instruction.id,v_result_id,'DRAFT',
           v_component.id,v_instruction.hold_purpose,v_instruction.pay_week_start,v_source_taken,v_taken,v_taken_vat,v_taken_inc) returning id into v_hold_id;
        insert into private.bpay_next_case_capacity_use(case_hold_id,case_component_id,case_id,candidate_id,
          pay_week_start,purpose,status,source_amount_ex_vat)
          values(v_hold_id,v_component.id,v_case.id,v_candidate,v_instruction.pay_week_start,v_instruction.hold_purpose,'ACTIVE',v_source_taken);
        if v_instruction.hold_purpose='PAYOUT' then
          update private.bpay_next_case_component set active_payout_source_ex_vat=active_payout_source_ex_vat+v_source_taken,
            component_revision=component_revision+1,updated_at_utc=pg_catalog.transaction_timestamp() where id=v_component.id;
          update private.bpay_next_finance_case set active_payout_hold_amount=active_payout_hold_amount+v_source_taken,
            active_hold_amount=active_hold_amount+v_source_taken,case_revision=case_revision+1,updated_at_utc=pg_catalog.transaction_timestamp() where id=v_case.id;
          update private.bpay_next_case_period set active_payout_source_ex_vat=active_payout_source_ex_vat+v_source_taken,
            period_revision=period_revision+1 where case_component_id=v_component.id and pay_week_start=v_instruction.pay_week_start;
        else
          update private.bpay_next_case_component set active_recovery_source_ex_vat=active_recovery_source_ex_vat+v_source_taken,
            component_revision=component_revision+1,updated_at_utc=pg_catalog.transaction_timestamp() where id=v_component.id;
          update private.bpay_next_finance_case set active_recovery_hold_amount=active_recovery_hold_amount+v_source_taken,
            active_hold_amount=active_hold_amount+v_source_taken,case_revision=case_revision+1,updated_at_utc=pg_catalog.transaction_timestamp() where id=v_case.id;
          update private.bpay_next_case_period set active_unrealised_recovery_source_ex_vat=active_unrealised_recovery_source_ex_vat+v_source_taken,
            period_revision=period_revision+1 where case_component_id=v_component.id and pay_week_start=v_instruction.pay_week_start;
          v_state.recovered_source_total:=v_state.recovered_source_total+v_source_taken;
          v_state.recovered_target_total:=v_state.recovered_target_total+v_taken;
        end if;
        if v_instruction.source_comparison_receipt_id is not null then
          select p.* into strict v_position from private.bpay_next_position p
            where p.work_id=v_instruction.canonical_source_work_id and p.component_key=v_instruction.canonical_source_component_key for update;
          v_carry:=private.bpay_next_source_comparison_case_capacity_carry_v2(p_job_id,v_position.source_book_revision,
            v_position.source_comparison_receipt_id,v_hold_id,'CASE_RESERVE');
        end if;
        update private.bpay_next_run_worker set active_case_hold_count=active_case_hold_count+1 where id=v_worker.id;
      end if;
      if v_instruction.payroll_stage='GROSS_ADD' then
        v_state.allocated_gross_additions:=v_state.allocated_gross_additions+v_taken;
        v_state.allocated_gross_additions_vat:=v_state.allocated_gross_additions_vat+v_taken_vat;
      elsif v_instruction.payroll_stage='GROSS_DEDUCT' then
        v_state.allocated_gross_deductions:=v_state.allocated_gross_deductions+v_taken;
        v_state.allocated_gross_deductions_vat:=v_state.allocated_gross_deductions_vat+v_taken_vat;
      elsif v_instruction.payroll_stage='NET_ADD' then
        v_state.allocated_net_additions:=v_state.allocated_net_additions+v_taken;
        v_state.allocated_net_additions_vat:=v_state.allocated_net_additions_vat+v_taken_vat;
      else
        v_state.allocated_net_recoveries:=v_state.allocated_net_recoveries+v_taken;
        v_state.allocated_net_recoveries_vat:=v_state.allocated_net_recoveries_vat+v_taken_vat;
      end if;
      v_e:=v_next_e;v_h:=v_next_h;
      v_state.processed_instruction_count:=v_state.processed_instruction_count+1;
      v_state.cursor_age_key:=v_instruction.age_key;v_state.cursor_case_id:=v_instruction.case_id;
      v_state.cursor_component_ordinal:=v_instruction.component_ordinal;v_state.cursor_case_component_id:=v_instruction.case_component_id;
      update private.bpay_next_run_worker set case_allocated_count=case_allocated_count+1 where id=v_worker.id;
    end loop;
    update private.bpay_next_case_allocation_channel set remaining_headroom=v_e where state_id=v_state.id and allocation_channel=v_worker.target_pay_channel;
    update private.bpay_next_case_allocation_state set processed_instruction_count=v_state.processed_instruction_count,
      remaining_worker_take_home=v_h,recovered_source_total=v_state.recovered_source_total,recovered_target_total=v_state.recovered_target_total,
      cursor_age_key=v_state.cursor_age_key,cursor_case_id=v_state.cursor_case_id,cursor_component_ordinal=v_state.cursor_component_ordinal,
      cursor_case_component_id=v_state.cursor_case_component_id,allocated_gross_additions=v_state.allocated_gross_additions,
      allocated_gross_deductions=v_state.allocated_gross_deductions,allocated_net_additions=v_state.allocated_net_additions,
      allocated_gross_additions_vat=v_state.allocated_gross_additions_vat,
      allocated_gross_deductions_vat=v_state.allocated_gross_deductions_vat,
      allocated_net_additions_vat=v_state.allocated_net_additions_vat,
      allocated_net_recoveries=v_state.allocated_net_recoveries,allocated_net_recoveries_vat=v_state.allocated_net_recoveries_vat where id=v_state.id;
    select exists(select 1 from private.bpay_next_run_case_instruction i where i.run_worker_id=v_worker.id
      and i.preparation_revision=v_worker.preparation_revision and i.selection_revision=v_state.selection_revision
      and (v_state.cursor_case_component_id is null or (i.age_key,i.case_id,i.component_ordinal,i.case_component_id)
        >(v_state.cursor_age_key,v_state.cursor_case_id,v_state.cursor_component_ordinal,v_state.cursor_case_component_id))) into v_has_more;
    if not v_has_more then
      if v_state.processed_instruction_count<>v_state.expected_instruction_count then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_PREPARE_ALLOCATION_COUNT_MISMATCH';
      end if;
      v_gross:=v_state.captured_work_gross+v_state.allocated_gross_additions-v_state.allocated_gross_deductions;
      v_gross_vat:=v_state.captured_work_vat+v_state.allocated_gross_additions_vat-v_state.allocated_gross_deductions_vat;
      if v_gross_vat<0 or v_gross_vat>=10000000000000000 then v_issue:='CASE_INPUT_UNBOUND';exit prepare_page;end if;
      if v_gross<0 or v_gross>=10000000000000000 or v_state.allocated_net_additions>=10000000000000000 then
        v_issue:='CASE_INPUT_UNBOUND';exit prepare_page;
      end if;
      -- Preserve certified-zero WORK/Source rows; case-only NET additions are
      -- real payable cash, not a made-up positive payroll gross or net.
      if v_worker.captured_line_count=0 and v_gross=0 and v_state.allocated_net_additions=0 then
        v_issue:='CASE_NO_PAYABLE_AMOUNT';exit prepare_page;
      end if;
      if v_worker.target_pay_channel='PAYE' and v_worker.captured_line_count=0 and v_gross=0 and v_state.payout_instruction_count>0 then
        insert into private.bpay_next_case_payout_basis(run_worker_id,candidate_id,preparation_revision,currency,
          beneficiary_kind,beneficiary_id,expected_instruction_count,captured_instruction_count,source_total_ex_vat,
          target_total_ex_vat,target_total_vat,target_total_inc_vat,status)
          values(v_worker.id,v_candidate,v_worker.preparation_revision,'GBP','CANDIDATE',v_candidate,
            v_state.payout_instruction_count,v_state.payout_instruction_count,v_state.allocated_net_additions,
            v_state.allocated_net_additions,0,v_state.allocated_net_additions,'SEALED');
      end if;
      select w.* into strict v_week from private.bpay_next_worker_week w
        where w.candidate_id=v_candidate and w.pay_week_start=v_state.pay_week_start for update;
      insert into private.bpay_next_worker_week_contribution(candidate_id,pay_week_start,original_run_worker_id,
        original_pay_date,original_created_at_utc,contribution_revision,basis_kind,original_gross_amount,
        payment_state,eligibility_state,eligible_arranged_amount,primary_binding_revision)
        values(v_candidate,v_state.pay_week_start,v_worker.id,v_run.pay_date,v_run.created_at_utc,1,'GROSS_FALLBACK',
          v_gross,'ARRANGED',case when v_gross>0 then 'ELIGIBLE' else 'EXCLUDED' end,v_gross,1);
      update private.bpay_next_worker_week set resolved_arranged_take_home=resolved_arranged_take_home+v_gross,
        period_revision=period_revision+1 where candidate_id=v_candidate and pay_week_start=v_state.pay_week_start;
      update private.bpay_next_case_allocation_state set status='COMPLETE',prepare_stage='COMPLETE',
        completed_at_utc=pg_catalog.transaction_timestamp() where id=v_state.id;
      update private.bpay_next_run_worker set gross_ex_vat=v_gross,gross_vat=v_gross_vat,gross_inc_vat=v_gross+v_gross_vat,
        status='READY',case_pending_binding_count=0 where id=v_worker.id;
      update private.bpay_next_job set status='DONE',phase='READY',cursor_key=null,lease_nonce=null,lease_until_utc=null,
        position_work_cursor=null,position_component_cursor=null where id=p_job_id;
      v_terminal:=true;
    end if;
  end prepare_page;

  if v_issue is not null then
    -- Candidate-wide issue: WORK FK remains NULL. Earlier frozen WORK and any
    -- already-created ACTIVE reservations remain exact and non-confirmable.
    update private.bpay_next_run_worker set status='REVIEW',review_issue_code=v_issue,review_issue_work_id=null,
      financial_resolution_count=financial_resolution_count+1,case_pending_binding_count=
        case when v_issue in ('CASE_SELECTION_REQUIRED','CASE_INPUT_UNBOUND','CASE_WEEK_BASIS_UNBOUND','CASE_RETURN_FLOOR_UNBOUND','CASE_SOURCE_PARTITION_REVIEW_REQUIRED') then 1 else 0 end
      where id=v_worker.id;
    if v_state.id is not null then
      update private.bpay_next_case_allocation_state set prepare_stage='REVIEW',weekly_binding_state='UNBOUND',
        instruction_capture_cursor=v_state.instruction_capture_cursor,captured_instruction_count=v_state.captured_instruction_count,
        payout_instruction_count=v_state.payout_instruction_count where id=v_state.id;
    end if;
    update private.bpay_next_job set status='DONE',phase='DONE',cursor_key=null,lease_nonce=null,lease_until_utc=null,
      position_work_cursor=null,position_component_cursor=null where id=p_job_id;
    v_terminal:=true;
  elsif not v_terminal then
    update private.bpay_next_job set cursor_key=(v_saved_cursor+1)::text where id=p_job_id;
  end if;
  update private.bpay_next_worker_control set financial_view_revision=financial_view_revision+1,
    updated_at_utc=pg_catalog.transaction_timestamp() where candidate_id=v_candidate;
  return pg_catalog.jsonb_build_object('done',v_terminal,'phase',case when v_issue is not null then 'REVIEW'
    when v_terminal then 'READY' else 'FINANCE_ALLOCATE' end,'cursor',case when v_terminal then null else (v_saved_cursor+1)::text end,
    'rows_visited',v_seen::text,'replay',false,'run_worker_id',v_worker.id,
    'worker_status',case when v_issue is not null then 'REVIEW' when v_terminal then 'READY' else 'PREPARING' end)
    ||case when v_issue is not null then pg_catalog.jsonb_build_object('issue_code',v_issue) else '{}'::jsonb end;
end
$function$;

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
          and s.status='BUILDING' and s.prepare_stage='ALLOCATE'
          and (s.weekly_binding_state='BOUND' or (s.weekly_binding_state='NOT_AFFECTED'
            and s.expected_instruction_count>0
            and (select count(*) from private.bpay_next_run_case_instruction fi
              where fi.run_worker_id=rw.id and fi.run_id=rw.run_id and fi.candidate_id=rw.candidate_id
                and fi.preparation_revision=rw.preparation_revision and fi.selection_revision=rw.case_selection_revision)
              =s.expected_instruction_count
            and not exists(select 1 from private.bpay_next_run_case_instruction fi
              left join private.bpay_next_case_selection_item si
                on si.run_id=fi.run_id and si.candidate_id=fi.candidate_id and si.selection_revision=fi.selection_revision
                  and si.case_id=fi.case_id and si.case_component_id=fi.case_component_id and si.is_selected
              where fi.run_worker_id=rw.id and fi.run_id=rw.run_id and fi.candidate_id=rw.candidate_id
                and fi.preparation_revision=rw.preparation_revision and fi.selection_revision=rw.case_selection_revision
                and ((fi.case_kind,fi.instruction_kind,fi.payroll_stage,fi.hold_purpose)
                  is distinct from ('OVERPAYMENT','RECOVERY','GROSS_DEDUCT','GROSS_RECOVERY')
                  or si.case_id is null))))
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

