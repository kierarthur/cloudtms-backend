-- DR14 capture-only other-Draft reason; exact current plan/prepare parents.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_case_allocation_plan_v1(p_scope jsonb)
returns jsonb language plpgsql stable security invoker
set search_path=pg_catalog,private,public
as $function$
declare
 v_mode text;v_candidate uuid;v_run_id uuid;v_run private.bpay_next_pay_run%rowtype;
 v_case private.bpay_next_finance_case%rowtype;v_component private.bpay_next_case_component%rowtype;
 v_rule private.bpay_next_case_rule%rowtype;v_period_rule private.bpay_next_case_rule%rowtype;
 v_origin private.bpay_next_case_create_request%rowtype;v_collection private.bpay_next_work_collection%rowtype;
 v_state private.bpay_next_case_allocation_state%rowtype;v_worker private.bpay_next_run_worker%rowtype;
 v_period private.bpay_next_case_period%rowtype;v_value record;v_row record;
 v_amendment jsonb;v_old_amendment jsonb;v_period_before jsonb;v_approved_principal numeric;
 v_instruction private.bpay_next_run_case_instruction%rowtype;v_instruction_json jsonb;v_rows jsonb:='[]';
 v_policy_id uuid;v_case_cut jsonb;v_memory_cut boolean;v_case_status text;v_case_due date;
 v_automatic boolean;v_explanation text;v_outstanding numeric;v_case_outstanding numeric;
 v_nominal numeric;v_opening_due numeric;v_capacity numeric;v_purpose text;v_age timestamptz;v_age_key bigint;
 v_period_operation text;v_resolution_ref text;v_e numeric;v_h numeric;v_taken numeric;v_next_e numeric;v_next_h numeric;
 v_foreign_period numeric;v_foreign_component numeric;v_foreign_case numeric;v_without_foreign numeric;v_foreign_reduction numeric;v_foreign_reason text;
 v_cap_reason text;v_affordability_reason text;v_capacity_reason text;v_keys text[];
begin
 if jsonb_typeof(p_scope) is distinct from 'object' then
  raise exception using errcode='22023',message='BPAY_NEXT_CASE_PLAN_SCOPE_INVALID';end if;
 v_mode:=p_scope->>'mode';
 if v_mode='DERIVE' then
  v_keys:=array['mode','candidate_id','case_id','case_component_id','pay_date','pay_week_start',
   'target_pay_channel','captured_default_floor','captured_default_floor_revision','weekly_binding_state','accepted_run_id'];
  if not(p_scope ?& v_keys) or p_scope-v_keys<>'{}'::jsonb then
   raise exception using errcode='22023',message='BPAY_NEXT_CASE_PLAN_SCOPE_INVALID';end if;
  v_candidate:=(p_scope->>'candidate_id')::uuid;v_run_id:=(p_scope->>'accepted_run_id')::uuid;
  v_run.pay_date:=(p_scope->>'pay_date')::date;
  v_state.pay_week_start:=(p_scope->>'pay_week_start')::date;
  v_worker.target_pay_channel:=p_scope->>'target_pay_channel';
  v_state.captured_default_floor:=(p_scope->>'captured_default_floor')::numeric;
  v_state.captured_default_floor_revision:=(p_scope->>'captured_default_floor_revision')::bigint;
  v_state.weekly_binding_state:=p_scope->>'weekly_binding_state';
  if v_candidate is null or v_run.pay_date is null or not isfinite(v_run.pay_date)
   or v_state.pay_week_start is distinct from v_run.pay_date-(extract(isodow from v_run.pay_date)::integer-1)
   or v_state.captured_default_floor is null or v_state.captured_default_floor<0
   or v_state.captured_default_floor::text in ('NaN','Infinity','-Infinity')
   or v_state.captured_default_floor<>trunc(v_state.captured_default_floor,2)
   or coalesce(v_state.captured_default_floor_revision,0)<1
   or v_state.weekly_binding_state not in ('BOUND','NOT_AFFECTED') then
   raise exception using errcode='22023',message='BPAY_NEXT_CASE_PLAN_SCOPE_INVALID';end if;
  if v_run_id is not null then
   select r.accepted_choice_descriptor into strict v_run.accepted_choice_descriptor from private.bpay_next_pay_run r
    where r.id=v_run_id and r.pay_date=v_run.pay_date and r.selection_state='SEALED';
  end if;
  select c.* into strict v_case from private.bpay_next_finance_case c
   where c.id=(p_scope->>'case_id')::uuid and c.candidate_id=v_candidate;
  select c.* into strict v_component from private.bpay_next_case_component c
   where c.id=(p_scope->>'case_component_id')::uuid and c.case_id=v_case.id and c.candidate_id=v_candidate;
        -- WB26: immutable rule/control choice from this original accepted cut.
        -- Balances below are still live and protected by the original money locks.
        v_memory_cut:=v_run_id is not null and v_run.accepted_choice_descriptor is not null
          and v_run.accepted_choice_descriptor ? 'memory_baseline_run_id';
        v_case_cut:=null;v_case_status:=v_case.status;v_case_due:=v_case.due_date;
        if v_memory_cut then
          select retained.basis into v_case_cut from private.bpay_next_selection_mode_memory retained
            where retained.run_id=v_run_id and retained.row_kind='LEAF'
              and retained.leaf_key='CASE:'||v_component.id::text
              and retained.candidate_id=v_candidate and retained.case_component_id=v_component.id;
          if v_case_cut is null or not(v_case_cut ?& array['case_id','case_component_id','case_revision',
              'case_state','case_due_date','case_kind','case_subtype','case_tax_treatment','case_instruction_kind',
              'case_component_ordinal','case_control_sequence','case_component_revision','rule_id','case_rule_revision'])
            or (v_case_cut->>'case_id',v_case_cut->>'case_component_id',v_case_cut->>'case_kind',v_case_cut->>'case_subtype',
                v_case_cut->>'case_tax_treatment',v_case_cut->>'case_instruction_kind',v_case_cut->>'direction',
                v_case_cut->>'payroll_stage',v_case_cut->>'source_pay_channel',v_case_cut->>'case_component_ordinal')
              is distinct from (v_case.id::text,v_component.id::text,v_component.case_kind,v_component.case_subtype,
                v_component.tax_treatment,v_component.instruction_kind,v_component.direction,v_component.payroll_stage,
                v_component.source_pay_channel,v_component.component_ordinal::text)
            or coalesce((v_case_cut->>'case_revision')::bigint,0)<1 or coalesce((v_case_cut->>'case_component_revision')::bigint,0)<1
            or v_case_cut->>'case_state' is distinct from 'OPEN'
            or v_case_cut->>'rule_id' is null or coalesce((v_case_cut->>'case_rule_revision')::bigint,0)<1
            or coalesce((v_case_cut->>'case_control_sequence')::bigint,-1)<0 then
            raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';
          end if;
          v_case_status:=v_case_cut->>'case_state';v_case_due:=(v_case_cut->>'case_due_date')::date;
          select r.* into v_rule from private.bpay_next_case_rule r where r.id=(v_case_cut->>'rule_id')::uuid
            and r.case_id=v_case.id and r.candidate_id=v_candidate
            and r.rule_revision=(v_case_cut->>'case_rule_revision')::bigint
            and (r.case_kind,r.case_subtype,r.tax_treatment)
              is not distinct from (v_component.case_kind,v_component.case_subtype,v_component.tax_treatment);
        else
          select r.* into v_rule from private.bpay_next_case_rule r where r.id=v_case.current_rule_id
            and r.case_id=v_case.id and r.candidate_id=v_candidate and r.rule_revision=v_case.current_rule_revision;
        end if;
        select r.* into v_origin from private.bpay_next_case_create_request r where r.case_id=v_case.id and r.candidate_id=v_candidate;
        -- Refuse the immutable selected identity before qualification: a stale
        -- Source collection must not disappear into an ordinary review path.
        if exists(select 1 from private.bpay_next_work_collection c
          join private.bpay_next_work w on w.id=c.work_id and w.candidate_id=c.candidate_id
          where c.case_id=v_case.id and c.case_component_id=v_component.id
            and c.candidate_id=v_candidate and w.work_kind='SOURCE') then
          raise exception using errcode='55000',message='BPAY_NEXT_COMPARISON_NOT_INSTALLED';
        end if;
        -- The exact original read qualification, in this STABLE snapshot.
  select b.* into v_collection from private.bpay_next_work_collection b
    join private.bpay_next_position p on p.work_id=b.work_id and p.component_key=b.component_key
    join private.bpay_next_work_revision r on r.id=p.applied_revision_id and r.work_id=b.work_id
    join private.bpay_next_work_revision original on original.id=b.original_revision_id and original.work_id=b.work_id
    join private.bpay_next_work work on work.id=b.work_id and work.candidate_id=b.candidate_id
    join private.bpay_next_approved_line original_line on original_line.id=b.original_approved_line_id
      and original_line.revision_id=original.id and original_line.component_key=b.component_key
    join private.bpay_next_finance_case c on c.id=b.case_id and c.candidate_id=b.candidate_id
    join private.bpay_next_case_component x on x.id=b.case_component_id and x.case_id=c.id and x.candidate_id=c.candidate_id
    join private.bpay_next_case_rule rule on rule.id=x.rule_id and rule.case_id=c.id and rule.candidate_id=c.candidate_id
    where b.case_id=v_case.id and b.case_component_id=v_component.id and b.candidate_id=v_candidate
      and b.id=b.case_id and b.id=b.case_component_id and b.paid_basis_qualified
      and b.qualification_state='READY' and b.original_source_pay_channel='PAYE' and b.original_tax_treatment='TAXABLE'
      and (b.observed_revision_id,b.observed_approved_source_ex_vat,b.observed_realised_source_ex_vat)
        is not distinct from (p.applied_revision_id,p.approved_source_ex_vat,p.realised_source_ex_vat)
      and p.source_basis_channel='PAYE' and p.approved_source_ex_vat>=0 and p.realised_source_ex_vat>=0
      and (p.realised_target_ex_vat,p.realised_target_vat,p.realised_target_inc_vat)
        is not distinct from (p.realised_source_ex_vat,0::numeric,p.realised_source_ex_vat)
      and ((work.work_kind='ORDINARY' and original.source_kind='ORDINARY' and r.source_kind='ORDINARY')
        or (work.work_kind='SOURCE' and original_line.component_kind='WORK'
          and original_line.source_component_id is not null
          and b.component_key='SOURCE:'||original_line.source_component_id::text
          and ((original.source_kind='SOURCE' and original.source_event_id is not null
            and (original.source_head_id is null or original.source_event_id=original.source_head_id)) or (original.source_kind='PROTECTED'
            and original.source_head_id is not null and original.source_event_id=original.source_head_id))
          and ((r.source_kind='SOURCE' and r.source_event_id is not null
            and (r.source_head_id is null or r.source_event_id=r.source_head_id)) or (r.source_kind='PROTECTED'
            and r.source_head_id is not null and r.source_event_id=r.source_head_id))))
      and r.source_pay_channel='PAYE' and r.sealed_at_utc is not null
      and ((r.detail_kind=original.detail_kind and exists(select 1 from private.bpay_next_approved_line a
        where a.revision_id=r.id and a.component_key=b.component_key and a.component_kind='WORK'
          and a.tax_treatment='TAXABLE' and a.source_pay_ex_vat=p.approved_source_ex_vat
          and (work.work_kind='ORDINARY' or a.source_component_id=original_line.source_component_id)))
        or (r.certified_zero and r.approved_source_ex_vat=0 and r.expected_line_count=0 and p.approved_source_ex_vat=0)
        or (b.segment_attribution_complete and coalesce((private.bpay_next_segment_collection_basis_v1(b.work_id,b.component_key,r.id)->>'complete')::boolean,false)))
      and (not b.segment_attribution_complete or (b.observed_segment_negative_ex_vat is not distinct from
        (private.bpay_next_segment_collection_basis_v1(b.work_id,b.component_key,r.id)->>'negative_source_ex_vat')::numeric
        and coalesce((private.bpay_next_segment_collection_basis_v1(b.work_id,b.component_key,r.id)->>'complete')::boolean,false)))
      and c.case_kind='OVERPAYMENT' and c.tax_treatment='TAXABLE' and c.principal_funded=0
      and x.case_kind='OVERPAYMENT' and x.case_subtype='OVERPAYMENT' and x.tax_treatment='TAXABLE'
      and x.source_pay_channel='PAYE' and x.instruction_kind='RECOVERY' and x.direction='DEDUCTION'
      and x.payroll_stage='GROSS_DEDUCT' and x.component_key='RECOVERY' and x.component_ordinal=1
      and c.current_rule_id=rule.id and c.current_rule_revision=rule.rule_revision and rule.id=b.id
      and rule.weekly_due_source_ex_vat is null
      and (c.principal_approved,c.principal_recovered,c.principal_written_off,c.active_recovery_hold_amount)
        is not distinct from (x.approved_source_ex_vat,x.recovered_source_ex_vat,x.written_off_source_ex_vat,x.active_recovery_source_ex_vat);

        v_automatic:=v_collection.id is not null;
        if v_state.weekly_binding_state='NOT_AFFECTED'
          and (v_case.case_kind,v_component.case_kind,v_component.instruction_kind,v_component.payroll_stage)
            is distinct from ('OVERPAYMENT','OVERPAYMENT','RECOVERY','GROSS_DEDUCT') then
          raise exception using errcode='23514',message='BPAY_NEXT_CASE_WEEK_BASIS_UNBOUND';
        end if;
        if v_rule.id is null or v_case.case_revision<1 or (not v_memory_cut and v_component.rule_id<>v_rule.id) then
          raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';
        end if;
        if not v_automatic then
          v_amendment:=private.bpay_next_case_edit_origin_v1(v_rule.id);
          v_approved_principal:=case when v_amendment is null then v_origin.principal_source_ex_vat
            else (v_amendment->>'principal')::numeric end;
          if v_amendment is not null and ((v_amendment->>'original_command_id')::uuid is distinct from v_origin.command_id
              or (v_amendment->>'case_id')::uuid is distinct from v_case.id
              or (v_amendment->>'candidate_id')::uuid is distinct from v_candidate
              or v_amendment->'input'->>'tax_treatment' is distinct from v_component.tax_treatment) then
            raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';end if;
        end if;
        if v_automatic then
          -- Genuine paid WORK origin, not a fabricated CASE_CREATE receipt.
          -- The immutable original paid policy and the exact applied position
          -- are authority; a newer queued WORK approval is not a reprice.
          if v_origin.command_id is not null then raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';end if;
          v_policy_id:=v_collection.original_valuation_policy_id;
        elsif v_origin.command_id is null
           or (not v_memory_cut and v_component.rule_id<>v_rule.id) or v_origin.case_kind<>v_case.case_kind
           or v_origin.source_pay_channel<>v_component.source_pay_channel
           or v_approved_principal<>v_component.approved_source_ex_vat
           or (v_component.id<>v_origin.primary_component_id
             and (v_origin.recovery_component_id is null or v_component.id<>v_origin.recovery_component_id))
           or (v_component.id=v_origin.primary_component_id and v_component.instruction_kind<>
             case when v_case.case_kind in ('LOAN','ADVANCE') then 'PAYOUT'
               when v_case.case_kind='CREDIT' then 'CREDIT' else 'RECOVERY' end)
           or (v_component.id=v_origin.recovery_component_id and v_component.instruction_kind<>'RECOVERY') then
          raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';
        else
          v_policy_id:=v_origin.valuation_policy_id;
        end if;
        if v_component.source_pay_channel<>'PAYE' or v_worker.target_pay_channel<>'PAYE' then
          raise exception using errcode='23514',message='BPAY_NEXT_CASE_TARGET_UNSUPPORTED';
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
          if v_rule.weekly_due_source_ex_vat is null then raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';end if;
          if v_case.case_kind in ('LOAN','ADVANCE') and v_case.principal_funded>0
             and (v_rule.original_funding_event_id is null or v_rule.original_funded_at_utc is null) then
            raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';
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
        if v_case_status<>'OPEN' or (v_case_due is not null and v_case_due>v_run.pay_date) then
          v_nominal:=0;v_explanation:='Selected case is not currently open/due; no reservation';
        end if;
        if v_outstanding<0 or v_case_outstanding<0 or v_nominal<0 then
          raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';
        end if;
        -- Opening due is independent of later principal. Never subtract the
        -- same realised recovery twice or use max(overlapping counters).
        select p.* into v_period from private.bpay_next_case_period p
          where p.case_component_id=v_component.id and p.pay_week_start=v_state.pay_week_start;
        v_period_operation:='KEEP';
        if not found then
          -- Prospective opening operands only; the writable owner persists them.
          v_period_operation:='INSERT';
          v_period.case_component_id:=v_component.id;v_period.case_id:=v_case.id;
          v_period.candidate_id:=v_candidate;v_period.pay_week_start:=v_state.pay_week_start;
          v_period.rule_id:=v_rule.id;v_period.period_revision:=1;
          v_period.opening_outstanding_source_ex_vat:=v_outstanding;
          v_period.opening_due_source_ex_vat:=v_opening_due;
          v_period.realised_recovery_source_ex_vat:=0;
          v_period.active_unrealised_recovery_source_ex_vat:=0;v_period.active_payout_source_ex_vat:=0;
          v_period.work_collection_id:=case when v_automatic then v_collection.id else null end;
        end if;
        if v_period.work_collection_id is distinct from (case when v_automatic then v_collection.id else null end) then
          raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';
        end if;
        if v_period.rule_id<>v_rule.id then
          -- Genuine original funding advances an immutable loan rule solely
          -- to bind its age/evidence. It is NOT a schedule/policy reprice.
          select r.* into strict v_period_rule from private.bpay_next_case_rule r where r.id=v_period.rule_id;
          v_old_amendment:=case when v_period_rule.id=v_origin.rule_id then null
            else private.bpay_next_case_edit_origin_v1(v_period_rule.id) end;
          if v_amendment is not null and v_period_rule.original_funding_event_id is null
             and v_period_rule.rule_revision<v_rule.rule_revision
             and (v_period_rule.id=v_origin.rule_id or v_old_amendment is not null)
             and (v_period_rule.case_id,v_period_rule.candidate_id,v_period_rule.case_created_at_utc)
                is not distinct from (v_case.id,v_candidate,v_origin.accepted_at_utc)
             and (v_period.realised_recovery_source_ex_vat,v_period.active_unrealised_recovery_source_ex_vat,v_period.active_payout_source_ex_vat)
                is not distinct from (0::numeric,0::numeric,0::numeric) then
            v_period_before:=to_jsonb(v_period);v_period_operation:='CASE_EDIT_OPENING';
            v_period.rule_id:=v_rule.id;v_period.period_revision:=v_period.period_revision+1;
            v_period.opening_outstanding_source_ex_vat:=v_outstanding;v_period.opening_due_source_ex_vat:=v_opening_due;
          else
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
            raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';
          end if;
          if v_component.instruction_kind='RECOVERY' and v_period_rule.original_funding_event_id is null then
            if (v_period.opening_outstanding_source_ex_vat,v_period.opening_due_source_ex_vat,
                v_period.realised_recovery_source_ex_vat,v_period.active_unrealised_recovery_source_ex_vat,v_period.active_payout_source_ex_vat)
                is distinct from (0::numeric,0::numeric,0::numeric,0::numeric,0::numeric) then
              raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';
            end if;
            -- Only this exact zero/unconsumed pay-week is made ready. Its
            -- frozen old zero instructions/results remain immutable and zero.
            v_period_operation:='FUNDING_ZERO';
            v_period.rule_id:=v_rule.id;v_period.opening_outstanding_source_ex_vat:=v_outstanding;
            v_period.opening_due_source_ex_vat:=v_opening_due;v_period.period_revision:=v_period.period_revision+1;
          end if;
          end if; -- exact approved amendment or retained original funding-only branch
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

        -- DR14 capture-only diagnostic. The authoritative v_capacity above,
        -- shared row allocation, nominal, principal and all money remain exact.
        -- Restore only another run's ACTIVE counters to identify the causal
        -- difference; realised amounts and this run's own holds stay deducted.
        v_foreign_reduction:=0;v_foreign_reason:=null;
        if v_run_id is not null and exists(select 1 from private.bpay_next_run_worker own
          where own.run_id=v_run_id and own.candidate_id=v_candidate and exists(select 1 from private.bpay_next_case_hold h
            where h.run_worker_id=own.id and h.case_id=v_case.id and h.status='ACTIVE')) then
          -- The actual fresh/retained INSTRUCTION_CAPTURE precedes all own
          -- ALLOCATE holds. An exceptional other caller with own holds has
          -- unknown attribution, never a guessed amount or business refusal.
          v_foreign_reduction:=null;
        elsif v_run_id is not null and v_capacity<v_nominal then
          -- With no own ACTIVE hold, these exact locked native counters are
          -- the foreign contribution. No foreign reservation/history scan.
          v_foreign_period:=case when v_component.instruction_kind='RECOVERY' then v_period.active_unrealised_recovery_source_ex_vat else 0 end;
          v_foreign_component:=case when v_component.instruction_kind='RECOVERY' then v_component.active_recovery_source_ex_vat else v_component.active_payout_source_ex_vat end;
          v_foreign_case:=case when v_component.instruction_kind='RECOVERY' then v_case.active_recovery_hold_amount else v_case.active_payout_hold_amount end;
          v_without_foreign:=case when v_automatic then
            least(v_nominal,greatest(v_outstanding-v_component.active_recovery_source_ex_vat+v_foreign_component,0),
              greatest(v_case_outstanding-v_case.active_recovery_hold_amount+v_foreign_case,0))
            when v_component.instruction_kind='RECOVERY' then
            least(v_nominal,greatest(v_period.opening_due_source_ex_vat-v_period.realised_recovery_source_ex_vat
              -v_period.active_unrealised_recovery_source_ex_vat+v_foreign_period,0),
              greatest(v_outstanding-v_component.active_recovery_source_ex_vat+v_foreign_component,0),
              greatest(v_case_outstanding-v_case.active_recovery_hold_amount+v_foreign_case,0))
            else least(v_nominal,greatest(v_outstanding-v_component.active_payout_source_ex_vat+v_foreign_component,0),
              greatest(v_case_outstanding-v_case.active_payout_hold_amount+v_foreign_case,0)) end;
          v_foreign_reduction:=greatest(v_without_foreign-v_capacity,0);
          if v_foreign_reduction>0 then
            v_foreign_reason:=case when not v_automatic and v_component.instruction_kind='RECOVERY' and v_foreign_period>0
              and greatest(v_period.opening_due_source_ex_vat-v_period.realised_recovery_source_ex_vat-v_period.active_unrealised_recovery_source_ex_vat,0)=v_capacity
              then 'SAME_WEEK_INSTALMENT' else 'CASE_BALANCE' end;
          end if;
        end if;
        begin
          select x.* into strict v_value from private.bpay_next_value_source_for_target_v1(
            v_component.approved_source_ex_vat,'PAYE','PAYE',null,v_policy_id,v_run.pay_date) x;
        exception when check_violation then
          if sqlerrm in ('BPAY_NEXT_TARGET_POLICY_WINDOW_MISSING','BPAY_NEXT_TARGET_POLICY_WINDOW_AMBIGUOUS') then
            raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';
          else raise;end if;
        end;
        if v_value.target_ex_vat<>v_component.approved_source_ex_vat or v_value.target_vat<>0
           or v_value.target_inc_vat<>v_value.target_ex_vat then
          raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';
        end if;
        v_resolution_ref:=case when v_automatic then 'WORK_COLLECTION:'||v_collection.id::text||':PAYE'
            when v_amendment is not null then 'CASE_EDIT:'||(v_amendment->>'command_id')||':PAYE'
            else 'CASE_CREATE:'||v_origin.command_id::text||':PAYE' end;
        -- WB41: current offer only. Native Create admitted Ready/selectable Case
        -- membership at its original cut; a later WAIT never reprices that offer.
        if v_run_id is null and v_automatic and private.bpay_next_work_wait_state_v1(v_collection.work_id) then
          v_nominal:=0;v_capacity:=0;v_explanation:='ORIGIN_UNAUTHORISED_WAIT';
        end if;
        v_age:=coalesce(v_rule.original_funded_at_utc,v_rule.case_created_at_utc);
        -- A genuinely bound stored credit keeps its original creator age. The
        -- exact point sidecar is immutable, not a live legacy money read.
        if not v_automatic then
          select coalesce(o.original_created_at_utc,v_age) into v_age
            from (select 1) s left join private.bpay_next_stored_credit_origin o
              on o.command_id=v_component.id and o.candidate_id=v_candidate;
        end if;
        v_age_key:=(extract(epoch from v_age)*1000000)::bigint;
  return jsonb_build_object('period_operation',v_period_operation,'period',to_jsonb(v_period),
   'period_amendment',case when v_period_operation='CASE_EDIT_OPENING' then
     jsonb_build_object('edit_command_id',v_amendment->'command_id','before',v_period_before) else null end,
   'resolution',jsonb_build_object('target_ex_vat',v_value.target_ex_vat,'target_vat',0,
    'target_inc_vat',v_value.target_inc_vat,'valuation_policy_id',v_policy_id,
    'valuation_window_id',v_value.policy_window_id,'resolution_ref',v_resolution_ref),
   'instruction',jsonb_build_object('case_component_id',v_component.id,
'case_id',v_case.id,
'candidate_id',v_candidate,
'captured_case_revision',v_case.case_revision,
'rule_id',v_rule.id,
'rule_revision',v_rule.rule_revision,
'case_kind',v_component.case_kind,
'case_subtype',v_component.case_subtype,
'tax_treatment',v_component.tax_treatment,
'instruction_kind',v_component.instruction_kind,
'direction',v_component.direction,
'payroll_stage',v_component.payroll_stage,
'hold_purpose',v_purpose,
'age_key',v_age_key,
'debt_age_at_utc',v_age,
'component_ordinal',v_component.component_ordinal,
'pay_week_start',v_state.pay_week_start,
'source_pay_channel','PAYE',
'target_pay_channel','PAYE',
'allocation_channel','PAYE')||
jsonb_build_object('currency','GBP',
'nominal_source_ex_vat',v_nominal,
'nominal_target_ex_vat',v_nominal,
'nominal_target_vat',0,
'nominal_target_inc_vat',v_nominal,
'capacity_source_ex_vat',v_capacity,
'capacity_target_ex_vat',v_capacity,
'capacity_target_vat',0,
'capacity_target_inc_vat',v_capacity,
'other_run_capacity_reduction_source_ex_vat',v_foreign_reduction,
'other_run_capacity_reduction_target_ex_vat',v_foreign_reduction,
'other_run_capacity_reason',v_foreign_reason,
'minimum_earnings_threshold',v_rule.minimum_earnings_threshold,
'take_home_floor_override',v_rule.take_home_floor_override,
'captured_default_floor',v_state.captured_default_floor,
'captured_default_floor_revision',v_state.captured_default_floor_revision,
'resolved_take_home_floor',coalesce(v_rule.take_home_floor_override,v_state.captured_default_floor),
'valuation_policy_id',v_policy_id,
'valuation_window_id',v_value.policy_window_id,
'resolution_ref',v_resolution_ref,
'explanation',v_explanation,
'work_collection_id',case when v_automatic then v_collection.id else null end));
 elsif v_mode='ALLOCATE' then
  v_keys:=array['mode','instructions','channel_headroom','worker_take_home'];
  if not(p_scope ?& v_keys) or p_scope-v_keys<>'{}'::jsonb
   or jsonb_typeof(p_scope->'instructions') is distinct from 'array'
   or jsonb_array_length(p_scope->'instructions')>1000 then
   raise exception using errcode='22023',message='BPAY_NEXT_CASE_PLAN_SCOPE_INVALID';end if;
  v_e:=(p_scope->>'channel_headroom')::numeric;v_h:=(p_scope->>'worker_take_home')::numeric;
  if v_e is null or v_h is null or v_e<0 or v_e::text in ('NaN','Infinity','-Infinity')
   or v_h::text in ('NaN','Infinity','-Infinity') or v_e<>trunc(v_e,2) or v_h<>trunc(v_h,2) then
   raise exception using errcode='22023',message='BPAY_NEXT_CASE_PLAN_SCOPE_INVALID';end if;
  if exists(select 1 from jsonb_array_elements(p_scope->'instructions') x
    group by x->>'case_component_id' having count(*)>1) then
   raise exception using errcode='23514',message='BPAY_NEXT_CASE_PLAN_INSTRUCTION_DUPLICATE';end if;
  for v_instruction in select i.* from jsonb_populate_recordset(null::private.bpay_next_run_case_instruction,p_scope->'instructions') i
   order by i.age_key,i.case_id,i.component_ordinal,i.case_component_id loop
   if v_instruction.case_component_id is null or v_instruction.case_id is null or v_instruction.age_key is null
    or v_instruction.component_ordinal is null or v_instruction.allocation_channel is distinct from 'PAYE'
    or v_instruction.nominal_target_ex_vat is null or v_instruction.capacity_target_ex_vat is null
    or v_instruction.nominal_target_ex_vat<0 or v_instruction.capacity_target_ex_vat<0 then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_PLAN_INSTRUCTION_INVALID';end if;
   if v_instruction.hold_purpose='PAYOUT' then
    v_taken:=least(v_instruction.nominal_target_ex_vat,v_instruction.capacity_target_ex_vat);
    v_cap_reason:=null;v_affordability_reason:=null;
    v_capacity_reason:=case when v_instruction.capacity_target_ex_vat<v_instruction.nominal_target_ex_vat then 'CASE_CAPACITY' else null end;
    v_next_e:=v_e;v_next_h:=v_h;
   else
    select x.* into strict v_row from private.bpay_next_case_allocation_row_v1(v_instruction.case_kind,
     v_instruction.nominal_target_ex_vat,v_instruction.capacity_target_ex_vat,v_e,v_h,
     v_instruction.minimum_earnings_threshold,v_instruction.take_home_floor_override,v_instruction.captured_default_floor) x;
    v_taken:=v_row.taken_amount;v_cap_reason:=v_row.cap_reason;v_affordability_reason:=v_row.affordability_reason;
    v_capacity_reason:=v_row.capacity_reason;v_next_e:=v_row.next_channel_headroom;v_next_h:=v_row.next_worker_take_home;
   end if;
   -- Typed control annotation on the original kernel's zero allocation, never
   -- a second money formula or a persisted later live-WAIT capture.
   if v_instruction.explanation='ORIGIN_UNAUTHORISED_WAIT' then
    if v_instruction.work_collection_id is null
      or (v_instruction.case_kind,v_instruction.instruction_kind,v_instruction.direction,
          v_instruction.payroll_stage,v_instruction.hold_purpose)
        is distinct from ('OVERPAYMENT','RECOVERY','DEDUCTION','GROSS_DEDUCT','GROSS_RECOVERY')
      or v_instruction.nominal_source_ex_vat is distinct from 0::numeric
      or v_instruction.capacity_source_ex_vat is distinct from 0::numeric
      or v_instruction.nominal_target_ex_vat is distinct from 0::numeric
      or v_instruction.capacity_target_ex_vat is distinct from 0::numeric
      or v_taken is distinct from 0::numeric or v_next_e is distinct from v_e or v_next_h is distinct from v_h then
     raise exception using errcode='23514',message='BPAY_NEXT_CASE_PLAN_INSTRUCTION_INVALID';end if;
    v_cap_reason:='ORIGIN_UNAUTHORISED_WAIT';v_affordability_reason:=null;v_capacity_reason:=null;
   end if;
   v_rows:=v_rows||jsonb_build_array(jsonb_build_object('instruction',to_jsonb(v_instruction),
    'taken_amount',v_taken,'cap_reason',v_cap_reason,'affordability_reason',v_affordability_reason,'capacity_reason',v_capacity_reason,
    'channel_headroom_before',v_e,'channel_headroom_after',v_next_e,'worker_take_home_before',v_h,'worker_take_home_after',v_next_h));
   v_e:=v_next_e;v_h:=v_next_h;
  end loop;
  return jsonb_build_object('rows',v_rows,'channel_headroom_after',v_e,'worker_take_home_after',v_h);
 else raise exception using errcode='22023',message='BPAY_NEXT_CASE_PLAN_SCOPE_INVALID';end if;
end
$function$;


create or replace function private.bpay_next_prepare_case_page_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,
  p_expected_cursor bigint default null,p_limit integer default 100
) returns jsonb
language plpgsql security invoker
set search_path=pg_catalog,private,public
as $function$
declare
  v_plan jsonb;v_plan_period jsonb;v_plan_row jsonb;v_plan_instructions jsonb;
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
  v_policy_id uuid;v_case_cut jsonb;v_memory_cut boolean;v_case_due date;v_case_status text;
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
  select private.bpay_next_prepare_job_run_v1(j.id),j.candidate_id into strict v_run_id,v_candidate
    from private.bpay_next_job j
    where j.id=p_job_id and j.job_kind='PREPARE';
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share nowait;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  -- Same lock order as the real lane; root UPDATE fences confirmation/cancel.
  select r.* into strict v_run from private.bpay_next_pay_run r where r.id=v_run_id for update nowait;
  select c.* into strict v_control from private.bpay_next_worker_control c where c.candidate_id=v_candidate for update nowait;
  select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id for update nowait;
  select w.* into strict v_worker from private.bpay_next_run_worker w
    where w.run_id=v_run_id and w.candidate_id=v_candidate for update nowait;
  select s.* into v_state from private.bpay_next_case_allocation_state s where s.run_worker_id=v_worker.id and s.preparation_revision=v_worker.captured_preparation_revision
    and s.run_worker_id=v_worker.id and s.pass_kind='DRAFT' and s.projection_no=0 for update nowait;
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
       'CASE_WEEK_BASIS_UNBOUND','CASE_RETURN_FLOOR_UNBOUND','CASE_NO_PAYABLE_AMOUNT') then
    return pg_catalog.jsonb_build_object('done',true,'phase','REVIEW','cursor',null,'rows_visited','0',
      'replay',true,'run_worker_id',v_worker.id,'worker_status',v_worker.status,'issue_code',v_worker.review_issue_code);
  end if;
  if not private.bpay_next_prepare_job_active_v1(p_job_id) then
    raise exception using errcode='55000',message='BPAY_NEXT_WORKER_ACTION_REVISION_STALE';end if;
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
  perform private.bpay_next_assert_candidate_financial_fence_v1(v_candidate,p_job_id,p_lease_nonce,p_owner_epoch);
  -- Only a positively proved no-case context may retain the old owner path.
  if v_state.id is null and not exists(select 1 from private.bpay_next_case_selection s where s.run_id=v_run_id and s.candidate_id=v_candidate)
     and (not exists(select 1 from private.bpay_next_finance_case c where c.candidate_id=v_candidate)
       or (v_run.accepted_choice_descriptor is not null
         and exists(select 1 from private.bpay_next_selection_candidate selected where selected.run_id=v_run_id and selected.candidate_id=v_candidate)
         and not exists(select 1 from private.bpay_next_run_choice_member member where member.run_id=v_run_id
           and member.member_kind='CASE_COMPONENT' and member.candidate_id=v_candidate))) then
    return private.bpay_next_complete_no_case_worker_v1(p_job_id,p_lease_nonce,p_owner_epoch);
  end if;

  <<prepare_page>>
  begin
    -- A resumed weekly/capture/allocation page uses its exact frozen selection
    -- revision. Refuse Source origins before any stage writes, including when
    -- the collection's current qualification reader would return no row.
    if v_state.id is not null and exists(
      select 1 from private.bpay_next_case_selection_item si
      join private.bpay_next_work_collection c on c.case_id=si.case_id
        and c.case_component_id=si.case_component_id and c.candidate_id=si.candidate_id
      join private.bpay_next_work w on w.id=c.work_id and w.candidate_id=si.candidate_id
      where si.run_id=v_run_id and si.candidate_id=v_candidate
        and si.selection_revision=v_state.selection_revision and si.is_selected
        and w.work_kind='SOURCE') then
      raise exception using errcode='55000',message='BPAY_NEXT_COMPARISON_NOT_INSTALLED';
    end if;
    if v_state.id is null then
      select s.* into v_selection from private.bpay_next_case_selection s
        where s.run_id=v_run_id and s.candidate_id=v_candidate order by s.selection_revision desc limit 1;
      if v_selection.run_id is null or v_selection.status<>'SEALED' then
        v_issue:='CASE_SELECTION_REQUIRED';exit prepare_page;
      end if;
      -- Comparison is owned by the later CM chain. Existing immutable Source
      -- origin identity is enough to refuse it; no absent pack column/type or
      -- fabricated financial certificate is consulted or created here.
      if exists(select 1 from private.bpay_next_case_selection_item si
        join private.bpay_next_work_collection c on c.case_id=si.case_id
          and c.case_component_id=si.case_component_id and c.candidate_id=si.candidate_id
        join private.bpay_next_work w on w.id=c.work_id and w.candidate_id=si.candidate_id
        where si.run_id=v_run_id and si.candidate_id=v_candidate
          and si.selection_revision=v_selection.selection_revision and si.is_selected
          and w.work_kind='SOURCE') then
        raise exception using errcode='55000',message='BPAY_NEXT_COMPARISON_NOT_INSTALLED';
      end if;
      if v_worker.target_pay_channel<>'PAYE' or v_worker.gross_vat<>0
         or v_worker.gross_ex_vat<>v_worker.gross_inc_vat then
        v_issue:='CASE_TARGET_UNSUPPORTED';exit prepare_page;
      end if;
      select selected.min_take_home_wtd into strict v_floor from private.bpay_next_selection_candidate selected
        where selected.run_id=v_run_id and selected.candidate_id=v_candidate;
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
      -- Exact indexed metadata absence is required BEFORE establishing a new
      -- zero week. Missing prior NEW facts are unresolved, not zero/history SUM.
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
        values(v_worker.id,v_run_id,v_candidate,v_selection.selection_revision,v_worker.captured_preparation_revision,'DRAFT',0,
         v_job.command_id,p_job_id,v_epoch,p_owner_epoch,v_control.financial_view_revision,'BUILDING',
         case when v_week_not_affected then 'NOT_AFFECTED' else 'UNBOUND' end,
         v_selection.selected_count,v_worker.gross_ex_vat,v_worker.gross_ex_vat,
         case when v_week_not_affected then 'INSTRUCTION_CAPTURE' else 'WEEK_CAPTURE' end,
         v_floor,v_week.default_floor_revision,v_week_start,v_week.period_revision,v_worker.gross_ex_vat)
        returning * into v_state;
      insert into private.bpay_next_case_allocation_channel(state_id,allocation_channel,initial_headroom,remaining_headroom)
        values(v_state.id,'PAYE',v_worker.gross_ex_vat,v_worker.gross_ex_vat);
    else
      if v_state.status<>'BUILDING' or v_state.preparation_revision<>v_worker.captured_preparation_revision
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
        select c.* into strict v_case from private.bpay_next_finance_case c where c.id=v_item.case_id and c.candidate_id=v_candidate for update;
        select c.* into strict v_component from private.bpay_next_case_component c
          where c.id=v_item.case_component_id and c.case_id=v_case.id and c.candidate_id=v_candidate for update;
        -- Original Case/component locks precede the original period lock.
        perform 1 from private.bpay_next_case_period p
          where p.case_component_id=v_component.id and p.pay_week_start=v_state.pay_week_start for update;
        begin
          v_plan:=private.bpay_next_case_allocation_plan_v1(jsonb_build_object('mode','DERIVE',
            'candidate_id',v_candidate,'case_id',v_case.id,'case_component_id',v_component.id,
            'pay_date',v_run.pay_date,'pay_week_start',v_state.pay_week_start,
            'target_pay_channel',v_worker.target_pay_channel,'captured_default_floor',v_state.captured_default_floor,
            'captured_default_floor_revision',v_state.captured_default_floor_revision,
            'weekly_binding_state',v_state.weekly_binding_state,'accepted_run_id',v_run_id));
        exception when check_violation then
          if sqlerrm in ('BPAY_NEXT_CASE_INPUT_UNBOUND','BPAY_NEXT_CASE_TARGET_UNSUPPORTED','BPAY_NEXT_CASE_WEEK_BASIS_UNBOUND') then
            v_issue:=substring(sqlerrm from 11);exit prepare_page;
          else raise;end if;
        end;
        v_plan_period:=v_plan->'period';
        if v_plan->>'period_operation'='INSERT' then
          insert into private.bpay_next_case_period(case_component_id,case_id,candidate_id,pay_week_start,rule_id,
            period_revision,opening_outstanding_source_ex_vat,opening_due_source_ex_vat,work_collection_id)
            values(v_component.id,v_case.id,v_candidate,v_state.pay_week_start,(v_plan_period->>'rule_id')::uuid,
              (v_plan_period->>'period_revision')::bigint,(v_plan_period->>'opening_outstanding_source_ex_vat')::numeric,
              (v_plan_period->>'opening_due_source_ex_vat')::numeric,(v_plan_period->>'work_collection_id')::uuid)
            on conflict(case_component_id,pay_week_start) do nothing;
        end if;
        select p.* into strict v_period from private.bpay_next_case_period p
          where p.case_component_id=v_component.id and p.pay_week_start=v_state.pay_week_start for update;
        if v_plan->>'period_operation'='CASE_EDIT_OPENING' then
          perform private.bpay_next_apply_case_period_amendment_v1(
            (v_plan->'period_amendment'->>'edit_command_id')::uuid,v_plan->'period_amendment'->'before',v_plan_period);
          select amended_period.* into strict v_period from private.bpay_next_case_period amended_period
            where case_component_id=v_component.id and pay_week_start=v_state.pay_week_start;
        end if;
        if v_plan->>'period_operation'='FUNDING_ZERO' then
          update private.bpay_next_case_period set rule_id=(v_plan_period->>'rule_id')::uuid,
            opening_outstanding_source_ex_vat=(v_plan_period->>'opening_outstanding_source_ex_vat')::numeric,
            opening_due_source_ex_vat=(v_plan_period->>'opening_due_source_ex_vat')::numeric,period_revision=period_revision+1
            where case_component_id=v_component.id and pay_week_start=v_state.pay_week_start
              and period_revision=(v_plan_period->>'period_revision')::bigint-1
              and (opening_outstanding_source_ex_vat,opening_due_source_ex_vat,realised_recovery_source_ex_vat,
                active_unrealised_recovery_source_ex_vat,active_payout_source_ex_vat)
                is not distinct from (0::numeric,0::numeric,0::numeric,0::numeric,0::numeric) returning * into v_period;
          if not found then raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';end if;
        end if;
        if to_jsonb(v_period) is distinct from v_plan_period then
          raise exception using errcode='23514',message='BPAY_NEXT_CASE_PLAN_PERIOD_CHANGED';end if;
        update private.bpay_next_case_component set resolution_state='RESOLVED',target_pay_channel='PAYE',
          target_ex_vat=(v_plan->'resolution'->>'target_ex_vat')::numeric,target_vat=(v_plan->'resolution'->>'target_vat')::numeric,
          target_inc_vat=(v_plan->'resolution'->>'target_inc_vat')::numeric,
          valuation_policy_id=(v_plan->'resolution'->>'valuation_policy_id')::uuid,
          valuation_window_id=(v_plan->'resolution'->>'valuation_window_id')::uuid,
          resolution_ref=v_plan->'resolution'->>'resolution_ref',component_revision=component_revision+1,
          updated_at_utc=pg_catalog.transaction_timestamp() where id=v_component.id returning * into v_component;
        v_plan:=jsonb_set(v_plan,'{instruction}',(v_plan->'instruction')||jsonb_build_object(
          'run_worker_id',v_worker.id,'run_id',v_run_id,'candidate_id',v_candidate,
          'selection_revision',v_state.selection_revision,'preparation_revision',v_worker.captured_preparation_revision,
          'captured_case_revision',v_case.case_revision,'captured_component_revision',v_component.component_revision,
          'captured_period_revision',v_period.period_revision));
        insert into private.bpay_next_run_case_instruction
          (run_worker_id,run_id,candidate_id,selection_revision,preparation_revision,case_component_id,case_id,
           captured_case_revision,captured_component_revision,captured_period_revision,rule_id,rule_revision,
           case_kind,case_subtype,tax_treatment,instruction_kind,direction,payroll_stage,hold_purpose,
           age_key,debt_age_at_utc,component_ordinal,pay_week_start,source_pay_channel,target_pay_channel,allocation_channel,currency,
           nominal_source_ex_vat,nominal_target_ex_vat,nominal_target_vat,nominal_target_inc_vat,
           capacity_source_ex_vat,capacity_target_ex_vat,capacity_target_vat,capacity_target_inc_vat,
           minimum_earnings_threshold,take_home_floor_override,captured_default_floor,captured_default_floor_revision,
           resolved_take_home_floor,valuation_policy_id,valuation_window_id,resolution_ref,explanation,work_collection_id,
           other_run_capacity_reduction_source_ex_vat,other_run_capacity_reduction_target_ex_vat,other_run_capacity_reason)
          select planned.run_worker_id,planned.run_id,planned.candidate_id,planned.selection_revision,planned.preparation_revision,planned.case_component_id,planned.case_id,planned.captured_case_revision,planned.captured_component_revision,planned.captured_period_revision,planned.rule_id,planned.rule_revision,planned.case_kind,planned.case_subtype,planned.tax_treatment,planned.instruction_kind,planned.direction,planned.payroll_stage,planned.hold_purpose,planned.age_key,planned.debt_age_at_utc,planned.component_ordinal,planned.pay_week_start,planned.source_pay_channel,planned.target_pay_channel,planned.allocation_channel,planned.currency,planned.nominal_source_ex_vat,planned.nominal_target_ex_vat,planned.nominal_target_vat,planned.nominal_target_inc_vat,planned.capacity_source_ex_vat,planned.capacity_target_ex_vat,planned.capacity_target_vat,planned.capacity_target_inc_vat,planned.minimum_earnings_threshold,planned.take_home_floor_override,planned.captured_default_floor,planned.captured_default_floor_revision,planned.resolved_take_home_floor,planned.valuation_policy_id,planned.valuation_window_id,planned.resolution_ref,planned.explanation,planned.work_collection_id,planned.other_run_capacity_reduction_source_ex_vat,planned.other_run_capacity_reduction_target_ex_vat,planned.other_run_capacity_reason from jsonb_populate_record(null::private.bpay_next_run_case_instruction,v_plan->'instruction') planned
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
          and i.preparation_revision=v_worker.captured_preparation_revision and i.selection_revision=v_state.selection_revision;
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
      where c.state_id=v_state.id and c.allocation_channel='PAYE' for update;
    v_e:=v_channel.remaining_headroom;v_h:=v_state.remaining_worker_take_home;
    select coalesce(jsonb_agg(page.value),'[]'::jsonb) into v_plan_instructions from (
      select to_jsonb(i) as value from private.bpay_next_run_case_instruction i
      where i.run_worker_id=v_worker.id and i.preparation_revision=v_worker.captured_preparation_revision
        and i.selection_revision=v_state.selection_revision
        and (v_state.cursor_case_component_id is null or (i.age_key,i.case_id,i.component_ordinal,i.case_component_id)
          >(v_state.cursor_age_key,v_state.cursor_case_id,v_state.cursor_component_ordinal,v_state.cursor_case_component_id))
      order by i.age_key,i.case_id,i.component_ordinal,i.case_component_id limit p_limit
    ) page;
    v_plan:=private.bpay_next_case_allocation_plan_v1(jsonb_build_object('mode','ALLOCATE',
      'instructions',v_plan_instructions,'channel_headroom',v_e,'worker_take_home',v_h));
    for v_plan_row in select value from jsonb_array_elements(v_plan->'rows') loop
      v_instruction:=jsonb_populate_record(null::private.bpay_next_run_case_instruction,v_plan_row->'instruction');
      v_seen:=v_seen+1;
      v_taken:=(v_plan_row->>'taken_amount')::numeric;v_cap_reason:=v_plan_row->>'cap_reason';
      v_affordability_reason:=v_plan_row->>'affordability_reason';v_capacity_reason:=v_plan_row->>'capacity_reason';
      v_next_e:=(v_plan_row->>'channel_headroom_after')::numeric;v_next_h:=(v_plan_row->>'worker_take_home_after')::numeric;
      insert into private.bpay_next_case_allocation_result
        (state_id,instruction_id,run_worker_id,candidate_id,preparation_revision,selection_revision,result_no,
         pass_kind,allocation_channel,hold_purpose,nominal_target_ex_vat,usable_capacity_target_ex_vat,
         allocated_source_ex_vat,allocated_target_ex_vat,allocated_target_vat,allocated_target_inc_vat,shortfall_target_ex_vat,
         cap_reason,affordability_reason,capacity_reason,channel_headroom_before,channel_headroom_after,worker_take_home_before,worker_take_home_after)
        values(v_state.id,v_instruction.id,v_worker.id,v_candidate,v_worker.captured_preparation_revision,v_state.selection_revision,
         v_state.processed_instruction_count+1,'DRAFT','PAYE',v_instruction.hold_purpose,v_instruction.nominal_target_ex_vat,
         v_instruction.capacity_target_ex_vat,v_taken,v_taken,0,v_taken,v_instruction.nominal_target_ex_vat-v_taken,
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
          values(v_worker.id,v_candidate,v_case.id,v_taken,'ACTIVE',v_instruction.id,v_result_id,'DRAFT',
           v_component.id,v_instruction.hold_purpose,v_instruction.pay_week_start,v_taken,v_taken,0,v_taken) returning id into v_hold_id;
    perform private.bpay_next_record_case_hold_transition_v1(p_job_id,v_hold_id,'ACTIVE');
        insert into private.bpay_next_case_capacity_use(case_hold_id,case_component_id,case_id,candidate_id,
          pay_week_start,purpose,status,source_amount_ex_vat)
          values(v_hold_id,v_component.id,v_case.id,v_candidate,v_instruction.pay_week_start,v_instruction.hold_purpose,'ACTIVE',v_taken);
        if v_instruction.hold_purpose='PAYOUT' then
          update private.bpay_next_case_component set active_payout_source_ex_vat=active_payout_source_ex_vat+v_taken,
            component_revision=component_revision+1,updated_at_utc=pg_catalog.transaction_timestamp() where id=v_component.id;
          update private.bpay_next_finance_case set active_payout_hold_amount=active_payout_hold_amount+v_taken,
            active_hold_amount=active_hold_amount+v_taken,case_revision=case_revision+1,updated_at_utc=pg_catalog.transaction_timestamp() where id=v_case.id;
          update private.bpay_next_case_period set active_payout_source_ex_vat=active_payout_source_ex_vat+v_taken,
            period_revision=period_revision+1 where case_component_id=v_component.id and pay_week_start=v_instruction.pay_week_start;
        else
          update private.bpay_next_case_component set active_recovery_source_ex_vat=active_recovery_source_ex_vat+v_taken,
            component_revision=component_revision+1,updated_at_utc=pg_catalog.transaction_timestamp() where id=v_component.id;
          update private.bpay_next_finance_case set active_recovery_hold_amount=active_recovery_hold_amount+v_taken,
            active_hold_amount=active_hold_amount+v_taken,case_revision=case_revision+1,updated_at_utc=pg_catalog.transaction_timestamp() where id=v_case.id;
          update private.bpay_next_case_period set active_unrealised_recovery_source_ex_vat=active_unrealised_recovery_source_ex_vat+v_taken,
            period_revision=period_revision+1 where case_component_id=v_component.id and pay_week_start=v_instruction.pay_week_start;
          v_state.recovered_source_total:=v_state.recovered_source_total+v_taken;
          v_state.recovered_target_total:=v_state.recovered_target_total+v_taken;
        end if;
        update private.bpay_next_run_worker set active_case_hold_count=active_case_hold_count+1 where id=v_worker.id;
      end if;
      if v_instruction.payroll_stage='GROSS_ADD' then v_state.allocated_gross_additions:=v_state.allocated_gross_additions+v_taken;
      elsif v_instruction.payroll_stage='GROSS_DEDUCT' then v_state.allocated_gross_deductions:=v_state.allocated_gross_deductions+v_taken;
      elsif v_instruction.payroll_stage='NET_ADD' then v_state.allocated_net_additions:=v_state.allocated_net_additions+v_taken;end if;
      v_e:=v_next_e;v_h:=v_next_h;
      v_state.processed_instruction_count:=v_state.processed_instruction_count+1;
      v_state.cursor_age_key:=v_instruction.age_key;v_state.cursor_case_id:=v_instruction.case_id;
      v_state.cursor_component_ordinal:=v_instruction.component_ordinal;v_state.cursor_case_component_id:=v_instruction.case_component_id;
      update private.bpay_next_run_worker set case_allocated_count=case_allocated_count+1 where id=v_worker.id;
    end loop;
    update private.bpay_next_case_allocation_channel set remaining_headroom=v_e where state_id=v_state.id and allocation_channel='PAYE';
    update private.bpay_next_case_allocation_state set processed_instruction_count=v_state.processed_instruction_count,
      remaining_worker_take_home=v_h,recovered_source_total=v_state.recovered_source_total,recovered_target_total=v_state.recovered_target_total,
      cursor_age_key=v_state.cursor_age_key,cursor_case_id=v_state.cursor_case_id,cursor_component_ordinal=v_state.cursor_component_ordinal,
      cursor_case_component_id=v_state.cursor_case_component_id,allocated_gross_additions=v_state.allocated_gross_additions,
      allocated_gross_deductions=v_state.allocated_gross_deductions,allocated_net_additions=v_state.allocated_net_additions where id=v_state.id;
    select exists(select 1 from private.bpay_next_run_case_instruction i where i.run_worker_id=v_worker.id
      and i.preparation_revision=v_worker.captured_preparation_revision and i.selection_revision=v_state.selection_revision
      and (v_state.cursor_case_component_id is null or (i.age_key,i.case_id,i.component_ordinal,i.case_component_id)
        >(v_state.cursor_age_key,v_state.cursor_case_id,v_state.cursor_component_ordinal,v_state.cursor_case_component_id))) into v_has_more;
    if not v_has_more then
      if v_state.processed_instruction_count<>v_state.expected_instruction_count then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_PREPARE_ALLOCATION_COUNT_MISMATCH';
      end if;
      v_gross:=v_state.captured_work_gross+v_state.allocated_gross_additions-v_state.allocated_gross_deductions;
      if v_gross<0 or v_gross>=10000000000000000 or v_state.allocated_net_additions>=10000000000000000 then
        v_issue:='CASE_INPUT_UNBOUND';exit prepare_page;
      end if;
      -- Preserve certified-zero WORK/Source rows; case-only NET additions are
      -- real payable cash, not a made-up positive payroll gross or net.
      if v_worker.captured_line_count=0 and v_gross=0 and v_state.allocated_net_additions=0 then
        v_issue:='CASE_NO_PAYABLE_AMOUNT';exit prepare_page;
      end if;
      if v_worker.captured_line_count=0 and v_gross=0 and v_state.payout_instruction_count>0 then
        insert into private.bpay_next_case_payout_basis(run_worker_id,candidate_id,preparation_revision,currency,
          beneficiary_kind,beneficiary_id,expected_instruction_count,captured_instruction_count,source_total_ex_vat,
          target_total_ex_vat,target_total_vat,target_total_inc_vat,status)
          values(v_worker.id,v_candidate,v_worker.captured_preparation_revision,'GBP','CANDIDATE',v_candidate,
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
      update private.bpay_next_run_worker set gross_ex_vat=v_gross,gross_vat=0,gross_inc_vat=v_gross,
        status='READY',case_pending_binding_count=0 where id=v_worker.id;

      v_terminal:=true;
    end if;
  end prepare_page;

  if v_issue is not null then
    -- Candidate-wide issue: WORK FK remains NULL. Earlier frozen WORK and any
    -- already-created ACTIVE reservations remain exact and non-confirmable.
    update private.bpay_next_run_worker set status='REVIEW',review_issue_code=v_issue,review_issue_work_id=null,
      financial_resolution_count=financial_resolution_count+1,case_pending_binding_count=
        case when v_issue in ('CASE_SELECTION_REQUIRED','CASE_INPUT_UNBOUND','CASE_WEEK_BASIS_UNBOUND','CASE_RETURN_FLOOR_UNBOUND') then 1 else 0 end
      where id=v_worker.id;
    if v_state.id is not null then
      update private.bpay_next_case_allocation_state set retained_prepare_stage=v_state.prepare_stage,retained_weekly_binding_state=v_state.weekly_binding_state,
        prepare_stage='REVIEW',weekly_binding_state='UNBOUND',
        instruction_capture_cursor=v_state.instruction_capture_cursor,captured_instruction_count=v_state.captured_instruction_count,
        payout_instruction_count=v_state.payout_instruction_count where id=v_state.id;
    end if;

    v_terminal:=true;
  elsif not v_terminal then
    update private.bpay_next_job set cursor_key=(v_saved_cursor+1)::text where id=p_job_id;
  end if;
  update private.bpay_next_worker_control set financial_view_revision=financial_view_revision+1,
    updated_at_utc=pg_catalog.transaction_timestamp() where candidate_id=v_candidate;
  if v_terminal then
    return private.bpay_next_begin_prepare_projection_v1(p_job_id,p_lease_nonce,p_owner_epoch,v_seen::integer);
  end if;
  return pg_catalog.jsonb_build_object('done',v_terminal,'phase',case when v_issue is not null then 'REVIEW'
    when v_terminal then 'READY' else 'FINANCE_ALLOCATE' end,'cursor',case when v_terminal then null else (v_saved_cursor+1)::text end,
    'rows_visited',v_seen::text,'replay',false,'run_worker_id',v_worker.id,
    'worker_status',case when v_issue is not null then 'REVIEW' when v_terminal then 'READY' else 'PREPARING' end)
    ||case when v_issue is not null then pg_catalog.jsonb_build_object('issue_code',v_issue) else '{}'::jsonb end;
end
$function$;


notify pgrst,'reload schema';
commit;
