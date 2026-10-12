-- WB41 BD131: exact origin WAIT recovery; full current1939/0559/0041 successors.
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
      left join private.bpay_next_run_case_instruction ci on ci.run_worker_id=rw.id and ci.preparation_revision=rw.captured_preparation_revision
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
    -- WB41: preserve v_base for RESERVED/accepted portions. Only this exact
    -- automatic origin's unaccepted AVAILABLE recovery can enter WAIT.
    if v_available>0 then
      if not v_item.is_selected and v_item.reason_priority>84
        and (v_case.case_kind,v_component.case_kind,v_component.case_subtype,
          v_component.instruction_kind,v_component.direction,v_component.payroll_stage)
          is not distinct from ('OVERPAYMENT','OVERPAYMENT','OVERPAYMENT','RECOVERY','DEDUCTION','GROSS_DEDUCT')
        and exists(select 1 from private.bpay_next_work_collection origin
          join private.bpay_next_work work on work.id=origin.work_id and work.candidate_id=origin.candidate_id
          where origin.case_id=v_case.id and origin.case_component_id=v_component.id and origin.candidate_id=v_candidate
            and origin.paid_basis_qualified and origin.qualification_state='READY'
            and private.bpay_next_work_wait_state_v1(work.id)) then
        v_item.section:='ACTION';v_item.reason_code:='ORIGIN_UNAUTHORISED_WAIT';v_item.reason_priority:=84;v_item.selectable:=false;
      end if;
      v_item.portion_kind:='AVAILABLE';insert into private.bpay_next_workbench_item select (v_item).*;
    end if;
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
  perform private.bpay_next_workbench_refresh_group_display_v1(v_group);
end
$function$;

create or replace function public.bpay_next_workbench_page_v1(p_request jsonb)
returns jsonb language plpgsql stable security definer
set search_path=pg_catalog,private,public
set statement_timeout='5s'
as $function$
declare
  v_source_receipt private.weekly_source_entitlement_publication_receipts%rowtype;v_source_member record;v_source_no integer;v_source_count integer;
  v_initial_a public.weekly_source_root_authorisations%rowtype;v_initial_t public.timesheets%rowtype;
  v_initial_f public.timesheets_financials%rowtype;v_initial_w private.bpay_next_work%rowtype;
  v_initial_r private.bpay_next_work_revision%rowtype;v_initial_p private.bpay_next_publication%rowtype;
  v_initial_c public.contracts%rowtype;v_initial_line private.bpay_next_approved_line%rowtype;
  v_initial_inventory jsonb;v_initial_basis jsonb;v_initial_origin jsonb;v_initial_details jsonb;
  v_initial_cert jsonb;v_initial_component jsonb;v_initial_n integer;
  v_est jsonb;v_descriptor jsonb;v_est_intent jsonb;v_est_scope jsonb;v_versions jsonb;v_est_leaves jsonb;v_est_instructions jsonb:='[]';
  v_est_plan jsonb;v_est_rows jsonb:='[]';v_est_row jsonb;v_est_key jsonb;v_est_last jsonb;v_est_bound jsonb;
  v_est_candidate public.candidates%rowtype;v_est_position private.bpay_next_position%rowtype;
  v_est_week private.bpay_next_worker_week%rowtype;v_est_binding private.bpay_next_source_component_binding_version%rowtype;
  v_est_record record;v_est_value record;v_est_source numeric:=0;v_est_gross numeric:=0;v_est_h numeric:=0;
  v_est_gross_add numeric:=0;v_est_gross_deduct numeric:=0;v_est_net_add numeric:=0;v_est_net_deduct numeric:=0;
  v_est_financial bigint;v_est_week_start date;v_est_count bigint;v_est_seen integer:=0;
  v_est_work_ids uuid[];v_est_components uuid[];v_est_wait_components uuid[];v_est_all_overpayment boolean;v_est_state text:='ESTIMATE';
  v_actor uuid;v_kind text;v_scope jsonb;v_context jsonb;v_after jsonb;v_cursor jsonb;
  v_page jsonb;v_response jsonb;v_read_scope jsonb;v_ns jsonb;v_memory jsonb;v_baseline uuid;v_revision private.bpay_next_workbench_revision%rowtype;
  v_epoch bigint;v_agency uuid;v_date_context jsonb;v_limit integer;v_error text;
  v_key_count integer;v_key jsonb;v_sort text;v_text text;v_keys text[];v_count_selection jsonb;v_progress jsonb;
begin
  -- STABLE reads share a single statement snapshot. No volatile authority or
  -- FOR SHARE helper; these are the existing service/active-admin predicates.
  if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),
    nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception using errcode='42501',message='BPAY_NEXT_WORKBENCH_FORBIDDEN';end if;
  -- WB-39 read-only native snooze management; original financial owners retained.
  if p_request->>'kind'='SNOOZES' then
    if jsonb_typeof(p_request)<>'object' or not(p_request?&array['actor_user_id','kind'])
      or jsonb_typeof(p_request->'actor_user_id')<>'string'
      or p_request->>'actor_user_id'!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
      raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
    return private.bpay_next_snooze_management_page_v1((p_request->>'actor_user_id')::uuid,p_request-array['actor_user_id','kind']);
  end if;
  -- WB-38 true read: context only; acknowledgement issuance remains W2.
  if p_request->>'kind'='SNOOZE_CHECK' then
    if jsonb_typeof(p_request)<>'object' or octet_length(p_request::text)>8192
      or not(p_request ?& array['actor_user_id','kind','snooze'])
      or p_request-array['actor_user_id','kind','snooze']<>'{}'::jsonb
      or jsonb_typeof(p_request->'actor_user_id') is distinct from 'string'
      or p_request->>'actor_user_id'!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
      raise exception using errcode='22023',message='BPAY_NEXT_SNOOZE_REQUEST_INVALID';end if;
    v_actor:=(p_request->>'actor_user_id')::uuid;
    return jsonb_build_object('ok',true,'kind','SNOOZE_CHECK',
      'context',private.bpay_next_snooze_context_v1(v_actor,p_request->'snooze'));
  end if;
  p_request:=p_request||jsonb_build_object('count_selection',coalesce(p_request->'count_selection','null'::jsonb),
    'progress_after',coalesce(p_request->'progress_after','null'::jsonb),'selection_memory',coalesce(p_request->'selection_memory','null'::jsonb),'estimate',coalesce(p_request->'estimate','null'::jsonb));
  v_keys:=array['actor_user_id','kind','candidate_id','client_id','channel','section',
    'search','sort','group_key','item_id','reason_code','after','limit','count_selection','progress_after','selection_memory','estimate'];
  if p_request is null or jsonb_typeof(p_request)<>'object' or octet_length(p_request::text)>32768
    or not(p_request ?& v_keys) or p_request-v_keys<>'{}'::jsonb then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  if jsonb_typeof(p_request->'actor_user_id')<>'string'
    or p_request->>'actor_user_id'!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
    raise exception using errcode='42501',message='BPAY_NEXT_WORKBENCH_FORBIDDEN';end if;
  v_actor:=(p_request->>'actor_user_id')::uuid;
  if not exists(select 1 from public.tms_users u where u.id=v_actor and u.is_active is true and u.role::text='admin') then
    raise exception using errcode='42501',message='BPAY_NEXT_WORKBENCH_FORBIDDEN';end if;
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m where m.id=1 and m.active_owner='NEXT';
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select r.* into strict v_revision from private.bpay_next_workbench_revision r where r.id=1;
  select d.installation_id into strict v_agency from private.cloudtms_database_identity d where d.singleton;
  v_kind:=p_request->>'kind';v_sort:=p_request->>'sort';
  if jsonb_typeof(p_request->'kind')<>'string'
    or v_kind not in ('HEADER','CANDIDATES','ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL',
      'CANDIDATE_GROUPS','GROUP_LINES','CANDIDATE_ESTIMATE','PICK_CANDIDATES','PICK_CLIENTS','EXPORT_ROWS')
    or jsonb_typeof(p_request->'channel')<>'string' or p_request->>'channel' not in ('ALL','PAYE','UMBRELLA')
    or jsonb_typeof(p_request->'section')<>'string' or p_request->>'section' not in ('ALL','READY','ACTION','BLOCKED','WAITING')
    or jsonb_typeof(p_request->'sort')<>'string' or (v_kind='EXPORT_ROWS' and v_sort not in ('C_NAME_ASC','C_NAME_DESC','C_AMOUNT_ASC','C_AMOUNT_DESC','C_DEDUCTIONS_ASC','C_DEDUCTIONS_DESC','C_STATUS_ASC','C_STATUS_DESC','G_PAYMENT_ASC','G_PAYMENT_DESC','G_CLIENT_ASC','G_CLIENT_DESC','G_CHANNEL_ASC','G_CHANNEL_DESC','G_AMOUNT_ASC','G_AMOUNT_DESC','G_DEDUCTIONS_ASC','G_DEDUCTIONS_DESC','G_REASON_ASC','G_REASON_DESC'))
    or (v_kind<>'EXPORT_ROWS' and (false or v_sort not in ('NAME_ASC','NAME_DESC','AMOUNT_ASC','AMOUNT_DESC','DEDUCTIONS_ASC','DEDUCTIONS_DESC','PAYMENT_ASC','PAYMENT_DESC','STATUS_ASC','STATUS_DESC','REASON_ASC','REASON_DESC','CLIENT_ASC','CLIENT_DESC','CHANNEL_ASC','CHANNEL_DESC')
    or (v_kind='CANDIDATES' and v_sort like 'PAYMENT_%')
    or (v_kind<>'CANDIDATES' and v_sort like 'STATUS_%')
    or (v_kind not in ('ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL','CANDIDATE_GROUPS') and v_sort like 'REASON_%')
    or (v_kind in ('ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL') and v_sort like 'DEDUCTIONS_%')
    or (v_kind not in ('CANDIDATES','ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL','CANDIDATE_GROUPS') and v_sort<>'NAME_ASC')
    or (v_sort like 'CLIENT_%' or v_sort like 'CHANNEL_%') and v_kind<>'CANDIDATE_GROUPS'
    or (v_kind='CANDIDATE_GROUPS' and v_sort='NAME_DESC')
))
    or jsonb_typeof(p_request->'search')<>'string' or length(p_request->>'search')>200
    or octet_length(p_request->>'search')>800
    or btrim(p_request->>'search')<>p_request->>'search'
    or jsonb_typeof(p_request->'limit')<>'number' or p_request->>'limit'!~'^[1-9][0-9]{0,2}$' then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  v_limit:=(p_request->>'limit')::integer;
  if v_limit not between 1 and 100 or (v_kind='CANDIDATE_GROUPS' and v_limit>10) then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  foreach v_text in array array['candidate_id','client_id','item_id'] loop
    if p_request->v_text<>'null'::jsonb and (jsonb_typeof(p_request->v_text)<>'string'
      or p_request->>v_text!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$') then
      raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  end loop;
  if p_request->'group_key'<>'null'::jsonb and (jsonb_typeof(p_request->'group_key')<>'string'
    or p_request->>'group_key'!~'^(WORK|CASE|ROW):[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$') then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  if p_request->>'group_key' like 'ROW:%' and v_kind not in ('GROUP_LINES','CANDIDATE_GROUPS') then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  if p_request->'reason_code'<>'null'::jsonb and (jsonb_typeof(p_request->'reason_code')<>'string'
    or p_request->>'reason_code'!~'^[A-Z][A-Z0-9_]{0,255}$') then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  if (v_kind in ('CANDIDATE_GROUPS','GROUP_LINES','CANDIDATE_ESTIMATE') and p_request->'candidate_id'='null'::jsonb)
    or (v_kind in ('GROUP_LINES','ISSUE_DETAIL') and p_request->'group_key'='null'::jsonb)
    or (v_kind='ISSUE_DETAIL' and p_request->'reason_code'='null'::jsonb)
    or (v_kind not in ('GROUP_LINES','ISSUE_DETAIL','CANDIDATE_GROUPS') and (p_request->'group_key'<>'null'::jsonb or p_request->'item_id'<>'null'::jsonb or p_request->'reason_code'<>'null'::jsonb)
    or (v_kind='CANDIDATE_GROUPS' and (p_request->'item_id'<>'null'::jsonb or p_request->'reason_code'<>'null'::jsonb)))
    or (v_kind not in ('CANDIDATES','PICK_CANDIDATES','PICK_CLIENTS','ACTION_REQUIRED','BLOCKED','EXPORT_ROWS') and p_request->>'search'<>'')
    or (v_kind='HEADER' and p_request->'after'<>'null'::jsonb) then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  v_count_selection:=p_request->'count_selection';
  if v_count_selection ? 'intent' then
   if v_kind<>'HEADER' or p_request->>'section'<>'READY' or jsonb_typeof(v_count_selection)<>'object'
    or not(v_count_selection ?& array['intent','memory_baseline_run_id'])
    or v_count_selection-array['intent','memory_baseline_run_id']<>'{}'::jsonb
    or not private.bpay_next_selection_memory_intent_valid_v1(v_count_selection->'intent') then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  else
  if v_count_selection<>'null'::jsonb then
    if v_kind<>'HEADER' or p_request->>'section'<>'READY' or jsonb_typeof(v_count_selection)<>'object'
      or not(v_count_selection ?& array['mode','candidate_ids'])
      or v_count_selection-array['mode','candidate_ids']<>'{}'::jsonb
      or jsonb_typeof(v_count_selection->'mode')<>'string' or v_count_selection->>'mode' not in ('ALL','NONE')
      or jsonb_typeof(v_count_selection->'candidate_ids')<>'array' then
      raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
    if exists(select 1 from jsonb_array_elements(v_count_selection->'candidate_ids') a(value)
      where jsonb_typeof(a.value)<>'string' or (a.value#>>'{}')!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$')
      or (select count(*)<>count(distinct a.value) from jsonb_array_elements(v_count_selection->'candidate_ids') a(value)) then
      raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  end if;
  end if;
  if p_request->'progress_after'<>'null'::jsonb and v_kind<>'HEADER' then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  v_scope:=p_request-array['actor_user_id','kind','after','limit','progress_after','estimate'];
  -- Fixed effective sections, explicit normalization; client cannot silently
  -- continue a cursor issued for a different list.
  if v_kind='CANDIDATES' then v_scope:=jsonb_set(v_scope,'{section}','"READY"'::jsonb);
  elsif v_kind='ACTION_REQUIRED' then v_scope:=jsonb_set(v_scope,'{section}','"ACTION"'::jsonb);
  elsif v_kind='BLOCKED' then v_scope:=jsonb_set(v_scope,'{section}','"BLOCKED"'::jsonb);end if;
  v_date_context:=public.pay_banking_official_date_context_v1(null::timestamptz);
  if v_date_context->>'ok'<>'true' or v_date_context->>'current_official_pay_date' is null then
    raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_DATE_UNAVAILABLE';end if;
  v_context:=jsonb_build_object('kind',v_kind,'agency_id',v_agency,'actor_user_id',v_actor,
    'owner_epoch',v_epoch::text,'official_pay_date',v_date_context->>'current_official_pay_date',
    'workbench_revision',v_revision.workbench_revision::text,'adoption_revision',v_revision.adoption_revision::text,
    'scope',v_scope);
  if v_kind='HEADER' then
    v_progress:=private.bpay_next_workbench_job_progress_v1(v_context,p_request->'progress_after',v_limit);
  end if;
  if v_revision.adoption_state<>'CURRENT' or v_revision.dirty_candidate_count<>0
    or v_revision.dirty_group_count<>0 or v_revision.pending_task_count<>0 then
    v_response:=jsonb_build_object('ok',false,'error_code','BPAY_NEXT_WORKBENCH_PROJECTION_NOT_CURRENT',
      'retryable',true,'reload_required',false,'workbench_revision',v_revision.workbench_revision::text,
      'complete',false,'projection_state',v_revision.adoption_state,'dirty_candidate_count',v_revision.dirty_candidate_count::text,
      'dirty_group_count',v_revision.dirty_group_count::text,'pending_task_count',v_revision.pending_task_count::text);
    if v_kind='HEADER' then v_response:=v_response||jsonb_build_object('job_progress',v_progress);end if;
    if octet_length(v_response::text)>120000 then raise exception using errcode='54000',message='BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE';end if;
    return v_response;
  end if;
  v_ns:=jsonb_build_object('agency_id',v_agency,'owner_epoch',v_epoch::text,'actor_user_id',v_actor,
   'official_pay_date',v_date_context->>'current_official_pay_date','scope',jsonb_build_object(
    'candidate_id',v_scope->'candidate_id','client_id',v_scope->'client_id','channel',v_scope->'channel'));
  v_baseline:=private.bpay_next_selection_memory_resolve_v1(v_ns,p_request->'selection_memory');
  v_memory:=jsonb_build_object('run_id',v_baseline,'scope',coalesce(nullif(p_request->'selection_memory'->'scope','null'::jsonb),v_ns->'scope'));
  if v_count_selection ? 'intent' and v_count_selection->'memory_baseline_run_id' is distinct from v_memory->'run_id' then
   raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_CURSOR_STALE';end if;
  if v_count_selection ? 'intent' then
   -- Same native identities as the current memory-aware Create owner.
   -- Component-only edits inherit defaults; supplied Work parents must match.
   if exists(select 1 from jsonb_array_elements(v_count_selection->'intent'->'candidates') chosen
     where not exists(select 1 from public.candidates candidate where candidate.id=(chosen->>'candidate_id')::uuid))
    or exists(select 1 from jsonb_array_elements(v_count_selection->'intent'->'components') c(value)
     where case when exists(select 1 from jsonb_array_elements(v_count_selection->'intent'->'works') work
       where work->>'work_id'=c.value->>'work_id') then not exists(
        select 1 from jsonb_array_elements(v_count_selection->'intent'->'works') work
         where work->>'work_id'=c.value->>'work_id' and work->>'candidate_id'=c.value->>'candidate_id'
          and work->>'expected_revision_id'=c.value->>'expected_revision_id')
      else not exists(select 1 from private.bpay_next_work w join private.bpay_next_work_revision r on r.work_id=w.id
       join private.bpay_next_workbench_item i on i.work_id=w.id and i.revision_id=r.id
       join private.bpay_next_workbench_group g on g.group_key=i.group_key and g.candidate_id=i.candidate_id
       where w.id=(c.value->>'work_id')::uuid and w.candidate_id=(c.value->>'candidate_id')::uuid
        and r.id=(c.value->>'expected_revision_id')::uuid and w.current_revision_id=r.id and w.applied_revision_id=r.id
        and i.approved_line_id=(c.value->>'approved_line_id')::uuid and i.candidate_id=w.candidate_id
        and i.section='READY' and i.selectable and i.amount_known and not i.is_selected
        and i.portion_kind in ('AVAILABLE','ZERO') and g.complete and not g.dirty
        and g.progress_state='CURRENT' and i.progress_state='CURRENT'
        and i.projection_revision=g.projection_revision and i.financial_view_revision=g.financial_view_revision
        and (v_memory->'scope'->>'candidate_id' is null or i.candidate_id=(v_memory->'scope'->>'candidate_id')::uuid)
        and (v_memory->'scope'->>'client_id' is null or i.client_id=(v_memory->'scope'->>'client_id')::uuid)
        and (v_memory->'scope'->>'channel'='ALL' or i.target_pay_channel=v_memory->'scope'->>'channel')) end) then
    raise exception using errcode='22023',message='BPAY_NEXT_CHOICE_NATIVE_IDENTITY_INVALID';end if;
   if exists(select 1 from unnest(array['works','components']) kind
    cross join lateral jsonb_array_elements(v_count_selection->'intent'->kind) chosen
    where not exists(select 1 from private.bpay_next_work w join private.bpay_next_work_revision wr on wr.work_id=w.id
     where w.id=(chosen->>'work_id')::uuid and w.candidate_id=(chosen->>'candidate_id')::uuid
      and wr.id=(chosen->>'expected_revision_id')::uuid and w.current_revision_id=wr.id and w.applied_revision_id=wr.id
      and (kind='works' or exists(select 1 from private.bpay_next_approved_line l where l.id=(chosen->>'approved_line_id')::uuid and l.revision_id=wr.id))))
    or exists(select 1 from jsonb_array_elements(v_count_selection->'intent'->'cases') chosen
     where not exists(select 1 from private.bpay_next_case_component cc where cc.id=(chosen->>'case_component_id')::uuid and cc.candidate_id=(chosen->>'candidate_id')::uuid)) then
    raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_CURSOR_STALE';end if;
  end if;
  v_context:=v_context||jsonb_build_object('selection_memory',v_memory);
  if v_kind='CANDIDATE_ESTIMATE' then
    v_est:=p_request->'estimate';v_descriptor:=v_est->'descriptor';
    if p_request->>'section'<>'READY' or p_request->'count_selection'<>'null'::jsonb
      or jsonb_typeof(v_est) is distinct from 'object'
      or not(v_est ?& array['descriptor','expected_financial_view_revision','versions','verify_only'])
      or v_est-array['descriptor','expected_financial_view_revision','versions','verify_only']<>'{}'::jsonb
      or jsonb_typeof(v_est->'verify_only') is distinct from 'boolean'
      or (v_est->'versions'='null'::jsonb and (v_est->'verify_only'='true'::jsonb or p_request->'after'<>'null'::jsonb))
      or jsonb_typeof(v_est->'expected_financial_view_revision') is distinct from 'string'
      or v_est->>'expected_financial_view_revision'!~'^(0|[1-9][0-9]{0,18})$'
      or (v_est->'versions'<>'null'::jsonb and (jsonb_typeof(v_est->'versions')<>'array' or jsonb_array_length(v_est->'versions')>8000))
      or jsonb_typeof(v_descriptor) is distinct from 'object'
      or not(v_descriptor ?& array['namespace','intent','revision','workbench_revision','adoption_revision','memory_baseline_run_id'])
      or v_descriptor-array['namespace','intent','revision','workbench_revision','adoption_revision','memory_baseline_run_id']<>'{}'::jsonb
      or not private.bpay_next_selection_memory_intent_valid_v1(v_descriptor->'intent')
      or jsonb_typeof(v_descriptor->'revision') is distinct from 'number'
      or v_descriptor->>'revision'!~'^(0|[1-9][0-9]{0,15})$' then
      raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
    if v_descriptor->'namespace' is distinct from jsonb_set(v_ns,'{scope}',v_memory->'scope')
      or v_descriptor->'memory_baseline_run_id' is distinct from v_memory->'run_id' then
      raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_CURSOR_STALE';end if;
    select c.* into strict v_est_candidate from public.candidates c where c.id=(p_request->>'candidate_id')::uuid;
    select c.financial_view_revision into strict v_est_financial from private.bpay_next_worker_control c where c.candidate_id=v_est_candidate.id;
    v_est_week_start:=(v_date_context->>'current_official_pay_date')::date-
      (extract(isodow from (v_date_context->>'current_official_pay_date')::date)::integer-1);
    if v_descriptor->>'workbench_revision' is distinct from v_revision.workbench_revision::text
      or v_descriptor->>'adoption_revision' is distinct from v_revision.adoption_revision::text
      or v_est->>'expected_financial_view_revision' is distinct from v_est_financial::text then v_est_state:='OUTDATED';end if;
    -- Bound the current indexed Candidate membership BEFORE choices materialises LEAF.
    select count(*) into v_est_count from (select 1 from private.bpay_next_workbench_item i
      join private.bpay_next_workbench_group g on g.group_key=i.group_key and g.candidate_id=i.candidate_id
      where i.candidate_id=v_est_candidate.id and
        ((i.section='READY' and i.selectable) or
          (i.section='ACTION' and i.reason_code='ORIGIN_UNAUTHORISED_WAIT' and i.member_kind='CASE_COMPONENT'))
        and i.amount_known and not i.is_selected
        and i.portion_kind in ('AVAILABLE','ZERO') and g.complete and not g.dirty
        and g.progress_state='CURRENT' and i.progress_state='CURRENT'
        and i.projection_revision=g.projection_revision and i.financial_view_revision=g.financial_view_revision
        and (v_scope->>'client_id' is null or i.client_id=(v_scope->>'client_id')::uuid)
        and (v_scope->>'channel'='ALL' or i.target_pay_channel=v_scope->>'channel') limit 1001) bounded;
    if v_est_count>1000 then v_est_state:='PENDING';end if;
    if v_est_state='ESTIMATE' then
      begin
        -- Only this Candidate's native overrides can affect their estimate.
        v_est_intent:=jsonb_build_object('mode',v_descriptor->'intent'->'mode');
        for v_est_record in select unnest(array['candidates','works','components','cases']) kind loop
          v_est_intent:=v_est_intent||jsonb_build_object(v_est_record.kind,(select coalesce(jsonb_agg(x.value),'[]'::jsonb)
            from jsonb_array_elements(v_descriptor->'intent'->v_est_record.kind) x(value) where x.value->>'candidate_id'=v_est_candidate.id::text));
        end loop;
        with chosen as materialized(select * from private.bpay_next_selection_memory_choices_v1(
          v_scope,v_baseline,v_est_intent) where selected),
        leaves as materialized(select * from private.bpay_next_selection_memory_leaves_v1(v_scope))
        select coalesce(jsonb_agg(jsonb_build_object('leaf_key',l.leaf_key,'candidate_id',l.candidate_id,'work_id',l.work_id,'revision_id',l.revision_id,'approved_line_id',l.approved_line_id,'case_component_id',l.case_component_id,'basis',l.basis) order by l.leaf_key),'[]'::jsonb) into v_est_leaves from leaves l
          join chosen c on c.candidate_id=l.candidate_id and
            (c.work_id=l.work_id and c.revision_id=l.revision_id and c.approved_line_id=l.approved_line_id
              or c.case_component_id=l.case_component_id);
        if jsonb_array_length(v_est_leaves)>1000 then
          raise exception using errcode='54000',message='BPAY_NEXT_ESTIMATE_BUDGET';end if;
        select coalesce(array_agg(distinct (l->>'work_id')::uuid) filter(where l->>'work_id' is not null),'{}'),
          coalesce(array_agg(distinct (l->>'case_component_id')::uuid) filter(where l->>'case_component_id' is not null),'{}')
          into v_est_work_ids,v_est_components from jsonb_array_elements(v_est_leaves) l;
        -- WB41 information only: paused recoveries are not choices or LEAF
        -- admission. Their original native versions join the existing estimate
        -- verification and plan, so Outstanding and zero recoverable stay distinct.
        select coalesce(array_agg(bounded.case_component_id),'{}') into v_est_wait_components from (
          select i.case_component_id from private.bpay_next_workbench_item i
            join private.bpay_next_workbench_group g on g.group_key=i.group_key and g.candidate_id=i.candidate_id
            where i.candidate_id=v_est_candidate.id and i.section='ACTION'
              and i.reason_code='ORIGIN_UNAUTHORISED_WAIT' and i.member_kind='CASE_COMPONENT'
              and i.portion_kind='AVAILABLE' and i.amount_known and not i.is_selected
              and i.accepted_run_id is null and i.case_component_id is not null
              and g.complete and not g.dirty and g.progress_state='CURRENT' and i.progress_state='CURRENT'
              and i.projection_revision=g.projection_revision and i.financial_view_revision=g.financial_view_revision
              and (v_scope->>'client_id' is null or i.client_id=(v_scope->>'client_id')::uuid)
              and (v_scope->>'channel'='ALL' or i.target_pay_channel=v_scope->>'channel')
          limit 1001) bounded;
        select coalesce(array_agg(distinct id),'{}') into v_est_components
          from unnest(v_est_components||v_est_wait_components) id;
        if cardinality(v_est_work_ids)+cardinality(v_est_components)>1000 then
          raise exception using errcode='54000',message='BPAY_NEXT_ESTIMATE_BUDGET';end if;
        select count(*) into v_est_count from (select 1 from private.bpay_next_worker_week_contribution x
          where x.candidate_id=v_est_candidate.id and x.pay_week_start=v_est_week_start
            and x.original_pay_date<=(v_date_context->>'current_official_pay_date')::date limit 1001) bounded;
        if v_est_count>1000 then raise exception using errcode='54000',message='BPAY_NEXT_ESTIMATE_BUDGET';end if;
        if exists(select 1 from jsonb_array_elements(v_descriptor->'intent'->'works') chosen
          where chosen->>'candidate_id'=v_est_candidate.id::text and not exists(select 1 from private.bpay_next_work w
            where w.id=(chosen->>'work_id')::uuid and w.candidate_id=v_est_candidate.id
              and w.current_revision_id=(chosen->>'expected_revision_id')::uuid and w.applied_revision_id=w.current_revision_id))
          or exists(select 1 from jsonb_array_elements(v_descriptor->'intent'->'components') chosen
            where chosen->>'candidate_id'=v_est_candidate.id::text and not exists(select 1 from private.bpay_next_work w
              join private.bpay_next_approved_line l on l.revision_id=w.current_revision_id
              where w.id=(chosen->>'work_id')::uuid and w.candidate_id=v_est_candidate.id
                and l.id=(chosen->>'approved_line_id')::uuid and w.current_revision_id=(chosen->>'expected_revision_id')::uuid
                and w.applied_revision_id=w.current_revision_id))
          or exists(select 1 from jsonb_array_elements(v_descriptor->'intent'->'cases') chosen
            where chosen->>'candidate_id'=v_est_candidate.id::text and not exists(select 1 from private.bpay_next_case_component cc
              where cc.id=(chosen->>'case_component_id')::uuid and cc.candidate_id=v_est_candidate.id)) then
          raise exception using errcode='23514',message='BPAY_NEXT_ESTIMATE_INPUT_UNBOUND';end if;
        select count(*) into v_est_count from (select 1 from private.bpay_next_approved_line l
          join private.bpay_next_work w on w.current_revision_id=l.revision_id where w.id=any(v_est_work_ids) limit 1001) bounded;
        if v_est_count>1000 then raise exception using errcode='54000',message='BPAY_NEXT_ESTIMATE_BUDGET';end if;
        if exists(select 1 from private.bpay_next_valuation_policy_window pw
          where pw.policy_id in (select r.valuation_policy_id from private.bpay_next_work_revision r join private.bpay_next_work w on w.current_revision_id=r.id where w.id=any(v_est_work_ids)
            union select o.valuation_policy_id from private.bpay_next_case_create_request o join private.bpay_next_case_component cc on cc.case_id=o.case_id where cc.id=any(v_est_components)
            union select o.original_valuation_policy_id from private.bpay_next_work_collection o where o.case_component_id=any(v_est_components))
          and (v_date_context->>'current_official_pay_date')::date>=pw.date_from and (v_date_context->>'current_official_pay_date')::date<=coalesce(pw.date_to,'infinity'::date)
          and pw.date_from=(select max(winner.date_from::date) from private.bpay_next_valuation_policy_window winner where winner.policy_id=pw.policy_id and (v_date_context->>'current_official_pay_date')::date>=winner.date_from and (v_date_context->>'current_official_pay_date')::date<=coalesce(winner.date_to,'infinity'::date))
          group by pw.policy_id having count(*)>1) then raise exception using errcode='23514',message='BPAY_NEXT_ESTIMATE_INPUT_UNBOUND';end if;
        -- Native identities and versions only; no monetary carry is returned to the browser.
        if exists(select 1 from private.bpay_next_source_component_binding b
          join private.bpay_next_source_component_binding_version bv on bv.id=b.current_binding_version_id
          join private.weekly_source_entitlement_publication_receipts receipt on receipt.id=bv.source_receipt_id
          where b.canonical_work_id=any(v_est_work_ids) and exists(select 1 from jsonb_array_elements(v_est_leaves) l where l->>'work_id'=b.canonical_work_id::text and l->'basis'->>'component_key'=b.canonical_component_key)
            and cardinality(receipt.head_ids) not between 1 and 2) then raise exception using errcode='23514',message='BPAY_NEXT_ESTIMATE_INPUT_UNBOUND';end if;
        with source_receipts as materialized(select distinct receipt.*,receipt.xmin::text as receipt_version
          from private.bpay_next_source_component_binding b join private.bpay_next_source_component_binding_version bv on bv.id=b.current_binding_version_id
          join private.weekly_source_entitlement_publication_receipts receipt on receipt.id=bv.source_receipt_id
          where b.canonical_work_id=any(v_est_work_ids) and exists(select 1 from jsonb_array_elements(v_est_leaves) l where l->>'work_id'=b.canonical_work_id::text and l->'basis'->>'component_key'=b.canonical_component_key)),
        source_members as materialized(select point.* from source_receipts receipt cross join lateral (
    select h.id,h.xmin::text as head_version,a.id as auth_id,a.xmin::text as auth_version,w.xmin::text as work_version,r.xmin::text as revision_version,p.xmin::text as publication_version,h.prior_head_id,w.id as work_id,r.id as revision_id,
           p.command_id,coalesce(p.predecessor_revision_id,
             case when p.command_id is null and w.current_revision_id is distinct from r.id
               then w.current_revision_id end) as predecessor_revision_id 
      from generate_subscripts(receipt.head_ids,1) member(member_no) join public.weekly_source_entitlement_heads h on h.id=receipt.head_ids[member.member_no]
      join public.weekly_source_root_authorisations a on a.root_timesheet_id=h.root_timesheet_id
        and a.withdrawn_at_utc is null and a.current_entitlement_head_id=h.id
      join private.bpay_next_work w on w.original_timesheet_id=h.root_timesheet_id
        and w.booking_id=h.root_family_booking_id and w.candidate_id=h.candidate_id
        and w.contract_id=h.contract_id and w.week_ending_date=h.week_ending_date and w.work_kind='SOURCE'
      join private.bpay_next_work_revision r on r.work_id=w.id and r.source_head_id=h.id and r.source_event_id=h.id
        and r.physical_timesheet_id=h.root_timesheet_id and r.physical_timesheet_version=h.root_timesheet_version
        and r.source_kind in ('SOURCE','PROTECTED')
      left join private.bpay_next_publication p on p.work_id=w.id and p.revision_id=r.id
        and p.candidate_id=w.candidate_id
      where h.id=any(receipt.head_ids) and h.state='COMMITTED_CURRENT'
        and h.candidate_id=receipt.candidate_id and h.root_timesheet_id=receipt.member_root_ids[member.member_no]
        and h.root_family_booking_id=receipt.member_family_booking_ids[member.member_no]
        and h.root_timesheet_version=receipt.member_root_versions[member.member_no]
        and h.decision_bundle_id=receipt.decision_bundle_id and h.bundle_revision=receipt.bundle_revision
        and h.publication_receipt_digest=receipt.request_digest
        and h.scope_change_tx_token=receipt.scope_change_tx_token limit 3
        ) point),native as (
          select jsonb_build_array('C',c.id,c.xmin::text) value from public.candidates c where c.id=v_est_candidate.id
          union all select jsonb_build_array('U',u.id,u.xmin::text) from public.umbrellas u where u.id=v_est_candidate.umbrella_id
          union all select jsonb_build_array('W',w.id,w.xmin::text) from private.bpay_next_work w where w.id=any(v_est_work_ids)
          union all select jsonb_build_array('R',r.id,r.xmin::text) from private.bpay_next_work_revision r
            join private.bpay_next_work w on w.current_revision_id=r.id where w.id=any(v_est_work_ids)
          union all select jsonb_build_array('P',p.work_id,p.component_key,p.xmin::text) from private.bpay_next_position p
            where p.work_id=any(v_est_work_ids) and exists(select 1 from jsonb_array_elements(v_est_leaves) l
              where l->>'work_id'=p.work_id::text and l->'basis'->>'component_key'=p.component_key)
          union all select jsonb_build_array('I',i.id,i.xmin::text) from private.bpay_next_workbench_item i
            where i.candidate_id=v_est_candidate.id and exists(select 1 from jsonb_array_elements(v_est_leaves) l
              where l->>'leaf_key'=case when i.work_id is not null then 'WORK:'||i.work_id::text||':'||i.component_key||':'||i.member_kind||':'||i.member_key
                else 'CASE:'||i.case_component_id::text end) and i.portion_kind in ('AVAILABLE','ZERO')
          union all select jsonb_build_array('FC',c.id,c.xmin::text) from private.bpay_next_finance_case c
            join private.bpay_next_case_component cc on cc.case_id=c.id where cc.id=any(v_est_components)
          union all select jsonb_build_array('CC',cc.id,cc.xmin::text) from private.bpay_next_case_component cc where cc.id=any(v_est_components)
          union all select jsonb_build_array('CR',r.id,r.xmin::text) from private.bpay_next_case_rule r
            join private.bpay_next_finance_case c on c.current_rule_id=r.id
            join private.bpay_next_case_component cc on cc.case_id=c.id where cc.id=any(v_est_components)
          union all select jsonb_build_array('CP',p.case_component_id,p.pay_week_start,p.xmin::text) from private.bpay_next_case_period p
            where p.case_component_id=any(v_est_components) and p.pay_week_start=v_est_week_start
          union all select jsonb_build_array('CO',o.id,o.xmin::text) from private.bpay_next_work_collection o where o.case_component_id=any(v_est_components)
          union all select jsonb_build_array('OP',p.work_id,p.component_key,p.xmin::text) from private.bpay_next_position p
            join private.bpay_next_work_collection o on o.work_id=p.work_id and o.component_key=p.component_key where o.case_component_id=any(v_est_components)
          union all select jsonb_build_array('OW',w.id,w.xmin::text) from private.bpay_next_work w
            join private.bpay_next_work_collection o on o.work_id=w.id where o.case_component_id=any(v_est_components)
          union all select jsonb_build_array('SN',sn.id,sn.xmin::text) from private.bpay_next_snooze sn
            where sn.candidate_id=v_est_candidate.id and (sn.work_id=any(v_est_work_ids) or sn.case_component_id=any(v_est_components))
              and sn.control_sequence=(select max(s.control_sequence::bigint) from private.bpay_next_snooze s
                where (sn.work_id is not null and s.work_id=sn.work_id or sn.case_component_id is not null and s.case_component_id=sn.case_component_id))
          union all select jsonb_build_array('SB',b.canonical_work_id,b.canonical_component_key,b.xmin::text)
            from private.bpay_next_source_component_binding b where b.canonical_work_id=any(v_est_work_ids) and exists(select 1 from jsonb_array_elements(v_est_leaves) l where l->>'work_id'=b.canonical_work_id::text and l->'basis'->>'component_key'=b.canonical_component_key)
          union all select jsonb_build_array('SV',bv.id,bv.xmin::text) from private.bpay_next_source_component_binding_version bv
            join private.bpay_next_source_component_binding b on b.current_binding_version_id=bv.id where b.canonical_work_id=any(v_est_work_ids) and exists(select 1 from jsonb_array_elements(v_est_leaves) l where l->>'work_id'=b.canonical_work_id::text and l->'basis'->>'component_key'=b.canonical_component_key)
          union all select jsonb_build_array('PL',p.command_id,p.xmin::text) from private.bpay_next_publication p
            join private.bpay_next_source_component_binding_version bv on p.command_id in (bv.publication_command_id_1,bv.publication_command_id_2)
            join private.bpay_next_source_component_binding b on b.current_binding_version_id=bv.id where b.canonical_work_id=any(v_est_work_ids) and exists(select 1 from jsonb_array_elements(v_est_leaves) l where l->>'work_id'=b.canonical_work_id::text and l->'basis'->>'component_key'=b.canonical_component_key)
          union all select jsonb_build_array('BC',c.id,c.xmin::text) from private.bpay_next_command c
            join private.bpay_next_source_component_binding_version bv on c.id in (bv.publication_command_id_1,bv.publication_command_id_2)
            join private.bpay_next_source_component_binding b on b.current_binding_version_id=bv.id where b.canonical_work_id=any(v_est_work_ids) and exists(select 1 from jsonb_array_elements(v_est_leaves) l where l->>'work_id'=b.canonical_work_id::text and l->'basis'->>'component_key'=b.canonical_component_key)
          union all select jsonb_build_array('BJ',j.id,j.xmin::text) from private.bpay_next_job j
            join private.bpay_next_source_component_binding_version bv on j.command_id in (bv.publication_command_id_1,bv.publication_command_id_2)
            join private.bpay_next_source_component_binding b on b.current_binding_version_id=bv.id where b.canonical_work_id=any(v_est_work_ids) and exists(select 1 from jsonb_array_elements(v_est_leaves) l where l->>'work_id'=b.canonical_work_id::text and l->'basis'->>'component_key'=b.canonical_component_key)
          union all select jsonb_build_array('BM',m.command_id,m.candidate_id,m.xmin::text) from private.bpay_next_command_member m
            join private.bpay_next_source_component_binding_version bv on m.command_id in (bv.publication_command_id_1,bv.publication_command_id_2)
            join private.bpay_next_source_component_binding b on b.current_binding_version_id=bv.id where b.canonical_work_id=any(v_est_work_ids) and exists(select 1 from jsonb_array_elements(v_est_leaves) l where l->>'work_id'=b.canonical_work_id::text and l->'basis'->>'component_key'=b.canonical_component_key)
          union all select jsonb_build_array('EB',d.decision_bundle_id,d.xmin::text) from public.weekly_source_entitlement_decision_bundles d
            join private.bpay_next_source_component_binding_version bv on bv.decision_bundle_id=d.decision_bundle_id
            join private.bpay_next_source_component_binding b on b.current_binding_version_id=bv.id where b.canonical_work_id=any(v_est_work_ids) and exists(select 1 from jsonb_array_elements(v_est_leaves) l where l->>'work_id'=b.canonical_work_id::text and l->'basis'->>'component_key'=b.canonical_component_key)
          union all select jsonb_build_array('RA',a.id,a.xmin::text) from public.weekly_source_root_authorisations a
            join private.bpay_next_source_component_binding_version bv on bv.initial_root_authorisation_id=a.id
            join private.bpay_next_source_component_binding b on b.current_binding_version_id=bv.id where b.canonical_work_id=any(v_est_work_ids) and exists(select 1 from jsonb_array_elements(v_est_leaves) l where l->>'work_id'=b.canonical_work_id::text and l->'basis'->>'component_key'=b.canonical_component_key)
          union all select jsonb_build_array('TS',t.timesheet_id,t.xmin::text) from public.timesheets t
            join private.bpay_next_source_component_binding_version bv on bv.initial_root_timesheet_id=t.timesheet_id
            join private.bpay_next_source_component_binding b on b.current_binding_version_id=bv.id where b.canonical_work_id=any(v_est_work_ids) and exists(select 1 from jsonb_array_elements(v_est_leaves) l where l->>'work_id'=b.canonical_work_id::text and l->'basis'->>'component_key'=b.canonical_component_key)
          union all select jsonb_build_array('TF',f.id,f.xmin::text) from public.timesheets_financials f
            join private.bpay_next_source_component_binding_version bv on bv.initial_financial_snapshot_id=f.id
            join private.bpay_next_source_component_binding b on b.current_binding_version_id=bv.id where b.canonical_work_id=any(v_est_work_ids) and exists(select 1 from jsonb_array_elements(v_est_leaves) l where l->>'work_id'=b.canonical_work_id::text and l->'basis'->>'component_key'=b.canonical_component_key)
          union all select jsonb_build_array('CT',c.id,c.xmin::text) from public.contracts c
            join private.bpay_next_work w on w.contract_id=c.id where w.id=any(v_est_work_ids)
          union all select jsonb_build_array('SR',receipt.id,receipt.receipt_version) from source_receipts receipt
          union all select jsonb_build_array('SH',m.id,m.head_version) from source_members m
          union all select jsonb_build_array('SA',m.auth_id,m.auth_version) from source_members m
          union all select jsonb_build_array('SW',m.work_id,m.work_version) from source_members m
          union all select jsonb_build_array('SVR',m.revision_id,m.revision_version) from source_members m
          union all select jsonb_build_array('SP',m.command_id,m.publication_version) from source_members m
          union all select jsonb_build_array('OR',r.id,r.xmin::text) from private.bpay_next_work_revision r
            join private.bpay_next_work_collection o on r.id=o.original_revision_id where o.case_component_id=any(v_est_components)
          union all select jsonb_build_array('AR',r.id,r.xmin::text) from private.bpay_next_work_revision r
            join private.bpay_next_position p on p.applied_revision_id=r.id join private.bpay_next_work_collection o on o.work_id=p.work_id and o.component_key=p.component_key where o.case_component_id=any(v_est_components)
          union all select jsonb_build_array('OL',l.id,l.xmin::text) from private.bpay_next_approved_line l
            join private.bpay_next_work_collection o on o.original_approved_line_id=l.id where o.case_component_id=any(v_est_components)
          union all select jsonb_build_array('AL',l.id,l.xmin::text) from private.bpay_next_approved_line l
            join private.bpay_next_position p on p.applied_revision_id=l.revision_id and p.component_key=l.component_key join private.bpay_next_work_collection o on o.work_id=p.work_id and o.component_key=p.component_key where o.case_component_id=any(v_est_components)
          union all select jsonb_build_array('CQ',o.command_id,o.xmin::text) from private.bpay_next_case_create_request o
            join private.bpay_next_case_component cc on cc.case_id=o.case_id where cc.id=any(v_est_components)
          union all select jsonb_build_array('PW',pw.policy_id,pw.source_window_id,pw.xmin::text) from private.bpay_next_valuation_policy_window pw
            where (v_date_context->>'current_official_pay_date')::date>=pw.date_from
              and (v_date_context->>'current_official_pay_date')::date<=coalesce(pw.date_to,'infinity'::date)
              and pw.date_from=(select max(winner.date_from::date) from private.bpay_next_valuation_policy_window winner where winner.policy_id=pw.policy_id and (v_date_context->>'current_official_pay_date')::date>=winner.date_from and (v_date_context->>'current_official_pay_date')::date<=coalesce(winner.date_to,'infinity'::date))
              and (exists(select 1 from private.bpay_next_work_revision r join private.bpay_next_work w on w.current_revision_id=r.id
                where w.id=any(v_est_work_ids) and r.valuation_policy_id=pw.policy_id)
                or exists(select 1 from private.bpay_next_case_create_request o join private.bpay_next_case_component cc on cc.case_id=o.case_id
                  where cc.id=any(v_est_components) and o.valuation_policy_id=pw.policy_id)
                or exists(select 1 from private.bpay_next_work_collection o where o.case_component_id=any(v_est_components)
                  and o.original_valuation_policy_id=pw.policy_id))
          union all select jsonb_build_array('WK',w.candidate_id,w.pay_week_start,w.xmin::text) from private.bpay_next_worker_week w
            where w.candidate_id=v_est_candidate.id and w.pay_week_start=v_est_week_start
          union all select jsonb_build_array('WC',x.id,x.xmin::text) from private.bpay_next_worker_week_contribution x
            where x.candidate_id=v_est_candidate.id and x.pay_week_start=v_est_week_start
              and x.original_pay_date<=(v_date_context->>'current_official_pay_date')::date
        ) select coalesce(jsonb_agg(value order by value::text),'[]'::jsonb) into v_versions from (select distinct value from native) exact_versions;
        if v_est->'versions'<>'null'::jsonb and v_est->'versions' is distinct from v_versions then
          v_est_state:='OUTDATED';
        else
          for v_est_record in select (l->>'work_id')::uuid work_id,(l->>'revision_id')::uuid revision_id,
              l->'basis'->>'component_key' component_key,(l->>'approved_line_id')::uuid approved_line_id,
              sum((l->'basis'->>'included_source_ex_vat')::numeric) included_source,
              count(*) segment_count,bool_and(coalesce((l->'basis'->>'pay_hold')::boolean,false)=false) not_held
            from jsonb_array_elements(v_est_leaves) l where l->>'work_id' is not null
            group by l->>'work_id',l->>'revision_id',l->'basis'->>'component_key',l->>'approved_line_id' loop
            select p.* into strict v_est_position from private.bpay_next_position p
              join private.bpay_next_work w on w.id=p.work_id and w.current_revision_id=v_est_record.revision_id
                and w.applied_revision_id=v_est_record.revision_id and w.approval_state='APPROVED'
              where p.work_id=v_est_record.work_id and p.component_key=v_est_record.component_key
                and p.applied_revision_id=v_est_record.revision_id;
            if private.bpay_next_work_wait_state_v1(v_est_record.work_id) or not v_est_record.not_held
              or v_est_record.included_source is null or v_est_record.included_source<0
              or exists(select 1 from private.bpay_next_workbench_item i
                join private.bpay_next_workbench_group g on g.group_key=i.group_key and g.candidate_id=i.candidate_id
                where i.work_id=v_est_record.work_id and i.component_key=v_est_record.component_key
                  and i.portion_kind in ('AVAILABLE','ZERO') and (i.financial_view_revision<>v_est_financial
                    or g.financial_view_revision<>v_est_financial or not g.complete or g.dirty or i.progress_state<>'CURRENT'))
              or exists(select 1 from jsonb_array_elements(v_descriptor->'intent'->'works') w
                where w->>'work_id'=v_est_record.work_id::text and w->>'expected_revision_id'<>v_est_record.revision_id::text)
              or exists(select 1 from jsonb_array_elements(v_descriptor->'intent'->'components') c
                where c->>'work_id'=v_est_record.work_id::text and c->>'expected_revision_id'<>v_est_record.revision_id::text) then
              raise exception using errcode='23514',message='BPAY_NEXT_ESTIMATE_INPUT_UNBOUND';end if;
            if exists(select 1 from private.bpay_next_work w where w.id=v_est_record.work_id and w.work_kind='SOURCE') then
              select bv.* into strict v_est_binding from private.bpay_next_source_component_binding b
                join private.bpay_next_source_component_binding_version bv on bv.id=b.current_binding_version_id
                where b.canonical_work_id=v_est_record.work_id and b.canonical_component_key=v_est_record.component_key
                  and bv.approval_work_id=v_est_record.work_id and bv.approval_revision_id=v_est_record.revision_id
                  and bv.approved_line_id=v_est_record.approved_line_id and bv.component_state='PRESENT'
                  and bv.candidate_id=v_est_candidate.id and bv.approval_component_key=v_est_record.component_key;
              -- Exact existing bound snapshot admission, without locks or a new certificate owner.
              if v_est_binding.origin_kind='INITIAL_AUTHORISED_TSFIN_V1' then
  select x.* into strict v_initial_a from public.weekly_source_root_authorisations x where x.id=v_est_binding.initial_root_authorisation_id;
  select x.* into strict v_initial_t from public.timesheets x where x.timesheet_id=v_initial_a.root_timesheet_id;
  select x.* into strict v_initial_c from public.contracts x where x.id=v_initial_t.contract_id;
  select x.* into strict v_initial_r from private.bpay_next_work_revision x where x.source_event_id=v_initial_a.id;
  select x.* into strict v_initial_w from private.bpay_next_work x where x.id=v_initial_r.work_id;
  select x.* into strict v_initial_f from public.timesheets_financials x where x.id=v_initial_r.financial_snapshot_id;
  select x.* into strict v_initial_p from private.bpay_next_publication x
    where x.command_id=private.bpay_next_source_command_id_v1(v_initial_a.id);
  if v_initial_a.withdrawn_at_utc is not null or v_initial_a.current_entitlement_head_id is not null
     or exists(select 1 from public.weekly_source_entitlement_heads h where btrim(h.root_family_booking_id)=btrim(v_initial_t.booking_id))
     or v_initial_t.is_current is distinct from true or v_initial_t.authorised_at_server is null
     or v_initial_t.revoked_at is not null or v_initial_t.archived_at_utc is not null
     or (v_initial_a.family_booking_id,v_initial_a.timesheet_version) is distinct from (v_initial_t.booking_id,v_initial_t.version)
     or (v_initial_w.work_kind,v_initial_w.candidate_id,v_initial_w.contract_id,v_initial_w.original_timesheet_id,v_initial_w.booking_id,v_initial_w.week_ending_date,v_initial_w.current_revision_id)
       is distinct from ('SOURCE'::text,v_initial_c.candidate_id,v_initial_c.id,v_initial_t.timesheet_id,v_initial_t.booking_id,v_initial_t.week_ending_date,v_initial_r.id)
     or v_initial_w.approval_state is distinct from 'APPROVED'
     or (v_initial_r.source_kind,v_initial_r.source_head_id,v_initial_r.source_event_id,v_initial_r.physical_timesheet_id,v_initial_r.physical_timesheet_version,v_initial_r.week_ending_date)
       is distinct from ('SOURCE'::text,null::uuid,v_initial_a.id,v_initial_t.timesheet_id,v_initial_t.version,v_initial_t.week_ending_date)
     or v_initial_r.approved_at_utc is null or v_initial_r.sealed_at_utc is null
     or v_initial_f.is_current is distinct from true or v_initial_f.is_stale is distinct from false or v_initial_f.authorised_at_utc is null
     or (v_initial_f.timesheet_id,v_initial_f.timesheet_version,v_initial_f.candidate_id,v_initial_f.client_id)
       is distinct from (v_initial_t.timesheet_id,v_initial_t.version,v_initial_w.candidate_id,v_initial_c.client_id)
     or (v_initial_p.work_id,v_initial_p.revision_id,v_initial_p.candidate_id,v_initial_p.revision_no)
       is distinct from (v_initial_w.id,v_initial_r.id,v_initial_w.candidate_id,v_initial_r.revision_no)
     or not exists(select 1 from private.bpay_next_command command
       join private.bpay_next_command_member member on member.command_id=command.id and member.candidate_id=v_initial_w.candidate_id
       where command.id=v_initial_p.command_id and command.command_kind='POSITION_APPLY'
         and command.module_epoch=v_epoch and command.status in ('SEALED','COMPLETE')
         and command.sealed_at_utc is not null and command.expected_member_count=1 and member.member_no=1
         -- INITIAL_METADATA_PHASE_V2_BEGIN: no fabricated enrollment/job.
         and ((command.status='SEALED' and command.enrolled_member_count=0
           and v_initial_p.status='QUEUED' and v_initial_p.phase='NEW' and v_initial_p.applied_at_utc is null
           and not exists(select 1 from private.bpay_next_job job where job.command_id=command.id))
          or (command.status='COMPLETE' and command.enrolled_member_count=1
           and v_initial_p.status='APPLIED' and v_initial_p.phase='DONE' and v_initial_p.applied_at_utc is not null
           and (select count(*) from private.bpay_next_job job where job.command_id=command.id)=1
           and exists(select 1 from private.bpay_next_job job
             join private.bpay_next_worker_control ctl on ctl.candidate_id=job.candidate_id
             where job.command_id=command.id and job.candidate_id=v_initial_w.candidate_id
               and job.job_kind='POSITION_APPLY' and job.command_sequence=command.agency_sequence
               and job.module_epoch=command.module_epoch and job.status='DONE' and job.phase='DONE'
               and job.owner_epoch>0 and job.owner_epoch<=ctl.active_owner_epoch
               and job.lease_nonce is null and job.lease_until_utc is null)))
         -- INITIAL_METADATA_PHASE_V2_END
       )
  then raise exception using errcode='23514',message='BPAY_NEXT_ESTIMATE_INPUT_UNBOUND';end if;

  select x.* into strict v_initial_line from private.bpay_next_approved_line x where x.id=v_est_binding.approved_line_id;
  v_initial_details:=jsonb_build_object('segments',v_initial_f.invoice_breakdown_json->'segments','additional_units',v_initial_f.additional_units_json,
    'actual_schedule',v_initial_f.actual_schedule_json,'rate_source_refs',v_initial_f.rate_source_refs_json);
  if (v_est_binding.candidate_id,v_est_binding.canonical_work_id,v_est_binding.approval_work_id,v_est_binding.approval_revision_id,
      v_est_binding.initial_root_authorisation_id,v_est_binding.initial_financial_snapshot_id,v_est_binding.initial_root_timesheet_id,
      v_est_binding.initial_root_version,v_est_binding.initial_authorisation_generation,v_est_binding.initial_authorised_row_signature)
    is distinct from (v_initial_w.candidate_id,v_initial_w.id,v_initial_w.id,v_initial_r.id,v_initial_a.id,v_initial_f.id,
      v_initial_t.timesheet_id,v_initial_t.version,v_initial_a.authorisation_generation,v_initial_a.authorised_row_signature)
    or v_initial_r.source_inventory_digest is distinct from v_est_binding.initial_inventory_digest
    or (v_est_binding.approved_line_id,v_est_binding.classification_line_id,v_est_binding.canonical_component_key,v_est_binding.approval_component_key,
        v_est_binding.approval_source_channel,v_est_binding.tax_treatment,v_est_binding.publication_command_id_1,v_est_binding.publication_work_id_1,v_est_binding.publication_revision_id_1)
      is distinct from (v_initial_line.id,v_initial_line.id,v_initial_line.component_key,v_initial_line.component_key,v_initial_r.source_pay_channel,
        v_initial_line.tax_treatment,v_initial_p.command_id,v_initial_w.id,v_initial_r.id)
    or (v_initial_line.revision_id,v_initial_line.source_component_id,v_initial_line.component_key,v_initial_line.evidence_ref)
      is distinct from (v_initial_r.id,v_est_binding.source_component_id,'SOURCE:'||v_est_binding.source_component_id::text,
        'source-component:'||v_est_binding.source_component_id::text||'#'||encode(v_est_binding.component_sha256,'hex'))
    or v_est_binding.initial_detail_digest is distinct from private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_APPROVED_DETAIL_V2',v_initial_details)
    or v_est_binding.initial_policy_digest is distinct from private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_APPROVED_POLICY_V2',v_initial_f.policy_snapshot_json)
    or v_est_binding.binding_digest is distinct from sha256(convert_to(jsonb_build_array('INITIAL_AUTHORISED_TSFIN_V1',v_est_binding.canonical_work_id,
      v_est_binding.canonical_component_key,v_est_binding.approval_revision_id,v_est_binding.approved_line_id,v_est_binding.initial_root_authorisation_id,
      encode(v_est_binding.initial_certificate_digest,'hex'),v_est_binding.publication_command_id_1::text,encode(v_est_binding.component_sha256,'hex'))::text,'UTF8'))
  then raise exception using errcode='23514',message='BPAY_NEXT_ESTIMATE_INPUT_UNBOUND';end if;

              else
                select x.* into strict v_source_receipt from private.weekly_source_entitlement_publication_receipts x where x.id=v_est_binding.source_receipt_id;
                if cardinality(v_source_receipt.head_ids) not between 1 and 2 or not exists(select 1 from public.weekly_source_entitlement_decision_bundles b
                  where b.decision_bundle_id=v_source_receipt.decision_bundle_id and b.bundle_revision=v_source_receipt.bundle_revision
                    and b.candidate_id=v_source_receipt.candidate_id and b.state='COMMITTED' and b.request_digest=v_source_receipt.request_digest
                    and b.decision_id=v_source_receipt.decision_id and b.decided_by_user_id=v_source_receipt.decided_by_user_id
                    and b.decision_bundle_id=v_est_binding.decision_bundle_id and b.bundle_revision=v_est_binding.bundle_revision) then
                  raise exception using errcode='23514',message='BPAY_NEXT_ESTIMATE_INPUT_UNBOUND';end if;
                v_source_count:=0;
                for v_source_no in 1..cardinality(v_source_receipt.head_ids) loop
    select h.id,h.prior_head_id,w.id as work_id,r.id as revision_id,
           p.command_id,coalesce(p.predecessor_revision_id,
             case when p.command_id is null and w.current_revision_id is distinct from r.id
               then w.current_revision_id end) as predecessor_revision_id into strict v_source_member
      from public.weekly_source_entitlement_heads h
      join public.weekly_source_root_authorisations a on a.root_timesheet_id=h.root_timesheet_id
        and a.withdrawn_at_utc is null and a.current_entitlement_head_id=h.id
      join private.bpay_next_work w on w.original_timesheet_id=h.root_timesheet_id
        and w.booking_id=h.root_family_booking_id and w.candidate_id=h.candidate_id
        and w.contract_id=h.contract_id and w.week_ending_date=h.week_ending_date and w.work_kind='SOURCE'
      join private.bpay_next_work_revision r on r.work_id=w.id and r.source_head_id=h.id and r.source_event_id=h.id
        and r.physical_timesheet_id=h.root_timesheet_id and r.physical_timesheet_version=h.root_timesheet_version
        and r.source_kind in ('SOURCE','PROTECTED')
      left join private.bpay_next_publication p on p.work_id=w.id and p.revision_id=r.id
        and p.candidate_id=w.candidate_id
      where h.id=v_source_receipt.head_ids[v_source_no] and h.state='COMMITTED_CURRENT'
        and h.candidate_id=v_source_receipt.candidate_id and h.root_timesheet_id=v_source_receipt.member_root_ids[v_source_no]
        and h.root_family_booking_id=v_source_receipt.member_family_booking_ids[v_source_no]
        and h.root_timesheet_version=v_source_receipt.member_root_versions[v_source_no]
        and h.decision_bundle_id=v_source_receipt.decision_bundle_id and h.bundle_revision=v_source_receipt.bundle_revision
        and h.publication_receipt_digest=v_source_receipt.request_digest
        and h.scope_change_tx_token=v_source_receipt.scope_change_tx_token;

                  if (v_source_member.command_id,v_source_member.work_id,v_source_member.revision_id) is not distinct from
                    (v_est_binding.publication_command_id_1,v_est_binding.publication_work_id_1,v_est_binding.publication_revision_id_1)
                    or (v_est_binding.publication_command_id_2 is not null and (v_source_member.command_id,v_source_member.work_id,v_source_member.revision_id) is not distinct from
                      (v_est_binding.publication_command_id_2,v_est_binding.publication_work_id_2,v_est_binding.publication_revision_id_2)) then v_source_count:=v_source_count+1;end if;
                end loop;
                if v_source_count<>(case when v_est_binding.publication_command_id_2 is null then 1 else 2 end) then raise exception using errcode='23514',message='BPAY_NEXT_ESTIMATE_INPUT_UNBOUND';end if;
              end if;
              if (select count(*) from private.bpay_next_publication p
                join private.bpay_next_work w on w.id=p.work_id
                where p.status='APPLIED' and p.phase='DONE' and p.applied_at_utc is not null
                  and w.current_revision_id=p.revision_id and w.applied_revision_id=p.revision_id
                  and ((p.command_id,p.work_id,p.revision_id) is not distinct from
                    (v_est_binding.publication_command_id_1,v_est_binding.publication_work_id_1,v_est_binding.publication_revision_id_1)
                    or (v_est_binding.publication_command_id_2 is not null and (p.command_id,p.work_id,p.revision_id) is not distinct from
                      (v_est_binding.publication_command_id_2,v_est_binding.publication_work_id_2,v_est_binding.publication_revision_id_2))))
                is distinct from (case when v_est_binding.publication_command_id_2 is null then 1 else 2 end)::bigint then
                raise exception using errcode='23514',message='BPAY_NEXT_ESTIMATE_INPUT_UNBOUND';end if;
              if v_est_position.source_book_state is distinct from 'READY' or coalesce(v_est_position.source_book_revision,0)<1
                or v_est_candidate.pay_method::text is distinct from v_est_position.source_basis_channel
                or v_est_position.source_basis_channel is distinct from v_est_position.source_book_seed_channel
                or v_est_binding.approval_source_channel is distinct from v_est_position.source_basis_channel
                or v_est_position.approved_source_ex_vat is distinct from (select l.source_pay_ex_vat from private.bpay_next_approved_line l where l.id=v_est_record.approved_line_id)
                or (v_est_position.native_realised_paye,v_est_position.native_held_paye,v_est_position.native_realised_umbrella,v_est_position.native_held_umbrella) is distinct from
                  (case when v_est_position.source_basis_channel='PAYE' then v_est_position.realised_source_ex_vat else 0::numeric end,
                   case when v_est_position.source_basis_channel='PAYE' then v_est_position.held_source_ex_vat else 0::numeric end,
                   case when v_est_position.source_basis_channel='UMBRELLA' then v_est_position.realised_source_ex_vat else 0::numeric end,
                   case when v_est_position.source_basis_channel='UMBRELLA' then v_est_position.held_source_ex_vat else 0::numeric end)
                or v_est_binding.tax_treatment is distinct from 'TAXABLE' or v_est_binding.classification_line_id is null
                or (v_est_position.source_basis_channel='PAYE' and (v_est_position.native_realised_paye,v_est_position.native_held_paye)
                  is distinct from (v_est_position.realised_source_ex_vat,v_est_position.held_source_ex_vat))
                or (v_est_position.source_basis_channel='UMBRELLA' and (v_est_position.native_realised_umbrella,v_est_position.native_held_umbrella)
                  is distinct from (v_est_position.realised_source_ex_vat,v_est_position.held_source_ex_vat)) then
                raise exception using errcode='23514',message='BPAY_NEXT_ESTIMATE_INPUT_UNBOUND';end if;
            end if;
            select x.* into strict v_est_value from private.bpay_next_work_revision r
              cross join lateral private.bpay_next_value_source_for_target_v1(v_est_record.included_source,r.source_pay_channel,
                v_est_candidate.pay_method::text,case when v_est_candidate.pay_method='UMBRELLA' then
                  (select u.vat_chargeable from public.umbrellas u where u.id=v_est_candidate.umbrella_id) else false end,
                r.valuation_policy_id,(v_date_context->>'current_official_pay_date')::date) x where r.id=v_est_record.revision_id;
            v_est_source:=v_est_source+v_est_record.included_source;v_est_gross:=v_est_gross+v_est_value.target_ex_vat;
          end loop;
          select cardinality(v_est_components)>0 and coalesce(bool_and((cc.case_kind,cc.instruction_kind,cc.payroll_stage)
            is not distinct from ('OVERPAYMENT','RECOVERY','GROSS_DEDUCT')),false) into v_est_all_overpayment
            from private.bpay_next_case_component cc where cc.id=any(v_est_components);
          v_est_h:=v_est_gross;
          select w.* into v_est_week from private.bpay_next_worker_week w where w.candidate_id=v_est_candidate.id and w.pay_week_start=v_est_week_start;
          if cardinality(v_est_components)>0 and not v_est_all_overpayment then
            if coalesce(v_est_week.unresolved_contribution_count,0)<>0 or exists(select 1 from private.bpay_next_run_worker w
              join private.bpay_next_pay_run r on r.id=w.run_id where w.candidate_id=v_est_candidate.id
                and w.status in ('READY','DRAFT','ISSUED','COMPLETE') and r.pay_date>=v_est_week_start
                and r.pay_date<=(v_date_context->>'current_official_pay_date')::date
                and not exists(select 1 from private.bpay_next_worker_week_contribution x where x.original_run_worker_id=w.id)) then
              raise exception using errcode='23514',message='BPAY_NEXT_ESTIMATE_WEEK_UNBOUND';end if;
            v_est_seen:=0;
            for v_est_record in select x.* from private.bpay_next_worker_week_contribution x
              where x.candidate_id=v_est_candidate.id and x.pay_week_start=v_est_week_start
                and x.original_pay_date<=(v_date_context->>'current_official_pay_date')::date
              order by x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id limit 1001 loop
              v_est_seen:=v_est_seen+1;
              if v_est_seen>1000 then raise exception using errcode='54000',message='BPAY_NEXT_ESTIMATE_BUDGET';end if;
              if v_est_record.eligibility_state='UNBOUND' then raise exception using errcode='23514',message='BPAY_NEXT_ESTIMATE_WEEK_UNBOUND';end if;
              v_est_h:=v_est_h+v_est_record.eligible_arranged_amount;
            end loop;
          end if;
          for v_est_record in select cc.* from private.bpay_next_case_component cc where cc.id=any(v_est_components)
            order by cc.case_id,cc.component_ordinal,cc.id loop
            v_est_plan:=private.bpay_next_case_allocation_plan_v1(jsonb_build_object('mode','DERIVE',
              'candidate_id',v_est_candidate.id,'case_id',v_est_record.case_id,'case_component_id',v_est_record.id,
              'pay_date',v_date_context->>'current_official_pay_date','pay_week_start',v_est_week_start,
              'target_pay_channel',v_est_candidate.pay_method::text,'captured_default_floor',v_est_candidate.min_take_home_wtd,
              'captured_default_floor_revision',coalesce(v_est_week.default_floor_revision,1),
              'weekly_binding_state',case when v_est_all_overpayment then 'NOT_AFFECTED' else 'BOUND' end,'accepted_run_id',null));
            if v_est_record.id=any(v_est_wait_components) and (
                not exists(select 1 from private.bpay_next_work_collection origin
                  join private.bpay_next_work work on work.id=origin.work_id and work.candidate_id=origin.candidate_id
                  where origin.case_id=v_est_record.case_id and origin.case_component_id=v_est_record.id
                    and origin.candidate_id=v_est_candidate.id and origin.paid_basis_qualified
                    and origin.qualification_state='READY' and private.bpay_next_work_wait_state_v1(work.id))
                or (v_est_plan->'instruction'->>'case_kind',v_est_plan->'instruction'->>'instruction_kind',
                    v_est_plan->'instruction'->>'direction',v_est_plan->'instruction'->>'payroll_stage',
                    v_est_plan->'instruction'->>'hold_purpose')
                  is distinct from ('OVERPAYMENT','RECOVERY','DEDUCTION','GROSS_DEDUCT','GROSS_RECOVERY')
                or v_est_plan->'instruction'->>'explanation' is distinct from 'ORIGIN_UNAUTHORISED_WAIT'
                or v_est_plan->'instruction'->>'work_collection_id' is null
                or (v_est_plan->'instruction'->>'nominal_source_ex_vat')::numeric is distinct from 0::numeric
                or (v_est_plan->'instruction'->>'capacity_source_ex_vat')::numeric is distinct from 0::numeric
                or (v_est_plan->'instruction'->>'nominal_target_ex_vat')::numeric is distinct from 0::numeric
                or (v_est_plan->'instruction'->>'capacity_target_ex_vat')::numeric is distinct from 0::numeric) then
              -- Maintained WAIT may survive an approved-but-unapplied pointer.
              -- It must never admit a positive informational recovery.
              raise exception using errcode='23514',message='BPAY_NEXT_ESTIMATE_INPUT_UNBOUND';end if;
            v_est_instructions:=v_est_instructions||jsonb_build_array(v_est_plan->'instruction');
          end loop;
          v_est_plan:=private.bpay_next_case_allocation_plan_v1(jsonb_build_object('mode','ALLOCATE',
            'instructions',v_est_instructions,'channel_headroom',v_est_gross,'worker_take_home',v_est_h));
          v_est_scope:=jsonb_build_object('descriptor',v_descriptor,'expected_financial_view_revision',v_est_financial::text,'versions',v_versions);
          v_context:=v_context||jsonb_build_object('estimate_scope',v_est_scope);
          v_after:=null;v_est_last:=null;v_est_seen:=0;
          if p_request->'after'<>'null'::jsonb then
            if jsonb_typeof(p_request->'after')<>'object' or not(p_request->'after' ?& array['context','key'])
              or (p_request->'after')-array['context','key']<>'{}'::jsonb or p_request->'after'->'context' is distinct from v_context
              or (p_request->'after'->'key'<>'null'::jsonb and (jsonb_typeof(p_request->'after'->'key')<>'array' or jsonb_array_length(p_request->'after'->'key')<>4))
              or (p_request->'after'->'key'='null'::jsonb and (jsonb_array_length(v_est_plan->'rows')<>0 or v_est->'verify_only'<>'true'::jsonb)) then
              raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_CURSOR_STALE';end if;
            v_after:=nullif(p_request->'after'->'key','null'::jsonb);
            if v_after is not null and not exists(select 1 from jsonb_array_elements(v_est_plan->'rows') r where
              jsonb_build_array(r->'instruction'->>'age_key',r->'instruction'->>'case_id',r->'instruction'->>'component_ordinal',r->'instruction'->>'case_component_id')=v_after) then
              raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_CURSOR_STALE';end if;
          end if;
          for v_est_row in select value from jsonb_array_elements(v_est_plan->'rows') loop
            if v_est_row->'instruction'->>'payroll_stage'='GROSS_ADD' then v_est_gross_add:=v_est_gross_add+(v_est_row->>'taken_amount')::numeric;
            elsif v_est_row->'instruction'->>'payroll_stage'='GROSS_DEDUCT' then v_est_gross_deduct:=v_est_gross_deduct+(v_est_row->>'taken_amount')::numeric;
            elsif v_est_row->'instruction'->>'payroll_stage'='NET_ADD' then v_est_net_add:=v_est_net_add+(v_est_row->>'taken_amount')::numeric;
            elsif v_est_row->'instruction'->>'payroll_stage'='NET_DEDUCT' then v_est_net_deduct:=v_est_net_deduct+(v_est_row->>'taken_amount')::numeric;end if;
            v_est_key:=jsonb_build_array(v_est_row->'instruction'->>'age_key',v_est_row->'instruction'->>'case_id',
              v_est_row->'instruction'->>'component_ordinal',v_est_row->'instruction'->>'case_component_id');
            if v_after is not null and ((v_est_key->>0)::bigint,(v_est_key->>1)::uuid,(v_est_key->>2)::bigint,(v_est_key->>3)::uuid)
              <=((v_after->>0)::bigint,(v_after->>1)::uuid,(v_after->>2)::bigint,(v_after->>3)::uuid) then continue;end if;
            v_est_seen:=v_est_seen+1;
            if v_est_seen<=v_limit and v_est->'verify_only'='false'::jsonb then
              v_est_last:=v_est_key;
              v_est_rows:=v_est_rows||jsonb_build_array(jsonb_build_object('case_id',v_est_row->'instruction'->'case_id',
                'case_component_id',v_est_row->'instruction'->'case_component_id','age_key',v_est_key->>0,
                'component_ordinal',v_est_key->>2,'recoverable_this_run',v_est_row->>'taken_amount',
                'cap_reason',v_est_row->'cap_reason','affordability_reason',v_est_row->'affordability_reason',
                'capacity_reason',v_est_row->'capacity_reason','payroll_stage',v_est_row->'instruction'->'payroll_stage'));
            end if;
          end loop;
          if v_est->'verify_only'='true'::jsonb and v_est_seen<>0 then
            raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_CURSOR_STALE';end if;
          v_est_bound:=jsonb_build_object('context',v_context,'key',coalesce(v_est_last,v_after));
          -- Conservative URL encoding bound checked again exactly by Worker/Office.
          if octet_length((p_request||jsonb_build_object('after',v_est_bound,'estimate',v_est||jsonb_build_object('versions',v_versions)))::text)*3>32768 then
            raise exception using errcode='54000',message='BPAY_NEXT_ESTIMATE_SCOPE_TOO_LARGE';end if;
          v_response:=jsonb_build_object('ok',true,'kind',v_kind,'state','ESTIMATE','context',v_context,
            'workbench_revision',v_revision.workbench_revision::text,'complete',v_est_seen<=v_limit,'rows',v_est_rows,
            'next_cursor',case when v_est_seen>v_limit then v_est_bound else null end,
            'verification_cursor',jsonb_build_object('context',v_context,'key',coalesce(v_est_last,v_after)),
            'estimated_gross_source_ex_vat',v_est_source::text,'estimated_gross_target_ex_vat',(v_est_gross+v_est_gross_add-v_est_gross_deduct)::text,
            'estimated_gross_deductions',v_est_gross_deduct::text,'estimated_net_additions',v_est_net_add::text,'estimated_net_deductions',v_est_net_deduct::text,
            'financial_view_revision_before',v_est_financial::text,'financial_view_revision_after',v_est_financial::text,
            'affected_candidate_count','1','page_receipt',jsonb_build_object('context',v_context,'start_key',v_after,'end_key',v_est_last,'row_count',jsonb_array_length(v_est_rows)::text));
          if octet_length(v_response::text)>120000 then raise exception using errcode='54000',message='BPAY_NEXT_ESTIMATE_SCOPE_TOO_LARGE';end if;
          return v_response;
        end if;
      exception when check_violation or no_data_found or too_many_rows or program_limit_exceeded then
        v_est_state:='PENDING';
      when object_not_in_prerequisite_state then
        if sqlerrm='BPAY_NEXT_COMPARISON_NOT_INSTALLED' then v_est_state:='PENDING';else raise;end if;
      end;
    end if;
    v_context:=v_context-'estimate_scope';
    return jsonb_build_object('ok',true,'kind',v_kind,'state',v_est_state,'context',v_context,
      'workbench_revision',v_revision.workbench_revision::text,'complete',true,'rows','[]'::jsonb,
      'next_cursor',null,'verification_cursor',null,'estimated_gross_source_ex_vat',null,'estimated_gross_target_ex_vat',null,
      'estimated_gross_deductions',null,'estimated_net_additions',null,'estimated_net_deductions',null,
      'financial_view_revision_before',v_est_financial::text,'financial_view_revision_after',v_est_financial::text,
      'affected_candidate_count','1','page_receipt',jsonb_build_object('context',v_context,'start_key',null,'end_key',null,'row_count','0'));
  elsif p_request->'estimate'<>'null'::jsonb then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';
  end if;

  v_read_scope:=v_scope||jsonb_build_object('_selection_memory',v_memory);
  v_cursor:=p_request->'after';
  if v_cursor<>'null'::jsonb then
    if jsonb_typeof(v_cursor)<>'object' or not(v_cursor ?& array['context','key'])
      or v_cursor-array['context','key']<>'{}'::jsonb or v_cursor->'context' is distinct from v_context then
      raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_CURSOR_STALE';end if;
    v_after:=v_cursor->'key';
    if v_kind='EXPORT_ROWS' then
      if jsonb_typeof(v_after)<>'array' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      if v_sort in ('C_NAME_ASC','C_NAME_DESC') then
        if jsonb_array_length(v_after)<>5 or jsonb_typeof(v_after->0)<>'string' or octet_length(v_after->>0)>8192 or jsonb_typeof(v_after->1)<>'boolean' or octet_length(v_after->>1)>8192 or jsonb_typeof(v_after->2)<>'string' or octet_length(v_after->>2)>8192 or jsonb_typeof(v_after->3)<>'string' or v_after->>3!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->4)<>'string' or v_after->>4!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      elsif v_sort in ('C_AMOUNT_ASC','C_AMOUNT_DESC') then
        if jsonb_array_length(v_after)<>4 or jsonb_typeof(v_after->0)<>'boolean' or octet_length(v_after->>0)>8192 or jsonb_typeof(v_after->1)<>'string' or v_after->>1!~'^-?(0|[1-9][0-9]*)(\.[0-9]+)?$' or jsonb_typeof(v_after->2)<>'string' or v_after->>2!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->3)<>'string' or v_after->>3!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      elsif v_sort in ('C_DEDUCTIONS_ASC','C_DEDUCTIONS_DESC') then
        if jsonb_array_length(v_after)<>6 or jsonb_typeof(v_after->0)<>'boolean' or octet_length(v_after->>0)>8192 or jsonb_typeof(v_after->1)<>'string' or octet_length(v_after->>1)>8192 or jsonb_typeof(v_after->2)<>'boolean' or octet_length(v_after->>2)>8192 or jsonb_typeof(v_after->3)<>'string' or octet_length(v_after->>3)>8192 or jsonb_typeof(v_after->4)<>'string' or v_after->>4!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->5)<>'string' or v_after->>5!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      elsif v_sort in ('C_STATUS_ASC','C_STATUS_DESC') then
        if jsonb_array_length(v_after)<>6 or jsonb_typeof(v_after->0)<>'boolean' or octet_length(v_after->>0)>8192 or jsonb_typeof(v_after->1)<>'boolean' or octet_length(v_after->>1)>8192 or jsonb_typeof(v_after->2)<>'string' or octet_length(v_after->>2)>8192 or jsonb_typeof(v_after->3)<>'string' or octet_length(v_after->>3)>8192 or jsonb_typeof(v_after->4)<>'string' or v_after->>4!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->5)<>'string' or v_after->>5!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      elsif v_sort in ('G_PAYMENT_ASC','G_PAYMENT_DESC') then
        if jsonb_array_length(v_after)<>5 or jsonb_typeof(v_after->0)<>'boolean' or octet_length(v_after->>0)>8192 or jsonb_typeof(v_after->1)<>'string' or v_after->>1!~'^[0-9]{4}-[0-9]{2}-[0-9]{2}$' or jsonb_typeof(v_after->2)<>'string' or octet_length(v_after->>2)>8192 or jsonb_typeof(v_after->3)<>'string' or v_after->>3!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->4)<>'string' or v_after->>4!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      elsif v_sort in ('G_CLIENT_ASC','G_CLIENT_DESC') then
        if jsonb_array_length(v_after)<>5 or jsonb_typeof(v_after->0)<>'boolean' or octet_length(v_after->>0)>8192 or jsonb_typeof(v_after->1)<>'string' or octet_length(v_after->>1)>8192 or jsonb_typeof(v_after->2)<>'string' or octet_length(v_after->>2)>8192 or jsonb_typeof(v_after->3)<>'string' or v_after->>3!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->4)<>'string' or v_after->>4!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      elsif v_sort in ('G_CHANNEL_ASC','G_CHANNEL_DESC') then
        if jsonb_array_length(v_after)<>5 or jsonb_typeof(v_after->0)<>'boolean' or octet_length(v_after->>0)>8192 or jsonb_typeof(v_after->1)<>'string' or octet_length(v_after->>1)>8192 or jsonb_typeof(v_after->2)<>'string' or octet_length(v_after->>2)>8192 or jsonb_typeof(v_after->3)<>'string' or v_after->>3!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->4)<>'string' or v_after->>4!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      elsif v_sort in ('G_AMOUNT_ASC','G_AMOUNT_DESC') then
        if jsonb_array_length(v_after)<>5 or jsonb_typeof(v_after->0)<>'boolean' or octet_length(v_after->>0)>8192 or jsonb_typeof(v_after->1)<>'string' or v_after->>1!~'^-?(0|[1-9][0-9]*)(\.[0-9]+)?$' or jsonb_typeof(v_after->2)<>'string' or octet_length(v_after->>2)>8192 or jsonb_typeof(v_after->3)<>'string' or v_after->>3!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->4)<>'string' or v_after->>4!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      elsif v_sort in ('G_DEDUCTIONS_ASC','G_DEDUCTIONS_DESC') then
        if jsonb_array_length(v_after)<>4 or jsonb_typeof(v_after->0)<>'boolean' or octet_length(v_after->>0)>8192 or jsonb_typeof(v_after->1)<>'string' or octet_length(v_after->>1)>8192 or jsonb_typeof(v_after->2)<>'string' or v_after->>2!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->3)<>'string' or v_after->>3!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      elsif v_sort in ('G_REASON_ASC','G_REASON_DESC') then
        if jsonb_array_length(v_after)<>5 or jsonb_typeof(v_after->0)<>'number' or v_after->>0!~'^[1-9][0-9]{0,2}$' or (v_after->>0)::numeric>200 or jsonb_typeof(v_after->1)<>'string' or octet_length(v_after->>1)>8192 or jsonb_typeof(v_after->2)<>'string' or octet_length(v_after->>2)>8192 or jsonb_typeof(v_after->3)<>'string' or v_after->>3!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->4)<>'string' or v_after->>4!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      else raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
    else
    v_key_count:=case when v_kind in ('CANDIDATES','PICK_CANDIDATES') then case when v_sort like 'DEDUCTIONS_%' or v_sort like 'STATUS_%' then 5 when v_sort like 'AMOUNT_%' then 3 else 4 end
      when v_kind='CANDIDATE_GROUPS' then case when v_sort like 'DEDUCTIONS_%' then 3 else 4 end when v_kind in ('ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL') then case when v_sort like 'PAYMENT_%' then 2 else 3 end else 1 end;
    if jsonb_typeof(v_after)<>'array' or jsonb_array_length(v_after)<>v_key_count then
      raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
    if v_kind in ('CANDIDATES','PICK_CANDIDATES') then
      if jsonb_typeof(v_after->(v_key_count-1))<>'string'
        or v_after->>(v_key_count-1)!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
        or (v_sort like 'AMOUNT_%' and (jsonb_typeof(v_after->0)<>'boolean' or jsonb_typeof(v_after->1)<>'string' or v_after->>1!~'^-?(0|[1-9][0-9]*)(\.[0-9]+)?$'))
        or (v_sort like 'DEDUCTIONS_%' and (jsonb_typeof(v_after->0)<>'boolean' or jsonb_typeof(v_after->1)<>'string' or jsonb_typeof(v_after->2)<>'boolean' or jsonb_typeof(v_after->3)<>'string'))
        or (v_sort like 'STATUS_%' and (jsonb_typeof(v_after->0)<>'boolean' or jsonb_typeof(v_after->1)<>'boolean' or jsonb_typeof(v_after->2)<>'string' or jsonb_typeof(v_after->3)<>'string'))
        or (v_sort like 'NAME_%' and (jsonb_typeof(v_after->0)<>'string' or jsonb_typeof(v_after->1)<>'boolean' or jsonb_typeof(v_after->2)<>'string')) then
        raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
    elsif v_kind='CANDIDATE_GROUPS' then
      if jsonb_typeof(v_after->(v_key_count-1))<>'string' or v_after->>(v_key_count-1)!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
        or jsonb_typeof(v_after->(v_key_count-2))<>'string' or v_after->>(v_key_count-2) not in ('WORK','CASE','ROW')
        or (v_sort like 'DEDUCTIONS_%' and jsonb_typeof(v_after->0)<>'boolean')
        or (v_sort like 'REASON_%' and (jsonb_typeof(v_after->0)<>'number' or v_after->>0!~'^[1-9][0-9]{0,2}$' or (v_after->>0)::numeric>200 or jsonb_typeof(v_after->1)<>'string'))
        or (v_sort not like 'DEDUCTIONS_%' and v_sort not like 'REASON_%' and (jsonb_typeof(v_after->0)<>'boolean' or jsonb_typeof(v_after->1)<>'string'))
        or ((v_sort='NAME_ASC' or v_sort like 'PAYMENT_%') and ((v_after->>0='true' and v_after->>1<>'') or (v_after->>0='false' and v_after->>1!~'^[0-9]{4}-[0-9]{2}-[0-9]{2}$')))
        or (v_sort like 'AMOUNT_%' and v_after->>1!~'^-?(0|[1-9][0-9]*)(\.[0-9]+)?$') then
        raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
    elsif v_kind in ('ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL') then
      if jsonb_typeof(v_after->(v_key_count-1))<>'string' or v_after->>(v_key_count-1)!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
        or (v_sort like 'AMOUNT_%' and (jsonb_typeof(v_after->0)<>'boolean' or jsonb_typeof(v_after->1)<>'string' or v_after->>1!~'^-?(0|[1-9][0-9]*)(\.[0-9]+)?$'))
        or (v_sort like 'NAME_%' and (jsonb_typeof(v_after->0)<>'string' or jsonb_typeof(v_after->1)<>'string' or v_after->>1!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'))
        or (v_sort like 'REASON_%' and (jsonb_typeof(v_after->0)<>'number' or v_after->>0!~'^(0|[1-9][0-9]*)$'
          or (v_after->>0)::numeric>32767 or jsonb_typeof(v_after->1)<>'string'))
        or (v_sort like 'PAYMENT_%' and jsonb_typeof(v_after->0)<>'string') then
        raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
    elsif jsonb_typeof(v_after->0)<>'string' or (v_kind='CANDIDATE_GROUPS'
      and v_after->>0!~'^(WORK|CASE):[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$')
      or (v_kind<>'CANDIDATE_GROUPS' and v_after->>0!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$') then
      raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
    end if;
  end if;
  case v_kind
    when 'HEADER' then v_page:=private.bpay_next_workbench_header_page_v1(v_read_scope,v_after,v_limit);
    when 'CANDIDATES' then v_page:=private.bpay_next_workbench_candidates_page_v1(v_read_scope,v_after,v_limit);
    when 'ACTION_REQUIRED' then v_page:=private.bpay_next_workbench_action_required_page_v1(v_read_scope,v_after,v_limit);
    when 'BLOCKED' then v_page:=private.bpay_next_workbench_blocked_page_v1(v_read_scope,v_after,v_limit);
    when 'ISSUE_DETAIL' then v_page:=private.bpay_next_workbench_issue_detail_page_v1(v_read_scope,v_after,v_limit);
    when 'CANDIDATE_GROUPS' then v_page:=private.bpay_next_workbench_candidate_groups_page_v1(v_read_scope,v_after,v_limit);
    when 'GROUP_LINES' then v_page:=private.bpay_next_workbench_group_lines_page_v1(v_read_scope,v_after,v_limit);
    when 'PICK_CANDIDATES' then v_page:=private.bpay_next_workbench_pick_candidates_page_v1(v_read_scope,v_after,v_limit);
    when 'PICK_CLIENTS' then v_page:=private.bpay_next_workbench_pick_clients_page_v1(v_read_scope,v_after,v_limit);
    when 'EXPORT_ROWS' then v_page:=private.bpay_next_workbench_export_rows_page_v1(v_read_scope,v_after,v_limit);
  end case;
  v_response:=jsonb_build_object('ok',true,'kind',v_kind,'workbench_revision',v_revision.workbench_revision::text,
    'complete',v_page->'complete','context',v_context,'rows',v_page->'rows',
    'affected_candidate_count',v_page->'affected_candidate_count',
    'next_cursor',case when v_page->>'complete'='true' then null else jsonb_build_object('context',v_context,'key',v_page->'last_key') end,
    'page_receipt',jsonb_build_object('context',v_context,'start_key',v_after,'end_key',v_page->'last_key',
      'row_count',jsonb_array_length(v_page->'rows')::text));
  if v_kind='HEADER' then
    v_response:=v_response||jsonb_build_object('job_progress',v_progress);
    v_response:=jsonb_set(v_response,'{rows,0}',(v_response#>'{rows,0}')||
      jsonb_build_object('oldest_pending_age_seconds',v_progress->'oldest_pending_age_seconds','can_create_draft',true));
  end if;
  if octet_length(v_response::text)>120000 then raise exception using errcode='54000',message='BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE';end if;
  return v_response;
exception when others then
  if p_request->>'kind'='SNOOZE_CHECK' then raise;end if;
  v_error:=case
    when sqlstate='42501' then 'BPAY_NEXT_WORKBENCH_FORBIDDEN'
    when sqlstate in ('22023','22P02','22003','22007','22008') then 'BPAY_NEXT_WORKBENCH_REQUEST_INVALID'
    when sqlstate='54000' then 'BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE'
    when sqlerrm in ('BPAY_NEXT_MODULE_NOT_ACTIVE','BPAY_NEXT_WORKBENCH_CURSOR_STALE',
      'BPAY_NEXT_WORKBENCH_PROJECTION_NOT_CURRENT','BPAY_NEXT_WORKBENCH_DATE_UNAVAILABLE') then sqlerrm
    else 'BPAY_NEXT_WORKBENCH_UNAVAILABLE' end;
  return jsonb_build_object('ok',false,'error_code',v_error,'retryable',v_error in
    ('BPAY_NEXT_WORKBENCH_UNAVAILABLE','BPAY_NEXT_WORKBENCH_PROJECTION_NOT_CURRENT'),
    'reload_required',v_error='BPAY_NEXT_WORKBENCH_CURSOR_STALE',
    'workbench_revision',v_revision.workbench_revision::text,'complete',false,
    'projection_state',v_revision.adoption_state,'dirty_candidate_count',v_revision.dirty_candidate_count::text,
    'dirty_group_count',v_revision.dirty_group_count::text,'pending_task_count',v_revision.pending_task_count::text);
end
$function$;

alter function private.bpay_next_case_allocation_plan_v1(jsonb) owner to postgres;
revoke all on function private.bpay_next_case_allocation_plan_v1(jsonb) from public,anon,authenticated,service_role;
alter function private.bpay_next_workbench_refresh_case_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_workbench_refresh_case_v1(uuid) from public,anon,authenticated,service_role;
alter function public.bpay_next_workbench_page_v1(jsonb) owner to postgres;
revoke all on function public.bpay_next_workbench_page_v1(jsonb) from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_workbench_page_v1(jsonb) to service_role;
notify pgrst,'reload schema';
commit;
