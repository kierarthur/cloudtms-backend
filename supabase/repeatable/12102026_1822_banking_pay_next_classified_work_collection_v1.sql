-- FC-15 current-parent successors; BD149 owner12October manual retention.
\set ON_ERROR_STOP on
begin;

-- Pure subordinate native observation; supplies no automatic money authority.
create or replace function private.bpay_next_work_collection_observation_v1(
  p_work_id uuid,p_component_key text,p_revision_id uuid
) returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare v_position private.bpay_next_position%rowtype;v_work private.bpay_next_work%rowtype;
  v_revision private.bpay_next_work_revision%rowtype;v_original private.bpay_next_work_revision%rowtype;
  v_basis private.bpay_next_work_collection%rowtype;v_line private.bpay_next_approved_line%rowtype;
  v_case private.bpay_next_finance_case%rowtype;v_component private.bpay_next_case_component%rowtype;
  v_current private.bpay_next_approved_line%rowtype;v_segments record;v_segmented boolean;
  v_complete boolean:=false;v_outstanding numeric;v_free numeric;v_negative numeric;v_positive numeric;v_overhang numeric;
begin
  select pos.* into v_position from private.bpay_next_position pos where pos.work_id=p_work_id and pos.component_key=p_component_key;
  select wrk.* into v_work from private.bpay_next_work wrk where wrk.id=p_work_id;
  select rev.* into v_revision from private.bpay_next_work_revision rev where rev.id=p_revision_id and rev.work_id=p_work_id;
  select coll.* into v_basis from private.bpay_next_work_collection coll where coll.work_id=p_work_id and coll.component_key=p_component_key;
  if v_basis.id is null or v_position.applied_revision_id is distinct from p_revision_id
    or v_work.current_revision_id is distinct from p_revision_id or v_work.approval_state<>'APPROVED'
    or v_revision.sealed_at_utc is null or v_position.approved_source_ex_vat<0 or v_position.realised_source_ex_vat<0 then
    return jsonb_build_object('complete',false,'segmented',false);end if;
  select rev.* into strict v_original from private.bpay_next_work_revision rev where rev.id=v_basis.original_revision_id;
  select line.* into strict v_line from private.bpay_next_approved_line line where line.id=v_basis.original_approved_line_id;
  select line.* into v_current from private.bpay_next_approved_line line where line.revision_id=p_revision_id and line.component_key=p_component_key;
  if v_basis.case_id is not null then
    select fc.* into strict v_case from private.bpay_next_finance_case fc where fc.id=v_basis.case_id;
    select cc.* into strict v_component from private.bpay_next_case_component cc where cc.id=v_basis.case_component_id;
    if (v_case.principal_approved,v_case.principal_recovered,v_case.principal_written_off,v_case.active_recovery_hold_amount)
      is distinct from (v_component.approved_source_ex_vat,v_component.recovered_source_ex_vat,
        v_component.written_off_source_ex_vat,v_component.active_recovery_source_ex_vat) then
      return jsonb_build_object('complete',false,'segmented',false);end if;
  end if;
  if v_revision.source_pay_channel is distinct from v_basis.original_source_pay_channel
    or v_position.source_basis_channel is distinct from v_basis.original_source_pay_channel then
    return jsonb_build_object('complete',false,'segmented',false);end if;
  -- The same typed authorised key, not an arbitrary absent DAY/aggregate.
  v_complete:=v_revision.detail_kind=v_original.detail_kind and v_current.id is not null
    and v_current.component_kind=v_line.component_kind
    and v_current.tax_treatment is not distinct from v_line.tax_treatment
    and v_current.source_component_id is not distinct from v_line.source_component_id
    and v_current.source_pay_ex_vat=v_position.approved_source_ex_vat;
  v_complete:=v_complete or (v_revision.certified_zero and v_revision.expected_line_count=0
    and v_revision.approved_source_ex_vat=0 and v_position.approved_source_ex_vat=0);
  -- A Source head must be the genuinely authorised committed book. An
  -- unrelated/unbound binding is retained as AMOUNT_UNBOUND, never zero.
  if v_work.work_kind='SOURCE' and v_revision.source_head_id is not null and not exists(
    select 1 from private.bpay_next_source_component_binding b
      join private.bpay_next_source_component_binding_version bv on bv.id=b.current_binding_version_id
      join private.weekly_source_entitlement_publication_receipts receipt on receipt.id=bv.source_receipt_id
      join public.weekly_source_entitlement_decision_bundles bundle on bundle.decision_bundle_id=bv.decision_bundle_id
        and bundle.bundle_revision=bv.bundle_revision and bundle.state='COMMITTED'
      join public.weekly_source_entitlement_heads head on head.id=bv.head_id and head.state='COMMITTED_CURRENT'
      join public.weekly_source_root_authorisations auth on auth.current_entitlement_head_id=head.id
        and auth.root_timesheet_id=head.root_timesheet_id and auth.withdrawn_at_utc is null
      join private.bpay_next_publication publication on publication.command_id=bv.publication_command_id_1
        and publication.work_id=p_work_id and publication.revision_id=p_revision_id and publication.candidate_id=v_work.candidate_id
      where b.canonical_work_id=p_work_id and b.canonical_component_key=p_component_key
        and bv.approval_work_id=p_work_id and bv.approval_component_key=p_component_key
        and bv.approval_revision_id=p_revision_id and bv.candidate_id=v_work.candidate_id
        and bv.publication_command_id_2 is null and bv.head_id=v_revision.source_head_id
        and v_revision.source_event_id=bv.head_id and bv.approval_source_channel=v_revision.source_pay_channel
        and receipt.decision_bundle_id=bv.decision_bundle_id and receipt.bundle_revision=bv.bundle_revision
        and receipt.request_digest=bv.request_digest and cardinality(receipt.head_ids)=1 and receipt.head_ids[1]=bv.head_id
        and publication.status in ('QUEUED','APPLYING','APPLIED')) then v_complete:=false;end if;
  v_segmented:=exists(select 1 from private.bpay_next_approved_segment a where a.revision_id=p_revision_id and a.component_key=p_component_key)
    or exists(select 1 from private.bpay_next_work_segment_origin_share s where s.work_id=p_work_id and s.component_key=p_component_key);
  if v_segmented then
    -- Reuse native maintained segment balances. Original actual target totals
    -- must also reconcile; no gross-to-net ratio or history re-sum.
    select count(d.segment_key) n,bool_and(d.approved_source_ex_vat>=0 and d.realised_source_ex_vat>=0) nonnegative,
      sum(d.approved_source_ex_vat) approved,sum(d.realised_source_ex_vat) realised,sum(d.held_source_ex_vat) held,
      sum(d.written_off_source_ex_vat) written_off,sum(d.active_recovery_source_ex_vat) recovery_held,
      sum(credit.ex) realised_ex,sum(credit.vat) realised_vat,sum(credit.inc) realised_inc,
      sum(held.ex) held_ex,sum(held.vat) held_vat,sum(held.inc) held_inc,
      sum(d.outstanding_source_ex_vat) outstanding,sum(d.free_recovery_source_ex_vat) free,
      sum(d.negative_source_ex_vat) negative,
      sum(greatest(d.approved_source_ex_vat-d.realised_source_ex_vat-d.held_source_ex_vat,0)) positive,
      sum(greatest(d.held_source_ex_vat-greatest(d.approved_source_ex_vat-d.realised_source_ex_vat,0),0)) overhang
      into v_segments from private.bpay_next_work_segment_debt_basis_v1(p_work_id,p_component_key,p_revision_id) d
      cross join lateral (select coalesce(sum(e.target_ex_vat),0) ex,coalesce(sum(e.target_vat),0) vat,coalesce(sum(e.target_inc_vat),0) inc
        from private.bpay_next_work_segment_effect_share e where e.work_id=p_work_id and e.component_key=p_component_key and e.segment_key=d.segment_key) credit
      cross join lateral (select coalesce(sum(s.target_reserved_ex_vat),0) ex,coalesce(sum(s.target_reserved_vat),0) vat,coalesce(sum(s.target_reserved_inc_vat),0) inc
        from private.bpay_next_work_segment_origin_share s join private.bpay_next_hold h on h.id=s.hold_id and h.status='ACTIVE'
        where s.work_id=p_work_id and s.component_key=p_component_key and s.segment_key=d.segment_key) held;
    v_complete:=v_complete and v_line.component_kind='WORK' and (v_revision.detail_kind='SHIFT' or v_revision.certified_zero)
      and v_segments.n>0 and v_segments.nonnegative
      and (v_segments.approved,v_segments.realised,v_segments.held,v_segments.realised_ex,v_segments.realised_vat,v_segments.realised_inc,
        v_segments.held_ex,v_segments.held_vat,v_segments.held_inc,v_segments.written_off,v_segments.recovery_held)
        is not distinct from (v_position.approved_source_ex_vat,v_position.realised_source_ex_vat,v_position.held_source_ex_vat,
          v_position.realised_target_ex_vat,v_position.realised_target_vat,v_position.realised_target_inc_vat,
          v_position.held_target_ex_vat,v_position.held_target_vat,v_position.held_target_inc_vat,
          coalesce(v_case.principal_written_off,0),coalesce(v_case.active_recovery_hold_amount,0));
    v_outstanding:=v_segments.outstanding;v_free:=v_segments.free;v_negative:=v_segments.negative;v_positive:=v_segments.positive;v_overhang:=v_segments.overhang;
  else
    v_outstanding:=greatest(v_position.realised_source_ex_vat-v_position.approved_source_ex_vat-coalesce(v_case.principal_written_off,0),
      coalesce(v_case.active_recovery_hold_amount,0),0);
    v_free:=v_outstanding-coalesce(v_case.active_recovery_hold_amount,0);
    v_negative:=greatest(v_position.realised_source_ex_vat-v_position.approved_source_ex_vat,0);
    v_positive:=greatest(v_position.approved_source_ex_vat-v_position.realised_source_ex_vat-v_position.held_source_ex_vat,0);
    v_overhang:=greatest(v_position.held_source_ex_vat-greatest(v_position.approved_source_ex_vat-v_position.realised_source_ex_vat,0),0);
  end if;
  return jsonb_build_object('complete',coalesce(v_complete,false),'segmented',v_segmented,
    'outstanding_source_ex_vat',case when v_complete then v_outstanding end,
    'free_recovery_source_ex_vat',case when v_complete then v_free end,
    'negative_source_ex_vat',case when v_complete then v_negative end,
    'positive_source_ex_vat',case when v_complete then v_positive end,
    'held_overhang_source_ex_vat',case when v_complete then v_overhang end);
end
$function$;


create or replace function private.bpay_next_work_collection_origin_v1(
  p_case_id uuid,p_component_id uuid,p_candidate_id uuid
) returns private.bpay_next_work_collection
language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_basis private.bpay_next_work_collection%rowtype;
begin
  select b.* into v_basis from private.bpay_next_work_collection b
    join private.bpay_next_position p on p.work_id=b.work_id and p.component_key=b.component_key
    join private.bpay_next_work_revision r on r.id=p.applied_revision_id and r.work_id=b.work_id
    join private.bpay_next_work_revision original on original.id=b.original_revision_id and original.work_id=b.work_id
    join private.bpay_next_work work on work.id=b.work_id and work.candidate_id=b.candidate_id
    join private.bpay_next_approved_line original_line on original_line.id=b.original_approved_line_id
      and original_line.revision_id=original.id and original_line.component_key=b.component_key
    join private.bpay_next_finance_case c on c.id=b.case_id and c.candidate_id=b.candidate_id
    join private.bpay_next_case_component x on x.id=b.case_component_id and x.case_id=c.id and x.candidate_id=c.candidate_id
    join private.bpay_next_case_rule rule on rule.id=x.rule_id and rule.case_id=c.id and rule.candidate_id=c.candidate_id
    where b.case_id=p_case_id and b.case_component_id=p_component_id and b.candidate_id=p_candidate_id
      and b.id=b.case_id and b.id=b.case_component_id and b.paid_basis_qualified
      and b.collection_mode='AUTOMATIC' and b.qualification_state='READY' and b.original_source_pay_channel='PAYE'
      and b.original_tax_treatment is not null and b.original_tax_treatment in ('TAXABLE','NON_TAXABLE')
      and (b.observed_revision_id,b.observed_approved_source_ex_vat,b.observed_realised_source_ex_vat)
        is not distinct from (p.applied_revision_id,p.approved_source_ex_vat,p.realised_source_ex_vat)
      and p.source_basis_channel='PAYE' and p.approved_source_ex_vat>=0 and p.realised_source_ex_vat>=0
      and (p.realised_target_ex_vat,p.realised_target_vat,p.realised_target_inc_vat)
        is not distinct from (p.realised_source_ex_vat,0::numeric,p.realised_source_ex_vat)
      and ((work.work_kind='ORDINARY' and original.source_kind='ORDINARY' and r.source_kind='ORDINARY')
        or (work.work_kind='EXPENSE' and original.source_kind='EXPENSE' and r.source_kind='EXPENSE'
          and original.financial_snapshot_id is not null and r.financial_snapshot_id is not null
          and original_line.component_kind in ('EXPENSE','MILEAGE') and original_line.tax_treatment='NON_TAXABLE')
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
        where a.revision_id=r.id and a.component_key=b.component_key and a.component_kind=original_line.component_kind
          and a.tax_treatment=original_line.tax_treatment and a.source_pay_ex_vat=p.approved_source_ex_vat
          and (work.work_kind in ('ORDINARY','EXPENSE') or a.source_component_id=original_line.source_component_id)))
        or (r.certified_zero and r.approved_source_ex_vat=0 and r.expected_line_count=0 and p.approved_source_ex_vat=0)
        or (b.segment_attribution_complete and coalesce((private.bpay_next_segment_collection_basis_v1(b.work_id,b.component_key,r.id)->>'complete')::boolean,false)))
      and (not b.segment_attribution_complete or (b.observed_segment_negative_ex_vat is not distinct from
        (private.bpay_next_segment_collection_basis_v1(b.work_id,b.component_key,r.id)->>'negative_source_ex_vat')::numeric
        and coalesce((private.bpay_next_segment_collection_basis_v1(b.work_id,b.component_key,r.id)->>'complete')::boolean,false)))
      and c.case_kind='OVERPAYMENT' and c.tax_treatment=b.original_tax_treatment and c.principal_funded=0
      and x.case_kind='OVERPAYMENT' and x.case_subtype='OVERPAYMENT' and x.tax_treatment=b.original_tax_treatment
      and x.source_pay_channel='PAYE' and x.instruction_kind='RECOVERY' and x.direction='DEDUCTION'
      and x.payroll_stage=case when b.original_tax_treatment='TAXABLE' then 'GROSS_DEDUCT' else 'NET_DEDUCT' end and x.component_key='RECOVERY' and x.component_ordinal=1
      and c.current_rule_id=rule.id and c.current_rule_revision=rule.rule_revision and rule.id=b.id
      and rule.weekly_due_source_ex_vat is null and rule.tax_treatment=b.original_tax_treatment
      and ((original.source_kind in ('ORDINARY','EXPENSE') and ((original_line.component_kind in ('WORK','ADDITIONAL') and original_line.tax_treatment='TAXABLE') or (original_line.component_kind in ('EXPENSE','MILEAGE') and original_line.tax_treatment='NON_TAXABLE')))
        or (work.work_kind='EXPENSE' and original.source_kind='EXPENSE' and r.source_kind='EXPENSE'
          and original.financial_snapshot_id is not null and r.financial_snapshot_id is not null
          and original_line.component_kind in ('EXPENSE','MILEAGE') and original_line.tax_treatment='NON_TAXABLE')
        or (work.work_kind='SOURCE' and original_line.component_kind='WORK' and original_line.tax_treatment='TAXABLE'))
      and (c.principal_approved,c.principal_recovered,c.principal_written_off,c.active_recovery_hold_amount)
        is not distinct from (x.approved_source_ex_vat,x.recovered_source_ex_vat,x.written_off_source_ex_vat,x.active_recovery_source_ex_vat);
  return v_basis;
end
$function$;

create or replace function private.bpay_next_reconcile_work_collection_v1(
  p_job_id uuid,p_work_id uuid,p_component_key text,p_effect_id uuid default null
) returns void
language plpgsql security invoker
set search_path=pg_catalog,private
as $function$
declare
  v_epoch bigint;v_candidate uuid;v_control private.bpay_next_worker_control%rowtype;
  v_job private.bpay_next_job%rowtype;v_work private.bpay_next_work%rowtype;
  v_position private.bpay_next_position%rowtype;v_revision private.bpay_next_work_revision%rowtype;
  v_original_revision private.bpay_next_work_revision%rowtype;
  v_original_approved private.bpay_next_approved_line%rowtype;
  v_basis private.bpay_next_work_collection%rowtype;v_effect private.bpay_next_financial_effect%rowtype;
  v_line private.bpay_next_run_line%rowtype;v_approved private.bpay_next_approved_line%rowtype;
  v_case private.bpay_next_finance_case%rowtype;v_component private.bpay_next_case_component%rowtype;
  v_state text;v_qualified boolean;v_capturable boolean;v_supported_paid boolean;
  v_original_detail text;v_whole_zero boolean;v_paired_origin boolean;
  v_segment_basis jsonb;v_segment_complete boolean;
  v_principal numeric;v_delta numeric;v_event text;
  v_observation jsonb;v_mode text;v_manual_amount numeric;v_manual_reason text;v_anchor uuid;
begin
  if p_job_id is null or p_work_id is null or p_component_key is null
     or pg_catalog.char_length(p_component_key) not between 1 and 256 then
    raise exception using errcode='22023',message='BPAY_NEXT_WORK_COLLECTION_INPUT_INVALID';
  end if;
  -- Parent owners already hold run (when applicable), Candidate control and
  -- job, in that order. These reentrant locks never acquire another run.
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select j.candidate_id into strict v_candidate from private.bpay_next_job j where j.id=p_job_id;
  select c.* into strict v_control from private.bpay_next_worker_control c where c.candidate_id=v_candidate for update;
  select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id for update;
  if v_job.status<>'LEASED' or v_job.module_epoch<>v_epoch
     or v_job.owner_epoch<>v_control.active_owner_epoch or v_job.lease_until_utc<=pg_catalog.clock_timestamp()
     or exists(select 1 from private.bpay_next_job j where j.candidate_id=v_candidate
       and j.command_sequence<v_job.command_sequence and j.status<>'DONE') then
    raise exception using errcode='55000',message='BPAY_NEXT_WORK_COLLECTION_OWNER_STALE';
  end if;
  select w.* into strict v_work from private.bpay_next_work w where w.id=p_work_id and w.candidate_id=v_candidate for update;
  select p.* into strict v_position from private.bpay_next_position p
    where p.work_id=p_work_id and p.component_key=p_component_key for update;
  if v_job.job_kind='POSITION_APPLY' then
    if p_effect_id is not null or v_job.phase not in ('NEW','REMOVED') or not exists(
      select 1 from private.bpay_next_publication p where p.command_id=v_job.command_id
        and p.work_id=p_work_id and p.candidate_id=v_candidate and p.status in ('QUEUED','APPLYING')
        and p.revision_id=v_position.applied_revision_id) then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_POSITION_SCOPE_INVALID';
    end if;
  elsif v_job.job_kind in ('CSV_SETTLEMENT','INTERNAL_SETTLEMENT') then
    if p_effect_id is null or v_job.phase<>'MEMBERS' then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_SETTLEMENT_SCOPE_INVALID';
    end if;
    select e.* into strict v_effect from private.bpay_next_financial_effect e where e.id=p_effect_id;
    if v_effect.effect_kind='WORK_COLLECTION_RECOVERED' then
      -- The negative WORK disposition is exactly the genuine posted CASE
      -- recovery, never a caller supplied negative or a cash-return inverse.
      if not exists(select 1 from private.bpay_next_work_collection b
        join private.bpay_next_case_event ce on ce.id=v_effect.operation_item_id and ce.case_id=b.case_id
        join private.bpay_next_run_case_instruction i on i.id=ce.instruction_id and i.work_collection_id=b.id
        join private.bpay_next_case_hold h on h.id=ce.operation_item_id and h.status='REALISED'
        join private.bpay_next_case_capacity_use u on u.case_hold_id=h.id and u.status='REALISED' and u.realisation_event_id=ce.id
        join private.bpay_next_outcome_request r on r.command_id=v_job.command_id and r.transfer_id=ce.original_transfer_id
        where b.work_id=p_work_id and b.component_key=p_component_key and b.candidate_id=v_candidate
          and ce.operation_id=v_job.command_id and ce.event_kind='RECOVERED'
          and ce.case_component_id=b.case_component_id and ce.source_amount_ex_vat>0
          and v_effect.operation_id=v_job.command_id and v_effect.work_id=b.work_id
          and v_effect.component_key=b.component_key and v_effect.candidate_id=b.candidate_id
          and v_effect.case_id=b.case_id and v_effect.original_transfer_id=ce.original_transfer_id
          and (v_effect.source_disposition_ex_vat,v_effect.target_amount_ex_vat,v_effect.target_amount_vat,v_effect.target_amount_inc_vat)
            is not distinct from (-ce.source_amount_ex_vat,-ce.target_amount_ex_vat,-ce.target_amount_vat,-ce.target_amount_inc_vat)) then
        raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_RECOVERY_SCOPE_INVALID';
      end if;
      v_capturable:=false;v_supported_paid:=true;
    else
    select l.* into strict v_line from private.bpay_next_hold h
      join private.bpay_next_run_line l on l.id=h.run_line_id
      join private.bpay_next_outcome_request r on r.command_id=v_job.command_id
      join private.bpay_next_transfer t on t.id=r.transfer_id
      where h.id=v_effect.operation_item_id and h.status='REALISED'
        and l.run_worker_id=r.run_worker_id and l.work_id=p_work_id and l.component_key=p_component_key
        and t.id=v_effect.original_transfer_id and t.candidate_id=v_candidate
        and t.original_transfer_id is null and t.return_cash_id is null
        and ((v_job.job_kind='CSV_SETTLEMENT' and t.execution_kind='BANK' and t.status in ('SETTLED','RETURNED'))
          or (v_job.job_kind='INTERNAL_SETTLEMENT' and t.execution_kind='INTERNAL_ZERO' and t.status='INTERNAL_PROCESSING'));
    if v_effect.operation_id<>v_job.command_id or v_effect.effect_kind<>'PAYROLL_SETTLED'
       or (v_effect.work_id,v_effect.component_key,v_effect.candidate_id)
         is distinct from (p_work_id,p_component_key,v_candidate)
       or v_effect.source_disposition_ex_vat<>v_line.source_consumed_ex_vat
       or v_effect.source_disposition_ex_vat<=0 then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_EFFECT_SCOPE_INVALID';
    end if;
    select a.* into strict v_approved from private.bpay_next_approved_line a where a.id=v_line.approved_line_id;
    -- Original ordinary or genuine Source PAYE -> PAYE WORK only. NULL
    -- tax is capturable as exact UNBOUND evidence, never inferred taxable.
    -- Do not return before checking an existing link: a later unsupported
    -- actual posting invalidates its paid qualification, not the payment.
    v_capturable:=v_work.work_kind='ORDINARY' and v_approved.component_kind in ('WORK','ADDITIONAL','EXPENSE','MILEAGE')
      and v_line.source_pay_channel='PAYE' and v_line.target_pay_channel='PAYE'
      and exists(select 1 from private.bpay_next_work_revision r
        where r.id=v_line.captured_revision_id and r.source_kind='ORDINARY');
    v_capturable:=v_capturable or (v_work.work_kind='EXPENSE' and v_approved.component_kind in ('EXPENSE','MILEAGE')
      and v_approved.tax_treatment='NON_TAXABLE' and v_line.source_pay_channel='PAYE' and v_line.target_pay_channel='PAYE'
      and exists(select 1 from private.bpay_next_work_revision r where r.id=v_line.captured_revision_id
        and r.source_kind='EXPENSE' and r.financial_snapshot_id is not null));
    -- PROTECTED is admitted only for 1550's actual committed-head branch;
    -- 0345's independent/headless protected target is not this authority.
    v_capturable:=v_capturable or (v_work.work_kind='SOURCE' and v_approved.component_kind='WORK'
      and v_approved.source_component_id is not null
      and v_approved.component_key='SOURCE:'||v_approved.source_component_id::text
      and v_line.source_pay_channel='PAYE' and v_line.target_pay_channel='PAYE'
      and exists(select 1 from private.bpay_next_work_revision r
        where r.id=v_line.captured_revision_id and r.work_id=v_work.id
          and ((r.source_kind='SOURCE' and r.source_event_id is not null
            and (r.source_head_id is null or r.source_event_id=r.source_head_id)) or (r.source_kind='PROTECTED'
            and r.source_head_id is not null and r.source_event_id=r.source_head_id))));
    v_supported_paid:=v_capturable and ((v_approved.component_kind in ('WORK','ADDITIONAL') and v_approved.tax_treatment='TAXABLE')
      or(v_work.work_kind in ('ORDINARY','EXPENSE') and v_approved.component_kind in ('EXPENSE','MILEAGE') and v_approved.tax_treatment='NON_TAXABLE'));
    end if;
  elsif v_job.job_kind='SIMPLE_CANCEL' then
    if p_effect_id is not null or v_job.phase<>'RELEASE' or not exists(
      select 1 from private.bpay_next_work_collection b
      join private.bpay_next_case_cancel_binding cb on cb.command_id=v_job.command_id and cb.candidate_id=b.candidate_id
      join private.bpay_next_cancel_request r on r.command_id=cb.command_id and r.status='CANCELLING'
      join private.bpay_next_run_worker w on w.id=cb.run_worker_id and w.status='CANCELLING'
      join private.bpay_next_case_hold h on h.run_worker_id=w.id and h.status='RELEASED'
      join private.bpay_next_run_case_instruction i on i.id=h.instruction_id and i.work_collection_id=b.id
      join private.bpay_next_case_capacity_use u on u.case_hold_id=h.id and u.status='RELEASED'
      where b.work_id=p_work_id and b.component_key=p_component_key and b.candidate_id=v_candidate
        and cb.status='CANCELLING' and cb.stage='CASE'
        and i.case_id=b.case_id and i.case_component_id=b.case_component_id) then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_CANCEL_SCOPE_INVALID';
    end if;
  elsif v_job.job_kind='PREPARE' then
    if p_effect_id is not null or not exists(select 1 from private.bpay_next_work_collection b
      where b.work_id=p_work_id and b.component_key=p_component_key and b.candidate_id=v_candidate
        and private.bpay_next_worker_released_collection_scope_v1(p_job_id,b.id,null)) then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_WORKER_RELEASE_SCOPE_INVALID';end if;
  elsif v_job.job_kind='PREPARATION_EXPIRY' then
    if p_effect_id is not null or v_job.phase<>'RELEASE' or not exists(
      select 1 from private.bpay_next_work_collection b
      where b.work_id=p_work_id and b.component_key=p_component_key and b.candidate_id=v_candidate
        and private.bpay_next_expired_work_collection_scope_v1(p_job_id,b.id,null)) then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_EXPIRY_SCOPE_INVALID';
    end if;
  elsif v_job.job_kind='PREPARATION_DISCARD' then
    if p_effect_id is not null or v_job.phase<>'RELEASE' or not exists(
      select 1 from private.bpay_next_work_collection b
      where b.work_id=p_work_id and b.component_key=p_component_key and b.candidate_id=v_candidate
        and private.bpay_next_discarded_work_collection_scope_v1(p_job_id,b.id,null)) then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_EXPIRY_SCOPE_INVALID';
    end if;
  else
    raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_JOB_KIND_INVALID';
  end if;
  select b.* into v_basis from private.bpay_next_work_collection b
    where b.work_id=p_work_id and b.component_key=p_component_key for update;
  if not found then
    -- An unpaid hold is not realised money. Existing pre-install paid rows
    -- have no invented basis/backfill; future actual settlement captures it.
    if p_effect_id is null then
      -- Current authorised change retains the original actual paid anchor.
      -- No business/history sweep, full-amount debt or invented payment.
      if v_position.realised_source_ex_vat<=0 then return;end if;
      select e.id into v_anchor from private.bpay_next_financial_effect e
        where e.work_id=p_work_id and e.component_key=p_component_key
          and e.effect_kind='PAYROLL_SETTLED' and e.source_disposition_ex_vat>0
        order by e.occurred_at_utc,e.id limit 1;
      if v_anchor is null then
        raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_PAID_ANCHOR_UNBOUND';end if;
      select eff.* into strict v_effect from private.bpay_next_financial_effect eff where eff.id=v_anchor;
      select l.* into strict v_line from private.bpay_next_hold h
        join private.bpay_next_run_line l on l.id=h.run_line_id
        where h.id=v_effect.operation_item_id and h.status='REALISED';
      select line.* into strict v_approved from private.bpay_next_approved_line line where line.id=v_line.approved_line_id;
      -- A lone exact original payment may qualify; mixed history never does.
      v_capturable:=v_work.work_kind='ORDINARY' and v_line.source_pay_channel='PAYE'
        and v_line.target_pay_channel='PAYE' and v_approved.component_kind in ('WORK','ADDITIONAL','EXPENSE','MILEAGE')
        and exists(select 1 from private.bpay_next_work_revision r where r.id=v_line.captured_revision_id and r.source_kind='ORDINARY');
      v_capturable:=v_capturable or(v_work.work_kind='EXPENSE' and v_line.source_pay_channel='PAYE' and v_line.target_pay_channel='PAYE'
        and v_approved.component_kind in ('EXPENSE','MILEAGE') and v_approved.tax_treatment='NON_TAXABLE'
        and exists(select 1 from private.bpay_next_work_revision r where r.id=v_line.captured_revision_id
          and r.source_kind='EXPENSE' and r.financial_snapshot_id is not null));
      v_supported_paid:=v_capturable and ((v_approved.component_kind in ('WORK','ADDITIONAL') and v_approved.tax_treatment='TAXABLE')
        or (v_approved.component_kind in ('EXPENSE','MILEAGE') and v_approved.tax_treatment='NON_TAXABLE'));
    else v_anchor:=p_effect_id;end if;
    insert into private.bpay_next_work_collection
      (work_id,component_key,candidate_id,original_effect_id,original_run_line_id,
       original_revision_id,original_approved_line_id,original_transfer_id,
       original_source_pay_channel,original_tax_treatment,paid_basis_qualified,original_valuation_policy_id,
       qualification_state,observed_revision_id,observed_approved_source_ex_vat,observed_realised_source_ex_vat,last_job_id,last_effect_id,collection_mode,manual_reason_code)
      values(p_work_id,p_component_key,v_candidate,v_anchor,v_line.id,v_line.captured_revision_id,
        v_line.approved_line_id,v_effect.original_transfer_id,v_line.source_pay_channel,v_approved.tax_treatment,
        coalesce(v_supported_paid,false) and v_position.realised_source_ex_vat=v_effect.source_disposition_ex_vat,v_line.valuation_policy_id,
        'CURRENT_APPROVAL_UNBOUND',v_position.applied_revision_id,v_position.approved_source_ex_vat,
        v_position.realised_source_ex_vat,p_job_id,p_effect_id,
        case when v_capturable then 'AUTOMATIC' else 'MANUAL' end,
        case when not coalesce(v_capturable,false) then 'AMOUNT_UNBOUND' end) returning * into v_basis;
  elsif v_basis.last_job_id=p_job_id and
      not (p_effect_id is not null and v_effect.effect_kind='WORK_COLLECTION_RECOVERED'
        and v_basis.last_effect_id is distinct from p_effect_id) then
    if (v_basis.observed_revision_id,v_basis.observed_approved_source_ex_vat,v_basis.observed_realised_source_ex_vat,v_basis.last_effect_id)
       is distinct from (v_position.applied_revision_id,v_position.approved_source_ex_vat,v_position.realised_source_ex_vat,p_effect_id) then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_SAME_JOB_CHANGED';
    end if;
    return;
  end if;
  v_qualified:=v_basis.paid_basis_qualified;
  if p_effect_id is not null and not coalesce(v_supported_paid,false) then v_qualified:=false;end if;
  select r.* into strict v_revision from private.bpay_next_work_revision r where r.id=v_position.applied_revision_id;
  select r.detail_kind into strict v_original_detail from private.bpay_next_work_revision r
    where r.id=v_basis.original_revision_id and r.work_id=p_work_id;
  select r.* into strict v_original_revision from private.bpay_next_work_revision r
    where r.id=v_basis.original_revision_id and r.work_id=p_work_id;
  select a.* into strict v_original_approved from private.bpay_next_approved_line a
    where a.id=v_basis.original_approved_line_id and a.revision_id=v_original_revision.id;
  v_paired_origin:=(v_work.work_kind='ORDINARY' and v_original_revision.source_kind='ORDINARY'
      and v_revision.source_kind='ORDINARY')
    or(v_work.work_kind='EXPENSE' and v_original_revision.source_kind='EXPENSE' and v_revision.source_kind='EXPENSE'
      and v_original_revision.financial_snapshot_id is not null and v_revision.financial_snapshot_id is not null
      and v_original_approved.component_kind in ('EXPENSE','MILEAGE') and v_original_approved.tax_treatment='NON_TAXABLE')
    or (v_work.work_kind='SOURCE' and v_original_approved.component_kind='WORK'
      and v_original_approved.source_component_id is not null
      and p_component_key='SOURCE:'||v_original_approved.source_component_id::text
      and ((v_original_revision.source_kind='SOURCE' and v_original_revision.source_event_id is not null
        and (v_original_revision.source_head_id is null
        or v_original_revision.source_event_id=v_original_revision.source_head_id)) or (v_original_revision.source_kind='PROTECTED'
        and v_original_revision.source_head_id is not null
        and v_original_revision.source_event_id=v_original_revision.source_head_id))
      and ((v_revision.source_kind='SOURCE' and v_revision.source_event_id is not null
        and (v_revision.source_head_id is null
        or v_revision.source_event_id=v_revision.source_head_id)) or (v_revision.source_kind='PROTECTED'
        and v_revision.source_head_id is not null and v_revision.source_event_id=v_revision.source_head_id)));
  -- Absence of a DAY key does not prove removal: an AGGREGATE or another
  -- business-date component may retain the entitlement. This first slice
  -- qualifies unchanged typed identity only. A genuinely sealed, certified
  -- whole-base empty zero is the sole missing-key exception; reconciliation
  -- across representations remains explicitly UNBOUND, not discarded.
  v_segment_basis:=private.bpay_next_segment_collection_basis_v1(p_work_id,p_component_key,v_position.applied_revision_id);
  v_segment_complete:=coalesce((v_segment_basis->>'complete')::boolean,false);
  v_whole_zero:=v_revision.sealed_at_utc is not null and v_revision.certified_zero
    and v_revision.approved_source_ex_vat=0 and v_revision.expected_line_count=0;
  if v_basis.original_tax_treatment is null or v_basis.original_tax_treatment not in ('TAXABLE','NON_TAXABLE') then v_state:='ORIGINAL_TAX_UNBOUND';
  elsif not v_qualified then v_state:='PAID_BASIS_UNBOUND';
  elsif v_job.job_kind not in ('SIMPLE_CANCEL','PREPARATION_EXPIRY','PREPARATION_DISCARD','PREPARE') and v_effect.effect_kind is distinct from 'WORK_COLLECTION_RECOVERED'
      and (v_work.approval_state<>'APPROVED'
        or v_work.current_revision_id is distinct from v_position.applied_revision_id) then
    v_state:='CURRENT_APPROVAL_UNBOUND';
  elsif v_paired_origin is not true or v_revision.source_pay_channel<>'PAYE'
      or v_position.source_basis_channel is distinct from 'PAYE'
      or (not v_whole_zero and not v_segment_complete and (v_revision.detail_kind<>v_original_detail
        or not exists(select 1 from private.bpay_next_approved_line a
          where a.revision_id=v_revision.id and a.component_key=p_component_key
            and a.component_kind=v_original_approved.component_kind and a.tax_treatment=v_original_approved.tax_treatment
            and (v_work.work_kind in ('ORDINARY','EXPENSE')
              or a.source_component_id=v_original_approved.source_component_id)))) then
    v_state:='CURRENT_SOURCE_UNBOUND';
  elsif v_position.approved_source_ex_vat<0 or v_position.realised_source_ex_vat<0 then v_state:='SIGNED_SOURCE_UNBOUND';
  else v_state:='READY';end if;
  if v_work.work_kind='SOURCE' and v_revision.source_head_id is not null and not exists (
    select 1 from private.bpay_next_source_component_binding b
      join private.bpay_next_source_component_binding_version v on v.id=b.current_binding_version_id
      join private.weekly_source_entitlement_publication_receipts receipt on receipt.id=v.source_receipt_id
      join public.weekly_source_entitlement_decision_bundles bundle on bundle.decision_bundle_id=v.decision_bundle_id
        and bundle.bundle_revision=v.bundle_revision and bundle.state='COMMITTED'
      join public.weekly_source_entitlement_heads head on head.id=v.head_id and head.state='COMMITTED_CURRENT'
      join public.weekly_source_root_authorisations auth on auth.current_entitlement_head_id=head.id
        and auth.root_timesheet_id=head.root_timesheet_id and auth.withdrawn_at_utc is null
      join private.bpay_next_publication publication on publication.command_id=v.publication_command_id_1
        and publication.work_id=v.approval_work_id and publication.revision_id=v.approval_revision_id
        and publication.candidate_id=v_candidate
      where b.canonical_work_id=p_work_id and b.canonical_component_key=p_component_key
        and v.approval_work_id=p_work_id and v.approval_component_key=p_component_key
        and v.approval_revision_id=v_position.applied_revision_id
        and v.head_id=v_revision.source_head_id and v_revision.source_event_id=v.head_id
        and v.publication_command_id_2 is null and v.candidate_id=v_candidate
        and v.approval_source_channel='PAYE' and v.tax_treatment='TAXABLE'
        and receipt.decision_bundle_id=v.decision_bundle_id and receipt.bundle_revision=v.bundle_revision
        and receipt.request_digest=v.request_digest and pg_catalog.cardinality(receipt.head_ids)=1
        and receipt.head_ids[1]=v.head_id and v_work.current_revision_id=v.approval_revision_id
        and ((v_job.job_kind='POSITION_APPLY' and publication.command_id=v_job.command_id
              and publication.status in ('QUEUED','APPLYING'))
          or (v_job.job_kind<>'POSITION_APPLY' and publication.status='APPLIED' and publication.phase='DONE'))
  ) then v_state:='CURRENT_SOURCE_UNBOUND';end if;
  v_observation:=private.bpay_next_work_collection_observation_v1(p_work_id,p_component_key,v_position.applied_revision_id);
  v_mode:=case when v_state='READY' and v_basis.original_source_pay_channel='PAYE' then 'AUTOMATIC' else 'MANUAL' end;
  v_manual_amount:=null;v_manual_reason:=null;
  if v_mode='MANUAL' then
    v_manual_reason:=case when coalesce((v_observation->>'complete')::boolean,false)
      then 'AUTOMATIC_COLLECTION_UNAVAILABLE' else 'AMOUNT_UNBOUND' end;
    if v_manual_reason='AUTOMATIC_COLLECTION_UNAVAILABLE' then
      v_manual_amount:=(v_observation->>'free_recovery_source_ex_vat')::numeric;end if;
  end if;
  if v_state='READY' or (v_basis.case_id is not null and v_manual_amount is not null) then
    if v_basis.case_id is null then
      v_principal:=case when v_segment_complete then (v_segment_basis->>'outstanding_source_ex_vat')::numeric
        else greatest(v_position.realised_source_ex_vat-v_position.approved_source_ex_vat,0) end;
      if v_principal>0 then
        insert into private.bpay_next_finance_case
          (id,candidate_id,case_kind,tax_treatment,status,principal_approved,order_key,case_revision)
          values(v_basis.id,v_candidate,'OVERPAYMENT',v_basis.original_tax_treatment,'OPEN',v_principal,v_job.command_sequence,1);
        insert into private.bpay_next_case_rule
          (id,case_id,candidate_id,rule_revision,case_kind,case_subtype,tax_treatment,case_created_at_utc)
          values(v_basis.id,v_basis.id,v_candidate,1,'OVERPAYMENT','OVERPAYMENT',v_basis.original_tax_treatment,pg_catalog.transaction_timestamp());
        update private.bpay_next_finance_case set current_rule_id=v_basis.id,current_rule_revision=1 where id=v_basis.id;
        insert into private.bpay_next_case_component
          (id,case_id,candidate_id,component_key,component_ordinal,component_revision,rule_id,
           case_kind,case_subtype,tax_treatment,instruction_kind,direction,payroll_stage,
           source_pay_channel,currency,approved_source_ex_vat,resolution_state)
          values(v_basis.id,v_basis.id,v_candidate,'RECOVERY',1,1,v_basis.id,
            'OVERPAYMENT','OVERPAYMENT',v_basis.original_tax_treatment,'RECOVERY','DEDUCTION',
            case when v_basis.original_tax_treatment='TAXABLE' then 'GROSS_DEDUCT' else 'NET_DEDUCT' end,'PAYE','GBP',v_principal,'REVIEW');
        v_basis.case_id:=v_basis.id;v_basis.case_component_id:=v_basis.id;v_delta:=v_principal;v_event:='APPROVED';
      end if;
    else
      select c.* into strict v_case from private.bpay_next_finance_case c where c.id=v_basis.case_id for update;
      select c.* into strict v_component from private.bpay_next_case_component c where c.id=v_basis.case_component_id for update;
      if (v_case.principal_approved,v_case.principal_recovered,v_case.principal_written_off,v_case.active_recovery_hold_amount)
         is distinct from (v_component.approved_source_ex_vat,v_component.recovered_source_ex_vat,
           v_component.written_off_source_ex_vat,v_component.active_recovery_source_ex_vat) then
        raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_BALANCE_MISMATCH';
      end if;
      -- R is actual SOURCE disposition, never held money/bank cash. Genuine
      -- future recovery must lower R with its own signed effect. Forgiveness
      -- W is separate; protected H is not repriced or released by an approval.
      v_principal:=v_case.principal_recovered+v_case.principal_written_off+
        case when v_mode='MANUAL' and coalesce((v_observation->>'segmented')::boolean,false) then
          (v_observation->>'outstanding_source_ex_vat')::numeric
        when v_segment_complete then (v_segment_basis->>'outstanding_source_ex_vat')::numeric else
        greatest(v_position.realised_source_ex_vat-v_position.approved_source_ex_vat-v_case.principal_written_off,
          v_case.active_recovery_hold_amount,0) end;
      v_delta:=v_principal-v_case.principal_approved;
      if v_delta<>0 then
        update private.bpay_next_finance_case set principal_approved=v_principal,
          status=case when v_principal=principal_recovered+principal_written_off then 'CLOSED'
            when status='PAUSED' then 'PAUSED' else 'OPEN' end,
          case_revision=case_revision+1,updated_at_utc=pg_catalog.transaction_timestamp() where id=v_basis.case_id;
        update private.bpay_next_case_component set approved_source_ex_vat=v_principal,
          component_revision=component_revision+1,resolution_state='REVIEW',
          target_pay_channel=null,target_ex_vat=null,target_vat=null,target_inc_vat=null,
          valuation_policy_id=null,valuation_window_id=null,resolution_ref=null,
          updated_at_utc=pg_catalog.transaction_timestamp() where id=v_basis.case_component_id;
        v_event:='CORRECTED';
      elsif v_principal=v_case.principal_recovered+v_case.principal_written_off and v_case.status<>'CLOSED' then
        update private.bpay_next_finance_case set status='CLOSED',case_revision=case_revision+1,
          updated_at_utc=pg_catalog.transaction_timestamp() where id=v_basis.case_id;
      end if;
    end if;
    if v_event is not null then
      insert into private.bpay_next_case_event
        (case_id,operation_id,operation_item_id,event_kind,approved_delta,original_transfer_id,original_effect_id)
        values(v_basis.case_id,v_job.command_id,v_basis.id,v_event,v_delta,
          v_basis.original_transfer_id,v_basis.original_effect_id);
    end if;
  end if;
  -- First insert records exact basis; this single follow-up records the
  -- observation/case under the same actual owner transaction. No view counter
  -- is independently incremented: existing position/posting owners do that.
  update private.bpay_next_work_collection set case_id=v_basis.case_id,case_component_id=v_basis.case_component_id,
    collection_mode=v_mode,manual_outstanding_source_ex_vat=v_manual_amount,manual_reason_code=v_manual_reason,
    paid_basis_qualified=v_qualified,qualification_state=v_state,observed_revision_id=v_position.applied_revision_id,
    observed_approved_source_ex_vat=v_position.approved_source_ex_vat,
    observed_realised_source_ex_vat=v_position.realised_source_ex_vat,
    segment_attribution_complete=v_segment_complete,
    observed_segment_negative_ex_vat=case when v_segment_complete then (v_segment_basis->>'negative_source_ex_vat')::numeric else 0 end,
    reconcile_revision=reconcile_revision+1,last_job_id=p_job_id,last_effect_id=p_effect_id,
    updated_at_utc=pg_catalog.transaction_timestamp()
    where id=v_basis.id;
  perform private.bpay_next_workbench_refresh_many_v1(array[p_work_id],
    case when v_basis.case_id is null then array[]::uuid[] else array[v_basis.case_id] end);
end
$function$;

create or replace function private.bpay_next_work_collection_guard_v1()
returns trigger language plpgsql
set search_path=pg_catalog,private
as $function$
declare v_effect private.bpay_next_financial_effect%rowtype;
  v_line private.bpay_next_run_line%rowtype;
begin
  if tg_op='DELETE' then
    raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_DELETE_FORBIDDEN';
  end if;
  if tg_op='UPDATE' then
    if (new.id,new.work_id,new.component_key,new.candidate_id,new.original_effect_id,
        new.original_run_line_id,new.original_revision_id,new.original_approved_line_id,
        new.original_transfer_id,new.original_source_pay_channel,new.original_tax_treatment,
        new.original_valuation_policy_id,new.created_at_utc)
       is distinct from
       (old.id,old.work_id,old.component_key,old.candidate_id,old.original_effect_id,
        old.original_run_line_id,old.original_revision_id,old.original_approved_line_id,
        old.original_transfer_id,old.original_source_pay_channel,old.original_tax_treatment,
        old.original_valuation_policy_id,old.created_at_utc)
       or (old.case_id is not null and (new.case_id,new.case_component_id)
           is distinct from (old.case_id,old.case_component_id))
       or (not old.paid_basis_qualified and new.paid_basis_qualified)
       or new.reconcile_revision<>old.reconcile_revision+1 then
      raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_BASIS_IMMUTABLE';
    end if;
  elsif new.reconcile_revision<>1 or new.case_id is not null
      or (new.last_effect_id is distinct from new.original_effect_id and not exists(select 1 from private.bpay_next_job j
        where j.id=new.last_job_id and j.job_kind='POSITION_APPLY' and new.last_effect_id is null)) then
    raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_INITIAL_STATE_INVALID';
  end if;
  select e.* into strict v_effect from private.bpay_next_financial_effect e where e.id=new.original_effect_id;
  select l.* into strict v_line from private.bpay_next_run_line l where l.id=new.original_run_line_id;
  if tg_op='INSERT' and not exists(select 1 from private.bpay_next_job j
      where j.id=new.last_job_id and j.command_id=v_effect.operation_id
        and j.job_kind in ('CSV_SETTLEMENT','INTERNAL_SETTLEMENT'))
      and not exists(select 1 from private.bpay_next_job j where j.id=new.last_job_id
        and j.job_kind='POSITION_APPLY' and j.phase in ('NEW','REMOVED') and new.last_effect_id is null) then
    raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_INITIAL_OWNER_INVALID';
  end if;
  if v_effect.effect_kind<>'PAYROLL_SETTLED' or v_effect.source_disposition_ex_vat<=0
     or (v_effect.work_id,v_effect.component_key,v_effect.candidate_id,v_effect.original_transfer_id)
       is distinct from (new.work_id,new.component_key,new.candidate_id,new.original_transfer_id)
     or (v_line.work_id,v_line.component_key,v_line.captured_revision_id,v_line.approved_line_id,
         v_line.source_pay_channel,v_line.valuation_policy_id)
       is distinct from (new.work_id,new.component_key,new.original_revision_id,new.original_approved_line_id,
         new.original_source_pay_channel,new.original_valuation_policy_id)
     or (new.collection_mode='AUTOMATIC' and (v_line.source_pay_channel<>'PAYE' or v_line.target_pay_channel<>'PAYE'))
     or (v_effect.source_disposition_ex_vat,v_effect.target_amount_ex_vat,v_effect.target_amount_vat,v_effect.target_amount_inc_vat)
       is distinct from (v_line.source_consumed_ex_vat,v_line.frozen_ex_vat,v_line.frozen_vat,v_line.frozen_inc_vat)
     or not exists(select 1 from private.bpay_next_hold h
       where h.id=v_effect.operation_item_id and h.run_line_id=v_line.id and h.status='REALISED'
         and h.source_reserved_ex_vat=v_effect.source_disposition_ex_vat
         and h.source_reserved_ex_vat=v_line.source_consumed_ex_vat)
     or not exists(select 1 from private.bpay_next_approved_line a
       join private.bpay_next_work_revision r on r.id=a.revision_id
       join private.bpay_next_work w on w.id=r.work_id
       where a.id=new.original_approved_line_id and a.component_kind in ('WORK','ADDITIONAL','EXPENSE','MILEAGE','PROTECTED_WORK','SOURCE_FIXED_EXPENSE')
         and a.tax_treatment is not distinct from new.original_tax_treatment
         and w.id=new.work_id and a.component_key=new.component_key
         and ((r.source_kind='ORDINARY' and w.work_kind='ORDINARY')
           or (w.work_kind='EXPENSE' and r.source_kind='EXPENSE' and r.financial_snapshot_id is not null
             and a.component_kind in ('EXPENSE','MILEAGE') and a.tax_treatment='NON_TAXABLE')
           or (new.collection_mode='MANUAL' and w.work_kind='SOURCE' and r.source_kind in ('SOURCE','PROTECTED'))
           or (w.work_kind='SOURCE' and a.source_component_id is not null
             and a.component_key='SOURCE:'||a.source_component_id::text
             and ((r.source_kind='SOURCE' and r.source_event_id is not null
               and (r.source_head_id is null or r.source_event_id=r.source_head_id)) or (r.source_kind='PROTECTED'
               and r.source_head_id is not null and r.source_event_id=r.source_head_id)))))
     or not exists(select 1 from private.bpay_next_outcome_request original_request
       join private.bpay_next_job original_job on original_job.command_id=original_request.command_id
       join private.bpay_next_transfer original_transfer on original_transfer.id=original_request.transfer_id
       where original_request.command_id=v_effect.operation_id and original_request.transfer_id=new.original_transfer_id
         and original_request.run_worker_id=v_line.run_worker_id and original_job.candidate_id=new.candidate_id
         and original_job.job_kind in ('CSV_SETTLEMENT','INTERNAL_SETTLEMENT')
         and original_transfer.candidate_id=new.candidate_id and original_transfer.original_transfer_id is null
         and original_transfer.return_cash_id is null)
     or not exists(select 1 from private.bpay_next_transfer_member m
       where m.transfer_id=new.original_transfer_id and m.run_line_id=v_line.id and m.subject_kind='WORK'
         and m.signed_cash_contribution=v_line.frozen_inc_vat) then
    raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_ORIGINAL_BASIS_INVALID';
  end if;
  if new.case_id is not null and
     (new.case_id<>new.id or new.case_component_id<>new.id or not exists(
       select 1 from private.bpay_next_finance_case c
       join private.bpay_next_case_component x on x.id=new.case_component_id and x.case_id=c.id
       join private.bpay_next_case_rule r on r.id=x.rule_id and r.case_id=c.id
       where c.id=new.case_id and c.candidate_id=new.candidate_id
         and c.case_kind='OVERPAYMENT' and c.tax_treatment=new.original_tax_treatment and c.principal_funded=0
         and c.principal_approved=x.approved_source_ex_vat
         and c.principal_recovered=x.recovered_source_ex_vat
         and c.principal_written_off=x.written_off_source_ex_vat
         and c.active_recovery_hold_amount=x.active_recovery_source_ex_vat
         and x.candidate_id=c.candidate_id and x.component_key='RECOVERY' and x.component_ordinal=1
         and x.case_kind='OVERPAYMENT' and x.tax_treatment=new.original_tax_treatment and x.source_pay_channel='PAYE'
         and x.instruction_kind='RECOVERY' and x.payroll_stage=case when new.original_tax_treatment='TAXABLE' then 'GROSS_DEDUCT' else 'NET_DEDUCT' end
         and c.current_rule_id=r.id and c.current_rule_revision=r.rule_revision
         and r.id=new.id and r.case_kind='OVERPAYMENT' and r.tax_treatment=new.original_tax_treatment and r.weekly_due_source_ex_vat is null)) then
    raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_CASE_OWNER_INVALID';
  end if;
  if not exists(select 1 from private.bpay_next_job j
     join private.bpay_next_worker_control c on c.candidate_id=j.candidate_id
     join private.bpay_next_module_control m on m.id=1 and m.active_owner='NEXT'
     where j.id=new.last_job_id and j.candidate_id=new.candidate_id
       and j.job_kind in ('POSITION_APPLY','CSV_SETTLEMENT','INTERNAL_SETTLEMENT','SIMPLE_CANCEL','PREPARATION_EXPIRY','PREPARATION_DISCARD','PREPARE')
       and j.status='LEASED' and j.module_epoch=m.owner_epoch
       and j.owner_epoch=c.active_owner_epoch and j.lease_until_utc>pg_catalog.clock_timestamp()
       and ((j.job_kind='POSITION_APPLY' and j.phase in ('NEW','REMOVED') and new.last_effect_id is null
         and exists(select 1 from private.bpay_next_publication p where p.command_id=j.command_id
           and p.work_id=new.work_id and p.candidate_id=new.candidate_id
           and p.revision_id=new.observed_revision_id and p.status in ('QUEUED','APPLYING')))
         or (j.job_kind in ('CSV_SETTLEMENT','INTERNAL_SETTLEMENT') and j.phase='MEMBERS'
           and exists(select 1 from private.bpay_next_financial_effect e
             join private.bpay_next_outcome_request r on r.command_id=j.command_id
             where e.id=new.last_effect_id and e.operation_id=j.command_id and e.effect_kind='PAYROLL_SETTLED'
               and e.work_id=new.work_id and e.component_key=new.component_key and e.candidate_id=new.candidate_id
               and e.original_transfer_id=r.transfer_id and e.source_disposition_ex_vat>0))
         or (j.job_kind in ('CSV_SETTLEMENT','INTERNAL_SETTLEMENT') and j.phase='MEMBERS'
           and exists(select 1 from private.bpay_next_financial_effect e
             join private.bpay_next_case_event ce on ce.id=e.operation_item_id and ce.operation_id=j.command_id
             join private.bpay_next_run_case_instruction i on i.id=ce.instruction_id and i.work_collection_id=new.id
             join private.bpay_next_case_hold h on h.id=ce.operation_item_id and h.status='REALISED'
             join private.bpay_next_case_capacity_use u on u.case_hold_id=h.id and u.status='REALISED' and u.realisation_event_id=ce.id
             join private.bpay_next_outcome_request r on r.command_id=j.command_id and r.transfer_id=ce.original_transfer_id
             where e.id=new.last_effect_id and e.operation_id=j.command_id and e.effect_kind='WORK_COLLECTION_RECOVERED'
               and ce.event_kind='RECOVERED' and ce.case_id=new.case_id and ce.case_component_id=new.case_component_id
               and e.work_id=new.work_id and e.component_key=new.component_key and e.candidate_id=new.candidate_id
               and e.original_transfer_id=ce.original_transfer_id
               and (e.source_disposition_ex_vat,e.target_amount_ex_vat,e.target_amount_vat,e.target_amount_inc_vat)
                 is not distinct from (-ce.source_amount_ex_vat,-ce.target_amount_ex_vat,-ce.target_amount_vat,-ce.target_amount_inc_vat)))
         or (j.job_kind='PREPARE' and new.last_effect_id is null
           and private.bpay_next_worker_released_collection_scope_v1(j.id,new.id,null))
         or (j.job_kind='PREPARATION_EXPIRY' and j.phase='RELEASE' and new.last_effect_id is null
           and private.bpay_next_expired_work_collection_scope_v1(j.id,new.id,null))
         or (j.job_kind='PREPARATION_DISCARD' and j.phase='RELEASE' and new.last_effect_id is null
           and private.bpay_next_discarded_work_collection_scope_v1(j.id,new.id,null))
         or (j.job_kind='SIMPLE_CANCEL' and j.phase='RELEASE' and new.last_effect_id is null
           and exists(select 1 from private.bpay_next_case_cancel_binding cb
             join private.bpay_next_cancel_request r on r.command_id=cb.command_id and r.status='CANCELLING'
             join private.bpay_next_run_worker w on w.id=cb.run_worker_id and w.status='CANCELLING'
             join private.bpay_next_case_hold h on h.run_worker_id=w.id and h.status='RELEASED'
             join private.bpay_next_run_case_instruction i on i.id=h.instruction_id and i.work_collection_id=new.id
             join private.bpay_next_case_capacity_use u on u.case_hold_id=h.id and u.status='RELEASED'
             where cb.command_id=j.command_id and cb.candidate_id=new.candidate_id
               and cb.status='CANCELLING' and cb.stage='CASE'
               and i.case_id=new.case_id and i.case_component_id=new.case_component_id)))) then
    raise exception using errcode='55000',message='BPAY_NEXT_WORK_COLLECTION_OWNER_STALE';
  end if;
  if not exists(select 1 from private.bpay_next_position p
      where p.work_id=new.work_id and p.component_key=new.component_key
        and (p.applied_revision_id,p.approved_source_ex_vat,p.realised_source_ex_vat)
          is not distinct from (new.observed_revision_id,new.observed_approved_source_ex_vat,new.observed_realised_source_ex_vat)) then
    raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_OBSERVATION_INVALID';
  end if;
  if new.segment_attribution_complete is distinct from coalesce((private.bpay_next_segment_collection_basis_v1(
      new.work_id,new.component_key,new.observed_revision_id)->>'complete')::boolean,false)
    or new.observed_segment_negative_ex_vat is distinct from (case when new.segment_attribution_complete then
      (private.bpay_next_segment_collection_basis_v1(new.work_id,new.component_key,new.observed_revision_id)->>'negative_source_ex_vat')::numeric else 0::numeric end) then
    -- The original INSERT records only its native paid anchor; the same-owner
    -- follow-up supplies the qualified observation, exactly as the predecessor.
    if tg_op<>'INSERT' or new.segment_attribution_complete or new.observed_segment_negative_ex_vat<>0 then
      raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_SEGMENT_OBSERVATION_INVALID';
    end if;
  end if;
  if new.collection_mode='MANUAL' then
    if (new.manual_reason_code,new.manual_outstanding_source_ex_vat) is distinct from
      (case when coalesce((private.bpay_next_work_collection_observation_v1(new.work_id,new.component_key,new.observed_revision_id)->>'complete')::boolean,false)
        then 'AUTOMATIC_COLLECTION_UNAVAILABLE' else 'AMOUNT_UNBOUND' end,
       case when coalesce((private.bpay_next_work_collection_observation_v1(new.work_id,new.component_key,new.observed_revision_id)->>'complete')::boolean,false)
        then (private.bpay_next_work_collection_observation_v1(new.work_id,new.component_key,new.observed_revision_id)->>'free_recovery_source_ex_vat')::numeric else null::numeric end)
      and tg_op<>'INSERT' then raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_MANUAL_OBSERVATION_INVALID';end if;
  end if;
  return new;
end
$function$;

create or replace function private.bpay_next_expired_work_collection_scope_v1(
  p_job_id uuid,p_collection_id uuid,p_case_hold_id uuid default null
) returns boolean language sql volatile security invoker set search_path=pg_catalog,private
as $function$
  select exists(select 1 from private.bpay_next_job j
    join private.bpay_next_module_control m on m.id=1 and m.active_owner='NEXT' and m.owner_epoch=j.module_epoch
    join private.bpay_next_worker_control c on c.candidate_id=j.candidate_id and c.active_owner_epoch=j.owner_epoch
    join private.bpay_next_preparation_expiry_worker x on x.job_id=j.id and x.command_id=j.command_id and x.candidate_id=j.candidate_id
    join private.bpay_next_preparation_expiry_request q on q.command_id=x.command_id and q.run_id=x.run_id
    join private.bpay_next_command command on command.id=j.command_id and command.command_kind='PREPARATION_EXPIRY'
      and command.module_epoch=j.module_epoch and command.agency_sequence=j.command_sequence
    join private.bpay_next_job original on original.id=x.original_job_id and original.command_id=q.original_prepare_command_id
      and original.candidate_id=x.candidate_id and original.module_epoch=j.module_epoch and original.job_kind='PREPARE' and original.status='DONE'
    join private.bpay_next_pay_run r on r.id=x.run_id
    join private.bpay_next_run_worker w on w.id=x.run_worker_id and w.run_id=x.run_id and w.candidate_id=x.candidate_id
    join private.bpay_next_work_collection b on b.id=p_collection_id and b.candidate_id=x.candidate_id
    join private.bpay_next_run_case_instruction i on i.run_worker_id=w.id and i.preparation_revision=x.preparation_revision
      and i.case_component_id=b.case_component_id and i.case_id=b.case_id and i.work_collection_id=b.id
      and i.selection_revision=x.selection_revision and i.valuation_policy_id=b.original_valuation_policy_id
    join private.bpay_next_case_allocation_state s on s.id=x.draft_state_id and s.run_worker_id=w.id
      and s.candidate_id=x.candidate_id and s.preparation_revision=x.preparation_revision and s.selection_revision=x.selection_revision
      and private.bpay_next_prepare_job_run_v1(s.job_id)=x.run_id and s.pass_kind='DRAFT' and s.projection_no=0
    join private.bpay_next_case_allocation_result a on a.state_id=s.id and a.instruction_id=i.id and a.pass_kind='DRAFT'
    join private.bpay_next_case_hold h on h.allocation_result_id=a.id and h.instruction_id=i.id
      and h.run_worker_id=w.id and h.case_id=b.case_id and h.case_component_id=b.case_component_id and h.candidate_id=x.candidate_id
    join private.bpay_next_case_capacity_use u on u.case_hold_id=h.id
    where j.id=p_job_id and j.job_kind='PREPARATION_EXPIRY' and j.status='LEASED' and j.phase='RELEASE'
      and j.lease_nonce is not null and j.lease_until_utc>pg_catalog.clock_timestamp()
      and j.cursor_key is not distinct from nullif(x.checkpoint,0)::text
      and x.status='RELEASING' and x.stage='CASE' and x.expected_case_hold_count>0
      and x.released_case_hold_count<x.expected_case_hold_count
      and q.status='CANCELLING' and q.accepted_at_utc>=q.captured_deadline
      and q.captured_deadline=r.preparation_expires_at_utc and r.preparation_expires_at_utc=r.created_at_utc+interval '72 hours'
      and r.status='CANCELLING' and r.confirmed_at_utc is null and r.review_revision=q.selection_review_revision+1
      and (r.selection_count,r.selection_page_count,r.selected_candidate_count)
        is not distinct from (q.selection_count,q.selection_page_count,q.expected_candidate_count)
      and (command.expected_member_count,command.enrolled_member_count)
        is not distinct from (q.expected_candidate_count,q.enrolled_candidate_count)
      and q.original_prepare_command_id=(select command_id from private.bpay_next_run_command where run_id=r.id)
      and not x.already_worker_released and w.status='CANCELLING' and w.captured_preparation_revision=x.preparation_revision
      and w.preparation_revision=coalesce(x.expected_execution_revision,x.preparation_revision) and w.case_selection_revision=x.selection_revision
      and w.realised_effect_count=0 and w.net_request_revision=0 and w.net_projection_revision=0
      and i.case_kind='OVERPAYMENT' and i.tax_treatment=b.original_tax_treatment and i.tax_treatment is not null and i.tax_treatment in ('TAXABLE','NON_TAXABLE') and i.source_pay_channel='PAYE' and i.target_pay_channel='PAYE'
      and i.instruction_kind='RECOVERY' and i.direction='DEDUCTION' and (i.tax_treatment,i.payroll_stage,i.hold_purpose) in
        (('TAXABLE','GROSS_DEDUCT','GROSS_RECOVERY'),('NON_TAXABLE','NET_DEDUCT','NET_RECOVERY_CAPACITY'))
      and h.status='RELEASED' and h.allocation_pass_kind='DRAFT' and h.projection_id is null
      and h.finished_at_utc>=q.accepted_at_utc and (x.case_hold_cursor is null or h.id>x.case_hold_cursor)
      and (p_case_hold_id is null or h.id=p_case_hold_id)
      and u.status='RELEASED' and u.realisation_event_id is null and u.source_amount_ex_vat=h.source_reserved_ex_vat
      and (h.source_reserved_ex_vat,h.target_amount_ex_vat,h.target_amount_vat,h.target_amount_inc_vat)
        is not distinct from (a.allocated_source_ex_vat,a.allocated_target_ex_vat,a.allocated_target_vat,a.allocated_target_inc_vat)
      and not exists(select 1 from private.bpay_next_transfer where run_worker_id=w.id)
      and not exists(select 1 from private.bpay_next_net_projection where run_worker_id=w.id)
      and not exists(select 1 from private.bpay_next_job earlier where earlier.candidate_id=j.candidate_id
        and earlier.command_sequence<j.command_sequence and earlier.status<>'DONE'))
$function$;

create or replace function private.bpay_next_worker_released_collection_scope_v1(p_job_id uuid,p_collection_id uuid,p_case_hold_id uuid default null)
returns boolean language sql volatile security invoker set search_path=pg_catalog,private
as $function$
  select exists(select 1 from private.bpay_next_job j
    join private.bpay_next_worker_reprepare_request x on x.job_id=j.id and x.action='RELEASE' and x.status='RUNNING' and x.stage='CASE'
    join private.bpay_next_run_worker w on w.id=x.run_worker_id and w.preparation_revision=x.target_preparation_revision
    join private.bpay_next_case_allocation_state s on s.id=x.draft_state_id and s.run_worker_id=w.id
      and s.preparation_revision=x.captured_preparation_revision and s.selection_revision=x.selection_revision
      and s.pass_kind='DRAFT' and s.projection_no=0 and s.module_epoch=x.module_epoch
      and private.bpay_next_prepare_job_run_v1(s.job_id)=x.run_id
    join private.bpay_next_work_collection b on b.id=p_collection_id and b.candidate_id=x.candidate_id
    join private.bpay_next_run_case_instruction i on i.run_worker_id=w.id and i.case_id=b.case_id and i.case_component_id=b.case_component_id
      and i.work_collection_id=b.id and i.preparation_revision=x.captured_preparation_revision and i.selection_revision=x.selection_revision
      and i.valuation_policy_id=b.original_valuation_policy_id
    join private.bpay_next_case_allocation_result a on a.state_id=s.id and a.instruction_id=i.id and a.pass_kind='DRAFT'
    join private.bpay_next_case_hold h on h.allocation_result_id=a.id and h.instruction_id=i.id and h.run_worker_id=w.id
      and h.case_id=b.case_id and h.case_component_id=b.case_component_id and h.candidate_id=x.candidate_id
    join private.bpay_next_case_capacity_use u on u.case_hold_id=h.id
    where j.id=p_job_id and private.bpay_next_prepare_release_scope_v1(j.id,w.id,'CASE')
      and i.case_kind='OVERPAYMENT' and i.tax_treatment=b.original_tax_treatment and i.tax_treatment is not null and i.tax_treatment in ('TAXABLE','NON_TAXABLE') and i.source_pay_channel='PAYE' and i.target_pay_channel='PAYE'
      and i.instruction_kind='RECOVERY' and i.direction='DEDUCTION' and (i.tax_treatment,i.payroll_stage,i.hold_purpose) in
        (('TAXABLE','GROSS_DEDUCT','GROSS_RECOVERY'),('NON_TAXABLE','NET_DEDUCT','NET_RECOVERY_CAPACITY'))
      and h.status='RELEASED' and h.allocation_pass_kind='DRAFT' and h.projection_id is null
      and h.finished_at_utc>=x.accepted_at_utc and (x.case_hold_cursor is null or h.id>x.case_hold_cursor)
      and x.released_case_hold_count<x.expected_case_hold_count and (p_case_hold_id is null or h.id=p_case_hold_id)
      and u.status='RELEASED' and u.realisation_event_id is null and u.source_amount_ex_vat=h.source_reserved_ex_vat
      and (h.source_reserved_ex_vat,h.target_amount_ex_vat,h.target_amount_vat,h.target_amount_inc_vat)
        is not distinct from (a.allocated_source_ex_vat,a.allocated_target_ex_vat,a.allocated_target_vat,a.allocated_target_inc_vat))
$function$;

create or replace function private.bpay_next_discarded_work_collection_scope_v1(
  p_job_id uuid,p_collection_id uuid,p_case_hold_id uuid default null
) returns boolean language sql volatile security invoker set search_path=pg_catalog,private
as $function$
  select exists(select 1 from private.bpay_next_job j
    join private.bpay_next_module_control m on m.id=1 and m.active_owner='NEXT' and m.owner_epoch=j.module_epoch
    join private.bpay_next_worker_control c on c.candidate_id=j.candidate_id and c.active_owner_epoch=j.owner_epoch
    join private.bpay_next_preparation_discard_worker x on x.job_id=j.id and x.command_id=j.command_id and x.candidate_id=j.candidate_id
    join private.bpay_next_preparation_discard_request q on q.command_id=x.command_id and q.run_id=x.run_id
    join private.bpay_next_command command on command.id=j.command_id and command.command_kind='PREPARATION_DISCARD'
      and command.module_epoch=j.module_epoch and command.agency_sequence=j.command_sequence
    join private.bpay_next_job original on original.id=x.original_job_id and original.command_id=q.original_prepare_command_id
      and original.candidate_id=x.candidate_id and original.module_epoch=j.module_epoch and original.job_kind='PREPARE' and original.status='DONE'
    join private.bpay_next_pay_run r on r.id=x.run_id
    join private.bpay_next_run_worker w on w.id=x.run_worker_id and w.run_id=x.run_id and w.candidate_id=x.candidate_id
    join private.bpay_next_work_collection b on b.id=p_collection_id and b.candidate_id=x.candidate_id
    join private.bpay_next_run_case_instruction i on i.run_worker_id=w.id and i.preparation_revision=x.preparation_revision
      and i.case_component_id=b.case_component_id and i.case_id=b.case_id and i.work_collection_id=b.id
      and i.selection_revision=x.selection_revision and i.valuation_policy_id=b.original_valuation_policy_id
    join private.bpay_next_case_allocation_state s on s.id=x.draft_state_id and s.run_worker_id=w.id
      and s.candidate_id=x.candidate_id and s.preparation_revision=x.preparation_revision and s.selection_revision=x.selection_revision
      and private.bpay_next_prepare_job_run_v1(s.job_id)=x.run_id and s.pass_kind='DRAFT' and s.projection_no=0
    join private.bpay_next_case_allocation_result a on a.state_id=s.id and a.instruction_id=i.id and a.pass_kind='DRAFT'
    join private.bpay_next_case_hold h on h.allocation_result_id=a.id and h.instruction_id=i.id
      and h.run_worker_id=w.id and h.case_id=b.case_id and h.case_component_id=b.case_component_id and h.candidate_id=x.candidate_id
    join private.bpay_next_case_capacity_use u on u.case_hold_id=h.id
    where j.id=p_job_id and j.job_kind='PREPARATION_DISCARD' and j.status='LEASED' and j.phase='RELEASE'
      and j.lease_nonce is not null and j.lease_until_utc>pg_catalog.clock_timestamp()
      and j.cursor_key is not distinct from nullif(x.checkpoint,0)::text
      and x.status='RELEASING' and x.stage='CASE' and x.expected_case_hold_count>0
      and x.released_case_hold_count<x.expected_case_hold_count
      and q.status='CANCELLING' and q.captured_selection_state=r.selection_state
      and r.status='CANCELLING' and r.confirmed_at_utc is null and r.review_revision=q.selection_review_revision+1
      and (r.selection_count,r.selection_page_count,r.selected_candidate_count)
        is not distinct from (q.selection_count,q.selection_page_count,q.expected_candidate_count)
      and (command.expected_member_count,command.enrolled_member_count)
        is not distinct from (q.expected_candidate_count,q.enrolled_candidate_count)
      and q.original_prepare_command_id=(select command_id from private.bpay_next_run_command where run_id=r.id)
      and not x.already_worker_released and w.status='CANCELLING' and w.captured_preparation_revision=x.preparation_revision
      and w.preparation_revision=coalesce(x.expected_execution_revision,x.preparation_revision) and w.case_selection_revision=x.selection_revision
      and w.realised_effect_count=0 and w.net_request_revision=0 and w.net_projection_revision=0
      and i.case_kind='OVERPAYMENT' and i.tax_treatment=b.original_tax_treatment and i.tax_treatment is not null and i.tax_treatment in ('TAXABLE','NON_TAXABLE') and i.source_pay_channel='PAYE' and i.target_pay_channel='PAYE'
      and i.instruction_kind='RECOVERY' and i.direction='DEDUCTION' and (i.tax_treatment,i.payroll_stage,i.hold_purpose) in
        (('TAXABLE','GROSS_DEDUCT','GROSS_RECOVERY'),('NON_TAXABLE','NET_DEDUCT','NET_RECOVERY_CAPACITY'))
      and h.status='RELEASED' and h.allocation_pass_kind='DRAFT' and h.projection_id is null
      and h.finished_at_utc>=q.accepted_at_utc and (x.case_hold_cursor is null or h.id>x.case_hold_cursor)
      and (p_case_hold_id is null or h.id=p_case_hold_id)
      and u.status='RELEASED' and u.realisation_event_id is null and u.source_amount_ex_vat=h.source_reserved_ex_vat
      and (h.source_reserved_ex_vat,h.target_amount_ex_vat,h.target_amount_vat,h.target_amount_inc_vat)
        is not distinct from (a.allocated_source_ex_vat,a.allocated_target_ex_vat,a.allocated_target_vat,a.allocated_target_inc_vat)
      and not exists(select 1 from private.bpay_next_transfer where run_worker_id=w.id)
      and not exists(select 1 from private.bpay_next_net_projection where run_worker_id=w.id)
      and not exists(select 1 from private.bpay_next_job earlier where earlier.candidate_id=j.candidate_id
        and earlier.command_sequence<j.command_sequence and earlier.status<>'DONE'))
$function$;

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
 v_restructure jsonb;v_period_restructure jsonb;
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
        -- FC13 / BD148: an accepted resolution belonging to this exact native
        -- cut must never fall through to the old automatic full-source quote.
        -- CM13a owns the actual decision consumer. Earlier accepted cuts remain
        -- on their earlier rule and price. No new cut/protocol or money formula.
        if exists(select 1 from private.bpay_next_case_channel_component d
          join private.bpay_next_case_channel_request q on q.command_id=d.command_id
          join private.bpay_next_case_channel_outcome o on o.command_id=q.command_id and o.outcome='APPLIED'
          join private.bpay_next_command cmd on cmd.id=q.command_id and cmd.command_kind='CASE_CHANNEL_RESOLVE'
            and cmd.status='COMPLETE' and cmd.module_epoch=q.module_epoch and cmd.agency_sequence=q.command_sequence
          join private.bpay_next_job j on j.id=o.job_id and j.command_id=q.command_id
            and j.candidate_id=v_candidate and j.job_kind='CASE_CHANNEL_RESOLVE' and j.status='DONE' and j.phase='DONE'
            and j.module_epoch=q.module_epoch and j.command_sequence=q.command_sequence
          where d.case_id=v_case.id and d.case_component_id=v_component.id and d.candidate_id=v_candidate
            and o.applied_case_revision<=case when v_memory_cut then(v_case_cut->>'case_revision')::bigint else v_case.case_revision end
            and d.applied_component_revision<=case when v_memory_cut then(v_case_cut->>'case_component_revision')::bigint else v_component.component_revision end) then
          raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';
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
      and b.collection_mode='AUTOMATIC' and b.qualification_state='READY' and b.original_source_pay_channel='PAYE'
      and b.original_tax_treatment is not null and b.original_tax_treatment in ('TAXABLE','NON_TAXABLE')
      and (b.observed_revision_id,b.observed_approved_source_ex_vat,b.observed_realised_source_ex_vat)
        is not distinct from (p.applied_revision_id,p.approved_source_ex_vat,p.realised_source_ex_vat)
      and p.source_basis_channel='PAYE' and p.approved_source_ex_vat>=0 and p.realised_source_ex_vat>=0
      and (p.realised_target_ex_vat,p.realised_target_vat,p.realised_target_inc_vat)
        is not distinct from (p.realised_source_ex_vat,0::numeric,p.realised_source_ex_vat)
      and ((work.work_kind='ORDINARY' and original.source_kind='ORDINARY' and r.source_kind='ORDINARY')
        or (work.work_kind='EXPENSE' and original.source_kind='EXPENSE' and r.source_kind='EXPENSE'
          and original.financial_snapshot_id is not null and r.financial_snapshot_id is not null
          and original_line.component_kind in ('EXPENSE','MILEAGE') and original_line.tax_treatment='NON_TAXABLE')
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
        where a.revision_id=r.id and a.component_key=b.component_key and a.component_kind=original_line.component_kind
          and a.tax_treatment=original_line.tax_treatment and a.source_pay_ex_vat=p.approved_source_ex_vat
          and (work.work_kind in ('ORDINARY','EXPENSE') or a.source_component_id=original_line.source_component_id)))
        or (r.certified_zero and r.approved_source_ex_vat=0 and r.expected_line_count=0 and p.approved_source_ex_vat=0)
        or (b.segment_attribution_complete and coalesce((private.bpay_next_segment_collection_basis_v1(b.work_id,b.component_key,r.id)->>'complete')::boolean,false)))
      and (not b.segment_attribution_complete or (b.observed_segment_negative_ex_vat is not distinct from
        (private.bpay_next_segment_collection_basis_v1(b.work_id,b.component_key,r.id)->>'negative_source_ex_vat')::numeric
        and coalesce((private.bpay_next_segment_collection_basis_v1(b.work_id,b.component_key,r.id)->>'complete')::boolean,false)))
      and c.case_kind='OVERPAYMENT' and c.tax_treatment=b.original_tax_treatment and c.principal_funded=0
      and x.case_kind='OVERPAYMENT' and x.case_subtype='OVERPAYMENT' and x.tax_treatment=b.original_tax_treatment
      and x.source_pay_channel='PAYE' and x.instruction_kind='RECOVERY' and x.direction='DEDUCTION'
      and x.payroll_stage=case when b.original_tax_treatment='TAXABLE' then 'GROSS_DEDUCT' else 'NET_DEDUCT' end and x.component_key='RECOVERY' and x.component_ordinal=1
      and c.current_rule_id=rule.id and c.current_rule_revision=rule.rule_revision and rule.id=b.id
      and rule.weekly_due_source_ex_vat is null and rule.tax_treatment=b.original_tax_treatment
      and ((original.source_kind in ('ORDINARY','EXPENSE') and ((original_line.component_kind in ('WORK','ADDITIONAL') and original_line.tax_treatment='TAXABLE') or (original_line.component_kind in ('EXPENSE','MILEAGE') and original_line.tax_treatment='NON_TAXABLE')))
        or (work.work_kind='EXPENSE' and original.source_kind='EXPENSE' and r.source_kind='EXPENSE'
          and original.financial_snapshot_id is not null and r.financial_snapshot_id is not null
          and original_line.component_kind in ('EXPENSE','MILEAGE') and original_line.tax_treatment='NON_TAXABLE')
        or (work.work_kind='SOURCE' and original_line.component_kind='WORK' and original_line.tax_treatment='TAXABLE'))
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
          v_restructure:=private.bpay_next_case_restructure_origin_v1(v_rule.id);
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
          v_period_restructure:=private.bpay_next_case_restructure_origin_v1(v_period_rule.id);
          if v_restructure is not null or v_period_restructure is not null then
            if (v_period_rule.case_id,v_period_rule.candidate_id,v_period_rule.case_kind,v_period_rule.case_subtype,v_period_rule.tax_treatment,v_period_rule.case_created_at_utc)
              is distinct from(v_rule.case_id,v_rule.candidate_id,v_rule.case_kind,v_rule.case_subtype,v_rule.tax_treatment,v_rule.case_created_at_utc)
              or (v_old_amendment is not null and v_old_amendment->>'original_command_id' is distinct from v_origin.command_id::text)
              or (v_period_rule.id<>v_origin.rule_id and v_old_amendment is null and v_period_rule.original_funding_event_id is null) then
              raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';end if;
            if v_restructure is not null and v_component.instruction_kind='RECOVERY'
              and v_period_rule.rule_revision<v_rule.rule_revision and v_state.pay_week_start>=v_rule.next_due_monday
              and (v_period.realised_recovery_source_ex_vat,v_period.active_unrealised_recovery_source_ex_vat,v_period.active_payout_source_ex_vat)
                 is not distinct from(0::numeric,0::numeric,0::numeric)
              and private.bpay_next_case_restructure_period_free_v1(v_component.id,v_state.pay_week_start,v_rule.id,v_run_id) then
              v_period_before:=to_jsonb(v_period);v_period_operation:='CASE_RESTRUCTURE_OPENING';
              v_period.rule_id:=v_rule.id;v_period.period_revision:=v_period.period_revision+1;
              v_period.opening_outstanding_source_ex_vat:=v_outstanding;v_period.opening_due_source_ex_vat:=v_opening_due;
            end if;
            -- KEEP earlier/used/protected opening and older immutable accepted cut.
          elsif v_amendment is not null and v_period_rule.original_funding_event_id is null
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
            when v_restructure is not null then 'CASE_RESTRUCTURE:'||(v_restructure->>'command_id')||':PAYE'
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
     jsonb_build_object('edit_command_id',v_amendment->'command_id','before',v_period_before)
     when v_period_operation='CASE_RESTRUCTURE_OPENING' then jsonb_build_object('restructure_command_id',v_restructure->'command_id','before',v_period_before) else null end,
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
        is distinct from ('OVERPAYMENT','RECOVERY','DEDUCTION',
          case when v_instruction.tax_treatment='TAXABLE' then 'GROSS_DEDUCT' else 'NET_DEDUCT' end,
          case when v_instruction.tax_treatment='TAXABLE' then 'GROSS_RECOVERY' else 'NET_RECOVERY_CAPACITY' end)
      or v_instruction.tax_treatment is null or v_instruction.tax_treatment not in ('TAXABLE','NON_TAXABLE')
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
      if not v_item.is_selected and exists(select 1 from private.bpay_next_work_collection manual
        where manual.case_id=v_case.id and manual.case_component_id=v_component.id and manual.candidate_id=v_candidate
          and manual.collection_mode='MANUAL') then
        v_item.section:='ACTION';v_item.reason_code:='MANUAL_COLLECTION';v_item.reason_priority:=81;v_item.selectable:=false;
      end if;
      if not v_item.is_selected and v_item.reason_priority>84
        and (v_case.case_kind,v_component.case_kind,v_component.case_subtype,
          v_component.instruction_kind,v_component.direction,v_component.payroll_stage)
          is not distinct from ('OVERPAYMENT','OVERPAYMENT','OVERPAYMENT','RECOVERY','DEDUCTION',
            case when v_component.tax_treatment='TAXABLE' then 'GROSS_DEDUCT' else 'NET_DEDUCT' end)
        and exists(select 1 from private.bpay_next_work_collection origin
          join private.bpay_next_work work on work.id=origin.work_id and work.candidate_id=origin.candidate_id
          where origin.case_id=v_case.id and origin.case_component_id=v_component.id and origin.candidate_id=v_candidate
            and origin.collection_mode='AUTOMATIC' and origin.paid_basis_qualified and origin.qualification_state='READY'
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

create or replace function private.bpay_next_workbench_refresh_work_core_v1(p_work_id uuid)
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
  v_pay_hold record;v_hold_canonical boolean;
  v_query_gate jsonb;v_query_scope jsonb;v_query_loaded boolean:=false;v_query_state text:='UNAVAILABLE';v_query_exclusions jsonb;v_segment_query_state text;
  v_wait_before boolean;v_wait_after boolean;v_case_id uuid;v_withdrawn_complete boolean;v_was_dirty boolean;
  v_manual private.bpay_next_work_collection%rowtype;v_manual_observation jsonb;v_manual_position private.bpay_next_position%rowtype;v_original_line private.bpay_next_approved_line%rowtype;
  v_positive numeric;v_recovery numeric;v_overhang numeric;v_snooze jsonb;v_count bigint:=0;v_portion text;v_zero boolean;
begin
  perform private.bpay_next_workbench_require_next_v1();
  select w.candidate_id into strict v_candidate from private.bpay_next_work w where w.id=p_work_id;
  select c.financial_view_revision into strict v_financial from private.bpay_next_worker_control c where c.candidate_id=v_candidate for update;
  select w.* into strict v_work from private.bpay_next_work w where w.id=p_work_id for update;
  -- Exact current booking authority; no historical/original-row fallback.
  select t.timesheet_id,tf.id financial_id,coalesce(tf.pay_on_hold,false) pay_on_hold,
    case when tf.pay_on_hold then tf.pay_on_hold_reason end pay_on_hold_reason into v_pay_hold
    from public.timesheets t left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
    where t.booking_id=v_work.booking_id and t.is_current;
  v_hold_canonical:=v_pay_hold.timesheet_id is not null and v_pay_hold.financial_id is not null;
  -- Read stored WAIT before replacing it; the withdrawal owner already changed work.
  select exists(select 1 from private.bpay_next_workbench_item i where i.group_key=v_group
    and i.reason_code='AWAITING_REAUTHORISATION') into v_wait_before;
  v_wait_after:=private.bpay_next_work_wait_state_v1(v_work.id);
  select exists(select 1 from private.bpay_next_workbench_group g where g.group_key=v_group and g.dirty) into v_was_dirty;
  -- Reauthorisation becomes effective only after final position application.
  -- Keep its stored WAIT marker during Processing for the final boundary hook.
  if v_wait_before and v_work.approval_state='APPROVED'
    and v_work.current_revision_id is distinct from v_work.applied_revision_id then
    update private.bpay_next_workbench_revision set workbench_revision=workbench_revision+1,
      dirty_group_count=dirty_group_count+case when v_was_dirty then 0 else 1 end,
      updated_at_utc=transaction_timestamp() where id=1 returning workbench_revision into v_projection;
    update private.bpay_next_workbench_group set complete=false,dirty=true,progress_state='PROCESSING',
      projection_revision=v_projection,financial_view_revision=v_financial,
      updated_at_utc=transaction_timestamp() where group_key=v_group;
    update private.bpay_next_workbench_item set selectable=false,progress_state='PROCESSING',
      projection_revision=v_projection,financial_view_revision=v_financial,
      updated_at_utc=transaction_timestamp() where group_key=v_group;
    return;
  end if;
  v_metadata:=private.bpay_next_workbench_ensure_metadata_v1(v_candidate);
  select c.pay_method::text into v_target from public.candidates c where c.id=v_candidate;
  if v_target not in ('PAYE','UMBRELLA') then v_target:=null;end if;
  select c.client_id into v_client from public.contracts c where c.id=v_work.contract_id;
  select r.* into v_revision from private.bpay_next_work_revision r
    where r.id=case when v_work.approval_state='WITHDRAWN' then v_work.applied_revision_id else v_work.current_revision_id end
      and r.work_id=v_work.id;
  v_complete:=v_work.approval_state='APPROVED' and v_revision.id is not null and v_revision.sealed_at_utc is not null
    and v_work.applied_revision_id=v_work.current_revision_id
    and (select count(*) from private.bpay_next_approved_line a where a.revision_id=v_revision.id)=v_revision.expected_line_count;
  v_withdrawn_complete:=v_work.approval_state='WITHDRAWN' and v_revision.id is not null
    and v_revision.sealed_at_utc is not null and not exists(select 1 from private.bpay_next_position p
      where p.work_id=v_work.id and p.applied_revision_id is distinct from v_work.applied_revision_id
        and (p.approved_source_ex_vat<>0 or p.realised_source_ex_vat<>0 or p.held_source_ex_vat<>0
          or p.realised_target_ex_vat<>0 or p.realised_target_vat<>0 or p.realised_target_inc_vat<>0
          or p.held_target_ex_vat<>0 or p.held_target_vat<>0 or p.held_target_inc_vat<>0));
  select array_agg(d) into v_before from private.bpay_next_workbench_group_contribution_v1(v_group) d;
  update private.bpay_next_workbench_revision set workbench_revision=workbench_revision+1,
    dirty_group_count=dirty_group_count-case when v_was_dirty then 1 else 0 end,updated_at_utc=transaction_timestamp()
    where id=1 returning workbench_revision into v_projection;
  delete from private.bpay_next_workbench_item where group_key=v_group;
  insert into private.bpay_next_workbench_group(group_key,group_kind,work_id,candidate_id,client_id,contract_id,
    physical_timesheet_id,current_revision_id,source_pay_channel,target_pay_channel,week_ending_date,
    complete,projection_revision,financial_view_revision,metadata_version,progress_state)
  values(v_group,'WORK',v_work.id,v_candidate,v_client,v_work.contract_id,v_revision.physical_timesheet_id,v_work.current_revision_id,
    v_revision.source_pay_channel,v_target,v_work.week_ending_date,coalesce(v_complete,false),v_projection,v_financial,v_metadata.metadata_version,
    case when v_complete then 'CURRENT' when v_work.approval_state='APPROVED' then 'PROCESSING' else 'UNAVAILABLE' end)
  on conflict(group_key) do update set client_id=excluded.client_id,contract_id=excluded.contract_id,
    physical_timesheet_id=excluded.physical_timesheet_id,current_revision_id=excluded.current_revision_id,
    source_pay_channel=excluded.source_pay_channel,target_pay_channel=excluded.target_pay_channel,week_ending_date=excluded.week_ending_date,
    complete=excluded.complete,dirty=false,projection_revision=excluded.projection_revision,financial_view_revision=excluded.financial_view_revision,
    metadata_version=excluded.metadata_version,progress_state=excluded.progress_state,error_code=null,updated_at_utc=transaction_timestamp();
  v_sunday:=v_today+(7-extract(isodow from v_today)::integer);
  select last_sequence into strict v_sequence from private.bpay_next_command_clock where id=1;
  if v_work.approval_state='WITHDRAWN' then
    -- Native retained positions supply A/C/H directly. No zero revision, job,
    -- debt, current residual offer, segment repricing or Case mutation here.
    for v_member in select p.component_key,p.approved_source_ex_vat approved_amount,
      p.realised_source_ex_vat realised_amount,p.held_source_ex_vat held_amount,p.source_basis_channel,
      a.id approved_line_id,a.component_kind,a.tax_treatment,a.work_date,a.approved_quantity,a.unit_label
      from private.bpay_next_position p left join private.bpay_next_approved_line a
        on a.revision_id=v_revision.id and a.component_key=p.component_key
      where p.work_id=v_work.id and v_withdrawn_complete and (p.applied_revision_id=v_work.applied_revision_id
        or p.approved_source_ex_vat<>0 or p.realised_source_ex_vat<>0 or p.held_source_ex_vat<>0
        or p.realised_target_ex_vat<>0 or p.realised_target_vat<>0 or p.realised_target_inc_vat<>0
        or p.held_target_ex_vat<>0 or p.held_target_vat<>0 or p.held_target_inc_vat<>0) order by p.component_key loop
      v_count:=v_count+1;v_item:=null;v_item.id:=gen_random_uuid();v_item.group_key:=v_group;v_item.candidate_id:=v_candidate;
      v_item.work_id:=v_work.id;v_item.revision_id:=v_revision.id;v_item.approved_line_id:=v_member.approved_line_id;
      v_item.component_key:=v_member.component_key;v_item.member_kind:='WORK_COMPONENT';v_item.member_key:=v_member.component_key;
      v_item.portion_kind:='WITHDRAWN';v_item.direction:='INFORMATIONAL';v_item.payroll_stage:='GROSS_ADD';
      v_item.client_id:=v_client;v_item.contract_id:=v_work.contract_id;v_item.physical_timesheet_id:=v_revision.physical_timesheet_id;
      v_item.week_ending_date:=v_work.week_ending_date;v_item.work_date:=v_member.work_date;
      v_item.candidate_display_name:=v_metadata.candidate_sort_name;v_item.candidate_reference:=v_metadata.tms_ref;
      v_item.client_display_name:=v_revision.client_display_name;v_item.timesheet_reference:=v_revision.timesheet_reference;
      v_item.component_kind:=v_member.component_kind;v_item.tax_treatment:=v_member.tax_treatment;
      v_item.approved_quantity:=v_member.approved_quantity;v_item.unit_label:=v_member.unit_label;
      v_item.source_pay_channel:=v_member.source_basis_channel;v_item.target_pay_channel:=v_target;
      v_item.amount_known:=true;v_item.nominal_source_ex_vat:=v_member.approved_amount;v_item.approved_source_ex_vat:=v_member.approved_amount;
      v_item.realised_source_ex_vat:=v_member.realised_amount;v_item.active_reserved_source_ex_vat:=v_member.held_amount;
      v_item.residual_source_ex_vat:=v_member.approved_amount-v_member.realised_amount-v_member.held_amount;
      v_item.gross_due_source_ex_vat:=0;v_item.included_source_ex_vat:=0;v_item.excluded_source_ex_vat:=0;
      v_item.recovery_source_ex_vat:=0;v_item.held_overhang_source_ex_vat:=0;v_item.written_off_source_ex_vat:=0;
      v_item.section:=case when v_wait_after then 'ACTION' else 'BLOCKED' end;
      v_item.reason_code:=case when v_wait_after then 'AWAITING_REAUTHORISATION' else 'APPROVAL_WITHDRAWN' end;
      v_item.reason_priority:=case when v_wait_after then 70 else 20 end;v_item.selectable:=false;
      v_item.is_selected:=false;v_item.selected_amount_known:=false;v_item.query_qualified:=false;v_item.query_state:='NOT_APPLICABLE';
      v_item.financial_view_revision:=v_financial;v_item.projection_revision:=v_projection;v_item.metadata_version:=v_metadata.metadata_version;
      v_item.progress_state:='CURRENT';v_item.updated_at_utc:=transaction_timestamp();
      insert into private.bpay_next_workbench_item select (v_item).*;
    end loop;
    update private.bpay_next_workbench_group set complete=v_count>0,certified_zero=false,
      progress_state=case when v_withdrawn_complete then 'CURRENT' when v_work.applied_revision_id is not null then 'PROCESSING' else 'UNAVAILABLE' end
      where group_key=v_group;
  else
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
    v_item.pay_hold:=coalesce(v_pay_hold.pay_on_hold,false);v_item.pay_hold_reason:=v_pay_hold.pay_on_hold_reason;
    v_item.selectable:=v_section='READY' and v_item.amount_known;v_base:=v_item;
    if not v_item.amount_known or v_positive>0 or (v_member.approved_amount=0 and v_member.realised_amount=0
      and v_member.held_amount=0 and v_revision.certified_zero and v_member.approved_line_id is not null) then
      v_item.portion_kind:=case when not v_item.amount_known then 'EVIDENCE' when v_positive=0 then 'ZERO' else 'AVAILABLE' end;
      -- WB40: only a new positive offer is blocked. The saved v_base keeps
      -- native H/R and accepted Draft selection untouched (owner E1/E3).
      if not v_item.is_selected and not v_hold_canonical and v_item.reason_priority>15 then
        v_item.reason_code:='DATA_UNAVAILABLE';v_item.reason_priority:=15;v_item.section:='BLOCKED';
      elsif not v_item.is_selected and v_item.pay_hold and v_item.reason_priority>40 then
        v_item.reason_code:='PAY_ON_HOLD';v_item.reason_priority:=40;v_item.section:='BLOCKED';
      end if;
      v_item.selectable:=v_item.section='READY' and v_item.amount_known;
      -- WB-07: query classification belongs only to this new positive portion.
      -- The original pre-query v_base/v_priority remain unchanged for H/R.
      if v_item.query_qualified and v_item.portion_kind='AVAILABLE' then
        if not v_query_loaded then
          v_query_gate:=null;v_query_state:='UNAVAILABLE';
          begin
            v_query_gate:=private.weekly_source_pay_query_gate_v1(v_revision.physical_timesheet_id);
          exception when others then
            v_query_gate:=null;
          end;
          v_query_loaded:=true;
          -- Guard object types separately: malformed scalar replies cannot
          -- reach the object-minus-keys operators or escape the unavailable path.
          if jsonb_typeof(v_query_gate)='object' then
            if (v_query_gate ?& array['ok','code','scope','blocked','query_state_sha256']
                and v_query_gate-array['ok','code','scope','blocked','query_state_sha256']='{}'::jsonb
                and v_query_gate->'ok'='true'::jsonb and v_query_gate->>'code'='OK'
                and jsonb_typeof(v_query_gate->'code')='string'
                and jsonb_typeof(v_query_gate->'blocked')='boolean'
                and jsonb_typeof(v_query_gate->'query_state_sha256')='string'
                and v_query_gate->>'query_state_sha256' ~ '^[0-9a-f]{64}$') is true then
              v_query_scope:=v_query_gate->'scope';
              if jsonb_typeof(v_query_scope)='object' then
                if (v_query_scope ?& array['root_timesheet_id','family_booking_id','target_family_id',
                    'candidate_id','client_id','contract_id','week_ending_date','root_version','family_bound_version']
                    and v_query_scope-array['root_timesheet_id','family_booking_id','target_family_id',
                      'candidate_id','client_id','contract_id','week_ending_date','root_version','family_bound_version']='{}'::jsonb
                    and v_query_scope->'root_timesheet_id'=to_jsonb(v_revision.physical_timesheet_id::text)
                    and v_query_scope->'family_booking_id'=to_jsonb(v_work.booking_id)
                    and v_query_scope->'candidate_id'=to_jsonb(v_work.candidate_id::text)
                    and v_query_scope->'client_id'=to_jsonb(v_client::text)
                    and v_query_scope->'contract_id'=to_jsonb(v_work.contract_id::text)
                    and v_query_scope->'week_ending_date'=to_jsonb(v_work.week_ending_date::text)
                    and v_query_scope->'root_version'=to_jsonb(v_revision.physical_timesheet_version::text)
                    and ((v_query_scope->'target_family_id'='null'::jsonb
                        and v_query_scope->'family_bound_version'='null'::jsonb)
                      or (jsonb_typeof(v_query_scope->'target_family_id')='string'
                        and v_query_scope->>'target_family_id' ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                        and jsonb_typeof(v_query_scope->'family_bound_version')='string'
                        and v_query_scope->>'family_bound_version' ~ '^[1-9][0-9]{0,18}$'
                        and (char_length(v_query_scope->>'family_bound_version')<19
                          or (v_query_scope->>'family_bound_version') collate "C"<='9223372036854775807' collate "C")))) is true then
                  v_query_state:=case when v_query_gate->'blocked'='true'::jsonb then 'OPEN' else 'CLEAR' end;
                  v_query_exclusions:=private.bpay_next_source_query_segment_exclusions_v1(v_revision.id,v_query_gate);
                end if;
              end if;
            end if;
          end if;
        end if;
        v_segment_query_state:=v_query_state;
        if v_query_state='OPEN' and v_query_exclusions is not null and v_member.segment_key is not null then
          v_segment_query_state:=case when exists(select 1 from jsonb_array_elements(v_query_exclusions) reason(value)
            where reason.value->>'segment_key'=v_member.segment_key) then 'OPEN' else 'CLEAR' end;
        end if;
        v_item.query_state:=v_segment_query_state;
        if v_segment_query_state='UNAVAILABLE' and v_item.reason_priority>60 then
          v_item.reason_code:='PAY_QUERY_UNAVAILABLE';v_item.reason_priority:=60;v_item.section:='BLOCKED';
        elsif v_segment_query_state='OPEN' and v_item.reason_priority>61 then
          v_item.reason_code:='PAY_QUERY_OPEN';v_item.reason_priority:=61;v_item.section:='ACTION';
        end if;
        v_item.selectable:=v_item.section='READY' and v_item.amount_known;
      end if;
      insert into private.bpay_next_workbench_item select (v_item).*;
    elsif v_recovery>0 then
      v_item.portion_kind:='RECOVERY';v_item.direction:='INFORMATIONAL';v_item.selectable:=false;
      select b.* into v_manual from private.bpay_next_work_collection b where b.work_id=v_work.id
        and b.component_key=v_member.component_key and b.collection_mode='MANUAL'
        and b.observed_revision_id=v_revision.id;
      if v_manual.id is not null then
        v_item.section:='ACTION';v_item.reason_priority:=81;
        -- This informational liability never borrows a frozen accepted offer.
        v_item.is_selected:=false;v_item.selected_amount_known:=false;v_item.selected_source_ex_vat:=null;
        v_item.accepted_run_id:=null;v_item.accepted_command_id:=null;v_item.accepted_module_epoch:=null;
        v_item.reason_code:=case when v_manual.case_id is not null then 'MANUAL_COLLECTION_REFERENCE'
          when v_manual.manual_outstanding_source_ex_vat is null then 'MANUAL_COLLECTION_UNAVAILABLE' else 'MANUAL_COLLECTION' end;
        v_item.nominal_source_ex_vat:=case when v_manual.case_id is not null then 0 else v_recovery end;
        if v_item.component_kind is null then
          select line.* into strict v_original_line from private.bpay_next_approved_line line where line.id=v_manual.original_approved_line_id;
          v_item.component_kind:=v_original_line.component_kind;v_item.tax_treatment:=v_original_line.tax_treatment;
        end if;
      elsif v_priority>83 then v_item.reason_code:='RECOVERY_MANAGED';v_item.reason_priority:=83;v_item.section:='ACTION';end if;
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
  end if;
  -- A known retained manual liability is independent of unavailable positive
  -- Source classification. Keep that Evidence; never certify a new offer.
  update private.bpay_next_workbench_item i set reason_code='MANUAL_COLLECTION_UNAVAILABLE'
    from private.bpay_next_work_collection b where i.group_key=v_group and i.work_id=b.work_id
      and i.component_key=b.component_key and i.portion_kind='EVIDENCE' and not i.amount_known
      and not i.is_selected and b.collection_mode='MANUAL' and b.case_id is null
      and b.observed_revision_id=v_revision.id and b.manual_outstanding_source_ex_vat is null;
  if v_work.approval_state='APPROVED' and v_work.current_revision_id=v_work.applied_revision_id then
    for v_manual in select b.* from private.bpay_next_work_collection b where b.work_id=v_work.id
      and b.collection_mode='MANUAL' and b.case_id is null and b.observed_revision_id=v_revision.id
      and b.manual_outstanding_source_ex_vat>0 and not exists(select 1 from private.bpay_next_workbench_item i
        where i.group_key=v_group and i.component_key=b.component_key and i.amount_known) order by b.component_key loop
      v_manual_observation:=private.bpay_next_work_collection_observation_v1(v_work.id,v_manual.component_key,v_revision.id);
      if not coalesce((v_manual_observation->>'complete')::boolean,false) then continue;end if;
      select pos.* into strict v_manual_position from private.bpay_next_position pos where pos.work_id=v_work.id and pos.component_key=v_manual.component_key;
      select line.* into strict v_original_line from private.bpay_next_approved_line line where line.id=v_manual.original_approved_line_id;
      v_count:=v_count+1;v_item:=null;v_item.id:=gen_random_uuid();v_item.group_key:=v_group;v_item.candidate_id:=v_candidate;
      v_item.work_id:=v_work.id;v_item.revision_id:=v_revision.id;v_item.component_key:=v_manual.component_key;
      v_item.member_kind:='WORK_COMPONENT';v_item.member_key:=v_manual.component_key;v_item.portion_kind:='RECOVERY';
      v_item.direction:='INFORMATIONAL';v_item.payroll_stage:=case when v_original_line.tax_treatment='NON_TAXABLE' then 'NET_DEDUCT' else 'GROSS_DEDUCT' end;
      v_item.client_id:=v_client;v_item.contract_id:=v_work.contract_id;v_item.physical_timesheet_id:=v_revision.physical_timesheet_id;
      v_item.week_ending_date:=v_work.week_ending_date;v_item.work_date:=v_original_line.work_date;
      v_item.candidate_display_name:=v_metadata.candidate_sort_name;v_item.candidate_reference:=v_metadata.tms_ref;
      v_item.client_display_name:=v_revision.client_display_name;v_item.timesheet_reference:=v_revision.timesheet_reference;
      v_item.component_kind:=v_original_line.component_kind;v_item.tax_treatment:=v_original_line.tax_treatment;
      v_item.source_pay_channel:=v_manual.original_source_pay_channel;v_item.target_pay_channel:=v_target;
      v_item.amount_known:=true;v_item.nominal_source_ex_vat:=v_manual.manual_outstanding_source_ex_vat;
      v_item.approved_source_ex_vat:=v_manual_position.approved_source_ex_vat;v_item.realised_source_ex_vat:=v_manual_position.realised_source_ex_vat;
      v_item.active_reserved_source_ex_vat:=v_manual_position.held_source_ex_vat;
      v_item.residual_source_ex_vat:=v_manual_position.approved_source_ex_vat-v_manual_position.realised_source_ex_vat-v_manual_position.held_source_ex_vat;
      v_item.gross_due_source_ex_vat:=(v_manual_observation->>'positive_source_ex_vat')::numeric;
      v_item.included_source_ex_vat:=0;v_item.excluded_source_ex_vat:=v_item.gross_due_source_ex_vat;
      v_item.recovery_source_ex_vat:=(v_manual_observation->>'negative_source_ex_vat')::numeric;
      v_item.held_overhang_source_ex_vat:=(v_manual_observation->>'held_overhang_source_ex_vat')::numeric;v_item.written_off_source_ex_vat:=0;
      v_item.section:='ACTION';v_item.reason_code:='MANUAL_COLLECTION';v_item.reason_priority:=81;v_item.selectable:=false;
      v_item.is_selected:=false;v_item.selected_amount_known:=false;v_item.query_qualified:=false;v_item.query_state:='NOT_APPLICABLE';
      v_item.financial_view_revision:=v_financial;v_item.projection_revision:=v_projection;v_item.metadata_version:=v_metadata.metadata_version;
      v_item.progress_state:='CURRENT';v_item.updated_at_utc:=transaction_timestamp();
      if v_item.included_source_ex_vat+v_item.excluded_source_ex_vat-v_item.recovery_source_ex_vat-v_item.held_overhang_source_ex_vat
        is distinct from v_item.residual_source_ex_vat then raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_MANUAL_DISPLAY_NOT_CONSERVED';end if;
      insert into private.bpay_next_workbench_item select (v_item).*;
    end loop;
  end if;
  if v_count=0 then
    v_item:=null;v_item.id:=gen_random_uuid();v_item.group_key:=v_group;v_item.candidate_id:=v_candidate;v_item.work_id:=v_work.id;
    v_item.revision_id:=v_revision.id;v_item.member_kind:='GROUP_EVIDENCE';v_item.member_key:=v_work.id::text;v_item.component_key:='';v_item.portion_kind:='EVIDENCE';
    v_item.client_id:=v_client;v_item.contract_id:=v_work.contract_id;v_item.physical_timesheet_id:=v_revision.physical_timesheet_id;
    v_item.week_ending_date:=v_work.week_ending_date;v_item.candidate_display_name:=v_metadata.candidate_sort_name;v_item.candidate_reference:=v_metadata.tms_ref;
    v_item.direction:='INFORMATIONAL';v_item.amount_known:=false;v_item.selected_amount_known:=false;v_item.is_selected:=false;v_item.selectable:=false;
    v_item.section:='BLOCKED';v_item.reason_code:=case when v_work.approval_state<>'APPROVED' then 'MISSING_APPROVAL' else 'DATA_UNAVAILABLE' end;
    if v_work.approval_state='WITHDRAWN' then
      v_item.reason_code:=case when v_wait_after then 'AWAITING_REAUTHORISATION' else 'APPROVAL_WITHDRAWN' end;
      v_item.section:=case when v_wait_after then 'ACTION' else 'BLOCKED' end;
    end if;
    v_item.reason_priority:=case when v_work.approval_state='WITHDRAWN' then case when v_wait_after then 70 else 20 end
      when v_work.approval_state<>'APPROVED' then 21 else 10 end;v_item.query_state:='NOT_APPLICABLE';v_item.query_qualified:=false;
    v_item.financial_view_revision:=v_financial;v_item.projection_revision:=v_projection;v_item.metadata_version:=v_metadata.metadata_version;
    v_item.progress_state:=case when v_work.approval_state='WITHDRAWN' and v_work.applied_revision_id is not null
      and not v_withdrawn_complete then 'PROCESSING' else 'UNAVAILABLE' end;
    v_item.updated_at_utc:=transaction_timestamp();v_item.target_pay_channel:=v_target;
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
  perform private.bpay_next_workbench_refresh_group_display_v1(v_group);
end
$function$;

create or replace function private.bpay_next_workbench_refresh_group_display_v1(p_group_key text)
returns void language plpgsql security definer set search_path=pg_catalog,private,public
as $function$
begin
  perform private.bpay_next_workbench_require_next_v1();
  if p_group_key is null then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_GROUP_INVALID';end if;
  delete from private.bpay_next_workbench_group_display where parent_group_key=p_group_key;
-- WB15 own-list display projection. Values never enter a financial command.
insert into private.bpay_next_workbench_group_display(
  presentation_key,presentation_kind,presentation_id,parent_group_key,candidate_id,case_component_id,
  section,client_id,week_ending_date,client_display_name,payment_label,timesheet_reference,
  target_pay_channel,has_deduction,selectable,reason_code,reason_priority,group_amount,ready_net_add_amount,
  gross_deduction_balance,net_deduction_balance,waiting_amount,waiting_net_add_amount,
  accepted_run_id,accepted_command_id,accepted_module_epoch,snooze_until_date,eligible_from_date,pay_hold_reason
)
with members as (
  select g.group_key parent_group_key,g.candidate_id,g.client_id,g.week_ending_date,g.target_pay_channel,g.certified_zero,
    case when g.group_kind='WORK' then 'WORK' when fc.case_kind='OVERPAYMENT' or i.case_component_id is null then 'CASE' else 'ROW' end presentation_kind,
    case when g.group_kind='WORK' then g.work_id when fc.case_kind='OVERPAYMENT' or i.case_component_id is null then g.case_id else i.case_component_id end presentation_id,
    case when g.group_kind='CASE' and fc.case_kind<>'OVERPAYMENT' then i.case_component_id end case_component_id,
    case when g.group_kind='WORK' then i.client_display_name else cl.name end client_display_name,
    case when g.group_kind='WORK' then 'Timesheet' when fc.case_kind='OVERPAYMENT' then 'Overpayment recovery'
      when cc.case_subtype in ('LOAN','PAYMENT_ADVANCE') then case when cc.direction='DEDUCTION' then 'Payment Advance repayment' else 'Payment Advance' end
      when cc.case_subtype='MANUAL_CREDIT' then 'Manual Credit' when cc.case_subtype='UNDERPAYMENT' then 'Underpayment'
      when cc.case_subtype='MANUAL_DEBT' then 'Manual Debt' else 'Payment needs attention' end payment_label,
    i.id,i.section item_section,i.direction,i.payroll_stage,i.portion_kind,i.member_kind,i.reason_code,i.reason_priority,
    i.is_selected,i.selectable,i.amount_known,i.selected_amount_known,i.timesheet_reference,i.included_source_ex_vat,
    i.gross_due_source_ex_vat,i.nominal_source_ex_vat,i.deduction_balance_source_ex_vat,
    i.active_reserved_source_ex_vat,i.selected_source_ex_vat,i.accepted_run_id,i.accepted_command_id,i.accepted_module_epoch,
    i.snooze_until_date,i.eligible_from_date,i.pay_hold_reason,
    (i.reason_code='MANUAL_COLLECTION_UNAVAILABLE' or not i.amount_known and not(g.certified_zero and i.member_kind='GROUP_EVIDENCE')
      or i.is_selected and not i.selected_amount_known) amount_unavailable,
    case when i.is_selected then i.selected_source_ex_vat
      when i.portion_kind='RESERVED' then i.active_reserved_source_ex_vat
      when i.direction='PAYMENT' and i.section='READY' then i.included_source_ex_vat
      when i.direction='PAYMENT' and i.section='WAITING' and i.payroll_stage='GROSS_ADD' then i.gross_due_source_ex_vat
      else i.nominal_source_ex_vat end display_value,
    case when i.is_selected and i.accepted_run_id is not null and i.accepted_command_id is not null and i.accepted_module_epoch is not null
      and exists(select 1 from private.bpay_next_run_command rc join private.bpay_next_command cmd on cmd.id=rc.command_id
        join private.bpay_next_pay_run pr on pr.id=rc.run_id where rc.run_id=i.accepted_run_id and rc.command_id=i.accepted_command_id
          and cmd.command_kind='PREPARE' and cmd.module_epoch=i.accepted_module_epoch and pr.selection_state='SEALED'
          and pr.status in ('PREPARING','REVIEW','DRAFT','EXECUTING'))
      then pg_catalog.jsonb_build_array(i.accepted_run_id::text,i.accepted_command_id::text,i.accepted_module_epoch::text) end accepted_pair
  from private.bpay_next_workbench_group g join private.bpay_next_workbench_item i on i.group_key=g.group_key
  left join private.bpay_next_finance_case fc on fc.id=g.case_id
  left join private.bpay_next_case_component cc on cc.id=i.case_component_id and cc.case_id=g.case_id
  left join public.clients cl on cl.id=g.client_id
  where g.group_key=p_group_key
), scoped as (
  select m.*,sec.section from members m cross join lateral(values(m.item_section),('ALL'::text)) sec(section)
), reduced as (
  select presentation_kind,presentation_id,parent_group_key,candidate_id,case_component_id,section,client_id,week_ending_date,target_pay_channel,
    min(client_display_name::text) client_display_name,min(payment_label::text) payment_label,min(timesheet_reference::text) timesheet_reference,
    bool_or(direction='DEDUCTION') has_deduction,bool_or(selectable) selectable,
    (array_agg(reason_code order by reason_priority,id))[1] reason_code,min(reason_priority::smallint) reason_priority,
    bool_or(amount_unavailable) amount_unavailable,
    coalesce(sum(case when direction='DEDUCTION' then -display_value when direction='PAYMENT' then display_value when reason_code='MANUAL_COLLECTION' and direction='INFORMATIONAL' then display_value else 0 end),0) group_amount,
    coalesce(sum(included_source_ex_vat) filter(where item_section='READY' and direction='PAYMENT' and payroll_stage='NET_ADD'),0) ready_net_add_amount,
    coalesce(sum(deduction_balance_source_ex_vat) filter(where direction='DEDUCTION' and payroll_stage='GROSS_DEDUCT'),0) gross_deduction_balance,
    coalesce(sum(deduction_balance_source_ex_vat) filter(where direction='DEDUCTION' and payroll_stage='NET_DEDUCT'),0) net_deduction_balance,
    coalesce(sum(gross_due_source_ex_vat) filter(where item_section='WAITING' and direction='PAYMENT' and payroll_stage='GROSS_ADD' and portion_kind<>'RESERVED'),0) waiting_amount,
    coalesce(sum(nominal_source_ex_vat) filter(where item_section='WAITING' and direction='PAYMENT' and payroll_stage='NET_ADD' and portion_kind<>'RESERVED'),0) waiting_net_add_amount,
    array_agg(distinct accepted_pair) filter(where accepted_pair is not null) accepted_pairs,
    min(snooze_until_date::date) snooze_until_date,min(eligible_from_date::date) eligible_from_date,min(pay_hold_reason::text) pay_hold_reason
  from scoped group by presentation_kind,presentation_id,parent_group_key,candidate_id,case_component_id,section,client_id,week_ending_date,target_pay_channel
)
select presentation_kind||':'||presentation_id::text,presentation_kind,presentation_id,parent_group_key,candidate_id,case_component_id,
  section,client_id,week_ending_date,client_display_name,payment_label,timesheet_reference,
  target_pay_channel,has_deduction,selectable,reason_code,reason_priority,
  case when not amount_unavailable then group_amount end,case when not amount_unavailable then ready_net_add_amount end,
  case when not amount_unavailable then gross_deduction_balance end,case when not amount_unavailable then net_deduction_balance end,
  case when not amount_unavailable then waiting_amount end,case when not amount_unavailable then waiting_net_add_amount end,
  case when cardinality(accepted_pairs)=1 then (accepted_pairs[1]->>0)::uuid end,
  case when cardinality(accepted_pairs)=1 then (accepted_pairs[1]->>1)::uuid end,
  case when cardinality(accepted_pairs)=1 then (accepted_pairs[1]->>2)::bigint end,
  snooze_until_date,eligible_from_date,pay_hold_reason
from reduced;

end
$function$;

alter function private.bpay_next_work_collection_origin_v1(uuid,uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_work_collection_origin_v1(uuid,uuid,uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_reconcile_work_collection_v1(uuid,uuid,text,uuid) owner to postgres;
revoke all on function private.bpay_next_reconcile_work_collection_v1(uuid,uuid,text,uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_work_collection_guard_v1() owner to postgres;
revoke all on function private.bpay_next_work_collection_guard_v1() from public,anon,authenticated,service_role;
alter function private.bpay_next_expired_work_collection_scope_v1(uuid,uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_expired_work_collection_scope_v1(uuid,uuid,uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_worker_released_collection_scope_v1(uuid,uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_worker_released_collection_scope_v1(uuid,uuid,uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_discarded_work_collection_scope_v1(uuid,uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_discarded_work_collection_scope_v1(uuid,uuid,uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_case_allocation_plan_v1(jsonb) owner to postgres;
revoke all on function private.bpay_next_case_allocation_plan_v1(jsonb) from public,anon,authenticated,service_role;
alter function private.bpay_next_workbench_refresh_case_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_workbench_refresh_case_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_workbench_refresh_work_core_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_workbench_refresh_work_core_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_workbench_refresh_group_display_v1(text) owner to postgres;
revoke all on function private.bpay_next_workbench_refresh_group_display_v1(text) from public,anon,authenticated,service_role;
alter function private.bpay_next_work_collection_observation_v1(uuid,text,uuid) owner to postgres;
revoke all on function private.bpay_next_work_collection_observation_v1(uuid,text,uuid) from public,anon,authenticated,service_role;

commit;
