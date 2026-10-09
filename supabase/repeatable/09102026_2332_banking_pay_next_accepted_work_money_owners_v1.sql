-- F-25 exact accepted WORK terms, current real money owners and managed capture.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_accepted_work_collection_origin_v1(
  p_effective_work_cut_id uuid,p_component_key text
) returns private.bpay_next_work_collection
language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_basis private.bpay_next_work_collection%rowtype;
begin
  select b.* into v_basis from private.bpay_next_effective_work_cut cut
    join private.bpay_next_accepted_input input on input.id=cut.accepted_input_id
    join private.bpay_next_accepted_work_member member on member.accepted_work_input_id=cut.accepted_work_input_id
      and member.component_key=p_component_key and member.work_id=cut.work_id
    join private.bpay_next_work_collection b on b.work_id=cut.work_id and b.component_key=member.component_key
      and b.candidate_id=cut.candidate_id
    join private.bpay_next_work work on work.id=b.work_id and work.candidate_id=b.candidate_id and work.work_kind='ORDINARY'
    join private.bpay_next_position p on p.work_id=b.work_id and p.component_key=b.component_key
    join private.bpay_next_work_revision r on r.id=b.original_revision_id and r.work_id=b.work_id and r.source_kind='ORDINARY'
    join private.bpay_next_approved_line approved on approved.id=b.original_approved_line_id
      and approved.revision_id=r.id and approved.component_key=b.component_key
    join private.bpay_next_finance_case c on c.id=b.case_id and c.candidate_id=b.candidate_id
    join private.bpay_next_case_component x on x.id=b.case_component_id and x.case_id=c.id and x.candidate_id=c.candidate_id
    join private.bpay_next_financial_effect effect on effect.id=b.original_effect_id
    join private.bpay_next_run_line line on line.id=b.original_run_line_id
    join private.bpay_next_command command on command.id=effect.operation_id
      and command.command_kind in ('CSV_SETTLEMENT','INTERNAL_SETTLEMENT')
    where cut.id=p_effective_work_cut_id and member.component_key=p_component_key
      and command.agency_sequence<input.accepted_sequence and command.status='COMPLETE'
      and b.paid_basis_qualified
      and b.id=b.case_id and b.id=b.case_component_id
      and b.original_source_pay_channel='PAYE' and b.original_tax_treatment='TAXABLE'
      and approved.component_kind='WORK' and approved.tax_treatment='TAXABLE'
      and r.source_pay_channel='PAYE' and r.sealed_at_utc is not null
      and effect.effect_kind='PAYROLL_SETTLED' and effect.source_disposition_ex_vat>0
      and (effect.work_id,effect.component_key,effect.candidate_id,effect.original_transfer_id)
        is not distinct from (b.work_id,b.component_key,b.candidate_id,b.original_transfer_id)
      and (line.work_id,line.component_key,line.captured_revision_id,line.approved_line_id,line.source_pay_channel,line.valuation_policy_id)
        is not distinct from (b.work_id,b.component_key,b.original_revision_id,b.original_approved_line_id,b.original_source_pay_channel,b.original_valuation_policy_id)
      and line.target_pay_channel='PAYE'
      and (effect.source_disposition_ex_vat,effect.target_amount_ex_vat,effect.target_amount_vat,effect.target_amount_inc_vat)
        is not distinct from (line.source_consumed_ex_vat,line.frozen_ex_vat,line.frozen_vat,line.frozen_inc_vat)
      and exists(select 1 from private.bpay_next_hold h where h.id=effect.operation_item_id
        and h.run_line_id=line.id and h.status='REALISED' and h.source_reserved_ex_vat=effect.source_disposition_ex_vat
        and h.source_reserved_ex_vat=line.source_consumed_ex_vat)
      and exists(select 1 from private.bpay_next_transfer_member m where m.transfer_id=b.original_transfer_id
        and m.run_line_id=line.id and m.subject_kind='WORK' and m.signed_cash_contribution=line.frozen_inc_vat)
      and c.case_kind='OVERPAYMENT' and c.tax_treatment='TAXABLE' and c.principal_funded=0
      and (x.case_kind,x.case_subtype,x.tax_treatment,x.source_pay_channel,x.instruction_kind,x.direction,x.payroll_stage,x.component_key,x.component_ordinal)
        is not distinct from ('OVERPAYMENT','OVERPAYMENT','TAXABLE','PAYE','RECOVERY','DEDUCTION','GROSS_DEDUCT','RECOVERY',1::bigint)
      and p.realised_source_ex_vat=b.observed_realised_source_ex_vat
      and p.source_basis_channel='PAYE' and p.realised_source_ex_vat>=0
      and (p.realised_target_ex_vat,p.realised_target_vat,p.realised_target_inc_vat)
        is not distinct from (p.realised_source_ex_vat,0::numeric,p.realised_source_ex_vat)
      and (c.principal_approved,c.principal_recovered,c.principal_written_off,c.active_recovery_hold_amount)
        is not distinct from (x.approved_source_ex_vat,x.recovered_source_ex_vat,x.written_off_source_ex_vat,x.active_recovery_source_ex_vat);
  return v_basis;
end
$function$;

create or replace function private.bpay_next_capture_position_page_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,
  p_expected_work_cursor uuid,p_expected_component_cursor text,
  p_limit integer default 100
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private
as $function$
declare
  v_run_id uuid;
  v_run private.bpay_next_pay_run%rowtype;
  v_job private.bpay_next_job%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_module_epoch bigint;
  v_row record;
  v_member record;
  v_seen integer:=0;
  v_selected_seen integer:=0;
  v_resolution_count integer:=0;
  v_managed_seen integer:=0;
  v_needs_resolution boolean;
  v_collection private.bpay_next_work_collection%rowtype;
  v_work_cursor uuid;
  v_component_cursor text;
  v_issue text;
  v_issue_work_id uuid;
  v_query_pin private.bpay_next_run_work_query_pin_v2%rowtype;
  v_accepted private.bpay_next_accepted_input%rowtype;
  v_binding private.bpay_next_source_component_binding_version%rowtype;v_book record;
  v_seed_effect uuid;v_seed_hold uuid;v_seed_holds uuid[];v_seed record;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null
     or p_limit not between 1 and 100
     or (p_expected_work_cursor is null and p_expected_component_cursor is not null) then
    raise exception using errcode='22023',
      message='BPAY_NEXT_CAPTURE_POSITION_INPUT_INVALID';
  end if;
  select rc.run_id into strict v_run_id
    from private.bpay_next_job j
    join private.bpay_next_run_command rc on rc.command_id=j.command_id
    where j.id=p_job_id and j.job_kind='PREPARE';
  select m.owner_epoch into v_module_epoch
    from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_module_epoch is null then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select * into strict v_run from private.bpay_next_pay_run
    where id=v_run_id for share;
  select candidate_id into strict v_job.candidate_id
    from private.bpay_next_job where id=p_job_id;
  perform 1 from private.bpay_next_worker_control
    where candidate_id=v_job.candidate_id for update;
  select * into strict v_job from private.bpay_next_job
    where id=p_job_id for update;
  if v_run.status<>'PREPARING' or v_run.selection_state<>'SEALED'
     or v_job.job_kind<>'PREPARE' or v_job.status<>'LEASED'
     or v_job.phase not in ('RESERVE','ALLOCATE')
     or v_job.module_epoch<>v_module_epoch
     or v_job.lease_nonce<>p_lease_nonce
     or v_job.owner_epoch<>p_owner_epoch
     or v_job.lease_until_utc<=pg_catalog.clock_timestamp() then
    raise exception using errcode='55000',
      message='BPAY_NEXT_CAPTURE_POSITION_LEASE_STALE';
  end if;
  if v_job.phase='ALLOCATE' then
    return pg_catalog.jsonb_build_object('done',true,'phase','ALLOCATE',
      'replay',true,'rows_visited',0);
  end if;
  if v_job.position_work_cursor is distinct from p_expected_work_cursor
     or v_job.position_component_cursor is distinct from p_expected_component_cursor then
    return pg_catalog.jsonb_build_object('done',false,'phase','RESERVE',
      'replay',true,'rows_visited',0,
      'work_cursor',v_job.position_work_cursor,
      'component_cursor',v_job.position_component_cursor);
  end if;
  select * into strict v_worker from private.bpay_next_run_worker
    where run_id=v_run_id and candidate_id=v_job.candidate_id for update;
  if v_worker.status<>'PREPARING' or v_worker.review_issue_code is not null then
    raise exception using errcode='55000',
      message='BPAY_NEXT_CAPTURE_POSITION_WORKER_NOT_PREPARING';
  end if;
  select a.* into strict v_accepted from private.bpay_next_accepted_input a
    where a.run_id=v_run_id and a.prepare_command_id=v_job.command_id and a.candidate_id=v_worker.candidate_id
      and a.module_epoch=v_job.module_epoch and a.accepted_sequence=v_job.command_sequence;
  if exists(select 1 from private.bpay_next_accepted_work_input a
    left join private.bpay_next_effective_work_cut cut on cut.accepted_work_input_id=a.id and cut.prepare_job_id=p_job_id
    left join private.bpay_next_run_work rw on rw.run_worker_id=v_worker.id and rw.work_id=a.work_id
      and rw.captured_revision_id=a.effective_revision_id
    where a.accepted_input_id=v_accepted.id and (cut.id is null or rw.id is null)) then
    raise exception using errcode='23514',message='BPAY_NEXT_CAPTURE_ACCEPTED_CUT_MISSING';end if;
  select wi.work_id into v_issue_work_id from private.bpay_next_accepted_work_input wi
    where wi.accepted_input_id=v_accepted.id and wi.selection_mode='ALL'
      and exists(select 1 from private.bpay_next_position position
        left join private.bpay_next_work_collection collection on collection.work_id=position.work_id and collection.component_key=position.component_key
        where position.work_id=wi.work_id and not exists(select 1 from private.bpay_next_accepted_work_member member
          where member.accepted_work_input_id=wi.id and member.component_key=position.component_key)
          and ((position.realised_source_ex_vat,position.held_source_ex_vat,position.realised_target_ex_vat,
             position.realised_target_vat,position.realised_target_inc_vat,position.held_target_ex_vat,
             position.held_target_vat,position.held_target_inc_vat) is distinct from
              (0::numeric,0::numeric,0::numeric,0::numeric,0::numeric,0::numeric,0::numeric,0::numeric)
            or collection.case_id is not null))
    order by wi.work_id limit 1;
  if found then
    update private.bpay_next_run_worker set status='REVIEW',review_issue_code='ACCEPTED_REMOVED_COMPONENT_UNBOUND',
      review_issue_work_id=v_issue_work_id where id=v_worker.id;
    update private.bpay_next_job set status='DONE',phase='DONE',cursor_key=null,lease_nonce=null,lease_until_utc=null where id=p_job_id;
    return jsonb_build_object('done',true,'phase','REVIEW','issue_code','ACCEPTED_REMOVED_COMPONENT_UNBOUND',
      'work_id',v_issue_work_id,'replay',false);
  end if;
  v_work_cursor:=v_job.position_work_cursor;
  v_component_cursor:=v_job.position_component_cursor;
  for v_member in
    select rw.id as run_work_id,rw.work_id,rw.captured_revision_id,
      member.component_key,wi.selection_mode,cut.id as effective_cut_id,
      member.effective_approved_line_id,member.source_binding_version_id,member.source_binding_sha256
    from private.bpay_next_accepted_work_input wi
    join private.bpay_next_effective_work_cut cut on cut.accepted_work_input_id=wi.id and cut.prepare_job_id=p_job_id
    join private.bpay_next_run_work rw on rw.run_worker_id=v_worker.id and rw.work_id=cut.work_id
      and rw.captured_revision_id=cut.effective_revision_id
    join private.bpay_next_accepted_work_member member on member.accepted_work_input_id=wi.id
      and member.work_id=cut.work_id and member.effective_revision_id=cut.effective_revision_id
    where wi.accepted_input_id=v_accepted.id
      and (v_work_cursor is null or rw.work_id>v_work_cursor
        or (rw.work_id=v_work_cursor and member.component_key>v_component_cursor))
    order by rw.work_id,member.component_key limit p_limit
  loop
    -- The Candidate control is already exclusively held. Lock this exact
    -- selected WORK before its position, matching the reentrant seed order.
    perform 1 from private.bpay_next_work w where w.id=v_member.work_id
      and w.candidate_id=v_worker.candidate_id for update;
    if not found then raise exception using errcode='23514',message='BPAY_NEXT_CAPTURE_POSITION_WORK_SCOPE_INVALID';end if;
    -- Read and lock the exact position after choosing the bounded member.
    -- Do not FOR SHARE the nullable side of an outer join (PostgreSQL refuses
    -- that), and never turn a missing selected position into an omitted line.
    select v_member.run_work_id as run_work_id,v_member.work_id as work_id,
           v_member.captured_revision_id as captured_revision_id,
           p.component_key,p.applied_revision_id,
           l.source_pay_ex_vat as approved_source_ex_vat,p.realised_source_ex_vat,
           p.held_source_ex_vat,p.realised_target_ex_vat,
           p.realised_target_vat,p.realised_target_inc_vat,
           p.held_target_ex_vat,p.held_target_vat,
           p.held_target_inc_vat,p.source_basis_channel,
           rev.source_pay_channel,rev.source_kind,rev.source_event_id,
           rev.source_head_id,rev.financial_snapshot_id,
           rev.physical_timesheet_id,rev.physical_timesheet_version,
           w.work_kind,w.candidate_id as origin_candidate_id,
           l.component_kind,l.id as approved_line_id,
           l.source_pay_ex_vat as approved_line_source_ex_vat,l.tax_treatment,
           p.source_book_state,p.source_book_revision
      into v_row
      from private.bpay_next_position p
      join private.bpay_next_work_revision rev
        on rev.id=v_member.captured_revision_id and rev.work_id=v_member.work_id
      join private.bpay_next_work w on w.id=rev.work_id
      join private.bpay_next_approved_line l
        on l.id=v_member.effective_approved_line_id and l.revision_id=v_member.captured_revision_id
       and l.component_key=v_member.component_key
      where p.work_id=v_member.work_id and p.component_key=v_member.component_key
      for update of p;
    v_issue:=null;
    if not found then
      v_issue:='CAPTURE_POSITION_CHANGED';
    elsif v_row.approved_line_id is distinct from v_member.effective_approved_line_id then
      v_issue:='POSITION_COMPONENT_EVIDENCE_MISSING';
    end if;
    if v_worker.target_pay_channel='UMBRELLA' and v_issue is null and
       (v_row.source_pay_channel is distinct from 'UMBRELLA'
        or v_row.work_kind not in ('ORDINARY','SOURCE')
        or v_row.component_kind is distinct from 'WORK'
        or v_row.tax_treatment is distinct from 'TAXABLE') then
      v_issue:='UMBRELLA_WORK_EXECUTION_UNSUPPORTED';
    end if;
    if v_issue is not null then
      update private.bpay_next_run_worker
        set status='REVIEW',review_issue_code=v_issue,
            review_issue_work_id=v_member.work_id
        where id=v_worker.id;
      update private.bpay_next_job
        set status='DONE',phase='DONE',cursor_key=null,
            lease_nonce=null,lease_until_utc=null
        where id=p_job_id;
      return pg_catalog.jsonb_build_object('done',true,'phase','REVIEW',
        'issue_code',v_issue,'work_id',v_member.work_id,'replay',false);
    end if;
    -- Typed initial I-1 is the ONLY new no-HEAD binding arm. Initial and
    -- common-HEAD captures require self-canonical same-channel TAXABLE WORK.
    v_binding:=null;v_book:=null;
    if v_row.work_kind='SOURCE' and (v_row.source_head_id is not null
      or (v_row.source_kind='SOURCE' and v_row.source_event_id is not null
        and v_row.financial_snapshot_id is not null)) then
      select v.* into v_binding from private.bpay_next_source_component_binding_version v
        where v.id=v_member.source_binding_version_id and v.binding_digest=v_member.source_binding_sha256
          and v.canonical_work_id=v_row.work_id and v.canonical_component_key=v_row.component_key
          and v.approval_work_id=v_row.work_id and v.approval_revision_id=v_row.captured_revision_id
          and v.approval_component_key=v_row.component_key
          and ((v_row.source_head_id is not null and v.origin_kind='COMMON_HEAD_RECEIPT_V1'
              and v.head_id=v_row.source_head_id)
            or (v_row.source_head_id is null and v.origin_kind='INITIAL_AUTHORISED_TSFIN_V1'
              and v.initial_root_authorisation_id=v_row.source_event_id
              and v.initial_financial_snapshot_id=v_row.financial_snapshot_id
              and v.initial_root_timesheet_id=v_row.physical_timesheet_id
              and v.initial_root_version=v_row.physical_timesheet_version));
      if v_binding.id is null or v_binding.publication_command_id_2 is not null
         or v_row.source_pay_channel not in ('PAYE','UMBRELLA')
         or v_worker.target_pay_channel is distinct from v_row.source_pay_channel
         or v_row.component_kind is distinct from 'WORK' or v_row.tax_treatment is distinct from 'TAXABLE' then
        v_issue:='SOURCE_BOOK_APPROVAL_UNBOUND';
      else
        if v_row.source_book_state='UNBOUND' then
          -- Capture is already a bounded selected point with real APPLIED/DONE
          -- authority. Adoption never borrows a queued writer's lease here.
          v_seed_effect:=null;v_seed_hold:=null;v_seed_holds:=null;
          if v_row.realised_source_ex_vat>0 then
            select b.original_effect_id into v_seed_effect from private.bpay_next_work_collection b
              where b.work_id=v_row.work_id and b.component_key=v_row.component_key
                and b.candidate_id=v_worker.candidate_id and b.paid_basis_qualified;
          end if;
          if v_row.held_source_ex_vat>0 then
            select pg_catalog.array_agg(h.id order by h.id) into v_seed_holds from (
              select id from private.bpay_next_hold where work_id=v_row.work_id
                and component_key=v_row.component_key and status='ACTIVE' order by id limit 2) h;
            if pg_catalog.cardinality(v_seed_holds) is distinct from 1 then
              raise exception using errcode='55000',message='BPAY_NEXT_SOURCE_BOOK_ACTIVE_HOLD_WITNESS_AMBIGUOUS';
            end if;
            v_seed_hold:=v_seed_holds[1];
          end if;
          select seed.* into strict v_seed from private.bpay_next_source_disposition_seed_accepted_v1(
            v_member.effective_cut_id,v_row.component_key,v_seed_effect,v_seed_hold) seed;
          if v_seed.qualification_state is distinct from 'READY' then
            raise exception using errcode='55000',message='BPAY_NEXT_SOURCE_BOOK_SELECTED_SEED_NOT_READY';
          end if;
          select p.source_book_state,p.source_book_revision into v_row.source_book_state,v_row.source_book_revision
            from private.bpay_next_position p where p.work_id=v_row.work_id and p.component_key=v_row.component_key;
        end if;
        select s.* into strict v_book from private.bpay_next_accepted_source_snapshot_v1(
          v_member.effective_cut_id,v_binding.id,v_row.source_book_revision) s;
        if v_book.qualification_state is distinct from 'READY'
           or (v_book.candidate_id,v_book.canonical_work_id,v_book.canonical_component_key,
               v_book.approval_work_id,v_book.approval_revision_id,v_book.approved_line_id,
               v_book.source_channel,v_book.tax_treatment,v_book.approved_source_ex_vat,
               v_book.native_realised_paye,v_book.native_held_paye,v_book.native_realised_umbrella,v_book.native_held_umbrella)
             is distinct from (v_worker.candidate_id,v_row.work_id,v_row.component_key,
               v_row.work_id,v_row.captured_revision_id,v_row.approved_line_id,
               v_row.source_pay_channel,'TAXABLE'::text,v_row.approved_source_ex_vat,
               case when v_row.source_pay_channel='PAYE' then v_row.realised_source_ex_vat else 0::numeric end,
               case when v_row.source_pay_channel='PAYE' then v_row.held_source_ex_vat else 0::numeric end,
               case when v_row.source_pay_channel='UMBRELLA' then v_row.realised_source_ex_vat else 0::numeric end,
               case when v_row.source_pay_channel='UMBRELLA' then v_row.held_source_ex_vat else 0::numeric end) then
          v_issue:='SOURCE_BOOK_APPROVAL_UNBOUND';
        end if;
      end if;
      if v_issue is not null then
        update private.bpay_next_run_worker set status='REVIEW',review_issue_code=v_issue,
          review_issue_work_id=v_row.work_id where id=v_worker.id;
        update private.bpay_next_job set status='DONE',phase='DONE',cursor_key=null,
          lease_nonce=null,lease_until_utc=null where id=p_job_id;
        return pg_catalog.jsonb_build_object('done',true,'phase','REVIEW','issue_code',v_issue,
          'work_id',v_row.work_id,'rows_visited',v_seen,'replay',false);
      end if;
    end if;
    v_collection:=null;
    -- Only the exact typed original ordinary PAYE liability can own a
    -- negative residual. Unknown signed/cross-basis/held shapes still REVIEW.
    if v_worker.target_pay_channel='PAYE' and v_row.source_pay_channel='PAYE'
       and v_row.approved_source_ex_vat-v_row.realised_source_ex_vat<0
       and (v_row.held_source_ex_vat,v_row.held_target_ex_vat,v_row.held_target_vat,v_row.held_target_inc_vat)
         is not distinct from (0::numeric,0::numeric,0::numeric,0::numeric) then
      select b.* into v_collection from private.bpay_next_work_collection b
        where b.work_id=v_row.work_id and b.component_key=v_row.component_key;
      if v_collection.case_id is not null then
        v_collection:=private.bpay_next_accepted_work_collection_origin_v1(
          v_member.effective_cut_id,v_row.component_key);
      else v_collection:=null;end if;
      if v_collection.id is not null and
         v_collection.observed_realised_source_ex_vat is distinct from v_row.realised_source_ex_vat then
        v_collection:=null;
      end if;
    end if;
    v_needs_resolution:=v_collection.id is null and (
       v_row.approved_line_id is null
       or v_row.approved_source_ex_vat<0
       or ((v_row.realised_source_ex_vat<>0
          or v_row.held_source_ex_vat<>0
          or v_row.realised_target_inc_vat<>0
          or v_row.held_target_inc_vat<>0)
         and v_row.source_basis_channel is distinct from v_row.source_pay_channel)
       or v_row.realised_source_ex_vat<0
       or v_row.realised_target_ex_vat<0 or v_row.realised_target_vat<0 or v_row.realised_target_inc_vat<0
       or (v_row.held_source_ex_vat>0 and v_row.approved_source_ex_vat-v_row.realised_source_ex_vat-v_row.held_source_ex_vat<=0)
       or (v_row.held_source_ex_vat=0 and v_row.held_target_inc_vat<>0)
       or v_row.approved_source_ex_vat-v_row.realised_source_ex_vat-v_row.held_source_ex_vat<0);
    -- Only genuine positive Source WORK consumes the retained disposition.
    -- Expense/additional/ordinary, zero, realised/held-away, managed negative
    -- and already-unresolved financial shapes do not become query holds.
    -- The actual pin validated the typed origin before this immutable row;
    -- missing evidence is REVIEW, never a lazy live Source reader fallback.
    if v_row.work_kind='SOURCE' and v_row.source_kind in ('SOURCE','PROTECTED')
       and v_row.source_event_id is not null
       and v_row.component_kind in ('WORK','PROTECTED_WORK')
       and not v_needs_resolution
       and v_row.approved_source_ex_vat-v_row.realised_source_ex_vat-v_row.held_source_ex_vat>0 then
      select qp.* into v_query_pin from private.bpay_next_run_work_query_pin_v2 qp
        where qp.run_work_id=v_row.run_work_id;
      if found and (v_query_pin.run_worker_id,v_query_pin.candidate_id,
          v_query_pin.work_id,v_query_pin.captured_revision_id,
          v_query_pin.prepare_job_id,v_query_pin.module_epoch,v_query_pin.gate_version)
          is distinct from (v_worker.id,v_worker.candidate_id,
          v_row.work_id,v_row.captured_revision_id,p_job_id,v_module_epoch,'PAY_QUERY_STATE_V3'::text) then
        raise exception using errcode='23514',message='BPAY_NEXT_CAPTURE_QUERY_PIN_MISMATCH';
      end if;
      if not found or v_query_pin.disposition='UNAVAILABLE' then
        v_issue:='SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE';
      elsif v_query_pin.disposition='BLOCKED' then
        v_issue:='SOURCE_PAY_QUERY_OPEN';
      elsif v_query_pin.disposition<>'CLEAR' or v_query_pin.scope is null
         or v_query_pin.query_state_sha256 is null
         or pg_catalog.octet_length(v_query_pin.query_state_sha256)<>32 then
        raise exception using errcode='23514',message='BPAY_NEXT_CAPTURE_QUERY_PIN_INVALID';
      end if;
      if v_issue is not null then
        update private.bpay_next_run_worker
          set status='REVIEW',review_issue_code=v_issue,review_issue_work_id=v_row.work_id
          where id=v_worker.id;
        update private.bpay_next_job
          set status='DONE',phase='DONE',cursor_key=null,
              lease_nonce=null,lease_until_utc=null
          where id=p_job_id;
        return pg_catalog.jsonb_build_object('done',true,'phase','REVIEW',
          'issue_code',v_issue,'work_id',v_row.work_id,'rows_visited',v_seen,'replay',false);
      end if;
    end if;
    insert into private.bpay_next_run_position
      (run_worker_id,run_work_id,work_id,captured_revision_id,
       component_key,approved_line_id,source_pay_channel,
       source_basis_channel,
       approved_source_ex_vat,realised_source_ex_vat,held_source_ex_vat,
       residual_source_ex_vat,realised_target_ex_vat,realised_target_vat,
       realised_target_inc_vat,held_target_ex_vat,held_target_vat,
       held_target_inc_vat,needs_financial_resolution,work_collection_id,work_collection_revision,
       source_binding_version_id,canonical_source_work_id,canonical_source_component_key,source_book_revision)
      values(v_worker.id,v_row.run_work_id,v_row.work_id,
       v_row.captured_revision_id,v_row.component_key,v_row.approved_line_id,
       v_row.source_pay_channel,v_row.source_basis_channel,
       v_row.approved_source_ex_vat,
       v_row.realised_source_ex_vat,v_row.held_source_ex_vat,
       v_row.approved_source_ex_vat-v_row.realised_source_ex_vat
         -v_row.held_source_ex_vat,
       v_row.realised_target_ex_vat,v_row.realised_target_vat,
       v_row.realised_target_inc_vat,v_row.held_target_ex_vat,
       v_row.held_target_vat,v_row.held_target_inc_vat,
       v_needs_resolution,v_collection.id,v_collection.reconcile_revision,
       v_binding.id,v_binding.canonical_work_id,v_binding.canonical_component_key,
       case when v_binding.id is null then null else v_row.source_book_revision end);
    if v_needs_resolution then
      v_resolution_count:=v_resolution_count+1;
    end if;
    if v_collection.id is not null then v_managed_seen:=v_managed_seen+1;end if;
    v_work_cursor:=v_row.work_id;
    v_component_cursor:=v_row.component_key;
    v_seen:=v_seen+1;
    if v_member.selection_mode='SUBSET' then
      v_selected_seen:=v_selected_seen+1;
    end if;
  end loop;
  if v_resolution_count>0 then
    update private.bpay_next_run_worker
      set financial_resolution_count=financial_resolution_count+v_resolution_count
      where id=v_worker.id;
  end if;
  if v_seen>0 then
    update private.bpay_next_run_worker
      set captured_position_count=captured_position_count+v_seen,
          expected_managed_position_count=expected_managed_position_count+v_managed_seen,
          captured_selected_component_count=
            captured_selected_component_count+v_selected_seen
      where id=v_worker.id;
  end if;
  if v_seen<p_limit then
    if v_worker.captured_selected_component_count+v_selected_seen
       <>v_worker.expected_selected_component_count then
      select rw.work_id into strict v_issue_work_id
        from private.bpay_next_run_work rw
        join private.bpay_next_work_choice choice
          on choice.run_id=v_run_id and choice.work_id=rw.work_id
         and choice.selection_mode='SUBSET'
        where rw.run_worker_id=v_worker.id
        order by rw.work_id limit 1;
      update private.bpay_next_run_worker
        set status='REVIEW',review_issue_code='CAPTURE_POSITION_CHANGED',
            review_issue_work_id=v_issue_work_id
        where id=v_worker.id;
      update private.bpay_next_job
        set status='DONE',phase='DONE',cursor_key=null,
            lease_nonce=null,lease_until_utc=null
        where id=p_job_id;
      return pg_catalog.jsonb_build_object('done',true,'phase','REVIEW',
        'issue_code','CAPTURE_POSITION_CHANGED','work_id',v_issue_work_id,'replay',false);
    end if;
    update private.bpay_next_job
      set phase='ALLOCATE',position_work_cursor=null,
          position_component_cursor=null
      where id=p_job_id;
    return pg_catalog.jsonb_build_object('done',true,'phase','ALLOCATE',
      'rows_visited',v_seen,'replay',false);
  end if;
  update private.bpay_next_job
    set position_work_cursor=v_work_cursor,
        position_component_cursor=v_component_cursor
    where id=p_job_id;
  return pg_catalog.jsonb_build_object('done',false,'phase','RESERVE',
    'rows_visited',v_seen,'work_cursor',v_work_cursor,
    'component_cursor',v_component_cursor,'replay',false);
end
$function$;

create or replace function private.bpay_next_hold_fresh_position_page_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,
  p_expected_work_cursor uuid,p_expected_component_cursor text,
  p_limit integer default 100
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_run_id uuid;
  v_run private.bpay_next_pay_run%rowtype;
  v_job private.bpay_next_job%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_module_epoch bigint;
  v_issue_work_id uuid;
  v_row record;
  v_cut private.bpay_next_effective_work_cut%rowtype;v_accepted_member private.bpay_next_accepted_work_member%rowtype;
  v_accepted_line private.bpay_next_approved_line%rowtype;
  v_position private.bpay_next_position%rowtype;
  v_value record;v_book record;
  v_run_line_id uuid;
  v_seen integer:=0;
  v_work_cursor uuid;
  v_component_cursor text;
  v_line_count bigint;
  v_managed_count bigint;
  v_total_ex numeric(18,2);
  v_total_vat numeric(18,2);
  v_total_inc numeric(18,2);
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null
     or p_limit not between 1 and 100
     or (p_expected_work_cursor is null and p_expected_component_cursor is not null) then
    raise exception using errcode='22023',message='BPAY_NEXT_HOLD_INPUT_INVALID';
  end if;
  select rc.run_id into strict v_run_id
    from private.bpay_next_job j
    join private.bpay_next_run_command rc on rc.command_id=j.command_id
    where j.id=p_job_id and j.job_kind='PREPARE';
  select m.owner_epoch into v_module_epoch
    from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_module_epoch is null then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select * into strict v_run from private.bpay_next_pay_run
    where id=v_run_id for share;
  select candidate_id into strict v_job.candidate_id
    from private.bpay_next_job where id=p_job_id;
  perform 1 from private.bpay_next_worker_control
    where candidate_id=v_job.candidate_id for update;
  select * into strict v_job from private.bpay_next_job
    where id=p_job_id for update;
  if v_run.status<>'PREPARING' or v_run.selection_state<>'SEALED'
     or v_job.job_kind<>'PREPARE' or v_job.status<>'LEASED'
     or v_job.phase not in ('ALLOCATE','FINANCE_ALLOCATE')
     or v_job.module_epoch<>v_module_epoch
     or v_job.lease_nonce<>p_lease_nonce
     or v_job.owner_epoch<>p_owner_epoch
     or v_job.lease_until_utc<=pg_catalog.clock_timestamp() then
    raise exception using errcode='55000',message='BPAY_NEXT_HOLD_LEASE_STALE';
  end if;
  if v_job.phase='FINANCE_ALLOCATE' then
    return pg_catalog.jsonb_build_object('done',true,
      'phase','FINANCE_ALLOCATE','rows_visited',0,'replay',true);
  end if;
  if v_job.position_work_cursor is distinct from p_expected_work_cursor
     or v_job.position_component_cursor is distinct from p_expected_component_cursor then
    return pg_catalog.jsonb_build_object('done',false,'phase','ALLOCATE',
      'rows_visited',0,'replay',true,
      'work_cursor',v_job.position_work_cursor,
      'component_cursor',v_job.position_component_cursor);
  end if;
  select * into strict v_worker from private.bpay_next_run_worker
    where run_id=v_run_id and candidate_id=v_job.candidate_id for update;
  if v_worker.status<>'PREPARING' or v_worker.review_issue_code is not null then
    raise exception using errcode='55000',
      message='BPAY_NEXT_HOLD_WORKER_NOT_PREPARING';
  end if;
  if v_worker.financial_resolution_count>0 then
    select rp.work_id into strict v_issue_work_id
      from private.bpay_next_run_position rp
      where rp.run_worker_id=v_worker.id and rp.needs_financial_resolution
      order by rp.work_id,rp.component_key limit 1;
    update private.bpay_next_run_worker
      set status='REVIEW',review_issue_code='FINANCIAL_RESOLUTION_REQUIRED',
          review_issue_work_id=v_issue_work_id
      where id=v_worker.id;
    update private.bpay_next_job
      set status='DONE',phase='DONE',cursor_key=null,
          lease_nonce=null,lease_until_utc=null
      where id=p_job_id;
    return pg_catalog.jsonb_build_object('done',true,'phase','REVIEW',
      'issue_code','FINANCIAL_RESOLUTION_REQUIRED',
      'work_id',v_issue_work_id,'replay',false);
  end if;
  if v_worker.target_pay_channel='UMBRELLA'
     and (v_worker.target_umbrella_id is null
       or v_worker.target_umbrella_vat_chargeable is null) then
    select rp.work_id into v_issue_work_id
      from private.bpay_next_run_position rp
      where rp.run_worker_id=v_worker.id
      order by rp.work_id,rp.component_key limit 1;
    if v_issue_work_id is not null then
      update private.bpay_next_run_worker
        set status='REVIEW',review_issue_code='TARGET_UMBRELLA_MISSING',
            review_issue_work_id=v_issue_work_id
        where id=v_worker.id;
      update private.bpay_next_job
        set status='DONE',phase='DONE',cursor_key=null,
            lease_nonce=null,lease_until_utc=null
        where id=p_job_id;
      return pg_catalog.jsonb_build_object('done',true,'phase','REVIEW',
        'issue_code','TARGET_UMBRELLA_MISSING',
        'work_id',v_issue_work_id,'replay',false);
    end if;
  end if;
  v_work_cursor:=v_job.position_work_cursor;
  v_component_cursor:=v_job.position_component_cursor;
  v_line_count:=v_worker.captured_line_count;
  v_managed_count:=v_worker.captured_managed_position_count;
  v_total_ex:=v_worker.gross_ex_vat;
  v_total_vat:=v_worker.gross_vat;
  v_total_inc:=v_worker.gross_inc_vat;
  for v_row in
    select rp.*,rw.id as pinned_work_id,rev.valuation_policy_id
      from private.bpay_next_run_position rp
      join private.bpay_next_run_work rw on rw.id=rp.run_work_id
      join private.bpay_next_work_revision rev
        on rev.id=rp.captured_revision_id and rev.work_id=rp.work_id
      where rp.run_worker_id=v_worker.id
        and (v_work_cursor is null or rp.work_id>v_work_cursor
          or (rp.work_id=v_work_cursor
            and rp.component_key>v_component_cursor))
      order by rp.work_id,rp.component_key limit p_limit
  loop
    if v_row.needs_financial_resolution
       or (v_row.work_collection_id is null and
         (v_row.approved_line_id is null or v_row.residual_source_ex_vat<0)) then
      raise exception using errcode='23514',
        message='BPAY_NEXT_HOLD_UNEXPECTED_POSITION_SHAPE';
    end if;
    select cut.* into strict v_cut from private.bpay_next_effective_work_cut cut
      join private.bpay_next_accepted_input input on input.id=cut.accepted_input_id
      where cut.work_id=v_row.work_id and cut.effective_revision_id=v_row.captured_revision_id
        and cut.prepare_job_id=p_job_id and cut.candidate_id=v_worker.candidate_id
        and input.run_id=v_run_id and input.prepare_command_id=v_job.command_id
        and input.module_epoch=v_job.module_epoch and input.accepted_sequence=v_job.command_sequence;
    select member.* into strict v_accepted_member from private.bpay_next_accepted_work_member member
      where member.accepted_work_input_id=v_cut.accepted_work_input_id and member.component_key=v_row.component_key;
    select line.* into strict v_accepted_line from private.bpay_next_approved_line line
      where line.id=v_accepted_member.effective_approved_line_id and line.revision_id=v_cut.effective_revision_id;
    if (v_row.approved_line_id,v_row.approved_source_ex_vat,v_row.source_pay_channel,v_row.source_binding_version_id)
      is distinct from (v_accepted_line.id,v_accepted_line.source_pay_ex_vat,
        (select r.source_pay_channel from private.bpay_next_work_revision r where r.id=v_cut.effective_revision_id),
        v_accepted_member.source_binding_version_id) then
      raise exception using errcode='23514',message='BPAY_NEXT_HOLD_ACCEPTED_POSITION_MISMATCH';end if;
    select * into strict v_position from private.bpay_next_position
      where work_id=v_row.work_id and component_key=v_row.component_key
      for update;
    if v_position.realised_source_ex_vat is distinct from
         v_row.realised_source_ex_vat
       or v_position.held_source_ex_vat is distinct from
         v_row.held_source_ex_vat
       or v_position.source_basis_channel is distinct from
         v_row.source_basis_channel
       or v_position.realised_target_ex_vat is distinct from
         v_row.realised_target_ex_vat
       or v_position.realised_target_vat is distinct from
         v_row.realised_target_vat
       or v_position.realised_target_inc_vat is distinct from
         v_row.realised_target_inc_vat
       or v_position.held_target_ex_vat is distinct from
         v_row.held_target_ex_vat
       or v_position.held_target_vat is distinct from
         v_row.held_target_vat
       or v_position.held_target_inc_vat is distinct from
         v_row.held_target_inc_vat then
      raise exception using errcode='55000',
        message='BPAY_NEXT_HOLD_CURRENT_POSITION_CHANGED';
    end if;
    if v_row.source_binding_version_id is not null then
      select s.* into strict v_book from private.bpay_next_accepted_source_snapshot_v1(
        v_cut.id,v_row.source_binding_version_id,v_row.source_book_revision) s;
      if v_book.qualification_state is distinct from 'READY'
         or (v_book.canonical_work_id,v_book.canonical_component_key,v_book.approval_work_id,
             v_book.approval_revision_id,v_book.approved_line_id,v_book.source_channel,v_book.tax_treatment,
             v_book.approved_source_ex_vat,v_book.native_realised_paye,v_book.native_held_paye,
             v_book.native_realised_umbrella,v_book.native_held_umbrella)
           is distinct from (v_row.work_id,v_row.component_key,v_row.work_id,
             v_row.captured_revision_id,v_row.approved_line_id,v_row.source_pay_channel,'TAXABLE'::text,
             v_row.approved_source_ex_vat,
             case when v_row.source_pay_channel='PAYE' then v_row.realised_source_ex_vat else 0::numeric end,
             case when v_row.source_pay_channel='PAYE' then v_row.held_source_ex_vat else 0::numeric end,
             case when v_row.source_pay_channel='UMBRELLA' then v_row.realised_source_ex_vat else 0::numeric end,
             case when v_row.source_pay_channel='UMBRELLA' then v_row.held_source_ex_vat else 0::numeric end)
         or v_row.source_pay_channel not in ('PAYE','UMBRELLA')
         or v_worker.target_pay_channel is distinct from v_row.source_pay_channel
         or v_row.canonical_source_work_id is distinct from v_row.work_id
         or v_row.canonical_source_component_key is distinct from v_row.component_key then
        raise exception using errcode='55000',message='BPAY_NEXT_SOURCE_BOOK_CAPTURE_CHANGED';
      end if;
    end if;
    if v_row.work_collection_id is not null then
      -- Classification and original provenance were frozen by RESERVE. Never
      -- reread a newer WORK approval or create an ordinary negative/zero hold.
      update private.bpay_next_run_position set managed_capture_completed=true
        where id=v_row.id and not managed_capture_completed;
      if not found then raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_CAPTURE_ALREADY_SPENT';end if;
      v_managed_count:=v_managed_count+1;
    else
    select * into strict v_value
      from private.bpay_next_value_source_for_target_v1(
        v_row.residual_source_ex_vat,v_row.source_pay_channel,
        v_worker.target_pay_channel,
        case when v_worker.target_pay_channel='UMBRELLA'
          then v_worker.target_umbrella_vat_chargeable else false end,
        v_row.valuation_policy_id,v_run.pay_date);
    v_line_count:=v_line_count+1;
    insert into private.bpay_next_run_line
      (run_worker_id,run_work_id,line_no,approved_line_id,
       captured_revision_id,work_id,component_key,source_consumed_ex_vat,
       source_pay_channel,target_pay_channel,valuation_policy_id,
       policy_window_id,frozen_ex_vat,frozen_vat,frozen_inc_vat,source_binding_version_id,
       canonical_source_work_id,canonical_source_component_key,source_book_revision,updates_original_source_scalar)
      values(v_worker.id,v_row.run_work_id,v_line_count,
       v_row.approved_line_id,v_row.captured_revision_id,v_row.work_id,
       v_row.component_key,v_row.residual_source_ex_vat,
       v_row.source_pay_channel,v_worker.target_pay_channel,
       v_row.valuation_policy_id,v_value.policy_window_id,
       v_value.target_ex_vat,v_value.target_vat,v_value.target_inc_vat,
       v_row.source_binding_version_id,v_row.canonical_source_work_id,v_row.canonical_source_component_key,
       v_row.source_book_revision,case when v_row.source_binding_version_id is null then null else true end)
      returning id into v_run_line_id;
    -- An authorised zero still has immutable shift/rate advice detail. It
    -- becomes a frozen zero line, never a zero-valued financial hold.
    if v_row.residual_source_ex_vat>0 then
      insert into private.bpay_next_hold
        (run_line_id,work_id,component_key,source_reserved_ex_vat,
         target_amount_ex_vat,target_amount_vat,target_amount_inc_vat,status)
        values(v_run_line_id,v_row.work_id,v_row.component_key,
         v_row.residual_source_ex_vat,v_value.target_ex_vat,
         v_value.target_vat,v_value.target_inc_vat,'ACTIVE');
      update private.bpay_next_position
        set held_source_ex_vat=held_source_ex_vat+v_row.residual_source_ex_vat,
            held_target_ex_vat=held_target_ex_vat+v_value.target_ex_vat,
            held_target_vat=held_target_vat+v_value.target_vat,
            held_target_inc_vat=held_target_inc_vat+v_value.target_inc_vat,
            updated_at_utc=pg_catalog.transaction_timestamp()
        where work_id=v_row.work_id and component_key=v_row.component_key;
    end if;
    v_total_ex:=v_total_ex+v_value.target_ex_vat;
    v_total_vat:=v_total_vat+v_value.target_vat;
    v_total_inc:=v_total_inc+v_value.target_inc_vat;
    end if;
    v_work_cursor:=v_row.work_id;
    v_component_cursor:=v_row.component_key;
    v_seen:=v_seen+1;
  end loop;
  if v_seen>0 then
    update private.bpay_next_run_worker
      set captured_line_count=v_line_count,
          captured_managed_position_count=v_managed_count,
          gross_ex_vat=v_total_ex,gross_vat=v_total_vat,
          gross_inc_vat=v_total_inc
      where id=v_worker.id;
    update private.bpay_next_worker_control
      set financial_view_revision=financial_view_revision+1,
          updated_at_utc=pg_catalog.transaction_timestamp()
      where candidate_id=v_job.candidate_id;
  end if;
  if v_seen<p_limit then
    if v_managed_count<>v_worker.expected_managed_position_count or exists(
      select 1 from private.bpay_next_run_position where run_worker_id=v_worker.id
        and work_collection_id is not null and not managed_capture_completed) then
      raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_CAPTURE_COUNT_MISMATCH';
    end if;
    update private.bpay_next_job
      set phase='FINANCE_ALLOCATE',position_work_cursor=null,
          position_component_cursor=null
      where id=p_job_id;
    return pg_catalog.jsonb_build_object('done',true,
      'phase','FINANCE_ALLOCATE','rows_visited',v_seen,'replay',false);
  end if;
  update private.bpay_next_job
    set position_work_cursor=v_work_cursor,
        position_component_cursor=v_component_cursor
    where id=p_job_id;
  return pg_catalog.jsonb_build_object('done',false,'phase','ALLOCATE',
    'rows_visited',v_seen,'work_cursor',v_work_cursor,
    'component_cursor',v_component_cursor,'replay',false);
end
$function$;

create or replace function private.bpay_next_post_simple_outcome_page_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,
  p_expected_cursor bigint,p_limit integer
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_epoch bigint;
  v_run uuid;
  v_candidate uuid;
  v_job private.bpay_next_job%rowtype;
  v_request private.bpay_next_outcome_request%rowtype;
  v_transfer private.bpay_next_transfer%rowtype;
  v_line private.bpay_next_run_line%rowtype;
  v_cut private.bpay_next_effective_work_cut%rowtype;v_accepted_member private.bpay_next_accepted_work_member%rowtype;
  v_book record;v_position private.bpay_next_position%rowtype;
  v_hold private.bpay_next_hold%rowtype;
  v_member record;
  v_cursor bigint;
  v_count bigint;
  v_seen integer:=0;
  v_original uuid;
  v_effect uuid;v_seed_effect uuid;
  v_return_cash private.bpay_next_return_cash%rowtype;
  v_internal private.bpay_next_internal_receipt%rowtype;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null
     or p_limit is null or p_limit not between 1 and 100
     or p_expected_cursor is not null and p_expected_cursor<1 then
    raise exception using errcode='22023',message='BPAY_NEXT_OUTCOME_PAGE_INPUT_INVALID';
  end if;
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select w.run_id,j.candidate_id into strict v_run,v_candidate
    from private.bpay_next_job j
    join private.bpay_next_outcome_request r on r.command_id=j.command_id
    join private.bpay_next_run_worker w on w.id=r.run_worker_id
    where j.id=p_job_id and r.candidate_id=j.candidate_id;
  perform 1 from private.bpay_next_pay_run where id=v_run for update;
  perform 1 from private.bpay_next_worker_control where candidate_id=v_candidate for update;
  select * into strict v_job from private.bpay_next_job where id=p_job_id for update;
  select * into strict v_request from private.bpay_next_outcome_request
    where command_id=v_job.command_id for update;
  perform 1 from private.bpay_next_run_worker where id=v_request.run_worker_id for update;
  select * into strict v_transfer from private.bpay_next_transfer
    where id=v_request.transfer_id for update;
  if exists(select 1 from private.bpay_next_run_worker w
            where w.id=v_request.run_worker_id and w.status in ('CANCELLING','CANCELLED')) then
    raise exception using errcode='55000',message='BPAY_NEXT_OUTCOME_CANCELLED_GROUP';
  end if;
  if v_job.job_kind not in ('CSV_SETTLEMENT','INTERNAL_SETTLEMENT') or v_job.module_epoch<>v_epoch
     or v_transfer.candidate_id<>v_candidate
     or v_transfer.run_worker_id<>v_request.run_worker_id then
    raise exception using errcode='23514',message='BPAY_NEXT_OUTCOME_SCOPE_INVALID';
  end if;
  if v_job.job_kind='INTERNAL_SETTLEMENT' then
    select * into strict v_internal from private.bpay_next_internal_receipt where id=v_request.internal_receipt_id;
    if v_request.outcome_id is not null or v_internal.command_id<>v_job.command_id
       or v_internal.transfer_id<>v_transfer.id or v_internal.run_worker_id<>v_request.run_worker_id
       or v_internal.candidate_id<>v_candidate or v_internal.projection_id<>v_transfer.projection_id
       or v_transfer.execution_kind<>'INTERNAL_ZERO' or v_transfer.cash_amount<>0
       or v_transfer.original_transfer_id is not null or v_transfer.return_cash_id is not null then
      raise exception using errcode='23514',message='BPAY_NEXT_INTERNAL_OUTCOME_SCOPE_INVALID';
    end if;
  elsif v_transfer.execution_kind<>'BANK' or v_request.outcome_id is null or v_request.internal_receipt_id is not null then
    raise exception using errcode='23514',message='BPAY_NEXT_BANK_OUTCOME_SCOPE_INVALID';
  end if;
  if v_job.status='DONE' and v_request.posting_complete then
    return pg_catalog.jsonb_build_object('done',true,'replay',true,
      'posted_members',v_request.posted_member_count);
  end if;
  if v_job.status<>'LEASED' or v_job.phase<>'MEMBERS'
     or v_job.lease_nonce<>p_lease_nonce or v_job.owner_epoch<>p_owner_epoch
     or v_job.lease_until_utc<=pg_catalog.clock_timestamp()
     or (v_job.job_kind='CSV_SETTLEMENT' and v_transfer.status not in ('SETTLED','RETURNED'))
     or (v_job.job_kind='INTERNAL_SETTLEMENT' and v_transfer.status<>'INTERNAL_PROCESSING')
     or (v_job.job_kind='INTERNAL_SETTLEMENT' and not exists(select 1 from private.bpay_next_worker_control
       where candidate_id=v_candidate and active_owner_epoch=p_owner_epoch))
     or exists(select 1 from private.bpay_next_job j where j.candidate_id=v_candidate
               and j.command_sequence<v_job.command_sequence and j.status<>'DONE') then
    raise exception using errcode='55000',message='BPAY_NEXT_OUTCOME_POST_NOT_ELIGIBLE';
  end if;
  if v_job.cursor_key is distinct from p_expected_cursor::text then
    return pg_catalog.jsonb_build_object('done',false,'replay',true,
      'cursor',v_job.cursor_key,'posted_members',v_request.posted_member_count);
  end if;
  v_cursor:=p_expected_cursor; v_count:=v_request.posted_member_count;
  if v_transfer.return_cash_id is not null then
    select * into strict v_return_cash from private.bpay_next_return_cash
      where id=v_transfer.return_cash_id for update;
    if v_transfer.member_count<>1 or v_return_cash.candidate_id<>v_candidate
       or v_return_cash.original_transfer_id<>v_transfer.original_transfer_id
       or v_return_cash.amount_held<v_transfer.cash_amount then
      raise exception using errcode='23514',message='BPAY_NEXT_REISSUE_CASH_HOLD_MISMATCH';
    end if;
  end if;
  for v_member in select * from private.bpay_next_transfer_member
    where transfer_id=v_transfer.id and (v_cursor is null or member_no>v_cursor)
    order by member_no limit p_limit
  loop
    if v_member.subject_kind='CASH_REISSUE' then
      if v_transfer.return_cash_id is null
         or v_member.signed_cash_contribution<>v_transfer.cash_amount then
        raise exception using errcode='23514',message='BPAY_NEXT_REISSUE_MEMBER_MISMATCH';
      end if;
      update private.bpay_next_return_cash
        set amount_held=amount_held-v_transfer.cash_amount,
            amount_reissued_paid=amount_reissued_paid+v_transfer.cash_amount
        where id=v_return_cash.id;
    elsif v_transfer.return_cash_id is not null then
      raise exception using errcode='23514',message='BPAY_NEXT_REISSUE_PAYROLL_MEMBER_FORBIDDEN';
    elsif v_member.subject_kind='WORK' then
      select * into strict v_line from private.bpay_next_run_line where id=v_member.run_line_id;
      if v_line.run_worker_id<>v_request.run_worker_id
         or v_line.target_pay_channel not in ('PAYE','UMBRELLA')
         or v_line.frozen_inc_vat<>v_member.signed_cash_contribution then
        raise exception using errcode='23514',message='BPAY_NEXT_OUTCOME_FROZEN_MEMBER_MISMATCH';
      end if;
      if v_line.target_pay_channel='UMBRELLA' and
         (v_line.source_pay_channel is distinct from 'UMBRELLA'
          or v_line.frozen_ex_vat is distinct from v_line.source_consumed_ex_vat
          or not exists(select 1 from private.bpay_next_approved_line a
            where a.id=v_line.approved_line_id and a.revision_id=v_line.captured_revision_id
              and a.component_key=v_line.component_key and a.component_kind='WORK' and a.tax_treatment='TAXABLE')) then
        raise exception using errcode='55000',message='BPAY_NEXT_UMBRELLA_WORK_EXECUTION_UNSUPPORTED';
      end if;
      if v_line.source_consumed_ex_vat>0 then
        select * into strict v_hold from private.bpay_next_hold where run_line_id=v_line.id for update;
        if v_hold.status<>'ACTIVE' or v_hold.work_id<>v_line.work_id
           or v_hold.component_key<>v_line.component_key
           or v_hold.source_reserved_ex_vat<>v_line.source_consumed_ex_vat
           or v_hold.target_amount_ex_vat<>v_line.frozen_ex_vat
           or v_hold.target_amount_vat<>v_line.frozen_vat
           or v_hold.target_amount_inc_vat<>v_line.frozen_inc_vat then
          raise exception using errcode='23514',message='BPAY_NEXT_OUTCOME_HOLD_MISMATCH';
        end if;
        select cut.* into v_cut from private.bpay_next_effective_work_cut cut
          join private.bpay_next_accepted_input input on input.id=cut.accepted_input_id
          join private.bpay_next_run_work rw on rw.id=v_line.run_work_id and rw.work_id=cut.work_id
            and rw.captured_revision_id=cut.effective_revision_id
          join private.bpay_next_run_worker worker on worker.id=rw.run_worker_id and worker.run_id=input.run_id
            and worker.candidate_id=input.candidate_id and worker.candidate_id=cut.candidate_id
          join private.bpay_next_run_command rc on rc.run_id=worker.run_id and rc.command_id=input.prepare_command_id
          where worker.id=v_request.run_worker_id and cut.work_id=v_line.work_id
            and cut.effective_revision_id=v_line.captured_revision_id and cut.candidate_id=v_candidate;
        if found then
          select member.* into strict v_accepted_member from private.bpay_next_accepted_work_member member
            where member.accepted_work_input_id=v_cut.accepted_work_input_id and member.component_key=v_line.component_key;
          if (v_line.approved_line_id,v_line.source_binding_version_id) is distinct from
            (v_accepted_member.effective_approved_line_id,v_accepted_member.source_binding_version_id) then
            raise exception using errcode='23514',message='BPAY_NEXT_OUTCOME_ACCEPTED_MEMBER_MISMATCH';end if;
          if v_line.source_binding_version_id is not null then
            -- The owned ACTIVE hold advanced the book after capture. Use its
            -- current actual money revision; never recapture approved terms.
            select p.* into strict v_position from private.bpay_next_position p
              where p.work_id=v_line.work_id and p.component_key=v_line.component_key for update;
            select snapshot.* into strict v_book from private.bpay_next_accepted_source_snapshot_v1(
              v_cut.id,v_line.source_binding_version_id,v_position.source_book_revision) snapshot;
            if v_book.qualification_state is distinct from 'READY'
              or (v_book.candidate_id,v_book.canonical_work_id,v_book.canonical_component_key,v_book.approved_line_id,
                  v_book.source_channel,v_book.tax_treatment)
                is distinct from (v_candidate,v_line.work_id,v_line.component_key,v_line.approved_line_id,v_line.source_pay_channel,'TAXABLE'::text) then
              raise exception using errcode='23514',message='BPAY_NEXT_OUTCOME_ACCEPTED_BOOK_UNQUALIFIED';end if;
          end if;
        elsif exists(select 1 from private.bpay_next_accepted_input input
          where input.run_id=v_run and input.candidate_id=v_candidate) then
          raise exception using errcode='23514',message='BPAY_NEXT_OUTCOME_ACCEPTED_CUT_MISSING';
        else
        -- A retained unbound frozen child may settle after common-HEAD adoption.
        -- Seed from the real ACTIVE hold/current APPLIED position only; never
        -- require its old capture to point at a newly-created binding version.
        if exists(select 1 from private.bpay_next_position p
          join private.bpay_next_source_component_binding b on b.canonical_work_id=p.work_id
            and b.canonical_component_key=p.component_key
          join private.bpay_next_source_component_binding_version v on v.id=b.current_binding_version_id
          where p.work_id=v_line.work_id and p.component_key=v_line.component_key
            and p.source_book_state='UNBOUND' and v.approval_work_id=p.work_id
            and v.approval_revision_id=p.applied_revision_id and v.publication_command_id_2 is null
            and v.approval_source_channel=v_line.source_pay_channel
            and v.approval_source_channel in ('PAYE','UMBRELLA') and v.tax_treatment='TAXABLE') then
          select b.original_effect_id into v_seed_effect from private.bpay_next_work_collection b
            join private.bpay_next_position p on p.work_id=b.work_id and p.component_key=b.component_key
            where b.work_id=v_line.work_id and b.component_key=v_line.component_key and p.realised_source_ex_vat>0;
          perform * from private.bpay_next_source_disposition_seed_v1(v_line.work_id,v_line.component_key,
            (select applied_revision_id from private.bpay_next_position where work_id=v_line.work_id
              and component_key=v_line.component_key),v_seed_effect,v_hold.id);
        end if;
        end if;
        select original_timesheet_id into strict v_original from private.bpay_next_work where id=v_line.work_id;
        insert into private.bpay_next_financial_effect
          (operation_id,operation_item_id,effect_kind,candidate_id,work_id,component_key,
           original_timesheet_id,original_transfer_id,source_disposition_ex_vat,
           target_amount_ex_vat,target_amount_vat,target_amount_inc_vat,source_run_line_id,
           source_binding_version_id,canonical_source_work_id,canonical_source_component_key,native_source_channel)
          values(v_job.command_id,v_hold.id,'PAYROLL_SETTLED',v_candidate,
            v_line.work_id,v_line.component_key,v_original,v_transfer.id,
            v_hold.source_reserved_ex_vat,v_hold.target_amount_ex_vat,
            v_hold.target_amount_vat,v_hold.target_amount_inc_vat,
            case when v_line.source_binding_version_id is null then null else v_line.id end,
            v_line.source_binding_version_id,v_line.canonical_source_work_id,v_line.canonical_source_component_key,
            case when v_line.source_binding_version_id is null then null else v_line.source_pay_channel end)
          returning id into v_effect;
        update private.bpay_next_run_worker
          set realised_effect_count=realised_effect_count+1
          where id=v_request.run_worker_id;
        update private.bpay_next_position
          set held_source_ex_vat=held_source_ex_vat-v_hold.source_reserved_ex_vat,
              held_target_ex_vat=held_target_ex_vat-v_hold.target_amount_ex_vat,
              held_target_vat=held_target_vat-v_hold.target_amount_vat,
              held_target_inc_vat=held_target_inc_vat-v_hold.target_amount_inc_vat,
              realised_source_ex_vat=realised_source_ex_vat+v_hold.source_reserved_ex_vat,
              realised_target_ex_vat=realised_target_ex_vat+v_hold.target_amount_ex_vat,
              realised_target_vat=realised_target_vat+v_hold.target_amount_vat,
              realised_target_inc_vat=realised_target_inc_vat+v_hold.target_amount_inc_vat,
              updated_at_utc=pg_catalog.transaction_timestamp()
          where work_id=v_line.work_id and component_key=v_line.component_key
            and source_basis_channel=v_line.source_pay_channel
            and held_source_ex_vat>=v_hold.source_reserved_ex_vat;
        if not found then
          raise exception using errcode='23514',message='BPAY_NEXT_OUTCOME_POSITION_MISMATCH';
        end if;
        update private.bpay_next_hold set status='REALISED',finished_at_utc=pg_catalog.transaction_timestamp()
          where id=v_hold.id;
        perform private.bpay_next_reconcile_work_collection_v1(
          p_job_id,v_line.work_id,v_line.component_key,v_effect);
      end if;
    elsif v_member.subject_kind='CASE' then
      perform private.bpay_next_post_case_transfer_member_v1(p_job_id,v_transfer.id,v_member.member_no);
    elsif v_member.subject_kind<>'NET_ADJUSTMENT' then
      raise exception using errcode='55000',message='BPAY_NEXT_SIMPLE_OUTCOME_CASE_NOT_SUPPORTED';
    end if;
    v_cursor:=v_member.member_no; v_count:=v_count+1; v_seen:=v_seen+1;
  end loop;
  if v_seen=0 or v_count>v_transfer.member_count then
    raise exception using errcode='23514',message='BPAY_NEXT_OUTCOME_MEMBER_COUNT_INVALID';
  end if;
  update private.bpay_next_outcome_request
    set posted_member_count=v_count,posting_complete=(v_count=v_transfer.member_count)
    where command_id=v_job.command_id;
  update private.bpay_next_worker_control
    set financial_view_revision=financial_view_revision+1,
        updated_at_utc=pg_catalog.transaction_timestamp()
    where candidate_id=v_candidate;
  if v_count=v_transfer.member_count then
    if exists(select 1 from private.bpay_next_transfer_member
              where transfer_id=v_transfer.id and member_no>v_cursor) then
      raise exception using errcode='23514',message='BPAY_NEXT_OUTCOME_EXTRA_MEMBER';
    end if;
    if v_job.job_kind='INTERNAL_SETTLEMENT' then
      perform private.bpay_next_complete_internal_zero_v1(p_job_id,v_cursor);
    else
      perform private.bpay_next_apply_week_outcome_v1(p_job_id,null);
    end if;
    perform private.bpay_next_complete_destination_leg_v1(p_job_id);
    update private.bpay_next_job set status='DONE',phase='COMPLETE',cursor_key=v_cursor::text,
      lease_nonce=null,lease_until_utc=null where id=p_job_id;
    update private.bpay_next_command set status='COMPLETE' where id=v_job.command_id;
  else
    update private.bpay_next_job set cursor_key=v_cursor::text where id=p_job_id;
  end if;
  return pg_catalog.jsonb_build_object('done',v_count=v_transfer.member_count,
    'replay',false,'cursor',v_cursor,'posted_members',v_count,'rows_visited',v_seen);
end
$function$;

create or replace function private.bpay_next_work_collection_capture_guard_v1()
returns trigger language plpgsql set search_path=pg_catalog,private
as $function$
declare v_basis private.bpay_next_work_collection%rowtype;v_parent_scope record;
  v_cut private.bpay_next_effective_work_cut%rowtype;v_member private.bpay_next_accepted_work_member%rowtype;
  v_line private.bpay_next_approved_line%rowtype;v_position private.bpay_next_position%rowtype;
begin
  if tg_table_name='bpay_next_case_period' then
    if tg_op='UPDATE' then
      if new.work_collection_id is distinct from old.work_collection_id then
        raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_PERIOD_ORIGIN_IMMUTABLE';
      end if;
      if new.work_collection_id is not null and
         ((new.case_component_id,new.case_id,new.candidate_id,new.pay_week_start,new.rule_id,
           new.opening_outstanding_source_ex_vat,new.opening_due_source_ex_vat)
           is distinct from (old.case_component_id,old.case_id,old.candidate_id,old.pay_week_start,old.rule_id,
             old.opening_outstanding_source_ex_vat,old.opening_due_source_ex_vat)
          or new.period_revision<>old.period_revision+1
          or new.realised_recovery_source_ex_vat<old.realised_recovery_source_ex_vat
          or new.active_payout_source_ex_vat<>0
          or not exists(select 1 from private.bpay_next_case_component x
            join private.bpay_next_finance_case c on c.id=x.case_id and c.candidate_id=x.candidate_id
            where x.id=new.case_component_id and x.id=new.work_collection_id
              and x.case_id=new.case_id and x.candidate_id=new.candidate_id
              and x.recovered_source_ex_vat>=new.realised_recovery_source_ex_vat
              and x.active_recovery_source_ex_vat>=new.active_unrealised_recovery_source_ex_vat
              and (c.principal_approved,c.principal_recovered,c.principal_written_off,c.active_recovery_hold_amount)
                is not distinct from (x.approved_source_ex_vat,x.recovered_source_ex_vat,x.written_off_source_ex_vat,x.active_recovery_source_ex_vat))) then
        raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_PERIOD_COUNTER_INVALID';
      end if;
      return new;
    end if;
    if new.work_collection_id is null then
      if exists(select 1 from private.bpay_next_work_collection b where b.case_id=new.case_id) then
        raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_PERIOD_ORIGIN_REQUIRED';
      end if;
      return new;
    end if;
select input.id as accepted_input_id, state.run_id,state.run_worker_id,state.preparation_revision,state.selection_revision,
       child.rule_id, child.rule_revision, child.valuation_policy_id,
       child.work_collection_id, state.id as allocation_state_id
into strict v_parent_scope
from private.bpay_next_case_allocation_state state
join private.bpay_next_job job
  on job.id=state.job_id and job.command_id=state.command_id
    and job.candidate_id=state.candidate_id and job.module_epoch=state.module_epoch
join private.bpay_next_worker_control owner
  on owner.candidate_id=job.candidate_id and owner.active_owner_epoch=job.owner_epoch
join private.bpay_next_module_control module
  on module.id=1 and module.active_owner='NEXT' and module.owner_epoch=job.module_epoch
join private.bpay_next_run_worker worker
  on worker.id=state.run_worker_id and worker.run_id=state.run_id
    and worker.candidate_id=state.candidate_id
join private.bpay_next_pay_run run on run.id=state.run_id
join private.bpay_next_run_command rc
  on rc.run_id=state.run_id and rc.command_id=job.command_id
join private.bpay_next_command command
  on command.id=job.command_id and command.command_kind='PREPARE'
    and command.agency_sequence=job.command_sequence and command.module_epoch=job.module_epoch
join private.bpay_next_accepted_input input
  on input.run_id=state.run_id and input.prepare_command_id=job.command_id
    and input.candidate_id=state.candidate_id and input.module_epoch=job.module_epoch
    and input.accepted_sequence=job.command_sequence
join private.bpay_next_accepted_case_input child
  on child.accepted_input_id=input.id and child.run_id=state.run_id
    and child.candidate_id=state.candidate_id and child.selection_revision=state.selection_revision
where state.candidate_id=new.candidate_id and state.pass_kind='DRAFT'
  and state.projection_no=0 and state.projection_id is null and state.status='BUILDING'
  and state.prepare_stage='INSTRUCTION_CAPTURE'
  and state.owner_epoch=job.owner_epoch and state.module_epoch=module.owner_epoch
  and state.preparation_revision=worker.preparation_revision
  and state.selection_revision=worker.case_selection_revision
  and state.pay_week_start=new.pay_week_start and state.pay_week_start=input.pay_week_start
  and worker.status='PREPARING' and worker.review_issue_code is null
  and run.status='PREPARING' and run.selection_state='SEALED'
  and job.job_kind='PREPARE' and job.status='LEASED' and job.phase='FINANCE_ALLOCATE'
  and job.lease_nonce is not null and job.lease_until_utc>pg_catalog.clock_timestamp()
  and input.case_scope='SEALED_SELECTION' and input.case_selection_revision=state.selection_revision
  and child.is_selected and child.case_id=new.case_id and child.case_component_id=new.case_component_id
  and child.economic_origin_kind='WORK_COLLECTION' and child.work_collection_id=new.work_collection_id
  and child.rule_id=new.rule_id;
    v_basis:=private.bpay_next_accepted_collection_origin_v1(v_parent_scope.accepted_input_id,new.case_component_id);
    if v_basis.id is distinct from new.work_collection_id then
      raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_PERIOD_ORIGIN_INVALID';
    end if;
    return new;
  end if;
  if tg_table_name='bpay_next_run_case_instruction' then
    if new.work_collection_id is null then
      if exists(select 1 from private.bpay_next_work_collection b where b.case_id=new.case_id) then
        raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_INSTRUCTION_ORIGIN_REQUIRED';
      end if;
      return new;
    end if;
select input.id as accepted_input_id, state.run_id,state.run_worker_id,state.preparation_revision,state.selection_revision,
       child.rule_id, child.rule_revision, child.valuation_policy_id,
       child.work_collection_id, state.id as allocation_state_id
into strict v_parent_scope
from private.bpay_next_case_allocation_state state
join private.bpay_next_job job
  on job.id=state.job_id and job.command_id=state.command_id
    and job.candidate_id=state.candidate_id and job.module_epoch=state.module_epoch
join private.bpay_next_worker_control owner
  on owner.candidate_id=job.candidate_id and owner.active_owner_epoch=job.owner_epoch
join private.bpay_next_module_control module
  on module.id=1 and module.active_owner='NEXT' and module.owner_epoch=job.module_epoch
join private.bpay_next_run_worker worker
  on worker.id=state.run_worker_id and worker.run_id=state.run_id
    and worker.candidate_id=state.candidate_id
join private.bpay_next_pay_run run on run.id=state.run_id
join private.bpay_next_run_command rc
  on rc.run_id=state.run_id and rc.command_id=job.command_id
join private.bpay_next_command command
  on command.id=job.command_id and command.command_kind='PREPARE'
    and command.agency_sequence=job.command_sequence and command.module_epoch=job.module_epoch
join private.bpay_next_accepted_input input
  on input.run_id=state.run_id and input.prepare_command_id=job.command_id
    and input.candidate_id=state.candidate_id and input.module_epoch=job.module_epoch
    and input.accepted_sequence=job.command_sequence
join private.bpay_next_accepted_case_input child
  on child.accepted_input_id=input.id and child.run_id=state.run_id
    and child.candidate_id=state.candidate_id and child.selection_revision=state.selection_revision
where state.candidate_id=new.candidate_id and state.pass_kind='DRAFT'
  and state.projection_no=0 and state.projection_id is null and state.status='BUILDING'
  and state.prepare_stage='INSTRUCTION_CAPTURE'
  and state.owner_epoch=job.owner_epoch and state.module_epoch=module.owner_epoch
  and state.preparation_revision=worker.preparation_revision
  and state.selection_revision=worker.case_selection_revision
  and state.pay_week_start=new.pay_week_start and state.pay_week_start=input.pay_week_start
  and worker.status='PREPARING' and worker.review_issue_code is null
  and run.status='PREPARING' and run.selection_state='SEALED'
  and job.job_kind='PREPARE' and job.status='LEASED' and job.phase='FINANCE_ALLOCATE'
  and job.lease_nonce is not null and job.lease_until_utc>pg_catalog.clock_timestamp()
  and input.case_scope='SEALED_SELECTION' and input.case_selection_revision=state.selection_revision
  and child.is_selected and child.case_id=new.case_id and child.case_component_id=new.case_component_id
  and child.economic_origin_kind='WORK_COLLECTION' and child.work_collection_id=new.work_collection_id
  and child.rule_id=new.rule_id;
    if (v_parent_scope.run_id,v_parent_scope.run_worker_id,v_parent_scope.preparation_revision,v_parent_scope.selection_revision,
        v_parent_scope.rule_revision,v_parent_scope.valuation_policy_id) is distinct from
      (new.run_id,new.run_worker_id,new.preparation_revision,new.selection_revision,new.rule_revision,new.valuation_policy_id) then
      raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_ACCEPTED_INSTRUCTION_SCOPE_INVALID';
    end if;
    v_basis:=private.bpay_next_accepted_collection_origin_v1(v_parent_scope.accepted_input_id,new.case_component_id);
    if v_basis.id is distinct from new.work_collection_id
       or new.valuation_policy_id is distinct from v_basis.original_valuation_policy_id then
      raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_INSTRUCTION_ORIGIN_INVALID';
    end if;
    return new;
  end if;
  if tg_op<>'INSERT' then
    if tg_op<>'UPDATE' or old.work_collection_id is null or old.managed_capture_completed
       or not new.managed_capture_completed
       or (pg_catalog.to_jsonb(new)-'managed_capture_completed')
         is distinct from (pg_catalog.to_jsonb(old)-'managed_capture_completed') then
      raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_CAPTURE_IMMUTABLE';
    end if;
  elsif new.work_collection_id is null then
    return new;
  else
    select b.* into strict v_basis from private.bpay_next_work_collection b where b.id=new.work_collection_id;
    select cut.* into strict v_cut from private.bpay_next_effective_work_cut cut
      join private.bpay_next_accepted_input input on input.id=cut.accepted_input_id
      join private.bpay_next_run_worker worker on worker.id=new.run_worker_id and worker.run_id=input.run_id
        and worker.candidate_id=input.candidate_id
      join private.bpay_next_run_work rw on rw.id=new.run_work_id and rw.run_worker_id=worker.id
        and rw.work_id=cut.work_id and rw.captured_revision_id=cut.effective_revision_id
      where cut.work_id=new.work_id and cut.candidate_id=worker.candidate_id
        and cut.effective_revision_id=new.captured_revision_id;
    select m.* into strict v_member from private.bpay_next_accepted_work_member m
      where m.accepted_work_input_id=v_cut.accepted_work_input_id and m.component_key=new.component_key;
    select l.* into strict v_line from private.bpay_next_approved_line l where l.id=v_member.effective_approved_line_id;
    select p.* into strict v_position from private.bpay_next_position p where p.work_id=new.work_id and p.component_key=new.component_key;
    v_basis:=private.bpay_next_accepted_work_collection_origin_v1(v_cut.id,new.component_key);
    if v_basis.id is distinct from new.work_collection_id or new.managed_capture_completed
       or new.work_collection_revision is distinct from v_basis.reconcile_revision
       or (new.captured_revision_id,new.approved_line_id,new.approved_source_ex_vat)
         is distinct from (v_cut.effective_revision_id,v_member.effective_approved_line_id,v_line.source_pay_ex_vat)
       or (new.realised_source_ex_vat,new.held_source_ex_vat,new.realised_target_ex_vat,new.realised_target_vat,
           new.realised_target_inc_vat,new.held_target_ex_vat,new.held_target_vat,new.held_target_inc_vat,new.source_basis_channel)
         is distinct from (v_position.realised_source_ex_vat,v_position.held_source_ex_vat,v_position.realised_target_ex_vat,
           v_position.realised_target_vat,v_position.realised_target_inc_vat,v_position.held_target_ex_vat,
           v_position.held_target_vat,v_position.held_target_inc_vat,v_position.source_basis_channel)
       or new.source_pay_channel<>'PAYE' or new.source_basis_channel is distinct from 'PAYE'
       or (new.held_source_ex_vat,new.held_target_ex_vat,new.held_target_vat,new.held_target_inc_vat)
         is distinct from (0::numeric,0::numeric,0::numeric,0::numeric) then
      raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_CAPTURE_ORIGIN_INVALID';
    end if;
  end if;
  if not exists(select 1 from private.bpay_next_run_work rw
    join private.bpay_next_run_worker w on w.id=rw.run_worker_id and w.status='PREPARING'
    join private.bpay_next_run_command rc on rc.run_id=w.run_id
    join private.bpay_next_job j on j.command_id=rc.command_id and j.candidate_id=w.candidate_id
    join private.bpay_next_worker_control c on c.candidate_id=j.candidate_id and c.active_owner_epoch=j.owner_epoch
    join private.bpay_next_module_control m on m.id=1 and m.active_owner='NEXT' and m.owner_epoch=j.module_epoch
    where rw.id=new.run_work_id and rw.run_worker_id=new.run_worker_id and rw.work_id=new.work_id
      and rw.captured_revision_id=new.captured_revision_id and j.job_kind='PREPARE' and j.status='LEASED'
      and j.phase=case when tg_op='INSERT' then 'RESERVE' else 'ALLOCATE' end
      and j.lease_until_utc>pg_catalog.clock_timestamp()) then
    raise exception using errcode='55000',message='BPAY_NEXT_COLLECTION_CAPTURE_OWNER_STALE';
  end if;
  return new;
end
$function$;

alter function private.bpay_next_accepted_work_collection_origin_v1(uuid,text) owner to postgres;
revoke all on function private.bpay_next_accepted_work_collection_origin_v1(uuid,text) from public,anon,authenticated,service_role;
alter function private.bpay_next_capture_position_page_v1(uuid,uuid,bigint,uuid,text,integer) owner to postgres;
revoke all on function private.bpay_next_capture_position_page_v1(uuid,uuid,bigint,uuid,text,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_hold_fresh_position_page_v1(uuid,uuid,bigint,uuid,text,integer) owner to postgres;
revoke all on function private.bpay_next_hold_fresh_position_page_v1(uuid,uuid,bigint,uuid,text,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_post_simple_outcome_page_v1(uuid,uuid,bigint,bigint,integer) owner to postgres;
revoke all on function private.bpay_next_post_simple_outcome_page_v1(uuid,uuid,bigint,bigint,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_work_collection_capture_guard_v1() owner to postgres;
revoke all on function private.bpay_next_work_collection_capture_guard_v1() from public,anon,authenticated,service_role;
notify pgrst,'reload schema';
commit;
