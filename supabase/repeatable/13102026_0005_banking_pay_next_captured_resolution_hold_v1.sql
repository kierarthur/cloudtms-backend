-- WB29/BD164: exact accepted treatment consumes one native Source reserve.
-- Current1419/1236 full successors; source-only canonical evidence, actual captured IDs retained.
\set ON_ERROR_STOP on
begin;
create or replace function private.bpay_next_capture_position_page_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,
  p_expected_work_cursor uuid,p_expected_component_cursor text,
  p_limit integer default 100
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private
as $function$
declare
  v_approval_revision_id uuid;v_approval_line_id uuid;v_effective_source_event_id uuid;
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
  v_issue text;v_answered_source boolean;
  v_issue_work_id uuid;
  v_query_pin private.bpay_next_run_work_query_pin_v2%rowtype;
  v_binding private.bpay_next_source_component_binding_version%rowtype;v_book record;
  v_segments bigint;v_snooze jsonb;v_segment_result jsonb;v_segment_visited integer:=0;
  v_seed_effect uuid;v_seed_hold uuid;v_seed_holds uuid[];v_seed record;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null
     or p_limit not between 1 and 100
     or (p_expected_work_cursor is null and p_expected_component_cursor is not null) then
    raise exception using errcode='22023',
      message='BPAY_NEXT_CAPTURE_POSITION_INPUT_INVALID';
  end if;
  select private.bpay_next_prepare_job_run_v1(j.id) into strict v_run_id
    from private.bpay_next_job j
    where j.id=p_job_id and j.job_kind='PREPARE';
  select m.owner_epoch into v_module_epoch
    from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_module_epoch is null then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select dr38_point_4.* into strict v_run from private.bpay_next_pay_run dr38_point_4
    where id=v_run_id for update;
  select candidate_id into strict v_job.candidate_id
    from private.bpay_next_job where id=p_job_id;
  perform 1 from private.bpay_next_worker_control
    where candidate_id=v_job.candidate_id for update;
  select dr38_point_5.* into strict v_job from private.bpay_next_job dr38_point_5
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
  select dr38_point_6.* into strict v_worker from private.bpay_next_run_worker dr38_point_6
    where run_id=v_run_id and candidate_id=v_job.candidate_id for update;
  if not private.bpay_next_prepare_job_active_v1(p_job_id) then
    raise exception using errcode='55000',message='BPAY_NEXT_WORKER_ACTION_REVISION_STALE';end if;
  if v_worker.status<>'PREPARING' or v_worker.review_issue_code is not null then
    raise exception using errcode='55000',
      message='BPAY_NEXT_CAPTURE_POSITION_WORKER_NOT_PREPARING';
  end if;
  v_work_cursor:=v_job.position_work_cursor;
  v_component_cursor:=v_job.position_component_cursor;
  for v_member in
    select s.* from private.bpay_next_selected_position_keys_v1(v_worker.id,v_work_cursor,v_component_cursor,p_limit) s
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
           p.approved_source_ex_vat,p.realised_source_ex_vat,
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
      left join private.bpay_next_approved_line l
        on l.revision_id=v_member.captured_revision_id
       and l.component_key=v_member.component_key
      where p.work_id=v_member.work_id and p.component_key=v_member.component_key
      for update of p;
    v_issue:=null;
    if not found
       or v_row.applied_revision_id is distinct from v_member.captured_revision_id then
      v_issue:='CAPTURE_POSITION_CHANGED';
    elsif v_row.approved_line_id is null
       and v_row.approved_source_ex_vat<>0 then
      v_issue:='POSITION_COMPONENT_EVIDENCE_MISSING';
    elsif v_row.approved_line_id is not null
       and v_row.approved_line_source_ex_vat is distinct from
         v_row.approved_source_ex_vat then
      v_issue:='POSITION_APPROVAL_AMOUNT_MISMATCH';
    end if;
    v_approval_revision_id:=v_row.captured_revision_id;v_approval_line_id:=v_row.approved_line_id;
    v_effective_source_event_id:=v_row.source_event_id;
    if v_issue is null and v_row.work_kind='SOURCE' then
      -- Only Source evidence uses the positively qualified canonical approval.
      -- Actual selected, captured and Position identities above remain strict.
      v_approval_revision_id:=private.bpay_next_treatment_approval_revision_v1(v_row.captured_revision_id);
      select r.source_event_id,a.id into v_effective_source_event_id,v_approval_line_id
        from private.bpay_next_work_revision r left join private.bpay_next_approved_line a
          on a.revision_id=r.id and a.component_key=v_row.component_key
        where r.id=v_approval_revision_id and r.work_id=v_row.work_id;
    end if;
    -- The Direct no-HEAD PROTECTED_WORK route retains its original restrictions.
    v_answered_source:=coalesce(v_row.work_kind='SOURCE'
      and (v_row.source_head_id is not null
        or (v_row.source_kind='SOURCE' and v_effective_source_event_id is not null
          and v_row.financial_snapshot_id is not null))
      and v_row.source_pay_channel in ('PAYE','UMBRELLA')
      and ((v_row.component_kind in ('WORK','ADDITIONAL') and v_row.tax_treatment='TAXABLE')
        or (v_row.component_kind in ('EXPENSE','MILEAGE') and v_row.tax_treatment='NON_TAXABLE')),false);
    if v_worker.target_pay_channel='UMBRELLA' and v_issue is null and not v_answered_source
      and not coalesce(v_row.source_pay_channel in ('PAYE','UMBRELLA') and (
        (v_row.work_kind='ORDINARY' and (
          (v_row.component_kind in ('WORK','ADDITIONAL') and v_row.tax_treatment='TAXABLE')
          or (v_row.component_kind in ('EXPENSE','MILEAGE') and v_row.tax_treatment='NON_TAXABLE')))
        or (v_row.work_kind='EXPENSE' and v_row.source_kind='EXPENSE' and v_row.financial_snapshot_id is not null
          and v_row.component_kind in ('EXPENSE','MILEAGE') and v_row.tax_treatment='NON_TAXABLE')),false) and
       (v_row.source_pay_channel is distinct from 'UMBRELLA'
        or v_row.work_kind not in ('ORDINARY','SOURCE')
        or v_row.component_kind is distinct from 'WORK'
        or v_row.tax_treatment is distinct from 'TAXABLE') then
      v_issue:='UMBRELLA_WORK_EXECUTION_UNSUPPORTED';
    end if;
    if v_issue is not null then
      v_issue_work_id:=v_member.work_id;exit;
    end if;
    -- Typed initial I-1 is the ONLY new no-HEAD binding arm. Initial and
    -- common-HEAD captures require self-canonical, exactly classified Source components.
    v_binding:=null;v_book:=null;
    if v_row.work_kind='SOURCE' and (v_row.source_head_id is not null
      or (v_row.source_kind='SOURCE' and v_effective_source_event_id is not null
        and v_row.financial_snapshot_id is not null)) then
      select v.* into v_binding from private.bpay_next_source_component_binding b
        join private.bpay_next_source_component_binding_version v on v.id=b.current_binding_version_id
        where b.canonical_work_id=v_row.work_id and b.canonical_component_key=v_row.component_key
          and v.approval_work_id=v_row.work_id and v.approval_revision_id=v_approval_revision_id
          and v.approval_component_key=v_row.component_key
          and ((v_row.source_head_id is not null and v.origin_kind='COMMON_HEAD_RECEIPT_V1'
              and v.head_id=v_row.source_head_id)
            or (v_row.source_head_id is null and v.origin_kind='INITIAL_AUTHORISED_TSFIN_V1'
              and v.initial_root_authorisation_id=v_effective_source_event_id
              and v.initial_financial_snapshot_id=v_row.financial_snapshot_id
              and v.initial_root_timesheet_id=v_row.physical_timesheet_id
              and v.initial_root_version=v_row.physical_timesheet_version));
      if v_binding.id is null or v_binding.publication_command_id_2 is not null
         or v_row.source_pay_channel not in ('PAYE','UMBRELLA')
         or not v_answered_source then
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
          select seed.* into strict v_seed from private.bpay_next_source_disposition_seed_v1(
            v_row.work_id,v_row.component_key,v_row.captured_revision_id,v_seed_effect,v_seed_hold) seed;
          if v_seed.qualification_state is distinct from 'READY' then
            raise exception using errcode='55000',message='BPAY_NEXT_SOURCE_BOOK_SELECTED_SEED_NOT_READY';
          end if;
          select p.source_book_state,p.source_book_revision into v_row.source_book_state,v_row.source_book_revision
            from private.bpay_next_position p where p.work_id=v_row.work_id and p.component_key=v_row.component_key;
        end if;
        select s.* into strict v_book from private.bpay_next_bound_source_snapshot_v1(
          v_binding.id,v_row.source_book_revision) s;
        if v_book.qualification_state is distinct from 'READY'
           or (v_book.candidate_id,v_book.canonical_work_id,v_book.canonical_component_key,
               v_book.approval_work_id,v_book.approval_revision_id,v_book.approved_line_id,
               v_book.source_channel,v_book.tax_treatment,v_book.approved_source_ex_vat,
               v_book.native_realised_paye,v_book.native_held_paye,v_book.native_realised_umbrella,v_book.native_held_umbrella)
             is distinct from (v_worker.candidate_id,v_row.work_id,v_row.component_key,
               v_row.work_id,v_approval_revision_id,v_approval_line_id,
               v_row.source_pay_channel,v_row.tax_treatment,v_row.approved_source_ex_vat,
               case when v_row.source_pay_channel='PAYE' then v_row.realised_source_ex_vat else 0::numeric end,
               case when v_row.source_pay_channel='PAYE' then v_row.held_source_ex_vat else 0::numeric end,
               case when v_row.source_pay_channel='UMBRELLA' then v_row.realised_source_ex_vat else 0::numeric end,
               case when v_row.source_pay_channel='UMBRELLA' then v_row.held_source_ex_vat else 0::numeric end) then
          v_issue:='SOURCE_BOOK_APPROVAL_UNBOUND';
        end if;
      end if;
      if v_issue is not null then
        v_issue_work_id:=v_row.work_id;exit;
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
        v_collection:=private.bpay_next_work_collection_origin_v1(
          v_collection.case_id,v_collection.case_component_id,v_worker.candidate_id);
      else v_collection:=null;end if;
      if v_collection.id is not null and
         (v_collection.observed_revision_id,v_collection.observed_approved_source_ex_vat,v_collection.observed_realised_source_ex_vat)
           is distinct from (v_row.captured_revision_id,v_row.approved_source_ex_vat,v_row.realised_source_ex_vat) then
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
       and v_effective_source_event_id is not null
       and v_row.component_kind in ('WORK','PROTECTED_WORK')
       and not v_needs_resolution
       and v_row.approved_source_ex_vat-v_row.realised_source_ex_vat-v_row.held_source_ex_vat>0 then
      select qp.* into v_query_pin from private.bpay_next_run_work_query_pin_v2 qp
        where qp.run_work_id=v_row.run_work_id;
      if found and (private.bpay_next_prepare_job_run_v1(v_query_pin.prepare_job_id) is distinct from v_run_id
        or (v_query_pin.run_worker_id,v_query_pin.candidate_id,
          v_query_pin.work_id,v_query_pin.captured_revision_id,
          v_query_pin.module_epoch,v_query_pin.gate_version)
          is distinct from (v_worker.id,v_worker.candidate_id,
          v_row.work_id,v_row.captured_revision_id,v_module_epoch,'PAY_QUERY_STATE_V3'::text)) then
        raise exception using errcode='23514',message='BPAY_NEXT_CAPTURE_QUERY_PIN_MISMATCH';
      end if;
      if not found or v_query_pin.disposition='UNAVAILABLE' then
        v_issue:='SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE';
      elsif v_query_pin.disposition='BLOCKED' then
        -- Only the original cut's complete, exact named shift exclusions
        -- can bypass a whole-Work blocker. Never read today's query here.
        if not exists(select 1 from private.bpay_next_run_work_query_snapshot snapshot
          where snapshot.run_id=v_run_id and snapshot.work_id=v_row.work_id
            and snapshot.expected_revision_id=v_row.captured_revision_id
            and snapshot.physical_timesheet_id::text=(v_query_pin.scope->>'root_timesheet_id')
            and jsonb_typeof(snapshot.segment_query_exclusions)='array'
            and jsonb_array_length(snapshot.segment_query_exclusions)>0)
        then v_issue:='SOURCE_PAY_QUERY_OPEN';end if;
      elsif v_query_pin.disposition<>'CLEAR' or v_query_pin.scope is null
         or v_query_pin.query_state_sha256 is null
         or pg_catalog.octet_length(v_query_pin.query_state_sha256)<>32 then
        raise exception using errcode='23514',message='BPAY_NEXT_CAPTURE_QUERY_PIN_INVALID';
      end if;
      if v_issue is not null then
        v_issue_work_id:=v_row.work_id;exit;
      end if;
    end if;
    select count(*) into v_segments from (
      select s.segment_key from private.bpay_next_approved_segment s
        where s.revision_id=v_row.captured_revision_id and s.component_key=v_row.component_key
      union select s.segment_key from private.bpay_next_work_segment_origin_share s
        where s.work_id=v_row.work_id and s.component_key=v_row.component_key) members;
    if v_segments>0 and v_row.work_kind='ORDINARY' then
      -- Exact segment negative ownership is sealed after complete child capture.
      -- Initial native net-negative classification cannot bypass that census.
      if v_collection.id is not null then v_needs_resolution:=true;end if;
      v_collection:=null;
    end if;
    v_snooze:=null;
    if v_segments=0 then
      v_snooze:=private.bpay_next_snooze_at_cut_v1(v_row.work_id,v_row.captured_revision_id,
        null,v_row.component_key,v_run.snooze_control_sequence,v_run.snooze_business_date);
    end if;
    insert into private.bpay_next_run_position
      (run_worker_id,run_work_id,work_id,captured_revision_id,
       component_key,approved_line_id,source_pay_channel,
       source_basis_channel,
       approved_source_ex_vat,realised_source_ex_vat,held_source_ex_vat,
       residual_source_ex_vat,realised_target_ex_vat,realised_target_vat,
       realised_target_inc_vat,held_target_ex_vat,held_target_vat,
       held_target_inc_vat,needs_financial_resolution,work_collection_id,work_collection_revision,
       source_binding_version_id,canonical_source_work_id,canonical_source_component_key,source_book_revision,
       segment_expected_member_count,segment_attribution_complete,snooze_id,snooze_version,snooze_excluded_positive_ex_vat)
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
       case when v_binding.id is null then null else v_row.source_book_revision end,
       v_segments,v_segments=0,(v_snooze->>'id')::uuid,(v_snooze->>'version')::bigint,
       case when v_snooze is not null then greatest(v_row.approved_source_ex_vat-v_row.realised_source_ex_vat-v_row.held_source_ex_vat,0) else 0 end);
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
  update private.bpay_next_job set position_work_cursor=v_work_cursor,position_component_cursor=v_component_cursor where id=p_job_id;
  if v_issue is not null then
    update private.bpay_next_run_worker set status='REVIEW',review_issue_code=v_issue,review_issue_work_id=v_issue_work_id where id=v_worker.id;
    return private.bpay_next_begin_prepare_projection_v1(p_job_id,p_lease_nonce,p_owner_epoch,v_seen);
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
      return private.bpay_next_begin_prepare_projection_v1(p_job_id,p_lease_nonce,p_owner_epoch,v_seen);
    end if;
    v_segment_result:=private.bpay_next_capture_segment_page_v1(v_worker.id,p_limit-v_seen);
    v_segment_visited:=(v_segment_result->>'rows_visited')::integer;
    select w.* into strict v_worker from private.bpay_next_run_worker w where w.id=v_worker.id;
    if v_worker.status='REVIEW' then
      return private.bpay_next_begin_prepare_projection_v1(p_job_id,p_lease_nonce,p_owner_epoch,v_seen+v_segment_visited);
    end if;
    if not (v_segment_result->>'done')::boolean then
      update private.bpay_next_job set position_work_cursor=v_work_cursor,position_component_cursor=v_component_cursor where id=p_job_id;
      return jsonb_build_object('done',false,'phase','RESERVE','rows_visited',v_seen+v_segment_visited,
        'work_cursor',v_work_cursor,'component_cursor',v_component_cursor,'replay',false);
    end if;
    update private.bpay_next_job
      set phase='ALLOCATE',position_work_cursor=null,
          position_component_cursor=null
      where id=p_job_id;
    return pg_catalog.jsonb_build_object('done',true,'phase','ALLOCATE',
      'rows_visited',v_seen+v_segment_visited,'replay',false);
  end if;
  update private.bpay_next_job
    set position_work_cursor=v_work_cursor,
        position_component_cursor=v_component_cursor
    where id=p_job_id;
  return pg_catalog.jsonb_build_object('done',false,'phase','RESERVE',
    'rows_visited',v_seen+v_segment_visited,'work_cursor',v_work_cursor,
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
  v_approval_revision_id uuid;v_approval_line_id uuid;v_choice_refs jsonb;v_line_refs jsonb;v_selected_quote jsonb;
  v_run_id uuid;
  v_run private.bpay_next_pay_run%rowtype;
  v_job private.bpay_next_job%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_module_epoch bigint;
  v_issue_work_id uuid;
  v_row record;
  v_position private.bpay_next_position%rowtype;
  v_value record;v_book record;
  v_reimbursement_window uuid;v_reimbursement_vat_pct numeric;v_reimbursement_window_count bigint;v_reimbursement_amounts jsonb;
  v_run_line_id uuid;v_hold_id uuid;v_included_source numeric(18,2);v_freeze_zero boolean;
  v_seen integer:=0;
  v_affected_work_ids uuid[]:=array[]::uuid[];
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
  select private.bpay_next_prepare_job_run_v1(j.id) into strict v_run_id
    from private.bpay_next_job j
    where j.id=p_job_id and j.job_kind='PREPARE';
  select m.owner_epoch into v_module_epoch
    from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_module_epoch is null then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select dr38_point_7.* into strict v_run from private.bpay_next_pay_run dr38_point_7
    where id=v_run_id for update;
  select candidate_id into strict v_job.candidate_id
    from private.bpay_next_job where id=p_job_id;
  perform 1 from private.bpay_next_worker_control
    where candidate_id=v_job.candidate_id for update;
  select dr38_point_8.* into strict v_job from private.bpay_next_job dr38_point_8
    where id=p_job_id for update;
  if not private.bpay_next_prepare_job_active_v1(p_job_id) then
    raise exception using errcode='55000',message='BPAY_NEXT_WORKER_ACTION_REVISION_STALE';end if;
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
  select dr38_point_9.* into strict v_worker from private.bpay_next_run_worker dr38_point_9
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
    return private.bpay_next_begin_prepare_projection_v1(p_job_id,p_lease_nonce,p_owner_epoch,0);
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
      return private.bpay_next_begin_prepare_projection_v1(p_job_id,p_lease_nonce,p_owner_epoch,0);
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
    select rp.*,rw.id as pinned_work_id,rev.valuation_policy_id,rev.source_kind as captured_source_kind,
      rev.financial_snapshot_id as captured_financial_snapshot_id,
      w.work_kind as captured_work_kind,l.component_kind as captured_component_kind,l.tax_treatment as captured_tax_treatment
      from private.bpay_next_run_position rp
      join private.bpay_next_run_work rw on rw.id=rp.run_work_id
      join private.bpay_next_work_revision rev
        on rev.id=rp.captured_revision_id and rev.work_id=rp.work_id
      join private.bpay_next_work w on w.id=rp.work_id
      left join private.bpay_next_approved_line l on l.id=rp.approved_line_id
        and l.revision_id=rp.captured_revision_id and l.component_key=rp.component_key
      where rp.run_worker_id=v_worker.id
        and (v_work_cursor is null or rp.work_id>v_work_cursor
          or (rp.work_id=v_work_cursor
            and rp.component_key>v_component_cursor))
      order by rp.work_id,rp.component_key limit p_limit
  loop
    if not v_row.segment_attribution_complete then
      raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_CAPTURE_INCOMPLETE';
    end if;
    v_included_source:=case when v_row.segment_member_count>0 then v_row.segment_included_positive_ex_vat
      else greatest(v_row.residual_source_ex_vat,0)-v_row.snooze_excluded_positive_ex_vat end;
    v_freeze_zero:=v_row.approved_line_id is not null and v_row.approved_source_ex_vat=0 and v_row.realised_source_ex_vat=0 and v_row.held_source_ex_vat=0
      and v_row.snooze_id is null and (v_row.segment_member_count=0 or exists(
        select 1 from private.bpay_next_run_work_segment_parent p join private.bpay_next_run_work_segment s on s.id=p.run_work_segment_id
          where p.run_work_id=v_row.run_work_id and p.component_key=v_row.component_key and s.disposition='INCLUDED'));
    if v_row.segment_member_count=0 and not private.bpay_next_choice_leaf_eligible_v1(
      v_run.id,v_row.work_id,v_row.captured_revision_id,v_row.approved_line_id,null) then
      -- Complete A/R/H remains captured; an unselected component supplies no pay.
      v_included_source:=0;v_freeze_zero:=false;
    end if;
    if v_row.needs_financial_resolution
       or (v_row.work_collection_id is null and
         ((v_row.approved_line_id is null and (v_row.segment_member_count=0 or v_included_source>0)) or v_row.residual_source_ex_vat<0)) then
      raise exception using errcode='23514',
        message='BPAY_NEXT_HOLD_UNEXPECTED_POSITION_SHAPE';
    end if;
    select dr38_point_10.* into strict v_position from private.bpay_next_position dr38_point_10
      where work_id=v_row.work_id and component_key=v_row.component_key
      for update;
    if v_position.applied_revision_id is distinct from v_row.captured_revision_id
       or v_position.approved_source_ex_vat is distinct from
         v_row.approved_source_ex_vat
       or v_position.realised_source_ex_vat is distinct from
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
      v_approval_revision_id:=private.bpay_next_treatment_approval_revision_v1(v_row.captured_revision_id);
      select a.id into v_approval_line_id from private.bpay_next_approved_line a
        where a.revision_id=v_approval_revision_id and a.component_key=v_row.component_key;
      select s.* into strict v_book from private.bpay_next_bound_source_snapshot_v1(
        v_row.source_binding_version_id,v_row.source_book_revision) s;
      if v_book.qualification_state is distinct from 'READY'
         or (v_book.canonical_work_id,v_book.canonical_component_key,v_book.approval_work_id,
             v_book.approval_revision_id,v_book.approved_line_id,v_book.source_channel,v_book.tax_treatment,
             v_book.approved_source_ex_vat,v_book.native_realised_paye,v_book.native_held_paye,
             v_book.native_realised_umbrella,v_book.native_held_umbrella)
           is distinct from (v_row.work_id,v_row.component_key,v_row.work_id,
             v_approval_revision_id,v_approval_line_id,v_row.source_pay_channel,v_row.captured_tax_treatment,
             v_row.approved_source_ex_vat,
             case when v_row.source_pay_channel='PAYE' then v_row.realised_source_ex_vat else 0::numeric end,
             case when v_row.source_pay_channel='PAYE' then v_row.held_source_ex_vat else 0::numeric end,
             case when v_row.source_pay_channel='UMBRELLA' then v_row.realised_source_ex_vat else 0::numeric end,
             case when v_row.source_pay_channel='UMBRELLA' then v_row.held_source_ex_vat else 0::numeric end)
         or v_row.source_pay_channel not in ('PAYE','UMBRELLA')
         or coalesce(v_row.captured_work_kind='SOURCE'
           and ((v_row.captured_component_kind in ('WORK','ADDITIONAL') and v_row.captured_tax_treatment='TAXABLE')
             or (v_row.captured_component_kind in ('EXPENSE','MILEAGE') and v_row.captured_tax_treatment='NON_TAXABLE')),false) is not true
         or v_row.canonical_source_work_id is distinct from v_row.work_id
         or v_row.canonical_source_component_key is distinct from v_row.component_key then
        raise exception using errcode='55000',message='BPAY_NEXT_SOURCE_BOOK_CAPTURE_CHANGED';
      end if;
    end if;
    select c.accepted_resolution_refs into v_choice_refs from private.bpay_next_work_choice c
      where c.run_id=v_run.id and c.work_id=v_row.work_id and c.expected_revision_id=v_row.captured_revision_id;
    v_line_refs:=null;
    if v_choice_refs is not null then
      select coalesce(jsonb_agg(x.value order by x.value->>'rate_family',x.value->>'rate_code'),'[]') into v_line_refs
        from jsonb_array_elements(v_choice_refs) x where (x.value->>'approved_line_id')::uuid=v_row.approved_line_id;
    end if;
    if v_row.work_collection_id is not null then
      -- Classification and original provenance were frozen by RESERVE. Never
      -- reread a newer WORK approval or create an ordinary negative/zero hold.
      update private.bpay_next_run_position set managed_capture_completed=true
        where id=v_row.id and not managed_capture_completed;
      if not found then raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_CAPTURE_ALREADY_SPENT';end if;
      v_managed_count:=v_managed_count+1;
    end if;
    if v_included_source>0 or v_freeze_zero then
    if ((v_row.source_binding_version_id is not null and v_row.captured_work_kind='SOURCE')
        or v_row.captured_work_kind='ORDINARY'
        or (v_row.captured_work_kind='EXPENSE' and v_row.captured_source_kind='EXPENSE'
          and v_row.captured_financial_snapshot_id is not null))
      and v_row.captured_component_kind in ('EXPENSE','MILEAGE') and v_row.captured_tax_treatment='NON_TAXABLE' then
      -- A classified ordinary/Source reimbursement keeps source ex; only ordinary line VAT applies.
      -- Use the same immutable winning policy window and existing VAT calculator.
      if v_included_source is null or v_row.valuation_policy_id is null or v_run.pay_date is null
        or v_row.source_pay_channel is null or v_row.source_pay_channel not in ('PAYE','UMBRELLA')
        or v_worker.target_pay_channel is null or v_worker.target_pay_channel not in ('PAYE','UMBRELLA')
        or (v_worker.target_pay_channel='UMBRELLA' and v_worker.target_umbrella_vat_chargeable is null)
        or v_included_source<>pg_catalog.round(v_included_source,2) then
        raise exception using errcode='22023',message='BPAY_NEXT_TARGET_VALUATION_INPUT_INVALID';end if;
      v_reimbursement_window:=null;v_reimbursement_vat_pct:=null;v_reimbursement_window_count:=null;
      select w.source_window_id,w.vat_rate_pct,count(*) over(partition by w.date_from)
        into v_reimbursement_window,v_reimbursement_vat_pct,v_reimbursement_window_count
        from private.bpay_next_valuation_policy_window w
        where w.policy_id=v_row.valuation_policy_id and v_run.pay_date>=w.date_from
          and v_run.pay_date<=coalesce(w.date_to,'infinity'::date)
        order by w.date_from desc,w.source_window_id desc limit 1;
      if v_reimbursement_window is null then
        raise exception using errcode='23514',message='BPAY_NEXT_TARGET_POLICY_WINDOW_MISSING';end if;
      if v_reimbursement_window_count<>1 then
        raise exception using errcode='23514',message='BPAY_NEXT_TARGET_POLICY_WINDOW_AMBIGUOUS';end if;
      if v_worker.target_pay_channel='UMBRELLA' then
        v_reimbursement_amounts:=public._pay_umbrella_vat_calc(v_included_source,
          v_reimbursement_vat_pct,v_worker.target_umbrella_vat_chargeable);
        select v_reimbursement_window as policy_window_id,v_included_source as target_ex_vat,
          (v_reimbursement_amounts->>'vat')::numeric as target_vat,
          (v_reimbursement_amounts->>'inc')::numeric as target_inc_vat into strict v_value;
      else
        select v_reimbursement_window as policy_window_id,v_included_source as target_ex_vat,
          0::numeric as target_vat,v_included_source as target_inc_vat into strict v_value;
      end if;
    elsif v_line_refs is not null and jsonb_array_length(v_line_refs)>0 then
      v_selected_quote:=private.bpay_next_captured_component_resolution_quote_v1(v_row.id,null);
      if v_selected_quote->'accepted_resolution_refs' is distinct from v_line_refs
        or (v_selected_quote->>'included_source_component_ex_vat')::numeric is distinct from v_included_source then
        raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_ACCEPTED_BASIS_CHANGED';end if;
      select (v_selected_quote->>'valuation_window_id')::uuid as policy_window_id,
        (v_selected_quote#>>'{component_quote,ex}')::numeric as target_ex_vat,
        (v_selected_quote#>>'{component_quote,vat}')::numeric as target_vat,
        (v_selected_quote#>>'{component_quote,inc}')::numeric as target_inc_vat into strict v_value;
    else
    select * into strict v_value
      from private.bpay_next_value_source_for_target_v1(
        v_included_source,v_row.source_pay_channel,
        v_worker.target_pay_channel,
        case when v_worker.target_pay_channel='UMBRELLA'
          then v_worker.target_umbrella_vat_chargeable else false end,
        v_row.valuation_policy_id,v_run.pay_date);
    end if;
    v_line_count:=v_line_count+1;
    insert into private.bpay_next_run_line
      (run_worker_id,run_work_id,line_no,approved_line_id,
       captured_revision_id,work_id,component_key,source_consumed_ex_vat,
       source_pay_channel,target_pay_channel,valuation_policy_id,
       policy_window_id,frozen_ex_vat,frozen_vat,frozen_inc_vat,source_binding_version_id,
       canonical_source_work_id,canonical_source_component_key,source_book_revision,updates_original_source_scalar,accepted_resolution_refs)
      values(v_worker.id,v_row.run_work_id,v_line_count,
       v_row.approved_line_id,v_row.captured_revision_id,v_row.work_id,
       v_row.component_key,v_included_source,
       v_row.source_pay_channel,v_worker.target_pay_channel,
       v_row.valuation_policy_id,v_value.policy_window_id,
       v_value.target_ex_vat,v_value.target_vat,v_value.target_inc_vat,
       v_row.source_binding_version_id,v_row.canonical_source_work_id,v_row.canonical_source_component_key,
       v_row.source_book_revision,case when v_row.source_binding_version_id is null then null else true end,v_line_refs)
      returning id into v_run_line_id;
    -- An authorised zero still has immutable shift/rate advice detail. It
    -- becomes a frozen zero line, never a zero-valued financial hold.
    if v_included_source>0 then
      insert into private.bpay_next_hold
        (run_line_id,work_id,component_key,source_reserved_ex_vat,
         target_amount_ex_vat,target_amount_vat,target_amount_inc_vat,status)
        values(v_run_line_id,v_row.work_id,v_row.component_key,
         v_included_source,v_value.target_ex_vat,
         v_value.target_vat,v_value.target_inc_vat,'ACTIVE') returning id into v_hold_id;
      perform private.bpay_next_record_work_segment_origin_shares_v1(v_run_line_id,v_hold_id);
      update private.bpay_next_position
        set held_source_ex_vat=held_source_ex_vat+v_included_source,
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
    v_affected_work_ids:=array_append(v_affected_work_ids,v_row.work_id);
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
    perform private.bpay_next_workbench_refresh_many_v1(v_affected_work_ids,null);
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
create or replace function private.bpay_next_record_work_segment_origin_shares_v1(p_run_line_id uuid,p_hold_id uuid)
returns void language plpgsql volatile security invoker set search_path=pg_catalog,private
as $function$
declare
  v_line private.bpay_next_run_line%rowtype; v_hold private.bpay_next_hold%rowtype;
  v_position private.bpay_next_run_position%rowtype; v_worker private.bpay_next_run_worker%rowtype;
  v_run private.bpay_next_pay_run%rowtype; v_total record; v_expected bigint;v_selected_quote jsonb;
begin
  select l.* into strict v_line from private.bpay_next_run_line l where l.id=p_run_line_id;
  select h.* into strict v_hold from private.bpay_next_hold h where h.id=p_hold_id;
  select p.* into strict v_position from private.bpay_next_run_position p
    where p.run_work_id=v_line.run_work_id and p.component_key=v_line.component_key;
  if (v_hold.run_line_id,v_hold.work_id,v_hold.component_key,v_hold.source_reserved_ex_vat,
      v_hold.target_amount_ex_vat,v_hold.target_amount_vat,v_hold.target_amount_inc_vat,v_hold.status)
    is distinct from (v_line.id,v_line.work_id,v_line.component_key,v_line.source_consumed_ex_vat,
      v_line.frozen_ex_vat,v_line.frozen_vat,v_line.frozen_inc_vat,'ACTIVE'::text) then
    raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_HOLD_PARENT_MISMATCH';
  end if;
  if v_position.segment_member_count=0 then return; end if;
  if not v_position.segment_attribution_complete or v_position.segment_included_positive_ex_vat<>v_hold.source_reserved_ex_vat then
    raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_HOLD_CAPTURE_INCOMPLETE';
  end if;
  select w.* into strict v_worker from private.bpay_next_run_worker w where w.id=v_line.run_worker_id;
  select r.* into strict v_run from private.bpay_next_pay_run r where r.id=v_worker.run_id;
  -- One completed captured parent; no live approval/history or NET allocation.
  -- Shared gross valuation of cumulative prefixes conserves the parent's pennies.
  if v_line.accepted_resolution_refs is not null and jsonb_array_length(v_line.accepted_resolution_refs)>0 then
    -- One complete native gather and cumulative bucket plan, never one gather per prefix.
    v_selected_quote:=private.bpay_next_captured_component_resolution_quote_v1(v_position.id,null);
    if v_selected_quote->'accepted_resolution_refs' is distinct from v_line.accepted_resolution_refs
      or ((v_selected_quote#>>'{component_quote,ex}')::numeric,(v_selected_quote#>>'{component_quote,vat}')::numeric,
          (v_selected_quote#>>'{component_quote,inc}')::numeric)
        is distinct from (v_line.frozen_ex_vat,v_line.frozen_vat,v_line.frozen_inc_vat) then
      raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_HOLD_SHARE_TOTAL_MISMATCH';end if;
    with prefixes as materialized (
      select x.segment_key,x.run_work_segment_id,x.source_ex_vat,
        (x.quote->>'ex')::numeric target_ex,(x.quote->>'vat')::numeric target_vat
      from jsonb_to_recordset(v_selected_quote->'prefix_targets') as x(segment_key text,run_work_segment_id uuid,source_ex_vat numeric,quote jsonb)
    ), shares as (
      select p.*,p.target_ex-coalesce(lag(p.target_ex) over(order by p.segment_key),0) share_ex,
        p.target_vat-coalesce(lag(p.target_vat) over(order by p.segment_key),0) share_vat from prefixes p
    )
    insert into private.bpay_next_work_segment_origin_share(hold_id,run_line_id,run_work_segment_id,
      work_id,component_key,segment_key,source_reserved_ex_vat,target_reserved_ex_vat,target_reserved_vat,target_reserved_inc_vat)
    select v_hold.id,v_line.id,p.run_work_segment_id,v_line.work_id,v_line.component_key,p.segment_key,
      p.source_ex_vat,p.share_ex,p.share_vat,p.share_ex+p.share_vat from shares p;
  else
  with members as materialized (
    select p.*,sum(p.included_positive_source_ex_vat) over(order by p.segment_key) cumulative_source
    from private.bpay_next_run_work_segment_parent p where p.run_work_id=v_line.run_work_id
      and p.component_key=v_line.component_key and p.included_positive_source_ex_vat>0
  ), quoted as materialized (
    select p.*,q.target_ex_vat,q.target_vat,q.target_inc_vat from members p
    cross join lateral private.bpay_next_value_source_for_target_v1(p.cumulative_source,v_line.source_pay_channel,
      v_line.target_pay_channel,case when v_line.target_pay_channel='UMBRELLA'
        then v_worker.target_umbrella_vat_chargeable else false end,v_line.valuation_policy_id,v_run.pay_date) q
  ), shares as (
    select p.*,p.target_ex_vat-coalesce(lag(p.target_ex_vat) over(order by p.segment_key),0) share_ex,
      p.target_vat-coalesce(lag(p.target_vat) over(order by p.segment_key),0) share_vat
    from quoted p
  )
  insert into private.bpay_next_work_segment_origin_share(hold_id,run_line_id,run_work_segment_id,
    work_id,component_key,segment_key,source_reserved_ex_vat,target_reserved_ex_vat,target_reserved_vat,target_reserved_inc_vat)
  select v_hold.id,v_line.id,p.run_work_segment_id,v_line.work_id,v_line.component_key,p.segment_key,
    p.included_positive_source_ex_vat,p.share_ex,p.share_vat,p.share_ex+p.share_vat from shares p;
  end if;
  select count(*),sum(s.source_reserved_ex_vat) source_total,sum(s.target_reserved_ex_vat) target_ex,
    sum(s.target_reserved_vat) target_vat,sum(s.target_reserved_inc_vat) target_inc into v_total
    from private.bpay_next_work_segment_origin_share s where s.hold_id=v_hold.id;
  select count(*) into v_expected from private.bpay_next_run_work_segment_parent p
    where p.run_work_id=v_line.run_work_id and p.component_key=v_line.component_key and p.included_positive_source_ex_vat>0;
  if (v_total.count,v_total.source_total,v_total.target_ex,v_total.target_vat,v_total.target_inc)
    is distinct from (v_expected,v_hold.source_reserved_ex_vat,v_hold.target_amount_ex_vat,v_hold.target_amount_vat,v_hold.target_amount_inc_vat) then
    raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_HOLD_SHARE_TOTAL_MISMATCH';
  end if;
end
$function$;
alter function private.bpay_next_capture_position_page_v1(uuid,uuid,bigint,uuid,text,integer) owner to postgres;
revoke all on function private.bpay_next_capture_position_page_v1(uuid,uuid,bigint,uuid,text,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_hold_fresh_position_page_v1(uuid,uuid,bigint,uuid,text,integer) owner to postgres;
revoke all on function private.bpay_next_hold_fresh_position_page_v1(uuid,uuid,bigint,uuid,text,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_record_work_segment_origin_shares_v1(uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_record_work_segment_origin_shares_v1(uuid,uuid) from public,anon,authenticated,service_role;
commit;
