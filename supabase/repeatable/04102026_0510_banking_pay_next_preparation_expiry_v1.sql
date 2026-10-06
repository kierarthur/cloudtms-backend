-- Actual pre-confirmation expiry, with fenced metadata enrollment and ordered
-- bounded WORK/CASE release. No Draft/payment/CSV/provider/internal expiry.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_preparation_expiry_fenced_v1(p_run_id uuid,p_prepare_command_id uuid)
returns boolean language sql stable security invoker set search_path=pg_catalog,private
as $function$
  select exists(select 1 from private.bpay_next_pay_run r
    join private.bpay_next_preparation_expiry_request x on x.run_id=r.id
    where r.id=p_run_id and r.status in ('CANCELLING','CANCELLED') and r.confirmed_at_utc is null
      and x.status in ('CANCELLING','EXPIRED') and x.original_prepare_command_id=p_prepare_command_id
      and x.captured_deadline=r.preparation_expires_at_utc
      and x.selection_count=r.selection_count and x.selection_page_count=r.selection_page_count
      and x.expected_candidate_count=r.selected_candidate_count
      and r.review_revision=x.selection_review_revision+1
      and (r.status='CANCELLING' or (x.status='EXPIRED' and x.completed_candidate_count=x.expected_candidate_count
        and x.enrolled_candidate_count=x.expected_candidate_count)))
$function$;

create or replace function private.bpay_next_accept_preparation_expiry_v1(
  p_command_id uuid,p_run_id uuid,p_actor_user_id uuid,p_expected_deadline timestamptz,
  p_observed_now timestamptz default null)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private,public
as $function$
declare v_epoch bigint;v_run private.bpay_next_pay_run%rowtype;
  v_request private.bpay_next_preparation_expiry_request%rowtype;v_sequence bigint;v_prepare uuid;v_now timestamptz;
begin
  if p_command_id is null or p_run_id is null or p_expected_deadline is null
     or not pg_catalog.isfinite(p_expected_deadline)
     or (p_observed_now is not null and not pg_catalog.isfinite(p_observed_now)) then
    raise exception using errcode='22023',message='BPAY_NEXT_EXPIRY_INPUT_INVALID';end if;
  select owner_epoch into v_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('bpay-next-preparation-expiry:'||p_command_id::text,0));
  select * into strict v_run from private.bpay_next_pay_run where id=p_run_id for update;
  select * into v_request from private.bpay_next_preparation_expiry_request where command_id=p_command_id;
  if found then
    if v_request.run_id<>p_run_id or v_request.actor_user_id is distinct from p_actor_user_id
       or v_request.captured_deadline<>p_expected_deadline
       or not exists(select 1 from private.bpay_next_command where id=p_command_id
         and command_kind='PREPARATION_EXPIRY' and module_epoch=v_epoch) then
      raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_REPLAY_CONFLICT';end if;
    select agency_sequence into strict v_sequence from private.bpay_next_command where id=p_command_id;
    return pg_catalog.jsonb_build_object('command_id',p_command_id,'run_id',p_run_id,'sequence',v_sequence::text,
      'phase',v_request.status,'expected_candidates',v_request.expected_candidate_count::text,
      'completed_candidates',v_request.completed_candidate_count::text,'replay',true);
  end if;
  -- Public wrappers MUST pass NULL, never a browser/request-supplied clock.
  -- Obtain real time after waiting for this same header as confirmation.
  -- The private ungranted operand permits labelled native 72h clock vectors.
  v_now:=coalesce(p_observed_now,pg_catalog.clock_timestamp());
  if p_actor_user_id is not null and not exists(select 1 from public.tms_users
      where id=p_actor_user_id and is_active is true and role::text='admin' for share) then
    raise exception using errcode='42501',message='BPAY_NEXT_EXPIRY_ACTOR_FORBIDDEN';end if;
  if v_run.confirmed_at_utc is not null or v_run.status not in ('PREPARING','REVIEW') then
    raise exception using errcode='55000',message='BPAY_NEXT_EXPIRY_NOT_UNCONFIRMED';end if;
  if v_run.preparation_expires_at_utc is null then
    raise exception using errcode='55000',message='BPAY_NEXT_EXPIRY_DEADLINE_UNBOUND';end if;
  if v_run.preparation_expires_at_utc<>p_expected_deadline then
    raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_DEADLINE_CONFLICT';end if;
  if v_now<v_run.preparation_expires_at_utc then
    raise exception using errcode='55000',message='BPAY_NEXT_EXPIRY_NOT_DUE';end if;
  if exists(select 1 from private.bpay_next_preparation_expiry_request where run_id=p_run_id) then
    raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_RUN_ALREADY_BOUND';end if;
  select command_id into v_prepare from private.bpay_next_run_command where run_id=p_run_id;
  if v_prepare is not null and not exists(select 1 from private.bpay_next_command
      where id=v_prepare and command_kind='PREPARE' and module_epoch=v_epoch
        and status in ('ENROLLING','SEALED') and expected_member_count=v_run.selected_candidate_count) then
    raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_PREPARE_BINDING_INVALID';end if;
  v_sequence:=private.bpay_next_receive_command_v1(p_command_id,'PREPARATION_EXPIRY');
  insert into private.bpay_next_preparation_expiry_request(command_id,run_id,original_prepare_command_id,
    captured_deadline,selection_count,selection_page_count,selection_review_revision,expected_candidate_count,
    initiator_kind,actor_user_id,accepted_at_utc,status,finished_at_utc)
    values(p_command_id,p_run_id,v_prepare,p_expected_deadline,v_run.selection_count,v_run.selection_page_count,v_run.review_revision,
      v_run.selected_candidate_count,case when p_actor_user_id is null then 'SYSTEM' else 'OFFICE' end,p_actor_user_id,v_now,
      case when v_run.selected_candidate_count=0 then 'EXPIRED' else 'CANCELLING' end,
      case when v_run.selected_candidate_count=0 then v_now else null end);
  update private.bpay_next_pay_run set status=case when selected_candidate_count=0 then 'CANCELLED' else 'CANCELLING' end,
    review_revision=review_revision+1 where id=p_run_id;
  update private.bpay_next_command set expected_member_count=v_run.selected_candidate_count,
    status=case when v_run.selected_candidate_count=0 then 'COMPLETE' else 'ENROLLING' end,
    sealed_at_utc=case when v_run.selected_candidate_count=0 then v_now else null end where id=p_command_id;
  return pg_catalog.jsonb_build_object('command_id',p_command_id,'run_id',p_run_id,'sequence',v_sequence::text,
    'phase',case when v_run.selected_candidate_count=0 then 'EXPIRED' else 'CANCELLING' end,
    'expected_candidates',v_run.selected_candidate_count::text,'completed_candidates','0','replay',false);
end
$function$;

-- Called ONLY by the existing ordered1430 enroller, one Candidate per call.
-- The root fence has already stopped every PREPARE financial writer. Retiring
-- an unfinished original job here is metadata, not bypassing another command.
create or replace function private.bpay_next_enroll_preparation_expiry_one_v1(p_command_id uuid)
returns uuid language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_run_id uuid;v_epoch bigint;v_run private.bpay_next_pay_run%rowtype;
  v_request private.bpay_next_preparation_expiry_request%rowtype;v_command private.bpay_next_command%rowtype;
  v_original private.bpay_next_job%rowtype;v_worker private.bpay_next_run_worker%rowtype;
  v_candidate uuid;v_member bigint;v_job uuid;v_state uuid;
begin
  select owner_epoch into v_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select run_id into strict v_run_id from private.bpay_next_preparation_expiry_request where command_id=p_command_id;
  select * into strict v_run from private.bpay_next_pay_run where id=v_run_id for share;
  select * into strict v_request from private.bpay_next_preparation_expiry_request where command_id=p_command_id for update;
  select * into strict v_command from private.bpay_next_command where id=p_command_id for update;
  if v_command.command_kind<>'PREPARATION_EXPIRY' or v_command.module_epoch<>v_epoch
     or v_command.expected_member_count<>v_request.expected_candidate_count
     or v_command.enrolled_member_count<>v_request.enrolled_candidate_count
     or v_run.confirmed_at_utc is not null or v_run.preparation_expires_at_utc<>v_request.captured_deadline
     or v_run.selection_count<>v_request.selection_count or v_run.selection_page_count<>v_request.selection_page_count
     or v_run.selected_candidate_count<>v_request.expected_candidate_count
     or v_run.review_revision<>v_request.selection_review_revision+1 then
    raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_ENROLLMENT_SCOPE_INVALID';end if;
  if v_request.expected_candidate_count=0 then
    if v_run.status<>'CANCELLED' or v_request.status<>'EXPIRED' or v_command.status<>'COMPLETE' then
      raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_EMPTY_NOT_COMPLETE';end if;
    return null;
  end if;
  if v_run.status<>'CANCELLING' or v_request.status<>'CANCELLING' or v_command.status<>'ENROLLING'
     or v_request.enrolled_candidate_count>=v_request.expected_candidate_count then
    raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_NOT_ENROLLABLE';end if;
  v_member:=v_request.enrolled_candidate_count+1;
  select candidate_id into strict v_candidate from private.bpay_next_selection_candidate where run_id=v_run_id and member_no=v_member;
  if v_request.original_prepare_command_id is not null then
    select * into strict v_original from private.bpay_next_job where command_id=v_request.original_prepare_command_id
      and candidate_id=v_candidate and job_kind='PREPARE' for update;
    if v_original.module_epoch<>v_epoch then raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_ORIGINAL_JOB_INVALID';end if;
    if v_original.status<>'DONE' then
      update private.bpay_next_job set status='DONE',phase='EXPIRED',lease_nonce=null,lease_until_utc=null,
        last_error_code='BPAY_NEXT_PREPARATION_EXPIRED' where id=v_original.id;
    end if;
  end if;
  select * into v_worker from private.bpay_next_run_worker where run_id=v_run_id and candidate_id=v_candidate;
  if v_worker.id is not null then
    if v_original.id is null or v_worker.status not in ('PREPARING','REVIEW','READY') then
      raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_WORKER_NOT_PREPARATION';end if;
    select id into v_state from private.bpay_next_case_allocation_state where run_worker_id=v_worker.id
      and preparation_revision=v_worker.preparation_revision and selection_revision=v_worker.case_selection_revision
      and pass_kind='DRAFT' and projection_no=0;
  end if;
  insert into private.bpay_next_command_member(command_id,candidate_id,member_no) values(p_command_id,v_candidate,v_member);
  insert into private.bpay_next_job(command_id,command_sequence,module_epoch,candidate_id,job_kind,status,phase,owner_epoch)
    values(p_command_id,v_command.agency_sequence,v_epoch,v_candidate,'PREPARATION_EXPIRY','READY','NEW',1) returning id into v_job;
  insert into private.bpay_next_preparation_expiry_worker(command_id,candidate_id,run_id,member_no,job_id,run_worker_id,
    original_job_id,original_job_status,original_job_phase,original_job_cursor,original_position_work_cursor,
    original_position_component_cursor,original_applied_line_count,original_worker_status,original_review_issue_code,
    original_review_issue_work_id,preparation_revision,selection_revision,draft_state_id,expected_work_count,expected_case_hold_count,status,stage)
    values(p_command_id,v_candidate,v_run_id,v_member,v_job,v_worker.id,v_original.id,v_original.status,v_original.phase,v_original.cursor_key,
      v_original.position_work_cursor,v_original.position_component_cursor,v_original.applied_line_count,v_worker.status,
      v_worker.review_issue_code,v_worker.review_issue_work_id,v_worker.preparation_revision,v_worker.case_selection_revision,v_state,
      coalesce(v_worker.captured_line_count,0),coalesce(v_worker.active_case_hold_count,0),'READY','NEW');
  update private.bpay_next_preparation_expiry_request set enrolled_candidate_count=v_member where command_id=p_command_id;
  update private.bpay_next_command set enrolled_member_count=v_member,
    status=case when v_member=expected_member_count then 'SEALED' else 'ENROLLING' end,
    sealed_at_utc=case when v_member=expected_member_count then pg_catalog.clock_timestamp() else null end where id=p_command_id;
  return v_job;
end
$function$;

create or replace function private.bpay_next_claim_preparation_expiry_job_v1(p_job_id uuid,p_lease_seconds integer default 120)
returns table(lease_nonce uuid,owner_epoch bigint) language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_epoch bigint;v_run_id uuid;v_candidate uuid;v_job private.bpay_next_job%rowtype;
  v_run private.bpay_next_pay_run%rowtype;v_nonce uuid;v_owner bigint;
begin
  if p_job_id is null or p_lease_seconds is null or p_lease_seconds not between 1 and 120 then
    raise exception using errcode='22023',message='BPAY_NEXT_EXPIRY_LEASE_INPUT_INVALID';end if;
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select run_id,candidate_id into strict v_run_id,v_candidate from private.bpay_next_preparation_expiry_worker where job_id=p_job_id;
  select * into strict v_run from private.bpay_next_pay_run where id=v_run_id for update;
  insert into private.bpay_next_worker_control(candidate_id) values(v_candidate) on conflict(candidate_id) do nothing;
  perform 1 from private.bpay_next_worker_control where candidate_id=v_candidate for update;
  select * into strict v_job from private.bpay_next_job where id=p_job_id for update;
  if v_run.status<>'CANCELLING' or v_run.confirmed_at_utc is not null or v_job.job_kind<>'PREPARATION_EXPIRY'
     or v_job.module_epoch<>v_epoch or v_job.status not in ('READY','LEASED')
     or (v_job.status='LEASED' and v_job.lease_until_utc>pg_catalog.clock_timestamp()) then
    raise exception using errcode='55000',message='BPAY_NEXT_EXPIRY_JOB_NOT_CLAIMABLE';end if;
  if exists(select 1 from private.bpay_next_job where candidate_id=v_candidate and command_sequence<v_job.command_sequence and status<>'DONE') then
    raise exception using errcode='55000',message='BPAY_NEXT_EARLIER_WORKER_COMMAND_PENDING';end if;
  update private.bpay_next_worker_control set active_owner_epoch=active_owner_epoch+1,updated_at_utc=pg_catalog.clock_timestamp()
    where candidate_id=v_candidate returning active_owner_epoch into v_owner;
  v_nonce:=pg_catalog.gen_random_uuid();
  update private.bpay_next_job set status='LEASED',owner_epoch=v_owner,lease_nonce=v_nonce,
    lease_until_utc=pg_catalog.clock_timestamp()+pg_catalog.make_interval(secs=>p_lease_seconds),attempt_count=attempt_count+1 where id=p_job_id;
  lease_nonce:=v_nonce;owner_epoch:=v_owner;return next;
end
$function$;

create or replace function private.bpay_next_apply_expired_week_v1(p_job_id uuid)
returns void language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_binding private.bpay_next_preparation_expiry_worker%rowtype;v_run private.bpay_next_pay_run%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;v_week private.bpay_next_worker_week%rowtype;
  v_prior private.bpay_next_worker_week_contribution%rowtype;v_old numeric;v_unbound bigint;
begin
  select * into strict v_binding from private.bpay_next_preparation_expiry_worker where job_id=p_job_id;
  select * into strict v_run from private.bpay_next_pay_run where id=v_binding.run_id;
  if v_binding.status<>'EXPIRED' or v_binding.stage<>'COMPLETE' or v_run.status<>'CANCELLING' or v_run.confirmed_at_utc is not null
     or not exists(select 1 from private.bpay_next_job j join private.bpay_next_worker_control c on c.candidate_id=j.candidate_id
       where j.id=p_job_id and j.job_kind='PREPARATION_EXPIRY' and j.status='LEASED' and j.phase='RELEASE'
         and j.command_id=v_binding.command_id and j.cursor_key=v_binding.checkpoint::text
         and j.owner_epoch=c.active_owner_epoch and j.lease_until_utc>pg_catalog.clock_timestamp()) then
    raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_WEEK_SCOPE_INVALID';end if;
  if v_binding.run_worker_id is null then return;end if;
  select * into strict v_worker from private.bpay_next_run_worker where id=v_binding.run_worker_id;
  if v_worker.status<>'CANCELLED' or v_worker.realised_effect_count<>0 or v_worker.active_case_hold_count<>0
     or v_worker.preparation_revision<>v_binding.preparation_revision or v_worker.case_selection_revision<>v_binding.selection_revision then
    raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_WEEK_WORKER_INVALID';end if;
  select * into v_week from private.bpay_next_worker_week where candidate_id=v_binding.candidate_id
    and pay_week_start=v_run.pay_date-(extract(isodow from v_run.pay_date)::integer-1) for update;
  select * into v_prior from private.bpay_next_worker_week_contribution where original_run_worker_id=v_worker.id for update;
  if v_prior.id is null then
    if v_binding.original_worker_status='READY' and v_binding.selection_revision>0 then
      raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_READY_WEEK_FACT_MISSING';end if;
    -- A partial non-READY preparation never published an arrangement. The
    -- genuine no-case READY owner also intentionally preserves absence when
    -- PAYE floor input is unbound, or the target is not PAYE. Preserve that
    -- absence; no zero contribution, live-policy guess or backfill is made.
    return;
  end if;
  if v_week.candidate_id is null or v_prior.candidate_id<>v_binding.candidate_id or v_prior.pay_week_start<>v_week.pay_week_start
     or v_prior.original_pay_date<>v_run.pay_date or v_prior.original_created_at_utc<>v_run.created_at_utc
     or v_prior.original_gross_amount<>v_worker.gross_inc_vat or v_prior.original_transfer_id is not null
     or exists(select 1 from private.bpay_next_internal_receipt where run_worker_id=v_worker.id)
     or v_prior.returned_cash_id is not null or v_prior.reissue_transfer_id is not null then
    raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_WEEK_FACT_NOT_EXACT';end if;
  if v_prior.payment_state='CANCELLED' and v_prior.eligibility_state='EXCLUDED' and v_prior.eligible_arranged_amount=0 then return;end if;
  if v_prior.payment_state<>'ARRANGED' then raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_WEEK_NOT_UNPAID';end if;
  v_old:=coalesce(v_prior.eligible_arranged_amount,0);v_unbound:=case when v_prior.eligibility_state='UNBOUND' then 1 else 0 end;
  update private.bpay_next_worker_week_contribution set payment_state='CANCELLED',eligibility_state='EXCLUDED',eligible_arranged_amount=0,
    contribution_revision=contribution_revision+1,primary_binding_revision=primary_binding_revision+1 where id=v_prior.id;
  update private.bpay_next_worker_week set resolved_arranged_take_home=resolved_arranged_take_home-v_old,
    unresolved_contribution_count=unresolved_contribution_count-v_unbound,period_revision=period_revision+1
    where candidate_id=v_binding.candidate_id and pay_week_start=v_week.pay_week_start
      and resolved_arranged_take_home>=v_old and unresolved_contribution_count>=v_unbound;
  if not found then raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_WEEK_DELTA_INVALID';end if;
end
$function$;

-- Exact automatic WORK hold-release authority. The nullable third operand is
-- for the collection guard's same typed point path, not a job payload. The
-- caller supplies a real hold PK; the guard resolves it through UNIQUE
-- (run_worker_id,preparation_revision,case_component_id), UNIQUE(state_id,
-- instruction_id), UNIQUE(allocation_result_id), and capacity-use PK.
-- PREPARATION_EXPIRY is neither confirmed-Draft cancellation nor recovery.
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
      and s.job_id=x.original_job_id and s.pass_kind='DRAFT' and s.projection_no=0
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
      and w.status='CANCELLING' and w.preparation_revision=x.preparation_revision and w.case_selection_revision=x.selection_revision
      and w.realised_effect_count=0 and w.net_request_revision=0 and w.net_projection_revision=0
      and i.case_kind='OVERPAYMENT' and i.tax_treatment='TAXABLE' and i.source_pay_channel='PAYE' and i.target_pay_channel='PAYE'
      and i.instruction_kind='RECOVERY' and i.direction='DEDUCTION' and i.payroll_stage='GROSS_DEDUCT' and i.hold_purpose='GROSS_RECOVERY'
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

create or replace function private.bpay_next_reconcile_expired_work_collection_v1(p_job_id uuid,p_case_hold_id uuid)
returns void language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_basis private.bpay_next_work_collection%rowtype;
begin
  select b.* into strict v_basis from private.bpay_next_case_hold h
    join private.bpay_next_run_case_instruction i on i.id=h.instruction_id
    join private.bpay_next_work_collection b on b.id=i.work_collection_id
      and b.case_id=h.case_id and b.case_component_id=h.case_component_id and b.candidate_id=h.candidate_id
    where h.id=p_case_hold_id;
  if not private.bpay_next_expired_work_collection_scope_v1(p_job_id,v_basis.id,p_case_hold_id) then
    raise exception using errcode='23514',message='BPAY_NEXT_WORK_COLLECTION_EXPIRY_SCOPE_INVALID';end if;
  -- No financial disposition: the existing liability formula sees the exact
  -- released H and the maintained applied A/R/W. It never changes R, frozen
  -- evidence or an original cash receipt.1519 must explicitly admit this owner.
  perform private.bpay_next_reconcile_work_collection_v1(p_job_id,v_basis.work_id,v_basis.component_key,null);
end
$function$;

create or replace function private.bpay_next_expire_preparation_worker_page_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,p_expected_checkpoint bigint,p_limit integer)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_epoch bigint;v_run_id uuid;v_candidate uuid;v_seen integer:=0;v_work_holds integer:=0;v_case_released integer:=0;v_more boolean;
  v_run private.bpay_next_pay_run%rowtype;v_control private.bpay_next_worker_control%rowtype;v_job private.bpay_next_job%rowtype;
  v_binding private.bpay_next_preparation_expiry_worker%rowtype;v_worker private.bpay_next_run_worker%rowtype;
  v_line private.bpay_next_run_line%rowtype;v_hold private.bpay_next_hold%rowtype;
  v_case_hold private.bpay_next_case_hold%rowtype;v_use private.bpay_next_case_capacity_use%rowtype;
  v_instruction private.bpay_next_run_case_instruction%rowtype;v_result private.bpay_next_case_allocation_result%rowtype;
  v_amount numeric;v_now timestamptz;v_checkpoint bigint;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null or p_owner_epoch<1
     or p_limit is null or p_limit not between 1 and 100 or (p_expected_checkpoint is not null and p_expected_checkpoint<1) then
    raise exception using errcode='22023',message='BPAY_NEXT_EXPIRY_PAGE_INPUT_INVALID';end if;
  select owner_epoch into v_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select run_id,candidate_id into strict v_run_id,v_candidate from private.bpay_next_preparation_expiry_worker where job_id=p_job_id;
  select * into strict v_run from private.bpay_next_pay_run where id=v_run_id for update;
  select * into strict v_control from private.bpay_next_worker_control where candidate_id=v_candidate for update;
  select * into strict v_job from private.bpay_next_job where id=p_job_id for update;
  select * into strict v_binding from private.bpay_next_preparation_expiry_worker where job_id=p_job_id for update;
  if v_binding.run_worker_id is not null then select * into strict v_worker from private.bpay_next_run_worker where id=v_binding.run_worker_id for update;end if;
  if v_job.job_kind<>'PREPARATION_EXPIRY' or v_job.module_epoch<>v_epoch or v_job.command_id<>v_binding.command_id
     or v_job.candidate_id<>v_candidate or v_job.cursor_key is distinct from nullif(v_binding.checkpoint,0)::text
     or not exists(select 1 from private.bpay_next_command c join private.bpay_next_preparation_expiry_request x on x.command_id=c.id
       where c.id=v_job.command_id and c.command_kind='PREPARATION_EXPIRY' and c.agency_sequence=v_job.command_sequence
         and c.module_epoch=v_epoch and x.run_id=v_run_id and x.captured_deadline=v_run.preparation_expires_at_utc
         and x.expected_candidate_count=c.expected_member_count and x.enrolled_candidate_count=c.enrolled_member_count
         and v_run.review_revision=x.selection_review_revision+1) then
    raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_PAGE_SCOPE_INVALID';end if;
  if v_job.status='DONE' and v_job.phase='EXPIRED' and v_binding.status='EXPIRED' and v_binding.stage='COMPLETE' then
    return pg_catalog.jsonb_build_object('phase','EXPIRED','cursor',v_job.cursor_key,'rows_visited','0',
      'work_released','0','case_holds_released','0',
      'run_worker_id',v_binding.run_worker_id,'replay',true);end if;
  if v_run.status<>'CANCELLING' or v_run.confirmed_at_utc is not null or v_job.status<>'LEASED'
     or v_job.phase not in ('NEW','RELEASE') or v_job.lease_nonce<>p_lease_nonce or v_job.owner_epoch<>p_owner_epoch
     or v_control.active_owner_epoch<>p_owner_epoch or v_job.lease_until_utc<=pg_catalog.clock_timestamp()
     or exists(select 1 from private.bpay_next_job where candidate_id=v_candidate and command_sequence<v_job.command_sequence and status<>'DONE') then
    raise exception using errcode='55000',message='BPAY_NEXT_EXPIRY_PAGE_LEASE_INVALID';end if;
  if coalesce(p_expected_checkpoint,0)<>v_binding.checkpoint then
    if coalesce(p_expected_checkpoint,0)>v_binding.checkpoint then raise exception using errcode='55000',message='BPAY_NEXT_EXPIRY_CURSOR_AHEAD';end if;
    return pg_catalog.jsonb_build_object('phase','RELEASE','cursor',v_job.cursor_key,'rows_visited','0',
      'work_released','0','case_holds_released','0',
      'run_worker_id',v_binding.run_worker_id,'replay',true);end if;
  -- Never put completion before its accepted receipt, including labelled
  -- owner-only simulated-clock vectors. Public receipts use the real clock.
  v_now:=greatest(pg_catalog.clock_timestamp(),(select accepted_at_utc from private.bpay_next_preparation_expiry_request where command_id=v_binding.command_id));
  if v_binding.status='READY' then
    if v_job.phase<>'NEW' or v_binding.stage<>'NEW' then raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_START_INVALID';end if;
    if v_worker.id is not null then
      if v_worker.status<>v_binding.original_worker_status or v_worker.captured_line_count<>v_binding.expected_work_count
         or v_worker.active_case_hold_count<>v_binding.expected_case_hold_count or v_worker.preparation_revision<>v_binding.preparation_revision
         or v_worker.case_selection_revision<>v_binding.selection_revision or v_worker.realised_effect_count<>0
         or v_worker.net_projection_revision<>0 or v_worker.net_request_revision<>0
         or exists(select 1 from private.bpay_next_transfer where run_worker_id=v_worker.id)
         or exists(select 1 from private.bpay_next_net_projection where run_worker_id=v_worker.id) then
        raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_GROUP_NOT_UNCONFIRMED';end if;
      -- The original issue is immutable in this exact expiry binding. Clear
      -- only the current status diagnostic to satisfy the existing REVIEW CK.
      -- Financial resolution counts, captured money and evidence stay intact.
      update private.bpay_next_run_worker set status='CANCELLING',review_issue_code=null,review_issue_work_id=null where id=v_worker.id;
      v_worker.status:='CANCELLING';
    end if;
    v_binding.status:='RELEASING';v_binding.stage:='WORK';v_binding.started_at_utc:=v_now;
    update private.bpay_next_job set phase='RELEASE' where id=p_job_id;
  end if;
  if v_binding.status<>'RELEASING' or (v_worker.id is not null and (v_worker.status<>'CANCELLING'
      or v_worker.realised_effect_count<>0 or v_worker.captured_line_count<>v_binding.expected_work_count)) then
    raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_FENCE_BROKEN';end if;
  if v_binding.stage='WORK' then
    for v_line in select * from private.bpay_next_run_line where run_worker_id=v_binding.run_worker_id
      and line_no>coalesce(v_binding.work_cursor,0) order by line_no,id limit p_limit loop
      select * into v_hold from private.bpay_next_hold where run_line_id=v_line.id for update;
      if found then
        if v_hold.status<>'ACTIVE' or v_hold.work_id<>v_line.work_id or v_hold.component_key<>v_line.component_key
           or (v_hold.source_reserved_ex_vat,v_hold.target_amount_ex_vat,v_hold.target_amount_vat,v_hold.target_amount_inc_vat)
             is distinct from (v_line.source_consumed_ex_vat,v_line.frozen_ex_vat,v_line.frozen_vat,v_line.frozen_inc_vat) then
          raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_WORK_HOLD_NOT_EXACT';end if;
        update private.bpay_next_position set held_source_ex_vat=held_source_ex_vat-v_hold.source_reserved_ex_vat,
          held_target_ex_vat=held_target_ex_vat-v_hold.target_amount_ex_vat,held_target_vat=held_target_vat-v_hold.target_amount_vat,
          held_target_inc_vat=held_target_inc_vat-v_hold.target_amount_inc_vat,updated_at_utc=v_now
          where work_id=v_hold.work_id and component_key=v_hold.component_key and held_source_ex_vat>=v_hold.source_reserved_ex_vat
            and held_target_ex_vat>=v_hold.target_amount_ex_vat and held_target_vat>=v_hold.target_amount_vat and held_target_inc_vat>=v_hold.target_amount_inc_vat;
        if not found then raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_WORK_POSITION_MISMATCH';end if;
        update private.bpay_next_hold set status='RELEASED',finished_at_utc=v_now where id=v_hold.id;v_work_holds:=v_work_holds+1;
      elsif (v_line.source_consumed_ex_vat,v_line.frozen_ex_vat,v_line.frozen_vat,v_line.frozen_inc_vat) is distinct from (0::numeric,0::numeric,0::numeric,0::numeric) then
        raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_POSITIVE_HOLD_MISSING';end if;
      v_binding.work_cursor:=v_line.line_no;v_binding.released_work_count:=v_binding.released_work_count+1;v_seen:=v_seen+1;
    end loop;
    select exists(select 1 from private.bpay_next_run_line where run_worker_id=v_binding.run_worker_id and line_no>coalesce(v_binding.work_cursor,0)) into v_more;
    if not v_more then
      if v_binding.released_work_count<>v_binding.expected_work_count then raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_WORK_COUNT_INVALID';end if;
      v_binding.stage:='CASE';end if;
  elsif v_binding.stage='CASE' then
    for v_case_hold in select * from private.bpay_next_case_hold where run_worker_id=v_binding.run_worker_id and status='ACTIVE'
      and (v_binding.case_hold_cursor is null or id>v_binding.case_hold_cursor) order by id limit p_limit loop
      select * into strict v_instruction from private.bpay_next_run_case_instruction where id=v_case_hold.instruction_id;
      select * into strict v_result from private.bpay_next_case_allocation_result where id=v_case_hold.allocation_result_id;
      perform 1 from private.bpay_next_finance_case where id=v_case_hold.case_id for update;
      perform 1 from private.bpay_next_case_component where id=v_case_hold.case_component_id for update;
      perform 1 from private.bpay_next_case_period where case_component_id=v_case_hold.case_component_id and pay_week_start=v_case_hold.pay_week_start for update;
      perform 1 from private.bpay_next_case_hold where id=v_case_hold.id for update;
      select * into strict v_use from private.bpay_next_case_capacity_use where case_hold_id=v_case_hold.id for update;
      if v_instruction.run_worker_id<>v_binding.run_worker_id or v_instruction.preparation_revision<>v_binding.preparation_revision
         or v_instruction.selection_revision<>v_binding.selection_revision or v_case_hold.candidate_id<>v_candidate
         or v_case_hold.allocation_pass_kind<>'DRAFT' or v_result.pass_kind<>'DRAFT'
         or v_result.instruction_id<>v_instruction.id or v_result.state_id is distinct from v_binding.draft_state_id
         or v_case_hold.projection_id is not null or v_use.status<>'ACTIVE' or v_use.realisation_event_id is not null
         or v_use.source_amount_ex_vat<>v_case_hold.source_reserved_ex_vat
         or (v_case_hold.source_reserved_ex_vat,v_case_hold.target_amount_ex_vat,v_case_hold.target_amount_vat,v_case_hold.target_amount_inc_vat)
           is distinct from (v_result.allocated_source_ex_vat,v_result.allocated_target_ex_vat,v_result.allocated_target_vat,v_result.allocated_target_inc_vat) then
        raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_CASE_HOLD_NOT_EXACT';end if;
      v_amount:=v_case_hold.source_reserved_ex_vat;
      if v_case_hold.purpose='PAYOUT' then
        update private.bpay_next_case_component set active_payout_source_ex_vat=active_payout_source_ex_vat-v_amount,
          component_revision=component_revision+1,updated_at_utc=v_now where id=v_case_hold.case_component_id and active_payout_source_ex_vat>=v_amount;
        if not found then raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_CASE_COMPONENT_CAPACITY_INVALID';end if;
        update private.bpay_next_finance_case set active_payout_hold_amount=active_payout_hold_amount-v_amount,active_hold_amount=active_hold_amount-v_amount,
          case_revision=case_revision+1,updated_at_utc=v_now where id=v_case_hold.case_id and active_payout_hold_amount>=v_amount and active_hold_amount>=v_amount;
      else
        update private.bpay_next_case_component set active_recovery_source_ex_vat=active_recovery_source_ex_vat-v_amount,
          component_revision=component_revision+1,updated_at_utc=v_now where id=v_case_hold.case_component_id and active_recovery_source_ex_vat>=v_amount;
        if not found then raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_CASE_COMPONENT_CAPACITY_INVALID';end if;
        update private.bpay_next_finance_case set active_recovery_hold_amount=active_recovery_hold_amount-v_amount,active_hold_amount=active_hold_amount-v_amount,
          case_revision=case_revision+1,updated_at_utc=v_now where id=v_case_hold.case_id and active_recovery_hold_amount>=v_amount and active_hold_amount>=v_amount;
      end if;
      if not found then raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_CASE_CAPACITY_INVALID';end if;
      if v_case_hold.purpose='PAYOUT' then
        update private.bpay_next_case_period set active_payout_source_ex_vat=active_payout_source_ex_vat-v_amount,period_revision=period_revision+1
          where case_component_id=v_case_hold.case_component_id and pay_week_start=v_case_hold.pay_week_start and active_payout_source_ex_vat>=v_amount;
      else
        update private.bpay_next_case_period set active_unrealised_recovery_source_ex_vat=active_unrealised_recovery_source_ex_vat-v_amount,period_revision=period_revision+1
          where case_component_id=v_case_hold.case_component_id and pay_week_start=v_case_hold.pay_week_start and active_unrealised_recovery_source_ex_vat>=v_amount;
      end if;
      if not found then raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_CASE_PERIOD_INVALID';end if;
      update private.bpay_next_case_hold set status='RELEASED',finished_at_utc=v_now where id=v_case_hold.id;
      update private.bpay_next_case_capacity_use set status='RELEASED' where case_hold_id=v_case_hold.id;
      update private.bpay_next_run_worker set active_case_hold_count=active_case_hold_count-1 where id=v_worker.id and active_case_hold_count>0;
      if not found then raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_CASE_HOLD_COUNT_INVALID';end if;
      if v_instruction.work_collection_id is not null then
        perform private.bpay_next_reconcile_expired_work_collection_v1(p_job_id,v_case_hold.id);
      end if;
      v_binding.case_hold_cursor:=v_case_hold.id;v_binding.released_case_hold_count:=v_binding.released_case_hold_count+1;v_seen:=v_seen+1;v_case_released:=v_case_released+1;
    end loop;
    select exists(select 1 from private.bpay_next_case_hold where run_worker_id=v_binding.run_worker_id and status='ACTIVE') into v_more;
    if not v_more then
      if v_binding.released_case_hold_count<>v_binding.expected_case_hold_count
         or (v_worker.id is not null and (select active_case_hold_count from private.bpay_next_run_worker where id=v_worker.id)<>0) then
        raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_CASE_COUNT_INVALID';end if;
      v_binding.stage:='FINAL';
    elsif v_seen=0 then raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_CASE_CURSOR_INVALID';end if;
  elsif v_binding.stage='FINAL' then
    if v_binding.released_work_count<>v_binding.expected_work_count or v_binding.released_case_hold_count<>v_binding.expected_case_hold_count
       or exists(select 1 from private.bpay_next_case_hold where run_worker_id=v_binding.run_worker_id and status='ACTIVE') then
      raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_FINAL_COUNT_INVALID';end if;
    if v_worker.id is not null then update private.bpay_next_run_worker set status='CANCELLED' where id=v_worker.id;end if;
    v_binding.stage:='COMPLETE';v_binding.status:='EXPIRED';v_binding.finished_at_utc:=v_now;v_seen:=1;
  else raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_STAGE_INVALID';end if;
  update private.bpay_next_preparation_expiry_worker set status=v_binding.status,stage=v_binding.stage,checkpoint=checkpoint+1,
    released_work_count=v_binding.released_work_count,released_case_hold_count=v_binding.released_case_hold_count,
    work_cursor=v_binding.work_cursor,case_hold_cursor=v_binding.case_hold_cursor,started_at_utc=v_binding.started_at_utc,
    finished_at_utc=v_binding.finished_at_utc where job_id=p_job_id returning checkpoint into v_checkpoint;
  update private.bpay_next_job set cursor_key=v_checkpoint::text,applied_line_count=v_binding.released_work_count,
    lease_until_utc=pg_catalog.clock_timestamp()+interval '120 seconds' where id=p_job_id;
  update private.bpay_next_worker_control set financial_view_revision=financial_view_revision+1,updated_at_utc=v_now where candidate_id=v_candidate;
  if v_binding.status='EXPIRED' then
    perform private.bpay_next_apply_expired_week_v1(p_job_id);
    update private.bpay_next_job set status='DONE',phase='EXPIRED',lease_nonce=null,lease_until_utc=null where id=p_job_id;
    update private.bpay_next_preparation_expiry_request set completed_candidate_count=completed_candidate_count+1,
      status=case when completed_candidate_count+1=expected_candidate_count and enrolled_candidate_count=expected_candidate_count then 'EXPIRED' else status end,
      finished_at_utc=case when completed_candidate_count+1=expected_candidate_count and enrolled_candidate_count=expected_candidate_count then v_now else null end
      where command_id=v_binding.command_id and completed_candidate_count<enrolled_candidate_count;
    if not found then raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_PARENT_COUNT_INVALID';end if;
    update private.bpay_next_pay_run set cancelled_candidate_count=cancelled_candidate_count+1,
      status=case when cancelled_candidate_count+1=selected_candidate_count then 'CANCELLED' else 'CANCELLING' end
      where id=v_run_id and cancelled_candidate_count<selected_candidate_count;
    if not found then raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_RUN_COUNT_INVALID';end if;
    if exists(select 1 from private.bpay_next_preparation_expiry_request where command_id=v_binding.command_id and status='EXPIRED') then
      update private.bpay_next_command set status='COMPLETE' where id=v_binding.command_id;end if;
  end if;
  return pg_catalog.jsonb_build_object('phase',case when v_binding.status='EXPIRED' then 'EXPIRED' else 'RELEASE' end,
    'cursor',v_checkpoint::text,'rows_visited',v_seen::text,'work_released',v_work_holds::text,
    'case_holds_released',v_case_released::text,'run_worker_id',v_binding.run_worker_id,'replay',false);
end
$function$;

-- Retained PREPARE queue messages cannot resurrect a stopped capture. Original
-- DONE receipts/cursors remain stored; expose current expiry truth separately.
create or replace function private.bpay_next_expired_prepare_receipt_v1(p_job_id uuid)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_job private.bpay_next_job%rowtype;v_run private.bpay_next_pay_run%rowtype;
  v_request private.bpay_next_preparation_expiry_request%rowtype;v_worker private.bpay_next_run_worker%rowtype;
  v_binding private.bpay_next_preparation_expiry_worker%rowtype;
begin
  select * into strict v_job from private.bpay_next_job where id=p_job_id and job_kind='PREPARE';
  select r.* into strict v_run from private.bpay_next_pay_run r join private.bpay_next_run_command rc on rc.run_id=r.id
    where rc.command_id=v_job.command_id;
  select * into strict v_request from private.bpay_next_preparation_expiry_request where run_id=v_run.id
    and original_prepare_command_id=v_job.command_id;
  if v_job.status<>'DONE' then raise exception using errcode='55000',message='BPAY_NEXT_PREPARATION_EXPIRING';end if;
  if v_run.status not in ('CANCELLING','CANCELLED') or v_run.confirmed_at_utc is not null
     or v_request.captured_deadline<>v_run.preparation_expires_at_utc
     or not private.bpay_next_preparation_expiry_fenced_v1(v_run.id,v_job.command_id)
     or v_job.phase not in ('EXPIRED','READY','DONE')
     or not exists(select 1 from private.bpay_next_command where id=v_request.command_id
       and command_kind='PREPARATION_EXPIRY' and module_epoch=v_job.module_epoch)
     or not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT' and owner_epoch=v_job.module_epoch) then
    raise exception using errcode='23514',message='BPAY_NEXT_EXPIRED_PREPARE_RECEIPT_SCOPE_INVALID';end if;
  select * into v_worker from private.bpay_next_run_worker where run_id=v_run.id and candidate_id=v_job.candidate_id;
  select * into v_binding from private.bpay_next_preparation_expiry_worker where command_id=v_request.command_id and candidate_id=v_job.candidate_id;
  if v_binding.job_id is not null and v_binding.original_job_id is distinct from v_job.id then
    raise exception using errcode='23514',message='BPAY_NEXT_EXPIRED_PREPARE_RECEIPT_JOB_MISMATCH';end if;
  return pg_catalog.jsonb_build_object('done',true,'phase','EXPIRED','cursor',v_job.cursor_key,
    'work_cursor',v_job.position_work_cursor,'component_cursor',v_job.position_component_cursor,'rows_visited','0',
    'replay',true,'run_worker_id',v_worker.id,'worker_status',v_worker.status,
    'issue_code',coalesce(v_worker.review_issue_code,v_binding.original_review_issue_code),
    'issue_work_id',coalesce(v_worker.review_issue_work_id,v_binding.original_review_issue_work_id));
end
$function$;

create or replace function private.bpay_next_preparation_expiry_guard_v1()
returns trigger language plpgsql set search_path=pg_catalog,private
as $function$
declare v_run private.bpay_next_pay_run%rowtype;v_request private.bpay_next_preparation_expiry_request%rowtype;
begin
  if tg_op='DELETE' then raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_EVIDENCE_DELETE_FORBIDDEN';end if;
  if tg_table_name='bpay_next_pay_run' then
    if tg_op='INSERT' then
      if new.preparation_expires_at_utc is null or new.preparation_expires_at_utc<>new.created_at_utc+interval '72 hours' then
        raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_CAPTURED_DEADLINE_REQUIRED';end if;
    elsif (new.created_at_utc,new.preparation_expires_at_utc) is distinct from (old.created_at_utc,old.preparation_expires_at_utc) then
      raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_DEADLINE_IMMUTABLE';end if;
    if new.status='CANCELLING' or (tg_op='UPDATE' and old.status='CANCELLING') then
      if new.confirmed_at_utc is not null or new.status not in ('CANCELLING','CANCELLED')
         or not exists(select 1 from private.bpay_next_preparation_expiry_request where run_id=new.id
           and captured_deadline=new.preparation_expires_at_utc and selection_count=new.selection_count
           and selection_page_count=new.selection_page_count and expected_candidate_count=new.selected_candidate_count
           and new.review_revision=selection_review_revision+1) then
        raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_HEADER_FENCE_INVALID';end if;
    end if;
    return new;
  end if;
  if tg_op='INSERT' then
    if tg_table_name='bpay_next_preparation_expiry_request' then
      select * into strict v_run from private.bpay_next_pay_run where id=new.run_id;
      if v_run.status not in ('PREPARING','REVIEW') or v_run.confirmed_at_utc is not null
         or new.captured_deadline is distinct from v_run.preparation_expires_at_utc
         or (new.selection_count,new.selection_page_count,new.selection_review_revision,new.expected_candidate_count)
           is distinct from (v_run.selection_count,v_run.selection_page_count,v_run.review_revision,v_run.selected_candidate_count)
         or new.enrolled_candidate_count<>0 or new.completed_candidate_count<>0
         or not exists(select 1 from private.bpay_next_command where id=new.command_id and command_kind='PREPARATION_EXPIRY' and status='RECEIVED')
         or new.original_prepare_command_id is distinct from (select command_id from private.bpay_next_run_command where run_id=new.run_id) then
        raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_REQUEST_INITIAL_INVALID';end if;
    else
      select * into strict v_request from private.bpay_next_preparation_expiry_request where command_id=new.command_id;
      select * into strict v_run from private.bpay_next_pay_run where id=new.run_id;
      if v_run.status<>'CANCELLING' or v_run.confirmed_at_utc is not null or v_request.status<>'CANCELLING'
         or new.run_id<>v_request.run_id or new.member_no<>v_request.enrolled_candidate_count+1
         or new.status<>'READY' or new.stage<>'NEW' or new.checkpoint<>0
         or new.released_work_count<>0 or new.released_case_hold_count<>0 or new.work_cursor is not null or new.case_hold_cursor is not null
         or not exists(select 1 from private.bpay_next_selection_candidate where run_id=new.run_id and candidate_id=new.candidate_id and member_no=new.member_no)
         or not exists(select 1 from private.bpay_next_job where id=new.job_id and command_id=new.command_id and candidate_id=new.candidate_id
           and job_kind='PREPARATION_EXPIRY' and status='READY' and phase='NEW')
         or (v_request.original_prepare_command_id is null) is distinct from (new.original_job_id is null)
         or (new.original_job_id is not null and not exists(select 1 from private.bpay_next_job where id=new.original_job_id
           and command_id=v_request.original_prepare_command_id and candidate_id=new.candidate_id and job_kind='PREPARE' and status='DONE'))
         or (new.run_worker_id is not null and not exists(select 1 from private.bpay_next_run_worker where id=new.run_worker_id
           and run_id=new.run_id and candidate_id=new.candidate_id and status=new.original_worker_status
           and preparation_revision=new.preparation_revision and case_selection_revision=new.selection_revision
           and captured_line_count=new.expected_work_count and active_case_hold_count=new.expected_case_hold_count
           and review_issue_code is not distinct from new.original_review_issue_code and review_issue_work_id is not distinct from new.original_review_issue_work_id)) then
        raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_WORKER_INITIAL_INVALID';end if;
    end if;
    return new;
  end if;
  if tg_table_name='bpay_next_preparation_expiry_request' then
    if old.status='EXPIRED' or (pg_catalog.to_jsonb(new)-'enrolled_candidate_count'-'completed_candidate_count'-'status'-'finished_at_utc')
       is distinct from (pg_catalog.to_jsonb(old)-'enrolled_candidate_count'-'completed_candidate_count'-'status'-'finished_at_utc')
       or new.enrolled_candidate_count not between old.enrolled_candidate_count and old.enrolled_candidate_count+1
       or new.completed_candidate_count not between old.completed_candidate_count and old.completed_candidate_count+1 then
      raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_REQUEST_TRANSITION_INVALID';end if;
  else
    if old.status='EXPIRED' or (pg_catalog.to_jsonb(new)-'checkpoint'-'status'-'stage'-'released_work_count'-'released_case_hold_count'-'work_cursor'-'case_hold_cursor'-'started_at_utc'-'finished_at_utc')
       is distinct from (pg_catalog.to_jsonb(old)-'checkpoint'-'status'-'stage'-'released_work_count'-'released_case_hold_count'-'work_cursor'-'case_hold_cursor'-'started_at_utc'-'finished_at_utc')
       or new.checkpoint<>old.checkpoint+1 or new.released_work_count<old.released_work_count or new.released_case_hold_count<old.released_case_hold_count
       or (old.work_cursor is not null and (new.work_cursor is null or new.work_cursor<old.work_cursor))
       or (old.case_hold_cursor is not null and (new.case_hold_cursor is null or new.case_hold_cursor<old.case_hold_cursor))
       or (old.started_at_utc is not null and new.started_at_utc is distinct from old.started_at_utc)
       or (old.stage='NEW' and new.stage not in ('WORK','CASE')) or (old.stage='WORK' and new.stage not in ('WORK','CASE'))
       or (old.stage='CASE' and new.stage not in ('CASE','FINAL')) or (old.stage='FINAL' and new.stage<>'COMPLETE') then
      raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_WORKER_TRANSITION_INVALID';end if;
  end if;
  return new;
end
$function$;
drop trigger if exists bpay_next_preparation_expiry_guard_v1 on private.bpay_next_pay_run;
create trigger bpay_next_preparation_expiry_guard_v1 before insert or update or delete on private.bpay_next_pay_run
  for each row execute function private.bpay_next_preparation_expiry_guard_v1();
drop trigger if exists bpay_next_preparation_expiry_guard_v1 on private.bpay_next_preparation_expiry_request;
create trigger bpay_next_preparation_expiry_guard_v1 before insert or update or delete on private.bpay_next_preparation_expiry_request
  for each row execute function private.bpay_next_preparation_expiry_guard_v1();
drop trigger if exists bpay_next_preparation_expiry_guard_v1 on private.bpay_next_preparation_expiry_worker;
create trigger bpay_next_preparation_expiry_guard_v1 before insert or update or delete on private.bpay_next_preparation_expiry_worker
  for each row execute function private.bpay_next_preparation_expiry_guard_v1();
alter function private.bpay_next_preparation_expiry_fenced_v1(uuid,uuid) owner to postgres;
alter function private.bpay_next_accept_preparation_expiry_v1(uuid,uuid,uuid,timestamptz,timestamptz) owner to postgres;
alter function private.bpay_next_enroll_preparation_expiry_one_v1(uuid) owner to postgres;
alter function private.bpay_next_claim_preparation_expiry_job_v1(uuid,integer) owner to postgres;
alter function private.bpay_next_apply_expired_week_v1(uuid) owner to postgres;
alter function private.bpay_next_expired_work_collection_scope_v1(uuid,uuid,uuid) owner to postgres;
alter function private.bpay_next_reconcile_expired_work_collection_v1(uuid,uuid) owner to postgres;
alter function private.bpay_next_expire_preparation_worker_page_v1(uuid,uuid,bigint,bigint,integer) owner to postgres;
alter function private.bpay_next_expired_prepare_receipt_v1(uuid) owner to postgres;
alter function private.bpay_next_preparation_expiry_guard_v1() owner to postgres;
revoke all on function private.bpay_next_preparation_expiry_fenced_v1(uuid,uuid),
  private.bpay_next_accept_preparation_expiry_v1(uuid,uuid,uuid,timestamptz,timestamptz),
  private.bpay_next_enroll_preparation_expiry_one_v1(uuid),private.bpay_next_claim_preparation_expiry_job_v1(uuid,integer),
  private.bpay_next_apply_expired_week_v1(uuid),private.bpay_next_expire_preparation_worker_page_v1(uuid,uuid,bigint,bigint,integer),
  private.bpay_next_expired_work_collection_scope_v1(uuid,uuid,uuid),private.bpay_next_reconcile_expired_work_collection_v1(uuid,uuid),
  private.bpay_next_expired_prepare_receipt_v1(uuid),private.bpay_next_preparation_expiry_guard_v1()
  from public,anon,authenticated,service_role;
commit;
