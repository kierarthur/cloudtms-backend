-- Internal narrow proof of the prepared-offer to frozen-Draft boundary.
-- No browser/service grant is made here. A full review reader, cases and
-- cancellation owner must be connected before this can be activated.
\set ON_ERROR_STOP on

begin;

create or replace function private.bpay_next_review_simple_run_v1(
  p_run_id uuid
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_run private.bpay_next_pay_run%rowtype;
  v_command private.bpay_next_command%rowtype;
  v_all_ready boolean;
  v_module_epoch bigint;
begin
  if p_run_id is null then
    raise exception using errcode='22023',
      message='BPAY_NEXT_REVIEW_RUN_ID_REQUIRED';
  end if;
  select owner_epoch into v_module_epoch
    from private.bpay_next_module_control
    where id=1 and active_owner='NEXT' for share;
  if v_module_epoch is null then
    raise exception using errcode='55000',
      message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select * into strict v_run from private.bpay_next_pay_run
    where id=p_run_id for update;
  if v_run.status='REVIEW' then
    -- A lost successful response must remain confirmable on exact replay.
    -- This parent lock also serializes expiry; exceptional frozen worker
    -- states are probed through the same partial index as first review.
    v_all_ready:=not exists (
      select 1 from private.bpay_next_run_worker w
      where w.run_id=p_run_id and w.status<>'READY');
    return pg_catalog.jsonb_build_object('phase','REVIEW','replay',true,
      'review_revision',v_run.review_revision,
      'selected_candidates',v_run.selected_candidate_count,
      'all_ready',v_all_ready);
  end if;
  if v_run.status<>'PREPARING' or v_run.selection_state<>'SEALED'
     or v_run.work_choice_count<>v_run.selection_count
     or v_run.sealed_work_choice_count<>v_run.selection_count
     or v_run.selected_candidate_count<1 then
    raise exception using errcode='55000',
      message='BPAY_NEXT_REVIEW_RUN_STATE_INVALID';
  end if;
  select c.* into strict v_command
    from private.bpay_next_run_command rc
    join private.bpay_next_command c on c.id=rc.command_id
    where rc.run_id=p_run_id;
  if v_command.status<>'SEALED' or v_command.module_epoch<>v_module_epoch
     or v_command.expected_member_count<>v_run.selected_candidate_count
     or v_command.enrolled_member_count<>v_run.selected_candidate_count then
    return pg_catalog.jsonb_build_object('phase','PREPARING',
      'reason','ENROLLMENT_INCOMPLETE');
  end if;
  if exists (select 1 from private.bpay_next_job j
       where j.command_id=v_command.id and j.job_kind='PREPARE'
         and j.status<>'DONE') then
    return pg_catalog.jsonb_build_object('phase','PREPARING',
      'reason','WORKERS_INCOMPLETE');
  end if;
  -- The sealed command enrolled exactly the selected member count. Each
  -- PREPARE job can become DONE only through an owner that creates its worker.
  -- Probe the partial indexes for unfinished/non-ready rows instead of
  -- counting every selected Candidate or frozen line at the final boundary.
  v_all_ready:=not exists (
    select 1 from private.bpay_next_run_worker w
    where w.run_id=p_run_id and w.status<>'READY');
  update private.bpay_next_pay_run
    set status='REVIEW',review_revision=review_revision+1
    where id=p_run_id
    returning * into v_run;
  return pg_catalog.jsonb_build_object('phase','REVIEW','replay',false,
    'review_revision',v_run.review_revision,
    'selected_candidates',v_run.selected_candidate_count,
    'all_ready',v_all_ready);
end
$function$;

-- Confirmation changes only the parent state. READY worker rows and their
-- immutable run lines remain the Draft's frozen constituents. It must not
-- reprice from current Timesheets, Source, rates or financial positions.
create or replace function private.bpay_next_confirm_simple_run_v1(
  p_run_id uuid,p_review_revision bigint
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_run private.bpay_next_pay_run%rowtype;
  v_module_epoch bigint;
  v_command private.bpay_next_command%rowtype;
begin
  if p_run_id is null or p_review_revision is null then
    raise exception using errcode='22023',
      message='BPAY_NEXT_CONFIRM_INPUT_INVALID';
  end if;
  select owner_epoch into v_module_epoch
    from private.bpay_next_module_control
    where id=1 and active_owner='NEXT' for share;
  if v_module_epoch is null then
    raise exception using errcode='55000',
      message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select * into strict v_run from private.bpay_next_pay_run
    where id=p_run_id for update;
  if v_run.status='DRAFT' and v_run.review_revision=p_review_revision
     and v_run.confirmed_at_utc is not null then
    return pg_catalog.jsonb_build_object('phase','DRAFT','replay',true,
      'review_revision',v_run.review_revision);
  end if;
  if v_run.status<>'REVIEW' or v_run.selection_state<>'SEALED'
     or v_run.work_choice_count<>v_run.selection_count
     or v_run.sealed_work_choice_count<>v_run.selection_count
     or v_run.review_revision<>p_review_revision
     or v_run.confirmed_at_utc is not null then
    raise exception using errcode='55000',
      message='BPAY_NEXT_CONFIRM_REVIEW_STATE_INVALID';
  end if;
  -- The real clock is read AFTER the header lock: a transaction started
  -- before the deadline cannot confirm late after waiting for that lock.
  -- The completed DRAFT replay above is permanent and never expires.
  if v_run.preparation_expires_at_utc is null
     or v_run.preparation_expires_at_utc<=pg_catalog.clock_timestamp() then
    raise exception using errcode='55000',
      message='BPAY_NEXT_CONFIRM_PREPARATION_EXPIRED_OR_UNBOUND';
  end if;
  select c.* into strict v_command
    from private.bpay_next_run_command rc
    join private.bpay_next_command c on c.id=rc.command_id
    where rc.run_id=p_run_id;
  if v_command.status<>'SEALED' or v_command.module_epoch<>v_module_epoch
     or v_command.expected_member_count<>v_run.selected_candidate_count
     or v_command.enrolled_member_count<>v_run.selected_candidate_count
     or exists (select 1 from private.bpay_next_job j
        where j.command_id=v_command.id and j.job_kind='PREPARE'
          and j.status<>'DONE')
     or exists (
       select 1 from private.bpay_next_run_worker
       where run_id=p_run_id and status<>'READY') then
    raise exception using errcode='55000',
      message='BPAY_NEXT_CONFIRM_UNRESOLVED_WORKER';
  end if;
  -- Each bounded holder transaction wrote its line, position and ACTIVE hold
  -- atomically before its worker became READY. Future cancellation/expiry
  -- owners must lock this parent and make a worker non-ready before release;
  -- no such owner is granted or activated in this narrow proof.
  update private.bpay_next_pay_run
    set status='DRAFT',confirmed_at_utc=pg_catalog.transaction_timestamp()
    where id=p_run_id;
  return pg_catalog.jsonb_build_object('phase','DRAFT','replay',false,
    'review_revision',p_review_revision);
end
$function$;

-- One bounded review page. Money comes only from frozen worker rows.
-- Current-revision comparisons have their own constituent page below; doing
-- an unbounded per-worker EXISTS here would make a one-worker page expensive.
create or replace function private.bpay_next_simple_review_worker_page_v1(
  p_run_id uuid,p_after_candidate_id uuid,p_limit integer
) returns table (
  run_worker_id uuid,candidate_id uuid,worker_status text,
  frozen_ex_vat numeric,frozen_vat numeric,frozen_inc_vat numeric,
  review_issue_code text
)
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_run_status text;
begin
  if p_run_id is null or p_limit is null or p_limit not between 1 and 100 then
    raise exception using errcode='22023',
      message='BPAY_NEXT_REVIEW_PAGE_INPUT_INVALID';
  end if;
  select r.status into strict v_run_status
    from private.bpay_next_pay_run r where r.id=p_run_id;
  if v_run_status not in ('REVIEW','DRAFT') then
    raise exception using errcode='55000',
      message='BPAY_NEXT_REVIEW_PAGE_NOT_AVAILABLE';
  end if;
  return query
    select w.id,w.candidate_id,w.status,
      case when w.status='READY' then w.gross_ex_vat else null end,
      case when w.status='READY' then w.gross_vat else null end,
      case when w.status='READY' then w.gross_inc_vat else null end,
       w.review_issue_code
    from private.bpay_next_run_worker w
    where w.run_id=p_run_id
      and (p_after_candidate_id is null or
           w.candidate_id>p_after_candidate_id)
    order by w.candidate_id,w.id
    limit p_limit;
end
$function$;

-- Informational currentness is read in bounded constituent pages. A caller
-- may say "no newer approval" only after reaching the final page; a missing
-- page is unknown, never false. This never changes frozen money or Draft state.
create or replace function private.bpay_next_simple_revision_comparison_page_v1(
  p_run_worker_id uuid,p_after_work_id uuid,p_limit integer
) returns table (
  run_work_id uuid,work_id uuid,original_timesheet_id uuid,
  captured_revision_id uuid,current_revision_id uuid,
  newer_approval_available boolean
)
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_run_status text;
  v_worker_status text;
begin
  if p_run_worker_id is null or p_limit is null
     or p_limit not between 1 and 100 then
    raise exception using errcode='22023',
      message='BPAY_NEXT_REVISION_PAGE_INPUT_INVALID';
  end if;
  select r.status,w.status into strict v_run_status,v_worker_status
    from private.bpay_next_run_worker w
    join private.bpay_next_pay_run r on r.id=w.run_id
    where w.id=p_run_worker_id;
  if v_run_status not in ('REVIEW','DRAFT')
     or v_worker_status<>'READY' then
    raise exception using errcode='55000',
      message='BPAY_NEXT_REVISION_PAGE_NOT_AVAILABLE';
  end if;
  return query
    with page as materialized (
      select rw.id,rw.work_id,rw.captured_revision_id
      from private.bpay_next_run_work rw
      where rw.run_worker_id=p_run_worker_id
        and (p_after_work_id is null or rw.work_id>p_after_work_id)
      order by rw.work_id,rw.id
      limit p_limit
    )
    select page.id,page.work_id,current_work.original_timesheet_id,
      page.captured_revision_id,current_work.current_revision_id,
      current_work.current_revision_id is distinct from
        page.captured_revision_id
    from page
    join private.bpay_next_work current_work on current_work.id=page.work_id
    order by page.work_id,page.id;
end
$function$;

-- Frozen constituent page for a Candidate breakdown. A separate typed shift,
-- break and rate-detail page will complete the Office/advice reader; this
-- function never touches the current Timesheet or current pay positions.
create or replace function private.bpay_next_simple_review_line_page_v1(
  p_run_worker_id uuid,p_after_line_no bigint,p_limit integer
) returns table (
  run_line_id uuid,line_no bigint,original_timesheet_id uuid,
  captured_revision_id uuid,component_key text,component_kind text,
  work_date date,source_pay_channel text,target_pay_channel text,
  source_consumed_ex_vat numeric,frozen_ex_vat numeric,frozen_vat numeric,
  frozen_inc_vat numeric
)
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_run_status text;
  v_worker_status text;
begin
  if p_run_worker_id is null or p_limit is null
     or p_limit not between 1 and 100
     or (p_after_line_no is not null and p_after_line_no<0) then
    raise exception using errcode='22023',
      message='BPAY_NEXT_REVIEW_LINE_PAGE_INPUT_INVALID';
  end if;
  select r.status,w.status into strict v_run_status,v_worker_status
    from private.bpay_next_run_worker w
    join private.bpay_next_pay_run r on r.id=w.run_id
    where w.id=p_run_worker_id;
  if v_run_status not in ('REVIEW','DRAFT')
     or v_worker_status<>'READY' then
    raise exception using errcode='55000',
      message='BPAY_NEXT_REVIEW_LINE_PAGE_NOT_AVAILABLE';
  end if;
  return query
    select l.id,l.line_no,work.original_timesheet_id,
      l.captured_revision_id,l.component_key,a.component_kind,
      a.work_date,l.source_pay_channel,l.target_pay_channel,
      l.source_consumed_ex_vat,l.frozen_ex_vat,l.frozen_vat,
      l.frozen_inc_vat
    from private.bpay_next_run_line l
    join private.bpay_next_approved_line a on a.id=l.approved_line_id
    join private.bpay_next_work work on work.id=l.work_id
    where l.run_worker_id=p_run_worker_id
      and l.line_no>coalesce(p_after_line_no,0)
    order by l.line_no,l.id
    limit p_limit;
end
$function$;

alter function private.bpay_next_review_simple_run_v1(uuid) owner to postgres;
alter function private.bpay_next_confirm_simple_run_v1(uuid,bigint) owner to postgres;
alter function private.bpay_next_simple_review_worker_page_v1(
  uuid,uuid,integer) owner to postgres;
alter function private.bpay_next_simple_revision_comparison_page_v1(
  uuid,uuid,integer) owner to postgres;
alter function private.bpay_next_simple_review_line_page_v1(
  uuid,bigint,integer) owner to postgres;
revoke all on function private.bpay_next_review_simple_run_v1(uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.bpay_next_confirm_simple_run_v1(uuid,bigint)
  from public,anon,authenticated,service_role;
revoke all on function private.bpay_next_simple_review_worker_page_v1(
  uuid,uuid,integer) from public,anon,authenticated,service_role;
revoke all on function private.bpay_next_simple_revision_comparison_page_v1(
  uuid,uuid,integer) from public,anon,authenticated,service_role;
revoke all on function private.bpay_next_simple_review_line_page_v1(
  uuid,bigint,integer) from public,anon,authenticated,service_role;

commit;
