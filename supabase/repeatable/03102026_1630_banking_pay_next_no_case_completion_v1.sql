-- Narrow first vertical completion: a selected Candidate with no finance-case
-- records can release its computation owner while retaining every exact hold.
-- The parent remains PREPARING; this does not confirm or create a Draft.
\set ON_ERROR_STOP on

begin;

create or replace function private.bpay_next_complete_no_case_worker_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint
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
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null then
    raise exception using errcode='22023',
      message='BPAY_NEXT_NO_CASE_COMPLETION_INPUT_INVALID';
  end if;
  select rc.run_id into strict v_run_id
    from private.bpay_next_job j
    join private.bpay_next_run_command rc on rc.command_id=j.command_id
    where j.id=p_job_id and j.job_kind='PREPARE';
  select owner_epoch into v_module_epoch
    from private.bpay_next_module_control
    where id=1 and active_owner='NEXT' for share;
  if v_module_epoch is null then
    raise exception using errcode='55000',
      message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select * into strict v_run from private.bpay_next_pay_run
    where id=v_run_id for share;
  select candidate_id into strict v_job.candidate_id
    from private.bpay_next_job where id=p_job_id;
  perform 1 from private.bpay_next_worker_control
    where candidate_id=v_job.candidate_id for update;
  select * into strict v_job from private.bpay_next_job
    where id=p_job_id for update;
  select * into strict v_worker from private.bpay_next_run_worker
    where run_id=v_run_id and candidate_id=v_job.candidate_id for update;
  if v_job.status='DONE' and v_job.phase='READY'
     and v_worker.status in ('READY','DRAFT','ISSUED','COMPLETE','CANCELLING','CANCELLED') then
    return pg_catalog.jsonb_build_object('done',true,'phase','READY',
      'worker_status',v_worker.status,
      'replay',true,'run_worker_id',v_worker.id);
  end if;
  if v_run.status<>'PREPARING' or v_run.selection_state<>'SEALED'
     or v_job.job_kind<>'PREPARE' or v_job.status<>'LEASED'
     or v_job.phase<>'FINANCE_ALLOCATE'
     or v_job.module_epoch<>v_module_epoch
     or v_job.lease_nonce<>p_lease_nonce
     or v_job.owner_epoch<>p_owner_epoch
     or v_job.lease_until_utc<=pg_catalog.clock_timestamp()
     or v_worker.status<>'PREPARING'
     or v_worker.review_issue_code is not null
     or v_worker.financial_resolution_count<>0 then
    raise exception using errcode='55000',
      message='BPAY_NEXT_NO_CASE_COMPLETION_STATE_INVALID';
  end if;
  if v_worker.captured_position_count=0
     or v_worker.captured_position_count<>v_worker.captured_line_count then
    raise exception using errcode='23514',
      message='BPAY_NEXT_NO_CASE_COMPLETION_LINE_COUNT_MISMATCH';
  end if;
  -- Any case, including a paused or closed one, is conservatively deferred
  -- to the common case-allocation owner. This uses candidate_id index EXISTS,
  -- not a scan of payment or Timesheet history.
  if exists(select 1 from private.bpay_next_finance_case c
       where c.candidate_id=v_job.candidate_id) then
    raise exception using errcode='55000',
      message='BPAY_NEXT_CASE_ALLOCATION_REQUIRED';
  end if;
  update private.bpay_next_run_worker set status='READY'
    where id=v_worker.id;
  perform private.bpay_next_publish_no_case_week_fact_v1(v_worker.id,null);
  update private.bpay_next_job
    set status='DONE',phase='READY',cursor_key=null,
        lease_nonce=null,lease_until_utc=null,
        position_work_cursor=null,position_component_cursor=null
    where id=p_job_id;
  update private.bpay_next_worker_control
    set financial_view_revision=financial_view_revision+1,
        updated_at_utc=pg_catalog.transaction_timestamp()
    where candidate_id=v_job.candidate_id;
  return pg_catalog.jsonb_build_object('done',true,'phase','READY',
    'replay',false,'run_worker_id',v_worker.id);
end
$function$;

alter function private.bpay_next_complete_no_case_worker_v1(
  uuid,uuid,bigint) owner to postgres;
revoke all on function private.bpay_next_complete_no_case_worker_v1(
  uuid,uuid,bigint) from public,anon,authenticated,service_role;

commit;
