-- Internal no-case PAYE projection owner. This consumes the ordered input
-- once; it does not build an executable transfer or mark money paid.
\set ON_ERROR_STOP on

begin;

create or replace function private.bpay_next_claim_simple_paye_net_job_v1(
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
    raise exception using errcode='22023',
      message='BPAY_NEXT_PAYE_NET_LEASE_INPUT_INVALID';
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
  select * into strict v_job from private.bpay_next_job
    where id=p_job_id for update;
  if v_job.module_epoch<>v_epoch or v_job.job_kind<>'PAYE_NET_ENTRY'
     or v_job.status not in ('READY','LEASED')
     or (v_job.status='LEASED'
         and v_job.lease_until_utc>pg_catalog.clock_timestamp())
     or exists(select 1 from private.bpay_next_job earlier
               where earlier.candidate_id=v_candidate_id
                 and earlier.command_sequence<v_job.command_sequence
                 and earlier.status<>'DONE') then
    raise exception using errcode='55000',
      message='BPAY_NEXT_PAYE_NET_JOB_NOT_CLAIMABLE';
  end if;
  update private.bpay_next_worker_control
    set active_owner_epoch=active_owner_epoch+1,
        updated_at_utc=pg_catalog.transaction_timestamp()
    where candidate_id=v_candidate_id
    returning active_owner_epoch into v_owner_epoch;
  v_nonce:=pg_catalog.gen_random_uuid();
  update private.bpay_next_job
    set status='LEASED',owner_epoch=v_owner_epoch,
        lease_nonce=v_nonce,
        lease_until_utc=pg_catalog.clock_timestamp()+
          pg_catalog.make_interval(secs=>p_lease_seconds),
        attempt_count=attempt_count+1
    where id=p_job_id;
  lease_nonce:=v_nonce; owner_epoch:=v_owner_epoch;
  return next;
end
$function$;

create or replace function private.bpay_next_apply_simple_paye_net_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_run_id uuid;
  v_candidate_id uuid;
  v_run private.bpay_next_pay_run%rowtype;
  v_job private.bpay_next_job%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_request private.bpay_next_paye_net_request%rowtype;
  v_projection_id uuid;
  v_epoch bigint;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null then
    raise exception using errcode='22023',
      message='BPAY_NEXT_PAYE_NET_APPLY_INPUT_INVALID';
  end if;
  select owner_epoch into v_epoch from private.bpay_next_module_control
    where id=1 and active_owner='NEXT' for share;
  if v_epoch is null then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select w.run_id,w.candidate_id into strict v_run_id,v_candidate_id
    from private.bpay_next_job j
    join private.bpay_next_paye_net_request r on r.command_id=j.command_id
    join private.bpay_next_run_worker w on w.id=r.run_worker_id
    where j.id=p_job_id and j.candidate_id=w.candidate_id;
  -- Header before Candidate control and worker; this also serialises a
  -- competing whole-Candidate Draft cancellation once that owner exists.
  select * into strict v_run from private.bpay_next_pay_run
    where id=v_run_id for update;
  perform 1 from private.bpay_next_worker_control
    where candidate_id=v_candidate_id for update;
  select * into strict v_job from private.bpay_next_job
    where id=p_job_id for update;
  select * into strict v_request from private.bpay_next_paye_net_request
    where command_id=v_job.command_id;
  select * into strict v_worker from private.bpay_next_run_worker
    where id=v_request.run_worker_id for update;
  if v_job.job_kind<>'PAYE_NET_ENTRY'
     or v_job.module_epoch<>v_epoch
     or v_job.candidate_id<>v_worker.candidate_id
     or v_run.id<>v_worker.run_id
     or v_request.candidate_id<>v_worker.candidate_id
     or v_request.case_draft_state_id is not null then
    raise exception using errcode='23514',
      message='BPAY_NEXT_PAYE_NET_JOB_SCOPE_INVALID';
  end if;
  if v_job.status='DONE' then
    select id into strict v_projection_id
      from private.bpay_next_net_projection
      where request_command_id=v_job.command_id;
    return pg_catalog.jsonb_build_object('phase','PROJECTED',
      'projection_id',v_projection_id,'replay',true);
  end if;
  if v_job.status<>'LEASED' or v_job.phase<>'NEW'
     or v_job.lease_nonce<>p_lease_nonce
     or v_job.owner_epoch<>p_owner_epoch
     or v_job.lease_until_utc<=pg_catalog.clock_timestamp()
     or v_run.status<>'DRAFT' or v_run.confirmed_at_utc is null
     or v_worker.status<>'READY'
     or v_worker.target_pay_channel<>'PAYE'
     or v_worker.net_request_revision<>1
     or v_worker.net_projection_revision<>0
     or v_request.request_no<>1
     or v_request.frozen_gross_inc_vat<>v_worker.gross_inc_vat
     or v_worker.case_selection_revision<>0
     or exists(select 1 from private.bpay_next_transfer t
               where t.run_worker_id=v_worker.id) then
    raise exception using errcode='55000',
      message='BPAY_NEXT_PAYE_NET_PROJECTION_NOT_ELIGIBLE';
  end if;
  insert into private.bpay_next_net_projection
    (run_worker_id,request_command_id,projection_no,input_kind,
     gross_ex_vat,gross_vat,gross_inc_vat,entered_paye_net,
     accepted_recoveries,cash_amount)
    values(v_worker.id,v_job.command_id,1,'PAYE_MANUAL',
      v_worker.gross_ex_vat,v_worker.gross_vat,v_worker.gross_inc_vat,
      v_request.entered_paye_net,0,v_request.entered_paye_net)
    returning id into v_projection_id;
  update private.bpay_next_run_worker
    set entered_paye_net=v_request.entered_paye_net,
        net_projection_revision=1
    where id=v_worker.id;
  perform private.bpay_next_publish_no_case_week_fact_v1(v_worker.id,v_projection_id);
  update private.bpay_next_job
    set status='DONE',phase='PROJECTED',lease_nonce=null,lease_until_utc=null
    where id=p_job_id;
  update private.bpay_next_command set status='COMPLETE'
    where id=v_job.command_id;
  return pg_catalog.jsonb_build_object('phase','PROJECTED',
    'projection_id',v_projection_id,'replay',false);
end
$function$;

alter function private.bpay_next_claim_simple_paye_net_job_v1(uuid,integer)
  owner to postgres;
alter function private.bpay_next_apply_simple_paye_net_v1(uuid,uuid,bigint)
  owner to postgres;
revoke all on function private.bpay_next_claim_simple_paye_net_job_v1(uuid,integer),
  private.bpay_next_apply_simple_paye_net_v1(uuid,uuid,bigint)
  from public,anon,authenticated,service_role;

commit;
